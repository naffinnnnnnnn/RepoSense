package ragapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

const AlgorithmVersion = "hybrid-rag@1"

type Config struct {
	DefaultTopK     int
	MaxTopK         int
	MaxQueryBytes   int
	MaxDocuments    int
	MaxContextChars int
	GraphDepth      int
	GraphSeedCount  int
	SymbolWeight    float64
	KeywordWeight   float64
	SemanticWeight  float64
	GraphWeight     float64
}

func DefaultConfig() Config {
	return Config{DefaultTopK: 10, MaxTopK: 100, MaxQueryBytes: 4096, MaxDocuments: 1_000_000,
		MaxContextChars: 24_000, GraphDepth: 1, GraphSeedCount: 5,
		SymbolWeight: .35, KeywordWeight: .25, SemanticWeight: .25, GraphWeight: .15}
}

type Service struct {
	repo       ports.RAGRepository
	graph      ports.GraphStore
	events     ports.EventPublisher
	observer   ports.Observer
	ids        ports.IDGenerator
	clock      ports.Clock
	vectorizer ports.Vectorizer
	reranker   ports.Reranker
	config     Config
}

func New(repo ports.RAGRepository, graphStore ports.GraphStore, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, vectorizer ports.Vectorizer, reranker ports.Reranker, config Config) (*Service, error) {
	if repo == nil {
		return nil, errors.New("RAG repository must not be nil")
	}
	if events == nil {
		events = noopPublisher{}
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = randomIDs{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	if vectorizer == nil {
		vectorizer = HashVectorizer{}
	}
	if reranker == nil {
		reranker = IdentityReranker{}
	}
	defaults := DefaultConfig()
	applyDefaults(&config, defaults)
	if config.DefaultTopK > config.MaxTopK {
		return nil, errors.New("default top_k must not exceed maximum top_k")
	}
	if config.SymbolWeight < 0 || config.KeywordWeight < 0 || config.SemanticWeight < 0 || config.GraphWeight < 0 {
		return nil, errors.New("retrieval weights must not be negative")
	}
	if config.SymbolWeight+config.KeywordWeight+config.SemanticWeight+config.GraphWeight == 0 {
		return nil, errors.New("at least one retrieval weight must be positive")
	}
	return &Service{repo: repo, graph: graphStore, events: events, observer: observer, ids: ids, clock: clock,
		vectorizer: vectorizer, reranker: reranker, config: config}, nil
}

func applyDefaults(config *Config, defaults Config) {
	if config.DefaultTopK <= 0 {
		config.DefaultTopK = defaults.DefaultTopK
	}
	if config.MaxTopK <= 0 {
		config.MaxTopK = defaults.MaxTopK
	}
	if config.MaxQueryBytes <= 0 {
		config.MaxQueryBytes = defaults.MaxQueryBytes
	}
	if config.MaxDocuments <= 0 {
		config.MaxDocuments = defaults.MaxDocuments
	}
	if config.MaxContextChars <= 0 {
		config.MaxContextChars = defaults.MaxContextChars
	}
	if config.GraphDepth <= 0 {
		config.GraphDepth = defaults.GraphDepth
	}
	if config.GraphSeedCount <= 0 {
		config.GraphSeedCount = defaults.GraphSeedCount
	}
	if config.SymbolWeight+config.KeywordWeight+config.SemanticWeight+config.GraphWeight == 0 {
		config.SymbolWeight, config.KeywordWeight, config.SemanticWeight, config.GraphWeight = defaults.SymbolWeight, defaults.KeywordWeight, defaults.SemanticWeight, defaults.GraphWeight
	}
}

func (s *Service) Index(ctx context.Context, scope common.Scope, artifacts []repository.CodeArtifact) (revision rag.IndexRevision, err error) {
	if err := scope.Validate(true); err != nil {
		return revision, domainError(rag.ErrInvalidInput, "validate_scope", err.Error(), false, err)
	}
	if len(artifacts) > s.config.MaxDocuments {
		return revision, domainError(rag.ErrInvalidInput, "validate_size", "index input exceeds configured document limit", false, nil)
	}
	finish := s.observer.Stage(ctx, "rag_index", labels(scope))
	defer func() { finish(err) }()

	documents, commit, buildErr := buildDocuments(scope, artifacts)
	if buildErr != nil {
		return revision, domainError(rag.ErrInvalidInput, "build_documents", buildErr.Error(), false, buildErr)
	}
	contentHash := revisionHash(scope, documents, s.vectorizer.Version())
	if current, lookupErr := s.repo.RevisionBySnapshot(ctx, scope); lookupErr == nil && current.ContentHash == contentHash {
		s.observer.Count("rag_index_idempotency_hits_total", 1, labels(scope))
		if publishErr := s.events.Publish(ctx, current.PublishedEvent); publishErr != nil {
			return current, domainError(rag.ErrPersistence, "publish_event", "failed to republish stored index event", true, publishErr)
		}
		return current, nil
	} else if lookupErr != nil && !rag.IsCode(lookupErr, rag.ErrIndexNotFound) {
		return revision, domainError(rag.ErrPersistence, "load_revision", "failed to inspect current index revision", true, lookupErr)
	}
	texts := make([]string, len(documents))
	for i := range documents {
		texts[i] = documents[i].Text
	}
	vectors, vectorErr := s.vectorizer.Vectorize(ctx, texts)
	if vectorErr != nil {
		return revision, domainError(rag.ErrIndexFailure, "vectorize", "failed to vectorize index documents", true, vectorErr)
	}
	if len(vectors) != len(documents) {
		return revision, domainError(rag.ErrIndexFailure, "vectorize", "vectorizer returned an unexpected result count", false, nil)
	}
	for i := range documents {
		documents[i].Embedding = append([]float64(nil), vectors[i]...)
		documents[i].EmbeddingRef = "vec_" + digest(documents[i].DocumentID, s.vectorizer.Version())
		documents[i].IndexVersion = AlgorithmVersion
	}
	now, revisionID := s.clock.Now().UTC(), s.ids.New("idx")
	revision = rag.IndexRevision{EntityMeta: rag.NewMeta(revisionID, scope, rag.IndexBuilding, now), RevisionID: revisionID,
		SnapshotID: scope.SnapshotID, CommitSHA: commit, Status: rag.IndexBuilding, AlgorithmVersion: algorithmLabel(s.vectorizer, s.reranker), ContentHash: contentHash, Documents: documents}
	revision.Status, revision.EntityMeta.Status, revision.UpdatedAt = rag.IndexReady, string(rag.IndexReady), s.clock.Now().UTC()
	revision.Normalize()
	revision.PublishedEvent = common.EventEnvelope{EventID: s.ids.New("evt"), EventType: "index.ready.v1", AggregateID: revision.RevisionID,
		OccurredAt: s.clock.Now().UTC(), Producer: "code-rag", PayloadVersion: 1, TraceID: scope.TraceID,
		Payload: map[string]any{"revision_id": revision.RevisionID, "snapshot_id": revision.SnapshotID, "commit_sha": revision.CommitSHA,
			"documents": revision.Stats.Documents, "vectors": revision.Stats.Vectors, "algorithm_version": revision.AlgorithmVersion}}
	if err = s.repo.Save(ctx, revision); err != nil {
		return rag.IndexRevision{}, domainError(rag.ErrPersistence, "publish_revision", "failed to atomically publish index revision", true, err)
	}
	if err = s.events.Publish(ctx, revision.PublishedEvent); err != nil {
		return revision, domainError(rag.ErrPersistence, "publish_event", "index revision was saved but event publication failed", true, err)
	}
	s.observer.Count("rag_index_documents_total", int64(revision.Stats.Documents), labels(scope))
	s.observer.Count("rag_index_vectors_total", int64(revision.Stats.Vectors), labels(scope))
	return revision, nil
}

func (s *Service) Search(ctx context.Context, request rag.RetrievalRequest) (bundle rag.EvidenceBundle, err error) {
	if err := request.Validate(s.config.MaxQueryBytes, s.config.MaxTopK); err != nil {
		return bundle, domainError(rag.ErrInvalidInput, "validate_request", err.Error(), false, err)
	}
	if request.TopK == 0 {
		request.TopK = s.config.DefaultTopK
	}
	strategies, _ := rag.NormalizeStrategies(request.Strategies)
	started := s.clock.Now().UTC()
	finish := s.observer.Stage(ctx, "rag_search", labels(request.Scope))
	defer func() { finish(err) }()

	revision, loadErr := s.repo.RevisionBySnapshot(ctx, request.Scope)
	if loadErr != nil {
		if rag.IsCode(loadErr, rag.ErrIndexNotFound) {
			return bundle, loadErr
		}
		return bundle, domainError(rag.ErrPersistence, "load_revision", "failed to load RAG index revision", true, loadErr)
	}
	if revision.Status != rag.IndexReady {
		return bundle, domainError(rag.ErrIndexNotFound, "validate_revision", "RAG index is not ready", true, nil)
	}
	documents, loadErr := s.repo.Documents(ctx, request.Scope, revision.RevisionID)
	if loadErr != nil {
		return bundle, domainError(rag.ErrPersistence, "load_documents", "failed to load RAG index documents", true, loadErr)
	}
	documents = filterDocuments(documents, request.Filters)
	diagnostics := rag.Diagnostics{RetrievalID: s.ids.New("ret"), QueryHash: "sha256:" + fullDigest(strings.TrimSpace(request.Query)),
		IndexRevisionID: revision.RevisionID, AlgorithmVersion: revision.AlgorithmVersion, Strategies: strategies, StrategyHits: map[rag.Strategy]int{}}
	if len(documents) == 0 {
		diagnostics.DurationMS = s.clock.Now().UTC().Sub(started).Milliseconds()
		bundle.Diagnostics = diagnostics
		return bundle, nil
	}

	scores, scoreErr := s.score(ctx, request, strategies, documents, &diagnostics)
	if scoreErr != nil {
		return bundle, scoreErr
	}
	hits := makeHits(scores, documents, s.config, strategies)
	diagnostics.Candidates = len(hits)
	if len(hits) > request.TopK*4 {
		hits = hits[:request.TopK*4]
	}
	hits, rerankErr := s.reranker.Rerank(ctx, strings.TrimSpace(request.Query), hits)
	if rerankErr != nil {
		return bundle, domainError(rag.ErrRetrievalFailure, "rerank", "failed to rerank retrieval candidates", true, rerankErr)
	}
	if validationErr := validateRerankedHits(hits, scores, documents); validationErr != nil {
		return bundle, domainError(rag.ErrRetrievalFailure, "rerank", validationErr.Error(), false, validationErr)
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].DocumentID < hits[j].DocumentID
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > request.TopK {
		hits = hits[:request.TopK]
	}
	bundle.Hits = hits
	bundle.ContextBundle = buildContext(hits, documents, s.config.MaxContextChars)
	for _, hit := range hits {
		bundle.ArtifactIDs = append(bundle.ArtifactIDs, hit.ArtifactID)
		bundle.Sources = append(bundle.Sources, hit.SourceRef)
	}
	diagnostics.DurationMS = s.clock.Now().UTC().Sub(started).Milliseconds()
	bundle.Diagnostics = diagnostics
	s.observer.Count("rag_search_requests_total", 1, labels(request.Scope))
	s.observer.Count("rag_search_hits_total", int64(len(hits)), labels(request.Scope))
	return bundle, nil
}

type documentScores struct{ symbol, keyword, semantic, graph float64 }

func (s *Service) score(ctx context.Context, request rag.RetrievalRequest, strategies []rag.Strategy, documents []rag.IndexDocument, diagnostics *rag.Diagnostics) (map[string]documentScores, error) {
	active := strategySet(strategies)
	queryTokens := tokenize(request.Query)
	scores := make(map[string]documentScores, len(documents))
	if active[rag.StrategySymbol] || active[rag.StrategyGraph] {
		for _, document := range documents {
			value := symbolScore(queryTokens, document.SymbolTerms)
			if value > 0 {
				current := scores[document.DocumentID]
				current.symbol = value
				scores[document.DocumentID] = current
				diagnostics.StrategyHits[rag.StrategySymbol]++
			}
		}
	}
	if active[rag.StrategyKeyword] || active[rag.StrategyGraph] {
		values := bm25Scores(queryTokens, documents)
		for id, value := range values {
			if value > 0 {
				current := scores[id]
				current.keyword = value
				scores[id] = current
				diagnostics.StrategyHits[rag.StrategyKeyword]++
			}
		}
	}
	if active[rag.StrategySemantic] {
		vectors, err := s.vectorizer.Vectorize(ctx, []string{request.Query})
		if err != nil {
			return nil, domainError(rag.ErrRetrievalFailure, "vectorize_query", "failed to vectorize query", true, err)
		}
		if len(vectors) != 1 {
			return nil, domainError(rag.ErrRetrievalFailure, "vectorize_query", "vectorizer returned an unexpected result count", false, nil)
		}
		for _, document := range documents {
			value := cosine(vectors[0], document.Embedding)
			if value > 0 {
				current := scores[document.DocumentID]
				current.semantic = value
				scores[document.DocumentID] = current
				diagnostics.StrategyHits[rag.StrategySemantic]++
			}
		}
	}
	if active[rag.StrategyGraph] {
		if err := s.expandGraph(ctx, request.Scope, documents, scores, diagnostics); err != nil {
			return nil, err
		}
	}
	return scores, nil
}

func (s *Service) expandGraph(ctx context.Context, scope common.Scope, documents []rag.IndexDocument, scores map[string]documentScores, diagnostics *rag.Diagnostics) error {
	if s.graph == nil {
		diagnostics.Warnings = append(diagnostics.Warnings, "graph retrieval was requested but no graph store is configured")
		return nil
	}
	type seed struct {
		id    string
		score float64
	}
	var seeds []seed
	byArtifact := map[string]string{}
	for _, document := range documents {
		byArtifact[document.ArtifactID] = document.DocumentID
	}
	for id, value := range scores {
		seeds = append(seeds, seed{id: id, score: value.symbol + value.keyword + value.semantic})
	}
	sort.Slice(seeds, func(i, j int) bool {
		if seeds[i].score == seeds[j].score {
			return seeds[i].id < seeds[j].id
		}
		return seeds[i].score > seeds[j].score
	})
	if len(seeds) > s.config.GraphSeedCount {
		seeds = seeds[:s.config.GraphSeedCount]
	}
	rootIDs := make([]string, 0, len(seeds))
	for _, value := range seeds {
		for _, document := range documents {
			if document.DocumentID == value.id {
				rootIDs = append(rootIDs, document.ArtifactID)
				break
			}
		}
	}
	if len(rootIDs) == 0 {
		diagnostics.Warnings = append(diagnostics.Warnings, "graph retrieval found no lexical seed")
		return nil
	}
	result, err := s.graph.Query(ctx, graph.Query{Scope: scope, RootIDs: rootIDs, Direction: graph.DirectionBoth, Depth: s.config.GraphDepth, Limit: s.config.MaxTopK * 10})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		diagnostics.Warnings = append(diagnostics.Warnings, "graph retrieval was partially unavailable")
		return nil
	}
	seedSet := map[string]bool{}
	for _, id := range rootIDs {
		seedSet[id] = true
	}
	for _, node := range result.Nodes {
		documentID, ok := byArtifact[node.ArtifactID]
		if !ok {
			continue
		}
		value := .65
		if seedSet[node.ArtifactID] {
			value = 1
		}
		current := scores[documentID]
		if value > current.graph {
			current.graph = value
			scores[documentID] = current
			diagnostics.StrategyHits[rag.StrategyGraph]++
		}
	}
	return nil
}

func buildDocuments(scope common.Scope, artifacts []repository.CodeArtifact) ([]rag.IndexDocument, string, error) {
	seen, commit := map[string]bool{}, ""
	documents := make([]rag.IndexDocument, 0, len(artifacts))
	for _, artifact := range artifacts {
		if strings.TrimSpace(artifact.ArtifactID) == "" || strings.TrimSpace(artifact.Name) == "" {
			return nil, "", fmt.Errorf("artifact_id and name are required")
		}
		if seen[artifact.ArtifactID] {
			return nil, "", fmt.Errorf("duplicate artifact_id %q", artifact.ArtifactID)
		}
		seen[artifact.ArtifactID] = true
		if err := artifact.SourceRef.Validate(); err != nil {
			return nil, "", fmt.Errorf("artifact %q: %w", artifact.ArtifactID, err)
		}
		if artifact.ContentHash == "" || artifact.SourceRef.ContentHash == "" || artifact.ContentHash != artifact.SourceRef.ContentHash {
			return nil, "", fmt.Errorf("artifact %q content hash does not match its source reference", artifact.ArtifactID)
		}
		if commit == "" {
			commit = artifact.SourceRef.CommitSHA
		} else if commit != artifact.SourceRef.CommitSHA {
			return nil, "", fmt.Errorf("artifacts span multiple commits")
		}
		text := documentText(artifact)
		documents = append(documents, rag.IndexDocument{DocumentID: "doc_" + digest(scope.TenantID, scope.RepositoryID, scope.SnapshotID, artifact.ArtifactID, artifact.ContentHash),
			ArtifactID: artifact.ArtifactID, ChunkType: artifact.Kind, Language: strings.ToLower(artifact.Language), SourceRef: artifact.SourceRef,
			Text: text, SymbolTerms: symbolTerms(artifact), GraphRefs: []string{artifact.ArtifactID}})
	}
	sort.Slice(documents, func(i, j int) bool { return documents[i].DocumentID < documents[j].DocumentID })
	return documents, commit, nil
}

func documentText(artifact repository.CodeArtifact) string {
	parts := []string{artifact.Name, artifact.QualifiedName, artifact.Signature, string(artifact.Kind), artifact.Language, filepath.ToSlash(artifact.SourceRef.Path)}
	keys := make([]string, 0, len(artifact.Attributes))
	for key := range artifact.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key, artifact.Attributes[key])
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func symbolTerms(artifact repository.CodeArtifact) []string {
	seen, result := map[string]bool{}, []string{}
	for _, value := range append(tokenize(artifact.Name), tokenize(artifact.QualifiedName)...) {
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	for _, value := range []string{strings.ToLower(artifact.Name), strings.ToLower(artifact.QualifiedName)} {
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func symbolScore(query, terms []string) float64 {
	if len(query) == 0 || len(terms) == 0 {
		return 0
	}
	matched, exact := 0.0, 0.0
	for _, q := range query {
		for _, term := range terms {
			if q == term {
				exact++
				break
			}
			if len(q) >= 2 && (strings.HasPrefix(term, q) || strings.HasPrefix(q, term)) {
				matched += .6
				break
			}
		}
	}
	return math.Min(1, (exact+matched)/float64(len(query)))
}

func bm25Scores(query []string, documents []rag.IndexDocument) map[string]float64 {
	result := map[string]float64{}
	if len(query) == 0 || len(documents) == 0 {
		return result
	}
	query = uniqueStrings(query)
	tokens, frequencies, documentFrequency, totalLength := map[string][]string{}, map[string]map[string]int{}, map[string]int{}, 0
	for _, document := range documents {
		values := tokenize(document.Text)
		tokens[document.DocumentID] = values
		totalLength += len(values)
		counts, present := map[string]int{}, map[string]bool{}
		for _, value := range values {
			counts[value]++
			present[value] = true
		}
		frequencies[document.DocumentID] = counts
		for _, q := range query {
			if present[q] {
				documentFrequency[q]++
			}
		}
	}
	average := float64(totalLength) / float64(len(documents))
	if average == 0 {
		average = 1
	}
	maxScore := 0.0
	for _, document := range documents {
		length, score := float64(len(tokens[document.DocumentID])), 0.0
		for _, q := range query {
			tf := float64(frequencies[document.DocumentID][q])
			if tf == 0 {
				continue
			}
			idf := math.Log(1 + (float64(len(documents))-float64(documentFrequency[q])+.5)/(float64(documentFrequency[q])+.5))
			score += idf * (tf * 2.2) / (tf + 1.2*(.25+.75*length/average))
		}
		result[document.DocumentID] = score
		if score > maxScore {
			maxScore = score
		}
	}
	if maxScore > 0 {
		for id := range result {
			result[id] /= maxScore
		}
	}
	return result
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func makeHits(scores map[string]documentScores, documents []rag.IndexDocument, config Config, strategies []rag.Strategy) []rag.Hit {
	byID := map[string]rag.IndexDocument{}
	for _, document := range documents {
		byID[document.DocumentID] = document
	}
	active, hits := strategySet(strategies), []rag.Hit{}
	for id, values := range scores {
		score := 0.0
		if active[rag.StrategySymbol] {
			score += values.symbol * config.SymbolWeight
		}
		if active[rag.StrategyKeyword] {
			score += values.keyword * config.KeywordWeight
		}
		if active[rag.StrategySemantic] {
			score += values.semantic * config.SemanticWeight
		}
		if active[rag.StrategyGraph] {
			score += values.graph * config.GraphWeight
		}
		if score <= 0 {
			continue
		}
		document, ok := byID[id]
		if !ok {
			continue
		}
		reasons := []string{}
		if active[rag.StrategySymbol] && values.symbol > 0 {
			reasons = append(reasons, "symbol")
		}
		if active[rag.StrategyKeyword] && values.keyword > 0 {
			reasons = append(reasons, "keyword")
		}
		if active[rag.StrategySemantic] && values.semantic > 0 {
			reasons = append(reasons, "semantic")
		}
		if active[rag.StrategyGraph] && values.graph > 0 {
			reasons = append(reasons, "graph")
		}
		hits = append(hits, rag.Hit{DocumentID: id, ArtifactID: document.ArtifactID, SourceRef: document.SourceRef, ChunkType: document.ChunkType,
			Language: document.Language, Score: score, Scores: rag.ScoreBreakdown{Symbol: values.symbol, Keyword: values.keyword, Semantic: values.semantic, Graph: values.graph}, Reasons: reasons})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].DocumentID < hits[j].DocumentID
		}
		return hits[i].Score > hits[j].Score
	})
	return hits
}

func buildContext(hits []rag.Hit, documents []rag.IndexDocument, limit int) rag.ContextBundle {
	byID := map[string]rag.IndexDocument{}
	for _, document := range documents {
		byID[document.DocumentID] = document
	}
	result := rag.ContextBundle{}
	for _, hit := range hits {
		document := byID[hit.DocumentID]
		remaining := limit - result.CharacterCount
		if remaining <= 0 {
			result.Truncated = true
			break
		}
		textRunes := []rune(document.Text)
		if len(textRunes) > remaining {
			textRunes = textRunes[:remaining]
			result.Truncated = true
		}
		text := string(textRunes)
		result.Items = append(result.Items, rag.ContextItem{DocumentID: hit.DocumentID, ArtifactID: hit.ArtifactID, Text: text, SourceRef: hit.SourceRef, Score: hit.Score})
		result.CharacterCount += len(textRunes)
		if result.Truncated {
			break
		}
	}
	return result
}

func validateRerankedHits(hits []rag.Hit, scores map[string]documentScores, documents []rag.IndexDocument) error {
	documentsByID := make(map[string]rag.IndexDocument, len(documents))
	for _, document := range documents {
		documentsByID[document.DocumentID] = document
	}
	seen := make(map[string]bool, len(hits))
	for _, hit := range hits {
		document, exists := documentsByID[hit.DocumentID]
		_, wasCandidate := scores[hit.DocumentID]
		if !exists || !wasCandidate || hit.ArtifactID != document.ArtifactID || hit.SourceRef != document.SourceRef {
			return fmt.Errorf("reranker returned an unknown or modified document")
		}
		if seen[hit.DocumentID] {
			return fmt.Errorf("reranker returned duplicate document %q", hit.DocumentID)
		}
		if math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) || hit.Score < 0 {
			return fmt.Errorf("reranker returned an invalid score")
		}
		seen[hit.DocumentID] = true
	}
	return nil
}

func filterDocuments(documents []rag.IndexDocument, filters rag.Filters) []rag.IndexDocument {
	languages, kinds, ids := map[string]bool{}, map[repository.ArtifactKind]bool{}, map[string]bool{}
	for _, value := range filters.Languages {
		languages[strings.ToLower(strings.TrimSpace(value))] = true
	}
	for _, value := range filters.ChunkTypes {
		kinds[value] = true
	}
	for _, value := range filters.ArtifactIDs {
		ids[value] = true
	}
	result := make([]rag.IndexDocument, 0, len(documents))
	for _, document := range documents {
		if len(languages) > 0 && !languages[strings.ToLower(document.Language)] {
			continue
		}
		if len(kinds) > 0 && !kinds[document.ChunkType] {
			continue
		}
		if len(ids) > 0 && !ids[document.ArtifactID] {
			continue
		}
		if len(filters.PathPrefixes) > 0 {
			matched := false
			path := filepath.ToSlash(document.SourceRef.Path)
			for _, prefix := range filters.PathPrefixes {
				clean := strings.TrimSuffix(filepath.ToSlash(filepath.Clean(prefix)), "/")
				if path == clean || strings.HasPrefix(path, clean+"/") {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		result = append(result, document)
	}
	return result
}

func strategySet(values []rag.Strategy) map[rag.Strategy]bool {
	result := map[rag.Strategy]bool{}
	for _, value := range values {
		result[value] = true
	}
	return result
}
func revisionHash(scope common.Scope, documents []rag.IndexDocument, vectorizer string) string {
	parts := []string{scope.TenantID, scope.RepositoryID, scope.SnapshotID, AlgorithmVersion, vectorizer}
	for _, document := range documents {
		parts = append(parts, document.DocumentID, document.SourceRef.ContentHash)
	}
	return "sha256:" + fullDigest(parts...)
}
func algorithmLabel(vectorizer ports.Vectorizer, reranker ports.Reranker) string {
	return AlgorithmVersion + ";vectorizer=" + vectorizer.Version() + ";reranker=" + reranker.Version()
}
func fullDigest(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		hash.Write([]byte(part))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
func digest(parts ...string) string { return fullDigest(parts...)[:24] }
func labels(scope common.Scope) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID}
}
func domainError(code rag.ErrorCode, operation, message string, retryable bool, cause error) *rag.DomainError {
	return &rag.DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type randomIDs struct{}

func (randomIDs) New(prefix string) string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(value[:])
}
