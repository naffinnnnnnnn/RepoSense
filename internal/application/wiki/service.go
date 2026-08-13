package wikiapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/wiki"
	"github.com/reposense/reposense/internal/ports"
)

type Config struct {
	MaxGraphNodes int
	RetrievalTopK int
}

func DefaultConfig() Config { return Config{MaxGraphNodes: 10_000, RetrievalTopK: 20} }

type Service struct {
	graph     ports.GraphStore
	retriever ports.Retriever
	repo      ports.WikiRepository
	generator ports.WikiContentGenerator
	events    ports.EventPublisher
	observer  ports.Observer
	ids       ports.IDGenerator
	clock     ports.Clock
	config    Config
}

func New(graphStore ports.GraphStore, retriever ports.Retriever, repo ports.WikiRepository, generator ports.WikiContentGenerator, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if graphStore == nil || repo == nil {
		return nil, errors.New("graph store and wiki repository must not be nil")
	}
	if generator == nil {
		generator = NewStructuredGenerator(StructuredGeneratorConfig{})
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
	defaults := DefaultConfig()
	if config.MaxGraphNodes <= 0 {
		config.MaxGraphNodes = defaults.MaxGraphNodes
	}
	if config.MaxGraphNodes > 10_000 {
		config.MaxGraphNodes = 10_000
	}
	if config.RetrievalTopK <= 0 {
		config.RetrievalTopK = defaults.RetrievalTopK
	}
	return &Service{graph: graphStore, retriever: retriever, repo: repo, generator: generator, events: events, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (s *Service) Generate(ctx context.Context, cmd wiki.GenerateCommand) (job wiki.Job, err error) {
	if err := cmd.Validate(); err != nil {
		return job, domainError(wiki.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if cmd.Locale == "" {
		cmd.Locale = wiki.DefaultLocale
	}
	if cached, ok, lookupErr := s.repo.FindJobByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); lookupErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return job, ctxErr
		}
		return job, domainError(wiki.ErrPersistence, "idempotency_lookup", "failed to check wiki idempotency key", true, lookupErr)
	} else if ok {
		if cached.SnapshotID != cmd.Scope.SnapshotID || cached.GraphRevisionID != cmd.GraphRevisionID {
			return job, domainError(wiki.ErrConflict, "idempotency_lookup", "idempotency key already used for another knowledge revision", false, nil)
		}
		s.observer.Count("wiki_generation_idempotency_hits_total", 1, labels(cmd.Scope))
		if publishErr := s.events.Publish(ctx, cached.PublishedEvent); publishErr != nil {
			return cached, domainError(wiki.ErrPersistence, "publish_event", "failed to republish stored wiki event", true, publishErr)
		}
		return cached, nil
	}
	finish := s.observer.Stage(ctx, "wiki_generate", labels(cmd.Scope))
	defer func() { finish(err) }()

	graphResult, queryErr := s.graph.Query(ctx, graph.Query{Scope: cmd.Scope, Direction: graph.DirectionBoth, Depth: 0, Limit: s.config.MaxGraphNodes})
	if queryErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return job, ctxErr
		}
		return job, domainError(wiki.ErrGraphNotReady, "load_graph", "graph revision is not ready for the requested snapshot", true, queryErr)
	}
	if graphResult.Diagnostics.RevisionID != cmd.GraphRevisionID {
		return job, domainError(wiki.ErrGraphNotReady, "validate_graph_revision", "requested graph revision does not match the snapshot's active revision", false, nil)
	}
	if len(graphResult.Nodes) == 0 {
		return job, domainError(wiki.ErrInsufficientEvidence, "validate_graph", "knowledge graph contains no entities", false, nil)
	}
	if graphResult.Diagnostics.Truncated {
		return job, domainError(wiki.ErrInsufficientEvidence, "validate_graph", "knowledge graph query was truncated; increase the configured limit before generating a complete wiki", false, nil)
	}

	evidence := ports.EvidenceBundle{}
	if s.retriever != nil {
		for _, slug := range cmd.SelectedSlugs() {
			bundle, searchErr := s.retriever.Search(ctx, ports.RetrievalRequest{Scope: cmd.Scope, Query: "wiki:" + slug, Strategies: []string{"SYMBOL", "KEYWORD", "GRAPH"}, TopK: s.config.RetrievalTopK})
			if searchErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return job, ctxErr
				}
				return job, domainError(wiki.ErrGenerationFailure, "retrieve_evidence", "failed to retrieve wiki evidence", true, searchErr)
			}
			evidence.ArtifactIDs = append(evidence.ArtifactIDs, bundle.ArtifactIDs...)
			evidence.Sources = append(evidence.Sources, bundle.Sources...)
		}
		evidence.Sources = wiki.NormalizeCitations(evidence.Sources)
	}

	generated, generateErr := s.generator.Generate(ctx, ports.WikiGenerationContext{Scope: cmd.Scope, Locale: cmd.Locale, PageSlugs: cmd.SelectedSlugs(), Graph: graphResult, Evidence: evidence})
	if generateErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return job, ctxErr
		}
		return job, domainError(wiki.ErrGenerationFailure, "generate_content", "wiki content generation failed", true, generateErr)
	}
	if generated.Model == "" {
		generated.Model = wiki.DefaultModel
	}
	if generated.PromptVersion == "" {
		generated.PromptVersion = wiki.DefaultPromptVersion
	}
	pages, buildErr := s.buildPages(ctx, cmd, generated, graphResult)
	if buildErr != nil {
		return job, buildErr
	}

	now := s.clock.Now().UTC()
	jobID := s.ids.New("wj")
	job = wiki.Job{EntityMeta: wiki.NewMeta(jobID, cmd.Scope, string(wiki.JobSucceeded), "svc_wiki", now), JobID: jobID,
		SnapshotID: cmd.Scope.SnapshotID, GraphRevisionID: cmd.GraphRevisionID, Status: wiki.JobSucceeded}
	for _, page := range pages {
		job.PageIDs = append(job.PageIDs, page.PageID)
	}
	job.PublishedEvent = common.EventEnvelope{EventID: s.ids.New("evt"), EventType: "wiki.published.v1", AggregateID: job.JobID,
		OccurredAt: s.clock.Now().UTC(), Producer: "ai-wiki", PayloadVersion: 1, TraceID: cmd.Scope.TraceID,
		Payload: map[string]any{"job_id": job.JobID, "snapshot_id": job.SnapshotID, "graph_revision_id": job.GraphRevisionID, "page_ids": append([]string(nil), job.PageIDs...), "page_count": len(pages), "locale": cmd.Locale, "model": generated.Model, "prompt_version": generated.PromptVersion, "token_usage": generated.TokenUsage}}
	spaceID := stableID("ws", cmd.Scope.TenantID, cmd.Scope.RepositoryID, cmd.Locale)
	space := wiki.Space{EntityMeta: wiki.NewMeta(spaceID, cmd.Scope, "ACTIVE", "svc_wiki", now), SpaceID: spaceID,
		Title: cmd.Scope.RepositoryID, Locale: cmd.Locale, ActiveSnapshotID: cmd.Scope.SnapshotID}
	if saveErr := s.repo.SavePublication(ctx, cmd.IdempotencyKey, space, job, pages); saveErr != nil {
		return wiki.Job{}, domainError(wiki.ErrPersistence, "save_publication", "failed to atomically publish wiki pages", true, saveErr)
	}
	if publishErr := s.events.Publish(ctx, job.PublishedEvent); publishErr != nil {
		return job, domainError(wiki.ErrPersistence, "publish_event", "wiki pages were saved but event publication failed", true, publishErr)
	}
	s.observer.Count("wiki_generation_pages_total", int64(len(pages)), labels(cmd.Scope))
	s.observer.Count("wiki_generation_citations_total", int64(countCitations(pages)), labels(cmd.Scope))
	if generated.TokenUsage > 0 {
		s.observer.Count("wiki_generation_tokens_total", int64(generated.TokenUsage), labels(cmd.Scope))
	}
	return job, nil
}

func (s *Service) GetPage(ctx context.Context, scope common.Scope, slug string) (page wiki.PageRevision, err error) {
	if validateErr := scope.Validate(true); validateErr != nil {
		return page, domainError(wiki.ErrInvalidInput, "validate", validateErr.Error(), false, validateErr)
	}
	if strings.TrimSpace(slug) == "" {
		return page, domainError(wiki.ErrInvalidInput, "validate", "slug must not be empty", false, nil)
	}
	finish := s.observer.Stage(ctx, "wiki_get_page", labels(scope))
	defer func() { finish(err) }()
	page, err = s.repo.GetPage(ctx, scope, slug)
	return page, err
}

func (s *Service) buildPages(ctx context.Context, cmd wiki.GenerateCommand, generated wiki.GenerationResult, graphResult graph.Result) ([]wiki.PageRevision, error) {
	expected := cmd.SelectedSlugs()
	if len(generated.Pages) != len(expected) {
		return nil, domainError(wiki.ErrGenerationFailure, "validate_output", "generator returned an unexpected page count", false, nil)
	}
	drafts := make(map[string]wiki.PageDraft, len(generated.Pages))
	for _, draft := range generated.Pages {
		if _, exists := drafts[draft.Slug]; exists {
			return nil, domainError(wiki.ErrGenerationFailure, "validate_output", "generator returned duplicate page slugs", false, nil)
		}
		drafts[draft.Slug] = draft
	}
	now := s.clock.Now().UTC()
	pages := make([]wiki.PageRevision, 0, len(expected))
	overviewExists := false
	for _, slug := range expected {
		if slug == "overview" {
			overviewExists = true
			break
		}
	}
	if !overviewExists {
		_, found, loadErr := s.repo.LatestPage(ctx, cmd.Scope, cmd.Locale, "overview")
		if loadErr != nil {
			return nil, domainError(wiki.ErrPersistence, "load_overview_page", "failed to load overview wiki revision", true, loadErr)
		}
		overviewExists = found
	}
	for _, slug := range expected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		draft, ok := drafts[slug]
		if !ok {
			return nil, domainError(wiki.ErrGenerationFailure, "validate_output", "generator omitted requested page "+slug, false, nil)
		}
		citations := wiki.NormalizeCitations(draft.Citations)
		for _, citation := range citations {
			if citation.CommitSHA != "" && !graphHasCommit(graphResult, citation.CommitSHA) {
				return nil, domainError(wiki.ErrGenerationFailure, "validate_citation", "generator returned a citation from another commit", false, nil)
			}
		}
		pageID := stableID("page", cmd.Scope.TenantID, cmd.Scope.RepositoryID, cmd.Locale, slug)
		revisionNo := int64(1)
		if prior, found, loadErr := s.repo.LatestPage(ctx, cmd.Scope, cmd.Locale, slug); loadErr != nil {
			return nil, domainError(wiki.ErrPersistence, "load_previous_page", "failed to load previous wiki revision", true, loadErr)
		} else if found {
			pageID, revisionNo = prior.PageID, prior.RevisionNo+1
		}
		page := wiki.PageRevision{EntityMeta: wiki.NewMeta(stableID("wpr", pageID, fmt.Sprint(revisionNo)), cmd.Scope, "ACTIVE", "svc_wiki", now),
			PageID: pageID, RevisionNo: revisionNo, SnapshotID: cmd.Scope.SnapshotID, GraphRevisionID: cmd.GraphRevisionID, Locale: cmd.Locale, Slug: slug,
			Title: strings.TrimSpace(draft.Title), ContentMarkdown: strings.TrimSpace(draft.ContentMarkdown), Citations: citations,
			GenerationMode: wiki.GenerationAI, ReviewStatus: wiki.ReviewApproved, Model: generated.Model, PromptVersion: generated.PromptVersion}
		if slug != "overview" && overviewExists {
			page.ParentPageID = stableID("page", cmd.Scope.TenantID, cmd.Scope.RepositoryID, cmd.Locale, "overview")
		}
		page.ContentHash = contentHash(page.ContentMarkdown, citations)
		page.EntityMeta.Version = revisionNo
		if validateErr := page.Validate(); validateErr != nil {
			return nil, domainError(wiki.ErrGenerationFailure, "validate_output", validateErr.Error(), false, validateErr)
		}
		pages = append(pages, page)
	}
	return pages, nil
}

func graphHasCommit(result graph.Result, commit string) bool {
	for _, node := range result.Nodes {
		if node.SourceRef != nil && node.SourceRef.CommitSHA == commit {
			return true
		}
	}
	for _, edge := range result.Edges {
		if edge.Evidence.CommitSHA == commit {
			return true
		}
	}
	return false
}
func contentHash(content string, refs []common.SourceRef) string {
	h := sha256.New()
	h.Write([]byte(content))
	for _, ref := range refs {
		fmt.Fprintf(h, "\x00%s:%s:%s:%d:%d:%s", ref.CommitSHA, ref.Path, ref.SymbolID, ref.StartLine, ref.EndLine, ref.ContentHash)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
func stableID(prefix string, parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return prefix + "_" + hex.EncodeToString(h.Sum(nil))[:24]
}
func countCitations(pages []wiki.PageRevision) int {
	total := 0
	for _, page := range pages {
		total += len(page.Citations)
	}
	return total
}
func labels(scope common.Scope) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID}
}
func domainError(code wiki.ErrorCode, operation, message string, retryable bool, cause error) *wiki.DomainError {
	return &wiki.DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
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
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

var _ ports.WikiService = (*Service)(nil)
