package rag

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type Strategy string

const (
	StrategySymbol   Strategy = "SYMBOL"
	StrategyKeyword  Strategy = "KEYWORD"
	StrategySemantic Strategy = "SEMANTIC"
	StrategyGraph    Strategy = "GRAPH"
)

type IndexStatus string

const (
	IndexBuilding IndexStatus = "BUILDING"
	IndexReady    IndexStatus = "READY"
	IndexFailed   IndexStatus = "FAILED"
)

type Filters struct {
	Languages    []string                  `json:"languages,omitempty"`
	ChunkTypes   []repository.ArtifactKind `json:"chunk_types,omitempty"`
	PathPrefixes []string                  `json:"path_prefixes,omitempty"`
	ArtifactIDs  []string                  `json:"artifact_ids,omitempty"`
}

func (f Filters) Validate() error {
	for _, value := range append(append([]string{}, f.Languages...), f.ArtifactIDs...) {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("filter values must not be empty")
		}
	}
	for _, prefix := range f.PathPrefixes {
		clean := filepath.ToSlash(filepath.Clean(strings.TrimSpace(prefix)))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || filepath.IsAbs(prefix) {
			return fmt.Errorf("path prefix must be repository relative: %q", prefix)
		}
	}
	for _, kind := range f.ChunkTypes {
		if strings.TrimSpace(string(kind)) == "" {
			return fmt.Errorf("chunk_types must not contain empty values")
		}
	}
	return nil
}

type RetrievalRequest struct {
	Scope      common.Scope `json:"scope"`
	Query      string       `json:"query"`
	Strategies []string     `json:"strategies,omitempty"`
	Filters    Filters      `json:"filters,omitempty"`
	TopK       int          `json:"top_k"`
}

func (r RetrievalRequest) Validate(maxQueryBytes, maxTopK int) error {
	if err := r.Scope.Validate(true); err != nil {
		return err
	}
	query := strings.TrimSpace(r.Query)
	if query == "" {
		return fmt.Errorf("query must not be empty")
	}
	if len(query) > maxQueryBytes {
		return fmt.Errorf("query exceeds %d bytes", maxQueryBytes)
	}
	if r.TopK < 0 || r.TopK > maxTopK {
		return fmt.Errorf("top_k must be between 0 and %d", maxTopK)
	}
	if _, err := NormalizeStrategies(r.Strategies); err != nil {
		return err
	}
	return r.Filters.Validate()
}

func NormalizeStrategies(values []string) ([]Strategy, error) {
	if len(values) == 0 {
		return []Strategy{StrategySymbol, StrategyKeyword, StrategySemantic, StrategyGraph}, nil
	}
	seen := map[Strategy]bool{}
	result := make([]Strategy, 0, len(values))
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		// Provider terminology is accepted at the boundary without leaking into
		// the stable domain strategy names.
		if value == "BM25" {
			value = string(StrategyKeyword)
		}
		if value == "VECTOR" {
			value = string(StrategySemantic)
		}
		strategy := Strategy(value)
		switch strategy {
		case StrategySymbol, StrategyKeyword, StrategySemantic, StrategyGraph:
		default:
			return nil, fmt.Errorf("unsupported retrieval strategy %q", value)
		}
		if !seen[strategy] {
			seen[strategy] = true
			result = append(result, strategy)
		}
	}
	return result, nil
}

type IndexDocument struct {
	DocumentID   string                  `json:"document_id"`
	ArtifactID   string                  `json:"artifact_id"`
	ChunkType    repository.ArtifactKind `json:"chunk_type"`
	Language     string                  `json:"language"`
	SourceRef    common.SourceRef        `json:"source_ref"`
	Text         string                  `json:"text"`
	SymbolTerms  []string                `json:"symbol_terms,omitempty"`
	GraphRefs    []string                `json:"graph_refs,omitempty"`
	EmbeddingRef string                  `json:"embedding_ref,omitempty"`
	Embedding    []float64               `json:"-"`
	IndexVersion string                  `json:"index_version"`
}

type IndexStats struct {
	Documents int `json:"documents"`
	Languages int `json:"languages"`
	Vectors   int `json:"vectors"`
}

type IndexRevision struct {
	common.EntityMeta
	RevisionID       string               `json:"revision_id"`
	SnapshotID       string               `json:"snapshot_id"`
	CommitSHA        string               `json:"commit_sha"`
	Status           IndexStatus          `json:"index_status"`
	AlgorithmVersion string               `json:"algorithm_version"`
	ContentHash      string               `json:"content_hash"`
	Stats            IndexStats           `json:"stats"`
	Documents        []IndexDocument      `json:"-"`
	PublishedEvent   common.EventEnvelope `json:"-"`
}

type ScoreBreakdown struct {
	Symbol   float64 `json:"symbol"`
	Keyword  float64 `json:"keyword"`
	Semantic float64 `json:"semantic"`
	Graph    float64 `json:"graph"`
	Rerank   float64 `json:"rerank,omitempty"`
}

type Hit struct {
	DocumentID string                  `json:"document_id"`
	ArtifactID string                  `json:"artifact_id"`
	SourceRef  common.SourceRef        `json:"source_ref"`
	ChunkType  repository.ArtifactKind `json:"chunk_type"`
	Language   string                  `json:"language"`
	Score      float64                 `json:"score"`
	Scores     ScoreBreakdown          `json:"scores"`
	Reasons    []string                `json:"reasons"`
}

type ContextItem struct {
	DocumentID string           `json:"document_id"`
	ArtifactID string           `json:"artifact_id"`
	Text       string           `json:"text"`
	SourceRef  common.SourceRef `json:"source_ref"`
	Score      float64          `json:"score"`
}

type ContextBundle struct {
	Items          []ContextItem `json:"items"`
	CharacterCount int           `json:"character_count"`
	Truncated      bool          `json:"truncated"`
}

type Diagnostics struct {
	RetrievalID      string           `json:"retrieval_id"`
	QueryHash        string           `json:"query_hash"`
	IndexRevisionID  string           `json:"index_revision_id"`
	AlgorithmVersion string           `json:"algorithm_version"`
	Strategies       []Strategy       `json:"strategies"`
	StrategyHits     map[Strategy]int `json:"strategy_hits"`
	Candidates       int              `json:"candidates"`
	DurationMS       int64            `json:"duration_ms"`
	CacheHit         bool             `json:"cache_hit"`
	Warnings         []string         `json:"warnings,omitempty"`
}

type EvidenceBundle struct {
	Hits          []Hit         `json:"hits"`
	ContextBundle ContextBundle `json:"context_bundle"`
	Diagnostics   Diagnostics   `json:"diagnostics"`
	// Compatibility fields are intentionally retained for existing Wiki and
	// Agent consumers. They are derived from Hits and never scored separately.
	ArtifactIDs []string           `json:"artifact_ids"`
	Sources     []common.SourceRef `json:"sources"`
}

func (r *IndexRevision) Normalize() {
	sort.Slice(r.Documents, func(i, j int) bool { return r.Documents[i].DocumentID < r.Documents[j].DocumentID })
	languages, vectors := map[string]bool{}, 0
	for _, document := range r.Documents {
		languages[strings.ToLower(document.Language)] = true
		if len(document.Embedding) > 0 {
			vectors++
		}
	}
	r.Stats = IndexStats{Documents: len(r.Documents), Languages: len(languages), Vectors: vectors}
}

func NewMeta(id string, scope common.Scope, status IndexStatus, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: string(status), Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: "svc_code_rag", TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}
