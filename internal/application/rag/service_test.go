package ragapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
)

type sequenceIDs struct{ n int }

func (i *sequenceIDs) New(prefix string) string { i.n++; return prefix + string(rune('0'+i.n)) }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC) }

type eventSink struct{ events []common.EventEnvelope }

func (s *eventSink) Publish(_ context.Context, event common.EventEnvelope) error {
	s.events = append(s.events, event)
	return nil
}

type countingVectorizer struct {
	calls int
	err   error
	HashVectorizer
}

func (v *countingVectorizer) Vectorize(ctx context.Context, texts []string) ([][]float64, error) {
	v.calls++
	if v.err != nil {
		return nil, v.err
	}
	return v.HashVectorizer.Vectorize(ctx, texts)
}
func (v *countingVectorizer) Version() string { return "counting@1" }

func TestIndexAndHybridSearchRespectFiltersAndIdempotency(t *testing.T) {
	repositoryAdapter := memory.NewRAGRepository()
	events, vectors := &eventSink{}, &countingVectorizer{}
	service, err := New(repositoryAdapter, nil, events, nil, &sequenceIDs{}, fixedClock{}, vectors, nil, Config{MaxContextChars: 50})
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	artifacts := []repository.CodeArtifact{
		artifact("handle", repository.ArtifactFunction, "HandlePayment", "internal/payment.HandlePayment", "go", "internal/payment/handler.go", 10, 30),
		artifact("config", repository.ArtifactFunction, "LoadConfig", "internal/config.LoadConfig", "go", "internal/config/load.go", 2, 8),
		artifact("web", repository.ArtifactFunction, "handlePayment", "web.handlePayment", "typescript", "web/payment.ts", 1, 5),
	}
	revision, err := service.Index(context.Background(), scope, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if revision.Status != rag.IndexReady || revision.Stats.Documents != 3 || len(events.events) != 1 || events.events[0].EventType != "index.ready.v1" {
		t.Fatalf("unexpected revision: %#v events=%#v", revision, events.events)
	}
	again, err := service.Index(context.Background(), scope, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if again.RevisionID != revision.RevisionID || vectors.calls != 1 || len(events.events) != 2 || events.events[0].EventID != events.events[1].EventID {
		t.Fatalf("idempotent indexing failed: revision=%#v calls=%d events=%#v", again, vectors.calls, events.events)
	}
	bundle, err := service.Search(context.Background(), rag.RetrievalRequest{Scope: scope, Query: "HandlePayment payment handler", Strategies: []string{"SYMBOL", "BM25", "VECTOR"},
		Filters: rag.Filters{Languages: []string{"GO"}, PathPrefixes: []string{"internal/payment"}}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Hits) != 1 || bundle.Hits[0].ArtifactID != "handle" || len(bundle.ContextBundle.Items) != 1 {
		t.Fatalf("unexpected search result: %#v", bundle)
	}
	if bundle.Hits[0].Scores.Symbol == 0 || bundle.Hits[0].Scores.Keyword == 0 || bundle.Diagnostics.QueryHash == "" || bundle.Diagnostics.QueryHash == "HandlePayment payment handler" {
		t.Fatalf("missing score or diagnostics: %#v", bundle)
	}
	if len(bundle.ArtifactIDs) != 1 || len(bundle.Sources) != 1 || bundle.Sources[0].CommitSHA != "commit" {
		t.Fatalf("compatibility evidence was not derived: %#v", bundle)
	}
}

func TestSearchIsStrictlySnapshotAndTenantScoped(t *testing.T) {
	repositoryAdapter := memory.NewRAGRepository()
	service, _ := New(repositoryAdapter, nil, nil, nil, &sequenceIDs{}, fixedClock{}, nil, nil, Config{})
	scope := common.Scope{TenantID: "tenant-a", RepositoryID: "repo", SnapshotID: "snap"}
	if _, err := service.Index(context.Background(), scope, []repository.CodeArtifact{artifact("a", repository.ArtifactFunction, "Secret", "Secret", "go", "secret.go", 1, 2)}); err != nil {
		t.Fatal(err)
	}
	other := scope
	other.TenantID = "tenant-b"
	_, err := service.Search(context.Background(), rag.RetrievalRequest{Scope: other, Query: "Secret"})
	if !rag.IsCode(err, rag.ErrIndexNotFound) {
		t.Fatalf("expected scoped index miss, got %v", err)
	}
}

func TestIndexRejectsMixedCommitsAndWrapsVectorizerFailures(t *testing.T) {
	service, _ := New(memory.NewRAGRepository(), nil, nil, nil, &sequenceIDs{}, fixedClock{}, nil, nil, Config{})
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	one, two := artifact("a", repository.ArtifactFunction, "A", "A", "go", "a.go", 1, 2), artifact("b", repository.ArtifactFunction, "B", "B", "go", "b.go", 1, 2)
	two.SourceRef.CommitSHA = "other"
	if _, err := service.Index(context.Background(), scope, []repository.CodeArtifact{one, two}); !rag.IsCode(err, rag.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
	failing := &countingVectorizer{err: errors.New("provider unavailable")}
	service, _ = New(memory.NewRAGRepository(), nil, nil, nil, &sequenceIDs{}, fixedClock{}, failing, nil, Config{})
	if _, err := service.Index(context.Background(), scope, []repository.CodeArtifact{one}); !rag.IsCode(err, rag.ErrIndexFailure) {
		t.Fatalf("expected index failure, got %v", err)
	}
}

type graphStub struct {
	result graph.Result
	err    error
}

type invalidReranker struct{}

func (invalidReranker) Version() string { return "invalid@1" }
func (invalidReranker) Rerank(_ context.Context, _ string, hits []rag.Hit) ([]rag.Hit, error) {
	if len(hits) > 0 {
		hits[0].SourceRef.Path = "fabricated.go"
	}
	return hits, nil
}

func (g graphStub) Build(context.Context, graph.BuildCommand) (graph.Revision, error) {
	return graph.Revision{}, errors.New("not implemented")
}
func (g graphStub) Query(context.Context, graph.Query) (graph.Result, error) { return g.result, g.err }

func TestGraphStrategyExpandsLexicalSeedAndDegradesWhenUnavailable(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	artifacts := []repository.CodeArtifact{artifact("handler", repository.ArtifactFunction, "Handle", "api.Handle", "go", "api.go", 1, 3), artifact("service", repository.ArtifactFunction, "Execute", "service.Execute", "go", "service.go", 1, 4)}
	graphResult := graph.Result{Nodes: []graph.Entity{{NodeID: "n1", ArtifactID: "handler"}, {NodeID: "n2", ArtifactID: "service"}}}
	service, _ := New(memory.NewRAGRepository(), graphStub{result: graphResult}, nil, nil, &sequenceIDs{}, fixedClock{}, nil, nil, Config{})
	if _, err := service.Index(context.Background(), scope, artifacts); err != nil {
		t.Fatal(err)
	}
	bundle, err := service.Search(context.Background(), rag.RetrievalRequest{Scope: scope, Query: "Handle", Strategies: []string{"GRAPH"}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Hits) != 2 || bundle.Diagnostics.StrategyHits[rag.StrategyGraph] != 2 {
		t.Fatalf("graph expansion failed: %#v", bundle)
	}

	service.graph = graphStub{err: errors.New("neo4j unavailable")}
	bundle, err = service.Search(context.Background(), rag.RetrievalRequest{Scope: scope, Query: "Handle", Strategies: []string{"SYMBOL", "GRAPH"}, TopK: 5})
	if err != nil || len(bundle.Hits) == 0 || len(bundle.Diagnostics.Warnings) != 1 {
		t.Fatalf("partial graph degradation failed: %#v %v", bundle, err)
	}
}

func TestSearchRejectsEvidenceFabricatedByReranker(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	service, _ := New(memory.NewRAGRepository(), nil, nil, nil, &sequenceIDs{}, fixedClock{}, nil, invalidReranker{}, Config{})
	if _, err := service.Index(context.Background(), scope, []repository.CodeArtifact{artifact("handle", repository.ArtifactFunction, "Handle", "api.Handle", "go", "api.go", 1, 3)}); err != nil {
		t.Fatal(err)
	}
	_, err := service.Search(context.Background(), rag.RetrievalRequest{Scope: scope, Query: "Handle", Strategies: []string{"SYMBOL"}})
	if !rag.IsCode(err, rag.ErrRetrievalFailure) {
		t.Fatalf("expected fabricated evidence to be rejected, got %v", err)
	}
}

func artifact(id string, kind repository.ArtifactKind, name, qualified, language, path string, start, end int) repository.CodeArtifact {
	ref := common.SourceRef{CommitSHA: "commit", Path: path, SymbolID: id, StartLine: start, EndLine: end, ContentHash: "sha256:" + id}
	return repository.CodeArtifact{ArtifactID: id, Kind: kind, Name: name, QualifiedName: qualified, Language: language, SourceRef: ref, Signature: name + "() error", ContentHash: ref.ContentHash}
}
