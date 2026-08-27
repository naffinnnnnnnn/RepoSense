package wikiapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/wiki"
	"github.com/reposense/reposense/internal/ports"
)

type graphStub struct {
	query func(graph.Query) (graph.Result, error)
}

func (g graphStub) Build(context.Context, graph.BuildCommand) (graph.Revision, error) {
	return graph.Revision{}, errors.New("unused")
}
func (g graphStub) Query(_ context.Context, query graph.Query) (graph.Result, error) {
	return g.query(query)
}

type generatorSpy struct {
	calls  int
	result wiki.GenerationResult
	err    error
}

func (g *generatorSpy) Generate(_ context.Context, input ports.WikiGenerationContext) (wiki.GenerationResult, error) {
	g.calls++
	if g.err != nil {
		return wiki.GenerationResult{}, g.err
	}
	if g.result.Pages != nil {
		return g.result, nil
	}
	pages := make([]wiki.PageDraft, 0, len(input.PageSlugs))
	ref := *input.Graph.Nodes[0].SourceRef
	for _, slug := range input.PageSlugs {
		pages = append(pages, wiki.PageDraft{Slug: slug, Title: slug, ContentMarkdown: "# " + slug, Citations: []common.SourceRef{ref}})
	}
	return wiki.GenerationResult{Pages: pages, Model: "fake-model", PromptVersion: "fake-v1", TokenUsage: 42}, nil
}

type eventRecorder struct {
	events []common.EventEnvelope
	err    error
}

func (p *eventRecorder) Publish(_ context.Context, event common.EventEnvelope) error {
	p.events = append(p.events, event)
	return p.err
}

type sequenceIDs struct{ next int }

func (s *sequenceIDs) New(prefix string) string {
	s.next++
	return fmt.Sprintf("%s_%d", prefix, s.next)
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC) }

func TestServiceGenerateGetPageAndIdempotency(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	repo := memory.NewWikiRepository()
	generator := &generatorSpy{}
	events := &eventRecorder{}
	service, err := New(graphStub{query: func(graph.Query) (graph.Result, error) { return sampleGraph("gr", "sha"), nil }}, nil, repo, generator, events, nil, &sequenceIDs{}, fixedClock{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	cmd := wiki.GenerateCommand{Scope: scope, GraphRevisionID: "gr", Locale: "zh-CN", IdempotencyKey: "wiki:snap"}
	job, err := service.Generate(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != wiki.JobSucceeded || len(job.PageIDs) != 6 || generator.calls != 1 {
		t.Fatalf("unexpected job: %#v calls=%d", job, generator.calls)
	}
	page, err := service.GetPage(context.Background(), scope, "architecture")
	if err != nil {
		t.Fatal(err)
	}
	if page.RevisionNo != 1 || page.GraphRevisionID != "gr" || page.Model != "fake-model" || len(page.Citations) != 1 || page.ContentHash == "" {
		t.Fatalf("unexpected page: %#v", page)
	}
	cached, err := service.Generate(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if cached.JobID != job.JobID || generator.calls != 1 || len(events.events) != 2 {
		t.Fatalf("idempotency failed: %#v calls=%d events=%d", cached, generator.calls, len(events.events))
	}
	if events.events[0].EventType != "wiki.published.v1" || events.events[0].Payload["page_count"] != 6 {
		t.Fatalf("unexpected event: %#v", events.events[0])
	}
}

func TestServiceRejectsWrongGraphRevisionAndUngroundedOutput(t *testing.T) {
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	baseGraph := graphStub{query: func(graph.Query) (graph.Result, error) { return sampleGraph("actual", "sha"), nil }}
	service, _ := New(baseGraph, nil, memory.NewWikiRepository(), nil, nil, nil, nil, nil, Config{})
	_, err := service.Generate(context.Background(), wiki.GenerateCommand{Scope: scope, GraphRevisionID: "requested", IdempotencyKey: "key"})
	if !wiki.IsCode(err, wiki.ErrGraphNotReady) {
		t.Fatalf("expected graph-not-ready, got %v", err)
	}

	bad := &generatorSpy{result: wiki.GenerationResult{Model: "fake", PromptVersion: "v1", Pages: []wiki.PageDraft{{Slug: "overview", Title: "bad", ContentMarkdown: "bad", Citations: []common.SourceRef{{CommitSHA: "other", Path: "x.go", StartLine: 1, EndLine: 1}}}}}}
	service, _ = New(baseGraph, nil, memory.NewWikiRepository(), bad, nil, nil, nil, nil, Config{})
	_, err = service.Generate(context.Background(), wiki.GenerateCommand{Scope: scope, GraphRevisionID: "actual", PageScope: []string{"overview"}, IdempotencyKey: "key"})
	if !wiki.IsCode(err, wiki.ErrGenerationFailure) {
		t.Fatalf("expected generation failure, got %v", err)
	}
}

func TestServiceRejectsTruncatedKnowledgeGraph(t *testing.T) {
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	service, _ := New(graphStub{query: func(graph.Query) (graph.Result, error) {
		result := sampleGraph("gr", "sha")
		result.Diagnostics.Truncated = true
		return result, nil
	}}, nil, memory.NewWikiRepository(), nil, nil, nil, nil, nil, Config{})
	_, err := service.Generate(context.Background(), wiki.GenerateCommand{Scope: scope, GraphRevisionID: "gr", IdempotencyKey: "key"})
	if !wiki.IsCode(err, wiki.ErrInsufficientEvidence) {
		t.Fatalf("expected insufficient evidence, got %v", err)
	}
}

func TestServiceCreatesStableIncrementalPageRevisions(t *testing.T) {
	repo := memory.NewWikiRepository()
	store := graphStub{query: func(query graph.Query) (graph.Result, error) {
		return sampleGraph("gr-"+query.Scope.SnapshotID, "sha-"+query.Scope.SnapshotID), nil
	}}
	service, _ := New(store, nil, repo, nil, nil, nil, &sequenceIDs{}, fixedClock{}, Config{})
	scope1 := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s1"}
	if _, err := service.Generate(context.Background(), wiki.GenerateCommand{Scope: scope1, GraphRevisionID: "gr-s1", PageScope: []string{"overview"}, IdempotencyKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	first, _ := service.GetPage(context.Background(), scope1, "overview")
	scope2 := scope1
	scope2.SnapshotID = "s2"
	if _, err := service.Generate(context.Background(), wiki.GenerateCommand{Scope: scope2, GraphRevisionID: "gr-s2", PageScope: []string{"overview"}, IdempotencyKey: "k2"}); err != nil {
		t.Fatal(err)
	}
	second, _ := service.GetPage(context.Background(), scope2, "overview")
	if first.PageID != second.PageID || first.RevisionNo != 1 || second.RevisionNo != 2 || first.ContentHash == second.ContentHash {
		t.Fatalf("unexpected revisions: first=%#v second=%#v", first, second)
	}
	if _, err := service.GetPage(context.Background(), common.Scope{TenantID: "other", RepositoryID: "r", SnapshotID: "s2"}, "overview"); !wiki.IsCode(err, wiki.ErrPageNotFound) {
		t.Fatalf("tenant isolation failed: %v", err)
	}
}

func TestServiceRecoversFromEventPublicationFailure(t *testing.T) {
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	repo := memory.NewWikiRepository()
	generator := &generatorSpy{}
	events := &eventRecorder{err: errors.New("broker unavailable")}
	service, _ := New(graphStub{query: func(graph.Query) (graph.Result, error) { return sampleGraph("gr", "sha"), nil }}, nil, repo, generator, events, nil, &sequenceIDs{}, fixedClock{}, Config{})
	cmd := wiki.GenerateCommand{Scope: scope, GraphRevisionID: "gr", PageScope: []string{"interfaces"}, IdempotencyKey: "key"}
	first, err := service.Generate(context.Background(), cmd)
	if !wiki.IsCode(err, wiki.ErrPersistence) || first.JobID == "" {
		t.Fatalf("expected saved job and publication error, got job=%#v err=%v", first, err)
	}
	events.err = nil
	retried, err := service.Generate(context.Background(), cmd)
	if err != nil || retried.JobID != first.JobID || generator.calls != 1 {
		t.Fatalf("retry did not replay stored publication: job=%#v err=%v calls=%d", retried, err, generator.calls)
	}
	page, err := service.GetPage(context.Background(), scope, "interfaces")
	if err != nil || page.ParentPageID != "" {
		t.Fatalf("first partial publication should remain readable without a dangling parent: %#v err=%v", page, err)
	}
}

func TestRepositoryGraphWikiIntegration(t *testing.T) {
	ctx := context.Background()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	commit := "abc123"
	mainRef := common.SourceRef{CommitSHA: commit, Path: "cmd/api/main.go", SymbolID: "main", StartLine: 1, EndLine: 10, ContentHash: "sha256:main"}
	helperRef := common.SourceRef{CommitSHA: commit, Path: "internal/app.go", SymbolID: "run", StartLine: 5, EndLine: 20, ContentHash: "sha256:run"}
	parseStore := memory.NewRepositoryStore()
	parsed := repository.ParseResult{Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusSucceeded)}, SnapshotID: scope.SnapshotID, CommitSHA: commit, SyncStatus: repository.StatusSucceeded}, Job: repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusSucceeded)}, JobID: "parse-job", SnapshotID: scope.SnapshotID, Status: repository.StatusSucceeded, Progress: 100},
		Artifacts: []repository.CodeArtifact{
			{ArtifactID: "main", Kind: repository.ArtifactFunction, Name: "main", QualifiedName: "cmd.api.main", Language: "go", SourceRef: mainRef, ContentHash: mainRef.ContentHash, Attributes: map[string]string{"language": "go"}},
			{ArtifactID: "run", Kind: repository.ArtifactFunction, Name: "run", QualifiedName: "internal.run", Language: "go", SourceRef: helperRef, ContentHash: helperRef.ContentHash, Attributes: map[string]string{"language": "go"}},
		}, Relations: []repository.CodeRelation{{RelationID: "calls", Kind: repository.RelationCalls, From: "main", To: "run", Evidence: mainRef, Confidence: 1}}}
	if err := parseStore.SaveResult(ctx, "parse", parsed); err != nil {
		t.Fatal(err)
	}
	graphRepo := memory.NewGraphRepository()
	graphService, err := graphapp.New(parseStore, graphRepo, nil, nil, &sequenceIDs{}, fixedClock{}, nil, graphapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := graphService.Build(ctx, graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph"})
	if err != nil {
		t.Fatal(err)
	}
	wikiService, err := New(graphService, nil, memory.NewWikiRepository(), nil, nil, nil, &sequenceIDs{}, fixedClock{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := wikiService.Generate(ctx, wiki.GenerateCommand{Scope: scope, GraphRevisionID: revision.RevisionID, IdempotencyKey: "wiki"})
	if err != nil {
		t.Fatal(err)
	}
	if len(job.PageIDs) != 6 {
		t.Fatalf("expected complete wiki, got %#v", job)
	}
	flow, err := wikiService.GetPage(ctx, scope, "key-flows")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flow.ContentMarkdown, "cmd.api.main") || !strings.Contains(flow.ContentMarkdown, "internal.run") || len(flow.Citations) < 2 {
		t.Fatalf("graph relationship was not integrated: %s", flow.ContentMarkdown)
	}
}
