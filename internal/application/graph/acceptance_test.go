package graphapp

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

// These tests encode the accepted behavior of the Code Knowledge Graph. Some
// intentionally fail until the production behavior described by the acceptance
// specification is implemented.

type acceptanceSource struct {
	input graph.BuildInput
	err   error
}

func (s acceptanceSource) GraphInput(context.Context, common.Scope) (graph.BuildInput, error) {
	return s.input, s.err
}

type barrierSource struct {
	input    graph.BuildInput
	expected int64
	arrived  atomic.Int64
	ready    chan struct{}
	once     sync.Once
}

func newBarrierSource(input graph.BuildInput, expected int64) *barrierSource {
	return &barrierSource{input: input, expected: expected, ready: make(chan struct{})}
}

func (s *barrierSource) GraphInput(ctx context.Context, _ common.Scope) (graph.BuildInput, error) {
	if s.arrived.Add(1) == s.expected {
		s.once.Do(func() { close(s.ready) })
	}
	select {
	case <-ctx.Done():
		return graph.BuildInput{}, ctx.Err()
	case <-s.ready:
		return s.input, nil
	}
}

type atomicIDs struct{ next atomic.Int64 }

func (g *atomicIDs) New(prefix string) string {
	return prefix + "-" + stringID(g.next.Add(1))
}

func stringID(value int64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = digits[value%10]
		value /= 10
	}
	return string(buf[i:])
}

type scopedEventSink struct {
	mu     sync.Mutex
	scopes []common.Scope
	events []common.EventEnvelope
}

func (s *scopedEventSink) Publish(ctx context.Context, event common.EventEnvelope) error {
	scope, _ := repository.EventScopeFromContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopes = append(s.scopes, scope)
	s.events = append(s.events, event)
	return nil
}

func TestAcceptanceBuildRejectsGraphInputFromAnotherScope(t *testing.T) {
	requested := common.Scope{TenantID: "tenant-a", RepositoryID: "repo-a", SnapshotID: "snapshot-a", TraceID: "trace"}
	returned := common.Scope{TenantID: "tenant-b", RepositoryID: "repo-a", SnapshotID: "snapshot-a", TraceID: "trace"}
	input := validBuildInput(returned, "commit-a")
	service := newAcceptanceService(t, acceptanceSource{input: input}, memory.NewGraphRepository(), nil)

	_, err := service.Build(context.Background(), graph.BuildCommand{Scope: requested, Mode: graph.BuildFull, IdempotencyKey: "scope"})
	assertGraphCode(t, err, graph.ErrInvalidInput)
	assertNoPublishedRevision(t, service, requested)
}

func TestAcceptanceFullBuildAllowsEmptySucceededSnapshot(t *testing.T) {
	scope := acceptanceScope("empty")
	input := graph.BuildInput{Snapshot: snapshot(scope, "", "commit-empty", nil)}
	service := newAcceptanceService(t, acceptanceSource{input: input}, memory.NewGraphRepository(), nil)

	revision, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if revision.BuildStatus != graph.RevisionActive || revision.Stats.Nodes != 0 || revision.Stats.Edges != 0 {
		t.Fatalf("empty succeeded snapshot must publish an empty active graph: %#v", revision)
	}
}
func TestAcceptanceBuildRejectsSourceReferencesFromAnotherCommit(t *testing.T) {
	scope := acceptanceScope("commit-mismatch")
	tests := map[string]func(*graph.BuildInput){
		"artifact source":   func(input *graph.BuildInput) { input.Artifacts[0].SourceRef.CommitSHA = "other-commit" },
		"relation evidence": func(input *graph.BuildInput) { input.Relations[0].Evidence.CommitSHA = "other-commit" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			input := validBuildInput(scope, "commit-a")
			mutate(&input)
			repo := memory.NewGraphRepository()
			service := newAcceptanceService(t, acceptanceSource{input: input}, repo, nil)
			_, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: name})
			assertGraphCode(t, err, graph.ErrBuildFailure)
		})
	}
}

func TestAcceptanceBuildRejectsMalformedArtifactsAndRelations(t *testing.T) {
	tests := map[string]func(*graph.BuildInput){
		"duplicate artifact id": func(input *graph.BuildInput) {
			duplicate := input.Artifacts[0]
			duplicate.Name = "duplicate"
			input.Artifacts = append(input.Artifacts, duplicate)
		},
		"unknown artifact kind": func(input *graph.BuildInput) {
			input.Artifacts[0].Kind = repository.ArtifactKind("UNKNOWN")
		},
		"duplicate relation id": func(input *graph.BuildInput) {
			duplicate := input.Relations[0]
			duplicate.To = "symbol:another"
			input.Relations = append(input.Relations, duplicate)
		},
		"empty relation target": func(input *graph.BuildInput) { input.Relations[0].To = "" },
		"unknown relation kind": func(input *graph.BuildInput) {
			input.Relations[0].Kind = repository.RelationKind("UNKNOWN")
		},
		"nan confidence": func(input *graph.BuildInput) { input.Relations[0].Confidence = math.NaN() },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			scope := acceptanceScope("invalid-" + name)
			input := validBuildInput(scope, "commit-a")
			mutate(&input)
			repo := memory.NewGraphRepository()
			service := newAcceptanceService(t, acceptanceSource{input: input}, repo, nil)
			_, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: name})
			assertGraphCode(t, err, graph.ErrBuildFailure)
			assertNoPublishedRevision(t, service, scope)
		})
	}
}

func TestAcceptanceIncrementalBuildRejectsArtifactSelection(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, err := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	parentScope := acceptanceScope("parent")
	parentInput := validBuildInput(parentScope, "commit-parent")
	storeParseResult(t, repositories, "parse-parent", parentInput)
	if _, err := service.Build(context.Background(), graph.BuildCommand{Scope: parentScope, Mode: graph.BuildFull, IdempotencyKey: "parent"}); err != nil {
		t.Fatal(err)
	}

	childScope := acceptanceScope("child")
	childInput := validBuildInput(childScope, "commit-child")
	childInput.Snapshot.ParentSnapshotID = parentScope.SnapshotID
	childInput.Snapshot.ChangedPaths = []repository.ChangedPath{{Path: childInput.Artifacts[0].SourceRef.Path, Kind: repository.ChangeModified}}
	storeParseResult(t, repositories, "parse-child", childInput)

	_, err = service.Build(context.Background(), graph.BuildCommand{Scope: childScope, Mode: graph.BuildIncremental, ArtifactIDs: []string{childInput.Artifacts[0].ArtifactID}, IdempotencyKey: "child"})
	assertGraphCode(t, err, graph.ErrInvalidInput)
}

func TestAcceptanceIdempotencyKeyCannotBeReusedAcrossSnapshots(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, err := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	firstScope := acceptanceScope("first")
	secondScope := acceptanceScope("second")
	storeParseResult(t, repositories, "parse-first", validBuildInput(firstScope, "commit-first"))
	storeParseResult(t, repositories, "parse-second", validBuildInput(secondScope, "commit-second"))
	if _, err := service.Build(context.Background(), graph.BuildCommand{Scope: firstScope, Mode: graph.BuildFull, IdempotencyKey: "shared-key"}); err != nil {
		t.Fatal(err)
	}

	_, err = service.Build(context.Background(), graph.BuildCommand{Scope: secondScope, Mode: graph.BuildFull, IdempotencyKey: "shared-key"})
	assertGraphCode(t, err, graph.ErrConflict)
}

func TestAcceptanceIdempotencyKeyCannotBeReusedForDifferentBuildRequest(t *testing.T) {
	scope := acceptanceScope("fingerprint")
	input := validBuildInput(scope, "commit-a")
	service := newAcceptanceService(t, acceptanceSource{input: input}, memory.NewGraphRepository(), nil)
	if _, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "shared-key"}); err != nil {
		t.Fatal(err)
	}

	_, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, ArtifactIDs: []string{input.Artifacts[0].ArtifactID}, IdempotencyKey: "shared-key"})
	assertGraphCode(t, err, graph.ErrConflict)
}
func TestAcceptanceConcurrentIdempotentBuildReturnsOnlyPersistedWinner(t *testing.T) {
	scope := acceptanceScope("concurrent")
	input := validBuildInput(scope, "commit-a")
	repo := memory.NewGraphRepository()
	events := &scopedEventSink{}
	const calls = 32
	service, err := New(newBarrierSource(input, calls), repo, events, nil, &atomicIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	revisions := make(chan graph.Revision, calls)
	errorsCh := make(chan error, calls)
	var group sync.WaitGroup
	for i := 0; i < calls; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			revision, buildErr := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "same-key"})
			revisions <- revision
			errorsCh <- buildErr
		}()
	}
	close(start)
	group.Wait()
	close(revisions)
	close(errorsCh)

	for buildErr := range errorsCh {
		if buildErr != nil {
			t.Fatalf("identical concurrent request failed: %v", buildErr)
		}
	}
	persisted, err := repo.RevisionBySnapshot(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for revision := range revisions {
		if revision.RevisionID != persisted.RevisionID || revision.PublishedEvent.EventID != persisted.PublishedEvent.EventID {
			t.Fatalf("request returned non-persisted revision: got=%s/%s persisted=%s/%s", revision.RevisionID, revision.PublishedEvent.EventID, persisted.RevisionID, persisted.PublishedEvent.EventID)
		}
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	for _, event := range events.events {
		if event.EventID != persisted.PublishedEvent.EventID {
			t.Fatalf("published non-canonical event %q; canonical=%q", event.EventID, persisted.PublishedEvent.EventID)
		}
	}
}

func TestAcceptanceGraphEventCarriesTrustedScope(t *testing.T) {
	scope := acceptanceScope("event-scope")
	events := &scopedEventSink{}
	service := newAcceptanceService(t, acceptanceSource{input: validBuildInput(scope, "commit-a")}, memory.NewGraphRepository(), events)
	if _, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "event"}); err != nil {
		t.Fatal(err)
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	if len(events.scopes) != 1 || events.scopes[0] != scope {
		t.Fatalf("publisher did not receive trusted graph scope: got=%#v want=%#v", events.scopes, scope)
	}
}

func TestAcceptanceGraphSourceInfrastructureFailureRemainsRetryable(t *testing.T) {
	scope := acceptanceScope("source-error")
	cause := &graph.DomainError{Code: graph.ErrPersistence, Operation: "load_graph_input", Message: "storage unavailable", Retryable: true}
	service := newAcceptanceService(t, acceptanceSource{err: cause}, memory.NewGraphRepository(), nil)

	_, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "source"})
	assertGraphCode(t, err, graph.ErrPersistence)
	var domainErr *graph.DomainError
	if !errors.As(err, &domainErr) || !domainErr.Retryable {
		t.Fatalf("infrastructure failure must remain retryable: %v", err)
	}
}

func newAcceptanceService(t *testing.T, source acceptanceSource, repo *memory.GraphRepository, events *scopedEventSink) *Service {
	t.Helper()
	var service *Service
	var err error
	if events == nil {
		service, err = New(source, repo, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	} else {
		service, err = New(source, repo, events, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	}
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func acceptanceScope(snapshotID string) common.Scope {
	return common.Scope{TenantID: "tenant-a", RepositoryID: "repo-a", SnapshotID: snapshotID, TraceID: "trace-a"}
}

func validBuildInput(scope common.Scope, commit string) graph.BuildInput {
	source := artifact("source", repository.ArtifactFunction, "source", "pkg.source", commit, 1, 2)
	target := artifact("target", repository.ArtifactFunction, "target", "pkg.target", commit, 4, 5)
	source.SourceRef.Path = "pkg/source.go"
	target.SourceRef.Path = "pkg/target.go"
	return graph.BuildInput{
		Snapshot:  snapshot(scope, "", commit, []repository.ChangedPath{{Path: "pkg/source.go", Kind: repository.ChangeAdded}, {Path: "pkg/target.go", Kind: repository.ChangeAdded}}),
		Artifacts: []repository.CodeArtifact{source, target},
		Relations: []repository.CodeRelation{relation("calls", repository.RelationCalls, source.ArtifactID, target.ArtifactID, commit, "pkg/source.go", 2, 1)},
	}
}

func storeParseResult(t *testing.T, store *memory.RepositoryStore, key string, input graph.BuildInput) {
	t.Helper()
	result := repository.ParseResult{Snapshot: input.Snapshot, Artifacts: input.Artifacts, Relations: input.Relations, DeletedPaths: input.DeletedPaths}
	completeResult(&result)
	if err := store.SaveResult(context.Background(), key, result); err != nil {
		t.Fatal(err)
	}
}

func assertGraphCode(t *testing.T, err error, code graph.ErrorCode) {
	t.Helper()
	if !graph.IsCode(err, code) {
		t.Fatalf("expected graph error %s, got %v", code, err)
	}
}

func assertNoPublishedRevision(t *testing.T, service *Service, scope common.Scope) {
	t.Helper()
	_, err := service.Query(context.Background(), graph.Query{Scope: scope, Limit: 1})
	if !graph.IsCode(err, graph.ErrRevisionNotFound) {
		t.Fatalf("failed build published a revision: %v", err)
	}
}
