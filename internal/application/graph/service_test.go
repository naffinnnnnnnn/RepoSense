package graphapp

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type sequenceIDs struct{ n int }

func (i *sequenceIDs) New(prefix string) string { i.n++; return prefix + string(rune('0'+i.n)) }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 8, 12, 6, 0, 0, 0, time.UTC) }

type eventSink struct{ events []common.EventEnvelope }

func (s *eventSink) Publish(_ context.Context, e common.EventEnvelope) error {
	s.events = append(s.events, e)
	return nil
}

func TestServiceBuildsAndQueriesParserResolvedGraph(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, err := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s1", TraceID: "trace"}
	commit := "aaaaaaaa"
	file := artifact("file", repository.ArtifactFile, "service.py", "service.py", commit, 1, 10)
	caller := artifact("caller", repository.ArtifactFunction, "login", "service.login", commit, 2, 4)
	callee := artifact("callee", repository.ArtifactFunction, "issue", "tokens.issue", commit, 6, 8)
	parsed := repository.ParseResult{
		Snapshot:  snapshot(scope, "", commit, []repository.ChangedPath{{Path: "service.py", Kind: repository.ChangeAdded}, {Path: "tokens.py", Kind: repository.ChangeAdded}}),
		Artifacts: []repository.CodeArtifact{file, caller, callee},
		Relations: []repository.CodeRelation{
			relation("contains", repository.RelationContains, file.ArtifactID, caller.ArtifactID, commit, "service.py", 2, 1),
			relation("calls", repository.RelationCalls, caller.ArtifactID, callee.ArtifactID, commit, "service.py", 3, .8),
		},
	}
	completeResult(&parsed)
	if err := repositories.SaveResult(context.Background(), "parse1", parsed); err != nil {
		t.Fatal(err)
	}

	revision, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph1"})
	if err != nil {
		t.Fatal(err)
	}
	if revision.Stats.Nodes != 3 || revision.Stats.Edges != 2 || revision.Stats.UnresolvedTargets != 0 {
		t.Fatalf("unexpected stats: %#v", revision.Stats)
	}
	result, err := service.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{caller.ArtifactID}, Depth: 1, Direction: graph.DirectionOutgoing, RelationTypes: []repository.RelationKind{repository.RelationCalls}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 2 || len(result.Edges) != 1 {
		t.Fatalf("unexpected call graph: %#v", result)
	}
}

func TestFullBuildDoesNotDependOnParentGraphRevision(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, err := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "child", TraceID: "trace"}
	input := validBuildInput(scope, "child-commit")
	input.Snapshot.ParentSnapshotID = "missing-parent"
	storeParseResult(t, repositories, "parse-child", input)

	revision, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "child-full"})
	if err != nil {
		t.Fatalf("FULL build must not require a parent graph revision: %v", err)
	}
	if revision.ParentRevisionID != "" || revision.SnapshotID != scope.SnapshotID {
		t.Fatalf("FULL revision unexpectedly depends on parent: %#v", revision)
	}
}

func TestServiceRejectsFailedParseResultBeforeGraphBuild(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	events := &eventSink{}
	service, err := New(repositories, graphs, events, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "failed-snapshot", TraceID: "trace"}
	parsed := repository.ParseResult{
		Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusFailed)}, SnapshotID: scope.SnapshotID, CommitSHA: "sha", SyncStatus: repository.StatusFailed, ErrorCode: string(repository.ErrParseFailure), ErrorMessage: "parse failed"},
		Job:      repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusFailed)}, JobID: "job", SnapshotID: scope.SnapshotID, Status: repository.StatusFailed, ErrorCode: string(repository.ErrParseFailure), ErrorMessage: "parse failed"},
	}
	if err := repositories.SaveResult(context.Background(), "failed", parsed); err != nil {
		t.Fatal(err)
	}
	_, buildErr := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph-failed"})
	if !graph.IsCode(buildErr, graph.ErrInvalidInput) {
		t.Fatalf("Graph must reject a failed ParseResult: %v", buildErr)
	}
	if len(events.events) != 0 {
		t.Fatalf("failed ParseResult published graph event: %#v", events.events)
	}
}

func artifact(id string, kind repository.ArtifactKind, name, qualified, commit string, start, end int) repository.CodeArtifact {
	return repository.CodeArtifact{ArtifactID: id, Kind: kind, Name: name, QualifiedName: qualified, Language: "python", SourceRef: common.SourceRef{CommitSHA: commit, Path: name + ".py", SymbolID: id, StartLine: start, EndLine: end, ContentHash: "sha256:x"}, ContentHash: "sha256:x"}
}

func relation(id string, kind repository.RelationKind, from, to, commit, path string, line int, confidence float64) repository.CodeRelation {
	return repository.CodeRelation{RelationID: id, Kind: kind, From: from, To: to, Evidence: common.SourceRef{CommitSHA: commit, Path: path, StartLine: line, EndLine: line, ContentHash: "sha256:e"}, Confidence: confidence}
}

func snapshot(scope common.Scope, parent, commit string, changes []repository.ChangedPath) repository.Snapshot {
	return repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusSucceeded)}, SnapshotID: scope.SnapshotID, ParentSnapshotID: parent, CommitSHA: commit, SyncStatus: repository.StatusSucceeded, ChangedPaths: changes}
}

func completeResult(result *repository.ParseResult) {
	result.Job = repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: result.Snapshot.TenantID, RepositoryID: result.Snapshot.RepositoryID, Status: string(repository.StatusSucceeded)}, JobID: result.Snapshot.SnapshotID + "-job", SnapshotID: result.Snapshot.SnapshotID, Status: repository.StatusSucceeded, Progress: 100}
}
