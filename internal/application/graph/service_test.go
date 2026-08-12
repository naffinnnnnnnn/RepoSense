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

func TestServiceBuildsAndQueriesResolvedGraph(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	events := &eventSink{}
	service, err := New(repositories, graphs, events, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s1", TraceID: "trace"}
	commit := "aaaaaaaa"
	file := artifact("file", repository.ArtifactFile, "service.py", "service.py", commit, 1, 10)
	caller := artifact("caller", repository.ArtifactFunction, "login", "service.login", commit, 2, 4)
	callee := artifact("callee", repository.ArtifactFunction, "issue", "tokens.issue", commit, 6, 8)
	parsed := repository.ParseResult{Snapshot: snapshot(scope, "", commit, []repository.ChangedPath{{Path: "service.py", Kind: repository.ChangeAdded}, {Path: "tokens.py", Kind: repository.ChangeAdded}}), Artifacts: []repository.CodeArtifact{file, caller, callee}, Relations: []repository.CodeRelation{
		relation("contains", repository.RelationContains, file.ArtifactID, caller.ArtifactID, commit, "service.py", 2, 1), relation("calls", repository.RelationCalls, caller.ArtifactID, "symbol:issue", commit, "service.py", 3, .8)}}
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
	if len(events.events) != 1 || events.events[0].EventType != "graph.published.v1" {
		t.Fatalf("missing publication event: %#v", events.events)
	}
	result, err := service.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{caller.ArtifactID}, Depth: 1, Direction: graph.DirectionOutgoing, RelationTypes: []repository.RelationKind{repository.RelationCalls}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 2 || len(result.Edges) != 1 {
		t.Fatalf("unexpected call graph: %#v", result)
	}
	cached, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph1"})
	if err != nil || cached.RevisionID != revision.RevisionID {
		t.Fatalf("idempotency failed: %#v %v", cached, err)
	}
	if len(events.events) != 2 || events.events[0].EventID != events.events[1].EventID {
		t.Fatalf("idempotent retry must republish the same event identity: %#v", events.events)
	}
}

func TestIncrementalBuildRemovesChangedAndDeletedArtifactsWithoutMutatingParent(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, _ := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	baseScope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "base"}
	oldCommit := "old"
	oldA := artifact("stable-a", repository.ArtifactFunction, "a", "a", oldCommit, 1, 2)
	oldB := artifact("stable-b", repository.ArtifactFunction, "b", "b", oldCommit, 1, 2)
	oldC := artifact("stable-c", repository.ArtifactFunction, "c", "helpers.c", oldCommit, 1, 2)
	base := repository.ParseResult{Snapshot: snapshot(baseScope, "", oldCommit, []repository.ChangedPath{{Path: "a.py", Kind: repository.ChangeAdded}, {Path: "b.py", Kind: repository.ChangeAdded}}), Artifacts: []repository.CodeArtifact{oldA, oldB}, Relations: []repository.CodeRelation{relation("oldcall", repository.RelationCalls, oldA.ArtifactID, oldB.ArtifactID, oldCommit, "a.py", 1, 1), relation("stablecall", repository.RelationCalls, oldC.ArtifactID, oldA.ArtifactID, oldCommit, "helpers.py", 1, 1)}}
	oldA.SourceRef.Path = "a.py"
	oldB.SourceRef.Path = "b.py"
	oldC.SourceRef.Path = "helpers.py"
	base.Artifacts = []repository.CodeArtifact{oldA, oldB, oldC}
	if err := repositories.SaveResult(context.Background(), "p1", base); err != nil {
		t.Fatal(err)
	}
	parent, err := service.Build(context.Background(), graph.BuildCommand{Scope: baseScope, Mode: graph.BuildFull, IdempotencyKey: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	childScope := baseScope
	childScope.SnapshotID = "child"
	newCommit := "new"
	newA := artifact("stable-a", repository.ArtifactFunction, "a", "a", newCommit, 1, 3)
	newA.SourceRef.Path = "a.py"
	child := repository.ParseResult{Snapshot: snapshot(childScope, "base", newCommit, []repository.ChangedPath{{Path: "a.py", Kind: repository.ChangeModified}, {Path: "b.py", Kind: repository.ChangeDeleted}}), Artifacts: []repository.CodeArtifact{newA}, Relations: []repository.CodeRelation{relation("newcall", repository.RelationCalls, newA.ArtifactID, "symbol:c", newCommit, "a.py", 2, .8)}, DeletedPaths: []string{"b.py"}}
	if err := repositories.SaveResult(context.Background(), "p2", child); err != nil {
		t.Fatal(err)
	}
	got, err := service.Build(context.Background(), graph.BuildCommand{Scope: childScope, Mode: graph.BuildIncremental, IdempotencyKey: "g2"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentRevisionID != parent.RevisionID || got.Stats.Nodes != 2 || got.Stats.Edges != 2 || got.Stats.UnresolvedTargets != 0 {
		t.Fatalf("bad incremental revision: %#v", got)
	}
	parentAgain, err := graphs.RevisionBySnapshot(context.Background(), baseScope)
	if err != nil {
		t.Fatal(err)
	}
	if len(parentAgain.Nodes) != 3 || len(parentAgain.Edges) != 2 {
		t.Fatal("parent revision was mutated")
	}
}

func TestAmbiguousSymbolStaysUnresolved(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, _ := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	commit := "sha"
	caller := artifact("c", repository.ArtifactFunction, "call", "call", commit, 1, 2)
	one := artifact("1", repository.ArtifactFunction, "same", "x.same", commit, 1, 2)
	two := artifact("2", repository.ArtifactFunction, "same", "y.same", commit, 1, 2)
	one.SourceRef.Path = "x.py"
	two.SourceRef.Path = "y.py"
	input := repository.ParseResult{Snapshot: snapshot(scope, "", commit, nil), Artifacts: []repository.CodeArtifact{caller, one, two}, Relations: []repository.CodeRelation{relation("r", repository.RelationCalls, "c", "symbol:same", commit, "main.py", 1, .5)}}
	if err := repositories.SaveResult(context.Background(), "p", input); err != nil {
		t.Fatal(err)
	}
	revision, err := service.Build(context.Background(), graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if revision.Stats.AmbiguousRelations != 1 || revision.Stats.UnresolvedTargets != 1 || revision.Stats.Nodes != 4 {
		t.Fatalf("ambiguity was not preserved: %#v", revision.Stats)
	}
}

func artifact(id string, kind repository.ArtifactKind, name, qualified, commit string, start, end int) repository.CodeArtifact {
	return repository.CodeArtifact{ArtifactID: id, Kind: kind, Name: name, QualifiedName: qualified, Language: "python", SourceRef: common.SourceRef{CommitSHA: commit, Path: name + ".py", SymbolID: id, StartLine: start, EndLine: end, ContentHash: "sha256:x"}, ContentHash: "sha256:x"}
}
func relation(id string, kind repository.RelationKind, from, to, commit, path string, line int, confidence float64) repository.CodeRelation {
	return repository.CodeRelation{RelationID: id, Kind: kind, From: from, To: to, Evidence: common.SourceRef{CommitSHA: commit, Path: path, StartLine: line, EndLine: line, ContentHash: "sha256:e"}, Confidence: confidence}
}
func snapshot(scope common.Scope, parent, commit string, changes []repository.ChangedPath) repository.Snapshot {
	return repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID}, SnapshotID: scope.SnapshotID, ParentSnapshotID: parent, CommitSHA: commit, SyncStatus: repository.StatusSucceeded, ChangedPaths: changes}
}
