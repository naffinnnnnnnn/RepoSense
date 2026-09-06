package graphapp

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
)

func TestIntegrationFullSnapshotsRemainIndependent(t *testing.T) {
	repositories := memory.NewRepositoryStore()
	graphs := memory.NewGraphRepository()
	service, err := New(repositories, graphs, nil, nil, &sequenceIDs{}, fixedClock{}, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}

	firstScope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot-1", TraceID: "trace-1"}
	secondScope := firstScope
	secondScope.SnapshotID = "snapshot-2"
	secondScope.TraceID = "trace-2"
	first := validBuildInput(firstScope, "commit-1")
	second := validBuildInput(secondScope, "commit-2")
	second.Snapshot.ParentSnapshotID = firstScope.SnapshotID
	second.Artifacts[0].Name = "source-v2"
	storeParseResult(t, repositories, "parse-1", first)
	storeParseResult(t, repositories, "parse-2", second)

	firstRevision, err := service.Build(context.Background(), graph.BuildCommand{Scope: firstScope, Mode: graph.BuildFull, IdempotencyKey: "graph-1"})
	if err != nil {
		t.Fatal(err)
	}
	secondRevision, err := service.Build(context.Background(), graph.BuildCommand{Scope: secondScope, Mode: graph.BuildFull, IdempotencyKey: "graph-2"})
	if err != nil {
		t.Fatal(err)
	}
	if secondRevision.ParentRevisionID != "" {
		t.Fatalf("FULL revision depends on parent graph: %#v", secondRevision)
	}

	firstAgain, err := graphs.RevisionBySnapshot(context.Background(), firstScope)
	if err != nil {
		t.Fatal(err)
	}
	if firstAgain.RevisionID != firstRevision.RevisionID || firstAgain.Nodes[0].Name == "source-v2" {
		t.Fatalf("second FULL build mutated first revision: first=%#v second=%#v", firstAgain, secondRevision)
	}
}
