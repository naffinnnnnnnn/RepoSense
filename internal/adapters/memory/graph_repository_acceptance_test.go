package memory

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestAcceptanceQueryRejectsMoreRootsThanLimit(t *testing.T) {
	store, scope := acceptanceGraph(t)
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1", "n2"}, Depth: 0, Limit: 1})
	assertMemoryGraphCode(t, err, graph.ErrInvalidInput)
}

func TestAcceptanceQueryRejectsAnyMissingRoot(t *testing.T) {
	store, scope := acceptanceGraph(t)
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1", "missing"}, Depth: 1, Limit: 10})
	assertMemoryGraphCode(t, err, graph.ErrInvalidInput)
}

func TestAcceptanceQueryRejectsUnknownFilters(t *testing.T) {
	store, scope := acceptanceGraph(t)
	tests := map[string]graph.Query{
		"entity type":   {Scope: scope, EntityTypes: []graph.EntityType{"UNKNOWN"}, Limit: 10},
		"relation type": {Scope: scope, RelationTypes: []repository.RelationKind{"UNKNOWN"}, Limit: 10},
	}
	for name, query := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := store.Query(context.Background(), query)
			assertMemoryGraphCode(t, err, graph.ErrInvalidInput)
		})
	}
}

func TestAcceptanceQueryRejectsRootExcludedByEntityFilter(t *testing.T) {
	store, scope := acceptanceGraph(t)
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1"}, EntityTypes: []graph.EntityType{graph.EntityClass}, Depth: 1, Limit: 10})
	assertMemoryGraphCode(t, err, graph.ErrInvalidInput)
}

func TestAcceptanceQueryLimitIsStrictForEveryResult(t *testing.T) {
	store, scope := acceptanceGraph(t)
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1", "n2"}, Depth: 0, Limit: 1})
	if err == nil && len(result.Nodes) > 1 {
		t.Fatalf("query returned %d nodes with limit 1", len(result.Nodes))
	}
}

func TestAcceptanceTruncatedOnlyReportsEligibleReachableOmissions(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "disconnected"}
	store := NewGraphRepository()
	revision := graph.Revision{
		EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID},
		RevisionID: "revision-disconnected", SnapshotID: scope.SnapshotID, BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{
			{NodeID: "n1", ArtifactID: "a1", EntityType: graph.EntityFunction, Name: "root"},
			{NodeID: "n2", ArtifactID: "a2", EntityType: graph.EntityFunction, Name: "disconnected"},
		},
	}
	if err := store.Save(context.Background(), "key", revision); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1"}, Depth: 10, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Diagnostics.Truncated {
		t.Fatal("disconnected nodes must not make a complete rooted query appear truncated")
	}
}

func TestAcceptanceTruncatedIsTrueWhenReachableNodeWasOmitted(t *testing.T) {
	store, scope := acceptanceGraph(t)
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1"}, Direction: graph.DirectionOutgoing, Depth: 2, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Diagnostics.Truncated {
		t.Fatal("reachable node omitted by limit must set truncated")
	}
}

func acceptanceGraph(t *testing.T) (*GraphRepository, common.Scope) {
	t.Helper()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"}
	store := NewGraphRepository()
	revision := graph.Revision{
		EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID},
		RevisionID: "revision", SnapshotID: scope.SnapshotID, BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{
			{NodeID: "n1", ArtifactID: "a1", EntityType: graph.EntityFunction, Name: "one"},
			{NodeID: "n2", ArtifactID: "a2", EntityType: graph.EntityFunction, Name: "two"},
			{NodeID: "n3", ArtifactID: "a3", EntityType: graph.EntityClass, Name: "three"},
		},
		Edges: []graph.Relation{
			{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "n1", ToNodeID: "n2"},
			{EdgeID: "e2", RelationType: repository.RelationContains, FromNodeID: "n3", ToNodeID: "n1"},
		},
	}
	if err := store.Save(context.Background(), "key", revision); err != nil {
		t.Fatal(err)
	}
	return store, scope
}

func assertMemoryGraphCode(t *testing.T, err error, code graph.ErrorCode) {
	t.Helper()
	if !graph.IsCode(err, code) {
		t.Fatalf("expected graph error %s, got %v", code, err)
	}
}
