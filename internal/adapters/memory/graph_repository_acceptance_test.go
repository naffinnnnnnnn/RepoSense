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
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"a1", "missing"}, Depth: 1, Limit: 10})
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
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"a1"}, EntityTypes: []graph.EntityType{graph.EntityClass}, Depth: 1, Limit: 10})
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
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"a1"}, Depth: 10, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Diagnostics.Truncated {
		t.Fatal("disconnected nodes must not make a complete rooted query appear truncated")
	}
}

func TestAcceptanceTruncatedIsTrueWhenReachableNodeWasOmitted(t *testing.T) {
	store, scope := acceptanceGraph(t)
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"a1"}, Direction: graph.DirectionOutgoing, Depth: 2, Limit: 1})
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

func TestAcceptanceQueryRejectsInternalNodeIDRoot(t *testing.T) {
	store, scope := acceptanceGraph(t)
	_, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1"}, Depth: 0, Limit: 10})
	assertMemoryGraphCode(t, err, graph.ErrInvalidInput)
}

func TestAcceptanceEntityFilterStopsTraversal(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "entity-filter"}
	store := NewGraphRepository()
	revision := graph.Revision{
		EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID},
		RevisionID: "revision-entity-filter", SnapshotID: scope.SnapshotID, BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{
			{NodeID: "root", ArtifactID: "root-artifact", EntityType: graph.EntityFunction, Name: "root"},
			{NodeID: "middle", ArtifactID: "middle-artifact", EntityType: graph.EntityClass, Name: "middle"},
			{NodeID: "hidden-child", ArtifactID: "child-artifact", EntityType: graph.EntityFunction, Name: "child"},
		},
		Edges: []graph.Relation{
			{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "root", ToNodeID: "middle"},
			{EdgeID: "e2", RelationType: repository.RelationCalls, FromNodeID: "middle", ToNodeID: "hidden-child"},
		},
	}
	if err := store.Save(context.Background(), "entity-filter", revision); err != nil {
		t.Fatal(err)
	}

	result, err := store.Query(context.Background(), graph.Query{
		Scope: scope, RootIDs: []string{"root-artifact"}, EntityTypes: []graph.EntityType{graph.EntityFunction}, Depth: 2, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 1 || result.Nodes[0].ArtifactID != "root-artifact" || len(result.Edges) != 0 {
		t.Fatalf("entity filter must stop traversal through excluded nodes: %#v", result)
	}
}

func TestAcceptanceDiagnosticRelationsAreExcludedByDefault(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "diagnostic"}
	store := NewGraphRepository()
	revision := graph.Revision{
		EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID},
		RevisionID: "revision-diagnostic", SnapshotID: scope.SnapshotID, BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{
			{NodeID: "root", ArtifactID: "root-artifact", EntityType: graph.EntityFunction, Name: "root"},
			{NodeID: "issue", EntityType: graph.EntitySymbol, Name: "unresolved target", Properties: map[string]string{"resolution": "UNRESOLVED"}},
		},
		Edges: []graph.Relation{
			{EdgeID: "uncertain", RelationType: repository.RelationKind("UNCERTAIN_RELATION"), FromNodeID: "root", ToNodeID: "issue"},
		},
	}
	if err := store.Save(context.Background(), "diagnostic", revision); err != nil {
		t.Fatal(err)
	}

	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"root-artifact"}, Depth: 1, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 1 || len(result.Edges) != 0 {
		t.Fatalf("business query leaked diagnostic graph: %#v", result)
	}
}

func TestAcceptanceQueryOrdersNodesByDistanceThenNodeID(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "ordering"}
	store := NewGraphRepository()
	revision := graph.Revision{
		EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID},
		RevisionID: "revision-ordering", SnapshotID: scope.SnapshotID, BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{
			{NodeID: "root", ArtifactID: "root-artifact", EntityType: graph.EntityFunction, Name: "root"},
			{NodeID: "z-depth-one", ArtifactID: "one", EntityType: graph.EntityFunction, Name: "one"},
			{NodeID: "a-depth-two", ArtifactID: "two", EntityType: graph.EntityFunction, Name: "two"},
		},
		Edges: []graph.Relation{
			{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "root", ToNodeID: "z-depth-one"},
			{EdgeID: "e2", RelationType: repository.RelationCalls, FromNodeID: "z-depth-one", ToNodeID: "a-depth-two"},
		},
	}
	if err := store.Save(context.Background(), "ordering", revision); err != nil {
		t.Fatal(err)
	}

	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"root-artifact"}, Direction: graph.DirectionOutgoing, Depth: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"root", "z-depth-one", "a-depth-two"}
	if len(result.Nodes) != len(want) {
		t.Fatalf("nodes=%#v want=%#v", result.Nodes, want)
	}
	for i := range want {
		if result.Nodes[i].NodeID != want[i] {
			t.Fatalf("node order=%#v want=%#v", result.Nodes, want)
		}
	}
}
