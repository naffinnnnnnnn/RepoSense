package memory

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestGraphRepositoryQueryIsolationFilteringAndCopies(t *testing.T) {
	store := NewGraphRepository()
	scope := common.Scope{TenantID: "tenant-a", RepositoryID: "repo", SnapshotID: "snap"}
	revision := graph.Revision{EntityMeta: common.EntityMeta{TenantID: "tenant-a", RepositoryID: "repo"}, RevisionID: "gr1", SnapshotID: "snap", BuildStatus: graph.RevisionActive,
		Nodes: []graph.Entity{{NodeID: "n1", ArtifactID: "a1", EntityType: graph.EntityFunction, Name: "one", Properties: map[string]string{"x": "1"}}, {NodeID: "n2", ArtifactID: "a2", EntityType: graph.EntityFunction, Name: "two"}, {NodeID: "n3", ArtifactID: "a3", EntityType: graph.EntityClass, Name: "three"}},
		Edges: []graph.Relation{{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "n1", ToNodeID: "n2"}, {EdgeID: "e2", RelationType: repository.RelationContains, FromNodeID: "n3", ToNodeID: "n1"}}}
	if err := store.Save(context.Background(), "key", revision); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"a1"}, Depth: 1, Direction: graph.DirectionOutgoing, RelationTypes: []repository.RelationKind{repository.RelationCalls}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 2 || len(result.Edges) != 1 || result.Edges[0].EdgeID != "e1" {
		t.Fatalf("unexpected projection: %#v", result)
	}
	result.Nodes[0].Properties["x"] = "mutated"
	again, err := store.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{"n1"}, Depth: 0})
	if err != nil {
		t.Fatal(err)
	}
	if again.Nodes[0].Properties["x"] != "1" {
		t.Fatal("stored graph was modified through query result")
	}
	other := scope
	other.TenantID = "tenant-b"
	if _, err := store.Query(context.Background(), graph.Query{Scope: other}); !graph.IsCode(err, graph.ErrRevisionNotFound) {
		t.Fatalf("expected tenant isolation, got %v", err)
	}
}

func TestGraphRepositoryRejectsConflictingPublication(t *testing.T) {
	store := NewGraphRepository()
	base := graph.Revision{EntityMeta: common.EntityMeta{TenantID: "t", RepositoryID: "r"}, RevisionID: "one", SnapshotID: "s", BuildStatus: graph.RevisionActive}
	if err := store.Save(context.Background(), "key", base); err != nil {
		t.Fatal(err)
	}
	base.RevisionID = "two"
	if err := store.Save(context.Background(), "other", base); !graph.IsCode(err, graph.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}
