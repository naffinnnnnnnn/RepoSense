package visualization

import (
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func validQuery() Query {
	return Query{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"},
		GraphRevisionID: "revision", ViewType: ViewCallGraph, Depth: 2}
}

func TestQueryValidationAndNormalization(t *testing.T) {
	query := validQuery()
	query.Filters.EntityTypes = []graph.EntityType{graph.EntityFunction, graph.EntityFunction}
	if err := query.Validate(); err == nil {
		t.Fatal("expected duplicate entity type to be rejected")
	}
	query = validQuery()
	query.Filters.RelationTypes = []repository.RelationKind{"UNKNOWN"}
	if err := query.Validate(); err == nil {
		t.Fatal("expected unknown relation type to be rejected")
	}
	query = validQuery()
	query.RootIDs = []string{"z", "a"}
	query.Filters.Languages = []string{"Go", "Python"}
	normalized := query.Normalize()
	if normalized.Limit != 500 || normalized.Layout != LayoutDAG || normalized.Theme != ThemeLight || normalized.Filters.Direction != graph.DirectionBoth {
		t.Fatalf("defaults were not applied: %#v", normalized)
	}
	if normalized.RootIDs[0] != "a" || normalized.Filters.Languages[0] != "go" {
		t.Fatalf("normalization is not stable: %#v", normalized)
	}
	if query.RootIDs[0] != "z" || query.Filters.Languages[0] != "Go" {
		t.Fatalf("normalization mutated caller input: %#v", query)
	}
}

func TestQueryRejectsBoundaryViolations(t *testing.T) {
	tests := []Query{
		{GraphRevisionID: "r", ViewType: ViewCallGraph},
		func() Query { q := validQuery(); q.Depth = 11; return q }(),
		func() Query { q := validQuery(); q.Limit = 5001; return q }(),
		func() Query { q := validQuery(); q.Filters.MinConfidence = 1.1; return q }(),
		func() Query { q := validQuery(); q.RootIDs = []string{""}; return q }(),
	}
	for i, query := range tests {
		if err := query.Validate(); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
}
