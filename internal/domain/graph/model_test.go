package graph

import (
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestBuildCommandOnlyAcceptsCompleteFullBuilds(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	if err := (BuildCommand{Scope: scope, Mode: BuildFull, IdempotencyKey: "key"}).Validate(); err != nil {
		t.Fatal(err)
	}

	tests := map[string]BuildCommand{
		"unknown mode":       {Scope: scope, Mode: "UNKNOWN", IdempotencyKey: "key"},
		"incremental mode":   {Scope: scope, Mode: BuildIncremental, IdempotencyKey: "key"},
		"empty idempotency":  {Scope: scope, Mode: BuildFull},
		"partial full build": {Scope: scope, Mode: BuildFull, IdempotencyKey: "key", ArtifactIDs: []string{"artifact"}},
	}
	for name, command := range tests {
		t.Run(name, func(t *testing.T) {
			if command.Validate() == nil {
				t.Fatalf("expected invalid command: %#v", command)
			}
		})
	}
}

func TestQueryValidationRejectsInvalidBoundsAndEnums(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	tests := map[string]Query{
		"depth below minimum": {Scope: scope, Depth: -1},
		"depth above maximum": {Scope: scope, Depth: 11},
		"limit below minimum": {Scope: scope, Limit: -1},
		"limit above maximum": {Scope: scope, Limit: 10_001},
		"invalid direction":   {Scope: scope, Direction: Direction("SIDEWAYS")},
		"invalid entity type": {Scope: scope, EntityTypes: []EntityType{"UNKNOWN"}},
		"invalid relation":    {Scope: scope, RelationTypes: []repository.RelationKind{"UNKNOWN"}},
	}
	for name, query := range tests {
		t.Run(name, func(t *testing.T) {
			if query.Validate() == nil {
				t.Fatalf("expected invalid query: %#v", query)
			}
		})
	}
}
