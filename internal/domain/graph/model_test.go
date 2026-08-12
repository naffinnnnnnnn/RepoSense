package graph

import (
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
)

func TestBuildCommandAndQueryValidation(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	if err := (BuildCommand{Scope: scope, Mode: BuildFull, IdempotencyKey: "key"}).Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []BuildCommand{
		{Scope: scope, Mode: "UNKNOWN", IdempotencyKey: "key"},
		{Scope: scope, Mode: BuildFull},
		{Scope: scope, Mode: BuildFull, IdempotencyKey: "key", ArtifactIDs: []string{"a", "a"}},
	}
	for _, command := range bad {
		if command.Validate() == nil {
			t.Fatalf("expected invalid command: %#v", command)
		}
	}
	if err := (Query{Scope: scope, Depth: 11}).Validate(); err == nil {
		t.Fatal("expected excessive depth to be rejected")
	}
	if err := (Query{Scope: scope, Direction: Direction("SIDEWAYS")}).Validate(); err == nil {
		t.Fatal("expected invalid direction to be rejected")
	}
}
