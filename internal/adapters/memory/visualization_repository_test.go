package memory

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/visualization"
)

func TestVisualizationRepositoryIsolationExpiryAndCopies(t *testing.T) {
	now := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	repo := NewVisualizationRepository()
	repo.now = func() time.Time { return now }
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	projection := visualization.Projection{ExpiresAt: now.Add(time.Minute), Nodes: []visualization.Node{{ID: "n", Properties: map[string]string{"x": "1"}}},
		Layout: visualization.Layout{Positions: map[string]visualization.Position{"n": {X: 1}}}}
	if err := repo.Save(context.Background(), scope, "hash", projection); err != nil {
		t.Fatal(err)
	}
	got, ok, err := repo.Get(context.Background(), scope, "hash")
	if err != nil || !ok {
		t.Fatalf("cache miss: %#v %v", got, err)
	}
	got.Nodes[0].Properties["x"] = "changed"
	again, _, _ := repo.Get(context.Background(), scope, "hash")
	if again.Nodes[0].Properties["x"] != "1" {
		t.Fatal("cached projection was mutated")
	}
	other := scope
	other.TenantID = "other"
	if _, ok, _ := repo.Get(context.Background(), other, "hash"); ok {
		t.Fatal("tenant isolation failed")
	}
	now = now.Add(2 * time.Minute)
	if _, ok, _ := repo.Get(context.Background(), scope, "hash"); ok {
		t.Fatal("expired projection was returned")
	}
}
