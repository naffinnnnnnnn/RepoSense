package visualizationapp

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/domain/visualization"
)

func TestDeterministicLayoutHandlesCyclesAndCancellation(t *testing.T) {
	nodes := []visualization.Node{{ID: "b"}, {ID: "a"}, {ID: "c"}}
	edges := []visualization.Edge{{Source: "a", Target: "b"}, {Source: "b", Target: "a"}}
	first, err := (DeterministicLayout{}).Layout(context.Background(), visualization.LayoutDAG, nodes, edges)
	if err != nil || len(first.Positions) != 3 {
		t.Fatalf("cyclic graph was not laid out: %#v %v", first, err)
	}
	second, _ := (DeterministicLayout{}).Layout(context.Background(), visualization.LayoutDAG, nodes, edges)
	for id, position := range first.Positions {
		if second.Positions[id] != position {
			t.Fatalf("layout is not deterministic for %s", id)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (DeterministicLayout{}).Layout(ctx, visualization.LayoutGrid, nodes, nil); err == nil {
		t.Fatal("expected canceled context")
	}
}
