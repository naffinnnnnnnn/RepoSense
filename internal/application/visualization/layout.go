package visualizationapp

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/reposense/reposense/internal/domain/visualization"
)

const LayoutAlgorithmVersion = "deterministic-layout@1"

// DeterministicLayout produces stable coordinates for caching, golden tests,
// and static exports. A production force-directed engine can replace it via
// ports.LayoutEngine without changing the service.
type DeterministicLayout struct{}

func (DeterministicLayout) Layout(ctx context.Context, kind visualization.LayoutType, nodes []visualization.Node, edges []visualization.Edge) (visualization.Layout, error) {
	if err := ctx.Err(); err != nil {
		return visualization.Layout{}, err
	}
	positions := make(map[string]visualization.Position, len(nodes))
	switch kind {
	case visualization.LayoutDAG:
		layoutDAG(ctx, positions, nodes, edges)
	case visualization.LayoutGrid:
		layoutGrid(ctx, positions, nodes)
	case visualization.LayoutRadial:
		layoutRadial(ctx, positions, nodes)
	default:
		return visualization.Layout{}, fmt.Errorf("unsupported layout %q", kind)
	}
	if err := ctx.Err(); err != nil {
		return visualization.Layout{}, err
	}
	return visualization.Layout{Algorithm: kind, AlgorithmVersion: LayoutAlgorithmVersion, Positions: positions}, nil
}

func layoutGrid(ctx context.Context, out map[string]visualization.Position, nodes []visualization.Node) {
	ids := sortedNodeIDs(nodes)
	columns := int(math.Ceil(math.Sqrt(float64(len(ids)))))
	if columns == 0 {
		return
	}
	for i, id := range ids {
		if ctx.Err() != nil {
			return
		}
		out[id] = visualization.Position{X: float64(i%columns) * 220, Y: float64(i/columns) * 140}
	}
}

func layoutRadial(ctx context.Context, out map[string]visualization.Position, nodes []visualization.Node) {
	ids := sortedNodeIDs(nodes)
	if len(ids) == 1 {
		out[ids[0]] = visualization.Position{}
		return
	}
	radius := math.Max(180, float64(len(ids))*28)
	for i, id := range ids {
		if ctx.Err() != nil {
			return
		}
		angle := 2 * math.Pi * float64(i) / float64(len(ids))
		out[id] = visualization.Position{X: math.Round(radius * math.Cos(angle)), Y: math.Round(radius * math.Sin(angle))}
	}
}

func layoutDAG(ctx context.Context, out map[string]visualization.Position, nodes []visualization.Node, edges []visualization.Edge) {
	ids := sortedNodeIDs(nodes)
	known := make(map[string]bool, len(ids))
	indegree := make(map[string]int, len(ids))
	adjacency := make(map[string][]string, len(ids))
	for _, id := range ids {
		known[id] = true
	}
	for _, edge := range edges {
		if known[edge.Source] && known[edge.Target] && edge.Source != edge.Target {
			adjacency[edge.Source] = append(adjacency[edge.Source], edge.Target)
			indegree[edge.Target]++
		}
	}
	for id := range adjacency {
		sort.Strings(adjacency[id])
	}
	queue := make([]string, 0, len(ids))
	for _, id := range ids {
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}
	layer := make(map[string]int, len(ids))
	seen := make(map[string]bool, len(ids))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		for _, target := range adjacency[id] {
			if layer[target] < layer[id]+1 {
				layer[target] = layer[id] + 1
			}
			indegree[target]--
			if indegree[target] == 0 {
				queue = append(queue, target)
			}
		}
	}
	maxLayer := 0
	for _, value := range layer {
		if value > maxLayer {
			maxLayer = value
		}
	}
	// Strongly connected components left by Kahn are placed in a stable final
	// layer. This keeps cyclic call graphs renderable and deterministic.
	for _, id := range ids {
		if !seen[id] {
			layer[id] = maxLayer + 1
		}
	}
	groups := make(map[int][]string)
	for _, id := range ids {
		groups[layer[id]] = append(groups[layer[id]], id)
	}
	for level, group := range groups {
		for row, id := range group {
			if ctx.Err() != nil {
				return
			}
			out[id] = visualization.Position{X: float64(level) * 260, Y: float64(row) * 140}
		}
	}
}

func sortedNodeIDs(nodes []visualization.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	return ids
}
