package memory

import (
	"context"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/visualization"
)

// VisualizationRepository is a bounded-lifetime reference cache. Production
// adapters can implement the same port with Redis or PostgreSQL.
type VisualizationRepository struct {
	mu    sync.RWMutex
	items map[string]visualization.Projection
	now   func() time.Time
}

func NewVisualizationRepository() *VisualizationRepository {
	return &VisualizationRepository{items: make(map[string]visualization.Projection), now: time.Now}
}

func visualizationKey(scope common.Scope, queryHash string) string {
	return scope.TenantID + "\x00" + scope.RepositoryID + "\x00" + scope.SnapshotID + "\x00" + queryHash
}

func (r *VisualizationRepository) Get(ctx context.Context, scope common.Scope, queryHash string) (visualization.Projection, bool, error) {
	if err := ctx.Err(); err != nil {
		return visualization.Projection{}, false, err
	}
	key := visualizationKey(scope, queryHash)
	r.mu.RLock()
	projection, ok := r.items[key]
	r.mu.RUnlock()
	if !ok {
		return visualization.Projection{}, false, nil
	}
	if !projection.ExpiresAt.After(r.now()) {
		r.mu.Lock()
		delete(r.items, key)
		r.mu.Unlock()
		return visualization.Projection{}, false, nil
	}
	return cloneProjection(projection), true, nil
}

func (r *VisualizationRepository) Save(ctx context.Context, scope common.Scope, queryHash string, projection visualization.Projection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.items[visualizationKey(scope, queryHash)] = cloneProjection(projection)
	r.mu.Unlock()
	return nil
}

func cloneProjection(value visualization.Projection) visualization.Projection {
	value.Nodes = append([]visualization.Node(nil), value.Nodes...)
	for i := range value.Nodes {
		value.Nodes[i].Properties = cloneStringMap(value.Nodes[i].Properties)
	}
	value.Edges = append([]visualization.Edge(nil), value.Edges...)
	for i := range value.Edges {
		value.Edges[i].Properties = cloneStringMap(value.Edges[i].Properties)
	}
	value.Layout.Positions = make(map[string]visualization.Position, len(value.Layout.Positions))
	for id, position := range value.Layout.Positions {
		value.Layout.Positions[id] = position
	}
	value.SourceLinks = make(map[string]visualization.SourceLink, len(value.SourceLinks))
	for id, link := range value.SourceLinks {
		value.SourceLinks[id] = link
	}
	value.Diagnostics.Warnings = append([]string(nil), value.Diagnostics.Warnings...)
	return value
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
