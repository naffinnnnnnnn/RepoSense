package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type GraphRepository struct {
	mu          sync.RWMutex
	revisions   map[string]graph.Revision
	bySnapshot  map[string]string
	idempotency map[string]string
}

func NewGraphRepository() *GraphRepository {
	return &GraphRepository{revisions: map[string]graph.Revision{}, bySnapshot: map[string]string{}, idempotency: map[string]string{}}
}

func graphScopeKey(s common.Scope) string                   { return s.TenantID + "\x00" + s.RepositoryID }
func graphSnapshotKey(s common.Scope) string                { return graphScopeKey(s) + "\x00" + s.SnapshotID }
func graphIdempotencyKey(s common.Scope, key string) string { return graphScopeKey(s) + "\x00" + key }

func (s *GraphRepository) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (graph.Revision, bool, error) {
	if err := ctx.Err(); err != nil {
		return graph.Revision{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.idempotency[graphIdempotencyKey(scope, key)]
	if !ok {
		return graph.Revision{}, false, nil
	}
	return cloneGraphRevision(s.revisions[id]), true, nil
}

func (s *GraphRepository) RevisionBySnapshot(ctx context.Context, scope common.Scope) (graph.Revision, error) {
	if err := ctx.Err(); err != nil {
		return graph.Revision{}, err
	}
	if err := scope.Validate(true); err != nil {
		return graph.Revision{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.bySnapshot[graphSnapshotKey(scope)]
	if !ok {
		return graph.Revision{}, &graph.DomainError{Code: graph.ErrRevisionNotFound, Operation: "load_revision", Message: "graph revision not found"}
	}
	return cloneGraphRevision(s.revisions[id]), nil
}

func (s *GraphRepository) Save(ctx context.Context, key string, revision graph.Revision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if revision.BuildStatus != graph.RevisionActive {
		return fmt.Errorf("only ACTIVE graph revisions can be published")
	}
	if revision.RevisionID == "" || revision.SnapshotID == "" {
		return fmt.Errorf("revision_id and snapshot_id are required")
	}
	scope := common.Scope{TenantID: revision.TenantID, RepositoryID: revision.RepositoryID, SnapshotID: revision.SnapshotID}
	s.mu.Lock()
	defer s.mu.Unlock()
	ik := graphIdempotencyKey(scope, key)
	if existingID, ok := s.idempotency[ik]; ok {
		existing := s.revisions[existingID]
		if existing.SnapshotID != revision.SnapshotID {
			return &graph.DomainError{Code: graph.ErrConflict, Operation: "save_revision", Message: "idempotency key already used for another snapshot"}
		}
		return nil
	}
	sk := graphSnapshotKey(scope)
	if existingID, ok := s.bySnapshot[sk]; ok && existingID != revision.RevisionID {
		return &graph.DomainError{Code: graph.ErrConflict, Operation: "save_revision", Message: "snapshot already has a published graph revision"}
	}
	stored := cloneGraphRevision(revision)
	s.revisions[revision.RevisionID] = stored
	s.bySnapshot[sk] = revision.RevisionID
	s.idempotency[ik] = revision.RevisionID
	return nil
}

func (s *GraphRepository) Query(ctx context.Context, q graph.Query) (graph.Result, error) {
	started := time.Now()
	if err := q.Validate(); err != nil {
		return graph.Result{}, &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "validate_query", Message: err.Error(), Cause: err}
	}
	if err := ctx.Err(); err != nil {
		return graph.Result{}, err
	}
	revision, err := s.RevisionBySnapshot(ctx, q.Scope)
	if err != nil {
		return graph.Result{}, err
	}
	limit := q.Limit
	if limit == 0 {
		limit = 500
	}
	direction := q.Direction
	if direction == "" {
		direction = graph.DirectionBoth
	}
	nodes := make(map[string]graph.Entity, len(revision.Nodes))
	artifactToNode := make(map[string]string, len(revision.Nodes))
	for _, node := range revision.Nodes {
		nodes[node.NodeID] = node
		if node.ArtifactID != "" {
			artifactToNode[node.ArtifactID] = node.NodeID
		}
	}
	relationAllowed := relationTypeSet(q.RelationTypes)
	entityAllowed := entityTypeSet(q.EntityTypes)
	selected := map[string]bool{}
	frontier := make([]string, 0, len(q.RootIDs))
	if len(q.RootIDs) == 0 {
		ids := make([]string, 0, len(nodes))
		for id := range nodes {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if len(selected) >= limit {
				break
			}
			if entityMatches(nodes[id], entityAllowed) {
				selected[id] = true
			}
		}
	} else {
		for _, root := range q.RootIDs {
			id := root
			if _, ok := nodes[id]; !ok {
				id = artifactToNode[root]
			}
			if id == "" {
				continue
			}
			if !selected[id] {
				selected[id] = true
				frontier = append(frontier, id)
			}
		}
		if len(frontier) == 0 {
			return graph.Result{}, &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "resolve_roots", Message: "no root node or artifact was found"}
		}
		for depth := 0; depth < q.Depth && len(frontier) > 0 && len(selected) < limit; depth++ {
			next := []string{}
			for _, edge := range revision.Edges {
				if err := ctx.Err(); err != nil {
					return graph.Result{}, err
				}
				if !relationMatches(edge, relationAllowed) {
					continue
				}
				for _, current := range frontier {
					neighbor := ""
					if (direction == graph.DirectionBoth || direction == graph.DirectionOutgoing) && edge.FromNodeID == current {
						neighbor = edge.ToNodeID
					}
					if (direction == graph.DirectionBoth || direction == graph.DirectionIncoming) && edge.ToNodeID == current {
						neighbor = edge.FromNodeID
					}
					if neighbor != "" && !selected[neighbor] && len(selected) < limit {
						selected[neighbor] = true
						next = append(next, neighbor)
					}
				}
			}
			frontier = next
		}
	}
	result := graph.Result{Diagnostics: graph.Diagnostics{RevisionID: revision.RevisionID, Visited: len(selected), DurationMS: time.Since(started).Milliseconds()}}
	for id := range selected {
		if node, ok := nodes[id]; ok && entityMatches(node, entityAllowed) {
			result.Nodes = append(result.Nodes, cloneEntity(node))
		}
	}
	visible := map[string]bool{}
	for _, node := range result.Nodes {
		visible[node.NodeID] = true
	}
	for _, edge := range revision.Edges {
		if visible[edge.FromNodeID] && visible[edge.ToNodeID] && relationMatches(edge, relationAllowed) {
			result.Edges = append(result.Edges, cloneRelation(edge))
		}
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].NodeID < result.Nodes[j].NodeID })
	sort.Slice(result.Edges, func(i, j int) bool { return result.Edges[i].EdgeID < result.Edges[j].EdgeID })
	result.Diagnostics.Truncated = len(selected) >= limit && len(selected) < len(nodes)
	return result, nil
}

func relationTypeSet(values []repository.RelationKind) map[repository.RelationKind]bool {
	if len(values) == 0 {
		return nil
	}
	out := map[repository.RelationKind]bool{}
	for _, v := range values {
		out[v] = true
	}
	return out
}
func entityTypeSet(values []graph.EntityType) map[graph.EntityType]bool {
	if len(values) == 0 {
		return nil
	}
	out := map[graph.EntityType]bool{}
	for _, v := range values {
		out[v] = true
	}
	return out
}
func relationMatches(v graph.Relation, allowed map[repository.RelationKind]bool) bool {
	return allowed == nil || allowed[v.RelationType]
}
func entityMatches(v graph.Entity, allowed map[graph.EntityType]bool) bool {
	return allowed == nil || allowed[v.EntityType]
}

func cloneGraphRevision(r graph.Revision) graph.Revision {
	r.Nodes = append([]graph.Entity(nil), r.Nodes...)
	for i := range r.Nodes {
		r.Nodes[i] = cloneEntity(r.Nodes[i])
	}
	r.Edges = append([]graph.Relation(nil), r.Edges...)
	for i := range r.Edges {
		r.Edges[i] = cloneRelation(r.Edges[i])
	}
	if r.PublishedEvent.Payload != nil {
		payload := make(map[string]any, len(r.PublishedEvent.Payload))
		for key, value := range r.PublishedEvent.Payload {
			payload[key] = value
		}
		r.PublishedEvent.Payload = payload
	}
	return r
}
func cloneEntity(v graph.Entity) graph.Entity {
	if v.Properties != nil {
		m := map[string]string{}
		for k, x := range v.Properties {
			m[k] = x
		}
		v.Properties = m
	}
	if v.SourceRef != nil {
		r := *v.SourceRef
		v.SourceRef = &r
	}
	return v
}
func cloneRelation(v graph.Relation) graph.Relation {
	if v.Properties != nil {
		m := map[string]string{}
		for k, x := range v.Properties {
			m[k] = x
		}
		v.Properties = m
	}
	return v
}
