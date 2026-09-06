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
		if existing.RequestFingerprint != revision.RequestFingerprint {
			return &graph.DomainError{Code: graph.ErrConflict, Operation: "save_revision", Stage: "save_revision", Message: "idempotency key already used for another graph request"}
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
	ordered := []string{}
	distances := map[string]int{}
	if len(q.RootIDs) == 0 {
		ids := make([]string, 0, len(nodes))
		for id, node := range nodes {
			if entityMatches(node, entityAllowed) && !diagnosticNode(node) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		ordered = ids
	} else {
		rootSet := map[string]struct{}{}
		for _, root := range q.RootIDs {
			id := artifactToNode[root]
			if id == "" {
				return graph.Result{}, &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "resolve_roots", Stage: "resolve_roots", Message: "root artifact was not found"}
			}
			if !entityMatches(nodes[id], entityAllowed) || diagnosticNode(nodes[id]) {
				return graph.Result{}, &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "resolve_roots", Stage: "resolve_roots", Message: "root artifact is excluded by the query"}
			}
			rootSet[id] = struct{}{}
		}
		if len(rootSet) > limit {
			return graph.Result{}, &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "resolve_roots", Stage: "resolve_roots", Message: "root count exceeds query limit"}
		}
		frontier := make([]string, 0, len(rootSet))
		for id := range rootSet {
			frontier = append(frontier, id)
			distances[id] = 0
		}
		sort.Strings(frontier)
		ordered = append(ordered, frontier...)
		for depth := 0; depth < q.Depth && len(frontier) > 0; depth++ {
			nextSet := map[string]struct{}{}
			for _, edge := range revision.Edges {
				if err := ctx.Err(); err != nil {
					return graph.Result{}, err
				}
				if diagnosticRelation(edge) || !relationMatches(edge, relationAllowed) {
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
					if neighbor != "" {
						if _, seen := distances[neighbor]; seen {
							continue
						}
						node, exists := nodes[neighbor]
						if !exists || diagnosticNode(node) || !entityMatches(node, entityAllowed) {
							continue
						}
						nextSet[neighbor] = struct{}{}
					}
				}
			}
			next := make([]string, 0, len(nextSet))
			for id := range nextSet {
				distances[id] = depth + 1
				next = append(next, id)
			}
			sort.Strings(next)
			ordered = append(ordered, next...)
			frontier = next
		}
	}
	visited := len(ordered)
	truncated := len(ordered) > limit
	if truncated {
		ordered = ordered[:limit]
	}
	result := graph.Result{Diagnostics: graph.Diagnostics{RevisionID: revision.RevisionID, Visited: visited, Truncated: truncated, DurationMS: time.Since(started).Milliseconds()}}
	visible := map[string]bool{}
	for _, id := range ordered {
		result.Nodes = append(result.Nodes, cloneEntity(nodes[id]))
		visible[id] = true
	}
	for _, edge := range revision.Edges {
		if !diagnosticRelation(edge) && visible[edge.FromNodeID] && visible[edge.ToNodeID] && relationMatches(edge, relationAllowed) {
			result.Edges = append(result.Edges, cloneRelation(edge))
		}
	}
	sort.Slice(result.Edges, func(i, j int) bool { return result.Edges[i].EdgeID < result.Edges[j].EdgeID })
	return result, nil
}

func diagnosticRelation(edge graph.Relation) bool {
	return edge.RelationType == repository.RelationKind("UNCERTAIN_RELATION") || edge.RelationType == repository.RelationKind("POSSIBLE_TARGET")
}

func diagnosticNode(node graph.Entity) bool {
	return node.Properties != nil && node.Properties["resolution"] != ""
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
