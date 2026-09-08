package neo4j

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func (s *GraphDataStore) QueryRevision(ctx context.Context, revisionID string, query graph.Query) (graph.Result, error) {
	started := time.Now()
	if err := query.Validate(); err != nil {
		return graph.Result{}, invalidInput("query_revision", "query", err.Error(), err)
	}
	if strings.TrimSpace(revisionID) == "" {
		return graph.Result{}, invalidInput("query_revision", "query", "revision_id is required", nil)
	}
	limit := query.Limit
	if limit == 0 {
		limit = 500
	}
	if err := s.config.Validate(); err != nil {
		return graph.Result{}, invalidInput("query_revision", "query", "graph query budgets are invalid", err)
	}
	if limit > s.config.MaxNodes {
		return graph.Result{}, queryBudgetError("query_revision", "requested node limit exceeds the configured query budget")
	}
	worstCaseQueries := 4
	if len(query.RootIDs) != 0 {
		worstCaseQueries += query.Depth
	}
	if worstCaseQueries > s.config.MaxDatabaseQueries {
		return graph.Result{}, queryBudgetError("query_revision", "requested traversal exceeds the database query budget")
	}
	direction := query.Direction
	if direction == "" {
		direction = graph.DirectionBoth
	}

	value, err := s.read(ctx, "query_revision", "query", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		if err := requireSealedRevision(ctx, tx, query.Scope, revisionID); err != nil {
			return nil, err
		}
		ordered, distances, err := discoverNodes(ctx, tx, revisionID, query, direction, limit, s.config)
		if err != nil {
			return nil, err
		}
		visited := len(ordered)
		truncated := visited > limit
		if truncated {
			ordered = ordered[:limit]
		}
		nodes, err := loadNodes(ctx, tx, revisionID, ordered)
		if err != nil {
			return nil, err
		}
		edges, err := loadEdges(ctx, tx, revisionID, ordered, query.RelationTypes, s.config.MaxEdges)
		if err != nil {
			return nil, err
		}
		return queryData{ordered: ordered, distances: distances, nodes: nodes, edges: edges, visited: visited, truncated: truncated}, nil
	})
	if err != nil {
		return graph.Result{}, err
	}
	data, ok := value.(queryData)
	if !ok {
		return graph.Result{}, graphStoreError("query_revision", "query", fmt.Errorf("neo4j returned an invalid graph result"))
	}
	result := graph.Result{Diagnostics: graph.Diagnostics{RevisionID: revisionID, Visited: data.visited, Truncated: data.truncated, DurationMS: time.Since(started).Milliseconds()}}
	for _, nodeID := range data.ordered {
		if node, exists := data.nodes[nodeID]; exists {
			result.Nodes = append(result.Nodes, node)
		}
	}
	result.Edges = data.edges
	return result, nil
}

type queryData struct {
	ordered   []string
	distances map[string]int
	nodes     map[string]graph.Entity
	edges     []graph.Relation
	visited   int
	truncated bool
}

func requireSealedRevision(ctx context.Context, tx neo4jdriver.ManagedTransaction, scope common.Scope, revisionID string) error {
	result, err := tx.Run(ctx, `MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id,candidate_status:'SEALED'}) RETURN count(r) AS matches`, scopeParams(scope, revisionID))
	if err != nil {
		return err
	}
	record, err := result.Single(ctx)
	if err != nil {
		return err
	}
	if intValue(record.AsMap()["matches"]) != 1 {
		return revisionNotFound("query_revision", nil)
	}
	return nil
}

func discoverNodes(ctx context.Context, tx neo4jdriver.ManagedTransaction, revisionID string, query graph.Query, direction graph.Direction, limit int, config GraphDataConfig) ([]string, map[string]int, error) {
	entityTypes := entityTypeStrings(query.EntityTypes)
	if len(query.RootIDs) == 0 {
		result, err := tx.Run(ctx, `MATCH (n:CodeEntity) WHERE n.revision_id=$revision_id
AND (size($entity_types)=0 OR n.entity_type IN $entity_types)
RETURN n.node_id AS node_id ORDER BY node_id LIMIT $limit`, map[string]any{"revision_id": revisionID, "entity_types": entityTypes, "limit": limit + 1})
		if err != nil {
			return nil, nil, err
		}
		ids, err := collectIDs(ctx, result)
		return ids, map[string]int{}, err
	}

	roots := uniqueSorted(query.RootIDs)
	if len(roots) > config.MaxRoots {
		return nil, nil, queryBudgetError("resolve_roots", "root count exceeds the configured query budget")
	}
	if len(roots) > limit {
		return nil, nil, invalidInput("resolve_roots", "query", "root count exceeds query limit", nil)
	}
	result, err := tx.Run(ctx, `UNWIND $roots AS root
OPTIONAL MATCH (n:CodeEntity {revision_id:$revision_id,artifact_id:root})
RETURN root AS root, n.node_id AS node_id, n.entity_type AS entity_type ORDER BY root`, map[string]any{"revision_id": revisionID, "roots": roots})
	if err != nil {
		return nil, nil, err
	}
	allowed := stringSet(entityTypes)
	rootNodeIDs := make([]string, 0, len(roots))
	for result.Next(ctx) {
		values := result.Record().AsMap()
		nodeID, _ := values["node_id"].(string)
		entityType, _ := values["entity_type"].(string)
		if nodeID == "" || (len(allowed) != 0 && !allowed[entityType]) {
			return nil, nil, &graph.DomainError{Code: graph.ErrRootNotFound, Operation: "resolve_roots", Stage: "query", Dependency: "neo4j", Message: "root artifact was not found or is excluded by entity filters", Retryable: false}
		}
		rootNodeIDs = append(rootNodeIDs, nodeID)
	}
	if err := result.Err(); err != nil {
		return nil, nil, err
	}
	if len(rootNodeIDs) != len(roots) {
		return nil, nil, &graph.DomainError{Code: graph.ErrRootNotFound, Operation: "resolve_roots", Stage: "query", Dependency: "neo4j", Message: "one or more root artifacts were not found", Retryable: false}
	}
	sort.Strings(rootNodeIDs)
	ordered := append([]string(nil), rootNodeIDs...)
	distances := make(map[string]int, limit+1)
	for _, id := range rootNodeIDs {
		distances[id] = 0
	}
	frontier := rootNodeIDs
	for depth := 0; depth < query.Depth && len(frontier) > 0 && len(ordered) <= limit; depth++ {
		remaining := limit + 1 - len(ordered)
		pageLimit := remaining
		if pageLimit > config.MaxFrontier+1 {
			pageLimit = config.MaxFrontier + 1
		}
		cypher := neighborQuery(direction)
		result, err := tx.Run(ctx, cypher, map[string]any{
			"revision_id": revisionID, "frontier": frontier, "seen": ordered,
			"relation_types": relationTypeStrings(query.RelationTypes), "entity_types": entityTypes, "limit": pageLimit,
		})
		if err != nil {
			return nil, nil, err
		}
		next, err := collectIDs(ctx, result)
		if err != nil {
			return nil, nil, err
		}
		if len(next) > config.MaxFrontier {
			return nil, nil, queryBudgetError("query_revision", "traversal frontier exceeds the configured query budget")
		}
		for _, id := range next {
			distances[id] = depth + 1
		}
		ordered = append(ordered, next...)
		frontier = next
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if distances[ordered[i]] == distances[ordered[j]] {
			return ordered[i] < ordered[j]
		}
		return distances[ordered[i]] < distances[ordered[j]]
	})
	return ordered, distances, nil
}

func neighborQuery(direction graph.Direction) string {
	pattern := "(current)-[edge]-(next)"
	if direction == graph.DirectionOutgoing {
		pattern = "(current)-[edge]->(next)"
	} else if direction == graph.DirectionIncoming {
		pattern = "(current)<-[edge]-(next)"
	}
	return fmt.Sprintf(`MATCH (current) WHERE current.revision_id=$revision_id AND current.node_id IN $frontier
MATCH %s
WHERE edge.revision_id=$revision_id AND edge.logical_relation=true
	  AND next:CodeEntity AND NOT (next.node_id IN $seen)
  AND (size($relation_types)=0 OR type(edge) IN $relation_types)
  AND (size($entity_types)=0 OR next.entity_type IN $entity_types)
RETURN DISTINCT next.node_id AS node_id ORDER BY node_id LIMIT $limit`, pattern)
}

func loadNodes(ctx context.Context, tx neo4jdriver.ManagedTransaction, revisionID string, ids []string) (map[string]graph.Entity, error) {
	if len(ids) == 0 {
		return map[string]graph.Entity{}, nil
	}
	result, err := tx.Run(ctx, `MATCH (n:CodeEntity) WHERE n.revision_id=$revision_id AND n.node_id IN $ids RETURN n{.*} AS node`, map[string]any{"revision_id": revisionID, "ids": ids})
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]graph.Entity, len(ids))
	for result.Next(ctx) {
		properties, _ := result.Record().AsMap()["node"].(map[string]any)
		node, err := entityFromProperties(properties)
		if err != nil {
			return nil, err
		}
		nodes[node.NodeID] = node
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	if len(nodes) != len(ids) {
		return nil, &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "load_nodes", Stage: "query", Dependency: "neo4j", Message: "sealed graph is missing selected nodes", Retryable: true}
	}
	return nodes, nil
}

func loadEdges(ctx context.Context, tx neo4jdriver.ManagedTransaction, revisionID string, ids []string, relationTypes []repository.RelationKind, maxEdges int) ([]graph.Relation, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	result, err := tx.Run(ctx, `MATCH (source)-[edge]->(target)
WHERE edge.revision_id=$revision_id AND edge.logical_relation=true
	  AND source:CodeEntity AND target:CodeEntity
  AND source.node_id IN $ids AND target.node_id IN $ids
  AND (size($relation_types)=0 OR type(edge) IN $relation_types)
RETURN edge{.*,relation_type:type(edge),from_node_id:source.node_id,to_node_id:target.node_id} AS edge
ORDER BY edge.relation_id LIMIT $limit`, map[string]any{"revision_id": revisionID, "ids": ids, "relation_types": relationTypeStrings(relationTypes), "limit": maxEdges + 1})
	if err != nil {
		return nil, err
	}
	edges := make([]graph.Relation, 0)
	for result.Next(ctx) {
		properties, _ := result.Record().AsMap()["edge"].(map[string]any)
		edge, err := relationFromProperties(properties)
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	if len(edges) > maxEdges {
		return nil, queryBudgetError("query_revision", "result edge count exceeds the configured query budget")
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].EdgeID < edges[j].EdgeID })
	return edges, nil
}

func entityFromProperties(values map[string]any) (graph.Entity, error) {
	nodeID, _ := values["node_id"].(string)
	entityType, _ := values["entity_type"].(string)
	name, _ := values["name"].(string)
	if nodeID == "" || entityType == "" || name == "" {
		return graph.Entity{}, &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "decode_node", Stage: "query", Dependency: "neo4j", Message: "sealed graph contains an invalid node", Retryable: true}
	}
	properties := map[string]string{}
	if raw, _ := values["attributes_json"].(string); raw != "" {
		if err := json.Unmarshal([]byte(raw), &properties); err != nil {
			return graph.Entity{}, &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "decode_node", Stage: "query", Dependency: "neo4j", Message: "sealed graph contains invalid node attributes", Retryable: true, Cause: err}
		}
	}
	if externalID, _ := values["external_id"].(string); externalID != "" {
		properties["external_identity"] = externalID
	}
	entity := graph.Entity{NodeID: nodeID, EntityType: graph.EntityType(entityType), ArtifactID: stringValue(values["artifact_id"]), Name: name, QualifiedName: stringValue(values["qualified_name"]), Properties: properties}
	if path := stringValue(values["source_path"]); path != "" {
		entity.SourceRef = &common.SourceRef{CommitSHA: stringValue(values["source_commit_sha"]), Path: path, SymbolID: stringValue(values["source_symbol_id"]), StartLine: intValue(values["source_start_line"]), EndLine: intValue(values["source_end_line"]), ContentHash: stringValue(values["source_content_hash"])}
	}
	return entity, nil
}

func relationFromProperties(values map[string]any) (graph.Relation, error) {
	id, relationType := stringValue(values["relation_id"]), stringValue(values["relation_type"])
	from, to := stringValue(values["from_node_id"]), stringValue(values["to_node_id"])
	if id == "" || relationType == "" || from == "" || to == "" {
		return graph.Relation{}, &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "decode_edge", Stage: "query", Dependency: "neo4j", Message: "sealed graph contains an invalid relationship", Retryable: true}
	}
	confidence, _ := values["confidence"].(float64)
	return graph.Relation{EdgeID: id, RelationType: repository.RelationKind(relationType), FromNodeID: from, ToNodeID: to, Confidence: confidence,
		Evidence: common.SourceRef{CommitSHA: stringValue(values["evidence_commit_sha"]), Path: stringValue(values["evidence_path"]), SymbolID: stringValue(values["evidence_symbol_id"]), StartLine: intValue(values["evidence_start_line"]), EndLine: intValue(values["evidence_end_line"]), ContentHash: stringValue(values["evidence_content_hash"])}}, nil
}

func collectIDs(ctx context.Context, result neo4jdriver.Result) ([]string, error) {
	ids := make([]string, 0)
	for result.Next(ctx) {
		if id, _ := result.Record().Get("node_id"); id != nil {
			ids = append(ids, fmt.Sprint(id))
		}
	}
	return ids, result.Err()
}

func uniqueSorted(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		set[value] = struct{}{}
	}
	return sortedKeys(set)
}

func entityTypeStrings(values []graph.EntityType) []string {
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = string(value)
	}
	return out
}

func relationTypeStrings(values []repository.RelationKind) []string {
	out := make([]string, len(values))
	for index, value := range values {
		out[index] = string(value)
	}
	return out
}

func stringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
