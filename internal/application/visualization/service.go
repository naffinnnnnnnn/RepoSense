package visualizationapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/visualization"
	"github.com/reposense/reposense/internal/ports"
)

const ProjectionAlgorithmVersion = "graph-projection@1"

type Config struct {
	CacheTTL time.Duration
}

func DefaultConfig() Config { return Config{CacheTTL: 15 * time.Minute} }

type Service struct {
	graph    ports.GraphStore
	repo     ports.VisualizationRepository
	layout   ports.LayoutEngine
	observer ports.Observer
	ids      ports.IDGenerator
	clock    ports.Clock
	config   Config
}

func New(graphStore ports.GraphStore, repo ports.VisualizationRepository, layout ports.LayoutEngine, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if graphStore == nil {
		return nil, errors.New("graph store must not be nil")
	}
	if repo == nil {
		repo = noCacheRepository{}
	}
	if layout == nil {
		layout = DeterministicLayout{}
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = randomIDs{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	if config.CacheTTL <= 0 {
		config = DefaultConfig()
	}
	return &Service{graph: graphStore, repo: repo, layout: layout, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (s *Service) Project(ctx context.Context, raw visualization.Query) (projection visualization.Projection, err error) {
	started := s.clock.Now()
	if err := raw.Validate(); err != nil {
		return projection, domainError(visualization.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	query := raw.Normalize()
	queryHash, err := hashQuery(query)
	if err != nil {
		return projection, domainError(visualization.ErrProjectionFailure, "hash_query", "failed to hash visualization query", false, err)
	}
	finish := s.observer.Stage(ctx, "visualization_project", labels(query.Scope, query.ViewType))
	defer func() { finish(err) }()

	cacheWarnings := []string(nil)
	if cached, ok, cacheErr := s.repo.Get(ctx, query.Scope, queryHash); cacheErr != nil {
		cacheWarnings = append(cacheWarnings, "projection cache read failed; result was rebuilt")
		s.observer.Count("visualization_cache_errors_total", 1, labels(query.Scope, query.ViewType))
	} else if ok && cached.GraphRevisionID == query.GraphRevisionID {
		cached.Diagnostics.CacheHit = true
		cached.Diagnostics.DurationMS = elapsedMS(started, s.clock.Now())
		s.observer.Count("visualization_cache_hits_total", 1, labels(query.Scope, query.ViewType))
		return cached, nil
	}

	entityTypes, relationTypes, warnings := viewProfile(query)
	warnings = append(warnings, cacheWarnings...)
	graphResult, queryErr := s.graph.Query(ctx, graph.Query{
		Scope: query.Scope, RootIDs: query.RootIDs, RelationTypes: relationTypes, EntityTypes: entityTypes,
		Direction: query.Filters.Direction, Depth: query.Depth, Limit: query.Limit,
	})
	if queryErr != nil {
		if graph.IsCode(queryErr, graph.ErrRevisionNotFound) {
			return projection, domainError(visualization.ErrGraphNotFound, "query_graph", "graph revision not found", false, queryErr)
		}
		if graph.IsCode(queryErr, graph.ErrInvalidInput) {
			return projection, domainError(visualization.ErrInvalidInput, "query_graph", queryErr.Error(), false, queryErr)
		}
		return projection, domainError(visualization.ErrProjectionFailure, "query_graph", "failed to query graph", true, queryErr)
	}
	if graphResult.Diagnostics.RevisionID != query.GraphRevisionID {
		return projection, domainError(visualization.ErrRevisionMismatch, "verify_revision", "requested graph revision does not match snapshot's published revision", false, nil)
	}

	nodes, edges, sourceLinks, filterWarnings := convertAndFilter(graphResult, query.Filters)
	warnings = append(warnings, filterWarnings...)
	if len(query.RootIDs) > 0 {
		var rootsFound bool
		nodes, edges, sourceLinks, rootsFound = pruneToRoots(nodes, edges, sourceLinks, query.RootIDs, query.Filters.Direction, query.Depth)
		if !rootsFound {
			warnings = append(warnings, "all requested roots were removed by visualization filters")
		}
	}
	layoutStarted := s.clock.Now()
	layoutResult, layoutErr := s.layout.Layout(ctx, query.Layout, nodes, edges)
	if layoutErr != nil {
		return projection, domainError(visualization.ErrProjectionFailure, "layout", "failed to layout projection", true, layoutErr)
	}
	now := s.clock.Now().UTC()
	projection = visualization.Projection{
		ProjectionID: s.ids.New("proj"), GraphRevisionID: query.GraphRevisionID, SnapshotID: query.Scope.SnapshotID,
		ViewType: query.ViewType, Theme: query.Theme, Nodes: nodes, Edges: edges, Layout: layoutResult,
		SourceLinks: sourceLinks, RenderStatus: "READY", CreatedAt: now, ExpiresAt: now.Add(s.config.CacheTTL),
		Diagnostics: visualization.Diagnostics{QueryHash: queryHash, GraphVisited: graphResult.Diagnostics.Visited,
			InputNodes: len(graphResult.Nodes), InputEdges: len(graphResult.Edges), OutputNodes: len(nodes), OutputEdges: len(edges),
			Truncated: graphResult.Diagnostics.Truncated, DurationMS: elapsedMS(started, now),
			LayoutDurationMS: elapsedMS(layoutStarted, now), Warnings: uniqueSorted(warnings)},
	}
	if query.IncludeMermaid {
		projection.Mermaid = renderMermaid(nodes, edges)
	}
	if cacheErr := s.repo.Save(ctx, query.Scope, queryHash, projection); cacheErr != nil {
		projection.Diagnostics.Warnings = uniqueSorted(append(projection.Diagnostics.Warnings, "projection cache write failed; result was returned without caching"))
		s.observer.Count("visualization_cache_errors_total", 1, labels(query.Scope, query.ViewType))
	}
	s.observer.Count("visualization_nodes_total", int64(len(nodes)), labels(query.Scope, query.ViewType))
	s.observer.Count("visualization_edges_total", int64(len(edges)), labels(query.Scope, query.ViewType))
	return projection, nil
}

func viewProfile(q visualization.Query) ([]graph.EntityType, []repository.RelationKind, []string) {
	var entities []graph.EntityType
	var relations []repository.RelationKind
	warnings := []string(nil)
	switch q.ViewType {
	case visualization.ViewModuleDependency:
		entities = []graph.EntityType{graph.EntityModule, graph.EntityPackage, graph.EntityFile}
		relations = []repository.RelationKind{repository.RelationImports, repository.RelationDependsOn}
	case visualization.ViewClassDiagram:
		entities = []graph.EntityType{graph.EntityClass, graph.EntityInterface}
		relations = []repository.RelationKind{repository.RelationExtends, repository.RelationImplements}
	case visualization.ViewCallGraph:
		entities = []graph.EntityType{graph.EntityFunction, graph.EntityMethod, graph.EntitySymbol}
		relations = []repository.RelationKind{repository.RelationCalls}
	case visualization.ViewDataFlow:
		entities = []graph.EntityType{graph.EntityFunction, graph.EntityMethod, graph.EntitySymbol, graph.EntityConfig}
		relations = []repository.RelationKind{repository.RelationCalls, repository.RelationDependsOn}
		warnings = append(warnings, "DATA_FLOW currently shows only extracted CALLS and DEPENDS_ON relations; variable-level flow is not available in Code IR")
	}
	if len(q.Filters.EntityTypes) > 0 {
		entities = intersectEntityTypes(entities, q.Filters.EntityTypes)
		if len(entities) == 0 {
			entities = []graph.EntityType{graph.EntityType("__NO_ENTITY_MATCH__")}
		}
	}
	if len(q.Filters.RelationTypes) > 0 {
		relations = intersectRelationTypes(relations, q.Filters.RelationTypes)
		if len(relations) == 0 {
			relations = []repository.RelationKind{repository.RelationKind("__NO_RELATION_MATCH__")}
		}
	}
	return entities, relations, warnings
}

func convertAndFilter(result graph.Result, filters visualization.Filters) ([]visualization.Node, []visualization.Edge, map[string]visualization.SourceLink, []string) {
	allowedNodes := make(map[string]bool, len(result.Nodes))
	sourceLinks := make(map[string]visualization.SourceLink)
	nodes := make([]visualization.Node, 0, len(result.Nodes))
	warnings := []string(nil)
	for _, item := range result.Nodes {
		if len(filters.Languages) > 0 {
			language := entityLanguage(item)
			if language == "" {
				warnings = append(warnings, "some nodes could not be evaluated by language because language metadata was unavailable")
				continue
			}
			if !containsString(filters.Languages, language) {
				continue
			}
		}
		allowedNodes[item.NodeID] = true
		nodes = append(nodes, visualization.Node{ID: item.NodeID, Label: item.Name, EntityType: item.EntityType,
			ArtifactID: item.ArtifactID, QualifiedName: item.QualifiedName, Properties: cloneMap(item.Properties)})
		if item.SourceRef != nil {
			if err := item.SourceRef.Validate(); err == nil {
				sourceLinks[item.NodeID] = sourceLink(*item.SourceRef)
			} else {
				warnings = append(warnings, "some invalid node source references were omitted")
			}
		}
	}
	edges := make([]visualization.Edge, 0, len(result.Edges))
	for _, item := range result.Edges {
		if !allowedNodes[item.FromNodeID] || !allowedNodes[item.ToNodeID] || item.Confidence < filters.MinConfidence {
			continue
		}
		edges = append(edges, visualization.Edge{ID: item.EdgeID, Source: item.FromNodeID, Target: item.ToNodeID,
			Label: string(item.RelationType), Type: item.RelationType, Confidence: item.Confidence, Properties: cloneMap(item.Properties)})
		if item.Evidence.Path != "" {
			if err := item.Evidence.Validate(); err == nil {
				sourceLinks[item.EdgeID] = sourceLink(item.Evidence)
			} else {
				warnings = append(warnings, "some invalid edge source references were omitted")
			}
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return nodes, edges, sourceLinks, warnings
}

func pruneToRoots(nodes []visualization.Node, edges []visualization.Edge, links map[string]visualization.SourceLink, rootIDs []string, direction graph.Direction, depth int) ([]visualization.Node, []visualization.Edge, map[string]visualization.SourceLink, bool) {
	selected := make(map[string]bool, len(nodes))
	frontier := []string(nil)
	rootSet := make(map[string]bool, len(rootIDs))
	for _, root := range rootIDs {
		rootSet[root] = true
	}
	for _, node := range nodes {
		if rootSet[node.ID] || rootSet[node.ArtifactID] {
			selected[node.ID] = true
			frontier = append(frontier, node.ID)
		}
	}
	rootsFound := len(frontier) > 0
	for level := 0; level < depth && len(frontier) > 0; level++ {
		next := []string(nil)
		for _, edge := range edges {
			for _, current := range frontier {
				neighbor := ""
				if (direction == graph.DirectionBoth || direction == graph.DirectionOutgoing) && edge.Source == current {
					neighbor = edge.Target
				}
				if (direction == graph.DirectionBoth || direction == graph.DirectionIncoming) && edge.Target == current {
					neighbor = edge.Source
				}
				if neighbor != "" && !selected[neighbor] {
					selected[neighbor] = true
					next = append(next, neighbor)
				}
			}
		}
		frontier = next
	}
	filteredNodes := make([]visualization.Node, 0, len(selected))
	filteredLinks := make(map[string]visualization.SourceLink)
	for _, node := range nodes {
		if selected[node.ID] {
			filteredNodes = append(filteredNodes, node)
			if link, ok := links[node.ID]; ok {
				filteredLinks[node.ID] = link
			}
		}
	}
	filteredEdges := make([]visualization.Edge, 0, len(edges))
	for _, edge := range edges {
		if selected[edge.Source] && selected[edge.Target] {
			filteredEdges = append(filteredEdges, edge)
			if link, ok := links[edge.ID]; ok {
				filteredLinks[edge.ID] = link
			}
		}
	}
	return filteredNodes, filteredEdges, filteredLinks, rootsFound
}

func renderMermaid(nodes []visualization.Node, edges []visualization.Edge) string {
	var out strings.Builder
	out.WriteString("flowchart LR\n")
	for _, node := range nodes {
		fmt.Fprintf(&out, "  %s[\"%s\"]\n", mermaidID(node.ID), escapeMermaid(node.Label))
	}
	for _, edge := range edges {
		fmt.Fprintf(&out, "  %s -->|%s| %s\n", mermaidID(edge.Source), escapeMermaid(edge.Label), mermaidID(edge.Target))
	}
	return out.String()
}

func mermaidID(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "n" + hex.EncodeToString(digest[:6])
}

func escapeMermaid(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "\"", "&quot;", "<", "&lt;", ">", "&gt;", "[", "&#91;", "]", "&#93;", "|", "&#124;", "{", "&#123;", "}", "&#125;", "\n", " ", "\r", " ")
	return replacer.Replace(value)
}

func hashQuery(query visualization.Query) (string, error) {
	// Trace IDs correlate one request and must not fragment otherwise identical
	// projection cache entries.
	query.Scope.TraceID = ""
	encoded, err := json.Marshal(query)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func entityLanguage(entity graph.Entity) string {
	if language := strings.ToLower(strings.TrimSpace(entity.Properties["language"])); language != "" {
		return language
	}
	if entity.SourceRef == nil {
		return ""
	}
	switch strings.ToLower(filepath.Ext(entity.SourceRef.Path)) {
	case ".go":
		return "go"
	case ".py":
		return "python"
	case ".ts", ".tsx":
		return "typescript"
	case ".js", ".jsx":
		return "javascript"
	case ".java":
		return "java"
	default:
		return ""
	}
}

func sourceLink(ref common.SourceRef) visualization.SourceLink {
	return visualization.SourceLink{CommitSHA: ref.CommitSHA, Path: ref.Path, SymbolID: ref.SymbolID, StartLine: ref.StartLine, EndLine: ref.EndLine}
}

func intersectEntityTypes(defaults, requested []graph.EntityType) []graph.EntityType {
	if defaults == nil {
		return append([]graph.EntityType(nil), requested...)
	}
	allowed := make(map[graph.EntityType]bool, len(defaults))
	for _, value := range defaults {
		allowed[value] = true
	}
	result := make([]graph.EntityType, 0, len(requested))
	for _, value := range requested {
		if allowed[value] {
			result = append(result, value)
		}
	}
	return result
}

func intersectRelationTypes(defaults, requested []repository.RelationKind) []repository.RelationKind {
	if defaults == nil {
		return append([]repository.RelationKind(nil), requested...)
	}
	allowed := make(map[repository.RelationKind]bool, len(defaults))
	for _, value := range defaults {
		allowed[value] = true
	}
	result := make([]repository.RelationKind, 0, len(requested))
	for _, value := range requested {
		if allowed[value] {
			result = append(result, value)
		}
	}
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func cloneMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
func uniqueSorted(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
func elapsedMS(start, end time.Time) int64 {
	value := end.Sub(start).Milliseconds()
	if value < 0 {
		return 0
	}
	return value
}
func labels(scope common.Scope, view visualization.ViewType) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID, "view_type": string(view)}
}
func domainError(code visualization.ErrorCode, operation, message string, retryable bool, cause error) error {
	return &visualization.DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
}

type noCacheRepository struct{}

func (noCacheRepository) Get(context.Context, common.Scope, string) (visualization.Projection, bool, error) {
	return visualization.Projection{}, false, nil
}
func (noCacheRepository) Save(context.Context, common.Scope, string, visualization.Projection) error {
	return nil
}

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type randomIDs struct{}

func (randomIDs) New(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}
