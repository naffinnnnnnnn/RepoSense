package visualization

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type ViewType string

const (
	ViewRepositoryMap    ViewType = "REPOSITORY_MAP"
	ViewModuleDependency ViewType = "MODULE_DEPENDENCY"
	ViewClassDiagram     ViewType = "CLASS_DIAGRAM"
	ViewCallGraph        ViewType = "CALL_GRAPH"
	ViewDataFlow         ViewType = "DATA_FLOW"
)

type LayoutType string

const (
	LayoutDAG    LayoutType = "DAG"
	LayoutGrid   LayoutType = "GRID"
	LayoutRadial LayoutType = "RADIAL"
)

type Theme string

const (
	ThemeLight Theme = "light"
	ThemeDark  Theme = "dark"
)

type Filters struct {
	EntityTypes   []graph.EntityType        `json:"entity_types,omitempty"`
	RelationTypes []repository.RelationKind `json:"relation_types,omitempty"`
	Direction     graph.Direction           `json:"direction,omitempty"`
	MinConfidence float64                   `json:"min_confidence,omitempty"`
	Languages     []string                  `json:"languages,omitempty"`
}

type Query struct {
	Scope           common.Scope `json:"scope"`
	GraphRevisionID string       `json:"graph_revision_id"`
	ViewType        ViewType     `json:"view_type"`
	RootIDs         []string     `json:"root_ids,omitempty"`
	Depth           int          `json:"depth"`
	Filters         Filters      `json:"filters,omitempty"`
	Layout          LayoutType   `json:"layout,omitempty"`
	Theme           Theme        `json:"theme,omitempty"`
	Limit           int          `json:"limit,omitempty"`
	IncludeMermaid  bool         `json:"include_mermaid,omitempty"`
}

func (q Query) Validate() error {
	if err := q.Scope.Validate(true); err != nil {
		return err
	}
	if strings.TrimSpace(q.GraphRevisionID) == "" {
		return fmt.Errorf("graph_revision_id must not be empty")
	}
	if !validView(q.ViewType) {
		return fmt.Errorf("invalid view_type %q", q.ViewType)
	}
	if q.Depth < 0 || q.Depth > 10 {
		return fmt.Errorf("depth must be between 0 and 10")
	}
	if q.Limit < 0 || q.Limit > 5_000 {
		return fmt.Errorf("limit must be between 0 and 5000")
	}
	if q.Layout != "" && q.Layout != LayoutDAG && q.Layout != LayoutGrid && q.Layout != LayoutRadial {
		return fmt.Errorf("invalid layout %q", q.Layout)
	}
	if q.Theme != "" && q.Theme != ThemeLight && q.Theme != ThemeDark {
		return fmt.Errorf("invalid theme %q", q.Theme)
	}
	if q.Filters.Direction != "" && q.Filters.Direction != graph.DirectionBoth && q.Filters.Direction != graph.DirectionIncoming && q.Filters.Direction != graph.DirectionOutgoing {
		return fmt.Errorf("invalid direction %q", q.Filters.Direction)
	}
	if q.Filters.MinConfidence < 0 || q.Filters.MinConfidence > 1 {
		return fmt.Errorf("min_confidence must be between 0 and 1")
	}
	if err := validateUniqueStrings("root_ids", q.RootIDs); err != nil {
		return err
	}
	languages := make([]string, len(q.Filters.Languages))
	for i, language := range q.Filters.Languages {
		languages[i] = strings.ToLower(strings.TrimSpace(language))
	}
	if err := validateUniqueStrings("languages", languages); err != nil {
		return err
	}
	seenEntities := make(map[graph.EntityType]bool, len(q.Filters.EntityTypes))
	for _, value := range q.Filters.EntityTypes {
		if !validEntityType(value) {
			return fmt.Errorf("invalid entity_type %q", value)
		}
		if seenEntities[value] {
			return fmt.Errorf("entity_types must not contain duplicate value %q", value)
		}
		seenEntities[value] = true
	}
	seenRelations := make(map[repository.RelationKind]bool, len(q.Filters.RelationTypes))
	for _, value := range q.Filters.RelationTypes {
		if !validRelationType(value) {
			return fmt.Errorf("invalid relation_type %q", value)
		}
		if seenRelations[value] {
			return fmt.Errorf("relation_types must not contain duplicate value %q", value)
		}
		seenRelations[value] = true
	}
	return nil
}

// Normalize applies stable defaults before hashing, querying, and caching.
func (q Query) Normalize() Query {
	q.RootIDs = append([]string(nil), q.RootIDs...)
	q.Filters.EntityTypes = append([]graph.EntityType(nil), q.Filters.EntityTypes...)
	q.Filters.RelationTypes = append([]repository.RelationKind(nil), q.Filters.RelationTypes...)
	q.Filters.Languages = append([]string(nil), q.Filters.Languages...)
	if q.Limit == 0 {
		q.Limit = 500
	}
	if q.Layout == "" {
		if q.ViewType == ViewRepositoryMap {
			q.Layout = LayoutGrid
		} else {
			q.Layout = LayoutDAG
		}
	}
	if q.Theme == "" {
		q.Theme = ThemeLight
	}
	if q.Filters.Direction == "" {
		q.Filters.Direction = graph.DirectionBoth
	}
	for i := range q.Filters.Languages {
		q.Filters.Languages[i] = strings.ToLower(strings.TrimSpace(q.Filters.Languages[i]))
	}
	sort.Strings(q.RootIDs)
	sort.Slice(q.Filters.EntityTypes, func(i, j int) bool { return q.Filters.EntityTypes[i] < q.Filters.EntityTypes[j] })
	sort.Slice(q.Filters.RelationTypes, func(i, j int) bool { return q.Filters.RelationTypes[i] < q.Filters.RelationTypes[j] })
	sort.Strings(q.Filters.Languages)
	return q
}

type Node struct {
	ID            string            `json:"id"`
	Label         string            `json:"label"`
	EntityType    graph.EntityType  `json:"entity_type"`
	ArtifactID    string            `json:"artifact_id,omitempty"`
	QualifiedName string            `json:"qualified_name,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
}

type Edge struct {
	ID         string                  `json:"id"`
	Source     string                  `json:"source"`
	Target     string                  `json:"target"`
	Label      string                  `json:"label"`
	Type       repository.RelationKind `json:"type"`
	Confidence float64                 `json:"confidence"`
	Properties map[string]string       `json:"properties,omitempty"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Layout struct {
	Algorithm        LayoutType          `json:"algorithm"`
	AlgorithmVersion string              `json:"algorithm_version"`
	Positions        map[string]Position `json:"positions"`
}

type SourceLink struct {
	CommitSHA string `json:"commit_sha"`
	Path      string `json:"path"`
	SymbolID  string `json:"symbol_id,omitempty"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

type Diagnostics struct {
	QueryHash        string   `json:"query_hash"`
	GraphVisited     int      `json:"graph_visited"`
	InputNodes       int      `json:"input_nodes"`
	InputEdges       int      `json:"input_edges"`
	OutputNodes      int      `json:"output_nodes"`
	OutputEdges      int      `json:"output_edges"`
	Truncated        bool     `json:"truncated"`
	CacheHit         bool     `json:"cache_hit"`
	DurationMS       int64    `json:"duration_ms"`
	LayoutDurationMS int64    `json:"layout_duration_ms"`
	Warnings         []string `json:"warnings,omitempty"`
}

type Projection struct {
	ProjectionID    string                `json:"projection_id"`
	GraphRevisionID string                `json:"graph_revision_id"`
	SnapshotID      string                `json:"snapshot_id"`
	ViewType        ViewType              `json:"view_type"`
	Theme           Theme                 `json:"theme"`
	Nodes           []Node                `json:"nodes"`
	Edges           []Edge                `json:"edges"`
	Layout          Layout                `json:"layout"`
	SourceLinks     map[string]SourceLink `json:"source_links"`
	Mermaid         string                `json:"mermaid,omitempty"`
	RenderStatus    string                `json:"render_status"`
	CreatedAt       time.Time             `json:"created_at"`
	ExpiresAt       time.Time             `json:"expires_at"`
	Diagnostics     Diagnostics           `json:"diagnostics"`
}

func validView(view ViewType) bool {
	switch view {
	case ViewRepositoryMap, ViewModuleDependency, ViewClassDiagram, ViewCallGraph, ViewDataFlow:
		return true
	default:
		return false
	}
}

func validEntityType(value graph.EntityType) bool {
	switch value {
	case graph.EntityRepository, graph.EntityModule, graph.EntityFile, graph.EntityClass, graph.EntityInterface,
		graph.EntityFunction, graph.EntityMethod, graph.EntityImport, graph.EntityConfig, graph.EntityDocument,
		graph.EntityPackage, graph.EntitySymbol:
		return true
	default:
		return false
	}
}

func validRelationType(value repository.RelationKind) bool {
	switch value {
	case repository.RelationContains, repository.RelationImports, repository.RelationCalls, repository.RelationExtends,
		repository.RelationImplements, repository.RelationDependsOn:
		return true
	default:
		return false
	}
}

func validateUniqueStrings(name string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not contain empty values", name)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s must not contain duplicate value %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}
