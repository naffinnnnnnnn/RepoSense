package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type BuildMode string

const (
	BuildFull        BuildMode = "FULL"
	BuildIncremental BuildMode = "INCREMENTAL"
)

type RevisionStatus string

const (
	RevisionBuilding RevisionStatus = "BUILDING"
	RevisionActive   RevisionStatus = "ACTIVE"
	RevisionFailed   RevisionStatus = "FAILED"
)

type EntityType string

const (
	EntityRepository EntityType = "REPOSITORY"
	EntityModule     EntityType = "MODULE"
	EntityFile       EntityType = "FILE"
	EntityClass      EntityType = "CLASS"
	EntityInterface  EntityType = "INTERFACE"
	EntityFunction   EntityType = "FUNCTION"
	EntityMethod     EntityType = "METHOD"
	EntityImport     EntityType = "IMPORT"
	EntityConfig     EntityType = "CONFIG"
	EntityDocument   EntityType = "DOCUMENT"
	EntityPackage    EntityType = "PACKAGE"
	EntitySymbol     EntityType = "SYMBOL"
)

type Direction string

const (
	DirectionBoth     Direction = "BOTH"
	DirectionOutgoing Direction = "OUTGOING"
	DirectionIncoming Direction = "INCOMING"
)

type BuildCommand struct {
	Scope          common.Scope `json:"scope"`
	ArtifactIDs    []string     `json:"artifact_ids,omitempty"`
	Mode           BuildMode    `json:"mode"`
	IdempotencyKey string       `json:"idempotency_key"`
}

func (c BuildCommand) Validate() error {
	if err := c.Scope.Validate(true); err != nil {
		return err
	}
	if c.Mode != BuildFull && c.Mode != BuildIncremental {
		return fmt.Errorf("mode must be FULL or INCREMENTAL")
	}
	if strings.TrimSpace(c.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key must not be empty")
	}
	seen := make(map[string]struct{}, len(c.ArtifactIDs))
	for _, id := range c.ArtifactIDs {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("artifact_ids must not contain empty values")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate artifact_id %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

type Entity struct {
	NodeID        string            `json:"node_id"`
	EntityType    EntityType        `json:"entity_type"`
	ArtifactID    string            `json:"artifact_id,omitempty"`
	Name          string            `json:"name"`
	QualifiedName string            `json:"qualified_name,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
	SourceRef     *common.SourceRef `json:"source_ref,omitempty"`
	ValidFrom     string            `json:"valid_from"`
	ValidTo       string            `json:"valid_to,omitempty"`
}

type Relation struct {
	EdgeID       string                  `json:"edge_id"`
	RelationType repository.RelationKind `json:"relation_type"`
	FromNodeID   string                  `json:"from_node_id"`
	ToNodeID     string                  `json:"to_node_id"`
	Evidence     common.SourceRef        `json:"evidence"`
	Confidence   float64                 `json:"confidence"`
	Properties   map[string]string       `json:"properties,omitempty"`
}

type RevisionStats struct {
	Nodes              int `json:"nodes"`
	Edges              int `json:"edges"`
	UnresolvedTargets  int `json:"unresolved_targets"`
	AmbiguousRelations int `json:"ambiguous_relations"`
}

type Revision struct {
	common.EntityMeta
	RevisionID       string               `json:"revision_id"`
	SnapshotID       string               `json:"snapshot_id"`
	ParentRevisionID string               `json:"parent_revision_id,omitempty"`
	CommitSHA        string               `json:"commit_sha"`
	BuildMode        BuildMode            `json:"build_mode"`
	BuildStatus      RevisionStatus       `json:"build_status"`
	AlgorithmVersion string               `json:"algorithm_version"`
	Stats            RevisionStats        `json:"stats"`
	Nodes            []Entity             `json:"-"`
	Edges            []Relation           `json:"-"`
	PublishedEvent   common.EventEnvelope `json:"-"`
}

type Query struct {
	Scope         common.Scope              `json:"scope"`
	RootIDs       []string                  `json:"root_ids,omitempty"`
	RelationTypes []repository.RelationKind `json:"relation_types,omitempty"`
	EntityTypes   []EntityType              `json:"entity_types,omitempty"`
	Direction     Direction                 `json:"direction,omitempty"`
	Depth         int                       `json:"depth"`
	Limit         int                       `json:"limit"`
}

func (q Query) Validate() error {
	if err := q.Scope.Validate(true); err != nil {
		return err
	}
	if q.Depth < 0 || q.Depth > 10 {
		return fmt.Errorf("depth must be between 0 and 10")
	}
	if q.Limit < 0 || q.Limit > 10_000 {
		return fmt.Errorf("limit must be between 0 and 10000")
	}
	if q.Direction != "" && q.Direction != DirectionBoth && q.Direction != DirectionIncoming && q.Direction != DirectionOutgoing {
		return fmt.Errorf("invalid direction %q", q.Direction)
	}
	return nil
}

type Diagnostics struct {
	RevisionID string `json:"revision_id"`
	Visited    int    `json:"visited"`
	Truncated  bool   `json:"truncated"`
	DurationMS int64  `json:"duration_ms"`
}

type Result struct {
	Nodes       []Entity    `json:"nodes"`
	Edges       []Relation  `json:"edges"`
	Diagnostics Diagnostics `json:"diagnostics"`
}

type BuildInput struct {
	Snapshot     repository.Snapshot
	Artifacts    []repository.CodeArtifact
	Relations    []repository.CodeRelation
	DeletedPaths []string
}

func EntityTypeFor(kind repository.ArtifactKind) EntityType { return EntityType(kind) }

func (r *Revision) Normalize() {
	sort.Slice(r.Nodes, func(i, j int) bool { return r.Nodes[i].NodeID < r.Nodes[j].NodeID })
	sort.Slice(r.Edges, func(i, j int) bool { return r.Edges[i].EdgeID < r.Edges[j].EdgeID })
	r.Stats.Nodes, r.Stats.Edges = len(r.Nodes), len(r.Edges)
}

func NewMeta(id string, scope common.Scope, status RevisionStatus, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: string(status), Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: "svc_knowledge_graph", TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}
