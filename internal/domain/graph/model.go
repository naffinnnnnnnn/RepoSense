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
	RevisionBuilding   RevisionStatus = "BUILDING"
	RevisionActive     RevisionStatus = "ACTIVE"
	RevisionFailed     RevisionStatus = "FAILED"
	RevisionSuperseded RevisionStatus = "SUPERSEDED"
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
	if hasSurroundingSpace(c.Scope.TenantID) || hasSurroundingSpace(c.Scope.RepositoryID) || hasSurroundingSpace(c.Scope.SnapshotID) {
		return fmt.Errorf("scope identity fields must not contain surrounding whitespace")
	}
	if c.Mode != BuildFull {
		return fmt.Errorf("graph builds only support FULL mode")
	}
	if strings.TrimSpace(c.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key must not be empty")
	}
	if hasSurroundingSpace(c.IdempotencyKey) {
		return fmt.Errorf("idempotency_key must not contain surrounding whitespace")
	}
	if len(c.ArtifactIDs) != 0 {
		return fmt.Errorf("partial graph builds are not supported")
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
	RevisionID          string               `json:"revision_id"`
	SnapshotID          string               `json:"snapshot_id"`
	ParentRevisionID    string               `json:"parent_revision_id,omitempty"`
	CommitSHA           string               `json:"commit_sha"`
	BuildMode           BuildMode            `json:"build_mode"`
	BuildStatus         RevisionStatus       `json:"build_status"`
	AlgorithmVersion    string               `json:"algorithm_version"`
	ParserResultVersion string               `json:"parser_result_version,omitempty"`
	GraphSchemaVersion  string               `json:"graph_schema_version,omitempty"`
	BuildPolicyVersion  string               `json:"build_policy_version,omitempty"`
	QualityStatus       QualityStatus        `json:"quality_status,omitempty"`
	Quality             QualityStats         `json:"quality"`
	RequestFingerprint  string               `json:"request_fingerprint"`
	Stats               RevisionStats        `json:"stats"`
	Nodes               []Entity             `json:"-"`
	Edges               []Relation           `json:"-"`
	PublishedEvent      common.EventEnvelope `json:"-"`
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
	if hasSurroundingSpace(q.Scope.TenantID) || hasSurroundingSpace(q.Scope.RepositoryID) || hasSurroundingSpace(q.Scope.SnapshotID) {
		return fmt.Errorf("scope identity fields must not contain surrounding whitespace")
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
	for _, entityType := range q.EntityTypes {
		if !validEntityType(entityType) {
			return fmt.Errorf("invalid entity type %q", entityType)
		}
	}
	for _, relationType := range q.RelationTypes {
		if !validRelationType(relationType) {
			return fmt.Errorf("invalid relation type %q", relationType)
		}
	}
	for _, rootID := range q.RootIDs {
		if strings.TrimSpace(rootID) == "" || hasSurroundingSpace(rootID) {
			return fmt.Errorf("root_ids must contain exact non-empty artifact ids")
		}
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

type DiagnosticQuery struct {
	Scope       common.Scope `json:"scope"`
	ArtifactIDs []string     `json:"artifact_ids"`
	Limit       int          `json:"limit"`
}

func (q DiagnosticQuery) Validate() error {
	if err := q.Scope.Validate(true); err != nil {
		return err
	}
	if hasSurroundingSpace(q.Scope.TenantID) || hasSurroundingSpace(q.Scope.RepositoryID) || hasSurroundingSpace(q.Scope.SnapshotID) {
		return fmt.Errorf("scope identity fields must not contain surrounding whitespace")
	}
	if len(q.ArtifactIDs) == 0 {
		return fmt.Errorf("at least one artifact_id is required")
	}
	if q.Limit < 0 || q.Limit > 10_000 {
		return fmt.Errorf("limit must be between 0 and 10000")
	}
	for _, artifactID := range q.ArtifactIDs {
		if strings.TrimSpace(artifactID) == "" || hasSurroundingSpace(artifactID) {
			return fmt.Errorf("artifact_ids must not contain empty values")
		}
	}
	return nil
}

type ResolutionIssue struct {
	IssueID              string                  `json:"issue_id"`
	RelationID           string                  `json:"relation_id"`
	SourceArtifactID     string                  `json:"source_artifact_id"`
	IntendedKind         repository.RelationKind `json:"intended_kind"`
	ResolutionStatus     ResolutionStatus        `json:"resolution_status"`
	RawTargetSymbol      string                  `json:"raw_target_symbol"`
	ReasonCode           string                  `json:"reason_code"`
	Confidence           float64                 `json:"confidence"`
	Evidence             common.SourceRef        `json:"evidence"`
	CandidateArtifactIDs []string                `json:"candidate_artifact_ids,omitempty"`
}

type DiagnosticResult struct {
	RevisionID string            `json:"revision_id"`
	Issues     []ResolutionIssue `json:"issues"`
	Truncated  bool              `json:"truncated"`
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

func hasSurroundingSpace(value string) bool { return value != strings.TrimSpace(value) }

func validEntityType(value EntityType) bool {
	switch value {
	case EntityRepository, EntityModule, EntityFile, EntityClass, EntityInterface, EntityFunction, EntityMethod, EntityImport, EntityConfig, EntityDocument, EntityPackage, EntitySymbol:
		return true
	default:
		return false
	}
}

func validRelationType(value repository.RelationKind) bool {
	switch value {
	case repository.RelationContains, repository.RelationImports, repository.RelationCalls, repository.RelationExtends, repository.RelationImplements, repository.RelationDependsOn:
		return true
	default:
		return false
	}
}
