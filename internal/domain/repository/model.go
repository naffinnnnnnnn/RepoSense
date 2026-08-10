package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

type ParseScope string

const (
	ScopeFull        ParseScope = "FULL"
	ScopeIncremental ParseScope = "INCREMENTAL"
)

type ChangeKind string

const (
	ChangeAdded    ChangeKind = "ADDED"
	ChangeModified ChangeKind = "MODIFIED"
	ChangeDeleted  ChangeKind = "DELETED"
	ChangeRenamed  ChangeKind = "RENAMED"
)

type ChangedPath struct {
	Path    string     `json:"path"`
	OldPath string     `json:"old_path,omitempty"`
	Kind    ChangeKind `json:"kind"`
}

type SyncCommand struct {
	Scope          common.Scope `json:"scope"`
	RepositoryPath string       `json:"repository_path"`
	Provider       string       `json:"provider"`
	Ref            string       `json:"ref"`
	CredentialsRef string       `json:"credentials_ref,omitempty"`
	IncludePaths   []string     `json:"include_paths,omitempty"`
	IdempotencyKey string       `json:"idempotency_key"`
}

func (c SyncCommand) Validate() error {
	if err := c.Scope.Validate(false); err != nil {
		return err
	}
	if strings.TrimSpace(c.RepositoryPath) == "" || strings.TrimSpace(c.Ref) == "" {
		return fmt.Errorf("repository_path 和 ref 不能为空")
	}
	if c.Provider != "" && c.Provider != "local" {
		return fmt.Errorf("本地适配器不支持 provider %q", c.Provider)
	}
	if strings.TrimSpace(c.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key 不能为空")
	}
	return nil
}

type Snapshot struct {
	common.EntityMeta
	SnapshotID       string        `json:"snapshot_id"`
	Provider         string        `json:"provider"`
	Ref              string        `json:"ref"`
	CommitSHA        string        `json:"commit_sha"`
	ParentSnapshotID string        `json:"parent_snapshot_id,omitempty"`
	SyncStatus       Status        `json:"sync_status"`
	ChangedPaths     []ChangedPath `json:"changed_paths"`
	ErrorCode        string        `json:"error_code,omitempty"`
	ErrorMessage     string        `json:"error_message,omitempty"`
	RetryCount       int           `json:"retry_count"`
}

type ParseJob struct {
	common.EntityMeta
	JobID         string     `json:"job_id"`
	SnapshotID    string     `json:"snapshot_id"`
	ParserVersion string     `json:"parser_version"`
	Scope         ParseScope `json:"scope"`
	Status        Status     `json:"job_status"`
	Progress      int        `json:"progress"`
	ErrorCode     string     `json:"error_code,omitempty"`
	ErrorMessage  string     `json:"error_message,omitempty"`
	RetryCount    int        `json:"retry_count"`
}

type ArtifactKind string

const (
	ArtifactFile      ArtifactKind = "FILE"
	ArtifactModule    ArtifactKind = "MODULE"
	ArtifactClass     ArtifactKind = "CLASS"
	ArtifactInterface ArtifactKind = "INTERFACE"
	ArtifactFunction  ArtifactKind = "FUNCTION"
	ArtifactMethod    ArtifactKind = "METHOD"
	ArtifactImport    ArtifactKind = "IMPORT"
	ArtifactConfig    ArtifactKind = "CONFIG"
	ArtifactDocument  ArtifactKind = "DOCUMENT"
)

type CodeArtifact struct {
	ArtifactID    string            `json:"artifact_id"`
	Kind          ArtifactKind      `json:"kind"`
	Name          string            `json:"name"`
	QualifiedName string            `json:"qualified_name"`
	Language      string            `json:"language"`
	SourceRef     common.SourceRef  `json:"source_ref"`
	Signature     string            `json:"signature,omitempty"`
	ContentHash   string            `json:"content_hash"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

type RelationKind string

const (
	RelationContains   RelationKind = "CONTAINS"
	RelationImports    RelationKind = "IMPORTS"
	RelationCalls      RelationKind = "CALLS"
	RelationExtends    RelationKind = "EXTENDS"
	RelationImplements RelationKind = "IMPLEMENTS"
	RelationDependsOn  RelationKind = "DEPENDS_ON"
)

type CodeRelation struct {
	RelationID string           `json:"relation_id"`
	Kind       RelationKind     `json:"kind"`
	From       string           `json:"from"`
	To         string           `json:"to"`
	Evidence   common.SourceRef `json:"evidence"`
	Confidence float64          `json:"confidence"`
}

type ParseResult struct {
	Snapshot     Snapshot             `json:"snapshot"`
	Job          ParseJob             `json:"job"`
	Artifacts    []CodeArtifact       `json:"artifacts"`
	Relations    []CodeRelation       `json:"relations"`
	DeletedPaths []string             `json:"deleted_paths,omitempty"`
	SkippedFiles []SkippedFile        `json:"skipped_files,omitempty"`
	Event        common.EventEnvelope `json:"event"`
}

type SkippedFile struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type FileContent struct {
	Path    string
	Content []byte
}

type ParsedFile struct {
	Artifacts []CodeArtifact
	Relations []CodeRelation
}

func NewMeta(id string, scope common.Scope, status Status, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: string(status), Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: "svc_parser", TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}
