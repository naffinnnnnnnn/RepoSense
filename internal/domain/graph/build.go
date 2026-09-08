package graph

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type JobStatus string

const (
	JobPending   JobStatus = "PENDING"
	JobBuilding  JobStatus = "BUILDING"
	JobSucceeded JobStatus = "SUCCEEDED"
	JobFailed    JobStatus = "FAILED"
	JobCancelled JobStatus = "CANCELLED"
)

type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptLost      AttemptStatus = "LOST"
	AttemptCancelled AttemptStatus = "CANCELLED"
)

type CandidateStatus string

const (
	CandidateStaging CandidateStatus = "STAGING"
	CandidateSealed  CandidateStatus = "SEALED"
	CandidateFailed  CandidateStatus = "FAILED"
)

type QualityStatus string

const (
	QualityHealthy  QualityStatus = "HEALTHY"
	QualityDegraded QualityStatus = "DEGRADED"
)

type ResolutionStatus string

const (
	ResolutionResolved   ResolutionStatus = "RESOLVED"
	ResolutionExternal   ResolutionStatus = "EXTERNAL"
	ResolutionAmbiguous  ResolutionStatus = "AMBIGUOUS"
	ResolutionUnresolved ResolutionStatus = "UNRESOLVED"
)

type BuildVersions struct {
	ParserResultVersion   string `json:"parser_result_version"`
	GraphSchemaVersion    string `json:"graph_schema_version"`
	GraphAlgorithmVersion string `json:"graph_algorithm_version"`
	BuildPolicyVersion    string `json:"build_policy_version"`
}

func (v BuildVersions) Validate() error {
	if strings.TrimSpace(v.ParserResultVersion) == "" || strings.TrimSpace(v.GraphSchemaVersion) == "" || strings.TrimSpace(v.GraphAlgorithmVersion) == "" || strings.TrimSpace(v.BuildPolicyVersion) == "" {
		return fmt.Errorf("all build versions are required")
	}
	return nil
}

type SnapshotMetadata struct {
	Scope               common.Scope `json:"scope"`
	CommitSHA           string       `json:"commit_sha"`
	ParserResultVersion string       `json:"parser_result_version"`
	ArtifactCount       int64        `json:"artifact_count"`
	RelationCount       int64        `json:"relation_count"`
	ParseResultChecksum string       `json:"parse_result_checksum"`
	SchemaVersion       string       `json:"schema_version"`
	CompletedAt         time.Time    `json:"completed_at"`
}

func (m SnapshotMetadata) Validate(scope common.Scope) error {
	if err := scope.Validate(true); err != nil {
		return err
	}
	if hasSurroundingSpace(scope.TenantID) || hasSurroundingSpace(scope.RepositoryID) || hasSurroundingSpace(scope.SnapshotID) {
		return fmt.Errorf("scope identity fields must not contain surrounding whitespace")
	}
	if m.Scope.TenantID != scope.TenantID || m.Scope.RepositoryID != scope.RepositoryID || m.Scope.SnapshotID != scope.SnapshotID {
		return fmt.Errorf("parser metadata scope does not match request scope")
	}
	if strings.TrimSpace(m.CommitSHA) == "" || strings.TrimSpace(m.ParserResultVersion) == "" || strings.TrimSpace(m.ParseResultChecksum) == "" || strings.TrimSpace(m.SchemaVersion) == "" || m.CompletedAt.IsZero() {
		return fmt.Errorf("parser metadata identity and version fields are required")
	}
	if m.ArtifactCount < 0 || m.RelationCount < 0 {
		return fmt.Errorf("parser metadata counts must not be negative")
	}
	return nil
}

type PageIdentity struct {
	Scope               common.Scope `json:"scope"`
	CommitSHA           string       `json:"commit_sha"`
	ParserResultVersion string       `json:"parser_result_version"`
	ParseResultChecksum string       `json:"parse_result_checksum"`
	Cursor              string       `json:"cursor,omitempty"`
	NextCursor          string       `json:"next_cursor,omitempty"`
}

func (p PageIdentity) Validate(metadata SnapshotMetadata, requestedCursor string) error {
	if p.Scope.TenantID != metadata.Scope.TenantID || p.Scope.RepositoryID != metadata.Scope.RepositoryID || p.Scope.SnapshotID != metadata.Scope.SnapshotID || p.CommitSHA != metadata.CommitSHA || p.ParserResultVersion != metadata.ParserResultVersion || p.ParseResultChecksum != metadata.ParseResultChecksum {
		return fmt.Errorf("parser page identity changed while reading snapshot")
	}
	if p.Cursor != requestedCursor {
		return fmt.Errorf("parser page cursor %q does not match requested cursor %q", p.Cursor, requestedCursor)
	}
	if p.NextCursor != "" && p.NextCursor == requestedCursor {
		return fmt.Errorf("parser page cursor did not advance")
	}
	return nil
}

type ArtifactPage struct {
	PageIdentity
	Artifacts []repository.CodeArtifact `json:"artifacts"`
}

type ResolvedRelation struct {
	RelationID           string                  `json:"relation_id"`
	Kind                 repository.RelationKind `json:"kind"`
	FromArtifactID       string                  `json:"from_artifact_id"`
	TargetArtifactID     string                  `json:"target_artifact_id,omitempty"`
	RawTargetSymbol      string                  `json:"raw_target_symbol,omitempty"`
	ResolutionStatus     ResolutionStatus        `json:"resolution_status"`
	Evidence             common.SourceRef        `json:"evidence"`
	Confidence           float64                 `json:"confidence"`
	ExternalIdentity     string                  `json:"external_identity,omitempty"`
	AmbiguousCandidates  []string                `json:"ambiguous_candidates,omitempty"`
	ResolutionReasonCode string                  `json:"resolution_reason_code,omitempty"`
}

func (r ResolvedRelation) Validate() error {
	if strings.TrimSpace(r.RelationID) == "" || strings.TrimSpace(r.FromArtifactID) == "" || !validRelationType(r.Kind) {
		return fmt.Errorf("relation identity, source and kind are required")
	}
	if math.IsNaN(r.Confidence) || math.IsInf(r.Confidence, 0) || r.Confidence < 0 || r.Confidence > 1 {
		return fmt.Errorf("relation confidence must be finite and between 0 and 1")
	}
	if err := r.Evidence.Validate(); err != nil {
		return err
	}
	switch r.ResolutionStatus {
	case ResolutionResolved:
		if strings.TrimSpace(r.TargetArtifactID) == "" || r.ExternalIdentity != "" || len(r.AmbiguousCandidates) != 0 {
			return fmt.Errorf("resolved relation requires target_artifact_id")
		}
	case ResolutionExternal:
		if strings.TrimSpace(r.ExternalIdentity) == "" || r.TargetArtifactID != "" || len(r.AmbiguousCandidates) != 0 {
			return fmt.Errorf("external relation requires external_identity")
		}
	case ResolutionAmbiguous:
		if strings.TrimSpace(r.RawTargetSymbol) == "" || strings.TrimSpace(r.ResolutionReasonCode) == "" || r.TargetArtifactID != "" || r.ExternalIdentity != "" || len(r.AmbiguousCandidates) == 0 {
			return fmt.Errorf("ambiguous relation requires a raw target and candidates")
		}
		seen := map[string]struct{}{}
		for _, candidate := range r.AmbiguousCandidates {
			if strings.TrimSpace(candidate) == "" {
				return fmt.Errorf("ambiguous candidates must not contain empty values")
			}
			if _, duplicate := seen[candidate]; duplicate {
				return fmt.Errorf("ambiguous candidates must be unique")
			}
			seen[candidate] = struct{}{}
		}
	case ResolutionUnresolved:
		if strings.TrimSpace(r.RawTargetSymbol) == "" || strings.TrimSpace(r.ResolutionReasonCode) == "" || r.TargetArtifactID != "" || r.ExternalIdentity != "" || len(r.AmbiguousCandidates) != 0 {
			return fmt.Errorf("unresolved relation requires a raw target and no candidates")
		}
	default:
		return fmt.Errorf("unsupported resolution status %q", r.ResolutionStatus)
	}
	return nil
}

type RelationPage struct {
	PageIdentity
	Relations []ResolvedRelation `json:"relations"`
}

type BuildJob struct {
	JobID              string        `json:"job_id"`
	Scope              common.Scope  `json:"scope"`
	IdempotencyKey     string        `json:"idempotency_key"`
	TriggerEventID     string        `json:"trigger_event_id,omitempty"`
	RequestFingerprint string        `json:"request_fingerprint"`
	CommitSHA          string        `json:"commit_sha"`
	Versions           BuildVersions `json:"versions"`
	Status             JobStatus     `json:"status"`
	RevisionID         string        `json:"revision_id,omitempty"`
	EventID            string        `json:"event_id"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

type BuildAttempt struct {
	AttemptID      string        `json:"attempt_id"`
	JobID          string        `json:"job_id"`
	RevisionID     string        `json:"revision_id"`
	Status         AttemptStatus `json:"status"`
	LeaseOwner     string        `json:"lease_owner"`
	LeaseExpiresAt time.Time     `json:"lease_expires_at"`
	Fence          int64         `json:"fence"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

func (a BuildAttempt) ValidateLease(owner string, fence int64, now time.Time) error {
	if a.Status != AttemptRunning || strings.TrimSpace(owner) == "" || a.LeaseOwner != owner || a.Fence != fence || fence <= 0 || a.LeaseExpiresAt.IsZero() || !now.Before(a.LeaseExpiresAt) {
		return &DomainError{Code: ErrLeaseLost, Operation: "validate_lease", Stage: "lease", Message: "graph build lease is no longer valid", Retryable: false}
	}
	return nil
}

func CanTransitionJob(from, to JobStatus) bool {
	switch from {
	case JobPending:
		return to == JobBuilding || to == JobCancelled || to == JobFailed
	case JobBuilding:
		return to == JobSucceeded || to == JobFailed || to == JobCancelled
	default:
		return false
	}
}

func CanTransitionAttempt(from, to AttemptStatus) bool {
	return from == AttemptRunning && (to == AttemptSucceeded || to == AttemptFailed || to == AttemptLost || to == AttemptCancelled)
}

func CanTransitionCandidate(from, to CandidateStatus) bool {
	return from == CandidateStaging && (to == CandidateSealed || to == CandidateFailed)
}
