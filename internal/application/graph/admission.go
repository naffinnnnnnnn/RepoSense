package graphapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

// BuildAdmissionConfig contains the API-side limits required to validate and
// durably enqueue a build. The admission path reads Parser metadata only; it
// never reads artifact/relation pages or writes Neo4j.
type BuildAdmissionConfig struct {
	GraphSchemaVersion    string
	GraphAlgorithmVersion string
	BuildPolicyVersion    string
	MaxPendingGlobal      int
	MaxPendingPerTenant   int
	MetadataTimeout       time.Duration
	StoreTimeout          time.Duration
}

func (c BuildAdmissionConfig) Validate() error {
	if !exactIdentity(c.GraphSchemaVersion) || !exactIdentity(c.GraphAlgorithmVersion) || !exactIdentity(c.BuildPolicyVersion) {
		return fmt.Errorf("graph schema, algorithm and build policy versions are required exact identities")
	}
	if c.MaxPendingGlobal <= 0 || c.MaxPendingGlobal > 1_000_000 || c.MaxPendingPerTenant <= 0 || c.MaxPendingPerTenant > c.MaxPendingGlobal {
		return fmt.Errorf("ordered pending graph build quotas must be positive")
	}
	if c.MetadataTimeout <= 0 || c.MetadataTimeout > time.Minute || c.StoreTimeout <= 0 || c.StoreTimeout > time.Minute {
		return fmt.Errorf("graph admission metadata and store timeouts must be positive and at most one minute")
	}
	return nil
}

type BuildAdmissionService struct {
	reader    *SourceReader
	admission ports.GraphBuildAdmissionStore
	observer  ports.Observer
	ids       ports.IDGenerator
	clock     ports.Clock
	config    BuildAdmissionConfig
}

func NewBuildAdmissionService(reader *SourceReader, admission ports.GraphBuildAdmissionStore, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config BuildAdmissionConfig) (*BuildAdmissionService, error) {
	if reader == nil || admission == nil {
		return nil, fmt.Errorf("parser metadata reader and graph build admission store are required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
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
	return &BuildAdmissionService{reader: reader, admission: admission, observer: observer, ids: ids, clock: clock, config: config}, nil
}

// Submit validates a FULL build request and durably enqueues it. Successful
// return means the job is pending or is the canonical idempotent prior job;
// graph construction is always performed asynchronously by a Worker.
func (s *BuildAdmissionService) Submit(ctx context.Context, command graph.BuildCommand) (job graph.BuildJob, err error) {
	ctx, finish := startGraphStage(s.observer, ctx, "graph_build_admission", map[string]string{
		"operation": "enqueue", "tenant_id": command.Scope.TenantID, "repository_id": command.Scope.RepositoryID,
		"snapshot_id": command.Scope.SnapshotID, "trace_id": command.Scope.TraceID,
	})
	defer func() { finish(err) }()
	if command.Mode == "" {
		return graph.BuildJob{}, admissionError(graph.ErrInvalidInput, "validate", "request", false, "graph build mode is required", nil)
	}
	if command.Mode != graph.BuildFull {
		return graph.BuildJob{}, admissionError(graph.ErrUnsupportedBuildMode, "validate", "request", false, "graph builds only support FULL mode", nil)
	}
	if len(command.ArtifactIDs) != 0 {
		return graph.BuildJob{}, admissionError(graph.ErrPartialBuildNotAllowed, "validate", "request", false, "partial graph builds are not supported", nil)
	}
	if validateErr := command.Validate(); validateErr != nil {
		return graph.BuildJob{}, admissionError(graph.ErrInvalidInput, "validate", "request", false, "graph build request is invalid", validateErr)
	}

	metadataCtx, cancelMetadata := context.WithTimeout(ctx, s.config.MetadataTimeout)
	metadataCtx, finishMetadata := startGraphStage(s.observer, metadataCtx, "graph_parser_metadata", map[string]string{"operation": "read", "dependency": "parser"})
	metadata, err := s.reader.Metadata(metadataCtx, command.Scope)
	cancelMetadata()
	finishMetadata(err)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return graph.BuildJob{}, admissionError(graph.ErrParserResultUnavailable, "metadata", "parser", true, "parser metadata lookup timed out", context.DeadlineExceeded)
		}
		return graph.BuildJob{}, err
	}

	versions := graph.BuildVersions{
		ParserResultVersion: metadata.ParserResultVersion,
		GraphSchemaVersion:  s.config.GraphSchemaVersion, GraphAlgorithmVersion: s.config.GraphAlgorithmVersion,
		BuildPolicyVersion: s.config.BuildPolicyVersion,
	}
	fingerprint, err := graph.RequestFingerprint(graph.FingerprintInput{Scope: command.Scope, CommitSHA: metadata.CommitSHA, Versions: versions})
	if err != nil {
		return graph.BuildJob{}, admissionError(graph.ErrInvalidInput, "fingerprint", "request", false, "graph build identity is invalid", err)
	}
	now := s.clock.Now().UTC()
	requested := graph.BuildJob{
		JobID: s.ids.New("gj"), Scope: command.Scope, IdempotencyKey: command.IdempotencyKey,
		RequestFingerprint: fingerprint, CommitSHA: metadata.CommitSHA, Versions: versions,
		Status: graph.JobPending, EventID: s.ids.New("evt"), CreatedAt: now, UpdatedAt: now,
	}
	if !exactIdentity(requested.JobID) || !exactIdentity(requested.EventID) || now.IsZero() {
		return graph.BuildJob{}, admissionError(graph.ErrBuildFailure, "identity", "service", true, "graph build admission could not allocate job identity", nil)
	}
	storeCtx, cancelStore := context.WithTimeout(ctx, s.config.StoreTimeout)
	storeCtx, finishEnqueue := startGraphStage(s.observer, storeCtx, "graph_control_enqueue", map[string]string{"operation": "enqueue", "dependency": "postgresql"})
	stored, _, err := s.admission.EnqueueGraphBuildWithinQuota(storeCtx, requested, s.config.MaxPendingGlobal, s.config.MaxPendingPerTenant)
	cancelStore()
	finishEnqueue(err)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return graph.BuildJob{}, admissionError(graph.ErrControlStoreUnavailable, "enqueue", "postgresql", true, "graph build enqueue timed out", context.DeadlineExceeded)
		}
		return graph.BuildJob{}, err
	}
	if err := validateAdmittedJob(requested, stored); err != nil {
		return graph.BuildJob{}, err
	}
	s.observer.Count("graph_build_admissions_total", 1, map[string]string{"status": "accepted"})
	return stored, nil
}

func validateAdmittedJob(requested, stored graph.BuildJob) error {
	if !exactIdentity(stored.JobID) || !exactIdentity(stored.EventID) || stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		return admissionError(graph.ErrGraphInconsistent, "enqueue", "postgresql", true, "graph control plane returned an invalid build job", nil)
	}
	if stored.Scope.TenantID != requested.Scope.TenantID || stored.Scope.RepositoryID != requested.Scope.RepositoryID || stored.Scope.SnapshotID != requested.Scope.SnapshotID ||
		stored.RequestFingerprint != requested.RequestFingerprint || stored.CommitSHA != requested.CommitSHA || stored.Versions != requested.Versions {
		return admissionError(graph.ErrGraphInconsistent, "enqueue", "postgresql", true, "graph control plane returned a build job with inconsistent identity", nil)
	}
	switch stored.Status {
	case graph.JobPending, graph.JobBuilding, graph.JobSucceeded, graph.JobFailed, graph.JobCancelled:
		return nil
	default:
		return admissionError(graph.ErrGraphInconsistent, "enqueue", "postgresql", true, "graph control plane returned a build job with invalid state", nil)
	}
}

func admissionError(code graph.ErrorCode, operation, dependency string, retryable bool, message string, cause error) error {
	return &graph.DomainError{Code: code, Operation: operation, Stage: "admission", Dependency: dependency, Retryable: retryable, Message: message, Cause: cause}
}
