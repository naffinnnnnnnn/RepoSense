package graphapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type WorkerConfig struct {
	LeaseDuration           time.Duration
	HeartbeatInterval       time.Duration
	BuildTimeout            time.Duration
	ControlTimeout          time.Duration
	BatchSize               int
	MaxArtifacts            int64
	MaxRelations            int64
	MaxArtifactBytes        int
	MaxRelationBytes        int
	MaxProperties           int
	MaxCandidates           int
	MaxErrorSamples         int
	MaxBatchBytes           int
	MaxRevisionBytes        int64
	SourceAttempts          int
	BatchAttempts           int
	RetryInitialBackoff     time.Duration
	RetryMaxBackoff         time.Duration
	MaxConcurrentGlobal     int
	MaxConcurrentTenant     int
	MaxConcurrentRepository int
	QualityPolicy           graph.QualityPolicy
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		LeaseDuration: 30 * time.Second, HeartbeatInterval: 10 * time.Second,
		BuildTimeout: 30 * time.Minute, ControlTimeout: 10 * time.Second,
		BatchSize: 1_000, MaxArtifacts: 1_000_000, MaxRelations: 5_000_000,
		MaxArtifactBytes: 256 * 1024, MaxRelationBytes: 256 * 1024,
		MaxProperties: 256, MaxCandidates: 100, MaxErrorSamples: 20, MaxBatchBytes: 8 * 1024 * 1024,
		MaxRevisionBytes: 4 * 1024 * 1024 * 1024,
		SourceAttempts:   3, BatchAttempts: 3,
		RetryInitialBackoff: 100 * time.Millisecond, RetryMaxBackoff: 2 * time.Second,
		MaxConcurrentGlobal: 4, MaxConcurrentTenant: 2, MaxConcurrentRepository: 1,
		QualityPolicy: graph.QualityPolicy{
			MaxInvalidArtifacts: 100, MaxInvalidArtifactRatio: 0.001,
			MaxInvalidRelations: 500, MaxInvalidRelationRatio: 0.001,
		},
	}
}

func (c WorkerConfig) Validate() error {
	if c.LeaseDuration <= 0 || c.HeartbeatInterval <= 0 || c.HeartbeatInterval*2 >= c.LeaseDuration {
		return fmt.Errorf("heartbeat interval must be positive and less than half the lease duration")
	}
	if c.BuildTimeout <= 0 || c.ControlTimeout <= 0 || c.BatchSize <= 0 || c.BatchSize > 10_000 {
		return fmt.Errorf("build/control timeouts and a batch size between 1 and 10000 are required")
	}
	if c.MaxArtifacts < 0 || c.MaxRelations < 0 || c.SourceAttempts <= 0 || c.BatchAttempts <= 0 {
		return fmt.Errorf("capacity limits must not be negative and retry attempts must be positive")
	}
	if c.MaxArtifactBytes <= 0 || c.MaxRelationBytes <= 0 || c.MaxProperties < 0 || c.MaxCandidates < 0 || c.MaxErrorSamples <= 0 || c.MaxBatchBytes <= 0 || c.MaxRevisionBytes <= 0 {
		return fmt.Errorf("record, property, candidate, batch and revision capacity limits must be positive")
	}
	if c.MaxArtifactBytes > c.MaxBatchBytes || c.MaxRelationBytes > c.MaxBatchBytes {
		return fmt.Errorf("single-record capacity must not exceed batch capacity")
	}
	if c.RetryInitialBackoff <= 0 || c.RetryMaxBackoff < c.RetryInitialBackoff {
		return fmt.Errorf("valid retry backoff bounds are required")
	}
	if c.MaxConcurrentGlobal <= 0 || c.MaxConcurrentGlobal > 10_000 || c.MaxConcurrentTenant <= 0 || c.MaxConcurrentTenant > c.MaxConcurrentGlobal || c.MaxConcurrentRepository != 1 {
		return fmt.Errorf("build concurrency must be in range, tenant must not exceed global, and repository must equal one")
	}
	return c.QualityPolicy.Validate()
}

type Worker struct {
	reader     *SourceReader
	buildStore ports.GraphBuildStore
	dataStore  ports.GraphDataRepository
	observer   ports.Observer
	ids        ports.IDGenerator
	clock      ports.Clock
	config     WorkerConfig
}

func NewWorker(reader *SourceReader, buildStore ports.GraphBuildStore, dataStore ports.GraphDataRepository, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config WorkerConfig) (*Worker, error) {
	if reader == nil || buildStore == nil || dataStore == nil {
		return nil, fmt.Errorf("source reader, graph build store and graph data repository are required")
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
	return &Worker{
		reader:     reader.withRetry(config.SourceAttempts, config.RetryInitialBackoff, config.RetryMaxBackoff),
		buildStore: buildStore, dataStore: dataStore, observer: observer, ids: ids, clock: clock, config: config,
	}, nil
}

// RunOnce claims and processes at most one graph build. The graph-worker role
// calls Run to execute bounded concurrent polling loops around this unit.
func (w *Worker) RunOnce(ctx context.Context, owner string) (processed bool, err error) {
	ctx, finish := startGraphStage(w.observer, ctx, "graph_worker", map[string]string{"operation": "build"})
	defer func() { finish(err) }()
	if strings.TrimSpace(owner) == "" || owner != strings.TrimSpace(owner) {
		return false, workerError(graph.ErrInvalidInput, "claim", false, "worker owner must be an exact non-empty identity", nil)
	}

	now := w.clock.Now().UTC()
	attemptID, revisionID := w.ids.New("gat"), w.ids.New("gr")
	if strings.TrimSpace(attemptID) == "" || strings.TrimSpace(revisionID) == "" {
		return false, workerError(graph.ErrBuildFailure, "claim", false, "graph worker could not allocate build identities", nil)
	}
	var job graph.BuildJob
	var attempt graph.BuildAttempt
	var claimed bool
	claimCtx, finishClaim := startGraphStage(w.observer, ctx, "graph_worker_claim", map[string]string{"operation": "claim", "dependency": "postgresql"})
	if limited, ok := w.buildStore.(ports.GraphBuildConcurrencyStore); ok {
		job, attempt, claimed, err = limited.ClaimGraphBuildWithinLimits(claimCtx, owner, attemptID, revisionID, now, w.config.LeaseDuration,
			w.config.MaxConcurrentGlobal, w.config.MaxConcurrentTenant, w.config.MaxConcurrentRepository)
	} else {
		job, attempt, claimed, err = w.buildStore.ClaimGraphBuild(claimCtx, owner, attemptID, revisionID, now, w.config.LeaseDuration)
	}
	finishClaim(err)
	if err != nil || !claimed {
		status := "empty"
		if err != nil {
			status = "failed"
		}
		w.observer.Count("graph_worker_claims_total", 1, map[string]string{"status": status})
		return false, err
	}
	if err := validateClaim(job, attempt, owner, attemptID, revisionID, now); err != nil {
		return true, err
	}
	ctx, attemptFinish := startGraphStage(w.observer, ctx, "graph_worker_attempt", map[string]string{
		"operation": "build", "tenant_id": job.Scope.TenantID, "repository_id": job.Scope.RepositoryID,
		"snapshot_id": job.Scope.SnapshotID, "job_id": job.JobID, "attempt_id": attempt.AttemptID,
		"revision_id": attempt.RevisionID, "trace_id": job.Scope.TraceID, "trace_root": "true",
	})
	defer func() { attemptFinish(err) }()
	buildReason := "initial"
	if attempt.Fence > 1 {
		buildReason = "retry_or_takeover"
	}
	w.observer.Count("graph_worker_claims_total", 1, map[string]string{"status": "claimed", "build_reason": buildReason})
	w.observer.Count("graph_worker_attempts_total", 1, map[string]string{"status": "started", "build_reason": buildReason})

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, w.config.BuildTimeout)
	defer timeoutCancel()
	buildCtx, cancelCause := context.WithCancelCause(timeoutCtx)
	guard := newLeaseGuard(w, job, attempt, cancelCause)
	guard.start(buildCtx)

	err = w.execute(buildCtx, guard, job, attempt)
	guard.stop()
	if err == nil {
		w.observer.Count("graph_worker_builds_total", 1, map[string]string{"status": "succeeded"})
		return true, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if cause := context.Cause(buildCtx); cause != nil {
			err = cause
		}
	}
	failure := classifyWorkerFailure(ctx, timeoutCtx, err)
	err = &failure
	w.observer.Count("graph_worker_builds_total", 1, map[string]string{"status": "failed", "error_code": string(failure.Code)})

	if failure.Code == graph.ErrLeaseLost || failure.Code == graph.ErrActivationOutcomeUnknown || failure.Code == graph.ErrWorkerShutdown {
		return true, err
	}
	failCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.config.ControlTimeout)
	defer cancel()
	if recordErr := w.buildStore.FailGraphAttempt(failCtx, job, attempt, failure, w.clock.Now().UTC()); recordErr != nil {
		return true, recordErr
	}
	return true, err
}

func validateClaim(job graph.BuildJob, attempt graph.BuildAttempt, owner, attemptID, revisionID string, now time.Time) error {
	if err := job.Scope.Validate(true); err != nil {
		return workerError(graph.ErrLeaseLost, "claim", false, "claimed graph job identity is invalid", err)
	}
	if err := job.Versions.Validate(); err != nil {
		return workerError(graph.ErrLeaseLost, "claim", false, "claimed graph job versions are invalid", err)
	}
	if job.Status != graph.JobBuilding || strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(job.CommitSHA) == "" || strings.TrimSpace(job.RequestFingerprint) == "" || strings.TrimSpace(job.EventID) == "" || job.RevisionID != revisionID {
		return workerError(graph.ErrLeaseLost, "claim", false, "claimed graph job identity is invalid", nil)
	}
	if attempt.JobID != job.JobID || attempt.AttemptID != attemptID || attempt.RevisionID != revisionID || attempt.Status != graph.AttemptRunning || attempt.LeaseOwner != owner || attempt.Fence <= 0 || attempt.LeaseExpiresAt.IsZero() || !now.Before(attempt.LeaseExpiresAt) {
		return workerError(graph.ErrLeaseLost, "claim", false, "claimed graph attempt identity is invalid", nil)
	}
	return nil
}

func (w *Worker) execute(ctx context.Context, guard *leaseGuard, job graph.BuildJob, attempt graph.BuildAttempt) error {
	metadataCtx, finishMetadata := startGraphStage(w.observer, ctx, "graph_parser_metadata", map[string]string{"operation": "read", "dependency": "parser"})
	metadata, err := w.reader.Metadata(metadataCtx, job.Scope)
	finishMetadata(err)
	if err != nil {
		return err
	}
	if metadata.CommitSHA != job.CommitSHA || metadata.ParserResultVersion != job.Versions.ParserResultVersion {
		return graphError(graph.ErrParserResultChanged, "metadata", "parser", false, "parser result identity does not match the graph job", nil)
	}
	if metadata.ArtifactCount > w.config.MaxArtifacts || metadata.RelationCount > w.config.MaxRelations {
		return workerError(graph.ErrBuildCapacityExceeded, "capacity", false, "parser snapshot exceeds graph build capacity", nil)
	}
	if err := guard.renew(ctx); err != nil {
		return err
	}
	candidateCtx, finishCandidate := startGraphStage(w.observer, ctx, "graph_neo4j_candidate", map[string]string{"operation": "create", "dependency": "neo4j", "revision_id": attempt.RevisionID})
	err = w.retryBatch(candidateCtx, "candidate", "neo4j", func() error { return w.dataStore.CreateCandidate(candidateCtx, job, attempt) })
	finishCandidate(err)
	if err != nil {
		return err
	}

	consumer := &workerPageConsumer{worker: w, guard: guard, job: job, attempt: attempt, metadata: metadata,
		quality:            graph.QualityStats{ResolutionCounts: map[string]int64{}, ErrorCounts: map[string]int64{}},
		invalidArtifactIDs: map[string]struct{}{}}
	artifactCtx, finishArtifacts := startGraphStage(w.observer, ctx, "graph_parser_pages", map[string]string{"operation": "read", "stage": "artifacts", "dependency": "parser"})
	artifactCount, err := w.reader.readArtifacts(artifactCtx, metadata, consumer)
	finishArtifacts(err)
	if err != nil {
		return err
	}
	if artifactCount != metadata.ArtifactCount {
		return graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact count changed", nil)
	}
	if err := guard.renew(ctx); err != nil {
		return err
	}
	completeCtx, finishComplete := startGraphStage(w.observer, ctx, "graph_neo4j_artifact_stage", map[string]string{"operation": "complete", "dependency": "neo4j", "revision_id": attempt.RevisionID})
	err = w.retryBatch(completeCtx, "artifact_stage", "neo4j", func() error {
		return w.dataStore.CompleteArtifactStage(completeCtx, job, attempt, int(consumer.quality.WrittenArtifacts))
	})
	finishComplete(err)
	if err != nil {
		return err
	}

	relationCtx, finishRelations := startGraphStage(w.observer, ctx, "graph_parser_pages", map[string]string{"operation": "read", "stage": "relations", "dependency": "parser"})
	relationCount, err := w.reader.readRelations(relationCtx, metadata, consumer)
	finishRelations(err)
	if err != nil {
		return err
	}
	if relationCount != metadata.RelationCount {
		return graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation count changed", nil)
	}
	qualityStatus, err := graph.EvaluateQuality(w.config.QualityPolicy, consumer.quality)
	if err != nil {
		return err
	}
	stats := graph.RevisionStats{
		Nodes: int(consumer.quality.WrittenArtifacts), Edges: int(consumer.quality.WrittenRelations),
		UnresolvedTargets:  int(consumer.quality.ResolutionCounts[string(graph.ResolutionUnresolved)]),
		AmbiguousRelations: int(consumer.quality.ResolutionCounts[string(graph.ResolutionAmbiguous)]),
	}
	if err := guard.renew(ctx); err != nil {
		return err
	}
	sealCtx, finishSeal := startGraphStage(w.observer, ctx, "graph_neo4j_seal", map[string]string{"operation": "seal", "dependency": "neo4j", "revision_id": attempt.RevisionID})
	err = w.sealCandidate(sealCtx, job, attempt, stats, qualityStatus)
	finishSeal(err)
	if err != nil {
		return err
	}
	if err := guard.renew(ctx); err != nil {
		return err
	}
	now := w.clock.Now().UTC()
	revision, event := completedRevision(job, attempt, stats, consumer.quality, qualityStatus, now)
	activateCtx, finishActivate := startGraphStage(w.observer, ctx, "graph_control_activate", map[string]string{"operation": "activate", "dependency": "postgresql", "revision_id": attempt.RevisionID})
	err = w.retryBatch(activateCtx, "activate", "postgresql", func() error {
		return w.buildStore.ActivateGraphRevision(activateCtx, job, attempt, revision, event, now)
	})
	finishActivate(err)
	if err == nil {
		w.observer.Count("graph_worker_nodes_total", int64(stats.Nodes), map[string]string{"quality_status": string(qualityStatus)})
		w.observer.Count("graph_worker_edges_total", int64(stats.Edges), map[string]string{"quality_status": string(qualityStatus)})
		w.observer.Count("graph_worker_invalid_artifacts_total", consumer.quality.InvalidArtifacts, map[string]string{"quality_status": string(qualityStatus)})
		w.observer.Count("graph_worker_invalid_relations_total", consumer.quality.InvalidRelations, map[string]string{"quality_status": string(qualityStatus)})
		for errorCode, count := range consumer.quality.ErrorCounts {
			w.observer.Count("graph_worker_quality_errors_total", count, map[string]string{"error_code": errorCode, "quality_status": string(qualityStatus)})
		}
	}
	return err
}

func (w *Worker) sealCandidate(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, stats graph.RevisionStats, quality graph.QualityStatus) error {
	delay := w.config.RetryInitialBackoff
	var lastErr error
	for number := 1; number <= w.config.BatchAttempts; number++ {
		lastErr = w.dataStore.SealCandidate(ctx, job, attempt, stats, quality)
		if lastErr == nil {
			return nil
		}
		if !retryableError(lastErr) {
			return lastErr
		}
		status, statusErr := w.dataStore.CandidateStatus(ctx, job.Scope, attempt.RevisionID)
		if statusErr == nil {
			switch status {
			case graph.CandidateSealed:
				return nil
			case graph.CandidateStaging:
			default:
				return workerError(graph.ErrGraphValidationFailed, "seal", false, "candidate entered an invalid state while sealing", lastErr)
			}
		}
		if number < w.config.BatchAttempts {
			w.observer.Count("graph_worker_retries_total", 1, map[string]string{"status": "scheduled", "stage": "seal", "dependency": "neo4j"})
			if err := waitContext(ctx, delay); err != nil {
				return err
			}
			delay *= 2
			if delay > w.config.RetryMaxBackoff {
				delay = w.config.RetryMaxBackoff
			}
		}
	}
	return &graph.DomainError{Code: graph.ErrActivationOutcomeUnknown, Operation: "graph_worker", Stage: "seal", Dependency: "neo4j", Message: "graph candidate seal outcome is unknown", Retryable: true, Cause: lastErr}
}

func (w *Worker) retryBatch(ctx context.Context, stage, dependency string, operation func() error) error {
	delay := w.config.RetryInitialBackoff
	for attempt := 1; attempt <= w.config.BatchAttempts; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}
		if attempt == w.config.BatchAttempts || !retryableError(err) {
			return err
		}
		w.observer.Count("graph_worker_retries_total", 1, map[string]string{"status": "scheduled", "stage": stage, "dependency": dependency})
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
		delay *= 2
		if delay > w.config.RetryMaxBackoff {
			delay = w.config.RetryMaxBackoff
		}
	}
	return workerError(graph.ErrBuildFailure, "retry", false, "graph batch retry exhausted", nil)
}

type leaseGuard struct {
	worker  *Worker
	job     graph.BuildJob
	attempt graph.BuildAttempt
	cancel  context.CancelCauseFunc
	stopCh  chan struct{}
	doneCh  chan struct{}
	once    sync.Once
	mu      sync.Mutex
}

func newLeaseGuard(worker *Worker, job graph.BuildJob, attempt graph.BuildAttempt, cancel context.CancelCauseFunc) *leaseGuard {
	return &leaseGuard{worker: worker, job: job, attempt: attempt, cancel: cancel, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
}

func (g *leaseGuard) start(ctx context.Context) {
	go func() {
		defer close(g.doneCh)
		ticker := time.NewTicker(g.worker.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.stopCh:
				return
			case <-ticker.C:
				if err := g.renew(ctx); err != nil {
					g.cancel(err)
					return
				}
			}
		}
	}()
}

func (g *leaseGuard) renew(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	expiresAt := g.worker.clock.Now().UTC().Add(g.worker.config.LeaseDuration)
	controlCtx, cancel := context.WithTimeout(ctx, g.worker.config.ControlTimeout)
	defer cancel()
	err := g.worker.buildStore.HeartbeatGraphBuild(controlCtx, g.job.JobID, g.attempt.AttemptID, g.attempt.LeaseOwner, g.attempt.Fence, expiresAt)
	status := "succeeded"
	if err != nil {
		status = "failed"
	}
	g.worker.observer.Count("graph_worker_heartbeats_total", 1, map[string]string{"status": status})
	return err
}

func (g *leaseGuard) stop() {
	g.once.Do(func() { close(g.stopCh) })
	<-g.doneCh
}

func classifyWorkerFailure(parent, timeoutCtx context.Context, err error) graph.DomainError {
	var domainErr *graph.DomainError
	if errors.As(err, &domainErr) {
		return *domainErr
	}
	if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
		return *workerError(graph.ErrBuildTimeout, "build", true, "graph build timed out", err)
	}
	if errors.Is(parent.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return *workerError(graph.ErrWorkerShutdown, "build", true, "graph worker was stopped", err)
	}
	return *workerError(graph.ErrBuildFailure, "build", false, "graph build failed", err)
}

func workerError(code graph.ErrorCode, stage string, retryable bool, message string, cause error) *graph.DomainError {
	return &graph.DomainError{Code: code, Operation: "graph_worker", Stage: stage, Retryable: retryable, Message: message, Cause: cause}
}

func completedRevision(job graph.BuildJob, attempt graph.BuildAttempt, stats graph.RevisionStats, quality graph.QualityStats, qualityStatus graph.QualityStatus, now time.Time) (graph.Revision, common.EventEnvelope) {
	meta := graph.NewMeta(attempt.RevisionID, job.Scope, graph.RevisionActive, attempt.CreatedAt)
	meta.UpdatedAt = now
	revision := graph.Revision{
		EntityMeta: meta, RevisionID: attempt.RevisionID, SnapshotID: job.Scope.SnapshotID,
		CommitSHA: job.CommitSHA, BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive,
		AlgorithmVersion: job.Versions.GraphAlgorithmVersion, ParserResultVersion: job.Versions.ParserResultVersion,
		GraphSchemaVersion: job.Versions.GraphSchemaVersion, BuildPolicyVersion: job.Versions.BuildPolicyVersion,
		QualityStatus: qualityStatus, Quality: quality, RequestFingerprint: job.RequestFingerprint, Stats: stats,
	}
	event := newPublishedEvent(revision, job.EventID, now)
	revision.PublishedEvent = event
	return revision, event
}
