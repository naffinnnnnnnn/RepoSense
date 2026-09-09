package graphapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type ReconcilerConfig struct {
	BatchSize        int
	OrphanRetention  time.Duration
	OperationTimeout time.Duration
}

func (c ReconcilerConfig) Validate() error {
	if c.BatchSize <= 0 || c.BatchSize > 1000 || c.OrphanRetention <= 0 || c.OperationTimeout <= 0 {
		return fmt.Errorf("reconciler batch, orphan retention and operation timeout must be positive")
	}
	return nil
}

type ReconciliationReport struct {
	ExpiredAttempts int
	DeletedOrphans  int
	VerifiedActive  int
	Inconsistent    int
}

type Reconciler struct {
	buildStore ports.GraphReconciliationBuildStore
	store      ports.GraphReconciliationStore
	dataStore  ports.GraphReconciliationDataRepository
	observer   ports.Observer
	ids        ports.IDGenerator
	clock      ports.Clock
	config     ReconcilerConfig

	cursorMu     sync.Mutex
	activeCursor string
}

func NewReconciler(buildStore ports.GraphReconciliationBuildStore, store ports.GraphReconciliationStore, dataStore ports.GraphReconciliationDataRepository, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config ReconcilerConfig) (*Reconciler, error) {
	if buildStore == nil || store == nil || dataStore == nil {
		return nil, fmt.Errorf("graph build, reconciliation and data stores are required")
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
	return &Reconciler{buildStore: buildStore, store: store, dataStore: dataStore, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (r *Reconciler) RunOnce(ctx context.Context) (report ReconciliationReport, err error) {
	runCtx, cancelRun := context.WithTimeout(ctx, r.config.OperationTimeout)
	defer cancelRun()
	ctx = runCtx
	ctx, finish := startGraphStage(r.observer, ctx, "graph_reconciler", map[string]string{"operation": "reconcile"})
	defer func() { finish(err) }()
	now := r.clock.Now().UTC()
	var failures []error

	expired, listErr := r.store.ExpiredGraphAttempts(ctx, now, r.config.BatchSize)
	if listErr != nil {
		failures = append(failures, listErr)
	} else {
		r.observer.Count("graph_reconciler_expired_attempts_detected_total", int64(len(expired)), nil)
		for _, attempt := range expired {
			if ctx.Err() != nil {
				failures = append(failures, ctx.Err())
				break
			}
			startedAt := r.clock.Now().UTC()
			actionCtx, finishAction := startGraphStage(r.observer, ctx, "graph_reconciler_action", map[string]string{
				"operation": "repair", "action": "expire_attempt", "attempt_id": attempt.AttemptID, "revision_id": attempt.RevisionID,
			})
			result := "LOST_REBUILD_PENDING"
			if job, jobErr := r.buildStore.GraphJob(actionCtx, attempt.JobID); jobErr == nil {
				if status, statusErr := r.dataStore.CandidateStatus(actionCtx, job.Scope, attempt.RevisionID); statusErr == nil && status == graph.CandidateSealed {
					result = "SEALED_REBUILD_PENDING"
				} else if statusErr != nil {
					result = "LOST_CANDIDATE_STATUS_UNKNOWN"
				}
			} else {
				result = "LOST_JOB_STATE_UNKNOWN"
			}
			actionErr := r.store.MarkGraphAttemptLost(actionCtx, attempt.JobID, attempt.AttemptID, attempt.Fence, now)
			if actionErr == nil {
				report.ExpiredAttempts++
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "expire_attempt", "status": "repaired"})
			} else if hasGraphErrorCode(actionErr, graph.ErrLeaseLost) {
				result = "SKIPPED_LEASE_CHANGED"
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "expire_attempt", "status": "skipped"})
			} else {
				result = "FAILED"
				failures = append(failures, actionErr)
			}
			auditErr := r.recordRun(actionCtx, "expire_attempt", "attempt", attempt.AttemptID, result, startedAt, actionErr)
			finishAction(errors.Join(actionErr, auditErr))
			if auditErr != nil {
				failures = append(failures, auditErr)
			}
		}
	}

	cutoff := now.Add(-r.config.OrphanRetention)
	orphans, listErr := r.store.UnreferencedGraphCandidates(ctx, cutoff, now, r.config.BatchSize)
	if listErr != nil {
		failures = append(failures, listErr)
	} else {
		r.observer.Count("graph_reconciler_orphans_detected_total", int64(len(orphans)), nil)
		for _, attempt := range orphans {
			if ctx.Err() != nil {
				failures = append(failures, ctx.Err())
				break
			}
			startedAt := r.clock.Now().UTC()
			actionCtx, finishAction := startGraphStage(r.observer, ctx, "graph_reconciler_action", map[string]string{
				"operation": "repair", "action": "delete_orphan", "attempt_id": attempt.AttemptID, "revision_id": attempt.RevisionID,
			})
			result, actionErr := r.deleteOrphan(actionCtx, attempt)
			if actionErr == nil {
				report.DeletedOrphans++
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "delete_orphan", "status": "repaired"})
			} else {
				failures = append(failures, actionErr)
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "delete_orphan", "status": "failed"})
			}
			auditErr := r.recordRun(actionCtx, "delete_orphan", "revision", attempt.RevisionID, result, startedAt, actionErr)
			finishAction(errors.Join(actionErr, auditErr))
			if auditErr != nil {
				failures = append(failures, auditErr)
			}
		}
	}

	refs, listErr := r.activeRefs(ctx)
	if listErr != nil {
		failures = append(failures, listErr)
	} else {
		for _, ref := range refs {
			startedAt := r.clock.Now().UTC()
			actionCtx, finishAction := startGraphStage(r.observer, ctx, "graph_reconciler_action", map[string]string{
				"operation": "verify", "action": "verify_active", "tenant_id": ref.Scope.TenantID,
				"repository_id": ref.Scope.RepositoryID, "snapshot_id": ref.Scope.SnapshotID, "revision_id": ref.RevisionID,
			})
			result := "VERIFIED"
			verified := false
			var actionErr error
			revision, revisionErr := r.buildStore.ActiveGraphRevision(actionCtx, ref.Scope)
			switch {
			case revisionErr != nil:
				actionErr = revisionErr
			case revision.RevisionID != ref.RevisionID:
				// The pointer changed after the reconciliation page was read. The
				// replacement will be checked on a subsequent pass.
				result = "SKIPPED_POINTER_CHANGED"
			case revision.RevisionID == ref.RevisionID:
				actionErr = r.dataStore.VerifyRevision(actionCtx, revision)
				verified = actionErr == nil
			}
			if hasGraphErrorCode(actionErr, graph.ErrRevisionNotFound) {
				actionErr = activeInconsistency(actionErr)
			}
			if verified {
				report.VerifiedActive++
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "verify_active", "status": "verified"})
			} else if actionErr == nil {
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "verify_active", "status": "skipped"})
			} else if hasGraphErrorCode(actionErr, graph.ErrGraphInconsistent) {
				result = "INCONSISTENT"
				report.Inconsistent++
				failures = append(failures, actionErr)
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "verify_active", "status": "inconsistent"})
			} else {
				result = "FAILED"
				failures = append(failures, actionErr)
				r.observer.Count("graph_reconciler_actions_total", 1, map[string]string{"action": "verify_active", "status": "failed"})
			}
			auditErr := r.recordRun(actionCtx, "verify_active", "revision", ref.RevisionID, result, startedAt, actionErr)
			finishAction(errors.Join(actionErr, auditErr))
			if auditErr != nil {
				failures = append(failures, auditErr)
			}
		}
	}
	return report, errors.Join(failures...)
}

func (r *Reconciler) deleteOrphan(ctx context.Context, attempt graph.BuildAttempt) (string, error) {
	job, err := r.buildStore.GraphJob(ctx, attempt.JobID)
	if err != nil {
		return "FAILED", err
	}
	_, err = r.dataStore.CandidateStatus(ctx, job.Scope, attempt.RevisionID)
	if hasGraphErrorCode(err, graph.ErrRevisionNotFound) {
		return "ALREADY_ABSENT", nil
	}
	if err != nil {
		return "FAILED", err
	}
	if err := r.dataStore.DeleteCandidate(ctx, job.Scope, attempt.RevisionID, attempt.Fence); err != nil {
		if _, statusErr := r.dataStore.CandidateStatus(ctx, job.Scope, attempt.RevisionID); hasGraphErrorCode(statusErr, graph.ErrRevisionNotFound) {
			return "ALREADY_ABSENT", nil
		}
		return "FAILED", err
	}
	return "DELETED", nil
}

func (r *Reconciler) activeRefs(ctx context.Context) ([]graph.ActiveRevisionRef, error) {
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()
	refs, err := r.store.ActiveGraphRevisionRefs(ctx, r.activeCursor, r.config.BatchSize)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		r.activeCursor = ""
	} else if len(refs) < r.config.BatchSize {
		r.activeCursor = ""
	} else {
		r.activeCursor = refs[len(refs)-1].RevisionID
	}
	return refs, nil
}

func (r *Reconciler) recordRun(ctx context.Context, action, targetType, targetID, result string, started time.Time, actionErr error) error {
	code, message := reconciliationError(actionErr)
	run := graph.ReconciliationRun{RunID: r.ids.New("grc"), Action: action, TargetType: targetType,
		TargetID: targetID, Result: result, ErrorCode: code, ErrorMessage: message, StartedAt: started, CompletedAt: r.clock.Now().UTC()}
	if !exactIdentity(run.RunID) {
		return workerError(graph.ErrBuildFailure, "reconciliation_audit", true, "reconciler could not allocate audit identity", nil)
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.config.OperationTimeout)
	defer cancel()
	return r.store.RecordGraphReconciliationRun(auditCtx, run)
}

func reconciliationError(err error) (graph.ErrorCode, string) {
	if err == nil {
		return "", ""
	}
	var domainErr *graph.DomainError
	if errors.As(err, &domainErr) {
		return domainErr.Code, domainErr.Message
	}
	return graph.ErrBuildFailure, "graph reconciliation operation failed"
}

func activeInconsistency(cause error) error {
	if hasGraphErrorCode(cause, graph.ErrGraphInconsistent) {
		return cause
	}
	return &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "reconcile_active", Stage: "reconcile", Dependency: "neo4j", Message: "active graph revision is missing", Retryable: true, Cause: cause}
}

func hasGraphErrorCode(err error, code graph.ErrorCode) bool {
	var domainErr *graph.DomainError
	return errors.As(err, &domainErr) && domainErr.Code == code
}
