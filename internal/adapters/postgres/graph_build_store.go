package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type GraphControlStore struct{ pool *pgxpool.Pool }

var (
	_ ports.GraphBuildStore          = (*GraphControlStore)(nil)
	_ ports.GraphOutboxStore         = (*GraphControlStore)(nil)
	_ ports.GraphRejectedEventStore  = (*GraphControlStore)(nil)
	_ ports.GraphReconciliationStore = (*GraphControlStore)(nil)
)

func NewGraphControlStore(ctx context.Context, dsn string) (*GraphControlStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &GraphControlStore{pool: pool}, nil
}

func NewGraphControlStoreWithPool(pool *pgxpool.Pool) *GraphControlStore {
	return &GraphControlStore{pool: pool}
}

func (s *GraphControlStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *GraphControlStore) EnqueueGraphBuild(ctx context.Context, requested graph.BuildJob) (graph.BuildJob, bool, error) {
	if err := validateEnqueueJob(requested); err != nil {
		return graph.BuildJob{}, false, err
	}
	requested.Status = graph.JobPending
	requested.RevisionID = ""
	requested.CreatedAt = requested.CreatedAt.UTC()
	requested.UpdatedAt = requested.UpdatedAt.UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return graph.BuildJob{}, false, controlStoreError("enqueue", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `INSERT INTO graph_build_jobs(
job_id,tenant_id,repository_id,snapshot_id,idempotency_key,request_fingerprint,commit_sha,
parser_result_version,graph_schema_version,graph_algorithm_version,build_policy_version,
status,revision_id,event_id,trace_id,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'PENDING',NULL,$12,$13,$14,$15)
ON CONFLICT(request_fingerprint) DO NOTHING`, requested.JobID, requested.Scope.TenantID,
		requested.Scope.RepositoryID, requested.Scope.SnapshotID, requested.IdempotencyKey,
		requested.RequestFingerprint, requested.CommitSHA, requested.Versions.ParserResultVersion,
		requested.Versions.GraphSchemaVersion, requested.Versions.GraphAlgorithmVersion,
		requested.Versions.BuildPolicyVersion, requested.EventID, requested.Scope.TraceID, requested.CreatedAt, requested.UpdatedAt)
	if err != nil {
		return graph.BuildJob{}, false, enqueueStoreError(err)
	}
	created := tag.RowsAffected() == 1
	if !created {
		existing, loadErr := scanGraphJob(tx.QueryRow(ctx, graphJobSelect+` WHERE request_fingerprint=$1`, requested.RequestFingerprint))
		if loadErr != nil {
			return graph.BuildJob{}, false, controlStoreError("load_fingerprint_job", loadErr)
		}
		requested.JobID = existing.JobID
	}
	bound, err := bindGraphIdempotency(ctx, tx, requested)
	if err != nil {
		return graph.BuildJob{}, false, err
	}
	if bound.RequestFingerprint != requested.RequestFingerprint {
		return graph.BuildJob{}, false, idempotencyConflict("idempotency key is already bound to a different graph build")
	}
	stored, err := graphJobByID(ctx, tx, bound.JobID)
	if err != nil {
		return graph.BuildJob{}, false, controlStoreError("load_enqueued_job", err)
	}
	if err := tx.Commit(ctx); err != nil {
		if recovered, readErr := s.graphJobByFingerprint(ctx, requested.RequestFingerprint); readErr == nil {
			return recovered, false, nil
		}
		return graph.BuildJob{}, false, &graph.DomainError{Code: graph.ErrActivationOutcomeUnknown, Operation: "enqueue", Stage: "commit", Dependency: "postgresql", Message: "graph build enqueue outcome is unknown", Retryable: true, Cause: err}
	}
	return stored, created, nil
}

type idempotencyBinding struct {
	JobID              string
	RequestFingerprint string
}

func bindGraphIdempotency(ctx context.Context, tx pgx.Tx, requested graph.BuildJob) (idempotencyBinding, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO graph_idempotency(
tenant_id,repository_id,idempotency_key,request_fingerprint,job_id,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, requested.Scope.TenantID,
		requested.Scope.RepositoryID, requested.IdempotencyKey, requested.RequestFingerprint,
		requested.JobID, requested.CreatedAt, requested.UpdatedAt)
	if err != nil {
		return idempotencyBinding{}, controlStoreError("bind_idempotency", err)
	}
	if tag.RowsAffected() == 1 {
		return idempotencyBinding{JobID: requested.JobID, RequestFingerprint: requested.RequestFingerprint}, nil
	}
	var binding idempotencyBinding
	err = tx.QueryRow(ctx, `SELECT job_id,request_fingerprint FROM graph_idempotency
WHERE tenant_id=$1 AND repository_id=$2 AND idempotency_key=$3`, requested.Scope.TenantID,
		requested.Scope.RepositoryID, requested.IdempotencyKey).Scan(&binding.JobID, &binding.RequestFingerprint)
	if err != nil {
		return idempotencyBinding{}, controlStoreError("read_idempotency", err)
	}
	return binding, nil
}

func (s *GraphControlStore) GraphJob(ctx context.Context, jobID string) (graph.BuildJob, error) {
	if strings.TrimSpace(jobID) == "" {
		return graph.BuildJob{}, invalidControlInput("job_id is required")
	}
	job, err := graphJobByID(ctx, s.pool, jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return graph.BuildJob{}, &graph.DomainError{Code: graph.ErrRevisionNotFound, Operation: "graph_job", Stage: "control", Message: "graph build job was not found", Retryable: false, Cause: err}
	}
	if err != nil {
		return graph.BuildJob{}, controlStoreError("graph_job", err)
	}
	return job, nil
}

func (s *GraphControlStore) graphJobByFingerprint(ctx context.Context, fingerprint string) (graph.BuildJob, error) {
	return scanGraphJob(s.pool.QueryRow(ctx, graphJobSelect+` WHERE request_fingerprint=$1`, fingerprint))
}

func (s *GraphControlStore) ClaimGraphBuild(ctx context.Context, owner, attemptID, revisionID string, now time.Time, lease time.Duration) (graph.BuildJob, graph.BuildAttempt, bool, error) {
	if strings.TrimSpace(owner) == "" || strings.TrimSpace(attemptID) == "" || strings.TrimSpace(revisionID) == "" || now.IsZero() || lease <= 0 {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, invalidControlInput("owner, attempt_id, revision_id, now and a positive lease are required")
	}
	now = now.UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim", err)
	}
	defer tx.Rollback(ctx)

	var jobID string
	err = tx.QueryRow(ctx, `SELECT j.job_id FROM graph_build_jobs j
WHERE j.cancel_requested=false
  AND j.status IN ('PENDING','BUILDING')
  AND NOT EXISTS (
    SELECT 1 FROM graph_build_attempts a
    WHERE a.job_id=j.job_id AND a.status='RUNNING' AND a.lease_expires_at>$1
  )
ORDER BY j.created_at,j.job_id
FOR UPDATE OF j SKIP LOCKED LIMIT 1`, now).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, nil
	}
	if err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_select", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE graph_build_attempts SET status='LOST',updated_at=$2,
error_code='LEASE_LOST',error_message='graph build lease expired',retryable=true
WHERE job_id=$1 AND status='RUNNING' AND lease_expires_at<=$2`, jobID, now); err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("expire_attempt", err)
	}
	var attemptNo int
	var fence int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(attempt_no),0)+1,COALESCE(MAX(fence),0)+1
FROM graph_build_attempts WHERE job_id=$1`, jobID).Scan(&attemptNo, &fence); err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_sequence", err)
	}
	expiresAt := now.Add(lease)
	_, err = tx.Exec(ctx, `INSERT INTO graph_build_attempts(
attempt_id,job_id,revision_id,attempt_no,status,lease_owner,lease_expires_at,fence,created_at,updated_at)
VALUES($1,$2,$3,$4,'RUNNING',$5,$6,$7,$8,$8)`, attemptID, jobID, revisionID,
		attemptNo, owner, expiresAt, fence, now)
	if err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_insert_attempt", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE graph_build_jobs SET status='BUILDING',revision_id=$2,
error_code=NULL,error_message=NULL,retryable=false,updated_at=$3 WHERE job_id=$1`, jobID, revisionID, now); err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_update_job", err)
	}
	job, err := graphJobByID(ctx, tx, jobID)
	if err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_load_job", err)
	}
	attempt := graph.BuildAttempt{AttemptID: attemptID, JobID: jobID, RevisionID: revisionID,
		Status: graph.AttemptRunning, LeaseOwner: owner, LeaseExpiresAt: expiresAt,
		Fence: fence, CreatedAt: now, UpdatedAt: now}
	if err := tx.Commit(ctx); err != nil {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, controlStoreError("claim_commit", err)
	}
	return job, attempt, true, nil
}

func (s *GraphControlStore) HeartbeatGraphBuild(ctx context.Context, jobID, attemptID, owner string, fence int64, expiresAt time.Time) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(attemptID) == "" || strings.TrimSpace(owner) == "" || fence <= 0 || expiresAt.IsZero() {
		return invalidControlInput("heartbeat identity, fence and expiry are required")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE graph_build_attempts SET lease_expires_at=$5,updated_at=CURRENT_TIMESTAMP
WHERE job_id=$1 AND attempt_id=$2 AND lease_owner=$3 AND fence=$4 AND status='RUNNING'
  AND lease_expires_at>CURRENT_TIMESTAMP AND $5>CURRENT_TIMESTAMP
  AND EXISTS (SELECT 1 FROM graph_build_jobs j WHERE j.job_id=$1 AND j.cancel_requested=false)`, jobID, attemptID, owner, fence, expiresAt.UTC())
	if err != nil {
		return controlStoreError("heartbeat", err)
	}
	if tag.RowsAffected() != 1 {
		var cancelRequested bool
		if err := s.pool.QueryRow(ctx, `SELECT cancel_requested FROM graph_build_jobs WHERE job_id=$1`, jobID).Scan(&cancelRequested); err == nil && cancelRequested {
			return &graph.DomainError{Code: graph.ErrBuildCancelled, Operation: "heartbeat", Stage: "control", Dependency: "postgresql", Message: "graph build was cancelled", Retryable: false}
		}
		return leaseLostError("heartbeat")
	}
	return nil
}

func (s *GraphControlStore) FailGraphAttempt(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, failure graph.DomainError, now time.Time) error {
	if now.IsZero() || strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(attempt.AttemptID) == "" {
		return invalidControlInput("job, attempt and now are required")
	}
	now = now.UTC()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return controlStoreError("fail_attempt", err)
	}
	defer tx.Rollback(ctx)
	attemptStatus, jobStatus := graph.AttemptFailed, graph.JobFailed
	if failure.Code == graph.ErrBuildCancelled {
		attemptStatus, jobStatus = graph.AttemptCancelled, graph.JobCancelled
	}
	tag, err := tx.Exec(ctx, `UPDATE graph_build_attempts SET status=$9,error_code=$6,
error_message=$7,retryable=$8,updated_at=$5 WHERE job_id=$1 AND attempt_id=$2
AND lease_owner=$3 AND fence=$4 AND status='RUNNING' AND lease_expires_at>$5`, job.JobID,
		attempt.AttemptID, attempt.LeaseOwner, attempt.Fence, now, failure.Code, failure.Message, failure.Retryable, attemptStatus)
	if err != nil {
		return controlStoreError("fail_attempt_update", err)
	}
	if tag.RowsAffected() != 1 {
		return leaseLostError("fail_attempt")
	}
	tag, err = tx.Exec(ctx, `UPDATE graph_build_jobs SET status=$7,error_code=$3,error_message=$4,
retryable=$5,updated_at=$6 WHERE job_id=$1 AND status='BUILDING' AND revision_id=$2`, job.JobID,
		attempt.RevisionID, failure.Code, failure.Message, failure.Retryable, now, jobStatus)
	if err != nil {
		return controlStoreError("fail_job_update", err)
	}
	if tag.RowsAffected() != 1 {
		return &graph.DomainError{Code: graph.ErrActivationConflict, Operation: "fail_attempt", Stage: "control", Message: "graph build job changed before failure was recorded", Retryable: false}
	}
	if err := tx.Commit(ctx); err != nil {
		return controlStoreError("fail_attempt_commit", err)
	}
	return nil
}

func (s *GraphControlStore) ActivateGraphRevision(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, revision graph.Revision, event common.EventEnvelope, now time.Time) error {
	if err := validateActivation(job, attempt, revision, event, now); err != nil {
		return err
	}
	now = now.UTC()
	if committed, err := s.activationCommitted(ctx, job.JobID, revision.RevisionID, event.EventID); err == nil && committed {
		return nil
	}
	statsJSON, err := json.Marshal(revision.Stats)
	if err != nil {
		return invalidControlInput("revision stats cannot be encoded")
	}
	qualityJSON, err := json.Marshal(revision.Quality)
	if err != nil {
		return invalidControlInput("revision quality cannot be encoded")
	}
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return invalidControlInput("event payload cannot be encoded")
	}
	createdAt := revision.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = now
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return controlStoreError("activate", err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE graph_build_attempts SET status='SUCCEEDED',updated_at=$5
WHERE job_id=$1 AND attempt_id=$2 AND lease_owner=$3 AND fence=$4 AND status='RUNNING'
AND revision_id=$6 AND lease_expires_at>$5`, job.JobID, attempt.AttemptID, attempt.LeaseOwner,
		attempt.Fence, now, revision.RevisionID)
	if err != nil {
		return controlStoreError("activate_attempt", err)
	}
	if tag.RowsAffected() != 1 {
		return leaseLostError("activate")
	}

	var previousRevisionID string
	err = tx.QueryRow(ctx, `SELECT revision_id FROM graph_active_revisions
WHERE tenant_id=$1 AND repository_id=$2 AND snapshot_id=$3 FOR UPDATE`, job.Scope.TenantID,
		job.Scope.RepositoryID, job.Scope.SnapshotID).Scan(&previousRevisionID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return controlStoreError("lock_active_revision", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO graph_revisions(
revision_id,job_id,attempt_id,tenant_id,repository_id,snapshot_id,commit_sha,request_fingerprint,
parser_result_version,graph_schema_version,graph_algorithm_version,build_policy_version,build_mode,
build_status,quality_status,stats,quality,event_id,trace_id,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'FULL','ACTIVE',$13,$14,$15,$16,$17,$18,$19)`,
		revision.RevisionID, job.JobID, attempt.AttemptID, job.Scope.TenantID, job.Scope.RepositoryID,
		job.Scope.SnapshotID, revision.CommitSHA, job.RequestFingerprint, revision.ParserResultVersion,
		revision.GraphSchemaVersion, revision.AlgorithmVersion, revision.BuildPolicyVersion,
		revision.QualityStatus, statsJSON, qualityJSON, event.EventID, event.TraceID, createdAt, now)
	if err != nil {
		return activationError("insert_revision", err)
	}
	if previousRevisionID != "" && previousRevisionID != revision.RevisionID {
		if _, err := tx.Exec(ctx, `UPDATE graph_revisions SET build_status='SUPERSEDED',updated_at=$2
WHERE revision_id=$1 AND build_status='ACTIVE'`, previousRevisionID, now); err != nil {
			return activationError("supersede_revision", err)
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO graph_active_revisions(
tenant_id,repository_id,snapshot_id,revision_id,activated_at) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(tenant_id,repository_id,snapshot_id) DO UPDATE
SET revision_id=EXCLUDED.revision_id,activated_at=EXCLUDED.activated_at`, job.Scope.TenantID,
		job.Scope.RepositoryID, job.Scope.SnapshotID, revision.RevisionID, now)
	if err != nil {
		return activationError("switch_active_revision", err)
	}
	tag, err = tx.Exec(ctx, `UPDATE graph_build_jobs SET status='SUCCEEDED',revision_id=$2,
error_code=NULL,error_message=NULL,retryable=false,updated_at=$3
WHERE job_id=$1 AND status='BUILDING' AND revision_id=$2`, job.JobID, revision.RevisionID, now)
	if err != nil {
		return controlStoreError("activate_job", err)
	}
	if tag.RowsAffected() != 1 {
		return &graph.DomainError{Code: graph.ErrActivationConflict, Operation: "activate", Stage: "control", Message: "graph build job changed before activation", Retryable: false}
	}
	_, err = tx.Exec(ctx, `INSERT INTO graph_outbox_events(
event_id,tenant_id,repository_id,snapshot_id,revision_id,event_type,aggregate_id,occurred_at,
producer,payload_version,trace_id,payload,next_attempt_at,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13,$13)`, event.EventID,
		job.Scope.TenantID, job.Scope.RepositoryID, job.Scope.SnapshotID, revision.RevisionID,
		event.EventType, event.AggregateID, event.OccurredAt.UTC(), event.Producer, event.PayloadVersion,
		event.TraceID, payloadJSON, now)
	if err != nil {
		return activationError("insert_outbox", err)
	}
	if err := tx.Commit(ctx); err != nil {
		if committed, readErr := s.activationCommitted(ctx, job.JobID, revision.RevisionID, event.EventID); readErr == nil && committed {
			return nil
		}
		return &graph.DomainError{Code: graph.ErrActivationOutcomeUnknown, Operation: "activate", Stage: "commit", Dependency: "postgresql", Message: "graph activation outcome is unknown", Retryable: true, Cause: err}
	}
	return nil
}

func (s *GraphControlStore) activationCommitted(ctx context.Context, jobID, revisionID, eventID string) (bool, error) {
	var committed bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(
SELECT 1 FROM graph_build_jobs j
JOIN graph_revisions r ON r.job_id=j.job_id
JOIN graph_outbox_events o ON o.revision_id=r.revision_id
WHERE j.job_id=$1 AND j.status='SUCCEEDED' AND r.revision_id=$2
AND r.build_status IN ('ACTIVE','SUPERSEDED') AND o.event_id=$3)`,
		jobID, revisionID, eventID).Scan(&committed)
	return committed, err
}

func (s *GraphControlStore) ActiveGraphRevision(ctx context.Context, scope common.Scope) (graph.Revision, error) {
	if err := scope.Validate(true); err != nil {
		return graph.Revision{}, invalidControlInput(err.Error())
	}
	revision, err := scanGraphRevision(s.pool.QueryRow(ctx, graphRevisionSelect+`
JOIN graph_active_revisions a ON a.revision_id=r.revision_id
WHERE a.tenant_id=$1 AND a.repository_id=$2 AND a.snapshot_id=$3`, scope.TenantID, scope.RepositoryID, scope.SnapshotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return graph.Revision{}, &graph.DomainError{Code: graph.ErrRevisionNotFound, Operation: "active_revision", Stage: "control", Message: "active graph revision was not found", Retryable: false, Cause: err}
	}
	if err != nil {
		return graph.Revision{}, controlStoreError("active_revision", err)
	}
	return revision, nil
}

const graphJobSelect = `SELECT job_id,tenant_id,repository_id,snapshot_id,idempotency_key,
request_fingerprint,commit_sha,parser_result_version,graph_schema_version,graph_algorithm_version,
build_policy_version,status,COALESCE(revision_id,''),event_id,trace_id,created_at,updated_at FROM graph_build_jobs`

func graphJobByID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, jobID string) (graph.BuildJob, error) {
	return scanGraphJob(q.QueryRow(ctx, graphJobSelect+` WHERE job_id=$1`, jobID))
}

func scanGraphJob(row pgx.Row) (graph.BuildJob, error) {
	var job graph.BuildJob
	var status string
	err := row.Scan(&job.JobID, &job.Scope.TenantID, &job.Scope.RepositoryID, &job.Scope.SnapshotID,
		&job.IdempotencyKey, &job.RequestFingerprint, &job.CommitSHA, &job.Versions.ParserResultVersion,
		&job.Versions.GraphSchemaVersion, &job.Versions.GraphAlgorithmVersion,
		&job.Versions.BuildPolicyVersion, &status, &job.RevisionID, &job.EventID, &job.Scope.TraceID, &job.CreatedAt, &job.UpdatedAt)
	job.Status = graph.JobStatus(status)
	return job, err
}

const graphRevisionSelect = `SELECT r.revision_id,r.tenant_id,r.repository_id,r.snapshot_id,r.commit_sha,
r.request_fingerprint,r.parser_result_version,r.graph_schema_version,r.graph_algorithm_version,
r.build_policy_version,r.build_mode,r.build_status,r.quality_status,r.stats,r.quality,r.event_id,
r.trace_id,r.created_at,r.updated_at FROM graph_revisions r `

func scanGraphRevision(row pgx.Row) (graph.Revision, error) {
	var revision graph.Revision
	var mode, status, qualityStatus string
	var statsJSON, qualityJSON []byte
	var eventID string
	err := row.Scan(&revision.RevisionID, &revision.TenantID, &revision.RepositoryID,
		&revision.SnapshotID, &revision.CommitSHA, &revision.RequestFingerprint,
		&revision.ParserResultVersion, &revision.GraphSchemaVersion, &revision.AlgorithmVersion,
		&revision.BuildPolicyVersion, &mode, &status, &qualityStatus, &statsJSON, &qualityJSON,
		&eventID, &revision.TraceID, &revision.CreatedAt, &revision.UpdatedAt)
	if err != nil {
		return graph.Revision{}, err
	}
	revision.ID = revision.RevisionID
	revision.BuildMode = graph.BuildMode(mode)
	revision.BuildStatus = graph.RevisionStatus(status)
	revision.Status = status
	revision.QualityStatus = graph.QualityStatus(qualityStatus)
	revision.PublishedEvent.EventID = eventID
	if err := json.Unmarshal(statsJSON, &revision.Stats); err != nil {
		return graph.Revision{}, err
	}
	if err := json.Unmarshal(qualityJSON, &revision.Quality); err != nil {
		return graph.Revision{}, err
	}
	return revision, nil
}

func validateEnqueueJob(job graph.BuildJob) error {
	if err := job.Scope.Validate(true); err != nil {
		return invalidControlInput(err.Error())
	}
	if err := job.Versions.Validate(); err != nil {
		return invalidControlInput(err.Error())
	}
	decoded, err := hex.DecodeString(job.RequestFingerprint)
	if err != nil || len(decoded) != 32 {
		return invalidControlInput("request_fingerprint must be a SHA-256 hex digest")
	}
	if strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(job.IdempotencyKey) == "" || strings.TrimSpace(job.CommitSHA) == "" || strings.TrimSpace(job.EventID) == "" {
		return invalidControlInput("job identity, idempotency key, commit and event identity are required")
	}
	if job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
		return invalidControlInput("job timestamps are required")
	}
	return nil
}

func validateActivation(job graph.BuildJob, attempt graph.BuildAttempt, revision graph.Revision, event common.EventEnvelope, now time.Time) error {
	if now.IsZero() || strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(attempt.AttemptID) == "" || strings.TrimSpace(revision.RevisionID) == "" {
		return invalidControlInput("activation identities and timestamp are required")
	}
	if job.RevisionID != revision.RevisionID || attempt.JobID != job.JobID || attempt.RevisionID != revision.RevisionID || revision.RequestFingerprint != job.RequestFingerprint {
		return &graph.DomainError{Code: graph.ErrActivationConflict, Operation: "activate", Stage: "validation", Message: "job, attempt and revision identities do not match", Retryable: false}
	}
	if revision.BuildMode != graph.BuildFull || revision.CommitSHA != job.CommitSHA || revision.ParserResultVersion != job.Versions.ParserResultVersion || revision.GraphSchemaVersion != job.Versions.GraphSchemaVersion || revision.AlgorithmVersion != job.Versions.GraphAlgorithmVersion || revision.BuildPolicyVersion != job.Versions.BuildPolicyVersion {
		return &graph.DomainError{Code: graph.ErrActivationConflict, Operation: "activate", Stage: "validation", Message: "revision build identity does not match the job", Retryable: false}
	}
	if revision.TenantID != job.Scope.TenantID || revision.RepositoryID != job.Scope.RepositoryID || revision.SnapshotID != job.Scope.SnapshotID || revision.BuildStatus != graph.RevisionActive {
		return &graph.DomainError{Code: graph.ErrActivationConflict, Operation: "activate", Stage: "validation", Message: "revision scope or activation status does not match the job", Retryable: false}
	}
	if revision.QualityStatus != graph.QualityHealthy && revision.QualityStatus != graph.QualityDegraded {
		return invalidControlInput("revision quality status must be HEALTHY or DEGRADED")
	}
	if err := revision.Quality.Validate(); err != nil {
		return invalidControlInput(err.Error())
	}
	if strings.TrimSpace(event.EventID) == "" || event.EventID != job.EventID || event.EventType != "graph.published.v1" || event.AggregateID != revision.RevisionID || strings.TrimSpace(event.Producer) == "" || event.PayloadVersion != 1 || event.OccurredAt.IsZero() {
		return invalidControlInput("published event identity and metadata are invalid")
	}
	return nil
}

func invalidControlInput(message string) error {
	return &graph.DomainError{Code: graph.ErrInvalidInput, Operation: "graph_control_store", Stage: "validation", Message: message, Retryable: false}
}

func idempotencyConflict(message string) error {
	return &graph.DomainError{Code: graph.ErrIdempotencyConflict, Operation: "enqueue", Stage: "idempotency", Message: message, Retryable: false}
}

func leaseLostError(operation string) error {
	return &graph.DomainError{Code: graph.ErrLeaseLost, Operation: operation, Stage: "lease", Message: "graph build lease is no longer valid", Retryable: false}
}

func controlStoreError(operation string, cause error) error {
	return &graph.DomainError{Code: graph.ErrControlStoreUnavailable, Operation: operation, Stage: "control", Dependency: "postgresql", Message: "graph control store operation failed", Retryable: true, Cause: cause}
}

func enqueueStoreError(cause error) error {
	var pgError *pgconn.PgError
	if errors.As(cause, &pgError) && pgError.Code == "23505" {
		return idempotencyConflict("graph event or job identity is already bound to another build")
	}
	return controlStoreError("enqueue_job", cause)
}

func activationError(operation string, cause error) error {
	return &graph.DomainError{Code: graph.ErrActivationFailed, Operation: operation, Stage: "activation", Dependency: "postgresql", Message: "graph activation transaction failed", Retryable: true, Cause: cause}
}
