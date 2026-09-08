package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/reposense/reposense/internal/domain/graph"
)

func (s *GraphControlStore) PendingGraphEvents(ctx context.Context, limit int, dueAt time.Time) ([]graph.OutboxRecord, error) {
	if limit <= 0 || limit > 1000 || dueAt.IsZero() {
		return nil, invalidControlInput("outbox limit must be between 1 and 1000 and due_at is required")
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id,repository_id,snapshot_id,revision_id,event_id,
event_type,aggregate_id,occurred_at,producer,payload_version,trace_id,payload,delivery_count,
next_attempt_at,COALESCE(last_error,''),created_at FROM graph_outbox_events
WHERE published_at IS NULL AND dead_lettered_at IS NULL AND next_attempt_at<=$1
ORDER BY next_attempt_at,created_at,event_id LIMIT $2`, dueAt.UTC(), limit)
	if err != nil {
		return nil, controlStoreError("pending_outbox", err)
	}
	defer rows.Close()
	records := make([]graph.OutboxRecord, 0)
	for rows.Next() {
		var record graph.OutboxRecord
		var payloadJSON []byte
		if err := rows.Scan(&record.Scope.TenantID, &record.Scope.RepositoryID, &record.Scope.SnapshotID,
			&record.RevisionID, &record.Event.EventID, &record.Event.EventType,
			&record.Event.AggregateID, &record.Event.OccurredAt, &record.Event.Producer,
			&record.Event.PayloadVersion, &record.Event.TraceID, &payloadJSON,
			&record.DeliveryCount, &record.NextAttemptAt, &record.LastError, &record.CreatedAt); err != nil {
			return nil, controlStoreError("scan_outbox", err)
		}
		if err := json.Unmarshal(payloadJSON, &record.Event.Payload); err != nil {
			return nil, controlStoreError("decode_outbox", err)
		}
		record.Scope.TraceID = record.Event.TraceID
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, controlStoreError("pending_outbox", err)
	}
	return records, nil
}

func (s *GraphControlStore) ClaimGraphEvents(ctx context.Context, limit int, dueAt, leaseUntil time.Time) ([]graph.OutboxRecord, error) {
	if limit <= 0 || limit > 1000 || dueAt.IsZero() || !leaseUntil.After(dueAt) {
		return nil, invalidControlInput("outbox claim requires a limit between 1 and 1000 and an ordered lease window")
	}
	rows, err := s.pool.Query(ctx, `WITH selected AS (
  SELECT event_id FROM graph_outbox_events
  WHERE published_at IS NULL AND dead_lettered_at IS NULL AND next_attempt_at<=$1
  ORDER BY next_attempt_at,created_at,event_id
  FOR UPDATE SKIP LOCKED LIMIT $2
), claimed AS (
  UPDATE graph_outbox_events o SET next_attempt_at=$3,updated_at=$1
  FROM selected s WHERE o.event_id=s.event_id
  RETURNING o.tenant_id,o.repository_id,o.snapshot_id,o.revision_id,o.event_id,
    o.event_type,o.aggregate_id,o.occurred_at,o.producer,o.payload_version,o.trace_id,o.payload,
    o.delivery_count,o.next_attempt_at,COALESCE(o.last_error,''),o.created_at
)
SELECT * FROM claimed ORDER BY created_at,event_id`, dueAt.UTC(), limit, leaseUntil.UTC())
	if err != nil {
		return nil, controlStoreError("claim_outbox", err)
	}
	defer rows.Close()
	return scanGraphOutboxRecords(rows)
}

type graphOutboxRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanGraphOutboxRecords(rows graphOutboxRows) ([]graph.OutboxRecord, error) {
	records := make([]graph.OutboxRecord, 0)
	for rows.Next() {
		var record graph.OutboxRecord
		var payloadJSON []byte
		if err := rows.Scan(&record.Scope.TenantID, &record.Scope.RepositoryID, &record.Scope.SnapshotID,
			&record.RevisionID, &record.Event.EventID, &record.Event.EventType,
			&record.Event.AggregateID, &record.Event.OccurredAt, &record.Event.Producer,
			&record.Event.PayloadVersion, &record.Event.TraceID, &payloadJSON,
			&record.DeliveryCount, &record.NextAttemptAt, &record.LastError, &record.CreatedAt); err != nil {
			return nil, controlStoreError("scan_outbox", err)
		}
		if err := json.Unmarshal(payloadJSON, &record.Event.Payload); err != nil {
			return nil, controlStoreError("decode_outbox", err)
		}
		record.Scope.TraceID = record.Event.TraceID
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, controlStoreError("scan_outbox", err)
	}
	return records, nil
}

func (s *GraphControlStore) MarkGraphEventPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
	if strings.TrimSpace(eventID) == "" || publishedAt.IsZero() {
		return invalidControlInput("event_id and published_at are required")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE graph_outbox_events SET published_at=$2,last_error=NULL,
updated_at=$2 WHERE event_id=$1 AND published_at IS NULL AND dead_lettered_at IS NULL`, eventID, publishedAt.UTC())
	if err != nil {
		return controlStoreError("publish_outbox", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var published *time.Time
	var dead *time.Time
	err = s.pool.QueryRow(ctx, `SELECT published_at,dead_lettered_at FROM graph_outbox_events WHERE event_id=$1`, eventID).Scan(&published, &dead)
	if errors.Is(err, pgx.ErrNoRows) {
		return &graph.DomainError{Code: graph.ErrRevisionNotFound, Operation: "publish_outbox", Stage: "outbox", Message: "graph outbox event was not found", Retryable: false, Cause: err}
	}
	if err != nil {
		return controlStoreError("read_outbox", err)
	}
	if published != nil {
		return nil
	}
	return &graph.DomainError{Code: graph.ErrConflict, Operation: "publish_outbox", Stage: "outbox", Message: "graph outbox event is dead-lettered", Retryable: false}
}

func (s *GraphControlStore) MarkGraphEventFailed(ctx context.Context, eventID, failure string, nextAttemptAt time.Time, deadLetter bool) error {
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(failure) == "" || nextAttemptAt.IsZero() {
		return invalidControlInput("event_id, failure and next_attempt_at are required")
	}
	var tag pgconn.CommandTag
	var err error
	if deadLetter {
		tag, err = s.pool.Exec(ctx, `UPDATE graph_outbox_events SET delivery_count=delivery_count+1,
last_error=$2,next_attempt_at=$3,dead_lettered_at=$3,updated_at=$3
WHERE event_id=$1 AND published_at IS NULL AND dead_lettered_at IS NULL`, eventID, failure, nextAttemptAt.UTC())
	} else {
		tag, err = s.pool.Exec(ctx, `UPDATE graph_outbox_events SET delivery_count=delivery_count+1,
last_error=$2,next_attempt_at=$3,updated_at=CURRENT_TIMESTAMP
WHERE event_id=$1 AND published_at IS NULL AND dead_lettered_at IS NULL`, eventID, failure, nextAttemptAt.UTC())
	}
	if err != nil {
		return controlStoreError("fail_outbox", err)
	}
	if tag.RowsAffected() != 1 {
		return &graph.DomainError{Code: graph.ErrConflict, Operation: "fail_outbox", Stage: "outbox", Message: "graph outbox event is missing or finalized", Retryable: false}
	}
	return nil
}

func (s *GraphControlStore) ExpiredGraphAttempts(ctx context.Context, before time.Time, limit int) ([]graph.BuildAttempt, error) {
	if before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, invalidControlInput("expiry cutoff and a limit between 1 and 1000 are required")
	}
	rows, err := s.pool.Query(ctx, `SELECT attempt_id,job_id,revision_id,status,lease_owner,
lease_expires_at,fence,created_at,updated_at FROM graph_build_attempts
WHERE status='RUNNING' AND lease_expires_at<=$1 ORDER BY lease_expires_at,attempt_id LIMIT $2`, before.UTC(), limit)
	if err != nil {
		return nil, controlStoreError("expired_attempts", err)
	}
	defer rows.Close()
	return scanGraphAttempts(rows)
}

func (s *GraphControlStore) MarkGraphAttemptLost(ctx context.Context, jobID, attemptID string, fence int64, now time.Time) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(attemptID) == "" || fence <= 0 || now.IsZero() {
		return invalidControlInput("lost attempt identity, fence and timestamp are required")
	}
	now = now.UTC()
	tag, err := s.pool.Exec(ctx, `UPDATE graph_build_attempts SET status='LOST',error_code='LEASE_LOST',
error_message='graph build lease expired',retryable=true,updated_at=$4
WHERE job_id=$1 AND attempt_id=$2 AND fence=$3 AND status='RUNNING' AND lease_expires_at<=$4`, jobID, attemptID, fence, now)
	if err != nil {
		return controlStoreError("mark_attempt_lost", err)
	}
	if tag.RowsAffected() != 1 {
		return leaseLostError("mark_attempt_lost")
	}
	return nil
}

func (s *GraphControlStore) UnreferencedGraphCandidates(ctx context.Context, before, now time.Time, limit int) ([]graph.BuildAttempt, error) {
	if before.IsZero() || now.IsZero() || !before.Before(now) || limit <= 0 || limit > 1000 {
		return nil, invalidControlInput("ordered orphan cutoff/current time and a limit between 1 and 1000 are required")
	}
	rows, err := s.pool.Query(ctx, `SELECT a.attempt_id,a.job_id,a.revision_id,a.status,a.lease_owner,
a.lease_expires_at,a.fence,a.created_at,a.updated_at FROM graph_build_attempts a
LEFT JOIN graph_revisions r ON r.revision_id=a.revision_id
LEFT JOIN graph_active_revisions ar ON ar.revision_id=a.revision_id
WHERE a.status IN ('FAILED','LOST','CANCELLED') AND a.updated_at<=$1 AND a.lease_expires_at<=$2
AND r.revision_id IS NULL AND ar.revision_id IS NULL
AND NOT EXISTS (
  SELECT 1 FROM graph_build_jobs live_job
  WHERE live_job.revision_id=a.revision_id AND live_job.status IN ('PENDING','BUILDING','SUCCEEDED')
)
AND NOT EXISTS (
  SELECT 1 FROM graph_build_attempts live_attempt
  WHERE live_attempt.revision_id=a.revision_id
    AND live_attempt.status='RUNNING' AND live_attempt.lease_expires_at>$2
)
ORDER BY a.updated_at,a.attempt_id LIMIT $3`, before.UTC(), now.UTC(), limit)
	if err != nil {
		return nil, controlStoreError("unreferenced_candidates", err)
	}
	defer rows.Close()
	return scanGraphAttempts(rows)
}

type graphAttemptRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanGraphAttempts(rows graphAttemptRows) ([]graph.BuildAttempt, error) {
	attempts := make([]graph.BuildAttempt, 0)
	for rows.Next() {
		var attempt graph.BuildAttempt
		var status string
		if err := rows.Scan(&attempt.AttemptID, &attempt.JobID, &attempt.RevisionID, &status,
			&attempt.LeaseOwner, &attempt.LeaseExpiresAt, &attempt.Fence, &attempt.CreatedAt,
			&attempt.UpdatedAt); err != nil {
			return nil, controlStoreError("scan_attempt", err)
		}
		attempt.Status = graph.AttemptStatus(status)
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, controlStoreError("scan_attempts", err)
	}
	return attempts, nil
}

func (s *GraphControlStore) RecordRejectedGraphEvent(ctx context.Context, rejected graph.RejectedEvent) (bool, error) {
	if strings.TrimSpace(rejected.EventID) == "" || strings.TrimSpace(rejected.EventType) == "" || rejected.ErrorCode == "" || strings.TrimSpace(rejected.ErrorMessage) == "" || rejected.RejectedAt.IsZero() {
		return false, invalidControlInput("rejected event identity, error and timestamp are required")
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO graph_rejected_events(
event_id,event_type,tenant_id,repository_id,error_code,error_message,payload_digest,rejected_at)
VALUES($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,NULLIF($7,''),$8) ON CONFLICT(event_id) DO NOTHING`,
		rejected.EventID, rejected.EventType, rejected.TenantID, rejected.RepositoryID, rejected.ErrorCode,
		rejected.ErrorMessage, rejected.PayloadDigest, rejected.RejectedAt.UTC())
	if err != nil {
		return false, controlStoreError("record_rejected_event", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *GraphControlStore) RecordGraphReconciliationRun(ctx context.Context, run graph.ReconciliationRun) error {
	if strings.TrimSpace(run.RunID) == "" || strings.TrimSpace(run.Action) == "" || strings.TrimSpace(run.TargetType) == "" || strings.TrimSpace(run.TargetID) == "" || strings.TrimSpace(run.Result) == "" || run.StartedAt.IsZero() || run.CompletedAt.IsZero() || run.CompletedAt.Before(run.StartedAt) {
		return invalidControlInput("reconciliation run identity, result and ordered timestamps are required")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO graph_reconciliation_runs(
run_id,action,target_type,target_id,result,error_code,error_message,started_at,completed_at)
VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8,$9)`, run.RunID, run.Action,
		run.TargetType, run.TargetID, run.Result, run.ErrorCode, run.ErrorMessage,
		run.StartedAt.UTC(), run.CompletedAt.UTC())
	if err != nil {
		return controlStoreError("record_reconciliation_run", err)
	}
	return nil
}

func (s *GraphControlStore) ActiveGraphRevisionRefs(ctx context.Context, afterRevisionID string, limit int) ([]graph.ActiveRevisionRef, error) {
	if limit <= 0 || limit > 1000 {
		return nil, invalidControlInput("active revision limit must be between 1 and 1000")
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id,repository_id,snapshot_id,revision_id
FROM graph_active_revisions WHERE revision_id>$1 ORDER BY revision_id LIMIT $2`, afterRevisionID, limit)
	if err != nil {
		return nil, controlStoreError("active_revision_refs", err)
	}
	defer rows.Close()
	refs := make([]graph.ActiveRevisionRef, 0)
	for rows.Next() {
		var ref graph.ActiveRevisionRef
		if err := rows.Scan(&ref.Scope.TenantID, &ref.Scope.RepositoryID, &ref.Scope.SnapshotID, &ref.RevisionID); err != nil {
			return nil, controlStoreError("scan_active_revision_ref", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, controlStoreError("active_revision_refs", err)
	}
	return refs, nil
}
