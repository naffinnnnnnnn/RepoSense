package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type RepositoryStore struct{ pool *pgxpool.Pool }

func NewRepositoryStore(ctx context.Context, dsn string) (*RepositoryStore, error) {
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
	return &RepositoryStore{pool: pool}, nil
}
func NewRepositoryStoreWithPool(pool *pgxpool.Pool) *RepositoryStore {
	return &RepositoryStore{pool: pool}
}
func (s *RepositoryStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *RepositoryStore) BindRepository(ctx context.Context, b repository.RepositoryBinding) error {
	if b.TenantID == "" || b.RepositoryID == "" || b.CanonicalIdentity == "" {
		return errors.New("repository binding 不完整")
	}
	if b.Provider == "" {
		b.Provider = "local"
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	if b.UpdatedAt.IsZero() {
		b.UpdatedAt = b.CreatedAt
	}
	command, err := s.pool.Exec(ctx, `INSERT INTO repositories(tenant_id,repository_id,provider,canonical_identity,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,repository_id) DO NOTHING`, b.TenantID, b.RepositoryID, b.Provider, b.CanonicalIdentity, b.CreatedAt, b.UpdatedAt)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	var identity, provider string
	if err := s.pool.QueryRow(ctx, `SELECT canonical_identity,provider FROM repositories WHERE tenant_id=$1 AND repository_id=$2`, b.TenantID, b.RepositoryID).Scan(&identity, &provider); err != nil {
		return err
	}
	if identity != b.CanonicalIdentity || provider != b.Provider {
		return &repository.IdempotencyConflictError{Message: "RepositoryID 已绑定到另一仓库"}
	}
	return nil
}

func (s *RepositoryStore) AcquireIdempotency(ctx context.Context, task repository.ParseTask) (repository.ParseTask, bool, error) {
	commandJSON, err := json.Marshal(task.Command)
	if err != nil {
		return repository.ParseTask{}, false, err
	}
	changedJSON, _ := json.Marshal([]repository.ChangedPath{})
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return repository.ParseTask{}, false, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO repository_snapshots(snapshot_id,tenant_id,repository_id,provider,ref,commit_sha,parent_snapshot_id,sync_status,changed_paths,trace_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8,$9,$10,$11,$12)`, task.Snapshot.SnapshotID, task.Snapshot.TenantID, task.Snapshot.RepositoryID, task.Snapshot.Provider, task.Snapshot.Ref, task.Snapshot.CommitSHA, task.Snapshot.ParentSnapshotID, task.Snapshot.SyncStatus, changedJSON, task.Snapshot.TraceID, task.Snapshot.CreatedAt, task.Snapshot.UpdatedAt)
	if err != nil {
		if isUnique(err) {
			existing, loadErr := s.taskByKeyTx(ctx, tx, task.Command.Scope, task.Command.IdempotencyKey)
			if loadErr != nil {
				return repository.ParseTask{}, false, loadErr
			}
			if existing.CommandFingerprint != task.CommandFingerprint {
				return repository.ParseTask{}, false, &repository.IdempotencyConflictError{Message: "幂等键已绑定到不同命令"}
			}
			return existing, false, nil
		}
		return repository.ParseTask{}, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO parse_jobs(job_id,tenant_id,repository_id,snapshot_id,parser_version,scope,status,progress,retry_count,command,command_fingerprint,repository_identity,retryable,attempt,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, task.Job.JobID, task.Job.TenantID, task.Job.RepositoryID, task.Job.SnapshotID, task.Job.ParserVersion, task.Job.Scope, task.Job.Status, task.Job.Progress, task.Job.RetryCount, commandJSON, task.CommandFingerprint, task.RepositoryIdentity, task.Retryable, max(1, task.Attempt), task.Job.CreatedAt, task.Job.UpdatedAt)
	if err != nil {
		return repository.ParseTask{}, false, err
	}
	commandTag, err := tx.Exec(ctx, `INSERT INTO parser_idempotency(tenant_id,repository_id,idempotency_key,command_fingerprint,job_id,snapshot_id,status,retryable,retry_count,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'RUNNING',$7,$8,$9,$10) ON CONFLICT DO NOTHING`, task.Command.Scope.TenantID, task.Command.Scope.RepositoryID, task.Command.IdempotencyKey, task.CommandFingerprint, task.Job.JobID, task.Snapshot.SnapshotID, task.Retryable, task.Job.RetryCount, task.Job.CreatedAt, task.Job.UpdatedAt)
	if err != nil {
		return repository.ParseTask{}, false, err
	}
	if commandTag.RowsAffected() == 0 {
		existing, loadErr := s.taskByKeyTx(ctx, tx, task.Command.Scope, task.Command.IdempotencyKey)
		if loadErr != nil {
			return repository.ParseTask{}, false, loadErr
		}
		if existing.CommandFingerprint != task.CommandFingerprint {
			return repository.ParseTask{}, false, &repository.IdempotencyConflictError{Message: "幂等键已绑定到不同命令"}
		}
		return existing, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return repository.ParseTask{}, false, err
	}
	return task, true, nil
}

func (s *RepositoryStore) TaskByJobID(ctx context.Context, scope common.Scope, jobID string) (repository.ParseTask, error) {
	return s.scanTask(s.pool.QueryRow(ctx, taskSelect+` WHERE j.tenant_id=$1 AND j.repository_id=$2 AND j.job_id=$3`, scope.TenantID, scope.RepositoryID, jobID))
}
func (s *RepositoryStore) taskByKeyTx(ctx context.Context, tx pgx.Tx, scope common.Scope, key string) (repository.ParseTask, error) {
	return s.scanTask(tx.QueryRow(ctx, taskSelect+` JOIN parser_idempotency i ON i.job_id=j.job_id WHERE i.tenant_id=$1 AND i.repository_id=$2 AND i.idempotency_key=$3`, scope.TenantID, scope.RepositoryID, key))
}

const taskSelect = `SELECT j.command,j.command_fingerprint,j.repository_identity,j.retryable,j.attempt,COALESCE(j.lease_owner,''),j.lease_expires_at,j.cancel_requested,
j.job_id,j.snapshot_id,j.parser_version,j.scope,j.status,j.progress,COALESCE(j.error_code,''),COALESCE(j.error_message,''),j.retry_count,j.tenant_id,j.repository_id,j.created_at,j.updated_at,
s.provider,s.ref,COALESCE(s.commit_sha,''),COALESCE(s.parent_snapshot_id,''),s.sync_status,s.changed_paths,COALESCE(s.error_code,''),COALESCE(s.error_message,''),s.retry_count,s.trace_id,s.created_at,s.updated_at
FROM parse_jobs j JOIN repository_snapshots s ON s.snapshot_id=j.snapshot_id`

type rowScanner interface{ Scan(...any) error }

func (s *RepositoryStore) scanTask(row rowScanner) (repository.ParseTask, error) {
	var task repository.ParseTask
	var commandJSON, changedJSON []byte
	var lease *time.Time
	var jobStatus, snapshotStatus string
	err := row.Scan(&commandJSON, &task.CommandFingerprint, &task.RepositoryIdentity, &task.Retryable, &task.Attempt, &task.LeaseOwner, &lease, &task.CancelRequested, &task.Job.JobID, &task.Job.SnapshotID, &task.Job.ParserVersion, &task.Job.Scope, &jobStatus, &task.Job.Progress, &task.Job.ErrorCode, &task.Job.ErrorMessage, &task.Job.RetryCount, &task.Job.TenantID, &task.Job.RepositoryID, &task.Job.CreatedAt, &task.Job.UpdatedAt, &task.Snapshot.Provider, &task.Snapshot.Ref, &task.Snapshot.CommitSHA, &task.Snapshot.ParentSnapshotID, &snapshotStatus, &changedJSON, &task.Snapshot.ErrorCode, &task.Snapshot.ErrorMessage, &task.Snapshot.RetryCount, &task.Snapshot.TraceID, &task.Snapshot.CreatedAt, &task.Snapshot.UpdatedAt)
	if err != nil {
		return task, err
	}
	if err := json.Unmarshal(commandJSON, &task.Command); err != nil {
		return task, err
	}
	_ = json.Unmarshal(changedJSON, &task.Snapshot.ChangedPaths)
	task.Job.Status = repository.Status(jobStatus)
	task.Job.EntityMeta.Status = jobStatus
	task.Snapshot.SyncStatus = repository.Status(snapshotStatus)
	task.Snapshot.EntityMeta.Status = snapshotStatus
	task.Snapshot.SnapshotID = task.Job.SnapshotID
	task.Snapshot.TenantID = task.Job.TenantID
	task.Snapshot.RepositoryID = task.Job.RepositoryID
	if lease != nil {
		task.LeaseExpiresAt = *lease
	}
	return task, nil
}

func (s *RepositoryStore) ClaimPendingJob(ctx context.Context, owner string, lease time.Duration) (repository.ParseTask, bool, error) {
	if owner == "" || lease <= 0 {
		return repository.ParseTask{}, false, errors.New("owner 和 lease 必须有效")
	}
	var jobID, tenantID, repositoryID string
	err := s.pool.QueryRow(ctx, `WITH candidate AS (SELECT job_id FROM parse_jobs WHERE cancel_requested=false AND (status='PENDING' OR (status='RUNNING' AND lease_expires_at<NOW())) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE parse_jobs j SET status='RUNNING',lease_owner=$1,lease_expires_at=NOW()+$2::interval,updated_at=NOW() FROM candidate c WHERE j.job_id=c.job_id RETURNING j.job_id,j.tenant_id,j.repository_id`, owner, lease.String()).Scan(&jobID, &tenantID, &repositoryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return repository.ParseTask{}, false, nil
	}
	if err != nil {
		return repository.ParseTask{}, false, err
	}
	task, err := s.TaskByJobID(ctx, common.Scope{TenantID: tenantID, RepositoryID: repositoryID}, jobID)
	return task, err == nil, err
}
func (s *RepositoryStore) ClaimJob(ctx context.Context, jobID, owner string, lease time.Duration) (repository.ParseTask, error) {
	if owner == "" || lease <= 0 {
		return repository.ParseTask{}, errors.New("owner 和 lease 必须有效")
	}
	var tenantID, repositoryID string
	err := s.pool.QueryRow(ctx, `UPDATE parse_jobs SET status='RUNNING',lease_owner=$2,lease_expires_at=NOW()+$3::interval,updated_at=NOW() WHERE job_id=$1 AND cancel_requested=false AND (status='PENDING' OR (status='RUNNING' AND lease_expires_at<NOW())) RETURNING tenant_id,repository_id`, jobID, owner, lease.String()).Scan(&tenantID, &repositoryID)
	if err != nil {
		return repository.ParseTask{}, err
	}
	return s.TaskByJobID(ctx, common.Scope{TenantID: tenantID, RepositoryID: repositoryID}, jobID)
}
func (s *RepositoryStore) HeartbeatJob(ctx context.Context, jobID, owner string, lease time.Duration, progress int) error {
	tag, err := s.pool.Exec(ctx, `UPDATE parse_jobs SET lease_expires_at=NOW()+$3::interval,progress=$4,updated_at=NOW() WHERE job_id=$1 AND lease_owner=$2 AND status='RUNNING'`, jobID, owner, lease.String(), progress)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("任务租约已丢失")
	}
	return nil
}

func (s *RepositoryStore) SaveResultIfLatest(ctx context.Context, key, expectedParentID string, result repository.ParseResult) error {
	return s.finalize(ctx, key, expectedParentID, "", result, result.Job.Status == repository.StatusFailed || result.Job.Status == repository.StatusCancelled)
}
func (s *RepositoryStore) CompleteResult(ctx context.Context, key, expectedParentID, owner string, result repository.ParseResult) error {
	return s.finalize(ctx, key, expectedParentID, owner, result, false)
}
func (s *RepositoryStore) FailResult(ctx context.Context, key, owner string, result repository.ParseResult, retryable bool) error {
	return s.finalizeWithRetry(ctx, key, "", owner, result, true, retryable)
}
func (s *RepositoryStore) SaveResult(ctx context.Context, key string, result repository.ParseResult) error {
	return s.finalize(ctx, key, result.Snapshot.ParentSnapshotID, "", result, result.Job.Status == repository.StatusFailed || result.Job.Status == repository.StatusCancelled)
}
func (s *RepositoryStore) finalize(ctx context.Context, key, parent, owner string, result repository.ParseResult, failed bool) error {
	return s.finalizeWithRetry(ctx, key, parent, owner, result, failed, false)
}
func (s *RepositoryStore) finalizeWithRetry(ctx context.Context, key, parent, owner string, result repository.ParseResult, failed, retryable bool) error {
	if err := result.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if !failed {
		var latest string
		err = tx.QueryRow(ctx, `SELECT snapshot_id FROM repository_snapshots WHERE tenant_id=$1 AND repository_id=$2 AND sync_status='SUCCEEDED' ORDER BY created_at DESC,snapshot_id DESC LIMIT 1 FOR UPDATE`, result.Snapshot.TenantID, result.Snapshot.RepositoryID).Scan(&latest)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if errors.Is(err, pgx.ErrNoRows) {
			latest = ""
		}
		if latest != parent {
			return &repository.IdempotencyConflictError{Message: "增量基线已变化"}
		}
	}
	changed, _ := json.Marshal(result.Snapshot.ChangedPaths)
	_, err = tx.Exec(ctx, `UPDATE repository_snapshots SET provider=$2,ref=$3,commit_sha=NULLIF($4,''),parent_snapshot_id=NULLIF($5,''),sync_status=$6,changed_paths=$7,error_code=NULLIF($8,''),error_message=NULLIF($9,''),retry_count=$10,updated_at=$11 WHERE snapshot_id=$1`, result.Snapshot.SnapshotID, result.Snapshot.Provider, result.Snapshot.Ref, result.Snapshot.CommitSHA, result.Snapshot.ParentSnapshotID, result.Snapshot.SyncStatus, changed, result.Snapshot.ErrorCode, result.Snapshot.ErrorMessage, result.Snapshot.RetryCount, result.Snapshot.UpdatedAt)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE parse_jobs SET parser_version=$2,scope=$3,status=$4,progress=$5,error_code=NULLIF($6,''),error_message=NULLIF($7,''),retry_count=$8,retryable=$9,lease_owner=NULL,lease_expires_at=NULL,updated_at=$10 WHERE job_id=$1 AND ($11='' OR lease_owner=$11)`, result.Job.JobID, result.Job.ParserVersion, result.Job.Scope, result.Job.Status, result.Job.Progress, result.Job.ErrorCode, result.Job.ErrorMessage, result.Job.RetryCount, retryable, result.Job.UpdatedAt, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("任务租约已丢失")
	}
	if _, err = tx.Exec(ctx, `DELETE FROM code_artifacts WHERE snapshot_id=$1`, result.Snapshot.SnapshotID); err != nil {
		return err
	}
	for _, a := range result.Artifacts {
		source, _ := json.Marshal(a.SourceRef)
		attributes, _ := json.Marshal(a.Attributes)
		if _, err = tx.Exec(ctx, `INSERT INTO code_artifacts(snapshot_id,artifact_id,kind,name,qualified_name,language,source_ref,signature,content_hash,attributes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, result.Snapshot.SnapshotID, a.ArtifactID, a.Kind, a.Name, a.QualifiedName, a.Language, source, a.Signature, a.ContentHash, attributes); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM code_relations WHERE snapshot_id=$1`, result.Snapshot.SnapshotID); err != nil {
		return err
	}
	for _, r := range result.Relations {
		evidence, _ := json.Marshal(r.Evidence)
		if _, err = tx.Exec(ctx, `INSERT INTO code_relations(snapshot_id,relation_id,kind,from_ref,to_ref,evidence,confidence) VALUES($1,$2,$3,$4,$5,$6,$7)`, result.Snapshot.SnapshotID, r.RelationID, r.Kind, r.From, r.To, evidence, r.Confidence); err != nil {
			return err
		}
	}
	payload, _ := json.Marshal(result.Event.Payload)
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,tenant_id,repository_id,aggregate_id,event_type,payload_version,trace_id,payload,occurred_at,next_attempt_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9) ON CONFLICT(event_id) DO NOTHING`, result.Event.EventID, result.Snapshot.TenantID, result.Snapshot.RepositoryID, result.Event.AggregateID, result.Event.EventType, result.Event.PayloadVersion, result.Event.TraceID, payload, result.Event.OccurredAt)
	if err != nil {
		return err
	}
	idStatus := "SUCCEEDED"
	if failed {
		idStatus = "FAILED"
	}
	_, err = tx.Exec(ctx, `UPDATE parser_idempotency SET status=$4,retryable=$5,retry_count=$6,lease_owner=NULL,lease_expires_at=NULL,job_id=$7,snapshot_id=$8,updated_at=$9 WHERE tenant_id=$1 AND repository_id=$2 AND idempotency_key=$3`, result.Snapshot.TenantID, result.Snapshot.RepositoryID, key, idStatus, retryable, result.Job.RetryCount, result.Job.JobID, result.Snapshot.SnapshotID, result.Job.UpdatedAt)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *RepositoryStore) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error) {
	task, err := s.scanTask(s.pool.QueryRow(ctx, taskSelect+` JOIN parser_idempotency i ON i.job_id=j.job_id WHERE i.tenant_id=$1 AND i.repository_id=$2 AND i.idempotency_key=$3`, scope.TenantID, scope.RepositoryID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return repository.ParseResult{}, false, nil
	}
	if err != nil {
		return repository.ParseResult{}, false, err
	}
	result, err := s.loadResult(ctx, task)
	return result, err == nil, err
}
func (s *RepositoryStore) loadResult(ctx context.Context, task repository.ParseTask) (repository.ParseResult, error) {
	result := repository.ParseResult{Snapshot: task.Snapshot, Job: task.Job, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}, DeletedPaths: []string{}, SkippedFiles: []repository.SkippedFile{}}
	rows, err := s.pool.Query(ctx, `SELECT artifact_id,kind,name,qualified_name,language,source_ref,signature,content_hash,attributes FROM code_artifacts WHERE snapshot_id=$1 ORDER BY artifact_id`, task.Snapshot.SnapshotID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var a repository.CodeArtifact
		var source, attrs []byte
		if err := rows.Scan(&a.ArtifactID, &a.Kind, &a.Name, &a.QualifiedName, &a.Language, &source, &a.Signature, &a.ContentHash, &attrs); err != nil {
			rows.Close()
			return result, err
		}
		_ = json.Unmarshal(source, &a.SourceRef)
		_ = json.Unmarshal(attrs, &a.Attributes)
		result.Artifacts = append(result.Artifacts, a)
	}
	rows.Close()
	rows, err = s.pool.Query(ctx, `SELECT relation_id,kind,from_ref,to_ref,evidence,confidence FROM code_relations WHERE snapshot_id=$1 ORDER BY relation_id`, task.Snapshot.SnapshotID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var r repository.CodeRelation
		var evidence []byte
		if err := rows.Scan(&r.RelationID, &r.Kind, &r.From, &r.To, &evidence, &r.Confidence); err != nil {
			rows.Close()
			return result, err
		}
		_ = json.Unmarshal(evidence, &r.Evidence)
		result.Relations = append(result.Relations, r)
	}
	rows.Close()
	var payload []byte
	err = s.pool.QueryRow(ctx, `SELECT event_id,event_type,aggregate_id,payload_version,trace_id,payload,occurred_at FROM outbox_events WHERE aggregate_id=$1 ORDER BY occurred_at DESC LIMIT 1`, task.Snapshot.SnapshotID).Scan(&result.Event.EventID, &result.Event.EventType, &result.Event.AggregateID, &result.Event.PayloadVersion, &result.Event.TraceID, &payload, &result.Event.OccurredAt)
	if err == nil {
		result.Event.Producer = "repository-parser"
		_ = json.Unmarshal(payload, &result.Event.Payload)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	return result, nil
}

func (s *RepositoryStore) LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error) {
	var snap repository.Snapshot
	var status string
	var changed []byte
	err := s.pool.QueryRow(ctx, `SELECT snapshot_id,provider,ref,COALESCE(commit_sha,''),COALESCE(parent_snapshot_id,''),sync_status,changed_paths,COALESCE(error_code,''),COALESCE(error_message,''),retry_count,trace_id,created_at,updated_at FROM repository_snapshots WHERE tenant_id=$1 AND repository_id=$2 AND sync_status='SUCCEEDED' ORDER BY created_at DESC,snapshot_id DESC LIMIT 1`, scope.TenantID, scope.RepositoryID).Scan(&snap.SnapshotID, &snap.Provider, &snap.Ref, &snap.CommitSHA, &snap.ParentSnapshotID, &status, &changed, &snap.ErrorCode, &snap.ErrorMessage, &snap.RetryCount, &snap.TraceID, &snap.CreatedAt, &snap.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return snap, false, nil
	}
	if err != nil {
		return snap, false, err
	}
	snap.TenantID = scope.TenantID
	snap.RepositoryID = scope.RepositoryID
	snap.SyncStatus = repository.Status(status)
	snap.EntityMeta.Status = status
	_ = json.Unmarshal(changed, &snap.ChangedPaths)
	return snap, true, nil
}
func (s *RepositoryStore) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	var snap repository.Snapshot
	var status string
	var changed []byte
	err := s.pool.QueryRow(ctx, `SELECT snapshot_id,provider,ref,COALESCE(commit_sha,''),COALESCE(parent_snapshot_id,''),sync_status,changed_paths,COALESCE(error_code,''),COALESCE(error_message,''),retry_count,trace_id,created_at,updated_at FROM repository_snapshots WHERE tenant_id=$1 AND repository_id=$2 AND snapshot_id=$3`, scope.TenantID, scope.RepositoryID, scope.SnapshotID).Scan(&snap.SnapshotID, &snap.Provider, &snap.Ref, &snap.CommitSHA, &snap.ParentSnapshotID, &status, &changed, &snap.ErrorCode, &snap.ErrorMessage, &snap.RetryCount, &snap.TraceID, &snap.CreatedAt, &snap.UpdatedAt)
	if err != nil {
		return snap, err
	}
	snap.TenantID = scope.TenantID
	snap.RepositoryID = scope.RepositoryID
	snap.SyncStatus = repository.Status(status)
	snap.EntityMeta.Status = status
	_ = json.Unmarshal(changed, &snap.ChangedPaths)
	return snap, nil
}
func (s *RepositoryStore) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var status string
	if err := s.pool.QueryRow(ctx, `SELECT sync_status FROM repository_snapshots WHERE tenant_id=$1 AND repository_id=$2 AND snapshot_id=$3`, scope.TenantID, scope.RepositoryID, scope.SnapshotID).Scan(&status); err != nil {
		return nil, "", err
	}
	if status != "SUCCEEDED" {
		return []repository.CodeArtifact{}, "", errors.New("失败快照不提供 artifact")
	}
	rows, err := s.pool.Query(ctx, `SELECT artifact_id,kind,name,qualified_name,language,source_ref,signature,content_hash,attributes FROM code_artifacts WHERE snapshot_id=$1 AND artifact_id>$2 ORDER BY artifact_id LIMIT $3`, scope.SnapshotID, cursor, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []repository.CodeArtifact{}
	for rows.Next() {
		var a repository.CodeArtifact
		var source, attrs []byte
		if err := rows.Scan(&a.ArtifactID, &a.Kind, &a.Name, &a.QualifiedName, &a.Language, &source, &a.Signature, &a.ContentHash, &attrs); err != nil {
			return nil, "", err
		}
		_ = json.Unmarshal(source, &a.SourceRef)
		_ = json.Unmarshal(attrs, &a.Attributes)
		out = append(out, a)
	}
	next := ""
	if len(out) > limit {
		next = out[limit-1].ArtifactID
		out = out[:limit]
	}
	return out, next, rows.Err()
}

func (s *RepositoryStore) RequestCancel(ctx context.Context, scope common.Scope, jobID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var snapshotID, status string
	if err := tx.QueryRow(ctx, `SELECT snapshot_id,status FROM parse_jobs WHERE tenant_id=$1 AND repository_id=$2 AND job_id=$3 FOR UPDATE`, scope.TenantID, scope.RepositoryID, jobID).Scan(&snapshotID, &status); err != nil {
		return err
	}
	if status != string(repository.StatusPending) && status != string(repository.StatusRunning) {
		return errors.New("任务不可取消")
	}
	if status == string(repository.StatusPending) {
		if _, err = tx.Exec(ctx, `UPDATE parse_jobs SET cancel_requested=true,status='CANCELLED',error_code='INVALID_INPUT',error_message='解析任务已取消',updated_at=NOW() WHERE job_id=$1`, jobID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE repository_snapshots SET sync_status='CANCELLED',error_code='INVALID_INPUT',error_message='解析任务已取消',updated_at=NOW() WHERE snapshot_id=$1`, snapshotID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE parser_idempotency SET status='FAILED',retryable=false,updated_at=NOW() WHERE job_id=$1`, jobID); err != nil {
			return err
		}
	} else if _, err = tx.Exec(ctx, `UPDATE parse_jobs SET cancel_requested=true,updated_at=NOW() WHERE job_id=$1`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *RepositoryStore) RetryFailed(ctx context.Context, scope common.Scope, jobID, newJobID, newSnapshotID, commitSHA, parentSnapshotID string, now time.Time) (repository.ParseTask, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return repository.ParseTask{}, err
	}
	defer tx.Rollback(ctx)
	task, err := s.scanTask(tx.QueryRow(ctx, taskSelect+` WHERE j.tenant_id=$1 AND j.repository_id=$2 AND j.job_id=$3 FOR UPDATE`, scope.TenantID, scope.RepositoryID, jobID))
	if err != nil {
		return repository.ParseTask{}, err
	}
	if task.Job.Status != repository.StatusFailed || !task.Retryable {
		return task, errors.New("任务不可重试")
	}
	task.Job.JobID = newJobID
	task.Job.SnapshotID = newSnapshotID
	task.Job.Status = repository.StatusPending
	task.Job.EntityMeta.Status = string(repository.StatusPending)
	task.Job.Progress = 0
	task.Job.ErrorCode = ""
	task.Job.ErrorMessage = ""
	task.Job.Scope = repository.ScopeFull
	if parentSnapshotID != "" {
		task.Job.Scope = repository.ScopeIncremental
	}
	task.Job.RetryCount++
	task.Job.CreatedAt = now
	task.Job.UpdatedAt = now
	task.Snapshot.SnapshotID = newSnapshotID
	task.Snapshot.CommitSHA = commitSHA
	task.Snapshot.ParentSnapshotID = parentSnapshotID
	task.Snapshot.SyncStatus = repository.StatusPending
	task.Snapshot.EntityMeta.Status = string(repository.StatusPending)
	task.Snapshot.ErrorCode = ""
	task.Snapshot.ErrorMessage = ""
	task.Snapshot.CreatedAt = now
	task.Snapshot.UpdatedAt = now
	task.Attempt++
	task.CancelRequested = false
	commandJSON, err := json.Marshal(task.Command)
	if err != nil {
		return repository.ParseTask{}, err
	}
	changedJSON, _ := json.Marshal([]repository.ChangedPath{})
	if _, err = tx.Exec(ctx, `INSERT INTO repository_snapshots(snapshot_id,tenant_id,repository_id,provider,ref,commit_sha,parent_snapshot_id,sync_status,changed_paths,trace_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),'PENDING',$8,$9,$10,$10)`, newSnapshotID, scope.TenantID, scope.RepositoryID, task.Snapshot.Provider, task.Snapshot.Ref, task.Snapshot.CommitSHA, task.Snapshot.ParentSnapshotID, changedJSON, task.Snapshot.TraceID, now); err != nil {
		return repository.ParseTask{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO parse_jobs(job_id,tenant_id,repository_id,snapshot_id,parser_version,scope,status,progress,retry_count,command,command_fingerprint,repository_identity,retryable,attempt,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'PENDING',0,$7,$8,$9,$10,false,$11,$12,$12)`, newJobID, scope.TenantID, scope.RepositoryID, newSnapshotID, task.Job.ParserVersion, task.Job.Scope, task.Job.RetryCount, commandJSON, task.CommandFingerprint, task.RepositoryIdentity, task.Attempt, now); err != nil {
		return repository.ParseTask{}, err
	}
	tag, err := tx.Exec(ctx, `UPDATE parser_idempotency SET job_id=$4,snapshot_id=$5,status='RUNNING',retryable=false,retry_count=retry_count+1,lease_owner=NULL,lease_expires_at=NULL,updated_at=$6 WHERE tenant_id=$1 AND repository_id=$2 AND idempotency_key=$3 AND job_id=$7 AND status='FAILED' AND retryable=true`, scope.TenantID, scope.RepositoryID, task.Command.IdempotencyKey, newJobID, newSnapshotID, now, jobID)
	if err != nil {
		return repository.ParseTask{}, err
	}
	if tag.RowsAffected() != 1 {
		return repository.ParseTask{}, &repository.IdempotencyConflictError{Message: "失败任务已被其他请求重试"}
	}
	if err := tx.Commit(ctx); err != nil {
		return repository.ParseTask{}, err
	}
	return task, nil
}

func (s *RepositoryStore) PendingOutbox(ctx context.Context, limit int, now time.Time) ([]repository.OutboxRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id,repository_id,event_id,event_type,aggregate_id,trace_id,payload_version,payload,occurred_at,delivery_count,COALESCE(last_error,''),next_attempt_at FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL AND next_attempt_at<=$1 ORDER BY occurred_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []repository.OutboxRecord{}
	for rows.Next() {
		var r repository.OutboxRecord
		if err := rows.Scan(&r.TenantID, &r.RepositoryID, &r.EventID, &r.EventType, &r.AggregateID, &r.TraceID, &r.PayloadVersion, &r.Payload, &r.OccurredAt, &r.DeliveryCount, &r.LastError, &r.NextAttemptAt); err != nil {
			return nil, err
		}
		r.Event = common.EventEnvelope{EventID: r.EventID, EventType: r.EventType, AggregateID: r.AggregateID, TraceID: r.TraceID, PayloadVersion: r.PayloadVersion, Producer: "repository-parser", OccurredAt: r.OccurredAt}
		_ = json.Unmarshal(r.Payload, &r.Event.Payload)
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *RepositoryStore) MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at=$2,delivery_count=delivery_count+1,last_error=NULL WHERE event_id=$1 AND published_at IS NULL`, eventID, publishedAt)
	return err
}
func (s *RepositoryStore) MarkOutboxFailed(ctx context.Context, eventID, sanitizedError string, next time.Time, dead bool) error {
	if len(sanitizedError) > 512 {
		sanitizedError = sanitizedError[:512]
	}
	var deadAt any
	if dead {
		deadAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET delivery_count=delivery_count+1,last_error=$2,next_attempt_at=$3,dead_lettered_at=$4 WHERE event_id=$1 AND published_at IS NULL`, eventID, sanitizedError, next, deadAt)
	return err
}

func (s *RepositoryStore) CleanupExpired(ctx context.Context, now time.Time, policy ports.RetentionPolicy) (ports.RetentionResult, error) {
	var result ports.RetentionResult
	if policy.FailedTaskRetention <= 0 || policy.OutboxRetention <= 0 {
		return result, errors.New("retention policy 必须为正数")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	failedBefore := now.Add(-policy.FailedTaskRetention)
	if _, err = tx.Exec(ctx, `DELETE FROM parser_idempotency i USING parse_jobs j WHERE i.job_id=j.job_id AND j.status IN ('FAILED','CANCELLED') AND j.updated_at<$1`, failedBefore); err != nil {
		return result, err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM parse_jobs WHERE status IN ('FAILED','CANCELLED') AND updated_at<$1`, failedBefore)
	if err != nil {
		return result, err
	}
	result.FailedTasks = tag.RowsAffected()
	if _, err = tx.Exec(ctx, `DELETE FROM repository_snapshots WHERE sync_status IN ('FAILED','CANCELLED') AND updated_at<$1`, failedBefore); err != nil {
		return result, err
	}
	tag, err = tx.Exec(ctx, `DELETE FROM outbox_events WHERE (published_at IS NOT NULL OR dead_lettered_at IS NOT NULL) AND occurred_at<$1`, now.Add(-policy.OutboxRetention))
	if err != nil {
		return result, err
	}
	result.OutboxEvents = tag.RowsAffected()
	return result, tx.Commit(ctx)
}

func isUnique(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "23505"
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
