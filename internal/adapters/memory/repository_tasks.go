package memory

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

func (s *RepositoryStore) BindRepository(ctx context.Context, b repository.RepositoryBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := scopeKey(common.Scope{TenantID: b.TenantID, RepositoryID: b.RepositoryID})
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.bindings[key]; ok && (existing.CanonicalIdentity != b.CanonicalIdentity || existing.Provider != b.Provider) {
		return &repository.IdempotencyConflictError{Message: "RepositoryID 已绑定到另一仓库"}
	}
	s.bindings[key] = b
	return nil
}

func (s *RepositoryStore) AcquireIdempotency(ctx context.Context, task repository.ParseTask) (repository.ParseTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseTask{}, false, err
	}
	key := resultKey(task.Command.Scope, task.Command.IdempotencyKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if jobID, ok := s.taskKeys[key]; ok {
		existing := s.tasks[jobID]
		if existing.CommandFingerprint != task.CommandFingerprint {
			return repository.ParseTask{}, false, &repository.IdempotencyConflictError{Message: "幂等键已绑定到不同命令"}
		}
		return cloneTask(existing), false, nil
	}
	task.Job.Status = repository.StatusPending
	task.Job.EntityMeta.Status = string(repository.StatusPending)
	task.Snapshot.SyncStatus = repository.StatusPending
	task.Snapshot.EntityMeta.Status = string(repository.StatusPending)
	s.tasks[task.Job.JobID] = cloneTask(task)
	s.taskKeys[key] = task.Job.JobID
	s.snapshots[task.Snapshot.SnapshotID] = task.Snapshot
	return cloneTask(task), true, nil
}

func (s *RepositoryStore) TaskByJobID(ctx context.Context, scope common.Scope, jobID string) (repository.ParseTask, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseTask{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[jobID]
	if !ok || task.Job.TenantID != scope.TenantID || task.Job.RepositoryID != scope.RepositoryID {
		return repository.ParseTask{}, errors.New("任务不存在")
	}
	return cloneTask(task), nil
}

func (s *RepositoryStore) ClaimPendingJob(ctx context.Context, owner string, lease time.Duration) (repository.ParseTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseTask{}, false, err
	}
	if owner == "" || lease <= 0 {
		return repository.ParseTask{}, false, errors.New("owner 和 lease 必须有效")
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return s.tasks[ids[i]].Job.CreatedAt.Before(s.tasks[ids[j]].Job.CreatedAt) })
	for _, id := range ids {
		task := s.tasks[id]
		claimable := task.Job.Status == repository.StatusPending || (task.Job.Status == repository.StatusRunning && !task.LeaseExpiresAt.IsZero() && task.LeaseExpiresAt.Before(now))
		if !claimable || task.CancelRequested {
			continue
		}
		task.Job.Status = repository.StatusRunning
		task.Job.EntityMeta.Status = string(repository.StatusRunning)
		task.Snapshot.SyncStatus = repository.StatusRunning
		task.Snapshot.EntityMeta.Status = string(repository.StatusRunning)
		task.LeaseOwner = owner
		task.LeaseExpiresAt = now.Add(lease)
		task.Job.UpdatedAt = now
		task.Snapshot.UpdatedAt = now
		s.tasks[id] = task
		s.snapshots[task.Snapshot.SnapshotID] = task.Snapshot
		return cloneTask(task), true, nil
	}
	return repository.ParseTask{}, false, nil
}
func (s *RepositoryStore) ClaimJob(ctx context.Context, jobID, owner string, lease time.Duration) (repository.ParseTask, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseTask{}, err
	}
	if owner == "" || lease <= 0 {
		return repository.ParseTask{}, errors.New("owner 和 lease 必须有效")
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[jobID]
	if !ok {
		return repository.ParseTask{}, errors.New("任务不存在")
	}
	if task.CancelRequested {
		return repository.ParseTask{}, errors.New("任务已取消")
	}
	if task.Job.Status != repository.StatusPending && !(task.Job.Status == repository.StatusRunning && task.LeaseExpiresAt.Before(now)) {
		return repository.ParseTask{}, errors.New("任务不可领取")
	}
	task.Job.Status = repository.StatusRunning
	task.Job.EntityMeta.Status = string(repository.StatusRunning)
	task.Snapshot.SyncStatus = repository.StatusRunning
	task.Snapshot.EntityMeta.Status = string(repository.StatusRunning)
	task.LeaseOwner = owner
	task.LeaseExpiresAt = now.Add(lease)
	task.Job.UpdatedAt = now
	task.Snapshot.UpdatedAt = now
	s.tasks[jobID] = task
	s.snapshots[task.Snapshot.SnapshotID] = task.Snapshot
	return cloneTask(task), nil
}

func (s *RepositoryStore) HeartbeatJob(ctx context.Context, jobID, owner string, lease time.Duration, progress int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[jobID]
	if !ok || task.LeaseOwner != owner || task.Job.Status != repository.StatusRunning {
		return errors.New("任务租约已丢失")
	}
	task.LeaseExpiresAt = time.Now().UTC().Add(lease)
	task.Job.Progress = progress
	task.Job.UpdatedAt = time.Now().UTC()
	s.tasks[jobID] = task
	return nil
}

func (s *RepositoryStore) SaveResultIfLatest(ctx context.Context, key, parent string, result repository.ParseResult) error {
	return s.complete(ctx, key, parent, "", result, false, false)
}
func (s *RepositoryStore) CompleteResult(ctx context.Context, key, parent, owner string, result repository.ParseResult) error {
	return s.complete(ctx, key, parent, owner, result, false, false)
}
func (s *RepositoryStore) FailResult(ctx context.Context, key, owner string, result repository.ParseResult, retryable bool) error {
	return s.complete(ctx, key, "", owner, result, true, retryable)
}
func (s *RepositoryStore) complete(ctx context.Context, key, parent, owner string, result repository.ParseResult, failed, retryable bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := result.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, ok := s.tasks[result.Job.JobID]; owner != "" && (!ok || task.LeaseOwner != owner) {
		return errors.New("任务租约已丢失")
	}
	scope := common.Scope{TenantID: result.Snapshot.TenantID, RepositoryID: result.Snapshot.RepositoryID}
	if !failed && s.latest[scopeKey(scope)] != parent {
		return &repository.IdempotencyConflictError{Message: "增量基线已变化"}
	}
	if err := s.saveResultLocked(key, result); err != nil {
		return err
	}
	if task, ok := s.tasks[result.Job.JobID]; ok {
		task.Job = result.Job
		task.Snapshot = result.Snapshot
		task.Retryable = retryable
		task.LeaseOwner = ""
		task.LeaseExpiresAt = time.Time{}
		s.tasks[result.Job.JobID] = task
	}
	return nil
}

func (s *RepositoryStore) RequestCancel(ctx context.Context, scope common.Scope, jobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[jobID]
	if !ok || task.Job.TenantID != scope.TenantID || task.Job.RepositoryID != scope.RepositoryID {
		return errors.New("任务不存在")
	}
	if task.Job.Status != repository.StatusPending && task.Job.Status != repository.StatusRunning {
		return errors.New("任务不可取消")
	}
	task.CancelRequested = true
	if task.Job.Status == repository.StatusPending {
		task.Job.Status = repository.StatusCancelled
		task.Job.EntityMeta.Status = string(repository.StatusCancelled)
		task.Job.ErrorCode, task.Job.ErrorMessage = string(repository.ErrInvalidInput), "解析任务已取消"
		task.Snapshot.SyncStatus = repository.StatusCancelled
		task.Snapshot.EntityMeta.Status = string(repository.StatusCancelled)
		task.Snapshot.ErrorCode, task.Snapshot.ErrorMessage = string(repository.ErrInvalidInput), "解析任务已取消"
	}
	s.tasks[jobID] = task
	return nil
}

func (s *RepositoryStore) RetryFailed(ctx context.Context, scope common.Scope, jobID, newJobID, newSnapshotID, commitSHA, parentSnapshotID string, now time.Time) (repository.ParseTask, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseTask{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.tasks[jobID]
	if !ok || old.Job.TenantID != scope.TenantID || old.Job.RepositoryID != scope.RepositoryID {
		return repository.ParseTask{}, errors.New("任务不存在")
	}
	if old.Job.Status != repository.StatusFailed || !old.Retryable {
		return repository.ParseTask{}, errors.New("任务不可重试")
	}
	task := cloneTask(old)
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
	s.tasks[newJobID] = task
	s.taskKeys[resultKey(scope, task.Command.IdempotencyKey)] = newJobID
	s.snapshots[newSnapshotID] = task.Snapshot
	return cloneTask(task), nil
}

func (s *RepositoryStore) PendingOutbox(ctx context.Context, limit int, now time.Time) ([]repository.OutboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []repository.OutboxRecord{}
	for _, record := range s.outbox {
		if record.PublishedAt.IsZero() && record.DeadLetteredAt.IsZero() && !record.NextAttemptAt.After(now) {
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *RepositoryStore) MarkOutboxPublished(ctx context.Context, eventID string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.outbox[eventID]
	if !ok {
		return errors.New("outbox event 不存在")
	}
	record.PublishedAt = at
	record.DeliveryCount++
	s.outbox[eventID] = record
	return nil
}
func (s *RepositoryStore) MarkOutboxFailed(ctx context.Context, eventID, message string, next time.Time, dead bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.outbox[eventID]
	if !ok {
		return errors.New("outbox event 不存在")
	}
	record.DeliveryCount++
	record.LastError = message
	record.NextAttemptAt = next
	if dead {
		record.DeadLetteredAt = time.Now().UTC()
	}
	s.outbox[eventID] = record
	return nil
}

func cloneTask(task repository.ParseTask) repository.ParseTask {
	task.Command.IncludePaths = append([]string(nil), task.Command.IncludePaths...)
	task.Snapshot.ChangedPaths = append([]repository.ChangedPath(nil), task.Snapshot.ChangedPaths...)
	return task
}

func (s *RepositoryStore) CleanupExpired(ctx context.Context, now time.Time, policy ports.RetentionPolicy) (ports.RetentionResult, error) {
	var result ports.RetentionResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if policy.FailedTaskRetention <= 0 || policy.OutboxRetention <= 0 {
		return result, errors.New("retention policy 必须为正数")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	failedBefore := now.Add(-policy.FailedTaskRetention)
	for id, task := range s.tasks {
		if (task.Job.Status == repository.StatusFailed || task.Job.Status == repository.StatusCancelled) && task.Job.UpdatedAt.Before(failedBefore) {
			key := resultKey(task.Command.Scope, task.Command.IdempotencyKey)
			delete(s.tasks, id)
			delete(s.snapshots, task.Snapshot.SnapshotID)
			delete(s.taskKeys, key)
			delete(s.results, key)
			result.FailedTasks++
		}
	}
	outboxBefore := now.Add(-policy.OutboxRetention)
	for id, record := range s.outbox {
		if (!record.PublishedAt.IsZero() || !record.DeadLetteredAt.IsZero()) && record.OccurredAt.Before(outboxBefore) {
			delete(s.outbox, id)
			result.OutboxEvents++
		}
	}
	return result, nil
}
