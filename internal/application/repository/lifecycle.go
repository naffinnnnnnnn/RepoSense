package repositoryapp

import (
	"context"
	"errors"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

const synchronousLease = 2 * time.Minute

func (s *Service) Submit(ctx context.Context, cmd repository.SyncCommand) (repository.ParseJob, error) {
	taskStore, ok := s.store.(ports.RepositoryTaskStore)
	if !ok {
		return repository.ParseJob{}, errors.New("RepositoryStore 不支持异步任务契约")
	}
	if err := cmd.Validate(); err != nil {
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if err := validatePatterns(cmd.IncludePaths); err != nil {
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	fingerprint := commandFingerprint(cmd)
	var cachedTask repository.ParseTask
	if cached, found, lookupErr := s.store.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); lookupErr != nil {
		return repository.ParseJob{}, domainError(repository.ErrPersistence, "idempotency_lookup", "检查幂等键失败", true, lookupErr)
	} else if found {
		s.observer.Count("repository_parse_idempotency_hits_total", 1, labels(cmd.Scope))
		task, taskErr := taskStore.TaskByJobID(ctx, cmd.Scope, cached.Job.JobID)
		if taskErr != nil {
			return repository.ParseJob{}, taskErr
		}
		if task.CommandFingerprint != fingerprint {
			s.observer.Count("repository_parse_idempotency_conflicts_total", 1, labels(cmd.Scope))
			return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "idempotency_conflict", "幂等键已绑定到不同命令", false, nil)
		}
		cachedTask = task
		if task.Job.Status == repository.StatusPending || task.Job.Status == repository.StatusRunning {
			return task.Job, nil
		}
		if task.Job.Status == repository.StatusSucceeded {
			if !isMutableRef(cmd.Ref) {
				return task.Job, nil
			}
			s.locksMu.Lock()
			published, known := s.published[cached.Event.EventID]
			s.locksMu.Unlock()
			if known && !published {
				return task.Job, nil
			}
		}
	}
	identity := ""
	if s.workspace != nil {
		prepared, err := s.workspace.Prepare(ctx, cmd)
		if err != nil {
			return repository.ParseJob{}, domainError(repository.ErrGitFailure, "prepare_workspace", "准备仓库工作区失败", true, err)
		}
		cmd.RepositoryPath = prepared.Path
		cmd.Provider = prepared.Provider
		identity = prepared.CanonicalIdentity
	} else if cmd.RepositoryURL != "" {
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "prepare_workspace", "远程仓库需要 RepositoryWorkspace", false, nil)
	}
	if identity == "" {
		var err error
		identity, err = canonicalRepositoryIdentity(cmd.RepositoryPath)
		if err != nil {
			return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "repository_identity", "仓库路径无效", false, err)
		}
	}
	now := s.clock.Now().UTC()
	binding := repository.RepositoryBinding{TenantID: cmd.Scope.TenantID, RepositoryID: cmd.Scope.RepositoryID, Provider: providerOrLocal(cmd.Provider), CanonicalIdentity: identity, CreatedAt: now, UpdatedAt: now}
	if err := taskStore.BindRepository(ctx, binding); err != nil {
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "repository_identity", "RepositoryID 已绑定到另一仓库", false, err)
	}
	commit, err := s.git.ResolveCommit(ctx, cmd.RepositoryPath, cmd.Ref)
	if err != nil {
		return s.failSubmission(ctx, taskStore, cmd, fingerprint, identity, "", descriptorFor(err, repository.ErrGitFailure, "resolve_commit", "解析 Git 引用失败", true), err)
	}
	if cachedTask.Job.Status == repository.StatusSucceeded {
		if cachedTask.Snapshot.CommitSHA == commit {
			return cachedTask.Job, nil
		}
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "idempotency_conflict", "可变 ref 已指向新的 commit，请使用新的幂等键", false, nil)
	}
	previous, hasPrevious, err := s.store.LatestSnapshot(ctx, cmd.Scope)
	if err != nil {
		return s.failSubmission(ctx, taskStore, cmd, fingerprint, identity, commit, describe(repository.ErrPersistence, "latest_snapshot", "加载增量基线失败", true), err)
	}
	snapshotID, err := s.newID("snap")
	if err != nil {
		return repository.ParseJob{}, domainError(repository.ErrPersistence, "generate_id", "生成任务标识失败", true, err)
	}
	jobID, err := s.newID("job")
	if err != nil {
		return repository.ParseJob{}, domainError(repository.ErrPersistence, "generate_id", "生成任务标识失败", true, err)
	}
	version := "registry@2"
	if registry, ok := s.parsers.(interface{ Version() string }); ok {
		version = registry.Version()
	}
	scope := repository.ScopeFull
	parent := ""
	if hasPrevious {
		scope = repository.ScopeIncremental
		parent = previous.SnapshotID
	}
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, cmd.Scope, repository.StatusPending, now), SnapshotID: snapshotID, Provider: providerOrLocal(cmd.Provider), Ref: cmd.Ref, CommitSHA: commit, ParentSnapshotID: parent, SyncStatus: repository.StatusPending, ChangedPaths: []repository.ChangedPath{}}
	job := repository.ParseJob{EntityMeta: repository.NewMeta(jobID, cmd.Scope, repository.StatusPending, now), JobID: jobID, SnapshotID: snapshotID, ParserVersion: version, Scope: scope, Status: repository.StatusPending}
	task := repository.ParseTask{Command: cmd, CommandFingerprint: fingerprint, RepositoryIdentity: identity, Job: job, Snapshot: snapshot, Attempt: 1}
	acquired, created, err := taskStore.AcquireIdempotency(ctx, task)
	if err != nil {
		var conflict *repository.IdempotencyConflictError
		if errors.As(err, &conflict) {
			return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "idempotency_conflict", conflict.Error(), false, err)
		}
		return repository.ParseJob{}, domainError(repository.ErrPersistence, "acquire_idempotency", "创建解析任务失败", true, err)
	}
	if created {
		return acquired.Job, nil
	}
	switch acquired.Job.Status {
	case repository.StatusPending, repository.StatusRunning, repository.StatusSucceeded:
		return acquired.Job, nil
	case repository.StatusFailed:
		if !acquired.Retryable {
			return acquired.Job, domainError(repository.ErrorCode(acquired.Job.ErrorCode), "idempotency_failed", acquired.Job.ErrorMessage, false, nil)
		}
		newSnapshotID, idErr := s.newID("snap")
		if idErr != nil {
			return repository.ParseJob{}, idErr
		}
		newJobID, idErr := s.newID("job")
		if idErr != nil {
			return repository.ParseJob{}, idErr
		}
		retried, retryErr := taskStore.RetryFailed(ctx, cmd.Scope, acquired.Job.JobID, newJobID, newSnapshotID, commit, parent, now)
		if retryErr != nil {
			return repository.ParseJob{}, domainError(repository.ErrPersistence, "retry_task", "创建重试任务失败", true, retryErr)
		}
		return retried.Job, nil
	case repository.StatusCancelled:
		return acquired.Job, domainError(repository.ErrInvalidInput, "idempotency_cancelled", "解析任务已取消，请使用新的幂等键", false, nil)
	default:
		return repository.ParseJob{}, domainError(repository.ErrPersistence, "idempotency_state", "未知幂等状态", false, nil)
	}
}

func (s *Service) Execute(ctx context.Context, scope common.Scope, jobID string) (repository.ParseResult, error) {
	return s.execute(ctx, scope, jobID, "")
}

func (s *Service) execute(ctx context.Context, scope common.Scope, jobID, claimedOwner string) (repository.ParseResult, error) {
	taskStore, ok := s.store.(ports.RepositoryTaskStore)
	if !ok {
		return repository.ParseResult{}, errors.New("RepositoryStore 不支持异步任务契约")
	}
	task, err := taskStore.TaskByJobID(ctx, scope, jobID)
	if err != nil {
		return repository.ParseResult{}, domainError(repository.ErrPersistence, "load_task", "加载解析任务失败", true, err)
	}
	if task.Job.Status == repository.StatusSucceeded || task.Job.Status == repository.StatusFailed || task.Job.Status == repository.StatusCancelled {
		result, found, loadErr := s.store.FindByIdempotencyKey(ctx, scope, task.Command.IdempotencyKey)
		if loadErr != nil {
			return result, loadErr
		}
		if found {
			if task.Job.Status == repository.StatusFailed || task.Job.Status == repository.StatusCancelled {
				return result, domainError(repository.ErrorCode(result.Job.ErrorCode), "cached_failure", result.Job.ErrorMessage, task.Retryable, nil)
			}
			return result, nil
		}
	}
	owner := "execute:" + jobID
	if task.Job.Status == repository.StatusPending {
		task, err = taskStore.ClaimJob(ctx, jobID, owner, synchronousLease)
		if err != nil {
			return repository.ParseResult{}, domainError(repository.ErrPersistence, "claim_job", "领取解析任务失败", true, err)
		}
	} else if task.Job.Status == repository.StatusRunning {
		if claimedOwner == "" || task.LeaseOwner != claimedOwner {
			return repository.ParseResult{Job: task.Job, Snapshot: task.Snapshot}, domainError(repository.ErrPersistence, "job_already_running", "解析任务已由其他 Worker 领取", true, nil)
		}
	}
	if task.CancelRequested || task.Job.Status == repository.StatusCancelled {
		return repository.ParseResult{Job: task.Job, Snapshot: task.Snapshot}, domainError(repository.ErrInvalidInput, "cancelled", "解析任务已取消", false, nil)
	}
	ids := &acceptedIDs{snapshotID: task.Snapshot.SnapshotID, jobID: task.Job.JobID, delegate: s.ids}
	store := acceptedStore{RepositoryStore: s.store, tasks: taskStore, observer: s.observer, key: task.Command.IdempotencyKey, parent: task.Snapshot.ParentSnapshotID, jobID: task.Job.JobID, owner: task.LeaseOwner}
	direct, createErr := New(resolvedGit{GitRepository: s.git, commit: task.Snapshot.CommitSHA}, s.parsers, store, noopPublisher{}, s.observer, ids, s.clock, s.config)
	if createErr != nil {
		return repository.ParseResult{}, createErr
	}
	command := task.Command
	command.Ref = task.Snapshot.CommitSHA
	command.RepositoryURL = ""
	result, executeErr := direct.syncDirect(ctx, command)
	return result, executeErr
}

func (s *Service) Sync(ctx context.Context, cmd repository.SyncCommand) (repository.ParseResult, error) {
	if _, ok := s.store.(ports.RepositoryTaskStore); !ok {
		return s.syncDirect(ctx, cmd)
	}
	unlock, _ := s.acquireFor(s.keyLocks, scopeKey(cmd.Scope)+"\x00"+cmd.IdempotencyKey)
	defer unlock()
	job, err := s.Submit(ctx, cmd)
	if err != nil {
		if job.JobID == "" {
			return repository.ParseResult{}, err
		}
		result, found, loadErr := s.store.FindByIdempotencyKey(context.WithoutCancel(ctx), cmd.Scope, cmd.IdempotencyKey)
		if loadErr != nil || !found {
			return result, errors.Join(err, loadErr)
		}
		return result, err
	}
	result, executeErr := s.Execute(ctx, cmd.Scope, job.JobID)
	if result.Event.EventID != "" {
		s.locksMu.Lock()
		already := s.published[result.Event.EventID]
		s.locksMu.Unlock()
		if !already {
			s.setPublished(result.Event.EventID, false)
			publishErr := s.events.Publish(ctx, result.Event)
			if publishErr != nil {
				return result, errors.Join(executeErr, domainError(repository.ErrPersistence, "publish_event", "Outbox 事件同步投递失败", true, publishErr))
			}
			s.markPublished(result.Event.EventID)
			if outbox, ok := s.store.(ports.OutboxStore); ok {
				_ = outbox.MarkOutboxPublished(context.WithoutCancel(ctx), result.Event.EventID, s.clock.Now().UTC())
			}
		}
	}
	return result, executeErr
}

// failSubmission persists failures that happen after repository identity has been
// accepted but before a worker can execute the task. This keeps early runtime
// failures queryable and uses the same transactional result/outbox boundary.
func (s *Service) failSubmission(ctx context.Context, store ports.RepositoryTaskStore, cmd repository.SyncCommand, fingerprint, identity, commit string, failure repository.FailureDescriptor, cause error) (repository.ParseJob, error) {
	now := s.clock.Now().UTC()
	snapshotID, idErr := s.newID("snap")
	if idErr != nil {
		return repository.ParseJob{}, errors.Join(descriptorError(failure, cause), domainError(repository.ErrPersistence, "generate_id", "生成失败任务标识失败", true, idErr))
	}
	jobID, idErr := s.newID("job")
	if idErr != nil {
		return repository.ParseJob{}, errors.Join(descriptorError(failure, cause), domainError(repository.ErrPersistence, "generate_id", "生成失败任务标识失败", true, idErr))
	}
	version := "registry@2"
	if registry, ok := s.parsers.(interface{ Version() string }); ok {
		version = registry.Version()
	}
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, cmd.Scope, repository.StatusPending, now), SnapshotID: snapshotID, Provider: providerOrLocal(cmd.Provider), Ref: cmd.Ref, CommitSHA: commit, SyncStatus: repository.StatusPending, ChangedPaths: []repository.ChangedPath{}}
	job := repository.ParseJob{EntityMeta: repository.NewMeta(jobID, cmd.Scope, repository.StatusPending, now), JobID: jobID, SnapshotID: snapshotID, ParserVersion: version, Scope: repository.ScopeFull, Status: repository.StatusPending}
	task := repository.ParseTask{Command: cmd, CommandFingerprint: fingerprint, RepositoryIdentity: identity, Job: job, Snapshot: snapshot, Attempt: 1}
	acquired, created, acquireErr := store.AcquireIdempotency(context.WithoutCancel(ctx), task)
	if acquireErr != nil {
		return repository.ParseJob{}, errors.Join(descriptorError(failure, cause), domainError(repository.ErrPersistence, "acquire_idempotency", "创建失败任务记录失败", true, acquireErr))
	}
	if !created {
		return acquired.Job, nil
	}
	failedAt := notBefore(s.clock.Now().UTC(), now)
	job.Status, job.EntityMeta.Status, job.ErrorCode, job.ErrorMessage, job.UpdatedAt = repository.StatusFailed, string(repository.StatusFailed), string(failure.Code), failure.Message, failedAt
	snapshot.SyncStatus, snapshot.EntityMeta.Status, snapshot.ErrorCode, snapshot.ErrorMessage, snapshot.UpdatedAt = repository.StatusFailed, string(repository.StatusFailed), string(failure.Code), failure.Message, failedAt
	eventID, eventErr := s.newID("evt")
	if eventErr != nil {
		return job, errors.Join(descriptorError(failure, cause), domainError(repository.ErrPersistence, "generate_event_id", "生成失败事件标识失败", true, eventErr))
	}
	result := repository.ParseResult{Job: job, Snapshot: snapshot, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}, DeletedPaths: []string{}, SkippedFiles: []repository.SkippedFile{}, Event: repository.NewParseFailedEvent(eventID, cmd.Scope, failedAt, repository.ParseFailedPayload{SnapshotID: snapshotID, ErrorCode: failure.Code, Retryable: failure.Retryable})}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.FailureCleanupTimeout)
	defer cancel()
	if saveErr := store.FailResult(cleanupCtx, cmd.IdempotencyKey, "", result, failure.Retryable); saveErr != nil {
		return job, errors.Join(descriptorError(failure, cause), domainError(repository.ErrPersistence, "save_failure", "保存提交失败状态失败", true, saveErr))
	}
	s.observer.Count("repository_parse_jobs_total", 1, map[string]string{"status": string(repository.StatusFailed), "error_code": string(failure.Code), "operation": failure.Operation})
	return job, descriptorError(failure, cause)
}

type acceptedIDs struct {
	snapshotID, jobID string
	delegate          ports.IDGenerator
	calls             int
}

func (i *acceptedIDs) New(prefix string) string { id, _ := i.NewID(prefix); return id }
func (i *acceptedIDs) NewID(prefix string) (string, error) {
	i.calls++
	if i.calls == 1 {
		return i.snapshotID, nil
	}
	if i.calls == 2 {
		return i.jobID, nil
	}
	if generator, ok := i.delegate.(ports.FallibleIDGenerator); ok {
		return generator.NewID(prefix)
	}
	id := i.delegate.New(prefix)
	if id == "" {
		return "", errors.New("ID 生成器返回空值")
	}
	return id, nil
}

type acceptedStore struct {
	ports.RepositoryStore
	tasks                     ports.RepositoryTaskStore
	observer                  ports.Observer
	key, parent, jobID, owner string
}

func (acceptedStore) FindByIdempotencyKey(context.Context, common.Scope, string) (repository.ParseResult, bool, error) {
	return repository.ParseResult{}, false, nil
}
func (s acceptedStore) SaveResult(ctx context.Context, _ string, result repository.ParseResult) error {
	if result.Job.Status == repository.StatusSucceeded {
		err := s.tasks.CompleteResult(ctx, s.key, s.parent, s.owner, result)
		var conflict *repository.IdempotencyConflictError
		if errors.As(err, &conflict) && s.observer != nil {
			s.observer.Count("repository_parse_snapshot_conflicts_total", 1, nil)
		}
		return err
	}
	retryable, _ := result.Event.Payload["retryable"].(bool)
	return s.tasks.FailResult(ctx, s.key, s.owner, result, retryable)
}
func (s acceptedStore) UpdateProgress(ctx context.Context, progress int) error {
	if s.owner == "" {
		return errors.New("任务缺少租约所有者")
	}
	return s.tasks.HeartbeatJob(ctx, s.jobID, s.owner, synchronousLease, progress)
}

func descriptorError(descriptor repository.FailureDescriptor, cause error) error {
	return domainError(descriptor.Code, descriptor.Operation, descriptor.Message, descriptor.Retryable, cause)
}

type resolvedGit struct {
	ports.GitRepository
	commit string
}

func (g resolvedGit) ResolveCommit(context.Context, string, string) (string, error) {
	return g.commit, nil
}
