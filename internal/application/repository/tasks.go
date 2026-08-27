package repositoryapp

import (
	"context"
	"errors"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

func (s *Service) GetJob(ctx context.Context, scope common.Scope, jobID string) (repository.ParseJob, error) {
	store, ok := s.store.(ports.RepositoryTaskStore)
	if !ok {
		return repository.ParseJob{}, errors.New("Store 不支持任务查询")
	}
	task, err := store.TaskByJobID(ctx, scope, jobID)
	if err != nil {
		return repository.ParseJob{}, domainError(repository.ErrRepositoryNotFound, "job_lookup", "解析任务不存在", false, err)
	}
	return task.Job, nil
}
func (s *Service) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	snapshot, err := s.store.GetSnapshot(ctx, scope)
	if err != nil {
		return snapshot, domainError(repository.ErrRepositoryNotFound, "snapshot_lookup", "解析快照不存在", false, err)
	}
	return snapshot, nil
}
func (s *Service) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	items, next, err := s.store.Artifacts(ctx, scope, cursor, limit)
	if err != nil {
		return nil, "", domainError(repository.ErrInvalidInput, "snapshot_not_succeeded", "只有成功快照可以查询 Artifact", false, err)
	}
	return items, next, nil
}
func (s *Service) Cancel(ctx context.Context, scope common.Scope, jobID string) error {
	store, ok := s.store.(ports.RepositoryTaskStore)
	if !ok {
		return errors.New("Store 不支持任务取消")
	}
	if err := store.RequestCancel(ctx, scope, jobID); err != nil {
		return domainError(repository.ErrInvalidInput, "job_state_conflict", "任务不存在或当前状态不可取消", false, err)
	}
	return nil
}
func (s *Service) Retry(ctx context.Context, scope common.Scope, jobID string) (repository.ParseJob, error) {
	store, ok := s.store.(ports.RepositoryTaskStore)
	if !ok {
		return repository.ParseJob{}, errors.New("Store 不支持任务重试")
	}
	task, err := store.TaskByJobID(ctx, scope, jobID)
	if err != nil || task.Job.Status != repository.StatusFailed || !task.Retryable {
		return repository.ParseJob{}, domainError(repository.ErrInvalidInput, "job_state_conflict", "任务不可重试或已被其他请求重试", false, err)
	}
	return s.Submit(ctx, task.Command)
}

type WorkerConfig struct {
	Owner                       string
	Lease, Heartbeat, IdleDelay time.Duration
	FailureDelay                time.Duration
	ParseTimeout                time.Duration
}
type Worker struct {
	service *Service
	store   ports.RepositoryTaskStore
	config  WorkerConfig
}

func NewWorker(service *Service, store ports.RepositoryTaskStore, config WorkerConfig) (*Worker, error) {
	if service == nil || store == nil {
		return nil, errors.New("Worker Service 和 Store 不能为空")
	}
	if config.Owner == "" || config.Lease <= 0 || config.Heartbeat <= 0 || config.Heartbeat >= config.Lease || config.IdleDelay <= 0 || config.FailureDelay <= 0 || config.ParseTimeout <= 0 {
		return nil, errors.New("Worker 配置无效")
	}
	return &Worker{service: service, store: store, config: config}, nil
}
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	task, found, err := w.store.ClaimPendingJob(ctx, w.config.Owner, w.config.Lease)
	if err != nil || !found {
		return found, err
	}
	scope := common.Scope{TenantID: task.Job.TenantID, RepositoryID: task.Job.RepositoryID, TraceID: task.Job.TraceID}
	executeCtx, cancel := context.WithTimeout(ctx, w.config.ParseTimeout)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(w.config.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-executeCtx.Done():
				return
			case <-ticker.C:
				current, loadErr := w.store.TaskByJobID(executeCtx, scope, task.Job.JobID)
				if loadErr != nil || current.CancelRequested {
					cancel()
					return
				}
				if heartbeatErr := w.store.HeartbeatJob(executeCtx, task.Job.JobID, w.config.Owner, w.config.Lease, current.Job.Progress); heartbeatErr != nil {
					cancel()
					return
				}
			}
		}
	}()
	_, err = w.service.execute(executeCtx, scope, task.Job.JobID, w.config.Owner)
	cancel()
	<-heartbeatDone
	return true, err
}
func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			found, err := w.RunOnce(ctx)
			delay := time.Duration(0)
			if err != nil && !errors.Is(err, context.Canceled) {
				delay = w.config.FailureDelay
			} else if !found {
				delay = w.config.IdleDelay
			}
			timer.Reset(delay)
		}
	}
}
