package repositoryapp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestSubmitPersistsResolveCommitFailureAsQueryableTerminalTask(t *testing.T) {
	store := memory.NewRepositoryStore()
	git := &fakeGit{resolveErr: errors.New("temporary remote failure")}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	command := repository.SyncCommand{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo-submit-failure", TraceID: "trace"}, RepositoryPath: ".", Provider: "local", Ref: "main", IdempotencyKey: "key"}
	job, submitErr := service.Submit(context.Background(), command)
	if submitErr == nil || job.JobID == "" {
		t.Fatalf("应返回可查询失败任务和原始错误：job=%#v err=%v", job, submitErr)
	}
	stored, err := service.GetJob(context.Background(), command.Scope, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != repository.StatusFailed || stored.ErrorCode != string(repository.ErrGitFailure) {
		t.Fatalf("失败终态错误：%#v", stored)
	}
	result, found, err := store.FindByIdempotencyKey(context.Background(), command.Scope, command.IdempotencyKey)
	if err != nil || !found || result.Event.EventType != "parse.failed.v1" {
		t.Fatalf("失败结果或 outbox 不完整：%#v found=%v err=%v", result, found, err)
	}
}

func TestRetryReResolvesCommitInsteadOfReusingEmptyFailedSnapshot(t *testing.T) {
	store := memory.NewRepositoryStore()
	git := &fakeGit{resolveErr: errors.New("temporary remote failure"), commits: map[string]string{}, files: map[string]map[string][]byte{}, diffs: map[string][]repository.ChangedPath{}}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo-retry-resolve", TraceID: "trace"}
	command := repository.SyncCommand{Scope: scope, RepositoryPath: ".", Provider: "local", Ref: "main", IdempotencyKey: "retry-key"}
	failed, submitErr := service.Submit(context.Background(), command)
	if submitErr == nil || failed.JobID == "" {
		t.Fatalf("未形成首次失败任务：%#v %v", failed, submitErr)
	}
	sha := "2222222222222222222222222222222222222222"
	git.resolveErr = nil
	git.commits["main"] = sha
	git.files[sha] = map[string][]byte{"a.go": []byte("package a")}
	retried, err := service.Retry(context.Background(), scope, failed.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.JobID == failed.JobID || retried.Status != repository.StatusPending {
		t.Fatalf("未创建新 attempt：%#v", retried)
	}
	task, err := store.TaskByJobID(context.Background(), scope, retried.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Snapshot.CommitSHA != sha {
		t.Fatalf("重试未重新解析不可变 commit：%#v", task.Snapshot)
	}
}

type controlledClock struct{ now time.Time }

func (c *controlledClock) Now() time.Time { return c.now }

type failingEventPublisher struct{ calls int }

func (p *failingEventPublisher) Publish(context.Context, common.EventEnvelope) error {
	p.calls++
	return errors.New("broker unavailable: secret-content-must-not-be-stored")
}

func TestOutboxRetriesWithSanitizedErrorThenDeadLetters(t *testing.T) {
	store := memory.NewRepositoryStore()
	now := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	result := succeededResultForReliability(scope, now)
	if err := store.SaveResult(context.Background(), "key", result); err != nil {
		t.Fatal(err)
	}
	clock := &controlledClock{now: now}
	publisher := &failingEventPublisher{}
	dispatcher, err := repositoryapp.NewOutboxDispatcher(store, publisher, clock, repositoryapp.OutboxConfig{BatchSize: 10, MaxAttempts: 2, BaseBackoff: time.Second, MaxBackoff: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("首次 broker 失败应返回错误")
	}
	clock.now = now.Add(time.Second)
	pending, err := store.PendingOutbox(context.Background(), 10, clock.now)
	if err != nil || len(pending) != 1 || pending[0].LastError != "事件发布失败" {
		t.Fatalf("Outbox 错误必须脱敏：%#v err=%v", pending, err)
	}
	if _, err := dispatcher.DispatchOnce(context.Background()); err == nil {
		t.Fatal("达到最大次数仍应报告发布错误")
	}
	clock.now = now.Add(2 * time.Second)
	records, err := store.PendingOutbox(context.Background(), 10, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 || publisher.calls != 2 {
		t.Fatalf("死信后不应继续投递：records=%d calls=%d", len(records), publisher.calls)
	}
}

type triggerRecorder struct{ calls int }

func (t *triggerRecorder) TriggerParseCompleted(_ context.Context, _ common.Scope, snapshot repository.Snapshot) error {
	if snapshot.SyncStatus != repository.StatusSucceeded {
		return errors.New("not succeeded")
	}
	t.calls++
	return nil
}

func TestDownstreamOnlyConsumesCompletedSucceededSnapshot(t *testing.T) {
	store := memory.NewRepositoryStore()
	now := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	result := succeededResultForReliability(scope, now)
	if err := store.SaveResult(context.Background(), "key", result); err != nil {
		t.Fatal(err)
	}
	trigger := &triggerRecorder{}
	handler, err := repositoryapp.NewParseEventHandler(store, trigger)
	if err != nil {
		t.Fatal(err)
	}
	failed := repository.NewParseFailedEvent("failed", scope, now, repository.ParseFailedPayload{SnapshotID: "snap", ErrorCode: repository.ErrGitFailure, Retryable: true})
	if err := handler.Handle(context.Background(), scope, failed); err != nil || trigger.calls != 0 {
		t.Fatalf("failed 事件不应触发下游：%v", err)
	}
	if err := handler.Handle(context.Background(), scope, result.Event); err != nil || trigger.calls != 1 {
		t.Fatalf("completed 事件应触发一次：calls=%d err=%v", trigger.calls, err)
	}
	badScope := scope
	badScope.TenantID = "other"
	if err := handler.Handle(context.Background(), badScope, result.Event); err == nil {
		t.Fatal("跨租户事件作用域必须拒绝")
	}
}

func succeededResultForReliability(scope common.Scope, now time.Time) repository.ParseResult {
	if scope.SnapshotID == "" {
		scope.SnapshotID = "snap"
	}
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta(scope.SnapshotID, scope, repository.StatusSucceeded, now), SnapshotID: scope.SnapshotID, Provider: "local", Ref: "main", CommitSHA: "1111111111111111111111111111111111111111", SyncStatus: repository.StatusSucceeded, ChangedPaths: []repository.ChangedPath{}}
	job := repository.ParseJob{EntityMeta: repository.NewMeta("job", scope, repository.StatusSucceeded, now), JobID: "job", SnapshotID: scope.SnapshotID, ParserVersion: "test", Scope: repository.ScopeFull, Status: repository.StatusSucceeded, Progress: 100}
	event := repository.NewParseCompletedEvent("evt", scope, now, repository.ParseCompletedPayload{SnapshotID: scope.SnapshotID, CommitSHA: snapshot.CommitSHA, DeletedPaths: []string{}})
	return repository.ParseResult{Snapshot: snapshot, Job: job, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}, DeletedPaths: []string{}, SkippedFiles: []repository.SkippedFile{}, Event: event}
}
