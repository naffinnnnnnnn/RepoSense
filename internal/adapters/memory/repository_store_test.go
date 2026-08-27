package memory

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestTaskLeaseExpiryAllowsCrashRecoveryAndPendingCancellation(t *testing.T) {
	store := NewRepositoryStore()
	now := time.Now().UTC()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo-lease", TraceID: "trace"}
	if err := store.BindRepository(context.Background(), repository.RepositoryBinding{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Provider: "local", CanonicalIdentity: "local:/repo", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	task := repository.ParseTask{Command: repository.SyncCommand{Scope: scope, RepositoryPath: ".", Ref: "main", IdempotencyKey: "key"}, CommandFingerprint: "fingerprint", RepositoryIdentity: "local:/repo", Attempt: 1,
		Job:      repository.ParseJob{EntityMeta: repository.NewMeta("job", scope, repository.StatusPending, now), JobID: "job", SnapshotID: "snap", ParserVersion: "test", Scope: repository.ScopeFull, Status: repository.StatusPending},
		Snapshot: repository.Snapshot{EntityMeta: repository.NewMeta("snap", scope, repository.StatusPending, now), SnapshotID: "snap", Provider: "local", Ref: "main", SyncStatus: repository.StatusPending, ChangedPaths: []repository.ChangedPath{}}}
	if _, created, err := store.AcquireIdempotency(context.Background(), task); err != nil || !created {
		t.Fatalf("acquire created=%v err=%v", created, err)
	}
	claimed, found, err := store.ClaimPendingJob(context.Background(), "worker-1", time.Millisecond)
	if err != nil || !found || claimed.LeaseOwner != "worker-1" {
		t.Fatalf("首次领取失败：%#v %v", claimed, err)
	}
	time.Sleep(5 * time.Millisecond)
	recovered, found, err := store.ClaimPendingJob(context.Background(), "worker-2", time.Second)
	if err != nil || !found || recovered.Job.JobID != "job" || recovered.LeaseOwner != "worker-2" {
		t.Fatalf("租约接管失败：%#v %v", recovered, err)
	}

	second := task
	second.Command.Scope.RepositoryID = "repo-cancel"
	second.Command.IdempotencyKey = "cancel-key"
	second.Job = repository.ParseJob{EntityMeta: repository.NewMeta("job-cancel", second.Command.Scope, repository.StatusPending, now), JobID: "job-cancel", SnapshotID: "snap-cancel", ParserVersion: "test", Scope: repository.ScopeFull, Status: repository.StatusPending}
	second.Snapshot = repository.Snapshot{EntityMeta: repository.NewMeta("snap-cancel", second.Command.Scope, repository.StatusPending, now), SnapshotID: "snap-cancel", Provider: "local", Ref: "main", SyncStatus: repository.StatusPending, ChangedPaths: []repository.ChangedPath{}}
	if err := store.BindRepository(context.Background(), repository.RepositoryBinding{TenantID: "tenant", RepositoryID: "repo-cancel", Provider: "local", CanonicalIdentity: "local:/cancel", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AcquireIdempotency(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := store.RequestCancel(context.Background(), second.Command.Scope, "job-cancel"); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.TaskByJobID(context.Background(), second.Command.Scope, "job-cancel")
	if err != nil || cancelled.Job.Status != repository.StatusCancelled {
		t.Fatalf("pending 取消失败：%#v %v", cancelled, err)
	}
}

func TestStoreIsolatesTenantsAndReturnsDefensiveCopies(t *testing.T) {
	store := NewRepositoryStore()
	ctx := context.Background()
	result := repository.ParseResult{
		Snapshot:  repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: "tenant-a", RepositoryID: "repo", Status: string(repository.StatusSucceeded)}, SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded, ChangedPaths: []repository.ChangedPath{{Path: "a.py", Kind: repository.ChangeAdded}}},
		Job:       repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: "tenant-a", RepositoryID: "repo", Status: string(repository.StatusSucceeded)}, JobID: "job", SnapshotID: "snap", Status: repository.StatusSucceeded, Progress: 100},
		Artifacts: []repository.CodeArtifact{{ArtifactID: "a", Attributes: map[string]string{"async": "true"}}}, Event: common.EventEnvelope{Payload: map[string]any{"deleted_paths": []string{"old.py"}}},
	}
	if err := store.SaveResult(ctx, "key", result); err != nil {
		t.Fatal(err)
	}
	result.Snapshot.ChangedPaths[0].Path = "mutated"
	result.Artifacts[0].Attributes["async"] = "false"
	got, ok, err := store.FindByIdempotencyKey(ctx, common.Scope{TenantID: "tenant-a", RepositoryID: "repo"}, "key")
	if err != nil || !ok {
		t.Fatalf("查询结果异常：ok=%v err=%v", ok, err)
	}
	if got.Snapshot.ChangedPaths[0].Path != "a.py" || got.Artifacts[0].Attributes["async"] != "true" {
		t.Fatalf("已存储结果被外部修改：%#v", got)
	}
	if _, ok, _ := store.FindByIdempotencyKey(ctx, common.Scope{TenantID: "tenant-b", RepositoryID: "repo"}, "key"); ok {
		t.Fatal("幂等结果发生跨租户泄漏")
	}
	if _, err := store.GetSnapshot(ctx, common.Scope{TenantID: "tenant-b", RepositoryID: "repo", SnapshotID: "snap"}); err == nil {
		t.Fatal("跨租户快照读取不应成功")
	}
}

func TestArtifactsUsesBoundedCursorPagination(t *testing.T) {
	store := NewRepositoryStore()
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "snap"}
	result := repository.ParseResult{Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: "t", RepositoryID: "r", Status: string(repository.StatusSucceeded)}, SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded}, Job: repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: "t", RepositoryID: "r", Status: string(repository.StatusSucceeded)}, JobID: "job", SnapshotID: "snap", Status: repository.StatusSucceeded, Progress: 100}, Artifacts: []repository.CodeArtifact{{ArtifactID: "1"}, {ArtifactID: "2"}, {ArtifactID: "3"}}}
	if err := store.SaveResult(context.Background(), "key", result); err != nil {
		t.Fatal(err)
	}
	first, cursor, err := store.Artifacts(context.Background(), scope, "", 2)
	if err != nil || len(first) != 2 || cursor != "2" {
		t.Fatalf("第一页结果异常：%#v %q %v", first, cursor, err)
	}
	second, cursor, err := store.Artifacts(context.Background(), scope, cursor, 2)
	if err != nil || len(second) != 1 || cursor != "" {
		t.Fatalf("第二页结果异常：%#v %q %v", second, cursor, err)
	}
}

// TestStoreRejectsInconsistentParseResultStatus 验证持久化边界不会接受互相矛盾的终态。
// Job.Status、Snapshot.SyncStatus 和两个 EntityMeta.Status 必须表达同一个解析结果。
func TestStoreRejectsInconsistentParseResultStatus(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*repository.ParseResult)
	}{
		{name: "snapshot_meta_mismatch", mutate: func(result *repository.ParseResult) {
			result.Snapshot.EntityMeta.Status = string(repository.StatusFailed)
		}},
		{name: "job_meta_mismatch", mutate: func(result *repository.ParseResult) {
			result.Job.EntityMeta.Status = string(repository.StatusFailed)
		}},
		{name: "job_snapshot_mismatch", mutate: func(result *repository.ParseResult) {
			result.Job.Status = repository.StatusFailed
			result.Job.EntityMeta.Status = string(repository.StatusFailed)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := repository.ParseResult{
				Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: "tenant", RepositoryID: "repo", Status: string(repository.StatusSucceeded)}, SnapshotID: "snapshot", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded},
				Job:      repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: "tenant", RepositoryID: "repo", Status: string(repository.StatusSucceeded)}, JobID: "job", SnapshotID: "snapshot", Status: repository.StatusSucceeded, Progress: 100},
			}
			tt.mutate(&result)
			if err := NewRepositoryStore().SaveResult(context.Background(), "key", result); err == nil {
				t.Fatalf("不一致的 ParseResult 终态不应持久化：%#v", result)
			}
		})
	}
}

// TestArtifactsRejectsFailedSnapshot 对应 10.5：失败任务即使已经聚合了部分 Artifact，
// 下游也不能把这些不完整数据当作可消费的成功快照返回。
func TestArtifactsRejectsFailedSnapshot(t *testing.T) {
	store := NewRepositoryStore()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "failed-snapshot"}
	result := repository.ParseResult{
		Snapshot:  repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusFailed)}, SnapshotID: scope.SnapshotID, CommitSHA: "sha", SyncStatus: repository.StatusFailed, ErrorCode: string(repository.ErrParseFailure), ErrorMessage: "代码文件解析失败"},
		Job:       repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusFailed)}, JobID: "failed-job", SnapshotID: scope.SnapshotID, Status: repository.StatusFailed, ErrorCode: string(repository.ErrParseFailure), ErrorMessage: "代码文件解析失败"},
		Artifacts: []repository.CodeArtifact{{ArtifactID: "partial-artifact", Kind: repository.ArtifactFunction, Name: "partial"}},
	}
	if err := store.SaveResult(context.Background(), "failed-key", result); err != nil {
		t.Fatalf("准备失败快照异常：%v", err)
	}

	artifacts, _, err := store.Artifacts(context.Background(), scope, "", 100)
	if err == nil {
		t.Fatalf("失败快照的部分 Artifact 不应暴露给下游：%#v", artifacts)
	}
}
