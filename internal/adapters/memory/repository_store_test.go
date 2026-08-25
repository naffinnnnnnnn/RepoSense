package memory

import (
	"context"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestStoreIsolatesTenantsAndReturnsDefensiveCopies(t *testing.T) {
	store := NewRepositoryStore()
	ctx := context.Background()
	result := repository.ParseResult{
		Snapshot:  repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: "tenant-a", RepositoryID: "repo"}, SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded, ChangedPaths: []repository.ChangedPath{{Path: "a.py", Kind: repository.ChangeAdded}}},
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
	result := repository.ParseResult{Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: "t", RepositoryID: "r"}, SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded}, Artifacts: []repository.CodeArtifact{{ArtifactID: "1"}, {ArtifactID: "2"}, {ArtifactID: "3"}}}
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
