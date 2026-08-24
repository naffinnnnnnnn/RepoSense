package repositoryapp_test

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type fakeGit struct {
	commits  map[string]string
	files    map[string]map[string][]byte
	diffs    map[string][]repository.ChangedPath
	readErr  error
	resolves int
}

func (f *fakeGit) ResolveCommit(_ context.Context, _ string, ref string) (string, error) {
	f.resolves++
	value, ok := f.commits[ref]
	if !ok {
		return "", errors.New("ref 不存在")
	}
	return value, nil
}
func (f *fakeGit) ListFiles(_ context.Context, _ string, commit string) ([]string, error) {
	var out []string
	for name := range f.files[commit] {
		out = append(out, name)
	}
	return out, nil
}
func (f *fakeGit) Diff(_ context.Context, _ string, from, to string) ([]repository.ChangedPath, error) {
	return f.diffs[from+":"+to], nil
}
func (f *fakeGit) ReadFile(_ context.Context, _ string, commit, path string) ([]byte, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.files[commit][path], nil
}

type sequenceIDs struct{ value int }

func (s *sequenceIDs) New(prefix string) string {
	s.value++
	return fmt.Sprintf("%s_%d", prefix, s.value)
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC) }

// recordingPublisher 为构造测试提供一个显式、可工作的事件发布依赖。
type recordingPublisher struct{}

func (recordingPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

// recordingObserver 为构造测试提供一个显式、可工作的可观测依赖。
type recordingObserver struct{}

func (recordingObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (recordingObserver) Count(string, int64, map[string]string) {}

// failingEntropyReader 模拟操作系统安全随机源不可用。
type failingEntropyReader struct{}

func (failingEntropyReader) Read([]byte) (int, error) {
	return 0, errors.New("安全随机源不可用")
}

func TestServiceFullIncrementalAndIdempotent(t *testing.T) {
	sha1, sha2 := "1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222"
	git := &fakeGit{commits: map[string]string{"v1": sha1, "v2": sha2}, files: map[string]map[string][]byte{
		sha1: {"src/a.ts": []byte("export function a() {\n return b();\n}\n"), "image.bin": []byte{0, 1}},
		sha2: {"src/a.ts": []byte("export function a() {\n return c();\n}\n"), "src/b.py": []byte("def b():\n    return 1\n")}},
		diffs: map[string][]repository.ChangedPath{sha1 + ":" + sha2: {{Path: "src/a.ts", Kind: repository.ChangeModified}, {Path: "src/b.py", Kind: repository.ChangeAdded}, {Path: "old.py", Kind: repository.ChangeDeleted}}}}
	store := memory.NewRepositoryStore()
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, nil, nil, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	first, err := service.Sync(context.Background(), repository.SyncCommand{Scope: scope, RepositoryPath: "ignored", Ref: "v1", IdempotencyKey: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Job.Scope != repository.ScopeFull || first.Event.EventType != "parse.completed.v1" || len(first.Artifacts) == 0 {
		t.Fatalf("全量解析结果不符合预期：%#v", first)
	}
	secondCmd := repository.SyncCommand{Scope: scope, RepositoryPath: "ignored", Ref: "v2", IdempotencyKey: "two"}
	second, err := service.Sync(context.Background(), secondCmd)
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.Scope != repository.ScopeIncremental || second.Snapshot.ParentSnapshotID != first.Snapshot.SnapshotID {
		t.Fatalf("未保留增量基线：%#v", second.Snapshot)
	}
	if len(second.DeletedPaths) != 1 || second.DeletedPaths[0] != "old.py" {
		t.Fatalf("删除路径不符合预期：%#v", second.DeletedPaths)
	}
	resolves := git.resolves
	cached, err := service.Sync(context.Background(), secondCmd)
	if err != nil {
		t.Fatal(err)
	}
	if cached.Snapshot.SnapshotID != second.Snapshot.SnapshotID || git.resolves != resolves {
		t.Fatal("幂等请求不应重复执行解析工作")
	}
}

func TestServicePersistsSanitizedFailure(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"a.py": []byte("def a(): pass")}}, readErr: errors.New("secret token abc")}
	store := memory.NewRepositoryStore()
	ids := &sequenceIDs{}
	service, _ := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, nil, nil, ids, fixedClock{}, repositoryapp.DefaultConfig())
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo"}
	cmd := repository.SyncCommand{Scope: scope, RepositoryPath: "ignored", Ref: "main", IdempotencyKey: "failure"}
	result, err := service.Sync(context.Background(), cmd)
	if err == nil || result.Job.Status != repository.StatusFailed || result.Job.ErrorMessage != "仓库解析失败" {
		t.Fatalf("失败信息未脱敏或未持久化：result=%#v err=%v", result, err)
	}
	cached, ok, lookupErr := store.FindByIdempotencyKey(context.Background(), scope, "failure")
	if lookupErr != nil || !ok || cached.Event.EventType != "parse.failed.v1" {
		t.Fatalf("未找到失败结果：%#v %v", cached, lookupErr)
	}
}

func TestServiceRejectsUnsafeIncludePattern(t *testing.T) {
	service, _ := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), nil, nil, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	_, err := service.Sync(context.Background(), repository.SyncCommand{Scope: common.Scope{TenantID: "t", RepositoryID: "r"}, RepositoryPath: "x", Ref: "main", IdempotencyKey: "key", IncludePaths: []string{"../secret/**"}})
	if !repository.IsCode(err, repository.ErrInvalidInput) {
		t.Fatalf("预期错误码为 INVALID_INPUT，实际为：%v", err)
	}
}

// TestServiceDoesNotReuseMutableRefResultAfterRefMoves 验证 CLI 风格幂等键不会把移动后的 ref 映射到旧 commit。
func TestServiceDoesNotReuseMutableRefResultAfterRefMoves(t *testing.T) {
	sha1 := "1111111111111111111111111111111111111111"
	sha2 := "2222222222222222222222222222222222222222"
	git := &fakeGit{
		commits: map[string]string{"main": sha1},
		files: map[string]map[string][]byte{
			sha1: {"a.py": []byte("def first():\n    return 1\n")},
			sha2: {"a.py": []byte("def second():\n    return 2\n")},
		},
		diffs: map[string][]repository.ChangedPath{
			sha1 + ":" + sha2: {{Path: "a.py", Kind: repository.ChangeModified}},
		},
	}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), nil, nil, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	cmd := repository.SyncCommand{
		Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo"},
		RepositoryPath: "ignored",
		Ref:            "main",
		IdempotencyKey: "repo:main",
	}
	first, err := service.Sync(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.CommitSHA != sha1 {
		t.Fatalf("首次解析 commit 不符合预期：%s", first.Snapshot.CommitSHA)
	}

	// 模拟 main 从 sha1 前进到 sha2；命令文本和 CLI 自动生成的幂等键保持不变。
	git.commits["main"] = sha2
	second, err := service.Sync(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	// 工程预期不能静默返回 sha1 的缓存结果，至少应解析到 sha2 或报告幂等冲突。
	if second.Snapshot.CommitSHA != sha2 {
		t.Fatalf("ref 已移动到 %s，但幂等结果仍指向旧 commit %s", sha2, second.Snapshot.CommitSHA)
	}
}

// TestNewRejectsMissingOperationalDependencies 验证事件发布和可观测依赖缺失时构造函数会明确失败。
func TestNewRejectsMissingOperationalDependencies(t *testing.T) {
	t.Run("missing_event_publisher", func(t *testing.T) {
		_, err := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), nil, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err == nil {
			t.Fatal("缺少 EventPublisher 时不应静默使用 noopPublisher")
		}
	})
	t.Run("missing_observer", func(t *testing.T) {
		_, err := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, nil, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err == nil {
			t.Fatal("缺少 Observer 时不应静默使用 noopObserver")
		}
	})
}

// TestRandomIDsDoesNotPanicWhenEntropyIsUnavailable 验证安全随机源异常不会直接终止 worker 进程。
func TestRandomIDsDoesNotPanicWhenEntropyIsUnavailable(t *testing.T) {
	const childMarker = "REPOSENSE_FAIL_ENTROPY_CHILD"
	if os.Getenv(childMarker) == "1" {
		// crypto/rand 的 fatal error 无法由 recover 捕获，必须放在子进程中注入。
		cryptorand.Reader = failingEntropyReader{}
		_ = (repositoryapp.RandomIDs{}).New("snap")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestRandomIDsDoesNotPanicWhenEntropyIsUnavailable$")
	cmd.Env = append(os.Environ(), childMarker+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("安全随机源不可用时不应终止 worker 进程：%v：%s", err, output)
	}
}

// TestNewRejectsNonPositiveResourceLimits 验证零值和负数资源上限不会被静默替换成默认配置。
func TestNewRejectsNonPositiveResourceLimits(t *testing.T) {
	tests := []struct {
		name   string
		config repositoryapp.Config
	}{
		{name: "zero_max_files", config: repositoryapp.Config{MaxFiles: 0, MaxFileBytes: 1024}},
		{name: "negative_max_files", config: repositoryapp.Config{MaxFiles: -1, MaxFileBytes: 1024}},
		{name: "zero_max_file_bytes", config: repositoryapp.Config{MaxFiles: 1, MaxFileBytes: 0}},
		{name: "negative_max_file_bytes", config: repositoryapp.Config{MaxFiles: 1, MaxFileBytes: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, tt.config)
			if err == nil {
				t.Fatalf("非正数资源上限应返回配置错误，实际配置为：%#v", tt.config)
			}
		})
	}
}
