package repositoryapp_test

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
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
	mu       sync.Mutex
	resolves int
}

func (f *fakeGit) ResolveCommit(_ context.Context, _ string, ref string) (string, error) {
	f.mu.Lock()
	f.resolves++
	f.mu.Unlock()
	value, ok := f.commits[ref]
	if !ok {
		return "", errors.New("ref 不存在")
	}
	return value, nil
}
func (f *fakeGit) ResolveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resolves
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

type sequenceIDs struct {
	mu    sync.Mutex
	value int
}

func (s *sequenceIDs) New(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value++
	return fmt.Sprintf("%s_%d", prefix, s.value)
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC) }

// recordingPublisher 为构造测试提供一个显式、可工作的事件发布依赖。
type recordingPublisher struct{}

func (recordingPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

// collectingPublisher 保存发布记录，用于验证幂等命中不会重复产生外部副作用。
type collectingPublisher struct {
	mu     sync.Mutex
	events []common.EventEnvelope
}

func (p *collectingPublisher) Publish(_ context.Context, event common.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *collectingPublisher) Events() []common.EventEnvelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]common.EventEnvelope(nil), p.events...)
}

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

// lookupErrorStore 在幂等查询阶段返回指定错误，其他方法均不应被调用。
type lookupErrorStore struct{ err error }

func (s lookupErrorStore) FindByIdempotencyKey(context.Context, common.Scope, string) (repository.ParseResult, bool, error) {
	return repository.ParseResult{}, false, s.err
}
func (lookupErrorStore) LatestSnapshot(context.Context, common.Scope) (repository.Snapshot, bool, error) {
	panic("幂等查询失败后不应加载增量基线")
}
func (lookupErrorStore) SaveResult(context.Context, string, repository.ParseResult) error {
	panic("幂等查询失败后不应保存结果")
}
func (lookupErrorStore) GetSnapshot(context.Context, common.Scope) (repository.Snapshot, error) {
	panic("幂等查询失败后不应读取快照")
}
func (lookupErrorStore) Artifacts(context.Context, common.Scope, string, int) ([]repository.CodeArtifact, string, error) {
	panic("幂等查询失败后不应读取制品")
}

// coordinatedMissStore 强制两个并发请求都在首轮幂等查询中得到 miss，
// 用于稳定复现“查询与保存不是一个原子操作”的竞态窗口。
type coordinatedMissStore struct {
	delegate *memory.RepositoryStore
	mu       sync.Mutex
	lookups  int
	ready    chan struct{}
}

func newCoordinatedMissStore() *coordinatedMissStore {
	return &coordinatedMissStore{delegate: memory.NewRepositoryStore(), ready: make(chan struct{})}
}

func (s *coordinatedMissStore) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error) {
	s.mu.Lock()
	s.lookups++
	lookup := s.lookups
	if lookup == 2 {
		close(s.ready)
	}
	s.mu.Unlock()
	if lookup <= 2 {
		select {
		case <-s.ready:
			return repository.ParseResult{}, false, nil
		case <-ctx.Done():
			return repository.ParseResult{}, false, ctx.Err()
		}
	}
	return s.delegate.FindByIdempotencyKey(ctx, scope, key)
}
func (s *coordinatedMissStore) LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error) {
	return s.delegate.LatestSnapshot(ctx, scope)
}
func (s *coordinatedMissStore) SaveResult(ctx context.Context, key string, result repository.ParseResult) error {
	return s.delegate.SaveResult(ctx, key, result)
}
func (s *coordinatedMissStore) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	return s.delegate.GetSnapshot(ctx, scope)
}
func (s *coordinatedMissStore) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	return s.delegate.Artifacts(ctx, scope, cursor, limit)
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

// TestServiceBindsIdempotencyKeyToCommand 验证幂等键只能重放同一业务命令，
// 不能在 Ref 或 IncludePaths 已变化时静默返回旧结果。
func TestServiceBindsIdempotencyKeyToCommand(t *testing.T) {
	sha1 := "1111111111111111111111111111111111111111"
	sha2 := "2222222222222222222222222222222222222222"
	tests := []struct {
		name       string
		firstRef   string
		secondRef  string
		firstPaths []string
		secondPath []string
		wantCommit string
		wantPath   string
	}{
		{name: "different_ref", firstRef: "v1", secondRef: "v2", wantCommit: sha2},
		{name: "different_include_paths", firstRef: "v1", secondRef: "v1", firstPaths: []string{"src/**"}, secondPath: []string{"docs/**"}, wantCommit: sha1, wantPath: "docs/readme.md"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			git := &fakeGit{
				commits: map[string]string{"v1": sha1, "v2": sha2},
				files: map[string]map[string][]byte{
					sha1: {"src/a.py": []byte("def a(): pass"), "docs/readme.md": []byte("docs")},
					sha2: {"src/b.py": []byte("def b(): pass")},
				},
			}
			service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
			firstCommand := repository.SyncCommand{Scope: scope, RepositoryPath: "repo", Ref: tt.firstRef, IncludePaths: tt.firstPaths, IdempotencyKey: "shared-key"}
			if _, err := service.Sync(context.Background(), firstCommand); err != nil {
				t.Fatal(err)
			}
			secondCommand := repository.SyncCommand{Scope: scope, RepositoryPath: "repo", Ref: tt.secondRef, IncludePaths: tt.secondPath, IdempotencyKey: "shared-key"}
			second, secondErr := service.Sync(context.Background(), secondCommand)
			// 合法策略可以是按新命令执行，也可以明确报告幂等冲突；唯一禁止的是静默返回旧结果。
			if secondErr != nil {
				var conflict *repository.DomainError
				if !errors.As(secondErr, &conflict) || conflict.Operation != "idempotency_conflict" || conflict.Retryable {
					t.Fatalf("同一幂等键绑定不同命令时应返回明确、不可重试的幂等冲突：%v", secondErr)
				}
				return
			}
			if second.Snapshot.CommitSHA != tt.wantCommit {
				t.Fatalf("幂等键复用了不同 Ref 的旧结果：got commit=%s want=%s", second.Snapshot.CommitSHA, tt.wantCommit)
			}
			if tt.wantPath != "" && (len(second.Snapshot.ChangedPaths) != 1 || second.Snapshot.ChangedPaths[0].Path != tt.wantPath) {
				t.Fatalf("幂等键复用了不同 IncludePaths 的旧结果：%#v", second.Snapshot.ChangedPaths)
			}
		})
	}
}

// TestConcurrentRequestsWithSameIdempotencyKeyExecuteOnce 验证“查询未命中到保存结果”
// 必须具备原子占位或等价的单飞机制，两个并发请求不能同时执行解析。
func TestConcurrentRequestsWithSameIdempotencyKeyExecuteOnce(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"a.py": []byte("def a(): pass")}}}
	store := newCoordinatedMissStore()
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	cmd := repository.SyncCommand{
		Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"},
		RepositoryPath: "repo",
		Ref:            "main",
		IdempotencyKey: "concurrent-key",
	}
	errorsCh := make(chan error, 2)
	for range 2 {
		go func() {
			_, syncErr := service.Sync(context.Background(), cmd)
			errorsCh <- syncErr
		}()
	}
	for range 2 {
		if syncErr := <-errorsCh; syncErr != nil {
			t.Fatalf("并发幂等请求不应失败：%v", syncErr)
		}
	}
	if got := git.ResolveCount(); got != 1 {
		t.Fatalf("相同幂等键的并发请求应只执行一次 Git 解析，ResolveCommit 调用次数=%d", got)
	}
}

// TestIdempotencyHitDoesNotRepublishCompletedEvent 验证缓存命中只返回已保存结果，
// 不会把同一个 EventID 的完成事件再次发布给外部消费者。
func TestIdempotencyHitDoesNotRepublishCompletedEvent(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"a.py": []byte("def a(): pass")}}}
	publisher := &collectingPublisher{}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), publisher, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	cmd := repository.SyncCommand{
		Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"},
		RepositoryPath: "repo",
		Ref:            "main",
		IdempotencyKey: "event-key",
	}
	first, err := service.Sync(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Sync(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.SnapshotID != second.Snapshot.SnapshotID {
		t.Fatal("幂等命中应返回同一个已保存结果")
	}
	events := publisher.Events()
	if len(events) != 1 {
		ids := make([]string, 0, len(events))
		for _, event := range events {
			ids = append(ids, event.EventID)
		}
		t.Fatalf("幂等命中不应重复发布完成事件：publish count=%d event IDs=%v", len(events), ids)
	}
}

// TestServiceClassifiesIdempotencyLookupErrors 验证调用取消/超时不会被误报为存储故障，
// 同时真实的存储错误仍保留 PERSISTENCE_FAILURE、操作阶段和原始 cause。
func TestServiceClassifiesIdempotencyLookupErrors(t *testing.T) {
	command := repository.SyncCommand{
		Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"},
		RepositoryPath: "repo",
		Ref:            "main",
		IdempotencyKey: "lookup-error",
	}
	for _, contextErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(contextErr.Error(), func(t *testing.T) {
			service, err := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), lookupErrorStore{err: contextErr}, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			_, syncErr := service.Sync(context.Background(), command)
			if !errors.Is(syncErr, contextErr) {
				t.Fatalf("应保留上下文错误，got=%v want=%v", syncErr, contextErr)
			}
			if repository.IsCode(syncErr, repository.ErrPersistence) {
				t.Fatalf("上下文错误不应被分类为 PERSISTENCE_FAILURE：%v", syncErr)
			}
		})
	}

	t.Run("storage_failure", func(t *testing.T) {
		cause := errors.New("database unavailable")
		service, err := repositoryapp.New(&fakeGit{}, parseradapter.DefaultRegistry(), lookupErrorStore{err: cause}, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		_, syncErr := service.Sync(context.Background(), command)
		if !repository.IsCode(syncErr, repository.ErrPersistence) || !errors.Is(syncErr, cause) {
			t.Fatalf("真实存储错误应保留分类及 cause：%v", syncErr)
		}
		var domainErr *repository.DomainError
		if !errors.As(syncErr, &domainErr) || domainErr.Operation != "idempotency_lookup" || !domainErr.Retryable {
			t.Fatalf("存储错误元数据不完整：%#v", domainErr)
		}
	})
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

// TestServiceRejectsMissingTraceIDBeforeDependencies 验证直接调用 Service 时也必须提供
// 可用于串联 Snapshot、事件和日志的 TraceID，并且应在访问 Git 或 Store 前拒绝无效命令。
func TestServiceRejectsMissingTraceIDBeforeDependencies(t *testing.T) {
	for _, traceID := range []string{"", "   "} {
		name := "empty"
		if traceID != "" {
			name = "whitespace"
		}
		t.Run(name, func(t *testing.T) {
			git := &fakeGit{}
			service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Sync(context.Background(), repository.SyncCommand{
				Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: traceID},
				RepositoryPath: "ignored",
				Ref:            "main",
				IdempotencyKey: "trace-required",
			})
			if !repository.IsCode(err, repository.ErrInvalidInput) {
				t.Fatalf("缺失 TraceID 应返回 INVALID_INPUT，实际为：%v", err)
			}
			if git.resolves != 0 {
				t.Fatalf("TraceID 校验失败后不应访问 Git，ResolveCommit 调用次数为：%d", git.resolves)
			}
		})
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
