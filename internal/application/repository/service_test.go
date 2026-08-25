package repositoryapp_test

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
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
	reads    int
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
	f.mu.Lock()
	f.reads++
	f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.files[commit][path], nil
}
func (f *fakeGit) ReadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
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

// observingBaselineStore 记录每次请求实际读取到的增量基线。
// 测试通过在 Git Diff 阶段暂停第一个请求，观察第二个请求能否越过仓库级串行边界。
type observingBaselineStore struct {
	delegate *memory.RepositoryStore
	observed chan string
}

func newObservingBaselineStore() *observingBaselineStore {
	return &observingBaselineStore{delegate: memory.NewRepositoryStore(), observed: make(chan string, 2)}
}

func (s *observingBaselineStore) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error) {
	return s.delegate.FindByIdempotencyKey(ctx, scope, key)
}
func (s *observingBaselineStore) LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error) {
	baseline, ok, err := s.delegate.LatestSnapshot(ctx, scope)
	if err == nil && ok {
		s.observed <- baseline.SnapshotID
	}
	return baseline, ok, nil
}
func (s *observingBaselineStore) SaveResult(ctx context.Context, key string, result repository.ParseResult) error {
	return s.delegate.SaveResult(ctx, key, result)
}
func (s *observingBaselineStore) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	return s.delegate.GetSnapshot(ctx, scope)
}
func (s *observingBaselineStore) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	return s.delegate.Artifacts(ctx, scope, cursor, limit)
}

// blockingFirstDiffGit 将第一个请求暂停在耗时的 Diff 阶段，第二个请求仍可尝试读取基线。
type blockingFirstDiffGit struct {
	mu           sync.Mutex
	diffCalls    int
	firstEntered chan struct{}
	releaseFirst chan struct{}
}

func newBlockingFirstDiffGit() *blockingFirstDiffGit {
	return &blockingFirstDiffGit{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
}
func (*blockingFirstDiffGit) ResolveCommit(_ context.Context, _ string, ref string) (string, error) {
	switch ref {
	case "commit-a":
		return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
	case "commit-b":
		return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nil
	default:
		return "", errors.New("ref 不存在")
	}
}
func (*blockingFirstDiffGit) ListFiles(context.Context, string, string) ([]string, error) {
	return []string{}, nil
}
func (g *blockingFirstDiffGit) Diff(ctx context.Context, _ string, _, _ string) ([]repository.ChangedPath, error) {
	g.mu.Lock()
	g.diffCalls++
	call := g.diffCalls
	if call == 1 {
		close(g.firstEntered)
	}
	g.mu.Unlock()
	if call == 1 {
		select {
		case <-g.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []repository.ChangedPath{}, nil
}
func (*blockingFirstDiffGit) ReadFile(context.Context, string, string, string) ([]byte, error) {
	return nil, nil
}

// identityCheckingGit 为两个不同仓库返回互不相容的 commit，并记录错误的跨仓库 Diff。
type identityCheckingGit struct {
	mu        sync.Mutex
	diffCalls int
}

func (*identityCheckingGit) ResolveCommit(_ context.Context, repositoryPath, _ string) (string, error) {
	switch repositoryPath {
	case "repo-a":
		return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
	case "repo-b":
		return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nil
	default:
		return "", errors.New("仓库不存在")
	}
}
func (*identityCheckingGit) ListFiles(context.Context, string, string) ([]string, error) {
	return []string{}, nil
}
func (g *identityCheckingGit) Diff(context.Context, string, string, string) ([]repository.ChangedPath, error) {
	g.mu.Lock()
	g.diffCalls++
	g.mu.Unlock()
	return nil, errors.New("增量基线不属于当前仓库")
}
func (*identityCheckingGit) ReadFile(context.Context, string, string, string) ([]byte, error) {
	return nil, nil
}
func (g *identityCheckingGit) DiffCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.diffCalls
}

// missingBaselineGit 模拟强制推送后旧 commit 已从远端历史和本地对象库中消失。
type missingBaselineGit struct {
	commit string
	listed int
}

func (g *missingBaselineGit) ResolveCommit(context.Context, string, string) (string, error) {
	return g.commit, nil
}
func (g *missingBaselineGit) ListFiles(context.Context, string, string) ([]string, error) {
	g.listed++
	return []string{"new.py"}, nil
}
func (*missingBaselineGit) Diff(context.Context, string, string, string) ([]repository.ChangedPath, error) {
	return nil, &repository.DomainError{Code: repository.ErrRefNotFound, Operation: "diff_base", Message: "增量基线 commit 不存在"}
}
func (*missingBaselineGit) ReadFile(context.Context, string, string, string) ([]byte, error) {
	return []byte("def current(): pass"), nil
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

// TestServiceRejectsRepositoryPathReuseForSameRepositoryID 验证 RepositoryID 与物理仓库身份一一对应。
// 如果调用方把同一个 RepositoryID 指向另一条路径，服务必须在构造跨仓库 Diff 前明确拒绝。
func TestServiceRejectsRepositoryPathReuseForSameRepositoryID(t *testing.T) {
	git := &identityCheckingGit{}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "shared-repo-id", TraceID: "trace"}
	if _, err := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo-a", Ref: "main", IdempotencyKey: "repo-a-first",
	}); err != nil {
		t.Fatal(err)
	}
	_, secondErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo-b", Ref: "main", IdempotencyKey: "repo-b-second",
	})
	var domainErr *repository.DomainError
	if !errors.As(secondErr, &domainErr) || domainErr.Code != repository.ErrInvalidInput || domainErr.Operation != "repository_identity" || domainErr.Retryable {
		t.Fatalf("RepositoryID 复用到不同路径时应返回不可重试的仓库身份冲突：%v", secondErr)
	}
	if calls := git.DiffCalls(); calls != 0 {
		t.Fatalf("仓库身份校验应发生在 Diff 前，跨仓库 Diff 调用次数=%d", calls)
	}
}

// TestConcurrentIncrementalSyncsFormLinearSnapshotChain 验证同一仓库的增量同步必须串行决定基线。
// 第一个请求停在 Diff 时启动第二个请求，可以直接检验耗时区间是否仍受仓库级并发控制。
func TestConcurrentIncrementalSyncsFormLinearSnapshotChain(t *testing.T) {
	store := newObservingBaselineStore()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	base := repository.ParseResult{Snapshot: repository.Snapshot{
		EntityMeta: repository.NewMeta("snapshot-0", scope, repository.StatusSucceeded, fixedClock{}.Now()),
		SnapshotID: "snapshot-0", CommitSHA: "0000000000000000000000000000000000000000", SyncStatus: repository.StatusSucceeded,
	}}
	if err := store.delegate.SaveResult(context.Background(), "base", base); err != nil {
		t.Fatal(err)
	}
	git := newBlockingFirstDiffGit()
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result repository.ParseResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	run := func(ref, key string) {
		result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
			Scope: scope, RepositoryPath: "repo", Ref: ref, IdempotencyKey: key,
		})
		outcomes <- outcome{result: result, err: syncErr}
	}
	go run("commit-a", "sync-a")

	// 确认第一个请求已经读取 S0 并进入无锁风险最大的 Diff 阶段。
	select {
	case baseline := <-store.observed:
		if baseline != "snapshot-0" {
			t.Fatalf("首个增量请求基线异常：%s", baseline)
		}
	case <-time.After(time.Second):
		t.Fatal("首个请求未读取增量基线")
	}
	select {
	case <-git.firstEntered:
	case <-time.After(time.Second):
		t.Fatal("首个请求未进入 Diff")
	}

	go run("commit-b", "sync-b")
	// 给第二个请求机会越过无锁区间；随后无论是否已经读到基线，都释放第一个请求，避免测试死锁。
	select {
	case <-store.observed:
	case <-time.After(100 * time.Millisecond):
	}
	close(git.releaseFirst)

	results := make([]repository.ParseResult, 0, 2)
	for range 2 {
		select {
		case got := <-outcomes:
			if got.err != nil {
				t.Fatalf("并发增量同步不应失败：%v", got.err)
			}
			results = append(results, got.result)
		case <-time.After(2 * time.Second):
			t.Fatal("并发增量同步未结束")
		}
	}
	// 线性历史必须是 S0 -> S1 -> S2；两个结果都以 S0 为父节点说明发生了分叉。
	linear := (results[0].Snapshot.ParentSnapshotID == "snapshot-0" && results[1].Snapshot.ParentSnapshotID == results[0].Snapshot.SnapshotID) ||
		(results[1].Snapshot.ParentSnapshotID == "snapshot-0" && results[0].Snapshot.ParentSnapshotID == results[1].Snapshot.SnapshotID)
	if !linear {
		t.Fatalf("并发同步产生了分叉快照：first=%s<- %s second=%s<- %s",
			results[0].Snapshot.SnapshotID, results[0].Snapshot.ParentSnapshotID,
			results[1].Snapshot.SnapshotID, results[1].Snapshot.ParentSnapshotID)
	}
}

// TestServiceFallsBackToFullSyncWhenPreviousCommitDisappears 验证强制推送或对象清理后，
// 旧基线 commit 不可达时不会永久阻断同步，而是安全降级为当前 commit 的全量解析。
func TestServiceFallsBackToFullSyncWhenPreviousCommitDisappears(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	store := memory.NewRepositoryStore()
	base := repository.ParseResult{Snapshot: repository.Snapshot{
		EntityMeta: repository.NewMeta("old-snapshot", scope, repository.StatusSucceeded, fixedClock{}.Now()),
		SnapshotID: "old-snapshot", CommitSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SyncStatus: repository.StatusSucceeded,
	}}
	if err := store.SaveResult(context.Background(), "old", base); err != nil {
		t.Fatal(err)
	}
	git := &missingBaselineGit{commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "after-force-push",
	})
	if syncErr != nil {
		t.Fatalf("旧基线消失后应降级为全量同步，而不是直接失败：%v", syncErr)
	}
	if result.Job.Scope != repository.ScopeFull || result.Snapshot.ParentSnapshotID != "" || git.listed != 1 {
		t.Fatalf("强推降级结果不完整：scope=%s parent=%q list_calls=%d", result.Job.Scope, result.Snapshot.ParentSnapshotID, git.listed)
	}
}

// TestServiceAppliesMaxFilesAfterFilteringBeforeReads 验证文件数上限针对实际解析范围，
// 并且超限判断必须发生在任何 ReadFile 调用之前，避免已知无效请求继续消耗 I/O。
func TestServiceAppliesMaxFilesAfterFilteringBeforeReads(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	t.Run("filter_then_limit", func(t *testing.T) {
		git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {
			"src/a.py": []byte("def a(): pass"), "docs/readme.md": []byte("docs"),
		}}}
		service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.Config{MaxFiles: 1, MaxFileBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
			Scope: scope, RepositoryPath: "repo", Ref: "main", IncludePaths: []string{"src/**"}, IdempotencyKey: "filtered",
		})
		if syncErr != nil || len(result.Snapshot.ChangedPaths) != 1 || result.Snapshot.ChangedPaths[0].Path != "src/a.py" || git.ReadCount() != 1 {
			t.Fatalf("应先过滤再应用文件数上限：result=%#v reads=%d err=%v", result.Snapshot.ChangedPaths, git.ReadCount(), syncErr)
		}
	})
	t.Run("reject_before_read", func(t *testing.T) {
		git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {
			"src/a.py": []byte("def a(): pass"), "src/b.py": []byte("def b(): pass"),
		}}}
		service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.Config{MaxFiles: 1, MaxFileBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		_, syncErr := service.Sync(context.Background(), repository.SyncCommand{
			Scope: scope, RepositoryPath: "repo", Ref: "main", IncludePaths: []string{"src/**"}, IdempotencyKey: "too-many",
		})
		if !repository.IsCode(syncErr, repository.ErrInvalidInput) || git.ReadCount() != 0 {
			t.Fatalf("超限应在读取文件前拒绝：reads=%d err=%v", git.ReadCount(), syncErr)
		}
	})
}

// TestServiceRejectsMalformedGitChanges 验证 ChangedPath 不是可信输入。
// 空路径、未知 Kind 和结构不完整的 Rename 都必须在排序及读取文件前被统一拒绝。
func TestServiceRejectsMalformedGitChanges(t *testing.T) {
	baseSHA := "0000000000000000000000000000000000000000"
	currentSHA := "1111111111111111111111111111111111111111"
	tests := []struct {
		name   string
		change repository.ChangedPath
	}{
		{name: "empty_path", change: repository.ChangedPath{Kind: repository.ChangeModified}},
		{name: "unknown_kind", change: repository.ChangedPath{Path: "safe.unsupported", Kind: repository.ChangeKind("UNKNOWN")}},
		{name: "rename_without_old_path", change: repository.ChangedPath{Path: "safe.unsupported", Kind: repository.ChangeRenamed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
			store := memory.NewRepositoryStore()
			base := repository.ParseResult{Snapshot: repository.Snapshot{
				EntityMeta: repository.NewMeta("base", scope, repository.StatusSucceeded, fixedClock{}.Now()),
				SnapshotID: "base", CommitSHA: baseSHA, SyncStatus: repository.StatusSucceeded,
			}}
			if err := store.SaveResult(context.Background(), "base", base); err != nil {
				t.Fatal(err)
			}
			git := &fakeGit{
				commits: map[string]string{"main": currentSHA},
				files:   map[string]map[string][]byte{currentSHA: {}},
				diffs:   map[string][]repository.ChangedPath{baseSHA + ":" + currentSHA: {tt.change}},
			}
			service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			_, syncErr := service.Sync(context.Background(), repository.SyncCommand{
				Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "malformed-" + tt.name,
			})
			var domainErr *repository.DomainError
			if !errors.As(syncErr, &domainErr) || domainErr.Code != repository.ErrGitFailure || domainErr.Operation != "validate_changes" || domainErr.Retryable {
				t.Fatalf("畸形 Git 变更应返回不可重试的 validate_changes 错误：%v", syncErr)
			}
			if git.ReadCount() != 0 {
				t.Fatalf("变更校验失败后不应读取文件，reads=%d", git.ReadCount())
			}
		})
	}
}

// TestServiceDeduplicatesIdenticalGitChanges 验证适配器偶然返回重复条目时，
// 同一 Path、OldPath 和 Kind 只进入一次解析范围，避免重复解析和重复 Artifact。
func TestServiceDeduplicatesIdenticalGitChanges(t *testing.T) {
	baseSHA := "0000000000000000000000000000000000000000"
	currentSHA := "1111111111111111111111111111111111111111"
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	store := memory.NewRepositoryStore()
	base := repository.ParseResult{Snapshot: repository.Snapshot{
		EntityMeta: repository.NewMeta("base", scope, repository.StatusSucceeded, fixedClock{}.Now()),
		SnapshotID: "base", CommitSHA: baseSHA, SyncStatus: repository.StatusSucceeded,
	}}
	if err := store.SaveResult(context.Background(), "base", base); err != nil {
		t.Fatal(err)
	}
	duplicate := repository.ChangedPath{Path: "same.unsupported", Kind: repository.ChangeModified}
	git := &fakeGit{
		commits: map[string]string{"main": currentSHA},
		files:   map[string]map[string][]byte{currentSHA: {}},
		diffs:   map[string][]repository.ChangedPath{baseSHA + ":" + currentSHA: {duplicate, duplicate}},
	}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "duplicates",
	})
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	if len(result.Snapshot.ChangedPaths) != 1 || result.Snapshot.ChangedPaths[0] != duplicate {
		t.Fatalf("完全相同的 Git 变更应被去重：%#v", result.Snapshot.ChangedPaths)
	}
}

// TestServiceOrdersEqualPathsDeterministically 验证 Path 相同时仍有 Kind、OldPath 等次级排序规则。
// 同一组变更无论适配器返回顺序如何，都必须生成完全相同的 ChangedPaths。
func TestServiceOrdersEqualPathsDeterministically(t *testing.T) {
	baseSHA := "0000000000000000000000000000000000000000"
	currentSHA := "1111111111111111111111111111111111111111"
	modified := repository.ChangedPath{Path: "same.unsupported", Kind: repository.ChangeModified}
	renamed := repository.ChangedPath{Path: "same.unsupported", OldPath: "old.unsupported", Kind: repository.ChangeRenamed}
	run := func(changes []repository.ChangedPath, key string) []repository.ChangedPath {
		t.Helper()
		scope := common.Scope{TenantID: "tenant", RepositoryID: "repo-" + key, TraceID: "trace"}
		store := memory.NewRepositoryStore()
		base := repository.ParseResult{Snapshot: repository.Snapshot{
			EntityMeta: repository.NewMeta("base", scope, repository.StatusSucceeded, fixedClock{}.Now()),
			SnapshotID: "base", CommitSHA: baseSHA, SyncStatus: repository.StatusSucceeded,
		}}
		if err := store.SaveResult(context.Background(), "base", base); err != nil {
			t.Fatal(err)
		}
		git := &fakeGit{commits: map[string]string{"main": currentSHA}, files: map[string]map[string][]byte{currentSHA: {}}, diffs: map[string][]repository.ChangedPath{baseSHA + ":" + currentSHA: changes}}
		service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
			Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: key,
		})
		if syncErr != nil {
			t.Fatal(syncErr)
		}
		return result.Snapshot.ChangedPaths
	}
	forward := run([]repository.ChangedPath{modified, renamed}, "forward")
	reverse := run([]repository.ChangedPath{renamed, modified}, "reverse")
	if !reflect.DeepEqual(forward, reverse) {
		t.Fatalf("相同 Path 的输出顺序依赖适配器输入：forward=%#v reverse=%#v", forward, reverse)
	}
}

// TestServiceValidatesDeletedAndRenamedOldPaths 验证所有进入结果的路径都经过仓库相对路径检查。
// Deleted 不调用 ReadFile，Rename 的 OldPath 也不会传给 ReadFile，因此必须在变更校验阶段单独覆盖。
func TestServiceValidatesDeletedAndRenamedOldPaths(t *testing.T) {
	baseSHA := "0000000000000000000000000000000000000000"
	currentSHA := "1111111111111111111111111111111111111111"
	tests := []struct {
		name   string
		change repository.ChangedPath
		files  map[string][]byte
	}{
		{name: "unsafe_deleted_path", change: repository.ChangedPath{Path: "../secret.go", Kind: repository.ChangeDeleted}, files: map[string][]byte{}},
		{name: "unsafe_rename_old_path", change: repository.ChangedPath{Path: "safe.go", OldPath: "../secret.go", Kind: repository.ChangeRenamed}, files: map[string][]byte{"safe.go": []byte("package safe")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
			store := memory.NewRepositoryStore()
			base := repository.ParseResult{Snapshot: repository.Snapshot{
				EntityMeta: repository.NewMeta("base", scope, repository.StatusSucceeded, fixedClock{}.Now()),
				SnapshotID: "base", CommitSHA: baseSHA, SyncStatus: repository.StatusSucceeded,
			}}
			if err := store.SaveResult(context.Background(), "base", base); err != nil {
				t.Fatal(err)
			}
			git := &fakeGit{
				commits: map[string]string{"main": currentSHA},
				files:   map[string]map[string][]byte{currentSHA: tt.files},
				diffs:   map[string][]repository.ChangedPath{baseSHA + ":" + currentSHA: {tt.change}},
			}
			service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			key := "unsafe-" + tt.name
			_, syncErr := service.Sync(context.Background(), repository.SyncCommand{
				Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: key,
			})
			if !repository.IsCode(syncErr, repository.ErrGitFailure) {
				t.Fatalf("不安全的删除语义路径必须被拒绝：%v", syncErr)
			}
			if git.ReadCount() != 0 {
				t.Fatalf("路径校验必须发生在 ReadFile 前，reads=%d", git.ReadCount())
			}
			if _, saved, lookupErr := store.FindByIdempotencyKey(context.Background(), scope, key); lookupErr != nil || saved {
				t.Fatalf("不安全变更不应被持久化：saved=%v err=%v", saved, lookupErr)
			}
		})
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
