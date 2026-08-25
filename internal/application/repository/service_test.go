package repositoryapp_test

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
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

// flakyPublisher 模拟事件 Broker 首次不可用、后续恢复，并区分发布尝试和成功投递。
type flakyPublisher struct {
	mu        sync.Mutex
	failures  int
	attempts  []common.EventEnvelope
	delivered []common.EventEnvelope
}

func (p *flakyPublisher) Publish(_ context.Context, event common.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts = append(p.attempts, event)
	if p.failures > 0 {
		p.failures--
		return errors.New("事件 Broker 暂时不可用")
	}
	p.delivered = append(p.delivered, event)
	return nil
}

func (p *flakyPublisher) Counts() (attempts, delivered int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.attempts), len(p.delivered)
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

// saveErrorStore 只在最终 SaveResult 阶段失败，用来确认已经完成的解析结果不会被清空。
type saveErrorStore struct {
	delegate *memory.RepositoryStore
	err      error
}

func (s saveErrorStore) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error) {
	return s.delegate.FindByIdempotencyKey(ctx, scope, key)
}
func (s saveErrorStore) LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error) {
	return s.delegate.LatestSnapshot(ctx, scope)
}
func (s saveErrorStore) SaveResult(context.Context, string, repository.ParseResult) error {
	return s.err
}
func (s saveErrorStore) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	return s.delegate.GetSnapshot(ctx, scope)
}
func (s saveErrorStore) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	return s.delegate.Artifacts(ctx, scope, cursor, limit)
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

// fixedParserRegistry 将指定解析器用于所有路径，便于精确观察大小限制前后是否调用 Parse。
type fixedParserRegistry struct{ parser ports.LanguageParser }

func (r fixedParserRegistry) ForPath(string) (ports.LanguageParser, bool) { return r.parser, true }

type countingLanguageParser struct {
	mu       sync.Mutex
	calls    int
	parseErr error
}

func (*countingLanguageParser) Language() string     { return "counting" }
func (*countingLanguageParser) Extensions() []string { return []string{".test"} }
func (*countingLanguageParser) Version() string      { return "counting@1" }
func (p *countingLanguageParser) Parse(context.Context, string, repository.FileContent) (repository.ParsedFile, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return repository.ParsedFile{}, p.parseErr
}
func (p *countingLanguageParser) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// cancelingReadGit 在 ReadFile 内取消请求并返回真实读取错误，模拟超时与底层失败同时发生。
type cancelingReadGit struct {
	commit string
	cancel context.CancelFunc
	cause  error
}

func (g *cancelingReadGit) ResolveCommit(context.Context, string, string) (string, error) {
	return g.commit, nil
}
func (*cancelingReadGit) ListFiles(context.Context, string, string) ([]string, error) {
	return []string{"a.py"}, nil
}
func (*cancelingReadGit) Diff(context.Context, string, string, string) ([]repository.ChangedPath, error) {
	return nil, nil
}
func (g *cancelingReadGit) ReadFile(context.Context, string, string, string) ([]byte, error) {
	g.cancel()
	return nil, g.cause
}

// steppingClock 每次调用向前推进固定步长，用于验证 CreatedAt、UpdatedAt 和 OccurredAt 的顺序。
type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	value := c.now
	c.now = c.now.Add(c.step)
	return value
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

// TestServiceEnforcesMaxFileBytesBeforeParser 验证大小边界使用严格的“超过”语义，
// 等于上限的文件允许解析，超过一个字节则记录 too_large 且绝不调用 Parser。
func TestServiceEnforcesMaxFileBytesBeforeParser(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	tests := []struct {
		name       string
		content    []byte
		wantCalls  int
		wantReason string
	}{
		{name: "exact_limit", content: []byte("abcd"), wantCalls: 1},
		{name: "one_byte_over", content: []byte("abcde"), wantCalls: 0, wantReason: "too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"file.test": tt.content}}}
			parser := &countingLanguageParser{}
			service, err := repositoryapp.New(git, fixedParserRegistry{parser: parser}, memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.Config{MaxFiles: 10, MaxFileBytes: 4})
			if err != nil {
				t.Fatal(err)
			}
			result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
				Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "size-" + tt.name,
			})
			if syncErr != nil {
				t.Fatal(syncErr)
			}
			if calls := parser.Calls(); calls != tt.wantCalls {
				t.Fatalf("Parser 调用次数错误：got=%d want=%d", calls, tt.wantCalls)
			}
			if tt.wantReason == "" {
				if len(result.SkippedFiles) != 0 {
					t.Fatalf("边界内文件不应跳过：%#v", result.SkippedFiles)
				}
			} else if len(result.SkippedFiles) != 1 || result.SkippedFiles[0].Reason != tt.wantReason {
				t.Fatalf("超限文件应记录原因：%#v", result.SkippedFiles)
			}
		})
	}
}

// TestServiceSkipsBinaryBeforeParser 验证明确的二进制内容不会进入语言 Parser，
// 同时在结果中保留 binary 原因，便于调用方区分“不支持”与“内容不是文本”。
func TestServiceSkipsBinaryBeforeParser(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"binary.test": {0x00, 0x01, 0x02}}}}
	parser := &countingLanguageParser{}
	service, err := repositoryapp.New(git, fixedParserRegistry{parser: parser}, memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "binary",
	})
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	if parser.Calls() != 0 || len(result.SkippedFiles) != 1 || result.SkippedFiles[0].Reason != "binary" {
		t.Fatalf("二进制文件应在 Parser 前跳过：calls=%d skipped=%#v", parser.Calls(), result.SkippedFiles)
	}
}

// TestServicePersistsParserFailure 验证语言 Parser 的错误使用 PARSE_FAILURE 收口，
// 保存脱敏后的失败 Job、Snapshot 和 parse.failed.v1 事件，并保留原始 cause 供内部诊断。
func TestServicePersistsParserFailure(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	cause := errors.New("语法树构造失败：内部细节")
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"broken.test": []byte("broken")}}}
	parser := &countingLanguageParser{parseErr: cause}
	store := memory.NewRepositoryStore()
	service, err := repositoryapp.New(git, fixedParserRegistry{parser: parser}, store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	key := "parser-failure"
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: key,
	})
	if !repository.IsCode(syncErr, repository.ErrParseFailure) || !errors.Is(syncErr, cause) {
		t.Fatalf("Parser 错误分类或 cause 丢失：%v", syncErr)
	}
	if result.Job.Status != repository.StatusFailed || result.Job.ErrorMessage != "仓库解析失败" || result.Snapshot.SyncStatus != repository.StatusFailed {
		t.Fatalf("Parser 失败状态未脱敏或不完整：%#v %#v", result.Job, result.Snapshot)
	}
	cached, found, lookupErr := store.FindByIdempotencyKey(context.Background(), scope, key)
	if lookupErr != nil || !found || cached.Event.EventType != "parse.failed.v1" || cached.Job.ErrorCode != string(repository.ErrParseFailure) {
		t.Fatalf("Parser 失败现场未持久化：found=%v result=%#v err=%v", found, cached, lookupErr)
	}
}

// TestServiceRecordsUnsupportedFilesWithoutReading 验证当前 MVP 对未支持语言并非无声丢弃：
// 它应在 SkippedFiles 和完成事件中留下明确记录，同时避免无意义的 Git Blob 读取。
func TestServiceRecordsUnsupportedFilesWithoutReading(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {
		"main.go": []byte("package main"), "lib.rs": []byte("fn main() {}"),
	}}}
	publisher := &collectingPublisher{}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), publisher, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "unsupported",
	})
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	if len(result.SkippedFiles) != 2 || result.SkippedFiles[0].Path != "lib.rs" || result.SkippedFiles[1].Path != "main.go" {
		t.Fatalf("未支持文件记录不完整：%#v", result.SkippedFiles)
	}
	for _, skipped := range result.SkippedFiles {
		if skipped.Reason != "unsupported" {
			t.Fatalf("未支持文件原因错误：%#v", skipped)
		}
	}
	if git.ReadCount() != 0 {
		t.Fatalf("未支持文件不应读取 Blob，reads=%d", git.ReadCount())
	}
	events := publisher.Events()
	if len(events) != 1 || events[0].Payload["skipped_count"] != 2 {
		t.Fatalf("完成事件应包含跳过数量：%#v", events)
	}
}

// TestServicePersistsOriginalFailureAfterContextCancellation 验证业务 Context 已取消时，
// 失败现场使用独立且有界的清理 Context 保存，最终仍返回最初的 Git 错误而不是 save_failure。
func TestServicePersistsOriginalFailureAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cause := errors.New("读取 Git Blob 失败")
	git := &cancelingReadGit{commit: "1111111111111111111111111111111111111111", cancel: cancel, cause: cause}
	store := memory.NewRepositoryStore()
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	key := "cancel-during-read"
	result, syncErr := service.Sync(ctx, repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: key,
	})
	if !repository.IsCode(syncErr, repository.ErrGitFailure) || !errors.Is(syncErr, cause) {
		t.Fatalf("取消后的失败保存不应覆盖原始 Git 错误：%v", syncErr)
	}
	if result.Job.Status != repository.StatusFailed || result.Snapshot.SyncStatus != repository.StatusFailed {
		t.Fatalf("失败状态不完整：job=%s snapshot=%s", result.Job.Status, result.Snapshot.SyncStatus)
	}
	cached, found, lookupErr := store.FindByIdempotencyKey(context.Background(), scope, key)
	if lookupErr != nil || !found || cached.Event.EventType != "parse.failed.v1" || cached.Job.ErrorCode != string(repository.ErrGitFailure) {
		t.Fatalf("取消后仍应保存原始失败现场：found=%v result=%#v err=%v", found, cached, lookupErr)
	}
}

// TestFailureProgressIncludesAlreadyInspectedChanges 验证失败进度按已经完成检查的变更计算。
// 即使前一个文件因 unsupported 被跳过，它也已经处理完成，第二个文件失败时进度应为 50%。
func TestFailureProgressIncludesAlreadyInspectedChanges(t *testing.T) {
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
	git := &fakeGit{
		commits: map[string]string{"main": currentSHA},
		files:   map[string]map[string][]byte{currentSHA: {"b.py": []byte("def b(): pass")}},
		diffs: map[string][]repository.ChangedPath{baseSHA + ":" + currentSHA: {
			{Path: "a.unsupported", Kind: repository.ChangeAdded},
			{Path: "b.py", Kind: repository.ChangeModified},
		}},
		readErr: errors.New("读取第二个文件失败"),
	}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "partial-progress",
	})
	if !repository.IsCode(syncErr, repository.ErrGitFailure) {
		t.Fatalf("预期第二个文件读取失败：%v", syncErr)
	}
	if result.Job.Progress != 50 {
		t.Fatalf("失败进度未包含已经检查的跳过文件：got=%d want=50", result.Job.Progress)
	}
}

// TestCompletedParseResultSurvivesSaveFailure 验证解析已经成功收口后，即使持久化失败，
// 调用方仍能拿到带 Snapshot、Job 和完成事件的 ParseResult，而不是无法诊断的零值。
func TestCompletedParseResultSurvivesSaveFailure(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	cause := errors.New("数据库写入失败")
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {}}}
	store := saveErrorStore{delegate: memory.NewRepositoryStore(), err: cause}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "save-failure",
	})
	if !repository.IsCode(syncErr, repository.ErrPersistence) || !errors.Is(syncErr, cause) {
		t.Fatalf("保存失败分类错误：%v", syncErr)
	}
	if result.Snapshot.SnapshotID == "" || result.Snapshot.CommitSHA != sha || result.Job.Status != repository.StatusSucceeded || result.Event.EventType != "parse.completed.v1" {
		t.Fatalf("保存失败不应清空已经完成的 ParseResult：%#v", result)
	}
}

// TestPublishFailureCanBeRecoveredByIdempotentRetry 验证先保存后发布的当前恢复语义：
// 首次 Broker 失败保留成功 ParseResult，使用同一幂等键重试后只补发事件，不重复解析 Git。
func TestPublishFailureCanBeRecoveredByIdempotentRetry(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {}}}
	publisher := &flakyPublisher{failures: 1}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), publisher, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	cmd := repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "publish-retry",
	}
	first, firstErr := service.Sync(context.Background(), cmd)
	if !repository.IsCode(firstErr, repository.ErrPersistence) || first.Job.Status != repository.StatusSucceeded {
		t.Fatalf("首次发布失败应保留成功解析结果：result=%#v err=%v", first, firstErr)
	}
	resolveCalls := git.ResolveCount()
	second, secondErr := service.Sync(context.Background(), cmd)
	if secondErr != nil || second.Snapshot.SnapshotID != first.Snapshot.SnapshotID || git.ResolveCount() != resolveCalls {
		t.Fatalf("幂等重试应只补发已保存事件：result=%#v resolves=%d err=%v", second, git.ResolveCount(), secondErr)
	}
	attempts, delivered := publisher.Counts()
	if attempts != 2 || delivered != 1 {
		t.Fatalf("发布恢复次数错误：attempts=%d delivered=%d", attempts, delivered)
	}
}

// TestServiceProducesConsistentTerminalMetadata 验证成功结果中的 Job、Snapshot、EntityMeta
// 使用一致终态，并满足 CreatedAt <= UpdatedAt <= Event.OccurredAt 的时间顺序。
func TestServiceProducesConsistentTerminalMetadata(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	clock := &steppingClock{now: time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC), step: time.Second}
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {}}}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, clock, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "metadata",
	})
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	if result.Job.Status != repository.StatusSucceeded || result.Job.EntityMeta.Status != string(repository.StatusSucceeded) || result.Job.Progress != 100 ||
		result.Snapshot.SyncStatus != repository.StatusSucceeded || result.Snapshot.EntityMeta.Status != string(repository.StatusSucceeded) {
		t.Fatalf("终态字段不一致：job=%#v snapshot=%#v", result.Job, result.Snapshot)
	}
	if result.Job.UpdatedAt != result.Snapshot.UpdatedAt || result.Job.UpdatedAt.Before(result.Job.CreatedAt) || result.Event.OccurredAt.Before(result.Job.UpdatedAt) {
		t.Fatalf("终态时间顺序错误：created=%s updated=%s event=%s", result.Job.CreatedAt, result.Job.UpdatedAt, result.Event.OccurredAt)
	}
}

// TestRepositoryEventsConformToPublishedSchemas 从 api/events 读取真实 Schema，
// 对 Service 实际构造的完成和失败事件执行递归必填字段、常量与 JSON 类型校验。
func TestRepositoryEventsConformToPublishedSchemas(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	t.Run("completed", func(t *testing.T) {
		git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {}}}
		service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, syncErr := service.Sync(context.Background(), repository.SyncCommand{Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "schema-completed"})
		if syncErr != nil {
			t.Fatal(syncErr)
		}
		assertEventMatchesSchema(t, result.Event, "parse.completed.v1.schema.json")
	})
	t.Run("failed", func(t *testing.T) {
		git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {"a.py": []byte("def a(): pass")}}, readErr: errors.New("read failed")}
		service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, syncErr := service.Sync(context.Background(), repository.SyncCommand{Scope: scope, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "schema-failed"})
		if syncErr == nil {
			t.Fatal("预期读取失败")
		}
		assertEventMatchesSchema(t, result.Event, "parse.failed.v1.schema.json")
	})
}

// TestCompletedEventPayloadMatchesParseResult 验证事件中的数量与实际持久化结果来自同一份收口数据，
// 避免消费者按错误的 artifact、relation 或 skipped 数量启动下游 Graph/RAG 工作。
func TestCompletedEventPayloadMatchesParseResult(t *testing.T) {
	sha := "1111111111111111111111111111111111111111"
	git := &fakeGit{commits: map[string]string{"main": sha}, files: map[string]map[string][]byte{sha: {
		"a.py": []byte("def a():\n    return helper()\n"), "main.go": []byte("package main"),
	}}}
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), recordingPublisher{}, recordingObserver{}, &sequenceIDs{}, fixedClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result, syncErr := service.Sync(context.Background(), repository.SyncCommand{
		Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}, RepositoryPath: "repo", Ref: "main", IdempotencyKey: "event-counts",
	})
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	payload := result.Event.Payload
	if payload["snapshot_id"] != result.Snapshot.SnapshotID || payload["commit_sha"] != result.Snapshot.CommitSHA ||
		payload["artifact_count"] != len(result.Artifacts) || payload["relation_count"] != len(result.Relations) || payload["skipped_count"] != len(result.SkippedFiles) {
		t.Fatalf("完成事件与 ParseResult 数量不一致：payload=%#v result=%#v", payload, result)
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

// assertEventMatchesSchema 使用仓库中的真实 JSON Schema 校验事件。
// 只实现这些事件 Schema 实际使用的关键字，避免测试复制一份独立的事件模板。
func assertEventMatchesSchema(t *testing.T, event common.EventEnvelope, schemaName string) {
	t.Helper()
	schemaBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "events", schemaName))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		t.Fatal(err)
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(eventBytes, &document); err != nil {
		t.Fatal(err)
	}
	validateJSONSchemaNode(t, schema, document, "event")
}

func validateJSONSchemaNode(t *testing.T, schema map[string]any, value any, location string) {
	t.Helper()
	if constant, exists := schema["const"]; exists && !reflect.DeepEqual(constant, value) {
		t.Fatalf("%s 不符合 const：got=%#v want=%#v", location, value, constant)
	}
	if expectedType, _ := schema["type"].(string); expectedType != "" && !matchesJSONType(expectedType, value) {
		t.Fatalf("%s JSON 类型错误：got=%T want=%s value=%#v", location, value, expectedType, value)
	}
	object, isObject := value.(map[string]any)
	if !isObject {
		return
	}
	for _, required := range stringValues(schema["required"]) {
		if _, exists := object[required]; !exists {
			t.Fatalf("%s 缺少必填字段 %q", location, required)
		}
	}
	properties, _ := schema["properties"].(map[string]any)
	if additional, exists := schema["additionalProperties"].(bool); exists && !additional {
		for key := range object {
			if _, declared := properties[key]; !declared {
				t.Fatalf("%s 包含 Schema 未声明字段 %q", location, key)
			}
		}
	}
	for key, rawProperty := range properties {
		propertySchema, ok := rawProperty.(map[string]any)
		propertyValue, exists := object[key]
		if ok && exists {
			validateJSONSchemaNode(t, propertySchema, propertyValue, location+"."+key)
		}
	}
}

func stringValues(value any) []string {
	raw, _ := value.([]any)
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func matchesJSONType(expected string, value any) bool {
	switch expected {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		number, ok := value.(float64)
		return ok && number == float64(int64(number))
	default:
		return true
	}
}
