package repositoryapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

// Config 控制仓库分析规模
// MaxFiles:一个仓库最多允许处理的文件数量
// MaxFileBytes:单个文件允许的最大字节数
type Config struct {
	MaxFiles     int
	MaxFileBytes int
}

func DefaultConfig() Config { return Config{MaxFiles: 100_000, MaxFileBytes: 2 << 20} }

// Service 完成仓库分析需要的所用能力
// git:访问Git仓库(本地Git命令、Go Git库、远程Git API、测试用内存实现)
// parsers:根据文件类型选择解析器
// store:持久化仓库分析结果
// events:发布业务事件(类似"分析任务已开始"、"仓库已经读取"等事件)
// observer:收集或上报服务运行过程中的可观测信息
// ids:生成业务标识符，负责唯一ID
// clock:抽象系统时间
// config:把资源策略注入服务
type Service struct {
	git      ports.GitRepository
	parsers  ports.ParserRegistry
	store    ports.RepositoryStore
	events   ports.EventPublisher
	observer ports.Observer
	ids      ports.IDGenerator
	clock    ports.Clock
	config   Config
}

// New 构造函数:最终返回一个可安全运行的服务实例
func New(git ports.GitRepository, parsers ports.ParserRegistry, store ports.RepositoryStore, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	// 保证核心依赖不为nil
	if git == nil || parsers == nil || store == nil {
		return nil, errors.New("Git、解析器注册表和存储不能为空")
	}
	// events、observer、ids、clock为可选依赖，
	if events == nil {
		events = noopPublisher{} // 空对象模式
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = RandomIDs{}
	}
	if clock == nil {
		clock = SystemClock{}
	}
	if config.MaxFiles <= 0 {
		config.MaxFiles = DefaultConfig().MaxFiles
	}
	if config.MaxFileBytes <= 0 {
		config.MaxFileBytes = DefaultConfig().MaxFileBytes
	}
	return &Service{git: git, parsers: parsers, store: store, events: events, observer: observer, ids: ids, clock: clock, config: config}, nil
}

// Sync 仓库同步与代码解析
// 输入:
//
//	ctx:控制取消、超市以及调用链上下文
//	cmd:同步命令，包括仓库地址、Git引用等
//
// 输出:
//
//	ParseResult:同步与解析结果
//	error:参数、Git、解析、存储
func (s *Service) Sync(ctx context.Context, cmd repository.SyncCommand) (result repository.ParseResult, err error) {
	// 负责校验命令自身的基本约束，错误被转换为统一的domainError
	// domain Error(错误类型，失败阶段，对外信息，是否可重试，原始错误)
	if err := cmd.Validate(); err != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if err := validatePatterns(cmd.IncludePaths); err != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	// 幂等性检查:防止同一个同步请求被重复执行
	if cached, ok, err := s.store.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); err != nil {
		return result, domainError(repository.ErrPersistence, "idempotency_lookup", "检查幂等键失败", true, err)
	} else if ok {
		// 记录幂等命中的次数
		s.observer.Count("repository_parse_idempotency_hits_total", 1, labels(cmd.Scope))
		if err := s.events.Publish(ctx, cached.Event); err != nil {
			return cached, domainError(repository.ErrPersistence, "publish_event", "发布已存储事件失败", true, err)
		}
		return cached, nil
	}
	// 启动同步阶段观测
	finish := s.observer.Stage(ctx, "sync", labels(cmd.Scope))
	defer func() { finish(err) }()
	// 解析目标的提交
	commit, err := s.git.ResolveCommit(ctx, cmd.RepositoryPath, cmd.Ref)
	if err != nil {
		return result, err
	}
	// 确定全量或增量分析
	previous, hasPrevious, err := s.store.LatestSnapshot(ctx, cmd.Scope)
	if err != nil {
		return result, domainError(repository.ErrPersistence, "latest_snapshot", "加载增量基线失败", true, err)
	}
	var changes []repository.ChangedPath
	parseScope := repository.ScopeFull
	// 增量分析
	if hasPrevious {
		parseScope = repository.ScopeIncremental
		changes, err = s.git.Diff(ctx, cmd.RepositoryPath, previous.CommitSHA, commit)
		// 全量分析
	} else {
		var files []string
		files, err = s.git.ListFiles(ctx, cmd.RepositoryPath, commit)
		changes = make([]repository.ChangedPath, 0, len(files))
		for _, file := range files {
			changes = append(changes, repository.ChangedPath{Path: file, Kind: repository.ChangeAdded})
		}
	}
	if err != nil {
		return result, err
	}
	// 过滤、限制和排序
	changes = filterChanges(changes, cmd.IncludePaths)
	if len(changes) > s.config.MaxFiles {
		return result, domainError(repository.ErrInvalidInput, "scope", fmt.Sprintf("解析范围超过 %d 个文件", s.config.MaxFiles), false, nil)
	}
	// 排序：解析顺序稳定、输出顺序稳定、测试结果可重复等等工程价值
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	// 初始化快照、解析任务和结果
	now := s.clock.Now().UTC() // 时间转换为UTC
	snapshotID, jobID := s.ids.New("snap"), s.ids.New("job")
	parentID := ""
	if hasPrevious {
		parentID = previous.SnapshotID
	}
	// 创建运行中的快照
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, cmd.Scope, repository.StatusRunning, now), SnapshotID: snapshotID,
		Provider: providerOrLocal(cmd.Provider), Ref: cmd.Ref, CommitSHA: commit, ParentSnapshotID: parentID, SyncStatus: repository.StatusRunning, ChangedPaths: changes}
	// 创建解析任务
	job := repository.ParseJob{EntityMeta: repository.NewMeta(jobID, cmd.Scope, repository.StatusRunning, now), JobID: jobID, SnapshotID: snapshotID,
		ParserVersion: "registry@1", Scope: parseScope, Status: repository.StatusRunning}
	// 初始化结果聚合
	result = repository.ParseResult{Snapshot: snapshot, Job: job, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}}
	// 逐个处理变更文件
	for index, change := range changes {
		// 文件已删除
		if change.Kind == repository.ChangeDeleted {
			result.DeletedPaths = append(result.DeletedPaths, change.Path)
			continue
		}
		// 文件被重命名
		if change.Kind == repository.ChangeRenamed && change.OldPath != "" {
			result.DeletedPaths = append(result.DeletedPaths, change.OldPath)
		}
		// 根据路径选择解析器
		languageParser, supported := s.parsers.ForPath(change.Path)
		if !supported {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "unsupported"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "unsupported"})
			continue
		}
		// 读取文件内容
		content, readErr := s.git.ReadFile(ctx, cmd.RepositoryPath, commit, change.Path)
		// 读取失败时
		if readErr != nil {
			return s.fail(ctx, cmd, result, repository.ErrGitFailure, "read_file", readErr)
		}
		// 限制单文件大小
		if len(content) > s.config.MaxFileBytes {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "too_large"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "too_large"})
			continue
		}
		// 过滤二进制文件
		if isBinary(content) {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "binary"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "binary"})
			continue
		}
		// 调用语言解析器
		parsed, parseErr := languageParser.Parse(ctx, commit, repository.FileContent{Path: change.Path, Content: content})
		if parseErr != nil {
			return s.fail(ctx, cmd, result, repository.ErrParseFailure, "parse_file", parseErr)
		}
		// 聚合解析结果和更新进度
		result.Artifacts = append(result.Artifacts, parsed.Artifacts...)
		result.Relations = append(result.Relations, parsed.Relations...)
		result.Job.Progress = (index + 1) * 100 / max(1, len(changes))
		s.observer.Count("repository_parse_files_total", 1, map[string]string{"language": languageParser.Language()})
	}
	// 更新状态
	result.Job.Progress = 100
	result.Job.Status, result.Job.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Job.UpdatedAt = s.clock.Now().UTC()
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Snapshot.UpdatedAt = result.Job.UpdatedAt
	// 创建完成事件
	result.Event = s.event(cmd.Scope, "parse.completed.v1", snapshotID, map[string]any{"snapshot_id": snapshotID, "commit_sha": commit,
		"artifact_count": len(result.Artifacts), "relation_count": len(result.Relations), "deleted_paths": result.DeletedPaths, "skipped_count": len(result.SkippedFiles)})
	// 先持久化、再发布事件
	if err = s.store.SaveResult(ctx, cmd.IdempotencyKey, result); err != nil {
		return repository.ParseResult{}, domainError(repository.ErrPersistence, "save_result", "保存解析结果失败", true, err)
	}
	if err = s.events.Publish(ctx, result.Event); err != nil {
		return result, domainError(repository.ErrPersistence, "publish_event", "解析结果已保存，但事件发布失败", true, err)
	}
	s.observer.Count("repository_parse_artifacts_total", int64(len(result.Artifacts)), labels(cmd.Scope))
	return result, nil
}

// 统一失败收口
// result:解析到失败位置时的阶段性结果
// cause:底层原始错误
func (s *Service) fail(ctx context.Context, cmd repository.SyncCommand, result repository.ParseResult, code repository.ErrorCode, operation string, cause error) (repository.ParseResult, error) {
	// 统一生成失败时间
	now := s.clock.Now().UTC()
	// 将解析任务转换成失败状态
	result.Job.Status, result.Job.EntityMeta.Status, result.Job.ErrorCode, result.Job.ErrorMessage = repository.StatusFailed, string(repository.StatusFailed), string(code), "仓库解析失败"
	result.Job.UpdatedAt = now
	// 将快照转换为失败状态
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status, result.Snapshot.ErrorCode, result.Snapshot.ErrorMessage = repository.StatusFailed, string(repository.StatusFailed), string(code), "仓库解析失败"
	result.Snapshot.UpdatedAt = now
	// 创建版本化失败时间
	result.Event = s.event(cmd.Scope, "parse.failed.v1", result.Snapshot.SnapshotID, map[string]any{"snapshot_id": result.Snapshot.SnapshotID, "error_code": string(code), "retryable": true})
	// 保存失败现场
	if saveErr := s.store.SaveResult(ctx, cmd.IdempotencyKey, result); saveErr != nil {
		return result, domainError(repository.ErrPersistence, "save_failure", "保存解析失败状态失败", true, saveErr)
	}
	// 发布失败时间，先保存再发布
	if publishErr := s.events.Publish(ctx, result.Event); publishErr != nil {
		return result, domainError(repository.ErrPersistence, "publish_failure", "解析失败状态已保存，但事件发布失败", true, publishErr)
	}
	return result, domainError(code, operation, "仓库解析失败", true, cause)
}

// 标准事件信封
func (s *Service) event(scope common.Scope, eventType, aggregate string, payload map[string]any) common.EventEnvelope {
	return common.EventEnvelope{EventID: s.ids.New("evt"), EventType: eventType, AggregateID: aggregate, OccurredAt: s.clock.Now().UTC(), Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID, Payload: payload}
}

// 根据路径匹配规则筛选Git变更(将"整个残酷的Git变更"投影成"用户关注范围内的变更")
func filterChanges(changes []repository.ChangedPath, patterns []string) []repository.ChangedPath {
	// 没有过滤规则
	if len(patterns) == 0 {
		return changes
	}
	// 预分配输出切片
	out := make([]repository.ChangedPath, 0, len(changes))
	for _, change := range changes {
		// 分别判断新旧路径是否在过滤范围内
		newIncluded, oldIncluded := matchesPatterns(change.Path, patterns), change.OldPath != "" && matchesPatterns(change.OldPath, patterns)
		switch {
		case change.Kind == repository.ChangeRenamed && oldIncluded && !newIncluded:
			out = append(out, repository.ChangedPath{Path: change.OldPath, Kind: repository.ChangeDeleted})
		case change.Kind == repository.ChangeRenamed && newIncluded && !oldIncluded:
			out = append(out, repository.ChangedPath{Path: change.Path, Kind: repository.ChangeAdded})
		case newIncluded:
			out = append(out, change)
		}
	}
	return out
}

// 判断文件路径是否匹配路径模式
func matchesPatterns(file string, patterns []string) bool {
	for _, pattern := range patterns {
		matched, _ := path.Match(pattern, filepath.ToSlash(file))
		prefix := strings.TrimSuffix(pattern, "/**")
		if matched || prefix != pattern && strings.HasPrefix(filepath.ToSlash(file), strings.TrimSuffix(prefix, "/")+"/") {
			return true
		}
	}
	return false
}
func validatePatterns(patterns []string) error {
	for _, pattern := range patterns {
		if strings.TrimSpace(pattern) == "" || strings.HasPrefix(pattern, "/") || hasParentSegment(pattern) {
			return fmt.Errorf("包含路径无效：%q", pattern)
		}
		if _, err := path.Match(pattern, "probe"); err != nil {
			return fmt.Errorf("包含路径无效：%q", pattern)
		}
	}
	return nil
}

// 判断路径模式是否包含独立的父目录字段
func hasParentSegment(pattern string) bool {
	for _, segment := range strings.Split(filepath.ToSlash(pattern), "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}
func isBinary(content []byte) bool {
	sample := content
	if len(sample) > 8000 {
		sample = sample[:8000]
	}
	return bytes.IndexByte(sample, 0) >= 0 || !utf8.Valid(sample)
}
func providerOrLocal(provider string) string {
	if provider == "" {
		return "local"
	}
	return provider
}
func labels(scope common.Scope) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "trace_id": scope.TraceID}
}
func domainError(code repository.ErrorCode, op, message string, retry bool, cause error) error {
	return &repository.DomainError{Code: code, Operation: op, Message: message, Retryable: retry, Cause: cause}
}

type RandomIDs struct{}

func (RandomIDs) New(prefix string) string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("安全随机数源不可用")
	}
	return prefix + "_" + hex.EncodeToString(value[:])
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}
