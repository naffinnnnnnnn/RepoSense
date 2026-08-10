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

type Config struct {
	MaxFiles     int
	MaxFileBytes int
}

func DefaultConfig() Config { return Config{MaxFiles: 100_000, MaxFileBytes: 2 << 20} }

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

func New(git ports.GitRepository, parsers ports.ParserRegistry, store ports.RepositoryStore, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if git == nil || parsers == nil || store == nil {
		return nil, errors.New("Git、解析器注册表和存储不能为空")
	}
	if events == nil {
		events = noopPublisher{}
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

func (s *Service) Sync(ctx context.Context, cmd repository.SyncCommand) (result repository.ParseResult, err error) {
	if err := cmd.Validate(); err != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if err := validatePatterns(cmd.IncludePaths); err != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if cached, ok, err := s.store.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); err != nil {
		return result, domainError(repository.ErrPersistence, "idempotency_lookup", "检查幂等键失败", true, err)
	} else if ok {
		s.observer.Count("repository_parse_idempotency_hits_total", 1, labels(cmd.Scope))
		if err := s.events.Publish(ctx, cached.Event); err != nil {
			return cached, domainError(repository.ErrPersistence, "publish_event", "发布已存储事件失败", true, err)
		}
		return cached, nil
	}

	finish := s.observer.Stage(ctx, "sync", labels(cmd.Scope))
	defer func() { finish(err) }()
	commit, err := s.git.ResolveCommit(ctx, cmd.RepositoryPath, cmd.Ref)
	if err != nil {
		return result, err
	}

	previous, hasPrevious, err := s.store.LatestSnapshot(ctx, cmd.Scope)
	if err != nil {
		return result, domainError(repository.ErrPersistence, "latest_snapshot", "加载增量基线失败", true, err)
	}
	var changes []repository.ChangedPath
	parseScope := repository.ScopeFull
	if hasPrevious {
		parseScope = repository.ScopeIncremental
		changes, err = s.git.Diff(ctx, cmd.RepositoryPath, previous.CommitSHA, commit)
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
	changes = filterChanges(changes, cmd.IncludePaths)
	if len(changes) > s.config.MaxFiles {
		return result, domainError(repository.ErrInvalidInput, "scope", fmt.Sprintf("解析范围超过 %d 个文件", s.config.MaxFiles), false, nil)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })

	now := s.clock.Now().UTC()
	snapshotID, jobID := s.ids.New("snap"), s.ids.New("job")
	parentID := ""
	if hasPrevious {
		parentID = previous.SnapshotID
	}
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, cmd.Scope, repository.StatusRunning, now), SnapshotID: snapshotID,
		Provider: providerOrLocal(cmd.Provider), Ref: cmd.Ref, CommitSHA: commit, ParentSnapshotID: parentID, SyncStatus: repository.StatusRunning, ChangedPaths: changes}
	job := repository.ParseJob{EntityMeta: repository.NewMeta(jobID, cmd.Scope, repository.StatusRunning, now), JobID: jobID, SnapshotID: snapshotID,
		ParserVersion: "registry@1", Scope: parseScope, Status: repository.StatusRunning}
	result = repository.ParseResult{Snapshot: snapshot, Job: job, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}}

	for index, change := range changes {
		if change.Kind == repository.ChangeDeleted {
			result.DeletedPaths = append(result.DeletedPaths, change.Path)
			continue
		}
		if change.Kind == repository.ChangeRenamed && change.OldPath != "" {
			result.DeletedPaths = append(result.DeletedPaths, change.OldPath)
		}
		languageParser, supported := s.parsers.ForPath(change.Path)
		if !supported {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "unsupported"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "unsupported"})
			continue
		}
		content, readErr := s.git.ReadFile(ctx, cmd.RepositoryPath, commit, change.Path)
		if readErr != nil {
			return s.fail(ctx, cmd, result, repository.ErrGitFailure, "read_file", readErr)
		}
		if len(content) > s.config.MaxFileBytes {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "too_large"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "too_large"})
			continue
		}
		if isBinary(content) {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "binary"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "binary"})
			continue
		}
		parsed, parseErr := languageParser.Parse(ctx, commit, repository.FileContent{Path: change.Path, Content: content})
		if parseErr != nil {
			return s.fail(ctx, cmd, result, repository.ErrParseFailure, "parse_file", parseErr)
		}
		result.Artifacts = append(result.Artifacts, parsed.Artifacts...)
		result.Relations = append(result.Relations, parsed.Relations...)
		result.Job.Progress = (index + 1) * 100 / max(1, len(changes))
		s.observer.Count("repository_parse_files_total", 1, map[string]string{"language": languageParser.Language()})
	}
	result.Job.Progress = 100
	result.Job.Status, result.Job.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Job.UpdatedAt = s.clock.Now().UTC()
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Snapshot.UpdatedAt = result.Job.UpdatedAt
	result.Event = s.event(cmd.Scope, "parse.completed.v1", snapshotID, map[string]any{"snapshot_id": snapshotID, "commit_sha": commit,
		"artifact_count": len(result.Artifacts), "relation_count": len(result.Relations), "deleted_paths": result.DeletedPaths, "skipped_count": len(result.SkippedFiles)})
	if err = s.store.SaveResult(ctx, cmd.IdempotencyKey, result); err != nil {
		return repository.ParseResult{}, domainError(repository.ErrPersistence, "save_result", "保存解析结果失败", true, err)
	}
	if err = s.events.Publish(ctx, result.Event); err != nil {
		return result, domainError(repository.ErrPersistence, "publish_event", "解析结果已保存，但事件发布失败", true, err)
	}
	s.observer.Count("repository_parse_artifacts_total", int64(len(result.Artifacts)), labels(cmd.Scope))
	return result, nil
}

func (s *Service) fail(ctx context.Context, cmd repository.SyncCommand, result repository.ParseResult, code repository.ErrorCode, operation string, cause error) (repository.ParseResult, error) {
	now := s.clock.Now().UTC()
	result.Job.Status, result.Job.EntityMeta.Status, result.Job.ErrorCode, result.Job.ErrorMessage = repository.StatusFailed, string(repository.StatusFailed), string(code), "仓库解析失败"
	result.Job.UpdatedAt = now
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status, result.Snapshot.ErrorCode, result.Snapshot.ErrorMessage = repository.StatusFailed, string(repository.StatusFailed), string(code), "仓库解析失败"
	result.Snapshot.UpdatedAt = now
	result.Event = s.event(cmd.Scope, "parse.failed.v1", result.Snapshot.SnapshotID, map[string]any{"snapshot_id": result.Snapshot.SnapshotID, "error_code": string(code), "retryable": true})
	if saveErr := s.store.SaveResult(ctx, cmd.IdempotencyKey, result); saveErr != nil {
		return result, domainError(repository.ErrPersistence, "save_failure", "保存解析失败状态失败", true, saveErr)
	}
	if publishErr := s.events.Publish(ctx, result.Event); publishErr != nil {
		return result, domainError(repository.ErrPersistence, "publish_failure", "解析失败状态已保存，但事件发布失败", true, publishErr)
	}
	return result, domainError(code, operation, "仓库解析失败", true, cause)
}

func (s *Service) event(scope common.Scope, eventType, aggregate string, payload map[string]any) common.EventEnvelope {
	return common.EventEnvelope{EventID: s.ids.New("evt"), EventType: eventType, AggregateID: aggregate, OccurredAt: s.clock.Now().UTC(), Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID, Payload: payload}
}
func filterChanges(changes []repository.ChangedPath, patterns []string) []repository.ChangedPath {
	if len(patterns) == 0 {
		return changes
	}
	out := make([]repository.ChangedPath, 0, len(changes))
	for _, change := range changes {
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
