package repositoryapp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func (s *Service) syncDirect(ctx context.Context, cmd repository.SyncCommand) (result repository.ParseResult, err error) {
	if validateErr := cmd.Validate(); validateErr != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", validateErr.Error(), false, validateErr)
	}
	if validateErr := validatePatterns(cmd.IncludePaths); validateErr != nil {
		return result, domainError(repository.ErrInvalidInput, "validate", validateErr.Error(), false, validateErr)
	}
	fingerprint := commandFingerprint(cmd)
	repositoryIdentity := ""
	if s.workspace != nil {
		prepared, prepareErr := s.workspace.Prepare(ctx, cmd)
		if prepareErr != nil {
			return result, domainError(repository.ErrGitFailure, "prepare_workspace", "准备仓库工作区失败", true, prepareErr)
		}
		cmd.RepositoryPath, cmd.Provider = prepared.Path, prepared.Provider
		repositoryIdentity = prepared.CanonicalIdentity
	} else if cmd.RepositoryURL != "" {
		return result, domainError(repository.ErrInvalidInput, "prepare_workspace", "远程仓库需要 RepositoryWorkspace", false, nil)
	}
	initialMiss := false
	if cached, ok, lookupErr := s.lookup(ctx, cmd); lookupErr != nil {
		return result, lookupErr
	} else if ok {
		if reusable, reuseErr := s.reusableCached(ctx, cmd, fingerprint, cached); reuseErr != nil {
			return result, reuseErr
		} else if reusable {
			return s.deliverCached(ctx, cmd, cached)
		}
	} else {
		initialMiss = true
	}

	unlockKey, _ := s.acquireFor(s.keyLocks, scopeKey(cmd.Scope)+"\x00"+cmd.IdempotencyKey)
	defer unlockKey()
	if cached, ok, lookupErr := s.lookup(ctx, cmd); lookupErr != nil {
		return result, lookupErr
	} else if ok {
		if initialMiss && cached.Job.Status == repository.StatusSucceeded {
			return s.deliverCached(ctx, cmd, cached)
		}
		if reusable, reuseErr := s.reusableCached(ctx, cmd, fingerprint, cached); reuseErr != nil {
			return result, reuseErr
		} else if reusable {
			return s.deliverCached(ctx, cmd, cached)
		}
	}

	unlockRepo := s.lockFor(s.repoLocks, scopeKey(cmd.Scope))
	defer unlockRepo()
	finish := s.observer.Stage(ctx, "sync", labels(cmd.Scope))
	defer func() { finish(err) }()

	now := s.clock.Now().UTC()
	snapshotID, idErr := s.newID("snap")
	if idErr != nil {
		return result, domainError(repository.ErrPersistence, "generate_id", "生成任务标识失败", true, idErr)
	}
	jobID, idErr := s.newID("job")
	if idErr != nil {
		return result, domainError(repository.ErrPersistence, "generate_id", "生成任务标识失败", true, idErr)
	}
	parserVersion := "registry@2"
	if registry, ok := s.parsers.(interface{ Version() string }); ok {
		parserVersion = registry.Version()
	}
	result = repository.ParseResult{
		Snapshot:  repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, cmd.Scope, repository.StatusRunning, now), SnapshotID: snapshotID, Provider: providerOrLocal(cmd.Provider), Ref: cmd.Ref, SyncStatus: repository.StatusRunning, ChangedPaths: []repository.ChangedPath{}},
		Job:       repository.ParseJob{EntityMeta: repository.NewMeta(jobID, cmd.Scope, repository.StatusRunning, now), JobID: jobID, SnapshotID: snapshotID, ParserVersion: parserVersion, Scope: repository.ScopeFull, Status: repository.StatusRunning},
		Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}, DeletedPaths: []string{}, SkippedFiles: []repository.SkippedFile{},
	}

	identity, identityErr := canonicalRepositoryIdentity(cmd.RepositoryPath)
	if repositoryIdentity != "" {
		identity, identityErr = repositoryIdentity, nil
	}
	if identityErr != nil {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrInvalidInput, "repository_identity", "仓库路径无效", false), identityErr)
	}
	if !s.bindIdentity(cmd.Scope, identity) {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrInvalidInput, "repository_identity", "RepositoryID 已绑定到另一仓库", false), errors.New("仓库身份冲突"))
	}

	commit, resolveErr := s.git.ResolveCommit(ctx, cmd.RepositoryPath, cmd.Ref)
	if resolveErr != nil {
		return s.finalizeFailure(ctx, cmd, result, descriptorFor(resolveErr, repository.ErrGitFailure, "resolve_commit", "解析 Git 引用失败", true), resolveErr)
	}
	result.Snapshot.CommitSHA = commit
	previous, hasPrevious, baselineErr := s.store.LatestSnapshot(ctx, cmd.Scope)
	if baselineErr != nil {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "latest_snapshot", "加载增量基线失败", true), baselineErr)
	}

	var changes []repository.ChangedPath
	if hasPrevious {
		result.Job.Scope = repository.ScopeIncremental
		result.Snapshot.ParentSnapshotID = previous.SnapshotID
		changes, err = s.git.Diff(ctx, cmd.RepositoryPath, previous.CommitSHA, commit)
		if repository.IsCode(err, repository.ErrRefNotFound) {
			result.Job.Scope = repository.ScopeFull
			result.Snapshot.ParentSnapshotID = ""
			var files []string
			files, err = s.git.ListFiles(ctx, cmd.RepositoryPath, commit)
			changes = filesAsChanges(files)
		}
	} else {
		var files []string
		files, err = s.git.ListFiles(ctx, cmd.RepositoryPath, commit)
		changes = filesAsChanges(files)
	}
	if err != nil {
		op := "list_files"
		if hasPrevious {
			op = "diff"
		}
		return s.finalizeFailure(ctx, cmd, result, descriptorFor(err, repository.ErrGitFailure, op, "读取 Git 变更失败", true), err)
	}
	changes, err = normalizeChanges(changes)
	if err != nil {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrGitFailure, "validate_changes", "Git 变更数据无效", false), err)
	}
	changes = filterChanges(changes, cmd.IncludePaths)
	if len(changes) > s.config.MaxFiles {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrInvalidInput, "scope", fmt.Sprintf("解析范围超过 %d 个文件", s.config.MaxFiles), false), nil)
	}
	result.Snapshot.ChangedPaths = changes
	for index, change := range changes {
		if change.Kind == repository.ChangeDeleted {
			result.DeletedPaths = append(result.DeletedPaths, change.Path)
			if progressErr := s.setProgress(ctx, &result, index, len(changes)); progressErr != nil {
				return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "update_progress", "更新任务进度失败", true), progressErr)
			}
			continue
		}
		if change.Kind == repository.ChangeRenamed {
			result.DeletedPaths = append(result.DeletedPaths, change.OldPath)
		}
		languageParser, supported := s.parsers.ForPath(change.Path)
		if !supported {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "unsupported"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "unsupported"})
			if progressErr := s.setProgress(ctx, &result, index, len(changes)); progressErr != nil {
				return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "update_progress", "更新任务进度失败", true), progressErr)
			}
			continue
		}
		content, readErr := s.git.ReadFile(ctx, cmd.RepositoryPath, commit, change.Path)
		if readErr != nil {
			return s.finalizeFailure(ctx, cmd, result, descriptorFor(readErr, repository.ErrGitFailure, "read_file", "读取仓库文件失败", true), readErr)
		}
		if len(content) > s.config.MaxFileBytes {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "too_large"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "too_large"})
			if progressErr := s.setProgress(ctx, &result, index, len(changes)); progressErr != nil {
				return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "update_progress", "更新任务进度失败", true), progressErr)
			}
			continue
		}
		if isBinary(content) {
			result.SkippedFiles = append(result.SkippedFiles, repository.SkippedFile{Path: change.Path, Reason: "binary"})
			s.observer.Count("repository_parse_files_skipped_total", 1, map[string]string{"reason": "binary"})
			if progressErr := s.setProgress(ctx, &result, index, len(changes)); progressErr != nil {
				return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "update_progress", "更新任务进度失败", true), progressErr)
			}
			continue
		}
		parsed, parseErr := languageParser.Parse(ctx, commit, repository.FileContent{Path: change.Path, Content: content})
		if parseErr != nil {
			return s.finalizeFailure(ctx, cmd, result, descriptorFor(parseErr, repository.ErrParseFailure, "parse_file", "代码文件解析失败", false), parseErr)
		}
		result.Artifacts = append(result.Artifacts, parsed.Artifacts...)
		result.Relations = append(result.Relations, parsed.Relations...)
		if progressErr := s.setProgress(ctx, &result, index, len(changes)); progressErr != nil {
			return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "update_progress", "更新任务进度失败", true), progressErr)
		}
		s.observer.Count("repository_parse_files_total", 1, map[string]string{"language": languageParser.Language()})
	}
	result.Job.Progress = 100
	result.Job.Status, result.Job.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Job.UpdatedAt = notBefore(s.clock.Now().UTC(), result.Job.CreatedAt)
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status = repository.StatusSucceeded, string(repository.StatusSucceeded)
	result.Snapshot.UpdatedAt = result.Job.UpdatedAt
	eventID, idErr := s.newID("evt")
	if idErr != nil {
		return s.finalizeFailure(ctx, cmd, result, describe(repository.ErrPersistence, "generate_event_id", "生成事件标识失败", true), idErr)
	}
	result.Event = repository.NewParseCompletedEvent(eventID, cmd.Scope, s.clock.Now(), repository.ParseCompletedPayload{SnapshotID: snapshotID, CommitSHA: commit, ArtifactCount: len(result.Artifacts), RelationCount: len(result.Relations), DeletedPaths: result.DeletedPaths, SkippedCount: len(result.SkippedFiles)})
	result.Event.OccurredAt = notBefore(result.Event.OccurredAt, result.Job.UpdatedAt)
	if saveErr := s.store.SaveResult(ctx, cmd.IdempotencyKey, result); saveErr != nil {
		return result, domainError(repository.ErrPersistence, "save_result", "保存解析结果失败", true, saveErr)
	}
	s.rememberFingerprint(cmd, fingerprint)
	s.setPublished(result.Event.EventID, false)
	if publishErr := s.events.Publish(ctx, result.Event); publishErr != nil {
		return result, domainError(repository.ErrPersistence, "publish_event", "解析结果已保存，但事件发布失败", true, publishErr)
	}
	s.markPublished(result.Event.EventID)
	s.observer.Count("repository_parse_artifacts_total", int64(len(result.Artifacts)), labels(cmd.Scope))
	s.observer.Count("repository_parse_jobs_total", 1, map[string]string{"status": string(repository.StatusSucceeded)})
	return result, nil
}

func (s *Service) finalizeFailure(ctx context.Context, cmd repository.SyncCommand, result repository.ParseResult, failure repository.FailureDescriptor, cause error) (repository.ParseResult, error) {
	now := notBefore(s.clock.Now().UTC(), result.Job.CreatedAt)
	terminal := repository.StatusFailed
	if failure.Operation == "cancelled" {
		terminal = repository.StatusCancelled
	}
	result.Job.Status, result.Job.EntityMeta.Status = terminal, string(terminal)
	result.Job.ErrorCode, result.Job.ErrorMessage, result.Job.UpdatedAt = string(failure.Code), failure.Message, now
	result.Snapshot.SyncStatus, result.Snapshot.EntityMeta.Status = terminal, string(terminal)
	result.Snapshot.ErrorCode, result.Snapshot.ErrorMessage, result.Snapshot.UpdatedAt = string(failure.Code), failure.Message, now
	eventID, eventIDErr := s.newID("evt")
	if eventIDErr == nil {
		result.Event = repository.NewParseFailedEvent(eventID, cmd.Scope, s.clock.Now(), repository.ParseFailedPayload{SnapshotID: result.Snapshot.SnapshotID, ErrorCode: failure.Code, Retryable: failure.Retryable})
	}
	result.Event.OccurredAt = notBefore(result.Event.OccurredAt, now)
	primary := domainError(failure.Code, failure.Operation, failure.Message, failure.Retryable, cause)
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.FailureCleanupTimeout)
	defer cancel()
	if saveErr := s.store.SaveResult(cleanupCtx, cmd.IdempotencyKey, result); saveErr != nil {
		return result, errors.Join(primary, domainError(repository.ErrPersistence, "save_failure", "保存解析失败状态失败", true, saveErr))
	}
	s.observer.Count("repository_parse_jobs_total", 1, map[string]string{"status": string(terminal), "error_code": string(failure.Code), "operation": failure.Operation})
	if publishErr := s.events.Publish(cleanupCtx, result.Event); publishErr != nil {
		return result, errors.Join(primary, domainError(repository.ErrPersistence, "publish_failure", "解析失败状态已保存，但事件发布失败", true, publishErr))
	}
	return result, primary
}

func (s *Service) lookup(ctx context.Context, cmd repository.SyncCommand) (repository.ParseResult, bool, error) {
	cached, ok, err := s.store.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey)
	if err == nil {
		return cached, ok, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return repository.ParseResult{}, false, err
	}
	return repository.ParseResult{}, false, domainError(repository.ErrPersistence, "idempotency_lookup", "检查幂等键失败", true, err)
}

func (s *Service) reusableCached(ctx context.Context, cmd repository.SyncCommand, fingerprint string, cached repository.ParseResult) (bool, error) {
	if cached.Job.Status != repository.StatusSucceeded {
		return false, nil
	}
	s.locksMu.Lock()
	recorded, known := s.fingerprints[scopeKey(cmd.Scope)+"\x00"+cmd.IdempotencyKey]
	s.locksMu.Unlock()
	if known && recorded != fingerprint {
		return false, domainError(repository.ErrInvalidInput, "idempotency_conflict", "幂等键已绑定到不同命令", false, nil)
	}
	if cached.Snapshot.Ref != cmd.Ref {
		return false, domainError(repository.ErrInvalidInput, "idempotency_conflict", "幂等键已绑定到不同命令", false, nil)
	}
	s.locksMu.Lock()
	published, deliveryKnown := s.published[cached.Event.EventID]
	s.locksMu.Unlock()
	if deliveryKnown && !published {
		return true, nil
	}
	if isMutableRef(cmd.Ref) {
		commit, err := s.git.ResolveCommit(ctx, cmd.RepositoryPath, cmd.Ref)
		if err != nil {
			return false, err
		}
		return commit == cached.Snapshot.CommitSHA, nil
	}
	return true, nil
}

func (s *Service) deliverCached(ctx context.Context, cmd repository.SyncCommand, cached repository.ParseResult) (repository.ParseResult, error) {
	s.observer.Count("repository_parse_idempotency_hits_total", 1, labels(cmd.Scope))
	s.locksMu.Lock()
	published := s.published[cached.Event.EventID]
	s.locksMu.Unlock()
	if !published {
		if err := s.events.Publish(ctx, cached.Event); err != nil {
			return cached, domainError(repository.ErrPersistence, "publish_event", "发布已存储事件失败", true, err)
		}
		s.markPublished(cached.Event.EventID)
	}
	return cached, nil
}

func (s *Service) lockFor(values map[string]*sync.Mutex, key string) func() {
	s.locksMu.Lock()
	lock := values[key]
	if lock == nil {
		lock = &sync.Mutex{}
		values[key] = lock
	}
	s.locksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (s *Service) acquireFor(values map[string]*sync.Mutex, key string) (func(), bool) {
	s.locksMu.Lock()
	lock := values[key]
	if lock == nil {
		lock = &sync.Mutex{}
		values[key] = lock
	}
	s.locksMu.Unlock()
	if lock.TryLock() {
		return lock.Unlock, false
	}
	lock.Lock()
	return lock.Unlock, true
}
func (s *Service) bindIdentity(scope common.Scope, identity string) bool {
	key := scopeKey(scope)
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if existing := s.identities[key]; existing != "" {
		return existing == identity
	}
	s.identities[key] = identity
	return true
}
func (s *Service) rememberFingerprint(cmd repository.SyncCommand, fingerprint string) {
	s.locksMu.Lock()
	s.fingerprints[scopeKey(cmd.Scope)+"\x00"+cmd.IdempotencyKey] = fingerprint
	s.locksMu.Unlock()
}
func (s *Service) markPublished(id string) {
	s.setPublished(id, true)
}
func (s *Service) setPublished(id string, value bool) {
	s.locksMu.Lock()
	s.published[id] = value
	s.locksMu.Unlock()
}
func (s *Service) setProgress(ctx context.Context, result *repository.ParseResult, index, total int) error {
	result.Job.Progress = (index + 1) * 100 / max(1, total)
	if reporter, ok := s.store.(interface {
		UpdateProgress(context.Context, int) error
	}); ok {
		return reporter.UpdateProgress(ctx, result.Job.Progress)
	}
	return nil
}

func normalizeChanges(changes []repository.ChangedPath) ([]repository.ChangedPath, error) {
	seen := map[string]bool{}
	out := make([]repository.ChangedPath, 0, len(changes))
	for _, change := range changes {
		if change.Kind != repository.ChangeAdded && change.Kind != repository.ChangeModified && change.Kind != repository.ChangeDeleted && change.Kind != repository.ChangeRenamed && change.Kind != repository.ChangeTypeChanged {
			return nil, fmt.Errorf("未知变更类型 %q", change.Kind)
		}
		clean, err := repository.CanonicalPath(change.Path)
		if err != nil {
			return nil, err
		}
		change.Path = clean
		if change.Kind == repository.ChangeRenamed {
			if change.OldPath == "" {
				return nil, fmt.Errorf("rename 缺少 old_path")
			}
			change.OldPath, err = repository.CanonicalPath(change.OldPath)
			if err != nil {
				return nil, err
			}
		} else if change.OldPath != "" {
			return nil, fmt.Errorf("非 rename 不能包含 old_path")
		}
		key := change.Path + "\x00" + string(change.Kind) + "\x00" + change.OldPath
		if !seen[key] {
			seen[key] = true
			out = append(out, change)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].OldPath < out[j].OldPath
	})
	return out, nil
}
func filesAsChanges(files []string) []repository.ChangedPath {
	out := make([]repository.ChangedPath, 0, len(files))
	for _, file := range files {
		out = append(out, repository.ChangedPath{Path: file, Kind: repository.ChangeAdded})
	}
	return out
}
func describe(code repository.ErrorCode, op, message string, retry bool) repository.FailureDescriptor {
	return repository.FailureDescriptor{Code: code, Operation: op, Message: message, Retryable: retry}
}
func descriptorFor(err error, fallback repository.ErrorCode, op, message string, retry bool) repository.FailureDescriptor {
	if errors.Is(err, context.Canceled) {
		return describe(repository.ErrInvalidInput, "cancelled", "解析任务已取消", false)
	}
	var domain *repository.DomainError
	if errors.As(err, &domain) {
		return describe(domain.Code, domain.Operation, domain.Message, domain.Retryable)
	}
	return describe(fallback, op, message, retry)
}
func canonicalRepositoryIdentity(value string) (string, error) {
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return strings.ToLower(filepath.Clean(absolute)), nil
}
func commandFingerprint(cmd repository.SyncCommand) string {
	paths := append([]string(nil), cmd.IncludePaths...)
	for i := range paths {
		paths[i] = strings.ReplaceAll(paths[i], `\`, "/")
	}
	sort.Strings(paths)
	return strings.Join([]string{cmd.RepositoryPath, cmd.RepositoryURL, cmd.Provider, cmd.Ref, cmd.CredentialsRef, strings.Join(paths, "\x00")}, "\x01")
}
func isMutableRef(ref string) bool {
	lower := strings.ToLower(strings.TrimSpace(ref))
	return lower == "head" || lower == "main" || lower == "master" || strings.HasPrefix(lower, "refs/heads/")
}
func notBefore(value, floor time.Time) time.Time {
	if value.Before(floor) {
		return floor
	}
	return value
}
func scopeKey(scope common.Scope) string { return scope.TenantID + "\x00" + scope.RepositoryID }

func (s *Service) newID(prefix string) (string, error) {
	if generator, ok := s.ids.(interface{ NewID(string) (string, error) }); ok {
		return generator.NewID(prefix)
	}
	id := s.ids.New(prefix)
	if id == "" {
		return "", errors.New("ID 生成器返回空值")
	}
	return id, nil
}
