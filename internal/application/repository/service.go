package repositoryapp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type Config struct {
	MaxFiles, MaxFileBytes int
	FailureCleanupTimeout  time.Duration
}

func DefaultConfig() Config {
	return Config{MaxFiles: 100_000, MaxFileBytes: 2 << 20, FailureCleanupTimeout: 5 * time.Second}
}

type Service struct {
	git                      ports.GitRepository
	parsers                  ports.ParserRegistry
	store                    ports.RepositoryStore
	events                   ports.EventPublisher
	observer                 ports.Observer
	ids                      ports.IDGenerator
	clock                    ports.Clock
	config                   Config
	workspace                ports.RepositoryWorkspace
	locksMu                  sync.Mutex
	keyLocks, repoLocks      map[string]*sync.Mutex
	identities, fingerprints map[string]string
	published                map[string]bool
}

func (s *Service) WithWorkspace(workspace ports.RepositoryWorkspace) *Service {
	s.workspace = workspace
	return s
}

func New(git ports.GitRepository, parsers ports.ParserRegistry, store ports.RepositoryStore, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if git == nil || parsers == nil || store == nil {
		return nil, errors.New("Git、解析器注册表和存储不能为空")
	}
	if events == nil && observer != nil || events != nil && observer == nil {
		return nil, errors.New("事件发布器和可观测组件必须显式注入")
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
	if config.FailureCleanupTimeout == 0 {
		config.FailureCleanupTimeout = DefaultConfig().FailureCleanupTimeout
	}
	if config.MaxFiles <= 0 || config.MaxFileBytes <= 0 || config.FailureCleanupTimeout < 0 {
		return nil, errors.New("资源限制必须为正数")
	}
	return &Service{git: git, parsers: parsers, store: store, events: events, observer: observer, ids: ids, clock: clock, config: config,
		keyLocks: map[string]*sync.Mutex{}, repoLocks: map[string]*sync.Mutex{}, identities: map[string]string{}, fingerprints: map[string]string{}, published: map[string]bool{}}, nil
}

func filterChanges(changes []repository.ChangedPath, patterns []string) []repository.ChangedPath {
	if len(patterns) == 0 {
		return append([]repository.ChangedPath(nil), changes...)
	}
	out := make([]repository.ChangedPath, 0, len(changes))
	for _, change := range changes {
		newIncluded := matchesPatterns(change.Path, patterns)
		oldIncluded := change.OldPath != "" && matchesPatterns(change.OldPath, patterns)
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
		matched, err := repository.MatchPattern(pattern, file)
		if err == nil && matched {
			return true
		}
	}
	return false
}
func validatePatterns(patterns []string) error {
	for _, pattern := range patterns {
		if _, err := repository.CanonicalPattern(pattern); err != nil {
			return err
		}
	}
	return nil
}
func isBinary(content []byte) bool { return bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content) }
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
	id, _ := (RandomIDs{}).NewID(prefix)
	return id
}
func (RandomIDs) NewID(prefix string) (string, error) {
	var value [12]byte
	if _, err := rand.Reader.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(value[:]), nil
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
