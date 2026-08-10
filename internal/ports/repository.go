package ports

import (
	"context"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type GitRepository interface {
	ResolveCommit(ctx context.Context, repositoryPath, ref string) (string, error)
	ListFiles(ctx context.Context, repositoryPath, commitSHA string) ([]string, error)
	Diff(ctx context.Context, repositoryPath, fromCommit, toCommit string) ([]repository.ChangedPath, error)
	ReadFile(ctx context.Context, repositoryPath, commitSHA, path string) ([]byte, error)
}

type LanguageParser interface {
	Language() string
	Extensions() []string
	Version() string
	Parse(ctx context.Context, commitSHA string, file repository.FileContent) (repository.ParsedFile, error)
}

type ParserRegistry interface {
	ForPath(path string) (LanguageParser, bool)
}

type RepositoryStore interface {
	FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error)
	LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error)
	SaveResult(ctx context.Context, key string, result repository.ParseResult) error
	GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error)
	Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error)
}

type EventPublisher interface {
	Publish(ctx context.Context, event common.EventEnvelope) error
}

type Observer interface {
	Stage(ctx context.Context, name string, attributes map[string]string) func(error)
	Count(name string, value int64, attributes map[string]string)
}

type IDGenerator interface{ New(prefix string) string }
type Clock interface{ Now() time.Time }
