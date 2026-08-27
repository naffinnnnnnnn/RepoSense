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

type PreparedRepository struct {
	Path              string
	CanonicalIdentity string
	Provider          string
}
type RepositoryWorkspace interface {
	Prepare(ctx context.Context, command repository.SyncCommand) (PreparedRepository, error)
}
type GitCredential struct{ Environment map[string]string }
type CredentialResolver interface {
	ResolveGitCredential(ctx context.Context, tenantID, credentialsRef string) (GitCredential, error)
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

type RepositoryTaskStore interface {
	RepositoryStore
	BindRepository(ctx context.Context, binding repository.RepositoryBinding) error
	AcquireIdempotency(ctx context.Context, task repository.ParseTask) (repository.ParseTask, bool, error)
	TaskByJobID(ctx context.Context, scope common.Scope, jobID string) (repository.ParseTask, error)
	ClaimPendingJob(ctx context.Context, owner string, lease time.Duration) (repository.ParseTask, bool, error)
	ClaimJob(ctx context.Context, jobID, owner string, lease time.Duration) (repository.ParseTask, error)
	HeartbeatJob(ctx context.Context, jobID, owner string, lease time.Duration, progress int) error
	SaveResultIfLatest(ctx context.Context, key, expectedParentID string, result repository.ParseResult) error
	CompleteResult(ctx context.Context, key, expectedParentID, leaseOwner string, result repository.ParseResult) error
	FailResult(ctx context.Context, key, leaseOwner string, result repository.ParseResult, retryable bool) error
	RequestCancel(ctx context.Context, scope common.Scope, jobID string) error
	RetryFailed(ctx context.Context, scope common.Scope, jobID, newJobID, newSnapshotID, commitSHA, parentSnapshotID string, now time.Time) (repository.ParseTask, error)
}

type OutboxStore interface {
	PendingOutbox(ctx context.Context, limit int, now time.Time) ([]repository.OutboxRecord, error)
	MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error
	MarkOutboxFailed(ctx context.Context, eventID, sanitizedError string, nextAttempt time.Time, deadLetter bool) error
}

type RetentionPolicy struct {
	FailedTaskRetention time.Duration
	OutboxRetention     time.Duration
}
type RetentionResult struct {
	FailedTasks, OutboxEvents int64
}
type RepositoryMaintenanceStore interface {
	CleanupExpired(context.Context, time.Time, RetentionPolicy) (RetentionResult, error)
}

type EventPublisher interface {
	Publish(ctx context.Context, event common.EventEnvelope) error
}

type Observer interface {
	Stage(ctx context.Context, name string, attributes map[string]string) func(error)
	Count(name string, value int64, attributes map[string]string)
}

type IDGenerator interface{ New(prefix string) string }
type FallibleIDGenerator interface {
	NewID(prefix string) (string, error)
}
type Clock interface{ Now() time.Time }
