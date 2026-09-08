package graphapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type ParserPageConsumer interface {
	ConsumeArtifactPage(context.Context, graph.SnapshotMetadata, []repository.CodeArtifact) error
	ConsumeRelationPage(context.Context, graph.SnapshotMetadata, []graph.ResolvedRelation) error
}

type SourceReader struct {
	source           ports.PagedGraphSource
	pageSize         int
	supportedSchemas map[string]struct{}
	retry            sourceRetryPolicy
}

type sourceRetryPolicy struct {
	attempts int
	initial  time.Duration
	maximum  time.Duration
}

func NewSourceReader(source ports.PagedGraphSource, pageSize int, supportedSchemas []string) (*SourceReader, error) {
	if source == nil {
		return nil, errors.New("paged graph source must not be nil")
	}
	if pageSize <= 0 || pageSize > 10_000 {
		return nil, errors.New("parser page size must be between 1 and 10000")
	}
	schemas := make(map[string]struct{}, len(supportedSchemas))
	for _, schema := range supportedSchemas {
		schema = strings.TrimSpace(schema)
		if schema == "" {
			return nil, errors.New("supported parser schemas must not contain empty values")
		}
		schemas[schema] = struct{}{}
	}
	if len(schemas) == 0 {
		return nil, errors.New("at least one parser schema must be configured")
	}
	return &SourceReader{source: source, pageSize: pageSize, supportedSchemas: schemas, retry: sourceRetryPolicy{attempts: 1}}, nil
}

func (r *SourceReader) Read(ctx context.Context, scope common.Scope, consumer ParserPageConsumer) (graph.SnapshotMetadata, error) {
	if consumer == nil {
		return graph.SnapshotMetadata{}, errors.New("parser page consumer must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return graph.SnapshotMetadata{}, err
	}
	metadata, err := r.Metadata(ctx, scope)
	if err != nil {
		return graph.SnapshotMetadata{}, err
	}
	if err := r.ReadPages(ctx, metadata, consumer); err != nil {
		return graph.SnapshotMetadata{}, err
	}
	return metadata, nil
}

func (r *SourceReader) Metadata(ctx context.Context, scope common.Scope) (graph.SnapshotMetadata, error) {
	metadata, err := retryValue(ctx, r.retry, func() (graph.SnapshotMetadata, error) {
		return r.source.SnapshotMetadata(ctx, scope)
	})
	if err != nil {
		return graph.SnapshotMetadata{}, sourceError("metadata", err)
	}
	if err := metadata.Validate(scope); err != nil {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultInvalid, "metadata", "parser", false, "parser metadata is invalid", err)
	}
	if _, ok := r.supportedSchemas[metadata.SchemaVersion]; !ok {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultInvalid, "metadata", "parser", false, "parser schema is not supported", nil)
	}
	return metadata, nil
}

func (r *SourceReader) ReadPages(ctx context.Context, metadata graph.SnapshotMetadata, consumer ParserPageConsumer) error {
	if consumer == nil {
		return errors.New("parser page consumer must not be nil")
	}
	if err := metadata.Validate(metadata.Scope); err != nil {
		return graphError(graph.ErrParserResultInvalid, "metadata", "parser", false, "parser metadata is invalid", err)
	}
	artifactCount, err := r.readArtifacts(ctx, metadata, consumer)
	if err != nil {
		return err
	}
	if artifactCount != metadata.ArtifactCount {
		return graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact count changed", nil)
	}
	relationCount, err := r.readRelations(ctx, metadata, consumer)
	if err != nil {
		return err
	}
	if relationCount != metadata.RelationCount {
		return graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation count changed", nil)
	}
	return nil
}

func (r *SourceReader) readArtifacts(ctx context.Context, metadata graph.SnapshotMetadata, consumer ParserPageConsumer) (int64, error) {
	var count int64
	cursor := ""
	lastID := ""
	for {
		page, err := retryValue(ctx, r.retry, func() (graph.ArtifactPage, error) {
			return r.source.ArtifactPage(ctx, metadata.Scope, cursor, r.pageSize)
		})
		if err != nil {
			return 0, sourceError("artifact_pages", err)
		}
		if err := page.PageIdentity.Validate(metadata, cursor); err != nil {
			return 0, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact page identity changed", err)
		}
		if len(page.Artifacts) > r.pageSize || count+int64(len(page.Artifacts)) > metadata.ArtifactCount {
			return 0, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact page exceeds declared bounds", nil)
		}
		for _, artifact := range page.Artifacts {
			if strings.TrimSpace(artifact.ArtifactID) == "" || artifact.SourceRef.CommitSHA != metadata.CommitSHA {
				return 0, graphError(graph.ErrParserResultInvalid, "artifact_pages", "parser", false, "parser artifact identity is invalid", nil)
			}
			if lastID != "" && artifact.ArtifactID <= lastID {
				return 0, graphError(graph.ErrParserResultInvalid, "artifact_pages", "parser", false, "parser artifact ids are not strictly ordered", nil)
			}
			lastID = artifact.ArtifactID
		}
		if err := consumer.ConsumeArtifactPage(ctx, metadata, page.Artifacts); err != nil {
			return 0, err
		}
		count += int64(len(page.Artifacts))
		if page.NextCursor == "" {
			return count, nil
		}
		if len(page.Artifacts) == 0 || page.NextCursor == cursor {
			return 0, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact cursor did not make progress", nil)
		}
		cursor = page.NextCursor
	}
}

func (r *SourceReader) readRelations(ctx context.Context, metadata graph.SnapshotMetadata, consumer ParserPageConsumer) (int64, error) {
	var count int64
	cursor := ""
	lastID := ""
	for {
		page, err := retryValue(ctx, r.retry, func() (graph.RelationPage, error) {
			return r.source.RelationPage(ctx, metadata.Scope, cursor, r.pageSize)
		})
		if err != nil {
			return 0, sourceError("relation_pages", err)
		}
		if err := page.PageIdentity.Validate(metadata, cursor); err != nil {
			return 0, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation page identity changed", err)
		}
		if len(page.Relations) > r.pageSize || count+int64(len(page.Relations)) > metadata.RelationCount {
			return 0, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation page exceeds declared bounds", nil)
		}
		for _, relation := range page.Relations {
			if strings.TrimSpace(relation.RelationID) == "" || strings.TrimSpace(relation.FromArtifactID) == "" || relation.Evidence.CommitSHA != metadata.CommitSHA {
				return 0, graphError(graph.ErrParserResultInvalid, "relation_pages", "parser", false, "parser relation identity is invalid", nil)
			}
			if lastID != "" && relation.RelationID <= lastID {
				return 0, graphError(graph.ErrParserResultInvalid, "relation_pages", "parser", false, "parser relation ids are not strictly ordered", nil)
			}
			lastID = relation.RelationID
		}
		if err := consumer.ConsumeRelationPage(ctx, metadata, page.Relations); err != nil {
			return 0, err
		}
		count += int64(len(page.Relations))
		if page.NextCursor == "" {
			return count, nil
		}
		if len(page.Relations) == 0 || page.NextCursor == cursor {
			return 0, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation cursor did not make progress", nil)
		}
		cursor = page.NextCursor
	}
}

func (r *SourceReader) withRetry(attempts int, initial, maximum time.Duration) *SourceReader {
	copy := *r
	copy.retry = sourceRetryPolicy{attempts: attempts, initial: initial, maximum: maximum}
	return &copy
}

func retryValue[T any](ctx context.Context, policy sourceRetryPolicy, operation func() (T, error)) (T, error) {
	var zero T
	if policy.attempts <= 0 {
		return zero, fmt.Errorf("source retry attempts must be positive")
	}
	delay := policy.initial
	for attempt := 1; attempt <= policy.attempts; attempt++ {
		value, err := operation()
		if err == nil {
			return value, nil
		}
		if attempt == policy.attempts || !retryableError(err) {
			return zero, err
		}
		if err := waitContext(ctx, delay); err != nil {
			return zero, err
		}
		delay *= 2
		if delay > policy.maximum {
			delay = policy.maximum
		}
	}
	return zero, fmt.Errorf("source retry exhausted")
}

func retryableError(err error) bool {
	var domainErr *graph.DomainError
	if errors.As(err, &domainErr) {
		return domainErr.Retryable
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sourceError(stage string, err error) error {
	if err == nil {
		return nil
	}
	var domainErr *graph.DomainError
	if errors.As(err, &domainErr) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return graphError(graph.ErrParserResultUnavailable, stage, "parser", true, "parser result is unavailable", err)
}

func graphError(code graph.ErrorCode, stage, dependency string, retryable bool, message string, cause error) *graph.DomainError {
	return &graph.DomainError{Code: code, Operation: stage, Stage: stage, Dependency: dependency, Retryable: retryable, Message: message, Cause: cause}
}
