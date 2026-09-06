package graphapp

import (
	"context"
	"errors"
	"strings"

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
	return &SourceReader{source: source, pageSize: pageSize, supportedSchemas: schemas}, nil
}

func (r *SourceReader) Read(ctx context.Context, scope common.Scope, consumer ParserPageConsumer) (graph.SnapshotMetadata, error) {
	if consumer == nil {
		return graph.SnapshotMetadata{}, errors.New("parser page consumer must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return graph.SnapshotMetadata{}, err
	}
	metadata, err := r.source.SnapshotMetadata(ctx, scope)
	if err != nil {
		return graph.SnapshotMetadata{}, sourceError("metadata", err)
	}
	if err := metadata.Validate(scope); err != nil {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultInvalid, "metadata", "parser", false, "parser metadata is invalid", err)
	}
	if _, ok := r.supportedSchemas[metadata.SchemaVersion]; !ok {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultInvalid, "metadata", "parser", false, "parser schema is not supported", nil)
	}

	artifactCount, err := r.readArtifacts(ctx, metadata, consumer)
	if err != nil {
		return graph.SnapshotMetadata{}, err
	}
	if artifactCount != metadata.ArtifactCount {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact count changed", nil)
	}
	relationCount, err := r.readRelations(ctx, metadata, consumer)
	if err != nil {
		return graph.SnapshotMetadata{}, err
	}
	if relationCount != metadata.RelationCount {
		return graph.SnapshotMetadata{}, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation count changed", nil)
	}
	return metadata, nil
}

func (r *SourceReader) readArtifacts(ctx context.Context, metadata graph.SnapshotMetadata, consumer ParserPageConsumer) (int64, error) {
	var count int64
	cursor := ""
	seen := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	for {
		if _, exists := seen[cursor]; exists {
			return 0, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact cursor repeated", nil)
		}
		seen[cursor] = struct{}{}
		page, err := r.source.ArtifactPage(ctx, metadata.Scope, cursor, r.pageSize)
		if err != nil {
			return 0, sourceError("artifact_pages", err)
		}
		if err := page.PageIdentity.Validate(metadata, cursor); err != nil {
			return 0, graphError(graph.ErrParserResultChanged, "artifact_pages", "parser", false, "parser artifact page identity changed", err)
		}
		for _, artifact := range page.Artifacts {
			if strings.TrimSpace(artifact.ArtifactID) == "" || artifact.SourceRef.CommitSHA != metadata.CommitSHA {
				return 0, graphError(graph.ErrParserResultInvalid, "artifact_pages", "parser", false, "parser artifact identity is invalid", nil)
			}
			if _, duplicate := seenIDs[artifact.ArtifactID]; duplicate {
				return 0, graphError(graph.ErrParserResultInvalid, "artifact_pages", "parser", false, "parser artifact id is duplicated across pages", nil)
			}
			seenIDs[artifact.ArtifactID] = struct{}{}
		}
		if err := consumer.ConsumeArtifactPage(ctx, metadata, page.Artifacts); err != nil {
			return 0, err
		}
		count += int64(len(page.Artifacts))
		if page.NextCursor == "" {
			return count, nil
		}
		cursor = page.NextCursor
	}
}

func (r *SourceReader) readRelations(ctx context.Context, metadata graph.SnapshotMetadata, consumer ParserPageConsumer) (int64, error) {
	var count int64
	cursor := ""
	seen := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	for {
		if _, exists := seen[cursor]; exists {
			return 0, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation cursor repeated", nil)
		}
		seen[cursor] = struct{}{}
		page, err := r.source.RelationPage(ctx, metadata.Scope, cursor, r.pageSize)
		if err != nil {
			return 0, sourceError("relation_pages", err)
		}
		if err := page.PageIdentity.Validate(metadata, cursor); err != nil {
			return 0, graphError(graph.ErrParserResultChanged, "relation_pages", "parser", false, "parser relation page identity changed", err)
		}
		for _, relation := range page.Relations {
			if err := relation.Validate(); err != nil || relation.Evidence.CommitSHA != metadata.CommitSHA {
				return 0, graphError(graph.ErrParserResultInvalid, "relation_pages", "parser", false, "parser relation is invalid", err)
			}
			if _, duplicate := seenIDs[relation.RelationID]; duplicate {
				return 0, graphError(graph.ErrParserResultInvalid, "relation_pages", "parser", false, "parser relation id is duplicated across pages", nil)
			}
			seenIDs[relation.RelationID] = struct{}{}
		}
		if err := consumer.ConsumeRelationPage(ctx, metadata, page.Relations); err != nil {
			return 0, err
		}
		count += int64(len(page.Relations))
		if page.NextCursor == "" {
			return count, nil
		}
		cursor = page.NextCursor
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
