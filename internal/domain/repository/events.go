package repository

import (
	"context"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

type eventScopeContextKey struct{}

func WithEventScope(ctx context.Context, scope common.Scope) context.Context {
	return context.WithValue(ctx, eventScopeContextKey{}, scope)
}

func EventScopeFromContext(ctx context.Context) (common.Scope, bool) {
	scope, ok := ctx.Value(eventScopeContextKey{}).(common.Scope)
	return scope, ok && scope.TenantID != "" && scope.RepositoryID != ""
}

type ParseCompletedPayload struct {
	SnapshotID    string   `json:"snapshot_id"`
	CommitSHA     string   `json:"commit_sha"`
	ArtifactCount int      `json:"artifact_count"`
	RelationCount int      `json:"relation_count"`
	DeletedPaths  []string `json:"deleted_paths"`
	SkippedCount  int      `json:"skipped_count"`
}

type ParseFailedPayload struct {
	SnapshotID string    `json:"snapshot_id"`
	ErrorCode  ErrorCode `json:"error_code"`
	Retryable  bool      `json:"retryable"`
}

func NewParseCompletedEvent(eventID string, scope common.Scope, occurredAt time.Time, payload ParseCompletedPayload) common.EventEnvelope {
	deleted := append([]string{}, payload.DeletedPaths...)
	return common.EventEnvelope{EventID: eventID, EventType: "parse.completed.v1", AggregateID: payload.SnapshotID,
		OccurredAt: occurredAt.UTC(), Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID,
		Payload: map[string]any{"snapshot_id": payload.SnapshotID, "commit_sha": payload.CommitSHA, "artifact_count": payload.ArtifactCount,
			"relation_count": payload.RelationCount, "deleted_paths": deleted, "skipped_count": payload.SkippedCount}}
}

func NewParseFailedEvent(eventID string, scope common.Scope, occurredAt time.Time, payload ParseFailedPayload) common.EventEnvelope {
	return common.EventEnvelope{EventID: eventID, EventType: "parse.failed.v1", AggregateID: payload.SnapshotID,
		OccurredAt: occurredAt.UTC(), Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID,
		Payload: map[string]any{"snapshot_id": payload.SnapshotID, "error_code": string(payload.ErrorCode), "retryable": payload.Retryable}}
}
