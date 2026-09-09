package graphapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type ConsumerDisposition string

const (
	ConsumerAck   ConsumerDisposition = "ACK"
	ConsumerRetry ConsumerDisposition = "RETRY"
)

type ConsumerResult struct {
	Disposition ConsumerDisposition
	Job         graph.BuildJob
	Created     bool
	Rejected    bool
}

type ConsumerConfig struct {
	GraphSchemaVersion    string
	GraphAlgorithmVersion string
	BuildPolicyVersion    string
	MaxEventBytes         int
	MaxPendingGlobal      int
	MaxPendingPerTenant   int
	StoreTimeout          time.Duration
}

func (c ConsumerConfig) Validate() error {
	if !exactIdentity(c.GraphSchemaVersion) || !exactIdentity(c.GraphAlgorithmVersion) || !exactIdentity(c.BuildPolicyVersion) {
		return fmt.Errorf("graph schema, algorithm and build policy versions are required exact identities")
	}
	if c.MaxEventBytes <= 0 || c.MaxEventBytes > 16*1024*1024 || c.MaxPendingGlobal <= 0 || c.MaxPendingGlobal > 1_000_000 || c.MaxPendingPerTenant <= 0 || c.MaxPendingPerTenant > c.MaxPendingGlobal || c.StoreTimeout <= 0 || c.StoreTimeout > time.Minute {
		return fmt.Errorf("event size and ordered pending quotas must be positive")
	}
	return nil
}

type Consumer struct {
	admission ports.GraphBuildAdmissionStore
	rejected  ports.GraphRejectedEventStore
	observer  ports.Observer
	ids       ports.IDGenerator
	clock     ports.Clock
	config    ConsumerConfig
}

func NewConsumer(admission ports.GraphBuildAdmissionStore, rejected ports.GraphRejectedEventStore, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config ConsumerConfig) (*Consumer, error) {
	if admission == nil || rejected == nil {
		return nil, fmt.Errorf("graph admission and rejected-event stores are required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = randomIDs{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Consumer{admission: admission, rejected: rejected, observer: observer, ids: ids, clock: clock, config: config}, nil
}

type parseCompletedPayload struct {
	SnapshotID          string   `json:"snapshot_id"`
	CommitSHA           string   `json:"commit_sha"`
	ParserResultVersion string   `json:"parser_result_version"`
	ArtifactCount       int64    `json:"artifact_count"`
	RelationCount       int64    `json:"relation_count"`
	DeletedPaths        []string `json:"deleted_paths,omitempty"`
	SkippedCount        int64    `json:"skipped_count,omitempty"`
}

type parseFailedPayload struct {
	SnapshotID string `json:"snapshot_id"`
	ErrorCode  string `json:"error_code"`
	Retryable  bool   `json:"retryable"`
}

func (c *Consumer) Handle(ctx context.Context, trustedScope common.Scope, event common.EventEnvelope) (result ConsumerResult, err error) {
	result.Disposition = ConsumerRetry
	ctx, finish := startGraphStage(c.observer, ctx, "graph_consumer", map[string]string{
		"operation": "consume", "tenant_id": trustedScope.TenantID, "repository_id": trustedScope.RepositoryID,
		"snapshot_id": trustedScope.SnapshotID, "trace_id": event.TraceID,
	})
	defer func() { finish(err) }()

	encoded, encodeErr := json.Marshal(event)
	if encodeErr != nil {
		return c.reject(ctx, trustedScope, event, nil, "event cannot be encoded")
	}
	if len(encoded) > c.config.MaxEventBytes {
		return c.reject(ctx, trustedScope, event, encoded, "event exceeds graph consumer capacity")
	}
	if event.EventType == "parse.failed.v1" {
		if validationErr := validateEventEnvelope(trustedScope, event, true); validationErr != nil {
			return c.reject(ctx, trustedScope, event, encoded, validationErr.Error())
		}
		payload, validationErr := decodeParseFailed(event)
		if validationErr != nil {
			return c.reject(ctx, trustedScope, event, encoded, validationErr.Error())
		}
		if payload.SnapshotID != trustedScope.SnapshotID || event.AggregateID != payload.SnapshotID {
			return c.reject(ctx, trustedScope, event, encoded, "event snapshot identity does not match trusted scope")
		}
		c.observer.Count("graph_consumer_events_total", 1, map[string]string{"status": "ignored"})
		return ConsumerResult{Disposition: ConsumerAck}, nil
	}
	if event.EventType != "parse.completed.v1" {
		return c.reject(ctx, trustedScope, event, encoded, "unsupported graph trigger event type")
	}
	if validationErr := validateEventEnvelope(trustedScope, event, true); validationErr != nil {
		return c.reject(ctx, trustedScope, event, encoded, validationErr.Error())
	}
	payload, validationErr := decodeParseCompleted(event)
	if validationErr != nil {
		return c.reject(ctx, trustedScope, event, encoded, validationErr.Error())
	}
	if payload.SnapshotID != trustedScope.SnapshotID || event.AggregateID != payload.SnapshotID {
		return c.reject(ctx, trustedScope, event, encoded, "event snapshot identity does not match trusted scope")
	}
	buildScope := trustedScope
	buildScope.TraceID = event.TraceID
	versions := graph.BuildVersions{ParserResultVersion: payload.ParserResultVersion, GraphSchemaVersion: c.config.GraphSchemaVersion, GraphAlgorithmVersion: c.config.GraphAlgorithmVersion, BuildPolicyVersion: c.config.BuildPolicyVersion}
	fingerprint, fingerprintErr := graph.RequestFingerprint(graph.FingerprintInput{Scope: buildScope, CommitSHA: payload.CommitSHA, Versions: versions})
	if fingerprintErr != nil {
		return c.reject(ctx, trustedScope, event, encoded, "event cannot produce a graph build fingerprint")
	}
	now := c.clock.Now().UTC()
	job := graph.BuildJob{JobID: c.ids.New("gj"), Scope: buildScope, IdempotencyKey: "parse-event:" + event.EventID, TriggerEventID: event.EventID,
		RequestFingerprint: fingerprint, CommitSHA: payload.CommitSHA, Versions: versions, Status: graph.JobPending,
		EventID: c.ids.New("evt"), CreatedAt: now, UpdatedAt: now}
	if !exactIdentity(job.JobID) || !exactIdentity(job.EventID) {
		return result, workerError(graph.ErrBuildFailure, "consumer_identity", true, "graph consumer could not allocate job identities", nil)
	}
	storeCtx, cancelStore := context.WithTimeout(ctx, c.config.StoreTimeout)
	storeCtx, finishEnqueue := startGraphStage(c.observer, storeCtx, "graph_control_enqueue", map[string]string{"operation": "enqueue", "dependency": "postgresql"})
	stored, created, enqueueErr := c.admission.EnqueueGraphBuildWithinQuota(storeCtx, job, c.config.MaxPendingGlobal, c.config.MaxPendingPerTenant)
	cancelStore()
	finishEnqueue(enqueueErr)
	if enqueueErr != nil {
		if retryableError(enqueueErr) || errors.Is(enqueueErr, context.Canceled) || errors.Is(enqueueErr, context.DeadlineExceeded) {
			c.observer.Count("graph_consumer_events_total", 1, map[string]string{"status": "retry"})
			return result, enqueueErr
		}
		return c.reject(ctx, trustedScope, event, encoded, "graph build event conflicts with persisted identity")
	}
	c.observer.Count("graph_consumer_events_total", 1, map[string]string{"status": "accepted"})
	return ConsumerResult{Disposition: ConsumerAck, Job: stored, Created: created}, nil
}

func validateEventEnvelope(scope common.Scope, event common.EventEnvelope, requireSnapshot bool) error {
	if err := scope.Validate(requireSnapshot); err != nil {
		return fmt.Errorf("trusted event scope is invalid")
	}
	if !exactIdentity(scope.TenantID) || !exactIdentity(scope.RepositoryID) || requireSnapshot && !exactIdentity(scope.SnapshotID) {
		return fmt.Errorf("trusted event scope identities are invalid")
	}
	if !exactIdentity(event.EventID) || !exactIdentity(event.EventType) || !exactIdentity(event.AggregateID) || event.OccurredAt.IsZero() || event.Producer != "repository-parser" || event.PayloadVersion != 1 || !exactIdentity(event.TraceID) || event.Payload == nil {
		return fmt.Errorf("event envelope is invalid")
	}
	if scope.TraceID != "" && scope.TraceID != event.TraceID {
		return fmt.Errorf("event trace identity does not match trusted scope")
	}
	return nil
}

func decodeParseCompleted(event common.EventEnvelope) (parseCompletedPayload, error) {
	for _, required := range []string{"snapshot_id", "commit_sha", "parser_result_version", "artifact_count", "relation_count"} {
		if _, exists := event.Payload[required]; !exists {
			return parseCompletedPayload{}, fmt.Errorf("parse.completed payload is missing a required field")
		}
	}
	encoded, err := json.Marshal(event.Payload)
	if err != nil {
		return parseCompletedPayload{}, fmt.Errorf("parse.completed payload cannot be encoded")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var payload parseCompletedPayload
	if err := decoder.Decode(&payload); err != nil {
		return parseCompletedPayload{}, fmt.Errorf("parse.completed payload is invalid")
	}
	if !exactIdentity(payload.SnapshotID) || !exactIdentity(payload.CommitSHA) || !exactIdentity(payload.ParserResultVersion) || payload.ArtifactCount < 0 || payload.RelationCount < 0 || payload.SkippedCount < 0 {
		return parseCompletedPayload{}, fmt.Errorf("parse.completed payload identity or counts are invalid")
	}
	return payload, nil
}

func decodeParseFailed(event common.EventEnvelope) (parseFailedPayload, error) {
	for _, required := range []string{"snapshot_id", "error_code", "retryable"} {
		if _, exists := event.Payload[required]; !exists {
			return parseFailedPayload{}, fmt.Errorf("parse.failed payload is missing a required field")
		}
	}
	encoded, err := json.Marshal(event.Payload)
	if err != nil {
		return parseFailedPayload{}, fmt.Errorf("parse.failed payload cannot be encoded")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var payload parseFailedPayload
	if err := decoder.Decode(&payload); err != nil {
		return parseFailedPayload{}, fmt.Errorf("parse.failed payload is invalid")
	}
	if !exactIdentity(payload.SnapshotID) || !exactIdentity(payload.ErrorCode) {
		return parseFailedPayload{}, fmt.Errorf("parse.failed payload identity is invalid")
	}
	return payload, nil
}

func (c *Consumer) reject(ctx context.Context, scope common.Scope, event common.EventEnvelope, encoded []byte, message string) (ConsumerResult, error) {
	if encoded == nil {
		encoded, _ = json.Marshal(event)
		if encoded == nil {
			encoded = []byte(fmt.Sprintf("%#v", event))
		}
	}
	digest := sha256.Sum256(encoded)
	digestText := hex.EncodeToString(digest[:])
	eventID := event.EventID
	if !exactIdentity(eventID) {
		eventID = "invalid_" + digestText
	}
	eventType := event.EventType
	if !exactIdentity(eventType) {
		eventType = "unknown"
	}
	rejected := graph.RejectedEvent{EventID: eventID, EventType: eventType, ErrorCode: graph.ErrInvalidInput,
		ErrorMessage: message, PayloadDigest: digestText, RejectedAt: c.clock.Now().UTC()}
	if exactIdentity(scope.TenantID) {
		rejected.TenantID = scope.TenantID
	}
	if exactIdentity(scope.RepositoryID) {
		rejected.RepositoryID = scope.RepositoryID
	}
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.StoreTimeout)
	defer cancel()
	storeCtx, finishReject := startGraphStage(c.observer, storeCtx, "graph_control_reject", map[string]string{"operation": "reject", "dependency": "postgresql"})
	_, storeErr := c.rejected.RecordRejectedGraphEvent(storeCtx, rejected)
	finishReject(storeErr)
	if storeErr != nil {
		c.observer.Count("graph_consumer_events_total", 1, map[string]string{"status": "retry"})
		return ConsumerResult{Disposition: ConsumerRetry}, storeErr
	}
	c.observer.Count("graph_consumer_events_total", 1, map[string]string{"status": "rejected"})
	return ConsumerResult{Disposition: ConsumerAck, Rejected: true}, nil
}

func exactIdentity(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}
