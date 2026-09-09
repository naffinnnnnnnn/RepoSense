package graphapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
)

type admissionVerificationStore struct {
	job      graph.BuildJob
	err      error
	enqueues int
	rejected []graph.RejectedEvent
}

func (s *admissionVerificationStore) EnqueueGraphBuildWithinQuota(_ context.Context, requested graph.BuildJob, _, _ int) (graph.BuildJob, bool, error) {
	s.enqueues++
	if s.err != nil {
		return graph.BuildJob{}, false, s.err
	}
	s.job = requested
	return requested, true, nil
}
func (s *admissionVerificationStore) RecordRejectedGraphEvent(_ context.Context, rejected graph.RejectedEvent) (bool, error) {
	s.rejected = append(s.rejected, rejected)
	return true, nil
}

func TestConsumerPersistsBeforeAckRejectsPoisonAndRetriesTransientControlFailure(t *testing.T) {
	now := time.Date(2026, 9, 9, 4, 5, 6, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	valid := common.EventEnvelope{EventID: "parse-event", EventType: "parse.completed.v1", AggregateID: scope.SnapshotID, OccurredAt: now,
		Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID, Payload: map[string]any{"snapshot_id": scope.SnapshotID, "commit_sha": "commit", "parser_result_version": "parser-v1", "artifact_count": 1, "relation_count": 0}}
	config := ConsumerConfig{GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1", MaxEventBytes: 64 * 1024, MaxPendingGlobal: 10, MaxPendingPerTenant: 5, StoreTimeout: time.Second}
	store := &admissionVerificationStore{}
	consumer, err := NewConsumer(store, store, nil, &verificationIDs{}, verificationClock{now}, config)
	if err != nil {
		t.Fatal(err)
	}
	result, err := consumer.Handle(context.Background(), scope, valid)
	if err != nil || result.Disposition != ConsumerAck || !result.Created || store.enqueues != 1 || result.Job.TriggerEventID != valid.EventID {
		t.Fatalf("valid event was not durably accepted: result=%#v err=%v store=%#v", result, err, store)
	}
	poison := valid
	poison.EventID = "bad-event"
	poison.Payload = map[string]any{"snapshot_id": scope.SnapshotID}
	result, err = consumer.Handle(context.Background(), scope, poison)
	if err != nil || result.Disposition != ConsumerAck || !result.Rejected || len(store.rejected) != 1 || store.rejected[0].PayloadDigest == "" {
		t.Fatalf("poison event was not durably rejected and ACKed: result=%#v err=%v rejected=%#v", result, err, store.rejected)
	}
	store.err = &graph.DomainError{Code: graph.ErrControlStoreUnavailable, Retryable: true, Message: "postgres unavailable"}
	valid.EventID = "retry-event"
	result, err = consumer.Handle(context.Background(), scope, valid)
	if !graph.IsCode(err, graph.ErrControlStoreUnavailable) || result.Disposition != ConsumerRetry || result.Rejected {
		t.Fatalf("transient control failure must NAK/retry: result=%#v err=%v", result, err)
	}
}

type outboxVerificationStore struct {
	records   []graph.OutboxRecord
	published []string
	failed    []string
	dead      []bool
	markErr   error
}

func (s *outboxVerificationStore) PendingGraphEvents(context.Context, int, time.Time) ([]graph.OutboxRecord, error) {
	return append([]graph.OutboxRecord(nil), s.records...), nil
}
func (s *outboxVerificationStore) ClaimGraphEvents(context.Context, int, time.Time, time.Time) ([]graph.OutboxRecord, error) {
	return append([]graph.OutboxRecord(nil), s.records...), nil
}
func (s *outboxVerificationStore) MarkGraphEventPublished(_ context.Context, eventID string, _ time.Time) error {
	s.published = append(s.published, eventID)
	return s.markErr
}
func (s *outboxVerificationStore) MarkGraphEventFailed(_ context.Context, eventID, _ string, _ time.Time, dead bool) error {
	s.failed, s.dead = append(s.failed, eventID), append(s.dead, dead)
	return nil
}

type publisherVerification struct{ fail map[string]error }

func (p publisherVerification) Publish(_ context.Context, event common.EventEnvelope) error {
	return p.fail[event.EventID]
}

func TestOutboxKeepsStableEventIdentityAcrossSuccessFailureAndDeadLetter(t *testing.T) {
	now := time.Date(2026, 9, 9, 4, 5, 6, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	event := func(id string) common.EventEnvelope {
		return common.EventEnvelope{EventID: id, EventType: "graph.published.v1", AggregateID: "revision", OccurredAt: now, Producer: "code-knowledge-graph", PayloadVersion: 1, TraceID: "trace", Payload: map[string]any{"revision_id": "revision"}}
	}
	store := &outboxVerificationStore{records: []graph.OutboxRecord{{Event: event("ok"), Scope: scope, RevisionID: "revision"}, {Event: event("dead"), Scope: scope, RevisionID: "revision", DeliveryCount: 1}}}
	dispatcher, err := NewGraphOutboxDispatcher(store, publisherVerification{fail: map[string]error{"dead": errors.New("nats unavailable")}}, nil, verificationClock{now}, GraphOutboxConfig{BatchSize: 2, MaxAttempts: 2, ClaimDuration: 5 * time.Second, PublishTimeout: time.Second, StoreTimeout: time.Second, BaseBackoff: time.Second, MaxBackoff: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	published, err := dispatcher.DispatchOnce(context.Background())
	if published != 1 || !graph.IsCode(err, graph.ErrEventPublishFailed) || strings.Join(store.published, ",") != "ok" || strings.Join(store.failed, ",") != "dead" || len(store.dead) != 1 || !store.dead[0] {
		t.Fatalf("outbox outcome mismatch: published=%d err=%v store=%#v", published, err, store)
	}
}

type reconciliationVerificationStore struct {
	job       graph.BuildJob
	revision  graph.Revision
	expired   []graph.BuildAttempt
	orphans   []graph.BuildAttempt
	refs      []graph.ActiveRevisionRef
	lost      []string
	audits    []graph.ReconciliationRun
	deleted   []string
	verifyErr error
}

func (s *reconciliationVerificationStore) GraphJob(context.Context, string) (graph.BuildJob, error) {
	return s.job, nil
}
func (s *reconciliationVerificationStore) ActiveGraphRevision(context.Context, common.Scope) (graph.Revision, error) {
	return s.revision, nil
}
func (s *reconciliationVerificationStore) ExpiredGraphAttempts(context.Context, time.Time, int) ([]graph.BuildAttempt, error) {
	return s.expired, nil
}
func (s *reconciliationVerificationStore) MarkGraphAttemptLost(_ context.Context, _, attemptID string, _ int64, _ time.Time) error {
	s.lost = append(s.lost, attemptID)
	return nil
}
func (s *reconciliationVerificationStore) UnreferencedGraphCandidates(context.Context, time.Time, time.Time, int) ([]graph.BuildAttempt, error) {
	return s.orphans, nil
}
func (s *reconciliationVerificationStore) ActiveGraphRevisionRefs(context.Context, string, int) ([]graph.ActiveRevisionRef, error) {
	return s.refs, nil
}
func (s *reconciliationVerificationStore) RecordGraphReconciliationRun(_ context.Context, run graph.ReconciliationRun) error {
	s.audits = append(s.audits, run)
	return nil
}
func (*reconciliationVerificationStore) CandidateStatus(context.Context, common.Scope, string) (graph.CandidateStatus, error) {
	return graph.CandidateStaging, nil
}
func (s *reconciliationVerificationStore) VerifyRevision(context.Context, graph.Revision) error {
	return s.verifyErr
}
func (s *reconciliationVerificationStore) DeleteCandidate(_ context.Context, _ common.Scope, revisionID string, _ int64) error {
	s.deleted = append(s.deleted, revisionID)
	return nil
}

func TestReconcilerRepairsExpiredAndOrphanedWorkAndSurfacesActiveLoss(t *testing.T) {
	now := time.Date(2026, 9, 9, 4, 5, 6, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	job := verificationJob(now, scope)
	revision := graph.Revision{EntityMeta: graph.NewMeta("active", scope, graph.RevisionActive, now), RevisionID: "active", SnapshotID: scope.SnapshotID, CommitSHA: "commit", BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive, AlgorithmVersion: "algorithm-v1", ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", BuildPolicyVersion: "policy-v1", QualityStatus: graph.QualityHealthy}
	store := &reconciliationVerificationStore{job: job, revision: revision,
		expired:   []graph.BuildAttempt{{AttemptID: "expired", JobID: job.JobID, RevisionID: "expired-revision", Fence: 1}},
		orphans:   []graph.BuildAttempt{{AttemptID: "orphan", JobID: job.JobID, RevisionID: "orphan-revision", Fence: 2}},
		refs:      []graph.ActiveRevisionRef{{Scope: scope, RevisionID: revision.RevisionID}},
		verifyErr: &graph.DomainError{Code: graph.ErrRevisionNotFound, Message: "missing"}}
	reconciler, err := NewReconciler(store, store, store, nil, &verificationIDs{}, verificationClock{now}, ReconcilerConfig{BatchSize: 10, OrphanRetention: time.Hour, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.RunOnce(context.Background())
	if report.ExpiredAttempts != 1 || report.DeletedOrphans != 1 || report.Inconsistent != 1 || !graph.IsCode(err, graph.ErrGraphInconsistent) {
		t.Fatalf("reconciliation report=%#v err=%v", report, err)
	}
	if strings.Join(store.lost, ",") != "expired" || strings.Join(store.deleted, ",") != "orphan-revision" || len(store.audits) != 3 {
		t.Fatalf("reconciliation side effects mismatch: %#v", store)
	}
}

type queryVerificationStore struct {
	revision graph.Revision
	result   graph.Result
	verified int
}

func (s *queryVerificationStore) ActiveGraphRevision(context.Context, common.Scope) (graph.Revision, error) {
	return s.revision, nil
}
func (s *queryVerificationStore) VerifyRevision(context.Context, graph.Revision) error {
	s.verified++
	return nil
}
func (s *queryVerificationStore) QueryRevision(context.Context, string, graph.Query) (graph.Result, error) {
	return s.result, nil
}
func (s *queryVerificationStore) QueryRevisionDiagnostics(context.Context, string, graph.DiagnosticQuery) (graph.DiagnosticResult, error) {
	return graph.DiagnosticResult{RevisionID: s.revision.RevisionID, Issues: []graph.ResolutionIssue{}}, nil
}

func TestQueryServicePinsEveryReadToTheExactActiveRevision(t *testing.T) {
	now := time.Date(2026, 9, 9, 4, 5, 6, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	revision := graph.Revision{EntityMeta: graph.NewMeta("revision", scope, graph.RevisionActive, now), RevisionID: "revision", SnapshotID: scope.SnapshotID, CommitSHA: "commit", BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive, AlgorithmVersion: "algorithm-v1", ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", BuildPolicyVersion: "policy-v1", QualityStatus: graph.QualityHealthy}
	store := &queryVerificationStore{revision: revision, result: graph.Result{Nodes: []graph.Entity{}, Edges: []graph.Relation{}, Diagnostics: graph.Diagnostics{RevisionID: revision.RevisionID}}}
	service, err := NewQueryService(store, store, nil, QueryServiceConfig{Timeout: time.Second, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(context.Background(), graph.Query{Scope: scope, RootIDs: []string{}, Direction: graph.DirectionBoth, Depth: 0, Limit: 10}); err != nil || store.verified != 1 {
		t.Fatalf("exact active revision was not verified: %v", err)
	}
	store.result.Diagnostics.RevisionID = "other"
	if _, err := service.Query(context.Background(), graph.Query{Scope: scope, Depth: 0, Limit: 10}); !graph.IsCode(err, graph.ErrGraphInconsistent) {
		t.Fatalf("mismatched neo4j result must fail closed: %v", err)
	}
}
