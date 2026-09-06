package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
)

func TestGraphControlStorePostgresLifecycle(t *testing.T) {
	store, pool, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	first := graphControlTestJob("job-1", "key-1", strings.Repeat("a", 64), "event-1", now)
	stored, created, err := store.EnqueueGraphBuild(ctx, first)
	if err != nil || !created || stored.JobID != first.JobID {
		t.Fatalf("enqueue first job: created=%v job=%#v err=%v", created, stored, err)
	}
	eventConflict := graphControlTestJob("job-event-conflict", "key-event-conflict", strings.Repeat("d", 64), first.EventID, now)
	if _, _, err := store.EnqueueGraphBuild(ctx, eventConflict); !graph.IsCode(err, graph.ErrIdempotencyConflict) {
		t.Fatalf("same EventID with a different build must conflict: %v", err)
	}

	duplicate := graphControlTestJob("job-duplicate", "key-2", first.RequestFingerprint, "event-duplicate", now)
	stored, created, err = store.EnqueueGraphBuild(ctx, duplicate)
	if err != nil || created || stored.JobID != first.JobID {
		t.Fatalf("fingerprint must converge to original job: created=%v job=%#v err=%v", created, stored, err)
	}

	conflict := graphControlTestJob("job-conflict", first.IdempotencyKey, strings.Repeat("b", 64), "event-conflict", now)
	if _, _, err := store.EnqueueGraphBuild(ctx, conflict); !graph.IsCode(err, graph.ErrIdempotencyConflict) {
		t.Fatalf("same key with a different fingerprint must conflict: %v", err)
	}
	var jobCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM graph_build_jobs`).Scan(&jobCount); err != nil || jobCount != 1 {
		t.Fatalf("idempotency conflict left an orphan job: count=%d err=%v", jobCount, err)
	}

	job, attempt, found, err := store.ClaimGraphBuild(ctx, "worker-1", "attempt-1", "revision-1", now, time.Minute)
	if err != nil || !found || job.JobID != first.JobID || attempt.Fence != 1 {
		t.Fatalf("claim first attempt: found=%v job=%#v attempt=%#v err=%v", found, job, attempt, err)
	}
	if _, _, found, err := store.ClaimGraphBuild(ctx, "worker-2", "attempt-blocked", "revision-blocked", now.Add(time.Second), time.Minute); err != nil || found {
		t.Fatalf("valid lease must block a second claim: found=%v err=%v", found, err)
	}
	if err := store.HeartbeatGraphBuild(ctx, job.JobID, attempt.AttemptID, attempt.LeaseOwner, attempt.Fence, time.Now().UTC().Add(2*time.Minute)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	revision := graphControlTestRevision(job, attempt, now)
	event := graphControlTestEvent(job, revision, now)
	if err := store.ActivateGraphRevision(ctx, job, attempt, revision, event, now.Add(2*time.Second)); err != nil {
		t.Fatalf("activate graph revision: %v", err)
	}
	if err := store.ActivateGraphRevision(ctx, job, attempt, revision, event, now.Add(3*time.Second)); err != nil {
		t.Fatalf("activation retry must read back the committed outcome: %v", err)
	}
	active, err := store.ActiveGraphRevision(ctx, job.Scope)
	if err != nil || active.RevisionID != revision.RevisionID || active.BuildStatus != graph.RevisionActive {
		t.Fatalf("active revision: %#v err=%v", active, err)
	}
	events, err := store.PendingGraphEvents(ctx, 10, now.Add(time.Minute))
	if err != nil || len(events) != 1 || events[0].Event.EventID != event.EventID || events[0].RevisionID != revision.RevisionID {
		t.Fatalf("pending outbox: %#v err=%v", events, err)
	}
	if err := store.MarkGraphEventFailed(ctx, event.EventID, "nats unavailable", now.Add(2*time.Minute), false); err != nil {
		t.Fatalf("mark outbox retry: %v", err)
	}
	if events, err = store.PendingGraphEvents(ctx, 10, now.Add(time.Minute)); err != nil || len(events) != 0 {
		t.Fatalf("backoff must hide event: %#v err=%v", events, err)
	}
	if err := store.MarkGraphEventPublished(ctx, event.EventID, now.Add(3*time.Minute)); err != nil {
		t.Fatalf("mark outbox published: %v", err)
	}
	if events, err = store.PendingGraphEvents(ctx, 10, now.Add(4*time.Minute)); err != nil || len(events) != 0 {
		t.Fatalf("published event must not be pending: %#v err=%v", events, err)
	}
	rejected := graph.RejectedEvent{EventID: "bad-event", EventType: "parse.completed.v1", ErrorCode: graph.ErrInvalidInput, ErrorMessage: "invalid payload", RejectedAt: now}
	if created, err := store.RecordRejectedGraphEvent(ctx, rejected); err != nil || !created {
		t.Fatalf("record rejected event: created=%v err=%v", created, err)
	}
	if created, err := store.RecordRejectedGraphEvent(ctx, rejected); err != nil || created {
		t.Fatalf("rejected EventID must be idempotent: created=%v err=%v", created, err)
	}
	run := graph.ReconciliationRun{RunID: "run-1", Action: "MARK_LOST", TargetType: "ATTEMPT", TargetID: attempt.AttemptID, Result: "NOOP", StartedAt: now, CompletedAt: now.Add(time.Second)}
	if err := store.RecordGraphReconciliationRun(ctx, run); err != nil {
		t.Fatalf("record reconciliation run: %v", err)
	}
}

func TestGraphControlStorePostgresLeaseRecoveryAndFencing(t *testing.T) {
	store, _, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	requested := graphControlTestJob("job-lease", "key-lease", strings.Repeat("c", 64), "event-lease", now)
	if _, _, err := store.EnqueueGraphBuild(ctx, requested); err != nil {
		t.Fatal(err)
	}
	job1, attempt1, found, err := store.ClaimGraphBuild(ctx, "worker-old", "attempt-old", "revision-old", now, time.Second)
	if err != nil || !found {
		t.Fatalf("claim old attempt: found=%v err=%v", found, err)
	}
	expiredAt := now.Add(2 * time.Second)
	expired, err := store.ExpiredGraphAttempts(ctx, expiredAt, 10)
	if err != nil || len(expired) != 1 || expired[0].AttemptID != attempt1.AttemptID {
		t.Fatalf("expired attempts: %#v err=%v", expired, err)
	}
	job2, attempt2, found, err := store.ClaimGraphBuild(ctx, "worker-new", "attempt-new", "revision-new", expiredAt, time.Minute)
	if err != nil || !found || attempt2.Fence <= attempt1.Fence || job2.RevisionID != attempt2.RevisionID {
		t.Fatalf("reclaim with fencing: found=%v job=%#v attempt=%#v err=%v", found, job2, attempt2, err)
	}
	oldRevision := graphControlTestRevision(job1, attempt1, now)
	oldEvent := graphControlTestEvent(job1, oldRevision, now)
	if err := store.ActivateGraphRevision(ctx, job1, attempt1, oldRevision, oldEvent, expiredAt); !graph.IsCode(err, graph.ErrLeaseLost) {
		t.Fatalf("stale worker must be fenced from activation: %v", err)
	}
	failure := graph.DomainError{Code: graph.ErrGraphBatchWriteFailed, Message: "batch failed", Retryable: true}
	if err := store.FailGraphAttempt(ctx, job2, attempt2, failure, expiredAt.Add(time.Second)); err != nil {
		t.Fatalf("fail current attempt: %v", err)
	}
	orphans, err := store.UnreferencedGraphCandidates(ctx, expiredAt.Add(2*time.Second), 10)
	if err != nil || len(orphans) != 2 {
		t.Fatalf("failed and lost candidates should be reconcilable: %#v err=%v", orphans, err)
	}
}

func TestGraphControlMigrationDown(t *testing.T) {
	_, pool, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", "000002_code_knowledge_graph.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("graph migration rollback: %v", err)
	}
	var table *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('graph_build_jobs')::text`).Scan(&table); err != nil {
		t.Fatal(err)
	}
	if table != nil {
		t.Fatalf("down migration left graph_build_jobs: %s", *table)
	}
}

func graphControlTestStore(t *testing.T) (*GraphControlStore, *pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("REPOSENSE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("REPOSENSE_TEST_POSTGRES_DSN is not configured")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("graph_control_test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", "000002_code_knowledge_graph.up.sql"))
	if err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(up)); err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	}
	return NewGraphControlStoreWithPool(pool), pool, cleanup
}

func graphControlTestJob(jobID, key, fingerprint, eventID string, now time.Time) graph.BuildJob {
	return graph.BuildJob{
		JobID:          jobID,
		Scope:          common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"},
		IdempotencyKey: key, RequestFingerprint: fingerprint, CommitSHA: "commit",
		Versions: graph.BuildVersions{ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1"},
		Status:   graph.JobPending, EventID: eventID, CreatedAt: now, UpdatedAt: now,
	}
}

func graphControlTestRevision(job graph.BuildJob, attempt graph.BuildAttempt, now time.Time) graph.Revision {
	return graph.Revision{
		EntityMeta: graph.NewMeta(attempt.RevisionID, job.Scope, graph.RevisionActive, now),
		RevisionID: attempt.RevisionID, SnapshotID: job.Scope.SnapshotID, CommitSHA: job.CommitSHA,
		BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive,
		AlgorithmVersion: job.Versions.GraphAlgorithmVersion, ParserResultVersion: job.Versions.ParserResultVersion,
		GraphSchemaVersion: job.Versions.GraphSchemaVersion, BuildPolicyVersion: job.Versions.BuildPolicyVersion,
		QualityStatus: graph.QualityHealthy, RequestFingerprint: job.RequestFingerprint,
		Quality: graph.QualityStats{ResolutionCounts: map[string]int64{}, ErrorCounts: map[string]int64{}},
	}
}

func graphControlTestEvent(job graph.BuildJob, revision graph.Revision, now time.Time) common.EventEnvelope {
	return common.EventEnvelope{EventID: job.EventID, EventType: "graph.published.v1", AggregateID: revision.RevisionID,
		OccurredAt: now, Producer: "reposense-graph-worker", PayloadVersion: 1, TraceID: job.Scope.TraceID,
		Payload: map[string]any{"revision_id": revision.RevisionID, "snapshot_id": job.Scope.SnapshotID}}
}
