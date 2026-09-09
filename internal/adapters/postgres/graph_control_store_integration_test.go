//go:build integration

package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	orphans, err := store.UnreferencedGraphCandidates(ctx, expiredAt.Add(2*time.Second), expiredAt.Add(3*time.Second), 10)
	if err != nil || len(orphans) != 2 {
		t.Fatalf("failed and lost candidates should be reconcilable: %#v err=%v", orphans, err)
	}
}

func TestGraphControlStorePostgresConcurrentAdmissionIsBoundedAndIdempotent(t *testing.T) {
	store, pool, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	const replayCount = 32
	replayed := graphControlTestJob("job-replay", "key-replay", strings.Repeat("e", 64), "event-replay", now)
	var replayReady, replayWG sync.WaitGroup
	replayReady.Add(replayCount)
	replayWG.Add(replayCount)
	replayStart := make(chan struct{})
	replayErrors := make(chan error, replayCount)
	replayIDs := make(chan string, replayCount)
	for range replayCount {
		go func() {
			defer replayWG.Done()
			replayReady.Done()
			<-replayStart
			stored, _, err := enqueueGraphWithTransientRetry(ctx, store, replayed, 100, 100)
			if err != nil {
				replayErrors <- err
				return
			}
			replayIDs <- stored.JobID
		}()
	}
	replayReady.Wait()
	close(replayStart)
	replayWG.Wait()
	close(replayErrors)
	close(replayIDs)
	for err := range replayErrors {
		t.Errorf("concurrent replay failed after retries: %v", err)
	}
	for id := range replayIDs {
		if id != replayed.JobID {
			t.Errorf("concurrent replay diverged to job %q", id)
		}
	}

	const uniqueCount = 12
	var quotaReady, quotaWG sync.WaitGroup
	quotaReady.Add(uniqueCount)
	quotaWG.Add(uniqueCount)
	quotaStart := make(chan struct{})
	created := make(chan string, uniqueCount)
	rejected := make(chan error, uniqueCount)
	for i := range uniqueCount {
		go func(index int) {
			defer quotaWG.Done()
			quotaReady.Done()
			<-quotaStart
			fingerprint := fmt.Sprintf("%064x", index+100)
			job := graphControlTestJob(fmt.Sprintf("job-quota-%d", index), fmt.Sprintf("key-quota-%d", index), fingerprint, fmt.Sprintf("event-quota-%d", index), now)
			stored, wasCreated, err := enqueueGraphWithTransientRetry(ctx, store, job, 5, 3)
			if graph.IsCode(err, graph.ErrBuildCapacityExceeded) {
				rejected <- err
				return
			}
			if err != nil {
				rejected <- fmt.Errorf("unexpected admission error: %w", err)
				return
			}
			if !wasCreated {
				rejected <- fmt.Errorf("unique request converged to %s", stored.JobID)
				return
			}
			created <- stored.JobID
		}(i)
	}
	quotaReady.Wait()
	close(quotaStart)
	quotaWG.Wait()
	close(created)
	close(rejected)
	createdCount, capacityCount := 0, 0
	for range created {
		createdCount++
	}
	for err := range rejected {
		if !graph.IsCode(err, graph.ErrBuildCapacityExceeded) {
			t.Error(err)
			continue
		}
		capacityCount++
	}
	if createdCount != 2 || capacityCount != uniqueCount-2 {
		t.Fatalf("quota admission mismatch: created=%d capacity_rejected=%d", createdCount, capacityCount)
	}
	var total, tenantPending int
	if err := pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE tenant_id='tenant') FROM graph_build_jobs WHERE status IN ('PENDING','BUILDING')`).Scan(&total, &tenantPending); err != nil {
		t.Fatal(err)
	}
	if total != 3 || tenantPending != 3 {
		t.Fatalf("concurrent quota oversubscribed: total=%d tenant=%d", total, tenantPending)
	}
}

func TestGraphControlStorePostgresRollingRevisionSwitchIsAtomic(t *testing.T) {
	store, pool, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	activate := func(job graph.BuildJob, owner, attemptID, revisionID string, at time.Time) graph.Revision {
		t.Helper()
		if _, created, err := store.EnqueueGraphBuild(ctx, job); err != nil || !created {
			t.Fatalf("enqueue %s: created=%v err=%v", job.JobID, created, err)
		}
		claimed, attempt, found, err := store.ClaimGraphBuild(ctx, owner, attemptID, revisionID, at, time.Minute)
		if err != nil || !found || claimed.JobID != job.JobID {
			t.Fatalf("claim %s: found=%v claimed=%#v err=%v", job.JobID, found, claimed, err)
		}
		revision := graphControlTestRevision(claimed, attempt, at)
		if err := store.ActivateGraphRevision(ctx, claimed, attempt, revision, graphControlTestEvent(claimed, revision, at), at.Add(time.Second)); err != nil {
			t.Fatalf("activate %s: %v", revisionID, err)
		}
		return revision
	}

	firstJob := graphControlTestJob("job-old", "key-old", strings.Repeat("6", 64), "event-old", now)
	first := activate(firstJob, "worker-old", "attempt-old", "revision-old", now)
	secondJob := graphControlTestJob("job-new", "key-new", strings.Repeat("7", 64), "event-new", now.Add(2*time.Second))
	secondJob.Versions.GraphAlgorithmVersion = "algorithm-v2"
	second := activate(secondJob, "worker-new", "attempt-new", "revision-new", now.Add(2*time.Second))

	active, err := store.ActiveGraphRevision(ctx, secondJob.Scope)
	if err != nil || active.RevisionID != second.RevisionID || active.AlgorithmVersion != "algorithm-v2" {
		t.Fatalf("active pointer did not atomically switch: active=%#v err=%v", active, err)
	}
	var firstStatus, secondStatus string
	if err := pool.QueryRow(ctx, `SELECT
(SELECT build_status FROM graph_revisions WHERE revision_id=$1),
(SELECT build_status FROM graph_revisions WHERE revision_id=$2)`, first.RevisionID, second.RevisionID).Scan(&firstStatus, &secondStatus); err != nil {
		t.Fatal(err)
	}
	if firstStatus != string(graph.RevisionSuperseded) || secondStatus != string(graph.RevisionActive) {
		t.Fatalf("rolling statuses old=%s new=%s", firstStatus, secondStatus)
	}
	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM graph_outbox_events WHERE revision_id IN ($1,$2)`, first.RevisionID, second.RevisionID).Scan(&outboxCount); err != nil || outboxCount != 2 {
		t.Fatalf("rolling switch lost publication intent: count=%d err=%v", outboxCount, err)
	}
}

func TestGraphControlMigrationDown(t *testing.T) {
	_, pool, cleanup := graphControlTestStore(t)
	defer cleanup()
	ctx := context.Background()
	for _, migration := range []string{"000003_graph_runtime.down.sql", "000002_code_knowledge_graph.down.sql"} {
		down, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", migration))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(down)); err != nil {
			t.Fatalf("graph migration rollback %s: %v", migration, err)
		}
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname=current_schema() AND tablename LIKE 'graph_%'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("down migrations left %d graph tables", remaining)
	}
}

func enqueueGraphWithTransientRetry(ctx context.Context, store *GraphControlStore, job graph.BuildJob, maxGlobal, maxTenant int) (graph.BuildJob, bool, error) {
	var stored graph.BuildJob
	var created bool
	var err error
	for attempt := 0; attempt < 32; attempt++ {
		stored, created, err = store.EnqueueGraphBuildWithinQuota(ctx, job, maxGlobal, maxTenant)
		if err == nil || graph.IsCode(err, graph.ErrBuildCapacityExceeded) || graph.IsCode(err, graph.ErrIdempotencyConflict) {
			return stored, created, err
		}
		runtime.Gosched()
	}
	return stored, created, err
}

func graphControlTestStore(t *testing.T) (*GraphControlStore, *pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("REPOSENSE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("REPOSENSE_TEST_POSTGRES_DSN is required for the integration gate")
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
	for _, migration := range []string{"000002_code_knowledge_graph.up.sql", "000003_graph_runtime.up.sql"} {
		up, readErr := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", migration))
		if readErr != nil {
			pool.Close()
			admin.Close()
			t.Fatal(readErr)
		}
		if _, execErr := pool.Exec(ctx, string(up)); execErr != nil {
			pool.Close()
			admin.Close()
			t.Fatalf("apply %s: %v", migration, execErr)
		}
	}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	}
	return NewGraphControlStoreWithPool(pool), pool, cleanup
}
