package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestRepositoryStorePostgresConcurrencyPersistenceAndLeaseRecovery(t *testing.T) {
	dsn := os.Getenv("REPOSENSE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("REPOSENSE_TEST_POSTGRES_DSN 未配置")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("repository_parser_test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", "000001_repository_parser.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	store := NewRepositoryStoreWithPool(pool)
	now := time.Now().UTC()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", TraceID: "trace"}
	if err := store.BindRepository(ctx, repository.RepositoryBinding{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Provider: "local", CanonicalIdentity: "local:/repo", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	const clients = 8
	jobs := make(chan string, clients)
	errs := make(chan error, clients)
	var wait sync.WaitGroup
	for i := 0; i < clients; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			task := postgresTask(scope, fmt.Sprintf("job-%d", i), fmt.Sprintf("snap-%d", i), now)
			got, _, err := store.AcquireIdempotency(ctx, task)
			if err == nil {
				jobs <- got.Job.JobID
			}
			errs <- err
		}(i)
	}
	wait.Wait()
	close(jobs)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	unique := map[string]bool{}
	for job := range jobs {
		unique[job] = true
	}
	if len(unique) != 1 {
		t.Fatalf("并发同键产生多个任务：%v", unique)
	}

	restarted := NewRepositoryStoreWithPool(pool)
	claimed, found, err := restarted.ClaimPendingJob(ctx, "worker-1", time.Millisecond)
	if err != nil || !found {
		t.Fatalf("重启后领取失败：%#v %v", claimed, err)
	}
	time.Sleep(5 * time.Millisecond)
	recovered, found, err := restarted.ClaimPendingJob(ctx, "worker-2", time.Second)
	if err != nil || !found || recovered.Job.JobID != claimed.Job.JobID || recovered.LeaseOwner != "worker-2" {
		t.Fatalf("租约恢复失败：%#v %v", recovered, err)
	}
	var snapshots, jobsCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repository_snapshots`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM parse_jobs`).Scan(&jobsCount); err != nil {
		t.Fatal(err)
	}
	if snapshots != 1 || jobsCount != 1 {
		t.Fatalf("幂等冲突遗留孤立数据：snapshots=%d jobs=%d", snapshots, jobsCount)
	}
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "postgres", "000001_repository_parser.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("迁移回滚失败：%v", err)
	}
	var repositoriesTable *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('repositories')::text`).Scan(&repositoriesTable); err != nil {
		t.Fatal(err)
	}
	if repositoriesTable != nil {
		t.Fatalf("down migration 未删除 repositories：%s", *repositoriesTable)
	}
}

func postgresTask(scope common.Scope, jobID, snapshotID string, now time.Time) repository.ParseTask {
	command := repository.SyncCommand{Scope: scope, RepositoryPath: ".", Provider: "local", Ref: "main", IdempotencyKey: "same-key"}
	return repository.ParseTask{Command: command, CommandFingerprint: "same-fingerprint", RepositoryIdentity: "local:/repo", Attempt: 1,
		Job:      repository.ParseJob{EntityMeta: repository.NewMeta(jobID, scope, repository.StatusPending, now), JobID: jobID, SnapshotID: snapshotID, ParserVersion: "test", Scope: repository.ScopeFull, Status: repository.StatusPending},
		Snapshot: repository.Snapshot{EntityMeta: repository.NewMeta(snapshotID, scope, repository.StatusPending, now), SnapshotID: snapshotID, Provider: "local", Ref: "main", SyncStatus: repository.StatusPending, ChangedPaths: []repository.ChangedPath{}}}
}
