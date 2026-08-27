package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reposense/reposense/internal/adapters/gitcli"
	"github.com/reposense/reposense/internal/adapters/memory"
	natspublisher "github.com/reposense/reposense/internal/adapters/nats"
	"github.com/reposense/reposense/internal/adapters/observability"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	postgresadapter "github.com/reposense/reposense/internal/adapters/postgres"
	workspaceadapter "github.com/reposense/reposense/internal/adapters/workspace"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/config"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

var workerRepositoryStore = memory.NewRepositoryStore()
var workerEventPublisher ports.EventPublisher = workerPublisher{}

type workerPublisher struct{}

func (workerPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if len(os.Args) > 1 && os.Args[1] == "run" {
		err = runDaemon(ctx)
	} else {
		err = run(os.Args[1:])
	}
	if err != nil && err != context.Canceled {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runDaemon(ctx context.Context) error {
	cfg, err := config.LoadRepositoryRuntime()
	if err != nil {
		return err
	}
	store, err := postgresadapter.NewRepositoryStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("连接 Repository PostgreSQL 失败")
	}
	defer store.Close()
	publisher, err := natspublisher.Connect(cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("连接 Repository NATS 失败")
	}
	defer publisher.Close()
	git, err := gitcli.NewWithOutputLimit(cfg.GitOutputBytes)
	if err != nil {
		return err
	}
	workspaces, err := workspaceadapter.New(workspaceadapter.Config{CacheDir: cfg.WorkspaceCache, Retention: cfg.WorkspaceRetention, GitOutputBytes: cfg.GitOutputBytes}, workspaceadapter.EnvCredentialResolver{})
	if err != nil {
		return err
	}
	observer := observability.New(log.New(os.Stderr, "", 0))
	service, err := repositoryapp.New(git, parseradapter.DefaultRegistry(), store, publisher, observer, repositoryapp.RandomIDs{}, repositoryapp.SystemClock{}, cfg.Service)
	if err != nil {
		return err
	}
	service.WithWorkspace(workspaces)
	worker, err := repositoryapp.NewWorker(service, store, cfg.Worker)
	if err != nil {
		return err
	}
	dispatcher, err := repositoryapp.NewOutboxDispatcher(store, publisher, repositoryapp.SystemClock{}, cfg.Outbox)
	if err != nil {
		return err
	}
	dispatcher.WithObserver(observer)
	errCh := make(chan error, 2)
	go func() { errCh <- worker.Run(ctx) }()
	go func() {
		ticker := time.NewTicker(cfg.Worker.IdleDelay)
		cleanup := time.NewTicker(time.Hour)
		defer ticker.Stop()
		defer cleanup.Stop()
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case <-ticker.C:
				_, _ = dispatcher.DispatchOnce(ctx)
			case now := <-cleanup.C:
				_ = workspaces.Cleanup(ctx, now.UTC())
				if maintenance, ok := any(store).(ports.RepositoryMaintenanceStore); ok {
					_, _ = maintenance.CleanupExpired(ctx, now.UTC(), cfg.Retention)
				}
			}
		}
	}()
	return <-errCh
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "parse" {
		return fmt.Errorf("用法：worker parse --repo 路径 --tenant-id 租户ID --repository-id 仓库ID [--ref REF] [--include GLOB]")
	}
	flags := flag.NewFlagSet("parse", flag.ContinueOnError)
	repoPath := flags.String("repo", "", "本地 Git 仓库路径")
	tenantID := flags.String("tenant-id", "", "租户隔离键")
	repositoryID := flags.String("repository-id", "", "仓库隔离键")
	ref := flags.String("ref", "HEAD", "Git 分支、标签或 commit")
	include := flags.String("include", "", "可选的仓库相对路径 Glob")
	traceID := flags.String("trace-id", "", "链路追踪关联标识")
	timeout := flags.Duration("timeout", 2*time.Minute, "解析任务总超时时间")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("不接受位置参数：%v", flags.Args())
	}
	if *timeout <= 0 {
		return &repository.DomainError{Code: repository.ErrInvalidInput, Operation: "validate_timeout", Message: "timeout 必须为正数", Retryable: false}
	}
	ids := repositoryapp.RandomIDs{}
	if *traceID == "" {
		*traceID = ids.New("tr")
		if *traceID == "" {
			return fmt.Errorf("生成 trace id 失败")
		}
	}
	patterns := []string(nil)
	if *include != "" {
		patterns = []string{*include}
	}
	logger := log.New(os.Stderr, "", 0)
	service, err := repositoryapp.New(gitcli.New(), parseradapter.DefaultRegistry(), workerRepositoryStore, workerEventPublisher,
		observability.New(logger), ids, repositoryapp.SystemClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := service.Sync(ctx, repository.SyncCommand{Scope: common.Scope{TenantID: *tenantID, RepositoryID: *repositoryID, TraceID: *traceID},
		RepositoryPath: *repoPath, Provider: "local", Ref: *ref, IncludePaths: patterns, IdempotencyKey: *repositoryID + ":" + *ref + ":" + *traceID})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
