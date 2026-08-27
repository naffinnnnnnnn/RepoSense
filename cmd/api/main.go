package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reposense/reposense/internal/adapters/gitcli"
	natspublisher "github.com/reposense/reposense/internal/adapters/nats"
	"github.com/reposense/reposense/internal/adapters/observability"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	postgresadapter "github.com/reposense/reposense/internal/adapters/postgres"
	workspaceadapter "github.com/reposense/reposense/internal/adapters/workspace"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/config"
	httptransport "github.com/reposense/reposense/internal/transport/hertz"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadRepositoryRuntime()
	if err != nil {
		return err
	}
	store, err := postgresadapter.NewRepositoryStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return errors.New("连接 Repository PostgreSQL 失败")
	}
	defer store.Close()
	publisher, err := natspublisher.Connect(cfg.NATSURL)
	if err != nil {
		return errors.New("连接 Repository NATS 失败")
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
	handler, err := httptransport.NewRepositoryHandler(service, httptransport.HeaderAuthenticator{})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case serveErr := <-errCh:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	}
}
