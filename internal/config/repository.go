package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/ports"
)

// RepositoryRuntime contains every operational limit needed by the API,
// worker, Git client, workspace cache and outbox dispatcher. Values are read
// once at startup so invalid production configuration fails fast.
type RepositoryRuntime struct {
	PostgresDSN, NATSURL, HTTPAddress, WorkspaceCache string
	Service                                           repositoryapp.Config
	ParseTimeout, WorkspaceRetention                  time.Duration
	GitOutputBytes                                    int
	Worker                                            repositoryapp.WorkerConfig
	Outbox                                            repositoryapp.OutboxConfig
	Retention                                         ports.RetentionPolicy
}

func LoadRepositoryRuntime() (RepositoryRuntime, error) {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	cacheDefault := filepath.Join(os.TempDir(), "reposense-workspaces")
	c := RepositoryRuntime{
		PostgresDSN:    strings.TrimSpace(os.Getenv("REPOSENSE_POSTGRES_DSN")),
		NATSURL:        strings.TrimSpace(os.Getenv("REPOSENSE_NATS_URL")),
		HTTPAddress:    envString("REPOSENSE_HTTP_ADDRESS", ":8080"),
		WorkspaceCache: envString("REPOSENSE_WORKSPACE_CACHE", cacheDefault),
		Service:        repositoryapp.DefaultConfig(),
		Worker:         repositoryapp.WorkerConfig{Owner: envString("REPOSENSE_WORKER_OWNER", host), Lease: 60 * time.Second, Heartbeat: 20 * time.Second, IdleDelay: time.Second, FailureDelay: 5 * time.Second, ParseTimeout: 15 * time.Minute},
		Outbox:         repositoryapp.OutboxConfig{BatchSize: 100, MaxAttempts: 10, BaseBackoff: time.Second, MaxBackoff: 5 * time.Minute},
		Retention:      ports.RetentionPolicy{FailedTaskRetention: 30 * 24 * time.Hour, OutboxRetention: 7 * 24 * time.Hour},
		ParseTimeout:   15 * time.Minute, WorkspaceRetention: 24 * time.Hour, GitOutputBytes: 64 << 20,
	}
	var err error
	if c.Service.MaxFiles, err = envInt("REPOSENSE_MAX_FILES", c.Service.MaxFiles); err != nil {
		return c, err
	}
	if c.Service.MaxFileBytes, err = envInt("REPOSENSE_MAX_FILE_BYTES", c.Service.MaxFileBytes); err != nil {
		return c, err
	}
	if c.GitOutputBytes, err = envInt("REPOSENSE_GIT_OUTPUT_BYTES", c.GitOutputBytes); err != nil {
		return c, err
	}
	if c.Outbox.BatchSize, err = envInt("REPOSENSE_OUTBOX_BATCH_SIZE", c.Outbox.BatchSize); err != nil {
		return c, err
	}
	if c.Outbox.MaxAttempts, err = envInt("REPOSENSE_OUTBOX_MAX_ATTEMPTS", c.Outbox.MaxAttempts); err != nil {
		return c, err
	}
	if c.ParseTimeout, err = envDuration("REPOSENSE_PARSE_TIMEOUT", c.ParseTimeout); err != nil {
		return c, err
	}
	c.Worker.ParseTimeout = c.ParseTimeout
	if c.Service.FailureCleanupTimeout, err = envDuration("REPOSENSE_FAILURE_CLEANUP_TIMEOUT", c.Service.FailureCleanupTimeout); err != nil {
		return c, err
	}
	if c.Worker.Lease, err = envDuration("REPOSENSE_JOB_LEASE", c.Worker.Lease); err != nil {
		return c, err
	}
	if c.Worker.Heartbeat, err = envDuration("REPOSENSE_JOB_HEARTBEAT", c.Worker.Heartbeat); err != nil {
		return c, err
	}
	if c.Worker.IdleDelay, err = envDuration("REPOSENSE_WORKER_IDLE_DELAY", c.Worker.IdleDelay); err != nil {
		return c, err
	}
	if c.Worker.FailureDelay, err = envDuration("REPOSENSE_WORKER_FAILURE_DELAY", c.Worker.FailureDelay); err != nil {
		return c, err
	}
	if c.Outbox.BaseBackoff, err = envDuration("REPOSENSE_OUTBOX_BASE_BACKOFF", c.Outbox.BaseBackoff); err != nil {
		return c, err
	}
	if c.Outbox.MaxBackoff, err = envDuration("REPOSENSE_OUTBOX_MAX_BACKOFF", c.Outbox.MaxBackoff); err != nil {
		return c, err
	}
	if c.WorkspaceRetention, err = envDuration("REPOSENSE_WORKSPACE_RETENTION", c.WorkspaceRetention); err != nil {
		return c, err
	}
	if c.Retention.FailedTaskRetention, err = envDuration("REPOSENSE_FAILED_TASK_RETENTION", c.Retention.FailedTaskRetention); err != nil {
		return c, err
	}
	if c.Retention.OutboxRetention, err = envDuration("REPOSENSE_OUTBOX_RETENTION", c.Retention.OutboxRetention); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func (c RepositoryRuntime) Validate() error {
	if c.PostgresDSN == "" || c.NATSURL == "" {
		return errors.New("REPOSENSE_POSTGRES_DSN 和 REPOSENSE_NATS_URL 必须配置")
	}
	if c.HTTPAddress == "" || c.WorkspaceCache == "" || c.Service.MaxFiles <= 0 || c.Service.MaxFileBytes <= 0 || c.Service.FailureCleanupTimeout <= 0 || c.ParseTimeout <= 0 || c.WorkspaceRetention <= 0 || c.GitOutputBytes <= 0 || c.Worker.Owner == "" || c.Worker.Lease <= 0 || c.Worker.Heartbeat <= 0 || c.Worker.Heartbeat >= c.Worker.Lease || c.Worker.IdleDelay <= 0 || c.Worker.FailureDelay <= 0 || c.Worker.ParseTimeout <= 0 || c.Outbox.BatchSize <= 0 || c.Outbox.MaxAttempts <= 0 || c.Outbox.BaseBackoff <= 0 || c.Outbox.MaxBackoff < c.Outbox.BaseBackoff || c.Retention.FailedTaskRetention <= 0 || c.Retention.OutboxRetention <= 0 {
		return errors.New("Repository Parser 运行配置必须为正数且 heartbeat 小于 lease")
	}
	return nil
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func envInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s 必须是正整数", name)
	}
	return value, nil
}
func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s 必须是正 duration", name)
	}
	return value, nil
}
