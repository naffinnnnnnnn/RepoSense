package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/reposense/reposense/internal/adapters/gitcli"
	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/adapters/observability"
	parseradapter "github.com/reposense/reposense/internal/adapters/parser"
	repositoryapp "github.com/reposense/reposense/internal/application/repository"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
	ids := repositoryapp.RandomIDs{}
	if *traceID == "" {
		*traceID = ids.New("tr")
	}
	patterns := []string(nil)
	if *include != "" {
		patterns = []string{*include}
	}
	logger := log.New(os.Stderr, "", 0)
	service, err := repositoryapp.New(gitcli.New(), parseradapter.DefaultRegistry(), memory.NewRepositoryStore(), nil,
		observability.New(logger), ids, repositoryapp.SystemClock{}, repositoryapp.DefaultConfig())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := service.Sync(ctx, repository.SyncCommand{Scope: common.Scope{TenantID: *tenantID, RepositoryID: *repositoryID, TraceID: *traceID},
		RepositoryPath: *repoPath, Provider: "local", Ref: *ref, IncludePaths: patterns, IdempotencyKey: *repositoryID + ":" + *ref})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
