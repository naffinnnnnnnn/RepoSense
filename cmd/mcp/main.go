package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/adapters/observability"
	agentapp "github.com/reposense/reposense/internal/application/agent"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	mcpapp "github.com/reposense/reposense/internal/application/mcp"
	ragapp "github.com/reposense/reposense/internal/application/rag"
	wikiapp "github.com/reposense/reposense/internal/application/wiki"
	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
	mcptransport "github.com/reposense/reposense/internal/transport/mcp"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("reposense-mcp", flag.ContinueOnError)
	tenantID := flags.String("tenant-id", env("REPOSENSE_TENANT_ID"), "tenant bound to this MCP process")
	clientID := flags.String("client-id", env("REPOSENSE_MCP_CLIENT_ID"), "authorized MCP client ID")
	principalID := flags.String("principal-id", env("REPOSENSE_MCP_PRINCIPAL_ID"), "authorized user or service principal")
	repositories := flags.String("repositories", env("REPOSENSE_MCP_REPOSITORIES"), "comma-separated repository IDs allowed for this process")
	quota := flags.Int("quota", 120, "quota units per client and capability window")
	quotaWindow := flags.Duration("quota-window", time.Minute, "rate-limit window")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*tenantID) == "" || strings.TrimSpace(*clientID) == "" || strings.TrimSpace(*principalID) == "" || len(csv(*repositories)) == 0 {
		return fmt.Errorf("tenant-id, client-id, principal-id, and repositories are required (flags or REPOSENSE_MCP_* environment variables)")
	}

	// The local composition deliberately uses replaceable in-memory adapters.
	// Production composition can inject PostgreSQL/pgvector/Neo4j adapters into
	// these same application ports without changing the MCP transport.
	logger := log.New(os.Stderr, "", 0)
	observer := observability.New(logger)
	repositoryStore := memory.NewRepositoryStore()
	graphService, err := graphapp.New(repositoryStore, memory.NewGraphRepository(), nil, observer, nil, nil, nil, graphapp.Config{})
	if err != nil {
		return err
	}
	ragService, err := ragapp.New(memory.NewRAGRepository(), graphService, nil, observer, nil, nil, nil, nil, ragapp.Config{})
	if err != nil {
		return err
	}
	wikiService, err := wikiapp.New(graphService, ragService, memory.NewWikiRepository(), nil, nil, observer, nil, nil, wikiapp.Config{})
	if err != nil {
		return err
	}
	agentService, err := agentapp.New(graphService, ragService, memory.NewAgentRepository(), nil, nil, nil, observer, nil, nil, agentapp.Config{})
	if err != nil {
		return err
	}
	limiter, err := mcpapp.NewFixedWindowLimiter(*quota, *quotaWindow)
	if err != nil {
		return err
	}
	facade, err := mcpapp.New(mcpapp.Services{Retriever: ragService, Graph: graphService, Wiki: wikiService, Agent: agentService}, memory.NewMCPAudit(), limiter, observer, nil, mcpapp.Config{})
	if err != nil {
		return err
	}
	grant := mcpdomain.ClientGrant{ClientID: strings.TrimSpace(*clientID), PrincipalID: strings.TrimSpace(*principalID), TenantID: strings.TrimSpace(*tenantID), RepositoryScopes: csv(*repositories), PermissionScopes: []string{mcpdomain.ScopeRepositoryRead}, Status: mcpdomain.GrantActive}
	server, err := mcptransport.NewServer(facade, grant, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(ctx, &sdk.StdioTransport{})
}

func csv(value string) []string {
	seen, out := map[string]bool{}, []string{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }
