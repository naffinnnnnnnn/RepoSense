package mcpapp_test

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	agentapp "github.com/reposense/reposense/internal/application/agent"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	mcpapp "github.com/reposense/reposense/internal/application/mcp"
	ragapp "github.com/reposense/reposense/internal/application/rag"
	wikiapp "github.com/reposense/reposense/internal/application/wiki"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/wiki"
)

func TestFacadeIntegratesWithKnowledgeModules(t *testing.T) {
	facade, audit, grant, scope := integratedFacade(t, 100)
	ctx := context.Background()

	search, err := facade.SearchCode(ctx, grant, mcpapp.SearchCodeInput{RequestScope: requestScope(scope), Query: "HandleLogin token", TopK: 5})
	if err != nil || len(search.Evidence.Hits) == 0 || search.Evidence.Hits[0].SourceRef.Path == "" {
		t.Fatalf("search integration failed: %#v %v", search, err)
	}
	symbol, err := facade.GetSymbol(ctx, grant, mcpapp.GetSymbolInput{RequestScope: requestScope(scope), SymbolID: "handler"})
	if err != nil || symbol.Symbol.ArtifactID != "handler" {
		t.Fatalf("symbol integration failed: %#v %v", symbol, err)
	}
	chain, err := facade.FindCallChain(ctx, grant, mcpapp.FindCallChainInput{RequestScope: requestScope(scope), SymbolID: "handler", Direction: graph.DirectionOutgoing, Depth: 2})
	if err != nil || len(chain.Graph.Nodes) != 2 || len(chain.Graph.Edges) != 1 {
		t.Fatalf("call-chain integration failed: %#v %v", chain, err)
	}
	page, err := facade.GetWikiPage(ctx, grant, mcpapp.GetWikiPageInput{RequestScope: requestScope(scope), Slug: "architecture"})
	if err != nil || page.Page.Slug != "architecture" || len(page.Page.Citations) == 0 {
		t.Fatalf("wiki integration failed: %#v %v", page, err)
	}
	answer, err := facade.AskRepository(ctx, grant, mcpapp.AskRepositoryInput{RequestScope: requestScope(scope), Question: "How does HandleLogin call IssueToken?", Locale: "en-US"})
	if err != nil || answer.RunID == "" || answer.Answer.InsufficientEvidence || len(answer.Answer.Citations) == 0 {
		t.Fatalf("agent integration failed: %#v %v", answer, err)
	}

	records := audit.Records()
	if len(records) != 5 {
		t.Fatalf("expected one audit record per successful invocation, got %d", len(records))
	}
	for _, record := range records {
		if record.ResultStatus != mcpdomain.InvocationSucceeded || record.RequestHash == "" || record.TraceID == "" || record.RepositoryID != scope.RepositoryID || record.SnapshotID != scope.SnapshotID {
			t.Fatalf("invalid audit record: %#v", record)
		}
	}
}

func TestFacadeRejectsCrossRepositoryAndRateLimitedCalls(t *testing.T) {
	facade, audit, grant, scope := integratedFacade(t, 1)
	_, err := facade.SearchCode(context.Background(), grant, mcpapp.SearchCodeInput{RequestScope: mcpapp.RequestScope{RepositoryID: "another", SnapshotID: scope.SnapshotID}, Query: "login"})
	if !mcpdomain.IsCode(err, mcpdomain.ErrPermissionDenied) {
		t.Fatalf("expected repository denial, got %v", err)
	}
	_, err = facade.FindCallChain(context.Background(), grant, mcpapp.FindCallChainInput{RequestScope: requestScope(scope), SymbolID: "handler", Depth: 1})
	if !mcpdomain.IsCode(err, mcpdomain.ErrRateLimited) {
		t.Fatalf("expected rate limit, got %v", err)
	}
	records := audit.Records()
	if len(records) != 2 || records[0].ResultStatus != mcpdomain.InvocationDenied || records[1].ResultStatus != mcpdomain.InvocationLimited {
		t.Fatalf("denials were not audited: %#v", records)
	}
}

func integratedFacade(t *testing.T, quota int) (*mcpapp.Service, *memory.MCPAudit, mcpdomain.ClientGrant, common.Scope) {
	t.Helper()
	ctx := context.Background()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	commit := "abcdef123456"
	refHandler := common.SourceRef{CommitSHA: commit, Path: "internal/auth.go", SymbolID: "handler", StartLine: 10, EndLine: 20, ContentHash: "sha256:handler"}
	refToken := common.SourceRef{CommitSHA: commit, Path: "internal/token.go", SymbolID: "token", StartLine: 5, EndLine: 12, ContentHash: "sha256:token"}
	artifacts := []repository.CodeArtifact{
		{ArtifactID: "handler", Kind: repository.ArtifactFunction, Name: "HandleLogin", QualifiedName: "auth.HandleLogin", Language: "go", SourceRef: refHandler, Signature: "HandleLogin(request) error", ContentHash: refHandler.ContentHash},
		{ArtifactID: "token", Kind: repository.ArtifactFunction, Name: "IssueToken", QualifiedName: "token.IssueToken", Language: "go", SourceRef: refToken, Signature: "IssueToken(user) string", ContentHash: refToken.ContentHash},
	}
	store := memory.NewRepositoryStore()
	parsed := repository.ParseResult{Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusSucceeded)}, SnapshotID: scope.SnapshotID, CommitSHA: commit, SyncStatus: repository.StatusSucceeded}, Job: repository.ParseJob{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID, Status: string(repository.StatusSucceeded)}, JobID: "parse-job", SnapshotID: scope.SnapshotID, Status: repository.StatusSucceeded, Progress: 100}, Artifacts: artifacts,
		Relations: []repository.CodeRelation{{RelationID: "calls", Kind: repository.RelationCalls, From: "handler", To: "token", Evidence: refHandler, Confidence: 1}}}
	if err := store.SaveResult(ctx, "parse", parsed); err != nil {
		t.Fatal(err)
	}
	graphService, err := graphapp.New(store, memory.NewGraphRepository(), nil, nil, nil, nil, nil, graphapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := graphService.Build(ctx, graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph"})
	if err != nil {
		t.Fatal(err)
	}
	ragService, err := ragapp.New(memory.NewRAGRepository(), graphService, nil, nil, nil, nil, nil, nil, ragapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ragService.Index(ctx, scope, artifacts); err != nil {
		t.Fatal(err)
	}
	wikiService, err := wikiapp.New(graphService, ragService, memory.NewWikiRepository(), nil, nil, nil, nil, nil, wikiapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wikiService.Generate(ctx, wiki.GenerateCommand{Scope: scope, GraphRevisionID: revision.RevisionID, Locale: "en-US", PageScope: []string{"architecture"}, IdempotencyKey: "wiki"}); err != nil {
		t.Fatal(err)
	}
	agentService, err := agentapp.New(graphService, ragService, memory.NewAgentRepository(), nil, nil, nil, nil, nil, nil, agentapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	audit := memory.NewMCPAudit()
	limiter, err := mcpapp.NewFixedWindowLimiter(quota, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	facade, err := mcpapp.New(mcpapp.Services{Retriever: ragService, Graph: graphService, Wiki: wikiService, Agent: agentService}, audit, limiter, nil, nil, mcpapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	grant := mcpdomain.ClientGrant{ClientID: "client", PrincipalID: "user", TenantID: scope.TenantID, RepositoryScopes: []string{scope.RepositoryID}, PermissionScopes: []string{mcpdomain.ScopeRepositoryRead}, Status: mcpdomain.GrantActive, ExpiresAt: time.Now().Add(time.Hour)}
	return facade, audit, grant, scope
}

func requestScope(scope common.Scope) mcpapp.RequestScope {
	return mcpapp.RequestScope{RepositoryID: scope.RepositoryID, SnapshotID: scope.SnapshotID, TraceID: scope.TraceID}
}
