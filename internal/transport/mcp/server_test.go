package mcptransport

import (
	"context"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	mcpapp "github.com/reposense/reposense/internal/application/mcp"
	"github.com/reposense/reposense/internal/domain/common"
	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
	"github.com/reposense/reposense/internal/domain/wiki"
)

type fakeFacade struct{ wikiCalls int }

func (*fakeFacade) SearchCode(context.Context, mcpdomain.ClientGrant, mcpapp.SearchCodeInput) (mcpapp.SearchCodeOutput, error) {
	return mcpapp.SearchCodeOutput{}, nil
}
func (*fakeFacade) GetSymbol(context.Context, mcpdomain.ClientGrant, mcpapp.GetSymbolInput) (mcpapp.GetSymbolOutput, error) {
	return mcpapp.GetSymbolOutput{}, nil
}
func (*fakeFacade) FindCallChain(context.Context, mcpdomain.ClientGrant, mcpapp.FindCallChainInput) (mcpapp.FindCallChainOutput, error) {
	return mcpapp.FindCallChainOutput{}, nil
}
func (f *fakeFacade) GetWikiPage(_ context.Context, _ mcpdomain.ClientGrant, input mcpapp.GetWikiPageInput) (mcpapp.GetWikiPageOutput, error) {
	f.wikiCalls++
	return mcpapp.GetWikiPageOutput{Page: wiki.PageRevision{PageID: "page", SnapshotID: input.SnapshotID, Slug: input.Slug, ContentMarkdown: "# Architecture", Citations: []common.SourceRef{{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 2, ContentHash: "hash"}}}}, nil
}
func (f *fakeFacade) ReadWikiPageResource(ctx context.Context, grant mcpdomain.ClientGrant, input mcpapp.GetWikiPageInput) (mcpapp.GetWikiPageOutput, error) {
	return f.GetWikiPage(ctx, grant, input)
}
func (*fakeFacade) AskRepository(context.Context, mcpdomain.ClientGrant, mcpapp.AskRepositoryInput) (mcpapp.AskRepositoryOutput, error) {
	return mcpapp.AskRepositoryOutput{}, nil
}

func TestOfficialSDKDiscoversCallsToolsAndReadsResource(t *testing.T) {
	ctx := context.Background()
	facade := &fakeFacade{}
	server, err := NewServer(facade, mcpdomain.ClientGrant{ClientID: "client"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := sdk.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := sdk.NewClient(&sdk.Implementation{Name: "contract-test", Version: "1.0"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 5 {
		t.Fatalf("expected five discoverable tools, got %d", len(tools.Tools))
	}
	for _, tool := range tools.Tools {
		if tool.InputSchema == nil || tool.OutputSchema == nil || tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool contract is incomplete: %#v", tool)
		}
	}
	result, err := clientSession.CallTool(ctx, &sdk.CallToolParams{Name: mcpapp.GetWikiPageTool, Arguments: map[string]any{"repository_id": "repo", "snapshot_id": "snap", "slug": "architecture"}})
	if err != nil || result.IsError || result.StructuredContent == nil {
		t.Fatalf("tool call failed: %#v %v", result, err)
	}
	templates, err := clientSession.ListResourceTemplates(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(templates.ResourceTemplates) != 1 || templates.ResourceTemplates[0].URITemplate != WikiResourceTemplate {
		t.Fatalf("resource template missing: %#v", templates)
	}
	resource, err := clientSession.ReadResource(ctx, &sdk.ReadResourceParams{URI: "reposense://wiki/repo/snap/architecture"})
	if err != nil || len(resource.Contents) != 1 || resource.Contents[0].Text == "" || facade.wikiCalls != 2 {
		t.Fatalf("resource read failed: %#v calls=%d err=%v", resource, facade.wikiCalls, err)
	}
}
