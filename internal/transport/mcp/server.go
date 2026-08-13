package mcptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	mcpapp "github.com/reposense/reposense/internal/application/mcp"
	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
)

const WikiResourceTemplate = "reposense://wiki/{repository_id}/{snapshot_id}/{slug}"

type Facade interface {
	SearchCode(context.Context, mcpdomain.ClientGrant, mcpapp.SearchCodeInput) (mcpapp.SearchCodeOutput, error)
	GetSymbol(context.Context, mcpdomain.ClientGrant, mcpapp.GetSymbolInput) (mcpapp.GetSymbolOutput, error)
	FindCallChain(context.Context, mcpdomain.ClientGrant, mcpapp.FindCallChainInput) (mcpapp.FindCallChainOutput, error)
	GetWikiPage(context.Context, mcpdomain.ClientGrant, mcpapp.GetWikiPageInput) (mcpapp.GetWikiPageOutput, error)
	ReadWikiPageResource(context.Context, mcpdomain.ClientGrant, mcpapp.GetWikiPageInput) (mcpapp.GetWikiPageOutput, error)
	AskRepository(context.Context, mcpdomain.ClientGrant, mcpapp.AskRepositoryInput) (mcpapp.AskRepositoryOutput, error)
}

func NewServer(facade Facade, grant mcpdomain.ClientGrant, logger *slog.Logger) (*sdk.Server, error) {
	if facade == nil {
		return nil, errors.New("MCP facade must not be nil")
	}
	server := sdk.NewServer(&sdk.Implementation{Name: "reposense", Title: "RepoSense", Version: mcpapp.CapabilityVersion, Description: "Version-pinned repository knowledge tools and resources."}, &sdk.ServerOptions{
		Instructions: "Every request must name repository_id and snapshot_id. Results are restricted to the configured client grant and include source citations when available.",
		Logger:       logger,
	})
	readOnly := &sdk.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: boolPtr(false), DestructiveHint: boolPtr(false)}
	sdk.AddTool(server, &sdk.Tool{Name: mcpapp.SearchCodeTool, Description: "Search code in an immutable repository snapshot and return ranked evidence with source locations.", Annotations: readOnly},
		func(ctx context.Context, _ *sdk.CallToolRequest, input mcpapp.SearchCodeInput) (*sdk.CallToolResult, mcpapp.SearchCodeOutput, error) {
			out, err := facade.SearchCode(ctx, grant, input)
			return nil, out, err
		})
	sdk.AddTool(server, &sdk.Tool{Name: mcpapp.GetSymbolTool, Description: "Get one symbol by graph node ID or artifact ID from an immutable snapshot.", Annotations: readOnly},
		func(ctx context.Context, _ *sdk.CallToolRequest, input mcpapp.GetSymbolInput) (*sdk.CallToolResult, mcpapp.GetSymbolOutput, error) {
			out, err := facade.GetSymbol(ctx, grant, input)
			return nil, out, err
		})
	sdk.AddTool(server, &sdk.Tool{Name: mcpapp.FindCallChainTool, Description: "Traverse CALLS edges around a symbol with a bounded direction, depth, and result limit.", Annotations: readOnly},
		func(ctx context.Context, _ *sdk.CallToolRequest, input mcpapp.FindCallChainInput) (*sdk.CallToolResult, mcpapp.FindCallChainOutput, error) {
			out, err := facade.FindCallChain(ctx, grant, input)
			return nil, out, err
		})
	sdk.AddTool(server, &sdk.Tool{Name: mcpapp.GetWikiPageTool, Description: "Read a generated Wiki page and its exact source citations from an immutable snapshot.", Annotations: readOnly},
		func(ctx context.Context, _ *sdk.CallToolRequest, input mcpapp.GetWikiPageInput) (*sdk.CallToolResult, mcpapp.GetWikiPageOutput, error) {
			out, err := facade.ReadWikiPageResource(ctx, grant, input)
			return nil, out, err
		})
	sdk.AddTool(server, &sdk.Tool{Name: mcpapp.AskRepositoryTool, Description: "Ask an evidence-backed repository question. The answer is grounded in the selected snapshot.", Annotations: readOnly},
		func(ctx context.Context, _ *sdk.CallToolRequest, input mcpapp.AskRepositoryInput) (*sdk.CallToolResult, mcpapp.AskRepositoryOutput, error) {
			out, err := facade.AskRepository(ctx, grant, input)
			return nil, out, err
		})

	server.AddResourceTemplate(&sdk.ResourceTemplate{Name: mcpapp.WikiResource, Title: "RepoSense Wiki page", Description: "A generated, version-pinned Wiki page with source citations.", MIMEType: "application/json", URITemplate: WikiResourceTemplate},
		func(ctx context.Context, req *sdk.ReadResourceRequest) (*sdk.ReadResourceResult, error) {
			input, err := parseWikiURI(req.Params.URI)
			if err != nil {
				return nil, err
			}
			out, err := facade.GetWikiPage(ctx, grant, input)
			if err != nil {
				return nil, err
			}
			payload, err := json.Marshal(out.Page)
			if err != nil {
				return nil, fmt.Errorf("encode wiki resource: %w", err)
			}
			return &sdk.ReadResourceResult{Contents: []*sdk.ResourceContents{{URI: req.Params.URI, MIMEType: "application/json", Text: string(payload)}}}, nil
		})
	return server, nil
}

func parseWikiURI(raw string) (mcpapp.GetWikiPageInput, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "reposense" || u.Host != "wiki" {
		return mcpapp.GetWikiPageInput{}, sdk.ResourceNotFoundError(raw)
	}
	parts := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if len(parts) != 3 {
		return mcpapp.GetWikiPageInput{}, sdk.ResourceNotFoundError(raw)
	}
	for i := range parts {
		parts[i], err = url.PathUnescape(parts[i])
		if err != nil || strings.TrimSpace(parts[i]) == "" {
			return mcpapp.GetWikiPageInput{}, sdk.ResourceNotFoundError(raw)
		}
	}
	return mcpapp.GetWikiPageInput{RequestScope: mcpapp.RequestScope{RepositoryID: parts[0], SnapshotID: parts[1]}, Slug: parts[2]}, nil
}

func boolPtr(value bool) *bool { return &value }
