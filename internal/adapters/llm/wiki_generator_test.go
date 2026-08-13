package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

type fakeChatModel struct {
	response ChatResponse
	err      error
	request  ChatRequest
}

func (m *fakeChatModel) Complete(_ context.Context, request ChatRequest) (ChatResponse, error) {
	m.request = request
	return m.response, m.err
}

func TestWikiGeneratorUsesCatalogCitationsAndUsage(t *testing.T) {
	model := &fakeChatModel{response: ChatResponse{Content: `{"pages":[{"slug":"overview","title":"Overview","content_markdown":"# Overview\nGrounded summary.","citation_indexes":[0]}]}`, Model: "model-x", InputTokens: 10, OutputTokens: 5}}
	generator, err := NewWikiGenerator(model, WikiGeneratorConfig{PromptVersion: "prompt-v2"})
	if err != nil {
		t.Fatal(err)
	}
	ref := common.SourceRef{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 3, ContentHash: "sha256:x"}
	result, err := generator.Generate(context.Background(), ports.WikiGenerationContext{Locale: "en-US", PageSlugs: []string{"overview"}, Graph: graph.Result{Nodes: []graph.Entity{{NodeID: "n", EntityType: graph.EntityFunction, Name: "main", SourceRef: &ref}}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Model != "model-x" || result.PromptVersion != "prompt-v2" || result.TokenUsage != 15 || len(result.Pages[0].Citations) != 1 || !strings.Contains(result.Pages[0].ContentMarkdown, "Source Evidence") {
		t.Fatalf("unexpected result: %#v", result)
	}
	if !strings.Contains(model.request.SystemPrompt, "untrusted data") || !strings.Contains(model.request.UserPrompt, "evidence_catalog") {
		t.Fatalf("unsafe or incomplete prompt: %#v", model.request)
	}
}

func TestWikiGeneratorRejectsMalformedAndFabricatedCitations(t *testing.T) {
	ref := common.SourceRef{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 3}
	for name, content := range map[string]string{"malformed": `{`, "unknown citation": `{"pages":[{"slug":"overview","title":"x","content_markdown":"x","citation_indexes":[9]}]}`} {
		t.Run(name, func(t *testing.T) {
			generator, _ := NewWikiGenerator(&fakeChatModel{response: ChatResponse{Content: content}}, WikiGeneratorConfig{})
			_, err := generator.Generate(context.Background(), ports.WikiGenerationContext{PageSlugs: []string{"overview"}, Graph: graph.Result{Nodes: []graph.Entity{{NodeID: "n", Name: "main", SourceRef: &ref}}}})
			if err == nil {
				t.Fatal("expected response validation error")
			}
		})
	}
}
