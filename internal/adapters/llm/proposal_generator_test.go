package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/ports"
)

func proposalContext() ports.ProposalGenerationContext {
	ref := common.SourceRef{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 2, ContentHash: "sha256:12345678"}
	return ports.ProposalGenerationContext{Command: assistant.CodingCommand{Intent: assistant.IntentPatch, Instruction: "fix"},
		Evidence: ports.EvidenceBundle{Sources: []common.SourceRef{ref}, ContextBundle: rag.ContextBundle{Items: []rag.ContextItem{{SourceRef: ref, Text: "old"}}}}}
}

func TestProposalGeneratorProducesProviderNeutralDraft(t *testing.T) {
	model := &fakeChatModel{response: ChatResponse{Content: `{"summary":"fix","explanation":"reason","risk_level":"low","file_changes":[{"path":"main.go","base_content_hash":"sha256:12345678","unified_diff":"--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}],"test_plan":["go test ./..."],"citation_indexes":[0]}`, Model: "model", InputTokens: 3, OutputTokens: 4}}
	generator, err := NewProposalGenerator(model, ProposalGeneratorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := generator.GenerateProposal(context.Background(), proposalContext())
	if err != nil {
		t.Fatal(err)
	}
	if draft.RiskLevel != assistant.RiskLow || draft.TokenUsage != 7 || draft.Model != "model" || len(draft.FileChanges) != 1 {
		t.Fatalf("unexpected draft %#v", draft)
	}
	if strings.Contains(model.request.SystemPrompt, "old") || !strings.Contains(model.request.UserPrompt, "evidence_catalog") {
		t.Fatalf("unexpected prompts %#v", model.request)
	}
}

func TestProposalGeneratorRejectsUnknownOutputFieldsAndTrailingJSON(t *testing.T) {
	for _, content := range []string{
		`{"summary":"x","explanation":"x","risk_level":"LOW","file_changes":[],"test_plan":[],"citation_indexes":[0],"applied":true}`,
		`{"summary":"x","explanation":"x","risk_level":"LOW","file_changes":[],"test_plan":[],"citation_indexes":[0]} {}`,
	} {
		generator, _ := NewProposalGenerator(&fakeChatModel{response: ChatResponse{Content: content}}, ProposalGeneratorConfig{})
		if _, err := generator.GenerateProposal(context.Background(), proposalContext()); err == nil {
			t.Fatalf("expected strict JSON rejection for %s", content)
		}
	}
}
