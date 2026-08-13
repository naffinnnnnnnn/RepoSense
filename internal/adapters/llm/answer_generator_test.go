package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/ports"
)

func TestAnswerGeneratorUsesEvidenceCatalogAndTracksUsage(t *testing.T) {
	model := &fakeChatModel{response: ChatResponse{Content: `{"answer_markdown":"Handle calls Service.","citation_indexes":[0]}`, Model: "model-x", InputTokens: 8, OutputTokens: 3}}
	generator, err := NewAnswerGenerator(model, AnswerGeneratorConfig{PromptVersion: "qa-v2"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := generator.GenerateAnswer(context.Background(), ports.AnswerGenerationContext{Question: "flow?", Locale: "en-US", Citations: []common.SourceRef{{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.TokenUsage != 11 || result.Model != "model-x" || result.PromptVersion != "qa-v2" || len(result.CitationIndexes) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if !strings.Contains(model.request.SystemPrompt, "untrusted data") || !strings.Contains(model.request.UserPrompt, "evidence_catalog") {
		t.Fatalf("unsafe prompt: %#v", model.request)
	}
}

func TestAnswerGeneratorRejectsUnknownCitation(t *testing.T) {
	model := &fakeChatModel{response: ChatResponse{Content: `{"answer_markdown":"claim","citation_indexes":[4]}`}}
	generator, _ := NewAnswerGenerator(model, AnswerGeneratorConfig{})
	_, err := generator.GenerateAnswer(context.Background(), ports.AnswerGenerationContext{Citations: []common.SourceRef{{CommitSHA: "sha", Path: "main.go", StartLine: 1, EndLine: 1}}})
	if err == nil {
		t.Fatal("expected citation validation error")
	}
}
