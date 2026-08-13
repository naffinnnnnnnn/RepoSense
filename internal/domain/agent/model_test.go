package agent

import (
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
)

func testScope() common.Scope {
	return common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
}

func TestQuestionCommandValidateGuardBoundaries(t *testing.T) {
	valid := QuestionCommand{Scope: testScope(), ConversationID: "conv", Question: "How does auth work?", Permissions: []string{ReadPermission}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid command: %v", err)
	}
	for name, mutate := range map[string]func(*QuestionCommand){
		"empty question":    func(c *QuestionCommand) { c.Question = "  " },
		"missing snapshot":  func(c *QuestionCommand) { c.Scope.SnapshotID = "" },
		"permission denied": func(c *QuestionCommand) { c.Permissions = []string{"wiki:read"} },
		"invalid locale":    func(c *QuestionCommand) { c.Locale = "fr-FR" },
		"question too long": func(c *QuestionCommand) { c.Question = strings.Repeat("界", MaxQuestionLength+1) },
	} {
		t.Run(name, func(t *testing.T) {
			cmd := valid
			mutate(&cmd)
			if err := cmd.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNormalizeCitationsRejectsCrossVersionAndTraversal(t *testing.T) {
	valid := common.SourceRef{CommitSHA: "sha", Path: `src\main.go`, SymbolID: "main", StartLine: 2, EndLine: 4, ContentHash: "hash"}
	refs := []common.SourceRef{valid, valid,
		{CommitSHA: "other", Path: "other.go", StartLine: 1, EndLine: 1},
		{CommitSHA: "sha", Path: "../secret", StartLine: 1, EndLine: 1}}
	got, discarded := NormalizeCitations(refs, "sha", 10)
	if len(got) != 1 || got[0].Path != "src/main.go" {
		t.Fatalf("unexpected normalized refs: %#v", got)
	}
	if discarded != 2 {
		t.Fatalf("discarded=%d, want 2", discarded)
	}
}

func TestAnswerRequiresEvidenceUnlessExplicitlyInsufficient(t *testing.T) {
	if err := (Answer{AnswerMarkdown: "claim"}).Validate(); err == nil {
		t.Fatal("expected citation validation error")
	}
	if err := (Answer{AnswerMarkdown: "not enough evidence", InsufficientEvidence: true}).Validate(); err != nil {
		t.Fatal(err)
	}
}
