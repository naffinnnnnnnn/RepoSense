package assistant

import (
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
)

func validRef() common.SourceRef {
	return common.SourceRef{CommitSHA: "abc", Path: "main.go", StartLine: 1, EndLine: 3, ContentHash: "sha256:12345678"}
}

func TestCodingCommandValidation(t *testing.T) {
	valid := CodingCommand{Scope: common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}, SessionID: "session", UserID: "user",
		Intent: IntentPatch, Instruction: "fix it", SelectedRefs: []common.SourceRef{validRef()}, Constraints: []string{"keep API"},
		Permissions: []string{ReadPermission}, IdempotencyKey: "key"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*CodingCommand)
	}{
		{"unknown intent", func(c *CodingCommand) { c.Intent = "DELETE_REPOSITORY" }},
		{"missing permission", func(c *CodingCommand) { c.Permissions = nil }},
		{"duplicate refs", func(c *CodingCommand) { c.SelectedRefs = append(c.SelectedRefs, validRef()) }},
		{"oversize instruction", func(c *CodingCommand) { c.Instruction = strings.Repeat("x", MaxIntentLength+1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := valid
			cmd.SelectedRefs, cmd.Constraints, cmd.Permissions = append([]common.SourceRef(nil), valid.SelectedRefs...), append([]string(nil), valid.Constraints...), append([]string(nil), valid.Permissions...)
			test.mutate(&cmd)
			if err := cmd.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestFileChangeRejectsPathAndMultiFileInjection(t *testing.T) {
	valid := FileChange{Path: "main.go", BaseContentHash: "sha256:12345678", UnifiedDiff: "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []FileChange{
		{Path: "../main.go", BaseContentHash: valid.BaseContentHash, UnifiedDiff: valid.UnifiedDiff},
		{Path: "main.go\n+++ b/evil", BaseContentHash: valid.BaseContentHash, UnifiedDiff: valid.UnifiedDiff},
		{Path: "main.go", BaseContentHash: valid.BaseContentHash, UnifiedDiff: valid.UnifiedDiff + "--- a/other.go\n+++ b/other.go\n@@ -1 +1 @@\n-x\n+y\n"},
	} {
		if err := change.Validate(); err == nil {
			t.Fatalf("expected rejection for %#v", change)
		}
	}
}

func TestProposalRequiresGroundingAndIntentSafeChanges(t *testing.T) {
	base := ChangeProposal{ProposalID: "cp", SessionID: "s", UserID: "u", SnapshotID: "snap", BaseCommitSHA: "abc", Intent: IntentPatch,
		Summary: "fix", FileChanges: []FileChange{{Path: "main.go", BaseContentHash: "sha256:12345678", UnifiedDiff: "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}},
		RiskLevel: RiskLow, Citations: []common.SourceRef{validRef()}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	base.Citations[0].CommitSHA = "other"
	if err := base.Validate(); err == nil {
		t.Fatal("cross-commit citation must be rejected")
	}
	base.Citations[0] = validRef()
	base.Intent = IntentExplain
	if err := base.Validate(); err == nil {
		t.Fatal("explain proposal must not change files")
	}
}
