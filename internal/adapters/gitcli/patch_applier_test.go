package gitcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/ports"
)

func createPatchRepo(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "main.go"}, {"commit", "-m", "base"}} {
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return root, strings.TrimSpace(string(out))
}

func parserHashForTest(content string) string {
	h := sha256.New()
	h.Write([]byte(content))
	h.Write([]byte{0})
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func TestPatchApplierChecksHashAndAppliesWithoutCommitting(t *testing.T) {
	root, sha := createPatchRepo(t)
	applier, err := NewPatchApplier(root)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.PatchApplyRequest{Scope: common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}, ProposalID: "cp", BaseCommitSHA: sha,
		FileChanges: []assistant.FileChange{{Path: "main.go", BaseContentHash: parserHashForTest("old\n"), UnifiedDiff: "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}}}
	result, err := applier.ApplyPatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(filepath.Join(root, "main.go"))
	if strings.ReplaceAll(string(content), "\r\n", "\n") != "new\n" || result.CommitSHA != "" || len(result.Validation) != 3 {
		t.Fatalf("content=%q result=%#v", content, result)
	}
	head, _ := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if strings.TrimSpace(string(head)) != sha {
		t.Fatal("assistant must not commit automatically")
	}
}

func TestPatchApplierRejectsDriftBeforeMutation(t *testing.T) {
	root, sha := createPatchRepo(t)
	applier, _ := NewPatchApplier(root)
	request := ports.PatchApplyRequest{Scope: common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}, ProposalID: "cp", BaseCommitSHA: sha,
		FileChanges: []assistant.FileChange{{Path: "main.go", BaseContentHash: "sha256:00000000", UnifiedDiff: "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}}}
	if _, err := applier.ApplyPatch(context.Background(), request); err == nil {
		t.Fatal("expected hash drift rejection")
	}
	content, _ := os.ReadFile(filepath.Join(root, "main.go"))
	if string(content) != "old\n" {
		t.Fatalf("file mutated on failed validation: %q", content)
	}
}
