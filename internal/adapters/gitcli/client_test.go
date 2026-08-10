package gitcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reposense/reposense/internal/domain/repository"
)

func TestClientReadsImmutableCommitAndDiff(t *testing.T) {
	repoPath := t.TempDir()
	git(t, repoPath, "init", "-b", "main")
	git(t, repoPath, "config", "user.email", "test@reposense.local")
	git(t, repoPath, "config", "user.name", "RepoSense Test")
	write(t, repoPath, "a.py", "def first():\n    return 1\n")
	git(t, repoPath, "add", "a.py")
	git(t, repoPath, "commit", "-m", "first")
	client := New()
	ctx := context.Background()
	first, err := client.ResolveCommit(ctx, repoPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	write(t, repoPath, "a.py", "def second():\n    return 2\n")
	write(t, repoPath, "b.ts", "export function b() {}\n")
	git(t, repoPath, "add", "a.py", "b.ts")
	git(t, repoPath, "commit", "-m", "second")
	second, err := client.ResolveCommit(ctx, repoPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	changes, err := client.Diff(ctx, repoPath, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if !hasChange(changes, "a.py", repository.ChangeModified) || !hasChange(changes, "b.ts", repository.ChangeAdded) {
		t.Fatalf("变更结果不符合预期：%#v", changes)
	}
	oldContent, err := client.ReadFile(ctx, repoPath, first, "a.py")
	if err != nil {
		t.Fatal(err)
	}
	if string(oldContent) != "def first():\n    return 1\n" {
		t.Fatalf("读取了可变工作树，而不是指定 commit：%q", oldContent)
	}
	if _, err := client.ReadFile(ctx, repoPath, second, "../secret"); !repository.IsCode(err, repository.ErrInvalidInput) {
		t.Fatalf("预期拒绝目录穿越路径，实际错误为：%v", err)
	}
}

func hasChange(changes []repository.ChangedPath, path string, kind repository.ChangeKind) bool {
	for _, item := range changes {
		if item.Path == path && item.Kind == kind {
			return true
		}
	}
	return false
}
func write(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
func git(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
