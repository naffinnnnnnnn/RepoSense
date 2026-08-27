package workspace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestCanonicalRemoteRejectsCredentialsAndNormalizesIdentity(t *testing.T) {
	identity, remote, err := CanonicalRemote("https://GitHub.COM/Org/Repo.git?token=ignored#fragment")
	if err != nil {
		t.Fatal(err)
	}
	if identity != "https://github.com/org/repo" || remote != "https://github.com/Org/Repo.git" {
		t.Fatalf("identity=%q remote=%q", identity, remote)
	}
	if _, _, err := CanonicalRemote("https://user:secret@example.com/org/repo.git"); err == nil {
		t.Fatal("URL 内嵌凭据应被拒绝")
	}
}

func TestLocalWorkspaceUsesCanonicalPathAndCleanupStaysContained(t *testing.T) {
	testRoot, err := os.MkdirTemp(".", ".workspace-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	cache := filepath.Join(testRoot, "cache")
	workspace, err := New(Config{CacheDir: cache, Retention: time.Hour, GitOutputBytes: 1024}, nil)
	if err != nil {
		t.Fatal(err)
	}
	repositoryPath := filepath.Join(cache, "local-repository")
	if err := os.MkdirAll(repositoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	prepared, err := workspace.Prepare(context.Background(), repository.SyncCommand{Scope: common.Scope{TenantID: "t", RepositoryID: "r"}, Provider: "local", RepositoryPath: repositoryPath})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(prepared.Path) || !strings.HasPrefix(prepared.CanonicalIdentity, "local:") {
		t.Fatalf("准备结果错误：%#v", prepared)
	}
	stale := filepath.Join(cache, "stale.git")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(filepath.Dir(cache), "outside")
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Cleanup(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("过期缓存未清理：%v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("缓存外目录被影响：%v", err)
	}
}
