package gitcli

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/repository"
)

// TestMain 同时承担可控 Git 子进程入口。测试会把当前测试二进制复制为 git，
// 从而无需修改生产代码即可精确注入 Git 的 stdout、stderr 和退出码。
func TestMain(m *testing.M) {
	if os.Getenv("REPOSENSE_GIT_HELPER") == "1" {
		stdout, _ := base64.StdEncoding.DecodeString(os.Getenv("REPOSENSE_GIT_STDOUT"))
		stderr, _ := base64.StdEncoding.DecodeString(os.Getenv("REPOSENSE_GIT_STDERR"))
		_, _ = os.Stdout.Write(stdout)
		_, _ = os.Stderr.Write(stderr)
		code, _ := strconv.Atoi(os.Getenv("REPOSENSE_GIT_EXIT_CODE"))
		os.Exit(code)
	}
	os.Exit(m.Run())
}

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

// TestClientPreservesRepositoryNotFoundAcrossOperations 验证所有 Git 接口都保留
// “仓库不存在”这一根因，而不是按调用接口重新包装成 ref 或普通 Git 失败。
func TestClientPreservesRepositoryNotFoundAcrossOperations(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-repository")
	client := New()
	tests := []struct {
		name string
		call func() error
	}{
		{name: "resolve", call: func() error { _, err := client.ResolveCommit(context.Background(), missing, "main"); return err }},
		{name: "list_files", call: func() error {
			_, err := client.ListFiles(context.Background(), missing, strings.Repeat("a", 40))
			return err
		}},
		{name: "diff", call: func() error {
			_, err := client.Diff(context.Background(), missing, strings.Repeat("a", 40), strings.Repeat("b", 40))
			return err
		}},
		{name: "read_file", call: func() error {
			_, err := client.ReadFile(context.Background(), missing, strings.Repeat("a", 40), "a.go")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !repository.IsCode(err, repository.ErrRepositoryNotFound) {
				t.Fatalf("仓库不存在的根因不应被接口层覆盖：%v", err)
			}
		})
	}
}

// TestClientPreservesContextCancellationAcrossOperations 验证取消属于调用生命周期，
// 不能被误报成 ref 不存在或可重试的 Git 仓库故障。
func TestClientPreservesContextCancellationAcrossOperations(t *testing.T) {
	repoPath := t.TempDir()
	client := New()
	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{name: "resolve", call: func(ctx context.Context) error { _, err := client.ResolveCommit(ctx, repoPath, "main"); return err }},
		{name: "list_files", call: func(ctx context.Context) error {
			_, err := client.ListFiles(ctx, repoPath, strings.Repeat("a", 40))
			return err
		}},
		{name: "diff", call: func(ctx context.Context) error {
			_, err := client.Diff(ctx, repoPath, strings.Repeat("a", 40), strings.Repeat("b", 40))
			return err
		}},
		{name: "read_file", call: func(ctx context.Context) error {
			_, err := client.ReadFile(ctx, repoPath, strings.Repeat("a", 40), "a.go")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := tt.call(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("应保留 context.Canceled：%v", err)
			}
			if repository.IsCode(err, repository.ErrRefNotFound) || repository.IsCode(err, repository.ErrGitFailure) {
				t.Fatalf("取消不应被分类成 Git 业务错误：%v", err)
			}
		})
	}
}

// TestClientClassifiesMissingGitExecutable 验证运行环境没有 Git CLI 时返回基础设施故障，
// 而不是把“进程无法启动”误判为用户提供的 ref 不存在。
func TestClientClassifiesMissingGitExecutable(t *testing.T) {
	t.Setenv("PATH", "")
	_, err := New().ResolveCommit(context.Background(), t.TempDir(), "main")
	if !repository.IsCode(err, repository.ErrGitFailure) {
		t.Fatalf("Git 可执行文件缺失应返回 GIT_FAILURE：%v", err)
	}
}

// TestDiffClassifiesMissingCommitAsRefNotFound 使用真实仓库验证强推后的关键根因：
// Diff 的旧 commit 不存在属于基线引用失效，应用层只有拿到该类型才能决定降级全量同步。
func TestDiffClassifiesMissingCommitAsRefNotFound(t *testing.T) {
	repoPath := t.TempDir()
	git(t, repoPath, "init", "-b", "main")
	git(t, repoPath, "config", "user.email", "test@reposense.local")
	git(t, repoPath, "config", "user.name", "RepoSense Test")
	write(t, repoPath, "a.go", "package main\n")
	git(t, repoPath, "add", "a.go")
	git(t, repoPath, "commit", "-m", "first")
	head, err := New().ResolveCommit(context.Background(), repoPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	missing := strings.Repeat("f", len(head))
	_, err = New().Diff(context.Background(), repoPath, missing, head)
	if !repository.IsCode(err, repository.ErrRefNotFound) {
		t.Fatalf("Diff 基线不存在应返回 REF_NOT_FOUND：%v", err)
	}
}

// TestCappedBufferEnforcesGitOutputLimit 验证 Git CLI 输出缓冲区在边界内完整保留，
// 超过限制后停止增长并设置 exceeded，防止大仓库输出无限占用内存。
func TestCappedBufferEnforcesGitOutputLimit(t *testing.T) {
	buffer := cappedBuffer{limit: 4}
	if written, err := buffer.Write([]byte("abcd")); err != nil || written != 4 || buffer.exceeded || buffer.Len() != 4 {
		t.Fatalf("恰好达到上限时不应截断：written=%d len=%d exceeded=%v err=%v", written, buffer.Len(), buffer.exceeded, err)
	}
	if written, err := buffer.Write([]byte("ef")); err != nil || written != 2 || !buffer.exceeded || buffer.Len() != 4 || buffer.String() != "abcd" {
		t.Fatalf("超过上限后应停止增长：written=%d content=%q exceeded=%v err=%v", written, buffer.String(), buffer.exceeded, err)
	}
}

// TestGitOutputLimitFailureIsNotRetryable 验证固定输出上限不是随机故障：
// 对相同仓库和命令立即重试仍会超过同一上限，因此必须标记为不可重试。
func TestGitOutputLimitFailureIsNotRetryable(t *testing.T) {
	cause := errors.New("Git 输出超过固定字节限制")
	err := gitFailure("list_files", cause)
	var domainErr *repository.DomainError
	if !errors.As(err, &domainErr) || domainErr.Code != repository.ErrGitFailure || domainErr.Operation != "list_files" || domainErr.Retryable || !errors.Is(err, cause) {
		t.Fatalf("Git 输出上限错误分类不正确：%#v", domainErr)
	}
}

// TestDiffHandlesCopyAndRejectsUnknownStatus 验证 Git name-status 的已知状态需要完整映射，
// 对未来版本或异常输出中的未知状态则必须显式失败，不能静默丢失变更。
func TestDiffHandlesCopyAndRejectsUnknownStatus(t *testing.T) {
	t.Run("copy_is_added", func(t *testing.T) {
		installGitHelper(t, []byte("C100\x00old.go\x00new.go\x00"), nil, 0)
		changes, err := New().Diff(context.Background(), t.TempDir(), strings.Repeat("a", 40), strings.Repeat("b", 40))
		if err != nil {
			t.Fatalf("复制状态应被识别：%v", err)
		}
		if len(changes) != 1 || changes[0].Kind != repository.ChangeAdded || changes[0].Path != "new.go" {
			t.Fatalf("复制应投影为新增文件：%#v", changes)
		}
	})
	t.Run("unknown_is_error", func(t *testing.T) {
		installGitHelper(t, []byte("Z\x00future.go\x00"), nil, 0)
		_, err := New().Diff(context.Background(), t.TempDir(), strings.Repeat("a", 40), strings.Repeat("b", 40))
		if !repository.IsCode(err, repository.ErrGitFailure) {
			t.Fatalf("未知 Git 状态必须显式失败：%v", err)
		}
		cause := errors.Unwrap(err)
		if cause == nil || !strings.Contains(cause.Error(), "未知 Git 变更状态") {
			t.Fatalf("错误应指出未知状态，而不是偶然的格式错误：%v", err)
		}
	})
}

// installGitHelper 把当前测试二进制复制成临时 git 命令，并通过环境变量配置输出。
// 使用 base64 是因为 Windows 环境变量不能安全携带 name-status 所需的 NUL 分隔符。
func installGitHelper(t *testing.T, stdout, stderr []byte, exitCode int) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	name := "git"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, name), data, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	t.Setenv("REPOSENSE_GIT_HELPER", "1")
	t.Setenv("REPOSENSE_GIT_STDOUT", base64.StdEncoding.EncodeToString(stdout))
	t.Setenv("REPOSENSE_GIT_STDERR", base64.StdEncoding.EncodeToString(stderr))
	t.Setenv("REPOSENSE_GIT_EXIT_CODE", strconv.Itoa(exitCode))
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
