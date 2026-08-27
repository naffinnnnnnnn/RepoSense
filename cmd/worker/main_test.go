package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

// TestRunPersistsStateAcrossInvocationsForIncrementalSync 验证两次独立 CLI 调用能否共享上一次成功快照。
// 第二次调用发生在仓库产生新 commit 之后，工程预期应基于第一次快照执行增量解析。
func TestRunPersistsStateAcrossInvocationsForIncrementalSync(t *testing.T) {
	originalStore := workerRepositoryStore
	workerRepositoryStore = memory.NewRepositoryStore()
	t.Cleanup(func() { workerRepositoryStore = originalStore })
	repoPath := createWorkerTestRepository(t)

	firstOutput, err := captureWorkerStdout(t, func() error {
		return run(workerArgs(repoPath))
	})
	if err != nil {
		t.Fatal(err)
	}
	var first repository.ParseResult
	if err := json.Unmarshal(firstOutput, &first); err != nil {
		t.Fatalf("第一次 CLI 输出不是合法 ParseResult：%v，输出=%q", err, firstOutput)
	}
	if first.Job.Scope != repository.ScopeFull {
		t.Fatalf("首次解析应为 FULL，实际为：%s", first.Job.Scope)
	}

	// 在同一仓库创建第二个 commit，随后用新的 CLI 调用解析相同 ref。
	writeWorkerTestFile(t, repoPath, "a.py", "def second():\n    return 2\n")
	runWorkerGit(t, repoPath, "add", "a.py")
	runWorkerGit(t, repoPath, "commit", "-m", "second")

	secondOutput, err := captureWorkerStdout(t, func() error {
		return run(workerArgs(repoPath))
	})
	if err != nil {
		t.Fatal(err)
	}
	var second repository.ParseResult
	if err := json.Unmarshal(secondOutput, &second); err != nil {
		t.Fatalf("第二次 CLI 输出不是合法 ParseResult：%v，输出=%q", err, secondOutput)
	}

	// 如果 CLI 复用了持久化 Store，第二次应能读取第一次的 Snapshot 并进入 INCREMENTAL。
	if second.Job.Scope != repository.ScopeIncremental {
		t.Fatalf("第二次独立 CLI 调用应为 INCREMENTAL，实际为：%s", second.Job.Scope)
	}
}

// TestRunPublishesRepositoryParseEvent 记录 CLI 事件发布验证当前受到的结构性限制。
func TestRunPublishesRepositoryParseEvent(t *testing.T) {
	repoPath := createWorkerTestRepository(t)
	recorder := &workerEventRecorder{}
	original := workerEventPublisher
	originalStore := workerRepositoryStore
	workerEventPublisher = recorder
	workerRepositoryStore = memory.NewRepositoryStore()
	t.Cleanup(func() { workerEventPublisher = original; workerRepositoryStore = originalStore })
	if _, err := captureWorkerStdout(t, func() error { return run(workerArgs(repoPath)) }); err != nil {
		t.Fatal(err)
	}
	if len(recorder.events) != 1 || recorder.events[0].EventType != "parse.completed.v1" {
		t.Fatalf("CLI 未发布解析事件：%#v", recorder.events)
	}
}

type workerEventRecorder struct{ events []common.EventEnvelope }

func (r *workerEventRecorder) Publish(_ context.Context, event common.EventEnvelope) error {
	r.events = append(r.events, event)
	return nil
}

// TestRunRejectsNonPositiveTimeoutAsInvalidInput 验证零值和负数超时会在参数层被明确拒绝。
func TestRunRejectsNonPositiveTimeoutAsInvalidInput(t *testing.T) {
	repoPath := createWorkerTestRepository(t)
	tests := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append(workerArgs(repoPath), "--timeout", tt.value)
			err := run(args)
			// 非正数 timeout 属于调用参数错误，不应延迟成存储或 Context 运行期错误。
			if !repository.IsCode(err, repository.ErrInvalidInput) {
				t.Fatalf("非正数 timeout 应返回 INVALID_INPUT，实际为：%v", err)
			}
		})
	}
}

// TestRunRejectsUnexpectedPositionalArguments 验证所有 flag 解析结束后仍有多余参数时 CLI 会拒绝执行。
func TestRunRejectsUnexpectedPositionalArguments(t *testing.T) {
	repoPath := createWorkerTestRepository(t)
	args := append(workerArgs(repoPath), "unexpected")
	_, err := captureWorkerStdout(t, func() error {
		return run(args)
	})
	if err == nil {
		t.Fatal("CLI 不应静默忽略多余位置参数")
	}
}

// workerArgs 返回一组完整且合法的 CLI 参数，便于每个异常测试只改变目标输入。
func workerArgs(repoPath string) []string {
	return []string{"parse", "--repo", repoPath, "--tenant-id", "tenant", "--repository-id", "repo", "--ref", "HEAD"}
}

// createWorkerTestRepository 创建包含一个 commit 的真实临时 Git 仓库。
func createWorkerTestRepository(t *testing.T) string {
	t.Helper()
	repoPath := t.TempDir()
	runWorkerGit(t, repoPath, "init", "-b", "main")
	runWorkerGit(t, repoPath, "config", "user.email", "test@reposense.local")
	runWorkerGit(t, repoPath, "config", "user.name", "RepoSense Test")
	writeWorkerTestFile(t, repoPath, "a.py", "def first():\n    return 1\n")
	runWorkerGit(t, repoPath, "add", "a.py")
	runWorkerGit(t, repoPath, "commit", "-m", "first")
	return repoPath
}

// writeWorkerTestFile 在临时仓库内写入测试文件。
func writeWorkerTestFile(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// runWorkerGit 使用参数数组执行真实 Git，避免通过 Shell 拼接命令。
func runWorkerGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v 执行失败：%v：%s", args, err, output)
	}
}

// captureWorkerStdout 捕获 run 写出的 JSON，避免测试污染 go test 输出并便于反序列化断言。
func captureWorkerStdout(t *testing.T, fn func() error) ([]byte, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writer
	runErr := fn()
	_ = writer.Close()
	os.Stdout = original
	output, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	return output, runErr
}
