package gitcli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/reposense/reposense/internal/domain/repository"
)

// Git输出上限
const maxGitOutput = 64 << 20

// Client 无状态Git客户端
type Client struct{}

// New 构造函数
func New() *Client { return &Client{} }

func (c *Client) ResolveCommit(ctx context.Context, repoPath, ref string) (string, error) {
	if strings.TrimSpace(ref) == "" || strings.HasPrefix(ref, "-") {
		return "", invalid("resolve", "ref 无效", nil)
	}
	out, err := c.run(ctx, repoPath, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", &repository.DomainError{Code: repository.ErrRefNotFound, Operation: "resolve", Message: "无法解析 Git ref", Cause: err}
	}
	sha := strings.TrimSpace(string(out))
	if len(sha) != 40 && len(sha) != 64 {
		return "", invalid("resolve", "Git 返回了无效的 commit 标识", nil)
	}
	return sha, nil
}

func (c *Client) ListFiles(ctx context.Context, repoPath, commit string) ([]string, error) {
	out, err := c.run(ctx, repoPath, "ls-tree", "-r", "-z", "--name-only", commit)
	if err != nil {
		return nil, gitFailure("list_files", err)
	}
	return splitZero(out), nil
}

func (c *Client) Diff(ctx context.Context, repoPath, from, to string) ([]repository.ChangedPath, error) {
	if from == to {
		return []repository.ChangedPath{}, nil
	}
	out, err := c.run(ctx, repoPath, "diff", "--name-status", "-z", "--find-renames", from, to)
	if err != nil {
		return nil, gitFailure("diff", err)
	}
	parts := splitZero(out)
	changes := make([]repository.ChangedPath, 0, len(parts)/2)
	for i := 0; i < len(parts); {
		status := parts[i]
		i++
		if status == "" || i >= len(parts) {
			return nil, gitFailure("diff", errors.New("name-status 输出格式错误"))
		}
		kind := repository.ChangeModified
		switch status[0] {
		case 'A':
			kind = repository.ChangeAdded
		case 'D':
			kind = repository.ChangeDeleted
		case 'R':
			kind = repository.ChangeRenamed
		case 'M', 'T':
			kind = repository.ChangeModified
		default:
			continue
		}
		if kind == repository.ChangeRenamed {
			if i+1 >= len(parts) {
				return nil, gitFailure("diff", errors.New("重命名输出格式错误"))
			}
			changes = append(changes, repository.ChangedPath{OldPath: parts[i], Path: parts[i+1], Kind: kind})
			i += 2
		} else {
			changes = append(changes, repository.ChangedPath{Path: parts[i], Kind: kind})
			i++
		}
	}
	return changes, nil
}

func (c *Client) ReadFile(ctx context.Context, repoPath, commit, path string) ([]byte, error) {
	clean, err := cleanRelative(path)
	if err != nil {
		return nil, invalid("read_file", err.Error(), err)
	}
	out, err := c.run(ctx, repoPath, "show", commit+":"+clean)
	if err != nil {
		return nil, gitFailure("read_file", err)
	}
	return out, nil
}

func (c *Client) run(ctx context.Context, repoPath string, args ...string) ([]byte, error) {
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil || !info.IsDir() {
		return nil, &repository.DomainError{Code: repository.ErrRepositoryNotFound, Operation: "git", Message: "仓库目录不存在", Cause: err}
	}
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "core.quotepath=false", "-C", abs}, args...)...)
	var stdout cappedBuffer
	stdout.limit = maxGitOutput
	var stderr cappedBuffer
	stderr.limit = 4096
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		message := strings.TrimSpace(stderr.String())
		if len(message) > 1024 {
			message = message[:1024]
		}
		return nil, fmt.Errorf("Git 命令执行失败：%s", message)
	}
	if stdout.exceeded {
		return nil, fmt.Errorf("Git 输出超过 %d 字节限制", maxGitOutput)
	}
	return stdout.Bytes(), nil
}

type cappedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.exceeded = true
	}
	_, _ = b.Buffer.Write(p)
	return original, nil
}

func splitZero(data []byte) []string {
	raw := strings.Split(string(data), "\x00")
	if len(raw) > 0 && raw[len(raw)-1] == "" {
		raw = raw[:len(raw)-1]
	}
	return raw
}
func cleanRelative(path string) (string, error) {
	clean := filepath.ToSlash(filepath.Clean(path))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || filepath.IsAbs(path) {
		return "", fmt.Errorf("路径必须是仓库相对路径")
	}
	return clean, nil
}
func invalid(op, message string, cause error) error {
	return &repository.DomainError{Code: repository.ErrInvalidInput, Operation: op, Message: message, Cause: cause}
}
func gitFailure(op string, cause error) error {
	return &repository.DomainError{Code: repository.ErrGitFailure, Operation: op, Message: "Git 仓库操作失败", Retryable: true, Cause: cause}
}
