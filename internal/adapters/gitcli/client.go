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
const defaultMaxGitOutput = 64 << 20

// Client 无状态Git客户端
type Client struct{ maxOutput int }

// New 构造函数
func New() *Client { return &Client{maxOutput: defaultMaxGitOutput} }

func NewWithOutputLimit(limit int) (*Client, error) {
	if limit <= 0 {
		return nil, errors.New("Git 输出限制必须为正数")
	}
	return &Client{maxOutput: limit}, nil
}

func (c *Client) ResolveCommit(ctx context.Context, repoPath, ref string) (string, error) {
	if strings.TrimSpace(ref) == "" || strings.HasPrefix(ref, "-") {
		return "", invalid("resolve", "ref 无效", nil)
	}
	out, err := c.run(ctx, repoPath, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", classify(err, "resolve", true)
	}
	sha := strings.TrimSpace(string(out))
	if len(sha) != 40 && len(sha) != 64 {
		return "", invalid("resolve", "Git 返回了无效的 commit 标识", nil)
	}
	return sha, nil
}

func (c *Client) ListFiles(ctx context.Context, repoPath, commit string) ([]string, error) {
	out, err := c.run(ctx, repoPath, "ls-tree", "-r", "-z", "--format=%(objecttype) %(path)", commit)
	if err != nil {
		return nil, classify(err, "list_files", true)
	}
	entries := splitZero(out)
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		kind, name, ok := strings.Cut(entry, " ")
		if !ok {
			return nil, gitFailure("list_files", errors.New("ls-tree 输出格式错误"))
		}
		if kind == "blob" {
			files = append(files, name)
		}
	}
	return files, nil
}

func (c *Client) Diff(ctx context.Context, repoPath, from, to string) ([]repository.ChangedPath, error) {
	if from == to {
		return []repository.ChangedPath{}, nil
	}
	out, err := c.run(ctx, repoPath, "diff", "--name-status", "-z", "--find-renames", from, to)
	if err != nil {
		return nil, classify(err, "diff", true)
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
		case 'C':
			kind = repository.ChangeCopied
		case 'M':
			kind = repository.ChangeModified
		case 'T':
			kind = repository.ChangeTypeChanged
		default:
			return nil, gitFailure("diff", fmt.Errorf("未知 Git 变更状态 %q", status))
		}
		if kind == repository.ChangeRenamed || kind == repository.ChangeCopied {
			if i+1 >= len(parts) {
				return nil, gitFailure("diff", errors.New("重命名输出格式错误"))
			}
			if kind == repository.ChangeCopied {
				changes = append(changes, repository.ChangedPath{Path: parts[i+1], Kind: repository.ChangeAdded})
			} else {
				changes = append(changes, repository.ChangedPath{OldPath: parts[i], Path: parts[i+1], Kind: kind})
			}
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
	typeOut, err := c.run(ctx, repoPath, "cat-file", "-t", commit+":"+clean)
	if err != nil {
		return nil, classify(err, "read_file_type", true)
	}
	if strings.TrimSpace(string(typeOut)) != "blob" {
		return nil, &repository.DomainError{Code: repository.ErrGitFailure, Operation: "read_file_type", Message: "Git 对象不是 Blob", Retryable: false}
	}
	out, err := c.run(ctx, repoPath, "show", commit+":"+clean)
	if err != nil {
		return nil, classify(err, "read_file", true)
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
	limit := c.maxOutput
	if limit <= 0 {
		limit = defaultMaxGitOutput
	}
	stdout.limit = limit
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
		return nil, &commandError{cause: err, stderr: message}
	}
	if stdout.exceeded {
		return nil, &outputLimitError{limit: limit}
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
	retryable := true
	var limit *outputLimitError
	if errors.As(cause, &limit) || strings.Contains(cause.Error(), "输出超过") {
		retryable = false
	}
	return &repository.DomainError{Code: repository.ErrGitFailure, Operation: op, Message: "Git 仓库操作失败", Retryable: retryable, Cause: cause}
}

type commandError struct {
	cause  error
	stderr string
}

func (e *commandError) Error() string { return "Git 命令执行失败" }
func (e *commandError) Unwrap() error { return e.cause }

type outputLimitError struct{ limit int }

func (e *outputLimitError) Error() string {
	return fmt.Sprintf("Git 输出超过 %d 字节限制", e.limit)
}

func classify(err error, operation string, refSensitive bool) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var domain *repository.DomainError
	if errors.As(err, &domain) {
		return err
	}
	var executable *exec.Error
	if errors.As(err, &executable) {
		return &repository.DomainError{Code: repository.ErrGitFailure, Operation: "git_cli_missing", Message: "Git CLI 不可用", Retryable: false, Cause: err}
	}
	var command *commandError
	if refSensitive && errors.As(err, &command) {
		lower := strings.ToLower(command.stderr)
		if strings.Contains(lower, "bad object") || strings.Contains(lower, "unknown revision") || strings.Contains(lower, "not a valid object") || strings.Contains(lower, "needed a single revision") || strings.Contains(lower, "ambiguous argument") {
			return &repository.DomainError{Code: repository.ErrRefNotFound, Operation: operation, Message: "Git ref 或 commit 不存在", Retryable: false, Cause: err}
		}
	}
	return gitFailure(operation, err)
}
