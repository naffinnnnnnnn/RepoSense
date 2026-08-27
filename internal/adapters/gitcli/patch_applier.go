package gitcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/ports"
)

// PatchApplier applies approved proposals to one configured checkout. It does
// not commit or stage changes. git apply validates the complete patch before
// changing the worktree; base hashes prevent applying against drifted files.
type PatchApplier struct {
	root string
	mu   sync.Mutex
}

func NewPatchApplier(root string) (*PatchApplier, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("repository root is not a directory: %w", err)
	}
	return &PatchApplier{root: abs}, nil
}

func (a *PatchApplier) ApplyPatch(ctx context.Context, request ports.PatchApplyRequest) (ports.PatchApplyResult, error) {
	if err := request.Scope.Validate(true); err != nil {
		return ports.PatchApplyResult{}, err
	}
	if strings.TrimSpace(request.ProposalID) == "" || strings.TrimSpace(request.BaseCommitSHA) == "" || len(request.FileChanges) == 0 {
		return ports.PatchApplyResult{}, fmt.Errorf("proposal id, base commit and file changes are required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	started := time.Now()
	head, err := a.git(ctx, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return ports.PatchApplyResult{}, fmt.Errorf("resolve worktree HEAD: %w", err)
	}
	if strings.TrimSpace(string(head)) != request.BaseCommitSHA {
		return ports.PatchApplyResult{}, fmt.Errorf("worktree HEAD drifted from proposal base commit")
	}
	var patch bytes.Buffer
	for _, change := range request.FileChanges {
		if err := change.Validate(); err != nil {
			return ports.PatchApplyResult{}, err
		}
		clean, err := cleanRelative(change.Path)
		if err != nil {
			return ports.PatchApplyResult{}, err
		}
		target := filepath.Join(a.root, filepath.FromSlash(clean))
		if err := rejectSymlinkPath(a.root, target); err != nil {
			return ports.PatchApplyResult{}, fmt.Errorf("resolve base file %q: %w", clean, err)
		}
		content, err := os.ReadFile(target)
		if err != nil {
			return ports.PatchApplyResult{}, fmt.Errorf("read base file %q: %w", clean, err)
		}
		if parserContentHash(content) != change.BaseContentHash {
			return ports.PatchApplyResult{}, fmt.Errorf("base content hash mismatch for %q", clean)
		}
		patch.WriteString(strings.TrimSuffix(strings.ReplaceAll(change.UnifiedDiff, "\r\n", "\n"), "\n"))
		patch.WriteByte('\n')
	}
	patchBytes := patch.Bytes()
	if _, err := a.git(ctx, patchBytes, "apply", "--check", "--whitespace=error-all", "-"); err != nil {
		return ports.PatchApplyResult{Validation: []assistant.ValidationResult{{Name: "git_apply_check", Status: assistant.ValidationFailed, Message: "unified diff did not apply cleanly"}}}, err
	}
	if _, err := a.git(ctx, patchBytes, "apply", "--whitespace=error-all", "-"); err != nil {
		return ports.PatchApplyResult{Validation: []assistant.ValidationResult{{Name: "git_apply", Status: assistant.ValidationFailed, Message: "atomic patch apply failed"}}}, err
	}
	return ports.PatchApplyResult{Validation: []assistant.ValidationResult{{Name: "base_hash", Status: assistant.ValidationPassed},
		{Name: "git_apply_check", Status: assistant.ValidationPassed}, {Name: "git_apply", Status: assistant.ValidationPassed, DurationMS: time.Since(started).Milliseconds()}}}, nil
}

func rejectSymlinkPath(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path resolves outside repository root")
	}
	current := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symbolic links are not writable")
		}
	}
	return nil
}

func (a *PatchApplier) git(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "core.quotepath=false", "-C", a.root}, args...)...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr cappedBuffer
	stdout.limit, stderr.limit = defaultMaxGitOutput, 4096
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("git patch command failed")
	}
	if stdout.exceeded {
		return nil, fmt.Errorf("git output exceeds %d bytes", defaultMaxGitOutput)
	}
	return stdout.Bytes(), nil
}

// Parser source hashes append a NUL delimiter through digest(parts...). Using
// the same algorithm keeps proposal base hashes compatible with indexed files.
func parserContentHash(content []byte) string {
	hash := sha256.New()
	hash.Write(content)
	hash.Write([]byte{0})
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

var _ ports.PatchApplier = (*PatchApplier)(nil)
