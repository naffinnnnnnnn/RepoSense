package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type Config struct {
	CacheDir       string
	Retention      time.Duration
	GitOutputBytes int
}
type Workspace struct {
	config      Config
	credentials ports.CredentialResolver
	locksMu     sync.Mutex
	locks       map[string]*sync.Mutex
}

func New(config Config, resolver ports.CredentialResolver) (*Workspace, error) {
	if strings.TrimSpace(config.CacheDir) == "" {
		return nil, errors.New("workspace cache dir 不能为空")
	}
	absolute, err := filepath.Abs(config.CacheDir)
	if err != nil {
		return nil, err
	}
	if filepath.Clean(absolute) == filepath.VolumeName(absolute)+string(filepath.Separator) {
		return nil, errors.New("workspace cache dir 不能是卷根目录")
	}
	config.CacheDir = absolute
	if config.Retention <= 0 {
		return nil, errors.New("workspace retention 必须为正数")
	}
	if config.GitOutputBytes <= 0 {
		return nil, errors.New("workspace Git 输出限制必须为正数")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	return &Workspace{config: config, credentials: resolver, locks: map[string]*sync.Mutex{}}, nil
}

func (w *Workspace) Prepare(ctx context.Context, command repository.SyncCommand) (ports.PreparedRepository, error) {
	provider := command.Provider
	if provider == "" {
		provider = "local"
		if command.RepositoryURL != "" {
			provider = "git"
		}
	}
	if provider == "local" {
		path, identity, err := canonicalLocal(command.RepositoryPath)
		return ports.PreparedRepository{Path: path, CanonicalIdentity: identity, Provider: provider}, err
	}
	identity, remote, err := CanonicalRemote(command.RepositoryURL)
	if err != nil {
		return ports.PreparedRepository{}, err
	}
	unlock := w.lock(identity)
	defer unlock()
	hash := sha256.Sum256([]byte(command.Scope.TenantID + "\x00" + identity))
	target := filepath.Join(w.config.CacheDir, hex.EncodeToString(hash[:16])+".git")
	environment := map[string]string{}
	if command.CredentialsRef != "" {
		if w.credentials == nil {
			return ports.PreparedRepository{}, errors.New("credentials_ref 已提供但未配置凭据解析器")
		}
		credential, resolveErr := w.credentials.ResolveGitCredential(ctx, command.Scope.TenantID, command.CredentialsRef)
		if resolveErr != nil {
			return ports.PreparedRepository{}, fmt.Errorf("解析 Git 凭据引用失败: %w", resolveErr)
		}
		environment = credential.Environment
	}
	if _, statErr := os.Stat(target); errors.Is(statErr, os.ErrNotExist) {
		if err := w.git(ctx, "", environment, "clone", "--mirror", "--filter=blob:none", "--", remote, target); err != nil {
			return ports.PreparedRepository{}, err
		}
	} else if statErr != nil {
		return ports.PreparedRepository{}, statErr
	} else {
		if err := w.git(ctx, target, environment, "remote", "update", "--prune"); err != nil {
			return ports.PreparedRepository{}, err
		}
	}
	return ports.PreparedRepository{Path: target, CanonicalIdentity: identity, Provider: provider}, nil
}

func CanonicalRemote(value string) (identity, remote string, err error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", "", errors.New("repository_url 无效")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "ssh" {
		return "", "", errors.New("repository_url 只允许 https 或 ssh")
	}
	if parsed.User != nil {
		return "", "", errors.New("repository_url 禁止内嵌凭据")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Path = strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/"), ".git")
	if parsed.Path == "" {
		return "", "", errors.New("repository_url 缺少 namespace")
	}
	identity = parsed.Scheme + "://" + parsed.Host + strings.ToLower(parsed.Path)
	parsed.Path = parsed.Path + ".git"
	return identity, parsed.String(), nil
}
func canonicalLocal(value string) (string, string, error) {
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", "", errors.New("本地仓库目录不存在")
	}
	identity := filepath.Clean(resolved)
	if filepath.Separator == '\\' {
		identity = strings.ToLower(identity)
	}
	return resolved, "local:" + filepath.ToSlash(identity), nil
}

func (w *Workspace) git(ctx context.Context, repositoryPath string, environment map[string]string, args ...string) error {
	commandArgs := []string{"-c", "core.quotepath=false"}
	if repositoryPath != "" {
		commandArgs = append(commandArgs, "-C", repositoryPath)
	}
	commandArgs = append(commandArgs, args...)
	cmd := exec.CommandContext(ctx, "git", commandArgs...)
	cmd.Env = os.Environ()
	for key, value := range environment {
		if strings.ContainsRune(key, '=') || strings.ContainsAny(key, "\r\n") {
			return errors.New("凭据环境变量名称无效")
		}
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	var output limitedBuffer
	output.limit = w.config.GitOutputBytes
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("Git workspace 操作失败")
	}
	if output.exceeded {
		return errors.New("Git workspace 输出超过限制")
	}
	return nil
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *limitedBuffer) Write(value []byte) (int, error) {
	size := len(value)
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return size, nil
	}
	if len(value) > remaining {
		value = value[:remaining]
		b.exceeded = true
	}
	_, _ = b.Buffer.Write(value)
	return size, nil
}
func (w *Workspace) lock(key string) func() {
	w.locksMu.Lock()
	lock := w.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		w.locks[key] = lock
	}
	w.locksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (w *Workspace) Cleanup(ctx context.Context, now time.Time) error {
	entries, err := os.ReadDir(w.config.CacheDir)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(w.config.CacheDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < w.config.Retention {
			continue
		}
		target := filepath.Join(root, entry.Name())
		relative, err := filepath.Rel(root, target)
		if err != nil || relative == "." || strings.HasPrefix(relative, "..") || filepath.IsAbs(relative) {
			return errors.New("workspace 清理目标越界")
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}
