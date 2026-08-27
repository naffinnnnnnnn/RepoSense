package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/reposense/reposense/internal/ports"
)

// EnvCredentialResolver is a minimal secret-provider boundary for deployments
// that inject secrets as environment variables. The reference itself is
// hashed into the variable name, and the secret value is never returned in an
// error or log message.
type EnvCredentialResolver struct{}

type envCredential struct {
	TenantID    string            `json:"tenant_id"`
	Environment map[string]string `json:"environment"`
}

func (EnvCredentialResolver) ResolveGitCredential(ctx context.Context, tenantID, ref string) (ports.GitCredential, error) {
	if err := ctx.Err(); err != nil {
		return ports.GitCredential{}, err
	}
	if tenantID == "" || strings.TrimSpace(ref) == "" {
		return ports.GitCredential{}, errors.New("凭据引用无效")
	}
	digest := sha256.Sum256([]byte(ref))
	name := "REPOSENSE_GIT_CREDENTIAL_" + strings.ToUpper(hex.EncodeToString(digest[:12]))
	raw := os.Getenv(name)
	if raw == "" {
		return ports.GitCredential{}, errors.New("凭据引用不存在")
	}
	var secret envCredential
	if err := json.Unmarshal([]byte(raw), &secret); err != nil {
		return ports.GitCredential{}, errors.New("凭据配置格式无效")
	}
	if secret.TenantID != tenantID {
		return ports.GitCredential{}, errors.New("无权使用该凭据引用")
	}
	allowed := map[string]bool{"GIT_ASKPASS": true, "GIT_SSH_COMMAND": true, "SSH_AUTH_SOCK": true, "GIT_TERMINAL_PROMPT": true}
	for key := range secret.Environment {
		if !allowed[key] {
			return ports.GitCredential{}, errors.New("凭据包含不允许的 Git 环境变量")
		}
	}
	return ports.GitCredential{Environment: secret.Environment}, nil
}
