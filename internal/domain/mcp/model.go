package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

type CapabilityKind string

const (
	CapabilityTool     CapabilityKind = "TOOL"
	CapabilityResource CapabilityKind = "RESOURCE"

	ScopeRepositoryRead = "repo:read"
	GrantActive         = "ACTIVE"
)

type Capability struct {
	Name           string         `json:"name"`
	Kind           CapabilityKind `json:"kind"`
	Version        string         `json:"version"`
	Description    string         `json:"description"`
	RequiredScopes []string       `json:"required_scopes"`
	QuotaUnits     int            `json:"quota_units"`
}

type ClientGrant struct {
	ClientID         string    `json:"client_id"`
	PrincipalID      string    `json:"principal_id"`
	TenantID         string    `json:"tenant_id"`
	RepositoryScopes []string  `json:"repository_scopes"`
	PermissionScopes []string  `json:"permission_scopes"`
	ExpiresAt        time.Time `json:"expires_at"`
	Status           string    `json:"status"`
}

func (g ClientGrant) Authorize(now time.Time, repositoryID string, required []string) error {
	if strings.TrimSpace(g.ClientID) == "" || strings.TrimSpace(g.PrincipalID) == "" || strings.TrimSpace(g.TenantID) == "" {
		return NewError(ErrUnauthenticated, "authorize", "client grant is incomplete", false, nil)
	}
	if g.Status != GrantActive || (!g.ExpiresAt.IsZero() && !now.Before(g.ExpiresAt)) {
		return NewError(ErrUnauthenticated, "authorize", "client grant is inactive or expired", false, nil)
	}
	if !contains(g.RepositoryScopes, repositoryID) && !contains(g.RepositoryScopes, "*") {
		return NewError(ErrPermissionDenied, "authorize", "repository access is not granted", false, nil)
	}
	for _, scope := range required {
		if !contains(g.PermissionScopes, scope) {
			return NewError(ErrPermissionDenied, "authorize", "required permission scope is missing", false, nil)
		}
	}
	return nil
}

type InvocationStatus string

const (
	InvocationSucceeded InvocationStatus = "SUCCEEDED"
	InvocationFailed    InvocationStatus = "FAILED"
	InvocationDenied    InvocationStatus = "DENIED"
	InvocationLimited   InvocationStatus = "RATE_LIMITED"
)

type Invocation struct {
	InvocationID string           `json:"invocation_id"`
	Capability   string           `json:"capability"`
	Version      string           `json:"version"`
	RequestHash  string           `json:"request_hash"`
	ResultStatus InvocationStatus `json:"result_status"`
	ErrorCode    ErrorCode        `json:"error_code,omitempty"`
	ClientID     string           `json:"client_id"`
	PrincipalID  string           `json:"principal_id"`
	TenantID     string           `json:"tenant_id"`
	RepositoryID string           `json:"repository_id"`
	SnapshotID   string           `json:"snapshot_id"`
	TraceID      string           `json:"trace_id"`
	LatencyMS    int64            `json:"latency_ms"`
	QuotaUnits   int              `json:"quota_units"`
	OccurredAt   time.Time        `json:"occurred_at"`
}

type AuditSink interface {
	Record(context.Context, Invocation) error
}

type RateLimiter interface {
	Allow(context.Context, string, int) (time.Duration, error)
}

func RequestHash(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		b = []byte("unmarshalable")
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
