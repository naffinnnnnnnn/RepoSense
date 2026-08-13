package mcp

import (
	"strings"
	"testing"
	"time"
)

func TestClientGrantAuthorizationBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	valid := ClientGrant{ClientID: "client", PrincipalID: "user", TenantID: "tenant", RepositoryScopes: []string{"repo"}, PermissionScopes: []string{ScopeRepositoryRead}, Status: GrantActive, ExpiresAt: now.Add(time.Minute)}
	if err := valid.Authorize(now, "repo", []string{ScopeRepositoryRead}); err != nil {
		t.Fatalf("valid grant denied: %v", err)
	}
	cases := []struct {
		name  string
		grant ClientGrant
		repo  string
		code  ErrorCode
	}{
		{"expired", func() ClientGrant { g := valid; g.ExpiresAt = now; return g }(), "repo", ErrUnauthenticated},
		{"inactive", func() ClientGrant { g := valid; g.Status = "REVOKED"; return g }(), "repo", ErrUnauthenticated},
		{"cross repository", valid, "other", ErrPermissionDenied},
		{"missing permission", func() ClientGrant { g := valid; g.PermissionScopes = nil; return g }(), "repo", ErrPermissionDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.grant.Authorize(now, tc.repo, []string{ScopeRepositoryRead}); !IsCode(err, tc.code) {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
}

func TestRequestHashIsDeterministicAndDoesNotContainInput(t *testing.T) {
	value := map[string]any{"question": "private source question", "top_k": 10}
	a, b := RequestHash(value), RequestHash(value)
	if a != b || !strings.HasPrefix(a, "sha256:") || len(a) != len("sha256:")+64 {
		t.Fatalf("invalid hash %q / %q", a, b)
	}
	if strings.Contains(a, "private") {
		t.Fatal("request hash leaked input")
	}
}
