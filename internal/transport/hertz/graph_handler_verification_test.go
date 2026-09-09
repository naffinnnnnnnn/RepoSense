package hertz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/graph"
)

type graphBuildVerificationStub struct{ command graph.BuildCommand }

func (s *graphBuildVerificationStub) Submit(_ context.Context, command graph.BuildCommand) (graph.BuildJob, error) {
	s.command = command
	return graph.BuildJob{JobID: "job", Scope: command.Scope, Status: graph.JobPending}, nil
}

type graphQueryVerificationStub struct{ err error }

func (s *graphQueryVerificationStub) Query(context.Context, graph.Query) (graph.Result, error) {
	return graph.Result{Nodes: []graph.Entity{}, Edges: []graph.Relation{}}, s.err
}
func (s *graphQueryVerificationStub) QueryDiagnostics(context.Context, graph.DiagnosticQuery) (graph.DiagnosticResult, error) {
	return graph.DiagnosticResult{Issues: []graph.ResolutionIssue{}}, s.err
}

func TestGraphHTTPBuildUsesAuthenticatedScopeAndOnlyReturnsAcceptedJob(t *testing.T) {
	build, query := &graphBuildVerificationStub{}, &graphQueryVerificationStub{}
	handler, err := NewGraphHandler(build, query, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/repo/snapshots/snapshot/graph", strings.NewReader(`{"tenant_id":"tenant","mode":"FULL"}`))
	request.Header.Set("X-Tenant-ID", "tenant")
	request.Header.Set("X-Trace-ID", "trace")
	request.Header.Set("Idempotency-Key", "key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || build.command.Scope.TenantID != "tenant" || build.command.Scope.RepositoryID != "repo" || build.command.Scope.SnapshotID != "snapshot" || build.command.IdempotencyKey != "key" {
		t.Fatalf("status=%d command=%#v body=%s", response.Code, build.command, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/repositories/repo/snapshots/snapshot/graph", strings.NewReader(`{"tenant_id":"other","mode":"FULL"}`))
	request.Header.Set("X-Tenant-ID", "tenant")
	request.Header.Set("X-Trace-ID", "trace")
	request.Header.Set("Idempotency-Key", "key")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("forged tenant status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGraphHTTPErrorMappingIsStableAndSanitized(t *testing.T) {
	tests := []struct {
		code   graph.ErrorCode
		status int
	}{
		{graph.ErrInvalidInput, 400}, {graph.ErrRootNotFound, 404}, {graph.ErrIdempotencyConflict, 409},
		{graph.ErrQueryBudgetExceeded, 429}, {graph.ErrParserResultInvalid, 422}, {graph.ErrGraphInconsistent, 503}, {graph.ErrQueryTimeout, 504},
	}
	for _, test := range tests {
		status, payload := graphErrorResponse(&graph.DomainError{Code: test.code, Message: "safe", Retryable: test.status >= 500, Cause: errors.New("postgres://secret:password@host/source-code")})
		if status != test.status || payload.Code != string(test.code) || payload.Message != "safe" || strings.Contains(payload.Message, "password") {
			t.Fatalf("code=%s status=%d payload=%#v", test.code, status, payload)
		}
	}
}

func TestGraphHealthSeparatesLivenessStartupReadinessAndDraining(t *testing.T) {
	dependencyUp := false
	handler := NewGraphHealthHandler(nil, 100*time.Millisecond, GraphHealthCheck{Name: "postgresql", Check: func(context.Context) error {
		if !dependencyUp {
			return errors.New("down")
		}
		return nil
	}})
	assertStatus := func(path string, want int) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusServiceUnavailable)
	handler.MarkStarted()
	assertStatus("/startupz", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/livez", http.StatusOK)
	dependencyUp = true
	assertStatus("/readyz", http.StatusOK)
	handler.BeginShutdown()
	assertStatus("/readyz", http.StatusServiceUnavailable)
}
