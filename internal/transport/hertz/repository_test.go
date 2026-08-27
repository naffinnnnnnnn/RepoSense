package hertz

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type repositoryServiceStub struct {
	submitted repository.SyncCommand
	job       repository.ParseJob
	err       error
}

func (s *repositoryServiceStub) Submit(_ context.Context, command repository.SyncCommand) (repository.ParseJob, error) {
	s.submitted = command
	return s.job, s.err
}
func (*repositoryServiceStub) GetJob(context.Context, common.Scope, string) (repository.ParseJob, error) {
	return repository.ParseJob{}, errors.New("unused")
}
func (*repositoryServiceStub) GetSnapshot(context.Context, common.Scope) (repository.Snapshot, error) {
	return repository.Snapshot{}, errors.New("unused")
}
func (*repositoryServiceStub) Artifacts(context.Context, common.Scope, string, int) ([]repository.CodeArtifact, string, error) {
	return nil, "", errors.New("unused")
}
func (*repositoryServiceStub) Cancel(context.Context, common.Scope, string) error {
	return errors.New("unused")
}
func (*repositoryServiceStub) Retry(context.Context, common.Scope, string) (repository.ParseJob, error) {
	return repository.ParseJob{}, errors.New("unused")
}

func TestParseEndpointReturns202WithoutExecutingAndUsesAuthenticatedScope(t *testing.T) {
	service := &repositoryServiceStub{job: repository.ParseJob{JobID: "job", SnapshotID: "snap", Status: repository.StatusPending}}
	handler, err := NewRepositoryHandler(service, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/repo/parse", strings.NewReader(`{"repository_path":"D:/repo","provider":"local","ref":"main"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "key")
	request.Header.Set("X-Tenant-ID", "tenant")
	request.Header.Set("X-Trace-ID", "trace")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if service.submitted.Scope.TenantID != "tenant" || service.submitted.Scope.RepositoryID != "repo" || service.submitted.Scope.TraceID != "trace" || service.submitted.IdempotencyKey != "key" {
		t.Fatalf("提交作用域错误：%#v", service.submitted)
	}
	var job repository.ParseJob
	if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil || job.JobID != "job" {
		t.Fatalf("响应错误：%#v %v", job, err)
	}
}

func TestParseEndpointMapsIdempotencyConflictTo409(t *testing.T) {
	service := &repositoryServiceStub{err: &repository.DomainError{Code: repository.ErrInvalidInput, Operation: "idempotency_conflict", Message: "conflict"}}
	handler, _ := NewRepositoryHandler(service, nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/repositories/repo/parse", strings.NewReader(`{"repository_path":"D:/repo","ref":"main"}`))
	request.Header.Set("Idempotency-Key", "key")
	request.Header.Set("X-Tenant-ID", "tenant")
	request.Header.Set("X-Trace-ID", "trace")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
