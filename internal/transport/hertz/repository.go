package hertz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type RepositoryService interface {
	Submit(context.Context, repository.SyncCommand) (repository.ParseJob, error)
	GetJob(context.Context, common.Scope, string) (repository.ParseJob, error)
	GetSnapshot(context.Context, common.Scope) (repository.Snapshot, error)
	Artifacts(context.Context, common.Scope, string, int) ([]repository.CodeArtifact, string, error)
	Cancel(context.Context, common.Scope, string) error
	Retry(context.Context, common.Scope, string) (repository.ParseJob, error)
}
type RequestIdentity struct{ TenantID, TraceID string }
type Authenticator interface {
	Authenticate(*http.Request) (RequestIdentity, error)
}
type HeaderAuthenticator struct{}

func (HeaderAuthenticator) Authenticate(request *http.Request) (RequestIdentity, error) {
	identity := RequestIdentity{TenantID: strings.TrimSpace(request.Header.Get("X-Tenant-ID")), TraceID: strings.TrimSpace(request.Header.Get("X-Trace-ID"))}
	if identity.TenantID == "" || identity.TraceID == "" {
		return identity, errors.New("缺少租户或 Trace 身份")
	}
	return identity, nil
}

type RepositoryHandler struct {
	service RepositoryService
	auth    Authenticator
	mux     *http.ServeMux
}

func NewRepositoryHandler(service RepositoryService, auth Authenticator) (*RepositoryHandler, error) {
	if service == nil {
		return nil, errors.New("Repository Service 不能为空")
	}
	if auth == nil {
		auth = HeaderAuthenticator{}
	}
	handler := &RepositoryHandler{service: service, auth: auth, mux: http.NewServeMux()}
	handler.routes()
	return handler, nil
}
func (h *RepositoryHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(response, request)
}
func (h *RepositoryHandler) routes() {
	h.mux.HandleFunc("POST /v1/repositories/{repository_id}/parse", h.submit)
	h.mux.HandleFunc("GET /v1/repositories/{repository_id}/jobs/{job_id}", h.job)
	h.mux.HandleFunc("POST /v1/repositories/{repository_id}/jobs/{job_id}/cancel", h.cancel)
	h.mux.HandleFunc("POST /v1/repositories/{repository_id}/jobs/{job_id}/retry", h.retry)
	h.mux.HandleFunc("GET /v1/repositories/{repository_id}/snapshots/{snapshot_id}", h.snapshot)
	h.mux.HandleFunc("GET /v1/repositories/{repository_id}/snapshots/{snapshot_id}/artifacts", h.artifacts)
}

type submitRequest struct {
	RepositoryPath string   `json:"repository_path"`
	RepositoryURL  string   `json:"repository_url"`
	Provider       string   `json:"provider"`
	Ref            string   `json:"ref"`
	CredentialsRef string   `json:"credentials_ref"`
	IncludePaths   []string `json:"include_paths"`
}

func (h *RepositoryHandler) submit(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	key := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(response, http.StatusBadRequest, "Idempotency-Key 不能为空")
		return
	}
	var body submitRequest
	if err := decodeJSON(request, &body); err != nil {
		writeError(response, http.StatusBadRequest, "请求 JSON 无效")
		return
	}
	job, err := h.service.Submit(request.Context(), repository.SyncCommand{Scope: common.Scope{TenantID: identity.TenantID, RepositoryID: request.PathValue("repository_id"), TraceID: identity.TraceID}, RepositoryPath: body.RepositoryPath, RepositoryURL: body.RepositoryURL, Provider: body.Provider, Ref: body.Ref, CredentialsRef: body.CredentialsRef, IncludePaths: body.IncludePaths, IdempotencyKey: key})
	if err != nil {
		if job.JobID != "" {
			writeJSON(response, domainHTTPStatus(err), map[string]any{"error": err.Error(), "job": job})
			return
		}
		writeDomainError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, job)
}
func (h *RepositoryHandler) job(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	job, err := h.service.GetJob(request.Context(), h.scope(request, identity, ""), request.PathValue("job_id"))
	if err != nil {
		writeDomainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, job)
}
func (h *RepositoryHandler) cancel(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	if err := h.service.Cancel(request.Context(), h.scope(request, identity, ""), request.PathValue("job_id")); err != nil {
		writeDomainError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}
func (h *RepositoryHandler) retry(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	job, err := h.service.Retry(request.Context(), h.scope(request, identity, ""), request.PathValue("job_id"))
	if err != nil {
		writeDomainError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, job)
}
func (h *RepositoryHandler) snapshot(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	snapshot, err := h.service.GetSnapshot(request.Context(), h.scope(request, identity, request.PathValue("snapshot_id")))
	if err != nil {
		writeDomainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, snapshot)
}
func (h *RepositoryHandler) artifacts(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.identity(response, request)
	if !ok {
		return
	}
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			writeError(response, http.StatusBadRequest, "limit 无效")
			return
		}
		limit = value
	}
	items, next, err := h.service.Artifacts(request.Context(), h.scope(request, identity, request.PathValue("snapshot_id")), request.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeDomainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}
func (h *RepositoryHandler) identity(response http.ResponseWriter, request *http.Request) (RequestIdentity, bool) {
	identity, err := h.auth.Authenticate(request)
	if err != nil {
		writeError(response, http.StatusUnauthorized, "认证上下文无效")
		return identity, false
	}
	return identity, true
}
func (h *RepositoryHandler) scope(request *http.Request, identity RequestIdentity, snapshotID string) common.Scope {
	return common.Scope{TenantID: identity.TenantID, RepositoryID: request.PathValue("repository_id"), SnapshotID: snapshotID, TraceID: identity.TraceID}
}
func decodeJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}
func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}
func writeDomainError(response http.ResponseWriter, err error) {
	writeError(response, domainHTTPStatus(err), err.Error())
}
func domainHTTPStatus(err error) int {
	status := http.StatusInternalServerError
	var domain *repository.DomainError
	if errors.As(err, &domain) {
		switch domain.Code {
		case repository.ErrInvalidInput:
			if domain.Operation == "idempotency_conflict" || domain.Operation == "job_state_conflict" || domain.Operation == "snapshot_not_succeeded" {
				status = http.StatusConflict
			} else {
				status = http.StatusBadRequest
			}
		case repository.ErrRepositoryNotFound, repository.ErrRefNotFound:
			status = http.StatusNotFound
		case repository.ErrGitFailure, repository.ErrPersistence:
			status = http.StatusServiceUnavailable
		case repository.ErrParseFailure:
			status = http.StatusUnprocessableEntity
		}
	} else if strings.Contains(strings.ToLower(err.Error()), "不存在") {
		status = http.StatusNotFound
	}
	return status
}
