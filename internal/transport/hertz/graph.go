package hertz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

const maxGraphRequestBytes = 1 << 20

type GraphBuildService interface {
	Submit(context.Context, graph.BuildCommand) (graph.BuildJob, error)
}

type GraphQueryService interface {
	Query(context.Context, graph.Query) (graph.Result, error)
	QueryDiagnostics(context.Context, graph.DiagnosticQuery) (graph.DiagnosticResult, error)
}

type GraphHandler struct {
	build GraphBuildService
	query GraphQueryService
	auth  Authenticator
	mux   *http.ServeMux
}

func NewGraphHandler(build GraphBuildService, query GraphQueryService, auth Authenticator) (*GraphHandler, error) {
	if build == nil || query == nil {
		return nil, fmt.Errorf("graph build and query services are required")
	}
	if auth == nil {
		auth = HeaderAuthenticator{}
	}
	handler := &GraphHandler{build: build, query: query, auth: auth, mux: http.NewServeMux()}
	handler.routes()
	return handler, nil
}

func (h *GraphHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(response, request)
}

func (h *GraphHandler) routes() {
	h.mux.HandleFunc("POST /v1/repositories/{repository_id}/snapshots/{snapshot_id}/graph", h.submitBuild)
	h.mux.HandleFunc("GET /v1/repositories/{repository_id}/snapshots/{snapshot_id}/graph", h.queryGraph)
	h.mux.HandleFunc("GET /v1/repositories/{repository_id}/snapshots/{snapshot_id}/graph/diagnostics", h.queryDiagnostics)
}

type graphBuildRequest struct {
	TenantID    string          `json:"tenant_id,omitempty"`
	Mode        graph.BuildMode `json:"mode"`
	ArtifactIDs []string        `json:"artifact_ids,omitempty"`
}

func (h *GraphHandler) submitBuild(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.graphIdentity(response, request)
	if !ok {
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeGraphError(response, request, graphAdmissionHTTPError(graph.ErrInvalidInput, "Idempotency-Key is required"))
		return
	}
	var body graphBuildRequest
	if err := decodeGraphJSON(request, &body); err != nil {
		writeGraphError(response, request, graphAdmissionHTTPError(graph.ErrInvalidInput, "request JSON is invalid"))
		return
	}
	if body.TenantID != "" && body.TenantID != identity.TenantID {
		writeGraphError(response, request, graphAdmissionHTTPError(graph.ErrInvalidInput, "request tenant does not match the authenticated scope"))
		return
	}
	job, err := h.build.Submit(request.Context(), graph.BuildCommand{
		Scope: h.graphScope(request, identity), Mode: body.Mode, ArtifactIDs: body.ArtifactIDs, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeGraphError(response, request, err)
		return
	}
	writeJSON(response, http.StatusAccepted, job)
}

func (h *GraphHandler) queryGraph(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.graphIdentity(response, request)
	if !ok {
		return
	}
	depth, ok := graphIntegerParameter(response, request, "depth", 1, 0)
	if !ok {
		return
	}
	limit, ok := graphIntegerParameter(response, request, "limit", 500, 1)
	if !ok {
		return
	}
	result, err := h.query.Query(request.Context(), graph.Query{
		Scope: h.graphScope(request, identity), RootIDs: request.URL.Query()["root_id"],
		RelationTypes: graphRelationTypes(request.URL.Query()["relation_type"]), EntityTypes: graphEntityTypes(request.URL.Query()["entity_type"]),
		Direction: graph.Direction(request.URL.Query().Get("direction")), Depth: depth, Limit: limit,
	})
	if err != nil {
		writeGraphError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (h *GraphHandler) queryDiagnostics(response http.ResponseWriter, request *http.Request) {
	identity, ok := h.graphIdentity(response, request)
	if !ok {
		return
	}
	limit, ok := graphIntegerParameter(response, request, "limit", 100, 1)
	if !ok {
		return
	}
	result, err := h.query.QueryDiagnostics(request.Context(), graph.DiagnosticQuery{
		Scope: h.graphScope(request, identity), ArtifactIDs: request.URL.Query()["artifact_id"], Limit: limit,
	})
	if err != nil {
		writeGraphError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, result)
}

func (h *GraphHandler) graphIdentity(response http.ResponseWriter, request *http.Request) (RequestIdentity, bool) {
	identity, err := h.auth.Authenticate(request)
	if err != nil || identity.TenantID == "" || identity.TraceID == "" || identity.TenantID != strings.TrimSpace(identity.TenantID) || identity.TraceID != strings.TrimSpace(identity.TraceID) {
		writeJSON(response, http.StatusUnauthorized, graphErrorEnvelope{Error: graphAPIError{Code: "UNAUTHENTICATED", Message: "authentication context is invalid", Retryable: false}})
		return identity, false
	}
	return identity, true
}

func (h *GraphHandler) graphScope(request *http.Request, identity RequestIdentity) common.Scope {
	return common.Scope{
		TenantID: identity.TenantID, RepositoryID: request.PathValue("repository_id"),
		SnapshotID: request.PathValue("snapshot_id"), TraceID: identity.TraceID,
	}
}

func decodeGraphJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxGraphRequestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain one JSON value")
	}
	return nil
}

func graphIntegerParameter(response http.ResponseWriter, request *http.Request, name string, defaultValue, minimum int) (int, bool) {
	raw := request.URL.Query().Get(name)
	if raw == "" {
		return defaultValue, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		writeGraphError(response, request, graphAdmissionHTTPError(graph.ErrInvalidInput, name+" is invalid"))
		return 0, false
	}
	return value, true
}

func graphRelationTypes(values []string) []repository.RelationKind {
	result := make([]repository.RelationKind, len(values))
	for index, value := range values {
		result[index] = repository.RelationKind(value)
	}
	return result
}

func graphEntityTypes(values []string) []graph.EntityType {
	result := make([]graph.EntityType, len(values))
	for index, value := range values {
		result[index] = graph.EntityType(value)
	}
	return result
}

type graphAPIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type graphErrorEnvelope struct {
	Error graphAPIError `json:"error"`
}

func writeGraphError(response http.ResponseWriter, request *http.Request, err error) {
	status, apiError := graphErrorResponse(err)
	if request.Context().Err() != nil {
		return
	}
	writeJSON(response, status, graphErrorEnvelope{Error: apiError})
}

func graphErrorResponse(err error) (int, graphAPIError) {
	var domainErr *graph.DomainError
	if errors.As(err, &domainErr) {
		status := graphHTTPStatus(domainErr.Code)
		message := strings.TrimSpace(domainErr.Message)
		if message == "" {
			message = "graph request failed"
		}
		return status, graphAPIError{Code: string(domainErr.Code), Message: message, Retryable: domainErr.Retryable}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, graphAPIError{Code: string(graph.ErrQueryTimeout), Message: "graph request timed out", Retryable: true}
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, graphAPIError{Code: string(graph.ErrQueryCancelled), Message: "graph request was cancelled", Retryable: false}
	}
	return http.StatusInternalServerError, graphAPIError{Code: "INTERNAL_ERROR", Message: "internal graph service error", Retryable: false}
}

func graphHTTPStatus(code graph.ErrorCode) int {
	switch code {
	case graph.ErrInvalidInput, graph.ErrUnsupportedBuildMode, graph.ErrPartialBuildNotAllowed:
		return http.StatusBadRequest
	case graph.ErrSnapshotNotFound, graph.ErrRevisionNotFound, graph.ErrRootNotFound:
		return http.StatusNotFound
	case graph.ErrIdempotencyConflict, graph.ErrConflict, graph.ErrActivationConflict:
		return http.StatusConflict
	case graph.ErrBuildCapacityExceeded, graph.ErrQueryBudgetExceeded:
		return http.StatusTooManyRequests
	case graph.ErrQueryTimeout, graph.ErrBuildTimeout:
		return http.StatusGatewayTimeout
	case graph.ErrQueryCancelled, graph.ErrWorkerShutdown, graph.ErrBuildCancelled:
		return http.StatusRequestTimeout
	case graph.ErrParserResultInvalid, graph.ErrParserResultChanged, graph.ErrGraphValidationFailed:
		return http.StatusUnprocessableEntity
	case graph.ErrParserResultUnavailable, graph.ErrGraphInconsistent, graph.ErrGraphStoreUnavailable,
		graph.ErrGraphBatchWriteFailed, graph.ErrControlStoreUnavailable, graph.ErrActivationFailed,
		graph.ErrActivationOutcomeUnknown, graph.ErrPersistence, graph.ErrEventPublishFailed,
		graph.ErrLeaseLost, graph.ErrBuildFailure:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func graphAdmissionHTTPError(code graph.ErrorCode, message string) error {
	return &graph.DomainError{Code: code, Operation: "graph_http", Stage: "request", Dependency: "http", Message: message, Retryable: false}
}
