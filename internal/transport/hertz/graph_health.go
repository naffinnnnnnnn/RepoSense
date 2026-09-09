package hertz

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

type GraphHealthCheck struct {
	Name  string
	Check func(context.Context) error
}

type GraphHealthHandler struct {
	mux     *http.ServeMux
	checks  []GraphHealthCheck
	timeout time.Duration
	started atomic.Bool
	closing atomic.Bool
}

func NewGraphHealthHandler(metrics http.Handler, timeout time.Duration, checks ...GraphHealthCheck) *GraphHealthHandler {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	handler := &GraphHealthHandler{mux: http.NewServeMux(), checks: append([]GraphHealthCheck(nil), checks...), timeout: timeout}
	sort.Slice(handler.checks, func(i, j int) bool { return handler.checks[i].Name < handler.checks[j].Name })
	handler.mux.HandleFunc("GET /livez", handler.liveness)
	handler.mux.HandleFunc("GET /readyz", handler.readiness)
	handler.mux.HandleFunc("GET /startupz", handler.startup)
	if metrics != nil {
		handler.mux.Handle("GET /metrics", metrics)
	}
	return handler
}

func (h *GraphHealthHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(response, request)
}

func (h *GraphHealthHandler) MarkStarted()   { h.started.Store(true) }
func (h *GraphHealthHandler) BeginShutdown() { h.closing.Store(true) }

func (h *GraphHealthHandler) liveness(response http.ResponseWriter, _ *http.Request) {
	writeHealth(response, http.StatusOK, "alive", nil)
}

func (h *GraphHealthHandler) startup(response http.ResponseWriter, _ *http.Request) {
	if !h.started.Load() {
		writeHealth(response, http.StatusServiceUnavailable, "starting", nil)
		return
	}
	writeHealth(response, http.StatusOK, "started", nil)
}

func (h *GraphHealthHandler) readiness(response http.ResponseWriter, request *http.Request) {
	if !h.started.Load() || h.closing.Load() {
		writeHealth(response, http.StatusServiceUnavailable, "not_ready", nil)
		return
	}
	failed := make([]string, 0)
	for _, check := range h.checks {
		if check.Name == "" || check.Check == nil {
			failed = append(failed, "invalid_check")
			continue
		}
		ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
		err := check.Check(ctx)
		cancel()
		if err != nil {
			failed = append(failed, check.Name)
		}
	}
	if len(failed) != 0 {
		writeHealth(response, http.StatusServiceUnavailable, "not_ready", failed)
		return
	}
	writeHealth(response, http.StatusOK, "ready", nil)
}

func writeHealth(response http.ResponseWriter, status int, state string, failed []string) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	payload := struct {
		Status string   `json:"status"`
		Failed []string `json:"failed_dependencies,omitempty"`
	}{Status: state, Failed: failed}
	_ = json.NewEncoder(response).Encode(payload)
}
