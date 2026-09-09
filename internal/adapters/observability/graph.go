package observability

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var graphMetricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
var graphTraceIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)

var graphMetricLabelNames = []string{"operation", "status", "error_code", "stage", "dependency", "build_reason", "action", "quality_status"}

type GraphTelemetry struct {
	Observer *GraphObserver
	Metrics  http.Handler
	provider *sdktrace.TracerProvider
}

// NewGraphTelemetry creates one isolated registry and one OTLP trace provider
// per Graph role. The exporter uses HTTPS and never writes span bodies or
// dependency errors to process logs.
func NewGraphTelemetry(ctx context.Context, serviceName, endpoint string, logger *log.Logger) (*GraphTelemetry, error) {
	if strings.TrimSpace(serviceName) == "" || strings.TrimSpace(endpoint) == "" {
		return nil, fmt.Errorf("Graph telemetry service name and OTLP endpoint are required")
	}
	if logger == nil {
		logger = log.New(os.Stderr, "", 0)
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		logger.Print(`{"level":"ERROR","message":"graph telemetry export failed"}`)
	}))
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("initialize Graph OTLP exporter: %w", err)
	}
	// Do not inherit arbitrary OTEL_RESOURCE_ATTRIBUTES: a deployment-wide
	// environment entry may contain tenant or secret material outside Graph's
	// controlled telemetry contract.
	res := resource.NewSchemaless(attribute.String("service.name", serviceName))
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	// Only W3C trace context crosses service boundaries. Arbitrary baggage can
	// contain user-controlled or sensitive values and is deliberately dropped.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	observer, err := newGraphObserver(logger, registry, provider.Tracer("github.com/reposense/reposense/graph"))
	if err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	return &GraphTelemetry{Observer: observer, Metrics: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), provider: provider}, nil
}

func (t *GraphTelemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

type GraphObserver struct {
	logger     *log.Logger
	tracer     trace.Tracer
	registry   *prometheus.Registry
	durations  *prometheus.HistogramVec
	operations *prometheus.CounterVec

	mu       sync.Mutex
	counters map[string]*prometheus.CounterVec
	gauges   map[string]*prometheus.GaugeVec
}

func newGraphObserver(logger *log.Logger, registry *prometheus.Registry, tracer trace.Tracer) (*GraphObserver, error) {
	if logger == nil {
		logger = log.Default()
	}
	durations := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "reposense_graph_stage_duration_seconds", Help: "Duration of Code Knowledge Graph stages.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "status", "error_code", "stage", "dependency"})
	if err := registry.Register(durations); err != nil {
		return nil, err
	}
	operations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "reposense_graph_stage_operations_total", Help: "Completed Code Knowledge Graph stage operations.",
	}, []string{"operation", "status", "error_code", "stage", "dependency"})
	if err := registry.Register(operations); err != nil {
		return nil, err
	}
	return &GraphObserver{logger: logger, tracer: tracer, registry: registry, durations: durations, operations: operations,
		counters: map[string]*prometheus.CounterVec{}, gauges: map[string]*prometheus.GaugeVec{}}, nil
}

func (o *GraphObserver) Stage(ctx context.Context, name string, attributes map[string]string) func(error) {
	_, finish := o.StartStage(ctx, name, attributes)
	return finish
}

// StartStage is an optional extension consumed by the Graph application. It
// returns the span-bearing context without changing the shared Observer port.
func (o *GraphObserver) StartStage(ctx context.Context, name string, attributes map[string]string) (context.Context, func(error)) {
	if o == nil {
		return ctx, func(error) {}
	}
	if attributes != nil && (!trace.SpanContextFromContext(ctx).IsValid() || attributes["trace_root"] == "true") {
		ctx = contextWithGraphTrace(ctx, name, attributes)
	}
	started := time.Now()
	spanCtx, span := o.tracer.Start(ctx, name, trace.WithAttributes(traceAttributes(attributes)...))
	return spanCtx, func(err error) {
		metadata := graphErrorMetadata(err)
		labels := metricLabels(attributes)
		labels["stage"] = firstExact(metadata.stage, labels["stage"], name)
		labels["status"] = "succeeded"
		if err != nil {
			labels["status"] = "failed"
			labels["error_code"] = firstExact(metadata.code, labels["error_code"])
			labels["dependency"] = firstExact(metadata.dependency, labels["dependency"])
			span.SetStatus(codes.Error, metadata.code)
			span.RecordError(errors.New(firstExact(metadata.code, "operation_failed")))
		}
		o.durations.WithLabelValues(labels["operation"], labels["status"], labels["error_code"], labels["stage"], labels["dependency"]).Observe(time.Since(started).Seconds())
		o.operations.WithLabelValues(labels["operation"], labels["status"], labels["error_code"], labels["stage"], labels["dependency"]).Inc()
		span.SetAttributes(attribute.String("reposense.status", labels["status"]), attribute.String("reposense.error_code", labels["error_code"]), attribute.String("reposense.stage", labels["stage"]), attribute.String("reposense.dependency", labels["dependency"]))
		span.End()
		o.writeStageLog(spanCtx, time.Since(started), labels)
	}
}

func (o *GraphObserver) Count(name string, value int64, attributes map[string]string) {
	if o == nil || value < 0 {
		return
	}
	collector := o.counter(name)
	if collector == nil {
		return
	}
	collector.WithLabelValues(metricLabelValues(attributes)...).Add(float64(value))
}

func (o *GraphObserver) Gauge(name string, value float64, attributes map[string]string) {
	if o == nil {
		return
	}
	collector := o.gauge(name)
	if collector == nil {
		return
	}
	collector.WithLabelValues(metricLabelValues(attributes)...).Set(value)
}

func (o *GraphObserver) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		ctx, finish := o.StartStage(ctx, "graph_http", map[string]string{"operation": normalizedHTTPMethod(request.Method)})
		writer := &graphStatusWriter{ResponseWriter: response, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				finish(errors.New("http_handler_panic"))
				panic(recovered)
			}
			if writer.status >= http.StatusInternalServerError {
				finish(errors.New("http_server_error"))
			} else {
				finish(nil)
			}
		}()
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

type graphStatusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *graphStatusWriter) Write(payload []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(payload)
}

func (w *graphStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *graphStatusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (o *GraphObserver) counter(name string) *prometheus.CounterVec {
	name = prometheusName(name)
	if name == "" {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if collector := o.counters[name]; collector != nil {
		return collector
	}
	collector := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: "Code Knowledge Graph counter " + name + "."}, graphMetricLabelNames)
	if err := o.registry.Register(collector); err != nil {
		return nil
	}
	o.counters[name] = collector
	return collector
}

func (o *GraphObserver) gauge(name string) *prometheus.GaugeVec {
	name = prometheusName(name)
	if name == "" {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if collector := o.gauges[name]; collector != nil {
		return collector
	}
	collector := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: "Code Knowledge Graph gauge " + name + "."}, graphMetricLabelNames)
	if err := o.registry.Register(collector); err != nil {
		return nil
	}
	o.gauges[name] = collector
	return collector
}

func (o *GraphObserver) writeStageLog(ctx context.Context, duration time.Duration, labels map[string]string) {
	fields := map[string]any{"level": "INFO", "message": "graph stage completed", "stage": labels["stage"],
		"duration_ms": duration.Milliseconds(), "status": labels["status"]}
	if labels["operation"] != "" {
		fields["operation"] = labels["operation"]
	}
	if labels["error_code"] != "" {
		fields["error_code"] = labels["error_code"]
	}
	if labels["dependency"] != "" {
		fields["dependency"] = labels["dependency"]
	}
	if labels["status"] == "failed" {
		fields["level"] = "ERROR"
	}
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		fields["otel_trace_id"], fields["otel_span_id"] = spanContext.TraceID().String(), spanContext.SpanID().String()
	}
	encoded, _ := json.Marshal(fields)
	o.logger.Print(string(encoded))
}

type errorMetadata struct{ code, stage, dependency string }

func graphErrorMetadata(err error) errorMetadata {
	if err == nil {
		return errorMetadata{}
	}
	var graphErr *graph.DomainError
	if errors.As(err, &graphErr) {
		return errorMetadata{code: string(graphErr.Code), stage: graphErr.Stage, dependency: graphErr.Dependency}
	}
	var repositoryErr *repository.DomainError
	if errors.As(err, &repositoryErr) {
		return errorMetadata{code: string(repositoryErr.Code), stage: repositoryErr.Operation}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorMetadata{code: "TIMEOUT", stage: "context"}
	}
	if errors.Is(err, context.Canceled) {
		return errorMetadata{code: "CANCELLED", stage: "context"}
	}
	return errorMetadata{code: "OPERATION_FAILED"}
}

func metricLabels(attributes map[string]string) map[string]string {
	labels := make(map[string]string, len(graphMetricLabelNames))
	for _, name := range graphMetricLabelNames {
		labels[name] = ""
	}
	for name, value := range attributes {
		if _, ok := labels[name]; ok {
			labels[name] = boundedLabel(value)
		}
	}
	return labels
}

func metricLabelValues(attributes map[string]string) []string {
	labels := metricLabels(attributes)
	values := make([]string, 0, len(graphMetricLabelNames))
	for _, name := range graphMetricLabelNames {
		values = append(values, labels[name])
	}
	return values
}

func traceAttributes(attributes map[string]string) []attribute.KeyValue {
	allowed := map[string]bool{"operation": true, "status": true, "error_code": true, "stage": true, "dependency": true, "build_reason": true, "action": true, "quality_status": true,
		"tenant_id": true, "repository_id": true, "snapshot_id": true, "job_id": true, "attempt_id": true, "revision_id": true, "trace_id": true}
	values := make([]attribute.KeyValue, 0, len(attributes))
	for name, value := range attributes {
		if allowed[name] && value != "" {
			bounded, ok := boundedTraceValue(name, value)
			if ok {
				values = append(values, attribute.String("reposense."+name, bounded))
			}
		}
	}
	return values
}

func prometheusName(name string) string {
	name = strings.TrimSpace(name)
	if !strings.HasPrefix(name, "graph_") || !graphMetricName.MatchString(name) {
		return ""
	}
	return "reposense_" + name
}

func boundedLabel(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 128 {
		return "invalid"
	}
	return value
}

func boundedTraceValue(name, value string) (string, bool) {
	value = strings.TrimSpace(value)
	switch name {
	case "tenant_id", "repository_id", "snapshot_id", "job_id", "attempt_id", "revision_id", "trace_id":
		if !graphTraceIdentity.MatchString(value) {
			return "", false
		}
		return value, true
	}
	if len(value) > 256 {
		return value[:256], true
	}
	return value, value != ""
}

func normalizedHTTPMethod(method string) string {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet:
		return "get"
	case http.MethodPost:
		return "post"
	case http.MethodPut:
		return "put"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	case http.MethodHead:
		return "head"
	case http.MethodOptions:
		return "options"
	default:
		return "other"
	}
}

// contextWithGraphTrace restores trace correlation after an asynchronous
// PostgreSQL boundary. The persisted Graph TraceID is used only when it is a
// valid W3C TraceID; a synthetic remote parent keeps independently deployed
// Consumer, Worker and Outbox spans in the same trace without persisting a
// fabricated span as application data.
func contextWithGraphTrace(ctx context.Context, stage string, attributes map[string]string) context.Context {
	traceID, err := trace.TraceIDFromHex(attributes["trace_id"])
	if err != nil || !traceID.IsValid() {
		return ctx
	}
	digest := sha256.Sum256([]byte(stage + "\x00" + attributes["job_id"] + "\x00" + attributes["revision_id"]))
	var spanID trace.SpanID
	copy(spanID[:], digest[:len(spanID)])
	if !spanID.IsValid() {
		return ctx
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, parent)
}

func firstExact(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
