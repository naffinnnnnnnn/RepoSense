package observability

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/reposense/reposense/internal/domain/graph"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestGraphTelemetryUsesOnlyBoundedLowCardinalityMetricLabelsAndSanitizedLogs(t *testing.T) {
	registry := prometheus.NewRegistry()
	var output bytes.Buffer
	observer, err := newGraphObserver(log.New(&output, "", 0), registry, noop.NewTracerProvider().Tracer("test"))
	if err != nil {
		t.Fatal(err)
	}
	attributes := map[string]string{
		"operation": "build", "status": "started", "tenant_id": "tenant-secret", "repository_id": "repo-secret",
		"snapshot_id": "snapshot-secret", "job_id": "job-secret", "attempt_id": "attempt-secret", "revision_id": "revision-secret",
		"trace_id": "0123456789abcdef0123456789abcdef", "password": "do-not-log", "source": "sensitive source code",
	}
	observer.Count("graph_worker_attempts_total", 1, attributes)
	finish := observer.Stage(context.Background(), "graph_worker", attributes)
	finish(&graph.DomainError{Code: graph.ErrGraphStoreUnavailable, Stage: "write", Dependency: "neo4j", Message: "safe", Cause: errors.New("neo4j://user:password@host/source")})
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if !strings.Contains(family.GetName(), "reposense_graph") {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				switch label.GetName() {
				case "tenant_id", "repository_id", "snapshot_id", "job_id", "attempt_id", "revision_id", "trace_id", "password", "source":
					t.Fatalf("high-cardinality or sensitive metric label leaked: %s", label.GetName())
				}
			}
		}
	}
	logText := output.String()
	for _, secret := range []string{"password", "sensitive source code", "neo4j://", "tenant-secret", "repo-secret"} {
		if strings.Contains(logText, secret) {
			t.Fatalf("structured log leaked %q: %s", secret, logText)
		}
	}
	if !strings.Contains(logText, string(graph.ErrGraphStoreUnavailable)) || !strings.Contains(logText, `"dependency":"neo4j"`) {
		t.Fatalf("structured log lost safe failure identity: %s", logText)
	}
}

func TestGraphTelemetryRejectsUnscopedOrInvalidMetricNames(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := newGraphObserver(log.New(&bytes.Buffer{}, "", 0), registry, noop.NewTracerProvider().Tracer("test"))
	if err != nil {
		t.Fatal(err)
	}
	observer.Count("tenant_specific_metric", 1, nil)
	observer.Count("graph_bad metric", 1, nil)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if strings.Contains(family.GetName(), "tenant_specific") || strings.Contains(family.GetName(), "bad_metric") {
			t.Fatalf("invalid dynamic metric was registered: %s", family.GetName())
		}
	}
}
