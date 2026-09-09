package verification

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGraphPublishedSchemaRequiresEveryProductionIdentityAndQualityField(t *testing.T) {
	contents := readGraphFile(t, "api", "events", "graph.published.v1.schema.json")
	type objectSchema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	var schema objectSchema
	if err := json.Unmarshal(contents, &schema); err != nil {
		t.Fatal(err)
	}
	required := make(map[string]bool, len(schema.Required))
	for _, field := range schema.Required {
		required[field] = true
	}
	for _, field := range []string{"event_id", "event_type", "aggregate_id", "occurred_at", "producer", "payload_version", "trace_id", "payload"} {
		if !required[field] || schema.Properties[field] == nil {
			t.Errorf("graph.published.v1 does not require envelope field %q", field)
		}
	}
	var payload objectSchema
	if err := json.Unmarshal(schema.Properties["payload"], &payload); err != nil {
		t.Fatalf("decode graph.published.v1 payload schema: %v", err)
	}
	required = make(map[string]bool, len(payload.Required))
	for _, field := range payload.Required {
		required[field] = true
	}
	for _, field := range []string{"tenant_id", "repository_id", "snapshot_id", "commit_sha", "revision_id", "parser_result_version", "graph_schema_version", "algorithm_version", "build_policy_version", "quality_status", "nodes", "edges", "unresolved_targets", "ambiguous_relations", "input_artifacts", "written_artifacts", "invalid_artifacts", "input_relations", "written_relations", "invalid_relations", "resolution_counts", "error_counts", "error_samples"} {
		if !required[field] || payload.Properties[field] == nil {
			t.Errorf("graph.published.v1 does not require payload field %q", field)
		}
	}
}

func TestGraphMigrationsContainControlPlaneDataPlaneAndObservabilityGates(t *testing.T) {
	postgres := string(readGraphFile(t, "migrations", "postgres", "000002_code_knowledge_graph.up.sql")) + string(readGraphFile(t, "migrations", "postgres", "000003_graph_runtime.up.sql"))
	for _, object := range []string{"graph_build_jobs", "graph_build_attempts", "graph_idempotency", "graph_revisions", "graph_active_revisions", "graph_outbox_events", "graph_rejected_events", "graph_reconciliation_runs", "request_fingerprint", "fence", "trigger_event_id"} {
		if !strings.Contains(postgres, object) {
			t.Errorf("PostgreSQL graph migration is missing %s", object)
		}
	}
	neo4j := string(readGraphFile(t, "migrations", "neo4j", "000002_graph_data_plane.up.cypher")) + string(readGraphFile(t, "migrations", "neo4j", "000003_graph_observability.up.cypher"))
	for _, object := range []string{"graph_revision_id", "graph_artifact_identity", "graph_external_identity", "graph_resolution_issue_identity", "graph_uncertain_identity", "graph_possible_target_identity", "graph_revision_candidate_status", "graph_revision_quality_status"} {
		if !strings.Contains(neo4j, object) {
			t.Errorf("Neo4j graph migration is missing %s", object)
		}
	}
}

func TestGraphProductionEntrypointHasFiveRolesAndNoMemoryRepository(t *testing.T) {
	contents := string(readGraphFile(t, "cmd", "graph", "main.go"))
	for _, role := range []string{"GraphRoleAPI", "GraphRoleConsumer", "GraphRoleWorker", "GraphRoleOutbox", "GraphRoleReconciler"} {
		if !strings.Contains(contents, role) {
			t.Errorf("production entrypoint is missing %s", role)
		}
	}
	for _, forbidden := range []string{"adapters/memory", "NewGraphRepository", "NameResolver"} {
		if strings.Contains(contents, forbidden) {
			t.Errorf("production entrypoint contains forbidden fallback %q", forbidden)
		}
	}
}

func TestGraphOpenAPIExposesAsyncBuildQueryAndDiagnostics(t *testing.T) {
	contents := string(readGraphFile(t, "api", "openapi", "reposense.yaml"))
	for _, contract := range []string{"/v1/repositories/{repository_id}/snapshots/{snapshot_id}/graph:", "'202':", "Idempotency-Key", "/v1/repositories/{repository_id}/snapshots/{snapshot_id}/graph/diagnostics:", "Query budget exhausted", "ACTIVE graph consistency unavailable"} {
		if !strings.Contains(contents, contract) {
			t.Errorf("Graph OpenAPI contract is missing %q", contract)
		}
	}
}

func readGraphFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve verification source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
	contents, err := os.ReadFile(filepath.Join(append([]string{root}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return contents
}
