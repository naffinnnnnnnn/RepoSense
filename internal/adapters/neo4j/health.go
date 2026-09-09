package neo4j

import (
	"context"
	"fmt"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/graph"
)

func (s *GraphDataStore) Health(ctx context.Context) error {
	if s == nil || s.driver == nil {
		return fmt.Errorf("graph Neo4j driver is not configured")
	}
	return s.driver.VerifyConnectivity(ctx)
}

// GraphOperationalMetrics reports bounded candidate and quality state. It is
// collected by graph-reconciler, whose data-plane account already needs to
// inspect revisions; API and Worker accounts do not gain monitoring grants.
func (s *GraphDataStore) GraphOperationalMetrics(ctx context.Context) (map[string]float64, error) {
	if err := s.Health(ctx); err != nil {
		return nil, err
	}
	session := s.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: s.database})
	defer session.Close(ctx)
	value, err := session.ExecuteRead(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `MATCH (r:GraphRevision)
RETURN count(CASE WHEN r.candidate_status=$staging THEN 1 END) AS staging,
count(CASE WHEN r.candidate_status=$sealed THEN 1 END) AS sealed,
count(CASE WHEN r.candidate_status=$failed THEN 1 END) AS failed,
count(CASE WHEN r.quality_status=$healthy THEN 1 END) AS healthy,
count(CASE WHEN r.quality_status=$degraded THEN 1 END) AS degraded`, map[string]any{
			"staging": string(graph.CandidateStaging), "sealed": string(graph.CandidateSealed),
			"failed": string(graph.CandidateFailed), "healthy": string(graph.QualityHealthy),
			"degraded": string(graph.QualityDegraded),
		})
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, err
		}
		return record.AsMap(), nil
	})
	if err != nil {
		return nil, err
	}
	values, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Graph Neo4j metrics result is invalid")
	}
	return map[string]float64{
		"graph_staging_revisions":  float64(intValue(values["staging"])),
		"graph_sealed_revisions":   float64(intValue(values["sealed"])),
		"graph_failed_revisions":   float64(intValue(values["failed"])),
		"graph_healthy_revisions":  float64(intValue(values["healthy"])),
		"graph_degraded_revisions": float64(intValue(values["degraded"])),
	}, nil
}

func (s *GraphDataStore) VerifyGraphSchema(ctx context.Context) error {
	if err := s.Health(ctx); err != nil {
		return err
	}
	required := map[string]bool{
		"graph_revision_id": false, "graph_external_identity": false,
		"graph_artifact_identity": false, "graph_resolution_issue_identity": false,
		"graph_contains_identity": false, "graph_imports_identity": false,
		"graph_calls_identity": false, "graph_extends_identity": false,
		"graph_implements_identity": false, "graph_depends_on_identity": false,
		"graph_uncertain_identity": false, "graph_possible_target_identity": false,
	}
	requiredIndexes := map[string]bool{
		"graph_revision_candidate_status": false,
		"graph_revision_quality_status":   false,
	}
	session := s.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: s.database})
	defer session.Close(ctx)
	if err := verifyGraphSchemaNames(ctx, session, `SHOW CONSTRAINTS YIELD name WHERE name IN $names RETURN name`, required); err != nil {
		return err
	}
	if err := verifyGraphSchemaNames(ctx, session, `SHOW INDEXES YIELD name WHERE name IN $names RETURN name`, requiredIndexes); err != nil {
		return err
	}
	return nil
}

func verifyGraphSchemaNames(ctx context.Context, session neo4jdriver.Session, query string, required map[string]bool) error {
	_, err := session.ExecuteRead(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, map[string]any{"names": graphSchemaNames(required)})
		if err != nil {
			return nil, err
		}
		for result.Next(ctx) {
			name, _ := result.Record().Get("name")
			if text, ok := name.(string); ok {
				required[text] = true
			}
		}
		return nil, result.Err()
	})
	if err != nil {
		return err
	}
	for name, present := range required {
		if !present {
			return fmt.Errorf("Graph Neo4j schema is incompatible: required schema object %s is missing", name)
		}
	}
	return nil
}

func graphSchemaNames(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
