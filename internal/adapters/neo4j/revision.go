package neo4j

import (
	"context"
	"fmt"
	"strings"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/graph"
)

// VerifyRevision proves that the PostgreSQL ACTIVE revision identifies the
// same immutable SEALED graph in Neo4j. PostgreSQL remains the publication
// authority; Neo4j is never searched for a replacement or "latest" graph.
func (s *GraphDataStore) VerifyRevision(ctx context.Context, expected graph.Revision) error {
	if err := validateExpectedRevision(expected); err != nil {
		return invalidInput("verify_revision", "query", err.Error(), err)
	}
	value, err := s.read(ctx, "verify_revision", "query", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `MATCH (r:GraphRevision {revision_id:$revision_id})
RETURN count(r) AS matches,head(collect(r{
  .tenant_id,.repository_id,.snapshot_id,.commit_sha,.candidate_status,
  .parser_result_version,.graph_schema_version,.graph_algorithm_version,.build_policy_version,
  .quality_status,.node_count,.edge_count,.unresolved_count,.ambiguous_count
})) AS revision`, map[string]any{"revision_id": expected.RevisionID})
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, err
		}
		values := record.AsMap()
		if intValue(values["matches"]) != 1 {
			return nil, revisionNotFound("verify_revision", nil)
		}
		stored, ok := values["revision"].(map[string]any)
		if !ok {
			return nil, inconsistentRevision("neo4j returned invalid revision metadata", nil)
		}
		if !revisionMetadataMatches(stored, expected) {
			return nil, inconsistentRevision("active revision metadata does not match the sealed graph", nil)
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	if value != nil {
		return inconsistentRevision("neo4j returned an unexpected revision verification result", nil)
	}
	return nil
}

func validateExpectedRevision(revision graph.Revision) error {
	for name, value := range map[string]string{
		"revision_id": revision.RevisionID, "tenant_id": revision.TenantID,
		"repository_id": revision.RepositoryID, "snapshot_id": revision.SnapshotID,
		"commit_sha": revision.CommitSHA, "parser_result_version": revision.ParserResultVersion,
		"graph_schema_version": revision.GraphSchemaVersion, "graph_algorithm_version": revision.AlgorithmVersion,
		"build_policy_version": revision.BuildPolicyVersion,
	} {
		if value == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("%s must be an exact non-empty identity", name)
		}
	}
	if revision.BuildStatus != graph.RevisionActive {
		return fmt.Errorf("revision must be ACTIVE in the control plane")
	}
	if revision.Stats.Nodes < 0 || revision.Stats.Edges < 0 || revision.Stats.UnresolvedTargets < 0 || revision.Stats.AmbiguousRelations < 0 {
		return fmt.Errorf("revision statistics must not be negative")
	}
	return nil
}

func revisionMetadataMatches(stored map[string]any, expected graph.Revision) bool {
	identities := map[string]string{
		"tenant_id": expected.TenantID, "repository_id": expected.RepositoryID,
		"snapshot_id": expected.SnapshotID, "commit_sha": expected.CommitSHA,
		"candidate_status": string(graph.CandidateSealed), "parser_result_version": expected.ParserResultVersion,
		"graph_schema_version": expected.GraphSchemaVersion, "graph_algorithm_version": expected.AlgorithmVersion,
		"build_policy_version": expected.BuildPolicyVersion, "quality_status": string(expected.QualityStatus),
	}
	for key, wanted := range identities {
		if stringValue(stored[key]) != wanted {
			return false
		}
	}
	counts := map[string]int{
		"node_count": expected.Stats.Nodes, "edge_count": expected.Stats.Edges,
		"unresolved_count": expected.Stats.UnresolvedTargets, "ambiguous_count": expected.Stats.AmbiguousRelations,
	}
	for key, wanted := range counts {
		if intValue(stored[key]) != wanted {
			return false
		}
	}
	return true
}

func inconsistentRevision(message string, cause error) error {
	return &graph.DomainError{Code: graph.ErrGraphInconsistent, Operation: "verify_revision", Stage: "query", Dependency: "neo4j", Message: message, Retryable: true, Cause: cause}
}
