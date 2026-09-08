package neo4j

import (
	"context"
	"fmt"
	"sort"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func (s *GraphDataStore) QueryRevisionDiagnostics(ctx context.Context, revisionID string, query graph.DiagnosticQuery) (graph.DiagnosticResult, error) {
	if err := query.Validate(); err != nil {
		return graph.DiagnosticResult{}, invalidInput("query_revision_diagnostics", "query", err.Error(), err)
	}
	if revisionID == "" {
		return graph.DiagnosticResult{}, invalidInput("query_revision_diagnostics", "query", "revision_id is required", nil)
	}
	if err := s.config.Validate(); err != nil {
		return graph.DiagnosticResult{}, invalidInput("query_revision_diagnostics", "query", "graph query budgets are invalid", err)
	}
	roots := uniqueSorted(query.ArtifactIDs)
	if len(roots) > s.config.MaxRoots {
		return graph.DiagnosticResult{}, queryBudgetError("query_revision_diagnostics", "artifact count exceeds the configured diagnostic query budget")
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > s.config.MaxNodes {
		return graph.DiagnosticResult{}, queryBudgetError("query_revision_diagnostics", "requested issue limit exceeds the configured query budget")
	}
	value, err := s.read(ctx, "query_revision_diagnostics", "query", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		if err := requireSealedRevision(ctx, tx, query.Scope, revisionID); err != nil {
			return nil, err
		}
		if err := requireDiagnosticRoots(ctx, tx, revisionID, roots); err != nil {
			return nil, err
		}
		result, err := tx.Run(ctx, `
MATCH (source:CodeEntity)-[edge:UNCERTAIN_RELATION]->(issue:ResolutionIssue)
WHERE source.revision_id=$revision_id AND edge.revision_id=$revision_id
  AND issue.revision_id=$revision_id AND source.artifact_id IN $roots
OPTIONAL MATCH (issue)-[possible:POSSIBLE_TARGET]->(candidate:CodeEntity)
WHERE possible.revision_id=$revision_id AND candidate.revision_id=$revision_id
WITH source,edge,issue,collect(candidate.artifact_id) AS candidates
RETURN issue.issue_id AS issue_id,edge.relation_id AS relation_id,
  source.artifact_id AS source_artifact_id,edge.intended_kind AS intended_kind,
  issue.resolution_status AS resolution_status,issue.raw_target AS raw_target,
  issue.reason_code AS reason_code,edge.confidence AS confidence,
  edge.evidence_commit_sha AS evidence_commit_sha,edge.evidence_path AS evidence_path,
  edge.evidence_symbol_id AS evidence_symbol_id,edge.evidence_start_line AS evidence_start_line,
  edge.evidence_end_line AS evidence_end_line,edge.evidence_content_hash AS evidence_content_hash,
  candidates ORDER BY issue_id LIMIT $limit`, map[string]any{"revision_id": revisionID, "roots": roots, "limit": limit + 1})
		if err != nil {
			return nil, err
		}
		issues := make([]graph.ResolutionIssue, 0)
		for result.Next(ctx) {
			values := result.Record().AsMap()
			confidence, _ := values["confidence"].(float64)
			candidates := stringSlice(values["candidates"])
			sort.Strings(candidates)
			issues = append(issues, graph.ResolutionIssue{
				IssueID: stringValue(values["issue_id"]), RelationID: stringValue(values["relation_id"]),
				SourceArtifactID: stringValue(values["source_artifact_id"]), IntendedKind: repository.RelationKind(stringValue(values["intended_kind"])),
				ResolutionStatus: graph.ResolutionStatus(stringValue(values["resolution_status"])), RawTargetSymbol: stringValue(values["raw_target"]),
				ReasonCode: stringValue(values["reason_code"]), Confidence: confidence, CandidateArtifactIDs: candidates,
				Evidence: common.SourceRef{CommitSHA: stringValue(values["evidence_commit_sha"]), Path: stringValue(values["evidence_path"]), SymbolID: stringValue(values["evidence_symbol_id"]), StartLine: intValue(values["evidence_start_line"]), EndLine: intValue(values["evidence_end_line"]), ContentHash: stringValue(values["evidence_content_hash"])},
			})
		}
		if err := result.Err(); err != nil {
			return nil, err
		}
		return issues, nil
	})
	if err != nil {
		return graph.DiagnosticResult{}, err
	}
	issues, ok := value.([]graph.ResolutionIssue)
	if !ok {
		return graph.DiagnosticResult{}, graphStoreError("query_revision_diagnostics", "query", fmt.Errorf("neo4j returned an invalid diagnostic result"))
	}
	truncated := len(issues) > limit
	if truncated {
		issues = issues[:limit]
	}
	return graph.DiagnosticResult{RevisionID: revisionID, Issues: issues, Truncated: truncated}, nil
}

func requireDiagnosticRoots(ctx context.Context, tx neo4jdriver.ManagedTransaction, revisionID string, roots []string) error {
	result, err := tx.Run(ctx, `UNWIND $roots AS root OPTIONAL MATCH (n:CodeEntity {revision_id:$revision_id,artifact_id:root}) RETURN count(n) AS found`, map[string]any{"revision_id": revisionID, "roots": roots})
	if err != nil {
		return err
	}
	record, err := result.Single(ctx)
	if err != nil {
		return err
	}
	if intValue(record.AsMap()["found"]) != len(roots) {
		return &graph.DomainError{Code: graph.ErrRootNotFound, Operation: "query_revision_diagnostics", Stage: "query", Dependency: "neo4j", Message: "one or more diagnostic root artifacts were not found", Retryable: false}
	}
	return nil
}

func stringSlice(value any) []string {
	if items, ok := value.([]string); ok {
		return append([]string(nil), items...)
	}
	items, _ := value.([]any)
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok && text != "" {
			result = append(result, text)
		}
	}
	return result
}
