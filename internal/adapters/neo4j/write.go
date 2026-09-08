package neo4j

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func (s *GraphDataStore) WriteArtifactBatch(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, artifacts []repository.CodeArtifact) error {
	if err := validateBuildIdentity(job, attempt); err != nil {
		return err
	}
	rows, err := artifactRows(artifacts)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	for _, artifact := range artifacts {
		if artifact.SourceRef.CommitSHA != job.CommitSHA {
			return validationError("write_artifact_batch", "write_artifacts", "artifact source commit does not match the graph candidate")
		}
	}
	_, err = s.write(ctx, "write_artifact_batch", "write_artifacts", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		if err := requireCandidateStage(ctx, tx, job, attempt, "WRITING"); err != nil {
			return nil, err
		}
		result, err := tx.Run(ctx, `
UNWIND $rows AS row
MERGE (n:CodeEntity {revision_id:$revision_id, artifact_id:row.artifact_id})
ON CREATE SET n.tenant_id=$tenant_id, n.repository_id=$repository_id,
  n.snapshot_id=$snapshot_id, n.node_id=row.artifact_id,
  n.entity_type=row.entity_type, n.name=row.name,
  n.qualified_name=row.qualified_name, n.language=row.language,
  n.signature=row.signature, n.content_hash=row.content_hash,
  n.attributes_json=row.attributes_json,
  n.source_commit_sha=row.source_commit_sha, n.source_path=row.source_path,
  n.source_symbol_id=row.source_symbol_id, n.source_start_line=row.source_start_line,
  n.source_end_line=row.source_end_line, n.source_content_hash=row.source_content_hash,
  n.record_hash=row.record_hash
RETURN n.artifact_id AS id, n.record_hash AS record_hash`, map[string]any{
			"rows": rows, "revision_id": attempt.RevisionID, "tenant_id": job.Scope.TenantID,
			"repository_id": job.Scope.RepositoryID, "snapshot_id": job.Scope.SnapshotID,
		})
		if err != nil {
			return nil, err
		}
		return verifyWrittenRows(ctx, result, rows)
	})
	return err
}

func (s *GraphDataStore) WriteRelationBatch(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, relations []graph.ResolvedRelation) error {
	if err := validateBuildIdentity(job, attempt); err != nil {
		return err
	}
	groups, rows, err := relationRows(relations)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	for _, relation := range relations {
		if relation.Evidence.CommitSHA != job.CommitSHA {
			return validationError("write_relation_batch", "write_relations", "relation evidence commit does not match the graph candidate")
		}
	}
	_, err = s.write(ctx, "write_relation_batch", "write_relations", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		if err := requireCandidateStage(ctx, tx, job, attempt, "COMPLETE"); err != nil {
			return nil, err
		}
		if err := rejectRelationConflicts(ctx, tx, attempt.RevisionID, rows); err != nil {
			return nil, err
		}
		for key, grouped := range groups {
			var writeErr error
			switch {
			case strings.HasPrefix(key, "FACT:"):
				writeErr = writeFactRows(ctx, tx, job, attempt, strings.TrimPrefix(key, "FACT:"), grouped, false)
			case strings.HasPrefix(key, "EXTERNAL:"):
				writeErr = writeFactRows(ctx, tx, job, attempt, strings.TrimPrefix(key, "EXTERNAL:"), grouped, true)
			case key == string(graph.ResolutionAmbiguous):
				writeErr = writeAmbiguousRows(ctx, tx, job, attempt, grouped)
			case key == string(graph.ResolutionUnresolved):
				writeErr = writeUnresolvedRows(ctx, tx, job, attempt, grouped)
			default:
				writeErr = validationError("write_relation_batch", "write_relations", "unsupported relation group")
			}
			if writeErr != nil {
				return nil, writeErr
			}
		}
		return nil, nil
	})
	return err
}

func (s *GraphDataStore) SealCandidate(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, expected graph.RevisionStats, quality graph.QualityStatus) error {
	if err := validateBuildIdentity(job, attempt); err != nil {
		return err
	}
	if expected.Nodes < 0 || expected.Edges < 0 || expected.UnresolvedTargets < 0 || expected.AmbiguousRelations < 0 || (quality != graph.QualityHealthy && quality != graph.QualityDegraded) {
		return invalidInput("seal_candidate", "seal", "valid revision statistics and quality status are required", nil)
	}
	_, err := s.write(ctx, "seal_candidate", "seal", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `
MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id,job_id:$job_id,attempt_id:$attempt_id,fence:$fence})
CALL { WITH r MATCH (n:CodeEntity {revision_id:r.revision_id}) RETURN count(n) AS nodes }
CALL { WITH r MATCH ()-[e]->() WHERE e.revision_id=r.revision_id AND e.logical_relation=true RETURN count(e) AS edges }
CALL { WITH r OPTIONAL MATCH (i:ResolutionIssue {revision_id:r.revision_id,resolution_status:'UNRESOLVED'}) RETURN count(i) AS unresolved }
CALL { WITH r OPTIONAL MATCH (i:ResolutionIssue {revision_id:r.revision_id,resolution_status:'AMBIGUOUS'}) RETURN count(i) AS ambiguous }
CALL { WITH r MATCH (n) WHERE n.revision_id=r.revision_id AND
  (coalesce(n.tenant_id,'')<>r.tenant_id OR coalesce(n.repository_id,'')<>r.repository_id OR coalesce(n.snapshot_id,'')<>r.snapshot_id)
  RETURN count(n) AS invalid_scope }
CALL { WITH r MATCH (source)-[e]->(target)
  WHERE (source.revision_id=r.revision_id OR target.revision_id=r.revision_id) AND
  (coalesce(e.revision_id,'')<>r.revision_id OR coalesce(source.revision_id,'')<>r.revision_id OR coalesce(target.revision_id,'')<>r.revision_id)
  RETURN count(e) AS invalid_closure }
RETURN r.candidate_status AS status, r.artifact_stage AS artifact_stage, r.quality_status AS quality_status,
  r.node_count AS stored_nodes, r.edge_count AS stored_edges,
  r.unresolved_count AS stored_unresolved, r.ambiguous_count AS stored_ambiguous,
  nodes, edges, unresolved, ambiguous, invalid_scope, invalid_closure`, candidateParams(job, attempt))
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, validationError("seal_candidate", "seal", "candidate does not exist or attempt fence does not match")
		}
		values := record.AsMap()
		actual := graph.RevisionStats{Nodes: intValue(values["nodes"]), Edges: intValue(values["edges"]), UnresolvedTargets: intValue(values["unresolved"]), AmbiguousRelations: intValue(values["ambiguous"])}
		if intValue(values["invalid_scope"]) != 0 || intValue(values["invalid_closure"]) != 0 {
			return nil, validationError("seal_candidate", "seal", "candidate contains cross-scope nodes or relationships")
		}
		if actual != expected {
			return nil, validationError("seal_candidate", "seal", fmt.Sprintf("candidate counts do not match expected revision statistics: actual=%+v expected=%+v", actual, expected))
		}
		status, _ := values["status"].(string)
		if values["artifact_stage"] != "COMPLETE" {
			return nil, validationError("seal_candidate", "seal", "artifact stage must be complete before sealing")
		}
		if status == string(graph.CandidateSealed) {
			if values["quality_status"] != string(quality) || intValue(values["stored_nodes"]) != expected.Nodes || intValue(values["stored_edges"]) != expected.Edges || intValue(values["stored_unresolved"]) != expected.UnresolvedTargets || intValue(values["stored_ambiguous"]) != expected.AmbiguousRelations {
				return nil, validationError("seal_candidate", "seal", "sealed candidate metadata conflicts with requested seal")
			}
			return nil, nil
		}
		if status != string(graph.CandidateStaging) {
			return nil, validationError("seal_candidate", "seal", "only a STAGING candidate can be sealed")
		}
		updated, err := tx.Run(ctx, `
MATCH (r:GraphRevision {revision_id:$revision_id,attempt_id:$attempt_id,fence:$fence,candidate_status:'STAGING'})
SET r.candidate_status='SEALED', r.quality_status=$quality_status,
  r.node_count=$nodes, r.edge_count=$edges,
  r.unresolved_count=$unresolved, r.ambiguous_count=$ambiguous,
  r.sealed_at=datetime()
RETURN count(r) AS updated`, map[string]any{"revision_id": attempt.RevisionID, "attempt_id": attempt.AttemptID, "fence": attempt.Fence, "quality_status": string(quality), "nodes": expected.Nodes, "edges": expected.Edges, "unresolved": expected.UnresolvedTargets, "ambiguous": expected.AmbiguousRelations})
		if err != nil {
			return nil, err
		}
		updatedRecord, err := updated.Single(ctx)
		if err != nil || intValue(updatedRecord.AsMap()["updated"]) != 1 {
			return nil, validationError("seal_candidate", "seal", "candidate changed while it was being sealed")
		}
		return nil, nil
	})
	return err
}

func requireCandidateStage(ctx context.Context, tx neo4jdriver.ManagedTransaction, job graph.BuildJob, attempt graph.BuildAttempt, artifactStage string) error {
	params := candidateParams(job, attempt)
	params["artifact_stage"] = artifactStage
	result, err := tx.Run(ctx, `MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id,job_id:$job_id,attempt_id:$attempt_id,fence:$fence,candidate_status:'STAGING',artifact_stage:$artifact_stage}) RETURN count(r) AS matches`, params)
	if err != nil {
		return err
	}
	record, err := result.Single(ctx)
	if err != nil || intValue(record.AsMap()["matches"]) != 1 {
		return validationError("write_batch", "candidate", "candidate phase is invalid or attempt fence does not match")
	}
	return nil
}

func artifactRows(artifacts []repository.CodeArtifact) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(artifacts))
	seen := map[string]string{}
	for _, artifact := range artifacts {
		if strings.TrimSpace(artifact.ArtifactID) == "" || strings.TrimSpace(artifact.Name) == "" || !validArtifactKind(artifact.Kind) {
			return nil, validationError("write_artifact_batch", "write_artifacts", "artifact identity, type and name are required")
		}
		if err := artifact.SourceRef.Validate(); err != nil {
			return nil, validationError("write_artifact_batch", "write_artifacts", "artifact source reference is invalid")
		}
		hash := recordHash(artifact)
		if previous, exists := seen[artifact.ArtifactID]; exists {
			if previous != hash {
				return nil, validationError("write_artifact_batch", "write_artifacts", "duplicate artifact id has conflicting content")
			}
			continue
		}
		seen[artifact.ArtifactID] = hash
		attributes, _ := json.Marshal(artifact.Attributes)
		rows = append(rows, map[string]any{
			"id": artifact.ArtifactID, "artifact_id": artifact.ArtifactID, "entity_type": string(graph.EntityTypeFor(artifact.Kind)),
			"name": artifact.Name, "qualified_name": artifact.QualifiedName, "language": artifact.Language,
			"signature": artifact.Signature, "content_hash": artifact.ContentHash, "attributes_json": string(attributes),
			"source_commit_sha": artifact.SourceRef.CommitSHA, "source_path": artifact.SourceRef.Path,
			"source_symbol_id": artifact.SourceRef.SymbolID, "source_start_line": artifact.SourceRef.StartLine,
			"source_end_line": artifact.SourceRef.EndLine, "source_content_hash": artifact.SourceRef.ContentHash, "record_hash": hash,
		})
	}
	return rows, nil
}

func relationRows(relations []graph.ResolvedRelation) (map[string][]map[string]any, []map[string]any, error) {
	groups := map[string][]map[string]any{}
	all := make([]map[string]any, 0, len(relations))
	seen := map[string]string{}
	for _, relation := range relations {
		if err := relation.Validate(); err != nil {
			return nil, nil, validationError("write_relation_batch", "write_relations", err.Error())
		}
		hash := recordHash(relation)
		if previous, exists := seen[relation.RelationID]; exists {
			if previous != hash {
				return nil, nil, validationError("write_relation_batch", "write_relations", "duplicate relation id has conflicting content")
			}
			continue
		}
		seen[relation.RelationID] = hash
		row := map[string]any{
			"id": relation.RelationID, "relation_id": relation.RelationID, "kind": string(relation.Kind),
			"from_artifact_id": relation.FromArtifactID, "target_artifact_id": relation.TargetArtifactID,
			"raw_target_symbol": relation.RawTargetSymbol, "resolution_status": string(relation.ResolutionStatus),
			"external_identity": relation.ExternalIdentity, "candidates": relation.AmbiguousCandidates,
			"reason_code": relation.ResolutionReasonCode, "confidence": relation.Confidence,
			"evidence_commit_sha": relation.Evidence.CommitSHA, "evidence_path": relation.Evidence.Path,
			"evidence_symbol_id": relation.Evidence.SymbolID, "evidence_start_line": relation.Evidence.StartLine,
			"evidence_end_line": relation.Evidence.EndLine, "evidence_content_hash": relation.Evidence.ContentHash,
			"record_hash": hash,
		}
		if relation.ResolutionStatus == graph.ResolutionAmbiguous {
			possibleTargets := make([]map[string]any, 0, len(relation.AmbiguousCandidates))
			for _, candidateID := range relation.AmbiguousCandidates {
				possibleTargets = append(possibleTargets, map[string]any{
					"artifact_id": candidateID,
					"relation_id": "possible:" + recordHash(struct {
						RelationID  string `json:"relation_id"`
						CandidateID string `json:"candidate_id"`
					}{relation.RelationID, candidateID}),
				})
			}
			row["possible_targets"] = possibleTargets
		}
		key := string(relation.ResolutionStatus)
		if relation.ResolutionStatus == graph.ResolutionResolved {
			key = "FACT:" + string(relation.Kind)
		} else if relation.ResolutionStatus == graph.ResolutionExternal {
			key = "EXTERNAL:" + string(relation.Kind)
		}
		groups[key] = append(groups[key], row)
		all = append(all, row)
	}
	return groups, all, nil
}

func rejectRelationConflicts(ctx context.Context, tx neo4jdriver.ManagedTransaction, revisionID string, rows []map[string]any) error {
	ids := make([]string, 0, len(rows))
	expected := map[string]string{}
	for _, row := range rows {
		id, hash := row["relation_id"].(string), row["record_hash"].(string)
		ids, expected[id] = append(ids, id), hash
	}
	result, err := tx.Run(ctx, `MATCH ()-[e]->() WHERE e.revision_id=$revision_id AND e.logical_relation=true AND e.relation_id IN $ids RETURN e.relation_id AS id,e.record_hash AS record_hash`, map[string]any{"revision_id": revisionID, "ids": ids})
	if err != nil {
		return err
	}
	for result.Next(ctx) {
		values := result.Record().AsMap()
		id, _ := values["id"].(string)
		hash, _ := values["record_hash"].(string)
		if expected[id] != hash {
			return validationError("write_relation_batch", "write_relations", "relation id is already bound to conflicting content")
		}
	}
	return result.Err()
}

func writeFactRows(ctx context.Context, tx neo4jdriver.ManagedTransaction, job graph.BuildJob, attempt graph.BuildAttempt, relationType string, rows []map[string]any, external bool) error {
	query, ok := factQuery(relationType, external)
	if !ok {
		return validationError("write_relation_batch", "write_relations", "unsupported fact relation type")
	}
	result, err := tx.Run(ctx, query, map[string]any{"rows": rows, "revision_id": attempt.RevisionID, "tenant_id": job.Scope.TenantID, "repository_id": job.Scope.RepositoryID, "snapshot_id": job.Scope.SnapshotID})
	if err != nil {
		return err
	}
	written, err := countRows(ctx, result)
	if err != nil {
		return err
	}
	if written != len(rows) {
		return validationError("write_relation_batch", "write_relations", "relation source or resolved target is missing from this revision")
	}
	return nil
}

func factQuery(relationType string, external bool) (string, bool) {
	allowed := map[string]bool{"CONTAINS": true, "IMPORTS": true, "CALLS": true, "EXTENDS": true, "IMPLEMENTS": true, "DEPENDS_ON": true}
	if !allowed[relationType] {
		return "", false
	}
	target := "MATCH (target:CodeEntity {revision_id:$revision_id,artifact_id:row.target_artifact_id})"
	if external {
		target = `MERGE (target:ExternalEntity {revision_id:$revision_id,external_id:row.external_identity})
ON CREATE SET target.tenant_id=$tenant_id,target.repository_id=$repository_id,target.snapshot_id=$snapshot_id,
 target.node_id='external:'+row.external_identity,target.entity_type='SYMBOL',target.name=row.external_identity,target.qualified_name=row.external_identity`
	}
	return fmt.Sprintf(`UNWIND $rows AS row
MATCH (source:CodeEntity {revision_id:$revision_id,artifact_id:row.from_artifact_id})
%s
MERGE (source)-[edge:%s {revision_id:$revision_id,relation_id:row.relation_id}]->(target)
ON CREATE SET edge.logical_relation=true,edge.record_hash=row.record_hash,edge.confidence=row.confidence,
 edge.resolution_status=row.resolution_status,edge.evidence_commit_sha=row.evidence_commit_sha,
 edge.evidence_path=row.evidence_path,edge.evidence_symbol_id=row.evidence_symbol_id,
 edge.evidence_start_line=row.evidence_start_line,edge.evidence_end_line=row.evidence_end_line,
 edge.evidence_content_hash=row.evidence_content_hash
RETURN edge.relation_id AS id`, target, relationType), true
}

func writeAmbiguousRows(ctx context.Context, tx neo4jdriver.ManagedTransaction, job graph.BuildJob, attempt graph.BuildAttempt, rows []map[string]any) error {
	result, err := tx.Run(ctx, `UNWIND $rows AS row
MATCH (source:CodeEntity {revision_id:$revision_id,artifact_id:row.from_artifact_id})
MERGE (issue:ResolutionIssue {revision_id:$revision_id,issue_id:row.relation_id})
ON CREATE SET issue.tenant_id=$tenant_id,issue.repository_id=$repository_id,issue.snapshot_id=$snapshot_id,
 issue.node_id='issue:'+row.relation_id,issue.resolution_status='AMBIGUOUS',issue.raw_target=row.raw_target_symbol,
 issue.reason_code=row.reason_code,issue.intended_kind=row.kind,issue.record_hash=row.record_hash
MERGE (source)-[edge:UNCERTAIN_RELATION {revision_id:$revision_id,relation_id:row.relation_id}]->(issue)
ON CREATE SET edge.logical_relation=true,edge.record_hash=row.record_hash,edge.intended_kind=row.kind,
 edge.resolution_status='AMBIGUOUS',edge.confidence=row.confidence,edge.evidence_commit_sha=row.evidence_commit_sha,
 edge.evidence_path=row.evidence_path,edge.evidence_symbol_id=row.evidence_symbol_id,
 edge.evidence_start_line=row.evidence_start_line,edge.evidence_end_line=row.evidence_end_line,
 edge.evidence_content_hash=row.evidence_content_hash
WITH row,issue,edge
UNWIND row.possible_targets AS target
MATCH (candidate:CodeEntity {revision_id:$revision_id,artifact_id:target.artifact_id})
MERGE (issue)-[possible:POSSIBLE_TARGET {revision_id:$revision_id,relation_id:target.relation_id}]->(candidate)
ON CREATE SET possible.logical_relation=false
RETURN row.relation_id AS id, count(candidate) AS candidates`, map[string]any{"rows": rows, "revision_id": attempt.RevisionID, "tenant_id": job.Scope.TenantID, "repository_id": job.Scope.RepositoryID, "snapshot_id": job.Scope.SnapshotID})
	if err != nil {
		return err
	}
	seen, candidates := 0, 0
	for result.Next(ctx) {
		seen++
		candidates += intValue(result.Record().AsMap()["candidates"])
	}
	if err := result.Err(); err != nil {
		return err
	}
	expectedCandidates := 0
	for _, row := range rows {
		expectedCandidates += len(row["possible_targets"].([]map[string]any))
	}
	if seen != len(rows) || candidates != expectedCandidates {
		return validationError("write_relation_batch", "write_relations", "ambiguous relation source or candidate target is missing from this revision")
	}
	return nil
}

func writeUnresolvedRows(ctx context.Context, tx neo4jdriver.ManagedTransaction, job graph.BuildJob, attempt graph.BuildAttempt, rows []map[string]any) error {
	result, err := tx.Run(ctx, `UNWIND $rows AS row
MATCH (source:CodeEntity {revision_id:$revision_id,artifact_id:row.from_artifact_id})
MERGE (issue:ResolutionIssue {revision_id:$revision_id,issue_id:row.relation_id})
ON CREATE SET issue.tenant_id=$tenant_id,issue.repository_id=$repository_id,issue.snapshot_id=$snapshot_id,
 issue.node_id='issue:'+row.relation_id,issue.resolution_status='UNRESOLVED',issue.raw_target=row.raw_target_symbol,
 issue.reason_code=row.reason_code,issue.intended_kind=row.kind,issue.record_hash=row.record_hash
MERGE (source)-[edge:UNCERTAIN_RELATION {revision_id:$revision_id,relation_id:row.relation_id}]->(issue)
ON CREATE SET edge.logical_relation=true,edge.record_hash=row.record_hash,edge.intended_kind=row.kind,
 edge.resolution_status='UNRESOLVED',edge.confidence=row.confidence,edge.evidence_commit_sha=row.evidence_commit_sha,
 edge.evidence_path=row.evidence_path,edge.evidence_symbol_id=row.evidence_symbol_id,
 edge.evidence_start_line=row.evidence_start_line,edge.evidence_end_line=row.evidence_end_line,
 edge.evidence_content_hash=row.evidence_content_hash
RETURN edge.relation_id AS id`, map[string]any{"rows": rows, "revision_id": attempt.RevisionID, "tenant_id": job.Scope.TenantID, "repository_id": job.Scope.RepositoryID, "snapshot_id": job.Scope.SnapshotID})
	if err != nil {
		return err
	}
	written, err := countRows(ctx, result)
	if err != nil {
		return err
	}
	if written != len(rows) {
		return validationError("write_relation_batch", "write_relations", "unresolved relation source is missing from this revision")
	}
	return nil
}

func verifyWrittenRows(ctx context.Context, result neo4jdriver.Result, rows []map[string]any) (any, error) {
	expected := map[string]string{}
	for _, row := range rows {
		expected[row["id"].(string)] = row["record_hash"].(string)
	}
	seen := 0
	for result.Next(ctx) {
		values := result.Record().AsMap()
		id, _ := values["id"].(string)
		hash, _ := values["record_hash"].(string)
		if expected[id] != hash {
			return nil, validationError("write_artifact_batch", "write_artifacts", "artifact id is already bound to conflicting content")
		}
		seen++
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	if seen != len(rows) {
		return nil, validationError("write_artifact_batch", "write_artifacts", "artifact batch was not written completely")
	}
	return nil, nil
}

func countRows(ctx context.Context, result neo4jdriver.Result) (int, error) {
	count := 0
	for result.Next(ctx) {
		count++
	}
	return count, result.Err()
}

func validArtifactKind(kind repository.ArtifactKind) bool {
	switch kind {
	case repository.ArtifactFile, repository.ArtifactModule, repository.ArtifactClass, repository.ArtifactInterface, repository.ArtifactFunction, repository.ArtifactMethod, repository.ArtifactImport, repository.ArtifactConfig, repository.ArtifactDocument:
		return true
	default:
		return false
	}
}

func recordHash(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func intValue(value any) int {
	number, _ := asInt64(value)
	return int(number)
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
