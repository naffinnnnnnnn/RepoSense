package neo4j

import (
	"context"
	"fmt"
	"strings"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

// GraphDataStore persists immutable, revision-scoped graph candidates in Neo4j.
// Driver is safe for concurrent use; every operation creates its own Session.
type GraphDataStore struct {
	driver   neo4jdriver.Driver
	database string
	config   GraphDataConfig
}

var _ ports.GraphDataRepository = (*GraphDataStore)(nil)
var _ ports.GraphDiagnosticsRepository = (*GraphDataStore)(nil)
var _ ports.GraphReconciliationDataRepository = (*GraphDataStore)(nil)
var _ ports.GraphProductionQueryRepository = (*GraphDataStore)(nil)

type GraphDataConfig struct {
	MaxRoots           int
	MaxNodes           int
	MaxEdges           int
	MaxFrontier        int
	MaxDatabaseQueries int
}

func DefaultGraphDataConfig() GraphDataConfig {
	return GraphDataConfig{MaxRoots: 128, MaxNodes: 10_000, MaxEdges: 50_000, MaxFrontier: 10_000, MaxDatabaseQueries: 16}
}

func (c GraphDataConfig) Validate() error {
	if c.MaxRoots <= 0 || c.MaxNodes <= 0 || c.MaxEdges <= 0 || c.MaxFrontier <= 0 || c.MaxDatabaseQueries < 4 {
		return fmt.Errorf("neo4j graph query budgets must be positive and max database queries must be at least 4")
	}
	return nil
}

func NewGraphDataStore(ctx context.Context, uri, username, password, database string) (*GraphDataStore, error) {
	if strings.TrimSpace(uri) == "" || strings.TrimSpace(username) == "" || password == "" {
		return nil, fmt.Errorf("neo4j uri, username and password are required")
	}
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.BasicAuth(username, password, ""))
	if err != nil {
		return nil, graphStoreError("create_driver", "connect", err)
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(ctx)
		return nil, graphStoreError("verify_connectivity", "connect", err)
	}
	return &GraphDataStore{driver: driver, database: database, config: DefaultGraphDataConfig()}, nil
}

func NewGraphDataStoreWithDriver(driver neo4jdriver.Driver, database string) *GraphDataStore {
	return &GraphDataStore{driver: driver, database: database, config: DefaultGraphDataConfig()}
}

func NewGraphDataStoreWithConfig(driver neo4jdriver.Driver, database string, config GraphDataConfig) (*GraphDataStore, error) {
	if driver == nil {
		return nil, fmt.Errorf("neo4j driver is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &GraphDataStore{driver: driver, database: database, config: config}, nil
}

func (s *GraphDataStore) Close(ctx context.Context) error {
	if s == nil || s.driver == nil {
		return nil
	}
	return s.driver.Close(ctx)
}

func (s *GraphDataStore) CreateCandidate(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt) error {
	if err := validateBuildIdentity(job, attempt); err != nil {
		return err
	}
	params := candidateParams(job, attempt)
	value, err := s.write(ctx, "create_candidate", "candidate", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `
MERGE (r:GraphRevision {revision_id: $revision_id})
ON CREATE SET r.tenant_id=$tenant_id, r.repository_id=$repository_id,
  r.snapshot_id=$snapshot_id, r.commit_sha=$commit_sha, r.job_id=$job_id,
  r.attempt_id=$attempt_id, r.fence=$fence, r.candidate_status='STAGING', r.artifact_stage='WRITING',
  r.parser_result_version=$parser_result_version,
  r.graph_schema_version=$graph_schema_version,
  r.graph_algorithm_version=$graph_algorithm_version,
  r.build_policy_version=$build_policy_version,
  r.created_at=datetime($created_at)
RETURN r.tenant_id AS tenant_id, r.repository_id AS repository_id,
  r.snapshot_id AS snapshot_id, r.commit_sha AS commit_sha, r.job_id AS job_id,
  r.attempt_id AS attempt_id, r.fence AS fence,
  r.parser_result_version AS parser_result_version,
  r.graph_schema_version AS graph_schema_version,
  r.graph_algorithm_version AS graph_algorithm_version,
  r.build_policy_version AS build_policy_version,
  r.candidate_status AS candidate_status`, params)
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
		return err
	}
	stored, ok := value.(map[string]any)
	if !ok || !candidateMatches(stored, params) {
		return validationError("create_candidate", "candidate", "revision id is already bound to another graph candidate")
	}
	return nil
}

func (s *GraphDataStore) CandidateStatus(ctx context.Context, scope common.Scope, revisionID string) (graph.CandidateStatus, error) {
	if err := scope.Validate(true); err != nil || strings.TrimSpace(revisionID) == "" {
		return "", invalidInput("candidate_status", "candidate", "valid scope and revision_id are required", err)
	}
	value, err := s.read(ctx, "candidate_status", "candidate", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id}) RETURN r.candidate_status AS status`, scopeParams(scope, revisionID))
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, revisionNotFound("candidate_status", err)
		}
		status, _ := record.Get("status")
		return status, nil
	})
	if err != nil {
		return "", err
	}
	status, ok := value.(string)
	if !ok || (graph.CandidateStatus(status) != graph.CandidateStaging && graph.CandidateStatus(status) != graph.CandidateSealed && graph.CandidateStatus(status) != graph.CandidateFailed) {
		return "", validationError("candidate_status", "candidate", "candidate has an invalid status")
	}
	return graph.CandidateStatus(status), nil
}

func (s *GraphDataStore) CompleteArtifactStage(ctx context.Context, job graph.BuildJob, attempt graph.BuildAttempt, expectedArtifacts int) error {
	if err := validateBuildIdentity(job, attempt); err != nil {
		return err
	}
	if expectedArtifacts < 0 {
		return invalidInput("complete_artifact_stage", "write_artifacts", "expected artifact count must not be negative", nil)
	}
	_, err := s.write(ctx, "complete_artifact_stage", "write_artifacts", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `
MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id,job_id:$job_id,attempt_id:$attempt_id,fence:$fence})
OPTIONAL MATCH (n:CodeEntity {revision_id:$revision_id})
RETURN r.candidate_status AS status,r.artifact_stage AS artifact_stage,count(n) AS artifacts`, candidateParams(job, attempt))
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, validationError("complete_artifact_stage", "write_artifacts", "candidate does not exist or attempt fence does not match")
		}
		values := record.AsMap()
		if intValue(values["artifacts"]) != expectedArtifacts {
			return nil, validationError("complete_artifact_stage", "write_artifacts", "artifact count does not match the expected valid input count")
		}
		stage, _ := values["artifact_stage"].(string)
		if stage == "COMPLETE" {
			return nil, nil
		}
		if values["status"] != string(graph.CandidateStaging) || stage != "WRITING" {
			return nil, validationError("complete_artifact_stage", "write_artifacts", "artifact stage cannot be completed from the current candidate state")
		}
		updated, err := tx.Run(ctx, `MATCH (r:GraphRevision {revision_id:$revision_id,attempt_id:$attempt_id,fence:$fence,candidate_status:'STAGING',artifact_stage:'WRITING'}) SET r.artifact_stage='COMPLETE',r.artifact_count=$artifacts RETURN count(r) AS updated`, map[string]any{"revision_id": attempt.RevisionID, "attempt_id": attempt.AttemptID, "fence": attempt.Fence, "artifacts": expectedArtifacts})
		if err != nil {
			return nil, err
		}
		updatedRecord, err := updated.Single(ctx)
		if err != nil || intValue(updatedRecord.AsMap()["updated"]) != 1 {
			return nil, validationError("complete_artifact_stage", "write_artifacts", "candidate changed while completing the artifact stage")
		}
		return nil, nil
	})
	return err
}

func (s *GraphDataStore) DeleteCandidate(ctx context.Context, scope common.Scope, revisionID string, fence int64) error {
	if err := scope.Validate(true); err != nil || strings.TrimSpace(revisionID) == "" || fence <= 0 {
		return invalidInput("delete_candidate", "cleanup", "valid scope, revision_id and fence are required", err)
	}
	value, err := s.write(ctx, "delete_candidate", "cleanup", func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `
MATCH (r:GraphRevision {revision_id:$revision_id,tenant_id:$tenant_id,repository_id:$repository_id,snapshot_id:$snapshot_id,fence:$fence})
WHERE r.candidate_status IN ['STAGING','FAILED','SEALED']
OPTIONAL MATCH (n) WHERE n.revision_id=$revision_id
DETACH DELETE n
RETURN count(*) AS deleted`, map[string]any{"revision_id": revisionID, "tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "fence": fence})
		if err != nil {
			return nil, err
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, err
		}
		deleted, _ := record.Get("deleted")
		return deleted, nil
	})
	if err != nil {
		return err
	}
	if count, _ := asInt64(value); count == 0 {
		return validationError("delete_candidate", "cleanup", "candidate is missing, sealed, or owned by another attempt")
	}
	return nil
}

func (s *GraphDataStore) read(ctx context.Context, operation, stage string, work neo4jdriver.ManagedTransactionWork) (any, error) {
	if s == nil || s.driver == nil {
		return nil, graphStoreError(operation, stage, fmt.Errorf("neo4j driver is not configured"))
	}
	session := s.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: s.database})
	defer session.Close(ctx)
	value, err := session.ExecuteRead(ctx, work)
	if err != nil {
		if _, ok := err.(*graph.DomainError); ok {
			return nil, err
		}
		return nil, graphStoreError(operation, stage, err)
	}
	return value, nil
}

func (s *GraphDataStore) write(ctx context.Context, operation, stage string, work neo4jdriver.ManagedTransactionWork) (any, error) {
	if s == nil || s.driver == nil {
		return nil, graphStoreError(operation, stage, fmt.Errorf("neo4j driver is not configured"))
	}
	session := s.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: s.database})
	defer session.Close(ctx)
	value, err := session.ExecuteWrite(ctx, work)
	if err != nil {
		if _, ok := err.(*graph.DomainError); ok {
			return nil, err
		}
		return nil, graphStoreError(operation, stage, err)
	}
	return value, nil
}

func validateBuildIdentity(job graph.BuildJob, attempt graph.BuildAttempt) error {
	if err := job.Scope.Validate(true); err != nil {
		return invalidInput("validate_candidate", "candidate", "valid graph build scope is required", err)
	}
	if strings.TrimSpace(job.JobID) == "" || strings.TrimSpace(job.CommitSHA) == "" || strings.TrimSpace(job.RequestFingerprint) == "" || strings.TrimSpace(attempt.AttemptID) == "" || strings.TrimSpace(attempt.RevisionID) == "" || strings.TrimSpace(attempt.LeaseOwner) == "" || attempt.Fence <= 0 || attempt.CreatedAt.IsZero() {
		return invalidInput("validate_candidate", "candidate", "complete job and attempt identity is required", nil)
	}
	if err := job.Versions.Validate(); err != nil {
		return invalidInput("validate_candidate", "candidate", "complete build versions are required", err)
	}
	if attempt.JobID != job.JobID || (job.RevisionID != "" && job.RevisionID != attempt.RevisionID) || job.Status != graph.JobBuilding || attempt.Status != graph.AttemptRunning {
		return validationError("validate_candidate", "candidate", "job and running attempt identity do not match")
	}
	return nil
}

func candidateParams(job graph.BuildJob, attempt graph.BuildAttempt) map[string]any {
	return map[string]any{
		"revision_id": attempt.RevisionID, "tenant_id": job.Scope.TenantID, "repository_id": job.Scope.RepositoryID,
		"snapshot_id": job.Scope.SnapshotID, "commit_sha": job.CommitSHA, "job_id": job.JobID,
		"attempt_id": attempt.AttemptID, "fence": attempt.Fence, "parser_result_version": job.Versions.ParserResultVersion,
		"graph_schema_version": job.Versions.GraphSchemaVersion, "graph_algorithm_version": job.Versions.GraphAlgorithmVersion,
		"build_policy_version": job.Versions.BuildPolicyVersion, "created_at": attempt.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
}

func candidateMatches(stored, expected map[string]any) bool {
	for _, key := range []string{"tenant_id", "repository_id", "snapshot_id", "commit_sha", "job_id", "attempt_id", "parser_result_version", "graph_schema_version", "graph_algorithm_version", "build_policy_version"} {
		if fmt.Sprint(stored[key]) != fmt.Sprint(expected[key]) {
			return false
		}
	}
	fence, ok := asInt64(stored["fence"])
	return ok && fence == expected["fence"] && (stored["candidate_status"] == string(graph.CandidateStaging) || stored["candidate_status"] == string(graph.CandidateSealed))
}

func scopeParams(scope common.Scope, revisionID string) map[string]any {
	return map[string]any{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "revision_id": revisionID}
}

func asInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int64:
		return number, true
	case int:
		return int64(number), true
	default:
		return 0, false
	}
}
