package graphapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/ports"
)

// QueryServiceConfig controls resources owned by the production query path.
// Neo4j traversal budgets remain in the data-plane adapter so both layers can
// reject work before returning a partial result.
type QueryServiceConfig struct {
	Timeout       time.Duration
	MaxConcurrent int
}

func DefaultQueryServiceConfig() QueryServiceConfig {
	return QueryServiceConfig{Timeout: 10 * time.Second, MaxConcurrent: 64}
}

func (c QueryServiceConfig) Validate() error {
	if c.Timeout <= 0 || c.Timeout > 5*time.Minute {
		return fmt.Errorf("query timeout must be greater than zero and at most five minutes")
	}
	if c.MaxConcurrent <= 0 || c.MaxConcurrent > 100_000 {
		return fmt.Errorf("query concurrency must be between 1 and 100000")
	}
	return nil
}

// QueryService resolves the exact PostgreSQL ACTIVE pointer before reading
// Neo4j. It never searches for a latest revision or falls back to an older one.
type QueryService struct {
	revisions ports.GraphActiveRevisionStore
	graph     ports.GraphProductionQueryRepository
	observer  ports.Observer
	config    QueryServiceConfig
	slots     chan struct{}
}

func NewQueryService(revisions ports.GraphActiveRevisionStore, graphRepository ports.GraphProductionQueryRepository, observer ports.Observer, config QueryServiceConfig) (*QueryService, error) {
	if revisions == nil || graphRepository == nil {
		return nil, fmt.Errorf("graph active revision store and production query repository are required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if observer == nil {
		observer = noopObserver{}
	}
	return &QueryService{
		revisions: revisions,
		graph:     graphRepository,
		observer:  observer,
		config:    config,
		slots:     make(chan struct{}, config.MaxConcurrent),
	}, nil
}

func (s *QueryService) Query(ctx context.Context, query graph.Query) (result graph.Result, err error) {
	ctx, finish := startGraphStage(s.observer, ctx, "graph_query", map[string]string{
		"operation": "query", "tenant_id": query.Scope.TenantID, "repository_id": query.Scope.RepositoryID,
		"snapshot_id": query.Scope.SnapshotID, "trace_id": query.Scope.TraceID,
	})
	defer func() { finish(err) }()
	if validateErr := query.Validate(); validateErr != nil {
		return graph.Result{}, queryError(graph.ErrInvalidInput, "validate", "request", false, "graph query is invalid", validateErr)
	}
	if err := s.acquire(ctx); err != nil {
		return graph.Result{}, err
	}
	defer s.release()

	queryCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	revision, err := s.resolveAndVerify(queryCtx, query.Scope)
	if err != nil {
		return graph.Result{}, classifyQueryContext(queryCtx, err)
	}
	dataCtx, finishData := startGraphStage(s.observer, queryCtx, "graph_neo4j_query", map[string]string{"operation": "query", "dependency": "neo4j", "revision_id": revision.RevisionID})
	result, err = s.graph.QueryRevision(dataCtx, revision.RevisionID, query)
	finishData(err)
	if err != nil {
		return graph.Result{}, classifyQueryContext(queryCtx, activeQueryError("query_revision", err))
	}
	if result.Diagnostics.RevisionID != revision.RevisionID {
		return graph.Result{}, inconsistentQueryResult("query_revision", "neo4j", "neo4j returned a result for an unexpected graph revision", nil)
	}
	if result.Nodes == nil {
		result.Nodes = []graph.Entity{}
	}
	if result.Edges == nil {
		result.Edges = []graph.Relation{}
	}
	s.observer.Count("graph_query_requests_total", 1, map[string]string{"operation": "query", "status": "succeeded"})
	s.observer.Count("graph_query_result_nodes_total", int64(len(result.Nodes)), map[string]string{"operation": "query"})
	s.observer.Count("graph_query_visited_total", int64(result.Diagnostics.Visited), map[string]string{"operation": "query"})
	if result.Diagnostics.Truncated {
		s.observer.Count("graph_query_truncated_total", 1, map[string]string{"operation": "query"})
	}
	return result, nil
}

func (s *QueryService) QueryDiagnostics(ctx context.Context, query graph.DiagnosticQuery) (result graph.DiagnosticResult, err error) {
	ctx, finish := startGraphStage(s.observer, ctx, "graph_query", map[string]string{
		"operation": "diagnostics", "tenant_id": query.Scope.TenantID, "repository_id": query.Scope.RepositoryID,
		"snapshot_id": query.Scope.SnapshotID, "trace_id": query.Scope.TraceID,
	})
	defer func() { finish(err) }()
	if validateErr := query.Validate(); validateErr != nil {
		return graph.DiagnosticResult{}, queryError(graph.ErrInvalidInput, "validate_diagnostics", "request", false, "graph diagnostics query is invalid", validateErr)
	}
	if err := s.acquire(ctx); err != nil {
		return graph.DiagnosticResult{}, err
	}
	defer s.release()

	queryCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	revision, err := s.resolveAndVerify(queryCtx, query.Scope)
	if err != nil {
		return graph.DiagnosticResult{}, classifyQueryContext(queryCtx, err)
	}
	dataCtx, finishData := startGraphStage(s.observer, queryCtx, "graph_neo4j_query", map[string]string{"operation": "diagnostics", "dependency": "neo4j", "revision_id": revision.RevisionID})
	result, err = s.graph.QueryRevisionDiagnostics(dataCtx, revision.RevisionID, query)
	finishData(err)
	if err != nil {
		return graph.DiagnosticResult{}, classifyQueryContext(queryCtx, activeQueryError("query_diagnostics", err))
	}
	if result.RevisionID != revision.RevisionID {
		return graph.DiagnosticResult{}, inconsistentQueryResult("query_diagnostics", "neo4j", "neo4j returned diagnostics for an unexpected graph revision", nil)
	}
	if result.Issues == nil {
		result.Issues = []graph.ResolutionIssue{}
	}
	s.observer.Count("graph_query_requests_total", 1, map[string]string{"operation": "diagnostics", "status": "succeeded"})
	s.observer.Count("graph_query_diagnostic_issues_total", int64(len(result.Issues)), map[string]string{"operation": "diagnostics"})
	if result.Truncated {
		s.observer.Count("graph_query_truncated_total", 1, map[string]string{"operation": "diagnostics"})
	}
	return result, nil
}

func (s *QueryService) resolveAndVerify(ctx context.Context, scope common.Scope) (graph.Revision, error) {
	controlCtx, finishControl := startGraphStage(s.observer, ctx, "graph_active_revision", map[string]string{"operation": "resolve", "dependency": "postgresql"})
	revision, err := s.revisions.ActiveGraphRevision(controlCtx, scope)
	finishControl(err)
	if err != nil {
		return graph.Revision{}, err
	}
	if err := validateActiveQueryRevision(scope, revision); err != nil {
		return graph.Revision{}, err
	}
	dataCtx, finishData := startGraphStage(s.observer, ctx, "graph_neo4j_verify", map[string]string{"operation": "verify", "dependency": "neo4j", "revision_id": revision.RevisionID})
	if err := s.graph.VerifyRevision(dataCtx, revision); err != nil {
		finishData(err)
		return graph.Revision{}, activeQueryError("verify_revision", err)
	}
	finishData(nil)
	return revision, nil
}

func (s *QueryService) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classifyQueryContext(ctx, err)
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
		s.observer.Count("graph_query_requests_total", 1, map[string]string{"operation": "admission", "status": "rejected"})
		return queryError(graph.ErrQueryBudgetExceeded, "query_admission", "capacity", false, "graph query concurrency budget is exhausted", nil)
	}
}

func (s *QueryService) release() { <-s.slots }

func validateActiveQueryRevision(scope common.Scope, revision graph.Revision) error {
	if revision.TenantID != scope.TenantID || revision.RepositoryID != scope.RepositoryID || revision.SnapshotID != scope.SnapshotID {
		return inconsistentQueryResult("active_revision", "postgresql", "active graph revision scope does not match the query", nil)
	}
	for _, value := range []string{
		revision.RevisionID, revision.CommitSHA, revision.ParserResultVersion,
		revision.GraphSchemaVersion, revision.AlgorithmVersion, revision.BuildPolicyVersion,
	} {
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return inconsistentQueryResult("active_revision", "postgresql", "active graph revision metadata is invalid", nil)
		}
	}
	if revision.BuildStatus != graph.RevisionActive || (revision.QualityStatus != graph.QualityHealthy && revision.QualityStatus != graph.QualityDegraded) {
		return inconsistentQueryResult("active_revision", "postgresql", "active graph revision state is invalid", nil)
	}
	if revision.Stats.Nodes < 0 || revision.Stats.Edges < 0 || revision.Stats.UnresolvedTargets < 0 || revision.Stats.AmbiguousRelations < 0 {
		return inconsistentQueryResult("active_revision", "postgresql", "active graph revision statistics are invalid", nil)
	}
	return nil
}

func activeQueryError(operation string, cause error) error {
	if cause == nil || graph.IsCode(cause, graph.ErrGraphInconsistent) {
		return cause
	}
	if graph.IsCode(cause, graph.ErrRevisionNotFound) {
		return inconsistentQueryResult(operation, "neo4j", "active graph revision is missing from neo4j", cause)
	}
	return cause
}

func classifyQueryContext(queryCtx context.Context, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, context.Canceled) || errors.Is(queryCtx.Err(), context.Canceled) {
		return queryError(graph.ErrQueryCancelled, "query", "context", false, "graph query was cancelled", context.Canceled)
	}
	if errors.Is(cause, context.DeadlineExceeded) || errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
		return queryError(graph.ErrQueryTimeout, "query", "timeout", true, "graph query timed out", context.DeadlineExceeded)
	}
	return cause
}

func inconsistentQueryResult(operation, dependency, message string, cause error) error {
	return queryError(graph.ErrGraphInconsistent, operation, dependency, true, message, cause)
}

func queryError(code graph.ErrorCode, operation, dependency string, retryable bool, message string, cause error) error {
	return &graph.DomainError{
		Code: code, Operation: operation, Stage: "query", Dependency: dependency,
		Message: message, Retryable: retryable, Cause: cause,
	}
}
