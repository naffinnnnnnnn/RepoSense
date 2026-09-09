//go:build integration

package neo4j

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestGraphDataStoreCandidateSealQueryDiagnosticsAndIsolation(t *testing.T) {
	uri, user, password := os.Getenv("REPOSENSE_TEST_NEO4J_URI"), os.Getenv("REPOSENSE_TEST_NEO4J_USER"), os.Getenv("REPOSENSE_TEST_NEO4J_PASSWORD")
	if uri == "" || user == "" || password == "" {
		t.Fatal("REPOSENSE_TEST_NEO4J_URI, REPOSENSE_TEST_NEO4J_USER and REPOSENSE_TEST_NEO4J_PASSWORD are required for the integration gate")
	}
	database := os.Getenv("REPOSENSE_TEST_NEO4J_DATABASE")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, err := NewGraphDataStore(ctx, uri, user, password, database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	applyGraphNeo4jMigrations(t, ctx, store.driver, database)

	suffix := fmt.Sprint(time.Now().UnixNano())
	now := time.Now().UTC()
	scope := common.Scope{TenantID: "tenant-" + suffix, RepositoryID: "repo-" + suffix, SnapshotID: "snapshot-" + suffix, TraceID: "trace-" + suffix}
	job := graph.BuildJob{JobID: "job-" + suffix, Scope: scope, IdempotencyKey: "key-" + suffix, RequestFingerprint: strings.Repeat("a", 64), CommitSHA: "commit-" + suffix,
		Versions: graph.BuildVersions{ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1"},
		Status:   graph.JobBuilding, RevisionID: "revision-" + suffix, EventID: "event-" + suffix, CreatedAt: now, UpdatedAt: now}
	attempt := graph.BuildAttempt{AttemptID: "attempt-" + suffix, JobID: job.JobID, RevisionID: job.RevisionID, Status: graph.AttemptRunning,
		LeaseOwner: "worker-1", LeaseExpiresAt: now.Add(time.Minute), Fence: 1, CreatedAt: now, UpdatedAt: now}
	t.Cleanup(func() { _ = store.DeleteCandidate(context.Background(), scope, attempt.RevisionID, attempt.Fence) })

	if err := store.CreateCandidate(ctx, job, attempt); err != nil {
		t.Fatal(err)
	}
	if status, err := store.CandidateStatus(ctx, scope, attempt.RevisionID); err != nil || status != graph.CandidateStaging {
		t.Fatalf("candidate status=%s err=%v", status, err)
	}
	if _, err := store.QueryRevision(ctx, attempt.RevisionID, graph.Query{Scope: scope, RootIDs: []string{"a"}, Depth: 1, Limit: 10}); !graph.IsCode(err, graph.ErrRevisionNotFound) {
		t.Fatalf("STAGING candidate must not be queryable: %v", err)
	}

	ref := func(path, symbol string) common.SourceRef {
		return common.SourceRef{CommitSHA: job.CommitSHA, Path: path, SymbolID: symbol, StartLine: 1, EndLine: 2, ContentHash: "hash-" + symbol}
	}
	artifacts := []repository.CodeArtifact{
		{ArtifactID: "a", Kind: repository.ArtifactFunction, Name: "A", QualifiedName: "pkg.A", Language: "go", SourceRef: ref("a.go", "a"), ContentHash: "ha"},
		{ArtifactID: "b", Kind: repository.ArtifactFunction, Name: "B", QualifiedName: "pkg.B", Language: "go", SourceRef: ref("b.go", "b"), ContentHash: "hb"},
		{ArtifactID: "c", Kind: repository.ArtifactFunction, Name: "C", QualifiedName: "pkg.C", Language: "go", SourceRef: ref("c.go", "c"), ContentHash: "hc"},
	}
	if err := store.WriteArtifactBatch(ctx, job, attempt, artifacts); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteArtifactBatch(ctx, job, attempt, artifacts); err != nil {
		t.Fatalf("artifact batch retry must be idempotent: %v", err)
	}
	if err := store.CompleteArtifactStage(ctx, job, attempt, len(artifacts)); err != nil {
		t.Fatal(err)
	}
	relations := []graph.ResolvedRelation{
		{RelationID: "r1", Kind: repository.RelationCalls, FromArtifactID: "a", TargetArtifactID: "b", ResolutionStatus: graph.ResolutionResolved, Evidence: ref("a.go", "r1"), Confidence: 1},
		{RelationID: "r2", Kind: repository.RelationCalls, FromArtifactID: "a", RawTargetSymbol: "C", ResolutionStatus: graph.ResolutionAmbiguous, AmbiguousCandidates: []string{"b", "c"}, ResolutionReasonCode: "MULTIPLE", Evidence: ref("a.go", "r2"), Confidence: .5},
		{RelationID: "r3", Kind: repository.RelationCalls, FromArtifactID: "b", RawTargetSymbol: "Missing", ResolutionStatus: graph.ResolutionUnresolved, ResolutionReasonCode: "NOT_FOUND", Evidence: ref("b.go", "r3"), Confidence: .2},
	}
	if err := store.WriteRelationBatch(ctx, job, attempt, relations); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteRelationBatch(ctx, job, attempt, relations); err != nil {
		t.Fatalf("relation batch retry must be idempotent: %v", err)
	}
	stats := graph.RevisionStats{Nodes: 3, Edges: 3, AmbiguousRelations: 1, UnresolvedTargets: 1}
	if err := store.SealCandidate(ctx, job, attempt, stats, graph.QualityHealthy); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteArtifactBatch(ctx, job, attempt, artifacts[:1]); !graph.IsCode(err, graph.ErrGraphValidationFailed) {
		t.Fatalf("SEALED candidate must be immutable: %v", err)
	}
	revision := graph.Revision{EntityMeta: graph.NewMeta(attempt.RevisionID, scope, graph.RevisionActive, now), RevisionID: attempt.RevisionID,
		SnapshotID: scope.SnapshotID, CommitSHA: job.CommitSHA, BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive,
		AlgorithmVersion: job.Versions.GraphAlgorithmVersion, ParserResultVersion: job.Versions.ParserResultVersion,
		GraphSchemaVersion: job.Versions.GraphSchemaVersion, BuildPolicyVersion: job.Versions.BuildPolicyVersion,
		QualityStatus: graph.QualityHealthy, RequestFingerprint: job.RequestFingerprint, Stats: stats}
	if err := store.VerifyRevision(ctx, revision); err != nil {
		t.Fatal(err)
	}
	result, err := store.QueryRevision(ctx, revision.RevisionID, graph.Query{Scope: scope, RootIDs: []string{"a"}, Direction: graph.DirectionOutgoing, Depth: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Nodes) != 2 || len(result.Edges) != 1 || result.Edges[0].EdgeID != "r1" {
		t.Fatalf("business query leaked diagnostics or lost facts: %#v", result)
	}
	diagnostics, err := store.QueryRevisionDiagnostics(ctx, revision.RevisionID, graph.DiagnosticQuery{Scope: scope, ArtifactIDs: []string{"a", "b"}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnostics.Issues) != 2 || diagnostics.Issues[0].RelationID != "r2" || len(diagnostics.Issues[0].CandidateArtifactIDs) != 2 || diagnostics.Issues[1].RelationID != "r3" || len(diagnostics.Issues[1].CandidateArtifactIDs) != 0 {
		t.Fatalf("diagnostic graph mismatch: %#v", diagnostics)
	}
	otherScope := scope
	otherScope.TenantID = "other-tenant"
	if _, err := store.QueryRevision(ctx, revision.RevisionID, graph.Query{Scope: otherScope, RootIDs: []string{"a"}, Depth: 0, Limit: 10}); !graph.IsCode(err, graph.ErrRevisionNotFound) {
		t.Fatalf("cross-tenant query must fail closed: %v", err)
	}
}

func TestGraphDataMigrationsRoundTrip(t *testing.T) {
	uri, user, password := os.Getenv("REPOSENSE_TEST_NEO4J_URI"), os.Getenv("REPOSENSE_TEST_NEO4J_USER"), os.Getenv("REPOSENSE_TEST_NEO4J_PASSWORD")
	if uri == "" || user == "" || password == "" {
		t.Fatal("REPOSENSE_TEST_NEO4J_URI, REPOSENSE_TEST_NEO4J_USER and REPOSENSE_TEST_NEO4J_PASSWORD are required for the integration gate")
	}
	database := os.Getenv("REPOSENSE_TEST_NEO4J_DATABASE")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, err := NewGraphDataStore(ctx, uri, user, password, database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	applyGraphNeo4jMigrations(t, ctx, store.driver, database)
	executeGraphNeo4jMigrations(t, ctx, store.driver, database, []string{
		"000003_graph_observability.down.cypher",
		"000002_graph_data_plane.down.cypher",
		"000001_code_knowledge_graph.down.cypher",
	})

	session := store.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: database})
	defer session.Close(ctx)
	for _, query := range []string{
		`SHOW CONSTRAINTS YIELD name WHERE name STARTS WITH 'graph_' RETURN count(*) AS remaining`,
		`SHOW INDEXES YIELD name WHERE name STARTS WITH 'graph_' RETURN count(*) AS remaining`,
	} {
		remaining, err := session.ExecuteRead(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
			result, runErr := tx.Run(ctx, query, nil)
			if runErr != nil {
				return nil, runErr
			}
			record, runErr := result.Single(ctx)
			if runErr != nil {
				return nil, runErr
			}
			value, _ := record.Get("remaining")
			return value, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if count, ok := remaining.(int64); !ok || count != 0 {
			t.Fatalf("Neo4j down migrations left graph schema objects: %#v", remaining)
		}
	}
}

func applyGraphNeo4jMigrations(t *testing.T, ctx context.Context, driver neo4jdriver.Driver, database string) {
	t.Helper()
	executeGraphNeo4jMigrations(t, ctx, driver, database, []string{"000001_code_knowledge_graph.up.cypher", "000002_graph_data_plane.up.cypher", "000003_graph_observability.up.cypher"})
}

func executeGraphNeo4jMigrations(t *testing.T, ctx context.Context, driver neo4jdriver.Driver, database string, names []string) {
	t.Helper()
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: database})
	defer session.Close(ctx)
	for _, name := range names {
		contents, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "neo4j", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range strings.Split(string(contents), ";") {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				result, runErr := tx.Run(ctx, statement, nil)
				if runErr != nil {
					return nil, runErr
				}
				_, consumeErr := result.Consume(ctx)
				return nil, consumeErr
			}); err != nil {
				t.Fatalf("apply %s: %v", name, err)
			}
		}
	}
}
