//go:build integration

package graphintegration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	natsadapter "github.com/reposense/reposense/internal/adapters/nats"
	neo4jadapter "github.com/reposense/reposense/internal/adapters/neo4j"
	postgresadapter "github.com/reposense/reposense/internal/adapters/postgres"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type integrationClock struct{ now time.Time }

func (c integrationClock) Now() time.Time { return c.now }

type integrationIDs struct {
	mu   sync.Mutex
	next int
}

func (g *integrationIDs) New(prefix string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("%s-e2e-%d", prefix, g.next)
}

type integrationSource struct {
	metadata  graph.SnapshotMetadata
	artifacts []repository.CodeArtifact
	relations []graph.ResolvedRelation
}

func (s integrationSource) SnapshotMetadata(context.Context, common.Scope) (graph.SnapshotMetadata, error) {
	return s.metadata, nil
}
func (s integrationSource) ArtifactPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.ArtifactPage, error) {
	return graph.ArtifactPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor}, Artifacts: s.artifacts}, nil
}
func (s integrationSource) RelationPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.RelationPage, error) {
	return graph.RelationPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor}, Relations: s.relations}, nil
}

func TestGraphPipelineFromParseCompletedToPublishedActiveQuery(t *testing.T) {
	postgresDSN := requiredIntegrationEnv(t, "REPOSENSE_TEST_POSTGRES_DSN")
	neo4jURI := requiredIntegrationEnv(t, "REPOSENSE_TEST_NEO4J_URI")
	neo4jUser := requiredIntegrationEnv(t, "REPOSENSE_TEST_NEO4J_USER")
	neo4jPassword := requiredIntegrationEnv(t, "REPOSENSE_TEST_NEO4J_PASSWORD")
	natsURL := requiredIntegrationEnv(t, "REPOSENSE_TEST_NATS_URL")
	neo4jDatabase := os.Getenv("REPOSENSE_TEST_NEO4J_DATABASE")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	control, pool, cleanupPostgres := integrationControlStore(t, ctx, postgresDSN)
	defer cleanupPostgres()
	data, neoDriver := integrationGraphStore(t, ctx, neo4jURI, neo4jUser, neo4jPassword, neo4jDatabase)
	t.Cleanup(func() { _ = neoDriver.Close(context.Background()) })
	publisher, stream, cleanupNATS := integrationPublisher(t, ctx, natsURL)
	defer cleanupNATS()
	defer publisher.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := fmt.Sprint(now.UnixNano())
	scope := common.Scope{TenantID: "tenant-" + suffix, RepositoryID: "repo-" + suffix, SnapshotID: "snapshot-" + suffix, TraceID: "0123456789abcdef0123456789abcdef"}
	t.Cleanup(func() {
		session := neoDriver.NewSession(context.Background(), neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: neo4jDatabase})
		defer session.Close(context.Background())
		_, _ = session.ExecuteWrite(context.Background(), func(tx neo4jdriver.ManagedTransaction) (any, error) {
			result, err := tx.Run(context.Background(), `MATCH (n {tenant_id:$tenant}) DETACH DELETE n`, map[string]any{"tenant": scope.TenantID})
			if err != nil {
				return nil, err
			}
			_, consumeErr := result.Consume(context.Background())
			return nil, consumeErr
		})
	})
	ref := func(path, symbol string) common.SourceRef {
		return common.SourceRef{CommitSHA: "commit-" + suffix, Path: path, SymbolID: symbol, StartLine: 1, EndLine: 2, ContentHash: "hash-" + symbol}
	}
	source := integrationSource{
		metadata: graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit-" + suffix, ParserResultVersion: "parser-v1", ArtifactCount: 3, RelationCount: 2, ParseResultChecksum: "checksum-" + suffix, SchemaVersion: "parser-schema-v1", CompletedAt: now},
		artifacts: []repository.CodeArtifact{
			{ArtifactID: "a", Kind: repository.ArtifactFunction, Name: "A", QualifiedName: "pkg.A", Language: "go", SourceRef: ref("a.go", "a"), ContentHash: "ha"},
			{ArtifactID: "b", Kind: repository.ArtifactFunction, Name: "B", QualifiedName: "pkg.B", Language: "go", SourceRef: ref("b.go", "b"), ContentHash: "hb"},
			{ArtifactID: "c", Kind: repository.ArtifactFunction, Name: "C", QualifiedName: "pkg.C", Language: "go", SourceRef: ref("c.go", "c"), ContentHash: "hc"},
		},
		relations: []graph.ResolvedRelation{
			{RelationID: "r1", Kind: repository.RelationCalls, FromArtifactID: "a", TargetArtifactID: "b", ResolutionStatus: graph.ResolutionResolved, Evidence: ref("a.go", "r1"), Confidence: 1},
			{RelationID: "r2", Kind: repository.RelationCalls, FromArtifactID: "a", RawTargetSymbol: "C", ResolutionStatus: graph.ResolutionAmbiguous, AmbiguousCandidates: []string{"b", "c"}, ResolutionReasonCode: "MULTIPLE", Evidence: ref("a.go", "r2"), Confidence: .5},
		},
	}
	reader, err := graphapp.NewSourceReader(source, 100, []string{"parser-schema-v1"})
	if err != nil {
		t.Fatal(err)
	}
	ids, clock := &integrationIDs{}, integrationClock{now: now}
	consumer, err := graphapp.NewConsumer(control, control, nil, ids, clock, graphapp.ConsumerConfig{GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1", MaxEventBytes: 1 << 20, MaxPendingGlobal: 100, MaxPendingPerTenant: 10, StoreTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	parseEvent := common.EventEnvelope{EventID: "parse-" + suffix, EventType: "parse.completed.v1", AggregateID: scope.SnapshotID, OccurredAt: now, Producer: "repository-parser", PayloadVersion: 1, TraceID: scope.TraceID,
		Payload: map[string]any{"snapshot_id": scope.SnapshotID, "commit_sha": source.metadata.CommitSHA, "parser_result_version": source.metadata.ParserResultVersion, "artifact_count": source.metadata.ArtifactCount, "relation_count": source.metadata.RelationCount}}
	consumed, err := consumer.Handle(ctx, scope, parseEvent)
	if err != nil || consumed.Disposition != graphapp.ConsumerAck || !consumed.Created {
		t.Fatalf("consumer result=%#v err=%v", consumed, err)
	}
	workerConfig := graphapp.DefaultWorkerConfig()
	workerConfig.LeaseDuration, workerConfig.HeartbeatInterval = 30*time.Second, 5*time.Second
	workerConfig.BuildTimeout, workerConfig.ControlTimeout = 30*time.Second, 5*time.Second
	workerConfig.BatchSize, workerConfig.SourceAttempts, workerConfig.BatchAttempts = 2, 2, 2
	workerConfig.RetryInitialBackoff, workerConfig.RetryMaxBackoff = 10*time.Millisecond, 50*time.Millisecond
	workerConfig.MaxConcurrentGlobal, workerConfig.MaxConcurrentTenant = 1, 1
	worker, err := graphapp.NewWorker(reader, control, data, nil, ids, clock, workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(ctx, "worker-e2e")
	if err != nil || !processed {
		t.Fatalf("worker processed=%v err=%v", processed, err)
	}
	active, err := control.ActiveGraphRevision(ctx, scope)
	if err != nil || active.Stats.Nodes != 3 || active.Stats.Edges != 2 || active.Stats.AmbiguousRelations != 1 {
		t.Fatalf("active revision=%#v err=%v", active, err)
	}
	queryService, err := graphapp.NewQueryService(control, data, nil, graphapp.QueryServiceConfig{Timeout: 10 * time.Second, MaxConcurrent: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := queryService.Query(ctx, graph.Query{Scope: scope, RootIDs: []string{"a"}, Direction: graph.DirectionOutgoing, Depth: 2, Limit: 10})
	if err != nil || len(result.Nodes) != 2 || len(result.Edges) != 1 || result.Edges[0].EdgeID != "r1" {
		t.Fatalf("active query=%#v err=%v", result, err)
	}
	diagnostics, err := queryService.QueryDiagnostics(ctx, graph.DiagnosticQuery{Scope: scope, ArtifactIDs: []string{"a"}, Limit: 10})
	if err != nil || len(diagnostics.Issues) != 1 || diagnostics.Issues[0].RelationID != "r2" {
		t.Fatalf("diagnostics=%#v err=%v", diagnostics, err)
	}
	dispatcher, err := graphapp.NewGraphOutboxDispatcher(control, publisher, nil, clock, graphapp.GraphOutboxConfig{BatchSize: 10, MaxAttempts: 5, ClaimDuration: 30 * time.Second, PublishTimeout: time.Second, StoreTimeout: time.Second, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	published, err := dispatcher.DispatchOnce(ctx)
	if err != nil || published != 1 {
		t.Fatalf("outbox published=%d err=%v", published, err)
	}
	message, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var publishedEvent common.EventEnvelope
	if err := json.Unmarshal(message.Data, &publishedEvent); err != nil || publishedEvent.EventID != consumed.Job.EventID || publishedEvent.AggregateID != active.RevisionID {
		t.Fatalf("published event=%#v err=%v", publishedEvent, err)
	}
	reconciler, err := graphapp.NewReconciler(control, control, data, nil, ids, clock, graphapp.ReconcilerConfig{BatchSize: 100, OrphanRetention: time.Hour, OperationTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.RunOnce(ctx)
	if err != nil || report.VerifiedActive != 1 || report.Inconsistent != 0 {
		t.Fatalf("reconciliation report=%#v err=%v", report, err)
	}
	var jobs, revisions, outbox int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM graph_build_jobs),(SELECT count(*) FROM graph_revisions),(SELECT count(*) FROM graph_outbox_events)`).Scan(&jobs, &revisions, &outbox); err != nil || jobs != 1 || revisions != 1 || outbox != 1 {
		t.Fatalf("control-plane cardinality jobs=%d revisions=%d outbox=%d err=%v", jobs, revisions, outbox, err)
	}
	var fence int64
	if err := pool.QueryRow(ctx, `SELECT fence FROM graph_build_attempts WHERE revision_id=$1`, active.RevisionID).Scan(&fence); err != nil {
		t.Fatal(err)
	}
	if err := data.DeleteCandidate(ctx, scope, active.RevisionID, fence); err != nil {
		t.Fatalf("inject ACTIVE graph loss: %v", err)
	}
	if _, err := queryService.Query(ctx, graph.Query{Scope: scope, RootIDs: []string{"a"}, Depth: 0, Limit: 10}); !graph.IsCode(err, graph.ErrGraphInconsistent) {
		t.Fatalf("lost ACTIVE data must fail closed with GRAPH_INCONSISTENT: %v", err)
	}
	report, err = reconciler.RunOnce(ctx)
	if report.Inconsistent != 1 || !graph.IsCode(err, graph.ErrGraphInconsistent) {
		t.Fatalf("reconciler must detect injected ACTIVE loss: report=%#v err=%v", report, err)
	}
}

func requiredIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for the integration gate", name)
	}
	return value
}

func integrationControlStore(t *testing.T, ctx context.Context, dsn string) (*postgresadapter.GraphControlStore, *pgxpool.Pool, func()) {
	t.Helper()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("graph_e2e_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"000002_code_knowledge_graph.up.sql", "000003_graph_runtime.up.sql"} {
		if _, err := pool.Exec(ctx, string(readIntegrationFile(t, "migrations", "postgres", name))); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	}
	return postgresadapter.NewGraphControlStoreWithPool(pool), pool, cleanup
}

func integrationGraphStore(t *testing.T, ctx context.Context, uri, user, password, database string) (*neo4jadapter.GraphDataStore, neo4jdriver.Driver) {
	t.Helper()
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.BasicAuth(user, password, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		t.Fatal(err)
	}
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: database})
	defer session.Close(ctx)
	for _, name := range []string{"000001_code_knowledge_graph.up.cypher", "000002_graph_data_plane.up.cypher", "000003_graph_observability.up.cypher"} {
		for _, statement := range strings.Split(string(readIntegrationFile(t, "migrations", "neo4j", name)), ";") {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, statement, nil)
				if err != nil {
					return nil, err
				}
				_, consumeErr := result.Consume(ctx)
				return nil, consumeErr
			}); err != nil {
				driver.Close(ctx)
				t.Fatalf("apply %s: %v", name, err)
			}
		}
	}
	return neo4jadapter.NewGraphDataStoreWithDriver(driver, database), driver
}

func integrationPublisher(t *testing.T, ctx context.Context, url string) (*natsadapter.Publisher, jetstream.Stream, func()) {
	t.Helper()
	connection, err := gonats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("GRAPH_E2E_%d", time.Now().UnixNano())
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{"graph.published.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := natsadapter.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	return publisher, stream, func() { _ = js.DeleteStream(context.Background(), name); connection.Close() }
}

func readIntegrationFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve integration source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(current), "..", "..", ".."))
	contents, err := os.ReadFile(filepath.Join(append([]string{root}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return contents
}
