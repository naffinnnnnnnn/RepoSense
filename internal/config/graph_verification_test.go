package config

import (
	"testing"
	"time"

	neo4jadapter "github.com/reposense/reposense/internal/adapters/neo4j"
	graphapp "github.com/reposense/reposense/internal/application/graph"
)

func TestGraphRuntimeValidatesEveryIndependentProductionRole(t *testing.T) {
	for _, role := range []GraphRole{GraphRoleAPI, GraphRoleConsumer, GraphRoleWorker, GraphRoleOutbox, GraphRoleReconciler} {
		config := validGraphRuntimeForVerification(role)
		if err := config.Validate(); err != nil {
			t.Fatalf("role %s rejected valid production configuration: %v", role, err)
		}
	}
}

func TestGraphRuntimeFailsClosedForInsecureDependenciesAndDangerousBudgets(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*GraphRuntime)
	}{
		{"postgres plaintext", func(c *GraphRuntime) { c.PostgresDSN = "postgres://db/graph?sslmode=disable" }},
		{"neo4j plaintext", func(c *GraphRuntime) { c.Neo4jURI = "bolt://neo4j:7687" }},
		{"nats plaintext", func(c *GraphRuntime) { c.Role, c.NATSURL = GraphRoleConsumer, "nats://nats:4222" }},
		{"otel plaintext", func(c *GraphRuntime) { c.OTLPEndpoint = "http://otel:4318" }},
		{"shared listeners", func(c *GraphRuntime) { c.HTTPAddress = c.AdminAddress }},
		{"repository concurrency", func(c *GraphRuntime) { c.Role, c.Worker.MaxConcurrentRepository = GraphRoleWorker, 2 }},
		{"missing parser contract", func(c *GraphRuntime) { c.SupportedParserSchemas = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validGraphRuntimeForVerification(GraphRoleAPI)
			test.mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("dangerous production configuration was accepted")
			}
		})
	}
}

func validGraphRuntimeForVerification(role GraphRole) GraphRuntime {
	worker := graphapp.DefaultWorkerConfig()
	return GraphRuntime{
		Role: role, PostgresDSN: "postgres://graph@postgres/graph?sslmode=verify-full",
		Neo4jURI: "neo4j+s://neo4j:7687", Neo4jUser: "graph", Neo4jPass: "secret", Neo4jDB: "neo4j",
		NATSURL: "tls://nats:4222", NATSCredentialsFile: "/run/secrets/graph.creds",
		HTTPAddress: ":8081", AdminAddress: ":9091", WorkerOwner: "worker-1",
		TLSCertFile: "/run/secrets/tls.crt", TLSKeyFile: "/run/secrets/tls.key", OTLPEndpoint: "https://otel:4318",
		ParserPageSize: 1000, SupportedParserSchemas: []string{"parser-v1"}, Worker: worker,
		Admission: graphapp.BuildAdmissionConfig{GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1", MaxPendingGlobal: 100, MaxPendingPerTenant: 10, MetadataTimeout: time.Second, StoreTimeout: time.Second},
		Consumer:  graphapp.ConsumerConfig{GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1", MaxEventBytes: 1024, MaxPendingGlobal: 100, MaxPendingPerTenant: 10, StoreTimeout: time.Second},
		Query:     graphapp.QueryServiceConfig{Timeout: time.Second, MaxConcurrent: 10}, Data: neo4jadapter.DefaultGraphDataConfig(),
		Outbox:          graphapp.GraphOutboxConfig{BatchSize: 10, MaxAttempts: 5, ClaimDuration: 30 * time.Second, PublishTimeout: time.Second, StoreTimeout: time.Second, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second},
		Reconciler:      graphapp.ReconcilerConfig{BatchSize: 10, OrphanRetention: time.Hour, OperationTimeout: time.Second},
		WorkerIdleDelay: time.Second, WorkerFailureDelay: time.Second, OutboxInterval: time.Second, ReconcilerInterval: time.Second,
		ConsumerStream: "PARSER", ConsumerDurable: "GRAPH", ConsumerFetchBatch: 10, ConsumerFetchWait: time.Second, ConsumerHandleTimeout: time.Second, ConsumerRetryDelay: time.Second,
		ReadinessTimeout: time.Second, StartupTimeout: time.Second, ShutdownTimeout: time.Second, MetricsInterval: time.Second,
	}
}
