package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	neo4jadapter "github.com/reposense/reposense/internal/adapters/neo4j"
	graphapp "github.com/reposense/reposense/internal/application/graph"
)

type GraphRole string

const (
	GraphRoleAPI        GraphRole = "graph-api"
	GraphRoleConsumer   GraphRole = "graph-consumer"
	GraphRoleWorker     GraphRole = "graph-worker"
	GraphRoleOutbox     GraphRole = "graph-outbox"
	GraphRoleReconciler GraphRole = "graph-reconciler"
)

func ParseGraphRole(value string) (GraphRole, error) {
	role := GraphRole(strings.TrimSpace(value))
	switch role {
	case GraphRoleAPI, GraphRoleConsumer, GraphRoleWorker, GraphRoleOutbox, GraphRoleReconciler:
		return role, nil
	default:
		return "", fmt.Errorf("未知 Graph 运行角色 %q", value)
	}
}

// GraphRuntime is the validated, immutable configuration shared by the five
// independently deployed Graph roles. Role-specific validation prevents a
// process from silently starting without one of its required dependencies.
type GraphRuntime struct {
	Role GraphRole

	PostgresDSN         string
	Neo4jURI            string
	Neo4jUser           string
	Neo4jPass           string
	Neo4jDB             string
	NATSURL             string
	NATSCredentialsFile string

	HTTPAddress  string
	AdminAddress string
	WorkerOwner  string
	TLSCertFile  string
	TLSKeyFile   string
	OTLPEndpoint string

	ParserPageSize         int
	SupportedParserSchemas []string
	Worker                 graphapp.WorkerConfig
	Admission              graphapp.BuildAdmissionConfig
	Consumer               graphapp.ConsumerConfig
	Query                  graphapp.QueryServiceConfig
	Data                   neo4jadapter.GraphDataConfig
	Outbox                 graphapp.GraphOutboxConfig
	Reconciler             graphapp.ReconcilerConfig

	WorkerIdleDelay       time.Duration
	WorkerFailureDelay    time.Duration
	OutboxInterval        time.Duration
	ReconcilerInterval    time.Duration
	ConsumerStream        string
	ConsumerDurable       string
	ConsumerFetchBatch    int
	ConsumerFetchWait     time.Duration
	ConsumerHandleTimeout time.Duration
	ConsumerRetryDelay    time.Duration
	ReadinessTimeout      time.Duration
	StartupTimeout        time.Duration
	ShutdownTimeout       time.Duration
	MetricsInterval       time.Duration
}

func LoadGraphRuntime(role GraphRole) (GraphRuntime, error) {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	versions := struct{ schema, algorithm, policy string }{
		schema:    strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_SCHEMA_VERSION")),
		algorithm: strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_ALGORITHM_VERSION")),
		policy:    strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_BUILD_POLICY_VERSION")),
	}
	c := GraphRuntime{
		Role:                   role,
		PostgresDSN:            strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_POSTGRES_DSN")),
		Neo4jURI:               strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NEO4J_URI")),
		Neo4jUser:              strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NEO4J_USERNAME")),
		Neo4jPass:              os.Getenv("REPOSENSE_GRAPH_NEO4J_PASSWORD"),
		Neo4jDB:                strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NEO4J_DATABASE")),
		NATSURL:                strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NATS_URL")),
		NATSCredentialsFile:    strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NATS_CREDENTIALS_FILE")),
		HTTPAddress:            envString("REPOSENSE_GRAPH_HTTP_ADDRESS", ":8081"),
		AdminAddress:           envString("REPOSENSE_GRAPH_ADMIN_ADDRESS", ":9091"),
		WorkerOwner:            envString("REPOSENSE_GRAPH_WORKER_OWNER", host),
		TLSCertFile:            strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_TLS_CERT_FILE")),
		TLSKeyFile:             strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_TLS_KEY_FILE")),
		OTLPEndpoint:           strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_OTEL_ENDPOINT")),
		ParserPageSize:         1000,
		SupportedParserSchemas: envExactList("REPOSENSE_GRAPH_PARSER_SCHEMAS"),
		Worker:                 graphapp.DefaultWorkerConfig(),
		Admission: graphapp.BuildAdmissionConfig{
			GraphSchemaVersion: versions.schema, GraphAlgorithmVersion: versions.algorithm,
			BuildPolicyVersion: versions.policy, MaxPendingGlobal: 10_000,
			MaxPendingPerTenant: 1_000, MetadataTimeout: 10 * time.Second, StoreTimeout: 10 * time.Second,
		},
		Consumer: graphapp.ConsumerConfig{
			GraphSchemaVersion: versions.schema, GraphAlgorithmVersion: versions.algorithm,
			BuildPolicyVersion: versions.policy, MaxEventBytes: 1 << 20,
			MaxPendingGlobal: 10_000, MaxPendingPerTenant: 1_000, StoreTimeout: 10 * time.Second,
		},
		Query: graphapp.DefaultQueryServiceConfig(),
		Data:  neo4jadapter.DefaultGraphDataConfig(),
		Outbox: graphapp.GraphOutboxConfig{
			BatchSize: 100, MaxAttempts: 10, ClaimDuration: 20 * time.Minute,
			PublishTimeout: 5 * time.Second, StoreTimeout: 5 * time.Second,
			BaseBackoff: time.Second, MaxBackoff: 5 * time.Minute,
		},
		Reconciler:      graphapp.ReconcilerConfig{BatchSize: 100, OrphanRetention: 24 * time.Hour, OperationTimeout: 5 * time.Minute},
		WorkerIdleDelay: time.Second, WorkerFailureDelay: 5 * time.Second,
		OutboxInterval: time.Second, ReconcilerInterval: time.Minute,
		ConsumerStream:     strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NATS_STREAM")),
		ConsumerDurable:    strings.TrimSpace(os.Getenv("REPOSENSE_GRAPH_NATS_DURABLE")),
		ConsumerFetchBatch: 100, ConsumerFetchWait: time.Second,
		ConsumerHandleTimeout: 30 * time.Second, ConsumerRetryDelay: 5 * time.Second,
		ReadinessTimeout: 3 * time.Second, StartupTimeout: 30 * time.Second, ShutdownTimeout: 30 * time.Second,
		MetricsInterval: 15 * time.Second,
	}
	if err := loadGraphNumbers(&c); err != nil {
		return c, err
	}
	if err := loadGraphDurations(&c); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	if _, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile); err != nil {
		return c, errors.New("Graph TLS certificate or private key is invalid")
	}
	if c.requiresNATS() {
		info, err := os.Stat(c.NATSCredentialsFile)
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return c, errors.New("Graph NATS credentials secret file is invalid")
		}
	}
	return c, nil
}

func (c GraphRuntime) Validate() error {
	if _, err := ParseGraphRole(string(c.Role)); err != nil {
		return err
	}
	if strings.TrimSpace(c.PostgresDSN) == "" {
		return errors.New("REPOSENSE_GRAPH_POSTGRES_DSN 必须配置")
	}
	if !securePostgresDSN(c.PostgresDSN) {
		return errors.New("Graph PostgreSQL 必须启用 TLS")
	}
	if c.requiresNeo4j() && (strings.TrimSpace(c.Neo4jURI) == "" || strings.TrimSpace(c.Neo4jUser) == "" || c.Neo4jPass == "") {
		return errors.New("当前 Graph 角色必须配置 Neo4j URI、用户名和密码")
	}
	if c.requiresNATS() && (strings.TrimSpace(c.NATSURL) == "" || strings.TrimSpace(c.NATSCredentialsFile) == "") {
		return errors.New("当前 Graph 角色必须配置 NATS URL 和 credentials secret file")
	}
	if c.requiresNeo4j() && !secureNeo4jURI(c.Neo4jURI) {
		return errors.New("Graph Neo4j 必须使用经过验证的 TLS URI")
	}
	if c.requiresNATS() && !secureNATSURLs(c.NATSURL) {
		return errors.New("Graph NATS 必须使用 TLS URL")
	}
	if !validListenAddress(c.AdminAddress) || strings.TrimSpace(c.TLSCertFile) == "" || strings.TrimSpace(c.TLSKeyFile) == "" {
		return errors.New("Graph admin 地址、TLS certificate 和 private key 必须配置")
	}
	if !secureHTTPSURL(c.OTLPEndpoint) {
		return errors.New("Graph OTLP endpoint 必须是有效的 HTTPS URL")
	}
	if c.Role == GraphRoleConsumer && (strings.TrimSpace(c.ConsumerStream) == "" || strings.TrimSpace(c.ConsumerDurable) == "") {
		return errors.New("graph-consumer 必须配置 NATS stream 和 durable consumer")
	}
	if c.Role == GraphRoleAPI && (!validListenAddress(c.HTTPAddress) || c.HTTPAddress == c.AdminAddress) {
		return errors.New("Graph HTTP 与 admin 必须是不同的有效监听地址")
	}
	if c.Role == GraphRoleWorker && (strings.TrimSpace(c.WorkerOwner) == "" || c.WorkerOwner != strings.TrimSpace(c.WorkerOwner)) {
		return errors.New("Graph Worker owner 必须是非空精确值")
	}
	if (c.Role == GraphRoleAPI || c.Role == GraphRoleWorker) && (c.ParserPageSize <= 0 || c.ParserPageSize > 10_000 || len(c.SupportedParserSchemas) == 0) {
		return errors.New("Parser 页面大小必须在 1..10000 且至少配置一个受支持 Schema")
	}
	for _, schema := range c.SupportedParserSchemas {
		if strings.TrimSpace(schema) == "" || schema != strings.TrimSpace(schema) {
			return errors.New("Parser Schema 必须是非空精确值")
		}
	}
	if c.Role == GraphRoleWorker {
		if err := c.Worker.Validate(); err != nil {
			return fmt.Errorf("Graph Worker 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleAPI {
		if err := c.Admission.Validate(); err != nil {
			return fmt.Errorf("Graph Admission 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleConsumer {
		if err := c.Consumer.Validate(); err != nil {
			return fmt.Errorf("Graph Consumer 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleAPI {
		if err := c.Query.Validate(); err != nil {
			return fmt.Errorf("Graph Query 配置无效: %w", err)
		}
	}
	if c.requiresNeo4j() {
		if err := c.Data.Validate(); err != nil {
			return fmt.Errorf("Graph Neo4j 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleOutbox {
		if err := c.Outbox.Validate(); err != nil {
			return fmt.Errorf("Graph Outbox 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleReconciler {
		if err := c.Reconciler.Validate(); err != nil {
			return fmt.Errorf("Graph Reconciler 配置无效: %w", err)
		}
	}
	if c.Role == GraphRoleWorker && (c.Worker.MaxConcurrentGlobal <= 0 || c.Worker.MaxConcurrentTenant <= 0 || c.Worker.MaxConcurrentRepository != 1 || c.Worker.MaxConcurrentTenant > c.Worker.MaxConcurrentGlobal) {
		return errors.New("Graph 构建并发必须满足 repository=1、tenant<=global")
	}
	if c.Role == GraphRoleWorker && (c.WorkerIdleDelay <= 0 || c.WorkerFailureDelay <= 0) {
		return errors.New("Graph Worker 轮询配置必须为正数")
	}
	if c.Role == GraphRoleOutbox && c.OutboxInterval <= 0 {
		return errors.New("Graph Outbox 轮询配置必须为正数")
	}
	if c.Role == GraphRoleReconciler && c.ReconcilerInterval <= 0 {
		return errors.New("Graph Reconciler 轮询配置必须为正数")
	}
	if c.Role == GraphRoleConsumer && (c.ConsumerFetchBatch <= 0 || c.ConsumerFetchBatch > 1000 || c.ConsumerFetchWait <= 0 || c.ConsumerHandleTimeout <= 0 || c.ConsumerRetryDelay <= 0) {
		return errors.New("Graph Consumer 批次和超时配置必须为正数")
	}
	if c.Role == GraphRoleConsumer && (c.ConsumerFetchWait > time.Minute || c.ConsumerHandleTimeout > 5*time.Minute || c.ConsumerRetryDelay > time.Hour) {
		return errors.New("Graph Consumer fetch、处理和重试时长超过安全上限")
	}
	if c.ReadinessTimeout <= 0 || c.ReadinessTimeout > time.Minute || c.StartupTimeout <= 0 || c.StartupTimeout > 5*time.Minute || c.ShutdownTimeout <= 0 || c.ShutdownTimeout > 5*time.Minute || c.MetricsInterval <= 0 || c.MetricsInterval > 5*time.Minute {
		return errors.New("Graph readiness、startup 和 shutdown 超时必须位于安全范围")
	}
	return nil
}

func (c GraphRuntime) requiresNeo4j() bool {
	return c.Role == GraphRoleAPI || c.Role == GraphRoleWorker || c.Role == GraphRoleReconciler
}

func (c GraphRuntime) requiresNATS() bool {
	return c.Role == GraphRoleConsumer || c.Role == GraphRoleOutbox
}

func loadGraphNumbers(c *GraphRuntime) (err error) {
	bindings := []struct {
		name   string
		target *int
	}{
		{"REPOSENSE_GRAPH_PARSER_PAGE_SIZE", &c.ParserPageSize},
		{"REPOSENSE_GRAPH_WORKER_BATCH_SIZE", &c.Worker.BatchSize},
		{"REPOSENSE_GRAPH_WORKER_MAX_PROPERTIES", &c.Worker.MaxProperties},
		{"REPOSENSE_GRAPH_WORKER_MAX_CANDIDATES", &c.Worker.MaxCandidates},
		{"REPOSENSE_GRAPH_WORKER_MAX_ERROR_SAMPLES", &c.Worker.MaxErrorSamples},
		{"REPOSENSE_GRAPH_WORKER_MAX_ARTIFACT_BYTES", &c.Worker.MaxArtifactBytes},
		{"REPOSENSE_GRAPH_WORKER_MAX_RELATION_BYTES", &c.Worker.MaxRelationBytes},
		{"REPOSENSE_GRAPH_WORKER_MAX_BATCH_BYTES", &c.Worker.MaxBatchBytes},
		{"REPOSENSE_GRAPH_WORKER_SOURCE_ATTEMPTS", &c.Worker.SourceAttempts},
		{"REPOSENSE_GRAPH_WORKER_BATCH_ATTEMPTS", &c.Worker.BatchAttempts},
		{"REPOSENSE_GRAPH_WORKER_MAX_CONCURRENT_GLOBAL", &c.Worker.MaxConcurrentGlobal},
		{"REPOSENSE_GRAPH_WORKER_MAX_CONCURRENT_TENANT", &c.Worker.MaxConcurrentTenant},
		{"REPOSENSE_GRAPH_WORKER_MAX_CONCURRENT_REPOSITORY", &c.Worker.MaxConcurrentRepository},
		{"REPOSENSE_GRAPH_MAX_PENDING_GLOBAL", &c.Admission.MaxPendingGlobal},
		{"REPOSENSE_GRAPH_MAX_PENDING_TENANT", &c.Admission.MaxPendingPerTenant},
		{"REPOSENSE_GRAPH_QUERY_MAX_CONCURRENT", &c.Query.MaxConcurrent},
		{"REPOSENSE_GRAPH_QUERY_MAX_ROOTS", &c.Data.MaxRoots},
		{"REPOSENSE_GRAPH_QUERY_MAX_NODES", &c.Data.MaxNodes},
		{"REPOSENSE_GRAPH_QUERY_MAX_EDGES", &c.Data.MaxEdges},
		{"REPOSENSE_GRAPH_QUERY_MAX_FRONTIER", &c.Data.MaxFrontier},
		{"REPOSENSE_GRAPH_QUERY_MAX_DATABASE_QUERIES", &c.Data.MaxDatabaseQueries},
		{"REPOSENSE_GRAPH_OUTBOX_BATCH_SIZE", &c.Outbox.BatchSize},
		{"REPOSENSE_GRAPH_OUTBOX_MAX_ATTEMPTS", &c.Outbox.MaxAttempts},
		{"REPOSENSE_GRAPH_RECONCILER_BATCH_SIZE", &c.Reconciler.BatchSize},
		{"REPOSENSE_GRAPH_CONSUMER_MAX_EVENT_BYTES", &c.Consumer.MaxEventBytes},
		{"REPOSENSE_GRAPH_CONSUMER_FETCH_BATCH", &c.ConsumerFetchBatch},
	}
	for _, binding := range bindings {
		if *binding.target, err = envGraphInt(binding.name, *binding.target, false); err != nil {
			return err
		}
	}
	if c.Worker.MaxArtifacts, err = envGraphInt64("REPOSENSE_GRAPH_WORKER_MAX_ARTIFACTS", c.Worker.MaxArtifacts); err != nil {
		return err
	}
	if c.Worker.MaxRelations, err = envGraphInt64("REPOSENSE_GRAPH_WORKER_MAX_RELATIONS", c.Worker.MaxRelations); err != nil {
		return err
	}
	if c.Worker.MaxRevisionBytes, err = envGraphInt64("REPOSENSE_GRAPH_WORKER_MAX_REVISION_BYTES", c.Worker.MaxRevisionBytes); err != nil {
		return err
	}
	if c.Worker.QualityPolicy.MaxInvalidArtifacts, err = envGraphInt("REPOSENSE_GRAPH_MAX_INVALID_ARTIFACTS", c.Worker.QualityPolicy.MaxInvalidArtifacts, true); err != nil {
		return err
	}
	if c.Worker.QualityPolicy.MaxInvalidRelations, err = envGraphInt("REPOSENSE_GRAPH_MAX_INVALID_RELATIONS", c.Worker.QualityPolicy.MaxInvalidRelations, true); err != nil {
		return err
	}
	if c.Worker.QualityPolicy.MaxInvalidArtifactRatio, err = envGraphFloat("REPOSENSE_GRAPH_MAX_INVALID_ARTIFACT_RATIO", c.Worker.QualityPolicy.MaxInvalidArtifactRatio); err != nil {
		return err
	}
	if c.Worker.QualityPolicy.MaxInvalidRelationRatio, err = envGraphFloat("REPOSENSE_GRAPH_MAX_INVALID_RELATION_RATIO", c.Worker.QualityPolicy.MaxInvalidRelationRatio); err != nil {
		return err
	}
	c.Consumer.MaxPendingGlobal = c.Admission.MaxPendingGlobal
	c.Consumer.MaxPendingPerTenant = c.Admission.MaxPendingPerTenant
	return nil
}

func loadGraphDurations(c *GraphRuntime) (err error) {
	bindings := []struct {
		name   string
		target *time.Duration
	}{
		{"REPOSENSE_GRAPH_WORKER_LEASE", &c.Worker.LeaseDuration},
		{"REPOSENSE_GRAPH_WORKER_HEARTBEAT", &c.Worker.HeartbeatInterval},
		{"REPOSENSE_GRAPH_WORKER_BUILD_TIMEOUT", &c.Worker.BuildTimeout},
		{"REPOSENSE_GRAPH_CONTROL_TIMEOUT", &c.Worker.ControlTimeout},
		{"REPOSENSE_GRAPH_WORKER_RETRY_INITIAL_BACKOFF", &c.Worker.RetryInitialBackoff},
		{"REPOSENSE_GRAPH_WORKER_RETRY_MAX_BACKOFF", &c.Worker.RetryMaxBackoff},
		{"REPOSENSE_GRAPH_ADMISSION_METADATA_TIMEOUT", &c.Admission.MetadataTimeout},
		{"REPOSENSE_GRAPH_ADMISSION_STORE_TIMEOUT", &c.Admission.StoreTimeout},
		{"REPOSENSE_GRAPH_CONSUMER_STORE_TIMEOUT", &c.Consumer.StoreTimeout},
		{"REPOSENSE_GRAPH_QUERY_TIMEOUT", &c.Query.Timeout},
		{"REPOSENSE_GRAPH_OUTBOX_CLAIM", &c.Outbox.ClaimDuration},
		{"REPOSENSE_GRAPH_OUTBOX_PUBLISH_TIMEOUT", &c.Outbox.PublishTimeout},
		{"REPOSENSE_GRAPH_OUTBOX_STORE_TIMEOUT", &c.Outbox.StoreTimeout},
		{"REPOSENSE_GRAPH_OUTBOX_BASE_BACKOFF", &c.Outbox.BaseBackoff},
		{"REPOSENSE_GRAPH_OUTBOX_MAX_BACKOFF", &c.Outbox.MaxBackoff},
		{"REPOSENSE_GRAPH_RECONCILER_ORPHAN_RETENTION", &c.Reconciler.OrphanRetention},
		{"REPOSENSE_GRAPH_RECONCILER_OPERATION_TIMEOUT", &c.Reconciler.OperationTimeout},
		{"REPOSENSE_GRAPH_WORKER_IDLE_DELAY", &c.WorkerIdleDelay},
		{"REPOSENSE_GRAPH_WORKER_FAILURE_DELAY", &c.WorkerFailureDelay},
		{"REPOSENSE_GRAPH_OUTBOX_INTERVAL", &c.OutboxInterval},
		{"REPOSENSE_GRAPH_RECONCILER_INTERVAL", &c.ReconcilerInterval},
		{"REPOSENSE_GRAPH_CONSUMER_FETCH_WAIT", &c.ConsumerFetchWait},
		{"REPOSENSE_GRAPH_CONSUMER_HANDLE_TIMEOUT", &c.ConsumerHandleTimeout},
		{"REPOSENSE_GRAPH_CONSUMER_RETRY_DELAY", &c.ConsumerRetryDelay},
		{"REPOSENSE_GRAPH_READINESS_TIMEOUT", &c.ReadinessTimeout},
		{"REPOSENSE_GRAPH_STARTUP_TIMEOUT", &c.StartupTimeout},
		{"REPOSENSE_GRAPH_SHUTDOWN_TIMEOUT", &c.ShutdownTimeout},
		{"REPOSENSE_GRAPH_METRICS_INTERVAL", &c.MetricsInterval},
	}
	for _, binding := range bindings {
		if *binding.target, err = envDuration(binding.name, *binding.target); err != nil {
			return err
		}
	}
	return nil
}

func envExactList(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, strings.TrimSpace(part))
	}
	return values
}

func envGraphInt(name string, fallback int, allowZero bool) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 || (!allowZero && value == 0) {
		return 0, fmt.Errorf("%s 必须是有效的非负或正整数", name)
	}
	return value, nil
}

func envGraphInt64(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s 必须是非负整数", name)
	}
	return value, nil
}

func envGraphFloat(name string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return 0, fmt.Errorf("%s 必须是 0..1 的有限数", name)
	}
	return value, nil
}

func securePostgresDSN(dsn string) bool {
	lower := strings.ToLower(strings.TrimSpace(dsn))
	if parsed, err := url.Parse(lower); err == nil && (parsed.Scheme == "postgres" || parsed.Scheme == "postgresql") {
		modes := parsed.Query()["sslmode"]
		return len(modes) == 1 && securePostgresMode(modes[0])
	}
	found := ""
	for _, field := range strings.Fields(lower) {
		if key, value, ok := strings.Cut(field, "="); ok && key == "sslmode" {
			if found != "" {
				return false
			}
			found = value
		}
	}
	return securePostgresMode(found)
}

func securePostgresMode(mode string) bool {
	return mode == "require" || mode == "verify-ca" || mode == "verify-full"
}

func secureNeo4jURI(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && parsed.Host != "" && parsed.User == nil && (parsed.Scheme == "neo4j+s" || parsed.Scheme == "bolt+s")
}

func secureNATSURLs(raw string) bool {
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		parsed, err := url.Parse(strings.TrimSpace(part))
		if err != nil || parsed.Scheme != "tls" || parsed.Host == "" || parsed.User != nil {
			return false
		}
	}
	return true
}

func secureHTTPSURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func validListenAddress(address string) bool {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" || address != trimmed {
		return false
	}
	_, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535
}
