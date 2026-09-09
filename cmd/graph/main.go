package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gonats "github.com/nats-io/nats.go"
	natsadapter "github.com/reposense/reposense/internal/adapters/nats"
	neo4jadapter "github.com/reposense/reposense/internal/adapters/neo4j"
	"github.com/reposense/reposense/internal/adapters/observability"
	postgresadapter "github.com/reposense/reposense/internal/adapters/postgres"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	"github.com/reposense/reposense/internal/config"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/ports"
	httptransport "github.com/reposense/reposense/internal/transport/hertz"
)

type parserSourceFactory func(context.Context, config.GraphRuntime) (ports.PagedGraphSource, error)

var openParserSource parserSourceFactory = func(context.Context, config.GraphRuntime) (ports.PagedGraphSource, error) {
	return nil, errors.New("Repository Parser 尚未提供最终 PagedGraphSource 生产适配器；拒绝使用 Graph 内符号猜测降级")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) != 2 {
		writeProcessFailure("Graph role argument is required")
		os.Exit(2)
	}
	role, err := config.ParseGraphRole(os.Args[1])
	if err == nil {
		err = run(ctx, role, openParserSource)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		writeProcessFailure(sanitizeStartupError(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, role config.GraphRole, sourceFactory parserSourceFactory) error {
	cfg, err := config.LoadGraphRuntime(role)
	if err != nil {
		return graphProcessFailure("Graph configuration or secret validation failed", err)
	}
	startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	telemetry, err := observability.NewGraphTelemetry(startupCtx, "reposense-"+string(role), cfg.OTLPEndpoint, log.New(os.Stderr, "", 0))
	cancelStartup()
	if err != nil {
		return graphProcessFailure("Graph telemetry initialization failed", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = telemetry.Shutdown(shutdownCtx)
	}()
	switch role {
	case config.GraphRoleAPI:
		return runAPI(ctx, cfg, sourceFactory, telemetry)
	case config.GraphRoleConsumer:
		return runConsumer(ctx, cfg, telemetry)
	case config.GraphRoleWorker:
		return runWorker(ctx, cfg, sourceFactory, telemetry)
	case config.GraphRoleOutbox:
		return runOutbox(ctx, cfg, telemetry)
	case config.GraphRoleReconciler:
		return runReconciler(ctx, cfg, telemetry)
	default:
		return fmt.Errorf("不支持的 Graph 运行角色 %q", role)
	}
}

func runAPI(ctx context.Context, cfg config.GraphRuntime, sourceFactory parserSourceFactory, telemetry *observability.GraphTelemetry) error {
	source, sourceHealth, err := requireParserSource(ctx, cfg, sourceFactory)
	if err != nil {
		return graphProcessFailure("Graph Parser source initialization failed", err)
	}
	reader, err := graphapp.NewSourceReader(source, cfg.ParserPageSize, cfg.SupportedParserSchemas)
	if err != nil {
		return err
	}
	control, err := openControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer control.Close()
	data, err := openDataStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeDataStore(data, cfg.ShutdownTimeout)
	if err := verifyStartup(ctx, cfg.StartupTimeout, control.VerifyGraphSchema, graphPrivilegeCheck(control, cfg.Role), data.VerifyGraphSchema, sourceHealth.Health); err != nil {
		return graphProcessFailure("Graph API startup dependency, privilege, or schema validation failed", err)
	}
	build, err := graphapp.NewBuildAdmissionService(reader, control, telemetry.Observer, graphapp.RandomIDs{}, graphapp.SystemClock{}, cfg.Admission)
	if err != nil {
		return err
	}
	query, err := graphapp.NewQueryService(control, data, telemetry.Observer, cfg.Query)
	if err != nil {
		return err
	}
	handler, err := httptransport.NewGraphHandler(build, query, httptransport.HeaderAuthenticator{})
	if err != nil {
		return err
	}
	health := httptransport.NewGraphHealthHandler(telemetry.Metrics, cfg.ReadinessTimeout,
		healthCheck("postgresql", control), healthCheck("neo4j", data), healthCheck("parser", sourceHealth))
	public := graphHTTPServer(cfg.HTTPAddress, telemetry.Observer.HTTPMiddleware(handler))
	admin := graphHTTPServer(cfg.AdminAddress, health)
	return serveGraphRole(ctx, cfg, health, []*http.Server{public, admin}, nil, control, telemetry.Observer)
}

func runConsumer(ctx context.Context, cfg config.GraphRuntime, telemetry *observability.GraphTelemetry) error {
	control, err := openControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer control.Close()
	subscriber, err := natsadapter.ConnectGraphSubscriber(cfg.NATSURL, cfg.ConsumerStream, cfg.ConsumerDurable,
		gonats.UserCredentials(cfg.NATSCredentialsFile), gonats.Timeout(cfg.StartupTimeout), gonats.DrainTimeout(cfg.ShutdownTimeout))
	if err != nil {
		return graphProcessFailure("Graph NATS durable consumer connection failed", err)
	}
	defer subscriber.Close()
	subscriber.SetObserver(telemetry.Observer)
	if err := verifyStartup(ctx, cfg.StartupTimeout, control.VerifyGraphSchema, graphPrivilegeCheck(control, cfg.Role), subscriber.Health); err != nil {
		return graphProcessFailure("Graph Consumer startup dependency, privilege, or schema validation failed", err)
	}
	consumer, err := graphapp.NewConsumer(control, control, telemetry.Observer, graphapp.RandomIDs{}, graphapp.SystemClock{}, cfg.Consumer)
	if err != nil {
		return err
	}
	health := httptransport.NewGraphHealthHandler(telemetry.Metrics, cfg.ReadinessTimeout,
		healthCheck("postgresql", control), healthCheck("nats", subscriber))
	admin := graphHTTPServer(cfg.AdminAddress, health)
	workload := func(roleCtx context.Context) error {
		return subscriber.Consume(roleCtx, cfg.ConsumerFetchBatch, cfg.ConsumerFetchWait, cfg.ConsumerHandleTimeout, cfg.ConsumerRetryDelay,
			func(handleCtx context.Context, scope common.Scope, event common.EventEnvelope) (bool, error) {
				result, handleErr := consumer.Handle(handleCtx, scope, event)
				return result.Disposition == graphapp.ConsumerAck, handleErr
			})
	}
	return serveGraphRole(ctx, cfg, health, []*http.Server{admin}, workload, control, telemetry.Observer)
}

func runWorker(ctx context.Context, cfg config.GraphRuntime, sourceFactory parserSourceFactory, telemetry *observability.GraphTelemetry) error {
	source, sourceHealth, err := requireParserSource(ctx, cfg, sourceFactory)
	if err != nil {
		return graphProcessFailure("Graph Parser source initialization failed", err)
	}
	reader, err := graphapp.NewSourceReader(source, cfg.ParserPageSize, cfg.SupportedParserSchemas)
	if err != nil {
		return err
	}
	control, err := openControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer control.Close()
	data, err := openDataStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeDataStore(data, cfg.ShutdownTimeout)
	if err := verifyStartup(ctx, cfg.StartupTimeout, control.VerifyGraphSchema, graphPrivilegeCheck(control, cfg.Role), data.VerifyGraphSchema, sourceHealth.Health); err != nil {
		return graphProcessFailure("Graph Worker startup dependency, privilege, or schema validation failed", err)
	}
	worker, err := graphapp.NewWorker(reader, control, data, telemetry.Observer, graphapp.RandomIDs{}, graphapp.SystemClock{}, cfg.Worker)
	if err != nil {
		return err
	}
	health := httptransport.NewGraphHealthHandler(telemetry.Metrics, cfg.ReadinessTimeout,
		healthCheck("postgresql", control), healthCheck("neo4j", data), healthCheck("parser", sourceHealth))
	admin := graphHTTPServer(cfg.AdminAddress, health)
	return serveGraphRole(ctx, cfg, health, []*http.Server{admin}, func(roleCtx context.Context) error {
		return worker.Run(roleCtx, cfg.WorkerOwner, cfg.WorkerIdleDelay, cfg.WorkerFailureDelay)
	}, control, telemetry.Observer)
}

func runOutbox(ctx context.Context, cfg config.GraphRuntime, telemetry *observability.GraphTelemetry) error {
	control, err := openControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer control.Close()
	publisher, err := natsadapter.Connect(cfg.NATSURL, gonats.UserCredentials(cfg.NATSCredentialsFile),
		gonats.Timeout(cfg.StartupTimeout), gonats.DrainTimeout(cfg.ShutdownTimeout))
	if err != nil {
		return graphProcessFailure("Graph NATS publisher connection failed", err)
	}
	defer publisher.Close()
	if err := verifyStartup(ctx, cfg.StartupTimeout, control.VerifyGraphSchema, graphPrivilegeCheck(control, cfg.Role), publisher.Health); err != nil {
		return graphProcessFailure("Graph Outbox startup dependency, privilege, or schema validation failed", err)
	}
	dispatcher, err := graphapp.NewGraphOutboxDispatcher(control, publisher, telemetry.Observer, graphapp.SystemClock{}, cfg.Outbox)
	if err != nil {
		return err
	}
	health := httptransport.NewGraphHealthHandler(telemetry.Metrics, cfg.ReadinessTimeout,
		healthCheck("postgresql", control), healthCheck("nats", publisher))
	admin := graphHTTPServer(cfg.AdminAddress, health)
	return serveGraphRole(ctx, cfg, health, []*http.Server{admin}, func(roleCtx context.Context) error {
		return dispatcher.Run(roleCtx, cfg.OutboxInterval)
	}, control, telemetry.Observer)
}

func runReconciler(ctx context.Context, cfg config.GraphRuntime, telemetry *observability.GraphTelemetry) error {
	control, err := openControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer control.Close()
	data, err := openDataStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeDataStore(data, cfg.ShutdownTimeout)
	if err := verifyStartup(ctx, cfg.StartupTimeout, control.VerifyGraphSchema, graphPrivilegeCheck(control, cfg.Role), data.VerifyGraphSchema); err != nil {
		return graphProcessFailure("Graph Reconciler startup dependency, privilege, or schema validation failed", err)
	}
	reconciler, err := graphapp.NewReconciler(control, control, data, telemetry.Observer, graphapp.RandomIDs{}, graphapp.SystemClock{}, cfg.Reconciler)
	if err != nil {
		return err
	}
	health := httptransport.NewGraphHealthHandler(telemetry.Metrics, cfg.ReadinessTimeout,
		healthCheck("postgresql", control), healthCheck("neo4j", data))
	admin := graphHTTPServer(cfg.AdminAddress, health)
	return serveGraphRole(ctx, cfg, health, []*http.Server{admin}, func(roleCtx context.Context) error {
		return reconciler.Run(roleCtx, cfg.ReconcilerInterval)
	}, control, telemetry.Observer, graphMetricSource{dependency: "neo4j", collect: data.GraphOperationalMetrics})
}

func requireParserSource(ctx context.Context, cfg config.GraphRuntime, factory parserSourceFactory) (ports.PagedGraphSource, ports.HealthChecker, error) {
	if factory == nil {
		return nil, nil, errors.New("Graph Parser source factory 未配置")
	}
	source, err := factory(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	if source == nil {
		return nil, nil, errors.New("Graph Parser source factory 返回空依赖")
	}
	health, ok := source.(ports.HealthChecker)
	if !ok {
		return nil, nil, errors.New("Graph Parser source 未实现 readiness 契约")
	}
	return source, health, nil
}

func openControlStore(ctx context.Context, cfg config.GraphRuntime) (*postgresadapter.GraphControlStore, error) {
	store, err := postgresadapter.NewGraphControlStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, graphProcessFailure("Graph PostgreSQL control-plane connection failed", err)
	}
	return store, nil
}

func openDataStore(ctx context.Context, cfg config.GraphRuntime) (*neo4jadapter.GraphDataStore, error) {
	store, err := neo4jadapter.NewGraphDataStoreConfigured(ctx, cfg.Neo4jURI, cfg.Neo4jUser, cfg.Neo4jPass, cfg.Neo4jDB, cfg.Data)
	if err != nil {
		return nil, graphProcessFailure("Graph Neo4j data-plane connection failed", err)
	}
	return store, nil
}

func verifyStartup(parent context.Context, timeout time.Duration, checks ...func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	for _, check := range checks {
		if check == nil {
			return errors.New("startup check is not configured")
		}
		if err := check(ctx); err != nil {
			return errors.New("startup check failed")
		}
	}
	return nil
}

func healthCheck(name string, checker ports.HealthChecker) httptransport.GraphHealthCheck {
	return httptransport.GraphHealthCheck{Name: name, Check: checker.Health}
}

func graphPrivilegeCheck(store *postgresadapter.GraphControlStore, role config.GraphRole) func(context.Context) error {
	return func(ctx context.Context) error { return store.VerifyGraphPrivileges(ctx, string(role)) }
}

func graphHTTPServer(address string, handler http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 1 << 20, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}
}

type graphMetricSource struct {
	dependency string
	collect    func(context.Context) (map[string]float64, error)
}

func serveGraphRole(parent context.Context, cfg config.GraphRuntime, health *httptransport.GraphHealthHandler, servers []*http.Server, workload func(context.Context) error, control *postgresadapter.GraphControlStore, observer *observability.GraphObserver, additionalMetrics ...graphMetricSource) error {
	roleCtx, cancelRole := context.WithCancel(parent)
	defer cancelRole()
	serverErrors := make(chan error, len(servers))
	for _, server := range servers {
		server := server
		go func() {
			err := server.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			serverErrors <- err
		}()
	}
	var workloadDone chan error
	if workload != nil {
		workloadDone = make(chan error, 1)
		go func() { workloadDone <- workload(roleCtx) }()
	}
	sources := append([]graphMetricSource{{dependency: "postgresql", collect: func(ctx context.Context) (map[string]float64, error) {
		return control.GraphOperationalMetrics(ctx, string(cfg.Role))
	}}}, additionalMetrics...)
	go collectOperationalMetrics(roleCtx, cfg, sources, observer)
	health.MarkStarted()
	var result error
	workloadFinished := false
	select {
	case <-parent.Done():
		result = parent.Err()
	case result = <-serverErrors:
	case result = <-workloadDone:
		workloadFinished = true
	}
	health.BeginShutdown()
	cancelRole()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			shutdownErr := errors.New("Graph HTTP shutdown failed")
			if result == nil || errors.Is(result, context.Canceled) {
				result = shutdownErr
			} else {
				result = errors.Join(result, shutdownErr)
			}
		}
	}
	if workloadDone != nil && !workloadFinished {
		select {
		case workloadErr := <-workloadDone:
			if result == nil && workloadErr != nil && !errors.Is(workloadErr, context.Canceled) {
				result = workloadErr
			}
		case <-shutdownCtx.Done():
			shutdownErr := errors.New("Graph workload shutdown timed out")
			if result == nil || errors.Is(result, context.Canceled) {
				result = shutdownErr
			} else {
				result = errors.Join(result, shutdownErr)
			}
		}
	}
	return result
}

func collectOperationalMetrics(ctx context.Context, cfg config.GraphRuntime, sources []graphMetricSource, observer *observability.GraphObserver) {
	collect := func() {
		for _, source := range sources {
			if source.collect == nil {
				observer.Count("graph_metrics_collection_total", 1, map[string]string{"status": "failed", "dependency": source.dependency})
				continue
			}
			checkCtx, cancel := context.WithTimeout(ctx, cfg.ReadinessTimeout)
			metrics, err := source.collect(checkCtx)
			cancel()
			if err != nil {
				observer.Count("graph_metrics_collection_total", 1, map[string]string{"status": "failed", "dependency": source.dependency})
				continue
			}
			for name, value := range metrics {
				observer.Gauge(name, value, nil)
			}
			observer.Count("graph_metrics_collection_total", 1, map[string]string{"status": "succeeded", "dependency": source.dependency})
		}
	}
	collect()
	ticker := time.NewTicker(cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collect()
		}
	}
}

func closeDataStore(store *neo4jadapter.GraphDataStore, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = store.Close(ctx)
}

func sanitizeStartupError(err error) string {
	if err == nil {
		return ""
	}
	var processErr *graphProcessError
	if errors.As(err, &processErr) {
		return processErr.reason
	}
	return "Graph role startup or runtime failed; inspect structured telemetry for the stable stage and error code"
}

type graphProcessError struct {
	reason string
	cause  error
}

func (e *graphProcessError) Error() string { return e.reason }
func (e *graphProcessError) Unwrap() error { return e.cause }

func graphProcessFailure(reason string, cause error) error {
	return &graphProcessError{reason: reason, cause: cause}
}

func writeProcessFailure(reason string) {
	payload, _ := json.Marshal(map[string]string{"level": "ERROR", "message": "graph role failed", "reason": reason})
	_, _ = fmt.Fprintln(os.Stderr, string(payload))
}
