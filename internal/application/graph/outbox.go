package graphapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type GraphOutboxConfig struct {
	BatchSize      int
	MaxAttempts    int
	ClaimDuration  time.Duration
	PublishTimeout time.Duration
	StoreTimeout   time.Duration
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
}

func (c GraphOutboxConfig) Validate() error {
	if c.BatchSize <= 0 || c.BatchSize > 1000 || c.MaxAttempts <= 0 || c.MaxAttempts > 1000 || c.ClaimDuration <= 0 || c.PublishTimeout <= 0 || c.StoreTimeout <= 0 || c.BaseBackoff <= 0 || c.MaxBackoff < c.BaseBackoff {
		return fmt.Errorf("graph outbox batch, attempts, timeouts and backoff are invalid")
	}
	maximum := time.Duration(1<<63 - 1)
	if c.PublishTimeout > maximum-c.StoreTimeout {
		return fmt.Errorf("graph outbox operation timeout is too large")
	}
	perRecord := c.PublishTimeout + c.StoreTimeout
	if perRecord > maximum/time.Duration(c.BatchSize) || c.ClaimDuration <= perRecord*time.Duration(c.BatchSize) {
		return fmt.Errorf("graph outbox claim duration must cover the configured batch")
	}
	return nil
}

type GraphOutboxDispatcher struct {
	store     ports.GraphOutboxStore
	publisher ports.EventPublisher
	observer  ports.Observer
	clock     ports.Clock
	config    GraphOutboxConfig
}

func NewGraphOutboxDispatcher(store ports.GraphOutboxStore, publisher ports.EventPublisher, observer ports.Observer, clock ports.Clock, config GraphOutboxConfig) (*GraphOutboxDispatcher, error) {
	if store == nil || publisher == nil {
		return nil, fmt.Errorf("graph outbox store and publisher are required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &GraphOutboxDispatcher{store: store, publisher: publisher, observer: observer, clock: clock, config: config}, nil
}

func (d *GraphOutboxDispatcher) DispatchOnce(ctx context.Context) (published int, err error) {
	now := d.clock.Now().UTC()
	ctx, finish := startGraphStage(d.observer, ctx, "graph_outbox", map[string]string{"operation": "dispatch"})
	defer func() { finish(err) }()
	claimCtx, cancelClaim := context.WithTimeout(ctx, d.config.StoreTimeout)
	records, err := d.store.ClaimGraphEvents(claimCtx, d.config.BatchSize, now, now.Add(d.config.ClaimDuration))
	cancelClaim()
	if err != nil {
		d.observer.Count("graph_outbox_operations_total", 1, map[string]string{"status": "claim_failed"})
		return 0, err
	}
	d.observer.Count("graph_outbox_claimed_total", int64(len(records)), nil)
	var failures []error
	for _, record := range records {
		if contextErr := ctx.Err(); contextErr != nil {
			failures = append(failures, contextErr)
			break
		}
		messageCtx, finishPublish := startGraphStage(d.observer, repository.WithEventScope(ctx, record.Scope), "graph_outbox_publish", map[string]string{
			"operation": "publish", "dependency": "nats", "tenant_id": record.Scope.TenantID,
			"repository_id": record.Scope.RepositoryID, "snapshot_id": record.Scope.SnapshotID,
			"revision_id": record.RevisionID, "trace_id": record.Scope.TraceID, "trace_root": "true",
		})
		publishCtx, cancelPublish := context.WithTimeout(messageCtx, d.config.PublishTimeout)
		publishErr := d.publisher.Publish(publishCtx, record.Event)
		cancelPublish()
		finishPublish(publishErr)
		if publishErr == nil {
			storeCtx, cancelStore := context.WithTimeout(context.WithoutCancel(ctx), d.config.StoreTimeout)
			storeCtx, finishStore := startGraphStage(d.observer, storeCtx, "graph_outbox_mark", map[string]string{"operation": "published", "dependency": "postgresql"})
			markErr := d.store.MarkGraphEventPublished(storeCtx, record.Event.EventID, d.clock.Now().UTC())
			cancelStore()
			finishStore(markErr)
			if markErr != nil {
				failures = append(failures, markErr)
				d.observer.Count("graph_outbox_operations_total", 1, map[string]string{"status": "mark_unknown"})
				continue
			}
			published++
			d.observer.Count("graph_outbox_operations_total", 1, map[string]string{"status": "published"})
			continue
		}
		if ctx.Err() != nil {
			failures = append(failures, ctx.Err())
			break
		}

		publishFailure := graphOutboxError(graph.ErrEventPublishFailed, "publish", true, "graph event publication failed", publishErr)
		attempt := record.DeliveryCount + 1
		deadLetter := attempt >= d.config.MaxAttempts
		failedAt := d.clock.Now().UTC()
		nextAttempt := failedAt.Add(graphOutboxBackoff(d.config, attempt))
		if deadLetter {
			nextAttempt = failedAt
		}
		storeCtx, cancelStore := context.WithTimeout(context.WithoutCancel(ctx), d.config.StoreTimeout)
		storeCtx, finishStore := startGraphStage(d.observer, storeCtx, "graph_outbox_mark", map[string]string{"operation": "failed", "dependency": "postgresql"})
		markErr := d.store.MarkGraphEventFailed(storeCtx, record.Event.EventID, "graph event publication failed", nextAttempt, deadLetter)
		cancelStore()
		finishStore(markErr)
		status := "retry_scheduled"
		if deadLetter {
			status = "dead_lettered"
		}
		d.observer.Count("graph_outbox_operations_total", 1, map[string]string{"status": status})
		if markErr != nil {
			failures = append(failures, errors.Join(publishFailure, markErr))
		} else {
			failures = append(failures, publishFailure)
		}
	}
	return published, errors.Join(failures...)
}

func graphOutboxBackoff(config GraphOutboxConfig, attempt int) time.Duration {
	delay := config.BaseBackoff
	for number := 1; number < attempt && delay < config.MaxBackoff; number++ {
		if delay > config.MaxBackoff/2 {
			return config.MaxBackoff
		}
		delay *= 2
	}
	if delay > config.MaxBackoff {
		return config.MaxBackoff
	}
	return delay
}

func graphOutboxError(code graph.ErrorCode, stage string, retryable bool, message string, cause error) error {
	return &graph.DomainError{Code: code, Operation: "graph_outbox", Stage: stage, Dependency: "nats", Retryable: retryable, Message: message, Cause: cause}
}
