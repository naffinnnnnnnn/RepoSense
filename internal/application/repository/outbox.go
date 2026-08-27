package repositoryapp

import (
	"context"
	"errors"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type OutboxConfig struct {
	BatchSize, MaxAttempts  int
	BaseBackoff, MaxBackoff time.Duration
}
type OutboxDispatcher struct {
	store     ports.OutboxStore
	publisher ports.EventPublisher
	clock     ports.Clock
	observer  ports.Observer
	config    OutboxConfig
}

func NewOutboxDispatcher(store ports.OutboxStore, publisher ports.EventPublisher, clock ports.Clock, config OutboxConfig) (*OutboxDispatcher, error) {
	if store == nil || publisher == nil || clock == nil {
		return nil, errors.New("Outbox Store、Publisher 和 Clock 不能为空")
	}
	if config.BatchSize <= 0 || config.MaxAttempts <= 0 || config.BaseBackoff <= 0 || config.MaxBackoff < config.BaseBackoff {
		return nil, errors.New("Outbox 配置无效")
	}
	return &OutboxDispatcher{store: store, publisher: publisher, clock: clock, observer: noopObserver{}, config: config}, nil
}
func (d *OutboxDispatcher) WithObserver(observer ports.Observer) *OutboxDispatcher {
	if observer != nil {
		d.observer = observer
	}
	return d
}
func (d *OutboxDispatcher) DispatchOnce(ctx context.Context) (int, error) {
	now := d.clock.Now().UTC()
	records, err := d.store.PendingOutbox(ctx, d.config.BatchSize, now)
	if err != nil {
		d.observer.Count("repository_outbox_load_failures_total", 1, nil)
		return 0, err
	}
	d.observer.Count("repository_outbox_pending", int64(len(records)), nil)
	if len(records) > 0 {
		d.observer.Count("repository_outbox_oldest_age_seconds", int64(now.Sub(records[0].OccurredAt).Seconds()), nil)
	}
	published := 0
	var failures []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return published, errors.Join(append(failures, err)...)
		}
		publishCtx := repository.WithEventScope(ctx, common.Scope{TenantID: record.TenantID, RepositoryID: record.RepositoryID, SnapshotID: record.AggregateID, TraceID: record.TraceID})
		publishErr := d.publisher.Publish(publishCtx, record.Event)
		if publishErr == nil {
			if markErr := d.store.MarkOutboxPublished(ctx, record.EventID, d.clock.Now().UTC()); markErr != nil {
				failures = append(failures, markErr)
				continue
			}
			published++
			continue
		}
		attempt := record.DeliveryCount + 1
		d.observer.Count("repository_outbox_publish_failures_total", 1, map[string]string{"event_type": record.EventType})
		dead := attempt >= d.config.MaxAttempts
		if dead {
			d.observer.Count("repository_outbox_dead_letter_total", 1, map[string]string{"event_type": record.EventType})
		}
		backoff := d.config.BaseBackoff
		for i := 1; i < attempt && backoff < d.config.MaxBackoff; i++ {
			backoff *= 2
			if backoff > d.config.MaxBackoff {
				backoff = d.config.MaxBackoff
			}
		}
		if markErr := d.store.MarkOutboxFailed(context.WithoutCancel(ctx), record.EventID, "事件发布失败", now.Add(backoff), dead); markErr != nil {
			failures = append(failures, errors.Join(publishErr, markErr))
		} else {
			failures = append(failures, publishErr)
		}
	}
	return published, errors.Join(failures...)
}
