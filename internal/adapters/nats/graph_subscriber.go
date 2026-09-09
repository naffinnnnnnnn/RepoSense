package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/ports"
)

type GraphMessageHandler func(context.Context, common.Scope, common.EventEnvelope) (bool, error)

// GraphSubscriber owns a durable JetStream pull subscription. It translates
// transport headers into the trusted scope consumed by the Graph application;
// payload tenant fields are never used as an authorization source.
type GraphSubscriber struct {
	connection   *gonats.Conn
	subscription *gonats.Subscription
	observer     ports.Observer
}

func (s *GraphSubscriber) SetObserver(observer ports.Observer) {
	if s != nil {
		s.observer = observer
	}
}

func ConnectGraphSubscriber(url, stream, durable string, options ...gonats.Option) (*GraphSubscriber, error) {
	if strings.TrimSpace(url) == "" || !exactTransportIdentity(stream) || !exactTransportIdentity(durable) {
		return nil, fmt.Errorf("NATS URL、stream 和 durable consumer 必须配置为精确非空值")
	}
	connection, err := gonats.Connect(url, options...)
	if err != nil {
		return nil, err
	}
	js, err := connection.JetStream()
	if err != nil {
		connection.Close()
		return nil, err
	}
	subscription, err := js.PullSubscribe("parse.*.v1", durable, gonats.BindStream(stream), gonats.ManualAck(), gonats.AckExplicit())
	if err != nil {
		connection.Close()
		return nil, err
	}
	return &GraphSubscriber{connection: connection, subscription: subscription}, nil
}

func (s *GraphSubscriber) Close() {
	if s == nil {
		return
	}
	if s.subscription != nil {
		_ = s.subscription.Unsubscribe()
	}
	if s.connection != nil {
		_ = s.connection.Drain()
		s.connection.Close()
	}
}

func (s *GraphSubscriber) Health(ctx context.Context) error {
	if s == nil || s.connection == nil || s.subscription == nil {
		return fmt.Errorf("Graph NATS subscription is not configured")
	}
	return s.connection.FlushWithContext(ctx)
}

func (s *GraphSubscriber) Consume(ctx context.Context, batch int, fetchWait, handleTimeout, retryDelay time.Duration, handler GraphMessageHandler) error {
	if s == nil || s.subscription == nil || handler == nil {
		return fmt.Errorf("Graph NATS subscription 和 handler 必须配置")
	}
	if batch <= 0 || batch > 1000 || fetchWait <= 0 || handleTimeout <= 0 || retryDelay <= 0 {
		return fmt.Errorf("Graph NATS 消费批次和超时必须为正数")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		messages, err := s.subscription.Fetch(batch, gonats.MaxWait(fetchWait))
		if errors.Is(err, gonats.ErrTimeout) {
			continue
		}
		if err != nil {
			return err
		}
		for _, message := range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			traceCtx := extractTraceContext(ctx, message.Header)
			scope := common.Scope{
				TenantID:     message.Header.Get("RepoSense-Tenant-ID"),
				RepositoryID: message.Header.Get("RepoSense-Repository-ID"),
				SnapshotID:   message.Header.Get("RepoSense-Snapshot-ID"),
			}
			s.count("graph_consumer_transport_total", "received")
			if metadata, metadataErr := message.Metadata(); metadataErr == nil {
				if metadata.NumDelivered > 1 {
					s.count("graph_consumer_transport_total", "redelivered")
				}
				if gauge, ok := s.observer.(interface {
					Gauge(string, float64, map[string]string)
				}); ok {
					gauge.Gauge("graph_consumer_pending_messages", float64(metadata.NumPending), nil)
				}
			}
			event, valid := decodeGraphEvent(message.Data)
			if !valid {
				s.count("graph_consumer_transport_total", "poison")
			}
			handleCtx, cancel := context.WithTimeout(traceCtx, handleTimeout)
			ack, handleErr := handler(handleCtx, scope, event)
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if handleErr != nil || !ack {
				s.count("graph_consumer_transport_total", "nak")
				if err := message.NakWithDelay(retryDelay); err != nil {
					return err
				}
				continue
			}
			if err := message.Ack(); err != nil {
				s.count("graph_consumer_transport_total", "ack_failed")
				return err
			}
			s.count("graph_consumer_transport_total", "acked")
		}
	}
}

func decodeGraphEvent(payload []byte) (common.EventEnvelope, bool) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var event common.EventEnvelope
	if err := decoder.Decode(&event); err == nil {
		var trailing any
		if trailingErr := decoder.Decode(&trailing); errors.Is(trailingErr, io.EOF) {
			return event, true
		}
	}
	digest := sha256.Sum256(payload)
	return common.EventEnvelope{
		EventID: "invalid_" + hex.EncodeToString(digest[:]), EventType: "invalid",
		AggregateID: "invalid", OccurredAt: time.Unix(1, 0).UTC(), Producer: "invalid",
		PayloadVersion: 1, TraceID: "invalid", Payload: map[string]any{"payload_digest": hex.EncodeToString(digest[:])},
	}, false
}

func (s *GraphSubscriber) count(name, status string) {
	if s != nil && s.observer != nil {
		s.observer.Count(name, 1, map[string]string{"status": status})
	}
}

func exactTransportIdentity(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}
