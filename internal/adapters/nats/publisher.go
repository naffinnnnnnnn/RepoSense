package nats

import (
	"context"
	"encoding/json"
	"errors"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type jetStreamPublisher interface {
	PublishMsg(context.Context, *gonats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}
type Publisher struct {
	connection *gonats.Conn
	jetstream  jetStreamPublisher
}

func Connect(url string, options ...gonats.Option) (*Publisher, error) {
	connection, err := gonats.Connect(url, options...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, err
	}
	return &Publisher{connection: connection, jetstream: js}, nil
}
func NewPublisher(js jetStreamPublisher) *Publisher { return &Publisher{jetstream: js} }
func (p *Publisher) Close() {
	if p.connection != nil {
		_ = p.connection.Drain()
		p.connection.Close()
	}
}
func (p *Publisher) Publish(ctx context.Context, event common.EventEnvelope) error {
	if p == nil || p.jetstream == nil {
		return errors.New("NATS JetStream 未配置")
	}
	if event.EventID == "" || event.EventType == "" {
		return errors.New("事件标识或类型为空")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	message := &gonats.Msg{Subject: event.EventType, Data: payload, Header: gonats.Header{}}
	if scope, ok := repository.EventScopeFromContext(ctx); ok {
		message.Header.Set("RepoSense-Tenant-ID", scope.TenantID)
		message.Header.Set("RepoSense-Repository-ID", scope.RepositoryID)
		message.Header.Set("RepoSense-Snapshot-ID", scope.SnapshotID)
	}
	_, err = p.jetstream.PublishMsg(ctx, message, jetstream.WithMsgID(event.EventID))
	return err
}
