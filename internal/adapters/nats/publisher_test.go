package nats

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type recordingJetStream struct{ message *gonats.Msg }

func (r *recordingJetStream) PublishMsg(_ context.Context, message *gonats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	r.message = message
	return &jetstream.PubAck{}, nil
}

func TestPublisherPreservesSchemaBodyAndAddsTrustedScopeHeaders(t *testing.T) {
	js := &recordingJetStream{}
	publisher := NewPublisher(js)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	event := repository.NewParseCompletedEvent("evt", scope, fixedTime(), repository.ParseCompletedPayload{SnapshotID: "snap", CommitSHA: "abc", DeletedPaths: []string{}})
	if err := publisher.Publish(repository.WithEventScope(context.Background(), scope), event); err != nil {
		t.Fatal(err)
	}
	if js.message == nil || js.message.Subject != "parse.completed.v1" {
		t.Fatalf("未发布正确 subject：%#v", js.message)
	}
	if got := js.message.Header.Get("RepoSense-Tenant-ID"); got != "tenant" {
		t.Fatalf("tenant header=%q", got)
	}
	if got := js.message.Header.Get("RepoSense-Repository-ID"); got != "repo" {
		t.Fatalf("repository header=%q", got)
	}
	if got := js.message.Header.Get("RepoSense-Snapshot-ID"); got != "snap" {
		t.Fatalf("snapshot header=%q", got)
	}
	if string(js.message.Data) == "" {
		t.Fatal("事件 JSON 不能为空")
	}
}

func fixedTime() time.Time { return time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC) }

func TestPublisherJetStreamIntegration(t *testing.T) {
	url := os.Getenv("REPOSENSE_TEST_NATS_URL")
	if url == "" {
		t.Skip("REPOSENSE_TEST_NATS_URL 未配置")
	}
	connection, err := gonats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := fmt.Sprintf("REPOSITORY_PARSER_TEST_%d", time.Now().UnixNano())
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{"parse.completed.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer js.DeleteStream(context.Background(), name)
	publisher := NewPublisher(js)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	event := repository.NewParseCompletedEvent("evt-integration-"+name, scope, time.Now(), repository.ParseCompletedPayload{SnapshotID: "snap", CommitSHA: "abc", DeletedPaths: []string{}})
	if err := publisher.Publish(repository.WithEventScope(ctx, scope), event); err != nil {
		t.Fatal(err)
	}
	message, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if message.Header.Get("RepoSense-Tenant-ID") != "tenant" || message.Header.Get("Nats-Msg-Id") != event.EventID {
		t.Fatalf("JetStream 头错误：%v", message.Header)
	}
}
