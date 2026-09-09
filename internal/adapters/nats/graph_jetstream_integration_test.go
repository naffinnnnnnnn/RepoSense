//go:build integration

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

func TestGraphJetStreamPublishConsumeAckAndRedelivery(t *testing.T) {
	url := os.Getenv("REPOSENSE_TEST_NATS_URL")
	if url == "" {
		t.Fatal("REPOSENSE_TEST_NATS_URL is required for the integration gate")
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := fmt.Sprintf("GRAPH_VERIFY_%d", time.Now().UnixNano())
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{"parse.*.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })

	publisher := NewPublisher(js)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	event := repository.NewParseCompletedEvent("evt-"+name, scope, time.Now().UTC(), repository.ParseCompletedPayload{SnapshotID: "snap", CommitSHA: "abc", DeletedPaths: []string{}})
	if err := publisher.Publish(repository.WithEventScope(ctx, scope), event); err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Header.Get("RepoSense-Tenant-ID") != scope.TenantID || stored.Header.Get("Nats-Msg-Id") != event.EventID {
		t.Fatalf("trusted headers were not preserved: %v", stored.Header)
	}

	durable := "graph-verifier"
	subscriber, err := ConnectGraphSubscriber(url, name, durable)
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Close()
	consumeCtx, stop := context.WithCancel(ctx)
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Consume(consumeCtx, 1, 250*time.Millisecond, 2*time.Second, 10*time.Millisecond,
			func(_ context.Context, trusted common.Scope, received common.EventEnvelope) (bool, error) {
				calls++
				if trusted.TenantID != scope.TenantID || trusted.RepositoryID != scope.RepositoryID || received.EventID != event.EventID {
					return false, fmt.Errorf("transport identity mismatch")
				}
				if calls == 1 {
					return false, nil
				}
				stop()
				return true, nil
			})
	}()
	select {
	case <-ctx.Done():
		t.Fatal("message was not redelivered before the integration deadline")
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("expected one NAK redelivery followed by ACK, calls=%d", calls)
	}
}
