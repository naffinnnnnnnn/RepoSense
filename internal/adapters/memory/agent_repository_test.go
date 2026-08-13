package memory

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
)

func TestAgentRepositoryPinsConversationAndCopiesRuns(t *testing.T) {
	repo := NewAgentRepository()
	scope := common.Scope{TenantID: "t", RepositoryID: "r", SnapshotID: "s"}
	now := time.Unix(1, 0)
	conversation := agent.Conversation{ConversationID: "c", Scope: scope}
	run := agent.Run{EntityMeta: agent.NewMeta("run", scope, string(agent.RunRunning), "u", now), RunID: "run", ConversationID: "c", SnapshotID: "s", Status: agent.RunRunning, ToolCalls: []agent.ToolCall{{Arguments: map[string]string{"limit": "1"}}}}
	if err := repo.CreateRun(context.Background(), conversation, run); err != nil {
		t.Fatal(err)
	}
	loaded, _ := repo.GetRun(context.Background(), "run")
	loaded.ToolCalls[0].Arguments["limit"] = "changed"
	again, _ := repo.GetRun(context.Background(), "run")
	if again.ToolCalls[0].Arguments["limit"] != "1" {
		t.Fatal("repository leaked mutable state")
	}
	other := conversation
	other.Scope.SnapshotID = "other"
	otherRun := run
	otherRun.RunID = "run2"
	otherRun.SnapshotID = "other"
	if err := repo.CreateRun(context.Background(), other, otherRun); err == nil {
		t.Fatal("expected snapshot pinning error")
	}
}
