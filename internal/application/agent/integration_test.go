package agentapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

// This contract-level integration test uses the real Graph application facade
// and memory graph adapter, proving that QA consumes the already-developed
// module only through its stable GraphStore port.
func TestAgentIntegratesWithPublishedGraphRevision(t *testing.T) {
	ctx := context.Background()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	refA := source("sha", "internal/api.go")
	refA.SymbolID = "api.Handle"
	refB := source("sha", "internal/service.go")
	refB.SymbolID = "service.Execute"
	graphs := memory.NewGraphRepository()
	revision := graph.Revision{EntityMeta: graph.NewMeta("gr_1", scope, graph.RevisionActive, time.Unix(10, 0)), RevisionID: "gr_1", SnapshotID: scope.SnapshotID,
		CommitSHA: "sha", BuildStatus: graph.RevisionActive, AlgorithmVersion: "test", Nodes: []graph.Entity{
			{NodeID: "n_api", EntityType: graph.EntityFunction, Name: "Handle", SourceRef: &refA},
			{NodeID: "n_service", EntityType: graph.EntityFunction, Name: "Execute", SourceRef: &refB},
		}, Edges: []graph.Relation{{EdgeID: "e_calls", RelationType: repository.RelationCalls, FromNodeID: "n_api", ToNodeID: "n_service", Evidence: refA, Confidence: 1}}}
	revision.Normalize()
	if err := graphs.Save(ctx, "graph-key", revision); err != nil {
		t.Fatal(err)
	}
	graphService, err := graphapp.New(memory.NewRepositoryStore(), graphs, nil, nil, nil, nil, nil, graphapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	agentRepo := memory.NewAgentRepository()
	service, err := New(graphService, nil, agentRepo, nil, nil, nil, nil, &sequenceIDs{}, fixedClock{time.Unix(20, 0)}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	cmd := command()
	cmd.Scope = scope
	cmd.Question = "What is the call chain?"
	stream, err := service.Ask(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	events := collect(stream)
	run, err := agentRepo.GetRun(ctx, events[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != agent.RunCompleted || run.Answer == nil || len(run.Answer.Citations) != 2 {
		t.Fatalf("unexpected integrated run: %#v", run)
	}
	if !strings.Contains(run.Answer.AnswerMarkdown, "Handle") || !strings.Contains(run.Answer.AnswerMarkdown, "CALLS") || !strings.Contains(run.Answer.AnswerMarkdown, "Execute") {
		t.Fatalf("answer did not use graph relationship: %s", run.Answer.AnswerMarkdown)
	}
}
