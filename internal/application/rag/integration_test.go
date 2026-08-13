package ragapp_test

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/application/agent"
	ragapp "github.com/reposense/reposense/internal/application/rag"
	agentdomain "github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
)

type ids struct{ n int }

func (i *ids) New(prefix string) string { i.n++; return prefix + string(rune('a'+i.n)) }

type clock struct{}

func (clock) Now() time.Time { return time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC) }

func TestRAGRetrieverIntegratesWithRepositoryAgent(t *testing.T) {
	ctx := context.Background()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"}
	ref := common.SourceRef{CommitSHA: "commit", Path: "internal/auth/handler.go", SymbolID: "handle", StartLine: 12, EndLine: 28, ContentHash: "sha256:handle"}
	artifact := repository.CodeArtifact{ArtifactID: "handle", Kind: repository.ArtifactFunction, Name: "HandleLogin", QualifiedName: "auth.HandleLogin", Language: "go", SourceRef: ref, Signature: "HandleLogin(request) error", ContentHash: ref.ContentHash}
	sharedIDs := &ids{}
	retriever, err := ragapp.New(memory.NewRAGRepository(), nil, nil, nil, sharedIDs, clock{}, nil, nil, ragapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retriever.Index(ctx, scope, []repository.CodeArtifact{artifact}); err != nil {
		t.Fatal(err)
	}
	agentRepository := memory.NewAgentRepository()
	agentService, err := agentapp.New(nil, retriever, agentRepository, nil, nil, nil, nil, sharedIDs, clock{}, agentapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := agentService.Ask(ctx, agentdomain.QuestionCommand{Scope: scope, ConversationID: "conversation", UserID: "user", Question: "How does HandleLogin work?", Permissions: []string{agentdomain.ReadPermission}, Locale: "en-US"})
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	for event := range stream {
		runID = event.RunID
	}
	run, err := agentRepository.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != agentdomain.RunCompleted || run.Answer == nil || run.Answer.InsufficientEvidence || len(run.Answer.Citations) != 1 || run.Answer.Citations[0].Path != ref.Path {
		t.Fatalf("RAG evidence did not reach the agent: %#v", run)
	}
}
