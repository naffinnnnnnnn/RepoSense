package assistantapp

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	ragapp "github.com/reposense/reposense/internal/application/rag"
	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/repository"
)

func TestRepositoryRAGAssistantIntegration(t *testing.T) {
	ctx := context.Background()
	scope := assistantCommand().Scope
	ref := assistantSource("sha")
	store := memory.NewRepositoryStore()
	result := repository.ParseResult{Snapshot: repository.Snapshot{EntityMeta: repository.NewMeta("snap", scope, repository.StatusSucceeded, time.Unix(1, 0)),
		SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded},
		Artifacts: []repository.CodeArtifact{{ArtifactID: "file", Kind: repository.ArtifactFile, Name: "main.go", QualifiedName: "main.go", Language: "go", SourceRef: ref, ContentHash: ref.ContentHash},
			{ArtifactID: "main", Kind: repository.ArtifactFunction, Name: "main", QualifiedName: "main", Language: "go", SourceRef: ref, ContentHash: ref.ContentHash}}}
	if err := store.SaveResult(ctx, "parse-key", result); err != nil {
		t.Fatal(err)
	}
	ragRepo := memory.NewRAGRepository()
	retriever, err := ragapp.New(ragRepo, nil, nil, nil, &testIDs{}, testClock{time.Unix(2, 0)}, nil, nil, ragapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retriever.Index(ctx, scope, result.Artifacts); err != nil {
		t.Fatal(err)
	}
	generator := &draftGenerator{draft: patchDraft()}
	service, err := New(store, retriever, memory.NewAssistantRepository(), generator, nil, nil, nil, &testIDs{}, testClock{time.Unix(3, 0)}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := service.Propose(ctx, assistantCommand())
	if err != nil {
		t.Fatal(err)
	}
	if proposal.ApprovalStatus != assistant.ProposalAwaitingApproval || len(proposal.Citations) != 1 || proposal.Citations[0].CommitSHA != "sha" {
		t.Fatalf("unexpected integrated proposal %#v", proposal)
	}
}
