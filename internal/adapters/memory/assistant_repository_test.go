package memory

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
)

func assistantFixture() (common.Scope, assistant.CodingSession, assistant.ChangeProposal) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}
	session := assistant.CodingSession{SessionID: "session", UserID: "user", BaseCommitSHA: "sha"}
	proposal := assistant.ChangeProposal{EntityMeta: assistant.NewMeta("cp", scope, assistant.ProposalAwaitingApproval, "user", time.Unix(1, 0)),
		ProposalID: "cp", SessionID: "session", UserID: "user", SnapshotID: "snap", BaseCommitSHA: "sha", ApprovalStatus: assistant.ProposalAwaitingApproval}
	return scope, session, proposal
}

func TestAssistantRepositoryScopesAndCompareAndSwap(t *testing.T) {
	repo := NewAssistantRepository()
	_, session, proposal := assistantFixture()
	if err := repo.CreateProposal(context.Background(), "key", session, proposal); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetProposal(context.Background(), common.Scope{TenantID: "other", RepositoryID: "repo", SnapshotID: "snap"}, proposal.ProposalID); err == nil {
		t.Fatal("cross-tenant proposal read must fail")
	}
	proposal.ApprovalStatus = assistant.ProposalApplying
	if err := repo.UpdateProposal(context.Background(), proposal, 99); err == nil {
		t.Fatal("stale version must fail")
	}
	if err := repo.UpdateProposal(context.Background(), proposal, 1); err != nil {
		t.Fatal(err)
	}
	proposal.Version = 2
	proposal.ApprovalStatus = assistant.ProposalAwaitingApproval
	if err := repo.UpdateProposal(context.Background(), proposal, 2); err == nil {
		t.Fatal("reverse state transition must fail")
	}
}
