package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
)

// AssistantRepository is a concurrency-safe local adapter. The create and
// compare-and-swap update operations mirror the transaction boundaries a
// PostgreSQL implementation must provide.
type AssistantRepository struct {
	mu          sync.RWMutex
	sessions    map[string]assistant.CodingSession
	proposals   map[string]assistant.ChangeProposal
	idempotency map[string]string
}

func NewAssistantRepository() *AssistantRepository {
	return &AssistantRepository{sessions: map[string]assistant.CodingSession{}, proposals: map[string]assistant.ChangeProposal{}, idempotency: map[string]string{}}
}

func assistantScopeKey(scope common.Scope) string {
	return scope.TenantID + "\x00" + scope.RepositoryID + "\x00" + scope.SnapshotID
}
func proposalKey(scope common.Scope, proposalID string) string {
	return assistantScopeKey(scope) + "\x00" + proposalID
}
func assistantIdempotencyKey(scope common.Scope, key string) string {
	return assistantScopeKey(scope) + "\x00" + key
}

func (r *AssistantRepository) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (assistant.ChangeProposal, bool, error) {
	if err := ctx.Err(); err != nil {
		return assistant.ChangeProposal{}, false, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	proposalID, ok := r.idempotency[assistantIdempotencyKey(scope, key)]
	if !ok {
		return assistant.ChangeProposal{}, false, nil
	}
	proposal, ok := r.proposals[proposalKey(scope, proposalID)]
	return cloneProposal(proposal), ok, nil
}

func (r *AssistantRepository) CreateProposal(ctx context.Context, idempotencyKey string, session assistant.CodingSession, proposal assistant.ChangeProposal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope := common.Scope{TenantID: proposal.TenantID, RepositoryID: proposal.RepositoryID, SnapshotID: proposal.SnapshotID}
	r.mu.Lock()
	defer r.mu.Unlock()
	idemKey := assistantIdempotencyKey(scope, idempotencyKey)
	if existingID, exists := r.idempotency[idemKey]; exists {
		if existingID == proposal.ProposalID {
			return nil
		}
		return errors.New("assistant idempotency key already exists")
	}
	pKey := proposalKey(scope, proposal.ProposalID)
	if _, exists := r.proposals[pKey]; exists {
		return fmt.Errorf("proposal %q already exists", proposal.ProposalID)
	}
	sKey := assistantScopeKey(scope) + "\x00" + session.SessionID
	if current, exists := r.sessions[sKey]; exists {
		if current.UserID != session.UserID || current.BaseCommitSHA != session.BaseCommitSHA {
			return errors.New("coding session is pinned to another user or base commit")
		}
	} else {
		r.sessions[sKey] = cloneSession(session)
	}
	r.proposals[pKey] = cloneProposal(proposal)
	r.idempotency[idemKey] = proposal.ProposalID
	return nil
}

func (r *AssistantRepository) GetProposal(ctx context.Context, scope common.Scope, proposalID string) (assistant.ChangeProposal, error) {
	if err := ctx.Err(); err != nil {
		return assistant.ChangeProposal{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	proposal, exists := r.proposals[proposalKey(scope, proposalID)]
	if !exists {
		return assistant.ChangeProposal{}, fmt.Errorf("proposal %q not found", proposalID)
	}
	return cloneProposal(proposal), nil
}

func (r *AssistantRepository) UpdateProposal(ctx context.Context, proposal assistant.ChangeProposal, expectedVersion int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope := common.Scope{TenantID: proposal.TenantID, RepositoryID: proposal.RepositoryID, SnapshotID: proposal.SnapshotID}
	key := proposalKey(scope, proposal.ProposalID)
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.proposals[key]
	if !exists {
		return fmt.Errorf("proposal %q not found", proposal.ProposalID)
	}
	if current.Version != expectedVersion {
		return fmt.Errorf("proposal version conflict: expected %d, current %d", expectedVersion, current.Version)
	}
	if current.SessionID != proposal.SessionID || current.UserID != proposal.UserID || current.BaseCommitSHA != proposal.BaseCommitSHA {
		return errors.New("immutable proposal identity changed")
	}
	if !validProposalTransition(current.ApprovalStatus, proposal.ApprovalStatus) {
		return fmt.Errorf("invalid proposal transition %s -> %s", current.ApprovalStatus, proposal.ApprovalStatus)
	}
	proposal.Version = expectedVersion + 1
	r.proposals[key] = cloneProposal(proposal)
	return nil
}

func validProposalTransition(from, to assistant.ProposalStatus) bool {
	switch from {
	case assistant.ProposalAwaitingApproval:
		return to == assistant.ProposalApplying || to == assistant.ProposalRejected
	case assistant.ProposalApplying:
		return to == assistant.ProposalApplied || to == assistant.ProposalFailed
	default:
		return false
	}
}

func cloneSession(session assistant.CodingSession) assistant.CodingSession {
	copy := session
	copy.ContextRefs = append([]common.SourceRef(nil), session.ContextRefs...)
	return copy
}

func cloneProposal(proposal assistant.ChangeProposal) assistant.ChangeProposal {
	copy := proposal
	copy.FileChanges = append([]assistant.FileChange(nil), proposal.FileChanges...)
	copy.TestPlan = append([]string(nil), proposal.TestPlan...)
	copy.Citations = append([]common.SourceRef(nil), proposal.Citations...)
	copy.Validation = append([]assistant.ValidationResult(nil), proposal.Validation...)
	if proposal.PublishedEvent.Payload != nil {
		copy.PublishedEvent.Payload = make(map[string]any, len(proposal.PublishedEvent.Payload))
		for key, value := range proposal.PublishedEvent.Payload {
			copy.PublishedEvent.Payload[key] = value
		}
	}
	return copy
}
