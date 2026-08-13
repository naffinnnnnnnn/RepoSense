package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/reposense/reposense/internal/domain/agent"
	"github.com/reposense/reposense/internal/domain/common"
)

// AgentRepository is a concurrency-safe test/local adapter. Production can
// replace it with a transactional PostgreSQL adapter using the same port.
type AgentRepository struct {
	mu            sync.RWMutex
	conversations map[string]agent.Conversation
	runs          map[string]agent.Run
}

func NewAgentRepository() *AgentRepository {
	return &AgentRepository{conversations: map[string]agent.Conversation{}, runs: map[string]agent.Run{}}
}

func (r *AgentRepository) CreateRun(ctx context.Context, conversation agent.Conversation, run agent.Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.runs[run.RunID]; exists {
		return fmt.Errorf("agent run %q already exists", run.RunID)
	}
	if current, exists := r.conversations[conversation.ConversationID]; exists {
		if current.Scope.TenantID != conversation.Scope.TenantID || current.Scope.RepositoryID != conversation.Scope.RepositoryID || current.Scope.SnapshotID != conversation.Scope.SnapshotID {
			return errors.New("conversation is pinned to another repository snapshot")
		}
	} else {
		r.conversations[conversation.ConversationID] = conversation
	}
	r.runs[run.RunID] = cloneRun(run)
	return nil
}

func (r *AgentRepository) UpdateRun(ctx context.Context, run agent.Run) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.runs[run.RunID]
	if !exists {
		return fmt.Errorf("agent run %q not found", run.RunID)
	}
	if current.ConversationID != run.ConversationID || current.SnapshotID != run.SnapshotID || current.TenantID != run.TenantID || current.RepositoryID != run.RepositoryID {
		return errors.New("immutable agent run identity changed")
	}
	if terminal(current.Status) && current.Status != run.Status {
		return fmt.Errorf("agent run in terminal status %s cannot transition to %s", current.Status, run.Status)
	}
	run.Version = current.Version + 1
	r.runs[run.RunID] = cloneRun(run)
	return nil
}

func (r *AgentRepository) GetRun(ctx context.Context, runID string) (agent.Run, error) {
	if err := ctx.Err(); err != nil {
		return agent.Run{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	run, exists := r.runs[runID]
	if !exists {
		return agent.Run{}, fmt.Errorf("agent run %q not found", runID)
	}
	return cloneRun(run), nil
}

func terminal(status agent.RunStatus) bool {
	return status == agent.RunCompleted || status == agent.RunFailed
}

func cloneRun(run agent.Run) agent.Run {
	copy := run
	copy.Plan.Steps = append([]agent.PlanStep(nil), run.Plan.Steps...)
	copy.Plan.Strategies = append([]string(nil), run.Plan.Strategies...)
	copy.ToolCalls = append([]agent.ToolCall(nil), run.ToolCalls...)
	for i := range copy.ToolCalls {
		if run.ToolCalls[i].Arguments != nil {
			copy.ToolCalls[i].Arguments = make(map[string]string, len(run.ToolCalls[i].Arguments))
			for key, value := range run.ToolCalls[i].Arguments {
				copy.ToolCalls[i].Arguments[key] = value
			}
		}
	}
	if run.Answer != nil {
		answer := *run.Answer
		answer.Citations = append([]common.SourceRef(nil), run.Answer.Citations...)
		answer.Warnings = append([]string(nil), run.Answer.Warnings...)
		copy.Answer = &answer
	}
	if run.PublishedEvent.Payload != nil {
		copy.PublishedEvent.Payload = make(map[string]any, len(run.PublishedEvent.Payload))
		for key, value := range run.PublishedEvent.Payload {
			copy.PublishedEvent.Payload[key] = value
		}
	}
	return copy
}
