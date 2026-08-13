package memory

import (
	"context"
	"sync"

	mcpdomain "github.com/reposense/reposense/internal/domain/mcp"
)

// MCPAudit is an append-only in-memory audit adapter for local composition and tests.
// Production adapters can persist the same domain records without changing the MCP facade.
type MCPAudit struct {
	mu      sync.RWMutex
	records []mcpdomain.Invocation
}

func NewMCPAudit() *MCPAudit { return &MCPAudit{} }

func (a *MCPAudit) Record(ctx context.Context, invocation mcpdomain.Invocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, invocation)
	return nil
}

func (a *MCPAudit) Records() []mcpdomain.Invocation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]mcpdomain.Invocation(nil), a.records...)
}

var _ mcpdomain.AuditSink = (*MCPAudit)(nil)
