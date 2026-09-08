package graph

import (
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

// OutboxRecord contains delivery state kept outside the immutable event.
type OutboxRecord struct {
	Scope         common.Scope         `json:"scope"`
	RevisionID    string               `json:"revision_id"`
	Event         common.EventEnvelope `json:"event"`
	DeliveryCount int                  `json:"delivery_count"`
	NextAttemptAt time.Time            `json:"next_attempt_at"`
	LastError     string               `json:"last_error,omitempty"`
	CreatedAt     time.Time            `json:"created_at"`
}

type RejectedEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	TenantID      string    `json:"tenant_id,omitempty"`
	RepositoryID  string    `json:"repository_id,omitempty"`
	ErrorCode     ErrorCode `json:"error_code"`
	ErrorMessage  string    `json:"error_message"`
	PayloadDigest string    `json:"payload_digest,omitempty"`
	RejectedAt    time.Time `json:"rejected_at"`
}

type ReconciliationRun struct {
	RunID        string    `json:"run_id"`
	Action       string    `json:"action"`
	TargetType   string    `json:"target_type"`
	TargetID     string    `json:"target_id"`
	Result       string    `json:"result"`
	ErrorCode    ErrorCode `json:"error_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
}

type ActiveRevisionRef struct {
	Scope      common.Scope `json:"scope"`
	RevisionID string       `json:"revision_id"`
}
