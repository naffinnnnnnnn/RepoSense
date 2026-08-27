package repository

import (
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

type RepositoryBinding struct {
	TenantID          string    `json:"tenant_id"`
	RepositoryID      string    `json:"repository_id"`
	Provider          string    `json:"provider"`
	CanonicalIdentity string    `json:"canonical_identity"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type ParseTask struct {
	Command            SyncCommand `json:"command"`
	CommandFingerprint string      `json:"command_fingerprint"`
	RepositoryIdentity string      `json:"repository_identity"`
	Job                ParseJob    `json:"job"`
	Snapshot           Snapshot    `json:"snapshot"`
	Retryable          bool        `json:"retryable"`
	Attempt            int         `json:"attempt"`
	LeaseOwner         string      `json:"lease_owner,omitempty"`
	LeaseExpiresAt     time.Time   `json:"lease_expires_at,omitempty"`
	CancelRequested    bool        `json:"cancel_requested"`
}

type OutboxRecord struct {
	TenantID       string               `json:"tenant_id"`
	RepositoryID   string               `json:"repository_id"`
	Event          common.EventEnvelope `json:"event"`
	EventID        string               `json:"event_id"`
	EventType      string               `json:"event_type"`
	AggregateID    string               `json:"aggregate_id"`
	TraceID        string               `json:"trace_id"`
	PayloadVersion int                  `json:"payload_version"`
	Payload        []byte               `json:"payload"`
	OccurredAt     time.Time            `json:"occurred_at"`
	PublishedAt    time.Time            `json:"published_at,omitempty"`
	DeliveryCount  int                  `json:"delivery_count"`
	LastError      string               `json:"last_error,omitempty"`
	NextAttemptAt  time.Time            `json:"next_attempt_at,omitempty"`
	DeadLetteredAt time.Time            `json:"dead_lettered_at,omitempty"`
}

type IdempotencyConflictError struct{ Message string }

func (e *IdempotencyConflictError) Error() string { return e.Message }
