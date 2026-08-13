package assistant

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput         ErrorCode = "INVALID_INPUT"
	ErrPermissionDenied     ErrorCode = "PERMISSION_DENIED"
	ErrSnapshotNotFound     ErrorCode = "SNAPSHOT_NOT_FOUND"
	ErrInsufficientEvidence ErrorCode = "INSUFFICIENT_EVIDENCE"
	ErrGenerationFailure    ErrorCode = "PROPOSAL_GENERATION_FAILURE"
	ErrProposalNotFound     ErrorCode = "PROPOSAL_NOT_FOUND"
	ErrProposalConflict     ErrorCode = "PROPOSAL_CONFLICT"
	ErrApprovalRequired     ErrorCode = "APPROVAL_REQUIRED"
	ErrApplyFailure         ErrorCode = "PROPOSAL_APPLY_FAILURE"
	ErrPersistence          ErrorCode = "PERSISTENCE_FAILURE"
)

type DomainError struct {
	Code      ErrorCode
	Operation string
	Message   string
	Retryable bool
	Cause     error
}

func (e *DomainError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }
func (e *DomainError) Unwrap() error { return e.Cause }

func IsCode(err error, code ErrorCode) bool {
	var target *DomainError
	return errors.As(err, &target) && target.Code == code
}
