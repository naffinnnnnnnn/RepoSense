package agent

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput         ErrorCode = "INVALID_INPUT"
	ErrPermissionDenied     ErrorCode = "PERMISSION_DENIED"
	ErrKnowledgeUnavailable ErrorCode = "KNOWLEDGE_UNAVAILABLE"
	ErrGenerationFailure    ErrorCode = "ANSWER_GENERATION_FAILURE"
	ErrRunCancelled         ErrorCode = "AGENT_RUN_CANCELLED"
	ErrRunNotFound          ErrorCode = "AGENT_RUN_NOT_FOUND"
	ErrRunNotResumable      ErrorCode = "AGENT_RUN_NOT_RESUMABLE"
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
