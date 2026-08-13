package wiki

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput         ErrorCode = "INVALID_INPUT"
	ErrGraphNotReady        ErrorCode = "GRAPH_NOT_READY"
	ErrInsufficientEvidence ErrorCode = "INSUFFICIENT_EVIDENCE"
	ErrGenerationFailure    ErrorCode = "WIKI_GENERATION_FAILURE"
	ErrPageNotFound         ErrorCode = "WIKI_PAGE_NOT_FOUND"
	ErrConflict             ErrorCode = "WIKI_REVISION_CONFLICT"
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
