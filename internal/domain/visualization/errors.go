package visualization

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput      ErrorCode = "INVALID_INPUT"
	ErrGraphNotFound     ErrorCode = "GRAPH_REVISION_NOT_FOUND"
	ErrRevisionMismatch  ErrorCode = "GRAPH_REVISION_MISMATCH"
	ErrProjectionFailure ErrorCode = "PROJECTION_FAILURE"
	ErrPersistence       ErrorCode = "PERSISTENCE_FAILURE"
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
