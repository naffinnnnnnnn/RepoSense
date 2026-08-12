package graph

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput     ErrorCode = "INVALID_INPUT"
	ErrSnapshotNotFound ErrorCode = "SNAPSHOT_NOT_FOUND"
	ErrRevisionNotFound ErrorCode = "GRAPH_REVISION_NOT_FOUND"
	ErrConflict         ErrorCode = "GRAPH_REVISION_CONFLICT"
	ErrBuildFailure     ErrorCode = "GRAPH_BUILD_FAILURE"
	ErrPersistence      ErrorCode = "PERSISTENCE_FAILURE"
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
