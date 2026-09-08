package graph

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput             ErrorCode = "INVALID_INPUT"
	ErrUnsupportedBuildMode     ErrorCode = "UNSUPPORTED_BUILD_MODE"
	ErrPartialBuildNotAllowed   ErrorCode = "PARTIAL_BUILD_NOT_ALLOWED"
	ErrIdempotencyConflict      ErrorCode = "IDEMPOTENCY_CONFLICT"
	ErrSnapshotNotFound         ErrorCode = "SNAPSHOT_NOT_FOUND"
	ErrParserResultUnavailable  ErrorCode = "PARSER_RESULT_UNAVAILABLE"
	ErrParserResultInvalid      ErrorCode = "PARSER_RESULT_INVALID"
	ErrParserResultChanged      ErrorCode = "PARSER_RESULT_CHANGED"
	ErrBuildCapacityExceeded    ErrorCode = "BUILD_CAPACITY_EXCEEDED"
	ErrRevisionNotFound         ErrorCode = "GRAPH_REVISION_NOT_FOUND"
	ErrConflict                 ErrorCode = "GRAPH_REVISION_CONFLICT"
	ErrBuildFailure             ErrorCode = "GRAPH_BUILD_FAILURE"
	ErrPersistence              ErrorCode = "PERSISTENCE_FAILURE"
	ErrQueryBudgetExceeded      ErrorCode = "QUERY_BUDGET_EXCEEDED"
	ErrGraphInconsistent        ErrorCode = "GRAPH_INCONSISTENT"
	ErrGraphStoreUnavailable    ErrorCode = "GRAPH_STORE_UNAVAILABLE"
	ErrGraphBatchWriteFailed    ErrorCode = "GRAPH_BATCH_WRITE_FAILED"
	ErrGraphValidationFailed    ErrorCode = "GRAPH_VALIDATION_FAILED"
	ErrControlStoreUnavailable  ErrorCode = "CONTROL_STORE_UNAVAILABLE"
	ErrLeaseLost                ErrorCode = "LEASE_LOST"
	ErrActivationConflict       ErrorCode = "ACTIVATION_CONFLICT"
	ErrActivationFailed         ErrorCode = "ACTIVATION_FAILED"
	ErrActivationOutcomeUnknown ErrorCode = "ACTIVATION_OUTCOME_UNKNOWN"
	ErrRootNotFound             ErrorCode = "ROOT_NOT_FOUND"
	ErrBuildCancelled           ErrorCode = "BUILD_CANCELLED"
	ErrWorkerShutdown           ErrorCode = "WORKER_SHUTDOWN"
	ErrBuildTimeout             ErrorCode = "BUILD_TIMEOUT"
	ErrQueryCancelled           ErrorCode = "QUERY_CANCELLED"
	ErrQueryTimeout             ErrorCode = "QUERY_TIMEOUT"
	ErrEventPublishFailed       ErrorCode = "EVENT_PUBLISH_FAILED"
)

type DomainError struct {
	Code       ErrorCode
	Operation  string
	Stage      string
	Dependency string
	Message    string
	Retryable  bool
	Cause      error
}

func (e *DomainError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }
func (e *DomainError) Unwrap() error { return e.Cause }

func IsCode(err error, code ErrorCode) bool {
	var target *DomainError
	return errors.As(err, &target) && target.Code == code
}
