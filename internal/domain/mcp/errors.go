package mcp

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput       ErrorCode = "INVALID_INPUT"
	ErrUnauthenticated    ErrorCode = "UNAUTHENTICATED"
	ErrPermissionDenied   ErrorCode = "PERMISSION_DENIED"
	ErrRateLimited        ErrorCode = "RATE_LIMITED"
	ErrCapabilityDisabled ErrorCode = "CAPABILITY_DISABLED"
	ErrUpstreamFailure    ErrorCode = "UPSTREAM_FAILURE"
	ErrAuditFailure       ErrorCode = "AUDIT_FAILURE"
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

func NewError(code ErrorCode, operation, message string, retryable bool, cause error) *DomainError {
	return &DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
}

func IsCode(err error, code ErrorCode) bool {
	var target *DomainError
	return errors.As(err, &target) && target.Code == code
}
