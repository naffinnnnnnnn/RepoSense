package repository

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput       ErrorCode = "INVALID_INPUT"
	ErrRepositoryNotFound ErrorCode = "REPOSITORY_NOT_FOUND"
	ErrRefNotFound        ErrorCode = "REF_NOT_FOUND"
	ErrGitFailure         ErrorCode = "GIT_FAILURE"
	ErrParseFailure       ErrorCode = "PARSE_FAILURE"
	ErrPersistence        ErrorCode = "PERSISTENCE_FAILURE"
)

type DomainError struct {
	Code      ErrorCode
	Operation string
	Message   string
	Retryable bool
	Cause     error
}

type FailureDescriptor struct {
	Code      ErrorCode
	Operation string
	Message   string
	Retryable bool
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *DomainError) Unwrap() error { return e.Cause }

func IsCode(err error, code ErrorCode) bool {
	var target *DomainError
	return errors.As(err, &target) && target.Code == code
}
