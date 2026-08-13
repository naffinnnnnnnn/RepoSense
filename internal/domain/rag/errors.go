package rag

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrInvalidInput     ErrorCode = "INVALID_INPUT"
	ErrIndexNotFound    ErrorCode = "RAG_INDEX_NOT_FOUND"
	ErrIndexConflict    ErrorCode = "RAG_INDEX_CONFLICT"
	ErrIndexFailure     ErrorCode = "RAG_INDEX_FAILURE"
	ErrRetrievalFailure ErrorCode = "RAG_RETRIEVAL_FAILURE"
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
