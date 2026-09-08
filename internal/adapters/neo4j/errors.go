package neo4j

import (
	"context"
	"errors"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/reposense/reposense/internal/domain/graph"
)

func invalidInput(operation, stage, message string, cause error) error {
	return &graph.DomainError{Code: graph.ErrInvalidInput, Operation: operation, Stage: stage, Dependency: "neo4j", Message: message, Retryable: false, Cause: cause}
}

func validationError(operation, stage, message string) error {
	return &graph.DomainError{Code: graph.ErrGraphValidationFailed, Operation: operation, Stage: stage, Dependency: "neo4j", Message: message, Retryable: false}
}

func revisionNotFound(operation string, cause error) error {
	return &graph.DomainError{Code: graph.ErrRevisionNotFound, Operation: operation, Stage: "query", Dependency: "neo4j", Message: "graph revision was not found", Retryable: false, Cause: cause}
}

func queryBudgetError(operation, message string) error {
	return &graph.DomainError{Code: graph.ErrQueryBudgetExceeded, Operation: operation, Stage: "query", Dependency: "neo4j", Message: message, Retryable: false}
}

func graphStoreError(operation, stage string, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, context.Canceled) {
		return cause
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return &graph.DomainError{Code: graph.ErrGraphStoreUnavailable, Operation: operation, Stage: stage, Dependency: "neo4j", Message: "neo4j operation timed out", Retryable: true, Cause: cause}
	}
	if neo4jdriver.IsRetryable(cause) {
		return &graph.DomainError{Code: graph.ErrGraphStoreUnavailable, Operation: operation, Stage: stage, Dependency: "neo4j", Message: "neo4j is temporarily unavailable", Retryable: true, Cause: cause}
	}
	code := graph.ErrGraphStoreUnavailable
	message := "neo4j is unavailable"
	if stage == "write_artifacts" || stage == "write_relations" {
		code, message = graph.ErrGraphBatchWriteFailed, "neo4j graph batch write failed"
	} else if stage == "seal" {
		code, message = graph.ErrGraphValidationFailed, "neo4j candidate validation failed"
	}
	return &graph.DomainError{Code: code, Operation: operation, Stage: stage, Dependency: "neo4j", Message: message, Retryable: false, Cause: cause}
}
