package rag

import (
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
)

func TestRetrievalRequestValidationAndStrategyAliases(t *testing.T) {
	request := RetrievalRequest{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}, Query: "  HandlePayment  ", Strategies: []string{"bm25", "VECTOR", "symbol", "symbol"}, TopK: 20}
	if err := request.Validate(100, 50); err != nil {
		t.Fatal(err)
	}
	strategies, err := NormalizeStrategies(request.Strategies)
	if err != nil {
		t.Fatal(err)
	}
	want := []Strategy{StrategyKeyword, StrategySemantic, StrategySymbol}
	if len(strategies) != len(want) {
		t.Fatalf("unexpected strategies: %#v", strategies)
	}
	for i := range want {
		if strategies[i] != want[i] {
			t.Fatalf("unexpected strategies: %#v", strategies)
		}
	}
}

func TestRetrievalRequestRejectsInvalidBoundaries(t *testing.T) {
	base := RetrievalRequest{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap"}, Query: "query"}
	tests := []RetrievalRequest{
		{Scope: base.Scope, Query: "   "},
		{Scope: base.Scope, Query: "query", TopK: 101},
		{Scope: base.Scope, Query: "query", Strategies: []string{"SQL"}},
		{Scope: base.Scope, Query: "query", Filters: Filters{PathPrefixes: []string{"../secret"}}},
	}
	for i, request := range tests {
		if err := request.Validate(100, 100); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}
