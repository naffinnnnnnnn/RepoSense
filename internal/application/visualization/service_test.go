package visualizationapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/visualization"
)

type graphStub struct {
	result graph.Result
	err    error
	calls  int
}

func (s *graphStub) Build(context.Context, graph.BuildCommand) (graph.Revision, error) {
	return graph.Revision{}, nil
}
func (s *graphStub) Query(context.Context, graph.Query) (graph.Result, error) {
	s.calls++
	return s.result, s.err
}

type testIDs struct{ n int }

func (i *testIDs) New(prefix string) string { i.n++; return prefix + "-test" }

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type failingCache struct{}

func (failingCache) Get(context.Context, common.Scope, string) (visualization.Projection, bool, error) {
	return visualization.Projection{}, false, errors.New("cache unavailable")
}
func (failingCache) Save(context.Context, common.Scope, string, visualization.Projection) error {
	return errors.New("cache unavailable")
}

func visualizationQuery() visualization.Query {
	return visualization.Query{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"},
		GraphRevisionID: "revision", ViewType: visualization.ViewCallGraph, Depth: 1, IncludeMermaid: true,
		Filters: visualization.Filters{MinConfidence: .8, Languages: []string{"go"}}}
}

func TestProjectFiltersRendersCachesAndDefensivelyCopies(t *testing.T) {
	ref := common.SourceRef{CommitSHA: "abc", Path: "main.go", SymbolID: "a", StartLine: 1, EndLine: 3, ContentHash: "sha256:x"}
	stub := &graphStub{result: graph.Result{Diagnostics: graph.Diagnostics{RevisionID: "revision", Visited: 3},
		Nodes: []graph.Entity{{NodeID: "n1", EntityType: graph.EntityFunction, Name: `caller "quoted"]`, ArtifactID: "a", SourceRef: &ref, Properties: map[string]string{"language": "go"}},
			{NodeID: "n2", EntityType: graph.EntityFunction, Name: "callee", SourceRef: &common.SourceRef{CommitSHA: "abc", Path: "other.go", StartLine: 1, EndLine: 2}},
			{NodeID: "n3", EntityType: graph.EntityFunction, Name: "python", SourceRef: &common.SourceRef{CommitSHA: "abc", Path: "x.py", StartLine: 1, EndLine: 2}}},
		Edges: []graph.Relation{{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "n1", ToNodeID: "n2", Confidence: .9, Evidence: ref},
			{EdgeID: "e2", RelationType: repository.RelationCalls, FromNodeID: "n2", ToNodeID: "n3", Confidence: .95},
			{EdgeID: "e3", RelationType: repository.RelationCalls, FromNodeID: "n2", ToNodeID: "n1", Confidence: .5}}}}
	cache := memory.NewVisualizationRepository()
	clock := testClock{now: time.Now().UTC()}
	service, err := New(stub, cache, nil, nil, &testIDs{}, clock, Config{CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Project(context.Background(), visualizationQuery())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Nodes) != 2 || len(first.Edges) != 1 || first.Edges[0].ID != "e1" {
		t.Fatalf("unexpected filtered projection: %#v", first)
	}
	if !strings.Contains(first.Mermaid, "&quot;") || !strings.Contains(first.Mermaid, "&#93;") || first.SourceLinks["n1"].Path != "main.go" || len(first.Layout.Positions) != 2 {
		t.Fatalf("render metadata is incomplete: %#v", first)
	}
	first.Nodes[0].Properties["language"] = "mutated"
	second, err := service.Project(context.Background(), visualizationQuery())
	if err != nil {
		t.Fatal(err)
	}
	if !second.Diagnostics.CacheHit || stub.calls != 1 || second.Nodes[0].Properties["language"] != "go" {
		t.Fatalf("cache or defensive copy failed: %#v calls=%d", second, stub.calls)
	}
}

func TestProjectRejectsRevisionDriftAndInvalidInput(t *testing.T) {
	stub := &graphStub{result: graph.Result{Diagnostics: graph.Diagnostics{RevisionID: "other"}}}
	service, _ := New(stub, nil, nil, nil, nil, nil, Config{})
	if _, err := service.Project(context.Background(), visualizationQuery()); !visualization.IsCode(err, visualization.ErrRevisionMismatch) {
		t.Fatalf("expected revision mismatch, got %v", err)
	}
	query := visualizationQuery()
	query.Scope.TenantID = ""
	if _, err := service.Project(context.Background(), query); !visualization.IsCode(err, visualization.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestDataFlowStatesCurrentEvidenceLimit(t *testing.T) {
	stub := &graphStub{result: graph.Result{Diagnostics: graph.Diagnostics{RevisionID: "revision"}}}
	service, _ := New(stub, nil, nil, nil, nil, nil, Config{})
	query := visualizationQuery()
	query.ViewType = visualization.ViewDataFlow
	query.Filters = visualization.Filters{}
	projection, err := service.Project(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Diagnostics.Warnings) != 1 || !strings.Contains(projection.Diagnostics.Warnings[0], "variable-level") {
		t.Fatalf("missing data-flow limitation warning: %#v", projection.Diagnostics.Warnings)
	}
}

func TestProjectDegradesWhenOptionalCacheFails(t *testing.T) {
	stub := &graphStub{result: graph.Result{Diagnostics: graph.Diagnostics{RevisionID: "revision"}}}
	service, _ := New(stub, failingCache{}, nil, nil, nil, nil, Config{})
	query := visualizationQuery()
	query.Filters = visualization.Filters{}
	projection, err := service.Project(context.Background(), query)
	if err != nil {
		t.Fatalf("optional cache failure should not fail projection: %v", err)
	}
	if len(projection.Diagnostics.Warnings) != 2 {
		t.Fatalf("cache degradation was not observable: %#v", projection.Diagnostics.Warnings)
	}
}
