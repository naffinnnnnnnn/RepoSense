package visualizationapp_test

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	graphapp "github.com/reposense/reposense/internal/application/graph"
	visualizationapp "github.com/reposense/reposense/internal/application/visualization"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/domain/visualization"
)

type integrationSource struct{ input graph.BuildInput }

func (s integrationSource) GraphInput(context.Context, common.Scope) (graph.BuildInput, error) {
	return s.input, nil
}

type integrationIDs struct{ n int }

func (i *integrationIDs) New(prefix string) string {
	i.n++
	return prefix + "-integration"
}

type integrationClock struct{}

func (integrationClock) Now() time.Time { return time.Now().UTC() }

func TestKnowledgeGraphToCallGraphProjection(t *testing.T) {
	ctx := context.Background()
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	commit := "abc123"
	callerRef := common.SourceRef{CommitSHA: commit, Path: "cmd/api/main.go", SymbolID: "caller", StartLine: 10, EndLine: 15, ContentHash: "sha256:caller"}
	calleeRef := common.SourceRef{CommitSHA: commit, Path: "internal/service.go", SymbolID: "callee", StartLine: 20, EndLine: 30, ContentHash: "sha256:callee"}
	input := graph.BuildInput{
		Snapshot: repository.Snapshot{EntityMeta: common.EntityMeta{TenantID: scope.TenantID, RepositoryID: scope.RepositoryID}, SnapshotID: scope.SnapshotID, CommitSHA: commit, SyncStatus: repository.StatusSucceeded},
		Artifacts: []repository.CodeArtifact{
			{ArtifactID: "caller", Kind: repository.ArtifactFunction, Name: "main", QualifiedName: "cmd.api.main", Language: "go", SourceRef: callerRef, ContentHash: callerRef.ContentHash, Attributes: map[string]string{"language": "go"}},
			{ArtifactID: "callee", Kind: repository.ArtifactFunction, Name: "Serve", QualifiedName: "internal.Serve", Language: "go", SourceRef: calleeRef, ContentHash: calleeRef.ContentHash, Attributes: map[string]string{"language": "go"}},
		},
		Relations: []repository.CodeRelation{{RelationID: "call", Kind: repository.RelationCalls, From: "caller", To: "callee", Evidence: callerRef, Confidence: .99}},
	}
	graphRepository := memory.NewGraphRepository()
	ids := &integrationIDs{}
	graphService, err := graphapp.New(integrationSource{input: input}, graphRepository, nil, nil, ids, integrationClock{}, nil, graphapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := graphService.Build(ctx, graph.BuildCommand{Scope: scope, Mode: graph.BuildFull, IdempotencyKey: "graph-build"})
	if err != nil {
		t.Fatal(err)
	}
	visualService, err := visualizationapp.New(graphService, memory.NewVisualizationRepository(), nil, nil, ids, integrationClock{}, visualizationapp.Config{CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := visualService.Project(ctx, visualization.Query{Scope: scope, GraphRevisionID: revision.RevisionID,
		ViewType: visualization.ViewCallGraph, RootIDs: []string{"caller"}, Depth: 1, IncludeMermaid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Nodes) != 2 || len(projection.Edges) != 1 || projection.Edges[0].Type != repository.RelationCalls {
		t.Fatalf("unexpected end-to-end projection: %#v", projection)
	}
	if projection.SourceLinks[projection.Edges[0].ID].Path != callerRef.Path || projection.GraphRevisionID != revision.RevisionID || projection.Mermaid == "" {
		t.Fatalf("revision or source traceability was lost: %#v", projection)
	}
}
