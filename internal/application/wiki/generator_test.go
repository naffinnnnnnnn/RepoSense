package wikiapp

import (
	"context"
	"strings"
	"testing"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

func TestStructuredGeneratorBuildsAllGroundedPages(t *testing.T) {
	generator := NewStructuredGenerator(StructuredGeneratorConfig{MaxItemsPerSection: 5})
	result, err := generator.Generate(context.Background(), ports.WikiGenerationContext{Locale: "zh-CN", PageSlugs: []string{"overview", "architecture", "modules", "key-flows", "interfaces", "development-guide"}, Graph: sampleGraph("gr", "sha")})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pages) != 6 {
		t.Fatalf("expected six pages, got %d", len(result.Pages))
	}
	for _, page := range result.Pages {
		if len(page.Citations) == 0 || !strings.Contains(page.ContentMarkdown, "源码依据") || !strings.Contains(page.ContentMarkdown, "src/main.go:1-8") {
			t.Fatalf("page %s is not grounded: %#v", page.Slug, page)
		}
	}
}

func TestStructuredGeneratorEnglishAndNoEvidence(t *testing.T) {
	generator := NewStructuredGenerator(StructuredGeneratorConfig{})
	result, err := generator.Generate(context.Background(), ports.WikiGenerationContext{Locale: "en-US", PageSlugs: []string{"overview"}, Graph: sampleGraph("gr", "sha")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Pages[0].ContentMarkdown, "Source Evidence") {
		t.Fatal("expected English output")
	}
	empty := sampleGraph("gr", "sha")
	for index := range empty.Nodes {
		empty.Nodes[index].SourceRef = nil
	}
	empty.Edges = nil
	if _, err := generator.Generate(context.Background(), ports.WikiGenerationContext{PageSlugs: []string{"overview"}, Graph: empty}); err == nil {
		t.Fatal("expected insufficient evidence error")
	}
}

func sampleGraph(revisionID, commit string) graph.Result {
	ref := common.SourceRef{CommitSHA: commit, Path: "src/main.go", SymbolID: "main", StartLine: 1, EndLine: 8, ContentHash: "sha256:main"}
	helperRef := common.SourceRef{CommitSHA: commit, Path: "internal/helper.go", SymbolID: "helper", StartLine: 3, EndLine: 9, ContentHash: "sha256:helper"}
	return graph.Result{Nodes: []graph.Entity{
		{NodeID: "n1", EntityType: graph.EntityFunction, ArtifactID: "main", Name: "main", QualifiedName: "cmd.main", SourceRef: &ref, Properties: map[string]string{"language": "go"}},
		{NodeID: "n2", EntityType: graph.EntityFunction, ArtifactID: "helper", Name: "helper", QualifiedName: "internal.helper", SourceRef: &helperRef, Properties: map[string]string{"language": "go"}},
	}, Edges: []graph.Relation{{EdgeID: "e1", RelationType: repository.RelationCalls, FromNodeID: "n1", ToNodeID: "n2", Evidence: ref, Confidence: 1}},
		Diagnostics: graph.Diagnostics{RevisionID: revisionID, Visited: 2}}
}
