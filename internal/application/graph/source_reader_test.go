package graphapp

import (
	"context"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type pagedSourceFake struct {
	metadata  graph.SnapshotMetadata
	artifacts map[string]graph.ArtifactPage
	relations map[string]graph.RelationPage
}

func (f pagedSourceFake) SnapshotMetadata(context.Context, common.Scope) (graph.SnapshotMetadata, error) {
	return f.metadata, nil
}
func (f pagedSourceFake) ArtifactPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.ArtifactPage, error) {
	return f.artifacts[cursor], nil
}
func (f pagedSourceFake) RelationPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.RelationPage, error) {
	return f.relations[cursor], nil
}

type pageCollector struct{ artifacts, relations int }

func (c *pageCollector) ConsumeArtifactPage(_ context.Context, _ graph.SnapshotMetadata, values []repository.CodeArtifact) error {
	c.artifacts += len(values)
	return nil
}
func (c *pageCollector) ConsumeRelationPage(_ context.Context, _ graph.SnapshotMetadata, values []graph.ResolvedRelation) error {
	c.relations += len(values)
	return nil
}

func TestSourceReaderConsumesStableVersionedPages(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"}
	metadata := graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-1", ParseResultChecksum: "sum", SchemaVersion: "schema-1", CompletedAt: time.Now(), ArtifactCount: 2, RelationCount: 1}
	identity := func(cursor, next string) graph.PageIdentity {
		return graph.PageIdentity{Scope: scope, CommitSHA: metadata.CommitSHA, ParserResultVersion: metadata.ParserResultVersion, ParseResultChecksum: metadata.ParseResultChecksum, Cursor: cursor, NextCursor: next}
	}
	ref := common.SourceRef{CommitSHA: "commit", Path: "pkg/a.go", StartLine: 1, EndLine: 1, ContentHash: "hash"}
	source := pagedSourceFake{metadata: metadata,
		artifacts: map[string]graph.ArtifactPage{
			"":       {PageIdentity: identity("", "second"), Artifacts: []repository.CodeArtifact{{ArtifactID: "a", Kind: repository.ArtifactFunction, Name: "a", SourceRef: ref}}},
			"second": {PageIdentity: identity("second", ""), Artifacts: []repository.CodeArtifact{{ArtifactID: "b", Kind: repository.ArtifactFunction, Name: "b", SourceRef: ref}}},
		},
		relations: map[string]graph.RelationPage{"": {PageIdentity: identity("", ""), Relations: []graph.ResolvedRelation{{RelationID: "r", Kind: repository.RelationCalls, FromArtifactID: "a", TargetArtifactID: "b", ResolutionStatus: graph.ResolutionResolved, Evidence: ref, Confidence: 1}}}},
	}
	reader, err := NewSourceReader(source, 100, []string{"schema-1"})
	if err != nil {
		t.Fatal(err)
	}
	collector := &pageCollector{}
	if _, err := reader.Read(context.Background(), scope, collector); err != nil {
		t.Fatal(err)
	}
	if collector.artifacts != 2 || collector.relations != 1 {
		t.Fatalf("unexpected collected counts: %#v", collector)
	}
}

func TestSourceReaderRejectsPageIdentityChange(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"}
	metadata := graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-1", ParseResultChecksum: "sum", SchemaVersion: "schema-1", CompletedAt: time.Now()}
	changed := graph.PageIdentity{Scope: scope, CommitSHA: "other", ParserResultVersion: metadata.ParserResultVersion, ParseResultChecksum: metadata.ParseResultChecksum}
	source := pagedSourceFake{metadata: metadata, artifacts: map[string]graph.ArtifactPage{"": {PageIdentity: changed}}, relations: map[string]graph.RelationPage{}}
	reader, err := NewSourceReader(source, 100, []string{"schema-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Read(context.Background(), scope, &pageCollector{})
	assertGraphCode(t, err, graph.ErrParserResultChanged)
}

func TestSourceReaderRejectsDuplicateIdentityAcrossPages(t *testing.T) {
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot"}
	metadata := graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-1", ParseResultChecksum: "sum", SchemaVersion: "schema-1", CompletedAt: time.Now(), ArtifactCount: 2}
	identity := func(cursor, next string) graph.PageIdentity {
		return graph.PageIdentity{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-1", ParseResultChecksum: "sum", Cursor: cursor, NextCursor: next}
	}
	ref := common.SourceRef{CommitSHA: "commit", Path: "pkg/a.go", StartLine: 1, EndLine: 1, ContentHash: "hash"}
	artifact := repository.CodeArtifact{ArtifactID: "same", Kind: repository.ArtifactFunction, Name: "same", SourceRef: ref}
	source := pagedSourceFake{metadata: metadata, artifacts: map[string]graph.ArtifactPage{
		"":     {PageIdentity: identity("", "next"), Artifacts: []repository.CodeArtifact{artifact}},
		"next": {PageIdentity: identity("next", ""), Artifacts: []repository.CodeArtifact{artifact}},
	}, relations: map[string]graph.RelationPage{"": {PageIdentity: identity("", "")}}}
	reader, err := NewSourceReader(source, 100, []string{"schema-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Read(context.Background(), scope, &pageCollector{})
	assertGraphCode(t, err, graph.ErrParserResultInvalid)
}
