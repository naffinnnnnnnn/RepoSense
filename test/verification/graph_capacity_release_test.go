//go:build release

package verification

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	graphapp "github.com/reposense/reposense/internal/application/graph"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type capacitySource struct {
	metadata       graph.SnapshotMetadata
	pageSize       int
	maxRequested   int
	artifactCursor int
}

func (s *capacitySource) SnapshotMetadata(context.Context, common.Scope) (graph.SnapshotMetadata, error) {
	return s.metadata, nil
}
func (s *capacitySource) ArtifactPage(_ context.Context, _ common.Scope, cursor string, limit int) (graph.ArtifactPage, error) {
	if limit > s.maxRequested {
		s.maxRequested = limit
	}
	start := 0
	if cursor != "" {
		value, err := strconv.Atoi(cursor)
		if err != nil {
			return graph.ArtifactPage{}, err
		}
		start = value
	}
	remaining := int(s.metadata.ArtifactCount) - start
	count := limit
	if count > remaining {
		count = remaining
	}
	items := make([]repository.CodeArtifact, count)
	for index := range items {
		id := fmt.Sprintf("artifact-%09d", start+index)
		items[index] = repository.CodeArtifact{ArtifactID: id, Kind: repository.ArtifactFunction, Name: id,
			SourceRef: common.SourceRef{CommitSHA: s.metadata.CommitSHA, Path: "generated.go", SymbolID: id, StartLine: 1, EndLine: 1, ContentHash: "hash"}, ContentHash: "hash"}
	}
	next := ""
	if start+count < int(s.metadata.ArtifactCount) {
		next = strconv.Itoa(start + count)
	}
	s.artifactCursor = start + count
	return graph.ArtifactPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor, NextCursor: next}, Artifacts: items}, nil
}
func (s *capacitySource) RelationPage(_ context.Context, _ common.Scope, cursor string, limit int) (graph.RelationPage, error) {
	if limit > s.maxRequested {
		s.maxRequested = limit
	}
	return graph.RelationPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor}}, nil
}

type capacityConsumer struct {
	artifacts int64
	maxPage   int
}

func (c *capacityConsumer) ConsumeArtifactPage(_ context.Context, _ graph.SnapshotMetadata, values []repository.CodeArtifact) error {
	c.artifacts += int64(len(values))
	if len(values) > c.maxPage {
		c.maxPage = len(values)
	}
	return nil
}
func (*capacityConsumer) ConsumeRelationPage(context.Context, graph.SnapshotMetadata, []graph.ResolvedRelation) error {
	return nil
}

func TestGraphCapacityGateUsesBoundedStreamingPages(t *testing.T) {
	count := int64(100_000)
	if raw := os.Getenv("REPOSENSE_GRAPH_CAPACITY_ARTIFACTS"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value <= 0 {
			t.Fatalf("REPOSENSE_GRAPH_CAPACITY_ARTIFACTS must be positive: %q", raw)
		}
		count = value
	}
	deadline := 30 * time.Second
	if raw := os.Getenv("REPOSENSE_GRAPH_CAPACITY_MAX_DURATION"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			t.Fatalf("REPOSENSE_GRAPH_CAPACITY_MAX_DURATION must be positive: %q", raw)
		}
		deadline = value
	}
	now := time.Now().UTC()
	scope := common.Scope{TenantID: "capacity", RepositoryID: "capacity", SnapshotID: "capacity", TraceID: "capacity"}
	source := &capacitySource{metadata: graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-v1", ArtifactCount: count, ParseResultChecksum: "checksum", SchemaVersion: "parser-schema-v1", CompletedAt: now}, pageSize: 500}
	reader, err := graphapp.NewSourceReader(source, source.pageSize, []string{"parser-schema-v1"})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &capacityConsumer{}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	if _, err := reader.Read(ctx, scope, consumer); err != nil {
		t.Fatal(err)
	}
	duration := time.Since(started)
	if consumer.artifacts != count || consumer.maxPage > source.pageSize || source.maxRequested > source.pageSize || source.artifactCursor != int(count) {
		t.Fatalf("unbounded or incomplete capacity run: artifacts=%d max_page=%d max_requested=%d cursor=%d", consumer.artifacts, consumer.maxPage, source.maxRequested, source.artifactCursor)
	}
	t.Logf(`{"artifacts":%d,"page_size":%d,"duration_ms":%d}`, count, source.pageSize, duration.Milliseconds())
}
