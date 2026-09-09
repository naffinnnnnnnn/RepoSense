package graphapp

import (
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
)

func newPublishedEvent(revision graph.Revision, eventID string, occurredAt time.Time) common.EventEnvelope {
	quality := revision.Quality
	resolutionCounts := copyPublishedCounts(quality.ResolutionCounts)
	errorCounts := copyPublishedCounts(quality.ErrorCounts)
	errorSamples := append([]graph.QualitySample{}, quality.ErrorSamples...)
	return common.EventEnvelope{
		EventID: eventID, EventType: "graph.published.v1", AggregateID: revision.RevisionID,
		OccurredAt: occurredAt.UTC(), Producer: "code-knowledge-graph", PayloadVersion: 1, TraceID: revision.TraceID,
		Payload: map[string]any{
			"tenant_id": revision.TenantID, "repository_id": revision.RepositoryID,
			"snapshot_id": revision.SnapshotID, "commit_sha": revision.CommitSHA,
			"revision_id": revision.RevisionID, "event_id": eventID,
			"parser_result_version": revision.ParserResultVersion,
			"graph_schema_version":  revision.GraphSchemaVersion,
			"algorithm_version":     revision.AlgorithmVersion,
			"build_policy_version":  revision.BuildPolicyVersion,
			"quality_status":        string(revision.QualityStatus),
			"nodes":                 revision.Stats.Nodes, "edges": revision.Stats.Edges,
			"unresolved_targets":  revision.Stats.UnresolvedTargets,
			"ambiguous_relations": revision.Stats.AmbiguousRelations,
			"input_artifacts":     quality.InputArtifacts, "written_artifacts": quality.WrittenArtifacts,
			"invalid_artifacts": quality.InvalidArtifacts, "input_relations": quality.InputRelations,
			"written_relations": quality.WrittenRelations, "invalid_relations": quality.InvalidRelations,
			"resolution_counts": resolutionCounts, "error_counts": errorCounts, "error_samples": errorSamples,
			"trace_id": revision.TraceID,
		},
	}
}

func copyPublishedCounts(source map[string]int64) map[string]int64 {
	result := make(map[string]int64, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
