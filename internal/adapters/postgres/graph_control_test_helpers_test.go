package postgres

import (
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
)

func graphControlTestJob(jobID, key, fingerprint, eventID string, now time.Time) graph.BuildJob {
	return graph.BuildJob{
		JobID: jobID, Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"},
		IdempotencyKey: key, RequestFingerprint: fingerprint, CommitSHA: "commit",
		Versions: graph.BuildVersions{ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1"},
		Status:   graph.JobPending, EventID: eventID, CreatedAt: now, UpdatedAt: now,
	}
}

func graphControlTestRevision(job graph.BuildJob, attempt graph.BuildAttempt, now time.Time) graph.Revision {
	return graph.Revision{
		EntityMeta: graph.NewMeta(attempt.RevisionID, job.Scope, graph.RevisionActive, now), RevisionID: attempt.RevisionID,
		SnapshotID: job.Scope.SnapshotID, CommitSHA: job.CommitSHA, BuildMode: graph.BuildFull, BuildStatus: graph.RevisionActive,
		AlgorithmVersion: job.Versions.GraphAlgorithmVersion, ParserResultVersion: job.Versions.ParserResultVersion,
		GraphSchemaVersion: job.Versions.GraphSchemaVersion, BuildPolicyVersion: job.Versions.BuildPolicyVersion,
		QualityStatus: graph.QualityHealthy, RequestFingerprint: job.RequestFingerprint,
		Quality: graph.QualityStats{ResolutionCounts: map[string]int64{}, ErrorCounts: map[string]int64{}},
	}
}

func graphControlTestEvent(job graph.BuildJob, revision graph.Revision, now time.Time) common.EventEnvelope {
	return common.EventEnvelope{EventID: job.EventID, EventType: "graph.published.v1", AggregateID: revision.RevisionID,
		OccurredAt: now, Producer: "reposense-graph-worker", PayloadVersion: 1, TraceID: job.Scope.TraceID,
		Payload: map[string]any{"revision_id": revision.RevisionID, "snapshot_id": job.Scope.SnapshotID}}
}
