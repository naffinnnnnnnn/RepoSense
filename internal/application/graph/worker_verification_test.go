package graphapp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type verificationClock struct{ now time.Time }

func (c verificationClock) Now() time.Time { return c.now }

type verificationIDs struct {
	mu   sync.Mutex
	next int
}

func (g *verificationIDs) New(prefix string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return prefix + "-verify-" + string(rune('a'+g.next))
}

type verificationSource struct {
	metadata      graph.SnapshotMetadata
	artifacts     []repository.CodeArtifact
	relations     []graph.ResolvedRelation
	artifactReads int
	relationReads int
}

func (s *verificationSource) SnapshotMetadata(context.Context, common.Scope) (graph.SnapshotMetadata, error) {
	return s.metadata, nil
}
func (s *verificationSource) ArtifactPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.ArtifactPage, error) {
	s.artifactReads++
	return graph.ArtifactPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor}, Artifacts: s.artifacts}, nil
}
func (s *verificationSource) RelationPage(_ context.Context, _ common.Scope, cursor string, _ int) (graph.RelationPage, error) {
	s.relationReads++
	return graph.RelationPage{PageIdentity: graph.PageIdentity{Scope: s.metadata.Scope, CommitSHA: s.metadata.CommitSHA, ParserResultVersion: s.metadata.ParserResultVersion, ParseResultChecksum: s.metadata.ParseResultChecksum, Cursor: cursor}, Relations: s.relations}, nil
}

type verificationBuildStore struct {
	requested graph.BuildJob
	claimed   bool
	heartbeat error
	failed    []graph.DomainError
	activated []graph.Revision
	events    []common.EventEnvelope
	sequence  *[]string
}

func (s *verificationBuildStore) EnqueueGraphBuild(context.Context, graph.BuildJob) (graph.BuildJob, bool, error) {
	return graph.BuildJob{}, false, errors.New("unexpected enqueue")
}
func (s *verificationBuildStore) GraphJob(context.Context, string) (graph.BuildJob, error) {
	return s.requested, nil
}
func (s *verificationBuildStore) ClaimGraphBuild(_ context.Context, owner, attemptID, revisionID string, now time.Time, lease time.Duration) (graph.BuildJob, graph.BuildAttempt, bool, error) {
	if s.claimed {
		return graph.BuildJob{}, graph.BuildAttempt{}, false, nil
	}
	s.claimed = true
	job := s.requested
	job.Status, job.RevisionID, job.UpdatedAt = graph.JobBuilding, revisionID, now
	return job, graph.BuildAttempt{AttemptID: attemptID, JobID: job.JobID, RevisionID: revisionID, Status: graph.AttemptRunning, LeaseOwner: owner, LeaseExpiresAt: now.Add(lease), Fence: 1, CreatedAt: now, UpdatedAt: now}, true, nil
}
func (s *verificationBuildStore) HeartbeatGraphBuild(context.Context, string, string, string, int64, time.Time) error {
	return s.heartbeat
}
func (s *verificationBuildStore) FailGraphAttempt(_ context.Context, _ graph.BuildJob, _ graph.BuildAttempt, failure graph.DomainError, _ time.Time) error {
	s.failed = append(s.failed, failure)
	return nil
}
func (s *verificationBuildStore) ActivateGraphRevision(_ context.Context, _ graph.BuildJob, _ graph.BuildAttempt, revision graph.Revision, event common.EventEnvelope, _ time.Time) error {
	if s.sequence != nil {
		*s.sequence = append(*s.sequence, "activate")
	}
	s.activated = append(s.activated, revision)
	s.events = append(s.events, event)
	return nil
}
func (s *verificationBuildStore) ActiveGraphRevision(context.Context, common.Scope) (graph.Revision, error) {
	return graph.Revision{}, errors.New("unexpected active lookup")
}

type verificationDataStore struct {
	sequence              *[]string
	artifactCalls         int
	temporaryArtifactFail bool
	created               int
	sealed                int
}

func (s *verificationDataStore) CreateCandidate(context.Context, graph.BuildJob, graph.BuildAttempt) error {
	s.created++
	*s.sequence = append(*s.sequence, "candidate")
	return nil
}
func (s *verificationDataStore) WriteArtifactBatch(_ context.Context, _ graph.BuildJob, _ graph.BuildAttempt, artifacts []repository.CodeArtifact) error {
	s.artifactCalls++
	if s.temporaryArtifactFail {
		s.temporaryArtifactFail = false
		return &graph.DomainError{Code: graph.ErrGraphStoreUnavailable, Retryable: true, Message: "temporary"}
	}
	for _, artifact := range artifacts {
		*s.sequence = append(*s.sequence, "artifact:"+artifact.ArtifactID)
	}
	return nil
}
func (s *verificationDataStore) CompleteArtifactStage(_ context.Context, _ graph.BuildJob, _ graph.BuildAttempt, count int) error {
	*s.sequence = append(*s.sequence, "artifacts-complete:"+string(rune('0'+count)))
	return nil
}
func (s *verificationDataStore) WriteRelationBatch(_ context.Context, _ graph.BuildJob, _ graph.BuildAttempt, relations []graph.ResolvedRelation) error {
	for _, relation := range relations {
		*s.sequence = append(*s.sequence, "relation:"+relation.RelationID)
	}
	return nil
}
func (s *verificationDataStore) SealCandidate(context.Context, graph.BuildJob, graph.BuildAttempt, graph.RevisionStats, graph.QualityStatus) error {
	s.sealed++
	*s.sequence = append(*s.sequence, "seal")
	return nil
}
func (*verificationDataStore) CandidateStatus(context.Context, common.Scope, string) (graph.CandidateStatus, error) {
	return graph.CandidateStaging, nil
}
func (*verificationDataStore) QueryRevision(context.Context, string, graph.Query) (graph.Result, error) {
	return graph.Result{}, errors.New("unexpected query")
}
func (*verificationDataStore) DeleteCandidate(context.Context, common.Scope, string, int64) error {
	return errors.New("unexpected delete")
}

func TestWorkerProductionPipelineIsOrderedRetriedAndPublishesThroughOutboxOnly(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	ref := func(path, id string) common.SourceRef {
		return common.SourceRef{CommitSHA: "commit", Path: path, SymbolID: id, StartLine: 1, EndLine: 1, ContentHash: "hash"}
	}
	source := &verificationSource{metadata: graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-v1", ArtifactCount: 2, RelationCount: 1, ParseResultChecksum: "checksum", SchemaVersion: "parser-schema-v1", CompletedAt: now},
		artifacts: []repository.CodeArtifact{{ArtifactID: "a", Kind: repository.ArtifactFunction, Name: "A", SourceRef: ref("a.go", "a"), ContentHash: "ha"}, {ArtifactID: "b", Kind: repository.ArtifactFunction, Name: "B", SourceRef: ref("b.go", "b"), ContentHash: "hb"}},
		relations: []graph.ResolvedRelation{{RelationID: "r", Kind: repository.RelationCalls, FromArtifactID: "a", TargetArtifactID: "b", ResolutionStatus: graph.ResolutionResolved, Evidence: ref("a.go", "r"), Confidence: 1}}}
	reader, err := NewSourceReader(source, 1_000, []string{"parser-schema-v1"})
	if err != nil {
		t.Fatal(err)
	}
	sequence := []string{}
	builds := &verificationBuildStore{requested: verificationJob(now, scope), sequence: &sequence}
	data := &verificationDataStore{sequence: &sequence, temporaryArtifactFail: true}
	config := verificationWorkerConfig()
	worker, err := NewWorker(reader, builds, data, nil, &verificationIDs{}, verificationClock{now}, config)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background(), "worker-1")
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if got := strings.Join(sequence, ","); got != "candidate,artifact:a,artifact:b,artifacts-complete:2,relation:r,seal,activate" {
		t.Fatalf("pipeline order=%s", got)
	}
	if data.artifactCalls != 3 {
		t.Fatalf("first artifact batch should retry once before the second batch, calls=%d", data.artifactCalls)
	}
	if len(builds.activated) != 1 || len(builds.events) != 1 || builds.events[0].EventType != "graph.published.v1" {
		t.Fatalf("activation/outbox event mismatch: revisions=%d events=%#v", len(builds.activated), builds.events)
	}
	if builds.activated[0].Stats.Nodes != 2 || builds.activated[0].Stats.Edges != 1 || builds.activated[0].QualityStatus != graph.QualityHealthy {
		t.Fatalf("revision statistics mismatch: %#v", builds.activated[0])
	}
}

func TestWorkerRejectsCapacityBeforeCandidateAndStopsOnLeaseLoss(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	scope := common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snapshot", TraceID: "trace"}
	for _, test := range []struct {
		name      string
		metadata  graph.SnapshotMetadata
		heartbeat error
		code      graph.ErrorCode
	}{
		{name: "capacity", metadata: graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-v1", ArtifactCount: 2, ParseResultChecksum: "checksum", SchemaVersion: "parser-schema-v1", CompletedAt: now}, code: graph.ErrBuildCapacityExceeded},
		{name: "lease", metadata: graph.SnapshotMetadata{Scope: scope, CommitSHA: "commit", ParserResultVersion: "parser-v1", ParseResultChecksum: "checksum", SchemaVersion: "parser-schema-v1", CompletedAt: now}, heartbeat: &graph.DomainError{Code: graph.ErrLeaseLost, Message: "lost"}, code: graph.ErrLeaseLost},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &verificationSource{metadata: test.metadata}
			reader, _ := NewSourceReader(source, 100, []string{"parser-schema-v1"})
			sequence := []string{}
			builds := &verificationBuildStore{requested: verificationJob(now, scope), heartbeat: test.heartbeat}
			data := &verificationDataStore{sequence: &sequence}
			config := verificationWorkerConfig()
			config.MaxArtifacts = 1
			worker, _ := NewWorker(reader, builds, data, nil, &verificationIDs{}, verificationClock{now}, config)
			processed, err := worker.RunOnce(context.Background(), "worker-1")
			if !processed || !graph.IsCode(err, test.code) {
				t.Fatalf("processed=%v err=%v", processed, err)
			}
			if data.created != 0 || source.artifactReads != 0 || len(builds.activated) != 0 {
				t.Fatalf("failure crossed expensive/publication boundary: data=%#v source=%#v", data, source)
			}
		})
	}
}

func verificationJob(now time.Time, scope common.Scope) graph.BuildJob {
	return graph.BuildJob{JobID: "job", Scope: scope, IdempotencyKey: "key", RequestFingerprint: strings.Repeat("a", 64), CommitSHA: "commit",
		Versions: graph.BuildVersions{ParserResultVersion: "parser-v1", GraphSchemaVersion: "graph-v1", GraphAlgorithmVersion: "algorithm-v1", BuildPolicyVersion: "policy-v1"},
		Status:   graph.JobPending, EventID: "event", CreatedAt: now, UpdatedAt: now}
}

func verificationWorkerConfig() WorkerConfig {
	config := DefaultWorkerConfig()
	config.LeaseDuration, config.HeartbeatInterval = time.Second, 100*time.Millisecond
	config.BuildTimeout, config.ControlTimeout = 2*time.Second, time.Second
	config.BatchSize, config.MaxArtifacts, config.MaxRelations = 1, 10, 10
	config.SourceAttempts, config.BatchAttempts = 1, 2
	config.RetryInitialBackoff, config.RetryMaxBackoff = time.Millisecond, 2*time.Millisecond
	config.MaxConcurrentGlobal, config.MaxConcurrentTenant = 1, 1
	return config
}
