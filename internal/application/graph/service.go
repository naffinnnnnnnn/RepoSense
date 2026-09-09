package graphapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

const AlgorithmVersion = "parser-resolved@2"
const GraphSchemaVersion = "graph@2"
const BuildPolicyVersion = "strict@1"

type Config struct {
	MaxArtifacts int
	MaxRelations int
}

func DefaultConfig() Config { return Config{MaxArtifacts: 1_000_000, MaxRelations: 5_000_000} }

type Service struct {
	source   ports.GraphSource
	repo     ports.GraphRepository
	events   ports.EventPublisher
	observer ports.Observer
	ids      ports.IDGenerator
	clock    ports.Clock
	config   Config
	buildMu  sync.Mutex
}

// New retains the deprecated resolver argument for source compatibility with
// local composition. It is deliberately ignored: Parser is the only authority
// for relation targets.
func New(source ports.GraphSource, repo ports.GraphRepository, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, _ any, config Config) (*Service, error) {
	if source == nil || repo == nil {
		return nil, errors.New("graph source and repository must not be nil")
	}
	if events == nil {
		events = noopPublisher{}
	}
	if observer == nil {
		observer = noopObserver{}
	}
	if ids == nil {
		ids = randomIDs{}
	}
	if clock == nil {
		clock = systemClock{}
	}
	defaults := DefaultConfig()
	if config.MaxArtifacts <= 0 {
		config.MaxArtifacts = defaults.MaxArtifacts
	}
	if config.MaxRelations <= 0 {
		config.MaxRelations = defaults.MaxRelations
	}
	return &Service{source: source, repo: repo, events: events, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (s *Service) Build(ctx context.Context, cmd graph.BuildCommand) (revision graph.Revision, err error) {
	if cmd.Mode != graph.BuildFull {
		return revision, domainError(graph.ErrUnsupportedBuildMode, "validate", "graph builds only support FULL mode", false, nil)
	}
	if len(cmd.ArtifactIDs) != 0 {
		return revision, domainError(graph.ErrPartialBuildNotAllowed, "validate", "partial graph builds are not supported", false, nil)
	}
	if validateErr := cmd.Validate(); validateErr != nil {
		return revision, domainError(graph.ErrInvalidInput, "validate", validateErr.Error(), false, validateErr)
	}
	ctx, finish := startGraphStage(s.observer, ctx, "graph_build", labels(cmd.Scope))
	defer func() { finish(err) }()
	input, err := s.source.GraphInput(ctx, cmd.Scope)
	if err != nil {
		var graphErr *graph.DomainError
		if errors.As(err, &graphErr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return revision, err
		}
		return revision, domainError(graph.ErrSnapshotNotFound, "load_parse_result", "snapshot parse result not found", false, err)
	}
	if validateErr := validateBuildInputIdentity(cmd.Scope, input); validateErr != nil {
		return revision, validateErr
	}
	if len(input.Artifacts) > s.config.MaxArtifacts || len(input.Relations) > s.config.MaxRelations {
		return revision, domainError(graph.ErrInvalidInput, "validate_size", "graph build input exceeds configured limits", false, nil)
	}
	fingerprint, fingerprintErr := graph.RequestFingerprint(graph.FingerprintInput{Scope: cmd.Scope, CommitSHA: input.Snapshot.CommitSHA, Versions: graph.BuildVersions{
		ParserResultVersion: "legacy-parser-result@1", GraphSchemaVersion: GraphSchemaVersion, GraphAlgorithmVersion: AlgorithmVersion, BuildPolicyVersion: BuildPolicyVersion,
	}})
	if fingerprintErr != nil {
		return revision, domainError(graph.ErrInvalidInput, "fingerprint", "graph request identity is invalid", false, fingerprintErr)
	}

	// The compatibility service serializes publication after source loading. The
	// production path uses the shared GraphBuildStore unique constraints instead.
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	if cached, ok, lookupErr := s.repo.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); lookupErr != nil {
		return revision, domainError(graph.ErrPersistence, "idempotency_lookup", "failed to check graph idempotency key", true, lookupErr)
	} else if ok {
		if cached.RequestFingerprint != fingerprint {
			return revision, domainError(graph.ErrConflict, "idempotency_lookup", "idempotency key is already bound to another graph request", false, nil)
		}
		s.observer.Count("graph_build_idempotency_hits_total", 1, labels(cmd.Scope))
		return cached, nil
	}
	now := s.clock.Now().UTC()
	revisionID := s.ids.New("gr")
	revision = graph.Revision{EntityMeta: graph.NewMeta(revisionID, cmd.Scope, graph.RevisionBuilding, now), RevisionID: revisionID,
		SnapshotID: cmd.Scope.SnapshotID, CommitSHA: input.Snapshot.CommitSHA, BuildMode: graph.BuildFull, BuildStatus: graph.RevisionBuilding, AlgorithmVersion: AlgorithmVersion,
		ParserResultVersion: "legacy-parser-result@1", GraphSchemaVersion: GraphSchemaVersion, BuildPolicyVersion: BuildPolicyVersion, QualityStatus: graph.QualityHealthy, RequestFingerprint: fingerprint}
	stats, buildErr := s.apply(ctx, &revision, input.Artifacts, input.Relations)
	if buildErr != nil {
		return graph.Revision{}, buildErr
	}
	revision.Stats.UnresolvedTargets, revision.Stats.AmbiguousRelations = stats.unresolved, stats.ambiguous
	revision.BuildStatus, revision.EntityMeta.Status, revision.UpdatedAt = graph.RevisionActive, string(graph.RevisionActive), s.clock.Now().UTC()
	revision.Normalize()
	revision.Quality = graph.QualityStats{InputArtifacts: int64(len(input.Artifacts)), WrittenArtifacts: int64(revision.Stats.Nodes - revision.Stats.UnresolvedTargets), InputRelations: int64(len(input.Relations)), WrittenRelations: int64(revision.Stats.Edges)}
	revision.PublishedEvent = newPublishedEvent(revision, s.ids.New("evt"), s.clock.Now().UTC())
	if err = s.repo.Save(ctx, cmd.IdempotencyKey, revision); err != nil {
		return graph.Revision{}, domainError(graph.ErrPersistence, "publish_revision", "failed to atomically publish graph revision", true, err)
	}
	canonical, ok, lookupErr := s.repo.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey)
	if lookupErr != nil || !ok {
		return graph.Revision{}, domainError(graph.ErrPersistence, "publish_revision", "saved graph revision could not be read back", true, lookupErr)
	}
	revision = canonical
	publishCtx := repository.WithEventScope(ctx, cmd.Scope)
	if err = s.events.Publish(publishCtx, revision.PublishedEvent); err != nil {
		return revision, domainError(graph.ErrPersistence, "publish_event", "graph revision was saved but event publication failed", true, err)
	}
	s.observer.Count("graph_build_nodes_total", int64(revision.Stats.Nodes), labels(cmd.Scope))
	s.observer.Count("graph_build_edges_total", int64(revision.Stats.Edges), labels(cmd.Scope))
	s.observer.Count("graph_build_unresolved_total", int64(revision.Stats.UnresolvedTargets), labels(cmd.Scope))
	return revision, nil
}

func (s *Service) Query(ctx context.Context, q graph.Query) (result graph.Result, err error) {
	ctx, finish := startGraphStage(s.observer, ctx, "graph_query", labels(q.Scope))
	defer func() { finish(err) }()
	result, err = s.repo.Query(ctx, q)
	if err != nil {
		return graph.Result{}, err
	}
	s.observer.Count("graph_query_results_total", int64(len(result.Nodes)), labels(q.Scope))
	return result, nil
}

type buildStats struct{ unresolved, ambiguous int }

func validateBuildInputIdentity(scope common.Scope, input graph.BuildInput) error {
	if input.Snapshot.TenantID != scope.TenantID || input.Snapshot.RepositoryID != scope.RepositoryID || input.Snapshot.SnapshotID != scope.SnapshotID {
		return domainError(graph.ErrInvalidInput, "validate_snapshot", "parser snapshot scope does not match graph request", false, nil)
	}
	if input.Snapshot.SyncStatus != repository.StatusSucceeded || strings.TrimSpace(input.Snapshot.CommitSHA) == "" {
		return domainError(graph.ErrInvalidInput, "validate_snapshot", "only a complete succeeded snapshot can be graphed", false, nil)
	}
	return nil
}

func validArtifactKind(kind repository.ArtifactKind) bool {
	switch kind {
	case repository.ArtifactFile, repository.ArtifactModule, repository.ArtifactClass, repository.ArtifactInterface, repository.ArtifactFunction, repository.ArtifactMethod, repository.ArtifactImport, repository.ArtifactConfig, repository.ArtifactDocument:
		return true
	default:
		return false
	}
}

func validRelationKind(kind repository.RelationKind) bool {
	switch kind {
	case repository.RelationContains, repository.RelationImports, repository.RelationCalls, repository.RelationExtends, repository.RelationImplements, repository.RelationDependsOn:
		return true
	default:
		return false
	}
}

func (s *Service) apply(ctx context.Context, revision *graph.Revision, artifacts []repository.CodeArtifact, relations []repository.CodeRelation) (buildStats, error) {
	nodes := map[string]graph.Entity{}
	edges := map[string]graph.Relation{}
	artifactMap := map[string]repository.CodeArtifact{}
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return buildStats{}, err
		}
		if strings.TrimSpace(artifact.ArtifactID) == "" || strings.TrimSpace(artifact.Name) == "" || !validArtifactKind(artifact.Kind) {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", "artifact id and name are required", false, nil)
		}
		if _, duplicate := artifactMap[artifact.ArtifactID]; duplicate {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", "duplicate artifact id", false, nil)
		}
		if err := artifact.SourceRef.Validate(); err != nil {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", err.Error(), false, err)
		}
		if artifact.SourceRef.CommitSHA != revision.CommitSHA || artifact.SourceRef.SymbolID != "" && artifact.SourceRef.SymbolID != artifact.ArtifactID {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", "artifact source identity does not match snapshot", false, nil)
		}
		artifactMap[artifact.ArtifactID] = artifact
		ref := artifact.SourceRef
		nodes[nodeID(artifact.ArtifactID)] = graph.Entity{NodeID: nodeID(artifact.ArtifactID), EntityType: graph.EntityTypeFor(artifact.Kind), ArtifactID: artifact.ArtifactID, Name: artifact.Name, QualifiedName: artifact.QualifiedName, Properties: cloneStrings(artifact.Attributes), SourceRef: &ref, ValidFrom: revision.CommitSHA}
	}
	stats := buildStats{}
	seenRelations := map[string]struct{}{}
	for _, relation := range relations {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if strings.TrimSpace(relation.RelationID) == "" || strings.TrimSpace(relation.From) == "" || strings.TrimSpace(relation.To) == "" || !validRelationKind(relation.Kind) {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "relation identity, endpoints and kind are required", false, nil)
		}
		if _, duplicate := seenRelations[relation.RelationID]; duplicate {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "duplicate relation id", false, nil)
		}
		seenRelations[relation.RelationID] = struct{}{}
		if _, exists := artifactMap[relation.From]; !exists {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "relation source artifact does not exist", false, nil)
		}
		if math.IsNaN(relation.Confidence) || math.IsInf(relation.Confidence, 0) || relation.Confidence < 0 || relation.Confidence > 1 {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "relation confidence must be between 0 and 1", false, nil)
		}
		if err := relation.Evidence.Validate(); err != nil {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", err.Error(), false, err)
		}
		if relation.Evidence.CommitSHA != revision.CommitSHA {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "relation evidence does not match snapshot", false, nil)
		}
		toID := ""
		properties := map[string]string{}
		if _, resolved := artifactMap[relation.To]; resolved {
			toID = nodeID(relation.To)
		} else {
			stats.unresolved++
			toID = issueNodeID(relation.RelationID)
			if _, exists := nodes[toID]; !exists {
				nodes[toID] = graph.Entity{NodeID: toID, EntityType: graph.EntitySymbol, Name: relation.To, QualifiedName: relation.To, Properties: map[string]string{"resolution": string(graph.ResolutionUnresolved), "relation_id": relation.RelationID}, ValidFrom: revision.CommitSHA}
			}
			properties["resolution"] = string(graph.ResolutionUnresolved)
			properties["intended_kind"] = string(relation.Kind)
		}
		edgeID := edgeID(relation.RelationID, nodeID(relation.From), toID)
		relationType := relation.Kind
		if properties["resolution"] != "" {
			relationType = repository.RelationKind("UNCERTAIN_RELATION")
		}
		edges[edgeID] = graph.Relation{EdgeID: edgeID, RelationType: relationType, FromNodeID: nodeID(relation.From), ToNodeID: toID, Evidence: relation.Evidence, Confidence: relation.Confidence, Properties: properties}
	}
	revision.Nodes = revision.Nodes[:0]
	for _, n := range nodes {
		revision.Nodes = append(revision.Nodes, n)
	}
	revision.Edges = revision.Edges[:0]
	connected := map[string]bool{}
	for _, e := range edges {
		if _, a := nodes[e.FromNodeID]; a {
			if _, b := nodes[e.ToNodeID]; b {
				revision.Edges = append(revision.Edges, e)
				connected[e.FromNodeID], connected[e.ToNodeID] = true, true
			}
		}
	}
	revision.Nodes = revision.Nodes[:0]
	for _, n := range nodes {
		if n.ArtifactID != "" || connected[n.NodeID] {
			revision.Nodes = append(revision.Nodes, n)
		}
	}
	stats = buildStats{}
	for _, e := range revision.Edges {
		switch e.Properties["resolution"] {
		case string(graph.ResolutionUnresolved):
			stats.unresolved++
		case string(graph.ResolutionAmbiguous):
			stats.unresolved++
			stats.ambiguous++
		}
	}
	return stats, nil
}

func digest(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:24]
}
func nodeID(id string) string              { return "node_" + digest("artifact", id) }
func issueNodeID(relationID string) string { return "issue_" + digest("resolution", relationID) }
func edgeID(id, from, to string) string    { return "edge_" + digest(id, from, to) }
func cloneStrings(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func labels(s common.Scope) map[string]string {
	return map[string]string{"operation": "legacy", "tenant_id": s.TenantID, "repository_id": s.RepositoryID, "snapshot_id": s.SnapshotID, "trace_id": s.TraceID}
}
func domainError(code graph.ErrorCode, op, msg string, retry bool, cause error) *graph.DomainError {
	return &graph.DomainError{Code: code, Operation: op, Stage: op, Message: msg, Retryable: retry, Cause: cause}
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, common.EventEnvelope) error { return nil }

type noopObserver struct{}

func (noopObserver) Stage(context.Context, string, map[string]string) func(error) {
	return func(error) {}
}
func (noopObserver) Count(string, int64, map[string]string) {}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type randomIDs struct{}

func (randomIDs) New(prefix string) string {
	var b [12]byte
	if _, e := rand.Read(b[:]); e != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
