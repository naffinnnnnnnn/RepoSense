package graphapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

const AlgorithmVersion = "symbol-resolver@1"

type Config struct {
	MaxArtifacts int
	MaxRelations int
}

func DefaultConfig() Config { return Config{MaxArtifacts: 1_000_000, MaxRelations: 5_000_000} }

// Resolver is deliberately replaceable: later AST/LSP/embedding based symbol
// disambiguation can be introduced without changing graph persistence or APIs.
type Resolver interface {
	Resolve(target string, source repository.CodeArtifact, artifacts []repository.CodeArtifact) (artifactID string, ambiguous bool)
}

type Service struct {
	source   ports.GraphSource
	repo     ports.GraphRepository
	events   ports.EventPublisher
	observer ports.Observer
	ids      ports.IDGenerator
	clock    ports.Clock
	resolver Resolver
	config   Config
}

func New(source ports.GraphSource, repo ports.GraphRepository, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, resolver Resolver, config Config) (*Service, error) {
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
	if resolver == nil {
		resolver = NameResolver{}
	}
	defaults := DefaultConfig()
	if config.MaxArtifacts <= 0 {
		config.MaxArtifacts = defaults.MaxArtifacts
	}
	if config.MaxRelations <= 0 {
		config.MaxRelations = defaults.MaxRelations
	}
	return &Service{source: source, repo: repo, events: events, observer: observer, ids: ids, clock: clock, resolver: resolver, config: config}, nil
}

func (s *Service) Build(ctx context.Context, cmd graph.BuildCommand) (revision graph.Revision, err error) {
	if err := cmd.Validate(); err != nil {
		return revision, domainError(graph.ErrInvalidInput, "validate", err.Error(), false, err)
	}
	if cached, ok, lookupErr := s.repo.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); lookupErr != nil {
		return revision, domainError(graph.ErrPersistence, "idempotency_lookup", "failed to check graph idempotency key", true, lookupErr)
	} else if ok {
		s.observer.Count("graph_build_idempotency_hits_total", 1, labels(cmd.Scope))
		if publishErr := s.events.Publish(ctx, cached.PublishedEvent); publishErr != nil {
			return cached, domainError(graph.ErrPersistence, "publish_event", "failed to republish stored graph event", true, publishErr)
		}
		return cached, nil
	}
	finish := s.observer.Stage(ctx, "graph_build", labels(cmd.Scope))
	defer func() { finish(err) }()
	input, err := s.source.GraphInput(ctx, cmd.Scope)
	if err != nil {
		return revision, domainError(graph.ErrSnapshotNotFound, "load_parse_result", "snapshot parse result not found", false, err)
	}
	if input.Snapshot.SyncStatus != repository.StatusSucceeded {
		return revision, domainError(graph.ErrInvalidInput, "validate_snapshot", "only a succeeded snapshot can be graphed", false, nil)
	}
	if len(input.Artifacts) > s.config.MaxArtifacts || len(input.Relations) > s.config.MaxRelations {
		return revision, domainError(graph.ErrInvalidInput, "validate_size", "graph build input exceeds configured limits", false, nil)
	}
	artifacts, err := selectArtifacts(input.Artifacts, cmd.ArtifactIDs)
	if err != nil {
		return revision, domainError(graph.ErrInvalidInput, "select_artifacts", err.Error(), false, err)
	}
	now := s.clock.Now().UTC()
	revisionID := s.ids.New("gr")
	revision = graph.Revision{EntityMeta: graph.NewMeta(revisionID, cmd.Scope, graph.RevisionBuilding, now), RevisionID: revisionID,
		SnapshotID: cmd.Scope.SnapshotID, CommitSHA: input.Snapshot.CommitSHA, BuildMode: cmd.Mode, BuildStatus: graph.RevisionBuilding, AlgorithmVersion: AlgorithmVersion}
	if cmd.Mode == graph.BuildIncremental {
		if input.Snapshot.ParentSnapshotID == "" {
			return graph.Revision{}, domainError(graph.ErrInvalidInput, "load_parent", "incremental build requires a parent snapshot", false, nil)
		}
		parentScope := cmd.Scope
		parentScope.SnapshotID = input.Snapshot.ParentSnapshotID
		parent, parentErr := s.repo.RevisionBySnapshot(ctx, parentScope)
		if parentErr != nil {
			return graph.Revision{}, domainError(graph.ErrRevisionNotFound, "load_parent", "parent graph revision not found", false, parentErr)
		}
		revision.ParentRevisionID = parent.RevisionID
		revision.Nodes, revision.Edges = carryForward(parent, changedPathSet(input))
	}
	stats, buildErr := s.apply(ctx, &revision, artifacts, input.Relations)
	if buildErr != nil {
		return graph.Revision{}, buildErr
	}
	revision.Stats.UnresolvedTargets, revision.Stats.AmbiguousRelations = stats.unresolved, stats.ambiguous
	revision.BuildStatus, revision.EntityMeta.Status, revision.UpdatedAt = graph.RevisionActive, string(graph.RevisionActive), s.clock.Now().UTC()
	revision.Normalize()
	revision.PublishedEvent = common.EventEnvelope{EventID: s.ids.New("evt"), EventType: "graph.published.v1", AggregateID: revision.RevisionID, OccurredAt: s.clock.Now().UTC(), Producer: "code-knowledge-graph", PayloadVersion: 1, TraceID: cmd.Scope.TraceID,
		Payload: map[string]any{"revision_id": revision.RevisionID, "snapshot_id": revision.SnapshotID, "commit_sha": revision.CommitSHA, "nodes": revision.Stats.Nodes, "edges": revision.Stats.Edges, "unresolved_targets": revision.Stats.UnresolvedTargets, "algorithm_version": revision.AlgorithmVersion}}
	if err = s.repo.Save(ctx, cmd.IdempotencyKey, revision); err != nil {
		return graph.Revision{}, domainError(graph.ErrPersistence, "publish_revision", "failed to atomically publish graph revision", true, err)
	}
	if err = s.events.Publish(ctx, revision.PublishedEvent); err != nil {
		return revision, domainError(graph.ErrPersistence, "publish_event", "graph revision was saved but event publication failed", true, err)
	}
	s.observer.Count("graph_build_nodes_total", int64(revision.Stats.Nodes), labels(cmd.Scope))
	s.observer.Count("graph_build_edges_total", int64(revision.Stats.Edges), labels(cmd.Scope))
	s.observer.Count("graph_build_unresolved_total", int64(revision.Stats.UnresolvedTargets), labels(cmd.Scope))
	return revision, nil
}

func (s *Service) Query(ctx context.Context, q graph.Query) (result graph.Result, err error) {
	finish := s.observer.Stage(ctx, "graph_query", labels(q.Scope))
	defer func() { finish(err) }()
	result, err = s.repo.Query(ctx, q)
	if err != nil {
		return graph.Result{}, err
	}
	s.observer.Count("graph_query_results_total", int64(len(result.Nodes)), labels(q.Scope))
	return result, nil
}

type buildStats struct{ unresolved, ambiguous int }

func (s *Service) apply(ctx context.Context, revision *graph.Revision, artifacts []repository.CodeArtifact, relations []repository.CodeRelation) (buildStats, error) {
	nodes := map[string]graph.Entity{}
	candidates := append([]repository.CodeArtifact(nil), artifacts...)
	for _, n := range revision.Nodes {
		nodes[n.NodeID] = n
		if n.ArtifactID != "" && n.SourceRef != nil {
			candidates = append(candidates, repository.CodeArtifact{ArtifactID: n.ArtifactID, Kind: repository.ArtifactKind(n.EntityType), Name: n.Name, QualifiedName: n.QualifiedName, SourceRef: *n.SourceRef})
		}
	}
	edges := map[string]graph.Relation{}
	for _, e := range revision.Edges {
		edges[e.EdgeID] = e
	}
	artifactMap := map[string]repository.CodeArtifact{}
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return buildStats{}, err
		}
		if artifact.ArtifactID == "" || artifact.Name == "" {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", "artifact id and name are required", false, nil)
		}
		if err := artifact.SourceRef.Validate(); err != nil {
			return buildStats{}, domainError(graph.ErrBuildFailure, "validate_artifact", err.Error(), false, err)
		}
		artifactMap[artifact.ArtifactID] = artifact
		ref := artifact.SourceRef
		nodes[nodeID(artifact.ArtifactID)] = graph.Entity{NodeID: nodeID(artifact.ArtifactID), EntityType: graph.EntityTypeFor(artifact.Kind), ArtifactID: artifact.ArtifactID, Name: artifact.Name, QualifiedName: artifact.QualifiedName, Properties: cloneStrings(artifact.Attributes), SourceRef: &ref, ValidFrom: revision.CommitSHA}
	}
	stats := buildStats{}
	for _, relation := range relations {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		fromArtifact, fromInSelection := artifactMap[relation.From]
		if !fromInSelection {
			continue
		}
		if relation.Confidence < 0 || relation.Confidence > 1 {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", "relation confidence must be between 0 and 1", false, nil)
		}
		if err := relation.Evidence.Validate(); err != nil {
			return stats, domainError(graph.ErrBuildFailure, "validate_relation", err.Error(), false, err)
		}
		toArtifactID, ambiguous := s.resolver.Resolve(relation.To, fromArtifact, candidates)
		if ambiguous {
			stats.ambiguous++
		}
		toID := ""
		properties := map[string]string{}
		if toArtifactID != "" {
			toID = nodeID(toArtifactID)
		} else {
			stats.unresolved++
			toID = externalNodeID(relation.To)
			typ, name := externalTypeAndName(relation.To)
			if _, exists := nodes[toID]; !exists {
				nodes[toID] = graph.Entity{NodeID: toID, EntityType: typ, Name: name, QualifiedName: relation.To, Properties: map[string]string{"resolution": "unresolved"}, ValidFrom: revision.CommitSHA}
			}
			properties["resolution"] = "unresolved"
			if ambiguous {
				properties["resolution"] = "ambiguous"
			}
		}
		edgeID := edgeID(relation.RelationID, nodeID(relation.From), toID)
		edges[edgeID] = graph.Relation{EdgeID: edgeID, RelationType: relation.Kind, FromNodeID: nodeID(relation.From), ToNodeID: toID, Evidence: relation.Evidence, Confidence: relation.Confidence, Properties: properties}
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
		case "unresolved":
			stats.unresolved++
		case "ambiguous":
			stats.unresolved++
			stats.ambiguous++
		}
	}
	return stats, nil
}

// NameResolver applies deterministic local heuristics. Ambiguous targets remain
// explicit external nodes rather than silently linking to the wrong symbol.
type NameResolver struct{}

func (NameResolver) Resolve(target string, source repository.CodeArtifact, artifacts []repository.CodeArtifact) (string, bool) {
	for _, a := range artifacts {
		if a.ArtifactID == target {
			return a.ArtifactID, false
		}
	}
	prefix, name := splitTarget(target)
	candidates := []repository.CodeArtifact{}
	for _, a := range artifacts {
		matched := a.Name == name || a.QualifiedName == name || strings.HasSuffix(a.QualifiedName, "."+name)
		if prefix == "module" {
			normalized := strings.TrimSuffix(filepath.ToSlash(a.SourceRef.Path), filepath.Ext(a.SourceRef.Path))
			matched = matched || normalized == strings.ReplaceAll(name, ".", "/")
		}
		if matched {
			candidates = append(candidates, a)
		}
	}
	if len(candidates) == 1 {
		return candidates[0].ArtifactID, false
	}
	if len(candidates) > 1 {
		local := []repository.CodeArtifact{}
		for _, a := range candidates {
			if a.SourceRef.Path == source.SourceRef.Path {
				local = append(local, a)
			}
		}
		if len(local) == 1 {
			return local[0].ArtifactID, false
		}
		return "", true
	}
	return "", false
}

func selectArtifacts(all []repository.CodeArtifact, ids []string) ([]repository.CodeArtifact, error) {
	if len(ids) == 0 {
		return all, nil
	}
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	out := []repository.CodeArtifact{}
	for _, a := range all {
		if wanted[a.ArtifactID] {
			out = append(out, a)
			delete(wanted, a.ArtifactID)
		}
	}
	if len(wanted) > 0 {
		missing := []string{}
		for id := range wanted {
			missing = append(missing, id)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("artifact ids not found: %s", strings.Join(missing, ", "))
	}
	return out, nil
}
func carryForward(parent graph.Revision, changed map[string]bool) ([]graph.Entity, []graph.Relation) {
	nodes := []graph.Entity{}
	kept := map[string]bool{}
	for _, n := range parent.Nodes {
		if n.SourceRef != nil && changed[filepath.ToSlash(n.SourceRef.Path)] {
			continue
		}
		nodes = append(nodes, n)
		kept[n.NodeID] = true
	}
	edges := []graph.Relation{}
	for _, e := range parent.Edges {
		// Keep edges whose evidence/source file did not change even when their
		// target is being overlaid. apply will retain the edge if the stable
		// target node is recreated, or prune it when that target was deleted.
		if kept[e.FromNodeID] && !changed[filepath.ToSlash(e.Evidence.Path)] {
			edges = append(edges, e)
		}
	}
	return nodes, edges
}
func changedPathSet(input graph.BuildInput) map[string]bool {
	out := map[string]bool{}
	for _, p := range input.DeletedPaths {
		out[filepath.ToSlash(p)] = true
	}
	for _, c := range input.Snapshot.ChangedPaths {
		out[filepath.ToSlash(c.Path)] = true
		if c.OldPath != "" {
			out[filepath.ToSlash(c.OldPath)] = true
		}
	}
	return out
}
func splitTarget(target string) (string, string) {
	if i := strings.Index(target, ":"); i >= 0 {
		return target[:i], target[i+1:]
	}
	return "", target
}
func externalTypeAndName(target string) (graph.EntityType, string) {
	prefix, name := splitTarget(target)
	switch prefix {
	case "module":
		return graph.EntityModule, name
	case "package":
		return graph.EntityPackage, name
	default:
		return graph.EntitySymbol, name
	}
}
func digest(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:24]
}
func nodeID(id string) string             { return "node_" + digest("artifact", id) }
func externalNodeID(target string) string { return "node_" + digest("external", target) }
func edgeID(id, from, to string) string   { return "edge_" + digest(id, from, to) }
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
	return map[string]string{"tenant_id": s.TenantID, "repository_id": s.RepositoryID, "snapshot_id": s.SnapshotID, "trace_id": s.TraceID}
}
func domainError(code graph.ErrorCode, op, msg string, retry bool, cause error) *graph.DomainError {
	return &graph.DomainError{Code: code, Operation: op, Message: msg, Retryable: retry, Cause: cause}
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
