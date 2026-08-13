package assistantapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type Config struct {
	RetrievalTopK   int
	MaxCitations    int
	MaxFileChanges  int
	MaxDiffBytes    int
	MaxArtifactScan int
}

func DefaultConfig() Config {
	return Config{RetrievalTopK: 20, MaxCitations: 50, MaxFileChanges: 20, MaxDiffBytes: 1 << 20, MaxArtifactScan: 100_000}
}

type Service struct {
	snapshots ports.RepositoryStore
	retriever ports.Retriever
	repo      ports.AssistantRepository
	generator ports.ProposalGenerator
	applier   ports.PatchApplier
	events    ports.EventPublisher
	observer  ports.Observer
	ids       ports.IDGenerator
	clock     ports.Clock
	config    Config
}

func New(snapshots ports.RepositoryStore, retriever ports.Retriever, repo ports.AssistantRepository, generator ports.ProposalGenerator, applier ports.PatchApplier, events ports.EventPublisher, observer ports.Observer, ids ports.IDGenerator, clock ports.Clock, config Config) (*Service, error) {
	if snapshots == nil || repo == nil || generator == nil {
		return nil, errors.New("snapshot store, assistant repository and proposal generator must not be nil")
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
	if config.RetrievalTopK <= 0 {
		config.RetrievalTopK = defaults.RetrievalTopK
	}
	if config.MaxCitations <= 0 {
		config.MaxCitations = defaults.MaxCitations
	}
	if config.MaxFileChanges <= 0 {
		config.MaxFileChanges = defaults.MaxFileChanges
	}
	if config.MaxDiffBytes <= 0 {
		config.MaxDiffBytes = defaults.MaxDiffBytes
	}
	if config.MaxArtifactScan <= 0 {
		config.MaxArtifactScan = defaults.MaxArtifactScan
	}
	return &Service{snapshots: snapshots, retriever: retriever, repo: repo, generator: generator, applier: applier,
		events: events, observer: observer, ids: ids, clock: clock, config: config}, nil
}

func (s *Service) Propose(ctx context.Context, cmd assistant.CodingCommand) (proposal assistant.ChangeProposal, err error) {
	if validateErr := cmd.Validate(); validateErr != nil {
		code := assistant.ErrInvalidInput
		if strings.Contains(validateErr.Error(), "permission") {
			code = assistant.ErrPermissionDenied
		}
		return proposal, domainError(code, "validate", validateErr.Error(), false, validateErr)
	}
	if cached, ok, lookupErr := s.repo.FindByIdempotencyKey(ctx, cmd.Scope, cmd.IdempotencyKey); lookupErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return proposal, ctxErr
		}
		return proposal, domainError(assistant.ErrPersistence, "idempotency_lookup", "failed to check proposal idempotency key", true, lookupErr)
	} else if ok {
		if cached.SessionID != cmd.SessionID || cached.UserID != cmd.UserID || cached.SnapshotID != cmd.Scope.SnapshotID || cached.Intent != cmd.Intent {
			return proposal, domainError(assistant.ErrProposalConflict, "idempotency_lookup", "idempotency key already belongs to another coding command", false, nil)
		}
		s.observer.Count("assistant_proposal_idempotency_hits_total", 1, labels(cmd.Scope))
		return cached, nil
	}
	finish := s.observer.Stage(ctx, "assistant_propose", labels(cmd.Scope))
	defer func() { finish(err) }()

	snapshot, snapshotErr := s.snapshots.GetSnapshot(ctx, cmd.Scope)
	if snapshotErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return proposal, ctxErr
		}
		return proposal, domainError(assistant.ErrSnapshotNotFound, "load_snapshot", "requested repository snapshot was not found", false, snapshotErr)
	}
	if snapshot.SyncStatus != repository.StatusSucceeded || strings.TrimSpace(snapshot.CommitSHA) == "" {
		return proposal, domainError(assistant.ErrSnapshotNotFound, "validate_snapshot", "requested repository snapshot is not ready", true, nil)
	}
	for _, ref := range cmd.SelectedRefs {
		if ref.CommitSHA != snapshot.CommitSHA {
			return proposal, domainError(assistant.ErrInvalidInput, "validate_selected_refs", "selected source belongs to another commit", false, nil)
		}
	}

	evidence := ports.EvidenceBundle{}
	if s.retriever != nil {
		filters := rag.Filters{}
		seenPaths := map[string]bool{}
		for _, ref := range cmd.SelectedRefs {
			path := filepath.ToSlash(filepath.Clean(ref.Path))
			if !seenPaths[path] {
				filters.PathPrefixes = append(filters.PathPrefixes, path)
				seenPaths[path] = true
			}
		}
		evidence, err = s.retriever.Search(ctx, ports.RetrievalRequest{Scope: cmd.Scope, Query: cmd.Instruction,
			Strategies: []string{"SYMBOL", "KEYWORD", "SEMANTIC", "GRAPH"}, Filters: filters, TopK: s.config.RetrievalTopK})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return proposal, ctxErr
			}
			return proposal, domainError(assistant.ErrInsufficientEvidence, "retrieve_evidence", "failed to retrieve snapshot-scoped code evidence", true, err)
		}
	}
	catalog := append([]common.SourceRef(nil), cmd.SelectedRefs...)
	catalog = append(catalog, evidence.Sources...)
	for _, item := range evidence.ContextBundle.Items {
		catalog = append(catalog, item.SourceRef)
	}
	fileRefs, fileHashes, artifactErr := s.authoritativeFileRefs(ctx, cmd.Scope, snapshot.CommitSHA, catalog)
	if artifactErr != nil {
		return proposal, artifactErr
	}
	catalog = append(catalog, fileRefs...)
	catalog = assistant.NormalizeCitations(catalog, snapshot.CommitSHA, s.config.MaxCitations)
	if len(catalog) == 0 {
		return proposal, domainError(assistant.ErrInsufficientEvidence, "validate_evidence", "no valid evidence exists for the requested snapshot", false, nil)
	}
	evidence.Sources = catalog

	draft, generateErr := s.generator.GenerateProposal(ctx, ports.ProposalGenerationContext{Command: cmd, Evidence: evidence})
	if generateErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return proposal, ctxErr
		}
		return proposal, domainError(assistant.ErrGenerationFailure, "generate_proposal", "coding proposal generation failed", true, generateErr)
	}
	citations, outputErr := s.validateDraft(cmd.Intent, draft, catalog, fileHashes)
	if outputErr != nil {
		return proposal, domainError(assistant.ErrGenerationFailure, "validate_output", outputErr.Error(), false, outputErr)
	}
	if draft.Model == "" {
		draft.Model = assistant.DefaultModel
	}
	if draft.PromptVersion == "" {
		draft.PromptVersion = assistant.DefaultPromptVersion
	}
	now, proposalID := s.clock.Now().UTC(), s.ids.New("cp")
	proposal = assistant.ChangeProposal{EntityMeta: assistant.NewMeta(proposalID, cmd.Scope, assistant.ProposalAwaitingApproval, cmd.UserID, now),
		ProposalID: proposalID, SessionID: cmd.SessionID, UserID: cmd.UserID, SnapshotID: cmd.Scope.SnapshotID,
		BaseCommitSHA: snapshot.CommitSHA, Intent: cmd.Intent, Summary: strings.TrimSpace(draft.Summary), Explanation: strings.TrimSpace(draft.Explanation),
		FileChanges: cloneChanges(draft.FileChanges), TestPlan: cloneStrings(draft.TestPlan), RiskLevel: draft.RiskLevel,
		ApprovalStatus: assistant.ProposalAwaitingApproval, Citations: citations, Model: draft.Model, PromptVersion: draft.PromptVersion, TokenUsage: draft.TokenUsage}
	if validateErr := proposal.Validate(); validateErr != nil {
		return assistant.ChangeProposal{}, domainError(assistant.ErrGenerationFailure, "validate_output", validateErr.Error(), false, validateErr)
	}
	session := assistant.CodingSession{EntityMeta: assistant.NewMeta(cmd.SessionID, cmd.Scope, assistant.ProposalAwaitingApproval, cmd.UserID, now),
		SessionID: cmd.SessionID, UserID: cmd.UserID, Intent: cmd.Intent, BaseCommitSHA: snapshot.CommitSHA, ContextRefs: cloneRefs(citations)}
	if saveErr := s.repo.CreateProposal(ctx, cmd.IdempotencyKey, session, proposal); saveErr != nil {
		return assistant.ChangeProposal{}, domainError(assistant.ErrPersistence, "save_proposal", "failed to atomically save coding session and proposal", true, saveErr)
	}
	s.observer.Count("assistant_proposals_total", 1, mergeLabels(cmd.Scope, map[string]string{"intent": string(cmd.Intent), "risk": string(proposal.RiskLevel)}))
	s.observer.Count("assistant_proposal_files_total", int64(len(proposal.FileChanges)), labels(cmd.Scope))
	if proposal.TokenUsage > 0 {
		s.observer.Count("assistant_generation_tokens_total", int64(proposal.TokenUsage), labels(cmd.Scope))
	}
	return proposal, nil
}

func (s *Service) Apply(ctx context.Context, proposalID string, approval assistant.Approval) (result assistant.ApplyResult, err error) {
	if strings.TrimSpace(proposalID) == "" {
		return result, domainError(assistant.ErrInvalidInput, "validate", "proposal_id must not be empty", false, nil)
	}
	if validateErr := approval.Validate(); validateErr != nil {
		code := assistant.ErrInvalidInput
		if strings.Contains(validateErr.Error(), "permission") {
			code = assistant.ErrPermissionDenied
		}
		return result, domainError(code, "validate_approval", validateErr.Error(), false, validateErr)
	}
	finish := s.observer.Stage(ctx, "assistant_apply", mergeLabels(approval.Scope, map[string]string{"proposal_id": proposalID}))
	defer func() { finish(err) }()
	proposal, loadErr := s.repo.GetProposal(ctx, approval.Scope, proposalID)
	if loadErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		return result, domainError(assistant.ErrProposalNotFound, "load_proposal", "proposal was not found in the requested scope", false, loadErr)
	}
	if proposal.ApprovalStatus == assistant.ProposalApplied {
		if proposal.PublishedEvent.EventID != "" {
			if publishErr := s.events.Publish(ctx, proposal.PublishedEvent); publishErr != nil {
				return applyResult(proposal), domainError(assistant.ErrPersistence, "republish_event", "proposal is applied but event republication failed", true, publishErr)
			}
		}
		s.observer.Count("assistant_apply_idempotency_hits_total", 1, labels(approval.Scope))
		return applyResult(proposal), nil
	}
	if proposal.ApprovalStatus != assistant.ProposalAwaitingApproval {
		return result, domainError(assistant.ErrProposalConflict, "transition", fmt.Sprintf("proposal in status %s cannot be reviewed", proposal.ApprovalStatus), false, nil)
	}
	if !approval.Approved {
		proposal.ApprovalStatus, proposal.Status, proposal.UpdatedAt = assistant.ProposalRejected, string(assistant.ProposalRejected), s.clock.Now().UTC()
		proposal.Validation = append(proposal.Validation, assistant.ValidationResult{Name: "human_approval", Status: assistant.ValidationFailed, Message: strings.TrimSpace(approval.Reason)})
		if updateErr := s.repo.UpdateProposal(ctx, proposal, proposal.Version); updateErr != nil {
			return result, domainError(assistant.ErrProposalConflict, "reject_proposal", "proposal changed while recording rejection", true, updateErr)
		}
		s.observer.Count("assistant_proposal_rejections_total", 1, labels(approval.Scope))
		return applyResult(proposal), nil
	}
	if len(proposal.FileChanges) == 0 {
		return result, domainError(assistant.ErrApplyFailure, "validate_patch", "proposal contains no code changes to apply", false, nil)
	}
	if s.applier == nil {
		return result, domainError(assistant.ErrApplyFailure, "apply_patch", "no patch applier is configured", false, nil)
	}
	expectedVersion := proposal.Version
	proposal.ApprovalStatus, proposal.Status, proposal.UpdatedAt = assistant.ProposalApplying, string(assistant.ProposalApplying), s.clock.Now().UTC()
	proposal.Validation = append(proposal.Validation, assistant.ValidationResult{Name: "human_approval", Status: assistant.ValidationPassed})
	if updateErr := s.repo.UpdateProposal(ctx, proposal, expectedVersion); updateErr != nil {
		return result, domainError(assistant.ErrProposalConflict, "claim_proposal", "proposal changed while applying", true, updateErr)
	}
	proposal.Version = expectedVersion + 1
	applyOutput, applyErr := s.applier.ApplyPatch(ctx, ports.PatchApplyRequest{Scope: approval.Scope, ProposalID: proposal.ProposalID,
		BaseCommitSHA: proposal.BaseCommitSHA, FileChanges: cloneChanges(proposal.FileChanges)})
	proposal.Validation = append(proposal.Validation, applyOutput.Validation...)
	if applyErr != nil {
		proposal.ApprovalStatus, proposal.Status, proposal.FailureCode, proposal.FailureMessage = assistant.ProposalFailed, string(assistant.ProposalFailed), string(assistant.ErrApplyFailure), "patch validation or atomic apply failed"
		proposal.UpdatedAt = s.clock.Now().UTC()
		if updateErr := s.repo.UpdateProposal(ctx, proposal, proposal.Version); updateErr != nil {
			return result, domainError(assistant.ErrPersistence, "record_apply_failure", "patch failed and proposal failure state could not be recorded", true, errors.Join(applyErr, updateErr))
		}
		return applyResult(proposal), domainError(assistant.ErrApplyFailure, "apply_patch", "patch validation or atomic apply failed", false, applyErr)
	}
	proposal.ApprovalStatus, proposal.Status, proposal.AppliedCommitSHA, proposal.UpdatedAt = assistant.ProposalApplied, string(assistant.ProposalApplied), applyOutput.CommitSHA, s.clock.Now().UTC()
	proposal.PublishedEvent = common.EventEnvelope{EventID: s.ids.New("evt"), EventType: "proposal.applied.v1", AggregateID: proposal.ProposalID,
		OccurredAt: s.clock.Now().UTC(), Producer: "coding-assistant", PayloadVersion: 1, TraceID: approval.Scope.TraceID,
		Payload: map[string]any{"proposal_id": proposal.ProposalID, "session_id": proposal.SessionID, "snapshot_id": proposal.SnapshotID,
			"base_commit_sha": proposal.BaseCommitSHA, "applied_commit_sha": proposal.AppliedCommitSHA, "file_count": len(proposal.FileChanges), "principal_id": approval.PrincipalID}}
	if updateErr := s.repo.UpdateProposal(ctx, proposal, proposal.Version); updateErr != nil {
		return result, domainError(assistant.ErrPersistence, "save_apply_result", "patch was applied but proposal state could not be finalized", true, updateErr)
	}
	if publishErr := s.events.Publish(ctx, proposal.PublishedEvent); publishErr != nil {
		return applyResult(proposal), domainError(assistant.ErrPersistence, "publish_event", "proposal was applied but event publication failed", true, publishErr)
	}
	s.observer.Count("assistant_proposal_applied_total", 1, labels(approval.Scope))
	return applyResult(proposal), nil
}

func (s *Service) validateDraft(intent assistant.Intent, draft assistant.ProposalDraft, catalog []common.SourceRef, fileHashes map[string]string) ([]common.SourceRef, error) {
	if strings.TrimSpace(draft.Summary) == "" {
		return nil, errors.New("generator returned an empty summary")
	}
	if len(draft.FileChanges) > s.config.MaxFileChanges {
		return nil, fmt.Errorf("generator returned more than %d file changes", s.config.MaxFileChanges)
	}
	total := 0
	for _, change := range draft.FileChanges {
		total += len(change.UnifiedDiff)
		if err := change.Validate(); err != nil {
			return nil, err
		}
		path := filepath.ToSlash(filepath.Clean(change.Path))
		if authoritative, exists := fileHashes[path]; !exists {
			return nil, fmt.Errorf("generator changed file %q without authoritative file evidence", path)
		} else if authoritative != change.BaseContentHash {
			return nil, fmt.Errorf("generator returned a non-authoritative base hash for %q", path)
		}
	}
	if total > s.config.MaxDiffBytes {
		return nil, fmt.Errorf("generated diff exceeds %d bytes", s.config.MaxDiffBytes)
	}
	if intent == assistant.IntentExplain && len(draft.FileChanges) != 0 {
		return nil, errors.New("explanation intent cannot produce file changes")
	}
	if intent != assistant.IntentExplain && len(draft.FileChanges) == 0 {
		return nil, errors.New("code-changing intent returned no file changes")
	}
	if draft.RiskLevel != assistant.RiskLow && draft.RiskLevel != assistant.RiskMedium && draft.RiskLevel != assistant.RiskHigh {
		return nil, errors.New("generator returned an invalid risk level")
	}
	if len(draft.CitationIndexes) == 0 {
		return nil, errors.New("generator returned no evidence citations")
	}
	seen, citations := map[int]bool{}, make([]common.SourceRef, 0, len(draft.CitationIndexes))
	for _, index := range draft.CitationIndexes {
		if index < 0 || index >= len(catalog) {
			return nil, fmt.Errorf("generator returned unknown citation index %d", index)
		}
		if seen[index] {
			return nil, fmt.Errorf("generator returned duplicate citation index %d", index)
		}
		seen[index] = true
		citations = append(citations, catalog[index])
	}
	return citations, nil
}

func (s *Service) authoritativeFileRefs(ctx context.Context, scope common.Scope, commit string, evidence []common.SourceRef) ([]common.SourceRef, map[string]string, error) {
	wanted := map[string]bool{}
	for _, ref := range evidence {
		wanted[filepath.ToSlash(filepath.Clean(ref.Path))] = true
	}
	refs, hashes := []common.SourceRef{}, map[string]string{}
	cursor, scanned := "", 0
	for {
		artifacts, next, err := s.snapshots.Artifacts(ctx, scope, cursor, 1000)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, nil, ctxErr
			}
			return nil, nil, domainError(assistant.ErrSnapshotNotFound, "load_file_artifacts", "failed to load authoritative file hashes", true, err)
		}
		scanned += len(artifacts)
		if scanned > s.config.MaxArtifactScan {
			return nil, nil, domainError(assistant.ErrInsufficientEvidence, "load_file_artifacts", "artifact scan exceeded configured safety limit", false, nil)
		}
		for _, artifact := range artifacts {
			path := filepath.ToSlash(filepath.Clean(artifact.SourceRef.Path))
			if artifact.Kind == repository.ArtifactFile && artifact.SourceRef.CommitSHA == commit && wanted[path] {
				ref := artifact.SourceRef
				ref.ContentHash = artifact.ContentHash
				refs = append(refs, ref)
				hashes[path] = artifact.ContentHash
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return refs, hashes, nil
}

func applyResult(proposal assistant.ChangeProposal) assistant.ApplyResult {
	return assistant.ApplyResult{ProposalID: proposal.ProposalID, Status: proposal.ApprovalStatus, AppliedCommitSHA: proposal.AppliedCommitSHA,
		Validation: append([]assistant.ValidationResult(nil), proposal.Validation...)}
}
func cloneChanges(values []assistant.FileChange) []assistant.FileChange {
	return append([]assistant.FileChange(nil), values...)
}
func cloneStrings(values []string) []string { return append([]string(nil), values...) }
func cloneRefs(values []common.SourceRef) []common.SourceRef {
	return append([]common.SourceRef(nil), values...)
}
func labels(scope common.Scope) map[string]string {
	return map[string]string{"tenant_id": scope.TenantID, "repository_id": scope.RepositoryID, "snapshot_id": scope.SnapshotID, "trace_id": scope.TraceID}
}
func mergeLabels(scope common.Scope, extra map[string]string) map[string]string {
	result := labels(scope)
	for key, value := range extra {
		result[key] = value
	}
	return result
}
func domainError(code assistant.ErrorCode, operation, message string, retryable bool, cause error) *assistant.DomainError {
	return &assistant.DomainError{Code: code, Operation: operation, Message: message, Retryable: retryable, Cause: cause}
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
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(value[:])
}

var _ ports.CodingAssistant = (*Service)(nil)
