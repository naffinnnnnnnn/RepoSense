package assistantapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/reposense/reposense/internal/adapters/memory"
	"github.com/reposense/reposense/internal/domain/assistant"
	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/rag"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

type snapshotStore struct {
	snapshot  repository.Snapshot
	artifacts []repository.CodeArtifact
}

func (s snapshotStore) FindByIdempotencyKey(context.Context, common.Scope, string) (repository.ParseResult, bool, error) {
	return repository.ParseResult{}, false, nil
}
func (s snapshotStore) LatestSnapshot(context.Context, common.Scope) (repository.Snapshot, bool, error) {
	return s.snapshot, true, nil
}
func (s snapshotStore) SaveResult(context.Context, string, repository.ParseResult) error { return nil }
func (s snapshotStore) GetSnapshot(_ context.Context, scope common.Scope) (repository.Snapshot, error) {
	if scope.TenantID != s.snapshot.TenantID || scope.RepositoryID != s.snapshot.RepositoryID || scope.SnapshotID != s.snapshot.SnapshotID {
		return repository.Snapshot{}, errors.New("not found")
	}
	return s.snapshot, nil
}
func (s snapshotStore) Artifacts(context.Context, common.Scope, string, int) ([]repository.CodeArtifact, string, error) {
	return append([]repository.CodeArtifact(nil), s.artifacts...), "", nil
}

type assistantRetriever struct {
	bundle ports.EvidenceBundle
	err    error
}

func (r assistantRetriever) Index(context.Context, common.Scope, []repository.CodeArtifact) (ports.IndexRevision, error) {
	return ports.IndexRevision{}, nil
}
func (r assistantRetriever) Search(context.Context, ports.RetrievalRequest) (ports.EvidenceBundle, error) {
	return r.bundle, r.err
}

type draftGenerator struct {
	draft assistant.ProposalDraft
	err   error
	calls int
	mu    sync.Mutex
}

func (g *draftGenerator) GenerateProposal(context.Context, ports.ProposalGenerationContext) (assistant.ProposalDraft, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	return g.draft, g.err
}

type fakeApplier struct {
	result ports.PatchApplyResult
	err    error
	calls  int
	mu     sync.Mutex
	gate   chan struct{}
}

func (a *fakeApplier) ApplyPatch(context.Context, ports.PatchApplyRequest) (ports.PatchApplyResult, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if a.gate != nil {
		<-a.gate
	}
	return a.result, a.err
}

type eventPublisher struct {
	mu     sync.Mutex
	events []common.EventEnvelope
}

func (p *eventPublisher) Publish(_ context.Context, event common.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

type testIDs struct {
	mu sync.Mutex
	n  int
}

func (i *testIDs) New(prefix string) string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n++
	return fmt.Sprintf("%s_%d", prefix, i.n)
}

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

func assistantSource(commit string) common.SourceRef {
	return common.SourceRef{CommitSHA: commit, Path: "main.go", SymbolID: "main", StartLine: 1, EndLine: 2, ContentHash: "sha256:12345678"}
}
func assistantCommand() assistant.CodingCommand {
	return assistant.CodingCommand{Scope: common.Scope{TenantID: "tenant", RepositoryID: "repo", SnapshotID: "snap", TraceID: "trace"},
		SessionID: "session", UserID: "user", Intent: assistant.IntentPatch, Instruction: "fix main", Permissions: []string{assistant.ReadPermission}, IdempotencyKey: "key"}
}
func patchDraft() assistant.ProposalDraft {
	return assistant.ProposalDraft{Summary: "fix main", Explanation: "grounded", RiskLevel: assistant.RiskLow, CitationIndexes: []int{0},
		FileChanges: []assistant.FileChange{{Path: "main.go", BaseContentHash: "sha256:12345678", UnifiedDiff: "--- a/main.go\n+++ b/main.go\n@@ -1 +1 @@\n-old\n+new\n"}}, TestPlan: []string{"go test ./..."}}
}
func newAssistantService(t *testing.T, generator *draftGenerator, applier *fakeApplier, publisher *eventPublisher) (*Service, *memory.AssistantRepository) {
	t.Helper()
	scope := assistantCommand().Scope
	snapshot := repository.Snapshot{EntityMeta: repository.NewMeta("snap", scope, repository.StatusSucceeded, time.Unix(1, 0)), SnapshotID: "snap", CommitSHA: "sha", SyncStatus: repository.StatusSucceeded}
	repo := memory.NewAssistantRepository()
	fileRef := assistantSource("sha")
	service, err := New(snapshotStore{snapshot: snapshot, artifacts: []repository.CodeArtifact{{ArtifactID: "file", Kind: repository.ArtifactFile, SourceRef: fileRef, ContentHash: fileRef.ContentHash}}}, assistantRetriever{bundle: ports.EvidenceBundle{Sources: []common.SourceRef{assistantSource("sha")},
		ContextBundle: rag.ContextBundle{Items: []rag.ContextItem{{SourceRef: assistantSource("sha"), Text: "old"}}}}}, repo, generator, applier, publisher, nil, &testIDs{}, testClock{time.Unix(2, 0)}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	return service, repo
}

func TestProposeCreatesGroundedAwaitingApprovalProposalAndIsIdempotent(t *testing.T) {
	generator := &draftGenerator{draft: patchDraft()}
	service, _ := newAssistantService(t, generator, &fakeApplier{}, &eventPublisher{})
	first, err := service.Propose(context.Background(), assistantCommand())
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Propose(context.Background(), assistantCommand())
	if err != nil {
		t.Fatal(err)
	}
	if first.ProposalID != second.ProposalID || first.ApprovalStatus != assistant.ProposalAwaitingApproval || len(first.Citations) != 1 || generator.calls != 1 {
		t.Fatalf("unexpected proposals first=%#v second=%#v calls=%d", first, second, generator.calls)
	}
}

func TestProposeRejectsCrossCommitSelectedRefAndFabricatedCitation(t *testing.T) {
	generator := &draftGenerator{draft: patchDraft()}
	service, _ := newAssistantService(t, generator, &fakeApplier{}, &eventPublisher{})
	cmd := assistantCommand()
	cmd.SelectedRefs = []common.SourceRef{assistantSource("other")}
	if _, err := service.Propose(context.Background(), cmd); !assistant.IsCode(err, assistant.ErrInvalidInput) {
		t.Fatalf("expected cross-commit rejection, got %v", err)
	}
	generator.draft.CitationIndexes = []int{99}
	cmd.SelectedRefs, cmd.IdempotencyKey = nil, "other-key"
	if _, err := service.Propose(context.Background(), cmd); !assistant.IsCode(err, assistant.ErrGenerationFailure) {
		t.Fatalf("expected fabricated citation rejection, got %v", err)
	}
}

func TestProposeRejectsNonAuthoritativeFileHash(t *testing.T) {
	draft := patchDraft()
	draft.FileChanges[0].BaseContentHash = "sha256:deadbeef"
	service, _ := newAssistantService(t, &draftGenerator{draft: draft}, &fakeApplier{}, &eventPublisher{})
	if _, err := service.Propose(context.Background(), assistantCommand()); !assistant.IsCode(err, assistant.ErrGenerationFailure) {
		t.Fatalf("expected authoritative hash rejection, got %v", err)
	}
}

func TestApplyRequiresWriteApprovalPublishesEventAndIsIdempotent(t *testing.T) {
	publisher := &eventPublisher{}
	applier := &fakeApplier{result: ports.PatchApplyResult{Validation: []assistant.ValidationResult{{Name: "git_apply", Status: assistant.ValidationPassed}}}}
	service, _ := newAssistantService(t, &draftGenerator{draft: patchDraft()}, applier, publisher)
	proposal, _ := service.Propose(context.Background(), assistantCommand())
	approval := assistant.Approval{Scope: assistantCommand().Scope, PrincipalID: "reviewer", Approved: true}
	if _, err := service.Apply(context.Background(), proposal.ProposalID, approval); !assistant.IsCode(err, assistant.ErrPermissionDenied) {
		t.Fatalf("expected permission denial, got %v", err)
	}
	approval.Permissions = []string{assistant.WritePermission}
	first, err := service.Apply(context.Background(), proposal.ProposalID, approval)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Apply(context.Background(), proposal.ProposalID, approval)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != assistant.ProposalApplied || second.Status != assistant.ProposalApplied || applier.calls != 1 || len(publisher.events) != 2 {
		t.Fatalf("first=%#v second=%#v calls=%d events=%d", first, second, applier.calls, len(publisher.events))
	}
	if publisher.events[0].EventType != "proposal.applied.v1" {
		t.Fatalf("unexpected event %#v", publisher.events[0])
	}
}

func TestApplyRejectionNeverCallsPatchApplier(t *testing.T) {
	applier := &fakeApplier{}
	service, _ := newAssistantService(t, &draftGenerator{draft: patchDraft()}, applier, &eventPublisher{})
	proposal, _ := service.Propose(context.Background(), assistantCommand())
	result, err := service.Apply(context.Background(), proposal.ProposalID, assistant.Approval{Scope: assistantCommand().Scope, PrincipalID: "reviewer", Approved: false, Permissions: []string{assistant.WritePermission}, Reason: "unsafe"})
	if err != nil || result.Status != assistant.ProposalRejected || applier.calls != 0 {
		t.Fatalf("result=%#v err=%v calls=%d", result, err, applier.calls)
	}
}

func TestConcurrentApplyClaimsProposalOnce(t *testing.T) {
	gate := make(chan struct{})
	applier := &fakeApplier{gate: gate}
	service, _ := newAssistantService(t, &draftGenerator{draft: patchDraft()}, applier, &eventPublisher{})
	proposal, _ := service.Propose(context.Background(), assistantCommand())
	approval := assistant.Approval{Scope: assistantCommand().Scope, PrincipalID: "reviewer", Approved: true, Permissions: []string{assistant.WritePermission}}
	errs := make(chan error, 2)
	go func() { _, err := service.Apply(context.Background(), proposal.ProposalID, approval); errs <- err }()
	for {
		applier.mu.Lock()
		calls := applier.calls
		applier.mu.Unlock()
		if calls == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	go func() { _, err := service.Apply(context.Background(), proposal.ProposalID, approval); errs <- err }()
	time.Sleep(20 * time.Millisecond)
	close(gate)
	first, second := <-errs, <-errs
	conflicts := 0
	for _, err := range []error{first, second} {
		if assistant.IsCode(err, assistant.ErrProposalConflict) {
			conflicts++
		} else if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
	}
	if applier.calls != 1 || conflicts != 1 {
		t.Fatalf("calls=%d conflicts=%d errors=%v/%v", applier.calls, conflicts, first, second)
	}
}
