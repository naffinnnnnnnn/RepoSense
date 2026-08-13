package assistant

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

const (
	ReadPermission       = "repo:read"
	WritePermission      = "repo:write"
	DefaultModel         = "structured-coding-assistant"
	DefaultPromptVersion = "coding-assistant-v1"
	MaxIntentLength      = 8_000
	MaxConstraintLength  = 2_000
)

type Intent string

const (
	IntentExplain  Intent = "EXPLAIN"
	IntentRefactor Intent = "REFACTOR"
	IntentPatch    Intent = "PATCH"
)

type ProposalStatus string

const (
	ProposalAwaitingApproval ProposalStatus = "AWAITING_APPROVAL"
	ProposalApplying         ProposalStatus = "APPLYING"
	ProposalApplied          ProposalStatus = "APPLIED"
	ProposalRejected         ProposalStatus = "REJECTED"
	ProposalFailed           ProposalStatus = "FAILED"
)

type RiskLevel string

const (
	RiskLow    RiskLevel = "LOW"
	RiskMedium RiskLevel = "MEDIUM"
	RiskHigh   RiskLevel = "HIGH"
)

type ValidationStatus string

const (
	ValidationPassed  ValidationStatus = "PASSED"
	ValidationFailed  ValidationStatus = "FAILED"
	ValidationSkipped ValidationStatus = "SKIPPED"
)

type CodingCommand struct {
	Scope          common.Scope       `json:"scope"`
	SessionID      string             `json:"session_id"`
	UserID         string             `json:"user_id"`
	Intent         Intent             `json:"intent"`
	Instruction    string             `json:"instruction"`
	SelectedRefs   []common.SourceRef `json:"selected_refs,omitempty"`
	Constraints    []string           `json:"constraints,omitempty"`
	Permissions    []string           `json:"permissions"`
	IdempotencyKey string             `json:"idempotency_key"`
}

func (c CodingCommand) Validate() error {
	if err := c.Scope.Validate(true); err != nil {
		return err
	}
	if strings.TrimSpace(c.SessionID) == "" || strings.TrimSpace(c.UserID) == "" {
		return errors.New("session_id and user_id must not be empty")
	}
	switch c.Intent {
	case IntentExplain, IntentRefactor, IntentPatch:
	default:
		return fmt.Errorf("unsupported coding intent %q", c.Intent)
	}
	instruction := strings.TrimSpace(c.Instruction)
	if instruction == "" {
		return errors.New("instruction must not be empty")
	}
	if len([]rune(instruction)) > MaxIntentLength {
		return fmt.Errorf("instruction exceeds %d characters", MaxIntentLength)
	}
	if strings.TrimSpace(c.IdempotencyKey) == "" {
		return errors.New("idempotency_key must not be empty")
	}
	if err := validatePermission(c.Permissions, ReadPermission); err != nil {
		return err
	}
	seenRefs := map[string]bool{}
	for _, ref := range c.SelectedRefs {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("selected_refs contains an invalid source: %w", err)
		}
		key := sourceKey(ref)
		if seenRefs[key] {
			return errors.New("selected_refs contains duplicates")
		}
		seenRefs[key] = true
	}
	seenConstraints := map[string]bool{}
	for _, constraint := range c.Constraints {
		constraint = strings.TrimSpace(constraint)
		if constraint == "" {
			return errors.New("constraints must not contain empty values")
		}
		if len([]rune(constraint)) > MaxConstraintLength {
			return fmt.Errorf("constraint exceeds %d characters", MaxConstraintLength)
		}
		if seenConstraints[constraint] {
			return errors.New("constraints contains duplicates")
		}
		seenConstraints[constraint] = true
	}
	return nil
}

type CodingSession struct {
	common.EntityMeta
	SessionID     string             `json:"session_id"`
	UserID        string             `json:"user_id"`
	Intent        Intent             `json:"intent"`
	BaseCommitSHA string             `json:"base_commit_sha"`
	ContextRefs   []common.SourceRef `json:"context_refs"`
}

type FileChange struct {
	Path            string `json:"path"`
	BaseContentHash string `json:"base_content_hash"`
	UnifiedDiff     string `json:"unified_diff"`
}

func (f FileChange) Validate() error {
	clean := filepath.ToSlash(filepath.Clean(strings.TrimSpace(f.Path)))
	if strings.ContainsAny(f.Path, "\r\n\x00") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || filepath.IsAbs(f.Path) {
		return fmt.Errorf("file change path must be repository relative: %q", f.Path)
	}
	if !strings.HasPrefix(f.BaseContentHash, "sha256:") || len(strings.TrimPrefix(f.BaseContentHash, "sha256:")) < 8 {
		return fmt.Errorf("file %q has an invalid base content hash", clean)
	}
	diff := strings.ReplaceAll(f.UnifiedDiff, "\r\n", "\n")
	if strings.TrimSpace(diff) == "" || strings.Contains(diff, "GIT binary patch") || strings.ContainsRune(diff, '\x00') {
		return fmt.Errorf("file %q must contain a textual unified diff", clean)
	}
	oldHeader, newHeader := "--- a/"+clean+"\n", "+++ b/"+clean+"\n"
	if !strings.HasPrefix(diff, oldHeader+newHeader) || !strings.Contains(diff, "\n@@ ") {
		return fmt.Errorf("file %q has invalid or mismatched unified diff headers", clean)
	}
	remainder := strings.TrimPrefix(diff, oldHeader+newHeader)
	for _, forbidden := range []string{"\n--- a/", "\n+++ b/", "\n--- /dev/null", "\n+++ /dev/null", "diff --git ", "rename from ", "rename to ", "new file mode ", "deleted file mode "} {
		if strings.Contains(remainder, forbidden) {
			return fmt.Errorf("file %q diff attempts an undeclared multi-file or lifecycle change", clean)
		}
	}
	return nil
}

type ValidationResult struct {
	Name       string           `json:"name"`
	Status     ValidationStatus `json:"status"`
	Message    string           `json:"message,omitempty"`
	DurationMS int64            `json:"duration_ms,omitempty"`
}

type ChangeProposal struct {
	common.EntityMeta
	ProposalID       string               `json:"proposal_id"`
	SessionID        string               `json:"session_id"`
	UserID           string               `json:"user_id"`
	SnapshotID       string               `json:"snapshot_id"`
	BaseCommitSHA    string               `json:"base_commit_sha"`
	Intent           Intent               `json:"intent"`
	Summary          string               `json:"summary"`
	Explanation      string               `json:"explanation,omitempty"`
	FileChanges      []FileChange         `json:"file_changes"`
	TestPlan         []string             `json:"test_plan"`
	RiskLevel        RiskLevel            `json:"risk_level"`
	ApprovalStatus   ProposalStatus       `json:"approval_status"`
	Citations        []common.SourceRef   `json:"citations"`
	Validation       []ValidationResult   `json:"validation_results,omitempty"`
	AppliedCommitSHA string               `json:"applied_commit_sha,omitempty"`
	FailureCode      string               `json:"failure_code,omitempty"`
	FailureMessage   string               `json:"failure_message,omitempty"`
	Model            string               `json:"model"`
	PromptVersion    string               `json:"prompt_version"`
	TokenUsage       int                  `json:"token_usage"`
	PublishedEvent   common.EventEnvelope `json:"-"`
}

func (p ChangeProposal) Validate() error {
	if p.ProposalID == "" || p.SessionID == "" || p.UserID == "" || p.SnapshotID == "" || p.BaseCommitSHA == "" {
		return errors.New("proposal identity and base revision must not be empty")
	}
	if strings.TrimSpace(p.Summary) == "" {
		return errors.New("proposal summary must not be empty")
	}
	switch p.RiskLevel {
	case RiskLow, RiskMedium, RiskHigh:
	default:
		return fmt.Errorf("invalid risk level %q", p.RiskLevel)
	}
	if p.Intent != IntentExplain && len(p.FileChanges) == 0 {
		return errors.New("a code-changing proposal must include at least one file change")
	}
	if p.Intent == IntentExplain && len(p.FileChanges) > 0 {
		return errors.New("an explanation proposal must not include file changes")
	}
	seen := map[string]bool{}
	for _, change := range p.FileChanges {
		if err := change.Validate(); err != nil {
			return err
		}
		path := filepath.ToSlash(filepath.Clean(change.Path))
		if seen[path] {
			return fmt.Errorf("proposal contains duplicate file change %q", path)
		}
		seen[path] = true
	}
	if len(p.Citations) == 0 {
		return errors.New("proposal must contain source citations")
	}
	for _, citation := range p.Citations {
		if err := citation.Validate(); err != nil {
			return err
		}
		if citation.CommitSHA != p.BaseCommitSHA {
			return errors.New("proposal citation does not belong to its base commit")
		}
	}
	return nil
}

type Approval struct {
	Scope       common.Scope `json:"scope"`
	PrincipalID string       `json:"principal_id"`
	Approved    bool         `json:"approved"`
	Permissions []string     `json:"permissions"`
	Reason      string       `json:"reason,omitempty"`
}

func (a Approval) Validate() error {
	if err := a.Scope.Validate(true); err != nil {
		return err
	}
	if strings.TrimSpace(a.PrincipalID) == "" {
		return errors.New("principal_id must not be empty")
	}
	return validatePermission(a.Permissions, WritePermission)
}

type ApplyResult struct {
	ProposalID       string             `json:"proposal_id"`
	Status           ProposalStatus     `json:"status"`
	AppliedCommitSHA string             `json:"applied_commit_sha,omitempty"`
	Validation       []ValidationResult `json:"validation_results,omitempty"`
}

type ProposalDraft struct {
	Summary         string
	Explanation     string
	FileChanges     []FileChange
	TestPlan        []string
	RiskLevel       RiskLevel
	CitationIndexes []int
	Model           string
	PromptVersion   string
	TokenUsage      int
}

func NormalizeCitations(refs []common.SourceRef, commit string, limit int) []common.SourceRef {
	unique := map[string]common.SourceRef{}
	for _, ref := range refs {
		ref.Path = filepath.ToSlash(filepath.Clean(ref.Path))
		if ref.Validate() != nil || (commit != "" && ref.CommitSHA != commit) {
			continue
		}
		unique[sourceKey(ref)] = ref
	}
	out := make([]common.SourceRef, 0, len(unique))
	for _, ref := range unique {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].StartLine < out[j].StartLine
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func NewMeta(id string, scope common.Scope, status ProposalStatus, actor string, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: string(status), Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: actor, TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}

func validatePermission(values []string, required string) error {
	seen, allowed := map[string]bool{}, false
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return errors.New("permissions must not contain empty values")
		}
		if seen[value] {
			return fmt.Errorf("duplicate permission %q", value)
		}
		seen[value], allowed = true, allowed || value == required
	}
	if !allowed {
		return fmt.Errorf("permission %q is required", required)
	}
	return nil
}

func sourceKey(ref common.SourceRef) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%s", ref.CommitSHA, ref.Path, ref.SymbolID, ref.StartLine, ref.EndLine, ref.ContentHash)
}
