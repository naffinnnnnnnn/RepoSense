package agent

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
	MaxQuestionLength = 8_000
	ReadPermission    = "repo:read"
)

type RunStatus string

const (
	RunPending     RunStatus = "PENDING"
	RunRunning     RunStatus = "RUNNING"
	RunInterrupted RunStatus = "INTERRUPTED"
	RunCompleted   RunStatus = "COMPLETED"
	RunFailed      RunStatus = "FAILED"
)

type StepStatus string

const (
	StepPending   StepStatus = "PENDING"
	StepRunning   StepStatus = "RUNNING"
	StepCompleted StepStatus = "COMPLETED"
	StepFailed    StepStatus = "FAILED"
)

type Intent string

const (
	IntentArchitecture    Intent = "ARCHITECTURE"
	IntentCallChain       Intent = "CALL_CHAIN"
	IntentTroubleshooting Intent = "TROUBLESHOOTING"
	IntentImpactAnalysis  Intent = "IMPACT_ANALYSIS"
	IntentGeneral         Intent = "GENERAL"
)

type EventType string

const (
	EventRunStarted EventType = "RUN_STARTED"
	EventPlanned    EventType = "PLANNED"
	EventRetrieved  EventType = "RETRIEVED"
	EventEvaluated  EventType = "EVALUATED"
	EventCompleted  EventType = "COMPLETED"
	EventFailed     EventType = "FAILED"
)

type QuestionCommand struct {
	Scope          common.Scope `json:"scope"`
	ConversationID string       `json:"conversation_id"`
	UserID         string       `json:"user_id"`
	Question       string       `json:"question"`
	Permissions    []string     `json:"permissions"`
	Locale         string       `json:"locale,omitempty"`
}

func (c QuestionCommand) Validate() error {
	if err := c.Scope.Validate(true); err != nil {
		return err
	}
	if strings.TrimSpace(c.ConversationID) == "" {
		return errors.New("conversation_id must not be empty")
	}
	question := strings.TrimSpace(c.Question)
	if question == "" {
		return errors.New("question must not be empty")
	}
	if len([]rune(question)) > MaxQuestionLength {
		return fmt.Errorf("question exceeds %d characters", MaxQuestionLength)
	}
	if c.Locale != "" && c.Locale != "zh-CN" && c.Locale != "en-US" {
		return errors.New("locale must be zh-CN or en-US")
	}
	seen, allowed := map[string]bool{}, false
	for _, permission := range c.Permissions {
		permission = strings.TrimSpace(permission)
		if permission == "" {
			return errors.New("permissions must not contain empty values")
		}
		if seen[permission] {
			return fmt.Errorf("duplicate permission %q", permission)
		}
		seen[permission] = true
		allowed = allowed || permission == ReadPermission
	}
	if !allowed {
		return fmt.Errorf("permission %q is required", ReadPermission)
	}
	return nil
}

type Conversation struct {
	common.EntityMeta
	ConversationID string       `json:"conversation_id"`
	UserID         string       `json:"user_id"`
	Scope          common.Scope `json:"scope"`
	Title          string       `json:"title"`
}

type PlanStep struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Status      StepStatus `json:"status"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	ErrorCode   string     `json:"error_code,omitempty"`
}

type Plan struct {
	Intent     Intent     `json:"intent"`
	Steps      []PlanStep `json:"steps"`
	Strategies []string   `json:"strategies"`
	GraphDepth int        `json:"graph_depth"`
}

type ToolCall struct {
	CallID      string            `json:"call_id"`
	Tool        string            `json:"tool"`
	Round       int               `json:"round"`
	Arguments   map[string]string `json:"arguments,omitempty"`
	Status      StepStatus        `json:"status"`
	ResultCount int               `json:"result_count"`
	LatencyMS   int64             `json:"latency_ms"`
	ErrorCode   string            `json:"error_code,omitempty"`
}

type Answer struct {
	AnswerMarkdown       string             `json:"answer_md"`
	Citations            []common.SourceRef `json:"citations"`
	InsufficientEvidence bool               `json:"insufficient_evidence"`
	Warnings             []string           `json:"warnings,omitempty"`
	Model                string             `json:"model"`
	PromptVersion        string             `json:"prompt_version"`
}

func (a Answer) Validate() error {
	if strings.TrimSpace(a.AnswerMarkdown) == "" {
		return errors.New("answer_md must not be empty")
	}
	if !a.InsufficientEvidence && len(a.Citations) == 0 {
		return errors.New("an evidence-backed answer requires citations")
	}
	for _, citation := range a.Citations {
		if err := citation.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type Run struct {
	common.EntityMeta
	RunID          string               `json:"run_id"`
	ConversationID string               `json:"conversation_id"`
	SnapshotID     string               `json:"snapshot_id"`
	Question       string               `json:"question"`
	Status         RunStatus            `json:"run_status"`
	Plan           Plan                 `json:"plan"`
	ToolCalls      []ToolCall           `json:"tool_calls"`
	Answer         *Answer              `json:"answer,omitempty"`
	TokenUsage     int                  `json:"token_usage"`
	LatencyMS      int64                `json:"latency_ms"`
	FailureCode    string               `json:"failure_code,omitempty"`
	FailureMessage string               `json:"failure_message,omitempty"`
	PublishedEvent common.EventEnvelope `json:"-"`
}

type Event struct {
	RunID      string         `json:"run_id"`
	Type       EventType      `json:"type"`
	Sequence   int            `json:"sequence"`
	OccurredAt time.Time      `json:"occurred_at"`
	Payload    map[string]any `json:"payload,omitempty"`
}

// NormalizeCitations removes invalid and duplicate references, normalizes path
// separators, and guarantees deterministic output. If expectedCommit is set,
// references from any other commit are discarded.
func NormalizeCitations(refs []common.SourceRef, expectedCommit string, limit int) ([]common.SourceRef, int) {
	seen := map[string]bool{}
	out, discarded := make([]common.SourceRef, 0, len(refs)), 0
	for _, ref := range refs {
		ref.Path = filepath.ToSlash(filepath.Clean(ref.Path))
		if ref.Validate() != nil || (expectedCommit != "" && ref.CommitSHA != expectedCommit) {
			discarded++
			continue
		}
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%s", ref.CommitSHA, ref.Path, ref.SymbolID, ref.StartLine, ref.EndLine, ref.ContentHash)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].StartLine != out[j].StartLine {
			return out[i].StartLine < out[j].StartLine
		}
		return out[i].EndLine < out[j].EndLine
	})
	if limit > 0 && len(out) > limit {
		discarded += len(out) - limit
		out = out[:limit]
	}
	return out, discarded
}

func NewMeta(id string, scope common.Scope, status string, createdBy string, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: status, Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: createdBy, TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}
