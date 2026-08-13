package wiki

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/reposense/reposense/internal/domain/common"
)

const (
	DefaultLocale        = "zh-CN"
	DefaultModel         = "structured-wiki-generator"
	DefaultPromptVersion = "wiki-v1"
)

type JobStatus string

const (
	JobRunning   JobStatus = "RUNNING"
	JobSucceeded JobStatus = "SUCCEEDED"
	JobFailed    JobStatus = "FAILED"
)

type ReviewStatus string

const (
	ReviewDraft    ReviewStatus = "DRAFT"
	ReviewApproved ReviewStatus = "APPROVED"
)

type GenerationMode string

const (
	GenerationAI       GenerationMode = "AI"
	GenerationAssisted GenerationMode = "AI_ASSISTED"
	GenerationManual   GenerationMode = "MANUAL"
)

// Canonical page slugs form the stable MVP navigation contract.
var CanonicalPageSlugs = []string{
	"overview",
	"architecture",
	"modules",
	"key-flows",
	"interfaces",
	"development-guide",
}

type GenerateCommand struct {
	Scope           common.Scope `json:"scope"`
	GraphRevisionID string       `json:"graph_revision_id"`
	Locale          string       `json:"locale"`
	PageScope       []string     `json:"page_scope,omitempty"`
	IdempotencyKey  string       `json:"idempotency_key"`
}

func (c GenerateCommand) Validate() error {
	if err := c.Scope.Validate(true); err != nil {
		return err
	}
	if strings.TrimSpace(c.GraphRevisionID) == "" {
		return fmt.Errorf("graph_revision_id must not be empty")
	}
	if strings.TrimSpace(c.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key must not be empty")
	}
	if c.Locale != "" && c.Locale != "zh-CN" && c.Locale != "en-US" {
		return fmt.Errorf("unsupported locale %q", c.Locale)
	}
	allowed := make(map[string]bool, len(CanonicalPageSlugs))
	for _, slug := range CanonicalPageSlugs {
		allowed[slug] = true
	}
	seen := map[string]bool{}
	for _, slug := range c.PageScope {
		if !allowed[slug] {
			return fmt.Errorf("unsupported page scope %q", slug)
		}
		if seen[slug] {
			return fmt.Errorf("duplicate page scope %q", slug)
		}
		seen[slug] = true
	}
	return nil
}

func (c GenerateCommand) SelectedSlugs() []string {
	if len(c.PageScope) == 0 {
		return append([]string(nil), CanonicalPageSlugs...)
	}
	wanted := make(map[string]bool, len(c.PageScope))
	for _, slug := range c.PageScope {
		wanted[slug] = true
	}
	out := make([]string, 0, len(wanted))
	for _, slug := range CanonicalPageSlugs {
		if wanted[slug] {
			out = append(out, slug)
		}
	}
	return out
}

type Space struct {
	common.EntityMeta
	SpaceID          string `json:"space_id"`
	Title            string `json:"title"`
	Locale           string `json:"locale"`
	ActiveSnapshotID string `json:"active_snapshot_id"`
}

type PageRevision struct {
	common.EntityMeta
	PageID          string             `json:"page_id"`
	RevisionNo      int64              `json:"revision_no"`
	SnapshotID      string             `json:"snapshot_id"`
	GraphRevisionID string             `json:"graph_revision_id"`
	Locale          string             `json:"locale"`
	ParentPageID    string             `json:"parent_page_id,omitempty"`
	Slug            string             `json:"slug"`
	Title           string             `json:"title"`
	ContentMarkdown string             `json:"content_md"`
	Citations       []common.SourceRef `json:"citations"`
	GenerationMode  GenerationMode     `json:"generation_mode"`
	ReviewStatus    ReviewStatus       `json:"review_status"`
	Model           string             `json:"model"`
	PromptVersion   string             `json:"prompt_version"`
	ContentHash     string             `json:"content_hash"`
}

func (p PageRevision) Validate() error {
	if p.PageID == "" || p.RevisionNo < 1 || p.SnapshotID == "" || p.GraphRevisionID == "" || p.Locale == "" {
		return fmt.Errorf("page identity and revisions are required")
	}
	if strings.TrimSpace(p.Slug) == "" || strings.TrimSpace(p.Title) == "" || strings.TrimSpace(p.ContentMarkdown) == "" {
		return fmt.Errorf("page slug, title and content must not be empty")
	}
	if len(p.Citations) == 0 {
		return fmt.Errorf("page %q must contain at least one source citation", p.Slug)
	}
	for _, citation := range p.Citations {
		if err := citation.Validate(); err != nil {
			return fmt.Errorf("page %q has invalid citation: %w", p.Slug, err)
		}
	}
	return nil
}

type Job struct {
	common.EntityMeta
	JobID           string               `json:"job_id"`
	SnapshotID      string               `json:"snapshot_id"`
	GraphRevisionID string               `json:"graph_revision_id"`
	Status          JobStatus            `json:"job_status"`
	PageIDs         []string             `json:"page_ids"`
	ErrorCode       string               `json:"error_code,omitempty"`
	ErrorMessage    string               `json:"error_message,omitempty"`
	PublishedEvent  common.EventEnvelope `json:"-"`
}

type PageDraft struct {
	Slug            string
	Title           string
	ContentMarkdown string
	Citations       []common.SourceRef
}

type GenerationResult struct {
	Pages         []PageDraft
	Model         string
	PromptVersion string
	TokenUsage    int
}

func NormalizeCitations(refs []common.SourceRef) []common.SourceRef {
	unique := map[string]common.SourceRef{}
	for _, ref := range refs {
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%s", ref.CommitSHA, ref.Path, ref.SymbolID, ref.StartLine, ref.EndLine, ref.ContentHash)
		unique[key] = ref
	}
	out := make([]common.SourceRef, 0, len(unique))
	for _, ref := range unique {
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
	return out
}

func NewMeta(id string, scope common.Scope, status string, actor string, now time.Time) common.EntityMeta {
	return common.EntityMeta{ID: id, TenantID: scope.TenantID, RepositoryID: scope.RepositoryID,
		SchemaVersion: 1, Status: status, Version: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
		CreatedBy: actor, TraceID: scope.TraceID, Classification: "CONFIDENTIAL"}
}
