package common

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type Scope struct {
	TenantID     string `json:"tenant_id"`
	RepositoryID string `json:"repository_id"`
	SnapshotID   string `json:"snapshot_id"`
	TraceID      string `json:"trace_id"`
}

func (s Scope) Validate(requireSnapshot bool) error {
	if strings.TrimSpace(s.TenantID) == "" || strings.TrimSpace(s.RepositoryID) == "" {
		return errors.New("tenant_id 和 repository_id 不能为空")
	}
	if requireSnapshot && strings.TrimSpace(s.SnapshotID) == "" {
		return errors.New("snapshot_id 不能为空")
	}
	return nil
}

type SourceRef struct {
	CommitSHA   string `json:"commit_sha"`
	Path        string `json:"path"`
	SymbolID    string `json:"symbol_id,omitempty"`
	StartLine   int    `json:"start_line"`
	EndLine     int    `json:"end_line"`
	ContentHash string `json:"content_hash"`
}

func (r SourceRef) Validate() error {
	if r.CommitSHA == "" || r.Path == "" || r.StartLine < 1 || r.EndLine < r.StartLine {
		return errors.New("源码引用无效")
	}
	clean := filepath.ToSlash(filepath.Clean(r.Path))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || filepath.IsAbs(r.Path) {
		return fmt.Errorf("源码路径必须是仓库相对路径：%q", r.Path)
	}
	return nil
}

type EntityMeta struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id"`
	RepositoryID   string     `json:"repository_id,omitempty"`
	SchemaVersion  int        `json:"schema_version"`
	Status         string     `json:"status"`
	Version        int64      `json:"version"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CreatedBy      string     `json:"created_by"`
	TraceID        string     `json:"trace_id"`
	Classification string     `json:"classification"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty"`
}

type EventEnvelope struct {
	EventID        string         `json:"event_id"`
	EventType      string         `json:"event_type"`
	AggregateID    string         `json:"aggregate_id"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Producer       string         `json:"producer"`
	PayloadVersion int            `json:"payload_version"`
	TraceID        string         `json:"trace_id"`
	Payload        map[string]any `json:"payload"`
}
