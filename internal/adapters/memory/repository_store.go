package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/graph"
	"github.com/reposense/reposense/internal/domain/repository"
)

type RepositoryStore struct {
	mu        sync.RWMutex
	results   map[string]repository.ParseResult
	latest    map[string]string
	snapshots map[string]repository.Snapshot
	tasks     map[string]repository.ParseTask
	taskKeys  map[string]string
	bindings  map[string]repository.RepositoryBinding
	outbox    map[string]repository.OutboxRecord
}

func NewRepositoryStore() *RepositoryStore {
	return &RepositoryStore{results: map[string]repository.ParseResult{}, latest: map[string]string{}, snapshots: map[string]repository.Snapshot{}, tasks: map[string]repository.ParseTask{}, taskKeys: map[string]string{}, bindings: map[string]repository.RepositoryBinding{}, outbox: map[string]repository.OutboxRecord{}}
}

func (s *RepositoryStore) GraphInput(ctx context.Context, scope common.Scope) (graph.BuildInput, error) {
	if err := scope.Validate(true); err != nil {
		return graph.BuildInput{}, err
	}
	if err := ctx.Err(); err != nil {
		return graph.BuildInput{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, result := range s.results {
		if result.Snapshot.SnapshotID == scope.SnapshotID && result.Snapshot.TenantID == scope.TenantID && result.Snapshot.RepositoryID == scope.RepositoryID {
			copy := cloneResult(result)
			return graph.BuildInput{Snapshot: copy.Snapshot, Artifacts: copy.Artifacts, Relations: copy.Relations, DeletedPaths: copy.DeletedPaths}, nil
		}
	}
	return graph.BuildInput{}, fmt.Errorf("snapshot not found")
}
func scopeKey(s common.Scope) string              { return s.TenantID + "\x00" + s.RepositoryID }
func resultKey(s common.Scope, key string) string { return scopeKey(s) + "\x00" + key }

func (s *RepositoryStore) FindByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (repository.ParseResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return repository.ParseResult{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rk := resultKey(scope, key)
	if jobID, exists := s.taskKeys[rk]; exists {
		if task, taskExists := s.tasks[jobID]; taskExists {
			if stored, resultExists := s.results[rk]; !resultExists || stored.Job.JobID != jobID {
				return repository.ParseResult{Job: task.Job, Snapshot: task.Snapshot, Artifacts: []repository.CodeArtifact{}, Relations: []repository.CodeRelation{}, DeletedPaths: []string{}, SkippedFiles: []repository.SkippedFile{}}, true, nil
			}
		}
	}
	r, ok := s.results[rk]
	return cloneResult(r), ok, nil
}
func (s *RepositoryStore) LatestSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return repository.Snapshot{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.latest[scopeKey(scope)]
	if !ok {
		return repository.Snapshot{}, false, nil
	}
	snapshot := s.snapshots[id]
	snapshot.ChangedPaths = append([]repository.ChangedPath(nil), snapshot.ChangedPaths...)
	return snapshot, true, nil
}
func (s *RepositoryStore) SaveResult(ctx context.Context, key string, result repository.ParseResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("解析结果无效: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveResultLocked(key, result)
}
func (s *RepositoryStore) saveResultLocked(key string, result repository.ParseResult) error {
	rk := resultKey(common.Scope{TenantID: result.Snapshot.TenantID, RepositoryID: result.Snapshot.RepositoryID}, key)
	stored := cloneResult(result)
	s.results[rk] = stored
	s.snapshots[result.Snapshot.SnapshotID] = stored.Snapshot
	if result.Snapshot.SyncStatus == repository.StatusSucceeded {
		s.latest[scopeKey(common.Scope{TenantID: result.Snapshot.TenantID, RepositoryID: result.Snapshot.RepositoryID})] = result.Snapshot.SnapshotID
	}
	if result.Event.EventID != "" {
		payload, _ := json.Marshal(result.Event.Payload)
		s.outbox[result.Event.EventID] = repository.OutboxRecord{TenantID: result.Snapshot.TenantID, RepositoryID: result.Snapshot.RepositoryID, Event: result.Event, EventID: result.Event.EventID, EventType: result.Event.EventType, AggregateID: result.Event.AggregateID, TraceID: result.Event.TraceID, PayloadVersion: result.Event.PayloadVersion, Payload: payload, OccurredAt: result.Event.OccurredAt, NextAttemptAt: result.Event.OccurredAt}
	}
	return nil
}
func (s *RepositoryStore) GetSnapshot(ctx context.Context, scope common.Scope) (repository.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return repository.Snapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.snapshots[scope.SnapshotID]
	if !ok || v.TenantID != scope.TenantID || v.RepositoryID != scope.RepositoryID {
		return repository.Snapshot{}, fmt.Errorf("快照不存在")
	}
	v.ChangedPaths = append([]repository.ChangedPath(nil), v.ChangedPaths...)
	return v, nil
}
func (s *RepositoryStore) Artifacts(ctx context.Context, scope common.Scope, cursor string, limit int) ([]repository.CodeArtifact, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	start := 0
	if cursor != "" {
		var err error
		start, err = strconv.Atoi(cursor)
		if err != nil || start < 0 {
			return nil, "", fmt.Errorf("游标无效")
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var found []repository.CodeArtifact
	for _, result := range s.results {
		if result.Snapshot.SnapshotID == scope.SnapshotID && result.Snapshot.TenantID == scope.TenantID && result.Snapshot.RepositoryID == scope.RepositoryID {
			if result.Snapshot.SyncStatus != repository.StatusSucceeded {
				return []repository.CodeArtifact{}, "", fmt.Errorf("失败快照不提供 artifact")
			}
			found = result.Artifacts
			break
		}
	}
	if start >= len(found) {
		return []repository.CodeArtifact{}, "", nil
	}
	end := start + limit
	next := ""
	if end < len(found) {
		next = strconv.Itoa(end)
	} else {
		end = len(found)
	}
	return append([]repository.CodeArtifact(nil), found[start:end]...), next, nil
}
func cloneResult(r repository.ParseResult) repository.ParseResult {
	r.Artifacts = append([]repository.CodeArtifact(nil), r.Artifacts...)
	for i := range r.Artifacts {
		if r.Artifacts[i].Attributes != nil {
			attributes := make(map[string]string, len(r.Artifacts[i].Attributes))
			for key, value := range r.Artifacts[i].Attributes {
				attributes[key] = value
			}
			r.Artifacts[i].Attributes = attributes
		}
	}
	r.Relations = append([]repository.CodeRelation(nil), r.Relations...)
	r.DeletedPaths = append([]string(nil), r.DeletedPaths...)
	r.SkippedFiles = append([]repository.SkippedFile(nil), r.SkippedFiles...)
	r.Snapshot.ChangedPaths = append([]repository.ChangedPath(nil), r.Snapshot.ChangedPaths...)
	if r.Event.Payload != nil {
		payload := make(map[string]any, len(r.Event.Payload))
		for key, value := range r.Event.Payload {
			if values, ok := value.([]string); ok {
				payload[key] = append([]string(nil), values...)
			} else {
				payload[key] = value
			}
		}
		r.Event.Payload = payload
	}
	return r
}
