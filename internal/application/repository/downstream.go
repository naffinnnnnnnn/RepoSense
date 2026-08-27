package repositoryapp

import (
	"context"
	"errors"
	"fmt"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/repository"
	"github.com/reposense/reposense/internal/ports"
)

// ParseCompletedTrigger is implemented by Graph/RAG orchestration adapters.
// The handler deliberately receives an exact, tenant-scoped succeeded snapshot.
type ParseCompletedTrigger interface {
	TriggerParseCompleted(context.Context, common.Scope, repository.Snapshot) error
}

type ParseEventHandler struct {
	store    ports.RepositoryStore
	triggers []ParseCompletedTrigger
}

func NewParseEventHandler(store ports.RepositoryStore, triggers ...ParseCompletedTrigger) (*ParseEventHandler, error) {
	if store == nil {
		return nil, errors.New("Repository Store 不能为空")
	}
	for _, trigger := range triggers {
		if trigger == nil {
			return nil, errors.New("Parse completed trigger 不能为空")
		}
	}
	return &ParseEventHandler{store: store, triggers: append([]ParseCompletedTrigger(nil), triggers...)}, nil
}

func (h *ParseEventHandler) Handle(ctx context.Context, scope common.Scope, event common.EventEnvelope) error {
	if event.EventType == "parse.failed.v1" {
		return nil
	}
	if event.EventType != "parse.completed.v1" {
		return fmt.Errorf("不支持事件类型 %q", event.EventType)
	}
	snapshotID, ok := event.Payload["snapshot_id"].(string)
	if !ok || snapshotID == "" || event.AggregateID != snapshotID {
		return errors.New("parse.completed 事件快照身份无效")
	}
	if scope.TenantID == "" || scope.RepositoryID == "" || scope.SnapshotID != snapshotID {
		return errors.New("parse.completed 事件缺少可信租户作用域")
	}
	snapshot, err := h.store.GetSnapshot(ctx, scope)
	if err != nil {
		return err
	}
	if snapshot.SyncStatus != repository.StatusSucceeded {
		return errors.New("下游拒绝非成功快照")
	}
	for _, trigger := range h.triggers {
		if err := trigger.TriggerParseCompleted(ctx, scope, snapshot); err != nil {
			return err
		}
	}
	return nil
}
