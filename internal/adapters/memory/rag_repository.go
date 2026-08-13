package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/rag"
)

// RAGRepository is an immutable-revision in-memory adapter. It mirrors the
// atomic publication semantics expected from the future PostgreSQL/pgvector
// adapter and enforces the complete tenant/repository/snapshot scope key.
type RAGRepository struct {
	mu         sync.RWMutex
	revisions  map[string]rag.IndexRevision
	bySnapshot map[string]string
}

func NewRAGRepository() *RAGRepository {
	return &RAGRepository{revisions: map[string]rag.IndexRevision{}, bySnapshot: map[string]string{}}
}

func ragScopeKey(s common.Scope) string {
	return s.TenantID + "\x00" + s.RepositoryID + "\x00" + s.SnapshotID
}

func (r *RAGRepository) RevisionBySnapshot(ctx context.Context, scope common.Scope) (rag.IndexRevision, error) {
	if err := ctx.Err(); err != nil {
		return rag.IndexRevision{}, err
	}
	if err := scope.Validate(true); err != nil {
		return rag.IndexRevision{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.bySnapshot[ragScopeKey(scope)]
	if !ok {
		return rag.IndexRevision{}, &rag.DomainError{Code: rag.ErrIndexNotFound, Operation: "load_revision", Message: "RAG index revision not found"}
	}
	return cloneRAGRevision(r.revisions[id]), nil
}

func (r *RAGRepository) Save(ctx context.Context, revision rag.IndexRevision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if revision.Status != rag.IndexReady {
		return fmt.Errorf("only READY index revisions can be published")
	}
	if revision.RevisionID == "" || revision.SnapshotID == "" || revision.ContentHash == "" {
		return fmt.Errorf("revision_id, snapshot_id, and content_hash are required")
	}
	scope := common.Scope{TenantID: revision.TenantID, RepositoryID: revision.RepositoryID, SnapshotID: revision.SnapshotID}
	if err := scope.Validate(true); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := ragScopeKey(scope)
	if existingID, ok := r.bySnapshot[key]; ok {
		existing := r.revisions[existingID]
		if existing.ContentHash == revision.ContentHash {
			return nil
		}
	}
	r.revisions[revision.RevisionID] = cloneRAGRevision(revision)
	r.bySnapshot[key] = revision.RevisionID
	return nil
}

func (r *RAGRepository) Documents(ctx context.Context, scope common.Scope, revisionID string) ([]rag.IndexDocument, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := scope.Validate(true); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	revision, ok := r.revisions[revisionID]
	if !ok || revision.TenantID != scope.TenantID || revision.RepositoryID != scope.RepositoryID || revision.SnapshotID != scope.SnapshotID {
		return nil, &rag.DomainError{Code: rag.ErrIndexNotFound, Operation: "load_documents", Message: "RAG index documents not found"}
	}
	return cloneDocuments(revision.Documents), nil
}

func cloneRAGRevision(revision rag.IndexRevision) rag.IndexRevision {
	revision.Documents = cloneDocuments(revision.Documents)
	if revision.PublishedEvent.Payload != nil {
		payload := make(map[string]any, len(revision.PublishedEvent.Payload))
		for key, value := range revision.PublishedEvent.Payload {
			payload[key] = value
		}
		revision.PublishedEvent.Payload = payload
	}
	return revision
}

func cloneDocuments(documents []rag.IndexDocument) []rag.IndexDocument {
	result := append([]rag.IndexDocument(nil), documents...)
	for i := range result {
		result[i].SymbolTerms = append([]string(nil), result[i].SymbolTerms...)
		result[i].GraphRefs = append([]string(nil), result[i].GraphRefs...)
		result[i].Embedding = append([]float64(nil), result[i].Embedding...)
	}
	return result
}
