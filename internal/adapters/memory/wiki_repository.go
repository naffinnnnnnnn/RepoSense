package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/reposense/reposense/internal/domain/common"
	"github.com/reposense/reposense/internal/domain/wiki"
	"github.com/reposense/reposense/internal/ports"
)

// WikiRepository provides atomic, tenant-scoped in-memory persistence for
// tests and local composition. Returned values are deep copies.
type WikiRepository struct {
	mu          sync.RWMutex
	spaces      map[string]wiki.Space
	jobs        map[string]wiki.Job
	pages       map[string]wiki.PageRevision
	bySnapshot  map[string]string
	latest      map[string]string
	idempotency map[string]string
}

func NewWikiRepository() *WikiRepository {
	return &WikiRepository{spaces: map[string]wiki.Space{}, jobs: map[string]wiki.Job{}, pages: map[string]wiki.PageRevision{},
		bySnapshot: map[string]string{}, latest: map[string]string{}, idempotency: map[string]string{}}
}

func wikiScopeKey(scope common.Scope) string { return scope.TenantID + "\x00" + scope.RepositoryID }
func wikiSnapshotPageKey(scope common.Scope, slug string) string {
	return wikiScopeKey(scope) + "\x00" + scope.SnapshotID + "\x00" + slug
}
func wikiLatestPageKey(scope common.Scope, locale, slug string) string {
	return wikiScopeKey(scope) + "\x00" + locale + "\x00" + slug
}
func wikiIdempotencyKey(scope common.Scope, key string) string {
	return wikiScopeKey(scope) + "\x00" + key
}

func (s *WikiRepository) FindJobByIdempotencyKey(ctx context.Context, scope common.Scope, key string) (wiki.Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return wiki.Job{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobID, ok := s.idempotency[wikiIdempotencyKey(scope, key)]
	if !ok {
		return wiki.Job{}, false, nil
	}
	return cloneWikiJob(s.jobs[jobID]), true, nil
}

func (s *WikiRepository) LatestPage(ctx context.Context, scope common.Scope, locale, slug string) (wiki.PageRevision, bool, error) {
	if err := ctx.Err(); err != nil {
		return wiki.PageRevision{}, false, err
	}
	if err := scope.Validate(true); err != nil {
		return wiki.PageRevision{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	revisionID, ok := s.latest[wikiLatestPageKey(scope, locale, slug)]
	if !ok {
		return wiki.PageRevision{}, false, nil
	}
	return cloneWikiPage(s.pages[revisionID]), true, nil
}

func (s *WikiRepository) SavePublication(ctx context.Context, key string, space wiki.Space, job wiki.Job, pages []wiki.PageRevision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" || job.JobID == "" || job.Status != wiki.JobSucceeded || len(pages) == 0 {
		return fmt.Errorf("a succeeded job with pages and an idempotency key is required")
	}
	scope := common.Scope{TenantID: job.TenantID, RepositoryID: job.RepositoryID, SnapshotID: job.SnapshotID}
	if err := scope.Validate(true); err != nil {
		return err
	}
	if space.TenantID != scope.TenantID || space.RepositoryID != scope.RepositoryID || space.ActiveSnapshotID != scope.SnapshotID {
		return fmt.Errorf("wiki space and job scope mismatch")
	}
	seen := map[string]bool{}
	for _, page := range pages {
		if err := page.Validate(); err != nil {
			return err
		}
		if page.TenantID != scope.TenantID || page.RepositoryID != scope.RepositoryID || page.SnapshotID != scope.SnapshotID || page.GraphRevisionID != job.GraphRevisionID || page.Locale != space.Locale {
			return fmt.Errorf("wiki page and publication scope mismatch")
		}
		if seen[page.Slug] {
			return fmt.Errorf("duplicate page slug %q", page.Slug)
		}
		seen[page.Slug] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ik := wikiIdempotencyKey(scope, key)
	if existingID, ok := s.idempotency[ik]; ok {
		existing := s.jobs[existingID]
		if existing.SnapshotID != job.SnapshotID || existing.GraphRevisionID != job.GraphRevisionID {
			return &wiki.DomainError{Code: wiki.ErrConflict, Operation: "save_publication", Message: "idempotency key already used for another knowledge revision"}
		}
		return nil
	}
	for _, page := range pages {
		sk := wikiSnapshotPageKey(scope, page.Slug)
		if existingID, ok := s.bySnapshot[sk]; ok {
			existing := s.pages[existingID]
			if existing.ContentHash != page.ContentHash || existing.Locale != page.Locale {
				return &wiki.DomainError{Code: wiki.ErrConflict, Operation: "save_publication", Message: "snapshot already has a different page revision"}
			}
		}
		lk := wikiLatestPageKey(scope, page.Locale, page.Slug)
		if existingID, ok := s.latest[lk]; ok && s.pages[existingID].RevisionNo >= page.RevisionNo {
			return &wiki.DomainError{Code: wiki.ErrConflict, Operation: "save_publication", Message: "wiki page revision is stale"}
		}
	}

	s.spaces[space.SpaceID] = cloneWikiSpace(space)
	s.jobs[job.JobID] = cloneWikiJob(job)
	for _, page := range pages {
		stored := cloneWikiPage(page)
		s.pages[page.ID] = stored
		s.bySnapshot[wikiSnapshotPageKey(scope, page.Slug)] = page.ID
		s.latest[wikiLatestPageKey(scope, page.Locale, page.Slug)] = page.ID
	}
	s.idempotency[ik] = job.JobID
	return nil
}

func (s *WikiRepository) GetPage(ctx context.Context, scope common.Scope, slug string) (wiki.PageRevision, error) {
	if err := ctx.Err(); err != nil {
		return wiki.PageRevision{}, err
	}
	if err := scope.Validate(true); err != nil {
		return wiki.PageRevision{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	revisionID, ok := s.bySnapshot[wikiSnapshotPageKey(scope, slug)]
	if !ok {
		return wiki.PageRevision{}, &wiki.DomainError{Code: wiki.ErrPageNotFound, Operation: "get_page", Message: "wiki page not found"}
	}
	page := s.pages[revisionID]
	if page.TenantID != scope.TenantID || page.RepositoryID != scope.RepositoryID {
		return wiki.PageRevision{}, &wiki.DomainError{Code: wiki.ErrPageNotFound, Operation: "get_page", Message: "wiki page not found"}
	}
	return cloneWikiPage(page), nil
}

func cloneWikiSpace(value wiki.Space) wiki.Space { return value }
func cloneWikiJob(value wiki.Job) wiki.Job {
	value.PageIDs = append([]string(nil), value.PageIDs...)
	if value.PublishedEvent.Payload != nil {
		payload := map[string]any{}
		for key, item := range value.PublishedEvent.Payload {
			if list, ok := item.([]string); ok {
				payload[key] = append([]string(nil), list...)
			} else {
				payload[key] = item
			}
		}
		value.PublishedEvent.Payload = payload
	}
	return value
}
func cloneWikiPage(value wiki.PageRevision) wiki.PageRevision {
	value.Citations = append([]common.SourceRef(nil), value.Citations...)
	return value
}

var _ ports.WikiRepository = (*WikiRepository)(nil)
