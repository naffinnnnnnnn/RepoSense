package postgres

import (
	"context"
	"fmt"
)

// GraphOperationalMetrics returns only the gauges visible to one deployment
// role, so collecting metrics does not require broad cross-role table grants.
func (s *GraphControlStore) GraphOperationalMetrics(ctx context.Context, role string) (map[string]float64, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("graph PostgreSQL pool is not configured")
	}
	metrics := map[string]float64{}
	switch role {
	case "graph-api", "graph-consumer":
		var pending int64
		var oldest float64
		err := s.pool.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP-min(created_at))),0)
FROM graph_build_jobs WHERE status='PENDING'`).Scan(&pending, &oldest)
		if err != nil {
			return nil, err
		}
		metrics["graph_pending_jobs"] = float64(pending)
		metrics["graph_pending_oldest_age_seconds"] = oldest
	case "graph-worker":
		var pending, running int64
		err := s.pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM graph_build_jobs WHERE status='PENDING'),
  (SELECT count(*) FROM graph_build_attempts WHERE status='RUNNING' AND lease_expires_at>CURRENT_TIMESTAMP)`).Scan(&pending, &running)
		if err != nil {
			return nil, err
		}
		metrics["graph_pending_jobs"] = float64(pending)
		metrics["graph_running_attempts"] = float64(running)
	case "graph-outbox":
		var pending, dead int64
		var oldest float64
		err := s.pool.QueryRow(ctx, `SELECT
  count(*) FILTER (WHERE published_at IS NULL AND dead_lettered_at IS NULL),
  count(*) FILTER (WHERE dead_lettered_at IS NOT NULL),
  COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP-min(created_at) FILTER (WHERE published_at IS NULL AND dead_lettered_at IS NULL))),0)
FROM graph_outbox_events`).Scan(&pending, &dead, &oldest)
		if err != nil {
			return nil, err
		}
		metrics["graph_outbox_pending"] = float64(pending)
		metrics["graph_outbox_dead_letter"] = float64(dead)
		metrics["graph_outbox_oldest_age_seconds"] = oldest
	case "graph-reconciler":
		var running, active, failed int64
		err := s.pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM graph_build_attempts WHERE status='RUNNING' AND lease_expires_at>CURRENT_TIMESTAMP),
  (SELECT count(*) FROM graph_revisions WHERE build_status='ACTIVE'),
  (SELECT count(*) FROM graph_build_jobs WHERE status='FAILED')`).Scan(&running, &active, &failed)
		if err != nil {
			return nil, err
		}
		metrics["graph_running_attempts"] = float64(running)
		metrics["graph_active_revisions"] = float64(active)
		metrics["graph_failed_jobs"] = float64(failed)
	default:
		return nil, fmt.Errorf("unknown Graph metrics role")
	}
	return metrics, nil
}
