DROP INDEX IF EXISTS graph_build_jobs_trigger_event_idx;
ALTER TABLE graph_build_jobs DROP CONSTRAINT IF EXISTS graph_build_jobs_trigger_event_exact;
ALTER TABLE graph_build_jobs DROP COLUMN IF EXISTS trigger_event_id;
