ALTER TABLE graph_build_jobs ADD COLUMN trigger_event_id text;

CREATE UNIQUE INDEX graph_build_jobs_trigger_event_idx
    ON graph_build_jobs(trigger_event_id)
    WHERE trigger_event_id IS NOT NULL;

ALTER TABLE graph_build_jobs ADD CONSTRAINT graph_build_jobs_trigger_event_exact
    CHECK (trigger_event_id IS NULL OR (length(btrim(trigger_event_id)) > 0 AND trigger_event_id = btrim(trigger_event_id)));
