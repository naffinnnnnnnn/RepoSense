CREATE TABLE graph_build_jobs (
    job_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    snapshot_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_fingerprint text NOT NULL UNIQUE,
    commit_sha text NOT NULL,
    parser_result_version text NOT NULL,
    graph_schema_version text NOT NULL,
    graph_algorithm_version text NOT NULL,
    build_policy_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','BUILDING','SUCCEEDED','FAILED','CANCELLED')),
    revision_id text,
    event_id text NOT NULL UNIQUE,
    trace_id text NOT NULL DEFAULT '',
    cancel_requested boolean NOT NULL DEFAULT false,
    error_code text,
    error_message text,
    retryable boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK (length(btrim(job_id)) > 0),
    CHECK (length(btrim(tenant_id)) > 0),
    CHECK (length(btrim(repository_id)) > 0),
    CHECK (length(btrim(snapshot_id)) > 0),
    CHECK (length(btrim(idempotency_key)) > 0),
    CHECK (length(request_fingerprint) = 64),
    CHECK (length(btrim(commit_sha)) > 0),
    CHECK (revision_id IS NULL OR length(btrim(revision_id)) > 0),
    CHECK (length(btrim(event_id)) > 0)
);

CREATE INDEX graph_build_jobs_claim_idx
    ON graph_build_jobs(status, created_at, job_id)
    WHERE cancel_requested = false AND status IN ('PENDING','BUILDING');
CREATE INDEX graph_build_jobs_scope_idx
    ON graph_build_jobs(tenant_id, repository_id, snapshot_id, created_at DESC);

CREATE TABLE graph_idempotency (
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_fingerprint text NOT NULL,
    job_id text NOT NULL REFERENCES graph_build_jobs(job_id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, repository_id, idempotency_key),
    CHECK (length(btrim(idempotency_key)) > 0),
    CHECK (length(request_fingerprint) = 64)
);

CREATE INDEX graph_idempotency_job_idx ON graph_idempotency(job_id);

CREATE TABLE graph_build_attempts (
    attempt_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES graph_build_jobs(job_id) ON DELETE CASCADE,
    revision_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    status text NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','LOST','CANCELLED')),
    lease_owner text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    error_code text,
    error_message text,
    retryable boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE(job_id, attempt_no),
    UNIQUE(job_id, fence),
    CHECK (length(btrim(attempt_id)) > 0),
    CHECK (length(btrim(revision_id)) > 0),
    CHECK (length(btrim(lease_owner)) > 0)
);

CREATE INDEX graph_build_attempts_expired_idx
    ON graph_build_attempts(lease_expires_at, created_at)
    WHERE status = 'RUNNING';
CREATE INDEX graph_build_attempts_revision_idx
    ON graph_build_attempts(revision_id, created_at);

CREATE TABLE graph_revisions (
    revision_id text PRIMARY KEY,
    job_id text NOT NULL UNIQUE REFERENCES graph_build_jobs(job_id),
    attempt_id text NOT NULL UNIQUE REFERENCES graph_build_attempts(attempt_id),
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    snapshot_id text NOT NULL,
    commit_sha text NOT NULL,
    request_fingerprint text NOT NULL UNIQUE,
    parser_result_version text NOT NULL,
    graph_schema_version text NOT NULL,
    graph_algorithm_version text NOT NULL,
    build_policy_version text NOT NULL,
    build_mode text NOT NULL CHECK (build_mode = 'FULL'),
    build_status text NOT NULL CHECK (build_status IN ('ACTIVE','FAILED','SUPERSEDED')),
    quality_status text NOT NULL CHECK (quality_status IN ('HEALTHY','DEGRADED')),
    stats jsonb NOT NULL,
    quality jsonb NOT NULL,
    event_id text NOT NULL UNIQUE,
    trace_id text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (tenant_id, repository_id, snapshot_id, revision_id),
    CHECK (jsonb_typeof(stats) = 'object'),
    CHECK (jsonb_typeof(quality) = 'object')
);

CREATE INDEX graph_revisions_scope_idx
    ON graph_revisions(tenant_id, repository_id, snapshot_id, created_at DESC);

CREATE TABLE graph_active_revisions (
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    snapshot_id text NOT NULL,
    revision_id text NOT NULL UNIQUE,
    activated_at timestamptz NOT NULL,
	PRIMARY KEY (tenant_id, repository_id, snapshot_id),
	FOREIGN KEY (tenant_id, repository_id, snapshot_id, revision_id)
		REFERENCES graph_revisions(tenant_id, repository_id, snapshot_id, revision_id)
);

CREATE TABLE graph_outbox_events (
    event_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    snapshot_id text NOT NULL,
    revision_id text NOT NULL,
    event_type text NOT NULL,
    aggregate_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    producer text NOT NULL,
    payload_version integer NOT NULL CHECK (payload_version > 0),
    trace_id text NOT NULL DEFAULT '',
    payload jsonb NOT NULL,
    delivery_count integer NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    next_attempt_at timestamptz NOT NULL,
    published_at timestamptz,
    dead_lettered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
	CHECK (jsonb_typeof(payload) = 'object'),
	FOREIGN KEY (tenant_id, repository_id, snapshot_id, revision_id)
		REFERENCES graph_revisions(tenant_id, repository_id, snapshot_id, revision_id)
);

CREATE INDEX graph_outbox_events_pending_idx
    ON graph_outbox_events(next_attempt_at, created_at, event_id)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;

CREATE TABLE graph_rejected_events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    tenant_id text,
    repository_id text,
    error_code text NOT NULL,
    error_message text NOT NULL,
    payload_digest text,
    rejected_at timestamptz NOT NULL
);

CREATE TABLE graph_reconciliation_runs (
    run_id text PRIMARY KEY,
    action text NOT NULL,
    target_type text NOT NULL,
    target_id text NOT NULL,
    result text NOT NULL,
    error_code text,
    error_message text,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL
);

CREATE INDEX graph_reconciliation_runs_target_idx
    ON graph_reconciliation_runs(target_type, target_id, started_at DESC);
