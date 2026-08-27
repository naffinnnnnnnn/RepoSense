CREATE TABLE repositories (
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    provider text NOT NULL,
    canonical_identity text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, repository_id),
    UNIQUE (tenant_id, canonical_identity)
);

CREATE TABLE repository_snapshots (
    snapshot_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    provider text NOT NULL,
    ref text NOT NULL,
    commit_sha text,
    parent_snapshot_id text REFERENCES repository_snapshots(snapshot_id),
    sync_status text NOT NULL CHECK (sync_status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    changed_paths jsonb NOT NULL DEFAULT '[]',
    error_code text,
    error_message text,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    trace_id text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, repository_id) REFERENCES repositories(tenant_id, repository_id),
    CHECK (sync_status <> 'SUCCEEDED' OR commit_sha IS NOT NULL),
    CHECK (sync_status <> 'FAILED' OR (error_code IS NOT NULL AND error_message IS NOT NULL))
);
CREATE INDEX repository_snapshots_scope_idx ON repository_snapshots(tenant_id, repository_id, created_at DESC);
CREATE UNIQUE INDEX repository_snapshots_success_commit_uq ON repository_snapshots(tenant_id, repository_id, commit_sha) WHERE sync_status = 'SUCCEEDED';

CREATE TABLE parse_jobs (
    job_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
    parser_version text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('FULL','INCREMENTAL')),
    status text NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    progress integer NOT NULL CHECK (progress BETWEEN 0 AND 100),
    error_code text,
    error_message text,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    command jsonb NOT NULL,
    command_fingerprint text NOT NULL,
    repository_identity text NOT NULL,
    retryable boolean NOT NULL DEFAULT false,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    lease_owner text,
    lease_expires_at timestamptz,
    cancel_requested boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (snapshot_id)
);
CREATE INDEX parse_jobs_claim_idx ON parse_jobs(status, lease_expires_at, created_at);
CREATE INDEX parse_jobs_scope_idx ON parse_jobs(tenant_id, repository_id, created_at DESC);

CREATE TABLE code_artifacts (
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id) ON DELETE CASCADE,
    artifact_id text NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    qualified_name text NOT NULL,
    language text NOT NULL,
    source_ref jsonb NOT NULL,
    signature text NOT NULL DEFAULT '',
    content_hash text NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (snapshot_id, artifact_id)
);
CREATE INDEX code_artifacts_symbol_idx ON code_artifacts(snapshot_id, qualified_name, artifact_id);

CREATE TABLE code_relations (
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id) ON DELETE CASCADE,
    relation_id text NOT NULL,
    kind text NOT NULL,
    from_ref text NOT NULL,
    to_ref text NOT NULL,
    evidence jsonb NOT NULL,
    confidence double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    PRIMARY KEY (snapshot_id, relation_id)
);

CREATE TABLE parser_idempotency (
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    idempotency_key text NOT NULL,
    command_fingerprint text NOT NULL,
    job_id text NOT NULL REFERENCES parse_jobs(job_id),
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
    status text NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    retryable boolean NOT NULL DEFAULT false,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    lease_owner text,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, repository_id, idempotency_key)
);

CREATE TABLE outbox_events (
    event_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload_version integer NOT NULL,
    trace_id text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    published_at timestamptz,
    delivery_count integer NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    last_error text,
    next_attempt_at timestamptz NOT NULL,
    dead_lettered_at timestamptz,
    FOREIGN KEY (tenant_id, repository_id) REFERENCES repositories(tenant_id, repository_id)
);
CREATE INDEX outbox_events_pending_idx ON outbox_events(next_attempt_at, occurred_at) WHERE published_at IS NULL AND dead_lettered_at IS NULL;
