CREATE TABLE repository_snapshots (
    snapshot_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    repository_id text NOT NULL,
    provider text NOT NULL,
    ref text NOT NULL,
    commit_sha text NOT NULL,
    parent_snapshot_id text REFERENCES repository_snapshots(snapshot_id),
    sync_status text NOT NULL CHECK (sync_status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    changed_paths jsonb NOT NULL DEFAULT '[]',
    error_code text,
    error_message text,
    retry_count integer NOT NULL DEFAULT 0,
    trace_id text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (tenant_id, repository_id, commit_sha)
);
CREATE INDEX repository_snapshots_scope_idx ON repository_snapshots(tenant_id, repository_id, created_at DESC);

CREATE TABLE parse_jobs (
    job_id text PRIMARY KEY,
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
    parser_version text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('FULL','INCREMENTAL')),
    status text NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    progress integer NOT NULL CHECK (progress BETWEEN 0 AND 100),
    error_code text,
    error_message text,
    retry_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE code_artifacts (
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
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
CREATE INDEX code_artifacts_symbol_idx ON code_artifacts(snapshot_id, qualified_name);

CREATE TABLE code_relations (
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
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
    snapshot_id text NOT NULL REFERENCES repository_snapshots(snapshot_id),
    PRIMARY KEY (tenant_id, repository_id, idempotency_key)
);

CREATE TABLE outbox_events (
    event_id text PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload_version integer NOT NULL,
    trace_id text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    published_at timestamptz
);
CREATE INDEX outbox_events_unpublished_idx ON outbox_events(occurred_at) WHERE published_at IS NULL;

