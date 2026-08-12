// All graph records are revision-scoped. A new revision is made visible only
// after every node and edge has been written and the revision status is ACTIVE.
CREATE CONSTRAINT graph_revision_identity IF NOT EXISTS
FOR (r:GraphRevision) REQUIRE (r.tenant_id, r.repository_id, r.snapshot_id) IS UNIQUE;

CREATE CONSTRAINT graph_node_identity IF NOT EXISTS
FOR (n:CodeEntity) REQUIRE (n.tenant_id, n.repository_id, n.revision_id, n.node_id) IS UNIQUE;

CREATE INDEX graph_node_artifact IF NOT EXISTS
FOR (n:CodeEntity) ON (n.tenant_id, n.repository_id, n.revision_id, n.artifact_id);

CREATE INDEX graph_node_type IF NOT EXISTS
FOR (n:CodeEntity) ON (n.tenant_id, n.repository_id, n.revision_id, n.entity_type);

CREATE INDEX graph_revision_status IF NOT EXISTS
FOR (r:GraphRevision) ON (r.tenant_id, r.repository_id, r.build_status);
