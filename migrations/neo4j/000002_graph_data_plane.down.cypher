DROP CONSTRAINT graph_possible_target_identity IF EXISTS;
DROP CONSTRAINT graph_uncertain_identity IF EXISTS;
DROP CONSTRAINT graph_depends_on_identity IF EXISTS;
DROP CONSTRAINT graph_implements_identity IF EXISTS;
DROP CONSTRAINT graph_extends_identity IF EXISTS;
DROP CONSTRAINT graph_calls_identity IF EXISTS;
DROP CONSTRAINT graph_imports_identity IF EXISTS;
DROP CONSTRAINT graph_contains_identity IF EXISTS;
DROP INDEX graph_resolution_issue_status IF EXISTS;
DROP INDEX graph_code_entity_node_id IF EXISTS;
DROP INDEX graph_external_node_id IF EXISTS;
DROP INDEX graph_revision_scope_status IF EXISTS;
DROP CONSTRAINT graph_resolution_issue_identity IF EXISTS;
DROP CONSTRAINT graph_artifact_identity IF EXISTS;
DROP CONSTRAINT graph_external_identity IF EXISTS;
DROP CONSTRAINT graph_revision_id IF EXISTS;

CREATE CONSTRAINT graph_revision_identity IF NOT EXISTS
FOR (r:GraphRevision) REQUIRE (r.tenant_id, r.repository_id, r.snapshot_id) IS UNIQUE;
