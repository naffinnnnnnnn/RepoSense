// Phase 5 graph data-plane schema. Revision identity is global so the same
// snapshot may have multiple immutable algorithm/schema revisions.
DROP CONSTRAINT graph_revision_identity IF EXISTS;

CREATE CONSTRAINT graph_revision_id IF NOT EXISTS
FOR (r:GraphRevision) REQUIRE r.revision_id IS UNIQUE;

CREATE CONSTRAINT graph_external_identity IF NOT EXISTS
FOR (n:ExternalEntity) REQUIRE (n.revision_id, n.external_id) IS UNIQUE;

CREATE CONSTRAINT graph_artifact_identity IF NOT EXISTS
FOR (n:CodeEntity) REQUIRE (n.revision_id, n.artifact_id) IS UNIQUE;

CREATE CONSTRAINT graph_resolution_issue_identity IF NOT EXISTS
FOR (n:ResolutionIssue) REQUIRE (n.revision_id, n.issue_id) IS UNIQUE;

CREATE INDEX graph_revision_scope_status IF NOT EXISTS
FOR (r:GraphRevision) ON (r.tenant_id, r.repository_id, r.snapshot_id, r.candidate_status);

CREATE INDEX graph_external_node_id IF NOT EXISTS
FOR (n:ExternalEntity) ON (n.revision_id, n.node_id);

CREATE INDEX graph_code_entity_node_id IF NOT EXISTS
FOR (n:CodeEntity) ON (n.revision_id, n.node_id);

CREATE INDEX graph_resolution_issue_status IF NOT EXISTS
FOR (n:ResolutionIssue) ON (n.revision_id, n.resolution_status);

CREATE CONSTRAINT graph_contains_identity IF NOT EXISTS FOR ()-[r:CONTAINS]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_imports_identity IF NOT EXISTS FOR ()-[r:IMPORTS]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_calls_identity IF NOT EXISTS FOR ()-[r:CALLS]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_extends_identity IF NOT EXISTS FOR ()-[r:EXTENDS]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_implements_identity IF NOT EXISTS FOR ()-[r:IMPLEMENTS]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_depends_on_identity IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_uncertain_identity IF NOT EXISTS FOR ()-[r:UNCERTAIN_RELATION]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
CREATE CONSTRAINT graph_possible_target_identity IF NOT EXISTS FOR ()-[r:POSSIBLE_TARGET]-() REQUIRE (r.revision_id, r.relation_id) IS UNIQUE;
