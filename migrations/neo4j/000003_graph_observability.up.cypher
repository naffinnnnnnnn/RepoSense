// Global, low-cardinality operational gauges are collected by the dedicated
// graph-reconciler role. These indexes keep candidate/quality counts from
// degrading into repeated full label scans as revision history grows.
CREATE INDEX graph_revision_candidate_status IF NOT EXISTS
FOR (r:GraphRevision) ON (r.candidate_status);

CREATE INDEX graph_revision_quality_status IF NOT EXISTS
FOR (r:GraphRevision) ON (r.quality_status);
