package postgres

import (
	"context"
	"fmt"
	"strings"
)

func (s *GraphControlStore) Health(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("graph PostgreSQL pool is not configured")
	}
	return s.pool.Ping(ctx)
}

// VerifyGraphPrivileges enforces the PostgreSQL half of the per-role least
// privilege contract. Extra write privileges are rejected just like missing
// privileges; migration owners must never be used by an application role.
func (s *GraphControlStore) VerifyGraphPrivileges(ctx context.Context, role string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("graph PostgreSQL pool is not configured")
	}
	allowed, ok := graphRolePrivileges(role)
	if !ok {
		return fmt.Errorf("unknown Graph database role")
	}
	var superuser, createRole, createDatabase, replication, bypassRLS bool
	if err := s.pool.QueryRow(ctx, `SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls
FROM pg_roles WHERE rolname=current_user`).Scan(&superuser, &createRole, &createDatabase, &replication, &bypassRLS); err != nil {
		return err
	}
	if superuser || createRole || createDatabase || replication || bypassRLS {
		return fmt.Errorf("Graph application role has forbidden PostgreSQL role attributes")
	}
	var schemaUsage, schemaCreate bool
	if err := s.pool.QueryRow(ctx, `SELECT has_schema_privilege(current_user,current_schema(),'USAGE'),
has_schema_privilege(current_user,current_schema(),'CREATE')`).Scan(&schemaUsage, &schemaCreate); err != nil {
		return err
	}
	if !schemaUsage || schemaCreate {
		return fmt.Errorf("Graph application role has an incompatible PostgreSQL schema privilege")
	}
	operations := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	for _, table := range graphControlTables {
		for _, operation := range operations {
			var granted bool
			if err := s.pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,
format('%I.%I',current_schema(),$1),$2)`, table, operation).Scan(&granted); err != nil {
				return err
			}
			expected := allowed[table+":"+operation]
			if granted != expected {
				return fmt.Errorf("Graph PostgreSQL privilege contract mismatch for %s", role)
			}
		}
	}
	return nil
}

var graphControlTables = []string{
	"graph_build_jobs", "graph_idempotency", "graph_build_attempts", "graph_revisions",
	"graph_active_revisions", "graph_outbox_events", "graph_rejected_events", "graph_reconciliation_runs",
}

func graphRolePrivileges(role string) (map[string]bool, bool) {
	definitions := map[string][]string{
		"graph-api": {
			"graph_build_jobs:SELECT,INSERT", "graph_idempotency:SELECT,INSERT",
			"graph_revisions:SELECT", "graph_active_revisions:SELECT",
		},
		"graph-consumer": {
			"graph_build_jobs:SELECT,INSERT", "graph_idempotency:SELECT,INSERT",
			"graph_rejected_events:INSERT",
		},
		"graph-worker": {
			"graph_build_jobs:SELECT,UPDATE", "graph_build_attempts:SELECT,INSERT,UPDATE",
			"graph_revisions:SELECT,INSERT,UPDATE", "graph_active_revisions:SELECT,INSERT,UPDATE",
			"graph_outbox_events:SELECT,INSERT",
		},
		"graph-outbox": {"graph_outbox_events:SELECT,UPDATE"},
		"graph-reconciler": {
			"graph_build_jobs:SELECT", "graph_build_attempts:SELECT,UPDATE", "graph_revisions:SELECT",
			"graph_active_revisions:SELECT", "graph_reconciliation_runs:INSERT",
		},
	}
	entries, ok := definitions[role]
	if !ok {
		return nil, false
	}
	allowed := map[string]bool{}
	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 2)
		for _, operation := range strings.Split(parts[1], ",") {
			allowed[parts[0]+":"+operation] = true
		}
	}
	return allowed, true
}

// VerifyGraphSchema fails startup when the independently executed Graph
// migrations have not reached the minimum schema consumed by this binary.
func (s *GraphControlStore) VerifyGraphSchema(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("graph PostgreSQL pool is not configured")
	}
	var tablesReady, runtimeColumnReady bool
	err := s.pool.QueryRow(ctx, `SELECT
  to_regclass('graph_build_jobs') IS NOT NULL
	AND to_regclass('graph_idempotency') IS NOT NULL
  AND to_regclass('graph_build_attempts') IS NOT NULL
  AND to_regclass('graph_revisions') IS NOT NULL
  AND to_regclass('graph_active_revisions') IS NOT NULL
  AND to_regclass('graph_outbox_events') IS NOT NULL
  AND to_regclass('graph_rejected_events') IS NOT NULL
  AND to_regclass('graph_reconciliation_runs') IS NOT NULL,
  EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema=current_schema() AND table_name='graph_build_jobs' AND column_name='trigger_event_id')`).Scan(&tablesReady, &runtimeColumnReady)
	if err != nil {
		return err
	}
	if !tablesReady || !runtimeColumnReady {
		return fmt.Errorf("Graph PostgreSQL schema is incompatible")
	}
	return nil
}
