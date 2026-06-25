//! Shared ``ormx_migrations`` state-table DDL and queries.

/// Idempotent DDL for the migration state table.
pub const ENSURE_MIGRATIONS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS ormx_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT now()
)
"#;

pub const SELECT_APPLIED_VERSIONS: &str =
    "SELECT version FROM ormx_migrations ORDER BY version";

pub const INSERT_APPLIED_VERSION: &str =
    "INSERT INTO ormx_migrations (version) VALUES ($1)";

pub const DELETE_APPLIED_VERSION: &str =
    "DELETE FROM ormx_migrations WHERE version = $1";
