//! Universal ORMX migration utilities — shared across language SDKs.

mod check;
mod discover;
mod dml;
mod error;
mod runner;
mod state;

pub use check::{
    check_migration_record, check_migration_records, check_sql_script, check_version_format,
};
pub use discover::{
    discover_sql_migrations, down_path_for, folder_down_path_for, migration_display_name,
    read_folder_sql_migration, read_sql_migration, sql_migration_version, SqlMigration,
};
pub use dml::{delete_from, insert_rows, sql_literal, SqlValue};
pub use error::MigrateError;
pub use runner::{
    find_record, latest_applied_version, pending_migrations, sort_records, MigrationRecord,
};
pub use state::{
    DELETE_APPLIED_VERSION, ENSURE_MIGRATIONS_TABLE, INSERT_APPLIED_VERSION,
    SELECT_APPLIED_VERSIONS,
};
