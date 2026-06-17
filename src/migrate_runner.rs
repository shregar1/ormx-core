//! Postgres migration apply/rollback (sqlx) — used by ``ormx-core`` Python bindings.

use std::collections::HashSet;

use futures::TryStreamExt;
use sqlx::postgres::PgPool;
use sqlx::Executor;

use ormx_migrate::{
    check_migration_records, find_record, latest_applied_version, pending_migrations,
    MigrationRecord, MigrateError, DELETE_APPLIED_VERSION, ENSURE_MIGRATIONS_TABLE,
    INSERT_APPLIED_VERSION, SELECT_APPLIED_VERSIONS,
};

async fn execute_many<'e, E>(executor: E, sql: &str) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let mut stream = executor.execute_many(sqlx::raw_sql(sql));
    while stream.try_next().await?.is_some() {}
    Ok(())
}

fn sqlx_err(e: sqlx::Error) -> MigrateError {
    MigrateError::Io(std::io::Error::other(e.to_string()))
}

pub async fn ensure_migrations_table(pool: &PgPool) -> Result<(), MigrateError> {
    sqlx::raw_sql(ENSURE_MIGRATIONS_TABLE.trim())
        .execute(pool)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

pub async fn fetch_applied_versions(pool: &PgPool) -> Result<HashSet<String>, MigrateError> {
    let rows: Vec<(String,)> = sqlx::query_as(SELECT_APPLIED_VERSIONS)
        .fetch_all(pool)
        .await
        .map_err(sqlx_err)?;
    Ok(rows.into_iter().map(|(v,)| v).collect())
}

pub async fn apply_migration_records(
    pool: &PgPool,
    records: &[MigrationRecord],
) -> Result<Vec<String>, MigrateError> {
    check_migration_records(records)?;
    ensure_migrations_table(pool).await?;
    let applied = fetch_applied_versions(pool).await?;
    let pending = pending_migrations(records, &applied);
    let mut newly_applied = Vec::with_capacity(pending.len());

    for migration in pending {
        let mut tx = pool.begin().await.map_err(sqlx_err)?;
        execute_many(&mut *tx, &migration.up).await.map_err(sqlx_err)?;
        sqlx::query(INSERT_APPLIED_VERSION)
            .bind(&migration.version)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        tx.commit().await.map_err(sqlx_err)?;
        newly_applied.push(migration.version.clone());
    }

    Ok(newly_applied)
}

pub async fn stamp_migration_records(
    pool: &PgPool,
    records: &[MigrationRecord],
) -> Result<Vec<String>, MigrateError> {
    check_migration_records(records)?;
    ensure_migrations_table(pool).await?;
    let applied = fetch_applied_versions(pool).await?;
    let pending = pending_migrations(records, &applied);
    let mut newly_stamped = Vec::with_capacity(pending.len());

    for migration in pending {
        sqlx::query(INSERT_APPLIED_VERSION)
            .bind(&migration.version)
            .execute(pool)
            .await
            .map_err(sqlx_err)?;
        newly_stamped.push(migration.version.clone());
    }

    Ok(newly_stamped)
}

pub async fn rollback_latest_migration(
    pool: &PgPool,
    records: &[MigrationRecord],
) -> Result<Option<String>, MigrateError> {
    check_migration_records(records)?;
    ensure_migrations_table(pool).await?;
    let applied = fetch_applied_versions(pool).await?;

    let latest = match latest_applied_version(&applied) {
        Some(v) => v,
        None => return Ok(None),
    };

    let target = match find_record(records, &latest) {
        Some(m) => m,
        None => {
            eprintln!(
                "  ormx: WARNING — version {latest} is in state but has no matching record. \
                 Skipping rollback; restore the migration file or manually remove the row."
            );
            return Ok(None);
        }
    };

    let mut tx = pool.begin().await.map_err(sqlx_err)?;
    execute_many(&mut *tx, &target.down).await.map_err(sqlx_err)?;
    sqlx::query(DELETE_APPLIED_VERSION)
        .bind(&latest)
        .execute(&mut *tx)
        .await
        .map_err(sqlx_err)?;
    tx.commit().await.map_err(sqlx_err)?;

    Ok(Some(latest))
}
