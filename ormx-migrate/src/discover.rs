use std::fs;
use std::path::{Path, PathBuf};

use crate::error::MigrateError;

/// A paired SQL migration loaded from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMigration {
    pub version: String,
    pub up: String,
    pub down: String,
    pub up_path: PathBuf,
}

const UP_SUFFIX: &str = ".up.sql";
const DOWN_SUFFIX: &str = ".down.sql";
const FOLDER_UP: &str = "up.sql";
const FOLDER_DOWN: &str = "down.sql";

/// Version stem from a flat ``{version}.up.sql`` path.
pub fn sql_migration_version(up_path: &Path) -> Result<String, MigrateError> {
    let name = up_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| MigrateError::InvalidIdentifier(up_path.display().to_string()))?;
    if !name.ends_with(UP_SUFFIX) {
        return Err(MigrateError::InvalidIdentifier(format!(
            "expected *.up.sql, got {name:?}"
        )));
    }
    Ok(name[..name.len() - UP_SUFFIX.len()].to_string())
}

/// Down path for a flat ``{version}.up.sql`` file.
pub fn down_path_for(up_path: &Path) -> PathBuf {
    let version = sql_migration_version(up_path).unwrap_or_default();
    up_path.with_file_name(format!("{version}{DOWN_SUFFIX}"))
}

/// Down path for a folder migration ``{version}/up.sql``.
pub fn folder_down_path_for(migration_dir: &Path) -> PathBuf {
    migration_dir.join(FOLDER_DOWN)
}

fn is_migration_directory(name: &str) -> bool {
    !name.starts_with('.') && !name.starts_with('_')
}

fn insert_migration(
    by_version: &mut std::collections::BTreeMap<String, SqlMigration>,
    migration: SqlMigration,
) -> Result<(), MigrateError> {
    if let Some(existing) = by_version.get(&migration.version) {
        return Err(MigrateError::DuplicateVersion {
            version: migration.version.clone(),
            first: existing.up_path.display().to_string(),
            second: migration.up_path.display().to_string(),
        });
    }
    by_version.insert(migration.version.clone(), migration);
    Ok(())
}

/// Load one flat paired SQL migration (``{version}.up.sql`` + ``{version}.down.sql``).
pub fn read_sql_migration(up_path: &Path) -> Result<SqlMigration, MigrateError> {
    let version = sql_migration_version(up_path)?;
    let down_path = up_path.with_file_name(format!("{version}{DOWN_SUFFIX}"));
    if !down_path.is_file() {
        return Err(MigrateError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing down migration for {}: expected {}",
                up_path.file_name().unwrap_or_default().to_string_lossy(),
                down_path.file_name().unwrap_or_default().to_string_lossy()
            ),
        )));
    }
    let up = fs::read_to_string(up_path)?;
    let down = fs::read_to_string(&down_path)?;
    Ok(SqlMigration {
        version,
        up,
        down,
        up_path: up_path.to_path_buf(),
    })
}

/// Load one folder SQL migration (``{version}/up.sql`` + ``{version}/down.sql``).
pub fn read_folder_sql_migration(migration_dir: &Path) -> Result<SqlMigration, MigrateError> {
    let version = migration_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| MigrateError::InvalidIdentifier(migration_dir.display().to_string()))?
        .to_string();

    let up_path = migration_dir.join(FOLDER_UP);
    let down_path = migration_dir.join(FOLDER_DOWN);

    if !up_path.is_file() {
        return Err(MigrateError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing up migration for {}: expected {}",
                migration_dir.display(),
                up_path.display()
            ),
        )));
    }
    if !down_path.is_file() {
        return Err(MigrateError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing down migration for {}: expected {}",
                migration_dir.display(),
                down_path.display()
            ),
        )));
    }

    let up = fs::read_to_string(&up_path)?;
    let down = fs::read_to_string(&down_path)?;
    Ok(SqlMigration {
        version,
        up,
        down,
        up_path,
    })
}

/// Discover SQL migrations in ``directory``, sorted by version.
///
/// Supports two layouts (do not mix both for the same version):
///
/// - **Flat:** ``{version}.up.sql`` + ``{version}.down.sql``
/// - **Folder:** ``{version}/up.sql`` + ``{version}/down.sql``
///
/// Skips pairs with a missing counterpart (prints a warning to stderr).
/// Fails on duplicate version stems across layouts.
pub fn discover_sql_migrations(directory: &Path) -> Result<Vec<SqlMigration>, MigrateError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }

    let mut by_version: std::collections::BTreeMap<String, SqlMigration> =
        std::collections::BTreeMap::new();

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        if path.is_file() && name.ends_with(UP_SUFFIX) {
            match read_sql_migration(&path) {
                Ok(migration) => insert_migration(&mut by_version, migration)?,
                Err(MigrateError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    eprintln!(
                        "  ormx: skipping {} — {}",
                        path.file_name().unwrap_or_default().to_string_lossy(),
                        e
                    );
                }
                Err(e) => return Err(e),
            }
            continue;
        }

        if path.is_dir() && is_migration_directory(name) {
            let up_path = path.join(FOLDER_UP);
            if !up_path.is_file() {
                continue;
            }
            match read_folder_sql_migration(&path) {
                Ok(migration) => insert_migration(&mut by_version, migration)?,
                Err(MigrateError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    eprintln!(
                        "  ormx: skipping {} — {}",
                        path.file_name().unwrap_or_default().to_string_lossy(),
                        e
                    );
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(by_version.into_values().collect())
}

/// Friendly name for CLI output (strip leading timestamp prefix).
pub fn migration_display_name(version: &str) -> &str {
    match version.split_once('_') {
        Some((_, rest)) if !rest.is_empty() => rest,
        _ => version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn version_and_down_path() {
        let up = Path::new("20260524100001_user_role_lk.up.sql");
        assert_eq!(
            sql_migration_version(up).unwrap(),
            "20260524100001_user_role_lk"
        );
        assert_eq!(
            down_path_for(up),
            PathBuf::from("20260524100001_user_role_lk.down.sql")
        );
    }

    #[test]
    fn folder_down_path() {
        let dir = Path::new("20260524100001_user_role_lk");
        assert_eq!(
            folder_down_path_for(dir),
            PathBuf::from("20260524100001_user_role_lk/down.sql")
        );
    }

    #[test]
    fn discover_flat_sorted() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        fs::write(root.join("20260524100002_beta.up.sql"), "CREATE TABLE b;").unwrap();
        fs::write(root.join("20260524100002_beta.down.sql"), "DROP TABLE b;").unwrap();
        fs::write(root.join("20260524100001_alpha.up.sql"), "CREATE TABLE a;").unwrap();
        fs::write(root.join("20260524100001_alpha.down.sql"), "DROP TABLE a;").unwrap();

        let migs = discover_sql_migrations(root).unwrap();
        assert_eq!(migs.len(), 2);
        assert_eq!(migs[0].version, "20260524100001_alpha");
        assert_eq!(migs[1].version, "20260524100002_beta");
    }

    #[test]
    fn discover_folder_sorted() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let alpha = root.join("20260524100001_alpha");
        let beta = root.join("20260524100002_beta");
        fs::create_dir_all(&alpha).unwrap();
        fs::create_dir_all(&beta).unwrap();
        fs::write(alpha.join("up.sql"), "CREATE TABLE a;").unwrap();
        fs::write(alpha.join("down.sql"), "DROP TABLE a;").unwrap();
        fs::write(beta.join("up.sql"), "CREATE TABLE b;").unwrap();
        fs::write(beta.join("down.sql"), "DROP TABLE b;").unwrap();

        let migs = discover_sql_migrations(root).unwrap();
        assert_eq!(migs.len(), 2);
        assert_eq!(migs[0].version, "20260524100001_alpha");
        assert_eq!(migs[0].up_path, alpha.join("up.sql"));
        assert_eq!(migs[1].version, "20260524100002_beta");
    }

    #[test]
    fn discover_mixed_layouts() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        fs::write(root.join("20260524100001_flat.up.sql"), "SELECT 1;").unwrap();
        fs::write(root.join("20260524100001_flat.down.sql"), "SELECT 1;").unwrap();

        let folder = root.join("20260524100002_folder");
        fs::create_dir_all(&folder).unwrap();
        fs::write(folder.join("up.sql"), "SELECT 2;").unwrap();
        fs::write(folder.join("down.sql"), "SELECT 2;").unwrap();

        let migs = discover_sql_migrations(root).unwrap();
        assert_eq!(migs.len(), 2);
        assert_eq!(migs[0].version, "20260524100001_flat");
        assert_eq!(migs[1].version, "20260524100002_folder");
    }

    #[test]
    fn duplicate_version_across_layouts_errors() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        fs::write(root.join("20260524100001_dup.up.sql"), "SELECT 1;").unwrap();
        fs::write(root.join("20260524100001_dup.down.sql"), "SELECT 1;").unwrap();

        let folder = root.join("20260524100001_dup");
        fs::create_dir_all(&folder).unwrap();
        fs::write(folder.join("up.sql"), "SELECT 2;").unwrap();
        fs::write(folder.join("down.sql"), "SELECT 2;").unwrap();

        let err = discover_sql_migrations(root).unwrap_err();
        assert!(err.to_string().contains("duplicate migration version"));
    }

    #[test]
    fn ignores_non_migration_directories() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("__pycache__")).unwrap();
        fs::create_dir_all(root.join("_scratch")).unwrap();
        fs::create_dir(root.join("20260524100001_empty")).unwrap();

        let migs = discover_sql_migrations(root).unwrap();
        assert!(migs.is_empty());
    }
}
