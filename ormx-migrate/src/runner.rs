//! Migration planning — language-agnostic apply/rollback logic.

use std::collections::HashSet;

use crate::error::MigrateError;

/// Versioned UP/DOWN SQL pair (from ``.sql`` files or native SDK modules).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRecord {
    pub version: String,
    pub up: String,
    pub down: String,
}

/// Records sorted by ``version``, excluding already-applied versions.
pub fn pending_migrations<'a>(
    records: &'a [MigrationRecord],
    applied: &HashSet<String>,
) -> Vec<&'a MigrationRecord> {
    let mut pending: Vec<&MigrationRecord> = records
        .iter()
        .filter(|m| !applied.contains(&m.version))
        .collect();
    pending.sort_by(|a, b| a.version.cmp(&b.version));
    pending
}

/// Most recently applied version (lexicographic = timestamp order).
pub fn latest_applied_version(applied: &HashSet<String>) -> Option<String> {
    applied.iter().max().cloned()
}

pub fn find_record<'a>(
    records: &'a [MigrationRecord],
    version: &str,
) -> Option<&'a MigrationRecord> {
    records.iter().find(|m| m.version == version)
}

/// Validate records are uniquely versioned and return them sorted.
pub fn sort_records(mut records: Vec<MigrationRecord>) -> Result<Vec<MigrationRecord>, MigrateError> {
    records.sort_by(|a, b| a.version.cmp(&b.version));
    for window in records.windows(2) {
        if window[0].version == window[1].version {
            return Err(MigrateError::DuplicateVersion {
                version: window[0].version.clone(),
                first: window[0].version.clone(),
                second: window[1].version.clone(),
            });
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(v: &str) -> MigrationRecord {
        MigrationRecord {
            version: v.into(),
            up: format!("UP {v}"),
            down: format!("DOWN {v}"),
        }
    }

    #[test]
    fn pending_skips_applied_and_sorts() {
        let records = vec![rec("20260524100002_b"), rec("20260524100001_a")];
        let applied = HashSet::from(["20260524100001_a".to_string()]);
        let pending = pending_migrations(&records, &applied);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].version, "20260524100002_b");
    }

    #[test]
    fn latest_applied() {
        let applied = HashSet::from([
            "20260524100001_a".to_string(),
            "20260524100003_c".to_string(),
        ]);
        assert_eq!(
            latest_applied_version(&applied).as_deref(),
            Some("20260524100003_c")
        );
    }
}
