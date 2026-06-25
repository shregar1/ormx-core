use crate::error::MigrateError;

/// Scalar value for DML literal rendering (Postgres-compatible).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

pub fn sql_literal(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Bool(true) => "TRUE".to_string(),
        SqlValue::Bool(false) => "FALSE".to_string(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

fn validate_identifier(name: &str) -> Result<&str, MigrateError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(MigrateError::InvalidIdentifier(name.to_string()));
    }
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return Err(MigrateError::InvalidIdentifier(name.to_string()));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(MigrateError::InvalidIdentifier(name.to_string()));
    }
    Ok(name)
}

/// Build ``INSERT INTO ... VALUES ...`` for one or more rows.
pub fn insert_rows(
    table: &str,
    columns: &[&str],
    rows: &[Vec<SqlValue>],
    on_conflict: Option<&str>,
) -> Result<String, MigrateError> {
    if rows.is_empty() {
        return Ok(String::new());
    }

    let table_name = validate_identifier(table)?;
    let col_names: Result<Vec<&str>, _> = columns.iter().map(|c| validate_identifier(c)).collect();
    let col_names = col_names?;
    let cols_sql = col_names.join(", ");

    let mut value_groups = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != columns.len() {
            return Err(MigrateError::RowColumnMismatch {
                row_len: row.len(),
                col_len: columns.len(),
            });
        }
        let values: Vec<String> = row.iter().map(sql_literal).collect();
        value_groups.push(format!("({})", values.join(", ")));
    }

    let conflict = on_conflict.map(|c| format!(" {c}")).unwrap_or_default();

    if value_groups.len() == 1 {
        return Ok(format!(
            "INSERT INTO {table_name} ({cols_sql}) VALUES {}{conflict};",
            value_groups[0]
        ));
    }

    Ok(format!(
        "INSERT INTO {table_name} ({cols_sql}) VALUES\n  {}{conflict};",
        value_groups.join(",\n  ")
    ))
}

/// Build ``DELETE FROM table [WHERE ...]``.
pub fn delete_from(table: &str, where_clause: Option<&str>) -> Result<String, MigrateError> {
    let table_name = validate_identifier(table)?;
    match where_clause {
        None => Ok(format!("DELETE FROM {table_name};")),
        Some(w) => Ok(format!("DELETE FROM {table_name} WHERE {w};")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_escaping() {
        assert_eq!(sql_literal(&SqlValue::Null), "NULL");
        assert_eq!(sql_literal(&SqlValue::Bool(true)), "TRUE");
        assert_eq!(sql_literal(&SqlValue::Text("O'Brien".into())), "'O''Brien'");
    }

    #[test]
    fn insert_single_and_multi() {
        let sql = insert_rows(
            "user_role_lk",
            &["id", "code"],
            &[vec![
                SqlValue::Int(1),
                SqlValue::Text("SEEKER".into()),
            ]],
            None,
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO user_role_lk (id, code) VALUES (1, 'SEEKER');"
        );

        let sql = insert_rows(
            "user_role_lk",
            &["id", "code"],
            &[
                vec![SqlValue::Int(1), SqlValue::Text("SEEKER".into())],
                vec![SqlValue::Int(2), SqlValue::Text("PROVIDER".into())],
            ],
            None,
        )
        .unwrap();
        assert!(sql.contains("(1, 'SEEKER')"));
        assert!(sql.contains("(2, 'PROVIDER')"));
    }
}
