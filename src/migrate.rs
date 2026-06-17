//! PyO3 bindings for ``ormx-migrate`` — shared migration utilities.

use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3_asyncio::tokio::future_into_py;
use std::path::Path;

use crate::Engine;
use crate::migrate_runner::{apply_migration_records, rollback_latest_migration, stamp_migration_records};
use ormx_migrate::{
    check_migration_records, delete_from, discover_sql_migrations, insert_rows,
    migration_display_name, sort_records, sql_literal, MigrationRecord, SqlMigration, SqlValue,
};

fn migrate_err(e: ormx_migrate::MigrateError) -> PyErr {
    PyException::new_err(e.to_string())
}

fn py_to_sql_value(value: &PyAny) -> PyResult<SqlValue> {
    if value.is_none() {
        return Ok(SqlValue::Null);
    }
    if value.is_instance_of::<pyo3::types::PyBool>() {
        return Ok(SqlValue::Bool(value.extract::<bool>()?));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(SqlValue::Int(i));
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(SqlValue::Float(f));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(SqlValue::Text(s));
    }
    Err(PyTypeError::new_err(format!(
        "unsupported SQL literal type: {}",
        value.get_type().name()?
    )))
}

fn records_from_py(records: &PyAny) -> PyResult<Vec<MigrationRecord>> {
    let list = records.downcast::<PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let dict = item.downcast::<PyDict>()?;
        let version: String = dict
            .get_item("version")?
            .ok_or_else(|| PyValueError::new_err("migration record missing 'version'"))?
            .extract()?;
        let up: String = dict
            .get_item("up")?
            .ok_or_else(|| PyValueError::new_err("migration record missing 'up'"))?
            .extract()?;
        let down: String = dict
            .get_item("down")?
            .ok_or_else(|| PyValueError::new_err("migration record missing 'down'"))?
            .extract()?;
        out.push(MigrationRecord { version, up, down });
    }
    sort_records(out).map_err(migrate_err)
}

fn extract_engine(executor: &PyAny) -> PyResult<PyRef<Engine>> {
    executor.extract()
}

fn sql_migration_to_dict(py: Python<'_>, m: &SqlMigration) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("version", &m.version)?;
    d.set_item("up", &m.up)?;
    d.set_item("down", &m.down)?;
    d.set_item("up_path", m.up_path.to_string_lossy().to_string())?;
    d.set_item("kind", "sql")?;
    Ok(d.into())
}

/// Discover paired ``*.up.sql`` / ``*.down.sql`` files in ``directory``.
#[pyfunction]
fn discover_sql_migrations_py(directory: &str) -> PyResult<Vec<Py<PyDict>>> {
    let migs = discover_sql_migrations(Path::new(directory)).map_err(migrate_err)?;
    Python::with_gil(|py| migs.iter().map(|m| sql_migration_to_dict(py, m)).collect())
}

/// Strip timestamp prefix from a migration version for CLI output.
#[pyfunction]
fn migration_display_name_py(version: &str) -> String {
    migration_display_name(version).to_string()
}

/// Render a Python scalar as a SQL literal string.
#[pyfunction]
fn sql_literal_py(value: &PyAny) -> PyResult<String> {
    Ok(sql_literal(&py_to_sql_value(value)?))
}

/// Build ``INSERT INTO ... VALUES ...`` from Python rows (list of list or list of dict).
#[pyfunction]
#[pyo3(signature = (table, columns, rows, on_conflict=None))]
fn insert_rows_py(
    _py: Python<'_>,
    table: &str,
    columns: Vec<String>,
    rows: &PyAny,
    on_conflict: Option<&str>,
) -> PyResult<String> {
    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let row_list = rows.downcast::<PyList>()?;

    let mut parsed_rows: Vec<Vec<SqlValue>> = Vec::with_capacity(row_list.len());
    for item in row_list.iter() {
        if let Ok(dict) = item.downcast::<PyDict>() {
            let mut values = Vec::with_capacity(columns.len());
            for col in &columns {
                let v = dict
                    .get_item(col)?
                    .ok_or_else(|| PyValueError::new_err(format!("missing column {col:?}")))?;
                values.push(py_to_sql_value(v)?);
            }
            parsed_rows.push(values);
        } else if let Ok(seq) = item.downcast::<PyTuple>() {
            if seq.len() != columns.len() {
                return Err(PyValueError::new_err(format!(
                    "row length {} does not match columns length {}",
                    seq.len(),
                    columns.len()
                )));
            }
            let mut values = Vec::with_capacity(columns.len());
            for v in seq.iter() {
                values.push(py_to_sql_value(v)?);
            }
            parsed_rows.push(values);
        } else if let Ok(seq) = item.downcast::<PyList>() {
            if seq.len() != columns.len() {
                return Err(PyValueError::new_err(format!(
                    "row length {} does not match columns length {}",
                    seq.len(),
                    columns.len()
                )));
            }
            let mut values = Vec::with_capacity(columns.len());
            for v in seq.iter() {
                values.push(py_to_sql_value(v)?);
            }
            parsed_rows.push(values);
        } else {
            return Err(PyTypeError::new_err(
                "each row must be a list, tuple, or dict",
            ));
        }
    }

    insert_rows(
        table,
        &col_refs,
        &parsed_rows,
        on_conflict,
    )
    .map_err(migrate_err)
}

/// Build ``DELETE FROM table [WHERE ...]``.
#[pyfunction]
#[pyo3(signature = (table, where_clause=None))]
fn delete_from_py(table: &str, where_clause: Option<&str>) -> PyResult<String> {
    delete_from(table, where_clause).map_err(migrate_err)
}

/// Validate UP/DOWN SQL for every migration record (list of ``{version, up, down}`` dicts).
#[pyfunction]
fn check_migration_records_py(records: &PyAny) -> PyResult<()> {
    let parsed = records_from_py(records)?;
    check_migration_records(&parsed).map_err(migrate_err)
}

/// Apply pending migrations from ``records`` (list of ``{version, up, down}`` dicts).
#[pyfunction]
fn apply_migration_records_py<'p>(
    py: Python<'p>,
    engine: &PyAny,
    records: &PyAny,
) -> PyResult<&'p PyAny> {
    let engine = extract_engine(engine)?;
    let pool = engine.pg_pool();
    let parsed = records_from_py(records)?;
    future_into_py(py, async move {
        let applied = apply_migration_records(&pool, &parsed)
            .await
            .map_err(migrate_err)?;
        for version in &applied {
            eprintln!(
                "  ormx: applied {}",
                migration_display_name(version)
            );
        }
        if applied.is_empty() {
            eprintln!("  ormx: nothing to apply");
        }
        Python::with_gil(|py| Ok(applied.into_py(py)))
    })
}

/// Mark pending migrations as applied without running their SQL.
#[pyfunction]
fn stamp_migration_records_py<'p>(
    py: Python<'p>,
    engine: &PyAny,
    records: &PyAny,
) -> PyResult<&'p PyAny> {
    let engine = extract_engine(engine)?;
    let pool = engine.pg_pool();
    let parsed = records_from_py(records)?;
    future_into_py(py, async move {
        let stamped = stamp_migration_records(&pool, &parsed)
            .await
            .map_err(migrate_err)?;
        for version in &stamped {
            eprintln!(
                "  ormx: stamped {}",
                migration_display_name(version)
            );
        }
        if stamped.is_empty() {
            eprintln!("  ormx: nothing to stamp");
        }
        Python::with_gil(|py| Ok(stamped.into_py(py)))
    })
}

/// Roll back the most recently applied migration in ``records``.
#[pyfunction]
fn rollback_latest_migration_py<'p>(
    py: Python<'p>,
    engine: &PyAny,
    records: &PyAny,
) -> PyResult<&'p PyAny> {
    let engine = extract_engine(engine)?;
    let pool = engine.pg_pool();
    let parsed = records_from_py(records)?;
    future_into_py(py, async move {
        let rolled = rollback_latest_migration(&pool, &parsed)
            .await
            .map_err(migrate_err)?;
        match rolled {
            Some(version) => {
                eprintln!(
                    "  ormx: rolled back {}",
                    migration_display_name(&version)
                );
                Python::with_gil(|py| Ok(version.into_py(py)))
            }
            None => {
                eprintln!("  ormx: no migrations to roll back");
                Python::with_gil(|py| Ok(py.None()))
            }
        }
    })
}

pub(crate) fn register(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(discover_sql_migrations_py, m)?)?;
    m.add_function(wrap_pyfunction!(migration_display_name_py, m)?)?;
    m.add_function(wrap_pyfunction!(sql_literal_py, m)?)?;
    m.add_function(wrap_pyfunction!(insert_rows_py, m)?)?;
    m.add_function(wrap_pyfunction!(delete_from_py, m)?)?;
    m.add_function(wrap_pyfunction!(check_migration_records_py, m)?)?;
    m.add_function(wrap_pyfunction!(apply_migration_records_py, m)?)?;
    m.add_function(wrap_pyfunction!(stamp_migration_records_py, m)?)?;
    m.add_function(wrap_pyfunction!(rollback_latest_migration_py, m)?)?;
    Ok(())
}
