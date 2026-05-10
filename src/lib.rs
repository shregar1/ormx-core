use pyo3::exceptions::{PyException};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{Row, Column, any::AnyPoolOptions};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tracing::{info, warn};
use serde_json::{Value as JsonValue, json};

#[derive(Error, Debug)]
pub enum OrmxError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Engine error: {0}")]
    Engine(String),
    #[error("Not found")]
    NotFound,
}

impl From<OrmxError> for PyErr {
    fn from(err: OrmxError) -> PyErr {
        PyException::new_err(err.to_string())
    }
}

#[pyclass]
pub struct Engine {
    pool: Arc<sqlx::AnyPool>,
}

fn row_to_json(r: &sqlx::any::AnyRow) -> JsonValue {
    let mut map = serde_json::Map::new();
    for col in r.columns() {
        let name = col.name();
        // Standardize on string representation for sqlx::AnyRow to avoid complex trait issues
        // In a production scenario, we would use a more sophisticated type-matching system
        let val = if let Ok(v) = r.try_get::<String, _>(name) {
            json!(v)
        } else if let Ok(v) = r.try_get::<i32, _>(name) {
            json!(v)
        } else if let Ok(v) = r.try_get::<bool, _>(name) {
            json!(v)
        } else {
            json!(null)
        };
        map.insert(name.to_string(), val);
    }
    JsonValue::Object(map)
}

#[pymethods]
impl Engine {
    #[staticmethod]
    fn connect(py: Python<'_>, url: String) -> PyResult<&PyAny> {
        future_into_py(py, async move {
            sqlx::any::install_default_drivers();
            
            let mut normalized_url = url.clone();
            if url.starts_with("cockroachdb://") {
                normalized_url = url.replace("cockroachdb://", "postgres://");
            } else if url.starts_with("planetscale://") {
                normalized_url = url.replace("planetscale://", "mysql://");
            } else if url.starts_with("tidb://") {
                normalized_url = url.replace("tidb://", "mysql://");
            } else if url.starts_with("yugabyte://") || url.starts_with("yugabytedb://") {
                normalized_url = url.replace("yugabyte://", "postgres://").replace("yugabytedb://", "postgres://");
            } else if url.starts_with("neon://") {
                normalized_url = url.replace("neon://", "postgres://");
            } else if url.starts_with("libsql://") {
                normalized_url = url.replace("libsql://", "sqlite://");
            }

            let pool = AnyPoolOptions::new()
                .max_connections(20)
                .connect(&normalized_url)
                .await
                .map_err(OrmxError::from)?;
            
            info!("ORMX Engine connected (Universal Protocol Mapping enabled)");
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn execute<'p>(&self, py: Python<'p>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let start = Instant::now();
            let mut query = sqlx::query(&sql);
            if let Some(p) = params {
                for param in p {
                    let s: String = Python::with_gil(|py| {
                        param.extract::<String>(py).unwrap_or_else(|_| "null".to_string())
                    });
                    query = query.bind(s);
                }
            }
            let result = query.execute(&*pool).await.map_err(OrmxError::from)?;
            let duration = start.elapsed();
            if duration.as_millis() > 100 {
                warn!(sql = %sql, duration = ?duration, "Slow query detected");
            }
            Ok(result.rows_affected())
        })
    }

    fn fetch_one<'p>(&self, py: Python<'p>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            if let Some(p) = params {
                for param in p {
                    let s: String = Python::with_gil(|py| {
                        param.extract::<String>(py).unwrap_or_else(|_| "null".to_string())
                    });
                    query = query.bind(s);
                }
            }
            let row = query.fetch_optional(&*pool).await.map_err(OrmxError::from)?;
            match row {
                Some(r) => {
                    let json_val = row_to_json(&r);
                    Ok(serde_json::to_string(&json_val).unwrap())
                }
                None => Err(OrmxError::NotFound.into()),
            }
        })
    }

    fn fetch_many<'p>(&self, py: Python<'p>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            if let Some(p) = params {
                for param in p {
                    let s: String = Python::with_gil(|py| {
                        param.extract::<String>(py).unwrap_or_else(|_| "null".to_string())
                    });
                    query = query.bind(s);
                }
            }
            let rows = query.fetch_all(&*pool).await.map_err(OrmxError::from)?;
            let results: Vec<JsonValue> = rows.iter().map(row_to_json).collect();
            Ok(serde_json::to_string(&results).unwrap())
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    Ok(())
}
