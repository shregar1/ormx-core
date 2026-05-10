use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{Pool, Postgres, MySql, Sqlite, Row, Column, Any, any::AnyPoolOptions};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tracing::{info, warn};

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

#[pymethods]
impl Engine {
    #[staticmethod]
    fn connect(py: Python<'_>, url: String) -> PyResult<&PyAny> {
        future_into_py(py, async move {
            sqlx::any::install_default_drivers();
            let pool = AnyPoolOptions::new()
                .max_connections(20)
                .connect(&url)
                .await
                .map_err(OrmxError::from)?;
            
            info!("ORMX Engine connected (Multi-Dialect enabled)");
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

    fn fetch_one<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let row = sqlx::query(&sql).fetch_optional(&*pool).await.map_err(OrmxError::from)?;
            match row {
                Some(r) => {
                    let mut map = std::collections::HashMap::new();
                    for col in r.columns() {
                        // In sqlx Any, we might need to handle specific type mapping
                        // For MVP multi-dialect, we cast everything to string if possible
                        let val: String = r.try_get(col.name()).unwrap_or_else(|_| "null".to_string());
                        map.insert(col.name().to_string(), val);
                    }
                    Ok(serde_json::to_string(&map).unwrap())
                }
                None => Err(OrmxError::NotFound.into()),
            }
        })
    }

    fn fetch_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let rows = sqlx::query(&sql).fetch_all(&*pool).await.map_err(OrmxError::from)?;
            let mut results = Vec::new();
            for r in rows {
                let mut map = std::collections::HashMap::new();
                for col in r.columns() {
                    let val: String = r.try_get(col.name()).unwrap_or_else(|_| "null".to_string());
                    map.insert(col.name().to_string(), val);
                }
                results.push(map);
            }
            Ok(serde_json::to_string(&results).unwrap())
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    Ok(())
}
