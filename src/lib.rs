use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{postgres::PgPoolOptions, PgPool, Pool, Postgres, Row, Column, Transaction as SqlxTransaction};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{info, error, warn};

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
        match err {
            OrmxError::Database(e) => {
                let msg = e.to_string();
                if msg.contains("unique constraint") {
                    PyException::new_err(format!("IntegrityError: {}", msg))
                } else if msg.contains("foreign key") {
                    PyException::new_err(format!("IntegrityError: {}", msg))
                } else {
                    PyException::new_err(format!("QueryError: {}", msg))
                }
            }
            OrmxError::NotFound => PyException::new_err("NotFoundError: Record not found"),
            OrmxError::Engine(s) => PyRuntimeError::new_err(s),
        }
    }
}

#[pyclass]
pub struct Engine {
    pool: Arc<PgPool>,
}

#[pyclass]
pub struct Transaction {
    tx: Arc<Mutex<Option<SqlxTransaction<'static, Postgres>>>>,
}

#[pymethods]
impl Engine {
    #[staticmethod]
    fn connect(py: Python<'_>, url: String) -> PyResult<&PyAny> {
        future_into_py(py, async move {
            let pool = PgPoolOptions::new()
                .max_connections(20)
                .connect(&url)
                .await
                .map_err(OrmxError::from)?;
            
            info!("ORMX Engine connected to database");
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
            let start = Instant::now();
            let row = sqlx::query(&sql).fetch_optional(&*pool).await.map_err(OrmxError::from)?;
            let duration = start.elapsed();
            
            if duration.as_millis() > 100 {
                warn!(sql = %sql, duration = ?duration, "Slow query detected");
            }

            match row {
                Some(r) => {
                    let mut map = std::collections::HashMap::new();
                    for col in r.columns() {
                        let val: String = r.try_get(col.name()).unwrap_or_else(|_| "null".to_string());
                        map.insert(col.name().to_string(), val);
                    }
                    Ok(serde_json::to_string(&map).unwrap())
                }
                None => Err(OrmxError::NotFound.into()),
            }
        })
    }
}

#[pymethods]
impl Transaction {
    fn commit<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let tx_mutex = self.tx.clone();
        future_into_py(py, async move {
            let mut tx_opt = tx_mutex.lock().await;
            if let Some(tx) = tx_opt.take() {
                tx.commit().await.map_err(OrmxError::from)?;
                info!("Transaction committed");
                Ok(())
            } else {
                Err(OrmxError::Engine("Transaction already finalized".into()).into())
            }
        })
    }

    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let tx_mutex = self.tx.clone();
        future_into_py(py, async move {
            let mut tx_opt = tx_mutex.lock().await;
            if let Some(tx) = tx_opt.take() {
                tx.rollback().await.map_err(OrmxError::from)?;
                warn!("Transaction rolled back");
                Ok(())
            } else {
                Err(OrmxError::Engine("Transaction already finalized".into()).into())
            }
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<Transaction>()?;
    Ok(())
}
