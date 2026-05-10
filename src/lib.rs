use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{postgres::PgPoolOptions, PgPool, Pool, Postgres, Row, Column, Transaction as SqlxTransaction};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Error, Debug)]
pub enum OrmxError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Engine error: {0}")]
    Engine(String),
}

impl From<OrmxError> for PyErr {
    fn from(err: OrmxError) -> PyErr {
        PyException::new_err(err.to_string())
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
            
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn begin<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let tx = pool.begin().await.map_err(OrmxError::from)?;
            Ok(Transaction { tx: Arc::new(Mutex::new(Some(tx))) })
        })
    }

    fn execute<'p>(&self, py: Python<'p>, sql: String, params: Option<Vec<PyObject>>) -> PyResult<&'p PyAny> {
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
            let result = query.execute(&*pool).await.map_err(OrmxError::from)?;
            Ok(result.rows_affected())
        })
    }

    fn fetch_one<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let row = sqlx::query(&sql).fetch_one(&*pool).await.map_err(OrmxError::from)?;
            let mut map = std::collections::HashMap::new();
            for col in row.columns() {
                let val: String = row.try_get(col.name()).unwrap_or_else(|_| "null".to_string());
                map.insert(col.name().to_string(), val);
            }
            Ok(serde_json::to_string(&map).unwrap())
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
