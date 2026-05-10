use pyo3::exceptions::{PyException};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{Row, Column, any::AnyPoolOptions, AnyPool};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;
use thiserror::Error;
use serde_json::{Value as JsonValue, json};

#[derive(Error, Debug)]
pub enum OrmxError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Shard error: {0}")]
    ShardNotFound(String),
}

impl From<OrmxError> for PyErr {
    fn from(err: OrmxError) -> PyErr {
        PyException::new_err(err.to_string())
    }
}

#[pyclass]
#[derive(Clone, Copy)]
pub enum ShardingStrategy {
    Hash,
    Range,
    Geo,
}

#[pyclass]
pub struct Cluster {
    shards: Arc<HashMap<String, AnyPool>>,
    strategy: ShardingStrategy,
}

#[pymethods]
impl Cluster {
    #[new]
    fn new(strategy: ShardingStrategy) -> Self {
        Cluster {
            shards: Arc::new(HashMap::new()),
            strategy,
        }
    }

    fn add_shard(&mut self, name: String, url: String) -> PyResult<()> {
        // We use a simplified connection for the MVP
        Ok(())
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
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn execute<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let result = sqlx::query(&sql).execute(&*pool).await.map_err(OrmxError::from)?;
            Ok(result.rows_affected())
        })
    }

    fn fetch_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let rows = sqlx::query(&sql).fetch_all(&*pool).await.map_err(OrmxError::from)?;
            let mut results = Vec::new();
            for r in rows {
                let mut map = HashMap::new();
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
    m.add_class::<Cluster>()?;
    m.add_class::<ShardingStrategy>()?;
    Ok(())
}
