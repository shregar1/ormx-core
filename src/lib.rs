use pyo3::exceptions::{PyException};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{Row, Column, any::AnyPoolOptions, AnyPool};
use std::sync::Arc;
use std::collections::HashMap;
use serde_json::{Value as JsonValue, json};

#[pyclass]
#[derive(Clone, Copy)]
pub enum ShardingStrategyType {
    Hash,
    Range,
    Geo,
    List,
    Directory,
    Custom,
}

#[pyclass]
#[derive(Clone)]
pub struct ShardingConfig {
    pub strategy: ShardingStrategyType,
    pub range_map: HashMap<i64, String>,
    pub geo_map: HashMap<String, String>,
    pub list_map: HashMap<String, String>,
    pub directory_url: Option<String>,
    pub custom_callback: Option<PyObject>,
}

#[pymethods]
impl ShardingConfig {
    #[new]
    fn new(strategy: ShardingStrategyType) -> Self {
        ShardingConfig {
            strategy,
            range_map: HashMap::new(),
            geo_map: HashMap::new(),
            list_map: HashMap::new(),
            directory_url: None,
            custom_callback: None,
        }
    }

    fn set_range(&mut self, upper_bound: i64, shard: String) {
        self.range_map.insert(upper_bound, shard);
    }

    fn set_geo(&mut self, region: String, shard: String) {
        self.geo_map.insert(region, shard);
    }

    fn set_list(&mut self, key: String, shard: String) {
        self.list_map.insert(key, shard);
    }

    fn set_directory(&mut self, url: String) {
        self.directory_url = Some(url);
    }

    fn set_custom(&mut self, callback: PyObject) {
        self.custom_callback = Some(callback);
    }
}

#[pyclass]
pub struct Cluster {
    pools: Arc<HashMap<String, AnyPool>>,
    config: ShardingConfig,
}

#[pymethods]
impl Cluster {
    #[new]
    fn new(config: ShardingConfig) -> Self {
        Cluster {
            pools: Arc::new(HashMap::new()),
            config,
        }
    }

    fn add_shard(&mut self, _name: String, _url: String) {
    }

    fn route(&self, py: Python<'_>, value: String) -> PyResult<String> {
        let config = &self.config;
        match config.strategy {
            ShardingStrategyType::Custom => {
                if let Some(callback) = &config.custom_callback {
                    let res = callback.call1(py, (value,))?;
                    let shard: String = res.extract(py)?;
                    Ok(shard)
                } else {
                    Err(PyException::new_err("Custom strategy selected but no callback provided"))
                }
            }
            ShardingStrategyType::Hash => {
                let hash = md5::compute(value);
                let shards: Vec<_> = self.pools.keys().collect();
                if shards.is_empty() { return Ok("default".to_string()); }
                Ok(shards[(hash[0] as usize) % shards.len()].clone())
            }
            ShardingStrategyType::Geo => {
                Ok(config.geo_map.get(&value).cloned().unwrap_or_else(|| "default".to_string()))
            }
            ShardingStrategyType::Range => {
                let val: i64 = value.parse().unwrap_or(0);
                let mut sorted_keys: Vec<_> = config.range_map.keys().cloned().collect();
                sorted_keys.sort();
                for k in sorted_keys {
                    if val <= k {
                        return Ok(config.range_map.get(&k).unwrap().clone());
                    }
                }
                Ok("default".to_string())
            }
            ShardingStrategyType::List => {
                Ok(config.list_map.get(&value).cloned().unwrap_or_else(|| "default".to_string()))
            }
            ShardingStrategyType::Directory => {
                Ok("default".to_string())
            }
        }
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
            
            // Legacy & Enterprise Protocol Mapping
            let mut normalized_url = url.clone();
            if url.starts_with("oracle://") || url.starts_with("db2://") || url.starts_with("access://") {
                // In an enterprise environment, we use a specialized ODBC/Native Bridge
                // For the universal driver, we map to any-compatible ODBC strings if supported
                // For now, we normalize to ensure driver registration is handled
                normalized_url = url.replace("oracle://", "odbc://").replace("db2://", "odbc://").replace("access://", "odbc://");
            }

            let pool = AnyPoolOptions::new()
                .max_connections(20)
                .connect(&normalized_url)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn fetch_many<'p>(&self, py: Python<'p>, sql: String, params: Vec<String>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            for p in params {
                query = query.bind(p);
            }
            let rows = query.fetch_all(&*pool).await.map_err(|e| PyException::new_err(e.to_string()))?;
            let mut results = Vec::new();
            for r in rows {
                let mut map = serde_json::Map::new();
                for col in r.columns() {
                    let name = col.name();
                    // Try to get as i64, then String, then fallback to null
                    let val = if let Ok(v) = r.try_get::<i64, _>(name) {
                        json!(v)
                    } else if let Ok(v) = r.try_get::<String, _>(name) {
                        json!(v)
                    } else if let Ok(v) = r.try_get::<f64, _>(name) {
                        json!(v)
                    } else if let Ok(v) = r.try_get::<bool, _>(name) {
                        json!(v)
                    } else {
                        JsonValue::Null
                    };
                    map.insert(name.to_string(), val);
                }
                results.push(JsonValue::Object(map));
            }
            Ok(serde_json::to_string(&results).unwrap())
        })
    }

    fn execute<'p>(&self, py: Python<'p>, sql: String, params: Vec<String>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            for p in params {
                query = query.bind(p);
            }
            query.execute(&*pool).await.map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<Cluster>()?;
    m.add_class::<ShardingStrategyType>()?;
    m.add_class::<ShardingConfig>()?;
    Ok(())
}
