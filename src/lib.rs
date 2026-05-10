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
}

#[pyclass]
#[derive(Clone)]
pub struct ShardingConfig {
    pub strategy: ShardingStrategyType,
    pub range_map: HashMap<i64, String>,
    pub geo_map: HashMap<String, String>,
    pub list_map: HashMap<String, String>,
    pub directory_url: Option<String>,
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

    fn route(&self, value: String) -> String {
        let config = &self.config;
        match config.strategy {
            ShardingStrategyType::Hash => {
                let hash = md5::compute(value);
                let shards: Vec<_> = self.pools.keys().collect();
                if shards.is_empty() { return "default".to_string(); }
                shards[(hash[0] as usize) % shards.len()].clone()
            }
            ShardingStrategyType::Geo => {
                config.geo_map.get(&value).cloned().unwrap_or_else(|| "default".to_string())
            }
            ShardingStrategyType::Range => {
                let val: i64 = value.parse().unwrap_or(0);
                let mut sorted_keys: Vec<_> = config.range_map.keys().cloned().collect();
                sorted_keys.sort();
                for k in sorted_keys {
                    if val <= k {
                        return config.range_map.get(&k).unwrap().clone();
                    }
                }
                "default".to_string()
            }
            ShardingStrategyType::List => {
                config.list_map.get(&value).cloned().unwrap_or_else(|| "default".to_string())
            }
            ShardingStrategyType::Directory => {
                "default".to_string()
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
            let pool = AnyPoolOptions::new()
                .max_connections(20)
                .connect(&url)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn fetch_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let rows = sqlx::query(&sql).fetch_all(&*pool).await.map_err(|e| PyException::new_err(e.to_string()))?;
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
    m.add_class::<ShardingStrategyType>()?;
    m.add_class::<ShardingConfig>()?;
    Ok(())
}
