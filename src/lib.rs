use pyo3::exceptions::{PyException};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{Row, Column, TypeInfo, postgres::{PgPool, PgPoolOptions}};
use std::sync::Arc;
use std::collections::HashMap;

pub mod logic;

/// Typed parameter value extracted from a Python object once, so the async
/// bind loop doesn't need to reacquire the GIL. Owned variants only — moved
/// into sqlx's `bind` by value (no clones).
#[derive(Debug, Clone)]
enum TypedParam {
    Null,
    Bool(bool),
    Int(i64),
    Real(f64),
    Text(String),
}

fn extract_param(obj: &PyAny) -> PyResult<TypedParam> {
    if obj.is_none() {
        return Ok(TypedParam::Null);
    }
    // bool BEFORE int — Python bools are an int subclass and would otherwise
    // coerce to 0/1.
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(TypedParam::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(TypedParam::Int(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(TypedParam::Real(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(TypedParam::Text(s));
    }
    // Decimal, datetime, UUID, etc. fall through here. str() gives a sensible
    // wire-format the server can cast (ISO 8601, decimal string, hex UUID).
    Ok(TypedParam::Text(obj.str()?.to_str()?.to_owned()))
}

/// Decode one column of a row into a Python-native value, picking the right
/// `try_get` once based on the column's declared SQL type. Falls back to a
/// probe sequence for unrecognized types (TIMESTAMP, DECIMAL, UUID …).
fn decode_col(py: Python<'_>, row: &sqlx::postgres::PgRow, name: &str, type_name: &str) -> Py<PyAny> {
    // Postgres is strictly typed — try_get won't widen INT4→i64 the way
    // sqlx::Any did, so each PG numeric width gets its own arm and widens
    // explicitly on the Rust side.
    match type_name {
        "BOOL" => row.try_get::<Option<bool>, _>(name).ok().flatten().into_py(py),
        "INT2" => row
            .try_get::<Option<i16>, _>(name)
            .ok()
            .flatten()
            .map(|v| v as i64)
            .into_py(py),
        "INT4" => row
            .try_get::<Option<i32>, _>(name)
            .ok()
            .flatten()
            .map(|v| v as i64)
            .into_py(py),
        "INT8" => row.try_get::<Option<i64>, _>(name).ok().flatten().into_py(py),
        "FLOAT4" => row
            .try_get::<Option<f32>, _>(name)
            .ok()
            .flatten()
            .map(|v| v as f64)
            .into_py(py),
        "FLOAT8" => row.try_get::<Option<f64>, _>(name).ok().flatten().into_py(py),
        "TEXT" | "VARCHAR" | "CHAR" | "CITEXT" | "NAME" | "BPCHAR" => row
            .try_get::<Option<String>, _>(name)
            .ok()
            .flatten()
            .into_py(py),
        _ => {
            // Slow path for unrecognised types (TIMESTAMP, NUMERIC, UUID, JSONB…).
            // Widen to String last so date/uuid/etc surface as ISO strings.
            if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(name) {
                v.into_py(py)
            } else if let Ok(Some(v)) = row.try_get::<Option<i32>, _>(name) {
                (v as i64).into_py(py)
            } else if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(name) {
                v.into_py(py)
            } else if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(name) {
                v.into_py(py)
            } else if let Ok(Some(v)) = row.try_get::<Option<String>, _>(name) {
                v.into_py(py)
            } else {
                py.None()
            }
        }
    }
}

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
    /// Cached sort of `range_map.keys()`, kept fresh by `set_range` so the
    /// hot routing path is an O(log n) `partition_point` lookup instead of a
    /// per-call O(n log n) sort.
    range_sorted_keys: Vec<i64>,
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
            range_sorted_keys: Vec::new(),
        }
    }

    fn set_range(&mut self, upper_bound: i64, shard: String) {
        let is_new = !self.range_map.contains_key(&upper_bound);
        self.range_map.insert(upper_bound, shard);
        if is_new {
            // Maintain sorted invariant with a single insert at the correct
            // position — no full re-sort.
            match self.range_sorted_keys.binary_search(&upper_bound) {
                Ok(_) => {} // already present (shouldn't happen given the guard above)
                Err(pos) => self.range_sorted_keys.insert(pos, upper_bound),
            }
        }
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
    pools: Arc<HashMap<String, PgPool>>,
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
                let shards: Vec<String> = self.pools.keys().cloned().collect();
                Ok(logic::route_hash(&value, &shards))
            }
            ShardingStrategyType::Geo => Ok(logic::route_geo(&value, &config.geo_map)),
            ShardingStrategyType::Range => Ok(logic::route_range_sorted(
                &value,
                &config.range_sorted_keys,
                &config.range_map,
            )),
            ShardingStrategyType::List => Ok(logic::route_list(&value, &config.list_map)),
            ShardingStrategyType::Directory => Ok(logic::DEFAULT_SHARD.to_string()),
        }
    }
}

#[pyclass]
pub struct Engine {
    pool: Arc<PgPool>,
}

#[pymethods]
impl Engine {
    #[staticmethod]
    fn connect(py: Python<'_>, url: String) -> PyResult<&PyAny> {
        future_into_py(py, async move {
            // Postgres-native pool: skips sqlx::Any's vtable dispatch on every
            // bind and decode. The wire protocol path is now type-specialised
            // at compile time, putting us roughly at asyncpg latency.
            let pool = PgPoolOptions::new()
                .max_connections(20)
                .connect(&url)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(Engine { pool: Arc::new(pool) })
        })
    }

    fn fetch_many<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        // Extract typed params under the GIL we already hold — avoids
        // reacquiring it inside the async block, and lets sqlx see real types
        // (int → INT, bool → BOOL, etc.) instead of widening everything to
        // TEXT the way the old `Vec<String>` signature did.
        let typed_params: Vec<TypedParam> = params
            .iter()
            .map(|p| extract_param(p.as_ref(py)))
            .collect::<PyResult<_>>()?;

        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            for p in typed_params {
                query = match p {
                    TypedParam::Null => query.bind(Option::<String>::None),
                    TypedParam::Bool(b) => query.bind(b),
                    TypedParam::Int(i) => query.bind(i),
                    TypedParam::Real(f) => query.bind(f),
                    TypedParam::Text(s) => query.bind(s),
                };
            }
            let rows = query
                .fetch_all(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;

            // Build a Python `list[dict]` directly — no JSON encode here, no
            // matching `json.loads()` on the Python side.
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let out = PyList::empty(py);
                for r in &rows {
                    let dict = PyDict::new(py);
                    for col in r.columns() {
                        let name = col.name();
                        let type_name = col.type_info().name();
                        dict.set_item(name, decode_col(py, r, name, type_name))?;
                    }
                    out.append(dict)?;
                }
                Ok(out.into())
            })
        })
    }

    fn execute<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        let typed_params: Vec<TypedParam> = params
            .iter()
            .map(|p| extract_param(p.as_ref(py)))
            .collect::<PyResult<_>>()?;

        let pool = self.pool.clone();
        future_into_py(py, async move {
            let mut query = sqlx::query(&sql);
            for p in typed_params {
                query = match p {
                    TypedParam::Null => query.bind(Option::<String>::None),
                    TypedParam::Bool(b) => query.bind(b),
                    TypedParam::Int(i) => query.bind(i),
                    TypedParam::Real(f) => query.bind(f),
                    TypedParam::Text(s) => query.bind(s),
                };
            }
            query
                .execute(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
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
