use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_asyncio::tokio::future_into_py;
use sqlx::{
    postgres::{PgPool, PgPoolOptions, PgRow},
    Column, Postgres, Row, TypeInfo,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

pub mod logic;

// ---------------------------------------------------------------------------
// Parameter extraction / binding (shared by Engine + Transaction)
// ---------------------------------------------------------------------------

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

fn extract_params(py: Python<'_>, params: &[PyObject]) -> PyResult<Vec<TypedParam>> {
    params
        .iter()
        .map(|p| extract_param(p.as_ref(py)))
        .collect()
}

/// Bind a sequence of typed params onto a sqlx Query. Generic over the
/// arguments type (`PgArguments`) so it works for both pool and tx queries.
fn bind_typed<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    params: Vec<TypedParam>,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for p in params {
        query = match p {
            TypedParam::Null => query.bind(Option::<String>::None),
            TypedParam::Bool(b) => query.bind(b),
            TypedParam::Int(i) => query.bind(i),
            TypedParam::Real(f) => query.bind(f),
            TypedParam::Text(s) => query.bind(s),
        };
    }
    query
}

// ---------------------------------------------------------------------------
// Row decoding
// ---------------------------------------------------------------------------

/// Decode one column of a row into a Python-native value, picking the right
/// `try_get` once based on the column's declared SQL type. Falls back to a
/// probe sequence for unrecognized types (TIMESTAMP, DECIMAL, UUID …).
fn decode_col(py: Python<'_>, row: &PgRow, name: &str, type_name: &str) -> Py<PyAny> {
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

fn rows_to_py_list(py: Python<'_>, rows: &[PgRow]) -> PyResult<Py<PyAny>> {
    let out = PyList::empty(py);
    for r in rows {
        let dict = PyDict::new(py);
        for col in r.columns() {
            let name = col.name();
            let type_name = col.type_info().name();
            dict.set_item(name, decode_col(py, r, name, type_name))?;
        }
        out.append(dict)?;
    }
    Ok(out.into())
}

// ---------------------------------------------------------------------------
// Sharding (unchanged from previous revision)
// ---------------------------------------------------------------------------

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
            match self.range_sorted_keys.binary_search(&upper_bound) {
                Ok(_) => {}
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

    fn add_shard(&mut self, _name: String, _url: String) {}

    fn route(&self, py: Python<'_>, value: String) -> PyResult<String> {
        let config = &self.config;
        match config.strategy {
            ShardingStrategyType::Custom => {
                if let Some(callback) = &config.custom_callback {
                    let res = callback.call1(py, (value,))?;
                    let shard: String = res.extract(py)?;
                    Ok(shard)
                } else {
                    Err(PyException::new_err(
                        "Custom strategy selected but no callback provided",
                    ))
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

// ---------------------------------------------------------------------------
// Engine + Transaction
// ---------------------------------------------------------------------------

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
            Ok(Engine {
                pool: Arc::new(pool),
            })
        })
    }

    /// Close all pooled connections. Idempotent — safe to call repeatedly,
    /// and a no-op once the pool's already closed.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.close().await;
            Ok(())
        })
    }

    /// Begin a transaction. Returns a [`Transaction`] handle that owns a
    /// pooled connection until the user commits, rolls back, or drops it.
    ///
    /// Concurrent reads on the pool (other handlers) still get their own
    /// connections — only this transaction's writes are isolated.
    fn begin<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let tx = pool
                .begin()
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(Transaction {
                inner: Arc::new(TokioMutex::new(Some(tx))),
            })
        })
    }

    fn fetch_many<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        // Extract typed params under the GIL we already hold — avoids
        // reacquiring it inside the async block.
        let typed_params = extract_params(py, &params)?;
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let query = bind_typed(sqlx::query(&sql), typed_params);
            let rows = query
                .fetch_all(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Python::with_gil(|py| rows_to_py_list(py, &rows))
        })
    }

    /// Run an INSERT/UPDATE/DELETE/DDL statement. Returns the number of
    /// rows affected (matches the contract the Python ``Query.update`` /
    /// ``Query.delete`` methods rely on).
    fn execute<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        let typed_params = extract_params(py, &params)?;
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let query = bind_typed(sqlx::query(&sql), typed_params);
            let result = query
                .execute(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(result.rows_affected())
        })
    }
}

/// A live DB transaction.
///
/// Wraps a ``sqlx::Transaction<'static, Postgres>`` inside an ``Arc<Mutex<…>>``
/// so it can move across the PyO3 boundary and be operated on from
/// concurrent async code (the GIL serializes Python-side access, but
/// the underlying transaction may be touched from multiple `await`
/// points within a handler). Once committed or rolled back the inner
/// `Option` becomes `None`; subsequent calls are no-ops rather than
/// panics, so the Python `__aexit__` cleanup path can't bite users.
#[pyclass]
pub struct Transaction {
    inner: Arc<TokioMutex<Option<sqlx::Transaction<'static, Postgres>>>>,
}

#[pymethods]
impl Transaction {
    /// Commit the transaction. If already committed/rolled back, returns
    /// immediately — making the Python context-manager cleanup idempotent.
    fn commit<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(tx) = guard.take() {
                tx.commit()
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;
            }
            Ok(())
        })
    }

    /// Roll back the transaction. Idempotent — same contract as `commit`.
    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(tx) = guard.take() {
                tx.rollback()
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn fetch_many<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        let typed_params = extract_params(py, &params)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.as_mut().ok_or_else(|| {
                PyException::new_err(
                    "Transaction already committed or rolled back; cannot fetch_many",
                )
            })?;
            let query = bind_typed(sqlx::query(&sql), typed_params);
            let rows = query
                .fetch_all(&mut **tx)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Python::with_gil(|py| rows_to_py_list(py, &rows))
        })
    }

    fn execute<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        let typed_params = extract_params(py, &params)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.as_mut().ok_or_else(|| {
                PyException::new_err(
                    "Transaction already committed or rolled back; cannot execute",
                )
            })?;
            let query = bind_typed(sqlx::query(&sql), typed_params);
            let result = query
                .execute(&mut **tx)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(result.rows_affected())
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<Cluster>()?;
    m.add_class::<ShardingStrategyType>()?;
    m.add_class::<ShardingConfig>()?;
    Ok(())
}
