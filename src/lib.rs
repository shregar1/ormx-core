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
    /// Raw bytes — binds as ``BYTEA``. Previously these fell through to
    /// ``obj.str()`` (TEXT), silently corrupting non-UTF-8 binary data.
    Bytes(Vec<u8>),
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
    // ``bytes`` BEFORE ``str`` — PyO3's ``extract::<String>`` doesn't try
    // bytes, so the order matters only for clarity here. The check IS
    // needed because the old fallthrough (``obj.str()``) would call
    // ``str()`` on a ``bytes`` and produce ``"b'\\x00\\x01...'"`` — visibly
    // wrong on the wire.
    if let Ok(b) = obj.extract::<&[u8]>() {
        return Ok(TypedParam::Bytes(b.to_vec()));
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
            TypedParam::Bytes(b) => query.bind(b),
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
        "BYTEA" => row
            .try_get::<Option<Vec<u8>>, _>(name)
            .ok()
            .flatten()
            .map(|v| pyo3::types::PyBytes::new(py, &v).into_py(py))
            .unwrap_or_else(|| py.None()),
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
    // ``Mutex`` so ``add_shard`` can mutate after construction. The
    // pool registry is read on every ``route()`` call — usually after
    // every shard has been added, so contention is effectively zero.
    pools: Arc<std::sync::Mutex<HashMap<String, PgPool>>>,
    config: ShardingConfig,
}

#[pymethods]
impl Cluster {
    #[new]
    fn new(config: ShardingConfig) -> Self {
        Cluster {
            pools: Arc::new(std::sync::Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Connect to ``url`` and register the resulting pool under ``name``.
    ///
    /// Was a no-op stub since the original implementation — this is
    /// what makes the sharding feature actually do something. The
    /// pool is created with the same defaults as ``Engine::connect``;
    /// for finer control, build the ``Engine`` separately and shove
    /// it into the registry via a future ``add_engine`` method.
    fn add_shard<'p>(
        &mut self,
        py: Python<'p>,
        name: String,
        url: String,
    ) -> PyResult<&'p PyAny> {
        let pools = self.pools.clone();
        future_into_py(py, async move {
            let pool = PgPoolOptions::new()
                .max_connections(20)
                .connect(&url)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            pools.lock().unwrap().insert(name, pool);
            Ok(())
        })
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
                    Err(PyException::new_err(
                        "Custom strategy selected but no callback provided",
                    ))
                }
            }
            ShardingStrategyType::Hash => {
                let shards: Vec<String> =
                    self.pools.lock().unwrap().keys().cloned().collect();
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

    /// Run a multi-statement SQL script via the Postgres simple-query
    /// protocol.
    ///
    /// Unlike ``execute`` (which uses prepared statements + cannot
    /// handle multiple commands in one call), this routes through
    /// sqlx's ``raw_sql`` path — no Parse / Bind / Execute, just the
    /// raw text sent to the server. That makes it the right primitive
    /// for migration files (which mix CREATE TABLE / CREATE FUNCTION /
    /// ALTER TABLE statements) and for any DDL that contains
    /// dollar-quoted blocks (PL/pgSQL, JSON-with-semicolons in default
    /// values, etc).
    ///
    /// No parameter binding — the simple-query protocol doesn't support
    /// it. If you need bind parameters, use ``execute`` per-statement
    /// instead.
    fn execute_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            use sqlx::Executor;
            // ``execute_many`` returns a stream; we drain it for side
            // effects + accumulate rows-affected. The transaction-
            // boundary contract is the caller's: a Transaction-bound
            // execute_many is naturally inside the tx; an Engine-
            // bound one auto-commits each statement (Postgres default).
            let mut total: u64 = 0;
            let mut stream = pool.execute_many(sqlx::raw_sql(&sql));
            use futures::TryStreamExt;
            while let Some(result) = stream
                .try_next()
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?
            {
                total += result.rows_affected();
            }
            Ok(total)
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

    /// Multi-statement raw SQL inside the transaction.
    ///
    /// See ``Engine::execute_many`` for the rationale. Inside a
    /// transaction this gets the additional benefit that the whole
    /// script is rolled back atomically if any statement fails — which
    /// is exactly what the migration runner wants when applying a
    /// timestamp-prefixed file.
    fn execute_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.as_mut().ok_or_else(|| {
                PyException::new_err(
                    "Transaction already committed or rolled back; cannot execute_many",
                )
            })?;
            use futures::TryStreamExt;
            use sqlx::Executor;
            let mut total: u64 = 0;
            let mut stream = (&mut **tx).execute_many(sqlx::raw_sql(&sql));
            while let Some(result) = stream
                .try_next()
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?
            {
                total += result.rows_affected();
            }
            Ok(total)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite backend
// ---------------------------------------------------------------------------
//
// Mirror of ``Engine`` for ``sqlx::Sqlite``. We keep PG + SQLite as
// separate PyClasses (rather than a generic-over-backend ``Engine``)
// because:
//
//   1. PyO3 doesn't play well with generic ``#[pyclass]`` types — each
//      monomorphisation needs its own ``#[pyclass]`` entry.
//   2. The two backends have noticeably different type-decode paths
//      (SQLite has no BYTEA, no JSONB, no TIMESTAMPTZ, no NUMERIC).
//   3. URL-based dispatch is a four-line check in the Python wrapper.
//
// Most of the Engine impl is structurally identical; the row-decode
// path and the placeholder syntax differ. Bind params use ``?1``,
// ``?2``, etc. for SQLite (the Python ``get_placeholder`` already
// switches by dialect — that side is unchanged).

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};

fn decode_col_sqlite(py: Python<'_>, row: &SqliteRow, name: &str) -> Py<PyAny> {
    // SQLite is dynamically typed: every column has a "declared" type
    // but the actual storage class is one of INTEGER / REAL / TEXT /
    // BLOB / NULL. Probe order matters:
    //
    //  1. Integer (the most common case for IDs)
    //  2. Float
    //  3. Bool (would normally lose to integer, but sqlx maps the
    //     "BOOLEAN" declared type through directly when present)
    //  4. **String BEFORE Vec<u8>** — sqlx's ``try_get::<String>``
    //     succeeds on TEXT storage AND fails cleanly on BLOB (it
    //     validates UTF-8). If we tried Vec<u8> first, every TEXT
    //     column would come back as ``bytes`` because BLOB storage
    //     accepts anything.
    //  5. Vec<u8> for actual BLOB.
    if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(name) {
        return v.into_py(py);
    }
    if let Ok(Some(v)) = row.try_get::<Option<f64>, _>(name) {
        return v.into_py(py);
    }
    if let Ok(Some(v)) = row.try_get::<Option<bool>, _>(name) {
        return v.into_py(py);
    }
    if let Ok(Some(v)) = row.try_get::<Option<String>, _>(name) {
        return v.into_py(py);
    }
    if let Ok(Some(v)) = row.try_get::<Option<Vec<u8>>, _>(name) {
        return pyo3::types::PyBytes::new(py, &v).into_py(py);
    }
    py.None()
}

fn sqlite_rows_to_py_list(py: Python<'_>, rows: &[SqliteRow]) -> PyResult<Py<PyAny>> {
    let out = PyList::empty(py);
    for r in rows {
        let dict = PyDict::new(py);
        for col in r.columns() {
            let name = col.name();
            dict.set_item(name, decode_col_sqlite(py, r, name))?;
        }
        out.append(dict)?;
    }
    Ok(out.into())
}

fn bind_typed_sqlite<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    params: Vec<TypedParam>,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for p in params {
        query = match p {
            TypedParam::Null => query.bind(Option::<String>::None),
            TypedParam::Bool(b) => query.bind(b),
            TypedParam::Int(i) => query.bind(i),
            TypedParam::Real(f) => query.bind(f),
            TypedParam::Text(s) => query.bind(s),
            TypedParam::Bytes(b) => query.bind(b),
        };
    }
    query
}

#[pyclass]
pub struct SqliteEngine {
    pool: Arc<SqlitePool>,
}

#[pymethods]
impl SqliteEngine {
    #[staticmethod]
    fn connect(py: Python<'_>, url: String) -> PyResult<&PyAny> {
        future_into_py(py, async move {
            // ``SqliteConnectOptions::from_str`` parses both ``sqlite::memory:``
            // and ``sqlite:///path/to/file.db`` forms. ``create_if_missing``
            // mirrors the typical sqlx setup for local-dev convenience.
            let opts: SqliteConnectOptions = url
                .parse::<SqliteConnectOptions>()
                .map_err(|e| PyException::new_err(e.to_string()))?
                .create_if_missing(true);
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(opts)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(SqliteEngine {
                pool: Arc::new(pool),
            })
        })
    }

    fn close<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.close().await;
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
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let query = bind_typed_sqlite(sqlx::query(&sql), typed_params);
            let rows = query
                .fetch_all(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Python::with_gil(|py| sqlite_rows_to_py_list(py, &rows))
        })
    }

    fn execute<'p>(
        &self,
        py: Python<'p>,
        sql: String,
        params: Vec<PyObject>,
    ) -> PyResult<&'p PyAny> {
        let typed_params = extract_params(py, &params)?;
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let query = bind_typed_sqlite(sqlx::query(&sql), typed_params);
            let result = query
                .execute(&*pool)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(result.rows_affected())
        })
    }

    fn execute_many<'p>(&self, py: Python<'p>, sql: String) -> PyResult<&'p PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            use futures::TryStreamExt;
            use sqlx::Executor;
            let mut total: u64 = 0;
            let mut stream = pool.execute_many(sqlx::raw_sql(&sql));
            while let Some(result) = stream
                .try_next()
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?
            {
                total += result.rows_affected();
            }
            Ok(total)
        })
    }
}

#[pymodule]
fn ormx_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<SqliteEngine>()?;
    m.add_class::<Cluster>()?;
    m.add_class::<ShardingStrategyType>()?;
    m.add_class::<ShardingConfig>()?;
    Ok(())
}
