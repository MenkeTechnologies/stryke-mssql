//! stryke-mssql — Microsoft SQL Server cdylib loaded in-process by stryke via
//! dlopen.
//!
//! Each `#[no_mangle] extern "C" fn mssql__*` is a JSON-string-in /
//! JSON-string-out wrapper around `tiberius` (the pure-Rust TDS driver).
//! tiberius is async, so this cdylib owns one multi-thread tokio runtime and
//! presents a **blocking facade**: every handler `block_on`s the async call.
//! A `Client` is cached per `(host, port, db, auth, encrypt)` for the life of
//! the stryke process; a connection that errors is evicted so the next call
//! reconnects.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Query, Row};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type Conn = Client<Compat<TcpStream>>;

// ── runtime + connection cache ──────────────────────────────────────────────

static RT: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("build tokio runtime"))
}

static CONNS: OnceCell<std::sync::Mutex<HashMap<ConnKey, Arc<AsyncMutex<Conn>>>>> = OnceCell::new();

fn conns() -> &'static std::sync::Mutex<HashMap<ConnKey, Arc<AsyncMutex<Conn>>>> {
    CONNS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    url: String,
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    encrypt: String,
    trust: bool,
}

fn conn_key(opts: &Value) -> ConnKey {
    ConnKey {
        url: opts
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        host: opts
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1")
            .to_string(),
        port: opts.get("port").and_then(|v| v.as_u64()).unwrap_or(1433) as u16,
        database: opts
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        username: opts
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        password: opts
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        encrypt: opts
            .get("encrypt")
            .and_then(|v| v.as_str())
            .unwrap_or("required")
            .to_string(),
        trust: opts
            .get("trust_cert")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    }
}

fn config_for(key: &ConnKey) -> Result<Config> {
    if !key.url.is_empty() {
        return Config::from_ado_string(&key.url).map_err(|e| anyhow!("parse url: {e}"));
    }
    let mut config = Config::new();
    config.host(&key.host);
    config.port(key.port);
    if !key.database.is_empty() {
        config.database(&key.database);
    }
    config.authentication(AuthMethod::sql_server(&key.username, &key.password));
    config.encryption(match key.encrypt.as_str() {
        "off" => EncryptionLevel::Off,
        "not_supported" => EncryptionLevel::NotSupported,
        _ => EncryptionLevel::Required,
    });
    if key.trust {
        config.trust_cert();
    }
    Ok(config)
}

async fn connect(key: &ConnKey) -> Result<Conn> {
    let config = config_for(key)?;
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| anyhow!("connect {}: {e}", config.get_addr()))?;
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| anyhow!("tds handshake: {e}"))
}

/// Get (or open) the cached client for these opts and run `f` against it.
/// On error the connection is evicted so a later call reconnects cleanly.
fn with_client<T, F, Fut>(opts: &Value, f: F) -> Result<T>
where
    F: FnOnce(Arc<AsyncMutex<Conn>>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let key = conn_key(opts);
    rt().block_on(async {
        let handle = {
            let existing = conns().lock().unwrap().get(&key).cloned();
            match existing {
                Some(h) => h,
                None => {
                    let client = connect(&key).await?;
                    let h = Arc::new(AsyncMutex::new(client));
                    conns().lock().unwrap().insert(key.clone(), Arc::clone(&h));
                    h
                }
            }
        };
        let out = f(Arc::clone(&handle)).await;
        if out.is_err() {
            conns().lock().unwrap().remove(&key);
        }
        out
    })
}

// ── query building + row conversion ─────────────────────────────────────────

/// Bind JSON params onto a Query as `@P1`, `@P2`, … placeholders.
fn bind_params<'a>(mut q: Query<'a>, params: &'a [Value]) -> Query<'a> {
    for p in params {
        match p {
            Value::Null => q.bind(Option::<i32>::None),
            Value::Bool(b) => q.bind(*b),
            Value::Number(n) if n.is_i64() => q.bind(n.as_i64().unwrap()),
            Value::Number(n) if n.is_u64() => q.bind(n.as_u64().unwrap() as i64),
            Value::Number(n) => q.bind(n.as_f64().unwrap()),
            Value::String(s) => q.bind(s.as_str()),
            other => q.bind(other.to_string()),
        }
    }
    q
}

fn params_of(v: &Value) -> Vec<Value> {
    v.get("params")
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default()
}

/// Convert one cell to JSON by trying each TDS type in turn — robust without a
/// full ColumnType match. Order matters: most specific first.
fn cell_to_json(row: &Row, i: usize) -> Value {
    if let Ok(v) = row.try_get::<bool, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<i32, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<i64, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<f32, _>(i) {
        return v.map_or(Value::Null, |x| json!(x as f64));
    }
    if let Ok(v) = row.try_get::<f64, _>(i) {
        return v.map_or(Value::Null, Value::from);
    }
    if let Ok(v) = row.try_get::<&str, _>(i) {
        return v.map_or(Value::Null, |s| json!(s));
    }
    if let Ok(v) = row.try_get::<rust_decimal::Decimal, _>(i) {
        return v.map_or(Value::Null, |d| json!(d.to_string()));
    }
    if let Ok(v) = row.try_get::<uuid::Uuid, _>(i) {
        return v.map_or(Value::Null, |u| json!(u.to_string()));
    }
    if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(i) {
        return v.map_or(Value::Null, |d| {
            json!(d.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        });
    }
    if let Ok(v) = row.try_get::<chrono::NaiveDate, _>(i) {
        return v.map_or(Value::Null, |d| json!(d.to_string()));
    }
    if let Ok(v) = row.try_get::<chrono::NaiveTime, _>(i) {
        return v.map_or(Value::Null, |d| json!(d.to_string()));
    }
    if let Ok(v) = row.try_get::<&[u8], _>(i) {
        return v.map_or(Value::Null, |b| json!(B64.encode(b)));
    }
    Value::Null
}

fn row_to_json(row: &Row) -> Value {
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        obj.insert(col.name().to_string(), cell_to_json(row, i));
    }
    Value::Object(obj)
}

async fn run_query(
    conn: &Arc<AsyncMutex<Conn>>,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Value>> {
    let mut client = conn.lock().await;
    let q = bind_params(Query::new(sql), params);
    let stream = q
        .query(&mut client)
        .await
        .map_err(|e| anyhow!("query: {e}"))?;
    let rows = stream
        .into_first_result()
        .await
        .map_err(|e| anyhow!("read rows: {e}"))?;
    Ok(rows.iter().map(row_to_json).collect())
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-mssql handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
/// `p` must be a pointer previously returned by an export, or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + liveness ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mssql__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn mssql__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_client(
            &v,
            |c| async move { run_query(&c, "SELECT 1 AS ok", &[]).await },
        )?;
        Ok(json!({ "value": !rows.is_empty() }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__server_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_client(&v, |c| async move {
            run_query(&c, "SELECT @@VERSION AS version", &[]).await
        })?;
        Ok(json!({ "value": rows.first().and_then(|r| r.get("version")).cloned() }))
    })
}

// ── query / execute ─────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mssql__query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_client(&v, |c| async move { run_query(&c, &sql, &params).await })?;
        Ok(json!({ "rows": rows }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__query_one(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_client(&v, |c| async move { run_query(&c, &sql, &params).await })?;
        Ok(json!({ "row": rows.into_iter().next() }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__scalar(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_client(&v, |c| async move { run_query(&c, &sql, &params).await })?;
        let value = rows
            .first()
            .and_then(|r| r.as_object())
            .and_then(|o| o.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({ "value": value }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params = params_of(&v);
        let rows = with_client(&v, |c| async move { run_query(&c, &sql, &params).await })?;
        Ok(json!({ "value": !rows.is_empty() }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__execute(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params = params_of(&v);
        let affected = with_client(&v, |c| async move {
            let mut client = c.lock().await;
            let q = bind_params(Query::new(&sql), &params);
            let res = q
                .execute(&mut client)
                .await
                .map_err(|e| anyhow!("execute: {e}"))?;
            Ok(res.rows_affected().iter().sum::<u64>())
        })?;
        Ok(json!({ "affected": affected }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__simple_query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let rows = with_client(&v, |c| async move {
            let mut client = c.lock().await;
            let stream = client
                .simple_query(sql)
                .await
                .map_err(|e| anyhow!("simple_query: {e}"))?;
            let rows = stream
                .into_first_result()
                .await
                .map_err(|e| anyhow!("read: {e}"))?;
            Ok(rows.iter().map(row_to_json).collect::<Vec<_>>())
        })?;
        Ok(json!({ "rows": rows }))
    })
}

/// Run several statements inside one transaction on a single connection.
/// Commits on success, rolls back if any statement errors.
#[no_mangle]
pub extern "C" fn mssql__batch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let statements: Vec<String> = v
            .get("statements")
            .and_then(|s| s.as_array())
            .ok_or_else(|| anyhow!("missing statements array"))?
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect();
        let affected = with_client(&v, |c| async move {
            let mut client = c.lock().await;
            client
                .simple_query("BEGIN TRANSACTION")
                .await
                .map_err(|e| anyhow!("begin: {e}"))?
                .into_results()
                .await
                .ok();
            let mut total = 0u64;
            for sql in &statements {
                match client.execute(sql, &[]).await {
                    Ok(r) => total += r.rows_affected().iter().sum::<u64>(),
                    Err(e) => {
                        client.simple_query("ROLLBACK").await.ok();
                        return Err(anyhow!("statement failed (rolled back): {e}"));
                    }
                }
            }
            client
                .simple_query("COMMIT")
                .await
                .map_err(|e| anyhow!("commit: {e}"))?;
            Ok(total)
        })?;
        Ok(json!({ "affected": affected }))
    })
}

// ── introspection ───────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mssql__databases(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_client(&v, |c| async move {
            run_query(&c, "SELECT name FROM sys.databases ORDER BY name", &[]).await
        })?;
        Ok(json!({ "rows": rows }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let rows = with_client(&v, |c| async move {
            run_query(
                &c,
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME",
                &[],
            )
            .await
        })?;
        Ok(json!({ "rows": rows }))
    })
}

#[no_mangle]
pub extern "C" fn mssql__columns(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = v["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?
            .to_string();
        let params = vec![Value::String(table)];
        let rows = with_client(&v, |c| async move {
            run_query(
                &c,
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH \
                 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @P1 ORDER BY ORDINAL_POSITION",
                &params,
            )
            .await
        })?;
        Ok(json!({ "rows": rows }))
    })
}

// ── pure URL helpers ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn mssql__parse_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
        Ok(parse_ado(url))
    })
}

/// Replace the password in an ADO/JDBC connection string with `***`.
#[no_mangle]
pub extern "C" fn mssql__redact_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
        Ok(json!({ "value": redact_ado(url) }))
    })
}

/// Build an ADO connection string from a components map (inverse of parse_url).
#[no_mangle]
pub extern "C" fn mssql__build_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let parts = v
            .get("parts")
            .and_then(|x| x.as_object())
            .ok_or_else(|| anyhow!("missing parts object"))?;
        Ok(json!({ "value": build_ado(parts) }))
    })
}

/// Quote a T-SQL identifier as `[name]` (doubling `]`), for names that can't be
/// parameterized.
#[no_mangle]
pub extern "C" fn mssql__quote_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = v
            .get("name")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("missing name"))?;
        Ok(json!({ "value": quote_ident(name) }))
    })
}

/// Split a T-SQL script into batches on `GO` separator lines.
#[no_mangle]
pub extern "C" fn mssql__split_batch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let script = v
            .get("script")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("missing script"))?;
        Ok(json!({ "batches": split_batch(script) }))
    })
}

/// Build an ADO-style connection string from a components map. Keys are emitted
/// in a stable order; unknown keys pass through verbatim after the known set.
fn build_ado(v: &serde_json::Map<String, Value>) -> String {
    const KNOWN: [(&str, &str); 6] = [
        ("server", "Server"),
        ("database", "Database"),
        ("user_id", "User Id"),
        ("password", "Password"),
        ("encrypt", "Encrypt"),
        ("trust_server_certificate", "TrustServerCertificate"),
    ];
    let mut parts = Vec::new();
    for (key, label) in KNOWN {
        if let Some(val) = v.get(key).map(value_str) {
            if !val.is_empty() {
                parts.push(format!("{label}={val}"));
            }
        }
    }
    let mut extra: Vec<&String> = v
        .keys()
        .filter(|k| !KNOWN.iter().any(|(known, _)| *known == k.as_str()))
        .collect();
    extra.sort();
    for k in extra {
        parts.push(format!("{k}={}", value_str(&v[k])));
    }
    parts.join(";")
}

/// Render a JSON scalar to its connection-string text.
fn value_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Quote a T-SQL identifier as `[name]`, doubling any `]`. Use for identifiers
/// (table/column names) that can't be parameterized.
fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Split a T-SQL script into batches on lines containing only `GO` (the SSMS
/// batch separator, case-insensitive). Empty batches are dropped.
fn split_batch(script: &str) -> Vec<String> {
    let mut batches = Vec::new();
    let mut cur = String::new();
    for line in script.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            if !cur.trim().is_empty() {
                batches.push(cur.trim().to_string());
            }
            cur.clear();
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        batches.push(cur.trim().to_string());
    }
    batches
}

// ── pure logic (unit-tested) ────────────────────────────────────────────────

/// Parse an ADO-style `key=value;key=value` connection string into a JSON map
/// (keys lowercased; `password`/`pwd` redacted).
fn parse_ado(url: &str) -> Value {
    let mut obj = serde_json::Map::new();
    for pair in url.split(';') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((k, val)) = pair.split_once('=') {
            let key = k.trim().to_lowercase();
            let value = if matches!(key.as_str(), "password" | "pwd") {
                "***".to_string()
            } else {
                val.trim().to_string()
            };
            obj.insert(key, Value::String(value));
        }
    }
    Value::Object(obj)
}

fn redact_ado(url: &str) -> String {
    url.split(';')
        .map(|pair| {
            if let Some((k, _)) = pair.split_once('=') {
                if matches!(k.trim().to_lowercase().as_str(), "password" | "pwd") {
                    return format!("{}=***", k);
                }
            }
            pair.to_string()
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_key_defaults() {
        let k = conn_key(&json!({"host": "db"}));
        assert_eq!(k.host, "db");
        assert_eq!(k.port, 1433);
        assert_eq!(k.encrypt, "required");
        assert!(!k.trust);
    }

    #[test]
    fn parse_ado_redacts_password() {
        let v = parse_ado("Server=db;Database=app;User Id=sa;Password=Secret1;");
        assert_eq!(v["server"], "db");
        assert_eq!(v["database"], "app");
        assert_eq!(v["user id"], "sa");
        assert_eq!(v["password"], "***");
    }

    #[test]
    fn redact_ado_keeps_structure() {
        assert_eq!(
            redact_ado("Server=db;Password=Secret1;Database=app"),
            "Server=db;Password=***;Database=app"
        );
        assert_eq!(redact_ado("Server=db;pwd=x"), "Server=db;pwd=***");
    }

    #[test]
    fn params_extraction() {
        let v = json!({"sql": "SELECT 1", "params": [1, "a", true, null]});
        let p = params_of(&v);
        assert_eq!(p.len(), 4);
        assert_eq!(p[1], json!("a"));
        assert!(params_of(&json!({"sql": "x"})).is_empty());
    }

    #[test]
    fn build_ado_stable_order_and_extras() {
        let m = json!({
            "database": "app",
            "server": "db",
            "user_id": "sa",
            "password": "Secret1",
            "application name": "stryke"
        });
        assert_eq!(
            build_ado(m.as_object().unwrap()),
            "Server=db;Database=app;User Id=sa;Password=Secret1;application name=stryke"
        );
    }

    #[test]
    fn build_ado_roundtrips_through_parse() {
        let m = json!({"server": "db", "database": "app"});
        let url = build_ado(m.as_object().unwrap());
        let parsed = parse_ado(&url);
        assert_eq!(parsed["server"], "db");
        assert_eq!(parsed["database"], "app");
    }

    #[test]
    fn quote_ident_escapes_brackets() {
        assert_eq!(quote_ident("users"), "[users]");
        assert_eq!(quote_ident("weird]name"), "[weird]]name]");
    }

    #[test]
    fn split_batch_on_go_lines() {
        let script = "CREATE TABLE a (id int)\nGO\nINSERT INTO a VALUES (1)\ngo\n";
        let b = split_batch(script);
        assert_eq!(b.len(), 2);
        assert_eq!(b[0], "CREATE TABLE a (id int)");
        assert_eq!(b[1], "INSERT INTO a VALUES (1)");
        // a "GO" inside a longer token is not a separator
        assert_eq!(split_batch("SELECT 'GONE'").len(), 1);
    }
}
