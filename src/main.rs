//! moviedb — self-hosted movie rating API. Single binary: `serve` (the API)
//! and `refresh` (re-pull OMDB data, cron-able). Ported branch-for-branch
//! from the retired FastAPI implementation (git history: main.py,
//! refresh_omdb.py), which itself replaced the original Lambdas.
//!
//! Endpoints (all require x-api-key):
//!   POST /        ?title=&rating=&year=        fetch OMDB, store, snapshot ratings
//!   GET  /                                     all stored movies
//!   GET  /single  ?imdbid=  OR  ?title=&year=  single movie
//!   GET  /history ?imdbid=  OR  ?title=&year=  ratings snapshots, oldest first
//!
//! Primary key is imdb_id (stable, exact); title/year remain queryable via
//! indexed generated columns. Every POST and refresh snapshots the full
//! ratings array into ratings_history, making rating drift observable.
//! Errors are FastAPI-shaped JSON: {"detail": "..."} with 401/404/422/429/520.

use std::collections::HashMap;
use std::env;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Map, Value, json};

const DEFAULT_DB_PATH: &str = "/var/lib/moviedb/movies.db";
const DEFAULT_OMDB_URL: &str = "https://www.omdbapi.com/";

struct AppState {
    db: Mutex<Connection>,
    client: reqwest::Client,
    api_key: String,
    omdb_key: String,
    omdb_url: String,
}

// ---------------------------------------------------------------------------
// shared helpers

/// Python: datetime.now(timezone.utc).isoformat(timespec="seconds").
/// Must render the "+00:00" suffix (not "Z"): ratings_history ordering is
/// lexical on these strings, so the format has to stay byte-identical.
fn utcnow() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, false)
}

fn init_db(path: &str) -> rusqlite::Result<()> {
    let db = Connection::open(path)?;
    db.execute(
        "
        CREATE TABLE IF NOT EXISTS movies (
            imdb_id TEXT PRIMARY KEY,
            data    JSON NOT NULL,
            title   TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
            year    TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
        )
        ",
        [],
    )?;
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies (title, year)",
        [],
    )?;
    db.execute(
        "
        CREATE TABLE IF NOT EXISTS ratings_history (
            imdb_id  TEXT NOT NULL,
            title    TEXT NOT NULL,
            observed TEXT NOT NULL,   -- ISO-8601 UTC timestamp of the snapshot
            source   TEXT NOT NULL,
            value    TEXT NOT NULL,
            PRIMARY KEY (imdb_id, observed, source)
        )
        ",
        [],
    )?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    Ok(())
}

fn ratings_entry_field<'a>(entry: &'a Value, key: &str) -> &'a str {
    // Python: r.get(key, "?") — OMDB always sends strings here.
    entry.get(key).and_then(Value::as_str).unwrap_or("?")
}

/// Insert one history row per ratings entry. Python: snapshot_ratings().
fn snapshot_ratings(
    db: &Connection,
    imdbid: &str,
    title: &str,
    ratings: &[Value],
    observed: &str,
) -> rusqlite::Result<()> {
    for r in ratings {
        db.execute(
            "INSERT OR IGNORE INTO ratings_history VALUES (?, ?, ?, ?, ?)",
            params![
                imdbid,
                title,
                observed,
                ratings_entry_field(r, "Source"),
                ratings_entry_field(r, "Value"),
            ],
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// server

fn detail(status: StatusCode, msg: &str) -> Response {
    // FastAPI's error shape: {"detail": "<msg>"}.
    (status, Json(json!({ "detail": msg }))).into_response()
}

fn internal_error() -> Response {
    detail(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
}

/// Constant-time byte comparison: if lengths differ fail, else XOR-fold.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn check_key(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get("x-api-key")
        .map(|v| ct_eq(v.as_bytes(), state.api_key.as_bytes()))
        .unwrap_or(false);
    if !ok {
        return detail(StatusCode::UNAUTHORIZED, "Invalid API key");
    }
    next.run(req).await
}

#[derive(Deserialize)]
struct AddParams {
    title: String,
    rating: String,
    year: String,
}

#[derive(Deserialize)]
struct LookupParams {
    imdbid: Option<String>,
    title: Option<String>,
    year: Option<String>,
}

fn personal_entry(rating: &str) -> Value {
    json!({ "Source": "Personal", "Value": rating })
}

/// Locate a movie row by imdbid, or by (title, year). Python: resolve_movie().
/// Empty strings are falsy in Python, so they count as "not provided" here too.
fn resolve_movie(
    db: &Connection,
    p: &LookupParams,
) -> Result<Option<(String, String)>, Box<Response>> {
    let imdbid = p.imdbid.as_deref().filter(|s| !s.is_empty());
    let title = p.title.as_deref().filter(|s| !s.is_empty());
    let year = p.year.as_deref().filter(|s| !s.is_empty());

    let row = if let Some(imdbid) = imdbid {
        db.query_row(
            "SELECT imdb_id, data FROM movies WHERE imdb_id = ?",
            params![imdbid],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
    } else if let (Some(title), Some(year)) = (title, year) {
        db.query_row(
            "SELECT imdb_id, data FROM movies WHERE title = ? AND year = ?",
            params![title, year],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
    } else {
        return Err(Box::new(detail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Provide imdbid, or title and year",
        )));
    };
    row.map_err(|_| Box::new(internal_error()))
}

async fn add_movie(State(state): State<Arc<AppState>>, Query(p): Query<AddParams>) -> Response {
    let resp = match state
        .client
        .get(&state.omdb_url)
        .query(&[
            ("apikey", state.omdb_key.as_str()),
            ("t", p.title.as_str()),
            ("y", p.year.as_str()),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return internal_error(),
    };
    let movie: Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return internal_error(),
    };

    if movie.get("Response").and_then(Value::as_str) == Some("True") {
        let Some(obj) = movie.as_object() else {
            return internal_error();
        };
        // Lowercase every key, in the original order (Map preserves insertion
        // order via the preserve_order feature — required so stored JSON key
        // order matches the Python dict behavior).
        let mut out = Map::new();
        for (key, value) in obj {
            if key == "Ratings" {
                let mut ratings = value.as_array().cloned().unwrap_or_default();
                ratings.push(personal_entry(&p.rating));
                out.insert("ratings".to_string(), Value::Array(ratings));
            } else {
                out.insert(key.to_lowercase(), value.clone());
            }
        }
        if !out.contains_key("ratings") {
            out.insert(
                "ratings".to_string(),
                Value::Array(vec![personal_entry(&p.rating)]),
            );
        }
        // shift_remove, not remove: with preserve_order, plain remove is a
        // swap_remove and would scramble key order. Python dict.pop keeps order.
        out.shift_remove("response");

        let Some(imdbid) = out.get("imdbid").and_then(Value::as_str).map(String::from) else {
            return internal_error(); // Python: KeyError -> 500
        };
        let Some(title) = out.get("title").and_then(Value::as_str).map(String::from) else {
            return internal_error(); // Python: KeyError -> 500
        };
        let ratings = out
            .get("ratings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let now = utcnow();
        let data = match serde_json::to_string(&out) {
            Ok(s) => s,
            Err(_) => return internal_error(),
        };
        // Single transaction: upsert + history snapshot.
        let mut conn = state.db.lock().unwrap();
        let result = (|| -> rusqlite::Result<()> {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT OR REPLACE INTO movies (imdb_id, data) VALUES (?, ?)",
                params![imdbid, data],
            )?;
            snapshot_ratings(&tx, &imdbid, &title, &ratings, &now)?;
            tx.commit()
        })();
        if result.is_err() {
            return internal_error();
        }
        // Python: PlainTextResponse(out["title"]) — 200, text/plain.
        return (StatusCode::OK, title).into_response();
    }

    let error = movie.get("Error").and_then(Value::as_str).unwrap_or("");
    if error == "Daily request limit reached!" {
        return detail(
            StatusCode::TOO_MANY_REQUESTS,
            "OMDB API request limit reached!",
        );
    }
    if error == "Movie not found!" {
        return detail(StatusCode::NOT_FOUND, "Movie Not Found!");
    }
    detail(StatusCode::from_u16(520).unwrap(), "Unknown Error!")
}

async fn list_movies(State(state): State<Arc<AppState>>) -> Response {
    let conn = state.db.lock().unwrap();
    let result = (|| -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare("SELECT data FROM movies")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect()
    })();
    let raw = match result {
        Ok(rows) => rows,
        Err(_) => return internal_error(),
    };
    let mut docs: Vec<Value> = Vec::with_capacity(raw.len());
    for data in raw {
        match serde_json::from_str(&data) {
            Ok(v) => docs.push(v),
            Err(_) => return internal_error(),
        }
    }
    Json(docs).into_response()
}

async fn get_single(State(state): State<Arc<AppState>>, Query(p): Query<LookupParams>) -> Response {
    let conn = state.db.lock().unwrap();
    let row = match resolve_movie(&conn, &p) {
        Ok(row) => row,
        Err(resp) => return *resp,
    };
    let Some((_, data)) = row else {
        return detail(StatusCode::NOT_FOUND, "Movie Not Found!");
    };
    match serde_json::from_str::<Value>(&data) {
        Ok(doc) => Json(doc).into_response(),
        Err(_) => internal_error(),
    }
}

async fn get_history(
    State(state): State<Arc<AppState>>,
    Query(p): Query<LookupParams>,
) -> Response {
    let conn = state.db.lock().unwrap();
    let row = match resolve_movie(&conn, &p) {
        Ok(row) => row,
        Err(resp) => return *resp,
    };
    let Some((imdb_id, _)) = row else {
        return detail(StatusCode::NOT_FOUND, "Movie Not Found!");
    };
    let result = (|| -> rusqlite::Result<Vec<Value>> {
        let mut stmt = conn.prepare(
            "
        SELECT observed, source, value FROM ratings_history
        WHERE imdb_id = ? ORDER BY observed ASC, source ASC
        ",
        )?;
        let rows = stmt.query_map(params![imdb_id], |r| {
            Ok(json!({
                "observed": r.get::<_, String>(0)?,
                "source": r.get::<_, String>(1)?,
                "value": r.get::<_, String>(2)?,
            }))
        })?;
        rows.collect()
    })();
    match result {
        Ok(snaps) => Json(json!({ "imdbid": imdb_id, "snapshots": snaps })).into_response(),
        Err(_) => internal_error(),
    }
}

async fn serve(host: String, port: u16) {
    let api_key = require_env("API_KEY");
    let omdb_key = require_env("OMDB_KEY");
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string());
    let omdb_url = env::var("OMDB_URL").unwrap_or_else(|_| DEFAULT_OMDB_URL.to_string());

    if let Err(e) = init_db(&db_path) {
        eprintln!("failed to initialize database at {db_path}: {e}");
        exit(1);
    }
    let conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to open database at {db_path}: {e}");
            exit(1);
        }
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client");

    let state = Arc::new(AppState {
        db: Mutex::new(conn),
        client,
        api_key,
        omdb_key,
        omdb_url,
    });
    let app = Router::new()
        .route("/", get(list_movies).post(add_movie))
        .route("/single", get(get_single))
        .route("/history", get(get_history))
        .layer(middleware::from_fn_with_state(state.clone(), check_key))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(format!("{host}:{port}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {host}:{port}: {e}");
            exit(1);
        }
    };
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("server error: {e}");
        exit(1);
    }
}

// ---------------------------------------------------------------------------
// refresh (port of refresh_omdb.py)

/// Python: personal_rating() — the first ratings entry with Source == "Personal".
/// A missing OR JSON-null "Value" is None in Python, so both mean skip here.
fn personal_rating(doc: &Value) -> Option<Value> {
    let ratings = doc.get("ratings").and_then(Value::as_array)?;
    for r in ratings {
        if r.get("Source").and_then(Value::as_str) == Some("Personal") {
            return r.get("Value").cloned().filter(|v| !v.is_null());
        }
    }
    None
}

/// Python: rebuild_doc().
fn rebuild_doc(omdb: &Map<String, Value>, personal_value: Value, now: &str) -> Map<String, Value> {
    let mut out = Map::new();
    for (key, value) in omdb {
        if key == "Ratings" {
            out.insert(
                "ratings".to_string(),
                Value::Array(value.as_array().cloned().unwrap_or_default()),
            );
        } else {
            out.insert(key.to_lowercase(), value.clone());
        }
    }
    if !out.contains_key("ratings") {
        out.insert("ratings".to_string(), Value::Array(Vec::new()));
    }
    out.get_mut("ratings")
        .and_then(Value::as_array_mut)
        .expect("ratings is a list")
        .push(json!({ "Source": "Personal", "Value": personal_value }));
    out.shift_remove("response"); // shift, not swap: keep key order (see serve)
    out.insert("_refreshed".to_string(), Value::String(now.to_string()));
    out
}

fn doc_str<'a>(doc: &'a Value, key: &str, default: &'a str) -> &'a str {
    doc.get(key).and_then(Value::as_str).unwrap_or(default)
}

/// Python repr() of a JSON scalar, for the dry-run diff line.
fn py_repr(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
        other => other.to_string(),
    }
}

async fn refresh(db_path: String, limit: Option<i64>, sleep_secs: f64, dry_run: bool) {
    let api_key = match env::var("OMDB_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            // Python: sys.exit(msg) — message to stderr, exit code 1.
            eprintln!("OMDB_KEY not set. Try: export $(grep OMDB_KEY /etc/moviedb.env)");
            exit(1);
        }
    };
    let omdb_url = env::var("OMDB_URL").unwrap_or_else(|_| DEFAULT_OMDB_URL.to_string());

    let mut db = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to open database at {db_path}: {e}");
            exit(1);
        }
    };
    let rows_result = (|| -> rusqlite::Result<Vec<(String, String)>> {
        let mut stmt = db.prepare(
            "
        SELECT imdb_id, data FROM movies
        ORDER BY COALESCE(json_extract(data, '$._refreshed'), '') ASC
        ",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect()
    })();
    let mut rows = match rows_result {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("query failed: {e}");
            exit(1);
        }
    };
    // Python: `if args.limit: rows = rows[:args.limit]` — 0 is falsy (no
    // truncation) and negative limits use slice semantics (drop from the end).
    if let Some(limit) = limit
        && limit != 0
    {
        let keep = if limit < 0 {
            rows.len().saturating_sub(limit.unsigned_abs() as usize)
        } else {
            (limit as usize).min(rows.len())
        };
        rows.truncate(keep);
    }

    // Python fetch_omdb uses urlopen(timeout=15).
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client");

    let mut refreshed = 0usize;
    let mut skipped = 0usize;
    for (imdb_id, data) in &rows {
        let doc: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("invalid JSON for {imdb_id}: {e}");
                exit(1); // Python: json.loads raises, crashing the script
            }
        };
        let Some(rating) = personal_rating(&doc) else {
            println!(
                "SKIP {}: no Personal rating",
                doc_str(&doc, "title", imdb_id)
            );
            skipped += 1;
            continue;
        };

        let omdb: Value = match async {
            client
                .get(&omdb_url)
                .query(&[("apikey", api_key.as_str()), ("i", imdb_id.as_str())])
                .send()
                .await?
                .json()
                .await
        }
        .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("OMDB request failed for {imdb_id}: {e}");
                exit(1); // Python: urlopen raises, crashing the script
            }
        };

        if omdb.get("Response").and_then(Value::as_str) != Some("True") {
            let error = omdb.get("Error").and_then(Value::as_str).unwrap_or("");
            if error == "Daily request limit reached!" {
                println!(
                    "OMDB daily limit hit after {refreshed} refreshes. Re-run tomorrow — progress is saved."
                );
                break;
            }
            println!(
                "SKIP {} [{}]: {}",
                doc_str(&doc, "title", "?"),
                imdb_id,
                error
            );
            skipped += 1;
            continue;
        }

        let now = utcnow();
        let omdb_obj = omdb.as_object().expect("OMDB response is a JSON object");
        let new_doc = rebuild_doc(omdb_obj, rating, &now);
        let new_ratings = new_doc
            .get("ratings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // A Response=True doc without Title would persist with a NULL generated
        // title column, permanently breaking ?title=&year= lookups. Python
        // KeyErrors inside the transaction (rollback + crash); we skip instead.
        let Some(title) = new_doc.get("title").and_then(Value::as_str) else {
            println!("SKIP {imdb_id}: OMDB response missing Title — not persisted");
            skipped += 1;
            tokio::time::sleep(Duration::from_secs_f64(sleep_secs)).await;
            continue;
        };
        let year = new_doc.get("year").and_then(Value::as_str).unwrap_or("?");

        if dry_run {
            let empty = Vec::new();
            let old_ratings = doc
                .get("ratings")
                .and_then(Value::as_array)
                .unwrap_or(&empty);
            let mut old: HashMap<&str, &Value> = HashMap::new();
            for r in old_ratings {
                old.insert(
                    ratings_entry_field(r, "Source"),
                    r.get("Value").unwrap_or(&Value::Null),
                );
            }
            // Insertion-ordered map of the new ratings (last value wins).
            let mut new_pairs: Vec<(&str, &Value)> = Vec::new();
            for r in &new_ratings {
                let s = ratings_entry_field(r, "Source");
                let v = r.get("Value").unwrap_or(&Value::Null);
                match new_pairs.iter_mut().find(|(k, _)| *k == s) {
                    Some(pair) => pair.1 = v,
                    None => new_pairs.push((s, v)),
                }
            }
            let mut changed: Vec<String> = Vec::new();
            for (s, v) in &new_pairs {
                if *s == "Personal" {
                    continue;
                }
                let old_v = old.get(s).copied();
                if old_v != Some(v) {
                    changed.push(format!(
                        "'{}': ({}, {})",
                        s,
                        old_v.map(py_repr).unwrap_or_else(|| "None".to_string()),
                        py_repr(v),
                    ));
                }
            }
            let diff = if changed.is_empty() {
                "no rating changes".to_string()
            } else {
                format!("{{{}}}", changed.join(", "))
            };
            println!("DRY  {title} ({year}): {diff}");
            refreshed += 1;
            tokio::time::sleep(Duration::from_secs_f64(sleep_secs)).await;
            continue;
        }

        let new_data =
            serde_json::to_string(&Value::Object(new_doc.clone())).expect("doc serializes");
        let result = (|| -> rusqlite::Result<()> {
            let tx = db.transaction()?;
            tx.execute(
                "UPDATE movies SET data = ? WHERE imdb_id = ?",
                params![new_data, imdb_id],
            )?;
            snapshot_ratings(&tx, imdb_id, title, &new_ratings, &now)?;
            tx.commit()
        })();
        if let Err(e) = result {
            eprintln!("write failed for {imdb_id}: {e}");
            exit(1); // Python: sqlite3 raises, crashing the script
        }
        println!("OK   {title} ({year})");
        refreshed += 1;
        tokio::time::sleep(Duration::from_secs_f64(sleep_secs)).await;
    }

    println!(
        "\nDone: {} refreshed, {} skipped, {} remaining.",
        refreshed,
        skipped,
        rows.len() - refreshed - skipped
    );
}

// ---------------------------------------------------------------------------
// CLI

fn require_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| {
        eprintln!("{name} not set");
        exit(1);
    })
}

#[derive(Parser)]
#[command(name = "moviedb", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the API server
    Serve {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value_t = 8000)]
        port: u16,
    },
    /// Re-pull OMDB data for all movies, preserving Personal ratings
    Refresh {
        /// SQLite database path (falls back to $DB_PATH)
        db_path: Option<String>,
        /// Only process the N oldest-refreshed movies (Python slice semantics)
        #[arg(long, allow_negative_numbers = true)]
        limit: Option<i64>,
        /// Seconds to sleep between OMDB requests
        #[arg(long, default_value_t = 0.5)]
        sleep: f64,
        /// Print rating deltas without writing
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() {
    match Cli::parse().command {
        Command::Serve { host, port } => serve(host, port).await,
        Command::Refresh {
            db_path,
            limit,
            sleep,
            dry_run,
        } => {
            let db_path = db_path
                .or_else(|| env::var("DB_PATH").ok().filter(|s| !s.is_empty()))
                .unwrap_or_else(|| {
                    eprintln!("moviedb refresh: no db_path given and DB_PATH not set");
                    exit(2);
                });
            refresh(db_path, limit, sleep, dry_run).await;
        }
    }
}
