//! The `serve` subcommand: axum app, route handlers, and the `AppState` they
//! share. Endpoints (all require x-api-key):
//!   POST /movies  ?title=&rating=&year=     fetch OMDB, upsert, snapshot ratings
//!   GET  /movies  [?title=] [?year=]        the collection, optionally filtered
//!   GET  /movies/recent  [?limit=]          most-recently-refreshed movies (default 10, max 50)
//!   GET  /movies/{imdb_id}                  one movie
//!   GET  /movies/{imdb_id}/history          ratings snapshots, oldest first
//!
//! URLs identify resources: a movie's canonical address is
//! `/movies/{imdb_id}` (stable, exact — also what the 201 `Location` header
//! points at), and title/year lookup is a *filter on the collection* via the
//! indexed generated columns. A filter matching several movies returns them
//! all, and one matching none returns `[]` — with a non-unique key,
//! multiple/zero matches are data, not errors. Only `/movies/{imdb_id}` can
//! 404. Every POST snapshots the full ratings array
//! into `ratings_history`, making rating drift observable, and returns the
//! stored movie doc as JSON: 201 + `Location` if this `imdb_id` is new, 200
//! if it already existed. Collection responses (`/movies`, `/movies/recent`)
//! are bare JSON arrays; a response is an object only when it carries fields
//! beyond the collection itself (`/history`'s `imdb_id`).
//!
//! Every error response is an RFC 9457 problem-details object
//! (`application/problem+json`): `{"type", "title", "status", "detail"}`,
//! where `title` restates the status line and `detail` explains the
//! occurrence. `type` is `"about:blank"` (the RFC's "the status code says it
//! all" default) except where one status covers two distinguishable
//! problems: the 503s carry `urn:moviedb:problem:omdb-quota-exhausted` vs
//! `urn:moviedb:problem:at-capacity` so clients can branch without parsing
//! `detail` prose. Statuses:
//!   400 malformed path parameter (undecodable percent-escapes)
//!   401 missing/invalid x-api-key
//!   404 unknown imdb_id, or a path this API doesn't serve
//!   405 method not supported on this path (see the `Allow` header)
//!   422 query params missing, empty, or unparseable
//!   502 OMDB returned an error this server doesn't recognize
//!   503 OMDB's shared daily request quota is exhausted, or all DB permits
//!       stayed busy past the load-shed deadline (see `Retry-After` on both)
//!   500 internal error (see server logs for detail)

use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{FromRequestParts, Path, Query, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Row, Rows, TransactionBehavior, params};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;

use crate::db::{DB_POOL_SIZE, build_pool, snapshot_ratings};
use crate::util::{
    DEFAULT_OMDB_URL, OmdbError, classify_omdb_error, ct_eq, env_nonempty, normalize_omdb, utcnow,
};

const DEFAULT_DB_PATH: &str = "/var/lib/moviedb/movies.db";

struct AppState {
    db: Pool<SqliteConnectionManager>,
    /// One permit per pooled connection: admission to the blocking pool for
    /// DB work, awaited async-side so a waiting request costs a parked
    /// future, never a parked OS thread (see `with_conn`).
    db_permits: Arc<Semaphore>,
    client: reqwest::Client,
    api_key: String,
    omdb_key: String,
    omdb_url: String,
}

/// How long a request may wait for a DB permit before being load-shed with
/// a 503. Every DB query here is single-digit-ms, so a full pool that stays
/// full this long means the server is genuinely underwater — shedding beats
/// stacking up requests the client has long since given up on.
const DB_PERMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// RFC 9457 `type` URIs for the errors where the status code alone is
/// ambiguous (both 503s). URNs, not URLs: there's no docs host to
/// dereference, and the RFC only requires identity — clients compare, they
/// don't fetch.
const PROBLEM_OMDB_QUOTA: &str = "urn:moviedb:problem:omdb-quota-exhausted";
const PROBLEM_AT_CAPACITY: &str = "urn:moviedb:problem:at-capacity";

/// This API's error shape, used everywhere: an RFC 9457 problem-details
/// object. `instance` is omitted — it's optional, and these helpers are
/// called from extractors and closures that don't carry the request URI.
fn problem(status: StatusCode, ptype: &str, msg: &str) -> Response {
    let body = json!({
        "type": ptype,
        "title": status.canonical_reason().unwrap_or(""),
        "status": status.as_u16(),
        "detail": msg,
    });
    (
        status,
        [(header::CONTENT_TYPE, "application/problem+json")],
        body.to_string(),
    )
        .into_response()
}

fn detail(status: StatusCode, msg: &str) -> Response {
    problem(status, "about:blank", msg)
}

fn internal_error(context: impl std::fmt::Display) -> Response {
    eprintln!("{context}");
    detail(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
}

fn raw_json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Joins rows that each hold a complete JSON document into one JSON array,
/// copying each row's bytes straight out of SQLite: the docs never become
/// `Value` trees or per-row `String`s. One buffer, so the response goes out
/// with a `Content-Length` rather than chunked.
fn collect_json_array(
    mut rows: Rows<'_>,
    mut push_doc: impl FnMut(&mut String, &Row<'_>) -> rusqlite::Result<()>,
) -> rusqlite::Result<String> {
    let mut body = String::from("[");
    while let Some(row) = rows.next()? {
        if body.len() > 1 {
            body.push(',');
        }
        push_doc(&mut body, row)?;
    }
    body.push(']');
    Ok(body)
}

fn doc_column<'r>(row: &'r Row<'_>, idx: usize) -> rusqlite::Result<&'r str> {
    Ok(row.get_ref(idx)?.as_str()?)
}

/// Appends `doc` to `body` with one extra string member, without parsing it.
/// Leans on a `data` column always holding a non-empty JSON object this
/// server serialized, so it ends in `}` and already has a member to
/// comma-separate from; anything else (a hand-edited row) is appended
/// untouched rather than spliced into invalid JSON. `value` is written
/// unescaped — callers pass fixed-format ASCII timestamps.
fn push_with_member(body: &mut String, doc: &str, key: &str, value: &str) {
    let Some(without_close) = doc.strip_suffix('}').filter(|d| d.len() >= 2) else {
        body.push_str(doc);
        return;
    };
    body.push_str(without_close);
    body.push_str(",\"");
    body.push_str(key);
    body.push_str("\":\"");
    body.push_str(value);
    body.push_str("\"}");
}

/// Runs `f` against a pooled connection on a blocking-pool thread. Both pool
/// checkout and every rusqlite call are synchronous; running them inline in
/// an async handler would park whichever tokio worker thread runs it for as
/// long as the checkout/query takes — on this LXC's few worker threads, that
/// can stall unrelated in-flight requests, not just the one waiting on the
/// DB. `f` returns the finished Response itself (not just a query result)
/// since what happens after the query varies per handler (streaming, Json,
/// plain text) and none of it does further I/O, so there's no reason to hop
/// back to the async side first.
async fn with_conn<F>(state: &AppState, f: F) -> Response
where
    F: FnOnce(&mut Connection) -> Response + Send + 'static,
{
    // Acquire the permit *before* spawn_blocking, on the async side: permits
    // == pool size, so by the time a task reaches the blocking pool a
    // connection is guaranteed free and pool.get() below never waits. The
    // alternative — letting excess tasks block inside pool.get() — parks
    // them on blocking-pool threads, eating the DNS headroom main.rs
    // reserves above DB_POOL_SIZE and breaking its thread-count invariant.
    let permit = match tokio::time::timeout(
        DB_PERMIT_TIMEOUT,
        Arc::clone(&state.db_permits).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        // acquire_owned only errors if the semaphore is closed, which
        // nothing here ever does.
        Ok(Err(e)) => return internal_error(format!("DB semaphore closed: {e}")),
        Err(_) => {
            let mut resp = problem(
                StatusCode::SERVICE_UNAVAILABLE,
                PROBLEM_AT_CAPACITY,
                "Server is at capacity; try again shortly",
            );
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            return resp;
        }
    };
    let pool = state.db.clone();
    tokio::task::spawn_blocking(move || {
        // Hold the permit for the full duration of the DB work, releasing
        // it only once the connection is back in the pool.
        let _permit = permit;
        let mut conn = match pool.get() {
            Ok(c) => c,
            Err(e) => return internal_error(format!("failed to check out DB connection: {e}")),
        };
        f(&mut conn)
    })
    .await
    // Dev builds only: release compiles with panic=abort, so a panicking DB
    // task takes the whole process down (systemd Restart=on-failure covers
    // it) before this JoinError can ever be observed.
    .unwrap_or_else(|e| internal_error(format!("DB task panicked: {e}")))
}

async fn check_key(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get("x-api-key")
        .is_some_and(|v| ct_eq(v.as_bytes(), state.api_key.as_bytes()));
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

/// Optional filters on the `GET /movies` collection. Each combines with
/// AND; both absent means the whole collection. Empty strings count as "not
/// provided", so an accidental `?year=` with no value doesn't try to match
/// a literal empty-string year column.
#[derive(Deserialize)]
struct FilterParams {
    title: Option<String>,
    year: Option<String>,
}

#[derive(Deserialize)]
struct RecentParams {
    limit: Option<usize>,
}

const DEFAULT_RECENT_LIMIT: usize = 10;
const MAX_RECENT_LIMIT: usize = 50;

/// Like `axum::extract::Query`, but a parse failure is a 422 problem-details
/// body instead of axum's default 400 plain-text rejection.
struct Params<T>(T);

impl<T, S> FromRequestParts<S> for Params<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = Response;

    // No `.await` in the body (Query::try_from_uri is synchronous) — a
    // plain fn returning an already-ready future avoids spawning an async
    // state machine for what's just a synchronous parse.
    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> {
        std::future::ready(
            Query::<T>::try_from_uri(&parts.uri)
                .map(|Query(v)| Params(v))
                .map_err(|e| detail(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string())),
        )
    }
}

/// `axum::extract::Path<String>`, but a rejection (in practice only
/// undecodable percent-escapes) is a 400 problem-details body instead of
/// axum's plain-text default.
struct ImdbId(String);

impl<S> FromRequestParts<S> for ImdbId
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Path::<String>::from_request_parts(parts, state).await {
            Ok(Path(id)) => Ok(ImdbId(id)),
            Err(e) => Err(detail(StatusCode::BAD_REQUEST, &e.to_string())),
        }
    }
}

/// Upserts one movie plus its `ratings_history` snapshot in a single
/// transaction, returning the stored doc as JSON: 201 with a `Location`
/// header if this `imdb_id` is new, 200 if it already existed.
async fn upsert_movie(
    state: &AppState,
    imdbid: String,
    title: String,
    ratings: Vec<Value>,
    data: String,
    now: String,
) -> Response {
    with_conn(state, move |conn| {
        let result = (|| -> rusqlite::Result<bool> {
            // Immediate, not deferred: this transaction reads (the existence
            // probe) before it writes. A deferred transaction would take a
            // read snapshot first, and if the refresh process commits between
            // the SELECT and the INSERT, the write-lock upgrade fails with
            // SQLITE_BUSY *without invoking the busy handler* — the snapshot
            // is stale, so busy_timeout's 5s of retries never happens.
            // Starting immediate takes the write lock up front, where the
            // busy handler does apply.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existed = tx
                .prepare_cached("SELECT 1 FROM movies WHERE imdb_id = ?")?
                .query_row(params![imdbid], |_| Ok(()))
                .optional()?
                .is_some();
            tx.execute(
                "INSERT OR REPLACE INTO movies (imdb_id, data) VALUES (?, ?)",
                params![imdbid, data],
            )?;
            snapshot_ratings(&tx, &imdbid, &title, &ratings, &now)?;
            tx.commit()?;
            Ok(existed)
        })();
        match result {
            Ok(existed) => {
                let status = if existed {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                };
                let mut resp =
                    (status, [(header::CONTENT_TYPE, "application/json")], data).into_response();
                if !existed && let Ok(location) = format!("/movies/{imdbid}").parse() {
                    resp.headers_mut().insert(header::LOCATION, location);
                }
                resp
            }
            Err(e) => internal_error(format!("failed to write movie {imdbid}: {e}")),
        }
    })
    .await
}

async fn add_movie(State(state): State<Arc<AppState>>, Params(p): Params<AddParams>) -> Response {
    // Same empty-means-missing treatment resolve_movie gives GET lookups:
    // an accidental `?title=` would otherwise go to OMDB as an empty title
    // and come back as whatever OMDB answers (a 404 or 502) instead of the
    // 422 every other unresolvable request gets.
    if p.title.is_empty() || p.rating.is_empty() || p.year.is_empty() {
        return detail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "title, rating, and year must be non-empty",
        );
    }
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
        // without_url(): reqwest::Error's Display includes the request URL
        // (query string and all) when one is attached — which for a failed
        // send() means the OMDB apikey ends up readable in journalctl.
        Err(e) => return internal_error(format!("OMDB request failed: {}", e.without_url())),
    };
    let omdb = match resp.json::<Value>().await {
        Ok(Value::Object(obj)) if obj.get("Response").and_then(Value::as_str) == Some("True") => {
            obj
        }
        Ok(other) => {
            return omdb_failure(other.get("Error").and_then(Value::as_str).unwrap_or(""), &p);
        }
        Err(e) => {
            return internal_error(format!(
                "OMDB response JSON parse failed: {}",
                e.without_url()
            ));
        }
    };

    let now = utcnow();
    let out = normalize_omdb(omdb, Value::String(p.rating), &now);
    let Some(imdbid) = out.get("imdb_id").and_then(Value::as_str).map(String::from) else {
        eprintln!("OMDB response missing imdbID");
        return detail(StatusCode::BAD_GATEWAY, "OMDB response missing imdbID");
    };
    let Some(title) = out.get("title").and_then(Value::as_str).map(String::from) else {
        eprintln!("OMDB response missing Title");
        return detail(StatusCode::BAD_GATEWAY, "OMDB response missing Title");
    };
    // Owned: the write runs on a blocking-pool thread (see with_conn), which
    // needs a 'static closure.
    let ratings: Vec<Value> = out
        .get("ratings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let data = match serde_json::to_string(&out) {
        Ok(s) => s,
        Err(e) => return internal_error(format!("failed to serialize movie doc: {e}")),
    };
    upsert_movie(&state, imdbid, title, ratings, data, now).await
}

/// Maps a Response=False OMDB payload's `Error` onto this API's answer.
fn omdb_failure(error: &str, p: &AddParams) -> Response {
    match classify_omdb_error(error) {
        OmdbError::DailyLimit => {
            // 503, not 429: the upstream's shared quota is spent, the caller
            // isn't being rate-limited. OMDB doesn't document when its
            // counter resets, so Retry-After is a conservative 24h.
            let mut resp = problem(
                StatusCode::SERVICE_UNAVAILABLE,
                PROBLEM_OMDB_QUOTA,
                "OMDB's daily request limit is exhausted; try again later",
            );
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("86400"));
            resp
        }
        OmdbError::NotFound => detail(StatusCode::NOT_FOUND, "Movie not found"),
        OmdbError::Other => {
            eprintln!(
                "OMDB returned an unrecognized error for {} ({}): {error:?}",
                p.title, p.year
            );
            detail(StatusCode::BAD_GATEWAY, "Unrecognized error from OMDB")
        }
    }
}

async fn list_movies(
    State(state): State<Arc<AppState>>,
    Params(p): Params<FilterParams>,
) -> Response {
    with_conn(&state, move |conn| {
        let title = p.title.as_deref().filter(|s| !s.is_empty());
        let year = p.year.as_deref().filter(|s| !s.is_empty());
        let (sql, filters) = match (title, year) {
            (None, None) => ("SELECT data FROM movies", vec![]),
            (Some(t), None) => ("SELECT data FROM movies WHERE title = ?", vec![t]),
            (None, Some(y)) => ("SELECT data FROM movies WHERE year = ?", vec![y]),
            (Some(t), Some(y)) => (
                "SELECT data FROM movies WHERE title = ? AND year = ?",
                vec![t, y],
            ),
        };
        let result = (|| -> rusqlite::Result<String> {
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query(rusqlite::params_from_iter(filters))?;
            collect_json_array(rows, |body, row| {
                body.push_str(doc_column(row, 0)?);
                Ok(())
            })
        })();
        match result {
            Ok(body) => raw_json(body),
            Err(e) => internal_error(format!("failed to list movies: {e}")),
        }
    })
    .await
}

/// The dashboard's "recently catalogued" view: the N movies with the most
/// recent `ratings_history` snapshot, newest first, each with that
/// timestamp folded in as `last_refreshed` (textually, see
/// `push_with_member`).
async fn get_recent(
    State(state): State<Arc<AppState>>,
    Params(p): Params<RecentParams>,
) -> Response {
    let limit = p
        .limit
        .map_or(DEFAULT_RECENT_LIMIT, |n| n.min(MAX_RECENT_LIMIT));
    with_conn(&state, move |conn| {
        let result = (|| -> rusqlite::Result<String> {
            // Correlated MAX, not GROUP BY: the grouped form aggregates
            // every ratings_history row before LIMIT discards all but N, so
            // it costs more on every refresh run forever. `observed` is the
            // second column of that table's primary key, so with imdb_id
            // fixed each subquery is a seek to the last index entry.
            // Movie-bounded instead of history-bounded: 19ms -> 0.7ms today.
            //
            // The imdb_id tiebreaker keeps LIMIT deterministic — one refresh
            // run stamps all its snapshots with the same `observed`.
            let mut stmt = conn.prepare_cached(
                "
                SELECT m.data,
                       (
                           SELECT MAX(h.observed)
                           FROM ratings_history h
                           WHERE h.imdb_id = m.imdb_id
                       ) AS last_refreshed
                FROM movies m
                WHERE last_refreshed IS NOT NULL
                ORDER BY last_refreshed DESC, m.imdb_id ASC
                LIMIT ?
                ",
            )?;
            let rows = stmt.query(params![limit as i64])?;
            collect_json_array(rows, |body, row| {
                push_with_member(
                    body,
                    doc_column(row, 0)?,
                    "last_refreshed",
                    doc_column(row, 1)?,
                );
                Ok(())
            })
        })();
        match result {
            Ok(body) => raw_json(body),
            Err(e) => internal_error(format!("failed to list recent movies: {e}")),
        }
    })
    .await
}

async fn get_movie(State(state): State<Arc<AppState>>, ImdbId(imdb_id): ImdbId) -> Response {
    with_conn(&state, move |conn| {
        let row = (|| -> rusqlite::Result<Option<String>> {
            conn.prepare_cached("SELECT data FROM movies WHERE imdb_id = ?")?
                .query_row(params![imdb_id], |r| r.get::<_, String>(0))
                .optional()
        })();
        match row {
            Ok(Some(data)) => raw_json(data),
            Ok(None) => detail(StatusCode::NOT_FOUND, "Movie not found"),
            Err(e) => internal_error(format!("failed to fetch movie {imdb_id}: {e}")),
        }
    })
    .await
}

async fn get_history(State(state): State<Arc<AppState>>, ImdbId(imdb_id): ImdbId) -> Response {
    with_conn(&state, move |conn| {
        // One query, not an existence probe followed by the history select.
        // The outer join keeps the distinction those two encoded: no rows at
        // all means the *movie* is unknown (404), whereas a single all-NULL
        // row means a known movie that simply has no snapshots yet — a real
        // resource whose history is empty.
        //
        // Merging costs nothing: ratings_history's primary key is
        // (imdb_id, observed, source), so with imdb_id fixed the index already
        // yields the ORDER BY's exact order, and SQLite plans two index
        // searches with no sort (verified with EXPLAIN QUERY PLAN).
        let result = (|| -> rusqlite::Result<Vec<Option<Value>>> {
            let mut stmt = conn.prepare_cached(
                "
        SELECT h.observed, h.source, h.value
        FROM movies m LEFT JOIN ratings_history h ON h.imdb_id = m.imdb_id
        WHERE m.imdb_id = ? ORDER BY h.observed ASC, h.source ASC
        ",
            )?;
            let rows = stmt.query_map(params![imdb_id], |r| {
                Ok(match r.get::<_, Option<String>>(0)? {
                    Some(observed) => Some(json!({
                        "observed": observed,
                        "source": r.get::<_, String>(1)?,
                        "value": r.get::<_, String>(2)?,
                    })),
                    None => None,
                })
            })?;
            rows.collect()
        })();
        match result {
            Ok(rows) if rows.is_empty() => detail(StatusCode::NOT_FOUND, "Movie not found"),
            Ok(rows) => {
                let snaps: Vec<Value> = rows.into_iter().flatten().collect();
                Json(json!({ "imdb_id": imdb_id, "snapshots": snaps })).into_response()
            }
            Err(e) => internal_error(format!("failed to fetch history for {imdb_id}: {e}")),
        }
    })
    .await
}

/// axum's own 404/405 have empty bodies; these keep the problem-details
/// contract on every error. axum still sets `Allow` on the 405.
async fn fallback_not_found() -> Response {
    detail(StatusCode::NOT_FOUND, "Not Found")
}

async fn fallback_method_not_allowed() -> Response {
    detail(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed")
}

/// Resolves when SIGTERM (systemctl stop/restart, pct shutdown) or SIGINT
/// (^C in a terminal) arrives; axum then stops accepting, drains in-flight
/// requests, and returns — instead of the default of the signal killing the
/// process mid-response. systemd's `TimeoutStopSec` (90s default) still
/// backstops a hung drain with SIGKILL.
async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = sigint.recv() => {},
    }
    eprintln!("shutdown signal received, draining in-flight requests");
}

fn require_env(name: &str) -> String {
    // Empty is as fatal as unset: an `API_KEY=` line would otherwise make
    // check_key accept a blank x-api-key header, silently disabling auth.
    env_nonempty(name).unwrap_or_else(|| {
        eprintln!("{name} not set (or empty)");
        exit(1);
    })
}

pub(crate) async fn serve(host: String, port: u16) {
    let api_key = require_env("API_KEY");
    let omdb_key = require_env("OMDB_KEY");
    let db_path = env_nonempty("DB_PATH").unwrap_or_else(|| DEFAULT_DB_PATH.to_string());
    let omdb_url = env_nonempty("OMDB_URL").unwrap_or_else(|| DEFAULT_OMDB_URL.to_string());

    let pool = match build_pool(&db_path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to initialize database at {db_path}: {e}");
            exit(1);
        }
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client");

    let state = Arc::new(AppState {
        db: pool,
        db_permits: Arc::new(Semaphore::new(DB_POOL_SIZE as usize)),
        client,
        api_key,
        omdb_key,
        omdb_url,
    });
    let app = Router::new()
        .route("/movies", get(list_movies).post(add_movie))
        .route("/movies/recent", get(get_recent))
        .route("/movies/{imdb_id}", get(get_movie))
        .route("/movies/{imdb_id}/history", get(get_history))
        // Registered before the auth layer so unknown paths and wrong
        // methods still answer 401 first without a valid key.
        .fallback(fallback_not_found)
        .method_not_allowed_fallback(fallback_method_not_allowed)
        .layer(middleware::from_fn_with_state(state.clone(), check_key))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(format!("{host}:{port}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {host}:{port}: {e}");
            exit(1);
        }
    };
    // axum::serve doesn't set TCP_NODELAY. A response written in more than
    // one piece (GET /movies' body is ~270KB) otherwise meets the classic
    // Nagle/delayed-ACK stall on its last partial segment.
    let listener = axum::serve::ListenerExt::tap_io(listener, |tcp_stream| {
        if let Err(e) = tcp_stream.set_nodelay(true) {
            eprintln!("failed to set TCP_NODELAY on incoming connection: {e}");
        }
    });
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("server error: {e}");
        exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spliced(doc: &str, key: &str, value: &str) -> String {
        let mut body = String::new();
        push_with_member(&mut body, doc, key, value);
        body
    }

    #[test]
    fn push_with_member_appends_to_a_stored_doc() {
        // The ordinary case: a doc this server serialized, gaining one field.
        // Byte-for-byte what serde_json would have produced by parsing the doc
        // into a Value, inserting, and re-serializing — that equivalence is
        // the whole justification for not doing so.
        let doc = r#"{"title":"The Matrix","imdb_id":"tt0133093"}"#;
        assert_eq!(
            spliced(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00"),
            r#"{"title":"The Matrix","imdb_id":"tt0133093","last_refreshed":"2026-01-01T00:00:00.000+00:00"}"#
        );
    }

    #[test]
    fn push_with_member_preserves_non_ascii_verbatim() {
        // SQLite's json_set would escape this to é, making the same movie
        // come back byte-different from /movies and /movies/recent. Splicing
        // never touches the existing bytes, which is why it's preferred.
        let doc = r#"{"actors":"Penélope Cruz"}"#;
        let out = spliced(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00");
        assert!(out.contains("Penélope"), "got: {out}");
    }

    #[test]
    fn push_with_member_leaves_anything_it_did_not_write_alone() {
        // A hand-edited row that isn't a non-empty JSON object must come back
        // untouched rather than spliced into invalid JSON.
        for input in ["{}", "", "[]", "null", "not json"] {
            assert_eq!(
                spliced(input, "last_refreshed", "2026-01-01T00:00:00.000+00:00"),
                input
            );
        }
    }

    #[test]
    fn push_with_member_output_stays_parseable() {
        let doc = r#"{"a":1,"b":{"nested":"}"},"c":[1,2]}"#;
        let out = spliced(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00");
        let v: Value = serde_json::from_str(&out).expect("spliced doc parses");
        assert_eq!(v["last_refreshed"], "2026-01-01T00:00:00.000+00:00");
        assert_eq!(v["b"]["nested"], "}");
    }
}
