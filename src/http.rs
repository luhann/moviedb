//! The `serve` subcommand: the axum app and its route handlers. Every
//! endpoint needs a valid `x-api-key`.
//!
//!   POST /movies  ?title=&rating=&year=     fetch from OMDB, store, snapshot ratings
//!   GET  /movies  [?title=] [?year=]        all movies, optionally filtered
//!   GET  /movies/recent  [?limit=]          most recently refreshed (default 10, max 50)
//!   GET  /movies/{imdb_id}                  one movie
//!   GET  /movies/{imdb_id}/history          ratings snapshots, oldest first
//!
//! A movie lives at `/movies/{imdb_id}`. Title and year are filters on the
//! collection, not an address: they aren't unique, so a filter can match
//! any number of movies and always returns an array. Only `/movies/{imdb_id}`
//! can 404.
//!
//! POST writes a `ratings_history` snapshot every time, which is how rating
//! changes get recorded. It returns the stored doc: 201 with `Location` for
//! a new movie, 200 for one that already existed.
//!
//! Errors are RFC 9457 problem-details objects with `type` `about:blank`.
//!
//!   400 malformed path parameter (bad percent-escapes)
//!   401 missing or wrong x-api-key
//!   404 unknown imdb_id, or a path the API doesn't serve
//!   405 wrong method (with `Allow`)
//!   422 query parameter missing, empty or unparseable
//!   502 OMDB couldn't be reached, sent something that isn't JSON, returned
//!       an error we don't recognise, or left out imdbID/Title
//!   503 OMDB's daily quota is used up (with `Retry-After`)
//!   504 OMDB didn't answer in time
//!   500 internal error (details in the server log)

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
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Mutex;

use crate::db::{self, snapshot_ratings};
use crate::util::{
    OmdbError, classify_omdb_error, ct_eq, db_path, normalize_omdb, omdb_url, require_env, utcnow,
};

struct AppState {
    /// One connection is plenty for a single user. It's behind an async
    /// mutex so a request waiting its turn doesn't tie up a thread (see
    /// `with_conn`).
    db: Arc<Mutex<Connection>>,
    client: reqwest::Client,
    api_key: String,
    omdb_key: String,
    omdb_url: String,
}

/// Builds an RFC 9457 problem-details response. The optional `instance`
/// member is left out because most callers don't have the request URI.
fn detail(status: StatusCode, msg: &str) -> Response {
    let body = json!({
        "type": "about:blank",
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

fn internal_error(context: impl std::fmt::Display) -> Response {
    eprintln!("{context}");
    detail(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
}

fn raw_json(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Runs a query whose first column holds JSON documents and returns them as
/// a JSON array, copying the text straight out of SQLite without parsing it.
/// Building one buffer also means the response gets a `Content-Length`
/// instead of being chunked.
fn json_array(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<Response> {
    let mut stmt = conn.prepare_cached(sql)?;
    let mut rows = stmt.query(params)?;
    let mut body = String::from("[");
    while let Some(row) = rows.next()? {
        if body.len() > 1 {
            body.push(',');
        }
        body.push_str(row.get_ref(0)?.as_str()?);
    }
    body.push(']');
    Ok(raw_json(body))
}

/// Runs `f` with the database connection on a blocking thread. rusqlite is
/// synchronous, and running it directly in a handler would block one of the
/// container's few worker threads, stalling other requests too. `f` builds
/// the whole response, since nothing after the query needs to be async. A
/// database error becomes a 500, logged as "failed to {what}".
async fn with_conn<F>(state: &AppState, what: &'static str, f: F) -> Response
where
    F: FnOnce(&mut Connection) -> rusqlite::Result<Response> + Send + 'static,
{
    // Wait for the connection here, before spawn_blocking, so waiting
    // requests don't each hold a blocking thread.
    let mut conn = Arc::clone(&state.db).lock_owned().await;
    tokio::task::spawn_blocking(move || {
        f(&mut conn).unwrap_or_else(|e| internal_error(format!("failed to {what}: {e}")))
    })
    .await
    // Only reachable in dev builds. Release uses panic=abort, so a panic
    // kills the process and systemd restarts it.
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
    /// Your score, a whole number from 0 to 100.
    rating: u8,
    year: String,
}

/// Optional `GET /movies` filters, combined with AND. An empty value such as
/// `?year=` counts as not given.
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

/// `axum::extract::Query`, but a parse failure returns our 422 problem
/// response instead of axum's plain-text 400.
struct Params<T>(T);

impl<T, S> FromRequestParts<S> for Params<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::try_from_uri(&parts.uri)
            .map(|Query(v)| Params(v))
            .map_err(|e| detail(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()))
    }
}

/// `axum::extract::Path<String>`, but a rejection (in practice, bad
/// percent-escapes) returns our 400 problem response instead of axum's
/// plain-text one.
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

/// Stores a movie and its `ratings_history` snapshot in one transaction.
/// Returns 201 with `Location` for a new movie, 200 for an existing one.
async fn upsert_movie(
    state: &AppState,
    imdb_id: String,
    data: String,
    personal: u8,
    snapshot: Vec<Value>,
) -> Response {
    with_conn(state, "write movie", move |conn| {
        let now = utcnow();
        // This must be IMMEDIATE. The transaction reads before it writes, so
        // a deferred one would start as a reader. If the refresh job
        // committed in between, upgrading to a writer would fail with
        // SQLITE_BUSY straight away, without the 5s busy_timeout retry.
        // Taking the write lock up front avoids that.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existed = tx
            .prepare_cached("SELECT 1 FROM movies WHERE imdb_id = ?")?
            .query_row(params![imdb_id], |_| Ok(()))
            .optional()?
            .is_some();
        tx.execute(
            "INSERT OR REPLACE INTO movies (imdb_id, data, personal, refreshed) VALUES (?, ?, ?, ?)",
            params![imdb_id, data, personal, now],
        )?;
        snapshot_ratings(&tx, &imdb_id, &snapshot, &now)?;
        let doc: String = tx.query_row(
            "SELECT doc FROM movie_docs WHERE imdb_id = ?",
            params![imdb_id],
            |r| r.get(0),
        )?;
        tx.commit()?;

        let status = if existed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        };
        let mut resp = (status, [(header::CONTENT_TYPE, "application/json")], doc).into_response();
        if !existed && let Ok(location) = format!("/movies/{imdb_id}").parse() {
            resp.headers_mut().insert(header::LOCATION, location);
        }
        Ok(resp)
    })
    .await
}

async fn add_movie(State(state): State<Arc<AppState>>, Params(p): Params<AddParams>) -> Response {
    // Empty counts as missing, as it does for the GET filters. Otherwise
    // `?title=` would be sent to OMDB and come back as a 404 or 502
    // instead of a 422.
    if p.title.is_empty() || p.year.is_empty() {
        return detail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "title and year must be non-empty",
        );
    }
    if p.rating > 100 {
        return detail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "rating must be a whole number from 0 to 100",
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
        Err(e) => return omdb_unreachable(e),
    };
    let omdb = match resp.json::<Value>().await {
        Ok(Value::Object(obj)) if obj.get("Response").and_then(Value::as_str) == Some("True") => {
            obj
        }
        Ok(other) => {
            return omdb_failure(other.get("Error").and_then(Value::as_str).unwrap_or(""), &p);
        }
        Err(e) => return omdb_unreachable(e),
    };

    let doc = normalize_omdb(omdb);
    let Some(imdb_id) = doc.get("imdb_id").and_then(Value::as_str).map(String::from) else {
        eprintln!("OMDB response missing imdbID");
        return detail(StatusCode::BAD_GATEWAY, "OMDB response missing imdbID");
    };
    // Without a title the movie could never be found by a ?title= filter.
    if doc.get("title").and_then(Value::as_str).is_none() {
        eprintln!("OMDB response missing Title");
        return detail(StatusCode::BAD_GATEWAY, "OMDB response missing Title");
    }
    // History records your rating alongside OMDB's.
    let mut snapshot = doc["ratings"].as_array().cloned().unwrap_or_default();
    snapshot.push(json!({ "source": "Personal", "value": p.rating.to_string() }));
    let data = match serde_json::to_string(&doc) {
        Ok(s) => s,
        Err(e) => return internal_error(format!("failed to serialize movie doc: {e}")),
    };
    upsert_movie(&state, imdb_id, data, p.rating, snapshot).await
}

/// OMDB didn't give us a JSON answer: it timed out, couldn't be reached, or
/// sent something else.
fn omdb_unreachable(e: reqwest::Error) -> Response {
    // without_url() keeps the OMDB API key, which is in the query string,
    // out of the log.
    let e = e.without_url();
    eprintln!("OMDB request failed: {e}");
    let (status, msg) = if e.is_timeout() {
        (StatusCode::GATEWAY_TIMEOUT, "OMDB did not respond in time")
    } else if e.is_connect() {
        (StatusCode::BAD_GATEWAY, "Could not connect to OMDB")
    } else {
        (StatusCode::BAD_GATEWAY, "Bad response from OMDB")
    };
    detail(status, msg)
}

/// Turns an OMDB error response into ours.
fn omdb_failure(error: &str, p: &AddParams) -> Response {
    match classify_omdb_error(error) {
        OmdbError::DailyLimit => {
            // 503 rather than 429, because it's OMDB's quota that ran out,
            // not the caller who's being limited. OMDB doesn't say when the
            // quota resets, so ask for a retry in 24h to be safe.
            let mut resp = detail(
                StatusCode::SERVICE_UNAVAILABLE,
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
    with_conn(&state, "list movies", move |conn| {
        let title = p.title.as_deref().filter(|s| !s.is_empty());
        let year = p.year.as_deref().filter(|s| !s.is_empty());
        let (sql, filters) = match (title, year) {
            (None, None) => ("SELECT doc FROM movie_docs", vec![]),
            (Some(t), None) => ("SELECT doc FROM movie_docs WHERE title = ?", vec![t]),
            (None, Some(y)) => ("SELECT doc FROM movie_docs WHERE year = ?", vec![y]),
            (Some(t), Some(y)) => (
                "SELECT doc FROM movie_docs WHERE title = ? AND year = ?",
                vec![t, y],
            ),
        };
        json_array(conn, sql, params_from_iter(filters))
    })
    .await
}

/// The dashboard's "recently catalogued" list: the N most recently
/// refreshed movies, newest first.
async fn get_recent(
    State(state): State<Arc<AppState>>,
    Params(p): Params<RecentParams>,
) -> Response {
    let limit = p
        .limit
        .map_or(DEFAULT_RECENT_LIMIT, |n| n.min(MAX_RECENT_LIMIT));
    with_conn(&state, "list recent movies", move |conn| {
        json_array(
            conn,
            "
            SELECT doc FROM movie_docs
            ORDER BY refreshed DESC, imdb_id DESC
            LIMIT ?
            ",
            [limit as i64],
        )
    })
    .await
}

async fn get_movie(State(state): State<Arc<AppState>>, ImdbId(imdb_id): ImdbId) -> Response {
    with_conn(&state, "fetch movie", move |conn| {
        let data = conn
            .prepare_cached("SELECT doc FROM movie_docs WHERE imdb_id = ?")?
            .query_row(params![imdb_id], |r| r.get::<_, String>(0))
            .optional()?;
        Ok(match data {
            Some(data) => raw_json(data),
            None => detail(StatusCode::NOT_FOUND, "Movie not found"),
        })
    })
    .await
}

async fn get_history(State(state): State<Arc<AppState>>, ImdbId(imdb_id): ImdbId) -> Response {
    with_conn(&state, "fetch history", move |conn| {
        // One query instead of checking the movie exists first. With the
        // LEFT JOIN, no rows means an unknown movie (404), and one row of
        // NULLs means a known movie with no snapshots yet (empty history).
        //
        // The primary key already gives the ORDER BY order, so SQLite does
        // two index lookups and no sort (checked with EXPLAIN QUERY PLAN).
        let mut stmt = conn.prepare_cached(
            "
            SELECT h.observed, h.source, h.value
            FROM movies m LEFT JOIN ratings_history h ON h.imdb_id = m.imdb_id
            WHERE m.imdb_id = ? ORDER BY h.observed ASC, h.source ASC
            ",
        )?;
        let rows = stmt
            .query_map(params![imdb_id], |r| {
                Ok(match r.get::<_, Option<String>>(0)? {
                    Some(observed) => Some(json!({
                        "observed": observed,
                        "source": r.get::<_, String>(1)?,
                        "value": r.get::<_, String>(2)?,
                    })),
                    None => None,
                })
            })?
            .collect::<rusqlite::Result<Vec<Option<Value>>>>()?;
        if rows.is_empty() {
            return Ok(detail(StatusCode::NOT_FOUND, "Movie not found"));
        }
        let snapshots: Vec<Value> = rows.into_iter().flatten().collect();
        Ok(Json(json!({ "imdb_id": imdb_id, "snapshots": snapshots })).into_response())
    })
    .await
}

/// axum's own 404 and 405 have empty bodies, so replace them with problem
/// responses. axum still adds the `Allow` header to the 405.
async fn fallback_not_found() -> Response {
    detail(StatusCode::NOT_FOUND, "Not Found")
}

async fn fallback_method_not_allowed() -> Response {
    detail(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed")
}

/// Waits for SIGTERM or SIGINT so axum can finish in-flight requests before
/// exiting, rather than dying mid-response. If that hangs, systemd sends
/// SIGKILL after `TimeoutStopSec` (90s by default).
async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = sigint.recv() => {},
    }
    eprintln!("shutdown signal received, draining in-flight requests");
}

pub(crate) async fn serve(host: String, port: u16) {
    let api_key = require_env("API_KEY");
    let omdb_key = require_env("OMDB_KEY");
    let db_path = db_path();
    let omdb_url = omdb_url();

    let db = match db::open(&db_path) {
        Ok(db) => db,
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
        db: Arc::new(Mutex::new(db)),
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
        // Added before the auth layer, so an unknown path or wrong method
        // without a valid key still gets a 401.
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
    // axum::serve doesn't set TCP_NODELAY. Without it, large responses
    // (GET /movies is ~270KB) can stall on the last packet because of Nagle
    // and delayed ACKs.
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
