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
//! multiple/zero matches are data, not errors, so the old 300/404 answers
//! for title+year lookups are gone with the lookup endpoint itself. Only
//! `/movies/{imdb_id}` can 404. Every POST snapshots the full ratings array
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

use std::env;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{FromRequestParts, Path, Query, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use futures_util::stream;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;

use crate::db::{DB_POOL_SIZE, build_pool, snapshot_ratings};
use crate::util::{DEFAULT_OMDB_URL, ct_eq, snake_case, snake_case_entry_keys, utcnow};

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

/// Streams rows that are each already a complete JSON object as one JSON
/// array. Same reasoning as `raw_json`: the bytes came out of a `data` column
/// we wrote, so they go out as array elements without ever becoming a `Value`
/// tree, and without being concatenated into one contiguous buffer first.
fn json_array_response(rows: Vec<String>) -> Response {
    let n = rows.len();
    let chunks = std::iter::once(Bytes::from_static(b"["))
        .chain(rows.into_iter().enumerate().map(move |(i, doc)| {
            let mut buf = doc.into_bytes();
            if i + 1 < n {
                buf.push(b',');
            }
            Bytes::from(buf)
        }))
        .chain(std::iter::once(Bytes::from_static(b"]")))
        .map(Ok::<_, std::convert::Infallible>);
    (
        [(header::CONTENT_TYPE, "application/json")],
        Body::from_stream(stream::iter(chunks)),
    )
        .into_response()
}

/// Appends one string-valued member to a stored movie doc without parsing it,
/// preserving `raw_json`'s zero-round-trip property for responses that need to
/// add a field. Leans on the same invariant: a `data` column always holds a
/// non-empty JSON object this server serialized, so it ends in `}` and already
/// has a member to comma-separate from. Anything else (a hand-edited row) is
/// returned untouched rather than spliced into invalid JSON. Callers pass
/// their own fixed-format ASCII timestamps, which need no escaping.
fn splice_member(mut doc: String, key: &str, value: &str) -> String {
    if doc.len() < 3 || !doc.ends_with('}') {
        return doc;
    }
    doc.truncate(doc.len() - 1);
    doc.push_str(",\"");
    doc.push_str(key);
    doc.push_str("\":\"");
    doc.push_str(value);
    doc.push_str("\"}");
    doc
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

/// Like `axum::extract::Query`, but a parse failure returns this API's own
/// `{"detail": "..."}` JSON shape (422) instead of axum's default rejection
/// (400, plain text) — every error this API returns has the same shape.
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

fn personal_entry(rating: &str) -> Value {
    json!({ "source": "Personal", "value": rating })
}

/// `axum::extract::Path<String>`, but a rejection (in practice only
/// undecodable percent-escapes in the path segment) comes back in this
/// API's `{"detail": "..."}` shape as a 400 — malformed request syntax —
/// instead of axum's plain-text default.
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

/// Normalizes a Response=True OMDB payload into the shape it's stored and
/// served in: every key snake_cased (Map preserves insertion order via the
/// `preserve_order` feature, so this keeps OMDB's original field order rather
/// than an arbitrary hash order), `Ratings` folded into `ratings` with each
/// entry's keys snake_cased and the Personal entry appended, and the
/// now-redundant `response` field dropped.
fn normalize_omdb_movie(movie: Value, rating: &str) -> Result<Map<String, Value>, Box<Response>> {
    // Consume `movie` (rather than borrow+clone every field) — it's not
    // read again after this, so moving each value into `out` skips a clone
    // of every OMDB field (Plot, Actors, Poster, ...) per request.
    let Value::Object(obj) = movie else {
        return Err(Box::new(internal_error(
            "OMDB response was not a JSON object",
        )));
    };
    let mut out = Map::new();
    for (key, value) in obj {
        if key == "Ratings" {
            let mut ratings = match value {
                Value::Array(a) => snake_case_entry_keys(a),
                _ => Vec::new(),
            };
            ratings.push(personal_entry(rating));
            out.insert("ratings".to_string(), Value::Array(ratings));
        } else {
            out.insert(snake_case(&key), value);
        }
    }
    if !out.contains_key("ratings") {
        out.insert(
            "ratings".to_string(),
            Value::Array(vec![personal_entry(rating)]),
        );
    }
    // shift_remove, not remove: with preserve_order, plain remove is a
    // swap_remove and would scramble the remaining keys' order.
    out.shift_remove("response");
    Ok(out)
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
    let movie: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            return internal_error(format!(
                "OMDB response JSON parse failed: {}",
                e.without_url()
            ));
        }
    };

    if movie.get("Response").and_then(Value::as_str) == Some("True") {
        let out = match normalize_omdb_movie(movie, &p.rating) {
            Ok(out) => out,
            Err(resp) => return *resp,
        };

        let Some(imdbid) = out.get("imdb_id").and_then(Value::as_str).map(String::from) else {
            // OMDB violating its own contract (Response=True without an
            // imdbID) — nothing sensible to persist or serve.
            return internal_error("OMDB response missing imdbID");
        };
        let Some(title) = out.get("title").and_then(Value::as_str).map(String::from) else {
            return internal_error("OMDB response missing Title");
        };
        // Owned, not borrowed: the write below runs on a blocking-pool thread
        // (see with_conn), which needs a 'static closure — a &[Value] into
        // `out` can't cross that boundary.
        let ratings: Vec<Value> = out
            .get("ratings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let now = utcnow();
        let data = match serde_json::to_string(&out) {
            Ok(s) => s,
            Err(e) => return internal_error(format!("failed to serialize movie doc: {e}")),
        };
        return upsert_movie(&state, imdbid, title, ratings, data, now).await;
    }

    let error = movie.get("Error").and_then(Value::as_str).unwrap_or("");
    if error == "Daily request limit reached!" {
        // Our own request rate isn't the problem — OMDB's shared daily quota
        // is exhausted. 503 (not 429) is the correct signal for "this
        // service's upstream dependency is temporarily unavailable, retry
        // later", as opposed to "you personally are being rate-limited".
        // Retry-After is a conservative fixed 24h; OMDB doesn't document
        // exactly when its daily counter resets.
        let mut resp = problem(
            StatusCode::SERVICE_UNAVAILABLE,
            PROBLEM_OMDB_QUOTA,
            "OMDB's daily request limit is exhausted; try again later",
        );
        resp.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("86400"));
        return resp;
    }
    if error == "Movie not found!" {
        return detail(StatusCode::NOT_FOUND, "Movie not found");
    }
    eprintln!(
        "OMDB returned an unrecognized error for {} ({}): {error}",
        p.title, p.year
    );
    // 502: this server acted as a client to OMDB and got back an error it
    // doesn't recognize — a Bad Gateway in the literal sense, and a real,
    // standard HTTP status (unlike the Cloudflare-specific 520 this used to
    // return).
    detail(StatusCode::BAD_GATEWAY, "Unrecognized error from OMDB")
}

async fn list_movies(
    State(state): State<Arc<AppState>>,
    Params(p): Params<FilterParams>,
) -> Response {
    with_conn(&state, move |conn| {
        let title = p.title.as_deref().filter(|s| !s.is_empty());
        let year = p.year.as_deref().filter(|s| !s.is_empty());
        // A filter is just a narrower collection: several matches are all
        // returned, zero matches is `[]` — with the non-unique (title, year)
        // key, both are ordinary answers, not the 300/404 errors the old
        // exactly-one /single lookup had to hand out.
        let (sql, filters) = match (title, year) {
            (None, None) => ("SELECT data FROM movies", vec![]),
            (Some(t), None) => ("SELECT data FROM movies WHERE title = ?", vec![t]),
            (None, Some(y)) => ("SELECT data FROM movies WHERE year = ?", vec![y]),
            (Some(t), Some(y)) => (
                "SELECT data FROM movies WHERE title = ? AND year = ?",
                vec![t, y],
            ),
        };
        let result = (|| -> rusqlite::Result<Vec<String>> {
            // prepare_cached, not prepare: pooled connections are long-lived,
            // so SQLite parses and plans each of the four literal SQL variants
            // once per connection instead of once per request.
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(filters), |r| {
                r.get::<_, String>(0)
            })?;
            rows.collect()
        })();
        match result {
            Ok(raw) => json_array_response(raw),
            Err(e) => internal_error(format!("failed to list movies: {e}")),
        }
    })
    .await
}

/// The dashboard's "recently catalogued" view: the N movies with the most
/// recent `ratings_history` snapshot, newest first. Each doc needs its
/// `last_refreshed` timestamp folded in, which `splice_member` does textually
/// — so this keeps `list_movies`' zero-parse property instead of round-tripping
/// every doc through a `Value` tree just to add one field.
///
/// Still buffered into one body rather than streamed like `list_movies`, and
/// that is load-bearing rather than an oversight: this endpoint is
/// `LIMIT`-bounded to `MAX_RECENT_LIMIT`, so one write with a `Content-Length`
/// beats a chunked response whose framing overhead, on a body this small,
/// costs more than the parse the splice already saved. Measured — streaming it
/// was 14-18% *slower* than the version this replaced.
async fn get_recent(
    State(state): State<Arc<AppState>>,
    Params(p): Params<RecentParams>,
) -> Response {
    let limit = p
        .limit
        .unwrap_or(DEFAULT_RECENT_LIMIT)
        .min(MAX_RECENT_LIMIT);
    with_conn(&state, move |conn| {
        let result = (|| -> rusqlite::Result<Vec<(String, String)>> {
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
            let rows = stmt.query_map(params![limit as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            rows.collect()
        })();
        match result {
            Ok(raw) => {
                // `last_refreshed` plus its quoting and separator adds a fixed
                // ~50 bytes per doc; sizing up front keeps this to one alloc.
                let mut body =
                    String::with_capacity(raw.iter().map(|(d, _)| d.len() + 50).sum::<usize>() + 2);
                body.push('[');
                for (i, (data, last_refreshed)) in raw.into_iter().enumerate() {
                    if i > 0 {
                        body.push(',');
                    }
                    body.push_str(&splice_member(data, "last_refreshed", &last_refreshed));
                }
                body.push(']');
                raw_json(body)
            }
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

/// axum's built-in responses for an unmatched path (404) and a matched path
/// with an unsupported method (405) have empty bodies — these two replace
/// them so the `{"detail": ...}` contract holds on every error, not just the
/// handler-level ones. axum still sets the `Allow` header on the 405.
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
    // Empty is as fatal as unset: an `API_KEY=` line in the env file would
    // otherwise make check_key accept a blank x-api-key header (empty header
    // values are legal HTTP), silently disabling auth.
    match env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("{name} not set (or empty)");
            exit(1);
        }
    }
}

pub(crate) async fn serve(host: String, port: u16) {
    let api_key = require_env("API_KEY");
    let omdb_key = require_env("OMDB_KEY");
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| DEFAULT_DB_PATH.to_string());
    let omdb_url = env::var("OMDB_URL").unwrap_or_else(|_| DEFAULT_OMDB_URL.to_string());

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
    // axum::serve doesn't set TCP_NODELAY on accepted sockets. GET /movies'
    // chunked body (no Content-Length, since the row count isn't known until
    // the query runs) writes headers and the first chunk as separate TCP
    // segments; without NODELAY that hits the classic Nagle/delayed-ACK
    // stall — a flat ~35ms tax on every uncontended list, confirmed by
    // benchmark. GET /movies/{imdb_id} is unaffected (known Content-Length,
    // one write).
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

    #[test]
    fn splice_member_appends_to_a_stored_doc() {
        // The ordinary case: a doc this server serialized, gaining one field.
        // Byte-for-byte what serde_json would have produced by parsing the doc
        // into a Value, inserting, and re-serializing — that equivalence is
        // the whole justification for not doing so.
        let doc = r#"{"title":"The Matrix","imdb_id":"tt0133093"}"#.to_string();
        assert_eq!(
            splice_member(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00"),
            r#"{"title":"The Matrix","imdb_id":"tt0133093","last_refreshed":"2026-01-01T00:00:00.000+00:00"}"#
        );
    }

    #[test]
    fn splice_member_preserves_non_ascii_verbatim() {
        // SQLite's json_set would escape this to é, making the same movie
        // come back byte-different from /movies and /movies/recent. Splicing
        // never touches the existing bytes, which is why it's preferred.
        let doc = r#"{"actors":"Penélope Cruz"}"#.to_string();
        let out = splice_member(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00");
        assert!(out.contains("Penélope"), "got: {out}");
    }

    #[test]
    fn splice_member_leaves_anything_it_did_not_write_alone() {
        // A hand-edited row that isn't a non-empty JSON object must come back
        // untouched rather than spliced into invalid JSON.
        for input in ["{}", "", "[]", "null", "not json"] {
            assert_eq!(
                splice_member(
                    input.to_string(),
                    "last_refreshed",
                    "2026-01-01T00:00:00.000+00:00"
                ),
                input
            );
        }
    }

    #[test]
    fn splice_member_output_stays_parseable() {
        let doc = r#"{"a":1,"b":{"nested":"}"},"c":[1,2]}"#.to_string();
        let out = splice_member(doc, "last_refreshed", "2026-01-01T00:00:00.000+00:00");
        let v: Value = serde_json::from_str(&out).expect("spliced doc parses");
        assert_eq!(v["last_refreshed"], "2026-01-01T00:00:00.000+00:00");
        assert_eq!(v["b"]["nested"], "}");
    }
}
