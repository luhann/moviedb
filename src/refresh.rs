//! The `refresh` subcommand: re-fetch OMDB data for every stored movie,
//! least recently refreshed first. It only ever replaces OMDB's data; your
//! rating is in its own column and never touched.

use std::error::Error;
use std::process::{ExitCode, exit};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use serde_json::{Map, Value};

use crate::db::{ratings_entry_field, set_connection_pragmas, snapshot_ratings};
use crate::util::{OmdbError, classify_omdb_error, normalize_omdb, omdb_url, require_env, utcnow};

/// How many failures in a row end the run. One timeout is worth skipping,
/// but several in a row means OMDB or the network is down, and every
/// remaining movie would just time out too.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;

fn doc_str<'a>(doc: &'a Value, key: &str, default: &'a str) -> &'a str {
    doc.get(key).and_then(Value::as_str).unwrap_or(default)
}

fn ratings_of(doc: &Map<String, Value>) -> &[Value] {
    doc.get("ratings")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// Each source's rating value, in order. If a source appears twice, the
/// last value wins.
fn ratings_by_source(ratings: &[Value]) -> Map<String, Value> {
    ratings
        .iter()
        .map(|r| {
            let value = r.get("value").cloned().unwrap_or(Value::Null);
            (ratings_entry_field(r, "source").to_string(), value)
        })
        .collect()
}

/// The `--dry-run` summary of what changed between the stored ratings and
/// the new ones. For example:
/// `IMDb: "8.7/10" -> "8.8/10", Rotten Tomatoes: (new) -> "95%"`.
fn dry_run_diff(old_ratings: &[Value], new_ratings: &[Value]) -> String {
    let old = ratings_by_source(old_ratings);
    let new = ratings_by_source(new_ratings);

    let mut changes: Vec<String> = new
        .iter()
        .filter_map(|(source, value)| match old.get(source) {
            Some(old_value) if old_value == value => None,
            Some(old_value) => Some(format!("{source}: {old_value} -> {value}")),
            None => Some(format!("{source}: (new) -> {value}")),
        })
        .collect();
    changes.extend(
        old.iter()
            .filter(|(source, _)| !new.contains_key(*source))
            .map(|(source, old_value)| format!("{source}: {old_value} -> (removed)")),
    );

    if changes.is_empty() {
        "no rating changes".to_string()
    } else {
        changes.join(", ")
    }
}

/// Opens the database without `SQLITE_OPEN_CREATE`, so a mistyped path fails
/// instead of creating an empty database there. The pragmas still need
/// setting because they only apply to one connection.
fn open_db(db_path: &str) -> Connection {
    let db = match Connection::open_with_flags(
        db_path,
        OpenFlags::default() - OpenFlags::SQLITE_OPEN_CREATE,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to open database at {db_path}: {e}");
            exit(1);
        }
    };
    if let Err(e) = set_connection_pragmas(&db) {
        eprintln!("failed to set connection pragmas: {e}");
        exit(1);
    }
    db
}

/// The `limit` least recently refreshed movies, or all of them if `None`.
fn load_movies(db: &Connection, limit: Option<usize>) -> rusqlite::Result<Vec<(String, String)>> {
    // SQLite reads a negative LIMIT as "no limit".
    let limit = limit.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX));
    db.prepare("SELECT imdb_id, data FROM movies ORDER BY refreshed, imdb_id LIMIT ?")?
        .query_map([limit], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

fn build_omdb_client() -> reqwest::Client {
    // Long enough for a slow OMDB, short enough that one stuck request
    // can't hang the run.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}

/// Settings that stay the same for every `refresh_movie` call.
struct RefreshConfig<'a> {
    client: &'a reqwest::Client,
    omdb_url: &'a str,
    api_key: &'a str,
    dry_run: bool,
}

enum RefreshOutcome {
    Refreshed,
    Skipped,
    /// A timeout or bad response. Carry on with the next movie. This one
    /// keeps its old `refreshed`, so it goes first next run.
    Failed,
    /// OMDB's daily limit was hit. Stop; everything so far is saved.
    DailyLimitReached,
    /// Nothing else in the run will work either (the key was rejected, OMDB
    /// sent an error we don't know, or a DB write failed). Stop and fail.
    Fatal,
}

/// Re-fetches one movie, then either prints the dry-run diff or saves it.
/// Makes exactly one OMDB request.
async fn refresh_movie(
    db: &mut Connection,
    cfg: &RefreshConfig<'_>,
    imdb_id: &str,
    stored: &Map<String, Value>,
) -> RefreshOutcome {
    let label = stored
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or(imdb_id);
    let response: reqwest::Result<Value> = async {
        cfg.client
            .get(cfg.omdb_url)
            .query(&[("apikey", cfg.api_key), ("i", imdb_id)])
            .send()
            .await?
            .json()
            .await
    }
    .await;
    let omdb = match response {
        Ok(Value::Object(obj)) if obj.get("Response").and_then(Value::as_str) == Some("True") => {
            obj
        }
        Ok(other) => {
            let error = doc_str(&other, "Error", "");
            return match classify_omdb_error(error) {
                OmdbError::DailyLimit => RefreshOutcome::DailyLimitReached,
                OmdbError::NotFound => {
                    println!("SKIP {label} [{imdb_id}]: {error}");
                    RefreshOutcome::Skipped
                }
                OmdbError::Other => {
                    eprintln!("ABORT at {label} [{imdb_id}]: OMDB error {error:?}");
                    RefreshOutcome::Fatal
                }
            };
        }
        // without_url() keeps the API key out of the log.
        Err(e) => {
            eprintln!(
                "FAIL {label} [{imdb_id}]: OMDB request failed: {}",
                e.without_url()
            );
            return RefreshOutcome::Failed;
        }
    };

    let doc = normalize_omdb(omdb);
    // Without a title the movie could never be found by a ?title= filter,
    // so don't save it.
    let Some(title) = doc.get("title").and_then(Value::as_str) else {
        println!("SKIP {imdb_id}: OMDB response missing Title — not persisted");
        return RefreshOutcome::Skipped;
    };
    let year = doc.get("year").and_then(Value::as_str).unwrap_or("?");

    if cfg.dry_run {
        let diff = dry_run_diff(ratings_of(stored), ratings_of(&doc));
        println!("DRY  {title} ({year}): {diff}");
        return RefreshOutcome::Refreshed;
    }
    match write_refreshed(db, imdb_id, &doc) {
        Ok(()) => {
            println!("OK   {title} ({year})");
            RefreshOutcome::Refreshed
        }
        Err(e) => {
            eprintln!("ABORT at {imdb_id}: write failed: {e}");
            RefreshOutcome::Fatal
        }
    }
}

/// Replaces the movie's OMDB data and snapshots OMDB's ratings, in one
/// transaction.
fn write_refreshed(
    db: &mut Connection,
    imdb_id: &str,
    doc: &Map<String, Value>,
) -> Result<(), Box<dyn Error>> {
    let now = utcnow();
    // Take the write lock up front, where busy_timeout applies.
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "UPDATE movies SET data = ?, refreshed = ? WHERE imdb_id = ?",
        params![serde_json::to_string(doc)?, now, imdb_id],
    )?;
    snapshot_ratings(&tx, imdb_id, ratings_of(doc), &now)?;
    tx.commit()?;
    Ok(())
}

pub(crate) async fn refresh(
    db_path: String,
    limit: Option<usize>,
    pause: Duration,
    dry_run: bool,
) -> ExitCode {
    let api_key = require_env("OMDB_KEY");
    let omdb_url = omdb_url();
    let mut db = open_db(&db_path);
    let rows = load_movies(&db, limit).unwrap_or_else(|e| {
        eprintln!("failed to load movies: {e}");
        exit(1);
    });
    let total = rows.len();
    let client = build_omdb_client();
    let cfg = RefreshConfig {
        client: &client,
        omdb_url: &omdb_url,
        api_key: &api_key,
        dry_run,
    };

    let (mut refreshed, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    let mut consecutive_failures = 0;
    let mut fatal = false;
    for (imdb_id, data) in &rows {
        let stored: Map<String, Value> = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("FAIL {imdb_id}: stored doc is not a JSON object: {e}");
                failed += 1;
                continue;
            }
        };

        match refresh_movie(&mut db, &cfg, imdb_id, &stored).await {
            RefreshOutcome::Refreshed => {
                refreshed += 1;
                consecutive_failures = 0;
            }
            RefreshOutcome::Skipped => {
                skipped += 1;
                consecutive_failures = 0;
            }
            RefreshOutcome::Failed => {
                failed += 1;
                consecutive_failures += 1;
                if consecutive_failures == MAX_CONSECUTIVE_FAILURES {
                    eprintln!("ABORT: {MAX_CONSECUTIVE_FAILURES} OMDB requests failed in a row");
                    fatal = true;
                    break;
                }
            }
            RefreshOutcome::DailyLimitReached => {
                println!(
                    "OMDB daily limit hit after {refreshed} refreshes. Re-run tomorrow — progress is saved."
                );
                break;
            }
            RefreshOutcome::Fatal => {
                fatal = true;
                break;
            }
        }
        tokio::time::sleep(pause).await;
    }

    let processed = refreshed + skipped + failed;
    let verb = if dry_run { "checked" } else { "refreshed" };
    println!(
        "\nDone: {refreshed} {verb}, {skipped} skipped, {failed} failed, {} remaining.",
        total - processed
    );
    if fatal || failed > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn doc_str_present_and_default() {
        let doc = json!({"title": "The Matrix"});
        assert_eq!(doc_str(&doc, "title", "?"), "The Matrix");
        assert_eq!(doc_str(&doc, "missing", "?"), "?");
    }

    #[test]
    fn dry_run_diff_reports_no_changes_when_ratings_are_identical() {
        let ratings = [json!({"source": "IMDb", "value": "8.7/10"})];
        assert_eq!(dry_run_diff(&ratings, &ratings), "no rating changes");
    }

    #[test]
    fn dry_run_diff_reports_changed_and_new_sources() {
        let old = [json!({"source": "IMDb", "value": "8.7/10"})];
        let new = [
            json!({"source": "IMDb", "value": "8.8/10"}),
            json!({"source": "Rotten Tomatoes", "value": "95%"}),
        ];
        assert_eq!(
            dry_run_diff(&old, &new),
            r#"IMDb: "8.7/10" -> "8.8/10", Rotten Tomatoes: (new) -> "95%""#
        );
    }

    #[test]
    fn dry_run_diff_reports_removed_sources() {
        let old = [
            json!({"source": "IMDb", "value": "8.7/10"}),
            json!({"source": "Rotten Tomatoes", "value": "95%"}),
        ];
        let new = [json!({"source": "IMDb", "value": "8.7/10"})];
        assert_eq!(
            dry_run_diff(&old, &new),
            r#"Rotten Tomatoes: "95%" -> (removed)"#
        );
    }
}
