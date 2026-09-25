//! The `refresh` subcommand: re-pull OMDB data for every stored movie,
//! oldest-refreshed first, preserving each movie's Personal rating.

use std::collections::HashMap;
use std::error::Error;
use std::process::{ExitCode, exit};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use serde_json::{Map, Value};

use crate::db::{ratings_entry_field, set_connection_pragmas, snapshot_ratings};
use crate::util::{
    DEFAULT_OMDB_URL, OmdbError, classify_omdb_error, env_nonempty, normalize_omdb, utcnow,
};

/// Transient failures in a row before the run gives up: one timeout is
/// worth skipping past, but a run of them means OMDB (or the network) is
/// down, and every further movie would just burn its timeout.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// The first ratings entry with source == "Personal". A missing entry or a
/// JSON-null value both mean "not rated yet" and should be skipped; an
/// empty string or 0 is still a real (if odd) rating and must be kept.
fn personal_rating(doc: &Value) -> Option<Value> {
    let ratings = doc.get("ratings").and_then(Value::as_array)?;
    for r in ratings {
        if r.get("source").and_then(Value::as_str) == Some("Personal") {
            return r.get("value").cloned().filter(|v| !v.is_null());
        }
    }
    None
}

fn doc_str<'a>(doc: &'a Value, key: &str, default: &'a str) -> &'a str {
    doc.get(key).and_then(Value::as_str).unwrap_or(default)
}

/// Renders a `--dry-run` diff of every non-Personal rating source between
/// the currently-stored doc and the freshly-fetched one, e.g.
/// `IMDb: "8.7/10" -> "8.8/10", Rotten Tomatoes: (new) -> "95%"`.
fn dry_run_diff(old_doc: &Value, new_ratings: &[Value]) -> String {
    let empty = Vec::new();
    let old_ratings = old_doc
        .get("ratings")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let mut old: HashMap<&str, &Value> = HashMap::new();
    for r in old_ratings {
        old.insert(
            ratings_entry_field(r, "source"),
            r.get("value").unwrap_or(&Value::Null),
        );
    }
    // Insertion-ordered map of the new ratings (last value wins).
    let mut new_pairs: Vec<(&str, &Value)> = Vec::new();
    for r in new_ratings {
        let s = ratings_entry_field(r, "source");
        let v = r.get("value").unwrap_or(&Value::Null);
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
        match old.get(s) {
            Some(old_v) if *old_v == *v => {}
            Some(old_v) => changed.push(format!("{s}: {old_v} -> {v}")),
            None => changed.push(format!("{s}: (new) -> {v}")),
        }
    }
    let mut removed: Vec<String> = Vec::new();
    for (s, old_v) in &old {
        if *s == "Personal" {
            continue;
        }
        if !new_pairs.iter().any(|(k, _)| *k == *s) {
            removed.push(format!("{s}: {old_v} -> (removed)"));
        }
    }
    removed.sort();
    changed.extend(removed);

    if changed.is_empty() {
        "no rating changes".to_string()
    } else {
        changed.join(", ")
    }
}

fn require_omdb_key() -> String {
    env_nonempty("OMDB_KEY").unwrap_or_else(|| {
        eprintln!("OMDB_KEY not set. Try: export $(grep OMDB_KEY /etc/moviedb.env)");
        exit(1);
    })
}

/// Deliberately opened without `SQLITE_OPEN_CREATE` (unlike the server's
/// schema-creating `db::build_pool`): a typo'd `db_path` must fail here at
/// open rather than leave an empty DB file behind at the bad path. Pragmas
/// are per-connection, so they still need setting even though `serve`
/// already put the file into WAL mode.
fn open_db(db_path: &str) -> Connection {
    let mut db = match Connection::open_with_flags(
        db_path,
        OpenFlags::default() - OpenFlags::SQLITE_OPEN_CREATE,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to open database at {db_path}: {e}");
            exit(1);
        }
    };
    if let Err(e) = set_connection_pragmas(&mut db) {
        eprintln!("failed to set connection pragmas: {e}");
        exit(1);
    }
    db
}

/// The `limit` oldest-`_refreshed` movies (all of them if `None`). Rows with
/// no stamp at all sort first.
fn load_movies(db: &Connection, limit: Option<usize>) -> Vec<(String, String)> {
    // SQLite reads a negative LIMIT as "no limit".
    let limit = limit.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX));
    let rows_result = (|| -> rusqlite::Result<Vec<(String, String)>> {
        let mut stmt = db.prepare(
            "
        SELECT imdb_id, data FROM movies
        ORDER BY COALESCE(json_extract(data, '$._refreshed'), '') ASC
        LIMIT ?
        ",
        )?;
        let rows = stmt.query_map([limit], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect()
    })();
    rows_result.unwrap_or_else(|e| {
        eprintln!("query failed: {e}");
        exit(1);
    })
}

fn build_omdb_client() -> reqwest::Client {
    // 15s: generous for a slow OMDB response without letting one stuck
    // request hang an unattended run.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}

/// Fixed config threaded through every `refresh_movie` call.
struct RefreshConfig<'a> {
    client: &'a reqwest::Client,
    omdb_url: &'a str,
    api_key: &'a str,
    dry_run: bool,
}

enum RefreshOutcome {
    Refreshed,
    Skipped,
    /// Worth moving past (a timeout, a garbled response) — the movie keeps
    /// its old `_refreshed`, so it sorts first next run.
    Failed,
    /// OMDB's daily cap was hit: stop cleanly, progress so far is committed.
    DailyLimitReached,
    /// Nothing later in the run can succeed either (a rejected key, an OMDB
    /// error this code doesn't know, a DB write failing): stop and fail.
    Fatal,
}

/// Re-pulls one movie and either prints a dry-run diff or writes the movie
/// plus a `ratings_history` snapshot. Always makes exactly one OMDB request.
async fn refresh_movie(
    db: &mut Connection,
    cfg: &RefreshConfig<'_>,
    imdb_id: &str,
    stored: &Value,
) -> RefreshOutcome {
    let label = doc_str(stored, "title", imdb_id);
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
        // without_url(): the request URL carries the apikey.
        Err(e) => {
            eprintln!(
                "FAIL {label} [{imdb_id}]: OMDB request failed: {}",
                e.without_url()
            );
            return RefreshOutcome::Failed;
        }
    };

    // A doc without a title would persist with a NULL generated `title`
    // column, invisible to ?title= filters — skip rather than write it.
    let Some(title) = omdb.get("Title").and_then(Value::as_str).map(String::from) else {
        println!("SKIP {imdb_id}: OMDB response missing Title — not persisted");
        return RefreshOutcome::Skipped;
    };
    let year = omdb
        .get("Year")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    let now = utcnow();

    if cfg.dry_run {
        let personal = personal_rating(stored).unwrap_or(Value::Null);
        let new_doc = normalize_omdb(omdb, personal, &now);
        let new_ratings = new_doc["ratings"].as_array().map_or(&[][..], Vec::as_slice);
        println!(
            "DRY  {title} ({year}): {}",
            dry_run_diff(stored, new_ratings)
        );
        return RefreshOutcome::Refreshed;
    }

    match write_refreshed(db, imdb_id, &title, omdb, &now) {
        Ok(true) => {
            println!("OK   {title} ({year})");
            RefreshOutcome::Refreshed
        }
        Ok(false) => {
            println!("SKIP {title}: Personal rating removed during the run");
            RefreshOutcome::Skipped
        }
        Err(e) => {
            eprintln!("ABORT at {imdb_id}: write failed: {e}");
            RefreshOutcome::Fatal
        }
    }
}

/// Writes the refreshed doc and its snapshot. The Personal rating is read
/// here, inside the write lock, not taken from the copy loaded at the start
/// of the run: a re-rate POSTed while the run was working through earlier
/// movies would otherwise be overwritten with the old value. Returns false
/// if the movie no longer has a Personal rating to carry over.
fn write_refreshed(
    db: &mut Connection,
    imdb_id: &str,
    title: &str,
    omdb: Map<String, Value>,
    now: &str,
) -> Result<bool, Box<dyn Error>> {
    // Immediate for the same reason as http::upsert_movie: take the write
    // lock up front, where busy_timeout applies.
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current: String = tx.query_row(
        "SELECT data FROM movies WHERE imdb_id = ?",
        params![imdb_id],
        |r| r.get(0),
    )?;
    let Some(personal) = personal_rating(&serde_json::from_str(&current)?) else {
        return Ok(false);
    };
    let new_doc = normalize_omdb(omdb, personal, now);
    let new_ratings = new_doc["ratings"].as_array().map_or(&[][..], Vec::as_slice);
    tx.execute(
        "UPDATE movies SET data = ? WHERE imdb_id = ?",
        params![serde_json::to_string(&new_doc)?, imdb_id],
    )?;
    snapshot_ratings(&tx, imdb_id, title, new_ratings, now)?;
    tx.commit()?;
    Ok(true)
}

pub(crate) async fn refresh(
    db_path: String,
    limit: Option<usize>,
    pause: Duration,
    dry_run: bool,
) -> ExitCode {
    let api_key = require_omdb_key();
    let omdb_url = env_nonempty("OMDB_URL").unwrap_or_else(|| DEFAULT_OMDB_URL.to_string());
    let mut db = open_db(&db_path);
    let rows = load_movies(&db, limit);
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
        let stored: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("FAIL {imdb_id}: stored doc is not valid JSON: {e}");
                failed += 1;
                continue;
            }
        };
        // Checked before the request so an unrated movie costs no quota.
        // The authoritative read is inside the write (see write_refreshed).
        if personal_rating(&stored).is_none() {
            println!(
                "SKIP {}: no Personal rating",
                doc_str(&stored, "title", imdb_id)
            );
            skipped += 1;
            continue;
        }

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
    fn personal_rating_finds_first_personal_source() {
        let doc = json!({
            "ratings": [
                {"source": "IMDb", "value": "8.7/10"},
                {"source": "Personal", "value": "9/10"},
            ]
        });
        assert_eq!(personal_rating(&doc), Some(json!("9/10")));
    }

    #[test]
    fn personal_rating_none_when_missing_ratings_or_source() {
        assert_eq!(personal_rating(&json!({})), None);
        assert_eq!(
            personal_rating(&json!({"ratings": [{"source": "IMDb", "value": "8.7/10"}]})),
            None
        );
    }

    #[test]
    fn personal_rating_none_on_null_value_but_some_on_other_falsy() {
        // A JSON null value means "not rated yet"; an empty string or 0 is
        // a real (if odd) rating and must NOT be treated the same way.
        assert_eq!(
            personal_rating(&json!({"ratings": [{"source": "Personal", "value": null}]})),
            None
        );
        assert_eq!(
            personal_rating(&json!({"ratings": [{"source": "Personal", "value": ""}]})),
            Some(json!(""))
        );
        assert_eq!(
            personal_rating(&json!({"ratings": [{"source": "Personal", "value": 0}]})),
            Some(json!(0))
        );
    }

    #[test]
    fn doc_str_present_and_default() {
        let doc = json!({"title": "The Matrix"});
        assert_eq!(doc_str(&doc, "title", "?"), "The Matrix");
        assert_eq!(doc_str(&doc, "missing", "?"), "?");
    }

    #[test]
    fn dry_run_diff_reports_no_changes_when_ratings_are_identical() {
        let old = json!({"ratings": [{"source": "IMDb", "value": "8.7/10"}]});
        let new_ratings = vec![json!({"source": "IMDb", "value": "8.7/10"})];
        assert_eq!(dry_run_diff(&old, &new_ratings), "no rating changes");
    }

    #[test]
    fn dry_run_diff_reports_changed_and_new_sources_but_skips_personal() {
        let old = json!({"ratings": [
            {"source": "IMDb", "value": "8.7/10"},
            {"source": "Personal", "value": "9/10"},
        ]});
        let new_ratings = vec![
            json!({"source": "IMDb", "value": "8.8/10"}),
            json!({"source": "Rotten Tomatoes", "value": "95%"}),
            json!({"source": "Personal", "value": "9/10"}),
        ];
        assert_eq!(
            dry_run_diff(&old, &new_ratings),
            r#"IMDb: "8.7/10" -> "8.8/10", Rotten Tomatoes: (new) -> "95%""#
        );
    }

    #[test]
    fn dry_run_diff_reports_removed_sources() {
        // A source present in the old doc but absent from the fresh fetch
        // must show up in the diff, not vanish silently.
        let old = json!({"ratings": [
            {"source": "IMDb", "value": "8.7/10"},
            {"source": "Rotten Tomatoes", "value": "95%"},
            {"source": "Personal", "value": "9/10"},
        ]});
        let new_ratings = vec![
            json!({"source": "IMDb", "value": "8.7/10"}),
            json!({"source": "Personal", "value": "9/10"}),
        ];
        assert_eq!(
            dry_run_diff(&old, &new_ratings),
            r#"Rotten Tomatoes: "95%" -> (removed)"#
        );
    }
}
