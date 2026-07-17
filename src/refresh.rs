//! The `refresh` subcommand: re-pull OMDB data for every stored movie,
//! oldest-refreshed first, preserving each movie's Personal rating.

use std::collections::HashMap;
use std::env;
use std::process::exit;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use serde_json::{Map, Value, json};

use crate::db::{ratings_entry_field, set_connection_pragmas, snapshot_ratings};
use crate::util::{DEFAULT_OMDB_URL, snake_case, snake_case_entry_keys, utcnow};

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

/// Snake-cases every OMDB field name (matching the schema `movies.data` is
/// stored and served in), folds `Ratings` into `ratings` with each entry's
/// keys snake_cased, appends the preserved Personal rating, drops the
/// now-redundant `response` field, and stamps `_refreshed` so the next
/// run's oldest-first ordering advances.
fn rebuild_doc(omdb: &Map<String, Value>, personal_value: &Value, now: &str) -> Map<String, Value> {
    let mut out = Map::new();
    for (key, value) in omdb {
        if key == "Ratings" {
            out.insert(
                "ratings".to_string(),
                Value::Array(snake_case_entry_keys(
                    value.as_array().cloned().unwrap_or_default(),
                )),
            );
        } else {
            out.insert(snake_case(key), value.clone());
        }
    }
    if !out.contains_key("ratings") {
        out.insert("ratings".to_string(), Value::Array(Vec::new()));
    }
    out.get_mut("ratings")
        .and_then(Value::as_array_mut)
        .expect("ratings is a list")
        .push(json!({ "source": "Personal", "value": personal_value }));
    // shift_remove, not remove: with preserve_order, plain remove is a
    // swap_remove and would scramble the remaining keys' order.
    out.shift_remove("response");
    out.insert("_refreshed".to_string(), Value::String(now.to_string()));
    out
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
    if changed.is_empty() {
        "no rating changes".to_string()
    } else {
        changed.join(", ")
    }
}

fn require_omdb_key() -> String {
    match env::var("OMDB_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("OMDB_KEY not set. Try: export $(grep OMDB_KEY /etc/moviedb.env)");
            exit(1);
        }
    }
}

/// Deliberately opened without `SQLITE_OPEN_CREATE` (unlike the server's
/// schema-creating `db::build_pool`): a typo'd `db_path` must fail right
/// here at open — a default `Connection::open` would create an empty DB
/// file at the bad path and only fail at the first SELECT, leaving the
/// stray file behind. Pragmas are per-connection though (see
/// `set_connection_pragmas`), so they still need setting here even though
/// `serve` already put the file into WAL mode.
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

/// All movies, oldest-`_refreshed` first (never-refreshed movies sort
/// first, so freshly-added ones get picked up before anything else), capped
/// to the first `limit` if given.
fn load_movies(db: &Connection, limit: Option<usize>) -> Vec<(String, String)> {
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
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

fn build_omdb_client() -> reqwest::Client {
    // 15s timeout: generous for a slow OMDB response without letting one
    // stuck request hang an entire (potentially unattended, cron-driven) run.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client")
}

/// Fixed config threaded through every `refresh_movie` call — grouped into
/// one struct so adding a call doesn't mean adding another positional arg.
struct RefreshConfig<'a> {
    client: &'a reqwest::Client,
    omdb_url: &'a str,
    api_key: &'a str,
    sleep_secs: f64,
    dry_run: bool,
}

enum RefreshOutcome {
    Refreshed,
    Skipped,
    /// OMDB's daily cap was hit fetching this movie — stop the whole run,
    /// not just this one movie; progress so far is already committed.
    DailyLimitReached,
}

/// Refreshes a single movie: skips it if it has no Personal rating yet, or
/// if OMDB no longer recognizes it; otherwise merges the fresh OMDB data
/// with the preserved Personal rating and either prints a dry-run diff or
/// writes the movie plus a `ratings_history` snapshot.
async fn refresh_movie(
    db: &mut Connection,
    cfg: &RefreshConfig<'_>,
    imdb_id: &str,
    data: &str,
) -> RefreshOutcome {
    let doc: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("invalid JSON for {imdb_id}: {e}");
            exit(1);
        }
    };
    let Some(rating) = personal_rating(&doc) else {
        println!(
            "SKIP {}: no Personal rating",
            doc_str(&doc, "title", imdb_id)
        );
        return RefreshOutcome::Skipped;
    };

    let omdb: Value = match async {
        cfg.client
            .get(cfg.omdb_url)
            .query(&[("apikey", cfg.api_key), ("i", imdb_id)])
            .send()
            .await?
            .json()
            .await
    }
    .await
    {
        Ok(v) => v,
        Err(e) => {
            // without_url(): strip the apikey-bearing request URL before
            // this hits stdout/journalctl.
            eprintln!("OMDB request failed for {imdb_id}: {}", e.without_url());
            exit(1);
        }
    };

    if omdb.get("Response").and_then(Value::as_str) != Some("True") {
        let error = omdb.get("Error").and_then(Value::as_str).unwrap_or("");
        if error == "Daily request limit reached!" {
            return RefreshOutcome::DailyLimitReached;
        }
        println!("SKIP {} [{}]: {}", doc_str(&doc, "title", "?"), imdb_id, error);
        // An OMDB call was still made for this movie — sleep the same as
        // every other post-request path below, or a run full of
        // not-found/renamed titles hammers OMDB with no throttling at all.
        tokio::time::sleep(Duration::from_secs_f64(cfg.sleep_secs)).await;
        return RefreshOutcome::Skipped;
    }

    let now = utcnow();
    let omdb_obj = omdb.as_object().expect("OMDB response is a JSON object");
    let new_doc = rebuild_doc(omdb_obj, &rating, &now);
    let new_ratings = new_doc
        .get("ratings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    // A Response=True doc without Title would persist with a NULL generated
    // title column, permanently breaking ?title=&year= lookups — skip
    // instead of writing it.
    let Some(title) = new_doc.get("title").and_then(Value::as_str) else {
        println!("SKIP {imdb_id}: OMDB response missing Title — not persisted");
        tokio::time::sleep(Duration::from_secs_f64(cfg.sleep_secs)).await;
        return RefreshOutcome::Skipped;
    };
    let year = new_doc.get("year").and_then(Value::as_str).unwrap_or("?");

    if cfg.dry_run {
        println!("DRY  {title} ({year}): {}", dry_run_diff(&doc, &new_ratings));
        tokio::time::sleep(Duration::from_secs_f64(cfg.sleep_secs)).await;
        return RefreshOutcome::Refreshed;
    }

    let new_data = serde_json::to_string(&new_doc).expect("doc serializes");
    let result = (|| -> rusqlite::Result<()> {
        // Immediate for the same reason as http::upsert_movie: write
        // transactions take the write lock up front, where busy_timeout
        // applies, instead of risking a stale-snapshot upgrade failure
        // against the concurrently-serving API process.
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE movies SET data = ? WHERE imdb_id = ?",
            params![new_data, imdb_id],
        )?;
        snapshot_ratings(&tx, imdb_id, title, &new_ratings, &now)?;
        tx.commit()
    })();
    if let Err(e) = result {
        eprintln!("write failed for {imdb_id}: {e}");
        exit(1);
    }
    println!("OK   {title} ({year})");
    tokio::time::sleep(Duration::from_secs_f64(cfg.sleep_secs)).await;
    RefreshOutcome::Refreshed
}

pub(crate) async fn refresh(db_path: String, limit: Option<usize>, sleep_secs: f64, dry_run: bool) {
    let api_key = require_omdb_key();
    let omdb_url = env::var("OMDB_URL").unwrap_or_else(|_| DEFAULT_OMDB_URL.to_string());
    let mut db = open_db(&db_path);
    let rows = load_movies(&db, limit);
    let total = rows.len();
    let client = build_omdb_client();
    let cfg = RefreshConfig {
        client: &client,
        omdb_url: &omdb_url,
        api_key: &api_key,
        sleep_secs,
        dry_run,
    };

    let mut refreshed = 0usize;
    let mut skipped = 0usize;
    for (imdb_id, data) in &rows {
        match refresh_movie(&mut db, &cfg, imdb_id, data).await {
            RefreshOutcome::Refreshed => refreshed += 1,
            RefreshOutcome::Skipped => skipped += 1,
            RefreshOutcome::DailyLimitReached => {
                println!(
                    "OMDB daily limit hit after {refreshed} refreshes. Re-run tomorrow — progress is saved."
                );
                break;
            }
        }
    }

    println!(
        "\nDone: {refreshed} refreshed, {skipped} skipped, {} remaining.",
        total - refreshed - skipped
    );
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
    fn rebuild_doc_snake_cases_keys_folds_ratings_and_appends_personal() {
        let omdb = json!({
            "Title": "The Matrix",
            "Year": "1999",
            "imdbID": "tt0133093",
            "BoxOffice": "$172,076,928",
            "Ratings": [{"Source": "Internet Movie Database", "Value": "8.7/10"}],
            "Response": "True",
        });
        let out = rebuild_doc(omdb.as_object().unwrap(), &json!("9/10"), "2026-01-01T00:00:00+00:00");

        assert_eq!(out.get("title"), Some(&json!("The Matrix")));
        assert_eq!(out.get("year"), Some(&json!("1999")));
        // Multi-word / acronym OMDB keys land as snake_case.
        assert_eq!(out.get("imdb_id"), Some(&json!("tt0133093")));
        assert_eq!(out.get("box_office"), Some(&json!("$172,076,928")));
        // "Response" is dropped entirely, not just renamed.
        assert!(!out.contains_key("response"));
        assert!(!out.contains_key("Response"));
        assert_eq!(
            out.get("_refreshed"),
            Some(&json!("2026-01-01T00:00:00+00:00"))
        );
        let ratings = out.get("ratings").unwrap().as_array().unwrap();
        assert_eq!(
            *ratings,
            vec![
                json!({"source": "Internet Movie Database", "value": "8.7/10"}),
                json!({"source": "Personal", "value": "9/10"}),
            ]
        );
    }

    #[test]
    fn rebuild_doc_adds_ratings_key_when_omdb_omitted_it() {
        let omdb = json!({"Title": "No Ratings Field"});
        let out = rebuild_doc(omdb.as_object().unwrap(), &json!("5/10"), "now");
        assert_eq!(
            out.get("ratings").unwrap().as_array().unwrap(),
            &vec![json!({"source": "Personal", "value": "5/10"})]
        );
    }

    #[test]
    fn rebuild_doc_preserves_non_string_personal_value() {
        // A manually-edited DB row could have a non-string Personal value;
        // rebuild_doc must round-trip it as-is, not stringify it.
        let omdb = json!({"Title": "X"});
        let out = rebuild_doc(omdb.as_object().unwrap(), &json!(9), "now");
        let ratings = out.get("ratings").unwrap().as_array().unwrap();
        assert_eq!(ratings[0]["value"], json!(9));
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
}
