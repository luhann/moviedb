//! SQLite schema, connection setup and query helpers. The server and the
//! refresh job run as separate processes against the same database file, and
//! both use these.

use std::time::Duration;

use rusqlite::{Connection, params};
use serde_json::Value;

/// `data` is OMDB's response, normalized (see `util::normalize_omdb`).
/// `personal` and `refreshed` are ours, kept apart so a refresh can replace
/// `data` without touching your rating.
const TABLES: &str = "
    CREATE TABLE IF NOT EXISTS movies (
        imdb_id   TEXT PRIMARY KEY,
        data      JSON NOT NULL,
        personal  INTEGER NOT NULL CHECK (personal BETWEEN 0 AND 100),
        refreshed TEXT NOT NULL,   -- when `data` was last fetched from OMDB
        title     TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
        year      TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
    );
    CREATE TABLE IF NOT EXISTS ratings_history (
        imdb_id  TEXT NOT NULL,
        observed TEXT NOT NULL,   -- ISO-8601 UTC timestamp of the snapshot
        source   TEXT NOT NULL,
        value    TEXT NOT NULL,
        PRIMARY KEY (imdb_id, observed, source)
    );
";

const INDEXES: &str = "
    CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies (title, year);
    -- year is second in the index above, so a year-only filter needs its own.
    CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);
    -- Serves both /movies/recent (newest first) and refresh (oldest first).
    CREATE INDEX IF NOT EXISTS idx_movies_refreshed ON movies (refreshed, imdb_id);
";

/// Each movie as the API serves it: OMDB's data with our two fields added.
/// Every endpoint that returns movies selects `doc` from here. Recreated on
/// every start so a change to it always applies.
///
/// The fields are appended as text rather than with json_set, which parses
/// and rewrites every doc on every read: GET /movies measured 7x slower that
/// way. It relies on `data` always being a non-empty JSON object written by
/// serde (so it ends in `}`), and `refreshed` needing no escaping.
const VIEWS: &str = "
    DROP VIEW IF EXISTS movie_docs;
    CREATE VIEW movie_docs AS
    SELECT imdb_id, title, year, refreshed,
           substr(data, 1, length(data) - 1)
               || ',\"personal\":' || personal
               || ',\"refreshed\":\"' || refreshed || '\"}' AS doc
    FROM movies;
";

pub(crate) fn set_connection_pragmas(db: &Connection) -> rusqlite::Result<()> {
    db.busy_timeout(Duration::from_secs(5))?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    db.pragma_update(None, "mmap_size", 64 * 1024 * 1024)?;
    db.pragma_update(None, "cache_size", -8 * 1024)?;
    db.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

/// Opens the database, creating it and its schema if needed.
pub(crate) fn open(path: &str) -> rusqlite::Result<Connection> {
    let db = Connection::open(path)?;
    // Set WAL first, because what synchronous=NORMAL does depends on the
    // journal mode.
    db.pragma_update(None, "journal_mode", "WAL")?;
    set_connection_pragmas(&db)?;
    init_schema(&db)?;
    Ok(db)
}

fn init_schema(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(TABLES)?;
    db.execute_batch(INDEXES)?;
    db.execute_batch(VIEWS)
}

pub(crate) fn ratings_entry_field<'a>(entry: &'a Value, key: &str) -> &'a str {
    // OMDB always sends strings, but a hand-edited row might not.
    entry.get(key).and_then(Value::as_str).unwrap_or("?")
}

/// Insert one history row per ratings entry.
pub(crate) fn snapshot_ratings(
    db: &Connection,
    imdb_id: &str,
    ratings: &[Value],
    observed: &str,
) -> rusqlite::Result<()> {
    let mut stmt = db.prepare_cached(
        "INSERT OR IGNORE INTO ratings_history (imdb_id, observed, source, value)
         VALUES (?, ?, ?, ?)",
    )?;
    for r in ratings {
        stmt.execute(params![
            imdb_id,
            observed,
            ratings_entry_field(r, "source"),
            ratings_entry_field(r, "value"),
        ])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn memory_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        init_schema(&db).unwrap();
        db
    }

    fn count(db: &Connection, sql: &str) -> i64 {
        db.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn ratings_entry_field_present_missing_and_non_string() {
        let entry = json!({ "source": "IMDb", "value": "8.7/10" });
        assert_eq!(ratings_entry_field(&entry, "source"), "IMDb");
        assert_eq!(ratings_entry_field(&entry, "value"), "8.7/10");
        assert_eq!(ratings_entry_field(&entry, "nope"), "?");
        // Present, but not a string.
        let numeric = json!({ "source": "IMDb", "value": 87 });
        assert_eq!(ratings_entry_field(&numeric, "value"), "?");
    }

    #[test]
    fn snapshot_ratings_inserts_one_row_per_entry() {
        let db = memory_db();
        let ratings = vec![
            json!({ "source": "IMDb", "value": "8.7/10" }),
            json!({ "source": "Personal", "value": "90" }),
        ];
        snapshot_ratings(&db, "tt0133093", &ratings, "2026-01-01T00:00:00+00:00").unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ratings_history"), 2);
    }

    #[test]
    fn snapshot_ratings_ignores_exact_duplicate_snapshot() {
        // A second snapshot with the same (imdb_id, observed, source) must
        // not add another row.
        let db = memory_db();
        let ratings = vec![json!({ "source": "IMDb", "value": "8.7/10" })];
        let observed = "2026-01-01T00:00:00+00:00";
        snapshot_ratings(&db, "tt0133093", &ratings, observed).unwrap();
        snapshot_ratings(&db, "tt0133093", &ratings, observed).unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ratings_history"), 1);
    }

    #[test]
    fn movie_docs_appends_personal_and_refreshed_to_the_stored_json() {
        let db = memory_db();
        let data = r#"{"title":"Amélie","plot":"a } in text","ratings":[{"source":"IMDb","value":"8.3/10"}]}"#;
        db.execute(
            "INSERT INTO movies (imdb_id, data, personal, refreshed) VALUES ('tt1', ?, 85, '2026-01-01T00:00:00.000+00:00')",
            [data],
        )
        .unwrap();
        let doc: String = db
            .query_row("SELECT doc FROM movie_docs", [], |r| r.get(0))
            .unwrap();
        // The stored bytes come back untouched, with our fields after them.
        assert_eq!(
            doc,
            r#"{"title":"Amélie","plot":"a } in text","ratings":[{"source":"IMDb","value":"8.3/10"}],"personal":85,"refreshed":"2026-01-01T00:00:00.000+00:00"}"#
        );
        serde_json::from_str::<Value>(&doc).unwrap();
    }

    #[test]
    fn set_connection_pragmas_applies_normal_synchronous() {
        let db = Connection::open_in_memory().unwrap();
        set_connection_pragmas(&db).unwrap();
        // NORMAL is 1 (https://www.sqlite.org/pragma.html#pragma_synchronous)
        assert_eq!(count(&db, "PRAGMA synchronous"), 1);
        // MEMORY is 2 (https://www.sqlite.org/pragma.html#pragma_temp_store)
        assert_eq!(count(&db, "PRAGMA temp_store"), 2);
        // A negative cache_size is in KiB and reads back unchanged.
        assert_eq!(count(&db, "PRAGMA cache_size"), -8 * 1024);
    }
}
