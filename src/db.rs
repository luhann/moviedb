//! SQLite schema, connection setup, and small query helpers shared by the
//! server (`http.rs`) and the refresh job (`refresh.rs`) — they run as
//! separate processes/connections against the same database file.

use std::time::Duration;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, params};
use serde_json::Value;

pub(crate) const DB_POOL_SIZE: u32 = 8;

pub(crate) fn set_connection_pragmas(db: &mut Connection) -> rusqlite::Result<()> {
    db.busy_timeout(Duration::from_secs(5))?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    db.pragma_update(None, "mmap_size", 64 * 1024 * 1024)?;
    db.pragma_update(None, "cache_size", -8 * 1024)?;
    db.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

fn init_db(path: &str) -> rusqlite::Result<Connection> {
    let mut db = Connection::open(path)?;
    // WAL first: synchronous=NORMAL's meaning depends on journal_mode.
    db.pragma_update(None, "journal_mode", "WAL")?;
    set_connection_pragmas(&mut db)?;
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
    // `year` is the *second* column of the composite index above, so a
    // year-only filter (GET /movies?year=) can't use it and full-scans,
    // recomputing json_extract for the VIRTUAL column on every row.
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year)",
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
    Ok(db)
}

pub(crate) fn build_pool(
    path: &str,
) -> Result<Pool<SqliteConnectionManager>, Box<dyn std::error::Error>> {
    init_db(path)?;
    let manager = SqliteConnectionManager::file(path).with_init(set_connection_pragmas);
    Ok(Pool::builder()
        .max_size(DB_POOL_SIZE)
        .connection_timeout(Duration::from_secs(5))
        .build(manager)?)
}

pub(crate) fn ratings_entry_field<'a>(entry: &'a Value, key: &str) -> &'a str {
    // Stored ratings entries hold snake_case keys ("source"/"value" — OMDB's
    // "Source"/"Value" are renamed at ingest, see util::snake_case_entry_keys)
    // and OMDB always sends the values as strings; default to "?" if an entry
    // is missing the field entirely or holds something else (e.g. a
    // hand-edited DB row).
    entry.get(key).and_then(Value::as_str).unwrap_or("?")
}

/// Insert one history row per ratings entry. Python: `snapshot_ratings()`.
pub(crate) fn snapshot_ratings(
    db: &Connection,
    imdbid: &str,
    title: &str,
    ratings: &[Value],
    observed: &str,
) -> rusqlite::Result<()> {
    let mut stmt =
        db.prepare_cached("INSERT OR IGNORE INTO ratings_history VALUES (?, ?, ?, ?, ?)")?;
    for r in ratings {
        stmt.execute(params![
            imdbid,
            title,
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

    #[test]
    fn ratings_entry_field_present_missing_and_non_string() {
        let entry = json!({ "source": "IMDb", "value": "8.7/10" });
        assert_eq!(ratings_entry_field(&entry, "source"), "IMDb");
        assert_eq!(ratings_entry_field(&entry, "value"), "8.7/10");
        // Key absent entirely.
        assert_eq!(ratings_entry_field(&entry, "nope"), "?");
        // Present but not a JSON string (e.g. a manually-edited value).
        let numeric = json!({ "source": "IMDb", "value": 87 });
        assert_eq!(ratings_entry_field(&numeric, "value"), "?");
    }

    fn memory_db_with_history_table() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute(
            "CREATE TABLE ratings_history (
                imdb_id  TEXT NOT NULL,
                title    TEXT NOT NULL,
                observed TEXT NOT NULL,
                source   TEXT NOT NULL,
                value    TEXT NOT NULL,
                PRIMARY KEY (imdb_id, observed, source)
            )",
            [],
        )
        .unwrap();
        db
    }

    #[test]
    fn snapshot_ratings_inserts_one_row_per_entry() {
        let db = memory_db_with_history_table();
        let ratings = vec![
            json!({ "source": "IMDb", "value": "8.7/10" }),
            json!({ "source": "Personal", "value": "9/10" }),
        ];
        snapshot_ratings(
            &db,
            "tt0133093",
            "The Matrix",
            &ratings,
            "2026-01-01T00:00:00+00:00",
        )
        .unwrap();

        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM ratings_history", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn snapshot_ratings_ignores_exact_duplicate_snapshot() {
        // Same (imdb_id, observed, source) — as happens if refresh runs
        // twice with an unchanged rating at the same timestamp — must not
        // duplicate the history row.
        let db = memory_db_with_history_table();
        let ratings = vec![json!({ "source": "IMDb", "value": "8.7/10" })];
        let observed = "2026-01-01T00:00:00+00:00";
        snapshot_ratings(&db, "tt0133093", "The Matrix", &ratings, observed).unwrap();
        snapshot_ratings(&db, "tt0133093", "The Matrix", &ratings, observed).unwrap();

        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM ratings_history", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn set_connection_pragmas_applies_normal_synchronous() {
        let mut db = Connection::open_in_memory().unwrap();
        set_connection_pragmas(&mut db).unwrap();
        // NORMAL == 1 (see https://www.sqlite.org/pragma.html#pragma_synchronous)
        let synchronous: i64 = db
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 1);
        // MEMORY == 2 (https://www.sqlite.org/pragma.html#pragma_temp_store)
        let temp_store: i64 = db.query_row("PRAGMA temp_store", [], |r| r.get(0)).unwrap();
        assert_eq!(temp_store, 2);
        // Negative values are a KiB budget, echoed back as set.
        let cache_size: i64 = db.query_row("PRAGMA cache_size", [], |r| r.get(0)).unwrap();
        assert_eq!(cache_size, -8 * 1024);
    }
}
