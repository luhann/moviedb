//! SQLite schema, connection setup and query helpers. The server and the
//! refresh job run as separate processes against the same database file, and
//! both use these.

use std::error::Error;
use std::time::Duration;

use rusqlite::{Connection, params};
use serde_json::{Map, Value};

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

/// Opens the database, creating it and its schema if needed, and migrating
/// one created by 2.x.
pub(crate) fn open(path: &str) -> Result<Connection, Box<dyn Error>> {
    let mut db = Connection::open(path)?;
    // Set WAL first, because what synchronous=NORMAL does depends on the
    // journal mode.
    db.pragma_update(None, "journal_mode", "WAL")?;
    set_connection_pragmas(&db)?;
    init_schema(&mut db)?;
    Ok(db)
}

fn init_schema(db: &mut Connection) -> Result<(), Box<dyn Error>> {
    if has_column(db, "movies", "data")? && !has_column(db, "movies", "personal")? {
        migrate_from_2x(db)?;
    }
    db.execute_batch(TABLES)?;
    db.execute_batch(INDEXES)?;
    db.execute_batch(VIEWS)?;
    Ok(())
}

/// 2.x kept your rating as a "Personal" entry in OMDB's ratings array and the
/// refresh time as `_refreshed` inside the doc, and repeated the title in
/// every history row. This moves the rating and the time into their own
/// columns and drops the title, in one transaction. If any rating isn't a
/// whole number from 0 to 100, nothing changes and the error lists them.
fn migrate_from_2x(db: &mut Connection) -> Result<(), Box<dyn Error>> {
    let tx = db.transaction()?;
    tx.execute_batch(
        "ALTER TABLE movies RENAME TO movies_2x;
         ALTER TABLE ratings_history DROP COLUMN title;",
    )?;
    tx.execute_batch(TABLES)?;

    let mut problems = Vec::new();
    {
        let mut read = tx.prepare("SELECT imdb_id, data FROM movies_2x ORDER BY imdb_id")?;
        let mut last_snapshot =
            tx.prepare("SELECT MAX(observed) FROM ratings_history WHERE imdb_id = ?")?;
        let mut write = tx.prepare(
            "INSERT INTO movies (imdb_id, data, personal, refreshed) VALUES (?, ?, ?, ?)",
        )?;
        let mut rows = read.query([])?;
        while let Some(row) = rows.next()? {
            let imdb_id: String = row.get(0)?;
            let mut doc: Map<String, Value> = serde_json::from_str(row.get_ref(1)?.as_str()?)?;
            let Some(personal) = take_personal(&mut doc) else {
                problems.push(format!(
                    "{imdb_id}: no Personal rating that is a whole number from 0 to 100"
                ));
                continue;
            };
            let refreshed = match doc.remove("_refreshed") {
                Some(Value::String(s)) => Some(s),
                _ => last_snapshot.query_row([&imdb_id], |r| r.get(0))?,
            };
            let Some(refreshed) = refreshed else {
                problems.push(format!(
                    "{imdb_id}: no _refreshed and no history to take it from"
                ));
                continue;
            };
            write.execute(params![
                imdb_id,
                serde_json::to_string(&doc)?,
                personal,
                refreshed
            ])?;
        }
    }
    if !problems.is_empty() {
        // Dropping `tx` rolls everything back.
        return Err(format!(
            "can't migrate the 2.x database, nothing was changed:\n  {}",
            problems.join("\n  ")
        )
        .into());
    }
    tx.execute_batch("DROP TABLE movies_2x")?;
    tx.commit()?;
    Ok(())
}

/// Removes the "Personal" entry from a 2.x doc's ratings and returns its
/// value, if that's a whole number from 0 to 100.
fn take_personal(doc: &mut Map<String, Value>) -> Option<u8> {
    let ratings = doc.get_mut("ratings")?.as_array_mut()?;
    let index = ratings
        .iter()
        .position(|r| r.get("source").and_then(Value::as_str) == Some("Personal"))?;
    let entry = ratings.remove(index);
    let score = match entry.get("value")? {
        Value::String(s) => s.parse::<u8>().ok(),
        Value::Number(n) => n.as_u64().and_then(|n| u8::try_from(n).ok()),
        _ => None,
    };
    score.filter(|s| *s <= 100)
}

fn has_column(db: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_xinfo(?) WHERE name = ?)",
        params![table, column],
        |r| r.get(0),
    )
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
        let mut db = Connection::open_in_memory().unwrap();
        init_schema(&mut db).unwrap();
        db
    }

    fn count(db: &Connection, sql: &str) -> i64 {
        db.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    const SCHEMA_2X: &str = "
        CREATE TABLE movies (
            imdb_id TEXT PRIMARY KEY,
            data    JSON NOT NULL,
            title   TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
            year    TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
        );
        CREATE INDEX idx_movies_title_year ON movies (title, year);
        CREATE TABLE ratings_history (
            imdb_id TEXT NOT NULL, title TEXT NOT NULL, observed TEXT NOT NULL,
            source TEXT NOT NULL, value TEXT NOT NULL,
            PRIMARY KEY (imdb_id, observed, source)
        );
    ";

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
    fn migrates_a_2x_database() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA_2X).unwrap();
        db.execute_batch(
            r#"
            INSERT INTO movies (imdb_id, data) VALUES
                ('tt1', '{"title":"Stamped","ratings":[{"source":"IMDb","value":"8/10"},{"source":"Personal","value":"90"}],"_refreshed":"2026-03-01T00:00:00.000+00:00"}'),
                ('tt2', '{"title":"Unstamped","ratings":[{"source":"Personal","value":"0"}]}');
            INSERT INTO ratings_history VALUES
                ('tt1', 'Stamped',   '2026-03-01T00:00:00.000+00:00', 'IMDb', '8/10'),
                ('tt2', 'Unstamped', '2026-01-01T00:00:00.000+00:00', 'Personal', '0'),
                ('tt2', 'Unstamped', '2026-02-01T00:00:00.000+00:00', 'Personal', '0');
            "#,
        )
        .unwrap();

        init_schema(&mut db).unwrap();
        // A second start changes nothing.
        init_schema(&mut db).unwrap();

        assert!(!has_column(&db, "ratings_history", "title").unwrap());
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ratings_history"), 3);
        let doc = |id: &str| -> Value {
            let doc: String = db
                .query_row("SELECT doc FROM movie_docs WHERE imdb_id = ?", [id], |r| {
                    r.get(0)
                })
                .unwrap();
            serde_json::from_str(&doc).unwrap()
        };
        assert_eq!(
            doc("tt1"),
            json!({
                "title": "Stamped",
                "ratings": [{"source": "IMDb", "value": "8/10"}],
                "personal": 90,
                "refreshed": "2026-03-01T00:00:00.000+00:00",
            })
        );
        // No _refreshed, so it comes from the newest snapshot.
        assert_eq!(
            doc("tt2"),
            json!({
                "title": "Unstamped",
                "ratings": [],
                "personal": 0,
                "refreshed": "2026-02-01T00:00:00.000+00:00",
            })
        );
    }

    #[test]
    fn migration_changes_nothing_if_a_rating_is_invalid() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA_2X).unwrap();
        db.execute_batch(
            r#"
            INSERT INTO movies (imdb_id, data) VALUES
                ('tt1', '{"title":"Fine","ratings":[{"source":"Personal","value":"80"}],"_refreshed":"2026-01-01T00:00:00.000+00:00"}'),
                ('tt2', '{"title":"Out of ten","ratings":[{"source":"Personal","value":"9/10"}],"_refreshed":"2026-01-01T00:00:00.000+00:00"}'),
                ('tt3', '{"title":"Too high","ratings":[{"source":"Personal","value":"101"}],"_refreshed":"2026-01-01T00:00:00.000+00:00"}'),
                ('tt4', '{"title":"Unrated","ratings":[{"source":"Personal","value":null}],"_refreshed":"2026-01-01T00:00:00.000+00:00"}');
            INSERT INTO ratings_history VALUES ('tt1', 'Fine', '2026-01-01T00:00:00.000+00:00', 'Personal', '80');
            "#,
        )
        .unwrap();

        let err = init_schema(&mut db).unwrap_err().to_string();
        for id in ["tt2", "tt3", "tt4"] {
            assert!(err.contains(id), "{err}");
        }
        assert!(!err.contains("tt1"), "{err}");
        // Still the 2.x layout.
        assert!(!has_column(&db, "movies", "personal").unwrap());
        assert!(has_column(&db, "ratings_history", "title").unwrap());
        assert_eq!(count(&db, "SELECT COUNT(*) FROM movies"), 4);
    }

    #[test]
    fn take_personal_accepts_whole_numbers_from_0_to_100_only() {
        let score = |value: Value| {
            let mut doc = json!({"ratings": [{"source": "Personal", "value": value}]});
            take_personal(doc.as_object_mut().unwrap())
        };
        assert_eq!(score(json!("0")), Some(0));
        assert_eq!(score(json!("100")), Some(100));
        assert_eq!(score(json!(77)), Some(77));
        for bad in [
            json!("101"),
            json!("9/10"),
            json!("7.5"),
            json!(""),
            json!(-1),
            json!(null),
        ] {
            assert_eq!(score(bad.clone()), None, "{bad}");
        }
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
