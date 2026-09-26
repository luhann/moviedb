//! Helpers and defaults used by both the server and the refresh job.

use std::process::exit;

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value, json};

const DEFAULT_OMDB_URL: &str = "https://www.omdbapi.com/";
const DEFAULT_DB_PATH: &str = "/var/lib/moviedb/movies.db";

/// Converts an OMDB field name to snake_case. Acronyms stay one word:
/// "totalSeasons" -> "total_seasons", "BoxOffice" -> "box_office",
/// "imdbID" -> "imdb_id", "DVD" -> "dvd". Every stored and served key is
/// spelled by this function.
pub(crate) fn snake_case(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    let mut out = String::with_capacity(key.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev_lower = i > 0 && chars[i - 1].is_lowercase();
            let prev_upper = i > 0 && chars[i - 1].is_uppercase();
            let next_lower = i + 1 < chars.len() && chars[i + 1].is_lowercase();
            if prev_lower || (prev_upper && next_lower) {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Snake-cases the keys of each ratings entry ("Source" -> "source").
/// Entries that aren't objects are left as they are.
pub(crate) fn snake_case_entry_keys(entries: Vec<Value>) -> Vec<Value> {
    entries
        .into_iter()
        .map(|entry| match entry {
            Value::Object(obj) => Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (snake_case(&k), v))
                    .collect::<Map<String, Value>>(),
            ),
            other => other,
        })
        .collect()
}

/// Turns a successful OMDB response into the doc we store. Keys are
/// snake_cased and keep OMDB's order, `Response` is dropped, and `ratings`
/// is always there, even if OMDB left it out. Both POST and the refresh job
/// build their docs here.
pub(crate) fn normalize_omdb(omdb: Map<String, Value>) -> Map<String, Value> {
    let mut out: Map<String, Value> = omdb
        .into_iter()
        .filter(|(key, _)| key != "Response")
        .map(|(key, value)| match (key.as_str(), value) {
            ("Ratings", Value::Array(entries)) => (
                "ratings".to_string(),
                Value::Array(snake_case_entry_keys(entries)),
            ),
            (_, value) => (snake_case(&key), value),
        })
        .collect();
    out.entry("ratings").or_insert_with(|| json!([]));
    out
}

/// What an OMDB error response (Response=False) means for us.
pub(crate) enum OmdbError {
    /// The API key's daily quota is used up. OMDB words this two ways.
    DailyLimit,
    /// OMDB has no movie with that title (`t=`) or ID (`i=`).
    NotFound,
    /// Anything else, including `Invalid API key!`. Retrying won't help.
    Other,
}

pub(crate) fn classify_omdb_error(error: &str) -> OmdbError {
    match error {
        "Daily request limit reached!" | "Request limit reached!" => OmdbError::DailyLimit,
        "Movie not found!" | "Incorrect IMDb ID." | "Error getting data." => OmdbError::NotFound,
        _ => OmdbError::Other,
    }
}

/// Reads an environment variable, treating an empty value as unset. An empty
/// `DB_PATH` would otherwise give SQLite an empty path, which it opens as a
/// temporary database that is thrown away on exit.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Reads an environment variable that must be set, or exits. An empty value
/// counts as missing: with `API_KEY=`, a blank x-api-key header would pass
/// the key check and auth would be off.
pub(crate) fn require_env(name: &str) -> String {
    env_nonempty(name).unwrap_or_else(|| {
        eprintln!("{name} not set (or empty)");
        exit(1);
    })
}

pub(crate) fn db_path() -> String {
    env_nonempty("DB_PATH").unwrap_or_else(|| DEFAULT_DB_PATH.to_string())
}

pub(crate) fn omdb_url() -> String {
    env_nonempty("OMDB_URL").unwrap_or_else(|| DEFAULT_OMDB_URL.to_string())
}

/// The current UTC time, e.g. "2026-07-17T12:34:56.789+00:00".
///
/// `ratings_history` is sorted by this string, so the format must never
/// change: always the same width, always "+00:00" rather than "Z". It uses
/// milliseconds because the timestamp is part of the history table's primary
/// key. With whole seconds, two POSTs of the same movie in one second would
/// clash and the second snapshot would be dropped.
pub(crate) fn utcnow() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, false)
}

/// Compares two byte strings in constant time, so the time taken doesn't
/// reveal how much of the API key was right. A length mismatch is folded
/// into the result instead of returning early.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    if a.len() != b.len() {
        diff |= 0xff;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn utcnow_is_fixed_width_millis_with_utc_offset_suffix() {
        // History is sorted by this string, so the format must not drift.
        let now = utcnow();
        assert!(now.ends_with("+00:00"), "got: {now}");
        assert!(!now.ends_with('Z'), "got: {now}");
        // "2026-07-17T12:34:56.789+00:00" is 29 bytes with '.' at index 19.
        assert_eq!(now.len(), 29, "got: {now}");
        assert_eq!(now.as_bytes()[19], b'.', "got: {now}");
    }

    #[test]
    fn snake_case_handles_words_acronyms_and_mixed() {
        // Every multi-word OMDB key, plus the acronyms that trip up a naive
        // conversion.
        assert_eq!(snake_case("Title"), "title");
        assert_eq!(snake_case("imdbID"), "imdb_id");
        assert_eq!(snake_case("imdbRating"), "imdb_rating");
        assert_eq!(snake_case("imdbVotes"), "imdb_votes");
        assert_eq!(snake_case("BoxOffice"), "box_office");
        assert_eq!(snake_case("totalSeasons"), "total_seasons");
        assert_eq!(snake_case("DVD"), "dvd");
        assert_eq!(snake_case("Metascore"), "metascore");
        // Keys that are already snake_case come back unchanged.
        assert_eq!(snake_case("imdb_id"), "imdb_id");
        assert_eq!(snake_case("_refreshed"), "_refreshed");
    }

    #[test]
    fn snake_case_entry_keys_maps_object_keys_only() {
        let entries = vec![
            json!({ "Source": "IMDb", "Value": "8.7/10" }),
            json!("not an object"),
        ];
        assert_eq!(
            snake_case_entry_keys(entries),
            vec![
                json!({ "source": "IMDb", "value": "8.7/10" }),
                json!("not an object"),
            ]
        );
    }

    #[test]
    fn normalize_omdb_snake_cases_keys_and_drops_response() {
        let omdb = json!({
            "Title": "The Matrix",
            "Year": "1999",
            "Ratings": [{"Source": "Internet Movie Database", "Value": "8.7/10"}],
            "imdbID": "tt0133093",
            "BoxOffice": "$172,076,928",
            "Response": "True",
        });
        let Value::Object(omdb) = omdb else {
            unreachable!()
        };
        let out = normalize_omdb(omdb);
        assert_eq!(
            Value::Object(out),
            json!({
                "title": "The Matrix",
                "year": "1999",
                "ratings": [{"source": "Internet Movie Database", "value": "8.7/10"}],
                "imdb_id": "tt0133093",
                "box_office": "$172,076,928",
            })
        );
    }

    #[test]
    fn normalize_omdb_keeps_omdb_key_order() {
        let Value::Object(omdb) = json!({"Year": "1999", "Title": "X", "Ratings": []}) else {
            unreachable!()
        };
        let out = normalize_omdb(omdb);
        assert_eq!(out.keys().collect::<Vec<_>>(), ["year", "title", "ratings"]);
    }

    #[test]
    fn normalize_omdb_adds_ratings_when_omdb_omitted_it() {
        let Value::Object(omdb) = json!({"Title": "No Ratings Field"}) else {
            unreachable!()
        };
        assert_eq!(normalize_omdb(omdb)["ratings"], json!([]));
    }

    #[test]
    fn classify_omdb_error_recognises_both_daily_limit_spellings() {
        for e in ["Daily request limit reached!", "Request limit reached!"] {
            assert!(
                matches!(classify_omdb_error(e), OmdbError::DailyLimit),
                "{e}"
            );
        }
        for e in [
            "Movie not found!",
            "Incorrect IMDb ID.",
            "Error getting data.",
        ] {
            assert!(matches!(classify_omdb_error(e), OmdbError::NotFound), "{e}");
        }
        for e in ["Invalid API key!", "", "Something new"] {
            assert!(matches!(classify_omdb_error(e), OmdbError::Other), "{e}");
        }
    }

    #[test]
    fn ct_eq_matches_only_identical_bytes() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secre1"));
        assert!(!ct_eq(b"secret", b"secrets")); // different length
        assert!(!ct_eq(b"secrets", b"secret")); // different length, swapped
    }

    #[test]
    fn ct_eq_folds_length_mismatch_constant_time() {
        // One input is a prefix of the other. Still not equal.
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        assert!(!ct_eq(b"abc", b"abc\0"));
    }
}
