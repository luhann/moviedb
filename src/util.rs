//! Small helpers and defaults shared by more than one of the other modules
//! (`http.rs`'s server and `refresh.rs`'s refresh job both need these).

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value};

pub(crate) const DEFAULT_OMDB_URL: &str = "https://www.omdbapi.com/";

/// OMDB field name -> this API's snake_case: an underscore lands before an
/// uppercase run's start when preceded by lowercase ("totalSeasons" ->
/// "total_seasons"), and before a run's *last* letter when the run is
/// followed by lowercase ("BoxOffice" -> "box_office") — so acronyms stay
/// single words: "imdbID" -> "imdb_id", "DVD" -> "dvd". This is the only
/// place that decides the stored/served key spelling.
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

/// Snake-cases the keys inside each ratings entry ("Source" -> "source",
/// "Value" -> "value" in practice), leaving non-object entries untouched.
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

/// Normalizes a Response=True OMDB payload into the stored/served shape:
/// every key snake_cased in OMDB's original order (`preserve_order`),
/// `Ratings` folded into `ratings` with the Personal entry appended,
/// `Response` dropped, and `_refreshed` stamped. The single source of truth
/// for the doc shape — `http::add_movie` and the refresh job both build
/// their docs here.
pub(crate) fn normalize_omdb(
    omdb: Map<String, Value>,
    personal: Value,
    refreshed: &str,
) -> Map<String, Value> {
    let mut out = Map::new();
    let mut ratings = Vec::new();
    for (key, value) in omdb {
        match key.as_str() {
            "Response" => {}
            "Ratings" => {
                if let Value::Array(entries) = value {
                    ratings = snake_case_entry_keys(entries);
                }
                // Reserves OMDB's position for the key; filled in below.
                out.insert("ratings".to_string(), Value::Null);
            }
            _ => {
                out.insert(snake_case(&key), value);
            }
        }
    }
    let mut personal_entry = Map::new();
    personal_entry.insert("source".to_string(), Value::from("Personal"));
    personal_entry.insert("value".to_string(), personal);
    ratings.push(Value::Object(personal_entry));
    out.insert("ratings".to_string(), Value::Array(ratings));
    out.insert("_refreshed".to_string(), Value::from(refreshed));
    out
}

/// How a Response=False OMDB payload's `Error` string should be handled.
pub(crate) enum OmdbError {
    /// The key's daily quota is spent. OMDB has been seen sending both
    /// spellings.
    DailyLimit,
    /// OMDB answered, and has no such title (`t=`) or id (`i=`).
    NotFound,
    /// Anything else, including `Invalid API key!` — nothing a retry of the
    /// same request will fix.
    Other,
}

pub(crate) fn classify_omdb_error(error: &str) -> OmdbError {
    match error {
        "Daily request limit reached!" | "Request limit reached!" => OmdbError::DailyLimit,
        "Movie not found!" | "Incorrect IMDb ID." | "Error getting data." => OmdbError::NotFound,
        _ => OmdbError::Other,
    }
}

/// An environment variable, with set-but-empty treated as unset: `DB_PATH=`
/// would otherwise hand SQLite an empty path, which it opens as a private
/// temporary database — a different one per pooled connection.
pub(crate) fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// UTC timestamp at millisecond precision with a "+00:00" (not "Z") suffix,
/// e.g. "2026-07-17T12:34:56.789+00:00". `ratings_history` ordering is
/// lexical on this string, so the format has to stay byte-identical (fixed
/// width, fixed offset) run to run. Millis, not secs: `observed` is part of
/// the `ratings_history` primary key, and at second precision two POSTs of
/// the same movie within one second collide — `INSERT OR IGNORE` then
/// silently drops the newer snapshot.
pub(crate) fn utcnow() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, false)
}

/// Constant-time byte comparison: XOR-folds all bytes up to the shorter
/// length, then folds the length mismatch into the same diff so the
/// comparison time depends only on the shorter input — not on whether the
/// lengths match (which an early return would leak).
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
        // ratings_history ordering is lexical on this string — the format
        // must stay "+00:00" (never "Z") and fixed-width millisecond
        // precision, or drift silently breaks ordering.
        let now = utcnow();
        assert!(now.ends_with("+00:00"), "got: {now}");
        assert!(!now.ends_with('Z'), "got: {now}");
        // "2026-07-17T12:34:56.789+00:00" — 29 bytes, '.' at index 19.
        assert_eq!(now.len(), 29, "got: {now}");
        assert_eq!(now.as_bytes()[19], b'.', "got: {now}");
    }

    #[test]
    fn snake_case_handles_words_acronyms_and_mixed() {
        // The full multi-word OMDB key set, plus the acronym shapes that
        // break naive camel->snake splitting.
        assert_eq!(snake_case("Title"), "title");
        assert_eq!(snake_case("imdbID"), "imdb_id");
        assert_eq!(snake_case("imdbRating"), "imdb_rating");
        assert_eq!(snake_case("imdbVotes"), "imdb_votes");
        assert_eq!(snake_case("BoxOffice"), "box_office");
        assert_eq!(snake_case("totalSeasons"), "total_seasons");
        assert_eq!(snake_case("DVD"), "dvd");
        assert_eq!(snake_case("Metascore"), "metascore");
        // Already-snake input is a fixed point (idempotent on re-normalize).
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
    fn normalize_omdb_snake_cases_keys_folds_ratings_and_stamps() {
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
        let out = normalize_omdb(omdb, json!("9/10"), "2026-01-01T00:00:00.000+00:00");

        assert_eq!(
            out.keys().collect::<Vec<_>>(),
            [
                "title",
                "year",
                "ratings",
                "imdb_id",
                "box_office",
                "_refreshed"
            ]
        );
        assert_eq!(
            out["ratings"],
            json!([
                {"source": "Internet Movie Database", "value": "8.7/10"},
                {"source": "Personal", "value": "9/10"},
            ])
        );
        assert_eq!(out["_refreshed"], json!("2026-01-01T00:00:00.000+00:00"));
    }

    #[test]
    fn normalize_omdb_adds_ratings_when_omdb_omitted_it() {
        let Value::Object(omdb) = json!({"Title": "No Ratings Field"}) else {
            unreachable!()
        };
        let out = normalize_omdb(omdb, json!("5/10"), "now");
        assert_eq!(
            out["ratings"],
            json!([{"source": "Personal", "value": "5/10"}])
        );
    }

    #[test]
    fn normalize_omdb_preserves_non_string_personal_value() {
        // A hand-edited row can hold a non-string Personal value; it must
        // round-trip as-is, not be stringified.
        let Value::Object(omdb) = json!({"Title": "X"}) else {
            unreachable!()
        };
        let out = normalize_omdb(omdb, json!(9), "now");
        assert_eq!(out["ratings"][0]["value"], json!(9));
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
        // Shorter-side prefix matches but lengths differ — must still fail,
        // without returning early on the length check alone.
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        assert!(!ct_eq(b"abc", b"abc\0"));
    }
}
