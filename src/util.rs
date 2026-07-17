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
/// place that decides the stored/served key spelling; the migration script
/// (`scripts/migrate_snake_case.py`) must agree with it.
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
/// Shared by the two places that fold an OMDB `Ratings` array into a stored
/// doc (`http::normalize_omdb_movie`, `refresh::rebuild_doc`).
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

/// Constant-time byte comparison: if lengths differ fail, else XOR-fold.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
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
        use serde_json::json;
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
    fn ct_eq_matches_only_identical_bytes() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secre1"));
        assert!(!ct_eq(b"secret", b"secrets")); // different length
        assert!(!ct_eq(b"secrets", b"secret")); // different length, swapped
    }
}
