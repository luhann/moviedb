//! Small helpers and defaults shared by more than one of the other modules
//! (`http.rs`'s server and `refresh.rs`'s refresh job both need these).

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value, json};

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
/// every key snake_cased (Map preserves insertion order via the
/// `preserve_order` feature, so this keeps OMDB's original field order),
/// `Ratings` folded into `ratings` with each entry's keys snake_cased and
/// the Personal entry appended, and the now-redundant `response` field
/// dropped. The single source of truth for the doc shape — both the server
/// (`http::add_movie`) and the refresh job (`refresh::refresh_movie`) go
/// through here, so the two can't drift on what a stored movie looks like.
pub(crate) fn normalize_omdb(omdb: Map<String, Value>, personal: Value) -> Map<String, Value> {
    let mut out = Map::new();
    for (key, value) in omdb {
        if key == "Ratings" {
            let mut ratings = match value {
                Value::Array(a) => snake_case_entry_keys(a),
                _ => Vec::new(),
            };
            ratings.push(json!({ "source": "Personal", "value": personal }));
            out.insert("ratings".to_string(), Value::Array(ratings));
        } else {
            out.insert(snake_case(&key), value);
        }
    }
    if !out.contains_key("ratings") {
        out.insert(
            "ratings".to_string(),
            Value::Array(vec![json!({ "source": "Personal", "value": personal })]),
        );
    }
    // shift_remove, not remove: with preserve_order, plain remove is a
    // swap_remove and would scramble the remaining keys' order.
    out.shift_remove("response");
    out
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

    #[test]
    fn ct_eq_folds_length_mismatch_constant_time() {
        // Shorter-side prefix matches but lengths differ — must still fail,
        // without returning early on the length check alone.
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        assert!(!ct_eq(b"abc", b"abc\0"));
    }
}
