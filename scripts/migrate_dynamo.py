"""Migrate a DynamoDB scan export into the moviedb SQLite database (v2 schema).

Usage:
    aws dynamodb scan --table-name movies --output json > movies.json
    python3 migrate_dynamo.py movies.json /var/lib/moviedb/movies.db

Keys each movie on imdbid and seeds ratings_history with one snapshot per
movie, timestamped at migration time. Caveat for later analysis: these
values were actually observed at the original (unknown) pull date, so the
first snapshot per movie is an observation with unknown lag — treat the
migration-time snapshots as "current as of migration at the latest".

No boto3 dependency.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone


def deserialize(av):
    ((t, v),) = av.items()
    if t == "S":
        return v
    if t == "N":
        return int(v) if "." not in v else float(v)
    if t == "BOOL":
        return v
    if t == "NULL":
        return None
    if t == "L":
        return [deserialize(x) for x in v]
    if t == "M":
        return {k: deserialize(x) for k, x in v.items()}
    if t in ("SS", "NS", "BS"):
        return list(v)
    raise ValueError(f"Unhandled DynamoDB type: {t}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS movies (
    imdb_id TEXT PRIMARY KEY,
    data    JSON NOT NULL,
    title   TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
    year    TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
);
CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies (title, year);
CREATE TABLE IF NOT EXISTS ratings_history (
    imdb_id  TEXT NOT NULL,
    title    TEXT NOT NULL,
    observed TEXT NOT NULL,
    source   TEXT NOT NULL,
    value    TEXT NOT NULL,
    PRIMARY KEY (imdb_id, observed, source)
);
"""


def main(export_path: str, db_path: str) -> None:
    with open(export_path) as f:
        export = json.load(f)

    items = [
        {k: deserialize(v) for k, v in item.items()} for item in export["Items"]
    ]

    db = sqlite3.connect(db_path)
    db.executescript(SCHEMA)

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    migrated = skipped = 0
    for item in items:
        imdbid = item.get("imdbid")
        if not imdbid:
            print(f"SKIP {item.get('title', '?')} ({item.get('year', '?')}): "
                  f"no imdbid in item — insert manually if wanted")
            skipped += 1
            continue
        db.execute(
            "INSERT OR REPLACE INTO movies (imdb_id, data) VALUES (?, ?)",
            (imdbid, json.dumps(item)),
        )
        for r in item.get("ratings", []):
            db.execute(
                "INSERT OR IGNORE INTO ratings_history VALUES (?, ?, ?, ?, ?)",
                (imdbid, item.get("title", "?"), now,
                 r.get("Source", "?"), r.get("Value", "?")),
            )
        migrated += 1
    db.commit()

    count = db.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    hist = db.execute("SELECT COUNT(*) FROM ratings_history").fetchone()[0]
    print(f"Export: {len(export['Items'])} items. Migrated: {migrated}, "
          f"skipped: {skipped}. Table rows: {count}. History snapshots: {hist}.")
    if export.get("LastEvaluatedKey"):
        print("WARNING: export has LastEvaluatedKey — scan was paginated and "
              "this file is INCOMPLETE.")
    db.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: migrate_dynamo.py <export.json> <movies.db>")
    main(sys.argv[1], sys.argv[2])
