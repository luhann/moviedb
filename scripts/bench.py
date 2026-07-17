#!/usr/bin/env python3
"""Throughput benchmark for moviedb's GET endpoints. Stdlib only.

Deliberately excludes POST /movies — that path is bound by OMDB's own rate limit,
not by anything moviedb's server does, so hammering it measures OMDB, not
this codebase.

There are two independent things worth measuring instead (see src/main.rs):
  - GETs run against an r2d2 pool of 8 WAL-mode SQLite connections
    (DB_POOL_SIZE), each query on a spawn_blocking thread — so req/s should
    scale with concurrency up to ~8 in-flight queries, then plateau on pool
    checkout contention.
  - GET /movies collects the whole table into memory per in-flight request
    (list_movies: `rows.collect()`) before streaming it out — so N
    concurrent list requests hold N full copies of the table in memory at
    once. Against systemd's MemoryMax=256M, this is the one likely to
    actually OOM-kill the service, independent of pool contention.
    GET /movies/{imdb_id} does a point lookup and never pays this cost, so
    comparing the two endpoints tells you which bottleneck you hit.

Two subcommands:
  seed  write N synthetic rows directly into a SQLite file, bypassing OMDB
        entirely, so table size is controllable and repeatable.
  run   hammer a running instance at increasing concurrency levels and
        report req/s + latency percentiles.

Usage:
    python3 scripts/bench.py seed --db /tmp/bench.db --rows 5000

    DB_PATH=/tmp/bench.db API_KEY=bench OMDB_KEY=unused \\
        target/x86_64-unknown-linux-musl/release/moviedb serve --port 8123 &

    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies --concurrency 1,2,4,8,16,32,64 --duration 5

    # then compare against the point-lookup path to isolate pool
    # contention from the whole-table memory cost:
    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies/tt0000000 --concurrency 1,2,4,8,16,32,64 --duration 5

Watch memory on the box being tested while this runs — this script doesn't
sample it remotely:
    systemctl show moviedb -p MemoryCurrent
    journalctl -u moviedb -f   # look for an OOM kill under high --rows + concurrency

Run against a scratch DB/instance first, not the live single-user service:
high concurrency at a large --rows is meant to find the OOM point, and the
live service has no headroom to spare (MemoryMax=256M).
"""
import argparse
import http.client
import json
import random
import sqlite3
import string
import sys
import threading
import time
from urllib.parse import urlsplit


def seed(args):
    conn = sqlite3.connect(args.db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            imdb_id TEXT PRIMARY KEY,
            data    JSON NOT NULL,
            title   TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
            year    TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
        )
    """)  # must match init_db in src/main.rs
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies (title, year)")

    rng = random.Random(args.seed)
    rows = []
    for i in range(args.rows):
        imdb_id = f"tt{i:07d}"
        # ~1.2KB doc, roughly matching a real OMDB payload, so total table
        # size scales predictably with --rows.
        doc = {
            "title": f"Synthetic Movie {i}", "year": str(1950 + i % 75),
            "rated": "PG-13", "released": "01 Jan 2000", "runtime": "120 min",
            "genre": "Drama, Action", "director": "Someone",
            "writer": "Someone Else", "actors": "A, B, C",
            "plot": "".join(rng.choices(string.ascii_lowercase + " ", k=800)),
            "language": "English", "country": "USA", "awards": "N/A",
            "poster": "https://example.com/poster.jpg", "metascore": "70",
            "imdb_rating": "7.1", "imdb_votes": "10,000", "imdb_id": imdb_id,
            "type": "movie", "dvd": "N/A", "box_office": "$1,000,000",
            "production": "N/A", "website": "N/A",
            "ratings": [
                {"source": "Internet Movie Database", "value": "7.1/10"},
                {"source": "Rotten Tomatoes", "value": "70%"},
                {"source": "Personal", "value": "8/10"},
            ],
        }
        rows.append((imdb_id, json.dumps(doc)))
    conn.executemany(
        "INSERT OR REPLACE INTO movies (imdb_id, data) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    print(f"seeded {args.rows} rows into {args.db}")


class Worker(threading.Thread):
    """One persistent HTTP/1.1 connection per thread, reused across requests
    — avoids TCP/TLS handshake cost confounding the throughput number."""

    def __init__(self, host, port, use_ssl, path, key, stop_at, latencies_ms):
        super().__init__()
        self.host, self.port, self.use_ssl = host, port, use_ssl
        self.path, self.key, self.stop_at = path, key, stop_at
        self.latencies_ms = latencies_ms
        # per-worker counter, summed after join: a shared `errors[0] += 1` is
        # a non-atomic read-modify-write and undercounts across threads
        # (list.append below is a single atomic op, so latencies can be shared)
        self.errors = 0
        self.count = 0

    def run(self):
        conn_cls = http.client.HTTPSConnection if self.use_ssl else http.client.HTTPConnection
        conn = conn_cls(self.host, self.port, timeout=10)
        headers = {"x-api-key": self.key}
        while time.monotonic() < self.stop_at:
            t0 = time.monotonic()
            try:
                conn.request("GET", self.path, headers=headers)
                resp = conn.getresponse()
                resp.read()  # must drain body before reusing the connection
                if resp.status != 200:
                    self.errors += 1
                else:
                    self.latencies_ms.append((time.monotonic() - t0) * 1000)
                    self.count += 1
            except Exception:
                self.errors += 1
                conn.close()
                conn = conn_cls(self.host, self.port, timeout=10)


def run_at_concurrency(url, path, key, concurrency, duration):
    parts = urlsplit(url)
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    latencies_ms = []
    stop_at = time.monotonic() + duration
    workers = [
        Worker(host, port, parts.scheme == "https", path, key, stop_at, latencies_ms)
        for _ in range(concurrency)
    ]
    start = time.monotonic()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.monotonic() - start
    total = sum(w.count for w in workers)
    return total, elapsed, latencies_ms, sum(w.errors for w in workers)


def pct(data, p):
    if not data:
        return float("nan")
    data = sorted(data)
    k = (len(data) - 1) * p
    f, c = int(k), min(int(k) + 1, len(data) - 1)
    return data[f] + (data[c] - data[f]) * (k - f)


def run(args):
    levels = [int(c) for c in args.concurrency.split(",")]
    print(f"path={args.path}")
    print(f"{'conc':>5} {'req/s':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'errors':>7}")
    for c in levels:
        total, elapsed, latencies, errors = run_at_concurrency(
            args.url, args.path, args.key, c, args.duration)
        rps = total / elapsed if elapsed else 0
        print(f"{c:>5} {rps:>8.1f} {pct(latencies, 0.50):>8.1f} "
              f"{pct(latencies, 0.95):>8.1f} {pct(latencies, 0.99):>8.1f} {errors:>7}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed", help="write synthetic rows directly into a SQLite file")
    s.add_argument("--db", required=True)
    s.add_argument("--rows", type=int, default=5000)
    s.add_argument("--seed", type=int, default=0)

    r = sub.add_parser("run", help="load-test a running instance")
    r.add_argument("--url", required=True, help="e.g. http://127.0.0.1:8123")
    r.add_argument("--key", required=True, help="value of x-api-key")
    r.add_argument("--path", default="/movies", help="e.g. /movies or /movies/tt0000000")
    r.add_argument("--concurrency", default="1,2,4,8,16,32,64")
    r.add_argument("--duration", type=float, default=5.0, help="seconds per concurrency level")

    args = ap.parse_args()
    if args.cmd == "seed":
        seed(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
