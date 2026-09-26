#!/usr/bin/env python3
"""Throughput benchmark for moviedb's GET endpoints. Stdlib only.

POST /movies is left out on purpose. Its speed depends on OMDB's rate limit,
so benchmarking it would measure OMDB rather than moviedb.

Two subcommands:
  seed  write N synthetic rows straight into a SQLite file, without OMDB,
        so the table size is under your control and repeatable.
  run   load a running instance at increasing concurrency and report
        req/s, goodput and latency percentiles.

Usage:
    python3 scripts/bench.py seed --db /tmp/bench.db --rows 5000

    DB_PATH=/tmp/bench.db API_KEY=bench OMDB_KEY=unused \\
        target/x86_64-unknown-linux-musl/release/moviedb serve --port 8123 &

    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies --concurrency 1,2,4,8,16,32,64 --duration 5

    # compare with a single-movie lookup to separate the fixed cost of a
    # request from the cost of a large response body:
    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies/tt0000000 --concurrency 1,2,4,8,16,32,64 --duration 5

    # check whether compression helps at a given body size:
    python3 scripts/bench.py run --url https://omdb.luhann.com --key "$KEY" \\
        --path /movies --accept-encoding 'zstd, br, gzip' --concurrency 8

This script doesn't watch the server's memory. Do that on the server while
it runs:
    systemctl show moviedb -p MemoryCurrent
    journalctl -u moviedb -f

Run large --rows sweeps against a scratch database and instance, not the
live service. That's where you might hit an out-of-memory kill, and the live
service has little room to spare (MemoryMax=256M).


Why the client uses several processes
-------------------------------------
One Python process can't send requests fast enough to load the server,
because of the GIL. Against a local instance serving a 195KB list, one
process tops out around 2.5k req/s however many threads it has. Four
processes reach about 8.9k and eight about 16.5k against the same server.

A threads-only client therefore measures itself, and the results look
convincingly like server contention: throughput peaks around concurrency 2
and then drops. So `--concurrency` is always the total number of
connections, spread across processes. Each result row prints the split so
you can spot a client-bound result.


Results so far
--------------
Measured on prod with 2.x (184 movies, 200KB list body, 8 pooled
connections):
- GET /movies is limited by per-request work on the server, not bandwidth.
  Caddy already compresses responses (200KB becomes about 46-53KB), and
  asking for compression doesn't help: throughput goes from about 530 to 480
  req/s. A body 4x smaller that isn't any faster rules bandwidth out.
  --accept-encoding is there so you can check this again.

2.3 against 3.0, measured locally on 2026-09-26 with the server pinned to 2
cores and the same 222 seeded movies. req/s at concurrency 1 / 8 / 32:

                                2.3 (8-connection pool)    3.0 (one connection)
    GET /movies                 4.6k / 15.2k / 14.2k       1.4k / 1.8k / 1.8k
    GET /movies/tt0000001       8.2k / 54.0k / 67.8k       7.1k / 23.2k / 21.4k
    GET /movies/recent?limit=50 1.1k /  1.0k /  1.0k       3.1k / 5.2k / 5.3k
    GET /movies/{id}/history    7.8k / 52.3k / 67.1k       8.6k / 41.5k / 40.8k

- One connection costs 20-40% at high concurrency and nothing for a single
  client, which is the only load this service sees.
- GET /movies is slower because every doc now gets `personal` and
  `refreshed` appended by the movie_docs view (0.7ms instead of 0.2ms per
  request). Building the docs with json_set instead was 7x slower than 2.3.
  Appending in Rust would get most of it back; for one user it isn't worth
  it.
- GET /movies/recent is faster because it reads an index instead of running
  a subquery per movie.


Memory
------
GET /movies builds the whole response in memory for each request in
flight. Measure the effect rather than working it out, because the
arithmetic comes out far too low. Against MemoryMax=256M, starting from an
idle baseline of 15MB (on a server that hadn't served anything yet):

    184 rows  (200KB body)   conc 64   ->  83MB peak   (+68MB)
    2000 rows (2.9MB body)   conc 64   -> 151MB peak  (+136MB)

These were measured with the 8-connection pool in 2.x. Memory stopped
growing from concurrency 8 onwards (39/105/118/136/136 MB at 4/8/16/32/64 on
the 2000-row table), because only 8 requests could build a response at once.
The peak was still well above what was actually in use, because RSS includes
memory the allocator keeps hold of. That's why a guess of "about 8x the
table" was out by about a factor of six.

3.0 builds one response at a time. In the comparison above (222 rows, RSS
sampled every 50ms) the peak across all four endpoints was 33-43MB, against
68-151MB for 2.3.

Growth isn't linear in table size either: 10x the rows only doubled it,
because most of it is fixed per-connection and allocator overhead. From the
two points above, the 2.x limit was somewhere around 5k rows; 3.0's should
be higher, but that hasn't been measured. To get the real
number for a given table, sample RSS while you run the sweep:
    while :; do awk '/VmRSS/{print $2}' /proc/$(pgrep -x moviedb)/status; sleep 0.2; done

Take the baseline from a freshly started server. An earlier version of this
note said memory stayed flat under load, because its "idle" reading came
from a server that had already been benchmarked and was still holding on to
that memory.
"""
import argparse
import http.client
import json
import multiprocessing as mp
import os
import random
import sqlite3
import string
import threading
import time
from urllib.parse import urlsplit


def seed(args):
    conn = sqlite3.connect(args.db)
    # Must match TABLES and INDEXES in src/db.rs.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS movies (
            imdb_id   TEXT PRIMARY KEY,
            data      JSON NOT NULL,
            personal  INTEGER NOT NULL CHECK (personal BETWEEN 0 AND 100),
            refreshed TEXT NOT NULL,
            title     TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
            year      TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
        );
        CREATE INDEX IF NOT EXISTS idx_movies_title_year ON movies (title, year);
        CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);
        CREATE INDEX IF NOT EXISTS idx_movies_refreshed ON movies (refreshed, imdb_id);
    """)

    rng = random.Random(args.seed)
    rows = []
    for i in range(args.rows):
        imdb_id = f"tt{i:07d}"
        # About 1.2KB, similar to a real OMDB doc.
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
            ],
        }
        refreshed = f"2026-01-01T00:00:{i % 60:02d}.{i % 1000:03d}+00:00"
        rows.append((imdb_id, json.dumps(doc), 80, refreshed))
    conn.executemany(
        "INSERT OR REPLACE INTO movies (imdb_id, data, personal, refreshed)"
        " VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    print(f"seeded {args.rows} rows into {args.db}")


class Worker(threading.Thread):
    """One HTTP/1.1 connection per thread, reused for every request so
    connection setup doesn't skew the numbers."""

    def __init__(self, host, port, use_ssl, path, key, encoding, start_at, duration):
        super().__init__()
        self.host, self.port, self.use_ssl = host, port, use_ssl
        self.path, self.key, self.encoding = path, key, encoding
        self.start_at, self.duration = start_at, duration
        # Each worker keeps its own totals, since processes can't share
        # them. The parent adds them up at the end.
        self.latencies_ms = []
        self.errors = 0
        self.count = 0
        self.bytes = 0

    def _connect(self):
        cls = http.client.HTTPSConnection if self.use_ssl else http.client.HTTPConnection
        conn = cls(self.host, self.port, timeout=10)
        conn.connect()
        return conn

    def run(self):
        headers = {"x-api-key": self.key}
        if self.encoding:
            headers["Accept-Encoding"] = self.encoding
        try:
            conn = self._connect()
        except Exception:
            conn = None

        # Connect first, then wait for the shared start time. That keeps TLS
        # handshakes out of the measurement, and every worker starts at the
        # same moment. Without it, a high concurrency run would count dozens
        # of handshakes against the server's p99. time.time() is used
        # because monotonic clocks can't always be compared between
        # processes.
        delay = self.start_at - time.time()
        if delay > 0:
            time.sleep(delay)

        deadline = time.monotonic() + self.duration
        while time.monotonic() < deadline:
            if conn is None:
                try:
                    conn = self._connect()
                except Exception:
                    self.errors += 1
                    time.sleep(0.01)  # don't spin hot on a refused port
                    continue
            t0 = time.monotonic()
            try:
                conn.request("GET", self.path, headers=headers)
                resp = conn.getresponse()
                body = resp.read()  # must drain before reusing the connection
                if resp.status != 200:
                    self.errors += 1
                else:
                    self.latencies_ms.append((time.monotonic() - t0) * 1000)
                    # Bytes as sent, so compressed when --accept-encoding
                    # is used.
                    self.bytes += len(body)
                    self.count += 1
            except Exception:
                self.errors += 1
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None


def _run_child(payload):
    """One client process: run `threads` workers and return their totals."""
    threads, host, port, use_ssl, path, key, encoding, start_at, duration = payload
    workers = [
        Worker(host, port, use_ssl, path, key, encoding, start_at, duration)
        for _ in range(threads)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return {
        "count": sum(w.count for w in workers),
        "errors": sum(w.errors for w in workers),
        "bytes": sum(w.bytes for w in workers),
        "latencies": [ms for w in workers for ms in w.latencies_ms],
    }


def split_concurrency(total, max_procs):
    """Spread `total` connections over as many processes as cores allow.

    One process per connection until the cores run out, so the GIL is never
    the bottleneck. After that, connections are spread evenly: 64 over 32
    cores is 2 each.
    """
    procs = max(1, min(total, max_procs))
    base, extra = divmod(total, procs)
    return [base + (1 if i < extra else 0) for i in range(procs)]


def run_at_concurrency(url, path, key, encoding, total_conc, duration, max_procs):
    parts = urlsplit(url)
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    use_ssl = parts.scheme == "https"
    splits = split_concurrency(total_conc, max_procs)

    # Give every process time to start and connect before the run begins.
    # More processes need longer.
    start_at = time.time() + 0.75 + 0.05 * len(splits)
    payloads = [
        (t, host, port, use_ssl, path, key, encoding, start_at, duration)
        for t in splits
    ]

    if len(payloads) == 1:
        results = [_run_child(payloads[0])]
    else:
        with mp.Pool(processes=len(payloads)) as pool:
            results = pool.map(_run_child, payloads)

    total = sum(r["count"] for r in results)
    errors = sum(r["errors"] for r in results)
    nbytes = sum(r["bytes"] for r in results)
    latencies = [ms for r in results for ms in r["latencies"]]
    # Divide by `duration` rather than a measured time. Every worker runs for
    # exactly that long, and a measured time would include process startup.
    return total, duration, latencies, errors, nbytes, len(splits)


def pct(data, p):
    if not data:
        return float("nan")
    data = sorted(data)
    k = (len(data) - 1) * p
    f, c = int(k), min(int(k) + 1, len(data) - 1)
    return data[f] + (data[c] - data[f]) * (k - f)


def run(args):
    levels = [int(c) for c in args.concurrency.split(",")]
    max_procs = args.procs or os.cpu_count() or 1
    print(f"path={args.path}  encoding={args.accept_encoding or 'identity'}  "
          f"client procs<={max_procs}")
    print(f"{'conc':>5} {'procs':>6} {'req/s':>9} {'MB/s':>8} {'p50 ms':>8} "
          f"{'p95 ms':>8} {'p99 ms':>8} {'errors':>7}")
    for c in levels:
        total, elapsed, latencies, errors, nbytes, procs = run_at_concurrency(
            args.url, args.path, args.key, args.accept_encoding, c,
            args.duration, max_procs)
        rps = total / elapsed if elapsed else 0
        mbs = nbytes / elapsed / 1e6 if elapsed else 0
        print(f"{c:>5} {procs:>6} {rps:>9.1f} {mbs:>8.1f} "
              f"{pct(latencies, 0.50):>8.1f} {pct(latencies, 0.95):>8.1f} "
              f"{pct(latencies, 0.99):>8.1f} {errors:>7}")


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
    r.add_argument("--concurrency", default="1,2,4,8,16,32,64",
                   help="total connections per level, split across processes")
    r.add_argument("--duration", type=float, default=5.0, help="seconds per concurrency level")
    r.add_argument("--procs", type=int, default=None,
                   help="cap on client processes (default: CPU count)")
    r.add_argument("--accept-encoding", default=None,
                   help="e.g. 'zstd, br, gzip'; omitted means identity")

    args = ap.parse_args()
    if args.cmd == "seed":
        seed(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
