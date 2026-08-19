#!/usr/bin/env python3
"""Throughput benchmark for moviedb's GET endpoints. Stdlib only.

Deliberately excludes POST /movies — that path is bound by OMDB's own rate
limit, not by anything moviedb's server does, so hammering it measures OMDB,
not this codebase.

The load generator is multi-process, not just multi-threaded, and that is the
whole reason this file is shaped the way it is. A single CPython process is
GIL-bound well below what the server can serve: measured against a local
instance returning a 195KB list body, one process plateaus at ~2.5k req/s no
matter how many threads it runs, while four processes reach ~8.9k and eight
reach ~16.5k against that same server. A threads-only client therefore reports
the *client's* ceiling and calls it the server's — and it does so in a
convincing shape, with throughput peaking around concurrency 2 and sagging
after, which reads exactly like server-side contention. `--concurrency` here is
always the TOTAL connection count; it is split across processes so the client
stays out of the way. The split is printed with every row so a client-bound
result stays visible instead of being silently absorbed.

What the measurements actually showed (prod, 184 movies, 200KB list body):
  - DB_POOL_SIZE=8 is not the limiting factor for anything. Point lookups run
    ~10us in SQLite, so 8 pooled connections sustain ~11k req/s end-to-end;
    the pool never becomes the queue.
  - GET /movies is bound by per-request server cost, not bandwidth. Caddy
    already negotiates zstd/br/gzip (200KB -> ~46-53KB on the wire), and
    asking for compression does not raise throughput — it moves ~530 req/s to
    ~480. A 4x smaller body buying nothing is what rules bandwidth out, so
    --accept-encoding exists to let you re-check that rather than assume it.
  - GET /movies/recent is the costliest path per row: 50 rows through it
    (~359 req/s) is slower than all 184 movies through GET /movies
    (~530 req/s). The correlated MAX(observed) subquery and the per-doc
    splice dominate, so vary --path over ?limit= to see it.

GET /movies still collects the whole table per in-flight request
(list_movies: `rows.collect()`), and the resulting memory curve is worth
measuring rather than deriving — the arithmetic understates it badly. Measured
against systemd's MemoryMax=256M, from a 15MB never-served idle baseline:

    184 rows  (200KB body)   conc 64   ->  83MB peak   (+68MB)
    2000 rows (2.9MB body)   conc 64   -> 151MB peak  (+136MB)

Two things that only show up on a real run. First, growth plateaus from
concurrency 8 (39/105/118/136/136 MB at conc 4/8/16/32/64 on the 2000-row
table): DB_POOL_SIZE caps how many whole-table copies are ever live at once,
so pushing concurrency past the pool costs no further memory. Second, the
plateau still lands far above the live set, because peak RSS includes
allocator retention rather than just what is in flight — which is why a
"~8x the table" estimate is off by roughly a factor of six.

Growth is also not linear in table size (10x the rows only doubled it), most
of it being fixed per-connection and allocator overhead. Extrapolating the two
points above puts the cap somewhere near 5k rows. Sample RSS while you sweep
if you want the real number for a given table:
    while :; do awk '/VmRSS/{print $2}' /proc/$(pgrep -x moviedb)/status; sleep 0.2; done

Take the measure-first point generally: an earlier pass at this file reported
RSS as flat under load, having read its "idle" baseline off a server that had
already served the ladder, so it was reading retained memory as the floor.

Two subcommands:
  seed  write N synthetic rows directly into a SQLite file, bypassing OMDB
        entirely, so table size is controllable and repeatable.
  run   hammer a running instance at increasing concurrency levels and
        report req/s, goodput, and latency percentiles.

Usage:
    python3 scripts/bench.py seed --db /tmp/bench.db --rows 5000

    DB_PATH=/tmp/bench.db API_KEY=bench OMDB_KEY=unused \\
        target/x86_64-unknown-linux-musl/release/moviedb serve --port 8123 &

    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies --concurrency 1,2,4,8,16,32,64 --duration 5

    # compare against the point-lookup path to separate per-request server
    # cost from anything proportional to the response body:
    python3 scripts/bench.py run --url http://127.0.0.1:8123 --key bench \\
        --path /movies/tt0000000 --concurrency 1,2,4,8,16,32,64 --duration 5

    # and re-check the bandwidth question on any new body size:
    python3 scripts/bench.py run --url https://omdb.luhann.com --key "$KEY" \\
        --path /movies --accept-encoding 'zstd, br, gzip' --concurrency 8

Watch memory on the box being tested while this runs — this script doesn't
sample it remotely:
    systemctl show moviedb -p MemoryCurrent
    journalctl -u moviedb -f

Run a large --rows sweep against a scratch DB/instance rather than the live
service: that is the configuration that can still find an OOM, and the live
service has no headroom to spare (MemoryMax=256M).
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            imdb_id TEXT PRIMARY KEY,
            data    JSON NOT NULL,
            title   TEXT GENERATED ALWAYS AS (json_extract(data, '$.title')) VIRTUAL,
            year    TEXT GENERATED ALWAYS AS (json_extract(data, '$.year'))  VIRTUAL
        )
    """)  # must match init_db in src/db.rs
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

    def __init__(self, host, port, use_ssl, path, key, encoding, start_at, duration):
        super().__init__()
        self.host, self.port, self.use_ssl = host, port, use_ssl
        self.path, self.key, self.encoding = path, key, encoding
        self.start_at, self.duration = start_at, duration
        # All per-worker, summed after join. Threads within one process could
        # share a list (append is atomic), but processes can't share anything
        # without pickling it back, so every worker owns its own totals and
        # the aggregation happens once, in the parent.
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

        # Connect first, then wait: the handshake is paid before the measured
        # window, and every worker in every process unblocks at the same
        # wall-clock instant. Without this, high concurrency levels open N TLS
        # connections *during* the run and charge the resulting handshake
        # storm to the server's p99. time.time() rather than time.monotonic()
        # because monotonic epochs are only comparable across processes on
        # some platforms; over a sub-second barrier, wall-clock skew is a
        # non-issue.
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
                    # Wire bytes, so this stays the compressed size when
                    # --accept-encoding is in play. That's the number the
                    # bandwidth question needs.
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
    """One client process: run `threads` workers and hand totals back up."""
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

    One process per connection until cores run out, because a process with one
    connection can never be the GIL bottleneck. Remainder threads land on the
    lowest-indexed processes, so 64 connections over 32 cores is 32x2 rather
    than a lopsided split that would make some processes saturate first.
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

    # Enough slack for every child to fork, import, and finish its handshakes
    # before the barrier lifts. Scales with process count because forking 32
    # interpreters costs more than forking 2.
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
    # `duration` is the denominator rather than a measured elapsed: the barrier
    # makes every worker's window that long by construction, and a measured
    # span would fold each child's fork and join overhead into the rate.
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
