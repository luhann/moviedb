"""Property-based fuzzing of the moviedb API against openapi.yaml.

Where smoke_test.py asserts the cases I thought of, this asserts the contract
itself: schemathesis generates requests from openapi.yaml and checks every
response against it — status documented, content type documented, body
matching the schema, no 5xx. It's the drift detector for a hand-written spec.

Same harness as smoke_test.py (real binary, stub OMDB, temp DB) on different
ports, so both can run at once. Nothing here touches the real OMDB or a real
database: every generated POST resolves against the stub.

schemathesis isn't a repo dependency — it's fetched into an ephemeral env by
`uv run` for the duration of the run, so nothing is installed system-wide.

Usage:
    python3 tests/fuzz_test.py [path-to-binary] [-n N] [-w N] [--seed N]
    # default: target/x86_64-unknown-linux-musl/release/moviedb
"""

import argparse
import http.client
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import HTTPServer

# The stub OMDB server and its canned payload are smoke_test.py's; importing
# keeps one definition of "what OMDB looks like" instead of two that drift.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from smoke_test import Stub  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC = os.path.join(REPO, "openapi.yaml")
DEFAULT_BIN = os.path.join(REPO, "target/x86_64-unknown-linux-musl/release/moviedb")

# Deliberately not smoke_test.py's 8123/8098, so the two suites can run
# concurrently (and so a stray process from one doesn't silently serve the
# other's requests).
API_PORT = 8125
OMDB_PORT = 8099
KEY = "fuzz-test-key"

# The imdb_id the stub always resolves to, seeded before the run so the
# {imdb_id} endpoints have a real 200 path and not just 404s.
SEED_ID = "tt0133093"

# `all` minus one. positive_data_acceptance asserts that schema-valid input
# never gets a 4xx, which is wrong for a lookup: `tt404` is a perfectly valid
# imdb_id for a movie that simply isn't in the database, and 404 is the
# correct answer. The check can't tell "malformed" from "well-formed but
# absent", so it would fail on correct behaviour every run.
EXCLUDED_CHECKS = "positive_data_acceptance"


def api_request(method, path, key=KEY, timeout=5):
    """Minimal client for warmup/seeding — the fuzzing itself is schemathesis'."""
    conn = http.client.HTTPConnection("127.0.0.1", API_PORT, timeout=timeout)
    try:
        conn.request(method, path, headers={"x-api-key": key} if key else {})
        resp = conn.getresponse()
        return resp.status, resp.read().decode()
    finally:
        conn.close()


def wait_for_server(proc):
    for _ in range(50):
        if proc.poll() is not None:
            sys.exit(f"server exited during startup (code {proc.poll()})")
        try:
            api_request("GET", "/movies")
            return
        except OSError:
            time.sleep(0.1)
    sys.exit("server did not answer within 5s")


def schemathesis_argv():
    """Prefer an already-installed schemathesis; otherwise let uv fetch one.

    Pinned to v4: the flag names below (--url, --exclude-checks, --mode) are
    4.x spellings and differ in 3.x.
    """
    if shutil.which("schemathesis"):
        return ["schemathesis"]
    if shutil.which("uv"):
        return ["uv", "run", "--quiet", "--with", "schemathesis>=4,<5", "--",
                "schemathesis"]
    sys.exit("need either `schemathesis` or `uv` on PATH")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("binary", nargs="?", default=DEFAULT_BIN)
    ap.add_argument("-n", "--max-examples", type=int, default=50,
                    help="test cases per operation (default 50)")
    ap.add_argument("-w", "--workers", type=int, default=2,
                    help="concurrent workers (default 2)")
    ap.add_argument("--seed", type=int,
                    help="fix the random seed to reproduce a failing run")
    args = ap.parse_args()

    if not os.path.exists(args.binary):
        sys.exit(f"binary not found: {args.binary} — build it or pass a path")
    if not os.path.exists(SPEC):
        sys.exit(f"spec not found: {SPEC}")

    with tempfile.TemporaryDirectory() as tmp:
        srv = HTTPServer(("127.0.0.1", OMDB_PORT), Stub)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        env = dict(
            os.environ,
            API_KEY=KEY,
            OMDB_KEY="stub",
            DB_PATH=os.path.join(tmp, "fuzz.db"),
            OMDB_URL=f"http://127.0.0.1:{OMDB_PORT}/",
        )
        proc = subprocess.Popen(
            [args.binary, "serve", "--host", "127.0.0.1", "--port", str(API_PORT)],
            env=env,
        )
        try:
            wait_for_server(proc)

            # Seed via the API rather than sqlite3 so the row is written the
            # way the server writes one (normalized doc + a history snapshot),
            # which is what the response schemas describe.
            status, body = api_request(
                "POST", "/movies?title=The+Matrix&rating=9/10&year=1999"
            )
            if status not in (200, 201) or json.loads(body).get("imdb_id") != SEED_ID:
                sys.exit(f"seeding failed: {status} {body[:200]}")

            cmd = schemathesis_argv() + [
                "run", SPEC,
                "--url", f"http://127.0.0.1:{API_PORT}",
                "--header", f"x-api-key: {KEY}",
                "--checks", "all",
                "--exclude-checks", EXCLUDED_CHECKS,
                # Generate both valid and deliberately invalid input: the
                # 400/422 boundaries are the part of this API most worth
                # having a fuzzer lean on.
                "--mode", "all",
                "--max-examples", str(args.max_examples),
                "--workers", str(args.workers),
                # The real key travels in --header; without this schemathesis
                # also synthesizes its own x-api-key values from the security
                # scheme, and every generated request becomes a 401.
                "--generation-with-security-parameters", "false",
                "--continue-on-failure",
            ]
            if args.seed is not None:
                cmd += ["--seed", str(args.seed)]

            print(f"$ {' '.join(cmd)}\n", flush=True)
            return subprocess.call(cmd, cwd=REPO)
        finally:
            proc.terminate()
            proc.wait()
            srv.shutdown()


if __name__ == "__main__":
    sys.exit(main())
