"""Property-based fuzzing of the moviedb API against openapi.yaml.

smoke_test.py tests the cases I thought of. This one checks the server
against the spec: schemathesis generates requests from openapi.yaml and
checks that every response has a documented status and content type, a body
matching the schema, and isn't a 5xx. Since the spec is written by hand,
this is what catches it falling out of date.

It uses the same setup as smoke_test.py (the real binary, a fake OMDB and a
temporary database) on different ports, so both can run at once. Nothing
touches the real OMDB or a real database.

schemathesis isn't a dependency of the repo. If it isn't installed,
`uv run` fetches it into a temporary environment for the run.

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

# Reuse smoke_test.py's fake OMDB so there's only one to keep up to date.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from smoke_test import Stub  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC = os.path.join(REPO, "openapi.yaml")
DEFAULT_BIN = os.path.join(REPO, "target/x86_64-unknown-linux-musl/release/moviedb")

# Different ports from smoke_test.py (8123/8098), so the two can run at the
# same time without answering each other's requests.
API_PORT = 8125
OMDB_PORT = 8099
KEY = "fuzz-test-key"

# The fake OMDB always returns this movie. It's added before the run so the
# {imdb_id} endpoints can return 200 and not only 404.
SEED_ID = "tt0133093"

# Every check except positive_data_acceptance, which expects valid input
# never to get a 4xx. `tt404` is a valid imdb_id for a movie that isn't in
# the database, and 404 is the right answer, so that check would always
# fail.
EXCLUDED_CHECKS = "positive_data_acceptance"


def api_request(method, path, key=KEY, timeout=5):
    """A small client for startup and seeding. schemathesis does the fuzzing."""
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
    """Use an installed schemathesis if there is one, otherwise get it via uv.

    Pinned to v4 because the flags below (--url, --exclude-checks, --mode)
    are named differently in 3.x.
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

            # Add the movie through the API rather than sqlite3, so it's
            # stored exactly the way the server stores movies.
            status, body = api_request(
                "POST", "/movies?title=The+Matrix&rating=90&year=1999"
            )
            if status not in (200, 201) or json.loads(body).get("imdb_id") != SEED_ID:
                sys.exit(f"seeding failed: {status} {body[:200]}")

            cmd = schemathesis_argv() + [
                "run", SPEC,
                "--url", f"http://127.0.0.1:{API_PORT}",
                "--header", f"x-api-key: {KEY}",
                "--checks", "all",
                "--exclude-checks", EXCLUDED_CHECKS,
                # Generate invalid input as well as valid, since the 400/422
                # handling is what most needs fuzzing.
                "--mode", "all",
                "--max-examples", str(args.max_examples),
                "--workers", str(args.workers),
                # The real key is passed with --header. Without this,
                # schemathesis makes up its own x-api-key values and every
                # request gets a 401.
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
