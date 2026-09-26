"""End-to-end test for the moviedb binary. Stdlib only.

Runs the real binary against a fake OMDB server and a temporary database.
Covers every endpoint, auth, the error responses (including unknown paths
and wrong methods), and the awkward refresh cases: an OMDB response with no
Title, a re-rate during a run, a rejected key, running out of quota, and
OMDB failing.

Usage:
    python3 tests/smoke_test.py [path-to-binary]
    # default: target/x86_64-unknown-linux-musl/release/moviedb
"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.path.join(REPO, "target/x86_64-unknown-linux-musl/release/moviedb")
)
API = "http://127.0.0.1:8123"
KEY = "smoke-test-key"

OMDB_DOC = {
    "Title": "The Matrix",
    "Year": "1999",
    "Rated": "R",
    "Ratings": [{"Source": "Internet Movie Database", "Value": "8.7/10"}],
    "imdbID": "tt0133093",
    "Type": "movie",
    "Response": "True",
}

# Set in main(). The fake OMDB uses it to re-rate a movie mid-refresh.
STUB_DB = None


def rerate_mid_run(imdb_id, value):
    """Changes a movie's rating the way a POST during a refresh would."""
    con = sqlite3.connect(STUB_DB)
    con.execute("UPDATE movies SET personal = ? WHERE imdb_id = ?", (value, imdb_id))
    con.commit()
    con.close()


class Stub(BaseHTTPRequestHandler):
    def do_GET(self):
        q = parse_qs(urlparse(self.path).query)
        apikey = q.get("apikey", [""])[0]
        title = q.get("t", [""])[0]
        imdb_id = q.get("i", [""])[0]
        status = 200
        if apikey == "revoked":
            status, doc = 401, {"Response": "False", "Error": "Invalid API key!"}
        elif apikey == "exhausted" or title == "TriggerDailyLimitAlt":
            doc = {"Response": "False", "Error": "Request limit reached!"}
        elif title == "TriggerDailyLimit":
            doc = {"Response": "False", "Error": "Daily request limit reached!"}
        elif title == "TriggerUnknownError":
            doc = {
                "Response": "False",
                "Error": "Some new OMDB error this stub doesn't know",
            }
        elif title == "TriggerHangUp":
            return
        elif title == "TriggerNotJson" or imdb_id.startswith("ttfail"):
            body = b"<html>upstream exploded</html>"
            self.send_response(502)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        elif imdb_id == "tt0000005":
            doc = {"Response": "False", "Error": "Error getting data."}
        else:
            doc = dict(OMDB_DOC)
            if imdb_id:
                doc["imdbID"] = imdb_id
            # tt0000002 succeeds but has no Title
            if imdb_id == "tt0000002":
                del doc["Title"]
            if imdb_id == "tt0000003":
                rerate_mid_run(imdb_id, 42)
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def req_full(method, path, key=KEY):
    r = urllib.request.Request(f"{API}{path}", method=method)
    if key:
        r.add_header("x-api-key", key)
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status, resp.read().decode(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), dict(e.headers)


def req(method, path, key=KEY):
    status, body, _ = req_full(method, path, key)
    return status, body


def scenario_db(tmp, src, name, keep_ids, extra_rows=()):
    """A copy of `src` holding only `keep_ids` plus `extra_rows`."""
    path = os.path.join(tmp, name + ".db")
    con = sqlite3.connect(src)
    con.execute("VACUUM INTO ?", (path,))
    con.close()
    con = sqlite3.connect(path)
    marks = ",".join("?" * len(keep_ids))
    con.execute(f"DELETE FROM movies WHERE imdb_id NOT IN ({marks})", keep_ids)
    for imdb_id, doc in extra_rows:
        insert_movie(con, imdb_id, doc)
    con.commit()
    con.close()
    return path


def movie_data(db_path, imdb_id):
    con = sqlite3.connect(db_path)
    row = con.execute("SELECT data FROM movies WHERE imdb_id = ?", (imdb_id,)).fetchone()
    con.close()
    return row[0]


# Sorts before anything refreshed during the test.
LONG_AGO = "2000-01-01T00:00:00.000+00:00"


def insert_movie(con, imdb_id, doc, personal=80):
    """Stores a movie directly, as if last refreshed long ago."""
    con.execute(
        "INSERT INTO movies (imdb_id, data, personal, refreshed) VALUES (?, ?, ?, ?)",
        (imdb_id, json.dumps(doc), personal, LONG_AGO),
    )


def titled(title):
    return {"title": title, "ratings": []}


def main():
    global STUB_DB
    if not os.path.exists(BIN):
        sys.exit(f"binary not found: {BIN} — build it or pass a path")
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "smoke.db")
        STUB_DB = db_path
        srv = HTTPServer(("127.0.0.1", 8098), Stub)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        env = dict(
            os.environ,
            API_KEY=KEY,
            OMDB_KEY="stub",
            DB_PATH=db_path,
            OMDB_URL="http://127.0.0.1:8098/",
        )

        proc = subprocess.Popen(
            [BIN, "serve", "--host", "127.0.0.1", "--port", "8123"], env=env
        )
        try:
            # Stop here if the server dies or never starts. Otherwise the
            # first test fails with a confusing traceback.
            for _ in range(50):
                if proc.poll() is not None:
                    sys.exit(f"server exited during startup (code {proc.poll()})")
                try:
                    req("GET", "/movies")
                    break
                except OSError:  # URLError incl. connection refused
                    time.sleep(0.1)
            else:
                sys.exit("server did not answer within 5s")

            # Check the full problem shape once here. Later error tests only
            # check the fields they care about.
            s, b, headers = req_full("GET", "/movies", key=None)
            prob = json.loads(b)
            results.append(
                (
                    "401 problem+json shape without key",
                    s == 401
                    and headers.get("content-type") == "application/problem+json"
                    and prob["type"] == "about:blank"
                    and prob["title"] == "Unauthorized"
                    and prob["status"] == 401
                    and "Invalid API key" in prob["detail"],
                )
            )

            # Unknown paths and wrong methods should still get JSON errors.
            # axum's defaults have empty bodies.
            s, b = req("GET", "/nonexistent")
            results.append(("404 JSON on unmatched path", s == 404 and "detail" in b))
            s, b, headers = req_full("DELETE", "/movies")
            results.append(
                (
                    "405 JSON + Allow on unsupported method",
                    s == 405 and "detail" in b and "GET" in headers.get("allow", ""),
                )
            )
            s, b = req("GET", "/nonexistent", key=None)
            results.append(
                ("401 precedes 404 on unmatched path", s == 401 and "detail" in b)
            )

            s, b, headers = req_full(
                "POST", "/movies?title=The+Matrix&rating=90&year=1999"
            )
            doc = json.loads(b) if s == 201 else {}
            results.append(
                (
                    "POST creates movie -> 201 JSON + Location",
                    s == 201
                    and doc.get("title") == "The Matrix"
                    and doc.get("imdb_id") == "tt0133093"  # imdbID -> snake_case
                    and doc.get("personal") == 90
                    and all(r["source"] != "Personal" for r in doc["ratings"])
                    and "response" not in doc
                    and headers.get("location") == "/movies/tt0133093",
                )
            )
            # POST fetches from OMDB too, so it sets refreshed. Otherwise a
            # new movie would go to the front of the next refresh.
            results.append(
                (
                    "POST sets refreshed",
                    doc.get("refreshed", "").endswith("+00:00")
                    and "_refreshed" not in doc,
                )
            )

            s, b = req("GET", "/movies")
            docs = json.loads(b)
            results.append(
                (
                    "GET /movies doc shape",
                    s == 200
                    and len(docs) == 1
                    and docs[0] == doc,
                )
            )
            s, _ = req("GET", "/movies/tt0133093")
            results.append(("GET /movies/{imdb_id}", s == 200))
            s, b = req("GET", "/movies?title=The+Matrix&year=1999")
            results.append(
                ("GET /movies title+year filter", s == 200 and len(json.loads(b)) == 1)
            )
            s, b = req("GET", "/movies?year=1999")
            results.append(
                (
                    "GET /movies single-param filter",
                    s == 200 and len(json.loads(b)) == 1,
                )
            )
            # A filter that matches nothing returns [], not an error.
            s, b = req("GET", "/movies?title=No+Such+Movie&year=1900")
            results.append(
                (
                    "GET /movies unmatched filter -> 200 []",
                    s == 200 and json.loads(b) == [],
                )
            )

            # Title and year aren't unique. Two movies with the same pair
            # should both be returned.
            dup_db = sqlite3.connect(db_path)
            insert_movie(
                dup_db,
                "tt0133093-dup",
                {"title": "The Matrix", "year": "1999", "ratings": []},
            )
            dup_db.commit()
            dup_db.close()
            s, b = req("GET", "/movies?title=The+Matrix&year=1999")
            ids = (
                sorted(d.get("imdb_id", "dup-has-none") for d in json.loads(b))
                if s == 200
                else []
            )
            results.append(
                (
                    "duplicate title+year filter returns both",
                    s == 200 and len(ids) == 2 and "tt0133093" in ids,
                )
            )
            s, _ = req("GET", "/movies/tt0133093-dup")
            results.append(("GET /movies/{imdb_id} still works for dup", s == 200))
            dup_db = sqlite3.connect(db_path)
            dup_db.execute("DELETE FROM movies WHERE imdb_id = 'tt0133093-dup'")
            dup_db.commit()
            dup_db.close()

            s, _ = req("GET", "/movies/tt9999999")
            results.append(("404 unknown id", s == 404))
            s, b = req("GET", "/movies/tt0133093/history")
            h = json.loads(b)
            results.append(
                (
                    "GET history shape+timestamp",
                    s == 200
                    and h["imdb_id"] == "tt0133093"
                    and len(h["snapshots"]) == 2
                    and h["snapshots"][0]["observed"].endswith("+00:00"),
                )
            )
            s, _ = req("GET", "/movies/tt9999999/history")
            results.append(("404 history for unknown id", s == 404))

            # Missing POST params get a 422 problem response, like any other
            # bad request, not axum's plain-text 400.
            s, b = req("POST", "/movies?title=The+Matrix")  # rating & year missing
            results.append(
                ("422 JSON on malformed POST params", s == 422 and "detail" in b)
            )

            # Empty params are treated as missing (422), not sent to OMDB.
            s, b = req("POST", "/movies?title=&rating=90&year=1999")
            results.append(("422 on empty POST param", s == 422 and "non-empty" in b))

            # rating is a whole number from 0 to 100, checked before OMDB is
            # asked anything.
            for bad in ["101", "9/10", "7.5", "-1", ""]:
                s, b = req("POST", f"/movies?title=The+Matrix&rating={bad}&year=1999")
                results.append((f"422 on rating={bad}", s == 422 and "detail" in b))

            # OMDB running out of quota isn't the caller's fault, so it's a
            # 503 with Retry-After, not a 429.
            s, b, headers = req_full(
                "POST", "/movies?title=TriggerDailyLimit&rating=1&year=2000"
            )
            prob = json.loads(b)
            results.append(
                (
                    "503 + Retry-After on OMDB daily limit",
                    s == 503
                    and "daily request limit" in prob["detail"].lower()
                    and prob["type"] == "about:blank"
                    and headers.get("retry-after") == "86400",
                )
            )

            # OMDB sometimes words the same error this way.
            s, b, headers = req_full(
                "POST", "/movies?title=TriggerDailyLimitAlt&rating=1&year=2000"
            )
            results.append(
                (
                    "503 on OMDB's alternate daily-limit wording",
                    s == 503 and headers.get("retry-after") == "86400",
                )
            )

            # An OMDB error we don't recognise is a 502.
            s, b = req("POST", "/movies?title=TriggerUnknownError&rating=1&year=2000")
            results.append(
                ("502 on unrecognized OMDB error", s == 502 and "detail" in b)
            )

            # OMDB failing to give a JSON answer is a 502 too, not a 500.
            s, b = req("POST", "/movies?title=TriggerNotJson&rating=1&year=2000")
            results.append(
                ("502 when OMDB's reply isn't JSON", s == 502 and "Bad response" in b)
            )
            s, b = req("POST", "/movies?title=TriggerHangUp&rating=1&year=2000")
            results.append(
                ("502 when OMDB hangs up", s == 502 and "Bad response" in b)
            )

            # POSTing a stored movie again is an update: 200, not 201, and
            # the new rating replaces the old one. No sleep is needed, since
            # timestamps have millisecond precision and won't clash with the
            # first POST's snapshot.
            s, b = req("POST", "/movies?title=The+Matrix&rating=95&year=1999")
            doc = json.loads(b) if s == 200 else {}
            results.append(
                ("POST updates existing movie -> 200 JSON", s == 200 and doc.get("personal") == 95)
            )
            s, b = req("GET", "/movies/tt0133093/history")
            h = json.loads(b)
            results.append(
                (
                    "update POST appended a second snapshot pair",
                    s == 200 and len(h["snapshots"]) == 4,
                )
            )

            # /movies/recent returns plain stored docs, like GET /movies.
            s, b = req("GET", "/movies/recent?limit=5")
            recent = json.loads(b)
            results.append(
                (
                    "GET /movies/recent bare array of stored docs",
                    s == 200
                    and isinstance(recent, list)
                    and len(recent) == 1
                    and recent[0]["imdb_id"] == "tt0133093"
                    and recent[0]["refreshed"].endswith("+00:00")
                    and "last_refreshed" not in recent[0],
                )
            )

            # limit=0 should return []. There's a movie in the database, so
            # this proves it wasn't treated as limit=1.
            s, b = req("GET", "/movies/recent?limit=0")
            results.append(
                ("GET /movies/recent?limit=0 -> []", s == 200 and json.loads(b) == [])
            )
        finally:
            proc.terminate()
            proc.wait()

        # An empty API_KEY must stop the server starting. Otherwise an empty
        # x-api-key header would match it.
        p = subprocess.run(
            [BIN, "serve", "--port", "8124"],
            env=dict(env, API_KEY=""),
            capture_output=True,
            text=True,
            timeout=10,
        )
        results.append(
            ("refuses empty API_KEY", p.returncode == 1 and "API_KEY" in p.stderr)
        )

        # An empty DB_PATH should mean "use the default path", not an empty
        # path (which SQLite opens as a temporary database). The default path
        # doesn't exist on a workstation, so the server should fail to start.
        try:
            p = subprocess.run(
                [BIN, "serve", "--host", "127.0.0.1", "--port", "8124"],
                env=dict(env, DB_PATH=""),
                capture_output=True,
                text=True,
                timeout=5,
            )
            empty_db_path_ok = "/var/lib/moviedb/movies.db" in p.stderr
        except subprocess.TimeoutExpired:
            empty_db_path_ok = False
        results.append(("empty DB_PATH falls back to the default", empty_db_path_ok))

        # Refresh edge cases: a response with no Title is skipped without
        # saving, and a re-rate made during the run is kept.
        db = sqlite3.connect(db_path)
        b_doc = {"title": "TitleGone", "year": "2003", "ratings": []}
        insert_movie(db, "tt0000002", b_doc)
        insert_movie(db, "tt0000003", titled("Rerated"), personal=50)
        insert_movie(db, "tt0000005", titled("GoneFromOmdb"))
        db.commit()
        db.close()

        out = subprocess.run(
            [BIN, "refresh", db_path, "--sleep", "0"],
            env=env,
            capture_output=True,
            text=True,
        )
        db = sqlite3.connect(db_path)
        b_after = db.execute(
            "SELECT data FROM movies WHERE imdb_id='tt0000002'"
        ).fetchone()[0]
        results.append(
            (
                "refresh: missing Title not persisted",
                "missing Title" in out.stdout and b_after == json.dumps(b_doc),
            )
        )
        results.append(
            (
                "refresh: The Matrix refreshed",
                "OK   The Matrix (1999)" in out.stdout and out.returncode == 0,
            )
        )
        personal, refreshed = db.execute(
            "SELECT personal, refreshed FROM movies WHERE imdb_id = 'tt0000003'"
        ).fetchone()
        results.append(
            (
                "refresh: keeps a re-rate that landed mid-run",
                personal == 42 and refreshed != LONG_AGO,
            )
        )
        results.append(
            (
                "refresh: unknown id skipped, run still succeeds",
                "SKIP GoneFromOmdb [tt0000005]" in out.stdout,
            )
        )
        db.close()

        refresh_env = dict(env)
        del refresh_env["DB_PATH"]

        def run_refresh(path, *extra, **env_over):
            return subprocess.run(
                [BIN, "refresh", path, "--sleep", "0", *extra],
                env=dict(refresh_env, **env_over),
                capture_output=True,
                text=True,
                timeout=60,
            )

        # One failure is skipped, but the run still exits non-zero.
        path = scenario_db(
            tmp, db_path, "transient", ["tt0133093"], [("ttfail1", titled("Flaky"))]
        )
        out = run_refresh(path)
        results.append(
            (
                "refresh: continues past a transient failure, exits non-zero",
                out.returncode == 1
                and "FAIL Flaky" in out.stderr
                and "OK   The Matrix" in out.stdout
                and "1 failed" in out.stdout,
            )
        )

        # Three failures in a row means OMDB is down, so stop. The Matrix has
        # been refreshed before, so it comes last.
        path = scenario_db(
            tmp,
            db_path,
            "outage",
            ["tt0133093"],
            [(f"ttfail{i}", titled(f"Flaky{i}")) for i in range(3)],
        )
        out = run_refresh(path)
        results.append(
            (
                "refresh: aborts after consecutive failures",
                out.returncode == 1
                and "failed in a row" in out.stderr
                and "1 remaining" in out.stdout,
            )
        )

        # A rejected key stops the run. Otherwise every movie would be
        # skipped and the run would exit 0.
        path = scenario_db(tmp, db_path, "revoked", ["tt0133093"])
        before = movie_data(path, "tt0133093")
        out = run_refresh(path, OMDB_KEY="revoked")
        results.append(
            (
                "refresh: rejected OMDB key aborts non-zero",
                out.returncode == 1
                and "Invalid API key" in out.stderr
                and movie_data(path, "tt0133093") == before,
            )
        )

        # Running out of quota is normal. Stop and exit 0.
        path = scenario_db(tmp, db_path, "exhausted", ["tt0133093"])
        before = movie_data(path, "tt0133093")
        out = run_refresh(path, OMDB_KEY="exhausted")
        results.append(
            (
                "refresh: alternate daily-limit wording stops cleanly",
                out.returncode == 0
                and "daily limit hit" in out.stdout
                and movie_data(path, "tt0133093") == before,
            )
        )

        out = subprocess.run(
            [BIN, "refresh", db_path, "--sleep=-1"],
            env=refresh_env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        results.append(
            (
                "refresh: rejects a negative --sleep",
                out.returncode == 2 and "negative" in out.stderr,
            )
        )

    ok = True
    for name, passed in results:
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
        ok &= passed
    print("\n" + ("all good" if ok else "FAILURES — do not ship"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
