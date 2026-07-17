"""End-to-end smoke test for the moviedb binary. Stdlib only.

Runs the real binary against a stub OMDB server and a temp DB: the full
endpoint matrix (auth, POST /movies, GET /movies with and without filters,
GET /movies/recent, GET /movies/{imdb_id}, GET /movies/{imdb_id}/history,
error paths) plus
refresh edge cases (null Personal rating, OMDB response missing Title).

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
BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    REPO, "target/x86_64-unknown-linux-musl/release/moviedb")
API = "http://127.0.0.1:8123"
KEY = "smoke-test-key"

OMDB_DOC = {
    "Title": "The Matrix", "Year": "1999", "Rated": "R",
    "Ratings": [{"Source": "Internet Movie Database", "Value": "8.7/10"}],
    "imdbID": "tt0133093", "Type": "movie", "Response": "True",
}


class Stub(BaseHTTPRequestHandler):
    def do_GET(self):
        q = parse_qs(urlparse(self.path).query)
        title = q.get("t", [""])[0]
        if title == "TriggerDailyLimit":
            doc = {"Response": "False", "Error": "Daily request limit reached!"}
        elif title == "TriggerUnknownError":
            doc = {"Response": "False", "Error": "Some new OMDB error this stub doesn't know"}
        else:
            doc = dict(OMDB_DOC)
            # refresh-by-id path: tt0000002 gets Response=True with no Title
            if q.get("i", [""])[0] == "tt0000002":
                del doc["Title"]
                doc["imdbID"] = "tt0000002"
        body = json.dumps(doc).encode()
        self.send_response(200)
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


def main():
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "smoke.db")
        srv = HTTPServer(("127.0.0.1", 8098), Stub)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        env = dict(os.environ, API_KEY=KEY, OMDB_KEY="stub", DB_PATH=db_path,
                   OMDB_URL="http://127.0.0.1:8098/")

        proc = subprocess.Popen(
            [BIN, "serve", "--host", "127.0.0.1", "--port", "8123"], env=env)
        try:
            for _ in range(50):
                try:
                    req("GET", "/movies")
                    break
                except Exception:
                    time.sleep(0.1)

            s, b = req("GET", "/movies", key=None)
            results.append(("401 without key", s == 401 and "Invalid API key" in b))

            s, b, headers = req_full("POST", "/movies?title=The+Matrix&rating=9/10&year=1999")
            doc = json.loads(b) if s == 201 else {}
            results.append((
                "POST creates movie -> 201 JSON + Location",
                s == 201 and doc.get("title") == "The Matrix"
                and doc.get("imdb_id") == "tt0133093"  # imdbID -> snake_case
                and doc["ratings"][-1] == {"source": "Personal", "value": "9/10"}
                and "response" not in doc
                and headers.get("location") == "/movies/tt0133093"))

            s, b = req("GET", "/movies")
            docs = json.loads(b)
            results.append((
                "GET /movies doc shape",
                s == 200 and len(docs) == 1 and docs[0]["title"] == "The Matrix"
                and docs[0]["ratings"][-1] == {"source": "Personal", "value": "9/10"}
                and "response" not in docs[0]))
            s, _ = req("GET", "/movies/tt0133093")
            results.append(("GET /movies/{imdb_id}", s == 200))
            s, b = req("GET", "/movies?title=The+Matrix&year=1999")
            results.append((
                "GET /movies title+year filter",
                s == 200 and len(json.loads(b)) == 1))
            s, b = req("GET", "/movies?year=1999")
            results.append((
                "GET /movies single-param filter",
                s == 200 and len(json.loads(b)) == 1))
            # A filter matching nothing is an empty collection, not an error.
            s, b = req("GET", "/movies?title=No+Such+Movie&year=1900")
            results.append(("GET /movies unmatched filter -> 200 []", s == 200 and json.loads(b) == []))

            # (title, year) isn't UNIQUE — a second movie sharing both is
            # ordinary filter output (both rows), not an error and not a
            # silent pick of one of them.
            dup_db = sqlite3.connect(db_path)
            dup_db.execute("INSERT INTO movies VALUES (?, ?)", ("tt0133093-dup", json.dumps(
                {"title": "The Matrix", "year": "1999", "ratings": []})))
            dup_db.commit()
            dup_db.close()
            s, b = req("GET", "/movies?title=The+Matrix&year=1999")
            ids = sorted(d.get("imdb_id", "dup-has-none") for d in json.loads(b)) if s == 200 else []
            results.append((
                "duplicate title+year filter returns both",
                s == 200 and len(ids) == 2 and "tt0133093" in ids))
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
            results.append((
                "GET history shape+timestamp",
                s == 200 and h["imdb_id"] == "tt0133093" and len(h["snapshots"]) == 2
                and h["snapshots"][0]["observed"].endswith("+00:00")))
            s, _ = req("GET", "/movies/tt9999999/history")
            results.append(("404 history for unknown id", s == 404))

            # Missing required POST params must fail the same way every other
            # invalid request does (422 JSON), not axum's default rejection
            # (400 plain text) — previously a documented gap, now closed.
            s, b = req("POST", "/movies?title=The+Matrix")  # rating & year missing
            results.append(("422 JSON on malformed POST params", s == 422 and "detail" in b))

            # Present-but-empty params are as unresolvable as missing ones —
            # 422, not a pass-through to OMDB and whatever it answers.
            s, b = req("POST", "/movies?title=&rating=9/10&year=1999")
            results.append(("422 on empty POST param", s == 422 and "non-empty" in b))

            # OMDB's shared daily quota being exhausted is an upstream
            # problem, not the caller's — 503 + Retry-After, not 429.
            s, b, headers = req_full("POST", "/movies?title=TriggerDailyLimit&rating=1&year=2000")
            results.append((
                "503 + Retry-After on OMDB daily limit",
                s == 503 and "daily request limit" in b.lower()
                and headers.get("retry-after") == "86400"))

            # An OMDB error this server doesn't recognize is a bad response
            # from an upstream dependency — 502, not the non-standard 520.
            s, b = req("POST", "/movies?title=TriggerUnknownError&rating=1&year=2000")
            results.append(("502 on unrecognized OMDB error", s == 502 and "detail" in b))

            # Re-POSTing an already-stored imdb_id is an update, not a
            # create: 200, not 201 — and the new rating replaces the old one.
            # No sleep needed: `observed` is millisecond-precision, so this
            # snapshot can't collide with the first POST's (which second-
            # precision timestamps used to, silently dropping it).
            s, b = req("POST", "/movies?title=The+Matrix&rating=9.5/10&year=1999")
            doc = json.loads(b) if s == 200 else {}
            results.append((
                "POST updates existing movie -> 200 JSON",
                s == 200 and doc.get("ratings", [{}])[-1] == {"source": "Personal", "value": "9.5/10"}))
            s, b = req("GET", "/movies/tt0133093/history")
            h = json.loads(b)
            results.append((
                "update POST appended a second snapshot pair",
                s == 200 and len(h["snapshots"]) == 4))

            # /movies/recent is a bare array like GET /movies (the
            # envelope-free collection convention), with last_refreshed
            # folded into each doc, not carried on a wrapper object.
            s, b = req("GET", "/movies/recent?limit=5")
            recent = json.loads(b)
            results.append((
                "GET /movies/recent bare array + last_refreshed",
                s == 200 and isinstance(recent, list) and len(recent) == 1
                and recent[0]["imdb_id"] == "tt0133093"
                and recent[0]["last_refreshed"].endswith("+00:00")))
        finally:
            proc.terminate()
            proc.wait()

        # empty API_KEY must be a startup failure, not silently-open auth
        # (an empty x-api-key header is legal HTTP and would match it)
        p = subprocess.run([BIN, "serve", "--port", "8124"],
                           env=dict(env, API_KEY=""),
                           capture_output=True, text=True, timeout=10)
        results.append(("refuses empty API_KEY",
                        p.returncode == 1 and "API_KEY" in p.stderr))

        # refresh edge cases: null Personal Value skips without an OMDB call;
        # Response=True missing Title skips without persisting
        db = sqlite3.connect(db_path)
        db.execute("INSERT INTO movies VALUES (?, ?)", ("tt0000001", json.dumps(
            {"title": "NullVal", "year": "2001",
             "ratings": [{"source": "Personal", "value": None}]})))
        b_doc = json.dumps({"title": "TitleGone", "year": "2003",
                            "ratings": [{"source": "Personal", "value": "8/10"}]})
        db.execute("INSERT INTO movies VALUES (?, ?)", ("tt0000002", b_doc))
        db.commit()
        db.close()

        out = subprocess.run([BIN, "refresh", db_path, "--sleep", "0"],
                             env=env, capture_output=True, text=True)
        db = sqlite3.connect(db_path)
        b_after = db.execute(
            "SELECT data FROM movies WHERE imdb_id='tt0000002'").fetchone()[0]
        results.append(("refresh: null Personal Value skipped",
                        "SKIP NullVal: no Personal rating" in out.stdout))
        results.append(("refresh: missing Title not persisted",
                        "missing Title" in out.stdout and b_after == b_doc))
        results.append(("refresh: The Matrix refreshed",
                        "OK   The Matrix (1999)" in out.stdout
                        and out.returncode == 0))
        db.close()

    ok = True
    for name, passed in results:
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
        ok &= passed
    print("\n" + ("all good" if ok else "FAILURES — do not ship"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
