"""End-to-end smoke test for the moviedb binary. Stdlib only.

Runs the real binary against a stub OMDB server and a temp DB: the full
endpoint matrix (auth, POST, GET /, /single, /history, error paths) plus
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


def req(method, path, key=KEY):
    r = urllib.request.Request(f"{API}{path}", method=method)
    if key:
        r.add_header("x-api-key", key)
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


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
                    req("GET", "/")
                    break
                except Exception:
                    time.sleep(0.1)

            s, b = req("GET", "/", key=None)
            results.append(("401 without key", s == 401 and "Invalid API key" in b))
            s, b = req("POST", "/?title=The+Matrix&rating=9/10&year=1999")
            results.append(("POST -> 200 plaintext title", s == 200 and b == "The Matrix"))
            s, b = req("GET", "/")
            docs = json.loads(b)
            results.append((
                "GET / doc shape",
                s == 200 and len(docs) == 1 and docs[0]["title"] == "The Matrix"
                and docs[0]["ratings"][-1] == {"Source": "Personal", "Value": "9/10"}
                and "response" not in docs[0]))
            s, _ = req("GET", "/single?imdbid=tt0133093")
            results.append(("GET /single by imdbid", s == 200))
            s, _ = req("GET", "/single?title=The+Matrix&year=1999")
            results.append(("GET /single by title+year", s == 200))
            s, b = req("GET", "/single")
            results.append(("422 no params", s == 422 and "Provide imdbid" in b))
            s, _ = req("GET", "/single?imdbid=tt9999999")
            results.append(("404 unknown id", s == 404))
            s, b = req("GET", "/history?imdbid=tt0133093")
            h = json.loads(b)
            results.append((
                "GET /history shape+timestamp",
                s == 200 and h["imdbid"] == "tt0133093" and len(h["snapshots"]) == 2
                and h["snapshots"][0]["observed"].endswith("+00:00")))
        finally:
            proc.terminate()
            proc.wait()

        # refresh edge cases: null Personal Value skips without an OMDB call;
        # Response=True missing Title skips without persisting
        db = sqlite3.connect(db_path)
        db.execute("INSERT INTO movies VALUES (?, ?)", ("tt0000001", json.dumps(
            {"title": "NullVal", "year": "2001",
             "ratings": [{"Source": "Personal", "Value": None}]})))
        b_doc = json.dumps({"title": "TitleGone", "year": "2003",
                            "ratings": [{"Source": "Personal", "Value": "8/10"}]})
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
