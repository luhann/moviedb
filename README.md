# moviedb

This replaces my old moviedb REST API, which ran as three AWS Lambdas. It's
now a single static Rust binary backed by SQLite, self-hosted on my Proxmox
cluster.

The binary has two subcommands:

- `moviedb serve` runs the REST API.
- `moviedb refresh` pulls updated ratings from [OMDB](https://www.omdbapi.com/)
  and records the old ones in a `ratings_history` table, so I can track how
  ratings change over time for every movie I've watched.

## API

Every request needs an `x-api-key` header. Every response, including errors,
is JSON.

```
POST /movies   ?title=<title>&rating=<0-100>&year=<2026> fetch from OMDB, store it, snapshot its ratings
GET  /movies   [?title=<title>] [?year=<year>]          all movies, optionally filtered
GET  /movies/recent  [?limit=<n>]                       most recently refreshed first (default 10, max 50)
GET  /movies/{imdb_id}                                  one movie
GET  /movies/{imdb_id}/history                          ratings snapshots, oldest first
```

Movies are keyed by IMDb ID, so `/movies/{imdb_id}` is a movie's address.
Looking a movie up by title and year is a filter on `GET /movies`: it's exact
and case-sensitive, and it returns a list. Title and year together aren't
unique, so a filter can match several movies or none, and you get back an
array either way (possibly empty). Only `/movies/{imdb_id}` returns 404 for a
missing movie.

`POST /movies` returns the stored movie. The status is 201 with a `Location`
header if the movie is new, and 200 if it was already stored.

A movie is OMDB's data with snake_case keys (`imdb_id`, `imdb_rating`,
`box_office`), where each entry in `ratings` looks like
`{"source": ..., "value": ...}`. Two fields are ours: `personal` is your
score, a whole number from 0 to 100, and `refreshed` is when the movie was
last fetched from OMDB, by POST or by a refresh. The list endpoints return a
plain JSON array. `/history` returns an object because it also includes the
`imdb_id`.

### Errors

Errors use the [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem
format (`application/problem+json` with `type`, `title`, `status` and
`detail`).

| Status | Meaning |
|---|---|
| 400 | Malformed path parameter |
| 401 | Missing or wrong `x-api-key` |
| 404 | Unknown `imdb_id`, or a path the API doesn't serve |
| 405 | Wrong method (the response includes `Allow`) |
| 422 | A query parameter is missing, empty or can't be parsed |
| 502 | OMDB couldn't be reached, sent something that isn't JSON, returned an error the server doesn't recognise, or sent a movie with no ID or title |
| 503 | OMDB's daily quota is used up (the response includes `Retry-After`) |
| 504 | OMDB didn't respond within 10 seconds |
| 500 | Internal error |

`type` is always `about:blank`; the status code says what went wrong.

The full spec is in `openapi.yaml`.

## Build

```bash
cargo build --release
# -> target/x86_64-unknown-linux-musl/release/moviedb  (static-pie, ~5.5MB)
python3 tests/smoke_test.py     # end-to-end check, run before pushing
```

The build needs a **nightly** toolchain for now. The cranelift dev profile
uses `cargo-features = ["codegen-backend"]`, and the target rustflags include
`-Z threads`. None of the release code depends on nightly, so delete those
lines if you want a stable build.

### Create the LXC (on the Proxmox host)

This step is optional. You can run the binary anywhere; this is just how I
deploy it.

```bash
pveam update
pveam download local debian-12-standard_12.7-1_amd64.tar.zst

pct create 210 local:vztmpl/debian-12-standard_12.7-1_amd64.tar.zst \
  --hostname moviedb \
  --cores 1 --memory 512 --swap 0 \
  --rootfs local-lvm:4 \
  --net0 name=eth0,bridge=vmbr0,ip=dhcp \
  --unprivileged 1 --features nesting=1 \
  --onboot 1

pct start 210
pct enter 210
```

## Install

This installs the binary and the systemd units. By default `moviedb-refresh`
runs once a month.

```bash
mkdir -p /opt/moviedb

cp dist/moviedb.env.example /etc/moviedb.env
chmod 600 /etc/moviedb.env
# edit /etc/moviedb.env: set API_KEY (openssl rand -hex 32) and OMDB_KEY

cp dist/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now moviedb moviedb-refresh.timer
systemctl status moviedb
systemd-analyze security moviedb   # exposure score, expect ~1.x
```

For later deploys from a workstation, `scripts/deploy.sh` builds, runs the
smoke test, pushes the binary into the container and restarts the service.

## Dashboard

`web/dashboard.html` is a single-file dashboard for the API. It's a static
file with no build step.

`scripts/deploy.sh` ships the page and `web/fonts/` after the API has
restarted, so if the binary deploy fails the old page stays up.
`scripts/deploy.sh --web-only` ships just the page and fonts without touching
the API.

The fonts are self-hosted rather than loaded from a CDN, so **`fonts/` has to
be deployed alongside the page**. Without it the page still loads, but falls
back to system fonts.

It uses the [patroclus](https://github.com/luhann/patroclus) theme. The token
block at the top of the file is the same one my other sites use, plus two
values they don't need (the meta accent and the shadow), so you can diff it
against patroclus's `design.yaml`. Dark mode is the default. Light mode is
switched on with the toggle in the header and remembered in `localStorage`
under `theme_pref_v2`.

I serve it with Caddy's `file_server`. The page is copied straight to that
container over ssh (`WEB_HOST=`, default `root@omdb.trusted`) instead of
through the Proxmox host like the binary. Set `WEB_ROOT=` and `WEB_PORT=` if
your Caddy root and port differ from the defaults. The page is installed as
`index.html`.

After copying, the script checks the page's checksum on the container, then
requests the page and every font from Caddy directly and again through
`PUBLIC_URL=` (default `https://omdb.luhann.com`; set it empty to skip). A
font that didn't make it fails the deploy.

To push by hand instead:

```bash
pct exec 401 -- mkdir -p /opt/moviedb/web/fonts   # pct push won't create it
pct push 401 web/dashboard.html /opt/moviedb/web/index.html --perms 0644
for f in web/fonts/*.woff2; do
    pct push 401 "$f" "/opt/moviedb/web/fonts/$(basename "$f")" --perms 0644
done
```

## Routing

Up to you. I use [traefik](https://github.com/traefik/traefik) as a reverse
proxy, but anything that can reach the API works.

## Upgrading to 3.0

3.0 changes the database schema, and `moviedb serve` migrates it the first
time it starts. If any stored Personal rating isn't a whole number from 0 to
100, the migration changes nothing, lists those movies and the server
doesn't start. After a migration, a 2.x binary can no longer use the
database, so take a backup first (see Backups below) if you might want to
roll back. The API changes are:

- Your rating is a top-level `personal` number instead of a `Personal` entry
  in `ratings`, which now holds only OMDB's ratings. `rating=` on POST must
  be a whole number from 0 to 100.
- `_refreshed` is now `refreshed`. `/movies/recent` sorts by it and no
  longer adds a `last_refreshed` field (it always had the same value).
- A refresh no longer repeats your rating in the history, so `Personal`
  snapshots record when you rated.
- The 503 for running out of database capacity is gone. A request waits for
  the database instead.
- Every error's `type` is `about:blank`. The quota 503 used to have its own
  URN.

## Changes from the Lambda version

- **Movies are keyed by `imdb_id`** instead of DynamoDB's (title, year). If
  OMDB corrects a title, POSTing it again no longer creates a duplicate.
  Title/year lookup still works as a filter on `GET /movies`.
- **POST returns the stored movie as JSON** (201 with `Location` if new, 200
  if it already existed). The Lambda returned the bare title as plain text.
- **`year` is required on POST.** The Lambda threw a KeyError (502) without
  it; this returns a 422.
- **No trailing slashes.** `/movies/` is a 404, not a redirect to `/movies`.
  The FastAPI version returned a 307, which `curl -L` followed without
  complaint. Fix the URL rather than the router.
- **Ratings history.** Every POST adds a row to `ratings_history` for your
  rating and each of OMDB's, and every refresh adds one for each of OMDB's. The migration seeded one snapshot per movie,
  stamped with the migration time, but those ratings were fetched at some
  earlier, unrecorded date. Read the first snapshot for each movie as
  "correct as of the migration at the latest".
- **Refreshing.** `moviedb refresh [db_path]` re-fetches every movie by IMDb
  ID. It only replaces OMDB's data, never your rating.
  - It works through the movies that were refreshed longest ago first, and
    both POST and refresh record when each movie was last fetched. That means
    a run cut short by OMDB's 1000-requests-a-day limit picks up where it left
    off next time.
  - Hitting the daily limit ends the run cleanly (exit 0).
  - A timeout or bad response skips that movie, and the run exits non-zero at
    the end. Three failures in a row, a rejected API key or an unknown OMDB
    error stop the run immediately, also non-zero.
  - `--dry-run` prints the rating changes without writing anything. `--sleep`
    sets the pause between requests (default 0.5s).

  `moviedb-refresh.timer` runs it monthly under the same sandbox as the API,
  and logs go to `journalctl -u moviedb-refresh`. To run it now:
  `systemctl start moviedb-refresh`.

  **Don't run a real refresh directly as root.** SQLite would create WAL/SHM
  files owned by root, which the service's dynamic user can't replace. If you
  need extra flags, run it inside the unit's sandbox:
  `systemd-run --wait --pty -p DynamicUser=yes -p User=moviedb -p StateDirectory=moviedb -p EnvironmentFile=/etc/moviedb.env /opt/moviedb/moviedb refresh --dry-run`.
- **Backups.** `/var/lib/moviedb/movies.db` is the whole dataset, so back up
  the container or just that file. The database is in WAL mode, so use
  `sqlite3 movies.db ".backup backup.db"` to get a consistent copy rather
  than copying the file while the service is running.
