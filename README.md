# moviedb

moviedb is my personal movie database. It keeps track of every movie I have watched, my own rating for each one, and
the ratings from IMDb, Rotten Tomatoes and Metacritic — including how those ratings change over time.

The original version was three AWS Lambda functions behind API Gateway, storing movies in DynamoDB. This version is a
single static Rust binary backed by SQLite, and it runs in an LXC on my Proxmox cluster. The binary has two subcommands:

- `moviedb serve` runs the REST API.
- `moviedb refresh` re-fetches every movie from the [OMDB API](https://www.omdbapi.com/) and records the new ratings in
  a `ratings_history` table.

A simplified schematic of how it all fits together looks like this:

```
Traefik -> /movies          -> moviedb serve -> SQLite (and the OMDB API when adding a movie)
Traefik -> / and /fonts/    -> Caddy -> web/dashboard.html
systemd timer (monthly)     -> moviedb refresh -> OMDB API -> SQLite
```

## API

Every request needs an `x-api-key` header, and every response (including errors) is JSON.

```
POST /movies   ?title=<title>&rating=<0-100>&year=<2026> fetch from OMDB, store it, snapshot its ratings
GET  /movies   [?title=<title>] [?year=<year>]          all movies, optionally filtered
GET  /movies/recent  [?limit=<n>]                       most recently refreshed first (default 10, max 50)
GET  /movies/{imdb_id}                                  one movie
GET  /movies/{imdb_id}/history                          ratings snapshots, oldest first
```

Movies are keyed by their IMDb ID, so `/movies/{imdb_id}` is the address of a movie. Looking a movie up by title and
year is just a filter on `GET /movies`. The filter is exact and case-sensitive, and because a title and year together
aren't unique it always returns a list — which might hold several movies, or none at all. Only `/movies/{imdb_id}`
returns a 404 for a missing movie.

`POST /movies` returns the stored movie, with a 201 and a `Location` header if the movie is new, or a 200 if it was
already stored. POSTing a movie I have already stored is also how I change my rating for it.

A movie is OMDB's data with the keys converted to snake_case (`imdb_id`, `imdb_rating`, `box_office`), and each entry
in `ratings` looks like `{"source": ..., "value": ...}`. On top of that there are two fields of my own: `personal` is
my rating, a whole number from 0 to 100, and `refreshed` is when the movie was last fetched from OMDB (by either a POST
or a refresh). The list endpoints return a plain JSON array, while `/history` returns an object since it also includes
the `imdb_id`.

The full spec is in `openapi.yaml`.

### Errors

Errors use the [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem format, i.e. `application/problem+json` with
`type`, `title`, `status` and `detail`. `type` is always `about:blank`, so the status code is what tells you what went
wrong:

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

## Building

```bash
cargo build --release
# -> target/x86_64-unknown-linux-musl/release/moviedb  (static-pie, ~5MB)
python3 tests/smoke_test.py     # end-to-end check, run before pushing
```

The build currently needs a **nightly** toolchain. The cranelift dev profile uses `cargo-features = ["codegen-backend"]`,
and the target rustflags include `-Z threads`. None of the release code actually depends on nightly, so if you want a
stable build you can just delete those lines.

The smoke test runs the binary against a stub OMDB, so it checks every endpoint end to end without using up any of the
real OMDB quota.

## Installing

Copy the binary to `/opt/moviedb/moviedb` and the repo's `dist/` directory into the container, then install the
config and the systemd units. By default `moviedb-refresh` runs once a month.

```bash
cp dist/moviedb.env.example /etc/moviedb.env
chmod 600 /etc/moviedb.env
# edit /etc/moviedb.env: set API_KEY (openssl rand -hex 32) and OMDB_KEY

cp dist/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now moviedb moviedb-refresh.timer
systemctl status moviedb
systemd-analyze security moviedb   # exposure score, mine is 1.6
```

After that, I deploy from my workstation with `scripts/deploy.sh`. It builds the binary, runs the smoke test, pushes
the binary into the container and restarts the service.

## Refreshing

`moviedb refresh [db_path]` re-fetches every movie from OMDB by its IMDb ID. It only ever replaces OMDB's data, and
never touches my rating.

OMDB limits you to 1000 requests a day, so a refresh works through the movies that were refreshed longest ago
first. Both a POST and a refresh record when a movie was last fetched, which means a run that gets cut short by the
daily limit simply picks up where it left off next time. Hitting the limit ends the run cleanly (exit 0).

A timeout or a bad response skips that movie, and the run exits non-zero at the end. Three failures in a row, a
rejected API key or an OMDB error it doesn't recognise all stop the run immediately, also with a non-zero exit.
`--dry-run` prints the rating changes without writing anything, and `--sleep` sets the pause between requests
(0.5s by default).

`moviedb-refresh.timer` runs the refresh monthly in the same sandbox as the API, and the logs go to
`journalctl -u moviedb-refresh`. To run it now, use `systemctl start moviedb-refresh`.

**Don't run a real refresh directly as root.** SQLite would create the WAL and SHM files owned by root, and the
service's dynamic user can't replace them. If you need extra flags, run it inside the unit's sandbox instead:

```bash
systemd-run --wait --pty -p DynamicUser=yes -p User=moviedb -p StateDirectory=moviedb \
  -p EnvironmentFile=/etc/moviedb.env /opt/moviedb/moviedb refresh --dry-run
```

## Backups

`/var/lib/moviedb/movies.db` is the whole dataset, so back up either the container or just that file. The database is
in WAL mode, which means copying the file while the service is running might not give you a consistent copy. Use
`sqlite3 movies.db ".backup backup.db"` instead.

## Dashboard

`web/dashboard.html` is a single-file dashboard for the API. It's just a static file, with no build step.

It uses my [patroclus](https://github.com/luhann/patroclus) theme. The token block at the top of the file is the same
one my other sites use, plus two values they don't need (the meta accent and the shadow), so you can diff it against
patroclus's `design.yaml`. Dark mode is the default. The toggle in the header switches to light mode, and the choice is
remembered in `localStorage` under `theme_pref_v2`.

I serve it with Caddy's `file_server`, and the page is installed as `index.html`. The fonts are self-hosted rather
than loaded from a CDN, so **`fonts/` has to be deployed alongside the page**. Without it the page still loads, but
falls back to system fonts.

`scripts/deploy.sh` ships the page and `web/fonts/` once the API has restarted, so if the binary deploy fails the old
page stays up. `scripts/deploy.sh --web-only` ships just the page and fonts, without touching the API. Unlike the
binary, the page is copied straight to the container over ssh (`WEB_HOST=`, `root@omdb.trusted` by default) rather than
through the Proxmox host. If your Caddy root and port differ from mine, set `WEB_ROOT=` and `WEB_PORT=`.

After copying, the script checks the page's checksum on the container. It then requests the page and every font from
Caddy directly, and again through `PUBLIC_URL=` (`https://omdb.luhann.com` by default, or set it empty to skip). If a
font didn't make it, the deploy fails.

## Routing

This is up to you. I use [Traefik](https://github.com/traefik/traefik) as a reverse proxy, but anything that can reach
the API will work.

## Upgrading to 3.0

3.0 changed the database schema, and only the `v3.0.0` tag can migrate a 2.x database. To upgrade, run that tag's
`moviedb serve` against the database once, then move to the latest version. Later versions don't include the
migration. Once migrated, a 2.x binary can no longer use the database, so take a backup first (see
[Backups](#backups)).

The API changes in 3.0 were:

- My rating is now a top-level `personal` number, rather than a `Personal` entry in `ratings`. `ratings` now only
  holds OMDB's ratings, and `rating=` on POST must be a whole number from 0 to 100.
- `_refreshed` is now `refreshed`. `/movies/recent` sorts by it, and no longer adds a `last_refreshed` field (it always
  had the same value anyway).
- A refresh no longer repeats my rating in the history, so `Personal` snapshots now record when I actually rated a
  movie.
- The 503 for running out of database capacity is gone. A request now waits for the database instead.
- Every error's `type` is `about:blank`. The quota 503 used to have its own URN.

## Changes From the Lambda Version

- Movies are keyed by `imdb_id` instead of DynamoDB's (title, year). If OMDB corrects a title, POSTing the movie again
  no longer creates a duplicate. Looking a movie up by title and year still works as a filter on `GET /movies`.
- POST returns the stored movie as JSON (201 with `Location` if it's new, 200 if it already existed). The Lambda
  returned the bare title as plain text.
- `year` is required on POST. Without it the Lambda threw a KeyError (502); this returns a 422.
- There are no trailing slashes. `/movies/` is a 404, not a redirect to `/movies`. The FastAPI version returned a 307,
  which `curl -L` happily followed without complaint.
- There is now a ratings history. Every POST adds a row to `ratings_history` for my rating and each of OMDB's, and
  every refresh adds one for each of OMDB's. When I first moved off the Lambdas, the migration seeded one snapshot per
  movie, stamped with the migration time. Of course, those ratings were actually fetched at some earlier (unrecorded)
  date, so the first snapshot for each of those movies should be read as "correct as of the migration, at the latest".
