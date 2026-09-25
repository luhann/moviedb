# moviedb

This is a replacement of my old moviedb lambda REST API. I am self-hosting it
on my proxmox server. It is a single static Rust binary + SQLite, replacing the three Lambdas. 

The binary has `moviedb serve` and `moviedb refresh`. `serve` starts the REST API, `refresh` pulls updated ratings
from [OMDB](https://www.omdbapi.com/) and stores the previous ratings in the `ratings_history` table so that I can
record data on longitudinal ratings changes across all movies I have watched.

Endpoints (every response, success or error, is JSON; movie-doc keys are
snake_case — `imdb_id`, `imdb_rating`, `box_office`, ratings entries
`{"source": ..., "value": ...}`):

```
POST /movies   ?title=<title>&rating=<77>&year=<2026>   fetch OMDB, upsert, snapshot ratings
                 -> 201 + Location: /movies/{imdb_id} (new) or 200 (existing), body is the stored doc
GET  /movies   [?title=<title>] [?year=<year>]          the collection, optionally filtered
GET  /movies/recent  [?limit=<n>]                       most-recently-refreshed movies, newest
                                                          first (default 10, max 50), with
                                                          last_refreshed folded into each doc
GET  /movies/{imdb_id}                                  one movie
GET  /movies/{imdb_id}/history                          ratings snapshots, oldest first
```

A movie's canonical URI is `/movies/{imdb_id}`; title/year lookup is a
filter on the collection (exact, case-sensitive, indexed). A filter matching
several movies returns them all and one matching none returns `[]` — with a
non-unique key, multiple/zero matches are data, not errors. Only
`/movies/{imdb_id}` can 404. Collection responses (`/movies`,
`/movies/recent`) are bare JSON arrays; a response is an object only when it
carries fields beyond the collection itself (`/history`'s `imdb_id`).

Errors are RFC 9457 problem-details objects (`application/problem+json`,
members `type`/`title`/`status`/`detail`) with a standard HTTP status: 400
(malformed path parameter), 401 (bad/missing `x-api-key`), 404 (unknown
`imdb_id` or unmatched path), 405 (wrong method, with `Allow`), 422 (query
params missing/empty/unparseable), 502/503 (OMDB returned something
unrecognized / its daily quota is exhausted or the server is briefly out of
DB capacity — both 503s carry `Retry-After`), 500 (internal error). `type`
is `about:blank` except for the two 503s, which carry
`urn:moviedb:problem:omdb-quota-exhausted` vs
`urn:moviedb:problem:at-capacity` so clients can tell them apart without
parsing `detail`.

## Build

```bash
cargo build --release
# -> target/x86_64-unknown-linux-musl/release/moviedb  (static-pie, ~5.5MB)
python3 tests/smoke_test.py     # full end-to-end check before pushing
```

Note: the build currently requires a **nightly** toolchain
(`cargo-features = ["codegen-backend"]` for the cranelift dev profile, and
`-Z threads` in the target rustflags). Remove those lines if a stable-only
build is needed, no non-dev code depends on nightly features.

### Create the LXC (on the Proxmox host)

This step is optional, you can deploy the binary anywhere you like. This is how I deploy it.

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

The code below installs the single binary, and all systemd services. By default, `moviedb-refresh` will run once a month
to pull updated ratings.

```bash
mkdir -p /opt/moviedb

cp dist/moviedb.env.example /etc/moviedb.env
chmod 600 /etc/moviedb.env
# edit /etc/moviedb.env: set API_KEY (openssl rand -hex 32) and OMDB_KEY

cp dist/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now moviedb moviedb-refresh.timer
systemctl status moviedb
systemd-analyze security moviedb   # exposure score; expect ~1.x
```
## Dashboard

`web/dashboard.html` is a single-file dashboard over the same API, served as a
static file — nothing builds or bundles it. `scripts/deploy.sh` ships it with
the binary: the page and `web/fonts/` go to the web root after the API
restarts, so a failed binary deploy leaves the old page in place. It carries
the
[patroclus](https://github.com/luhann/patroclus) screen theme: the token block
at the top of the file is the one my other sites carry, plus the two values
they have no use for (the meta accent and the shadow), so it can be diffed
against patroclus's `design.yaml`. Dark is the default, light is
opt-in through the toggle in the masthead, and the choice is remembered in this
browser under `theme_pref_v2`. The three faces are self-hosted from `web/fonts/`
rather than pulled from a CDN, so **`fonts/` has to be pushed alongside the
page** or the type silently falls back.

Mine sits behind a Caddy `file_server`, whose root and port the deploy script
has to be told about if they aren't the defaults — set `WEB_ROOT=` and
`WEB_PORT=` alongside `PVE_HOST=`/`VMID=`. The page is pushed under the name
that root serves as its index. After pushing, the script checks the page's
checksum on the container and then asks Caddy for the page and for one of the
fonts, so a font that never landed fails the deploy rather than showing up as
type that has quietly fallen back.

If pushing manually:

```bash
pct exec 401 -- mkdir -p /opt/moviedb/web/fonts   # pct push won't create it
pct push 401 web/dashboard.html /opt/moviedb/web/index.html --perms 0644
for f in web/fonts/*.woff2; do
    pct push 401 "$f" "/opt/moviedb/web/fonts/$(basename "$f")" --perms 0644
done
```

## Routing

Up to you. I use [traefik](https://github.com/traefik/traefik) as my reverse proxy, but any way you request from the API will work.

## Notes (vs. the Lambdas)

- **Key changed deliberately**: `imdb_id` primary key instead of DynamoDB's
  (title, year). Re-POSTing after OMDB corrects a title no longer creates a
  duplicate row. Title/year lookup survives as a `GET /movies` collection
  filter (exact, case-sensitive, indexed).
- **POST returns the stored movie doc as JSON** (201 + `Location` if the
  imdb_id is new, 200 if it already existed), not the Lambda's bare-title
  plain-text body.
- **`year` is required on POST**; the Lambda threw a KeyError (502) if
  omitted, this returns a clean 422 problem-details body (axum's query
  rejection, normalized to this API's error shape).
- **No trailing slashes**: `/movies/` is an unmatched route (a problem-JSON
  404), not a redirect to `/movies`. FastAPI 307-redirected these, which `curl -L`
  followed silently; axum matches exactly. Slashless resource paths are the
  API convention — fix the URL, not the router.
- **Ratings history**: every POST and refresh appends one row per rating
  source to `ratings_history`. Migration seeds one snapshot per movie
  timestamped at migration time — note those values were actually pulled at
  an earlier, unrecorded date, so treat the first snapshot per movie as
  "current as of migration at the latest".
- **Refreshing**: `moviedb refresh [db_path]` re-pulls by imdbid, preserves
  the Personal rating, and is resumable across OMDB's 1000/day limit (oldest-
  refreshed-first ordering + `_refreshed` timestamps, which POST stamps too,
  since it is just as fresh a pull). The Personal rating is re-read inside
  each write, so a re-rate made while a run is in progress survives it.
  Hitting the daily limit stops the run cleanly (exit 0). A timeout or
  garbled response skips that movie, and the run exits non-zero at the end;
  three in a row, a rejected key, or an OMDB error it doesn't recognise
  stop the run at once, also non-zero. `--dry-run` prints rating deltas
  without writing; `--sleep` throttles (default 0.5s).
  `moviedb-refresh.timer` runs it monthly in the same sandbox as the API;
  logs land in `journalctl -u moviedb-refresh`. Trigger manually with
  `systemctl start moviedb-refresh`. **Don't run real refreshes directly as
  root** — SQLite would create WAL/SHM files the dynamic user can't replace.
  For ad-hoc flags, borrow the unit's sandbox:
  `systemd-run --wait --pty -p DynamicUser=yes -p User=moviedb -p StateDirectory=moviedb -p EnvironmentFile=/etc/moviedb.env /opt/moviedb/moviedb refresh --dry-run`.
- **Backups**: `/var/lib/moviedb/movies.db` is the entire dataset. Add the
  LXC (or just that path) to your weekly backup job. With WAL mode enabled,
  use `sqlite3 movies.db ".backup backup.db"` for consistent snapshots rather
  than copying the file while the service is running.
