# moviedb — self-hosted Lambda/DynamoDB replacement

This is a replacement of my old moviedb lambda REST API. I am self-hosting it
on my proxmox server. It is a single static Rust binary + SQLite, replacing the three Lambdas. 

The binary has `moviedb serve` and `moviedb refresh`. `serve` starts the REST API, `refresh` pulls updated ratings
from [OMDB](https://www.omdbapi.com/) and stores the previous ratings in the `ratings_history` table so that I can
record data on longitudinal ratings changes across all movies I have watched.

Endpoints:

```
POST /         ?title=<title>&rating=<77>&year=<2026>        fetch OMDB, upsert, snapshot ratings
GET  /                                      all movies
GET  /single   ?imdbid=  OR  ?title=&year=  one movie
GET  /history  ?imdbid=  OR  ?title=&year=  ratings snapshots, oldest first
```

## Build

```bash
cargo build --release
# -> target/x86_64-unknown-linux-musl/release/moviedb  (static-pie, ~5MB)
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

# copy the binary + migrate_dynamo.py in (pct push from the host, or scp)

cp dist/moviedb.env.example /etc/moviedb.env
chmod 600 /etc/moviedb.env
# edit /etc/moviedb.env: set API_KEY (openssl rand -hex 32) and OMDB_KEY

cp dist/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now moviedb moviedb-refresh.timer
systemctl status moviedb
systemd-analyze security moviedb   # exposure score; expect ~1.x
```

Upgrades are the same push + `systemctl restart moviedb`. `scripts/deploy.sh`
does the whole upgrade from the workstation (build, smoke test, push via the
Proxmox host, restart, verify); set `PVE_HOST`/`VMID` if the defaults
(`root@pve.lan`, 210) don't match. If pushing manually:

```bash
# --perms matters: pct push defaults to 0644 root:root on EVERY push,
# so an upgrade push without it strips the exec bit -> systemd 203/EXEC
pct push 210 target/x86_64-unknown-linux-musl/release/moviedb /opt/moviedb/moviedb --perms 0755
```

## Migrate the DynamoDB data

This step is also optional, it is included because previously this REST API pushed data to AWS DynamoDB, if you are starting
from scratch this is not necessary.

On any machine with AWS credentials:

```bash
aws dynamodb scan --table-name movies --output json > movies.json
```

If the output contains `LastEvaluatedKey`, the scan paginated — at 1MB per
page you're fine unless you've rated thousands of movies, but the migration
script warns if the export is incomplete.

Copy `movies.json` into the LXC, then:

```bash
systemctl stop moviedb
python3 /opt/moviedb/migrate_dynamo.py movies.json /var/lib/moviedb/movies.db
systemctl start moviedb
```

(The migration script is stdlib-only Python.)

The script prints export count vs. table row count, make sure that they match.

## Routing

Up to you. I use [traefik](https://github.com/traefik/traefik) as my reverse proxy, but any way you request from the API will work.


## Notes (vs. the Lambdas)

- **Key changed deliberately**: `imdb_id` primary key instead of DynamoDB's
  (title, year). Re-POSTing after OMDB corrects a title no longer creates a
  duplicate row. `/single` still accepts `title` + `year` (exact,
  case-sensitive, indexed) so existing curls work; `imdbid` also accepted.
- **POST returns the bare title as plain text**, matching the Lambda body.
- **`year` is required on POST**; the Lambda threw a KeyError (502) if
  omitted, this returns a clean 400 (axum's query rejection; the FastAPI
  version returned 422 — accepted drift, nothing checks these codes).
- **No trailing slashes**: `/single/` is an unmatched route (empty 404), not
  a redirect to `/single`. FastAPI 307-redirected these, which `curl -L`
  followed silently; axum matches exactly. Slashless resource paths are the
  API convention — fix the URL, not the router.
- **Ratings history**: every POST and refresh appends one row per rating
  source to `ratings_history`. Migration seeds one snapshot per movie
  timestamped at migration time — note those values were actually pulled at
  an earlier, unrecorded date, so treat the first snapshot per movie as
  "current as of migration at the latest".
- **Refreshing**: `moviedb refresh [db_path]` re-pulls by imdbid, preserves
  the Personal rating, and is resumable across OMDB's 1000/day limit (oldest-
  refreshed-first ordering + `_refreshed` timestamps). `--dry-run` prints
  rating deltas without writing; `--sleep` throttles (default 0.5s).
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
