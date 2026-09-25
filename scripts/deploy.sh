#!/usr/bin/env bash
# Deploy moviedb from the workstation: build, smoke-test, copy to the Proxmox
# host, push into the LXC, restart the service, verify — then copy the static
# dashboard and its fonts into the web root Caddy serves.
#
# The binary goes in through the Proxmox host because pct still works when the
# guest's own networking or sshd does not, which is worth having for the thing
# that has to come back after a bad deploy. The dashboard is just files: it
# goes straight to the container over ssh, and so doesn't care which node the
# guest happens to be on.
#
# Usage: scripts/deploy.sh
# Override the target with PVE_HOST=..., VMID=..., WEB_HOST=..., WEB_ROOT=...
# or WEB_PORT=... in the environment.
set -euo pipefail

# pct is node-local: PVE_HOST must be whichever cluster node currently hosts
# the container, so both defaults move together if omdb is ever migrated.
PVE_HOST="${PVE_HOST:-root@fragment2.trusted}"
VMID="${VMID:-401}"
BIN="target/x86_64-unknown-linux-musl/release/moviedb"
# Caddy's `root * ...` and listen address for the dashboard vhost. The
# Caddyfile itself is stock and untracked, so these two are what have to be
# kept in step with it by hand.
WEB_HOST="${WEB_HOST:-root@omdb.trusted}"
WEB_ROOT="${WEB_ROOT:-/opt/moviedb/web}"
WEB_PORT="${WEB_PORT:-8080}"
# Where the dashboard is actually read from. Checked last, because everything
# before it can pass on a page nobody can load correctly. Set empty to skip
# when deploying from somewhere this name doesn't resolve.
PUBLIC_URL="${PUBLIC_URL:-https://omdb.luhann.com}"

# VMID is interpolated into the remote ssh command below unquoted; reject
# anything that isn't a plain integer before it gets anywhere near a shell.
[[ "$VMID" =~ ^[0-9]+$ ]] || {
    echo "ABORT: VMID must be numeric, got: $VMID" >&2
    exit 1
}

# Same reasoning for WEB_ROOT, which is also interpolated into remote shell:
# an absolute path of ordinary path characters, nothing a shell would expand.
[[ "$WEB_ROOT" =~ ^/[A-Za-z0-9._/-]+$ ]] || {
    echo "ABORT: WEB_ROOT must be a plain absolute path, got: $WEB_ROOT" >&2
    exit 1
}
[[ "$WEB_PORT" =~ ^[0-9]+$ ]] || {
    echo "ABORT: WEB_PORT must be numeric, got: $WEB_PORT" >&2
    exit 1
}

cd "$(dirname "$0")/.."

# A RUSTFLAGS in the environment replaces .cargo/config.toml's rustflags
# wholesale — including the pinned target-cpu — and this workstation's Gentoo
# profile exports "-C target-cpu=native". Building the deploy artifact under
# that gives the Coffee Lake LXC host a Zen 3 binary that SIGILLs on the first
# AMD-only instruction. Config wins here; interactive builds are unaffected.
unset RUSTFLAGS

cargo build --release --locked
file "$BIN" | grep -q 'static-pie linked' || {
    echo "ABORT: $BIN is not static-pie linked — wrong toolchain/config?" >&2
    exit 1
}

python3 tests/smoke_test.py

# The page loads its three faces from web/fonts/ by relative URL. Deploying
# the page without them leaves the type silently falling back to Georgia and
# Arial, which looks like nothing is wrong — so refuse to ship a half set.
shopt -s nullglob
FONTS=(web/fonts/*.woff2)
shopt -u nullglob
(( ${#FONTS[@]} == 3 )) || {
    echo "ABORT: expected 3 woff2 files in web/fonts/, found ${#FONTS[@]}" >&2
    exit 1
}

# One master connection so password auth prompts exactly once; the scp and
# ssh below multiplex over it.
CTL="$HOME/.ssh/deploy-moviedb-%r@%h"
trap 'ssh -o ControlPath="$CTL" -O exit "$PVE_HOST" 2>/dev/null || true;
      ssh -o ControlPath="$CTL" -O exit "$WEB_HOST" 2>/dev/null || true' EXIT
ssh -o ControlMaster=yes -o ControlPath="$CTL" -o ControlPersist=60 -fN "$PVE_HOST"
ssh -o ControlMaster=yes -o ControlPath="$CTL" -o ControlPersist=60 -fN "$WEB_HOST"

scp -o ControlPath="$CTL" "$BIN" "$PVE_HOST:/tmp/moviedb.deploy"
ssh -o ControlPath="$CTL" "$PVE_HOST" "
    set -e
    # push under a temp name, then rename: writing over the running binary
    # would fail with ETXTBSY. --perms is load-bearing (pct push defaults to
    # 0644 root:root on every push -> systemd 203/EXEC).
    pct push $VMID /tmp/moviedb.deploy /opt/moviedb/moviedb.new --perms 0755
    rm /tmp/moviedb.deploy
    pct exec $VMID -- mv /opt/moviedb/moviedb.new /opt/moviedb/moviedb
    pct exec $VMID -- systemctl restart moviedb
    pct exec $VMID -- systemctl is-active moviedb
    pct exec $VMID -- /opt/moviedb/moviedb --version
"

# The dashboard is static: Caddy picks up a new file on the next request, so
# there is nothing to restart. It goes out after the API rather than before,
# so a failed binary deploy aborts with the old page still in place.
WEB_SHA="$(sha256sum web/dashboard.html | cut -d' ' -f1)"
FONT_PROBE="$(basename "${FONTS[0]}")"

ssh -o ControlPath="$CTL" "$WEB_HOST" "mkdir -p /tmp/moviedb-web.deploy/fonts $WEB_ROOT/fonts"
scp -o ControlPath="$CTL" web/dashboard.html "$WEB_HOST:/tmp/moviedb-web.deploy/index.html"
scp -o ControlPath="$CTL" "${FONTS[@]}" "$WEB_HOST:/tmp/moviedb-web.deploy/fonts/"

ssh -o ControlPath="$CTL" "$WEB_HOST" "
    set -e
    # install(1) writes the temp copy inside the destination directory, so it
    # is already on the target filesystem and the mv is a rename(2) — atomic.
    # Caddy serves whatever is at the path when a request arrives, so a plain
    # copy over the live file could hand someone a truncated page. -m is
    # explicit rather than inherited: Caddy runs as its own user and reads
    # these through 'other'.
    install -m 0644 /tmp/moviedb-web.deploy/index.html $WEB_ROOT/.index.html.new
    mv $WEB_ROOT/.index.html.new $WEB_ROOT/index.html

    for f in /tmp/moviedb-web.deploy/fonts/*.woff2; do
        name=\"\$(basename \"\$f\")\"
        install -m 0644 \"\$f\" \"$WEB_ROOT/fonts/.\$name.new\"
        mv \"$WEB_ROOT/fonts/.\$name.new\" \"$WEB_ROOT/fonts/\$name\"
    done
    rm -rf /tmp/moviedb-web.deploy

    # Verify the bytes landed, then that Caddy actually serves them — the
    # checksum alone would still pass if the file were unreadable to Caddy's
    # user, and a 200 on the page alone would not notice a missing font.
    #
    # The probe asks Caddy, not the filesystem, so it only proves *this*
    # deploy if WEB_ROOT is the root the Caddyfile names. Point WEB_ROOT
    # somewhere else and the page probe still passes, on the page already
    # being served from the real root.
    got=\$(sha256sum $WEB_ROOT/index.html | cut -d' ' -f1)
    [ \"\$got\" = \"$WEB_SHA\" ] || {
        echo \"ABORT: dashboard checksum mismatch on the container\" >&2
        exit 1
    }
    for url in / '/fonts/$FONT_PROBE'; do
        code=\$(curl -fsS -o /dev/null -w '%{http_code}' \"http://127.0.0.1:$WEB_PORT\$url\") || {
            echo \"ABORT: Caddy did not serve \$url\" >&2
            exit 1
        }
        echo \"served \$url -> \$code\"
    done
"

# The probes above talk to Caddy directly, which is the one vantage point
# that cannot see a reverse-proxy problem. The fonts are requested by relative
# URL, so they only arrive if the proxy routes their path as well as `/` — and
# when it doesn't, the page still returns 200 and quietly renders in the
# fallback stack. That is what this checks, from where a reader sits.
if [[ -n "$PUBLIC_URL" ]]; then
    for path in "/" "/fonts/$FONT_PROBE"; do
        code="$(curl -fsS -o /dev/null -w '%{http_code}' "$PUBLIC_URL$path")" || {
            echo "ABORT: $PUBLIC_URL$path did not load — if the page is fine and" >&2
            echo "       only the font path fails, the reverse proxy is routing" >&2
            echo "       / but not /fonts/." >&2
            exit 1
        }
        echo "public $PUBLIC_URL$path -> $code"
    done
fi

echo "deployed $("$BIN" --version) to LXC $VMID via $PVE_HOST"
echo "deployed dashboard + ${#FONTS[@]} fonts to $WEB_HOST:$WEB_ROOT"
