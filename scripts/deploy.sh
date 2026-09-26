#!/usr/bin/env bash
# Deploy moviedb from the workstation. Builds and smoke-tests the binary,
# pushes it into the LXC through the Proxmox host, restarts the service and
# checks it came up. Then copies the dashboard and its fonts to Caddy's web
# root.
#
# The binary goes through the Proxmox host because pct works even when the
# container's network or sshd doesn't, which matters when recovering from a
# bad deploy. The dashboard is just files, so it goes straight to the
# container over ssh.
#
# Usage: scripts/deploy.sh [--web-only]
#   --web-only  only ship the dashboard and fonts (no build, no smoke test,
#               no API restart)
# Override the targets with PVE_HOST, VMID, WEB_HOST, WEB_ROOT, WEB_PORT or
# PUBLIC_URL in the environment.
set -euo pipefail

WEB_ONLY=0
case "${1:-}" in
    "") ;;
    --web-only) WEB_ONLY=1 ;;
    *)
        echo "usage: $0 [--web-only]" >&2
        exit 2
        ;;
esac

# pct only sees containers on its own node, so PVE_HOST has to be the node
# the container is on. Update both if it's migrated.
PVE_HOST="${PVE_HOST:-root@fragment2.trusted}"
VMID="${VMID:-401}"
BIN="target/x86_64-unknown-linux-musl/release/moviedb"
# Caddy's root and port for the dashboard. These have to match
# omdb/Caddyfile in the homelab repo by hand.
WEB_HOST="${WEB_HOST:-root@omdb.trusted}"
WEB_ROOT="${WEB_ROOT:-/opt/moviedb/web}"
WEB_PORT="${WEB_PORT:-8080}"
# The public address of the dashboard, checked last. Set it empty to skip
# the check when deploying from somewhere that can't resolve it.
PUBLIC_URL="${PUBLIC_URL-https://omdb.luhann.com}"

# These end up unquoted in remote shell commands, so reject anything that
# isn't a plain number or path.
[[ "$VMID" =~ ^[0-9]+$ ]] || {
    echo "ABORT: VMID must be numeric, got: $VMID" >&2
    exit 1
}
[[ "$WEB_ROOT" =~ ^/[A-Za-z0-9._/-]+$ ]] || {
    echo "ABORT: WEB_ROOT must be a plain absolute path, got: $WEB_ROOT" >&2
    exit 1
}
[[ "$WEB_PORT" =~ ^[0-9]+$ ]] || {
    echo "ABORT: WEB_PORT must be numeric, got: $WEB_PORT" >&2
    exit 1
}

cd "$(dirname "$0")/.."

# RUSTFLAGS would replace all the rustflags in .cargo/config.toml, including
# the pinned target-cpu. The workstation sets "-C target-cpu=native", which
# builds a Zen 3 binary that crashes on the Intel nodes. Unsetting it here
# doesn't affect normal builds.
unset RUSTFLAGS

if (( ! WEB_ONLY )); then
    cargo build --release --locked
    file "$BIN" | grep -q 'static-pie linked' || {
        echo "ABORT: $BIN is not static-pie linked — wrong toolchain/config?" >&2
        exit 1
    }

    python3 tests/smoke_test.py
fi

# The page loads its three fonts from web/fonts/. Without them it falls back
# to Georgia and Arial, which is easy to miss, so refuse to deploy without
# all three.
shopt -s nullglob
FONTS=(web/fonts/*.woff2)
shopt -u nullglob
(( ${#FONTS[@]} == 3 )) || {
    echo "ABORT: expected 3 woff2 files in web/fonts/, found ${#FONTS[@]}" >&2
    exit 1
}

# Open one shared connection per host so the password is only asked for
# once.
CTL="$HOME/.ssh/deploy-moviedb-%r@%h"
HOSTS=("$WEB_HOST")
(( WEB_ONLY )) || HOSTS=("$PVE_HOST" "${HOSTS[@]}")
trap 'for h in "${HOSTS[@]}"; do ssh -o ControlPath="$CTL" -O exit "$h" 2>/dev/null || true; done' EXIT
for h in "${HOSTS[@]}"; do
    ssh -o ControlMaster=yes -o ControlPath="$CTL" -o ControlPersist=60 -fN "$h"
done

if (( ! WEB_ONLY )); then
    scp -o ControlPath="$CTL" "$BIN" "$PVE_HOST:/tmp/moviedb.deploy"
    ssh -o ControlPath="$CTL" "$PVE_HOST" "
        set -e
        # Push to a temp name and rename, because overwriting the running
        # binary fails with ETXTBSY. --perms is needed because pct push
        # defaults to 0644, which systemd can't execute (203/EXEC).
        pct push $VMID /tmp/moviedb.deploy /opt/moviedb/moviedb.new --perms 0755
        rm /tmp/moviedb.deploy
        pct exec $VMID -- mv /opt/moviedb/moviedb.new /opt/moviedb/moviedb
        pct exec $VMID -- systemctl restart moviedb
        pct exec $VMID -- systemctl is-active moviedb
        pct exec $VMID -- /opt/moviedb/moviedb --version
    "
fi

# Caddy serves the new files on the next request, so nothing needs
# restarting. This runs after the API deploy so a failure there leaves the
# old page in place.
WEB_SHA="$(sha256sum web/dashboard.html | cut -d' ' -f1)"
# Check the page and every font.
PROBE_PATHS="/"
for f in "${FONTS[@]}"; do
    PROBE_PATHS+=" /fonts/$(basename "$f")"
done

ssh -o ControlPath="$CTL" "$WEB_HOST" "mkdir -p /tmp/moviedb-web.deploy/fonts $WEB_ROOT/fonts"
scp -o ControlPath="$CTL" web/dashboard.html "$WEB_HOST:/tmp/moviedb-web.deploy/index.html"
scp -o ControlPath="$CTL" "${FONTS[@]}" "$WEB_HOST:/tmp/moviedb-web.deploy/fonts/"

ssh -o ControlPath="$CTL" "$WEB_HOST" "
    set -e
    # Install to a temp file in the same directory and rename it into
    # place, so Caddy never serves a half-written file. The files need to be
    # world-readable because Caddy runs as its own user.
    install -m 0644 /tmp/moviedb-web.deploy/index.html $WEB_ROOT/.index.html.new
    mv $WEB_ROOT/.index.html.new $WEB_ROOT/index.html

    for f in /tmp/moviedb-web.deploy/fonts/*.woff2; do
        name=\"\$(basename \"\$f\")\"
        install -m 0644 \"\$f\" \"$WEB_ROOT/fonts/.\$name.new\"
        mv \"$WEB_ROOT/fonts/.\$name.new\" \"$WEB_ROOT/fonts/\$name\"
    done
    rm -rf /tmp/moviedb-web.deploy

    # Check the page arrived intact, then that Caddy serves it and every
    # font. The checksum alone would pass even if Caddy couldn't read the
    # file. PROBE_PATHS is split on spaces on purpose; font names have none.
    #
    # If WEB_ROOT doesn't match the Caddyfile's root, these checks still
    # pass on the old page Caddy is serving, so keep the two in sync.
    got=\$(sha256sum $WEB_ROOT/index.html | cut -d' ' -f1)
    [ \"\$got\" = \"$WEB_SHA\" ] || {
        echo \"ABORT: dashboard checksum mismatch on the container\" >&2
        exit 1
    }
    for url in $PROBE_PATHS; do
        code=\$(curl -fsS -o /dev/null -w '%{http_code}' \"http://127.0.0.1:$WEB_PORT\$url\") || {
            echo \"ABORT: Caddy did not serve \$url\" >&2
            exit 1
        }
        echo \"served \$url -> \$code\"
    done
"

# The checks above go to Caddy directly, so they can't catch a reverse proxy
# problem. If the proxy routes `/` but not `/fonts/`, the page still loads,
# just in the fallback fonts. Check through the public URL too.
if [[ -n "$PUBLIC_URL" ]]; then
    for path in $PROBE_PATHS; do
        code="$(curl -fsS -o /dev/null -w '%{http_code}' "$PUBLIC_URL$path")" || {
            echo "ABORT: $PUBLIC_URL$path did not load — if the page is fine and" >&2
            echo "       only the font path fails, the reverse proxy is routing" >&2
            echo "       / but not /fonts/." >&2
            exit 1
        }
        echo "public $PUBLIC_URL$path -> $code"
    done
fi

(( WEB_ONLY )) || echo "deployed $("$BIN" --version) to LXC $VMID via $PVE_HOST"
echo "deployed dashboard + ${#FONTS[@]} fonts to $WEB_HOST:$WEB_ROOT"
