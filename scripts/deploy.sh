#!/usr/bin/env bash
# Deploy the moviedb binary from the workstation: build, smoke-test, copy to
# the Proxmox host, push into the LXC, restart the service, verify.
#
# Usage: scripts/deploy.sh
# Override the target with PVE_HOST=... or VMID=... in the environment.
set -euo pipefail

# pct is node-local: PVE_HOST must be whichever cluster node currently hosts
# the container, so both defaults move together if omdb is ever migrated.
PVE_HOST="${PVE_HOST:-root@fragment2.trusted}"
VMID="${VMID:-401}"
BIN="target/x86_64-unknown-linux-musl/release/moviedb"

# VMID is interpolated into the remote ssh command below unquoted; reject
# anything that isn't a plain integer before it gets anywhere near a shell.
[[ "$VMID" =~ ^[0-9]+$ ]] || {
    echo "ABORT: VMID must be numeric, got: $VMID" >&2
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

# One master connection so password auth prompts exactly once; the scp and
# ssh below multiplex over it.
CTL="$HOME/.ssh/deploy-moviedb-%r@%h"
trap 'ssh -o ControlPath="$CTL" -O exit "$PVE_HOST" 2>/dev/null || true' EXIT
ssh -o ControlMaster=yes -o ControlPath="$CTL" -o ControlPersist=60 -fN "$PVE_HOST"

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

echo "deployed $("$BIN" --version) to LXC $VMID via $PVE_HOST"
