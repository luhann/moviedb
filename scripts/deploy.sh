#!/usr/bin/env bash
# Deploy the moviedb binary from the workstation: build, smoke-test, copy to
# the Proxmox host, push into the LXC, restart the service, verify.
#
# Usage: scripts/deploy.sh
# Override the target with PVE_HOST=... or VMID=... in the environment.
set -euo pipefail

PVE_HOST="${PVE_HOST:-root@pve.lan}"
VMID="${VMID:-210}"
BIN="target/x86_64-unknown-linux-musl/release/moviedb"

cd "$(dirname "$0")/.."

cargo build --release
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
