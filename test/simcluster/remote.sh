#!/usr/bin/env bash
# remote.sh — WSL-side driver: build the static tether binary + stage vendor binaries, rsync the
# whole test/simcluster/ tree to the Ubuntu sim server, then run `simcluster` there over ssh.
# The server has no Go toolchain, so the tether/nats/nk binaries are all produced on WSL (amd64,
# matching the server) and shipped. NOT a Makefile target (keeps sim-* off the release-gate build).
#
# Usage: ./remote.sh [--build] <simcluster subcommand + args...>
#        ./remote.sh --build up --brokers 1 --agents 1 --ctl 1
#        ./remote.sh status
set -euo pipefail

SERVER="${SIM_SERVER:-weiland@192.168.1.150}"
REMOTE_DIR="${SIM_REMOTE_DIR:-/home/weiland/dist_experiment_control/test/simcluster}"

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
# M10: single-source the nats-server pin from the REAL installer (never a hardcoded literal that can
# silently drift from what the fleet runs — the tool's whole justification).
NATS_VERSION="v$(grep -oE 'NATS_SERVER_VERSION[^0-9]*[0-9]+\.[0-9]+\.[0-9]+' "$repo/scripts/install.sh" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
[ "$NATS_VERSION" != "v" ] || { echo "[remote] could not read NATS_SERVER_VERSION from install.sh" >&2; exit 1; }
vendor="$here/vendor"
mkdir -p "$vendor"

do_build=0
if [ "${1:-}" = "--build" ]; then do_build=1; shift; fi

stage_binaries() {
    echo "[remote] building static tether (CGO_ENABLED=0 amd64)…"
    ( cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION="${SIM_VERSION:-v0.0.0-simcluster}" )
    cp "$repo/bin/tether" "$vendor/tether"

    # S5 / s3-s5-plan §1.E: a SECOND build with ONLY the version string bumped → vendor/tether-next, the
    # staged "new" binary drills 30/31 upgrade to. SAME source / proto / commandVersion / schema (g5
    # rolling-safety hard precondition — a real proto/command delta is an incompatible reinstall, not a
    # rolling upgrade). artifact.sh serves it with a host-computed SHA256SUMS; 30 stages it per-host, 31
    # serves it as the self-hosted mirror the agent allow-list (#28) refuses. Version-string-only delta is
    # enough to drive `node ls --brokers` skew, whole-host readback, and PID-preserving re-exec.
    echo "[remote] building tether-next (VERSION=${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}, same source)…"
    ( cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}" )
    cp "$repo/bin/tether" "$vendor/tether-next"

    # nats-server pinned to the production install.sh version (NOT go.mod's embedded newer one).
    if [ ! -x "$vendor/nats-server" ] || ! "$vendor/nats-server" --version 2>/dev/null | grep -q "${NATS_VERSION#v}"; then
        echo "[remote] fetching nats-server $NATS_VERSION…"
        tmp="$(mktemp -d)"
        curl -fsSL -o "$tmp/n.tgz" \
          "https://github.com/nats-io/nats-server/releases/download/${NATS_VERSION}/nats-server-${NATS_VERSION}-linux-amd64.tar.gz"
        tar xzf "$tmp/n.tgz" -C "$tmp"
        cp "$tmp/nats-server-${NATS_VERSION}-linux-amd64/nats-server" "$vendor/nats-server"
        rm -rf "$tmp"
    fi

    # nats + nk CLIs (host build == amd64 == server). Cached.
    if [ ! -x "$vendor/nats" ]; then
        echo "[remote] go install nats CLI…"
        GOBIN="$vendor" go install github.com/nats-io/natscli/nats@latest
    fi
    if [ ! -x "$vendor/nk" ]; then
        echo "[remote] go install nk…"
        GOBIN="$vendor" go install github.com/nats-io/nkeys/nk@latest
    fi

    cp "$repo/scripts/install.sh" "$vendor/install.sh"
    echo "[remote] vendor staged: $(cd "$vendor" && ls -1 | tr '\n' ' ')"
}

if [ "$do_build" = "1" ]; then stage_binaries; fi
[ -x "$vendor/tether" ] || { echo "[remote] no vendor/tether — run with --build first" >&2; exit 1; }

echo "[remote] rsync tree → $SERVER:$REMOTE_DIR"
ssh "$SERVER" "mkdir -p '$REMOTE_DIR'"
# B1: `secrets/` (the minted CA/account/route key stash) is generated SERVER-SIDE by lib/secrets.sh and
# never exists in this source tree — so `--delete`/`--delete-excluded` would wipe it on EVERY rsync,
# minting a fresh CA next verb and breaking a running persistent cluster's route mTLS. Protect the
# gitignored server-only paths (mirror .gitignore) so --delete leaves them alone.
# EXT-REVIEW-B9: `backups/` is the S0-backup-vault (lib/vault.sh) — also generated SERVER-SIDE and
# declared "survives the volume disaster; only `nuke` reaps it". Without protecting it here, the very next
# `remote.sh` call's `--delete` would reap it, breaking SIM_KEEP diagnostics, cross-command recovery and
# the "only nuke reaps it" lifecycle contract. It is gitignored (like secrets/), so mirror that here.
rsync -a --delete \
    --filter='P /secrets/***' --filter='P /backups/***' --filter='P /ssh_config' --filter='P *.local' \
    --exclude '.git' \
    "$here/" "$SERVER:$REMOTE_DIR/"

if [ "$#" -gt 0 ]; then
    # `drill-all` dispatches to run-drills.sh (parallel whole-suite runner); everything else to simcluster.
    entry=./simcluster
    if [ "$1" = "drill-all" ]; then entry=./run-drills.sh; shift; fi
    echo "[remote] ssh → ${entry##*/} $*"
    # F3 (external review): quote REMOTE_DIR + EACH arg with printf %q so argv boundaries survive the remote
    # shell. `$*` flattens everything into one string — args with spaces/quotes/;/$()/nested `sh -c` payloads
    # (exactly what `exec <node> -- <cmd…>` / `ctl -- <tether…>` pass) would break apart or become remote
    # shell syntax (injection). This preserves exact argv and is injection-safe.
    remote_cmd="cd $(printf '%q' "$REMOTE_DIR") && $entry"
    for a in "$@"; do remote_cmd="$remote_cmd $(printf '%q' "$a")"; done
    ssh -t "$SERVER" "$remote_cmd"
fi
