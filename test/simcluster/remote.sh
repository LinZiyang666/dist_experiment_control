#!/usr/bin/env bash
# remote.sh — external-driver script: build the static tether binary + stage vendor binaries, rsync the
# whole test/simcluster/ tree to the Ubuntu sim server, then run `simcluster` there over ssh.
# Use this only when you are NOT on weilandserver (on the server itself call ./simcluster directly).
# The tether/nats/nk binaries are produced on the driver box (amd64,
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
# M10: the nats-server pin is single-sourced from the REAL installer (never a hardcoded literal that
# can silently drift from what the fleet runs). That logic — and the whole vendor build — lives in
# lib/stage.sh so this driver and local.sh cannot diverge.
export SIM_REPO="$repo"
export SIM_VENDOR="$here/vendor"
export SIM_TAG="remote"
# shellcheck source=lib/stage.sh
. "$here/lib/stage.sh"
vendor="$SIM_VENDOR"
mkdir -p "$vendor"

# On the sim host itself this driver would rsync the machine onto itself and ssh into localhost —
# pointless and confusing. Point the operator at the on-host driver instead.
if sim_is_sim_host && [ "${SIM_ALLOW_SELF_RSYNC:-0}" != "1" ]; then
    echo "[remote] this machine IS the sim host — use ./local.sh ${*:-status} instead" >&2
    echo "[remote] (override with SIM_ALLOW_SELF_RSYNC=1 if you really mean to rsync onto self)" >&2
    exit 2
fi

do_build=0
if [ "${1:-}" = "--build" ]; then do_build=1; shift; fi

if [ "$do_build" = "1" ]; then sim_stage_binaries; fi
[ -x "$vendor/tether" ] || { echo "[remote] no vendor/tether — run with --build first" >&2; exit 1; }

# Every ssh here carries the same keepalive trio. WHY: a whole-suite `drill-all` holds one ssh open for
# hours with long silent stretches, and we have seen the server-side runner finish and exit — every .rc
# written — while the LOCAL ssh never returned, leaving the operator unable to tell "still running" from
# "wedged". ServerAlive* makes the client give up after ~3min of a dead peer instead of blocking forever
# (a NAT/conntrack idle-eviction drops the flow silently, so TCP alone never notices); ConnectTimeout
# bounds the handshake so an unreachable sim host fails fast instead of hanging the driver up front.
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=30 -o ServerAliveCountMax=6"

echo "[remote] rsync tree → $SERVER:$REMOTE_DIR"
# shellcheck disable=SC2086  # SSH_OPTS is a fixed literal above and MUST word-split into separate flags
ssh $SSH_OPTS "$SERVER" "mkdir -p '$REMOTE_DIR'"
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
    # shellcheck disable=SC2086  # ditto — fixed literal, deliberate split; -t and $remote_cmd unchanged
    ssh $SSH_OPTS -t "$SERVER" "$remote_cmd"
fi
