#!/bin/sh
# 62-remote-fs-safe.sh — S1 FEASIBILITY SPIKE (roadmap OQ-2), resolved to a FUSE-APPROX drill: reproduce
# a HUNG network-style mount on the real stack and assert tether's remote_fs postures (auto / off+--safe)
# against it, while HONESTLY labeling it a REAPABLE (S/T-state) approximation — NOT true uninterruptible-D
# (which needs kernel nfsd + a hard mount, a shared-host hazard → NOT-COVERED). Every wedge-touching
# command is bounded by an external `timeout`; the trap SIGCONTs + umounts so the instance always nukes.
# See docs/reviews/s1-plan.md §3.3. remote_fs decision logic is hermetic-dense (internal/spawnsafe); the
# deploy delta asserted here is a REAL fuse.hangfs mount + real bootHangable scan + real bounded statfs.
#
# Grounded facts (verified live 2026-07-11): remote_fs.mode ∈ {auto,off} ("safe" is NOT a mode; --safe is
# a per-call flag); a `fuse.hangfs` mount is spawnsafe-hangable (classifyFstype prefix "fuse."); with the
# daemon SIGSTOP'd, statfs BLOCKS and the agent's bounded probe FAST-FAILS (no agent wedge — measured):
#   exec --cwd <wedged>        → code remote_fs_unsafe_cwd (cwd on a dead mount → lexical cwd fail-fast in
#                                spawnsafe.Prepare, BEFORE any argv[0] resolution)
#   exec <wedged>/abs-argv0    → code remote_fs_unhealthy  (argv[0] on an unresponsive mount)
#   exec --safe --cwd <wedged> → code remote_fs_unsafe_cwd (per-call --safe escalates past mode:off, still
#                                fails the same dead-cwd check)
#
# NOT-COVERED (registered, measured): (Arm 2) true uninterruptible-D and the mode:off-WITHOUT-safe legacy
# hang — reproducing either would drive an unbounded chdir/exec into the wedge, risking an agent/shared-
# host wedge; the FUSE daemon here is kill-9-reapable (T/S-state), an APPROXIMATION, so equating it with
# real-D would be a Mandate-① false-GREEN. Left to dedicated hardware.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
URL="nats://brk1:4222"
MNT=/mnt/hung

# S1-12: match the daemon by `[h]angfs.py` so this wrapper's OWN cmdline (which contains the literal
# `[h]angfs.py`) does not self-match — the regex needs `h` immediately followed by `angfs`, which the
# bracketed literal breaks — so pkill never SIGKILLs its own shell alongside the daemon.
_cleanup_hang() {
    "$SIM" exec agt1 -- sh -c "pkill -CONT -f '[h]angfs.py' 2>/dev/null; timeout 8 umount -f -l $MNT 2>/dev/null; fusermount -u $MNT 2>/dev/null; pkill -9 -f '[h]angfs.py' 2>/dev/null; true" >/dev/null 2>&1 || true
}
trap '_cleanup_hang' EXIT INT TERM

# helpers
_hangfs_mounted(){ "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts; }
_statfs_healthy(){ "$SIM" exec agt1 -- timeout 5 stat -f "$MNT" >/dev/null 2>&1; }
_statfs_blocks() { ! "$SIM" exec agt1 -- timeout 5 stat -f "$MNT" >/dev/null 2>&1; }
# S1-05: background the daemon then POLL for the mount (no fixed sleep — flake-prone under parallel load).
_mount_hangfs() {
    "$SIM" exec agt1 -- sh -c "mkdir -p $MNT; grep -q fuse.hangfs /proc/mounts && exit 0; setsid python3 /opt/sim/hangfs.py $MNT >/tmp/hangfs.log 2>&1 &"
    poll_until 20 1 "fuse.hangfs mounted at $MNT" -- _hangfs_mounted
}
_wedge()        { "$SIM" exec agt1 -- pkill -STOP -f hangfs.py; }
# S1-05: after SIGCONT, POLL until statfs recovers instead of a fixed sleep.
_heal()         { "$SIM" exec agt1 -- pkill -CONT -f hangfs.py; poll_until 10 1 "statfs healthy after SIGCONT" -- _statfs_healthy; }
_agent_online() { "$SIM" ctl -- node ls 2>/dev/null | grep -qE "^agt1[[:space:]].*ONLINE"; }
_fuse_stopped() { "$SIM" exec agt1 -- sh -c 'p=$(pgrep -f hangfs.py | head -1); [ -n "$p" ] && awk "{print \$3}" /proc/$p/stat 2>/dev/null | grep -qE "^[Tt]"'; }
RFS()           { timeout 25 "$SIM" ctl -- exec "$@"; }

drill_begin "62-remote-fs-safe (SPIKE→FUSE-approx: remote_fs auto/off+--safe fast-fail on a wedged mount)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 1 agent + 1 ctl"   "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (N=1)"                  "$SIM" init brk1
assert_ok "session $SID + ctl login"         "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                   "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"

# ── Capability probe (LIVE): mount a fuse.hangfs, confirm it is spawnsafe-hangable + statfs-healthy ───
assert_ok "probe: mount fuse.hangfs at $MNT (healthy)"   _mount_hangfs
assert_ok "probe: mount fstype is fuse.hangfs (spawnsafe classifies fuse.* as a hangable network mount)" \
    sh -c "\"$SIM\" exec agt1 -- grep -q 'fuse.hangfs' /proc/mounts"
assert_ok "probe: statfs healthy while the daemon runs"  sh -c "\"$SIM\" exec agt1 -- timeout 5 stat -f $MNT >/dev/null 2>&1"

# ── Arm 1 — default AUTO: mount present at agent boot → wedge → bounded fast-fail (no agent wedge) ────
# reprovision restarts the agent WITH the healthy mount present so its boot-time scan sets bootHangable.
assert_ok "Arm1 setup: reprovision agt1 = default (auto), agent boots with the hangable mount present" \
    agent_provision_yaml agt1 "$SID" "$URL" open
assert_ok "Arm1 control: exec works normally under auto with a HEALTHY mount (auto is inert)" \
    timeout 25 "$SIM" ctl -- exec agt1 -- true
# wedge it.
assert_ok "Arm1 inject: SIGSTOP the fuse daemon → mount wedged"  _wedge
# D-discriminator (measured): the wedged daemon is STOPPED (T/S-state), NOT uninterruptible-D — this is the
# reason Arm 2 (true-D) is NOT-COVERED. + statfs must now block (proves the wedge reaches the probe path).
assert_ok "Arm1 discriminator: fuse daemon is T/S-state, NOT uninterruptible-D (reapable approximation)" _fuse_stopped
assert_ok "Arm1 discriminator: statfs on the mount now BLOCKS (bounded probe)"  _statfs_blocks
# THE deploy assertions — bounded, and they must FAST-FAIL (not wedge the agent):
assert_refuses "Arm1a auto: exec --cwd wedged-mount fast-fails remote_fs_unsafe_cwd (flag BEFORE node)" \
    "remote_fs_unsafe_cwd" \
    RFS --cwd "$MNT" agt1 -- whoami
assert_ok "Arm1 alive-control: a NORMAL exec STILL works after the wedge (bounded probe didn't wedge the agent)" \
    timeout 25 "$SIM" ctl -- exec agt1 -- true
assert_refuses "Arm1b auto: exec abs-argv0 on wedged mount fast-fails remote_fs_unhealthy" \
    "remote_fs_unhealthy" \
    RFS agt1 -- "$MNT/probe"
assert_ok "Arm1 heal: SIGCONT the daemon (restore healthy mount for Arm 3)"  _heal

# ── Arm 3 — mode:off + --safe: per-call escalation past mode:off still fast-fails on the wedge ────────
assert_ok "Arm3 setup: reprovision agt1 = remote_fs.mode off (agent reboots with healthy mount)" \
    agent_provision_yaml agt1 "$SID" "$URL" remotefs:off
assert_ok "Arm3 inject: SIGSTOP the fuse daemon → mount wedged again"  _wedge
assert_ok "Arm3 discriminator: still T/S-state (reapable approximation)"  _fuse_stopped
assert_refuses "Arm3 off+--safe: exec --safe --cwd wedged-mount fast-fails remote_fs_unsafe_cwd (escalation, flags BEFORE node)" \
    "remote_fs_unsafe_cwd" \
    RFS --safe --cwd "$MNT" agt1 -- whoami
assert_ok "Arm3 alive-control: normal exec still works (agent not wedged)"  timeout 25 "$SIM" ctl -- exec agt1 -- true
assert_ok "Arm3 heal: SIGCONT the daemon"  _heal

# ── Arm 2 / true-D — NOT-COVERED (measured reason; a valid spike outcome, host-safety) ───────────────
# The mechanism above is a REAPABLE FUSE approximation (T/S-state; kill-9 reaps it at cleanup). True
# uninterruptible-D needs kernel nfsd + a hard mount, and observing the mode:off-WITHOUT-safe legacy hang
# would drive an UNBOUNDED chdir/exec into the wedge — both risk wedging the agent/shared weilandserver.
# Registered in docs/deploy-tier-gotchas.md (OQ-2). NOT force-greened; asserted from the live T/S state.
warn "62 Arm 2 NOT-COVERED (measured): true uninterruptible-D + mode:off-without-safe legacy hang are a"
warn "  shared-host wedge hazard; the FUSE daemon here is T/S-state kill-9-reapable (an approximation)."
warn "  Left to dedicated hardware. Recorded as OQ-2 in docs/deploy-tier-gotchas.md."
# S1-10: back this verdict with a LIVE measurement, not a self-declaring `true`. The Arm1/Arm3 discriminators
# already proved T/S-state + statfs-blocks; here we re-measure that the wedge HEALED after the Arm3 SIGCONT
# (statfs recovers) — i.e. the FUSE stall is SIGCONT-REVERSIBLE / reapable, which is exactly why it is an
# APPROXIMATION and true uninterruptible-D stays NOT-COVERED (a real-D mount would NOT recover on SIGCONT).
assert_ok "Arm2 true-D + mode:off-without-safe NOT-COVERED (live: wedge was SIGCONT-reversible = reapable approx, not true-D)" \
    _statfs_healthy

_cleanup_hang
drill_end
