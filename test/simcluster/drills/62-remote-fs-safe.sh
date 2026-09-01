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
# Arm 1S-3's evidence dump reads the agent slog. It MUST go through this one mapping
# (CLAUDE.md gate: simcluster log oracle) and it MUST be sourced — a missing source is a
# runtime not-found that `bash -n` cannot see.
. "$HERE/drills/lib/logs.sh"
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
_1S_DIR=
_cleanup_1s() {
    [ -z "$_1S_DIR" ] || rm -rf -- "$_1S_DIR" 2>/dev/null || true
}
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): the combined form resumed after the handler on INT/TERM,
# so a Ctrl-C would un-wedge the mount and then keep driving the remaining wedge/umount steps. Same cleanup
# fn, now via drill_install_traps (EXIT registered separately; INT/TERM exit 128+signo without resuming).
_cleanup_all() { _cleanup_hang; _cleanup_1s; }
drill_install_traps _cleanup_all

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

# ── Arm 1S — STALE-HEALTHY: the mount is judged healthy FIRST and dies AFTER (gotcha #81) ─────────────
# Arm 1 above covers "never probed before it died": nothing had consulted $MNT, so the first lexical
# check probes it, finds it dead, and fast-fails. That is NOT the production shape. On timan107 an agent
# had been running 18 days, had already cached "healthy" for /shared, and when the NFS died that verdict
# was never revisited — the whole remote_fs feature silently degraded to two deadlines while a dead dir
# stayed at the head of $PATH. This arm is the only one that exercises that transition, which is why it
# is a separate arm: priming inside Arm 1 would quietly turn Arm1a/Arm1b into a different test.
#
# Priming uses an explicit argv[0] under the mount, NOT --cwd. Both cache the verdict, but --cwd would
# drive a real chdir into the mount; abs-argv0 costs one statfs (fast while healthy) plus one ENOENT
# execve. The 30s exposure this arm does accept (1S-2) is bounded by the agent's own execve watchdog and
# reversed by SIGCONT, so it does not reopen the true-D NOT-COVERED question below.
RFS40() { timeout 45 "$SIM" ctl -- exec "$@"; }   # > the agent's 30s execve watchdog; RFS's 25 is not

# 1S-0 prime: consult $MNT while it is HEALTHY so the verdict is cached (expected to fail with ENOENT —
#             the exit code is irrelevant, the cached verdict is the point).
"$SIM" ctl -- exec agt1 -- "$MNT/probe" >/dev/null 2>&1 || true
assert_ok "1S prime discriminator: mount still statfs-healthy after priming" _statfs_healthy

assert_ok "1S inject: SIGSTOP hangfs → the mount dies AFTER it was judged healthy"  _wedge
assert_ok "1S discriminator: still T/S-state, not uninterruptible-D (reapable approximation)" _fuse_stopped
assert_ok "1S discriminator: statfs on the mount now BLOCKS"                        _statfs_blocks

# Shared runner for the two abs-argv0 commands of this arm. State goes through FILES, not
# shell variables: assert_ok captures with `$(...)`, so a predicate runs in a subshell and
# any global it sets is discarded — the first version of this instrumentation used globals
# and both oracles silently read empty strings, which let one of them "pass" vacuously.
#
# THREE oracles per command, because there are three different owners and a two-way split
# provably mis-assigns one of them (external re-review RR-F3, reproduced live):
#   A. transport  — did ctl get any terminal state back inside the wrapper?   (ctl/NATS/harness)
#   B. delivery   — did the broker forward it AND did the agent receive it?   (broker/routing)
#   C. product    — is the agent's code the expected one?                     (agent behaviour)
# With only A and B, a broker refusal (`node_offline: status=STALE`, rc=70) passed A — it IS
# a terminal state — and then failed the product oracle, filing a routing problem as an agent
# bug. C is asserted ONLY when A and B both hold, so the product oracle can never be
# blamed for a request the agent never saw or for output ctl never received.
#
# Owner identification uses the agent's own slog, cursor-bounded to this command. That is a
# weaker contract than a request ID (concurrent identical argv could not be told apart); it
# holds here only because this arm is strictly sequential, and it is written down rather than
# assumed. A real request-ID contract is the follow-up.
_1S_DIR="${TMPDIR:-/tmp}/tether-1s-$$"
mkdir -p "$_1S_DIR"
# _1s_run <tag> <exec-args...>
_1s_run() {
    _t=$1
    shift
    _cursor=$(sim_agent_slog_cursor agt1)
    case "$_cursor" in
        ''|*[!0-9]*)
            printf '%s\n' "harness_error: could not capture a numeric agent slog cursor" > "$_1S_DIR/$_t.out"
            printf '%s\n' 125 > "$_1S_DIR/$_t.rc"
            printf '%s\n' 0 > "$_1S_DIR/$_t.cursor"
            return 0
            ;;
    esac
    printf '%s\n' "$_cursor" > "$_1S_DIR/$_t.cursor"
    timeout 45 "$SIM" ctl -- exec "$@" > "$_1S_DIR/$_t.out" 2>&1
    echo $? > "$_1S_DIR/$_t.rc"
    return 0
}
# Every printf format is passed via '%s\n': a literal starting with `--` is read as an
# option by dash's printf (`printf: Illegal option --`), which truncated the first dump.
_1s_evidence() {
    printf '%s\n' "$1 rc=$(cat "$_1S_DIR/$1.rc" 2>/dev/null)"
    printf '%s\n' "--- ctl stdout+stderr ---"
    cat "$_1S_DIR/$1.out" 2>/dev/null
    printf '%s\n' "--- agent slog since THIS command's cursor ---"
    sim_agent_slog_since agt1 "$(cat "$_1S_DIR/$1.cursor" 2>/dev/null)" 200
    printf '%s\n' "--- agent request-start lines after this command's cursor ---"
    sim_agent_slog_count agt1 'msg="agent: exec".*pid=' "$(cat "$_1S_DIR/$1.cursor" 2>/dev/null)"
}
# Oracle A — transport. A missing rc means the harness itself broke; that fails too, so a
# broken harness reports itself instead of impersonating a product verdict. 124 is the
# wrapper's own kill; 125/126/127 and 128+signo are `timeout`/exec failures, i.e. harness
# faults rather than ctl business exits, and are rejected here for the same reason.
_1s_terminal() {
    rm -f "$_1S_DIR/$1.terminal"
    _rc=$(cat "$_1S_DIR/$1.rc" 2>/dev/null)
    case "$_rc" in
        ""|124|125|126|127) _1s_evidence "$1"; return 1 ;;
    esac
    if [ "$_rc" -ge 128 ] 2>/dev/null; then _1s_evidence "$1"; return 1; fi
    : > "$_1S_DIR/$1.terminal"
    return 0
}
# Oracle B — delivery. The agent must have logged an exec after this command's cursor, and
# the reply must not be a broker-side refusal. Both halves matter: node_offline never reaches
# the agent, and a silent non-delivery leaves the count at zero.
_1s_delivered() {
    rm -f "$_1S_DIR/$1.delivered"
    if grep -qE 'node_offline|not_a_member|node_not_found|no_responders' "$_1S_DIR/$1.out" 2>/dev/null; then
        _1s_evidence "$1"
        return 1
    fi
    # Match only the request-start record. The looser `agent: exec` also matched a
    # late `agent: exec spawn bounded-start failed` warning from the PREVIOUS request,
    # whose handler can outlive ctl's 45s wrapper — exactly the failure under study.
    _n=$(sim_agent_slog_count agt1 'msg="agent: exec".*pid=' "$(cat "$_1S_DIR/$1.cursor" 2>/dev/null)")
    case "$_n" in
        1) ;;
        *) _1s_evidence "$1"; return 1 ;;
    esac
    : > "$_1S_DIR/$1.delivered"
    return 0
}
# Oracle C — product code. Only meaningful once A and B both hold.
_1s_code() {
    grep -q "$2" "$_1S_DIR/$1.out" 2>/dev/null && return 0
    _1s_evidence "$1"
    return 1
}

# 1S-2 control: the first command still pays the full execve watchdog. That is the known, documented cost
#               of a stale verdict — AND it is the evidence the agent learns from. Getting
#               remote_fs_spawn_timeout here (rather than remote_fs_unhealthy) is what proves the verdict
#               really was cached-healthy going in.
assert_ok "1S-2 run: first abs-argv0 after the mount died (captures rc + output)" _1s_run first agt1 -- "$MNT/probe"
assert_ok "1S-2 oracle A (transport): ctl got a terminal state back inside the wrapper" _1s_terminal first
assert_ok "1S-2 oracle B (delivery): the broker forwarded it and the agent received it" _1s_delivered first
if [ -f "$_1S_DIR/first.terminal" ] && [ -f "$_1S_DIR/first.delivered" ]; then
    assert_ok "1S-2 oracle C (product): the code is remote_fs_spawn_timeout (proves the verdict was cached-healthy going in)" \
        _1s_code first remote_fs_spawn_timeout
else
    warn "1S-2 oracle C (product): NOT EVALUATED because transport or delivery failed"
fi

# 1S-3 ★ the payload: that timeout must have expired the cached verdict, so the SAME command now
#        re-probes and fast-fails. Before the #81 fix this stayed remote_fs_spawn_timeout forever.
#
# THREE ORACLES, ON PURPOSE. This arm has been observed red twice — once at a 25s ctl budget
# and once at 45s — both times as rc=124 with EMPTY evidence, on a loaded box, with the
# runs on either side green in ~2s. rc=124 is the useless answer: it cannot distinguish
# "the verdict was never expired" (a product regression) from "the control plane never
# delivered a terminal state" (ctl/NATS/harness). Widening the budget did not fix that and
# the earlier claim here that 45s "will give a diagnosis code" was falsified by the
# external review reproducing rc=124 at 45s.
#
# So the three questions are asked separately, and A/B dump evidence when they
# fail — an empty failure record is what made the first two occurrences unattributable.
# Oracle C here is the PAYLOAD: reaching it with remote_fs_spawn_timeout means the
# evidence-driven invalidation did not take effect — a product regression, not infra.
assert_ok "1S-3 run: second abs-argv0 after the stall (captures rc + output)" _1s_run second agt1 -- "$MNT/probe"
assert_ok "1S-3 oracle A (transport): ctl got a terminal state back inside the wrapper" _1s_terminal second
assert_ok "1S-3 oracle B (delivery): the broker forwarded it and the agent received it" _1s_delivered second
if [ -f "$_1S_DIR/second.terminal" ] && [ -f "$_1S_DIR/second.delivered" ]; then
    assert_ok "1S-3 oracle C ★ (product): the code is remote_fs_unhealthy (evidence-driven re-probe happened)" \
        _1s_code second remote_fs_unhealthy
else
    warn "1S-3 oracle C ★ (product): NOT EVALUATED because transport or delivery failed"
fi

# 1S-4 ★ --safe must not need the sacrificial first command: it discards cached verdicts up front.
assert_ok "1S heal (re-arm the scenario for the --safe half)"  _heal
"$SIM" ctl -- exec agt1 -- "$MNT/probe" >/dev/null 2>&1 || true   # re-prime the healthy verdict
# Discriminator for the re-prime. Without it this half can pass for the WRONG reason:
# after 1S-3 the mount is sticky-dead, and if the late-probe drain failed to restore it
# the --safe assertion below would still see remote_fs_unhealthy — the right answer from
# the wrong state, i.e. a vacuous pass. A dead verdict fails --cwd fast; a healthy one
# does not, and the mount is alive at this instant so the chdir is not a hazard.
assert_ok "1S re-prime discriminator: the verdict really is HEALTHY again (a dead one would refuse --cwd)" \
    timeout 25 "$SIM" ctl -- exec --cwd "$MNT" agt1 -- true
assert_ok "1S re-wedge"  _wedge
assert_refuses "1S --safe discards cached verdicts: FIRST --safe call fast-fails remote_fs_unhealthy" \
    "remote_fs_unhealthy" \
    RFS --safe agt1 -- "$MNT/probe"

# 1S-5 mandatory guards on this arm's own exposure (plan §-1 OQ-5): the agent must still be serving, and
#      the mount must be handed back healthy to Arm 3.
assert_ok "1S alive-control: a normal exec still works (the abandoned execve did not wedge the agent)" \
    timeout 25 "$SIM" ctl -- exec agt1 -- true
assert_ok "1S heal: SIGCONT the daemon (hand a healthy mount to Arm 3)"  _heal

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
not_covered "62 Arm 2 (measured): true uninterruptible-D + mode:off-without-safe legacy hang" \
    "they are a shared-host wedge hazard; the FUSE daemon here is T/S-state kill-9-reapable (an approximation). Left to dedicated hardware. Recorded as OQ-2 in docs/deploy-tier-gotchas.md." gap
# S1-10: back this verdict with a LIVE measurement, not a self-declaring `true`. The Arm1/Arm3 discriminators
# already proved T/S-state + statfs-blocks; here we re-measure that the wedge HEALED after the Arm3 SIGCONT
# (statfs recovers) — i.e. the FUSE stall is SIGCONT-REVERSIBLE / reapable, which is exactly why it is an
# APPROXIMATION and true uninterruptible-D stays NOT-COVERED (a real-D mount would NOT recover on SIGCONT).
assert_ok "Arm2 true-D + mode:off-without-safe NOT-COVERED (live: wedge was SIGCONT-reversible = reapable approx, not true-D)" \
    _statfs_healthy

_cleanup_hang
drill_end
