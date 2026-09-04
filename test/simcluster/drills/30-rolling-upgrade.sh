#!/bin/sh
# 30-rolling-upgrade.sh — S5 (N=3, G5 #8 debt): `cluster upgrade` rolling broker upgrade. Binary staging is an
# EXPLICIT privileged step (never made to look automatic); dry-run previews the ordered roll (followers-first/
# leader-last); the real roll runs a raft WRITE probe (session create — a control-plane raft write, which
# DODGES the #29-dead cluster-expose data plane) that must see NO not_leader/503; whole-host asserts each
# broker's version flips v-cur→v-next WHILE its tether-broker MainPID is UNCHANGED (syscall.Exec re-exec).
#
# ── R9-D (2026-07-19): THE G5 MECHANISM ITSELF IS NOW COVERED; BEFORE THIS IT WAS NET-ZERO ────────────────────
# The 2026-07-18 full-suite review found this drill's G5 coverage was worth NOTHING: H1 (a version oracle that
# queried a schema the product never emitted ⇒ permanently empty) + H3 (a discarded roll rc and a roll.log that
# never reached the operator) meant that on the ONE run where the roll actually executed, no assertion could
# have proven it did. R4 fixed the oracle. What was still missing — and is added here — is the MECHANISM:
#
#   1. WHOLE-HOST (OQ-6, the supply gap). #19's ONLY definition of "this host is upgraded" is broker AND its
#      co-located agent both at target (node_versions.go:35). The sim ran no co-located agent anywhere, so that
#      leg had been a standing not_covered since the drill was written. drills/lib/colocated.sh now supplies the
#      machine (Mandate ③ — the sim builds hosts, tether upgrades them), and this drill builds ALL THREE of P3's
#      presence states in ONE cluster so a single roll exercises every branch of the plan:
#        (a) DECLARED   — broker.yaml colocated_agent_nid=colo-<host>, an nid DELIBERATELY != node_id so
#                         `assumed:false` proves the declaration (not the node_id==nid convention) was consumed;
#        (b) OBSERVED   — an agent registered under nid == node_id, NOT declared ⇒ `assumed:true`;
#        (c) ABSENT     — the leader host runs no agent at all ⇒ the plan must mark its step broker-only.
#   2. THE (b)-STATE HALT. P3's asymmetry is that a DECLARED agent stays PRESENT even while it is DOWN, so the
#      roll HALTs loudly instead of quietly "upgrading" half a host. This drill STOPS the declared agent and
#      requires the HALT — and requires it at the FIRST hop, so the partial state is exactly checkable: only
#      that host's broker advanced, the other two are untouched, and `node ls --brokers` reports SKEW on it.
#      A roll that SUCCEEDS here is an ASSERT-FAIL: silently skipping a declared-but-down agent is the P3 bug.
#   3. THE FAILURE PATH OUT. A HALT deliberately leaves the roll lock held (cluster_upgrade_drive.go:29-40), so
#      membership stays fenced. R7b's `tether cluster unlock` is the operator's out and its drill coverage was
#      registered to R9 (simcluster-coverage-inventory.md §2 row 157). It is exercised for real here: the
#      default clear must REFUSE a lease somebody is still renewing, --force must clear AND confirm, and — the
#      part that actually matters — the NEXT roll must get PAST acquire-lock, which is the only terminal proof
#      that membership was really unfenced rather than a command merely printing that it was.
#   4. PER-HOP ADVANCE + CONTINUITY. The HALT gives a deterministic mid-roll observation (hop 1 done, hops 2-3
#      untouched) that a completed roll cannot; the raft write probe runs across BOTH rolls, so continuity is
#      judged over the failure path too, not just the happy one.
#
# FALSE-GREEN GUARDS:
#  - staging is a labeled `[env: PRIVILEGED]` step asserted distinctly (never let the sim make the orchestration
#    look more automatic than it is — G5 rule); --expect-sha256 = the STAGED on-disk sha.
#  - version-readback ∧ PID-same are asserted TOGETHER — "no upgrade happened" also satisfies zero-disruption +
#    PID-same, so both must hold.
#  - the write probe is a REAL raft write (session create commits via propose→commit), non-vacuity-guarded.
#  - every version oracle reads the PRODUCT's own correlation (`node ls --brokers --json`), which is the same
#    computation `cluster upgrade`'s idempotency uses — not a side channel that could agree with a broken
#    correlation by accident.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/cluster.sh"; . "$HERE/drills/lib/colocated.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
NEXTVER="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}"
CTL() { "$SIM" ctl -- "$@"; }
# cluster upgrade is a ctl op over NATS (cluster_upgrade.go:68 connectCtl → needs an ACTIVE SESSION), NOT a
# broker admin-socket op like `cluster rebalance proxy` (callAdmin). So we log in broker-locally as the tether
# uid on the leader first (mirroring grow's `cluster add`, simcluster:186), then every cluster upgrade call
# reuses that session under the same HOME. account-seed lives on the broker (/etc/tether/secrets), so the
# leader broker is the right place to run it from.
_broker_login() { dexec -u tether "$LDR" -- env HOME=/var/lib/tether tether login -s "$SID" --pin "$PIN" --nats-url nats://brk1:4222; }
_BUP()    { dexec -u tether "$LDR" -- env HOME=/var/lib/tether tether cluster upgrade "$@"; }
_UNLOCK() { dexec -u tether "$LDR" -- env HOME=/var/lib/tether tether cluster unlock "$@"; }
SEED=/etc/tether/secrets/account.nk

# staging: root-install tether-next → /usr/local/bin/tether on EACH broker host (the documented privileged
# precondition — /usr/local/bin is root-owned, User=tether can't write it, so an operator stages it per host).
# NB coreutils `install` UNLINKS the destination first, so the RUNNING broker + co-located agent keep their old
# inode and only advance when `cluster upgrade` re-execs them. That is exactly the production sequence.
_stage_next() {
    # STAGED_SHA is computed in the drill body (below), NOT here: assert_ok runs its command in a subshell
    # (_as_capture), so a global set INSIDE _stage_next would be lost to the later _staged_sha_ok / _do_roll.
    [ -n "${STAGED_SHA:-}" ] || return 1
    for b in brk1 brk2 brk3; do
        d cp "$HERE/vendor/tether-next" "$(ctr_name "$b"):/tmp/tether-next" >/dev/null 2>&1 || return 1
        dexec "$b" -- install -m 0755 /tmp/tether-next /usr/local/bin/tether || return 1
    done
}
_staged_sha_ok() {
    for b in brk1 brk2 brk3; do
        _s=$(dexec "$b" -- sha256sum /usr/local/bin/tether 2>/dev/null | awk '{print $1}')
        [ "$_s" = "$STAGED_SHA" ] || return 1
    done
}
# RUNNING broker version, self-reported via node ls — NOT the on-disk /usr/local/bin/tether (staging already
# overwrote that to NEXTVER, so an on-disk check can't tell "staged" from "rolled"; only the running process's
# syscall.Exec re-exec flips the SELF-REPORTED release). roll re-execs the process → its release flips v-cur→NEXTVER.
# SCHEMA (node_ls_brokers v2, cmd/tether/node_versions.go): {schema, schema_version, brokers:[{node_id,
# agent_nid, assumed, broker_ver, agent_ver, skew, whole_host_at}]}. It is NOT `.nodes[].nid/.release` —
# querying that (the pre-fix bug) made every path here read an EMPTY string, which silently turned
# _all_on_next permanently FALSE and _dryrun_no_touch permanently TRUE (a permanent empty green).
_nlb() { CTL node ls --brokers --json 2>/dev/null; }
# `|tostring`, NOT `// empty`. jq's `//` treats BOTH null AND **false** as "absent", so `.assumed // empty`
# emits NOTHING for a row whose assumed is `false` — which would make `_field_is <h> assumed false` a
# PERMANENTLY-FALSE assertion (the H1 family, inverted: an oracle that can never pass no matter what the
# product does). Caught by this batch's non-vacuity harness, not by any run. tostring renders false as
# "false" and a genuinely missing row still yields no output at all, so both fail-closed cases survive.
_nlb_field() { _nlb | jq -r --arg n "$1" --arg f "$2" '.brokers[]?|select(.node_id==$n)|.[$f]|tostring' 2>/dev/null; }
_ver_of() { _nlb_field "$1" broker_ver; }
_agent_ver_of() { _nlb_field "$1" agent_ver; }
_mainpid() { "$SIM" exec "$1" -- systemctl show tether-broker -p MainPID --value 2>/dev/null; }
_all_on_next() { for b in brk1 brk2 brk3; do [ "$(_ver_of "$b")" = "$NEXTVER" ] || return 1; done; }
# _whole_host_at <broker> <version> : #19's ONE done-criterion — the broker daemon AND its co-located agent are
# both at <version>, read off the product's own correlation row. Both halves must be a REAL version string: an
# unreadable row yields "|" which can never equal "<v>|<v>", so this cannot pass by default.
_whole_host_at() {
    _wha=$(_nlb | jq -r --arg n "$1" '.brokers[]?|select(.node_id==$n)|"\(.broker_ver)|\(.agent_ver)"' 2>/dev/null)
    [ "$_wha" = "$2|$2" ]
}
_field_is() { [ "$(_nlb_field "$1" "$2")" = "$3" ]; }
_broker_ver_is() { [ "$(_ver_of "$1")" = "$2" ]; }
# Negations get their OWN named predicate. `sh -c "! _whole_host_at …"` would be the noshc false-green: a new
# shell inherits no sourced function, so the call dies "not found" and `!` turns that into a PERMANENT PASS.
_not_whole_host_at() { ! _whole_host_at "$1" "$2"; }
# _host_pair_is <broker> "<broker_ver>|<agent_ver>" : the row's two legs read as ONE tuple, so a state (c) host
# is pinned as "at target AND still agentless" rather than by two assertions that could each pass separately.
_host_pair_is() {
    [ "$(_nlb | jq -r --arg n "$1" '.brokers[]?|select(.node_id==$n)|"\(.broker_ver)|\(.agent_ver)"' 2>/dev/null)" = "$2" ]
}
# NON-VACUITY: "not on NEXTVER" must not be satisfiable by an UNREADABLE version. An empty _ver_of (broker
# absent from the JSON, ctl down, or the schema drifting again) FAILS here instead of passing by default —
# without this guard the whole dry-run-touched-nothing claim degrades back to a permanent empty green.
_dryrun_no_touch() {
    for b in brk1 brk2 brk3; do
        _dnt_v=$(_ver_of "$b")
        [ -n "$_dnt_v" ] || { warn "30: _dryrun_no_touch VACUOUS — no readable broker_ver for $b in node ls --brokers --json (schema drift or ctl failure); refusing to pass on an unread version"; return 1; }
        [ "$_dnt_v" = "$NEXTVER" ] && return 1
    done
    return 0
}
# grow lock leak (gotcha #31): cluster add's releaseGrowLock was BEST-EFFORT and used to leave cluster_grow_active
# set, which BLOCKS the roll at acquire-lock with "a cluster membership operation is in flight". R7a made the
# clear a registry pass with bounded retry (and `cluster add` now exits non-zero when the release does not
# confirm); R7b put the marker under a lease+TTL with `cluster unlock` as the operator's out. So this drill no
# longer waits for the ROLL to trip over the leak — it PROBES the marker directly before rolling (below), which
# is both earlier and unambiguous. This predicate stays as the defensive guard for a roll that trips anyway.
# NB the signature must cover BOTH acquire-lock refusal shapes, not just the round2-B2 wording: the G4 §B
# symmetric fence says "a `cluster add` grow of <j> is in PROGRESS", which the original
# 'membership operation|in flight' pattern did NOT match — a grow-fenced roll would have fallen through to the
# "undocumented failure" branch. `acquire upgrade lock` is the catch-all wrapper driveUpgrade puts on every
# acquire refusal, so it covers any future wording too.
_roll_not_blocked() { ! _roll_halted_on_growlock; }
_roll_halted_on_growlock() { grep -qiE 'membership operation|in flight|join/retire|grows are serialized|acquire upgrade lock|grow of .* is in progress' /tmp/roll.log 2>/dev/null; }
# ── membership-lock probes. Both run `cluster unlock --dry-run`, which is zero-mutation and reports each lock's
# state + lease. Captured to completion (never piped into `grep -q`, round-5 §M1) and rc-independent on purpose:
# a HELD lock whose lease is LIVE makes the verb exit non-zero AFTER printing the state line we are reading.
_upgrade_lock_held()  { _ulh=$(_UNLOCK --upgrade --dry-run 2>&1); printf '%s' "$_ulh" | grep -qi 'upgrade lock: HELD'; }
_upgrade_lock_clear() { _ulc=$(_UNLOCK --upgrade --dry-run 2>&1); printf '%s' "$_ulc" | grep -qiE 'upgrade lock: not held|no membership locks are held'; }
_grow_lock_held()     { _glh=$(_UNLOCK --grow --dry-run 2>&1);    printf '%s' "$_glh" | grep -qi 'grow lock: HELD'; }
_grow_lock_clear()    { _glc=$(_UNLOCK --grow --dry-run 2>&1);    printf '%s' "$_glc" | grep -qiE 'grow lock: not held|no membership locks are held'; }
# ── the roll PLAN (dry-run) as a first-class oracle. P3 put the whole-host-vs-broker-only DECISION in the plan
# (Step.BrokerOnly) precisely so the executor consumes a decision the operator was SHOWN, so the plan is where
# the three presence states are legible.
_plan_out()  { _BUP --to-version "$NEXTVER" --dry-run 2>&1; }
_plan_line() { _plan_out | awk -v n="$1" '$1=="UPGRADE" && $2==n'; }
_plan_is_broker_only() { _pbo=$(_plan_line "$1"); [ -n "$_pbo" ] && printf '%s' "$_pbo" | grep -q 'broker-only'; }
_plan_is_whole_host()  { _pwh=$(_plan_line "$1"); [ -n "$_pwh" ] && ! printf '%s' "$_pwh" | grep -q 'broker-only'; }
_plan_hosts_all_three() { [ "$(_plan_out | awk '$1=="UPGRADE"' | wc -l)" = 3 ]; }
_first_hop() { _plan_out | awk '$1=="UPGRADE"{print $2; exit}'; }
# roll ORDER oracle (Stage-C cluster-1): the leader's UPGRADE line must be the LAST upgrade line in the dry-run
# plan (followers-first / leader-last). dry-run doesn't acquire the upgrade lock, so this is reachable even under #31.
_roll_order_ok() {
    _o=$(_plan_out)
    _leader_ln=$(printf '%s' "$_o" | grep -niE 'UPGRADE.*leader|leader.*(transfer|last|upgrade)' | head -1 | cut -d: -f1)
    _last_ln=$(printf '%s' "$_o" | grep -niE 'UPGRADE ' | tail -1 | cut -d: -f1)
    [ -n "$_leader_ln" ] && [ -n "$_last_ln" ] && [ "$_leader_ln" = "$_last_ln" ]
}
# raft WRITE probe: a `session create` loop (each is a raft propose→commit) in ctl1, logging any not_leader/503.
# Create-only (no rm — session rm is a Tier-2 typed-confirm that would stall the loop); it dodges the #29-dead
# expose data plane entirely (control-plane raft write, not a tunnel). TAGGED so the two roll phases get
# independent windows (a phase-2 verdict must not be able to pass on phase-1's samples, or vice versa) and
# distinct session names (`session create` is not idempotent — a reused name fails and would forge a WRITEFAIL).
_start_write_probe() {
    # each `session create <tag>wpN` is a fresh raft propose→commit; --nats-url is REQUIRED (the sim ctl has no
    # default broker URL — simcluster:317 passes it too; without it session create fails "broker unreachable"
    # and every line would be a spurious WRITEFAIL, masquerading as an upgrade-induced raft outage).
    #
    # THE PROBE RUNS IN AN ISOLATED $HOME (R9-D — a harness bug this batch measured live). `session create`
    # calls cli.WriteCurrentSession (session.go:69): the session it just made becomes the ACTIVE one for that
    # HOME. The probe used to run under the SAME HOME=/home/sim the sim's ctl uses, so from its very first
    # iteration the ctl's active session was a throwaway `wpN` — and every SESSION-SCOPED read afterwards was
    # answered for an empty session. `node ls` printed "(no nodes)"; the AGENT half of `node ls --brokers`
    # therefore read agent_ver="?" and skew=false on a host that was genuinely skewed. The BROKER half kept
    # working (it comes from the cluster-health probe, which is not session-scoped), which is precisely why
    # this survived unnoticed for as long as the drill had no agent-side oracle to expose it.
    dexec ctl1 -- install -o sim -g sim -m 0755 -d "/home/sim/probe-$1" >/dev/null 2>&1 || return 1
    # THE ISOLATED $HOME IS ALSO AN ISOLATED IDENTITY. EnsureIdentity mints a separate nkey per home,
    # so this probe's fingerprint is NOT the one `$SIM session` admitted — without its own admission
    # every iteration is refused (not_allowed) and the log fills with WRITEFAIL that has nothing to do
    # with the roll. That is the same class of forged verdict the --nats-url note above guards against:
    # a probe whose failures are all fixture-induced cannot testify about raft write availability.
    admit_creator brk1 ctl1 sim "HOME=/home/sim/probe-$1" || return 1
    dexec ctl1 -- sh -c "nohup sh -c \"i=0; while [ \\\$i -lt 1200 ]; do runuser -u sim -- env HOME=/home/sim/probe-$1 tether session create $1wp\\\$i --pin 111111 --nats-url nats://brk1:4222 >>/tmp/wp-$1.log 2>&1 && echo WROTE-$1wp\\\$i >>/tmp/wp-$1.log || echo WRITEFAIL >>/tmp/wp-$1.log; i=\\\$((i+1)); sleep 0.3; done\" >/dev/null 2>&1 & echo ok" >/dev/null 2>&1
    # D2: start the observation-only scene watcher for this phase's window (snapshots the cluster at the
    # FIRST failure-signature hit so the HALT-window question can be adjudicated from a preserved scene).
    _start_scene_watcher "$1"
}
# _ctl_session_intact : the ctl's ACTIVE session is still the drill's own. A guard, not a decoration: every
# agent-side oracle below is session-scoped, and the failure mode above was silent (empty node list ⇒ "?"),
# so it is asserted explicitly rather than trusted.
_ctl_session_intact() { [ "$(dexec -u sim ctl1 -- sh -c 'cat /home/sim/.tether/current_session 2>/dev/null')" = "$SID" ]; }
# The write-probe failure signatures — shared by _probe_clean and the scene watcher so they can never drift.
_scene_sigs='not_leader|503|no.responder|unavailable|no servers|WRITEFAIL'
# _start_scene_watcher (D2 / release-readiness-followups §6.1) — a HOST-side, OBSERVATION-ONLY background
# loop that snapshots the cluster the INSTANT the write-probe first logs a failure signature, so the
# undecided drill-30 HALT-window question (real write-availability defect vs parallel-sweep infra artifact
# vs over-strict predicate) can be adjudicated from a preserved scene rather than the END state SIM_KEEP=1
# leaves. It adds ZERO assert sites, never writes into /tmp/wp-*.log (cannot forge WROTE/WRITEFAIL), and a
# mid-roll `cluster status` that itself fails is EVIDENCE, not an error (captured best-effort with || true).
_start_scene_watcher() {
    _tag=$1
    rm -f "/tmp/wp-$_tag.scene" "/tmp/wp-$_tag.scene-done" 2>/dev/null || true
    # CRITICAL: DETACH the subshell's stdin/stdout/stderr. _start_write_probe runs under assert_ok, whose
    # _as_capture wraps it in a $(...) command substitution — that substitution blocks until EOF on the
    # pipe, and a backgrounded subshell that INHERITS the pipe's write end (and loops until a roll that
    # only starts AFTER this returns) would wedge the drill at "start probe" on EVERY run. `</dev/null
    # >/dev/null 2>&1` frees the pipe so the capture returns promptly; the scene is written to its own file.
    # The loop is BOUNDED (~20min = 2400×0.5s > the roll's worst case) so an aborted drill cannot leak an
    # immortal host-side dexec loop even if _stop_scene_watcher never runs.
    _ldr="${LDR:-brk1}"
    (
        _n=0
        while [ ! -f "/tmp/wp-$_tag.scene-done" ] && [ "$_n" -lt 2400 ]; do
            _n=$((_n + 1))
            _hit=$(dexec ctl1 -- grep -m1 -inE "$_scene_sigs" "/tmp/wp-$_tag.log" 2>/dev/null || true)
            if [ -n "$_hit" ]; then
                {
                    echo "=== SCENE @ $(date -u +%Y-%m-%dT%H:%M:%SZ) tag=$_tag ==="
                    echo "first failure-signature line: $_hit"
                    for _b in brk1 brk2 brk3; do
                        echo "--- $_b cluster status (leader at start = $_ldr) ---"
                        dexec -u tether "$_b" -- tether cluster status 2>&1 | head -24 || true
                        echo "--- $_b systemctl show (MainPID / ExecMainStartTimestamp) ---"
                        dexec "$_b" -- systemctl show tether-broker -p MainPID,ExecMainStartTimestamp 2>&1 || true
                        echo "--- $_b journalctl -u tether-broker (last 80) ---"
                        dexec "$_b" -- journalctl -u tether-broker -n 80 --no-pager 2>&1 || true
                    done
                } > "/tmp/wp-$_tag.scene" 2>&1
                : > "/tmp/wp-$_tag.scene-done"
                break
            fi
            sleep 0.5
        done
    ) </dev/null >/dev/null 2>&1 &
    echo $! > "/tmp/wp-$_tag.watcher.pid"
}
_stop_scene_watcher() {
    if [ -f "/tmp/wp-$1.watcher.pid" ]; then
        kill "$(cat "/tmp/wp-$1.watcher.pid")" 2>/dev/null || true
        rm -f "/tmp/wp-$1.watcher.pid" 2>/dev/null || true
    fi
}
# _replay_scene surfaces the captured failure-instant scene into the drill output (like _dump_roll_log for
# the roll): SIM_KEEP=1 only preserves the END state, so without this the failure instant is lost.
_replay_scene() {
    if [ -s "/tmp/wp-$1.scene" ]; then
        warn "30: write-probe[$1] saw a failure signature — replaying the captured FIRST-HIT scene:"
        while IFS= read -r _sl; do log "30: scene[$1]| $_sl"; done < "/tmp/wp-$1.scene"
        warn "30: ---- end scene[$1] ----"
    fi
}
_stop_write_probe() {
    dexec ctl1 -- pkill -f "session create $1wp" >/dev/null 2>&1 || true
    _stop_scene_watcher "$1"
}
_probe_clean() {
    _stop_write_probe "$1"
    # non-vacuity guard (Stage-C mandate-2/pin-1): the probe must have ACTUALLY committed writes — else a
    # silent/never-run probe grep-MISSES the failure markers and false-greens (grep -q on an empty/missing file
    # exits 1/2, the leading ! flips both to PASS). Require ≥1 WROTE-* committed line first.
    dexec ctl1 -- grep -qE "^WROTE-$1wp[0-9]" "/tmp/wp-$1.log" 2>/dev/null || { warn "30: probe[$1] VACUOUS — /tmp/wp-$1.log has no committed session-create; a zero-not_leader here is meaningless"; return 1; }
    # NOTE: the scene replay is NOT done here — _probe_clean runs UNDER assert_ok's _as_capture ($(...)),
    # which swallows all but the last few lines. The drill body calls _replay_scene AFTER the CONTINUITY
    # assert (uncaptured, like _dump_roll_log), so the full multi-line scene reaches the transcript.
    if dexec ctl1 -- grep -qiE "$_scene_sigs" "/tmp/wp-$1.log" 2>/dev/null; then
        return 1
    fi
    return 0
}
# _do_roll RETURNS the roll's exit code (it used to be discarded, so a roll that failed for a reason other
# than #31 was indistinguishable from a clean one). /tmp/roll.log is the ONLY record of why a roll failed —
# _dump_roll_log replays it into the drill output, since a log that only exists inside the run is useless
# for post-hoc attribution.
_do_roll() {
    _BUP --to-version "$NEXTVER" \
        --expect-sha256 "$STAGED_SHA" --account-seed "$SEED" --backup-taken >/tmp/roll.log 2>&1
    _ROLL_RC=$?
    return "$_ROLL_RC"
}
_dump_roll_log() {
    warn "30: cluster upgrade roll rc=${_ROLL_RC:-?} — dumping /tmp/roll.log verbatim:"
    if [ -s /tmp/roll.log ]; then
        while IFS= read -r _rl_line; do log "30: roll.log| $_rl_line"; done < /tmp/roll.log
    else
        warn "30: /tmp/roll.log is MISSING or EMPTY — the roll left nothing to attribute the failure to"
    fi
    warn "30: ---- end /tmp/roll.log (rc=${_ROLL_RC:-?}) ----"
}
# the P3 (b)-state HALT signature: the roll stopped AT the host whose DECLARED agent is down, naming the AGENT
# leg. Both halves are required — "HALTED at <host>" alone would also match a transfer-leader or reload failure.
_halted_on_agent_leg() {
    grep -qE "HALTED at $FIRSTHOP" /tmp/roll.log 2>/dev/null \
        && grep -qiE 'agent re-exec refused|agent_no_responders' /tmp/roll.log 2>/dev/null
}
# ── the roll's OWN account of what it did to each host. These pin the MECHANISM, which the end-state
# oracles cannot: a host can arrive at the target version for reasons other than the roll (an operator
# restarting a daemon onto the already-staged binary does it too), and a drill that only reads the end state
# would credit `cluster upgrade` for an advance it never performed.
_roll_reexeced_agent_of() {   # the roll ran the AGENT leg on <host> and declared the whole host done
    grep -qE "re-exec $1's co-located agent into $NEXTVER" /tmp/roll.log 2>/dev/null \
        && grep -qE "$1 \(broker\+agent\) at $NEXTVER" /tmp/roll.log 2>/dev/null
}
_roll_skipped() { grep -qE "SKIP +$1 \(already at target\)" /tmp/roll.log 2>/dev/null; }

drill_begin "30-rolling-upgrade (N=3 cluster upgrade: whole-host roll incl. OQ-6 co-located agents, the P3 three-state plan, the (b)-HALT + cluster unlock recovery — G5 #8)"
"$SIM" nuke >/dev/null 2>&1 || true
# external-review R5-M5: drill 30 OWNS the #31 grow-lock pin, so it uses grow_to_3's SINGLE no-retry path (3rd
# arg 0) — NO shared nuke+retry can launder its first-grow evidence. The #31 leak does not block VOTER (grow
# reaches VOTER + reports success while leaking the lock), so one attempt still lands the leaked-lock state.
# Run OUTSIDE assert_ok so GROW-ATTEMPTS (must be "1 (retry=0)") is VISIBLE first-class evidence of the no-retry path.
grow_to_3 0 1 0; _g3rc=$?
assert_ok "grow_to_3 (N=3 HA cluster, SINGLE no-retry attempt — 30 owns #31; GROW-ATTEMPTS logged above must be 1/retry=0)"  sh -c "exit $_g3rc"
assert_ok "session lab + ctl login (owner)"   "$SIM" session "$SID" --pin "$PIN"
LDR=$(sim_leader); log "30: leader=$LDR ; staged version=$NEXTVER"
[ -n "$LDR" ] || setup_fail "30: no raft leader after grow — nothing below can be attributed"
assert_ok "broker-local ctl login on leader $LDR (cluster upgrade + cluster unlock are ctl-over-NATS ops, unlike the admin-socket rebalance)"  _broker_login

# ── #31 grow-lock hygiene, PROBED not inferred (R9-D) ─────────────────────────
# Pre-R9-D this was pinned by waiting for the REAL roll to trip over the leaked marker at acquire-lock — which
# conflated "the lock leaked" with "the upgrade is unreachable" and left every mechanism assertion below hostage
# to it. `cluster unlock --dry-run` reads the marker + its lease directly, so the leak is now pinned EARLIER and
# UNAMBIGUOUSLY, and the remedy is tether's OWN verb (never a sim workaround that would hide the gap).
if _grow_lock_held; then
    product_red "#31 (owner: R7a release-failure shape / R7b lease+TTL) — 'cluster add' reported SUCCESS and every node reached VOTER, yet the cluster_grow_active marker is STILL held afterwards. While it is held, join/retire/upgrade are all refused, so a grow that says it worked leaves membership fenced. Probed directly via cluster unlock --dry-run (zero mutation)"
    assert_ok "#31-remedy tether's OWN out clears it: cluster unlock --grow --force (rc=0 ONLY when a post-clear re-probe confirms the marker is gone — cluster_unlock.go:236,279-305)"  _UNLOCK --grow --force --account-seed "$SEED"
    assert_ok "#31-remedy-effect an INDEPENDENT zero-mutation re-probe now reports the grow lock NOT held (the clear is real, not just a printed claim)"  _grow_lock_clear
else
    assert_ok "[R7a] NO grow lock leaked after 'cluster add' — cluster_grow_active was released AND the release confirmed (a KEPT invariant since R7a; before it, a silent best-effort release routinely left the marker set and fenced membership)"  _grow_lock_clear
fi

# ── OQ-6 SUPPLY: build P3's three presence states on the three broker hosts ───
# The DECLARED agent goes on the FIRST hop of the roll deliberately: the (b)-state HALT below then leaves a
# partial state with exactly one host advanced, which is the only mid-roll observation this drill can make
# deterministically. Read the hop order from the product's own plan rather than assuming brk2 — an election
# that moved the leader would silently invalidate a hardcoded guess.
FIRSTHOP=$(_first_hop)
[ -n "$FIRSTHOP" ] || setup_fail "30: could not read the first UPGRADE hop out of the dry-run plan"
[ "$FIRSTHOP" != "$LDR" ] || setup_fail "30: the plan's first hop is the LEADER ($LDR) — followers-first is violated, so the whole hop-order premise is void"
OTHERHOP=""
for _b in $(list_nodes broker); do
    [ "$_b" = "$LDR" ] && continue
    [ "$_b" = "$FIRSTHOP" ] && continue
    OTHERHOP=$_b; break
done
[ -n "$OTHERHOP" ] || setup_fail "30: no second follower to host the OBSERVED-presence agent"
DECL_NID="colo-$FIRSTHOP"     # != node_id ON PURPOSE: proves the DECLARATION was consumed, not the convention
OBS_NID="$OTHERHOP"           # == node_id: the pre-G5 convention, discovered by OBSERVATION
log "30: presence fixture — DECLARED=$DECL_NID on $FIRSTHOP (first hop) ; OBSERVED=$OBS_NID on $OTHERHOP ; ABSENT on the leader $LDR"
assert_setup "[env: OQ-6 SUPPLY] declare $DECL_NID as $FIRSTHOP's co-located agent in broker.yaml + restart the broker (provisioning: the sim builds the HOST shape; every upgrade action below is tether's)"  colocated_agent_declare "$FIRSTHOP" "$DECL_NID"
assert_setup "[env: OQ-6 SUPPLY] start the DECLARED co-located agent $DECL_NID on broker host $FIRSTHOP (out of the root-owned /usr/local/bin/tether — the re-exec-only path a real co-located agent uses)"  colocated_agent_up "$FIRSTHOP" "$SID" "$PIN" "$DECL_NID"
assert_setup "[env: OQ-6 SUPPLY] start an UNDECLARED co-located agent nid=$OBS_NID on broker host $OTHERHOP (node_id==nid convention ⇒ presence must be OBSERVED from the session node list)"  colocated_agent_up "$OTHERHOP" "$SID" "$PIN" "$OBS_NID"
VCUR=$(_ver_of "$LDR"); log "30: v-cur (running release before any roll) = [$VCUR]"
[ -n "$VCUR" ] || setup_fail "30: could not read the CURRENT broker release — every version assertion below would be vacuous"
[ "$VCUR" != "$NEXTVER" ] || setup_fail "30: v-cur already equals the target $NEXTVER — the roll would be a no-op and every advance assertion vacuous"

# ── P3 three-state correlation, read from the product's own dual-version table ─
log "30: P3 presence correlation as the PRODUCT reports it → $(_nlb | jq -c '[.brokers[]?|{node_id,agent_nid,assumed,broker_ver,agent_ver,skew}]' 2>/dev/null)"
assert_ok "P3-(a)-nid DECLARED: $FIRSTHOP's row names agent_nid=$DECL_NID — the broker.yaml declaration was consumed; this nid differs from the node_id, so it CANNOT have come from the fallback convention"  _field_is "$FIRSTHOP" agent_nid "$DECL_NID"
assert_ok "P3-(a)-assumed $FIRSTHOP is NOT an assumed pairing (assumed=false ⇒ self-declared, node_versions.go:56-60)"  _field_is "$FIRSTHOP" assumed false
assert_ok "P3-(b) OBSERVED: $OTHERHOP correlates to nid=$OBS_NID via the node_id==nid convention (assumed=true — the operator is never shown a silently-wrong pairing)"  _field_is "$OTHERHOP" assumed true
assert_ok "P3-(c) ABSENT: the leader $LDR reports agent_ver='?' — no agent has EVER registered under its node_id, which is the only thing that makes a host state (c) rather than a merely-down agent"  _field_is "$LDR" agent_ver '?'
# BASELINE (anti-vacuity for the post-roll whole-host claim): both agent hosts must start at v-cur on BOTH
# legs, else "whole_host_at NEXTVER" after the roll could be satisfied by an equality that predated it.
assert_ok "P3-(a) $FIRSTHOP whole-host BASELINE is $VCUR on both legs — the post-roll assertion therefore measures a real advance"  _whole_host_at "$FIRSTHOP" "$VCUR"
assert_ok "P3-(b) $OTHERHOP whole-host BASELINE is $VCUR on both legs"  _whole_host_at "$OTHERHOP" "$VCUR"

# ── privileged staging (explicit, distinct — NOT automatic) ──────────────────
STAGED_SHA=$(sha256sum "$HERE/vendor/tether-next" 2>/dev/null | awk '{print $1}')
log "30: staged sha256=$STAGED_SHA"
assert_ok "[env: PRIVILEGED STAGING — operator stages the new binary; cluster upgrade does NOT] install tether-next → /usr/local/bin/tether on each broker"  _stage_next
assert_ok "staged on-disk sha256 matches on every broker (--expect-sha256 anchor; the co-located agents' re-exec target is this SAME staged file)"  _staged_sha_ok

# ── over-automation guards: cluster upgrade refuses without the operator-supplied safety inputs ──
assert_refuses "no --account-seed → refused (each broker verifies an account-signed trigger)" \
               "account-seed|account seed|required" \
               _BUP --to-version "$NEXTVER" --expect-sha256 "$STAGED_SHA" --backup-taken
assert_refuses "no --backup-taken → refused (first-boot migration is one-way; restore is the only recovery)" \
               "backup" \
               _BUP --to-version "$NEXTVER" --expect-sha256 "$STAGED_SHA" --account-seed "$SEED"

# ── dry-run: ordered roll plan, per-host SCOPE, zero hosts touched ───────────
assert_ok "cluster upgrade --dry-run plans EVERY broker host (exactly 3 UPGRADE steps — a count, not a keyword grep that any prose could satisfy)"  _plan_hosts_all_three
assert_ok "cluster upgrade --dry-run roll ORDER: the leader's UPGRADE is the LAST upgrade line (followers-first/leader-last; Stage-C cluster-1)"  _roll_order_ok
assert_ok "PLAN-(c) the agentless leader $LDR's step is marked '[broker-only — no co-located agent on this host]' — P3's state (c) rendered where the operator can see it BEFORE the roll (cluster_upgrade.go:renderUpgradePlan)"  _plan_is_broker_only "$LDR"
assert_ok "PLAN-(a) the DECLARED-agent host $FIRSTHOP is NOT broker-only — a whole-host step (broker + agent legs)"  _plan_is_whole_host "$FIRSTHOP"
assert_ok "PLAN-(b) the OBSERVED-agent host $OTHERHOP is NOT broker-only either — presence inferred from the node list still yields a whole-host step"  _plan_is_whole_host "$OTHERHOP"
assert_ok "dry-run left every broker's RUNNING version on v-cur (no host re-exec'd; on-disk was already staged to NEXTVER so this checks the SELF-REPORTED running release, not the binary on disk)"  _dryrun_no_touch

# ── PHASE 1 — the P3 (b)-state HALT: a DECLARED agent that is DOWN ──────────
# P3's asymmetry (cluster_upgrade.go:320-341): a declared agent is PRESENT as a CONFIGURATION fact, so a host
# whose declared agent is stopped stays not-at-target and the roll must HALT LOUDLY on it. The pre-P3 failure
# chain this reproduces verbatim is `HALTED at <host>: agent re-exec refused: agent_no_responders`.
MP1=$(_mainpid brk1); MP2=$(_mainpid brk2); MP3=$(_mainpid brk3)
assert_setup "[env] stop the DECLARED co-located agent $DECL_NID on the FIRST hop $FIRSTHOP (constructing P3 state (b): declared, therefore PRESENT, but not answering)"  colocated_agent_stop "$FIRSTHOP"
assert_ok "start background raft write-probe (phase-1 window) in ctl1"  _start_write_probe p1
assert_ok "PROBE-ISOLATION the ctl's ACTIVE session is still '$SID' after the probe started writing — the probe creates sessions, and 'session create' rewrites the active-session pointer of whatever HOME it runs in. If it shared the ctl's HOME, every session-scoped oracle below (the whole AGENT half of node ls --brokers) would silently answer for an empty throwaway session and report agent_ver='?'"  poll_until 20 2 "ctl session still $SID" -- _ctl_session_intact
_do_roll; _dump_roll_log   # rc is JUDGED below; the log is replayed unconditionally so the HALT is attributable
if _roll_halted_on_growlock; then
    # Not expected any more — the grow lock was probed + cleared above. If it is back, that is the acquire-lock
    # window re-fencing membership, and the P3 arm cannot be judged (the roll never reached a host).
    product_red "#31 acquire-lock window re-fenced the roll AFTER the lock was probed clear (owner: R7b lease+TTL) — the real cluster upgrade HALTED at acquire-upgrade-lock with 'a cluster membership operation (join/retire) is in flight', although cluster unlock reported the grow lock cleared moments earlier"
    not_covered "30 PHASE-1 P3 (b)-state HALT + the whole-host roll mechanism THIS RUN" "the roll never reached a host: it was fenced at acquire-lock by the #31 marker recorded PRODUCT-RED above. Every mechanism assertion below (per-hop advance, skew, unlock recovery, whole-host readback) would be describing a roll that did not run." gap
    HALTED_ON_AGENT=0
elif [ "${_ROLL_RC:-1}" = 0 ]; then
    # THE false-green this arm exists for: a declared-but-down agent SILENTLY skipped. Pre-P3 the executor
    # re-derived presence mid-roll, so a host whose agent had vanished could be quietly downgraded to
    # broker-only and reported done — a half-upgraded host counted as upgraded.
    assert_ok "PHASE-1 [P3 (b)] the roll must HALT on a DECLARED-but-DOWN co-located agent, NEVER report success — it returned rc=0, i.e. it silently treated $FIRSTHOP as broker-only and called a half-upgraded host done. That is the P3 regression"  sh -c "false"
    HALTED_ON_AGENT=0
elif _halted_on_agent_leg; then
    assert_ok "PHASE-1 [P3 (b)] the roll HALTED at the first hop $FIRSTHOP naming the AGENT leg ('HALTED at $FIRSTHOP' + 'agent re-exec refused'/'agent_no_responders') — a declared agent stays PRESENT while it is down, so the roll stops loudly instead of quietly skipping half the host"  _halted_on_agent_leg
    HALTED_ON_AGENT=1
else
    assert_ok "PHASE-1 the roll failed (rc=${_ROLL_RC:-?}) for a reason that is NEITHER the expected P3 agent-leg HALT at $FIRSTHOP NOR the #31 acquire-lock fence — an undocumented failure, surfaced rather than laundered into one of the known shapes (see the verbatim roll.log above)"  sh -c "false"
    HALTED_ON_AGENT=0
fi

if [ "$HALTED_ON_AGENT" = 1 ]; then
    # ── PER-HOP ADVANCE: the only deterministic mid-roll observation available. Exactly one host advanced. ──
    assert_ok "PHASE-1 per-hop [1/3] the halted hop $FIRSTHOP's BROKER daemon did advance to $NEXTVER (its reload succeeded; the HALT came afterwards, on the agent leg)"  poll_until 60 3 "$FIRSTHOP broker on next" -- _broker_ver_is "$FIRSTHOP" "$NEXTVER"
    assert_ok "PHASE-1 per-hop [2/3] $OTHERHOP is UNTOUCHED (still $VCUR) — the roll advances ONE host at a time; a roll that fanned out would have moved it too"  _broker_ver_is "$OTHERHOP" "$VCUR"
    assert_ok "PHASE-1 per-hop [3/3] the leader $LDR is UNTOUCHED (still $VCUR) — leader-last held even on the failure path"  _broker_ver_is "$LDR" "$VCUR"
    assert_ok "PHASE-1 SKEW $FIRSTHOP now reports skew=true — broker at $NEXTVER, its co-located agent still at $VCUR. This is #19's whole-host verdict doing its job: the host is NOT done, and the operator is told so by name"  _field_is "$FIRSTHOP" skew true
    assert_ok "PHASE-1 SKEW $FIRSTHOP is NOT whole-host-at-target (the half-upgraded host must not read as done on either version)"  _not_whole_host_at "$FIRSTHOP" "$NEXTVER"
    assert_ok "PHASE-1 CONTINUITY the raft write-probe saw NO not_leader / 503 / no-responders ACROSS the failed roll — a HALT must leave a SAFE partial state, i.e. writable, not a wedged cluster"  _probe_clean p1
    _replay_scene p1   # uncaptured: surface the full first-hit scene into the transcript if one was captured

    # ── THE WAY OUT: `tether cluster unlock` (R7b; drill coverage owner = R9) ──
    assert_ok "UNLOCK-precond the HALTed roll LEFT the roll lock HELD (cluster_upgrade_drive.go deliberately does not release on HALT, so membership stays fenced while the cluster sits half-rolled)"  _upgrade_lock_held
    assert_refuses "UNLOCK-safety the DEFAULT clear REFUSES a lock whose lease is still in the future — the orchestrator only died seconds ago, so from the cluster's point of view somebody may still be renewing it. Ripping it out by default would re-admit the concurrent membership change the lock exists to fence" \
        "still being renewed|--force|lease" \
        _UNLOCK --upgrade --account-seed "$SEED"
    assert_ok "UNLOCK-force --force clears the roll lock AND CONFIRMS it: rc=0 only after a post-clear re-probe finds it gone (cluster_unlock.go:236,279-305 — 'probably cleared' is explicitly not good enough)"  _UNLOCK --upgrade --force --account-seed "$SEED"
    assert_ok "UNLOCK-effect an INDEPENDENT zero-mutation re-probe now reports the roll lock NOT held"  _upgrade_lock_clear

    # ── PHASE 2 — repair the (b) state and resume; the roll must complete whole-host ──
    assert_setup "[env] restart the DECLARED co-located agent $DECL_NID on $FIRSTHOP (repairing state (b); the roll is resumed by re-running it, which is the documented recovery)"  colocated_agent_start "$FIRSTHOP"
    assert_ok "PHASE-2 the repaired agent is back ONLINE before resuming"  poll_until 90 3 "$DECL_NID ONLINE" -- _colo_online "$DECL_NID"
    # The restart picks up the ALREADY-STAGED /usr/local/bin/tether, so $FIRSTHOP becomes whole-host at target
    # WITHOUT the roll touching it. Stated explicitly, because it decides what the resumed roll can be asked to
    # prove about this host: idempotency (below), NOT the agent-leg mechanism — which is asserted on $OTHERHOP,
    # where the roll genuinely has to perform the re-exec.
    assert_ok "PHASE-2 the repaired agent came back running the STAGED binary ($NEXTVER), so $FIRSTHOP is whole-host at target BEFORE the resumed roll — the operator's restart did that, not cluster upgrade"  poll_until 60 3 "$FIRSTHOP whole-host pre-resume" -- _whole_host_at "$FIRSTHOP" "$NEXTVER"
    assert_ok "start background raft write-probe (phase-2 window) in ctl1"  _start_write_probe p2
    _do_roll; _P2RC=$?; _dump_roll_log
    assert_ok "PHASE-2 the resumed roll COMPLETED (rc=0). Under R8 that is more than 'every host reported the target': driveUpgrade also gates rc=0 on the leader's agent-confirmed home verdict and on never having lost the roll lock mid-roll"  sh -c "[ '$_P2RC' = 0 ]"
    assert_ok "PHASE-2 [UNLOCK TERMINAL PROOF] the resumed roll got PAST acquire-lock — no 'membership operation in flight' anywhere in its log. This, not the unlock command's own success message, is the proof membership was really unfenced"  _roll_not_blocked
    assert_ok "PHASE-2 [whole-host] every broker's RUNNING (self-reported) version == $NEXTVER after the roll"  poll_until 60 3 "all brokers on next" -- _all_on_next
    assert_ok "PHASE-2 [MECHANISM — the roll's OWN agent leg] cluster upgrade re-exec'd $OTHERHOP's co-located agent and declared that host '(broker+agent) at $NEXTVER' in its own output. Without this the drill would only be reading END STATES, and an end state can be reached without the roll: a restarted daemon picks up the staged binary by itself. THIS is the whole-host mechanism actually executing"  _roll_reexeced_agent_of "$OTHERHOP"
    assert_ok "PHASE-2 [IDEMPOTENT RESUME] the resumed roll SKIPPED $FIRSTHOP as 'already at target' — a re-run does not re-restart daemons that are already current (cluster_upgrade_drive.go skips the agent leg only when it is ALREADY at target), which is what makes 'fix and re-run to resume' safe advice"  _roll_skipped "$FIRSTHOP"
    assert_ok "PHASE-2 [WHOLE-HOST (a)] $FIRSTHOP is done by #19's ONLY criterion: broker AND its DECLARED co-located agent $DECL_NID are both at $NEXTVER (OQ-6 — never once asserted before this batch, because the sim ran no co-located agent)"  poll_until 90 3 "$FIRSTHOP whole-host at next" -- _whole_host_at "$FIRSTHOP" "$NEXTVER"
    assert_ok "PHASE-2 [WHOLE-HOST (b)] $OTHERHOP is done on both legs too, with its agent discovered by OBSERVATION (nid==node_id) rather than declaration"  poll_until 90 3 "$OTHERHOP whole-host at next" -- _whole_host_at "$OTHERHOP" "$NEXTVER"
    assert_ok "PHASE-2 [WHOLE-HOST] $FIRSTHOP's skew is CLEARED (skew=false) — the half-upgraded state phase 1 pinned is gone, on the same oracle that reported it"  _field_is "$FIRSTHOP" skew false
    assert_ok "PHASE-2 [WHOLE-HOST] $OTHERHOP reports no skew either"  _field_is "$OTHERHOP" skew false
    assert_ok "PHASE-2 [(c) BROKER-ONLY] the agentless leader $LDR reached $NEXTVER on its broker leg and STILL reports agent_ver='?' — a broker-only host is done with no agent leg at all, and the roll never invented one"  _host_pair_is "$LDR" "$NEXTVER|?"
    assert_ok "PHASE-2 [re-exec] tether-broker MainPID UNCHANGED across BOTH rolls (syscall.Exec in place, not a kill/restart) — brk1=$MP1 brk2=$MP2 brk3=$MP3" \
              sh -c "[ \"\$(\"$SIM\" exec brk1 -- systemctl show tether-broker -p MainPID --value)\" = '$MP1' ] && [ \"\$(\"$SIM\" exec brk2 -- systemctl show tether-broker -p MainPID --value)\" = '$MP2' ] && [ \"\$(\"$SIM\" exec brk3 -- systemctl show tether-broker -p MainPID --value)\" = '$MP3' ]"
    assert_ok "PHASE-2 CONTINUITY the write-probe saw NO not_leader / 503 / no-responders across the completing roll (M1, independent phase-2 window — it cannot pass on phase-1's samples)"  _probe_clean p2
    _replay_scene p2   # uncaptured: surface the full first-hit scene into the transcript if one was captured
else
    _stop_write_probe p1
    not_covered "30 the G5 roll MECHANISM (per-hop advance, whole-host dual-version, skew, cluster unlock recovery, PID-preserving re-exec, write continuity) THIS RUN" "PHASE-1 did not produce the P3 (b)-state HALT it constructs (see the RED / PRODUCT-RED above and the verbatim roll.log). Every assertion in the block below depends on the roll having stopped exactly where the fixture aimed it; running them anyway would attribute an unrelated failure's partial state to the G5 mechanism." gap
fi

# Stage-C mandate-2/cluster-2 stated-reason ledger. (a) — the co-located-agent whole-host leg — is RETIRED:
# OQ-6 was adjudicated a SUPPLY gap and drills/lib/colocated.sh now supplies it, so the leg is asserted above
# instead of being declared uncoverable.
if [ "${HALTED_ON_AGENT:-0}" = 1 ]; then
    _NC_B="the write-probe's non-vacuity is enforced structurally (WROTE-* guard in _probe_clean: it FAILS if the probe never committed a write), and BOTH rolls actually ran this run, so continuity was genuinely judged on the failure path AND the happy path. What 30-C would add on top is a NEGATIVE control — an N=2 write-fence proving the probe can go RED when raft writes really do stall — and THAT is what remains uncovered here"
else
    _NC_B="the write-probe's non-vacuity is enforced structurally (WROTE-* guard in _probe_clean), AND the continuity assertions only run when the roll reached the fixture's HALT point; it did not this run, so they are SUPPRESSED and 30-C's negative control has nothing to guard — dropping it here is honest, not a hidden vacuity"
fi
not_covered "30 (with stated reasons — Stage-C mandate-2/cluster-2): (b) Arm 30-C N=2 write-fence NEGATIVE CONTROL" \
    "$_NC_B" gap
not_covered "30 (with stated reasons — Stage-C mandate-2/cluster-2): (c) mid-run HALT/resume of the LEADER hop" \
    "the HALT this drill constructs is at the FIRST hop, deliberately: that is the only partial state whose per-hop expectation is deterministic. A leader-hop HALT would additionally have to survive the leadership transfer the plan performs first, and the sim has no way to interrupt the roll at that instant. What IS covered when the fixture lands: G5 roll ORDER (leader-last), per-hop advance, whole-host dual-version across all three P3 presence states, the (b)-state HALT + its skew verdict, cluster unlock recovery with a terminal re-roll proof, PID-preserving re-exec, and raft-write continuity across BOTH the failed and the completing roll." gap
drill_end
