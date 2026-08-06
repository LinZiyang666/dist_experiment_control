#!/bin/sh
# 94-agent-reconcile — S9 / G-C. N=1 cluster + 2 agents: G.1 reconciliation on agent re-registration —
# the missed-exit direction, the ORPHAN direction (manufactured entirely through product paths), the G.5
# audit trail (history --kind proc/port), and `ps` LOST.
# Plan: docs/reviews/s7-s9-plan.md §3.1. Expected landing: GREEN (#61 candidate).
# Runtime ~11min. Topology: 1 broker + 2 agents + 1 ctl (N=1-family — parallelises freely, no grow).
#
# ── TWO STRUCTURAL FACTS THAT SHAPE THIS DRILL (both source-verified; the roadmap draft had them wrong) ─
#
# 1. `exec` CANNOT PRODUCE AN ORPHAN. internal/agent/agent.go:1133-1136 says verbatim: "Only PTY sessions
#    are reachable from a.procs; exec children are sync-managed … v1 doesn't track". The sole writer of
#    a.procs is run.go:229's registerProc. So the orphan arm MUST use `tether run` (a real PTY). This is
#    NOT a defect — it is v1's stated design — so it is not a gotcha. But it IS written here, because
#    anyone "simplifying" the orphan arm to `exec` later would produce a structurally false green.
#    (The roadmap's `exec sleep 9999` only applies to the OTHER direction: missed-exit -> EXITED(-1).)
#
# 2. THE AUDIT `pid` IS A ULID, NOT AN OS PID. internal/broker/exec.go:415 documents the audit payload as
#    {"pid": "01H..."} — tether's own process id, which is what `ps --json` reports as `.processes[].pid`.
#    The OS pid from `pgrep` is a DIFFERENT identifier. Grepping history for an OS pid would never match,
#    and the arm would fail for a reason that has nothing to do with reconciliation. Both identifiers are
#    captured here and used for their own purpose: the ULID for tether's audit rows, the OS pid for the
#    OS-truth legs.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. See fact 1 above: the orphan carrier must be `run`, never `exec`.
#  2. `node ls` RETURNING TO ONLINE IS NOT EVIDENCE OF RE-REGISTRATION. The old bundle still contains
#     agt1's node row, and heartbeats (broker.go:1289-1302, every 5s) write livenessDB DIRECTLY — flipping
#     it back to ONLINE with ZERO register calls. The primary oracle is the agent's own
#     `agent: re-registered after reconnect` line (proxy.go:486), pinned to that exact string rather than
#     the looser `agent: registered` (which the stuck-reconnect rebuild path at roster.go:485-486 also
#     emits).
#  3. RESTARTING THE BROKER DOES NOT DISCONNECT THE AGENT. They are two independent processes; the agent's
#     NATS connection survives. Step 3b asserts this as a POSITIVE invariant, which simultaneously proves
#     step 4 (the nats-server restart) is not optional.
#  4. THREE INDEPENDENT TIMEOUTS (B1/B2/B3). Merging them would make "broker never came back", "agent never
#     re-registered" and "the directive never executed" indistinguishable.
#  5. `killed_orphan` CARRIES NO rc — proven in BOTH directions (rc=<nil> present, rc=<number> absent).
#     Product guarantee: exec.go:451-454 only sets RC for exit|reconciled_closed.
#  6. THE ORPHAN ARM DOES NOT END AT "the process is gone": B4 (the audit row) and B5 (the agent log) are
#     what show the kill was a RECONCILIATION rather than something else killing it.
#  7. THE CONTROL SOURCE MUST BE POLLED AND PAIRED (B7): prove X is alive -> assert Y is dead -> prove X is
#     STILL alive in the same window. Proving X only before Y does not exclude both dying together.
#  8. A CURSOR GATE is mandatory: without one we would match a re-registration line from an EARLIER
#     reconnect and call it evidence. Since h1 the agent's slog lives in its own file rather than the
#     journal, so the cursor is a BYTE OFFSET into that file (see _jcursor / _agent_slog_after).
#  9. THE 15-MINUTE PORT REVOKE TIMER (expose.go:484-485: reconcilePorts only revokes for a node OFFLINE
#     >15min) must not contaminate B7 — the backup->restore window is asserted to be < 300s.
# 10. RESTORE RUNS AS User=tether. That is broker-ops.md:621-626 (#6) and the product's own words
#     (offline.go:945-946) — not a sim convenience. The root-restore trap is drill 50-J1's subject; 94
#     neither repeats it nor covers for it.
# 11. R-NOSHC: never wrap a harness function in `sh -c` — the new shell has no functions, the command dies
#     as "not found", and under `!` that becomes a permanent true.
#
# ── WHY `restart nats-server` AND NOT THE PARTITION PRIMITIVE ───────────────────────────────────────
# We need the agent to lose and regain its NATS connection WHILE KEEPING its managed child alive.
# `systemctl restart nats-server` gives a clean TCP close -> the agent reconnects in seconds
# (MaxReconnects(-1), agent.go:1481-1490). A silent DROP would instead need nats.go's PingInterval (2min)
# x MaxPingsOut (2) ~= 4-6 minutes to notice. Restarting the AGENT is not an option at all: systemd's
# cgroup would kill the very child under test.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/vault.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/events.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/logs.sh"

SID=lab
PIN=949494
NURL="nats://brk1:4222"
EV_NURL="$NURL"
EVCAP=/tmp/94-events.jsonl
BK94=/var/lib/tether/bk-94-pre
INST="$INSTANCE"

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }

# LIVENESS, NOT HEALTH. `cluster status` returns a HEALTH exit code by design (0=healthy, 1=DEGRADED,
# 2=QUORUM_LOST, 3=FORCE_SINGLE — clusterstatus.go:66-101), and a restored/DR'd lone voter is DEGRADED
# FOREVER. Using the exit code as a liveness probe polls until timeout against a perfectly healthy broker
# and manufactures a FAKE product failure. Liveness = "it answered with parseable JSON".
_broker_ready() { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id != null' >/dev/null 2>&1; }
# AGENT SLOG ORACLE — h1 moved this out from under journalctl.
#
# origin: docs/reviews/h1-external-review.md F3. Until h1 the agent's slog went
# to the journal (the sim unit sets no StandardOutput=, so it defaulted there)
# and this drill grepped `journalctl -u tether-agent`. h1 gives the agent its
# own size-capped file at $HOME/.tether/agent/<sid>/agent.log and leaves the
# journal carrying only panics + pre-logger boot output. The old oracle
# therefore found NOTHING and reported two assertion failures against a product
# that was behaving correctly — a failing oracle masquerading as a product
# verdict, which is the one thing this harness must never do.
#
# The MAPPING now lives in drills/lib/logs.sh and these are thin aliases. Two
# copies of a path mapping is precisely how F3 happened: the move updated one
# and left the other reading a file nothing writes to any more.
#
# The cursor is a BYTE OFFSET into that file rather than a timestamp: it needs
# no clock-format agreement, and it cannot let a line written BEFORE the cursor
# false-pass a later assertion (the exact failure mode the timestamp gate
# replaced --after-cursor for).
_jcursor()          { sim_agent_slog_cursor "$1"; }
# _agent_slog_after: does the agent's slog contain $3 in the bytes written
# AFTER offset $2? Reads the native log file (h1 contract), not the journal.
_agent_slog_after() { sim_agent_slog_grep "$1" "$3" "$2"; }
# _agent_panic_journal: the OTHER half of the h1 split — panics and pre-logger
# boot output still land on the journal via raw fd 2. Kept separate on purpose:
# blurring the two oracles would let a slog assertion pass on a panic line.
_agent_panic_journal() { dexec "$1" -- sh -c "journalctl -u tether-agent --since='$2' --no-pager 2>/dev/null | grep -qF '$3'"; }

_node_status() { "$SIM" ctl -- node ls -a --json 2>/dev/null | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.status // empty'; }
# `node ls --json` uses `nid` — NOT `node_id`. (`node_id` is `cluster status --json`'s field; the two
# APIs differ, proto/messages.go:379-385 vs the cluster status report.) Querying the wrong key silently
# matches nothing, so a perfectly ONLINE agent polls to timeout and looks like a product failure.
_agt1_online()  { [ "$(_node_status agt1)" = ONLINE ]; }
_agt1_offline() { [ "$(_node_status agt1)" = OFFLINE ]; }
_agt1_stale_or_offline() { _s=$(_node_status agt1); [ "$_s" = STALE ] || [ "$_s" = OFFLINE ]; }

# ps --json schema (SB-94-PSJSON, closed by reading proto/messages.go:410-418):
#   .processes[] = {pid (ULID), nid, argv[], status: RUNNING|EXITED|LOST, exit_code}
_ps_status() { "$SIM" ctl -- ps -a --json 2>/dev/null | jq -r --arg p "$1" '.processes[]?|select(.pid==$p)|.status // empty'; }
_ps_exit()   { "$SIM" ctl -- ps -a --json 2>/dev/null | jq -r --arg p "$1" '.processes[]?|select(.pid==$p)|.exit_code // empty'; }
_ulid_of()   { "$SIM" ctl -- ps -a --json 2>/dev/null | jq -r --arg n "$1" --arg c "$2" '[.processes[]?|select(.nid==$n)|select(.argv|join(" ")|test($c))][0].pid // empty'; }


# ── predicates (FUNCTIONS — R-NOSHC: `sh -c` inherits no harness functions) ─────────────────────────
_no_orph() { ! dexec agt1 -- pgrep -f 'sleep 9199' >/dev/null 2>&1; }
# FOREGROUND exec kept RUNNING by backgrounding the CTL-side call (drill 60:41). tether blocks on the
# process -> records it RUNNING -> killing the agent makes it a missed exit.
# Background the exec in the DRILL's own shell (drill 60:158's pattern), NOT inside a nested `sh -c` that
# exits and takes the process with it. Track PIDs so the trap can reap them.
_A0_PIDS=""
_a0_start() { "$SIM" ctl -- exec "$1" -- sleep "$2" >/dev/null 2>&1 & _A0_PIDS="$_A0_PIDS $!"; return 0; }
_a0_two_running() { "$SIM" ctl -- ps --json 2>/dev/null | jq -e '[.processes[]?|select(.nid=="agt1")|select(.argv|join(" ")|test("sleep 948[12]"))|select(.status=="RUNNING")]|length==2' >/dev/null 2>&1; }
_a0_ctrl_running() { "$SIM" ctl -- ps --json 2>/dev/null | jq -e '[.processes[]?|select(.nid=="agt2")|select(.argv|join(" ")|test("sleep 9483"))|select(.status=="RUNNING")]|length==1' >/dev/null 2>&1; }
_b2_start_pty_proc() {
    # A real PTY session via pty-run.py (S0-pty, drill 60's precedent). Backgrounded: `run` is interactive
    # and stays attached; we only need the child registered in a.procs.
    dexec -u sim ctl1 -- sh -c "nohup env HOME=/home/sim python3 /opt/sim/pty-run.py -- tether run agt1 -- sh -c 'printf ORPHREADY\\n; exec sleep 9199' >/tmp/94-run.log 2>&1 & echo started" >/dev/null 2>&1
}
_b2_proc_running() { "$SIM" ctl -- ps --json 2>/dev/null | jq -e '[.processes[]?|select(.nid=="agt1")|select(.argv|join(" ")|test("sleep 9199"))|select(.status=="RUNNING")]|length>=1' >/dev/null 2>&1; }
_b3_window_under_300s() { [ $(( $(date +%s) - BK_TS )) -lt 300 ]; }
_b3_no_reregister() { ! _agent_slog_after agt1 "$CUR0" 'agent: re-registered after reconnect'; }
_b4_orphan_row()   { "$SIM" ctl -- history --kind proc -n 100 2>/dev/null | grep -qF "PROC  $SID/agt1  pid=$PIDO  kind=killed_orphan  rc=<nil>"; }
# The NEGATIVE half of the no-rc guarantee: no killed_orphan row for this ULID may carry a numeric rc.
_b4_orphan_no_rc() { ! "$SIM" ctl -- history --kind proc -n 100 2>/dev/null | grep -qE "pid=$PIDO  kind=killed_orphan  rc=-?[0-9]+"; }
_b6_session_ok()   { "$SIM" ctl -- session ls --json 2>/dev/null | jq -e --arg s "$SID" '[.sessions[]?|select(.name==$s)]|length==1' >/dev/null 2>&1; }
_b6_roster_exact() { [ "$(dexec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db 'select group_concat(node_id) from cluster_nodes' 2>/dev/null | tr -d '\r')" = brk1 ]; }
_b7_port_row()     { "$SIM" ctl -- history --kind port -n 100 2>/dev/null | grep -qF "PORT  $SID/agt1  port=$PY  name=web2  kind=reconciled"; }

_cleanup() {
    for _pp in $_A0_PIDS; do kill "$_pp" 2>/dev/null || true; done
    ev_stop ctl1 || true
    dexec brk1 -- systemctl start nats-server tether-broker >/dev/null 2>&1 || true
    true
}

drill_begin "94-agent-reconcile (N=1 + 2 agents: G.1 missed-exit + orphan via product paths + G.5 audit + ps LOST)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

# ── SETUP (N=1 CLUSTER mode: backup/restore only accept a cluster bundle, restore.go:91-93; and a bare
#    P2 broker does not serve NATS sessions at all — SB-43 established that) ──────────────────────────
assert_setup "up 1 broker + 2 agents + 1 ctl"                "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_setup "init brk1 (standalone -> N=1 cluster)"         "$SIM" init brk1
assert_setup "vault init (AFTER cluster setup: any nuke on the way here would reap it)" vault_init
assert_setup "session $SID + ctl login"                      "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                               "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "agent-join agt2"                               "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (S0-tunnel)"         agent_provision_yaml agt1 "$SID" "$NURL" open
assert_setup "provision agt2 agent.yaml (S0-tunnel)"         agent_provision_yaml agt2 "$SID" "$NURL" open
assert_setup "sys.events observer up (member core-sub — permissions.go:147 allows it; 80/81 have done this for two batches)" \
    ev_sub_start ctl1 "$SID" "$EVCAP"
assert_setup "the event subscription is really established (a missed event must not be confusable with a late sub)" \
    poll_until 20 2 "sys.events sub ready" -- ev_ready ctl1 "$EVCAP"

# ══ ARM GROUP A — G.1 missed-exit (exec + docker kill agent) ════════════════════════════════════════
# FOREGROUND exec, backgrounded on the CTL side (drill 60:41): tether blocks on the process and records
# it RUNNING, so killing the agent leaves it as a MISSED EXIT to reconcile. A `nohup … &` on the AGENT
# side instead makes the sh -c wrapper exit 0 with the sleep orphaned OUTSIDE tether's view — recorded as
# kind=exit rc=0, so there is nothing to reconcile (diagnosed live this session: argv was the wrapper,
# exit_code was null not -1, and A2 failed for that reason, not a product one).
assert_ok "A0a exec sleep 9481 on agt1 as a FOREGROUND process (ctl-side backgrounded — tether tracks it RUNNING)" _a0_start agt1 9481
assert_ok "A0b exec sleep 9482 on agt1 (#2)" _a0_start agt1 9482
assert_ok "A0c exec sleep 9483 on agt2 (THE CONTROL: it must survive agt1's reconciliation untouched)" _a0_start agt2 9483
assert_ok "A0d the BROKER sees both agt1 procs RUNNING (tracked, not orphaned outside its view — this is what makes a missed-exit possible)" \
    poll_until 30 2 "agt1 has two RUNNING procs in ps" -- _a0_two_running
assert_ok "A0e and agt2's child is RUNNING in the broker's view too" \
    poll_until 30 2 "agt2 has a RUNNING proc" -- _a0_ctrl_running

# ── A1 — ps LOST. Slow by nature: StaleAfter/OfflineAfter have NO broker.yaml or flag knob (DOC-24),
#    so the drill can only wait. LOST is derived AT READ TIME (exec.go:326-345) and never persisted.
assert_ok "A1a docker kill agt1 (power-loss class: the whole container dies)" node_kill agt1
assert_ok "A1b agt1 leaves ONLINE (heartbeat stops)" poll_until 40 3 "agt1 STALE or OFFLINE" -- _agt1_stale_or_offline
assert_ok "A1c agt1 reaches OFFLINE" poll_until 90 3 "agt1 OFFLINE" -- _agt1_offline

# ── A1d/e/f — D6: the `ps` LOST DERIVED state, now ASSERTED (was an overclaim: the header/title named
# "ps LOST" but the drill only checked NODE status, never a proc's LOST). exec.go:326-345: a storage-RUNNING
# proc whose OWNING NODE is OFFLINE reads back as LOST in `tether ps`; the SQLite row stays RUNNING (LOST is
# never persisted). agt1's two exec children (seeded RUNNING in A0, PROVEN RUNNING in A0d) are still
# storage-RUNNING and agt1 is OFFLINE now, so ps MUST derive LOST for them — while agt2's child stays
# RUNNING because agt2 is ONLINE. Resolve the ULIDs WHILE agt1 is OFFLINE (the rows are still in the proc
# table). This closes the RUNNING(A0d) → LOST(here) → EXITED(A2) three-state chain, so none of the three is
# vacuous. The ULIDs are re-resolved after B0b for arm A2 (they are stable).
LOST1=$(_ulid_of agt1 'sleep 9481'); LOST2=$(_ulid_of agt1 'sleep 9482'); LCTRL=$(_ulid_of agt2 'sleep 9483')
[ -n "$LOST1" ] && [ -n "$LOST2" ] && [ -n "$LCTRL" ] || setup_fail "could not resolve the seeded ULIDs while agt1 is OFFLINE (needed for the ps LOST derivation — the proc rows must still be readable)"
_a1_lost1() { [ "$(_ps_status "$LOST1")" = LOST ]; }
_a1_lost2() { [ "$(_ps_status "$LOST2")" = LOST ]; }
_a1_ctrl_running() { [ "$(_ps_status "$LCTRL")" = RUNNING ]; }
assert_ok "A1d ps LOST: agt1's first exec child is DERIVED LOST now that agt1 is OFFLINE (storage-RUNNING row + OFFLINE owning node -> LOST at read time, exec.go:326-345 — NOT persisted)" \
    poll_until 60 3 "agt1 proc #1 derives LOST in ps" -- _a1_lost1
assert_ok "A1e ps LOST: agt1's second exec child is DERIVED LOST too" \
    poll_until 30 3 "agt1 proc #2 derives LOST in ps" -- _a1_lost2
assert_ok "A1f DISCRIMINATOR: agt2's child is STILL RUNNING (agt2 is ONLINE) — LOST is per-owning-node-status, not a table-wide relabel; without this the LOST above could be a blanket mislabel of every RUNNING row" \
    _a1_ctrl_running

# ══ ARM GROUP B — the ORPHAN, manufactured entirely through product paths ═══════════════════════════
# The roadmap's original recipe (SIGSTOP the broker, then start a process) is UNREACHABLE through the
# product: `run`/`exec` are both ctl -> BROKER -> agent forwards (broker.go:831-832,837), so with the
# broker stopped the process never starts at all. That is DOC-4's correction, delivered here.
# The reachable recipe is: backup -> start a PTY-managed process -> stop broker -> restore the OLDER
# bundle (a legitimate product-level rollback to an earlier committed state) -> start broker -> force the
# agent to reconnect -> on re-register the broker sees a process it has no record of -> orphan.
assert_ok "B0a bring agt1 back" node_start agt1
assert_ok "B0b agt1 is ONLINE again" poll_until 90 3 "agt1 back ONLINE" -- _agt1_online

# G.1's missed-exit direction resolves on THIS re-registration.
PIDA1=$(_ulid_of agt1 'sleep 9481')
PIDA2=$(_ulid_of agt1 'sleep 9482')
PIDB=$(_ulid_of agt2 'sleep 9483')
[ -n "$PIDA1" ] && [ -n "$PIDA2" ] && [ -n "$PIDB" ] || setup_fail "could not resolve the ULIDs of the seeded processes from ps --json"

_a2_both_exited() { [ "$(_ps_status "$PIDA1")" = EXITED ] && [ "$(_ps_status "$PIDA2")" = EXITED ]; }
_a2_ctrl_running() { [ "$(_ps_status "$PIDB")" = RUNNING ]; }
_a2_exit_minus1() { [ "$(_ps_exit "$PIDA1")" = "-1" ] && [ "$(_ps_exit "$PIDA2")" = "-1" ]; }
# DIAGNOSTIC (top level, not inside an assert — R-DIAG-OUTSIDE): dump the real ps + history shapes so a
# field-name / value mismatch is visible rather than a bare "want 0 got 1".
log "A2-diag ps --json (agt1 procs):"; "$SIM" ctl -- ps -a --json 2>/dev/null | jq -c '[.processes[]?|select(.nid=="agt1")|{pid,status,exit_code,argv:(.argv|join(" "))}]' >&2 || true
log "A2-diag history --kind proc (last 8):"; "$SIM" ctl -- history --kind proc -n 8 2>&1 | tail -8 >&2 || true
assert_ok "A2a G.1: agt1's two processes are reconciled to EXITED after re-registration" \
    poll_until 60 3 "both of agt1's procs are EXITED" -- _a2_both_exited
assert_ok "A2b they carry exit_code -1 (the synthetic 'we never saw it exit' marker)" _a2_exit_minus1
# THE DISCRIMINATOR: reconciliation is node-scoped. If agt2's process also died, this would be a
# table-wide massacre wearing reconciliation's clothes.
assert_ok "A2c agt2's process is STILL RUNNING — reconciliation is node-scoped, not a table-wide sweep" _a2_ctrl_running
assert_ok "A2d the agent_registered event was captured (inventory row 30, delivered by the inventory's own oracle)" \
    ev_seen ctl1 "$EVCAP" agent_registered

# ── A3 — G.5: the audit trail. The ONLY way an operator can answer "why did my process suddenly show
#    EXITED(-1)?". Two literal spaces between fields: history.go:573-576 Fprintf's them directly and does
#    NOT pass through a tabwriter. The pid here is the ULID (see fact 2 in the headnote).
_a3_proc_row()     { "$SIM" ctl -- history --kind proc -n 100 2>/dev/null | grep -qF "PROC  $SID/agt1  pid=$PIDA1  kind=reconciled_closed  rc=-1"; }
_a3_no_ctrl_row()  { ! "$SIM" ctl -- history --kind proc -n 100 2>/dev/null | grep -qF "pid=$PIDB  kind=reconciled_closed"; }
_a3_reader_alive() { _a3_out=$("$SIM" ctl -- history --kind proc -n 100 2>&1); [ -n "$_a3_out" ]; }
assert_ok "A3a the audit reader itself works (absence below is only evidence if the reader is alive)" _a3_reader_alive
assert_ok "A3b G.5: history --kind proc carries kind=reconciled_closed rc=-1 for agt1's process" \
    poll_until 30 3 "the reconciled_closed audit row lands" -- _a3_proc_row
assert_ok "A3c and NOT for agt2's untouched process (paired with A3b: node-scoped, not table-wide)" _a3_no_ctrl_row

# ── B — the orphan proper ───────────────────────────────────────────────────────────────────────────
assert_ok "B1a take a backup BEFORE the orphan process exists (as User=tether — broker-ops.md:621-626 #6)" \
    out_matches 'online backup complete: .*self=brk1, source=leader' \
    _bt brk1 -- tether cluster backup --out "$BK94"
BK_TS=$(date +%s)

# THE ORPHAN CARRIER MUST BE `run` (a real PTY): only PTY sessions land in a.procs (agent.go:1133-1136).
assert_ok "B2a start a PTY-managed process via 'tether run' (exec CANNOT produce an orphan — agent.go:1133-1136)" \
    _b2_start_pty_proc
assert_ok "B2b the broker sees it RUNNING" poll_until 30 2 "the PTY proc shows RUNNING" -- _b2_proc_running
assert_ok "B2c and the OS really is running it (broker view + OS truth, double-proven — this one has to SURVIVE the injection, unlike the double-fault arm)" \
    dexec agt1 -- pgrep -f 'sleep 9199'
PIDO=$(_ulid_of agt1 'sleep 9199')
[ -n "$PIDO" ] || setup_fail "could not resolve the orphan-to-be's ULID"

# Also seed a port AFTER the backup, for the G.5 port-reconcile leg (B7).
TOKY=$(expose_serve_sentinel agt1 8094) || setup_fail "could not start the web2 sentinel on agt1"
assert_ok "B2d expose a SECOND port after the backup (its row will be absent from the restored DB)" \
    "$SIM" ctl -- expose agt1 --local 8094 --name web2
PY=$("$SIM" ctl -- expose explain web2 --json 2>/dev/null | jq -r '.public_port // empty')
[ -n "$PY" ] || setup_fail "could not read web2's public port"

CUR0=$(_jcursor agt1)
assert_ok "B3a stop the broker (an OPERATOR stop: Restart=always does not fight us)" \
    dexec brk1 -- systemctl stop tether-broker
assert_ok "B3b restore the OLDER bundle as User=tether (a product-level rollback to an earlier committed state)" \
    out_matches 'restore complete: node brk1 is now a single-voter cluster' \
    _pty brk1 brk1 -- tether cluster recovery restore "$BK94" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets
assert_ok "B3c the restore happened well inside the 15-min port-revoke timer (expose.go:484-485) — otherwise B7 would be contaminated" \
    _b3_window_under_300s
assert_ok "B3d start the broker again" dexec brk1 -- systemctl start nats-server tether-broker
assert_ok "B3e the broker is ready" poll_until 90 3 "brk1 ready after the rollback" -- _broker_ready

# ── B3b-invariant — THE ANTI-FALSE-GREEN LOAD-BEARER ────────────────────────────────────────────────
# Restarting the BROKER does not disturb the AGENT's NATS connection (two independent processes). Asserted
# as a positive invariant, this simultaneously proves that step 4 below is NOT optional.
assert_ok "B3f the orphan process is STILL ALIVE after the broker rollback (it has to survive the injection for the arm to mean anything)" \
    dexec agt1 -- pgrep -f 'sleep 9199'
assert_ok "B3g INVARIANT: restarting the BROKER did NOT make the agent re-register (they are independent processes — which is exactly why the nats-server restart below is not optional)" \
    _b3_no_reregister

# ── step 4 — force a reconnect that PRESERVES the managed child ─────────────────────────────────────
CUR=$(_jcursor agt1)
assert_ok "B4a restart nats-server (NEVER 'systemctl restart tether-agent': systemd's cgroup would kill the very child under test)" \
    dexec brk1 -- systemctl restart nats-server

# Three INDEPENDENT timeouts — merging them would blur three different failures into one.
assert_ok "B1-timeout the broker is reachable again" poll_until 60 2 "broker ready post nats restart" -- _broker_ready
# DIAGNOSTIC (R-DIAG-OUTSIDE): what did the agent actually do after the nats restart?
log "B2-diag agent slog after nats restart:"; sim_agent_slog_tail agt1 15 >&2 || true
assert_ok "B2-timeout PRIMARY: the agent RE-REGISTERED (proxy.go:486's exact line, gated on a journal cursor). 'node ls' going ONLINE is explicitly NOT accepted: the restored bundle still holds agt1's node row, so heartbeats alone flip it back with zero register calls" \
    poll_until 60 2 "agt1 logs 'agent: re-registered after reconnect'" -- _agent_slog_after agt1 "$CUR" 'agent: re-registered after reconnect'
assert_ok "B3-timeout the orphan was KILLED (the broker returned a drop directive for a process it has no record of)" \
    poll_until 30 1 "the orphan process is gone" -- _no_orph

# ── B4/B5 — the arm does NOT end at "the process is gone" ───────────────────────────────────────────
assert_ok "B5 the agent logged the orphan kill (this line IS the evidence the drop directive was received AND acted on; the reconnect path prints no drop_procs count — DOC-25)" \
    _agent_slog_after agt1 "$CUR" 'agent: kill orphan'
assert_ok "B4a G.5: the killed_orphan audit row exists for the orphan's ULID" \
    poll_until 30 3 "the killed_orphan audit row lands" -- _b4_orphan_row
assert_ok "B4b and it carries NO rc (product guarantee: exec.go:451-454 sets RC only for exit|reconciled_closed) — asserted in BOTH directions" \
    _b4_orphan_no_rc

# ── B6 — unrelated rows are intact ──────────────────────────────────────────────────────────────────
assert_ok "B6a the session from the bundle is intact" _b6_session_ok
assert_ok "B6b the reconciled EXITED rows from arm group A are still there" _a2_both_exited
assert_ok "B6c #49-hardening: the roster is exactly {brk1} — a resurrected pruned peer would be a restore-side defect" \
    _b6_roster_exact

# ── B7 — G.5's port face + the PAIRED control source ────────────────────────────────────────────────
# web2 was created AFTER the backup, so the restored DB has no row for it while the agent still holds the
# token: register takes reconcile.go:280-283's !ok branch -> revoke + pubAuditPort(kind=reconciled).
assert_ok "B7a the port-reconcile audit row exists (kind=reconciled — note: 'reconciled' is AuditPort's kind; AuditProc's is 'reconciled_closed', schema/audit.go:36/:51)" \
    poll_until 60 3 "the port reconcile audit row lands" -- _b7_port_row
assert_ok "B7b DATA PLANE: web2's public port is really dead (curl exit 7)" \
    poll_until 60 3 "web2's port refused" -- dp_curl_refused ctl1 "http://brk1:$PY/"

drill_end
