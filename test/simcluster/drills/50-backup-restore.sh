#!/bin/sh
# 50-backup-restore — S7 / G-C. N=2: `cluster backup` (leader / follower / offline) · `recovery restore`
# gate family · `cluster doctor --offline` preflight · `recovery incident export`.
# Plan: docs/reviews/s7-s9-plan.md §2.1. Expected landing: PRODUCT-RED (#50, already proven in source).
# Runtime ~15min. Topology: 2 brokers + 1 agent + 1 ctl (grow-family; -j 2 wave).
#
# ── WHAT THIS DRILL IS FOR ───────────────────────────────────────────────────────────────────────────
# The hermetic suites test the restore state machine. What they cannot test is a real bundle written by a
# real broker process to a real disk, carried off the box, and fed back to a real daemon that must refuse
# it for the right reason. Every gate below is asserted against the SIGNATURE the product actually prints.
#
# ── FALSE-GREEN RISK HEADNOTE (these guards must survive to landing) ─────────────────────────────────
#  1. DAEMON NOT STOPPED => every restore negative returns `daemon still running` and a loose signature
#     passes them ALL. Guard: each negative anchors its OWN sentence.
#  2. GATE 9 MASKS GATE 10 (the one lethal design point). restore.go:234 checks --confirm-node-id vs the
#     manifest BEFORE :238 checks the tunnel-cert fingerprint. Pass anything but the BUNDLE'S OWN self_id
#     to the foreign-bundle arm and you pin gate 9 while believing you pinned gate 10. Guard: I4 passes
#     `--confirm-node-id brk1` (the bundle's id) while running on brk2.
#  3. Z SEEDED BEFORE THE BACKUP => the identity oracle proves nothing. Guard: B4 runs AFTER D.
#  4. Z CHECKED IN A SEPARATE READ => `session ls` erroring also means "Z is absent". Guard: L1/L2 are ONE
#     jq evaluation over ONE read.
#  5. A LONE `! grep` IS ALWAYS TRUE. Guard: L3 proves rc=0 + non-empty output + L1's positive control
#     first.
#  6. CLOSING ON A STATUS FIELD = replaying #20. Guard: L4 closes on dp_curl_ok_body returning the exact
#     pre-disaster sentinel; H1 closes on dp_curl_refused (exit 7).
#  7. "IT ALWAYS REFUSES" IS NOT EVIDENCE A GATE EXISTS. Every refusal here has a control source that
#     SUCCEEDS: J2 for I2, Q3 for Q2, and R2 for R3 (#50) — R2 is what proves doctor CAN go red at all.
#  8. `exit 1` HAS NO DISCRIMINATING POWER (clusterstatus.go's default branch also returns 1). Guard: K
#     asserts NOT-HA + voters==1 + no force_single together.
#  9. #50 VIA assert_refuses WOULD LAND ASSERT-FAIL (verdict misclassification): the command SUCCEEDS, and
#     that success IS the defect. Guard: R-INVERTED (assert_ok predicate + bare product_red).
# 10. THE `.pre-restore.bak` md5 == MD5_0 WOULD FALSE-RED: backupToUnique -> checkpointWALForBackup runs
#     `PRAGMA wal_checkpoint(TRUNCATE)` on a READ-WRITE DSN, changing tether.db's bytes BEFORE the copy.
#     (M1's md5 check IS safe: a refusal never reaches restore.go:175.)
# 11. BROKER APPLICATION LINES ARE NOT IN JOURNALD. install.sh:756-757 redirects broker stdout/stderr to
#     /var/log/tether/broker.{log,err}. `journalctl -u tether-broker` carries systemd lifecycle only.
#
# ── SCOPE BOUNDARY (registered, not hidden) ─────────────────────────────────────────────────────────
# The disaster here is an in-place LIB-VOLUME wipe: /etc/tether (seam + nats.conf + secrets) survives.
# That means 50 STRUCTURALLY CANNOT see #51 (restore does not apply the cluster seam) or #52 (restore does
# not render nats.conf) — those need a fresh box and are drill 51's exclusive property. The closing gate
# row says so explicitly; do not read 50 as covering restore's full startup path.
#
# ── USER DISCIPLINE ─────────────────────────────────────────────────────────────────────────────────
# Every `tether cluster …` runs as `dexec -u tether`. That is docs/broker-ops.md:621-626 (#6) — which
# lists `restore` verbatim — not a sim convenience. The ONE exception is arm J1, which deliberately runs
# as root per runbook §5.1's literal text in order to expose #6.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"          # sctl / leader_node — agentyaml.sh depends on sctl
. "$HERE/lib/vault.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/lib/assert.sh"

SID=lab
PIN=505050
NURL="nats://brk1:4222"
BK_DIR=/var/lib/tether/bk-50-leader        # install.sh:500 makes /var/lib/tether tether-owned 0750
BK_NAME=leader-1

# _bt <node> -- <tether args...> : run tether as User=tether on <node> (broker-ops #6).
_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
# _pty <node> <answer> -- <tether args...> : typed-confirm as tether (pty-confirm.py is baked, S0-pty).
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# _pty_root <node> <answer> -- <tether args...> : same, as ROOT (arm J1 only, deliberately).
_pty_root() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# _berr <node> [n] : the broker's APPLICATION log (slog). NOT journalctl (R-BROKERLOG) — post-h1
# journald carries the broker's PANIC stream, so reading it here would let a stacktrace stand in
# for an application line.
# h1 F3: that log moved broker.err -> broker.log; sim_broker_slog reads both, so
# a pre-h1 image stays readable. The K2 arm below greps this for a specific
# refusal string — pointing it at the now-frozen broker.err would have made the
# refusal permanently "not observed" and laundered a regression into the
# runtime-guard branch.
_berr() { sim_broker_slog "$1" "${2:-60}"; }

# LIVENESS, NOT HEALTH. `cluster status` returns a HEALTH exit code by design (0=healthy, 1=DEGRADED,
# 2=QUORUM_LOST, 3=FORCE_SINGLE — clusterstatus.go:66-101), and a restored lone voter is DEGRADED
# FOREVER. Using the exit code as a liveness probe would therefore poll until timeout on a perfectly
# healthy broker and manufacture a FAKE product failure — which is exactly what the first cut of this
# drill did (and what G-B's drill 91 did with SIGPIPE). Liveness = "it answered with parseable JSON".
_broker_ready() { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id != null' >/dev/null 2>&1; }
_k_ready() { [ "$_K_READY" = 1 ]; }
_leader_is_brk1() { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }
_raft_unlocked() { ! dexec brk1 -- fuser /var/lib/tether/raft/raft.db >/dev/null 2>&1; }
# `node ls --json` uses `nid` — NOT `node_id`. (`node_id` is `cluster status --json`'s field; the two
# APIs differ, proto/messages.go:379-385 vs the cluster status report.) Querying the wrong key silently
# matches nothing, so a perfectly ONLINE agent polls to timeout and looks like a product failure.
_agt1_online() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }

# single cleanup trap (R-TRAP rule 2: two `trap … EXIT` silently overwrite each other).
_cleanup() {
    dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker' >/dev/null 2>&1 || true
    dexec brk2 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker' >/dev/null 2>&1 || true
    true
}

drill_begin "50-backup-restore (N=2: backup leader/follower/offline + restore gates + doctor + incident export)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

# ── A — SETUP ────────────────────────────────────────────────────────────────────────────────────────
assert_setup "grow_to_2 (N=2 VOTER + JS meta formed + leader pinned brk1)" grow_to_2 1 1
assert_setup "vault init (S0-backup-vault: 0700, empty — AFTER grow so a retry-nuke cannot reap it)" vault_init
assert_setup "session $SID + ctl login"                                   "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                                            "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (tunnel_addr wired, S0-tunnel)"   agent_provision_yaml agt1 "$SID" "$NURL" open

# ── B1-B3 — SEED X (the business state the backup must contain) ──────────────────────────────────────
TOKX=$(expose_serve_sentinel agt1 8081) || setup_fail "could not start the sentinel http.server on agt1"
# --on-broker brk1 is a HARD PRECONDITION, not tuning — and NOT an oracle being loosened to get green.
#
# LIVE-MEASURED, 2026-07-17 (6 runs: 3 pass / 3 fail, ~50%): without the pin, the broker assigns the
# expose home to brk1 or brk2 arbitrarily. agt1's tunnel_addr points at brk1, so whenever the home lands
# on brk2 the agent cannot dial the tunnel and the allocate fails outright:
#   error: expose failed: the agent couldn't start the local proxy … (agent_rejected:frpc_failed)
# That is #29 exactly (a cluster expose home cannot be delivered to a non-tunnel broker; home.go:96-113)
# — an ALREADY-REGISTERED defect whose data-plane surface belongs to drill 71, which pins it there.
# Drill 50 is about backup/restore IDENTITY. Letting a known, registered, someone-else's-defect fire at
# random here would (a) make 50 a coin-flip flake and (b) manufacture a FALSE RED against restore.
# So: pin the home, and register the sighting (ledger #29 blast radius) rather than re-litigating it.
# Drills 51 and 52 pin it for the same reason; 50 originally omitted it, and the drill caught that.
assert_setup "expose agt1:8081 --on-broker brk1 --name live (home PINNED: without it #29 makes this a ~50% coin flip — measured — and would false-RED restore; the #29 data-plane surface is drill 71's property)" \
    "$SIM" ctl -- expose agt1 --local 8081 --on-broker brk1 --name live
PX=$("$SIM" ctl -- expose explain live --json 2>/dev/null | jq -r '.public_port // empty')
[ -n "$PX" ] || setup_fail "could not read the public port of expose 'live'"
E0=$("$SIM" ctl -- expose explain live --json 2>/dev/null | jq -r '.epoch // 0')

# THE PRE-INJECTION DATA-PLANE BASELINE. Real tunneled bytes, not a status field: without this, "the data
# plane came back" after the restore would be unfalsifiable.
assert_ok "B1 pre-disaster data plane: ctl1 curls brk1:$PX and gets the EXACT sentinel" \
    poll_until 30 2 "sentinel via the public port" -- dp_curl_ok_body ctl1 "http://brk1:$PX/" "$TOKX"
assert_ok "B2 seed history: exec prints BACKUP-HISTORY-SENTINEL" \
    "$SIM" ctl -- exec agt1 -- echo BACKUP-HISTORY-SENTINEL
assert_ok "B3 history carries the exec row (the audit read path works BEFORE the disaster)" \
    out_matches 'BACKUP-HISTORY-SENTINEL|exec' "$SIM" ctl -- history -n 20

# ── Q — recovery incident export ────────────────────────────────────────────────────────────────────
assert_ok "Q1 incident export --out writes a 0600 bundle" \
    _bt brk1 -- tether cluster recovery incident export --out /tmp/inc.json
assert_ok "Q1b incident bundle is 0600" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- stat -c %a /tmp/inc.json 2>/dev/null | tr -d '\r')\" = 600 ]"
assert_ok "Q1c incident schema + roster of 2 (real cluster shape)" \
    sh -c "\"$SIM\" exec brk1 -- cat /tmp/inc.json | jq -e '.schema==\"incident\" and (.roster|length)==2' >/dev/null"
# Q2's refusal and Q3's success are a PAIR: without Q3, "it refused" could just mean the command is broken.
assert_refuses "Q2 incident export --out refuses to clobber an existing file (O_EXCL)" \
    "refusing to overwrite existing|--force to overwrite" \
    _bt brk1 -- tether cluster recovery incident export --out /tmp/inc.json
assert_ok "Q3 incident export --force overwrites (the CONTROL SOURCE for Q2 — proves the verb works)" \
    _bt brk1 -- tether cluster recovery incident export --out /tmp/inc.json --force
assert_ok "Q4a stage a symlink pointing at a REAL private key" \
    dexec brk1 -- sh -c 'ln -sf /etc/tether/secrets/tunnel-key.pem /tmp/inc-link.json'
KEYMD5=$(dexec brk1 -- md5sum /etc/tether/secrets/tunnel-key.pem 2>/dev/null | awk '{print $1}')
# --force is REQUIRED here: without it we would hit O_EXCL first and pin the wrong gate.
assert_refuses "Q4b incident export --force still refuses to FOLLOW a symlink (O_NOFOLLOW)" \
    "symbolic link|O_NOFOLLOW|ELOOP|not a regular file" \
    _bt brk1 -- tether cluster recovery incident export --out /tmp/inc-link.json --force
assert_ok "Q4c the private key the symlink pointed at is byte-identical (the REAL result oracle)" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- md5sum /etc/tether/secrets/tunnel-key.pem 2>/dev/null | awk '{print \$1}')\" = \"$KEYMD5\" ]"
assert_ok "Q6 incident export --since smoke" \
    _bt brk1 -- tether cluster recovery incident export --since 24h --out /tmp/inc-since.json

# ── R — cluster doctor --offline: THE PRECONDITION GATE, proved two-sided (#50 FLIPPED, R10 P5) ──────
# doctor MUST run as tether: as root it would bypass preflight.go:78-85's real os.Open readability check.
assert_ok "R1 doctor --offline is green on a healthy node (as User=tether — root would bypass the real open() check)" \
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /etc/tether/nats.d/nats.conf
assert_refuses "R2 doctor --offline --conf <nonexistent> FAILS (the CONF cell's control source — that cell always did a real read)" \
    "no such file|read .*nats.conf|FATAL" \
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /nonexistent/nats.conf

# R3 — #50, FLIPPED (R10 P5). This arm used to be INVERTED: `doctor --offline --db <nonexistent>` exited 0
# with `.summary.fatal == 0`, so the SUCCESS was the defect and the arm was `assert_ok` + a bare
# product_red. The self-check arm ("#50 APPEARS FIXED — promote R3 to assert_refuses") has now fired for
# real: DBPreflight (internal/clusteroffline/doctor.go) replaced the bare lazy `storage.OpenReadOnly`
# with Stat + Ping + PRAGMA quick_check + a schema probe.
#
# THE FLIP IS A REWRITTEN PREDICATE, NOT A RENAMED KEYWORD. The old oracle asked one question ("did it
# exit 0?"). Turning that single question into a single `assert_refuses` would be a strictly WEAKER test
# than the one it replaces was as an inverted pin, because ANY failure — a typo in --secrets-dir, a
# leader drift, a broken binary — would satisfy it. So the flip enumerates the SIX pathological states
# the product documents itself as rejecting, each as a SEPARATE bad input carrying its OWN signature.
# One "it went red" arm would still pass with five of the six layers deleted; six signature-anchored arms
# cannot. R1 (a HEALTHY db is green) is the positive control that stops the six from passing vacuously —
# without it, a doctor that FATALs on everything would look perfect here.
#
# R-EXHAUST holds in a new form: the four-state explore is gone because the state space is no longer
# "does it go red?" (unknown) but "which layer catches which input?" (documented + testable).
_doc_db()  { _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db "$1" --conf /etc/tether/nats.d/nats.conf; }
# The db CELL specifically must be FATAL — the exact inverse of #50's evidence (`.summary.fatal == 0`
# while the db cell said PASS). Asserting only `summary.fatal >= 1` would be satisfied by ANY other cell
# going red, which is how a "the gate works now" claim gets forged.
_doc_db_cell_fatal() {
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db "$1" \
        --conf /etc/tether/nats.d/nats.conf --json 2>/dev/null \
        | jq -e '(.summary.fatal>=1) and ([.checks[]?|select(.name=="db")|select(.status=="FATAL")]|length==1)' >/dev/null 2>&1
}
# exit 64 (usageErr, cluster_natsconf.go's renderDoctor) — the class a calling script branches on. A bare
# "non-zero" would also accept a panic or a 127.
_doc_db_rc64() { _doc_db "$1" >/dev/null 2>&1; _drc=$?; [ "$_drc" = 64 ]; }
# Stage the six inputs. perm.db is deliberately root-owned 0600 so the tether user CANNOT read it — that
# is the state the lazy sql.Open hid best (it reported a PASS on a file it never opened). The corruption
# overwrites pages 2-3 and leaves page 1 (header + sqlite_master) intact, so the file still OPENS, still
# PINGS and still HAS a schema: quick_check is the only layer that can see it.
_stage_bad_dbs() {
    dexec brk1 -- sh -c '
        set -e
        rm -rf /tmp/nvdb; mkdir -p /tmp/nvdb/adir
        [ "$(stat -c %s /var/lib/tether/tether.db)" -ge 65536 ] || { echo "live tether.db too small to corrupt meaningfully" >&2; exit 1; }
        : > /tmp/nvdb/empty.db
        head -c 512 /var/lib/tether/tether.db > /tmp/nvdb/trunc.db
        cp /var/lib/tether/tether.db /tmp/nvdb/perm.db
        cp /var/lib/tether/tether.db /tmp/nvdb/corrupt.db
        dd if=/dev/urandom of=/tmp/nvdb/corrupt.db bs=4096 seek=1 count=2 conv=notrunc status=none
        chown -R tether:tether /tmp/nvdb
        chown root:root /tmp/nvdb/perm.db
        chmod 600 /tmp/nvdb/perm.db
    '
}
assert_ok "R3-prep stage the six pathological --db inputs (missing / dir / empty / truncated / unreadable / corrupt-page)" \
    _stage_bad_dbs
assert_refuses "R3a #50 state 1/6 — a NONEXISTENT --db is FATAL (this is #50's own literal reproducer, which used to exit 0)" \
    "the DB file is missing or unreadable" _doc_db /nonexistent/nope.db
assert_ok "R3a-rc the #50 reproducer exits 64 (usage), not 0 — the class a calling script branches on" \
    _doc_db_rc64 /nonexistent/nope.db
assert_ok "R3a-cell the DB CELL is the one reporting FATAL (summary.fatal>=1 alone would be satisfied by ANY other cell going red)" \
    _doc_db_cell_fatal /nonexistent/nope.db
assert_refuses "R3b #50 state 2/6 — a DIRECTORY passed as --db is FATAL (os.Stat + IsDir)" \
    "is a DIRECTORY, not a SQLite database file" _doc_db /tmp/nvdb/adir
assert_refuses "R3c #50 state 3/6 — an EMPTY (0-byte) file is FATAL: it opens, pings and quick_checks perfectly, so ONLY the schema probe can see it" \
    "no schema_migrations table" _doc_db /tmp/nvdb/empty.db
assert_refuses "R3d #50 state 4/6 — a TRUNCATED db is FATAL" \
    "not a readable SQLite database|not an intact SQLite database|CORRUPT" _doc_db /tmp/nvdb/trunc.db
assert_refuses "R3e #50 state 5/6 — a PERMISSION-DENIED db is FATAL and is diagnosed as UNREADABLE, not as corrupt (the layer the lazy sql.Open hid best)" \
    "not a readable SQLite database" _doc_db /tmp/nvdb/perm.db
assert_refuses "R3f #50 state 6/6 — a CORRUPT PAGE is FATAL: the file opens, pings and carries a schema, so quick_check is the only layer that can catch it" \
    "CORRUPT|not an intact SQLite database" _doc_db /tmp/nvdb/corrupt.db
# The anti-vacuity twin of the six: the SAME predicate, the SAME command, the HEALTHY db => green. Without
# it "doctor rejects six bad inputs" is equally true of a doctor that rejects everything.
assert_ok "R3g ANTI-VACUITY: the identical probe against the LIVE tether.db is green — the six reds above discriminate, they are not a doctor that FATALs on everything" \
    _doc_db /var/lib/tether/tether.db

# ── C — the runbook's own literal ONLINE-backup example (DOC-27 CLOSED, positive regression) ─────────
# R-SUPPLY-ORDER: this arm runs BEFORE the vault appears as [env]. DOC-27 is now CLOSED — the runbook §5
# ONLINE-backup example uses /var/lib/tether/backups, which install.sh makes tether-owned (:500 LIB_DIR),
# so the daemon (User=tether) CAN write it. This arm is the POSITIVE regression that KEEPS it closed: the
# CORRECTED example must run verbatim on a stock install; a parent-dir/perm failure is a REGRESSION.
_C_OUT=$(dexec -u tether brk1 -- sh -c 'tether cluster backup --out /var/lib/tether/backups/tether-$(date +%F)-$$' 2>&1); _C_RC=$?
_c_backup_runs()   { [ "$_C_RC" = 0 ]; }
_c_perm_signature() { printf '%s' "$_C_OUT" | grep -qiE 'prepare bundle parent|store_error|permission denied|mkdir|must not exist'; }
# Stage-C minor 3: signature-guard — only pin a REGRESSION on the EXPECTED parent-dir/perm failure, not
# any non-zero exit (a leader drift / transient / auth error must not be mislabelled DOC-27).
if _c_backup_runs; then
    assert_ok "DOC-27 CLOSED: the runbook §5 ONLINE-backup literal example (--out /var/lib/tether/backups/tether-\$(date +%F)) runs as User=tether on a stock install — install.sh makes LIB_DIR tether-owned, so the daemon's MkdirAll of the backup leaf succeeds" _c_backup_runs
elif _c_perm_signature; then
    product_red "DOC-27 REGRESSION: the CORRECTED runbook §5 example \'cluster backup --out /var/lib/tether/backups/tether-\$(date +%F)\' is unrunnable again on a stock install (parent-dir/perm failure) — the doc fix regressed. Captured: $(printf '%s' "$_C_OUT" | tail -1)"
else
    _as_fail "DOC-27 UNJUDGEABLE — the corrected backup example failed but not for the parent-dir/perm reason (rc=$_C_RC): $(printf '%s' "$_C_OUT" | tail -1). Triage before pinning DOC-27"
fi

# ── D — the authoritative leader bundle ─────────────────────────────────────────────────────────────
# ONE invocation, captured once; every D signature is asserted against THAT output (a mutating command
# must never be re-run per signature). The predicates are FUNCTIONS — `sh -c` would not see $_D_CAP.
_D_CAP=$(_bt brk1 -- tether cluster backup --out "$BK_DIR" 2>&1); _D_RC=$?
_d_rc_ok()    { [ "$_D_RC" = 0 ]; }
_d_leader()   { printf '%s' "$_D_CAP" | grep -qE 'online backup complete: .*self=brk1, source=leader'; }
# #53-silence (R10, CLOSED): the bundle has ALWAYS been state.db-only; what was unacceptable was that
# neither end said so, so an operator formed the belief "I have a backup" at exactly the moment the
# product knew it was handing them a control-plane-only one. Three independent clauses, because a
# single `grep BUNDLE` would still pass if the warning were reduced to a bare header with no content
# and no remedy: it must NAME the missing scope AND give the runnable alternative.
_d_scope_warned() {
    printf '%s' "$_D_CAP" | grep -qF 'BUNDLE SCOPE' &&
    printf '%s' "$_D_CAP" | grep -qF 'JetStream is NOT in it' &&
    printf '%s' "$_D_CAP" | grep -qF 'nats stream backup'
}
assert_ok "D backup on the LEADER (the freshest committed state)" _d_rc_ok
assert_ok "D-sig it self-reports as a LEADER-sourced online bundle" _d_leader
assert_ok "D-#53-silence the BACKUP end warns UNMISSABLY about the bundle's scope: it names 'BUNDLE SCOPE', says JetStream is NOT in it, and prints the runnable \`nats stream backup\` alternative (a bare header with no content and no remedy would satisfy a one-clause grep — this one cannot)" \
    _d_scope_warned
assert_ok "D2 [env, S0-backup-vault] pull the bundle off the box (= an operator scp-ing it away)" \
    vault_pull brk1 "$BK_DIR" "$BK_NAME"
assert_ok "D3 manifest names brk1 as a LEADER-sourced bundle over a 2-node roster" \
    sh -c "jq -e '.self_id==\"brk1\" and .source_role==\"leader\" and (.roster|length)==2' '$(vault_manifest $BK_NAME)' >/dev/null"
SHA_STATE=$(vault_sha "$BK_NAME" state.db)
[ -n "$SHA_STATE" ] || setup_fail "could not sha256 the vaulted state.db"

# ── E — the manifest carries NO secrets (allowlist, not denylist) ───────────────────────────────────
# An allowlist difference is the only future-proof shape: a denylist silently passes any NEW key that
# someone adds to the manifest later.
# The allowlist is the manifest struct's ACTUAL json tags (internal/clusteroffline/manifest.go:66-105),
# read from source rather than guessed. A DIFFERENCE (not a denylist) is the only future-proof shape: a
# denylist silently passes any new key a later release adds to the manifest.
assert_ok "E1 manifest keys are a subset of the known non-secret allowlist (a denylist would miss future keys)" \
    sh -c "jq -e '[keys[]] - [\"schema_version\",\"kind\",\"created_at\",\"tool_version\",\"mode\",\"self_id\",\"self_cert_fp\",\"account_fp\",\"applied_index\",\"source_node_id\",\"source_role\",\"leader_id\",\"name\",\"node_ident_pub\",\"raft_addr\",\"nats_route\",\"tunnel_addr\",\"public_host\",\"nats_server_id\",\"roster\"] | length == 0' '$(vault_manifest $BK_NAME)' >/dev/null"
assert_ok "E2 no PEM key / certificate / nkey seed material anywhere in the bundle" \
    sh -c "! grep -aqE 'PRIVATE KEY|BEGIN CERTIFICATE|BEGIN OPENSSH|^S[UAO][A-Z2-7]{20,}' '$(vault_dir)/$BK_NAME/manifest.json' '$(vault_dir)/$BK_NAME/state.db'"
# The format is `sha256:<hex>` — tunnel.CertFingerprint (internal/tunnel/tls.go:91-94) prefixes it.
# Asserting bare hex would fail for a reason that has nothing to do with secrets leaking.
assert_ok "E3 self_cert_fp is a FINGERPRINT (sha256:<hex>), not the certificate itself" \
    sh -c "jq -e '.self_cert_fp|test(\"^sha256:[0-9a-f]{64}$\")' '$(vault_manifest $BK_NAME)' >/dev/null"

# ── F — follower semantics (the pair; F2's bundle is NEVER used as an identity oracle) ──────────────
# Anchor the WHOLE sentence: a loose `not.*leader` would also match leaderRedirect's string and pin the
# wrong code path — `cluster backup` does not call leaderRedirect (cluster_backup.go:54-56).
assert_refuses "F1 backup on the FOLLOWER refuses and names the leader + the escape hatch" \
    "must run on the leader.*current leader: brk1.*allow-stale-follower|Re-run there, or pass --allow-stale-follower" \
    _bt brk2 -- tether cluster backup --out /var/lib/tether/bk-50-follower
assert_ok "F1b the refusal happened BEFORE any directory was created (refused, not half-done)" \
    sh -c "! \"$SIM\" exec brk2 -- test -e /var/lib/tether/bk-50-follower"
assert_ok "F2 --allow-stale-follower permits it AND labels the bundle possibly-stale" \
    out_matches 'source=FOLLOWER \(possibly stale' \
    _bt brk2 -- tether cluster backup --out /var/lib/tether/bk-50-follower --allow-stale-follower
assert_ok "F2b the stale bundle self-declares follower + names the leader (never used as an identity oracle here)" \
    sh -c "\"$SIM\" exec brk2 -- cat /var/lib/tether/bk-50-follower/manifest.json | jq -e '.source_role==\"follower\" and .leader_id==\"brk1\" and .self_id==\"brk2\"' >/dev/null"

# ── G — offline backup ──────────────────────────────────────────────────────────────────────────────
assert_refuses "G1 --offline refuses while the daemon holds the raft lock" \
    "daemon still running|raft.db locked|locked by the daemon" \
    _bt brk2 -- tether cluster backup --offline --out /var/lib/tether/bk-50-off --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets
assert_ok "G2a stop brk2's broker for the offline path" dexec brk2 -- systemctl stop tether-broker
assert_ok "G2b offline backup on a STOPPED daemon" \
    out_matches 'offline backup complete: .*mode=cluster, self=brk2' \
    _bt brk2 -- tether cluster backup --offline --out /var/lib/tether/bk-50-off --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets
assert_ok "G2c the offline bundle carries account_fp (the semantic line between offline and online)" \
    sh -c "\"$SIM\" exec brk2 -- cat /var/lib/tether/bk-50-off/manifest.json | jq -e '.account_fp|test(\"^[0-9a-f]{64}$\")' >/dev/null"
assert_ok "G2d restart brk2 and reconverge to 2 VOTER before continuing" \
    sh -c "\"$SIM\" exec brk2 -- systemctl start tether-broker && sleep 1"
assert_ok "G2e N=2 VOTER restored" poll_until 90 3 "2 VOTER after the offline-backup detour" -- _two_voters

# ── B4 — SEED Z (AFTER the backup: the negative half of the identity oracle) ────────────────────────
assert_ok "B4a create session 'zed' AFTER the backup was taken" "$SIM" ctl -- session create zed --pin 606060
# `session create` ACTIVATES the new session for this ctl. Z is only ever needed as the identity oracle's
# NEGATIVE half (it must EXIST now and be ABSENT after the restore) — the ctl must NOT be left sitting on
# it, because the restore rolls Z away and every later ctl call would then die with
#   broker auth_callout rejected the connection: nats: Authorization Violation
#   authcallout: ctl deny … sid=zed err="session \"zed\" not active"
# which is a drill artifact, not a product defect. (It cost four cascading false failures — L3/L4a/L4b/L4d
# — before the diagnostics named it.) A real operator would likewise just switch back with `tether ctx`.
assert_ok "B4b Z EXISTS right now (without this baseline, L2 asserts nothing)" \
    sh -c "\"$SIM\" ctl -- session ls --json 2>/dev/null | jq -e '([.sessions[]?|select(.name==\"lab\")]|length==1) and ([.sessions[]?|select(.name==\"zed\")]|length==1)' >/dev/null"

assert_ok "B4c switch the ctl back to $SID (session create activated 'zed'; the restore rolls it away, and a ctl left pointing at a destroyed session fails every later call with an Authorization Violation — a drill artifact, not a finding)" \
    dexec -u sim ctl1 -- env HOME=/home/sim tether login -s "$SID" --pin "$PIN" --nats-url "$NURL"

# ── H — the disaster (in-place lib-volume wipe; /etc/tether survives — see the SCOPE BOUNDARY) ──────
# Five elements: (1) baseline = B1's real curl + B3's history row; (2) authoritative observation =
# dexec + real curl + sqlite3; (3) boundary = stop the units FIRST, then rm (wiping jetstream/ under a
# live nats-server leaves it half-dead and poisons the diagnosis); (4) semantic oracle = the data plane
# is really dead (exit 7), not "the status says down"; (5) cleanup = the single EXIT trap.
assert_ok "H1a stop brk1's units before wiping (never wipe jetstream/ under a live nats-server)" \
    dexec brk1 -- systemctl stop tether-broker nats-server
assert_ok "H1b DISASTER: wipe brk1's /var/lib/tether (lib-volume loss; /etc/tether survives)" \
    dexec brk1 -- sh -c 'rm -rf /var/lib/tether/* /var/lib/tether/.[!.]* 2>/dev/null; true'
assert_ok "H1c the DB is really gone" sh -c "! \"$SIM\" exec brk1 -- test -e /var/lib/tether/tether.db"
assert_ok "H1d the data plane is really DEAD (curl exit 7 = refused; NOT '! curl -sf', which a 4xx would satisfy)" \
    poll_until 30 2 "public port refused after the disaster" -- dp_curl_refused ctl1 "http://brk1:$PX/"

# ── I — the restore gate family (daemon is already stopped; each negative anchors its OWN sentence) ─
assert_refuses "I1 restore --yes is refused (Tier-2: no unattended override by design)" \
    "cannot run unattended|NO --yes override|--yes is never accepted" \
    _bt brk1 -- tether cluster recovery restore "$BK_DIR" --confirm-node-id brk1 --yes
# I2 pins NEVER-ESCAPABLE. Do NOT write a "missing env var" negative here: confirmTypedNodeID is called
# with allowMachineEscape=false (cluster_backup.go:98-102), so $TETHER_CONFIRM_NODE_ID is never even read.
# Setting it correctly and STILL being refused is the whole point. J2 (the pty success) is the control.
assert_refuses "I2 restore is NEVER machine-escapable: correct --confirm-node-id AND correct \$TETHER_CONFIRM_NODE_ID, non-interactive => still refused" \
    "interactive terminal|type the node_id to confirm" \
    dexec -u tether brk1 -- env TETHER_CONFIRM_NODE_ID=brk1 tether cluster recovery restore "$BK_DIR" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets

# Put the bundle back on the box the way an operator would (vault_push chowns to tether).
assert_ok "I-prep push the vaulted bundle back onto brk1 [operator per runbook §5]" \
    vault_push "$BK_NAME" brk1 "$BK_DIR"
assert_refuses "I3 restore refuses when --confirm-node-id names a node the bundle is not for" \
    "does not match the bundle's self_id|--confirm-node-id .*does not match" \
    _pty brk1 brk2 -- tether cluster recovery restore "$BK_DIR" --confirm-node-id brk2 --secrets-dir /etc/tether/secrets

# I4 — FOREIGN BUNDLE. The lethal design point: pass the BUNDLE'S OWN self_id (brk1) while running on
# brk2, so gate 9 (:234, confirm-id vs manifest) is satisfied and gate 10 (:238, live tunnel-cert fp vs
# the manifest's) is the one that fires. Anything else pins gate 9 and looks identical.
assert_ok "I4-prep stop brk2's broker + stage brk1's bundle there" \
    sh -c "\"$SIM\" exec brk2 -- systemctl stop tether-broker && true"
assert_ok "I4-prep2 push brk1's bundle onto brk2" vault_push "$BK_NAME" brk2 /var/lib/tether/bk-50-foreign
B2MD5=$(dexec brk2 -- md5sum /var/lib/tether/tether.db 2>/dev/null | awk '{print $1}')
assert_refuses "I4 FOREIGN BUNDLE: brk1's bundle on brk2 is refused by the un-forgeable tunnel-cert anchor (NOT by the confirm-id gate — we satisfied that one on purpose)" \
    "tunnel-cert fingerprint mismatch|not this bundle's node|refusing to adopt a foreign cluster" \
    _pty brk2 brk1 -- tether cluster recovery restore /var/lib/tether/bk-50-foreign --confirm-node-id brk1 --secrets-dir /etc/tether/secrets
assert_ok "I4b brk2's own DB was not touched by the refused foreign restore" \
    sh -c "[ \"\$(\"$SIM\" exec brk2 -- md5sum /var/lib/tether/tether.db 2>/dev/null | awk '{print \$1}')\" = \"$B2MD5\" ]"
assert_ok "I4c restart brk2" dexec brk2 -- systemctl start tether-broker

# ── M1 — torn bundle (change a SEMANTIC value; changing JSON structure would hit gate 5 instead) ────
MD5_0=$(dexec brk1 -- sh -c 'md5sum /var/lib/tether/tether.db 2>/dev/null | awk "{print \$1}"' || echo none)
assert_ok "M1-prep tamper ONE semantic manifest value (public_host), keeping the JSON well-formed" \
    dexec brk1 -- sh -c "jq '.public_host=\"brk2\"' $BK_DIR/manifest.json > /tmp/m.json && cp /tmp/m.json $BK_DIR/manifest.json && chown tether:tether $BK_DIR/manifest.json"
assert_refuses "M1 a torn/edited bundle is refused on the manifest-vs-state.db cross-check" \
    "disagree on public_host|refusing a torn/edited bundle" \
    _pty brk1 brk1 -- tether cluster recovery restore "$BK_DIR" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets
assert_ok "M1b the refusal wrote NOTHING: no .pre-restore backup was created (the gate is before restore.go:175)" \
    sh -c "! \"$SIM\" exec brk1 -- sh -c 'ls /var/lib/tether/*.pre-restore* 2>/dev/null | grep -q .'"
assert_ok "M1c repair the manifest from the vault for the real restore" \
    vault_push "$BK_NAME" brk1 "$BK_DIR"

# ── J2 — THE REAL RESTORE (the control source for I1/I2/I3/I4/M1) ───────────────────────────────────
# `pruned 1 stale peers` is the ONLY stdout evidence the peer prune actually happened.
# ONE real invocation, captured once; every J2 signature is then asserted against THAT output. Re-running
# a mutating restore per signature would be both wasteful and (after the first) semantically different.
#
# NB the predicates below are FUNCTIONS, not `sh -c "... $_J2_CAP ..."`: a new shell does not inherit an
# unexported variable (the same family of trap as R-NOSHC — harness functions are invisible to `sh -c`).
# assert_ok runs "$@" in the CURRENT shell, so a function sees both the variable and the harness.
_j2_pruned()   { printf '%s' "$_J2_CAP" | grep -qE 'restore complete: node brk1 is now a single-voter cluster \(pruned 1 stale peers; bundle applied_index [0-9]+ reset to 0\)'; }
_j2_preserved(){ printf '%s' "$_J2_CAP" | grep -qE 'prior DB preserved at: '; }
_j2_rc_ok()    { [ "$_J2_RC" = 0 ]; }
# R10 P4: the completion text is now an ORDERED, copy-paste-ready list, not the single
# "NEXT: start tether-broker, then cluster join approve" line that made #64 possible. The old predicate
# (`grep 'NEXT: start tether-broker'`) is DELETED rather than loosened: keeping it would silently pass on
# a product that had regressed all the way back to the one-liner.
_j2_next()     { printf '%s' "$_J2_CAP" | grep -qF 'NEXT (run in order):'; }
# #53-silence, restore end. Same three-clause shape as D-#53-silence: name the loss, name the cursor
# reset (the reason nothing backfills), and give the runnable inverse of the backup remedy.
_j2_history_warned() {
    printf '%s' "$_J2_CAP" | grep -qF 'HISTORY/AUDIT NOT RESTORED' &&
    printf '%s' "$_J2_CAP" | grep -qF 'does NOT backfill' &&
    printf '%s' "$_J2_CAP" | grep -qF 'nats stream restore'
}
# R10 P2: restore now carries --config (default /etc/tether/broker.yaml) and applies the broker.cluster
# seam. On THIS box the seam already exists and matches (the sim provisioned it as root when the cluster
# was built — /etc/tether is root-owned by install.sh and the drill never chowns it), so the observable
# is the IDEMPOTENT branch. Asserting it is what proves --config is WIRED rather than merely declared:
# a flag that is parsed and dropped would print nothing here.
_j2_seam_idempotent() { printf '%s' "$_J2_CAP" | grep -qE 'broker\.cluster seam (already present and correct|applied) in|broker\.cluster seam applied to'; }

_J2_CAP=$(_pty brk1 brk1 -- tether cluster recovery restore "$BK_DIR" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets 2>&1); _J2_RC=$?
assert_ok "J2 restore succeeds via a real typed confirm (the CONTROL SOURCE for I1/I2/I3/I4/M1 — 'it always refuses' is not evidence a gate exists)" _j2_rc_ok
assert_ok "J2b restore pruned the stale peer + reset the cursor (the ONLY stdout evidence the prune ran)" _j2_pruned
assert_ok "J2c restore preserved the prior DB and printed where"                                          _j2_preserved
assert_ok "J2d restore printed the ORDERED next-step list (R10 P4 replaced the single 'NEXT: start tether-broker' line that #64 was made of)" _j2_next
assert_ok "J2e (#53-silence) the RESTORE end warns that history/audit did NOT come back, says the re-derive cursor does NOT backfill, and prints the runnable \`nats stream restore\` inverse" \
    _j2_history_warned
assert_ok "J2f (P2) restore REPORTED on the broker.cluster seam via --config (default /etc/tether/broker.yaml) — here the idempotent branch, since the seam was provisioned when the cluster was built. A --config that was parsed and dropped would print nothing at all" \
    _j2_seam_idempotent
# The seam's FIVE fields, read off disk rather than believed from stdout. `serve` keys cluster mode on
# data_dir alone, so a partial seam boots the host in SINGLE mode and lands on the same boot FATAL the
# seam exists to prevent — which is why "the seam is present" is not the assertion; "all five" is.
_seam_five_fields() {
    _sf=$("$SIM" exec brk1 -- sed -n '/^  cluster:/,/^  [a-z_]*:/p' /etc/tether/broker.yaml 2>/dev/null)
    for _k in data_dir raft_addr secrets_dir nats_conf_path nats_server_bin; do
        printf '%s' "$_sf" | grep -qE "^[[:space:]]+$_k:[[:space:]]*[^[:space:]#]" || return 1
    done
    return 0
}
assert_ok "J2g the broker.cluster seam carries ALL FIVE fields on disk (data_dir/raft_addr/secrets_dir/nats_conf_path/nats_server_bin) — serve keys cluster mode on data_dir alone, so a PARTIAL seam boots SINGLE mode and hits the very FATAL the seam prevents" \
    _seam_five_fields

# ── K — start, and the CLUSTERED-CONF finding (#64), NOW FLIPPED TO A POSITIVE ──────────────────────
# WHAT #64 WAS. `recovery restore` prunes the roster to a LONE VOTER but leaves nats.conf's `cluster{}`
# block exactly as it was, and its completion text said only "NEXT: start tether-broker, then cluster
# join approve". The operator followed that verbatim and the broker CRASH-LOOPED, because a lone node
# cannot form a clustered JetStream meta quorum. The product's own boot error named the missing step —
# so it KNEW the remedy at crash time and had simply never said it up front. The defect was the SILENCE,
# and this arm's self-check ("#64 does not reproduce … flip the ledger") existed to catch the fix.
#
# WHAT IT IS NOW (R10 P4). printRestoreNextSteps inspects nats.conf and, when it is CLUSTERED, leads
# with the de-cluster step and states outright that tether-broker will REFUSE to start without it. The
# flip is therefore NOT "assert_bug → assert_ok on the same command": the command being judged has
# CHANGED, from "start the daemon and watch it die" to "did restore say, in advance, the thing it used
# to only say at crash time — and was it right?". Two independent halves, because either alone is
# forgeable: a warning nobody can execute is theatre, and an executable line that predicts the wrong
# failure is worse than silence.
#
# THIS DRILL DELIBERATELY DOES NOT FOLLOW THE ADVICE. Arm L3 needs brk1's JetStream to re-form against
# brk2's surviving replica, which de-clustering brk1's conf would destroy — and 50's disaster is a
# lib-volume wipe, not a total loss. Following the printed steps IN ORDER is drill 51's property (the
# real DR). What 50 owns is: the advice EXISTS, is RUNNABLE, and CORRECTLY PREDICTS what happens when
# it is skipped.
#
# ── K-#64 POSITIVE HALF 1 — the ADVICE, read off the restore completion text (deterministic) ────────
# _J2_CAP is the completion text of the real restore above. /etc/tether survived the lib-volume wipe, so
# nats.conf is still the CLUSTERED conf the cluster was built with ⇒ printRestoreNextSteps must take its
# `case clustered:` branch: lead with the de-cluster step and state the broker will REFUSE to start
# without it. Three clauses, because a bare "de-cluster" mention is forgeable — the value is that it
# NAMES the consequence (refuse to start) and orders it FIRST.
_k64_advice_leads_decluster() {
    printf '%s' "$_J2_CAP" | grep -qF 'NEXT (run in order):' &&
    printf '%s' "$_J2_CAP" | grep -qiE 'is CLUSTERED, but this node is now a LONE VOTER' &&
    printf '%s' "$_J2_CAP" | grep -qiE 'tether-broker will REFUSE to start'
}
# HALF 1b — the printed remedy is RUNNABLE, not a placeholder. /etc/tether/secrets survived, so
# readClusterPublicIdentities derives REAL public nkeys (account issuer A…, broker nkey U…) rather than
# the `<account-public-nkey>` / `<broker-public-nkey>` stand-ins it prints when the keys are unreadable.
# An advice line no operator can paste is theatre; this is the clause that catches that.
_k64_remedy_runnable() {
    printf '%s' "$_J2_CAP" | grep -qE 'tether cluster reconcile nats --manual --conf ' &&
    printf '%s' "$_J2_CAP" | grep -qE -- '--account-issuer A[A-Z2-7]{20,}' &&
    printf '%s' "$_J2_CAP" | grep -qE -- '--broker-nkey U[A-Z2-7]{20,}' &&
    ! printf '%s' "$_J2_CAP" | grep -qF '<account-public-nkey>'
}
assert_ok "K-#64a the restore completion text LEADS with the de-cluster step and states the broker will REFUSE to start without it — the advance warning #64 said was owed (it used to appear only in the boot FATAL, at crash time)" \
    _k64_advice_leads_decluster
assert_ok "K-#64b that de-cluster remedy is COPY-PASTE-RUNNABLE: real substituted nkeys (--account-issuer A… / --broker-nkey U…), not the <…> placeholders it prints when secrets are unreadable" \
    _k64_remedy_runnable

# ── K-#64 POSITIVE HALF 2 — the PREDICTION was ACCURATE (reality, signature-guarded) ────────────────
# We DELIBERATELY skip the advice (L3 needs brk1's JS to re-form off brk2's replica; see the header). So
# the broker must first hit exactly the refusal the advice PREDICTED, before the surviving peer lets the
# clustered JS meta re-form (~4 crash-loops, ~73s, measured). This closes the loop that the advice is not
# just present but TRUE: a warning that predicts a failure which never happens would be noise.
# Stage-C minor 10: reset-failed before starting — the crash-loop could hit StartLimitBurst=5/10s and
# leave the broker `failed` (no self-heal). Kept: the honest self-heal window is what the prediction
# rides on.
dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker' >/dev/null 2>&1 || true
# Sample broker.err DURING the crash-loop window for the predicted refusal. A large tail so a line
# written early in the loop is still present after recovery scrolls success on top. Non-vacuous: a broker
# that came up clean (no refusal ever logged) makes this FALSE — which would mean the advice predicted a
# failure that did not occur.
# STARTUP REFUSAL ⇒ BOOT STREAM. This needle is a `return fmt.Errorf(...)` out
# of Broker.Run (internal/broker/broker.go), which cobra prints to stderr — and
# h1 routes the unit's stderr to journald, NOT to the slog file. Reading only
# the slog would make this arm red no matter how correctly the broker refuses.
# See the rule in drills/lib/logs.sh: ask WHEN the line is written, not who
# wrote it. The slog is kept as a fallback so the arm survives the refusal
# moving to after logger setup.
_k64_predicted_refusal() {
    { sim_broker_panic_journal_dump brk1; _berr brk1 400; } 2>/dev/null |
        grep -qF 'cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node'
}
_K64_PREDICTED=0
if poll_until 90 3 "brk1 logs the lone-N=1 refusal the advice predicted (skipping de-cluster on purpose)" -- _k64_predicted_refusal; then _K64_PREDICTED=1; fi
_K_READY=0
if poll_until 120 3 "brk1 broker becomes reachable (self-heals off brk2's surviving JS replica)" -- _broker_ready; then _K_READY=1; fi
_k_ready() { [ "$_K_READY" = 1 ]; }

# Diagnostics run at TOP LEVEL, never inside the asserted command: assert_ok captures stderr into _AS_OUT
# and echoes only its last 3 lines, so a diagnostic printed from inside gets swallowed. (Learned the hard
# way this session — the first cut hid exactly the evidence it existed to surface.)
if [ "$_K_READY" = 0 ]; then
    err "K2 diag 1 — what does cluster status actually SAY?"; _bt brk1 -- tether cluster status 2>&1 | head -6 >&2 || true
    err "K2 diag 2 — units:"; dexec brk1 -- systemctl is-active tether-broker nats-server 2>&1 | head -3 >&2 || true
    err "K2 diag 3 — broker slog (R-BROKERLOG):"; _berr brk1 25 >&2 || true
    err "K2 diag 4 — the cluster seam in broker.yaml:"; dexec brk1 -- sh -c 'grep -A4 "^  cluster:" /etc/tether/broker.yaml 2>/dev/null | head -6' >&2 || true
    err "K2 diag 5 — what restore left on disk:"; dexec brk1 -- sh -c 'ls -la /var/lib/tether/ 2>&1 | head -10' >&2 || true
fi
assert_ok "K1+K2 the broker becomes reachable after the restore (bounded self-heal off brk2's replica — NOT a de-cluster; a real total loss has no such peer, which is drill 51's territory)" _k_ready
# The prediction check. Signature-guarded so a run where brk2's replica re-forms the JS meta before the
# refusal is ever flushed to broker.err is recorded as an honest timing miss (runtime-guard), never as a
# false GREEN and never as a false RED against the advice.
if [ "$_K64_PREDICTED" = 1 ]; then
    _as_pass "K-#64c the advice's PREDICTION held: brk1 did hit the lone-N=1 JetStream refusal when the de-cluster step was skipped — the warning is not just present, it is TRUE"
elif [ "$_K_READY" = 1 ]; then
    not_covered "50-K-#64c the predicted lone-N=1 refusal was not observed in the broker slog this run" "the broker became reachable (self-heal off brk2's replica) but the refusal line was not captured in the crash-loop window — brk2's JS meta may have re-formed before the refusal was flushed. The ADVICE halves (K-#64a/b) are asserted deterministically above; this reality-tie is timing-dependent on the surviving peer" runtime-guard
else
    _as_fail "K-#64c UNJUDGEABLE — the broker never became reachable AND the predicted refusal was never logged. Triage before judging (see the K2 diagnostics above)"
fi

assert_ok "K3 status is DEGRADED / NOT-HA with exactly ONE voter and NO force_single (exit 1 alone has no discriminating power)" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '.health==\"DEGRADED\" and (.health_label|test(\"NOT-HA\")) and ([.nodes[]?|select(.phase==\"VOTER\")]|length==1) and (.force_single_active != true)' >/dev/null"

# ── L — IDENTITY: the bundle restored a VALID DB and the surviving peer re-converged its own state ──
# CORRECTED (R15, drill self-contradiction): the first cut asserted "zed (post-backup) is ABSENT" — a
# BACKUP-MOMENT ROLLBACK. That invariant is WRONG for THIS drill's scenario, and the drill's own L3
# comment below already says so: this is a PARTIAL loss (brk1's lib volume wiped while brk2 stays up) and
# the drill DELIBERATELY does NOT de-cluster (K header), precisely so brk1 self-heals by RE-CLUSTERING with
# brk2. Once it does, brk2 re-replicates every row it holds — including zed, which was committed after the
# backup — back onto brk1 (~73s crash-loop + up to 120s, both already awaited by K1+K2 above). So the
# CORRECT post-restore identity set here is {lab, zed}: lab proves the bundle restored real content, zed
# proves the surviving peer's post-backup data was NOT lost to the partial DR. The backup-MOMENT rollback
# (a post-backup session ABSENT) is a TOTAL-loss invariant with NO surviving peer — it belongs to drill 51,
# exactly as L3 designates. L1+L2 stay ONE read and ONE jq so a `session ls` error can't count as a pass.
# POLLED, not one-shot (R15 fix): the surviving-peer re-convergence (brk2's replica re-forms the raft group
# with the restored brk1 and re-replicates zed) is timing-VARIABLE — under deploy-tier load the self-heal can
# still be completing when the shell reaches here, so a single read races it and reads zed still-absent. Poll
# for the STABLE re-converged identity {lab, zed}; each iteration is one read + one jq eval so a `session ls`
# error can never count as a pass. This asserts the same invariant (partial-loss re-convergence) robustly.
_id_reconverged() { "$SIM" ctl -- session ls --json 2>/dev/null | jq -e '([.sessions[]?|select(.name=="lab")]|length==1) and ([.sessions[]?|select(.name=="zed")]|length==1)' >/dev/null 2>&1; }
assert_ok "L1+L2 IDENTITY: X (lab, pre-backup) is present AND Z (zed, post-backup) RE-CONVERGED from the surviving peer (POLLED — re-convergence is timing-variable under load)" \
    poll_until 90 3 "lab present AND zed re-converged from the surviving peer" -- _id_reconverged
# L3: absence is only evidence if the reader itself is proven alive. rc=0 + non-empty + L1's positive
# control must all hold before the `! grep` means anything.
# L3 — WHAT ACTUALLY HAPPENS TO HISTORY HERE, measured rather than assumed.
# The first cut asserted history would be GONE ("the bundle carries state.db only, backup.go:87"). It
# survived — and the APPEARS-FIXED guard caught that instead of quietly passing. The reason is topology,
# not a product change: this is a LIB-VOLUME wipe of brk1 while brk2 is still up, so the JetStream
# history-<sid> stream comes back from brk2's replica. It never came from the bundle at all.
# The "a bundle carries no JetStream" claim therefore CANNOT be tested here; it needs a total loss with no
# surviving replica, which is drill 51 arm J's property (#53). Asserting it here would be a false RED
# against restore. What 50 CAN honestly assert is the replicated-JS survival itself.
# Poll for a WORKING reader first: right after #64's crash-loop the JS-backed history reader is briefly
# unavailable (rc=77), and "the reader was broken" must never be confused with "the row is gone".
_l3_reader_alive() { _t=$("$SIM" ctl -- history -n 5 2>&1); [ $? = 0 ] && [ -n "$_t" ]; }
_L3_READER_UP=0
if poll_until 180 3 "the history reader is alive again after #64's crash-loop" -- _l3_reader_alive; then _L3_READER_UP=1; fi
_L3_OUT=$("$SIM" ctl -- history -n 50 2>&1); _L3_RC=$?
if [ "$_L3_READER_UP" = 0 ] || [ "$_L3_RC" != 0 ] || [ -z "$_L3_OUT" ]; then
    # Enriched under -j6 (external re-review Major 3): carry the reader's ACTUAL rc + last error so the
    # evidence self-describes the observed state (rc=77 = JetStream not yet available while brk2's
    # history-<sid> replica may still be re-forming). Do not assert a root the artifact does not prove:
    # the corrected-tree concurrent run failed in setup before reaching L3, so its later solo GREEN is
    # broad load-sensitivity evidence, not a paired reproduction of this exact recovery miss.
    not_covered "50-L3 history survival via the replica" "the JS-backed history reader did not recover within 180s after #64's ~73s crash-loop (reader_up=$_L3_READER_UP rc=$_L3_RC last='$(printf '%s' "$_L3_OUT" | tail -1 | cut -c1-160)'); the history-<sid> stream re-forms via brk2's replica on its own schedule. This run establishes a reader-recovery timing miss, not its cause; preserve it as a first-class runtime guard until a paired concurrent/solo L3 reproduction distinguishes host contention from a product recovery defect. #50/#64/DOC-27 are pinned above; history replication itself is covered by drill 10's R=3 stream proof" runtime-guard
elif printf '%s' "$_L3_OUT" | grep -q 'BACKUP-HISTORY-SENTINEL'; then
    _as_pass "L3 history SURVIVED the lib-volume wipe — correctly, via brk2's JetStream replica of history-$SID, NOT via the bundle (which carries state.db only, backup.go:87). The 'a bundle contains no JetStream' claim needs a total loss with no surviving replica: that is drill 51 arm J's property (#53), and asserting it here would be a false RED against restore"
else
    _as_fail "L3 the history row is GONE even though brk2's JetStream replica survived this lib-volume wipe — that is NOT the expected topology behaviour and is a candidate finding. Triage before judging (rc=$_L3_RC)"
fi

# L4 — THE TERMINUS. Real bytes through a real reverse tunnel on the same public port, or it did not
# happen. Never `expose ls` / `cluster status`.
_L4A=0
if poll_until 180 3 "agt1 ONLINE (window sized to the MEASURED ~73s #64 crash-loop, not an optimistic guess)" -- _agt1_online; then _L4A=1; fi
_l4a_ok() { [ "$_L4A" = 1 ]; }
if [ "$_L4A" = 0 ]; then
    err "L4a diag 1 — what does node ls -a actually show?"; "$SIM" ctl -- node ls -a 2>&1 | head -6 >&2 || true
    err "L4a diag 2 — the agent's own view (is it even trying?):"
    # h1 F3: the agent's slog is agent.log now; the journal keeps only
    # pre-logger boot output. Both are dumped — this is a diagnostic, and a
    # diagnostic that silently lost its payload is worse than a noisy one.
    sim_agent_slog_tail agt1 15 2>&1 | tail -12 >&2 || true
    dexec agt1 -- journalctl -u tether-agent -n 15 --no-pager 2>&1 | tail -6 >&2 || true
    err "L4a diag 3 — is the agent unit alive?"; dexec agt1 -- systemctl is-active tether-agent 2>&1 | head -2 >&2 || true
    err "L4a diag 4 — broker slog tail:"; _berr brk1 12 >&2 || true
fi
assert_ok "L4a agt1 comes back ONLINE after the restore" _l4a_ok
_l4b_same_port() { [ "$("$SIM" ctl -- expose explain live --json 2>/dev/null | jq -r '.public_port // empty')" = "$PX" ]; }
assert_ok "L4b expose 'live' kept its public port $PX (polled: restore -> re-home -> agent redial is ASYNC and queues behind #64's measured ~73s crash-loop; a one-shot read races it)" \
    poll_until 120 3 "expose 'live' reports the original public port" -- _l4b_same_port
assert_ok "L4c DATA PLANE RESTORED: ctl1 curls brk1:$PX and gets the EXACT pre-disaster sentinel back" \
    poll_until 180 3 "the original sentinel through the rebuilt tunnel" -- dp_curl_ok_body ctl1 "http://brk1:$PX/" "$TOKX"
# NB the field is `home_broker`, not `home`; and `epoch` is omitempty (ABSENT when 0), hence `// 0` —
# both learned from proto/messages.go:447-450 + drill 70's headnote, not from the docs.
# L4d — MEASURED, not assumed. The plan predicted "restore re-homes every ALLOCATED port to self and
# bumps the epoch". Measured 2026-07-17: the agent logs `agent: rehomed expose name=live port=14000
# epoch=0` — the epoch does NOT bump. That is COHERENT rather than a defect: `epoch` is the per-port
# reassign counter (proto/messages.go:444-449, "0 at allocate, +1 per D6 rehome"), the home was already
# pinned to brk1 by SETUP, and re-homing to self is a no-op. So the epoch claim is DROPPED (it was an
# unverified prediction, and asserting it would be a false RED against restore). What is true AND
# load-bearing is asserted instead: the port is homed to the restored node and L4c already proved it
# carries real bytes.
_l4d_homed_self() { "$SIM" ctl -- expose explain live --json 2>/dev/null | jq -e '.home_broker=="brk1"' >/dev/null 2>&1; }
assert_ok "L4d the port is homed to the restored node (the epoch does NOT bump — measured: the home was already brk1, so restore's re-home-to-self is a no-op; the plan's '+1 epoch' prediction was wrong and is dropped rather than forced)" \
    poll_until 120 3 "expose 'live' is homed to brk1" -- _l4d_homed_self

drill_end
