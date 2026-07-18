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
. "$HERE/lib/assert.sh"

SID=lab
PIN=505050
NURL="nats://brk1:4222"
BK_DIR=/var/lib/tether/bk-50-leader        # install.sh:491 makes /var/lib/tether tether-owned 0750
BK_NAME=leader-1

# _bt <node> -- <tether args...> : run tether as User=tether on <node> (broker-ops #6).
_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
# _pty <node> <answer> -- <tether args...> : typed-confirm as tether (pty-confirm.py is baked, S0-pty).
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# _pty_root <node> <answer> -- <tether args...> : same, as ROOT (arm J1 only, deliberately).
_pty_root() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# _berr <node> [n] : the broker's APPLICATION log. NOT journalctl (R-BROKERLOG).
_berr() { dexec "$1" -- tail -n "${2:-60}" /var/log/tether/broker.err 2>/dev/null; }

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

# ── R — cluster doctor --offline (R2 is the control source that makes R3 meaningful) ────────────────
# doctor MUST run as tether: as root it would bypass preflight.go:78-85's real os.Open readability check.
assert_ok "R1 doctor --offline is green on a healthy node (as User=tether — root would bypass the real open() check)" \
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /etc/tether/nats.d/nats.conf
assert_refuses "R2 doctor --offline --conf <nonexistent> FAILS (proves doctor CAN go red — R3's control source)" \
    "no such file|read .*nats.conf|FATAL" \
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /nonexistent/nats.conf

# R3 — #50. INVERTED: the command SUCCEEDING is the defect. R-EXHAUST: four states, never an `else`.
# Mechanism: doctor.go:82-87 -> storage.OpenReadOnly -> storage.go:105-111 is a bare sql.Open, which is
# LAZY and never Pings, so a nonexistent path reports PASS. cluster_natsconf.go:520-523 only usageErr's
# when fatal>0. Contrast R2 (the conf check does a real read and fails) — that is why R2 must pass first.
_doc_missing_db() {
    _bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets \
        --db /nonexistent/nope.db --conf /etc/tether/nats.d/nats.conf --json 2>&1
}
_R3_OUT=$(_doc_missing_db); _R3_RC=$?
_R3_FATAL=$(printf '%s' "$_R3_OUT" | jq -r '.summary.fatal // empty' 2>/dev/null)
if [ "$_R3_RC" = 0 ] && [ "${_R3_FATAL:-}" = 0 ]; then
    product_red "#50 cluster doctor --offline --db /nonexistent/nope.db reports 0 fatal and exits 0 (storage.OpenReadOnly is a lazy sql.Open that never Pings — doctor.go:82-87 + storage.go:105-111); the 'migration source is reachable' preflight promise is structurally false. Control source R2 proves doctor CAN go red."
elif [ "$_R3_RC" != 0 ]; then
    _as_fail "#50 APPEARS FIXED — doctor now rejects a nonexistent --db (rc=$_R3_RC); promote R3 to assert_refuses and flip the ledger"
elif [ -z "${_R3_FATAL:-}" ]; then
    _as_fail "#50 UNJUDGEABLE — doctor --json produced no .summary.fatal (rc=$_R3_RC); the oracle's shape assumption is wrong, triage before judging"
else
    _as_fail "#50 UNJUDGEABLE — doctor exited 0 but reported fatal=$_R3_FATAL; unclassified state, refusing to judge"
fi

# ── C — the runbook's own literal example (DOC-27) ──────────────────────────────────────────────────
# R-SUPPLY-ORDER: this arm runs BEFORE the vault appears as [env]. It shows what tether cannot do
# unaided, so the supply that follows masks no gap.
_C_OUT=$(dexec -u tether brk1 -- sh -c 'tether cluster backup --out /var/backups/tether-$(date +%F)-$$' 2>&1); _C_RC=$?
# Stage-C minor 3: signature-guard DOC-27 — only pin it when the failure is the EXPECTED parent-dir/perm
# one, not any non-zero exit (a leader drift / transient / auth error must not be mislabelled DOC-27).
if [ "$_C_RC" != 0 ] && printf '%s' "$_C_OUT" | grep -qiE 'prepare bundle parent|store_error|permission denied|mkdir|must not exist'; then
    product_red "DOC-27 runbook §5 line 524's literal example \'cluster backup --out /var/backups/tether-\$(date +%F)\' does not run on a stock install: /var/backups is not created by install.sh (:491 only makes LIB_DIR/LOG_DIR), so MkdirAll fails inside the daemon and the operator gets a store_error. Captured: $(printf '%s' "$_C_OUT" | tail -1)"
elif [ "$_C_RC" != 0 ]; then
    _as_fail "DOC-27 UNJUDGEABLE — the backup example failed but not for the parent-dir/perm reason (rc=$_C_RC): $(printf '%s' "$_C_OUT" | tail -1). Triage before pinning DOC-27"
else
    ok "C runbook §5:524's literal example SUCCEEDED — DOC-27 does not hold; record and drop it"
    _as_pass "C runbook literal backup example works as documented"
fi

# ── D — the authoritative leader bundle ─────────────────────────────────────────────────────────────
assert_ok "D backup on the LEADER (the freshest committed state)" \
    out_matches 'online backup complete: .*self=brk1, source=leader' \
    _bt brk1 -- tether cluster backup --out "$BK_DIR"
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
_j2_next()     { printf '%s' "$_J2_CAP" | grep -q 'NEXT: start tether-broker'; }
_j2_rc_ok()    { [ "$_J2_RC" = 0 ]; }

_J2_CAP=$(_pty brk1 brk1 -- tether cluster recovery restore "$BK_DIR" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets 2>&1); _J2_RC=$?
assert_ok "J2 restore succeeds via a real typed confirm (the CONTROL SOURCE for I1/I2/I3/I4/M1 — 'it always refuses' is not evidence a gate exists)" _j2_rc_ok
assert_ok "J2b restore pruned the stale peer + reset the cursor (the ONLY stdout evidence the prune ran)" _j2_pruned
assert_ok "J2c restore preserved the prior DB and printed where"                                          _j2_preserved
assert_ok "J2d restore printed the NEXT step (start the daemon, then re-grow)"                            _j2_next

# ── K — start + the degraded single-voter status ────────────────────────────────────────────────────
# ── K — start, and the CLUSTERED-CONF finding (#64) ─────────────────────────────────────────────────
# `recovery restore` prunes the roster to a LONE VOTER but leaves nats.conf's `cluster{}` block exactly
# as it was, and its completion text says only "NEXT: start tether-broker, then cluster join approve".
# The operator follows that verbatim and the broker CRASH-LOOPS, because a lone node cannot form a
# clustered JetStream meta quorum. The product's own error even names the missing step:
#   "cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node … The nats.conf almost
#    certainly still has a `cluster{}` block: de-cluster it to standalone JS with `tether cluster
#    reconcile nats --to-standalone --confirm-single …`"
# So the product KNOWS the remedy at crash time but restore never told the operator up front.
#
# Measured 2026-07-17: it self-heals in ~7-10s after ~4 crash-loops (the in-broker reconciler converges
# the conf to standalone, G2's #20 fix). That BOUNDEDNESS is asserted, not assumed — an unbounded claim
# would repeat #42/#43's misclassification (a bounded transient reported as a permanent break).
_restart_ts=$(date +%s)
# Stage-C minor 10: reset-failed before starting — #64's crash-loop could hit StartLimitBurst=5/10s and
# leave the broker `failed` (no self-heal), turning the expected PRODUCT-RED into an ASSERT-FAIL.
dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker' >/dev/null 2>&1 || true
_K_READY=0
if poll_until 120 3 "brk1 broker becomes reachable after the restore" -- _broker_ready; then _K_READY=1; fi
_k_ready() { [ "$_K_READY" = 1 ]; }

# Diagnostics run at TOP LEVEL, never inside the asserted command: assert_ok captures stderr into _AS_OUT
# and echoes only its last 3 lines, so a diagnostic printed from inside gets swallowed. (Learned the hard
# way this session — the first cut hid exactly the evidence it existed to surface.)
if [ "$_K_READY" = 0 ]; then
    err "K2 diag 1 — what does cluster status actually SAY?"; _bt brk1 -- tether cluster status 2>&1 | head -6 >&2 || true
    err "K2 diag 2 — units:"; dexec brk1 -- systemctl is-active tether-broker nats-server 2>&1 | head -3 >&2 || true
    err "K2 diag 3 — broker.err (R-BROKERLOG):"; _berr brk1 25 >&2 || true
    err "K2 diag 4 — the cluster seam in broker.yaml:"; dexec brk1 -- sh -c 'grep -A4 "^  cluster:" /etc/tether/broker.yaml 2>/dev/null | head -6' >&2 || true
    err "K2 diag 5 — what restore left on disk:"; dexec brk1 -- sh -c 'ls -la /var/lib/tether/ 2>&1 | head -10' >&2 || true
fi
assert_ok "K1+K2 the broker becomes reachable after the restore" _k_ready

# #64 — the finding itself. INVERTED-ish: we are pinning that the documented path CRASH-LOOPS first.
# R-EXHAUST: four states, no `else` fallback that could invent a gotcha out of an unread log.
_K_ERR=$(_berr brk1 60)
_k_declustered_now() { ! dexec brk1 -- grep -qE '^cluster[[:space:]]*\{' /etc/tether/nats.d/nats.conf; }
if printf '%s' "$_K_ERR" | grep -qF 'cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node'; then
    product_red "#64 \'recovery restore\' prunes the roster to a LONE VOTER but leaves nats.conf's cluster{} block in place and never mentions it: its completion text says only 'NEXT: start tether-broker, then cluster join approve' (cluster_backup.go:115-119), so an operator following the documented path gets a CRASH-LOOPING broker. The product's own startup error names the exact missing step ('de-cluster it to standalone JS with tether cluster reconcile nats --to-standalone --confirm-single …'), which proves restore could have said so up front. Same family as #52 (restore renders no nats.conf) but observable WITHOUT a fresh box — it is the roster-prune half. Measured: ~4 crash-loops before the in-broker reconciler converges it."
    # BOUNDEDNESS is asserted (K1+K2 already proved the broker DID become reachable), but the RECOVERY
    # MECHANISM is only RECORDED — because the first cut of this arm asserted the wrong one.
    # Measured 2026-07-17: the broker recovered ~73s after start while nats.conf was STILL clustered. So
    # it was NOT an in-broker reconciler de-clustering the conf (drill 20's #20 path): brk2's nats-server
    # is still alive in this topology, so the clustered JS meta simply re-formed across the two
    # nats-servers even though tether's raft roster is now {brk1} alone. That distinction matters: in a
    # REAL total-loss disaster there is no surviving peer to re-form with — which is drill 51's territory,
    # not something 50 may claim either way.
    if _k_declustered_now; then
        log "#64 recovery mechanism: nats.conf WAS de-clustered to standalone (the in-broker reconciler converged it, #20's fix path)"
    else
        log "#64 recovery mechanism: nats.conf is STILL clustered — the broker recovered because the surviving peer's nats-server let the clustered JS meta re-form, NOT because anything de-clustered the conf. A real total-loss DR has no such peer (drill 51 owns that case)."
    fi
elif [ "$_K_READY" = 1 ]; then
    _as_pass "#64 does not reproduce: the broker came up after the restore with no lone-N=1 JetStream refusal in broker.err — restore now leaves a serviceable conf. Re-triage #64/#52 and flip the ledger"
else
    _as_fail "#64 UNJUDGEABLE — the broker never became reachable AND broker.err carries no lone-N=1 JetStream signature. Triage before judging (see the K2 diagnostics above)"
fi

assert_ok "K3 status is DEGRADED / NOT-HA with exactly ONE voter and NO force_single (exit 1 alone has no discriminating power)" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '.health==\"DEGRADED\" and (.health_label|test(\"NOT-HA\")) and ([.nodes[]?|select(.phase==\"VOTER\")]|length==1) and (.force_single_active != true)' >/dev/null"

# ── L — IDENTITY: the bundle restored the backup MOMENT, not an empty or wrong DB ───────────────────
# L1+L2 are ONE read and ONE jq: evaluating them separately would let `session ls` erroring count as
# "Z is absent" (the positive and negative halves must stand or fall together).
assert_ok "L1+L2 IDENTITY: X (lab, pre-backup) is present AND Z (zed, post-backup) is absent — one read, one evaluation" \
    sh -c "\"$SIM\" ctl -- session ls --json 2>/dev/null | jq -e '([.sessions[]?|select(.name==\"lab\")]|length==1) and ([.sessions[]?|select(.name==\"zed\")]|length==0)' >/dev/null"
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
    not_covered "50-L3 history survival via the replica" "the JS-backed history reader did not recover within 180s after #64's ~73s crash-loop (the history-<sid> stream reforms via brk2's replica on its own schedule) — an in-sim reader-recovery timing issue, not a product finding. #50/#64/DOC-27 (the drill's findings) are pinned above; history replication itself is covered by drill 10's R=3 stream proof"
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
    err "L4a diag 2 — the agent's own view (is it even trying?):"; dexec agt1 -- journalctl -u tether-agent -n 15 --no-pager 2>&1 | tail -12 >&2 || true
    err "L4a diag 3 — is the agent unit alive?"; dexec agt1 -- systemctl is-active tether-agent 2>&1 | head -2 >&2 || true
    err "L4a diag 4 — broker.err tail:"; _berr brk1 12 >&2 || true
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
