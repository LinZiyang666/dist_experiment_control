#!/bin/sh
# 43-migrate-live-data.sh — S6 (N=1→cluster, outcome (b)): the runbook §4
# standalone→cluster cutover (`cluster init --from-existing`) safety surface + rollback mechanics. The
# A bare pre-`init` P2 broker does not serve NATS sessions (`nats: nkeys not supported`), so the fixture
# first performs the documented single-mode auth_callout provisioning. It then creates real session,
# agent, expose and history state and proves the SAME objects survive cutover and exact rollback.
#
# FALSE-GREEN GUARDS (plan §9-43): --check proven by .bak-ABSENCE + byte-equality (exit 0 insufficient);
# machine-confirm negative paired with the unattended cutover as its positive control; rollback steps
# labeled [operator per runbook §4]; NEVER fake auth_callout to force a green business-survival.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/secrets.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/dataplane.sh"; . "$HERE/drills/lib/agentyaml.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
INST="${INSTANCE:-sim}"
# init invocation args (mirrors simcluster cmd_init), run as tether on brk1.
_init() { dexec -u tether brk1 -- sh -c "TETHER_CONFIRM_NODE_ID='${TETHER_CONFIRM_NODE_ID:-}' tether cluster init --from-existing --self-id brk1 --name brk1 --data-dir /var/lib/tether --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets --node-ident-pub $PUB --raft-addr brk1:7400 --nats-route nats://brk1:6222 --tunnel-addr brk1:7000 --public-host brk1 $1 2>&1"; }
_eport() { "$SIM" ctl -- expose explain live --json 2>/dev/null | jq -r '.public_port // empty'; }
_session_live() { "$SIM" ctl -- session ls --json 2>/dev/null | jq -e --arg s "$SID" '(.sessions // [])[]?|select(.session_id==$s or .id==$s or .name==$s)' >/dev/null 2>&1; }

drill_begin "S6-43 migrate-live-data: live rows + init from-existing cutover + exact rollback (outcome b)"

# ── SETUP: 1 un-init P2 standalone broker + agent + ctl; create tether.db (standalone boot) ──────────
assert_setup "up 1 un-init P2 broker + agent + ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_setup "start the uninitialized P2 broker once to create tether.db" "$SIM" start-broker brk1
assert_setup "wait for a non-empty pre-migration tether.db" poll_until 20 1 "non-empty tether.db" -- sh -c "$SIM exec brk1 -- test -s /var/lib/tether/tether.db"
# mint + distribute secrets (provisioning; needed for init's node-ident-pub + secrets-dir).
assert_setup "mint the documented single-mode auth_callout secrets" secrets_mint_node "$INST" brk1
assert_setup "distribute the documented single-mode auth_callout secrets" secrets_distribute "$INST" brk1
PUB=$(secrets_node_ident_pub "$INST" brk1); [ -n "$PUB" ] || setup_fail "no node-ident-pub"
ACCT=$(secrets_account_pub "$INST") || setup_fail "no account issuer"
BRK=$(secrets_broker_pub "$INST" brk1) || setup_fail "no broker nkey"
log "node-ident-pub=$PUB account-issuer=$ACCT broker-nkey=$BRK"

# ── A outcome-(b): documented single-mode auth_callout provisioning, then REAL live rows/data plane ─────
assert_setup "A [env, broker-ops §3.4] stop services before rendering standalone auth_callout" \
    "$SIM" exec brk1 -- systemctl stop tether-broker nats-server
assert_setup "A [env] render standalone nats.conf with auth_callout issuer/broker nkey" \
    dexec -u tether brk1 -- tether cluster reconcile nats --manual --server-name brk1 --route-url nats://brk1:6222 \
        --account-issuer "$ACCT" --broker-nkey "$BRK" --secrets-dir /etc/tether/secrets
assert_setup "A [env] enable the documented single-mode auth_callout seed flag in the real service unit" \
    "$SIM" exec brk1 -- sh -eu -c 'grep -q -- "--auth-callout-seeds-dir" /etc/systemd/system/tether-broker.service || sed -i "/^ExecStart=/ s|$| --auth-callout-seeds-dir /etc/tether/secrets|" /etc/systemd/system/tether-broker.service; systemctl daemon-reload; nats-server -t -c /etc/tether/nats.d/nats.conf; systemctl start nats-server tether-broker; test "$(systemctl is-active nats-server)" = active; test "$(systemctl is-active tether-broker)" = active'
assert_setup "A create the pre-migration live session" "$SIM" session "$SID" --pin "$PIN"
assert_setup "A join the pre-migration live agent" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "A provision agt1 tunnel endpoint for the live expose" agent_provision_yaml agt1 "$SID" nats://brk1:4222 open
TOK=$(expose_serve_sentinel agt1 8080) || setup_fail "cannot start live sentinel"
assert_ok "A create pre-migration live expose row" "$SIM" ctl -- expose agt1 --local 8080 --name live
P=$(_eport); [ -n "$P" ] || setup_fail "cannot resolve pre-migration expose port"
assert_ok "A pre-migration DATA PLANE returns the exact sentinel" poll_until 30 2 "pre-migration sentinel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"
assert_ok "A create a pre-migration history row with a real exec" "$SIM" ctl -- exec agt1 -- sh -c 'echo MIGRATION-HISTORY-SENTINEL'
assert_ok "A pre-migration session row is readable" _session_live

# ── B init --check zero-write: .bak ABSENT + tether.db byte-identical (exit 0 is insufficient) ──────
assert_setup "B stop broker for offline migration checks" "$SIM" exec brk1 -- systemctl stop tether-broker
MD5_DB0=$("$SIM" exec brk1 -- md5sum /var/lib/tether/tether.db 2>/dev/null | awk '{print $1}')
assert_setup "B save exact pre-migration nats.conf and broker.yaml rollback anchors" \
    "$SIM" exec brk1 -- sh -c 'cp -p /etc/tether/nats.d/nats.conf /root/nats.pre-migration; cp -p /etc/tether/broker.yaml /root/broker.pre-migration'
log "DIAG init --check →"; _init "--check" | sed 's/^/[diag check] /' | head -4
assert_ok "B init --check: NO tether.db.bak written (zero-write, not just exit 0)" \
    sh -c "! $SIM exec brk1 -- test -f /var/lib/tether/tether.db.bak"
assert_ok "B init --check: tether.db byte-identical (md5 unchanged)" \
    sh -c "[ \"\$($SIM exec brk1 -- md5sum /var/lib/tether/tether.db 2>/dev/null | awk '{print \$1}')\" = '$MD5_DB0' ]"

# ── C init --yes rejected (Tier-2, machineEscapable=true but --yes still not an override for quorum ops) ─
assert_refuses "C init --yes rejected (NO --yes override, Tier-2)" \
    "NO --yes override|cannot run unattended" \
    dexec -u tether brk1 -- sh -c "tether cluster init --from-existing --self-id brk1 --name brk1 --data-dir /var/lib/tether --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets --node-ident-pub $PUB --raft-addr brk1:7400 --nats-route nats://brk1:6222 --tunnel-addr brk1:7000 --public-host brk1 --yes 2>&1"

# ── D init machine-confirm missing env → refused (double-factor); the POSITIVE control is arm E (unattended) ─
assert_refuses "D init machine-confirm missing env → refused" \
    "type the|confirm-node-id|TTY|interactive terminal|aborted" \
    dexec -u tether brk1 -- sh -c "TETHER_CONFIRM_NODE_ID= tether cluster init --from-existing --self-id brk1 --name brk1 --data-dir /var/lib/tether --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets --node-ident-pub $PUB --raft-addr brk1:7400 --nats-route nats://brk1:6222 --tunnel-addr brk1:7000 --public-host brk1 --confirm-node-id brk1 2>&1"

# ── E noninteractive from-existing cutover (=D positive control), then execute printed operator steps ───
CUT_OUT=$(TETHER_CONFIRM_NODE_ID=brk1 _init "--confirm-node-id brk1"); CUT_RC=$?
log "E machine-confirm cutover →"; printf '%s\n' "$CUT_OUT" | sed 's/^/[cutover] /' | head -12
assert_ok "E noninteractive machine-confirm cutover succeeds without a PTY and prints the reconcile step" \
    sh -c "[ '$CUT_RC' = 0 ] && printf '%s' \"\$0\" | grep -qE '^ *1\\. tether cluster reconcile nats --manual'" "$CUT_OUT"
assert_setup "E execute the printed standalone N=1 reconcile inputs" \
    dexec -u tether brk1 -- tether cluster reconcile nats --manual --server-name brk1 --route-url nats://brk1:6222 \
        --account-issuer "$ACCT" --broker-nkey "$BRK" --secrets-dir /etc/tether/secrets
assert_setup "E apply cluster seam, validate nats.conf, restart services" \
    "$SIM" exec brk1 -- sh -eu -c 'grep -qE "^    raft_addr:" /etc/tether/broker.yaml || printf "  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk1:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n    nats_server_bin: nats-server\n" >> /etc/tether/broker.yaml; nats-server -t -c /etc/tether/nats.d/nats.conf; systemctl restart nats-server; systemctl start tether-broker'
assert_ok "E from-existing cutover reaches an authoritative N=1 leader" \
    poll_until 90 3 "N=1 leader" -- sh -c "$SIM status --json 2>/dev/null | jq -e '.leader_id==\"brk1\" and ([.nodes[]?|select(.phase==\"VOTER\")]|length)==1' >/dev/null"
assert_ok "E cutover: tether.db.bak is non-empty mode 0600" \
    sh -c "$SIM exec brk1 -- sh -c 'test -s /var/lib/tether/tether.db.bak && test \"\$(stat -c %a /var/lib/tether/tether.db.bak)\" = 600'"
# cluster-mode is authoritatively marked by the broker.yaml cluster seam (raft_addr) that cmd_init writes
# (simcluster:301) — more reliable than grepping the nats.conf (which reconcile may render with a differently
# formatted cluster block). A leader was elected (arm above), so the broker IS cluster-mode; assert the seam.
# Stage-C M13: `simcluster init` reports a leader (E arm above) yet NEITHER the broker.yaml raft_addr seam
# NOR the nats.conf cluster{} block is present in drill 43's from-existing-with-my-setup context — while the
# SAME cmd_init works via grow_to_3 in 40/41/90. measure-and-record + EXPOSE the candidate: this is either a
# real init cluster-ization gap in the from-existing-on-a-P2-started-broker path OR a residue of 43's own
# setup sequence (start-broker + pre-mint + negatives before init). Do NOT relabel green (review M13);
# EXPOSE it as a candidate needing an isolated clean-cmd_init root-cause (SB-43).
log "DIAG post-init broker.yaml + nats.conf →"; "$SIM" exec brk1 -- sh -c 'grep -nE "raft_addr|cluster|data_dir" /etc/tether/broker.yaml 2>/dev/null | head -4; echo "--nats.d--"; grep -nE "^cluster|cluster \{|routes" /etc/tether/nats.d/nats.conf 2>/dev/null | head -3' 2>&1 | sed 's/^/[diag e] /' | head -8
assert_ok "E cutover: broker.yaml carries the exact brk1 raft_addr cluster seam" \
    sh -c "$SIM exec brk1 -- grep -qE '^    raft_addr: brk1:7400$' /etc/tether/broker.yaml"

# F: the same objects, not freshly-created substitutes, survive migration.
assert_ok "F original session survives the cutover" poll_until 45 3 "session survives cutover" -- _session_live
assert_ok "F original agent reconnects ONLINE" poll_until 45 3 "agent online" -- sh -c "$SIM ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid==\"agt1\" and (.status|ascii_upcase)==\"ONLINE\")' >/dev/null"
assert_ok "F original expose port serves the exact same sentinel after cutover" poll_until 45 3 "same expose sentinel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"
assert_ok "F original history contains the pre-migration exec sentinel" \
    sh -c "$SIM ctl -- history 2>/dev/null | grep -q 'MIGRATION-HISTORY-SENTINEL'"

# ── G ROLLBACK [operator per runbook §4]: restore tether.db.bak + de-cluster → back to standalone ───
# M2: rollback is proven 3 WAYS (no `; true` masking). Capture the pristine bak md5 BEFORE the rollback
# (arm E created tether.db.bak = the pre-migration DB) so ① can byte-compare.
MD5_BAK=$("$SIM" exec brk1 -- md5sum /var/lib/tether/tether.db.bak 2>/dev/null | awk '{print $1}')
assert_ok "G rollback [operator per runbook §4]: stop + restore DB, exact nats.conf and exact broker.yaml" \
    "$SIM" exec brk1 -- sh -eu -c 'systemctl stop tether-broker nats-server; cp -f /var/lib/tether/tether.db.bak /var/lib/tether/tether.db; rm -f /var/lib/tether/tether.db-wal /var/lib/tether/tether.db-shm; cp -f /root/nats.pre-migration /etc/tether/nats.d/nats.conf; cp -f /root/broker.pre-migration /etc/tether/broker.yaml; nats-server -t -c /etc/tether/nats.d/nats.conf'
assert_ok "G rollback ① DB byte-exact: tether.db == the pristine tether.db.bak (md5 match, not just cp exit 0)" \
    sh -c "[ -n '$MD5_BAK' ] && [ \"\$($SIM exec brk1 -- md5sum /var/lib/tether/tether.db 2>/dev/null | awk '{print \$1}')\" = '$MD5_BAK' ]"
assert_ok "G rollback ② cluster-mode OFF: broker.yaml no longer has the raft_addr cluster seam" \
    sh -c "! $SIM exec brk1 -- grep -qE '^    raft_addr:' /etc/tether/broker.yaml"
assert_ok "G rollback ③ process restart: broker boots as working standalone with auth_callout" \
    "$SIM" exec brk1 -- sh -eu -c 'systemctl start nats-server tether-broker; test "$(systemctl is-active nats-server)" = active; test "$(systemctl is-active tether-broker)" = active'
assert_ok "G rollback ④ the ORIGINAL session is readable after rollback" poll_until 45 3 "session after rollback" -- _session_live
assert_ok "G rollback ④ the ORIGINAL agent reconnects ONLINE without re-provisioning" \
    poll_until 90 3 "agent online after rollback" -- sh -c "$SIM ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid==\"agt1\" and (.status|ascii_upcase)==\"ONLINE\")' >/dev/null"
if poll_until 120 3 "same rollback sentinel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"; then
    _as_pass "G rollback ④ the ORIGINAL expose port returns the SAME pre-migration sentinel"
else
    _as_fail "G rollback ④ the ORIGINAL expose port did not return the SAME pre-migration sentinel"
    log "DIAG rollback node/expose views and reconnect logs:"
    "$SIM" ctl -- node ls --json 2>&1 | sed 's/^/[diag node] /' | head -30 || true
    "$SIM" ctl -- expose explain live --json 2>&1 | sed 's/^/[diag expose] /' | head -50 || true
    "$SIM" exec agt1 -- sh -c 'tail -n 80 /var/log/tether/agent.err /var/log/tether/agent.log 2>/dev/null' 2>&1 | sed 's/^/[diag agent] /' || true
    "$SIM" exec brk1 -- sh -c 'tail -n 100 /var/log/tether/broker.err /var/log/tether/broker.log 2>/dev/null' 2>&1 | sed 's/^/[diag broker] /' || true
fi

drill_end
