#!/bin/sh
# 91-client-converge.sh — S8 (N=1→2→3, GREEN; G3 debt A/C/D): the client-view auto-convergence journey.
# A1 first publish (the ONE allowed manual) → A2 grow → `seeds show` AUTO-includes the new member (no
# publish, gen advances) → A3 retire → dead endpoint auto-disappears → D cli broker auto-failover via a
# NON-floor survivor (kill the ctl-pinned floor, `node ls` still succeeds via a survivor) → C offline
# force-single → seeds converge survivor-ONLY. G3 essence: every convergence after the first publish is
# operator-command-FREE (any step needing a manual publish = a defect).
#
# SOURCE FACTS (verified): DeriveSeedEndpoints returns nil for empty/`nats://`-only prev (seed_converge.go)
# → A1 MUST use tls:// endpoints. `seeds show` has NO --json — 4 plain lines (seed_generation:/bootstrap_url:/
# account_pub:/endpoints: comma-joined). cli broker auto-failover is SILENT (DialFor returns a floor-last
# multi-URL; the NATS client picks a survivor) → the D oracle is `node ls` SUCCEEDS while broker_url==dead-floor
# (a data-plane oracle, not a log line). #31 is intermittent (SB-40) → A3's retire branches best-effort.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
BOOT="https://brk1/.well-known/tether/cluster-manifest"
S()  { dexec -u tether "$LDR" -- tether cluster seeds "$@"; }        # seeds publish/show on the leader admin socket
# seeds show endpoints line contains node $1 ?
_ep_has() { S show 2>/dev/null | grep -E '^endpoints:' | grep -q "$1"; }
_gen()    { S show 2>/dev/null | sed -n 's/^seed_generation: *//p' | head -1; }

drill_begin "S8-91 client-converge: A1 publish + A2 grow-auto-include + D cli-failover + C survivor-only (G3 A/C/D)"

# ── SETUP: N=1 first (init brk1), publish, then grow incrementally so A2 observes each grow ──────────
assert_setup "up 3 brokers + agent + ctl" "$SIM" up --brokers 3 --agents 1 --ctl 1
assert_setup "init brk1 (N=1 leader)" "$SIM" init brk1
LDR=brk1
assert_setup "create + activate ctl session" "$SIM" session "$SID" --pin "$PIN"

# ── A1 first publish (tls:// endpoint; the ONE allowed manual) ──────────────────────────────────────
# _publish_ok: run S publish (function, called DIRECTLY — never dexec inside sh -c) + assert it published.
# seed_generation is a large monotonic timestamp (not sequential "1") — assert on "published", not a value.
_publish_ok() { S publish --bootstrap "$BOOT" --endpoint tls://brk1:443 --sid "$SID" 2>&1 | grep -qE 'seeds published|published .*seed_generation|seed_generation='; }
assert_ok "A1 seeds publish (bootstrap + tls://brk1:443) → published (seed_generation set)" _publish_ok
log "DIAG seeds show →"; S show 2>&1 | sed 's/^/[diag show] /' | head -8
assert_ok "A1 seeds show lists tls://brk1:443" _ep_has "brk1"
GEN0=$(_gen); log "A1 seed_generation=$GEN0"

# ── A2 grow → seeds show AUTO-includes the new member (NO publish; change-gated auto-publish) ────────
assert_setup "A2 grow brk2 reaches VOTER" "$SIM" grow brk2
assert_ok "A2 grow brk2 → seeds show AUTO-includes brk2 (NO manual publish) within 60s" \
    poll_until 60 4 "brk2 in endpoints" -- _ep_has "brk2"
assert_ok "A2 seed_generation ADVANCED past $GEN0 after grow (change-gated auto-publish)" \
    sh -c "[ \"\$($SIM exec $LDR -- tether cluster seeds show 2>/dev/null | sed -n 's/^seed_generation: *//p' | head -1)\" != '$GEN0' ]"
assert_setup "A2 grow brk3 reaches VOTER" "$SIM" grow brk3
# gate on brk3 actually reaching VOTER first (a grow flake ≠ an auto-converge failure), then poll seeds ≤120s.
poll_until 90 4 "brk3 reaches VOTER" -- sh -c "$SIM status --json 2>/dev/null | jq -e '.nodes[]?|select(.node_id==\"brk3\" and .phase==\"VOTER\")' >/dev/null" || warn "A2: brk3 did not reach VOTER (grow flake — auto-converge can't include a non-voter)"
# measure-and-record: brk2 auto-converged into seeds (arm above) but brk3 consistently does not within 120s.
# If brk3 IS a VOTER yet never appears in `seeds show`, that is a REAL candidate G3 auto-converge gap (the
# change-gated auto-publish includes the 2nd broker but not the 3rd) — EXPOSE it, do not just extend the timeout.
if poll_until 120 5 "brk3 in endpoints" -- _ep_has "brk3"; then
    _as_pass "A2 grow brk3 → seeds show AUTO-includes brk3 (NO manual publish, change-gated auto-publish)"
elif "$SIM" status --json 2>/dev/null | jq -e '.nodes[]?|select(.node_id=="brk3" and .phase=="VOTER")' >/dev/null 2>&1; then
    # brk3 IS a VOTER but never appeared in `seeds show` within 120s, while brk2 DID — a REAL G3 auto-converge
    # defect (the change-gated auto-publish includes the 2nd broker but not the 3rd), reproduced this run and
    # signature-matched (VOTER present, endpoint absent) → PRODUCT-RED, not a silent NOT-COVERED-GREEN.
    product_red "#46 seeds auto-converge OMITS the 3rd voter [#46] — brk3 reached VOTER but never appeared in 'seeds show' endpoints within 120s while brk2 DID; the change-gated auto-publish includes the 2nd broker but not the 3rd (seed_converge.go root-cause SB-91)"
else
    not_covered "A2 grow brk3 → seeds auto-include (brk3 never reached VOTER this run — grow flake)" \
        "auto-converge cannot include a non-voter; the G3 auto-converge assertion for the 3rd broker was not exercisable this run (grow flake, not a G3 gap)"
fi

# ── D cli broker auto-failover + trust-anchor negative ──────────────────────────────────────
# Persist a NON-leader floor, OOB-pin a different survivor, and warm the signed cache. Killing the floor
# therefore causes no raft election; `node ls` succeeding proves DialFor used a survivor.
ACCT=$(secrets_account_pub "$INSTANCE") || setup_fail "derive cluster account public key"
D_FLOOR=$(a_non_leader_voter) || setup_fail "select a non-leader floor for ctl failover"
D_SURV=$(list_nodes broker | grep -vx "$D_FLOOR" | head -1)
[ -n "$D_SURV" ] || setup_fail "select a survivor distinct from the ctl floor"
assert_setup "D persist ctl broker_url on non-leader floor $D_FLOOR" \
    "$SIM" ctl -- login -s "$SID" --broker "nats://$D_FLOOR:4222"
INV=$("$SIM" ctl -- cluster invite --account-pub "$ACCT" --seed "nats://$D_SURV:4222" 2>/dev/null) \
    || setup_fail "mint OOB discovery invite"
assert_setup "D pin account trust + OOB survivor $D_SURV" "$SIM" ctl -- cluster pin "$INV"
assert_setup "D warm signed roster/seed cache over the persisted floor" "$SIM" ctl -- node ls
assert_ok "D cache belongs to floor and contains the OOB survivor" sh -c \
    "$SIM exec ctl1 -- sh -c 'test \"\$(cat /home/sim/.tether/broker_url)\" = nats://$D_FLOOR:4222 && jq -e \".floor_url==\\\"nats://$D_FLOOR:4222\\\" and (.invite_seeds|index(\\\"nats://$D_SURV:4222\\\")!=null)\" /home/sim/.tether/cluster_endpoints.json >/dev/null'"

# Preserve the signed document for the negative.  Build a second VALID OOB-only cache for recovery:
# mixing the roster's tls:// endpoints with this sim's explicit nats:// invite makes nats.go require TLS
# for every URL and fails with "secure connection not available" before failover is exercised.
assert_setup "D save exact signed endpoint cache" "$SIM" exec ctl1 -- cp /home/sim/.tether/cluster_endpoints.json /tmp/cluster_endpoints.signed.json
assert_setup "D construct valid OOB-only recovery cache (same pinned account + invite)" "$SIM" exec ctl1 -- sh -c \
    'jq '\''del(.roster,.seeds) | .roster_gen=0 | .seed_gen=0'\'' /home/sim/.tether/cluster_endpoints.json > /tmp/cluster_endpoints.valid.json && chown sim:sim /tmp/cluster_endpoints.valid.json && chmod 600 /tmp/cluster_endpoints.valid.json'
assert_setup "D corrupt signed roster+seed cache without corrupting JSON" "$SIM" exec ctl1 -- sh -c \
    'jq '\'' .invite_seeds=[] | .fetched_at="2099-01-01T00:00:00Z" | if .roster then .roster.sig=("00"+(.roster.sig[2:])) else . end | if .seeds then .seeds.sig=("00"+(.seeds.sig[2:])) else . end '\'' /tmp/cluster_endpoints.signed.json > /tmp/ce.torn && chown sim:sim /tmp/ce.torn && chmod 600 /tmp/ce.torn && mv /tmp/ce.torn /home/sim/.tether/cluster_endpoints.json'
assert_setup "D kill non-leader floor $D_FLOOR" d kill "$(ctr_name "$D_FLOOR")"
assert_setup "D floor is provably dead (no election injected)" poll_until 30 2 "$D_FLOOR :4222 refused" -- tcp_refused "$D_FLOOR" 4222
assert_refuses "D trust anchor: torn signed cache is ignored (dead floor cannot escape via untrusted endpoints)" \
    "connect|connection refused|no servers available|unavailable|timeout|server misbehaving|secure connection not available" "$SIM" ctl -- node ls
assert_setup "D restore exact valid pinned cache" "$SIM" exec ctl1 -- cp /tmp/cluster_endpoints.valid.json /home/sim/.tether/cluster_endpoints.json
assert_ok "D live failover: node ls succeeds via survivor while persisted floor remains dead" "$SIM" ctl -- node ls
assert_ok "D failover did not silently rewrite the operator's dead floor" sh -c \
    "$SIM exec ctl1 -- sh -c 'test \"\$(cat /home/sim/.tether/broker_url)\" = nats://$D_FLOOR:4222'"
assert_setup "D restart the killed floor container" d start "$(ctr_name "$D_FLOOR")"
assert_setup "D restarted floor returns as a VOTER" poll_until 90 3 "$D_FLOOR returns VOTER" -- sh -c \
    "$SIM status --json 2>/dev/null | jq -e '.nodes[]?|select(.node_id==\"$D_FLOOR\" and .phase==\"VOTER\")' >/dev/null"

# ── A3 retire → endpoint disappears and generation advances (no manual publish) ─────────────────────
LDR=$(sim_leader) || setup_fail "resolve leader before A3 retire"
A3_T=$(a_non_leader_voter) || setup_fail "select non-leader retire target"
A3_GEN=$(_gen)
A3_OUT=$(dexec -u tether "$LDR" -- sh -c "python3 /opt/sim/pty-confirm.py $A3_T -- tether cluster retire $A3_T --wait --timeout 3m 2>&1")
A3_RC=$?
log "A3 retire rc=$A3_RC output →"; printf '%s\n' "$A3_OUT" | sed 's/^/[A3] /'
if [ "$A3_RC" = 0 ]; then
    _as_pass "A3 cluster retire $A3_T --wait completed"
    assert_ok "A3 retired endpoint auto-disappears and seed generation advances (NO publish)" \
        poll_until 90 4 "$A3_T absent + generation advanced" -- sh -c \
        "$SIM exec $LDR -- tether cluster seeds show 2>/dev/null | grep -E '^endpoints:' | grep -qv '$A3_T' && test \"\$($SIM exec $LDR -- tether cluster seeds show 2>/dev/null | sed -n 's/^seed_generation: *//p' | head -1)\" != '$A3_GEN'"
elif printf '%s' "$A3_OUT" | grep -qiE 'grow of .* is in progress|membership operation .* in flight'; then
    product_red "#31 leaked grow lock blocks A3 retire — captured output matched membership operation/grow in flight"
else
    _as_fail "A3 retire failed for an undocumented reason (rc=$A3_RC)" "$(printf '%s' "$A3_OUT" | tail -5)"
fi

# ── C offline force-single → seeds converge survivor-ONLY (LAST; destructive) ───────────────────────
# Stage-C M7: at N=3 the survivor has TWO other peers, and offline force-single requires ALL non-self peers
# dead (else HARD-REFUSE, which the old --confirm-peers-dead <one> swallowed via ||true → RED-or-vacuous).
# Kill BOTH non-survivors + confirm both; oracle is POSITIVE (survivor present) AND NEGATIVE (both dropped).
SURV=$(sim_leader 2>/dev/null || echo brk1)
DEADPS=$(list_nodes broker | grep -vx "$SURV")            # the two non-survivor brokers
_deadre=$(printf '%s' "$DEADPS" | tr '\n' '|' | sed 's/|$//')   # brk2|brk3 for the negative grep
_confirm=""; for _d in $DEADPS; do _confirm="$_confirm --confirm-peers-dead $_d"; done
log "C: survivor=$SURV dead-peers-to-prune=[$(printf '%s' "$DEADPS" | tr '\n' ' ')]"
for _d in $DEADPS; do assert_setup "C kill $_d" d kill "$(ctr_name "$_d")"; done
for _d in $DEADPS; do assert_setup "C prove $_d dead" poll_until 30 2 "$_d dead" -- tcp_refused "$_d" 7400; done
assert_setup "C stop survivor broker for offline recovery" "$SIM" exec "$SURV" -- systemctl stop tether-broker
# shellcheck disable=SC2086
# Round-5 §M1: this arm USED to pipe force-single into `grep -qiE '…|single-voter|force.single'`. That
# matched ForceSingle's INTERNAL "…is now a single-voter cluster" log line — emitted BEFORE the CLI's
# nats.conf de-cluster step — so grep -q exited, SIGPIPE killed tether mid-operation, the de-cluster never
# ran, and the survivor crash-looped exit 70 forever while the drill blamed the product for "seeds not
# converging". Run it to COMPLETION (no pipe, rc-checked), then assert the REAL post-conditions.
# shellcheck disable=SC2086
assert_setup "C offline force-single commits with every non-self peer explicitly confirmed dead (runs to completion, rc=0)" \
    "$SIM" exec "$SURV" -- runuser -u tether -- python3 /opt/sim/pty-confirm.py "$SURV" -- tether cluster recovery force-single --self-id "$SURV" --self-addr "$SURV:7400" $_confirm
# The load-bearing post-condition (#20): OFFLINE force-single MUST leave nats.conf standalone. A clustered
# conf at N=1 can never form the JS meta quorum → the broker fail-closes exit 70 → an un-recoverable brick
# (force-single refuses to re-run once the peers are pruned, and `reconcile nats --to-standalone` needs the
# very broker that cannot start). Assert the FILE, not a log string.
assert_ok "C force-single de-clustered nats.conf to standalone (no cluster{} block — #20 post-condition, not a log line)" \
    sh -c "! $SIM exec $SURV -- grep -qE '^cluster[[:space:]]*\\{' /etc/tether/nats.d/nats.conf"
assert_setup "C reset JetStream and restart both standalone services" "$SIM" exec "$SURV" -- sh -c \
    'mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.$(date +%s) 2>/dev/null || true; systemctl restart nats-server; systemctl start tether-broker; systemctl is-active --quiet nats-server tether-broker'
# Round-5 §M1: `is-active` immediately after `systemctl start` races the failure — a broker that boots and
# then exit-70s is still "activating" at that instant, so the crash-loop leaked downstream and resurfaced as
# a misleading "seeds do not converge". Gate on a SETTLED survivor: NRestarts stops climbing AND the admin
# socket actually answers. A crash-loop now fails HERE, naming itself.
# NB the liveness probe must NOT require rc=0: after force-single `cluster status` deliberately exits 3
# (the force_single_active health banner). Liveness = the admin socket ANSWERS at all, i.e. no dial error.
assert_ok "C survivor is SETTLED after the restart (admin socket answers; not exit-70 crash-looping)" \
    poll_until 60 4 "survivor settled + admin socket answering" -- sh -c "
      [ \"\$($SIM exec $SURV -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')\" = active ] &&
      ! $SIM exec $SURV -- runuser -u tether -- tether cluster status 2>&1 | grep -qiE 'admin socket|no such file|connection refused|dial unix'"
LDR=$SURV
# POSITIVE+NEGATIVE oracle (M7): survivor endpoint PRESENT and BOTH dead endpoints ABSENT (G3 drop-only, no publish).
assert_ok "C seeds converge to survivor-ONLY: $SURV present + [$_deadre] dropped from endpoints (no manual publish) within 90s" \
    poll_until 90 4 "seeds = survivor only" -- sh -c "$SIM exec $SURV -- tether cluster seeds show 2>/dev/null | grep -E '^endpoints:' | grep -q '$SURV' && ! $SIM exec $SURV -- tether cluster seeds show 2>/dev/null | grep -E '^endpoints:' | grep -qE '$_deadre'"

drill_end
