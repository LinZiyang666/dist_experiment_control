#!/bin/sh
# 11-grow-gaps.sh — the tether gaps a REAL grow hits, asserted by tether's OWN signatures (never sim
# labels). Option B: grow still reaches VOTER, but ONLY by hand-fixing tether gaps — this drill pins that
# so the exit-0 success can never be consumed as if it were a working `tether cluster add`. RED today;
# flip each to a plain regression the day the product fix lands. Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
GLOG=/tmp/sim-11-grow-brk2.log

drill_begin "#8/#I1 grow gaps (grow runs tether's real commands + exposes where they stall)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers"     "$SIM" up --brokers 3
assert_ok "init brk1 (N=1)"  "$SIM" init brk1

# Run a REAL grow (first grow, N=1→2), capturing its transcript. Option B: it must still reach VOTER —
# but only via the labeled workarounds, which we then assert on tether's OWN output (not sim [GAP] labels).
"$SIM" grow brk2 >"$GLOG" 2>&1 || true
assert_ok "option B: grow brk2 still reaches VOTER via the documented workarounds" \
    sh -c "grep -q 'is now a VOTER' '$GLOG'"

# #8 (REAL tether string, not a sim label): grow runs `join approve --wait --timeout 30s`, which cannot
# complete before the NATS mesh forms and prints tether's own give-up line 'still in flight (<state>)
# after 30s' (cluster_join.go:197). NOTE: this cannot auto-flip on the real #8 fix — the documented fix is
# a new `tether cluster add` orchestration, but the sim hardcodes bare `approve --wait` BEFORE it renders
# the mesh (a sim decision, option B), so a fixed tether would still stall here until cmd_grow is rewritten
# to call the new command. That rewrite is the intended flip trigger.
assert_ok "#8 surfaced: tether's own 'still in flight' from approve --wait pre-mesh (real string, not the sim label)" \
    sh -c "grep -qiE 'still in flight|is BLOCKED' '$GLOG'"

# #3 (REAL tether signature, not a sim label): grow first ATTEMPTS the operator's `reconcile nats --all
# --wait --timeout 25s`, which times out because the in-broker reconciler on the standalone former-N1 conf
# can't harvest route mTLS. Assert BOTH the real CLI timeout string AND the laggard's harvest reason are on
# the record (external-review F1). The day tether fixes the first-grow harvest path, `--all` converges → no
# timeout line → this flips (and grow drops #3 from the trailer).
assert_ok "#3 surfaced: reconcile nats --all timed out with tether's real harvest-failure reason (not a sim label)" \
    sh -c "grep -qiE 'reconcile nats: timed out.*not converged' '$GLOG' && grep -qiE 'no cluster.{0,4}block to harvest' '$GLOG'"

# Mandate ④: grow admits it limped — the GREW-VIA-WORKAROUNDS trailer must name EVERY workaround this
# (first) grow performed, so a product fix that removes ANY one changes the list and flips a token below.
# First-grow (N=1→2) set: #I1 joiner-init · #24 SAN-certs · #5 pty-confirm · #8 approve-stall · #10
# mesh-before-joiner · #3 auto-reconcile-can't-harvest · #4 former-N1-JS-reset · #23 SIGHUP-reload.
for _tok in '#I1' '#24' '#5' '#8' '#10' '#3' '#4' '#23'; do
    assert_ok "trailer names $_tok (first-grow workaround set — dropping it flips this drill)" \
        sh -c "grep -E 'GREW-VIA-WORKAROUNDS:' '$GLOG' | grep -qE '(^|[ ,])$_tok([,]|\$)'"
done
# and it must NOT name #22 on the FIRST grow (#3 and #22 are mutually exclusive per grow; #22 is a later-grow blocker):
assert_ok "trailer does NOT name #22 on the first grow (#3/#22 mutually exclusive)" \
    sh -c "! { grep -E 'GREW-VIA-WORKAROUNDS:' '$GLOG' | grep -qE '(^|[ ,])#22([,]|\$)'; }"

# #I1: a fresh joiner (cluster seam set, but NO `cluster init` → no raft state) cannot serve in cluster
# mode — tether's own `serve` refuses (internal/broker/cutover.go:60). This is an assert_REFUSES, not
# assert_bug: the refusal is a fail-CLOSED invariant tether KEEPS ("the daemon never auto-bootstraps a
# cluster onto a live DB"); assert_bug would wrongly celebrate a safety-regression (guard removed) as
# "APPEARS FIXED". It PROVES the #I1 GAP — the `join` flow does NOT bootstrap the joiner's raft state and
# the runbook §1 joiner path (keygen→prepare→approve) omits the required init step — since brk3 was
# prepared/served without init and left no raft/. brk3 is still fresh (never grown).
assert_ok "brk3 is a fresh broker with no raft state (never grown)" \
    sh -c "! $SIM exec brk3 -- test -d /var/lib/tether/raft"
# mint + distribute brk3's secrets (a real operator provisions the new broker's keys before joining) so
# the serve refusal below is isolated to the MISSING RAFT STATE, not missing secrets.
secrets_mint_node "$INSTANCE" brk3 >/dev/null 2>&1
secrets_distribute "$INSTANCE" brk3 >/dev/null 2>&1
assert_ok "add the cluster seam to fresh brk3 (NO init — as a naive operator following runbook §1 would)" \
    dexec brk3 -- sh -c "printf '  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk3:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.conf\n    nats_server_bin: nats-server\n' >> /etc/tether/broker.yaml"
assert_refuses "#I1 cluster-mode serve refuses a fresh joiner (join does not bootstrap raft state; runbook §1 omits the joiner init)" \
    "no raft state exists|never auto-bootstraps" \
    dexec -u tether brk3 -- sh -c "timeout 8 tether serve --config /etc/tether/broker.yaml 2>&1"

# Deferred follow-on gaps (grow already LABELS + trailer-names them; assertions bespoke): #10 (clustered-
# alone joiner exit-70 — the mesh-before-joiner ordering that dodges it is itself the evidence), #4
# (standalone-JS-no-migrate orphans), #24 (CN-only route cert), #5 (init no machine-escape confirm). #22
# is in 13-inbroker-reconcile-perm.sh; #23 is deferred (its clean-exit trigger is unlocated per gotcha #23
# → not deterministically reproducible from a restart; grow labels + recovers it). The trailer assertions
# above are the regression guard for all of these until dedicated drills land.
drill_end
