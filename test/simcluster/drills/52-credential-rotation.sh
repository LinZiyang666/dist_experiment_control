#!/bin/sh
# 52-credential-rotation — S7 / G-C. N=2: `rotate-tunnel-cert` · the account.nk + CA rotation runbook
# (§2.1) · `cluster keygen` · the C7 guided rotation (`retire --compromised
# --require-credential-rotation`) and its NOT-SAFE alert lifecycle.
# Plan: docs/reviews/s7-s9-plan.md §2.3. Expected landing: PRODUCT-RED (#54 is source-certain).
# Runtime ~12min. Topology: 2 brokers + 1 agent + 1 ctl (grow-family).
#
# ── WHY WRITING THIS MUCH BASH IS *NOT* A MANDATE (4) VIOLATION ─────────────────────────────────────
# This drill mints CAs, distributes keys and rolls brokers by hand. That looks exactly like "the sim
# doing tether's job" — and it is not, for a reason the product states about itself:
#   cmd/tether/cluster_rotation.go:12-20 — "this is a PRINTER/CHECKLIST … NOT an automator. It NEVER
#   generates or moves private key material (rejection #2)."
# So the sim minting keys is PLAYING THE OPERATOR'S PKI: `[by design: rejection #2]`.
# Mandate (4) applies to "tether claims it can and cannot"; it does not apply to "tether declares it will
# not". THE RED LINE: if anyone ever adds a `secrets_rotate_and_reload()` that does the whole thing in one
# call, THAT is Mandate (4)'s inversion — the rotation "succeeding" only because the sim wrote clever bash
# would be tether's failure being concealed. Keep every step separate and visible.
#
# ── THREE roadmap CLAIMS THIS DRILL OVERTURNED (plan §11-K-4 corrects the roadmap) ──────────────────
#  * `cluster keygen` mints a node-ident **U… user seed** (cluster_offline.go:480 -> GenerateUserSeed),
#    NOT an account seed. runbook §2.1 step 2 itself says `nk -gen account`.
#  * `reconcile nats --all --wait` is PURE POLLING ("It NEVER bumps a generation",
#    cluster_reconcile.go:78) — so the runbook's ordering is wrong: you must RESTART the broker first.
#  * There is no "staging/rehearsal tool surface": cluster_rotation.go is 118 lines of printer + banner +
#    alert-raise. No staging/dry-run/verify verb exists.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. A7 without a redial => the curl is green over the OLD connection (rotate-tunnel-cert only hot-swaps
#     the server-side cert; established connections are untouched). But `systemctl restart tether-agent`
#     is ALSO banned: WHEN the agent redials is the very semantics under test, and bouncing it for tether
#     would certify "re-pin works" while hiding "it only works if you restart the whole fleet"
#     (Mandate 2). We inject a partition instead — a real network does that.
#  2. A8 asserting only "the unit failed" would eat ANY crash as green. Guard: the exact
#     `matches neither the pinned` string, read from broker.err (NOT journalctl — R-BROKERLOG).
#  3. B2 asserting only "md5 unchanged" is ALSO true when the reconciler never ran at all. Guard: B0
#     first proves the reconciler DOES re-render on a change it understands. Without B0, #54's mechanism
#     is inferred rather than observed, and Stage-C would rightly throw it out.
#  4. B5d without a control source: "cannot connect via brk1" might just mean brk1's network broke.
#     Guard: the SAME identity succeeds via brk2 in the same window, plus a recovery leg.
#  5. B7 without a brk2-alive guard: "the mesh did not form" might just mean brk2 died.
#  6. SETUP without a JS-meta-formed precondition: B7's "cluster_size dropped to 1" could mean it was
#     never 2. grow_to_2 asserts this unconditionally.
#  7. D6 closing on `resp.OK`/stdout would only re-test the request shape hermetic tests already cover.
#     Guard: read the alert from ctl over REAL NATS.
#  8. D-spine without a clean baseline: a transient `manual:*` from setup would make "the alert is there
#     after retire" pass for the wrong reason.
#  9. #54 via assert_bug would land ASSERT-FAIL ("APPEARS FIXED") because the command SUCCEEDS — the
#     success IS the defect. Guard: R-INVERTED.
# 10. secrets_push_file forgetting chmod 0600 => SecretsPreflight hard-refuses => #54's evidence is
#     laundered into a fake SETUP-RED.
# 11. B2 swaps ONLY account.nk, NEVER the route leaf / tunnel cert: re-minting the tunnel leaf bricks the
#     broker (fp is PINNED in cluster_nodes, clusterwrite.go:173-190), and re-minting the route leaf breaks
#     the route mesh so reconcile goes UNREACHABLE and masks #54. account.nk alone = the mesh-preserving
#     #54 repro.
# 12. A8's trap MUST `systemctl reset-failed`: StartLimitBurst=5/10s is deliberately NOT disabled
#     (install.sh:752-753), so a leftover start-limit would crash B1's baseline and misattribute the root
#     cause to A8.
# 13. FG-guard 3 is assert_setup level, not assert_ok: a brick left over from A8 would masquerade as a
#     product conclusion in arm group B.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"          # sctl / leader_node — agentyaml.sh depends on sctl
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/lib/assert.sh"

SID=lab
PIN=525252
NURL="nats://brk1:4222"
INST="$INSTANCE"

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
_berr() { dexec "$1" -- tail -n "${2:-60}" /var/log/tether/broker.err 2>/dev/null; }

_brk_active()      { [ "$(dexec "$1" -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')" = active ]; }
_brk_not_active()  { ! _brk_active "$1"; }
_nats_active()     { [ "$(dexec "$1" -- systemctl is-active nats-server 2>/dev/null | tr -d '\r')" = active ]; }
# nats.conf renders `    issuer: "<pub>"` (natscluster/config.go:167 uses %q). Extract the quoted value.
_issuer_of()       { dexec "$1" -- sh -c "grep -oE 'issuer:[[:space:]]*\"[A-Z0-9]+\"' /etc/tether/nats.d/nats.conf 2>/dev/null | head -1 | grep -oE '[A-Z0-9]{40,}'" | tr -d '\r'; }
_conf_md5()        { dexec "$1" -- md5sum /etc/tether/nats.d/nats.conf 2>/dev/null | awk '{print $1}'; }
_js_size()         { dexec brk1 -- curl -s --max-time 4 'http://127.0.0.1:8223/jsz?meta=1' 2>/dev/null | jq -r '.meta_cluster.cluster_size // 0'; }
_js_size_is_2()    { [ "$(_js_size)" = 2 ]; }
_js_size_is_1()    { [ "$(_js_size)" = 1 ]; }
_ctl_via()         { "$SIM" exec ctl1 -- sh -c "runuser -u sim -- env HOME=/home/sim tether node ls --nats-url nats://$1:4222 >/dev/null 2>&1"; }
_ctl_via_brk1()    { _ctl_via brk1; }
_ctl_via_brk2()    { _ctl_via brk2; }
_ctl_via_brk1_refused() { ! _ctl_via brk1; }
# `node ls --json` uses `nid` — NOT `node_id`. (`node_id` is `cluster status --json`'s field; the two
# APIs differ, proto/messages.go:379-385 vs the cluster status report.) Querying the wrong key silently
# matches nothing, so a perfectly ONLINE agent polls to timeout and looks like a product failure.
_agt1_online()     { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }
_no_credrot_alert() { ! "$SIM" ctl -- alert ls --json 2>/dev/null | jq -e '.alerts[]?|select(.dedup_key|test("^manual:credrot:"))' >/dev/null 2>&1; }
_credrot_alert_severe() { "$SIM" ctl -- alert ls --json 2>/dev/null | jq -e '.alerts[]?|select(.dedup_key=="manual:credrot:brk2")|select(.severity=="severe")' >/dev/null 2>&1; }
_credrot_alert_gone()   { ! "$SIM" ctl -- alert ls --json 2>/dev/null | jq -e '.alerts[]?|select(.dedup_key=="manual:credrot:brk2")' >/dev/null 2>&1; }


# ── predicates (FUNCTIONS, never `sh -c`: a new shell inherits no harness functions — R-NOSHC) ───────
_a3_fp_agrees() {
    # tether refused the bogus fp AND the fp it reports for the on-disk cert equals ours.
    printf '%s' "$_A3_OUT" | grep -qE 'does not match requested --cert-fp' || return 1
    _a3_got=$(printf '%s' "$_A3_OUT" | grep -oE 'fingerprint "[^"]+"' | head -1 | sed 's/.*"\(.*\)"/\1/' | sed 's/^sha256://')
    [ -n "$_a3_got" ] || return 1
    [ "$_a3_got" = "$FP_OLD" ]
}
_a6_window_readable() {
    _a6=$("$SIM" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' 2>/dev/null)
    # the brk1 node object, whatever the exact field names, must contain: the NEW fp, the OLD fp (prev),
    # and a non-null expiry. Match on the raw node JSON so an unverified path cannot false-fail it.
    _node=$(printf '%s' "$_a6" | jq -c '.nodes[]?|select(.node_id=="brk1" or .nid=="brk1")' 2>/dev/null)
    [ -n "$_node" ] || return 1
    printf '%s' "$_node" | grep -qF "$FP_NEW" || return 1     # new pin present
    printf '%s' "$_node" | grep -qF "$FP_OLD" || return 1     # previous pin retained
    printf '%s' "$_node" | grep -qiE 'valid|until|expir'      # some expiry field present
}
_a8_stage_unpinned_leaf() {
    # EXT-REVIEW-B3: the pre-A8 snapshot + FP_BRK2_OLD capture happen in the MAIN body (see below), NOT
    # here — assert_ok runs this fn inside a $() command substitution (a subshell), so a global set here
    # would be lost in the parent (R-CTX). This fn only mints + lands the new UNPINNED leaf.
    secrets_mint_tunnel_only "$INST" brk2 || return 1
    secrets_push_file "$INST" brk2 tunnel-cert.pem || return 1
    secrets_push_file "$INST" brk2 tunnel-key.pem || return 1
}
_a8_pin_mismatch_logged() { _berr brk2 80 | grep -qE 'matches neither the pinned'; }
_a8_recover_brk2() {
    # EXT-REVIEW-B3: restore the SAVED previous tunnel leaf (the pre-A8 snapshot), NOT the stash copy that
    # secrets_mint_tunnel_only overwrote with the new unpinned leaf. Hard-assert it fingerprints back to
    # the ORIGINAL pin — otherwise "recovery" would re-ship the unpinned brick and this arm would be a lie.
    secrets_restore_tunnel_snapshot "$INST" brk2 preA8 || return 1
    [ "$(secrets_tunnel_fp "$INST" brk2)" = "$FP_BRK2_OLD" ] \
        || { err "_a8_recover_brk2: restored leaf fp != the original pin ($FP_BRK2_OLD) — recovery would be a no-op"; return 1; }
    secrets_push_file "$INST" brk2 tunnel-cert.pem >/dev/null 2>&1
    secrets_push_file "$INST" brk2 tunnel-key.pem >/dev/null 2>&1
    dexec brk2 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start tether-broker' >/dev/null 2>&1
    poll_until 60 3 "brk2's broker is active again" -- _brk_active brk2
}
# B0: prove the reconcilers are ALIVE, so B2's "md5 unchanged" cannot be dismissed as "the reconciler is
# simply dead". EXT-REVIEW (re-run): the old probe dirtied nats.conf with a comment and waited for a
# re-render — but reconcile is GENERATION-GATED (cluster_reconcile.go:39-40: it renders "when the desired
# topology generation advances", NOT over an arbitrary manual edit), so that never happens without a
# generation bump and the probe flaked. The valid liveness proof is the `--all --wait` semantics: it
# BLOCKS until every broker's reconciler reports convergence (cluster_reconcile.go:40), so exit 0 proves
# the reconcilers are alive and reachable — a dead one would hang the wait, not return 0. This is exactly
# the contrast #54 needs: the reconcilers are demonstrably alive (B0), yet a swapped account.nk still does
# not advance the generation, so no re-render happens and the issuer stays OLD (B2) — the mechanism is now
# OBSERVED (alive + inert), not inferred.
_b0_reconciler_alive() { _bt brk1 -- tether cluster reconcile nats --all --wait >/dev/null 2>&1; }
# Swap ONLY the account.nk (the auth_callout issuer's source), NOT the route cert: the route mTLS cert
# needs a nats-server restart to take effect (guide step A.3), and swapping it here would break the route
# mesh and make reconcile report UNREACHABLE — a real fact, but not #54's issuer-skew repro. Leaving the
# route alone keeps the mesh up so reconcile really returns "converged" while the issuer stays OLD = #54.
_b2_push_gen2() {
    _g2acct="$(_secrets_gen_dir "$INST" 2)/account.nk"
    [ -f "$_g2acct" ] || { err "_b2_push_gen2: gen2 account.nk missing"; return 1; }
    for _n in brk1 brk2; do
        d cp "$_g2acct" "$(ctr_name "$_n")":/etc/tether/secrets/account.nk >/dev/null 2>&1 || return 1
        dexec "$_n" -- sh -c 'chown tether:tether /etc/tether/secrets/account.nk && chmod 600 /etc/tether/secrets/account.nk' >/dev/null 2>&1 || return 1
    done
}
_issuer_changed_brk1() { [ -n "$(_issuer_of brk1)" ] && [ "$(_issuer_of brk1)" != "$ACCT_OLD" ]; }
_both_issuers_new()    { _i1=$(_issuer_of brk1); _i2=$(_issuer_of brk2); [ -n "$_i1" ] && [ "$_i1" != "$ACCT_OLD" ] && [ "$_i1" = "$_i2" ]; }
_b6_both_ctl_ok()      { _ctl_via_brk1 && _ctl_via_brk2; }
_b5d_recover() {
    dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start tether-broker' >/dev/null 2>&1
    poll_until 60 3 "the ctl identity connects via brk1 again after it restarts" -- _ctl_via_brk1
}
_c1_keygen_0600() {
    _bt brk1 -- tether cluster keygen --out /tmp/kg.nk >/dev/null 2>&1 || return 1
    [ "$(dexec brk1 -- stat -c %a /tmp/kg.nk 2>/dev/null | tr -d '\r')" = 600 ] || return 1
    [ "$(dexec brk1 -- stat -c %U /tmp/kg.nk 2>/dev/null | tr -d '\r')" = tether ]
}
_c1_pub_matches_nk() {
    # tether's own reported pub vs what the INDEPENDENT nk tool derives. Verifying tether with tether
    # (e.g. `cluster node-pub`) would be circular.
    _c1_t=$(_bt brk1 -- tether cluster keygen --out /tmp/kg2.nk 2>&1 | grep -oE '\bU[A-Z2-7]{50,}\b' | head -1)
    [ -n "$_c1_t" ] || return 1
    _c1_nk=$(dexec brk1 -- nk -inkey /tmp/kg2.nk -pubout 2>/dev/null | tr -d '\r')
    [ -n "$_c1_nk" ] && [ "$_c1_t" = "$_c1_nk" ]
}
_c2_hidden() { ! _bt brk1 -- tether cluster --help 2>&1 | grep -qE '^[[:space:]]+keygen[[:space:]]'; }
_c3_no_out_no_write() {
    _c3_before=$(dexec brk1 -- sh -c 'ls -1 /tmp | wc -l' | tr -d '\r')
    _bt brk1 -- tether cluster keygen >/dev/null 2>&1 || return 1
    [ "$(dexec brk1 -- sh -c 'ls -1 /tmp | wc -l' | tr -d '\r')" = "$_c3_before" ]
}
_d2_warned()          { printf '%s' "$_D2_OUT" | grep -qE 'retire is NOT a credential revocation'; }
_d2_refused_tty()     { printf '%s' "$_D2_OUT" | grep -qiE 'interactive terminal|type the node_id to confirm|aborted'; }
_d2_no_side_effects() {
    _bt brk1 -- tether cluster ops ls 2>/dev/null | grep -qE 'brk2' && return 1
    _no_credrot_alert
}
_d4_op_created()      { printf '%s' "$_D_OUT" | grep -qE 'retire operation .* created'; }
_d4_guide_printed()   { printf '%s' "$_D_OUT" | grep -qF '=== CREDENTIAL ROTATION GUIDE (compromised node brk2) ==='; }
_d4_guide_says_raised(){ printf '%s' "$_D_OUT" | grep -qE 'severe alert manual:credrot:brk2'; }
_d5_banner()          { printf '%s' "$_D_OUT" | grep -qE 'credentials are NOT yet rotated .* can still authenticate'; }
_d7_stdout_parseable(){ "$SIM" ctl -- node ls --json 2>/dev/null | jq -e '.nodes' >/dev/null 2>&1; }

# ONE cleanup trap (R-TRAP). reset-failed is mandatory: A8 deliberately builds a brick and
# StartLimitBurst=5/10s is deliberately NOT disabled by install.sh (:752-753).
_cleanup() {
    fault_cleanup_all || true
    for _n in brk1 brk2; do
        dexec "$_n" -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker 2>/dev/null' >/dev/null 2>&1 || true
    done
    rm -rf /tmp/52-gen2 2>/dev/null || true
    true
}

drill_begin "52-credential-rotation (N=2: rotate-tunnel-cert + account.nk/CA rotation + keygen + C7 guided rotation)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

# ── SETUP + the three false-green guards ────────────────────────────────────────────────────────────
assert_setup "grow_to_2 (N=2 VOTER + JS meta FORMED at size 2 + leader pinned brk1)" grow_to_2 1 1
assert_setup "session $SID + ctl login"                     "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                              "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (tunnel_addr, S0-tunnel)" agent_provision_yaml agt1 "$SID" "$NURL" open
TOK=$(expose_serve_sentinel agt1 8092) || setup_fail "could not start the sentinel http.server on agt1"
assert_setup "expose agt1:8092 --on-broker brk1 --name rot" "$SIM" ctl -- expose agt1 --local 8092 --on-broker brk1 --name rot
PUB=$("$SIM" ctl -- expose explain rot --json 2>/dev/null | jq -r '.public_port // empty')
[ -n "$PUB" ] || setup_fail "could not read the public port of expose 'rot'"
# FG-guard 2: the data plane really works before we touch anything.
assert_setup "FG-guard 2: pre-rotation data plane serves the exact sentinel" \
    poll_until 30 2 "sentinel via brk1:$PUB" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
# FG-guard 3 is assert_setup (NOT assert_ok) on purpose: B5d's whole discriminating power is "brk1 refuses
# while brk2 accepts the same identity". If either path is already broken here, later B-group results would
# be harness artifacts wearing a product conclusion's clothes.
assert_setup "FG-guard 3a: the ctl identity connects via brk1 (B5d's control baseline)" _ctl_via_brk1
assert_setup "FG-guard 3b: the SAME identity connects via brk2 (B5d's control baseline)" _ctl_via_brk2

# ═══ ARM GROUP A — rotate-tunnel-cert ═══════════════════════════════════════════════════════════════
# NB the signature must NOT start with `--`: assert_refuses feeds it to `grep -qiE "$sig"`, and grep
# parses a leading `-` as an option even inside quotes. Anchor on `cert-fp is required` (still unique).
assert_refuses "A1 rotate-tunnel-cert without --cert-fp is refused" \
    "cert-fp is required" \
    _bt brk1 -- tether cluster rotate-tunnel-cert brk1
FP_OLD=$(secrets_tunnel_fp "$INST" brk1) || setup_fail "could not compute brk1's on-disk tunnel-cert fingerprint"
# A3 doubles as a harness self-check: if OUR fp math disagreed with tether's, every later fp assertion
# would be meaningless. tls.go:91-94 defines fp = sha256(cert.Raw) = sha256 over the DER bytes.
_A3_OUT=$(_bt brk1 -- tether cluster rotate-tunnel-cert brk1 --cert-fp "sha256:0000000000000000000000000000000000000000000000000000000000000000" 2>&1) || true
assert_ok "A3 a wrong --cert-fp is refused AND the fp tether reports for the on-disk cert equals what secrets_tunnel_fp computes (harness self-check: a mismatch here would invalidate every later fp oracle)" \
    _a3_fp_agrees

# A4 — #56: the two refusals form a LOOP.
assert_refuses "A4a on the FOLLOWER, rotate is redirected to the leader host" \
    "not the leader|re-run on the leader host" \
    _bt brk2 -- tether cluster rotate-tunnel-cert brk2 --cert-fp "sha256:$FP_OLD"
_A4_OUT=$(_bt brk1 -- tether cluster rotate-tunnel-cert brk2 --cert-fp "sha256:$FP_OLD" 2>&1) || true
if printf '%s' "$_A4_OUT" | grep -qE 'transfer leadership to brk2 first'; then
    product_red "#56 rotate-tunnel-cert gives CIRCULAR advice: on the follower it says 'not the leader — re-run on the leader host: brk1' (clusterstatus.go:649-657 + cluster.go:625), but on the leader, for that same target, it says 'transfer leadership to brk2 first' (clusterdrain.go:386-388). An operator following each message in turn is sent back and forth forever; the real requirement (the target must BE the leader) is never stated in one place."
elif printf '%s' "$_A4_OUT" | grep -qE 'not the leader|leader host'; then
    _as_fail "#56 UNJUDGEABLE — the leader-side rotate for a remote target gave a leader-redirect rather than the self-only refusal; the loop premise does not hold as modelled. Captured: $(printf '%s' "$_A4_OUT" | tail -1)"
else
    _as_fail "#56 APPEARS FIXED / UNJUDGEABLE — the leader-side rotate for a remote target neither looped nor refused as documented. Captured: $(printf '%s' "$_A4_OUT" | tail -1)"
fi

assert_ok "A5a mint a NEW tunnel leaf for brk1 (operator PKI — [by design: rejection #2])" \
    secrets_mint_tunnel_only "$INST" brk1
FP_NEW=$(secrets_tunnel_fp "$INST" brk1) || setup_fail "could not fingerprint the new tunnel leaf"
assert_ok "A5b the new leaf really is a different identity" sh -c "[ \"$FP_NEW\" != \"$FP_OLD\" ]"
assert_ok "A5c push the new leaf onto brk1 (0600, tether-owned — SecretsPreflight hard-refuses 0077 bits)" \
    secrets_push_file "$INST" brk1 tunnel-cert.pem
assert_ok "A5d push the matching key" secrets_push_file "$INST" brk1 tunnel-key.pem
assert_ok "A5 rotate-tunnel-cert commits and hot-swaps the LIVE certificate" \
    out_matches 'cert rotation committed; target broker hot-swapped its live tunnel certificate' \
    _bt brk1 -- tether cluster rotate-tunnel-cert brk1 --cert-fp "sha256:$FP_NEW"
# A6 — the deploy-tier delivery of the roadmap's "pin rotation window" line: the window is REAL and
# readable, carrying both the previous pin and an expiry.
# The exact JSON path for the per-node cert pins is not source-confirmed here, so probe the real status
# and require the NEW fp + the OLD fp (as prev) + an expiry to all be present SOMEWHERE in brk1's node
# object. Proves the window is observable and carries both pins without hard-coding an unverified path.
assert_ok "A6 the rotation WINDOW is observable: brk1's node carries the new pin, the previous pin, and an expiry (probed, not a guessed path)" \
    _a6_window_readable

# A7 — re-pin under real traffic. EXT-REVIEW-B4: the old first stage polled the ALREADY-ESTABLISHED tunnel
# and called a successful curl "the agent re-pinned on its own" — but an existing TLS session keeps serving
# bytes WITHOUT ever observing the rotated cert, so that proved nothing (and usually pre-empted the real
# redial stage). There is no connection-generation signal to prove a self-repin from the data plane alone,
# so we do the only self-proving thing: UNCONDITIONALLY force a fresh redial by cutting the tunnel port
# briefly (an INJECTION a real network does, NOT an operator action — `systemctl restart tether-agent` is
# banned: it would certify "re-pin works" while hiding "only a fleet-wide bounce works"), then require the
# agent to RECONNECT with a new TLS handshake against the ROTATED server cert and serve the sentinel again.
# EXT-REVIEW round-2 R2-F1: a SHORT DROP only blocks new SYNs; the established TLS/yamux session survives
# (yamux.Client(conn,nil) uses the default 30s keepalive, tunnel.go:1032), so post-heal traffic proves
# nothing. So we blackhole brk1's TUNNEL LISTENING PORT (7000) for LONGER than the keepalive.
# FAULT WIRING (ext-review round-3): block it on brk1 (the node that LISTENS on 7000), not on agt1. Both
# forms cut agt1<->brk1:7000 (SIMFAULT is on BOTH INPUT+OUTPUT with BOTH --dport/--sport, so an agt1-side
# rule ALSO drops agt1's egress to :7000 — empirically fault on => agt1->brk1:7000 rc 1->124), but blocking
# the LISTENING port on brk1 is the unambiguous idiom the reviewer asked for and touches only the tunnel
# (7000), leaving NATS/route/raft/public-port up so the rest of the cluster stays healthy. We do NOT also
# cut NATS: the down->up DATA-PLANE edge is the load-bearing proof and does not need it.
# Require THREE facts before claiming re-pin:
#   (1) a DATA-PLANE DOWN-EDGE — the public port stops serving, proving the OLD tunnel session actually
#       DIED (past the 30s keepalive), not merely that new connects are blocked. No down-edge -> not_covered.
#   (2) the data plane serving the sentinel again — the fresh yamux redial (supervise -> redialWithBackoff,
#       tunnel.go:1063 reads the CURRENT rotated cert pins) succeeded against the ROTATED cert = genuine re-pin.
#   (3) a journalctl reconnect corroboration — INDEPENDENT, logged but NOT gating (its exact line is
#       agent-reconnect-path dependent, and with NATS left up it is usually unconfirmed; the data-plane edge carries it).
_A7_TS=$(dexec agt1 -- date '+%Y-%m-%d %H:%M:%S' 2>/dev/null | tr -d '\r')
_a7_dp_down()     { ! dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"; }
_a7_reconnected() { dexec agt1 -- sh -c "journalctl -u tether-agent --since '$_A7_TS' --no-pager 2>/dev/null | grep -qE 're-registered after reconnect|agent: registered|rebuilding session'"; }
assert_ok "A7a inject: blackhole brk1's tunnel LISTENING port 7000 (silent DROP on brk1) — long enough to outlast the 30s yamux keepalive so agt1's OLD session actually dies (the only way to force a genuine redial); only 7000 is cut, so NATS/route/raft stay up and the cluster is otherwise healthy" \
    fault_partition_on brk1 7000
assert_ok "A7b PROOF the block took: agt1's connect to brk1:7000 HANGS (rc=124, not a refusal) — impossible unless the tunnel port is dropped, so it is the down-edge's proven fault source" \
    poll_until 20 2 "agt1->brk1:7000 blackholed" -- fault_assert_blackholed agt1 brk1 7000
# The re-pin proof is the DOWN-EDGE + UP-EDGE pair: the old session provably DIES (down-edge, past the
# keepalive), then a FRESH session re-serves the sentinel (up-edge). Because the old session is provably
# gone, the up-edge can ONLY be a fresh yamux redial (supervise->redialWithBackoff reads the CURRENT rotated
# cert pins, tunnel.go:1063) — the exact thing a short DROP could never show. The journal reconnect is an
# extra INDEPENDENT corroboration, logged but not gating (its exact line is agent-reconnect-path dependent).
if poll_until 80 3 "the DATA PLANE goes DOWN — the OLD tunnel session died past the 30s keepalive (not just new connects blocked)" -- _a7_dp_down; then
    assert_ok "A7c heal the partition" fault_partition_off brk1
    if poll_until 100 3 "the data plane serves the exact sentinel again — after a PROVEN down-edge this can only be a fresh redial that re-pinned to the ROTATED cert" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"; then
        if _a7_reconnected; then _a7_rc="journal-CONFIRMED (agt1 re-registered/​rebuilt in the same window)"; else _a7_rc="journal-unconfirmed (the down->up data-plane edge is the load-bearing proof)"; fi
        _as_pass "A7d GENUINE re-pin proven: the OLD session provably DIED (data-plane down-edge past the 30s keepalive), then a FRESH session re-served the sentinel against brk1's ROTATED cert (up-edge) — NOT surviving-session traffic. Reconnect: $_a7_rc"
    else
        product_red "#63 CANDIDATE: after 'rotate-tunnel-cert' + a partition that provably killed the old tunnel session (down-edge seen), the data plane never returned within 100s — the fresh handshake against the rotated cert failed. rotate-tunnel-cert only hot-swaps the server cert (clusterwrite.go:247-250); if the fleet cannot re-pin on redial, the documented online rotation is unusable."
    fi
else
    assert_ok "A7c heal the partition (down-edge not observed)" fault_partition_off brk1
    not_covered "52 A7 re-pin (no session drop)" "the data plane never went down within 80s of blackholing brk1's tunnel port — the established TLS/yamux session outlasted the window, so a genuine redial could not be forced in-sim; re-pin is not judged rather than falsely claimed from surviving-session traffic (this is exactly the round-1 false-green the external review flagged)"
fi

# A8 — the fail-closed brick + DOC-23.
# EXT-REVIEW-B3: snapshot brk2's currently-pinned leaf and capture its fingerprint HERE, in the main body
# — assert_ok runs its function inside a $() subshell, so a global set inside _a8_stage_unpinned_leaf is
# lost in the parent (R-CTX). This is the exact previous generation that A8f must roll back to.
assert_ok "A8-pre snapshot brk2's currently-pinned tunnel leaf so recovery can restore the exact old generation (not the new unpinned brick)" \
    secrets_snapshot_tunnel "$INST" brk2 preA8
FP_BRK2_OLD=$(secrets_tunnel_fp "$INST" brk2) || setup_fail "could not fingerprint brk2's original pinned tunnel leaf for the A8 recovery gate"
assert_ok "A8a mint a new tunnel leaf for brk2 and land it WITHOUT rotating the pin" \
    _a8_stage_unpinned_leaf
assert_ok "A8b restart brk2's broker (it must now refuse to start: the on-disk cert matches neither pin)" \
    dexec brk2 -- sh -c 'systemctl restart tether-broker 2>/dev/null; true'
assert_ok "A8c brk2's broker does NOT reach active (fail-closed, as designed)" \
    poll_until 25 3 "brk2 broker stays down" -- _brk_not_active brk2
# The exact string, read from broker.err. "the unit failed" would swallow ANY crash as green.
assert_ok "A8d broker.err carries the EXACT pin-mismatch refusal (not merely 'the unit failed' — that would eat any crash as green)" \
    _a8_pin_mismatch_logged
# DOC-23: the error text's own second remedy is unreachable — wireClusterEarly returns at broker.go:691,
# long before the admin socket is created at :1060.
_A8_REM=$(_bt brk2 -- tether cluster rotate-tunnel-cert brk2 --cert-fp "sha256:$(secrets_tunnel_fp "$INST" brk2)" 2>&1) || true
if printf '%s' "$_A8_REM" | grep -qiE 'no such file|connection refused|socket'; then
    product_red "DOC-23 the pin-mismatch error tells the operator to 're-run tether cluster rotate-tunnel-cert', but that remedy is UNREACHABLE in the state it appears in: wireClusterEarly fails at broker.go:691 and returns before the admin socket is created at :1060, so the command cannot connect. The only real way out is restoring the old cert file. Captured: $(printf '%s' "$_A8_REM" | tail -1)"
else
    ok "DOC-23 does not hold — the admin socket answered while the broker was bricked; record and drop it"
    _as_pass "A8e the suggested remedy is reachable in the bricked state (DOC-23 withdrawn)"
fi
assert_ok "A8f restore brk2's SAVED OLD leaf (pre-A8 snapshot, fp==original pin — NOT the new unpinned leaf) and bring it back; the fail-closed broker reaching active is itself proof the restored cert matches the pin (reset-failed first: StartLimitBurst=5/10s is deliberately NOT disabled)" \
    _a8_recover_brk2
assert_ok "A8g N=2 VOTER + JS meta back at size 2 before arm group B (proves the cluster fully re-converged on the recovered old pin, not just that the unit started)" \
    poll_until 120 3 "JS meta back at 2" -- _js_size_is_2

# ═══ ARM GROUP C — cluster keygen (MOVED here, before the B-group rotation — Stage-C B4) ═════════════
# keygen is a pure OFFLINE action (mint a key + cross-check with independent `nk`), auth-independent. It
# ran AFTER the B-group + D-group gate originally, so when #54 broke auth the D-group gate exited the
# drill and keygen (inventory row 179) was silently skipped — an orphan. Run it here while the cluster is
# still healthy; it does not depend on the rotation at all.
assert_ok "C1a keygen mints a key on real disk at 0600" _c1_keygen_0600
# Cross-check against an INDEPENDENT tool. Using tether's own `cluster node-pub` would be circular.
assert_ok "C1b the public key tether prints matches what the independent \'nk\' tool derives from the seed (never verify tether with tether)" \
    _c1_pub_matches_nk
assert_ok "C2a keygen is Hidden: it does not appear in \'cluster --help\'" \
    _c2_hidden
assert_ok "C2b but it is runnable and self-documents" _bt brk1 -- tether cluster keygen --help
assert_ok "C3 without --out, keygen writes nothing to disk" _c3_no_out_no_write

# ═══ ARM GROUP B — account.nk + CA rotation (the heart of the drill) ════════════════════════════════
ACCT_OLD=$(_issuer_of brk1)
[ -n "$ACCT_OLD" ] || setup_fail "could not read brk1's auth_callout issuer from nats.conf"
MD5_1_0=$(_conf_md5 brk1); MD5_2_0=$(_conf_md5 brk2)
assert_ok "B1 baseline: both brokers' nats.conf carry the SAME account issuer" \
    sh -c "[ \"$ACCT_OLD\" = \"$(_issuer_of brk2)\" ]"

# B0 — THE LOAD-BEARING CONTROL. Without it, B2's "md5 unchanged" is equally true when the reconciler is
# simply dead, and #54's mechanism would be inferred rather than observed.
assert_ok "B0 CONTROL: the reconcilers are ALIVE — 'reconcile nats --all --wait' blocks on every broker's reconciler reporting convergence, so exit 0 proves none is dead (a dead one would hang the wait). This is what makes B2's 'issuer unchanged' mean 'alive reconciler chose not to act', not 'reconciler never ran'" \
    _b0_reconciler_alive

# B2 — #54. INVERTED: the command SUCCEEDING while nothing changes IS the defect.
assert_ok "B2a mint generation-2 trust material (new account.nk + new CA), leaving _shared/gen1 untouched" \
    secrets_mint_gen "$INST" 2
assert_ok "B2b swap ONLY the account.nk (the auth_callout issuer's source) on BOTH brokers — NOT the route leaf or CA (that would break the route mesh and mask #54 behind an UNREACHABLE; the account.nk skew alone reproduces #54 while the mesh stays up)" \
    _b2_push_gen2
_B2_OUT=$(_bt brk1 -- tether cluster reconcile nats --all --wait 2>&1); _B2_RC=$?
MD5_1_1=$(_conf_md5 brk1); MD5_2_1=$(_conf_md5 brk2); ACCT_NOW=$(_issuer_of brk1)
if [ "$_B2_RC" != 0 ] && printf '%s' "$_B2_OUT" | grep -qiE 'UNREACHABLE|not converged|timed out'; then
    product_red "#54 rotating account.nk on a RUNNING cluster cannot even proceed: \`reconcile nats --all --wait\` fails with brk2(UNREACHABLE) because the live auth_callout can no longer validate connections against the swapped-but-not-reloaded seed (serve.go:203-218 loads seeds ONCE at startup; cluster_reconcile.go:78 is pure polling and NEVER bumps a generation). There is no product-level re-render+verify path and no atomic switch-over — the operator is left with a half-rotated, partially-unreachable cluster. Captured: $(printf '%s' "$_B2_OUT" | tail -1)"
    B2_ROTATION_LIVE=0
elif [ "$_B2_RC" = 0 ] && printf '%s' "$_B2_OUT" | grep -qiE 'converged|generation'; then
    if [ "$MD5_1_1" = "$MD5_1_0" ] && [ "$MD5_2_1" = "$MD5_2_0" ] && [ "$ACCT_NOW" = "$ACCT_OLD" ]; then
        product_red "#54 rotating account.nk has NO product-level re-render or verify path, and \`reconcile nats --all --wait\` reports a FALSE ALL-CLEAR: it printed convergence and exited 0 while BOTH brokers' nats.conf are byte-identical to the pre-rotation baseline and auth_callout still issues against the OLD account key ($ACCT_OLD). Mechanism: reconcile is pure polling (cluster_reconcile.go:78) and auth_callout seeds load ONCE at startup (serve.go:203-218), so nothing short of a broker restart adopts the new issuer."
        B2_ROTATION_LIVE=0
    elif [ "$ACCT_NOW" != "$ACCT_OLD" ]; then
        _as_fail "#54 APPEARS FIXED — reconcile re-rendered the issuer to $ACCT_NOW without a restart; promote B2 and flip the ledger"
        B2_ROTATION_LIVE=1
    else
        _as_fail "#54 UNJUDGEABLE — reconcile exited 0 but conf md5s changed while the issuer did not (brk1 $MD5_1_0->$MD5_1_1)"
        B2_ROTATION_LIVE=0
    fi
else
    _as_fail "#54 UNJUDGEABLE — reconcile nats --all --wait failed for an unclassified reason (rc=$_B2_RC): $(printf '%s' "$_B2_OUT" | tail -1)"
    B2_ROTATION_LIVE=0
fi

# B3 — #54's second facet: doctor is blind to the skew it just created.
_B3_OUT=$(_bt brk1 -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /etc/tether/nats.d/nats.conf --json 2>&1); _B3_RC=$?
_B3_FATAL=$(printf '%s' "$_B3_OUT" | jq -r '.summary.fatal // empty' 2>/dev/null)
if [ "$_B3_RC" = 0 ] && [ "${_B3_FATAL:-}" = 0 ] && [ "$ACCT_NOW" = "$ACCT_OLD" ]; then
    product_red "#54 (facet 2) \'cluster doctor --offline\' reports 0 fatal on a broker whose on-disk account.nk no longer matches the auth_callout issuer its nats.conf is rendered with — i.e. it is blind to exactly the skew a half-finished rotation produces. The only code that can print this skew (cluster_secrets.go:46-47) hangs off the rotation guide and \'init --from-existing\'; a RUNNING cluster has no verb that surfaces it."
elif [ "$ACCT_NOW" != "$ACCT_OLD" ]; then
    ok "B3 skipped — no skew exists to be blind to (B2 appears fixed)"
else
    _as_fail "#54 (facet 2) UNJUDGEABLE — doctor rc=$_B3_RC fatal=${_B3_FATAL:-?} under a known account/issuer skew; triage"
fi

# ── B4-B7 GATE ──────────────────────────────────────────────────────────────────────────────────────
# #54 is pinned above (B2/B3). Everything that followed in the original design — "restart adopts the new
# issuer" (B4), the skew probe (B5/B5d), "the fleet is alive under the new anchor" (B6/B7) — assumed the
# live rotation could be driven to completion. The finding is that it CANNOT: swapping account.nk on a
# running cluster leaves reconcile UNREACHABLE (or a false all-clear), with no atomic switch-over verb.
# So the tail is recorded as INCOMPLETE rather than cascaded into a dozen asserts against a half-rotated
# cluster. The rolling-restart recovery + fresh-CA route-leaf rejection (#55/B7) are owed to staging where
# a full out-of-band re-mint + coordinated rolling restart can be staged safely.
if [ "${B2_ROTATION_LIVE:-0}" = 0 ]; then
    # Stage-C B5: the plan said "B5d runs unconditionally", but that is impossible GIVEN #54, and this
    # gate is the honest consequence. B5d's precondition is "brk1's conf issuer == NEW while only brk2's
    # OLD-key responder answers". #54 is precisely that the issuer NEVER becomes NEW on a running cluster
    # (reconcile is pure polling and NEVER re-renders — cluster_reconcile.go:78 + serve.go:203-218's
    # once-at-startup seed load). So the NEW-issuer skew window B5d needs cannot be constructed live at
    # all — the rotation cannot even reach the state where #55's rejection window would exist. The plan's
    # "B5d runs unconditionally" was written before this was measured; the finding supersedes it, and the
    # not_covered reason is source-level rather than "owed to staging".
    not_covered "52 B4-B7 + B5d (live-rotation completion + #55 auth-rejection window)" \
        "the account.nk rotation cannot be driven to the NEW-issuer state on a RUNNING cluster — that IS #54 (reconcile never re-renders the issuer: cluster_reconcile.go:78 + serve.go:203-218 load auth_callout seeds ONCE at startup). B5d's precondition (brk1 issuer==NEW while brk2 still answers with OLD) therefore cannot be constructed live, so #55's auth-rejection window has no reproducible in-sim state — it is a downstream consequence of #54, not a separately testable window. A full rotation needs a coordinated rolling restart with the fleet's credentials re-signed out-of-band; owed to staging"
fi

# ── REBUILD a clean cluster for the INDEPENDENT D-group (C7 guided rotation) ─────────────────────────
# D (retire --compromised --require-credential-rotation) does not depend on the account.nk rotation, so
# restore the gen-1 trust material and a healthy N=2 before running it.
for _n in brk1 brk2; do
    d cp "$(_secrets_shared_dir "$INST")/account.nk" "$(ctr_name "$_n")":/etc/tether/secrets/account.nk >/dev/null 2>&1 || true
    dexec "$_n" -- sh -c 'chown tether:tether /etc/tether/secrets/account.nk && chmod 600 /etc/tether/secrets/account.nk && systemctl reset-failed tether-broker nats-server 2>/dev/null; systemctl restart nats-server tether-broker' >/dev/null 2>&1 || true
done
# Health check must exercise AUTH, not just JS meta (JS forms even when auth_callout is broken, which
# would false-pass this gate and run the D-group against an auth-broken cluster). A real ctl operation
# round-trips through auth_callout, so it fails iff auth is genuinely working again. It ALSO must confirm
# brk1's LOCAL admin socket answers: `retire --require-credential-rotation` opens with a
# callAdmin(OpClusterStatus) pre-check (cluster_retire.go:39) BEFORE its confirm prompt, and that socket
# comes up LATE in broker startup (after the rebuild restart) — so a NATS-auth-only gate would run the
# retire before the socket exists and it would exit rc=69 "child exited before confirm" for a
# setup-timing reason unrelated to C7. A `tether cluster status` on brk1 goes through that same socket.
_d_cluster_authworks() {
    "$SIM" ctl -- node ls --json >/dev/null 2>&1 && _js_size_is_2 \
        && _bt brk1 -- tether cluster status >/dev/null 2>&1
}
if ! poll_until 120 3 "N=2 with WORKING AUTH again for the D-group" -- _d_cluster_authworks; then
    not_covered "52 D-group (C7 guided rotation)" "could not rebuild a cluster with working auth after the B-group account.nk rotation broke it; the C7 retire arm round-trips through auth_callout and needs a live authenticating cluster. The account.nk rotation left it unrecoverable in-sim (a further consequence of #54: no atomic rotation, no in-place recovery). C7's guide/alert lifecycle is hermetically covered (cluster_rotation_test.go)"
    drill_end; exit "$?"
fi


# ═══ ARM GROUP D — C7 guided rotation ═══════════════════════════════════════════════════════════════
assert_ok "D0 CLEAN BASELINE: no manual:credrot:* alert exists before we retire anything (a transient one from setup would make D6 pass for the wrong reason)" \
    _no_credrot_alert
assert_refuses "D1 --require-credential-rotation without --compromised is a usage error" \
    "requires --compromised" \
    _bt brk1 -- tether cluster retire brk2 --require-credential-rotation
# D3 — #36's family: retire registers 5 flags and NO yes-rejector (unlike restore), so --yes is not a
# Tier-2 refusal at all — it is an unknown flag. Same divergence, no new number.
assert_refuses "D3 --yes on retire is an UNKNOWN FLAG (not a Tier-2 refusal like restore's): retire registers no yes-rejector — the #36 divergence, not a new number" \
    "unknown flag: --yes" \
    _bt brk1 -- tether cluster retire brk2 --compromised --require-credential-rotation --yes
# D2 — the real TTY gate. Hermetic tests inject via cmd.SetIn and NEVER exercise the
# `in == os.Stdin && !term.IsTerminal` branch; only a real non-TTY exec does.
_D2_OUT=$(_bt brk1 -- tether cluster retire brk2 --compromised --require-credential-rotation 2>&1) || true
assert_ok "D2a the WARNING precedes the confirm (the operator is told retire != revocation BEFORE being asked)" \
    _d2_warned
assert_ok "D2b non-interactive is refused at the real TTY gate (the branch hermetic tests structurally cannot reach)" \
    _d2_refused_tty
assert_ok "D2c the refusal had ZERO side effects: no op created, no alert raised" _d2_no_side_effects

# D-spine — single attempt, branch on the real output (#31 is intermittent; never hardcode either outcome).
_D_OUT=$(_pty brk1 brk2 -- tether cluster retire brk2 --compromised --require-credential-rotation 2>&1); _D_RC=$?
if printf '%s' "$_D_OUT" | grep -qiE 'grow of .* is in progress|already in flight'; then
    product_red "#31 the leaked \'cluster add\' grow lock BLOCKS the C7 guided rotation spine — and it does so AFTER the operator has already been made to type the node id (cluster_retire.go:50's callAdmin runs after :47's typed confirm), which is a new facet of #31's blast radius. Captured: $(printf '%s' "$_D_OUT" | tail -1)"
    not_covered "52 D4-D8: the C7 guided-rotation alert lifecycle (guide + severe manual:credrot alert raise -> read from ctl -> clear)" "the retire op could not be created: blocked by the #31 grow-lock leak, exactly as it blocks drills 40/41. Not clearing it by inventing a workaround: G-B proved even the canonical clear does not work (ledger:186-198)"
elif [ "$_D_RC" = 0 ] || printf '%s' "$_D_OUT" | grep -q 'retire operation'; then
    assert_ok "D4a the retire op was created"        _d4_op_created
    assert_ok "D4b the guided rotation checklist was printed" _d4_guide_printed
    assert_ok "D4c the guide states the alert was RAISED (the single discriminator — the checklist body itself is hermetically covered)" \
        _d4_guide_says_raised
    assert_ok "D5 the NOT-SAFE banner is printed and contains no 'done/safe/complete' wording (rejection #5's floor)" \
        _d5_banner
    # D6 — the signature arm. Hermetic tests only ever proved the raise REQUEST's shape against a stub
    # adminsock; nobody has ever shown the alert lands in the store and is readable by a member over NATS.
    assert_ok "D6 THE SIGNATURE ARM: ctl reads the severe manual:credrot:brk2 alert over REAL NATS (hermetic only ever proved the raise request's SHAPE against a stub socket — never that it persists and is readable)" \
        poll_until 30 2 "the credrot alert is readable from ctl" -- _credrot_alert_severe
    assert_ok "D7 with a severe alert live, stdout stays parseable (the banner goes to stderr)" _d7_stdout_parseable
    assert_ok "D8a alert clear removes it" "$SIM" ctl -- alert clear manual:credrot:brk2
    assert_ok "D8b it is really gone" poll_until 20 2 "the credrot alert is gone" -- _credrot_alert_gone
    assert_ok "D8c clear is idempotent" "$SIM" ctl -- alert clear manual:credrot:brk2
else
    _as_fail "D-spine UNJUDGEABLE — retire --compromised --require-credential-rotation failed for an unclassified reason (rc=$_D_RC): $(printf '%s' "$_D_OUT" | tail -1)"
fi

# NOT a defect, recorded so nobody files it later: `alert clear` deliberately does not verify the rotation
# actually happened (cluster_rotation.go:13-16, rejection #5 — the product says so about itself).

drill_end
