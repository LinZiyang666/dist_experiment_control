#!/bin/sh
# 52-credential-rotation — S7 / G-C. N=2: `rotate-tunnel-cert` · the account.nk + CA rotation runbook
# (§2.1) · `cluster keygen` · the C7 guided rotation (`retire --compromised
# --require-credential-rotation`) and its NOT-SAFE alert lifecycle.
# Plan: docs/reviews/s7-s9-plan.md §2.3 + docs/reviews/r2-plan.md (R11 drill flip). Expected landing: R11
# CLOSED #54/#55/#56/#63/DOC-23 — those arms are now POSITIVE regressions. The remaining non-green driver
# is the D-spine's intermittent #31 grow-lock (→ PRODUCT-RED when it fires) or the A7 runtime-guard (→
# INCOMPLETE when the established tunnel session outlasts the kill window); otherwise GREEN.
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
#     `matches neither the pinned` string, read from the broker slog (NOT journalctl — R-BROKERLOG).
#     h1 F3: the slog is broker.log now (broker.err pre-h1); sim_broker_slog reads both.
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
#  9. #54/#55 (R11 FIXED): B2/B3 assert the skew is now VISIBLE (reconcile FAILS-CLOSED, doctor FATALs);
#     the B-55 arm CONSTRUCTS the partial-rotation skew (restart ONE broker) and asserts per-broker
#     visibility with a non-vacuity control (the adopting broker's skew CLEARS). No longer an inversion.
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
. "$HERE/drills/lib/logs.sh"
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
# h1 F3: broker slog moved broker.err -> broker.log; sim_broker_slog reads both.
_berr() { sim_broker_slog "$1" "${2:-60}"; }

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
# ── #54/#55 auth_callout issuer-skew oracles (R11 P6 made these skews VISIBLE) ──────────────────────
# doctor --offline reads ONLY local files (secrets + nats.conf) so it works even when the cluster's
# client auth is broken. It appends a FATAL `auth-issuer-skew` check when the on-disk account.nk derives
# a DIFFERENT auth_callout issuer than the one rendered into nats.conf (cluster_natsconf.go:478 ->
# clusterAuthIssuerSkewChecks), and renderDoctor exits NON-ZERO whenever fatal>0 (cluster_natsconf.go:524).
_doctor_offline() { _bt "$1" -- tether cluster doctor --offline --secrets-dir /etc/tether/secrets --db /var/lib/tether/tether.db --conf /etc/tether/nats.d/nats.conf --json 2>&1; }
_doctor_issuer_skew_fatal() {  # $1 broker: doctor FATALs auth-issuer-skew AND exits non-zero
    _dsf_out=$(_doctor_offline "$1"); _dsf_rc=$?
    [ "$_dsf_rc" != 0 ] || return 1                                     # fail-closed: a skew must be a non-zero exit
    printf '%s' "$_dsf_out" | jq -e '.checks[]?|select(.name=="auth-issuer-skew")|select(.status=="FATAL")' >/dev/null 2>&1 || return 1
    _dsf_f=$(printf '%s' "$_dsf_out" | jq -r '.summary.fatal // 0' 2>/dev/null)   # .fatal is an int; `// 0` only guards null
    [ "${_dsf_f:-0}" -ge 1 ] 2>/dev/null
}
_doctor_no_issuer_skew() {     # $1 broker: NO auth-issuer-skew check present (the skew cleared / never was)
    ! printf '%s' "$(_doctor_offline "$1")" | jq -e '.checks[]?|select(.name=="auth-issuer-skew")' >/dev/null 2>&1
}
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
# #56 (R11 P11 FIXED): rotate-tunnel-cert is a SELF-ONLY verb that BYPASSES the generic mutating-verb
# leader-redirect (clusterstatus.go:665) and gives self-only guidance (clusterdrain.go:400-414). The two
# arms read outputs CAPTURED in the main body (globals, so a $()-subshell predicate can read them — R-CTX).
_a4a_selfonly_guidance() {
    # on the FOLLOWER, rotating SELF: "this IS the target broker, but it is a FOLLOWER … Transfer
    # leadership to it" — and NEVER the OLD wrong generic redirect "re-run on the leader host".
    printf '%s' "$_A4A_OUT" | grep -qiE 'this IS the target broker, but it is a FOLLOWER|transfer leadership to it' || return 1
    ! printf '%s' "$_A4A_OUT" | grep -qiE 're-run on the leader host'
}
_a4b_wronghost_guidance() {
    # on the LEADER for a REMOTE target: "must run ON the target broker brk2 … Re-run it on brk2" — NOT a
    # circular loop and NOT the generic "re-run on the leader host" (which would still fail: target must BE leader).
    printf '%s' "$_A4B_OUT" | grep -qiE 'must run ON the target broker|Re-run it on brk2' || return 1
    ! printf '%s' "$_A4B_OUT" | grep -qiE 're-run on the leader host'
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
# DOC-23 (R11 P12 FIXED): the pin-mismatch fail-closed error no longer points at the unreachable
# `rotate-tunnel-cert` (which dials an admin socket that is never up in this bricked state — wireClusterEarly
# returns before the socket exists). It now guides a FILE-level restore of the pinned cert/key pair, then a
# restart (tunnelCertPinMismatchError, clusterwrite.go:195-201). Assert broker.err's refusal carries the
# file-restore guidance and NEVER names rotate-tunnel-cert.
_a8_doc23_file_recovery() {
    _dr=$(_berr brk2 60)
    printf '%s' "$_dr" | grep -qiE 'matches neither the pinned' || return 1     # the refusal is present
    printf '%s' "$_dr" | grep -qiE 'FILE-level restore'         || return 1     # points at a file restore
    printf '%s' "$_dr" | grep -qiE 'tunnel-cert\.pem'           || return 1     # names the pinned cert/key
    printf '%s' "$_dr" | grep -qiE 'restart the broker'         || return 1     # then restart
    ! printf '%s' "$_dr" | grep -qiE 'rotate-tunnel-cert'                       # NOT the dead-end command
}
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
# brk1's rendered auth_callout issuer moved OFF the old account key (its reconciler adopted a new seed
# after a restart — the #55 constructible-skew mechanism).
_issuer_changed_brk1() { [ -n "$(_issuer_of brk1)" ] && [ "$(_issuer_of brk1)" != "$ACCT_OLD" ]; }
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
# D2c ATTRIBUTION (R11 drill-side transport fix — NOT an assertion relaxation): the refused NON-INTERACTIVE
# retire's only possible OP side-effect is a NEW removal op (retire/drain) for brk2. brk2's healthy VOTER
# membership row is NOT one: deriveClusterOps ALWAYS lists every roster member and renders a converged VOTER
# as kind=add/state=done (clusterops.go:136-137). The old `ops ls | grep brk2` matched that healthy row and
# false-failed the moment R11 fixed the admin-socket output pollution that used to leave `ops ls` empty here
# — turning brk2's mere MEMBERSHIP (and, when it fires, the intermittent #31 grow-lock residue the D-spine
# already product_reds, owner R14) into a spurious ASSERT-FAIL misattributed to R11. Read the captured
# roster (a global, so this $()-subshell predicate can see it — R-CTX) and assert ONLY what the refusal
# itself could have caused: no removal op opened for brk2, and no credrot alert raised.
_d2_no_side_effects() {
    printf '%s' "$_D2C_OPS" | jq -e '.ops[]?|select(.node_id=="brk2")|select(.kind=="retire" or .kind=="drain")' >/dev/null 2>&1 && return 1
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

# A4 — #56 (R11 P11 FIXED). This used to be a documented UX LOOP: the follower said "re-run on the leader
# host: brk1", the leader (for that same target) said "transfer leadership to brk2 first" — back and forth
# forever, the real requirement (the target must BE the leader) never stated in one place. rotate-tunnel-cert
# is now a SELF-ONLY verb that bypasses the generic leader-redirect and gives DISTINCT, executable guidance.
_A4A_OUT=$(_bt brk2 -- tether cluster rotate-tunnel-cert brk2 --cert-fp "sha256:$FP_OLD" 2>&1) || true
assert_ok "A4a #56 FIXED: on the FOLLOWER, rotating SELF gives self-only guidance ('this IS the target broker, but it is a FOLLOWER — transfer leadership to it, then re-run here'), and NEVER the OLD wrong 'run on the leader host' (which sent the operator to the leader to rotate a follower — still fails, because the TARGET must BE the leader)" \
    _a4a_selfonly_guidance
_A4B_OUT=$(_bt brk1 -- tether cluster rotate-tunnel-cert brk2 --cert-fp "sha256:$FP_OLD" 2>&1) || true
assert_ok "A4b #56 FIXED: on the LEADER for a REMOTE target, the guidance points to running it ON the target ('must run ON the target broker brk2 … Re-run it on brk2'), NOT a circular 'transfer leadership' loop and NOT 'run on the leader host'" \
    _a4b_wronghost_guidance
# NON-VACUITY control: a GENERIC mutating verb on the follower STILL redirects to the leader host — so A4a/A4b
# assert a rotate-tunnel-cert-SPECIFIC self-only bypass, not a blanket removal of the leader-redirect
# (mirrors the product's TestGenericMutatingVerbStillRedirectsOnFollower). drain on a follower fails-fast at
# the leader gate BEFORE any drain logic runs, so it has zero side effects.
assert_refuses "A4c control: a GENERIC mutating verb (cluster drain) on the follower STILL says 're-run on the leader host' — proving the self-only bypass A4a/A4b assert is rotate-tunnel-cert-specific, not a global redirect removal" \
    "re-run on the leader host" \
    _bt brk2 -- tether cluster drain brk1

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
#   (3) an agent-slog reconnect corroboration — INDEPENDENT, logged but NOT gating (its exact line is
#       agent-reconnect-path dependent, and with NATS left up it is usually unconfirmed; the data-plane edge carries it).
# h1 F3: agent slog moved journald -> agent.log; the cursor is a byte offset.
_A7_TS=$(sim_agent_slog_cursor agt1)
_a7_dp_down()     { ! dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"; }
_a7_reconnected() { sim_agent_slog_grep agt1 're-registered after reconnect|agent: registered|rebuilding session' "$_A7_TS"; }
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
    # #63 REFUTED (R6 3/3) + SOURCED (R11): the re-pin carrier is the FULL register reply — homeForRegister
    # reads the roster's cert pins LIVE, so the next full register after any NATS reconnect/boot carries the
    # rotated pin (TestRotatedCertPinRidesTheRegisterReply). So re-pin is a KEPT invariant here: after a
    # PROVEN down-edge (old session died past the keepalive), the FRESH redial MUST re-serve the sentinel
    # against the ROTATED cert. A failure is now a REGRESSION of that invariant (ASSERT-FAIL), not a known
    # defect — #63 is closed, so it is no longer a product_red candidate.
    assert_ok "A7d GENUINE re-pin (KEPT invariant, #63 REFUTED): after the OLD session provably DIED (down-edge past the 30s keepalive), a FRESH yamux redial re-serves the exact sentinel against brk1's ROTATED cert (up-edge) — NOT surviving-session traffic. A failure here is a re-pin REGRESSION, not a known defect" \
        poll_until 100 3 "the data plane serves the exact sentinel again — after a PROVEN down-edge this can only be a fresh redial that re-pinned to the ROTATED cert" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
    # journal corroboration — INDEPENDENT, logged but NOT gating (its exact line is reconnect-path dependent).
    if _a7_reconnected; then ok "A7d SLOG-CONFIRMED: agt1 re-registered/rebuilt in the same window"; else log "A7d slog-unconfirmed (the down->up data-plane edge is the load-bearing proof)"; fi
else
    assert_ok "A7c heal the partition (down-edge not observed)" fault_partition_off brk1
    not_covered "52 A7 re-pin (no session drop)" "the data plane never went down within 80s of blackholing brk1's tunnel port — the established TLS/yamux session outlasted the window, so a genuine redial could not be forced in-sim; re-pin is not judged rather than falsely claimed from surviving-session traffic (this is exactly the round-1 false-green the external review flagged)" runtime-guard
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
assert_ok "A8d the broker slog carries the EXACT pin-mismatch refusal (not merely 'the unit failed' — that would eat any crash as green)" \
    _a8_pin_mismatch_logged
# DOC-23 (R11 P12 FIXED): the OLD pin-mismatch error told the operator to re-run
# `tether cluster rotate-tunnel-cert` — UNREACHABLE in this state (wireClusterEarly returns before the
# admin socket is created, so the command can never connect). The text now points at the ONLY real way
# out: a FILE-level restore of the pinned tunnel-cert.pem/tunnel-key.pem, then a restart.
assert_ok "A8e DOC-23 FIXED: the pin-mismatch refusal in the broker slog now guides FILE-level recovery ('FILE-level restore', 'put the PREVIOUS tunnel-cert.pem + tunnel-key.pem back', 'restart the broker') and NEVER points at the unreachable 'rotate-tunnel-cert' command it used to dead-end the operator with" \
    _a8_doc23_file_recovery
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

# B2 — #54 facet 1 (R11 P6 FIXED). This USED to be an INVERSION (the command SUCCEEDING while nothing
# changed WAS the defect): `reconcile nats --all --wait` printed a false all-clear ("converged", exit 0)
# while the swapped-but-not-re-rendered account.nk still authorized against the OLD issuer. It now
# FAILS-CLOSED on that auth_callout issuer skew (clusterAuthIssuerSkewError, cluster_reconcile.go:92).
assert_ok "B2a mint generation-2 trust material (new account.nk + new CA), leaving _shared/gen1 untouched" \
    secrets_mint_gen "$INST" 2
assert_ok "B2b swap ONLY the account.nk (the auth_callout issuer's source) on BOTH brokers — NOT the route leaf or CA (that would break the route mesh and mask the skew behind an UNREACHABLE; the account.nk skew alone constructs the issuer skew while the route mesh stays up)" \
    _b2_push_gen2
# The skew is now on-disk gen-2 vs rendered gen-1 issuer on BOTH brokers (neither restarted). `reconcile
# nats --all --wait` is the command the rotation runbook §2.1 VERIFIES with — it must now REFUSE, not lie.
assert_refuses "B2 #54 facet 1 FIXED: with account.nk swapped-but-not-re-rendered, 'reconcile nats --all --wait' now FAILS-CLOSED naming the auth_callout issuer skew (was a FALSE all-clear: 'converged' + exit 0). The runbook's VERIFY command no longer certifies an un-adopted issuer as converged" \
    "auth_callout identity skew|DIFFERENT auth_callout issuer|NOT converged" \
    _bt brk1 -- tether cluster reconcile nats --all --wait
# Strengthening: the fail-closed reconcile is a pure VERIFY step (#54 runbook §2.1 correction: it does NOT
# re-render — RESTART re-renders). So both confs are byte-unchanged and the issuer is still OLD.
assert_ok "B2c the fail-closed reconcile did NOT mutate the conf (VERIFY-only, not a fixer): both brokers' nats.conf md5 are unchanged and brk1's issuer is still OLD" \
    sh -c "[ \"$MD5_1_0\" = \"$(_conf_md5 brk1)\" ] && [ \"$MD5_2_0\" = \"$(_conf_md5 brk2)\" ] && [ \"$ACCT_OLD\" = \"$(_issuer_of brk1)\" ]"

# B3 — #54 facet 2 (R11 P6 FIXED): `cluster doctor --offline` USED to be BLIND to the skew (0 fatal, no
# hint). It now cross-checks the on-disk seeds against the rendered conf (clusterAuthIssuerSkewChecks) and
# FATALs auth-issuer-skew, exiting non-zero.
assert_ok "B3 #54 facet 2 FIXED: 'cluster doctor --offline --json' on the swapped-but-not-re-rendered brk1 now FATALs auth-issuer-skew and EXITS non-zero (was blind: 0 fatal). The JSON carries a FATAL auth-issuer-skew check and summary.fatal>=1 — a RUNNING cluster finally has a verb that surfaces the skew" \
    _doctor_issuer_skew_fatal brk1

# ═══ ARM GROUP B-55 — the PARTIAL-rotation skew is CONSTRUCTIBLE and now VISIBLE (#55 CLOSED, R11) ═════
# State on entry: gen-2 account.nk on BOTH brokers' disk, BOTH nats.conf still render the OLD issuer,
# neither restarted (B2/B3 proved that SYMMETRIC not-yet-adopted skew is now visible). R6 REFUTED #55's
# "structurally not constructible" premise: restart ONE broker carrying the new seed and its in-broker
# reconciler CONTENT-COMPARES the desired conf (rendered from the PROCESS-STARTUP gen-2 seed —
# topology_reconcile.go:212-236 → natsreconcile.ReconcileOnce, NOT generation-gated) against the OLD-issuer
# conf and swaps it within ~20s. That leaves the cluster DIVERGENT (brk1 gen-2, brk2 gen-1) — the exact #55
# auth-rejection window (auth_callout is a cross-broker queue group, so ~1/N of dials hit the wrong issuer).
# R11 makes the un-rolled broker's skew VISIBLE where it used to be silent. Restart ONLY brk1 and assert
# BOTH edges (the second is the non-vacuity control):
assert_setup "55-restart: restart ONLY brk1's broker so its reconciler adopts the gen-2 issuer (brk2 left un-rolled — this IS the #55 partial-rotation window)" \
    dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl restart tether-broker'
assert_ok "55a brk1 CONVERGES: its in-broker reconciler re-renders nats.conf to the gen-2 issuer within ~20s (proves R6's constructible-skew mechanism — a plain restart with a new seed adopts it, no membership change needed)" \
    poll_until 45 3 "brk1's nats.conf issuer moved OFF the OLD account key" -- _issuer_changed_brk1
assert_ok "55b NON-VACUITY control: with brk1 converged (on-disk gen-2 == rendered gen-2), doctor --offline on brk1 no longer flags auth-issuer-skew — the skew FATAL is NOT a blanket always-on, it clears on the broker that actually adopted the new issuer" \
    _doctor_no_issuer_skew brk1
assert_ok "55c #55 CONSTRUCTED + VISIBLE: the UN-rolled brk2 (on-disk gen-2 vs its OLD-issuer conf) is now caught — doctor --offline on brk2 FATALs auth-issuer-skew and exits non-zero. Before R11 this partial-rotation divergence was SILENT (the 'not constructible' premise #55 was parked on); the danger an operator would otherwise miss is now detectable" \
    _doctor_issuer_skew_fatal brk2

# ── REBUILD a clean cluster for the INDEPENDENT D-group (C7 guided rotation) ─────────────────────────
# D (retire --compromised --require-credential-rotation) does not depend on the account.nk rotation, so
# restore the gen-1 trust material and a healthy N=2 before running it.
for _n in brk1 brk2; do
    d cp "$(_secrets_shared_dir "$INST")/account.nk" "$(ctr_name "$_n")":/etc/tether/secrets/account.nk >/dev/null 2>&1 || true
    dexec "$_n" -- sh -c 'chown tether:tether /etc/tether/secrets/account.nk && chmod 600 /etc/tether/secrets/account.nk && systemctl reset-failed tether-broker nats-server 2>/dev/null; systemctl restart nats-server tether-broker' >/dev/null 2>&1 || true
done
# Health check must exercise AUTH, not just JS meta (JS forms even when auth_callout is broken, which
# would false-pass this gate and run the D-group against an auth-broken cluster). A real ctl operation
# (`node ls`, session-scoped) round-trips through auth_callout, so it fails iff auth is genuinely working
# again — ctl truly LOGS IN over NATS, no bypassable intermediate state. It ALSO must confirm brk1's LOCAL
# admin socket answers: `retire --require-credential-rotation` opens with a callAdmin(OpClusterStatus)
# pre-check (cluster_retire.go:39) BEFORE its confirm prompt, and that socket comes up LATE in broker
# startup (after the rebuild restart) — so a NATS-auth-only gate would run the retire before the socket
# exists and it would exit rc=69 "child exited before confirm".
#
# Q1 FIX (R6): the socket-liveness item USED to be `cluster status` gated on ITS EXIT CODE — but that
# command os.Exit()s the HEALTH code (0..3), and on ANY N=2 cluster it exits 1 (DEGRADED-WRITABLE / NOT-HA:
# ProjectQuorum(2,false).FaultTolerance==0 ⟺ v<=2, clusterstatus.go:94-95). This drill is ALWAYS N=2, so
# that item was STRUCTURALLY UNSATISFIABLE and starved the whole healthy tail regardless of auth — a drill
# bug that conflated FAULT TOLERANCE (a valid N=2 verdict) with LIVENESS/AUTH. The socket-liveness proof
# does NOT require HA: PARSE `cluster status --json` (parseable even at exit 1) for facts that ARE
# satisfiable at N=2 — the socket answered, a leader is elected, and both voters are present.
_d_brk1_socket_n2() {
    _dbs=$(_bt brk1 -- tether cluster status --json 2>/dev/null)               # parseable JSON even at exit 1 (NOT-HA)
    _dbs_leader=$(printf '%s' "$_dbs" | jq -r '.leader_id // ""' 2>/dev/null)  # leader_id is a string; // "" only guards null
    [ -n "$_dbs_leader" ] || return 1                                          # a leader IS elected (the socket answered with a real view)
    _dbs_v=$(printf '%s' "$_dbs" | jq -r '[.nodes[]?|select(.role=="leader" or .role=="voter")]|length' 2>/dev/null)
    [ "${_dbs_v:-0}" = 2 ]                                                     # both voters present — satisfiable at N=2 (an HA verdict is NOT)
}
_d_cluster_authworks() {
    "$SIM" ctl -- node ls --json >/dev/null 2>&1 && _js_size_is_2 && _d_brk1_socket_n2
}
if ! poll_until 120 3 "N=2 with WORKING AUTH again for the D-group" -- _d_cluster_authworks; then
    not_covered "52 D-group (C7 guided rotation)" "could not rebuild a cluster with working auth after the B-group account.nk rotation broke it; the C7 retire arm round-trips through auth_callout and needs a live authenticating cluster. The account.nk rotation left it unrecoverable in-sim (a further consequence of #54: no atomic rotation, no in-place recovery). C7's guide/alert lifecycle is hermetically covered (cluster_rotation_test.go)" gap
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
# Capture the post-refusal op roster ONCE (a global the predicate reads — R-CTX) and log it so the
# "zero side effects" verdict is auditable: a converged VOTER legitimately renders kind=add/state=done —
# that is membership, not a retire side effect (and any leaked #31 grow-lock is owned by R14, exposed by
# the D-spine's product_red below, never a D2 side effect).
_D2C_OPS=$(_bt brk1 -- tether cluster ops ls --json 2>/dev/null)
log "D2c ops-ls after the refused retire → $(printf '%s' "$_D2C_OPS" | jq -c '[.ops[]?|{node_id,kind,state}]' 2>/dev/null)"
assert_ok "D2c the refusal had ZERO side effects: no NEW removal (retire/drain) op was opened for brk2 and no credrot alert was raised (brk2's healthy VOTER membership row — kind=add/state=done — is NOT a side effect)" _d2_no_side_effects

# D-spine — single attempt, branch on the real output (#31 is intermittent; never hardcode either outcome).
_D_OUT=$(_pty brk1 brk2 -- tether cluster retire brk2 --compromised --require-credential-rotation 2>&1); _D_RC=$?
# Emit the retire outcome as a CAUSE diagnostic (external review re-review Major 1: band signatures match
# `[simcluster]`/`[warn]` cause lines, not the assertion title). The #69 band keys on the `not leader`
# cause that appears here, so a retire failure for a DIFFERENT reason (a different tail) is NOT #69.
log "52 D-spine: retire --compromised --require-credential-rotation rc=$_D_RC outcome: $(printf '%s' "$_D_OUT" | tail -1)"
if printf '%s' "$_D_OUT" | grep -qiE 'grow of .* is in progress|already in flight'; then
    product_red "#31 the leaked \'cluster add\' grow lock BLOCKS the C7 guided rotation spine — and it does so AFTER the operator has already been made to type the node id (cluster_retire.go:50's callAdmin runs after :47's typed confirm), which is a new facet of #31's blast radius. Captured: $(printf '%s' "$_D_OUT" | tail -1)"
    not_covered "52 D4-D8: the C7 guided-rotation alert lifecycle (guide + severe manual:credrot alert raise -> read from ctl -> clear)" "the retire op could not be created: blocked by the #31 grow-lock leak, exactly as it blocks drills 40/41. Not clearing it by inventing a workaround: G-B proved even the canonical clear does not work (ledger:186-198)" gap
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
    # D8a/D8c TRANSPORT (drill-side, not a product defect): `alert clear`/`alert raise` are OPERATOR-ONLY
    # admin-socket verbs (cmd/tether/alert.go:29-31 — they dial the broker's local admin socket, UNLIKE the
    # member NATS RPCs `alert ls`/`alert ack` used above). ctl has no broker admin socket, so `ctl -- alert
    # clear` dialed /var/run/tether/admin.sock and failed rc=69. The D-spine RAISED this alert over brk1's
    # admin socket (the retire ran via `_pty brk1 …`), so clear it through the SAME channel: ON brk1.
    assert_ok "D8a alert clear removes it (operator-only admin-socket verb — run ON brk1 via the broker exec channel, not ctl)" _bt brk1 -- tether alert clear manual:credrot:brk2
    assert_ok "D8b it is really gone" poll_until 20 2 "the credrot alert is gone" -- _credrot_alert_gone
    assert_ok "D8c clear is idempotent (same broker admin-socket channel as D8a)" _bt brk1 -- tether alert clear manual:credrot:brk2
else
    _as_fail "D-spine UNJUDGEABLE — retire --compromised --require-credential-rotation failed for an unclassified reason (rc=$_D_RC): $(printf '%s' "$_D_OUT" | tail -1)"
fi

# NOT a defect, recorded so nobody files it later: `alert clear` deliberately does not verify the rotation
# actually happened (cluster_rotation.go:13-16, rejection #5 — the product says so about itself).

drill_end
