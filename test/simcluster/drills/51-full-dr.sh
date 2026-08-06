#!/bin/sh
# 51-full-dr — S7 / G-C. N=3 -> TOTAL LOSS -> 1 -> 2: runbook §5.2's full-cluster disaster recovery,
# executed literally, step by step, with every deviation counted.
# Plan: docs/reviews/s7-s9-plan.md §2.2. Expected landing: PRODUCT-RED (#31/#45 on re-grow) or INCOMPLETE
# (#53-scope WONTFIX gap). Runtime ~18min. Topology: 3 brokers + 1 agent + 1 ctl (grow-family).
#
# ── WHAT R10 CHANGED — the DR TAIL now runs end-to-end via product verbs ─────────────────────────────
# #51 (restore could not apply the broker.cluster seam) is FIXED: R10 P2 gave `recovery restore` a
#      `--config` flag that calls applyClusterSeam, writing the FULL FIVE-field seam. The old drill had
#      to HAND-WRITE a 3-field seam (with the INVALID key `nats_route`); that hand-write is DELETED and
#      replaced by `restore --config` (arm F-b7/b8). MEASURED constraint: the seam lands in the
#      root-owned /etc/tether/broker.yaml, so restore must run as ROOT — as `sudo -u tether` it fails
#      CLOSED ("seam could NOT be applied … permission denied"). Running as root then leaves root-owned
#      data the daemon cannot read, so the ONE residual undocumented step is broker-ops #6's
#      `chown -R tether:tether /var/lib/tether` (arm G-own — a DR-STEP gap; §5.2 omits it).
# #52 (restore neither rendered nor MENTIONED nats.conf): R10 P4 makes restore print the exact,
#      argument-substituted `reconcile nats --manual` command (= runbook §5.2 step 3). The drill runs
#      THAT printed command verbatim (arm G-nats), not a drill-authored render.
# #53 was SPLIT (2026-07-19): #53-silence CLOSED (both ends warn — B-vault1c + J1); #53-scope
#      WONTFIX-BY-DESIGN (bundle stays state.db-only). See arm J.
#
# => RUNBOOK §5.2 NOW COMPOSES INTO A LIVE BROKER + a served terminus (arm H2), for the first time.
#    The ONE step it still omits (the #6 chown) is booked as a gap, not papered over.
#
# ── LABEL TAXONOMY (decidable, not a matter of taste) ────────────────────────────────────────────────
#   [env]                       sim supplies the machine (Mandate 3): up/install.sh, enable nats-server,
#                               chown after vault_push, systemctl reset-failed
#   [operator per runbook §x]   the doc's own script says a human does this
#   [by design: rejection #N]   the product explicitly declares it out of scope
#   [GAP #N]                    a step the script does NOT contain and tether cannot do => A DEFECT
# Decision rule: [operator] iff the action has a corresponding sentence in runbook §5.1/§5.2. Otherwise
# [GAP]. The ONE remaining [GAP] is the #6 chown (arm G-own): restore --config MUST run as root to write
# the root-owned seam, leaving root-owned data, and §5.2 never tells the operator to chown it back.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. THE FRESH-BOX GATE IS THE ON/OFF SWITCH FOR THE WHOLE DRILL'S TRUTH. If any of D's four `!`
#     assertions is missing, every later "it recovered" could just mean the box was never broken.
#  2. C-vault-oracle MUST be a sha256 comparison, not `test -e`: a truncated/half-written leftover would
#     let "the bundle survived off-cluster" pass for the wrong reason. (vault_init's rm-then-assert-empty
#     is this guard's twin.)
#  3. E-remint-neg IS WHAT MAKES D LEGITIMATE. Without it, "secrets_distribute is a RESTORE, not a
#     shortcut" is just a claim. It must also assert zero mutation.
#  4. H2 NEVER closes on status/explain; dp_curl_refused uses exit 7, not `! curl -sf`.
#  5. H1 MUST NOT restart or re-provision the agent (#48's standing prohibition), and must prove
#     `! ONLINE` BEFORE starting the broker — the ~5s heartbeat residue would otherwise false-green it.
#  6. The #6-chown gap is booked (arm G-own) BEFORE its clearing step (drill 40's discipline w.r.t. #31),
#     and the seam is asserted FIVE-fields-on-disk (F-b8), never "present"; the invalid `nats_route` key
#     the old hand-write used must be ABSENT. A wrong-reason failure stays an ASSERT-FAIL.
#  7. `--on-broker brk1` + `transfer-leader brk1` are HARD PRECONDITIONS, not tuning: homeForExpose
#     returns nil for a non-tunnel voter (home.go:96-113, #29), so at N=3 the expose only serves when
#     home==brk1. Miss it and A-base's curl fails for an environmental reason = a false RED against DR.
#  8. `pruned 2 stale peers` is CONSTANT (every restore prunes on a freshly copied staging from the same
#     3-node bundle). The discriminator between F-b/F-c/F-d is the `.bak` LADDER, never the prune count.
#  9. Arm I must not let grow_to_3's retry launder #31 evidence.
# 10. D's fresh box only holds if it comes from `$SIM up` (which runs provision-node.sh:39-45, rewriting
#     install.sh's `host: 127.0.0.1` to 0.0.0.0). A hand-built box would false-RED for an env reason.
# 11. THE DR-STEP-LEDGER IS A FIRST-CLASS DELIVERABLE. Wrapping the script in a `dr_restore_all()` that
#     reports GREEN is explicitly forbidden: that is Mandate (4)'s inversion — the operation "succeeding"
#     only because the sim wrote clever bash IS tether's failure being concealed.
# 12. Broker application lines are in the broker's own slog FILE, NOT journalctl (R-BROKERLOG).
#     h1 F3: that file is /var/log/tether/broker.log (broker.yaml `log_file:`); pre-h1 hosts have
#     /var/log/tether/broker.err. sim_broker_slog reads both. journald now holds PANICS, not slog.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"          # sctl / leader_node — agentyaml.sh depends on sctl
. "$HERE/lib/vault.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/lib/assert.sh"

SID=lab
PIN=515151
NURL="nats://brk1:4222"
DR_BUNDLE=/var/lib/tether/dr-51
BK_NAME=dr-51
INST="$INSTANCE"

# DR-STEP-LEDGER counters (Mandate (4), made auditable + quantitative).
DR_DOCUMENTED=6        # runbook §5.2 advertises SIX numbered steps (R10-corrected: 1 install+secrets,
                       # 2 restore --config + verify seam, 3 reconcile nats --manual + restart nats,
                       # 4 start broker + status, 5 re-grow, 6 agents reconnect)
DR_REQUIRED=0          # steps we actually had to perform
DR_UNDOCUMENTED=0      # steps the script does NOT contain  => each one is a gap
DR_GAPS=""
_dr_step()  { DR_REQUIRED=$((DR_REQUIRED+1)); log "DR-STEP $DR_REQUIRED [operator per runbook §5.2]: $1"; }
_dr_env()   { log "DR-STEP [env, Mandate 3 — sim supplies the machine]: $1"; }
_dr_gap()   { DR_REQUIRED=$((DR_REQUIRED+1)); DR_UNDOCUMENTED=$((DR_UNDOCUMENTED+1)); DR_GAPS="$DR_GAPS $2"; warn "DR-STEP $DR_REQUIRED [GAP $2 — NOT in runbook §5.2; tether cannot do this itself]: $1"; }

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# _pty_root: the typed-confirm restore run as ROOT (= the real `sudo tether cluster recovery restore`).
# On a fresh DR box the P2 --config seam MUST be written into the ROOT-owned /etc/tether/broker.yaml, and
# the broker-ops-#6 user (tether) cannot write there (measured: "seam could NOT be applied … permission
# denied", failing CLOSED). So the seam-applying path is the root one — with the #6 chown remedy after.
_pty_root() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
# h1 F3: broker slog moved broker.err -> broker.log; sim_broker_slog reads both.
_berr() { sim_broker_slog "$1" "${2:-80}"; }

# `node ls --json` uses `nid` — NOT `node_id`. (`node_id` is `cluster status --json`'s field; the two
# APIs differ, proto/messages.go:379-385 vs the cluster status report.) Querying the wrong key silently
# matches nothing, so a perfectly ONLINE agent polls to timeout and looks like a product failure.
_apy_online() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }
_apy_offline() { ! _apy_online; }
# LIVENESS, NOT HEALTH. `cluster status` returns a HEALTH exit code by design (0=healthy, 1=DEGRADED,
# 2=QUORUM_LOST, 3=FORCE_SINGLE — clusterstatus.go:66-101), and a restored/DR'd lone voter is DEGRADED
# FOREVER. Using the exit code as a liveness probe polls until timeout against a perfectly healthy broker
# and manufactures a FAKE product failure. Liveness = "it answered with parseable JSON".
_brk1_ready() { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id != null' >/dev/null 2>&1; }
_leader_is_brk1_now() { "$SIM" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }
_two_voters_now() {
    _t=$(sim_leader) || return 1
    [ "$("$SIM" exec "$_t" -- tether cluster status --json 2>/dev/null | jq '[.nodes[]?|select(.phase=="VOTER")]|length' 2>/dev/null)" = 2 ]
}

# R-NOSHC predicates. These MUST be functions, never `sh -c "…"`: a new shell does not inherit harness
# functions, so `sh -c "! node_exists brk1"` would fail with "not found", `!` would invert it, and the
# assertion would be PERMANENTLY TRUE — a false green that no amount of running would ever catch.
_destroy_all_brokers() { rm_node brk1 --vols; rm_node brk2 --vols; rm_node brk3 --vols; return 0; }
_no_broker_containers() { ! node_exists brk1 && ! node_exists brk2 && ! node_exists brk3; }
# Stage a re-minted (FOREIGN) secrets tree on brk1. A function, not `sh -c`: `d` and `ctr_name` are
# harness functions and would be invisible to a new shell.
_stage_remint_secrets() {
    rm -rf /tmp/51-remint || return 1
    cp -r "$HERE/secrets/$INST/brk1-remint" /tmp/51-remint || return 1
    dexec brk1 -- rm -rf /tmp/remint-secrets >/dev/null 2>&1 || true
    d cp /tmp/51-remint "$(ctr_name brk1)":/tmp/remint-secrets >/dev/null 2>&1 || return 1
    dexec brk1 -- sh -c 'chown -R tether:tether /tmp/remint-secrets && chmod 700 /tmp/remint-secrets && chmod 600 /tmp/remint-secrets/*.pem /tmp/remint-secrets/*.nk 2>/dev/null; true' >/dev/null 2>&1
}

_no_broker_volumes() {
    for _v in "$(vol_etc brk1)" "$(vol_lib brk1)" "$(vol_etc brk2)" "$(vol_lib brk2)" "$(vol_etc brk3)" "$(vol_lib brk3)"; do
        d volume inspect "$_v" >/dev/null 2>&1 && return 1
    done
    return 0
}

# _ensure_leader_brk1 : make brk1 the leader, idempotently. grow_to_3 inits brk1 so it is usually
# already the leader; `transfer-leader brk1` then errors "already the leader" (exit 70). Only transfer
# when needed — a no-op must not SETUP-RED.
_ensure_leader_brk1() {
    if _leader_is_brk1_now 2>/dev/null || { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }; then return 0; fi
    _cur=$(sim_leader) || return 1
    "$SIM" exec "$_cur" -- sh -c "runuser -u tether -- tether cluster transfer-leader brk1 --wait"
}
_cleanup() {
    rm -rf /tmp/51-remint 2>/dev/null || true
    true
}

drill_begin "51-full-dr (N=3 -> total loss -> restore -> re-grow: runbook §5.2 executed literally)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

# ── SETUP + A-base — the pre-disaster world ─────────────────────────────────────────────────────────
assert_setup "grow_to_3 (N=3 HA)"                            grow_to_3 1 1
# vault_init AFTER grow: grow_to_3's retry path nukes on a failed attempt, and nuke now reaps the vault
# (simcluster:cmd_nuke). Initialising the vault before the cluster is built would let that nuke delete it
# out from under us. DR-wise this is also correct: the backup vault exists to hold a backup OF a cluster,
# so it comes into being after the cluster does.
assert_setup "vault init (S0-backup-vault: survives rm_node --vols, dies with nuke — created AFTER grow so grow_to_3's retry-nuke cannot reap it)" vault_init
assert_setup "ensure brk1 is the leader (grow inits brk1, so this is usually a no-op — a redundant transfer to self would error 70)" _ensure_leader_brk1
# HARD PRECONDITION, not tuning: brk1 must be leader AND agt1's tunnel broker AND the expose home, or
# #29 (home.go:96-113) makes the expose unserviceable and A-base fails for an environmental reason.
assert_setup "leader is brk1 (required: online backup is leader-gated AND #29 means the expose only serves when home==brk1)" _leader_is_brk1_now
assert_setup "session $SID + ctl login"                      "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                               "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (tunnel_addr -> brk1, S0-tunnel)" agent_provision_yaml agt1 "$SID" "$NURL" open

TOK=$(expose_serve_sentinel agt1 8091) || setup_fail "could not start the sentinel http.server on agt1"
assert_setup "expose agt1:8091 --on-broker brk1 --name dr" "$SIM" ctl -- expose agt1 --local 8091 --on-broker brk1 --name dr
P=$("$SIM" ctl -- expose explain dr --json 2>/dev/null | jq -r '.public_port // empty')
[ -n "$P" ] || setup_fail "could not read the public port of expose 'dr'"
E0=$("$SIM" ctl -- expose explain dr --json 2>/dev/null | jq -r '.epoch // 0')

assert_ok "A-base1 three VOTER before the disaster" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '[.nodes[]?|select(.phase==\"VOTER\")]|length==3' >/dev/null"
assert_ok "A-base2 session $SID exists" \
    sh -c "\"$SIM\" ctl -- session ls --json 2>/dev/null | jq -e '[.sessions[]?|select(.name==\"$SID\")]|length==1' >/dev/null"
assert_ok "A-base3 PRE-DISASTER DATA PLANE: ctl1 curls brk1:$P and gets the exact sentinel (real tunneled bytes)" \
    poll_until 30 2 "sentinel through the tunnel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"
assert_ok "A-base4 seed an exec row into history" "$SIM" ctl -- exec agt1 -- echo DR-HISTORY-SENTINEL
SHA_TUNCERT=$(sha256sum "$HERE/secrets/$INST/brk1/tunnel-cert.pem" 2>/dev/null | awk '{print $1}')
SHA_TUNKEY=$(sha256sum "$HERE/secrets/$INST/brk1/tunnel-key.pem" 2>/dev/null | awk '{print $1}')
[ -n "$SHA_TUNCERT" ] && [ -n "$SHA_TUNKEY" ] || setup_fail "could not fingerprint brk1's tunnel material in the operator key vault"

# ── B-vault — take the bundle and carry it off the cluster ──────────────────────────────────────────
# --out lands in /var/lib/tether (tether-owned 0750, install.sh:491), NOT /var/backups: drill 50 arm C
# already proved that wall exists (DOC-27).
_dr_step "take an online backup on the leader"
# Capture ONCE (a mutating command must never be re-run per signature). The scope warning goes to stderr
# (cmd.ErrOrStderr), so 2>&1 carries both the completion line and the #53-silence warning.
_BK_CAP=$(_bt brk1 -- tether cluster backup --out "$DR_BUNDLE" 2>&1); _BK_RC=$?
_bk_rc_ok()  { [ "$_BK_RC" = 0 ]; }
_bk_leader() { printf '%s' "$_BK_CAP" | grep -qE 'online backup complete: .*self=brk1, source=leader'; }
# #53-silence (R10, CLOSED): the BACKUP end must warn UNMISSABLY that the bundle is state.db-only. Three
# clauses — a bare "BUNDLE SCOPE" header with no content and no remedy would satisfy one grep and still be
# the exact silence #53 was about, so it must NAME what is missing (JetStream) AND give the runnable
# alternative (`nats stream backup`).
_bk_scope_warned() {
    printf '%s' "$_BK_CAP" | grep -qF 'BUNDLE SCOPE' &&
    printf '%s' "$_BK_CAP" | grep -qF 'JetStream is NOT in it' &&
    printf '%s' "$_BK_CAP" | grep -qF 'nats stream backup'
}
assert_ok "B-vault1 online backup on the LEADER" _bk_rc_ok
assert_ok "B-vault1b it self-reports as a LEADER-sourced online bundle" _bk_leader
assert_ok "B-vault1c (#53-silence) the BACKUP end warns the bundle is state.db-only: names 'BUNDLE SCOPE', says JetStream is NOT in it, and prints the runnable \`nats stream backup\` alternative — the belief 'I have a backup' can no longer form silently over a control-plane-only bundle" \
    _bk_scope_warned
_dr_env "carry the bundle off the machine (an operator would scp it; S0-backup-vault)"
assert_ok "B-vault2 pull the bundle into the off-cluster vault" vault_pull brk1 "$DR_BUNDLE" "$BK_NAME"
assert_ok "B-vault3 the bundle is a leader snapshot of the full 3-node roster" \
    sh -c "jq -e '.self_id==\"brk1\" and .source_role==\"leader\" and (.roster|length)==3' '$(vault_manifest $BK_NAME)' >/dev/null"
assert_ok "B-vault4 the bundle carries NO key material" \
    sh -c "! grep -aqE 'PRIVATE KEY|BEGIN OPENSSH|^S[UAO][A-Z2-7]{20,}' '$(vault_dir)/$BK_NAME/manifest.json' '$(vault_dir)/$BK_NAME/state.db'"
SHA_STATE=$(vault_sha "$BK_NAME" state.db)
SHA_MAN=$(vault_sha "$BK_NAME" manifest.json)
[ -n "$SHA_STATE" ] && [ -n "$SHA_MAN" ] || setup_fail "could not sha256 the vaulted bundle"

# ── C-disaster — TOTAL LOSS: all three brokers and all six volumes ──────────────────────────────────
# Five elements: (1) baseline = A-base's real curl + roster; (2) observation = docker + host sha256;
# (3) boundary = every broker container AND its volumes, agent untouched (step 4 needs it alive);
# (4) semantic oracle = the data plane is really dead + the vault is really still readable;
# (5) cleanup = the single EXIT trap (the containers are gone by design, nuke reaps the rest).
assert_ok "C-disaster1 DESTROY all three brokers and their volumes (total cluster loss)" \
    _destroy_all_brokers
assert_ok "C-disaster2 no broker container survives" _no_broker_containers
assert_ok "C-disaster3 all six named volumes are gone (the disaster is real, not just stopped containers)" _no_broker_volumes
assert_ok "C-disaster4 agt1's container is STILL RUNNING (runbook §5.2 step 4 is about it reconnecting)" \
    node_running agt1
# After the broker containers are DELETED, docker DNS cannot resolve brk1, so curl exits 6 (couldn't
# resolve) rather than 7 (connection refused). Either is "the data plane is gone" — the point is it does
# NOT return the sentinel. Assert the negative of the positive oracle.
_c_dataplane_dead() { ! dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"; }
assert_ok "C-disaster5 the data plane is really dead after total loss (no sentinel — brk1 unresolvable/refused)" \
    poll_until 30 2 "public port dead after total loss" -- _c_dataplane_dead
# THE REASON S0-backup-vault EXISTS, in one assertion. sha256, not `test -e`: a truncated leftover would
# make "it survived" pass for the wrong reason.
assert_ok "C-vault-oracle THE BUNDLE SURVIVED OFF-CLUSTER: byte-identical sha256 after the disaster that destroyed every volume" \
    sh -c "[ \"\$(sha256sum '$(vault_dir)/$BK_NAME/state.db' | awk '{print \$1}')\" = \"$SHA_STATE\" ] && [ \"\$(sha256sum '$(vault_dir)/$BK_NAME/manifest.json' | awk '{print \$1}')\" = \"$SHA_MAN\" ]"
assert_ok "C-vault-oracle2 and it still parses as a 3-node leader bundle" \
    sh -c "jq -e '(.roster|length)==3 and .self_id==\"brk1\"' '$(vault_manifest $BK_NAME)' >/dev/null"

# ── D-freshbox — a genuinely NEW machine [operator per runbook §5.2 step 1] ─────────────────────────
_dr_step "provision a fresh box and restore the node's ORIGINAL secrets from the operator key vault"
_dr_env "install tether on the fresh box (install.sh via \`simcluster up\`)"
assert_ok "D-freshbox1 bring up a fresh brk1 (real install.sh path)" "$SIM" up --brokers 1 --agents 1 --ctl 1
# THE FRESH GATE — four assertions. Miss any one and every later "recovered" claim is unfalsifiable.
assert_ok "D-fresh-gate1 the new box has NO tether.db"        sh -c "! \"$SIM\" exec brk1 -- test -e /var/lib/tether/tether.db"
assert_ok "D-fresh-gate2 the new box has NO raft state"       sh -c "! \"$SIM\" exec brk1 -- test -e /var/lib/tether/raft"
assert_ok "D-fresh-gate3 broker.yaml has NO cluster seam (install.sh:548-556 comments it out)" \
    sh -c "! \"$SIM\" exec brk1 -- grep -qE '^[[:space:]]+raft_addr:' /etc/tether/broker.yaml"
assert_ok "D-fresh-gate4 nats.conf has NO auth_callout (install.sh:690-704's stock standalone conf)" \
    sh -c "! \"$SIM\" exec brk1 -- grep -q auth_callout /etc/tether/nats.d/nats.conf"
# Restoring the node's ORIGINAL tunnel material from the operator's vault is the runbook's own step 1 —
# and it is REQUIRED, because that fingerprint is restore's un-forgeable provenance anchor
# (restore.go:237-245). Freshly minting it would be a NEW identity, which the product would correctly
# refuse; arm E proves exactly that.
assert_ok "D-freshbox2 restore brk1's ORIGINAL secrets from the operator key vault [operator per runbook §5.2 step 1]" \
    secrets_distribute "$INST" brk1
assert_ok "D-freshbox3 the restored tunnel CERT is byte-identical to the pre-disaster one" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sha256sum /etc/tether/secrets/tunnel-cert.pem 2>/dev/null | awk '{print \$1}')\" = \"$SHA_TUNCERT\" ]"
assert_ok "D-freshbox4 the restored tunnel KEY is byte-identical too (init.go:312-336 reads both)" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sha256sum /etc/tether/secrets/tunnel-key.pem 2>/dev/null | awk '{print \$1}')\" = \"$SHA_TUNKEY\" ]"
_dr_env "start nats-server on the fresh box"
assert_ok "D-freshbox5 enable nats-server on the fresh box" dexec brk1 -- systemctl enable --now nats-server
assert_ok "D-freshbox6 push the vaulted bundle back onto the box [operator per runbook §5.2 step 2]" \
    vault_push "$BK_NAME" brk1 "$DR_BUNDLE"

# ── E-remint-neg — the proof that D-freshbox2 was a RESTORE and not a shortcut ──────────────────────
# Mint a brand-new identity inside THIS instance's namespace (so `nuke` reaps it — never point
# SECRETS_STASH at a scratch dir with a different instance name: nuke would never find it and every run
# would leak a key tree onto the host).
assert_ok "E-remint-prep mint a NEW node identity (new tunnel cert = new fingerprint)" \
    secrets_mint_node "$INST" brk1-remint
assert_ok "E-remint-prep2 stage the foreign secrets on the box" _stage_remint_secrets
assert_refuses "E-remint-neg RE-MINTED secrets are REFUSED: the tunnel-cert fingerprint is restore's un-forgeable provenance anchor (so D-freshbox2 restoring the ORIGINAL material is a RESTORE, not a shortcut)" \
    "tunnel-cert fingerprint mismatch|not this bundle's node|refusing to adopt a foreign cluster" \
    _pty brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /tmp/remint-secrets
assert_ok "E-remint-neg2 the refusal wrote nothing (the gate is before any disk write, restore.go:104-107 << :112)" \
    sh -c "! \"$SIM\" exec brk1 -- test -e /var/lib/tether/tether.db"

# ── F — the DR restore itself ───────────────────────────────────────────────────────────────────────
assert_refuses "F-a a malformed --raft-addr is rejected (needs the pty: the confirm runs BEFORE validation)" \
    "must be host:port|--raft-addr" \
    _pty brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --raft-addr notahostport

# ── F-b — THE DR RESTORE (R10 P2: restore --config applies the broker.cluster seam) ─────────────────
# Run as ROOT (= `sudo tether cluster recovery restore`, runbook §5.2 step 2's own invocation): the seam
# lands in the ROOT-owned /etc/tether/broker.yaml, which the broker-ops-#6 user CANNOT write. Measured on
# the real stack: as `sudo -u tether` this same command RESTORES the DB but then FAILS CLOSED —
#   "restore: the DB was restored but the broker.cluster seam could NOT be applied … permission denied"
# — so the seam-applying path is the root one. --config + --nats-conf are the runbook's own step-2 flags.
_dr_step "restore the bundle onto the fresh box (as root: --config writes the seam into root-owned broker.yaml)"
_F_CAP=$(_pty_root brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --config /etc/tether/broker.yaml --nats-conf /etc/tether/nats.d/nats.conf --raft-addr brk1:7400 2>&1); _F_RC=$?
# Predicates are FUNCTIONS: `sh -c` would not inherit these unexported captures (R-NOSHC family).
_f_rc_ok()     { [ "$_F_RC" = 0 ]; }
_f_pruned()    { printf '%s' "$_F_CAP" | grep -qE 'restore complete: node brk1 is now a single-voter cluster \(pruned 2 stale peers; bundle applied_index [0-9]+ reset to 0\)'; }
_f_freshbox()  { printf '%s' "$_F_CAP" | grep -qE 'prior DB preserved at: \(none'; }
# R10 P2 — the seam was APPLIED (not hand-written). rc=0 already implies it: restore FAILS CLOSED when the
# seam cannot be written, so a zero exit means the write succeeded. The stdout line is the human evidence.
_f_seam_applied() { printf '%s' "$_F_CAP" | grep -qF 'broker.cluster seam applied to /etc/tether/broker.yaml'; }
_f_next_and_seamok() {
    printf '%s' "$_F_CAP" | grep -qF 'NEXT (run in order):' &&
    printf '%s' "$_F_CAP" | grep -qF 'broker.cluster seam is in /etc/tether/broker.yaml'
}
# The seam's FIVE fields, read off disk (not believed from stdout). This is the H2-flip's core assertion:
# the old drill HAND-WROTE 3 fields (data_dir, raft_addr, and the INVALID key `nats_route`) — restore
# --config now writes the correct FIVE. `serve` keys cluster mode on data_dir alone, so a partial seam
# boots SINGLE mode and lands on the very boot FATAL #51 was; that is why "all five" is the assertion.
_f_seam_five() {
    _sf=$("$SIM" exec brk1 -- sed -n '/^  cluster:/,/^  [a-z_]*:/p' /etc/tether/broker.yaml 2>/dev/null)
    for _k in data_dir raft_addr secrets_dir nats_conf_path nats_server_bin; do
        printf '%s' "$_sf" | grep -qE "^[[:space:]]+$_k:[[:space:]]*[^[:space:]#]" || return 1
    done
    # and the invalid `nats_route` key the old hand-write used must NOT be there.
    ! printf '%s' "$_sf" | grep -qE '^[[:space:]]+nats_route:'
}
assert_ok "F-b1 the DR restore succeeds [operator per runbook §5.2 step 2] — rc=0 means the seam WAS written (restore fails CLOSED if it cannot)" _f_rc_ok
assert_ok "F-b2 it pruned BOTH dead peers and reset the cursor" _f_pruned
# The fresh-box-exclusive discriminator: on a box that never had a DB there is nothing to preserve.
assert_ok "F-b3 'prior DB preserved at: (none — no prior DB on this host)' — the fresh-box-exclusive discriminator" _f_freshbox
assert_ok "F-b4 the roster is now EXACTLY {brk1} (#49-hardening: a resurrected pruned peer would be a restore-side defect)" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db 'select group_concat(node_id) from cluster_nodes' 2>/dev/null | tr -d '\r')\" = brk1 ]"
assert_ok "F-b5 the applied cursor was reset to 0" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db \"select value from cluster_meta where key='applied_index'\" 2>/dev/null | tr -d '\r')\" = 0 ]"
assert_ok "F-b6 no restore_in_progress / force_single_active markers linger" \
    sh -c "[ -z \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db \"select key from cluster_meta where key in ('restore_in_progress','force_single_active')\" 2>/dev/null | tr -d '\r')\" ]"
# ── H2-FLIP CORE — #51 is FIXED: restore --config applied the seam, no hand-write, all FIVE fields ────
assert_ok "F-b7 (#51 FIXED) restore --config APPLIED the broker.cluster seam itself — the old drill had to HAND-WRITE it because restore had no --config flag at all (cluster_backup.go pre-R10)" _f_seam_applied
assert_ok "F-b8 (H2-flip) the seam on disk carries ALL FIVE fields (data_dir/raft_addr/secrets_dir/nats_conf_path/nats_server_bin) and NOT the old hand-write's invalid \`nats_route\` key — serve keys cluster mode on data_dir, so a partial seam re-lands the #51 boot FATAL" _f_seam_five
assert_ok "F-b9 (R10 P4) restore printed the ORDERED next-step list with a ✓ seam confirmation, not the single 'NEXT: start tether-broker' line #52/#64 were made of" \
    _f_next_and_seamok

# ── F-c / F-d — restore ADDRESS OVERRIDE + the non-overwriting .pre-restore.N backup ladder (plan §2.2) ─
# EXT-REVIEW-B8: these were promised by the plan but omitted; 50 dropped its own backup-ladder coverage on
# the claim that 51 owned it, so their absence was a real hole. A SECOND restore on a box that NOW has a DB
# (from F-b) must honour a DIFFERENT --raft-addr (re-stamped into cluster_nodes.raft_addr, restore.go:308)
# and preserve the prior DB to tether.db.pre-restore.bak (backupToUnique, init.go:342-354). A THIRD restore
# must ladder the backup to .pre-restore.1.bak (O_EXCL, never clobbers, init.go:354). String/paths are the
# product's real output (cluster_backup.go:115-119), not guessed. Prune is CONSTANT 2 (each restore re-stages
# from the 3-node bundle → self), so the .bak ladder — not the prune count — is the F-b/F-c/F-d discriminator.
#
# --config "" (explicit opt-out) on BOTH re-runs: F-c overrides --raft-addr to 127.0.0.1:7400, which would
# MISMATCH the seam F-b just wrote (brk1:7400) and make applyClusterSeam HARD-error on a stale/incomplete
# seam. The seam is validated ONCE (F-b7/b8); these re-runs test the .bak ladder + address override only,
# and F-d restores the DB back to raft_addr=brk1:7400 so it ends matching the seam on disk. Run as root
# to match F-b (the box's data + DB are now root-owned until the #6 chown below).
_FC_CAP=$(_pty_root brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --config "" --raft-addr 127.0.0.1:7400 2>&1); _FC_RC=$?
_fc_rc_ok()      { [ "$_FC_RC" = 0 ]; }
# grep -qF (fixed substring), NOT a `$`-anchored regex: the _pty capture ends each line with a CR (\r),
# which an `…\.bak$` anchor never matches. `tether.db.pre-restore.bak` is not a substring of
# `…pre-restore.1.bak`, so this stays unambiguous vs the F-d ladder step.
_fc_preserved()  { printf '%s' "$_FC_CAP" | grep -qF 'prior DB preserved at: ' && printf '%s' "$_FC_CAP" | grep -qF 'tether.db.pre-restore.bak'; }
_fc_bak_exists() { "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.bak; }
_fc_addr()       { [ "$("$SIM" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db "select raft_addr from cluster_nodes where node_id='brk1'" 2>/dev/null | tr -d '\r')" = "127.0.0.1:7400" ]; }
assert_ok "F-c1 a SECOND restore with --raft-addr 127.0.0.1:7400 succeeds (address override on a box that now has a DB; --config \"\" opts out of a seam re-write that would mismatch F-b's)" _fc_rc_ok
assert_ok "F-c2 it preserved the prior DB to tether.db.pre-restore.bak (unlike F-b's fresh box, there now IS a prior DB — 'preserved at' is honest)" _fc_preserved
assert_ok "F-c3 the .pre-restore.bak file really exists on disk" _fc_bak_exists
assert_ok "F-c4 the self node's raft_addr was really re-stamped to 127.0.0.1:7400 (restore.go:308 sqlite readback — not just the CLI's word)" _fc_addr
_FD_CAP=$(_pty_root brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --config "" --raft-addr brk1:7400 2>&1); _FD_RC=$?
_fd_rc_ok()        { [ "$_FD_RC" = 0 ]; }
# grep -qF (fixed substring), NOT a `$`-anchored regex — the _pty capture ends lines with CR (\r), which
# breaks a `…\.1\.bak$` anchor (same fix as _fc_preserved).
_fd_ladder()       { printf '%s' "$_FD_CAP" | grep -qF 'prior DB preserved at: ' && printf '%s' "$_FD_CAP" | grep -qF 'tether.db.pre-restore.1.bak'; }
_fd_ladder_both()  { "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.bak && "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.1.bak; }
_fd_addr()         { [ "$("$SIM" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db "select raft_addr from cluster_nodes where node_id='brk1'" 2>/dev/null | tr -d '\r')" = "brk1:7400" ]; }
assert_ok "F-d1 a THIRD restore (back to --raft-addr brk1:7400) succeeds" _fd_rc_ok
assert_ok "F-d2 it LADDERED the prior-DB backup to .pre-restore.1.bak (O_EXCL → never overwrites .pre-restore.bak)" _fd_ladder
assert_ok "F-d3 BOTH .pre-restore.bak AND .pre-restore.1.bak exist — the ladder, not an overwrite" _fd_ladder_both
assert_ok "F-d4 the raft_addr is back to brk1:7400 (the override is not sticky across restores) — leaving the DB in the same effective state F-b produced, matching the seam on disk, so the #51 tail below is unaffected" _fd_addr

# ── G-own — THE RESIDUAL OWNERSHIP GAP (#51 fix meets broker-ops #6) ─────────────────────────────────
# #51 is FIXED: restore --config wrote the FULL seam (F-b7/b8), so there is NO hand-write and NO #51 boot
# FATAL. But the seam had to be written into the ROOT-owned /etc/tether/broker.yaml, so restore ran as
# ROOT (F-b), and a root-run offline op leaves root-owned data files the User=tether daemon cannot read
# (measured: "broker: probe raft state … stat raft.db: permission denied"). The remedy is broker-ops #6's
# `chown -R tether:tether /var/lib/tether` — DOCUMENTED there, but ABSENT from runbook §5.2. So P2's
# seam-in-restore and the #6 tether-ownership rule are in tension on a fresh box, and §5.2 does not
# reconcile them. Pinned as a DR-STEP gap BEFORE the clearing step (drill 40's #31 discipline).
_g_data_root_owned() { [ "$("$SIM" exec brk1 -- stat -c %U /var/lib/tether/tether.db 2>/dev/null | tr -d '\r')" = root ]; }
assert_ok "G-own1 the ROOT restore left root-owned data (the #6 hazard P2 forces: root was REQUIRED to write the root-owned seam, and sudo -u tether fails CLOSED on it — measured)" \
    _g_data_root_owned
_dr_gap "chown -R tether:tether /var/lib/tether — restore --config MUST run as root to write the root-owned broker.yaml seam (P2), which leaves root-owned data the User=tether daemon cannot read; broker-ops #6 documents this chown but runbook §5.2 does not" "#6-chown"
assert_ok "G-own2 [GAP #6-chown] apply the documented broker-ops #6 remedy so the daemon can read its data dir" \
    dexec brk1 -- chown -R tether:tether /var/lib/tether

# ── G-nats — runbook §5.2 step 3, EXECUTED FROM THE COMMAND restore ITSELF PRINTED (#52 addressed) ──
# #52 was "restore neither renders nor MENTIONS nats.conf". R10 P4 makes restore print the exact,
# argument-substituted `reconcile nats --manual` command (the same one runbook §5.2 step 3 lists). We run
# that printed command verbatim — the operator's copy-paste — rather than a drill-authored render. It
# writes the tether-owned nats.d/ conf and touches no data dir, so it runs as tether.
_RECON=$(printf '%s' "$_F_CAP" | tr -d '\r' | grep -F 'tether cluster reconcile nats --manual --conf' | head -1 | sed 's/^[[:space:]]*//')
_recon_present() { [ -n "$_RECON" ]; }  # a FUNCTION, not `sh -c` — a new shell would not see $_RECON (R-NOSHC)
_dr_step "render this lone voter's nats.conf via the reconcile command restore printed (runbook §5.2 step 3)"
assert_setup "G-nats0 restore printed a runnable reconcile command to copy verbatim [operator per runbook §5.2 step 3]" _recon_present
assert_ok "G-nats1 (#52) run restore's OWN printed reconcile step to render the standalone auth_callout conf — no drill-authored render" \
    dexec -u tether brk1 -- sh -c "$_RECON"
assert_ok "G-nats2 restart nats-server to load the rendered conf (runbook §5.2 step 3)" \
    dexec brk1 -- sh -c 'systemctl reset-failed nats-server 2>/dev/null; systemctl restart nats-server'

# ── G-start — runbook §5.2 step 4: start the daemon ─────────────────────────────────────────────────
_dr_step "start the daemon (runbook §5.2 step 4)"
assert_ok "G-start reset-failed + start tether-broker" \
    dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start tether-broker'

# ── DR-COMPLETION GATE ──────────────────────────────────────────────────────────────────────────────
# The documented steps (restore --config seam, the #6 chown, the printed reconcile render, start) should
# now compose into a live single-voter broker. If they DON'T, that is a residual DR gap — recorded as
# INCOMPLETE, never cascaded into ten asserts against a broker that never started.
if ! poll_until 90 3 "the broker is live after the documented DR steps + the #6 chown" -- _brk1_ready; then
    _berr brk1 40 >&2 || true
    not_covered "51 DR-completion (G3/H/J/I): the documented DR procedure did not compose into a live broker on the real stack" \
        "runbook §5.2 + the #6 chown did not bring the single-voter broker up here (DR-STEP-LEDGER: $DR_UNDOCUMENTED undocumented step(s)). The seam/nats findings are asserted above; the tail is gated rather than cascaded" gap
    log "DR-STEP-LEDGER: runbook-§5.2-documented=$DR_DOCUMENTED actually-required=$DR_REQUIRED undocumented=$DR_UNDOCUMENTED gaps=[${DR_GAPS# }]"
    drill_end; exit "$?"
fi

# ── G3 — N=1 ready: the DR spine completed end-to-end via documented product verbs ──────────────────
assert_ok "G3a the broker is finally ready — the DR completed by following runbook §5.2 with product verbs (restore --config seam, printed reconcile render) + the one #6 chown, NOT a drill-authored seam" \
    poll_until 90 3 "brk1 ready after the DR sequence" -- _brk1_ready
assert_ok "G3b exactly one voter" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '[.nodes[]?|select(.phase==\"VOTER\")]|length==1' >/dev/null"
assert_ok "G3c status is DEGRADED/NOT-HA (runbook §5.1 step 3's own words) and NOT force_single" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '.health==\"DEGRADED\" and (.health_label|test(\"NOT-HA\")) and (.force_single_active != true)' >/dev/null"

# ── H — runbook §5.2 step 4: the fleet reconnects BY ITSELF ─────────────────────────────────────────
# #48's prohibition is inherited verbatim: no agent restart, no cache deletion, no env fiddling. If the
# agent does not self-heal, that is a finding, not something to paper over.
_dr_step "the agents reconnect and re-pin by themselves (NO agent restart is permitted here)"
# H1a — anti-vacuity for H1b, but the observation window is the agent's reconnect latency, which after a
# SAME-IDENTITY DR (secrets + tunnel cert byte-identical, so agt1 redials the instant brk1 answers) can be
# shorter than the shell between G-start and here — MEASURED 2026-07-19: agt1 was already ONLINE by H1a.
# The load-bearing non-vacuity is elsewhere: the restored DB carries STALE last_seen (the backup moment),
# so H1b's ONLINE can ONLY come from a FRESH post-reconnect heartbeat, and H2 curls a real rebuilt tunnel.
# So if agt1 already re-registered (a fast reconnect), record a runtime-guard — the window closed, the DR
# did not fail. A hard assert here just measures the sim's reconnect timing (drill 71's #29 lesson).
if _apy_offline; then
    _as_pass "H1a agt1 is NOT online yet — H1b's ONLINE is a real reconnect, not residue (the offline window was observable this run)"
else
    not_covered "51 H1a offline-window (transient offline state not observable in-sim — a drill coverage hole, not a re-run valve)" "agt1 re-registered faster than the shell reached H1a (a fast same-identity reconnect after the DR — its tunnel cert is byte-identical, so it redials the instant brk1 answers; NOT a failure). CLASS gap (R14 re-adjudication): this is NOT a 're-run and it lands' runtime-guard — the byte-identical-cert reconnect RELIABLY out-races the shell (MEASURED 2026-07-19 + this run), so re-running never catches the sub-second offline window; observing it would need a harness that samples transient state faster than the agent redials (or artificially holding the agent offline, which would game the env — forbidden). It is a persistent drill coverage hole. The restored DB's STALE last_seen already makes H1b's ONLINE non-vacuous, and H2 proves the served terminus, so the END state is fully covered." gap
fi
assert_ok "H1b agt1 comes back ONLINE with ZERO operator action on the agent" \
    poll_until 90 3 "agt1 self-heals back to ONLINE" -- _apy_online
# THE TERMINUS. Same public port, the exact pre-disaster sentinel, through a real rebuilt reverse tunnel.
assert_ok "H2 THE DR TERMINUS: the pre-disaster expose serves the EXACT original sentinel again on the same public port $P (never 'ps'/'expose explain'/'cluster status')" \
    poll_until 90 3 "the original sentinel through the rebuilt tunnel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"

# ── J — #53 after a total loss, RE-JUDGED per the R10 split (#53-silence CLOSED / #53-scope WONTFIX) ──
# #53 was split (docs/deploy-tier-gotchas.md, 2026-07-19). The OLD "already fixed" criterion here —
# "the pre-disaster history row SURVIVED; the bundle now carries JetStream state" — can NEVER hold under
# the WONTFIX-BY-DESIGN (b) decision, so it would peg #53 at PRODUCT-RED forever. The split resolves it:
#   #53-silence → CLOSED: BOTH ends warn (backup: B-vault1c; restore: J1 below). The lie was the SILENCE.
#   #53-scope   → WONTFIX-BY-DESIGN: the bundle stays state.db-only because pulling JetStream in would
#                 force `backup --offline` (premise: daemon stopped) to talk to a LIVE nats-server, making
#                 the offline/online bundles silently differ in scope — the exact class of lie #53 is about.
# So after a TOTAL loss (no surviving replica — 51's territory, unlike 50's lib-wipe) history genuinely
# does NOT come back, and that is now the WARNED, by-design outcome.

# #53-silence, RESTORE end: _F_CAP must state history/audit did NOT come back and print the runnable
# `nats stream restore` inverse. Three clauses, same shape as the backup end (B-vault1c).
_f_history_warned() {
    printf '%s' "$_F_CAP" | grep -qF 'HISTORY/AUDIT NOT RESTORED' &&
    printf '%s' "$_F_CAP" | grep -qF 'does NOT backfill' &&
    printf '%s' "$_F_CAP" | grep -qF 'nats stream restore'
}
assert_ok "J1 (#53-silence CLOSED, restore end) restore WARNED that history/audit did NOT come back and printed the runnable \`nats stream restore\` inverse — paired with B-vault1c (backup end), NEITHER end is silent" \
    _f_history_warned

# The by-design consequence. Prove the reader is ALIVE first (else 'row absent' could just mean a broken
# ctl), then judge. The restored state.db carries session 'lab', so `session ls` is the liveness control.
_ctl_alive()     { "$SIM" ctl -- session ls --json 2>/dev/null | jq -e "[.sessions[]?|select(.name==\"$SID\")]|length==1" >/dev/null 2>&1; }
_J_OUT=$("$SIM" ctl -- history -n 50 2>&1)
_j_row_present() { printf '%s' "$_J_OUT" | grep -q 'DR-HISTORY-SENTINEL'; }
if ! _ctl_alive; then
    not_covered "51 #53-scope reader liveness" "the ctl could not read the restored session '$SID' after the DR, so 'history absent' cannot be told apart from a broken reader — an in-sim reader issue, not a product finding (the terminus H2 already proved the data plane is up)" runtime-guard
elif _j_row_present; then
    # A surviving history row after a TOTAL loss (no replica) could ONLY mean the bundle silently carried
    # JetStream — the exact thing #53-scope's WONTFIX rejects. That is an APPEARS-CHANGED signal against a
    # deliberate design decision ⇒ ASSERT-FAIL so the split gets re-triaged, never a quiet pass.
    _as_fail "#53-scope APPEARS CHANGED — a pre-disaster history row survived a TOTAL-loss DR, which can only mean the bundle now carries JetStream, contradicting the state.db-only WONTFIX-BY-DESIGN decision. Re-triage the split before judging"
else
    # history is gone — the WARNED, by-design outcome. #53-silence is GREEN (J1 + B-vault1c). #53-scope is
    # the registered design boundary; book it as a gap whose reason is the WONTFIX rationale, so the
    # end-of-programme ledger shows the boundary explicitly rather than silently passing over it.
    not_covered "51 #53-scope (history/audit NOT recoverable from a state.db-only bundle after total loss)" \
        "WONTFIX-BY-DESIGN (docs/deploy-tier-gotchas.md #53-scope, 2026-07-19): the bundle stays state.db-only — pulling JetStream in would force \`backup --offline\` (premise: daemon stopped) to talk to a live nats-server, making offline/online bundles silently differ in scope (the same class of lie #53 was about). The SILENCE is closed (both ends warn: J1 + B-vault1c); recoverability is a by-design boundary. Take a \`nats stream backup\` alongside every bundle (the warning names it)" gap
fi

# ── I — re-grow (runbook §5.2 step 3b) — resilient to #31/#45, never laundered ──────────────────────
_dr_step "re-grow the cluster back to N>=2"
assert_ok "I-prep bring up a second broker container" "$SIM" up --brokers 2 --agents 1 --ctl 1
_I_OUT=$("$SIM" grow brk2 2>&1); _I_RC=$?
if [ "$_I_RC" = 0 ] && poll_until 90 3 "N=2 VOTER after the DR re-grow" -- _two_voters_now; then
    _as_pass "I re-grow to N=2 succeeded after the DR"
    assert_ok "I2 the data plane STILL serves the original sentinel after the re-grow" \
        poll_until 60 3 "sentinel still served at N=2" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"
elif printf '%s' "$_I_OUT" | grep -qiE 'grow of .* is in progress|already in flight|NATS_ROLLED_OUT'; then
    product_red "[#GROW-ONTO-RECOVERED] re-growing after a DR restore is blocked — the DR spine completed at N=1 (survivor de-clustered to standalone, NOT force_single, JS healthy per G3c above) yet 'cluster add brk2' cannot complete: the grow cutover re-clusters the lone survivor and the 1→2 clustered-JS meta never forms for the joiner, stalling the join op. This is the grow-onto-RECOVERED-broker family — sibling of 42's #GROW-ONTO-FORCE-SINGLE (grow onto a force-single survivor) and the pc732 grow-onto-resnapshot'd-broker incident (memory project_cluster_ha_realmachine_test). Runbook §5.2 step 3b cannot be finished; disclosed as a deep HA-path defect owed a dedicated batch (R15 did NOT rush a fix in the clustered-JS meta path). NOTE: the SEPARATE #31/#45 mid-grow-bundle residue (a leaked grow lock / stalled retire op carried into the restored origin when the bundle was captured mid-membership-op) IS closed by the R15 restore-side fix (normalizeRestoreStaging now clears grow/upgrade markers+leases+non-terminal ops; hermetic verifier TestRestoreClearsStaleGrowUpgradeAndOpResidue) — but THIS drill's bundle is from a healthy N=3, so that residue path is not what blocks here. Captured: $(printf '%s' "$_I_OUT" | tail -1)"
else
    # Stage-C minor 9: a VOTER-promotion timeout (grow_to_2/3's own flake, deliberately excluded from
    # FLAKE_SIG) must not blanket-ASSERT-FAIL over the pinned #51 PRODUCT-RED. Only a truly unclassified
    # failure is ASSERT-FAIL.
    if printf '%s' "$_I_OUT" | grep -qiE 'VOTER|did not reach|timed out|catch'; then
        not_covered "51 re-grow after DR (arm I)" "the re-grow hit a VOTER-promotion timeout (grow flake, excluded from FLAKE_SIG); the DR spine already completed at N=1 and #51 is pinned above" runtime-guard
    else
        _as_fail "I re-grow after the DR failed for an unclassified reason (rc=$_I_RC): $(printf '%s' "$_I_OUT" | tail -1)"
    fi
fi

# ── THE LEDGER — Mandate (4) made quantitative ──────────────────────────────────────────────────────
log "DR-STEP-LEDGER: runbook-§5.2-documented=$DR_DOCUMENTED actually-required=$DR_REQUIRED undocumented=$DR_UNDOCUMENTED gaps=[${DR_GAPS# }]"
_ledger_incomplete() { [ "$DR_UNDOCUMENTED" -gt 0 ]; }
if _ledger_incomplete; then
    log "DR-STEP-LEDGER verdict: runbook §5.2 is INCOMPLETE — $DR_UNDOCUMENTED step(s) the operator MUST perform are absent from the documented procedure (see [GAP] lines above). This is the quantitative form of 'an operation that only succeeds because the sim wrote clever bash is tether's failure being concealed'."
fi

drill_end
