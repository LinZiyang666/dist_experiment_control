#!/bin/sh
# 51-full-dr — S7 / G-C. N=3 -> TOTAL LOSS -> 1 -> 2: runbook §5.2's full-cluster disaster recovery,
# executed literally, step by step, with every deviation counted.
# Plan: docs/reviews/s7-s9-plan.md §2.2. Expected landing: PRODUCT-RED (#51/#52, source-confirmed).
# Runtime ~18min. Topology: 3 brokers + 1 agent + 1 ctl (grow-family).
#
# ── WHY THIS DRILL EXISTS (two source-confirmed findings) ────────────────────────────────────────────
# #51  `cluster init` calls applyClusterSeam (cmd/tether/cluster.go:794-804). `newClusterRestoreCmd`'s
#      flag set (cluster_backup.go:123-129) has NO --config at all, so `recovery restore` is
#      STRUCTURALLY INCAPABLE of applying the broker.yaml cluster seam. install.sh comments the whole
#      `cluster:` block out (:548-556). A fresh DR box therefore has a restored cluster-mode DB and an
#      unset broker.cluster.data_dir -> assertClusterDBConsistent FATALs (cutover.go:117-120):
#      "refusing to silently downgrade a cluster DB to single mode".
# #52  `init` prints its NEXT step-1 `reconcile nats --manual …` (cluster.go:824-826). `restore`'s
#      completion text says only "NEXT: start tether-broker, then cluster join approve"
#      (cluster_backup.go:115-119). The stock nats.conf has no authorization/auth_callout block
#      (install.sh:690-704), while cluster mode turns auth_callout ON automatically (serve.go:203-218).
#      runbook §5.2 (:566-574) never mentions nats.conf.
#
# => RUNBOOK §5.2 EXECUTED LITERALLY FAILS TWICE. Both are pinned with assert_bug. NEITHER is quietly
#    patched with an [env] step. That is the entire point of the drill.
#
# ── LABEL TAXONOMY (decidable, not a matter of taste) ────────────────────────────────────────────────
#   [env]                       sim supplies the machine (Mandate 3): up/install.sh, enable nats-server,
#                               chown after vault_push, systemctl reset-failed
#   [operator per runbook §x]   the doc's own script says a human does this
#   [by design: rejection #N]   the product explicitly declares it out of scope
#   [GAP #N]                    a step the script does NOT contain and tether cannot do => A DEFECT
# Decision rule: [operator] iff the action has a corresponding sentence in runbook §5.1/§5.2. Otherwise
# [GAP]. For #51/#52 the source-level discriminators are: (a) restore has no --config flag, so even as
# root it can NEVER apply the seam; (b) the sibling command (init) prints the very step restore omits.
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
#  6. #51/#52 are pinned as first-class assert_bug BEFORE any [GAP] clearing step (drill 40's discipline
#     w.r.t. #31). A wrong-reason failure is an ASSERT-FAIL; the signature is never widened to fit.
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
# 12. Broker application lines are in /var/log/tether/broker.err, NOT journalctl (R-BROKERLOG).

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"          # sctl / leader_node — agentyaml.sh depends on sctl
. "$HERE/lib/vault.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
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
DR_DOCUMENTED=4        # runbook §5.2 advertises FOUR steps
DR_REQUIRED=0          # steps we actually had to perform
DR_UNDOCUMENTED=0      # steps the script does NOT contain  => each one is a gap
DR_GAPS=""
_dr_step()  { DR_REQUIRED=$((DR_REQUIRED+1)); log "DR-STEP $DR_REQUIRED [operator per runbook §5.2]: $1"; }
_dr_env()   { log "DR-STEP [env, Mandate 3 — sim supplies the machine]: $1"; }
_dr_gap()   { DR_REQUIRED=$((DR_REQUIRED+1)); DR_UNDOCUMENTED=$((DR_UNDOCUMENTED+1)); DR_GAPS="$DR_GAPS $2"; warn "DR-STEP $DR_REQUIRED [GAP $2 — NOT in runbook §5.2; tether cannot do this itself]: $1"; }

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_pty() { _pn=$1; _pa=$2; shift 2; [ "$1" = "--" ] && shift; dexec -u tether "$_pn" -- python3 /opt/sim/pty-confirm.py "$_pa" -- "$@"; }
_berr() { dexec "$1" -- tail -n "${2:-80}" /var/log/tether/broker.err 2>/dev/null; }

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
assert_ok "B-vault1 online backup on the LEADER" \
    out_matches 'online backup complete: .*self=brk1, source=leader' \
    _bt brk1 -- tether cluster backup --out "$DR_BUNDLE"
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

_dr_step "restore the bundle onto the fresh box"
_F_CAP=$(_pty brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --raft-addr brk1:7400 2>&1); _F_RC=$?
# Predicates are FUNCTIONS: `sh -c` would not inherit these unexported captures (R-NOSHC family).
_f_rc_ok()     { [ "$_F_RC" = 0 ]; }
_f_pruned()    { printf '%s' "$_F_CAP" | grep -qE 'restore complete: node brk1 is now a single-voter cluster \(pruned 2 stale peers; bundle applied_index [0-9]+ reset to 0\)'; }
_f_freshbox()  { printf '%s' "$_F_CAP" | grep -qE 'prior DB preserved at: \(none'; }
assert_ok "F-b1 the DR restore succeeds [operator per runbook §5.2 step 2]" _f_rc_ok
assert_ok "F-b2 it pruned BOTH dead peers and reset the cursor" _f_pruned
# The fresh-box-exclusive discriminator: on a box that never had a DB there is nothing to preserve.
assert_ok "F-b3 'prior DB preserved at: (none — no prior DB on this host)' — the fresh-box-exclusive discriminator" _f_freshbox
assert_ok "F-b4 the roster is now EXACTLY {brk1} (#49-hardening: a resurrected pruned peer would be a restore-side defect)" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db 'select group_concat(node_id) from cluster_nodes' 2>/dev/null | tr -d '\r')\" = brk1 ]"
assert_ok "F-b5 the applied cursor was reset to 0" \
    sh -c "[ \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db \"select value from cluster_meta where key='applied_index'\" 2>/dev/null | tr -d '\r')\" = 0 ]"
assert_ok "F-b6 no restore_in_progress / force_single_active markers linger" \
    sh -c "[ -z \"\$(\"$SIM\" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db \"select key from cluster_meta where key in ('restore_in_progress','force_single_active')\" 2>/dev/null | tr -d '\r')\" ]"

# ── F-c / F-d — restore ADDRESS OVERRIDE + the non-overwriting .pre-restore.N backup ladder (plan §2.2) ─
# EXT-REVIEW-B8: these were promised by the plan but omitted; 50 dropped its own backup-ladder coverage on
# the claim that 51 owned it, so their absence was a real hole. A SECOND restore on a box that NOW has a DB
# (from F-b) must honour a DIFFERENT --raft-addr (re-stamped into cluster_nodes.raft_addr, restore.go:308)
# and preserve the prior DB to tether.db.pre-restore.bak (backupToUnique, init.go:342-354). A THIRD restore
# must ladder the backup to .pre-restore.1.bak (O_EXCL, never clobbers, init.go:354). String/paths are the
# product's real output (cluster_backup.go:115-119), not guessed. Prune is CONSTANT 2 (each restore re-stages
# from the 3-node bundle → self), so the .bak ladder — not the prune count — is the F-b/F-c/F-d discriminator.
_FC_CAP=$(_pty brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --raft-addr 127.0.0.1:7400 2>&1); _FC_RC=$?
_fc_rc_ok()      { [ "$_FC_RC" = 0 ]; }
# grep -qF (fixed substring), NOT a `$`-anchored regex: the _pty capture ends each line with a CR (\r),
# which an `…\.bak$` anchor never matches. `tether.db.pre-restore.bak` is not a substring of
# `…pre-restore.1.bak`, so this stays unambiguous vs the F-d ladder step.
_fc_preserved()  { printf '%s' "$_FC_CAP" | grep -qF 'prior DB preserved at: ' && printf '%s' "$_FC_CAP" | grep -qF 'tether.db.pre-restore.bak'; }
_fc_bak_exists() { "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.bak; }
_fc_addr()       { [ "$("$SIM" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db "select raft_addr from cluster_nodes where node_id='brk1'" 2>/dev/null | tr -d '\r')" = "127.0.0.1:7400" ]; }
assert_ok "F-c1 a SECOND restore with --raft-addr 127.0.0.1:7400 succeeds (address override on a box that now has a DB)" _fc_rc_ok
assert_ok "F-c2 it preserved the prior DB to tether.db.pre-restore.bak (unlike F-b's fresh box, there now IS a prior DB — 'preserved at' is honest)" _fc_preserved
assert_ok "F-c3 the .pre-restore.bak file really exists on disk" _fc_bak_exists
assert_ok "F-c4 the self node's raft_addr was really re-stamped to 127.0.0.1:7400 (restore.go:308 sqlite readback — not just the CLI's word)" _fc_addr
_FD_CAP=$(_pty brk1 brk1 -- tether cluster recovery restore "$DR_BUNDLE" --confirm-node-id brk1 --secrets-dir /etc/tether/secrets --raft-addr brk1:7400 2>&1); _FD_RC=$?
_fd_rc_ok()        { [ "$_FD_RC" = 0 ]; }
# grep -qF (fixed substring), NOT a `$`-anchored regex — the _pty capture ends lines with CR (\r), which
# breaks a `…\.1\.bak$` anchor (same fix as _fc_preserved).
_fd_ladder()       { printf '%s' "$_FD_CAP" | grep -qF 'prior DB preserved at: ' && printf '%s' "$_FD_CAP" | grep -qF 'tether.db.pre-restore.1.bak'; }
_fd_ladder_both()  { "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.bak && "$SIM" exec brk1 -- test -e /var/lib/tether/tether.db.pre-restore.1.bak; }
_fd_addr()         { [ "$("$SIM" exec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db "select raft_addr from cluster_nodes where node_id='brk1'" 2>/dev/null | tr -d '\r')" = "brk1:7400" ]; }
assert_ok "F-d1 a THIRD restore (back to --raft-addr brk1:7400) succeeds" _fd_rc_ok
assert_ok "F-d2 it LADDERED the prior-DB backup to .pre-restore.1.bak (O_EXCL → never overwrites .pre-restore.bak)" _fd_ladder
assert_ok "F-d3 BOTH .pre-restore.bak AND .pre-restore.1.bak exist — the ladder, not an overwrite" _fd_ladder_both
assert_ok "F-d4 the raft_addr is back to brk1:7400 (the override is not sticky across restores) — leaving the DB in the same effective state F-b produced, so the #51 tail below is unaffected" _fd_addr

# ── G1 — runbook §5.2 step 3, EXECUTED LITERALLY => #51 ─────────────────────────────────────────────
# Pinned BEFORE any clearing step (drill 40's #31 discipline). The signature must come from broker.err:
# the broker's application output is redirected there by install.sh:756-757 and is NOT in journald.
_dr_start_per_runbook() {
    dexec brk1 -- systemctl start tether-broker >/dev/null 2>&1
    if poll_until 30 3 "brk1 broker becomes ready per runbook §5.2 step 3" -- _brk1_ready; then return 0; fi
    _berr brk1 80 >&2
    return 1
}
assert_bug "runbook §5.2 step 3 'start the daemon' on a fresh DR box" "#51" \
    "data_dir is unset|refusing to silently downgrade a cluster DB to single mode" \
    _dr_start_per_runbook
_dr_gap "hand-write the broker.yaml cluster seam that \`recovery restore\` cannot apply (it has no --config flag at all, cluster_backup.go:123-129, while its sibling \`cluster init\` does it automatically, cluster.go:794-804)" "#51"
assert_ok "G1-clear [GAP #51] write the cluster seam by hand" \
    dexec brk1 -- sh -c "python3 - <<'EOF'
import re
p='/etc/tether/broker.yaml'
s=open(p).read()
if 'raft_addr:' not in s:
    s=s.rstrip('\n')+'\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk1:7400\n    nats_route: nats://brk1:6222\n'
    open(p,'w').write(s)
EOF
systemctl reset-failed tether-broker 2>/dev/null; true"

# ── G2 — start again => #52 (the nats.conf half); explore->pin, three outcomes, no cascade ──────────
# The stock nats.conf has no auth_callout block, so a cluster-mode broker either (a) fails with an
# auth/nkey signature (#52 confirmed), (b) fails for another reason — the hand-written seam is necessarily
# incomplete on the real stack, which is itself #51/#52's operational consequence (recorded, not a
# widened #52), or (c) comes up (unlikely — would withdraw #52). Never cascade a hand-clear that fails.
_dr_start_after_seam() {
    dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl restart tether-broker' >/dev/null 2>&1
    poll_until 30 3 "brk1 ready after the seam was hand-written" -- _brk1_ready
}
if _dr_start_after_seam; then
    ok "G2 the broker came up against the stock nats.conf after the seam — #52 does NOT hold; record + re-triage"
    _as_pass "G2 restored broker serves without a nats.conf render (#52 withdrawn — SB-51-NATSSIG outcome b)"
else
    _G2_ERR=$(_berr brk1 80)
    if printf '%s' "$_G2_ERR" | grep -qiE 'nkeys not supported|auth_callout|authorization|nkey|Authorization Violation'; then
        product_red "#52 \`recovery restore\` neither renders nor mentions nats.conf: the restored cluster-mode broker turns auth_callout ON (serve.go:203-218) but the fresh box still has install.sh's stock standalone conf with no authorization block (:690-704), so the daemon cannot serve. Its sibling \`cluster init\` prints the exact \`reconcile nats --manual\` step (cluster.go:824-826) that restore omits. Captured: $(printf '%s' "$_G2_ERR" | grep -iE 'nkey|auth' | tail -1)"
        # EXT-REVIEW-B8: #52 (render nats.conf with an auth_callout block) is a step the DR PROCEDURE
        # requires but runbook §5.2 omits — so it is a required, undocumented GAP and must be booked through
        # the SAME `_dr_gap` helper as #51 (REQUIRED++ AND UNDOCUMENTED++). The prior code bumped only
        # DR_UNDOCUMENTED, leaving required=4 while plan §12 claimed required=5 — an inconsistent denominator.
        # (That this run then gives up before executing the render is captured by the DR-completion gate; the
        # ledger counts what the procedure REQUIRES, exactly as it does for the #51 seam gap.)
        _dr_gap "render nats.conf with an auth_callout block (the operator must run \`reconcile nats --manual\`) — restore neither renders nor mentions it, yet a cluster-mode broker cannot serve without it" "#52-natsconf"
    else
        # The broker did not come up, but not for an auth reason: the hand-written seam is incomplete on
        # the real stack. That is #51/#52's operational consequence, captured by the DR-completion gate
        # below — NOT a widened #52 and NOT a cascade.
        log "G2 note: after the hand-written seam the broker still did not start, and broker.err shows no auth/nkey signature — the manual seam is incomplete on the real stack. Captured: $(printf '%s' "$_G2_ERR" | tail -1). The DR-completion gate below records this honestly."
    fi
fi

# ── DR-COMPLETION GATE ──────────────────────────────────────────────────────────────────────────────
# #51/#52 are now PINNED. The remaining tail (G3/H/I) only makes sense if the manual [GAP] clears
# actually brought the broker up. On the real stack the documented manual recovery is itself incomplete
# (that IS #51/#52's operational consequence), so if the broker is not live here, the honest verdict is
# "DR cannot be completed by following the documented procedure" — recorded as INCOMPLETE, NOT cascaded
# into ten asserts against a broker that structurally never started.
if ! poll_until 60 3 "the broker is live after the manual gap-clears" -- _brk1_ready; then
    _berr brk1 40 >&2 || true
    not_covered "51 DR-completion (G3/H/I): the documented manual recovery could not bring the broker up on the real stack"         "runbook §5.2 + the two [GAP] clears (#51 seam, #52 nats.conf) do not compose into a working single-voter broker here — which IS #51/#52's operational consequence (DR-STEP-LEDGER shows $DR_UNDOCUMENTED undocumented steps). The core findings are pinned above; the tail is gated rather than cascaded"
    log "DR-STEP-LEDGER: runbook-§5.2-documented=$DR_DOCUMENTED actually-required=$DR_REQUIRED undocumented=$DR_UNDOCUMENTED gaps=[${DR_GAPS# }]"
    drill_end; exit "$?"
fi

# ── G3 — N=1 ready (the CONTROL SOURCE for G1/G2: it proves the box can come up at all) ─────────────
_dr_step "start the daemon"
assert_ok "G3a the broker is finally ready (the control source: G1/G2's refusals were about the missing steps, not a dead box)" \
    poll_until 90 3 "brk1 ready after the DR sequence" -- _brk1_ready
assert_ok "G3b exactly one voter" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '[.nodes[]?|select(.phase==\"VOTER\")]|length==1' >/dev/null"
assert_ok "G3c status is DEGRADED/NOT-HA (runbook §5.1 step 3's own words) and NOT force_single" \
    sh -c "\"$SIM\" exec brk1 -- sh -c 'runuser -u tether -- tether cluster status --json' | jq -e '.health==\"DEGRADED\" and (.health_label|test(\"NOT-HA\")) and (.force_single_active != true)' >/dev/null"

# ── H — runbook §5.2 step 4: the fleet reconnects BY ITSELF ─────────────────────────────────────────
# #48's prohibition is inherited verbatim: no agent restart, no cache deletion, no env fiddling. If the
# agent does not self-heal, that is a finding, not something to paper over.
_dr_step "the agents reconnect and re-pin by themselves (NO agent restart is permitted here)"
assert_ok "H1a the agent is NOT online yet (proving it BEFORE the broker starts — the ~5s heartbeat residue would false-green this)" \
    _apy_offline
assert_ok "H1b agt1 comes back ONLINE with ZERO operator action on the agent" \
    poll_until 90 3 "agt1 self-heals back to ONLINE" -- _apy_online
# THE TERMINUS. Same public port, the exact pre-disaster sentinel, through a real rebuilt reverse tunnel.
assert_ok "H2 THE DR TERMINUS: the pre-disaster expose serves the EXACT original sentinel again on the same public port $P (never 'ps'/'expose explain'/'cluster status')" \
    poll_until 90 3 "the original sentinel through the rebuilt tunnel" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"

# ── J — history/audit after a total loss (explore -> pin) ───────────────────────────────────────────
# Mechanism is already source-known: the bundle carries state.db only (backup.go:87) and
# audit_published_index is reset to 0 (restore.go:317), so nothing can re-derive it. Absence is only
# evidence if the reader is proven alive first.
_J_OUT=$("$SIM" ctl -- history -n 50 2>&1); _J_RC=$?
if [ "$_J_RC" != 0 ] && printf '%s' "$_J_OUT" | grep -qiE 'no such stream|not found|stream'; then
    product_red "#53 after a full-cluster DR the audit/history trail is GONE and nothing warns: the backup bundle carries state.db only (backup.go:87) while history lives in the JetStream history-<sid> stream, and restore resets audit_published_index to 0 (restore.go:317) so it can never be re-derived. runbook §5 never tells the operator their audit trail will not come back (DOC-19). Captured: $(printf '%s' "$_J_OUT" | tail -1)"
elif [ "$_J_RC" = 0 ] && printf '%s' "$_J_OUT" | grep -q 'DR-HISTORY-SENTINEL'; then
    _as_fail "#53 APPEARS FIXED — the pre-disaster history row survived the DR; the bundle now carries JetStream state. Withdraw #53 and re-triage DOC-19"
elif [ "$_J_RC" = 0 ]; then
    _as_pass "J history reads cleanly but the pre-disaster rows are gone (bundle is state.db only) — #53's operator-facing effect confirmed with a live reader"
else
    _as_fail "#53 UNJUDGEABLE — history returned rc=$_J_RC with no stream-shaped signature; triage before judging: $(printf '%s' "$_J_OUT" | tail -1)"
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
    product_red "[#31/#45] re-growing after a DR restore is blocked by the leaked grow lock / a stalled retire op — the DR spine itself completed at N=1, but runbook §5.2 step 3b cannot be finished. Captured: $(printf '%s' "$_I_OUT" | tail -1)"
else
    # Stage-C minor 9: a VOTER-promotion timeout (grow_to_2/3's own flake, deliberately excluded from
    # FLAKE_SIG) must not blanket-ASSERT-FAIL over the pinned #51 PRODUCT-RED. Only a truly unclassified
    # failure is ASSERT-FAIL.
    if printf '%s' "$_I_OUT" | grep -qiE 'VOTER|did not reach|timed out|catch'; then
        not_covered "51 re-grow after DR (arm I)" "the re-grow hit a VOTER-promotion timeout (grow flake, excluded from FLAKE_SIG); the DR spine already completed at N=1 and #51 is pinned above"
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
