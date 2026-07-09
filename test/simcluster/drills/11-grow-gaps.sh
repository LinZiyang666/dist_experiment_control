#!/bin/sh
# 11-grow-gaps.sh — G4 INVERTED: the manual grow gaps (#3/#4/#5/#8/#10) are GONE. `simcluster grow` now drives
# `tether cluster add`, which performs init/approve/mesh-render/former-N1-JS-reset/catch-up ITSELF — so the
# tether workaround SIGNATURES this drill used to assert (as RED) are now asserted ABSENT, and the
# GREW-VIA-WORKAROUNDS trailer is replaced by GREW-VIA-TETHER-CLUSTER-ADD. The #I1 serve-refusal invariant
# (a fresh joiner without raft state must NOT auto-bootstrap a cluster) STAYS — it is a fail-CLOSED guard
# tether keeps, not a workaround. Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
GLOG=/tmp/sim-11-grow-brk2.log

drill_begin "#8/#I1 grow via tether cluster add (workarounds GONE; #I1 serve-refusal invariant STAYS)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers"     "$SIM" up --brokers 3
assert_ok "init brk1 (N=1)"  "$SIM" init brk1

# Run a REAL grow (first grow, N=1→2) through `tether cluster add`, capturing its transcript.
"$SIM" grow brk2 >"$GLOG" 2>&1 || true
assert_ok "grow brk2 reaches VOTER via \`tether cluster add\`" \
    sh -c "grep -q 'is now a VOTER' '$GLOG'"
assert_ok "grow ran through tether cluster add (trailer names it, not workarounds)" \
    sh -c "grep -q 'GREW-VIA-TETHER-CLUSTER-ADD' '$GLOG'"

# m7 (#4 move-not-delete): brk1 ran as N=1 with a live standalone JS store (events/history streams), so the
# first-grow former-N1 cutover reset it via --reset-former-js — which MOVES it aside to jetstream.grow-bak.<epoch>,
# NEVER deletes it. Assert the backup exists (a delete-instead-of-move regression flips this).
assert_ok "#4 move-not-delete: former-N1 brk1 JS store moved to jetstream.grow-bak.<epoch> (never deleted)" \
    sh -c "$SIM exec brk1 -- sh -c 'ls -d /var/lib/tether/jetstream.grow-bak.* 2>/dev/null | grep -q grow-bak'"

# #8 INVERTED: the orchestrator issues `approve-join` NON-BLOCKING (no `--wait`), so tether's own pre-mesh
# give-up line ('still in flight (<state>) after 30s' / 'is BLOCKED') is NEVER printed — the chicken-and-egg
# stall is gone. Assert the signature is ABSENT.
assert_ok "#8 GONE: no pre-mesh 'still in flight'/'is BLOCKED' give-up (non-blocking approve)" \
    sh -c "! grep -qiE 'still in flight|is BLOCKED' '$GLOG'"

# #3 INVERTED: cluster add renders the first-grow route mesh via the secrets-dir mTLS fallback, so the
# 'no cluster{} block to harvest' / 'reconcile nats: timed out … not converged' failure is gone. Assert absent.
assert_ok "#3 GONE: no 'no cluster block to harvest' / 'reconcile … not converged' (secrets-dir fallback)" \
    sh -c "! grep -qiE 'no cluster.{0,4}block to harvest|reconcile nats: timed out.*not converged' '$GLOG'"

# The old GREW-VIA-WORKAROUNDS trailer (with #3/#4/#5/#8/#10 tokens) must NOT appear — cluster add does the work.
assert_ok "no GREW-VIA-WORKAROUNDS trailer (the manual grow is retired)" \
    sh -c "! grep -q 'GREW-VIA-WORKAROUNDS' '$GLOG'"

# TETHER-DOES-IT gate (Mandate ②, plan §9-A): cmd_grow must contain NO residual cluster-LIFECYCLE bash — every
# init/approve/mesh-render/JS-reset/catch-up is inside `tether cluster add`. The only sctl/dexec left in
# cmd_grow are provisioning (secrets-mint, standalone-boot, ctl login, root-owned broker.yaml seam, stop for
# init, restart-nats-to-load-conf, start) — never a hand `cluster init`/`join`/`reconcile nats`/`mv jetstream`.
_grow_src=$(awk '/^cmd_grow\(\)/{f=1} f{print} f&&/^}/{exit}' "$SIM")
assert_ok "gate A: cmd_grow does NOT hand-run cluster init/join/reconcile-nats/JS-reset (cluster add owns them)" \
    sh -c "! printf '%s' \"\$0\" | grep -vE '^\s*#' | grep -qiE 'tether cluster (init|join|reconcile)|mv .*jetstream|reconcile nats --(all|manual)|pty-confirm'" "$_grow_src"
assert_ok "gate A: cmd_grow DOES invoke tether cluster add" \
    sh -c "printf '%s' \"\$0\" | grep -q 'tether cluster add'" "$_grow_src"

# #I1 (STAYS — a fail-CLOSED invariant tether KEEPS, NOT a workaround): a fresh joiner with the cluster seam
# but NO `cluster init` (no raft state) must REFUSE to serve in cluster mode — the daemon never auto-bootstraps
# a cluster onto a live DB (internal/broker/cutover.go). `cluster add` performs the init itself, so a real grow
# never hits this; the invariant is proven directly here on brk3 (never grown).
assert_ok "brk3 is a fresh broker with no raft state (never grown)" \
    sh -c "! $SIM exec brk3 -- test -d /var/lib/tether/raft"
secrets_mint_node "$INSTANCE" brk3 >/dev/null 2>&1
secrets_distribute "$INSTANCE" brk3 >/dev/null 2>&1
assert_ok "add the cluster seam to fresh brk3 (NO init)" \
    dexec brk3 -- sh -c "printf '  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk3:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n    nats_server_bin: nats-server\n' >> /etc/tether/broker.yaml"
assert_refuses "#I1 INVARIANT: cluster-mode serve refuses a fresh joiner with no raft state (fail-closed; cluster add does the init)" \
    "no raft state exists|never auto-bootstraps" \
    dexec -u tether brk3 -- sh -c "timeout 8 tether serve --config /etc/tether/broker.yaml 2>&1"

drill_end
