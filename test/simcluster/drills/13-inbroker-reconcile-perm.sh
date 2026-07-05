#!/bin/sh
# 13-inbroker-reconcile-perm.sh — #22 RED: install.sh leaves the /etc/tether DIRECTORY root-owned, so a
# User=tether write of nats.conf perm-denies its atomic os.CreateTemp/`.bak` there. The in-broker C3
# reconciler (User=tether) hits THIS EXACT write path (internal/natsconf/takeover.go:185/190), so it can
# never auto-converge topology → the operator is forced to hand-render as root (why `simcluster grow`
# labels its mesh render [workaround #22/#3]).
#
# WHAT THIS DRILL EXERCISES: TWO views of #22. (1) PRIMARY — the REAL AUTO PATH: on the second grow
# (N=2→3, confs already clustered) grow's own `reconcile nats --all --wait` reaches the WRITE and the
# AUTOMATIC in-broker reconcilers (User=tether, brk1+brk2) perm-deny; we assert their real
# 'apply: natsconf: temp: … permission denied' non-convergence reason. (2) SECONDARY — the ISOLATED WRITE:
# `reconcile nats --manual` AS TETHER runs the SAME natsconf.Apply write directly (assert_bug flip
# semantics), decoupled from grow's timing. RED until install.sh chowns ETC to tether; flip to a plain
# regression then. Env: SIM, HERE, INSTANCE.
#
# NB: /etc/tether is NATURALLY root-owned here (a fresh docker named volume mounts root:root + install.sh
# never chowns ETC) — provision-node.sh deliberately does NOT chown it; `doctor` tripwires a tether-owned
# /etc/tether as a masked-#22 warning (an earlier build once baked a masking chown).
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
DLOG=/tmp/sim-13-grow-brk3.log

drill_begin "#22 in-broker reconciler (User=tether) cannot write the root-owned /etc/tether"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers"              "$SIM" up --brokers 3
assert_ok "init brk1 (N=1)"           "$SIM" init brk1
assert_ok "grow brk2 (N=2, clustered)" "$SIM" grow brk2
# a SECOND grow (N=2→3): the confs are already clustered, so the auto-reconcile path reaches the WRITE and
# the REAL in-broker reconcilers (User=tether, on brk1+brk2) perm-deny in /etc/tether — grow's own
# `reconcile nats --all` attempt observes it. Capture the transcript.
"$SIM" grow brk3 >"$DLOG" 2>&1 || true
assert_ok "grow brk3 reached VOTER (N=3)" sh -c "grep -q 'is now a VOTER' '$DLOG'"

# CONTROL — the #22 axis is the DIRECTORY owner (CreateTemp needs write on the dir; a file chown does not
# help). If this FAILS (dir is tether-owned) the sim is MASKING #22 — that is a control failure, not a
# silent gap loss, so it surfaces here rather than as a false-green on the assert_bug below.
assert_ok "/etc/tether DIRECTORY is root-owned (the #22 axis — CreateTemp needs dir write)" \
    sh -c "[ \"\$($SIM exec brk1 -- stat -c %U /etc/tether)\" = root ]"

# #22 via the REAL AUTO PATH (the honest reproduction): grow's `reconcile nats --all --wait` on the second
# grow timed out because the AUTOMATIC in-broker C3 reconcilers (User=tether) perm-denied their nats.conf
# write in the root-owned /etc/tether. Assert the real CLI timeout string AND the in-broker reconciler's
# own 'apply: natsconf: temp: … permission denied' reason are on the record — this observes the ACTUAL
# reconciler non-convergence (not the manual proxy below). Flips when install.sh chowns ETC → tether.
assert_ok "#22 (real auto path): grow's reconcile nats --all observed the in-broker reconciler perm-deny (apply: natsconf: temp: … permission denied)" \
    sh -c "grep -qiE 'reconcile nats: timed out.*not converged' '$DLOG' && grep -qiE 'apply: *natsconf: *temp:.*permission denied' '$DLOG'"

# #22 SECONDARY (isolated write, assert_bug flip semantics): reproduce the SAME natsconf.Apply write AS
# TETHER directly, decoupled from grow's timing. Apply writes a pristine `.bak` FIRST when none exists
# (takeover.go:185), then the `.nats.conf.*` temp (takeover.go:190); EITHER perm-denies first, so the
# signature accepts both. `--skip-dry-run` skips the `nats-server -t` validator (so a nats-server bump
# can't HARD-FAIL it); `--allow-partial-mesh` skips the live-roster voter-coverage check (so passing only
# brk2 as --peer with N=3 voters doesn't refuse for a non-#22 reason) — the WRITE is still what fails.
# Signature pinned to the natsconf-prefixed path token so gotcha #6's root-owned tether.lock (no `natsconf:`
# prefix) can't false-match. Validity of the nkeys is guaranteed by `grow` above having consumed them.
ACCT=$(secrets_account_pub "$INSTANCE")
BRK=$(secrets_broker_pub "$INSTANCE" brk1)
PEER=$(peer_triple "$INSTANCE" brk2)
assert_ok "derived well-formed reconcile args (account pub A…, broker nkey U…, peer triple)" \
    sh -c "printf %s '$ACCT' | grep -qE '^A' && printf %s '$BRK' | grep -qE '^U' && printf %s '$PEER' | grep -qE ',U'"
assert_bug "#22 a User=tether nats.conf write cannot create its temp/.bak in the root-owned /etc/tether (the write the in-broker reconciler shares)" \
    "#22" "natsconf: *(temp|write \.bak):.*permission denied|/etc/tether/[.]?nats\.conf(\.bak)?\.[0-9A-Za-z]+.*permission denied" \
    dexec -u tether brk1 -- tether cluster reconcile nats --manual --skip-dry-run --allow-partial-mesh --secrets-dir /etc/tether/secrets \
        --server-name brk1 --route-url nats://brk1:6222 --account-issuer "$ACCT" --broker-nkey "$BRK" \
        --peer "$PEER" --conf /etc/tether/nats.conf

# FLIP when install.sh chowns /etc/tether to tether (the #22 product fix): provision does NOT force the
# owner, so a fixed install.sh makes ETC tether-owned → the "/etc/tether DIRECTORY is root-owned" control
# above FAILS (RED) AND the assert_bug write succeeds (exit 0 → "APPEARS FIXED") — either way the drill
# goes RED, signalling "promote to a plain regression". (It does not silently stay GREEN.)
drill_end
