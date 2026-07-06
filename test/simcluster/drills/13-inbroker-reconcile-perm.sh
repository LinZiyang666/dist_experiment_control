#!/bin/sh
# 13-inbroker-reconcile-perm.sh — #22 (G1 FIXED, Option B): the in-broker C3 topology reconciler
# (User=tether) atomically rewrites its nats.conf (os.CreateTemp + rename), which needs a tether-WRITABLE
# directory. Pre-G1 that conf lived in the root-owned /etc/tether and the write perm-denied, so topology
# never auto-converged (this drill was RED). G1 relocates the conf into the tether-owned
# /etc/tether/nats.d/ subdir while /etc/tether ITSELF stays root-owned (the root-run caddy reads
# /etc/tether/Caddyfile — a tether-owned /etc/tether would be a tether->root privesc). So post-fix:
#   - /etc/tether stays root-owned (Caddyfile safe — the Option B invariant)
#   - /etc/tether/nats.d is tether-owned (the reconciler CAN write)
#   - a User=tether reconcile write into nats.d/ SUCCEEDS (was perm-denied pre-G1)
#   - grow's own `reconcile nats --all` auto-converges on a second grow (the perm-denied reason is gone)
# This drill is now a plain GREEN regression. If the product REGRESSES (nats.d/ missing, or /etc/tether
# hand-chowned), the asserts below go RED again.
#
# NB: /etc/tether is NATURALLY root-owned here (fresh docker named volume + install.sh never chowns ETC);
# /etc/tether/nats.d is tether-owned by the REAL install.sh (`install -d -o tether`), never a sim chown
# (Mandate ①). Re-vendor scripts/install.sh + rebuild the image before running (a stale image would still
# lack nats.d/ and this drill would correctly go RED).
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
DLOG=/tmp/sim-13-grow-brk3.log

drill_begin "#22 in-broker reconciler (User=tether) writes its nats.conf in the tether-owned nats.d/ (G1 fixed)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers"              "$SIM" up --brokers 3
assert_ok "init brk1 (N=1)"           "$SIM" init brk1
assert_ok "grow brk2 (N=2, clustered)" "$SIM" grow brk2
# a SECOND grow (N=2->3): the confs are already clustered, so the auto-reconcile path reaches the WRITE and,
# post-#22-fix, the in-broker reconcilers (User=tether, brk1+brk2) write the tether-owned nats.d/ and
# CONVERGE — no permission-denied reason. Capture the transcript.
"$SIM" grow brk3 >"$DLOG" 2>&1 || true
assert_ok "grow brk3 reached VOTER (N=3)" sh -c "grep -q 'is now a VOTER' '$DLOG'"

# CONTROL 1 (Option B invariant): /etc/tether STAYS root-owned (Caddyfile stays root-only). If it were
# tether-owned, someone hand-chowned it (the rejected pre-Option-B fix) — a tether->root privesc.
assert_ok "/etc/tether DIRECTORY is root-owned (Option B: Caddyfile stays root-only)" \
    sh -c "[ \"\$($SIM exec brk1 -- stat -c %U /etc/tether)\" = root ]"
# CONTROL 2 (#22 fix present): /etc/tether/nats.d is tether-owned (install.sh's `install -d -o tether`).
assert_ok "/etc/tether/nats.d is tether-owned (the #22 fix — reconciler-writable subdir)" \
    sh -c "[ \"\$($SIM exec brk1 -- stat -c %U /etc/tether/nats.d)\" = tether ]"

# SEAM GUARD (internal-review): every voter's broker.yaml must pin nats_conf_path at the nats.d/ path. A
# straggler on the OLD /etc/tether/nats.conf strands that node's reconciler at a nonexistent file (stat
# source: no such file), which would slip past the narrow perm-denied signature below — MASKING a
# non-converging node behind a GREEN drill. Assert every voter, so an init/grow seam left on the old path
# fails RED instead of masking.
assert_ok "every voter broker.yaml pins nats_conf_path at nats.d/ (no old-path seam straggler)" \
    sh -c "for b in brk1 brk2 brk3; do $SIM exec \$b -- grep -qE '^ *nats_conf_path: */etc/tether/nats.d/nats.conf *\$' /etc/tether/broker.yaml || { echo \"\$b broker.yaml nats_conf_path not nats.d/\"; exit 1; }; done"

# #22 FIXED via the REAL AUTO PATH: grow's `reconcile nats --all --wait` on the second grow no longer
# perm-denies — the in-broker reconciler writes the tether-owned nats.d/. Assert the perm-denied reason is
# GONE (it was the RED signature pre-G1). If it REAPPEARS, the fix regressed.
assert_ok "#22 fixed: grow's reconcile nats --all shows NO natsconf permission-denied reason" \
    sh -c "! grep -qiE 'natsconf: *(temp|write \\.bak):.*permission denied' '$DLOG'"
# and grow's trailer must NOT carry #22 anymore (it drops when the auto path converges — the honest signal):
assert_ok "grow trailer does NOT carry #22 (auto path converged post-fix)" \
    sh -c "! { grep -E 'GREW-VIA-WORKAROUNDS:' '$DLOG' | grep -qE '(^|[ ,])#22([,]|\$)'; }"

# #22 DIRECT WRITE (the exact natsconf.Apply path the in-broker reconciler shares): a User=tether reconcile
# write into the tether-owned nats.d/ now SUCCEEDS (was perm-denied in root-owned /etc/tether pre-G1).
# --skip-dry-run skips the `nats-server -t` validator; --allow-partial-mesh skips the live-roster voter
# coverage check (passing only brk2 as --peer with N=3 voters would otherwise refuse for a non-#22 reason).
ACCT=$(secrets_account_pub "$INSTANCE")
BRK=$(secrets_broker_pub "$INSTANCE" brk1)
PEER=$(peer_triple "$INSTANCE" brk2)
assert_ok "derived well-formed reconcile args (account pub A…, broker nkey U…, peer triple)" \
    sh -c "printf %s '$ACCT' | grep -qE '^A' && printf %s '$BRK' | grep -qE '^U' && printf %s '$PEER' | grep -qE ',U'"
assert_ok "#22 fixed: a User=tether nats.conf write into the tether-owned nats.d/ SUCCEEDS" \
    dexec -u tether brk1 -- tether cluster reconcile nats --manual --skip-dry-run --allow-partial-mesh --secrets-dir /etc/tether/secrets \
        --server-name brk1 --route-url nats://brk1:6222 --account-issuer "$ACCT" --broker-nkey "$BRK" \
        --peer "$PEER" --conf /etc/tether/nats.d/nats.conf

# REGRESSION GUARD: if the product reverts (nats.d/ removed, or /etc/tether hand-chowned), CONTROL 2 fails
# AND the User=tether write perm-denies — the drill goes RED, flagging the regression.
drill_end
