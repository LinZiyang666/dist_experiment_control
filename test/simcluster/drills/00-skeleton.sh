#!/bin/sh
# 00-skeleton.sh — walking skeleton (GREEN-only acceptance gate; plan §5.0).
# Proves the whole stack end-to-end on a real-systemd cross-process cluster: standalone→N=1 cutover,
# routes-mTLS/auth_callout, real `agent join`, and a tier-A + tier-B push/pull round-trip. Nothing
# else in the tool is trusted until this is green. Env injected by `simcluster drill`: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

drill_begin "00-skeleton (N=1 → agent join → push/pull)"

"$SIM" nuke >/dev/null 2>&1 || true

assert_ok  "up 1 broker + 1 agent + 1 ctl"          "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok  "init brk1 (standalone → N=1 cluster)"    "$SIM" init brk1
assert_ok  "N=1 healthy floor (leader + DEGRADED)"   sh -c "\"$SIM\" ctl -- >/dev/null 2>&1; \"$SIM\" exec brk1 -- sh -c 'tether cluster status --json' | jq -e '.leader_id==\"brk1\" and (.health==\"DEGRADED\" or .health==\"HEALTHY_HA\") and .exit_code==1' >/dev/null"
assert_ok  "session $SID + ctl login"                "$SIM" session "$SID" --pin "$PIN"
assert_ok  "agent-join agt1"                         "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
# The KIND column (device|leased) sits between NODE and STATUS since 1e9d32a (2026-08-19), so
# "agt1 followed by whitespace then ONLINE" can no longer match anything. Naming KIND rather than
# loosening to `.*ONLINE` keeps the assertion able to see a column reordering — and a device that
# shows up as `leased` is a real defect this row should still be able to fail on.
assert_ok  "node ls lists agt1 ONLINE"               sh -c "\"$SIM\" ctl -- node ls | grep -qE '^agt1[[:space:]]+(device|leased)[[:space:]]+ONLINE'"

# --- push/pull round-trip: tier-A (inline ≤8 MiB) + tier-B (JetStream Object Store >8 MiB) ---
assert_ok  "seed tier-A (1 KiB) + tier-B (20 MiB) files on ctl" \
    "$SIM" exec ctl1 -- sh -c 'head -c 1024 /dev/urandom > /tmp/tierA.bin && head -c 20000000 /dev/urandom > /tmp/tierB.bin && chmod 644 /tmp/tierA.bin /tmp/tierB.bin'

assert_ok  "tier-A push ctl→agt1"                    "$SIM" ctl -- push /tmp/tierA.bin agt1:/tmp/tierA.bin
assert_ok  "tier-A pull agt1→ctl"                    "$SIM" ctl -- pull agt1:/tmp/tierA.bin /tmp/tierA.back
assert_ok  "tier-A round-trip sha256 matches"        "$SIM" exec ctl1 -- sh -c 'a=$(sha256sum < /tmp/tierA.bin); b=$(sha256sum < /tmp/tierA.back); [ "$a" = "$b" ]'

assert_ok  "tier-B push ctl→agt1 (JS Object Store)"  "$SIM" ctl -- push /tmp/tierB.bin agt1:/tmp/tierB.bin
assert_ok  "tier-B pull agt1→ctl"                    "$SIM" ctl -- pull agt1:/tmp/tierB.bin /tmp/tierB.back
assert_ok  "tier-B round-trip sha256 matches"        "$SIM" exec ctl1 -- sh -c 'a=$(sha256sum < /tmp/tierB.bin); b=$(sha256sum < /tmp/tierB.back); [ "$a" = "$b" ]'

drill_end
