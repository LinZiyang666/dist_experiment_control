#!/bin/sh
# Independent external-review checks for the S7-S9 simcluster batch.
# These tests intentionally exercise harness contracts rather than tether product code.
set -u

HERE=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
SIMROOT=$(CDPATH= cd -- "$HERE/.." && pwd)
FAIL=0

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1" >&2; FAIL=$((FAIL + 1)); }

# A signal cleanup handler must terminate after cleanup. In dash/POSIX sh, a trapped
# TERM returns to the next command unless the handler exits explicitly.
signal_out=$(sh -c 'trap "printf cleanup\\n" TERM; kill -TERM $$; printf continued\\n' 2>&1)
case "$signal_out" in
  *continued*) pass "control: a TERM trap without exit continues execution" ;;
  *) fail "control invalid: TERM trap unexpectedly stopped the shell" ;;
esac
trap_users=$(grep -l "trap '_cleanup' EXIT INT TERM" \
  "$SIMROOT"/drills/50-backup-restore.sh \
  "$SIMROOT"/drills/51-full-dr.sh \
  "$SIMROOT"/drills/52-credential-rotation.sh \
  "$SIMROOT"/drills/94-agent-reconcile.sh \
  "$SIMROOT"/drills/95-broker-selfheal.sh \
  "$SIMROOT"/drills/96-mid-flight-chaos.sh \
  "$SIMROOT"/drills/97-soak-cycles.sh 2>/dev/null || true)
if [ -z "$trap_users" ]; then
  pass "drill signal handlers terminate instead of resuming destructive work"
else
  fail "drill signal handlers run cleanup but then resume: $(printf '%s' "$trap_users" | tr '\n' ' ')"
fi

# rotate-tunnel-cert hot-swaps only the server side; an established tunnel can keep
# serving bytes without observing the new certificate. A short silent DROP proves a
# new TCP connect hangs, but does not close an established TCP/yamux session. Therefore
# post-heal traffic alone still cannot prove that a fresh TLS handshake occurred.
a7_block=$(sed -n '/# A7 /,/# A8 /p' "$SIMROOT/drills/52-credential-rotation.sh")
if printf '%s' "$a7_block" | grep -v '^[[:space:]]*#' | grep -q 'fault_partition_on agt1 7000'; then
  fail "52-A7 installs local-port DROP rules on agt1, but 7000/4222 are listening on brk1; the agent's outbound connections are not cut"
elif printf '%s' "$a7_block" | grep -q 'fault_partition_off' &&
   printf '%s' "$a7_block" | grep -q -- '-- dp_curl_ok_body' &&
   ! printf '%s' "$a7_block" | grep -v '^[[:space:]]*#' | grep -qE '(journalctl|conntrack|session.*generation|reconnect.*event|redial.*event)'; then
  fail "52-A7 heals a short DROP then uses traffic alone; it still does not prove the old TLS/yamux session was replaced"
elif grep -q 'if poll_until 45 3 "the agent re-pins on its own after the rotation" -- dp_curl_ok_body' \
     "$SIMROOT/drills/52-credential-rotation.sh"; then
  fail "52-A7 accepts traffic on the pre-existing TLS tunnel as proof of re-pin"
else
  pass "52-A7 independently proves a new TLS/redial generation before claiming re-pin"
fi

# A8 replaces the host-stash copy before the negative arm. Recovery must preserve
# and explicitly restore the previously pinned generation, not push the new leaf again.
a8_recover=$(sed -n '/^_a8_recover_brk2()/,/^}/p' "$SIMROOT/drills/52-credential-rotation.sh")
if grep -q 'rm -f.*tunnel-cert.pem.*tunnel-key.pem' "$SIMROOT/lib/secrets.sh" &&
   printf '%s' "$a8_recover" | grep -q 'secrets_push_file.*tunnel-cert.pem' &&
   ! printf '%s' "$a8_recover" | grep -qE '(old|previous|saved|backup).*tunnel'; then
  fail "52-A8 overwrites the pinned leaf in the stash, then 'recovers' by pushing the same unpinned leaf"
else
  pass "52-A8 preserves and restores the exact previously pinned tunnel leaf"
fi

# OBJ_xfer-<sid> is a persistent per-session bucket. Stream existence in /jsz cannot
# distinguish an empty, correctly cleaned bucket from one containing an orphan object.
if grep -q "grep -c OBJ_xfer" "$SIMROOT/drills/96-mid-flight-chaos.sh"; then
  fail "96-A2 treats persistent OBJ_xfer bucket existence as orphan-object presence"
else
  pass "96-A2 inspects live object entries rather than only the backing stream"
fi

# The approved plan requires two restore-address exercises and a repeated-restore
# preservation ladder. Keep these as concrete coverage checks, not prose claims.
if grep -q -- '--raft-addr 127\.0\.0\.1:7400' "$SIMROOT/drills/51-full-dr.sh" &&
   grep -q -- '\.pre-restore\.1\.bak' "$SIMROOT/drills/51-full-dr.sh"; then
  pass "51 exercises restore address override and non-overwriting repeated-restore backups"
else
  fail "51 omits the planned restore-address override and .pre-restore.N.bak ladder"
fi

# The post-partition recovery probe is explicitly named 'via brk2'; require it to
# actually override the NATS URL to the formerly partitioned broker.
via_brk2=$(sed -n '/^_c2_biz_via_brk2()/,/^}/p' "$SIMROOT/drills/97-soak-cycles.sh")
case "$via_brk2" in
  *'nats://brk2:4222'*) pass "97 recovery business probe actually routes through brk2" ;;
  *) fail "97 recovery business probe named via-brk2 still uses the default brk1 path" ;;
esac

# The soak advertises four self-proven injection types. Type 3 must prove the
# transfer itself reached the product (or record it not-covered); a changed broker
# PID proves only the restart half and permits a GREEN run with no transfer at all.
xfer_started_refs=$(grep -c '_xfer_started' "$SIMROOT/drills/97-soak-cycles.sh" || true)
xfer_terminal_refs=$(grep -c '_xfer_terminal' "$SIMROOT/drills/97-soak-cycles.sh" || true)
if [ "$xfer_started_refs" -gt 1 ] || [ "$xfer_terminal_refs" -gt 1 ]; then
  pass "97 type-3 proves the concurrent transfer reached product history"
else
  fail "97 type-3 proves only the broker restart; its transfer-concurrency half is unobserved"
fi

# Consensus after heal means three successful observations which all report one
# non-empty leader. Counting only distinct non-empty values passes with two errors.
if printf '%s' "$via_brk2" >/dev/null &&
   grep -A8 '^_one_leader()' "$SIMROOT/drills/97-soak-cycles.sh" | grep -qE '(length|wc -l).*(=|eq).*3'; then
  pass "97 requires all three brokers to report the same leader after heal"
else
  fail "97 one-leader oracle passes when only one broker returns a leader report"
fi

# A failed /proc read must not become a healthy all-zero sample. Mock dexec so the
# helper reads the local /proc for a guaranteed-nonexistent PID.
log() { :; }
warn() { :; }
err() { :; }
dexec() {
  shift
  [ "${1:-}" = -- ] && shift
  "$@"
}
. "$SIMROOT/drills/lib/leak.sh"
missing_sample=$(leak_sample local 99999999 2>/dev/null)
if [ "$missing_sample" = "0 0 0" ]; then
  fail "leak_sample reports a failed /proc read as a successful all-zero sample"
else
  pass "leak_sample fails closed when the target process cannot be sampled"
fi

# The vault contract says only `nuke` reaps it. remote.sh uses rsync --delete, so
# server-generated backups need the same protect filter as server-generated secrets.
if grep -q -- "--filter='P /backups/\*\*\*'" "$SIMROOT/remote.sh" 2>/dev/null; then
  pass "remote rsync preserves the server-side backup vault"
else
  fail "remote rsync --delete can reap backups/ even though vault.sh says only nuke does"
fi

# INSTANCE reaches two rm -rf targets in cmd_nuke. Reject traversal and separators
# before any verb dispatch; quoting does not neutralize '..' path components.
if grep -qE 'INSTANCE.*(invalid|must match|\[A-Za-z0-9|\[a-zA-Z0-9)' "$SIMROOT/simcluster"; then
  pass "INSTANCE is validated before destructive path construction"
else
  fail "unvalidated INSTANCE is appended to rm -rf vault/stash paths (path traversal)"
fi

if [ "$FAIL" -ne 0 ]; then
  printf 's7-s9-external-review: %s failure(s)\n' "$FAIL" >&2
  exit 1
fi
printf 's7-s9-external-review: all checks passed\n'
