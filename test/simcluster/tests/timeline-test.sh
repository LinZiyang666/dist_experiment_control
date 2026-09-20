#!/bin/sh
# timeline-test.sh — hermetic control for the timeline sidecar (lib/log.sh _tl + timeline.sh). POSIX sh,
# no docker, sub-second except one 1 s poll.
#
#   1. With SIM_TIMELINE_FILE set, log/ok/warn/err and poll_until (met AND timeout) each append one
#      `<epoch>\t<kind>\t<text>` row; poll rows carry "met <el>s/<budget>s" / "TIMEOUT <el>s/<budget>s".
#   2. A poll inside a SUB-SHELL (the assert_ok-captured-predicate shape) still lands in the file.
#   3. With the variable unset, NOTHING is written and the console bytes are identical either way — the
#      byte-identity of the verdict line itself is pinned by tests/verdict-contract-test.sh; here the
#      whole captured console of a synthetic run is compared.
#   4. timeline.sh reports the largest gap with the line that preceded it, and the poll sum.
# origin: simcluster-speed plan §5.2 0b (mutation T-1: a timestamp written to the console → 3 fails).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
T="${TMPDIR:-/tmp}/timeline-test.$$"
mkdir -p "$T"
trap 'rm -rf "$T"' EXIT INT TERM
FAIL=0
ok()   { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; FAIL=1; }

# A synthetic run that exercises every writer. Executed twice: with and without the sidecar.
cat > "$T/run.sh" <<EOF
. "$SIM_ROOT/lib/log.sh"
log "one"; ok "two"; warn "three"; err "four"
poll_until 5 1 "immediate" -- true
( poll_until 5 1 "nested in a subshell" -- true )
poll_until 1 1 "never" -- false
EOF

SIM_TIMELINE_FILE="$T/tl.tsv" sh "$T/run.sh" > "$T/with.out" 2>&1; rc_with=$?
unset SIM_TIMELINE_FILE
sh "$T/run.sh" > "$T/without.out" 2>&1; rc_without=$?

# 3. console byte-identical, sidecar absent when unset
if cmp -s "$T/with.out" "$T/without.out"; then ok "console output byte-identical with and without the sidecar"; else fail "console differs when the sidecar is on:"; diff "$T/with.out" "$T/without.out" >&2; fi
[ "$rc_with" = "$rc_without" ] || fail "exit code differs with the sidecar ($rc_with vs $rc_without)"
# (the old `[ ! -e "$T/tl.tsv.unset" ]` line here tested a filename nothing ever writes — internal review
# round 1 R3-9; the unset→nothing-written property is the 8-row count below after BOTH runs, plus this:)
n_before=$(ls "$T" | wc -l)
sh "$T/run.sh" >/dev/null 2>&1 || true
[ "$(ls "$T" | wc -l)" -eq "$n_before" ] || fail "a run with SIM_TIMELINE_FILE unset created a file under the scratch dir"

# 4. the sidecar is BEST-EFFORT under the caller's own `set -euo pipefail` (simcluster:7): an unwritable
# path must neither print the shell's "cannot create" diagnostic nor abort the caller. The `|| true` on
# _tl's compound redirection is the whole of that guarantee, and nothing else pinned it (internal review
# round 1 R3-3: deleting it left T-1 and this test green while `log` under set -e stopped the script).
cat > "$T/strict.sh" <<EOF
set -euo pipefail
. "$SIM_ROOT/lib/log.sh"
log "before"
echo REACHED
EOF
strict_out=$(SIM_TIMELINE_FILE=/proc/self/no/such/dir/tl.tsv bash "$T/strict.sh" 2>&1); strict_rc=$?
if [ "$strict_rc" = 0 ] && printf '%s' "$strict_out" | grep -q '^REACHED$'; then ok "an unwritable sidecar does not abort a set -euo pipefail caller (log returned, REACHED printed)"; else fail "unwritable sidecar aborted the caller (rc=$strict_rc): $strict_out"; fi
printf '%s' "$strict_out" | grep -qi 'cannot create\|No such file' && fail "the shell's redirection diagnostic leaked to the console: $strict_out"

# 1./2. rows
[ -f "$T/tl.tsv" ] || { fail "no sidecar written"; echo "timeline-test: FAIL" >&2; exit 1; }
rows=$(wc -l < "$T/tl.tsv")
if [ "$rows" -eq 8 ]; then ok "8 rows: 4 log kinds + 3 polls (+1 console warn for the timeout)"; else fail "expected 8 rows, got $rows:"; cat "$T/tl.tsv" >&2; fi
grep -q "	log	one$" "$T/tl.tsv" || fail "log row missing"
grep -q "	ok	two$" "$T/tl.tsv" || fail "ok row missing"
grep -q "	warn	three$" "$T/tl.tsv" || fail "warn row missing"
grep -q "	err	four$" "$T/tl.tsv" || fail "err row missing"
grep -q "	poll	met 0s/5s immediate$" "$T/tl.tsv" || fail "met poll row missing/wrong"
grep -q "	poll	met 0s/5s nested in a subshell$" "$T/tl.tsv" || fail "SUB-SHELL poll row missing — the sidecar does not reach captured predicates"
grep -q "	poll	TIMEOUT 1s/1s never$" "$T/tl.tsv" || fail "timeout poll row missing/wrong"
awk -F'\t' 'NF!=3 || $1 !~ /^[0-9]+$/ { bad=1 } END { exit bad }' "$T/tl.tsv" || fail "a row is not epoch<TAB>kind<TAB>text"

# 4. reader: build a timeline with one big gap and check the report names the line before it
printf '1000\tlog\tup\n1005\tok\tinit\n1300\tpoll\tmet 295s/300s the long one\n1302\tok\tdone\n' > "$T/syn.tsv"
out=$(sh "$SIM_ROOT/timeline.sh" "$T/syn.tsv" 1)
printf '%s\n' "$out" | grep -q '^295 .*\[ok\] init$' && ok "timeline.sh names the line before the largest gap" || { fail "timeline.sh report wrong: $out"; }
printf '%s\n' "$out" | grep -q 'wall=302s' || fail "wall not reported: $out"
printf '%s\n' "$out" | grep -q 'poll_sum=295s' || fail "poll sum not reported: $out"
if sh "$SIM_ROOT/timeline.sh" "$T/nope.tsv" >/dev/null 2>&1; then fail "missing file accepted"; else ok "missing file refused"; fi

[ "$FAIL" = 0 ] && echo "timeline-test: PASS" || { echo "timeline-test: FAIL" >&2; exit 1; }
