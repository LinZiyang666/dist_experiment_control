#!/bin/sh
# 61-transfer-edges.sh — S1 GREEN: push/pull deploy-sensitive edges on the real stack (N=1 + agt1 + ctl).
# The transfer PATH-VALIDATION state machine is hermetic-dense; the ONLY deploy claim here is the real
# daemon loading a real agent.yaml from the real on-disk path across a real RESTART, enforcing the
# allow_roots tri-state on a REAL disk, with the tier boundary set by the REAL nats-server max_payload.
# See docs/reviews/s1-plan.md §3.2. sha_mismatch / path_race (injection-class) stay hermetic (§0.3).
#
# FALSE-GREEN GUARDS: (a) "everything refuses" could be a dead agent → Arm A open baseline + Arm D
# in-narrow positive control gate every refusal; (b) not_a_regular_file could pass on a missing path →
# seed a REAL ln -s + pin the exact code; (c) tier=b could be the 8 MiB static path → the promotion file
# is EXACTLY 1 MiB (< 8 MiB) so tier=b can only come from the real max_payload/2 clamp; (d) a permission
# wall could masquerade as a policy code → every policy refusal carries a `! io_error` negative guard.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
URL="nats://brk1:4222"

CTL() { "$SIM" ctl -- "$@"; }

# refuse_clean <desc> <code> <cmd...> : the cmd must FAIL with stderr `code=<code>` and NOT io_error
# (a permission wall must never masquerade as a policy code — §0.4 / plan MF-2).
refuse_clean() {
    _rc_desc=$1; _rc_code=$2; shift 2
    _rc_out=$("$@" 2>&1); _rc_rc=$?
    if [ "$_rc_rc" = 0 ]; then
        _as_fail "$_rc_desc (expected refusal code=$_rc_code, but SUCCEEDED)" "$(printf '%s' "$_rc_out" | tail -3)"
    elif printf '%s' "$_rc_out" | grep -qE "code=$_rc_code" && ! printf '%s' "$_rc_out" | grep -qi io_error; then
        _as_pass "$_rc_desc (refused code=$_rc_code, not io_error)"
    else
        _as_fail "$_rc_desc (want code=$_rc_code without io_error)" "$(printf '%s' "$_rc_out" | tail -3)"
    fi
}

# oracles
_sha_eq()   { "$SIM" exec ctl1 -- sh -c "a=\$(sha256sum < '$1'); b=\$(sha256sum < '$2'); [ \"\$a\" = \"\$b\" ]"; }
_landed()   { "$SIM" exec agt1 -- test -s "$1"; }
# cross-node content oracle (external review MAJOR-2): SHA-256 a ctl-local file vs an AGENT-side file (both
# hashed via `< file` so the filename never enters the digest). Returns 0 iff byte-identical — a no-op or
# wrong-content transfer fails this even when the transfer command exits 0.
_sha_eq_xnode() {  # <ctl_local_path> <agt_node> <agt_path>
    _sx_a=$("$SIM" exec ctl1 -- sh -c "sha256sum < '$1'" 2>/dev/null | awk '{print $1}')
    _sx_b=$("$SIM" exec "$2" -- sh -c "sha256sum < '$3'" 2>/dev/null | awk '{print $1}')
    [ -n "$_sx_a" ] && [ "$_sx_a" = "$_sx_b" ]
}
_push_out() { "$SIM" ctl -- push "$1" "$2" 2>&1; }
# S1-02 guard self-test: a malformed yaml must make agent_provision_yaml RETURN NON-ZERO (proving the
# ONLINE gate catches a failed reprovision, not silently pass on the old daemon's residual-ONLINE row).
_provision_bad_fails() { ! agent_provision_yaml agt1 "$SID" "$URL" badkey 15; }

drill_begin "61-transfer-edges (push/pull allow_roots tri-state + tier boundary on the real stack)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 1 agent + 1 ctl"        "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (N=1)"                       "$SIM" init brk1
assert_ok "session $SID + ctl login"             "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                       "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
# seed ctl-local source files, sim-owned (deploy value = real User=sim isolation on both ends).
assert_ok "seed ctl-local files (sim-owned): a.bin/u.bin/o.bin" \
    "$SIM" exec ctl1 -- runuser -u sim -- sh -c 'head -c 4096 /dev/urandom > /tmp/a.bin; head -c 512000 /dev/urandom > /tmp/u.bin; head -c 1048576 /dev/urandom > /tmp/o.bin'

# ── G0 — agent.yaml gate self-test (S1-02): a malformed yaml MUST make the fixture fail (not residual-pass)
assert_ok "G0 agent_provision_yaml FAILS on a malformed yaml (guard self-test: catches a bad reprovision)" \
    _provision_bad_fails

# ── Arm A — OPEN baseline (positive control for the whole drill) ──────────────────────────────────────
assert_ok "A0 reprovision agt1 agent.yaml = OPEN (flagless unit, yaml-driven; ONLINE poll)" \
    agent_provision_yaml agt1 "$SID" "$URL" open
assert_ok "A1 push OK (open, ctl→agt1 /home/sim/a.bin)"    "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/a.bin
assert_ok "A1b pushed file landed on agt1"                 _landed /home/sim/a.bin
assert_ok "A2 pull OK (open, agt1→ctl)"                    "$SIM" ctl -- pull agt1:/home/sim/a.bin /tmp/a.back
assert_ok "A2b pull round-trip sha256 matches"            _sha_eq /tmp/a.bin /tmp/a.back

# ── Arm B — mechanism walls (all modes; OPEN) ────────────────────────────────────────────────────────
refuse_clean "B1 push parent-missing (push-only asymmetry)"   "path_parent_missing"   "$SIM" ctl -- push /tmp/a.bin agt1:/no/such/dir/f
refuse_clean "B2 pull not-found (pull-side asymmetry)"        "path_not_found"        "$SIM" ctl -- pull agt1:/no/such/file /tmp/y
assert_ok    "B3-seed real symlink leaf on agt1 disk"        "$SIM" exec agt1 -- runuser -u sim -- ln -sf /etc/hostname /home/sim/evil
refuse_clean "B3 push symlink leaf refused (write-side lstat/O_NOFOLLOW)"  "not_a_regular_file"  "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/evil
refuse_clean "B4 pull symlink leaf refused (read-side lstat/O_NOFOLLOW)"   "not_a_regular_file"  "$SIM" ctl -- pull agt1:/home/sim/evil /tmp/z
assert_ok    "B5-seed regular dst on agt1"                   "$SIM" exec agt1 -- runuser -u sim -- sh -c 'head -c 100 /dev/urandom > /home/sim/dst.bin'
refuse_clean "B5a push dst_exists refused without --force"   "dst_exists"            "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/dst.bin
assert_ok    "B5b push --force succeeds (rc=0)"              "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/dst.bin --force
# B5c (external review MAJOR-2): prove --force actually OVERWROTE — dst.bin was 100 random bytes, a.bin is
# 4096 bytes; a no-op success (or a wrong-content write) leaves a different digest → FAIL. rc=0 alone is not
# proof of overwrite.
assert_ok    "B5c --force dst content == source SHA-256 (real overwrite, not just rc=0)" \
    _sha_eq_xnode /tmp/a.bin agt1 /home/sim/dst.bin
# B6: >2 GiB pull too_large — the deploy-reachable wall is PULL (push >2 GiB is CLI-local/hermetic). 3G
# is > 2147483648; a sparse inode never streams, so the refusal is pre-transfer.
assert_ok    "B6-seed >2 GiB sparse file on agt1 (3G, sim-owned)" \
    "$SIM" exec agt1 -- sh -c 'truncate -s 3G /home/sim/big.sparse && chown sim:sim /home/sim/big.sparse'
refuse_clean "B6 pull >2 GiB refused too_large (no bytes moved)"  "too_large"        "$SIM" ctl -- pull agt1:/home/sim/big.sparse /tmp/big

# ── Arm C — tier boundary (real nats max_payload; tier-B anchored on a landed oracle) ─────────────────
# S1-03: capture push's rc (not the pipe's) so a refused below-boundary push can't pass on the banner
# alone; and anchor on the LANDED file — the `tier=` banner prints BEFORE the transfer runs.
assert_ok "C1 ~500 KiB → tier A (512000 < 523264 boundary; push OK, not just the banner)" \
    sh -c "out=\$(\"$SIM\" ctl -- push /tmp/u.bin agt1:/home/sim/u.bin 2>&1); rc=\$?; printf '%s' \"\$out\" | grep -q 'tier=a' && [ \$rc -eq 0 ]"
assert_ok "C1b tier-A file landed on agt1 (below-boundary transfer completed, not just tier chosen)" \
    _landed /home/sim/u.bin
# C2 THE deploy claim: 1 MiB < 8 MiB static ceiling, so tier=b proves the real max_payload/2-1024 clamp
# fired. The tier= banner prints BEFORE the JS Object-Store Put, so anchor on the landed file + sha.
assert_ok "C2 1 MiB → tier B (real max_payload/2 clamp, not the 8 MiB static path)" \
    sh -c "\"$SIM\" ctl -- push /tmp/o.bin agt1:/home/sim/o.bin 2>&1 | grep -q 'tier=b'"
assert_ok "C2b tier-B file landed on agt1 (banner prints pre-Put → anchor on the result)"  _landed /home/sim/o.bin
assert_ok "C2c pull o.bin back + sha matches (tier-B round-trip completed)" \
    sh -c "\"$SIM\" ctl -- pull agt1:/home/sim/o.bin /tmp/o.back && \"$SIM\" exec ctl1 -- sh -c 'a=\$(sha256sum < /tmp/o.bin); b=\$(sha256sum < /tmp/o.back); [ \"\$a\" = \"\$b\" ]'"

# ── Arm D — NARROW (dir created + chowned sim BEFORE restart; else all-dropped narrow → reject-all) ───
assert_ok "D0 reprovision agt1 = NARROW /srv/data (helper installs the dir sim-owned pre-restart)" \
    agent_provision_yaml agt1 "$SID" "$URL" narrow:/srv/data
# F1 positive control: WITHOUT it every "narrowing works" assert is vacuous (dropped root → reject-all).
assert_ok "D-F1 push INSIDE narrow root OK (positive control)"  "$SIM" ctl -- push /tmp/a.bin agt1:/srv/data/ok.bin
assert_ok "D-F1b in-narrow file landed on agt1"                 _landed /srv/data/ok.bin
# F2/F3 target /home/sim (parent EXISTS but is OUTSIDE /srv/data) → path_outside_roots, NOT parent_missing
# (ValidateForWrite checks parent-exists BEFORE containment: transfer.go:750 then :763).
refuse_clean "D-F2 push OUTSIDE narrow root refused (write path)"  "path_outside_roots"  "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/outside.bin
refuse_clean "D-F3 pull OUTSIDE narrow root refused (independent read path)"  "path_outside_roots"  "$SIM" ctl -- pull agt1:/home/sim/a.bin /tmp/f3.back

# ── Arm E — DISABLED (§0.4 recovery arm: inject → refuse both ways → restore → re-prove) ──────────────
assert_ok "E0 reprovision agt1 = DISABLED (allow_roots: [] → modeDisabled)" \
    agent_provision_yaml agt1 "$SID" "$URL" disabled
refuse_clean "E1 push refused transfer_disabled (write path)"  "transfer_disabled"  "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/e.bin
refuse_clean "E2 pull refused transfer_disabled (read path)"   "transfer_disabled"  "$SIM" ctl -- pull agt1:/home/sim/a.bin /tmp/e.back
# reversibility control: restore OPEN + re-prove a round-trip → the refusal was the config flip, not decay.
assert_ok "E3 restore agt1 = OPEN"                             agent_provision_yaml agt1 "$SID" "$URL" open
# E4 (external review MAJOR-2): a REAL round-trip proves reversibility — push, then pull the file back and
# compare SHA-256 to the source. The old `push && test -s` passed on any non-empty file at the path (even a
# stale/wrong one), so it did not prove the restored transfer actually moved the right bytes both ways.
assert_ok "E4 push works again after restore (reversibility control)" \
    "$SIM" ctl -- push /tmp/a.bin agt1:/home/sim/recover.bin
assert_ok "E4b pull recover.bin back to ctl"                  "$SIM" ctl -- pull agt1:/home/sim/recover.bin /tmp/recover.back
assert_ok "E4c recover round-trip sha256 == source (real round-trip, not just test -s)"  _sha_eq /tmp/a.bin /tmp/recover.back

# ── Arm F — history --kind transfer pairing (disambiguate by path=, both kinds as separate greps) ─────
# A1 wrote a complete for /home/sim/a.bin; D-F2 (agent-reached path_outside_roots) is the failed source.
# transfer_id is NOT rendered (history.go:581-584) → key on the distinct path=. A loose (complete|failed)
# alternation would false-green on the success alone, so require BOTH as separate greps.
assert_ok "F-complete: history --kind transfer has a complete row for /home/sim/a.bin" \
    sh -c "\"$SIM\" ctl -- history --kind transfer -n 50 2>/dev/null | grep -E 'kind=complete' | grep -q 'a.bin'"
# NOTE (verify on server): only AGENT-reached refusals write a start+failed audit pair (plan #10). If the
# server shows no failed row for a prepare-refusal, this is a real observability finding to register in
# the ledger (demote to a signature-guarded note), NOT force-greened.
assert_ok "F-failed: history --kind transfer has a failed row for the outside.bin refusal" \
    sh -c "\"$SIM\" ctl -- history --kind transfer -n 50 2>/dev/null | grep -E 'kind=failed' | grep -q 'outside.bin'"

drill_end
