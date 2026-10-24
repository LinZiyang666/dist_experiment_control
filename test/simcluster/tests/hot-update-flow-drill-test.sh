#!/bin/sh
# origin: hot-update S5 plan §8 T48 / §9. Hermetic tests exercise the real
# shell guards and native verdict harness. Collection boundaries are doubles;
# these controls never claim real product continuity or a deploy receipt.
set -u
SIMDIR=${SIMDIR:-$(cd "$(dirname "$0")/.." && pwd)}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT HUP INT TERM
FLOW_EV="$TMP/evidence"; mkdir -m 700 "$FLOW_EV"
FLOW_NONCE=abcdef0123456789abcdef0123456789
PASS=0; FAIL=0; SELECT=${1:-all}
check() {
    _check_desc=$1; shift
    if "$@"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); printf 'FAIL: %s\n' "$_check_desc" >&2; fi
}
reject() {
    _reject_desc=$1; shift
    if "$@" >/dev/null 2>&1; then FAIL=$((FAIL+1)); printf 'FAIL: accepted %s\n' "$_reject_desc" >&2; else PASS=$((PASS+1)); fi
}
extract() {
    _ext_file=$1; shift
    for _ext_fn in "$@"; do
        awk -v fn="$_ext_fn" '$0 ~ "^"fn"\\(\\)[ \t]*\\{" {infn=1} infn {print; if ($0 ~ /^}$/) infn=0}' "$_ext_file"
    done
}
. "$SIMDIR/drills/lib/hot_update_flow_faults.sh"
. "$SIMDIR/drills/lib/legacy_flow_supply.sh"
eval "$(extract "$SIMDIR/drills/75-hot-update-flows.sh" _flow_evidence_init _flow_upgrade_start _flow_upgrade_wait _flow_broker_ready _flow_allow_brokers)"

# origin: hot-update S5 plan §9.1 / DRILL-RUN. A valid N3 leader change
# must pass, while unreachable, lagging or partial views remain setup RED.
if [ "$SELECT" = all ] || [ "$SELECT" = C ]; then
    cat > "$TMP/broker-ready" <<'BROKERREADY'
#!/bin/sh
cat "$BROKER_READY_DOCUMENT"
BROKERREADY
    chmod +x "$TMP/broker-ready"
    BROKER_READY_DOCUMENT="$TMP/ready.json"; export BROKER_READY_DOCUMENT
    upgradecfg_allow_broker() { printf '%s\n' "$1" >> "$TMP/allowed-brokers"; }
    ARM=C
    _flow_allow_brokers
    check 'N3 deploys the artifact allowlist on every voter' python3 - "$TMP/allowed-brokers" <<'ALLOWLIST'
import pathlib, sys
assert pathlib.Path(sys.argv[1]).read_text().splitlines() == ['brk1', 'brk2', 'brk3']
ALLOWLIST
    SIM="$TMP/broker-ready"; ARM=C
    jq -n '{health:"HEALTHY_HA", partial:false, errors:[], leader_id:"brk3", nodes:["brk1","brk2","brk3"]|map({node_id:.,phase:"VOTER",reachable:true,applied_lag:0,inconsistent:false})}' > "$BROKER_READY_DOCUMENT"
    check 'N3 accepts healthy successor leader with original brk1 voter' _flow_broker_ready
    for expr in '.leader_id=""' '.health="DEGRADED"' '.partial=true' '.errors=["unreachable"]' '.nodes[0].reachable=false' '.nodes[0].applied_lag=1' '.nodes[0].inconsistent=true' '.nodes[0].node_id="foreign"' '.nodes=.nodes[:2]'; do
        mutate_ready_original="$TMP/ready-original.json"
        cp "$BROKER_READY_DOCUMENT" "$mutate_ready_original"
        jq "$expr" "$mutate_ready_original" > "$BROKER_READY_DOCUMENT"
        reject "N3 broker readiness rejects $expr" _flow_broker_ready
        cp "$mutate_ready_original" "$BROKER_READY_DOCUMENT"
    done
fi

# A missing deploy collector must retain native SETUP-RED before provisioning
# or installing product cleanup. No collection double supplies continuity here.
(
    INSTANCE=drill-flow-hermetic
    SIM_EVIDENCE_DIR="$TMP/unavailable"
    . "$SIMDIR/lib/log.sh"
    . "$SIMDIR/lib/assert.sh"
    ARM=A
    drill_begin '75 missing live collector control'
    _flow_evidence_init
) > "$TMP/unavailable.out" 2>&1
_unavailable_rc=$?
check 'missing live collector is native setup rc2' test "$_unavailable_rc" = 2
check 'missing live collector creates no flow evidence directory' python3 -I -B - "$TMP/unavailable" <<'PYUNAVAILABLE'
import pathlib, sys
assert not list(pathlib.Path(sys.argv[1]).glob('hot-update-flow.*'))
PYUNAVAILABLE
check 'missing live collector has native SETUP-RED receipt' grep -q '^DRILL-VERDICT verdict=SETUP-RED rc=2 ' "$TMP/unavailable.out"

# Scan actual source, not a duplicate manifest fixture. Exact dictionaries make
# a missing arm, duplicated header or altered budget independently observable.
check 'five frozen headers six arms early F and native wait' python3 - "$SIMDIR" <<'PYSTATIC'
import pathlib, re, runpy, sys
root = pathlib.Path(sys.argv[1]); source=(root/'drills/75-hot-update-flows.sh').read_text()
headers = dict(arms='A C R B W L', fixture='A=N1 C=N3-live R=N1 B=N1 W=N1 L=N1',
               grows='A=0 C=2 R=0 B=0 W=0 L=0', worst='A=420 C=600 R=420 B=540 W=660 L=420',
               forgoes='A=- C=- R=- B=- W=- L=-')
for key, expected in headers.items():
    values=re.findall(r'^# '+key+r': (.*)$', source, re.M)
    assert values==[expected], 'manifest '+key
assert source.count('case "${ARM:?}" in')==1, 'one top-level arm dispatch'
body=source.split('case "${ARM:?}" in')[1]
for arm in 'A C R B W L'.split(): assert re.search(r'^    '+arm+r'\)', body, re.M), 'arm '+arm
legacy=(root/'drills/39-hot-update-legacy-n1.sh').read_text()
# origin: hot-update S6 plan §10.2; BR is additive, the S5 arms remain exact.
# origin: S8 internal review s8-hermetic F6. Reconcile all five current
# headers against the independent frozen union, including every old arm.
expected = runpy.run_path(str(root/'tests/testdata/upgrade-legacy-manifest.py'))['MANIFEST39']
for key, value in expected.items():
    assert re.findall(r'^# '+key+r': (.*)$', legacy, re.M)==[value], 'legacy manifest '+key
f=legacy.split('case "${ARM:?}" in')[1].split('P)')[0]
assert f.index('F)')<f.index('_flow_legacy_arm'), 'F dispatch before exec-only setup'
assert '_h0_prepare' not in f, 'F must not share exec-only setup'
prepare=source.split('_flow_prepare() {')[1].split('\n}\n')[0]
assert prepare.index('legacy_flow_broker_install')<prepare.index('session "$SID"'), 'old broker before session'
assert prepare.index('legacy_flow_agent_install')<prepare.index('flow_fixture start'), 'old exporter before business'
assert 'FLOW_UPGRADE_RC=$?' in source and 'FLOW_UPGRADE_JOB=$!' in source, 'native wait identity'
assert 'flow_fault_valid "$FLOW_EV/legacy-fault.json"' in source, 'fault prior before legacy assertion'
assert 'flow_open_partial_valid "$FLOW_EV/open-partial.json"' in source, 'OPEN and cipher prior'
assert 'flow_watchdog_valid "$FLOW_EV/watchdog.json"' in source, 'watchdog prior'
assert 'flow_carrier_rebuild' in body.split('C)')[0], 'A same-cohort carrier rebuild'
# origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
c=body.split('    C)')[1].split('    R)')[0]
assert c.index('_flow_prepare capacity-32') < c.index('_flow_upgrade'), 'C phases before real same-PID cut'
assert prepare.index('flow_fixture prepare') < prepare.index('flow_fixture saturation') < prepare.index('flow_fixture start'), 'C FIN/recovery before normal traffic'
assert 'assert_ok "32 active reservations typed capacity refusals natural FIN and recovery"' in prepare, 'capacity is a product assertion'
assert c.count('grow_to_3') == 1, 'one live N3 topology'
assert 'flow_fixture checkpoint active-progress' in body, 'B current-state progress'
assert 'flow_fixture arm-revoke prepare final-cut resume' in body, 'R observed race windows'
# origin: hot-update S5 plan §9.1 / DRILL-RUN. Evidence collection cannot
# serialize behind the OPEN deadline, or prolong the SYN fault past the cut.
a=body.split('C)')[0]
assert a.index('_flow_upgrade_start') < a.index('flow_fixture checkpoint open-partial'), 'upgrade runs during OPEN census'
assert a.index('_flow_upgrade_start') < a.index('flow_open_syn_on'), 'stage artifact before original connect budget'
assert '_flow_revoke_window final-cut flow_open_syn_off' in a, 'SYN fault ends at observed cut'
PYSTATIC
mutate() {
    _mut_file=$1; _mut_expr=$2; shift 2
    cp "$_mut_file" "$TMP/original.json"
    jq "$_mut_expr" "$TMP/original.json" > "$_mut_file"
    if [ "${1:-}" = rc2 ]; then
        shift
        "$@" > "$TMP/mutated-classifier.out" 2>&1
        _mut_rc=$?
        check "invalid observation stays rc2: $_mut_expr" test "$_mut_rc" = 2
    else
        reject "$_mut_expr" "$@"
    fi
    cp "$TMP/original.json" "$_mut_file"
}
if [ "$SELECT" = all ] || [ "$SELECT" = A ]; then
    cat > "$FLOW_EV/open-partial.json" <<'JSON'
{"nonce":"abcdef0123456789abcdef0123456789","open":{"original_fd":true,"phase":"EINPROGRESS","syn_block_observed":true,"target_accepts":0},"ss":{"attempt_len":512,"n":17,"cipher_suffix_bytes":495,"seal_calls":2,"application_redials":0}}
JSON
    check 'true kernel OPEN/partial priors' flow_open_partial_valid "$FLOW_EV/open-partial.json"
    for expr in '.open.original_fd=false' '.open.phase="connected"' '.open.syn_block_observed=false' '.open.target_accepts=1' '.ss.n=0' '.ss.n=512' '.ss.cipher_suffix_bytes=0' '.ss.seal_calls=0' '.ss.seal_calls="1"' '.ss.application_redials=1' 'del(.open)' '.nonce="foreign"'; do
        mutate "$FLOW_EV/open-partial.json" "$expr" flow_open_partial_valid "$FLOW_EV/open-partial.json"
    done
fi
if [ "$SELECT" = all ] || [ "$SELECT" = W ]; then
    cat > "$FLOW_EV/watchdog.json" <<'JSON'
{"nonce":"abcdef0123456789abcdef0123456789","agent_uid":1000,"permission_denied":true,"nats_block_ms":1500,"mode_restored":true,"commit_write_failed":true,"mark_ready":true,"pending":true,"watchdog_armed":true,"full_register_count":1,"reaper_running":true,"allocation_unchanged":true,"fresh_ss_grant":true,"ss_authority_available":true,"elapsed_s":121,"automatic":true,"second_cut_current":true,"generation_before":1,"generation_after":2,"final_marker":"rolled_back","original_running_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","final_running_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
JSON
    check 'real ready pending watchdog evidence' flow_watchdog_valid "$FLOW_EV/watchdog.json"
    for expr in '.agent_uid=0' '.permission_denied=false' '.nats_block_ms=2001' '.mode_restored=false' '.commit_write_failed=false' '.mark_ready=false' '.pending=false' '.watchdog_armed=false' '.full_register_count=2' '.reaper_running=false' '.allocation_unchanged=false' '.fresh_ss_grant=false' '.ss_authority_available=false' '.elapsed_s=119' '.elapsed_s="121"' '.agent_uid="1000"' 'del(.generation_before)' '.automatic=false' '.second_cut_current=false' '.generation_after=1' '.final_marker="committed"' '.final_running_sha="different"'; do
        mutate "$FLOW_EV/watchdog.json" "$expr" flow_watchdog_valid "$FLOW_EV/watchdog.json"
    done
fi
if [ "$SELECT" = all ] || [ "$SELECT" = L ]; then
    printf '%s\n' 'Error: upgrade lab/agt1 failed: deferred: legacy_session_busy (1 legacy listener) (agent_rejected:upgrade_deferred)' > "$FLOW_EV/refusal.out"
    check 'exact legacy native refusal' flow_legacy_refused 75 "$FLOW_EV/refusal.out"
    printf '%s\n' 'Error: deferred: legacy_session_busy (8 legacy sessions; flow ownership is legacy) (agent_rejected:upgrade_deferred)' > "$FLOW_EV/refusal.out"
    check 'one legacy reason retains semicolon-separated owner details' flow_legacy_refused 75 "$FLOW_EV/refusal.out"
    reject 'rc0 refusal' flow_legacy_refused 0 "$FLOW_EV/refusal.out"
    reject 'other refusal class' flow_legacy_refused 64 "$FLOW_EV/refusal.out"
    printf '%s\n' 'Error: upgrade lab/agt1 failed: deferred: other_busy (agent_rejected:upgrade_deferred)' > "$FLOW_EV/refusal.out"
    reject 'other busy' flow_legacy_refused 75 "$FLOW_EV/refusal.out"
    for text in 'deferred: legacy_session_busy_suffix (agent_rejected:upgrade_deferred)' 'deferred: flow_busy; legacy_session_busy (1 legacy listener) (agent_rejected:upgrade_deferred)' 'deferred: ss_busy; legacy_session_busy (agent_rejected:upgrade_deferred)' 'agent_rejected:install_failed diagnostic legacy_session_busy' 'deferred: ss_state_unrepresentable (diagnostic legacy_session_busy) (agent_rejected:upgrade_deferred)'; do
        printf '%s\n' "$text" > "$FLOW_EV/refusal.out"
        reject 'diagnostic or stale reason cannot certify legacy refusal' flow_legacy_refused 75 "$FLOW_EV/refusal.out"
    done
    # Transport controls are mocked, but the production supply predicate runs.
    sim_flow_legacy_source_ok() { return 0; }
    SIM_VENDOR="$TMP/vendor"; mkdir "$SIM_VENDOR"; printf old > "$SIM_VENDOR/tether-flow-legacy"
    SHA=$(sha256sum "$SIM_VENDOR/tether-flow-legacy" | awk '{print $1}')
    printf '{"tag":"v0.6.0"}\n' > "$SIM_VENDOR/tether-flow-legacy-source.json"
    UNIT='path=/usr/local/bin/tether-prev ;'; RUNNING_SHA=$SHA
    dexec() {
        shift 2
        if [ "$1" = systemctl ]; then
            if [ "$5" = MainPID ]; then printf '41\n'; else printf '%s\n' "$UNIT"; fi
        else printf '%s  /proc/41/exe\n' "$RUNNING_SHA"; fi
    }
    check 'actual old broker unit and running SHA' legacy_flow_broker_verify
    UNIT='path=/usr/local/bin/tether ;'
    reject 'old ctl presented as old broker' legacy_flow_broker_verify
    UNIT='path=/usr/local/bin/tether-prev ;'; RUNNING_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    reject 'old unit path with current broker process' legacy_flow_broker_verify
    jq -n --arg sha "$SHA" '{broker_running_sha:$sha,alpn:"",raw_prefix_verified:true,flow_header_seen:false,before_first_business:true,original_connections:4,samples_each_direction:20}' > "$FLOW_EV/legacy-transport.json"
    check 'actual old broker legacy raw negotiation' legacy_flow_raw_valid
    for expr in '.alpn="tether-flow/1"' '.raw_prefix_verified=false' '.flow_header_seen=true' '.before_first_business=false' '.original_connections=0' '.samples_each_direction=0' '.broker_running_sha="current"'; do
        mutate "$FLOW_EV/legacy-transport.json" "$expr" legacy_flow_raw_valid
    done
fi
if [ "$SELECT" = all ] || [ "$SELECT" = F ]; then
    cat > "$FLOW_EV/legacy-fault.json" <<'JSON'
{"nonce":"abcdef0123456789abcdef0123456789","valid_fault":true,"observed":true,"event_seq":1,"before":{"pid":41,"start_ticks":123,"running_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"after":{"pid":41,"start_ticks":123,"running_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"marker":{"state":"committed","boot_count":1}}
JSON
    jq '{running_sha:.before.running_sha,source:{tag:"v0.6.0",commit:"a3431a12be3cf947cc797c28fea93382fa68472d"}}' "$FLOW_EV/legacy-fault.json" > "$FLOW_EV/legacy-agent.json"
    printf '%s\n' '{"original_socket":true,"application_redials":0,"failure":"early_eof","samples_before":20}' > "$FLOW_EV/legacy-loss.json"
    ORACLE_RC=1
    FLOW_LEGACY_EXPECTED_LOSS=early_eof
    flow_fixture() { return "$ORACLE_RC"; }
    native() {
        _nat_expected=$1; _nat_rc=$2
        (
            INSTANCE=drill-flow-hermetic
            SIM_EVIDENCE_DIR="$TMP/native"
            . "$SIMDIR/lib/log.sh"
            . "$SIMDIR/lib/assert.sh"
            drill_begin '39.F hermetic classifier'
            flow_legacy_assertion
            drill_end
        ) > "$TMP/verdict.out" 2>&1
        _nat_actual=$?
        [ "$_nat_actual" = "$_nat_rc" ] && grep -q "^DRILL-VERDICT verdict=$_nat_expected rc=$_nat_rc " "$TMP/verdict.out"
    }
    check 'observed bare exec fault' flow_fault_valid "$FLOW_EV/legacy-fault.json"
    check 'native legacy failure becomes PRODUCT-RED rc3' native PRODUCT-RED 3
    FLOW_LEGACY_EXPECTED_LOSS=
    check 'unreviewed legacy signature is native SETUP-RED' native SETUP-RED 2
    FLOW_LEGACY_EXPECTED_LOSS=early_eof_or_reset
    check 'broad legacy loss pin is native SETUP-RED' native SETUP-RED 2
    FLOW_LEGACY_EXPECTED_LOSS=reset
    check 'reviewed reset cannot match observed EOF' native ASSERT-FAIL 1
    jq '.failure="reset"' "$FLOW_EV/legacy-loss.json" > "$TMP/reset-loss.json"
    cp "$FLOW_EV/legacy-loss.json" "$TMP/eof-loss.json"
    cp "$TMP/reset-loss.json" "$FLOW_EV/legacy-loss.json"
    check 'reviewed reset matches only actual reset' native PRODUCT-RED 3
    unset FLOW_LEGACY_EXPECTED_LOSS
    check 'receipt-pinned default matches observed reset' native PRODUCT-RED 3
    FLOW_LEGACY_EXPECTED_LOSS=early_eof
    check 'reviewed EOF cannot match observed reset' native ASSERT-FAIL 1
    cp "$TMP/eof-loss.json" "$FLOW_EV/legacy-loss.json"
    FLOW_LEGACY_EXPECTED_LOSS=early_eof
    check 'separate oracle native rc1 saved' test "$(cat "$FLOW_EV/continuity.rc")" = 1
    for expr in '.valid_fault=false' '.observed=false' '.event_seq=0' '.after.pid=42' '.after.start_ticks=124' '.after.running_sha=.before.running_sha' '.marker.state="pending"' '.marker.boot_count=0' '.marker.boot_count="1"' 'del(.before)'; do
        mutate "$FLOW_EV/legacy-fault.json" "$expr" flow_fault_valid "$FLOW_EV/legacy-fault.json"
    done
    mutate "$FLOW_EV/legacy-agent.json" '.source.tag="current"' rc2 flow_legacy_exporter
    mutate "$FLOW_EV/legacy-loss.json" '.failure="busy_refusal"' rc2 flow_legacy_exporter
    jq '.valid_fault=false' "$FLOW_EV/legacy-fault.json" > "$TMP/bad.json"
    mv "$FLOW_EV/legacy-fault.json" "$TMP/good-fault.json"; cp "$TMP/bad.json" "$FLOW_EV/legacy-fault.json"
    check 'missing fault prior cannot mint PRODUCT-RED' native ASSERT-FAIL 1
    mv "$TMP/good-fault.json" "$FLOW_EV/legacy-fault.json"
    ORACLE_RC=0; check 'unexpectedly fixed native oracle is ASSERT-FAIL' native ASSERT-FAIL 1
    ORACLE_RC=2; check 'invalid collection is ASSERT-FAIL not product loss' native ASSERT-FAIL 1
fi
if [ "$SELECT" = all ] || [ "$SELECT" = B ]; then
    . "$SIMDIR/drills/lib/artifact.sh"
    for side in a b; do
        printf '#!/bin/sh\nprintf "tether %s (proto v1)\\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\\nimage-abi v1\\n"\n' "$side" > "$TMP/reader-$side"
        chmod +x "$TMP/reader-$side"
    done
    check 'distinct SHA-bound flow and SS readers' artifact_flow_readers_ok "$TMP/reader-a" "$TMP/reader-b" "$TMP/readers.json"
    reject 'two names for same artifact' artifact_flow_readers_ok "$TMP/reader-a" "$TMP/reader-a" "$TMP/readers.json"
    cp "$TMP/reader-b" "$TMP/reader-good"
    sed 's/ flow-v1 ss-v1//' "$TMP/reader-good" > "$TMP/reader-b"
    reject 'same ABI without retained domains' artifact_flow_readers_ok "$TMP/reader-a" "$TMP/reader-b" "$TMP/readers.json"
    cp "$TMP/reader-good" "$TMP/reader-b"
    printf 'printf "image-abi v1\\n"\n' >> "$TMP/reader-b"
    reject 'duplicate ABI declaration' artifact_flow_readers_ok "$TMP/reader-a" "$TMP/reader-b" "$TMP/readers.json"
    SIM_VENDOR="$TMP/receipt-vendor"; mkdir -p "$SIM_VENDOR"
    printf '%s\n' '{"source_head":"head","dirty_patch_sha":"digest","build_flags":"flags","tether_next_sha256":"artifact"}' > "$SIM_VENDOR/tether-source.json"
    printf '%s\n' '{"running_sha":"running"}' > "$FLOW_EV/identity-final.json"
    _receipt_start=$(python3 -c 'import time; print(time.monotonic())')
    check 'receipt preserves native loss and historical role' artifact_flow_receipt F 1 PRODUCT-RED "$_receipt_start" '39-hot-update-legacy-n1.F'
    check 'receipt schema source native oracle and COMPLETE' jq -e '.source_head=="head" and .dirty_patch_sha=="digest" and .build_flags=="flags" and .artifact_sha=="artifact" and .running_sha=="running" and .native_rc==3 and .continuity_oracle_rc==1 and .verdict=="PRODUCT-RED" and .command=="39-hot-update-legacy-n1.F" and .status=="COMPLETE" and (.oracle_inputs_sha|length)==64' "$FLOW_EV/receipt.json"
    check 'receipt preserves inherited INCOMPLETE rc4 separately from busy75' artifact_flow_receipt B 0 INCOMPLETE "$_receipt_start"
    check 'incomplete receipt uses actual harness native class' jq -e '.native_rc==4 and .continuity_oracle_rc==0 and .verdict=="INCOMPLETE"' "$FLOW_EV/receipt.json"
    _receipt_digest=$(jq -r .oracle_inputs_sha "$FLOW_EV/receipt.json")
    printf 'additional original ledger input\n' > "$FLOW_EV/receipt-ledger.raw"
    check 'receipt incorporates raw oracle contents' artifact_flow_receipt F 1 PRODUCT-RED "$_receipt_start" '39-hot-update-legacy-n1.F'
    check 'changed raw input changes receipt digest' test "$(jq -r .oracle_inputs_sha "$FLOW_EV/receipt.json")" != "$_receipt_digest"
    _receipt_digest=$(jq -r .oracle_inputs_sha "$FLOW_EV/receipt.json")
    printf 'changed bytes in same ledger input\n' > "$FLOW_EV/receipt-ledger.raw"
    check 'receipt incorporates edited raw bytes' artifact_flow_receipt F 1 PRODUCT-RED "$_receipt_start" '39-hot-update-legacy-n1.F'
    check 'same input path edited bytes change receipt digest' test "$(jq -r .oracle_inputs_sha "$FLOW_EV/receipt.json")" != "$_receipt_digest"
fi
# Failed actions and background upgrades keep their native status even when a
# following diagnostic/cleanup succeeds. Parent-owned PID remains reapable.
if [ "$SELECT" = all ] || [ "$SELECT" = B ] || [ "$SELECT" = C ] || [ "$SELECT" = R ]; then
    fails() { return 17; }
    flow_step_run fails; saved=$?
    check 'step preserves native rc17' test "$saved" = 17
    check 'step saves rc17 before cleanup' test "$(cat "$FLOW_EV/steps/$FLOW_STEP_COUNT.rc")" = 17
    # origin: S8 internal review r9d-nonvacuity 75.R / 75.B.
    # Isolated B/R controls must select a real arm before the helper reads ARM.
    SIM="$TMP/sim-command"; ARM=R
    printf '#!/bin/sh\nexit 19\n' > "$SIM"; chmod +x "$SIM"
    # Only assertion boundary is replaced: observe the real helper's saved wait
    # rc separately, without letting the intentional failure abort this test.
    _flow_upgrade_start one url sha
    check 'parent captures original child PID' test -n "$FLOW_UPGRADE_JOB"
    wait "$FLOW_UPGRADE_JOB"; saved=$?
    check 'upgrade native rc19' test "$saved" = 19
    check 'upgrade stores native rc19' test "$(cat "$FLOW_EV/upgrade-one.rc")" = 19
    FLOW_UPGRADE_JOB=
fi
if [ "$SELECT" = all ] || [ "$SELECT" = fixture ]; then
    check 'live fixture process boundary and socket controls' python3 -I -B "$SIMDIR/tests/flow-fixture-test.py"
fi
printf 'flow-drill[%s]: %s controls passed, %s failed\n' "$SELECT" "$PASS" "$FAIL"
[ "$FAIL" = 0 ]
