#!/bin/sh
# origin: hot-update S6 plan §10 / §12.1 P30.
# This owner stages artifacts and drives product commands. P29 alone owns the
# original sockets, sealed SS custody and collector; P26/live suppliers own the
# independent ACK/JS and fault-barrier observations. Missing evidence is RED.
. "$HERE/drills/lib/hot_update_budget.sh"
if [ -r "$HERE/drills/lib/flow_fixture" ]; then . "$HERE/drills/lib/flow_fixture"; fi
BROKER_EV=
BROKER_COMMAND_JOB=
BROKER_WATCH_JOB=
BROKER_WATCH_JOBS=
BROKER_PROBE_JOB=
BROKER_FAULT_JOB=
FLOW_FIXTURE_JOB=

broker_step() {
    BROKER_STEP=$((BROKER_STEP + 1))
    BROKER_STEP_LOG="$BROKER_EV/step-$BROKER_STEP.out"
    "$@" > "$BROKER_STEP_LOG" 2>&1
    BROKER_STEP_RC=$?
    return "$BROKER_STEP_RC"
}

# Invoke in the main shell: assert_setup executes its predicate in a subshell,
# which would lose a started fixture's PID and its original sealed-FD owner.
broker_setup() {
    _bs_description=$1; shift
    broker_step "$@"
    _bs_rc=$?
    if [ "$_bs_rc" != 0 ]; then
        setup_fail "$_bs_description (native rc=$_bs_rc; $BROKER_STEP_LOG)"
    fi
    _AS_PASS=$((_AS_PASS + 1))
}

# origin: hot-update S6 plan §10.2 / T58. Collector INCOMPLETE cannot become
# an assert_ok failure or GREEN. Preserve all five child statuses; the separate
# BR classifier requires its reviewed pin before it can mint known legacy loss.
broker_native() {
    case "$1" in
        0) _AS_PASS=$((_AS_PASS + 1)) ;;
        1) _AS_FAIL=$((_AS_FAIL + 1)) ;;
        2) _AS_SETUP=$((_AS_SETUP + 1)) ;;
        3) _AS_PRODUCT_RED=$((_AS_PRODUCT_RED + 1)) ;;
        4) _AS_NC=$((_AS_NC + 1)); _AS_NC_GUARD=$((_AS_NC_GUARD + 1)) ;;
        *) _AS_SETUP=$((_AS_SETUP + 1)) ;;
    esac
}

# A supplier may supply observations/fixtures, never reload on behalf of this
# driver. Its exact operations are recorded in step logs and the P33 ledger.
# All args are separate shell arguments; no eval or private state serialization.
broker_supply() {
    _bsp_operation=$1; shift
    broker_setup "live supplier $_bsp_operation" "$BROKER_LIVE_SUPPLIER" "$_bsp_operation" \
        --evidence "$BROKER_EV" --flow-root "$FLOW_EV" --nonce "$FLOW_NONCE" \
        --topology "$BROKER_TOPOLOGY" --broker "$BROKER_NODE" "$@"
}

broker_cleanup() {
    # Always wait children in the shell that started them, including setup
    # aborts. TERM runs their own finally paths; teardown is not product repair.
    for _bc_job in $BROKER_WATCH_JOBS "$BROKER_COMMAND_JOB" "$BROKER_WATCH_JOB" "$BROKER_PROBE_JOB" "$BROKER_FAULT_JOB"; do
        [ -n "$_bc_job" ] || continue
        kill -TERM "$_bc_job" 2>/dev/null || true
        wait "$_bc_job" 2>/dev/null || true
    done
    BROKER_COMMAND_JOB=; BROKER_WATCH_JOB=; BROKER_PROBE_JOB=; BROKER_FAULT_JOB=
    BROKER_WATCH_JOBS=
    flow_fixture cleanup || return "$?"
}

broker_require_fixture() {
    # origin: hot-update S6 plan §10.1 / T58. Check before provisioning/traps.
    [ -r "$HERE/drills/lib/flow_fixture" ] && command -v flow_fixture >/dev/null 2>&1 || return 2
    case "${BROKER_LIVE_SUPPLIER:-}" in /*) ;; *) return 2 ;; esac
    [ -x "$BROKER_LIVE_SUPPLIER" ] || return 2
    [ -r "$HERE/drills/lib/hot_update_broker_probe.py" ] || return 2
    [ -r "$HERE/drills/lib/hot_update_mixed_probe.py" ] || return 2
}

broker_prepare() {
    broker_require_fixture || setup_fail 'broker live witness/transfer supplier is unavailable'
    BROKER_TOPOLOGY=$1
    BROKER_NODE=brk1
    BROKER_STEP=0
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail 'create broker evidence parent'
    BROKER_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-broker.${ARM}.${BROKER_TOPOLOGY}.XXXXXX") || setup_fail 'create broker evidence'
    chmod 0700 "$BROKER_EV" || setup_fail 'protect broker evidence'
    FLOW_EV="$BROKER_EV/flows"; mkdir -m 700 "$FLOW_EV" || setup_fail 'create flow evidence'
    FLOW_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail 'create cohort nonce'
    BROKER_REMOTE="/tmp/tether-37-$FLOW_NONCE"
    drill_install_traps broker_cleanup
    broker_setup 'original-socket and sealed-custody selftest' flow_fixture selftest
    broker_setup 'discard only isolated drill instance' "$SIM" nuke
    case "$BROKER_TOPOLOGY" in
        single)
            # A is the installed single product. Do not turn it into Raft N1.
            broker_setup 'installed single two agents and ctl' "$SIM" up --brokers 1 --agents 2 --ctl 1
            ;;
        sole-voter)
            broker_setup 'sole-voter two agents and ctl' "$SIM" up --brokers 1 --agents 2 --ctl 1
            broker_setup 'product sole-voter init' "$SIM" init brk1
            ;;
        quorum)
            [ "${2:-}" = grow_to_3 ] || setup_fail 'N3 grow supplier must be explicit in the arm manifest'
            broker_setup 'one strict N3 grow with two agents and ctl' "$2" 2 1 0
            ;;
        *) setup_fail 'unknown broker topology' ;;
    esac
    broker_supply topology-prior
    broker_setup 'create real isolated session' "$SIM" session "$SID" --pin "$PIN"
    broker_setup 'onboard first maintenance-capable agent' "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
    broker_setup 'onboard second maintenance-capable agent' "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
    if [ "$ARM" = B ]; then
        for _bp_node in brk1 brk2 brk3; do
            broker_setup 'real co-located shared-binary agent before first business' \
                colocated_agent_up "$_bp_node" "$SID" "$PIN" "$_bp_node"
        done
    fi
    # The supplier installs only observation channels and labeled deployment
    # config BEFORE business; it cannot restart or reconnect any original later.
    broker_supply observation-install
    broker_setup 'declare SSH four TCP two SS and long reliable exec' flow_fixture prepare broker
    broker_setup 'start original cohort' flow_fixture start
    broker_setup 'idle half-close and nonempty duplex priors' flow_fixture baseline
    broker_supply baseline
}

broker_stage() {
    _bst_binary=$1; _bst_node=$2
    [ -x "$_bst_binary" ] || return 2
    BROKER_SHA=$(sha256sum "$_bst_binary" | awk '{print $1}') || return 2
    BROKER_VERSION=$(timeout 5 "$_bst_binary" version | awk 'NR==1 {print $2}') || return 2
    [ -n "$BROKER_VERSION" ] || return 2
    # Privileged operator staging is explicit. Preserve the actual running
    # reader at .prev; never copy over a mapped executable's inode in place.
    d cp "$_bst_binary" "$(ctr_name "$_bst_node"):/tmp/tether-37-candidate" || return 2
    dexec "$_bst_node" -- sh -c 'set -eu; p=$(systemctl show tether-broker -p MainPID --value); test "$p" -gt 0; install -m 0755 "/proc/$p/exe" /usr/local/bin/tether.prev; install -m 0755 /tmp/tether-37-candidate /usr/local/bin/tether' || return 2
    _bst_sha=$(dexec "$_bst_node" -- sha256sum /usr/local/bin/tether | awk '{print $1}') || return 2
    [ "$_bst_sha" = "$BROKER_SHA" ]
}

broker_command_start() {
    [ -z "$BROKER_COMMAND_JOB" ] || return 2
    _bcs_label=$1; shift
    broker_child_run "$BROKER_EV/command-$_bcs_label.out" "$BROKER_EV/command-$_bcs_label.rc" "$@" &
    BROKER_COMMAND_JOB=$!
}

# origin: hot-update S8 plan §9.1 / L21. One shared FIX-ORPHAN owner also
# writes the native status AFTER reaping, so a cleanup defect cannot sign green.
broker_child_run() {
    _bcr_log=$1; _bcr_receipt=$2; shift 2
    case "${SIM_UPGRADE_SUPERVISOR:-}" in /*) ;; *) return 2 ;; esac
    [ -x "$SIM_UPGRADE_SUPERVISOR" ] || return 2
    exec "$SIM_UPGRADE_SUPERVISOR" --log "$_bcr_log" --receipt "$_bcr_receipt" -- "$@" >/dev/null 2>&1
}

broker_command_wait() {
    [ -n "$BROKER_COMMAND_JOB" ] || return 2
    wait "$BROKER_COMMAND_JOB"
    BROKER_COMMAND_RC=$?
    BROKER_COMMAND_JOB=
    return "$BROKER_COMMAND_RC"
}

broker_observers_start() {
    _bos_hop=$1
    # Supplier config binds ACTUAL CutID/producer event endpoints. An absent
    # phase or fresh-start slot remains native INCOMPLETE in P29's collector.
    broker_supply watch-config "$_bos_hop"
    _bos_nodes=$BROKER_NODE
    [ "$_bos_hop" != roll ] || _bos_nodes='brk1 brk2 brk3'
    for _bos_node in $_bos_nodes; do
        broker_child_run "$BROKER_EV/watch-$_bos_hop-$_bos_node.out" "$BROKER_EV/watch-$_bos_hop-$_bos_node.rc" \
        python3 -I -B "$HERE/drills/lib/hot_update_broker_probe.py" watch \
        --here "$HERE" --instance "$INSTANCE" --sim "$SIM" --broker "$_bos_node" \
        --root "$BROKER_EV/watch-$_bos_hop-$_bos_node" --events "$BROKER_EV/windows-$_bos_hop-$_bos_node.jsonl" \
        --config "$BROKER_EV/phases-$_bos_hop-$_bos_node.json" --duration 60 \
        --flow-fixture-root "$FLOW_EV" &
        BROKER_WATCH_JOB=$!
        BROKER_WATCH_JOBS="$BROKER_WATCH_JOBS $BROKER_WATCH_JOB"
        BROKER_WATCH_JOB=
    done
    # The scheduler runs in ctl's kernel clock domain alongside the watcher.
    # The supplier's scheduler-start stages the two P29 probes and budget file,
    # starts a remotely waitable process, and stays attached until its wait.
    broker_child_run "$BROKER_EV/probe-$_bos_hop.out" "$BROKER_EV/probe-$_bos_hop.rc" \
        "$BROKER_LIVE_SUPPLIER" scheduler-start --evidence "$BROKER_EV" --nonce "$FLOW_NONCE" \
        --hop "$_bos_hop" --events-dir "$BROKER_EV" \
        &
    BROKER_PROBE_JOB=$!
    broker_supply observers-ready "$_bos_hop"
}

broker_observers_wait() {
    for _bow_job in $BROKER_WATCH_JOBS; do
        wait "$_bow_job"; _bow_rc=$?
        broker_native "$_bow_rc"
    done
    BROKER_WATCH_JOBS=
    wait "$BROKER_PROBE_JOB"; _bow_rc=$?; BROKER_PROBE_JOB=
    broker_native "$_bow_rc"
}

broker_reload() {
    _br_hop=$1; _br_binary=$2
    _br_outcome=${3:-candidate}
    broker_setup 'privileged candidate and actual previous-reader staging' broker_stage "$_br_binary" "$BROKER_NODE"
    broker_supply pure-preflight "$_br_hop"
    broker_observers_start "$_br_hop"
    broker_command_start "$_br_hop" "$SIM" exec "$BROKER_NODE" -- /usr/local/bin/tether \
        broker upgrade reload --to-version "$BROKER_VERSION" --expect-sha256 "$BROKER_SHA"
    broker_command_wait; _br_rc=$?
    if [ "$_br_outcome" = current-rollback ]; then
        # Reload's candidate-completion query can truthfully end UNKNOWN when
        # the intended fault returned to prev. Judge the real command outcome
        # together with the barrier/current-state receipt, never translate it.
        broker_supply fault-command-outcome "$_br_hop" "$_br_rc"
    else
        broker_native "$_br_rc"
    fi
    broker_observers_wait
    broker_setup 'same PID different running SHA original agent and allocation identities' flow_fixture checkpoint "hop-$_br_hop"
    broker_supply current-witness "$_br_hop"
}

broker_roll() {
    _brl_binary=$1
    for _brl_node in brk1 brk2 brk3; do
        broker_setup 'privileged per-voter staging' broker_stage "$_brl_binary" "$_brl_node"
    done
    broker_supply roll-prior
    broker_observers_start roll
    # The actual driver, rather than this harness, owns fresh transfer at EVERY
    # step, maintenance holder/CAS and follower-first/leader-last ordering.
    broker_command_start roll "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
        /usr/local/bin/tether cluster upgrade --broker-only --to-version "$BROKER_VERSION" \
        --expect-sha256 "$BROKER_SHA" --account-seed /etc/tether/secrets/account.nk --backup-taken
    broker_command_wait; _brl_rc=$?
    broker_native "$_brl_rc"
    broker_observers_wait
    broker_supply roll-current-witness
}

# origin: hot-update S7 plan §10.6 / T43. Capable live responsibility now
# migrates before the default reaper. Legacy/unknown remain separate refusal
# fixtures in P29's manifest; a terminal-only cut cannot sign this lane.
broker_transfer_lane() {
    _btl_topology=$1
    . "$HERE/drills/lib/hot_update_transfer.sh"
    TRANSFER_TOPOLOGY=$_btl_topology
    TRANSFER_PARENT_CLEANUP=broker
    transfer_run T
    transfer_cleanup
    # transfer_prepare installs its own EXIT owner; restore the original flow
    # custody owner after the judged transfer fixture has been stopped.
    drill_install_traps broker_cleanup
    unset TRANSFER_TOPOLOGY TRANSFER_PARENT_CLEANUP
}

# Generate a throwaway source overlay; never mutate production files, add a
# build tag or leave an env fault switch in the release. Exact needles refuse
# source drift. Pure preflight executes neither of these import-only barriers.
# origin: hot-update S6 plan §10.2 / T58
broker_fault_overlay() {
    [ "$#" = 4 ] || return 2
    python3 -I -B - "$1" "$2" "$3" "$4" <<'PYFAULT'
import hashlib, json, pathlib, re, sys
repo, output, point, remote = map(str, sys.argv[1:])
if point not in ('held', 'active') or not re.fullmatch(r'/tmp/tether-37-[0-9a-f]{32}', remote):
    sys.exit(2)
original = pathlib.Path(repo).resolve()/'internal/brokerimage/import.go'
data = original.read_text()
out = pathlib.Path(output); out.mkdir(mode=0o700, parents=True, exist_ok=False)
entered = json.dumps(remote+'/fault-'+point+'-entered.json')
if point == 'held':
    needle = '\tfile, err := s.source.TakeFile(ref)\n'
    patch = needle + '''\t// origin: hot-update S6 plan §10.2 / T58; temporary held overlay.
\tif file != nil && len(s.taken) == 1 {
\t\tfault, faultErr := os.OpenFile(ENTERED, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
\t\tif faultErr == nil {
\t\t\t_, writeErr := fault.WriteString("{\\"point\\":\\"held\\",\\"ordinal\\":2,\\"nonnil_file\\":true}\\n")
\t\t\terr = errors.Join(ErrImage, writeErr, fault.Close())
\t\t}
\t}
'''.replace('ENTERED', entered)
else:
    needle = '\tif err = i.control.StartBusiness(deadline); err != nil {\n\t\treturn errors.Join(ErrActive, err)\n\t}\n'
    patch = needle + '''\t// origin: hot-update S6 plan §10.2 / T58; temporary ACTIVE overlay.
\tfault, faultErr := os.OpenFile(ENTERED, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
\tif faultErr == nil {
\t\t_, writeErr := fault.WriteString("{\\"point\\":\\"active\\",\\"active\\":true,\\"business_started\\":true}\\n")
\t\tif closeErr := fault.Close(); writeErr != nil || closeErr != nil {
\t\t\treturn errors.Join(ErrActive, writeErr, closeErr)
\t\t}
\t\t// The independent supplier releases only after NEW ACK and original byte
\t\t// advancement. Waiting consumes the existing cut deadline, never extends it.
\t\tfor deadlineAlive(deadline) {
\t\t\tif _, releaseErr := os.Stat(RELEASE); releaseErr == nil {
\t\t\t\treturn errors.Join(ErrActive, ErrImage)
\t\t\t}
\t\t\ttime.Sleep(time.Millisecond)
\t\t}
\t\treturn errors.Join(ErrActive, ErrImage)
\t}
'''.replace('ENTERED', entered).replace('RELEASE', json.dumps(remote+'/fault-active-release'))
    data = data.replace('\t"sync"\n', '\t"sync"\n\t"time"\n')
if data.count(needle) != 1:
    sys.exit(2)
mutant = out/'import.go'; mutant.write_text(data.replace(needle, patch))
(out/'overlay.json').write_text(json.dumps({'Replace': {str(original): str(mutant.resolve())}})+'\n')
(out/'source.json').write_text(json.dumps(dict(point=point, source_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),
    overlay_sha256=hashlib.sha256(mutant.read_bytes()).hexdigest(), entry_path=remote+'/fault-'+point+'-entered.json'))+'\n')
PYFAULT
}

broker_fault() {
    _bf_point=$1
    _bf_overlay="$BROKER_EV/overlay-$_bf_point"
    broker_setup 'legal import-only one-shot source overlay' broker_fault_overlay "$HERE/../.." "$_bf_overlay" "$_bf_point" "$BROKER_REMOTE"
    broker_setup 'build real fault artifact with unchanged compiled readers and schema' \
        go build -overlay "$_bf_overlay/overlay.json" -o "$_bf_overlay/tether" "$HERE/../../cmd/tether"
    broker_supply fault-arm "$_bf_point" "$BROKER_REMOTE" "$_bf_overlay/source.json"
    # ACTIVE release runs concurrently with the real unsafe command. It must
    # observe the exact entry receipt, candidate SHA, new ACK and original
    # offsets before touching its one-shot release file; no timer-based fault.
    if [ "$_bf_point" = active ]; then
        broker_child_run "$BROKER_EV/fault-active-release.out" "$BROKER_EV/fault-active-release.rc" \
            "$BROKER_LIVE_SUPPLIER" fault-active-release --evidence "$BROKER_EV" --remote "$BROKER_REMOTE" &
        BROKER_FAULT_JOB=$!
    fi
    broker_reload "fault-$_bf_point" "$_bf_overlay/tether" current-rollback
    if [ "$_bf_point" = active ]; then
        wait "$BROKER_FAULT_JOB"; _bf_rc=$?; BROKER_FAULT_JOB=
        broker_native "$_bf_rc"
    fi
    broker_supply fault-current-witness "$_bf_point"
}

broker_finish() {
    broker_setup 'finish original finite business without replacement sockets' flow_fixture finish
    broker_setup 'collect raw ledgers and four shared log streams before cleanup' flow_fixture collect
    broker_supply collect
    # Original sealed custody stays alive until the bundle oracle has used it.
    broker_step python3 -I -B "$HERE/drills/lib/hot_update_broker_probe.py" bundle-judge \
        --evidence "$BROKER_EV/control" --flow-fixture-root "$FLOW_EV"
    broker_native "$?"
    broker_supply receipt
}

# A file called "pin" is insufficient. Bind the reviewed literal observation
# to an actual old artifact, peeled source and archived first-run bytes. Escape
# the literal here so EOF|RST guesses or a diagnostic regex cannot widen it.
# origin: hot-update S6 plan §10.2 / T58
broker_legacy_pin_ok() {
    python3 -I -B - "$@" <<'PYPIN'
import hashlib, json, pathlib, re, sys
try:
    pinpath, binary, sourcepath = map(pathlib.Path, sys.argv[1:])
    pin = json.loads(pinpath.read_text()); source = json.loads(sourcepath.read_text())
    assert pin['schema'] == 'broker-legacy-pin-v1' and pin['reviewed_by'] == 'lead'
    assert re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+',pin['tag']) and pin['tag'] == source['tag']
    assert re.fullmatch(r'[0-9a-f]{40}',pin['source_sha']) and pin['source_sha'] == source['commit']
    assert pin['binary_sha256'] == hashlib.sha256(binary.read_bytes()).hexdigest()
    assert pin['preflight_capability'] == {'signed_reload':True,'n3':True,'legacy_run_unwind':True}
    signature = pin['observed_signature']
    assert isinstance(signature,str) and 0 < len(signature) <= 1024 and '\n' not in signature and '\r' not in signature
    receipt = pathlib.Path(pin['receipt_path'])
    assert not receipt.is_absolute() and '..' not in receipt.parts
    rawpath = (pinpath.parent/receipt).resolve()
    assert rawpath.is_relative_to(pinpath.parent.resolve())
    raw = rawpath.read_bytes()
    assert pin['receipt_sha256'] == hashlib.sha256(raw).hexdigest()
    assert json.loads(raw)['observed_signature'] == signature
    print('^'+re.escape(signature)+'$')
except (AssertionError,OSError,ValueError,KeyError,TypeError):
    sys.exit(2)
PYPIN
}

# origin: hot-update S6 plan §10.2 / T58. BR supplies a REAL old N3 follower
# before common N1 setup. UNPINNED is setup RED, never guessed EOF/RST success.
broker_legacy_arm() {
    broker_require_fixture || setup_fail 'broker legacy live supplier is unavailable'
    [ -n "${BROKER_LEGACY_PIN:-}" ] && [ -r "$BROKER_LEGACY_PIN" ] || setup_fail '39.BR reviewed legacy signature is UNPINNED'
    BROKER_LEGACY_BINARY=${BROKER_LEGACY_BINARY:-$SIM_VENDOR/tether-prev}
    BROKER_LEGACY_REGEX=$(broker_legacy_pin_ok "$BROKER_LEGACY_PIN" "$BROKER_LEGACY_BINARY" "$SIM_VENDOR/tether-prev-source.json") || setup_fail '39.BR reviewed pin provenance is invalid'
    BROKER_TOPOLOGY=quorum; BROKER_NODE=brk1; BROKER_STEP=0
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail 'create legacy evidence parent'
    BROKER_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-broker.BR.XXXXXX") || setup_fail 'create legacy evidence'
    chmod 0700 "$BROKER_EV" || setup_fail 'protect legacy evidence'
    FLOW_EV="$BROKER_EV/flows"; mkdir -m 700 "$FLOW_EV" || setup_fail 'create legacy flow evidence'
    FLOW_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail 'create legacy nonce'
    drill_install_traps broker_cleanup
    broker_setup 'discard only isolated legacy instance' "$SIM" nuke
    [ "${1:-}" = grow_to_3 ] || setup_fail 'legacy N3 grow must be explicit before common N1 setup'
    broker_setup 'independent legacy N3 two agents and ctl' "$1" 2 1 0
    broker_supply legacy-provenance "$BROKER_LEGACY_PIN"
    broker_supply legacy-install-before-business
    broker_supply legacy-follower-home-prior
    broker_supply legacy-business-start
    broker_setup 'stage candidate on original legacy follower' broker_stage "$SIM_VENDOR/tether-next" "$BROKER_NODE"
    # This is the OLD release's existing SIGNED remote reload entry. P30 drives
    # the reviewed argv itself; supplying fixture evidence cannot issue reload.
    broker_supply legacy-signed-command
    broker_command_start legacy python3 -I -B -c '
import json, pathlib, subprocess, sys
argv = json.loads(pathlib.Path(sys.argv[1]).read_text())["argv"]
if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) for arg in argv):
    sys.exit(2)
if not pathlib.Path(argv[0]).is_absolute():
    sys.exit(2)
# Source/binary/old signed verb/target verification belongs to the pinned supply
# receipt. Avoid shell evaluation and inherit the supervised process group.
sys.exit(subprocess.run(argv, check=False).returncode)
' "$BROKER_EV/legacy-command.json"
    broker_command_wait
    broker_supply legacy-command-outcome "$BROKER_COMMAND_RC"
    broker_supply legacy-collect
    broker_supply legacy-exec-proof "$BROKER_COMMAND_RC" "$BROKER_SHA"
    broker_setup 'actual signed historical broker exec' broker_legacy_exec_required
    # Classification belongs to the shared native harness: a corrected product
    # returns ASSERT-FAIL, incomplete observations SETUP-RED; only exact loss 1
    # may be matched. The pin is evidence-backed and has no broad default.
    broker_step "$BROKER_LIVE_SUPPLIER" legacy-signature --evidence "$BROKER_EV" --pin "$BROKER_LEGACY_PIN"
    _bla_rc=$?
    broker_legacy_verdict "$_bla_rc" "$BROKER_STEP_LOG"
}

broker_legacy_verdict() {
    broker_legacy_exec_required || { broker_native 2; return; }
    case "$1" in
        2|4) broker_native "$1" ;;
        0|1)
            # A diagnostic containing the pinned text is not a single native
            # observation. Unknown loss never earns the legacy owner waiver.
            if [ "$1" = 1 ] && { [ "$(wc -l < "$2")" != 1 ] || ! grep -qxE "$BROKER_LEGACY_REGEX" "$2"; }; then
                _AS_FAIL=$((_AS_FAIL + 1))
                return 0
            fi
            assert_bug 'original broker cohort survives legacy Run unwind' \
                'hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration' \
                "$BROKER_LEGACY_REGEX" broker_legacy_observation "$1" "$2"
            ;;
        *) setup_fail 'invalid legacy oracle status' ;;
    esac
}

broker_legacy_observation() {
    cat "$2"
    return "$1"
}

# origin: hot-update S8 plan §9.3 / P28. BR retains its own reviewed signed
# command and source pin. Agent migration evidence cannot certify Run unwind.
broker_legacy_exec_required() {
    python3 -I -B - "$BROKER_EV/legacy-exec.json" "$BROKER_LEGACY_PIN" "$BROKER_SHA" "$BROKER_COMMAND_RC" <<'PYBR'
import json,pathlib,sys
try:
    row=json.loads(pathlib.Path(sys.argv[1]).read_text())
    pin=json.loads(pathlib.Path(sys.argv[2]).read_text())
    assert sys.argv[4]=='0' and row['command_rc']==0
    assert row['source_sha']==pin['source_sha'] and row['signed_command'] is True
    assert row['old_running_sha']==pin['binary_sha256']
    assert row['new_running_sha']==sys.argv[3] and row['old_running_sha']!=row['new_running_sha']
    assert row['role']=='follower' and row['topology']=='N3' and row['run_unwind_observed'] is True
    before,after=row['before'],row['after']
    assert before==after and before['pid']>1 and before['start_ticks']>0 and before['boot_id']
except (AssertionError,OSError,ValueError,KeyError,TypeError):
    print('legacy signed broker exec is unproven',file=sys.stderr)
    sys.exit(2)
PYBR
}

# origin: hot-update S8 plan §7 / §9.0 / §9.3 / P28. P24/P22 supply the
# isolated role bindings and command descriptors; the driver issues each
# product command once. The supplier's prepare/observe/judge/stop operations
# are fixture/read-only ports, NEVER a second upgrade driver. Missing ports
# fail setup. The loaded ABI case uses P26's sole full-profile oracle; idle
# cases require P24's complete census/fence receipts without starting a workload.
#
# SIM_UPGRADE_COMPAT_SUPPLIER -- operation --arm --evidence --supervisor
# prepare writes plan.json: version, arm, commands [{argv, product_argv, cwd,
# timeout_s}], and for wire, wire_manifest (P22) and values (sealed P24 values);
# ABI-adjacent additionally names fixture_manifest (P26 version 1). observe captures
# executions.json (P22 source-copy-/proc receipts for ctl/agent/broker) and raw
# P24 oracles. await-fence observes CC-late-create while its upgrade is live.
# judge emits result.json {version,arm,kind,native_rc,command_rcs,
# signature,checks}; stop writes cleanup.json {empty:true,remaining:[]} AFTER
# shared-supervisor joins. These S8-side ports do not pretend an absent live
# adapter has passed. All files are retained under one fresh evidence root.
upgrade_39_compat() {
    _u39_case=$(upgrade_39_cases | awk -F '\t' -v arm="$1" '$1==arm')
    [ -n "$_u39_case" ] || { printf '%s\n' 'unknown compatibility arm' >&2; return 2; }
    case "${SIM_UPGRADE_COMPAT_SUPPLIER:-}" in /*) ;; *) printf '%s\n' 'P24/P22 live compatibility supplier is required' >&2; return 2 ;; esac
    [ -x "$SIM_UPGRADE_COMPAT_SUPPLIER" ] || return 2
    [ -x "${SIM_UPGRADE_SUPERVISOR:-}" ] || return 2
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || return 2
    _u39_ev=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-compat.$1.XXXXXX") || return 2
    "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE" "$_u39_ev" "$SIM_UPGRADE_COMPAT_SUPPLIER" "$SIM_UPGRADE_SUPERVISOR" "$_u39_case" <<'PYCOMPAT'
import importlib.util,json,os,pathlib,re,subprocess,sys,time
here,root,supplier,supervisor=map(pathlib.Path,sys.argv[1:5])
arm,kind,expected,cli,fixture,grows,worst=sys.argv[5].split('\t')
expected=int(expected); wanted=[int(x) for x in cli.split(',')]
checks={
 'LB-new':['legacy_busy','all_entry_policy','running_unchanged','original_business'],
 'LB-old':['legacy_busy','old_ctl_rc64','all_entry_policy','running_unchanged','original_business'],
 'LI-defer':['quiescence_unknown','running_unchanged','original_business'],
 'LI-interrupting':['all_consumers_capable','unique_issuer','dispatch_once','outcome_unknown','non_hot'],
 'M-route':['mixed_consumer','route_changed','no_dispatch','original_business'],
 'CC-idle':['complete_idle_census','fence_held','three_receipts'],
 'CC-late-create':['late_create_fenced','no_untracked_admission','three_receipts'],
 'CL-reject':['required_reader_missing','preactivation_refusal','original_business'],
 'ABI-adjacent':['nonempty_forward_decode','nonempty_reverse_decode','three_receipts'],
 'ABI-too-far':['two_epoch_gap','preactivation_refusal','original_business'],
 'ABI-unknown':['unknown_reader','preactivation_refusal','original_business'],
 'GO-hash':['distinct_toolchains','nonempty_checkpoint','continued_digest_equal'],
}
wire_checks={'exec':['exact_nonce','one_execution'], 'run':['real_pty','input_output','resize','unique_terminal'],
 'kill':['original_pid','signal15','unique_terminal','old_sender_when_applicable'],
 'expose':['public_port_nonce','cleanup'], 'transfer':['tier_b_8mib','push_pull_hash','unique_terminals']}
rc=2; fixture_module=None; fixture_started=False; writer=None; statuses=[]; signature='setup_unproven'
class NativeVerdict(Exception):
    pass
def error_native(error):
    code=getattr(error,'rc',2)
    return code if type(code) is int and code in (1,2,3,4) else 2
def join_native(first,second):
    # Match assert.sh: ASSERT-FAIL, SETUP-RED, PRODUCT-RED, INCOMPLETE, GREEN.
    priority={1:0,2:1,3:2,4:3,0:4}
    first=first if first in priority else 2; second=second if second in priority else 2
    return min((first,second),key=priority.__getitem__)
def require(ok,reason):
    if not ok: raise ValueError(reason)
def read(path):
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'duplicate_field'); value[key]=item
        return value
    return json.loads(pathlib.Path(path).read_text(),object_pairs_hook=unique)
def invoke(argv,label,timeout=55,cwd=None):
    # The shared launcher above owns this entire lineage, including setsid.
    with (root/(label+'.stdout')).open('xb') as out,(root/(label+'.stderr')).open('xb') as err:
        result=subprocess.run(argv,cwd=cwd,stdout=out,stderr=err,timeout=timeout,check=False)
    (root/(label+'.rc')).write_text(str(result.returncode)+'\n')
    return result.returncode
def supply(op):
    return invoke([str(supplier),op,'--arm',arm,'--evidence',str(root),'--supervisor',str(supervisor)],'supplier-'+op)
def run_commands(commands):
    if arm!='CC-late-create':
        return [invoke(cmd['argv'],'command-'+str(i),cmd['timeout_s'],cmd['cwd']) for i,cmd in enumerate(commands)]
    # A create after upgrade completion cannot test admission at the fence.
    # Keep the upgrade alive while P24 observes its actual fence, then issue
    # the competing create. The supplier only observes; it never issues either.
    first,late=commands
    started=time.monotonic()
    with (root/'command-0.stdout').open('xb') as out,(root/'command-0.stderr').open('xb') as err:
        child=subprocess.Popen(first['argv'],cwd=first['cwd'],stdout=out,stderr=err)
        try:
            require(supply('await-fence')==0 and child.poll() is None,'late_create_fence_missing')
            late_rc=invoke(late['argv'],'command-1',late['timeout_s'],late['cwd'])
            first_rc=child.wait(timeout=max(0,first['timeout_s']-(time.monotonic()-started)))
            (root/'command-0.rc').write_text(str(first_rc)+'\n')
            return [first_rc,late_rc]
        finally:
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=3)
                except subprocess.TimeoutExpired: child.kill(); child.wait()

def executions(plan):
    rows=read(root/'executions.json')
    required_roles={'parser'} if kind=='config' else {'exporter','reader'} if kind=='decoder' else {'ctl','agent','broker'}
    require(set(rows)==required_roles,'role_execution_missing')
    for role,row in rows.items():
        require(row['schema_version']==1 and row['pid']>1 and row['start_ticks']>0 and row['boot_id'] and row['observed_at'],'role_not_executed')
        artifact=row['artifact']
        for field in ('commit','tree_digest','archive_sha256','toolchain','build_flags','dirty_diff_sha256'):
            require(artifact[field],'source_provenance_missing')
        require(re.fullmatch('[0-9a-f]{64}',row['running_sha256']) and row['running_sha256']==row['copied_sha256']==artifact['binary_sha256'],'source_copy_execute_mismatch')
        require(row['argv'] and row['argv'][0]==row['copied_path'] and row['proc_exe']==row['copied_path'],'execution_path_mismatch')
    if kind=='wire':
        cell,verb=arm.split('-'); manifest=read(plan['wire_manifest'])
        selected=[row for row in manifest['rows'] if row['cell']==cell and row['verb']==verb]
        require(len(selected)==1,'wire_row_missing'); chosen=selected[0]
        for role in rows:
            require(rows[role]['copied_path']==chosen[role+'_path'] and rows[role]['running_sha256']==chosen[role+'_sha'],'quadrant_not_executed')
    return rows
try:
    require(supply('prepare')==0,'compatibility_prepare_failed')
    plan=read(root/'plan.json'); require(plan['version']==1 and plan['arm']==arm,'wrong_arm_plan')
    commands=plan['commands']; require(len(commands)==len(wanted),'command_count_mismatch')
    for cmd in commands:
        require(set(cmd)=={'argv','product_argv','cwd','timeout_s'},'command_schema')
        argv=cmd['argv']; product=cmd['product_argv']
        require(argv and product and all(type(x) is str and x for x in argv+product),'invalid_argv')
        require(pathlib.Path(argv[0]).is_absolute() and pathlib.Path(product[0]).is_absolute() and pathlib.Path(cmd['cwd']).is_absolute(),'absolute_command_required')
        require(argv[-len(product):]==product,'transport_changed_product_argv')
        require(type(cmd['timeout_s']) is int and 0<cmd['timeout_s']<=int(worst),'unbounded_command')
        require(('--interrupting' in product)==(arm=='LI-interrupting'),'interrupting_scope')
    if kind=='wire':
        cell,verb=arm.split('-'); required_checks=wire_checks[verb]
        templates=read(here.parent.parent/'test/hotupdate/testdata/upgrade_wire_argv.json')
        template=next(row for row in templates if row['cell']==cell and row['verb']==verb)
        values=plan['values']; require(set(values)=={'HOME_CELL','NATS_URL','NID','PID','NONCE'},'wire_values')
        require(all(type(v) is str and v for v in values.values()) and re.fullmatch('[0-9a-f]{32}',values['NONCE']),'wire_nonce')
        def bind(arg):
            for key in ('HOME_CELL','NATS_URL','NID','PID','NONCE'): arg=arg.replace(key,values[key])
            return arg
        primary=[bind(arg) for arg in template['argv']]
        require(commands[0]['product_argv']==primary,'frozen_wire_argv_changed')
        if verb in ('expose','transfer'):
            companion=read(plan['wire_manifest'])['companions'][cell]['expose_cleanup' if verb=='expose' else 'transfer_pull']
            require(commands[1]['product_argv']==companion,'wire_companion_missing')
    elif kind=='config':
        component=arm.split('-')[1]; product=commands[0]['product_argv']
        require(('-t' in product and '-c' in product) if component=='nats' else ('validate' in product and '--config' in product),'real_config_parser_required')
        required_checks=['same_rendered_config','actual_'+component+'_parser','selected_'+arm.split('-')[2]+'_binary']
    else: required_checks=checks[arm]
    if arm=='ABI-adjacent':
        path=here/'drills/lib/upgrade_tx_fixture.py'
        spec=importlib.util.spec_from_file_location('upgrade_tx_fixture',path)
        fixture_module=importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture_module)
        fm=fixture_module.manifest_read(plan['fixture_manifest'])
        require(fm['arm']==arm,'fixture_arm_mismatch')
        reader,writer=os.pipe()
        try: fixture_module.start(root/'fixture',fm,reader); fixture_started=True
        finally: os.close(reader)
    statuses=run_commands(commands)
    require(supply('observe')==0,'compatibility_observation_missing')
    executions(plan)
    if fixture_started:
        fixture_module.collect.sample(root/'fixture',fm['case_id'],__import__('secrets').token_hex(16))
        fixture_module.drain(root/'fixture',fm)
        fixture_rc=invoke([sys.executable,'-I','-B',str(here/'drills/lib/upgrade_tx_fixture.py'),'judge','--manifest',plan['fixture_manifest'],'--evidence',str(root/'fixture')],'fixture-judge')
        if fixture_rc!=0:
            rc=fixture_rc if fixture_rc in range(5) else 2
            raise NativeVerdict(rc)
    oracle_rc=supply('judge')
    result=read(root/'result.json')
    require(result['version']==1 and result['arm']==arm and result['kind']==kind,'wrong_result_identity')
    require(type(result['native_rc']) is int and result['native_rc']==oracle_rc and oracle_rc in range(5),'native_status_mismatch')
    require(result['command_rcs']==statuses,'command_status_rewritten')
    require(isinstance(result['signature'],str) and result['signature'] and '\n' not in result['signature'],'signature_missing')
    signature=result['signature']; rc=oracle_rc
    if statuses!=wanted: rc=1
    if not all(result['checks'].get(key) is True for key in required_checks): rc=1
    # The kind remains non_hot even when its assertion succeeds. Expected
    # native verdicts are compared by the external ledger, not rewritten here.
except NativeVerdict as error:
    rc=error.args[0]
except Exception as error:
    print('compatibility evidence or prerequisite unavailable: '+type(error).__name__,file=sys.stderr)
    rc=error_native(error)
finally:
    if fixture_started:
        try: fixture_module.stop(root/'fixture',fm)
        except Exception: rc=join_native(rc,2)
    if writer is not None: os.close(writer)
    try:
        cleanup_rc=supply('stop'); cleanup=read(root/'cleanup.json')
        if cleanup_rc!=0 or cleanup.get('empty') is not True or cleanup.get('remaining')!=[]: rc=join_native(rc,2)
    except (OSError,ValueError,KeyError,TypeError,subprocess.TimeoutExpired): rc=join_native(rc,2)
    (root/'receipt.json').write_text(json.dumps(dict(version=1,arm=arm,kind=kind,native_rc=rc,
        expected_native_rc=expected,expected_cli_rcs=wanted,command_rcs=statuses,signature=signature),sort_keys=True)+'\n')
    print('compatibility receipts: '+str(root),file=sys.stderr)
sys.exit(rc)
PYCOMPAT
}
