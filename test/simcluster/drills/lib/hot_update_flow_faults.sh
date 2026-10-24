#!/bin/sh
# origin: hot-update S5 plan §9.1–§9.3 / T48.
# Instruments run only in the isolated agent netns. The live flow_fixture supplies
# event collection and raw ledgers; it never supplies commit/resume/rollback.
FLOW_BIN=/home/sim/.local/bin
FLOW_DST=$FLOW_BIN/tether
FLOW_MARKER=$FLOW_BIN/.tether-upgrade.json
FLOW_MODE=

# Preserve native status immediately, in the main shell: assertion predicates
# execute in a subshell and cannot return job IDs or fixture tokens to callers.
flow_step_run() {
    FLOW_STEP_COUNT=$((${FLOW_STEP_COUNT:-0} + 1))
    mkdir -p "$FLOW_EV/steps" || return 2
    FLOW_STEP_LOG="$FLOW_EV/steps/$FLOW_STEP_COUNT.out"
    "$@" > "$FLOW_STEP_LOG" 2>&1
    FLOW_STEP_RC=$?
    printf '%s\n' "$FLOW_STEP_RC" > "$FLOW_EV/steps/$FLOW_STEP_COUNT.rc" || return 2
    return "$FLOW_STEP_RC"
}
flow_step_status() {
    if [ "$1" != 0 ]; then
        printf 'flow step failed: native_rc=%s evidence=%s\n' "$1" "$2" >&2
        tail -n 20 "$2" >&2
    fi
    return "$1"
}

flow_fault_valid() {
    # Actual event/identity evidence, not an invocation counter or a sleep.
    jq -e --arg nonce "$FLOW_NONCE" '
      def positive: type=="number" and .>0 and floor==.;
      .nonce==$nonce and .valid_fault==true and .observed==true and
      (.event_seq|positive) and (.before.pid|positive) and
      .before.pid==.after.pid and .before.start_ticks==.after.start_ticks and
      (.before.start_ticks|positive) and
      (.before.running_sha|test("^[0-9a-f]{64}$")) and
      (.after.running_sha|test("^[0-9a-f]{64}$")) and
      .before.running_sha!=.after.running_sha and
      .marker.state=="committed" and (.marker.boot_count|positive)' "$1" >/dev/null
}

flow_legacy_refused() {
    [ "$1" = 75 ] || return 1
    # origin: hot-update S5 plan §7.3 / §9.1 / T48. Substring matches would
    # accept stale flow_busy/ss_busy or a different refusal plus diagnostic text.
    python3 -I -B - "$2" <<'PYREFUSAL'
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
codes = re.findall(r'agent_rejected:([a-z_]+)\b', text)
reasons = re.findall(r'\bdeferred:\s*(.*?)\s+\(agent_rejected:upgrade_deferred\)\s*$', text, re.M)
valid = codes == ['upgrade_deferred'] and len(reasons) == 1
if valid:
    entries, depth, start = [], 0, 0
    for index, char in enumerate(reasons[0]):
        if char == '(': depth += 1
        elif char == ')': depth -= 1
        elif char == ';' and depth == 0:
            entries.append(reasons[0][start:index]); start = index+1
        if depth < 0: valid = False
    entries.append(reasons[0][start:])
    valid = valid and depth == 0 and len(entries) == 1 and re.fullmatch(r'legacy_session_busy(?:\s+\([^\n]*\))?\s*', entries[0]) is not None
sys.exit(0 if valid else 1)
PYREFUSAL
}

# The watchdog only earns its own receipt with a real failed durable commit,
# fresh SS grant and unchanged allocation throughout its original deadline.
flow_watchdog_valid() {
    jq -e --arg nonce "$FLOW_NONCE" '
      def uint: type=="number" and .>=0 and floor==.;
      def number: type=="number" and .>=0;
      (.agent_uid|uint) and (.nats_block_ms|number) and (.elapsed_s|number) and
      (.generation_before|uint) and (.generation_after|uint) and
      .nonce==$nonce and .agent_uid>0 and .permission_denied==true and
      .nats_block_ms>=0 and .nats_block_ms<=2000 and .mode_restored==true and
      .commit_write_failed==true and .mark_ready==true and .pending==true and
      .watchdog_armed==true and .full_register_count==1 and .reaper_running==true and
      .allocation_unchanged==true and .fresh_ss_grant==true and
      .ss_authority_available==true and .elapsed_s>=120 and .automatic==true and
      .second_cut_current==true and .generation_after>.generation_before and
      .final_marker=="rolled_back" and .final_running_sha==.original_running_sha and
      (.original_running_sha|test("^[0-9a-f]{64}$"))' "$1" >/dev/null
}

# Only the specified target SYN is suppressed. This cannot stall the cohort's
# existing sockets or the independent authority/control transport.
flow_artifact_gate_arm() {
    # All setup precedes the original OPEN; the artifact bytes/SHA and product
    # verification path stay unchanged. Only delivery of its final byte waits.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    _fag_secrets=$(_secrets_node_dir "$INSTANCE" "$ARTIFACT_NODE")
    d cp "$HERE/drills/lib/flow_artifact_gate.py" "$(ctr_name "$ARTIFACT_NODE"):/tmp/flow_artifact_gate.py" || return 2
    d cp "$_fag_secrets/artifact-cert.pem" "$(ctr_name "$ARTIFACT_NODE"):/tmp/artifact-cert.pem" || return 2
    d cp "$_fag_secrets/artifact-key.pem" "$(ctr_name "$ARTIFACT_NODE"):/tmp/artifact-key.pem" || return 2
    ingress_down "$ARTIFACT_NODE" 443 || return 2
    d exec -d "$(ctr_name "$ARTIFACT_NODE")" python3 -I -B /tmp/flow_artifact_gate.py serve --cert /tmp/artifact-cert.pem --key /tmp/artifact-key.pem || return 2
    poll_until 5 1 'gated artifact TLS listener' -- _ingress_front_up "$ARTIFACT_NODE" 443
}
flow_artifact_gate_wait() {
    dexec "$ARTIFACT_NODE" -- python3 -I -B /tmp/flow_artifact_gate.py wait
}
flow_artifact_gate_release() {
    dexec "$ARTIFACT_NODE" -- python3 -I -B /tmp/flow_artifact_gate.py verify
}

flow_open_syn_on() {
    _fos_ip=$1; _fos_port=$2
    case "$_fos_ip" in ''|*[!0-9.]*) return 2 ;; esac
    case "$_fos_port" in ''|*[!0-9]*) return 2 ;; esac
    [ "$_fos_port" -gt 0 ] && [ "$_fos_port" -le 65535 ] || return 2
    # Hold the actual SYN rather than drop it into a three-second retry.
    # The local sealed cut watcher releases that packet, keeping the original
    # product FD and its two-second deadline intact.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    d cp "$HERE/drills/lib/flow_open_fault.py" "$(ctr_name agt1):/tmp/flow_open_fault.py" || return 2
    dexec agt1 -- python3 -I -B /tmp/flow_open_fault.py arm --root "/run/tether-drill-custody/$FLOW_NONCE" --peer-root "/tmp/s5-flow-$FLOW_NONCE" || return 2
    dexec agt1 -- sh -eu -c '
        iptables -N FLOWOPEN 2>/dev/null || true
        iptables -C OUTPUT -j FLOWOPEN 2>/dev/null || iptables -I OUTPUT -j FLOWOPEN
        iptables -A FLOWOPEN -p tcp -d "$1" --dport "$2" --syn -j NFQUEUE --queue-num 75
    ' sh "$_fos_ip" "$_fos_port"
}
flow_open_syn_off() {
    # Verify the native cut release; a later host flush cannot substitute for it.
    dexec agt1 -- python3 -I -B /tmp/flow_open_fault.py verify --root "/run/tether-drill-custody/$FLOW_NONCE" || return 2
    dexec agt1 -- iptables -F FLOWOPEN
}

flow_carrier_rebuild() {
    flow_fixture carrier-baseline || return 2
    _fcr_ip=$(node_ip brk1) || return 2
    case "$_fcr_ip" in ''|*[!0-9.]*) return 2 ;; esac
    # Kill the carrier TCP OFD, not an application socket or agent process.
    dexec agt1 -- ss -K dst "$_fcr_ip" dport = 7000 || return 2
    flow_fixture wait-event carrier-restored 5 || return 2
    flow_fixture checkpoint carrier
}

flow_watchdog_arm() {
    d cp "$HERE/drills/lib/flow_open_fault.py" "$(ctr_name agt1):/tmp/flow_open_fault.py" || return 2
    dexec agt1 -- sh -eu -c '
        iptables -N FLOWNATS 2>/dev/null || true
        iptables -C OUTPUT -j FLOWNATS 2>/dev/null || iptables -I OUTPUT -j FLOWNATS
    ' || return 2
    dexec agt1 -- python3 -I -B /tmp/flow_open_fault.py watchdog-arm --root "/run/tether-drill-custody/$FLOW_NONCE" --queue 76 || return 2
    dexec agt1 -- iptables -A FLOWNATS -p tcp --syn --dport 4222 -j NFQUEUE --queue-num 76
}
flow_watchdog_first_commit_fault() {
    # Observe the cut before spending the boot observation deadline. The
    # netns-local watcher applies 0555 while holding the actual first NATS SYN.
    # origin: hot-update S5 plan §9.2 / T48 / DRILL-RUN
    flow_fixture wait-event final-cut 5 || return 2
    flow_fixture wait-event candidate-boot-persisted 2 || return 2
    dexec agt1 -- python3 -I -B /tmp/flow_open_fault.py watchdog-verify --root "/run/tether-drill-custody/$FLOW_NONCE" > "$FLOW_EV/nats-fault.json" || return 2
    FLOW_MODE=$(jq -er '.mode' "$FLOW_EV/nats-fault.json") || return 2
    poll_until 5 1 'first real commit write failed' -- sim_agent_slog_grep agt1 'agent: upgrade commit: marker write failed' "$FLOW_CURSOR" || return 2
    dexec agt1 -- cat "$FLOW_MARKER" > "$FLOW_EV/pending-marker.json" || return 2
    jq -e '.state=="pending" and .boot_count>0' "$FLOW_EV/pending-marker.json" >/dev/null || return 2
    flow_fault_modes_restore || return 2
    _fwcf_ms=$(jq -er '.nats_block_ms' "$FLOW_EV/nats-fault.json") || return 2
    flow_fixture watchdog-fault "$_fwcf_ms" || return 2
    flow_fixture checkpoint ready-pending
}
flow_fault_modes_restore() {
    [ -n "$FLOW_MODE" ] || return 0
    dexec agt1 -- chmod "$FLOW_MODE" "$FLOW_BIN" || return 2
    FLOW_MODE=
}
flow_fault_cleanup() {
    if [ -z "$FLOW_MODE" ] && [ -n "${FLOW_NONCE:-}" ]; then
        FLOW_MODE=$(dexec agt1 -- cat "/run/tether-drill-custody/$FLOW_NONCE/watchdog-mode.json" 2>/dev/null | jq -r '.mode // empty') || FLOW_MODE=
    fi
    flow_fault_modes_restore || warn 'flow fault directory mode restoration failed'
    dexec agt1 -- iptables -F FLOWOPEN >/dev/null 2>&1 || true
    dexec agt1 -- iptables -F FLOWNATS >/dev/null 2>&1 || true
}

# Shared by 75's native negative controls and 39.F. The original oracle rc is
# retained even if classification or cleanup subsequently fails.
flow_legacy_exporter() {
    flow_fixture continuity > "$FLOW_EV/continuity.out" 2>&1
    _fle_rc=$?
    printf '%s\n' "$_fle_rc" > "$FLOW_EV/continuity.rc"
    case "$_fle_rc" in 0) return 0 ;; 1) ;; *) return 2 ;; esac
    flow_fault_valid "$FLOW_EV/legacy-fault.json" || return 2
    jq -e --slurpfile source "$FLOW_EV/legacy-agent.json" '
      .before.running_sha==$source[0].running_sha and
      $source[0].source.tag=="v0.6.0" and
      $source[0].source.commit=="a3431a12be3cf947cc797c28fea93382fa68472d"' "$FLOW_EV/legacy-fault.json" >/dev/null || return 2
    # The raw judge owns the categorical EOF/RST observation. Invalid evidence,
    # a busy refusal, or merely a differing SHA cannot mint PRODUCT-RED.
    jq -e '.original_socket==true and .application_redials==0 and
      (.failure=="early_eof" or .failure=="reset") and .samples_before>=20' \
        "$FLOW_EV/legacy-loss.json" >/dev/null || return 2
    _fle_loss=$(jq -r '.failure' "$FLOW_EV/legacy-loss.json") || return 2
    printf 'HOT_UPDATE_LEGACY_FLOW_EXPORTER original=1 loss=%s valid_fault=1 continuity_rc=1\n' "$_fle_loss"
    return 1
}
flow_legacy_signature() {
    # origin: hot-update S5 plan §9.3 / T48. Only a reviewed observation
    # can pin F; an unobserved broad EOF/RST alternation is not a receipt.
    # Reviewed reset: s5-evidence/drills/run-201/evidence/hot-update-flow.F.sjWwEC/legacy-loss.json.
    # An explicitly empty override keeps the unpinned negative control intact.
    FLOW_LEGACY_EXPECTED_LOSS=${FLOW_LEGACY_EXPECTED_LOSS-reset}
    case "${FLOW_LEGACY_EXPECTED_LOSS:-}" in
        early_eof|reset)
            printf '^HOT_UPDATE_LEGACY_FLOW_EXPORTER original=1 loss=%s valid_fault=1 continuity_rc=1$\n' "$FLOW_LEGACY_EXPECTED_LOSS"
            ;;
        *) return 2 ;;
    esac
}
flow_legacy_assertion() {
    _fla_signature=$(flow_legacy_signature) || setup_fail '39.F requires a reviewed FLOW_LEGACY_EXPECTED_LOSS pin'
    assert_bug 'original legacy flow survives historical bare exec' \
        'hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration' \
        "$_fla_signature" flow_legacy_exporter
}

# origin: hot-update S5 plan §9.1 / T48. Pending buffers alone do not prove
# a kernel partial write or original EINPROGRESS socket existed before the cut.
flow_open_partial_valid() {
    jq -e --arg nonce "$FLOW_NONCE" '
      def positive: type=="number" and .>0 and floor==.;
      (.ss.attempt_len|positive) and (.ss.n|positive) and (.ss.seal_calls|positive) and
      .nonce==$nonce and .open.original_fd==true and .open.phase=="EINPROGRESS" and
      .open.syn_block_observed==true and .open.target_accepts==0 and
      .ss.attempt_len>0 and .ss.n>0 and .ss.n<.ss.attempt_len and
      .ss.cipher_suffix_bytes==(.ss.attempt_len-.ss.n) and
      .ss.seal_calls>0 and .ss.application_redials==0' "$1" >/dev/null
}
