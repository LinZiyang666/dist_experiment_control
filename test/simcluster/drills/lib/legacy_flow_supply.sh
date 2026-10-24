#!/bin/sh
# origin: hot-update S5 plan §4.1 / §9.3 / T48.
# The fixed historical exporter and dynamic N-1 are separate supplies. Call
# broker installation before session/expose/proxy creation, never just swap ctl.
# Depends on stage.sh, docker.sh and the caller's isolated instance.

legacy_flow_running_sha() {
    _lrs_raw=$(dexec "$1" -- sha256sum "/proc/$2/exe") || return 2
    printf '%s\n' "$_lrs_raw" | awk 'length($1)==64 && $1 !~ /[^0-9a-f]/ {print $1; found=1} END {if (!found) exit 2}'
}

legacy_flow_broker_install() {
    sim_flow_legacy_source_ok || return 2
    # Restart is provisioning, before any original cohort exists. Preserve the
    # existing configuration and validate it with the actual old reader.
    sctl brk1 stop tether-broker || return 2
    d cp "$SIM_VENDOR/tether-flow-legacy" "$(ctr_name brk1):/usr/local/bin/tether-prev" || return 2
    dexec brk1 -- sh -eu -c '
        chmod 0755 /usr/local/bin/tether-prev
        /usr/local/bin/tether-prev serve --config /etc/tether/broker.yaml --config-check
        mkdir -p /etc/systemd/system/tether-broker.service.d
        printf "[Service]\nExecStart=\nExecStart=/usr/local/bin/tether-prev serve --config /etc/tether/broker.yaml\n" > /etc/systemd/system/tether-broker.service.d/flow-legacy.conf
        systemctl daemon-reload
    ' || return 2
    sctl brk1 start tether-broker || return 2
    poll_until 30 1 'fixed historical broker running' -- legacy_flow_broker_verify
}

legacy_flow_broker_verify() {
    sim_flow_legacy_source_ok || return 2
    _lfb_pid=$(dexec brk1 -- systemctl show tether-broker -p MainPID --value) || return 2
    case "$_lfb_pid" in ''|*[!0-9]*|0|1) return 2 ;; esac
    _lfb_unit=$(dexec brk1 -- systemctl show tether-broker -p ExecStart --value) || return 2
    case "$_lfb_unit" in *'path=/usr/local/bin/tether-prev ;'*) ;; *) return 2 ;; esac
    _lfb_sha=$(legacy_flow_running_sha brk1 "$_lfb_pid") || return 2
    _lfb_expected=$(sha256sum "$SIM_VENDOR/tether-flow-legacy" | awk '{print $1}') || return 2
    [ "$_lfb_sha" = "$_lfb_expected" ] || return 2
    jq -n --arg sha "$_lfb_sha" --argjson pid "$_lfb_pid" --arg unit "$_lfb_unit" \
        --slurpfile source "$SIM_VENDOR/tether-flow-legacy-source.json" \
        '{role:"broker",pid:$pid,running_sha:$sha,unit_exec_start:$unit,source:$source[0]}' > "$FLOW_EV/legacy-broker.json"
}

legacy_flow_agent_install() {
    sim_flow_legacy_source_ok || return 2
    sctl agt1 stop tether-agent || return 2
    d cp "$SIM_VENDOR/tether-flow-legacy" "$(ctr_name agt1):$FLOW_DST" || return 2
    dexec agt1 -- chown sim:sim "$FLOW_DST" || return 2
    dexec agt1 -- chmod 0755 "$FLOW_DST" || return 2
    sctl agt1 start tether-agent || return 2
    poll_until 60 1 'fixed historical exporter online' -- flow_fixture online || return 2
    _lfa_pid=$(dexec agt1 -- systemctl show tether-agent -p MainPID --value) || return 2
    _lfa_sha=$(legacy_flow_running_sha agt1 "$_lfa_pid") || return 2
    _lfa_expected=$(sha256sum "$SIM_VENDOR/tether-flow-legacy" | awk '{print $1}') || return 2
    [ "$_lfa_sha" = "$_lfa_expected" ] || return 2
    jq -n --arg sha "$_lfa_sha" --argjson pid "$_lfa_pid" \
        --slurpfile source "$SIM_VENDOR/tether-flow-legacy-source.json" \
        '{role:"agent",pid:$pid,running_sha:$sha,source:$source[0]}' > "$FLOW_EV/legacy-agent.json"
}

# Evidence must contain actual negotiation and raw-byte observations, bound to
# the original cohort. Merely running an old CLI proves neither of these facts.
legacy_flow_raw_valid() {
    jq -e --slurpfile broker "$FLOW_EV/legacy-broker.json" '
      .broker_running_sha==$broker[0].running_sha and .alpn=="" and
      .raw_prefix_verified==true and .flow_header_seen==false and
      .before_first_business==true and .original_connections>0 and
      .samples_each_direction>=20' "$FLOW_EV/legacy-transport.json" >/dev/null
}
