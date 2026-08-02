# drills/lib/upgradecfg.sh — shared `node upgrade` allow-list deployment config (drills 31 + 33).
# POSIX sh. Depends on lib/docker.sh (dexec/sctl), lib/log.sh, the caller's poll_until, and drill vars
# $INSTANCE / $SID. Extracted VERBATIM from 31-node-upgrade-fleet.sh (upgrade-safety follow-up #2) so the
# success drill cannot drift from the fleet drill's labeled operator-config steps; 31's behavior is
# unchanged by the extraction.
#
# Both functions are LABELED DEPLOYMENT CONFIG (Mandate ③ — the operator's own job when self-hosting a
# release mirror), never compensation for a tether gap. Rationale comments preserved from 31.

# upgradecfg_allow_broker <broker-node> <ready-probe-fn>
# NON-destructive APPEND (indent 2, sibling of broker.cluster). A yaml round-trip rewrite dropped
# install.sh's cluster seam and bricked the broker (session create → no active session). broker.yaml is one
# top-level `broker:` map, so a 2-space-indented `upgrade:` block appended at EOF lands under broker:.
# TWO prefixes: `-artifact/` is the served release; `-mirror/` is a DELIBERATELY WIDER broker entry with no
# matching agent entry, so agent-gate probes can present a broker-allowed-but-agent-denied URL that reaches
# the AGENT url gate. An off-band URL matches NEITHER prefix → still broker-refused.
upgradecfg_allow_broker() {
    _uab_node=$1; _uab_probe=$2
    dexec "$_uab_node" -- sh -c "grep -q 'url_allow' /etc/tether/broker.yaml || printf '  upgrade:\n    url_allow:\n      - https://%s-artifact/\n      - https://%s-mirror/\n' '$INSTANCE' '$INSTANCE' >> /etc/tether/broker.yaml" \
        && sctl "$_uab_node" restart tether-broker \
        && poll_until 30 2 "$_uab_node broker back after url_allow config" -- "$_uab_probe"
}

# upgradecfg_allow_agent <agent-node> <ready-probe-fn> [yaml-subdir]
# Configure the AGENT's OWN upgrade URL allow-list — the #28 FIX (cmd/tether/agent.go
# resolveAgentUpgradeAllow + internal/agent/upgrade.go, which consumes Config.UpgradeURLAllowlist).
# agent-join runs the daemon FLAGFUL (--session/--nid/--nats-url) but with NO --upgrade-url-allow, and the
# daemon ALWAYS loads agent.yaml — so the yaml's top-level `upgrade.url_allow` is the ONLY lever a
# systemd-managed agent has, and it is exactly what an operator self-hosting a mirror must set. agent-join
# writes NO agent.yaml, so this CREATEs a minimal one (only the upgrade block; every other value stays on
# the unit's flags — a lone `upgrade:` map loads clean under the strict-KnownFields loader). Written as
# user sim so it stays sim:sim 0600 under the 0700 agent/<sid> tree (install.sh shape). The agent list
# carries ONLY the `-artifact/` prefix (NOT the broker's wider `-mirror/`) so agent-gate probes can hit
# the AGENT gate with a broker-allowed-but-agent-denied URL. Then restart + poll ONLINE so the NEW
# yaml-driven daemon is serving before any upgrade arm runs.
# SIGNATURE: <agent-node> <ready-probe-fn> [yaml-subdir=$SID] [home=/home/sim/.tether] [unit=tether-agent]
# $3/$4/$5 are drill 33's additions (internal review DOC-7): a shared-binary sibling instance lives in
# the SAME container under its own HOME tree and its own systemd unit, so the caller must be able to
# say which. Drill 31 passes none of them and is byte-identical to its pre-extraction behavior.
upgradecfg_allow_agent() {
    _uaa_node=$1; _uaa_probe=$2; _uaa_dir=${3:-$SID}
    _uaa_home=${4:-/home/sim/.tether}
    _uaa_unit=${5:-tether-agent}
    dexec "$_uaa_node" -- runuser -u sim -- sh -c "
        set -e
        install -d -m 0700 $_uaa_home/agent/$_uaa_dir
        f=$_uaa_home/agent/$_uaa_dir/agent.yaml
        grep -q 'url_allow' \"\$f\" 2>/dev/null || {
            printf 'upgrade:\n  url_allow:\n    - https://%s-artifact/\n' '$INSTANCE' >> \"\$f\"
            chmod 600 \"\$f\"
        }" \
        && sctl "$_uaa_node" restart "$_uaa_unit" \
        && poll_until 30 2 "$_uaa_node back ONLINE after agent.upgrade.url_allow config" -- "$_uaa_probe"
}
