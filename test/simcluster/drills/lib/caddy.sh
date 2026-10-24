# drills/lib/caddy.sh — real-Caddy H0 provisioning.
# Requires lib/docker.sh, lib/tether.sh, lib/log.sh and drills/lib/ingress.sh.
# Call only during initial setup, before original-flow READY. This is not an upgrade implementation.

caddy_wss_url() { printf 'wss://%s:443/nats\n' "$1"; }
caddy_tunnel_addr() { printf '%s:7000\n' "$1"; }

# origin: hot-update S10 plan §2.1 / §10.2 P11. Provision once, before READY;
# permissions and service lifecycle afterwards belong to S9's product steps.
caddy_before_ready() {
    if [ -n "${FULLSTACK_READY_FILE:-}" ]; then
        [ ! -e "$FULLSTACK_READY_FILE" ] && [ ! -L "$FULLSTACK_READY_FILE" ] || return 2
    fi
    if [ "${FULLSTACK_PROFILE:-0}" = 1 ]; then
        [ -n "${FULLSTACK_REMOTE_READY_FILE:-}" ] || return 2
        dexec "$1" -- /usr/local/bin/tether-test-supervise -- /usr/bin/test ! -e "$FULLSTACK_REMOTE_READY_FILE" || return 2
    fi
}

caddy_prepare() (
    set -eu
    _cp_node=$1
    case "$_cp_node" in ''|*[!a-z0-9-]*) die "caddy_prepare: expected sim node hostname" ;; esac
    caddy_before_ready "$_cp_node" || exit 2
    _cp_dir=$(_secrets_node_dir "$INSTANCE" "$_cp_node") || exit 1
    secrets_mint_ingress "$INSTANCE" "$_cp_node" ingress "$_cp_node" || exit 1
    # Fail rather than replace an existing ingress sidecar on the same instance/port.
    if d inspect "$(ctr_name "$_cp_node-ingress-443")" >/dev/null 2>&1; then
        die "caddy_prepare: existing ingress sidecar owns the Caddy test port"
    fi
    dexec "$_cp_node" -- test -x /usr/local/bin/caddy || die "image missing true Caddy artifact"
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
    d cp "$SIM_UPGRADE_SUPERVISOR" "$(ctr_name "$_cp_node"):/usr/local/bin/tether-test-supervise" || exit 1
    # Consume the installed unit; do not rewrite S9's User/capability/launcher
    # contract. Root legacy and managed tether units need different key access.
    _cp_user=$(dexec "$_cp_node" -- /usr/local/bin/tether-test-supervise -- /usr/bin/systemctl show caddy.service -p User --value) || exit 1
    case "$_cp_user" in
        ''|root) _cp_user=root; _cp_group=root; _cp_dir_mode=0700; _cp_key_mode=0600 ;;
        tether) _cp_group=tether; _cp_dir_mode=0750; _cp_key_mode=0640 ;;
        *) die "caddy_prepare: unsupported installed service user" ;;
    esac
    # A legacy root unit requires S9's explicit retrofit lane; fixture chown
    # cannot turn that lifecycle into a managed main-lane receipt.
    [ "${FULLSTACK_PROFILE:-0}" != 1 ] || [ "$_cp_user" = tether ] || exit 2
    dexec "$_cp_node" -- install -d -o root -g "$_cp_group" -m "$_cp_dir_mode" /etc/tether/caddy || exit 1
    d cp "$_cp_dir/ingress-cert.pem" "$(ctr_name "$_cp_node"):/etc/tether/caddy/cert.pem" || exit 1
    d cp "$_cp_dir/ingress-key.pem" "$(ctr_name "$_cp_node"):/etc/tether/caddy/key.pem" || exit 1
    dexec "$_cp_node" -- chown "root:$_cp_group" /etc/tether/caddy/cert.pem /etc/tether/caddy/key.pem || exit 1
    dexec "$_cp_node" -- chmod 0644 /etc/tether/caddy/cert.pem || exit 1
    dexec "$_cp_node" -- chmod "$_cp_key_mode" /etc/tether/caddy/key.pem || exit 1
    if [ "${FULLSTACK_PROFILE:-0}" = 1 ]; then
        [ -n "${FULLSTACK_CA_FILE:-}" ] && [ -n "${FULLSTACK_CERT_SHA:-}" ] || exit 2
        dexec "$_cp_node" -- /usr/local/bin/tether-test-supervise -- /usr/bin/python3 -I -B \
            /tmp/fullstack_provision.py certificate --cert /etc/tether/caddy/cert.pem \
            --key /etc/tether/caddy/key.pem --ca "$FULLSTACK_CA_FILE" --hostname "$_cp_node" \
            --sha "$FULLSTACK_CERT_SHA" --user "$_cp_user" || exit 2
    fi
    _cp_tmp=$(mktemp) || exit 1
    trap 'rm -f "$_cp_tmp"' EXIT HUP INT TERM
    if [ "${FULLSTACK_PROFILE:-0}" = 1 ]; then
        # S9 owns the complete route/admin configuration. Its sealed initial
        # rendering may contain well-known/manifest routes absent from H0.
        [ -n "${FULLSTACK_CADDY_CONFIG:-}" ] && [ -n "${FULLSTACK_CADDY_CONFIG_SHA:-}" ] || exit 2
        cp "$FULLSTACK_CADDY_CONFIG" "$_cp_tmp" || exit 2
        _cp_config_sha=$(sha256sum "$_cp_tmp" | awk '{print $1}')
        [ "$_cp_config_sha" = "$FULLSTACK_CADDY_CONFIG_SHA" ] || exit 2
    else
        cat > "$_cp_tmp" <<EOF
$_cp_node:443 {
    tls /etc/tether/caddy/cert.pem /etc/tether/caddy/key.pem
    handle /sub/* {
        reverse_proxy 127.0.0.1:8090
    }
    handle {
        reverse_proxy 127.0.0.1:8222
    }
}
EOF
    fi
    d cp "$_cp_tmp" "$(ctr_name "$_cp_node"):/etc/tether/Caddyfile" || exit 1
    dexec "$_cp_node" -- chown root:root /etc/tether/Caddyfile || exit 1
    dexec "$_cp_node" -- chmod 0644 /etc/tether/Caddyfile || exit 1
    # No unit replacement: production installer wrote the real caddy.service.
    dexec "$_cp_node" -- /usr/local/bin/caddy validate --config /etc/tether/Caddyfile --adapter caddyfile || exit 1
)

caddy_start() {
    _cs_node=$1
    caddy_before_ready "$_cs_node" || return 2
    sctl "$_cs_node" enable --now caddy || return 1
    poll_until 20 1 "$_cs_node real Caddy unit active" -- sctl "$_cs_node" is-active --quiet caddy
}

# First call ingress_trust_inject on ctl/agents before starting those processes.
# Successful authenticated request proves actual WSS, not merely HTTPS liveness.
caddy_probe_wss() {
    _cw_ctl=$1; _cw_broker=$2
    if [ "${FULLSTACK_PROFILE:-0}" = 1 ]; then
        # The four ctl homes are part of the runtime roster; a successful lab
        # session query is not evidence that any required SID can use WSS.
        _cw_runtime=${3:-}
        case "$_cw_runtime" in a0|a1) _cw_host=agt1 ;; a2|a3) _cw_host=agt2 ;; *) return 2 ;; esac
        [ "$_cw_ctl" = "$_cw_host" ] || return 2
        dexec -u "sim-$_cw_runtime" "$_cw_ctl" -- /usr/local/bin/tether-test-supervise -- \
            /usr/bin/env "HOME=/home/sim-$_cw_runtime/ctl" "/opt/s10/$_cw_runtime/tether" \
            node ls --json --nats-url "$(caddy_wss_url "$_cw_broker")"
        return $?
    fi
    dexec -u sim "$_cw_ctl" -- env HOME=/home/sim \
        tether node ls --json --nats-url "$(caddy_wss_url "$_cw_broker")"
}

# SHA is supplied from frozen vendor/caddy-source.json. Caller records returned PID/SHA/version.
caddy_runtime_receipt() {
    _cr_node=$1; _cr_expected=$2
    _cr_pid=$(sctl "$_cr_node" show caddy -p MainPID --value)
    case "$_cr_pid" in ''|0|*[!0-9]*) return 1 ;; esac
    _cr_sha=$(dexec "$_cr_node" -- sha256sum "/proc/$_cr_pid/exe" | awk '{print $1}')
    [ "$_cr_sha" = "$_cr_expected" ] || return 1
    printf 'CADDY_RUNTIME node=%s pid=%s sha256=%s\n' "$_cr_node" "$_cr_pid" "$_cr_sha"
    dexec "$_cr_node" -- "/proc/$_cr_pid/exe" version
}
