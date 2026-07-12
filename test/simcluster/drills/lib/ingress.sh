# drills/lib/ingress.sh — S0-ingress fixture: per-broker same-netns HTTPS reverse-proxy sidecar + instance
# test-CA leaf + trust injection + the labeled operator step that enables the loopback manifest listener.
# Sourced by drill 82. Depends on lib/secrets.sh (secrets_ensure_shared/_mint_leaf/_secrets_*_dir),
# lib/docker.sh (d/ctr_name/dexec), lib/tether.sh (sctl), lib/log.sh (poll_until).
#
# WHY (roadmap S2 §2.2 + gotcha #27): the product's well-known manifest (/.well-known/tether/cluster.json)
# is served on a LOOPBACK-ONLY, fail-closed listener (clustermanifest.Bind refuses non-loopback,
# manifest.go:19-27) — and, critically, it is NOT bound at all by default: broker.go:753 gates on
# ManifestAddr!="" and applyClusterSeam (cluster.go:799) omits manifest_listen, so a bare `cluster init`
# leaves the C2 bootstrap-URL onboarding leg dead (gotcha #27). S0-ingress reproduces the PRODUCTION
# posture faithfully: the operator enables manifest_listen (as install.sh's commented skeleton anticipates)
# and fronts it with a TLS-terminating reverse proxy (production Caddy; here a same-netns Python sidecar).
# The product listener is NEVER rebound or weakened (OQ-5); the sidecar only relays bytes to 127.0.0.1.
# Mandate ③/④: provisioning the operator's TLS front + trust anchor, not doing tether's job. The manifest
# is still built+account-signed by the broker and verified by the agent's own Go-x509 fetcher (AdoptDecision).
#
# CA-facility owner rule (inventory §3): the instance ed25519 CA is minted ONCE by secrets_ensure_shared and
# REUSED here (never re-minted — re-minting drifts already-running containers' trust). S2 is the CA owner.

INGRESS_MANIFEST_PORT="${INGRESS_MANIFEST_PORT:-7480}"   # broker loopback manifest_listen port

# secrets_mint_ingress <inst> <node> [leaf=ingress] [san_cn=$node] : mint a per-broker ingress leaf signed
# by the instance CA (reused, never re-minted). san_cn lets a wrong-SAN negative cert be minted (cn=notabroker).
secrets_mint_ingress() {
    _mi_inst=$1; _mi_node=$2; _mi_leaf=${3:-ingress}; _mi_cn=${4:-$2}
    secrets_ensure_shared "$_mi_inst"
    _mi_sh=$(_secrets_shared_dir "$_mi_inst"); _mi_nd=$(_secrets_node_dir "$_mi_inst" "$_mi_node")
    mkdir -p "$_mi_nd"; chmod 700 "$_mi_nd"
    [ -f "$_mi_nd/$_mi_leaf-cert.pem" ] && return 0    # idempotent
    _mint_leaf "$_mi_sh" "$_mi_nd" "$_mi_leaf" "$_mi_cn"
    log "ingress: minted leaf '$_mi_leaf' (SAN=DNS:$_mi_cn) for $_mi_node"
}

# ingress_enable_manifest <brk> : the LABELED OPERATOR STEP (gotcha #27 remediation). Insert
# manifest_listen into the broker.yaml cluster: block (root, as install.sh/an operator would), restart the
# broker, and poll until the loopback listener ANSWERS (200 or 503 — bound, not connection-refused).
# Drill 82 MUST first assert the default-off gap (bare curl → refused) BEFORE calling this.
ingress_enable_manifest() {
    _em_brk=$1
    dexec "$_em_brk" -- sh -c "grep -qE '^[[:space:]]*manifest_listen:' /etc/tether/broker.yaml || sed -i -E '/^  cluster:/a\\    manifest_listen: \"127.0.0.1:$INGRESS_MANIFEST_PORT\"' /etc/tether/broker.yaml" \
        || die "ingress_enable_manifest: could not edit broker.yaml on $_em_brk"
    sctl "$_em_brk" restart tether-broker
    poll_until 30 2 "$_em_brk manifest listener bound (loopback answers, not refused)" -- _ingress_manifest_answers "$_em_brk" \
        || die "ingress_enable_manifest: $_em_brk manifest listener never came up (check: $SIM logs $_em_brk tether-broker)"
    ok "ingress: manifest_listen enabled on $_em_brk (labeled operator step; gotcha #27 workaround)"
}

# _ingress_manifest_answers <brk> : predicate — the broker's loopback manifest listener ANSWERS (any HTTP
# code, incl 503; NOT connection-refused). curl exit 7 = refused (no listener).
_ingress_manifest_answers() {
    _code=$(dexec "$1" -- curl -s -o /dev/null -w '%{http_code}' --max-time 3 \
        "http://127.0.0.1:$INGRESS_MANIFEST_PORT/.well-known/tether/cluster.json" 2>/dev/null)
    [ -n "$_code" ] && [ "$_code" != "000" ]
}

# ingress_up <brk> [listen_port=443] [leaf=ingress] : start the same-netns HTTPS reverse-proxy sidecar. It
# shares the broker's netns (--network container:<brk>) so it is addressable as https://<brk>:<port> from
# peers AND reaches the broker's 127.0.0.1:<manifest_listen>. Labeled so cmd_down/cmd_nuke reap it.
ingress_up() {
    _iu_brk=$1; _iu_port=${2:-443}; _iu_leaf=${3:-ingress}
    _iu_nd=$(_secrets_node_dir "$INSTANCE" "$_iu_brk")
    [ -f "$_iu_nd/$_iu_leaf-cert.pem" ] || die "ingress_up: leaf '$_iu_leaf' not minted for $_iu_brk (call secrets_mint_ingress first)"
    _iu_name=$(ctr_name "$_iu_brk-ingress-$_iu_port")
    d rm -f "$_iu_name" >/dev/null 2>&1 || true
    run d run -d --name "$_iu_name" \
        --network "container:$(ctr_name "$_iu_brk")" --stop-signal SIGTERM \
        --label sim.instance="$INSTANCE" --label sim.role=ingress --label sim.nodeid="$_iu_brk-ingress-$_iu_port" \
        -v "$_iu_nd:/ingress-secrets:ro" \
        --entrypoint python3 "$IMAGE" \
        /opt/sim/ingress-proxy.py --listen ":$_iu_port" \
        --cert "/ingress-secrets/$_iu_leaf-cert.pem" --key "/ingress-secrets/$_iu_leaf-key.pem" \
        --route "/.well-known/tether/=127.0.0.1:$INGRESS_MANIFEST_PORT" >/dev/null \
        || die "ingress_up: could not start sidecar for $_iu_brk:$_iu_port"
    # readiness: from a THROWAWAY bridge container, curl the https front with the instance CA trusted.
    poll_until 20 2 "$_iu_brk:$_iu_port https ingress up" -- _ingress_front_up "$_iu_brk" "$_iu_port" \
        || die "ingress_up: $_iu_brk:$_iu_port https front never answered (check: docker logs $_iu_name)"
    ok "ingress: HTTPS front up on $_iu_brk:$_iu_port (same-netns → 127.0.0.1:$INGRESS_MANIFEST_PORT)"
}

# _ingress_front_up <brk> <port> : predicate — the https front is up + TLS-handshakes (any HTTP code). Curls
# the sidecar from INSIDE the broker container (the sidecar shares the broker netns, so 127.0.0.1:<port> is
# the front) — no throwaway container per poll iteration (R20: those contend under the parallel wave). Uses
# `curl -k` (insecure): readiness must NOT depend on cert CORRECTNESS — a wrong-SAN negative front
# (I-NEG-SAN) is a VALID running front whose cert the drill then deliberately fails to verify (J4/M2/I-NEG-*).
_ingress_front_up() {
    _code=$(dexec "$1" -- curl -sk -o /dev/null -w '%{http_code}' --max-time 4 \
        "https://127.0.0.1:$2/.well-known/tether/cluster.json" 2>/dev/null)
    [ -n "$_code" ] && [ "$_code" != "000" ]
}

# ingress_down <brk> [port=443] : remove a sidecar (best-effort; cmd_nuke also reaps via labels).
ingress_down() {
    _id_brk=$1; _id_port=${2:-443}
    d rm -f "$(ctr_name "$_id_brk-ingress-$_id_port")" >/dev/null 2>&1 || true
}

# ingress_trust_inject <node> [inst=$INSTANCE] : drop the instance CA into <node>'s system trust store so
# the agent's Go-x509 fetcher (fetch.go uses the system roots, no custom RootCAs) trusts the sim chain.
# Deployment job (Mandate ③) — real GitHub's CA is in the system roots too.
ingress_trust_inject() {
    _ti_node=$1; _ti_inst=${2:-$INSTANCE}
    _ti_sh=$(_secrets_shared_dir "$_ti_inst")
    d cp "$_ti_sh/cluster-ca.pem" "$(ctr_name "$_ti_node"):/usr/local/share/ca-certificates/tether-sim-ca.crt" \
        || die "ingress_trust_inject: could not copy CA into $_ti_node"
    dexec "$_ti_node" -- update-ca-certificates >/dev/null 2>&1 \
        || die "ingress_trust_inject: update-ca-certificates failed on $_ti_node"
    log "ingress: instance CA injected into $_ti_node system trust store"
}
