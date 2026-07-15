# drills/lib/artifact.sh — S0-artifact: an HTTPS static artifact server (drills 30/31 upgrade source).
# REUSES ingress-proxy.py (zero new TLS code) fronting a loopback `python3 -m http.server` serving
# /artifacts, with the instance CA REUSED (never re-minted — inventory §3 CA-owner rule: S2 owns mint).
# Depends on lib/docker.sh, lib/secrets.sh, drills/lib/ingress.sh, lib/log.sh + the drill's $HERE/$IMAGE.
#
# `node upgrade --url` / `install.sh --url` want an https RELEASE TARBALL (top-level `tether` binary) whose
# --sha256 is the tarball's sha (usage §5.19). artifact_up packs vendor/tether-next into that shape, computes
# the sha HOST-side, and serves it. The instance-CA leaf (SAN=DNS:<inst>-artifact) makes the front trusted
# once ingress_trust_inject drops the CA into a consumer's system roots — a DEPLOYMENT job (real GitHub's CA
# is in the system roots too). OQ-3 separation: the CA fixes only the TLS layer; the agent-side upgrade
# allow-list (#28) is an ORTHOGONAL second wall — the CA is NEVER used to green a 31 upgrade (agent refuses
# url_not_allowed_local BEFORE the TLS layer). It only makes 30's staging download + 31's broker-side forward
# faithful.

ARTIFACT_HTTP_PORT="${ARTIFACT_HTTP_PORT:-8099}"
ARTIFACT_NODE=artifact-http

# artifact_up <inst> <binary-host-path> <tarball-name> : pack <binary> into a release-style tarball, serve it
# at https://<inst>-artifact/<tarball-name> with a host-computed sha. Sets ART_URL + ART_SHA (globals).
artifact_up() {
    _au_inst=$1; _au_bin=$2; _au_tar=$3
    [ -f "$_au_bin" ] || die "artifact_up: binary not found: $_au_bin (run remote.sh --build for vendor/tether-next)"
    # 1. loopback http backend container on the bridge (network-alias <inst>-artifact for cross-container DNS).
    run d rm -f "$(ctr_name "$ARTIFACT_NODE")" >/dev/null 2>&1 || true
    run d run -d --name "$(ctr_name "$ARTIFACT_NODE")" --hostname "$_au_inst-artifact" \
        --network "$(net_name)" --network-alias "$_au_inst-artifact" \
        --label sim.instance="$_au_inst" --label sim.role=artifact --label sim.nodeid="$ARTIFACT_NODE" \
        --entrypoint sh "$IMAGE" -c "mkdir -p /artifacts && exec python3 -m http.server $ARTIFACT_HTTP_PORT --bind 127.0.0.1 --directory /artifacts" >/dev/null \
        || die "artifact_up: backend container failed"
    # 2. pack a release-style tarball (top-level `tether`) HOST-side, compute sha256, docker cp it in.
    #    fresh/perms precheck: recompute the sha INSIDE the container == host (torn/stale tarball fails loud).
    _au_tmp=$(mktemp -d) || die "artifact_up: mktemp failed"
    cp "$_au_bin" "$_au_tmp/tether"; ( cd "$_au_tmp" && tar czf "$_au_tar" tether )
    ART_SHA=$(sha256sum "$_au_tmp/$_au_tar" | awk '{print $1}')
    d cp "$_au_tmp/$_au_tar" "$(ctr_name "$ARTIFACT_NODE"):/artifacts/$_au_tar" || die "artifact_up: cp tarball failed"
    rm -rf "$_au_tmp"
    _au_incheck=$(dexec "$ARTIFACT_NODE" -- sh -c "sha256sum /artifacts/$_au_tar | awk '{print \$1}'")
    [ "$_au_incheck" = "$ART_SHA" ] || die "artifact_up: in-container sha ($_au_incheck) != host sha ($ART_SHA) — torn tarball"
    # 3. ingress sidecar sharing the backend's netns; instance-CA leaf SAN=DNS:<inst>-artifact; route /=backend.
    secrets_mint_ingress "$_au_inst" "$ARTIFACT_NODE" artifact "$_au_inst-artifact"
    ingress_up "$ARTIFACT_NODE" 443 artifact "/=127.0.0.1:$ARTIFACT_HTTP_PORT"
    ART_URL="https://$_au_inst-artifact/$_au_tar"
    ok "artifact_up: serving $ART_URL (sha256=$ART_SHA)"
}

# artifact_down : reap the artifact backend + its ingress sidecar (trap cleanup; cmd_nuke also reaps by label).
artifact_down() {
    ingress_down "$ARTIFACT_NODE" 443 2>/dev/null || true
    d rm -f "$(ctr_name "$ARTIFACT_NODE")" >/dev/null 2>&1 || true
}
