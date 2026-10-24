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
    ART_BASE_URL="https://$_au_inst-artifact"
    artifact_add "$_au_bin" "$_au_tar" || return 1
    ART_SHA=$ART2_SHA
    # 3. ingress sidecar sharing the backend's netns; instance-CA leaf SAN=DNS:<inst>-artifact; route /=backend.
    secrets_mint_ingress "$_au_inst" "$ARTIFACT_NODE" artifact "$_au_inst-artifact"
    ingress_up "$ARTIFACT_NODE" 443 artifact "/=127.0.0.1:$ARTIFACT_HTTP_PORT"
    ART_URL="https://$_au_inst-artifact/$_au_tar"
    ok "artifact_up: serving $ART_URL (sha256=$ART_SHA)"
}

# artifact_add <binary-host-path> <tarball-name> — add supply to the SAME backend.
# origin: hot-update S3 plan §8.2. G's reverse hop must not restart the server
# or remint trust; both hashes are verified exactly as artifact_up's first one.
artifact_add() {
    _aa_bin=$1; _aa_tar=$2
    [ -f "$_aa_bin" ] && [ -n "${ART_BASE_URL:-}" ] || return 1
    case "$_aa_tar" in ''|*[!a-zA-Z0-9._-]*) return 1 ;; esac
    _aa_tmp=$(mktemp -d) || return 1
    if ! cp "$_aa_bin" "$_aa_tmp/tether" || ! (cd "$_aa_tmp" && tar czf "$_aa_tar" tether); then
        rm -rf "$_aa_tmp"; return 1
    fi
    ART2_SHA=$(sha256sum "$_aa_tmp/$_aa_tar" | awk '{print $1}')
    if ! d cp "$_aa_tmp/$_aa_tar" "$(ctr_name "$ARTIFACT_NODE"):/artifacts/$_aa_tar"; then
        rm -rf "$_aa_tmp"; return 1
    fi
    rm -rf "$_aa_tmp"
    _aa_check=$(dexec "$ARTIFACT_NODE" -- sha256sum "/artifacts/$_aa_tar" | awk '{print $1}')
    [ "$_aa_check" = "$ART2_SHA" ] || return 1
    ART2_URL="$ART_BASE_URL/$_aa_tar"
}

# artifact_down : reap the artifact backend + its ingress sidecar (trap cleanup; cmd_nuke also reaps by label).
artifact_down() {
    ingress_down "$ARTIFACT_NODE" 443 2>/dev/null || true
    d rm -f "$(ctr_name "$ARTIFACT_NODE")" >/dev/null 2>&1 || true
}

# origin: hot-update S5 plan §5.2 / §9.5 / T48. B/W need two real readers,
# with SHA-bound capability declarations, not two filenames for the same bytes.
artifact_flow_readers_ok() (
    _afr_a=$1; _afr_b=$2; _afr_out=$3
    _afr_tmp=$(mktemp -d) || return 2
    trap 'rm -rf "$_afr_tmp"' EXIT HUP INT TERM
    for _afr_side in a b; do
        if [ "$_afr_side" = a ]; then _afr_bin=$_afr_a; else _afr_bin=$_afr_b; fi
        [ -x "$_afr_bin" ] || return 2
        # Bound disk output as well as elapsed time; validation below applies
        # the exact 64KiB reader limit (ulimit units differ between shells).
        (ulimit -f 128; timeout 5 "$_afr_bin" version > "$_afr_tmp/$_afr_side.version") || return 2
        sha256sum "$_afr_bin" > "$_afr_tmp/$_afr_side.sha" || return 2
    done
    python3 - "$_afr_tmp" "$_afr_out" <<'PYREADERS'
import json, pathlib, re, sys
try:
    root = pathlib.Path(sys.argv[1]); readers = []
    for side in ('a', 'b'):
        data = (root/(side+'.version')).read_bytes()
        if len(data)>65536: raise ValueError('version output exceeds limit')
        lines = data.decode().splitlines()
        if not lines or not re.fullmatch(r'tether \S+ \(proto v[1-9][0-9]*\)', lines[0]):
            raise ValueError('invalid release line')
        if lines.count('image-abi v1') != 1 or lines[-1] != 'image-abi v1':
            raise ValueError('invalid image ABI declaration')
        domain = [line for line in lines if line.startswith('image-domains')]
        if domain != ['image-domains exec-v1 pty-v1 flow-v1 ss-v1']:
            raise ValueError('missing flow/SS reader domains')
        readers.append(dict(sha256=(root/(side+'.sha')).read_text().split()[0], version=lines[0], domains=domain[0]))
    if readers[0]['sha256']==readers[1]['sha256']: raise ValueError('identical artifacts')
    pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(readers=readers), sort_keys=True)+'\n')
except (OSError, UnicodeError, ValueError, IndexError) as exc:
    print('flow reader supply rejected: '+str(exc), file=sys.stderr)
    sys.exit(2)
PYREADERS
)

# Receipt publication consumes immutable oracle files after collection. Digest
# paths as well as contents so swapped roles cannot share a receipt fingerprint.
artifact_flow_receipt() {
    python3 - "$FLOW_EV" "$SIM_VENDOR/tether-source.json" "$1" "$2" "$3" "$4" "${5:-75-hot-update-flows.$1}" <<'PYRECEIPT'
import hashlib, json, pathlib, sys, time
root, source, arm, native_rc, verdict, started, command = sys.argv[1:]
root = pathlib.Path(root); source = json.loads(pathlib.Path(source).read_text())
# origin: hot-update S5 plan §9.4. The byte oracle's rc1 is distinct from
# assert_bug's native PRODUCT-RED rc3; keeping one as the other forges a receipt.
verdict_rc = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
h = hashlib.sha256()
for path in sorted(root.rglob('*')):
    if not path.is_file() or path.name=='receipt.json': continue
    name = str(path.relative_to(root)).encode()
    h.update(len(name).to_bytes(8,'big')); h.update(name)
    with path.open('rb') as f: h.update(hashlib.file_digest(f,'sha256').digest())
identity = json.loads((root/'identity-final.json').read_text())
record = dict(source_head=source['source_head'], dirty_patch_sha=source['dirty_patch_sha'],
              build_flags=source['build_flags'], artifact_sha=source['tether_next_sha256'],
              running_sha=identity['running_sha'], role='agent', command=command,
              native_rc=verdict_rc[verdict], continuity_oracle_rc=int(native_rc), verdict=verdict, wall_s=time.monotonic()-float(started),
              oracle_inputs_sha=h.hexdigest(), evidence_path=str(root), status='COMPLETE')
(root/'receipt.json').write_text(json.dumps(record, sort_keys=True)+'\n')
PYRECEIPT
}
