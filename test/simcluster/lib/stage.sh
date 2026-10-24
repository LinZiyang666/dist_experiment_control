# lib/stage.sh — vendor staging shared by the two drivers. POSIX-ish sh.
# Sourced, never executed. No side effects at source time.
#
# WHY SHARED: the nats-server pin is read out of the REAL scripts/install.sh (never a hardcoded
# literal that can silently drift from what the fleet runs — that drift would defeat the whole
# tool). A second copy of this logic in local.sh would be exactly the kind of drift this guards
# against, so both remote.sh (external driver) and local.sh (on-host) call in here.
#
# Callers set before calling:
#   SIM_REPO    — repo root (has scripts/install.sh, Makefile)
#   SIM_VENDOR  — test/simcluster/vendor (created if missing)
#   SIM_TAG     — log prefix, e.g. "remote" / "local"

sim_stage_log() { printf '[%s] %s\n' "${SIM_TAG:-stage}" "$*" >&2; }

# Echo the nats-server version pinned by the production installer, e.g. "v2.10.22".
sim_nats_version() {
    _v="$(grep -oE 'NATS_SERVER_VERSION[^0-9]*[0-9]+\.[0-9]+\.[0-9]+' "$SIM_REPO/scripts/install.sh" \
          | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
    [ -n "$_v" ] || { sim_stage_log "could not read NATS_SERVER_VERSION from install.sh"; return 1; }
    printf 'v%s\n' "$_v"
}

# Build tether (+ tether-next) and stage every baked binary into $SIM_VENDOR.
sim_stage_binaries() (
    # A refused supervisor launch must invalidate yesterday's success too.
    rm -f "${SIM_VENDOR:?}/tether-source.json" "$SIM_VENDOR/infra/fullstack/catalog.json" || exit 1
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        sim_stage_log "shared process supervisor is required"; exit 2;
    }
    export SIM_REPO SIM_VENDOR SIM_TAG SIM_UPGRADE_SUPERVISOR
    export SIM_VERSION SIM_VERSION_NEXT SIM_STAGE_LIB
    export SIM_FULLSTACK_A_REF SIM_FULLSTACK_B_REF SIM_FULLSTACK_VERSION_PREFIX
    export SIM_INFRA_PREVIOUS_REF
    exec "$SIM_UPGRADE_SUPERVISOR" -- sh -c '. "$SIM_REPO/test/simcluster/lib/stage.sh"; sim_stage_binaries_supervised'
)

sim_stage_binaries_supervised() (
    # origin: S8 internal review rv-n1 F3
    # Explicit status checks also work when the caller uses this in an if/||
    # list (which disables errexit). Publish success only for this invocation.
    _sb_done=0 _sb_build=
    trap '_sb_rc=$?; if [ "$_sb_done" != 1 ] || [ "$_sb_rc" -ne 0 ]; then
        rm -f "${SIM_VENDOR:?}/tether-source.json" "$SIM_VENDOR/infra/fullstack/catalog.json" \
              "$SIM_VENDOR/tether" "$SIM_VENDOR/tether-next" || _sb_rc=1
    fi
    if [ -n "$_sb_build" ]; then rm -rf "$_sb_build" || _sb_rc=1; fi
    exit "$_sb_rc"' 0
    mkdir -p "${SIM_VENDOR:?}" || return 1
    command -v go >/dev/null 2>&1 || {
        sim_stage_log "no Go toolchain on PATH — needed to build tether. Install Go, or drive from a box that has it via ./remote.sh --build"
        return 1
    }
    nats_version="$(sim_nats_version)" || return 1
    # The identity file is rewritten at the END on success; until then there is none. A staging
    # that fails half-way (tether rebuilt, tether-prev build failed) must not leave the previous
    # staging's tether-source.json beside binaries it does not describe (S1 review D6).
    # origin: hot-update S10 plan §2.1 / P10. The S10 catalog is also a
    # success pointer; an early upstream build failure must invalidate it.
    rm -f "$SIM_VENDOR/tether-source.json" "$SIM_VENDOR/infra/fullstack/catalog.json" || return 1
    # Real Caddy is machine provisioning for H0; shared by local/remote drivers.
    . "$SIM_REPO/test/simcluster/lib/stage-caddy.sh" || return 1
    sim_stage_caddy || return 1
    _sb_build=$(mktemp -d) || return 1
    _sb_source=$(sim_build_source_identity) || return 1

    sim_stage_log "building static tether (CGO_ENABLED=0 amd64)…"
    ( cd "$SIM_REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build BIN="$_sb_build/current" VERSION="${SIM_VERSION:-v0.0.0-simcluster}" ) || return 1
    [ -x "$_sb_build/current" ] || return 1
    cp "$_sb_build/current" "$SIM_VENDOR/tether" || return 1
    _sb_current_sha=$(sim_artifact_sha "$SIM_VENDOR/tether") || return 1

    # S5 / s3-s5-plan §1.E: a SECOND build with ONLY the version string bumped → vendor/tether-next, the
    # staged "new" binary drills 30/31 upgrade to. SAME source / proto / commandVersion / schema (g5
    # rolling-safety hard precondition — a real proto/command delta is an incompatible reinstall, not a
    # rolling upgrade). artifact.sh serves it with a host-computed SHA256SUMS; 30 stages it per-host, 31
    # serves it as the self-hosted mirror the agent allow-list (#28) refuses.
    sim_stage_log "building tether-next (VERSION=${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}, same source)…"
    ( cd "$SIM_REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build BIN="$_sb_build/next" VERSION="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}" ) || return 1
    [ -x "$_sb_build/next" ] || return 1
    cp "$_sb_build/next" "$SIM_VENDOR/tether-next" || return 1
    _sb_next_sha=$(sim_artifact_sha "$SIM_VENDOR/tether-next") || return 1
    [ "$_sb_current_sha" != "$_sb_next_sha" ] || return 1

    # nats-server pinned to the production install.sh version (NOT go.mod's embedded newer one).
    if [ ! -x "$SIM_VENDOR/nats-server" ] \
       || ! "$SIM_VENDOR/nats-server" --version 2>/dev/null | grep -q "${nats_version#v}"; then
        sim_stage_log "fetching nats-server $nats_version…"
        tmp="$_sb_build/nats"
        mkdir -p "$tmp" || return 1
        curl -fsSL -o "$tmp/n.tgz" \
          "https://github.com/nats-io/nats-server/releases/download/${nats_version}/nats-server-${nats_version}-linux-amd64.tar.gz" || return 1
        tar xzf "$tmp/n.tgz" -C "$tmp" || return 1
        cp "$tmp/nats-server-${nats_version}-linux-amd64/nats-server" "$SIM_VENDOR/nats-server" || return 1
        rm -rf "$tmp" || return 1
    fi

    _sb_nats_version=$("$SIM_VENDOR/nats-server" --version) || return 1
    printf '%s\n' "$_sb_nats_version" | grep -q "${nats_version#v}" || return 1

    # nats + nk CLIs (host build == amd64 == sim host). Cached.
    if [ ! -x "$SIM_VENDOR/nats" ]; then
        sim_stage_log "go install nats CLI…"
        GOBIN="$SIM_VENDOR" go install github.com/nats-io/natscli/nats@latest || return 1
    fi
    if [ ! -x "$SIM_VENDOR/nk" ]; then
        sim_stage_log "go install nk…"
        GOBIN="$SIM_VENDOR" go install github.com/nats-io/nkeys/nk@latest || return 1
    fi

    [ -x "$SIM_VENDOR/nats" ] && [ -x "$SIM_VENDOR/nk" ] || return 1

    # Hot-update S1 (docs/reviews/hot-update-reliable-exec-plan.md §5.1): vendor/tether-prev is the
    # REAL previous release, built from its own tag's source in an isolated archive. tether-next is
    # the same source with a different version string and only proves the exec MECHANISM.
    # Historical v0.6.0 and dynamic adjacent-release supplies have separate names.
    sim_stage_prev || return 1
    sim_stage_dynamic_prev || return 1
    sim_stage_flow_legacy || return 1

    cp "$SIM_REPO/scripts/install.sh" "$SIM_VENDOR/install.sh" || return 1
    # One JSON that binds every staged tether binary to its source identity — the evidence single
    # source for "which code was actually under test".
    # origin: hot-update S10 plan §2.1 / P10. The supervised Caddy step
    # already supplies S9's official pair and support generations. Verify again
    # after the tether builds: the archive and final vendor bytes must agree.
    . "$SIM_REPO/test/simcluster/lib/stage-infra.sh" || return 1
    sim_verify_infra || return 1
    sim_stage_fullstack || return 1
    # Do not label a build with a later HEAD/diff or with bytes swapped by a
    # subsequent supply step. The unique output paths also exclude stale bin/.
    _sb_final_source=$(sim_build_source_identity) || return 1
    [ "$_sb_source" = "$_sb_final_source" ] || return 1
    _sb_final_current=$(sim_artifact_sha "$SIM_VENDOR/tether") || return 1
    _sb_final_next=$(sim_artifact_sha "$SIM_VENDOR/tether-next") || return 1
    [ "$_sb_current_sha" = "$_sb_final_current" ] && [ "$_sb_next_sha" = "$_sb_final_next" ] || return 1
    rm -rf "$_sb_build" || return 1
    _sb_build=
    sim_write_tether_source || return 1
    _sb_done=1
    sim_stage_log "vendor staged: $SIM_VENDOR"
)

sim_build_source_identity() (
    git -C "$SIM_REPO" rev-parse HEAD || exit 1
    sim_dirty_patch_sha
)

sim_artifact_sha() (
    _sas_value=$(sha256sum "$1") || exit 1
    printf '%s\n' "${_sas_value%% *}"
)

# origin: hot-update S8 plan §7.3 / P22. Clean and dirty trees use the
# same reachable release set. Exact release HEAD uses its previous reachable
# release; unrelated tags and merged lower-version side tags cannot move N-1.
sim_dynamic_prev_tag() (
    _head_tag="$(cd "$SIM_REPO" && git tag --list 'v*' --points-at HEAD --sort=-v:refname 2>/dev/null | head -1)"
    _tags="$(cd "$SIM_REPO" && git tag --list 'v*' --merged HEAD --sort=-v:refname)" || exit 1
    _prev=$(printf '%s\n' "$_tags" | awk -v h="$_head_tag" 'h=="" {print;exit} $0==h {f=1;next} f {print;exit}')
    [ -n "$_prev" ] || { sim_stage_log "no previous v* tag reachable from HEAD"; exit 1; }
    printf '%s\n' "$_prev"
)

# Preserve the old dynamic selector for callers outside this package. New
# consumers select a complete fixed/dynamic tuple with sim_select_previous.
sim_prev_tag() { sim_dynamic_prev_tag; }
sim_fixed_prev_tag() { printf '%s\n' "$SIM_FLOW_LEGACY_TAG"; }

# origin: hot-update S5 plan §4.1 / §9.3. Fixed historical supply is
# independent of dynamic N-1: cutting another release must not move drill L/F.
# S8 §7.3 also fixes tether-prev; dynamic supply has its own executable name.
SIM_FLOW_LEGACY_TAG=v0.6.0
SIM_FLOW_LEGACY_COMMIT=a3431a12be3cf947cc797c28fea93382fa68472d

sim_artifact_tool() (
    . "${SIM_STAGE_LIB:-$SIM_REPO/test/simcluster/lib}/upgrade-artifacts.sh"
    sim_upgrade_artifacts "$@"
)

sim_release_source_ok() {
    sim_artifact_tool verify "$2" "$1" "${3:-}" >/dev/null
}

# The supplier archives committed source and uses actual embedded Go buildinfo.
# Neither source files nor another worktree/index are modified during builds.
sim_stage_release() {
    sim_artifact_tool stage "$1" "$2" "${3:-}" >/dev/null
}

sim_stage_prev() {
    sim_stage_release "$SIM_FLOW_LEGACY_TAG" tether-prev "$SIM_FLOW_LEGACY_COMMIT"
}

sim_stage_dynamic_prev() (
    _sp_tag=$(sim_dynamic_prev_tag) || exit 1
    sim_stage_release "$_sp_tag" tether-dynamic-prev
)

sim_stage_flow_legacy() {
    sim_stage_release "$SIM_FLOW_LEGACY_TAG" tether-flow-legacy "$SIM_FLOW_LEGACY_COMMIT"
}

sim_prev_source_ok() {
    [ "$1" = "$SIM_FLOW_LEGACY_TAG" ] || return 1
    sim_release_source_ok tether-prev "$SIM_FLOW_LEGACY_TAG" "$SIM_FLOW_LEGACY_COMMIT"
}

# origin: hot-update S8 plan §6.1 / §7.3 / P22. Pair/probe/argv suppliers
# have explicit inputs; missing decoder or P24 probe dependencies stay red.
sim_stage_upgrade_pair() {
    sim_artifact_tool pair "$1" "$2"
}

# origin: hot-update S10 plan §2.1 / FullstackArtifactSupplyTest. The
# recursive vendor/infra COPY carries this bundle, including independent S8
# and release provenance. A missing real pair is a supply failure, never next.
sim_stage_fullstack() (
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        sim_stage_log "fullstack supply requires the shared process supervisor"; exit 2;
    }
    exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B \
        "$SIM_REPO/test/simcluster/lib/fullstack-artifacts.py" stage \
        "$SIM_REPO" "$SIM_VENDOR" \
        --a-ref "${SIM_FULLSTACK_A_REF:-}" --b-ref "${SIM_FULLSTACK_B_REF:-}" \
        --version-prefix "${SIM_FULLSTACK_VERSION_PREFIX:-v0.0.0-s10}"
)

# Read-only guards are also usable by fixture owners before READY. Image and
# executing joins are explicit operations of fullstack-artifacts.py, since a
# vendor-only check cannot attest to either of those later arrows.
sim_verify_fullstack() (
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        sim_stage_log "fullstack verification requires the shared process supervisor"; exit 2;
    }
    exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B "$SIM_REPO/test/simcluster/lib/fullstack-artifacts.py" verify \
        "$SIM_REPO" "$SIM_VENDOR"
)

sim_stage_wire_probe() {
    case "$2" in old|new) ;; *) return 2 ;; esac
    sim_artifact_tool probe "$1" "upgrade-wire-probe-$2" "$3"
}

sim_upgrade_execution_ok() {
    sim_artifact_tool execution-ok "$1" "$2" "$3" "$4" >/dev/null
}

sim_dynamic_prev_source_ok() (
    _sdp_tag=$(sim_dynamic_prev_tag) || exit 1
    [ "$1" = "$_sdp_tag" ] || exit 1
    sim_release_source_ok tether-dynamic-prev "$_sdp_tag"
)

# origin: S8 internal review rv-n1 F6
# Set all three consumer operands only after the matching verifier succeeds.
# Call directly (not in command substitution), then copy/hash/retain these
# paths together. An error clears any earlier selection instead of reusing it.
sim_select_previous() {
    SIM_PREVIOUS_TAG= SIM_PREVIOUS_BINARY= SIM_PREVIOUS_SOURCE=
    case "$1" in
        fixed)
            _ssp_tag=$(sim_fixed_prev_tag) || return 1
            _ssp_component=tether-prev
            sim_prev_source_ok "$_ssp_tag" || return 1 ;;
        dynamic)
            _ssp_tag=$(sim_dynamic_prev_tag) || return 1
            _ssp_component=tether-dynamic-prev
            sim_dynamic_prev_source_ok "$_ssp_tag" || return 1 ;;
        *) sim_stage_log "previous supply requires fixed or dynamic selection"; return 2 ;;
    esac
    SIM_PREVIOUS_TAG=$_ssp_tag
    SIM_PREVIOUS_BINARY=$SIM_VENDOR/$_ssp_component
    SIM_PREVIOUS_SOURCE=$SIM_VENDOR/$_ssp_component-source.json
}

sim_flow_legacy_source_ok() {
    sim_release_source_ok tether-flow-legacy "$SIM_FLOW_LEGACY_TAG" "$SIM_FLOW_LEGACY_COMMIT"
}

# origin: hot-update S5 plan §9.5 / T48. File counts miss an edited byte
# and untracked build input. Digest names, modes and actual content, including
# deletions and index changes relative to HEAD, without publishing source bytes.
sim_dirty_patch_sha() {
    python3 - "$SIM_REPO" <<'PYDIRTY'
import hashlib, os, pathlib, stat, subprocess, sys
root = pathlib.Path(sys.argv[1])
def git(*args): return subprocess.check_output(["git", "-C", str(root), *args])
h = hashlib.sha256(b"tether-source-dirty-v1\0")
def field(value):
    h.update(len(value).to_bytes(8, "big")); h.update(value)
field(git("diff", "--binary", "--no-ext-diff", "--no-textconv", "HEAD", "--"))
for name in sorted(git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")):
    if not name: continue
    path = root / os.fsdecode(name)
    info = path.lstat()
    field(name); field(str(stat.S_IMODE(info.st_mode)).encode())
    field(os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes())
print(h.hexdigest())
PYDIRTY
}

# Atomic receipt publication: no torn JSON and no dirty-count-only provenance.
sim_write_tether_source() (
    _sw_head=$(cd "$SIM_REPO" && git rev-parse HEAD) || return 1
    _sw_dirty=$(cd "$SIM_REPO" && git status --porcelain | wc -l | tr -d ' ')
    _sw_digest=$(sim_dirty_patch_sha) || return 1
    # A staged invocation must publish its pre-build source, not a later edit.
    [ -z "${_sb_source:-}" ] || [ "$_sb_source" = "$(printf '%s\n%s' "$_sw_head" "$_sw_digest")" ] || return 1
    python3 - "$SIM_VENDOR" "$_sw_head" "$_sw_dirty" "$_sw_digest" "${SIM_VERSION:-v0.0.0-simcluster}" "${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}" <<'PYIDENTITY'
import hashlib, json, os, pathlib, subprocess, sys
root, head, dirty, digest, version, next_version = sys.argv[1:]
root = pathlib.Path(root)
def sha(name):
    with (root/name).open("rb") as f: return hashlib.file_digest(f, "sha256").hexdigest()
def actual_go(name):
    raw = subprocess.check_output(["go", "version", "-m", str(root/name)], text=True)
    return raw.splitlines()[0].rsplit(": ", 1)[1]
toolchain = actual_go("tether")
next_toolchain = actual_go("tether-next")
prev = json.loads((root/"tether-prev-source.json").read_text())
dynamic = json.loads((root/"tether-dynamic-prev-source.json").read_text())
fixed = json.loads((root/"tether-flow-legacy-source.json").read_text()) if (root/"tether-flow-legacy-source.json").exists() else None
record = dict(schema_version=2, head=head, source_head=head, dirty_files=int(dirty),
              dirty_patch_sha=digest, tether_sha256=sha("tether"), tether_next_sha256=sha("tether-next"),
              tether_prev_tag=prev["tag"], tether_prev_commit=prev["commit"], tether_prev_sha256=sha("tether-prev"),
              go_version=toolchain,
              build_flags="CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build",
              version=version, next_version=next_version, flow_legacy=fixed,
              next_go_version=next_toolchain, dynamic_previous=dynamic)
tmp = root/"tether-source.json.tmp"
with tmp.open("w") as f: json.dump(record, f, sort_keys=True); f.write("\n")
os.replace(tmp, root/"tether-source.json")
PYIDENTITY
)

# True when this machine IS the sim host (weilandserver). Used by local.sh to refuse running
# somewhere it would spin containers on the wrong box, and by remote.sh to warn about the reverse.
# NOTE: `hostname` prints "weilandserver" while /etc/hostname says "weiland_server" — accept both,
# and fall back to the LAN address so a rename does not silently flip the answer.
sim_is_sim_host() {
    case "$(hostname 2>/dev/null)" in weilandserver|weiland_server) return 0 ;; esac
    case "$(hostname -s 2>/dev/null)" in weilandserver|weiland_server) return 0 ;; esac
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -qx "${SIM_HOST_IP:-192.168.0.200}"
}
