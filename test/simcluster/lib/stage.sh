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
sim_stage_binaries() {
    command -v go >/dev/null 2>&1 || {
        sim_stage_log "no Go toolchain on PATH — needed to build tether. Install Go, or drive from a box that has it via ./remote.sh --build"
        return 1
    }
    nats_version="$(sim_nats_version)" || return 1
    mkdir -p "$SIM_VENDOR"

    sim_stage_log "building static tether (CGO_ENABLED=0 amd64)…"
    ( cd "$SIM_REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION="${SIM_VERSION:-v0.0.0-simcluster}" )
    cp "$SIM_REPO/bin/tether" "$SIM_VENDOR/tether"

    # S5 / s3-s5-plan §1.E: a SECOND build with ONLY the version string bumped → vendor/tether-next, the
    # staged "new" binary drills 30/31 upgrade to. SAME source / proto / commandVersion / schema (g5
    # rolling-safety hard precondition — a real proto/command delta is an incompatible reinstall, not a
    # rolling upgrade). artifact.sh serves it with a host-computed SHA256SUMS; 30 stages it per-host, 31
    # serves it as the self-hosted mirror the agent allow-list (#28) refuses.
    sim_stage_log "building tether-next (VERSION=${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}, same source)…"
    ( cd "$SIM_REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}" )
    cp "$SIM_REPO/bin/tether" "$SIM_VENDOR/tether-next"

    # nats-server pinned to the production install.sh version (NOT go.mod's embedded newer one).
    if [ ! -x "$SIM_VENDOR/nats-server" ] \
       || ! "$SIM_VENDOR/nats-server" --version 2>/dev/null | grep -q "${nats_version#v}"; then
        sim_stage_log "fetching nats-server $nats_version…"
        tmp="$(mktemp -d)"
        curl -fsSL -o "$tmp/n.tgz" \
          "https://github.com/nats-io/nats-server/releases/download/${nats_version}/nats-server-${nats_version}-linux-amd64.tar.gz"
        tar xzf "$tmp/n.tgz" -C "$tmp"
        cp "$tmp/nats-server-${nats_version}-linux-amd64/nats-server" "$SIM_VENDOR/nats-server"
        rm -rf "$tmp"
    fi

    # nats + nk CLIs (host build == amd64 == sim host). Cached.
    if [ ! -x "$SIM_VENDOR/nats" ]; then
        sim_stage_log "go install nats CLI…"
        GOBIN="$SIM_VENDOR" go install github.com/nats-io/natscli/nats@latest
    fi
    if [ ! -x "$SIM_VENDOR/nk" ]; then
        sim_stage_log "go install nk…"
        GOBIN="$SIM_VENDOR" go install github.com/nats-io/nkeys/nk@latest
    fi

    cp "$SIM_REPO/scripts/install.sh" "$SIM_VENDOR/install.sh"
    sim_stage_log "vendor staged: $(cd "$SIM_VENDOR" && ls -1 | tr '\n' ' ')"
}

# True when this machine IS the sim host (weilandserver). Used by local.sh to refuse running
# somewhere it would spin containers on the wrong box, and by remote.sh to warn about the reverse.
# NOTE: `hostname` prints "weilandserver" while /etc/hostname says "weiland_server" — accept both,
# and fall back to the LAN address so a rename does not silently flip the answer.
sim_is_sim_host() {
    case "$(hostname 2>/dev/null)" in weilandserver|weiland_server) return 0 ;; esac
    case "$(hostname -s 2>/dev/null)" in weilandserver|weiland_server) return 0 ;; esac
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -qx "${SIM_HOST_IP:-192.168.1.150}"
}
