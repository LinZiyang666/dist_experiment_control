#!/bin/sh
# tether install.sh — single entry point for agent / ctl / broker
# install (architecture K). Pure POSIX sh; tested under bash, dash,
# busybox sh.
#
# CORE INVARIANT (K.0 §2): this script ONLY lays down files, generates
# configs, and writes systemd units. It NEVER starts anything. The
# caller is the only thing that runs `tether agent` / `tether serve` /
# `systemctl enable --now ...`. After this script returns,
# `pgrep tether` MUST be empty.
#
# Usage:
#   curl -fsSL https://<broker>/install.sh | sh -s -- --role agent  --broker wss://broker --session lab --pin XXX --nid lab-1
#   curl -fsSL https://<broker>/install.sh | sh -s -- --role ctl
#   curl -fsSL https://<broker>/install.sh | sudo sh -s -- --role broker --domain tether.example.com --acme-email admin@example.com
#
# Test / dev knobs (not for production callers):
#   --dry-run           do everything except download + chmod + write
#                       outside the work-root; useful for CI assertions.
#   --source-base URL   override the release tarball base URL (default:
#                       https://github.com/LinZiyang666/tether/releases/download/<ver>).
#   --version VER       pin the release version (default: latest tag).
#   --prefix DIR        override the install root (defaults: per role).
#   --skip-download     don't try to fetch the tarball; use the file at
#                       <prefix>/tether (already-staged binary).
#   --uninstall         tear down whatever this role wrote.
set -eu

ROLE=""
BROKER_URL=""
SESSION=""
PIN=""
NID=""
DOMAIN=""
ACME_EMAIL=""
DRY_RUN=0
SOURCE_BASE=""
VERSION="${TETHER_VERSION:-v0.0.0-dev}"
PREFIX=""
SKIP_DOWNLOAD=0
UNINSTALL=0

usage() {
    cat <<EOF
tether install.sh — install (do NOT start) one tether role.

Required:
  --role {agent,ctl,broker}

Agent-only:
  --broker URL    (e.g. wss://broker.example.com:443)
  --session SID
  --pin PIN
  --nid NID

Broker-only:
  --domain DOMAIN
  --acme-email EMAIL

Common:
  --version VER       (default: ${VERSION})
  --source-base URL
  --prefix DIR
  --dry-run
  --skip-download
  --uninstall
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --role)          ROLE="$2";          shift 2 ;;
        --broker)        BROKER_URL="$2";    shift 2 ;;
        --session)       SESSION="$2";       shift 2 ;;
        --pin)           PIN="$2";           shift 2 ;;
        --nid)           NID="$2";           shift 2 ;;
        --domain)        DOMAIN="$2";        shift 2 ;;
        --acme-email)    ACME_EMAIL="$2";    shift 2 ;;
        --version)       VERSION="$2";       shift 2 ;;
        --source-base)   SOURCE_BASE="$2";   shift 2 ;;
        --prefix)        PREFIX="$2";        shift 2 ;;
        --dry-run)       DRY_RUN=1;          shift ;;
        --skip-download) SKIP_DOWNLOAD=1;    shift ;;
        --uninstall)     UNINSTALL=1;        shift ;;
        -h|--help)       usage; exit 0 ;;
        *)
            echo "install.sh: unknown flag: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

# Architecture K.2 default: bare `curl install.sh | sh` installs ctl.
# Operators distinguish agent / broker by always passing --role.
if [ -z "$ROLE" ]; then
    ROLE="ctl"
fi

# -- common helpers --------------------------------------------------------

log() { printf '%s\n' "$*"; }
die() { printf 'install.sh: %s\n' "$*" >&2; exit 1; }

run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) $*"
    else
        eval "$*"
    fi
}

detect_os_arch() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    case "$ARCH" in
        x86_64|amd64)  ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *) die "unsupported arch: $ARCH (v1 supports amd64, arm64)" ;;
    esac
    case "$OS" in
        linux|darwin) ;;
        *) die "unsupported os: $OS (v1 supports linux, darwin)" ;;
    esac
}

source_tarball_url() {
    if [ -n "$SOURCE_BASE" ]; then
        printf '%s/tether_%s_%s_%s.tar.gz\n' "$SOURCE_BASE" "$VERSION" "$OS" "$ARCH"
    else
        printf 'https://github.com/LinZiyang666/tether/releases/download/%s/tether_%s_%s_%s.tar.gz\n' \
            "$VERSION" "$VERSION" "$OS" "$ARCH"
    fi
}

source_sha_url() {
    if [ -n "$SOURCE_BASE" ]; then
        printf '%s/SHA256SUMS\n' "$SOURCE_BASE"
    else
        printf 'https://github.com/LinZiyang666/tether/releases/download/%s/SHA256SUMS\n' "$VERSION"
    fi
}

# fetch <url> <out>: download with curl, fail on HTTP ≥ 400.
fetch() {
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) fetch $1 → $2"
        return 0
    fi
    curl -fsSL --retry 3 "$1" -o "$2" || die "download failed: $1"
}

# verify_sha <file> <sha256-hex>: bail if hash mismatches.
verify_sha() {
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) verify_sha $1"
        return 0
    fi
    have=""
    if command -v sha256sum >/dev/null 2>&1; then
        have=$(sha256sum "$1" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        have=$(shasum -a 256 "$1" | awk '{print $1}')
    else
        die "neither sha256sum nor shasum found; cannot verify download"
    fi
    if [ "$have" != "$2" ]; then
        die "sha256 mismatch: got $have, want $2"
    fi
}

# extract_to <tarball> <dest_dir>
extract_to() {
    run "tar -xzf '$1' -C '$2'"
}

# place_binary <src_tarball> <dest_dir>: extract tether to dest.
# Tarball layout from goreleaser: tether (a single binary at the
# archive root) + LICENSE + README.
place_binary() {
    src="$1"
    dest="$2"
    run "mkdir -p '$dest'"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) extract '$src' → '$dest/tether'"
        return 0
    fi
    tmp=$(mktemp -d)
    tar -xzf "$src" -C "$tmp"
    mv "$tmp/tether" "$dest/tether"
    chmod +x "$dest/tether"
    rm -rf "$tmp"
}

# -- role: agent -----------------------------------------------------------

install_agent() {
    [ -n "$BROKER_URL" ] || die "--broker required for --role agent"
    [ -n "$SESSION" ]    || die "--session required for --role agent"
    [ -n "$NID" ]        || die "--nid required for --role agent"
    # PIN is optional — only required on the very first connect.

    BIN_DIR="${PREFIX:-$HOME/.local/bin}"
    SESSION_DIR="$HOME/.tether/agent/$SESSION"

    detect_os_arch
    log "tether install (role=agent, version=$VERSION, os=$OS, arch=$ARCH)"

    run "mkdir -p '$BIN_DIR'"
    run "mkdir -p '$SESSION_DIR' '$SESSION_DIR/keys'"
    # 0700 across the per-session tree (architecture K.1 mandate).
    run "chmod 700 '$HOME/.tether' '$HOME/.tether/agent' '$SESSION_DIR' '$SESSION_DIR/keys'"

    if [ "$SKIP_DOWNLOAD" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        TARBALL=$(mktemp)
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        log "  + downloading $TARBALL_URL"
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 for $(basename "$TARBALL_URL") in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$BIN_DIR"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
    fi

    # Derive tunnel_addr from broker URL: split-ports model (A.3)
    # puts frps on host:7000. Host extraction handles wss://, ws://,
    # and bare host. Port is fixed at 7000 (architecture A.3).
    BROKER_HOST=$(printf '%s' "$BROKER_URL" | sed -E 's#^[a-z]+://##; s#[:/].*##')
    TUNNEL_ADDR="${BROKER_HOST}:7000"

    # agent.yaml: enough for `tether agent --session <sid>` to find
    # the broker without operator-supplied flags. PIN deliberately
    # NOT persisted (it's a one-time bootstrap secret).
    if [ "$DRY_RUN" -eq 0 ]; then
        cat > "$SESSION_DIR/agent.yaml" <<EOF
broker_url: $BROKER_URL
session: $SESSION
nid: $NID
tunnel_addr: $TUNNEL_ADDR
EOF
        chmod 600 "$SESSION_DIR/agent.yaml"
    else
        log "  + (dry-run) write $SESSION_DIR/agent.yaml"
    fi

    cat <<EOF

✔ tether installed to $BIN_DIR/tether
✔ agent config written to $SESSION_DIR/agent.yaml

To start the agent now (architecture K.1; install.sh does NOT start anything):
    setsid nohup $BIN_DIR/tether agent --session $SESSION --nid $NID${PIN:+ --pin $PIN} \\
      >> $SESSION_DIR/agent.log 2>&1 &

For auto-start across logins (optional):
    $BIN_DIR/tether agent --install-user-service --session $SESSION --nid $NID
    # generates ~/.config/systemd/user/tether-agent@$SESSION.service
EOF
}

uninstall_agent() {
    BIN_DIR="${PREFIX:-$HOME/.local/bin}"
    log "tether uninstall (role=agent)"
    run "rm -f '$BIN_DIR/tether'"
    run "rm -rf '$HOME/.tether'"
    log "  ✔ removed $BIN_DIR/tether and ~/.tether"
}

# -- role: ctl -------------------------------------------------------------

install_ctl() {
    detect_os_arch
    log "tether install (role=ctl, version=$VERSION, os=$OS, arch=$ARCH)"

    if [ -n "$PREFIX" ]; then
        BIN_DIR="$PREFIX"
    elif [ -w /usr/local/bin ] 2>/dev/null; then
        BIN_DIR="/usr/local/bin"
    else
        BIN_DIR="$HOME/.local/bin"
    fi
    run "mkdir -p '$BIN_DIR'"

    if [ "$SKIP_DOWNLOAD" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        TARBALL=$(mktemp)
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$BIN_DIR"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
    fi

    cat <<EOF

✔ tether installed to $BIN_DIR/tether
EOF
    if [ "$BIN_DIR" = "$HOME/.local/bin" ]; then
        echo "  Note: $BIN_DIR is not in PATH on a default shell."
        echo "        Add: export PATH=\"\$HOME/.local/bin:\$PATH\""
    fi
    cat <<'EOF'

Next steps:
    tether login --broker wss://<broker>:443 --session <sid> --pin <pin>
EOF
}

uninstall_ctl() {
    if [ -n "$PREFIX" ]; then BIN_DIR="$PREFIX"
    elif [ -f /usr/local/bin/tether ]; then BIN_DIR="/usr/local/bin"
    else BIN_DIR="$HOME/.local/bin"
    fi
    log "tether uninstall (role=ctl)"
    run "rm -f '$BIN_DIR/tether'"
    log "  ✔ removed $BIN_DIR/tether"
}

# -- role: broker ----------------------------------------------------------

install_broker() {
    [ -n "$DOMAIN" ]     || die "--domain required for --role broker"
    [ -n "$ACME_EMAIL" ] || die "--acme-email required for --role broker"

    detect_os_arch
    log "tether install (role=broker, version=$VERSION, os=$OS, arch=$ARCH, domain=$DOMAIN)"

    BIN_DIR="${PREFIX:-/usr/local/bin}"
    ETC_DIR="/etc/tether"
    LIB_DIR="/var/lib/tether"
    LOG_DIR="/var/log/tether"
    RUN_DIR="/var/run/tether"
    SYSTEMD_DIR="/etc/systemd/system"

    # System user — installs.sh does not run it; we just create it.
    if [ "$DRY_RUN" -eq 0 ] && ! id -u tether >/dev/null 2>&1; then
        run "useradd --system --home-dir '$LIB_DIR' --shell /usr/sbin/nologin tether"
    else
        log "  + (skip) useradd tether"
    fi

    run "mkdir -p '$BIN_DIR' '$ETC_DIR' '$SYSTEMD_DIR'"
    # Runtime dirs MUST be writable by the tether service user. The
    # systemd units run as User=tether; without these chowns the
    # daemon can't open SQLite, write JetStream files, or bind
    # admin.sock. install -d sets ownership + mode in one atomic
    # call (cleaner than mkdir + chmod + chown chain).
    if [ "$DRY_RUN" -eq 0 ]; then
        install -d -o tether -g tether -m 0750 "$LIB_DIR" "$LOG_DIR"
        install -d -o tether -g tether -m 0700 "$RUN_DIR"
    else
        log "  + (dry-run) install -d -o tether -g tether -m 0750 $LIB_DIR $LOG_DIR"
        log "  + (dry-run) install -d -o tether -g tether -m 0700 $RUN_DIR"
    fi

    if [ "$SKIP_DOWNLOAD" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        TARBALL=$(mktemp)
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$BIN_DIR"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
    fi

    install_nats_server "$BIN_DIR"
    install_caddy "$BIN_DIR"

    # broker.yaml — architecture A.3 skeleton.
    if [ "$DRY_RUN" -eq 0 ]; then
        cat > "$ETC_DIR/broker.yaml" <<EOF
broker:
  domain: $DOMAIN
  public_host: $DOMAIN
  tls:
    acme:
      email: $ACME_EMAIL
  nats:
    url: nats://127.0.0.1:4222
    wss_listen: ":443"
    ws_internal: "127.0.0.1:8222"
  frp:
    bind_addr: "0.0.0.0"
    control_listen: ":7000"
    port_range: "14000-14999"
  admin:
    socket: $RUN_DIR/admin.sock
  storage:
    db: $LIB_DIR/tether.db
    js_store: $LIB_DIR/jetstream
EOF
        chmod 644 "$ETC_DIR/broker.yaml"
    else
        log "  + (dry-run) write $ETC_DIR/broker.yaml"
    fi

    # Caddyfile (TLS termination + reverse proxy to NATS WebSocket).
    if [ "$DRY_RUN" -eq 0 ]; then
        cat > "$ETC_DIR/Caddyfile" <<EOF
{
    email $ACME_EMAIL
}

$DOMAIN:443 {
    reverse_proxy 127.0.0.1:8222
}
EOF
        chmod 644 "$ETC_DIR/Caddyfile"
    else
        log "  + (dry-run) write $ETC_DIR/Caddyfile"
    fi

    # systemd units — generated but NOT enabled or started (K.0 §2).
    write_systemd_units "$SYSTEMD_DIR" "$BIN_DIR" "$ETC_DIR" "$LIB_DIR" "$LOG_DIR"

    cat <<EOF

✔ broker files installed.
✔ systemd units created (nats-server, tether-broker, caddy).

To start the broker stack (install.sh did NOT start anything):
    sudo systemctl daemon-reload
    sudo systemctl enable --now nats-server tether-broker caddy
EOF
}

### Sidecar binary installs ##############################################
#
# Architecture A.3: broker host needs nats-server (JS-enabled) and
# caddy (TLS terminator). install.sh ships both as static
# binaries from upstream releases — pinned versions so the
# broker.yaml + Caddyfile we generate are always compatible. Each
# fetch is sha256-verified; failure is fatal.
#
# Pin policy: bump these together with broker.yaml / Caddyfile
# changes. Architecture J.5 keeps tetherd upgrades manual; sidecar
# upgrades are also manual and bundled with each tether release.

NATS_SERVER_VERSION="${TETHER_NATS_SERVER_VERSION:-v2.10.22}"
CADDY_VERSION="${TETHER_CADDY_VERSION:-2.7.6}"

install_nats_server() {
    bin="$1"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) nats-server install"
        return 0
    fi
    if [ -x "$bin/nats-server" ]; then
        log "  + nats-server already present at $bin/nats-server (skip)"
        return 0
    fi
    case "$ARCH" in amd64) na="amd64" ;; arm64) na="arm64" ;; *) die "nats arch $ARCH" ;; esac
    case "$OS"   in linux) no="linux" ;;  darwin) no="darwin" ;;  *) die "nats os $OS" ;; esac
    base="https://github.com/nats-io/nats-server/releases/download/${NATS_SERVER_VERSION}"
    name="nats-server-${NATS_SERVER_VERSION}-${no}-${na}"
    url="${base}/${name}.tar.gz"
    sums="${base}/SHA256SUMS"
    log "  + downloading $url"
    tmp=$(mktemp -d); trap "rm -rf '$tmp'" EXIT
    curl -fsSL --retry 3 "$url"  -o "$tmp/nats.tgz"  || die "fetch nats-server failed: $url"
    curl -fsSL --retry 3 "$sums" -o "$tmp/sums.txt" || die "fetch nats-server SHA256SUMS failed: $sums"
    expect=$(grep "${name}.tar.gz$" "$tmp/sums.txt" | awk '{print $1}')
    [ -n "$expect" ] || die "no sha256 for ${name}.tar.gz in NATS SHA256SUMS"
    verify_sha "$tmp/nats.tgz" "$expect"
    tar -xzf "$tmp/nats.tgz" -C "$tmp"
    install -m 0755 "$tmp/${name}/nats-server" "$bin/nats-server"
    rm -rf "$tmp"; trap - EXIT
    log "  ✔ installed $bin/nats-server (${NATS_SERVER_VERSION})"
}

install_caddy() {
    bin="$1"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) caddy install"
        return 0
    fi
    if [ -x "$bin/caddy" ]; then
        log "  + caddy already present at $bin/caddy (skip)"
        return 0
    fi
    case "$ARCH" in amd64) ca="amd64" ;; arm64) ca="arm64" ;; *) die "caddy arch $ARCH" ;; esac
    case "$OS"   in linux) co="linux" ;;  darwin) co="mac"   ;;  *) die "caddy os $OS" ;; esac
    base="https://github.com/caddyserver/caddy/releases/download/v${CADDY_VERSION}"
    name="caddy_${CADDY_VERSION}_${co}_${ca}"
    url="${base}/${name}.tar.gz"
    sums="${base}/caddy_${CADDY_VERSION}_checksums.txt"
    log "  + downloading $url"
    tmp=$(mktemp -d); trap "rm -rf '$tmp'" EXIT
    curl -fsSL --retry 3 "$url"  -o "$tmp/caddy.tgz" || die "fetch caddy failed: $url"
    curl -fsSL --retry 3 "$sums" -o "$tmp/sums.txt"  || die "fetch caddy checksums failed: $sums"
    expect=$(grep "${name}.tar.gz$" "$tmp/sums.txt" | awk '{print $1}')
    [ -n "$expect" ] || die "no sha256 for ${name}.tar.gz in Caddy checksums"
    verify_sha "$tmp/caddy.tgz" "$expect"
    tar -xzf "$tmp/caddy.tgz" -C "$tmp"
    install -m 0755 "$tmp/caddy" "$bin/caddy"
    rm -rf "$tmp"; trap - EXIT
    log "  ✔ installed $bin/caddy (${CADDY_VERSION})"
}

write_systemd_units() {
    sysd="$1"; bin="$2"; etc="$3"; lib="$4"; log_dir="$5"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) write $sysd/{nats-server,tether-broker,caddy}.service"
        return 0
    fi

    cat > "$sysd/nats-server.service" <<EOF
[Unit]
Description=NATS message broker (tether)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tether
ExecStart=/usr/local/bin/nats-server -js -sd $lib/jetstream -addr 127.0.0.1 -p 4222 --ws_listen 127.0.0.1:8222
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$sysd/nats-server.service"

    cat > "$sysd/tether-broker.service" <<EOF
[Unit]
Description=tether broker daemon
After=nats-server.service
Wants=nats-server.service

[Service]
Type=simple
User=tether
ExecStart=$bin/tether serve --config $etc/broker.yaml
Restart=on-failure
StandardOutput=append:$log_dir/broker.log
StandardError=append:$log_dir/broker.err

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$sysd/tether-broker.service"

    cat > "$sysd/caddy.service" <<EOF
[Unit]
Description=Caddy (TLS termination for tether NATS WSS)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/caddy run --config $etc/Caddyfile --adapter caddyfile
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$sysd/caddy.service"
}

uninstall_broker() {
    BIN_DIR="${PREFIX:-/usr/local/bin}"
    log "tether uninstall (role=broker)"
    for u in nats-server tether-broker caddy; do
        run "rm -f /etc/systemd/system/$u.service"
    done
    run "rm -f '$BIN_DIR/tether'"
    run "rm -rf /etc/tether /var/lib/tether /var/log/tether /var/run/tether"
    log "  ✔ removed broker files (systemd units, /etc/tether, /var/{lib,log,run}/tether)"
    log "  ✔ note: did NOT remove the 'tether' system user; do that manually if intended"
}

# -- dispatch --------------------------------------------------------------

case "$ROLE" in
    agent)
        if [ "$UNINSTALL" -eq 1 ]; then uninstall_agent; else install_agent; fi
        ;;
    ctl)
        if [ "$UNINSTALL" -eq 1 ]; then uninstall_ctl; else install_ctl; fi
        ;;
    broker)
        if [ "$UNINSTALL" -eq 1 ]; then uninstall_broker; else install_broker; fi
        ;;
    *)
        die "unknown --role $ROLE (must be one of: agent, ctl, broker)"
        ;;
esac
