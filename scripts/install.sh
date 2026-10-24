#!/bin/sh
# tether install.sh — single entry point for agent / ctl / broker
# install (architecture K). Pure POSIX sh; tested under bash, dash,
# busybox sh.
#
# CORE INVARIANT (K.0 §2, amended for #76): this script lays down files,
# generates configs, writes systemd units, and — broker role only —
# ENABLES those units for boot (symlink creation only; opt out with
# --no-enable). It NEVER STARTS anything: enabling creates wants/
# symlinks, not processes. The caller is the only thing that runs
# `tether agent` / `tether serve` / `systemctl start ...`. After this
# script returns, `pgrep tether` MUST be empty.
# (#76 rationale: units were "generated but NOT enabled", the enable
# step lived only in a printed banner, and the single production broker
# shipped with boot-autostart silently missing — one reboot took the
# whole fleet offline. Enablement is now the default because a symlink
# is exactly as reversible as the banner command was skippable.)
#
# Usage:
#   BASE=https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download
#   curl -fsSL "$BASE/install.sh" | sh -s -- --role agent  --broker wss://broker --session lab --pin XXX --nid lab-1
#   curl -fsSL "$BASE/install.sh" | sh -s -- --role ctl
#   curl -fsSL "$BASE/install.sh" | sudo sh -s -- --role broker --domain tether.example.com --acme-email admin@example.com
#
# Test / dev knobs (not for production callers):
#   --dry-run           preview only; no filesystem or host mutations.
#   --source-base URL   override the release tarball base URL (default:
#                       https://github.com/LinZiyang666/dist_experiment_control/releases/download/<ver>).
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
NO_ENABLE=0
HOT_UPDATE_RETROFIT=0
HOT_UPDATE_REGISTRATION=""
LOCAL_BIN=""
INSTALL_STAGE_ONLY=0
# Release archives carry these files at their root. Offline broker installs use
# the same set in TETHER_INFRA_SUPPORT_DIR; skipping a download never skips its
# hashes or platform checks. Neither support executable is run by the installer.
# origin: hot-update S9 plan §6.4 / §6.5 / T54
INFRA_SUPPORT_DIR="${TETHER_INFRA_SUPPORT_DIR:-}"
INFRA_WORKER_LIFETIME_S=15
INFRA_WORKER_STOP_S=5
INFRA_MAX_CONNECTIONS=4
INSTALL_TEMP_PATHS=""
# Client liveness, server half — ONE definition, used by the nats.conf template AND the KEPT report's
# merge hint below, so the two can never drift apart (internal review round 1 R3-5 / R4-F4: the hint
# used to hand-copy the values). The agent half is internal/agent AgentPingInterval / AgentMaxPingsOut;
# test/architecture/nats_ping_defaults_test.go reconciles these two lines against it and against drill
# 98's PING_INTERVAL_S / PING_MAX. Shapes are load-bearing: a QUOTED duration and a BARE integer are the
# only forms nats-server reads as intended and tether's config takeover accepts (internal/natsconf).
NATS_PING_INTERVAL="20s"
NATS_PING_MAX=2
# FORCE_CONFIG=1 lets a re-run overwrite configuration files that already exist. Default 0.
#
# origin: prerelease audit deploy-release-docs/DRD-F1 + DRD-F5. A bare re-run used to
# rewrite broker.yaml, the Caddyfile, nats.d/nats.conf and the unit files
# unconditionally, so "just re-run install.sh to upgrade" — which the docs suggested —
# silently reverted every local change: the `broker.cluster` block that makes a node a
# cluster member, an operator's auth_callout edits, a hand-tuned Caddy site. The node
# came back up healthy-looking and no longer part of its cluster.
#
# The fix is NOT to preserve everything unconditionally: a release that has to correct a
# unit file (the way G1 #23 shipped `Restart=always`) relies on the rewrite to reach
# existing machines. So an existing file is kept, the NEW content is written beside it as
# `<file>.new`, and the banner lists every one of them so an operator can diff and decide.
FORCE_CONFIG=0

usage() {
    # UNQUOTED heredoc ON PURPOSE: ${VERSION} below must interpolate so --help shows the
    # real default. The only other shell-active characters in the body are the ESCAPED
    # backticks on the --no-enable line (rendered literally); no other $, no $(…).
    cat <<EOF
tether install.sh — install (do NOT start) one tether role.

Required:
  --role {agent,ctl,broker}    (on macOS: only ctl is supported)

Agent-only:
  --broker URL    (e.g. wss://broker.example.com:443)
  --session SID
  --pin PIN
  --nid NID

Broker-only:
  --domain DOMAIN
  --acme-email EMAIL
  --no-enable         do not \`systemctl enable\` the broker units (default: enabled for boot, never started)

Common:
  --version VER       (default: ${VERSION})
  --source-base URL
  --prefix DIR
  --dry-run
  --skip-download
  --uninstall
  --force-config      overwrite existing config/unit files instead of writing <file>.new beside them
  --bin FILE          install a locally verified tether binary without any downloads
  --hot-update-registration FILE  complete deployment declaration for retrofit (all siblings and UIDs)
  --hot-update-retrofit  prepare hot-update directories and canonical startup templates only
                        (default role: broker; no binary replacement or service start)
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
        --force-config)  FORCE_CONFIG=1;     shift ;;
        --skip-download) SKIP_DOWNLOAD=1;    shift ;;
        --no-enable)     NO_ENABLE=1;        shift ;;
        --uninstall)     UNINSTALL=1;        shift ;;
        --bin)           LOCAL_BIN="$2";     SKIP_DOWNLOAD=1; shift 2 ;;
        --hot-update-retrofit) HOT_UPDATE_RETROFIT=1; shift ;;
        --hot-update-registration) HOT_UPDATE_REGISTRATION="$2"; shift 2 ;;
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
    if [ "$HOT_UPDATE_RETROFIT" -eq 1 ]; then ROLE="broker"; else ROLE="ctl"; fi
fi

# -- common helpers --------------------------------------------------------

log() { printf '%s\n' "$*"; }

# KEPT_CONFIGS accumulates every file this run declined to overwrite, so the closing
# banner can list them in one place instead of leaving the operator to notice a `.new`
# file some other day.
KEPT_CONFIGS=""

# KEPT_ANY survives report_kept_configs' clear, because the ✔ banners are printed
# AFTER that report and still have to know not to claim a file was written.
# origin: prerelease audit round 2, C7.
KEPT_ANY=0

# config_dest selects CONFIG_TARGET: the real path when the file does not exist
# yet or --force-config was given, `<path>.new` otherwise. A kept file is recorded
# for the banner. CONFIG_DEST is the broker's private generation inode, published
# by publish_config; unprivileged agent writes retain their existing destination.
#
# IT SETS A VARIABLE RATHER THAN ECHOING, deliberately. `$(config_dest …)` would run it
# in a SUBSHELL, so every KEPT_CONFIGS append would be discarded the moment the
# substitution finished and the banner would report nothing — while the file-keeping
# itself still worked, which is the worst combination: the protection happens and the
# operator is never told.
#
# origin: prerelease audit deploy-release-docs/DRD-F1 + DRD-F5. See FORCE_CONFIG above
# for why "keep it AND write the new one beside it" rather than either extreme.
# Only first-install management templates may bypass stage-only sidecars. Business
# configurations and existing support templates retain their review boundary.
# origin: S9 internal review rv-priv F4
infra_missing_template() {
    case "$1" in
        "$INFRA_ETC_DIR/templates/tether-infra-helper.socket"|"$INFRA_ETC_DIR/templates/tether-infra-helper@.service"|\
        "$INFRA_ETC_DIR/templates/nats-server.service.d/90-tether-infra.conf"|"$INFRA_ETC_DIR/templates/caddy.service.d/90-tether-infra.conf")
            [ ! -e "$1" ] && [ ! -L "$1" ] ;;
        *) return 1 ;;
    esac
}

config_dest() {
    config_target "$1"
    stage_config
}
# Share atomic, installer-owned staging between rendered templates and copies.
# origin: S9 internal review INFRA-SUITE-SUPPLIES F2 / S9 plan §6.4 / T54
stage_config() {
    CONFIG_TARGET="$CONFIG_DEST"
    [ "$ROLE" = broker ] && [ "$DRY_RUN" -eq 0 ] || return 0
    # nats.d is broker-writable. Generate in its trusted parent, never open or
    # chmod the selected final pathname. A late symlink can only be replaced.
    # Other broker targets already have a root-controlled containing directory.
    # origin: S9 internal review rv-priv F1
    _cd_stage=${CONFIG_TARGET%/*}
    case "$CONFIG_TARGET" in "$ETC_DIR/nats.d/"*) _cd_stage="$ETC_DIR" ;; esac
    infra_safe_path "$_cd_stage"
    [ "$(stat -c '%u' "$_cd_stage")" = "$(id -u)" ] || die "config staging parent must be installer-owned"
    _cd_mode=$(stat -c '%a' "$_cd_stage")
    [ "$((0$_cd_mode & 0002))" -eq 0 ] && { [ "$((0$_cd_mode & 0020))" -eq 0 ] || [ "$(stat -c '%g' "$_cd_stage")" = "$(id -g)" ]; } || die "config staging parent must not be writable by another identity"
    CONFIG_DEST=$(mktemp "$_cd_stage/.tether-config.XXXXXX")
    register_install_temp "$CONFIG_DEST"
}

publish_config() {
    chmod "$1" "$CONFIG_DEST"
    if [ "$ROLE" = broker ]; then
        sync "$CONFIG_DEST"
        # Pin the destination directory as cwd. Renaming one basename never
        # follows the old target (including a symlink installed after validation).
        # Refuse cross-filesystem fallback: publication must be one rename.
        (
            cd -P "${CONFIG_TARGET%/*}" || exit 1
            [ "$(stat -c '%d' .)" = "$(stat -c '%d' "$CONFIG_DEST")" ] || exit 1
            mv -Tf "$CONFIG_DEST" "./${CONFIG_TARGET##*/}" || exit 1
            sync .
        ) || die "atomic config publication failed"
        CONFIG_DEST="$CONFIG_TARGET"
        record_owned_file "$CONFIG_TARGET"
    fi
}

config_target() {
    _cd_path="$1"
    if [ "$ROLE" = broker ]; then
        infra_safe_path "$_cd_path"
        infra_safe_path "${_cd_path}.new"
    fi
    if [ "$INSTALL_STAGE_ONLY" -eq 1 ] && ! infra_missing_template "$_cd_path"; then
        KEPT_CONFIGS="${KEPT_CONFIGS}${_cd_path}
"
        KEPT_ANY=1
        CONFIG_DEST="${_cd_path}.new"
        [ ! -e "$CONFIG_DEST" ] || owned_file "$CONFIG_DEST" || die "operator sidecar already exists: $CONFIG_DEST"
        return 0
    fi
    if [ "$FORCE_CONFIG" -eq 1 ] || [ ! -e "$_cd_path" ]; then
        CONFIG_DEST="$_cd_path"
        # A `.new` left by an EARLIER kept re-run is now stale: this run is about to
        # write that same generated content to the real path, so the sidecar no longer
        # holds anything the operator has not got.
        #
        # origin: prerelease audit round 2, K-F9. Nothing ever removed a .new, and the
        # report's own closing line — "delete the ones you have decided against, or the
        # next run's report is the only thing telling them apart from the current
        # config" — became false the moment --force-config existed: the file that was
        # supposed to be the DIFFERENCE from the live config now duplicated it, and the
        # next report would not mention it at all (nothing was kept), so there was
        # nothing left to tell them apart.
        if [ "$DRY_RUN" -eq 0 ] && [ -e "${_cd_path}.new" ]; then
            if [ "$ROLE" != broker ] || owned_file "${_cd_path}.new"; then
                rm -f "${_cd_path}.new"
                log "  - removed the superseded ${_cd_path}.new (its content is now the live file)"
            else
                log "  + kept operator sidecar: ${_cd_path}.new"
            fi
        fi
        return 0
    fi
    KEPT_CONFIGS="${KEPT_CONFIGS}${_cd_path}
"
    KEPT_ANY=1
    CONFIG_DEST="${_cd_path}.new"
    if [ "$ROLE" = broker ] && [ -e "$CONFIG_DEST" ]; then
        owned_file "$CONFIG_DEST" || die "operator sidecar already exists: $CONFIG_DEST"
    fi
}

# dryrun_config_policy states what a real run would do to files that ALREADY EXIST.
#
# origin: prerelease audit round 2, K-F11. The policy used to be spelled out inline at
# each write block, so a broker dry-run printed it three times, and it was unconditional
# — announcing that config "would be KEPT" on a clean host that has none. Callers gate it
# on there being something to keep.
dryrun_config_policy() {
    if [ "$FORCE_CONFIG" -eq 1 ]; then
        log "  + (dry-run) --force-config: an EXISTING config/unit file would be OVERWRITTEN"
    else
        log "  + (dry-run) an EXISTING config/unit file would be KEPT; new content as <file>.new"
    fi
}

# report_kept_configs prints the summary, ONCE. It clears the list afterwards so a
# second call is a no-op — the report is emitted before the success banner AND before
# any step that can die(), and those two points can both be reached in one run.
# banner_kept_caveat qualifies a ✔ line that would otherwise claim a file this run
# did not write. origin: prerelease audit round 2, C7.
#
# The ✔ banners are QUOTED heredocs on purpose (nothing in them may expand under
# root), so the caveat cannot be interpolated into them and is printed as its own
# line instead. Silent on a run that kept nothing.
banner_kept_caveat() {
    [ "$KEPT_ANY" -eq 1 ] || return 0
    log "  ⚠ …except the files listed above as KEPT: those were NOT written by this run."
    log "    What is installed there is your previous content; the new content is in <file>.new."
}

report_kept_configs() {
    [ -n "$KEPT_CONFIGS" ] || return 0
    log ""
    log "  ⚠ EXISTING CONFIG KEPT — this run did NOT overwrite the following, and wrote the new"
    log "    content beside each one as <file>.new so you can diff before deciding:"
    printf '%s' "$KEPT_CONFIGS" | while IFS= read -r _kc; do
        [ -n "$_kc" ] || continue
        log "      $_kc  →  ${_kc}.new"
        # A kept nats.conf from before the client-liveness keys existed still runs nats-server's
        # ~4 minute dead-client detection (measured 4:00 on the deploy tier) while every upgraded
        # agent already pings at 20s. Name
        # the two lines so the operator merges exactly those and nothing else — the takeover
        # (internal/natsconf) keeps them verbatim, and a version-old broker binary would refuse
        # a conf carrying keys it does not recognise, so merge them only AFTER the binary is
        # upgraded (docs/broker-ops.md).
        case "$_kc" in
            */nats.d/nats.conf)
                # Three shapes, told apart by VALUE, not by key presence (internal review round 1 R3-5): a
                # conf without the keys, a conf that already carries these exact values (nothing to do), and
                # a conf that carries the keys with OTHER values — the operator tuned them, and this report
                # must name the difference rather than stay silent because a `^ping_interval` line exists.
                _kc_iv=$(sed -n 's/^ping_interval:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}[[:space:]]*$/\1/p' "$_kc" 2>/dev/null | head -1)
                _kc_mx=$(sed -n 's/^ping_max:[[:space:]]*\([0-9-][0-9]*\)[[:space:]]*$/\1/p' "$_kc" 2>/dev/null | head -1)
                if [ -z "$_kc_iv" ] && [ -z "$_kc_mx" ]; then
                    log "        this nats.conf predates the client-liveness keys; the .new adds two lines:"
                    log "          ping_interval: \"$NATS_PING_INTERVAL\""
                    log "          ping_max: $NATS_PING_MAX"
                    log "        merge them after the tether binary is upgraded, then 'nats-server --signal reload'"
                elif [ "$_kc_iv" != "$NATS_PING_INTERVAL" ] || [ "$_kc_mx" != "$NATS_PING_MAX" ]; then
                    log "        this nats.conf carries ping_interval=${_kc_iv:-<unset>} / ping_max=${_kc_mx:-<unset>}; this release's agents"
                    log "        assume ping_interval: \"$NATS_PING_INTERVAL\" / ping_max: $NATS_PING_MAX (the client half pings on that cadence)."
                    log "        A looser server half only slows dead-client detection; a missing half or a bare/unquoted"
                    log "        shape is refused by the config takeover — reconcile deliberately, then 'nats-server --signal reload'"
                fi
                ;;
        esac
    done
    log "    A bare re-run used to overwrite these, which silently reverted local edits —"
    log "    including the broker.cluster block that makes this node a cluster member."
    log "    Re-run with --force-config to take the new content as-is."
    log "    Nothing removes a .new file: delete the ones you have decided against, or the"
    log "    next run's report is the only thing telling them apart from the current config."
    KEPT_CONFIGS=""
}
die() { printf 'install.sh: %s\n' "$*" >&2; exit 1; }

# Track only this invocation's mktemp paths. Failed validation/copy must not
# leave writable partial slots for a later install to mistake for published
# artifacts. Permanent locks and selected slots never enter this list.
# origin: hot-update S9 plan §6.4 / T54
register_install_temp() {
    INSTALL_TEMP_PATHS="${INSTALL_TEMP_PATHS}$1
"
}

cleanup_install_temps() {
    printf '%s' "$INSTALL_TEMP_PATHS" | while IFS= read -r _cit_tmp; do
        [ -n "$_cit_tmp" ] && [ -e "$_cit_tmp" ] || continue
        # A failed private copy may already have made its directory immutable.
        # Restoring write access only on this unselected temporary is cleanup.
        if [ -d "$_cit_tmp" ]; then chmod u+w "$_cit_tmp"; fi
        rm -rf "$_cit_tmp"
    done
}
trap cleanup_install_temps 0

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
    # goreleaser's archive name_template uses {{ .Version }}, which strips
    # the leading "v" from the git tag (v0.1.1 → 0.1.1). The release URL
    # path segment still uses the tag verbatim. Keep both in sync here.
    v="${VERSION#v}"
    if [ -n "$SOURCE_BASE" ]; then
        printf '%s/tether_%s_%s_%s.tar.gz\n' "$SOURCE_BASE" "$v" "$OS" "$ARCH"
    else
        printf 'https://github.com/LinZiyang666/dist_experiment_control/releases/download/%s/tether_%s_%s_%s.tar.gz\n' \
            "$VERSION" "$v" "$OS" "$ARCH"
    fi
}

source_sha_url() {
    if [ -n "$SOURCE_BASE" ]; then
        printf '%s/SHA256SUMS\n' "$SOURCE_BASE"
    else
        printf 'https://github.com/LinZiyang666/dist_experiment_control/releases/download/%s/SHA256SUMS\n' "$VERSION"
    fi
}

# resolve_latest_version: sniff the redirect from /releases/latest to
# /releases/tag/<ver>. Plain HTML redirect, no GitHub API ratelimit,
# works under busybox curl. Returns empty if offline / unresolvable
# (caller decides whether that's fatal).
resolve_latest_version() {
    eff=$(curl -fsSIL -o /dev/null -w '%{url_effective}' \
        "https://github.com/LinZiyang666/dist_experiment_control/releases/latest" 2>/dev/null) || return 1
    case "$eff" in
        */releases/tag/*) printf '%s' "${eff##*/tag/}" | tr -d '\r\n ' ;;
        *) return 1 ;;
    esac
}

# maybe_resolve_version: when the caller didn't override --version
# and didn't override --source-base, replace the build-time default
# `v0.0.0-dev` with whatever the latest published tag is. This is
# what makes `curl .../latest/download/install.sh | sh` work without
# the operator pinning a version.
maybe_resolve_version() {
    # review附记(g75-g78): uninstall never downloads anything, so it must not
    # depend on resolving a release tag — an OFFLINE host could not uninstall
    # (the resolver died before any removal ran).
    [ "$HOT_UPDATE_RETROFIT" -eq 1 ] && return 0
    [ "$UNINSTALL" -eq 1 ] && return 0
    [ "$DRY_RUN" -eq 1 ] && return 0
    [ "$SKIP_DOWNLOAD" -eq 1 ] && return 0
    [ -n "$SOURCE_BASE" ] && return 0
    case "$VERSION" in
        latest|v0.0.0-dev|"")
            v=$(resolve_latest_version) || die "could not resolve latest release tag; pass --version vX.Y.Z explicitly or set TETHER_VERSION"
            VERSION="$v"
            ;;
    esac
}

# fetch <url> <out>: download with curl, fail on HTTP ≥ 400.
fetch() {
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) fetch $1 → $2"
        return 0
    fi
    curl -fsSL --retry 3 "$1" -o "$2" || die "download failed: $1"
}

# verify_sha <file> <hash-hex>: bail if hash mismatches.
# Auto-detects SHA-256 (64 hex chars) vs SHA-512 (128 hex chars)
# from the expected length. nats-server SHA256SUMS is sha256;
# Caddy caddy_<ver>_checksums.txt is sha512; tether's own
# SHA256SUMS (goreleaser default) is sha256.
verify_sha() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (skip) verify_sha $1"
        return 0
    fi
    expect_len=${#2}
    case "$expect_len" in
        64)  algo="sha256"; bits=256 ;;
        128) algo="sha512"; bits=512 ;;
        *)   die "verify_sha: unrecognized hash length $expect_len for $1 (need 64 or 128)" ;;
    esac
    have=""
    if command -v ${algo}sum >/dev/null 2>&1; then
        have=$(${algo}sum "$1" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        have=$(shasum -a $bits "$1" | awk '{print $1}')
    else
        die "neither ${algo}sum nor shasum found; cannot verify download"
    fi
    if [ "$have" != "$2" ]; then
        die "$algo mismatch: got $have, want $2"
    fi
}

# extract_to <tarball> <dest_dir>
extract_to() {
    run "tar -xzf '$1' -C '$2'"
}

# place_binary <src_tarball> <dest_dir>: extract tether to dest.
# Goreleaser keeps tether, LICENSE and README at the archive root; Linux
# releases also carry the support binaries, catalog and release manifest.
place_binary() {
    _pb_src="$1"
    _pb_dest="$2"
    run "mkdir -p '$_pb_dest'"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) extract '$_pb_src' → '$_pb_dest/tether'"
        return 0
    fi
    _pb_tmp=$(mktemp -d)
    register_install_temp "$_pb_tmp"
    # A verified archive must still have a safe layout: symlinks/hardlinks and
    # traversal must never turn extraction into writes outside the staging tree.
    # origin: hot-update S9 plan §3.1 / §6.5 / T54
    tar -tzf "$_pb_src" | awk '/^\// || /(^|\/)\.\.(\/|$)/ {bad=1} {if (seen[$0]++) bad=1} END {exit bad}' || die "unsafe or duplicate release archive path"
    tar -tvzf "$_pb_src" | awk 'substr($0,1,1) != "-" && substr($0,1,1) != "d" {bad=1} END {exit bad}' || die "release archive contains links or special files"
    tar -xzf "$_pb_src" -C "$_pb_tmp"
    [ -f "$_pb_tmp/tether" ] && [ ! -L "$_pb_tmp/tether" ] || die "release archive is missing tether"
    if [ "$ROLE" = broker ]; then stage_infra_support "$_pb_tmp"; fi
    install -m 0755 "$_pb_tmp/tether" "$_pb_dest/.tether.new"
    mv -f "$_pb_dest/.tether.new" "$_pb_dest/tether"
    record_owned_file "$_pb_dest/tether"
    rm -rf "$_pb_tmp"
}

# A local binary is already verified by the operator's supply manifest. Copy it
# beside the canonical entry and rename, so a running inode is never truncated.
# This is an explicit cold installation, not a hot upgrade or artifact receipt.
# origin: hot-update S8 plan §5.2 / §7.2 / P20
install_local_binary() {
    [ -n "$LOCAL_BIN" ] || return 0
    [ -f "$LOCAL_BIN" ] || die "--bin must name a regular local file"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) install -m 0755 $LOCAL_BIN $1/.tether.new"
        log "  + (dry-run) mv -f $1/.tether.new $1/tether"
    else
        install -m 0755 "$LOCAL_BIN" "$1/.tether.new"
        mv -f "$1/.tether.new" "$1/tether"
    fi
}

# place_completion <src_tarball>: extract the bundled zsh completion
# (completions/_tether in the tarball, pre-generated by goreleaser
# before-hook) into the first writable directory on zsh's default
# fpath, with a per-user fallback. Silent no-op when the tarball is
# absent (--skip-download / --dry-run) or doesn't ship the file
# (older releases). install.sh MUST NOT invoke the tether binary
# itself (K.0 §2 / test/p10/install_sh_test.go), which is why the
# completion is generated upstream at release time.
place_completion() {
    src="$1"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) zsh completion"
        return 0
    fi
    tmp=$(mktemp -d)
    register_install_temp "$tmp"
    if ! tar -xzf "$src" -C "$tmp" "completions/_tether" 2>/dev/null; then
        rm -rf "$tmp"
        log "  + (skip) zsh completion (not bundled in this release)"
        return 0
    fi
    # Probe the same fpath dirs zsh searches by default. Preference
    # order: Apple Silicon Homebrew, Intel Homebrew, distro-system,
    # then a per-user fallback that requires an fpath edit (we tell
    # the user explicitly).
    dir=""
    user_local=0
    for cand in \
        /opt/homebrew/share/zsh/site-functions \
        /usr/local/share/zsh/site-functions \
        /usr/share/zsh/site-functions
    do
        if [ -d "$cand" ] && [ -w "$cand" ]; then
            dir="$cand"
            break
        fi
    done
    if [ -z "$dir" ]; then
        dir="$HOME/.zsh/completions"
        mkdir -p "$dir"
        user_local=1
    fi
    mv "$tmp/completions/_tether" "$dir/_tether"
    rm -rf "$tmp"
    log "  ✔ zsh completion → $dir/_tether"
    if [ "$user_local" -eq 1 ]; then
        log "    Note: $dir is not in zsh's fpath by default. Add to ~/.zshrc:"
        log "          fpath=($dir \$fpath)"
    fi
    # macOS ships zsh but does NOT enable compsys in /etc/zshrc; many
    # Linux distros do. Always print the hint — cheap insurance, and
    # harmless if the user's shell framework already does it.
    log "    Note: if 'tether <Tab>' does nothing in a new shell, add to ~/.zshrc:"
    log "          autoload -Uz compinit && compinit"
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
    # macOS support is intentionally limited to --role ctl. agent needs
    # systemd user units + setsid for the documented start path, and we
    # don't ship a launchd equivalent. Fail fast instead of laying down
    # files that have no working start command on this OS.
    [ "$OS" = "darwin" ] && die "--role agent is not supported on macOS (only --role ctl is)"
    log "tether install (role=agent, version=$VERSION, os=$OS, arch=$ARCH)"

    run "mkdir -p '$BIN_DIR'"
    run "mkdir -p '$SESSION_DIR' '$SESSION_DIR/keys'"
    # 0700 across the per-session tree (architecture K.1 mandate).
    run "chmod 700 '$HOME/.tether' '$HOME/.tether/agent' '$SESSION_DIR' '$SESSION_DIR/keys'"

    if [ "$SKIP_DOWNLOAD" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        TARBALL=$(mktemp)
        register_install_temp "$TARBALL"
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        log "  + downloading $TARBALL_URL"
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        register_install_temp "$SHA_FILE"
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 for $(basename "$TARBALL_URL") in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$BIN_DIR"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
    fi

    install_local_binary "$BIN_DIR"

    # Derive tunnel_addr from broker URL: split-ports model (A.3)
    # puts frps on host:7000. Host extraction handles wss://, ws://,
    # and bare host. Port is fixed at 7000 (architecture A.3).
    BROKER_HOST=$(printf '%s' "$BROKER_URL" | sed -E 's#^[a-z]+://##; s#[:/].*##')
    TUNNEL_ADDR="${BROKER_HOST}:7000"

    # agent.yaml: enough for `tether agent --session <sid>` to find
    # the broker without operator-supplied flags. PIN deliberately
    # NOT persisted (it's a one-time bootstrap secret).
    if [ "$DRY_RUN" -eq 0 ]; then
        # UNQUOTED heredoc ON PURPOSE: expands $BROKER_URL/$SESSION/$NID/$TUNNEL_ADDR into
        # the config. The commented allow_roots block below is literal — no $ in it.
        config_dest "$SESSION_DIR/agent.yaml"
        cat > "$CONFIG_DEST" <<EOF
broker_url: $BROKER_URL
session: $SESSION
nid: $NID
tunnel_addr: $TUNNEL_ADDR
# file transfer (tether push / pull) — optional, OPEN by default:
# with no allow_roots key, push/pull can reach any path the agent user
# can (the same reach as run/exec). Uncomment to narrow, or disable:
# file_transfer:
#   allow_roots:           # narrow push/pull to these absolute prefixes
#     - /srv/data
#     - /tmp
#   # allow_roots: []      # explicit empty list disables push/pull entirely
#
# logging — optional, ALREADY BOUNDED without any of these keys.
# The agent's binary default writes a size-capped, rotating log to
# <home>/agent/<session>/agent.log, and re-points its own fd 2 at a bounded
# agent.boot.err beside it so panics and stacktraces cannot vanish. These
# knobs only RESIZE that; leaving them out is a supported choice, not an
# unbounded one. They are listed here because the broker's broker.yaml is
# written with its equivalents spelled out, and an operator comparing the two
# would otherwise conclude the agent has no cap at all.
# log_file: /var/log/tether/agent.log   # '-' means stderr (opts OUT of the cap)
# log_max_size_mb: 50
# log_max_backups: 2
#
# session proxy (tether proxy, P13) — optional, PARTICIPATE by default:
# with no proxy key this node serves as a proxy egress whenever the session
# owner runs 'proxy on'. Set participate: false ONLY on a node that cannot
# reach the broker's tunnel port (e.g. an egress firewall blocks :7000) so
# it stops dialing a tunnel it can never establish. #78 caveat: agent.yaml
# is strict-parsed — once written, a tether OLDER than this key refuses to
# boot (matters if this node might roll back).
# proxy:
#   participate: false
EOF
        # 600 on the .new file too: it can carry the same secrets as the real one.
        chmod 600 "$CONFIG_DEST"
    else
        log "  + (dry-run) write $SESSION_DIR/agent.yaml"
        if [ -e "$SESSION_DIR/agent.yaml" ]; then
            dryrun_config_policy
        fi
    fi

    # UNQUOTED heredoc ON PURPOSE: the next-steps banner must show the operator their real
    # paths/ids — $BIN_DIR, $SESSION_DIR, $SESSION, $NID, ${PIN:+ --pin $PIN}. Note the
    # trailing `\\` on the setsid line is also unquoted-heredoc-sensitive: it renders as one
    # backslash (a shell line continuation the operator can paste), which is the intent.
    # BEFORE the ✔ block, not after it.
    #
    # origin: prerelease audit round 2, K-F7 and CC-5. The success banner says "agent
    # config written", which is FALSE on a re-run that kept an existing file — and the
    # kept-config report used to print after it, where an operator who has already read a
    # ✔ does not look. Report first, then claim success.
    report_kept_configs
    cat <<EOF

✔ tether installed to $BIN_DIR/tether
✔ agent config: $SESSION_DIR/agent.yaml

To start the agent now (architecture K.1; install.sh does NOT start anything):
    setsid nohup $BIN_DIR/tether agent --session $SESSION --nid $NID${PIN:+ --pin $PIN} \\
      1>/dev/null 2>> $SESSION_DIR/agent.boot.err &

    # h1 F: the agent writes its own size-capped rotating log to
    # $SESSION_DIR/agent.log (50MB x 2) — do NOT redirect stdout there, or the
    # shell's unbounded append fd fights the in-process cap. stderr carries
    # only pre-logger boot output; the agent re-points fd 2 at agent.boot.err
    # itself once started, so this redirect just covers the boot window.

For auto-start across logins (optional):
    $BIN_DIR/tether agent --install-user-service --session $SESSION --nid $NID
    # generates ~/.config/systemd/user/tether-agent@$SESSION.service
EOF
    # The parenthetical this replaced ("see the note above…") was on the agent ✔ line
    # only, and the broker half had nothing at all — round 2, C7. One helper, both
    # halves, and it names what is actually on disk instead of pointing at a note.
    banner_kept_caveat
    report_kept_configs
}

# ~/.tether IS NOT THE AGENT'S DIRECTORY. It is this user's whole tether identity.
#
# origin: prerelease audit deploy-release-docs/#48. Uninstalling one agent deleted
# ~/.tether wholesale, which takes keys/default.nk with it — the ctl private key. That
# key cannot be regenerated: it is the owner fingerprint on every session this user
# created, so losing it orphans all of them, not just the one being uninstalled. The
# comparison that settles it is right below: uninstall_ctl deliberately does not touch
# this directory at all.
#
# With --session, remove only that session's subtree. Without one, the whole-tree
# removal is kept — an operator who asks for it may genuinely want it — but it is no
# longer silent.
uninstall_agent() {
    BIN_DIR="${PREFIX:-$HOME/.local/bin}"
    log "tether uninstall (role=agent)"
    run "rm -f '$BIN_DIR/tether'"
    if [ -n "$SESSION" ]; then
        run "rm -rf '$HOME/.tether/agent/$SESSION'"
        log "  ✔ removed $BIN_DIR/tether and ~/.tether/agent/$SESSION"
        log "    (kept ~/.tether — it holds keys/default.nk, this user's ctl identity)"
        return 0
    fi
    log ""
    log "  ⚠ REMOVING ALL OF ~/.tether, including keys/default.nk — this user's ctl private key."
    log "    That key cannot be regenerated. It is the owner fingerprint on every session this"
    log "    user created, so every one of them is orphaned, not just this agent's."
    log "    Pass --session <sid> to remove only one agent's directory instead."
    log ""
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
        register_install_temp "$TARBALL"
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        register_install_temp "$SHA_FILE"
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$BIN_DIR"
        place_completion "$TARBALL"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
        place_completion ""
    fi

    install_local_binary "$BIN_DIR"

    # UNQUOTED heredoc ON PURPOSE: expands $BIN_DIR (the probed install dir).
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
    # Mirror the install-time directory probe: rm from every fpath
    # candidate so a previous install into a now-unwritable dir is
    # still cleaned up.
    for cand in \
        /opt/homebrew/share/zsh/site-functions \
        /usr/local/share/zsh/site-functions \
        /usr/share/zsh/site-functions \
        "$HOME/.zsh/completions"
    do
        [ -f "$cand/_tether" ] && run "rm -f '$cand/_tether'"
    done
    log "  ✔ removed $BIN_DIR/tether"
}

# -- role: broker ----------------------------------------------------------

# enable_broker_units (#76): `systemctl enable` (NO --now) the three broker
# units so a host reboot brings the stack back. Enable creates wants/
# symlinks only — no process starts, `pgrep tether` stays empty, so the
# K.0 §2 never-start invariant is untouched. The single production broker
# shipped disabled once and a reboot would have taken the fleet offline;
# a printed banner had already proven insufficient. --no-enable opts out.
# ENABLED_UNITS records whether enable_broker_units actually created boot
# symlinks (review M4), so the closing banner cannot claim "ENABLED for boot"
# on a --no-enable run or a systemd-less host where enable was skipped —
# claiming it there would resurrect #76 on exactly the environment-guard path.
ENABLED_UNITS=0

# host_systemd_is_the_target reports whether this run may mutate the SERVICE MANAGER.
#
# origin: prerelease audit external review M-4. TETHER_INSTALL_ROOT was described as
# making the whole broker install hermetic, and it redirects every FILE — but
# `systemctl daemon-reload/enable` and `systemctl disable` are not files. They were
# still issued against the host, so a "hermetic" install under a temp root could enable
# the host's own same-named units, and a redirected uninstall could disable the real
# nats-server / tether-broker / caddy at boot. Files staying inside the root does not
# make a systemctl side effect hermetic.
#
# The existing real-install helper always appended --no-enable, which is exactly why no
# test saw the install half; the uninstall half had no live-systemd coverage at all.
#
# Used SYMMETRICALLY by enable_broker_units and the uninstall path — an asymmetric guard
# is how the install side got fixed once before while uninstall kept writing to the host.
host_systemd_is_the_target() {
    [ -z "${TETHER_INSTALL_ROOT:-}" ]
}

# log_would_run_systemctl records the service-manager call this run deliberately did not
# make. Deliberately NOT phrased as "(dry-run) systemctl …": that prefix is the shape a
# real preview takes, and a seam run must be distinguishable from one.
log_would_run_systemctl() {
    log "  + TETHER_INSTALL_ROOT: host service manager NOT touched (would have run: $*)"
}

enable_broker_units() {
    # Reinstall does not publish effective units or change enablement. Staged
    # templates take effect only after the product's authorized domain CAS.
    # origin: hot-update S9 plan §6.4 / T54 / M27
    if [ "$INSTALL_STAGE_ONLY" -eq 1 ]; then
        log "  + stage-only: service manager and boot enablement kept"
        return 0
    fi
    if ! host_systemd_is_the_target; then
        log_would_run_systemctl "systemctl daemon-reload && systemctl enable nats-server tether-broker caddy"
        return 0
    fi
    if [ "$NO_ENABLE" -eq 1 ]; then
        log "  + --no-enable: units left disabled; enable later with: systemctl enable --now nats-server tether-broker caddy"
        return 0
    fi
    # Only probe for a live systemd when actually executing: a dry-run must
    # preview the same intent on any build host (CI containers have no
    # /run/systemd/system, and a host-dependent dry-run is unassertable).
    if [ "$DRY_RUN" -eq 0 ]; then
        if ! command -v systemctl >/dev/null 2>&1 || [ ! -d /run/systemd/system ]; then
            log "  ! systemd is not running here — units NOT enabled. On the real host run: systemctl enable --now nats-server tether-broker caddy"
            return 0
        fi
    fi
    run "systemctl daemon-reload" || die "systemctl daemon-reload failed"
    run "systemctl enable nats-server tether-broker caddy" \
        || die "systemctl enable failed — units are written but the broker would NOT come back after a reboot; fix and re-run, or enable manually"
    log "  ✔ units enabled for boot (nats-server tether-broker caddy) — NOT started"
    ENABLED_UNITS=1
}

# write_journald_dropin (#77): give the broker host a tight journald size
# cap. journald's built-in default is min(10% of the fs, 4G) — NOT unbounded,
# but far too generous for a small disk sharing space with jetstream + DB
# (19G production disk ⇒ 1.9G of journal, measured live 2026-08-11). The cap
# is derived from the /var/log filesystem in three tiers and written as a
# drop-in ONLY when no other file already sets SystemMaxUse explicitly — an
# operator's own setting is always respected. Uniform M unit so tooling can
# grep a single shape.
#
# TETHER_JOURNALD_ROOT is a TEST-ONLY seam (hermetic tests point it at a
# tmpdir); production always resolves /etc/systemd.
write_journald_dropin() {
    [ "$INSTALL_STAGE_ONLY" -eq 0 ] || return 0
    # TETHER_INSTALL_ROOT (round 2, G2/K-F10) redirects it too, so ONE variable makes
    # a whole broker install hermetic. The dedicated TETHER_JOURNALD_ROOT still wins —
    # tests that only need this one file keep pointing it wherever they like.
    _jroot="${TETHER_JOURNALD_ROOT:-${TETHER_INSTALL_ROOT:-}/etc/systemd}"
    _dropin_dir="$_jroot/journald.conf.d"
    _dropin="$_dropin_dir/60-tether.conf"
    # review F3 OWNERSHIP: our file is identified by a stable MARKER on its
    # FIRST line, never by its path. A same-name file that lacks the marker is
    # an operator / site-policy file that merely collided on the name — we must
    # NOT overwrite or delete it (a root installer silently clobbering operator
    # config is the defect). Ownership is proven, not assumed.
    _marker='# managed-by: tether-install.sh (#77 journald cap)'

    # F3: a same-name file WE did not write ⇒ fail closed: respect it, return.
    if [ -f "$_dropin" ] && ! head -n1 "$_dropin" 2>/dev/null | grep -qF "$_marker"; then
        log "  + $_dropin exists but is NOT tether-owned (no marker) — treating as an operator/site-policy file; not overwriting"
        return 0
    fi

    # Respect any UNCOMMENTED SystemMaxUse= elsewhere. Only OUR OWN marked
    # drop-in is excluded from the scan (a re-install overwrites it to pick up
    # a grown disk); a foreign same-name file was already handled above. A
    # commented-out `#SystemMaxUse=` is NOT a setting (the live incident host
    # had exactly that commented stub). review Mi6: the pattern allows spaces
    # around '=' (systemd ignores them). review N9: also scan the vendor/runtime
    # drop-in dirs whose settings ours would otherwise shadow on filename sort.
    for _f in "$_jroot/journald.conf" "$_dropin_dir"/*.conf \
              /run/systemd/journald.conf.d/*.conf /usr/lib/systemd/journald.conf.d/*.conf; do
        [ -f "$_f" ] || continue
        if [ "$_f" = "$_dropin" ] && head -n1 "$_f" 2>/dev/null | grep -qF "$_marker"; then
            continue   # our own marked file — never counts as an operator setting
        fi
        if grep -q '^[[:space:]]*SystemMaxUse[[:space:]]*=' "$_f" 2>/dev/null; then
            log "  + journald SystemMaxUse already set in $_f (operator setting respected)"
            # review M5: remove our OWN (marker-proven) stale drop-in so it
            # stops shadowing the operator's setting (journald merges drop-ins
            # over the main config by filename order). Only OUR file is removed.
            if [ -f "$_dropin" ] && head -n1 "$_dropin" 2>/dev/null | grep -qF "$_marker"; then
                run "rm -f '$_dropin'"
                log "  + removed our stale journald drop-in $_dropin (operator setting now authoritative)"
            fi
            return 0
        fi
    done
    # Tier by the /var/log filesystem size (POSIX df -Pk; blocks of 1K). review
    # Mi9: a non-numeric df field (exotic df / pseudo-fs) falls to the SMALLEST
    # tier, not the largest — failure direction must be conservative.
    _fs_kb=$(df -Pk /var/log 2>/dev/null | awk 'NR==2 {print $2}')
    case "$_fs_kb" in ''|*[!0-9]*) _fs_kb=0 ;; esac
    if [ "$_fs_kb" -lt 10485760 ]; then
        _cap="200M"        # < 10 GiB
    elif [ "$_fs_kb" -lt 41943040 ]; then
        _cap="500M"        # < 40 GiB (the 19G production disk lands here)
    else
        _cap="1024M"       # >= 40 GiB
    fi
    if [ "$DRY_RUN" -eq 0 ]; then
        mkdir -p "$_dropin_dir"
        # review F2: QUOTED heredoc — the body is fully literal, so the phrase
        # "systemctl restart systemd-journald" in the comment can never be a
        # command substitution executed as root at install time (an unquoted
        # here-document with backticks did exactly that). Only the cap VALUE
        # varies, so it is appended by printf, not expanded inside the heredoc.
        {
            cat <<'JEOF'
# managed-by: tether-install.sh (#77 journald cap)
# Safe to delete; removed by tether install.sh --uninstall.
# journald's built-in default is min(10% of the filesystem, 4G): not unbounded,
# but too generous for a small broker disk shared with jetstream + tether.db.
# This cap is derived from the /var/log filesystem size at install time.
# An explicit SystemMaxUse anywhere else always wins (install.sh skips this
# file when one exists). Takes effect on the next journald restart or reboot.
[Journal]
JEOF
            printf 'SystemMaxUse=%s\n' "$_cap"
        } > "$_dropin"
        chmod 644 "$_dropin"
        log "  ✔ journald cap written: $_dropin (SystemMaxUse=$_cap)"
    else
        log "  + (dry-run) write $_dropin (SystemMaxUse=$_cap)"
    fi
    WROTE_JOURNALD_DROPIN=1
}

# All infrastructure paths resolve from one seam. Policy, journal, credentials,
# ACME selectors and lifetime store locks are bootstrap/runtime responsibilities;
# installing templates must never synthesize any of those authorities.
# origin: hot-update S9 plan §6.4 / T54
broker_paths() {
    BIN_DIR="${PREFIX:-${TETHER_INSTALL_ROOT:-}/usr/local/bin}"
    ETC_DIR="${TETHER_INSTALL_ROOT:-}/etc/tether"
    LIB_DIR="${TETHER_INSTALL_ROOT:-}/var/lib/tether"
    LOG_DIR="${TETHER_INSTALL_ROOT:-}/var/log/tether"
    RUN_DIR="${TETHER_INSTALL_ROOT:-}/var/run/tether"
    SYSTEMD_DIR="${TETHER_INSTALL_ROOT:-}/etc/systemd/system"
    LIBEXEC_DIR="${TETHER_INSTALL_ROOT:-}/usr/local/libexec"
    INFRA_DIR="${TETHER_INSTALL_ROOT:-}/var/lib/tether-infra"
    INFRA_ETC_DIR="$ETC_DIR/infra"
    INFRA_RUN_DIR="${TETHER_INSTALL_ROOT:-}/run/tether-infra"
}
# New root paths cannot alias operator trees. Check every existing ancestor,
# including dangling symlinks, before creating directories or changing modes.
# Fixed systemd paths also cannot contain shell or systemd substitutions.
# origin: hot-update S9 plan §6.4 / T54
infra_safe_path() {
    case "$1" in /*) ;; *) die "infrastructure path must be absolute: $1" ;; esac
    case "$1" in *[!A-Za-z0-9_./@-]*|*/../*|*/./*|*/..|*/.|*//*) die "infrastructure path must be clean: $1" ;; esac
    _isp="$1"
    while [ "$_isp" != / ]; do
        [ ! -L "$_isp" ] || die "infrastructure path must not be a symlink: $_isp"
        _isp=${_isp%/*}
        [ -n "$_isp" ] || _isp=/
    done
}
file_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}
# A filename never proves ownership. The root-private receipt binds its exact
# path and current bytes; changed files and same-name operator files survive.
# Receipts have their own content marker, including for binary targets that
# cannot embed a shell comment. No recursive deletion touches business data.
# origin: hot-update S9 plan §6.4 / T54 / M27
record_owned_file() {
    [ "${ROLE:-}" = broker ] && [ -n "${INFRA_DIR:-}" ] || return 0
    [ "$DRY_RUN" -eq 0 ] || return 0
    _rof_path="$1"
    [ -f "$_rof_path" ] && [ ! -L "$_rof_path" ] || die "managed target is not a regular file"
    _rof_key=$(printf '%s' "$_rof_path" | sha256sum | awk '{print $1}')
    _rof_hash=$(file_sha256 "$_rof_path")
    _rof_tmp=$(mktemp "$INFRA_DIR/ownership/.receipt.XXXXXX")
    register_install_temp "$_rof_tmp"
    _rof_metadata=$(stat -c '%u:%g:%a:%h' "$_rof_path")
    printf '# managed-by: tether-install.sh (content ownership v1)\n%s\n%s\n%s\n' "$_rof_path" "$_rof_hash" "$_rof_metadata" > "$_rof_tmp"
    chmod 600 "$_rof_tmp"
    mv -f "$_rof_tmp" "$INFRA_DIR/ownership/$_rof_key"
}
owned_file() {
    [ -n "${INFRA_DIR:-}" ] && [ -f "$1" ] && [ ! -L "$1" ] || return 1
    _of_key=$(printf '%s' "$1" | sha256sum | awk '{print $1}')
    _of_receipt="$INFRA_DIR/ownership/$_of_key"
    [ -f "$_of_receipt" ] && [ ! -L "$_of_receipt" ] || return 1
    [ "$(head -n 1 "$_of_receipt")" = '# managed-by: tether-install.sh (content ownership v1)' ] || return 1
    [ "$(sed -n '2p' "$_of_receipt")" = "$1" ] || return 1
    [ "$(sed -n '3p' "$_of_receipt")" = "$(file_sha256 "$1")" ] || return 1
    [ "$(sed -n '4p' "$_of_receipt")" = "$(stat -c '%u:%g:%a:%h' "$1")" ]
}
remove_owned_file() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) rm -f '$1' (only with matching content ownership receipt)"
    elif owned_file "$1"; then
        rm -f "$1"
        rm -f "$_of_receipt"
    elif [ -e "$1" ] || [ -L "$1" ]; then
        log "  + kept operator or modified file: $1"
    fi
}
infra_install_directories() {
    for _iid in "$INFRA_DIR" "$INFRA_DIR/ownership" "$INFRA_DIR/artifacts"; do
        infra_safe_path "$_iid"
        if [ "$DRY_RUN" -eq 1 ]; then
            log "  + (dry-run) install -d -m 0700 $_iid"
        elif [ -d "$_iid" ]; then
            [ "$(stat -c '%a' "$_iid")" = 700 ] || die "infrastructure directory must have mode 0700: $_iid"
            if host_systemd_is_the_target; then
                [ "$(stat -c '%u' "$_iid")" = 0 ] || die "infrastructure directory must be root-owned: $_iid"
            fi
        else
            install -d -m 0700 "$_iid"
        fi
    done
}
# An installed root policy or either journal is an authority, not a shell-readable
# idle flag. Unknown/completed/pending all require the root product's retirement
# CAS. Ordinary reinstall therefore stages only, and uninstall refuses before any
# host/file effect. The permanent journal lock serializes bootstrap publication;
# it is never truncated, renamed or removed by cleanup.
# origin: hot-update S9 plan §6.4 / T54 / M27
infra_has_authority() {
    for _iha_path in "$INFRA_ETC_DIR/policy.json" "$INFRA_DIR/policy.json" "$INFRA_DIR/journal.json" "$LIB_DIR/upgrade-state.json" \
        "$INFRA_RUN_DIR/control.sock" "$INFRA_RUN_DIR/host.sock" "$RUN_DIR/admin.sock"; do
        if [ -e "$_iha_path" ] || [ -L "$_iha_path" ]; then return 0; fi
    done
    if [ -L "$LIB_DIR/upgrade-control" ]; then return 0; fi
    if [ -e "$LIB_DIR/upgrade-control" ]; then
        [ -d "$LIB_DIR/upgrade-control" ] || return 0
        # An unreadable registry is unknown authority, never an empty domain.
        # origin: hot-update S9 plan §6.4 / T54 / M27
        _iha_domains=$(find "$LIB_DIR/upgrade-control" -name '*.domain' -print 2>/dev/null) || return 0
        [ -z "$_iha_domains" ] || return 0
    fi
    return 1
}
infra_install_admission() {
    broker_paths
    for _iia in "$BIN_DIR" "$LIBEXEC_DIR" "$INFRA_DIR" "$INFRA_ETC_DIR" "$SYSTEMD_DIR" "$INFRA_RUN_DIR"; do
        infra_safe_path "$_iia"
    done
    if infra_has_authority || [ -e "$BIN_DIR/tether" ] || [ -e "$BIN_DIR/nats-server" ] || [ -e "$BIN_DIR/caddy" ] ||
       [ -e "$LIBEXEC_DIR/tether-infra-helper" ] || [ -e "$LIBEXEC_DIR/tether-infra-launch" ] ||
       [ -e "$SYSTEMD_DIR/nats-server.service" ] || [ -e "$SYSTEMD_DIR/caddy.service" ] || [ -e "$SYSTEMD_DIR/tether-broker.service" ] ||
       [ -e "$ETC_DIR/broker.yaml" ] || [ -e "$ETC_DIR/Caddyfile" ] || [ -e "$ETC_DIR/nats.d/nats.conf" ]; then
        INSTALL_STAGE_ONLY=1
        if [ "$FORCE_CONFIG" -eq 1 ]; then
            # Preserve the explicit manual cold override on a stopped host.
            # Registered, running or unknown domains always require product CAS.
            # origin: hot-update S9 plan §6.4 / T54
            infra_has_authority && die "infra_busy: --force-config requires a retired upgrade domain"
            infra_business_running && die "infra_busy: --force-config requires stopped business processes"
            INSTALL_STAGE_ONLY=0
            log "  + explicit --force-config: cold installation on an unregistered, stopped host"
        else
            log "  + existing installation: stage-only; active/previous selectors, units and runtime metadata are kept"
        fi
    fi
}
infra_lock_installation() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    # Stage-only writes no active state and must work while the business domain
    # is busy. Only an initial publication/retirement needs the journal barrier.
    # origin: hot-update S9 plan §6.4 / T54 / M27
    [ "$INSTALL_STAGE_ONLY" -eq 0 ] || return 0
    infra_safe_path "$INFRA_DIR/journal.lock"
    if [ -e "$INFRA_DIR/journal.lock" ]; then
        [ -f "$INFRA_DIR/journal.lock" ] && [ "$(stat -c '%h:%a' "$INFRA_DIR/journal.lock")" = '1:600' ] || die "invalid permanent journal lock"
    else
        (umask 077; set -C; : > "$INFRA_DIR/journal.lock") || die "journal lock creation raced; retry installation"
    fi
    exec 9<> "$INFRA_DIR/journal.lock"
    flock -n 9 || die "infra_busy: root journal is locked"
    INFRA_INSTALL_LOCKED=1
    # Recheck after acquisition: bootstrap may have committed after the preview.
    if infra_has_authority; then
        INSTALL_STAGE_ONLY=1
        [ "$FORCE_CONFIG" -eq 0 ] || die "infra_busy: authority appeared before installation"
    fi
}
# The support manifest is a bounded, closed release format, not caller argv.
# Its source identity/protocol/previous-reader and all three file hashes are
# mandatory. ELF headers are inspected as bytes; no candidate --version or
# key generator is executed during installation, even with --skip-download.
# origin: hot-update S9 plan §3.1 / §6.5 / T54
validate_infra_support() {
    _vis="$1"
    infra_safe_path "$_vis"
    for _vis_name in tether-infra-helper tether-infra-launch infra-catalog.json infra-support.manifest; do
        _vis_file="$_vis/$_vis_name"
        [ -f "$_vis_file" ] && [ ! -L "$_vis_file" ] && [ "$(stat -c '%h' "$_vis_file")" = 1 ] || die "missing or aliased support file: $_vis_name"
    done
    [ "$(wc -c < "$_vis/infra-support.manifest")" -le 4096 ] || die "support manifest is too large"
    awk -v release="$VERSION" -v arch="$ARCH" '
        function digest(s) { return length(s) == 64 && s !~ /[^0-9a-f]/ && s !~ /^0+$/ }
        $1 == "format" && NF == 2 && $2 == "1" { seen[$1]++; next }
        $1 == "release" && NF == 2 && $2 == release { seen[$1]++; next }
        $1 == "goos" && NF == 2 && $2 == "linux" { seen[$1]++; next }
        $1 == "goarch" && NF == 2 && $2 == arch { seen[$1]++; next }
        $1 == "protocol" && NF == 2 && $2 == "1" { seen[$1]++; next }
        $1 == "previous-reader" && NF == 2 && $2 == "1" { seen[$1]++; next }
        $1 == "source-sha256" && NF == 2 && digest($2) { seen[$1]++; next }
        $1 == "sha256" && NF == 3 && digest($2) && ($3 == "tether-infra-helper" || $3 == "tether-infra-launch" || $3 == "infra-catalog.json") { seen[$3]++; next }
        { bad = 1 }
        END {
            split("format release goos goarch protocol previous-reader source-sha256 tether-infra-helper tether-infra-launch infra-catalog.json", required, " ")
            for (i in required) if (seen[required[i]] != 1) bad = 1
            exit bad
        }
    ' "$_vis/infra-support.manifest" || die "invalid release/platform/protocol support manifest"
    for _vis_name in tether-infra-helper tether-infra-launch infra-catalog.json; do
        _vis_hash=$(awk -v name="$_vis_name" '$1 == "sha256" && $3 == name {print $2}' "$_vis/infra-support.manifest")
        verify_sha "$_vis/$_vis_name" "$_vis_hash"
    done
    for _vis_name in tether-infra-helper tether-infra-launch; do
        [ -x "$_vis/$_vis_name" ] || die "support binary is not executable: $_vis_name"
        validate_infra_elf "$_vis/$_vis_name"
        case "$(stat -c '%a' "$_vis/$_vis_name")" in 755|555|500|700) ;; *) die "unsafe support binary permissions: $_vis_name" ;; esac
    done
}
validate_infra_elf() {
    _vie_header=$(od -An -v -tx1 -N20 "$1" | tr -d ' \n')
    [ "${#_vie_header}" -eq 40 ] || die "binary has a truncated ELF header"
    case "$_vie_header" in 7f454c46020101*) ;; *) die "binary is not little-endian ELF64" ;; esac
    _vie_machine=$(printf '%s' "$_vie_header" | cut -c37-40)
    case "$ARCH:$_vie_machine" in amd64:3e00|arm64:b700) ;; *) die "binary has the wrong ELF platform" ;; esac
}
# Offline supply has the same content checks as an archive install. The three
# cold binaries are accompanied by an exact SHA256SUMS subset; support hashes
# remain in the release-bound manifest. --bin replaces only the tether input.
# origin: hot-update S9 plan §6.4 / T54
validate_offline_broker_binaries() {
    _iob_source="${INFRA_SUPPORT_DIR:-$BIN_DIR}"
    validate_infra_support "$_iob_source"
    _iob_sums="$_iob_source/broker-binaries.sha256"
    [ -f "$_iob_sums" ] && [ ! -L "$_iob_sums" ] || die "--skip-download requires broker-binaries.sha256"
    [ "$(wc -c < "$_iob_sums")" -le 1024 ] || die "offline checksum list is too large"
    awk 'NF != 2 || length($1) != 64 || $1 ~ /[^0-9a-f]/ || ($2 != "tether" && $2 != "nats-server" && $2 != "caddy") {bad=1}
         {seen[$2]++} END {exit bad || seen["tether"] != 1 || seen["nats-server"] != 1 || seen["caddy"] != 1}' "$_iob_sums" || die "invalid offline binary checksum list"
    for _iob_name in tether nats-server caddy; do
        _iob_file="$_iob_source/$_iob_name"
        if [ "$_iob_name" = tether ] && [ -n "$LOCAL_BIN" ]; then _iob_file="$LOCAL_BIN"; fi
        [ -f "$_iob_file" ] && [ ! -L "$_iob_file" ] && [ -x "$_iob_file" ] || die "missing offline broker binary: $_iob_name"
        validate_infra_elf "$_iob_file"
        _iob_hash=$(awk -v name="$_iob_name" '$2 == name {print $1}' "$_iob_sums")
        verify_sha "$_iob_file" "$_iob_hash"
    done
}
install_offline_broker_binaries() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    validate_offline_broker_binaries
    stage_infra_support "$_iob_source"
    for _iob_name in tether nats-server caddy; do
        _iob_file="$_iob_source/$_iob_name"
        if [ "$_iob_name" = tether ] && [ -n "$LOCAL_BIN" ]; then _iob_file="$LOCAL_BIN"; fi
        install -m 0755 "$_iob_file" "$DOWNLOAD_BIN_DIR/.$_iob_name.new"
        _iob_hash=$(awk -v name="$_iob_name" '$2 == name {print $1}' "$_iob_sums")
        verify_sha "$DOWNLOAD_BIN_DIR/.$_iob_name.new" "$_iob_hash"
        mv -f "$DOWNLOAD_BIN_DIR/.$_iob_name.new" "$DOWNLOAD_BIN_DIR/$_iob_name"
        record_owned_file "$DOWNLOAD_BIN_DIR/$_iob_name"
    done
}
stage_broker_binaries() {
    [ "$INSTALL_STAGE_ONLY" -eq 1 ] && [ "$DRY_RUN" -eq 0 ] || return 0
    for _sbb_name in tether nats-server caddy; do
        _sbb_hash=$(file_sha256 "$DOWNLOAD_BIN_DIR/$_sbb_name")
        _sbb_slot="$INFRA_DIR/artifacts/$_sbb_hash"
        infra_safe_path "$_sbb_slot"
        if [ ! -d "$_sbb_slot" ]; then
            _sbb_tmp=$(mktemp -d "$INFRA_DIR/artifacts/.stage.XXXXXX")
            register_install_temp "$_sbb_tmp"
            install -m 0555 "$DOWNLOAD_BIN_DIR/$_sbb_name" "$_sbb_tmp/$_sbb_name"
            [ "$(file_sha256 "$_sbb_tmp/$_sbb_name")" = "$_sbb_hash" ] || die "binary changed during private staging"
            chmod 0555 "$_sbb_tmp"
            sync "$_sbb_tmp/$_sbb_name" "$_sbb_tmp"
            mv -T "$_sbb_tmp" "$_sbb_slot"
            sync "$INFRA_DIR/artifacts"
        else
            [ -f "$_sbb_slot/$_sbb_name" ] && [ ! -L "$_sbb_slot/$_sbb_name" ] &&
                [ "$(file_sha256 "$_sbb_slot/$_sbb_name")" = "$_sbb_hash" ] || die "immutable binary slot changed"
        fi
        record_owned_file "$_sbb_slot/$_sbb_name"
    done
    # --bin is S8's explicit cold replacement, distinct from an ordinary
    # reinstall. Preserve that operation only on an unregistered, stopped host;
    # an enabled domain must use its product transaction, even when idle.
    # origin: hot-update S8 plan §5.2 / P20; hot-update S9 plan §6.4 / T54
    if [ -n "$LOCAL_BIN" ]; then
        infra_has_authority && die "infra_busy: --bin requires a retired upgrade domain"
        infra_business_running && die "infra_busy: --bin requires stopped business processes"
        install -m 0755 "$DOWNLOAD_BIN_DIR/tether" "$BIN_DIR/.tether.new"
        mv -f "$BIN_DIR/.tether.new" "$BIN_DIR/tether"
        record_owned_file "$BIN_DIR/tether"
    fi
    rm -rf "$DOWNLOAD_BIN_DIR"
}
infra_business_running() {
    for _ibr_process in /proc/[0-9]*/exe; do
        _ibr_exe=$(readlink "$_ibr_process" 2>/dev/null) || continue
        case "$_ibr_exe" in
            "$BIN_DIR/tether"|"$BIN_DIR/tether (deleted)"|"$BIN_DIR/nats-server"|"$BIN_DIR/nats-server (deleted)"|"$BIN_DIR/caddy"|"$BIN_DIR/caddy (deleted)"|"$LIBEXEC_DIR/tether-infra-helper"|"$LIBEXEC_DIR/tether-infra-helper (deleted)"|"$LIBEXEC_DIR/tether-infra-launch"|"$LIBEXEC_DIR/tether-infra-launch (deleted)"|"$INFRA_DIR/artifacts/"*) return 0 ;;
        esac
    done
    return 1
}
stage_infra_support() {
    # Guard the staging owner itself: both archive and offline callers use it,
    # and a preview must not validate nonexistent supply or publish copies.
    # origin: hot-update S9 plan §6.4 / T54 / M27
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) stage infrastructure support from $1 into $INFRA_DIR/artifacts and $LIBEXEC_DIR"
        return 0
    fi
    validate_infra_support "$1"
    _sis_source="$1"
    _sis_digest=$(file_sha256 "$_sis_source/infra-support.manifest")
    _sis_slot="$INFRA_DIR/artifacts/$_sis_digest"
    infra_safe_path "$_sis_slot"
    if [ ! -d "$_sis_slot" ]; then
        _sis_tmp=$(mktemp -d "$INFRA_DIR/artifacts/.stage.XXXXXX")
        register_install_temp "$_sis_tmp"
        for _sis_name in tether-infra-helper tether-infra-launch infra-catalog.json infra-support.manifest; do
            install -m 0555 "$_sis_source/$_sis_name" "$_sis_tmp/$_sis_name"
        done
        chmod 0444 "$_sis_tmp/infra-catalog.json" "$_sis_tmp/infra-support.manifest"
        # Hash the final private copies. Source FDs/paths can change after their
        # first check; no executable is selected from that writable input inode.
        validate_infra_support "$_sis_tmp"
        [ "$(file_sha256 "$_sis_tmp/infra-support.manifest")" = "$_sis_digest" ] || die "support manifest changed during copy"
        chmod 0555 "$_sis_tmp"
        sync "$_sis_tmp/tether-infra-helper" "$_sis_tmp/tether-infra-launch" "$_sis_tmp/infra-catalog.json" "$_sis_tmp/infra-support.manifest" "$_sis_tmp"
        mv -T "$_sis_tmp" "$_sis_slot"
        sync "$INFRA_DIR/artifacts"
    else
        validate_infra_support "$_sis_slot"
        [ "$(file_sha256 "$_sis_slot/infra-support.manifest")" = "$_sis_digest" ] || die "immutable support slot changed"
    fi
    # Legacy stage-only installs have no worker capable of activating support.
    # Publish missing fixed entries under the permanent journal barrier, without
    # replacing an existing generation or touching a business entry/unit.
    # origin: S9 internal review rv-priv F4
    (
        if [ "${INFRA_INSTALL_LOCKED:-0}" -eq 0 ]; then
            INSTALL_STAGE_ONLY=0
            infra_lock_installation
        fi
        infra_safe_path "$LIBEXEC_DIR"
        [ -d "$LIBEXEC_DIR" ] || install -d -m 0755 "$LIBEXEC_DIR"
        for _sis_name in tether-infra-helper tether-infra-launch; do
            if [ -e "$LIBEXEC_DIR/$_sis_name" ] || [ -L "$LIBEXEC_DIR/$_sis_name" ]; then
                log "  + existing support entry kept; activate its staged candidate through product update-support: $_sis_name"
            else
                infra_has_authority && die "infra_busy: missing support entry requires authorized repair"
                _sis_entry=$(mktemp "$LIBEXEC_DIR/.support.XXXXXX")
                install -m 0755 "$_sis_slot/$_sis_name" "$_sis_entry"
                [ "$(file_sha256 "$_sis_entry")" = "$(file_sha256 "$_sis_slot/$_sis_name")" ] || die "support entry changed during publication"
                sync "$_sis_entry"
                # link is an atomic create-if-absent, unlike a check then install.
                if ! ln "$_sis_entry" "$LIBEXEC_DIR/$_sis_name"; then
                    rm -f "$_sis_entry"
                    die "support entry publication raced"
                fi
                rm -f "$_sis_entry"
                sync "$LIBEXEC_DIR"
                record_owned_file "$LIBEXEC_DIR/$_sis_name"
            fi
        done
    )
    for _sis_name in tether-infra-helper tether-infra-launch infra-catalog.json infra-support.manifest; do
        record_owned_file "$_sis_slot/$_sis_name"
    done
    log "  + support artifacts staged: $_sis_digest (files_installed only)"
}
# These are preparation templates. P40/bootstrap installs the management plane;
# the runtime persists FenceStart before loading the business drop-ins. Writing
# a live drop-in here would let always/2s race the final ACME copy on retrofit.
# Keep drop-in bytes identical to infrahost.ManagedUnitDropIn (T12).
# origin: hot-update S9 plan §5.3 / §6.4 / T54
write_infra_files() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) prepare $INFRA_ETC_DIR/templates/{tether-infra-helper.socket,tether-infra-helper@.service,nats-server.service.d/90-tether-infra.conf,caddy.service.d/90-tether-infra.conf}"
        log "  + files_installed only; root bootstrap enables management explicitly"
        return 0
    fi
    for _wif_dir in "$INFRA_ETC_DIR" "$INFRA_ETC_DIR/templates" "$INFRA_ETC_DIR/templates/nats-server.service.d" "$INFRA_ETC_DIR/templates/caddy.service.d"; do
        infra_safe_path "$_wif_dir"
        [ -d "$_wif_dir" ] || install -d -m 0755 "$_wif_dir"
    done
    config_dest "$INFRA_ETC_DIR/templates/tether-infra-helper.socket"
    # UNQUOTED heredoc ON PURPOSE: only the redirected runtime socket path expands.
    cat > "$CONFIG_DEST" <<EOF
# tether-infra owned v1
[Unit]
Description=tether infrastructure management socket

[Socket]
ListenStream=$INFRA_RUN_DIR/control.sock
Accept=yes
SocketUser=root
SocketGroup=tether
SocketMode=0660
DirectoryMode=0750
MaxConnections=$INFRA_MAX_CONNECTIONS
Service=tether-infra-helper@.service

[Install]
WantedBy=sockets.target
EOF
    publish_config 0644
    config_dest "$INFRA_ETC_DIR/templates/tether-infra-helper@.service"
    # UNQUOTED heredoc ON PURPOSE: only the redirected libexec entry expands.
    cat > "$CONFIG_DEST" <<EOF
# tether-infra owned v1
[Unit]
Description=tether infrastructure management worker

[Service]
Type=simple
User=root
Group=root
ExecStart=$LIBEXEC_DIR/tether-infra-helper worker
StandardInput=socket
StandardOutput=socket
StandardError=journal
Restart=no
RuntimeMaxSec=${INFRA_WORKER_LIFETIME_S}s
KillMode=control-group
TimeoutStopSec=${INFRA_WORKER_STOP_S}s
EOF
    publish_config 0644
    for _wif_component in nats caddy; do
        if [ "$_wif_component" = nats ]; then _wif_unit=nats-server; _wif_caps=""; else _wif_unit=caddy; _wif_caps=CAP_NET_BIND_SERVICE; fi
        config_dest "$INFRA_ETC_DIR/templates/$_wif_unit.service.d/90-tether-infra.conf"
        # UNQUOTED heredoc ON PURPOSE: the component and sole bind capability expand.
        cat > "$CONFIG_DEST" <<EOF
# tether-infra owned v1
[Service]
User=tether
Group=tether
SupplementaryGroups=
AmbientCapabilities=
CapabilityBoundingSet=
AmbientCapabilities=$_wif_caps
CapabilityBoundingSet=$_wif_caps
NoNewPrivileges=yes
ExecStart=
ExecStart=$LIBEXEC_DIR/tether-infra-launch --component=$_wif_component
Restart=always
RestartSec=2
RuntimeMaxSec=infinity
EOF
        publish_config 0644
    done
    # Fresh management units stay inactive until explicit root bootstrap.
    # Registered/stage-only installs preserve their effective units.
    # origin: S9 internal review INFRA-SUITE-SUPPLIES F2 / S9 plan §6.4 / T54
    if [ "$INSTALL_STAGE_ONLY" -eq 0 ] && ! infra_has_authority; then
        for _wif_management in tether-infra-helper.socket tether-infra-helper@.service; do
            config_target "$SYSTEMD_DIR/$_wif_management"
            stage_config
            cat "$INFRA_ETC_DIR/templates/$_wif_management" > "$CONFIG_DEST"
            publish_config 0644
        done
    fi
}
install_broker() {
    [ -n "$DOMAIN" ]     || die "--domain required for --role broker"
    [ -n "$ACME_EMAIL" ] || die "--acme-email required for --role broker"
    detect_os_arch
    # broker is Linux-only: useradd, systemd units, and /etc layout below
    # all assume a Linux host (architecture A.3 / K). docs/broker-ops.md §2.3
    # also positions broker as the public-internet node.
    [ "$OS" = "darwin" ] && die "--role broker is not supported on macOS (only --role ctl is); broker requires a Linux host"
    log "tether install (role=broker, version=$VERSION, os=$OS, arch=$ARCH, domain=$DOMAIN)"
    infra_install_admission
    # Reject invalid offline supply before directory, lock or metadata effects.
    # Staging revalidates the same inputs before publication under the lock.
    # origin: S9 internal review INFRA-SUITE-SUPPLIES F3 / S9 plan §6.4 / T54
    if [ "$SKIP_DOWNLOAD" -eq 1 ] && [ "$DRY_RUN" -eq 0 ]; then validate_offline_broker_binaries; fi
    infra_install_directories
    infra_lock_installation
    DOWNLOAD_BIN_DIR="$BIN_DIR"
    if [ "$INSTALL_STAGE_ONLY" -eq 1 ] && [ "$DRY_RUN" -eq 0 ]; then
        DOWNLOAD_BIN_DIR=$(mktemp -d "$INFRA_DIR/artifacts/.incoming.XXXXXX")
        register_install_temp "$DOWNLOAD_BIN_DIR"
    fi
    # TETHER_INSTALL_ROOT is a TEST SEAM, and it is here because the alternative was
    # shipping the BLOCKER's own role with no coverage at all.
    #
    # origin: prerelease audit round 2, G2 / K-F10 / CC-5. B4 — a bare re-run silently
    # reverting an operator's config — is about the BROKER role: broker.yaml, the
    # Caddyfile, nats.d/nats.conf and the unit files. Every guard written for it
    # exercised `--role agent`, which lands under $HOME, because these five paths are
    # absolute and a test cannot write to them. So reverting the preservation on the two
    # files B4 actually names left the whole suite green.
    #
    # It prefixes the SYSTEM directories only. It does not change what is written, in
    # what order, or under what conditions — a test using it exercises the same code the
    # real install runs. It is announced LOUDLY, because a root-prefixed install that
    # looked like a real one would be far worse than no seam.
    if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        log "  ⚠ TETHER_INSTALL_ROOT=$TETHER_INSTALL_ROOT — system paths are being REDIRECTED under it."
        log "    This is a TEST SEAM. A broker installed this way is NOT installed on this host."
    fi
    # System user — installs.sh does not run it; we just create it.
    #
    # Skipped under the TEST SEAM: creating a system account is a mutation of the HOST,
    # not of the redirected tree, and it is the one step that cannot be made harmless by
    # a path prefix. What the seam exists to exercise — which config files get written,
    # kept, or written as .new — happens entirely below this point and is unaffected.
    if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        log "  + (test seam) skip useradd tether — a redirected install must not touch the host's accounts"
    elif [ "$INSTALL_STAGE_ONLY" -eq 0 ] && [ "$DRY_RUN" -eq 0 ] && ! id -u tether >/dev/null 2>&1; then
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
    if [ "$INSTALL_STAGE_ONLY" -eq 1 ]; then
        log "  + stage-only: runtime directory ownership and modes kept"
    elif [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        # Same reasoning as useradd above: there is no `tether` user to own these under
        # the seam. Create them with the same MODES so anything asserting on permissions
        # still sees the real ones; only the ownership is the caller's. Route this
        # branch through run too: redirecting paths does not authorize dry-run writes.
        # origin: hot-update S9 plan §6.4 / T54 / M27
        run "install -d -m 0750 '$LIB_DIR' '$LOG_DIR'"
        run "install -d -m 0700 '$RUN_DIR'"
        run "install -d -m 0750 '$ETC_DIR/nats.d'"
    else
        run "install -d -o tether -g tether -m 0750 '$LIB_DIR' '$LOG_DIR'"
        run "install -d -o tether -g tether -m 0700 '$RUN_DIR'"
        # G1 #22: the C3 topology reconciler runs User=tether and atomically rewrites its
        # nats.conf (os.CreateTemp + rename), which needs a tether-WRITABLE directory. We keep
        # $ETC_DIR itself root-owned (the root-run caddy.service loads $ETC_DIR/Caddyfile — a
        # tether-owned $ETC_DIR would be a tether->root local privesc) and hand the reconciler
        # ONLY this dedicated subdir. nats-server (User=tether) reads the conf from here too.
        run "install -d -o tether -g tether -m 0750 '$ETC_DIR/nats.d'"
    fi
    if [ "$SKIP_DOWNLOAD" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
        TARBALL=$(mktemp)
        register_install_temp "$TARBALL"
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
        register_install_temp "$SHA_FILE"
        fetch "$SHA_URL" "$SHA_FILE"
        EXPECT=$(grep "$(basename "$TARBALL_URL")" "$SHA_FILE" | awk '{print $1}')
        [ -n "$EXPECT" ] || die "no sha256 in SHA256SUMS"
        verify_sha "$TARBALL" "$EXPECT"
        place_binary "$TARBALL" "$DOWNLOAD_BIN_DIR"
        rm -f "$TARBALL" "$SHA_FILE"
    else
        log "  + (skip) binary install"
    fi
    if [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        install_offline_broker_binaries
    else
        install_nats_server "$BIN_DIR"
        install_caddy "$BIN_DIR"
    fi
    stage_broker_binaries
    write_infra_files
    # broker.yaml — architecture A.3 skeleton.
    if [ "$DRY_RUN" -eq 0 ]; then
        # UNQUOTED heredoc ON PURPOSE: expands $DOMAIN, $ACME_EMAIL, $RUN_DIR, $LIB_DIR and
        # $ETC_DIR — including inside the commented-out `cluster:` block, so an operator who
        # uncomments it gets real paths rather than literal variable names.
        config_dest "$ETC_DIR/broker.yaml"
        cat > "$CONFIG_DEST" <<EOF
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
  sub:
    listen: "127.0.0.1:8090"   # P13 proxy subscription HTTP (Caddy fronts /sub/*); empty disables
  storage:
    db: $LIB_DIR/tether.db
    js_store: $LIB_DIR/jetstream
  observability:
    # h1 F: the broker's own size-capped rotating log sink. The unit sends
    # stdout/stderr to journald (panics + pre-logger boot output only); THIS
    # file is where slog writes, and the cap is enforced in-process so the
    # host needs no logrotate. 50MB x 2 backups = 150MB worst case.
    log_file: $LOG_DIR/broker.log
    log_max_size_mb: 50
    log_max_backups: 2
  # cluster: HA mode (opt-in via 'tether cluster init --from-existing'). Uncomment when joining/forming
  # a cluster. The C3 topology reconciler manages nats.conf in cluster mode; the loopback http:127.0.0.1:8223
  # monitor it probes is established by the one-time 'cluster reconcile nats --manual' cutover (reload
  # cannot hot-add it), so a FRESH single-mode install needs none of this.
  # cluster:
  #   data_dir: $LIB_DIR/raft           # presence of raft/ here = cluster mode
  #   secrets_dir: $ETC_DIR/secrets     # cluster-ca.pem, route-cert/key, broker.nk, account.nk, node-ident.nk
  #   manifest_listen: "127.0.0.1:7480" # C2 well-known cluster discovery manifest (Caddy fronts /.well-known/tether/*)
  #   nats_conf_path: /etc/tether/nats.d/nats.conf   # C3 reconciler target (tether-owned dir, #22; empty opts out)
  #   nats_server_bin: nats-server      # C3 reconciler -t dry-run + --signal reload binary
EOF
        publish_config 644
    else
        log "  + (dry-run) write $ETC_DIR/broker.yaml"
        dryrun_config_policy
    fi

    # Caddyfile (TLS termination + reverse proxy to NATS WebSocket).
    # PIN POLICY: the path-scoped `handle /sub/*` (P13 proxy subscription)
    # MUST come BEFORE the catch-all NATS WebSocket handle, or Clash requests
    # would be reverse-proxied to nats-server and the WSS upgrade would break.
    # Verify after any edit that `wss://$DOMAIN/nats` still upgrades.
    if [ "$DRY_RUN" -eq 0 ]; then
        # UNQUOTED heredoc ON PURPOSE: expands $ACME_EMAIL and $DOMAIN. The { } are Caddyfile
        # block syntax, not shell — they pass through untouched. No Caddy {placeholder} is used
        # here; if one is ever added it needs no escaping either (only $ and ` are shell-active).
        config_dest "$ETC_DIR/Caddyfile"
        cat > "$CONFIG_DEST" <<EOF
{
    email $ACME_EMAIL
}

$DOMAIN:443 {
    handle /sub/* {
        reverse_proxy 127.0.0.1:8090
    }
    handle {
        reverse_proxy 127.0.0.1:8222
    }
}
EOF
        publish_config 644
    else
        log "  + (dry-run) write $ETC_DIR/Caddyfile"
        dryrun_config_policy
    fi

    # systemd units — generated + (by default) ENABLED for boot, NEVER
    # started (K.0 §2 as amended for #76; --no-enable opts out).
    write_systemd_units "$SYSTEMD_DIR" "$BIN_DIR" "$ETC_DIR" "$LIB_DIR" "$LOG_DIR"
    # Reported HERE, before enable_broker_units, because that function has two documented
    # die() paths — and with `set -eu` a die between the config writes and the end of the
    # installer swallowed the whole kept-config report (round 2, K-F8). The operator most
    # needs to know a unit file was kept in exactly the run where enabling then failed.
    report_kept_configs
    enable_broker_units
    write_journald_dropin

    # QUOTED heredocs: these banners are fully literal (no variable, no command to run through
    # the shell), so nothing here should ever be interpreted. Quoting keeps a future edit that
    # adds a $ or a backtick from silently expanding — or executing — under root.
    #
    # THREE branches (review M4): the banner must match what actually happened.
    # Claiming "ENABLED for boot" on a --no-enable run OR a systemd-less host
    # (chroot image build, docker build) where enable was SKIPPED would send
    # the operator to just `start` and reboot-autostart would still be missing
    # — #76 resurrected on the environment-guard path. ENABLED_UNITS is set
    # only when enable actually ran.
    if [ "$INSTALL_STAGE_ONLY" -eq 1 ]; then
        cat <<'EOF'

Broker artifacts and templates staged (files_installed only).
The running stack, active/previous selectors, effective units and enablement
were preserved. Apply staged infrastructure through the authorized product
transaction; a reinstall does not activate candidates.
EOF
    elif [ "$ENABLED_UNITS" -eq 1 ]; then
        cat <<'EOF'

✔ broker files installed.
✔ systemd units created and ENABLED for boot (nats-server, tether-broker, caddy) — NOT started.

To start the broker stack now (install.sh never starts anything):
    sudo systemctl start nats-server tether-broker caddy
EOF
    elif [ "$NO_ENABLE" -eq 1 ]; then
        cat <<'EOF'

✔ broker files installed.
✔ systemd units created (nats-server, tether-broker, caddy) — NOT enabled (--no-enable).

To enable boot-autostart and start the broker stack (install.sh did NOT start anything):
    sudo systemctl daemon-reload
    sudo systemctl enable --now nats-server tether-broker caddy
EOF
    else
        cat <<'EOF'

✔ broker files installed.
✔ systemd units created — but NOT enabled: systemd is not running on this host.

On the real broker host you MUST enable boot-autostart yourself, or one reboot
takes the fleet offline (#76):
    sudo systemctl daemon-reload
    sudo systemctl enable --now nats-server tether-broker caddy
EOF
    fi
    banner_kept_caveat
    log "  + infrastructure state: files_installed; management socket is not activated by installation"
    log "    Enable management explicitly as root: /usr/local/libexec/tether-infra-helper bootstrap"
    if [ "$ARCH" != amd64 ]; then
        log "  + unsupported: infrastructure hot cut requires linux/amd64; cold installation remains available"
    fi
    if [ "${WROTE_JOURNALD_DROPIN:-0}" -eq 1 ]; then
        cat <<'EOF'

A journald size cap was installed (60-tether.conf). It takes effect on the
next reboot, or immediately after:
    sudo systemctl restart systemd-journald
EOF
    fi
    report_kept_configs
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

NATS_SERVER_VERSION="${TETHER_NATS_SERVER_VERSION:-v2.14.6}"
CADDY_VERSION="${TETHER_CADDY_VERSION:-2.7.6}"

install_nats_server() {
    bin="${DOWNLOAD_BIN_DIR:-$1}"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) nats-server install"
        return 0
    fi
    # RETROFIT AN EXISTING HOST, don't skip it.
    #
    # origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1 + L16-F6.
    # This used to return early whenever a nats-server binary existed, on any version.
    # The effect was that no server-side fix could ever reach a machine that had been
    # installed once: the fleet sat on the version of its FIRST install forever, while
    # every test in this repo measured go.mod's much newer embedded server. That gap is
    # not hypothetical — it is recorded as an incident in docs/reviews/INDEX.md
    # (2026-08-06), and it hid a real ACL escape for a whole release.
    #
    # Replacing the binary does NOT restart the service: nats-server keeps running from
    # the inode it opened, so the new version takes effect at the operator's next
    # restart. Saying so is the point — a silent swap would leave `nats-server --version`
    # and the running process disagreeing with no explanation.
    # Existing binaries are routed to immutable staging by admission. Do not
    # execute even --version here: the never-start inventory includes every
    # infrastructure executable, including short-lived version probes.
    # origin: hot-update S9 plan §6.4 / T54 / M27
    [ ! -e "$bin/nats-server" ] || [ "$FORCE_CONFIG" -eq 1 ] || die "existing nats-server entry requires stage-only installation"
    case "$ARCH" in amd64) na="amd64" ;; arm64) na="arm64" ;; *) die "nats arch $ARCH" ;; esac
    case "$OS"   in linux) no="linux" ;;  darwin) no="darwin" ;;  *) die "nats os $OS" ;; esac
    base="https://github.com/nats-io/nats-server/releases/download/${NATS_SERVER_VERSION}"
    name="nats-server-${NATS_SERVER_VERSION}-${no}-${na}"
    url="${base}/${name}.tar.gz"
    sums="${base}/SHA256SUMS"
    log "  + downloading $url"
    tmp=$(mktemp -d)
    register_install_temp "$tmp"
    curl -fsSL --retry 3 "$url"  -o "$tmp/nats.tgz"  || die "fetch nats-server failed: $url"
    curl -fsSL --retry 3 "$sums" -o "$tmp/sums.txt" || die "fetch nats-server SHA256SUMS failed: $sums"
    expect=$(grep "${name}.tar.gz$" "$tmp/sums.txt" | awk '{print $1}')
    [ -n "$expect" ] || die "no sha256 for ${name}.tar.gz in NATS SHA256SUMS"
    verify_sha "$tmp/nats.tgz" "$expect"
    tar -xzf "$tmp/nats.tgz" -C "$tmp"
    # install(1) truncates in place, which fails with ETXTBSY against a RUNNING binary.
    # Stage beside the target and rename: rename is atomic and legal while the old inode
    # is still executing, which is exactly the retrofit case above.
    install -m 0755 "$tmp/${name}/nats-server" "$bin/.nats-server.new" \
      || die "stage nats-server into $bin failed"
    mv -f "$bin/.nats-server.new" "$bin/nats-server" || die "install nats-server into $bin failed"
    record_owned_file "$bin/nats-server"
    rm -rf "$tmp"
    log "  ✔ installed candidate nats-server bytes (${NATS_SERVER_VERSION}); no runtime change"
}

install_caddy() {
    bin="${DOWNLOAD_BIN_DIR:-$1}"
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        log "  + (skip) caddy install"
        return 0
    fi
    if [ -x "$bin/caddy" ] && [ "$FORCE_CONFIG" -eq 0 ]; then
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
    tmp=$(mktemp -d)
    register_install_temp "$tmp"
    curl -fsSL --retry 3 "$url"  -o "$tmp/caddy.tgz" || die "fetch caddy failed: $url"
    curl -fsSL --retry 3 "$sums" -o "$tmp/sums.txt"  || die "fetch caddy checksums failed: $sums"
    expect=$(grep "${name}.tar.gz$" "$tmp/sums.txt" | awk '{print $1}')
    [ -n "$expect" ] || die "no sha256 for ${name}.tar.gz in Caddy checksums"
    verify_sha "$tmp/caddy.tgz" "$expect"
    tar -xzf "$tmp/caddy.tgz" -C "$tmp"
    # A failed copy must not truncate the canonical inode. Keep the verified old
    # inode until an explicit later rollback/retirement decision; installation
    # never runs a candidate to manufacture startup qualification.
    # origin: S9 internal review rv-priv F9
    infra_safe_path "$bin/caddy"
    _ic_new=$(mktemp "$bin/.caddy.new.XXXXXX")
    register_install_temp "$_ic_new"
    install -m 0755 "$tmp/caddy" "$_ic_new" || die "stage caddy failed; canonical entry retained"
    [ "$(file_sha256 "$_ic_new")" = "$(file_sha256 "$tmp/caddy")" ] || die "caddy changed during staging"
    sync "$_ic_new" || die "sync caddy candidate failed; canonical entry retained"
    _ic_previous=
    if [ -e "$bin/caddy" ]; then
        [ -f "$bin/caddy" ] && [ ! -L "$bin/caddy" ] || die "caddy entry must be regular"
        _ic_hash=$(file_sha256 "$bin/caddy")
        _ic_previous="$bin/.caddy.previous.$_ic_hash.$(stat -c '%d.%i' "$bin/caddy")"
        if [ -e "$_ic_previous" ] || [ -L "$_ic_previous" ]; then
            [ -f "$_ic_previous" ] && [ ! -L "$_ic_previous" ] && [ "$(file_sha256 "$_ic_previous")" = "$_ic_hash" ] || die "caddy rollback copy changed"
        else
            ln "$bin/caddy" "$_ic_previous" || die "cannot preserve caddy rollback inode"
        fi
        sync "$_ic_previous" "$bin" || die "sync caddy rollback failed; canonical entry retained"
    fi
    mv -Tf "$_ic_new" "$bin/caddy" || die "publish caddy failed; canonical entry retained"
    if ! sync "$bin"; then
        if [ -n "$_ic_previous" ]; then
            _ic_restore=$(mktemp "$bin/.caddy.restore.XXXXXX")
            register_install_temp "$_ic_restore"
            rm -f "$_ic_restore"
            ln "$_ic_previous" "$_ic_restore" && mv -Tf "$_ic_restore" "$bin/caddy" || die "caddy rollback failed; previous inode retained"
            sync "$bin" || die "caddy rollback directory sync failed; old inode restored"
        else
            rm -f "$bin/caddy"
            sync "$bin" || die "caddy first publication rollback sync failed"
        fi
        die "caddy publication directory sync failed; canonical entry restored"
    fi
    [ -z "$_ic_previous" ] || log "  + caddy rollback inode retained: $_ic_previous"
    record_owned_file "$bin/caddy"
    rm -rf "$tmp"
    log "  ✔ installed $bin/caddy (${CADDY_VERSION})"
}

write_systemd_units() {
    sysd="$1"; bin="$2"; etc="$3"; lib="$4"; log_dir="$5"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) write $sysd/{nats-server,tether-broker,caddy}.service + $etc/nats.d/nats.conf"
        dryrun_config_policy
        return 0
    fi

    # A legacy or partial installation can require stage-only while nats.d is
    # absent. Atomic publication still needs a real destination for nats.conf.new;
    # create only the missing directory and preserve every existing mode/owner.
    # Keep this after the preview return so dry-run cannot create even that parent.
    # origin: hot-update S9 plan §6.4 / S9 internal review FIX-INST F4
    infra_safe_path "$etc/nats.d"
    if [ ! -d "$etc/nats.d" ]; then
        if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
            install -d -m 0750 "$etc/nats.d"
        else
            install -d -o tether -g tether -m 0750 "$etc/nats.d"
        fi
    fi

    # nats-server WebSocket can ONLY be configured via a conf file —
    # there is no `--ws_listen` CLI flag (a previous version of this
    # script tried that; nats-server prints help and exits, systemd
    # marks the unit failed). Drop a minimal config that mirrors the
    # P11 architecture A.3 split-port plan: plain NATS on 4222 for
    # the broker process, internal WS on 8222 for Caddy to terminate
    # TLS in front of.
    # UNQUOTED heredoc ON PURPOSE: expands $lib into jetstream store_dir. Nothing else expands.
    config_dest "$etc/nats.d/nats.conf"
    cat > "$CONFIG_DEST" <<EOF
# tether nats-server config (generated by install.sh)
host: "127.0.0.1"
port: 4222

# Client liveness (server half). The agent pings every 20s and gives up after 2 unanswered
# (internal/agent AgentPingInterval / AgentMaxPingsOut); the server uses the same numbers so a
# silently dead client is dropped in about a minute (measured 58 s on the deploy tier) instead of
# nats-server's default several minutes. Values come from NATS_PING_INTERVAL / NATS_PING_MAX at the
# top of install.sh (one definition for this template and the KEPT merge hint).
# ping_interval MUST stay a quoted duration string: a bare 20 is deprecated by nats-server and
# fails 'nats-server -t'; an unquoted 2m is read as 2 000 000 seconds. tether's config takeover
# preserves both keys verbatim and refuses the wrong shapes (internal/natsconf).
ping_interval: "$NATS_PING_INTERVAL"
ping_max: $NATS_PING_MAX

jetstream {
  store_dir: "$lib/jetstream"
}

websocket {
  host: "127.0.0.1"
  port: 8222
  no_tls: true
}
EOF
    publish_config 644

    # UNQUOTED heredoc ON PURPOSE: ExecStart needs $bin and $etc. Because it is unquoted, the
    # comment prose below is shell-active too — P9: the backticks that once wrapped 'cluster add'
    # were run as a command substitution BY ROOT, printing "cluster: not found" and silently
    # deleting those two words from the installed unit. Prose in this body uses single quotes.
    config_dest "$sysd/nats-server.service"
    cat > "$CONFIG_DEST" <<EOF
[Unit]
Description=NATS message broker (tether)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tether
ExecStart=$bin/nats-server -c $etc/nats.d/nats.conf
# G4 §B / #23: Restart=always (not on-failure). The 'cluster add' grow cutover restarts nats-server with a
# same-uid SIGKILL (nats-server --signal stop) — the one lifecycle restart tether owns, since it never
# orchestrates systemctl and the reconciler is SIGHUP-only. Restart=always makes that revival deterministic
# and completes the #23 clean-exit hardening the broker unit already uses. The default StartLimit still trips
# a genuine crash-loop; a single deliberate SIGKILL is well under StartLimitBurst.
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
    publish_config 644

    write_broker_systemd_unit "$bin" "$etc" "$sysd" "$log_dir"

    # UNQUOTED heredoc ON PURPOSE: expands $bin and $etc into ExecStart.
    config_dest "$sysd/caddy.service"
    cat > "$CONFIG_DEST" <<EOF
[Unit]
Description=Caddy (TLS termination for tether NATS WSS)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$bin/caddy run --config $etc/Caddyfile --adapter caddyfile
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
    publish_config 644
}

# Kept separate so hot-update retrofit never generates NATS/Caddy templates.
# origin: hot-update S8 plan §5.2 / P20
write_broker_systemd_unit() {
    bin="$1"; etc="$2"; sysd="$3"; log_dir="$4"
    # Normal installation and S8 retrofit share this writer. Keep its preview
    # guard local so either caller can never create, chmod or receipt a unit.
    # origin: hot-update S8 plan §5.2 / P20; hot-update S9 plan §6.4 / T54 / M27
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) write $sysd/tether-broker.service (canonical ExecStart=$bin/tether serve --config $etc/broker.yaml)"
        return 0
    fi
    # UNQUOTED heredoc ON PURPOSE: expands $bin, $etc and $log_dir. The G1 #23 prose below is
    # shell-active as well (see P9 on the unit above) — keep it free of backticks and $(…).
    config_dest "$sysd/tether-broker.service"
    cat > "$CONFIG_DEST" <<EOF
[Unit]
Description=tether broker daemon
After=nats-server.service
Wants=nats-server.service

[Service]
Type=simple
User=tether
# admin.sock lives under /run/tether, which is tmpfs and wiped on every reboot.
# RuntimeDirectory makes systemd recreate it (owner=User, mode below) before each
# start, so the broker can bind its admin socket after a host reboot. Without it
# the unit fails on boot with "mkdir /var/run/tether: permission denied" (the
# tether user can't mkdir under /run) and Restart=on-failure just loops.
RuntimeDirectory=tether
RuntimeDirectoryMode=0700
# The canonical S8 entry resolves this role/config/UID effective selector before
# daemon initialization. Keep this path canonical across rollback and cold boot.
ExecStart=$bin/tether serve --config $etc/broker.yaml
# G1 #23: the broker clean-exits(0) on some nats-loss paths (serve.go maps a context.Canceled
# Run() return to exit 0). Restart=on-failure would NOT revive a clean exit, stranding the
# broker (inactive, not failed) after a nats blip. Restart=always revives any self-exit
# (including clean 0) but does NOT revive an operator systemctl stop/restart (systemd knows it
# initiated the stop), so operator stop/restart/upgrade is unaffected -- only an unexpected
# self-exit is revived. The default StartLimit still trips on a genuine crash-loop; we
# deliberately do NOT set StartLimitIntervalSec=0 (that would mask a wedged broker).
Restart=always
RestartSec=2
# h1 F: slog output goes to a PROCESS-OWNED, size-capped rotating file
# ($log_dir/broker.log, configured under observability: in broker.yaml), NOT
# to systemd's unbounded append: sink. The 2026-08-04 incident wrote 5.3GB of
# broker.err onto a 19GB disk exactly because append: has no cap and the host
# had no logrotate. stdout/stderr stay on journald, which IS capped, and carry
# only panics/stacktraces and pre-logger boot output.
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
    publish_config 644
}

# Retirement requires an absent root/S8 authority and an independently empty
# process set. Shell code cannot interpret an unknown journal as idle or consume
# a signed retirement CAS; an enabled host must retire through the root product.
# origin: hot-update S9 plan §6.4 / T54 / M27
uninstall_broker() {
    # The SAME path resolution install_broker uses, including TETHER_INSTALL_ROOT.
    # origin: prerelease audit round 2, G2 / K-F10 / CC-5 / external review M-4.
    # Prefixing files without prefixing service-manager effects was a live-host
    # mutation; keep the symmetric host_systemd_is_the_target guard below.
    broker_paths
    log "tether uninstall (role=broker)"
    for _ub_path in "$BIN_DIR" "$ETC_DIR" "$SYSTEMD_DIR" "$LIBEXEC_DIR" "$INFRA_DIR"; do
        infra_safe_path "$_ub_path"
    done
    infra_has_authority && die "infra_busy: product retirement CAS is required before uninstall"
    if [ "$DRY_RUN" -eq 0 ]; then
        # Keep both permanent lock inodes. In particular, never rm -rf the
        # business directory: it contains SQLite, Raft and irreplaceable keys.
        # origin: prerelease audit round 2, K-F13 / hot-update S9 §6.4.
        infra_install_directories
        infra_lock_installation
        infra_has_authority && die "infra_busy: authority appeared before uninstall"
        infra_business_running && die "infra_busy: installed executable is still running"
    fi
    _ub_units=""
    for _ub_unit in nats-server tether-broker caddy; do
        if [ "$DRY_RUN" -eq 1 ] || owned_file "$SYSTEMD_DIR/$_ub_unit.service"; then
            _ub_units="$_ub_units $_ub_unit"
            # #76 symmetry: disable before removing marked units; no --now.
            # Mi8: on a systemd-less host, remove only our exact boot links.
            _ub_link="$SYSTEMD_DIR/multi-user.target.wants/$_ub_unit.service"
            if [ "$DRY_RUN" -eq 1 ]; then
                log "  + (dry-run) remove $_ub_link (only if it points to the owned unit)"
            elif [ -L "$_ub_link" ]; then
                case "$(readlink "$_ub_link")" in
                    "../$_ub_unit.service"|"$SYSTEMD_DIR/$_ub_unit.service") rm -f "$_ub_link" ;;
                    *) log "  + kept operator boot link: $_ub_link" ;;
                esac
            fi
        fi
    done
    if [ -n "$_ub_units" ]; then
        if ! host_systemd_is_the_target; then
            log_would_run_systemctl "systemctl disable$_ub_units"
        elif [ "$DRY_RUN" -eq 1 ] || { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }; then
            run "systemctl disable$_ub_units" || die "cannot disable owned units; files retained"
        fi
    fi
    for _ub_unit in nats-server tether-broker caddy; do
        remove_owned_file "$SYSTEMD_DIR/$_ub_unit.service"
        remove_owned_file "$SYSTEMD_DIR/$_ub_unit.service.new"
    done
    for _ub_name in tether nats-server caddy; do remove_owned_file "$BIN_DIR/$_ub_name"; done
    for _ub_name in tether-infra-helper tether-infra-launch; do remove_owned_file "$LIBEXEC_DIR/$_ub_name"; done
    for _ub_target in "$ETC_DIR/broker.yaml" "$ETC_DIR/Caddyfile" "$ETC_DIR/nats.d/nats.conf" \
        "$INFRA_ETC_DIR/templates/tether-infra-helper.socket" "$INFRA_ETC_DIR/templates/tether-infra-helper@.service" \
        "$INFRA_ETC_DIR/templates/nats-server.service.d/90-tether-infra.conf" "$INFRA_ETC_DIR/templates/caddy.service.d/90-tether-infra.conf"; do
        remove_owned_file "$_ub_target"
        remove_owned_file "$_ub_target.new"
    done
    # #77 / review F3: a same-name operator journald drop-in is not ours.
    _u_dropin="${TETHER_JOURNALD_ROOT:-${TETHER_INSTALL_ROOT:-}/etc/systemd}/journald.conf.d/60-tether.conf"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) remove $_u_dropin (only if it carries the tether ownership marker)"
    elif [ -f "$_u_dropin" ] && [ ! -L "$_u_dropin" ] &&
         [ "$(head -n1 "$_u_dropin")" = '# managed-by: tether-install.sh (#77 journald cap)' ]; then
        rm -f "$_u_dropin"
    elif [ -e "$_u_dropin" ] || [ -L "$_u_dropin" ]; then
        log "  ! $_u_dropin is NOT tether-owned; left in place"
    fi
    if [ "$DRY_RUN" -eq 0 ]; then
        for _ub_slot in "$INFRA_DIR/artifacts/"*; do
            [ -d "$_ub_slot" ] && [ ! -L "$_ub_slot" ] || continue
            _ub_owned=1
            _ub_nonempty=0
            for _ub_file in "$_ub_slot/"* "$_ub_slot/".[!.]* "$_ub_slot/"..?*; do
                [ -e "$_ub_file" ] || [ -L "$_ub_file" ] || continue
                _ub_nonempty=1
                owned_file "$_ub_file" || _ub_owned=0
            done
            [ "$_ub_owned:$_ub_nonempty" = 1:1 ] || continue
            chmod 0700 "$_ub_slot"
            for _ub_file in "$_ub_slot/"*; do remove_owned_file "$_ub_file"; done
            rmdir "$_ub_slot"
        done
    fi
    if [ -n "$_ub_units" ]; then
        if ! host_systemd_is_the_target; then
            log_would_run_systemctl "systemctl daemon-reload"
        elif [ "$DRY_RUN" -eq 1 ] || { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }; then
            run "systemctl daemon-reload" || die "owned files removed but service manager reload failed"
        fi
    fi
    log "  + matching installer-owned files removed; operator files and business data retained"
    log "  + permanent journal/store/domain locks and the tether system user retained"
}

# Render the complete operator-supplied declaration, never a guessed singleton
# roster. The capable canonical entry derives and publishes the .domain record
# on its first explicit cold start; K.0 forbids invoking tether in this script.
# origin: S8 internal review remaining-orchestration F5 (R5) / S8 plan §5.1 / LQ1
hot_update_registration() {
    _hur_control=$1
    _hur_uid=$2
    _hur_gid=$3
    if [ -z "$HOT_UPDATE_REGISTRATION" ]; then
        log "  ! registration pending: supply --hot-update-registration FILE with every installation participant"
        return 0
    fi
    [ -f "$HOT_UPDATE_REGISTRATION" ] && [ ! -L "$HOT_UPDATE_REGISTRATION" ] || die "hot-update registration must be a regular declaration file"
    [ ! -L "$_hur_control/artifacts" ] && [ ! -L "$_hur_control/installation.json" ] || die "hot-update registration path must not be a symlink"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) render $HOT_UPDATE_REGISTRATION as $_hur_control/installation.json; create $_hur_control/artifacts"
        return 0
    fi
    install -d -o "$_hur_uid" -g "$_hur_gid" -m 0700 "$_hur_control/artifacts"
    if [ -e "$_hur_control/installation.json" ]; then
        cmp -s "$HOT_UPDATE_REGISTRATION" "$_hur_control/installation.json" || die "existing hot-update declaration differs; explicit deployment review required"
        return 0
    fi
    _hur_temp=$(mktemp "$_hur_control/.installation.XXXXXX")
    register_install_temp "$_hur_temp"
    cat "$HOT_UPDATE_REGISTRATION" > "$_hur_temp"
    chmod 0600 "$_hur_temp"
    chown "$_hur_uid:$_hur_gid" "$_hur_temp"
    sync "$_hur_temp"
    # O_EXCL publication: another retrofit may have won; never replace its
    # declaration or a pending daemon's registration under the same pathname.
    ln "$_hur_temp" "$_hur_control/installation.json" || die "hot-update declaration publication raced another writer"
    rm -f "$_hur_temp"
    sync "$_hur_control"
}

# Retrofit prepares installation-owned paths only. The daemon remains the sole
# journal writer; root publication belongs to the explicit upgrade stage command.
# Put artifacts outside /var/lib/tether: that business directory is daemon-owned
# and must not let an unprivileged sibling replace root-published executables.
# origin: hot-update S8 plan §5.1 / §5.2 / LQ1 / P20
hot_update_retrofit() {
    [ "$UNINSTALL" -eq 0 ] || die "--hot-update-retrofit excludes --uninstall"
    [ -z "$LOCAL_BIN" ] || die "--hot-update-retrofit excludes --bin; install a capable canonical entry explicitly"
    [ "$FORCE_CONFIG" -eq 0 ] || die "--hot-update-retrofit excludes --force-config; review startup changes before applying"
    case "$ROLE" in broker|agent) ;; *) die "--hot-update-retrofit requires --role broker or agent" ;; esac
    detect_os_arch
    [ "$OS" = linux ] || die "--hot-update-retrofit requires a Linux host"
    log "tether hot-update retrofit (role=$ROLE); no binary replacement or service start"
    if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        log "  + TETHER_INSTALL_ROOT=$TETHER_INSTALL_ROOT: system paths redirected; host service manager NOT touched"
    elif [ "$DRY_RUN" -eq 0 ] && [ "$(id -u)" -ne 0 ]; then
        die "--hot-update-retrofit requires root for installation-owned artifacts"
    fi
    if [ "$ROLE" = broker ]; then
        broker_paths
        for _hu_path in "$LIBEXEC_DIR" "$INFRA_DIR" "$INFRA_ETC_DIR" "$SYSTEMD_DIR" "$INFRA_RUN_DIR"; do
            infra_safe_path "$_hu_path"
        done
    fi
    if [ "$ROLE" = agent ]; then
        case "$SESSION" in ''|.|..|*[!A-Za-z0-9_-]*) die "--session must be a safe nonempty session ID for agent retrofit" ;; esac
        _hu_agent="$HOME/.tether/agents/$SESSION"
        for _hu_path in "$HOME/.tether" "$HOME/.tether/agents" "$_hu_agent" "$_hu_agent/upgrades"; do
            [ ! -L "$_hu_path" ] || die "agent upgrade directory must not be a symlink"
        done
    fi
    _hu_base="${TETHER_INSTALL_ROOT:-}/var/lib/tether-upgrade"
    # Reject aliases before chmod/chown; otherwise a root retrofit could change
    # the permissions of an unrelated live directory through a symlink.
    for _hu_path in "$_hu_base" "$_hu_base/artifacts" "$_hu_base/control"; do
        [ ! -L "$_hu_path" ] || die "hot-update directory must not be a symlink: $_hu_path"
    done
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) install -d -o root -g root -m 0755 $_hu_base $_hu_base/artifacts $_hu_base/control"
    elif [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        install -d -m 0755 "$_hu_base" "$_hu_base/artifacts" "$_hu_base/control"
    else
        install -d -o root -g root -m 0755 "$_hu_base" "$_hu_base/artifacts" "$_hu_base/control"
    fi
    if [ "$ROLE" = broker ]; then
        broker_paths
        # Preparation never installs live NATS/Caddy drop-ins or touches their
        # UID/storage. Root bootstrap owns their fence and effective-unit CAS.
        # origin: hot-update S9 plan §5.3 / §6.4 / T54
        infra_install_directories
        write_infra_files
        # Existing config/data ownership is not retroactively changed. Only the
        # dedicated control directory is handed to the existing service identity.
        [ ! -L "$LIB_DIR/upgrade-control" ] || die "broker upgrade-control must not be a symlink"
        if [ "$DRY_RUN" -eq 1 ]; then
            log "  + (dry-run) install -d -o tether -g tether -m 0700 $LIB_DIR/upgrade-control"
        else
            if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
                install -d -m 0700 "$LIB_DIR/upgrade-control"
            else
                id -u tether >/dev/null 2>&1 || die "existing broker service user tether is required"
                install -d -o tether -g tether -m 0700 "$LIB_DIR/upgrade-control"
            fi
        fi
        run "mkdir -p '$SYSTEMD_DIR'"
        write_broker_systemd_unit "$BIN_DIR" "$ETC_DIR" "$SYSTEMD_DIR" "${TETHER_INSTALL_ROOT:-}/var/log/tether"
    else
        if [ "$DRY_RUN" -eq 1 ]; then
            log "  + (dry-run) install -d -m 0700 $_hu_agent/upgrades (owned by agent HOME owner)"
        else
            # Root is acting on behalf of the HOME owner, never turning the
            # non-root agent's future journal directory into a root-only path.
            _hu_uid=$(ls -nd "$HOME" | awk '{print $3}')
            _hu_gid=$(ls -nd "$HOME" | awk '{print $4}')
            install -d -o "$_hu_uid" -g "$_hu_gid" -m 0700 "$HOME/.tether" "$HOME/.tether/agents" "$_hu_agent" "$_hu_agent/upgrades"
        fi
    fi
    if [ "$ROLE" = broker ]; then
        if [ -n "${TETHER_INSTALL_ROOT:-}" ] || [ "$DRY_RUN" -eq 1 ]; then
            _hu_uid=$(id -u)
            _hu_gid=$(id -g)
        else
            _hu_uid=$(id -u tether)
            _hu_gid=$(id -g tether)
        fi
        hot_update_registration "$LIB_DIR/upgrade-control" "$_hu_uid" "$_hu_gid"
    else
        _hu_uid=$(ls -nd "$HOME" | awk '{print $3}')
        _hu_gid=$(ls -nd "$HOME" | awk '{print $4}')
        hot_update_registration "$_hu_agent/upgrades" "$_hu_uid" "$_hu_gid"
    fi
    report_kept_configs
    cat <<'EOF'

Hot-update directories and canonical startup templates are prepared.
Review any .new template before applying it. The canonical entry must already
be S8-capable; first installation of that entry is an explicit non-hot operation.
The daemon signs the grant and remains the only upgrade journal writer.

Publish both authorized images explicitly as root:
    tether upgrade stage --grant grant.json --source source-image --candidate candidate-image --receipt-out receipt.json
Then the original actor starts the same transaction using its artifact receipt:
    node upgrade NID --tx-id TXID --artifact-receipt receipt.json
    broker upgrade reload --tx-id TXID --artifact-receipt receipt.json
EOF
}

# -- dispatch --------------------------------------------------------------

# Resolve VERSION once after argparse so every role's install path
# sees the same value (no per-role drift if the redirect flaps).
[ -z "$HOT_UPDATE_REGISTRATION" ] || [ "$HOT_UPDATE_RETROFIT" -eq 1 ] || die "--hot-update-registration requires --hot-update-retrofit"
if [ "$HOT_UPDATE_RETROFIT" -eq 1 ]; then
    hot_update_retrofit
    exit 0
fi
[ -z "$LOCAL_BIN" ] || [ "$UNINSTALL" -eq 0 ] || die "--bin excludes --uninstall"
[ -z "$LOCAL_BIN" ] || [ -f "$LOCAL_BIN" ] || die "--bin must name a regular local file"
maybe_resolve_version

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
