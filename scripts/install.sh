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
#   --dry-run           do everything except download + chmod + write
#                       outside the work-root; useful for CI assertions.
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

# KEPT_CONFIGS accumulates every file this run declined to overwrite, so the closing
# banner can list them in one place instead of leaving the operator to notice a `.new`
# file some other day.
KEPT_CONFIGS=""

# KEPT_ANY survives report_kept_configs' clear, because the ✔ banners are printed
# AFTER that report and still have to know not to claim a file was written.
# origin: prerelease audit round 2, C7.
KEPT_ANY=0

# config_dest decides where a generated config should actually be written and puts the
# answer in CONFIG_DEST: the real path when the file does not exist yet or
# --force-config was given, `<path>.new` otherwise. A kept file is recorded for the
# banner.
#
# IT SETS A VARIABLE RATHER THAN ECHOING, deliberately. `$(config_dest …)` would run it
# in a SUBSHELL, so every KEPT_CONFIGS append would be discarded the moment the
# substitution finished and the banner would report nothing — while the file-keeping
# itself still worked, which is the worst combination: the protection happens and the
# operator is never told.
#
# origin: prerelease audit deploy-release-docs/DRD-F1 + DRD-F5. See FORCE_CONFIG above
# for why "keep it AND write the new one beside it" rather than either extreme.
config_dest() {
    _cd_path="$1"
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
            rm -f "${_cd_path}.new"
            log "  - removed the superseded ${_cd_path}.new (its content is now the live file)"
        fi
        return 0
    fi
    KEPT_CONFIGS="${KEPT_CONFIGS}${_cd_path}
"
    KEPT_ANY=1
    CONFIG_DEST="${_cd_path}.new"
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
    done
    log "    A bare re-run used to overwrite these, which silently reverted local edits —"
    log "    including the broker.cluster block that makes this node a cluster member."
    log "    Re-run with --force-config to take the new content as-is."
    log "    Nothing removes a .new file: delete the ones you have decided against, or the"
    log "    next run's report is the only thing telling them apart from the current config."
    KEPT_CONFIGS=""
}
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
    if [ "$DRY_RUN" -eq 1 ] || [ "$SKIP_DOWNLOAD" -eq 1 ]; then
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
        TARBALL_URL=$(source_tarball_url)
        SHA_URL=$(source_sha_url)
        fetch "$TARBALL_URL" "$TARBALL"
        SHA_FILE=$(mktemp)
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

install_broker() {
    [ -n "$DOMAIN" ]     || die "--domain required for --role broker"
    [ -n "$ACME_EMAIL" ] || die "--acme-email required for --role broker"

    detect_os_arch
    # broker is Linux-only: useradd, systemd units, and /etc layout below
    # all assume a Linux host (architecture A.3 / K). docs/broker-ops.md §2.3
    # also positions broker as the public-internet node.
    [ "$OS" = "darwin" ] && die "--role broker is not supported on macOS (only --role ctl is); broker requires a Linux host"
    log "tether install (role=broker, version=$VERSION, os=$OS, arch=$ARCH, domain=$DOMAIN)"

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
    BIN_DIR="${PREFIX:-${TETHER_INSTALL_ROOT:-}/usr/local/bin}"
    ETC_DIR="${TETHER_INSTALL_ROOT:-}/etc/tether"
    LIB_DIR="${TETHER_INSTALL_ROOT:-}/var/lib/tether"
    LOG_DIR="${TETHER_INSTALL_ROOT:-}/var/log/tether"
    RUN_DIR="${TETHER_INSTALL_ROOT:-}/var/run/tether"
    SYSTEMD_DIR="${TETHER_INSTALL_ROOT:-}/etc/systemd/system"

    # System user — installs.sh does not run it; we just create it.
    #
    # Skipped under the TEST SEAM: creating a system account is a mutation of the HOST,
    # not of the redirected tree, and it is the one step that cannot be made harmless by
    # a path prefix. What the seam exists to exercise — which config files get written,
    # kept, or written as .new — happens entirely below this point and is unaffected.
    if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        log "  + (test seam) skip useradd tether — a redirected install must not touch the host's accounts"
    elif [ "$DRY_RUN" -eq 0 ] && ! id -u tether >/dev/null 2>&1; then
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
    if [ -n "${TETHER_INSTALL_ROOT:-}" ]; then
        # Same reasoning as useradd above: there is no `tether` user to own these under
        # the seam. Create them with the same MODES so anything asserting on permissions
        # still sees the real ones; only the ownership is the caller's.
        install -d -m 0750 "$LIB_DIR" "$LOG_DIR"
        install -d -m 0700 "$RUN_DIR"
        install -d -m 0750 "$ETC_DIR/nats.d"
    elif [ "$DRY_RUN" -eq 0 ]; then
        install -d -o tether -g tether -m 0750 "$LIB_DIR" "$LOG_DIR"
        install -d -o tether -g tether -m 0700 "$RUN_DIR"
        # G1 #22: the C3 topology reconciler runs User=tether and atomically rewrites its
        # nats.conf (os.CreateTemp + rename), which needs a tether-WRITABLE directory. We keep
        # $ETC_DIR itself root-owned (the root-run caddy.service loads $ETC_DIR/Caddyfile — a
        # tether-owned $ETC_DIR would be a tether->root local privesc) and hand the reconciler
        # ONLY this dedicated subdir. nats-server (User=tether) reads the conf from here too.
        install -d -o tether -g tether -m 0750 "$ETC_DIR/nats.d"
    else
        log "  + (dry-run) install -d -o tether -g tether -m 0750 $LIB_DIR $LOG_DIR"
        log "  + (dry-run) install -d -o tether -g tether -m 0700 $RUN_DIR"
        log "  + (dry-run) install -d -o tether -g tether -m 0750 $ETC_DIR/nats.d  (#22 reconciler-writable nats.conf dir)"
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
        chmod 644 "$CONFIG_DEST"
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
        chmod 644 "$CONFIG_DEST"
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
    if [ "$ENABLED_UNITS" -eq 1 ]; then
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
    bin="$1"
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
    if [ -x "$bin/nats-server" ]; then
        have="$("$bin/nats-server" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
        want="${NATS_SERVER_VERSION#v}"
        if [ "$have" = "$want" ]; then
            log "  + nats-server already at ${NATS_SERVER_VERSION} (skip)"
            return 0
        fi
        log "  ! nats-server at $bin/nats-server is ${have:-unknown}, this release pins ${NATS_SERVER_VERSION}"
        log "    replacing it; RESTART nats-server afterwards for the new binary to take effect"
        NATS_RETROFIT_FROM="${have:-unknown}"
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
    # install(1) truncates in place, which fails with ETXTBSY against a RUNNING binary.
    # Stage beside the target and rename: rename is atomic and legal while the old inode
    # is still executing, which is exactly the retrofit case above.
    install -m 0755 "$tmp/${name}/nats-server" "$bin/.nats-server.new" \
      || die "stage nats-server into $bin failed"
    mv -f "$bin/.nats-server.new" "$bin/nats-server" || die "install nats-server into $bin failed"
    rm -rf "$tmp"; trap - EXIT
    if [ -n "${NATS_RETROFIT_FROM:-}" ]; then
        log "  ✔ replaced $bin/nats-server (${NATS_RETROFIT_FROM} -> ${NATS_SERVER_VERSION})"
        log "    ACTION REQUIRED: sudo systemctl restart nats-server"
    else
        log "  ✔ installed $bin/nats-server (${NATS_SERVER_VERSION})"
    fi
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
        log "  + (dry-run) write $sysd/{nats-server,tether-broker,caddy}.service + $etc/nats.d/nats.conf"
        dryrun_config_policy
        return 0
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

jetstream {
  store_dir: "$lib/jetstream"
}

websocket {
  host: "127.0.0.1"
  port: 8222
  no_tls: true
}
EOF
    chmod 644 "$CONFIG_DEST"

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
    chmod 644 "$CONFIG_DEST"

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
    chmod 644 "$CONFIG_DEST"

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
    chmod 644 "$CONFIG_DEST"
}

uninstall_broker() {
    # The SAME path resolution install_broker uses, including the TETHER_INSTALL_ROOT
    # test seam — an uninstall that removed real system files while the matching install
    # wrote into a redirected tree would be the worst possible asymmetry.
    BIN_DIR="${PREFIX:-${TETHER_INSTALL_ROOT:-}/usr/local/bin}"
    ETC_DIR="${TETHER_INSTALL_ROOT:-}/etc/tether"
    LIB_DIR="${TETHER_INSTALL_ROOT:-}/var/lib/tether"
    LOG_DIR="${TETHER_INSTALL_ROOT:-}/var/log/tether"
    RUN_DIR="${TETHER_INSTALL_ROOT:-}/var/run/tether"
    SYSTEMD_DIR="${TETHER_INSTALL_ROOT:-}/etc/systemd/system"
    log "tether uninstall (role=broker)"
    # #76 symmetry: disable BEFORE removing the unit files, so no dangling
    # wants/ symlinks survive (this also cleans up after a manual
    # `systemctl enable` on a pre-#76 install). review Mi7: the systemd probe
    # is skipped in dry-run so the preview is host-INDEPENDENT and assertable
    # (mirrors enable_broker_units' own principle). review Mi8: on a
    # systemd-less host the disable is skipped, so explicitly rm the boot
    # symlinks too or they dangle after the unit files go, and the closing
    # log must not claim "disabled".
    # external review M-4: the seam guard is checked FIRST and symmetrically with the
    # install half. A redirected uninstall that ran `systemctl disable` would stop the
    # HOST's real nats-server / tether-broker / caddy from starting at boot — the worst
    # possible outcome for a flag documented as a test seam. The wants/ symlinks under
    # $SYSTEMD_DIR are inside the root, so the hand-removal branch below is the correct
    # (and sufficient) cleanup there.
    _DISABLED=0
    if ! host_systemd_is_the_target; then
        log_would_run_systemctl "systemctl disable nats-server tether-broker caddy"
        for u in nats-server tether-broker caddy; do
            run "rm -f '$SYSTEMD_DIR/multi-user.target.wants/$u.service'"
        done
    elif [ "$DRY_RUN" -eq 1 ] || { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }; then
        run "systemctl disable nats-server tether-broker caddy 2>/dev/null || true"
        _DISABLED=1
    else
        # No systemd to run `disable`: drop the wants/ symlinks by hand so the
        # unit removal below does not leave them dangling.
        for u in nats-server tether-broker caddy; do
            run "rm -f '$SYSTEMD_DIR/multi-user.target.wants/$u.service'"
        done
    fi
    for u in nats-server tether-broker caddy; do
        # ...and the `.new` sidecar a kept re-run may have written beside it. origin:
        # prerelease audit round 2, K-F9: uninstall removed the unit and left its .new
        # behind forever, so a host that had ever been re-installed kept three orphan
        # files that no later run ever mentions again.
        run "rm -f '$SYSTEMD_DIR/$u.service' '$SYSTEMD_DIR/$u.service.new'"
    done
    # #77 symmetry (review F3): remove the journald drop-in ONLY if it carries
    # our ownership marker — a same-name operator/site-policy file that merely
    # collided on the path must survive uninstall. Path is not proof; the
    # first-line marker is.
    _u_dropin="${TETHER_JOURNALD_ROOT:-${TETHER_INSTALL_ROOT:-}/etc/systemd}/journald.conf.d/60-tether.conf"
    _u_marker='# managed-by: tether-install.sh (#77 journald cap)'
    if [ "$DRY_RUN" -eq 1 ]; then
        log "  + (dry-run) remove $_u_dropin (only if it carries the tether ownership marker)"
    elif [ ! -f "$_u_dropin" ]; then
        : # nothing to remove
    elif head -n1 "$_u_dropin" 2>/dev/null | grep -qF "$_u_marker"; then
        run "rm -f '$_u_dropin'"
    else
        log "  ! $_u_dropin exists but is NOT tether-owned (no marker) — left in place"
    fi
    if ! host_systemd_is_the_target; then
        # external review M-4: nothing the host's systemd knows about changed, so there is
        # nothing for it to reload.
        log_would_run_systemctl "systemctl daemon-reload"
    elif [ "$DRY_RUN" -eq 1 ] || { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }; then
        # Best-effort: a daemon-reload refreshes systemd's view after the unit
        # files went; it is cleanup, not correctness. Tolerate failure (e.g. a
        # non-root/no-polkit context) so uninstall's file removals still stand.
        run "systemctl daemon-reload 2>/dev/null || true"
    fi
    run "rm -f '$BIN_DIR/tether'"
    # THE SAME WARNING THE AGENT SIBLING GOT, for a larger deletion.
    #
    # origin: prerelease audit round 2, K-F13. B7's principle — never delete something
    # irreplaceable without saying so — was applied to uninstall_agent in this very change
    # and not here, where /var/lib/tether holds the SQLite state, the raft data dir and the
    # secrets dir (the cluster CA and this node's tunnel key). Losing the secrets dir means
    # this node can never rejoin its cluster under the same identity.
    log ""
    log "  ⚠ REMOVING $LIB_DIR — the SQLite state, the raft data dir and the secrets"
    log "    dir (cluster CA + this node's tunnel key). The secrets cannot be regenerated:"
    log "    without them this node cannot rejoin its cluster under the same identity."
    log "    Take a backup first if this is not a decommission: tether cluster backup --offline"
    log ""
    # THE VARIABLES, NOT THE LITERALS. These were four hardcoded absolute paths, which
    # was merely redundant until TETHER_INSTALL_ROOT existed (round 2, G2) and then
    # became dangerous: a redirected uninstall would have rm -rf'd the REAL /etc/tether
    # and /var/lib/tether — the SQLite state, the raft dir and the unregenerable secrets
    # — on a host whose matching install had touched none of them.
    run "rm -rf '$ETC_DIR' '$LIB_DIR' '$LOG_DIR' '$RUN_DIR'"
    if [ "$_DISABLED" -eq 1 ]; then
        log "  ✔ removed broker files (systemd units disabled+removed, journald drop-in, /etc/tether, /var/{lib,log,run}/tether)"
    else
        log "  ✔ removed broker files (unit files + boot symlinks removed [systemd not running], journald drop-in, /etc/tether, /var/{lib,log,run}/tether)"
    fi
    log "  ✔ note: did NOT remove the 'tether' system user; do that manually if intended"
}

# -- dispatch --------------------------------------------------------------

# Resolve VERSION once after argparse so every role's install path
# sees the same value (no per-role drift if the redirect flaps).
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
