#!/bin/sh
# 32-install-lifecycle.sh — S5 (FRESH un-provisioned container + §8.4 running-broker upgrade): install.sh
# lifecycle. --dry-run zero-write (three roles), never-start invariant (install.sh starts nothing), --uninstall →
# zero + reinstall idempotent, ownership table (/etc/tether root-owned, nats.d tether-owned, agent ~/.local/bin
# sim-owned per S0-layout), REAL agent+ctl binary place_binary/run/never-start/uninstall (via S0-artifact), and
# §8.4 SINGLE-BROKER MANUAL UPGRADE (stop→swap→sqlite3 integrity_check→start→G.2 business convergence). Uses a
# FRESH bare container (run_node) for the install lifecycle (a virgin volume, not a provisioned node whose
# sentinel would false-green "zero-write"/"idempotent"), then a proper init'd brk1 for §8.4.
#
# external-review R5-M4 (all NOW COVERED): (1) manifest FAILS CLOSED on a partial/permission-failed find traversal
# (MANIFEST-FIND-ERR — the old `find|grep|sort` lost find's rc and accepted a partial list); (2) ONE combined EXIT
# trap (the two traps overwrote each other, leaking the artifact container on early failure); (3) REAL ctl binary
# boundary (its OWN place_binary path, not "same as agent"); (4) §8.4 implemented (the OPERATOR manual path —
# systemctl+install — NOT the #31/#28-blocked `cluster upgrade`/`node upgrade` verbs).
#
# FALSE-GREEN GUARDS:
#  - dry-run zero-write asserted by a CONTENT+metadata manifest (sha256/mode/numeric-uid-gid/link), fail-closed on
#    any traversal/stat/hash error (_snap returns a unique sentinel), not exit-0 and not a path-name md5.
#  - never-start asserted by `! pgrep -x tether` (NOT `is-active`, which a masked unit would also satisfy).
#  - ownership asserted with real stat -c %U against the install.sh layout (no sim chown — Mandate ①).
#  - §8.4 asserted by REAL tunneled bytes recovering (curl the sentinel) + version-flip ∧ MainPID-changed together
#    ("no upgrade happened" also satisfies convergence + would leave the version on v-cur).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/assert.sh"; . "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
INST=/opt/sim/install.sh
F=fresh32
# external-review R5-M4: ONE combined EXIT trap. The two separate `trap ... EXIT` statements overwrote each
# other (the 2nd replaced the 1st), so an early failure after artifact_up leaked the artifact container. Both
# artifact_down and the fresh-container reap now run on ANY exit path (the §8.4 sim topology is reaped by
# cmd_drill's finalizer nuke).
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): the combined `EXIT INT TERM` form still RESUMED after the
# handler on a Ctrl-C — cleanup would run and the drill would then keep going through the §8.4 stop/binary-swap
# steps. drill_install_traps keeps the single EXIT registration (the R5-M4 fix above) and additionally exits
# 128+signo on INT/TERM without resuming. Cleanup body unchanged.
_cleanup() { artifact_down 2>/dev/null; rm_node "$F" --vols 2>/dev/null; }
drill_install_traps _cleanup
IN()    { dexec "$F" -- "$@"; }
INSIM() { dexec -u sim "$F" -- env HOME=/home/sim "$@"; }
_systemd_up() { IN systemctl is-system-running 2>/dev/null | grep -qE 'running|degraded|starting'; }
# CONTENT+METADATA manifest (external-review M5 + self-review R3-M4): a --dry-run must change NOTHING, so the
# zero-write oracle must be sensitive to file CONTENT (sha256), mode, NUMERIC uid/gid, size AND link target — NOT
# just path NAMES. (The old path-name md5 let a dry-run overwrite content, or chmod/chown a path, and STILL show
# "zero write".) R3-M4 hardening over the round-3 manifest: (1) NUMERIC %u|%g (was symbolic %U|%G — a uid renumber
# 1000→1001 keeping the same NAME left the digest byte-identical); (2) symlinks now record their OWN mode/uid/gid
# (was path+target only — an owner change on a symlink was invisible); (3) a catch-all "O" branch records FIFO/
# socket/device/other types (were dropped by the L/D/F chain); (4) `while IFS= read -r` replaces `for f in $(find)`
# so whitespace-bearing paths are not word-split. Residual gap (noted, not faked): embedded-NEWLINE paths would
# still split — dash `read` has no `-d ''`; the tether/nats system paths never contain newlines.
_manifest() {
    # external-review R5-M4: CAPTURE the traversal rc + stderr — the old `find ... 2>/dev/null | grep | sort`
    # made grep/sort the pipeline exit, so a PARTIAL traversal (a readable root + an unreadable subtree) emitted a
    # nonempty partial list with NO error marker and was accepted as "zero write", HIDING files under the failed
    # subtree. Now: enumerate only roots that EXIST (a not-yet-created root is benign ENOENT, not a failure), run
    # find with stderr captured + rc checked, and emit MANIFEST-FIND-ERR (a fail-closed sentinel _snap rejects) on
    # ANY traversal error (EACCES on a subtree) so a permission/read failure can never certify zero-write.
    IN sh -c '_roots=""; for r in /etc/tether /home/sim/.tether /home/sim/.local/bin /etc/systemd/system; do [ -e "$r" ] && _roots="$_roots $r"; done
[ -n "$_roots" ] || { echo "MANIFEST-EMPTY-ROOTS-MISSING"; exit 0; }
_ferr=$(find $_roots 2>&1 1>/tmp/_mf.out); _frc=$?
if [ "$_frc" != 0 ] || [ -n "$_ferr" ]; then echo "MANIFEST-FIND-ERR rc=$_frc"; rm -f /tmp/_mf.out; exit 0; fi
_out=$(grep -iE "tether|nats" /tmp/_mf.out | sort); rm -f /tmp/_mf.out
[ -n "$_out" ] || { echo "MANIFEST-EMPTY-ROOTS-MISSING"; exit 0; }
printf "%s\n" "$_out" | while IFS= read -r f; do
if [ -L "$f" ]; then printf "L %s [%s] -> %s\n" "$f" "$(stat -c "%a|%u|%g" "$f" 2>/dev/null || echo STATERR)" "$(readlink "$f" 2>/dev/null || echo LINKERR)";
elif [ -d "$f" ]; then printf "D %s [%s]\n" "$f" "$(stat -c "%a|%u|%g" "$f" 2>/dev/null || echo STATERR)";
elif [ -f "$f" ]; then _st=$(stat -c "%a|%u|%g|%s" "$f" 2>/dev/null || echo STATERR); _h=$(sha256sum "$f" 2>/dev/null | cut -d" " -f1); printf "F %s [%s] %s\n" "$f" "$_st" "${_h:-HASHERR}";
else printf "O %s [%s]\n" "$f" "$(stat -c "%F|%a|%u|%g" "$f" 2>/dev/null || echo STATERR)";
fi
done'
}
# fail-CLOSED (external-review R4-M4): the manifest must be NON-EMPTY (roots exist) AND carry NO error marker — an
# empty digest or a serialized stat/hash/readlink error must NEVER certify "zero write" (two equally-failed snaps
# would otherwise compare equal). _snap returns the md5 ONLY for a valid manifest; else a UNIQUE sentinel that
# cannot equal any real snapshot (so every zero-write assertion FAILS on an invalid manifest).
_snap() {
    _m=$(_manifest)
    if [ -z "$_m" ] || printf '%s' "$_m" | grep -qE 'MANIFEST-EMPTY|MANIFEST-FIND|STATERR|HASHERR|LINKERR'; then
        printf 'MANIFEST-INVALID-%s' "$(printf '%s' "$_m" | md5sum | cut -d' ' -f1)"; return 1
    fi
    printf '%s' "$_m" | md5sum | cut -d' ' -f1
}
_snap_is(){ _s=$(_snap) || return 1; [ "$_s" = "$1" ]; }
# CONTENT-mutation self-test (R4-M4): prove the manifest is content-sensitive AND that a mutation is reversible
# BYTE-EXACT. Back the target file up OUTSIDE the manifest roots (cp -a, metadata-preserving), append one byte,
# snap (must DIFFER), then RESTORE from the backup (mv, byte-exact — NOT a sed line-delete that can leave residue)
# and snap again (must return to the ORIGINAL digest). Both halves gate.
_manifest_detects_content() {
    _mf0=$(_snap) || return 1
    IN sh -c 'f=$(find /etc/tether -name "*.yaml" -o -name "*.conf" 2>/dev/null | head -1); [ -n "$f" ] && cp -a "$f" /tmp/m5bak && printf "x" >> "$f"' || return 1
    _mf1=$(_snap)
    IN sh -c 'f=$(find /etc/tether -name "*.yaml" -o -name "*.conf" 2>/dev/null | head -1); [ -n "$f" ] && cp -a /tmp/m5bak "$f"; rm -f /tmp/m5bak' 2>/dev/null || true
    _mf2=$(_snap) || return 1
    [ "$_mf0" != "$_mf1" ] && [ "$_mf0" = "$_mf2" ]   # mutation MOVED the digest AND the byte-exact restore returned it
}
_no_tether_running() { ! IN pgrep -x tether >/dev/null 2>&1; }
_owner_is() { [ "$(IN stat -c '%U' "$1" 2>/dev/null)" = "$2" ]; }
_broker_install() { IN sh -c "sh $INST --role broker --skip-download --domain brkx --acme-email x@x.test"; }
_ownership_ok() { _owner_is /etc/tether root && _owner_is /etc/tether/nats.d tether; }
_uninstall_zeroed() {
    IN sh -c "sh $INST --role broker --uninstall" >/dev/null 2>&1
    # broker uninstall removes the binary + units; /etc/tether may persist (data) — assert the UNITS are gone.
    ! IN test -f /etc/systemd/system/tether-broker.service
}
# S0-layout: install.sh --role agent targets BIN_DIR=$HOME/.local/bin (user-writable) — verified via the
# dry-run output path (real install needs a tarball; the target dir is the load-bearing fact, dry-run shows it).
_agent_layout() {
    _o=$(INSIM sh -c "sh $INST --role agent --dry-run --broker wss://brkx/nats --session lab --nid a1 --pin 111111" 2>&1)
    printf '%s' "$_o" | grep -qE '\.local/bin'
}

drill_begin "32-install-lifecycle (fresh container install.sh lifecycle)"
"$SIM" nuke >/dev/null 2>&1 || true
rm_node "$F" --vols 2>/dev/null || true
run_node broker "$F"
assert_ok "fresh bare container systemd up (NOT provisioned)"  poll_until 30 2 "systemd running" -- _systemd_up

# ── dry-run zero-write (three roles; tree byte-identical after all three) ─────
B0=$(_snap); log "32: pre-dryrun tree md5=$B0"
# M5: ctl/agent are USER tools — run their dry-runs as the SIM user (not root), the production identity. broker
# is root (system service). The content+metadata manifest (B0) is captured as root so it still sees /home/sim writes.
assert_ok "ctl --dry-run as sim (zero download/chmod/write)"     INSIM sh -c "sh $INST --role ctl --dry-run"
assert_ok "agent --dry-run as sim (needs dummy broker/session/nid)"  INSIM sh -c "sh $INST --role agent --dry-run --broker wss://brkx/nats --session lab --nid a1 --pin 111111"
assert_ok "broker --dry-run as root (skip-download)"              IN sh -c "sh $INST --role broker --dry-run --skip-download --domain brkx --acme-email x@x.test"
assert_ok "dry-run ZERO write (CONTENT+metadata manifest byte-identical after 3 dry-runs — sha256/mode/owner/link, not just path names; M5)"  _snap_is "$B0"

# ── real broker install → never-start + ownership + manifest content-sensitivity self-test ─────
assert_ok "broker install (real, skip-download uses baked binaries)"  _broker_install
assert_ok "never-start invariant: no tether process after install (install.sh starts nothing)"  _no_tether_running
assert_ok "ownership: /etc/tether root-owned AND /etc/tether/nats.d tether-owned (real install.sh layout)"  _ownership_ok
# M5 self-test: with real config files now present, prove the zero-write manifest is CONTENT-sensitive (mutating a
# byte of an existing file moves the digest) — the guard that keeps the oracle from ever regressing to path-names.
assert_ok "manifest is CONTENT-sensitive (mutating a byte of an existing tracked file MOVES the digest — the old path-name md5 could not detect this; M5)"  _manifest_detects_content

# ── #76: broker units ENABLED for boot (symlink only), never started ──────────────────────────
# The K.0 §2 contract's two halves, pinned together: enabled≠started. is-enabled reports the boot
# symlink exists; pgrep asserts NO process; the two together are exactly the amended invariant. The
# live incident was a single broker shipped DISABLED — one reboot took the fleet offline.
_units_enabled()  { for u in nats-server tether-broker caddy; do IN systemctl is-enabled "$u" 2>/dev/null | grep -qx enabled || return 1; done; }
_units_disabled() { for u in nats-server tether-broker caddy; do _s=$(IN systemctl is-enabled "$u" 2>/dev/null); [ "$_s" = enabled ] && return 1; done; return 0; }
assert_ok "#76 broker units are ENABLED for boot (nats-server tether-broker caddy all is-enabled)"  _units_enabled
assert_ok "#76 enabled-but-NOT-started: still no tether process (enable is a symlink, not a start — K.0 §2 amended)"  _no_tether_running

# #76 --no-enable opt-out: a fresh install with --no-enable leaves the units disabled.
_broker_install_noenable() { IN sh -c "sh $INST --role broker --skip-download --no-enable --domain brkx --acme-email x@x.test" >/dev/null 2>&1; }
assert_ok "uninstall before the --no-enable check"  _uninstall_zeroed
assert_ok "#76 broker install --no-enable"  _broker_install_noenable
assert_ok "#76 --no-enable leaves the units DISABLED (the operator opted out)"  _units_disabled
assert_ok "reinstall broker (default, re-enables) for the #77 + uninstall checks below"  _broker_install
assert_ok "#76 default reinstall re-enabled the units"  _units_enabled

# ── #77: journald size cap drop-in, value derived from THIS container's /var/log filesystem ────
# The drill INDEPENDENTLY recomputes the expected tier from df (never trusts the product's output),
# so a wrong derivation is caught, not rubber-stamped.
_expected_journald_cap() {
    _kb=$(IN df -Pk /var/log 2>/dev/null | awk 'NR==2{print $2}'); _kb=${_kb:-0}
    if   [ "$_kb" -lt 10485760 ]; then echo 200M
    elif [ "$_kb" -lt 41943040 ]; then echo 500M
    else echo 1024M; fi
}
_journald_dropin_value() { IN sh -c "grep -h '^SystemMaxUse=' /etc/systemd/journald.conf.d/60-tether.conf 2>/dev/null | cut -d= -f2"; }
# assert_ok runs predicates as FUNCTIONS inheriting this shell (drill header /
# R-NOSHC) — an outer `sh -c` would start a fresh shell where these helpers are
# not-found, both command-subs empty, and `[ "" = "" ]` a false-GREEN identity.
# The non-empty guard makes a double-empty read FAIL rather than pass.
_caps_match() { _a=$(_journald_dropin_value); _b=$(_expected_journald_cap); [ -n "$_a" ] && [ "$_a" = "$_b" ]; }
assert_ok "#77 journald drop-in exists (/etc/systemd/journald.conf.d/60-tether.conf)"  IN test -f /etc/systemd/journald.conf.d/60-tether.conf
assert_ok "#77 drop-in SystemMaxUse == the tier the drill INDEPENDENTLY derives from df /var/log (not trusting the product's number)" \
    _caps_match

# #77 respect an operator's explicit setting: pre-seed journald.conf, reinstall, our drop-in must NOT appear.
_seed_operator_journald() { IN sh -c "printf '[Journal]\nSystemMaxUse=800M\n' > /etc/systemd/journald.conf"; }
assert_ok "uninstall before the operator-setting check"  _uninstall_zeroed
assert_ok "#77 pre-seed an explicit operator SystemMaxUse in journald.conf"  _seed_operator_journald
assert_ok "reinstall broker with the operator setting present"  _broker_install
assert_ok "#77 our drop-in is NOT written when the operator already set SystemMaxUse (operator setting respected)" \
    IN sh -c "! test -f /etc/systemd/journald.conf.d/60-tether.conf"
assert_ok "#77 cleanup: remove the seeded journald.conf + reinstall clean for the uninstall check"  \
    IN sh -c "rm -f /etc/systemd/journald.conf && sh $INST --role broker --skip-download --domain brkx --acme-email x@x.test >/dev/null 2>&1"

# ── uninstall → units gone (+ disabled + drop-in removed), reinstall idempotent ───────────────
# _units_notfound == _units_disabled (N10): both assert no unit reports is-enabled==enabled.
_uninstall_then_notfound() { IN sh -c "sh $INST --role broker --uninstall" >/dev/null 2>&1; _units_disabled; }
assert_ok "#76 uninstall DISABLES the units before removing them (no dangling boot symlinks)"  _uninstall_then_notfound
assert_ok "#77 uninstall removed our journald drop-in"  IN sh -c "! test -f /etc/systemd/journald.conf.d/60-tether.conf"
assert_ok "reinstall broker idempotent after the full-symmetry uninstall"  _broker_install
assert_ok "uninstall broker → tether-broker.service removed"  _uninstall_zeroed
assert_ok "reinstall broker idempotent (clean second install)"  _broker_install
assert_ok "reinstall still never-starts"  _no_tether_running

# ── agent install → ~/.local/bin (S0-layout: user-writable, per install.sh BIN_DIR=$HOME/.local/bin) ─────
assert_ok "agent install (as sim) targets ~/.local/bin (user-writable BIN_DIR; node upgrade atomic-replace target; S0-layout)"  _agent_layout

# ── REAL agent BINARY install (external-review R4-M4): NOT --skip-download (which SKIPS place_binary entirely).
#    Stage a REAL https release tarball via S0-artifact (the drill-31 source) so install.sh actually DOWNLOADS,
#    verifies the sha, and EXTRACTS + atomic-places the binary. Then prove real extraction (the binary RUNS) +
#    never-start + uninstall-removes-the-binary — boundaries --skip-download can NEVER exercise. ──
_VER=v0.0.0-sim; _TARBALL="tether_${_VER#v}_linux_amd64.tar.gz"
artifact_up "$INSTANCE" "$HERE/vendor/tether-next" "$_TARBALL" || die "32: artifact_up failed (run remote.sh --build for vendor/tether-next)"
dexec "$ARTIFACT_NODE" -- sh -c "printf '%s  %s\n' '$ART_SHA' '$_TARBALL' > /artifacts/SHA256SUMS"   # install.sh fetches <base>/SHA256SUMS
ingress_trust_inject "$F"   # the fresh container must trust the artifact's instance-CA leaf for the https fetch
_agent_real_install() { INSIM sh -c "sh $INST --role agent --source-base https://$INSTANCE-artifact --version $_VER --broker wss://brkx/nats --session lab --nid a1 --pin 111111" >/dev/null 2>&1 && INSIM test -x /home/sim/.local/bin/tether; }
_agent_bin_runs()      { INSIM /home/sim/.local/bin/tether version >/dev/null 2>&1; }
_agent_uninstalled()   { INSIM sh -c "sh $INST --role agent --uninstall" >/dev/null 2>&1; ! INSIM test -f /home/sim/.local/bin/tether; }
assert_ok "AGENT-real install.sh --role agent REAL-downloads+extracts the S0-artifact tarball → a real binary at ~/.local/bin/tether (NOT --skip-download; exercises place_binary atomic-place, not a stub)"  _agent_real_install
assert_ok "AGENT-real the placed binary RUNS ('tether version' — proves a genuine extraction, not a dry-run no-op)"  _agent_bin_runs
assert_ok "AGENT-real never-start: install.sh started nothing after the real agent install"  _no_tether_running
assert_ok "AGENT-real uninstall REMOVES the placed binary (~/.local/bin/tether gone — a real binary-placement + removal boundary)"  _agent_uninstalled

# ── REAL ctl BINARY install (external-review R5-M4): the ctl role's OWN real place_binary boundary — NOT "same
#    as agent". ctl as the sim user → /usr/local/bin unwritable → BIN_DIR=~/.local/bin (install_ctl:394-400).
#    Runs AFTER the agent uninstall above, so ~/.local/bin/tether is clean before the ctl install places it. ──
_ctl_real_install() { INSIM sh -c "sh $INST --role ctl --source-base https://$INSTANCE-artifact --version $_VER" >/dev/null 2>&1 && INSIM test -x /home/sim/.local/bin/tether; }
_ctl_bin_runs()     { INSIM /home/sim/.local/bin/tether version >/dev/null 2>&1; }
_ctl_uninstalled()  { INSIM sh -c "sh $INST --role ctl --uninstall" >/dev/null 2>&1; ! INSIM test -f /home/sim/.local/bin/tether; }
assert_ok "CTL-real install.sh --role ctl REAL-downloads+extracts the S0-artifact tarball → ~/.local/bin/tether (ctl's OWN place_binary path; as sim, /usr/local/bin unwritable → ~/.local/bin)"  _ctl_real_install
assert_ok "CTL-real the placed ctl binary RUNS ('tether version' — genuine extraction, not a dry-run no-op)"  _ctl_bin_runs
assert_ok "CTL-real never-start: install.sh --role ctl started nothing (ctl is a user tool — installs no service at all)"  _no_tether_running
assert_ok "CTL-real uninstall REMOVES the placed ctl binary (~/.local/bin/tether gone)"  _ctl_uninstalled
artifact_down 2>/dev/null || true

# ── §8.4 SINGLE-BROKER MANUAL UPGRADE (external-review R5-M4: locked plan §4-32 §8.4; broker-ops.md:530-537) ──
#    stop tether-broker → swap the binary → sqlite3 PRAGMA integrity_check → start → G.2 business convergence
#    (real read-write recovers). This is the OPERATOR manual path — NOT `cluster upgrade` (the #31 grow-lock verb)
#    nor `node upgrade` (the #28 agent-allowlist verb): a single live N=1 broker, systemctl stop/install/start,
#    so it is FEASIBLE without either wall. Runs on the sim's proper brk1 (init'd → auth_callout + JS). The fresh
#    container $F is reaped first (§8.4 needs a real init'd broker, not a bare install target).
rm_node "$F" --vols >/dev/null 2>&1 || true
NEXTVER="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}"
_biz_port() { "$SIM" ctl -- expose explain "$1" --json 2>/dev/null | jq -r '.public_port//empty' 2>/dev/null; }
_biz_serves() { _bp=$(_biz_port "$1"); [ -n "$_bp" ] && dp_curl_ok_body ctl1 "http://brk1:$_bp/" "$TOK84"; }
_mkbiz() { "$SIM" ctl -- expose rm agt1 --name "$1" >/dev/null 2>&1; "$SIM" ctl -- expose agt1 --local 8080 --name "$1" >/dev/null 2>&1; _biz_serves "$1"; }
# the RUNNING broker's self-reported version. N=1 has no colocated agent, so `node ls --brokers .BrokerVer`
# correlation is absent — read the broker's OWN `cluster status --json .release_version` (admin socket, broker-local,
# no ctl-session dependency). Post-restart with the swapped binary this flips v-cur→NEXTVER.
_ver84() { "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether tether cluster status --json 2>/dev/null | jq -r '.nodes[]?|select(.node_id=="brk1")|.release_version // empty' 2>/dev/null | head -1; }
_mainpid84() { "$SIM" exec brk1 -- systemctl show tether-broker -p MainPID --value 2>/dev/null; }
_swap84() { d cp "$HERE/vendor/tether-next" "$(ctr_name brk1):/tmp/tn84" >/dev/null 2>&1 && dexec brk1 -- install -m 0755 /tmp/tn84 /usr/local/bin/tether; }
_integrity84() { "$SIM" exec brk1 -- runuser -u tether -- sqlite3 /var/lib/tether/tether.db 'PRAGMA integrity_check;' 2>/dev/null | grep -qi '^ok$'; }
_ver84_is_next() { [ "$(_ver84)" = "$NEXTVER" ]; }

assert_ok "84-up 1 broker + 1 agent + 1 ctl (fresh sim topology for the §8.4 running-broker upgrade)"  "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "84-init brk1 (N=1 cluster: auth_callout + JS bootstrap)"  "$SIM" init brk1
assert_ok "84-session lab + ctl login (owner)"  "$SIM" session lab --pin 135790
# agent-join already onboards agt1 with a working tunnel to brk1 (its only broker) — no separate
# agent_provision_yaml needed for §8.4 (which only needs a plain expose, not a proxy policy). The baseline
# expose serving IS the gate that agt1's tunnel is up (an un-provisioned agent would fail 84-baseline).
assert_ok "84-agent-join agt1 (onboards + tunnel to brk1)"  "$SIM" agent-join agt1 --session lab --pin 135790
TOK84=$(expose_serve_sentinel agt1 8080); [ -n "$TOK84" ] || die "32-§8.4: agt1 sentinel empty"
assert_ok "84-baseline expose biz on brk1 serves the sentinel (business DATA plane LIVE pre-upgrade)"  poll_until_fixed 30 2 "biz serves" -- _mkbiz biz
PB84=$(_biz_port biz); MP84_0=$(_mainpid84); V84_0=$(_ver84)
log "32-§8.4: pre-upgrade brk1 running-version=[$V84_0] MainPID=$MP84_0 biz-port=$PB84 ; staged tether-next=$NEXTVER"
assert_ok "84-pre-version brk1 runs v-cur (NOT $NEXTVER yet — anti-vacuity for the flip below)"  sh -c "[ \"$V84_0\" != '$NEXTVER' ]"

# broker-ops §8.4 steps, each distinct (the [env PRIVILEGED] swap is never made to look automatic):
assert_ok "84-stop systemctl stop tether-broker (business goes DOWN)"  "$SIM" exec brk1 -- systemctl stop tether-broker
assert_ok "84-down expose biz curl → connection-refused (broker stopped; business genuinely down, curl exit 7 — not a half-open listener)"  poll_until 20 2 "brk1:$PB84 refused" -- dp_curl_refused ctl1 "http://brk1:$PB84/"
assert_ok "84-[env PRIVILEGED] swap /usr/local/bin/tether → tether-next (root-owned bin; operator installs it, cluster/node upgrade does NOT — a distinct labeled step)"  _swap84
assert_ok "84-staged-sha the on-disk binary is now the staged tether-next (sha matches vendor/tether-next)"  sh -c "[ \"\$(\"$SIM\" exec brk1 -- sha256sum /usr/local/bin/tether | awk '{print \$1}')\" = \"\$(sha256sum '$HERE/vendor/tether-next' | awk '{print \$1}')\" ]"
assert_ok "84-integrity PRAGMA integrity_check == ok (broker-ops §8.4 line 535 — the tether.db is intact across the binary swap)"  _integrity84
assert_ok "84-start systemctl start tether-broker (the NEW binary boots + runs G.2 reconcile)"  "$SIM" exec brk1 -- systemctl start tether-broker

# G.2 convergence — real business read-write recovers ON THE NEW BINARY (the #20 data-plane oracle, not a field):
assert_ok "84-converge-persist original expose biz serves the sentinel AGAIN on brk1:$PB84 (agent re-registered + state PERSISTED across the swap — G.2 reconcile; real tunneled bytes)"  poll_until 90 3 "biz recovers" -- _biz_serves biz
assert_ok "84-converge-write a NEW expose biz2 created POST-upgrade serves the sentinel (control-plane WRITE + data plane BOTH work on the new binary — real read-write, not a health field)"  poll_until_fixed 60 3 "biz2 serves" -- _mkbiz biz2
assert_ok "84-version-flip brk1 running (self-reported) version flipped v-cur→$NEXTVER via cluster status .release_version (a 'no upgrade happened' run would leave it on v-cur — the false-green guard the plan mandates)"  poll_until 60 3 "brk1 self-reports $NEXTVER" -- _ver84_is_next
assert_ok "84-pid-changed tether-broker MainPID CHANGED across §8.4 (a real systemctl stop/start swap, NOT drill 30's syscall.Exec in-place re-exec — version-flip ∧ PID-changed TOGETHER prove a genuine swap happened)"  sh -c "[ \"\$(\"$SIM\" exec brk1 -- systemctl show tether-broker -p MainPID --value)\" != '$MP84_0' ]"

log "32 R5-M4 NOW-COVERED: (1) find-traversal rc is captured — a partial/permission-failed traversal fails closed (MANIFEST-FIND-ERR); (2) ONE combined EXIT trap (artifact_down + fresh-container reap both run on any exit); (3) REAL ctl binary place_binary/run/never-start/uninstall (its OWN path, not 'same as agent'); (4) §8.4 single-broker manual upgrade — stop→swap→integrity_check→start→G.2 business convergence (real read-write recovers) with version-flip ∧ PID-changed guards. §8.4 is the OPERATOR manual path (systemctl+install), NOT the #31/#28-blocked cluster/node upgrade verbs."
drill_end
