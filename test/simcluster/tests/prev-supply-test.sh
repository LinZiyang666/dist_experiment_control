#!/bin/sh
# origin: hot-update S5 plan §9.3 / §9.5 / T48.
# Scratch git objects are imported directly. Tiny real Go artifacts exercise
# cache validation; they are mechanism fixtures, never release/S8 evidence.
set -eu
[ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
    printf 'previous-supply tests: shared process supervisor is required\n' >&2; exit 2;
}
# Re-enter through the shared helper before creating any scratch children.
if [ "${SIM_PREV_SUPPLY_SUPERVISED:-}" != 1 ]; then
    SIM_PREV_SUPPLY_SUPERVISED=1
    export SIM_PREV_SUPPLY_SUPERVISED
    exec "$SIM_UPGRADE_SUPERVISOR" -- sh "$0" "$@"
fi
HERE=$(cd "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT HUP INT TERM
SIM_REPO="$TMP/repo"; SIM_VENDOR="$TMP/vendor"; SIM_STAGE_LIB="$HERE/lib"; mkdir -p "$SIM_REPO" "$SIM_VENDOR"
git -C "$SIM_REPO" init -q
python3 - "$SIM_REPO" <<'PYGIT'
import subprocess, sys
stream = b''
for i, tag in enumerate(('v0.5.0', 'v0.6.0', None), 1):
    content = ('file-%d\n' % i).encode()
    stream += b'blob\nmark :%d\ndata %d\n' % (i, len(content)) + content + b'\n'
    stream += (b'commit refs/heads/main\nmark :%d\ncommitter Test <test@example.invalid> %d +0000\ndata 4\nmsg\n' % (10+i, i))
    if i > 1: stream += b'from :%d\n' % (9+i)
    stream += b'M 100644 :%d f\n' % i
    for name, content in {'go.mod': b'module github.com/LinZiyang666/tether\n\ngo 1.26.0\n',
                          'cmd/tether/main.go': b'package main\nimport "fmt"\nfunc main() { fmt.Println("mechanism fixture") }\n'}.items():
        stream += b'M 100644 inline '+name.encode()+b'\ndata '+str(len(content)).encode()+b'\n'+content
    stream += b'\n'
    if tag: stream += ('reset refs/tags/%s\nfrom :%d\n\n' % (tag, 10+i)).encode()
subprocess.run(['git', '-C', sys.argv[1], 'fast-import', '--quiet'], input=stream, check=True)
subprocess.run(['git', '-C', sys.argv[1], 'symbolic-ref', 'HEAD', 'refs/heads/main'], check=True)
subprocess.run(['git', '-C', sys.argv[1], 'read-tree', '--reset', '-u', 'HEAD'], check=True)
PYGIT
TOP_COMMIT=$(git -C "$SIM_REPO" rev-parse HEAD)
. "$HERE/lib/stage.sh"
check() { if ! "$@"; then printf 'FAIL: %s\n' "$*" >&2; exit 1; fi; }
reject() { if "$@"; then printf 'FAIL: accepted invalid supply: %s\n' "$*" >&2; exit 1; fi; }
check test "$(sim_prev_tag)" = v0.6.0
# Moving only a scratch branch + reading its tree gives the exact-tag state.
git -C "$SIM_REPO" update-ref refs/heads/main "$(git -C "$SIM_REPO" rev-parse v0.6.0)"
git -C "$SIM_REPO" read-tree --reset -u HEAD
check test "$(sim_prev_tag)" = v0.5.0
printf dirty >> "$SIM_REPO/f"
check test "$(sim_prev_tag)" = v0.5.0
DIGEST=$(sim_dirty_patch_sha)
printf changed >> "$SIM_REPO/f"
check test "$(sim_dirty_patch_sha)" != "$DIGEST"
DIGEST=$(sim_dirty_patch_sha)
printf input > "$SIM_REPO/untracked"
check test "$(sim_dirty_patch_sha)" != "$DIGEST"
DIGEST=$(sim_dirty_patch_sha)
printf changed > "$SIM_REPO/untracked"
check test "$(sim_dirty_patch_sha)" != "$DIGEST"
DIGEST=$(sim_dirty_patch_sha)
chmod +x "$SIM_REPO/untracked"
check test "$(sim_dirty_patch_sha)" != "$DIGEST"
# A failing archive build must discard both stale files and keep the real rc.
printf stale > "$SIM_VENDOR/tether-prev"; chmod +x "$SIM_VENDOR/tether-prev"
printf '{}' > "$SIM_VENDOR/tether-prev-source.json"
reject sim_stage_prev
check test ! -e "$SIM_VENDOR/tether-prev"
check test ! -e "$SIM_VENDOR/tether-prev-source.json"
check test "$(git -C "$SIM_REPO" worktree list --porcelain | awk '/^worktree / {n++} END {print n}')" = 1
COMMIT=$(git -C "$SIM_REPO" rev-parse 'v0.6.0^{commit}')
# Real scratch-source bytes exercise cache acceptance; the production fixed
# pin must still reject this unrelated tag with the same human version label.
check sim_stage_release v0.6.0 tether-prev
check sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
check sim_stage_release v0.6.0 tether-prev
reject sim_prev_source_ok v0.6.0
reject sim_prev_source_ok v0.5.0
cp "$SIM_VENDOR/tether-prev-source.json" "$TMP/good.json"
for expr in '.tag="v0.5.0"' '.commit="abc"' '.binary_sha256="000"' '.component="ctl"' 'del(.tag)' '.schema_version=true'; do
    jq "$expr" "$TMP/good.json" > "$SIM_VENDOR/tether-prev-source.json"
    reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
done
cp "$TMP/good.json" "$SIM_VENDOR/tether-prev-source.json"
cp "$SIM_VENDOR/tether-prev" "$TMP/good-binary"
printf swapped > "$SIM_VENDOR/tether-prev"
reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
cp "$TMP/good-binary" "$SIM_VENDOR/tether-prev"
# Fixed supply cannot be vouched for by a tag-only or fabricated commit receipt.
check test "$SIM_FLOW_LEGACY_TAG" = v0.6.0
check test "$SIM_FLOW_LEGACY_COMMIT" = a3431a12be3cf947cc797c28fea93382fa68472d
reject sim_stage_flow_legacy
check test ! -e "$SIM_VENDOR/tether-flow-legacy"
check test ! -e "$SIM_VENDOR/tether-flow-legacy-source.json"
check sim_stage_release v0.6.0 tether-flow-legacy
reject sim_flow_legacy_source_ok
cp "$SIM_VENDOR/tether-flow-legacy-source.json" "$TMP/fixed-good.json"
for expr in '.tag="v0.7.0"' '.commit="abc"' '.component="ctl"' '.binary_sha256="wrong"'; do
    jq "$expr" "$TMP/fixed-good.json" > "$SIM_VENDOR/tether-flow-legacy-source.json"
    reject sim_flow_legacy_source_ok
done
cp "$TMP/fixed-good.json" "$SIM_VENDOR/tether-flow-legacy-source.json"
printf swapped > "$SIM_VENDOR/tether-flow-legacy"
reject sim_flow_legacy_source_ok
cp "$TMP/good-binary" "$SIM_VENDOR/tether-flow-legacy"
cp "$TMP/good-binary" "$SIM_VENDOR/tether"; cp "$TMP/good-binary" "$SIM_VENDOR/tether-next"
check sim_stage_release v0.6.0 tether-dynamic-prev
check sim_write_tether_source
check jq -e --arg d "$(sim_dirty_patch_sha)" --arg c "$COMMIT" '.dirty_patch_sha==$d and .tether_prev_commit==$c and .flow_legacy.tag=="v0.6.0" and (.build_flags|length)>0 and (.dynamic_previous.toolchain|length)>0' "$SIM_VENDOR/tether-source.json"
# Preserve the S1 controls for permissions, missing receipts, version ordering,
# unreachable tags, exact oldest release and the nearer-but-older merged tag.
chmod -x "$SIM_VENDOR/tether-prev"
reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
chmod +x "$SIM_VENDOR/tether-prev"
cp "$SIM_VENDOR/tether-prev-source.json" "$TMP/source-good.json"
rm "$SIM_VENDOR/tether-prev-source.json"
reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
printf '{"tag":"v0.6.0"}\n' > "$SIM_VENDOR/tether-prev-source.json"
reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
cp "$TMP/source-good.json" "$SIM_VENDOR/tether-prev-source.json"
# Duplicate keys cannot hide a conflicting source claim.
printf '{"tag":"wrong","tag":"v0.6.0"}\n' > "$SIM_VENDOR/tether-prev-source.json"
reject sim_release_source_ok tether-prev v0.6.0 "$COMMIT"
cp "$TMP/source-good.json" "$SIM_VENDOR/tether-prev-source.json"
rm "$SIM_REPO/untracked"
git -C "$SIM_REPO" update-ref refs/heads/main "$(git -C "$SIM_REPO" rev-parse v0.5.0)"
git -C "$SIM_REPO" read-tree --reset -u HEAD
reject sim_prev_tag
check test "$(sim_prev_tag 2>/dev/null)" = ""
# An unrelated v0.7.0 cannot influence the development branch's N-1; a
# nearer v0.5.2 merged hotfix still cannot beat the higher reachable v0.6.0.
python3 - "$SIM_REPO" "$TOP_COMMIT" <<'PYSIDES'
import subprocess, sys
repo, top = sys.argv[1:]
old = subprocess.check_output(['git','-C',repo,'rev-parse','v0.5.0']).strip()
stream = (b'commit refs/heads/unrelated\nmark :101\ncommitter Test <test@example.invalid> 4 +0000\ndata 4\nmsg\nfrom '+old+b'\n\nreset refs/tags/v0.7.0\nfrom :101\n\n'
          b'commit refs/heads/hotfix\nmark :102\ncommitter Test <test@example.invalid> 5 +0000\ndata 4\nmsg\nfrom '+old+b'\nM 100644 inline g\ndata 7\nhotfix\n\nreset refs/tags/v0.5.2\nfrom :102\n\n'
          b'commit refs/heads/main\ncommitter Test <test@example.invalid> 6 +0000\ndata 4\nmsg\nfrom '+top.encode()+b'\nmerge :102\nM 100644 inline g\ndata 7\nhotfix\n\n')
subprocess.run(['git','-C',repo,'fast-import','--quiet'],input=stream,check=True)
subprocess.run(['git','-C',repo,'read-tree','--reset','-u','HEAD'],check=True)
PYSIDES
check test "$(sim_prev_tag)" = v0.6.0
git -C "$SIM_REPO" update-ref refs/tags/zzz-latest "$(git -C "$SIM_REPO" rev-parse HEAD)"
check test "$(sim_prev_tag)" = v0.6.0
git -C "$SIM_REPO" update-ref refs/heads/main "$(git -C "$SIM_REPO" rev-parse v0.6.0)"
git -C "$SIM_REPO" read-tree --reset -u HEAD
# v0.5.2 is unreachable from exact v0.6.0 and cannot become its previous
# release. Clean and dirty exact-tag trees share this rule.
check test "$(sim_prev_tag)" = v0.5.0
EMPTY_REPO="$TMP/empty-repo"; git init -q "$EMPTY_REPO"
SAVED_REPO=$SIM_REPO; SIM_REPO=$EMPTY_REPO
reject sim_prev_tag
check test "$(sim_prev_tag 2>/dev/null)" = ""
SIM_REPO=$SAVED_REPO
printf 'prev-supply: all controls passed\n'
