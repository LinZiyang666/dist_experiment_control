# lib/vault.sh — S0-backup-vault: the OFF-CLUSTER backup vault (G-C / S7; s7-s9-plan §1.2-(1)).
# POSIX sh. Sourced by drills 50/51/94. Depends on lib/log.sh (log/warn/err) + lib/docker.sh (d/ctr_name).
#
# WHAT IT IS. A per-instance host-side directory holding backup bundles that tether's `cluster backup`
# wrote INSIDE a container. Its whole reason to exist is drill 51 (full-cluster DR): the bundle must
# survive `rm_node --vols` (the simulated volume disaster) and still be readable afterwards. It is the
# exact analogue of an operator scp-ing a nightly bundle off the box.
#
# LIFECYCLE (the roadmap §2 tuple, restated so it can be checked against the code below):
#   owning batch   G-C / S7
#   consumers      50 (--out target), 51 (the ONLY consumer that needs it to survive a volume disaster),
#                  94 (borrows a leader bundle to manufacture an orphan via the product path)
#   naming         $BACKUP_VAULT/$INSTANCE/<bundle>/   — mirrors lib/secrets.sh:13's SECRETS_STASH form
#   precheck       vault_init: rm -rf -> mkdir -m 0700 -> ASSERT EMPTY (see the note on fail-on-exists)
#   key material   NONE. A bundle is not a credential by product design (clusteroffline/restore.go:12-13;
#                  the manifest carries fingerprints only). The DR role of trust material belongs to the
#                  EXISTING SECRETS_STASH (= the operator's key vault); we do not build a second one.
#   health check   vault_pull verifies both files are non-empty + manifest parses + records a sha256 that
#                  51-C re-computes AFTER the disaster (that sha comparison IS the "survived" oracle).
#   teardown       `simcluster nuke` ONLY. `down -v` / `rm_node --vols` must NOT touch it — that is the
#                  disaster we are surviving.
#
# MANDATE (3)/(4) — why supplying this is not doing tether's job for it. Mounting a backup disk and
# chowning it to the service user is OPERATIONS. Moving a bundle off the machine and back is
# `[operator per runbook §5]`. The sim only supplies the machine: `cluster backup` writes its own bundle
# and `recovery restore` reads it. Crucially, drill 50 arm C first runs the runbook's own literal example
# and REPORTS THAT IT FAILS *before* the vault appears as `[env]` — so the supply masks no gap
# (R-SUPPLY-ORDER, s7-s9-plan §1.4).
#
# WHY docker cp AND NOT a bind-mount (measured on the sim server 2026-07-17, SB-VAULT). The bundle is
# written by the BROKER PROCESS as User=tether inside the container. `docker cp` out lands the files as
# weiland:weiland 0644 on the host (readable, sha256-able, no sudo); a bind-mount would instead expose the
# host directory to container-uid writes (an R-NO-HOST-LEAK surface). `secrets_distribute` already uses
# the same `d cp` idiom. Pushing back IN lands as uid 1000 (NOT root) — hence vault_push chowns.

: "${BACKUP_VAULT:=$HERE/backups}"      # gitignored host-side vault, per-instance (see .gitignore)

vault_dir() { printf '%s/%s' "$BACKUP_VAULT" "$INSTANCE"; }

# vault_init : create THIS instance's vault, fail-closed on a non-empty result.
#
# Why rm-then-assert-empty rather than refuse-if-exists: INSTANCE is the deterministic `drill-<name>`
# (simcluster:532) and `cmd_drill` SKIPS its nuke under SIM_KEEP=1 while its trap catches only INT/TERM
# (never EXIT) — so one operator inspection, one host OOM, or one dropped ssh would poison every later run
# of that drill FOREVER. Fail-closed belongs on the postcondition, not the precondition: the hazard we
# actually care about is a STALE BUNDLE making 51-C's "still readable after the disaster" pass for the
# wrong reason, and the emptiness assertion is what rules that out.
vault_init() {
    _vi_d=$(vault_dir)
    if [ -e "$_vi_d" ]; then
        warn "vault_init: pre-existing vault at $_vi_d (mtime $(stat -c %y "$_vi_d" 2>/dev/null || echo '?')) — removing it; a stale bundle would let 51-C pass for the wrong reason"
        rm -rf "$_vi_d" || { err "vault_init: could not remove $_vi_d"; return 1; }
    fi
    mkdir -p -m 0700 "$_vi_d" || { err "vault_init: could not create $_vi_d"; return 1; }
    # ASSERT EMPTY — the postcondition that makes 51-C's oracle meaningful.
    if [ -n "$(ls -A "$_vi_d" 2>/dev/null)" ]; then
        err "vault_init: vault $_vi_d is NOT empty after init"; return 1
    fi
    log "vault: initialized $_vi_d (0700, empty)"
}

# vault_pull <node> <container-bundle-dir> <vault-name> : copy a bundle the BROKER wrote out to the host
# vault, then health-check it. Prints nothing; returns non-zero on any failure.
vault_pull() {
    _vp_node=$1; _vp_src=$2; _vp_name=$3
    _vp_dst="$(vault_dir)/$_vp_name"
    rm -rf "$_vp_dst"
    _vp_err=$(d cp "$(ctr_name "$_vp_node")":"$_vp_src" "$_vp_dst" 2>&1) \
        || { err "vault_pull: docker cp $_vp_node:$_vp_src failed: $_vp_err"; return 1; }
    # health check: both files present + non-empty + the manifest actually parses and names a self_id.
    [ -s "$_vp_dst/state.db" ]      || { err "vault_pull: $_vp_name/state.db missing or empty"; return 1; }
    [ -s "$_vp_dst/manifest.json" ] || { err "vault_pull: $_vp_name/manifest.json missing or empty"; return 1; }
    jq -e '.self_id != null' "$_vp_dst/manifest.json" >/dev/null 2>&1 \
        || { err "vault_pull: $_vp_name/manifest.json does not parse or has no self_id"; return 1; }
    log "vault: pulled $_vp_node:$_vp_src -> $_vp_dst (state.db $(stat -c %s "$_vp_dst/state.db") bytes)"
}

# vault_push <vault-name> <node> <container-dst-dir> : copy a bundle from the host vault back INTO a
# container and hand it to tether. docker cp lands host uid 1000 inside the container (measured), so we
# chown to tether — the restore runs as User=tether per broker-ops.md:621-626 (#6).
vault_push() {
    _vu_name=$1; _vu_node=$2; _vu_dst=$3
    _vu_src="$(vault_dir)/$_vu_name"
    [ -d "$_vu_src" ] || { err "vault_push: no such bundle in vault: $_vu_src"; return 1; }
    dexec "$_vu_node" -- rm -rf "$_vu_dst" >/dev/null 2>&1 || true
    d cp "$_vu_src" "$(ctr_name "$_vu_node")":"$_vu_dst" >/dev/null 2>&1 \
        || { err "vault_push: docker cp -> $_vu_node:$_vu_dst failed"; return 1; }
    dexec "$_vu_node" -- sh -c "chown -R tether:tether '$_vu_dst' && chmod 0700 '$_vu_dst'" >/dev/null 2>&1 \
        || { err "vault_push: could not chown $_vu_dst to tether on $_vu_node"; return 1; }
    log "vault: pushed $_vu_name -> $_vu_node:$_vu_dst (tether-owned)"
}

# vault_has <vault-name> : predicate — the bundle's two files exist and are non-empty in the vault.
vault_has() {
    _vh="$(vault_dir)/$1"
    [ -s "$_vh/state.db" ] && [ -s "$_vh/manifest.json" ]
}

# vault_sha <vault-name> <file> : print sha256 of a bundle file (state.db | manifest.json).
# 51-C's ONLY honest "the bundle survived the disaster" oracle is re-computing this and comparing —
# `test -e` would let a truncated / half-written leftover pass for the wrong reason.
vault_sha() { sha256sum "$(vault_dir)/$1/$2" 2>/dev/null | awk '{print $1}'; }

# vault_manifest <vault-name> : print the manifest path (for jq oracles).
vault_manifest() { printf '%s/%s/manifest.json' "$(vault_dir)" "$1"; }
