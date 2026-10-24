# lib/stage-infra.sh — verified S9 adjacent artifacts, shared by both drivers.
# Sourced, never executed. All builds/downloads remain under FIX-ORPHAN.
# origin: hot-update S9 plan §3.1 / §6.5 / §9.4 / T55
sim_stage_infra() (
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        sim_stage_log "infra supply requires the shared process supervisor"
        exit 2
    }
    export SIM_REPO SIM_VENDOR SIM_INFRA_PREVIOUS_REF
    # origin: hot-update S9 plan §9.1 / §9.4 / S9 COMPOSER. Validate the
    # accepted workload-owner handoff before expensive builds or downloads.
    "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B \
        "$SIM_REPO/test/simcluster/lib/infra-composer.py" validate \
        --supply "${SIM_INFRA_COMPOSER_SUPPLY:-}" || exit 2
    "$SIM_UPGRADE_SUPERVISOR" -- python3 \
        "$SIM_REPO/test/simcluster/lib/infra-artifact.py" stage \
        "$SIM_REPO" "$SIM_VENDOR" --previous-ref "${SIM_INFRA_PREVIOUS_REF:-}" || exit 2
    exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B \
        "$SIM_REPO/test/simcluster/lib/infra-composer.py" stage \
        --supply "${SIM_INFRA_COMPOSER_SUPPLY:-}" --vendor "$SIM_VENDOR"
)

# Missing files under --skip-download are setup failure, never a cache hit.
sim_verify_infra() {
    python3 "$SIM_REPO/test/simcluster/lib/infra-artifact.py" verify \
        "$SIM_VENDOR" "$SIM_REPO/scripts/install.sh" || return 2
    python3 -I -B "$SIM_REPO/test/simcluster/lib/infra-composer.py" verify --vendor "$SIM_VENDOR"
}
