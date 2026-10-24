# lib/stage-caddy.sh — Caddy staging shared by local and remote drivers.
# The target is Linux amd64, as with the existing tether/NATS sim profile.
# No downloaded executable runs on the driver; no cache bypasses verification.
sim_stage_caddy() {
    command -v python3 >/dev/null 2>&1 || {
        sim_stage_log "Caddy artifact staging requires host python3"
        return 1
    }
    # origin: hot-update S9 plan §9.4 / T55. The supervised production
    # staging path supplies the entire pair; the standalone Caddy verifier
    # remains available to the hermetic single-artifact controls.
    if [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
        . "$SIM_REPO/test/simcluster/lib/stage-infra.sh"
        sim_stage_infra || return 1
    else
        python3 "$SIM_REPO/test/simcluster/lib/caddy-artifact.py" stage "$SIM_REPO/scripts/install.sh" "$SIM_VENDOR" || return 1
    fi
}
