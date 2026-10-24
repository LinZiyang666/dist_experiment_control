#!/bin/sh
# Sourced S9 fixture adapter. No side effects until an explicit verb.
# The caller owns topology/product commands; this adapter owns the original
# clients, fixed observer slots, custody and their shared-supervised lifetimes.
# origin: hot-update S9 plan §9.1 / §9.5 / §12 P32
#
# origin: S9 FIX-INFRAVERBS (r9d finding 5). The verbs are exactly the set that
# drills 85/86 and 39.NI/CI call: preflight prepare start check observe fault
# heal exercise transaction stage-config config-digest finish cleanup. Each one
# takes --evidence "$INFRA_EV" and answers with a native status (0, 2 for setup
# or custody; check/finish return the independent oracle's 0/1/2/4). Only the
# two long-lived jobs (the composer keeper and the observer scheduler) are
# spawned here, because they must belong to the drill shell; every other verb
# is hot_update_infra_verbs.py under the shared supervisor. A missing composer
# binding, S7 handoff or product status receipt is SETUP-RED with the pending
# owner named; nothing here compensates for the product or the composer.

infra_require() {
    INFRA_LIB=${INFRA_LIB:-${HERE:-}/drills/lib}
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] &&
        [ -r "$INFRA_LIB/hot_update_infra_verbs.py" ] && [ -r "$INFRA_LIB/hot_update_infra_workload.py" ] || {
        printf '%s\n' 'infra fixture: shared supervisor and fixture library are required' >&2
        return 2
    }
    case "$SIM_UPGRADE_SUPERVISOR:$INFRA_LIB" in /*:/*) ;; *) return 2 ;; esac
}

# All Python readers, CLI clients and foreground keepers enter the same
# subreaper/lease/pidfd implementation. Never evaluate a command string.
infra_exec() {
    infra_require || return 2
    "$SIM_UPGRADE_SUPERVISOR" -- "$@"
}

infra_verbs() {
    infra_exec "${PYTHON3:-python3}" -I -B "$INFRA_LIB/hot_update_infra_verbs.py" "$@"
}

# infra_spawn <label> <absolute executable> <argv...>
# Use this for the composer keeper and the observer scheduler. The returned
# supervisor job belongs to the drill shell. Native status is written only
# after descendant cleanup.
infra_spawn() {
    infra_require || return 2
    [ "$#" -ge 2 ] && [ -n "${INFRA_EV:-}" ] || return 2
    _infra_label=$1; shift
    case "$_infra_label" in ''|*[!a-zA-Z0-9-]*) return 2 ;; esac
    case "$1" in /*) ;; *) return 2 ;; esac
    [ -x "$1" ] && [ ! -e "$INFRA_EV/$_infra_label.rc" ] || return 2
    "$SIM_UPGRADE_SUPERVISOR" --log "$INFRA_EV/$_infra_label.out" \
        --receipt "$INFRA_EV/$_infra_label.rc" -- "$@" &
    INFRA_SPAWN_JOB=$!
}

# The evidence argument must be this drill's own private root; a verb aimed at
# another directory is refused before any child starts.
infra_evidence_arg() {
    _iea_value=
    while [ "$#" -gt 0 ]; do
        if [ "$1" = --evidence ]; then
            [ "$#" -ge 2 ] && [ -z "$_iea_value" ] || return 2
            _iea_value=$2; shift
        fi
        shift
    done
    [ -n "$_iea_value" ] && [ "$_iea_value" = "${INFRA_EV:-}" ] || return 2
    printf '%s\n' "$_iea_value"
}

# Bounded join of one supervised job. A job that outlives its stop request is
# terminated through its supervisor (which reaps the subtree) and still fails.
infra_join() {
    _ij_job=$1; _ij_rc=0; _ij_tries=0
    while kill -0 "$_ij_job" 2>/dev/null && [ "$_ij_tries" -lt 600 ]; do
        sleep 0.1; _ij_tries=$((_ij_tries + 1))
    done
    if kill -0 "$_ij_job" 2>/dev/null; then
        kill "$_ij_job" 2>/dev/null; _ij_rc=2
    fi
    wait "$_ij_job" || _ij_rc=2
    return "$_ij_rc"
}

infra_prepare() {
    _ifp_ev=$(infra_evidence_arg "$@") || return 2
    [ -z "${INFRA_COHORT_JOB:-}" ] || return 2
    infra_verbs prepare "$@" || return "$?"
    # The composer keeper is exec'd in place inside its supervised job, so its
    # identity, lease and native receipt are the job's own.
    infra_spawn cohort "${INFRA_PYTHON:-/usr/bin/python3}" -I -B "$INFRA_LIB/hot_update_infra_verbs.py" \
        cohort-exec --evidence "$_ifp_ev" || return "$?"
    INFRA_COHORT_JOB=$INFRA_SPAWN_JOB
    infra_verbs prepared --evidence "$_ifp_ev"
}

infra_start() {
    _ifs_ev=$(infra_evidence_arg "$@") || return 2
    [ -z "${INFRA_FIXTURE_JOB:-}" ] || return 2
    # Freezes the manifest: composer READY, whole hop schedule and pinned budgets.
    infra_verbs start "$@" || return "$?"
    infra_spawn fixture "${INFRA_PYTHON:-/usr/bin/python3}" -I -B "$INFRA_LIB/hot_update_infra_workload.py" \
        serve --manifest "$_ifs_ev/manifest.json" --evidence "$_ifs_ev/observer" \
        --supervisor "$SIM_UPGRADE_SUPERVISOR" || return "$?"
    INFRA_FIXTURE_JOB=$INFRA_SPAWN_JOB
    infra_exec "${PYTHON3:-python3}" -I -B "$INFRA_LIB/hot_update_infra_workload.py" \
        wait-start --manifest "$_ifs_ev/manifest.json" --evidence "$_ifs_ev/observer" || return "$?"
    infra_verbs started --evidence "$_ifs_ev"
}

# Product retirement is judged BEFORE this cleanup. Reaping a fixture or a
# deployment cgroup cannot be used as evidence that old product owners retired.
infra_cleanup() {
    _ifc_ev=$(infra_evidence_arg "$@") || return 2
    _ifc_rc=0
    infra_verbs stop --evidence "$_ifc_ev" || _ifc_rc=2
    if [ -n "${INFRA_FIXTURE_JOB:-}" ]; then
        infra_join "$INFRA_FIXTURE_JOB" || _ifc_rc=2
        INFRA_FIXTURE_JOB=
    fi
    if [ -n "${INFRA_COHORT_JOB:-}" ]; then
        infra_join "$INFRA_COHORT_JOB" || _ifc_rc=2
        INFRA_COHORT_JOB=
    fi
    infra_verbs closed --evidence "$_ifc_ev" || _ifc_rc=2
    return "$_ifc_rc"
}

infra_fixture() {
    infra_require || return 2
    [ "$#" -ge 1 ] || return 2
    _infra_verb=$1; shift
    case "$_infra_verb" in
        preflight|observe|fault|heal|exercise|stage-config|config-digest|check|finish)
            infra_evidence_arg "$@" >/dev/null || return 2
            infra_verbs "$_infra_verb" "$@"
            ;;
        transaction)
            infra_evidence_arg "$@" >/dev/null || return 2
            # The Tx comes from the product's own halted output and status receipt.
            infra_verbs transaction --sim "${SIM:?}" "$@"
            ;;
        prepare) infra_prepare "$@" ;;
        start) infra_start "$@" ;;
        cleanup) infra_cleanup "$@" ;;
        *) printf '%s\n' 'infra fixture: unknown verb' >&2; return 2 ;;
    esac
}
