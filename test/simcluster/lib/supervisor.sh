# lib/supervisor.sh — the simcluster adapter to the shared FIX-ORPHAN process supervisor. POSIX sh.
# Sourced, never executed. No side effects at source time.
#
# WHY IT EXISTS: the S8 supply path (lib/stage.sh sim_stage_binaries, lib/upgrade-artifacts.sh and the
# two hermetic supply tests) refuses to start without SIM_UPGRADE_SUPERVISOR — an executable that takes
# `-- argv...`, keeps the caller's stdio/cwd/env, returns the child's native status and owns every
# descendant through timeout/panic/SIGKILL. Refusing is right (long Go builds otherwise leave orphans),
# but nothing supplied the executable, so the hermetic gate set went red on "shared process supervisor
# is required" the day those scripts landed. This file is the one place that supplies it.
#
# The adapter is the repository's own test launcher, test/fullstackharness/cmd/supervise: a thin main
# over internal/testharness.StartProcess (the FIX-ORPHAN subreaper + CLOEXEC lease) with an owner
# pidfd deathwatch. Its exit status is the child's (128+signal for a signal); 125 means descendants
# survived the child and were reaped, 126 a launcher setup failure — neither is ever success. It is
# built from the caller's own tree, so the supervisor that judges a candidate is that candidate's.
# Linux only, like the subreaper it wraps; elsewhere the build succeeds but every launch refuses.
# origin: hot-update S8 plan §11.2 P22 request (simcluster-side adapter); FIX-ORPHAN (S6 plan §9.3).

# sim_supervisor_build <repo> <dir> : build the adapter into <dir>/supervise. Build output goes to
# stderr; on success the adapter path is printed on stdout.
sim_supervisor_build() {
    _ssb_repo=$1; _ssb_dir=$2
    [ -f "$_ssb_repo/go.mod" ] && [ -d "$_ssb_repo/test/fullstackharness/cmd/supervise" ] || {
        printf 'shared supervisor: %s is not a tether tree with the FIX-ORPHAN launcher\n' "$_ssb_repo" >&2
        return 1
    }
    command -v go >/dev/null 2>&1 || { printf 'shared supervisor: no Go toolchain on PATH\n' >&2; return 1; }
    ( cd "$_ssb_repo" && go build -o "$_ssb_dir/supervise" ./test/fullstackharness/cmd/supervise ) >&2 || return 1
    printf '%s\n' "$_ssb_dir/supervise"
}

# sim_supervisor_provision <dir> : for the drivers' --build. Keep a caller's exported adapter; otherwise
# build one from $SIM_REPO into <dir> (the caller owns and removes <dir>) and export it.
sim_supervisor_provision() {
    [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ] || return 0
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "${SIM_REPO:?}" "$1") || return 1
    export SIM_UPGRADE_SUPERVISOR
}
