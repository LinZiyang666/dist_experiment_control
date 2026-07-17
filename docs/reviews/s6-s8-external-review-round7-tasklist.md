# S6–S8 external re-review round 7 tasklist

Boundary: only the post-round-6 unstaged changes intended to close B3-1. Earlier staged B1/B2/B3
work is context and is not reopened. Pass when the documented root-run offline path cannot leave an
unopenable lock for the daemon and no equivalent major regression is introduced.

## A. Change boundary and developer claim

- [x] Freeze the round-7 delta and map the developer response to production call sites.
- [x] Verify ForceSingle, Resnapshot, Recover, InitFromExisting, and RestoreFromBackup all use the shared lock.
- [x] Verify the private unsafe helper is removed and no alternate production acquisition path remains.

## B. Lock correctness

- [x] Trace creation, ownership mirroring, permission errors, contention, release, and daemon reacquisition.
- [x] Confirm the shared helper still provides non-blocking daemon/offline mutual exclusion without self-deadlock.
- [x] Check platform/build implications and failure behavior; distinguish blockers from stale comments or hardening.

## C. Regression quality and verification

- [x] Review the new source guard and real-entry regression for vacuity and mutation sensitivity.
- [x] Run focused unit tests and race tests for clusteroffline/cluster/broker.
- [x] Run diff checks and relevant Linux/Darwin builds; do not repeat sim-cluster unless local evidence is insufficient.

## D. Closure

- [x] Write a round-7 report beginning with `Pass` or `Fail`, including disposition, doubts, and evidence.
- [x] Complete this checklist, stage every file, verify no unstaged/untracked residue, and stop.
