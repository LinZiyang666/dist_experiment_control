# S6–S8 external re-review round 8 tasklist

Boundary: only the post-round-7 fix for B3-2. B1/B2/B3-1 and earlier staged work remain closed context.
Pass if a service-account-controlled lock path cannot redirect root ownership changes, normal locking still
works, and no major platform or lifecycle regression is introduced.

## A. Claim and implementation

- [x] Map the developer reply to the exact open/stat/chown/flock ordering.
- [x] Verify symlinks and non-regular files are rejected before ownership mutation.
- [x] Verify clean creation, existing regular lock, contention, release, and ownership mirroring remain intact.

## B. Security and portability

- [x] Check TOCTOU/path assumptions and whether the fix matches the documented root/service-account boundary.
- [x] Check Linux and Darwin flag/build behavior and error handling.
- [x] Review analogous root-run offline write sites only to validate the developer's bounded claim.

## C. Tests and closure

- [x] Review developer regressions and rerun the external RED to prove it flips green.
- [x] Run focused Go/race tests, relevant cross-builds, and diff checks; no sim-cluster rerun for local open semantics.
- [x] Write a round-8 Pass/Fail report with doubts and evidence, complete this checklist, stage all files, and stop.
