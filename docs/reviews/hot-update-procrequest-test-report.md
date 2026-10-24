Pass — bounded independent review of internal/procrequest only; not a full hot-update or reliable-exec feature verdict.

## Scope and changes

Added only `internal/procrequest/registry_test.go` and `registry_exec_test.go` (11 Tests). No production, gates, inventory, docs or deployment changes. Origin is `docs/reviews/hot-update-plan.md §4.2`. Read the wire decision, proto types, registry/state/boot-clock source and workload/handoff APIs. No confirmed production defect in this bounded review.

## Executed evidence

- `go test -race -count=3 ./internal/procrequest`: PASS, 4.558s, raw log `procrequest-registry-final.log`.
- Initial nine registry Tests: PASS 1.109s, `procrequest-registry-first.log`.
- Initial true same-PID exec Test: PASS 2.105s, `procrequest-exec-first.log`.
- `/tmp` Go overlay removing unknown-admission expiry check: expected FAIL, rc1, 0.038s; `registry_test.go:296` sees `fresh=true` after old terminal was settled, expired and collected. Log `procrequest-no-expiry-negative.log`.
- `/tmp` Go overlay removing terminal deep copy: expected FAIL, rc1, 0.038s; `registry_test.go:468` sees caller mutation change retained signal15 to1. Log `procrequest-terminal-alias-negative.log`.
- No production source was changed by either overlay. No full gates/lint/Docker/drill executed by this lane.

## Independent oracles

1. 32 simultaneous Reserve callers: only fresh winner performs real OS Start; exactly one child, one logical owner, lost initial response retry returns original owner, exact rc23 retained. No mock spawn counter substitutes for the OS child.
2. Signed lease field mutation, authenticated scope changes, all five effectful Exec fields, canonical environment map order, original PID transition checks, stale/same-epoch attachment checks, terminal and settlement idempotence.
3. Injected boot clock tests exact expiry boundaries without sleeping. Only settled+expired receipts collect; live/starting and unacknowledged terminal remain. Late retry never regains creation after GC; new LeaseID with old RequestID is explicitly a new request. Concurrent Collect versus32 late Reserve attempts also grants zero new creations.
4. Capacity rejects before side effects and keeps unexpired settled receipts. Starting blocks Freeze; failed Freeze does not strand completion. Frozen owner rejects every mutation and GC; restored owner requires explicit Thaw.
5. Restore rejects18 version/identity/key/quota/duplicate/phase/terminal/attachment errors, including valid-first/bad-last records. Failed restore returns no partially installed owner and original retained records remain unchanged. Valid empty exported state is accepted. Secret, records and terminal copies are detached.
6. Actual same-PID exec retains signing authority, original logical request and original child PID/startticks/PPID. The original inherited input pipe resumes that child and produces rc23. A distinct child genuinely exited17 before exec and its terminal survives. A cold fresh instance/key rejects old handles; this cold check is in-process New, not a boot/reboot drill.
7. Five explicit lifecycle rounds exercise Issue/Reserve/Running/Complete/Settle/Freeze/Restore/Thaw/Collect, assert empty terminal table, builtin goroutine and FD checks, FD tolerance0.

## Registry contract boundaries retained for integration

- Registry receives an already-authorized Scope. Tests do not prove broker JWT/member/ACTIVE checks, inbox authorization, revocation, leader/follower routing, N-1 failclosed, ctl persistence or canonical spec actually reaching every spawn implementation.
- `bool=true` is the internal caller's one creation grant; external agent integration must never Start on false/error. Full same-PID joint workload/stream/courier/registry snapshot is not activated here.
- Restore trusts the authenticated private handoff bundle and validates current boot; owner PID/startticks and cross-domain completeness belong to the outer handoff/runtime fence.
- Lost terminal-ack response after successful Settle, followed by lease expiry+GC, yields ErrExpired on a retransmission. The ctl already holding all output and terminal needs explicit completion semantics for that case; it must not issue a replacement create or wait forever. Reported to root as an integration boundary, not a registry bug.
- Process namespace reboot, real cold process startup, backward wall-clock changes, multiversion binary compatibility, crash recovery,500ms freeze and5s control availability require their own integration/deploy-tier gates. Primitive passing results do not supply those receipts.

## Test inventory and leak anchors

`registry_test.go`:
- 76 TestRegistryConcurrentReservationStartsOneOriginalChild
- 169 TestRegistryRejectsChangedLeaseScopeAndIntent
- 262 TestRegistryExpiryAndCollectionNeverRecreateOriginalRequest
- 342 TestRegistryCapacityAndFreezeProtectAdmittedResponsibilities
- 402 TestRegistryAttachmentAndTerminalTransitionsRemainIdempotent
- 500 TestRegistryRestoreRejectsPartialAndIncompatibleState
- 557 TestRegistryRestoreOwnsCopiesAndRequiresExplicitActivation
- 611 TestRegistryClockFailureCannotAdmitOrDiscardResponsibilities
- 643 TestRegistryConcurrentCollectionCannotReopenExpiredIdentity
- 689 TestRegistryRepeatedLifecycleReleasesResources: literal five rounds691, builtin g694 / FD695, exerciseRegistryLifecycle698.
`registry_exec_test.go:46`: TestRegistryExecRetainsCreationIdentityAndOriginalChild.

## SHA-256 of tested source

- registry_test.go: 19faa65f5e528421c355028862ff5b57149856b71c26ecf1b4b986bcaddc34dd
- registry_exec_test.go: d33fe8c850b73550126075d93969680b5ab7e60b4c59284945aab615d3663938
- registry.go: 632a09520c6461b9b6898cc97584fab4326124c30b8fff958adad3d590468955
- state.go: b9780fa30e1582ab8c8ee06110d1a254d0abeec05521856d2ccb614bf64645ae
- clock_linux.go: e448352b89bc875668068471734c51cd5e82cb5bd3f70f693a71bba5518fc245
- proto/process_resume.go: 12af128688793bdce257725fcd151a6510bcab831bf244b1144acbe3de2712e7
