Pass — scoped Input actor validation and read-only Replay/Output cross review; complete hot update remains unproven.

## Scope and authority

Only `input_test.go` and `input_exec_test.go` were authored by this lane. Production files, existing tests, inventories, vendor and deployment were not modified. This is internal component review, not the independent external review or the real H0 drill verdict.

## Evidence

- `go test -race ./internal/workload -run '^TestInput' -count=3 -timeout=90s`: **PASS**, 10 Test functions. Original log: `/tmp/tether-control-recovery/hot-update-input-final.log`.
- Actual result: `ok  	github.com/LinZiyang666/tether/internal/workload	4.439s`.
- Real 4 KiB kernel pipe fills during a 32 KiB write. Freeze interrupts that write and exports `Base=4096`, `End=65536`, matching `TIOCINQ`; the retained suffix matches source bytes. Thaw delivers the complete 64 KiB to the original reader with exactly one terminal EOF and no byte loss/duplication.
- Same-PID exec uses real `handoff.Prepare/Exec/Restore`. The original external reader remains open outside the replaced owner. Original writer inode and owner PID must match after exec, and the external reader compares all 64 KiB, not an exported source ledger. Its reader starts only after the new image receipt, so pre-exec backpressure and partial writing are real.
- Already delivered EOF imports without any target FD; no new writer delays the original EOF. Pre-canceled and wait-select cancellation both leave the original pump usable.
- Invalid metadata, missing destination, wrong pipe direction, missing CLOEXEC and blocking flags reject without stealing or modifying caller resources. Actual duplex socketpair is rejected by pipe-only input and remains usable in both directions.
- Closed receiving pipe retains accepted-but-undelivered bytes and reports WriteError; no fake delivery or EOF is manufactured.
- Warmup plus five repeated actual pipe/freeze/thaw/teardown cycles use the repository goroutine and FD gates (FD tolerance 0).

## Negative control

`/tmp/tether-control-recovery/input-partial-ack-overlay.json` changes only `Ack(Base + written)` to `Ack(Base + n)` in a temporary copy. No production file was edited. Original test and expected values were unchanged. `-race -run '^TestInputFreezeRetainsPartialWriteAndOriginalEOF$'` returned **FAIL**, package time 0.036s: exported Base became 32768 while the real pipe held 4096. Original stderr/stdout is `/tmp/tether-control-recovery/hot-update-input-partial-ack-negative.log`; exact pre-mutation source SHA and mutation description are in `input-partial-ack-negative-source.json`.

## Cross review of Replay / Output

No additional confirmed blocker found in current scope. Independent review verified bounded full-capacity reservation, linear offsets across physical wrap, actual writable/size-sealed memfd validation, no ACK on publish, frozen import, actual n>0 retention before pause, bounded exceptional Pending, and separation of FIN from source failure. The other lanes' tests independently compare original streams, retained bytes and actual exec identity; they do not replace observed output with source data. Root's wrong-direction/FD flags guard and the pipe-only Input contract were independently exercised here.

Ownership remains explicit: consumers may only use the documented replay operations; arbitrary external Close/Freeze or duplicated owners both Thawing are outside this primitive's contract. The runtime must enforce one owner and global intake/freeze order.

## Remaining boundaries

- No claim that actor tests satisfy the entire 500 ms freeze budget. `control`, actor, and replay mutex acquisition is not context-interruptible; the future coordinator must measure old-pump-stop through exec/Go-init/import/new-pump-ready and handle an over-budget safe point without destroying original resources.
- No NATS/ctl reliability, request deduplication, application bootstrap rollback, PTY EIO/terminal EOF, TCP half-close, authorization migration or crash-durable exactly-once claim.
- The real frozen H0 image remains independently **PRODUCT-RED rc3**. These later component tests do not change that historical runner result or prove these sources were inside its image.
- Tests are now frozen as requested; root owns inventory registration and final gates.

## Function inventory

- `TestInputFreezeRetainsPartialWriteAndOriginalEOF`
- `TestInputCanceledFreezeKeepsOriginalWriterUsable`
- `TestInputRestoreDeliveredEOFHasNoWriter`
- `TestInputRejectsWrongDirectionWithoutTakingOwnership`
- `TestInputRestoreRejectsInvalidStateWithoutClosingFiles`
- `TestInputDescriptorFlagsRejectedWithoutMutation`
- `TestInputRejectsSocketWithoutClosingPeer`
- `TestInputBrokenPipeRetainsUndeliveredSuffixWithoutEOF`
- `TestInputRepeatedLifecycleReleasesResources`
- `TestInputExecPreservesOriginalPipeWithoutLossOrDuplicate`

## Reviewed source/test SHA256

- `internal/workload/input.go`: `0ef8708f8ae1f43bffc6ce0c5a259ee9c3b80c0a30e208af496eb0f2f6411d44`
- `internal/workload/output.go`: `ba91dab927f788f51923e56147294e8c0f0dd1ec3663e5aeda3852a801039187`
- `internal/workload/replay.go`: `ace564d8bc652c8dce0550a5caf0d04900052cb40b6c673abccd2fe19545c495`
- `internal/workload/replay_linux.go`: `9b5cd66cba6487daa5f99ecc7077e53b54620b41df79dda5fbfe45b7f193c06a`
- `internal/workload/stdio.go`: `a8eb7466d02fff07cc07bb5512a6632a75ed9c6b85af7183047c25e2290cfe44`
- `internal/workload/input_test.go`: `a685414fb521ac7169eede9697ca3b006a22ca3a756c904e3459ee483f067c92`
- `internal/workload/input_exec_test.go`: `e3ea206a470caf6ccb6be8235eb3d34c32e28e7f14b648de5e2fc282bf889f48`

Post-test lint-only correction: inferred `ctx := base` replaces the redundant explicit `context.Context` type (QF1011); behavior and assertions unchanged, no repeat suite requested. Test file hash above reflects this correction.
