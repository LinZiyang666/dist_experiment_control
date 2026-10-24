Fail — this Runtime snapshot still has two confirmed failure-path findings assigned to the tunnel lane/root. The four independent successful-path tests pass; this is not a full hot-update review.

## Owned change and evidence

Only `internal/execsession/runtime_test.go` was added. No production, agent adapter, proto, gate or deployment changes by this lane.

- Initial four Tests: `-race -count=1`, PASS4.249s, `execsession-runtime-first.log`.
- Strengthened exact physical Start and lifecycle callback counts: four Tests `-race -count=3`, PASS10.743s, `execsession-runtime-final.log`.
- `/tmp` overlay releases stdout ACK before validating stderr ACK: expected FAIL, rc1,0.062s, `execsession-ack-partial-negative.log`; public resume then reports the incorrectly discarded stdout prefix at runtime_test.go:410. No production source mutation.
- Final subsequent test-only change improves a failure diagnostic: prints counts instead of reflecting live os.File pointers. No oracle/runtime change.

## Test inventory and exact oracles

- runtime_test.go:269 `TestRuntimeDuplicateCreateKeepsOneProcessAndExactStreams`: first creation response discarded;16 concurrent retries retain one logical PID. Counter increments immediately after every real cmd.Start, before Adopt can reject duplicate side effects. Exactly one physical Start, one Started and one Exited. Actual48KiB binary stdin is consumed through cat, exact stdout plus tail and independent stderr are retained, FINs and rc23 agree.
- runtime_test.go:311 `TestRuntimeDisconnectedClientResumesOriginalBackpressuredPipes`: original256KiB binary input exceeds pipe/replay capacities; a1KiB output replay fills without ACK, original Input remains backpressured, original child still exists. New attachment epoch2 resumes the same task. Lost first replay response is retried byte-for-byte with no implicit ACK. Exact full output/stderr, original OS PID and original stdout/stderr pipe inodes remain through terminal, before settlement closes them.
- runtime_test.go:370 `TestRuntimeRejectsCrossActorStaleAttachmentAndInvalidACK`: current-scope actor/SID/NID/lineage isolation, stale attachment rejection, one legal stdout prefix ACK paired with impossible stderr ACK leaves both original prefixes intact. Early/no-FIN and wrong-terminal ACK cannot settle; valid terminal ACK and its retry converge.
- runtime_test.go:430 `TestRuntimeRepeatedExecutionReleasesResources`: literal five rounds432, real command/input/two output streams/terminal/settlement/Close each round, exact lifecycle receipts, empty process and task tables; builtin goroutine435 and FD436 assertions, FD tolerance0; helper `exerciseRuntimeExecution`439.

## Cross review and limits

Tunnel lane independently read all four tests and accepted the physical Start counter, original inode oracle, original binary input ledger, ACK atomicity and resource checks. This lane read its failure source and agrees with two confirmed failures: canceled Close followed by late Start error leaves output replay resources, and post-Start pump setup failure closes replay before terminal can be observed/settled. Fixes and regression receipt belong to root/tunnel; positive-stream tests do not close those findings.

The fixture runs the actual Registry/Runtime/Manager/Input/Output/Replay and real exec.Cmd with explicit workload.NewStdio/AdoptDraining. Lifecycle callbacks retain real receipts locally. It does not run NATS, real broker authorization, agent courier, remote-FS spawn lease, wire routing/N-1, real ctl writes or deployment. Client disconnection means no Handle/ACK calls, not a real socket failure. No runtime freeze/export/import is implemented or tested here. No claim of500ms freeze,5s availability, complete upgrade, rollback or drill success.

## Snapshot

- runtime_test.go final diagnostic-only SHA256: a32ec72464b0f758223006bfb1b4edc7dbc4f762da96cc780e6fb25d6ac36642.
- runtime.go tested SHA256: b25f9b7481f4cea5f9aadc72b436c646376f1e69e6459fb93e7d25183fcf3943.
