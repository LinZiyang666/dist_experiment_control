Fail — runtime failure-path review, initial candidate

Scope: only `internal/execsession/runtime.go` failure, cancellation and resource ownership. Production was read only. `runtime_failure_test.go` is this lane's exclusive test file. No claim about the unfinished freeze/import or full hot-update feature. No production, inventory or gate edits.

## Confirmed findings

### F1 — Medium: canceled Close leaves a late failed startup owner and replay files

- Baseline `runtime.go:173,187–189,234–247,381–389`.
- Schedule: create allocates all three real replay memfds; Start blocks before child creation; Close with canceled context marks runtime closed but returns; Start is then released with an error; task.done closes. No caller invokes a second Close.
- Observed: completed closed runtime still has one task; both stdout/stderr Replay.Read return EOF instead of os.ErrClosed; a full replay reservation fails with ErrBackpressure. The same test's cleanup performs an explicit second Close only after assertions, to avoid leaking test resources.
- Cause: failStart retains final output logs for settlement even though runtime is closed; the only close/delete pass was abandoned by canceled Close. The documented eventual owner is not implemented on this path.
- Recommendation: the startup/observer owner must perform final closed-runtime resource close/delete before publishing completion, independent of whether a Close caller remains waiting. Preserve normal terminal replay when runtime is open.
- Evidence: `TestRuntimeCloseCanceledStartupReclaimsLateFailure`, reproducible red in both raw logs.

### F2 — Medium: pump initialization failure destroys terminal replay before protocol settlement

- Baseline `runtime.go:195–216,325–329,354–367`.
- Fixture uses a valid configured replay capacity of 8 bytes and a 16-byte startup warning. A real `/bin/sh` child starts with explicit pipes and is adopted by Manager.AdoptDraining; stdout Output is created; stderr OutputWithPrefix rejects the warning. This is a bounded local failure injection, not a claim that ordinary default-capacity warnings routinely exceed capacity.
- Observed: original child is killed and sole-owner reaped, Manager record retired; registry stores terminal with code -1 and signal 9. Resume nevertheless returns `file already closed` from Ack of the closed output replay, without a usable output response. Subsequent settlement cannot complete.
- Cause: cleanup uses closeTask for both physical process/pipe teardown and durable terminal-output ownership. Those lifetimes differ when a failed attempt still needs delivery and ACK.
- Recommendation: retain final/error output state until client acknowledgement, close only the failed physical actors/stdio when killing the child, and define explicit exceptional stream completion/acknowledgement. Do not fabricate successful EOF for a stream interrupted before true source EOF. Existing protocol says Error never fabricates successful EOF and requires FIN for settlement, so any error-terminal acknowledgement change must be coherent in agent and ctl.
- Evidence: `TestRuntimePumpSetupFailureRetainsObservableTerminal`, reproducible red with real child and Manager reaping.

## Passing controls

- `TestRuntimePartialAllocationFailureSettlesAndReleasesBudget`: stdin and stdout memfds allocate; stderr reservation fails. Start never runs, both empty outputs are real final streams, resume returns failure terminal, terminal ACK settles, task is removed and full budget is reusable.
- `TestRuntimeCloseReapsChildWithUnacknowledgedFullOutput`: actual child emits >8 bytes; replay reaches capacity with no ACK and Manager retains the original leader. Close kills/reaps original child, removes manager/runtime ownership and frees the complete reservation without requiring a client to drain.

## Raw execution

- `execsession-failure-before.log`: `go test -race ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget)$' -count=1 -v`; raw rc 1, FAIL 0.062s (two red, one green).
- `execsession-failure-confirmed.log`: same pattern plus `CloseReapsChildWithUnacknowledgedFullOutput`; raw rc 1, FAIL 0.095s (two red, two green). Before the second run the budget assertion was made contract-clean by setting its limit before first use; no production change. This does not replace the first receipt.
- No race report. No subprocess remains from successful completed test cleanup. No Docker, full gate, lint or external service was started for this review.

## Candidate hashes

```json
{
  "internal/execsession/runtime.go": "b25f9b7481f4cea5f9aadc72b436c646376f1e69e6459fb93e7d25183fcf3943",
  "internal/execsession/runtime_failure_test.go": "4e4dbd2d34beb22a981c9a647ba25025da071366351c73d3353035f95196d932",
  "/tmp/tether-control-recovery/execsession-failure-before.log": "b689cc90c7cb49db581947a5b30a6d747f4dfbc51b09cf288985754ee1613147",
  "/tmp/tether-control-recovery/execsession-failure-confirmed.log": "73eee4a211c8d6e869f1ed41fe525c95f5f677c9900182e0f31f41be35567123"
}
```

Test packaging after these runs: added `//go:build linux` to align with Replay memfd support; assertions and fixture logic unchanged. Current test SHA256: `345dc35bee4c9ea600d411135c9b3d67b2ef2dade615bda6c4479ae9a03e3abb`.
