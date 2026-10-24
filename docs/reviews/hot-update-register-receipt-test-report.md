Pass — bounded register/courier ACK ownership checks; full G.1 admission and hot-update completion are outside this result.

## Reviewed candidate and independent tests

Reviewed root's `registerReceipt`, `onRegisterSuccess`, `attempt`, per-attempt register snapshot creation, and removal of the two caller-level settlements. Production `proc_delivery.go` SHA256 at execution: `5c0d6c7ec83f5ba6533e07ca5937bcae44ff23aa7aa456c762c8af94a2aa7eb6`.

Added only `internal/agent/proc_register_receipt_test.go`, SHA256 `e0d6d4ca1a536e04bbb5066480aacfa3fa56e4a42c7c58a695ac58fa494e7bf2`. Five new top-level functions for root's inventory registration:

- `TestRegisterReceiptKeepsLateChildExit`: actual `onNATSReconnect` request is held; real exec child is created and exits 23 after the wire census; the old ACK is then released; real NATS proc.started/proc.exit delivery must preserve its minted task ID and rc=23.
- `TestRegisterReceiptConcurrentRepliesAreScoped`: two actual register requests with different captured exits, a third late exit, reverse ACK order, and a broker-write-failure accepted PID; positively clears the represented terminal entry while retaining the two unacknowledged ones.
- `TestRegisterReceiptRetryCarriesFreshExit`: real transient leader-unavailable reply triggers a new wire request; the new exit must appear and clear through that attempt's matching receipt.
- `TestRegisterReceiptDelayedACKKeepsReplacement`: register and courier arms both replace a same-key event while its old request is in flight, then require actual delivery of the new rc=29 rather than loss after the old rc=17 ACK.
- `TestRegisterReceiptRequiresExactExitRepresentation`: 11 missing/mismatched/non-success/contested/still-running controls; each is paired with the old-broker terminal positive where neither response list names the represented exit. No “never settle” implementation can satisfy these controls.

Embedded NATS, shell child, real request bodies and actual event delivery are real. Broker response scheduling is the test seam. No broker DB, Raft, real reconnect network fault, restored runtime or whole-fleet continuity was simulated.

## Executed

```sh
go test -race ./internal/agent -run '^TestRegisterReceipt' -count=3
```

PASS, 2.152s. Raw log: `/tmp/tether-control-recovery/proc-register-receipt-race.log`.

Negative control, production files unchanged: a temporary Go overlay restores whole-current-queue settlement while preserving the new method signature. The identical real late-child test fails specifically on the missing actual exit message, after its started message was delivered:

```sh
go test -race -overlay=/tmp/tether-control-recovery/proc-receipt-mutants/whole-queue.json ./internal/agent -run '^TestRegisterReceiptKeepsLateChildExit$' -count=1
```

Expected FAIL, 4.125s: `retained exit event did not reach NATS: nats: timeout`.
Raw log: `/tmp/tether-control-recovery/proc-receipt-mutants/whole-queue-negative.log`.

A second temporary overlay removing courier pointer identity is prepared at `proc-receipt-mutants/key-presence.json` but **NOT_RUN**: root began the real Caddy smoke and froze additional Go work. The fixed candidate's register/courier replacement arms did run three times above.

## Review conclusion and limits

No blocking error was found in the reviewed receipt-scoping delta. Tokens remain local to each actual attempt; replacement entries cannot be acknowledged by an older pointer; exact exit state/rc/end-time must have appeared on the wire; Lease/failed replies do not settle; current per-attempt refresh is exercised rather than assumed.

The started rule is conservative (`row.State == running`): it may retain a started entry when a terminal row is represented but the broker still lists that PID accepted after an exit write failure. This does not silently lose an event; full liveness/old-broker behavior still depends on the courier's existing retry policy and is not expanded by this report.

The separate stale-G.1 census/start-admission problem remains: these tests do not claim a stale request cannot cause a broker missed-exit transition while new work is admitted. Queue overflow/lifetime, persistent outbox, output replay and hot exec resource import also remain separate H1 work.

Only the new test file was edited by this lane. Root owns implementation, existing-test adaptations, inventory updates, integration and final gates. No staging, commit or deployment occurred.
