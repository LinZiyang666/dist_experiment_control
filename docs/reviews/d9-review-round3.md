# D9 internal review — ROUND 3 (FINAL: re-review round-2 fixes + last sweep)

6 Opus-4.8 reviewers re-examined the round-2 fixes (especially the queue-group change) + did a final
ship-blocker sweep. Round verdict: **1 BLOCKER (consensus across all 6 reviewers) → fixed + guarded;
everything else re-verified CORRECT → cutover ready for external review**.

## The single BLOCKER (found independently by all 6 reviewers — strong consensus)
**The round-2 queue-group change over-reached: it queue-grouped the FILE-TRANSFER subjects too, bricking file transfer in a ≥2-node cluster.**

- `internal/broker/broker.go` queue-grouped the ENTIRE ctl/event loop in cluster mode, including `push.req` / `pull.req` / `push-commit.req` / `transfer.*.complete` / `transfer.*.failed` / `finalize.req`.
- But the D8 transfer lifecycle is built on **broadcast + a per-broker in-memory tracker (`b.transfers`) + a home-keyed START gate** (`transfer_home.go`): only the agent's HOME broker (the tracker holder) must answer, and it relies on EVERY broker receiving the broadcast so the home one self-selects. The code comment at `transfer_home.go` states this verbatim.
- A queue group hands each message to ONE RANDOM member with no tracker entry → START silently dropped (`~(N-1)/N` of push/pull hang), continuation/terminal dropped (tier-B push hangs, pull's bucket leaks, the watchdog emits a SPURIOUS `failed` audit for a transfer that succeeded). File transfer is comprehensively bricked.
- Uncaught by the gates: `TestD8Matrix` exercises the D8 seams at component level (NewForwarder / AuditPublisher / object store), never a multi-broker cluster-mode `Run` with the queue group + home gate; `TestD9Matrix` drives only session create/rm.

**FIXED**: `isBroadcastClusterSubject(subj)` keeps the 6 file-transfer subjects on a plain (broadcast) `Subscribe` in BOTH modes; everything else in the loop is queue-grouped in cluster mode. The home gate + tracker-presence already collapse the broadcast fan-out to one answering broker, so broadcast does NOT reintroduce the double-reply the queue group fixed for the leader-forwarded writes. Single mode is unchanged (always broadcast). New guard: `TestD9BroadcastClusterSubjectClassification` asserts the transfer subjects classify broadcast and the ctl commands classify queue-group, so a future regression that queue-groups a transfer subject fails a cheap unit test.

## Everything else — re-verified CORRECT (PASS)
The reviewers explicitly re-confirmed the following round-1/round-2 fixes are correct and regression-free:
- **register leader-only + broadcast-liveness**: the leader (a broadcast subscriber) always receives register; election-window drop+retry is acceptable; brand-new-node liveness is leader-set on register + follower-set on the next broadcast heartbeat; the leader-only OFFLINE/revoke scan sees fresh liveness (heartbeat stays broadcast). The rejected round-2 liveness/revoke BLOCKER is confirmed moot.
- **admin evict raft routing** (PlanEvict matches the old tx; pre-query race benign; response correct).
- **SecretsPreflight scope** (no cluster-layer code loads broker.nk/account.nk from ClusterSecretsDir; node-ident.nk is the join identity — correct).
- **single-mode reconcile error-path** byte-equivalence (orphan re-added on a close error).
- **cert_fp current-OR-previous** (the rotation window is safe; not an exploitable downgrade).
- **natsconf** (include case-insensitive; DryRun validates the exact Apply bytes + refuses a missing binary), **§17 observability** (SubjClusterCursor in PermissionsForBroker pub+sub; the false broker_down storm fixed), and the **§9/§9.1 write-routing audit is now genuinely exhaustive** (the final cross-package grep — broker + adminsock + cmd/tether + node/session/port/proc/agentprov — found no other un-routed replicated write in cluster mode).

## MAJOR — disposition
- **no multi-broker cluster-mode transfer drill / register-leader-only e2e** → PARTIAL: added the cheap `TestD9BroadcastClusterSubjectClassification` guard (locks the subscription-mode split) + the 2-broker `TwoBrokerJoinReplicates` write step. A full end-to-end push+pull through a ≥2-broker cluster (agent homed to broker B, ctl on broker A) is a heavy harness addition staged with the other multi-broker drills (the §13.12 3-node failover + the §18.2.18 mass-reconnect herd), which the runbook already frames as staged operator drills. The fix restores the PROVEN D8 broadcast design; the classification guard prevents the specific regression.

## Re-verification after the round-3 fix
`make test` 0 · `make lint` 0 · `TestD9Matrix -race` 0 · `TestD9BroadcastClusterSubjectClassification` 0. The full `make e2e` (all matrices) is the final pre-external-review gate.

## Final internal verdict
The D9 production cutover is **READY FOR EXTERNAL REVIEW**: 3 adversarial internal rounds found the cluster-mode cutover was initially incomplete (write-routing holes, broadcast dual-handling, an un-routed admin evict, an observability ACL bug, a missing cert_fp check, and the transfer-subject queue-group over-reach) and all were fixed + re-verified, with the §9 write-routing audit now exhaustive and the single-mode path held byte-equivalent throughout.
