# D9 internal review — ROUND 2 (re-review the round-1 fixes + new issues)

6 Opus-4.8 reviewers re-examined the round-1 fixes for correctness/regressions + hunted what round 1
missed. Round verdict: **2 BLOCKERs found (1 real → fixed, 1 rejected) + MAJORs fixed → re-verified green**.

## Verdicts (as filed)
- write-routing — CONDITIONAL: round-1 fixes mechanically correct + §9 clean; found the liveness/revoke + dead-reconcile concerns + the untested forward path.
- cutover/detection/shutdown — PASS-with-minors: fixes 1/4/5 correct.
- migration/secrets — PASS-with-findings: probe + seed guard correct; the secrets-dir-vs-auth-callout-dir mismatch.
- natsconf — CONDITIONAL: DryRun/lowerKeys correct; include case + ClientListen.
- observability/cert_fp — PASS: the four focus fixes correct.
- red team — FAIL: the `admin evict` un-routed write + the broadcast dual-handling.

## BLOCKERs

| Finding | Disposition |
|---|---|
| **`tether admin evict` is an UN-ROUTED replicated write** (adminsock `handleEvict` does `s.backend.DB.Begin()`+DELETE on the RODB handle in cluster mode → "readonly database"; the §9 grep was scoped to `internal/broker` so it missed `internal/adminsock`. PlanEvict/OpNodeEvict existed since D2, never wired) | **ADOPTED + FIXED**: `VerbNodeEvict`/`evictNode` (Propose `PlanEvict` / forward); `Backend.EvictWrite` seam wired in cluster mode; `handleEvict` pre-queries existence then routes. Single mode keeps the direct tx. |
| **liveness/port-revoke: leader's stale view revokes a live agent on a follower** | **REJECTED (premise wrong)**: register AND heartbeat are BROADCAST (`nc.Subscribe`, broker.go:605/615), so EVERY broker — including the leader — handles every heartbeat and updates its OWN local liveness. The leader's view is therefore NOT stale for an agent "on a follower" (the leader handled the same broadcast heartbeat). The leader-only OFFLINE/revoke scan reads the leader's fresh view. No false revocation. Documented the broadcast-liveness model in the §3.5 notes. |

## The systemic issue the round-2 forward-path test surfaced (BLOCKER-class, FIXED)
The 2-broker forward-path test (added per the coverage MAJOR) immediately failed: `session.create` returned a spurious **"already exists"**. Root cause: the ctl-command + event subscriptions were `nc.Subscribe` (BROADCAST), so in a ≥2-node cluster EVERY broker handled each command — the followers' `proposeOrForward` all forwarded to the leader, and a follower's reply (already-exists, since the leader's own broadcast handling already committed) raced ahead of the leader's `ok`. **FIXED**: the ctl-command/event loop now uses a shared **queue group** (`tether-broker-ctl`) in cluster mode so exactly ONE broker handles each message (it forwards/proposes through raft); single mode keeps a plain `Subscribe` (a 1-member queue group is behaviorally identical → byte-equivalent). `register` is handled separately and is LEADER-ONLY in cluster mode (the leader's RODB is authoritative for the G.1 reconcile directives; a follower returns silently; heartbeat stays broadcast for liveness). This also kills the register/proc-event N× audit.

## MAJORs — dispositions
- **dead D4 reconcile path / re-derivable-audit claim** → ADOPTED (partial): register is now leader-only, so `reconcileOnRegister` runs ONCE (on the leader) — no N× audit, directives computed from the leader's authoritative RODB. The reconcile audit is published LIVE by the leader (single-writer at any instant), consistent with proc start/exit audit; the `VerbReconcile`/`PlanReconcileBatch` re-derivable path is retained (D4 mechanism, used by the forward wire) but not the live register path. d9-review §9.1 updated to stop implying reconcile audit is re-derived in the live cluster path.
- **forward-path untested** → ADOPTED: added the 2-broker `TwoBrokerJoinReplicates` write step (session.create through a follower-present cluster commits + the second create is rejected) + `SessionRmCascadeRoutesThroughRaft`.
- **SecretsPreflight checks the wrong dir** → ADOPTED: removed `broker.nk`/`account.nk` from the cluster `requiredSecrets` (they belong to the auth_callout config, validated by that layer); kept `node-ident.nk` (the cluster join identity). Tests updated.
- **`hasIncludeDirective` case-sensitive** → ADOPTED: lowercased the prefix match (an uppercase `Include` no longer bypasses the include guard).
- **single-mode reconcile error-path divergence** → ADOPTED: the PID-reuse arm re-adds the orphan to `agentByPID` even on a close error (preserving the pre-D9 schedule-the-kill behavior); only the audit is gated on a committed transition.
- **ClientListen host+port** → already required both (returns "" otherwise → the takeover CLI guard refuses); confirmed.

## MINORs
- tombstone not_leader→store_error UX on a follower: with the queue-group fix a follower forwards transparently (no not_leader unless there is genuinely no leader); acceptable (the ctl retries).

## Re-verification after round-2 fixes
`make test` 0 · `make lint` 0 · `TestD9Matrix -race` 0 (incl. the new 2-broker forward-path write) · `TestD8Matrix -race` 0 (queue-group change introduced no regression in the clustered transfer/alert suite).
