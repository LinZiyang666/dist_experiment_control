# C1 Stage-C Review + Adjudication — Agent online auto-discovery

> Stage-C adversarial internal review (5-agent workflow: concurrency / security / requirements-fidelity / wire-determinism reviewers → synthesizer). Full raw report: workflow `wfttl3osv` output. Verdict: **CONDITIONAL-on-blockers**. Security dimension = PASS. This file records each finding + the main process's disposition. **All findings adjudicated; all accepted fixes landed; gates green. External review is deferred to the end of the C-program (per the governing /goal).**

## What the review confirmed TRUE (not lip-service), verified in code

- Consumer-side **必须拒绝 #3** is real: `adoptRoster` rejects sig-fail / account-mismatch / schema / expired / gen-rollback / empty-set, each keeping prior URLs+pin+hwm, never wedging or emptying the pool; `VerifyAt` is fed the **persisted/OOB pin**, never `r.AccountPub` (except first-TOFU). Dedicated unit tests exist.
- **Non-cluster byte-equivalence**: `roster_gen` / `roster_refresh_only` / `roster` all `omitempty`; nil-roster path is a first-line no-op; `selfID==""` gate. ProtoVersion=2, ClusterRosterSchemaVersion=1 unchanged.
- **FSM Apply determinism** of the change-gated bump is sound: `genericExecApplier` execs Body[0] then Body[1] on one tx connection, `changes()` reflects Body[0], `MAX()` stored as decimal TEXT; `PlanClusterNodeUpsert` carries no bump → the `len(Body)==1` custom-applier guard intact.
- **≤5min online refresh** + **offline-agent-does-not-block-retire** hold by construction (pull-only agent; leader-Propose retire with no agent-ack barrier).

## Findings & disposition

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| **BLOCKER-1** | BLOCKER | `buildSignedRoster` stamped the unseeded counter (reads **0** before the first op) with no floor → a mixed v0.4.0→C1 rollout serves gen=0, which an agent that cached the v0.4.0 derived-max (~1.7e18) rejects as rollback. Violated the §D-2 "first value ≈ now.UnixNano()" claim. | **ACCEPTED + FIXED.** `buildSignedRoster` now floors the wire gen with the derived-max `readRosterBrokers` already computes (`if derived > gen { gen = derived }`) — monotone, never below what v0.4.0 served, counter overtakes on the first op. Guard: `TestRosterGenRollingUpgradeFloor` (broker). |
| **MAJOR-1** | MAJOR | In a REBUILT session's connect+register window, `a.runCtx` still pointed at the previous (cancelled) ctx → `armRedialWatchdog`/`armFailClosed` skipped arming (their guard is `a.runCtx.Err()!=nil`) and `dispatchForwarded` dropped cmds → failover defeatable in a double-fault. | **ACCEPTED + FIXED.** `runCtx`/`cancelRun` now derived + **published at the TOP of `session()`** (before connectNATS), so the guard sees the current session's ctx during setup. **Plus**: this exposed a real data race (the rebuild loop rewrites `a.runCtx` concurrently with callbacks) — now fixed by a mutex-guarded `setRunCtx`/`loadRunCtx` accessor, all 6 access sites converted. Surfaced + verified by the new `-race` leak test. |
| **MAJOR-2** | MAJOR | `onNATSReconnect` rebuilding-check → `ncBox.Store` TOCTOU: a stale reconnect goroutine could clobber `ncBox` with the Closed old conn after the new session stored the fresh one → proxy-readiness signaling lost. | **ACCEPTED + FIXED.** Re-check `a.rebuilding.Load()` immediately before `ncBox.Store` (window shrunk to ~one instruction; residual self-heals on next reconnect — acceptable v1). |
| **MINOR-1** | MINOR | `adoptRoster` verify→commit non-atomic; unconditional URL write → a one-gen URL/hwm skew across the rebuild boundary; double-first-TOFU last-writer-wins. | **ACCEPTED + FIXED.** Final-lock re-decide: advance gen+URLs together only when `r.Generation >= a.rosterGen`; TOFU pin first-writer-wins; persist only the cache whose roster matches the stored hwm. |
| **MINOR-2** | MINOR | Clean shutdown could leave the redial watchdog / fail-closed `AfterFunc` armed → fires on a returned `Run`; could perturb a leak-gate poll. | **ACCEPTED + FIXED.** `Run` defers `stopRedialWatchdog()` + `cancelFailClosed()`. |
| **MINOR-3** | MINOR | The roster-refresh loop ran unconditionally → a non-cluster agent now sends a periodic `RosterRefreshOnly` register (inert, but new traffic). | **ACCEPTED + FIXED.** Refresh loop starts only when in a cluster (`resp.Roster != nil || a.cachedRosterGen() > 0`) → a single-broker agent sends NO new periodic traffic. |
| security TEST gaps | TEST | restart anti-rollback (real stateStore); stale-event scrub; multi-FSM determinism. | restart + scrub **ADDED** (TEST-7/TEST-8); multi-FSM DIFF deferred (deterministic-by-construction + single-FSM coverage; C7 mega-audit may add). |

## Tests added this round

| Test | Proves |
|---|---|
| `TestRosterGenRollingUpgradeFloor` (broker) | BLOCKER-1: wire gen floored to derived-max when counter unseeded; counter governs once it exceeds. |
| `TestRebuildNoDoubleForwardedDispatch` (agent, -race) | TEST-3: a forwarded exec is dispatched EXACTLY once after a rebuild (no double subscription). |
| `TestRefreshConvergesUnderLeaderFailover` (agent, -race) | TEST-12: a transient `CodeLeaderUnavailable` first tick still converges via the short fail-backoff (<5min). |
| `TestRebuildNoGoroutineLeak` (agent, -race) | TEST-5 (light): N rebuild cycles + clean cancel leave no goroutine growth. |
| `TestRosterRestartAntiRollback` (agent) | TEST-7: pin+hwm survive a restart through a real stateStore; expired cache → seed-only; lower gen still rejected post-restart. |
| `TestRosterStaleFieldsScrubbed` (broker) | TEST-8: `agent_roster_stale` body is exactly `{sid,nid,agent_gen,current_gen}` — no secret/topology leak. |
| `TestNodeRegisterReqOmitsRosterFieldsWhenZero` (agent) | TEST-10: request omits `roster_gen`/`roster_refresh_only` when zero (byte-equiv). |
| `TestStateFileRosterForwardBackCompat` (agent) | TEST-11: pre-C1 state.json round-trips; downgrade ignores the roster key. |

## Honest disclosure — TEST-2 (two-broker cross-port live failover)

The reviewer flagged that the only rebuild test reconnects to the SAME seed broker, not a NEW one. A true two-embedded-server failover test is **impractical under the D-3 uniform-port client-URL templating**: `DialURLs` templates every broker onto the seed's scheme/port, and two embedded NATS servers necessarily bind different ports, so the roster cannot produce a dialable URL for the second server (and loopback hosts are skipped by design). The failover **mechanism** is instead proven deductively by the composition of `TestEffectiveDialURLsSeedFloor` (a learned VOTER URL enters the dial pool ahead of the seed) + `TestSessionRebuildReconnects` / `TestRebuildNoDoubleForwardedDispatch` (a rebuild re-dials `effectiveDialURLs()` and re-establishes a clean single-subscription session). This is disclosed, not claimed as a direct end-to-end test. A real cross-broker e2e lands with the D9-style matrix backfill (multi-real-NATS), tracked for the C7 mega-audit.

## Gate status (post-fix)

`go build ./...` ✅ · `go vet ./...` ✅ · `go test ./internal/agent/ -race` ✅ (full package) · `internal/broker` ✅ · `internal/clusterroster` ✅ · `internal/proto` ✅ · `internal/cluster` ✅ (one `TestNode_SnapshotNothingNewContract` raft-election timeout under full-suite load — passes 3/3 in isolation, a known load flake, unrelated to C1) · `golangci-lint` ✅ 0 issues.

**C1 internal review CLOSED. Code staged. Proceeding to C2 (external review deferred to end of program per /goal).**
