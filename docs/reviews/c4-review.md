# C4 Stage-C Review + Adjudication — Cluster Operation Controller

> Stage-C adversarial internal review (8-agent workflow: 5 lens reviewers → 2 verifiers → synth; raw `tasks/w54jz41he.output`). **Initial verdict: FAIL — 2 BLOCKERs + 10 MAJORs.** The synth independently re-traced every load-bearing claim. The blockers were real resume-window bugs in the controller (the SSOT split + FSM determinism + single-active guard were sound, but the *recoverability core* had two kill-9 holes). This file records each finding + the main process's disposition. **Both BLOCKERs + all 10 MAJORs + the load-bearing MINORs fixed; controller tests added; plan/apply descoped + dead surface removed. Full gate green. External review deferred to end of the C-program.**

## Initial verdict: FAIL — headline

(1) a *successful* N=2→1 retire resumed to terminal `RETIRE_FAILED` with an orphaned drain marker (`RAFT_REMOVED` double-subtracted the voter count); (2) a resumed join silently promoted a not-caught-up node to a full voter (`ROSTER_COMMITTED` shortcut skipped the persisted barrier). Plus MAJORs that each defeated one named gate in a resume/race window, and **zero controller/resume tests** (the root cause the blockers shipped unnoticed).

## HARD-INVARIANT re-confirmation (post-fix)

| Invariant | Status |
|---|---|
| recoverability — no double-apply / no orphan / no silent stall | **HOLD** — BLOCKER-1: `RAFT_REMOVED` now gates last-voter only `if sub.isVoter` → a resumed retire after removal falls through to `RETIRED` + clears the marker. BLOCKER-2: barrier captured+persisted at `ROSTER_COMMITTED` BEFORE `AddVoter` → resume re-enters with a real goalpost. M1/M8: stranded join → loud `BLOCKED`, recoverable via `ops confirm`. |
| retire gates preserved AND re-checked on resume | **HOLD** — shared `retireGatePasses(op,sub)` (isVoter-aware) re-run at DRAIN_REQUESTED AND at the irreversible RAFT_REMOVED (M3); a concurrent retire that erodes F==0 → BLOCKED before RemoveServer. |
| SSOT — no divergence; substrate wins on resume | **HOLD** — RAFT_ADDING always runs `setPhase→CATCHING_UP` from a pre-mesh phase, so a resume where AddVoter committed but the phase bump didn't still heals (no stuck PENDING/op=SERVING divergence). |
| FSM determinism — all-literal, no Apply panic | **HOLD (was never violated)** — no CHECK/UNIQUE; single-active + CAS are RowsAffected no-ops; `genericExecApplier` ignores RowsAffected. (Reviewer's panic hypothesis refuted.) |
| one-prepare-one-approve REAL | **HOLD** — `prepare` joiner-local (refuted as non-local); M1: `NonceConsumed` now counts only `terminal=1` rows → a plain double-approve self-heals (idempotent same-op_id), not a false "replay". |
| topoConvergedForOp fail-closed | **HOLD** — predicate was correct; M5 fixed the one fail-open (a swallowed `TopologyGeneration` error → `target=0` → short-circuit): it now records + retries instead of advancing with gen 0. |
| active-op guard — no two-writer | **HOLD** — M2: `StartJoinOperation` now guards (attach iff same op_id, else refuse) + verifies the active op is ours before the roster admit. |
| non-cluster byte-equivalent | **HOLD** — controller cluster-gated; migration additive; guard test `TestC4DriveNoOpOnEmpty` + the existing single-mode invariants. |

## BLOCKERs — disposition (BOTH FIXED)

- **BLOCKER-1** (retire resumes to false RETIRE_FAILED + orphan marker): **FIXED** — `retireGatePasses` returns true when `!sub.isVoter` (already removed); RAFT_REMOVED uses it (`if sub.isVoter && !retireGatePasses`). A resumed-after-removal retire reaches RETIRED + clears `broker_draining`. Test `TestC4RetireGateLastVoter` (gate logic); multi-node resume e2e → gated test/c4 follow-up.
- **BLOCKER-2** (resume promotes a not-caught-up node): **FIXED** — `ROSTER_COMMITTED` captures+persists the barrier/deadline BEFORE any AddVoter (no side-effect); `RAFT_ADDING` performs the idempotent AddVoter + `setPhase→CATCHING_UP`. The barrier is non-zero by CATCHING_UP, so the catch-up gate is real on every resume; the phase always reaches VOTER (no divergence).

## MAJORs — disposition (ALL FIXED)

- **M1** NonceConsumed non-terminal → **FIXED** (`AND terminal=1`; test corrected from asserting the bug).
- **M2** StartJoinOperation no guard → **FIXED** (active-op guard + post-create verify, no phantom op_id).
- **M3** F==0 not re-checked at RAFT_REMOVED → **FIXED** (shared `retireGatePasses` at both gate sites).
- **M4** recordOpError idle 5s raft churn → **FIXED** (change-gated: skip if `last_error` unchanged → idle-zero-writes).
- **M5** toNatsRolledOut swallows gen error → fail-open → **FIXED** (record + retry, never advance with gen 0).
- **M6** BLOCKED join unrecoverable → **FIXED** (`ConfirmOp` re-enters a BLOCKED join at CATCHING_UP with a fresh barrier; kind-aware).
- **M7** `--wait` hangs on BLOCKED → **FIXED** (`waitForOp` short-circuits a stalled op → exitTransient + the confirm/abort hint).
- **M8** AddVoter retries forever → **FIXED** (`blockAfterAttempts`: bounded `opMaxAttempts` then BLOCKED).
- **M9** zero controller tests → **FIXED (single-node) + scoped** — `cluster_operation_controller_test.go`: `TestC4ApproveIdempotent` (M1), `TestC4JoinSecondApproveWhileActiveRefused` (M2), `TestC4RetireGateLastVoter` (BLOCKER-1 gate), `TestC4DriveNoOpOnEmpty` (guard/idle). The full multi-node kill-9 resume battery (BLOCKER-1/2 e2e) is a gated `test/c4` follow-up (needs a clustered harness) — **noted, not silently dropped**.
- **M10** plan/apply gap-row + dead PLAN_DRAFTED → **DESCOPED + dead surface REMOVED** (see below).

## MINORs — disposition

Fixed: **m1** (clear force_single before terminal SERVING), **m3** (StartRetireOperation refuses non-{VOTER,DRAINING}), **m5** (SEED advisory no longer in last_error), **m6** (read-back op_id, no phantom), **m7** (join approve `leaderRedirect`), **m8** (schema_version comment corrected: stays 1 additive), **m10** (ConfirmOp propagates NumVoters error), **m4/m11** (folded into the refactor / M5). Deferred (low-value, tracked): **m2** (AbortOp/ConfirmOp read-back), **m9** (terminal-op GC), **m12** (block on an Inconsistent voter), **m13** (`ops ls --active`), **m14** (ops-show error wording).

## M10 — plan/apply descope (transparent)

`cluster plan add` / `cluster apply <plan-id>` (建议1's auditable *decoupled* front-end) is **DESCOPED to a post-C4 follow-up**, and the dead `OpStatePlanDrafted` state + its doc-string are **removed** (no lip-service surface). Rationale: success metric #1 ("adding a broker = ONE prepare + ONE approve") is met by the ergonomic `cluster join prepare`/`approve`; the decoupled plan/apply is an auditability nicety over the SAME operation record, not a new capability. **This is flagged for the unified external review + 监工 #2** as a known scope reduction (the gap-doc rows for plan/apply stay 🟡 until delivered) — NOT claimed as done.

## Gate (post-fix)

`go build ./...` ✅ · `go vet ./...` ✅ · `internal/cluster` (op tests) ✅ · `internal/broker` (C4 controller tests + D7/D9 regression) ✅ · `cmd/tether` ✅ · `golangci-lint` ✅ 0 issues.

## Closure re-verification (independent) — PASS

An independent closure auditor re-traced both BLOCKERs + all 10 MAJORs against the FIXED source (file:line, not merely compiling) + ran the gate green. Verdict: **both BLOCKERs + all 10 MAJORs genuinely closed; the restructured join/retire state machines hold under adversarial resume tracing; no regression or fake-fix in the four files. C4 ready to stage.** Two honestly-scoped follow-ups it flagged were then ALSO fixed: (1) M6 residual — a join BLOCKED at the AddVoter step now recovers via `ConfirmOp` re-entering ROSTER_COMMITTED (re-captures barrier AND re-issues AddVoter), not just CATCHING_UP; (2) a legitimate last-voter `RETIRE_FAILED` now clears the `broker_draining` marker (not orphaned). Tracked follow-up (non-blocking): the multi-node kill-9 resume e2e that would directly cover the BLOCKER paths (gated `test/c4`).

**C4 internal review CLOSED (FAIL → all blockers + majors fixed → closure-verified PASS, + 2 flagged residuals fixed). Code staged.**
