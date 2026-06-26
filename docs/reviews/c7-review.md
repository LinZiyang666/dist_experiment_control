# C7 Stage-C Review — adjudication

> Workflow: 5 lenses → adversarial verify → synth (9 agents, Opus 4.8). 16 findings, 1 BLOCKER + 4 MINORs confirmed (the proposed MAJOR exfil-guard finding was corrected to MINOR on verify). Verdict CONDITIONAL.
>
> **CLOSURE STATUS (post-fix, INDEPENDENT verification PASS): C7-ROT-1 + M1–M5 + Drill all CLOSED; no new security defect; rejection-#2 secret-leak CLEAR.**
>
> Main-process dispositions — ALL ACCEPTED:
> - **C7-ROT-1 (BLOCKER)** — `--require-credential-rotation` on an F>0 (N≥4) cluster double-created the retire op → "already in flight" → NO alert/guide/banner (a rejection-#5 false-safe on the canonical retire-a-compromised-voter-without-losing-HA case). FIX: the require path in cluster_retire.go is now SEPARATE — read-only OpClusterStatus pre-check (fail-fast single mode) → typed-confirm → a SINGLE callAdmin(OpClusterRetire, Confirmed:true) → alert+guide+banner ALWAYS run on success (F>0 AND F==0). Pinned by cluster_rotation_admin_test.go TestC7RetireCompromisedRaisesTrackingAlert (stub fails any 2nd retire → would fail on the old two-call code). Independent verify: CLOSED.
> - **M1** — the #2 exfil guard was a leaky denylist (D7 wanted allow-list). FIX: the guard now forbids the full READ/WRITE/TRANSPORT/EXEC token set (os.Open/io.ReadAll/os.Create/.Write/http.Post/net.Dial/exec.Command/.Publish) + forbids importing net/http,net,os/exec. cluster_rotation.go imports only {fmt,io,adminsock,cluster,clusteroffline} — structurally cannot exfil. (Caveat: comprehensive denylist+import-forbid, not a literal AST allow-list; residual risk negligible given the 5 safe imports.)
> - **M2** — advisory --compromised-only guide falsely claimed a live severe alert. FIX: printCredentialRotationGuide(raised bool) — advisory passes false (suppresses the severe-alert/alert-clear lines), require passes true. Test TestC7AdvisoryGuideDoesNotPromiseUnraisedAlert.
> - **M3** — no-flag F==0 abort string changed (byte-equiv break). FIX: the non-require path restores "confirm an F==0 retire" (verified byte-identical to the staged C4 baseline; the require block is purely additive in the diff).
> - **M4** — single-mode require prompted before fail-fast. FIX: the read-only OpClusterStatus pre-check fail-fasts single mode BEFORE the prompt. Test TestC7CompromisedSingleModeFailsFast.
> - **M5** — missing cheap unit tests. FIX: landed the admin-stub tests (RaisesTrackingAlert/SingleModeFailsFast/TypedConfirmNeverYes) + the rotation-printer units.
> - **Staging drill** — DEFERRED as a tracked follow-up (HONEST per the independent adjudication): the CORE quorum-loss→force-single→recover + force_single_active marker + RecoverCluster no-double-apply is ALREADY covered by test/d7 TestD7Matrix "ForceSingleRecoverRestart" + internal/clusteroffline/offline_test.go (live-peer HARD-REFUSE/flock/dump). The residual is the C7-specific TEMPORAL MONOTONE invariant (exit-code != 0 until the first N>=3 + AllAtTarget, + the N=2 non-green negative check) over the production wiring — no unproven safety invariant. Recorded in-tree as the registered skipped gated stub test/c7/drill_test.go; the cheaper inmem-raft d7-harness stitch is the suggested build (avoids clustered-JS flake). Tracked alongside the C5/C6 gated-drill follow-ups for the post-C8 mega-audit.
> - make test + make lint green.

---

All claims verified. Baseline confirms the abort-string change (`...confirm an F==0 retire...` → `...confirm...`, drain still says `an F==0 drain`). `EvalDestructiveGate` never reads the alert store (manual never hard-gates) and the reconciler explicitly never retires `manual` (alert_reconcile.go:156-157) — so the conservative #5 design holds everywhere except the C7-ROT-1 false-safe. Here is the consolidated report.

---

# C7 Stage-C Review — Compromised-node credential rotation (guided printer)

## Verdict

**CONDITIONAL** — the conservative guided-printer design is genuinely sound (#2 structurally near-impossible by construction; #5 banner-only / never-auto-cleared / never-hard-gates all hold), **but one real rejection-#5 false-safe (C7-ROT-1, BLOCKER) makes `--require-credential-rotation` silently produce NO not-safe state on any N≥4 cluster** — the exact failure the phase exists to prevent. Fix that one blocker, then PASS.

## Confirmed BLOCKERs

### C7-ROT-1 — `--require-credential-rotation` on an F>0 (N≥4) cluster double-creates the retire op → "already in flight" error → NO alert / guide / banner (rejection #5 false-safe)
`cmd/tether/cluster_retire.go:43-64` (+ `internal/broker/cluster_operation_controller.go:98-142`, `clusterstatus.go:606-617`)

**Scenario** (verified end-to-end; only correct for N≤3 where F==0):
1. First `callAdmin(Confirmed:false)`. For F>0, `StartRetireOperation` never reaches the F==0 gate (controller line 120 `proj.FaultTolerance==0 && !confirmed`); it **unconditionally creates the op** (line 124) and returns `opID, nil`. There is no dry-run — `Confirmed` only matters at the F==0 gate. `ProjectQuorum(4,true).FaultTolerance==1`, `ProjectQuorum(5,true)==1` (clusterdrain.go:30-43).
2. Handler sets `QuorumProj` only for `ErrQuorumConfirmRequired` (clusterstatus.go:608-617). F>0 ⇒ no error ⇒ `QuorumProj==nil`, response `{OK, OpID}`.
3. CLI line 43 `if resp.QuorumProj != nil || requireRotation` — `requireRotation==true` forces entry despite `QuorumProj==nil`; prompts (typed-confirm OK), sets `Confirmed=true`.
4. Second `callAdmin(Confirmed:true)` → `assertNoActiveOp` (controller 27-36/99) finds the op the **first** call created → `"an operation (… DRAIN_REQUESTED) is already in flight"`. Not `ErrQuorumConfirmRequired`, so handler returns `{Error:…}`.
5. `callAdmin` returns `(resp, nil)` on transport success (admin.go:209-218); broker error lives in `resp.Error`, not `err`. So line 59 `if err!=nil` is false; line 63 `if resp.Error!=""` returns `clusterAdminError` and exits non-zero. **The switch at lines 68-84 (`raiseCredRotationAlert` + `printCredentialRotationGuide` + `printNotSafeBanner`) is never reached.**

**Net effect on any N≥4 HA cluster** (the canonical case: retire a compromised voter *without* sacrificing fault tolerance): the compromised node IS removed (first call ran the op), but **no persistent severe `manual:credrot:<node>` alert is raised, no rotation guide, no non-green banner** — the cluster reports no NOT-SAFE state, and the operator sees only a confusing "already in flight" error. Re-running repeats the failure. This is precisely the rejection-table #5 false-safe (docs/v2-usability-proposals.md §"#5: retire is not a security boundary; never signal safe") that D6 enforcement was meant to guarantee. Untested — the C7 unit tests never stand up a live F>0 admin.

**Fix direction:** in require mode, do not rely on the F==0 two-call dance to be the only op-creating call. Either (a) prompt for the typed confirm *before* the first `callAdmin` and issue a single `Confirmed:true` create (the F==0 confirm is subsumed by the require confirm anyway, per plan §2/D1); or (b) detect that the first call already created the op (F>0: `resp.OpID!="" && resp.QuorumProj==nil`) and skip the second call, proceeding straight to the alert/guide/banner switch. Whichever path, the alert/banner/guide switch must run on the same success that created the op, for all F.

## Confirmed MAJORs

None. The exfil-guard finding originally proposed at MAJOR (C7-EXFIL-01) is **corrected to MINOR** on verification: every factual claim holds, but it is a test-only regression tripwire — the actual #2 "structurally incapable" guarantee is delivered by the *design* (a printer + `RotateTunnelCert(fp string)` + alert text; no function in the flow reads raw key bytes), not by this single scan. See M1.

## MINORs

### M1 — #2 structural guard is a leaky **denylist**, not the plan-bound **allow-list** (D7)
`cmd/tether/cluster_rotation_guard_test.go:23-32` *(dedup: C7-EXFIL-01 corrected→MINOR + C7-GUARD-3 + C7-BES-4)*

Plan D7/§3 bind #2 to an **allow-list** token-scan so the flow is "structurally incapable of moving private key material" — only `{derivePublicKey, readClusterPublicIdentities, AccountFingerprint, SecretsPreflight}` may touch secrets. The file's own doc-comment (line 10) even *says* "ALLOW-LIST token-scan" while implementing a **denylist** of 8 tokens (`os.WriteFile, ioutil.WriteFile, io.Copy, os/exec, exec.Command, .Publish(, .Request(, .ReadFile(`) + a 2-token presence check. Concrete holes (grep-confirmed absent from the forbidden set): READ — `os.Open(`+`io.ReadAll(` (and `io` is already imported at cluster_rotation.go:5); WRITE — `os.Create(`/`os.OpenFile(`+`.Write(`; TRANSPORT — `net/http` (`http.Post`/`http.NewRequest`), `net.Dial`+`conn.Write`. A future `os.Open`+`io.ReadAll`+`http.Post` of `account.nk` would exfil a seed and pass both the guard and the output-only canary (`cluster_rotation_test.go` inspects only `b.String()`). **Current production code is clean** — not a leak today, so not a rejection-table violation now; it is a hardening gap against future edits. Doc/impl mismatch worth fixing regardless.

**Fix direction:** convert to a real allow-list via `go/ast` over cluster_rotation.go — fail if any call reaches the fs/network/exec families except the sanctioned helpers + `fmt.Fprintf`-over-`io.Writer` + `callAdmin`. At minimum add `os.Open, os.OpenFile, os.Create, io.ReadAll, os.ReadDir, ioutil.ReadFile/ReadAll, http., net.Dial, .Write(`-on-`*os.File`/`net.Conn` to the forbidden set, and forbid importing `net/http`/`net`.

### M2 — Advisory `--compromised`-only guide falsely claims a live severe alert (soft #5 false-safe)
`cmd/tether/cluster_rotation.go:55,96-97` (+ `cluster_retire.go:76-78`)

`printCredentialRotationGuide` is shared by both paths and **unconditionally** asserts the alert is live: line 55 "The cluster reports NOT-SAFE (severe alert `manual:credrot:<node>`) until you finish…"; line 96-97 "WHEN DONE …: `tether alert clear manual:credrot:<node>`". But the advisory branch (`case compromised:`) never calls `raiseCredRotationAlert` and never prints the banner. So after plain `cluster retire <n> --compromised`: the guide tells the operator a severe alert is active and to clear it, yet `tether alert ls` shows none and `tether alert clear` is a no-op. An operator treating `alert ls` as source-of-truth sees an empty list — a soft #5 false-safe. Mitigated: the guide still loudly prints NOT-SAFE / "NOT A TRUST REVOCATION", and the recommended compromised path (now repointed in cluster.go) is the *require* path which does raise.

**Fix direction:** text-only — gate the "cluster reports a severe alert" / "`alert clear`" lines on whether the alert was actually raised (pass a `raised bool` into the printer, or emit those lines only from the require branch).

### M3 — No-flag F==0 retire abort string not byte-identical + drain/retire wording asymmetry
`cmd/tether/cluster_retire.go:55` *(dedup: C7-NOTSAFE-04 + C7-BES-1)*

The shared confirm block changed the plain (no-flag) F==0 abort message. Baseline (`git show HEAD:…:40`): `aborted (type the node_id to confirm an F==0 retire; --yes is not accepted)` → C7 (line 55): `aborted (type the node_id to confirm; --yes is not accepted)`. Reachable on the strictly no-flag path (plain `retire <n>` on an F==0 cluster, declined confirm). The success NOTE *is* byte-identical (default branch lines 81-83), so "no-flag path byte-identical" holds for the NOTE but **not** for the abort string. Also asymmetric: `drain` still says `an F==0 drain` (cluster.go:524), contradicting the file-header "retire F==0 gate is identical to drain". Cosmetic; no security/behavior impact.

**Fix direction:** restore `confirm an F==0 retire` for the `QuorumProj!=nil` branch (or update the plan's byte-identical claim and pin the new string + drain/retire symmetry in a test).

### M4 — Single-mode `--require-credential-rotation` prompts + extra admin round-trip before fail-fast (new single-mode surface)
`cmd/tether/cluster_retire.go:43`

In single (non-cluster) mode the backend is nil → first `resp.Error == "cluster mode not enabled"`. No-flag and `--compromised`-only fail-fast byte-identically (block at line 43 skipped). But `|| requireRotation` enters the block **on the error response**: `leaderRedirect` is false, so it prints the require WARNING, calls `confirmTypedNodeID` (interactive prompt against a non-cluster socket), then makes a second `callAdmin` before finally hitting `if resp.Error!=""`. Safe — it still fail-fasts with no state change (alert/guide gated on the success path) — and `require` is a brand-new flag, not a regression. But it contradicts the "adds no single-mode surface" framing.

**Fix direction:** check `resp.Error != ""` (and short-circuit) before entering the confirm block.

### M5 — Plan §6 named tests largely unimplemented (security-relevant coverage gap)
`cmd/tether/cluster_rotation_test.go` *(dedup: C7-NOTSAFE-03 + C7-BES-3 + the cheap predicate units from C7-BES-5/C7-DRILL-2)*

7 of 13 named rotation tests shipped. Missing and security-relevant (all cheap cmd/tether units, no flake excuse): `TestC7RetireCompromisedRaisesTrackingAlert` (no test asserts the alert is raised with kind=`manual`/severity=`severe`/label=`credrot:<n>` and that `DedupKeyNode("manual","credrot:"+n) == rotationTrackingKey(n)`), `TestC7CompromisedTypedConfirmNeverYes` (require must refuse `--yes`), `TestC7RetireExitsZeroOnSuccess`, `TestC7RotationManualAlertDoesNotHardGate` (the key #5 guarantee that a manual severe alert does NOT block destructive writes — holds today via `EvalDestructiveGate` ignoring the store, verified, but a future `gateDestructive` change consulting store-backed severe alerts would silently turn `credrot` into a cluster-wide write-lock with no failing test), `TestC7PlainRetireByteIdentical`, `TestC7CompromisedSingleModeFailsFast`. Plus the two untagged predicate units (`TestC7StatusExitCodeMapping`, `TestC7SeverityComposite`) were dropped along with the gated drill — these have no harness dependency and no flake excuse. Behavior is correct on inspection; this is unpinned-against-regression, not a defect.

**Fix direction:** land the missing cheap units now (especially `TestC7RotationManualAlertDoesNotHardGate` and `TestC7RetireCompromisedRaisesTrackingAlert`); these guard the exact #5 invariants the phase rests on.

## Staging-drill disposition

**Deferral is HONEST and ACCEPTABLE as a tracked follow-up — NOT a hard C7 ship-blocker.** Adjudicated across five reviewers; consensus confirmed by reading the code.

- `test/c7/` does not exist (confirmed: `ls test/c7` → no such directory).
- Basis (a) is **verifiable-true** (and slightly understates coverage): the offline force-single → recover → HARD-REFUSE primitives the drill leans on are independently proven — `test/d7/integration_test.go:367-472 testD7ForceSingleRecover` (registered line 132 "ForceSingleRecoverRestart") genuinely kills a 3-node cluster, runs `clusteroffline.ForceSingle` on a survivor (abandoning 2 confirmed-dead peers), `RecoverCluster` replay with no-double-apply (premark value + roster row count preserved, `applied_index` non-regressing), restarts writable N=1, and asserts the `force_single_active` marker (the exit-3 status source) is set. The live-peer TCP HARD-REFUSE + empty-state-refuse + flock + dump-before-wipe primitives are in `internal/clusteroffline/offline_test.go`.
- Basis (b) is corroborated by CLAUDE.md's documented clustered-JS starvation/flake.

**What genuinely remains uncovered (the residual):** the C7-specific **temporal monotone invariant** — `cluster status` exit code continuously ≠ 0 from quorum-loss through force-single (exit 3) through the N=2-stable waypoint, becoming 0 (HEALTHY_HA) **only** at the first N≥3 + AllAtTarget sample, asserted over the *production* `computeHealth`/`AlertReconciler`/force-single-clear wiring on a routed-clustered-JS + mTLS-raft topology — **plus** an explicit negative check that N=2-stable is still non-green even though `force_single_active` has cleared and no severe alert is present (the exact `manual:credrot` clears-at-N=2 trap the plan flagged in D9). The per-waypoint *verdicts* are already unit-proven (`clusterstatus_test.go:76` DEGRADED at N=2; `c6_test.go` NOT-HA ⟺ FT==0; `d9_external_review_test.go:63` stream-actual<target ⇒ not HEALTHY_HA; `clusterstatus_test.go:41 TestD7HealthExitCodeSSOT`), and JS converge-to-target is covered by `test/d8`. So no **unproven safety invariant** exists; the residual is a live regression-net stitching those proven verdicts in time over the production responder. This is over **pre-existing D9 machinery — C7 added zero production code to those paths.**

**Caveat (track explicitly, do not close silently):** the deferral is recorded nowhere in the diff — no `test/c7/`, no `c7_integration` registration in `test/e2e/all_phases_test.go`, no Makefile note. Record it in c7-review.md (this report) + a skipped/registered `c7_integration` stub so it is auditable. Note that a cheaper non-flaky stitch is buildable on the proven inmem-raft d7 harness (walk `startD7Cluster(3)`→kill→ForceSingle→regrow N1→N2→N3, sample `StatusReport().ExitCode` at each waypoint, stub `StreamActual`/`StreamTarget` like d9_external_review_test.go) — closing the residual with zero clustered-JS flake exposure, if/when prioritized.

## Suggested tests

1. **(BLOCKER pin, C7-ROT-1)** Fake admin via `adminsock.New(path, backend)` simulating F>0: for `OpClusterRetire` `Confirmed==false` return `{OK, OpID, QuorumProj:nil}`, for `Confirmed==true` return `{Error:"already in flight"}`; record `OpClusterAlertRaise` calls. Run `cluster retire brk-x --compromised --require-credential-rotation` with a typed-confirm via `cmd.SetIn`. Assert exactly ONE retire op created, the `manual`/`severe` `credrot:brk-x` alert IS raised, and the NOT-SAFE banner IS printed. Today fails (double-create, alert/banner never fire).
2. **(M1)** Replace the denylist with a `go/ast` allow-list over cluster_rotation.go + a negative fixture (a copy with injected `os.Open`+`io.ReadAll`+`http.Post` / `os.Create` / `net.Dial`) the guard must reject.
3. **(M2)** `TestC7AdvisoryGuideDoesNotPromiseUnraisedAlert`: render the advisory-path guide; assert it does NOT claim a live severe alert nor instruct `alert clear` unless raised; the require path keeps those lines. End-to-end `retire --compromised` (no require) asserting no `OpClusterAlertRaise`.
4. **(M3)** `TestC7PlainRetireF0AbortByteIdentical`: plain `retire <n>` with non-nil `QuorumProj` + refused confirm; assert the abort string equals the pre-C7 `…confirm an F==0 retire…` (or update plan + pin new string + drain/retire symmetry).
5. **(M4)** `TestC7CompromisedSingleModeFailsFast`: require mode against a stub returning `Error:"cluster mode not enabled"`; assert cluster-not-enabled error, NO alert, NO guide, and (ideally) no typed-confirm prompt.
6. **(M5)** `TestC7RetireCompromisedRaisesTrackingAlert` (capture `OpClusterAlertRaise`: kind=`manual`, severity=`severe`, label=`credrot:<n>`, and `DedupKeyNode==rotationTrackingKey==`clear key); `TestC7CompromisedTypedConfirmNeverYes`; `TestC7RetireExitsZeroOnSuccess`; `TestC7RotationManualAlertDoesNotHardGate` (`EvalDestructiveGate` over a writable-leader fixture ⇒ `Blocked()==false` regardless of any active manual severe alert; reconciler `ReconcileAlertsOnce` with `manual:credrot:*` in current ⇒ never appended to clears). Land `TestC7StatusExitCodeMapping` + `TestC7SeverityComposite` (untagged, no harness).
7. **(Drill follow-up)** Track gated `test/c7 TestC7DrillNotGreenUntilN3AndStreamsAtTarget` (CORE temporal monotone + N=2 negative check) as an explicit registered/skipped stub; prefer the inmem-raft d7 harness stitch over a real clustered-JS harness.

**Relevant files:** `/home/weiland/projects/dist_experiment_control/cmd/tether/cluster_retire.go`, `cmd/tether/cluster_rotation.go`, `cmd/tether/cluster_rotation_guard_test.go`, `cmd/tether/cluster_rotation_test.go`, `cmd/tether/cluster.go`, `internal/broker/cluster_operation_controller.go`, `internal/broker/clusterstatus.go`, `internal/broker/clusterdrain.go`, `internal/broker/alert_reconcile.go`, `internal/proto/alerts.go`, `test/d7/integration_test.go`, `internal/clusteroffline/offline_test.go`, `docs/reviews/c7-plan.md`, `docs/v2-usability-proposals.md`.