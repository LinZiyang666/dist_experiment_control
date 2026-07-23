# R16 external review tasklist

> For the EXTERNAL reviewer (the user, per CLAUDE.md §3 step 6 / §4). The main process cannot self-certify
> this step. Report → `docs/reviews/r16-external-review.md`; the main process then replies inline per finding
> and fixes.
>
> **This tree now carries TWO increments and they share this gate: R16 (§0–§5 below) and G67 (the §G
> sections at the end).** G67 was built on top of R16 after the deploy tier surfaced gotcha #67 during
> R16 validation. Review boundary = the UNCOMMITTED working tree: **44 changed + 17 new** files
> (`git diff HEAD` + `git status`); nothing is staged (staging is the reviewer's call).
>
> Context (read as context only — do NOT trust its conclusions): plan `docs/reviews/r16-plan.md`; the internal
> review + fix log are summarized in the plan's lineage and in the memory entry `project-r16-grow-onto-recovered`.

## 0. Boundary + gates (verify, don't take my word)

- [ ] Reconstruct the review boundary from `git status` / `git diff HEAD`; confirm **44 changed + 17 new**
      files across BOTH increments.
- [ ] Re-run the hard gates independently: `make test`, `make lint`, `make e2e`. My results on the FINAL
      tree (after the G67 internal review's fixes and after deleting the reviewers' scratch test files):
      `make test` **0 FAIL**, `make lint` **0 issues**, `make e2e` **ok 647.4s, 0 failures**.
      An earlier "0 FAIL" I reported was measured while review agents were concurrently writing test
      files into the tree — that measurement is void and this one supersedes it.
- [ ] Re-run `test/simcluster/tests/run-all.sh` (my result: ALL PASS incl. ledger-crosscheck, 8 open defects
      all pinned by a non-GREEN owner cell).

## 1. Attack the load-bearing product changes

- [ ] **B1** `internal/cluster/snapshot.go` — stripping `restore_in_progress` on EVERY snapshot install. Is
      the SOURCE node still correctly fenced until `clearRestoreInProgress`? Is there any OTHER node-local
      `cluster_meta` key that must not ride a snapshot (I enumerated them and claim only this one + `self_node_id`)?
- [ ] **A2c** `internal/clusteroffline/restore.go` — `GrowReadySnapshot` after `BootstrapSingleNode`, marker
      cleared last. Is the `applied_index=0` × snapshot-index pairing right for a RESTORED (direct-installed)
      DB, not just for `init --from-existing`? Crash between bootstrap and snapshot → re-run forward-completes?
- [ ] **A1** `cmd/tether/cluster_add_drive.go::resetJoinerJSStore` + `natsconf.JSStoreHasData` — the
      data-bearing heuristic ("any >0-byte regular file") decides whether a joiner's store is moved aside.
      Can it FALSE-NEGATIVE on a real returning node (skip a reset that was needed → re-open the wedge)?
      Can it FALSE-POSITIVE on a fresh joiner (needless move + a data-loss HALT)?
- [ ] **A3/A4** `--reset-js` on offline force-single and on `reconcile nats --to-standalone`. A3 REFUSES a
      data-bearing store without the flag *after* the raft/DB phases already committed (journalled, re-run
      forward-completes). Is that the right call for an EMERGENCY escape hatch, or should it complete +
      warn? A4 moves the store BEFORE the conf swap — confirm a refusal really leaves the conf untouched.
- [ ] **A5-min** `performGrowCutover` — epoch-specific residue evidence (`.grow-cutover-<epoch>.done` OR
      `grow-bak.<epoch>`) + loud refusal. Can a LEGITIMATE mid-grow resume be falsely refused? Can real
      residue still be silently absorbed?
- [ ] **Lane B (#57)** `internal/broker/xfer_inflight.go` — the ledger is deleted ONLY on a confirmed commit.
      Any remaining window where the ledger dies without a durable terminal? Is the synthetic record truly
      byte-deterministic (so `TransferRecordReqID` dedups a re-emit)?
- [ ] **Lane C (#58)** `internal/broker/transfer_reconcile.go` — leader-only cross-home GC. Can the leader
      reap an object still live on ANOTHER home (age floor vs cross-node clock skew)? Is the busy-bucket
      conjunct keyed correctly? Is the `3×transferTimeoutTierB` derivation defensible?
- [ ] **The two fixes the deploy tier forced** (both in `cluster_add_drive.go` / `broker.go`): the
      start-joiner readiness check is now a bounded 60s POLL, and the eager #57 boot finalize moved AFTER
      the admin socket. Is 60s the right bound? Does the poll now mask a genuinely misconfigured joiner?

## 2. Decisions I made deliberately — please second-guess them

- [ ] **A5-min scope**: the full in-cutover auto-reset + a `/jsz` meta-health verdict machine was DEFERRED;
      recovered clustered residue gets a LOUD REFUSAL pointing at `--to-standalone --confirm-single --reset-js`.
      Plan §0 decision 5. Is loud-refuse acceptable, or must the grow self-heal it?
- [ ] **Joiner ack model**: `--reset-former-js` / `--preserve-js-data` are REUSED for the joiner end (help +
      refusal text widened) instead of a third flag. Plan §0 decision 4.
- [ ] **#47 closed on evidence** (`docs/deploy-tier-gotchas.md`): the ledger demanded a post-fix remote re-run
      against a named oracle (invocation-2 rc=0 + VOTER + terminal SERVING); drill 42's GREEN run met all
      three. **Is that close justified, or premature on a single run?**
- [ ] **#67 registered but NOT fixed — the scope call I most want challenged.** I judged it OUT of R16:
      it is pre-existing (present in released v0.4.7), it is not an R16 regression, and folding a change to
      the user-facing transfer path in *after* the internal review had closed would ship unreviewed code on
      the data plane. Instead I registered it, wrote a dedicated deterministic pin
      (`drills/67-transient-js-refusal`), and left it OPEN. **Counter-argument you should weigh:** the fix
      is genuinely small and cheap — the client's prepare RPC deadline is `--timeout`, default **10
      minutes**, so the binding constraint is only the broker's internal 5s ceiling, and face B is a
      three-line error-handling change (stop discarding the `probeCaps` error; distinguish "the probe
      failed" from "no JetStream"). I also already folded TWO deploy-tier-forced fixes into this phase
      post-review (§1), so "no product changes after the internal review" is not a line I held absolutely.
      Decide whether #67 blocks the release, rides along in R16, or becomes the next leaf increment.
- [ ] **Ledger hygiene**: #31 was given drill 30 as its owner cell (the ledger always said its `[GAP #31]`
      pin lives there; the owner cell had never named it). Verify that is bookkeeping, not laundering.

## 3. What I did NOT prove (the honest gaps — please hold me to these)

- [ ] **#57 / #58 have NO deploy-tier demonstration.** The product fixes shipped and are pinned hermetically,
      but drill 96's 1 GiB tier-B upload again completed before the `docker kill`, so no dangling start row
      and no orphan set ever existed (peak orphan 2 ≤ tombstone floor 6; GC reap-log count 0). I therefore
      ADDED a non-vacuity gate to the #58 arm and left **#57/#58 OPEN**. Verify I did not bank a vacuous
      green anywhere else in that drill.
- [ ] **drill 42 stability = 3 of 4, and the 4th run's failure is a DIFFERENT, PRE-EXISTING defect.** After
      the two fixes the R16 rejoin arm passed in every run it reached (4/4). But repeat run 3 died in the
      SETUP fixture, before the R16 arm: `baseline: tier-B push works on healthy N=2` → `bucket_create_failed
      create_bucket: context deadline exceeded`. Root-caused (§4) to the tier-B push prepare path, which R16
      does not touch — registered as **new gotcha #67**, left OPEN. Judge whether I was right to scope it
      OUT of R16 rather than fold in a fix after the internal review had already closed.
- [x] **Test debt (plan M7) — two of three CLOSED after the internal review flagged them:**
      `TestPerformGrowCutoverRefusesRecoveredResidue` now drives `performGrowCutover` itself, so deleting the
      A5-min guard no longer reddens nothing; `TestXferInflightTerminalDropsLedger` pins a terminal path's
      `removeXferInflight` trailer (a dropped trailer would make the finalizer publish a contradictory
      synthetic `failed` row for a cleanly-ended transfer). **Still open:** the other three trailers
      (watchdog / handleEvTransfer / handleFinalizeReq) are covered end-to-end only, and there is still no
      hermetic restore-then-grow-a-joiner e2e — deploy-tier drill 51 covers that path for real instead.
- [ ] **Deferred, record-only** (plan §6): online force-single auto-de-cluster; `/jsz` verdict machine;
      `warnStandaloneJSGrow`'s `rm -rf` (the MANUAL-takeover grow direction — A4 only retired the shrink one);
      auto `nats stream backup→restore`; permanent-home-loss audit dangling; backup/sentinel litter pruning.

## 4. Deploy-tier evidence (weilandserver, real systemd + real standalone nats-server)

Everything below is a real run on the deploy tier, not a hermetic simulation. Re-run any of it with
`cd test/simcluster && ./remote.sh drill <name>` (add `--build` after changing Go code).

- **drill 51-full-dr** — `I re-grow to N=2 succeeded after the DR` + `I2 the data plane STILL serves the
  original sentinel`, **product_red=0**, pass=72. `#GROW-ONTO-RECOVERED` fixed (A2c). Verdict stays
  INCOMPLETE on orthogonal pre-existing gaps only (`[GAP #6-chown]`, `#53-scope` WONTFIX, H1a).
- **drill 42-rejoin-returning** — **GREEN** (`rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0
  pass=48`). Stability over **4 runs on the final image**: the R16 rejoin arm passed in **all 4 runs that
  reached it**; repeat run 3 died EARLIER, in the shared setup fixture (`baseline: tier-B push works on
  healthy N=2` → `bucket_create_failed: create_bucket: context deadline exceeded`, pass=47) — a
  pre-existing defect on a path R16 does not touch, now registered as **#67** with its own owner drill.
  Log evidence in order:
  `… waiting up to 1m0s for brk2's broker to serve cluster status` → `✓ brk2 is now a VOTER` →
  `cluster add complete.` → `F [REJOIN TERMINUS] … RE-GROWS …`. Pre-R16 this arm deadlocked on
  `n1ClusteredJetStreamFatal` every single run; pre-fix (post-A1) it still failed 3 of 4 on the one-shot
  readiness probe. The drill's own `mv` compensation is gone — the survivor's store is now reset by the
  product verb `force-single --reset-js`, asserted by a `jetstream.force-single-bak.*` file postcondition.
- **drill 96-mid-flight-chaos** — `product_red=0`, `assert_fail=0`, pass=38, verdict INCOMPLETE. **The
  #57/#58 arms did NOT exercise their mechanisms** (see §3): the 1 GiB upload beat the `docker kill`, so
  peak orphan was 2 against a tombstone floor of 6 and the GC reap-log count was 0. A non-vacuity gate was
  added so that shape records `not_covered` instead of a green that proves nothing.
- **NEW: #67 + `drills/67-transient-js-refusal`** (found by drill 42's repeat run 3, then root-caused with
  two targeted deploy-tier probes). A **transient** JetStream stall is reported to the operator as a
  **permanent capability absence**, with wrong advice and zero retry. Two faces, one root:
  - *face B, deterministically reproduced*: freeze the peer's `nats-server` (`SIGSTOP`) → the push is
    refused with `jetstream_unavailable: broker has no JetStream … either bump nats max_payload >= N or use
    tether expose + rsync`; `SIGCONT` and the **identical** push succeeds seconds later with no operator
    action (`OK (tier B, 12000000 bytes, 187ms)`). The broker never claimed an absence —
    `handleCapsReq` derives `JetStreamReady` from the **static** `b.js != nil`
    (`internal/broker/transfer.go:1017`); the sentence is synthesised client-side from the zero-value
    `CapsResp` left by the **swallowed** probe error at `cmd/tether/transfer.go:162`
    (`caps, _ := probeCaps(…)`). `max_payload` is unrelated, and the advice steers the operator off tier-B.
  - *face A, observed but not deterministically pinned*: the broker-side bucket provisioning is a
    **single 5s attempt with no retry** (`internal/broker/transfer.go:560`). A probe measured that create
    at **~57 ms** in a healthy window (push#1 1.253s with the create vs push#2 1.197s without, same
    cluster) — so the budget has ~100× margin and run 3's failure was a genuine multi-second stall being
    made fatal, **not** a chronically tight timeout. My earlier "5s is marginal" hypothesis was refuted by
    that probe. Meanwhile the broker's own reconcile loop treats the very same condition as transient and
    retries it every ~100 ms (`d5: replica reconcile … 503 10008 JetStream system temporarily unavailable`)
    — the retriable channel exists but the **user-facing** push path cannot reach it.

  **The pin now reproduces face A deterministically.** `drills/67-transient-js-refusal` v3 (clean
  `systemctl stop nats-server` on the peer instead of `SIGSTOP`) returns `verdict=PRODUCT-RED rc=3
  product_red=1 assert_fail=0 setup_red=0 not_covered=0 pass=13`, and the signature it catches is
  **word-for-word the one that broke drill 42 repeat run 3** — independent confirmation that run 3 was
  #67 and not an R16 regression.

  **Please audit how I got there — the oracle was wrong twice and I want that checked, not trusted:**
  - v1 recognised only the face-B wording → the first run hit a third signature and honestly recorded
    `not_covered`, not a forged RED. Guard worked; oracle too narrow.
  - v2 widened to "any refusal with no retry hint" → it **fired a PRODUCT-RED that I discarded rather
    than banked**: the real signature was `cannot reach broker … i/o timeout` (rc=69), a *connection*
    failure caused by `SIGSTOP` leaving a hung TCP peer that poisoned brk1's own route I/O. Calling that
    #67 would have been over-claiming. Verify I actually discarded it and did not quietly keep the green.
  - v3 requires the refusal to name a **registered tier-B/JetStream face**, plus proven transience.
  - **Still unpinned, stated plainly:** face B did NOT reproduce under the clean-stop injection. It rests
    on a **single manual probe** and has **no drill-level oracle**.

  **Severity went UP while I was pinning it, and you should weigh this against my "not in R16" call.**
  A repeat run failed at the *pre-injection* control: a freshly-grown, healthy N=2, **first** tier-B push,
  **no injection at all** → `code=bucket_create_failed create_bucket: context deadline exceeded` (rc=70),
  and a plain immediate retry then succeeded. So this is not an edge case that needs an operator to act
  inside a blip window — **"grow the cluster, then send a file" intermittently hard-fails on its own**,
  with no hint to retry. The drill measures that case rather than absorbing it: the control is allowed one
  retry, but a successful retry **always** records a `product_red` saying no injection was needed.

  I **registered and pinned** it but did **not fix** it — see §2 for the scope call I am asking you to judge.

## 5. Verdict

- [ ] Write `docs/reviews/r16-external-review.md` with Pass/Fail, findings, doubts, recommendations, and how
      you verified. Do NOT stage files — the main process replies inline and fixes; staging is your call after.

---

# G67 — the second increment in this same working tree

> R16 is parked at this gate; G67 was built on top of it and shares the gate. Plan:
> `docs/reviews/g67-plan.md`. Defect SSOT: `docs/deploy-tier-gotchas.md` `### #67` and `### #68`.
> Boundary: the same uncommitted tree — G67 adds `internal/jsstream/transient.go`,
> `internal/broker/xfer_provision.go`, `test/simcluster/drills/67-transient-js-refusal.sh` and their
> tests, and touches `internal/broker/transfer.go`, `cmd/tether/transfer.go`,
> `cmd/tether/error_hints.go`, `internal/natsconf/remedy.go`, `internal/broker/clusterstatus.go`,
> `internal/proto/messages.go` (doc comment only), `drills/92-…`, `drills/lib/setup-forcesingle.sh`.

## G0. What G67 does and does NOT claim

- **Claims:** a transient JetStream stall on the tier-B path is now reported HONESTLY (its own code,
  a retry instruction, an escalation path) and is bounded-retried before the refusal.
- **Does NOT claim** that "grow, then push" always works. Measured: unloaded, the first tier-B push
  after a grow succeeds in 1.66 s with zero retries; under heavy concurrent sim load the post-grow
  window exceeds the 8 s budget and the (honest) refusal still happens. The real fix belongs in
  `cluster add` — registered as **#67 sub-face 4**, unfixed.

## G1. Attack the load-bearing changes

- [ ] `jsstream.IsTransientProvisionErr` — permanent-first ordering, fail-permanent default. Can a
      permanent condition buy retries, or a transient one fail fast? `IsMetaGroupNotReady` is
      deliberately byte-unchanged — verify, and verify that calling it from here cannot change any
      verdict its reconcile-loop callers see.
- [ ] `provisionXferBucket` — budgets, per-attempt contexts, the abort branch, idempotency of a
      retried create, and the claim that it runs entirely BEFORE `transfers.put()` on BOTH verbs.
- [ ] The **budget-vs-head-of-line judgement** (the internal review asks for a human ruling):
      worst-case in-handler occupancy went 5 s → 9.5 s on `.push.req`, which nats.go delivers
      SERIALLY, so it is head-of-line latency for every other push on that broker. `maxTries=2`
      (≈5.5 s) buys most of the observed benefit at half the amplification. **Is 3 right?**
- [ ] Face B — `capsProbe` classification, the optimistic path, `tierAInlineCeiling`. Can a genuine
      single-broker no-JetStream deployment still get an honest PERMANENT refusal?
- [ ] The exit-code taxonomy after the review's M5 fix: `jetstream_not_ready`→75,
      `tier_b_store_too_small`→64, `broker_restarting`→75, `jetstream_unavailable`→64,
      `bucket_create_failed`→70 (unclassified). Does that agree with `docs/usage.md` §9.13 everywhere?

## G2. Judge these decisions

- [ ] **The drill/fixture assertion change.** `drills/lib/setup-forcesingle.sh` and drill 67 now accept
      "refused as transient → the documented retry succeeded". The internal review ruled this
      **legitimate, not laundering** (a non-transient refusal still hard-fails; a failed retry is a
      product_red) but found three accounting defects, all since fixed. **Re-judge it yourself** —
      this is the change most likely to be self-serving.
- [ ] **#68 marked FIXED** (R16's A4 updated one remedy copy and missed the SSOT, so the
      JetStream-UNAVAILABLE banner told operators to run a command that would refuse). The command
      itself was left unchanged and only a note was added. Right call?
- [ ] **Test debt paid down after the review**: the review showed five face-B mutations and the whole
      wall-clock budget could be deleted with every gate green. Verify the new pins actually redden.

## G3. What I did NOT do

- [ ] **#67 sub-face 4** (`cluster add` reports success before the JS meta can place assets) — the item
      most likely to bite a real operator. Registered, unfixed, and now countable via drill 67.
- [ ] **face B has no deploy-tier oracle** — the only injection that reproduced it (SIGSTOP) was
      retired for producing connection-level failures that are a different defect. Drill 67 declares
      this as an `nc_gap`, which is why its verdict is INCOMPLETE and not GREEN.
- [ ] `b.js` is probed once at boot, never re-probed (open sub-face 1) — the shared root of two
      further review minors.

---

# G69 — the third increment in this same working tree

> #67 **sub-face 4**: `cluster add` declared a join terminal SERVING before the JetStream meta could
> place an R=N asset, so "grow, then send a file" intermittently hard-failed. G67 made that failure
> honest and retryable; G69 removes the structural window. Plan: `docs/reviews/g69-plan.md`
> (§0 records the finalizer decisions, including one that OVERTURNED the main process's own scoping note).

## H1. Attack the load-bearing change

- [ ] `jsPlacementAdvance` + its call site in `driveJoin` — is the gate really a BOUNDED WAIT whose
      expiry outcome is ADVANCE, for every input (zero deadline, deadline already past, clock jump,
      leadership change, crash-resume)?
- [ ] **`jsGateExpiryReserve` — the BLOCKER the internal review found and I had shipped.** My doc
      comment claimed "no input holds an op past the deadline". True of the conjunct; **false of the
      ladder**: holding the op alive at NATS_ROLLED_OUT keeps `topoAdvance` (fail-CLOSED, expiry →
      `OpStateBlocked`) re-evaluated every tick, and it runs FIRST. The correlation is real —
      `topoConvergedForOp` needs each voter's `TopoReported`, set only when that node answered THAT
      tick's health scatter-gather, and the saturated host that slows JS placement is the host that
      drops health replies. Landing in BLOCKED means `blockedConfirmDecision(0,0,false)` →
      `cluster add` exits non-zero with the WRONG cause and `assertNoActiveOp` fences the membership
      plane. **Is a 30s reserve (≥2 observe ticks) actually sufficient, and is the reasoning right?**
- [ ] The predicate: `events` stream, `Configured >= target && Assigned >= target`, where `Assigned`
      deliberately does NOT filter on `Current`. Is assignment sufficient evidence that a NEW object
      store can be created at R=N? **I did not verify that**, and the measured failure was a
      `CreateObjectStore` *deadline*, not a placement refusal (plan §7.1).
- [ ] `AssignedReplicas` — is it correct for every `StreamInfo` shape nats.go produces? Note
      `ActualReplicas` nil-checks each peer and `AssignedReplicas` does not.
- [ ] Confirm `ActualReplicas` / `Ready` / `AllAtTarget` and every retire caller are behaviourally
      byte-unchanged (retire's data-safety gate must not have moved).

## H2. Judge these decisions

- [ ] **ADVANCE on expiry, not `OpStateBlocked`** (plan §0.3). Rejected on source evidence:
      `--auto-confirm-catchup` defaults to 0. The cost is that on expiry we ship a SERVING we could not
      prove — which is exactly today's behaviour, plus a durable timeline entry. Right trade?
- [ ] **The plan overturned my own scoping note** (`g69-subface4-scoping.md`): "reuse
      `clusterStreamsReady` symmetrically" would have made every grow wait for a full byte copy of every
      stream including multi-GiB `OBJ_xfer` buckets. Verify the refutation, and that the superseded note
      is clearly marked.
- [ ] **drill 41 + the #68 five-call-site correction.** R16's A4 changed a verb's contract; the same
      defect was then found FIVE times (drills 42/92/41, the `remedy.go` SSOT, and two hand-copied
      product literals the SSOT pin structurally could not see). Judge whether the sweep is now complete.

## H3. What I did NOT prove

- [ ] **The deploy-tier acceptance claim I made was wrong and the internal review caught it.** I cited
      "drill 67's sub-face-4 gap disappeared" as the oracle. That gap is NOT unconditional — it fires
      only when the first post-grow push fails and the retry succeeds — and the recorded PRE-fix
      baseline in `expected-verdicts.tsv` is *also* `nc_gap=1 pass=17`, so my "evidence" was
      **byte-identical to the pre-fix result**. An unloaded host cannot produce the precondition at all.
      I replaced it with a POSITIVE oracle (grow → assert no `WITHOUT proving JetStream placement`
      entry in any op timeline). **Then I ran it under the saturating regime** (7 concurrent
      clustered-JS drills — the regime that produced the original "3 attempts over 8s all timed out"):
      the positive oracle PASSED, the sub-face-4 gap did NOT fire, and the whole grow family
      (10/11/42/92) came back GREEN, which is also real-pressure evidence for the `jsGateExpiryReserve`
      BLOCKER fix. **Limit, stated plainly: this is ONE-ARMED, not a differential** — building the
      pre-G69 arm needs a stash-build on a 51-changed/20-new uncommitted tree, which I judged the wrong
      risk. So it shows the fix holds under the original failing load; it cannot rule out "this run
      simply did not trigger".
      **And the same batch exposed a residual, caught by my own guard**: on run 1, drill 12's fixture hit
      `#67 residual` — the first push was refused as transient and the immediate retry ALSO failed. The
      product's wording is "retry the same command **shortly**. If it **persists**…", i.e. a short
      WINDOW, not "one retry suffices", so the fixture was over-specified against the product's own
      promise; it is now a bounded window (5 attempts / ~25s) with the product_red tooth intact and the
      attempt count logged. Run 2 under the same saturation: **3/3 fixtures succeeded on the FIRST
      attempt**, 0 product_red. **Judge whether widening that assertion after it fired is contract
      alignment or laundering** — I have argued the former and registered the residual rather than
      absorbing it (`#67` sub-face 4 says G69 narrows the window and does NOT claim it is closed).
- [x] **`make e2e` failed twice — root-caused to MY OWN TEST, now fixed.** Sequence, in full: run 1
      FAIL (evidence LOST — I had piped it through `tail -3`, my second capture mistake this session);
      run 2 PASS (I nearly filed it as the documented flake); run 3 FAIL **with the full log**, naming
      `TestG67SizingTimeoutCannotMoveTheAdmissionDecision` — a test I wrote during the G67 review to pin
      a *rejection*. It called `jsStoreCeiling` twice and demanded **byte equality** of two live statfs
      readings taken milliseconds apart; free space moved ~26 MB under the concurrent suite.
      **The G69 internal review had explicitly flagged it (G-9: "structurally flaky and reddens the
      shared gate") and I read that line and did not act on it**, judging it test-only and pre-existing.
      It then reddened the release gate twice and nearly caused a reproducible failure to be archived as
      a flake. Rewritten to assert what it always should have — that the sizing deadline cannot move the
      admission DECISION (both ceilings statfs-derived, within 2%, same verdict for a probe size; a real
      regression is an order-of-magnitude change, never a fraction of a percent). `-count=5` clean, then
      **run 4: `make e2e` exit=0, ok 634s, 0 subtest failures.**
      **Judge this**: is the tolerance-based form still a real pin, or did I weaken it to make it pass?
- [ ] Sub-face 1 (`b.js` probed once at boot, never re-probed) remains open and is the shared root of
      two further review minors.
