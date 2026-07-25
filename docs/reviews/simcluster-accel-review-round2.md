# simcluster-accel increment — internal review round 2 (unified) + main-process responses

Unified adversarial internal review of the WHOLE increment (Group A separability machinery re-verified
after Group B touched the same files; Group B = V2/V4/V5/V6/H3/D1b/D4). A 13-agent Opus-4.8 workflow:
6 review dimensions → 6 unconditional verifier lanes → 1 synthesis. **Verdict: PASS-WITH-FIXES**, every
finding CONFIRMED (nothing refuted; three severities downgraded on the evidence).

The review's framing is exactly right and worth keeping: the increment is functionally correct today (no
false green, no changed verdict/blocker/exit, all gates green, Group A intact under Group B), but the two
MAJORs were **the precise defect classes this increment was built to eliminate** — an unswept exemption
and a flake dressed as a known defect — on predicates the plan named by name. Both are now fixed and
gate-pinned. All findings dispositioned below.

Legend: **[FIXED]** implemented + pinned; **[FIXED-GATED]** implemented + a new hermetic gate pins it;
**[FIXED-DOC]** corrected in code comments/docs.

---

## MAJORS — both FIXED

**MAJOR-1 — V2's effectful-predicate exemption sweep was incomplete. [FIXED-GATED]**
V2 flipped `poll_until` fixed→fast GLOBALLY, so effectful predicates in UNMODIFIED drills silently became
fast-start. My first sweep audited only the modified drills; the review caught six missed sites,
including `_construct_111` which the plan named verbatim. Converted to `poll_until_fixed`:
`74:_construct_111` (×2 call sites), `73:_construct_nontunnel`, `73:_qconstruct`, `32:_mkbiz` (×2). The
terminal-success-only mutators `40:_ctl_write_ok` and `96:_d3_survivor_write` are documented as
deliberately left on fast (a failed tick commits nothing, the poll exits at first success). **Now
gate-enforced, not audit-enforced**: new `tests/poll-mode-test.sh` asserts every effectful/stability
predicate is on `poll_until_fixed` AND proves the fast/fixed dispatch is live (fast catches a 2nd-sample
condition in ≤2 s, fixed holds the ≥5 s grid; either mutation the review found — deleting the fast branch
or the mode guard — now fails this gate). This closes the recurrence vector: the exemption can no longer
regress silently.

**MAJOR-2 — the #34 band keyed on a harness-conflated probe. [FIXED]**
Drill 74's C-ss-pre assertion ran the raw `_ss_via_agent` (which collapses `/sub`-fetch-failed,
ss-local-never-bound, and bytes-don't-flow into one `return 1`) with no stage classification, so a
harness bind flake and a genuine #34 strand produced the same first-fail line and both matched
MATCH-BAND(#34) — a flake bankable as a confirmed product reproduction. Applied the review's fix (a), the
honest close: C-ss-pre now uses `_ss_fail_stage` exactly as the sibling B-dp arm does — a `harness-*`
stage becomes a `not_covered` gap (still gating the auto edge off), and only a product-stage strand
produces the "C-ss-pre exit … flows bytes" ASSERT-FAIL the band matches. The band now matches a real #34
reproduction, not a harness flake. Replay confirms the band still classifies MATCH-BAND(#34) and still
blocks (exit 14 unchanged); a note in `expected-verdicts-log.md` records the H10 classification.

---

## MINORS — all FIXED

- **MINOR-1 — D1b left drills 20/91 mirroring drill 42's invocation but not its post-condition. [FIXED]**
  Both retained a hand-rolled `mv /var/lib/tether/jetstream …` and omitted drill 42's product oracle. The
  `mv` was a vestigial Mandate-④ concealment: with `--reset-js` the product already moves the store to
  `jetstream.force-single-bak.<epoch>`, so the sim `mv` did nothing on the happy path but would MASK a
  future `--reset-js` regression that returned rc=0 without moving a data-bearing store. Deleted the `mv`
  and added drill 42's oracle — `ls -d /var/lib/tether/jetstream.force-single-bak.*` — so the drill now
  asserts the PRODUCT moved the store. "Mirror the GREEN drill 42" is now literally true.
- **MINOR-2 — V5's added `2>&1` blinded the M1 recorder on a drill-90 setup failure. [FIXED]**
  A fresh attribution regression I introduced, cutting against the increment's own thesis. Dropped the
  trailing `2>&1`, kept `>/dev/null` — the old per-call form preserved stderr, which `_as_capture` reads;
  each in-loop `tether` already drops its own stdout, so the success path is unchanged and the
  failure-path attribution is restored.
- **MINOR-3 — the fast-start / poll_until_fixed exemption had zero hermetic coverage. [FIXED-GATED]**
  Precisely how MAJOR-1 slipped through. Closed by the new `tests/poll-mode-test.sh` (both the exemption
  baseline and the fast/fixed behaviour probe), registered in `run-all.sh`.
- **MINOR-4 — `_bring_up_node` was dead code with a false "kept for grow" rationale. [FIXED]**
  Confirmed zero callers (grow provisions its joiner inline). Deleted, with a comment recording why.
- **MINOR-5 — V6's stale-image gate trusts an inherited `SIM_IMAGE_CHECKED`. [FIXED-DOC]**
  All intended paths stay fail-closed; only a stray `export SIM_IMAGE_CHECKED=1` / `source run-drills.sh`
  could skip the guard. Documented at the gate that the variable is run-drills-internal and must never be
  exported interactively.

---

## What the review found SOUND (retained, not re-touched)

- **V4 parallel bring-up — the safety property was PROVEN, not reasoned**: the
  `if ( _provision_node ) …; then rc=0; else rc=$?; fi` captures rc under every exit path (die, retry
  fail, SIGKILL→missing rc, rc-write failure), and the post-wait check iterates the EXPECTED node list so
  a dead node cannot be silently skipped; reproduced under bash and dash. Concurrency-race-free; Mandate-
  clean (parallel provisioning is *more* faithful to a real multi-host install; an ordering race would be
  EXPOSED as a deviation).
- **V5 drill-90 batch** byte-identical product path; **V6 check-image** fail-closed on all intended paths;
  **V2 fast-start math** correct and re-entrancy-safe, accumulator depth-0 only; **H3 drill-10
  conversions** budget- and semantics-preserving (deploy-tier green); **D1b** the `--reset-js` sweep is
  the correct fix (§2.2/R3); **D4** band mechanics sound (owner+signature bound, blast radius one
  assertion, still blocks).
- **Group A survived Group B** — no `assert_*` deleted (1342 = 1342 call sites); B1/B2/B3/B4/MA1/MA4/MA6/
  MA8 all held after Group B touched the same files; **all gate teeth intact**; exit-code law byte-stable.

## Post-fix state

`sh tests/run-all.sh` → ALL PASS (**12 gates**, incl. the new `poll-mode-test`). Archive replay flags
exactly {20,30,52,91} with 74 banded MATCH-BAND(#34), exit 14. Deploy-tier verified green across
00/10/20/90/91 before the round-2 fixes; the MINOR-1 store-oracle change (20/91) and MINOR-2 (90) are
re-validated on the real stack after this response. No verdict, blocker count, or exit code can be
changed by the match axis or an attribution label.

**Owed next:** external review (CLAUDE.md §3 step 6), then the deploy-tier phases the plan defers (D2/D3
dispositions, V7 `-j` calibration, B1 baseline) which inherently need real sweeps.
