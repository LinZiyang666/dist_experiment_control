# simcluster acceleration — response to external re-review (round 2, Fail)

> ⚠ **PARTIALLY SUPERSEDED by `simcluster-accel-final-external-review.md` (round 3, Pass).** This document
> is kept as the round-2 record. Two of its claims were **retracted** by the final review's "Closed 6" and
> are corrected in place below (Major 3): `#71` is **NOT rooted and remains OPEN**, and drill 50's L3 miss
> has **no established causal root**. The final review also found four deterministic defects in the
> implementation this document describes (stale cause authorization, orphaned signal descendants, replay
> ownership blessing, zero-completion telemetry); those were fixed by the reviewer and are pinned by
> `tests/simcluster-accel-final-review-test.sh`. Read the final review for the current state; read
> `simcluster-accel-dispositions.md` for the corrected acceptance record.

> Developer response to `simcluster-accel-external-rereview.md` (Fail). Each finding is answered with the
> concrete fix, the file/line, and the gate that now pins it. The re-review's own reproduction test
> (`tests/simcluster-accel-external-rereview-test.sh`, 5 deterministic RED cases) is wired into the
> permanent hermetic gate set (`tests/run-all.sh`) and now passes 5/5 — the same adversarial cases that
> were RED at review time are the regression proof that they stay closed.

## Verification snapshot (corrected tree)

- Hermetic gate set `sh tests/run-all.sh` — **14/14** (the two reviewer reproduction files are now gates).
- `make lint` — **0 issues**. `make test` — **PASS**. `make e2e` — **PASS** (650.7 s, all matrices
  including `TestD7Matrix` 11.95 s — the exact case the re-review saw fail was a load flake; clean on this
  tree).
- Deploy-tier corrected-tree acceptance — **two `-j 6` sweeps** done on weilandserver. Deviation sets
  {20,50,52,74,92,96} (run1) and {20,73,74} (run2) — a shifting subset, union **seven** drills. Every
  deviation in both runs was M4-attributed to a known product gap (`#67`, `#69`, `#34`, `#70`) or to load
  sensitivity (50); no deterministic lever regression was identified in those executions. ⚠ **Corrected by
  the final review:** attribution is not acquittal — a LOAD-SENSITIVE label remains a blocker until
  disposed, and the `#71` snapshot (`_C3_COMMIT_PREHEAL = no`, D4b create refused rc=69) proves only that
  *this* run had no minority commit; it did **not** reproduce the old post-heal-brk1-line world and does
  **not** close `#71`. drill 50's concurrent run failed in `grow_to_2` setup before reaching L3, so its
  solo GREEN is broad load-sensitivity evidence, not a paired L3 reproduction. Corrected tables + verbatim
  logs in `simcluster-accel-dispositions.md` ("Corrected-tree acceptance").

## Major 1 — the `#67` band matched an assertion title, not the root cause → FIXED (root-bound)

The band matcher now resolves a signature against **CAUSE diagnostics only**, never the `[err]` assertion
title. `_fail_context` (`run-drills.sh`) returns the `[simcluster]`/`[warn]` diagnostic lines that precede
the first `[err]` line (colour-stripped); `classify_match` matches a band's `sig:` ERE against **that**
context, so a title-only red at the same assertion can no longer be laundered into `MATCH-BAND`. All three
live signatures were repointed at cause lines:

- `sig:retire-not-leader := 52 D-spine: retire .* error: not leader` — matched against a `[simcluster]`
  cause line drill 52 now logs after the retire (`drills/52-credential-rotation.sh`).
- `sig:c-ss-preflow := poll_until: timed out .* C pre-kill SS flows via`
- `sig:b-negctrl-create := negative-control expose reg create rc=70`

The re-review's mutation (rc=64/permission-denied at the same assertion) now classifies **DEVIATION**, not
`MATCH-BAND(#67)` — it is case 1 of `simcluster-accel-external-rereview-test.sh`, which passes. The
synthetic band fixture in `deviation-report-test.sh` was updated to emit its signature as a `[simcluster]`
cause line (mirroring a real drill), and the gate stays green.

## Major 2 — TERM/HUP/INT could forge `RUN-COMPLETE`; `running_drills` telemetry was wrong → FIXED

- **Signals now terminate the partial sweep before any sentinel.** `_on_signal()` kills the sampler,
  kills every tracked drill child (`_drill_pids`), prints an interrupted notice, and `exit`s with the
  signal-derived status (INT→130, TERM→143, HUP→129) — so control never reaches the summary that writes
  `RUN-COMPLETE`. Re-review reproduction case 5 (start a 5 s drill, TERM after 1 s, assert no
  `RUN-COMPLETE`) passes.
- **`running_drills` is now genuinely "in-flight".** The sampler computes `launched − completed`
  (`_drill_pids` launched minus `*.rc` files), not the cumulative `*.rc` count. It rises and falls with
  actual concurrency instead of monotonically increasing.

## Major 3 — the acceptance story claimed more than the evidence → PARTIALLY FIXED; two claims RETRACTED

⚠ **This section's original conclusion ("both reds rooted") was RETRACTED by the final external review
("Closed 6").** What survives is the added instrumentation and the fresh corrected-tree sweeps; what does
**not** survive is the causal closure I inferred from them. Corrected:

- **`#71` remains OPEN — not rooted.** The added pre-heal snapshot (`_C3_COMMIT_PREHEAL`, a `dexec` local
  grep that runs while the partition is still armed) is a genuine improvement: a `yes` while D1b/c still
  prove isolation fires `#65` PRODUCT-RED directly. But a `no` is only a point-in-time observation — the
  snapshot and the later `iptables -F` are **not an atomic boundary**, so a brk1 line first *seen* after
  heal may have landed on either side of it. My source argument (`sessions.go:77` +
  `clusterwrite.go:694-730`: a partitioned minority cannot commit) establishes the *intended invariant*;
  it is not an attribution for an observation that appears to violate it. Worse, the corrected-tree run
  never exercised that branch at all — it had **no** brk1 line even post-heal — so it cannot explain the
  old archive. Drill 96 now records that world as `NOT-COVERED[gap] #71 AMBIGUOUS` and `#71` stays OPEN
  and unbanded. (`_c3_committed_by`'s doc was also corrected: the broker log is a handler-side
  commit-success witness, not proof that that broker was the raft leader/committer.)
- **drill 50 has no established causal root.** The lever-independence argument stands (no lever code path
  slows brk2's `history-<sid>` replica re-formation; V2 poll fast-start detects recovery *sooner*, never
  later). The **contention** conclusion does not: the corrected concurrent run failed in `grow_to_2` setup
  **before reaching L3**, so its solo GREEN is broad load-sensitivity evidence, not a paired reproduction
  of the L3 miss. The drill's runtime-guard text no longer asserts that root; it records the observed
  state and preserves the guard until a paired concurrent/solo L3 reproduction distinguishes host
  contention from a product recovery defect.

The disposition no longer claims "clean of lever regressions" as an assertion resting on an amended
criterion. What the two corrected-tree `-j 6` sweeps actually support (final-review wording): every
deviation across both runs ({20,50,52,74,92,96} and {20,73,74}, union **seven** drills) was M4-attributed
to a known product gap (`#67`/`#69`/`#34`/`#70`) or to load sensitivity (50), and **no new deterministic
lever regression was identified in those executions** — which is not the same as declaring the product
evidence release-clean, and does not close `#71`. run2's drill 73 (`#34` manifesting, the R7-M3 endpoint
mismatch) does demonstrate that the pipeline exposes rather than launders a product defect, and that the
root-bound band (Major 1) correctly leaves 74 UNSTABLE instead of absorbing an off-arm failure.

## Medium 4 — `--image-prechecked` was a public forgeable bypass → FIXED (removed)

The `--image-prechecked` fast-path is **removed**. `cmd_drill` always calls `check_image_or_die`, which
compares the image's baked `tether` sha against the staged `vendor/tether` sha inside the container. A
stray `--image-prechecked` argument is parsed off and **silently ignored**. The one-per-sweep optimization
now lives in the runner as a single real `check-image` pre-abort before the launch loop — provenance is the
actual image inspection, not a public hash a caller can echo back. Re-review case 2 (forge the flag with an
impossible vendor binary) now fails to bypass. _(Confirmed live: the first corrected-tree launch aborted on
a genuinely stale image — the pre-abort is not decorative.)_

## Medium 5 — a non-existent `--logdir` alias could delete a live parent → FIXED

`run-drills.sh` now: (1) rejects any `--logdir` containing a `..` component (an alias whose real target
differs from its spelling) **before** any filesystem action; (2) `mkdir -p`s then canonicalizes with
`cd … && pwd -P`, using the resolved path thereafter; (3) refuses system roots, `$HOME`, and the source
dir; (4) requires a runner-written `.simdrills-owned` marker in the directory before the destructive
startup cleanup runs, so pointing `--logdir` at an arbitrary populated directory is refused. Re-review case
3 (`victim/new/..` with a `victim/keep.log`) no longer deletes `keep.log`.

## Medium 6 — validator and runtime did not share one slug grammar → FIXED (one literal grammar)

Both the validator (`tests/validate-verdicts.sh`) and the runtime resolver (`run-drills.sh`) now enforce
the same literal safe slug grammar `[A-Za-z0-9][A-Za-z0-9._-]*` via `_sig_regex`'s reject clause
(`case "$1" in *[!A-Za-z0-9._-]*|'') return 0 ;; [!A-Za-z0-9]*) return 0 ;; esac`). A definition like
`sig:x/y := …` is now rejected by **both** — the validator no longer accepts a slug the runtime sed cannot
resolve. `BAND-SIG-BADSLUG` in the validator self-test and re-review case 4 pin the metacharacter
rejection.

## Low 7 — signal/redaction hardening was improved but not complete → ADDRESSED

- **`cmd_up` INT/TERM** now stops provisioning and exits with the signal status (INT→130, TERM→143) after
  removing its `mktemp -d` scratch, rather than removing the directory and returning. Background
  provisioners are reaped through the tracked `_up_pids`.
- **`_as_redact`** now masks lower-case URI query material in addition to flags and `KEY=` assignments:
  `[?&](pin|token|secret|password|pass)=[^ &]+` is masked, so a captured `tether-invite:v1?pin=…` (drill 82)
  is redacted. The "fixed" claim is narrowed to exactly what the tests exercise (spaced/equal/quoted
  secrets, multiline, and now lower-case query material).

## Residuals stated honestly

- `#70` (grow-concurrency flake) stays a first-class DEVIATION, deliberately NOT banded — a real grow
  regression would fail at the same `grow_to_3` assertion, so banding it would launder regressions. The
  real fix is OQ-8 wave-splitting (a separate scheduling increment). This is the one residual set-instability.
- The genuine minority-stale-write `#65` variant remains structurally unreachable in-sim (condition Y: a
  long-lived pre-partition-authenticated client the CLI cannot provide) and stays a product `[GAP #71]`.
  The pre-heal instrument covers the reachable path.
