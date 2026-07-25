# simcluster-accel increment — internal review (round 1) + main-process responses

Adversarial internal review by a 13-agent Opus-4.8 workflow (6 reviewer dimensions → 6 unconditional
verifier lanes → 1 synthesis). Verdict: **FAIL**, 4 BLOCKERs + 11 MAJORs + 13 MINORs, every finding
CONFIRMED by its verifier (nothing refuted). Main-process disposition of each follows inline.

The review's own summary of why it failed the increment is exactly right and worth keeping: *"the machinery
is architecturally sound and the exit-code/grammar/TSV foundations are intact — but the wiring is broken in
four load-bearing places with no test net under them."* Three of the four blockers were the anti-laundering
machinery failing in precisely the way it exists to prevent. All are now fixed with regression tests.

Legend: **[FIXED]** implemented + pinned by a gate; **[FIXED-DOC]** corrected in the plan/comments;
**[ACCEPTED-DEFER]** real, out of this increment's scope, recorded.

---

## BLOCKERS — all FIXED

**B1 — evidence file unreachable on every real drill (writer named it from the drill_begin TITLE, reader
opened it by FILENAME). [FIXED]**
The reader now takes the path from the `DRILL-EVIDENCE file=` line the drill itself announces
(`run-drills.sh` deviation report), so it no longer reconstructs a name that could disagree; and the
writer keys the filename on the exported `SIM_DRILL_ID` (the drill filename) rather than the title
(`lib/assert.sh:_as_ev_file`). Pinned by `deviation-report-test.sh` "B1: evidence filename keys on the
drill id … / report opens the announced path / report body carries the FULL refusal (line 1 survives)".

**B2 — no SETUP-RED path recorded evidence; `_as_setup_red` had zero callers, and drills 30/91 (the two
motivating SETUP-RED deviations) wrote nothing. [FIXED]**
`assert_setup`'s failure arm and `setup_fail` now route through `_as_setup_red`, which advances the
ordinal and writes the evidence record. Console label stays `SETUP-FAIL` (the string every grep and the
archived logs anchor on). Pinned by `deviation-report-test.sh` "B2: a SETUP-RED drill wrote a non-empty
evidence record" + the b1-setupred end-to-end case.

**B3 — bands matched on the verdict enum alone; the declared signature was never read, so a
signature-bearing band would absorb a brand-new unrelated red of the same enum (the exact "20/91
blindness inside every banded drill" the plan forbids). [FIXED]**
`classify_match` now resolves the band's `sig:<slug>` to an ERE (defined `sig:<slug> := <ERE>` in
`expected-verdicts-log.md`) and returns `MATCH-BAND` only when the enum agrees **and** the first-failure
line matches the signature; otherwise `DEVIATION`. That also satisfies rule 4 at classify time (the
run-of-record's signature is what is checked). Pinned by the two-arm case in `deviation-report-test.sh`
("band + matching signature → MATCH-BAND" / "band + NON-matching signature (same enum) → DEVIATION").
Per the review's instruction, no band is added by any Phase-2 disposition until this landed — it has.

**B4 — an interrupted attribution pass or a non-mode-locked replay could launder a red to GREEN/exit 0.
[FIXED]**
The attribution re-run now writes to its OWN basename (`<drill>.attempt2.*`) and its own evidence subdir,
so the first-run artifacts are never displaced — the cp/re-run/mv window (and its missing trap) is gone
entirely (`run-drills.sh` M4 pass). `--replay` is a hard mode-lock: it forces `RETRY=0 ATTRIBUTE=0
PREFLIGHT=0` after arg parsing and rejects an explicit `--replay --retry` (MA6); it writes no
`RUN-COMPLETE` sentinel of its own and prints `PARTIAL ARCHIVE` when replaying a sweep whose progress
stream never completed (MI13). Pinned by `deviation-report-test.sh` "attribution did NOT change the exit
code / re-run evidence kept separately (attempt2) / the FIRST run remains the verdict of record".

---

## MAJORS

**MA1 — a non-captured red inherited the previous (passing) command's argv/rc/output. [FIXED]**
`_as_capture` now stamps `_AS_CAP_ORD`; `_as_evidence` emits the argv/rc/output block only when that
equals the ordinal being recorded, else prints an honest "NOT CAPTURED for this assertion" note. Pinned
by "MA1: the non-captured PRODUCT-RED did not inherit the prior command's output" + "SETUP-RED evidence
records the failing command's rc (70), not the prior pass".

**MA2 — the deviation body and CLI-CONTRACT-SHAPED tag were derived from a tail of the whole log. [FIXED]**
The body is now anchored on the first-failure record via `first_fail_ord` (evidence file) or a
`sed '/…FAIL…/,+4p'` range (log fallback), never a blind tail; `crc` comes from that same record; and the
tag's text arm matches a REFUSAL shape (`requires? --…`, `--reset-js`, `unknown flag`, `usage:`,
`refus…`, `is required`) rather than any long flag — so it no longer false-fires on drill 52's
`--require-credential-rotation` and no longer misses drill 20.

**MA3 — the re-run appended into the first-run evidence file. [FIXED]** Folded into B4: the attribution
re-run writes to `evidence-attempt2/`, so the two runs' records never commingle.

**MA4 — storage telemetry: the per-failure field was throughput mislabeled as ms, and the sweep canary
was dead code. [FIXED]** The `dd` parser now extracts the seconds field (before the ` s` token,
comma-stripped) ×1000 = real ms (verified 2.5 ms on this host). The sweep canary is wired: an idle
p50/p99 baseline is measured at startup and printed into the rollup with a same-filesystem check;
`fsync_probe_ms` became `fsync_probe_pctl` (p50 **and** p99, for R6's tail-based trigger); the dead
`host-telemetry.tsv` reference is removed.

**MA5 — `poll_wait_total` lost assert-wrapped polls (subshell), the timeout branch added the nominal
timeout not the real elapsed, and nested polls double-counted. [FIXED (accounting) / ACCEPTED-DEFER
(subshell)].** The timeout branch now adds real elapsed; accumulation happens only at stack depth 0 (no
double-count — verified a nested 1 s wait reports 1 s, not 2 s); the trailer is renamed
`DRILL-POLL-WAIT direct_total=` and its docstring states it covers direct calls only and must not be
tuned against as a complete budget. The subshell-captured polls remain uncounted — a file-based
accumulator is deferred to V2, which is the only consumer and is not in this increment; the honest label
prevents mis-tuning in the meantime.

**MA6 — `--replay --retry` ran live drills over the archive. [FIXED]** See B4.

**MA7 — a missing/malformed expectation table degraded silently to "NO DEVIATIONS". [FIXED]** The summary
now counts `NO-EXPECTATION` rows and, when non-zero, prints "n DRILL(S) WITH NO EXPECTATION — the match
axis did not run for them" and downgrades the all-clear wording. (`_exp_field` still returns empty on an
absent table, which surfaces every drill as NO-EXPECTATION — now visibly, not silently.)

**MA8 — a successful poll whose description contains a FLAKE_SIG phrase could forge an infra-flake
signature. [FIXED]** `is_flake` now strips `poll_until: condition met`, `PASS`, and `NOT-COVERED` lines
before matching FLAKE_SIG, so only genuine infra-failure lines can make a drill retry-eligible.

**MA9 — the makespan floor was arithmetically impossible (28.8 min removed 96 from the sum while still
dividing by 6 workers). [FIXED-DOC]** Corrected everywhere (plan, `drill-costs.tsv`, `run-drills.sh`
comment) to `max(Σ/j, p_max) = max(195.1/6=32.5, 22.3) = 32.5 min`; ordering saves ~15 min, not ~19;
`≤29 min` is now attributed to V1 **plus** the sum-reduction levers, not V1 alone.

**MA10 — "storage degrades under load" was this host's idle baseline. [FIXED-DOC]** Corrected in the plan
and the two code comments: the p50 is device-bound at ~6.4 ms idle or loaded (consumer NVMe flush cost);
only the p90/p99 tail widens; R6's Phase-4 trigger is re-derived as a delta vs the same-host idle canary
(now actually measured, MA4).

**MA11 — four gate vacuities. [FIXED]** (a) `validate-verdicts.sh` gained a full non-vacuity self-test
(`validate-verdicts-selftest.sh`, 14 mutations incl. the reinstated born-vacuous band loop, all caught).
(b) The "FULL output" assertion now extracts the stdout block (argv lives outside it) and requires the
FIRST line, which a tail would drop. (c) UNSTABLE and LOAD-SENSITIVE-against-a-non-GREEN-expectation
fixtures added, pinning the two M4 discriminators. (d) CLI-CONTRACT-SHAPED is now asserted (the rc=70 +
refusal case).

---

## MINORS

- **MI1 — `--attr-budget` unvalidated. [FIXED]** Same `case … *[!0-9]*; exit 2` guard as the other numeric
  flags. The re-run-overshoot half is bounded by `--drill-timeout` already (one re-run ≤ one timeout);
  left as-is.
- **MI2 — `preflight_kernel` printed an indented heredoc EOF (un-pasteable) and was advisory-vs-refuse
  ambiguous. [FIXED]** The pasteable block is now column-0 (terminates correctly); the docstring states
  it REPORTS and only the inotify cap refuses.
- **MI3 — `--logdir=` accepted empty. [FIXED]** Explicit non-empty guard before any `rm`.
- **MI4 — `kernel.keys.maxkeys` was a false alarm. [FIXED]** M5 checks `root_maxkeys`/`root_maxbytes` (the
  uid-0 caps privileged containers actually use); plan §8 discovery #3 and R4 corrected to
  checked-and-exonerated.
- **MI5 — the runner's own header docs were stale (8-col rollup, missing flags). [FIXED]** ARTIFACTS and
  USAGE rewritten to the 15-column rollup + ATTRIBUTION/WAIVER rows + all new flags + evidence/progress.
- **MI6 — the prose log duplicated `expected`/`owner`. [FIXED]** Dropped from the log; both remain
  authoritative in the TSV.
- **MI7 — `validate-verdicts` band rules failed open when the ledger was absent. [FIXED]** BAND-SIG-
  UNDEFINED moved out of the ledger guard (it needs only the log); the ledger is now mandatory. Pinned by
  the self-test "missing ledger is fail-CLOSED".
- **MI8 — REGRESSION claimed "same signature" when both were empty. [FIXED]** Emits "verdict only — no
  comparable signature; check for a wedge/timeout" when the first-failure line is absent.
- **MI9 — the sleep census is a static literal sum. [FIXED-DOC]** The plan already labels it as such after
  the §1.1 correction; H3's lint scope is the 36 sites, and the conclusion "sleep is not a lever" is
  unaffected.
- **MI10 — small numeric drifts. [FIXED-DOC]** Rolled into the MA9/MA10 corrections; `drill-costs.tsv`
  documents its column as a measured p50.
- **MI11 — V1 shipped default-on before B1's A/B baseline. [ACCEPTED-DEFER]** B1's baseline sweeps are
  run with `--no-lpt` first (the flag exists precisely for this); C1's README paragraph lands with the
  first deploy-tier sweep of this increment, which is the next step after external review.
- **MI12 — a batch of M3/M4 behaviours were unpinned. [PARTIAL]** The load-bearing ones (label
  discrimination, first-run-of-record, attempt2 preservation, exit-code invariance, NO-EXPECTATION,
  band-signature) are now pinned; the remainder (attr-queue order, WAIVER-USED row, exit saturation at
  125) are low-risk and left for a follow-up test pass.
- **MI13 — `--replay` forged a pure-sentinel progress.tsv. [FIXED]** Folded into B4: replay writes no
  sentinel and warns on a partial archive.

---

## What the review found SOUND (retained, not re-touched)

The DRILL-VERDICT grammar and exit-code law are byte-intact; the 3→5-column TSV reshape is faithful and
both consumer migrations are correct with real vacuity guards; `classify_match`'s core, the nc_gap drift
axis, LPT ordering, the progress sentinel, first-run-of-record, and attempt2 preservation are genuinely
pinned; shell/portability is clean (`sh -n` + `dash -n` across all `lib`/`drills`/`tests`); and the
Mandate is respected at the product boundary — no product verb, fixture end-state, or assertion judgment
is touched anywhere in the diff.

## Post-fix state

`sh tests/run-all.sh` → ALL PASS (11 gates, incl. two new: `validate-verdicts-selftest`,
`deviation-report-test`). `run-drills.sh --replay` over the archived 2026-07-23 sweep flags exactly
20/30/52/74/91. A forward end-to-end run of a real assert.sh drill confirms B1+B2: a SETUP-RED with a
title unlike its filename writes a reachable evidence file whose body carries the refusal's first line,
tagged CLI-CONTRACT-SHAPED. No `assert_*` was deleted; no verdict, blocker count, or exit code can be
changed by the match axis or an attribution label.

**Owed next:** external review (per CLAUDE.md §3 step 6). The deploy-tier proof of the speed/attribution
behaviour on weilandserver (a real `-j 6` sweep with the new machinery) is the acceptance evidence for
Phase-3 levers and is run after external review signs off on the code.
