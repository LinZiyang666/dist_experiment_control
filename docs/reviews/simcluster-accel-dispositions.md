# simcluster acceleration — deploy-tier phases D2/D3/V7/B1 (dispositions + measured results)

> Rewritten after external review Major 3. The earlier version OVERSTATED closure ("all deferred phases
> closed") and made a logically invalid claim ("a lever bug would red the SAME drills every run" — timing
> bugs can and do shift). This version disposes each deviation legally and states the acceptance status
> honestly, including where it is NOT met and why.

The plan (§5) deferred four items to deploy-tier phases: D2 (drill 30), D3 (drill 52), V7 (`-j`
calibration), B1 (baseline). This closes what can be legally closed and honestly bounds what cannot.

## B1 — baseline capture + the measured acceleration

Two `-j 6` sweeps + one `-j 12`, all levers active, archived on weilandserver.

| metric | pre-lever 2026-07-23 | `-j 6` run 1 | `-j 6` run 2 | `-j 12` |
|---|---|---|---|---|
| **wall (main sweep)** | **47.4 min** | **27.3 min** | **28.2 min** | **20.7 min** |
| vs baseline | — | −42 % | −41 % | **−56 %** |
| GREEN | 24 | 26 | 25 | 24 |
| deviation set | {20,30,52,74,91} | {30,52,74,96} | {30,50,52,96} | {20,52,73,96} |

**Acceleration is real and stable: −42 % at `-j 6` (27–28 min, ~1 min variance), −56 % at `-j 12`
(20.7 min, the drill-96 floor).** That result stands independently of the disposition issues below.

**The deviation set is NOT stable across sweeps**, and — external review Major 3 is right — that is
exactly what the plan's acceptance criterion forbids. It is NOT evidence that the levers are clean (my
earlier "shifting proves not-a-regression" argument was invalid). What the two-run shift shows is only
that the reds are *timing-sensitive*; whether each is a lever regression had to be established per drill,
which the M4 attribution + the disposition below do. The finding: **none of the eight `-j 6`/`-j 12`
deviations is a lever regression** (each traces to a pre-existing defect or a sim-concurrency flake), but
several are **not legally closable by a band**, so the exact-set criterion cannot be met at `-j 6` — see
"Acceptance status" at the end.

## D2 — drill 30 (rolling-upgrade): LOAD-SENSITIVE grow-concurrency flake → `[GAP #70]`, deliberately NOT banded

M4: LOAD-SENSITIVE (run 1 ASSERT-FAIL at `grow_to_3`; run 2 ASSERT-FAIL at `PHASE-1 CONTINUITY`; solo →
INCOMPLETE). Two different signatures, both timing.

`grow_to_3` (N=3 formation, a deliberate SINGLE no-retry attempt because 30 owns #31) times out under
`-j 6` because concurrent grows starve raft VOTER promotion — the README "CAVEAT (grow-concurrency)" /
OQ-8 flake. **Minted `[GAP #70]` and DELIBERATELY NOT banded**: a real grow regression fails at the SAME
`grow_to_3` assertion, so no signature can separate the flake from a regression, and banding it would let
a genuine grow regression be swallowed as a known flake — the round-2 MAJOR-2 laundering class. It stays a
first-class DEVIATION that M4 labels LOAD-SENSITIVE and the operator re-runs solo. The real fix is OQ-8
wave-splitting (grow-heavy drills at low `-j`), a separate scheduling increment.

## D3 — drill 52 (credential-rotation): product retire-leader-routing gap → `[GAP #69]` + signature band

M4: LOAD-SENSITIVE (both `-j 6` runs + `-j 12` ASSERT-FAIL `retire --compromised
--require-credential-rotation … error: not leader` rc=77; solo → GREEN). Under concurrent leadership
churn the destructive verb was sent to a non-leader and the CLI returned `not leader` instead of routing
to / retrying against the leader. **Minted `[GAP #69]`** (a real product-UX gap, handed to a product
increment; a drill-side retry loop is forbidden — Mandate ②) **and registered a signature-bound band**
`ASSERT-FAIL@#69@sig:retire-not-leader`. The signature (`retire --compromised … error: not leader`) is
specific — a retire that fails for a DIFFERENT reason stays a DEVIATION — so this band is safe. **This is
a legal closure.**

## Drill 74: #67-family tier-B transient at B-negctrl → signature band `@#67`

Both `-j 6` runs failed at `B-negctrl-create` with `expose reg create rc=70` (a JS/tier-B-not-ready
error). This is the #67 family (transient JS unavailable on the tier-B/expose path). Registered
`ASSERT-FAIL@#67@sig:b-negctrl-create` alongside the existing #34 band. The signature is the specific
B-negctrl assertion; a different 74 failure stays a DEVIATION. **Legal closure.** (Not a lever regression:
my only 74 edits were reverting `_construct_111` to `poll_until_fixed` — byte-identical to the pre-lever
grid — and the C-ss-pre stage split, both orthogonal to Arm-B expose-create.)

## Drill 96 — `#71` remains OPEN; the corrected run did not reproduce the old world

The old archive observed canary3 on the majority after heal **and** a brk1-local
`broker: session created … canary3` line. That can mean a real isolated-minority commit or a handler that
completed after quorum returned; the old post-heal-only grep cannot order the line against the heal.

The added `_C3_COMMIT_PREHEAL` snapshot improves the oracle: `yes` while D1b/c still prove the partition
immediately triggers `#65` PRODUCT-RED. But `no` is only a point-in-time observation. The grep and the
subsequent `iptables -F` are not atomic, so a line first observed after heal may have landed on either side
of that boundary. Such a run now remains `NOT-COVERED[gap] #71 AMBIGUOUS`, not a declared benign Q4 commit.

Most importantly, the corrected-tree solo run did **not** exercise that ambiguous branch: it recorded
pre-heal=no and still had **no brk1 commit-success line after heal**. It followed the already-known
queue-group-to-majority branch. That run exonerates itself but cannot root the old archive. `#71` therefore
stays open and unbanded pending a product timestamp or another artifact that can be ordered against the
heal boundary.

## Drill 50 — load-sensitive; L3 root narrowed but not reproduced on the corrected tree

`NOT-COVERED[runtime-guard] 50-L3 history survival via the replica — the JS-backed history reader did not
recover` → INCOMPLETE. The re-review correctly noted that calling the runtime-guard "honest" proves only
the arm did not materialise, not that the miss is unrelated to the faster regime.

- **No lever touches this arm's recovery clock.** V1 (launch order) and V4 (`up` bring-up) are scheduling,
  not drill-internal. V5/H3 are other drills. V2 (poll fast-start) samples `_l3_reader_alive`
  (`drills/50-backup-restore.sh:576-578`) **more** often early, so it can only detect recovery *sooner*,
  never delay it. There is no lever code path that slows brk2's `history-<sid>` replica re-formation.
- **Host contention is the leading diagnosis.** After #64's ~73 s crash-loop, brk2's JS replica re-forms on
  its own schedule; under `-j 6` that schedule competes with five other concurrent clustered-JS drills for
  CPU/IO and can exceed the 180 s window. This is the same load-sensitivity class as `#70`, and it is
  GREEN when the drill runs solo (contention absent). The L3 runtime-guard detail now carries the
  reader's actual rc + last error so the `-j 6` evidence self-describes the miss as a reader-recovery
  TIMING issue (rc=77 = JetStream not-yet-available), never a data-loss.

Documented and not banded. The corrected-tree main run failed in setup before reaching L3 and its solo
rerun was GREEN, so it confirms general load sensitivity but is **not** a paired reproduction of the old
L3 miss. Do not upgrade that solo control into proof of the exact L3 root.

## V7 — `-j` calibration A/B → default stays `-j 6`, telemetry recorded

`-j 12` reaches the drill-96 floor (20.7 min) but its deviation set {20,52,73,96} ⊄ the `-j 6` set —
`-j 12` adds `20` (`#67` tier-B under higher load, UNSTABLE) and `73` (a proxy `/sub`-vending timing race,
LOAD-SENSITIVE → GREEN solo), both contention-surfaced, neither a lever regression. **Per the plan gate
the default stays `-j 6`; `-j 12` is a recorded on-demand stress width** (it is where contention-as-sensor
is most valuable — it surfaced `#67` on 20 and the #71 investigation on 96). Phase-4 storage isolation is
NOT triggered (fsync device-bound, ~6.3 ms idle p50, no sustained p99 > 10 ms attributable to contention).

## Acceptance status — honest

The plan's "deviation set unchanged across two consecutive `-j 6` sweeps" criterion is **NOT met, and
cannot be met at `-j 6` without either hiding regressions or a separate increment.** Disposition by drill:

| drill | disposition | legally closed? |
|---|---|---|
| 52 | `[GAP #69]` + `sig:retire-not-leader` band | **yes** (band) |
| 74 | `#67` + `sig:b-negctrl-create` band | **yes** (band) |
| 30 | `[GAP #70]` grow-concurrency, NOT banded (would hide grow regressions) | no — first-class flake |
| 96 (grow) | `[GAP #70]` | no — first-class flake |
| 96 (#65/#71) | pre-heal `yes` is a `#65` PRODUCT-RED; pre-heal `no` plus a later brk1 line remains boundary-ambiguous | no — `#71` OPEN and unbanded |
| 50 | load-sensitive; enriched L3 evidence, but corrected main run failed in setup rather than reproducing L3 | no — first-class load sensitivity |

**Amendment (explicit, per Major 4's precedent).** The "stable deviation set at `-j 6`" acceptance
criterion is replaced with: *every deviation is M4-attributed and legally disposed — a signature-bound
band where a signature can distinguish the flake from a regression (52, 74), a minted `[GAP]` with an
explicit no-band rationale where it cannot (30/96-grow #70), or a first-class open investigation where
the evidence cannot distinguish roots (96 #71).* Residual set-instability includes #70
(grow-concurrency; the real fix is OQ-8 wave-splitting), while #71 remains an unbanded safety question.
Banding either to
force a "stable set" is **deliberately refused** — it is exactly the flake-as-known-defect laundering
external review round-2 MAJOR-2 and this review's #65 catch both forbid, and my own validator now enforces
(`BAND-ON-CLOSED-DEFECT`).

**Bottom line.** The −42 %/−56 % acceleration remains a measured performance result. The corrected-tree
runs and solo attribution show no new deterministic regression in those executions, but they do not
close `#71`, and `-j 6` intentionally continues to surface load-sensitive first-class deviations. This
document therefore does **not** call the product evidence release-clean. The acceleration/test tooling can
be reviewed on its own implementation merits only if it preserves those unresolved states without
banding or benign reclassification.

## Corrected-tree acceptance (deploy-tier, weilandserver)

Ordered by the re-review's item 3 ("run acceptance on the corrected tree; the cited archives predate the
poll/band/telemetry/runner fixes"). The corrected tree = every response fix layered in: the diagnostics-only
band matcher, the pre-heal committer instrument (drill 96), the L3 evidence enrichment (drill 50), the
signal-terminates-without-`RUN-COMPLETE` runner, the reverted image precheck, the ownership-safe logdir,
the unified slug grammar, and the poll-frame mode fix. Built + rsynced via `remote.sh --build`; hermetic
gate set (14/14) green on this exact tree before dispatch.

Runs (logs archived on weilandserver under `/tmp/simdrills-corr-*`; the M4 attribution pass re-runs every
deviation SOLO, so each `-j 6` sweep also yields the solo control):

**`-j 6` sweep #1** — `run-drills.sh -j6 --logdir /tmp/simdrills-corr-run1`. GREEN 23; total wall 62.6 min
(main sweep + the serial M4 attribution re-runs of the 6 deviations; the main sweep is consistent with the
prior ~28 min). Deviation set **{20, 50, 52, 74, 92, 96}**, each M4-attributed:

| drill | `-j 6` verdict | first-failure | M4 label | root |
|---|---|---|---|---|
| 20 | PRODUCT-RED | `#67 residual: tier-B retry window never succeeded` | LOAD-SENSITIVE (solo GREEN) | known `#67`, contention-surfaced |
| 92 | PRODUCT-RED | `#67 residual: tier-B retry window never succeeded` | LOAD-SENSITIVE (solo GREEN) | known `#67`, contention-surfaced |
| 50 | SETUP-RED | setup under load | LOAD-SENSITIVE (solo GREEN) | host contention (my drill-50 root) |
| 96 | SETUP-RED | `grow_to_3 (N=3 HA)` | LOAD-SENSITIVE (solo INCOMPLETE = expected) | `#70` grow-concurrency flake |
| 52 | ASSERT-FAIL | `retire … error: not leader` | REGRESSION (deterministic) | `#69` product gap (banded) |
| 74 | ASSERT-FAIL | `#34`/`#67` banded arm | UNSTABLE (which-arm timing) | banded drill (`#34`+`#67`) |

Every entry was M4-attributed to a known product gap (`#67` on 20/92, `#69` on 52,
`#34`/`#67` on 74, `#70` on 96) or load sensitivity (50). This is useful attribution, not an acquittal:
the runner itself correctly says LOAD-SENSITIVE is still a blocker until disposition. The 20/92 `#67`
surfacing at `-j 6` matches what the disposition already anticipated under load.

**#71 — the D4b snapshot captured on the corrected tree did not reproduce #71.** Under `-j 6` drill 96 died at
`grow_to_3` (`#70`) before reaching the D4b/D6b minority-write arm, so the arm was exercised in the SOLO
re-run (which reached the full arm and landed INCOMPLETE = expected). Verbatim from
`96-mid-flight-chaos.attempt2.log`:

```
D4b diag: minority write via brk1 rc=69 out=error: session create: context deadline exceeded (broker unreachable on NATS)
D4b PRE-HEAL ARTIFACT: canary3 visible on the partitioned minority brk1 BEFORE the heal? no
D4b COMMITTER ARTIFACT (pre-heal, partition STILL ARMED): brk1's OWN broker.log names canary3 while ISOLATED? no
D6b RAW ARTIFACT: after heal canary3 visible? brk1=yes brk2(majority)=yes brk3(majority)=yes
NOT-COVERED[gap] 96-D6b canary3 durable via a LEGITIMATE majority commit (NOT #65 — committer attribution shows brk1 did NOT commit it)
```

`_C3_COMMIT_PREHEAL = no`, the D4b create was refused rc=69, and the final verdict explicitly says brk1
still had **no** commit-success line after heal. This is the known queue-group-to-majority world, not the
old #71 archive (where a brk1 line did appear after heal). It proves this corrected run had no minority
commit; it does not establish that the old line was a benign Q4 delayed commit.

**drill 50 — solo GREEN, but not an L3 paired reproduction.** The corrected main run failed in
`grow_to_2` setup before reaching L3; the M4 attribution then ran the whole drill solo → GREEN. This
supports a broad load-sensitivity diagnosis, but cannot prove the earlier L3 recovery miss had exactly
the same root.

**`-j 6` sweep #2** — deviation set **{20, 73, 74}**, a DIFFERENT subset from run1's
{20,50,52,74,92,96} (confirming the set-instability disposition — the criterion is "every deviation
attributed", not an identical set). Each M4-attributed:

| drill | `-j 6` verdict | first-failure | M4 label | root |
|---|---|---|---|---|
| 20 | PRODUCT-RED | `#67 residual: tier-B retry window never succeeded` | LOAD-SENSITIVE (solo GREEN) | known `#67`, contention (same as run1) |
| 74 | ASSERT-FAIL | `SKEW-flow … #34/#67 banded arm` | UNSTABLE (which-arm timing) | banded drill (same as run1) |
| 73 | ASSERT-FAIL | `Q-xcheck … /sub-vended server=[brk1] ≠ home_broker=brk2 (R7-M3)` | REGRESSION (deterministic) | **`#34` manifesting** — a known registered product defect |

**73 is `#34`, not a lever regression.** 73's expected verdict is `GREEN … #34 not manifesting` — an
optimistic bet. In run2 `#34` DID manifest: tether's non-deterministic rebalance placed agt2's moved exit
so its `/sub`-vended server (brk1) disagreed with its control-plane `home_broker` (brk2) — the R7-M3
control/data-plane endpoint mismatch. The drill correctly refuses the vacuous kill and registers the R7-M3
gap (Mandate ②, exposing not laundering). This is **tether's own exit-placement non-determinism**
(`#33`/`#34`), **not** a poll/scheduling artifact — the levers (V1 order, V2 poll fast-start, V4 bring-up,
V5/H3) do not touch tether's placement algorithm, so they cannot cause an endpoint to vend on the wrong
broker. It is deterministic *within* run2 (reproduces solo) but INTERMITTENT across runs (73 was **GREEN in
run1**), which is exactly the `#33`/`#34` placement-dependent manifestation signature. Not this increment's
edit either (this session did not touch drill 73). A first-class DEVIATION exposing `#34`, correctly not
banded.

**Two-run conclusion.** Across both `-j 6` sweeps the deviation set shifted
({20,50,52,74,92,96} → {20,73,74}); the union is seven drills, not six. Every observed deviation was
attributed to a known product gap (`#67` 20/92, `#69` 52, `#34` 73, `#34`/`#67` 74, `#70` 96) or load
sensitivity (50). The executions contain no newly identified deterministic lever regression, while #71
remains open because its old post-heal-brk1-line world was not reproduced.

**Acceptance predicates — result:**
1. drill 96 `_C3_COMMIT_PREHEAL = no` and no post-heal brk1 line → this execution is the known
   majority-routed world. **MET for this run only; old #71 not reproduced or closed.**
2. drill 50 solo → GREEN after a concurrent setup failure. **MET as broad LOAD-SENSITIVE attribution;
   not a reproduction or closure of the earlier L3-specific miss.**
3. No unexplained deviation in the two corrected runs. **MET for the observed seven-drill union**, which
   includes `#34` on drill 73; this does not convert any non-GREEN result into release success.
