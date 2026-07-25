# simcluster acceleration & concurrency-stability — PLAN (main-process final)

> Leaf increment (post-1.0), per CLAUDE.md §2/§3. Stage A step 2: the main process is the sole author of
> record for this file. Drafted by a 13-agent adversarial workflow (6 drafters × 6 fixed critic lenses →
> 1 synthesis, Fable 5); every ruling below is the main process's, and where the candidate and the main
> process disagree the ruling text says so explicitly.
>
> Scope: `test/simcluster/` only (a dev tool — no Go `_test.go`, not in `go test`, not in CI). No tether
> product code is changed by this increment; the product work it uncovered is handed off in §6.

## 1. What was measured

One instrumented full sweep, `./remote.sh drill-all -j 6`, weilandserver (88 vCPU / 251 GB), 2026-07-23
10:02–10:49, on an image built from `c6b9c9e`.

```
38 drills: GREEN=24  PRODUCT-RED=0  INCOMPLETE=9  SETUP-RED=2  ASSERT-FAIL=3  INFRA-ABORT=0  (2844s)
14 BLOCKER(S)
```

- **Wall 47.4 min; sum of per-drill durations 195.2 min; mean 5.1 min; speedup only 4.1× at `-j 6`.**
- **The longest drill, `96-mid-flight-chaos`, is 22.2 min — and it was launched 37th of 38, at minute 25**
  because the launch order is lexicographic. The true parallel floor at `-j 6` is
  `max(Σ/j, p_max) = max(195.1/6 = 32.5, 22.3) = 32.5 min` (greedy longest-first reaches ~32.7).
  **~15 minutes are lost to launch ordering alone** — the alphabetical tail ran one long drill nearly
  solo. (Internal review MA9 corrected an earlier "→ 28.8 min / ~19 min": that removed 96 from the sum
  while still dividing by all 6 workers, one of which runs 96 — below the arithmetic minimum. ≤29 min is
  reachable only with V1 **plus** the sum-reduction levers, not by ordering alone.)
- **22.2 min is the hard floor** at any concurrency: it is one drill. Nothing but changing drill 96
  itself can go below it.
- **The host is not the bottleneck.** Under 33 concurrent containers: load 3.55/88, CPU 98 % idle,
  iowait 0, no steal, disk 11 % used.
- **Storage: the p50 is device-bound, only the tail moves under load.** The 4 KiB fdatasync p50 is
  ~6.4 ms whether the host is idle or under 33 containers (a consumer NVMe's flush cost, not contention —
  internal review MA10 corrected an earlier "measured to degrade under load"); what widens under load is
  the p90/p99 tail (idle p99 ≈ 6.5 ms sustained; loaded p90 11.7 / p99 13.0 / max 14.0 ms). Raft commit,
  SQLite WAL and the JetStream store all sit on this one shared path. R6's Phase-4 trigger is therefore
  re-derived as a **delta vs the same-host idle canary**, not an absolute threshold. `/tmp` (`$LOGDIR`)
  and `/var/lib/docker` are the **same ext4 LV**
  (`/dev/mapper/ubuntu--vg-ubuntu--lv`, verified live), so a canary written from `$LOGDIR` is
  representative of what the drills actually experience.
- **Five deviations from `expected-verdicts.tsv`**, and they are not one phenomenon (§2).

### 1.1 Correction of record: the "51 minutes of sleep" figure was wrong

An earlier count reported "42 blocking `sleep` calls totalling 3066 s (~51 min)" and the candidate plan
built its V3 lever (claimed "~25–30 min off the sum") on it. **That count was wrong**, and the error is
the same class this project has been bitten by before: it swept in strings that are not waits —
`tether exec … -- sleep 9663` (held-foreground seed processes that are the *subject* of `ps` assertions),
`sleep 999999` / `sleep 5117` / `sleep 987654` (managed child processes under test), and
`jq … test("sleep 966[12]")` selector literals.

Recount, excluding fixture payloads (`tether exec`, `_seed_held`, `jq test(`, `-- sleep`) and bounded to
≤3 digits:

```
36 calls, 175 s total (2.9 min) across the WHOLE suite
 73 s  22-forcesingle-online     18 s  93-metrics-observability     15 s  10-grow-to-3
 13 s  72-proxy-subscription      8 s  74-rebalance-on-return        8 s  42-rejoin-returning
 (everything else ≤ 6 s)
```

**Blocking `sleep` is 2.9 min of a 47.4 min suite. It is not a lever.** Ruling R1 below acts on this.

### 1.2 The suite cannot tell you where its own time goes

Drill logs carry no per-line timestamps and `poll_until` never records how long it actually waited. 96
took 22.2 minutes and there is no way, after the fact, to attribute that to any assertion or poll.
Every tuning proposal made before this is fixed is guesswork — which is precisely how §1.1 happened.
**Instrumentation is therefore a precondition, not an optimisation** (Phase 1).

Two things make this cheap, both established by reading the code:

- `run_one()` (`run-drills.sh:167`) **already computes `t0` and the elapsed time** for its
  `--drill-timeout` classification, and then throws it away. Per-drill duration is free.
- `poll_until` (`lib/log.sh:66`) already maintains an explicit **frame stack** (`_pu_push`/`_pu_peek`/
  `_pu_pop`, added for the R1/H5 re-entrancy fix). Recording a start epoch in the frame and emitting an
  elapsed record on pop instruments **all 315 call sites through one function**.

## 2. The five deviations — attributed

| drill | got | expected | attribution | class |
|---|---|---|---|---|
| 20-forcesingle-natsconf | ASSERT-FAIL | GREEN | `c6b9c9e` added a mandatory `--reset-js` gate to `cluster recovery force-single` on a non-empty JS store (`cmd/tether/cluster_offline.go:244,294`); the drill still calls the bare verb → rc=1 | **REAL PRODUCT REGRESSION** |
| 91-client-converge | SETUP-RED | GREEN | the same gate, rc=70 | **REAL PRODUCT REGRESSION** |
| 30-rolling-upgrade | SETUP-RED | INCOMPLETE | `poll_until: timed out after 90s waiting for: brk2 broker back up after declaring colocated agent colo-brk2` | timing / slow-start, **unattributed** |
| 52-credential-rotation | ASSERT-FAIL | GREEN | `retire --compromised --require-credential-rotation` → rc=77 `error: not leader` | leadership churn; also a product-UX question |
| 74-rebalance-on-return | ASSERT-FAIL | INCOMPLETE (known band) | `C-ss-pre exit agt2 flows bytes over the POST-RESTART baseline`, a per-nid sequential probe the drill itself documents as "~90s each" | timing / bandwidth |

**Two of five are not concurrency at all — they are a regression from the commit that had just been
pushed, and it is the sixth recurrence of the recorded "a contract change must sweep every call site"
class.** Attribution cost a human evening of solo re-runs.

### 2.1 Why they were nearly laundered — the mechanism, not the diligence

`run-drills.sh` **never reads `expected-verdicts.tsv`** (verified: the file's only consumers are
`tests/ledger-crosscheck.sh` and `tests/r16-g67-g69-external-rereview.sh`). The runner fails closed on
every non-GREEN verdict and printed `14 BLOCKER(S)` — but **9 of those 14 are recorded, owned,
deliberate INCOMPLETEs**. To find the 5 rows that actually deviate, an operator must hand-diff a 38-row
rollup against a 60-line prose TSV whose rows run to 4,800 characters.

In a wall of 14 blockers, two new red rows are indistinguishable from background. **The failure is
mechanical, and so is the fix**: the runner already parses every counter, and the TSV already has the
columns (`drill / expected / owner / batch / note`). This single gap is the strongest justification for
ordering separability ahead of speed.

### 2.2 Correction of record: drill 20 is NOT evidence of a non-atomic partial-application bug

An earlier main-process claim — that the `--reset-js` refusal fires *after* `nats.conf` has been
rewritten and therefore constitutes an unintended non-atomic partial application leaving the node at
`conf=standalone / store=clustered` — **is withdrawn.** Reading `cmd/tether/cluster_offline.go:202–250`:

1. `clusteroffline.ForceSingle(...)` commits the raft/DB phases — **irreversibly**;
2. the conf is then de-clustered (`:214`) with an explicit comment that this *must* happen, because the
   raft/DB phases are already committed and a clustered conf at N=1 makes the node unbootable (exit 70);
3. only then is `resetForceSingleJSStore` reached (`:244`), gated by `--reset-js`;
4. the recovery journal is cleared last, "the first moment the sequence is truly complete".

The intermediate state is **designed, journalled, and forward-completing** — the refusal text tells the
operator to "re-run this exact force-single command with `--reset-js` (the peers you confirmed dead are
journalled)". Drill 20's next assertion passing is the sequence working as specified, not a bug.

**The real product finding, which stands and is sharper: the `--reset-js` gate is evaluated *after* the
irreversible commit.** An operator who forgets the flag is refused only once raft, the DB and the conf
have all been mutated. It should be a **preflight** — the store's emptiness is discoverable from
`nats.conf` before `ForceSingle()` touches anything, so the refusal can and should come first. Handed
off in §6.

## 3. Non-goals — settled, do not re-litigate

- **Any accelerated-clock abstraction layer.** `libfaketime`/LD_PRELOAD cannot work (Go reads the clock
  via vDSO, bypassing libc; tether *and* nats-server are both Go). Linux time namespaces
  (`CLONE_NEWTIME`) provide an *offset* for `CLOCK_MONOTONIC`/`BOOTTIME` only — no rate multiplier, and
  they cannot touch `CLOCK_REALTIME`; using them would split the monotonic and realtime clocks (raft uses
  one, SQLite timestamps the other) and invent a new failure mode. A deterministic simulator
  (Shadow / FoundationDB class) *would* deliver the speedup, but only by replacing the OS, the network
  and the disk — i.e. by deleting real systemd, real `install.sh`, the real out-of-process nats-server
  and real fsync, which are the four things simcluster exists to exercise (Mandate; judgment inversion).
- **A fast-clock knob in the tether product.** Test-only product seams have already been rejected once by
  external review: `xfer_cross_home_reap_age` was clamped to a ≥15 min floor (F2) rather than allowed to
  compress for observability. The same rule binds here.
- **A sixth landing verdict, or any drill-side "this was the environment" self-classification.** The
  5-verdict enum is a parsed contract in three places. Attribution is a **runner-computed second axis**,
  never a landing verdict — an `ENV-RED` label is a laundering vector by construction.
- **Widening `FLAKE_SIG`, auto-retrying assert-level reds, verdict substitution by re-run, or a
  green-on-retry exit code.** This run is the proof: 20/91 would have been retried into a footnote.
- **tmpfs for any store whose drill asserts survival across a node stop/kill/start, or real durability/
  latency semantics** (96/#57 and every successor, permanently). The `--cap-store` sizing drills (21, 90)
  stay legitimate. Honest limit on the record: `docker kill` never drops the host page cache, so true
  power-loss durability is out of simcluster's reach in *any* configuration.
- **userns-remap.** It breaks `--privileged` + systemd and destroys `install.sh`'s real ownership
  semantics (Mandate ①). Per-uid caps are checked instead.
- **Migrating drill assertions into the Go tier as part of this increment**, and the tier-boundary
  doctrine that would license it — see ruling R8.

## 4. Rulings on the candidate plan

| # | Ruling |
|---|---|
| **R1** | **V3 ("blocking-sleep audit", claimed 25–30 min) is REJECTED as a speed lever.** Its premise — a 3,066 s sleep budget — is measurement error (§1.1); the true figure is 175 s. It survives only as a **hygiene item, folded into H3**: classify the 36 real sites and add a lint tag so an untagged bare blocking sleep fails `tests/lint-drills.sh`. Claimed saving: **≤2 min, and that is the ceiling, not the target.** One critic lens half-caught this ("the per-drill concentration counted workload sleeps") but still treated 3,066 s as the SSOT; the correction is the main process's. |
| **R2** | **P0.2 is discharged: 96 measured at 22.2 min**, confirming the stale `run-drills.sh:62` comment. Every floor in this plan is restated from the measured value. The candidate's conditional accounting is replaced by the numbers in §1. |
| **R3** | **The "non-atomic partial application" framing is withdrawn** (§2.2). The candidate's §5 hand-off — "move the `--reset-js` gate to a preflight *before any mutation*" — is right in its conclusion but wrong in its stated reason, and as literally worded it is impossible: the conf rewrite cannot be moved after the gate, because the raft/DB commit precedes it and skipping the rewrite bricks the node. The hand-off is restated correctly in §6. |
| **R4** | **Phase 4's runtime-`sudo` storage provisioning is confirmed impossible on this host** — `sudo -n` fails (`sudo: a password is required`, verified live). M5 is therefore **check-and-refuse only**; the auto-raise arm survives solely as best-effort for hosts with NOPASSWD. `fio` is present, `ioping` is absent — the canary uses `fio`/`dd`. (Correction, internal review MI4: the keyring cap the drills actually draw on is `kernel.keys.root_maxkeys` — privileged containers run PID1 as host **uid 0** — which is `1000000`, not the non-root `maxkeys=200`. M5 checks `root_maxkeys`/`root_maxbytes`; the keyring is checked-and-exonerated on this host. The inotify cap remains the one real per-uid limit, with no uid-0 exemption.) |
| **R5** | **Two near-free wins the candidate did not identify are promoted into Phase 1/2**: `run_one()` already computes per-drill elapsed and discards it; `poll_until`'s existing frame stack makes 315 call sites instrumentable through one function (§1.2). |
| **R6** | **Ruling on open question 1 (storage isolation)** — DEFER behind the Phase-4 trigger, as the candidate proposes. Trigger ratified as: *fsync canary p99 sustained > 10 ms at the adopted width* **and** *≥2 deviations attributed by written disposition to drill-oracle budgets under shared fsync rather than to product findings*. One-time interactive `sudo -S` provisioning on weilandserver is **acceptable in principle** but is explicitly **not on this increment's critical path**. |
| **R7** | **Open question 2 (where to stop)** — **wall ≈ 22.2 min is the accepted end-state for this increment.** simcluster is an on-demand gate, not CI. Opening drill 96's internals is wave-2 and this plan claims nothing from it. |
| **R8** | **Open question 6 (tier-boundary doctrine)** — NOT adopted, agreeing with the candidate. The drill-22 case is the reason: `TestForceSingleArmToken` covers injected-clock expiry, yet deleting drill 22's real-clock arm would still remove the only real-stack wiring probe on that path. A Go successor does not imply equivalent coverage (state machine ≠ wiring). Any future doctrine needs its own ruling and a deletion gate requiring *both* a Go successor *and* a named retained real-stack probe. |
| **R9** | **Open question 3 (iso scheduling class)** — specify now, build on first admission. No candidate qualifies under its own admission rule today (52 and 74 need dispositions first). Confirmed: **do not build it in this increment.** |
| **R10** | **Open question 4 (tsv split depth)** — **adopt the full split** (machine table + prose ledger + validator + same-commit consumer migration). Rationale: this is the increment's own contract-change moment, and the defect it exists to catch is precisely an unswept contract change. Doing it *with* a validator and a proven consumer sweep is the point, not overhead. |
| **R11** | **Open question 5 (M4 defaults)** — ratified as proposed: 60-minute attribution budget, expected-GREEN deviations first. With 5 deviations at a 5.1 min mean this is generous, which is correct for a phase whose failure mode is dropping a real regression (a count-based cap of 4 would have dropped 91 — a real regression — in this very run). |

## 5. The plan

Ordering is load-bearing: **separability → dispositions → speed → isolation (conditional).** No speed
lever may land before the machinery that would notice it laundering a red.

### Phase 0 — Preserve (perishable, do first)

**P0.1 Archive `/tmp/simdrills` from weilandserver.** `run-drills.sh:155` truncates `$LOGDIR` on the next
sweep, and this run's artifacts are the replay fixture for every Phase-1 acceptance check. Copy to
durable storage on the server *and* rsync a copy to WSL.
*Acceptance*: the archive holds all 38 `.log`/`.rc` pairs plus `rollup.txt`/`rollup.tsv`. *Cost*: minutes.

**P0.2 — DISCHARGED (R2).** 96 = 22.2 min.

### Phase 1 — Separability machinery

**M1 Failure-time evidence capture in `lib/assert.sh`.**
On `_as_fail` / `_as_setup_red` / `_as_misuse` / `_as_product_red`, append to
`$SIM_EVIDENCE_DIR/<drill>.evidence`: ISO timestamp, assertion ordinal + description, full argv, rc, and
the **full** `_AS_OUT` — today `tail -3`/`tail -5` (assert.sh:141/152/156/170/175) truncates exactly the
refusal text that attributes a red — plus `/proc/loadavg`, a 4 KiB `dd conv=fdatasync` probe (ms),
`docker ps` filtered to `$INSTANCE`, and free disk. Host-level items once per drill, per-assertion items
on every failure. Every probe under `timeout 10 … || true`. Capture must never touch counters or rc.
`drill_end` appends one `DRILL-EVIDENCE file=<path> first_fail_ord=<n>` line — **an appended line, never
a change to the `DRILL-VERDICT` grammar** (assert.sh:292).
*Effect*: drill 52's `not leader` stderr and drill 30's OQ-6 context become first-run facts instead of a
5.1-min solo re-run plus human time.
*Masking risk + pin*: a wedged probe could hang a red → the timeouts above; new
`tests/verdict-contract-test.sh` case with `SIM_EVIDENCE_DIR=/proc` (unwritable) must produce a
**byte-identical** verdict line. *Cost*: ~80 lines sh; +1–2 s per failing drill, zero on green paths.

**M2 Split `expected-verdicts.tsv` into a machine table + a prose ledger.** (R10)
Today one file mixes expectation with changelog — line 39 is 4,826 characters. Split into:
- `expected-verdicts.tsv`: `drill  expected  expected_nc_gap  bands  owner  note-ref`. **`owner` is
  retained** — `tests/ledger-crosscheck.sh:25,77` resolves gotcha ownership through non-GREEN owner cells
  and must keep working. `bands` is a comma list of `VERDICT@#NN@sig:<slug>`; a band **must** name an open
  defect and a normalised first-failing-assertion signature. **Verdict-enum-only bands are invalid** — a
  band matching "any ASSERT-FAIL in this drill" would recreate the 20/91 blindness inside every banded
  drill.
- `expected-verdicts-log.md`: the prose histories moved verbatim, keyed by drill; signature slugs defined
  here.
A `tests/` validator rejects malformed rows, unknown enums, signature-less or owner-less bands, drills on
disk missing from the table, and bands whose `#NN` the gotcha ledger records as closed. **Same commit**:
migrate both existing consumers.
*Masking risk + pin*: a band is a pre-authorised red, so the laundering temptation concentrates here.
Banded reds **still block** (reported as `BANDED-RED=n`, never folded into GREEN); a *different* red in a
banded drill is a DEVIATION; deleting the band is obligatory when `#NN` closes.
*Acceptance*: validator green **and `sh tests/run-all.sh` green** — proving the consumer sweep, i.e.
applying to ourselves the exact discipline whose absence produced §2.

**M3 One deviation reporter, one writer for `rollup.tsv`, plus progress and host telemetry.** *(needs M2)*
All inside `run-drills.sh`; the exit-code law (`run-drills.sh:263–`) is untouched.
- **Appended `rollup.tsv` columns** — `duration_s  attempts  first_verdict  expected  match`, with
  `match ∈ {MATCH, MATCH-BAND(#NN), DEVIATION, NO-EXPECTATION}` computed against M2's table.
  `duration_s` comes from the elapsed value `run_one` already computes (R5). `attempts`/`first_verdict`
  make green-via-retry machine-readable (today it is `(retried)` in the txt only). `FLAKE_SIG` unchanged.
- **A deviation report section** after the existing summary: per DEVIATION, the first-failing assertion,
  the last 5 lines of its **full** stderr (M1 evidence when present, `.log` tail otherwise), the evidence
  path, and a `CLI-CONTRACT-SHAPED` tag when the refusal matches `required|refus|usage:|--[a-z-]+` **or
  rc ∈ 64–78** — note the drafts' `exit 6[0-9]` regex would have missed **both** observed rcs (70 and 77).
  Also print FAIL-then-PASS ordinal sequences.
- **`progress.tsv`**, append-only, one `printf >>` per event under `PIPE_BUF`, with a `RUN-COMPLETE`
  sentinel. Today `rollup.tsv` is written only after every drill finishes, so this run produced **no**
  machine-readable summary for 47 minutes and a killed run produces none at all. No sentinel ⇒ tooling
  must report "partial".
- **Host co-measurement**: `preflight_fsync` (~200 × 4 KiB fdatasync samples via `fio` — `ioping` is
  absent, R4) probing **from `$LOGDIR`**, with a `findmnt` same-filesystem check against the docker root
  (verified same LV, §1), INFRA-ABORT on failure; then a 60 s background sampler appending
  `epoch, loadavg, running_drills, fsync_p50_ms, container_count` to `host-telemetry.tsv`. Per-assertion
  elapsed-ms stamps in `_as_pass`/`_as_fail`; `poll_until` logs `condition met after X.Xs (budget Ys)`
  and a per-drill `POLL-WAIT total=Ns` trailer (R5 — one function, 315 sites).
*Acceptance*: a **degraded replay** of the P0.1 archive flags exactly 20/30/52/74/91, with 20/91 as
DEVIATION carrying the `--reset-js` refusal text and the `CLI-CONTRACT-SHAPED` tag; a forward
forced-failure run proves the evidence-backed stderr path; `cat progress.tsv` mid-sweep shows completed
rows, and a killed runner leaves rows without the sentinel. *Cost*: ~150 lines bash + ~40 lines sh.

**M4 Attribution re-run phase — once, solo, additive, never substitutive.** *(needs M3)*
Default-on post-report phase (`--no-attribute` to skip). Queue = all DEVIATION rows **plus all
MATCH-BAND rows** (a band is confirmed, not trusted), expected-GREEN deviations first, bounded by a
**60-minute time budget** (R11); the remainder prints `UNATTRIBUTED` and still blocks. Each queued drill
re-runs **once, serially, `SIM_CONCURRENT=0`** — the seam already exists at `run-drills.sh:228/240` —
into `<drill>.attempt2.log`, first-run artifacts preserved (the existing R2-F1 discipline). Comparison is
**against the expectation table, never against GREEN** (defining it against GREEN would mislabel 30 and
74, two of the five motivating cases):

- re-run deviates with the same first-fail signature → **REGRESSION**;
- re-run matches the **expected** verdict → **LOAD-SENSITIVE** — *still a blocker, never auto-waived*,
  closed only by a written disposition;
- matches neither → **UNSTABLE** (investigate before any banding);
- the `FLAKE_SIG` setup-retry path is unchanged and shows up as **ENV-FLAKE** via `attempts`.

**The verdict of record and the exit code always come from the first run.** A MATCH-BAND whose re-run
shows a different signature is reclassified DEVIATION.
*On this run's data it would have labelled 20/91 REGRESSION and 52 LOAD-SENSITIVE automatically.*
*Acceptance*: mutation test — hand-edit drill 00 to assert a falsehood → DEVIATION → REGRESSION; inject a
transient (kill a container mid-poll) → LOAD-SENSITIVE with the first-run blocker retained in the exit
code. *Cost*: ~70 lines.

**M5 Kernel-counter preflight (generalise the inotify lesson).** *(independent)*
Extend `preflight_inotify()` (`run-drills.sh:132`) into a table-driven `preflight_kernel()` over
`fs.inotify.max_user_instances`/`max_user_watches`, **`kernel.keys.root_maxkeys`/`root_maxbytes` (the
uid-0 keyring caps the containers' PID1 actually draws on — R4/MI4)**, `kernel.threads-max`,
`kernel.pid_max`, `fs.file-max`, `fs.aio-max-nr`, `net.netfilter.nf_conntrack_max`,
`net.ipv4.neigh.default.gc_thresh3`. **Check-and-refuse** (R4): print current value, usage where cheap
(`/proc/*/fdinfo`, `/proc/key-users`), and the exact one-time remediation lines; PASS/FAIL table into the
rollup.
*Effect*: convicts or exonerates per-uid counters for **drill 30** within one sweep. (Not 91 — that is
the convicted `c6b9c9e` regression.)
*Masking risk*: none — raising a sim host's caps to accommodate 38 co-tenant clusters accommodates the
simulator's density, not tether. The inotify precedent is exactly this. *Cost*: ~40 lines.

**B1 Baseline capture.** Two instrumented `-j 6` sweeps on the same tree once M1–M5 land: the A/B
baseline and acceptance fixture for every speed lever. *Cost*: ~2 × ~48 min server time.

### Phase 2 — Dispositions

**D1 The `c6b9c9e` drill-side interlock.**
- **D1a (this increment)**: sharpen 20/91's red rather than silence it. `expected-verdicts.tsv`
  **keeps `expected=GREEN`** for both — the deviation must keep printing until the product fix lands;
  re-baselining would be laundering.
- **D1b (only after the §6 product increment merges)**: update the 20/91 call sites using drill 41's
  established acknowledgement-gate pattern (assert the refusal exists *before* the `--reset-js` happy
  path, with a precondition that the conf is still clustered so the guard cannot be banked for the wrong
  reason); **verify — do not assume — 22/41/42's call sites**. 20/91 then return to honest GREEN.

**D2 Disposition drill 30.** Solo re-run + `colocated.sh` fixture reading + M5's counter table. Legal
outcomes only: a fixture fix (Mandate ③ — provisioning is the simulator's job), a minted `[GAP #N]` with
a signature pin, or an owned signature-bound band. **"Unexplained" is not an outcome**; the row prints
every sweep until closed.

**D3 Disposition drill 52.** The product question first: does
`retire --compromised --require-credential-rotation` route to the leader or offer retry-with-redirect, or
does it hand a mid-election operator `error: not leader` and leave them to go leader-hunting? If the
latter, that is a candidate `[GAP #N]` — minted and pinned, exposed and not scripted around.
**Forbidden: any drill-side retry loop around the product verb** (Mandate ②). Slow fsync is a real
deployment regime — network storage exists — so "tether misbehaves under 13 ms fsync" is a *product*
finding by policy, not an excuse.

**D4 Disposition drill 74.** Confirm the #34 band via M4's signature comparison and register it as
signature-bound and owned. No probe changes in this increment.

### Phase 3 — Speed levers (each lands separately, A/B'd against B1)

Acceptance wording throughout: *"deviation set unchanged **except rows attributed to the known `c6b9c9e`
regression or its fix**"*.

**V1 Longest-first launch order + a checked-in cost manifest.** *(the single biggest win, §1)*
New repo file `test/simcluster/drill-costs.tsv` (`drill  p50_secs  class  note`), **checked in on the WSL
side** — `remote.sh`'s `rsync --delete` (remote.sh:89–92) protects only secrets/backups/ssh_config/
`*.local` and would destroy a server-side file. Seeded from the 2026-07-23 durations including 96 = 22.2
min (R2). Deliberately *not* extra columns in `expected-verdicts.tsv` (different lifecycle, different
parser, verdict SSOT). The runner sorts descending with a deterministic lexicographic tie-break; a drill
absent from the manifest sorts **first at max cost** so a new drill can never become an accidental
straggler; `--no-lpt` restores name order for forensics.
*Effect*: wall lower bound becomes `max(Σ/j, 22.3)`. At `-j 6`: **47.4 → ~32.5 min** (MA9-corrected;
≤29 min needs the sum-reduction levers too, not V1 alone).
*Masking risk + pin*: reordering changes neighbour sets and can move which timing-coupled drills go red →
the order is deterministic and M3's deviation report makes any shift visible; verdicts and blocking are
untouched. Stale costs can mis-order but cannot mis-judge. *Cost*: manifest + ~25 lines.

**V2 `poll_until` geometric backoff — one file, 315 sites untouched.**
Reinterpret the interval argument as a **cap**: sample immediately, then 200 → 400 → 800 ms … capped at
the declared interval. **Timeouts are unchanged** — the deadline is the contract, the interval is only
the sampling grid. Backoff state must live **in the `_pu_push` frame record**, never a global
(`poll_until` is deliberately nested; the R1/H5 frame stack at `lib/log.sh:44–64` exists because globals
were clobbered); integer-ms math only (drills run under `dash`).
Two **audited exemption classes**, both structural:
(a) **effectful predicates** — e.g. 74's `_construct_111` issues a product mutation per tick — keep
`poll_until_fixed`;
(b) **stability/convergence-shaped predicates** keep the fixed grid or migrate to H3's `assert_stable_for`.
Exemption (b) is not negotiable: `poll_until` returns at first success (`lib/log.sh:66`), so denser early
sampling *raises* the chance of banking a transiently-true state (the #46 class). "Denser sampling
exposes flaps" is false here and no empirical check can substitute for the audit.
*Effect*: the poll-grid overshoot bucket, honestly bracketed at **~5–11 min** of the sum from the interval
histogram — and measured exactly by M3's `POLL-WAIT` trailers, which is the point of doing M3 first.
*Cost*: ~30 lines in `lib/log.sh` + one audit pass.

**H3 Blocking-sleep hygiene (demoted from the candidate's V3 — R1).**
Classify the **36** real blocking-sleep sites and add a `tests/lint-drills.sh` rule so an untagged bare
blocking sleep fails lint. Legal tags: `stability-window:` / `prod-ttl:` / `retry-spacing:` /
`soak-settle:`. Convert only sites classified ONSET. Keep 22's `sleep 61` (`prod-ttl` — the sole
real-clock expiry wiring probe, R8) and 97's soak settle.
*Claimed saving: ≤2 min. This is hygiene, not a lever* — the value is that a future
"where does the time go?" question cannot be answered with a wrong grep again.

**V4 Setup spine: parallel bring-up + polled agent-join.**
(a) `cmd_up` issues all `docker run -d` first, then `wait_sysd` across all nodes, and backgrounds the
per-node provisioning step with per-node logs — the retry-once diagnostics at `simcluster:81–85` must
survive interleaving. (b) `cmd_agent_join` replaces the fixed `timeout 6` with a poll on the bind's
observable (the persisted member credential). **No tether state is pre-staged; the end state is
identical.** *Effect*: estimated ~8–12 min off the sum; acceptance is the measured phase share, not the
estimate. *Masking risk*: none for (a) — host-side launch interleaving only; low for (b) — the poll keys
on the same evidence the timeout approximated, and a genuinely slow bind waits exactly as long.

**V5 Drill 90: batch `_write_raft_gap` into one `dexec`.** All 72 product CLI invocations preserved
verbatim; only 71 docker-exec round-trips removed. ~1–2 min. Masking risk: none — the product path is
unchanged.

**V6 Launch storm: measure first, then hoist.** Timestamp `docker events` during a full-parallel launch
and report create→running p99 in the rollup; hoist `cmd_drill`'s per-drill stale-image sha-gate
`docker run` (`simcluster:549`) to **one check per sweep**, removing 38 container starts from the storm.
Add a `flock`-serialised instance-create phase **only if** measured p99 > 5 s.

**V7 Calibration A/B and the `-j` decision.** Same tree, full suite at `-j 6` and `-j 12`, machinery
live. Raise the documented default **only if** the `-j 12` deviation set ⊆ the `-j 6` set, with every
width-only red first passing M4 attribution **and** a written disposition. **A width-only red is never
pre-classified as environment** — #67 was exactly a concurrent-only *product* defect. Record fsync canary
p50/p99 at both widths. Crossover: `Σ/j < 22.2` at `j ≈ ⌈Σ/22.2⌉ ≈ 8–9` with today's numbers, recomputed
from the manifest at each re-baseline rather than hard-coded.
*Effect*: with V1–V5, wall ≈ **22.2 min** (the floor, R7) at `-j 12`; ~25–28 min at `-j 6`.
*Masking risk + pin*: higher `j` can create reds but cannot absorb them; telemetry is evidence, never an
auto-throttle (runs must stay reproducible).

### Phase 4 — Storage/CPU isolation (conditional, NOT on the critical path)

**Trigger (R6)**: fsync canary p99 sustained > 10 ms at the adopted width, **and** ≥2 deviations
attributed by written disposition to drill-oracle budgets under shared fsync rather than to product
findings. Ratified by the main process at V7.

Content, respecified for this host so the design is not lost: per-drill stores as a **static slot pool**
provisioned in one interactive `sudo -S` session (LVM thin LVs or loop files, mkfs, fstab-persisted,
weiland-chowned) — **zero root at runtime** (R4); runner assigns slots at launch; between-sweep cleanup
of root-owned residue via a privileged helper container (docker-group membership suffices). An
**in-effect gate**: `findmnt` per store must show the per-slot device or INFRA-ABORT — never a silent
fallback. Per-drill cpusets/memory threaded via `SIM_CPUSET`/`SIM_MEM` through `run_node`'s existing
extra-flags passthrough (`lib/docker.sh:27`, the `--sim-lib-tmpfs` precedent), with `taskset` readback
after **every** `run_node` call, not only post-`up` (mid-drill re-creations exist).

**A/B gate with a tree-independent masking canary**: "20/91 must stay red" evaporates the moment the
product fix lands, so the canary is instead a self-supplied known-red (a sandbox build with the fix
reverted, or a hand-edited fixture drill) that must stay red under full isolation, else abort and bisect.
**Every red→green flip under isolation requires a written disposition before the flip enters any
baseline** — a product gotcha `#N` with a pin for "tether misbehaves under load", or an explicit
oracle-budget ruling. Never a bare resource name.

*Fidelity note*: per-drill stores are arguably **more** faithful to the reference deployment (one node
set per machine's NVMe) than 38 clusters sharing one journal. The contended regime is then retained
deliberately as a labelled sensor, not left as the accidental default — see C1.

### C1 — Contention as a sensor (standing policy)

Every load-discovered product defect this suite has produced came from *unengineered* contention: **#67**
(found under 7-way saturation; face B still has no deploy-tier oracle), **#66** (leader-hop window, ~1
in 3 at `-j 3`), and this run's **52 rc=77** candidate. Every lever above reduces contention, so a stress
regime must be scheduled with obligations rather than assumed:

- **One stress sweep per release gate.** Today that is simply the full sweep at the calibrated high `-j`
  on the shared store — the current default *is* the contended regime. If Phase 4 lands it becomes
  `run-drills.sh --contended`, and its deviations flow through the **same** M3/M4 reporter and the same
  disposition machinery. A lane whose reds cannot mint gotchas is a lane whose reds are pre-dismissed:
  `--contended` results therefore *can* mint gotchas and *cannot* silently update `expected-verdicts.tsv`.
- **A registry** of which open stress-dependent items (#66, #67 face B, the 52 candidate) rely on which
  regime to reproduce. Any change that reduces default contention must update it.

## 6. Acceptance criteria for the increment

All runnable on weilandserver with **zero runtime sudo** (R4).

1. **Machinery** — `tests/verdict-contract-test.sh` green including the unwritable-evidence-dir
   byte-identical case; degraded replay of the P0.1 archive yields exactly the five deviation rows, with
   20/91 as DEVIATION carrying the `--reset-js` refusal text and the `CLI-CONTRACT-SHAPED` tag; forward
   mutation test → REGRESSION; transient injection → LOAD-SENSITIVE with the first-run exit code retained.
2. **Consumer sweep proven** — `sh tests/run-all.sh` green after M2.
3. **Dispositions on file** for 30, 52 and 74, each closed as a fixture fix, a minted `#N` + pin, or an
   owned signature-bound band; zero signature-less or owner-less bands (validator-enforced); 20/91 carry
   REGRESSION attribution every sweep until D1b and are **never re-baselined**.
4. **Speed, honestly floored** — sum 195.2 → **≤160 min**; wall at `-j 6` with LPT alone **≈ 32.5 min**
   (the makespan floor), and **≤ ~29 min only with V1 + the sum-reduction levers (V4/V5)**; wall at
   `-j 12`, if V7 raises it, **≈ 22.2 min** (the drill-96 floor, R7); deviation set across two
   consecutive post-lever sweeps unchanged vs B1 except `c6b9c9e`-attributed rows.
5. **`-j` default** changed only through V7's ⊆-and-dispositions gate; a blocked raise is recorded with
   its telemetry.
6. **No power lost** — no `FLAKE_SIG` change, no verdict substitution, exit-code law byte-compatible; a
   diff review confirms **no `assert_*` was deleted anywhere in the increment**; C1's policy and registry
   sit in `test/simcluster/README.md` next to the Mandate.

## 7. Out of scope — handed to other increments

- **The `c6b9c9e` product increment (named parent of D1b).** Restated per R3: **evaluate the `--reset-js`
  gate as a preflight, before `clusteroffline.ForceSingle()` commits anything.** The store's emptiness is
  discoverable from `nats.conf` up front, so an operator who forgot the flag can be refused while the node
  is still untouched, instead of after raft, the DB and the conf have all been mutated
  (`cmd/tether/cluster_offline.go:202–250`). The conf-rewrite-then-refuse ordering itself is **correct and
  must not be changed** (§2.2). Plus the full call-site sweep including operator-facing copy — the
  seventh-recurrence prevention. Own plan → implementation → internal review → external review.
- **The 52 leader-routing fix**, if D3's disposition mints the gap. This increment only exposes and pins it.
- **`Now`-seam injectability sweep** (~15–25 bare `time.Now()` timer-decision sites) — its own small
  product increment, and explicitly *not* justified by any drill-deletion argument (R8).
- **Wave-2, each behind its own gate**: 74 probe concurrency (needs a contamination-free per-exit
  attribution demonstration and re-review of the R8/R9/R10-locked assertion text); the 90/98 drill split
  (payoff at the committed configuration is ~0 once V1 lands, since the wall floor is 96); multi-host
  sharding (only if telemetry shows fsync saturation at the adopted `-j`); **attacking drill 96's
  internals** (R7 — this plan claims nothing from it).

## 8. Implementation status and what the build itself found

Landed in Stage B (the whole increment in one phase, per the user's directive to advance all levers
together): **P0.1, M1–M5, V1, V2, V4, V5, V6, H3 (drill-10 polls), D1b, D4**, plus the `poll_until`
instrumentation. All **eleven** hermetic gates pass (`sh tests/run-all.sh`), including three new ones —
`tests/validate-verdicts.sh`, `tests/validate-verdicts-selftest.sh` (14 mutations, all caught) and
`tests/deviation-report-test.sh` (grown to cover B1/B2/MA1, band-signature, and the three M4 labels).

Lever-by-lever:
- **V1** longest-first launch (drill-costs.tsv) — verified on the real stack (96 launches first).
- **V2** `poll_until` fast-start (sample every min(1s,interval) for the first interval, then the declared
  interval; deadline unchanged). Conservative 1s floor, not 200ms geometric, so it cannot bank a
  sub-second transient the old 2–3s grid would have missed. Two audited exemption classes on
  `poll_until_fixed`: EFFECTFUL predicates (`_rebalance_tick` — a rebalance per tick) and STABILITY-WINDOW
  predicates (`_dist_stable`, `_wh_leader_stable` — the #46 leader-flap class), plus all `-- false` settle
  timers.
- **V4** parallel bring-up: `cmd_up` launches every container first, then provisions all nodes
  concurrently (each into its own log, retry-once preserved). set-e/background hazard handled — a node
  whose `wait_sysd` dies still records a failing rc, and a MISSING rc is itself a failure, so a dead node
  can never green a sweep. Verified: `00-skeleton` GREEN on the real stack.
- **V5** drill-90: the 72 `tether alert` calls now run in ONE in-container `dexec` loop (byte-identical
  product path; ~71 docker-exec round-trips removed).
- **V6** the stale-image sha-gate is hoisted to one `simcluster check-image` per sweep (removes N
  container starts from the launch storm; the guard still runs, once).
- **H3** the five hand-rolled `for i in seq…; sleep 3` polls in drill 10 became real `poll_until`
  (instrumented + fast-start; the write-commit one is `poll_until_fixed`, being effectful). The
  lint-tag-every-sleep sweep is judged low-value churn against R1's ≤2 min ceiling and is not done.
- **D1b** the c6b9c9e call-site sweep: drills 20 and 91 now pass `--reset-js`, mirroring the GREEN drill
  42 — the correct fix, since §2.2/R3 established the gate is correct journalled behaviour, not a bug.
  (Their expectation was always GREEN; the deviation resolves once the deploy tier confirms them green.)
- **D4** the #34 flake on drill 74 is registered as a signature-bound band
  (`ASSERT-FAIL@#34@sig:c-ss-preflow`); replay confirms its archived ASSERT-FAIL now classifies
  MATCH-BAND(#34) — still blocking, no longer a fresh DEVIATION.

**D2, D3, V7, B1 — DISPOSED (not all "closed")** (2026-07-24, corrected after external review Major 3;
full detail in `docs/reviews/simcluster-accel-dispositions.md`):
- **B1**: two `-j 6` sweeps (27.3 / 28.2 min) + one `-j 12` (20.7 min). **Acceleration measured: −42 %
  (`-j 6`) / −56 % (`-j 12`)** vs the 47.4 min pre-lever baseline — real and stable. **The deviation set
  is NOT stable across sweeps** ({30,52,74,96} vs {30,50,52,96}); external review is right that this fails
  the plan's exact-set criterion, and the earlier "shifting proves not-a-regression" claim was invalid.
  What is established (per-drill, via M4 + disposition): none of the deviations is a lever regression.
- **D3 (52)**: minted `[GAP #69]` (retire-leader-routing product-UX gap) + `sig:retire-not-leader` band —
  **legally closed** (the signature distinguishes the flake from a real retire failure).
- **74**: `#67` tier-B transient at B-negctrl + `sig:b-negctrl-create` band — **legally closed**.
- **D2 (30) / 96-grow**: `[GAP #70]` grow-concurrency flake, **DELIBERATELY NOT banded** — a real grow
  regression fails at the same `grow_to_3` assertion, so banding would hide it (the round-2 MAJOR-2 /
  #65-catch laundering class). First-class LOAD-SENSITIVE; real fix is OQ-8 wave-splitting (separate).
- **96 (#65)**: `[GAP #71]` — a `-j 6` reproduction claiming the *minority* committed the write, which
  contradicts #65's REFUTED status (real minority-write durability, or a drill attribution bug — under
  investigation). **My own new validator caught the attempt to band it** (`BAND-ON-CLOSED-DEFECT` — #65
  is refuted). NOT banded; a genuine finding to be seen, not laundered.
- **50**: history-reader recovery runtime-guard fired under load → INCOMPLETE; an honest valve, documented.
- **V7**: default stays `-j 6` (`-j 12`'s set ⊄ `-j 6`'s — adds 20/#67 and 73/proxy); `-j 12` kept as an
  on-demand stress width; Phase-4 isolation not triggered (fsync device-bound).
- **Honest acceptance status**: the "stable deviation set at `-j 6`" criterion is NOT met and cannot be
  without hiding regressions or wave-splitting; it is amended (explicitly) to "every deviation
  M4-attributed and legally disposed — band where a signature separates flake from regression, minted GAP
  with a no-band rationale where it cannot." Bottom line (corrected after external re-review Major 3):
  **the −42 %/−56 % acceleration is a measured performance result but is NOT claimed release-clean** —
  `#71` (a minority-write PRODUCT-RED with two possible roots) and drill 50 (a recovery miss under load)
  are unresolved, so "no lever regression" cannot yet be asserted; both need deploy-tier rooting on the
  corrected tree. The `-j 6` grow-timing flakiness (#70) stays tractable-but-not-eliminated (OQ-8 wave-
  split is the future fix).

### Deploy-tier validation (2026-07-24, weilandserver)
Every new lever verified GREEN on the real stack:
- **V4** parallel bring-up: 00-skeleton, 10-grow-to-3, 20-forcesingle-natsconf, 90-alerts-lifecycle,
  91-client-converge — all GREEN.
- **V2 + H3**: 10-grow-to-3 GREEN, all five converted `poll_until` assertions pass.
- **V5**: 90-alerts-lifecycle GREEN (49 PASS; the batched 72-call alert loop ran in one dexec).
- **D1b**: **20 and 91 both GREEN — the c6b9c9e regression is fixed on the real stack.** 91's log shows
  `C offline force-single --reset-js commits …` and `C force-single de-clustered nats.conf to standalone`.
- **D4**: replay confirms 74's archived ASSERT-FAIL classifies MATCH-BAND(#34), still blocking.
- **V6**: every sweep ran the image check once (no per-drill container-start storm).
The two motivating deviations (20, 91) are now GREEN; the replayed archive deviation set is {20,30,52,91}
with 74 banded, and 20/91 resolve to GREEN on a fresh run.

### Round-2 unified internal review (Opus 4.8) → PASS-WITH-FIXES, all fixed
The whole increment (Group A machinery re-verified + Group B levers) went through a second 13-agent
unified internal review: **PASS-WITH-FIXES**, 2 MAJOR + 5 MINOR, all confirmed, all fixed
(`docs/reviews/simcluster-accel-review-round2.md`). Both MAJORs were the increment's own defect classes
recurring under the speed work:
- **MAJOR-1**: V2 flipped `poll_until` fixed→fast globally; the first sweep missed six effectful
  predicates in UNMODIFIED drills (incl. `_construct_111`, which the plan named). Fixed + a new gate
  `tests/poll-mode-test.sh` makes the exemption **gate-enforced, not audit-enforced** — the recurrence
  vector is closed.
- **MAJOR-2**: the #34 band keyed on a harness-conflated probe; a bind flake could bank as a confirmed
  #34. Fixed by mirroring the sibling B-dp `_ss_fail_stage` classification (harness-* → gap, product → the
  banded assert).
Twelve hermetic gates now pass. Round-2 fixes deploy-tier re-verified GREEN: 20/91 (MINOR-1 product
store-move oracle passes) and 90 (MINOR-2).

**Acceptance check §6.1 is met**: replaying the archived 2026-07-23 sweep through the real classifier
(`run-drills.sh --replay`, a new flag added for exactly this) flags **exactly** 20/30/52/74/91 —
5 DEVIATION, 33 MATCH — in seconds, against the human evening it originally took. The suite exit code
is unchanged at 14, proving the match axis is reporting and not authority.

Three things the implementation discovered that the plan did not anticipate:

1. **The band validator was born vacuous.** `printf '%s' "$bands" | tr ',' '\n' | while read` never
   executed its body: without a trailing newline `read` consumes the final field but returns non-zero,
   so every band rule silently checked nothing while the validator reported OK. Caught only because the
   mutation suite included a malformed band. Fixed, and the reason is recorded at the call site.
2. **Some drill predicates swallow the evidence M1 exists to record.** Drill 20's `_fs20` captured the
   force-single output into a local, grepped it, and discarded it — so `_AS_OUT` was EMPTY and the log
   showed only `(want exit 0, got 1)`. That is why drill 91 printed the `--reset-js` refusal on
   2026-07-23 and drill 20, failing on the *same* gate, showed nothing. `_fs20` now re-emits on failure,
   and `_as_evidence` prints an explicit "the predicate produced no output — the evidence was swallowed
   inside the drill" note rather than leaving a silent blank. A repo-wide sweep found ~14 further
   predicates with the same shape; converting them is follow-on work, and the note makes each one
   self-announcing when it next fails.
3. **M5 generalises the inotify lesson to eight more per-uid kernel counters** — a class Docker does not
   isolate, of which inotify was simply the first hit hard enough to notice. (An initial pass flagged
   `kernel.keys.maxkeys=200` as a hazard; internal review MI4 corrected this — privileged containers run
   PID1 as host uid 0, governed by `root_maxkeys=1000000`, so the keyring is checked-and-exonerated. The
   check now reads the uid-0 caps. The value of M5 stands: the *next* such limit is now caught by a
   preflight table instead of another multi-week misdiagnosis.)

### Requirements reconciled after external review (Major 4)

The external review correctly flagged that an earlier draft of this status section (a) contradicted
itself — a stale "Not yet done: V2/H3/V4–V7…" line survived alongside the later "landed" entries — and
(b) claimed M3/M5/V4/H3 landed while some committed sub-behavior was absent. Reconciled below with an
EXPLICIT decision per item (implement vs amend-with-justification); no requirement is silently narrowed.
(The stale contradicting line is deleted.)

- **M3 — now fully implemented.** The `host-telemetry.tsv` 60-second background sampler
  (`epoch, loadavg, running_drills, fsync_p50_ms, container_count`, killed by an EXIT/INT/TERM trap) and
  the fsync preflight were added: an UNMEASURABLE canary is now an INFRA-ABORT, a different-filesystem
  canary a loud recorded WARNING (amended from "infra-abort on same-fs mismatch" — killing a whole sweep
  because the canary is non-representative is disproportionate; a warning is the honest signal). The
  committed "per-assertion elapsed stamps" are carried in the evidence record's `took:` field and the
  `DRILL-POLL-WAIT` trailer rather than added to every console line, which would break the three
  grep-anchored `DRILL-VERDICT`/console parsers.
- **M5 — now fully implemented + amended.** `net.ipv4.neigh.default.gc_thresh3` added. The whole-set
  "check-and-refuse" is amended to **refuse-inotify / check-and-report-the-rest**: inotify is the one
  counter with a *reproduced* container-density hard failure (it refuses, `exit 3`); refusing on the
  others' unproven thresholds would wrongly abort a host whose lower cap is still sufficient. The code
  comments and the `return 0` are now consistent (the earlier "CHECK AND REFUSE" vs "REPORT" contradiction
  is gone).
- **V4 — (a) done, (b) amended.** Parallel bring-up (the ~8–12 min win) is implemented and deploy-tier
  green. The polled-agent-join sub-item (b) is **amended: the fixed bind wait is kept.** Rewriting the
  agent bind to poll the persisted member credential touches a core path every agent drill depends on and
  risks catching a half-completed member registration, for a ~5 s/drill saving on the few agent drills —
  a deploy-tier risk disproportionate to the gain, and not safely verifiable without iteration.
- **H3 — drill-10 conversion done, lint-tag sweep amended out.** The five hand-rolled polls in drill 10
  are real `poll_until` now. The full 36-sleep classification + lint rule is **descoped** per R1 (blocking
  sleep is 2.9 min suite-wide, not a lever; ≤2 min ceiling): the `poll-mode-test` gate (fast/fixed
  discipline + effectful backstop) and `lint-drills` already enforce the load-bearing poll/sleep
  discipline, so the tag-every-sleep churn buys little. This is an explicit descope, not a silent one.

## 9. Provenance

Stage A step 1 ran 13 agents (Fable 5) with zero errors: 6 drafters (triage/attribution, host isolation,
wall-clock reduction, tier boundary, scheduler/orchestration, skeptic/ROI), 6 fixed adversarial critic
lenses (Mandate, evidence rigour, host feasibility, defect masking, completeness, sequencing), 1
synthesiser. The candidate is archived outside the repo; this file supersedes it.

Critic lenses caught, among others: that runtime `sudo -n` fails on weilandserver (verified live by the
main process, R4); that drill 22's `sleep 61` has a Go successor which nonetheless does not cover the
wiring (R8); that a fixture-snapshot speed-up would manufacture the output of tether's own init/grow
(Mandate ①/③/④); and that `exit 6[0-9]` would have missed both observed refusal rcs. The main process
overruled the candidate on the sleep budget (R1), on the partial-application framing and the resulting
product hand-off (R3), and discharged its open measurement item (R2).
