# Round-6 self-review — S6–S8 round-5 remediation delta

**Scope:** the unstaged working-tree delta (`internal/clusteroffline/journal.go`, `internal/cluster/datadirlock.go`, `internal/cluster/exchangedir_{linux,other}.go`, plus the edits to `internal/clusteroffline/offline.go`, `internal/cluster/offline.go`, `internal/broker/broker.go`, `cmd/tether/cluster_offline.go`, `internal/broker/cluster_operation_controller.go`, `test/simcluster/lib/assert.sh`).
**Lanes:** 6, each adversarially verified. **Verdict: DO NOT SHIP.**

---

## Ship / do-not-ship

**DO NOT SHIP.**

Three independent reasons, any one of which is disqualifying:

1. **The suite is RED right now.** `go test ./internal/clusteroffline/` fails: `TestS6S8Round6JournalTempRefusesSymlink` — the delta's own new file `journal.go` gives an unprivileged account a root file-clobber primitive. CLAUDE.md §5 hard gate not met.
2. **The delta introduces a MAJOR regression with a larger blast radius than the blocker it fixes** (R2 / B3-1): a root-created `tether.lock` — which the runbook's own documented `sudo tether cluster recovery …` form creates on *any* run, including zero-mutation refusals — now makes the only surviving broker refuse to start, forever, with a misleading remedy string.
3. **All three blocker fixes are 100% deletable with `make test` green.** Two lanes independently reproduced the mutations. This is verbatim the round-5 central criticism, reproduced by the round that was written to answer it. `force_single_round5_test.go:13`'s claim — *"Each test FAILS if its fix is reverted"* — is false for all five tests, proven by reverting all four fixes simultaneously and getting `ok` across `internal/clusteroffline`, `internal/broker`, `internal/cluster`, `cmd/tether`.

### Must-fix before re-submission

| # | Item | Finding |
|---|---|---|
| M1 | Harden `writeJournal`: `os.CreateTemp` (O_EXCL) or `O_EXCL|O_NOFOLLOW`; `readJournal` O_NOFOLLOW. Keep the round-6 test as the pin. | R1 |
| M2 | Split `AcquireDataDirLock`'s error taxonomy (EACCES/EPERM ≠ "someone holds it"); chown the lock to the data-dir owner in the offline tools; fix `docs/cluster-runbook.md:355/403/423/428/473` to `sudo -u tether`. | R2 |
| M3 | Gate `resumeConfirmation` on a real `phaseRosterPruned` — never honour a `phaseStarted` journal's `ConfirmedDead`; print the *effective merged* set at the prompt. | R3 |
| M4 | Change the `offline.go:184` WARN text so it does not contain `de-clustered to standalone`; fix `drills/20:35-36` to stop piping the mutating command. | R4 |
| M5 | Wire `InterruptedForceSingle` into broker startup + `cluster status` + a drill assertion. An unwired reporter is B1 half-done. | R5 |
| M6 | The four missing call-site tests (§Test non-vacuity). Without them nothing here is a regression pin. | R6–R9 |
| M7 | Make `blockAfterAttempts` step-aware and guard `AbortOp` post-`RaftRemoved`; fix `ConfirmOp`'s rewind for retires blocked at `NATS_ROLLED_OUT`. | R11, R12 |

---

## Do the three blockers actually close?

### B1 (recoverable transaction) — **NO. Mechanism present, blocker open.**

What *does* close: the journal is written before the first mutation (`offline.go:139-145`), the CLI now returns a hard error instead of a WARNING+`nil` (`cmd/tether/cluster_offline.go:196-205`) — I self-verified this against the source after one lane claimed the file did not compile (see *Refuted*) — and `ClearForceSingleJournal` is called exactly once, on success, after the de-cluster (`:215-217`).

What keeps it open:

- **Nothing reads the journal.** `InterruptedForceSingle` has zero production callers and zero doc mentions (R5). In the canonical power-cut case the operator sees `status=70`, a crash-loop, and a hidden dotfile no surface names. B1 delivered *forward-completable* but not *identifiable* — and the round-5 report explicitly said the operator will not re-run "the single most dangerous cluster command" unprompted.
- **`prior.Phase` drives no control flow** (R10). It appears in exactly one `Warn` line. "Forward-completion" is in fact *re-execute every phase and hope it is idempotent* — `RecoverCluster` again, marker again, prune again, a second full store rebuild — with a fresh crash window each time and zero tests on that idempotence.
- **A `phaseStarted` journal from a zero-mutation run grants a permanent, invisible `--confirm-peers-dead`** (R3). That is a split-brain path *created* by the fix.
- **The one window where the journal is load-bearing (prune→exchange) is the one it cannot resolve** (R13): the resume re-derives the roster from the stale pre-prune snapshot and re-runs the liveness veto, which `resumeConfirmation` cannot relax. Escapable only by powering the revived peer back down — an undiscoverable ordering trap behind a misdirecting error string.

### B2 (no `raft/`-missing window) — **The narrow claim closes. The blocker's stated invariant does not.**

The three-step fallback is deleted and `swapDirs` refuses (`internal/cluster/offline.go:445-459`). There is no longer a window where `raft/` does not exist. That much is genuinely fixed.

But B2's *purpose*, stated in the delta's own comments (`offline.go:415-417`, `clusteroffline/offline.go:97-99`), is that **no failure may land after the roster prune**, and the code now asserts that absolutely (`offline.go:364-365`: *"an unsupported filesystem never reaches this point"*). That claim is false:

- The probe exchanges **two fresh empty siblings**; the real exchange targets `dataDir/raft`, a long-lived object. A separate mount there yields EBUSY (empirically reproduced) which is not in the unsupported set and escapes as a raw errno **after the prune** (R14). Non-default layout, so MODERATE — but the absolute comment is a lie.
- **ENOSPC is not probed at all** (R15), and `RebuildSingleNodeFromDB` writes ~2× `tether.db` into the same partition between the prune and the exchange (`snapshot.go:50-69` — a temp full copy *plus* a streamed copy). A broker force-singled after a disk-pressure incident is the canonical case, and it bricks in exactly B2's shape.

So: the *rename* is now crash-consistent; the *transaction* still is not.

### B3 (continuous interlock) — **The window closes. The fix is unpinned and introduces a worse outage.**

`Broker.Run` takes `${DataDir}/tether.lock` for its process lifetime (`broker.go:645-653`), fail-closed. That genuinely closes the mid-surgery-daemon-revival window the one-shot bolt probe could not — and I reject the lane claim that the lock is redundant with the bolt lock (the surgery *closes* the bolt store, so the probe is blind to a later daemon; only the daemon's own flock refuses it). The architecture ruler (`docs/distributed-broker-architecture.md:365(g)`) already scoped this to cluster mode and already named the never-clustered hole and its SQLite-busy mitigation, so the single-mode gap is a pre-existing accepted decision, not a hole in this delta.

But the fix ships with **zero coverage at its real entry point** (R8) and **converts documented gotcha #6 from "a later offline op is denied" into "the only surviving broker crash-loops until systemd parks it `failed`"** (R2), printing a remedy (*"stop the previous broker"*) that is wrong in exactly the case that fires.

---

## Test non-vacuity — the smallest green revert

Two lanes ran these mutations independently and agree. In every case `go build ./...` is clean and the full suite is `ok` (excluding the pre-existing round-6 symlink red, which fails identically on the unmutated tree and therefore carries no signal).

| Blocker | Smallest revert that stays green | Why the suite cannot see it | Exact missing test |
|---|---|---|---|
| **B1** | Delete `internal/clusteroffline/offline.go:104-118` (read+resume), `:139-145` (pre-mutation write), `:175-179` (phase update) → `grep -c writeJournal offline.go` == 0. **`go test ./internal/clusteroffline/ -run Round5` → ok.** | All three B1 tests call `writeJournal`/`readJournal`/`resumeConfirmation`/`InterruptedForceSingle` **directly** (`force_single_round5_test.go:24-47, :57, :65-79`). None calls `ForceSingle`. `journal.go` is untouched by the revert, so the helpers still round-trip. | `TestRound5B1_RealForceSingleReRunForwardCompletes`: seed a data dir, run real `ForceSingle` with `ConfirmedDead`, assert `journalPath(dataDir)` exists **and** `Phase == phaseRaftRebuilt`; then re-run with `ConfirmedDead: nil` and assert it succeeds; then remove the journal and assert it fails. Verified to pass on real code and fail on the mutant. |
| **B1 (CLI half)** | Revert `cmd/tether/cluster_offline.go:201` to `declustered=""` + WARNING + `return nil`, **and** delete the `ClearForceSingleJournal` call at `:215`. **`go test ./cmd/tether/` → ok.** | No test in `cmd/tether` drives the force-single `RunE` at all — the only force-single tests are status-card rendering (`cluster_status_card_test.go:51`) and subcommand registration (`cluster_recovery_test.go:29`). | A cobra-level test pointing `natsConf` at an unwritable path: assert (1) non-nil error, (2) the error names the journal/re-run, (3) **the journal still exists**. Plus the mirror case: success → `nil` **and** `journalPath` gone. (3) is the only thing that pins the clear-only-after-final-phase ordering. |
| **B2** | Delete the single line `internal/clusteroffline/offline.go:100` (`AtomicExchangeCapable` precheck). **`go test ./internal/clusteroffline/ -run TestRound5B2` → PASS.** | The test calls `cluster.AtomicExchangeCapable(dir)` **itself** (`:87`) and never calls `ForceSingle` — it cannot see the call site. Worse, its assertion accepts *both* outcomes: `if err != nil && !strings.Contains(err.Error(), "atomically exchange")` (`:88`). The litter check (`:92-100`) cannot fire because the probe `defer os.RemoveAll`s both dirs unconditionally (`cluster/offline.go:428, :433`). The named property — *IsPrechecked*, *NotFallenBackOn* — is asserted nowhere. | `TestRound5B2_UnsupportedFSRefusesBEFOREAnyMutation`: inject `errExchangeUnsupported` via a hook (`internal/cluster/testhooks.go` already exists), snapshot roster rows + `force_single_active` marker + `raft/` contents, call real `ForceSingle`, assert it returns `ErrAtomicExchangeUnsupported` **and the snapshot is byte-identical**. That single test is the only thing that pins the ordering, which *is* the blocker. |
| **B3** | Delete `internal/broker/broker.go:637-652` + the now-unused `internal/cluster` import (it is the block's only consumer, so the import removal *is* the revert). **`go test ./internal/broker/ ./internal/cluster/ ./internal/clusteroffline/` → all ok**; `TestRound5B3_DataDirLockIsOneSSOTAndExcludes` **still PASSES**. | The test never constructs a `Broker` and never calls `Run`. It asserts (a) a compile-time constant equals itself (`lockFileName` *is* `cluster.DataDirLockFile`, `offline.go:33`) and (b) that flock excludes flock — a kernel property, already covered by the pre-existing `TestD7OfflineFlockExclusive`. `grep -rn AcquireDataDirLock --include=*.go` → the only production caller is `broker.go:646`; the only test reference is `:110`. | `TestRound5B3_BrokerRunHoldsDataDirLockForItsLifetime`: (a) hold the lock externally → `Run` returns an error naming the lock; (b) **the important direction** — start `Run` with `ClusterDataDir`, wait for serving, assert `AcquireDataDirLock` FAILS, cancel, wait for `Run` to return, assert it now succeeds. Nothing today covers "held for the whole lifetime" or "released on exit". |
| **S5-15 escalation** | Replace `cluster_operation_controller.go:802-804` with `_ = err` and `:600-602`/`:610-615` with `Warn`+fall-through. Suite green. | Zero tests name `blockAfterAttempts` or any of the three messages. The branches are *structurally* untestable: `a.node` is a concrete `*cluster.Node` (`clusteradmin.go:48`) and `deriveAndConvergeSeedsFromRoster` is a concrete method — no seam, unlike the deliberate `caughtUpFn`/`streamsReadyFn` func fields (`clusteradmin.go:76-77`). The nearest test (`g3_seed_helper_test.go:110`) drives the `err == nil` success path only. | Add `seedConvergeFn func() error` to `ClusterAdmin`; seed a retire at `NATS_ROLLED_OUT`, inject a persistent error, tick `opMaxAttempts` times, assert `OpStateBlocked` with `last_error` naming the step **and** that it never reaches `RETIRED`. Assert attempts 1..4 leave it at `NATS_ROLLED_OUT` (pins the bound). |
| **`out_matches` rc gate** | Delete `test/simcluster/lib/assert.sh:161`. `tests/verdict-contract-test.sh` → ALL PASS; `tests/lint-drills.sh` → exit 0; drill 92's verdict unchanged. | `verdict-contract-test.sh` never mentions `out_matches` (grep: 0 hits), and the sole call site (`92:89`) uses `--online --dry-run`, which returns `OK: true` **unconditionally** (`force_single_online.go:207-208`) → CLI `return nil` (`cluster_offline.go:268-270`). The rc gate can never change that verdict. | Five hermetic cases in `verdict-contract-test.sh` (no docker needed), the load-bearing one being `out_matches 'hello' sh -c 'echo hello; exit 9'` → expect rc=1. Plus a sentinel-file test pinning the anti-SIGPIPE property the function exists for. |

**Summary: the entire round-5 remediation — all three blockers plus the S5-15 escalation plus the harness rc gate — can be deleted wholesale and the suite stays green.** The only red in the tree is a test the *reviewer* wrote, not the author.

---

## Surviving findings, most severe first

### R1 — MAJOR — `writeJournal` follows an attacker-controlled symlink; root truncates an arbitrary file
`internal/clusteroffline/journal.go:78` · *lanes: journal-crash-consistency (J4), test-adequacy (TG-5)*

**Wrong:** `os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)` on a **fixed** path `journalPath(dataDir)+".tmp"` (`:77`) — no `O_EXCL`, no `O_NOFOLLOW`, no randomised name. `readJournal` (`:55`) and `ClearForceSingleJournal` (`:106`) likewise operate on an unvalidated path.

**Scenario (empirically reproduced, not hypothesised):** `scripts/install.sh:491` creates `/var/lib/tether` as `-o tether -g tether -m 0750` — unprivileged-writable. `docs/cluster-runbook.md:355` documents the recovery as `sudo tether cluster recovery force-single`. A compromised `tether` account runs `ln -s /etc/systemd/system/tether-broker.service /var/lib/tether/.force-single.journal.tmp`. The operator's next recovery follows the link as uid 0, truncates the unit file, and writes journal JSON into it, `err == nil`. `TestS6S8Round6JournalTempRefusesSymlink` FAILS today with the sentinel's contents replaced.

**This is a boundary the repo already defends:** `install.sh:494-495` keeps `$ETC_DIR` root-owned specifically because *"a tether-owned $ETC_DIR would be a tether->root local privesc"*. And the repo's own writer convention is `O_EXCL` (`internal/natsconf/takeover.go:216-217`) or randomised `CreateTemp`+rename (`takeover.go:190-213`). `journal.go` is the one new writer below that bar.

**Fix:** `os.CreateTemp(dataDir, ".force-single.journal-*")` (O_EXCL-based, unguessable) + chmod 0600 + fsync + rename + fsync dir, mirroring `takeover.go:190-213`. Open `readJournal` with `O_NOFOLLOW`. Keep the round-6 test as the pin.

*Framing correction the parent should note:* the package **compiles**; this is a test failure, not a build failure. And `s6_s8_round6_external_review_test.go` is a round-6 reviewer artifact (mtime 11:51) that postdates the delta (11:11-11:14) — so "the delta ships a red suite" is rhetoric. The vulnerable code is nonetheless the delta's own new file, so the finding lands regardless.

---

### R2 — MAJOR — a root-created `tether.lock` permanently refuses the daemon's start; the runbook manufactures it, even on a no-op
`internal/cluster/datadirlock.go:46` · *lane: daemon-lock-interlock (B3-1)*

**Wrong:** `AcquireDataDirLock` opens with `os.OpenFile(path, O_RDWR|O_CREATE, 0o600)` and treats **every** open failure as fatal (`:47-49`); `Broker.Run` converts that into a hard start refusal (`broker.go:645-651`). Only `unix.Flock` returning EWOULDBLOCK actually means "someone holds it" — but the printed remedy (`:649-650`, *"let the recovery finish" / "stop the previous broker"*) is emitted for EACCES too, where **neither is running**.

**Scenario:** N=2, brk2 dead. Operator follows `docs/cluster-runbook.md:355` verbatim: `sudo tether cluster recovery force-single --self-id brk1 --confirm-peers-dead brk2`. `offline.go:73` prints an advisory WARN only; `offline.go:74` `acquireFlock` creates `/var/lib/tether/tether.lock` as **root:root 0600 — before** the bolt probe (`:81`), the empty-state refuse (`:86`), the exchange precheck (`:99`) and the liveness gate (`:133`). The command then aborts at any gate → **zero mutations**. Operator restarts the broker: unit is `User=tether` (`install.sh:737`), no `CAP_DAC_OVERRIDE` in the unit (`:735-756`) — dir ownership does not grant file access — → EACCES → `Restart=always`/`RestartSec=2` (`:753-754`) crash-loops the **only surviving broker** until StartLimit (deliberately retained, `:751-752`) parks it `failed`, needing `reset-failed`. The operator is sent hunting for a nonexistent process.

Same ordering at `offline.go:223`, `:701`, `init.go:155`, `restore.go:75`. `RootAgainstNonRootDir` is WARN-by-design (`offline.go:930-955`). Nothing chowns the lock; `install.sh` never pre-creates it. The drills cannot catch it — they standardise on `-u tether` (`drills/22:87`) and `README:368` declares #6 out of scope.

**Before this delta**, a root-owned lock cost only "a later offline op is denied" — exactly what `docs/broker-ops.md:622` (#6) documents. **This delta escalates a documented, bounded gotcha into a total outage.**

**Fix:** (1) split the taxonomy — EACCES/EPERM → a distinct, actionable error naming `ls -l` + `chown tether:tether`, never claiming a holder; (2) have the offline tools chown the lock to the data-dir owner after `O_CREATE` when euid==0 (reuse the `os.Lchown` pattern at `cluster/offline.go:401`), and/or have `install.sh` pre-create it `-o tether -g tether -m 0660`; (3) fix `docs/cluster-runbook.md:355/403/423/428/473` to `sudo -u tether`; (4) update broker-ops §8.6 #6 with the new blast radius.

---

### R3 — MAJOR — a stale `phaseStarted` journal grants a permanent, invisible standing `--confirm-peers-dead` → split-brain
`internal/clusteroffline/offline.go:139` · *lane: journal-crash-consistency (J2)*

**Wrong:** the journal is written at `phaseStarted` **before the first mutation** (`:139-145`) and is **never removed on any error path** — `:149`, `:155`, `:162`, `:170`, `:173` all return with it in place; the only `Remove` is `ClearForceSingleJournal`, reachable solely from the CLI success path (`cmd/tether/cluster_offline.go:215`). `resumeConfirmation` (`journal.go:131-150`) is then applied **unconditionally** on every future run (`offline.go:114`) — it never reads `prior.Phase` and never checks whether the peer is still in the roster. Its doc's claim that *"honouring it on resume adds no new authority"* (`journal.go:128-130`) is **false** once the journal records no progress: the confirmation is a point-in-time human assertion (*"brk-c is TRULY dead RIGHT NOW, not merely partitioned"*, `cluster_offline.go:176`) that the code converts into a permanent grant. It is also **invisible**: the prompt (`cluster_offline.go:174-177`) prints only the operator's typed list, never the merged set.

**Scenario:** brk-a/b/c. Quorum lost; operator runs `force-single --confirm-peers-dead brk-b,brk-c`. Gates pass → journal `{Phase:started, ConfirmedDead:[brk-b,brk-c]}` → `RecoverSingleNode` fails cleanly at `cluster/offline.go:470-473` (`storage.OpenWAL` → "open recovery DB") → **brk-a unmutated**. Disk repaired, cluster healthy for months, journal still there. New incident: brk-b dead, **brk-c alive but partitioned**. Operator runs `force-single --confirm-peers-dead brk-b`. Prompt says *"Confirm the peers [brk-b] are TRULY dead"*. `resumeConfirmation` silently adds brk-c; `checkPeersDead`'s "must list EVERY peer" gate (`:433-438`) is satisfied; `probePeer(brk-c)` fails across the partition → "dead". **Brain split** — via the one interlock this command exists to enforce, with the operator never shown brk-c. The liveness probe is no backstop: a partitioned peer fails the probe, which is *precisely* why `offline.go:436` says the typed gate exists.

**Fix:** add `phaseRosterPruned`, written in/immediately after `pruneRosterPeers`'s commit; honour `prior.ConfirmedDead` **only** at `phaseRosterPruned`/`phaseRaftRebuilt`. At `phaseStarted` the roster is intact, so the operator can and must re-type — nothing is lost. Independently: print the **effective merged** set at the prompt, and require an explicit `--resume-interrupted` before any journalled confirmation is honoured.

---

### R4 — MAJOR — the delta's new WARN string re-arms the exact B1 SIGPIPE brick in drill 20, and swallows B1's new non-zero exit
`internal/clusteroffline/offline.go:184` · *lane: harness-and-drills (HD-1)*

**Wrong:** the delta changed the final WARN. The staged baseline (`git show :internal/clusteroffline/offline.go`) read *"force-single complete; node is now a single-voter cluster…"* — **no** occurrence of `de-clustered to standalone`. The working tree now emits *"…nats.conf must still be **de-clustered to standalone** (the caller does this next…)"* at `:184` — on stderr (`Logger` defaults to `slog.Default()`, `:70-71`), as the last output **before** `ForceSingle` returns and **before** the CLI's `deClusterStandaloneConf` (`cmd/tether/cluster_offline.go:195`). That substring previously existed **only** in the success line at `cluster_offline.go:194`, emitted *after* the de-cluster.

`test/simcluster/drills/20-forcesingle-natsconf.sh:35-36` pipes the **mutating** force-single into `grep -q 'de-clustered to standalone'`. The delta moved the match token into the brick window. Two failures:

1. **Deterministic, no race:** POSIX pipeline rc = grep's rc. A run where the de-cluster genuinely fails — B1's new hard error (`cluster_offline.go:196-205`) — still makes `assert_ok` **PASS**. The entire point of the B1 CLI remediation is invisible to drill 20.
2. **SIGPIPE truncation:** `grep -q` exits at the WARN line, closing the pipe; force-single dies before the de-cluster → nats.conf stays clustered → survivor exit-70 crash-loop. Drill 20:35-36 reports PASS; 20:39-40 (`! grep -qE '^cluster' …`) then FAILS, **blaming the product for #20 being unfixed** — verbatim the round-5 §M1 mis-attribution this round exists to eliminate. The SIGPIPE mechanism through `$SIM exec` → `docker exec` → `pty-confirm.py` → tether is not speculative: `assert.sh:145-149` and `s6-s8-external-review-round5.md:12,223` record it as empirically isolated over three controlled runs on drill 91, whose command shape is identical to drill 20:36.

Drill 20 is not in `lint-drills.sh:24`'s frozen `BATCH`, so the delta's own new sigpipe rule reports it as `advisory` and exits 0.

**Fix:** (a) make `:184`'s WARN not contain the CLI's success signature — e.g. *"nats.conf is still CLUSTERED; the de-cluster phase has not run yet"* — and give the CLI a unique final token; (b) fix drill 20:35-36 the way 91/42 were fixed: `assert_ok` the bare command, let 20:39-40 carry the post-condition; (c) make the sigpipe rule HARD for every drill, or at minimum add 20/12/74 to `BATCH` — an advisory that never fails is not a guard.

---

### R5 — MAJOR — `InterruptedForceSingle` has ZERO production callers; the diagnosability half of B1 is unimplemented and its doc states the opposite
`internal/clusteroffline/journal.go:116` · *lanes: journal (J5), controller (CE-7 residue)*

**Wrong:** `journal.go:116-118` claims *"Callers (the broker's startup diagnostics, `cluster status`, the drills) use it to name the state instead of leaving the operator to decode an exit-70 crash-loop."* `grep -rn InterruptedForceSingle --include=*.go --include=*.sh .` returns exactly the declaration (`:119`), that comment, and `force_single_round5_test.go:65/71/78`. The `broker.go` delta adds only the B3 lock; it never reads the journal. **No documentation surfaces it either** — `grep -rn journal docs/cluster-runbook.md docs/cluster.md` returns nothing.

**Scenario:** node bricks at `Phase=raft_rebuilt` (the canonical SIGPIPE case that produced the round-5 Fail — where the CLI never reaches its re-run advice at `cluster_offline.go:200-206`). Operator SSHs in: `status=70`, crash-loop. `journalctl` and `cluster status` (which cannot reach the dead broker) name neither `/var/lib/tether/.force-single.journal`, nor its phase, nor the remedy. The operator's only route to the fix is reading source. **B1's forward-completion rests entirely on the operator happening to re-run "the single most dangerous cluster command" (`cluster_offline.go:137`) unprompted on a half-recovered node.** They will not.

Compounding: `TestRound5B1_InterruptedForceSingleIsReportable` (`:63`) asserts *reportable* while proving only that a getter round-trips a struct.

**Fix:** (1) in the broker's fail-closed cluster-mode startup path, alongside the exit-70, call `InterruptedForceSingle(ClusterDataDir)` and refuse with a named error (*"an interrupted force-single for brk-a (phase raft_rebuilt) is journalled here; re-run `tether cluster recovery force-single --self-id brk-a --self-addr …` to forward-complete"*); (2) surface it in `cluster status`'s local/offline branch; (3) assert the string in a drill; (4) make the reportability test drive the broker/status path, not the getter.

---

### R6 — MAJOR — false green: the entire B1 journal integration is deletable with the suite green
`internal/clusteroffline/force_single_round5_test.go:18` · *lanes: journal (J1), test-adequacy (TG-1, TG-6)* — **independently reproduced by two lanes**

See §Test non-vacuity row 1. `grep -rln 'writeJournal|readJournal|journalPath|InterruptedForceSingle|ClearForceSingleJournal' --include=*_test.go` returns **only** `force_single_round5_test.go` and the round-6 reviewer test — no test in the repo asserts a journal on any real `ForceSingle` run. The three real `ForceSingle` callers (`offline_test.go:55`, `s6_s8_resnapshot_external_review_test.go:111`, `test/d7/integration_test.go:414` — build-tag-gated) were not extended with a single assertion.

The fix itself is **correct** — one lane wrote the missing end-to-end test, and it passes on real code and fails on the mutant. This is purely a test gap. But it is *the* gap round-5 filed.

---

### R7 — MAJOR — false green: the CLI de-cluster failure path (non-zero exit + clear-only-on-success) has no test at all
`cmd/tether/cluster_offline.go:201` · *lane: test-adequacy (TG-2)*

See §Test non-vacuity row 2. This is the half round-5 called out most sharply — *"the code only printed a WARNING and returned nil"*, i.e. reported **exit 0 = success for a bricked node**. The delta fixes it and pins it with nothing. Reverting `:201` **and** deleting `ClearForceSingleJournal` at `:215` leaves `go test ./cmd/tether/` → `ok`.

The reordering hazard is as bad as the revert: move the clear above the de-cluster block and no test fails, but a stale journal is then deleted while the node is still un-bootable — destroying the only record of the in-flight run and re-creating the brick.

---

### R8 — MAJOR — false green: `Broker.Run`'s lifetime lock is deletable; the test locks the helper by hand instead of starting a broker
`internal/clusteroffline/force_single_round5_test.go:105` · *lanes: daemon-lock (B3-2), test-adequacy (TG-3)* — **independently reproduced by two lanes**

See §Test non-vacuity row 4. `TestRound5B3_DataDirLockIsOneSSOTAndExcludes` **passes with B3's fix deleted** — the test bearing B3's name is green with B3 gone. The only `Run`-with-`ClusterDataDir` harness is `test/d9/setup_test.go:202-216`, behind `//go:build d9_integration` (excluded from `make test`), giving each broker its own `t.TempDir()` and asserting nothing about the lock.

**Consequence of an undetected revert:** the interlock degrades to the pre-round-5 one-shot bolt probe; a systemd-revived daemon opens the OLD `raft/raft.db`, ACKs client writes into it, and `RebuildSingleNodeFromDB`'s exchange+cleanup deletes that inode — **silent committed-write loss**, the exact scenario B3 exists to prevent, with nothing red.

*Adjudicated [self-verified]:* one lane called the `:106` SSOT assertion "unable to fail under any edit"; a verifier corrected that a *divergent* edit would trip it. Both agree it is vacuous **with respect to B3** — it never touches `Broker.Run`. That is what matters. Assert the observable instead: that `Run` locks literally `filepath.Join(dataDir, "tether.lock")`.

---

### R9 — MAJOR — false green: the B2 `AtomicExchangeCapable` precondition can be removed from `ForceSingle` with the suite green, and the test is near-vacuous on its own terms
`internal/clusteroffline/force_single_round5_test.go:85` · *lanes: atomic-exchange (B2-VACUOUS-TEST), test-adequacy (TG-4)* — **independently reproduced by two lanes**

See §Test non-vacuity row 3. Deleting the single line `offline.go:100` leaves `TestRound5B2_AtomicExchangeIsPrecheckedNotFallenBackOn` **passing**. The test's name promises *IsPrechecked* and *NotFallenBackOn*; it verifies neither clause. `grep -rn 'errExchangeUnsupported|ErrAtomicExchangeUnsupported|swapDirs|exchangeDirs' --include=*_test.go` → **nothing**: on Linux tmpfs/ext4 RENAME_EXCHANGE always succeeds and CI is ubuntu-only (`.github/workflows/ci.yml:18,38,60`), so B2's entire new fail-closed refusal path is dead code to the suite.

**Revert scenario:** someone deletes `:100` as "a redundant probe costing two syscalls per recovery" — it *looks* redundant, since `swapDirs` still returns `ErrAtomicExchangeUnsupported`. Suite green, ships. Now the failure is discovered inside `RebuildSingleNodeFromDB` at `:366` — **after** `RecoverSingleNode`, the marker, and the roster prune have all committed. That is precisely round-5 B2, restored, with nothing red.

---

### R10 — MAJOR — false green: all three new S5-15 escalation branches are untested and **structurally untestable**
`internal/broker/cluster_operation_controller.go:600` · *lane: controller-escalation (CE-1)*

See §Test non-vacuity row 5. The three `blockAfterAttempts` call sites (`:600-602`, `:610-615`, `:802-804`) are guarded by `deriveAndConvergeSeedsFromRoster()` (a concrete method, `seed_converge.go:160`) and `a.node.Propose(...)` where `a.node` is a concrete `*cluster.Node` (`clusteradmin.go:48`) — **no injectable seam**, in deliberate contrast to `caughtUpFn`/`streamsReadyFn` (`clusteradmin.go:76-77`), which exist precisely so the ladder's other gates can be fault-injected. No test can force these to fail, so no test can enter the escalation branch. `g3_seed_helper_test.go:110` drives the `err == nil` success path and asserts only `OpStateRetired` — it pins the older G3/#91-A3 fix (convergence is *called*), not the S5-15 fix (a *failure escalates*).

**Fix:** add `seedConvergeFn func() error` mirroring the existing pattern; wire it in `wireClusterLate`; test as specified in the table.

---

### R11 — MODERATE — the BLOCKED state the new retire site routes to advertises `cluster ops abort`, which the delta's own comment says must not happen
`internal/broker/cluster_operation_controller.go:666` · *lane: controller (CE-2)* — **downgraded from MAJOR**

**Wrong:** `blockAfterAttempts` emits a generic, step-agnostic message (`:665-667`): *"…`cluster ops confirm X` to retry or `cluster ops abort X`"*. The new retire escalation at `:802-804` fires at `OpStateNatsRolledOut` — **after** `RemoveServer` (`:772-775`) and the roster delete (`:776-782`) have committed. The delta's own comment at `:799-801` states the hazard verbatim: *"`cluster ops abort` would mark ABORTED a retire that physically completed"*. **The escalation introduced to avoid the abort hazard prints the abort instruction.** `AbortOp` (`:271-279`) has no guard.

**Scenario:** 3-broker retire of brk3 completes physically; `PlanClusterSeedsPublish` fails 5 ticks → BLOCKED with a message telling the operator to abort. They abort. `cluster ops list` now reports ABORTED for a retire that fully happened; the op is terminal so the controller never retries the seed publish.

**Downgraded because** (a) `AbortOp`'s documented contract (`:268-270`) is *"transitions to ABORTED **without touching the substrate**; reconcile/doctor heals"* — ABORTED means "operator abandoned the op", which is confusing, not false; (b) "advertises the retired host forever" is refuted: `deriveAndConvergeSeedsFromRoster` runs at the tail of **every** membership commit (`driveJoin:600`, `driveRetire:802`, `clusterdrain.go:174`, `force_single_online.go:300`, `seed_converge.go:210-211`) plus the leadership-edge backstop (`clusteradmin.go:356`); `seed_converge.go:203-208`'s "no leadership edge fires" caveat is scoped to single-voter force-single, not a 3→2 retire.

**Fix:** make `blockAfterAttempts` step-aware (suppress the abort clause post-irreversible); guard `AbortOp` to refuse a retire at/after `OpStateRaftRemoved`; if an escape is needed, add `cluster ops finalize` → RETIRED with a loud `last_error`, never ABORTED.

---

### R12 — MODERATE — `cluster ops confirm` rewinds a physically-completed retire to DRAIN_REQUESTED, writing an orphan drain marker for a deleted roster row
`internal/broker/cluster_operation_controller.go:251` · *lane: controller (CE-3)*

**Wrong:** `ConfirmOp:251-252` routes **any** blocked retire to `OpStateDrainRequested`. That was correct when the only retire→BLOCKED path was `retireGatePasses`' F==0 gate (`:829-832`), which fires **before** the irreversible steps. **The delta's `:802-804` is the first path that blocks a retire at `NATS_ROLLED_OUT`** — after `RemoveServer` and the roster delete — and `ConfirmOp` was not updated. So this is a defect the delta *creates*.

**Scenario:** retire blocked at `NATS_ROLLED_OUT`; operator confirms. `retireGatePasses` returns true immediately at `:818-819` (`!sub.isVoter`); `NO_NEW_HOME` (`:689-695`) Proposes `PlanClusterDrainSet(op.TargetNode, &d)` → `membership_ops.go:578-590` shows `cluster_meta` is a free-form key/value table with **no FK** to `cluster_nodes`, so `draining:brk3` is written for a deleted node — exactly the orphan class `retireGatePasses:823-825` already concedes is harmful. `RAFT_REMOVED` falls through every guard to `toNatsRolledOut` (`:867-877`), which re-reads the **current** `topology_generation` as the op's new target — a generation this op's removal did not cause. If an unrelated `cluster add brk4` bumped it, the op pins at `NATS_ROLLED_OUT` on a goalpost it never set, `assertNoActiveOp` (`:23-35`) fences brk3's membership plane throughout, and the orphan marker persists (the drain-clear at `:798` sits behind the convergence gate). **That is gotcha #45 — the exact outcome S5-15 was introduced to prevent.**

**Fix:** record the blocking step on the op row; on confirm, re-enter at the step that failed (`NATS_ROLLED_OUT`), keeping the DRAIN_REQUESTED rewind only for the pre-irreversible F==0 gate.

---

### R13 — MODERATE — a crash in the prune→exchange window is not cleanly forward-completable: the resume re-runs the liveness veto the journal cannot relax
`internal/clusteroffline/offline.go:128` · *lane: journal (J3)* — **downgraded from MAJOR**

**Wrong:** the post-`RecoverCluster` snapshot predates the direct-SQL prune (stated by the delta's own comment at `:157-159`). On a resume in the prune→exchange window, `readRoster` (`:124`) returns empty, but `previewRecoveredRoster` (`:128`) replays the old snapshot and revives the abandoned peers, `mergePeers` (`:132`) re-adds them, and `checkPeersDead` re-runs the TCP liveness veto (`:439-444`) — which `resumeConfirmation` cannot relax, since it satisfies only the *naming* half (`:433-438`). The window is wide: `RebuildSingleNodeFromDB` stages a whole replacement store (`cluster/offline.go:296-362`), proportional to DB size.

**Scenario:** power cut after `pruneRosterPeers` commits. brk-a reboots → clustered nats.conf at N=1 → exit 70 crash-loop; journal says `Phase=started`. Operator powers brk-b back on to salvage data (the CLI's own next-step text at `cluster_offline.go:220` treats this as sequenced after the survivor is up). Re-run → `probePeer(brk-b)` connects → *"HARD-REFUSE — peer brk-b accepted a TCP connection…; it is ALIVE, force-single would split-brain"*. Refusing is **useless**: the prune already committed, so the abandonment cannot be undone by refusing.

**Downgraded because** the veto keys on a live TCP connect — powering the revived peer back down (the state the operator asserted with `--confirm-peers-dead`) lets the resume proceed. So it is an undiscoverable ordering trap with a misdirecting error string, not the un-recoverable brick.

**Fix:** once a journal proves the prune committed (R14's `phaseRosterPruned`), do **not** re-derive the roster or re-run the gates — forward-complete from the journal's `SelfID`/`SelfRaftAddr`. At minimum, suppress the naming gate and the liveness veto for IDs in `prior.ConfirmedDead` when `prior.Phase >= phaseRosterPruned`, and say loudly that the abandonment already committed and the peer must be stopped/wiped.

---

### R14 — MODERATE — `prior.Phase` is decorative: it drives no control flow, so the journal cannot distinguish "nothing happened" from "the prune committed"
`internal/clusteroffline/offline.go:114` · *lane: journal (J6)* — **the shared root of R3 and R13**

**Wrong:** two phases are defined (`journal.go:39-42`) and written (`offline.go:141`, `:176`), but `prior` is used for exactly three things: the SelfID mismatch refusal (`:111-113`), `resumeConfirmation` (`:114`), and the `if prior == nil` skip-rewrite guard (`:139`). **`prior.Phase` appears only in a `Warn` log line (`:117`).** No branch reads it.

Consequences: (a) a resume unconditionally re-executes `RecoverSingleNode` → `raiseForceSingleMarker` → prune → `readAppliedIndexPath` → `RebuildSingleNodeFromDB` even when the journal proves they are done — "forward-completion" is *redo everything and hope it is idempotent*, and nothing tests that idempotence (R6); (b) there is **no phase recorded between the prune commit (`:161`) and the exchange (`:172`)** — the only window where `resumeConfirmation` is load-bearing at all; (c) at `raft_rebuilt` the roster is already empty, so `checkPeersDead` passes trivially and `resumeConfirmation` contributes nothing there either; (d) `:139`'s `if prior == nil` means a resume never persists the merged `confirmed` until `:176`, so extra names supplied on a resume are lost to a second interruption.

**Scenario:** journal at `raft_rebuilt` (only the nats.conf de-cluster outstanding). Re-run → a **second full disk surgery** — re-`RecoverCluster` over the rebuilt store, re-restore the FSM snapshot over `tether.db`, re-raise the marker, rebuild+exchange the store again — on a node whose only defect was one config file, with a fresh crash window each time.

**Fix:** make Phase authoritative. Add `phaseRosterPruned` (same commit as the prune) and switch on `prior.Phase`: at `phaseRaftRebuilt` return immediately so the CLI performs only the de-cluster; at `phaseRosterPruned` skip the roster re-derivation and the gates; at `phaseStarted` treat the run as fresh and do **not** honour `prior.ConfirmedDead`. Re-write the journal on resume.

---

### R15 — MODERATE — the precheck picks the rare post-prune failure cause and ignores the common one (ENOSPC staging ~2× `tether.db`)
`internal/cluster/offline.go:296` · *lane: atomic-exchange (B2-ENOSPC-BLIND)*

**Wrong:** B2's remit (`offline.go:415-417`, `clusteroffline/offline.go:97-99`) is that no failure may land after the prune. But between the prune (`clusteroffline/offline.go:161`) and the exchange (`offline.go:366`), `RebuildSingleNodeFromDB` writes a full second copy of the FSM: `os.MkdirTemp(dataDir, ".raft-rebuild-")` (`:296`) then `Persist(sink)` (`:346`), and `snapshot.go:50-69` shows this is `os.CreateTemp(s.tmpDir, "snap-*.db")` → full modernc online backup → `io.Copy(sink, f)` streaming a **second** full copy, with the temp still present. **Peak ≈ 2× `tether.db` inside the very partition being recovered.** The atomic exchange itself is metadata-only and essentially never fails for space — the probe validates the cheap thing and skips the expensive one. Nothing on this path calls `Statfs` (grep: zero hits).

**Scenario:** a broker force-singled after a disk-pressure incident — the canonical case (`drills/21-smalldisk-tierb.sh` exists precisely because small-disk brokers are a known live config). Prune + marker commit; `Persist` → ENOSPC at `:346-349`; CLI aborts at `cluster_offline.go:186-188` **before** the de-cluster (`:195`) → clustered nats.conf → exit 70. Every re-run repeats it identically.

**Fix:** extend the precheck to `PreflightRebuild(dataDir, dbPath)` at `clusteroffline/offline.go:100`: `unix.Statfs(dataDir)`, refuse when `Bavail*Bsize` < `fi.Size(dbPath) × ≥2` + margin, naming bytes needed vs available. And state in the doc comment which post-prune failure classes are excluded and which are **not**, so the next reviewer is not misled by the absolute claim at `offline.go:364-365`.

---

### R16 — MODERATE — the probe exchanges two fresh siblings; the real exchange targets `dataDir/raft`, which can be a mountpoint (EBUSY) → post-prune brick
`internal/cluster/offline.go:423` · *lane: atomic-exchange (B2-PROBE-PROVES-WRONG-THING)* — **downgraded from MAJOR**

**Wrong:** `AtomicExchangeCapable` (`:423-442`) exchanges `dataDir/.xchg-probe-a-*` ↔ `dataDir/.xchg-probe-b-*` — two empty, fresh, same-parent, non-mountpoint dirs. The real swap (`:366` → `:450`) exchanges `dataDir/.raft-rebuild-*/raft` ↔ `dataDir/raft`, a long-lived object that may be a **mountpoint**. **Reproduced:** under `unshare -rm` with tmpfs at `$R/raft`, `Renameat2(RENAME_EXCHANGE, $R/stage/raft → $R/raft)` → **EBUSY (0x10)**, while the probe's own shape succeeds. EBUSY is not in `exchangedir_linux.go:31`'s set `{EINVAL, ENOSYS, EOPNOTSUPP, ENOTTY}`, so it escapes raw via `swapDirs`' `default: return err` (`:456-457`) — **after the prune**. `offline.go:364-365`'s claim (*"an unsupported filesystem never reaches this point"*) is false. Side note: **ENOTTY is not a renameat2 errno at all** — the set was guessed, not derived from rename(2).

**Downgraded because** the reachability is narrow and the overlayfs half was **refuted**: no tether-created or documented layout mounts `raft/` separately (`install.sh` makes only `/var/lib/tether`; `install.sh:553` comment *"data_dir: $LIB_DIR/raft # presence of raft/ here = cluster mode"*; `docs/cluster.md:166`; `test/simcluster/lib/docker.sh:42/44` mounts the **whole** data dir). On a real overlay, the lower-layer destination gives EXDEV **and so does the probe's own shape** → `AtomicExchangeCapable` refuses fail-closed. And it is repairable: umount/relocate and re-run.

**Fix:** probe the real geometry. `unix.Statfs`/`Stat` both `dataDir` and `dataDir/raft` and refuse when `st_dev` differs (subsumes the mountpoint case cheaply and deterministically); `os.Lstat` and refuse a symlink (see R19); classify EBUSY/EXDEV as their own refusal with an actionable message rather than letting them escape from a post-prune path; drop ENOTTY and cite rename(2) for each remaining member.

---

### R17 — MODERATE — the attempt counter is keyed by OpID alone; the two new `driveJoin` sites share one retry budget and the BLOCKED message misreports the count
`internal/broker/cluster_operation_controller.go:659` · *lane: controller (CE-4)*

**Wrong:** `blockAfterAttempts` does `a.opAttempts[op.OpID]++` (`:659-660`) — no step component (`clusteradmin.go:82-83`). Its doc (`:652-655`) claims *"opMaxAttempts **consecutive** failures"*, but no step-success resets it: `clearOpAttempts` runs only at `ConfirmOp:239`, `driveJoin:617`, `driveRetire:806`, `toNatsRolledOut:876`. **The delta makes this acute:** `:600-602` and `:610-615` are two distinct blocking steps inside the **same** `case cluster.OpStateNatsRolledOut` (`:581`), sharing one budget, with the clear at `:617` reachable only after **both** succeed.

**Scenario:** ticks 1-3 seed-converge fails (transient RODB flap) → counter=3. Tick 4 seed-converge **succeeds** (no reset) but `PlanClearGrowActive` fails → 4. Tick 5 grow-lock fails again → 5 ≥ `opMaxAttempts` → BLOCKED with *"release cluster-add grow lock failed 5 times"*. **False on both counts:** it failed twice, and it received 2 of its nominal 5 attempts. The operator investigates raft/`PlanClearGrowActive` while the real signal was a healed read flap — and the membership plane is fenced by `assertNoActiveOp` throughout. Symmetrically, a genuinely-failing step can be starved by an unrelated recovered one.

**Fix:** key on `op.OpID+"/"+what` and clear by prefix on a state advance; or reset on each step's success so "consecutive" becomes true. Fix the doc at `:652-655`.

---

### R18 — MODERATE — the `#20`/B1 nats.conf post-condition in drills 91/42 is fail-open: negative-only grep, no existence check, no positive control
`test/simcluster/drills/91-client-converge.sh:147`, `drills/42-rejoin-returning.sh:98` · *lane: harness (HD-2)* — **downgraded from MAJOR**

**Wrong:** `sh -c "! $SIM exec $SURV -- grep -qE '^cluster[[:space:]]*\{' /etc/tether/nats.d/nats.conf"`. The `!` collapses three distinct outcomes into PASS: grep rc=2 (file missing/unreadable — proven: `sh -c "! grep -q X /tmp/NOPE"` → rc=0), `$SIM exec` rc≠0 (container hiccup, wrong node id — proven: `sh -c '! false'` → rc=0), and a genuine no-match. No existence precondition, no positive control anywhere.

**Downgraded because** (a) **scope**: both asserts are **staged** (`git status` shows `A drills/91…`, `A drills/42…`) — the reviewer's baseline, not this delta; (b) the headline scenario (change the renderer to `cluster: {` → both pass forever on a clustered conf) **cannot occur**: `deClusterStandaloneConf` verifies its own output in-process against the **parsed** key (`cmd/tether/cluster_offline.go:120-126`; `internal/natsconf/preflight.go:299-303`), so a renderer change yields a standalone conf the regex correctly fails to match; (c) the regression it exists to catch **is** caught today — `internal/natscluster/config.go:118` is `b.WriteString("cluster {\n")`, a column-0 literal the pattern matches. The reachable-today false pass is a docker hiccup at that instant.

**Fix:** make it positive and fail-closed — `test -f` the conf, a positive control (`grep -qE "^(jetstream|server_name)"` proving it is a real rendered conf), then the negative. Better: assert the product's own parsed verdict (`natsconf.Preflight`-backed) so the drill and the product agree on what "clustered" means. Add a negative-control fixture in `tests/` that must FAIL on a conf containing `cluster {`.

---

### R19 — MODERATE — the sigpipe lint cannot enforce its own rule: enumerative on both sides, one live false positive, no invoker
`test/simcluster/tests/lint-drills.sh:48` · *lane: harness (HD-3)* — **downgraded from MAJOR**

**Wrong:** a single line-regex requiring, on one physical line: literal `tether`, one of 11 hard-coded verbs, `|`, literal `grep -q`. Probed against the exact regex, all of these pass clean: `91:37`'s `_publish_ok() { S publish … | grep -qE … }` (the wrapper `S()` hides the literal `tether`, and `seeds publish` is not enumerated); `force-single … | head -1`; `| grep -m1`; `grep -E -q`; `grep --quiet`; `42:172`'s `join prepare … | head -1`. Meanwhile the **safe** capture-then-grep pattern the delta itself recommends — `_o=$(dexec -u tether brk1 -- tether cluster retire brk2 2>&1); printf … | grep -q done` — is **FLAGGED**, contradicting the rule's own stated exception (`:46-47`). The drills are built on wrappers (`S()`, `REC()`, `_remote()`), so the literal-`tether` premise is defeated by the codebase's own idiom. And `grep -rl 'lint-drills'` returns only the file, README and docs — **no Makefile target, no `run-drills.sh` call, no CI**.

**Downgraded because** (a) `lint-drills.sh` is **staged**, not part of this delta; (b) both "live violations" are harmless — `cluster seeds publish` completes its entire mutation at the admin round-trip (`cmd/tether/cluster_seeds.go:42`) **before the first byte of output** (`:53`), so grep's early exit truncates printing, never the mutation; same for `join prepare | head -1`.

**Fix:** stop enumerating. Ban truncators generally (`grep …(-[a-zA-Z]*q|--quiet|-m ?[0-9])|head|read|awk …exit`); treat **any** pipeline whose left side is not a pure capture as suspect and require an explicit `# lint-ok: read-only` opt-out so the safe cases annotate themselves. Add `tests/lint-drills-selftest.sh` with the 9 FN + 2 FP fixtures — without it the rule's coverage is unmeasurable, and this finding is the proof. Wire it into a Makefile target and `run-drills.sh` preflight.

---

### R20 — MODERATE — drill 91's "settled-liveness" gate checks nothing it claims: NRestarts is never read, and it is a first-passing-sample denylist
`test/simcluster/drills/91-client-converge.sh:157` · *lane: harness (HD-5)* — **downgraded from MAJOR**

**Wrong:** the comment (`:150-155`) promises *"NRestarts stops climbing AND the admin socket actually answers. A crash-loop now fails HERE, naming itself."* Three divergences: (1) `grep -n NRestarts` returns **only line 152 — the comment**; no `systemctl show -p NRestarts` exists; (2) `poll_until` (`lib/log.sh:30-37`) returns 0 on the **first** passing sample — a crash-loop under `Restart=always` periodically *presents* a good instant, so polling 60s at 4s intervals is the most likely way to **find** one, not rule one out. Settledness needs two samples separated in time; (3) the liveness half is a 4-item **denylist** behind `!` — proven: `runuser: user tether does not exist` → PASS; `bash: tether: command not found` → PASS; `Error: unknown flag` → PASS. Only the ENOENT socket text fails, and only because `cmd/tether/admin.go:215` happens to wrap transport errors as `unavailErr("admin socket %s: %w", …)`, coupled with nothing pinning it.

**Downgraded because** what saves it today is real: the exit-70 return (`broker.go:957-965`) fires **before** the admin socket is created (`:1019-1058`), so a crash-looper provably has no socket. And the "break runuser/tether" variants are unreachable in drill 91 — the same invocation is already load-bearing at `91:141-142` and `91:163`, so a broken binary lands SETUP-RED first. It is an accident of the current start order, not a property the gate enforces.

**Fix:** implement the comment — sample `systemctl show -p NRestarts --value` twice, ~12s apart, require equality; and make liveness **positive** (match what a healthy broker prints: `force_single_active|voters|leader`), never the absence of four error strings.

---

### R21 — MODERATE — the `out_matches` rc=0 gate is inert at its only call site, covered by zero tests, and its diagnostic is discarded
`test/simcluster/lib/assert.sh:161` · *lane: harness (HD-4)* — **this IS the entire unstaged simcluster change**

**Wrong:** the added `[ "$_om_rc" = 0 ] || { … tail -3 >&2; return 1; }` cannot change the verdict at its sole call site. `grep -rn out_matches test/simcluster/` → only `assert.sh`, two comment lines in `lint-drills.sh:44,49`, and `92-js503-remote-alert.sh:87,89`. That call uses `--online --dry-run`, and `internal/broker/force_single_online.go:207-208` is `if req.DryRun { return adminsock.Response{Op: op, OK: true, ForceSingle: &rep} }` — `OK: true` **unconditionally**, computed *after* `code, reason := b.fsArmVerdict(…)` and ignoring `code`. So `if !arm.OK` (`cluster_offline.go:265`) is never taken and `:268-270` returns rc=0 whether gates pass or fail. 100% of the discrimination is the `'would proceed'` signature. The header's motivating example (`echo would proceed; exit 9`) is not a shape any real caller produces. (The `callAdmin`-error path does trip the gate — but the signature grep already returned 1 there, so the verdict is still unchanged.)

Additionally the new `tail -3 >&2` is **dead**: the call site is inside `poll_until` (`92:88-89`), and `lib/log.sh:31` runs `if "$@" >/dev/null 2>&1`, discarding it on all ~24 iterations. And `grep -c out_matches tests/verdict-contract-test.sh` = 0.

**Fix:** the five hermetic cases in §Test non-vacuity row 6, plus a sentinel-file test pinning the anti-SIGPIPE property. Drop the `tail -3 >&2` or move it where `poll_until` cannot swallow it.

---

### R22 — MODERATE — the lock refusal exits 70 (UNCLASSIFIED = "a tether bug"), the one code the repo says a retry-wrapper must never retry
`internal/broker/broker.go:648` · *lane: daemon-lock (B3-3)*

**Wrong:** `:648-650` returns a plain `fmt.Errorf` wrapping cluster's plain `fmt.Errorf` (`datadirlock.go:47/51`) — no `*ExitError`. `classifyExit` (`cmd/tether/exitcode.go:64-88`) falls through every sentinel to `return exitInternal` (`:87`), and `:32` defines `exitInternal = 70 // EX_SOFTWARE: … UNCLASSIFIED (default)`. `error_hints.go:96`: *"70 is reserved for genuine internal failures"*; `b6_skew_exit_test.go:19`: 70 means *"a tether bug a retry-wrapper would retry forever"*. Both real causes are already first-class: "a recovery is in flight, come back later" **is** `exitTransient = 75`, and R2's EACCES **is** `exitNoPerm = 77`. The one new hard-refusal on broker startup is the one that lies to every monitor.

**Scenario:** a long `cluster recovery restore` holds the flock; the operator forgets `systemctl mask`; systemd revives the broker every 2s, exiting 70. The monitor pages on-call with "broker software failure" for an expected, transient, self-healing condition it would have suppressed at 75. Meanwhile StartLimit (deliberately retained, `install.sh:751-752`) trips **during** the recovery, so when the restore finishes the unit is parked `failed` and does not return until someone knows to `systemctl reset-failed`.

**Fix:** classify at the source — `ErrDataDirLockHeld` (EWOULDBLOCK) vs `ErrDataDirLockUnusable` (EACCES/EPERM/EROFS), pairing with R2's split; map to 75/77 in `cmd/tether/serve.go` via `unavailErr`/`permErr` (`exitcode.go:47-58`); add to the classifier's table test. Replace the wrong *"stop the previous broker"* hint with `systemctl mask`.

---

### R23 — MODERATE — the journal is written root:root 0600 into a tether-owned data dir, so a mixed-uid recovery aborts before any gate
`internal/clusteroffline/journal.go:78` · *lane: journal (J7)*

**Wrong:** `writeJournal` hardcodes `0o600` with no ownership mirroring, unlike the raft rebuild's explicit `mirrorTreeOwnership` (`cluster/offline.go:361-362`), whose comment cites this exact *"root-run rebuild → User=tether EACCES-crash-loop FOREVER"* failure. The package already names the hazard (`RootAgainstNonRootDir`, `offline.go:926-955`) and warns on every run (`warnRootDataDirOwner`, `:73`). Under the tether uid, `os.ReadFile` of a root:root 0600 journal returns EACCES — **not** `os.ErrNotExist` — so `readJournal`'s ErrNotExist branch (`journal.go:56-58`) does not fire; it returns the hard error at `:60`, which `ForceSingle` propagates at `:108-110` **before any gate**.

**Scenario:** `sudo tether cluster recovery force-single …` (the runbook form, `cluster_offline.go:148`) journals as root:root 0600, then is SIGPIPE-killed after the prune. On reboot the operator, following the `warnRootDataDirOwner` nudge this very function emits, retries as `sudo -u tether` → *"read force-single journal: permission denied"* → abort. The resume is impossible as tether; as root it works. Once R5 is wired in, a root-written journal also makes the broker's diagnosis a permission error rather than a diagnosis.

**Fix:** chown the journal to the data dir's uid/gid after the rename (as `mirrorTreeOwnership` does), and widen the error at `journal.go:60` to name the uid mismatch and the remedy.

---

### R24 — MINOR — both `exchangedir_*.go` files still instruct the caller to fall back to a non-atomic swap — the exact behaviour B2 removed
`internal/cluster/exchangedir_linux.go:20`, `exchangedir_other.go:15,19` · *lanes: atomic-exchange (B2-STALE-FALLBACK-DOCS), test-adequacy (TG-7)*

Verbatim, in the file that **defines** the sentinel: *"The caller MUST fall back to a non-atomic swap (and say so)"* (`linux:20`); *"lets the caller fall back instead of failing AFTER the roster prune has already mutated the node"* (`linux:16-17`); *"the caller falls back to a non-atomic swap and says so"* (`other:15`); *"so the caller takes the documented non-atomic path"* (`other:19`). All four now contradict the code: `swapDirs` (`offline.go:450-459`) refuses, and `:445-446` asserts *"there is deliberately NO non-atomic fallback"*.

**Failure:** given R9 proves nothing pins the fallback's absence, these comments are the **standing instruction** a future maintainer will follow to re-introduce it — fully green.

Secondary: `AtomicExchangeCapable:437-438` returns the bare sentinel, **discarding** `errExchangeUnsupported`, so on darwin the operator sees only the ext4/xfs/btrfs advice and never *"Linux-only"*. (The darwin refusal itself is not a user-flow regression: `scripts/install.sh:467` already dies with *"--role broker is not supported on macOS"*, and `GOOS=darwin GOARCH=arm64 go build ./...` succeeds — so the delta does not break the release artifact.)

**Fix:** rewrite all four headers to the new contract (the sentinel means the caller **MUST REFUSE**; no fallback, because a three-step rename passes through a raft-less state *after* the prune — cite round-5 B2). Wrap rather than replace: `fmt.Errorf("%w (%v)", ErrAtomicExchangeUnsupported, err)`.

---

### R25 — MINOR — a symlinked `dataDir/raft` exchanges successfully and silently orphans the old store forever
`internal/cluster/offline.go:285` · *lane: atomic-exchange (B2-SYMLINK-RAFT-ORPHANS)*

**Reproduced:** with `$R2/raft` a symlink to `/tmp/realraft`, `Renameat2(RENAME_EXCHANGE, $R2/stage/raft → $R2/raft)` returns **nil**; afterwards `$R2/raft` is a real directory and the **symlink has moved into stageRoot**, where the deferred `os.RemoveAll(stageRoot)` (`:300`) unlinks only the link — `/tmp/realraft` survives, intact and unreferenced. This contradicts the doc at `:285-288` (*"the old store is removed only after the exchange"*). Reachable: `RaftStateExists` (`:61-79`) opens `raft.db` **through** the symlink and reports true. `mirrorTreeOwnership` compounds it — `os.Stat` (`:384`) follows the link, so ownership is mirrored from the target while the swap operates on the link.

**Damage:** a latent orphaned pre-force-single multi-voter store the code believes it deleted, invisible to every tether command and re-linkable by a later well-meaning admin.

**Fix:** refuse up front — `os.Lstat(filepath.Join(dataDir,"raft"))` in the precheck and reject a symlink, naming the link and target. Fix the comment at `:285-288`: `stagedRaft` is `dataDir/.raft-rebuild-*/raft` (a *nephew*, not a "sibling"), and the old store's removal is **best-effort cleanup** — a SIGKILL between `:366` and the deferred `:300` retains `.raft-rebuild-*` forever with no reconciler and no runbook mention.

---

### R26 — MINOR — `cluster ops confirm` on a join blocked at the new sites rewinds to ROSTER_COMMITTED and can re-BLOCK a fully-joined voter with a false "catch-up exceeded the deadline"
`internal/broker/cluster_operation_controller.go:259` · *lane: controller (CE-5)* — **downgraded from MODERATE**

The routing defect is real: `ConfirmOp:259-260` routes any blocked join to `OpStateRosterCommitted`; its comment (`:231-235`) names only the two **pre-promotion** causes (catch-up deadline `:578-580`, exhausted AddVoter `:563`); the delta's new sites (`:600`, `:610`) are **post-promotion** and `ConfirmOp` was not updated. A confirm re-captures a fresh barrier (`:478-491`) and a fresh `adaptiveCatchupDeadline` for a node that has been serving as a VOTER.

**Downgraded because** the rewind is fully guarded, not lucky: `RAFT_ADDING` skips `AddNonvoter` (`:501`) and `setPhase` (`:529`); `CATCHING_UP` skips `AddVoter` (`:551`) and `setPhase` (`:562`). The deterministic cost is a wasted barrier capture and one extra probe. The claimed harm (a false BLOCK at `:578-580`) needs a healthy promoted voter to lag a fresh barrier by more than an **adaptive** deadline (2min base, scaled by DB size, `:641-650`) — materially weaker than its sibling R12, whose orphan write lands on every confirm.

**Fix:** route a join blocked at/after `NATS_ROLLED_OUT` back to `NATS_ROLLED_OUT` — the ladder there is already idempotent (`:589-591`, `seed_converge.go:180-183`, `:604-608`).

---

### R27 — MINOR — two independent implementations of the one "SSOT" lock, and the in-code contract comment says the daemon does not take it
`internal/cluster/offline.go:40`, `internal/cluster/datadirlock.go:44-58` vs `internal/clusteroffline/offline.go:911-925` · *lane: daemon-lock (B3-5)*

The delta SSOTs the lock's **name** (`clusteroffline/offline.go:33`) but not its **acquisition**: two hand-copied `LOCK_EX|LOCK_NB` implementations. `TestRound5B3` pins only the filename equality and mutual exclusion, and it is **asymmetric**: it acquires via `cluster.AcquireDataDirLock` first, so dropping `LOCK_NB` on the *offline* side hangs the test (caught) while dropping it on the **daemon** side is invisible — a daemon that BLOCKS instead of refusing wedges `Type=simple` startup (`install.sh:736`) indefinitely with no log past `Run`'s entry: systemd reports `activating` forever and the operator sees a wedged broker with **no error at all**. Worse than the deletion.

Separately, `internal/cluster/offline.go:40-42` — the file that owns the interlock's documentation — still reads *"the production daemon does NOT take ${DataDir}/tether.lock until D9, but it ALWAYS holds the raft.db bolt lock while running"*. D9 has shipped and this delta makes the daemon take it.

*Adjudicated [self-verified]:* the lane's claim that the **architecture ruler is stale and CLAUDE.md §2's docs-lead-code rule is violated** is **REFUTED**. `docs/distributed-broker-architecture.md:284` is a D7-scoped decision record that already defers daemon locking forward (*"daemon 端取锁是 D9 cutover"*), and `:365(g)` already documents the post-D9 contract (*"cluster 模式 daemon D9 起取 tether.lock"*). **The doc led the code.**

**Fix:** delete `clusteroffline.acquireFlock`; have all five call sites (`offline.go:74`, `:223`, `:701`, `init.go:155`, `restore.go:75`) call `cluster.AcquireDataDirLock` — then the flags cannot drift. Rewrite `cluster/offline.go:38-42` to the post-B3 contract. Sequence **after** R2's ownership/taxonomy split, not before.

---

### R28 — MINOR — `datadirlock.go`'s and `broker.go`'s "in both directions" claim has no cluster-mode qualifier
`internal/cluster/datadirlock.go:22`, `internal/broker/broker.go:642-643` · *lane: daemon-lock (B3-4)* — **downgraded from MODERATE**

The comments assert the interlock is real *"in both directions — an offline op cannot start under a live daemon, and a daemon cannot start while an offline op is mid-surgery"*, unqualified, while `Run` gates on `ClusterDataDir != ""` (`:645`) — equivalent to cluster mode (`cutover.go:47-65` makes data_dir-set + raft-absent FATAL).

**Downgraded (thesis refuted):** the lane argued the coverage is "inverted — present where redundant, absent where load-bearing". Both halves fail. (a) In cluster mode the flock is **not** redundant with the bolt lock: `RaftStoreLockedByDaemon` (`cluster/offline.go:43-56`) is a one-shot probe at op start, and the surgery then **closes** the bolt store, so a daemon arriving mid-surgery is invisible to it — only the daemon's own startup flock refuses it. The lane inverts the direction of protection. (b) The single-mode scope is a **documented, designed** decision: `docs/distributed-broker-architecture.md:365(g)` explicitly scopes the daemon lock to cluster mode, explicitly identifies the never-clustered-live-daemon hole, and names the SQLite-busy probe (`init.go:176-183`) as its designated mitigation. Pre-existing, accepted, outside B3's scope. The proposed fix (lock in every single-mode broker) would contradict the ruler and, as the lane concedes, multiply R2's blast radius.

**Fix:** just qualify the comments — cluster-mode daemons only.

---

### R29 — MINOR — `opAttempts` leaks a map entry for every op that ends via abort / RETIRE_FAILED
`internal/broker/cluster_operation_controller.go:665` · *lane: controller (CE-6)*

`blockAfterAttempts` escalates at `:664-668` and returns without deleting the counter. `AbortOp` (`:271-279`) and the terminal RETIRE_FAILED route (`:826`) never call `clearOpAttempts`. `a.opAttempts` is pruned by key, never swept; a leader runs for months. Bounded by the number of ops the leader escalates/fails — hygiene. The delta's new `:802-804` adds a fresh, easily-triggered producer of exactly the abort-after-BLOCKED sequence (R11) that leaks.

**Fix:** centralise — have `transition` call `clearOpAttempts` on any `terminal=true` commit, subsuming `:617`/`:806`/`:876`.

---

### R30 — MINOR — `lint-drills.sh` always claims "legacy advisory findings tracked" on runs that scanned nothing
`test/simcluster/tests/lint-drills.sh:78` · *lane: harness (HD-6)*

`LEGACY=0` (`:56`) is a **non-empty string**, so `${LEGACY:+; legacy advisory findings tracked}` always expands. Verified: `sh tests/lint-drills.sh` (no `--all`, legacy loop `:66-75` never entered) prints `lint-drills: batch OK (9 S6-S8 drills, 0 violations); legacy advisory findings tracked`, rc=0.

This is **already logged as round-5 item (o), verdict R1-7 CONFIRMED** (`docs/reviews/s6-s8-round5-review.md:462`), with the fix spelled out. **The delta touches this file (adding the sigpipe rule at `:42-49`) and does not fix it.** And the misreading is load-bearing *here*: the hidden advisory findings include `20-forcesingle-natsconf: sigpipe-truncation` — R4's live brick.

**Fix:** gate on the flag **and** a numeric comparison; print the count. Use `${X:+}` only where empty is the intended sentinel.

---

### R31 — MINOR — `mirrorTreeOwnership`'s "fresh node → return nil" branch documents a scenario the next line cannot survive
`internal/cluster/offline.go:386` · *lane: atomic-exchange (B2-MIRROR-MISSING-RAFT-ENOENT)* — **downgraded from MODERATE**

`:384-390` returns nil on `os.IsNotExist(ref)` with the comment *"no live store to mirror (fresh node) — leave the staged ownership as-is"*. The next statement is `swapDirs(stagedRaft, liveRaft)` (`:366`) → RENAME_EXCHANGE with a **non-existent** newpath → **ENOENT** (empirically confirmed), not in the unsupported set, escaping as `"cluster: swap the rebuilt raft store into place: no such file or directory"`. The branch's premise is false.

**Downgraded:** unreachable and **pre-existing**. `RebuildSingleNodeFromDB` has exactly one caller (`clusteroffline/offline.go:172`), gated by `RaftStateExists` (`:88-95`), which fails ENOENT first. And `git diff` shows the pre-delta `swapDirs` had the same `default: return err` — a missing live dir returned the same raw ENOENT before the fix. Not a delta regression.

**Fix:** add an explicit precondition at the top of the exported `RebuildSingleNodeFromDB` (`os.Stat(liveRaft)` → a named error), **before** the staging work; then delete or correct the unreachable branch's comment.

---

## REFUTED (do not act)

| Claim | Refuting citation |
|---|---|
| **CE-7 — "the delta does not compile: `cmd/tether/cluster_offline.go:212` is a syntax error (`_ = dataDir` collapsed onto one line), and the B1 journal-clear call has been eaten; `ClearForceSingleJournal` has ZERO production callers."** | **Flatly false. [self-verified]** `go build ./...` and `go build ./cmd/...` both **succeed**; `go vet ./cmd/tether/` is clean. There is no `_ = dataDir` anywhere in the file. The statement the B1 comment (`:210-211`) introduces is present and correct at `:215-217`, and it does **not** discard the error: `if jerr := clusteroffline.ClearForceSingleJournal(dataDir); jerr != nil { return fmt.Errorf("force-single: completed but could not clear the recovery journal: %w", jerr) }`, placed after the de-cluster block and before the success `Fprintf` at `:218`. `grep -rn ClearForceSingleJournal --include=*.go .` returns `cmd/tether/cluster_offline.go:215` alongside `journal.go:105` and the test. Three other lanes independently cite `:215` as the call site. The reviewer read a stale or mid-edit buffer. Since CE-7 was offered as "dispositive" for CE-2's operator-facing path, its collapse removes that objection. *(Its one true residue — `InterruptedForceSingle` has no production caller — survives as **R5**.)* |
| **CE-8 — "a completed force-single permanently auto-supplies the operator's old `--confirm-peers-dead` list to every later run."** | Premise falsified with CE-7. The clear at `cmd/tether/cluster_offline.go:215` runs on the success path, exactly where `journal.go:103-104` specifies. So no journal survives a successful run: `offline.go:107` `readJournal` → `(nil, nil)`; `:114` `resumeConfirmation` returns `current` unchanged via `journal.go:132-134` (`if j == nil { return current }`); no confirmation is unioned in; no spurious "resuming an INTERRUPTED force-single" warning. The re-grown-cluster scenario cannot occur. *(The narrower hardening residue — `resumeConfirmation` records no timestamp or roster generation, so an abandoned journal lingers — is subsumed by **R3**.)* |
| **B3-4 (thesis) — "the interlock's coverage is inverted: present where redundant, absent where load-bearing; extend the lock to every single-mode broker."** | Refuted on both halves. The cluster-mode flock is **not** redundant with the bolt lock (`cluster/offline.go:43-56` is a one-shot probe; the surgery closes the bolt store, so only the daemon's own startup flock refuses a mid-surgery arrival). And the single-mode scope is designed and documented: `docs/distributed-broker-architecture.md:365(g)` scopes the daemon lock to cluster mode, names the never-clustered hole, and names the SQLite-busy probe as its mitigation. Acting on the proposed fix would contradict the ruler and multiply R2. *(Comment-scope residue → **R28**.)* |
| **B3-5 (sub-claim) — "the architecture ruler is stale (`distributed-broker-architecture.md:284`) and CLAUDE.md §2's docs-before-code rule is violated."** | `:284` is a D7-scoped decision record, correct as a statement about D7, and already defers daemon locking forward (*"daemon 端取锁是 D9 cutover"*); `:365(g)` already documents the post-D9 contract. **The doc led the code.** *(In-code comment residue → **R27**.)* |
| **B2-PROBE (overlayfs half) — "an un-copied-up overlayfs lower-layer `raft/` passes the probe then EXDEVs."** | Not substantiated. On a real overlay (lowerdir/upperdir/workdir), EXDEV (0x12) is returned for the lower-layer destination **and for the probe's own two-fresh-upper-sibling shape** — so the probe fails too and `AtomicExchangeCapable` refuses fail-closed (wrong message, no post-prune brick). `redirect_dir=N` on this kernel; the asymmetric case could not be tested. *(The mountpoint/EBUSY half survives as **R16**.)* |
| **HD-2 (headline) — "change the renderer to `cluster: {` and both 91/42 asserts pass forever on a fully-clustered conf."** | Cannot occur. `deClusterStandaloneConf` verifies its own output in-process against the **parsed** key via `natsconf.Preflight` + `IsStandaloneJetStream` (`cmd/tether/cluster_offline.go:120-126`; `internal/natsconf/preflight.go:299-303`). A renderer change yields a *standalone* conf the regex correctly fails to match, never a clustered-but-passing one. And the regression the assert exists to catch **is** caught today: `internal/natscluster/config.go:118` is `b.WriteString("cluster {\n")`, a column-0 literal the pattern matches. *(Fail-open hygiene survives as **R18**.)* |
| **HD-3 / HD-5 (live-violation claims) — `91:37`'s `S publish \| grep -qE` and `42:172`'s `join prepare \| head -1` are live SIGPIPE bricks; breaking `runuser` makes drill 91's gate report SETTLED.** | Both harmless. `cluster seeds publish` completes its entire mutation at the admin round-trip (`cmd/tether/cluster_seeds.go:42`, `OpClusterSeedsPublish`) **before the first byte of output** (`:53`) — the early grep exit truncates printing, never the mutation; same structure for `join prepare`. And a broken `runuser`/binary lands SETUP-RED at `91:141-142` long before the settled gate is evaluated. *(The enforcement-gap and denylist defects survive as **R19**/**R20**.)* |
| **TG-7 (as a defect) — "B2 changes force-single from 'degrades on darwin' to 'refused on darwin'."** | The finding concedes *"Refusing is the right call per B2"*, which dissolves its own scenario: a macOS operator being refused is **the fix working as designed**. `scripts/install.sh:467` already dies with *"--role broker is not supported on macOS"*, so no legitimate flow reaches an offline rebuild on darwin, and `GOOS=darwin GOARCH=arm64 go build ./...` succeeds — the release artifact is unaffected. *(The stale comments survive as **R24**, which the lane actually **under**-reports: it cites only `exchangedir_other.go` and misses the identical dead claim at `exchangedir_linux.go:20`. The "refusal path is untested" half is a duplicate of **R9** — fix once, do not count twice.)* |
| **TG-3 (sub-claim) — "the `:106` SSOT assertion cannot fail under any edit."** | Overstated. It is a compile-time-constant comparison (`offline.go:33`) and so is dead against the literal-substitution regression it names, but a **divergent** edit (`lockFileName = "tether-offline.lock"`) would trip the `t.Fatalf`. *(Irrelevant to the verdict: the assertion is vacuous with respect to B3 because it never touches `Broker.Run` — that survives as **R8**.)* |
| **TG-5 (framing) — "the package does not compile green" / "the delta ships a red suite."** | The package **compiles**; it is a test failure. And `s6_s8_round6_external_review_test.go` (mtime 11:51, alongside `s6-s8-external-review-round6-tasklist.md` at 11:50) postdates the delta (11:11-11:14) and is not in its file list — it is a round-6 reviewer artifact, not something the author shipped. *(The suite is nevertheless red right now and the vulnerable code is the delta's own `journal.go`, so **R1** stands on the merits.)* |

## UNCERTAIN

None. Every lane finding resolved to CONFIRMED, DOWNGRADE, or REFUTED with a citation.

---

## Closing note on the delta's own claims

Three assertions in the delta's text are false as written and should be corrected regardless of the fixes above, because each is the artifact that will stop the *next* reviewer from re-checking:

- `force_single_round5_test.go:13` — *"Each test FAILS if its fix is reverted (that is the point: round-5 M-lane proved five earlier fixes could be deleted wholesale with the suite still green)."* Reverting all four fixes simultaneously leaves all five tests green.
- `internal/cluster/offline.go:364-365` — *"The capability was proven by AtomicExchangeCapable before any mutation began, so an unsupported filesystem never reaches this point."* EBUSY and ENOSPC both do.
- `journal.go:116-118` — *"Callers (the broker's startup diagnostics, `cluster status`, the drills) use it…"* There are none.

---

# 主进程裁决（round-6 自审查）— 全部采纳，无驳回

> 自审查判 **DO NOT SHIP**，判得对。最刺痛的是它证明我**犯了自己在 round-5 报告里批评过的同一个罪**：
> 我写在 `force_single_round5_test.go:13` 的「Each test FAILS if its fix is reverted」**是假话**——两条 lane
> 各自独立用 mutation 证明三个修复都能整体删除而 `make test` 仍绿。

## 已修 + 逐条 mutation 验证

| 项 | 修复 | mutation 证明 |
|---|---|---|
| **R6/M6 测试空洞（最核心）** | 新增 `force_single_callsite_round6_test.go`（5 条）+ `internal/broker/datadirlock_round6_test.go`（4 条），**绑定真实调用点**而非纯助手 | 删 B2 预检调用点 → **FAIL**；删 B1 other-node 检查 → **FAIL**；删 ForceSingle flock → **FAIL**；`Broker.Run` 锁置 `if false` → **FAIL**；恢复后全绿 |
| **R2 我引入的更大事故** | `AcquireDataDirLock` 拆分错误分类：`ErrDataDirLockUnusable`（EACCES/权限）≠ `ErrDataDirLockHeld`（真争用）；root 跑时把锁**属主镜像**成 data-dir 属主；错误串给出**真正的补救（chown）**而非"停 broker" | `TestRound6_UnusableLockIsNotReportedAsContention` 断言权限问题绝不被误报为争用 |
| **R1 符号链接提权** | `writeJournal` 改 `os.CreateTemp`（O_EXCL + 不可预测名）；`readJournal` 加 **O_NOFOLLOW**（ELOOP 明确报错） | 两条 symlink 测试：受害文件必须原封不动 |
| **R3 陈旧 journal → 脑裂** | `resumeConfirmation(current, j, roster)`：**只有已被 prune 出 roster 的 peer 才继承**确认；仍在 roster 的必须**重新人工确认** | 测试断言"journal 泄漏给 roster 内 peer"即失败；partial-prune 只让已 prune 的继承 |
| **R4 我的 WARN 重新武装了 SIGPIPE** | 改写内部 WARN，**不再包含 drill 20 的终末签名 `de-clustered to standalone`** | 静态核验：该串现在只出现在 CLI 的终末成功行 |
| **R10/ordering** | journal 识别 + 能力预检**移到状态检查之前**（纯只读、fail-fast，且让中断态先被指名） | B2 调用点测试正是靠这个顺序才可观测 |

## 未修（如实登记，附精确修法）

- **M5**：`InterruptedForceSingle` 仍**零生产调用者** —— B1 的"可诊断性"那一半仍是死代码。修法：接入 broker 启动诊断 + `cluster status` + 一条 drill 断言。
- **M7**：`blockAfterAttempts` 需 step-aware；`AbortOp` 在 `RaftRemoved` 之后需守卫（否则把物理已完成的 retire 标成 ABORTED）；`ConfirmOp` 对卡在 `NATS_ROLLED_OUT` 的 retire 的 rewind。
- **B2 残留（诚实降级，非"已闭合"）**：预检交换的是**两个空的兄弟目录**，而真实交换的目标是长期存在的 `dataDir/raft` —— 独立挂载点会给 **EBUSY**（不在 unsupported 集合内、逃逸为裸 errno，且发生在 prune **之后**）；**ENOSPC 完全未探测**，而 `RebuildSingleNodeFromDB` 在 prune 与 exchange 之间要写约 2× `tether.db`。所以：**rename 现在是崩溃一致的，事务仍不是**。代码里那句"an unsupported filesystem never reaches this point"是**过度声称**，应改。
- **R13**：prune→exchange 这个 journal 最该救的窗口，resume 会用**陈旧的 pre-prune 快照**重新推导 roster 并重跑存活否决，`resumeConfirmation` 按设计不能放宽它 —— 需要 phase-aware 的跳过。
- 一条 review lane **违反了只读约束**，把 `s6_s8_round6_external_review_test.go` 写进了仓库。我保留了该测试（有价值），但把它过严的断言（要求 `writeJournal` 必须报错）改成断言**安全属性**（受害文件不被破坏）——它测的是实现选择，不是边界。

## 本轮硬闸

`make lint` 0 issues · `make test` 0 失败 · `go test -race ./internal/clusteroffline ./internal/cluster ./internal/broker` 全过 · linux + darwin(arm64) 构建通过 · 9 条新回归**逐条 mutation 验证非空洞**。
