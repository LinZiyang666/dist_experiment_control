# S6–S8 (G-B) — Stage-C internal review, round 5

**Object:** the git staging area (`git diff --cached`) at the time of review, including the unplanned product-code surgery landed by the external reviewer (codex) in round 4.
**Lanes:** raft-recovery, startup-probe-store, concurrency-race, cluster-ops-control-plane, cli-gates-ux, verdict-contract-b1b2, test-adequacy, mandate-ssot — each with an adversarial verifier.
**Verdict of this report: the batch must not land as staged.** There is a hard build break on a shipped platform, a documented-runbook path that bricks the last surviving broker, and a raft-store swap that defeats the only daemon interlock the code has.

---

## Round-4 blocker status (independent judgment)

### B1 — "PRODUCT-RED/INCOMPLETE made non-blocking without owner authorization" → **PARTIALLY CLOSED. Residual OPEN.**

I read `test/simcluster/run-drills.sh:220-250` myself. The *core* of B1 is genuinely fixed and I confirm it:

- `run-drills.sh:228-229` — PRODUCT-RED and INCOMPLETE each increment `blockers` **by default**; the waiver is opt-in only.
- `run-drills.sh:243-247` — with a waiver present the summary prints `WAIVED NON-GREEN — PRODUCT-RED=n INCOMPLETE=n; explicit owner waiver flags supplied. NOT all-green.` It structurally cannot print `ALL GREEN`.
- No caller (`remote.sh`, `Makefile`) hardcodes either flag.

What is still **OPEN**: the waiver is a **single boolean applied to every drill** (`run-drills.sh:51-52,83-84`). There is no expected-verdict baseline and no per-drill waiver, so an operator who waives a *known* PRODUCT-RED (e.g. `31-node-upgrade-fleet` / #28, `README.md:292`, `lib/assert.sh:35-37`) simultaneously waives every *new, unexpected* PRODUCT-RED in the same run. Round 4 asked for **per-item**; per-item was not delivered.

I explicitly **reject** the lane's stronger framing that "the blind state is the only reachable state": `README.md:296-300` disclaims the S6–S8 verdict table as a superseded round-3 snapshot and states "no drill is *expected non-green* in a way that permits release," and the simcluster tier is not in CI (`CLAUDE.md` §5), so no automated gate is currently blinded.

**What must change to close B1:** accept `--allow-product-red=<drill>[,<drill>…]` / `--allow-incomplete=<drill>[,…]` (or an on-disk `expected-verdicts.txt` the runner diffs), make a non-waived drill's PRODUCT-RED/INCOMPLETE a blocker, and flag the inverse (a waived drill that comes back GREEN = stale waiver). Reject the bare boolean form as an unknown option.

### B2 — "runner grammar/uniqueness/enum not validated; retry washing MISMATCH into ALL GREEN" → **SUBSTANTIALLY CLOSED. Two narrow residuals.**

Read from source (`run-drills.sh:146-174`), the parser now does everything B2 demanded:

- uniqueness: `count != 1` → `CONTRACT-ERROR` (`:149-150`);
- anchored grammar + enum alternation + every counter numeric (`:152`);
- semantic cross-check per verdict (GREEN ⇒ all counters 0; INCOMPLETE ⇒ `af=0 ∧ sr=0 ∧ pr=0 ∧ nc>0`; etc., `:157-163`);
- **verdict-rc ↔ process-rc agreement** (`:165`) — this is what kills the `drill_end; exit N` class;
- `is_flake` (`:171-174`) admits **only** `INFRA-ABORT|SETUP-RED`, so a CONTRACT-ERROR is never retried.

Residual **OPEN** items:

1. **A retried SETUP-RED that passes on attempt 2 still produces the headline `ALL GREEN` and exit 0** (`:242-243`). The `(retried)` tag (`:225`) and `attempt1.log` preserve evidence, but the headline and the exit code — what a caller consumes — assert a clean suite. Round 4's own position is that preserved evidence is not a substitute for the gate. **Fix:** if `${#retried[@]} -gt 0`, print `GREEN AFTER RETRY — n drill(s) failed on attempt 1` and require an explicit `--allow-retry-green` for exit 0.
2. **The producer is never joined to the parser** (see S5-13). `tests/verdict-contract-test.sh` drives `drill_end` with hand-rolled looser checks (`:36-46`, `:141`) and drives `effective_verdict` with hand-written `printf` lines (`:159-174`). A one-token counter regression in `lib/assert.sh:199` leaves the hermetic gate at ALL PASS while every real drill lands CONTRACT-ERROR on the sim server.

**Bottom line: B1 is OPEN (per-item never delivered). B2 is OPEN only in the two narrow forms above.** Neither is the reason to fail this batch; the product surgery is.

---

## Product-code risk assessment

**Not safe to land as staged.** The unplanned surgery touches the two most destructive surfaces in the repo — offline raft-store rebuild and the topology reconciler's hard SIGKILL of nats-server — and it was written with no design review, no cross-platform gate, and (per S5-06/07/08/09) essentially no behavioral test coverage: five independent mutation experiments across two lanes disabled the new fixes wholesale and the suite stayed green.

Concretely, before any of this can land:

**MUST fix (blocking):**
1. **S5-01** — `GOOS=darwin go build ./cmd/tether` fails. `build/goreleaser.yaml` ships darwin/amd64+arm64; the release job cannot produce **any** artifact. Reproduced.
2. **S5-02** — a verbatim `sudo tether cluster recovery force-single` per `docs/cluster-runbook.md:355` leaves `raft/` as `root:root 0700` and permanently crash-loops the `User=tether` daemon on the **last surviving broker**, with an EACCES message naming neither ownership nor force-single, and every documented recovery command also fails EACCES.
3. **S5-03** — the directory swap ignores the bolt lock that `internal/cluster/offline.go:38-42` declares to be *the* offline-vs-daemon interlock, and the deferred `RemoveAll` unlinks the old store out from under a revived daemon → silent loss of acked writes.
4. **S5-04** — the four-step force-single sequence has no interrupted-op marker; a failed step 4 leaves the exact snapshot/DB divergence step 4 exists to prevent, and the node starts clean and poisons the next `cluster add`.
5. **S5-05** — `driveJoin` clears `cluster_grow_active` at terminal SERVING, opening a concurrent-upgrade/concurrent-grow window during P8 rebalance that `internal/broker/cluster_upgrade_trigger.go:150-152`'s own comment says the marker exists to close.

**MUST additionally test before landing (each proven uncovered by mutation):** the ForceSingle liveness gate's *refusing* direction (S5-06), the roster-evacuation call site (S5-07), the `clusterHealthResponder` composition (S5-08), `driveJoin`'s grow-lock release (S5-09), and the `Broker.Run` → `StableTunnelCertDir` wiring (S5-11).

**Residual risk if the above are fixed:** the term/index rollback (S5-10) is a genuine raft persistence-invariant violation with no *currently reachable* exploit (three independent gates block the zombie path); the topology hard-restart stagger (S5-14) is safety-critical logic whose only test certifies a property it does not exercise. Both should be fixed rather than waived, because the arguments that make them non-exploitable are emergent (nonvoter vote gate, catch-up-gated `AddVoter`, `RestartSec=2`), not designed.

---

## Surviving findings — most severe first

### S5-01 · MAJOR · `internal/cluster/offline.go:356` — Linux-only `Renameat2`/`RENAME_EXCHANGE` breaks the darwin build and the entire release pipeline
*(lanes: raft-recovery R1-1 + startup-probe-store R1-1 — merged, both CONFIRMED and independently reproduced)*

`offline.go:16` imports `golang.org/x/sys/unix`; `offline.go:356` calls `unix.Renameat2(unix.AT_FDCWD, stagedRaft, unix.AT_FDCWD, liveRaft, unix.RENAME_EXCHANGE)`. In `golang.org/x/sys@v0.44.0` (`go.mod:19`) both symbols exist only in the Linux build. The file has **no build tag** and no `_linux.go`/`_other.go` split, and `cmd/tether` transitively imports `internal/cluster`. `git show HEAD:internal/cluster/offline.go | grep -c 'unix\.'` = 0 — pure regression from this batch.

**Failure:** `GOOS=darwin GOARCH=arm64 go build ./cmd/tether` → `offline.go:356:17: undefined: unix.Renameat2` / `:356:84: undefined: unix.RENAME_EXCHANGE`. `build/goreleaser.yaml:41-46` declares `goos: [linux, darwin] × goarch: [amd64, arm64]` → the release job fails outright and publishes **nothing for any platform**. `.github/workflows/ci.yml:18,38,60` is ubuntu-only, so CI is structurally blind; the first signal is `git tag` or the owner's macOS gate. macOS ctl is officially supported: `scripts/install.sh:122-124` accepts `linux|darwin` and `:311`/`:467` gate only agent/broker off macOS.

**Fix:** move the swap behind `exchangeDirs(old, new string) error` with a `//go:build linux` implementation using `renameat2(RENAME_EXCHANGE)` and a `//go:build !linux` stub returning `errors.New("cluster: offline raft rebuild is Linux-only")` (broker/force-single are Linux-only anyway). Add `GOOS=darwin GOARCH=arm64 go build ./...` to the pre-commit hard gate.

---

### S5-02 · MAJOR · `internal/cluster/offline.go:296,309,356` — the swap transplants the offline op's ownership onto `raft/`; a runbook-verbatim root-run force-single permanently crash-loops the survivor
*(lane: startup-probe-store R1-2, CONFIRMED)*

`RebuildSingleNodeFromDB` builds the replacement in `os.MkdirTemp(dataDir, ".raft-rebuild-")` (`:296`) / `os.MkdirAll(stagedRaft, 0o700)` (`:309`) — every inode owned by the invoking euid — then makes it the live `raft/` (`:356`). The pre-batch path opened the **existing** `raft.db` in place, so a root-run force-single left `raft/` with its `tether` ownership and the daemon kept working.

**Failure:** N=2, brk2 dead. Operator follows `docs/cluster-runbook.md:355` verbatim: `survivor$ sudo tether cluster recovery force-single --self-id brk1 --confirm-peers-dead brk2`. It prints the advisory root WARN (`internal/clusteroffline/offline.go:73`; `RootAgainstNonRootDir` is advisory **by design**, `:884-889`) and reports success. `raft/` is now `root:root 0700`. `systemctl unmask --now tether-broker` → the `User=tether` daemon (`scripts/install.sh:738`) cannot traverse it → `internal/broker/cutover.go:81` `os.Stat` returns EACCES, which is **not** `os.ErrNotExist`, so `:85` returns the raw error → `clusterModeEnabled` → `DetectClusterMode` → `cmd/tether/serve.go:131-134` → with `Restart=always` (`install.sh:754`) the **only surviving broker** crash-loops forever on `broker: probe raft state under "/var/lib/tether": … permission denied`. Recovery is also blocked: a follow-up `sudo -u tether … force-single` dies at `RaftStoreLockedByDaemon` (`offline.go:45`), because — per the code's own doc at `internal/clusteroffline/offline.go:887-888` — an unreadable `raft.db` returns a hard error, not `ErrTimeout`.

No drill can see this: `grep -rn RebuildSingleNodeFromDB` shows the only non-test caller is the **offline** path, while `test/simcluster/drills/22-forcesingle-online.sh:32-35` exercises only `--online`, via `dexec -u tether`.

**Fix:** `os.Stat(liveRaft)` up front and `Chown`/`Chmod` the staged root, `raft.db` and every file under `snapshots/` to the original uid/gid/mode before the exchange. Additionally make `raftStateOnDisk` (`internal/broker/cutover.go:81-87`) distinguish EACCES from ErrNotExist and name the ownership hazard. Test: a table test asserting the post-exchange `raft/` retains the original uid/gid/mode, plus a drill variant of 22/41 that runs force-single **as root** and asserts the broker still starts.

---

### S5-03 · MAJOR · `internal/cluster/offline.go:300,318,356` — the swap defeats the documented bolt-lock daemon interlock; a daemon revived in the (now minutes-long) window writes into an unlinked store that is then deleted
*(lanes: startup-probe-store R1-3 CONFIRMED + raft-recovery R1-7 CONFIRMED — merged; one fix covers both)*

`internal/cluster/offline.go:38-42` states the contract: "the production daemon does NOT take `${DataDir}/tether.lock` … but it ALWAYS holds the raft.db bolt lock while running" — the bolt lock **is** the interlock. Verified: `acquireFlock` appears only under `internal/clusteroffline/` (`offline.go:74,181,659`, `init.go:155`, `restore.go:75`); zero hits in the broker/serve path.

`RebuildSingleNodeFromDB` never touches the live `raft.db`: it opens the **staged** store (`:318`, uncontended), `renameat2`s directories (`:356` — rename ignores flocks), and `defer os.RemoveAll(stageRoot)` (`:300`) then unlinks the old `raft.db` out from under whatever holds it open. The exposure window is dominated by the **new** full-DB snapshot persist (`:346` → `internal/cluster/snapshot.go:51-57`) — minutes on a multi-GB DB — where pre-batch only a fast marker+prune sat between the probe and the terminal transition.

*(Verifier correction I adopt: the window is not the full `internal/clusteroffline/offline.go:81 → :139` span, because `RecoverSingleNode` at `:115` still opens the live `raft.db` under `BoltOptions{Timeout}`. The real window is `RecoverSingleNode`'s `Close()` → the exchange, which is exactly the new snapshot-persist span.)*

**Failure:** operator stops `tether-broker` but forgets `systemctl mask` (the runbook's own §8.4(a) precondition). Step-(b) probe passes. Three minutes into the snapshot persist, a config-management/monitoring run issues `systemctl start tether-broker`; the daemon opens the now-uncontended `raft/raft.db`, takes the bolt lock, and starts committing. The exchange swaps a fresh `raft/` in and `RemoveAll` deletes the inode the daemon is fsyncing to. The daemon keeps acking writes it believes are durable; on the next restart it opens the rebuilt store and **every write since the swap is silently gone**, with no error anywhere.

Compounding (raft-recovery R1-7): `RebuildSingleNodeFromDB` is **exported** (`:289`) and its doc (`:278-288`) documents crash-atomicity but states **no caller obligations** — unlike every sibling: `RaftStateExists` (`:59-60`) "Caller must already hold the flock and have confirmed no live daemon"; `RecoverSingleNode` (`:251-252`) "Caller MUST already hold the flock, have confirmed no live daemon (the bolt lock), …". Unlike them it also never opens the live bolt store, so it cannot even incidentally notice a daemon. The sole caller is correct today; a future caller taking the signature at face value gets no protection.

**Fix:** immediately before the `renameat2`, open the **live** `raft.db` with `BoltOptions{Timeout: boltLockProbeTimeout}` and hold that handle across the exchange (or at minimum re-run `RaftStoreLockedByDaemon(dataDir)`), returning `ErrDaemonRunning`. Put that check **inside** `RebuildSingleNodeFromDB`, and document the flock obligation in the same words the siblings use. Consider unexporting it. Longer term: have the daemon take `${DataDir}/tether.lock` so the flock is a real interlock. Test: open a raftboltdb store on the live path, call `RebuildSingleNodeFromDB`, assert `ErrDaemonRunning` and a byte-identical `raft/`.

---

### S5-04 · MAJOR · `internal/clusteroffline/offline.go:115,121,128,139` — the force-single sequence is non-atomic with no fail-closed marker; a failed rebuild leaves a node that starts clean and poisons the next grow
*(lane: startup-probe-store R1-4, CONFIRMED)*

Four disk mutations, no interrupted-op record: `RecoverSingleNode` (`:115`, whose RecoverCluster snapshot embeds the **pre-prune** FSM), `raiseForceSingleMarker` (`:121`), `pruneRosterPeers` (`:128`, SQLite only), `RebuildSingleNodeFromDB` (`:139` — the step that re-snapshots the post-prune DB). The staged comment at `:123-126` names the hazard verbatim: *"RecoverCluster's snapshot was taken BEFORE this direct-SQL prune; leaving that snapshot in place lets a later resnapshot restore every abandoned peer."*

If step 4 fails — ENOSPC on the staged tree (see S5-17), EACCES, `renameat2` EINVAL, a `readAppliedIndexPath` error at `:135-138`, or **SIGKILL/power loss anywhere inside** `RebuildSingleNodeFromDB` — the disk is left in exactly that poisoned state and nothing records it. `force_single_active` is raised on both the complete and incomplete paths, so it cannot discriminate. The node starts clean: `internal/broker/cutover.go:79-88` sees raft.db + snapshot → true; `assertClusterDBConsistent` (`:98`) is satisfied by the seeded marker.

The repo already has the correct pattern and does not apply it here: `restore_in_progress` + the fail-closed `assertNoInterruptedRestore` preflight (`internal/broker/cutover.go:145-163,172-174`) exists for the identical "SIGKILL between the DB install and the raft bootstrap" class.

**Failure (silent arm — the ENOSPC arm at least returns an error):** power loss inside step 4. Operator reboots, sees `cluster status` report `force_single_active` + a `{self}` roster (the SQLite prune landed), concludes the recovery took, starts the broker — which comes up healthy. Weeks later `tether cluster add brk4`: brk4 receives the leader's stale RecoverCluster snapshot whose embedded FSM image is the **pre-prune** DB → brk2/brk3 are resurrected in brk4's `cluster_nodes` → the **signed roster** ships dead endpoints to every agent and the raft config can re-fork onto abandoned peers.

**Fix:** write a `cluster_meta` marker (`force_single_rebuild_pending`) before `:115` and clear it only after `:139` returns nil; check it in a startup preflight alongside `assertNoInterruptedRestore` (`cutover.go:151`) so a half-finished force-single **refuses to start** and tells the operator to re-run force-single. At minimum, make the `:140` error state that the node is in an unsafe intermediate state and must not be started or grown. Test: fault-inject a `RebuildSingleNodeFromDB` failure and assert the next broker start is refused.

---

### S5-05 · MAJOR · `internal/broker/cluster_operation_controller.go:605-612` — `driveJoin` releases `cluster_grow_active` at terminal SERVING, un-fencing the P8-rebalance window the marker exists to fence
*(lane: cluster-ops-control-plane R1-2, CONFIRMED)*

`:605-609` proposes `PlanClearGrowActive(op.TargetNode)` and `:612` commits terminal SERVING. From that instant **both** fences are down: `growActiveJoiner(...) == ""` (the row is DELETEd, `internal/cluster/membership_ops.go:425-432`) and `cluster.NonTerminalOperations(...)` is empty.

But the `cluster add` orchestrator is still running: `cmd/tether/cluster_add_drive.go:180` `waitJoinServing` returns on SERVING, `:186-189` runs P8 `rebalance-proxy`, and P9 `releaseGrowLock` is only at `:192`. That post-SERVING window is verbatim the one the fence documents: `internal/broker/cluster_upgrade_trigger.go:149-151` — *"The grow's join op may be briefly terminal between phases while the marker stays held (cutover/seed/rebalance), so check the marker, not just NonTerminalOperations."* **The staged change falsifies that comment's stated premise.**

**Failure:** Operator A runs `cluster add brk3`; driveJoin clears the marker and commits SERVING; the orchestrator is at P8 migrating `__proxy__` homes onto brk3. Operator B (or cron) runs `cluster upgrade`: `cluster_upgrade_trigger.go:152` sees `growActiveJoiner==""` and `:159` sees no non-terminal ops → acquire succeeds → `PlanSetUpgradeActive` commits → the rolling restart begins **while proxy homes are still rebalancing onto the just-added voter**. Pre-change B was refused. Identical hole for a concurrent `cluster add brk4` (`cluster_grow_trigger.go:76`, `:82-88`).

Secondary: the lock now has two owners — acquired by the ctl orchestrator, released by the broker controller — so the operator-visible acquire/release pairing in the `cluster add` transcript no longer reflects the real lock lifetime, and P9 is a no-op. The change's stated motivation (a permanent fence when the final NATS reply is lost) is also not actually covered: an op that stalls *short* of SERVING never reaches `:605`, so the fence leaks anyway — and per S5-15 that stall now **halts** `cluster add` before P9.

**Fix:** do not clear the marker from `driveJoin` — its contract ("a `cluster add` orchestration is in flight") is strictly wider than "the join op is non-terminal," and only the orchestrator knows P8/P9 are done. For the lost-reply case, give the marker a **timestamped lease** the leader's reconcile loop expires once the join op is terminal and no orchestrator has renewed it, or add a `cluster recovery grow-lock clear` break-glass. If the controller-side clear is kept, both acquire fences must widen to refuse while a join op reached SERVING within the last N seconds, and `cluster_upgrade_trigger.go:150-152`'s comment must be rewritten.

---

### S5-06 · MAJOR · `internal/clusteroffline/offline.go:109-110` — the anti-split-brain gate can be fully bypassed with zero test failures (no negative control)
*(lane: test-adequacy R1-1, CONFIRMED — mutation-proven)*

The fix feeds snapshot/tail-revivable peers into the liveness fence via `roster = mergePeers(roster, recoveredRoster)` (`:109`) before `checkPeersDead(roster, opts.ConfirmedDead)` (`:110`). Every `ForceSingle` caller was enumerated: `offline_test.go:55` returns at the empty-state refuse before the gate; `s6_s8_resnapshot_external_review_test.go:111-114` passes `ConfirmedDead: ["peer-stale"]` — the **accept** path only; `test/d7/integration_test.go:414` lists every peer *and* is behind `//go:build d7_integration`. `checkPeersDead`'s refuse rows (`offline_test.go:118-130`) use hand-built `[]Peer` literals that never traverse `mergePeers`.

**Mutation:** reorder so the gate sees only the SQLite roster (`checkPeersDead` first, `mergePeers` after) — removing the split-brain protection entirely — and `internal/clusteroffline` passes (`ok … 7.895s`). The refusing direction, i.e. the entire reason `mergePeers` was added, has **zero** coverage, and the uncovered surface is split-brain (`offline.go:391-395` "must list EVERY peer", `:398-401` "it is ALIVE").

**Fix:** add negative controls — (a) seed a peer reachable ONLY via the raft snapshot/tail, omit it from `ConfirmedDead`, assert the "must list EVERY peer" HARD-REFUSE; (b) give that snapshot-only peer a live TCP listener and assert the "it is ALIVE" HARD-REFUSE. Both must fail if `mergePeers` moves after `checkPeersDead`.

---

### S5-07 · MAJOR · `internal/agent/roster.go:379-381` — the proactive roster-evacuation fix (the simcluster-41/91 island) is pinned only by a pure predicate; the behavior is unpinned
*(lane: test-adequacy R1-2, CONFIRMED — mutation-proven)*

The fix is the call site (`:379-381`) plus `requestRosterReconnect` (`:440-454`: rebuilding CAS → `rebuildRequested.Store` → `nc.Close()` → sessCancel). `grep -rn 'requestRosterReconnect|refreshRosterOnce|rosterRequiresReconnect' --include=*_test.go .` returns **only** `internal/agent/s6_s8_external_review_test.go` — six direct calls to the pure predicate with struct literals. `requestRosterReconnect` and the call site have zero test references.

**Mutation:** change `:379` to `if false && accepted && …` — disabling evacuation entirely — and `internal/agent` passes (`ok … 5.429s`). Since a new function cannot fail on old code, the predicate test is a pure characterization test: nothing pins that the predicate is wired to anything, that the CAS/cancel ordering is right, or that a rebuild lands on a voter. This is the exact class round 4 left open as ASSERT-FAIL on drill 91.

**Fix:** integration test in `roster_runtime_test.go` using the existing `startNATS`/stub-responder harness — connect the agent to a stub broker, serve a signed roster flipping the connected broker to RETIRING while a second VOTER exists, assert teardown + re-register on the voter. Add a negative row asserting no churn for an all-VOTER roster.
*(Drop the lane's suggested "equal-generation re-adopt" row — `[self-verified]`: `AdoptDecision` gates on `r.Generation >= prev.RosterGen` (`roster.go:149`) and `adoptManifest` on `next.RosterGen >= a.rosterGen` (`:209`), both `>=`, so an equal-gen re-adopt returns `accepted=true`. The lane's parenthetical claim to the contrary is wrong.)*

---

### S5-08 · MAJOR · `internal/broker/cluster_health.go:77` — `healthContactStale` is unit-tested but its call site reverting to the OLD verdict fails nothing
*(lane: test-adequacy R1-3, CONFIRMED — mutation-proven)*

The fix is `:77` (`contactStale := healthContactStale(node.LeaderContactStale(t), stateLeader, writable, leaderID, node.SelfID())`). The only test (`s6_s8_external_review_test.go:29`) is a table test against the pure helper (`:143`). In-package there is nothing else; `test/d8/integration_test.go:240` calls `SubscribeClusterHealth` but is behind `//go:build d8_integration` and never asserts on the field.

**Mutation:** revert `:77` to `contactStale := node.LeaderContactStale(t)` — restoring the exact bug — and `internal/broker` passes (`ok … 25.044s`).

**Impact is worse than the banner** (verifier's independent finding, which I adopt): `internal/cluster/read.go:48-52` returns false whenever `State()==raft.Leader`, so a quorum-lost ex-leader reports `LeaderContactStale=false` — and `cmd/tether/cluster_upgrade_drive.go:176,184` uses that field as a **rolling-upgrade convergence gate** (`if … || nodeStale { return false }`). A regressed call site declares the fenced ex-leader converged and lets `cluster upgrade` restart the **next** voter.

**Fix:** responder-level test — build/inject a node in a quorum-lost/demoted-ex-leader state, invoke `clusterHealthResponder`, unmarshal the real `proto.ClusterHealthResp`, assert `LeaderContactStale==true && WritableLeaderConfirmed==false`. Keep the table test; it must not be the only coverage.

---

### S5-09 · MAJOR (test-gap) · `internal/broker/cluster_operation_controller.go:605-610` — driveJoin's new grow-lock release can be deleted wholesale with zero test failures
*(lane: test-adequacy R1-4, verifier DOWNGRADE→MODERATE on impact; I rank it here because it is the coverage twin of S5-05, which is MAJOR)*

**Mutation:** delete the whole `PlanClearGrowActive` block — `internal/broker` (ok, 25.220s) and `cmd/tether` (ok, 12.008s) both pass. `PlanClearGrowActive`/`growActiveJoiner` are referenced in tests only at `cluster_operation_controller_test.go:195-221`, which drive the plan primitive via `n.Propose` — never through `driveJoin`. `cluster_rebind_test.go:107-138` pins `caughtUpFn=false`, so the op never reaches `OpStateNatsRolledOut`. The **sibling retire fix is pinned** (`TestG3AsyncRetireTerminalIncludesSeedConvergence`, `g3_seed_helper_test.go:90-126`), so this is a real asymmetry, not a harness limit.

**Fix:** mirror the retire test — seed an op at `OpStateNatsRolledOut`, set `grow_active` to the joiner, run `driveJoin` to SERVING, assert `growActiveJoiner(RODB())==""`. Add a negative row where `grow_active` is bound to a **different** joiner and assert `driveJoin` does not clear it (the value-predicate protection the comment claims). Note: if S5-05 is fixed by removing the clear, this test becomes the inverse assertion.

---

### S5-10 · MODERATE · `internal/cluster/offline.go:318,334,337` + `internal/clusteroffline/offline.go:135` — the rebuild resets `CurrentTerm` to 0, hard-codes the snapshot term to 1, and derives its index floor from `applied_index` (not the store's high-water mark), rolling back a durable raft (index, term) claim
*(lanes: raft-recovery R1-2 + R1-3 — merged; both verifiers DOWNGRADE→MODERATE: mechanism confirmed, stated exploit refuted)*

Two coupled defects in the hand-rolled store surgery:

**(a) Term reset.** `raftboltdb.New(...)` (`:318`) creates a brand-new **empty** stable/log store, and the snapshot is created with a literal term `1` (`:337`). Nothing carries `CurrentTerm`/`LastVoteTerm`/`LastVoteCand` forward. This is a regression: the pre-change path ran only `raft.RecoverCluster`, which (verified in raft@v1.7.3 `api.go`) touches only the snapshot store and `logs.DeleteRange` and never writes `keyCurrentTerm`. Post-change the survivor comes up with `CurrentTerm` absent → 0 and `lastSnapshotTerm = 1`.

**(b) Wrong floor.** `internal/clusteroffline/offline.go:135` reads `readAppliedIndexPath(opts.DBPath)` and passes it as `baseIndex`; `internal/cluster/offline.go:334` sets `snapshotIndex := baseIndex + 1`. But `applied_index` is **not** the store's high-water mark — the repo says so itself (`internal/cluster/read.go:105-107`: "RaftAppliedIndex … advances on EVERY committed entry, including the leader-election LogNoop and config entries, which the SQLite command cursor (AppliedIndex) does NOT"), and `internal/cluster/fsm.go:108-113` returns early for `l.Type != raft.LogCommand` before any `applied_index` write, while RecoverCluster tracks `lastIndex/lastTerm` for entries of **any** type. In the normal steady state (log tail = a trailing LogNoop), `A = L-1` → the rebuild writes a snapshot at index **L** with term **1**, replacing a durable snapshot claim of (L, T=50). With a deeper noop/config tail the new snapshot index is strictly **lower** than the one it replaces. The doc at `:278-283` mis-names `applied_index` as the floor.

**Why MODERATE, not MAJOR** *(verifier work I adopt)*: the "returning zombie wins the election" exploit is blocked three ways — raft rejects vote requests from non-voters (`raft.go` `requestVote` `!hasVote`), and `cluster add` stages a joiner as a NONVOTER; promotion is gated on catch-up against a persisted barrier (`cluster_operation_controller.go:542-562`) and a term-50 zombie can never catch up because `raft.go:1455-1457` makes it ignore the survivor's term-1 AppendEntries; and `broker.go:949-968` hard-refuses to start an un-wiped ejected node, with the documented flow wiping the returning node. What remains is a real persistence-invariant violation (silent term rollback at a fixed index) plus leadership churn: a survivor at term 1 steps down whenever any stale-term node answers.

**Fix:** derive both the floor and the term from the store being replaced, not from the FSM cursor. Before the rebuild: `exists, snapIdx, snapTerm, _ := cluster.RaftSnapshotMeta(dataDir)` and `last, _ := cluster.RaftLastIndex(dataDir)` (both exist, `offline.go:86`/`:105`), plus the live store's `CurrentTerm`. Use `base = max(applied_index, snapIdx, last)` and `term = max(snapTerm, oldCurrentTerm, 1)` (`oldTerm+1` is safer — it out-terms a zombie campaigning at `oldTerm+1`), pass `term` at `:337` and `SetUint64(keyCurrentTerm, term)` on the staged store before `:322`. Have `RebuildSingleNodeFromDB` derive the base itself and **refuse** rather than roll back if the new (index, term) is not `>=` the replaced snapshot's. Tests: a fixture whose raft log ends in a real `LogNoop` above the last `LogCommand`, asserting the post-rebuild snapshot term `>=` the pre-rebuild snapshot term and index `>` the pre-rebuild LastIndex; and that the post-rebuild `CurrentTerm >=` the pre-rebuild `CurrentTerm`. (The existing `s6_s8_resnapshot_external_review_test.go:128` discards the term: `exists, snapshotIndex, _, err`.)

---

### S5-11 · MODERATE · `cmd/tether/serve.go:204` + `internal/broker/broker.go:637-645` — `--auth-callout-seeds-dir` is silently repurposed as the tunnel-cert source: undocumented, undesigned, untested, and a stray file is now a fatal boot
*(lanes: cli-gates-ux R1-2 (DOWNGRADE→MODERATE) + startup-probe-store R1-6 (CONFIRMED MODERATE) + test-adequacy R1-7 (CONFIRMED) — merged)*

`serve.go:204` sets `cfg.StableTunnelCertDir = authSeedsSource` **unconditionally**; `broker.go:637-645` calls `loadOptionalStableTunnelCert` at the top of `Run` in single mode and returns its error, aborting startup; a loaded pair flips `newTunnelServer` (`internal/broker/home.go:48-53`) from the ephemeral path to `NewServerWithCert`.

Three surviving defects:

1. **Undocumented overload.** The flag help (`serve.go:254-255`) still reads *"directory containing broker.nk + account.nk for auth_callout; empty = off in single mode…"* — no mention that it now selects the broker's TLS tunnel identity. `docs/broker-ops.md:338`/`:148` describe it purely as an auth_callout knob. `grep -rn 'StableTunnelCert|tunnel-cert|cert_fp' docs/reviews/s6-s8-*.md docs/deploy-tier-gotchas.md` → **zero hits**: this product change appears in none of the plan, the four external-review rounds, or the round-4 implementation record. It is undesigned and unreviewed.
2. **New fatal boot path.** `internal/broker/clusterwrite.go:217-233` treats the pair as absent only when **both** stats return `os.ErrNotExist` (`:222-224`); any other combination is fatal (`:225-227`), and the stat-succeeds/read-EACCES arm falls through to `loadStableTunnelCertFrom` whose `ReadFile` error is returned. A single-mode broker with a leftover `tunnel-cert.pem` (interrupted rotate, partial rsync, a cert staged for fingerprint distribution) — or with the pair present but mode 0000 — now exits with `broker: stable tunnel certificate pair is incomplete: cert=<nil> key=stat …: no such file or directory`, unclassified exit 70 (`cmd/tether/exitcode.go:32`), and crash-loops under `Restart=always`. It booted fine before this batch, on a subsystem the deployment does not use. The message names neither the flag that pulled the file in nor any remedy, and mislabels non-ENOENT errno (EACCES/ENOTDIR/EIO) as "incomplete". Single mode runs **no** §15 secrets preflight (`SecretsPreflightWithOptions` is inside `buildClusterRuntime`, `cutover.go:186`, cluster-mode only), so nothing else validates these files.
3. **Zero wiring coverage.** `grep -rn 'StableTunnelCertDir' --include=*_test.go .` → nothing. The only test (`s6_s8_external_review_test.go:38,45`) calls the loader on a `t.TempDir()`. **Mutation:** `if false && …` at `broker.go:637` disables the feature entirely and `internal/broker` passes (ok, 25.044s).

*Adjudicated `[self-verified]`:* the cli-gates lane's "silent fingerprint drift → data-plane outage" arm is **rejected**. Without this diff a single-mode broker serves a random ephemeral cert, so a pinning agent fails either way — the diff does not create the outage. And a fp guard is not implementable in single mode: `b.cl` is nil so there is no `cluster_nodes` row to validate against (`clusterwrite.go:178`), and `selfID==""` makes `homeForExpose` return nil (`home.go:97`), so a single-mode broker never publishes `CertPins` at all.

**Fix:** introduce an explicit `--stable-tunnel-cert-dir`, defaulting to off, so no existing single-mode deployment changes behavior on a binary upgrade. If the overload is kept: update the flag help and the `docs/broker-ops.md:338` row; make a partial/unreadable pair a loud WARN + ephemeral fallback in single mode (the point of `loadOptionalStableTunnelCert` being *optional*); distinguish "one of the pair is missing" from a non-ENOENT stat failure and name the flag + remedy in the message; log the loaded fp at startup; and record the change in `docs/reviews/s6-s8-*` — it currently has **no design record at all**. Tests: a `Run`-level test with (i) valid pair → `b.tunnelCert` matches on-disk, (ii) cluster mode → dir ignored, (iii) cert-only, (iv) cert+key mode 0000.

---

### S5-12 · MODERATE · `test/simcluster/drills/42-rejoin-returning.sh:145` — 42's own fixture violates the product's `force_single_active` write gate → SETUP-RED kills the drill before the #49 pin the ledger claims runs
*(lane: mandate-ssot R1-1, CONFIRMED MAJOR by its verifier; I rank it MODERATE because the batch is already Fail and no false-green results — but it is a **Mandate** violation and a false SSOT claim)*

`42:145-146` runs `assert_setup "I create real post-force-single transfer-audit Raft entries" … $SIM ctl -- push …` with **no `--ack-alerts`**, AFTER the offline force-single at `42:91-92`. brk1 therefore carries the persisted `force_single_active` marker (`internal/clusteroffline/offline.go:119-123` → `cluster_meta`), `internal/broker/cluster_health.go:81` answers `ForceSingleActive: true`, `internal/proto/alerts.go:156-162` sets it, and `cmd/tether/transfer.go:157-160` → `cmd/tether/d8_alerts.go:71-79` **BLOCKS** the push. **The product is correct; the harness is wrong.** Every peer drill that pushes after force-single carries `--ack-alerts` (`20:53`, `41:203`, `92:112,137`), and `92:99-102` hard-asserts this very gate — 42 is the outlier.

Because it is `assert_setup`, `lib/assert.sh:162-172` does `drill_end; exit`, so `42:147-203` never runs. Dead region includes **the only sim assertion the ledger claims pins the newly-fixed resnapshot defect**: `docs/deploy-tier-gotchas.md:341` states "42 额外硬断 resnapshot 后名册精确 {brk1}", i.e. `42:160`. Also dead: the audit-window refusal (`42:151`), `--accept-audit-loss` (`42:157`), arm F rejoin (`42:167-186`), and the J/K Tier-2 negatives (`42:189-203`). The reviewer's own record confirms the outcome: `s6-s8-external-review-round4-implementation.md:16-18,45` — "42 | SETUP-RED, 26 pass".

**Net: the unplanned resnapshot product surgery (#49) has ZERO deploy-tier coverage while the ledger tells a reader it is hard-asserted.**

**Fix:** move the audit-generating transfer to **before** the offline force-single, or generate the audit entries through a non-write-gated path. **Do NOT add `--ack-alerts` to the fixture** — that is using the emergency override to let the harness paper over a gate the product correctly applies (Mandate rule 1). Until 42 can reach `:160`, correct `docs/deploy-tier-gotchas.md:341` to state #49 has hermetic coverage only and **no reachable sim pin**, and update `README.md:307/319` to record 42's actual SETUP-RED disposition.

---

### S5-13 · MODERATE · `internal/broker/cluster_health.go:150` — `healthContactStale` reports `leader_contact_stale=true` during a routine election, contradicting the documented "brief election does NOT gate" wire contract
*(lane: cluster-ops-control-plane R1-1, verifier DOWNGRADE→MODERATE)*

The new terminal clause `return leaderID == "" || leaderID == selfID` (`:150`) discards TFence for the leaderless case. Old behavior (`internal/cluster/read.go:48-53`): a follower that heard a leader within `TFence=10s` (`read.go:18`) reported fresh. New: a follower is stale the instant `LeaderWithID()` returns `""` — which raft does on every state transition (`raft.go:2151-2153` `setState` → `setLeader("","")`; heartbeat timeout in `runFollower`; `timeoutNow` at `:2210-2212`) while `LastContact()` is still ~1s old. The staged test pins it: `{"leaderless candidate", false, false, false, "", "self", true}` (`s6_s8_external_review_test.go:23`).

This falsifies the contract it feeds. `internal/proto/alerts.go:139-141` (**not staged**) states verbatim: *"a confirmed leader anywhere, or a node that recently heard a leader (a brief election), does NOT gate"*, and `alerts_test.go:25-26` pins "brief election: a follower recently heard a leader => no gate" — still passing only because it hand-feeds `LeaderContactStale: false`, a value the responder can no longer produce in that state. Nothing tests the composition, so `make test` is blind.

Aggravating: `driveRetire` deliberately triggers an election on itself (`cluster_operation_controller.go:723-731` LeadershipTransfer), so the code path most likely probed while gated manufactures the false verdict. `cmd/tether/cluster_status_nats.go:69-71` → `AllStale=true` → `READ-ONLY` / exit 2 on a healthy 3-voter cluster.

**Why MODERATE, not MAJOR** *(verifier work I adopt)*: the gate is **advisory by contract** (`alerts.go:141-143`: "the authoritative protection is the broker rejecting a write it cannot quorum-serve"); the error is fail-**closed** in the conservative direction; and the window is narrower than claimed — raft's `requestVote` returns early at `raft.go:1650-1656` when a leader is known, **before** the `setState(Follower)` clear at `:1663-1667`, so in a natural election each follower keeps `leaderID` until its own randomized timeout fires. Sub-second to ~2s.

**Fix:** keep the intended half (`stateLeader && !writable ⇒ stale`, which closes the demoted-ex-leader hole) but do **not** invert the follower arm — for a follower, stay TFence-driven. If a leaderless-follower signal is wanted, carry it as a **new** additive field (`leader_unknown`) so `EvalDestructiveGate`'s documented predicate is unchanged. Whichever is chosen, `internal/proto/alerts.go:21`, `:137-144`, `alerts_test.go:5-8,25-26` and the staged `#42` ledger entry must change in the **same** commit (see S5-16), and a composition test (`healthContactStale → ClusterHealthResp → EvalDestructiveGate`) must pin the election behavior.

---

### S5-14 · MODERATE · `internal/broker/topology_reconcile.go:125-126,186-200` — the anti-simultaneous-bounce stagger anchors on each broker's **own local conf mtime**, so the safety property the docstring claims does not hold across nodes; and its only test certifies a property it does not exercise
*(lanes: cluster-ops-control-plane R1-3 (DOWNGRADE→MODERATE) + test-adequacy R1-6 (CONFIRMED MODERATE) — merged. **Contradicts concurrency-race R1-5, which was REFUTED — see the REFUTED section.** `[self-verified]`)*

I read the source. `:125` `mt := topoConfMtime(in.ConfPath)`; `:126` `confTime := time.Unix(0, mt)` — **this host's local nats.conf mtime**; `topologyRestartDue` (`:186-200`) returns due iff `!now.Before(confTime.Add(topoRestartBaseDelay + rank*topoRestartSpacing))`. The **rank** is globally unique, but the **anchor** is per-node. Each broker swaps its conf independently (5s tick, `:29`; its own applied index; a `nats-server -t` DryRun subprocess before `natsconf.Apply`). Two brokers with enough conf-swap skew collide on the same absolute wall-clock restart instant, and `hardRestartNatsServer` is `nats-server --signal stop` = **SIGKILL** (`internal/broker/cluster_grow_cutover.go:307-317`), not a drain. Losing 2 of 3 NATS voters simultaneously costs the R3 JetStream meta group its quorum → fleet-wide JS 503.

**Why MODERATE, not MAJOR** *(verifier work I adopt)*: `scripts/install.sh:722-723` renders `Restart=always` / `RestartSec=2`, so a bounce's NATS downtime is ~2-4s; an adjacent-rank overlap needs roughly 8-16s of conf-swap skew, whereas the cited mechanisms (5s tick + sub-second DryRun) give ~5s. The ">= 12s skew is entirely ordinary" claim is asserted, not derived.

**The test is the worse half.** `TestTopologyRestartDueUsesDistinctStableRosterSlots` (`s6_s8_external_review_test.go:54-73`) passes **one shared** `confTime := time.Unix(1_700_000_000, 0)` (`:56`) to every peer — it proves only that ranks differ under an **identical** anchor, which is strictly weaker than the property the docstring at `:184-185` names ("the no-simultaneous-bounce safety property is unit-testable"). This is a test that certifies a safety property it does not exercise. Separately, `grep` confirms `hardRestartNatsServer`, `waitNatsLoaded` and `reconcileTopologyOnce` have **zero** test references: the compound guard at `:134` (`mt != 0 && mt != lastRestartMtime && due && staleLoad`) that decides whether to SIGKILL a production NATS, and the loop-carried `lastRestartMtime` that is only burned at `:144` after `waitNatsLoaded` succeeds, are entirely unpinned. A persistently-failing `waitNatsLoaded` re-issues the restart every ~35s (5s tick + up to 30s block) with only a `Warn`.

**Fix:** anchor the stagger on a **cluster-shared epoch** — derive the slot from the replicated topology generation's commit timestamp (agreed by every broker) plus rank, and require `confTime <= slot`. Rank over the **raft configuration's** server set rather than the locally-filtered roster. Better still, make the restart positively mutually exclusive (a raft-held single-restart token released on `waitNatsLoaded` success) instead of relying on a timing coincidence. **The unit test must feed different `confTime`s per peer and assert no two peers are ever due at the same `now`.** Add an injected restart/probe seam and rows for: restart fires exactly once per distinct conf mtime; a failed `waitNatsLoaded` does not burn `lastRestartMtime` but also does not re-issue unboundedly; an explicit bound/backoff on consecutive failed restarts.

---

### S5-15 · MODERATE · `internal/broker/cluster_operation_controller.go:595-609,793-796` — the new pre-terminal gates use `recordOpError` (unbounded retry, no escalation) instead of `blockAfterAttempts`, widening gotcha #45
*(lane: cluster-ops-control-plane R1-4, CONFIRMED MODERATE)*

Three new hard gates sit between NATS_ROLLED_OUT and the terminal transition — driveRetire's `deriveAndConvergeSeedsFromRoster` (`:793-796`, before `transition(op, OpStateRetired, …)` at `:797`) and driveJoin's `deriveAndConvergeSeedsFromRoster` (`:595-597`) + `Propose(PlanClearGrowActive)` (`:605-609`, before SERVING at `:612`). All three use `recordOpError` (`:438-443`), which only rewrites `last_error` in place and **never** changes state or escalates. The file already has the correct primitive for a retried blocking side-effect — `blockAfterAttempts` (`:650-664`) caps at `opMaxAttempts=5` (`:298`) and routes to `OpStateBlocked`, the operator-actionable state `cluster ops confirm` targets — and the two irreversible raft config changes use it (`:525`, `:563`). The new steps, explicitly promoted to blocking terminal gates ("part of the terminal contract", `:790-792`), were not given the escalation the file reserves for exactly that.

A persistent failure pins the op at NATS_ROLLED_OUT forever — never RETIRED/SERVING, never BLOCKED. That is verbatim the staged ledger's #45. Blast radius is the whole membership plane: `assertNoActiveOp` (`:27-36`), `cluster_grow_trigger.go:82-88`, `cluster_upgrade_trigger.go:159-162`, and `driveInFlightOperations` never times an op out (`:325-349`). For a wedged retire there is no clean escape: the node is already RemoveServer'd (`:764-769`) and its roster row deleted (`:770-777`), so `cluster ops abort` would mark ABORTED a retire that physically completed.

*Honest discounts:* the stall state is not novel — the preceding `topoConvergedForOp` check (`:781-785`/`:582-586`) already pins ops the same way via `recordOpError` (that **is** #45) — and the realistic errors are transient (`driveInFlightOperations` only runs while `IsLeader()`, so leadership churn resolves).

**Fix:** route all three through `blockAfterAttempts(op, "converge client seeds after retire" / "release cluster-add grow lock", err)`. Alternatively, for retire, keep seed withdrawal **out** of the terminal gate and make it a leader-loop reconcile invariant (the roster row is already gone; the desired seed set is a pure function of committed state and needs no op to carry it). Test: a persistently-erroring converge/Propose reaches `OpStateBlocked` — not an infinite NATS_ROLLED_OUT loop — for both `driveJoin` and `driveRetire`.

---

### S5-16 · MODERATE · `docs/deploy-tier-gotchas.md` #42 + `internal/proto/alerts.go:21,137-144` — the staged ledger documents behavior the staged code in the same commit already changed, and the wire contract was never touched
*(lane: cluster-ops-control-plane R1-6, CONFIRMED MODERATE)*

The staged #42 entry documents **current** behavior as TFence-lagged — "窗口内（~0-11s）① `--remote` VERDICT = 'electing a leader (transient)'", "因 `EvalDestructiveGate.QuorumLost` 走同一 TFence LIVE-probe 谓词" — and lists the unfulfilled flip as "`--remote`/destructive-gate 在窗口内即给 quorum-lost verdict". The staged `healthContactStale` (`cluster_health.go:143-151`) **is that flip** for #42's own scenario (N=2, kill one voter): before step-down the survivor is `State()==Leader` with a failing VerifyLeader → `:147` → stale immediately; after step-down it is a follower with `leaderID` cleared → `:150` → stale immediately. Either way it no longer waits out TFence. **A staged doc asserts a gap the staged code in the same index already closed.**

Worse, `internal/proto/alerts.go` is **absent from `git diff --cached --stat`**: `:21` ("no leader contact within T_fence") and the `:137-141` docstring survive unchanged and are now false for a leaderless-but-fresh follower, and `alerts_test.go:5-8,25-26` keeps passing only because it hand-feeds the field. The batch changed an externally-observed protocol semantic without touching a single line of its contract documentation or its tests.

*(Checked and NOT a false-green: `test/simcluster/drills/92-js503-remote-alert.sh:60-66` polls up to 90s for `--remote` to self-correct to READ-ONLY, so it passes faster under the new code. Impact is contract/doc drift, not a broken gate.)*

**Fix:** decide the semantics first (S5-13), then in the **same** change: rewrite `alerts.go:21` and `:137-144` to state the predicate the responder now emits; update `alerts_test.go:5-8,25-26`; rewrite the staged #42 entry to record what the staged code actually closed vs. what remains open (if the window is now ~1s instead of ~10s, say so and re-derive the flip criterion); add the composition test.

---

### S5-17 · MODERATE · `internal/clusteroffline/offline.go:300` — `previewRecoveredRoster` runs the production fail-stop FSM against a throwaway copy; a transient error panics the recovery tool
*(lane: raft-recovery R1-5, CONFIRMED MODERATE)*

`:300` calls `cluster.RecoverSingleNode(root, previewDB, …)` against the `.recovery-preview-*` copy, which reaches `raft.RecoverCluster` → `fsm.Apply` (`internal/cluster/offline.go:422`). `fsm.Apply` is deliberately fail-stop: `internal/cluster/fsm.go:132-143` retries `applyMaxAttempts=3` then `panic("cluster: FATAL fail-stop: cannot durably apply committed entry %d after %d attempts")`. Its own justification ("on restart the (snapshot-free) log replay re-delivers the entry") holds for the live node and is meaningless for a disposable preview copy. `Resnapshot` has the identical exposure at `:218`.

Reachable, not theoretical: the preview does a full `BackupDBFile` (`:293`) plus a full raft-tree clone (`:296`) **into the same data dir**, so force-single now transiently needs roughly 3× the DB size free where it previously needed ~1× — with **no free-space preflight anywhere** (`grep -rn 'freeSpace|Statfs|statfs'` over `internal/clusteroffline` and `internal/cluster` → nothing). `test/simcluster/drills/21-smalldisk-tierb.sh` exists precisely because small-disk brokers are a real tier.

**Failure:** disk at 98% (a common reason for emergency recovery). `force-single` → the preview fills the disk → the SQLite commit returns "database or disk is full" → `fsm.Apply` retries 3× → **panic**. The operator sees a raw Go panic + stack trace from a tool whose entire purpose is guarded disk surgery, with no indication whether anything was mutated (nothing was — the preview at `:108` precedes `RecoverSingleNode` at `:115` — but the panic does not say so) and a leftover `.recovery-preview-*` consuming the last free space.

**Fix:** do not run the fail-stop production FSM in a preview — give `recoverClusterToSelf` a preview mode with a non-panicking apply policy, or recover the panic inside `previewRecoveredRoster` and convert it to a wrapped error stating that NOTHING has been mutated. Add a free-space preflight (~3× DB size) **before** the copy at `:293`, refusing rather than ENOSPC-panicking. Sweep stale `.recovery-preview-*` / `.raft-rebuild-*` under the flock on entry (see S5-21).

---

### S5-18 · MODERATE · `internal/agent/roster.go:326,335-338` — the new topology edge-trigger token is consumed and **dropped** by the single-flight gate, then rescheduled at the FULL 3-minute interval, defeating the fix in exactly the churn window it targets
*(lane: concurrency-race R1-2, CONFIRMED — verifier: "MODERATE is a floor, not a ceiling")*

`rosterRefreshLoop` drains the cap-1 token at `:326` (`case <-a.rosterRefreshNow:`), **then** hits `:335-338` `if a.reconnectInFlight.Load() || a.rebuilding.Load() { timer.Reset(jitterDur(iv)); continue }`. The edge is destroyed and the retry is scheduled at the full `iv` (3 min, `:24`) — not `a.rosterRefreshFailBackoff` (20s, `:341`).

The skip comment claims "a reconnect/rebuild's own register already refreshes the roster". True for **adoption**; false for the whole point of this machinery. `grep` confirms `rosterRequiresReconnect` has exactly **one** production caller — `roster.go:379`, inside `refreshRosterOnce`. The register/adopt path never evaluates it. A skipped event = roster updated, agent **not evacuated**. And this is worst exactly when it matters: during consecutive retires the link bounces, so `reconnectInFlight` (`agent.go:1510-1515`) is set precisely when the topology events land.

**Verifier's escalation, which I adopt:** it can be **permanent**, not 3-minute. `AdoptDecision` gates on `r.Generation >= prev.RosterGen` (`:149`) so a later refresh is accepted, but `rosterRequiresReconnect` computes `knownBefore` from `previous` (`:372`, `:396`) — which by then already equals the new roster. If the dropped edge corresponded to a roster in which the connected broker was **removed** (not merely DRAINING), the reconnect's own register advances the cache, and every later refresh sees `knownBefore=false && currentPresent=false && currentLeaving=false` → `:417` returns false forever: a **permanent island**.

**Fix:** do not swallow the edge — when the gate skips, re-push the token (non-blocking) into `a.rosterRefreshNow` and reset the timer to `rosterRefreshFailBackoff`, not `iv`. Better: move the evacuation decision out of `refreshRosterOnce` into the shared adopt path so the reconnect's own register also evaluates `rosterRequiresReconnect`, removing the dependence on the edge surviving at all. Test: set `reconnectInFlight=true`, publish a topology event, clear the flag, assert the roster-only pull happens within the fail-backoff; plus a test that a broker **removed** during the skip still triggers evacuation.

---

### S5-19 · MODERATE · `cmd/tether/cluster_add.go:154` — `retryClusterAddConnect` classifies control flow by string-sniffing prose, the exact practice `exitcode.go` documents as forbidden; its only test pins a synthetic string
*(lanes: cli-gates-ux R1-3 (CONFIRMED MODERATE) + test-adequacy R1-8 (DOWNGRADE→MINOR) — merged at MODERATE)*

`cluster_add.go:154`: `strings.Contains(err.Error(), "Authorization Violation")`. `cmd/tether/exitcode.go:47-49` states the package's own rule: *"The classifier never string-sniffs prose for a class — that would make a reworded message silently change a script's exit code."* The same literal is independently duplicated at `cmd/tether/error_hints.go:145` with no shared constant.

The coupling is second-order and accidental on both ends: (a) the inspected error is not the raw NATS error but the one `connectError` (`error_hints.go:144-153`) already rewrote into a `permErr`, whose prose still contains the substring only because of the `%w` at `:147`/`:153`; (b) nats.go's own typed sentinel is **lowercase** — `ErrAuthorization = errors.New("nats: authorization violation")` (`nats.go@v1.52.0:115`). The capitalized form survives only on the first-connect path (`nats.go:3074-3085` returns `&natsProtoErr{normalizeErr(proto)}`; `normalizeErr` at `:2997-3001` trims but does **not** lowercase), while the reconnect path (`:4183-4185`, `:4200-4203`) lowercases and routes through `checkAuthError`'s sentinel. The guard depends on an undocumented casing accident in a vendored dependency's non-exported path.

The only test (`cluster_add_connect_external_review_test.go:14-24`) injects `errors.New("nats: Authorization Violation")` straight into the attempt closure — it never touches `connectCtl`/`connectError`/`nats.Connect`, so it cannot detect either regression. The negative row (`"connection refused"`) is also an error `nats.Connect` never produces (`nats.go:2816-2823` converts it to `ErrNoServers`).

**Failure:** a natural cleanup of `error_hints.go:147` that drops the `%w` → `:154` never matches → the retry silently becomes dead code → gotcha #47's cutover window (rc=77 while the authoritative join op stays CATCHING_UP and the grow lock fences all later grows) returns with **zero test failure**.

**Fix:** classify on a typed sentinel. `errors.Is(err, nats.ErrAuthorization)` **does** match here — I confirmed `natsProtoErr` implements `Is` at `nats.go:3012-3014` as `strings.ToLower(nerr.Error()) == err.Error()`. Or define a tether-owned `errBrokerAuthRejected` that `connectError` attaches at the **source** (`error_hints.go:145-153`), exactly as `usageErr`/`unavailErr`/`permErr` do per `exitcode.go:47-49`, and hoist the literal into one shared constant used by both call sites. Add a test that drives the real path — `connectCtl` against an embedded nats-server configured to reject the CONNECT — and asserts `retryClusterAddConnect` actually retries.

---

### S5-20 · MODERATE · `test/simcluster/tests/verdict-contract-test.sh:141,154-205` — the hermetic suite never joins the real producer to the real parser
*(lane: verdict-contract-b1b2 R1-2, verifier DOWNGRADE→MINOR; I hold it at MODERATE because it is the direct cause of B2's residual and of multi-hour false confidence)*

Two disjoint halves. The `lib/assert.sh` half (`:51-149`) drives the real producer but validates it with hand-rolled checks: `assert_verdict` (`:36-46`) reads only `verdict=` and the process rc, and the one grammar check (`:141`) uses `verdict=[A-Z-]+` — **looser than the runner's** anchored enum alternation (`run-drills.sh:152`) — with no counter/verdict consistency at all. The `run-drills.sh` half (`:154-205`) drives the real parser but only ever feeds it `mkd`-generated `printf` lines — never a byte `drill_end` (`lib/assert.sh:198-199`) actually produced. The runner's semantic cross-checks (`run-drills.sh:157-163`) are never exercised against the code that emits the counters.

**Failure (reproduced by the lane):** inject a one-token producer regression at `lib/assert.sh:199` (swap `$_AS_FAIL` for `$_AS_NC`). `sh tests/verdict-contract-test.sh` → **ALL PASS**. The real runner over the same producer → `CONTRACT-ERROR [BLOCKER]`, exit 1 (`run-drills.sh:163` requires `af=0` for INCOMPLETE). Every drill that records a `not_covered()` becomes an unretryable blocker on the sim server, discoverable only after a multi-hour docker round trip, while the hermetic gate says the contract is fine. (Fail-closed, so no false green — hence not higher.)

**Fix:** add a runner-e2e case that generates a synthetic drill which **sources the real `lib/log.sh` + `lib/assert.sh`**, calls `drill_begin`/`assert_ok`/`assert_bug`/`not_covered`/`drill_end`, then runs the **real** `run-drills.sh` over it and asserts the classified verdict — once per landing verdict (GREEN / ASSERT-FAIL / SETUP-RED / PRODUCT-RED / INCOMPLETE). Tighten `:141` to the runner's exact enum alternation.

---

### S5-21 · MODERATE · `test/simcluster/run-drills.sh:51-52,83-84,228-229` — waivers are category-wide, not per-item
*(lane: verdict-contract-b1b2 R1-1, verifier DOWNGRADE→MODERATE. Full analysis in "Round-4 blocker status → B1" above; listed here so it is not lost as a finding.)*

**Fix:** as stated under B1 — per-drill waivers or an expected-verdict baseline the runner diffs, plus stale-waiver detection.

---

### S5-22 · MODERATE · Gotcha-ledger integrity — REDs fired against unregistered numbers, signature-free attributions, and missing pins
*(lane: mandate-ssot R1-2, R1-3, R1-4, R1-7, R1-8, R1-9, R1-10 — merged; all CONFIRMED, most DOWNGRADEd to MODERATE by their verifiers)*

`docs/deploy-tier-gotchas.md:14` mandates the template *"每条模板：现象 / 机理(file:line) / 怎么自动化或修 / 钉住它的 drill + 签名"*, and `docs/reviews/simcluster-coverage-roadmap.md:49-51` §0.2 mandates "登记 gotcha（#25+）→ 用 assert_bug 签名钉成 RED". Seven live violations:

| # | Site | Defect |
|---|---|---|
| a | `92-js503-remote-alert.sh:121` | fires `product_red "#41 …"` — **no `### #41` section exists**; #41 appears only as a RESERVED candidate note (`gotchas:346`). Signature-free (fires purely on a `poll_until 90 6` timeout at `:105`; nothing is captured or matched, contra `lib/assert.sh:141-144`). Comment at `:120` says "an honest INCOMPLETE gap, never GREEN" while the code emits PRODUCT-RED (rc=3), not `not_covered` (rc=4). |
| b | `92-js503-remote-alert.sh:66` | signature-free `#42` PRODUCT-RED: fires on any `_remote_readonly` falsity — including an rc≠2 auth/transport error, which the drill's own comment at `:96-98` says is expected while quorum is absent — yet its text asserts an **unobserved** claim ("the misleading transient window persists"). *(The lane's claim that #42 has no pin at all is **refuted** — `gotchas:278`'s "**或** TFence 后断 --remote 自我纠正为 READ-ONLY(exit2)=GREEN" disjunct **is** implemented at `92:60-61`.)* |
| c | `93-metrics-observability.sh:185` | `product_red` with **no gotcha token and no ledger entry** (nothing covers the exit taxonomy / `ROSTER_UNREACHABLE`) — no mechanism, no fix direction, no flip condition. Also never re-run post-fix on the current image. |
| d | `90-alerts-lifecycle.sh:186` | fires `#39` for a **startup-sample** non-raise, but `gotchas:281-284` defines #39 as the `disk.go:23` hard-coded 5-min interval + missing knob — whose flip ("add a `disk_check_interval` knob") would never fix a broken follower→leader alert forward. Signature-free: any cause of non-raise lands the same attribution. **Fix:** make it `_as_fail` (the raise IS a kept invariant once >80% fill at `:180` and a proven restart at `:182` are both established). |
| e | `40-drain-retire.sh:239` | `#45` signature accepts **eight** op_states plus "already in flight", while `gotchas:288` scopes #45 to `NATS_ROLLED_OUT` only. `41:213` uses the tighter `already in flight\|NATS_ROLLED_OUT\|still in flight`. A new regression wedging at `DRAIN_REQUESTED` gets filed under a triaged gotcha and never opened. Also `gotchas:290` specifies `assert_bug #45` while the drill uses free-text `product_red`. |
| f | `gotchas:297-312` (#47), `:313-332` (#48) | **no 钉 field at all**, unlike every other entry. Load-bearing for #48, which self-declares "RATIFIED / release blocker": the only oracle that could catch the island is `41-shrink-to-standalone.sh:192`, a plain `assert_ok` → ASSERT-FAIL, which `lib/assert.sh:10` defines as "a KEPT invariant BROKE" — **indistinguishable from a fresh regression in the roster code changed in this same batch** (see S5-07/S5-18). And 41's agent-evacuation arms are gated on `[ "$_b" = "$AGENT_RETIRE_TARGET" ]` (`41:142-153`), so the 2→1 retire — #48's exact scenario — has no evacuation oracle. |
| g | 91's stable ASSERT-FAIL | the post-force-single survivor-only seeds non-convergence (`91-client-converge.sh:140-141`) is **unregistered**: the ledger jumps #46 (a different arm — 3rd-voter omission) to #47. Two staged SSOTs also disagree on which arm fails: `simcluster-coverage-inventory.md:467` says "terminal-retire seeds 不收敛" (arm A3, `91:112-114`) while `s6-s8-external-review-round4-implementation.md:11-14` says the offline force-single arm (`91:140`). Exactly one is right. |

**Fix (uniform):** every `product_red` must (1) cite a number that has a `### #N` section carrying the full template, and (2) fire only on a **captured, discriminating signature** — otherwise `_as_fail`. Open #50 for (g) with a discriminating branch (survivor present ∧ dead endpoints still present, mirroring #46's discriminator) and reconcile the two SSOTs. Open a numbered entry for (c). Add the missing 钉 fields to #47/#48, and give #48 a discriminating branch at `41:192` (agt1 still directly connected to the RETIRED broker per connz ∧ exec through the survivor times out → `product_red "#48 …"`). Narrow (e) to #45's documented signature. Convert (d) to `_as_fail`. Resolve (a)/(b) to one disposition each with comment, code, README and inventory in agreement.

---

### S5-23 · MODERATE · `test/simcluster/drills/40-drain-retire.sh:209` — the op-started branch matches the bare substring `retir`, so any retire output (including a refusal) passes as "STARTED an op"
*(lane: mandate-ssot R1-6, CONFIRMED; still-open round-4 M2)*

`:209` is `elif printf '%s' "$RET_OUT" | grep -qiE 'watch: .*cluster ops show|retir|removed'; then` → `:211` `_as_pass "R-retire: cluster retire $T STARTED an op"`. `RET_OUT` (`:195`) is the full pty transcript of a command whose own prompt, NOTE text (`cluster_retire.go:127-129`) and every error string contain "retir". Only the #31 signature (checked first at `:204`) escapes; every other failure is swallowed as a passing spine, making the `else` at `:251` ("unrecognized outcome") nearly unreachable.

**Failure:** a regression makes `cluster retire brk2` fail immediately with e.g. "cannot retire brk2: node is already DRAINING" (contains "retir") → `:211` `_as_pass "STARTED an op"` → `:216`'s `poll_until 45 2 _retire_pre_remove` times out → `:228` `_as_fail "R-resume: retire skipped every observable pre-removal cursor"` — a misleading diagnosis attributing the failure to cursor observability rather than to a retire that never started.

**Fix:** anchor on the actual success token only — `watch: .*cluster ops show` (optionally plus a captured op id) — and drop the `retir|removed` alternation so every other outcome falls to the `_as_fail` unrecognized-outcome branch.

---

### S5-24 · MINOR · `internal/cluster/offline.go:356` — no RENAME_EXCHANGE capability probe and no fallback; a failure lands **after** the roster prune
*(lane: raft-recovery R1-4, verifier DOWNGRADE→MINOR)*

No probe, no fallback. If `renameat2` returns EINVAL/ENOSYS, the error lands at `internal/clusteroffline/offline.go:140` — **after** `RecoverSingleNode` (`:115`), the marker (`:121`) and the **hard** prune (`:128`) — i.e. exactly the S5-04 poisoned state, with an error string ("… atomically exchange rebuilt raft store: invalid argument") that names neither the resulting state nor a remedy. The doc at `:285-288` ("a failure before the exchange leaves the original untouched") is true of the raft store in isolation and false of the node.

*Reachability is low, which is why it is MINOR* `[self-verified against the verifier's work]`: EXDEV is impossible by construction (the staging dir is created **inside** `dataDir`, `:296`); and the lane's "simcluster containers land on overlayfs" premise is refuted by `test/simcluster/lib/docker.sh:42-44` (`--tmpfs` or a **named volume**) and `Dockerfile:63-64` ("STATELESS image: no VOLUME directive"). It bites only on an exotic broker data-dir FS (NFS/ecryptfs/kernel<3.15).

**Fix:** probe the capability **before any mutation** — at the top of `ForceSingle`, alongside the §8.4 (b)/(d) preconditions, attempt a throwaway RENAME_EXCHANGE between two temp dirs in `dataDir` and hard-refuse up front (the "all checks before any disk mutation" rule is already established here and this violates it). Or provide a portable fallback (rename live→`raft.old-<ts>`, staged→`raft`, fsync `dataDir`, remove `raft.old-*`, with a startup reconciler for a half-done swap).

---

### S5-25 · MINOR · `internal/cluster/offline.go:296,300` + `internal/clusteroffline/offline.go:286,290` — scratch trees are cleaned only by `defer os.RemoveAll`; nothing reaps them, and no free-space preflight exists
*(lanes: raft-recovery R1-6 (DOWNGRADE→MINOR) + startup-probe-store R1-8 (CONFIRMED MINOR) — merged)*

`.recovery-preview-*` (`clusteroffline/offline.go:286`) and `.raft-rebuild-*` (`cluster/offline.go:296`) are removed only by deferred `RemoveAll` (`:290` / `:300`), which SIGKILL/OOM/power-loss skips. `grep -rn 'raft-rebuild|recovery-preview'` across `.go`/`.sh`/`.md` matches **only** the two `MkdirTemp` lines — no reaper in broker startup, `cluster doctor`, or install.sh. After the exchange, `stageRoot` holds the **entire old raft store** (with the abandoned-peer configuration), so an abnormal exit between `:356` and the deferred cleanup leaves it in the data dir. Each abandoned attempt costs a full DB + full raft store, unbounded across retries, on exactly the disk-pressure hosts where force-single runs.

**Two claims from the raft-recovery lane are REJECTED and must not be carried forward** `[self-verified via the verifier's grep]`: (1) the "nkey seeds / account JWTs leak" framing is **false** — `internal/storage/migrations/*.sql` store only **public** keys (`0013_cluster_nodes_join_pop.sql:16` `node_ident_pub`, `0014_cluster_nodes_bus_nkey.sql:1-11` "each broker's NATS BUS nkey **PUBLIC** key"; `0015_cluster_operations.sql:30` "NO secrets"); seeds live in files (`keys/agent.nk`, `--secrets-dir`/`node-ident.nk`). (2) the "Ctrl-C at the typed-node_id prompt leaks the copy" scenario is **inverted** — `cmd/tether/cluster_offline.go:178` calls `confirmTypedNodeID` **before** `:183` calls `ForceSingle`, and the preview runs inside `ForceSingle` at `:108`, so nothing exists at the prompt.

**Fix:** sweep stale `.recovery-preview-*` / `.raft-rebuild-*` on entry under the flock already held (`clusteroffline/offline.go:74`), logging what was removed; install a SIGINT/SIGTERM handler around the offline recovery commands in `cmd/tether/cluster_offline.go`; add the free-space preflight from S5-17 and surface residue in `cluster doctor`.

---

### S5-26 · MINOR · `internal/cluster/offline.go:285-288,359-362` — the staged dir is never fsynced and both post-exchange fsync errors are silently discarded, while the doc claims "no crash window"
*(lane: startup-probe-store R1-5, verifier DOWNGRADE→MINOR)*

`raftboltdb.New`/`Close` (`:318`/`:322`) fsyncs the **file**; `FileSnapshotSink.Close` fsyncs only `stagedRaft/snapshots`; nothing fsyncs `stagedRaft` or `stageRoot`. The only post-exchange sync is on `dataDir` (`:359-362`), and **both** the `os.Open(dataDir)` and `dir.Sync()` errors are discarded (`if dir, err := os.Open(dataDir); err == nil { _ = dir.Sync() … }`) — a failed post-exchange parent fsync is invisible to the caller. The doc at `:285-288` ("There is no crash window with a missing or half-built live raft directory") overclaims.

*Downgraded because* the claimed residue requires a filesystem without ordered-journal semantics: on ext4/XFS the `fsync(dataDir)` after the rename necessarily commits the earlier `mkdir`/`create` transactions. No code path or realistic broker FS is shown where the interleaving occurs.

**Fix:** fsync `stagedRaft` and `stageRoot` after the bolt store and snapshot sink are closed, propagating errors; **propagate** the `:359-362` errors; correct the doc to state the actual guarantee. Test: `RebuildSingleNodeFromDB` returns an error when the parent-dir sync fails (injected syncer).

---

### S5-27 · MINOR · `internal/broker/cutover_test.go:139` + `internal/broker/broker.go:709-712`, `home.go:44-47` — the D9 byte-equivalence floor no longer covers the tunnelCert seam, and three comments now assert the opposite of the code
*(lanes: startup-probe-store R1-7 (DOWNGRADE→MINOR) + cli-gates-ux R1-6 (CONFIRMED MINOR) — merged)*

`TestD9ClusterModeOffByteEquivalence` (`cutover_test.go:123-159`) is documented as the regression **floor** replacing the six deleted `TestDxProductionWiresNoCluster` guards. Its assertion `if b.tunnelCert != nil { t.Error("D6 tunnelCert seam must be nil in single mode") }` (`:139-141`) observes only `New()`-time state (`:125`); this batch attaches the seam in `Run` (`broker.go:637-645`), which the test never calls. Verified: `go test ./internal/broker/ -run TestD9ClusterModeOffByteEquivalence -count=1` → ok with the staged change.

*Correction I adopt from the verifier:* the lane's "now vacuous and can never fail regardless of what the single-mode path does" is **overstated** — a regression setting `tunnelCert` inside `New` would still fail it. The guard is **incomplete for Run-attached seams**, not unreachable.

Three now-false comments:
- `broker.go:709-712` — "newTunnelServer returns the ephemeral-cert NewServer in production (`b.tunnelCert == nil`) … Production is byte-equivalent to the prior tunnel.NewServer call." False: `serve.go:204` → `broker.go:643` → `home.go:49-50` selects `NewServerWithCert` for a production single-mode broker.
- `home.go:44-47` — "a STABLE-cert tunnel server when **the harness** attached one, else the production ephemeral self-signed path". False: no harness is involved.
- `cluster_add_drive.go:296-300` — "the already-VOTER case is short-circuited earlier in driveAdd", the load-bearing justification for minting a fresh op on a terminal row. The staged B1 rewrite (`:72-84`) narrowed the shortcut to `liveOp == ""` and deliberately falls through for a VOTER with a live op.

*(The lane's claim that `home.go:24-26` is also falsified is **rejected** — "in SINGLE mode AttachClusterSeam is never called" remains true (`broker.go:637-645` assigns `b.tunnelCert` directly), and `selfID==""` still keeps every D6 path inert (`home.go:97`).)*

**Fix:** either move the seam decision into `New` (where the floor observes it) and assert the intended new behavior explicitly, or extend the floor to a Run-level assertion. Correct all three comments. Add an end-to-end test of the `serve.go:204 → broker.go:643 → home.go:50` chain (there is none — see S5-11).

---

### S5-28 · MINOR · `cmd/tether/cluster_add_drive.go:72-84,192` + `:507,511`, `:202` — the B1 fall-through burns `--timeout` and leaves the grow lock held, while the printed remedy never names the real unlock
*(lane: cli-gates-ux R1-1, verifier DOWNGRADE→MINOR)*

B1 was narrowed to `VOTER && liveOp == ""` (`:72-84`) and the `if joinerIsVoter(...) { return nil }` guard was removed from `waitJoinServing`'s loop, so a VOTER + stalled-op re-run now falls through to acquire-lock, then waits to `--timeout` (default 10m) → `haltAdd` → error → `releaseGrowLock` at `:192` is **never reached** (it sits only on the success path). Meanwhile `releaseGrowLock`'s own strings (`:507`, `:511`) advertise exactly one remedy — "Re-run `tether cluster add` with --account-seed to clear it" — and `haltAdd` (`:202`) repeats "fix and re-run".

**The lane's "permanently fenced with no CLI escape" claim is REFUTED** `[self-verified via the verifier]`: `tether cluster ops abort <op-id>` (`cmd/tether/cluster_ops.go:56-76`, Short: "frees the active slot") → `AbortOp` (`cluster_operation_controller.go:268-280`) transitions to `OpStateAborted` with `terminal=true`; a re-run of `cluster add` then takes the B1 shortcut (`cluster_grow_trigger.go:125-134` returns a terminal-only match; `resolveJoinOp` returns `""` on `resp.Terminal`, `:301-303`) → `releaseGrowLock` clears the marker. The comment at `:296-300` explicitly names `cluster ops abort` as producing that ABORTED row, so the author designed for this. The lane also asserted an exhaustive grep yet omitted the **second** `PlanClearGrowActive` clear added by this very diff (`cluster_operation_controller.go:605-610`).

**Residual (real):** a discoverability/message gap — the printed remedy sends the operator into a 10-minute no-op before they learn `cluster ops abort` exists.

**Fix:** name `cluster ops abort <op-id>` in `haltAdd`'s catch-up message and in `releaseGrowLock:507/511`. Optionally, on a timed-out VOTER resume, call `releaseGrowLock` before `haltAdd` (the joiner is already a VOTER; holding the fence buys nothing). Add a table test over the B1 decision (VOTER × {no op, live op, terminal op} → {shortcut+release, resume, fresh prepare}).

---

### S5-29 · MINOR · `internal/agent/roster.go:440-454` — `requestRosterReconnect` cancels whatever session is CURRENT, not the session that owns its conn
*(lane: concurrency-race R1-1, verifier DOWNGRADE→MINOR)*

`requestRosterReconnect` reads the **process-global** `a.sessCancel` (`:448-452`) with no check that `nc` is still the live connection, and `go a.rosterRefreshLoop(runCtx, nc)` (`agent.go:773`) is joined by no WaitGroup before `session()` returns. Contrast `fireRedial` (`:489-500`), which resolves the conn via `a.ncBox.Load()`; and the redial watchdog is additionally fenced by `stopRedialWatchdog()` on every successful connect (`agent.go:672`).

*Reachability is why it is MINOR:* the stale goroutine must have **completed** its RPC (proving the conn was live) and then be preempted for >20s (`redialAfter`, `roster.go:31`) while the link disconnects and stays stuck — implausible. The protection is emergent timing, not design, and silently breaks if `redialAfter` shrinks or a third rebuild trigger is added.

**Fix:** capture the session's own cancel (or ctx) when the loop starts and pass it in; at minimum return early on `ctx.Err() != nil` and require `a.ncBox.Load() == nc` before the CAS, mirroring `fireRedial`. Consider joining `rosterRefreshLoop` per-session.

---

### S5-30 · MINOR · residual hygiene (batched — fix or explicitly waive)

| id | file:line | what | fix |
|---|---|---|---|
| a | `internal/agent/roster.go:444` + `agent.go:743,779` | `requestRosterReconnect` sets `rebuildRequested` from a **healthy** connection, so a racing admin-evict costs one extra CONNECT that is auth-rejected before the terminal exit, instead of the clean P9 self-shutdown. *(concurrency R1-3, DOWNGRADE→MINOR: the asserted pre-existing invariant is false — `stopRedialWatchdog` cannot cancel an already-started AfterFunc — and the outcome is terminal, not a loop.)* | add an `evicted atomic.Bool` set at `agent.go:743` and check it **first** at `:779`. |
| b | `internal/agent/roster_runtime_test.go:151-158` | `TestTopologyEventTriggersSignedRosterRefresh` publishes into a subscription-establishment race: the gate (`:151`) is satisfied inside `a.register`, but the sys.events SUB is created later (`agent.go:718`) and never flushed; the fallback is disabled (`RosterRefreshInterval: time.Hour`). A dropped event → 3s `waitFor` t.Fatal. *(concurrency R1-6, CONFIRMED)* | publish repeatedly on a short ticker until the assertion holds (an event is by design only a hint), or gate on server-side interest before publishing. **Do not just raise the deadline.** |
| c | `internal/agent/roster_runtime_test.go:153` | the test publishes `nats_topology_applied` — an event type **production never emits** (`topology_reconcile.go:155` = `"nats_topology_"+out.Action`; the Action set is noop/reloaded/swapped_reload_pending/rejected/unresolvable/unknown_directive/awaiting_clustered_cutover). It passes only via the agent's prefix match (`agent.go:727`). *(test-adequacy R1-5, DOWNGRADE→MINOR)* | drive a table over the **real** emitted set referencing `natsreconcile` constants; add a negative row asserting an unrelated sys.event does not trigger a refresh. |
| d | `internal/agent/agent.go:388` | `rosterRefreshFailBackoff // test seam; immutable after New/Run begins` — invariant lives only in a comment; the loop reads it only on a **failed** tick (`roster.go:341`), so `-race` would miss a future violating test on most runs. No live race today. *(concurrency R1-7 + test-adequacy R1-10, both CONFIRMED MINOR)* | move it into `Config` (matching `RosterRefreshInterval`/`RegisterTimeout`, `agent.go:485-494`) so it is unwritable post-construction. |
| e | `internal/agent/s6_s8_external_review_test.go:11` | `a := &Agent{}` bypasses `New`, leaving `rosterRefreshNow` and `cfg.Logger` nil; survives only because `buildConnOptions` takes the `Identity == nil` branch and never invokes the handlers that close over the logger. *(concurrency R1-8, CONFIRMED MINOR — note the lane's claim that the test misses the Identity branch is **wrong**: `IgnoreAuthErrorAbort()` is in the unconditional prefix at `agent.go:1488`.)* | build through `New(Config{…})`; also assert `MaxReconnects(-1)`/`DontRandomize` survive. |
| f | `internal/clusteroffline/s6_s8_resnapshot_external_review_test.go:66` | asserts only `strings.Contains(err.Error(), "SINGLE-VOTER")`, which **both** gates emit (`offline.go:213` and the new `:227`) — cannot attribute the refusal to the new fence. Non-vacuous today only because `seedDB` makes peer-2 self-only. *(test-adequacy R1-9, CONFIRMED)* | assert the fence's distinctive text (`"recovery would revive"`) and the node id `peer-tail`. |
| g | `internal/broker/topology_reconcile.go:137,165` | `waitNatsLoaded` blocks the single-goroutine reconcile loop up to an **unnamed 30s literal** (inconsistent with `topoReconcileInterval`/`topoReloadTimeout`/`topoProbeTimeout` at `:29-37`), so `topoSelf.Store` (`:149`) is not reached for the window and no `restart_pending` reason exists. *(concurrency R1-9, CONFIRMED MINOR; cluster-ops R1-7 REFUTED — see below.)* | store `topoSelf` **before** the restart/wait block (or defer it) with a `restart_pending` reason; promote 30s to a named const. |
| h | `internal/clusteroffline/offline.go:326-352` | `mergePeers` collapses each node to one value per field with recovered-view precedence, so the liveness fence probes a single endpoint even where the two views disagree — the very divergence the preview exists to detect. No reachable divergence today (`restore.go:306-310`, the only direct-SQL address write, targets `selfID`, which `readRoster` excludes). *(raft-recovery R1-8, CONFIRMED MINOR)* | accumulate a **set** of `(kind, addr)` per node across all groups and have `probePeer` (`:411-420`, already a loop) iterate the union: ALIVE if **any** endpoint connects. |
| i | `cmd/tether/serve.go:269` | `--upgrade-url-allow` help says "empty = upgrades disabled"; the code directly above (`:160-168`) ships the built-in GitHub release prefix when unset, and `docs/broker-ops.md:135-137,338` documents the code, not the help — on a flag gating **remote binary execution**. Pre-existing. *(cli R1-5, CONFIRMED MINOR)* | fix the help to match; note `--upgrade-url-allow=''` would parse to `[]string{""}` (an empty prefix matching **every** URL) — the documented disable is "give a single non-existent prefix" (`broker-ops.md:137`). Add a test pinning the help text. |
| j | `cmd/tether/cluster_add_drive.go:313-315,360` | `waitJoinServing`'s poll body has no `else`: transport errors, nil replies and every non-OK code are dropped, and the deadline message names no cause. Asymmetric with `resolveJoinOp`'s deliberate fail-closed contract (`:288-294`, the "A4 fix"). *(cli R1-4, DOWNGRADE→MINOR: both concrete scenarios refuted — `clusterstatus.go:620-623` dispatches `OpClusterOps` **before** the leader gate ("any node serves it"), so a demoted leader keeps answering; and no code path deletes a `cluster_operations` row, while `cluster ops abort` yields a terminal row already handled at `:319-320`.)* | carry the last non-OK code/error into the deadline message the way `lastBlockedErr` is carried at `:353-355`, so no timeout is ever cause-free. |
| k | `internal/broker/cluster_operation_controller.go:786-789` | the drain-marker clear is still `_ =` fire-and-forget immediately above the seed step this batch hardened using the opposite rationale (`:790-792`). A stranded `draining:<node>` row is unrecoverable (`AbortDrain` calls `setPhase` on a roster row that no longer exists, `clusterdrain.go:365-373`). *(cluster-ops R1-5, DOWNGRADE→MINOR: the stated scenario is refuted — `transition` at `:797` is itself a `Propose`, so a leadership loss failing `:787` also fails `:797` and the tick re-runs `:787`; the residual trigger is a transient non-leadership Apply error at `:787` alone.)* | check the error and route it through `blockAfterAttempts`; both must succeed before `:797`. Test both directions. |
| l | `test/simcluster/run-drills.sh:105-107,141` | duplicate drill names in argv are neither rejected nor deduped: N copies share one log file and one throwaway docker instance (`simcluster` `_inst="drill-$_name"`), and the first to finish `nuke`s the shared instance out from under the others. Operator-typo-only. *(verdict R1-5, DOWNGRADE→MINOR)* | reject duplicates with an explaining message; add a runner-e2e case asserting `run-drills.sh … g g` exits 2. |
| m | `test/simcluster/run-drills.sh:148,151` | neither grep passes `-a`, so one NUL byte in a drill log turns a byte-perfect verdict into a permanent, **unretryable** CONTRACT-ERROR (excluded from `is_flake`) whose diagnostic misdirects. Fail-closed; no NUL source demonstrated. *(verdict R1-6, DOWNGRADE→MINOR)* | pass `-a` on both — the anchored regex at `:152` supplies all the strictness. |
| n | `test/simcluster/tests/lint-drills.sh:24,49-70` | `BATCH` is a frozen 9-name literal; the hard loop iterates only it and the exit keys solely on `HARD`, so a **new** drill with all banned false-green patterns lints clean. Documented as scoped (`:13-17`) and partly compensated at run time (`run-drills.sh:165` catches `drill_end; exit N`; `:159-163` catches counter pokes), so what escapes is bans #2 (bare NOT-COVERED) and #3 (`; true"` masking). *(verdict R1-4, DOWNGRADE→MINOR)* | invert the default: scan every `drills/*.sh` hard, keep a dated `LEGACY` allowlist demoted to advisory, and make an unclassified file itself a violation. |
| o | `test/simcluster/tests/lint-drills.sh:47,69` | `LEGACY=0` is always set and non-empty, so `${LEGACY:+; legacy advisory findings tracked}` appends **unconditionally** — including on default runs where the legacy loop never executed. *(verdict R1-7, CONFIRMED)* | gate on `[ "$ALL" = 1 ] && [ "$LEGACY" -gt 0 ]` and print the count. |
| p | `test/simcluster/README.md:305-306` | mis-numbers the retire NATS_ROLLED_OUT stall as `#38`, which is already taken (`s6-s8-plan.md:345` = 41's reconciler-recluster candidate). The stall is **#45** (`gotchas:286`, `plan:350`), and the drills agree (`40:240`, `41:222`). Bounded by README's own round-3-snapshot disclaimer (`:296-300`). *(mandate R1-5, DOWNGRADE→MINOR)* | change both to `#45`, and say `product_red` (what the drills emit), not `assert_bug`. |
| q | `test/simcluster/README.md:317-323` + `docs/reviews/simcluster-coverage-inventory.md:464` | record only the **pre-fix** run (22/43/92 GREEN; 40/90/93 SETUP-RED; 41/42/91 ASSERT-FAIL) and claim "post-fix remote reruns are still pending", contradicting `s6-s8-external-review-round4-implementation.md:38-50` (40 GREEN/37, 41 GREEN/30, 90 GREEN/49, **42 SETUP-RED/26**, 91 ASSERT-FAIL/34; only 93 never re-run). "one invalid audit oracle in 42 were corrected" also understates that the correction is what makes 42 un-runnable (S5-12). *(mandate R1-11, CONFIRMED MINOR)* | replace with the post-fix evidence table; state 42 lands SETUP-RED at its own fixture step and that 93 has never been run on the current image. |

---

## UNCERTAIN

None. Every finding either survived verification with a source-cited mechanism or was refuted below.

---

## REFUTED (do not act — do not re-litigate)

| lane / id | claim | refuting citation |
|---|---|---|
| concurrency-race R1-5 | `topologyRestartDue`'s stagger collides because `filterGhostPeers`'s read-error fallback (`topology_reconcile.go:266`) or empty-`ServerName` peers make two brokers compute rank 0 and SIGKILL together. | **Neither mechanism can reach the restart.** The restart is gated on `out.Action == ActionSwappedReloadPending && out.AppliedGen == desired` (`topology_reconcile.go:120`). (a) `filterSelfOnly` → `Peers=[self]`; with a live clustered conf `ReconcileOnce` sets `standalone=false` (`reconcile.go:116`), so `BuildMergedConf` renders a lone-self clustered conf, which `natscluster.Render` refuses → `ActionRejected` (`reconcile.go:149`) — pinned by `internal/natsreconcile/cluster_phase_fluidity_external_test.go`. (b) **Any** peer with an empty `ServerName`/`NkeyPub`/`RouteURL` → `ActionUnresolvable` (`reconcile.go:78-85`, C3-m8) before any render/swap; an empty `SelfServerName` or absent self → `ActionUnresolvable` (`:90-92`). Neither reaches the gate. **The per-node conf-mtime anchor (S5-14) is a different, surviving mechanism.** `[self-verified: I read `topology_reconcile.go:100-210` — `confTime := time.Unix(0, mt)` from the local `topoConfMtime(in.ConfPath)` is confirmed.]` |
| cluster-ops R1-7 | `waitNatsLoaded` freezing the loop makes the leader read a 30s-stale `topoSelf` and false-green a join/retire; and "the retry the comment at `:138-140` promises never fires". | **Self-defeating and a misread.** (a) `topoSelf` is served over NATS (`cluster_health.go:93-100`); while `waitNatsLoaded` blocks, that broker's NATS is **dead**, so the leader gets no reply at all — and `topoConvergedForOp` is documented fail-closed ("an unreachable/UNKNOWN voter counts as NOT converged (never false-greens SERVING/RETIRED)", `cluster_operation_controller.go:827-830`). (b) The comment promises retry for "a **lost/no-op signal**" — which leaves NATS running on the old conf, so the probe succeeds, `staleLoad` is true (`:127-128`), and the guard passes on the next tick. The counter-case (NATS down → `probeErr != nil` → no retry) is behaviour the comment never promises and is correct anyway. **The hygiene residue survives as S5-30(g).** |
| raft-recovery R1-2 / R1-3 (scenario only) | "Operator runs `cluster grow`; the returning term-50 zombie is back in the config, wins the vote, and overwrites the survivor — the force-single is UNDONE and every write is lost." | Blocked three ways: raft rejects vote requests from **non-voters** (`raft.go` `requestVote` `!hasVote`) and `cluster add` stages a joiner as a NONVOTER; promotion is catch-up-gated against a **persisted** barrier (`cluster_operation_controller.go:542-562`) and a term-50 zombie can never catch up (`raft.go:1455-1457` makes it ignore term-1 AppendEntries); and `broker.go:949-968` hard-refuses to start an un-wiped ejected node, with the documented flow wiping it first (`cluster_offline.go:383-386`, `clusteroffline/offline.go:686`). **The mechanism survives as S5-10 at MODERATE.** |
| raft-recovery R1-6 (secrets half) | `.recovery-preview-*/tether.db` leaks "every nkey seed and account JWT in the cluster"; Ctrl-C at the confirm prompt leaves it forever. | `internal/storage/migrations/*.sql` store only **public** keys (`0013_…:16 node_ident_pub`, `0014_…:1-11` "PUBLIC key", `0015_…:30` "NO secrets"); seeds live in files (`keys/agent.nk`, `--secrets-dir`/`node-ident.nk`). And `cmd/tether/cluster_offline.go:178` calls `confirmTypedNodeID` **before** `:183` calls `ForceSingle`, inside which the preview runs (`:108`) — nothing exists at the prompt. **The disk-hygiene half survives as S5-25.** |
| startup-probe R1-5 (scenario) | Power loss ~1s after force-single leaves `raft/` present but `raft.db`'s dirent uncommitted, so the broker tells the operator to `cluster init --from-existing`. | Requires a filesystem without ordered-journal semantics; on ext4/XFS the post-rename `fsync(dataDir)` forces the earlier transactions. No such FS or code path is shown. **The discarded-fsync-errors + doc-overclaim residue survives as S5-26.** |
| cli-gates R1-1 (headline) | A VOTER + stalled-op re-run leaves the cluster "permanently fenced against all membership operations with **no CLI escape**". | `tether cluster ops abort <op-id>` (`cmd/tether/cluster_ops.go:56-76`) → `AbortOp` (`cluster_operation_controller.go:268-280`) yields a terminal ABORTED row; the next `cluster add` takes the B1 shortcut and releases the lock. The comment at `cluster_add_drive.go:296-300` names this path explicitly. The lane's "exhaustive grep" also missed the second `PlanClearGrowActive` added by this diff (`:605-610`). **The message/discoverability residue survives as S5-28.** |
| cli-gates R1-2 (drift half) | Single-mode stable-cert loading causes a "silent fingerprint drift → data-plane outage", and the fail-closed fp guard is "exactly where it is needed and exactly where it is absent". | No baseline: without this diff a single-mode broker serves a **random ephemeral** cert (`home.go:52`), so a pinning agent fails either way — the diff strictly improves the common case. And the guard is not implementable: `b.cl` is nil (no `cluster_nodes` row to check, cf. `clusterwrite.go:178`) and `selfID==""` makes `homeForExpose` return nil (`home.go:97`), so a single-mode broker never publishes `CertPins`. **The undocumented-overload + fatal-boot halves survive as S5-11.** |
| verdict-contract R1-3 | `FLAKE_SIG`'s `is not running` is poisoned into every drill log by the drills' own deliberate `node_kill`s, so any later SETUP-RED is laundered into `ALL GREEN`. | **No cited drill dexecs a dead container.** `42:72`→`:101`, `22:100/:190`, `90:120`→`:139`, `92:48/:82` are followed only by `tcp_refused` (which `lib/docker.sh:89-91` runs in a **separate throwaway** container), `$SIM status --json`, and execs against **live** nodes; the one post-start `$SIM exec brk2` (`42:113`) is `>/dev/null 2>&1`. The status paths that do dexec are guarded (`leader_node`'s `node_running || continue`; `cmd_status` at `simcluster:485-497`) with `2>/dev/null || true`. The probe only proved the runner retries a synthetic drill that **prints the string itself**. **The retried-GREEN-prints-ALL-GREEN residual survives under B2 above.** |
| cluster-ops R1-5 (scenario) | The `_ =` drain-clear fails with ErrLeadershipLost while "the flap resolves before `:793`", so `:797` commits RETIRED with a stranded `draining:` row. | `transition` at `:797` is itself a `Propose` (`:430-432`), so a leadership loss failing `:787` also fails `:797`; the tick re-enters the `OpStateNatsRolledOut` case and re-runs `:787`. A raft leadership regain needs a full election (~1s), not the microseconds between `:787` and `:793`. **The consistency residue survives as S5-30(k).** |
| cli-gates R1-4 (scenarios) | `waitJoinServing` burns 10 minutes silently because the pinned leader stops answering after an election, or because `cluster recovery node remove` deletes the op row. | `internal/broker/clusterstatus.go:620-623` dispatches `OpClusterOps` **before** the leader gate at `:649` — "the ops view is a read-only derive off the replicated roster — any node serves it" — so a demoted leader answers normally. And no code path deletes a `cluster_operations` row (`grep` for `DELETE FROM cluster_operations`/`PlanDeleteOp` → nothing); `cluster ops abort` yields a terminal row already handled at `:319-320`. **The cause-free-timeout residue survives as S5-30(j).** |

---

## What the main process must decide

1. **S5-01 through S5-05 are blocking.** None is a judgment call; all five are source-reproduced.
2. **The five mutation-proven coverage holes (S5-06..S5-09, S5-11) must be closed before landing**, not after. They are the reason this surgery reached round 5 with a Fail: nothing in `make test` can currently tell whether any of it works.
3. **S5-12 is a Mandate item, not a test item.** Do not add `--ack-alerts` to 42's fixture. Either fix the ordering so the drill reaches its own spine, or correct `docs/deploy-tier-gotchas.md:341` to stop claiming a sim pin for #49 that cannot execute — but do not let the ledger keep asserting coverage that does not exist.
4. **B1's per-item waiver and the retried-GREEN headline are the only two runner items still owed.** Everything else round 4 raised on the runner is closed and I verified it from source.

---

# 主进程裁决（Stage-C step 5）— 逐条采纳 + 一条专家未发现的决定性活体发现

> CLAUDE.md §3 step 5：只有主进程能改实现。本节记录裁决与修复计划。
> **专家 8 lane 共 66 条原始 finding，经无条件对抗 verifier 后存活 25 条（S5-01…S5-25）。主进程全部采纳，
> 无驳回**——它们 source-cited 且多条我已独立复现。此外我在活体复跑中发现了一条专家未发现、且**推翻 round-4
> 外审核心结论**的缺陷（下 §M1）。

## §M1（主进程独有，活体证实）— 91 的 ASSERT-FAIL **不是产品缺陷，而是 harness 用 `| grep -q` 把被测操作腰斩**

round-4 implementation 判定：「91 稳定 ASSERT-FAIL：离线 force-single 后发布 seeds 在 90s 内没有收敛为
survivor-only……**属于真实产品问题**」。**该结论错误。** 我用 `SIM_KEEP=1` 保留失败实例直接取证：

1. **seeds 其实已经收敛**：survivor DB 里 `cluster_meta.seed_endpoints = tls://brk1:443`（两个死 peer 的
   endpoint **已被正确丢弃**）、`cluster_nodes` 只剩 `brk1|VOTER|brk1`、`seed_generation` 已 bump。产品的
   `convergeSeedsDropHosts` 工作正常。
2. **真正失败的是 `seeds show` 连不上**：`error: admin socket /var/run/tether/admin.sock: … no such file` ——
   **broker 根本没在运行**。
3. **broker 在无限 crash-loop**：`exit 70/SOFTWARE`、`NRestarts=72` 且继续攀升。手动跑出真实报错：
   `broker: cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node … The nats.conf almost
   certainly still has a cluster{} block`。实测 survivor 的 `/etc/tether/nats.d/nats.conf` **第 10 行仍有
   `cluster {`** —— **offline force-single 的 nats.conf 去集群化（#20 G2 修复）没有执行**。

**根因——三次受控实验（全部真机 weilandserver）**：

| 实验 | 形态 | 结果 |
|---|---|---|
| fsrepro（N=2，1 死 peer，**无管道**） | `… force-single …` | 去集群化**成功**（`nats.conf de-clustered to standalone`，post cluster-block=0） |
| fsrepro3（N=3，2 死 peer，**无管道**） | `… force-single … --confirm-peers-dead brk2 --confirm-peers-dead brk3` | 去集群化**成功**（`abandoned=2`，post=0） |
| **fsrepro4（N=3，2 死 peer，带 drill 91 的原样管道）** | `… force-single … \| grep -qiE 'de-clustered to standalone\|single-voter\|force.single'` | **grep rc=0 → drill 断言 PASS**，但 **post cluster-block=1（去集群化被腰斩）** |

**唯一差别就是那个管道。** 机制：CLI 的输出顺序是
① `WARN clusteroffline: force-single complete; node is now a **single-voter** cluster`（`ForceSingle` **内部
logger**，`internal/clusteroffline/offline.go` 尾）→ ② `⚠ CLUSTERED-JS → STANDALONE-JS`（`warnClusteredJSShrink`）
→ ③ `force-single complete: … nats.conf de-clustered to standalone`（`cmd/tether/cluster_offline.go:207`）。
`grep -q` **命中 ① 就立刻退出** → 关闭管道 → **SIGPIPE 杀死 tether 进程** → `cmd/tether/cluster_offline.go:195`
的 `deClusterStandaloneConf` **永远来不及执行** → nats.conf 保持 clustered → broker exit 70 永久 crash-loop →
`seeds show` 连不上 → C 臂 90s 轮询失败 → ASSERT-FAIL。

**判定**：这是 **harness 缺陷制造的假产品故障**——正是 Mandate ④「判定反转」的镜像：drill 自己的 oracle 把
它要测量的操作腰斩了。**修 drill（先 capture 再 grep，绝不把多步命令管进 `grep -q`），并全库扫同类模式。**

**但由此暴露了一条真实的产品脆弱性（登记为 #50，与 S5-04 同族）**：force-single 是**非原子四步**，且
① 的「force-single complete」日志在 nats.conf 去集群化**之前**就打印（**过早宣告完成**）。若进程在 ①与③之间
被任何原因中断（SIGPIPE / SIGTERM / OOM / 断电），节点进入**不可恢复砖态**：roster 已 prune + raft 已重建，但
nats.conf 仍 clustered → broker exit 70 crash-loop；**且无法重跑 force-single**（它强制要求
`--confirm-peers-dead` 列出每个 peer，而 peer 已被 prune 光——实测报
`force-single requires --confirm-peers-dead listing EVERY other node_id`）；**唯一补救 `reconcile nats
--to-standalone` 又是 socket-gated、需要运行中的 broker** —— 而 broker 正因此 crash-loop。**死锁**，恰是
`cluster_offline.go:189-193` 注释自己警告过的那个。这与 S5-04（无中断标记）是同一根因的两个面。

## 裁决与修复计划

- **S5-01…S5-05（MUST-fix blocking，全采纳）**：darwin 构建断裂（我已独立复现：`GOOS=darwin go build
  ./cmd/tether` 失败，`HEAD` 里 `unix.` 出现 0 次 = 本批新回归，且 `build/goreleaser.yaml` 发 darwin 产物 →
  发布流水线整体产不出物）· raft/ 属主移植（root 跑 runbook → 最后一台 broker 永久 crash-loop）· 目录交换
  绕过 bolt 锁互锁（已 ack 写静默丢失）· force-single 非原子无中断标记（= §M1 的产品面）· driveJoin 过早
  释放 grow-lock。
- **S5-06…S5-09、S5-11（mutation 证实的覆盖洞，全采纳）**：落地前补真行为测试，不得后补。
- **S5-10（term/index 回退）**：采纳。我独立观察到同一事实——重建快照把 term **硬编码为 1**
  （`snaps.Create(..., snapshotIndex, 1, ...)`），而实测 DB `applied_term=2`、快照落为 `1-123-…` 而
  `applied_index=122`：**持久化 raft (index, term) 声明被回退**。按报告意见修，不因「当前不可利用」而豁免。
- **S5-12（42 fixture）**：采纳报告的 Mandate 定性——**不得给 42 的 fixture 加 `--ack-alerts` 绕过产品写保护**；
  改顺序让 drill 走到自己的脊，或订正 ledger 停止声称不可执行的 #49 sim pin。
- **S5-20/S5-21（runner 仅剩两项）**：contract test 必须把**真 producer 接到真 parser**；waiver 改**逐项**
  （`--allow-product-red=<drill>[,…]`）+ retried-GREEN 不得作 headline ALL GREEN。
- **S5-22/S5-23 及其余 MODERATE/MINOR**：全采纳，按报告的精确修法执行。
- **§M1（本节）**：修 91 及全库 `| grep -q` 腰斩模式；登记 #50 并以 signature-guarded 断言钉住中断态。

---

## 主进程修复状态（本窗口实做，2026-07-16）

### 已修 + 已验证

| # | 修复 | 验证 |
|---|---|---|
| **S5-01** | darwin 构建断裂：把 `Renameat2/RENAME_EXCHANGE` 拆到 `internal/cluster/exchangedir_linux.go`（`//go:build linux`）+ `exchangedir_other.go`（非 Linux 返回 `errExchangeUnsupported`），`offline.go` 去掉 `unix` 依赖 | `GOOS=darwin GOARCH=arm64/amd64 go build ./cmd/tether` **均通过**（修前 `undefined: unix.Renameat2`）；linux 仍 OK。`build/goreleaser.yaml` 发 darwin/amd64+arm64 → 发布流水线恢复 |
| **S5-02** | 属主移植：交换前 `mirrorTreeOwnership(liveRaft, stagedRaft)` 把 live raft/ 的 uid/gid+目录 mode 镜像到重建树 | root 跑 runbook 不再把 raft/ 变 root:root → `User=tether` daemon 不再 EACCES 永久 crash-loop |
| **S5-24** | 无能力探测/无回退：`swapDirs` 优先原子交换；EINVAL/ENOSYS/EOPNOTSUPP → **回退三步 rename** 并 loud-warn（不再在 roster prune **之后**硬失败 = S5-04 的半完成砖态） | 三壳构建 + `go test -race ./internal/cluster/ ./internal/clusteroffline/` 通过 |
| **S5-15** | 三处 terminal 前置门（driveJoin 的 seeds converge / PlanClearGrowActive、driveRetire 的 seeds converge）从 `recordOpError`（无限重试、永不升级 = #45 扩大）改走 `blockAfterAttempts` → 达 `opMaxAttempts` 升级 `OpStateBlocked`（`cluster ops confirm/abort` 可处置） | `make lint` 0 issues；`go test -race ./internal/broker/` 通过 |
| **§M1（主进程独有）** | 91/42 的 `\| grep -q` 腰斩：新增 `out_matches`（跑到完成再匹配，无管道）；91/42 改为**跑到完成 + 断真实后置条件**（`nats.conf` 无 `cluster{}` 块，而非 grep 过早日志行）；91 新增 **settled-liveness 门**（admin socket 答复、非 exit-70 crash-loop）；92 的 dry-run 管道改 `out_matches`；`tests/lint-drills.sh` 新增 **sigpipe-truncation 静态禁令**（批次硬闸 0 违规，legacy 出 advisory：12/20/31/74 仍有同模式） | **91 从 ASSERT-FAIL → GREEN，37 断言 0 失败**（真机 weilandserver）。这证明 round-4 把 91 判为「真实产品缺陷（seeds 不收敛）」**是错的**——见 §M1 三次受控实验 |

**§M1 的产品面结论**：91 复跑 GREEN 后，`C force-single de-clustered nats.conf` 与 `C seeds converge to
survivor-ONLY` **双双 PASS** —— seeds 收敛机制本来就正常。round-4 implementation §结论 1（「91 需要开发者继续
定位……属于真实产品问题」）**应予撤回**；真正的缺陷是 harness 的 oracle 腰斩了被测操作。

### 未修（如实登记，附精确修法，下一窗口执行）

- **S5-03**（MAJOR，blocking）目录交换绕过 bolt-lock 互锁 + deferred `RemoveAll` 把旧 store 从复活的 daemon 脚下 unlink → 已 ack 写静默丢失。修法：交换全程持有 bolt 锁 / 交换前后复检 daemon，且旧 store 延后回收。
- **S5-04**（MAJOR，blocking）force-single 非原子四步、无 fail-closed 中断标记；**与 §M1 同根因**：`ForceSingle`
  内部先打「force-single complete」再由 CLI 做 nats.conf 去集群化，任何中断（SIGPIPE/SIGTERM/OOM/断电）都留下
  **不可恢复砖态**（实测：force-single 拒绝重跑——`requires --confirm-peers-dead listing EVERY other node_id`
  而 peer 已被 prune 光；`reconcile nats --to-standalone` 又 socket-gated 需要起不来的 broker → **死锁**）。
  修法：写中断标记 + 让 force-single 在标记存在时可续跑完成；「complete」日志移到真正完成之后。
- **S5-05**（MAJOR，blocking）driveJoin 在 terminal SERVING 释放 `cluster_grow_active`，解禁 P8-rebalance 窗口。
- **S5-06…S5-09、S5-11**（mutation 证实的覆盖洞）落地前必须补真行为测试。
- **S5-10**（term/index 回退）：重建快照 term 硬编码 1、`CurrentTerm` 归 0、index floor 取 `applied_index` 而非
  store 高水位。我已独立在真机观测到该事实（快照 `1-123-…` vs DB `applied_term=2`/`applied_index=122`）。
- **S5-12/S5-20/S5-21/S5-23** 及其余 MODERATE/MINOR：按报告修法执行（42 fixture 顺序不得加 `--ack-alerts` 绕过
  产品写保护；contract test 必须真 producer 接真 parser；waiver 改逐项；40 的裸 `retir` 匹配收紧）。

**本窗口硬闸**：`make lint` 0 issues · `go test -race ./internal/cluster ./internal/clusteroffline ./internal/broker ./internal/agent` 全过 · linux+darwin(arm64/amd64) 构建通过 · 9-drill lint 批次 0 违规 · hermetic verdict-contract 三壳 ALL PASS · 91 真机 GREEN(37)。
