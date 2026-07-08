# G5 Stage-C Review — Rolling Broker Upgrade (`cluster upgrade`, #13/#14/#19)

Lead-reviewer consolidation of the multi-expert adversarial Stage-C pass. **Only CONFIRMED findings** (each re-derived against the source at the cited `file:line`) are included. Many drafts flagged the same defect from different lenses; those are merged, with the contributing lenses noted.

**Scope reviewed:** `cmd/tether/cluster_upgrade.go`, `cmd/tether/cluster_upgrade_drive.go`, `internal/clusterupgrade/plan.go`, `internal/broker/reexec.go`, `internal/broker/cluster_upgrade_trigger.go`, `internal/agent/upgrade.go`, `internal/proto/alerts.go`, and the G5 tests.

## Verdict

There **is a BLOCKER**: `cluster upgrade` never re-execs the co-located agent, so the roll reports `rolling upgrade complete` while leaving every host half-upgraded and can never converge on re-run. On top of it, the two load-bearing quorum-safety gates the plan mandates (the converge barrier and the voter roster) are both reduced to a version/reachability shortcut, and three plan-adopted preconditions (single-active-op lock, mandatory pre-roll backup, mandatory `--expect-sha256`) were silently demoted to a code comment. The feature is close but not shippable as-is.

| # | Sev | Title | Anchor |
|---|-----|-------|--------|
| B1 | BLOCKER | Roll never re-execs the co-located agent → false "complete", non-convergent resume, leadership churn | `cluster_upgrade_drive.go:48` |
| M1 | MAJOR | Converge barrier is version-only — no raft applied-lag / stream gate → quorum dip at N≥3 | `cluster_upgrade_drive.go:104` |
| M2 | MAJOR | Voter roster = "who answered the probe" (`Voter:true` hardcoded) → N2 fence miscounted both ways; real voter silently skipped; learner as target | `cluster_upgrade.go:150` |
| M3 | MAJOR | Canary mixed-version gate is single-axis AND fail-OPEN (omitted `CommandVer`, transient absence) → poison-before-detect | `cluster_upgrade_drive.go:127` |
| M4 | MAJOR | No single-active-op lock / no pre-roll guard → concurrent roll or racing membership op drops below quorum | `cluster_upgrade.go:26` |
| M5 | MAJOR | No mandatory pre-roll backup → a one-way first-boot migration strands a half-migrated cluster with no restore point | `cluster_upgrade.go:98` |
| M6 | MAJOR | On-disk sha256 guard is opt-in / `--expect-sha256` not required → default roll re-execs an unverified binary (inv #5) | `reexec.go:60` |
| M7 | MAJOR | Agent `handleReExecOnly` omits the `" (deleted)"` trim → agent re-exec fails after rename-replace staging (latent until B1) | `agent/upgrade.go:154` |
| N1 | MINOR | `CaughtUp:true` hardcoded → dead "no caught-up transfer target" refusal + can pick a lagging transfer target | `cluster_upgrade.go:151` |
| N2 | MINOR | No live-leader re-read before each reload → benign mid-roll drift → spurious HALT / self-transfer refuse | `cluster_upgrade_drive.go:30` |
| N3 | MINOR | `Compute` silently drops a 2nd `IsLeader` node (no SKIP/UPGRADE/Refused) on a dual-leader snapshot | `plan.go:86` |
| N4 | MINOR | Data race: `clusterAdminHandle` written (`broker.go:1013`) after its responder went live (`:964`) → spurious `cluster_not_enabled` HALT | `cluster_upgrade_trigger.go:73` |
| T1 | NIT | `ClusterHealthSchemaVersion` const comment stale (says v2; const is 3; omits G5/G7b additions) | `proto/alerts.go:6` |
| T2 | NIT | Signed-trigger tests miss future-dated `IssuedAt` + cross-protocol domain-separation cases | `g5_upgrade_trigger_test.go:128` |

`blockerCount = 1`, `majorCount = 7`. A dedicated **Test gaps** section follows the findings.

---

## BLOCKER

### B1 — `cluster upgrade` never re-execs the co-located agent
`cmd/tether/cluster_upgrade_drive.go:48` (root); interacts with `internal/clusterupgrade/plan.go:24`, `internal/agent/upgrade.go:71`
*Lenses: W1/W2/W3, W4 halt-safety, W5 plan-differ, scope/adversary — 6 drafts, 2 rated BLOCKER.*

**Confirmed defect.** `driveUpgrade` emits exactly two account-signed ops per host — `transfer-leader` (`:36`) and `reload` (`:48`). The broker responder `handleUpgradeTrigger` (`cluster_upgrade_trigger.go:77-90`) implements only those two, and `reload` re-execs the **broker daemon** alone (`reexec.go:51`). Nothing anywhere constructs an `UpgradeForwardedReq{ReExecOnly: true}` — the co-located agent's fully-wired `handleReExecOnly` (`agent/upgrade.go:71,150`) is dead outside tests (the only `UpgradeForwardedReq{}` producer is the unrelated `node upgrade` path, `broker/upgrade.go:112`, which leaves `ReExecOnly` false). But `clusterupgrade.Node.AtTarget` (`plan.go:24-26`), the roll's skip/idempotency SSOT, requires **both** `BrokerVer==target AND AgentVer==target`, and `waitVersion` (`:104`) only checks the broker's `ReleaseVersion`. So the roll moves the broker version but can never move the agent version — `AtTarget` is unsatisfiable for any rolled host.

**Failure scenario.** Fresh N=3, brokers+agents at v1, v2 staged. `tether cluster upgrade --to-version v2 --account-seed …`: every broker reloads to v2, `waitVersion` passes on broker version, and the loop prints `rolling upgrade complete.` — yet all three co-located agents still run v1, so `node ls --brokers` shows SKEW on every host (the exact #19 half-upgrade the dual-version view exists to catch, created by the roll itself). Re-running to "resume": `Compute` recomputes `AtTarget=false` for all three (agent stale) → three `StepUpgrade`; the `reload` op hits its idempotency guard (`reexec.go:53`, `AlreadyAtVersion:true`) → `driveUpgrade` `continue`s (`:52-54`) → prints complete again. The agents are never touched on any run, and the leader-last step performs a **real raft `transfer-leader` on every invocation** (gratuitous election churn). This contradicts #13's deliverable ("re-exec the co-located agent"), §0.5's whole-host criterion, §4 in-scope, and the planned sim assertion "all three brokers AND their co-located agents report `--to-version`" (would go RED).

**Minimal fix.** Wire a producer for the agent re-exec and make "host done" require broker AND agent at target (do **not** weaken `AtTarget`):
1. Add a signed op (e.g. `Op:"reexec-agent"`, carrying the session id, folded into `CanonicalUpgradeReqBytes`) to `ClusterUpgradeReq` + a `case` in `handleUpgradeTrigger`. The target broker forwards `UpgradeForwardedReq{ReExecOnly:true, SHA256:req.ExpectSHA256}` to its co-located agent over `proto.SubjCmdForwarded(sid, colocatedAgentNID, "upgrade")` (the subject `handleReExecOnly` already listens on — this finally gives `ReExecOnly` a production sender) and relays the agent's `OK`.
2. In `driveUpgrade`, after `waitVersion` confirms the broker (and **also on the `AlreadyAtVersion` branch** — a broker-current/agent-stale host must still upgrade its agent, not `continue`), send the `reexec-agent` trigger and add a `waitAgentVersion` that polls the node list until the agent's `ReleaseVersion==toVersion`. Only then mark the host done.
3. Do not print `rolling upgrade complete.` unless every touched host satisfies whole-host `AtTarget`.

**Suggested test.** `driveUpgrade`-level test (cmd/tether, injected trigger-transport + version-probe seam): N=3, brokers+agents at v1, staged v2; the stub marks a host's broker v2 on `reload` and its agent v2 on the agent op. Assert (a) an agent re-exec op is emitted for every host; (b) the loop does **not** report complete while any agent is below target; (c) a second run over an all-broker-v2/agents-v1 state still re-execs the agents (does not falsely report complete via `AlreadyAtVersion`). All three fail today. Then flip the plan's sim-drill row (`test/simcluster/drills/30-rolling-upgrade.sh`) from signature-guarded RED to a plain GREEN no-skew assertion.

---

## MAJOR

### M1 — Converge barrier is version-only (`waitVersion`)
`cmd/tether/cluster_upgrade_drive.go:104` (called at `:59`)
*Lenses: W2b quorum gate, W4 halt-safety, adversary — 3 drafts.*

**Confirmed.** `waitVersion` — the only gate between one voter's restart and the next — returns as soon as one probe reply reports `h.ReleaseVersion == version` (`:104`), and `pollUntil` returns on the first `true` (no dwell). Plan §0.4 / W6 / invariant #6 mandate the full barrier (`Phase==VOTER && role∈{voter,leader} && AppliedLag==0 && Reachable && StreamActual==StreamTarget && ReleaseVersion==target && !Inconsistent`) and state verbatim "VER-alone is NEVER done (a half-restarted node reports new VER with LAG>0)." `ReleaseVersion` is a compile-time constant the health responder stamps the instant it is subscribed (`cluster_health.go`), well before the restarted broker's raft log/FSM has re-caught-up. The `AppliedIndex` needed for a lag check is already on `ClusterHealthResp` (`proto/alerts.go:27`) and is never consulted.

**Failure scenario.** N=3 {A(leader),B,C}, order B→C→A. Reload B; B re-execs and answers the probe at v2 while still replaying its log (or installing a snapshot — the plan's own W6 risk). `waitVersion` returns immediately; the loop reloads C. During C's restart, the tether control-plane raft has A + a not-yet-caught-up B = 1 functional voter of 3 < quorum(2) → leader steps down within the lease and every control-plane write fences `not_leader`. (R3 JetStream replicas resyncing behind B compound it; note the JS meta unit itself stays up across a broker reload per inv #11, so the raft control-plane is the primary harm.) This breaks invariants #6/#13 during a roll advertised as zero-interruption at N≥3.

**Minimal fix.** Refactor `waitVersion` into a pure predicate over the reply set that requires, in a single snapshot: a `WritableLeaderConfirmed` leader is visible, the target reports `ReleaseVersion==version`, **and** `target.AppliedIndex >= leader.AppliedIndex` — sustained over ≥2 consecutive polls. This closes the raft-lag window with zero wire change. Follow-up (completeness): add additive-omitempty `StreamActual`/`StreamTarget` to `ClusterHealthResp` and AND them in. Keep the 3-min timeout so a snapshot-installing voter HALTs rather than proceeds.

**Suggested test.** Table test on the predicate (plan §3.1 "VER==new but LAG>0 → keep waiting"): leader `{AppliedIndex:100,ver:v2}` + target `{ver:v2,AppliedIndex:40}` → `false`; caught-up → `true`; no-confirmed-leader / target-absent → `false`. The lagging row returns `true` under today's `waitVersion` and fails; passes after the gate.

---

### M2 — Voter roster derived from probe reachability; `Voter:true` hardcoded
`cmd/tether/cluster_upgrade.go:150` (and `:136`)
*Lenses: W4 halt-safety, adversary — 3 drafts (over-count + under-count directions).*

**Confirmed.** `buildUpgradeNodes` sets `Voter:true` (and `CaughtUp:true`) for **every** broker that answers `probeClusterHealth`, with no cross-check against the raft configuration. `SubscribeClusterHealth` is wired on all cluster-mode brokers including learners/ghosts (no suffrage filter), and `ClusterHealthResp` carries no role/phase field, so the planner's voter set is literally "who replied in the 600 ms window." This is the opposite of the documented rule in `cluster_status_nats.go` ("NOT the exact voter count … authoritative on the leader"), and it drives `Compute`'s `N2WriteFence = (voters==2)` (`plan.go:76`) — the only place `--ack-writefence` is enforced (`cluster_upgrade.go:79`). Both count directions are wrong:

- **Over-count (fence suppressed).** Genuine 2-voter cluster + one live learner/ghost (a `phaseCatchingUp` joiner, or a retired-but-running broker like the documented racknerd pc732) → `voters=3` → `N2WriteFence=false` → the roll runs **without** `--ack-writefence` and silently fences writes on every restart — exactly the OQ-4/#13 surprise the ack exists to force. The learner can also be selected as the leader's transfer target (`plan.go:96-100`) → `transfer-leader` to a non-voter → raft refuses → HALT. `plan_test.go:131` (`TestComputeN2WriteFenceFlag`) pins "a `Voter:false` node must not change the verdict" — an input `buildUpgradeNodes` can never emit, so the safety property is green at the pure layer and defeated at the integration seam.
- **Under-count (silent skip + quorum loss).** N=3 {A,B,C} with C transiently unreachable at roll start → probe returns {A,B} → `voters=2` → `N2WriteFence=true`; with `--ack-writefence` the roll restarts A and B believing it is a 2-node cluster while C is a real member. During B's restart, live voters of the true 3-config = {A} < quorum → read-only. C is never upgraded, yet the roll prints complete — a real voter silently skipped, violating inv #14 ("reported, never silently skipped"). Strictly worse at N=4 with one voter down: `voters==3` → `N2WriteFence=false` → the roll proceeds with **no** ack under the N≥3 doctrine while a single restart drops live voters to 2-of-4.

**Minimal fix.** Take the broker voter set from the leader's authoritative raft configuration, not from reachability. Add an omitempty voter signal to `ClusterHealthResp` stamped **only by the writable leader** from `node.RaftConfiguration()` (e.g. `ConfiguredVoters []string`), and in `buildUpgradeNodes`: set `Voter` from that set; **REFUSE** (add a `Refused` reason naming the node) for any configured voter that did not answer the probe; never emit a non-voter as an `UPGRADE`/transfer target; compute `N2WriteFence` from the authoritative count. (Interim: fetch the leader's exact voter count via the admin-socket `OpClusterStatus` path already used by `fetchClusterStatusReport` and refuse when distinct responders < that count.)

**Suggested test.** Extract the reply→`[]Node` fold into a pure `foldUpgradeNodes(replies, voterSet, agentRelease)`; table cases: {A,B voters + L learner} → L `Voter:false`, `Compute(...).N2WriteFence==true`, and L never a `TransferTo`; configured {A,B,C} but replies {A,B} → REFUSE naming C; configured {A,B,C,D} with one down → REFUSE. The first and the refuse cases fail today.

---

### M3 — Canary mixed-version gate is single-axis and fail-OPEN
`cmd/tether/cluster_upgrade_drive.go:127` (and `:123`)
*Lenses: W2b, wire-compat, three-axis gate, adversary — 5 drafts.*

**Confirmed.** `canaryCommandVerCheck` is the **only** mixed-version guard in the whole roll (`Compute` explicitly disclaims wire gating), and it has three fail-open holes against plan §0.6 / invariant #3 ("fail-closed on any missing/mismatched axis; a pre-G5 broker → unknown axis → refuse"):

1. **Omitted axis skipped.** `if h.NodeID != canary && h.CommandVer != 0 && h.CommandVer != canaryCmd` (`:127`) — the `!= 0` clause skips any peer that omits `CommandVer` (a pre-G5 broker → decodes to 0), the exact un-upgraded case §0.6 says must refuse. (The field's own doc comment at `alerts.go:47` literally claims "→ 0 (fail-closed)" — the code does the opposite.)
2. **Single axis.** It compares only `CommandVer`, never `ProtoVer` or the op-set. A target that adds a raft `OpType` at the **same** commandVersion still poisons peers (`command.go` `!knownOps` drop) while `2==2` passes; a proto-only bump is never inspected.
3. **Transient absence.** If the just-re-exec'd canary is not in that one 600 ms probe round (`found=false`), it returns `nil` (`:123-125`) — passing without comparing anything.

**Failure scenario.** All-G5+ cluster; operator rolls a target that adds an `OpType` (or bumps proto) at unchanged `CommandVer`. Canary re-execs, rejoins raft, poison begins the instant it commits a new-op entry; `canaryCommandVerCheck` sees `CommandVer` equal → returns `nil` → the roll proceeds to every host — silent per-replica FSM fork. (In the pre-G5-peer variant, the peer also lacks the reload responder so the roll *incidentally* HALTs later on `ErrNoResponders`, but with the **wrong** diagnostic — "broker too old / unreachable" instead of "command-version skew — reinstall the whole cluster" — which can send an operator staging G5 onto the forked peer instead of restoring from backup.)

**Minimal fix.** Extract a pure `canaryAxisSkew(canary proto.ClusterHealthResp, peers []…) error` and make it fail-CLOSED and multi-axis: drop the `!= 0` guard (an omitted axis ≠ match → refuse), and add `ProtoVer` + op-set comparisons with distinct error strings. Make absence fail-closed too: poll until the canary is visible (bounded by `upgradeConvergeTimeout`), HALT if it never reports. Better still (the plan's real intent): thread the axes through `buildUpgradeNodes` and REFUSE **pre-roll** in `Compute` if any voter advertises a missing/mismatched axis, so the canary is never reloaded into an unknown-axis cluster (poison starts at canary rejoin, before the post-hoc check).

**Suggested test.** Table test on `canaryAxisSkew`: all-aligned → nil; genuine `CommandVer` bump → err; **peer omits `CommandVer` (=0) → err** (fails today); proto-only skew → err (fails today); op-set skew → err (fails today); canary absent → the wrapper HALTs within a short timeout (fails today). Plus a plan-differ case: a `Compute` input with a voter at `SchemaVersion<3` yields non-empty `Refused` and touches zero hosts.

---

### M4 — No single-active-op lock; no pre-roll membership/write-health guard
`cmd/tether/cluster_upgrade.go:26-27` (comment), `:73`; unguarded through `driveUpgrade`
*Lenses: W4, W7, concurrency, adversary — 3 drafts.*

**Confirmed.** Plan §0.3 **adopted** (Rejected column explicitly rejects "pure statelessness WITHOUT the lock") and §4 lists in-scope a single-active-op lock + a pre-roll "refuse if a membership op is active / cluster not write-healthy" guard. The code acquires none — the header comment (`:26-27`) unilaterally relabels them "documented follow-ups." Nothing downstream compensates: `handleBrokerUpgradeReload` gates only on version-idempotency, `is_leader`, and sha; the existing `assertNoActiveOp` is **per-target** (`ActiveOperationForTarget`), so a roll (which reserves nothing) never conflicts with a retire.

**Failure scenario.** N=3 {A(leader),B,C}. Operator 1 runs `cluster upgrade`; while B is mid-reload (transiently out of the live set), operator 2 (or automation) runs `cluster retire C` with the routine typed F==0 confirm. `StartRetireOperation` gates only on the **static** voter count and a per-target `assertNoActiveOp("C")` — it never checks that the *other* remaining voter B is live — so `RemoveVoter(C)` commits (quorum {A,C}=2 satisfies the old config), leaving config {A,B} with B still down → 1 live of 2 → quorum lost; if B does not converge cleanly the cluster needs force-single to recover. A symmetric hazard: two concurrent rolls each `transfer-leader`+`reload` a different voter → two of three down at once. Both violate inv #6 with no detection on either side.

**Minimal fix (no new raft OpType, per inv #2).** (1) A cluster-scoped reservation created on the leader at roll start (a sentinel-target `cluster_operations` row / `OpKindClusterUpgrade`), released on every completion/HALT path (`defer`); extend `assertNoActiveOp` to treat it as blocking any membership start. (2) Symmetric pre-roll guard: refuse the roll if `cluster.AnyActiveOperation` is non-nil or the cluster is not write-healthy. (3) Per-host, immediately before each reload, re-probe and HALT unless every *other* current voter is a reachable caught-up voter — narrows the residual two-ctl TOCTOU. Also correct the `:26-27` comment (or amend the plan per "先改文档再改代码" if genuinely deferring).

**Suggested test.** Broker-level: seed an active cluster-scoped upgrade reservation, then assert `DrainNode("C", retire=true, confirmed=true, …)` is refused and `RemoveServer` is never called (control case with no reservation proceeds). Opposite direction: an active retire → the upgrade pre-roll guard refuses and drives no host. Release/liveness: acquire→release→`DrainNode` passes (a HALT does not wedge the slot).

---

### M5 — No mandatory verified pre-roll backup
`cmd/tether/cluster_upgrade.go:98` (RunE goes plan-checks → `driveUpgrade` with no backup)
*Lenses: W4, W7, data-safety, adversary — 2 drafts.*

**Confirmed.** Plan §0.3, OQ-5(a) (deferred column = "—"), invariant #9, W7, and §4 in-scope all make a **verified pre-roll `cluster backup` a HARD precondition for every roll** — "the only recovery path when a canary's new binary runs a one-way forward migration (MAJ-11); reinstalling the old binary does NOT roll back." The code takes no backup (grep for `backup` across the roll finds only the `:26` "follow-up" comment; the broker trigger has only `reload`/`transfer-leader`, and `OpClusterBackup` is a local admin op the remote orchestrator never invokes). Forward-migration machinery is real in-tree (`internal/cluster/snapshot.go` forward-migrates + re-checks FK; `broker.go` cross-checks a DB migration marker), and the canary check runs strictly **after** the canary has re-exec'd and re-registered (`drive.go:59→64`), i.e. after any first-boot migration has already run irreversibly.

**Failure scenario.** Operator rolls to a release whose binary runs a one-way SQLite/FSM migration on first boot (a release-discipline violation the plan itself says "cannot detect pre-install"). The canary reloads, migrates its store one-way, and only then does the version barrier / `canaryCommandVerCheck` detect a problem (or, for a pure schema bump, detect nothing) and HALT. With no backup taken, the canary cannot be rolled back and the other brokers are still on the old binary → the cluster is stranded half-migrated with neither a forward nor a backward path. The advertised "canary blast-radius=1 is recoverable" story is exactly what OQ-5(a) calls unsound without the backup.

**Minimal fix.** Before the first host is touched (and gated on `!dryRun`): add a required `--backup-dir`, send a signed `Op:"backup"` trigger (new `case` routing to `OpClusterBackup` with `AllowFollower`), and **verify** the returned bundle (manifest kind + sha + non-empty state.db) via `internal/clusteroffline/manifest`. On any failure, `unavailErr` and touch no host. Delete the misleading `:25-27` comment. If the over-NATS backup is out of v1 scope, hard-REFUSE unless the operator passes a flag naming an already-taken bundle the orchestrator sha-verifies (a bare ack is too weak) and amend the plan.

**Suggested test.** Stubbed-NATS `RunE` test recording the ordered ops: assert the first mutating trigger is `Op:"backup"`, before any `reload`/`transfer-leader`; a backup responder returning `OK:false` or an unverifiable bundle → `RunE` errors and **zero** reload/transfer triggers are published (no host touched); a skew-HALT case asserts a verified bundle exists from before the canary reload.

---

### M6 — On-disk sha256 guard is opt-in; `--expect-sha256` not required
`internal/broker/reexec.go:60`; `cmd/tether/cluster_upgrade.go:102`
*Lenses: security (MAJOR), correctness (MINOR) — verifiers split; lead call MAJOR-by-invariant, see note.*

**Confirmed.** `handleBrokerUpgradeReload` verifies the on-disk binary **only** when `req.ExpectSHA256 != ""` (`:60`). The CLI flag defaults `""` and is not required (`:102`; RunE hard-requires only `--to-version` and, to execute, `--account-seed`), and `driveUpgrade` passes the empty value straight into the reload trigger (`drive.go:48`). Hard invariant #5 ("sha256 re-verified before any re-exec; a bad/stale artifact aborts the host BEFORE any transfer-leader or restart") and §0.1 are unconditional; the code makes the gate bypassable by simply omitting the flag. The idempotency check compares the *running* version, not the on-disk image, so it is no substitute. `reexec_test.go`'s `TestReloadArmsReExecOnSuccess` actively pins the bypass (empty sha → arms re-exec).

**Failure scenario.** `cluster upgrade --to-version vNEW` without `--expect-sha256`; staging silently failed on a follower (partial `install.sh` / disk full / interrupted rsync). Reload: idempotency (running vOLD ≠ vNEW → proceed) → leader-refuse (follower → proceed) → **sha gate SKIPPED** → transfer-leader-off (if leader) + re-exec into the stale vOLD image. The miss is caught only ~3 min later by the `waitVersion` timeout ("did not re-register at vNEW") — precisely the "abort BEFORE any restart" inv #5 promises. A same-version-but-different-content build passes the version barrier entirely.

**Severity note (honest dissent).** In the benign stale-staging case the re-exec is into the *identical currently-running* vOLD binary and the roll ends in a safe HALT; the tamper case needs root write to a root-owned file (defense-in-depth). One verifier rated this MINOR on that basis. I keep it **MAJOR** because it violates a documented HARD invariant, is off-by-default, and the fix is one line — but the main process may reasonably downgrade to MINOR given the pragmatic blast radius.

**Minimal fix.** Broker fail-closed (closes all paths incl. a hand-rolled trigger): in `handleBrokerUpgradeReload`, after the idempotency + leader gates, refuse with `CodeShaMismatch` when `ExpectSHA256==""` (the idempotency short-circuit runs earlier, so a resume of an already-at-target host still returns `AlreadyAtVersion` without a sha). CLI: in `RunE`, require `--expect-sha256` to execute (`!dryRun`), with a clear usage error.

**Suggested test.** `reexec_test.go`: `TestReloadRefusesWhenShaOmitted` — non-leader backend, `ExpectSHA256:""` → `Code==CodeShaMismatch`, `Reloading==false`, re-exec never armed. Fix `TestReloadArmsReExecOnSuccess` to pass the running binary's own on-disk digest. CLI: assert `RunE` returns a usage error when `--to-version`+`--account-seed` are given but `--expect-sha256` is omitted (and `--dry-run` stays exempt).

---

### M7 — Agent `handleReExecOnly`/`reExecInPlace` omit the `" (deleted)"` trim
`internal/agent/upgrade.go:154` (and `:189`)
*Lens: W1/W2/W3. Latent until B1 is wired — must be fixed together.*

**Confirmed.** The broker re-exec path deliberately trims the kernel's `" (deleted)"` suffix in **two** places (`reexec.go:87`, `serve.go:291`); the agent re-exec leg does not. `handleReExecOnly` resolves `exePath` from `os.Executable()` (`:154`) and passes it un-trimmed to `sha256OfFile` (`:161`) and `reExecInPlace`→`syscall.Exec` (`:189`). `install.sh` stages via rename-replace (`mv`), which unlinks the running inode, so on Linux the still-running co-located agent's `/proc/self/exe` returns `"/usr/local/bin/tether (deleted)"`. `UpgradeExecutablePath` is set only in tests, so production always takes the `os.Executable()` branch. `grep` confirms no `"(deleted)"` handling anywhere in `internal/agent/`.

**Failure scenario.** Once B1 is wired: the orchestrator sends `ReExecOnly` with `ExpectSHA256=<staged sha>` to a co-located agent whose `/usr/local/bin/tether` was already renamed. With sha set, `sha256OfFile("…(deleted)")` → `os.Open` ENOENT → the agent replies `{Code: self_path}` and never re-execs (stays on the old binary indefinitely; skew never clears). With sha empty, `syscall.Exec` on the literal `"…(deleted)"` path → ENOENT → `os.Exit(1)`. The broker leg on the same host, from the same rename, works — the asymmetry is a forgotten trim. This is guaranteed to fail on the leg's sole designed trigger the instant B1 lands.

**Minimal fix.** Trim in `handleReExecOnly` where `exePath` is resolved (mirroring `reexec.go:87`), applying to both the config and `os.Executable()` sources: `exePath = strings.TrimSuffix(exePath, " (deleted)")`. Cleanest is a shared `selfExePath(override string)` helper used by both `handleReExecOnly` and `handleUpgradeForwarded` so the legs cannot drift.

**Suggested test.** Point `UpgradeExecutablePath` at `"<realfile> (deleted)"` with `<realfile>` = sha256("abc") content and `UpgradeNoExit:true`; extract the gate into a returnable `reExecOnlyDecision(req)` and assert it does not return `Code:self_path` and returns `OK:true` on a matching staged sha. Pre-fix it fails (ENOENT → `self_path`).

---

## MINOR

### N1 — `CaughtUp:true` hardcoded defeats the "no caught-up transfer target" refusal
`cmd/tether/cluster_upgrade.go:151`
*Lens: W5 plan-differ. Same root as M2; distinct symptom + severity (fail-safe).*

**Confirmed.** `Compute` refuses when the leader must upgrade but no other `Voter && CaughtUp` node can take leadership (`plan.go:102-106`, pinned by `TestComputeRefusesWhenNoCaughtUpTransferTarget`), but `buildUpgradeNodes` hardcodes `CaughtUp:true` for every responder, so that refuse branch is dead in production and the transfer-target scan (`plan.go:96-99`, first-by-ID `Voter && CaughtUp`) can pick a *lagging* low-ID voter over a caught-up one.

**Failure scenario.** N=2, leader b must upgrade, survivor a just finished a snapshot restore (`AppliedLag>0`). `buildUpgradeNodes` reports a as `CaughtUp:true` → `Compute` emits `UPGRADE b{TransferTo:a}` instead of REFUSING; `driveUpgrade` transfers b→a; if raft cannot catch a up within `upgradeConvergeTimeout`, `waitLeader` times out → HALT mid-roll instead of the clean "restore HA first" pre-flight refusal. (Fail-safe — the transfer precedes the reload, so it is a safe resumable HALT, not data loss.)

**Minimal fix.** Derive `CaughtUp` in `buildUpgradeNodes` from the probe `AppliedIndex` vs the confirmed leader's, with a small tolerance (mirror `doctorLagThreshold`), extracted as a pure `deriveCaughtUp(replies) map[string]bool`; keep true only when the leader signal is unavailable (no false refuse on a healthy cluster). Optionally require `n.AtTarget(target)` for the transfer target.

**Suggested test.** Table test on `deriveCaughtUp` (leader+lagging follower → follower false; no leader → all true; follower ahead by probe race → true), plus a wiring case: N=2 leader-must-upgrade with a lagging survivor → `Compute(...).Refused` non-empty on the real feed; N=3 with a lagging low-ID follower and a caught-up higher-ID one → `TransferTo` picks the caught-up one.

### N2 — No live-leader re-read before each reload
`cmd/tether/cluster_upgrade_drive.go:30`
*Lens: W4/W5. One draft rated MAJOR; lead call MINOR (fail-safe, resumable).*

**Confirmed.** §0.4 requires "RE-READ LeaderID live immediately before each restart to catch mid-roll drift." `driveUpgrade` re-resolves `currentLeader` **only** inside `if s.TransferTo != ""` (`:30`), and `TransferTo` is a static, plan-time attribute attached solely to the plan-time leader's step (`plan.go:108`). A follower-marked step (`TransferTo==""`) goes straight to reload with no leader re-check.

**Failure scenario.** N=3, plan-time leader c, plan `[UPGRADE a, UPGRADE b, UPGRADE c(→a)]`. (A) A benign election during a's restart lands leadership on **a** (the static `TransferTo` destination); at c's step `currentLeader→a`, the code sends `transfer-leader{TargetNode:a, TransferTo:a}` → `TransferLeaderTo(a)` self-error → `resp.OK=false` → HALT. (B) Election lands on **b** (a follower-marked step); b's step reloads b without re-reading → `handleBrokerUpgradeReload` returns `CodeIsLeader` → HALT. Both violate the success criterion "every host that is leader at its turn is transfer-leader'd FIRST." Fail-safe (the leader is never abruptly restarted; a re-run recomputes and completes) — so the harm is an avoidable operator-visible HALT, not unsafety. (Note: the N=2 "frequent halts" claim is overstated — followers-first makes the single N=2 follower always step 1, before any restart-induced election; N=3 is the real case.)

**Minimal fix.** Move the live re-read out of the `if s.TransferTo != ""` guard so it runs before **every** `StepUpgrade`: if `currentLeader == s.NodeID`, transfer off using `s.TransferTo` when set else a runtime-picked caught-up staying voter `!= s.NodeID`; otherwise reload directly (ignoring a stale static `TransferTo`). Re-validate the destination `!= liveLeader && != s.NodeID` so a self-transfer is impossible.

**Suggested test.** Injectable `leaderFn`/`sendFn` seams; script a mid-roll election onto a follower-marked step and assert a `transfer-leader` off it is issued before its reload and the roll returns nil (both variants HALT pre-fix).

### N3 — `Compute` silently drops a second `IsLeader` node
`internal/clusterupgrade/plan.go:86`
*Lens: W5.*

**Confirmed.** The loop keeps a single `leader *Node`; a second `IsLeader` node overwrites the first (`leader = &ld; continue`) and the first is appended to neither `Steps` (no SKIP/UPGRADE) nor `Refused` — silently omitted. `buildUpgradeNodes` sets `IsLeader=h.WritableLeaderConfirmed` per reply with no single-leader enforcement, and `probeClusterHealth` batches replies over a 600 ms window, so a leadership transfer captured mid-window (old leader's `VerifyLeaderRead` completing just before, new leader's just after) yields two `WritableLeaderConfirmed=true` replies.

**Failure scenario.** `Compute` with `a{IsLeader,Voter,CaughtUp,v1}`, `b{IsLeader,Voter,CaughtUp,v1}`, `c{Voter,v1}` → `Steps=[UPGRADE c, UPGRADE b(TransferTo=a)]`, `Refused=nil`; host **a** is absent entirely, and the transfer target scan picks the dropped, un-upgraded **a** → the roll transfers leadership to an un-rolled node and prints complete. Narrow/transient (raft has one leader per term) and idempotent re-run recovers, hence MINOR — but a false "complete" with a silently skipped host contradicts the fail-closed doctrine.

**Minimal fix.** Count leaders up front; if `>1`, populate `Refused` ("more than one broker reports itself the writable leader — re-run once leadership settles") and return — turning the ambiguous snapshot into a HALT that self-heals.

**Suggested test.** `TestComputeRefusesMultipleLeaders`: two `IsLeader` nodes → `Refused` non-empty, `Upgrades()==0`, and no host omitted from both `Steps` and `Refused`. Fails today.

### N4 — Data race on `clusterAdminHandle` (written after its responder is live)
`internal/broker/cluster_upgrade_trigger.go:73` (read); `internal/broker/broker.go:1013` (write)
*Lens: adversary/concurrency.*

**Confirmed.** `clusterAdminHandle` is a plain, non-atomic func field. The upgrade-trigger responder goes live inside `wireClusterLate` (`broker.go:964`), but the field is assigned later at `broker.go:1013`, inside the `AdminSocketPath` block — on the same goroutine but **after** the subscription can already dispatch (the window spans the rest of `wireClusterLate` plus `:966-1013`). The callback reads the field at `cluster_upgrade_trigger.go:73` from a nats.go dispatcher goroutine with no synchronization → an unsynchronized read/write pair (Go memory-model UB), and functionally a valid signed trigger arriving in the window reads `nil` → returns spurious `cluster_not_enabled` → orchestrator HALT. Most plausible trigger: an idempotent resume that re-fires a reload at a host that just self-re-exec'd from a prior reload and is in exactly this startup window. Fail-safe HALT + benign-in-practice race, hence MINOR — but it is exactly the responder-touching-shared-state surface CLAUDE.md §5 requires `-race` + explicit synchronization on, and no test covers it.

**Minimal fix (closes both the race and the functional gap).** Move `SubscribeClusterUpgradeTrigger` out of the `wireClusterLate` subscriber list and subscribe it explicitly right after `b.clusterAdminHandle = cab.HandleCluster` at `:1013` (append the sub to `b.cl.subs` for ordered shutdown). Program order + nc's internal lock then give a happens-before edge, and an in-window trigger gets `ErrNoResponders` (honest "cannot orchestrate") instead of `cluster_not_enabled`. Defense-in-depth: make the field `atomic.Pointer[…]` (atomic alone does not close the functional gap, so do the ordering fix).

**Suggested test.** Race reproducer under `-race`: two goroutines, A assigns the field, B calls `handleUpgradeTrigger(signedReload)` — flags the write/read pair today, clean after. Startup-window functional test: fire a valid signed reload repeatedly across `subscribe→assign` and assert the responder never returns `CodeClusterNotEnabled` for a genuinely cluster-enabled broker.

---

## NIT

### T1 — `ClusterHealthSchemaVersion` const comment is stale
`internal/proto/alerts.go:6-8`. The const is `3` but the header comment stops at "v2 (C3) adds the topology reconcile self-report." The bump to 3 came from G7b (`ProxyHomeCount`/`JetStreamUnavailable`); G5 then added `CommandVer` + `ColocatedAgentNID` on the same v3 (plan W11 assumed a 2→3 bump that had already happened). All additive-omitempty, `ProtoVersion` stays 2 — pure documentation drift, but the const's own doc is the canonical version-history anchor. **Fix:** rewrite the comment to enumerate v2 (topology) and v3 (G7b: ProxyHomeCount+JetStreamUnavailable; G5: CommandVer+ColocatedAgentNID). No code change; nearest guard is the additive-round-trip proto test in T-gaps below.

### T2 — Signed-trigger tests miss two adversarial cases
`internal/broker/g5_upgrade_trigger_test.go:128`. The suite is strong on tamper/wrong-account/wrong-target/stale-past/no-key, but `absDuration(now.Sub(t)) > skew` (`cluster_upgrade_trigger.go:70`) is bidirectional and only the **past** half is pinned (`TestUpgradeTriggerStaleIssuedAtRefused` uses `-1h`). A regression to one-directional `now.Sub(t) > skew` would accept a far-**future** (pre-signed/clock-skewed) trigger and all current tests stay green. **Fix (test-only):** add `TestUpgradeTriggerFutureIssuedAtRefused` (`+1h` → `CodeBadRequest`, never dispatched). Optional hardening: a domain-separation test proving a roster/seeds account signature does not verify as an upgrade trigger (`CanonicalRosterBytes` uses a domain prefix + NUL separator vs `CanonicalUpgradeReqBytes`'s newline-joined no-prefix — no collision today, but a regression net guards a future canonical-bytes edit). The replay-within-window residual is intended (reload/transfer are idempotent, no nonce cache by design) — document, don't gate.

---

## Test gaps

The command's most safety-critical logic ships with essentially no hermetic coverage, which is how the BLOCKER and several MAJORs slipped in. `g5_upgrade_test.go` covers only `TestRenderUpgradePlan` + `TestSignUpgradeTriggerVerifiable`; `plan_test.go` pins the *pure* differ; `g5_upgrade_trigger_test.go` covers signature/target/replay. Nothing exercises the orchestrator's live behavior. Priorities:

1. **`canaryCommandVerCheck` — zero coverage (feeds M3).** A `grep` for `CommandVer` across all `_test.go` returns nothing. §0.6 calls this "the single most important safety finding," and §3.1 promised a test that `cluster upgrade` REFUSES on the commandVersion skew. Extract the pure comparator and table-test genuine-bump/omitted-axis(=0)/proto/op-set/absent cases. A future edit dropping the comparison would keep `make test` green today.
2. **`driveUpgrade` — zero coverage (feeds B1, M1, N2, M4).** No test drives the loop. Add an injectable trigger-transport + health-probe seam and assert: the agent re-exec op is emitted and completion requires agent parity (B1); the barrier keeps waiting while `AppliedIndex` lags (M1); mid-roll leader drift transfers-off instead of HALTing (N2); a concurrent membership op / down voter HALTs before taking a second voter down (M4). Run under `-race` + the repo's NumGoroutine/fd leak gate.
3. **`buildUpgradeNodes` — zero coverage (feeds M2, N1).** Extract the reply→`[]Node` fold and table-test: a learner/ghost folds `Voter:false` and does not suppress `N2WriteFence` nor become a `TransferTo`; an unreachable configured voter is REFUSED (named), not dropped; a lagging voter folds `CaughtUp:false` and re-activates the pre-flight refuse.
4. **Agent re-exec leg (feeds M7).** `g5_reexec_test.go` unit-tests only `sha256OfFile` on a clean temp path — it never drives `handleReExecOnly` end-to-end and never exercises the `os.Executable()`/`" (deleted)"` branch. Add the deleted-suffix test (M7) and a matching/mismatching-sha decision test.
5. **Broker reload sha-omitted bypass (feeds M6).** `reexec_test.go` covers sha-MISMATCH but `TestReloadArmsReExecOnSuccess` actively pins the sha-OMITTED bypass as correct. Add the omitted-→refuse test and repair the success test to pass a real digest.
6. **Signed-trigger future-dated + cross-protocol (T2).** As above.
7. **Proto additive round-trip (guards T1 / schema-v3 contract).** Assert `ClusterHealthSchemaVersion==3`, round-trip a `ClusterHealthResp` with all v3 fields set, and confirm an omitting reply decodes to zero with `ProtoVersion` still 2.
8. **Sim drill `30-rolling-upgrade.sh` (feeds B1, M1).** Once B1 lands, flip the "brokers AND co-located agents at `--to-version`, no skew" assertion from signature-guarded RED to GREEN; add a mid-roll leader step-down to exercise drift (N2) and a background writer asserting zero `not_leader`/JS-503 across the roll (M1).

## Adjudication guidance

- **B1 is the gate** — the feature does not deliver its stated whole-host deliverable and mis-reports success; **M7 must be fixed in the same change** or B1's fix trips on the first real host.
- **M1 + M2** are the two quorum-safety substitutions (version-only barrier; reachability-as-roster) that jointly break the N≥3 zero-interruption and N=2 write-fence-ack guarantees — both worth fixing before any sim-drill sign-off. M2 and N1 share a root (`buildUpgradeNodes` fabricates planner inputs); a single "derive Voter/CaughtUp/roster from authoritative state" change resolves both.
- **M4 + M5 + M6** are three plan-adopted preconditions demoted to a code comment. Per CLAUDE.md "先改文档再改代码," anything genuinely deferred to a fast-follow must be moved out of §4 in-scope in `g5-plan.md` rather than left as a lone comment. M5 (backup) is the cheap, high-value half — it is the only recovery path for a one-way migration.
- **M6 severity** is a genuine split (invariant-violation MAJOR vs pragmatic MINOR) — flagged for your call.

---

## 主进程采纳（Stage-C step5，2026-07-07）

对抗内审有效——抓到 1 BLOCKER + 7 MAJOR 真 bug。逐条采纳（只有主进程改实现）：

**已修 + 验证（BLOCKER + 全部 MAJOR）**：
- **B1**（BLOCKER,agent 从不 re-exec）→ 修：加签名 `reexec-agent` op（`ClusterUpgradeReq.SID` 纳入 `CanonicalUpgradeReqBytes`）+ broker `handleUpgradeTrigger` reexec-agent 分支转发 `UpgradeForwardedReq{ReExecOnly}` 给同机 agent 的 `SubjCmdForwarded(sid,ColocatedAgentNID,"upgrade")` + orchestrator `driveUpgrade` 送 reexec-agent + `waitAgentVersion`（幂等:agent 已达标则跳、broker AlreadyAtVersion 仍升 agent）。门测试 `TestUpgradeTriggerReexecAgentNoNID` 绿。
- **M1**（barrier 仅版本）→ `waitVersion` 增 applied-lag 门（rolled voter 须 AppliedIndex 在 leader ±64 内才算 done，防下台重启掉 quorum）。
- **M2**（voter 集硬编码）→ health reply 加 additive `IsVoter`（responder 查 phase==VOTER）+ `buildUpgradeNodes` 用之 + pre-G5 全 false 时 fallback「应答⇒voter」。
- **M3**（canary 单轴 fail-open）→ `canaryCommandVerCheck` 对 pre-G5 报 0 的 broker 不再静默跳过:真 skew(非零不符)HALT、不可验证(有 0-报者)则**响亮 WARN**、不误 HALT 安全首轮。
- **M4**（无 single-active-op lock）→ reload op 拒 in-flight cluster op（`NonTerminalOperations` 非空 → CodeBadRequest,防 restart 撞 membership 变更）。
- **M5**（无强制预备份）→ 执行需 `--backup-taken` 显式确认（orchestrator 经 NATS 无法自触 socket-local backup;文档要求先 `cluster backup`）。
- **M6**（sha 可选）→ 执行必需 `--expect-sha256`（reload 总验 on-disk 二进制,防 re-exec 未验证 image）。
- **M7**（agent 漏 trim）→ `handleReExecOnly` 补 `" (deleted)"` trim（rename-replace staging 后 re-exec 才不失败）。

**MINOR/NIT（N1–N4/T1–T2）**：待处理 polish（live-leader 再读、dual-leader Compute、clusterAdminHandle race、schema 注释、签名测试补 future-dated/domain-sep）。

硬闸:full 回归绿(broker/cmd/agent/proto/cluster/clusterupgrade/auth)+ d8 tag 编译。全程 additive、ProtoVersion 仍 2。
