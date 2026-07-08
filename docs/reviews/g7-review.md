# G7 Stage-C Review — Lead Reviewer Report

**Scope.** G7 = two leaf increments landed together: **G7a (data-plane)** — proxy-home render coherence (#2), `cluster status --homes --remote` aggregate (#16 homes fold), and `#18` return-edge auto-rebalance; **G7b (observability)** — `cluster status --remote` / `seeds show --remote` exit taxonomy (#16), and the sustained JS-503 signal (#20③).

**Method.** Adjudicated from the confirming-reviewer verdicts, re-verified the load-bearing code paths myself (D8 signature, `driveAutoRebalanceOnReturn` gate composition, `alert_reconcile.go` JS-503 raise/clear branches), **deduplicated** overlapping submissions (the two `#18`-coverage MAJORs → one; the two JS-503 ctl-fold MINORs → one; the two `seeds show --remote` findings [one filed NIT, one MINOR] → one MINOR), and re-ranked. Only CONFIRMED findings appear.

**Counts:** 1 BLOCKER, 2 MAJOR, 11 MINOR, 0 standalone NIT.

**One elevation, flagged for adjudication:** the JS-503 stuck-ACTIVE defect was filed MINOR by its confirming reviewer; I raise it to **MAJOR** (M1) with rationale below. A reasonable reviewer could hold MINOR — the main process should decide.

---

## BLOCKER

### B1 · [G7b] D8 integration suite no longer compiles — stale `SubscribeClusterHealth` caller
**`test/d8/integration_test.go:240`** (break introduced by the widened signature at `internal/broker/cluster_health.go:23`)

G7 grew the responder from 5 → 7 params:
`SubscribeClusterHealth(nc, node, db, now, topoSelf, jsUnavail func() bool, colocatedAgentNID string)`.
Both production callers (`internal/broker/clusterwrite.go:284,:289`) were updated; the sole **test** caller was left on the 5-arg form:
`broker.SubscribeClusterHealth(c.conns[i], c.nodes[i], c.dbs[i], now, nil)`.

**Failure scenario.** The break is behind the `d8_integration` build tag, so `make test` (untagged) and `make e2e` (`-tags e2e_matrix`) both stay green — it is invisible to the standard gates. But CLAUDE.md §5 mandates the D8 matrix precisely when broker cluster-health/transport code is touched (this batch). Running the mandated gate `go test -tags d8_integration -race ./test/d8/` (or `go vet -tags d8_integration ./test/d8/`) hard-fails: `not enough arguments in call to broker.SubscribeClusterHealth`. The **entire D8b regression net** — no-responders 503 fast-path timing (`:226-234`), `EvalDestructiveGate` zero-reply non-gating (`:235-237`), multi-broker corroboration probe (`:246-254`) — fails to build and provides zero coverage. This is a hard release blocker for a batch that touched the cluster-health surface.

**Minimal fix.** Pass the two new args at `:240`. Both callbacks are nil-guarded in the responder (`topoSelf!=nil` at `cluster_health.go:82`, `jsUnavail!=nil` at `:105`) and `colocatedAgentNID=""` is the documented CLI-fallback default, so `nil, ""` preserves this drill's behavior:
```go
sub, err := broker.SubscribeClusterHealth(c.conns[i], c.nodes[i], c.dbs[i], now, nil, nil, "")
```

**Suggested test.** Restoring buildability is the primary gate: `go test -tags d8_integration -race ./test/d8/` must compile and pass. Because the fix passes `nil/""`, the two new wire fields still get **zero** D8 coverage — add a focused sub-case that, on one broker, passes `jsUnavail=func() bool { return true }` and `colocatedAgentNID="Uagent…"`, probes via `probeHealth`, and asserts the returned `proto.ClusterHealthResp` carries `JetStreamUnavailable==true` and `ColocatedAgentNID=="Uagent…"` — pinning the G7 wire fields end-to-end so a future signature drift is caught by behavior, not only by the compiler.

---

## MAJOR

### M1 · [G7b] JS-503 signal sticks ACTIVE when a sustained-10008 outage transitions to a persistent non-10008 Observe error  *(elevated from MINOR)*
**`internal/broker/alert_reconcile.go:200-238`** (asymmetry at the `if oerr != nil` block, `:202-215`)

The JS-503 flag (`jsDownSince` + `SetJSUnavailable`) is mutated in only three Observe outcomes:
- **raise** — `oerr != nil && classifyJSUnavailable(oerr)` (sustained 10008): advance clock, raise at threshold (`:208-215`);
- **clear** — `else if rep.Observed` (a positive observation): reset + `SetJSUnavailable(false)` (`:216-221`);
- **demotion clear** — lost leadership: reset (`:176-183`).

An Observe result with `oerr != nil` **AND** `classifyJSUnavailable(oerr)==false` (a non-10008 error, e.g. a wrapped `ErrStreamNotFound` 10059 from `CollectStreamState` on a momentarily-absent `history-<sid>`, or a persistent `NumVoters` read failure inside `ObserveReplicas`) hits **neither** the raise branch (classifier false) **nor** the positive-clear branch (`rep.Observed` unreachable while `oerr != nil`). So once a sustained 10008 has set `jsUnavail=true`, a transition to a **persistent non-10008 error** leaves the atomic stuck `true` while the node stays leader. In **force-single N=1** leadership never changes, so the demotion-clear never fires either → the "impossible to miss" DATA-PLANE-DEGRADED banner is **stuck ACTIVE indefinitely**, contradicting **inv-6 ("无 stuck-ACTIVE")** — and, per the author's own comment (`js_health.go`), a 10059/timeout is explicitly "NOT JS wedged at 503", so if it must not *raise* the signal it must not *perpetuate* it.

**Failure scenario.** Force-single N=1 with wedged clustered JS raises the banner. Operator remediation partially recovers the meta, but one ACTIVE session's history stream is momentarily absent → `ObserveReplicas` returns wrapped 10059 every tick and never a clean positive observation. `jsDownSince` is not advanced, `SetJSUnavailable` is never called, leadership never changes → the JS-UNAVAILABLE banner stays lit even though the meta is answering, until the audit publisher happens to recreate the stream and a fully-clean observation lands (and never, if that pass is itself wedged).

**Why MAJOR (my elevation).** This is the only runtime product-code defect in the batch besides B1. It produces a **permanently-wrong operator alarm on force-single N=1 — the exact configuration #20③ was built to serve** (the racknerd incident) — with no self-heal path in N=1. That said, the blast radius is bounded: it only ever **over-reports** (banner stuck ON, never hides a real outage), and it requires a specific error-code transition. A reviewer holding this at MINOR is defensible; I rank it MAJOR because a self-perpetuating false alarm on the target config undermines the feature's whole reason for existing.

**Minimal fix.** Make the clear symmetric with the raise classifier: in the `if oerr != nil` block, add an `else` to the `classifyJSUnavailable` check that **clears** on any non-10008 error (JS is answering, just not at 503):
```go
if r.cfg.SetJSUnavailable != nil {
    if classifyJSUnavailable(oerr) {
        if r.jsDownSince.IsZero() { r.jsDownSince = now }
        if now.Sub(r.jsDownSince) >= jsDownThreshold { r.cfg.SetJSUnavailable(true) }
    } else {
        // non-10008 ⇒ JS is no longer wedged at 503; a stream-not-found/timeout would never
        // RAISE the signal, so it must never PERPETUATE it (inv-6; N=1 has no demotion escape).
        r.jsDownSince = time.Time{}
        r.cfg.SetJSUnavailable(false)
    }
}
```
This matches the existing "clear on the first positive Observe" philosophy (extended to "first non-10008 result") and does not regress the raise — a genuine no-quorum wedge returns 10008 consistently. It leaves the by-design 60s-threshold re-accrual on leader election untouched (see Rejected sub-claims).

**Suggested test.** Add to `internal/broker/g7_js_health_test.go`, reusing the existing `alertFake`/injectable-clock harness from `TestG7JSUnavailableSustainedDetection`: raise via sustained 10008 (advance clock past `jsDownThreshold`), assert `jsFlag==true`; then switch `observeErr` to a wrapped `&jetstream.APIError{ErrorCode: 10059}` returned every tick with `voters:1` (sole voter, never demotes), advance the clock, tick twice, and assert `jsFlag==false` (must clear) and stays false (must not re-raise). Fails on current code, passes after the fix; the existing sustained-detection test remains green.

### M2 · [G7a] `#18` auto-rebalance driver / gate composition / observe-wiring / convergence are untested — only the pure debounce arm has coverage  *(merges two MAJOR submissions)*
**`internal/broker/proxy_auto_rebalance.go:104-133` (gates), `observability.go:161-201` (observe-loop wiring)**

`g7_auto_rebalance_test.go` covers **only** the pure `autoRebalanceArm.tick()` (5 table cases), which receives `gatesClear` as a **caller-supplied literal bool**. A package-wide grep for `driveAutoRebalanceOnReturn`/`autoRebalanceEnabled`/`noInflightOps`/`recentProxyRehome`/`proxy_auto_rebalanced`/`TETHER_AUTO_REBALANCE` in `*_test.go` returns nothing. Everything that actually mutates the data plane is unverified:
- `autoRebalanceEnabled()` — the **ENV default-OFF** safety gate (`:104`; KD-3b / inv-11 black-hole-bounded). An inversion silently enables voluntary break-before-make moves, uncaught.
- the composition at `:115` — `len(downNow)==0 && !forceSingleActive(b.cfg.DB) && b.noInflightOps() && !b.recentProxyRehome()` — never run as a whole.
- `noInflightOps()` (`:137-140`), `recentProxyRehome()` (`:145-156`), the fire → `rebalanceProxyHomes(false)` → `pubSysEvent("proxy_auto_rebalanced")` flow (`:119-132`), the `observeOnce` return-edge computation (`prevDown[node] && !d.Active → returned`, `observability.go:171-173`), and the `runObserveLoop` `wasLeader true→false` `reset()` wiring (`observability.go:257-259`) — all untested.

The plan's own required hermetic cases **6–11** (`g7-plan.md:107-111`) — heap-then-return convergence to `max−min≤1` with **exactly one** `proxy_auto_rebalanced`; idle-zero-writes on an already-balanced return; orphan-clear/retire/force-single must NOT trigger; leadership-flap-while-returning under `-race` + the NumGoroutine/fd leak gate — plus the mandated e2e "hermetic proxy-rebalance-on-return" subtest (`g7-plan.md:129`) and the sim drill `NN-g7a-rebalance-return.sh` (`g7-plan.md:133`) are **all absent** (`test/e2e` has no G7; `test/simcluster/drills` has no g7a drill).

**Failure scenario.** A future edit drops or inverts the `!b.recentProxyRehome()` conjunct at `:115` (or inverts the `len(downNow)==0` term). `go test ./internal/broker/` stays fully green — the pure-arm tests feed `gatesClear` as a literal and never touch the DB-backed composition. Auto-rebalance is then free to fire **while the crash-failover reaper is mid-rehome** or while a broker is still down → two break-before-make movers fight the same `__proxy__` homes → unbounded churn + a black-hole window, guarding the KD-3b "black-hole bounded" invariant with nothing.

**Calibration (honest).** The trickiest logic (the debounce/oscillation math) *is* covered, and auto-fire is default-OFF, so the current production blast radius is ~zero. But default-OFF is itself an untested gate, and this is live-wired, data-plane-mutating code whose integration/adversarial cases the phase's **own finalized plan mandated and did not deliver**. Given the project's explicit adversarial-testing discipline (CLAUDE.md §5), this is a legitimate MAJOR Stage-C gap.

**Minimal fix.** No production logic change. Extract the inline composition into a unit-testable method so the DB-backed gates can be pinned **without** a raft/leader harness:
```go
func (b *Broker) autoRebalanceGatesClear(downNow []string) bool {
    return len(downNow) == 0 && !forceSingleActive(b.cfg.DB) && b.noInflightOps() && !b.recentProxyRehome()
}
```
then replace `:115` with `gatesClear := b.autoRebalanceGatesClear(downNow)` (behavior-preserving). This is the seam that makes the "drop-a-gate" regression go red at unit cost.

**Suggested test.** New `internal/broker/g7_auto_rebalance_gate_test.go` (uses `openDB(t)` + a fixed `Config.Now`; `&Broker{}` non-leader is safe — `rebalanceProxyHomes` errors rather than panics):
- **ENV default-OFF** (`TestAutoRebalanceEnabledDefaultOff`): empty / `off` / `1` / `true` / `ON` / `On` → false; `on` → true. Pins KD-3b/inv-11.
- **Per-gate composition** against `autoRebalanceGatesClear`: all-quiet → true; a voter in `downNow` → false; a `cluster_meta` `MetaKeyForceSingle` row → false; a non-terminal `cluster_operations` row (via `cluster.NonTerminalOperations`) → false; a `__proxy__`/`ALLOCATED` `port_allocations` row stamped `last_rehome_at = cluster.LitTime(now.UTC().Add(-autoRebalanceQuietWindow/2))` → false, and stamped `-2*autoRebalanceQuietWindow` → true. Deleting any conjunct from `autoRebalanceGatesClear` makes one of these go red.

**Owed per plan (heavier, needs the leader/Propose harness — track but may split):** cases 7–11 convergence / idle-zero / orphan-clear / leadership-flap-under-`-race`, the e2e subtest, and the `NN-g7a-rebalance-return.sh` sim drill. See also the narrower structural guards **m7, m8, m9** below, which are individually-trackable facets of this gap.

---

## MINOR

### m1 · [G7a] `proxy status` renders a crashed-VOTER home nondeterministically (leader hides it, follower vends a dead `host:port`)
**`internal/broker/proxy.go:1032` (and `:410`), via `homeReachable` at `proxy_reconcile.go:217-226`**

`proxyStatusNodes`/`proxyStatusNodesCluster` gate `PublicHost`/`PublicPort` on `proxyHomeHealthy → homeReachable`, which reads in-memory `b.lastObserve`. `b.lastObserve` is written **only** inside `observeOnce` (`observability.go:141`), which early-returns on a non-leader (`:112`), and is a plain per-broker field, **not** raft-replicated. `.proxy.status.req` is a queue-group RODB read served by **any** broker (`clusterwrite.go:57-77` comment; `broker.go:852-853` `QueueSubscribe`), with no leader forwarding. So for a **crashed-but-VOTER** remote home (phase stays VOTER + cert + public_host in replicated `cluster_nodes`; only leader-only `lastObserve` distinguishes crashed from healthy): the **leader** returns `proxyHomeHealthy=false` → hides the exit (correct); a **follower** has empty `lastObserve` → `homeReachable` defaults true → **vends `PublicHost=b.example:port`, a dead pointer**.

**Failure scenario.** brk-b (VOTER, public_host set) crashes. A ctl runs `tether proxy status --cluster` twice; NATS routes the first to the leader (shows `-`) and the second to a follower (shows `b.example:14005`). In an N=3 cluster the wrong (follower) answer is the **more likely** one (~2/3). The operator sees flapping/contradictory exit reporting for an unchanged `(sid,nid)`, and the follower render is factually over-optimistic.

**Minimal fix.** Mirror the established `IsLeaderView` disclosure the sibling `--homes` path already uses (`homes.go:11,18`). (a) Add `IsLeaderView bool` (omitempty) to `proto.ProxyStatusResp` (`internal/proto/messages.go:1028`) — additive, ProtoVersion-2-safe. (b) In `handleProxyStatus` set `resp.IsLeaderView = b.cl == nil || (b.cl.node != nil && b.cl.node.IsLeader())`. (c) In the ctl proxy-status renderer, when `IsLeaderView==false` print a one-line "reachability best-effort (non-leader view)" note next to the exit column. This surfaces the leader-only caveat without changing the deliberate any-broker read routing. (Heavier alternative — forward `proxy.status.req` to the leader — is rejected: it contradicts the intentional design.)

**Suggested test.** In `internal/broker/g7_proxy_home_test.go` (same package, can set unexported `b.lastObserve`): seed a replicated `cluster_nodes` VOTER row for brk-b + a `__proxy__` allocation homed on brk-b; build two `Broker{selfID:"brk-a"}` sharing the DB — `brkLeader.lastObserve=[]peerObserve{{NodeID:"brk-b",Reachable:false}}` and `brkFollower.lastObserve=nil`. Assert `proxyStatusNodesCluster("lab")` returns `PublicHost==""` on the leader and `"b.example"` on the follower, documenting the skew; after the fix, assert `IsLeaderView` is false on the follower / true on the leader and the ctl note appears iff false.

### m2 · [G7b] socket `cluster status` never surfaces the runtime `jsUnavail` signal — only the older config-inferred banner
**`internal/broker/clusterstatus.go:330`**

The socket `StatusReport` JS-degraded banner still fires only on `forceSingle && natsconf…IsClusteredJetStream()` — a config inference for one specific cause. The runtime-measured `b.jsUnavail` (sustained 10008, leader-observed) is folded **only** into the remote `ClusterHealthResp` path, not the socket report — even though `StatusReport` already fetches every broker's `ClusterHealthResp` into its `health` map (`:236`) and stamps other fields from it. So a JS-503 whose cause is **not** force-single+clustered-conf (e.g. a genuine 3-voter meta that cannot form quorum, or a bad `reconcile nats` that left JS mis-clustered while the :7400 command raft stays healthy) reaches the thinner `--remote` laptop view but not the authoritative on-host operator view. This contradicts KD-5 ("与 G2 socket banner 协调统一，runtime 检测更广").

**Failure scenario.** A 3-node cluster loses JetStream meta quorum without force-single. The `--remote` user sees the loud "DATA-PLANE DEGRADED — JetStream UNAVAILABLE (503)" ALERT and is told to run `cluster status` on the broker host — but when the operator SSHes in and does exactly that over the socket, `forceSingle` is false so the config-inferred banner is skipped; they instead get the misleading "a node is mid-join / roster INCONSISTENT" banner (the socket is not literally clean — `streamActual` collapses to 0 so exit is DEGRADED — but the banner misdirects at membership, not JetStream). **The remote→socket redirect dead-ends for any non-force-single cause.**

**Minimal fix.** After the existing force-single banner block (mark it fired with a `jsBannerAdded` bool), add a runtime block that ORs `hr.JetStreamUnavailable` across the in-scope `health` map and appends a cause-agnostic "DATA-PLANE DEGRADED: a broker reports JetStream UNAVAILABLE (sustained 503) …" banner **iff `!jsBannerAdded`** (de-dup against the force-single text). Reuses the health poll already run; no `ClusterAdmin` field, no new wiring; exit-code taxonomy untouched (banner-only, matching the G2 pattern and inv-9).

**Suggested test.** Table test in `internal/broker` (same package, set the unexported `admin.healthPoll`), modeled on `TestG2DataPlaneDegradedBanner`: (1) `healthPoll` returns a reply with `JetStreamUnavailable=true`, forceSingle off → `rep.Banner` contains "JetStream UNAVAILABLE"; (2) all false → no JS text; (3) force-single + clustered-conf **and** `healthPoll` true → "JetStream" appears exactly once (guard suppresses the runtime append); (4) `rep.ExitCode` unchanged vs the no-signal run.

### m3 · [G7b] `ProxyHomeCount` has no "reported" discriminator — `--homes --remote` silently undercounts during a rolling upgrade
**`cmd/tether/cluster_status_nats.go:217` (`foldProxyHomeCounts`) + `renderHomesRemote` `:249-252`**

`foldProxyHomeCounts` reads `r.ProxyHomeCount` unconditionally (`seen[r.NodeID] = r.ProxyHomeCount`) and sums a `(total)` line. `clusterStatusHomesRemote` broadcasts on the **existing D8b `cluster-health.req` subject** that a pre-G7 broker *does* subscribe and answer — so the "old broker → ErrNoResponders → graceful" invariant does **not** cover this path: the pre-G7 broker actively replies, its JSON simply omits `proxy_home_count`, which decodes to **0** — indistinguishable from a broker that genuinely hosts 0 `__proxy__` homes. This repeats exactly the anti-pattern `TopoReported` (`internal/proto/alerts.go:61`) was added to avoid: a consumer must be able to tell "reports 0" from "does not report". `ProxyHomeCount`, being *summed* and existing precisely to judge distribution balance (#16/#18), is more susceptible yet carries no `ProxyHomeReported`.

**Failure scenario.** N=3 mid rolling-upgrade: two brokers on G7, one still pre-G7 hosting 5 ALLOCATED `__proxy__` homes. `tether cluster status --homes --remote` renders the pre-G7 broker as `0` and the `(total)` line is **5 short**. The operator reads the distribution as skewed / homes-lost and may trigger an unnecessary manual `cluster rebalance proxy` (or conclude homes vanished) — the count is merely unobservable, with no unknown-signal.

**Minimal fix.** Mirror `TopoReported`. (a) Add `ProxyHomeReported bool` (omitempty) to `ClusterHealthResp` (`internal/proto/alerts.go`); set it `true` in the health responder's `SelfID != ""` block (`cluster_health.go:97`) before the count query so a G7 broker always reports (even at 0). (b) In `foldProxyHomeCounts` capture `r.ProxyHomeReported` per node; in `renderHomesRemote` print `?` for unreported brokers, exclude them from `total`, and render the total as a lower bound (`>=N (one or more brokers pre-G7)`) when any is unreported; carry `reported` into the `--json` shape so a monitor can detect the unknown.

**Suggested test.** `cmd/tether/g7_remote_test.go`: `TestG7HomesRemotePreG7Unknown` — replies `[{NodeID:"brk-a", ProxyHomeCount:3, ProxyHomeReported:true}, {NodeID:"brk-b"}]` (brk-b omits both → 0/false). Assert brk-b is `!Reported` (not a genuine 0), render contains `?` on the brk-b line, contains no definitive `(total) 3`, flags `>=3`, and the `--json` brk-b object carries `"reported": false`.

### m4 · [G7b] JS-503 (#20③) ctl-fold surfacing into `cluster status --remote` has ZERO ctl-side test  *(merges two MINOR submissions)*
**`cmd/tether/cluster_status_nats.go:75-77` (`summarizeClusterHealth` OR-fold) + `:161-169` (`renderCtlStatus` "DATA-PLANE DEGRADED" banner) + `:48` (`--json jetstream_unavailable`)**

`grep JetStreamUnavailable`/`jetstream_unavailable` across all `*_test.go` = **zero hits**. `TestSummarizeClusterHealth` has no `wantJS` column and no reply sets the flag; the four `renderCtlStatus` test invocations all pass `JetStreamUnavailable==false`; `TestG7CtlExitCodeContract` enumerates {0,2,3} but never sets the flag. So the OR-fold, the banner text + remedy pointer, the `--json` field emission, and the invariant "**JS-down does NOT change the 0/2/3 exit taxonomy**" (it's a hint, not an exit class — plan inv-9) are all unpinned. The only "DATA-PLANE DEGRADED" coverage that exists (`g2_banner_test.go`) tests the structurally-distinct **socket** banner, so it does not transfer. This surfacing is the whole point of #20③ over the pre-existing G2 banner (making the rot laptop-visible), and it is the **only machine-detectable JS-503 signal available without SSH to a broker host**. Plan case #17 (`g7-plan.md`) mandated exactly this test.

**Failure scenario.** A cleanup drops/inverts the fold at `:75-77`, or neuters the banner guard at `:161`. Every existing test stays green, `cluster status --remote` silently stops reporting a wedged data plane, and a JS-503 outage is again invisible from a laptop — the precise multi-day silent-rot regression (racknerd incident) #20③ exists to prevent.

**Minimal fix.** None to product code — the fold/banner/json are correct at HEAD. Add the missing table test.

**Suggested test.** `cmd/tether/g7_remote_test.go` (add `encoding/json`, `strings` imports): `TestG7CtlJSUnavailableSurfacing` — (a) one broker among several with `JetStreamUnavailable:true` → `summarizeClusterHealth(...).JetStreamUnavailable==true` and `renderCtlStatus` text contains the banner + "JetStream UNAVAILABLE" + the `reconcile nats --to-standalone` remedy; (b) all-healthy → flag false, banner absent; (c) writable + JS-down → `ctlExitCode==0` (taxonomy unperturbed); (d) `--json` round-trips `jetstream_unavailable=true`. Verify it fails against the mutation (delete `:75-77` or set `:161` to `if false`).

### m5 · [G7b] flag mutual-exclusion matrix for `cluster status` is only partially pinned (plan #19 unimplemented)
**`cmd/tether/cluster.go:168-179`**

G7 removed `MarkFlagsMutuallyExclusive("homes","remote")` and now routes `homes && remote` to `clusterStatusHomesRemote`, keeping offline⊥remote (`:170`), offline⊥watch (`:173`), remote⊥watch (`:174`), homes⊥offline (`:176`), homes⊥watch (`:179`). The only flag-parse test (`TestClusterStatusRemoteOfflineMutuallyExclusive`, `cluster_status_nats_test.go:280`) exercises **only** `--offline --remote`. No test pins `--homes --remote` (must PARSE — the new feature's CLI entry point is entirely untested at parse level), nor any of the surviving exclusions or triples. Plan #19 (`g7-plan.md:123`) required the full matrix.

**Failure scenario.** During a refactor of the `MarkFlagsMutuallyExclusive` block, someone re-adds `homes⊥remote`; `cluster status --homes --remote` now hard-fails at cobra flag-group validation (before RunE), breaking the G7b #16 feature — and `make test` stays green. Symmetrically, dropping a surviving exclusion lets a nonsensical combo through untested. (Correction to the original submission: dropping `remote⊥watch` would *not* loop the member broadcast — RunE returns the one-shot `clusterStatusRemote` before the `if watch>0` branch, so `--watch` is silently ignored; the exclusion is a loud-fail-on-typo guard, not the loop guard. The coverage gap and the homes+remote re-forbid scenario stand.)

**Minimal fix.** None to product code. Add the parse-matrix table test.

**Suggested test.** `cmd/tether/cluster_status_nats_test.go` (`strings`/`bytes` already imported): `TestClusterStatusFlagMutex` — over each forbidden combo/triple, `newClusterStatusCmd(&sp).SetArgs(args).Execute()` must error with cobra's stable substring `"none of the others can be"`; a separate `allow_homes_remote` subtest asserts `--homes --remote` never yields that mutex substring (a runtime/no-session error is fine). Verify it fails-closed: re-add `homes⊥remote` → `allow_homes_remote` fails; delete `remote⊥watch` → that forbid subtest fails. Subsumes the existing `TestClusterStatusRemoteOfflineMutuallyExclusive`.

### m6 · [G7b] `cluster seeds show --remote` has no tests — the 0/69 data contract and force-single-exit-0 asymmetry are unpinned  *(merges the MINOR + NIT submissions)*
**`cmd/tether/cluster_seeds.go:117-146`** (nil/nil-Seeds → `unavailErr` at `:135-136`; 4-field render at `:138-144`)

`clusterSeedsShowRemote` is referenced only in its own file — no test. The nil/nil-Seeds → 69 branch and the deliberate divergence from the `status --remote` 0/2/3 taxonomy have no guard. The nil-Seeds case is a **reachable** state, not defensive-impossible: `internal/broker/cluster_manifest.go:84` populates seeds "may be nil until cluster seeds publish", so a broker with a signed roster but no published seeds returns `ClusterManifest{Roster:<signed>, Seeds:nil}` over the same G3 roster-pull subject; the `m.Seeds == nil` check is what turns that into a clean exit-69 instead of an `s.Generation` nil-deref. Plan case #20 (`g7-plan.md:124`) + KD-6 mandated this.

**Failure scenario.** (a) A refactor simplifies `:135` to `if m == nil` (assuming a non-nil manifest always carries seeds) → **nil-deref panic / exit-2 crash** on the real roster-but-no-seeds path. (b) `:136` is changed from `unavailErr(...)` to plain `fmt.Errorf(...)` → `classifyExit` falls to its default → **exit 70 instead of 69**; a monitor branching on "69 == no seeds published, retry" misreads it as a tether software fault (70) and aborts/pages. Both ship undetected.

**Minimal fix.** None to product logic; to make it unit-testable without live NATS, extract the render/branch tail into a pure `renderRemoteSeeds(out io.Writer, m *proto.ClusterManifest) error` helper (byte-identical output) and have `clusterSeedsShowRemote` delegate. The helper structurally never takes `force_single` as input and never returns exit 3 — that *is* the asymmetry pin.

**Suggested test.** New `cmd/tether/g7_seeds_remote_test.go` (table-driven; `classifyExit(unavailErr(...))==exitUnavailable==69`): a populated `SeedBundle` → 4 fields rendered, `err==nil` (exit 0, and by construction never a 0/2/3 health code); `nil manifest` and `&proto.ClusterManifest{}` (Seeds==nil, the reachable skew) → **must not panic**, `err != nil`, `classifyExit(err)==69` (never 70), and nothing printed.

### m7 · [G7a] `movableProxyAllocs` `__proxy__`-only guard (inv-5) has zero DB-level coverage
**`internal/broker/proxy_rebalance.go:225`** — the sole `WHERE pa.name = '__proxy__'` literal

`proxy_rebalance_test.go` tests only the pure `planProxyRebalance` planner over **pre-filtered** `[]*port.Allocation` slices (every element hard-coded `Name:"__proxy__"`); no test exercises `movableProxyAllocs` against a DB containing a regular (rebuild-OFF) named expose co-homed with a `__proxy__` row. This SQL literal is the **only** thing making auto-rebalance (#18) structurally incapable of moving a non-proxy home. The mover chain is break-before-make (`moveProxyHomeTo → PlanReassignHome` emits `proxy_node_unready` + re-pushes the PROXY Home directive so the agent re-establishes); a regular expose has **no proxy re-establish path**. Plan test #8 + inv-5 (`g7-plan.md:108`) mandated this assertion.

**Failure scenario.** A future edit broadens or reuses the `pa.name` filter (e.g. to serve exposes). Auto-rebalance then break-before-makes a rebuild-OFF regular expose that has nothing to re-establish it → **permanently black-holed pinned port** — and every current test passes because they operate on pre-filtered slices, never the SQL.

**Minimal fix.** None to product code. Add the DB-level guard test.

**Suggested test.** `TestMovableProxyAllocsProxyOnly` (in a G7 test file): seed one `__proxy__` and one **regular** expose co-homed in the SAME proxy-enabled ACTIVE session on the SAME eligible voter (set `sessions.proxy_enabled=1` and `nodes.proxy_ready=1` to strip every other filter so `pa.name` is the sole discriminator); assert `movableProxyAllocs(voterSet)` returns exactly 1 row and it is the `__proxy__` port. Passes today; fails the instant the filter broadens.

### m8 · [G7a] `recentProxyRehome` lexical timestamp boundary is untested and driver-coupled-fragile
**`internal/broker/proxy_auto_rebalance.go:145-156`**

The reaper-quiet gate does a **string** compare `last_rehome_at > '<LitTime(now.UTC()-window)>'`. Correct today (both sides UTC, monotonic stripped, `time.Time.String()` trailing-zero trim is order-preserving, `NOT NULL DEFAULT ''` makes never-rehomed rows compare false), but CLAUDE.md/`sqlbake.go:78-84` flag `LitTime` as modernc/driver-coupled and §13.2-fragile, and there is no boundary test. The project's own `TestLitTimeMatchesBoundParam` pins **byte-identity**, not **lexical ordering** — a `String()` reformat that preserves the round-trip but breaks ordering, or a future edit dropping `.UTC()` at the sole writer (`port/plan.go:292`) or reader, passes that gate and silently flips this one. The redundant `last_rehome_at IS NOT NULL` clause (column is `NOT NULL DEFAULT ''`) means never-rehomed correctness rests on the non-obvious `'' > cutoff` being lexically false; a cleanup of that clause flips the gate.

**Failure scenario.** The gate either never blocks (auto-rebalance fights the reaper mid-failover) or always blocks (auto-rebalance never fires). Both silent. Mitigated (why MINOR): auto-fire is default-OFF and the gate fails conservative on read error (→true→skip).

**Minimal fix.** None to product code (matches the existing `TestListAllocatedForOfflineNodes` precedent, which functionally pins the analogous baked-lexical-time boundary). Add a boundary test.

**Suggested test.** `internal/broker/g7_recent_rehome_test.go`: stamp a `__proxy__`/`ALLOCATED` row's `last_rehome_at` (via a bound param, which `TestLitTimeMatchesBoundParam` proves byte-identical to `LitTime(now.UTC())`) at now−30s (recent → `recentProxyRehome()==true`), now−60s (edge, strict `>` → false), now−90s (stale → false), and `''` (never → false).

### m9 · [G7a] return-edge vs orphan-clear separation (plan test 10) and idle-zero-writes (plan test 9) are untested; the `-race` sub-claim (test 11) is NOT actionable
**`internal/broker/observability.go:161-197`** (`returned` appended only in the current-voter decide loop `:161-174`; departed nodes cleared by the separate orphan-clear pass `:188-197`)

**Real gaps (confirmed absent):**
- **Test 10 (strongest):** no test drives `observeOnce`/`decideObservabilityAlerts` with a **DEPARTED** node (retired/force-singled, removed from the VOTER set) still holding a stale `broker_down` alert and asserts it does **not** become a `returned` edge. `returned` is appended only inside the loop over current `voters`; the departed node is cleared by the separate orphan-clear pass that never touches `returned`. This is load-bearing.
- **Test 9 (secondary):** no end-to-end assertion through the arm+driver that an already-balanced return produces 0 Propose / 0 `proxy_auto_rebalanced` — only indirectly implied by `rebalanceProxyHomes` returning `Planned==0` + the `rep.Planned>0` event guard.

**Failure scenario.** A refactor folds the orphan-clear into the decide loop (or lets a retired/force-singled node's `broker_down` clear flow into `returned`) → a **spurious auto-rebalance during a membership shrink** — the most fragile moment. Nothing pins this.

**Not actionable — flagged so the main process doesn't chase it:** the finding's `-race`-on-`autoRebalanceArm` angle (test 11) is a false alarm. `autoRebalanceArm.reset()` (`observability.go:258`) and `.tick()` (via `observeOnce`) are **both** invoked only from the single `runObserveLoop` goroutine, in the same `case <-t.C:` body. There is no concurrent access; a `-race` test would exercise an impossible scenario. The leadership-loss reset semantic is already pinned by `TestAutoRebalanceArmResetClearsPending`. Do **not** add a `-race` test on the arm.

**Minimal fix.** None to product code. Add tests 10 and 9.

**Suggested test.** `TestDecideObservabilityAlertsSkipsNonVoters` — `voters:=["leader","b"]` (node `z` left the roster); a response map where `b` answered fresh; assert `decideObservabilityAlerts` yields **no** decision for `z` (would leak into `returned`) and skips the leader, while `b` (a current voter that answered) yields the legitimate `broker_down` CLEAR (the only return-edge source). Test 9 is heavier (Broker+DB harness with homes evenly pre-spread): drive the arm past dwell with `TETHER_AUTO_REBALANCE=on`, assert `rebalanceProxyHomes(false).Planned==0` and no event.

### m10 · [G7a] render-equivalence (#2) is only asserted default-vs-`--cluster`, never against the actual `/sub` body
**`internal/broker/g7_proxy_home_test.go:125-152`**

`TestG7DefaultVsClusterViewHostAgree` compares `proxyStatusNodes` vs `proxyStatusNodesCluster`, but **both** project the host through the identical Go helper `proxyHomeHealthy` (`proxy_reconcile.go:200`) gating `clusternodes.LookupByNodeID(home).PublicHost` — so the assertion is largely tautological w.r.t. the health gate and can never observe a divergence between the Go gate and the `/sub` render. The plan's case #2 + inv-12 + KD-2 (`g7-plan.md:100,90,27`) require a **third arm**: the actual `/sub` body (`internal/subhttp/subhttp.go:190-206`, `liveProxyNodes` cluster branch), which uses an **independent** raw SQL predicate `cn.phase='VOTER' AND cn.public_host != ''`. That branch has **zero** test coverage, and the two implementations already differ observably (`proxyHomeHealthy` additionally requires `CertFP != ''` and, for a remote home, `homeReachable`).

**Failure scenario.** A later change adds a graceful-drain sub-phase to `HomeNode.Eligible()` so `proxyHomeHealthy` keeps vending during drain, but leaves the `/sub` SQL `cn.phase='VOTER'` untouched. A draining home renders `host:port` in **both** default and `--cluster` status (the 2-arm test passes because they agree), yet `/sub` excludes it → `proxy status` again advertises an exit the subscription won't serve — exactly the bug class #2 fixed.

**Minimal fix.** None to product code. Add the third (`/sub`) arm. Broker already imports `subhttp` and subhttp does not import broker (no cycle), so a broker-package test can drive `subhttp.Handler(Config{DB, ClusterMode:true})` directly, as existing subhttp tests do.

**Suggested test.** `TestG7DefaultVsClusterVsSubHostAgree` — same seeded scenario (exit homed on remote voter brk-b, brk-a answering, `/sub` cluster-branch agent/session gates satisfied); render the real `/sub` body via `httptest`; assert the host it egresses for the `/sub`-rendered `(sid,nid)` equals the default/`--cluster` host, does **not** leak the answering broker's host, and actually contains the node id (else the arm is vacuous). Add a negative arm: a home `proxyHomeHealthy` accepts but the `/sub` SQL excludes must be absent from `/sub`.

### m11 · [G7a] `TETHER_AUTO_REBALANCE` is DEFAULT-OFF behind an UNDOCUMENTED env var — gotcha #18's symptom persists in every default deploy, and the required doc deliverable is missing
**`internal/broker/proxy_auto_rebalance.go:104`**

`autoRebalanceEnabled()` returns false unless `TETHER_AUTO_REBALANCE=="on"`, and the driver returns immediately when disabled (`:112-114`), so the #18 mechanism is **inert every observe tick** in a default deploy. Default-OFF itself is plan-sanctioned (KD-3b / OQ-E: the mihomo/Clash proxy-provider refresh cadence is unverifiable → the conservative default is correct — **not a bug**). The real defect is documentation: `grep -rn TETHER_AUTO_REBALANCE docs/` returns **only** `docs/reviews/g7-plan.md` (a planning artifact). It is absent from `docs/cluster.md`, `docs/cluster-runbook.md`, `docs/broker-ops.md`, and the `docs/usage.md:213-218` env-var table. No CLI flag and (by design) no `broker.yaml` key exist, so there is **zero operator-discoverable surface**. The plan required this doc twice and unconditionally: W18-6 ("doc 记明 black-hole 曝露上界") and inv-11 ("曝露上界 = 单次迁移窗, doc 记明"). (Reject the original submission's "silent inversion" sub-claim: the plan's `=off` example was illustrative under an assumed default-ON branch; the `=on` opt-in is the correct consequence of the conservative default, not an error.)

**Failure scenario.** An operator upgrades expecting "auto-rebalance on broker return", a failed broker returns, and the `__proxy__` homes stay skewed onto the survivors indefinitely. The operator has no documented way to learn a hidden opt-in env flag gates the behavior, or to understand its break-before-make black-hole exposure bound. The gotcha is functionally not closed for the default install.

**Minimal fix.** Doc-only (do **not** change the default-OFF code). (1) `docs/usage.md` env-var table: add a `TETHER_AUTO_REBALANCE` row (broker-only; `on` enables #18 auto-rebalance of `__proxy__` homes on broker return; default OFF; read at broker start via systemd `Environment=`, not `broker.yaml`). (2) `docs/cluster-runbook.md` §1.1 (after the manual `cluster rebalance proxy` block): an "Automatic rebalance on broker return (opt-in)" note — default OFF; the gate set (quiet cluster, ~30s return-dwell, ≤once/~5min cooldown); and the **required black-hole exposure bound** (break-before-make; a client that cached the old `/sub` host black-holes until it refetches; bounded to a single migration window per cooldown).

**Suggested test.** A mechanical code↔doc drift guard: a hermetic Go test (in `internal/broker`, reading the repo docs via a relative path) asserting at least one operator-facing doc contains the literal `TETHER_AUTO_REBALANCE` — the same string `autoRebalanceEnabled()` reads via `os.Getenv` — failing if the docs omit it. The planned `NN-g7a-rebalance-return.sh` sim drill should additionally assert both branches end-to-end (env unset → distribution stays skewed after return; `=on` → converges to `max−min≤1` with no operator command).

---

## Test gaps (summary)

The batch's dominant weakness is **test coverage on live-wired code**, exactly the classes the plan enumerated and did not deliver. Cross-referenced to plan items:

| Plan item | What it pins | Status | Report ref |
|---|---|---|---|
| §3 #6, #7 | `#18` gate composition; DB convergence + exactly-one event | **absent** | M2 |
| §3 #8 / inv-5 | `movableProxyAllocs` `__proxy__`-only DB guard | **absent** | m7 |
| §3 #9 / inv-7 | idle-zero-writes on an already-balanced return | **absent** | m9 |
| §3 #10 / inv-4 | return-edge vs orphan-clear separation | **absent** | m9 |
| §3 #11 | leadership-flap under `-race` + NumGoroutine/fd leak gate | **partly N/A** — arm is single-goroutine; leak/flap wiring still owed | M2, m9 |
| §3 #2 / inv-12 / KD-2 | 3-arm render-equivalence incl. the real `/sub` body | **2 of 3 arms** | m10 |
| §3 #17 | JS-503 synthetic bool folds into `--remote` verdict/banner | **absent** (ctl side) | m4 |
| §3 #17 (socket) | runtime `jsUnavail` unified with the G2 socket banner | **absent** + code gap | m2 |
| §3 #19 | full `homes/remote/offline/watch` flag-mutex matrix | **1 of 6 pairs** | m5 |
| §3 #20 / KD-6 | `seeds show --remote` 0/69 + force-single-exit-0 | **absent** | m6 |
| W18-6 / inv-11 | env + black-hole-bound "doc 记明" | **absent** | m11 |
| — | e2e "hermetic proxy-rebalance-on-return" subtest | **absent** (`test/e2e` has no G7) | M2 |
| — | `NN-g7a-rebalance-return.sh` sim drill | **absent** (`test/simcluster/drills` has no g7a) | M2, m11 |
| — | `recentProxyRehome` lexical boundary (LitTime §13.2-fragile) | **absent** | m8 |
| — | ProxyHomeCount reported-discriminator (mixed-version) | **absent** | m3 |
| D8b matrix | cluster-health regression net | **fails to build** | B1 |

**Runtime code defects (not coverage):** B1 (compile break), M1 (JS-503 stuck-active), m2 (socket omits `jsUnavail`), m3 (undercount), m1 (proxy-status nondeterminism). Everything else is a missing regression guard on code that is correct at HEAD — but several guard load-bearing safety invariants (inv-5 black-hole, inv-6 no-stuck, KD-3b black-hole-bounded) and were explicitly promised by the finalized plan.

## Adjudication notes (narrowed / rejected sub-claims — do not chase)
- **M1 "60s flicker on every leader election mid-outage"** — real behavior but **by design** (KD-4 anti-false-alarm 60s threshold; failover-surviving persistent alerts row was deferred, OQ-B). Only the stuck-ACTIVE asymmetry (Issue 1) is the defect. The `oerr==nil && !rep.Observed` path the finding also cited is **unreachable** from the real `ObserveReplicas` (returns `Observed:true` on every non-error path).
- **m9 `-race` on `autoRebalanceArm`** — non-actionable; `reset()`/`tick()` share the single `runObserveLoop` goroutine, no concurrency exists.
- **m5 "remote+watch loops the member broadcast"** — incorrect; RunE returns the one-shot remote path before the watch branch. The exclusion is a typo-guard; the coverage gap and homes+remote re-forbid scenario are the valid parts.
- **m11 "plan wrote `=off`, code shipped `=on` — silent inversion"** — misread; the polarity flip is the correct consequence of the conservative default, not an error. Only the missing doc is the defect.
---

## 主进程采纳（Stage-C step5，2026-07-07）

对抗内审抓到 1 BLOCKER + 2 MAJOR（+11 MINOR）真 bug。逐条采纳：

**已修 + 验证**：
- **B1**（BLOCKER,d8 套件旧 5-arg `SubscribeClusterHealth` 编译不过——我改签名漏了 build-tag 后的测试 caller）→ 补 `nil,""` 两参 → `go vet -tags d8_integration ./test/d8/` 通过。
- **M1**（JS-503 非-10008 error 转变 stuck-ACTIVE,N=1 无 demotion 逃生）→ `alert_reconcile.go` 加对称 clear:非-10008 error 也 reset jsDownSince + `SetJSUnavailable(false)`（与 positive-observe clear 对称,inv-6 无 stuck-ACTIVE）。

**MINOR（11 条）**：待处理 polish（含专家新增测试建议整合）。

硬闸:full 回归绿 + d8 tag 编译。全程 additive、ProtoVersion 仍 2、无新 alerts.kind migration。
