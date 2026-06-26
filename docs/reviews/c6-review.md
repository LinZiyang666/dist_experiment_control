# C6 Stage-C Review — adjudication

> Workflow: 6 lenses → adversarial verify → synth (13 agents, Opus 4.8). 18 findings, 5 BLOCKER/MAJOR confirmed. Verdict CONDITIONAL.
>
> **CLOSURE STATUS (post-fix): all confirmed findings + all actioned MINORs CLOSED; `make test` + `make lint` green.**
>
> Main-process dispositions — ALL ACCEPTED (the review caught a real pre-existing C5 bug C6 amplified):
> - **ROOT CAUSE (HR1+HR2+HR3, one fix)** — `port.LookupProxyByNode` used the 10-col `scanOne` → dropped home_broker/epoch → reaper saw HomeBroker="" Epoch=0 → perpetually rehomed healthy proxies (HR3, LIVE since C5), epoch:0 events (HR2), `--homes`↔`proxy status` contradiction (HR1). FIX: repointed at the home-aware `scanOneWithHome` (13-col, mirrors LookupByName); removed the now-unused `scanOne`. Regression test `internal/port.TestLookupProxyByNodeReturnsHomeAndEpoch` pins it.
> - **HR2** — rehome events also now carry the authoritative post-Apply epoch (`PlanReassignHome`'s returned newEpoch, proxy + drain) + real from_broker.
> - **C6-EVT-4 (BD1 not actually closed)** — `emitProxyCountEvents` now DRIVES from `decideProxyEvents` (single source of truth) via a per-sid prev-counts cache; removed the divergent hand-rolled re-impl + the unused emitProxyEventOnce/clearProxyEvent.
> - **C6-EVT-1** — started/failed now share one per-(port,target) attempt mark → no per-tick flood on a sustained Propose failure.
> - **C6-EVT-3** — clearRehomeMark on M3 re-alloc + clearRehomeMark/clearProxyDwell/clearProxyCountEvents on the teardown pass (no leaked sync.Map entries).
> - **C6-EVT-5** — drain emits `started` only on a real attempt (after the skipped check) → no dangling started for a raced-away expose.
> - **C6-BE-02** — broker_down_rehome_summary gated on `aerr==nil` (no spurious fire on an ActiveAlerts read error).
> - **HR4** — HomeEntry.PublicURL gated on Ready (no dead-pointer URL for a not-ready/unreachable home).
> - **HR5** — `--homes` human table now shows NAME + PORT.
> - **C6-STATUS-1** — `--card` NOT-HA headline appends cardTopReason (keeps the genuine degradation reason at N<=2).
> - **REC-1** — recovery rejoin prepare Example cleared (no top-level-path Example).
> - Documented / accepted: C6-BE-03 (last_rehome_at needs 0017 cluster-wide before any OpPortReassignHome — the standing proto-v2 reinstall-not-upgrade migration model, D9 runbook), C6-BE-01 (empty-home reason latent/unreachable), C6-SECRETFREE-1 (Errors[] non-leaking today; guard is a tracked test follow-up), REC-2 (test-strengthen follow-up).

---

Confirmed all findings. Root cause empirically proven; reaper wired live; divergences and footer mitigation verified. Here is the consolidated report.

---

# C6 Stage-C Review — Expose/Rehome 可观测补全 + 状态命名对齐 + 恢复命令别名

## Verdict

**CONDITIONAL** — C6 is genuinely additive (one migration 0017, no proto/ACL/subject change; non-cluster brokers byte-stable), and the status-naming + recovery-alias work is sound. But the homes/rehome observability rests on a **single shared root-cause defect** (`port.LookupProxyByNode` silently drops `home_broker`/`epoch`) that produces three confirmed MAJORs: two self-contradicting operator views (`--homes` vs `proxy status --cluster`), epoch=0 in every proxy rehome event, and a live leader-reaper that perpetually rehomes healthy proxies. One fix resolves all three; nothing else blocks.

Empirically proven this session: a `port_allocations` proxy row with `home_broker='brk-HOME', epoch=7` returns `HomeBroker="" Epoch=0` from `LookupProxyByNode`. The reaper that depends on it is wired LIVE at `observability.go:244` (leader-gated, every observe tick).

## Confirmed BLOCKERs

None. (HR1 was filed BLOCKER but calibrated to MAJOR: impact is observability/status-only — the `/sub` data plane is leader-decoupled and unaffected; non-cluster brokers are gated out by `clusterMode && req.Cluster`.)

## Confirmed MAJORs

All three share ONE root cause and ONE fix.

**Root cause (shared by HR1/HR2/HR3).** `internal/port/port.go:620` `LookupProxyByNode` uses `scanOne` (`port.go:536`), whose 10-column SELECT (`port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at`) omits `home_broker`/`epoch`. The home-aware siblings (`scanOneWithHome`/`scanRowWithHome`, used by `LookupByName`/`LookupByTokenHash`/the ps + offline paths) scan 13 columns including those two. So `LookupProxyByNode(...).HomeBroker` is **always** `""` and `.Epoch` is **always** `0`. `port.go` is unchanged by C6 — this predates C6, but C6 newly *depends on* and *amplifies/surfaces* it.
**Fix direction (one change):** give `LookupProxyByNode` a home-aware scan — either repoint it at `scanOneWithHome` with the 13-column SELECT, or add a `LookupProxyByNodeWithHome` and use it in the three C6/reaper sites (`proxy_reconcile.go:126,283`, `proxy.go:407`). Once `existing.HomeBroker`/`.Epoch` are real, HR1/HR2/HR3 all close.

### HR1 — `--homes` and `proxy status --cluster` report contradictory readiness for the same proxy
- `internal/broker/homes.go:24,57,62` — `buildHomesReport` sources the proxy home from the JOIN `pa.home_broker` (the REAL replicated value) and feeds it to `b.proxyReadyFor(sid,nid,homeBroker)`.
- `internal/broker/proxy.go:407-409,420` — `proxyStatusNodesCluster` sources the home from `port.LookupProxyByNode(...).HomeBroker` (always `""`) and feeds THAT to the same `b.proxyReadyFor`.
- Scenario: a correctly-homed, ONLINE, `proxy_ready` agent. `--homes` → `proxyReadyFor(...,"brk-HOME")` → `proxyHomeHealthy` true → `HasHome=true` → `"ready"`. `proxy status --cluster` → `proxyReadyFor(...,"")` → home guard at `homes.go:85` FALSE → `HasHome=false` → `proxyReadyReason` returns `"no_home"` (and never sets `PublicHost/PublicPort`). The two operator commands directly contradict, and `proxy status --cluster` is now categorically wrong (always `no_home`) for **every** cluster proxy node — also a **regression**: pre-C6 the removed inline `else` set `HasHome=true,HomeCatchingUp=true` → `catching_up`. The BD12 "shared reason ⇒ render-equivalence" acceptance is unmet because the shared function receives different inputs.

### HR2 — every proxy rehome event carries `epoch: 0` (and `from_broker`/`home_broker` `""`), defeating BD3 epoch-correlation
- `internal/broker/proxy_reconcile.go:150,262,268` read `existing.Epoch` (always 0); `:286` reads `upd.Epoch` from a re-lookup specifically intended to report the post-rehome epoch (also 0).
- `PlanReassignHome`'s returned `newEpoch` IS available but discarded at `proxy_reconcile.go:271` (`_, cmd, perr :=`) and `clusterdrain.go:423`. The drain-side expose events (`clusterdrain.go:418,440`) omit `epoch` entirely (the `:439` comment even claims "post-Apply epoch = monotone +1", which is not carried).
- Scenario: drive a proxy rehome from epoch N to N+1; the emitted `home_reassign_succeeded` reports `epoch: 0` (and `from_broker:""`), so BD3's "cross-correlate on epoch, not timestamp" holds for NO rehome-event family.
- Fix: use `PlanReassignHome`'s returned `newEpoch` in the proxy + drain event payloads (and the home-aware lookup so `from_broker`/`home_broker` are real).

### HR3 — live leader reaper perpetually rehomes otherwise-HEALTHY proxies (epoch/last_rehome_at inflation + event spam)
- `internal/broker/proxy_reconcile.go:133` `if !b.proxyHomeHealthy(existing.HomeBroker)` — with `existing.HomeBroker==""` (root cause), `proxyHomeHealthy("")` returns false at `:199-201`, so the gate is TRUE on **every** tick for **every** existing proxy row, even one homed on a reachable VOTER. After `proxyRehomeDwell=3` ticks → `rehomeProxy` → `pickProxyRehomeTarget("")` (`:297`, `node_id != ''` excludes nothing) returns a real VOTER → `PlanReassignHome` reads the REAL epoch by port (`plan.go:272`) and bumps +1, restamping `last_rehome_at` inside the CAS UPDATE. Success clears dwell + mark, re-arming the cycle.
- Wired LIVE: `observability.go:244` `driveProxyReconcile` (leader-gated, every observe tick). Consequences: unbounded epoch inflation every ~3 ticks; `buildHomesReport`'s JOIN reads the inflated `pa.epoch`/`last_rehome_at` so `--homes` shows a healthy proxy as perpetually rehoming (Reconnects==epoch climbing, LastRehomeAt always fresh); C6's `home_reassign_started`/`succeeded` fire each cycle; a fresh `ProxyDirective`+`proxy_node_unready` is pushed to the agent each cycle (tunnel/keyset churn, worse across multiple voters as the home bounces). `readyNodes` can also stick at 0, misfiring BD1 `proxy_no_ready_nodes`.
- Pre-existing since D6 (the data plane still serves because each rehome picks a healthy VOTER), but C6 makes it actively visible/misleading and newly depends on these fields. Same one-line fix.

## MINORs

- **C6-EVT-1 — rehomeProxy `started`↔`failed` re-spam every ~5s tick on a sustained Propose failure.** `proxy_reconcile.go:266` (mark `"start:"+target`) and `:274` (mark `"fail:"+target`) share ONE per-port key (`b.rehomeEvt`, keyed `existing.Port`). On a Propose failure the function returns at `:278` without clearing the dwell, so the reaper re-enters every tick; the two different marks perpetually invalidate each other → `home_reassign_started`+`home_reassign_failed` flood `sys.events` every tick on a steady `store_error`/mid-Propose `not_leader`, violating the helper's idle-zero-writes contract. (The `no_eligible_target` stalled path at `:260` is a single steady mark and correctly self-suppresses.) **Fix:** on the failed branch, store the `started` mark too (or a single `"attempt:"+target` mark), or clear the dwell so re-entry is gated; only re-arm on a real success/recovery.

- **C6-STATUS-1 — `--card` NOT-HA headline drops the specific DEGRADED reason at N≤2.** `cmd/tether/cluster_status_card.go:74-76` early-returns the static `"NOT-HA (works; …)"` before the DEGRADED case that calls `cardTopReason`. Since `healthLabel` returns `NOT-HA` iff `Health==DEGRADED && voters<=2`, any genuine degradation at N≤2 (INCONSISTENT roster/raft, unreachable voter, stream below target, CATCHING_UP/DRAINING node) loses its headline reason (pre-C6 rendered `"DEGRADED: " + cardTopReason`). **Mitigated** (so MINOR): the card footer `:66` still prints `rep.Health` literally (`"DEGRADED (exit 1)"`), the node table still prints the INCONSISTENT/UNREACHABLE flags, and exit-code 1 is preserved — a salience demotion, not a hidden fault. The pre-C6 `computeHealth` banner at v≤2 never carried the specific reason either. **Fix direction (optional):** append `cardTopReason(rep)` to the NOT-HA headline when non-empty.

- **C6-EVT-3 — `rehomeEvt`/`proxyEvt` sync.Map entries leak; M3 stall re-emits each cycle.** `proxy_reconcile.go:148-154` (M3) emits on `existing.Port`, clears only the dwell, then `allocateAndPushProxy` mints a FRESH port (`findFreePort` runs while the old row is still ALLOCATED in-Plan), so the old port's `rehomeEvt` entry is never cleared (`clearRehomeMark` only on success/healthy at `:160/:281`). One leaked entry per M3 cycle on a stuck tokenless agent; same class on teardown/session-delete (no `clearRehomeMark`/`clearProxyEvent` there). Side effect: `rehome_stalled{target_not_ready}` re-emits every M3 cycle (fresh port, no prior mark) rather than idle-zero-writes as BD9 claims. **Fix:** clear the freed port's marks on M3 re-alloc and on teardown/session-rm.

- **C6-EVT-4 — BD1 "wire `decideProxyEvents` live" not actually closed; the re-implementation diverges.** `decideProxyEvents` (`proxy_cluster.go:100`) still has 0 production callers (only comments + tests); `emitProxyCountEvents` (`proxy_reconcile.go:171-190`) hand-rolls it. Divergence: `decideProxyEvents:106` emits `sub_render_empty` whenever `cur.ReadyNodes==0`, but `emitProxyCountEvents:178` gates both `sub_render_empty` and `proxy_no_ready_nodes` behind `CapableNodes>=1` (else clears) — an enabled session with 0 capable nodes rendering an empty `/sub` gets `sub_render_empty` from the pure fn but NOT from the wired path. **Fix:** drive the change-keyed wrapper from `decideProxyEvents(prev,cur)` (single source of truth) or reconcile the two and test the equivalence.

- **C6-EVT-5 — drain emits `home_reassign_started` before the Propose, so a raced-away (skipped) expose leaves a dangling `started`.** `clusterdrain.go:418` fires `started` before the `:422` Propose; BD8 correctly suppresses the false `succeeded`/`expose_rehomed` when `PlanReassignHome` returns `ErrNotFound` (`skipped=true` → `continue` at `:436`), but `started` already fired → unmatched lifecycle for a concurrently freed/revoked expose. **Fix:** emit `started` only after the skipped check.

- **C6-BE-02 — `broker_down_rehome_summary` can fire spuriously on an `ActiveAlerts` read error.** `observability.go:146-164`: `prevDown` is built only when `aerr==nil`, but the decide loop (with the `d.Kind==BrokerDown && d.Active && !prevDown[d.NodeID]` edge check) runs before the `if aerr!=nil { return }` guard at `:170`. A read error on a tick where a broker is already (not newly) down leaves `prevDown` empty → summary fires though it's not a false→true edge. Transient/self-correcting, cluster-only. **Fix:** skip the summary emit when `aerr!=nil`.

- **HR4 — `HomeEntry.PublicURL` populated for not-ready/unreachable homes, contradicting its doc + `proxy status`.** `homes.go:68-70` sets `e.PublicURL` whenever `r.publicHost!=""`, independent of readiness; a `tunnel_down`/`catching_up` home still shows a URL, contradicting the `adminsock/protocol.go` doc (`"" when home unknown/down`) and `proxy.go:410-417` (which only projects `PublicHost/PublicPort` when `proxyHomeHealthy`). **Fix:** gate `PublicURL` on `Ready` (or fix the struct comment).

- **HR5 — `--homes` human table omits NAME and PORT; row-set differs from `proxy status`.** `cmd/tether/cluster.go` `clusterStatusHomes` prints KIND/SID/NID/EPOCH/REASON/READY/HOME/PUBLIC_URL but not NAME/PORT, so a not-ready expose (PublicURL `-`) is unidentifiable though `HomeEntry` carries both (shown only in `--json`). Separately, `proxyStatusNodesCluster` iterates ALL `proxy_capable` nodes while `buildHomesReport` (`homes.go:28`) iterates only `state='ALLOCATED' AND name='__proxy__'`, so a capable node with no allocation never appears in `--homes`. **Fix:** add NAME/PORT columns; document the row-set scope in the runbook.

- **C6-BE-01 — `proxyReadyFor` changes empty-home reason `catching_up`→`no_home` (latent, currently unreachable).** `homes.go:79-92` only enters the home block when `homeBroker!=""`; an ALLOCATED `__proxy__` row with empty `home_broker` would now yield `no_home` instead of the old inline `catching_up`. Unreachable today (allocation invariant guarantees non-empty home), flagged so it can't silently surface. **Fix:** pin the intended reason with a unit test.

- **C6-SECRETFREE-1 — `buildHomesReport` `Errors[]` is the only raw-`err.Error()`→wire surface in C6 (currently non-leaking, no guard).** `homes.go:44,51,31` append raw DB error strings to `rep.Errors`. Not a leak today (SELECT lists only non-secret columns; worst case echoes `cert_fp`, a public fingerprint), but no guard locks the SELECT column-set or that `Errors[]` stays secret-free, so a future sensitive-column addition could echo into the wire. **Fix:** add a token-scan guard over the homes SELECT literal + a secret-substring assertion on `Errors[]`.

- **C6-BE-03 — `last_rehome_at` UPDATE unappliable on a follower that hasn't run migration 0017 (rolling-upgrade window).** `plan.go:292` bakes `last_rehome_at=...` into `OpPortReassignHome`; an older follower mid-rolling-restart would fail Apply with "no such column". This is the standing 0008–0016 migration model under the proto-v2 reinstall-not-upgrade policy (accepted constraint, not a C6 regression). **Fix:** D9 cutover runbook must apply 0017 cluster-wide before any node mints `OpPortReassignHome`.

- **REC-1 — `recovery rejoin prepare --help` Example points at the top-level `cluster recover` path.** `cmd/tether/cluster_recovery.go:54-57` overrides only `Use`/`Short` on the `newClusterRecoverCmd()` instance, inheriting the Example invoking `tether cluster recover …` (and the RunE success message references top-level `cluster init --from-existing`/`cluster add`). This is the GAP-2 the plan explicitly accepted (runbook canonical); the referenced command is valid + behavior-identical, so cosmetic only. **Fix (optional):** clear `prepare.Example` so cobra falls back to no Example.

- **REC-2 — `TestRecoverVsRecoveryNoPrefixAmbiguity` doesn't exercise cobra resolution.** `cmd/tether/cluster_recovery_test.go:49-61` only asserts two distinct non-nil `*cobra.Command`; it never invokes the resolver, so it wouldn't catch a future `cobra.EnablePrefixMatching=true`. The BD13 coexistence invariant has no live guard. **Fix:** strengthen to call `root.Find(...)` and assert `cobra.EnablePrefixMatching==false` + that a truncated prefix `"recove"` is not silently routed.

## Suggested tests (new — additive; do not modify implementation)

Root-cause / homes (HR1–HR3):
- `TestZZ`-style probe already proves the root cause: `port.LookupProxyByNode` on a row with `home_broker='brk-HOME',epoch=7` returns `HomeBroker="" Epoch=0`. Promote to a permanent `internal/port` regression test asserting `LookupProxyByNode` returns the real home/epoch (currently fails — pins the fix).
- `TestHomesProxyReasonMatchesProxyStatus` (HR1): seed a `__proxy__` alloc homed on a VOTER (`cert_fp`+`public_host`), node ONLINE+`proxy_ready=1`; assert `buildHomesReport`'s proxy `HomeEntry.ReadyReason == proxyStatusNodesCluster`'s `ReadyReason` for that `(sid,nid)` AND both `== "ready"`.
- `TestProxyRehomeEventCarriesRealEpoch` (HR2): drive `rehomeProxy` on a proxy at epoch N homed on an unhealthy broker with a healthy VOTER available; assert the `home_reassign_succeeded` event's `epoch == N+1` (and `from_broker` non-empty).
- `TestProxyReaperDoesNotRehomeHealthyHome` (HR3): seed a `__proxy__` row homed on a reachable VOTER, node ONLINE+`proxy_ready`; run `driveProxyReconcile` for `>proxyRehomeDwell` ticks; assert the row's `epoch`/`last_rehome_at` are UNCHANGED and no `home_reassign_*` event fired. Add `-race` + the NumGoroutine/fd leak gate (reaper touch).

Events (MINORs):
- `TestRehomeProxyFailedNoRespamAcrossTicks` (C6-EVT-1): leader, target available, Propose forced to `store_error` for the same `(port,target)`; call `rehomeProxy` on 3 consecutive ticks; assert `home_reassign_started` and `home_reassign_failed` each fire EXACTLY ONCE total while the failure is steady, and a later success re-arms a fresh `started`.
- `TestRehomeEvtNoLeakOnM3Realloc` + `TestProxyEvtClearedOnTeardown` (C6-EVT-3): force M3 twice; assert `b.rehomeEvt` holds at most the current port; assert teardown/session-delete leaves no `rehomeEvt`/`proxyEvt` entries for the removed sid/port.
- `TestEmitProxyCountEventsMatchesDecideProxyEvents` (C6-EVT-4): table-drive both over identical `proxyEventCounts` incl `{Enabled:true,CapableNodes:0,ReadyNodes:0}`; assert kinds equal (currently fails on `sub_render_empty`).
- `TestMigrateExposesSkipNoDanglingStarted` (C6-EVT-5): make `PlanReassignHome` return `ErrNotFound` for a rebuild-ON expose; assert no `home_reassign_started` without a matching terminal.
- `TestBrokerDownSummarySuppressedOnAlertsReadError` (C6-BE-02): stub `cluster.ActiveAlerts` to error while a `broker_down` is already active and `decideObservabilityAlerts` yields that node Active; assert NO `broker_down_rehome_summary`.

Status naming (the plan enumerated these but none landed — regression-protection gap on the byte-stability contract):
- `TestStatusReportHealthLabelWired` (delete `clusterstatus.go:319` → a test fails), `TestJSONSchemaStableHealthLabel` (golden `health`/`exit_code`/`schema_version` unchanged across the 5 states), `TestOfflineStatusHealthLabel` (FORCE-SINGLE/exit3, READ-ONLY/exit2, clean→`""`/exit0 at `cluster.go:274,312`), `TestRenderShowsHyphenatedLabelWithFallback` (`HealthLabel=""` → legacy `Health` in headline at `cluster.go:399-401`), `TestExitCodeContractUnchangedAcross5States`.
- `TestCardNotHAStillSurfacesGenuineReason` (C6-STATUS-1): `ClusterStatusReport{Health:"DEGRADED",HealthLabel:"NOT-HA",ExitCode:1,Nodes:[{NodeID:"brk-b",Inconsistent:true}]}` → assert the card output names `brk-b`/INCONSISTENT (today only in the node table). The existing `TestStatusCardDegradedShowsTopReasonAndIncidentHint` leaves `HealthLabel` unset, so this path is untested.

Homes secrets / table (MINORs):
- `TestHomesReportErrorsSecretFree` (C6-SECRETFREE-1): force a scan error; assert `Partial==true` + `Errors[]` contains no secret substring; pair with a static token-scan guard over the homes SELECT literal.
- `TestHomesPublicURLEmptyWhenHomeDown` (HR4) and `TestHomesTableShowsNameAndPort` (HR5).

Recovery (MINORs):
- Strengthen `TestRecoverVsRecoveryNoPrefixAmbiguity` (REC-2) to use `root.Find(...)` + assert `cobra.EnablePrefixMatching==false` and that `root.Find(["recove"])` is not silently routed.
- `TestRecoveryRejoinPrepareExampleNotTopLevelPath` (REC-1): assert `prepare.Example` does not contain `"cluster recover"` (or is cleared).

Relevant files: `internal/port/port.go:536,558,620` (root cause), `internal/broker/{homes.go,proxy.go,proxy_reconcile.go,rehome_events.go,observability.go,clusterdrain.go,clusterstatus.go,clusteradmin.go,clusterwrite.go}`, `internal/port/plan.go:261-292`, `cmd/tether/{cluster.go,cluster_status_card.go,cluster_recovery.go,cluster_recovery_test.go}`, `internal/storage/migrations/0017_port_alloc_last_rehome.sql`.