# C1 Plan (FINAL) — Agent online auto-discovery: consume the signed broker roster

> Stage-A output. Drafted by a 9-agent adversarial workflow (4 lens drafters → 4 critics → 1 synthesizer; `critique:wire` stalled mid-stream but its DRAFT lens fed the synth and the other 3 critics + the synth's own code-verification covered the wire concerns). **The main process is the sole finalizer**; this file is the authoritative C1 plan. Verified against the tree at commit `f1a2bde` / tag `v0.4.0`.

## 0. Finalization decisions (main process — resolves §9 open questions)

| # | Open question | DECISION | Rationale |
|---|---|---|---|
| 1 | L3 live-failover redial (session-loop refactor + watchdog) — include or defer? | **INCLUDE** | Restart-bound failover would NOT meet success metric #2 in spirit; deferring + claiming convergence = lip-service the overseer will flag. L3 is isolated behind the `session()`+watchdog seam. |
| 2 | Monotone `roster_generation` counter (FSM-touching) — in scope? | **INCLUDE** | Only resolution where the consumer's strict "reject lower generation" is BOTH sound anti-rollback AND wedge-free after retire/recover/force-single. Add the bump to `PlanClusterCertRotate` too (CertFP is a signed roster field). |
| 3 | Client-URL: consumer templating vs signed field | **Consumer-side port-templating (D-3)**, no wire/canonical/schema change; `cfg.NATSURL` seed is a permanent floor; document wss-vhost + proxydial caveats. Signed `NatsClientURL` deferred to C2 if heterogeneous client ports become real. |
| 4 | account_pub pin bootstrap | **TOFU over the auth_callout-authenticated channel + optional OOB `Config.AccountPub` override** (authoritative, disables TOFU). Document the one-time first-connect residual (parity with today's trust, not a regression); C2's signed invite pre-seeds the pin. |
| 5 | Gossip trust | **Document residual; NO `IgnoreOldServers` in C1** (security-pragmatic v1; auth_callout still gates the nkey CONNECT; the watchdog rebuild re-bases on the signed set). Defer tls-pinning to C2. |
| 6 | Refresh / grace numbers | `RosterRefreshInterval` default **3min** (full-jittered); `rosterStaleGrace` **6min**. Both configurable. |

**Verification obligations carried into Stage-B** (do these first, fix the plan if any is false): (a) `genericExecApplier` applies ALL `Statement`s of a command in one Apply txn; (b) the `MAX(existing+1, leaderNow.UnixNano())` floor needs no migration (seeds in the same magnitude as the shipped derived-max → no mixed-version wedge); (c) `parseStateBytes` uses plain `json.Unmarshal` (no `DisallowUnknownFields`) so downgrade ignores the new `roster` key.

## Facts verified against the tree (settle the cross-draft disputes)

- **Drain DOES stamp `cluster_nodes.phase = DRAINING`.** `clusterdrain.go:139` `setPhase(nodeID, phaseDraining, []string{phaseVoter})` → `OpClusterNodePhase`; `:147` → `RETIRING`. `readRosterBrokers` (`cluster_roster.go:91`) projects `phase` verbatim into the signed roster. → **C1's draining work is consumer-only**; the producer-side "fold `DrainingNodes` + override Phase" idea is redundant and DROPPED. (`cluster_meta draining:<node>` stays the *alert* source only.)
- **Refresh-via-full-`register` IS fleet-scale raft amplification.** `registerNode` (`clusterwrite.go:509-525`) unconditionally `proposeOrForward(VerbNodeRegister,"",…)` (no 0011 dedup) + emits `agent_registered` (`broker.go:1043`) + `reconcileOnRegister` audit writes (`:1057`). → refresh MUST be a raft-free roster-only path (D-4).
- **`buildConnOptions` runs ONCE** (`Run:488`→`connectNATS:654`, before first register); nats.go `MaxReconnects(-1)` reuses the BOOT pool, never re-calls `buildConnOptions`. → a mid-session-learned roster does NOT enter the live pool via `nats.Servers`; live failover needs an explicit close+reconnect (D-1 L3).
- **`handleRegister` is leader-only** (`broker.go:963` `isClusterFollower()` early-return) → refresh path + stale event are leader-gated and single-mode byte-equivalent for free.
- **The monotone-counter pattern exists**; `NewCommand(op, body ...Statement)` is variadic (`command.go:201`); `cluster_meta` UPSERT-with-CAST-guard (`auditcursor.go:29-32`, `PlanClusterDrainSet:328-330`) + `readMetaUintDB:38` are the exact templates.

---

## 1. Objective + closes

**建议3 acceptance (verbatim):** *"加一台 broker 后，在线 agent 在 5 分钟内自动刷新 roster；离线 agent 不无限期阻塞 retire，受明确 TTL/阈值策略约束；manifest 签名失败、generation 倒退、server identity 不匹配时拒绝更新。"* Closes **success metric #2** (加 broker 后无需改任何 agent 配置) and **#7** (自动分发有 generation/签名/审计 —— 消费端接上).

| Gap row (proposals-gap) | C1 mechanism | Outcome |
|---|---|---|
| 在线 agent 自动刷新 roster / 加 broker 5min 内刷新 🟡 | §2 refresh ticker (≤3min) + dial-from-fresh-pool + stuck-reconnect rebuild | ✅ |
| 消费者验签/generation/identity 真正生效 🟡 | §3 `adoptRoster` → `VerifyAt(pin)` + strict-monotone on a truly-monotone generation | ✅ |
| sig 失败 / generation 倒退拒绝 ❌→✅ | §3 rows 3/4/7, tested at the agent, grep-guarded | ✅ |
| retire 标 draining + agent 停止偏好 🟡 | drain already stamps `Phase=DRAINING`; §2 selector de-prefers it (last tier) | ✅ |
| 离线 agent 不无限阻塞 retire 🟡 | broker-side TTL only; agent is pull-only, adds no retire barrier (§6) | ✅ |
| agent_roster_stale 事件 (建议5, 1/8) ❌ | §6 leader-gated `pubSysEvent` | ✅ |

---

## 2. Mechanism design

### D-1 — Relearn mechanism: persist-and-dial-from-fresh-pool + bounded stuck-reconnect session-rebuild

Three composing layers; **healthy agents never churn**:

- **(L1) Dial from the persisted set.** Every `connectNATS` (boot/restart/rebuild) builds the pool from `effectiveDialURLs()` = freshest persisted roster client-URLs ∪ `cfg.NATSURL` seed (deduped; **seed is a permanent non-removable floor**). Add `nats.DontRandomize()` so VOTER-first/draining-last order survives; shuffle WITHIN the VOTER tier (selector) to avoid fleet load concentration.
- **(L2) Periodic refresh** keeps the persisted set + cached generation current → satisfies the literal "refresh roster ≤5min".
- **(L3) Stuck-reconnect session-rebuild = live failover.** persist+next-reconnect is inert mid-session (verified); so we redial ONLY when nats.go is stuck-disconnected (its boot pool is dead — exactly when a newly-added broker rescues), giving a healthy agent ZERO churn. Gossip rejected as relearn source (`connect_urls` unauthenticated + NAT-internal).

**L3 = session-loop rebuild (not a parallel partial redial):**
- Refactor `Run`'s body (connect→subscribe→register→reconcile→replay→proxy→heartbeatLoop) into `func (a *Agent) session(ctx) (rebuild bool, err error)`; `Run` loops `for { rebuild, err := a.session(ctx); if !rebuild || err != nil { return err } }`.
- **Single conn-lifecycle owner:** the watchdog sets `a.rebuilding.Store(true)` (→ `onNATSReconnect` no-ops, so nats.go's own late reconnect can't re-subscribe on the dying conn), then `nc.Close()` (Close does NOT fire `ReconnectHandler`), then signals `session` to return `rebuild=true`. `session`'s existing defers (`subFwd/subEvict.Unsubscribe`, `proxyHandlerWG.Wait`, `nc.Drain`, `cancelFailClosed`) tear the OLD conn down BEFORE the next iteration subscribes → **never two live `…forwarded` subscriptions** (no double-dispatch). Next iteration clears `rebuilding`.
- Rebuild re-enters the whole session → reproduces the entire `onNATSReconnect` side-effect set (`cancelFailClosed`, `ncBox.Store`, re-subscribe forwarded+evict, register→`applyReconciliation`→`applyProxyDirective`, proxy re-ACK) by construction. Composes with D6 (`register→applyHomeDirectives` is epoch-ordered/idempotent).
- Watchdog timer = single-arm (`if timer != nil { return }`, like `armFailClosed`), `Stop()` on any successful (re)connect/rebuild, exits on `runCtx` → passes NumGoroutine+fd leak gate.
- **Draining does NOT force a redial of a healthy conn** (req #4 = "stops preferring NEW connections" → satisfied by the next reconnect/rebuild excluding draining via selector ordering). Live TCP breaks naturally (建议4-consistent).

### D-2 — Generation model: add a truly-monotone `roster_generation` counter; consumer strict-rejects lower gen

The shipped derived generation `max(added_at, phase_changed_at)` REGRESSES on retire-of-newest / recover / force-single. Therefore:
- "strict reject + seed floor + TTL" alone silently never converges after recover (up to TTL preferring a dead broker) → fails req #4 online → rejected.
- "consumer regression-escape (accept sig-valid lower gen)" reopens the rollback attack (必须拒绝 #3) → rejected.
- **monotone counter** makes strict reject both sound anti-rollback AND wedge-free → **adopted**.

Mechanism: `cluster_meta` key `roster_generation`, bumped in each broker membership/phase op via a shared all-literal statement (variadic `NewCommand`), wall-clock-floored for rolling-upgrade safety:
```sql
INSERT INTO cluster_meta(key,value) VALUES('roster_generation', <leaderNowUnixNano>)
ON CONFLICT(key) DO UPDATE
  SET value = MAX(CAST(cluster_meta.value AS INTEGER)+1, <leaderNowUnixNano>)
```
- Strictly monotone (≥ existing+1) → never regresses on prune/recover/force-single (standalone counter, snapshot+replay restored).
- Rolling-upgrade safe: first value ≈ `now.UnixNano()` (same magnitude as the old derived-max) → an agent that cached a v0.4.0 derived value never sees a lower value from an upgraded broker.
- Deterministic across replicas (leader bakes the `UnixNano()` literal, like cert-rotate's `LitTime`) → rides `genericExecApplier`.
- `buildSignedRoster` reads it via new raft-free `cluster.RosterGeneration(ro)` (mirror `AuditPublishedIndex`/`readMetaUintDB`) INSTEAD of the derived max.
- `readRosterBrokers` KEEPS computing the derived-max timestamp, returned SEPARATELY as the broker-local "last membership change wall-clock" → the §6 stale-event grace anchor (decoupled from the wire generation).
- Bump in **broker** membership ops only (`PlanClusterNodeUpsert`, `PlanClusterNodePhase`, `PlanClusterNodeRemove`, `PlanClusterCertRotate`), **NOT** `node.PlanRegister` (agent register ≠ topology change) → `agentGen < counter` cleanly measures topology lag.

### D-3 — Client-dial URL: consumer-side port-templating (no producer/wire/canonical/schema change)

`Select` returns `NatsRoute` = route port `:6222`, un-dialable by a client through auth_callout. The consumer builds `<scheme>://<public_host>:<port><path>` from the scheme+port+PATH of `cfg.NATSURL`, skipping empty/loopback `public_host`. Reject the signed `RosterBroker.NatsClientURL` field for C1: appending a 7th field to `CanonicalRosterBytes` changes the signed bytes even when empty → breaks signature verification against shipped v0.4.0 6-field-canon brokers on rolling upgrade (would force `ClusterRosterSchemaVersion`→2 + version-conditioned canonicalization). `cfg.NATSURL` seed is a permanent floor → an empty/loopback/unreachable `public_host` can never strand the agent.

### D-4 — Refresh transport: `NodeRegisterReq.RosterRefreshOnly bool` (additive/omitempty) → roster-only RODB read

The refresh ticker sends a register req with `RosterRefreshOnly=true`. `handleRegister` short-circuits AFTER the nid/proto/session-active gates but BEFORE `registerNode`(`:1018`), `agent_registered`(`:1043`), `reconcileOnRegister`(`:1057`): reply `resp.Roster = b.rosterForRegister(req)` only, return. → zero raft write, zero event, zero reconcile (pure RODB read). The ticker calls **`adoptRoster(resp.Roster)` only — never `applyReconciliation`** (no D6/proxy churn per interval). Boot+reconnect still funnel the roster through `applyReconciliation→adoptRoster`.

**Cadence:** `Config.RosterRefreshInterval` (0 ⇒ default 3min, full-jittered via `jitterDur`). On a failed/empty tick (e.g. `CodeLeaderUnavailable` during failover) retry on a SHORT bounded backoff (not a full interval) so success-after-one-failure stays <5min. Single goroutine in `session`, `select{ticker, ctx.Done}`, exits on `runCtx`. Single-flight: skip a tick while `reconnectInFlight`/rebuild is set (their own register already refreshed the roster).

### D-5 — account_pub pin: TOFU + optional OOB override

No pin exists today. Pin `r.AccountPub` from the first sig-valid roster, persist first-write-wins (never auto-repin), enforce `account==pin` thereafter. `Config.AccountPub` OOB override is authoritative and disables TOFU. **Load-bearing:** `adoptRoster` passes the PERSISTED pin to `VerifyAt`, **never `r.AccountPub`** — tested + grep-guarded.

### D-6 — Gossip: documented residual, no `IgnoreOldServers` in C1

Signed roster is authoritative on every (re)connect/rebuild; gossiped `connect_urls` are unauthenticated but (a) parity with pre-C1, (b) auth_callout still gates the nkey CONNECT, (c) the watchdog rebuild re-bases on the signed set. Document; defer `IgnoreOldServers` + tls-pinning to C2.

### D-7 — Draining policy: last-resort tail (de-prefer), uppercase enum, consumer-only

Selector 3-tier: **VOTER** (intra-tier shuffled) → **others** (`CATCHING_UP`, `JOIN_VERIFIED_PENDING_VOTER`) → **`DRAINING`/`RETIRING`/`VOTER_ADD_FAILED` last** (kept as last-resort, not dropped → "停止偏好" not "drop"; an all-draining roster still yields URLs). Uppercase enum strings; add `proto.RosterPhase*` SSOT consts. No producer change. Accepted transient: the drain marker is raised one Propose before the phase flip (`clusterwrite.go:476-484`) — sub-second window where phase is still VOTER; documented, not engineered around.

---

## 3. Verify/adopt decision table — enforced on the CONSUMER (`adoptRoster`)

`now=a.cfg.Now()`; `pin=persisted pin (or OOB)`; `hwm=persisted cached generation`. Every REJECT keeps prior good set + pin + hwm (never wedge, never empty the pool).

| # | Condition | Result | Action |
|---|---|---|---|
| 1 | `resp.Roster == nil` | — | NO-OP (non-cluster/single → byte-equiv) |
| 2 | `pin==""` (first) ∧ `VerifyAt(r, r.AccountPub, now)==nil` | OK | ADOPT-TOFU: `pin=r.AccountPub`, `hwm=r.Generation`, relearn, persist; log `roster_pinned` |
| 3 | sig invalid | error | REJECT `{reason=sig}` |
| 4 | `r.AccountPub != pin` | mismatch | REJECT `{reason=account_mismatch}` — pin passed, never `r.AccountPub` |
| 5 | `r.SchemaVersion > current` | schema | REJECT `{reason=schema}` |
| 6 | `r.ExpiresAt` passed `now` | expired | REJECT for adoption, keep prior URLs; do NOT regress hwm/pin |
| 7 | VerifyAt ok ∧ `r.Generation < hwm` | rollback | REJECT `{reason=gen_rollback}` (sound: gen now truly monotone) |
| 8 | VerifyAt ok ∧ `r.Generation == hwm` | idempotent | ACCEPT-IDEMPOTENT: refresh URL set, hwm unchanged |
| 9 | VerifyAt ok ∧ `r.Generation > hwm` | advance | ADOPT: `hwm=r.Generation`, relearn (D-7 tiers), persist |
| E | post-relearn usable set empty | floor | KEEP prior + seed (never install empty pool) |

---

## 4. File-level change list

**`internal/agent/agent.go`** — struct: `rosterMu sync.Mutex`, `pinAccount string`, `rosterGen uint64`, `rosterURLs []string` (in-memory mirror, loaded once at boot via the bounded reader), `rebuilding atomic.Bool`, watchdog timer fields. `Config`: `RosterRefreshInterval time.Duration`, `AccountPub string`, `Now func() time.Time`. `Run`(`:487`): refactor body into `session(ctx)(rebuild bool,err error)`; loop. `connectNATS`(`:654`): dial `effectiveDialURLs()` comma-joined. `buildConnOptions`(`:1274`): `nats.DontRandomize()`; `ReconnectHandler` no-op while `rebuilding`; `DisconnectErrHandler` also arms the watchdog (single-arm). `register`(`:733-748`): add `RosterGen: a.cachedRosterGen()`; thread `RosterRefreshOnly`. `applyReconciliation`(after `applyHomeDirectives`): `a.adoptRoster(resp.Roster)`. New: `adoptRoster`, `effectiveDialURLs`, `rosterRefreshLoop`, `cachedRosterGen`, `pinnedAccountPub`, `requestRebuild`, `armRedialWatchdog`/`stopRedialWatchdog`.

**`internal/agent/proxy.go`**: `onNATSReconnect`(`:353`) early-return when `a.rebuilding.Load()`.

**`internal/agent/state.go`**: `StateFile` += `Roster *RosterCache json:"roster,omitempty"`; `RosterCache{PinAccountPub, Generation, Roster *proto.ClusterRoster}`; `SetRosterCache`/`GetRosterCache`/`PinAccountPub` (mirror `SetProxy`/`GetProxy`).

**`internal/clusterroster/roster.go`**: new `DialURLs(r, templateURL) ([]string, error)` (client-templated, 3-tier, intra-VOTER shuffle, skip empty/loopback). Keep `Select`/`Verify`/`VerifyAt`.

**`internal/proto/cluster_roster.go`**: SSOT consts `RosterPhaseVoter/Draining/Retiring/AddFailed`. No struct/wire change.

**`internal/proto/messages.go`**: `NodeRegisterReq` += `RosterRefreshOnly bool json:"roster_refresh_only,omitempty"` (additive; `RosterGen` already present).

**`internal/broker/broker.go`** `handleRegister`(`:957`): after session-active gate, `if req.RosterRefreshOnly { reply roster-only; return }`; after `resp.Roster = b.rosterForRegister(req)`(`:1071`) call `b.maybeEmitRosterStale(sid,nid,req.RosterGen,resp.Roster,lastChange)`.

**`internal/broker/cluster_roster.go`**: `buildSignedRoster` reads `cluster.RosterGeneration(RODB())`; `readRosterBrokers` additionally returns the derived-max wall-clock. New `internal/broker/roster_stale.go` (predicate + grace const + bounded dedup + `pubSysEvent`).

**`internal/cluster/`**: new `RosterGeneration(ro)`; shared `rosterGenBumpStmt(now)`; appended in `PlanClusterNodeUpsert`, `PlanClusterNodePhase`, `PlanClusterNodeRemove`, `PlanClusterCertRotate`.

## 5. `state.json` schema + compat

```go
type StateFile struct {
    PortTokens []PortToken  `json:"port_tokens"`
    Proxy      *ProxyState  `json:"proxy,omitempty"`
    Roster     *RosterCache `json:"roster,omitempty"` // C1
}
type RosterCache struct {
    PinAccountPub string               `json:"pin_account_pub"`
    Generation    uint64               `json:"generation"`
    Roster        *proto.ClusterRoster `json:"roster,omitempty"`
}
```
Additive omitempty. Pre-C1 file → nil → first roster TOFU-pins. Boot: re-run `VerifyAt(cache.Roster, pin, now)`; pass → use cached URLs; expired → drop URLs (seed-only), keep pin+hwm (consumer-side offline-TTL bound; first reconnect re-register corrects the cache → stale window = one dial attempt, seed floor always present).

## 6. `agent_roster_stale` event

`pubSysEvent("agent_roster_stale", …)` on `sys.events` — NOT a D8 alert (no durable raise/clear; per-(sid,nid) would balloon `alerts`; not in 0009 enum; would be a raft write per stale register). Self-resolves (converged agent's next register `agentGen==gen` → silent).
- **Predicate (decoupled from generation value):** emit iff `r!=nil` (cluster, leader) ∧ `agentGen>0` ∧ `agentGen<genCounter` ∧ `now - lastMembershipChangeWallClock > rosterStaleGrace`. Grace anchored on the broker-local derived-max wall-clock, NEVER `time.Unix(0, generation)`.
- **Grace:** `rosterStaleGrace = 6*time.Minute` (5min SLA + 1min; > 3min jittered refresh so a converging agent never trips it).
- **Dedupe:** bounded leader-local `map[sidNid]uint64` (last-warned gen), cap ~4096 oldest-eviction. No leadership-reset hook (none exists; grace is the primary throttle).
- **Scrub:** keys ⊆ `{v,type,ts,sid,nid,agent_gen,current_gen}`. No token/PSK/cert_fp/nats_route.
- **Gating:** leader-only for free; single mode `selfID==""` → nil roster → helper returns immediately → no event, byte-equiv.

## 7. Byte-equivalence proof obligation + guards

Non-cluster byte-identical; ProtoVersion stays 2; `ClusterRosterSchemaVersion` stays 1. New wire = `RosterRefreshOnly`(omitempty) + existing `RosterGen` — additive.
- Req: `rosterGen==0 ∧ RosterRefreshOnly==false` → both omitted → byte-identical.
- Resp: `selfID==""` → `resp.Roster==nil` → key omitted → byte-identical; `adoptRoster(nil)` no-op → no `state.json` roster key; no event.
- Guards: extend `internal/broker/b7_roster_test.go` (`TestRosterForRegisterInertWhenSingleBroker`, `TestRosterRespMarshalOmitsKeyWhenNil`, `TestRosterGenerationNeverSuppressesAfterRecover` — **must still pass with the counter**, `TestBuildSignedRosterStampsTTL`). Add: req-omit-when-zero; single-mode roster-only request emits no event + no raft write; pre-C1 `state.json` round-trips with no `roster` key.

## 8. Adversarial test list (table-driven; `-race`+leak gate where noted)

**`internal/agent/roster_adopt_test.go`** (drive `adoptRoster`, fake `Now`): `TestAdoptRejectSigFail`, `TestAdoptRejectAccountMismatch` (+ grep-guard call site never passes `r.AccountPub`), `TestAdoptRejectGenRollback` (5→reject4→accept6), `TestAdoptRejectExpired`, `TestAdoptRejectSchema`, `TestAdoptTOFUFirstWins` (+ OOB wins), `TestAdoptMalformed`, `TestAdoptNilNoOp`, `TestAdoptEmptySetFloor` (T5).

**`internal/clusterroster/dialurls_test.go`**: `TestDialURLsClientPortNotRoute`, `TestDialURLsThreeTierShuffle`.

**`internal/agent/roster_runtime_test.go`** (embedded `nats-server/v2/test`, `-race`+leak gate): `TestRefreshConvergesWithin5Min` (incl. one injected `CodeLeaderUnavailable`), `TestRefreshNoRaftNoEvent`, `TestReconnectStormSingleFlight`, `TestStuckReconnectRebuildOneSub` (original-broker-returns-as-watchdog-fires → EXACTLY ONE live `…forwarded` sub + proxy not torn), `TestRebuildReestablishesAll`, `TestOfflineTTLBound`, `TestNonClusterByteEquiv`.

**`internal/broker/roster_stale_test.go`** + **`internal/cluster/roster_gen_test.go`**: `TestRosterStalePredicate` (incl. small-ordinal-vs-counter anchored on wall-clock), `TestRosterStaleLeaderOnlyScrubbedDedup` (`-race`), `TestRosterGenMonotoneNeverRegressesOnPrune` (retire max-stamp → still advances; recover/force-single preserves; rolling-upgrade first value ≥ derived-max), `TestRetireNotBlockedByStaleAgent` (reuse a D7 harness).

Dev scope: `go test ./internal/agent/ ./internal/broker/ ./internal/clusterroster/ ./internal/cluster/` + `-race` on runtime/stale/gen tests. C1 e2e enters the matrix at the D9 backfill.

## 9. Disposition of every critique BLOCKER/MAJOR

All folded except two REJECTED on false premise / unsafe (see "Facts verified"): broker-event "drain doesn't stamp phase" (false — verified it does) and its consumer regression-escape (reopens rollback — use the counter instead). Full mapping retained in the Stage-A workflow output (`tasks/w6cr8jzhf.output`, synth §"BLOCKER/MAJOR disposition"): refresh-amplification→D-4; strict-monotone-wedge→D-2; redial side-effects + double-sub→D-1 session-rebuild; `nats.Servers` inert→D-1 L3; client-URL→D-3; stale-predicate gen-coupling→§6 wall-clock anchor; pass-pinned-account→D-5+grep-guard; gossip→D-6; ordering→`DontRandomize`+intra-tier shuffle; all MINORs folded.
