# D8 plan — synth 轨迹（多专家对抗 workflow 输出，未定稿）

> 这是 Stage A workflow（4 drafter / 4 critic / 1 synth、全 Opus 4.8）的 **synth 候选**原文，留作审计轨迹。
> **定稿 = `d8-plan.md`**（主进程亲手核验承重事实 + 逐条裁决后）。本文件不作实现尺。

---

Critical finding for the banner/health ACL: members can already **Sub** to `tether.v2.sys.events` (line 119) and `tether.v2.ctrl.version.announce` (line 108) — both are session-independent broadcast subjects members already hold. This gives a clean, already-permitted path for the banner that avoids touching `cluster.*` ACL. But for the **health-probe REQUEST** (client-synth gating), members need a *pub* grant for a request subject — they have `ctrl.by.<actor>.session.list.req` etc. but no session-independent cluster-health request.

I now have all the ground truth I need. Let me write the synthesized candidate plan as my final response.

---

# CANDIDATE D8 PLAN (synthesizer draft for main-process finalization → `docs/reviews/d8-plan.md`)

Verified against live code; every disputed-fact correction below is grounded in a re-read (anchors cited inline). Critic BLOCKERs are adopted with the corrected approach and labeled `[ADOPTED: <critic-id>]`. Genuine uncertainty is surfaced as a numbered OQ in §9, never papered over.

---

## 1. Scope & layer model

D8 is **two parallel leaves**, but the critiques show they are NOT independent: `gate.go` (D8b client-synth gating) must wrap `push`/`pull` (D8a verbs), and the orphan-reaper (D8a) + retire-gate-enumeration (D8a) interact. **Decision: D8a lands first; D8b's `gate.go` is a pure ctl-side helper that D8a's push/pull call via a one-line hook agreed up front.** [resolves Critique 1 D4-MAJOR-6 / Critique 4 cross-cutting]

Three-layer discipline, identical to D5–D7:

| Layer | D8a | D8b |
|---|---|---|
| **(A) build-and-prove, NOT cutover** (harness-only, `cluster.Node` required) | home-routing gate, `VerbTransferAudit` forward, `OpTransferAudit` op + publisher replay, clustered `ensureXferBucket(ReplicasFor(nVoters))`, retire-gate xfer enumeration, home-gated orphan GC | `OpAlertRaise/Clear/Ack` ops + appliers, leader alert-reconcile loop, `VerbAlertSignal`/`VerbAlertAck` forwards, broker-side `SubscribeClusterHealth`/`SubscribeAlertLs` responders, disk-bridge forward |
| **(B) genuinely-LIVE** (ships, but inert at N=1 by construction) | **none** — all transfer-distribution inert until D9 | **ctl-side** `gateDestructive` + `withBanner` helpers (live in `cmd/tether`, but at N=1 no broker answers the probe → no-gate/no-banner); **disk.go falling-edge re-arm** stays as-is, new clear-signal is `alertSink`-gated (nil in prod) |
| **(C) inert-until-D9** (serve.go wiring) | `serve.go` byte-unchanged; prod `Broker` constructs no `cluster.Node`; `transferAuditSink`/`alertSink` nil; `xferTargetReplicas → ReplicasSingle` | same |

**Production invariant (the crisp guarantee):** At N=1 today, zero behavior change. No transfer audit forwards (sink nil → byte-identical `pubAuditTransfer`). No alert is ever written (no `cluster.Node` → alert loop never runs). ctl's gate/banner helpers detect "this is not a cluster" (probe → `ErrNoResponders`, see OQ7 resolution) and do nothing — **but they fail CLOSED on a known-cluster deployment when corroboration is impossible** (OQ7).

---

## 2. D8a transfer

### 2.1 Home routing (OQ1) — broadcast-SUB + home-keyed gate, NOT §4.1 forward for the data path

**Confirmed root fact** [Critique 1/2/3/4 all agree, verified `broker.go` plain `nc.Subscribe`]: `push.req`/`pull.req`/`push-commit.req`/`ev.transfer.*`/`finalize.req` are **plain subscriptions** → in a clustered NATS every broker receives every copy → at cutover, N brokers each run the handler (fan-out: N buckets ensured, N tracker entries, N forwards-to-agent, N audit writes). This is the most important D8a correctness issue.

**Mechanism:** a build-and-prove `transferHomeGate(sid, nid) (proceed bool, transient string)` inserted at the top of each handler body, **after** the existing `transferGate` (node-online/actor-member check), **before** any bucket/tracker work:
- `selfID == ""` (production) → `(true, "")` **unconditionally** — production byte-path identical. [the inert seam]
- home resolved == self → `(true, "")` (I am home, proceed).
- home resolved != self → `(false, "")` (silent return, no reply; the home handles it).
- home unresolved / ineligible → `(false, transient)` (see error-writer rule below).

Node→home reuses the **expose-independent** `resolveHomeForAgent(sid, nid)` (`home.go:63`, reads `nodes.nats_server` → `clusternodes.LookupByNatsServer` → eligible VOTER). A node with zero exposes still has a home via the D6 server-id bridge.

**§9 doc-first reconciliation** [ADOPTED: Critique 3 B1 — this is a doc-first BLOCKER]: §9 currently says "`push.req/pull.req` 经 §4.1 转发". The chosen design does **NOT** forward the data path via §4.1 (the broadcast SUB already delivers to home; an extra hop is waste). §9 line ~305 **must be rewritten** to: "数据面 push/pull 经 broadcast-SUB + home-keyed gate（home==self 才处理，余静默）；**仅 transfer audit 行经 §4.1 leader Apply**" + a §16 deviation entry. **The gate cannot ship while §9 says "经 §4.1 转发".**

**Home-unresolved single-error-writer** [ADOPTED: Critique 1 D1-BLOCKER-2, Critique 2 OQ-D, Critique 3 B2 — Draft 1's "agent's current server replies" is self-contradictory: the unresolved case is *defined by* a missing binding, so no broker can match on it → zero replies → ctl hangs]. **Resolution: adopt the D6 `home_catching_up` pattern — every non-home broker stays SILENT, and ctl times out → retries with backoff.** No broker emits a transient error on the unresolved/ineligible path. The retry eventually hits a resolved home (binding converges, rehome settles). This needs **no consensus-free single-writer claim and no `ConnectedServerName()` dependency**. The only cost is ctl-side timeout-then-retry on a genuinely-unresolvable target — acceptable (matches `transferGate`'s existing `node_offline` retry posture, and ctl already treats transfer reply timeouts as best-effort). **REJECTED**: Draft 1's "agent's current server" rule (unsound) and "leader replies on unresolved" (works but couples error path to leader; the silent-retry path is simpler and consensus-free).

**finalize.req (no nid in subject)** [verified `ParseTransferFinalize` → `(actor, sid, transferID)`, NO nid]: route by **tracker presence** — only the broker holding the tracker entry replies; `entry==nil` → silent return (no reply), ctl times out (it already ignores the finalize reply, `_ = sendFinalize`). **But fence by home-epoch** [ADOPTED: Critique 1 D1-MAJOR-3 — concurrent rehome can make old-home-A finalize/delete an object the agent now drives through new-home-B]: the finalize must carry/check the D6 home epoch; a finalize for a stale epoch is dropped, not actioned. **Do NOT bake the "finalize-times-out" behavior into a test as the expected contract** [ADOPTED: Critique 1 D1-MAJOR-2 / Critique 4 MAJOR-2] — the test asserts "client restart succeeds," not "finalize must time out."

### 2.2 Transfer audit re-derivability + idempotency (OQ2) — `OpTransferAudit`, **reqID-ledger anchored** (NOT JS-window-only)

**`OpTransferAudit`** = a per-event, **pure-Aux, empty-Body** op (there is no persistent audit DB row — arch line 131). Each lifecycle event (start/complete/failed) from each of the **six** `pubAuditTransfer` call sites [verified: 331, 473, 580, 705, 724, 961 — Critique 3 m1, Drafts undercounted as five] becomes **one** committed `OpTransferAudit` entry. The publisher replays it purely from Aux, like `publishReconcile`.

**Idempotency — the central BLOCKER correction** [ADOPTED: Critique 1 D2-BLOCKER-1, Critique 2 OQ-C, Critique 4 MAJOR-4]: Draft 2's "no reqID, JS dedup absorbs raft duplicates" is **UNSAFE** — the JS `Duplicates` window (`AuditDedupWindow`) is **finite**, and a post-election replay sweep or a delayed forwarder retry can re-publish a duplicate entry **outside** the window → two `complete` rows. JS dedup is a second line of defense, never the primary anchor across elections.

**Resolution:** `OpTransferAudit` **MUST carry a derived-hex reqID** `reqID = hex(sha256("xferaudit:"||transfer_id||":"||kind))[:64]` routed through the migration-0011 `cluster_reqid_ledger`. This suppresses the **second commit** (the only window-independent fix), exactly as D4 reconcile does. The ULID-vs-`<=64 lowercase hex` clash [verified `validReqID` node.go:342] is solved by the hex-of-sha256 derivation — `transfer_id` rides in Aux, the derived hex rides in `cmd.ReqID`.

**Verified the ledger works with empty Body** [resolves Critique 1 D2-BLOCKER-2]: `fsm.go:234-241` writes `insertReqIDTx` **after** the applier returns, gated only on `cmd.ReqID != ""` — independent of whether the applier ran any SQL. So a first commit registers the reqID; a retry hits the `appliedDedup` path (`fsm.go:191`), skips SQL, advances `applied_index`. The empty-Body `OpTransferAudit` via `genericExecApplier` (0 statements, deterministic no-op) is sound.

**Publisher replay** [ADOPTED: Critique 1/4 — the #1 false-green: `PublishOnce` default `advanced = idx` at audit_publisher.go:239 silently drops unknown ops]: add `case cmd.Op == cluster.OpTransferAudit` to the `PublishOnce` switch (next to `OpReconcileBatch`), publishing to `proto.SubjAuditTransfer(rec.Session)` with msg-id `q<reqID>:xfer:0` (the existing `auditMsgID` reqID-bearing form). **Mandatory anti-false-green test with a vacuity control** (a variant where the case is absent asserts the row is silently dropped — proving the test exercises the replay path), driven against the **real** `PublishOnce` loop control-flow, not a fabricated `cmd` [Critique 4 MAJOR-5].

**Contradictory complete/failed** [ADOPTED: Critique 2/3 M1 — a watchdog `failed` racing a real `complete` are different kinds → different reqIDs → both commit]: `claimFinalize` (in-memory, lost on home death) gates the live path; the reqID ledger bounds per-(transfer,kind) commits but does NOT prevent one `complete` + one `failed`. **Resolution: leader-Apply terminal-state guard** — the `OpTransferAudit` Plan reads the leader DB's last terminal kind for this transfer_id; **first-terminal-wins** (a `failed` after a committed `complete` is a Plan no-op). This makes the committed log contradiction-free (no §6.3 re-derivability hole). If main prefers not to add the read, the fallback is a **§16-registered "benign last-published-wins race"** — OQ below.

**`start` latency** [Critique 1 D1-MAJOR-4 / Critique 3 m5]: route `start` through leader Apply for audit-pair integrity (a re-derivable `complete` with no `start` is an integrity hole), but make the `start` forward **async/non-blocking** — do not block the agent-forward on its commit. §9 prose says all three "经 leader Apply"; keep that, add "(start 异步、不阻塞 agent-forward)". → **OQ9-A**.

### 2.3 Tier-B replicas + retire gate (OQ3)

**Already built in D5** [verified]: `XferReplicaState` (transfer.go:266), `raiseXferReplicas` (raise-only), `ReconcileOnce`/`reconcileSessions` fold `OBJ_xfer-<sid>` into `AllAtTarget` via the `XferState` hook (audit_publisher.go:455-458). D8a finishes the wiring:

1. **Flip live call sites off `ReplicasSingle`** via an inert seam: `b.xferTargetReplicas()` = `ReplicasSingle` when `b.node == nil` (production, byte-identical), else `ReplicasFor(NumVoters())`. Replace `jsstream.ReplicasSingle` at transfer.go:439, 539.

2. **Retire-gate enumeration MUST read JetStream's actual `OBJ_xfer-*` stream list, NOT the DB `ListSIDs`** [ADOPTED: Critique 1 D2-BLOCKER-3, Critique 2 OQ-E, Critique 4 BLOCKER-2 — `ListSIDs` has no production provider (all `staticSIDs` in test/d5), and a bucket can outlive its session row ("sticks around until session removed", transfer.go:182) → a purged-session orphan is invisible → retire false-greens on a 1-replica object → data loss]. The retire gate's replica enumeration uses `js.ListStreams` filtered `OBJ_xfer-*` (the boot reconciler already does this, transfer_reconcile.go:32) — the only fail-closed source. The DB `ListSIDs` stays acceptable for the **raise** pass (a missed bucket just delays a raise, not loses data).

3. **Split read-only `ObserveReplicas()` from raising `ReconcileOnce()`** [ADOPTED: Critique 1 D2-MAJOR-4, Critique 2 OQ-MAJOR, Critique 4 MAJOR-6 — `ReconcileOnce` RAISES as a side effect; using it as the retire-gate `streamsReady` probe means a drain readiness check MUTATES topology and can mask a stuck reconfig]. The retire gate consumes the **read-only** `ObserveReplicas()`; the background loop keeps the raising `ReconcileOnce()`.

### 2.4 In-flight best-effort + orphan GC (OQ4)

**Best-effort confirmed**: tracker + watchdog stay home-local; home death loses in-flight; rehome does not preserve in-flight; the **completed tier-B object survives** because it lives in the JS-quorum'd `OBJ_xfer-<sid>` stream (R=`ReplicasFor(3)`=3), not the broker.

**Boot orphan reaper** [ADOPTED corrected: Critique 4 BLOCKER-1 — the hazard is OBJECT-level, not stream-level (verified: `reconcileXferObjectsOnBoot` calls `store.Delete(obj.Name)`, never `DeleteStream`; the v0.2.2 rewrite removed stream deletion). The real bug: broker B's boot reaper sees broker A's live in-flight object in the **shared replicated bucket** as orphan (B's tracker is empty) → deletes A's live object.] **Resolution: home-ownership filter, NOT blanket disable** [ADOPTED: Critique 1 D1-MAJOR-2, Critique 2 OQ-F, Critique 3 M5 — disabling entirely leaks 8 GiB buckets forever and wedges the retire gate]: in clustered mode the object reaper only reaps objects for sessions whose **home is self** (`resolveHomeForAgent(...)==selfID`) AND whose transfer is terminal per committed `OpTransferAudit` state. A permanently-dead-home's orphans are reaped by the new home once assigned (or a leader-driven GC keyed on committed-terminal transfer state). Production (`selfID==""`) keeps today's behavior byte-identical.

---

## 3. D8b alerts

### 3.1 Store ops (OQ5) — three FSM ops on `genericExecApplier`, no new migration

Migration **0009 already ships** `alerts`/`alert_acks` with the exact CHECK enumerating the store-backed set [verified]; `quorum_lost`/`force_single_active` deliberately absent (client-synth). **No schema change.** Three ops, all leader-Plan, all-literal SQL, `genericExecApplier`:

- `OpAlertRaise` → `INSERT INTO alerts(...) SELECT <lits> WHERE NOT EXISTS (SELECT 1 FROM alerts WHERE dedup_key=<lit> AND state='ACTIVE')` — no-op-on-conflict (avoids the unique-index Apply error that would fork).
- `OpAlertClear` → `UPDATE alerts SET state='CLEARED', cleared_at=<lit> WHERE dedup_key=<lit> AND state='ACTIVE'`.
- `OpAlertAck` → `INSERT ... ON CONFLICT(dedup_key) DO UPDATE` (last-writer-wins, idempotent).

**Determinism proof re-grounded** [ADOPTED: Critique 1 D3-MAJOR/BLOCKER-1, Critique 2 — the SQL is safe but Draft 3's stated reason ("no error on any replica") is wrong and invites a forking "optimization"]: correctness rests on **strictly-ordered Apply + committed-state predicate** — every replica applies entries in committed order and evaluates `WHERE NOT EXISTS(ACTIVE)` against identical committed state → identical `RowsAffected` verdict. The §16 registry must state this proof explicitly.

### 3.2 Raise/clear ownership — leader-only level-triggered pass, **NOT folded into the audit-publisher tick**

[ADOPTED BLOCKER: Critique 1 D3-BLOCKER-2, Critique 3 B4, Critique 4 BLOCKER-3/BLOCKER-4]. Draft 3's "fold into `AuditPublisher.tick`" is **rejected** for three confirmed reasons:
1. **Liveness inversion** — `PublishOnce` can `return` early on an un-ACKed publish (audit_publisher.go) → alert raising wedges exactly when the cluster is degraded (the alert that says "degraded" can't fire).
2. **Audit-durability starvation** — an alert `node.Propose` blocking under applyMu would stall the audit-publish drain (the §2.3 "近似 0 丢失" mechanism).
3. **Idle-zero-writes broken** — a `WHERE NOT EXISTS` no-op is **still a committed raft entry**; a level-triggered pass re-proposing every tick re-breaks D5's F1 idle-zero-writes invariant.

**Resolution: a separate, constant-count, leader-gated goroutine** (`alertReconcile.Run`, its own `IsLeader() && !LeaderContactStale` gate, its own ctx-child — trivially leak-gate-clean, same shape as the publisher loop). It Proposes `OpAlertRaise/Clear` **only on a genuine state transition** (diff desired-vs-current ACTIVE rows; no unconditional re-propose). It may *read* a cached `ReplicaReport` for `replication_degraded` but issues no JS writes.

**Per-kind ownership + the clear-condition fix** [ADOPTED: Critique 2 OQ-G — clear must be `Observed && AllAtTarget`, never `!Degraded()` (which is true on `Observed=false` → false-clear/flap on a transient JS meta-not-ready blip; verified `Degraded()=Observed && len>0 && !AllAtTarget`)]:

| kind | sev | raise | clear (positive-observation only) |
|---|---|---|---|
| `replication_degraded` | severe | `report.Degraded()` | `report.Observed && report.AllAtTarget()` |
| `below_quorum` | info | serving-set F==0 (one projection, shared with `broker_down`) | F>=1 |
| `broker_draining` | info | `cluster_meta draining:<node>` present | key cleared |
| `broker_down` | info | roster contact-loss (same projection as below_quorum, [Critique 2 D3-MAJOR-5]) | contact restored / retired |
| `disk_pressure` | info | follower-forwarded edge (§3.3) | follower-forwarded clear (§3.3) |
| `raft_lag` | info | **DEFERRED to D9, writerless** (§3.4) | — |
| `manual` | info | operator | operator |

### 3.3 disk_pressure bridge — **level-triggered re-assert**, and a NEW clear-edge in disk.go

[ADOPTED BLOCKER: Critique 3 B3 / Critique 4 BLOCKER-4 — verified `disk.go` has NO falling-edge emission (recovery only sets `emitted=false`); Draft 3's "clear on dip-below" hooks nothing → disk_pressure would raise but never auto-clear]. Two changes:
1. **disk.go gains a clear-edge** `alertSink`-gated signal (the `else` re-arm branch also calls `b.alertSink.SignalDisk(active=false)` when transitioning from emitted→recovered). `pubSysEvent` raise call stays byte-unchanged; the new signal is `alertSink != nil`-gated (nil in production). The guard scans `disk.go` and allows the inert `if b.alertSink != nil` *read* while banning `b.alertSink =`/`alertSink:` *writes*.
2. **Level-triggered forward to avoid a stranded-ACTIVE on a lost clear** [ADOPTED: Critique 1 D3-MAJOR-4]: rather than rely on a single edge message surviving, the follower re-asserts its current disk state to the leader **every tick** (idempotent raise/clear on the leader). dedup_key = `disk_pressure:<node_id>`. This self-heals a dropped clear (the lone edge-triggered-forwarded kind otherwise has no reconciler). Forward verb `VerbAlertSignal` over the existing §4.1 forwarder (one ACL surface).

### 3.4 raft_lag — **DEFERRED to D9** [ADOPTED: Critique 1/2/3/4 unanimous]

Draft 3's "leader-self audit-publish-lag" is a **category error** (it measures JS-publisher backlog, not follower replication lag) and **operationally misleading** (operator sees `raft_lag`, suspects a follower, but the cause is leader-local JS). The leader genuinely cannot read follower applied-lag in D8 (that's the D9 follower-cursor transport, verified clusterstatus.go comment). **Keep `raft_lag` in the 0009 CHECK catalog writerless**, exactly as D5 left `replication_degraded`, with a §16 registration "per-follower raft_lag writer deferred to D9". The "不静默推后" directive is satisfied by the explicit registration.

### 3.5 Ack (OQ6) — cluster-level, display-only, permanent; re-appearance is pure client-side

`OpAlertAck` writes `alert_acks(dedup_key PK, acked_by, acked_at)`. Single cluster-level ack (not per-identity); `acked_by` = ctl nkey, **display-only**. Leader bakes `acked_at` (the follower/ctl must NOT — non-determinism). Ack via `VerbAlertAck` forward (no reqID needed — `ON CONFLICT DO UPDATE` is idempotent). Ack **suppresses only the inline ack-prompt, NOT the banner**.

**"Severe re-appears each new session"** [ADOPTED: Critique 2/3/4 m-resolved — consistent with §18.3 deleting `session_nonce`]: NO stored per-session state, NO client session-nonce. The ack row is permanent; the banner **always** renders severe ACTIVE alerts on every ctl invocation; ack only flips the inline-prompt bit. On ack, ctl prints "will re-appear next session". `alert ls` shows `acked_by` + when (LEFT JOIN, filter `state='ACTIVE'` — a regression test pins that no reader GROUP-BYs across CLEARED history).

### 3.6 Client-synth gating (OQ7) — VerifyLeader-confirmed, fail-closed on known-cluster, advisory framing

**Confirmed: no NATS cluster-health RPC exists today** [verified clusterstatus.go is adminsock-local]. D8b adds a **new minimal read-only NATS RPC** `cluster.health` answered by every broker (broadcast, no queue-group), returning `{writable_leader_confirmed bool, leader_id string, force_single_active bool, schema_version int}`.

**The leadership signal MUST be VerifyLeader-confirmed, not bare `State()==Leader`** [ADOPTED BLOCKER: Critique 4 BLOCKER-6 — a partitioned ex-leader within its `LeaderLeaseTimeout` still reports `State()==Leader` (verified read.go: leader is "stale=false" by lease), so a "any reply claims leadership → no gate" rule fails to fire precisely in the data-loss window]. The health responder answers `writable_leader_confirmed` only after a `VerifyLeaderRead`-style barrier (verified `VerifyLeaderRead` exists, read.go:66) — a partitioned ex-leader fails `VerifyLeader`. Followers answer `writable_leader_confirmed=false` + their known `leader_id` (for banner text only).

**Gating rule** [ADOPTED corrected: Critique 3 B3, Critique 4 BLOCKER-6 — drop the broken "leader_id must be empty" conjunct]:
- `quorum_lost` gate fires iff: **≥1 reply arrived** AND **no reply has `writable_leader_confirmed=true`**. (A quorum-confirmed leader anywhere → not quorum-lost. No confirmed leader among all reachable brokers → gate.)
- `force_single_active` gate fires iff **≥1 reply** reports it true (a strictly-local persisted D7 fact; reading it, not synthesizing).
- **Zero replies** → see boundary OQ below.

**The authoritative protection is server-side write rejection; client-synth is advisory UX** [ADOPTED: Critique 1 OQ6/Critique 4 — even VerifyLeader has the inherent lease window]. §10.4 prose must state: the ctl gate is a best-effort pre-check; the real safety is the broker rejecting a write it cannot quorum-serve (it fails loudly). This must not be oversold.

**Pure client jitter must NOT gate**: a single lagging follower replying `writable_leader_confirmed=false` does NOT gate as long as another reply confirms a writable leader.

### 3.7 Banner (OQ8) — client-assembled, cheap queue-group for alerts, broadcast only for destructive corroboration

[ADOPTED: Critique 1 D4-MAJOR-4/6, Critique 4 MAJOR-11 — Draft 4's "two broadcast RPCs on every ps/ls" is a thundering-herd + fixed-latency-floor tax]. **Split the two needs:**
- **Banner alert set** (for reads ps/ls and writes): one **queue-group** `alert.ls` request answered by **any one broker** via bounded-stale read (alerts are replicated — any broker can serve them). Cheap, one round-trip, best-effort (timeout → render command output anyway, skip banner).
- **Destructive corroboration** (writes only): the **broadcast** `cluster.health` probe (rare, latency acceptable).

Banner is **client-assembled, rendered to stderr** (stdout stays script-parseable; a test asserts `ps` stdout is byte-identical with/without active alerts). No per-Resp-struct `Banner` field [ADOPTED: Critique 2/3/4 — the prompt's hint is rejected; verified no `Banner` on `PsResp`, adding it to ~12 structs = wire churn + per-command leader read]. **`--json` commands suppress the stderr banner** [Critique 4 MAJOR — `cluster status --json` already carries banner in JSON].

**`below_quorum` at N=2 must NOT banner-spam** [ADOPTED joint: Critique 3 M-cross, Critique 4 MAJOR-8 — a standing `below_quorum` INFO on every ps/ls from everyone is alert-fatigue that voids the severe-gate's value]: **the always-on banner renders SEVERE only**; INFO kinds (`below_quorum`, `broker_draining`, etc.) live in `alert ls` (pulled on demand), not the banner.

**ACL** [ADOPTED BLOCKER: Critique 1 D4-BLOCKER-1, Critique 2 OQ-B, Critique 3 M4, Critique 4 BLOCKER-5 — `cluster.health`/`alert.ls` under `tether.v2.cluster.*` is broker-only (verified permissions.go:205-206/230-231) → members can't reach it → banner-for-everyone unbuildable; and members are denied `cluster.*` by the §13.8 RFI test]. **Resolution: place the two read RPCs OUTSIDE `tether.v2.cluster.*`.** Members already hold `Sub` on session-independent `tether.v2.sys.events` and `tether.v2.ctrl.version.announce` (verified permissions.go:108/119) — use a sibling **member-reachable** namespace, e.g. request `tether.v2.ctrl.health.req` / `tether.v2.ctrl.alert-ls.req` with a narrow `Pub` grant added to `PermissionsForActivatedMember` (+ `_INBOX.>` already present). Add a **positive** ACL test (member CAN reach `ctrl.health`/`ctrl.alert-ls`) AND keep the existing **negative** test green (member still CANNOT reach `cluster.apply.*`). This is a deliberate, tested carve-out — a D3-surface change that must be in the plan, not "待定". → **OQ9-B**.

---

## 4. Doc-first amendments (OQ10) — exact prose edits (land FIRST)

All in `docs/distributed-broker-architecture.md`:

1. **§10.1 line ~331 — surgical "三条"→"两条"** [Critique 3 m6 — do NOT rewrite the already-correct store-set list, do NOT duplicate the existing "§16 登记需求 §8 偏差" clause]: change only "三条…客户端合成" → "两条…客户端合成（`quorum_lost`/`force_single_active`）；`replication_degraded` store-backed（0009 CHECK、写者=D8b）虽 severe 不硬闸". Leave the store-set list intact.
2. **§9 line ~305 — data-path routing override** [Critique 3 B1, BLOCKER]: rewrite "经 §4.1 转发" → "数据面 push/pull 经 broadcast-SUB + home-keyed gate；仅 audit 行经 §4.1 leader Apply". + boot-reaper home-gated note.
3. **§9 audit line ~318** — name `OpTransferAudit`（纯-Aux 空-Body、`reqID=hex(sha256(transfer_id:kind))` 经 0011 ledger、`q<reqID>:xfer` 去重）+ leader-Apply terminal-state guard（first-terminal-wins）+ start 异步不阻塞 agent-forward + observe→commit 窗口 best-effort.
4. **§6.3 line ~183** — re-derivable set: `OpReconcileBatch` **+ OpTransferAudit**.
5. **§10.2 catalog footnote** — disk_pressure 经 follower level-triggered re-assert + `VerbAlertSignal` 转发；raft_lag **D9-deferred writerless**；broker_down ok/failed 计数 D9-deferred（D8b 报 contact-loss，message 不 over-promise）.
6. **§10.3** — banner 客户端组装（非挂回包）：reads 经 queue-group `alert.ls`（severe-only banner、stderr、best-effort）；writes 经 broadcast `cluster.health` 兼作 §10.4 闸；`--json` 抑制 stderr banner.
7. **§10.4** — client-synth = 两条；gate = VerifyLeader-confirmed（partitioned ex-leader 失败 VerifyLeader → 正确 gate）；**advisory pre-check，权威保护=broker 拒绝无法 quorum-serve 的写**；纯抖动/单 follower 滞后不 gate.
8. **§6.2 / §13.8** — carve-out: `ctrl.health.req`/`ctrl.alert-ls.req` member-reachable（非 broker-only `cluster.*`）；positive+negative ACL 测试.
9. **§16 deviation registry — new D8 block**: (a) 两条 client-synth + replication_degraded store-backed; (b) raft_lag/broker_down-counts D9-deferred; (c) banner client-assembled; (d) alert raise/clear = separate leader-gated loop (NOT folded into publisher tick); (e) ack permanent display-only, re-appearance pure client-side; (f) transfer audit re-derivable-once-committed + terminal-state-guard (or benign-race registration); (g) alert-SQL determinism = ordered-Apply+committed-predicate; (h) ACL carve-out for ctl.health/ctl.alert-ls.
10. **§19-D8 status line ~632** — "三条"→"两条 client-synth + replication_degraded store-backed"; flip checkbox at phase-done.

---

## 5. Build-and-prove guards & harness (OQ9)

**Guard `TestD8ProductionWiresNoCluster`** (mirror test/d7 token-scan: strip `//` comments, self-check `TestD8GuardSelfCheck`). Scan `cmd/tether/serve.go` + production broker/agent files.

`d8BannedTokens` (cutover *forms*, not bare symbols — [ADOPTED: Critique 1/2 OQ-I — bans must target `alertSink:`/`b.alertSink =`/struct-literal field-writes, since the inert `if b.alertSink != nil` read lives in scanned files]):
`transferAuditSink:`, `b.transferAuditSink =`, `alertSink:`, `b.alertSink =`, `node:` (struct-literal in serve.go/broker), `NewAlertReconciler`, `SubscribeClusterHealth(`, `SubscribeAlertLs(`, `startAlertReconcile(`, `INSERT INTO alerts`, `UPDATE alerts`, `INSERT INTO alert_acks`, `OpAlertRaise`/`OpAlertClear`/`OpAlertAck` (proposed from production).
**Allowed (live ctl path):** `proto.SubjClusterHealth`/`proto.SubjAlertLs` *publishes* in `cmd/tether` — the self-check fixture proves a client-side `nc.Request(proto.SubjClusterHealth…)` in `cmd/tether/expose.go` is NOT flagged while a broker-side `SubscribeClusterHealth(` IS. **Plus a positive assertion**: production `serve.go` subscribes zero `cluster.*`/`ctrl.health`/`ctrl.alert-ls` responders (count subscriptions, not just token-absence) [Critique 4 MAJOR-12].

`d8ExcludedFiles` (mechanism files): `transfer_home.go`, `transfer_audit_forward.go`, `alerts.go`/`alert_ops.go`, `alert_reconcile.go`, `alert_forward.go`, `cluster_health.go` + inherited `home.go`, `audit_publisher.go`, `cluster_forward.go`. **Each inert default pinned by a positive unit test** (`transferAuditSink==nil`, `xferTargetReplicas()==ReplicasSingle`, `alertSink==nil`) so exclusion isn't vacuous.

**Layering guards**: extend the D5 "internal/cluster no-NATS" scan to new files; keep `OpTransferAudit` Aux as `json.RawMessage` in `internal/cluster`, confine `schema.AuditTransfer` to the new `internal/xferaudit` leaf [verified cluster does NOT import schema — Critique 2 m2/MINOR-1]. `internal/clusternodes` stays pure-SQL.

**Harness `test/d8/`** (`//go:build d8_integration`, `TestD8Matrix -race`): build on `startRoutedJS` (d5) + `newHomeBroker` (d6) for N real routed brokers with the health/alert-ls responders + alert loop subscribed. **`TestD8Matrix` wired into `test/e2e/all_phases_test.go` with `-tags d8_integration`** [ADOPTED: Critique 4 cross-cutting — Drafts 1/2 omitted e2e wiring].

---

## 6. File-by-file change list (+ migration = next after 0013 → **none needed for D8b**; **D8a needs none** — 0011 ledger reused, 0009 alerts reused)

**New leaf `internal/xferaudit/`**: `plan.go` (`PlanTransferAudit`, `ReplayTransferAudit`, `xferAuxV1`, `transferReqID`); schema-only here.
**New `internal/broker/`**: `transfer_home.go` (`transferHomeGate`, home-gated reaper filter); `transfer_audit_forward.go` (`emitTransferAudit` dispatcher, `TransferAuditPayload`); `alert_ops.go`→`internal/cluster/` (`OpAlertRaise/Clear/Ack`, `PlanAlert*`, `DedupKey*`); `internal/cluster/alert_read.go` (`ActiveAlerts`); `alert_reconcile.go` (separate leader-gated `Run`); `alert_forward.go` (`VerbAlertSignal`/`VerbAlertAck`, payloads); `cluster_health.go` (`SubscribeClusterHealth` — VerifyLeader-confirmed); `SubscribeAlertLs`.
**New `cmd/tether/`**: `cluster_health.go` (`probeClusterHealth`), `banner.go` (`fetchAlerts` queue-group, `renderBanner` stderr severe-only, `withBanner`), `gate.go` (`gateDestructive`, `--ack-alerts`).
**Modified**: `internal/broker/transfer.go` (6 `pubAuditTransfer`→`emitTransferAudit`; `xferTargetReplicas`; home-gate calls; finalize epoch-fence); `internal/broker/disk.go` (clear-edge `alertSink` signal + level re-assert); `internal/broker/audit_publisher.go` (`OpTransferAudit` replay case; `ObserveReplicas` split); `internal/broker/cluster_forward.go` (`VerbTransferAudit`/`VerbAlertSignal`/`VerbAlertAck` cases); `internal/cluster/command.go` + `clustermeta.go` (register 4 new ops → `genericExecApplier`); `internal/auth/permissions.go` (member `ctrl.health.req`/`ctrl.alert-ls.req` Pub grants); `internal/proto/messages.go` (subjects SSOT + req/resp types); `cmd/tether/{expose,run,session,node,transfer,proxy,cluster}.go` (`gateDestructive` + `--ack-alerts`); `cmd/tether/{ps,ls,session,history}.go` (`withBanner`).

---

## 7. Test plan (mapped to the two EXIT gates; -race + leak gates)

**EXIT-A "tier-B object survives killing the HOME broker at N≥3"**: `TestD8TierBSurvivesHomeKill` — must kill **the home broker** (the one that ran prepare, identified explicitly), not an arbitrary broker [Critique 4 cross-cutting — vacuity escape].
**EXIT-B "severe gates destructive without false-positive"**: drills for {all-stale-no-confirmed-leader → gate; 1 fresh-confirmed-leader → no gate; stale-ex-leader-fails-VerifyLeader → gate; single-lagging-follower → no gate; force_single → gate}.

**Unit (`make test`)**: `OpTransferAudit` plan/replay byte-identical + poison-skip-on-forged-Aux (parity with D7 `errAppliedRejected` [Critique 4 MINOR-2]); reqID derivation stable/hex; **publisher-replays-transfer with vacuity control** (absent case → silent drop); `xferTargetReplicas` inert/clustered; `transferAuditSink==nil` default; alert raise idempotent / clear-re-raise history-rows / ack last-writer-wins / determinism DIFF-1 (two timezones) / clear requires `Observed && AllAtTarget` (not `!Degraded`); disk clear-edge fires + `pubSysEvent` unchanged; `ActiveAlerts` filters CLEARED; client-synth corroboration table (the EXIT-B cases above); banner stderr-not-stdout + `ps` stdout byte-stable; guard self-check (client-pub allowed, broker-sub banned).

**Gated harness (`TestD8Matrix -race`)**: home-routing no-fanout (exactly-one-broker handles prepare, real routed NATS not shared-DB sim [Critique 1 false-green]); audit re-derivable across election (idempotent, no double-row); EXIT-A; in-flight restarts (no false complete-row); rehome does-not-preserve-in-flight (epoch-fenced finalize, no cross-broker double-finalize); false-orphan NOT reaped on unrelated broker boot; retire-gate counts xfer buckets incl. a **bucket whose session is absent from ListSIDs** (JS-enumeration proof); alert replicated raise/clear via **real** `ReconcileOnce`→`Degraded` (not canned report [Critique 3 flag #7]); alert survives leader flap; disk_pressure follower-forward + lost-clear self-heal; cluster-level ack visible on all brokers; EXIT-B gating drills; alert-loop-not-wedged-by-stuck-publish (liveness inversion regression).

**-race + built-in NumGoroutine/fd leak gate** [ADOPTED: Critique 4 MAJOR-3/systemic — verified no leak gate on transfer tests today] on every new concurrent surface: watchdog-under-rehome, audit-forward, the separate alert-reconcile loop, the `withBanner` per-invocation goroutines (must be cancelled+joined on command return). Per CLAUDE.md §5, `-race` alone is insufficient.

---

## 8. Risk-ordered implementation sequence (riskiest-first)

1. **Doc-first edits §4** (gate everything; especially §9 data-path override + §10.1 + ACL carve-out + §16). Nothing builds until prose is consistent.
2. **`OpTransferAudit` reqID-ledger + publisher replay-case + vacuity test** (highest false-green + idempotency risk; verify empty-Body ledger write end-to-end).
3. **Retire-gate JS-enumeration + `ObserveReplicas` split** (data-loss-on-retire risk).
4. **Home routing gate + silent-retry-on-unresolved + finalize epoch-fence** (fan-out correctness).
5. **Home-gated orphan reaper** (cross-broker object-delete corruption).
6. **Alert store ops + separate leader-gated reconcile loop + clear-condition** (liveness-inversion + idle-zero-writes risk).
7. **client-synth gating VerifyLeader-confirmed + ACL carve-out** (EXIT-B; partitioned-ex-leader hole).
8. **disk bridge clear-edge + level re-assert**; **banner queue-group + severe-only**.
9. **Guards + harness + e2e wiring + leak gates**.

---

## 9. Residual OQ list for the main process (genuine decisions)

- **OQ9-A (transfer audit fidelity vs latency)**: `start` async-non-blocking leader-Apply (chosen, for audit-pair integrity) — confirm the §9 prose edit and that a harness assertion pins "start does not block agent-forward."
- **OQ9-B (terminal-state guard vs benign-race registration)**: add the leader-Apply first-terminal-wins read (contradiction-free log, costs a leader DB read) **vs** §16-register the benign complete/failed last-published-wins race (cheaper, accepts a re-derivability footnote). Synth recommends the **guard** (cleaner re-derivability), main adjudicates.
- **OQ9-C (zero-reply gating boundary)** [Critique 1/2 OQ-A/MAJOR-5]: at a **known-cluster** deployment, does `ErrNoResponders` (zero replies) **fail-closed** (block destructive unless `--ack-alerts`, since `ErrNoResponders` also fires during election/restart) or **fail-open** (proceed, since the broker rejects un-quorum'd writes anyway)? Needs an out-of-band "this is a cluster" signal so today's N=1 users never see friction while a real cluster mid-election does not silently fail-open. **This is the deepest residual boundary** — synth leans fail-closed-on-known-cluster + silent-on-non-cluster, but it requires a cluster-mode capability hint ctl can read.
- **OQ9-D (force_single_active live surface)** [Critique 2 OQ-A/D4-BLOCKER-2]: force_single_active is a live D7 single-node fact, but the only path to surface it to laptop-ctl is the new `ctrl.health` responder which is Layer-A (gated). So in D8b the force-single hard-gate is **build-and-prove-only, not live at N=1**. Confirm this is acceptable (the gate fires in the harness + post-D9; at N=1 production there is no cluster to force-single in the HA sense — D7's offline tool is single-node recovery). If a live N=1 force-single gate is required, it needs a production responder (breaks byte-unchanged) → defer. Synth recommends accepting build-and-prove-only.
- **OQ9-E (ConnectedServerName self-name)**: no longer load-bearing (silent-retry path chosen for unresolved), but if main prefers leader-replies-on-unresolved instead, this needs verification.

**Net:** the two most valuable draft findings (broadcast-SUB fan-out; publisher silent-skip) are verified-correct and central. The most dangerous draft *proposals* — Draft 2's no-reqID JS-window idempotency, Draft 3's fold-into-publisher-tick, Draft 4's bare-`State()` quorum rule and `cluster.*`-namespaced member RPCs — are all rejected with corrected mechanisms above. The boundary is now honest on the two fronts the critiques flagged: the member-ACL carve-out (OQ9-B doc + positive/negative test) and the force-single live-vs-gated split (OQ9-D).
