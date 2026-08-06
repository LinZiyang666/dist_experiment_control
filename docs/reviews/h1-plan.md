# h1 increment — plan (FINAL)

> Drafted by a 6-drafter / 6-critic / 1-synthesizer adversarial workflow (2026-08-04); finalized by
> the maintainer process. The synthesized body below is binding as amended by the
> **Maintainer finalization** section, which also answers open questions Q1–Q8.

## Maintainer finalization (2026-08-04, binding)

- **Q1 — `reply_too_large` exit class: 70 (`exitInternal`), CONFIRMED**, plus one line in
  usage.md §9.13 noting `reply_too_large` is the deterministic, non-retryable exception to the
  "retry on 70" guidance (post-A1 it can only be a tether bug; retrying cannot help).
- **Q2 — migration 0018 ships (option i), CONFIRMED**, with the rollback/log-replay constraint
  written into deploy-tier-gotchas.md exactly as specified in E2.
- **Q3 — PTY liveness grace floor 3min, CONFIRMED** (`kaGrace = max(6*interval, 3min)`).
  Suspend > grace ⇒ hangup is intended sshd-with-ClientAlive semantics; documented in usage.md.
- **Q4 — `portRetention` hardcoded const 24h, no knob, CONFIRMED.**
- **Q5 — agent log default-ON to `<Home>/agent/<sid>/agent.log` + dup2 fd2 → `agent.boot.err`,
  BOTH CONFIRMED.**
- **Q6 — capability gate stays CUT (X-1), CONFIRMED.** Lockstep-upgrade rule in
  deploy-tier-gotchas.md covers new FSM ops and new alert kinds; reversal condition stands.
- **Q7 — `proxy_bind_stalled` severity: OVERRULED severe → `info`.** The severe tier feeds the
  ctl banner + `--ack-alerts` friction gate on destructive commands (the force_single_active
  precedent). A stalled proxy bind on one node is an observability fact, not a cluster-integrity
  hazard — making it severe would force `--ack-alerts` onto every push/expose/run for as long as
  one laptop's network is broken (which is true TODAY for weiland-wsl and would be true at
  rollout). `tether alert ls` + webhook visibility is fully preserved at `info`.
- **Q8 — duplicate-audit residue ACCEPTED as best-effort event semantics, CONFIRMED.**
- **Citation correction:** E2's "the documented shape on `alert_read.go:77-80`" — no such file at
  HEAD; the precedent to follow is `planAlertSignal` in `internal/broker/alert_forward.go:44-62`
  (transition gate evaluated inside the plan path, nil command = no raft write). Same shape,
  correct citation.
- **Implementation note:** the courier/ps/E reads specified as `b.read()` resolve to
  `internal/broker/dbrole.go:80` (`readDB`, cannot write) — verified present.

Repo: tether @ 4e2a101 · fleet v0.4.7 · next release ships in ~10 days (first hop where upgrade smoothness matters). Load-bearing critique claims spot-verified against source: `node.go:426-428` (nil plan command = no-op Propose), `wire_freeze_test.go:139` (`proto.LocalProcess` frozen key set), `cfgdb_ratchet_test.go:83` (`exec.go:handlePsReq: 3` exact), `expose.go:414` (missing raw Respond site) — all confirmed.

## Goals

1. `tether ps` can never again time out silently: bounded replies, truncation surfaced, and a **generic** typed `reply_too_large` fallback + ERROR log on every broker reply path (G-A).
2. Replicated storage stops growing without bound: raft-safe retention GC for `processes` (EXITED) and `port_allocations` (FREED/REVOKED) in **both** modes (G-B).
3. Zombie class (a) eliminated at the root: proc lifecycle events survive agent NATS-conn rebuilds via a current-conn courier + broker ACK + register-snapshot replay (G-C).
4. Zombie class (b) eliminated at the root: ctl-liveness contract for interactive runs — keepalive + agent-side SIGHUP/master-close reaper, false-positive-proof (G-D).
5. Hot loops stop self-DoSing the broker: proxy-reaper M3 rotation backoff/cap + `proxy_bind_stalled` alert (proxy stays ON); d5 audit-publisher and alert-reconciler log/attempt discipline (G-E).
6. All tether logs (broker AND agent) hard-capped in-process; panics land somewhere durable on every launch style, including the frozen-argv fleet agents (G-F).

## Non-goals

- Raising NATS `max_payload`; disabling the proxy; fixing the JetStream storage budget (10047) itself.
- ProtoVersion bump — everything wire-side is additive/omitempty.
- G.1 "adopt instead of kill" orphan policy; rebinding per-run pty subs after session rebuild; chunk-stream decoders learning `code`; teaching `exec` a ctl-liveness contract; time-based log rotation/compression; migrating `reconcile_registry`/`home_delivery` onto `internal/backoff`; on-disk proc-event journal (see Adjudication C-6). Each is a named follow-up leaf, not silent scope creep.

---

## Workstream A — ps RPC bounding + generic reply-too-large

Base: Draft 1, amended per critiques.

### A1. Bound the ports side

- `internal/port/port.go`: add `ListBySessionFiltered(db, sid, ListBySessionOpts{IncludeFreed bool, Limit int})` + `CountBySession(db, sid, includeFreed bool)`. `ListBySession` stays as a thin unbounded wrapper (its 4 in-tree iterator callers unchanged: `reconcile.go:266`, `cluster_forward.go:647`, `proxy.go:172`).
- Ordering contract: `IncludeFreed=false` → `ORDER BY port ASC` (byte-identical to today for live rows). `IncludeFreed=true` → **`ORDER BY (state='ALLOCATED') DESC, created_at DESC, port ASC`** — live-ALLOCATED rows strictly first (amended from Draft 1's `(state='FREED') ASC`, which let >500 newer REVOKED rows push a live allocation out of a truncated `-a` view; per critique-4). No new index/migration.
- `internal/proc/proc.go`: symmetric `CountBySession(db, sid, includeExited)`.
- `internal/proto/messages.go` — additive/omitempty only:
  - `PsReq.IncludeFreedPorts bool \`json:"include_freed_ports,omitempty"\``
  - `PsResp` += `ProcsTotal int`, `ProcsTruncated bool`, `PortsTotal int`, `PortsTruncated bool` (all omitempty; zero-truncation replies marshal byte-identical to v0.4.7).
- `handlePsReq` (`internal/broker/exec.go:279-302`): hoist `serverMaxLimit=500` to cover both sections; ports via `ListBySessionFiltered{IncludeFreed: req.IncludeFreedPorts, Limit: serverMaxLimit}`. **All new count/list reads go through `b.read().SQL()`, never `b.cfg.DB`** — the cfgdb ratchet pins `exec.go:handlePsReq` at exactly 3 and must not grow (critiques 1/3). Count vs list are two non-transactional reads; accepted, documented (a row landing between them skews `PortsTotal` by one — cosmetic).
- `-a` semantics: `-a` = include EXITED procs AND FREED ports, both capped at 500 with truncation surfaced. No new CLI flag. Old ctl + new broker: FREED excluded server-side (un-breaks the incident fleet). New ctl + old broker: old broker ignores the field; ctl's client-side ALLOCATED filter (`ps.go:119-124`) stays as belt.

### A2. Generic reply-error mechanism

- New Broker method `respondBytes(msg *nats.Msg, payload []byte)` in `internal/broker/sessions.go` (no new file — `pkg-files internal/broker` is 70/70). Behavior:
  - `msg.Respond(payload)`; on error, log — **ERROR** for `ErrMaxPayload` and unexpected errors, **WARN** for `nats.ErrConnectionClosed`/`ErrConnectionDraining` (broker teardown must not become an ERROR burst; critique-2) — with subject + byte count.
  - On `ErrMaxPayload`: marshal `proto.ReplyTooLarge{Code: proto.CodeReplyTooLarge, Error: "<N>-byte reply exceeds server max_payload <M>"}` (M from `b.nc.Load().MaxPayload()`), send via direct `Respond` (structurally non-recursive, ≤~160B); failure of the fallback is log-and-return. Pin fallback size ≤512B with a unit test.
- `replyJSON` (`sessions.go:206`) becomes marshal + `respondBytes`.
- New wire bits (`internal/proto`): `CodeReplyTooLarge = "reply_too_large"` in `codes.go` (with exit-class rationale comment); `ReplyTooLarge{Code, Error string}` in `messages.go`.
- No new subjects (fallback uses the request's reply inbox) → zero ACL impact.
- **Reply-site census** (corrected: 25 raw sites; Draft 1 missed `expose.go:414` — verified):
  - `reply*Err` helpers (`replyErr` broker.go:1555, `replyExecErr` exec.go:103, `replyExposeErr` expose.go:328, `replyExposeRmErr` expose.go:424, `replyUpgradeErr` upgrade.go:125, `replyRunFailed` run.go:75, `replyKillFailed` run.go:130, `replyPushErr` transfer.go:1151): **minimal diff — swap each helper's inner `_ = msg.Respond` for `b.respondBytes` only** (adopted critique-6; do NOT rewrite helper bodies onto `replyJSON` — zero behavior delta, avoids error-code-coverage scanner re-registration churn).
  - Raw sites → `b.respondBytes`: expose.go:305/333/**414**/429, upgrade.go:116/130, broker.go:1414/1507, run.go:80/135, cluster_grow_trigger.go:36, cluster_upgrade_trigger.go:74, cluster_forward.go:561, cluster_health.go:49/154/195/209/214/219/223/226 (text protocol; payloads structurally tiny so the JSON fallback can't fire — the log is the value).
  - `authcallout.go:113`: **documented exemption** — inline log-only (a JSON fallback is garbage to nats-server's auth-callout consumer).
- **Non-ctl decoder audit (MANDATORY, same release — critique-4 MAJOR):** the agent register loop (`internal/agent/agent.go:1170-1198`) treats any `OK=false` code other than `CodeLeaderUnavailable` as an authoritative reject and **exits the agent**. An oversize register reply today is silently dropped and self-heals on retry; routed through the fallback it would kill the fleet node. Fix in the same commit: add `CodeReplyTooLarge` to the transient/retry case beside `CodeLeaderUnavailable` in that switch. Then audit every other non-ctl decoder of a `*Resp` for unknown-code-fatal behavior before its reply site is switched over (grep `Code !=` / code-switches in `internal/agent`, `internal/broker` cluster paths); record findings in the plan-review notes.
- **Enforcement:** AST-scan guard in new test file `internal/broker/reply_egress_test.go` (unit-named): every `.Respond(` in the package must be inside `respondBytes` or the pinned authcallout exemption; exact site count pinned (TLS-pairing-gate style — a new compliant-looking site goes red to force a read).

### A3. ctl side (`cmd/tether`)

- `error_hints.go`: hint + exit class for `reply_too_large` → **`exitInternal` (70)** (recommendation; open question Q1). ps.go needs no decode change (fallback lands in `resp.Code`, routed via `brokerErrorMessage`).
- `ps.go:93-98` timeout hint rewrite: drop the (now provably wrong) "processes table may be unusually large" blame; new text points at broker restart/overload, broker logs, `tether alert ls`, `TETHER_PS_TIMEOUT`.
- Truncation footers in both tables (`(showing 500 of NNN; live rows are never omitted — FREED history truncated)`); `psJSON` gains the 4 fields, `SchemaVersion` stays 1.

---

## Workstream B — raft-safe retention GC for `processes` + `port_allocations`

Base: Draft 2, with the capability gate CUT (see Adjudication X-1), keyset chunking KEPT (see Adjudication B-1), and four correctness amendments.

### B1. Two additive FSM ops, leader-planned explicit key sets, chunked

- `internal/cluster/command.go`: `OpProcGC`, `OpPortGC` consts + `knownOps` entries. `commandVersion` stays 2. **No `HasStorageGCOps()`, no capability gate, no `ClusterHealthResp` field** — replaced by a documented lockstep-upgrade rule (Adjudication X-1).
- `internal/cluster/clustermeta.go:85`: register both in `defaultAppliers()` via `genericExecApplier`.
- Baked Apply SQL (explicit keys, re-asserted terminal-state guard = deterministic idempotent no-op on replay):
  - `DELETE FROM processes WHERE pid IN (<≤500 LitText>) AND status='EXITED'`
  - `DELETE FROM port_allocations WHERE row_id IN (<≤500 LitInt>) AND state IN ('FREED','REVOKED')`
- `internal/proc/plan.go` `PlanGCExited(db, cutoff, limit)` and `internal/port/plan.go` `PlanGCTerminated(db, cutoff, limit)`:
  - **Cutoff is `now.Add(-retention).UTC()`** — `.UTC()` mandatory (also strips monotonic reading). Critique-1 verified the compare is lexical text over heterogeneous timestamp encodings: G.1 bakes `LitTime(now)` from raw `b.cfg.Now()` (local zone + `m=+…` suffix) while agent exits store `+0000 UTC`. Document the lexical-compare contract on both Plan funcs. **Also fix the pre-existing G.1 non-UTC bake** (`reconcile.go:155` path → `.UTC()` before `markProcExited`) — one line, prevents new heterogeneous rows.
  - Proc SELECT: `status='EXITED' AND COALESCE(ended_at, started_at) < ?` (COALESCE added for symmetry with ports — a NULL-`ended_at` EXITED row must not be immortal; critique-4). Port SELECT: `state IN ('FREED','REVOKED') AND COALESCE(revoked_at, created_at) < ?`, `ORDER BY row_id LIMIT ?`.
  - **Rows-close contract (critique-2 MAJOR):** plan closures materialize keys into a slice, `rows.Close()` + `rows.Err()` checked **before** building the Command. fsm.db and n.db share one `SetMaxOpenConns(1)` pool (`node.go:381-384`); a leaked `*sql.Rows` wedges `fsm.Apply` forever. State this in the plan funcs' comments; adversarial scan-error-injection test required.
  - Return `(nil, 0, nil)` when empty. **No RODB pre-count** — `Propose` treats a nil planned command as a no-op (`node.go:426-428`, verified); idle-zero-writes holds automatically (critiques 1/2).
- `internal/port/port.go`: single-mode twin `GCTerminated(db, cutoff) (int64, error)` beside `Free`/`Revoke`.
- `internal/broker/reconcile_passes.go`:
  - `proc-gc` pass re-registered `authorityLeader` (vacuous in single mode); single-mode branch keeps `singleWriter()` + `proc.GCExited` verbatim; the `if b.clusterMode { return nil }` skip is replaced by the cluster branch.
  - New `port-gc` pass, same cadence (`ProcGCInterval`), `authorityLeader`; single mode → `port.GCTerminated`.
  - Cluster branch shared helper written as a **free function** `gcProposeChunks(b *Broker, …)` (not a Broker method — keeps `type-methods Broker` at the A-only +1; critique-3): `reaperCaughtUp()` check → capture cutoff once per tick → loop ≤ `maxGCChunksPerTick=10` × `gcChunkRows=500` Proposes; Propose error → return it (registry backoff is the damper). Worst case per tick: 20 sequential synchronous Proposes; bound stated in a comment (one raft-apply-timeout stall max before backoff — critique-2).
- Knobs: `ProcRetention` (existing, default 1h) now honored in both modes. **`portRetention` is a hardcoded const 24h with a comment** — no yaml field, no serveconf churn (adopted critique-6; promote to a knob when someone asks). Open question Q4 confirms the value.

### B2. Rollout backlog sweep

No special path: first ~5 ticks drain 24,031 FREED + 8,479 EXITED at ≤5,000/table/tick on the 5-min cadence → converged ~25 min after deploy. Idempotent across crashes/replay (keyed DELETE + state guard; applied_index skip).

### B3. Interactions

G.1 (reads RUNNING/LOST only), PID-reuse defense, proxy reconcile, `__proxy__` ALLOCATED row: all disjoint (verified by critiques). A replayed C-courier exit for a GC'd row lands on `unknown_pid → OK`. Audit publisher: new ops are outside its replay set — no change. B alone un-bricks `ps` on racknerd even if A slips (24h retention caps a still-broken reaper at ~4k rows).

### B4. pc732 pre-flight

Downgraded from open question to runbook step (critiques 1/5): repo evidence settles it — `ghostfilter_test.go:9-11` documents pc732 as a SQLite-roster phase ghost absent from the committed raft config; `RecoverCluster({self})` (`node.go:521-562`) rewrites the config to self; racknerd commits at quorum 1 every ~20s. One-line pre-flight verification (`tether cluster status` raft-config dump) stays in the rollout runbook. (Moot for gating anyway since the capability gate is cut, but relevant to "GC actually runs".)

---

## Workstream C — durable proc-exit delivery (zombie class a)

Base: Draft 3 with the **on-disk journal cut to an in-memory pending set** (Adjudication C-6) and the BLOCKER-grade old-broker drain fix.

### C1. Publish via the current conn, never the captured one

- In-memory pending set + courier state live on a new `procCourier` struct field of `Agent` (all methods on `procCourier` — `type-methods internal/agent.Agent 126` is exact-count; critique-3). New prod file `internal/agent/proc_delivery.go` (responsibility-named; `internal/agent` is at 14 files, threshold 20 — safe).
- `run.go:326`/`exec.go:131` (exit) and `run.go:271`/`exec.go:128` (started) stop publishing directly; they enqueue into the courier, which loads `a.ncBox` per attempt. `run.go:327`'s `Kind:"exit"` RunChunk switches to the current conn (cheap win). Existing ordering kept: `unregisterProc` (run.go:319) precedes enqueue — **add a comment documenting the unregister→enqueue gap** (a register snapshot built between them omits the pid → G.1 marks -1; pre-existing window, must not be "fixed" into a double-report; critique-2).

### C2. Courier (single goroutine, session-independent)

- `runProcEventCourier(ctx)` started once in `Agent.Run` (parent ctx; leak-gate covered). Per-PID ordering: `started` strictly before `exit`; if `started` fails, skip that PID's `exit` this round.
- Attempt = `nc.RequestWithContext(proto.SubjEvProc(sid, nid, pid, kind), payload, 5s)` on the **existing** ev.proc subjects → zero new subjects, zero ACL change (agent pub `…ev.node.<nid>.>` permissions.go:193; `_INBOX.>` grants :230/:256; broker queue-group `broker.go:1081-1085`).
- Backoff 2s·2ⁿ cap 60s per entry. **Per-round attempt budget ≤8 requests** (critique-2: without it, >12 stuck entries make the round exceed the backoff cap and fresh exits queue behind stuck ones).
- **Old-broker demotion (fixes the BLOCKER + duplicate-audit spam, critiques 2/4/5):** after **3 consecutive request timeouts while `nc.IsConnected()`** (responder processed but never acks ⇒ pre-hop broker), park the entry as register-replay-only — stop couriering it. This degrades to today's fire-and-forget against a v0.4.7 broker instead of hammering it with duplicate `pubAuditProc` publishes every 60s forever.
- Nudges: every enqueue; after `a.register` succeeds in `session()` and in `onNATSReconnect`. `drainStarted(ctx)` before register in both places, budgeted as **one 5s request timeout** (not a 3s total — smaller than one RTT on the WSS links where reconnects are most common; critique-4). `onNATSReconnect` is single-flighted (agent.go:433-439) so the drain cannot stack.
- Cap: 4096 pending entries (memory), overflow = drop-oldest-exit-last + one WARN/min.

### C3. Broker ACK + full idempotency in `handleProcEvent` (`internal/broker/exec.go:114-160`)

- Respond only when `msg.Reply != ""` (old agents: byte-identical path). ACK via `b.replyJSON` (inherits A's egress hardening; ~60B, ErrMaxPayload impossible).
- New wire type `proto.ProcEventAck{OK bool, Code, Error string}` (omitempty).
- **`unknown_pid` must never be anchored on a stale follower read (critique-1 MAJOR):** the queue-grouped subscription means the request can land on a follower whose RODB lags the `started` insert; a `Get→ErrNotFound→{OK:true,unknown_pid}` ack would delete the courier entry for a real process — recreating class (a) through the ack path. Rule: **attempt the write unconditionally**; only a not-found derived on the *committed leader view* (`PlanMarkExited`'s `ErrNotFound` in cluster mode / direct single-mode not-found) may ack `unknown_pid`. Same discipline for `node_missing` on `started`. The local pre-check (via **`b.read().SQL()`**, never `b.cfg.DB` — cfgdb ratchet, critiques 1/3) is retained ONLY as audit-dedup: already-EXITED → `{OK:true, already_exited}` + **skip `pubAuditProc`**; already-recorded started → `{OK:true, already_recorded}` + skip insert/audit (kills the single-mode PK violation).
- Ack sent after commit (`proposeOrForward` is synchronous). `store_error`/`leader_unavailable` → `OK:false` → retry.
- Decision stands: core NATS request/reply, not JetStream (JS is broken on the incident fleet; the courier already provides retry).

### C4. Register integration (the N-1 replay channel)

- `buildLocalSnapshot` (agent.go:1306): append `LocalProcess{PID, State:"exited", RC, EndedAt}` per pending exit entry. New additive field `LocalProcess.EndedAt *time.Time \`json:"ended_at,omitempty"\``.
- **Gate fix (critique-3 MAJOR):** `proto.LocalProcess` IS transitively frozen in `internal/broker/wire_freeze_test.go:139` — add `"ended_at"` to the frozen key set in the same commit with the additive-omitempty rationale (the `upgrade_state`/`upgrade_detail` precedent at :135-139 is the documented form).
- `reconcileOnRegister` exited-branch + pure classifier `resolveReconcile` both use `min(EndedAt, now)` clamp, falling back to `now`; **both change identically** (`reconcile_marks_test.go` equivalence stays green). Determinism holds: leader re-runs the classifier and bakes literals (`cluster_forward.go:651`).
- **Clearance rule (fixes the drain BLOCKER, critique-2):** on register success, delete an exit entry when **the pid is absent from `resp.AcceptedProcesses`** (the snapshot carried the exit; absence from Accepted means the broker no longer believes it RUNNING — it reconciled it or already had it EXITED). Draft 3's `ReconciledProcesses`-membership rule is structurally broken against a v0.4.7 broker (old `handleProcEvent` already marked the row EXITED → the pid appears in neither list → entry immortal). Started entries clear on membership in `AcceptedProcesses` as drafted. Belt: max entry lifetime 24h.
- Layered contract (honest version, critique-4): L1 live ACKed request (RTT); L2 register-snapshot replay — exact rc **except** when G.1 already marked -1 in the unregister→enqueue race window (converges to today's semantics); L3 G.1 missed-exit(-1), unchanged.

### C5. `proc.started` gets the same treatment (not optional)

A dropped `started` is worse than a zombie: the next register's G.1 orphan pass **kills the user's live process** (`reconcile.go:257-263`). Shared mechanism, ~30 extra lines, plus `drainStarted` before register. Against an old broker the kill window equals today's — documented, no regression. Full "adopt instead of kill" is a named follow-up leaf.

---

## Workstream D — ctl-liveness contract for interactive runs (zombie class b)

Base: Draft 4, with the false-positive BLOCKER closed (probe-before-reap + 3min floor) and old-broker terminal-spam fixed. **Plan-level constraint: D ships only alongside C** (a reap during a rebuilt-conn window would otherwise convert class (b) back into class (a)).

### D1. Wire + subjects (additive)

- `RunReq.KAIntervalMS int \`json:"ka_interval_ms,omitempty"\`` (`messages.go:484ff`). 0/absent = capability not advertised = never reap. Int kept over bool (Adjudication D-4). Broker re-marshals RunReq at forward time (`internal/broker/run.go:46`), so old broker strips it → reaper off → safe degrade.
- `proto.SubjPtyKeepalive(sid, pid)` = `<prefix>.s.<sid>.pty.<pid>.ka`, empty body, ctl→agent direct (broker not in path; no raft surface).

### D2. ctl side (`cmd/tether/run.go`)

- `KAIntervalMS: 5000` in the RunReq; `pumpKeepaliveToBus(ctxRun, nc, subj)` ticker 5s, started with the other pumps regardless of `isTTY`. No new flags.
- **Old-broker spam fix (critique-4 MAJOR):** v0.4.7 member JWT has no `.ka` pub grant → server `-ERR` per publish → nats.go's default error handler writes to the **raw-mode terminal** every 5s. Fix: (a) set an async `ErrorHandler` in `connectCtl` (hygiene, needed anyway) that routes to the ctl logger, and (b) on the first permissions-violation for the ka subject, stop the pump for the session.

### D3. Agent side

- Session-scoped wildcard subscription in `Agent.session()` beside `subFwd` (agent.go:857-876): `<prefix>.s.<sid>.pty.*.ka` → `touchProcKA(pid)` (`// ctx-none: nats.go MsgHandler has no ctx`). Re-established on every rebuild; same teardown discipline as subFwd. This deliberately dodges the spawn-conn trap behind class (a).
- `procRec` additive fields under `procsMu`: `kaGrace time.Duration` (0 = never reap), `lastKA time.Time`, `reaped bool`, and **`waitDone bool` set by the handler immediately after `sess.Wait()` returns** — the reaper checks it under `procsMu` before Hangup, closing the recycled-pgid window between Wait and `unregisterProc` (critique-2; do not guard via `cmd.ProcessState`, that races Wait's write).
- Grace: clamp interval to [1s, 60s]; **`kaGrace = max(6*interval, 3min)`** (floor raised from 30s — a 30s floor reaps on ctl-host suspend/laptop sleep; critique-2). Suspend >grace ⇒ hangup is **ratified as intended** (sshd-with-ClientAliveInterval semantics), documented in usage.md with a failure-table row (open question Q3 confirms the floor).
- Reaper `ctlLivenessReaper(runCtx)` in `run.go`, started beside `rosterRefreshLoop` (agent.go:970), tick 5s. **False-positive guards (closes critique-4's BLOCKER — `nc.Status()` stays CONNECTED ~4-6min on a silent partition):**
  1. If `nc == nil || !connected` → restamp all + skip (unobservable silence never counts).
  2. Restamp on every reconnect: after the `.ka` subscribe in `session()`, and in `onNATSReconnect` after `ncBox.Store`.
  3. **Round-trip proof at decision time:** before acting on any expired candidate, the reaper performs one `nc.FlushTimeout(2s)` (or `RTT()`) probe. Probe fails → restamp all + skip. Probe succeeds → the candidate is NOT reaped this tick; it is marked `probeConfirmed` and reaped only on a **second consecutive reaper tick** (`ctlLivenessTick`, 5s) that still sees continued silence and a second successful probe (closes the heals-without-reconnect race where retransmitted kas arrive late).

  > **外审订正（h1 external review，「疑惑」第 3 条）**：本条原写「**one further full ka interval**」，实现落的是下一个 reaper tick。二者不一致，订正的是**文档**，因为实现是对的：这道阶梯的安全性来自**那次 round-trip 探测**，不来自等待时长。silence 是否有意义已由 `kaGrace`（下限 3min）判定完毕，第二次观测要排除的只是「conn 在上一 tick 谎报健康」这一种情形，5s 后再探一次足以排除。改成等满一个 ka interval 只会让僵尸多活几分钟而不增加任何安全性——而僵尸多活正是 h1 D 这条工作线要消灭的东西。
  - Pure decision fn `shouldReapRun(now, lastKA, grace, connected, probeState)` — table-testable.
  - Lock discipline: collect candidates under `procsMu`, probe and Hangup **outside** the lock, re-check `reaped`/`waitDone` under the lock before acting (critique-2).
- `pty.Session.Hangup()` in `internal/pty/pty.go`: under `s.mu`, no-op if closed/`waitDone`; `syscall.Kill(-pgid, SIGHUP)` (ESRCH ignored); close `s.Master` **without nil-ing the pointer and without setting `s.closed`**. Method comment carries the **corrected** justification (critique-4): the hazard is the unsynchronized `sess.Master` pointer read in the `.in` callback (run.go:280) racing `Close`'s nil-write — a data race under `-race` — not a nil-deref panic (`os.File` methods are nil-receiver safe). Closing the fd in place is safe for concurrent Read/Write and the later double-`Close`. No SIGKILL escalation; HUP-immune children keep their RUNNING row, honestly.
- Exit code expectation corrected: `pty.Wait` maps signal deaths to **rc=-1** (pty.go:166-168), not 128+HUP — walkthroughs, audit claims, and e2e assertions use -1.

### D4. Scope

PTY runs only; `exec` excluded (batch-shaped, often intended to outlive the submitter; no per-exec channel; stale exec rows are class (a) = workstream C).

---

## Workstream E — hot-loop backoff + alerts

Base: Draft 5, with the raise gate moved inside the Propose closure, clear-side hysteresis added, and cfgdb/ledger compliance.

### E1. `internal/backoff` (new stdlib-only leaf package)

As drafted: `Policy{Base, Cap}`, `Tracker` with `Due/Fail/Recover`, log discipline = log iff `First || ClassChanged`, one Info on `Recover` carrying `Suppressed`. Not goroutine-safe by design (single-goroutine owners; pin ownership in a comment at every field site — critique-2). **Anti-flap floor (critique-4):** `Recover` returns `ok=false` (folds into `Suppressed`) unless the failure run lasted ≥ one `Base` — bounds Warn/Info pairs under 100ms-granularity oscillation. Classes are coarse enum buckets, never `err.Error()`. No migration of `reconcile_registry`/`home_delivery` in this slice.

### E2. Reaper M3 rotation backoff + `proxy_bind_stalled` alert (`internal/broker/proxy_reconcile.go`)

- Leader-local `proxyRotate sync.Map` keyed `sid+"/"+nid` → `*backoff.Tracker` (comment: leader-local, observe-loop-only, Tracker not goroutine-safe). Rotation policy `{Base 20s, Cap 10min}`; first rotation gated by dwell as today; during holds the `:163` directive nudge continues — **proxy stays ON**.
- New helpers as **free functions taking `*Broker`** where a receiver buys nothing (type-methods ledger; critique-3).
- **Alert raise at the 3rd consecutive rotation (~2min):** kind `AlertKindProxyBindStalled`, severity `info` (maintainer Q7 ruling — `severe` would drag `--ack-alerts` friction onto every destructive command while one node's link is down), dedup key `proxy_bind_stalled:<sid>/<nid>`. **The already-ACTIVE gate lives inside the Propose plan closure** (leader db, under applyMu — the documented shape on `alert_read.go:77-80`): plan returns nil command when ACTIVE → race-free, zero raft entries while ACTIVE, cfgdb-ratchet-neutral (critique-1; Draft 5's outside-Propose `IsAlertActive(b.cfg.DB,…)` is dropped).
- **Clear-side hysteresis (critique-4 MAJOR — the incident link "intermittently heals"):** ready ticks are counted; the tracker is dropped and `PlanAlertClear` proposed only after **12 consecutive ready ticks (60s sustained)**; on shorter ready blips the backoff **decays one step** instead of resetting. Prevents raise/clear churn, webhook flapping, and backoff resets on a minutes-scale flapping link.
- Convergent clear pass in `driveProxyReconcile` (level-triggered, leader-restart-safe), reading via **`b.read()`** (pattern of `reconcileProxyTeardown`); sweeps stale trackers; malformed dedup keys cleared loudly; offline node's alert stays ACTIVE (an unreachable agent is still a stalled proxy).
- Hygiene deletes on teardown rows; ownership NOTE at `alert_reconcile.go:182-184` + proxy_reconcile.go header updated.
- Plumbing: `AlertKindProxyBindStalled` in `alert_ops.go` + `ValidAlertKind`; **migration `0018_alerts_kind_proxy_bind_stalled.sql`** (table rebuild — SQLite can't ALTER a CHECK; recreate `idx_alerts_dedup_active`; new CHECK is a strict superset). N-1 hazard adjudicated: ship 0018 AND write into `docs/deploy-tier-gotchas.md`: *(a)* broker cluster upgrades are lockstep before new alert kinds (or new FSM ops — see X-1) flow; *(b)* once `proxy_bind_stalled` has ever been raised, rollback below this release supports snapshot-restore only, never log-replay-from-scratch on a v0.4.7 binary (snapshot paths verified safe both directions via `fsm.Restore` forward-migrations, fsm.go:402-407). See open question Q2.
- Outcome on the live incident: rotations 20s→…→1/10min (≤144 FREED rows/day worst case vs ~4k), alert ACTIVE at ~2min, webhook fires via existing delta machinery, heal → sustained-ready clear.

### E3. d5 AuditPublisher + AlertReconciler discipline

- `AuditPublisher.tick` (`audit_publisher.go:139-151`): `pubBO` = log-discipline only (**`PublishOnce` cadence untouched — R-7 lag bound structurally preserved**); `reconBO` = attempt backoff `{1s, 5min}` on `ReconcileOnce` + log discipline. `ErrMetaGroupNotReady` participates in backoff, keeps no-Warn. Classifier `classifyJSErrClass` in `js_health.go`: `meta_not_ready` / `js_unavailable` / `api:<code>` / `ctx` / `other`. `ObserveReplicas` (D7 retire gate, `replication_degraded` inputs) not backed off — alerting latency unchanged. Accepted trade documented: post-outage replica-raise may start ≤5min late.
- AlertReconciler (`alert_reconcile.go:112-114, 229-231`): log-discipline-only Trackers, cadence untouched (`jsDownThreshold` needs per-tick evaluation). Kept in NOW: cost is ~six lines + tests, and the same JS outage drives its per-tick Warn at 2/s; **verify presence in racknerd's broker.err during rollout as a sanity check** (critique-6's demote condition), but do not gate the slice on it.
- Survey table (topology reconcile, observe loop, webhook poster, boot one-shots, reconcile registry): LATER/NONE verdicts as drafted, recorded in the plan doc.

---

## Workstream F — in-process size-capped rotating logs

Base: Draft 6, with the racknerd EACCES blocker fixed, the frozen-fd panic sink closed via dup2, flags cut to `--log-file` only, and an honest simcluster sweep.

### F1. `internal/logrotate` (new stdlib-only leaf; the only new prod file outside `internal/backoff`)

`Writer{path, maxBytes, backups, perm}`; `Open/Write/Close`. Semantics as drafted (rotate inline under the record mutex before the crossing write; single oversize record written whole — hard bound = cap + one record; numeric-suffix chain `.1..K`, oldest removed; `Open` stat-seeds `size` so a legacy giant rotates on first write) plus:
- **Degraded-mode reopen rate-limited to once per 5s** (critique-1: raft's hclog is bridged synchronously onto this sink; per-Write `open()` retries on a sick disk would stall raft's own goroutines on the 1-vCPU broker); between attempts, spill to stderr; transition line on state change; **periodic (once/min) reminder line while degraded** (critique-5: a permanently-degraded writer must not look like success).
- Rename-failure → truncate-in-place fallback with marker line (the anti-incident invariant: no failure mode leaves an unbounded file).
- Package scope-guard comment: size-only rotation; **degraded mode covers failed filesystems, not hung ones — same exposure as any file sink** (critique-2); cap is scoped to **bytes written through this Writer** (external appenders undercount it during the transition window); no multi-process locking.
- In-house over lumberjack: recorded rationale (truncate-fallback hard cap has no lumberjack equivalent; in-repo leak-gate precedent).

### F2. Wiring

- `cmd/tether/logging.go`: `newLogger(level, json, w io.Writer)`; `resolveLogSinkSpec` (flag > yaml > default; `-` = stderr). Keep the delta ≤~20 code lines (`main-noncli-code-lines` 1100 quantum — measure).
- **Flags: `--log-file` only, on serve and agent** (`-` = explicit stderr). `--log-max-size-mb`/`--log-max-backups` are **cut** (critique-6): size/backups come from yaml (broker: `observability.log_file/log_max_size_mb/log_max_backups` in `ObsSection`, non-strict decoder; agent: same keys in `agentYAML`, strict decoder — install.sh writes them commented-out only) with hardcoded defaults 50MB × 2. CLI golden churn = 2 flags.
- serve binary default = stderr (dev + embedded-NATS test harness); deployment default via broker.yaml. Agent daemon default = **ON**: empty spec resolves to `<Home>/agent/<sid>/agent.log`, 0600 — the binary default is the only channel that reaches the frozen-argv nohup fleet at the release hop (open question Q5 ratifies).
- **Panic sink for every launch style (closes critique-4/5 MAJOR):** at agent daemon startup, when the resolved sink is a file, open `<session dir>/agent.boot.err` O_APPEND|O_CREATE 0600 and **`dup2` it onto fd 2**. This makes the panic destination a property of the process, not of launch-time redirection — it rescues the 6 existing fleet agents whose inherited fd 2 would otherwise follow the rotation chain into an **unlinked inode forever** (self-upgrade is `syscall.Exec`, fds preserved for life). Native log keeps the `agent.log` name (rename-to-`agentd.log` rejected — see Adjudication F-3). Broker: panics go to journald via the unit; boot shim (`main.go:78`) stays raw stderr by design.

### F3. Deployment surface (`scripts/install.sh`, units, docs)

- Broker unit: `StandardOutput=journal` + `StandardError=journal` replacing both `append:` lines, with a comment block (slog → process-owned rotating file; stderr = panics/boot, journald-capped).
- broker.yaml template: `observability:` block (`log_file: $LOG_DIR/broker.log`, 50, 2).
- Agent banner: new recommended start line `1>/dev/null 2>> $SESSION_DIR/agent.boot.err`; agent.yaml heredoc gains the keys **as comments**.
- Agent user unit + simcluster unit: unchanged files, but note the journald divergence below.
- **Racknerd migration runbook (broker-ops.md, written this increment):** do NOT re-run install.sh (clobbers the hand-maintained `cluster:` section). Steps, in order: (1) `mkdir -p /var/log/journal && systemctl restart systemd-journald` + set `SystemMaxUse=` sized for the 19GB disk (persistent journal = the panic trail; critique-5); (2) hand-edit unit `Standard*` → journal; (3) append `observability:` block to broker.yaml; (4) **`rm -f /var/log/tether/broker.err /var/log/tether/broker.log`** — the existing files are root-owned (systemd `append:` opens them before privilege drop); a tether-uid `Open` would EACCES into permanent degraded mode, and *truncation does not fix ownership* (critique-5 BLOCKER); (5) daemon-reload + restart with the release; (6) **smoke-check: assert `/var/log/tether/broker.log` exists, is owned by tether, and is growing** (defends against silent-degraded).
- nats-server `LogRateLimit*`: ops note in broker-ops.md only, no code scope.
- **simcluster sweep (critique-5 MAJOR):** the drafted census was wrong in both directions. Re-census **mechanically at implementation time**: `grep -rl 'broker\.err\|agent\.log\|journalctl' test/simcluster/` (known: ≥14 drills incl. 41/50/67/80/96/98; journald-contract asserts in drills 33/51/22 invert — R-BROKERLOG's meaning flips; `_agent_journal_after` helper family must read the native agent file). Update all touched text; **run on weilandserver only a named subset**: the broker-provisioning drill + one agent-log-reading drill (e.g. 94) + one journald-contract drill (e.g. 51) — not `./run-drills.sh` (mandate: only the relevant ones; exposure iron-rule applies — if a drill shows the broker still needs `broker.err`, that is a finding).

---

## Cross-cutting

### Wire / ACL / gate update table (merged, single owner per ledger)

| Gate / ledger | Delta (whole increment) | Update |
|---|---|---|
| wire-inventory append-only (`internal/proto/wire_inventory_test.go`) | PsReq +1, PsResp +4, `ReplyTooLarge` (2), `ProcEventAck` (3), `LocalProcess.EndedAt`, `RunReq.KAIntervalMS`. **No `ClusterHealthResp.StorageGCOps` (gate cut)** | one updater run per landing workstream; pure appends; no ProtoVersion bump |
| broker↔broker wire freeze (`internal/broker/wire_freeze_test.go:139`) | `proto.LocalProcess` += `"ended_at"` | hand-edit frozen key set + rationale comment (upgrade_state precedent) — workstream C |
| proto golden (`golden_test.go`) | `ProcEventAck` fixture; `*time.Time`+omitempty keeps existing fixtures byte-identical | `-update-golden` |
| proto subject golden (`proto_test.go:56-61`) | +`SubjPtyKeepalive` row | table edit — workstream D |
| cfgdb ratchet (`test/determinism/cfgdb_ratchet_test.go`) | **ZERO edits — hard rule**: every new read in A (counts), C (pre-check), E (IsAlertActive-in-plan, clear-pass SELECT) uses `b.read()`/plan-closure db | none (that's the point) |
| type-methods (`structural_budget_test.go`) | **Broker 279 → 280** (`respondBytes` only; B/E helpers are free functions). **Agent 126 → 129** (`touchProcKA`, `restampProcKA`, `ctlLivenessReaper`; C's courier lives on `procCourier`) | ONE hand-edit each, one commit-message justification each; measured at merge, not per-draft |
| pkg-files | `internal/broker` stays 70 (no new prod file — respondBytes in sessions.go, backoff is its own pkg); `cmd/tether` stays 52; new pkgs `internal/backoff`, `internal/logrotate` below thresholds; `internal/agent` 14→15 (proc_delivery.go, threshold 20) | none |
| pkg-code-lines / main-noncli | broker ≈15.1k (+~350) vs 16000 quantum; agent ≈5.1k (+~500) vs 6000; cmd noncli 1100 quantum | **measure after merge** (three drafts hedged against a stale base); hand-raise with justification only if a quantum trips |
| ACL grants (`internal/auth/permissions.go`) | member Pub.Allow += `.s.<sid>.pty.*.ka`; agent Sub.Allow += same. Nothing else (ev.proc request/reply and reply fallback ride existing grants) | edit + `permissions_test.go` assertions; `acl_reconcile_test.go` structurally out of scope for pty family (verified) — run to confirm green |
| raft-Apply reachability + `TestKnownOpsAppliersSymmetry` (`node_readdr_test.go:132`) | OpProcGC/OpPortGC ride `genericExecApplier`; PlanGC* leader-only | both stay green unchanged — named so half-registration is a caught error |
| R7 registry equivalence tests | proc-gc authority→Leader + new port-gc pass | update frozen expectations in the same commit |
| CLI-surface golden | +2 flags (`--log-file` × serve/agent) | `-update-command-tree-golden`, reconcile every diff line |
| error-code gates (`cmd/tether`) | +`reply_too_large` (class 70) with emitter | classification entry; helper-list unchanged (minimal-diff helper edits) |
| migrations | **0018** = alerts-kind rebuild (E owns it; B has none) | storage migration tests extended (CHECK admits new kind, rejects garbage, dedup index survives) |
| naming freeze / origin gate | all new files responsibility-named (`proc_delivery.go`, `backoff.go`, `logrotate.go`, `reply_egress_test.go`, `proxy_reconcile_test.go`, `ctl_liveness_test.go`); `// origin: 2026-08-04 incident …` / plan-doc citations on every new guard | nothing to update, comply |
| ctx policy | courier/reaper on parent/run ctx; MsgHandlers annotated `// ctx-none`; logrotate is io.Writer-shaped | annotations only |
| leak gate | +1 courier goroutine, +1 reaper goroutine (ctx-bound), 1 fd per log Writer | extend agent teardown leak tests; `-race` on all touched concurrency |
| docs | deploy-tier-gotchas.md (lockstep rule + rollback/log-replay constraint), broker-ops.md (racknerd runbook, journald, alert kind), usage.md (suspend-reap semantics, ps truncation, broker.yaml keys), distributed-broker-architecture.md (op catalog, reaper backoff) | additive rows |

### Test plan (consolidated; every new guard mutation-verified in an isolated worktree, one mutation at a time — costs pre-announced per repo memory)

- **A**: `internal/port/port_test.go` (filter/order/count tables; adversarial: 600 FREED **and** 550 REVOKED newer than live ALLOCATED rows → live rows always present under Limit); `test/p4/ps_filter_test.go` incident replay (24k FREED seed; default reply <64KB; `-a` → 500 rows, truncated flags, every live row present); `internal/broker/reply_egress_test.go` (tiny-MaxPayload embedded server → typed fallback in <1s, ERROR log, ≤512B bound, closed-conn WARN-not-ERROR path, AST egress guard); cmd/tether hint/class/footer/json tests + rewritten-timeout-text test; **agent-register transient-code test** (broker replies `reply_too_large` → agent retries, does NOT exit). Mutations: raw Respond added → guard red; LIMIT deleted → replay red; sort key flipped → ordering red; `_ =` restored → fallback red; truncation hardcoded false → red.
- **B**: proc/port plan tests (EXITED/terminal-only selection; NULL `ended_at`/`revoked_at` COALESCE rows collected; LIMIT boundary; all-literal no-`?` no-`now` assertion; double-ExecCommand idempotency; `'` injection via LitText; **scan-error-injection → Rows closed, no wedge**; **G.1-shaped non-UTC-text row vs UTC cutoff**); `reconcile_passes_test.go` cluster harness (drain across ticks, chunk cap, non-leader proposes nothing, reaperCaughtUp gate); single-vs-cluster differential. Mutations: state guard dropped → red; baked keys → `datetime('now')` → red; LIMIT dropped → red; authority reverted → red.
- **C**: `internal/agent/proc_delivery_test.go` (class-a regression: swap ncBox conn before exit → row EXITED with real rc — mutation: publish on captured conn → red; ack-loss dedup → exactly one EXITED + one audit; per-PID ordering under churn with round-budget bound; **old-broker emulation: responder that PROCESSES but never replies** → entry demoted to register-replay after 3 timeouts, drains via absent-from-Accepted rule, no unbounded retries — this fixes Draft 3's test which only covered the never-received case; courier ctx exit under -race/leak gate); `internal/broker/proc_event_ack_test.go` (no-Reply old agent → no respond; follower-stale not-found does NOT ack unknown_pid; leader-derived not-found does; already_exited/already_recorded/store_error; ack-after-commit); `reconcile_marks_test.go` EndedAt clamp equivalence; p8/chaos extensions.
- **D**: `internal/pty/pty_test.go` (Hangup semantics; HUP-immune survivor; concurrent master IO under -race — the data-race regression test); agent tables (`shouldReapRun` incl. connected=false and probe-state rows; grace derivation incl. 3min floor); `test/p5/run_e2e_test.go` (reap on silence with rc=-1 assertion; no-capability never reaped; reconnect no-reap; **blackholed-but-open TCP conn (iptables DROP or harness-level socket wedge) → probe fails → no reap** — mutation-verify the probe, `nc.Close()` does not exercise this hole); permissions tests. Mutations: restamp deleted → red; grace-0 skip inverted → red; connected guard removed → red; probe removed → blackhole test red; Hangup nils Master → race test red; re-marshal drops field → capability-travel test red.
- **E**: `backoff_test.go` (schedule/cap/class/suppression/recover-floor truth tables); `proxy_reconcile_test.go` (fake clock 25-min schedule 20/40/…/600s; nudge continues during holds; alert exactly at rotation 3, **zero Proposes while ACTIVE** counted; **flapping link: ready-for-30s blips neither clear the alert nor reset backoff; 60s sustained ready clears once**; leader-restart staleness via seeded ACTIVE row; teardown hygiene; malformed key); migration 0018 tests (+ mutation: apply only through 0017 → CHECK failure red); `audit_publisher_test.go` R-7 protection (1000 failing ticks → 1000 PublishOnce calls, 1 Warn — mutation: gate PublishOnce behind Due → red); AlertReconciler cadence-untouched proof.
- **F**: `logrotate_test.go` (cap boundary tables; backup chain; -race no-loss/no-tear reassembly incl. **a concurrent external O_APPEND writer**; chain gaps; external rm/rename; stat-seed; rename-failure truncate fallback; degraded reopen **rate-limit** + reminder line; fd balance); cmd/tether precedence tables (2 flags + yaml); p10 install.sh assertions (journal lines, no `append:`, observability block, boot.err banner); **agent dup2 test** (fd 2 points at boot.err after daemon init; panic text lands there). Mutations M1–M6 as drafted + dup2-removed → panic-sink test red.
- Closing gates: `make test` + `make e2e-parallel` (the only full-matrix gate; no serial re-run) + `make lint`; `-race` + in-repo leak gate on agent/broker/pty touches. Never pipe gate output through `| tail`.

### Implementation order (dependencies explicit)

1. **A** (reply egress + ps bounding) — C's acks ride `replyJSON`; the agent transient-code fix lands here. No dependencies.
2. **B** (storage GC) — independent of A; lands early because it alone un-bricks racknerd `ps` and its backlog sweep wants soak time before release.
3. **E** (loops backoff + alert + migration 0018) — independent; shares `proxy_reconcile.go` with nothing above.
4. **C** (exit durability) — after A (replyJSON signature stable).
5. **D** (pty liveness) — after C, **same release as C, hard constraint**.
6. **F** (log caps + install.sh/unit + simcluster sweep) — last; it owns the deploy-tier drill run and the racknerd runbook, which should reflect the final binary.

Ledger edits (type-methods, budgets) happen once, at merge, with measured numbers.

### Rollout notes (first hop, v0.4.7 → next)

- **Broker before any agent** — hard runbook rule. A pre-hop broker never acks courier requests; the demotion belt bounds the damage, but the ordering avoids duplicate-audit noise entirely. Broker-first also delivers fresh auth-callout JWTs with `.ka` grants before new ctls appear.
- Racknerd broker swap runbook (F3): journald persistence → unit edit → yaml block → **rm root-owned broker.log/broker.err (5.3GB)** → restart → smoke-check tether-owned growing log + `tether ps` returns in ms + `tether alert ls` shows `proxy_bind_stalled:lab/weiland-wsl` ACTIVE (expected while wsl's dial is broken).
- Pre-flight: raft-config dump confirms pc732 absent from committed Configuration (expected per repo evidence; verify anyway).
- GC backlog: expect ~25 min of chunked drains post-restart; `ps -a` totals shrink accordingly.
- Version-skew summary: every feature degrades to v0.4.7 behavior when any leg is old (ps FREED-exclusion is view-only; courier demotes; reaper arms only end-to-end-new; ka pubs stop on first -ERR against an old broker). Rollback constraint: once `proxy_bind_stalled` has been raised, no from-scratch log replay on a v0.4.7 binary (snapshot-restore only) — documented in deploy-tier-gotchas.md.
- Fleet agents: self-upgrade preserves frozen argv/fds; the binary-default log file + dup2 panic sink require no per-host action. One-time ops note: legacy multi-GB `agent.log` becomes `.1` — `rm agent.log.1` at leisure.

### Open questions for the maintainer

1. **Q1** — `reply_too_large` exit class: plan says 70/`exitInternal` (post-fix it can only be a tether bug); tension with usage.md §9.13 "retry 70" for a deterministic failure. Confirm 70, or move to 64 per the M1/Y2 precedent.
2. **Q2** — Migration 0018 vs the 0016 "no new alerts.kind" precedent: plan ships 0018 + documented rollback constraint (option i). Alternative (option ii): surface the stall without a new alert kind (loses `tether alert ls`/webhook UX). Ratify (i).
3. **Q3** — D's grace floor 3min (suspend >3min ⇒ hangup, documented). Confirm the value; alternatives: 5min, or derive from a shortened agent `PingInterval`.
4. **Q4** — `portRetention` = const 24h (no knob). Confirm value and knob-lessness.
5. **Q5** — Agent log default-ON to `<Home>/agent/<sid>/agent.log` (asymmetric with serve's stderr default) + dup2 of fd 2 → `agent.boot.err`. Ratify both.
6. **Q6** — X-1 adjudication (capability gate cut in favor of one documented lockstep rule): confirm, given multi-broker rolling upgrades are not a supported near-term path. If they become one, BOTH B and E must gate.
7. **Q7** — `proxy_bind_stalled` severity `severe`: confirm (or `info` if severe is reserved for cluster integrity).
8. **Q8** — Duplicate-audit residue: rare cluster-mode duplicate `audit.proc{exit}` from the stale-RODB dedup window accepted as best-effort event semantics (vs threading a was-no-op result through `proposeOrForward`). Confirm accept.

---

## Adjudication log

**Cross-cutting**

- **X-1 ADOPTED (critique-6 cross-draft MAJOR):** capability gate (Draft 2) vs document-don't-gate (Draft 5) decided ONCE, in favor of **cut the gate**: one lockstep rule in deploy-tier-gotchas.md covers new FSM ops AND new alert kinds. Rationale: production is force-single N=1; any second broker before the *next* hop installs the new binary; the gate defends a topology that cannot exist yet and costs a wire field + health plumbing + tests. Reversal condition recorded (Q6). Draft 2's `StorageGCOps` field, `HasStorageGCOps`, and gate tests are deleted from scope.
- **X-2 ADOPTED (critique-3 MAJOR):** single owner for each exact-count ledger. Uniform `b.read()` rule → cfgdb table needs zero edits (critiques 1/2/3/5 all found independent violations in Drafts 1/3/5 — all fixed at the design level). One hand-raise each for Broker (280) and Agent (129), measured at merge.
- **X-3 ADOPTED (critique-3):** migration collision resolved — only E has a migration (0018); Draft 2 never actually proposed one.
- **X-4 ADOPTED (critiques 2/6):** "D ships only with C" promoted from risk note to plan constraint with ordering enforced in the implementation sequence.

**Workstream A**

- ADOPTED critique-4 MAJOR (register-reply fatal agent exit): agent transient-code fix + non-ctl decoder audit added as mandatory same-commit work. This was the most dangerous single finding across all six critiques — the mechanism as drafted would have converted a self-healing condition into fleet-node death.
- ADOPTED critiques 2/4 (census misses expose.go:414 — verified) and critique-2 (shutdown ERROR noise → WARN classification for closed/draining).
- ADOPTED critique-4 (REVOKED ordering hole): sort key changed to `(state='ALLOCATED') DESC`; REVOKED-heavy adversarial fixture added.
- ADOPTED critique-6 (minimal-diff helper edits instead of rewriting 8 helpers onto replyJSON): the constraint is satisfied by swapping the inner Respond; consolidation deferred.
- ADOPTED-AS-DOCUMENTED critique-4 (count/list non-transactional skew): accepted + documented rather than limit+1 derivation — totals have UX value and the skew is cosmetic.
- REJECTED nothing material in Draft 1's core design; exit-class recommendation (70) kept per critique-6's "decide fast" with Q1 for final say.

**Workstream B**

- ADOPTED critique-2 MAJOR (Rows-close/FSM-wedge contract) verbatim — plan text, comments, and an injection test.
- ADOPTED critique-1 MAJOR (lexical timestamp compare / G.1 non-UTC bake): UTC cutoff + G.1 one-line normalization + adversarial mixed-encoding test.
- ADOPTED critiques 1/2 (drop the RODB pre-count; nil plan command is already a no-op — verified `node.go:426-428`).
- ADOPTED critique-4 (proc-side COALESCE symmetry).
- ADOPTED critiques 1/5 (R1 downgrade: pc732 resolved from repo evidence; pre-flight check retained as hygiene).
- ADOPTED critique-6 (cut `PortRetention` yaml knob → const 24h) and critique-3 (free function; name `TestKnownOpsAppliersSymmetry`).
- **REJECTED critique-6 MAJOR (replace keyset chunking with single baked-cutoff DELETE):** partially valid — the 1-vCPU stall claim is unmeasured — but keyset keeps retention *comparison* leader-side only (the replicated DELETE keys off ids + terminal-state guard, immune to the heterogeneous-timestamp problem at Apply), bounds Apply txns by construction, avoids betting on `DELETE…LIMIT` compile options, and the machinery cost after dropping the pre-count and the capability gate is modest (~two plan funcs + one loop). The critique's own 根治 caveat ("if you want a hard Apply bound anyway, keep chunking") is exercised.

**Workstream C**

- ADOPTED critique-2 BLOCKER (old-broker drain): clearance rule changed to absent-from-`AcceptedProcesses`; Draft 3's `ReconciledProcesses` rule was structurally broken (old broker marks EXITED via the request itself → pid in neither list). Draft 3's own test only covered the never-received case — replaced with a processes-but-never-replies responder.
- ADOPTED critiques 2/4/5 (duplicate-audit hammering + round degeneration): 3-silent-timeouts demotion to register-replay-only + ≤8 requests/round budget + broker-before-agents runbook rule.
- ADOPTED critique-1 MAJOR (stale-follower `unknown_pid`): write-unconditionally, only leader-committed not-found acks unknown_pid; pre-check demoted to audit-dedup only.
- ADOPTED critique-3 MAJORs (LocalProcess wire-freeze entry; procCourier method placement; cfgdb via b.read()).
- ADOPTED critique-4 MINORs (honest L2 rc claim; drainStarted budget = one 5s RTT) and critique-2 MINOR (document the unregister→enqueue gap).
- **ADOPTED critique-6 MAJOR (cut the on-disk journal): C-6.** The disk journal's only marginal benefit is real-rc fidelity across an agent-process crash — not an incident fact, not a zombie class, and its absence degrades exactly to today's documented G.1(-1) behavior. Cutting it removes the fsync/tmp-rename/restart-scan/partial-write surface (~a third of the file + its whole crash-test matrix) while the in-memory courier + ACK + register replay fully closes class (a). Draft 3's `Home==""` mode becomes the only mode. Disk journal recorded as a possible follow-up leaf if rc-across-crash fidelity is ever demanded.
- KEPT Draft 3's `started` treatment (C5) — critique-6 endorsed it; the orphan-kill argument is verified.

**Workstream D**

- ADOPTED critique-4 BLOCKER (nc.Status() blind to silent partitions): round-trip probe at decision time + one-further-interval confirmation + restamp-on-probe-failure; blackholed-socket mutation test mandated (`nc.Close()` explicitly ruled insufficient).
- ADOPTED critique-2 MAJOR (suspend reaping) merged with the above: grace floor raised 30s → 3min, suspend semantics ratified + documented (Q3).
- ADOPTED critique-4 MAJOR (old-broker -ERR spam into raw terminal): ctl ErrorHandler + pump-stop on first permission violation.
- ADOPTED critique-2 (recycled-pgid `waitDone` flag; don't hold `procsMu` across Hangup) and critique-4 MINORs (rc=-1 not 128+HUP; corrected Hangup rationale — data race, not nil-deref).
- ADOPTED critique-3 MAJOR (Agent method ledger — the +3 is declared and hand-raised once).
- **REJECTED critique-6 (KAIntervalMS int → bool):** additive wire fields are forever; the int is the future-proof shape and the clamp/derivation cost is ~15 lines. The critique itself conceded this trade.

**Workstream E**

- ADOPTED critique-1 MAJOR (0018 vs 0016 precedent) as an **explicit decision** (option i: ship + document rollback/log-replay constraint), not silence — the alert-ls/webhook UX is the point of the alert; Q2 gives the maintainer the veto toward option ii.
- ADOPTED critique-1 MINOR (raise gate inside the Propose plan closure — the documented `alert_read.go` shape; kills both the cfgdb site and the gate→Propose race).
- ADOPTED critique-4 MAJOR (clear-side hysteresis: 60s sustained-ready before tracker drop/clear; decay-not-reset on blips) — directly targets the incident's "intermittently heals" trigger.
- ADOPTED critique-4 MINOR (Recover min-hold anti-flap floor in the Tracker) and critique-2 MINOR (ownership comments).
- ADOPTED critiques 2/3/5 (cfgdb via b.read() in the clear pass; free functions for the ledger).
- **PARTIALLY REJECTED critique-6 (demote AlertReconciler to LATER):** kept in NOW — the cost is a few lines under the same helper, the 500ms Warn sites are verified in source, and the same JS outage drives them; the critique's verification step (check racknerd's broker.err) is adopted as a rollout sanity check rather than a scope gate.

**Workstream F**

- ADOPTED critique-5 BLOCKER (root-owned broker.log → permanent silent degraded): runbook `rm -f` before restart + smoke-check + periodic degraded reminder line.
- ADOPTED critique-4 MAJOR (dup2 fd 2 → `agent.boot.err`) over critique-5's alternative (rename to `agentd.log`): **F-3.** dup2 is the root-cause form — it makes the panic destination process-owned for every launch style (nohup, user unit, future systemd), rescues the 6 frozen-fd fleet agents with zero per-host action, and lets the native log keep one name. The rename only relocates the problem (legacy fd 2 still ends on an unlinked inode of whatever file it was pointed at). Residual: stray legacy fd-1 stdout follows renames — accepted, near-zero traffic, cap scoped to Writer-written bytes (critique-2/4's size-skew point, adopted as documentation + external-appender test).
- ADOPTED critique-5 MAJOR (simcluster census wrong): mechanical re-census + journald-contract sweep + `_agent_journal_after` update; balanced against critique-6 (run only a named 3-drill subset, not the full suite).
- ADOPTED critique-1 MINOR (rate-limit degraded reopen — raft goroutines must not pay per-Write open() on a sick disk), critique-2 MINORs (hung-fs scope note; external-appender test), critique-5 MINOR (journald persistence as a scripted runbook step), critique-6 MINORs (cut 4 of 6 flags; nats-server rate-limit stays ops-note; drill-run scoping).
- KEPT Draft 6's in-house-over-lumberjack, agent default-ON, serve-stderr-default, and boot-shim-stays-raw decisions — no critique challenged their substance.