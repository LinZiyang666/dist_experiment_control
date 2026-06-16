# Proxy tunnel reconnect — "false-online" exit node after a data-plane drop — Implementation Plan

Date: 2026-06-16
Status: **DRAFTED — awaiting internal adversarial review (Stage C).** Stage-A produced + finalized; not yet implemented.
Target release: **v0.3.4** (patch — additive, no wire-string break, `ProtoVersion` stays 1; see §8)
Convention: post-1.0 leaf increment (agent data-plane resilience + broker-local readiness convergence). Not on the P0–P11 milestone line.

> **How this plan was produced.** Stage-A per CLAUDE.md §3: a 5-expert + 3-critic adversarial Workflow drafted
> and synthesized a candidate (run `wf_44623f8d-cbc`; the first attempt stalled mid-Draft and was resumed to
> completion — 9 agents, ~817K subagent tokens). The main process then **verified every load-bearing in-tree
> claim** (§0) and finalized. The critic panel overturned two draft positions that would have made the fix
> *worse than today* (see §0 deltas): "any DENY = terminal" (which makes the dominant `public_port_bind_failed`
> reconnect path permanently eject the node) and a per-redial new-`cancel` session model (which breaks
> `Close`/fail-closed cancellation). The finalizer added one fix the panel flagged as a residual hole but left
> unowned — the transient-DB-error → false-terminal path (R1) — and closed it at the source (Fix C).

---

## 0. Finalizer notes (main process)

### Load-bearing claims verified in-tree (file:line)

- **No reconnect exists.** `internal/tunnel/tunnel.go`: `Client` has only `Start`/`Open`/`Close`/`streamAcceptLoop`/`bridgeStreamToLocal`. `streamAcceptLoop` (`:650`) returns on `sess.Accept()` error without deleting `c.sessions[port]` or re-dialing. The doc comments promising reconnect are false: `ConnectAll()` (`:36`) exists ONLY in the comment (repo-wide grep: zero code matches); "Reconnects on session loss" (`:509`) is unimplemented.
- **Broker unbinds the public port on yamux close.** `tunnel.go:322-326` — a goroutine waits on `yamuxSess.CloseChan()` then `sess.listener.Close()`; `publicAcceptLoop`'s defer (`:335-344`) also closes the listener + removes the session. So an agent-side session drop → broker public port stops listening (matches the live `ss` evidence: 14004 absent).
- **`proxy_ready` is a write-once latch.** Agent publishes `ready` once in `proxyStartLocked` (`internal/agent/proxy.go:228`) after `AddProxy` succeeds; nothing clears it on a later tunnel drop. Broker `handleProxyReadyEvent` (`internal/broker/proxy.go:335`) sets `nodes.proxy_ready`; it **always honors `unready`** and gates `ready` on the master switch (`proxy.go:351-357`) — the asymmetry Fix B relies on.
- **The latch is the render gate.** `proxy status` `proxyStatusNodes` (`broker/proxy.go:839-867`) reads `proxy_ready` + an `ALLOCATED __proxy__` row. `/sub` `LiveProxyNodes` (`internal/subhttp/subhttp.go:139-147`) gates on `pa.state='ALLOCATED' AND n.status='ONLINE' AND n.proxy_ready=1` — none reflects data-plane liveness, so a dead exit is advertised to subscribers.
- **No self-heal today.** `repairProxy` is convergence-first: returns early when `on && ready && agentGen==brokerGen && agentEpoch==epoch` (`broker/proxy.go:589`); the not-ready branch (`:620-628`) re-pushes a **keyset-only** directive (`Token==""`). The OFFLINE reconcile (clears `proxy_ready`) does NOT fire on a tunnel-only drop because NATS stays up.
- **The flap hazard (Fix D).** A keyset-only push with `p.srv != nil` hits the agent `default:` branch (`agent/proxy.go:163-176`), which calls `SetKeys` **and `pubProxyReady(nc, true)`** (`:175`). So leaving the not-ready re-push unchanged would re-mark the node `ready=true` every heartbeat while the tunnel is still down → `/sub` ready↔unready flap.
- **R1 is real and DB-triggerable.** `tunnelTokenLookup` (`internal/broker/expose.go:53-78`) folds **every** `port.LookupByTokenHash` error — including a transient SQL fault — into a single `token_unknown_or_revoked` DENY (deliberate anti-enumeration, audit shard 02 F6). `port.LookupByTokenHash` (`internal/port/port.go:259`) DOES return `ErrNotFound` (`port.go:92`) distinctly from a store error, so the transient case is separable without leaking token validity.
- **Broker DENY wire reasons** (the reconnect taxonomy keys off these exact strings): `malformed_register` (`tunnel.go:205`), the `tokenLookup` error string (`:234`, today always `token_unknown_or_revoked`), `public_port_bind_failed` (`:247`), `session_closed` (the pre-OK + post-yamux fences, `:266`/`:295`).
- **Lock-order facts (Fix B deadlock guard).** `AddProxy → client.Open` runs **under `p.mu`** (`proxyStartLocked` at `agent/proxy.go:213`, called while `applyProxyDirective` holds `p.mu` from `:88`). `failClosedFire` holds `p.mu` then calls teardown→`RemoveProxy`→`Client.Close` which takes `c.mu` (`proxy.go:407-416`, `tunnel.go:637-648`). ⇒ global lock order is **`p.mu` → `c.mu`**; the liveness callback must take **neither**.
- **Seam-cost facts.** `tls.DialWithDialer(&net.Dialer{Timeout:5s},…)` is the current non-ctx dial (`tunnel.go:573-574`); `yamux.Server/Client(conn, nil)` (`:277`/`:597`). `NewClient` has 6 call sites (1 prod adapter `agent/tunnel_adapter.go:39` + 5 tests) → a **setter** for the session-state hook beats a constructor param. Agent obtains `nc` at `agent.go:395` (`connectNATS`) and re-obtains it on reconnect (`onNATSReconnect`), but holds no shared handle today → Fix B adds `a.ncBox atomic.Pointer[nats.Conn]`.

### Finalizer decisions / deltas vs the candidate

1. **`public_port_bind_failed` is TRANSIENT (retry), not terminal.** Overturns Draft 5's "any DENY = terminal". It is the dominant reconnect path (broker's old listener mid-teardown races the new `net.Listen`). It cannot resurrect a revoked exit: a revoked/disabled exit is denied at `tokenLookup` (`tunnel.go:231`) *before* `net.Listen` is reached, so a bind failure only ever occurs for a still-legitimate `ALLOCATED` row.
2. **Add Fix C (close R1 at the source).** Split `tunnelTokenLookup`: a real transient store error → a **new transient DENY reason `try_again`**; `ErrNotFound` / sid-nid-port mismatch / proxy-off → keep `token_unknown_or_revoked` (terminal). This preserves anti-enumeration (token validity still collapses to one terminal code; `try_again` fires only on the broker's own DB fault, which an attacker cannot induce) and removes the DB-hiccup-triggers-permanent-death hole. Without it, Fix A would still leave one reachable instance of the exact incident.
3. **Fix D suppresses the not-ready-only keyset nudge** (`repairProxy`). With Fix C in place, a persistently-not-ready node is either (a) transiently retrying (Fix A heals it) or (b) authoritatively terminal (must stay down). So suppressing the nudge is correct in both cases and needs **no stateful broker escalation** (candidate R1 option-b dropped — Fix C subsumes it).
4. **One `sessCtx`/`cancel` per port-generation, for life** (Draft 1 model). Redial swaps only `conn`+`yamuxSess` under `c.mu`; never a new `cancel`. Rejects Draft 3's per-redial `reinstallLocked` (it would let a concurrent `Close` cancel a stale func and orphan the live loop).
5. **R2 (deterministic fast rebind) and R3 (key-rotation-during-outage window) are deferred** to documented follow-ups (§9). The core fix is *correct* without them (heals within the broker's session-detection latency; the rotation window is a pre-existing eventual-consistency property).

---

## 1. Problem restatement

On a flaky/residential link, the long-lived reverse-tunnel TCP to `broker:17000` drops while the **separate** NATS `wss:443` control connection (auto-reconnecting) stays up and the agent process keeps running. The agent's `streamAcceptLoop` returns and never re-dials (Defect A); the broker unbinds the public port; and because `proxy_ready` is a write-once latch nothing clears it, so `proxy status` and `/sub` keep advertising a dead exit (Defect B). Subscribers get connection-refused on that Clash node. The only recovery today is `proxy off`/`on`, a process restart, or a NATS reconnect.

The fix is two primary layers plus two broker-local hardenings, all **additive, no wire-version bump**: a real ctx-anchored, DENY-terminal **reconnect supervisor** (Fix A); a **readiness-tracks-liveness** seam (Fix B); a **transient-DENY split** so a DB hiccup can't false-terminate the loop (Fix C); and a **repairProxy gate** so the heartbeat doesn't fight the liveness signal (Fix D).

---

## 2. Fix A — tunnel reconnect supervisor (`internal/tunnel/tunnel.go`)

### 2.1 State added to `clientSession`

```go
type clientSession struct {
    publicPort int
    localPort  int               // NEW: redial target (was dropped after Open)
    token      string            // NEW: in-memory only; NEVER logged, NEVER persisted by this layer
    conn       net.Conn
    yamuxSess  *yamux.Session
    cancel     context.CancelFunc // ONE per generation, for life (§6)
    gen        uint64            // NEW: stamped at Open; fences a stale supervisor
}
```

`Client` gains `nextGen uint64` (mu-guarded monotonic), `stateHook func(publicPort int, up bool)` (set once before `Start` via a setter), and `backoffBase`/`backoffMax` (defaults 500ms / 30s).

### 2.2 Refactor `Open` → `dialAndRegister` + `supervise`

Extract the dial+REGISTER+yamux body (`tunnel.go:567-601`) into:

```go
func (c *Client) dialAndRegister(ctx context.Context, publicPort, localPort int, token string) (net.Conn, *yamux.Session, error)
```

- **ctx-cancelable TLS dial (mandatory).** Replace `tls.DialWithDialer(&net.Dialer{Timeout:5s},…)` with `(&tls.Dialer{NetDialer:&net.Dialer{Timeout:5s}, Config:clientTLSConfig()}).DialContext(ctx, "tcp", c.brokerAddr)`. A 5s-blocked dial during a `Close`/ctx-cancel would otherwise exceed the 1s goleak poll window.
- **Error taxonomy.** `var ErrRegisterDenied = errors.New("tunnel: broker denied REGISTER")` + `type DenyError struct{ Reason string }` with `Error()` and `Is(target)==ErrRegisterDenied`. On `resp` with prefix `"DENY "`, return `&DenyError{Reason}`; any non-DENY garbage line, or dial/write/read/yamux error, is a plain (transient) error.

`Open` keeps **synchronous first-attempt** semantics (the `AddProxy` rollback contract in `tunnel_adapter.go:56-58` and `expose.go` depend on a first-Open failure propagating): validate `c.ctx`, call `dialAndRegister` once, return its error verbatim on failure. On success, install the `clientSession` under `c.mu` (existing ctx-recheck + old-session replace at `:609-627`), stamp `gen := c.nextGen++`, and spawn exactly one `go c.supervise(sessCtx, publicPort, localPort, token, gen, yamuxSess)` instead of the bare `streamAcceptLoop`. **Reconnect only covers drops after a successful first Open.**

### 2.3 `supervise` control flow

```go
func (c *Client) supervise(ctx context.Context, publicPort, localPort int, token string, gen uint64, initial *yamux.Session) {
    sess := initial
    for {
        c.runAcceptLoop(ctx, publicPort, localPort, sess) // today's streamAcceptLoop body, on THIS *yamux.Session value
        if ctx.Err() != nil { return }                    // Close/RemoveProxy/Start-cancel/fail-closed won → stop, no callback
        c.notifyState(publicPort, false)                  // Fix B: data-plane DOWN (one per drop)
        conn, ys, err := c.redialWithBackoff(ctx, publicPort, token)
        if err != nil {                                   // terminal (DENY) or ctx-cancel
            // The DOWN edge above is already the final state on a terminal exit
            // (no second notifyState needed); drop the slot (cancel+zero token).
            c.dropSession(publicPort, gen)                // under gen-check; calls the per-life cancel
            return
        }
        if !c.swapTransport(publicPort, gen, conn, ys) {  // superseded by a concurrent Open/Close
            _ = conn.Close(); _ = ys.Close(); return      // loser closes its freshly-dialed conn+ys (FD-leak guard)
        }
        sess = ys
        c.notifyState(publicPort, true)                   // Fix B: data-plane UP
    }
}
```

`runAcceptLoop` is the existing `streamAcceptLoop` body (`sess.Accept()` → `bridgeStreamToLocal`), bound to the `*yamux.Session` value passed in — NOT re-read from `c.sessions[port]` per iteration — so the hot path stays lock-free and `-race` clean.

### 2.4 DENY taxonomy (resolves the #1 critic conflict)

| Reason | Class | Why |
|---|---|---|
| `token_unknown_or_revoked` | **terminal** | row gone/mismatch, or proxy-off (authoritative — must not resurrect) |
| `session_closed` | **terminal** | `killGenSession`/pre-OK fence = `proxy off` kill switch |
| `malformed_register` | **terminal** | our own bug; retrying can't fix it |
| ctx-cancel | **terminal** | intentional teardown |
| `public_port_bind_failed` | **transient** | old broker listener mid-teardown races the new bind (dominant reconnect path) |
| `try_again` (NEW, Fix C) | **transient** | broker's own DB fault, not a token verdict |
| dial / write / read / EOF / timeout / yamux-setup error | **transient** | network blip |

**Security argument:** the loop re-binds only on a fresh broker `OK`, which passes `tokenLookup` + the pre-OK fence (`tunnel.go:261`) + the post-yamux install fence (`:295`). The broker's three fences remain the sole resurrection authority; reconnect adds no new path. `public_port_bind_failed` is reached only *after* a successful `tokenLookup`, i.e. for a still-legitimate row.

### 2.5 `redialWithBackoff`

Exponential backoff, full jitter, ctx-anchored, `time.NewTimer`+`Stop` (NOT `time.After` — timer-leak guard at the 30s cap vs the 1s leak poll):

```go
sleep := c.backoffBase
for {
    t := time.NewTimer(jitter(sleep))
    select {
    case <-ctx.Done(): t.Stop(); return nil, nil, ctx.Err()
    case <-t.C:
    }
    conn, ys, err := c.dialAndRegister(ctx, publicPort, localPort, token)
    if err == nil { return conn, ys, nil }
    if errors.Is(err, ErrRegisterDenied) {
        c.logger.Warn("tunnel: reconnect denied, stopping", "public_port", publicPort, "reason", denyReason(err)) // NEVER the token
        return nil, nil, err
    }
    sleep = min(sleep*2, c.backoffMax) // transient → back off, loop (unbounded, ctx-reaped)
}
```

---

## 3. Fix B — readiness reflects data-plane liveness

### 3.1 Signal path (no wire change)

`tunnel.Client.notifyState(port, up)` → adapter pass-through → `a.onTunnelSessionState(port, up)` → `pubProxyReady(nc, up)` → existing subject `proto.SubjEvNodeProxyReady(... ready|unready)` → broker `handleProxyReadyEvent` (**unchanged**; already honors `unready` always and gates `ready` on the switch).

### 3.2 Seam wiring

- **`tunnel.go`:** `func (c *Client) SetSessionStateHook(fn func(publicPort int, up bool))` (stored under `c.mu`, set before `Start`). `notifyState` invokes it nil-guarded **outside `c.mu`**.
- **`tunnel_adapter.go`:** thin pass-through `func (a *TunnelExposeAdapter) SetSessionStateHook(fn …) { a.client.SetSessionStateHook(fn) }`. Policy stays in the agent.
- **`agent` (`proxy.go`/`agent.go`):** add `a.ncBox atomic.Pointer[nats.Conn]` (stored after `connectNATS` at `agent.go:395` and re-stored in `onNATSReconnect`) and `a.proxyPublicPort atomic.Int64` (stored **before** `AddProxy` in `proxyStartLocked` so it's set before the supervisor can fire; cleared to 0 in `proxyTeardownLocked`/`proxyFailCleanupLocked`). The hook:
  ```go
  func (a *Agent) onTunnelSessionState(port int, up bool) {
      if int64(port) != a.proxyPublicPort.Load() { return } // ignore non-proxy expose ports
      if nc := a.ncBox.Load(); nc != nil { a.pubProxyReady(nc, up) }
  }
  ```
  Takes **neither `p.mu` nor `c.mu`** (deadlock guard, §6).
- The agent wires `adapter.SetSessionStateHook(a.onTunnelSessionState)` at construction.

### 3.3 Why the port filter

The incident node plausibly runs both a regular `expose` and the `__proxy__` expose. Only the proxy port's transitions may move `proxy_ready`; a flap on a normal expose must not. The `a.proxyPublicPort` filter enforces this.

---

## 4. Fix C — broker `tunnelTokenLookup` transient split (`internal/broker/expose.go`)

Distinguish a transient store fault from an authoritative token verdict, without leaking enumeration:

```go
a, err := port.LookupByTokenHash(b.cfg.DB, tokenHash)
if err != nil {
    if errors.Is(err, port.ErrNotFound) {
        return fmt.Errorf("token_unknown_or_revoked") // authoritative: absent/revoked → TERMINAL
    }
    b.cfg.Logger.Warn("tunnel: token lookup transient store error", "err", err)
    return fmt.Errorf("try_again")                     // broker DB fault → TRANSIENT (agent retries)
}
// sid/nid/port mismatch and proxy-off paths unchanged → token_unknown_or_revoked (terminal)
```

`port.LookupByTokenHash` already maps "no row" to `ErrNotFound` (`port.go:92`); any other return is a real SQL error. `try_again` only ever fires on the broker's own DB fault — never as a function of token validity — so it cannot be used to enumerate tokens. This closes the R1 hole at its source.

---

## 5. Fix D — `repairProxy` must not nudge a self-healing node (`internal/broker/proxy.go`)

Gate the not-ready re-push so it fires only on a genuine generation/epoch divergence, never *purely* because `proxy_ready==0` at a matching pair:

- The convergence-first early return (`:589`) stays.
- The not-ready branch (`:620-628`) re-pushes the keyset **only when `agentGen != brokerGen || agentEpoch != epoch`**. When the only mismatch is `!ready` at an otherwise-matching pair, **suppress** the push — let the agent's own reconnect (Fix A) be the authoritative `ready` re-publisher.

Rationale: with Fix C, a node stuck `on && !ready` is either transiently retrying (Fix A heals it; a keyset nudge would only re-ACK `ready=true` via the agent `default:` branch and cause the flap) or authoritatively terminal (must stay down). Either way the nudge is wrong. The OFF-repair branch (`:610-617`) is untouched — `proxy off` convergence still works.

---

## 6. Concurrency / locking discipline

- **One ctx/cancel per (port, generation), for life.** Redial swaps only `conn`+`yamuxSess` in place under `c.mu` (`swapTransport`), never `cancel`/`ctx`. `Client.Close` (`tunnel.go:645`) and `Start`-cleanup (`:548-552`) call `sess.cancel()` as the sole stop signal; a per-redial new-cancel would orphan the live loop on a concurrent `Close`.
- **Generation fence.** A concurrent `Open(port)` (from `applyProxyDirective` full rebuild) bumps `c.nextGen`, cancels the old session (old supervisor's `ctx.Done` → returns), installs a fresh session+gen+supervisor. `swapTransport(port, gen, …)`/`dropSession(port, gen)` mutate/delete **only if** `c.sessions[port].gen == gen`; the redial that lost the race closes its freshly-dialed `conn`+`ys` (no FD leak) and returns. No two supervisors ever own one port.
- **Lock order `p.mu` → `c.mu`** globally. The liveness hook fires on the supervisor goroutine and takes neither lock (reads `a.proxyPublicPort`/`a.ncBox` atomics, calls lock-free `pubProxyReady`). Mandatory because `AddProxy → Open` runs under `p.mu`.
- **Per-edge firing is loop-structural (no dedup variable).** The supervisor fires exactly one `notifyState(false)` per drop (right after `runAcceptLoop` returns) and one `notifyState(true)` per successful reconnect; they strictly alternate by construction — there is no separate `lastReported` bool. (As-built refinement vs an earlier draft that proposed a dedup flag; the internal review confirmed the loop structure is sufficient and the flag would be dead code. The cross-network publish is still throttled by the ≥500ms backoff floor.) No shared-mutable state ⇒ no `-race` finding.
- **fail-closed interplay.** A tunnel-only drop does NOT arm fail-closed. If NATS also drops, `failClosedFire → teardown → RemoveProxy → Client.Close(port)` cancels the supervisor ctx → backoff `select` hits `ctx.Done` and `runAcceptLoop` exits. No zombie redial against a torn-down SS server.
- **Broker side.** No fence change. Every redial is a fresh `handleAgent` under the same fence battery (incl. b9fb097). `publicAcceptLoop`'s `cur == sess` guard (`tunnel.go:337`) stops an old session's teardown from deleting a newer one; the `CloseChan` watcher closing an already-replaced `old.listener` is a harmless no-op; `inflightBySID` stays bounded (sequential per port).

---

## 7. Test plan (table-driven; `-race` + `goleak` on every tunnel/concurrency touch)

**`internal/tunnel` (new `export_test.go` seam):** `func (s *Server) DropTransport(port int) bool` (closes only the raw conn of a live session — a residential blip — WITHOUT advancing killGen) and `func (c *Client) SessionUp(port int) bool` for deterministic polling.
- `TestTunnelReconnectsAfterTransportDrop` — table `{1 drop, 3 drops}`; assert re-REGISTER count == 1+drops, public port re-binds, GET body returns, and a GET during the unbound window fails (proves the drop was observed).
- `TestTunnelReconnectStopsOnDeny` — table over `{token_unknown_or_revoked, session_closed, malformed_register}`; lookup flips to deny after first OK; assert bounded REGISTER count (no busy-loop), `SessionUp` stays false, `onSessionState(false)` fired exactly once, slot dropped, token zeroed.
- `TestTunnelReconnectBindFailedIsTransient` — broker returns `public_port_bind_failed` then `OK`; assert the loop retries and recovers.
- `TestTunnelReconnectTryAgainIsTransient` — broker returns `try_again` then `OK`; assert retry + recover (Fix C).
- `TestTunnelReconnectCtxCancelInterruptsBackoff` — drop into backoff, cancel `Start` ctx; assert prompt exit (proves `NewTimer`+`ctx.Done`, and `DialContext` cancels an in-flight dial).
- `TestTunnelReconnectFiresSessionStateCallback` — assert ordering `(port,true)`→`(port,false)`→`(port,true)`.
- `TestTunnelReconnectNeverLogsToken` — slog capture across drop→retry→DENY; token never appears.

**`test/concurrency` (`-race` + `goleak`; RE-BASELINE for the new steady-state +1 supervisor goroutine per open port — sample baseline before Open, assert return only after Close+ctx-cancel+poll):**
- `TestTunnelReconnectChurnNoGoroutineLeak` — 50 drop/reconnect cycles → `assertNoGoroutineLeak`.
- `TestTunnelReconnectVsConcurrentOpenSamePort` — drop+redial racing a concurrent `Open(port)`; assert exactly one live supervisor + one live yamux + loser's conn closed.
- `TestTunnelReconnectVsClose` — `Close(port)` during backoff; assert no re-insert, supervisor exits.

**`internal/agent` (`-race`):**
- `TestProxyReadinessReflectsTunnelDrop` — proxy port drop → `pubProxyReady(false)`, recover → `(true)`; a non-proxy expose drop does NOT fire the proxy hook (port filter).
- `TestProxyReadinessHookNoDeadlockUnderConcurrentDirective` — fire `onTunnelSessionState` while `applyProxyDirective` holds `p.mu` (AddProxy in flight); a hang fails the test (guards the `p.mu` deadlock).

**`internal/broker` (table-driven):**
- `TestRepairProxyNoStormWhenOnNotReadyAtSamePair` — `on=1, ready=0`, gens match → assert ZERO keyset re-push across N heartbeats (Fix D).
- `TestRepairProxyPushesOnGenEpochMismatch` — gen/epoch differ → assert push.
- `TestTunnelTokenLookupTransientIsTryAgain` / `…AbsentIsRevoked` — Fix C taxonomy (inject a store error vs `ErrNotFound`).

**`test/p13` (full stack via the REAL `TunnelExposeAdapter`):**
- `TestProxyFalseOnlineRecoversAfterTunnelDrop` — drop broker-side transport; during outage assert `proxy_ready→false` + `LiveProxyNodes` drops the node + public dial refused; after recovery assert `→true` + re-listed + bytes flow; **negative control:** `nc.Stats().Reconnects==0` and ctx never canceled (recovery via tunnel reconnect, NOT NATS/replay).
- `TestProxyDisableDuringTunnelDropStaysDown` — drop transport + owner `proxy off` (the REAL `disableProxy`: `CloseSession(sid)`+`port.Free`); assert node stays out of `/sub`, public port stays refused, reconnect gets DENY and the supervisor exits.

**`test/e2e/all_phases_test.go`:** add `TestProxyResilienceMatrix` running the tunnel-reconnect, concurrency, and p13 subsets under `-race`, mirroring `TestRemoteFSMatrix`, so a regression fails `make e2e`.

---

## 8. Scope / non-goals / wire-compat

- **In scope:** `internal/tunnel/tunnel.go` (clientSession fields; `DenyError`/`ErrRegisterDenied`; `dialAndRegister`; `supervise`/`runAcceptLoop`/`redialWithBackoff`; `swapTransport`/`dropSession`; `SetSessionStateHook`/`notifyState`; ctx-cancelable dial; doc fixes), `internal/agent/tunnel_adapter.go` (hook pass-through), `internal/agent/proxy.go`+`agent.go` (`ncBox`/`proxyPublicPort` atomics, `onTunnelSessionState`), `internal/broker/expose.go` (Fix C), `internal/broker/proxy.go` (Fix D), `export_test.go` seams.
- **Non-goals / wire-compat:** no `proto.ProtoVersion` bump; reuses the existing `proxy.ready`/`proxy.unready` subjects; reuses the REGISTER line grammar (the new `try_again` is a tunnel control-line DENY reason, not a `proto` wire message — additive, old agents simply treat any unknown DENY as terminal, which is the safe pre-fix behavior); no SS-server reconfiguration on reconnect (egress ACL `DenyPrivateDestinations` set once at `proxyStartLocked:193-194`, untouched — pinned by a test).
- **Doc fix:** correct `tunnel.go:33-36` + `:509` — delete the false `ConnectAll()` / "Reconnects on session loss" claims; describe the real supervisor + gen-fence + DENY-terminal reconnect.

---

## 9. Risks & deferred follow-ups

- **R2 — broker old-listener reaping latency (deferred).** A half-open residential TCP can leave the broker's old yamux session alive for the yamux keepalive window, during which redials get `public_port_bind_failed` and (correctly) retry — so a half-open drop heals in up to ~one keepalive interval rather than instantly. *Follow-up options:* close the old session's listener BEFORE `net.Listen` on a same-port REGISTER (touches the fence ordering — highest review risk), or tune the broker `yamux.Server` config keepalive down from the 30s default (low-risk config change). Not required for correctness; flagged for the internal review to weigh.
- **R3 — key rotation during outage (accept).** If a sub is revoked (epoch bumped) while the tunnel is down, the agent's SS server still holds old keys; on reconnect it publishes `ready` before the next heartbeat's keyset re-push corrects it — a brief, heartbeat-bounded window. Pre-existing eventual-consistency property; Fix B shrinks but does not eliminate it. Documented, not gated.
- **Open question for review:** backoff knobs (500ms / 30s / ×2 / full-jitter) — reuse `cfg.RegisterRetry*` or tunnel-local constants with an adapter override? Recommend tunnel-local with override.

**Verdict:** four-part additive fix (A reconnect + B liveness + C transient-DENY split + D repairProxy gate), no wire bump, shipping in one PR. Fix A and Fix B MUST ship together (B alone = auto-eject with no auto-re-add). The three critic conflicts are resolved in §0; R1 is closed by Fix C; R2/R3 are deferred with rationale.
