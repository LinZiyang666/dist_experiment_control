# Concurrency & Resource Lifecycle Audit

Scope: `internal/broker/*.go`, `internal/agent/*.go`, `internal/tunnel/*.go`,
`internal/adminsock/server.go` (read-only review).

## Verdict

15 findings (1 critical, 5 high, 7 medium, 2 low)

## Findings

### F1 — [critical] Tunnel server: agent control connections accepted after ctx cancel leak forever
**Where**: `internal/tunnel/tunnel.go:140-152` (`acceptLoop`) + `tunnel.go:157-241` (`handleAgent`) + `tunnel.go:125-135` (Start cleanup goroutine).

**Issue**: Three independent goroutines race during shutdown:
1. `acceptLoop` calls `ln.Accept()`. On error it checks `ctx.Err() != nil` to exit, otherwise `continue`s.
2. The Start cleanup goroutine (line 125) closes the listener and walks `s.sessions` once under `s.mu`.
3. Each accepted conn is dispatched into `go s.handleAgent(ctx, conn)`. `handleAgent` does TLS read of the REGISTER line, calls `tokenLookup`, binds the public port, then locks `s.mu` and inserts into `s.sessions`.

A REGISTER conn that arrived just before `ctx.Done` and is still inside `handleAgent` (waiting on the 5s read deadline, or in `tokenLookup`, or `net.Listen` for the public port) will lock `s.mu` AFTER the cleanup goroutine has already iterated and inserted itself into `s.sessions`. From that point nothing ever closes it: `acceptLoop` may have already returned, the Start cleanup goroutine ran exactly once, and there is no per-session ctx watcher (`yamuxSess.CloseChan` watcher at `tunnel.go:234` is started but only fires on remote close).

Additionally `acceptLoop` has no goroutine-tracking; it spawns `handleAgent` goroutines without a WaitGroup, so `Server.Close` returns immediately and callers cannot tell when the server is fully drained.

**Impact**: In production this only bites on broker shutdown — leaked TCP fd + bound public port + half-open yamux session, every shutdown that overlaps with an in-flight agent reconnect. In tests with rapid Start/Stop cycles you accumulate sockets and bound ports until ulimit hits. The bound public port matters: if a new broker starts on the same machine before the kernel reaps it, `net.Listen` on the same port returns EADDRINUSE.

**Fix**: Wrap `handleAgent` insertion in a "did Close already happen" check: e.g. add `s.closed bool` to Server, hold `s.mu`, if closed → drop the conn instead of inserting. Bonus: track in-flight handleAgent goroutines via `sync.WaitGroup` and have `Close()` `wg.Wait()` after canceling so `Close()` is meaningfully synchronous.

### F2 — [high] Tunnel client: `Open` after Start's ctx cancellation leaks a session forever
**Where**: `internal/tunnel/tunnel.go:331-344` (`Start`) + `tunnel.go:350-409` (`Open`).

**Issue**: `Start` spawns one goroutine that runs once on `<-c.ctx.Done()`, drains `c.sessions`, and exits. After that goroutine has run, no future cleanup happens. `Open` is independent — it dials, registers, and inserts into `c.sessions` with no check that `c.ctx` is still live. The only "guard" is `if c.ctx == nil`, which is the wrong question.

A caller pattern that triggers this: agent `Run` cancels ctx (e.g. via `agent_evicted`), the tunnel client cleanup runs, then a still-in-flight `expose.req.forwarded` handler calls `ExposeAdapter.AddProxy → tunnel.Client.Open`. The new clientSession is inserted into the now-permanently-empty map. `streamAcceptLoop` is started with `sessCtx` derived from already-canceled `c.ctx` so it exits immediately, but the TCP/TLS conn + yamux session in the map are never closed.

**Impact**: Every shutdown that races with an in-flight expose leaks one TLS conn + one yamux session per expose verb. In normal long-lived agent operation this never triggers; in tests with frequent restarts and in production during operator-initiated `tether admin evict`, it accumulates.

**Fix**: In `Open`, after acquiring `c.mu`, check `select { case <-c.ctx.Done(): return c.ctx.Err() default: }` before inserting. Even better, replace the one-shot cleanup goroutine with a "session inserter" that acquires mu and verifies ctx is alive.

### F3 — [high] `Server.Close()` does not close the control listener
**Where**: `internal/tunnel/tunnel.go:282-291`.

**Issue**: `Close()` walks `s.sessions` and closes per-session resources, but never closes the control listener `ln` from `Start`. The only path that closes `ln` is the Start cleanup goroutine triggered by `ctx.Done`. If a caller invokes `Close()` without canceling ctx (a pattern the API explicitly invites — the comment says "Idempotent"), the control listener and its `acceptLoop` goroutine survive forever, continuing to accept new agent connections that will then race with the half-closed Server.

**Impact**: API misuse silently leaks the bound `:7000` port and an accept-loop goroutine. Worse, newly-accepted agent connections will REGISTER successfully and bind public ports against a Server whose session map has just been reset to `map[int]*serverSession{}`, leaving the new sessions discoverable but with the original ones (held by the cleanup goroutine) gone — split-brain.

**Fix**: Have Server keep a reference to the listener (`s.ln net.Listener`), close it from `Close()`, and have the ctx-Done cleanup goroutine call `s.Close()` instead of duplicating the cleanup inline.

### F4 — [high] `Agent.killOrphanProcess` SIGKILL goroutine outlives ctx
**Where**: `internal/agent/agent.go:526-541`.

**Issue**: `killOrphanProcess` sends SIGTERM, then `go func() { time.Sleep(5*time.Second); ... SIGKILL }()`. The goroutine has no ctx parameter. If the agent is shutting down (parent ctx canceled) within those 5 seconds — common during eviction (which is what triggers killOrphanProcess via reconciliation) — the SIGKILL will still fire after the agent has otherwise drained. In a test harness that re-runs the agent in-process this can SIGKILL a process the next test iteration is depending on; in production it fires after `nc.Drain()` so there's nothing to log it.

**Impact**: Test harness flakiness; production: a 5s window where the agent's defers have run but a goroutine survives. The lookupProc check inside the goroutine guards against most damage, but the orphan PID is checked against the current `a.procs` — if that map has been mutated by a fresh PID (PID collision between separate runs of the same test) it kills the wrong process.

**Fix**: Take ctx as parameter, replace `time.Sleep` with `select { case <-time.After(5*time.Second): case <-ctx.Done(): return }`. Track outstanding kill goroutines via a WaitGroup that Run waits on before returning.

### F5 — [high] `agent.handleRunForwarded` has no ctx; PTY runs leak past agent shutdown
**Where**: `internal/agent/run.go:51-196` (`handleRunForwarded`) + `agent/exec.go:30-58` (`dispatchForwarded`).

**Issue**: `dispatchForwarded` spawns `go a.handleExecForwarded(...)`, `go a.handleRunForwarded(...)`, etc. None of these handlers take ctx. `handleRunForwarded` blocks on `sess.Wait()` (line 168) until the child process exits naturally; `handleExecForwarded` blocks on `cmd.Wait()` (exec.go:136) similarly. The two long-running NATS subscriptions opened inside (`pty.in`, `pty.resize` at run.go:144-159) are also tied to `sess.Wait()` returning.

If the agent's parent ctx is canceled (eviction, SIGTERM via supervisor) while a `tether run sleep 3600` is in flight, `Run` returns from `heartbeatLoop`, defers fire (Unsubscribe of `subFwd` / `subEvict`, Drain), but `handleRunForwarded` keeps the child running, keeps `pty.in/.resize` subscriptions live, and keeps publishing on `pty.<pid>.out` against a draining nats.Conn. The OS process exit normally reaps everything, but in unit tests that exercise multiple agents in one process (`agent_test.go` does this), the leaked subscriptions are still dispatching messages.

**Impact**: Test pollution: a follow-up test sending on `pty.<pid>.in` may hit the leaked handler from the previous test. Production: child process orphaned past supervisor restart window — when systemd restarts the agent, the old child is reparented to PID 1 instead of being SIGTERM'd, contradicting the architecture G.1 invariant that the new agent's reconcile sees actual processes.

**Fix**: Pass a `ctx` argument all the way down. In `handleRunForwarded`, when ctx is canceled, send SIGTERM to `sess` and proceed to cleanup (don't wait for child). Track in-flight handlers via WaitGroup so `Run` waits for them before returning.

### F6 — [medium] `adminsock.acceptLoop` ctx-watcher goroutine leaks if Close() is called before ctx cancel
**Where**: `internal/adminsock/server.go:156-172`.

**Issue**: `acceptLoop` first launches an inner goroutine `go func() { <-ctx.Done(); _ = s.Close() }()`. If shutdown is initiated by an explicit `s.Close()` (which the broker does via `defer b.admin.Close()` in `broker.go:430` — runs before ctx cancellation when Run returns due to a different defer), `acceptLoop` exits cleanly on `net.ErrClosed`, but the inner ctx-watcher goroutine sits forever waiting on `<-ctx.Done()`.

**Impact**: One leaked goroutine per Server.Start lifetime. Low impact in production (one broker = one server) but accumulates across in-process tests that repeatedly start/stop brokers and never trigger ctx cancel.

**Fix**: Make the ctx-watcher exit when the listener closes too: `select { case <-ctx.Done(): _ = s.Close(); case <-doneCh: }`, where `doneCh` is closed when `acceptLoop`'s `for` loop returns. Or simpler: don't race them — let `Close()` cancel an internal ctx.

### F7 — [medium] `handleSessionRm` finalize uses `context.Background()` instead of broker ctx
**Where**: `internal/broker/sessions.go:165-170`.

**Issue**: `finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)` creates a fresh root ctx. The broker's Run ctx (and any propagation of "broker shutting down") is invisible to `finalizeSessionRm`. If a session rm arrives concurrently with broker shutdown, the JS DeleteHistoryStream + SQLite drop run unaware that everything below them is being torn down.

The same pattern recurs in `broker.go:239` (`publishAudit` builds `context.Background()`-derived ctx) and is structurally fine for short-lived publish, but `finalizeSessionRm` is the long, multi-RPC one where ctx propagation matters.

**Impact**: During a graceful shutdown initiated mid-rm, the operator's `tether session rm` returns OK after 5s instead of being canceled; meanwhile the broker is past Drain and the JS Publish errors are logged with `nil`-like state. Worst case the SQLite drop succeeds but the JS DeleteHistoryStream lands against a closed `b.js` (panic? error? — depends on jetstream lib internals).

**Fix**: Plumb the broker Run ctx into `Broker` (`b.runCtx context.Context`, set in Run) and derive finalize timeouts from it: `context.WithTimeout(b.runCtx, 5*time.Second)`.

### F8 — [medium] `handleProcEvent` and `handlePtyFailed` use `time.Now()` instead of `b.cfg.Now()`
**Where**: `internal/broker/exec.go:167` (handleProcEvent indirectly via `pubAuditProc(... ev.EndedAt)`), `internal/broker/run.go:212` (`handlePtyFailed`).

**Issue**: `handlePtyFailed` calls `b.pubAuditProc(sid, ev.Reason, "" , ev.PID, nil, 0, time.Now().UTC())` — it should use `b.cfg.Now()`. `Config.Now` is the entire reason the field exists (deterministic time in tests, line 50 of broker.go). The same drift exists in any path that hand-codes `time.Now()` instead of going through `cfg.Now`.

**Impact**: Test flakiness. Under a frozen `cfg.Now`, the audit row's `ts` jumps to wall clock, breaking time-ordering assertions. Not a concurrency hazard but a hygiene bug in code that runs in the same review scope.

**Fix**: Replace `time.Now().UTC()` with `b.cfg.Now()` in handlePtyFailed and audit anywhere else `time.Now()` slipped in (rg `time\.Now\(\)` under `internal/broker`).

### F9 — [medium] `tunnel.Server.Start` cleanup goroutine holds `s.mu` while doing per-session cancel/Close — serializes on slow Close
**Where**: `internal/tunnel/tunnel.go:125-135`.

**Issue**: The cleanup goroutine does `s.mu.Lock()` then iterates ALL sessions, calling `sess.cancel()`, `sess.listener.Close()`, `sess.rawConn.Close()` for each, then `s.mu.Unlock()`. While holding mu, every concurrent `bridgePublicToYamux` (acquiring mu for `s.sessions[]` lookup — actually it doesn't, but `publicAcceptLoop`'s defer at line 248 does) and any in-flight `handleAgent` blocks. With many sessions this is a long critical section that can rope in dozens of goroutines.

More importantly: `sess.listener.Close()` synchronously waits for `accept` to return on Linux, and `sess.rawConn.Close()` on a TLS conn can block on the close_notify exchange. Doing these under mu means a single slow close stalls every other shutdown and every concurrent `handleAgent` insert.

**Impact**: Shutdown latency proportional to active session count. Tests with many sessions occasionally see "test took 10s" warnings during cleanup.

**Fix**: Take a snapshot of sessions under mu, drop mu, then close sessions outside the lock:
```go
s.mu.Lock()
snap := make([]*serverSession, 0, len(s.sessions))
for _, sess := range s.sessions { snap = append(snap, sess) }
s.sessions = map[int]*serverSession{}
s.mu.Unlock()
for _, sess := range snap { sess.cancel(); _ = sess.listener.Close(); _ = sess.rawConn.Close() }
```

### F10 — [medium] `agent.Run` heartbeat ctx and sub ctx mismatch — eviction triggers heartbeat shutdown but not handler shutdown
**Where**: `internal/agent/agent.go:229-267`.

**Issue**: `runCtx` is created (`context.WithCancel(ctx)`), and the `subEvict` handler calls `cancelRun()`. `runCtx` is then passed only to `heartbeatLoop`. The forwarded-message dispatcher (`subFwd`) and any in-flight handlers it spawned do not see `runCtx`. So an `agent_evicted` event:

1. Triggers `cancelRun()` → `heartbeatLoop` exits → `Run` returns → defers fire → `subFwd.Unsubscribe()`, `subEvict.Unsubscribe()`, `nc.Drain()`.
2. In-flight `handleRunForwarded` / `handleExecForwarded` / `handleUpgradeForwarded` continue to run with no signal that the agent has been told to leave. `handleUpgradeForwarded` is particularly bad — its 100ms-delayed `syscall.Exec` (upgrade.go:127) fires AFTER the agent was supposed to be evicted, replacing the agent binary even though the operator just kicked it out.

**Impact**: An evicted-during-upgrade agent re-execs itself into the new binary anyway, then re-registers — directly violating the P9 "evict means down within 1s" budget.

**Fix**: Plumb `runCtx` into all spawned handlers. At minimum, gate the `syscall.Exec` in `handleUpgradeForwarded` on a select against ctx. Generally: `dispatchForwarded` should receive `runCtx` and pass it through.

### F11 — [medium] `handleSessionList` ignores the per-iteration `IsOwner` error
**Where**: `internal/broker/sessions.go:100-110`.

**Issue**: `isOwner, _ := session.IsOwner(b.cfg.DB, s.SID, fp)` swallows the error. If SQLite returns an error mid-iteration (db locked, schema drift, etc.), `isOwner` defaults to false, and the response silently mislabels which sessions the caller owns. This is not a leak/race per se but it's the canonical "ignored Close-equivalent error" pattern the task asks for.

**Impact**: Caller's `tether session list` shows sessions as not-owned when in fact they are; subsequent owner-only operations (`session rm`, `node upgrade`) then fail with `not_owner` and the user has no clue why. Cosmetic in normal operation; misleading during DB-pressure incidents.

**Fix**: Either propagate the error (make the whole list call fail) or log the error and mark the entry with a clear "owner_unknown" marker. Don't silently set `isOwner=false`.

### F12 — [medium] `disk.go` monitor's first `check()` runs before `<-ctx.Done()` is selectable
**Where**: `internal/broker/disk.go:58-99`.

**Issue**: The goroutine does `check()` once, then enters the `select`. If `ctx` is already canceled at goroutine start, the initial `check()` still runs (which calls `usageFn`, possibly blocking on `syscall.Statfs` — which can block on a hung NFS mount). There is no upper bound on Statfs.

The bigger issue: `pubSysEvent` inside `check()` writes to `b.publishAudit` which sets up its own 2s ctx with `context.Background()` — the disk-pressure event fires during shutdown after `nc.Drain()` may have started. JS `Publish` against a draining conn returns an error, which the monitor logs as a warn but otherwise ignores; OK behaviorally but worth noting.

**Impact**: Shutdown can stall up to one Statfs syscall. On hung NFS that's "minutes". Otherwise harmless.

**Fix**: Move the initial `check()` inside the for-select with a `time.NewTimer(0)` "fire immediately" pattern, so ctx cancellation preempts the first probe.

### F13 — [low] `Client.Start` racy access to `c.ctx` from `Open`
**Where**: `internal/tunnel/tunnel.go:331-344` (`Start`) + `tunnel.go:351` (`Open` reads `c.ctx`).

**Issue**: `Start` writes `c.ctx, c.cancel = context.WithCancel(ctx)` without holding any mutex. `Open` reads `c.ctx` (line 351 nil check, line 389 use). The only synchronization is the documentation that says "Caller MUST call Start before AddProxy/Open". A concurrent Start+Open from two goroutines (agent Run + reconcile callback during early init) is technically a data race (`go test -race` would flag it).

**Impact**: Theoretical; in practice agent.Run calls Start synchronously before exposing the adapter. Race detector noise if anyone ever reorders things.

**Fix**: Either guard with `c.mu` or initialize `c.ctx` in `NewClient` instead of `Start`.

### F14 — [low] `Client.Close(publicPort)` reads then deletes, racing with `Start` cleanup
**Where**: `internal/tunnel/tunnel.go:413-424`.

**Issue**: `Close` does `s.mu.Lock(); sess, ok := c.sessions[pp]; delete(c.sessions, pp); c.mu.Unlock()` — fine in isolation. But the Start cleanup goroutine at line 333 walks `c.sessions` under mu, then assigns `c.sessions = map[int]*clientSession{}` (line 341). If `Close(pp)` runs between two Start-cleanup iterations (it can't — same mu), no race. Race-free, but if a future change allows Close to be called without holding mu (e.g. a fast-path), the pattern breaks. Worth a comment.

**Impact**: None today; low-priority maintainability concern.

**Fix**: Add a comment near `c.sessions = map[int]*clientSession{}` noting the only-under-mu invariant.

### F15 — [nit] Subscription deferred-Unsubscribe in for-loop relies on capturing parameter trick
**Where**: `internal/broker/broker.go:373`.

**Issue**: `defer func(s *nats.Subscription) { _ = s.Unsubscribe() }(sub)` inside a for-loop is correct (parameter capture pins each `sub`), but it's a known Go-gotcha pattern that future readers may "fix" to `defer sub.Unsubscribe()` and silently break. Worth a one-line comment about WHY the parameter is there. Same shape repeats in agent.go:222/252.

**Impact**: None unless someone refactors.

**Fix**: Add a comment, or use `defer cleanup(sub)` with a named helper that makes the capture explicit.

---

## Notes on what was checked but found clean

- `broker/broker.go` Run defer ordering: subscription Unsubscribes run before `nc.Drain()`, which is the correct order; auth callout sub registered conditionally is also correctly defer-Unsubscribed.
- `broker/sessions.go` `dropSessionRows` correctly defers `tx.Rollback()` after `Begin`, and Commit's error is returned (not ignored).
- `broker/admin.go` `adminAuditTail` GetMsg-in-loop with ctx cancellation correctly fails fast on each iteration (no infinite loop).
- `agent.go` `procs` map access is consistently guarded by `procsMu` (registerProc / unregisterProc / lookupProc / buildLocalSnapshot all hold the lock).
- `agent/state.go` stateStore correctly serializes via its `mu` and uses tmp+rename atomic writes.
- `agent/run.go` `pumpMasterToBus` reader goroutine correctly returns on EOF with no orphan-blocked send (the inner goroutine exits the same iteration that sets the err that makes the outer loop return).
- `tunnel.go` `bridge()` correctly closes both sides on either-direction termination; the buffered done channel with size 2 prevents the late-finishing goroutine from blocking.
- `adminsock/server.go` Close() is correctly idempotent under `s.closed` flag.
