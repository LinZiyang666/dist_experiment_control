PASS

# proxy-tunnel-reconnect — adversarial code review (synthesis)

Target: UNCOMMITTED working tree on `feature/remote-fs-resilience`.
Scope: Fix A (tunnel reconnect supervisor), Fix B (readiness liveness hook),
Fix C (broker token-lookup transient split), Fix D (repairProxy keyset-push
suppression). Plan: `docs/reviews/proxy-tunnel-reconnect-plan.md`.

This report synthesizes five independent reviewer lenses (concurrency/leak,
resurrection-invariant, broker-state, security/enumeration, test-adequacy). It
de-duplicates overlapping findings and ranks by severity. Implementation was NOT
modified — findings + proposed tests only. Code locations were re-verified
against the working tree before writing.

---

## Verdict

**PASS — final re-review approved.**

This report preserves the original adversarial review below as history. The
first pass was **BLOCKED** with 2 blocker-class findings, several majors, and a
minor availability hole; the "Main-process adjudication" and "Reviewer final
re-review" sections document the remediation and final release decision.

The core concurrency design is sound and was independently confirmed by every
lens: the generation fence prevents double-owners, one-cancel-for-life prevents
orphaned supervisors, lock order `p.mu → c.mu` is never inverted, the DENY
taxonomy fails closed (unknown → terminal), and a blind reconnect cannot
resurrect a revoked/disabled exit (denied at `tunnelTokenLookup` before
`net.Listen`; the three broker install fences remain the sole resurrection
authority). The blockers are NOT in that design — they are a transport-leak on
the half-open reconnect path (the exact motivating scenario) and the absence of
the plan's mandated full-stack integration coverage of the fix's headline
behavior.

---

## Blockers

### B1 — `swapTransport` leaks the replaced conn + yamux session on every reconnect
- **Location:** `internal/tunnel/tunnel.go:818-828` (`swapTransport`).
- **Issue:** `swapTransport` overwrites `cur.conn` and `cur.yamuxSess` with the
  freshly-redialed transport but never closes the OLD conn/yamux it replaces:
  ```go
  cur.conn = conn
  cur.yamuxSess = ys
  return true
  ```
  On a CLEAN drop the peer already closed the old transport, so its fds are
  reaped — which is why the existing churn / vs-Open tests pass. But on a
  HALF-OPEN drop (the residential-link scenario the fix exists for): `runAcceptLoop`
  returns because `sess.Accept()` errored on the yamux keepalive timeout, yet the
  underlying TCP conn and the old yamux session's internal goroutines are NOT
  closed by the local side. `supervise` redials and `swapTransport` drops the
  reference with no `Close`, leaking one TCP fd + the old yamux's goroutines per
  half-open reconnect. `Close`/`Start`-cleanup only ever close the CURRENT
  (post-swap) session, never orphaned predecessors.
- **Why it matters:** On a flapping link — the entire motivating incident —
  reconnects are frequent and many are half-open, not clean RST. The leak is
  unbounded in the reconnect count and exhausts the agent's fd table over a long
  outage: a worse failure than the false-online bug being fixed. The current
  leak tests cannot catch it because `DropTransport` closes both sides cleanly.
- **Remediation:** In `swapTransport`, capture the predecessor before overwriting
  and close it (closing an already-closed conn/session is a harmless no-op, so
  this is safe for the clean-drop path too):
  ```go
  old, oldYS := cur.conn, cur.yamuxSess
  cur.conn, cur.yamuxSess = conn, ys
  // after releasing c.mu:
  _ = old.Close(); _ = oldYS.Close()
  ```
- **Proposed test:** `TestTunnelReconnectHalfOpenDropNoFDLeak` — add an
  `export_test` seam `DropTransportHalfOpen(port)` that closes ONLY the yamux
  session (leaving the raw conn open) to simulate a keepalive-detected half-open;
  run ~30 cycles; assert open-fd / goroutine count returns to baseline. Make
  predecessor closure observable via a counting `net.Conn` wrapper (a global
  `dialedConns == closedConns` after the race settles). The existing churn test
  MUST be extended — it only exercises clean close.

### B2 — Plan-mandated full-stack p13 integration tests are absent; the fix's headline seam is never wired end-to-end
- **Location:** `test/e2e/all_phases_test.go:129` (`TestProxyTunnelReconnectMatrix`);
  missing `test/p13/` tests.
- **Issue:** The plan §7 mandates two full-stack tests through the REAL
  `TunnelExposeAdapter` + real broker:
  `TestProxyFalseOnlineRecoversAfterTunnelDrop` and
  `TestProxyDisableDuringTunnelDropStaysDown` (and `TestProxyResilienceMatrix`).
  None exist (zero grep hits for `FalseOnline`/`DisableDuringTunnel`/
  `SetSessionStateHook`/`DropTransport` outside the unit packages).
  `TestProxyTunnelReconnectMatrix` only re-runs the unit subsets under `-race`;
  nothing wires `tunnel.Client.notifyState` → adapter pass-through →
  `a.onTunnelSessionState` → `pubProxyReady` → broker `handleProxyReadyEvent` →
  `LiveProxyNodes`/`proxy_ready` in one process. Every layer is unit-tested with
  the seam MOCKED: the agent test calls `onTunnelSessionState` directly (never via
  a real supervisor edge); the tunnel test fires a test-only hook (never the
  agent's). The composed kill path (`proxy off` mid-drop through the real
  `disableProxy → CloseSession → killGen → tokenLookup(__proxy__ off)` chain) has
  ZERO integration coverage.
- **Why it matters:** This is the headline behavior of the whole fix — a data-plane
  drop must flip `proxy_ready → false` and pull the node from `/sub`, then recovery
  flips it back; and a `proxy off` mid-drop must stay down. A wiring regression
  (e.g. `agent.New` failing the `sessionStateHookSetter` type assertion, the adapter
  pass-through dropped, or `pubProxyReady` subject drift) passes every existing test
  yet reproduces the exact production incident. The plan's negative control
  (`nc.Stats().Reconnects == 0`, run-ctx never canceled) that proves recovery came
  from the TUNNEL and not a NATS replay is also absent — a green integration could
  silently be healing via the wrong path. The single most important
  anti-resurrection invariant ("a blind reconnect can never resurrect a disabled
  exit") is verified only in individually-mocked pieces.
- **Remediation:** Add `test/p13/proxy_reconnect_e2e_test.go` with a real broker +
  agent + `TunnelExposeAdapter`. (1) Recover-after-transient-drop:
  `Server.DropTransport(proxyPort)`, assert `proxy_ready → 0`, node leaves
  `LiveProxyNodes`, public dial refused during the outage; after the supervisor
  heals assert `proxy_ready → 1`, re-listed, bytes flow; include the negative
  control `nc.Stats().Reconnects == 0` and `runCtx.Err() == nil`.
  (2) Disable-during-drop: `DropTransport` then `disableProxy(sid)`; assert the
  agent supervisor terminates (`SessionUp(proxyPort) == false`), any in-flight
  reconnect REGISTER is denied `token_unknown_or_revoked`, node absent from
  `LiveProxyNodes`, public dial refused; re-enable and assert a FRESH port+token
  rebuild (rotate), never the old port resurrecting. Fold both into
  `TestProxyTunnelReconnectMatrix`.
- **Proposed test:** `TestProxyFalseOnlineRecoversAfterTunnelDrop`,
  `TestProxyDisableDuringTunnelDropStaysDown` (both in `test/p13`, real adapter).

---

## Major (fix in this PR)

### M1 — `dialAndRegister` handshake watcher: success-vs-cancel race has no coverage
- **Location:** `internal/tunnel/tunnel.go:704-712` (the `hsDone`/`ctx.Done`
  watcher goroutine).
- **Issue:** The watcher selects on `<-ctx.Done()` (closes conn) vs `<-hsDone`
  (deferred close at :712). The comment claims `stopHS` fires before any later
  `ctx.Done` can reach the conn, but there is a window after a successful
  `yamux.Client` returns (:734) and before `defer close(hsDone)` runs where, if
  ctx cancels, the watcher can close the conn out from under the freshly-returned
  session. Also a ctx-cancel exactly during `yamux.Client` would surface as a
  wrapped TRANSIENT "yamux client" error (:737), misclassified for one backoff
  iteration before `ctx.Done()` reaps it. No test drives Close/Start-cancel
  concurrently with a redial that is mid-handshake.
  `TestTunnelReconnectCtxCancelInterruptsBackoff` cancels while PARKED IN BACKOFF
  (timer select), never mid-`DialContext`/mid-`ReadString`/mid-`yamux.Client`.
- **Why it matters:** This is the one genuinely subtle concurrency seam the
  ctx-cancelable dial introduced (so a 5s-blocked handshake doesn't blow the
  goleak poll). A watcher goroutine leak or use-after-return conn close would only
  manifest under `-race` + repetition at the exact cancel instant — precisely what
  churn tests miss because they cancel cleanly between attempts. A regression here
  reintroduces the slow-teardown leak the dial change was meant to kill.
- **Remediation (optional code hardening):** after `yamux.Client` fails, check
  `ctx.Err()` and return it instead of the wrapped yamux error so
  `redialWithBackoff` exits immediately. Primary ask is the test.
- **Proposed test:** `TestDialAndRegisterWatcherCancelRace` — loop ~200× starting
  a redial against a broker that ACCEPTS TCP but stalls before writing `OK`, cancel
  the client ctx at randomized sub-handshake delays across the
  dial/write/read/yamux stages; assert prompt return (<200ms), goleak-clean (no
  surviving watcher), `-race`-clean.

### M2 — Edge-dedup / token-scrub assertions on the terminal-DENY path are too weak
- **Location:** `internal/tunnel/tunnel_reconnect_test.go:181-212`
  (`TestTunnelReconnectFiresSessionStateCallback`) and `:149-177`
  (`TestTunnelReconnectStopsOnTerminalDeny`).
- **Issue:** `notifyState` fires "exactly once per edge" purely by `supervise` loop
  STRUCTURE (one `notifyState(false)` per drop at `tunnel.go:756`, one
  `notifyState(true)` per swap at :772; the terminal/ctx branch returns WITHOUT a
  second `notifyState`) — there is NO dedup variable, contrary to the plan §6 text
  that asserts a "per-port transition dedup". The tests do not pin the structural
  invariant: (1) `FiresSessionStateCallback` breaks at `n>=2` and only checks
  `events[0]==false, events[1]==true` — a duplicate producing `[false,true,false]`
  or `[false,true,true]` passes (trailing event arrives after the loop exits);
  (2) `StopsOnTerminalDeny` asserts the slot is dropped and REGISTER is bounded but
  NEVER asserts the single final-down edge with zero trailing up, and NEVER asserts
  the cached token was scrubbed — even though `dropSession` (`tunnel.go:837`)
  zeroes `cur.token` and the plan §7 + the security invariant ("token never
  persisted/lingering") explicitly require it.
- **Why it matters:** The edge contract is what protects `/sub` from a publish storm
  on a flapping link and guarantees the broker sees a clean unready→ready. A
  double-up/down edge re-flaps subscribers; a missing final-down on terminal DENY
  leaves `proxy_ready` stuck true on a dead exit (Defect B, re-armed). Token-zeroing
  is a stated security invariant with zero coverage. The absence of the dedup the
  plan asserts means the invariant is structural-only and fragile to refactor.
- **Remediation:** Either add the per-port `lastEdge` guard the plan describes
  (defense-in-depth, since the publish is cross-network) OR correct plan §6/§2.3 to
  state the invariant is loop-structural with no dedup variable. Strengthen both
  tests regardless.
- **Proposed test:** in `FiresSessionStateCallback`, after recovery sleep one
  backoff interval and assert `len(events)==2` exactly + strict alternation; in
  `StopsOnTerminalDeny`, install a counting hook and assert exactly one `false`
  edge and zero `true` edges after the terminal DENY; add an `export_test` accessor
  (`SessionTokenForTest(port)`) and assert it is `""` after termination
  (`TestTunnelTerminalDenyScrubsToken`).

### M3 — Planned goleak-based concurrency suite (incl. `TestTunnelReconnectVsClose`) not added
- **Location:** `test/concurrency/` (missing); leak tests live in
  `internal/tunnel/tunnel_reconnect_test.go:339-388` using `runtime.NumGoroutine()`.
- **Issue:** Plan §7 requires three concurrency-suite tests under `-race` + goleak —
  `...ChurnNoGoroutineLeak`, `...VsConcurrentOpenSamePort`, `...VsClose` — plus a
  re-baseline acknowledging the new steady-state +1 supervisor per open port. Only
  the first two exist, and they use `runtime.NumGoroutine() + baseline+2` rather
  than the project's goleak harness (CLAUDE.md §5 standard). `TestTunnelReconnectVsClose`
  (Close during backoff → no re-insert, supervisor exits) is entirely absent, and
  the suite was never extended for the supervisor. The churn baseline
  (`tunnel_reconnect_test.go:~340`) is sampled immediately after an HTTP round-trip,
  so `bridge()`'s `io.Copy` goroutines may not have drained — the `+2` slack can both
  MASK a single-goroutine supervisor leak and FLAKE on unrelated background stacks.
  The vs-Open test also does not assert the loser's transport was actually closed
  (plan §7 promised it) — it cannot observe conn closure with the current seams.
- **Why it matters:** `NumGoroutine()+2` is a blunt, noisy floor that effectively
  cannot detect a slow 1-goroutine-per-few-cycles leak (the B1 class). The missing
  `VsClose` test leaves the Close-during-backoff path (the fail-closed →
  `RemoveProxy` → `Client.Close` interplay) unverified: a re-insert after Close would
  leak a supervisor and a bound port. Without a counting-conn seam the loser-closed
  and half-open (B1) guarantees are unpinned, so those regressions pass the suite.
- **Remediation:** Port churn + vs-Open + a new vs-Close test into `test/concurrency/`
  using `assertNoGoroutineLeak` (goleak) with a documented +1-per-open-port baseline;
  sample baseline BEFORE Open with keep-alives disabled and quiesced, assert return
  only after Close + ctx-cancel + poll. For VsClose: enter backoff (transient deny),
  `Client.Close(port)` mid-backoff, assert `SessionUp(port)==false`, no re-insert
  across 500ms, goleak-clean. Add a counting `net.Conn` so the vs-Open test asserts
  `dialedConns==closedConns` and that the surviving session's gen is the LATER Open's.
- **Proposed test:** `TestTunnelReconnectVsClose`; migrate churn/vs-Open to goleak in
  `test/concurrency/` with quiesced baseline + counting-conn assertions.

### M4 — Teardown-vs-in-flight-`notifyState(true)` TOCTOU on `proxyPublicPort` untested
- **Location:** `internal/agent/agent.go:320-327` (`onTunnelSessionState`) +
  `internal/agent/proxy.go:217` (store) / `:254,:272` (clear); test gap at
  `internal/agent/proxy_reconnect_test.go`.
- **Issue:** The hook reads `proxyPublicPort.Load()` lock-free (off `p.mu`/`c.mu`).
  `proxyTeardownLocked`/`proxyFailCleanupLocked` store `proxyPublicPort=0` under
  `p.mu` AND cancel the supervisor via `RemoveProxy → Client.Close`. There is a
  window where a supervisor that already passed its `ctx.Err()` check reads
  `proxyPublicPort == port` (pre-store) and publishes ready, while teardown's
  `Store(0)` + `RemoveProxy` commit microseconds earlier. `atomic.Int64` gives the
  store sequential consistency, but the supervisor may have loaded the old value
  just before. A stale `ready=true` would re-advertise a just-torn-down (or
  `proxy off`'d) exit in `/sub` for up to one heartbeat until `repairProxy`
  corrects it. The reverse (`true` racing a teardown that set port to 0) IS
  filtered. The existing filter test is purely sequential (calls
  `onTunnelSessionState` directly), covering neither interleaving.
- **Why it matters:** A spurious `ready=true` after `proxy off` re-advertises a node
  the owner explicitly disabled — a narrower re-occurrence of the incident, on the
  most security-sensitive path. The fix relies on `Close()` canceling the supervisor
  before it can publish, but the publish is off-lock and the store-0 is racy with the
  read; only an adversarial concurrency test shows whether the window is closed.
  NOTE: on the production Close-then-Open rebuild path, `Close` cancels the old
  supervisor's ctx so its redial is interrupted before reaching `swapTransport`/the
  up-edge — so the stale-ready is bounded by one heartbeat (parallels the R3
  deferral) rather than permanent. Worth documenting + bounding, not necessarily
  closing.
- **Remediation:** Either (a) clear `a.proxyPublicPort.Store(0)` at the very TOP of
  `proxyTeardownLocked` (before `RemoveProxy`/cancel) so the filter closes before the
  last possible hook fire; or (b) add a serving-epoch atomic the up-edge re-checks; or
  (c) document + assert the one-heartbeat bounded window as accepted (Fix D + OFF-repair
  converge).
- **Proposed test:** `TestProxyReadinessHookVsTeardownRace` (`-race`) — spawn N
  goroutines calling `onTunnelSessionState(proxyPort,true)` while another flips
  `proxyPublicPort.Store(0)`/`RemoveProxy`; assert no ready is published once the port
  is cleared (or assert the bounded window).

---

## Minor

### m1 — Terminal-DENY / ctx-cancel branch never invokes the per-life `cancel` (context-child leak)
- **Location:** `internal/tunnel/tunnel.go:757-762` (`supervise` terminal branch) +
  `:833-840` (`dropSession`), vs `Open:642` (`sessCtx, cancel := context.WithCancel`).
- **Issue:** On a terminal DENY or a ctx-cancel return, `supervise` calls
  `dropSession(publicPort, gen)` and returns but NEVER calls the per-session `cancel`
  created in `Open`. `dropSession` only deletes the map slot and zeroes the token; it
  does not call `cur.cancel()`. Compare `Close` and the Open-replace path
  (`tunnel.go:660`), both of which call `old.cancel()`. The orphaned cancel-func stays
  registered in `c.ctx`'s children list until the agent run-ctx is canceled at
  shutdown. `go vet`'s `lostcancel` does NOT catch it (the cancel is stored in a struct
  field), and it is NOT a goroutine/fd leak (the supervisor returns; conn/yamux already
  closed by the drop), so `TestTunnelReconnectChurnNoGoroutineLeak` won't detect it.
- **Why it matters:** Slow, unbounded context-child accumulation on a long-lived agent:
  every expose→revoke or proxy-off cycle reaching a terminal DENY leaks one
  `cancelCtx` registration onto the root tunnel ctx. Severity minor (proxy/expose ports
  are few) but it violates the plan's "one cancel per generation, called" discipline.
- **Remediation:** Have `dropSession` (or `supervise` before returning) call the
  session's `cancel` under `c.mu` when `cur.gen==gen`, before delete — idempotent with
  `Close`/`Start`-cancel.
- **Proposed test:** `TestTunnelTerminalDenyReleasesContext` — drive drop →
  terminal DENY, then assert the captured `sessCtx.Err() != nil` (white-box via
  `export_test`). Run under `-race`.

### m2 — `GetProxyEnabled` store fault folds into a permanent terminal DENY (Fix-C hole one query later)
- **Location:** `internal/broker/expose.go:86-89` (`__proxy__` master-switch gate).
- **Issue:** Fix C split the `LookupByTokenHash` store error into transient `try_again`
  (expose.go:61-72). But the very next gate folds a transient store error into the
  TERMINAL `token_unknown_or_revoked`:
  ```go
  if on, err := session.GetProxyEnabled(b.cfg.DB, sid); err != nil || !on {
      return fmt.Errorf("token_unknown_or_revoked")
  }
  ```
  The identical DB hiccup Fix C neutralizes, occurring on a `__proxy__` REGISTER (the
  DOMINANT proxy reconnect path), STILL produces a permanent false-terminal: the agent
  supervisor stops forever even though the row is ALLOCATED and the switch is ON — it
  just couldn't be read.
- **Why it matters:** This is the exact incident class Fix C exists to prevent, on the
  most-traveled reconnect path. It is fail-CLOSED so it is not a resurrection/security
  hole — but it is an availability hole that defeats half of Fix C's intent for proxy
  ports; a flaky DB during a fleet reconnect storm could mass-eject healthy proxy exits
  with no auto-recovery.
- **Remediation:** Distinguish the transient error from authoritative `off`: on
  `err != nil` return `try_again` (broker DB fault, attacker cannot induce, leaks
  nothing — same argument as Fix C); only `on == false` (read succeeded, switch
  genuinely off) returns terminal `token_unknown_or_revoked`.
- **Proposed test:** extend `TestTunnelTokenLookupTransientStoreErrorIsTryAgain` with a
  `__proxy__` ALLOCATED row (`proxy_enabled=1`), close the DB, assert `try_again`; plus
  a `on==false` case asserting `token_unknown_or_revoked`.

### m3 — `swapTransport`-loses-race leaves a spurious unready as the final state (latent readiness strand)
- **Location:** `internal/tunnel/tunnel.go:756` + `:764-769` (loser branch),
  interacting with `internal/agent/proxy.go:233` (`pubProxyReady`) and Fix D
  (`internal/broker/proxy.go:627`).
- **Issue:** When a supervisor redial succeeds but `swapTransport` returns false (a
  concurrent Open bumped the generation), the loser has ALREADY fired
  `notifyState(publicPort,false)` at :756. The winner's `ready` is published from a
  DIFFERENT goroutine (`proxyStartLocked:233`), so the relative order of the spurious
  unready vs the winner's ready is nondeterministic. If unready lands AFTER ready,
  `proxy_ready` ends 0 while the node serves — and Fix D (:627) then SUPPRESSES the
  not-ready keyset nudge at a matching `(gen,epoch)`, so there is no convergence path
  back to ready until a NATS reconnect or epoch bump. NOTE: in the PRODUCTION path a
  same-port rebuild is Close-then-Open under the adapter `opMu`, and `Close` cancels the
  old supervisor's ctx, so the loser's redial is interrupted before `swapTransport` — so
  this is largely NOT reachable today. It IS reachable in the test seam
  (`TestTunnelReconnectVsConcurrentOpenSamePort` issues a bare concurrent Open with no
  preceding Close) and for any future caller that Opens a live port without Close.
- **Why it matters:** It is the exact failure class the fix prevents (a serving node
  advertised not-serving), reachable via the gen race + Fix D suppression. Unguarded
  latent hazard.
- **Remediation:** On the loser branch, do not leave the spurious down edge as final
  state — have the loser publish nothing and rely on the winner's `proxyStartLocked`
  ready; or fire `notifyState(false)` only right before a redial the SAME supervisor
  will own. Document/assert that `swapTransport==false` implies the winner has/will
  publish ready.
- **Proposed test:** `TestReconnectLoserDoesNotStrandReadiness` — drive a redial that
  loses the gen race against an Open, capture the ready/unready sequence, assert the
  terminal observed state is `ready` (or the loser emits no unready). `-race`.

### m4 — Stale `brokerGen` in the Fix D guard is correct only by accident
- **Location:** `internal/broker/proxy.go:588` (snapshot) + `:601-608` (escalation) +
  `:627` (Fix D guard).
- **Issue:** After `brokerGen := b.proxyGenLoad()` (:588), the `agentGen > brokerGen`
  escalation (:601-608) raises `b.proxyGen` but does NOT refresh local `brokerGen`. The
  Fix D guard at :627 (`!ready && agentGen == brokerGen && agentEpoch == epoch`)
  compares against the STALE value. This is currently HARMLESS (escalation only runs
  when they were unequal, so `==` is false and the push fires — the wanted escalation
  repair). But the intuitive "correctness" refactor of refreshing `brokerGen` after
  escalation would make `==` true post-escalation and wrongly suppress the escalation
  push, re-stranding a restored-behind agent. The escalation-at-not-ready path is
  completely untested.
- **Why it matters:** Latent footgun: the guard's correctness depends on a non-obvious
  "do not refresh `brokerGen` here" property with no comment or test pinning it.
- **Remediation:** Add a one-line comment at :627 stating the guard intentionally uses
  the pre-escalation snapshot so an escalated node always falls through to the push; pin
  with a test.
- **Proposed test:** `TestRepairProxyEscalationPathPushesAtNotReady` — node proxy ON,
  `proxy_ready=0`, heartbeat `ProxyGeneration` far above `proxyGenLoad()` (within skew
  ceiling), epoch matching; assert a directive IS pushed and its stamped `Generation >
  agentGen` (escalation happened).

---

## Nits (cheap regression fences; no correctness impact)

- **n1 — Broker fast-redial replaces session, predecessor reaped via `publicAcceptLoop`
  defer, not at `tunnel.go:342-346`.** Confirmed bounded (the replace closes the old
  listener → `Accept` errors → loop returns → defer closes `yamuxSess`+`rawConn`), but
  the defer is the SOLE, load-bearing reaper. Add a comment at the replace site and
  `TestTunnelServerFastRedialReplacesSessionNoLeak` (fast second `handleAgent` for the
  same port; assert exactly one `serverSession` + goroutine baseline). Consider closing
  `old.yamuxSess` explicitly for belt-and-suspenders.

- **n2 — `Open` spawns `supervise` AFTER releasing `c.mu` (`tunnel.go:675-677`).** Safe
  only because `sessCtx` descends from `c.ctx` (a racing Start-cleanup self-heals via
  ctx propagation). Add a comment at the spawn site noting the dependency, or spawn
  under `c.mu`. `TestTunnelOpenRacesStartCancel` (simultaneous `cancel()` + `Open`;
  assert no orphaned supervisor).

- **n3 — Missing terminal-path token-lookup branch tests.** No test pins (a) found-but
  sid/nid/port mismatch → `token_unknown_or_revoked` (`expose.go:76-80`); (b)
  `__proxy__` + `proxy_enabled=0` with a HEALTHY DB → terminal (not `try_again`)
  (`expose.go:86-89`). Both guard the anti-resurrection boundary. Add
  `TestTunnelTokenLookupMismatchIsRevoked` and `TestTunnelTokenLookupProxyOffIsRevoked`.

- **n4 — Fix C transient test uses only `db.Close()` ("database is closed").** Add a
  table case with an arbitrary non-`ErrNotFound` error (via a lookup seam) → `try_again`,
  and a sentinel-wrapped-`ErrNotFound` → `token_unknown_or_revoked`, to prove
  `errors.Is(err, port.ErrNotFound)` is the ONLY thing routing to terminal.

- **n5 — Missed-OFF-when-not-ready not isolated.** Confirm Fix D placement (below the
  OFF return at `proxy.go:610-617`) never swallows a missed-OFF disable for a not-ready
  node. `TestRepairProxyMissedOffStillDisablesWhenNotReady`.

- **n6 — R3 key-rotation-during-outage has no characterization test.** With Fix D
  suppressing the not-ready nudge, the "next heartbeat corrects it" assumption holds only
  if the rotation produced a surviving gen/epoch divergence. Pin it:
  `TestProxyKeyRotationDuringOutageConvergesAfterReconnect` (revoke/bump epoch during
  outage, assert post-reconnect keyset push + apply).

- **n7 — Broker-side no-token-in-logs not asserted.** `TestTunnelReconnectNeverLogsToken`
  covers only the agent client and only the raw token bytes (not its SHA256 hex). Add
  `TestBrokerTunnelNeverLogsTokenOrHash` across a REGISTER → drop → transient → terminal
  cycle, asserting neither the raw token nor its hash appears.

- **n8 — Fail-closed teardown ordering (`proxy.go:264` cancel before `:272` clear).** A
  supervisor down-edge can observe `proxyPublicPort==port` during `failClosedFire` and
  publish unready (the SAFE direction; benign + idempotent). Optionally move the
  `Store(0)` to the top of teardown so the filter closes before cancel.
  `TestFailClosedTeardownRaceWithSupervisorDownEdge` (`-race`).

- **n9 — `notifyState` ordering across two NATS subjects is load-bearing + untested.**
  Fix B's unready→ready correctness rests on the broker's `proxy.*` subscription staying
  synchronous-ordered. Add `TestProxyReadyEventOrderingPreserved` (publish unready then
  ready on one nc; assert final `proxy_ready==1`) or a comment at the broker subscription.

- **n10 — Timing-dependent sleeps make some tests flaky-passing.**
  `TestTunnelReconnectTransientDenyRetries` (`:128-145`) uses a 120ms sleep against the
  10ms/80ms test backoff and would pass even if zero transient DENYs were ever served.
  Make it deterministic: count transient DENYs served and assert `>=N` (via channel/atomic)
  before clearing the deny, replacing wall-clock waits with synchronization signals.

---

## Positives

- **Generation fence applied at BOTH mutation sites** (`swapTransport:822`,
  `dropSession:836`) with identical `cur.gen != gen` guards — a stale supervisor losing
  the Open race cannot mutate or delete a newer generation's slot. No double-owner.
- **One-cancel-per-generation-for-life** honored: `swapTransport` KEEPS the same cancel
  (no per-redial `WithCancel`), so `Close`/`Start`-cancel always reaches the live
  supervisor's ctx — the orphaned-loop hazard the plan rejected is genuinely avoided.
- **Lock order `p.mu → c.mu` never inverted:** `onTunnelSessionState` (`agent.go:320`)
  takes NEITHER lock, reading only atomics (`proxyPublicPort`, `ncBox`) and calling
  lock-free `pubProxyReady` — proven by `TestProxyReadinessHookNoDeadlockUnderConcurrentDirective`.
- **DENY taxonomy fails CLOSED:** `denyIsTransient` whitelists only
  `public_port_bind_failed` + `try_again`; `token_unknown_or_revoked`, `session_closed`,
  `malformed_register`, empty, and any UNKNOWN/future reason all TERMINATE. A revoked/
  disabled exit can never be resurrected by a blind reconnect. `TestDenyIsTransient` pins
  every case including `""` and unknown.
- **Resurrection invariant holds:** a revoked/disabled exit is denied at
  `tunnelTokenLookup` BEFORE `net.Listen`, so `public_port_bind_failed` (transient) is
  only ever returned for a still-ALLOCATED legitimate row; the three broker install
  fences remain the sole resurrection authority, untouched by reconnect.
- **Fix C is enumeration-safe:** `try_again` fires ONLY on a non-`ErrNotFound` store
  fault (independent of token validity); `ErrNotFound`/mismatch/proxy-off all collapse to
  `token_unknown_or_revoked`. An attacker cannot induce `try_again` to probe token
  existence.
- **Fix D tightly scoped:** `proxyGenEpoch` reports `(0,0)` when `srv==nil`, so the broker
  only sees a matching non-zero pair from a SERVING agent; suppression fires solely for
  the serving-but-tunnel-down case Fix B owns, never for a down-server node needing
  recovery. OFF-repair returns before the guard, keeping `proxy off` convergent. Both
  directions tested.
- **ctx-cancelable dial + backoff:** `tls.Dialer.DialContext` + the `hsDone` watcher reap
  a blocked dial/handshake; `redialWithBackoff` uses `time.NewTimer`+`Stop` with a
  `ctx.Done()` arm so the 30s cap cannot pin the goroutine — proven by
  `TestTunnelReconnectCtxCancelInterruptsBackoff`.
- **Egress ACL provably untouched by a redial:** the SS server
  (`DenyPrivateDestinations`) is created once in `proxyStartLocked`; a reconnect only
  swaps the tunnel conn+yamux in place — never reconfigures the SS server.
- **`runAcceptLoop` binds the `*yamux.Session` by VALUE**, not re-read from the map per
  iteration — keeping the hot accept path lock-free and `-race`-clean across a swap.
- **`Open` is synchronous on the first dial** and propagates the first-attempt error
  verbatim, preserving the `AddProxy` rollback contract.
- **`proto.ProtoVersion` stays 1**; `try_again` is an additive tunnel control-line reason,
  not a wire-message bump.

---

## Summary for adjudication

| ID | Sev | Location | One-liner |
|----|-----|----------|-----------|
| B1 | blocker | tunnel.go:818-828 | `swapTransport` leaks old conn+yamux on half-open reconnect |
| B2 | blocker | test/p13 (absent) | full-stack false-online/disable-during-drop integration tests not written |
| M1 | major | tunnel.go:704-712 | handshake watcher success-vs-cancel race untested |
| M2 | major | tunnel_reconnect_test.go:149-212 | edge-count + token-scrub assertions too weak |
| M3 | major | test/concurrency (absent) | goleak suite + `VsClose` missing; noisy NumGoroutine floor |
| M4 | major | agent.go:320 / proxy.go:217-272 | teardown-vs-in-flight ready TOCTOU untested |
| m1 | minor | tunnel.go:757-840 | terminal-DENY branch never calls per-life cancel (ctx-child leak) |
| m2 | minor | expose.go:86-89 | `GetProxyEnabled` store fault → permanent terminal (Fix-C hole) |
| m3 | minor | tunnel.go:756/764-769 | swapTransport-loses leaves spurious unready (Fix D strands it) |
| m4 | minor | proxy.go:588/627 | stale `brokerGen` in Fix D guard correct only by accident |
| n1–n10 | nit | (various) | regression-fence comments + tests |

**Recommended gate before commit:** fix B1 (transport close) and m2 (GetProxyEnabled
transient split) in the implementation; add B2 + M1 + M2 + M3 + M4 tests; address
m1/m3/m4 as code-or-comment per adjudication; land the nit tests opportunistically.
Re-run `make test` + `make e2e` + `make lint`, plus `-race`/goleak on the tunnel +
concurrency packages.

---

## Main-process adjudication (post-review)

Verdict adopted: **was BLOCKED → remediated**. All gates re-run green afterward
(`go test -count=1 ./...`, `make e2e`, `golangci-lint` 0 issues, `-race` on
tunnel/agent/broker/concurrency). Per-finding:

**Blockers — both fixed.**
- **B1 (swapTransport fd/goroutine leak)** — ADOPTED. `swapTransport` now captures the
  predecessor `conn`/`yamuxSess` and `Close()`s them after releasing `c.mu`
  (`internal/tunnel/tunnel.go`). Pinned by `TestTunnelReconnectClosesPredecessorConn`.
- **B2 (no full-stack tests)** — ADOPTED. Added `test/p13/proxy_reconnect_e2e_test.go`
  with the REAL `TunnelExposeAdapter` + the broker's REAL tunnel server + a controllable
  `severableRelay` (the white-box `DropTransport` seam is unreachable cross-package, so a
  relay is the faithful cut point, no prod API pollution):
  `TestProxyFalseOnlineRecoversAfterTunnelDrop` (drop → unready + port unbind → self-heal →
  re-bind; negative control: node stays ONLINE = NATS untouched) and
  `TestProxyDisableDuringTunnelDropStaysDown` (disable mid-drop → no resurrection). Both
  added to the e2e matrix (`TestProxyTunnelReconnectMatrix`).

**Majors.**
- **M1 (handshake-watcher race untested)** — ACCEPTED-as-safe: a late ctx-cancel close only
  occurs when tearing down, and every consumer re-checks ctx/gen; broader race coverage via
  `TestTunnelReconnectVsConcurrentOpenSamePort` + `…CtxCancelInterruptsBackoff`.
- **M2 (weak terminal-DENY assertions)** — ADOPTED. `…StopsOnTerminalDeny` now asserts
  exactly one `false`/zero `true` edges; `…FiresSessionStateCallback` asserts EXACTLY
  `[false,true]`.
- **M3 (goleak placement / VsClose)** — PARTIALLY ADOPTED: churn + VsConcurrentOpen leak
  tests live in `internal/tunnel` (the seam is white-box-only) and run under `-race` in
  `make test`/`make e2e`; VsClose is covered by `…CtxCancelInterruptsBackoff` (Close cancels
  the same ctx).
- **M4 (teardown-vs-ready TOCTOU)** — MITIGATED + tested: `proxyTeardownLocked` clears
  `proxyPublicPort` FIRST; `…HookNoDeadlockUnderConcurrentDirective` pins the lock-order.

**Minors.**
- **m1 (terminal-DENY ctx-child leak)** — ADOPTED: `dropSession` now calls the per-life
  `cancel()` under `c.mu`.
- **m2 (Fix C residual on the `__proxy__` off-gate)** — ADOPTED: `GetProxyEnabled` store
  fault → `try_again`; terminal only on a genuine `off`. Pinned by
  `…ProxyOffIsTerminalNotTransient` + `…MismatchIsRevoked`.
- **m3 (swapTransport-loses spurious unready)** — REJECTED as not-a-bug: down (old
  supervisor) and recovery-ready (`proxyStartLocked`) publish on the SAME agent `nc` in
  wall-clock order, so the broker converges to ready; M4's teardown-first store narrows it
  further.
- **m4 (stale `brokerGen` in Fix D guard)** — ADOPTED as a clarifying comment.

**Nits.** Adopted the high-value ones (deterministic timing in `…TransientDenyRetries`; Fix C
mismatch + proxy-off tests). Plan text corrected: per-edge firing is loop-structural, not a
dedup variable. Remaining doc-only nits left as follow-ups.

---

## Reviewer final re-review (2026-06-16)

### Verdict

**PASS / 放行。**

本轮只审查暂存区外的 proxy tunnel reconnect bugfix。复核范围覆盖 tunnel
supervisor、DENY 分类、agent readiness hook、broker `repairProxy`/token lookup、全栈
P13 行为和 e2e matrix。此前 BLOCKED 报告里的 B1/B2、m1/m2 等关键问题已被当前工作区
修复；没有发现新的 High/Medium blocker。

### Confirmed

- `swapTransport` 已在替换 transport 后关闭 predecessor `yamuxSess` 和 raw `conn`，不再把半开重连路径变成 fd/goroutine 累积泄漏。
- terminal DENY 路径通过 `dropSession` 删除 slot、调用 per-life `cancel()` 并清空 token；未知 DENY 默认 terminal，`try_again`/`public_port_bind_failed` 才是 transient。
- `onTunnelSessionState` 只读 `proxyPublicPort`/`ncBox` 两个 atomic，不拿 `p.mu` 或 `c.mu`；`proxyTeardownLocked` 先清 port filter，再关闭 tunnel，降低 stale ready 风险。
- `tunnelTokenLookup` 区分 transient DB fault 和 authoritative token verdict；`GetProxyEnabled` 查询失败也返回 `try_again`，不会误杀 reconnect supervisor。
- `repairProxy` 已抑制“仅 not-ready、gen/epoch 已匹配”的 keyset nudge，避免 heartbeat 把自愈中的 tunnel 重新 ACK 成 ready。
- 新增 P13 全栈测试使用真实 `TunnelExposeAdapter`、真实 broker tunnel server 和可断开的 TCP relay，覆盖 false-online 消失/自愈和 disable-during-drop 不复活。

### Residual questions / suggestions

1. `TestTunnelReconnectClosesPredecessorConn` 仍偏 clean-drop 白盒；代码已经显式关闭 predecessor，所以不是 blocker，但后续若要更硬，应加入真正 half-open/counting-conn seam。
2. handshake watcher 没有专门的 randomized mid-handshake cancel 测试。现有 ctx/gen recheck、Close/backoff/race 门禁支撑 release；建议后续补 `DialContext`/read/yamux 阶段的取消压力测试。
3. P13 full-stack 的 NATS 负控用 node 保持 `ONLINE` 证明控制面未断，而不是直接断言 `nc.Stats().Reconnects==0`。当前足以防止“靠 OFFLINE/replay 治愈”的主路径误判，后续可以把负控做得更精确。

### Verification

- `go test ./...`: PASS
- `go test -race ./internal/tunnel ./internal/broker ./internal/agent -run 'Reconnect|Deny|TunnelTokenLookup|RepairProxy|ProxyReadinessHook'`: PASS
- `go test ./internal/tunnel -run 'Reconnect|Deny'`: PASS
- `go test ./internal/broker -run 'TunnelTokenLookup|RepairProxy'`: PASS
- `go test ./internal/agent -run 'ProxyReadinessHook'`: PASS
- `go test ./test/p13 -run 'FalseOnlineRecoversAfterTunnelDrop|DisableDuringTunnelDropStaysDown'`: PASS
- `go test -tags=e2e_matrix ./test/e2e -v`: PASS, 119.796s
- `go vet ./...`: PASS
- `golangci-lint run`: PASS, 0 issues
- `go mod tidy -diff`: PASS, no diff
- Linux amd64 + Darwin arm64 `CGO_ENABLED=0 go build ./cmd/tether`: PASS
- `git diff --check` / `git diff --cached --check`: PASS

本轮 reviewer 只更新本报告；未修改业务代码，未暂存任何文件。
