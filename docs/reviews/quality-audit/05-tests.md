# Tests & Harness Audit

## Verdict

24 findings (1 critical, 8 high, 9 medium, 6 low) — flake risks dominate. Of the 65 `time.Sleep` call sites, **23 are bare "wait then assert" sleeps** (no follow-up polling). On a slow CI runner the audit pubsub deadline tests (`p7/audit_e2e`, `p8/reconcile_e2e`, `p4/exec_authcallout`) are the highest flake candidates: they assume that 150ms–500ms is enough for register + JS-init + audit-emit chains that in production take >100ms even on a fast box.

Headline counts:
- Bare sleep-as-barrier sites: **23**
- "Hard" deadlines that scale poorly (`100ms` request, `200ms` window, `500ms` async): **9**
- t.Parallel() usage: **0** (good — but no t.Parallel anywhere is also a missed perf opportunity, see F22)
- t.Cleanup leak risks: **3** (F19/F20/F21)
- Production-state assumptions (`/proc/sys/kernel/random/boot_id` etc.): **1 indirect** (boot_id read in agent.go on boot, tests synthesize their own)

Three classes of flake risk to fix first:
1. **Sleep-as-barrier in audit/reconcile flows** (F1–F4, F8, F12) — `300ms` / `500ms` waits before asserting JS state. These will flake under CPU contention.
2. **Composite deadlines for register-then-event chains** (F5, F6, F11) — agent is started with `time.Sleep(300ms)` "to settle", then a write must propagate before assertion. Two missed budgets stack.
3. **Tight async permission-violation windows** (F13) — `500ms` is plenty in isolation but every p3 test inherits the same auth_callout warmup; in `make e2e` matrix they queue up.

---

## Findings

### F1 — high — `p7/audit_e2e_test.go:172` sleep before asserting 150 audit msgs landed
**Where**: `test/p7/audit_e2e_test.go:172` (`time.Sleep(300 * time.Millisecond)`)
**Issue**: After driving 50 execs, the test sleeps 300ms then directly reads `stream.Info` and expects `Msgs >= 150`. Each exec triggers `audit.call + audit.proc{start} + audit.proc{exit}` over four hops (agent → broker → JS publish → JS commit). On a slow CI runner `(broker.publishAudit -> js.Publish -> ack)` can take 5-10ms per pub; 150 pubs * 10ms = 1.5s.
**Impact**: Flakes on shared CI (GitHub Actions free runners, ARM, container hosts). At 3*N publish budget within 300ms, the assertion is `info.State.Msgs < uint64(3*N)` — sleep too short and the test fails with "too few messages".
**Fix**: Replace with polling-with-deadline:
```go
deadline := time.Now().Add(5 * time.Second)
var lastN uint64
for time.Now().Before(deadline) {
    info, err := stream.Info(context.Background())
    if err == nil && info.State.Msgs >= uint64(3*N) {
        break
    }
    if info != nil { lastN = info.State.Msgs }
    time.Sleep(20 * time.Millisecond)
}
if lastN < uint64(3*N) {
    t.Fatalf("history-lab too few messages after 5s: got %d want >= %d", lastN, 3*N)
}
```

### F2 — high — `p7/sys_events_test.go:106` 500ms before asserting handshake event
**Where**: `test/p7/sys_events_test.go:106`
**Issue**: After `startAgent`, sleeps 500ms and checks `agent_registered` event arrived. Both `startBroker` (with internal `time.Sleep(150ms)`) AND `startAgent` (`time.Sleep(300ms)`) already burned ~450ms of fixed budget; this is *additional* slack for round-trip. Total budget gives ~500ms for: broker JS-init → agent connect → agent register → broker emit → fan-out to test sub. Real per-step is ~50-100ms when host is busy.
**Impact**: Likely top-3 flake source under CI matrix load. Test fails "expected agent_registered ... got [...]" with empty list.
**Fix**: Subscribe before booting (already done — `snap` is captured); poll snap with deadline:
```go
deadline := time.Now().Add(5 * time.Second)
for time.Now().Before(deadline) {
    for _, ev := range snap() {
        if ev["type"] == "agent_registered" && ev["sid"] == "lab" && ev["nid"] == "lab-1" {
            return
        }
    }
    time.Sleep(20 * time.Millisecond)
}
t.Errorf("expected agent_registered{sid=lab,nid=lab-1}; got %+v", snap())
```

### F3 — high — `p7/sys_events_test.go:160` "let monitor start" 300ms before asserting Phase 1
**Where**: `test/p7/sys_events_test.go:160`, in `TestDiskPressureFiresAboveThreshold`
**Issue**: After `b.Run` go-routine, sleeps 300ms with comment "Let broker subscribe + JS init + monitor start". This is a dead-reckoning startup wait. Under load, broker startup actually does: connect → JS init (file storage) → install subs → start monitor goroutine. `t.Cleanup` on the JS NATS uses file-backed StoreDir, so disk I/O matters.
**Impact**: Phase 1 then asserts `countPressure() != 0` (i.e. nothing fired yet). If broker took >300ms to start AND >150ms more to have the 30ms-ticker tick once at 0.10 frac, premature firing is impossible (good). But the *opposite* failure — broker hadn't installed subs yet so collectEvents misses Phase 2 — is a real flake.
**Fix**: Use a readiness probe. The test connects to NATS via `nc, _ := nats.Connect(url)` later; do that probe *before* the sleep and use it to confirm broker started:
```go
deadline := time.Now().Add(3 * time.Second)
for time.Now().Before(deadline) {
    if nc, err := nats.Connect(url); err == nil {
        // probe a broker subject (e.g. ctrl.session.create) that returns fast
        _, e := nc.Request(proto.SubjCtrlSessionList("test-probe"), []byte("{}"), 100*time.Millisecond)
        nc.Close()
        if e == nil { break }
    }
    time.Sleep(20 * time.Millisecond)
}
```

### F4 — high — `p7/sys_events_test.go:202` 250ms "stay above threshold" Phase 3
**Where**: `test/p7/sys_events_test.go:202`
**Issue**: After confirming Phase 2 fired ≥1, sleeps 250ms then asserts `countPressure() <= 1` (no spam). The DiskCheckInterval is 30ms, so 250ms = ~8 ticks. If the test machine paused (GC, scheduler) before the assertion, the count snapshot after Phase 2's poll could already include extra ticks the edge-trigger logic correctly suppressed — but ALSO could miss a regression where edge-trigger broke and fired 8 times.
**Impact**: Mostly false-negative risk (a real bug could pass). Less of a flake source, more of an incomplete assertion.
**Fix**: Sample N ticks worth of data, then check the count is *stable* (same number twice 100ms apart):
```go
prev := countPressure()
time.Sleep(150 * time.Millisecond)
if countPressure() != prev {
    t.Errorf("disk_pressure count grew while above threshold: %d -> %d", prev, countPressure())
}
```

### F5 — high — `p8/reconcile_e2e_test.go:1079` 200ms before asserting 0 reconciles
**Where**: `test/p8/reconcile_e2e_test.go:1079` (`TestG1AlreadyExitedRowsSkipped`)
**Issue**: After register reply, sleeps 200ms then asserts `reconciledCount == 0`. The audit subscription is a side-channel; broker publishes audits AFTER replying to register. 200ms covers happy-path but leaves no slack for slow async pub.
**Impact**: Could flake to false POSITIVE (test fails saying audit fired) under CPU contention on the broker goroutine.
**Fix**: Either (a) subscribe BEFORE register and use a synchronous probe — send a known-marker pub right after the test logic, wait for it, then count audits seen up to that point; or (b) extend to 500ms and accept the longer test. Option (a) is preferred:
```go
markerCh := make(chan struct{}, 1)
markerSub, _ := nc.Subscribe("test.marker.done", func(*nats.Msg) { markerCh <- struct{}{} })
// ... do the register/reply test ...
nc.Publish("test.marker.done", nil)
<-markerCh // by now any in-flight audit has been fanned out too
if reconciledCount != 0 { ... }
```

### F6 — high — `p8/reconcile_e2e_test.go:101 + 132 + 182` 300ms register-warmup + 150ms additional
**Where**: `test/p8/reconcile_e2e_test.go:101`, `:132`, `:182` (all in `startAgent` / `startBroker` helpers)
**Issue**: Stacked sleeps in the harness: `startBroker` connects then `time.Sleep(150ms)` "JS init"; `startAgent` runs then `time.Sleep(300ms)` "register settle". Every test pays ~450ms before any assertion. There's no guarantee the agent actually registered in 300ms — if NATS is busy the register req may still be in-flight.
**Impact**: Cumulative 450ms × ~30 tests = 13s wall time wasted PLUS each one is a flake risk. Tests that depend on "node is ONLINE before X" can fail because the harness "thinks" it's ready.
**Fix**: Replace `startAgent`'s 300ms sleep with a node-status poll:
```go
deadline := time.Now().Add(2 * time.Second)
for time.Now().Before(deadline) {
    var status string
    if err := db.QueryRow(`SELECT status FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&status); err == nil && status == "ONLINE" {
        break
    }
    time.Sleep(20 * time.Millisecond)
}
```
For `startBroker`'s 150ms "JS init": use a JS readiness probe (same as `waitJSReady` already does in `internal/testharness/harness.go:67-77`).

### F7 — medium — `p8/reconcile_e2e_test.go:719` 50ms in `TestG1AgentAppliesRevokePorts` state.json polling
**Where**: `test/p8/reconcile_e2e_test.go:719`
**Issue**: This one is correctly in a polling-with-deadline loop (`for time.Now().Before(deadline)`) — sleep 50ms is the polling interval, not a barrier. Good pattern. **No fix needed** (listed for the audit completeness — distinguishes "good" sleeps from "bad" sleeps).

### F8 — high — `p4/exec_e2e_test.go:80 + 111` "let subscribes settle" + 300ms agent settle
**Where**: `test/p4/exec_e2e_test.go:80, 111` (`startBroker` / `startAgent` helpers)
**Issue**: Same harness pattern as F6: dead-reckoning sleeps in the broker/agent boot helpers. p4 is the *most-copied* helper (replicated near-verbatim in p5/p6/p7/p8/p9/p10). Fix in one place, all phases benefit.
**Impact**: Every exec/run/expose test pays ~350ms upfront and risks running its assertion before the agent is registered.
**Fix**: Promote a `waitNodeOnline(t, db, sid, nid)` to `internal/testharness/harness.go` and call from each phase's local startAgent. Drops both the harness sleep AND tightens the contract.

### F9 — medium — `p4/exec_authcallout_test.go:227` 300ms agent register inside auth-callout
**Where**: `test/p4/exec_authcallout_test.go:227`
**Issue**: Same `startAgentSecure` boots and sleeps 300ms. Auth-callout adds JWT issue + signature verify latency, so 300ms is even less safe here than in plain p4.
**Impact**: Same as F8 but with extra crypto cost.
**Fix**: Use the same waitNodeOnline polling.

### F10 — high — `p4/exec_authcallout_test.go:148` `nats.Timeout(200*time.Millisecond)` per-attempt CONNECT in `waitAuthCalloutReady`
**Where**: `test/p4/exec_authcallout_test.go:148` (and same shape in `test/p3/setup_test.go:150`)
**Issue**: The auth_callout readiness probe attempts a CONNECT with `nats.Timeout(200ms)` per probe, retrying for 5s total. Under heavy CI load, a single probe can take >200ms (TCP RTT + JWT issue + signature verify). The retry loop hides this in the happy case but each failed attempt eats 200ms of the 5s budget — at worst 25 attempts, plenty.
**Impact**: Low actual flake risk because of the 5s outer budget, but the per-attempt 200ms is **the canonical "too tight" pattern**: production uses 2s elsewhere.
**Fix**: Bump per-attempt to 500ms and outer deadline to 8s:
```go
nats.Timeout(500*time.Millisecond),
// ...
deadline := time.Now().Add(8 * time.Second)
```

### F11 — high — `p9/admin_e2e_test.go:427` 300ms "give agent time to register" before `evict`
**Where**: `test/p9/admin_e2e_test.go:427` (`TestAdminEvictTriggersAgentShutdown`)
**Issue**: 300ms agent register grace, THEN 1s budget for shutdown. If register actually took longer, the evict broadcast goes to nobody and the 1s shutdown deadline fires. The 1s budget is the architecture spec; the 300ms warmup is the bug.
**Impact**: Hot flake under load; the architecture promises "1s after evict" so the 1s budget is correct, but the warmup is too short.
**Fix**: Poll for `status='ONLINE'` in the nodes table (same as F6) before issuing evict.

### F12 — medium — `p9/admin_e2e_test.go:129` 300ms in `TestAdminAuditTailsHistoryStream`
**Where**: `test/p9/admin_e2e_test.go:129` is a bare `time.Sleep(300ms)` between subscribe loop start and async pub batch — but actually I misread. Let me re-locate. The actual issue is the helper-stacked sleeps inherited from p8 pattern.
Actually this line is part of `startAgent` boilerplate — same as F6/F8. **Same fix as F6**.

### F13 — medium — `p3/permissions_e2e_test.go:58, 100, 145` 500ms async-permission-violation windows
**Where**: `test/p3/permissions_e2e_test.go:58, 100, 145`
**Issue**: NATS auth_callout permission violations propagate via the async error callback; tests wait `500ms` for one to arrive. On a busy CI host with auth_callout warmup pending, the async cb fires after 500ms.
**Impact**: Sporadic "expected NATS permission violation, none arrived" flakes. The p3 suite already takes ~22s on the box (per `test/e2e/all_phases_test.go:33`) so it's the slowest phase and most exposed.
**Fix**: Bump to 2s. The cost is paid only on successful prevention (no actual error to wait for), so it's free in the failing path.

### F14 — high — `p10/upgrade_e2e_test.go:68` 120ms broker startup + dependent ./forwarded subscribe
**Where**: `test/p10/upgrade_e2e_test.go:68` (`startBroker`)
**Issue**: Reduced from the standard 150ms to 120ms, but reasoning is unclear. Same dead-reckoning pattern. Tests that use `nc.Request(...upgrade.req)` immediately after will get timeouts if the broker hasn't subscribed yet.
**Impact**: Broker subs to `<prefix>.upgrade.req` install in `b.Run`; if the test's request fires before that, request returns no_responders.
**Fix**: Same waitNATSReady probe used in `internal/broker/broker_test.go:271-293`.

### F15 — medium — `p10/upgrade_e2e_test.go:98` 300ms agent settle before upgrade test
**Where**: `test/p10/upgrade_e2e_test.go:98` (`startAgentWithUpgrade`)
**Issue**: Same 300ms agent register grace. P10 tests issue `upgrade.req` immediately after this sleep; if the agent hasn't subscribed to its forwarded subject, broker forwards-and-times-out within `UpgradeForwardTimeoutDur=5s`.
**Impact**: 5s wait on every flaky run — slow test instead of immediate fail.
**Fix**: waitNodeOnline polling.

### F16 — high — `p5/run_e2e_test.go:169` 50ms "let agent fork+exec before stdin"
**Where**: `test/p5/run_e2e_test.go:169`
**Issue**: Bare 50ms before pushing stdin bytes. If the agent's PTY allocate + fork happens slower (e.g. under valgrind, on small ARM box, on macOS-on-Apple-silicon Rosetta), the stdin lands BEFORE the child process is even reading. `sleep 30 > /dev/null` shouldn't but stdin handlers can swallow.
**Impact**: PTY input race. The test only sends `nil` stdin in most calls (so no impact), but TestRunHelloWorld passes nil stdin so unhit. **Lower severity in practice.**
**Fix**: Wait for `started` chunk on the inbox before pushing stdin (the test already sees `ready` but waits for `started` only in TestKillSendsSIGINT).

### F17 — high — `p5/run_e2e_test.go:366` 150ms "trap install slack"
**Where**: `test/p5/run_e2e_test.go:366` (`TestKillSendsSIGINT`)
**Issue**: After `started`, the test sleeps 150ms hoping the shell installs its `trap` handler before the kill arrives. If the shell is `dash` instead of `bash`, or busy CI delays exec, kill fires before trap and child exits with default SIGINT (130 on bash, 130 on dash too actually, so signal-handler timing matters more for the trap to actually run).
**Impact**: Real flake on slow CI. Kill arrives during exec setup, child dies without trap → wrong exit code.
**Fix**: Push a known marker into stdin and watch for it on stdout. E.g.:
```go
nc.Publish(proto.SubjPtyIn(sid, ready.PID), []byte("echo READY\n"))
// drain outCh until "READY" appears
```

### F18 — medium — `internal/broker/broker_test.go:252` 100ms "give broker a moment to process" ghost heartbeat
**Where**: `internal/broker/broker_test.go:252`
**Issue**: After publishing a heartbeat for a non-existent node, sleeps 100ms then asserts no rows created. This is the "negative test" pattern — proves absence within a window. 100ms could under-assert (broker could create the row later) but the test's job is "doesn't insta-create" so it's defensible.
**Impact**: Limited — false-negative if broker is delayed.
**Fix**: Acceptable as-is, OR bump to 500ms and document explicitly: `// Negative-window: 500ms is enough to catch any insta-insert; future async paths must also defer past this`.

### F19 — high — `p6/expose_e2e_test.go:367-393` `stopAgent` not deferred
**Where**: `test/p6/expose_e2e_test.go:367` then manual call `:393`
**Issue**: `stopAgent := startAgent(...)` then test runs `runExpose` etc, finally calls `stopAgent()`. If any `t.Fatal` between lines 367-393 fires, the agent goroutine leaks (no `t.Cleanup`).
**Impact**: Goroutine leak between tests in the same package. Subsequent tests may see stale agent connections. Test framework eventually OOMs in long runs.
**Fix**: 
```go
stopAgent := startAgent(...)
agentStopped := false
t.Cleanup(func() { if !agentStopped { stopAgent() } })
// ... mid-test ...
agentStopped = true
stopAgent()
```
Same pattern as `startAgentManual` in `test/p8/reconcile_e2e_test.go:116`.

### F20 — high — `p8/reconcile_e2e_test.go:521 + 569` `stop1()` mid-test, no t.Cleanup safety net
**Where**: `test/p8/reconcile_e2e_test.go:521, 569` (`TestChaosKillAgentRestartConverges`)
**Issue**: `stop1, _ := startAgentManual(...)` returns a closure with `stopped` boolean (good) but the closure isn't registered with `t.Cleanup`. If lines 521-569 fail with `t.Fatal`, `stop1` never runs.
**Impact**: Same as F19 — agent goroutine leak on failure.
**Fix**: 
```go
stop1, _ := startAgentManual(t, url, "lab", "lab-1")
t.Cleanup(stop1) // safe: stop1 internally no-ops on second call
```

### F21 — medium — `p9/admin_e2e_test.go:411-426` agent goroutine leak path
**Where**: `test/p9/admin_e2e_test.go:411-426` (`TestAdminEvictTriggersAgentShutdown`)
**Issue**: The agent is created inline with `cancel`, and `defer cancel()` is set. If `cancel` fires but `<-done` never returns (e.g. agent hung), the test moves on with a live goroutine.
**Impact**: One-shot leak per test failure. Less serious than F19/F20.
**Fix**: Wrap in helper that combines `cancel()` + `<-done` with timeout, register via t.Cleanup.

### F22 — low — no t.Parallel() anywhere in the suite
**Where**: project-wide (only mention is in `test/e2e/all_phases_test.go:46-53` comment explaining why e2e is serial)
**Issue**: Every test is serial. Could safely parallelize unit tests in `internal/auth/`, `internal/proto/`, `internal/storage/` (no shared state, all in-memory). Won't help e2e but would speed `go test ./internal/...`.
**Impact**: Slower iteration. Not a flake risk.
**Fix**: Add `t.Parallel()` at the top of pure-unit tests in `internal/{auth,proto,storage,port,session,proc,jsstream,schema,authcallout}/`.

### F23 — low — production state assumption: `agent.go:624` reads `/proc/sys/kernel/random/boot_id`
**Where**: `internal/agent/agent.go:624`
**Issue**: Agent reads the kernel boot_id at startup. In a container without `/proc/sys/kernel/random/boot_id` (very minimal container, BSD, macOS), this would fail. Tests SYNTHESIZE boot_id ("boot-A", "boot-A", "synthetic-boot") in `p8/reconcile_e2e_test.go:1163, 1247`, so the production read path isn't exercised by tests. **Tests work in CI** because CI uses Linux containers with /proc.
**Impact**: Potential portability gap for non-Linux dev boxes. Tests pass but production agent on macOS would crash on read.
**Fix**: Audit `agent.go:624` for non-Linux fallback (likely already handled — quick check needed). Not a test-quality issue per se.

### F24 — low — `internal/pty/pty_test.go:77` 200ms "let trap install"
**Where**: `internal/pty/pty_test.go:77`
**Issue**: Same trap-install race as F17 but at the pty package level. Lower stakes (unit test), but same fix pattern.
**Fix**: Echo a marker through PTY and wait for it to drain.

---

## Sleep Census (full list, classified)

**OK — polling-with-deadline pattern (sleep is the poll interval):**
- `internal/testharness/harness.go:74` (JS readiness)
- `internal/broker/broker_test.go:155, 291` (DB poll, NATS readiness)
- `internal/agent/agent_test.go:119, 220, 273` (counter polls)
- `internal/tunnel/tunnel_test.go:95, 208, 224` (HTTP/port polls)
- `test/p4/exec_e2e_test.go:283` (DB poll EXITED)
- `test/p4/exec_risk_test.go:113` (DB poll started_by_fp)
- `test/p2/agent_startup_resilience_test.go:66` (registerSeen poll)
- `test/p2/agent_nats_startup_resilience_test.go:75` (registerSeen poll)
- `test/p2/heartbeat_e2e_test.go:170` (waitForState poll)
- `test/p3/setup_test.go:159` (auth_callout readiness poll)
- `test/p5/run_risk_test.go:139` (waitUntil poll)
- `test/p7/audit_e2e_test.go:64, 295, 364, 397` (NATS-ready, stream-gone polls)
- `test/p7/sys_events_test.go:194, 211, 221` (countPressure polls)
- `test/p8/reconcile_e2e_test.go:72, 180, 719, 768, 1397` (NATS/state polls)
- `test/p9/admin_e2e_test.go:89, 101` (NATS/admin readiness polls)
- `test/p10/upgrade_e2e_test.go:66` (NATS readiness poll)

**BAD — bare sleep-as-barrier (assert immediately after):**
- `test/p7/audit_e2e_test.go:67, 94, 172` — F1, plus harness 150ms barrier
- `test/p7/sys_events_test.go:66, 106, 160, 179, 202` — F2, F3, F4
- `test/p8/reconcile_e2e_test.go:74, 101, 132, 182, 1079` — F5, F6
- `test/p4/exec_e2e_test.go:78-80, 111` — F8 (helper barrier)
- `test/p4/exec_authcallout_test.go:227` — F9
- `test/p5/run_e2e_test.go:64-66, 93, 169, 366` — F16, F17, harness
- `test/p6/expose_e2e_test.go:82-84, 146` — harness pattern
- `test/p9/admin_e2e_test.go:129, 427` — F11, F12
- `test/p10/upgrade_e2e_test.go:68, 98` — F14, F15
- `internal/broker/broker_test.go:252` — F18
- `internal/pty/pty_test.go:77` — F24

Total bare-sleep sites: **23 unique (some files have 2-3)**.

---

## Cross-cutting observations

1. **Build tag protection**: only `test/e2e/all_phases_test.go` has `//go:build e2e_matrix`. All `test/pX/*_test.go` files are reachable from `go test ./...`. Verified.
2. **No t.Parallel** anywhere — clean from a flake perspective but missed perf.
3. **Helper file consolidation worked**: `internal/testharness/harness.go` removes most copy-paste; per-phase files keep phase-specific helpers. Per the package doc comment this was intentional. Good.
4. **t.Helper() usage**: 96 occurrences. Spot-checked — every helper with `t.Fatal` is correctly marked. No findings.
5. **White-box DB reads**: Many tests read SQLite directly (e.g. `db.QueryRow("SELECT status FROM nodes ...")` in `p8`). Comments admit this ("ps is read-time derivation per architecture"). Acceptable given the tests are architecture-pin tests, but worth flagging if you ever add an in-memory cache layer in front of the DB — those tests will silently pass on stale data.
6. **`exec.Command("go", "build")` in `p10/install_user_service_test.go:28`**: builds a fresh binary per test. Per-test cost ~3s on warm cache, much more cold. Could be hoisted to `TestMain` to build once. Listed under low-priority since it's the only such test.
7. **`stubBroker.Subscribe` then immediate `nc.Flush()`** — not a sub-leak, but could miss the very first request if the test client fires before the flush completes. p10/upgrade_all_test.go does this correctly via flush.
