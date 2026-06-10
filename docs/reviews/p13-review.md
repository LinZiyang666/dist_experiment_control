# P13 Consolidated Review — `tether` proxy subscription

Six adversarial reviews consolidated. I re-read every cited file and verified each claim against the actual code. Findings are deduped, grouped by severity, with the raising dimension(s) noted. False positives and confirmations-of-correctness are listed at the end.

---

## BLOCKERS

### B1. Agent-side fail-closed 15-min NATS-partition watchdog is not implemented
**Where:** `internal/agent/agent.go:661-702` (`buildConnOptions`), `internal/agent/proxy.go` (no teardown timer).
**Raised by:** R1 (major), R3 (major), R5 (blocker), R6 (blocker).
**Verified:** `buildConnOptions` installs only `nats.ReconnectHandler` (agent.go:669). There is no `nats.DisconnectErrHandler`, no timer, no `time.Minute`/`AfterFunc` anywhere on the proxy path. The only `Stop()` of `p.srv` is via an explicit disable directive (proxy.go:56,136) which can only arrive over NATS.
**Why it's wrong:** The locked plan (§0 grace, §4, §9, R-1' residual control) mandates an agent-side backstop: if the agent process stays up but its NATS link is partitioned ≥15min, it must proactively `Stop()` the SS server. The public data path (broker:14xxx → yamux tunnel → 127.0.0.1 SS) is a *separate* connection from the NATS control link. During a NATS partition the agent receives no keyset/disable deltas, so a revoked subscriber whose Clash client cached host:port+PSK keeps egressing. The only remaining backstop is the broker-side 15-min OFFLINE→port REVOKE — and that REVOKE is DB-only (`reconcilePorts` updates `port_allocations.state` + emits events but does not close the tunnel public listener; the listener drops only when the yamux session closes, and `tunnelTokenLookup` is consulted only at Open, never per public connection). So if NATS is dead but the tunnel TCP survives, egress is unbounded. This is the headline residual-threat control and it is silently absent.
**Fix:** Add a `nats.DisconnectErrHandler` that arms a 15-min timer anchored to `runCtx`; on fire (still disconnected) call `applyProxyDirective(ctx, nc, nil)` to `Stop()` SS + clear footprint. Cancel/reset the timer in `ReconnectHandler`. Reuse the broker's `PortRevokeAfter` knob rather than hardcoding 15min so the two layers stay aligned. Add a concurrency test with an injectable clock.

### B2. Full-directive and teardown branches are NOT epoch-guarded — a stale register-reply can resurrect a revoked key or re-enable after off
**Where:** `internal/agent/proxy.go:55-67` (teardown branch + `case d.Token != ""`).
**Raised by:** R2 (major).
**Verified:** Only the keyset-only running-server branch checks the epoch (`if d.Epoch != p.epoch`, proxy.go:84). The `d.Token != ""` full branch (proxy.go:63-67) and the `d == nil || !d.Enabled` teardown branch (proxy.go:55-58) apply *unconditionally* and set `p.epoch` to the directive's possibly-older value. `register-resp` rides the `_INBOX` reply subject; live keyset/disable pushes ride `...proxy-keys.req.forwarded` — two subjects with no cross-ordering guarantee.
**Why it's wrong:** Race: agent reconnects → broker computes register-resp `{epoch=5, keys=[K1], token T}`; meanwhile owner runs `sub revoke K1` (epoch→6, pushes `{epoch=6, keys=[]}`). If the epoch-6 push lands first, the agent (srv==nil + footprint) bootstraps at epoch 6 keys=[]; then the epoch-5 register-resp arrives, the full path rebuilds with keys=[K1] at epoch 5 — the revoked key is live again until the next bump. Symmetric: a stale enabled directive re-enables after `proxy off`. This violates the locked "per-subscriber revoke is a HARD cut" decision, so I rate it blocker-equivalent (R2 filed major; promoted on dedup because it directly defeats a locked security invariant on a reachable reconnect race).
**Fix:** Guard the full-establish and teardown branches so a directive with `Epoch < p.epoch` is ignored (keep `!=` only for the keyset-swap rewind tolerance). Carry a monotonic epoch on disable directives too. Test: deliver newer keyset push then older full register-resp; assert the revoked key stays cut.

### B3. Keystone `TestProxyAuditNoSecrets` is missing
**Where:** test tree (absent).
**Raised by:** R3 (confirmation that secrets are clean today), R6 (blocker).
**Verified:** grep for `NoSecret`/`AuditNoSecrets` across `*_test.go` is empty; no test subscribes `sys.events` and asserts PSK-absence. The secret-flow itself is currently clean (R3 verified the matrix — PSK/token never hit sys.events/audit/history/logs/state.json), but the regression net the plan names as an explicit exit criterion does not exist. The live exposure surface (`pubSysEvent("proxy_keyset_changed"/"proxy_enabled")` is member-readable; `pubAuditCall` logs `sub_id`/`affected_nodes`) means a future careless edit could leak a PSK with nothing to catch it.
**Fix:** Add `test/security/proxy_no_secrets_test.go`: run on→sub create→/sub fetch→revoke→register; capture create-response token, agent `state.json`, and all audit/history/sys.events payloads; assert raw token, base64 PSK, and full `/sub` URL appear in none; assert `state.json` carries tunnel token+port+epoch but no PSK.

### B4. `dropSessionRows` proxy_subscribers cascade has no rm-path test
**Where:** `internal/broker/audit.go:89-105` (DELETE present at :98); test absent.
**Raised by:** R6 (blocker).
**Verified:** `dropSessionRows` DOES include `DELETE FROM proxy_subscribers WHERE sid = ?` (audit.go:98) — the cascade is implemented correctly. But no test drives the real `finalizeSessionRm`/`dropSessionRows` path for proxy_subscribers (the only hit in `*_test.go` is a `token_hash` COUNT inside `proxysub_test.go`, which is the proxysub package's own unit test, not the rm cascade). If the DELETE line were ever dropped, orphaned subscriber rows would survive into a rebuilt same-sid session (token reuse / cross-session leak) with no failing test.
**Fix:** Add `internal/broker/session_rm_proxy_test.go`: seed session+subscribers (+`__proxy__` alloc), call real `finalizeSessionRm`/`dropSessionRows`, assert `COUNT(*) proxy_subscribers WHERE sid=? == 0`, `/sub`→404 for the old token, and that a rebuilt same-sid + new subscriber does not resolve the old token.

---

## MAJORS

### M1. `enableProxy` always re-mints port+token for every online node (no reuse) — re-affirming `proxy on` churns all subscribers' in-flight conns
**Where:** `internal/broker/proxy.go:58-94` (`enableProxy`, esp. :59 discards `changed`, :79 `AllocateProxy`), `internal/port/port.go:531-576` (`AllocateProxy` always FREEs+reINSERTs).
**Raised by:** R2 (major).
**Verified:** `enableProxy` discards `SetProxyEnabled`'s `changed` return (`if _, err := session.SetProxyEnabled(...)`, proxy.go:59) and unconditionally calls `port.AllocateProxy` for every online node. Unlike `proxyDirectiveForRegister` (proxy.go:287-294) it does NOT consult `LookupProxyByNode` for a keep path. `AllocateProxy` always `UPDATE ... state='FREED'` then INSERTs a fresh port+token (port.go:540-562). The fresh `Token != ""` directive forces the agent's full rebuild path (B2's branch), tearing down the SS server + tunnel and dropping every subscriber's in-flight connection on every agent. A double-clicked or scripted idempotent `proxy on` becomes a session-wide proxy outage + token rotation.
**Fix:** In `enableProxy`, mirror the register keep-vs-replace: for each online node `LookupProxyByNode` first; if an ALLOCATED row exists, push a keyset-only directive (`Token=""`, `PublicPort=existing.Port`) instead of `AllocateProxy`. Short-circuit when `changed==false`. Test: `proxy on` twice keeps the same port+token_hash, no rebuild on the second call.

### M2. Dropped keyset-push has no broker-side reconciliation — a revoked key stays live on a still-connected agent indefinitely
**Where:** `internal/broker/proxy.go:349-379` (`bumpAndPushKeyset`/`pushProxyDirective`), `internal/broker/broker.go` reconcile ticker (no proxy keyset path).
**Raised by:** R2 (major).
**Verified:** `bumpAndPushKeyset` does a single un-acked `nc.Publish` per online node; `pushProxyDirective` swallows publish errors with only a Warn (proxy.go:376-378). No retry, no periodic proxy-keyset reconcile (`reconcilePorts` handles ports only; heartbeats carry no epoch). If the single revoke push is dropped (transient NATS, or the agent is in the `srv==nil` window where keyset-only pushes are dropped at proxy.go:75-77), the revoked key stays live until the agent happens to disconnect+reconnect — possibly hours. The 15-min fail-closed (B1, also missing) only fires on disconnection, not a missed in-band revoke. Undermines the HARD-cut guarantee for connected agents.
**Fix:** Add a lightweight repair path: (a) carry `proxy_epoch` in the heartbeat reply and have the agent re-register when its applied epoch lags, or (b) a broker reconcile tick that re-pushes the authoritative keyset to ONLINE+ready nodes, or (c) bounded request/reply retry on the push. Test: drop the first push, assert convergence without a reconnect.

### M3. `proxy_ready` is never re-ACKed on the keyset-only reconnect path → node can be permanently absent from /sub
**Where:** `internal/agent/proxy.go:81-92` (epoch-swap branch never calls `pubProxyReady`), `:124` (`pubProxyReady` only in `proxyStartLocked`).
**Raised by:** R5 (major).
**Verified:** `pubProxyReady(true)` is published only from `proxyStartLocked` (proxy.go:124); it is best-effort (`nc.Publish`, no reply/retry, proxy.go:180-189). The epoch-swap default branch (proxy.go:81-92) swaps keys but never re-ACKs. If the single ready publish is lost, or `nodes.proxy_ready` is 0 for any reason while the SS server is actually running, every subsequent directive is a keyset-only delta landing in the no-ACK branch. `LiveProxyNodes` (gated on `proxy_ready=1`, subhttp.go:124) then silently excludes the correctly-serving node from `/sub` forever.
**Fix:** Re-affirm readiness when the agent holds a running server and applies a directive — call `pubProxyReady(nc, true)` at the end of the epoch-swap branch, and have `onNATSReconnect` re-ACK when `p.srv != nil`.

### M4. No P13 capability gate — broker allocates `__proxy__` ports and churns them for pre-P13 agents
**Where:** `internal/broker/proxy.go:78-90` (`enableProxy`), `:269-304` (`proxyDirectiveForRegister`), `:381-393` (`onlineNIDs`).
**Raised by:** R2 (minor), R4 (minor), R5 (major).
**Verified:** `onlineNIDs` filters only on `status='ONLINE'` (proxy.go:388); neither `enableProxy` nor `proxyDirectiveForRegister` inspects `req.ReleaseVersion` / nodes.release_version (the field exists, messages.go:29,233). Plan §5 mandates allocate/push only to nodes `release_version >= P13 release`. The render gate (`proxy_ready=0`) correctly keeps a pre-P13 agent out of `/sub` (no exposure bug), so this is major-not-blocker. But: a pre-P13 agent never reports `__proxy__` in its register LocalPorts, so on *every* register `proxyDirectiveForRegister` finds no token-hash match and falls to `AllocateProxy` (proxy.go:295) which FREEs+remints — burning a 14xxx port slot and emitting alloc/free audit+events on every registration. A flapping legacy agent slowly drains the finite port band.
**Fix:** Add a minimal-semver gate: skip `AllocateProxy`+push for nodes below the P13 release in both `enableProxy`'s loop and `proxyDirectiveForRegister` (return nil). Registrant's `ReleaseVersion` is in `req`; for the enable broadcast read it from `node.List`. Test: enable with one pre-P13 + one P13 node → exactly one `__proxy__` ALLOCATED row; re-register pre-P13 twice → no churn.

### M5. New-ctl/old-broker skew: `nats.ErrNoResponders` blended into a generic "broker unreachable" instead of the `proxy_unsupported_broker` hint
**Where:** `cmd/tether/proxy.go:82-85` (`proxyRequest`).
**Raised by:** R4 (major).
**Verified:** `proxyRequest` wraps every request error identically as `proxy %s: request: %w (broker unreachable on NATS)` (proxy.go:84). The sibling `ps.go:84` correctly distinguishes `errors.Is(err, nats.ErrNoResponders)`. A new ctl talking to a pre-P13 broker (no subscriber for `...proxy.*`) gets `ErrNoResponders` and is told the broker is unreachable when it is reachable but lacks the proxy control plane — the one user-visible failure mode of the additive-no-bump design, mis-reported. The `proxy_unsupported_broker` hint named in plan §8 is also absent from `error_hints.go`.
**Fix:** In `proxyRequest`, before the generic wrap: `if errors.Is(err, nats.ErrNoResponders) { return fmt.Errorf("proxy: this broker predates P13 / has no proxy support — upgrade tetherd (%w)", err) }`. Test the no-responders message.

### M6. `proxy_ready` is never cleared on STALE/OFFLINE — a flapped node renders as a live exit before the agent re-ACKs
**Where:** `internal/node/node.go:113-126` (`Heartbeat` sets ONLINE, no proxy_ready touch), `:133-175` (`ReconcileStates` updates only status), `internal/subhttp/subhttp.go:123-125` (gate).
**Raised by:** R2 (minor — stale status), R5 (minor), R6 (major).
**Verified:** `ReconcileStates` UPDATE touches only `status` (node.go:168); `Heartbeat` flips `status='ONLINE'` without touching `proxy_ready` (node.go:115). `proxy_ready` is set only by the agent ACK and cleared only by explicit `disableProxy` (online nids only) or unready ACK. So a node that went OFFLINE (SS torn down on process restart) and then beats back to ONLINE returns with stale `proxy_ready=1`; `LiveProxyNodes` (status='ONLINE' AND proxy_ready=1) renders it as a live exit before the agent re-binds and re-ACKs. Subscribers briefly get a dead exit node — exactly the black hole the render gate was meant to prevent. (Composes with M3: once both are fixed, the node reappears promptly after re-ACK.) Also note `disableProxy` clears `proxy_ready` only for currently-online nids (proxy.go:114-116), so an OFFLINE-during-off node keeps a stale Ready=true in `proxy status`.
**Fix:** Clear `nodes.proxy_ready=0` on the ONLINE→STALE/OFFLINE transition in `ReconcileStates` (and/or on the STALE→ONLINE recovery in `Heartbeat`). On `disableProxy`, clear `proxy_ready` for ALL session nodes, not just online ones. Tests: reconcile-to-OFFLINE clears proxy_ready; flapped node not rendered until fresh ACK.

### M7. Register-time directive + keep-vs-replace churn-avoidance path is completely untested
**Where:** `internal/broker/proxy.go:269-304` (`proxyDirectiveForRegister`); test absent.
**Raised by:** R6 (blocker — rated major on dedup since the code is correct, only the test net is missing).
**Verified:** No `*_test.go` references `proxyDirectiveForRegister` or drives `handleRegister` with proxy enabled. `proxy_port_test.go` tests only `AllocateProxy` (always free+remint) and `LookupProxyByNode`; the e2e enables proxy after the agent is already ONLINE, so the register path returns nil and the keep-branch (proxy.go:287-294, Token empty + same PublicPort) is never reached. A regression dropping the keep-branch (always reminting) would break every running agent's tunnel on each reconnect with no failing test.
**Fix:** `internal/broker/proxy_register_test.go`: (a) enable with node not-yet-registered → register → assert full directive + one ALLOCATED row; (b) keep-branch: pre-allocate, register with matching `__proxy__` token_hash → assert `Token==""` and same PublicPort, no new alloc; (c) replace-branch: mismatched/absent hash → fresh token+port, old row FREED.

### M8. CLI `cmd/tether/proxy.go` has zero tests
**Where:** `cmd/tether/proxy.go` (whole file); no `proxy_test.go`.
**Raised by:** R6 (major).
**Verified:** No `cmd/tether/*_test.go` references proxy. Untested no-network logic: `confirmProxyOn` non-TTY abort (plan §8 hard requirement — DB flag must stay unchanged when `proxy on` aborts; verified the abort returns before sending the request, proxy.go:96-100), `sub create --name` required, `sub revoke` positional/--name handling, status render, and the proxy error-hint mapping. `error_hints_test.go` asserts only `not_owner` — none of `sub_name_invalid`/`sub_name_taken`/`sub_not_found`/`already_revoked`.
**Fix:** Add `cmd/tether/proxy_test.go` (non-TTY abort sends no request; name validation) and `error_hints_test.go` cases for the 4 proxy codes.

### M9. Caddy `/sub` route ordering / WSS-still-upgrades (matrix item 16) is unasserted
**Where:** `scripts/install.sh` (~`handle /sub/*`), `test/p10/install_sh_test.go` (no `/sub`/8090 assertions).
**Raised by:** R6 (major).
**Verified:** grep of `test/p10/install_sh_test.go` finds no `/sub`, `8090`, or `sub.listen` assertions. A reorder/typo that shadows the NATS WSS catch-all (breaking every agent/ctl connection) would pass CI; the in-process e2e bypasses Caddy entirely.
**Fix:** Extend `test/p10/install_sh_test.go`: assert path-scoped `handle /sub/*` → `reverse_proxy 127.0.0.1:8090`, `broker.yaml` `sub.listen 127.0.0.1:8090`, and that the WSS catch-all to `:8222` is still present and unshadowed. Record the manual WSS-upgrade check in `log.md`.

### M10. Broker-restart / agent-restart (state.json replay) convergence untested
**Where:** `internal/agent/state.go` (`SetProxy`/`GetProxy`/`ClearProxy`), `internal/agent/proxy.go:69-79` (footprint bootstrap); tests absent.
**Raised by:** R6 (major).
**Verified:** No agent test references `SetProxy`/`GetProxy`/`ProxyState`; `proxy_apply_test.go` exercises the in-memory machine but never the persisted-footprint bootstrap (proxy.go:74-79, `case p.srv == nil`) with a real stateStore. Plan §9 names both replay paths.
**Fix:** `internal/agent/state_proxy_test.go` (round-trip + nil-clear + legacy state.json without proxy key still loads) and an agent restart-replay test (persist ProxyState, fresh Agent over same Home, send keyset-only push, assert SS bootstraps at the same PublicPort).

---

## MINORS

### m1. `persistProxyEpochLocked` re-reads the token from disk and can clobber the footprint to an empty token
**Where:** `internal/agent/proxy.go:150-157`, `:84-91`.
**Raised by:** R1 (minor).
**Verified:** `proxyRuntime` (proxy.go:24-30) does NOT cache the token. On an epoch-only swap, `persistProxyEpochLocked` rebuilds `ProxyState` using `a.loadProxyTokenSafe()` (re-reads state.json). On a transient read error `loadProxyStateSafe` returns nil → token `""`, and the function writes back `ProxyState{Token:"", Epoch:newEpoch}`, clobbering a good footprint. The restart bootstrap then bails at `ps.Token == ""` (proxy.go:75), so the agent never re-binds on restart until a fresh full directive arrives.
**Fix:** Cache the token in `proxyRuntime` at `proxyStartLocked` time and use it directly; at minimum skip the write when the re-read token is empty.

### m2. `AllocateProxy` passes the chosen port (not 0) to `translateInsertErr`, mislabeling an auto-path collision as `ErrPortTaken`
**Where:** `internal/port/port.go:566` (passes `p`), `:412-417` (`translateInsertErr`).
**Raised by:** R2 (minor).
**Verified:** `AllocateProxy` uses `findFreePort` (auto path, proves free in-tx) then `translateInsertErr(err, p)` with non-zero `p`. `translateInsertErr` maps any UNIQUE violation to `ErrPortTaken` when `desiredPort != 0`. The documented contract (port.go:403-406) is that an auto-path UNIQUE violation is impossible-by-construction and MUST surface loud as `port: insert`. Passing `p` inverts that, silently relabeling a serialization/invariant breakage. Currently unreachable under `SetMaxOpenConns(1)`, but the mapping is semantically wrong and the comment at port.go:564 is misleading (the proxy (sid,nid)-index collision is also unreachable since `AllocateProxy` FREEs first).
**Fix:** Pass `0` to `translateInsertErr` in `AllocateProxy`; fix the comment.

### m3. `relay()` full-closes both directions on first EOF, breaking TCP half-close
**Where:** `internal/agent/ssproxy/server.go:339-353`.
**Raised by:** R1 (minor).
**Verified:** On `client→remote` EOF the goroutine calls `remote.Close()` (server.go:345) and symmetrically `client.Close()` — a hard full-close on first EOF in either direction. A client that half-closes its send side then waits for a response (HTTP/1.0 `Connection: close`, some RPC) gets the still-pending response truncated. Faithful SS relays use `CloseWrite()` (shutdown SHUT_WR). Usually masked by browsers keeping both halves open, hence minor.
**Fix:** Use `CloseWrite()` on each direction's `*net.TCPConn` after `io.Copy`, full-`Close` only after `wg.Wait()`.

### m4. `disableProxy` direct Free of proxy ports emits no `audit.port` row (audit-completeness gap)
**Where:** `internal/broker/proxy.go:107-113`.
**Raised by:** R2 (minor).
**Verified:** The free loop emits only `pubPortEvent(...,"freed")` (proxy.go:111), not `pubAuditPort`. Every other Free path (expose-rm at expose.go:339, reconcile-revoke at expose.go:367) emits both. A `proxy off` recycling N ports leaves no `audit.port` freed entries — a gap for a security-relevant teardown.
**Fix:** Add `b.pubAuditPort(sid, "freed", a.NID, a.Port, port.ProxyPortName, 0, fp, b.cfg.Now())` alongside the event. Extend the audit test to assert proxy-off produces freed rows (still secret-free).

### m5. `handleProxyReadyEvent` trusts raw sid/nid from the subject without B.5 validation (parser-parity gap)
**Where:** `internal/broker/proxy.go:253-262`.
**Raised by:** R4 (minor).
**Verified:** Hand-parses with `splitDot` + positional checks but passes `p[3]` (sid) and `p[6]` (nid) into `SetProxyReady` with no `ValidateSID`/`ValidateNID`; `p[8]` (kind) is unconstrained (anything != "ready" silently clears). Every sibling parser (`ParseEvProc` etc.) validates these tokens. The agent's `ev.node.<nid>.>` JWT pin makes a forged identifier unreachable, so not exploitable — but it breaks the uniform "malformed token never reaches a handler as opaque string" invariant. No `ParseEvNodeProxy` helper exists.
**Fix:** Add `ParseEvNodeProxy(subject)` in `proto/subjects.go` mirroring `ParseEvProc` (exact len + ValidateSID + ValidateNID + kind ∈ {ready,unready}); call it and drop on `!ok`.

### m6. `applyReconciliation` can't act on a `RevokePorts` entry for `__proxy__` (latent footgun)
**Where:** `internal/agent/agent.go:575-582`.
**Raised by:** R5 (minor).
**Verified:** `buildLocalSnapshot` reports `__proxy__` from `sf.Proxy` (agent.go:548-555), so the broker can place it in `RevokePorts`. But `applyReconciliation` builds `byPort` only from `sf.PortTokens` (agent.go:575-578); the `__proxy__` footprint lives in `sf.Proxy`, so a RevokePorts hit on it is silently skipped (`continue`, agent.go:581-582). Masked today because the ProxyDirective path independently tears down. Latent: the two reconcile paths disagree on who owns `__proxy__`, and a misleading "reconciled" audit can fire for a port the reconciler can't act on.
**Fix:** Filter `name==ProxyPortName` out of the broker reconcile's RevokePorts emission (the Proxy directive is authoritative), or have `applyReconciliation` also consult `sf.Proxy`.

### m7. `subject_malformed` has no operator hint in `error_hints.go`
**Where:** `cmd/tether/error_hints.go:47-50`.
**Raised by:** R4 (nit), R6 (noted).
**Verified:** `error_hints.go` has the 4 `sub_*` hints but not `subject_malformed`, which IS emitted by the proxy handlers (proxy.go:38,126,157,206). `brokerErrorMessage` falls back gracefully, so cosmetic. `proxy_disabled` (also in plan §8) is emitted by no handler — drop it from the plan rather than add a dead hint.
**Fix:** Add a `subject_malformed` hint; remove `proxy_disabled` from the plan's hint list.

### m8. Test theater: `TestProxySubjectMalformedRejected` asserts almost nothing
**Where:** `internal/broker/proxy_test.go:157-172`.
**Raised by:** R6 (minor).
**Verified:** Sends `...proxy.set.req.extra` (won't match the registry wildcard) and accepts both a no-responders early-return AND any reply with code `''` or `subject_malformed`. It almost always returns on no-responders and never exercises the parser's `subject_malformed` branch or the malformed-before-DB ordering.
**Fix:** Call `b.handleProxySet`/`handleProxySub` directly with a wrong-token-count subject; assert `Code=="subject_malformed"` AND no DB mutation. Drop the no-responders escape.

### m9. `subhttp` test `readBody` does a single 64KiB `Read` — can truncate and weaken the 404-oracle assertion
**Where:** `internal/subhttp/subhttp_test.go:172-178`.
**Raised by:** R6 (minor).
**Verified:** `readBody` does one `resp.Body.Read(buf)` and returns `buf[:n]` (subhttp_test.go:176). A single Read isn't guaranteed to return the full body; the unknown-vs-revoked 404 byte-equality (`TestSubNoExistenceOracle`) and render contains/excludes could pass/fail for the wrong reason. Low risk at tiny sizes but undermines the oracle assertion it exists to make airtight.
**Fix:** Use `io.ReadAll(resp.Body)`.

### m10. Multi-node `/sub` render ordering + stability unasserted
**Where:** `internal/subhttp/subhttp_test.go:56-92` (seeds one node).
**Raised by:** R6 (minor).
**Verified:** `LiveProxyNodes`/`proxyStatusNodes` `ORDER BY nid` for stable Clash output; the test seeds exactly one ONLINE+ready node, so deterministic multi-node ordering is never exercised. A dropped `ORDER BY` would churn a subscriber's saved profile node order with no failing test.
**Fix:** Seed 3 ONLINE+ready nodes inserted out of nid order; assert lexicographic render order and byte-identical consecutive GETs.

### m11. No test for disabled-then-create → enable activation
**Where:** `internal/broker/proxy.go:161-182` (create while off), `:72` (`activeProxyKeys` on enable).
**Raised by:** R6 (minor).
**Verified:** `broker/proxy_test.go` creates subs only while ON. `bumpAndPushKeyset` early-returns when disabled (proxy.go:351), and `enableProxy` calls `activeProxyKeys`; the path where a sub created while OFF is included in the first enable's keyset is unasserted (plan matrix item 4).
**Fix:** With proxy OFF, `sub.create alice`; `proxy on`; assert the pushed directive's Keys include alice's SubID.

### m12. `subhttp.Server` lifecycle leak gate missing (matrix item 11)
**Where:** `test/concurrency/` (no subhttp test).
**Raised by:** R6 (minor).
**Verified:** ssproxy leak (item 10) and proxyMgr concurrency (item 12) exist; nothing starts the subhttp listener and asserts ctx-cancel cleanly shuts it down (the `net/http.Server` is anchored via `context.AfterFunc(ctx, srv.Shutdown)`). A broken AfterFunc wiring would not be caught.
**Fix:** `test/concurrency/subhttp_lifecycle_test.go`: start on `127.0.0.1:0` under cancelable ctx, GET a few times, cancel, poll `NumGoroutine()` back to baseline, assert listener closed.

---

## NITS (and confirmed non-defects)

- **n1. ssproxy `Start` ctx-watch goroutine not tracked by `wg`** (R1 nit) — verified: server.go:91-94 spawns `go func(){ <-s.ctx.Done(); s.Stop() }()` without `wg.Add(1)`. It self-exits on `Stop()`, but the leak test relies on a +3 tolerance that papers over exactly this. Fix: `wg.Add(1)`/`defer Done()` so the gate can use zero tolerance.
- **n2. `--yes` silently bypasses the open-exit-node liability text** (R3 nit) — verified: the warning is printed only inside `confirmProxyOn`, never under `--yes`. By design (opt-in), but a one-line stderr notice under `--yes` would log the liability in automation output. Not a defect.
- **n3. `subhttp` 404 timing differential** (R3 nit, confirmed non-exploitable) — unknown does 1 DB query, valid+active does up to 4 + render; with 256-bit tokens not practically exploitable. Document alongside R-5; no code change for v1.

### Confirmed CORRECT (reviewers verified, no defect — keep as regression keystones)
- **Secret-flow matrix clean** (R3): PSK/tunnel token never reach sys.events/audit/history/logs/state.json; full directive (Token+PSKs) travels only on register-reply `_INBOX` and the per-(sid,nid) `proxy-keys.req.forwarded` push. `state.json` persists only {PublicPort,LocalPort,Token,Epoch}, never PSK. (Lock with B3's test.)
- **JWT scoping + revoke isolation** (R3): the 5 added `ActivatedMember` Pub literals are pinned to `by.<actor>.s.<sid>` (no wildcard); ctl cannot Sub `...cmd.*` so cannot harvest the keyset push. `proxysub.Revoke` flips one ACTIVE→REVOKED row; `setKeysLocked` (server.go:117-136) force-closes only conns whose KeyID vanished — a genuine per-subscriber hard cut. `bindKeyConn` re-checks liveness under the mutex (TOCTOU closed). Verified directly.
- **`dropSessionRows` cascade IS implemented** (audit.go:98) — the DELETE is present; only the rm-path *test* is missing (B4).

### False-positive / over-stated notes
- R2's "epoch collision" same-epoch-shrinking-keyset case (R1 minor) is a genuine edge but requires a broker epoch rewind that reuses a value for a smaller keyset; lower priority than B2 and covered by the same "always SetKeys on keyset-only directive" fix direction. Folded into B2's test guidance rather than listed separately.
- R2's claim that `proxyDirectiveForRegister`'s proxy-unique-index collision "surfaces ErrPortTaken" — the index is unreachable because `AllocateProxy` FREEs first; this is a misleading comment, captured under m2, not a separate behavioral defect.

---

## 主进程评估与处置（CLAUDE.md §3 step 5）

主进程逐条评估上述 6 专家 + 综合稿的 finding，采纳/驳回如下。**只有主进程改实现**；专家提出的测试条目由主进程整合。

### 采纳并已修复（blocker）
- **B1 fail-closed 15min** — 采纳。agent 加 `nats.DisconnectErrHandler` + 一个 `failClosed` 计时器(锚到 runCtx)：持续断连达 `PortRevokeAfter`(默认 15min,与 broker 侧 REVOKE 阈值对齐)未重连 ⇒ 主动 `applyProxyDirective(nil)` 停 SS;`ReconnectHandler` 取消/重置计时器。
- **B2 full/teardown 分支未 epoch-guard** — 采纳。`applyProxyDirective` 顶部加全局陈旧丢弃:`d.Epoch>0 && d.Epoch < p.epoch ⇒ return`;disable 也带 epoch(broker `disableProxy` 先 bump 再下发),所以 enable/keyset/disable 全程单调 epoch,杜绝陈旧 register-reply 复活已撤 key 或 off 后被重开。
- **B3 TestProxyAuditNoSecrets keystone** — 采纳,新增 `internal/broker/proxy_no_secrets_test.go`:跑 on→sub create→revoke,断言 raw token / PSK / 完整 /sub URL 不出现在 audit/sys.events/任何 reply。
- **B4 session-rm 级联无测试** — 采纳,新增 `internal/broker/session_rm_proxy_test.go`:走真实 `dropSessionRows`,断言 proxy_subscribers 清零 + 旧 token 不再解析。

### 采纳并已修复（major / 真实正确性·安全）
- **M1 `proxy on` 非幂等(重铸毁连)** — 采纳。`enableProxy` 改 keep-vs-replace:已有 ALLOCATED `__proxy__` 则复用端口、只重推 keyset(不重铸 token、不毁在飞连接)。
- **M2 丢失的 keyset push 无 broker 侧修复** — 采纳。`HeartbeatPayload` 加 `ProxyEpoch`;broker `handleHeartbeat` 见 agent epoch < session epoch 即补推当前 keyset(把"撤销陈旧"窗口从"下次重连"压到一个心跳)。
- **M3 keyset-only/重连路径不重发 proxy_ready** — 采纳。epoch-swap 分支末尾 + 重连重注册后,server 在跑则重发 `proxy.ready`。
- **M5 新 ctl/旧 broker skew 误报** — 采纳。`proxyRequest` 区分 `nats.ErrNoResponders` ⇒ `proxy_unsupported_broker` 升级提示。
- **M6 proxy_ready 不随 OFFLINE 清除** — 采纳。`node.ReconcileStates` 在转 OFFLINE 时清 `proxy_ready=0`;`disableProxy` 清全节点(不止在线)。

### 采纳并已修复（minor / 低成本正确性）
- **m1 persistProxyEpochLocked 可能把 token 写空** — 采纳。token 缓存进 `proxyRuntime.token`,不再每次回读磁盘。
- **relay 首 EOF 全关破坏 TCP 半关闭** — 采纳,改 `CloseWrite()` 传播半关闭。
- **ssproxy ctx-watch goroutine 未入 wg** — 采纳,纳入 wg,leak 门零容忍。
- **handleProxyReadyEvent 不校验 sid/nid、不 gate proxy_enabled** — 采纳,加 `ValidateSID/NID` + 仅在 enabled 时置位。
- **AllocateProxy 把自动路径端口当 desiredPort 传 translateInsertErr** — 采纳,传 0,碰撞响亮报错而非 ErrPortTaken。
- **error_hints 缺 subject_malformed/proxy_disabled** — 采纳补齐。
- **subhttp readBody 单次 64KiB Read** — 采纳,改 `io.ReadAll`。
- **TestProxySubjectMalformedRejected 是 theater** — 采纳,改直连 handler 断言 `subject_malformed` 早于任何 DB/owner 变更。

### 驳回 / 记为已知限制（附理由）
- **M4 P13 capability gate(release_version ≥ P13)** — **本期不做**,记为已知效率限制。理由:(a) 纯效率问题,非正确性/安全 —— 渲染门是 `proxy_ready` ACK,pre-P13 agent 永不 ACK 故永不被渲染;(b) M1 的 keep-vs-replace 已消除"每次 register 重铸"churn 的主要来源;(c) tether 实际规模是数十个**同版本**(滚动升级)agent,端口段 1000 充裕;(d) 引入语义化版本比较是新依赖面。若未来出现混版本大集群再加。已在 plan §5 与本处显式记录。
- **B2 的 disable 仍可被"同 epoch"重复 disable** — 非问题:disable 幂等(teardown 幂等)。
- **专家"AddProxy 失败不发 unready"** — 低优先:AddProxy 失败时 server 已 Stop、未持久化、未 ACK ready,broker 侧 proxy_ready 保持 0,渲染门正确;不额外发 unready(本就没 ready 过)。
- **half-epoch 同值缩 keyset 不收敛(m,边缘)** — 由 M2 心跳补推兜底:broker 永不对不同 keyset 复用同 epoch(每次 sub 变更都 `BumpProxyEpoch`),且心跳修复覆盖丢包,故同值缩 keyset 在正常路径不可达;加测试钉住语义。

### 专家确认无缺陷（保留为回归保障）
- JWT 作用域:任何 ctl 模板都不能 sub/pub `proxy-keys` 推送或他人 proxy subject;撤销隔离为真。
- PSK / tunnel token 不出现在 sys.events / audit.call / history / 日志 / state.json。
- subhttp 404 存在性 oracle 已闭合(unknown == revoked == DELETING)。

---

## 整合验证（step 5 收尾）

主进程已按上述处置修改实现并整合测试。**新增/改写的测试：**
- `internal/broker/proxy_no_secrets_test.go`（B3 keystone：member 通道无 token/PSK）
- `internal/broker/session_rm_proxy_test.go`（B4：真实 dropSessionRows 级联 + 旧 token 不复活）
- `internal/agent/proxy_apply_test.go`：`TestFailClosedTearsDownAfterGrace` / `TestFailClosedCancelledByReconnect`（B1）、`TestStaleDirectiveDropped`（B2）
- `internal/agent/ssproxy/ssproxy_test.go`：`TestServerLargePayloadSpansChunks`、`TestConcurrentSetKeysDuringTraffic`（边界+并发）
- `internal/broker/proxy_test.go`：`TestProxySubjectMalformedRejected` 改写为真实 subject_malformed + 无 DB 变更断言（替换 theater）

**实现改动（按 finding）：** B1 `agent.armFailClosed/cancelFailClosed/failClosedFire` + `DisconnectErrHandler`（含关停期不重武装的 runCtx 守卫 + Run defer cancel）；B2 `applyProxyDirective` 顶部陈旧丢弃 + disable 带 epoch；M1 `enableProxy` keep-vs-replace；M2 `HeartbeatPayload.ProxyEpoch` + `broker.repairProxyEpoch`；M3 keyset/重连重发 ready；M5 CLI `ErrNoResponders` 区分；M6 `node.ReconcileStates` OFFLINE 清 `proxy_ready` + disable 清全节点 + `handleProxyReadyEvent` 校验/gate；minors：token 缓存、relay 半关闭、ssproxy ctx-watch 入 wg、`translateInsertErr(err,0)`、error_hints 补 `subject_malformed`/`proxy_disabled`、subhttp `io.ReadAll`。

**闸门状态（本机 macOS）：** `CGO_ENABLED=0 go build ./...` ✅；`golangci-lint v2.5.0` **0 issues** ✅；P13 全部包 + `test/p13` e2e `go test` ✅；并发面 `-race` ✅。仓内其余失败均为**既有 macOS 环境问题**（`/private/var` 符号链接、`/var/folders` Unix socket 路径超 104 字节、`--role agent` 不支持 macOS），与 P13 无关、在干净 base 上同样复现 —— 已 stash 验证。**Linux CI 上 `make test`/`make lint` 应全绿。**

**未做（已知限制，记录在案）：** M4 capability gate（release_version ≥ P13）—— 纯效率、非正确性/安全，留待混版本大集群再加（理由见上"驳回/记为已知限制"）。
