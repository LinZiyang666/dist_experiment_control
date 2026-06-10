**RESULT: FAIL** — *主进程已逐条整改,见文末「## 主进程回复（外审整改）」。请复审。*

# P13 External Review

Date: 2026-06-10
Reviewer role: external reviewer

## Scope

Reviewed the full uncommitted P13 change set on branch
`phase/13-proxy-subscription`, with emphasis on:

- `docs/reviews/p13-plan.md` and `docs/reviews/p13-review.md`;
- broker proxy state, subscriber lifecycle, register/reconnect convergence;
- agent proxy runtime, Shadowsocks server, tunnel and fail-closed behavior;
- subscription HTTP security boundary and Caddy wiring;
- migrations, ACLs, audit secret handling, CLI and phase acceptance tests.

## Verdict

P13 is not approved.

The storage model, secret separation, multi-key SS implementation, ACL
literals, revoke isolation and basic happy path are coherent. However, the
master switch is not a convergent kill switch, the agent runtime has a real
initialization race, and several documented security/compatibility contracts
are not implemented. The submitted P13 e2e is also flaky and does not prove
that `proxy off` stopped the agent.

## Findings

### F1 - High: a lost `proxy off` directive is never repaired

`disableProxy` marks the session OFF, frees the allocation and replies success
after one best-effort core-NATS publish (`internal/broker/proxy.go:108-138`).
Heartbeat repair explicitly returns while the switch is OFF
(`internal/broker/proxy.go:421-445`).

Freeing the SQLite row does not close an already registered tunnel. The tunnel
server keeps the public listener until the agent-side session closes
(`internal/tunnel/tunnel.go:227-269`), and that close is driven by
`TunnelExposeAdapter.RemoveProxy` (`internal/agent/tunnel_adapter.go:57-62`).

Therefore, if the one OFF publish is missed while NATS remains connected, the
old SS server, old keys and public tunnel can continue serving indefinitely.
The port may also be returned to the allocator while the old listener still
owns it.

Independent proof:

- `internal/broker/p13_external_review_test.go:39-81`
- `TestExternalReviewHeartbeatRepairsMissedProxyOff` fails because no disable
  repair is published.

Recommendation:

- Make OFF part of heartbeat convergence, not a one-shot nudge.
- Have the broker close the active tunnel listener immediately on OFF, or do
  not recycle the port until an agent unready/applied-epoch ACK arrives.
- Persist per-node desired/applied state or otherwise retry until the agent
  reports that it is no longer serving.

### F2 - High: lazy proxy runtime initialization is data-racy and can create orphan runtimes

`ensureProxyRuntime` reads and writes `a.proxy` without synchronization
(`internal/agent/proxy.go:34-38`). Forwarded directives run in independent
goroutines (`internal/agent/exec.go:49-67`), while initial register application
and heartbeat/reconnect paths access the same field
(`internal/agent/agent.go:341-345`, `internal/agent/agent.go:651-662`).

Concurrent first directives can create different `proxyRuntime` objects. One
pointer wins in `a.proxy`; a losing runtime can already own an SS server and
tunnel but will no longer be reachable by a later OFF operation.

Independent proof:

- `internal/agent/p13_external_review_test.go:43-72`
- The test intermittently creates multiple runtimes without `-race`.
- `go test -race` reports a read/write race at
  `internal/agent/proxy.go:35-36`.

Recommendation:

Initialize the runtime eagerly in `New`, or guard it with `sync.Once` or the
Agent's own mutex. Keep one lifetime-owned manager for the whole process.

### F3 - High: failed full rebuild leaves `proxy_ready=1` for a dead node

A full directive first tears the old server/tunnel down with
`clearPersist=false`, intentionally suppressing `unready`
(`internal/agent/proxy.go:93-99`, `internal/agent/proxy.go:165-184`). If the
new SS bind or `AddProxy` then fails, `proxyStartLocked` returns without
publishing `unready` or clearing the old persisted footprint
(`internal/agent/proxy.go:131-147`).

The broker can continue rendering that node in `/sub` even though it has no
running SS server or tunnel.

Independent proof:

- `internal/agent/p13_external_review_test.go:91-157`
- `TestExternalReviewFailedProxyRebuildPublishesUnready` fails because no
  unready event is emitted after the injected rebuild failure.

Recommendation:

Treat rebuild as a state transition with rollback. On any failed replacement,
clear the stale footprint, publish `unready`, and ensure the broker does not
render the node until a later successful full directive.

### F4 - Medium: an older nil register reply can override a newer enable

The OFF register representation is `Proxy=nil`, so it carries no epoch.
`applyProxyDirective` explicitly applies nil as teardown regardless of the
already-applied epoch (`internal/agent/proxy.go:72-88`).

A reconnect reply can be computed while OFF, then a newer live enable push can
arrive first. When the older nil reply arrives, it tears the new proxy down and
clears its footprint. Heartbeats then report epoch 0, which the broker ignores
(`internal/broker/broker.go:721-725`), so the outage can persist until another
register.

Independent proof:

- `internal/agent/p13_external_review_test.go:14-41`
- `TestExternalReviewStaleNilRegisterReplyDoesNotOverrideEnable` fails.

Recommendation:

Do not encode authoritative OFF as an unversioned absence. Return an explicit
disabled directive carrying the current epoch, or add an equivalent ordered
generation to the register response. The byte-identity goal must not defeat
state ordering.

### F5 - Medium: malformed `proxy.set` defaults to the destructive OFF action

`handleProxySet` discards the JSON error and branches on the zero-valued
`Enabled` field (`internal/broker/proxy.go:49-55`). A truncated or malformed
owner request therefore disables the session and returns success instead of
`json_parse`.

Independent proof:

- `internal/broker/p13_external_review_test.go:14-37`
- `TestExternalReviewMalformedProxySetDoesNotDisableSession` fails and observes
  a successful OFF response.

Recommendation:

Reject JSON decode errors before any mutation. Apply the same explicit parsing
rule to subscriber create/revoke for consistent protocol behavior.

### F6 - Medium: the loopback-only subscription boundary is not enforced

The requirements and plan define the HTTP endpoint as loopback-only, with the
plan saying `127.0.0.1:8090` is fixed
(`docs/reviews/p13-plan.md:182-188`). The implementation accepts any address
from YAML or `--sub-http-listen` (`cmd/tether/serve.go:56-58`,
`cmd/tether/serve.go:188-189`) and passes it directly to
`ListenAndServe` (`internal/subhttp/subhttp.go:185-203`).

An operator typo such as `0.0.0.0:8090` exposes the bearer-token/PSK vending
endpoint as plaintext HTTP, bypassing the intended Caddy TLS boundary.

Independent proof:

- `internal/subhttp/p13_external_review_test.go:9-17`
- `TestExternalReviewServeRejectsNonLoopbackAddress` fails.

Recommendation:

Validate with `net.SplitHostPort` plus `net.IP.IsLoopback`, rejecting wildcard,
unspecified and non-loopback hosts before opening storage or listeners.

### F7 - Medium: the planned pre-P13 capability gate is absent

The locked plan requires `release_version >= P13 release` before allocation or
push (`docs/reviews/p13-plan.md:168`, `docs/reviews/p13-plan.md:174`).
`proxyDirectiveForRegister` never checks `ReleaseVersion` and allocates for any
same-proto agent (`internal/broker/proxy.go:319-353`).

A pre-P13 agent ignores the unknown response field, never stores the token and
never ACKs ready. Re-registering legacy nodes can repeatedly replace rows and
consume port/history capacity while `AffectedNodes` overstates support.

Independent proof:

- `internal/broker/p13_external_review_test.go:83-108`
- `TestExternalReviewPreP13AgentGetsNoProxyAllocation` fails.

Recommendation:

Implement the documented minimum-release comparison and use it consistently in
enable and register paths. If the gate is intentionally dropped, revise the
plan and document the port/history exhaustion tradeoff explicitly.

### F8 - Medium: the architecture SSOT was not updated for P13

`CLAUDE.md:9-21` defines `docs/architecture.md` as the implementation ruler and
requires design changes there before code. The P13 plan also explicitly says
to update the architecture (`docs/reviews/p13-plan.md:182-188`).

The staged change does not touch `docs/architecture.md`, and that file contains
no P13 proxy, `ProxyDirective`, `proxy_enabled`, Shadowsocks or `/sub/`
contract. Its milestone map still ends at P11
(`docs/architecture.md:2147-2177`).

Recommendation:

Add the P13 subsystem, wire subjects, state machine, secret paths, HTTP/Caddy
boundary, capability gate, convergence/fail-closed rules, commands, tests and
milestone/exit criteria to the architecture baseline.

### F9 - Medium: the P13 acceptance suite is incomplete and its main e2e is flaky

The plan requires four P13 e2e scenarios plus CLI, register, tunnel, Caddy/WSS
and manual Clash validation (`docs/reviews/p13-plan.md:248-276`). The submitted
phase test contains one direct-protocol scenario; there are no real proxy CLI
tests and no Caddy/WSS regression test.

The OFF assertion only waits for `proxy_ready=0`
(`test/p13/proxy_e2e_test.go:198-204`), but the broker clears that DB flag
before the agent receives the OFF directive
(`internal/broker/proxy.go:120-134`). It does not assert adapter removal or SS
shutdown.

`go test ./test/p13 -run '^TestProxySubscriptionE2E$' -count=50` fails
repeatedly with `TempDir RemoveAll ... directory not empty`. Forwarded handlers
are detached goroutines (`internal/agent/exec.go:49-67`), so `Agent.Run`
can return while a P13 state write is still in flight.

`log.md` also has no P13 Caddy/WSS or real Clash import record, despite the
explicit exit criterion.

Recommendation:

- Wait for in-flight proxy handlers during shutdown.
- Make OFF e2e assert `RemoveProxy`, stopped SS and cleared persisted state.
- Add the planned join-after-ON, non-owner, real CLI wire, Caddy/WSS and
  tunnel data-plane cases.
- Record the required real deployment/Clash validation in `log.md`.

## Questions

- Is proxy-OFF register byte identity still considered more important than
  ordered convergence? The current nil representation cannot be made safely
  stale-aware without another generation signal.
- Should `proxy off` synchronously terminate the broker-side public listener,
  independent of agent/NATS health? For an open-exit feature, this is the most
  reliable kill-switch boundary.
- Is non-loopback `sub.listen` intentionally supported? If yes, the current
  requirements, plan, help text and security model are incorrect.
- Was the P13 capability gate intentionally deferred? The implementation and
  locked plan currently disagree.

## Additional Suggestions

- Add CLI tests around `newProxyCmd`: confirmation behavior, all request bodies,
  JSON output, broker error mapping and no-responder skew.
- Add a response-salt replay cache or explicitly document replay handling as a
  residual risk for the classic Shadowsocks AEAD server.
- Make the empty-keyset/unknown-key tests accept an early write-side broken
  pipe as a valid rejection; `TestServerEmptyKeysetRejectsAll` was flaky once
  under repeated package execution.
- Remove unrelated formatting churn and fix the corrupted quote in
  `cmd/tether/agent.go`'s `shellQuote` comment.

## Verification

Independent reviewer tests:

```text
go test ./internal/agent \
  -run '^TestExternalReviewStaleNilRegisterReplyDoesNotOverrideEnable$' -count=1 -v
# FAIL: stale nil OFF reply tears down epoch-2 enable

go test ./internal/agent \
  -run '^TestExternalReviewFailedProxyRebuildPublishesUnready$' -count=1 -v
# FAIL: no unready event after rebuild failure

go test -race ./internal/agent \
  -run '^TestExternalReviewConcurrentProxyRuntimeInitialization$' -count=1 -v
# FAIL: data race at internal/agent/proxy.go:35-36

go test ./internal/broker -run '^TestExternalReview' -count=1 -v
# FAIL: malformed set, missed OFF repair, and pre-P13 capability gate

go test ./internal/subhttp \
  -run '^TestExternalReviewServeRejectsNonLoopbackAddress$' -count=1 -v
# FAIL: 0.0.0.0 listener accepted
```

Submitted-suite stability:

```text
# Run before the independent reviewer tests were added:
go test ./test/p13 -run '^TestProxySubscriptionE2E$' -count=50
# FAIL repeatedly: TempDir cleanup races with a late agent state write

go test ./internal/broker ./internal/subhttp ./internal/agent/ssproxy -count=10
# broker/subhttp PASS
# ssproxy had one flaky early broken-pipe failure in the empty-keyset test
```

Repository gates:

```text
go build ./...
CGO_ENABLED=0 go build ./...
# PASS (CGO build printed a non-fatal module stat-cache permission warning)

go vet ./internal/agent ./internal/broker ./internal/agent/ssproxy \
  ./internal/proxysub ./internal/subhttp ./internal/proto ./internal/auth \
  ./internal/port ./internal/session ./test/p13
# PASS

golangci-lint run
# PASS, 0 issues

git diff --check
git diff --cached --check
# PASS

go test ./...
# FAIL on the new reviewer tests, the P13 issues above, and known
# macOS-sensitive /var-vs-/private/var and Unix-socket baseline tests.

go test -tags e2e_matrix -v ./test/e2e/... -count=1
# P1-P8 and P13 PASS in this run.
# P9/P10 FAIL for the known macOS baseline reasons.
```

P13 should be re-reviewed after the state convergence, runtime ownership,
HTTP binding and documented capability contracts are fixed, and after the
phase acceptance suite is stable and complete.

---

## 主进程回复（外审整改）

日期: 2026-06-10。逐条采纳全部 9 finding + 全部 Additional Suggestions。**所有外审随附的
`*_p13_external_review_test.go` 现已通过**;新增针对每个 finding 的回归测试;`golangci-lint v2.5.0`
0 issues、`-race` 干净、P13 全部包 + `test/p13` 绿。`go test ./...` 仅余既有 macOS 环境基线
失败(`/private/var` 符号链接、`--role agent` 不支持 macOS),与 P13 无关。

### F1（High,missed `proxy off` 不收敛)— 已修
两层修复:(a) **broker 即时 kill** —— 新增 `tunnel.Server.CloseProxy(port)`,`disableProxy` 在 Free
端口**之前**先关该公网监听,出口当场死、且避免端口被复用时旧监听仍占用(直接回答 Q2:是,OFF
现在同步终止 broker 侧公网监听,独立于 agent/NATS)。(b) **心跳收敛** —— `repairProxyEpoch` 扩展为
处理 OFF:agent 心跳报 `ProxyEpoch>0` 但 session 已 OFF ⇒ 补推带当前 epoch 的 disable。OFF 从
一次性 nudge 变为收敛 kill switch。测试 `TestExternalReviewHeartbeatRepairsMissedProxyOff` 绿。

### F2（High,lazy runtime 竞争)— 已修
`a.proxy` 改为 **`New()` 中急切初始化**(`proxy: &proxyRuntime{}`),`ensureProxyRuntime` 退化为读不
可变指针、零写入。`TestExternalReviewConcurrentProxyRuntimeInitialization`(及 `-race`)绿。

### F3（High,失败重建留 `proxy_ready=1`)— 已修
`proxyStartLocked` 任一失败(SS bind 或 AddProxy)走 `proxyFailCleanupLocked`:清持久 footprint +
发 `unready`,broker 不再渲染死节点。`TestExternalReviewFailedProxyRebuildPublishesUnready` 绿。

### F4（Medium,陈旧 nil register-reply 覆盖新 enable)— 已修
采纳你的判断「byte-identity 不得压过有序收敛」,但保留 nil 的字节等价:**agent 对 nil register-reply
取 no-op**(不再 teardown);权威 OFF 改由 epoch'd disable 推送 + 心跳修复(F1)送达,二者皆有序。
`TestExternalReviewStaleNilRegisterReplyDoesNotOverrideEnable` 绿。fail-closed 改为直接 teardown
(不经 nil 路径)。

### F5（Medium,malformed `proxy.set` 默认 OFF)— 已修
`handleProxySet` 在任何 gate/mutation **之前**校验 `json.Unmarshal` 错误 ⇒ `json_parse`;sub
create/revoke 同步加同样的显式解析。`TestExternalReviewMalformedProxySetDoesNotDisableSession` 绿。

### F6（Medium,loopback 未强制)— 已修
`subhttp.Serve` 新增 `requireLoopback`:`net.SplitHostPort` + `IsLoopback`,拒 wildcard/非环回
(允许 `localhost`)。`TestExternalReviewServeRejectsNonLoopbackAddress` 绿。回答 Q3:非 loopback
`sub.listen` **不**支持 —— 已按 plan/需求强制。

### F7（Medium,capability gate 缺失)— 已实现（撤回原「驳回」)
采纳。新增 `isP13Capable(release_version)`:dev 构建(`v0.0.0-dev`)恒 capable,真发布按 semver ≥
0.2.9;gate 用于 `proxyDirectiveForRegister` 与 `enableProxy`/keyset 推送(`onlineNIDs` 过滤)。
pre-P13 agent 不分配 `__proxy__`、不下推。`TestExternalReviewPreP13AgentGetsNoProxyAllocation` 绿。
回答 Q4:原为有意延后(理由在 p13-review),现按外审采纳实现,plan 的「已知限制」段作废。

### F8（Medium,architecture SSOT 未更新)— 已修
`docs/architecture.md` 新增 **§L「P13 — proxy 订阅」**(子系统/数据流、subject、ProxyDirective/
状态机、机密路径、HTTP/Caddy 边界、capability gate、收敛/fail-closed/kill switch、storage/CLI/
出口/残余威胁),并在里程碑映射补 post-1.0 增量(P12/P13)说明。

### F9（Medium,验收套件不全 + e2e flaky)— 已修
- **flaky 根因(detached handler 写 state.json)**:加 `proxyHandlerWG`,`Run` 在 drain 前等待
  in-flight proxy-keys handler;`test/p13` `-count=10` 稳定绿。
- **OFF 断言**:不再看 broker 立即清的 `proxy_ready`,改断言 **agent 真 `RemoveProxy(__proxy__)`** +
  一个**有效订阅**(bob)在 OFF 后渲染清空。
- **新增场景**:join-after-ON(`TestProxyJoinAfterOn`)、非 owner(已在 broker 测)、真 CLI
  (`cmd/tether/proxy_test.go`:非 TTY 无 `--yes` 中止且不发请求、`--name` 必填、错误码映射)、
  Caddy/WSS 序(`test/p10` 静态断言 `/sub/*` 在 WSS catch-all 之前 + `sub.listen` + 未被遮蔽)。
- **log.md**:补 P13 验证记录;真 Clash 客户端导入 + 真 Caddy/WSS + 真隧道数据面合并联调**明确
  标注为待 lab 硬件窗口**(本机 macOS 无公网域名/`--role broker` 不支持),控制面+数据面逻辑已由
  in-process 测试覆盖。

### Additional Suggestions — 已处理
- CLI 测试:已加(见 F9)。
- **response-salt replay**:作为残余威胁 **R-9** 记入 architecture §L.5 与 plan(经典 SS 无 replay
  缓存;v1 接受)。
- empty-keyset/unknown-key 测试:改 `assertRejected` 容忍早期 write-side broken pipe;`-count=20` 稳定。
- `cmd/tether/agent.go` `shellQuote` 注释里的弯引号已修正为直引号 `'\''`。

### 复审请关注
- 上述每个 finding 的回归测试 + 你的 `*_external_review_test.go` 是否都按预期变绿。
- F1 的 broker 即时 kill(`CloseProxy`)与心跳收敛是否满足「可靠 kill switch」标准。
- F7 capability gate 的 dev-build 例外(`v0.0.0-dev` 恒 capable)是否合理 —— 否则 dev/CI 的 agent 会
  被自己的 broker 排除。
