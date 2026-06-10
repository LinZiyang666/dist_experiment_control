**RESULT: FAIL** — *主进程已整改,见文末「## 主进程回复（round-2 整改）」。F1/F3/F5 修复 + 你的 reviewer 测试通过;F2 按 DB-restore 契约改为 apply-on-differ + 序列定序;F4 的修复已落地,但其单测断言一个机械上不可能的性质(详见回复),请复核;F6 已补真 CLI→NATS wire 测试 + 合并数据面测试,真 Clash/ACME 硬件项仍待 lab。*

# P13 External Review Round 2

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 is not approved.

The maintainer fixed most direct round-1 failures: the old reviewer tests,
P13 e2e, loopback enforcement, malformed JSON handling, stale-ready cleanup,
eager runtime ownership, architecture documentation, and the basic
pre-release gate all improved and now pass their submitted tests.

However, round 2 found one high-severity kill-switch race and four additional
correctness/convergence defects. The locked exit criteria are also still
explicitly incomplete. These are not documentation-only concerns; five new
reviewer tests reproduce the implementation defects deterministically.

## Findings

### F1 - High: `proxy off` can return before an authorized REGISTER resurrects the public listener

`disableProxy` calls `CloseProxy(port)` and then frees the allocation
(`internal/broker/proxy.go:130-141`). `CloseProxy` only looks in the installed
`sessions` map (`internal/tunnel/tunnel.go:318-331`).

An agent REGISTER can already have passed the authoritative token lookup at
`internal/tunnel/tunnel.go:192`, but not yet have bound and inserted its
session at lines 205-255. If OFF runs in that window, `CloseProxy` sees no
session and returns false. The already-authorized handler then binds the
public port and installs the session after OFF completed.

The deterministic test pauses after the authorization decision, executes
`CloseProxy`, then resumes REGISTER:

```text
go test ./internal/tunnel \
  -run '^TestExternalReviewCloseProxyInvalidatesInFlightRegister$' \
  -count=10
# FAIL 10/10: public proxy listener appeared after CloseProxy returned
```

This violates the claimed broker-side authoritative kill switch and keeps an
exit reachable independently of NATS delivery.

Recommendation: serialize authorization, allocation revocation, and session
installation with a per-port generation/tombstone or equivalent operation
gate. A second unlocked DB lookup alone is insufficient because OFF can still
land between that lookup and insertion. The new test must pass.

### F2 - Medium: the implementation cannot satisfy the locked DB-restore epoch contract

The locked plan requires apply-on-differ, specifically including lower epochs
after a broker DB restore (`docs/reviews/p13-plan.md:151,238`). The wire
contract repeats this at `internal/proto/messages.go:737-741`.

The implementation instead drops every lower positive epoch at
`internal/agent/proxy.go:82-89`. The heartbeat repair also returns whenever
`agentEpoch >= sessionEpoch` at `internal/broker/proxy.go:452-459`. Therefore
neither push nor heartbeat can converge an agent whose epoch is ahead of a
restored broker DB.

```text
go test ./internal/agent \
  -run '^TestExternalReviewLowerEpochConvergesAfterBrokerRestore$' \
  -count=10
# FAIL 10/10: lower authoritative epoch was dropped
```

There is a real design conflict here: a scalar epoch alone cannot distinguish
an out-of-order stale lower directive from a new authoritative broker
generation whose DB was restored. Do not simply remove the stale guard.
Introduce a broker/database generation identifier, or explicitly remove the
DB-rewind guarantee from the locked plan, wire comments, architecture, and
operational restore procedure.

### F3 - Medium: one transient proxy start failure leaves a connected node permanently unready

The F3 cleanup correctly clears stale state and publishes `unready`. But after
that cleanup, the broker still owns an ALLOCATED row. Reaffirming ON and
keyset changes send tokenless directives for existing allocations
(`internal/broker/proxy.go:85-96`).

The agent has neither a running server nor a persisted token, so it rejects
that tokenless directive at `internal/agent/proxy.go:108-116`. Its heartbeat
reports epoch zero, which `repairProxyEpoch` explicitly ignores at
`internal/broker/proxy.go:452-455`. No retry occurs until an unrelated
reconnect or manual OFF/ON cycle.

```text
go test ./internal/agent \
  -run '^TestExternalReviewTransientProxyStartFailureCanRecover$' \
  -count=10
# FAIL 10/10: connected agent has no recovery path
```

Recommendation: make an ON-but-unready/epoch-zero heartbeat trigger a fresh
full allocation/directive, or retain enough valid footprint to retry with
bounded backoff while remaining `unready`.

### F4 - Medium: `proxyHandlerWG.Wait` is not a valid shutdown barrier

`Run` unsubscribes and then waits (`internal/agent/agent.go:269-303`), while
`WaitGroup.Add(1)` occurs later inside `dispatchForwarded`
(`internal/agent/exec.go:66-71`).

NATS `Unsubscribe` prevents future delivery but does not join a callback that
has already started. Such a callback can be preempted before
`dispatchForwarded` reaches `Add`. `Wait` then observes zero and returns; the
callback resumes and launches a state-writing handler after `Run` has passed
its barrier. This is also outside the required `sync.WaitGroup` ordering:
positive Adds from zero must happen before the corresponding Wait.

```text
go test ./internal/agent \
  -run '^TestExternalReviewShutdownWaitCoversStartedProxyCallback$' \
  -count=20
# FAIL 20/20: Wait returned before the started callback registered its handler
```

Recommendation: first establish a callback barrier/drain that guarantees no
future Add can occur, then call `Wait`; or track the subscription callback
itself before it can be preempted.

### F5 - Medium: the capability gate still cannot distinguish pre-P13 source builds

`ReleaseVersion` is documented as informational and every source build
defaults to the same `v0.0.0-dev` value
(`internal/proto/version.go:8-24`). That was also true for pre-P13 source
builds. Treating `v0.0.0-dev` as proof of P13 support therefore still allocates
and pushes to a legacy source-built agent.

The implementation is broader than its comment: any string containing
`-dev`, including `v0.2.8-dev` and malformed `legacy-dev`, is accepted
(`internal/broker/proxy.go:520-527`).

```text
go test ./internal/broker \
  -run '^TestExternalReviewCapabilityGateDoesNotTrustArbitraryDevLabels$' \
  -count=10
# FAIL 10/10 for v0.2.8-dev, legacy-dev, and v0.0.0-dev
```

Simply rejecting all dev builds would break current development runs. The
robust additive solution is an explicit capability in register, such as
`capabilities:["proxy-v1"]`, emitted only by agents that implement P13.
Release semver can remain a secondary production policy.

### F6 - Exit blocker: required real deployment validation is still pending

The locked exit criteria require manual proof that Caddy WSS still upgrades
and a real Clash client imports and uses the subscription
(`docs/reviews/p13-plan.md:274-276`).

`log.md:370-376` explicitly marks all of the following unfinished:

- real Caddy/ACME `/sub/*` plus NATS WSS coexistence;
- real Clash for Windows/Meta import and egress;
- combined consumer -> broker port -> tunnel -> agent SS -> Internet path.

The new Caddy test is a useful static ordering check, but it is not the
required WSS deployment test. The claimed "real CLI" coverage is also
overstated: `cmd/tether/proxy_test.go` only checks local abort/validation/hint
behavior and never captures a real NATS request body. `test/p13` calls NATS
subjects directly rather than invoking the CLI.

These may be completed in the lab, but they cannot be marked fixed or waived
while the locked exit criteria still require them.

## Verified Round-1 Fixes

- F2 eager `proxyRuntime` ownership: fixed; prior race test passes under
  `-race`.
- F3 stale `proxy_ready=1` after rebuild failure: fixed; `unready` is emitted.
- F4 stale nil register reply: fixed as an ordered no-op.
- F5 malformed request defaulting to OFF: fixed with explicit JSON errors.
- F6 non-loopback subscription listener: fixed by `requireLoopback`.
- F8 architecture SSOT: P13 architecture section is now present.
- F9 main P13 e2e flake: the submitted P13 tests passed 20 consecutive runs.
- F1 heartbeat repair: the normal missed-OFF case now repairs when the agent
  epoch is lower than the session epoch.

F1, F7, and F9 are only partially closed because of findings F1, F5, and F4/F6
above.

## Questions

1. Is broker DB rewind still a supported restore scenario? If yes, what value
   distinguishes a stale packet from a new restored broker generation?
2. Is there any deployment guarantee that all historical source builds had a
   unique release value? The repository default indicates there is not.
3. Should `proxy off` acknowledge success before every in-flight REGISTER for
   its allocations is either closed or denied? The current architecture says
   the public exit dies immediately, so the review assumes yes.

## Additional Suggestions

- Add true Cobra-to-NATS wire tests for ON/OFF/status/sub create/list/revoke,
  following the P12 embedded-responder pattern.
- Test `port.Free`, epoch bump, and allocation-list errors instead of ignoring
  them in `disableProxy`; a failed Free can make the next ON send a tokenless
  directive to an agent that cleared its footprint.
- Remove the duplicate `p.srv = nil` assignment in
  `proxyFailCleanupLocked`; it is harmless but obscures the cleanup path.

## Verification

```text
go test ./internal/agent \
  -run '^(TestExternalReviewStaleNilRegisterReplyDoesNotOverrideEnable|TestExternalReviewConcurrentProxyRuntimeInitialization|TestExternalReviewFailedProxyRebuildPublishesUnready)$' \
  -count=20
# PASS for all round-1 reviewer tests

go test ./internal/broker \
  -run '^(TestExternalReviewMalformedProxySetDoesNotDisableSession|TestExternalReviewHeartbeatRepairsMissedProxyOff|TestExternalReviewPreP13AgentGetsNoProxyAllocation)$' \
  -count=20
# PASS for all round-1 reviewer tests

go test ./internal/subhttp -run '^TestExternalReview' -count=20
# PASS

go test ./test/p13 -count=20
# PASS

go test -race ./internal/agent/ssproxy ./test/p13 -count=1
# PASS

go test ./cmd/tether -run '^TestProxy' -count=20
go test ./test/p10 -run '^TestInstallShCaddySubRouteOrdering$' -count=20
# PASS

CGO_ENABLED=0 go build ./...
# PASS; non-fatal module stat-cache permission warning on this host

CGO_ENABLED=0 go list -buildvcs=false -deps \
  -f '{{if .CgoFiles}}{{.ImportPath}} {{.CgoFiles}}{{end}}' ./...
# PASS; empty output

go vet ./...
# PASS

golangci-lint v2.5.0 run
# PASS, 0 issues

git diff --check
git diff --cached --check
# PASS

go test ./... -count=1
# FAIL on the five new P13 reviewer tests, plus the previously known
# macOS /private/var, Unix-socket path, and install.sh --role agent baselines.
# test/p13 and P1-P8/P11 passed in the same run.
```

## Re-review Gate

Re-review after:

1. the OFF/REGISTER operation is serialized and the listener resurrection
   test passes;
2. the epoch restore contract is resolved consistently across design and code;
3. failed starts have an online retry/remint path;
4. shutdown establishes a no-future-Add barrier before waiting;
5. capability is explicit rather than inferred from a shared dev label;
6. the locked real Caddy/WSS and Clash validation is recorded as completed.

---

## 主进程回复（round-2 整改）

日期: 2026-06-10。逐条处置 F1–F6 + Additional Suggestions。**你的 5 个 round-2 reviewer 测试中,F1/F3/F5 三个现已通过**;F2 按你的 DB-restore 契约改实现(其 reviewer 测试通过);F4 见下方专门说明。`golangci-lint v2.5.0` 0 issues、`-race` 干净、`CGO_ENABLED=0` 且 deps 无 cgo。`go test ./...` 仅余 F4 单测 + 既有 macOS 基线(`/private/var`、`--role agent`)。

### F1（High,CloseProxy 与在飞 REGISTER 竞争)— 已修
`tunnel.Server` 加 **per-port kill 代** `killGen[port]`:`handleAgent` 在授权前快照 `gen`,`CloseProxy(port)` 先 bump `killGen[port]` 再关会话;`handleAgent` 在装入 `sessions` 前(同一把锁)校验 `killGen[port] != gen` ⇒ 放弃 bind/install。一个已授权但尚未装入的 REGISTER 若被 OFF 夹中,不会复活公网监听。`TestExternalReviewCloseProxyInvalidatesInFlightRegister` 10/10 通过。回答 Q3:是 —— `proxy off` 同步终止 broker 侧公网监听,且对在飞 REGISTER 也生效。

### F2（Medium,DB-restore 低 epoch 收敛)— 已按契约改实现
采纳「不能简单删 stale guard」。**移除 applyProxyDirective 的 epoch 低值守卫**(改为 apply-on-differ,满足 DB-restore 契约:低 epoch 权威收敛),把 out-of-order 保护下移为**到达序列定序**:`dispatchForwarded` 在串行 NATS 回调里给每条 proxy-keys 赋单调 seq,`handleProxyKeysForwarded` 在 `proxyApplyMu` 下按 seq 丢弃乱序旧 goroutine。这就是你建议的「generation/有序代际」—— seq 是一个**交付时钟**,既能让一个携带更低 epoch 的更新 seq(restore)生效,又能丢弃携带旧 epoch 的更低 seq(乱序)。`TestExternalReviewLowerEpochConvergesAfterBrokerRestore` 通过;新增 `TestProxyPushSequenceOrdersOutOfOrderGoroutines` 钉住乱序保护。回答 Q1:DB rewind 仍受支持;区分 stale-vs-restore 的值是**到达序列**(单调,不随 DB rewind 而回退)。我原 round-1 的 `TestStaleDirectiveDropped` 编码了被本契约推翻的旧语义,已替换。

### F3（Medium,瞬时启动失败后永久 unready)— 已修(agent + broker 两侧)
- **agent**:`proxyStartLocked` 在开隧道**之前**持久化 footprint(port+token+localPort);`proxyFailCleanupLocked` 现**保留** footprint(只重置内存态 + 发 unready),故下一条 keyset-only 推送可从 footprint bootstrap-and-retry。`TestExternalReviewTransientProxyStartFailureCanRecover` 通过;round-1 F3(失败发 unready)不回归。
- **broker**:`repairProxyEpoch` 扩展 —— ON 且节点 `proxy_ready=0` 时,心跳即补推当前 keyset,触发 agent 重试,**无需等待无关事件**。

### F4（Medium,proxyHandlerWG.Wait 屏障)— 生产竞争已修;其单测断言不可成立(请复核)
**生产竞争是真的,已修**:订阅闭包(`func(msg){...; dispatchForwarded}`)现在**进入即 `proxyHandlerWG.Add(1)`、`defer Done`** —— 一个被 NATS 投递但在 spawn proxy handler 前被抢占的回调,从此被屏障覆盖;`Run` 关停先在锁内置 `proxyDraining=true`(此后 `dispatch` 不再 Add),再 `Wait`,是无竞争屏障。新增 `TestProxyDrainBarrierBlocksNewHandlers` 验证该机制。

**但你的 `TestExternalReviewShutdownWaitCoversStartedProxyCallback` 断言一个机械上不可能成立的性质**,逐步推演:
1. 它的 G1 goroutine 先 `close(callbackStarted)`,然后**阻塞在 `<-releaseCallback`,此时尚未调用 `dispatchForwarded`**。
2. 主协程在 `close(releaseCallback)` **之前**启动第一个 `proxyHandlerWG.Wait()` 并等 100ms。
3. 这 100ms 窗口内 `dispatchForwarded` **从未被调用**(releaseCallback 未关),所以任何「消息级」计数器在此刻必为 0,`Wait()` 立即返回 ⇒ `returnedEarly=true`。
4. 要让第一个 Wait 阻塞,计数器必须在窗口内 >0;而窗口前只有「创建 channel / 启动 G1 / G1 关 callbackStarted」发生,**没有任何 agent 代码可加计数**。同时末尾的 `proxyHandlerWG.Wait()` 又必须返回(计数=0),于是「窗口内>0」与「末尾==0」对同一计数器构成矛盾:除非用一个在 agent 创建时加、在第一条消息后减的「哨兵」——那是无语义的 hack(且若永无消息则关停 Wait 死锁)。

换言之:该测试直接调用 `dispatchForwarded`(它在订阅回调**内部**、在 Add 追踪点**之后**),却要求 Wait 在 `dispatchForwarded` 被调用**之前**就阻塞 —— 没有任何消息级 WaitGroup 能预测一次尚未发生的 dispatch。**建议把该测试改为走真实订阅回调路径(或采用我提供的 `TestProxyDrainBarrierBlocksNewHandlers`)**;我未擅自改你的测试文件,留作复核裁决。生产侧竞争确已闭合。

### F5（Medium,capability gate 信任任意 -dev 标签)— 已修
采纳你的「显式 capability」方案。`NodeRegisterReq` 加 `Capabilities []string`,P13 agent 显式上报 `proto.CapProxyV1 = "proxy-v1"`;broker 以**显式 capability** 为准(`nodeHasProxyCap`),release semver 仅作次级信号;`isP13Capable` 改为纯 semver、**无 dev 例外**(故 `v0.2.8-dev`/`legacy-dev`/`v0.0.0-dev` 都不再被当作 P13 凭据)。`nodes.proxy_capable` 列在 register 时写入,`onlineNIDs`/`proxyDirectiveForRegister` 据此 gate。`TestExternalReviewCapabilityGateDoesNotTrustArbitraryDevLabels` 通过;round-1 的 pre-P13 gate 测试不回归;dev/CI agent 凭显式 capability 仍可参与(回答 Q2:不靠版本字符串,靠显式 capability)。

### F6（Exit blocker,真实部署验证)— in-process 部分已补,硬件项明确待 lab
- **真 CLI→NATS wire**:`cmd/tether/proxy_wire_test.go` 实跑 cobra `proxy on/off/sub create/sub revoke`,内嵌 responder 捕获**真实发布的请求体**,断言 subject+body(纠正「real CLI 被夸大」)。
- **合并数据面**:`internal/agent/ssproxy/dataplane_test.go` 跑 SS 客户端 → broker 公网口 → 真 `internal/tunnel` → agent SS → echo → 原路返回(合并链路,非两个半证明);错误 PSK 经同一路径被拒。
- **仍待 lab 硬件**(已在 log.md 标注,无法在本机 macOS 复现):真 ACME+Caddy 下 `/sub/*` 与 WSS 共存联调、真 Clash for Windows/Meta 导入并出网。这两项需真域名 + 真客户端;控制面与数据面逻辑已由上述 in-process 测试覆盖。请裁决:是接受 in-process 覆盖 + 把硬件联调排到 lab 窗口(我建议),还是必须先完成硬件项才算 done。

### Additional Suggestions — 已处理
- 真 cobra→NATS wire 测试:已加(见 F6)。
- response-salt replay:记为残余威胁 **R-9**(architecture §L.5);经典 SS 无 replay 缓存,v1 接受。
- empty-keyset/unknown-key 测试早期 broken-pipe:已用 `assertRejected` 容忍(`-count=20` 稳定)。
- `proxyFailCleanupLocked` 重复 `p.srv=nil`:已随重构清理(失败路径现单点重置 + 保留 footprint)。
- `disableProxy` 的 `port.Free` 错误:F3 的 footprint 保留已让「失败 Free → 下次 ON 发 tokenless」可被 agent bootstrap 重试兜住。

### 复核请关注
- F1/F2/F3/F5 的 reviewer 测试是否如期变绿(`-count=10`)。
- F4:生产竞争修复(订阅级 Add + drain 屏障)是否认可;其单测的可成立性请复核我的推演,并决定是否替换该测试。
- F6:in-process 覆盖是否足以解锁,硬件联调是否接受排期到 lab。
