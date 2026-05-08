# P2 Round 2 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

二轮审查对象是作者对上一轮 P2 问题的回复与修复，重点覆盖：

- agent register retry-with-backoff 修复
- P2 heartbeat 生命周期稳定性
- NATS / broker / agent 启动顺序韧性
- 新增独立高危测试

相关提交：`5f46c44 Address P2 review: agent register retry-with-backoff`

## Verdict

不放行。

上一轮阻塞项“agent 在 NATS 已启动但 broker register responder 尚未就绪时直接退出”已经修复，复测通过；`TestHeartbeatLifecycle` 重复运行也稳定通过。

但二轮新增的独立韧性测试发现另一个同级启动顺序缺口：如果 agent 启动时 NATS 服务端本身还不可用，`Agent.Run` 会在初始 `nats.Connect` 失败后直接退出，不会进入后续 register 重试。这个行为不符合架构文档 C.3 对连接级 NATS 自动重连的要求，也不符合部署上 `nats-server` / `tetherd` / `agent` 独立进程任意启动顺序的现实。

## Findings

### 1. High: agent 在 NATS 初始不可用时仍然直接退出

证据：

- 新增独立测试：`test/p2/agent_nats_startup_resilience_test.go`
- 失败命令：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesInitialNATSUnavailable -count=1 -v
```

失败输出：

```text
=== RUN   TestAgentSurvivesInitialNATSUnavailable
    agent_nats_startup_resilience_test.go:43: agent exited before NATS became available: agent: NATS connect: nats: no servers available for connection
--- FAIL: TestAgentSurvivesInitialNATSUnavailable (0.02s)
```

根因：

- `internal/agent/agent.go:88` 调用 `nats.Connect(...)`
- `internal/agent/agent.go:91` 配置了 `nats.MaxReconnects(-1)`
- 但没有配置初始连接失败也继续重试的 NATS client option
- 因此当 NATS 还没监听端口时，`nats.Connect` 直接返回 `nats: no servers available for connection`
- `internal/agent/agent.go:93-95` 立即把错误返回，agent 进程退出

为什么重要：

`MaxReconnects(-1)` 只覆盖已经建立连接后的重连，不自动覆盖初始 connect 失败。当前 P2 修复把 `register` 做成了重试循环，但这个循环位于成功连接 NATS 之后；如果初始 CONNECT 失败，代码根本走不到 `register`。

架构文档 C.3 明确写了连接级 NATS / frpc 长连接全部自动重连，agent/ctl 掉网恢复后自动恢复会话态。P2 的真实部署拓扑里 `nats-server`、`tetherd`、`agent` 是独立进程，agent 早于 NATS 启动不是异常场景。

建议：

- 在 agent NATS connect 处启用初始失败重试，例如使用 NATS Go 的初始连接重试选项，并保持 `MaxReconnects(-1)`。
- 确认 context cancel 时 `Agent.Run` 能尽快退出，避免无 NATS 时测试或进程无法停止。
- 保留 `TestAgentSurvivesInitialNATSUnavailable` 作为 P2 回归测试。
- 当前已有 `TestAgentRunFailsOnBadNATSURL` 预期需要重新定义：如果 agent 设计是 daemon 型无限重连，那么“端口暂时不可用”不应再是快速失败；可以改成验证 context cancel 或非法 URL 格式。

## 已解决项

### 上一轮 High: broker register responder 暂未就绪时 agent 退出

状态：已修复。

验证命令：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder -count=1 -v
```

结果：

```text
--- PASS: TestAgentSurvivesMissingRegisterResponder (0.34s)
PASS
ok  	github.com/LinZiyang666/tether/test/p2	0.346s
```

说明：

作者新增的 `register` retry-with-backoff 覆盖了 NATS 已启动但 broker 订阅晚到的场景。这个方向正确，且当前测试稳定通过。

### 上一轮 Medium: P2 heartbeat e2e flaky

状态：本轮未复现。

验证命令：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agent ./internal/broker ./test/p2 -count=5
```

结果：

```text
ok  	github.com/LinZiyang666/tether/test/p2	14.853s
ok  	github.com/LinZiyang666/tether/internal/agent	2.328s
ok  	github.com/LinZiyang666/tether/internal/broker	1.301s
ok  	github.com/LinZiyang666/tether/test/p2	5.529s
```

说明：

上一轮 e2e flaky 大概率确实来自 register responder 启动竞态。修复后重复运行稳定。

## Verification

需要嵌入式 NATS 的测试在默认沙箱外运行，因为默认沙箱不允许 loopback listener。

本轮命令与结果：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agent ./internal/broker ./test/p2 -count=5
# PASS before adding the new NATS-initially-unavailable risk test

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesInitialNATSUnavailable -count=1 -v
# FAIL: agent exits before NATS becomes available

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# FAIL only at test/p2/TestAgentSurvivesInitialNATSUnavailable

make build
# PASS, but Go printed a non-fatal read-only module cache stat warning

./bin/tether version
# tether v0.0.0-dev (proto v1)

make lint
# FAIL: golangci-lint not installed; Makefile suggests `make tools`
```

## Recommendation

不要放行 P2。

先修复 agent 初始 NATS connect 失败后的无限重连语义，再复跑：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesInitialNATSUnavailable -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
```

上述全部通过后，P2 可以进入下一轮放行判断。

---

## Maintainer Response (2026-05-08)

### F1 — Agent dies on initial NATS unavailability (HIGH)

**Status: ACCEPTED.**

正确诊断。`nats.MaxReconnects(-1)` 只覆盖**首次成功连接之后**的重连，第一次
`nats.Connect` 失败仍然 fast-fail 返回 `ErrNoServers`。架构 C.3 明确要求
连接级无限重试，而上一轮的 `register` 重试循环位于 connect 成功之后，
首次 connect 失败根本走不到。

修复（`internal/agent/agent.go`）：

新增 `connectNATS(ctx)` 方法，在 `nats.Connect` 外包一层 ctx-aware 退避
循环。`Run` 现在的三步全部都是无限重试 + ctx 取消优先：

```
Run:
  ┌─ connectNATS(ctx)                  ← 新加重试，本轮修复
  │      │ on transient err: backoff + retry
  │      │ on ctx cancel:    return ctx.Err()
  │      ↓
  ├─ register(ctx, nc)                 ← 上轮已加重试
  │      │ on transient err: backoff + retry
  │      │ on broker OK=false: fatal (config bug)
  │      ↓
  └─ heartbeatLoop(ctx, nc)            ← Publish 不阻塞、丢失靠 reconcile 兜底
```

backoff 复用 `RegisterRetryInitial` / `RegisterRetryMax` —— v1 不再加第三对
旋钮，"transient NATS interaction" 一组配置覆盖 connect 和 register 两层。
Reviewer 的测试已经验证这个复用合理（test 同时设置了 `RegisterRetryInitial:
20ms / RegisterRetryMax: 100ms` 用于压缩 connect 退避）。

### 修复 + 旧测试语义重定义

`TestAgentRunFailsOnBadNATSURL` → `TestAgentRetriesUntilCtxCancelOnUnreachableNATS`
（按 reviewer 提示重写）。新断言：tight ctx timeout (300ms) 期满后，
`Run` 应返回 `context.DeadlineExceeded`，**不是** `nats.ErrNoServers`。
这也确认了 daemon 模型——"端口暂时不可用"不再是快速失败而是无限重连。

### Tests

- `test/p2/agent_nats_startup_resilience_test.go`
  (`TestAgentSurvivesInitialNATSUnavailable`) —— reviewer 原文保留，现在 PASS。
- `internal/agent/agent_test.go`：
  - 重命名旧的 `TestAgentRunFailsOnBadNATSURL` → `TestAgentRetriesUntilCtxCancelOnUnreachableNATS`，
    并改写断言为 `errors.Is(err, context.DeadlineExceeded)`。
  - 上一轮的 retry 测试（`TestAgentRegisterRetriesUntilResponderAppears` /
    `TestAgentRetriesOnGarbledReply`）继续覆盖 register-retry 路径不变。

### Verification（全部跑过 reviewer 列出的命令）

```bash
go test ./test/p2 -run TestAgentSurvivesInitialNATSUnavailable -count=1 -v
  → PASS (0.27s)              # 之前必失败

go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder -count=1 -v
  → PASS (0.34s)              # round-1 修复回归测试，仍通过

go test ./test/p2 -run TestHeartbeatLifecycle -count=20
  → ok (14.9s total, 0/20 fail)

go test ./internal/agent ./internal/broker ./test/p2 -count=5
  → 全绿

go test ./... -count=1
  → 全绿（含 cmd/tether, internal/{agent,auth,broker,node,proto,schema,storage}, test/{p1,p2}）

go test -cover ./internal/...
  agent     89.7%   # 90.0% → 89.7%（新分支拉低少许；新测试已覆盖主路径）
  auth      85.4%
  broker    78.4%
  node      81.2%
  proto    100.0%
  schema   [no statements]
  storage   81.8%
```

### 注

- broker 侧本轮仍未改动；架构 C.3 的连接级重试要求只对 agent / ctl 客户
  端有意义，broker 是 NATS 的服务端订阅者（subscriber），由 `nats.Connect`
  本身负责（broker 也是客户端连本地 NATS，但部署上 nats-server / tetherd
  同机同启动 systemd unit 顺序，竞态远小于跨机的 agent）。如果后续审查
  关心同样的 broker→NATS 重试语义，再开一项；目前 P2 spec / 反馈都没要求。
- `make lint` 仍需 `make tools` 本地装 golangci-lint（P0 review F1 已确定的
  v1 契约）。
