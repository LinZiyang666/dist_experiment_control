# P2 Round 3 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

三轮审查对象是作者对二轮问题的修复：

- agent 初始连接 NATS 失败时的无限重试
- 上一轮 register responder 晚启动回归
- P2 heartbeat 生命周期稳定性
- P2 全量重复测试与全仓回归

相关提交：`a61e0cd Address P2 round-2 review: agent NATS connect retry-with-backoff`

## Verdict

放行 P2。

二轮阻塞项已经修复：agent 在启动时如果 NATS 尚未可用，不再因 `nats: no servers available for connection` 直接退出，而是按 ctx-aware backoff 持续重试；NATS 启动后 agent 能继续 register 并进入 heartbeat。

本轮没有发现新的 P2 阻塞问题。

## Findings

### Blocking findings

无。

### 已验证修复：agent 初始 NATS 不可用时持续重试

代码路径：

- `internal/agent/agent.go:91`：`Run` 改为调用 `connectNATS(ctx)`
- `internal/agent/agent.go:120`：`connectNATS` 外包 `nats.Connect`，失败后按 backoff 重试，ctx 取消优先退出
- `internal/agent/agent_test.go:291`：旧的 closed-port fast-fail 语义已改成 `context.DeadlineExceeded`
- `test/p2/agent_nats_startup_resilience_test.go:19`：覆盖 agent 先启动、NATS 后启动、随后完成 register 的真实启动顺序

验证结果：

```text
=== RUN   TestAgentSurvivesInitialNATSUnavailable
--- PASS: TestAgentSurvivesInitialNATSUnavailable (0.27s)
PASS
ok  	github.com/LinZiyang666/tether/test/p2	0.279s
```

```text
=== RUN   TestAgentRetriesUntilCtxCancelOnUnreachableNATS
--- PASS: TestAgentRetriesUntilCtxCancelOnUnreachableNATS (0.30s)
PASS
ok  	github.com/LinZiyang666/tether/internal/agent	0.305s
```

原因判断：

这次修复覆盖了上一轮指出的真正缺口：`nats.MaxReconnects(-1)` 只处理首次成功连接后的断线重连，不处理首次 connect 失败。现在 `connectNATS(ctx)` 把初始 connect 也纳入同一类连接级重试语义，符合架构 C.3 对 agent 连接韧性的要求。

### 已验证回归：broker responder 晚启动与 heartbeat e2e

验证结果：

```text
ok  	github.com/LinZiyang666/tether/test/p2	1.706s    # TestAgentSurvivesMissingRegisterResponder -count=5
ok  	github.com/LinZiyang666/tether/test/p2	14.978s   # TestHeartbeatLifecycle -count=20
ok  	github.com/LinZiyang666/tether/test/p2	26.728s   # whole test/p2 package -count=20
```

说明：

上一轮和二轮的两个启动顺序缺口现在都被测试覆盖，并且重复运行未复现抖动。P2 exit criterion 里的 heartbeat 闭环在当前加速测试下稳定。

## Non-blocking concerns

### 1. `connectNATS` 当前会重试所有 connect 错误

当前实现把 `nats.Connect` 返回的所有错误都当作 transient。P2 无鉴权，主要错误就是 NATS 未启动或端口不可达，因此可以接受。

建议后续 P3/P7 处理：

- 引入 auth / WSS 后，区分明显 permanent 的配置错误，例如认证失败、URL 格式错误、TLS 配置错误。
- permanent error 应尽早给出清晰错误，避免用户因为错误配置看到 daemon 无限重试。

不阻塞 P2 的原因：

P2 目标是 dev NATS + agent/broker 最小 heartbeat 闭环；本轮覆盖的核心风险是进程启动顺序，而不是最终用户配置诊断。

### 2. connect retry 复用 `RegisterRetryInitial` / `RegisterRetryMax`

复用同一组 backoff 参数能保持实现简单，本阶段可接受。

建议后续如配置面扩大，可以改名为更通用的 NATS interaction retry 参数，或者拆成 connect/register 两组配置。当前命名略窄，但不影响行为正确性。

### 3. broker 初始 NATS 不可用仍未纳入本轮放行条件

作者说明 broker 侧未改动，依赖本机部署顺序降低竞态风险。本轮没有把它列为 P2 blocker。

原因：

- P2 phase 明确测试目标是 broker + agent heartbeat 闭环；当前所有 P2 测试均已通过。
- 架构 C.3 / 后续 M7 对整体韧性要求更宽，broker 初始 NATS 不可用可以在后续 resilience 对账阶段单独补测试与策略。
- agent 侧跨机器、跨启动顺序风险更直接，已经作为 P2 blocker 修复。

建议：

后续进入韧性阶段时，为 broker 也补一个 `tether serve` 早于 NATS 启动的测试，届时明确是依赖 service manager 重启，还是 broker 自身也实现初始 connect retry。

## Verification

需要嵌入式 NATS 的测试在默认沙箱外运行，因为默认沙箱不允许 loopback listener。

本轮命令与结果：

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesInitialNATSUnavailable -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agent -run TestAgentRetriesUntilCtxCancelOnUnreachableNATS -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder -count=5
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agent ./internal/broker ./test/p2 -count=5
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -count=20
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test -cover ./internal/...
# PASS
```

覆盖率结果：

```text
internal/agent    89.7%
internal/auth     85.4%
internal/broker   78.4%
internal/node     81.2%
internal/proto   100.0%
internal/schema  [no statements]
internal/storage  81.8%
```

构建与工具：

```bash
make build
# PASS
# Go printed a non-fatal read-only module cache stat warning.

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0

make lint
# FAIL: golangci-lint not found. Run: make tools
```

## Recommendation

P2 可以放行，进入 P3。

保留本轮新增的启动顺序测试作为回归测试。后续 P3/P7 需要继续收敛 permanent connect error 分类、broker 初始 NATS 不可用策略，以及完整重连对账语义。

---

## Maintainer Response (2026-05-08)

**ACK — P2 放行确认，开始 P3。**

无新 finding 需处理。三项 non-blocking 建议并入后续 phase backlog：

| 建议 | 何时处理 |
|---|---|
| Permanent connect error 分类（auth 失败 / URL 格式 / TLS 配置） | **P3** —— 引入 auth_callout 后认证错误天然成为 permanent，届时一起做 |
| `RegisterRetryInitial/Max` 命名略窄 | **P3 顺手** —— 如果配置面扩大就改成 `NATSRetry*`，否则保留 |
| broker 早于 NATS 启动的重试 | **P7 韧性阶段** —— 配合 reconcile 一起做 |

`test/p2/agent_startup_resilience_test.go` 与 `test/p2/agent_nats_startup_resilience_test.go`
两个回归测试随仓保留。
