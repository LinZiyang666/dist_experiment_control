**RESULT: FAIL** — *主进程已整改,见文末「## 主进程回复（round-3 整改）」。F1–F5 全部采纳并修复,你的 4 个 round-3 reviewer 测试现已全部通过(`-count=10`/`-race`);引入统一 `(generation, epoch)` 定序模型 + 显式 capability;文档已同步。F6 真硬件(Caddy/ACME + 真 Clash)需项目 owner 裁决(见 Q3 回复)。*

# P13 External Review Round 3

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 is not approved.

The round-2 remediation substantially improved the submission:

- the original in-flight REGISTER test now passes;
- lower epochs can be applied by the agent;
- transient tunnel-start failures retain enough footprint to retry;
- arbitrary dev version labels no longer prove capability;
- the production shutdown barrier is materially improved;
- real Cobra-to-NATS and combined SS-over-tunnel tests were added.

However, the fixes do not close the full production paths. Two kill-switch
races remain, both advertised heartbeat recovery paths are incomplete, the
architecture SSOT contradicts the implementation, and the locked real
deployment exit criteria are still explicitly pending.

## Findings

### F1 - High: OFF still authorizes a fresh proxy REGISTER after the kill generation was bumped

The new `killGen` correctly rejects a REGISTER that snapshotted its generation
before `CloseProxy` (`internal/tunnel/tunnel.go:196-205,248-258`). The submitted
round-2 tunnel test verifies that case.

It does not cover a REGISTER that starts after `CloseProxy` increments the
generation but before `port.Free` finishes:

1. `disableProxy` sets the switch OFF.
2. `CloseProxy` increments `killGen` (`internal/tunnel/tunnel.go:330-339`).
3. Before `port.Free` at `internal/broker/proxy.go:140`, a new REGISTER
   snapshots the new generation.
4. `tunnelTokenLookup` authorizes solely from the still-ALLOCATED row and does
   not check the proxy master switch (`internal/broker/expose.go:53-69`).
5. Its generation still matches, so the public listener is installed after
   OFF.

The production authorization boundary itself demonstrates the gap:

```text
go test ./internal/broker \
  -run '^TestExternalReviewProxyTokenDeniedWhileSwitchOff$' \
  -count=10
# FAIL 10/10: proxy token was authorized while the switch was OFF
```

This remains a kill-switch violation. `port.Free` errors are also discarded,
which can extend the authorization window indefinitely while OFF still
replies success.

Recommendation: for `__proxy__` rows, make `tunnelTokenLookup` require the
session switch to be ON, and make OFF's switch/row transition transactional or
otherwise fail visibly when allocation revocation fails. Keep `killGen` for
the already-authorized pre-close case.

### F2 - High: a stale register reply can resurrect the proxy after a newer OFF

The new sequence only orders live `proxy-keys` handlers
(`internal/agent/proxy.go:249-271`). Reconnect register replies bypass it and
call `applyProxyDirective` directly at `internal/agent/proxy.go:288-294`.

Because the lower-epoch guard was removed, this ordering is possible:

1. register reply is computed while ON at epoch 1;
2. OFF push at epoch 2 is received and stops the proxy;
3. the register goroutine resumes and applies its epoch-1 enabled reply;
4. the embedded proxy and tunnel are restarted after OFF.

```text
go test ./internal/agent \
  -run '^TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush$' \
  -count=10
# FAIL 10/10: epoch-1 register reply restarted serving after epoch-2 OFF
```

An agent-local arrival sequence assigned only to one delivery path cannot
distinguish this stale reply from an authoritative DB restore.

Recommendation: order every directive source with the same broker-issued
identity, for example `(broker_generation, epoch)`. A newer generation accepts
an epoch rewind after restore; within one generation, lower epochs are stale.
Register replies and live pushes must carry and compare the same pair.

### F3 - Medium: the advertised zero-epoch heartbeat retry path is unreachable

`repairProxyEpoch` was extended to retry an ON-but-unready node, including
`agentEpoch == 0` (`internal/broker/proxy.go:476-490`). But `handleHeartbeat`
still calls it only when `ProxyEpoch > 0`
(`internal/broker/broker.go:722-726`).

A failed start reports epoch zero, so the exact failure the change intends to
repair never reaches the repair function:

```text
go test ./internal/broker \
  -run '^TestExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy$' \
  -count=10
# FAIL 10/10: no retry directive
```

The agent-side retained-footprint retry works when a keyset directive is
manually supplied, but there is still no automatic online trigger.

Recommendation: call the repair path for every valid heartbeat and let it
decide from switch, readiness, capability, allocation, and epoch state.

### F4 - Medium: DB-rewind convergence still fails for connected agents

The locked plan supports broker DB epoch rewind. A broker restart need not
disconnect an agent from an independently running NATS server, so heartbeat is
the convergence path for an agent still reporting a higher pre-restore epoch.

`repairProxyEpoch` returns when a ready agent has
`agentEpoch >= sessionEpoch` (`internal/broker/proxy.go:480-482`). Therefore a
ready epoch-5 agent never receives the restored authoritative epoch 1:

```text
go test ./internal/broker \
  -run '^TestExternalReviewHeartbeatConvergesAfterDBEpochRewind$' \
  -count=10
# FAIL 10/10: no restored directive
```

This is the broker half of F2's unresolved ordering model. The same
broker-generation contract should drive heartbeat repair.

### F5 - Medium: architecture and locked plan no longer describe the implementation

The implementation now adds:

- `NodeRegisterReq.Capabilities`;
- `proto.CapProxyV1`;
- persisted `nodes.proxy_capable`;
- live-push arrival sequencing;
- `killGen`;
- retained failed-start footprint.

The architecture still says lower epochs are dropped and dev
`v0.0.0-dev` is inherently capable
(`docs/architecture.md:1889-1894`). The plan still describes a release-only
capability gate and does not define the new ordering/generation contract
(`docs/reviews/p13-plan.md:151-176`).

This is not harmless wording: the contradictory epoch statements correspond
directly to F2/F4, and the documented capability rule contradicts the new
wire contract.

Recommendation: settle the ordering design first, then update architecture,
plan, protocol comments, migration notes, and operational restore behavior as
one consistent contract.

### F6 - Exit blocker: real Caddy/WSS and Clash validation remains incomplete

The new coverage is useful and passed:

- `cmd/tether/proxy_wire_test.go` exercises real Cobra commands and NATS
  request bodies for on/off/create/revoke;
- `internal/agent/ssproxy/dataplane_test.go` exercises the combined broker
  public port -> tunnel -> agent SS -> echo path.

But the locked exit criteria still require real Caddy WSS coexistence and a
real Clash import (`docs/reviews/p13-plan.md:264-276`). `log.md:375-380`
explicitly leaves both unchecked.

I do not accept an implementation-time waiver of locked phase exit criteria.
The project owner may revise those criteria explicitly, or the lab validation
must be completed before phase closeout. Also, the new CLI wire test still
does not cover `proxy status` or `proxy sub ls`.

## Round-2 Disposition

### Accepted fixes

- F1 pre-close in-flight REGISTER generation check: fixed for that exact race.
- F2 agent apply-on-differ primitive: lower directives can now be applied.
- F3 retained footprint and keyset-triggered retry: fixed.
- F4 shutdown barrier: accepted. The prior reviewer test invoked
  `dispatchForwarded` before the production subscription wrapper and demanded
  tracking before any agent callback code ran; that test was invalid and has
  been replaced. `TestProxyDrainBarrierBlocksNewHandlers` passed repeatedly
  and under `-race`.
- F5 arbitrary `-dev` labels: fixed; explicit capability supports dev builds.
- F6 real CLI wire and combined in-process data plane: added and passing.

### Still partial

- F1 does not cover a REGISTER beginning after the generation bump but before
  allocation revocation.
- F2 sequences only live pushes, not register replies or broker generations.
- F3's heartbeat trigger is filtered out at its caller.
- F6's hardware exit criteria remain pending.

## Questions

1. Is DB restore guaranteed to restart NATS as well as tetherd? The current
   architecture does not state that, and the components are deployed
   separately.
2. Should OFF return `OK` after `BumpProxyEpoch`, allocation listing, or
   `port.Free` fails? All three errors are currently ignored.
3. If hardware validation is to become post-phase work, who is authorizing the
   change to the locked exit criteria, and where will that revised contract be
   recorded?

## Verification

```text
go test ./internal/tunnel \
  -run '^TestExternalReviewCloseProxyInvalidatesInFlightRegister$' \
  -count=20
# PASS

go test ./internal/agent \
  -run '^TestExternalReview(LowerEpochConvergesAfterBrokerRestore|TransientProxyStartFailureCanRecover)$' \
  -count=20
# PASS

go test ./internal/broker \
  -run '^TestExternalReviewCapabilityGateDoesNotTrustArbitraryDevLabels$' \
  -count=20
# PASS

go test ./internal/agent \
  -run '^TestProxy(PushSequenceOrdersOutOfOrderGoroutines|DrainBarrierBlocksNewHandlers)$' \
  -count=50
# PASS

go test -race ./internal/agent \
  -run '^TestProxyDrainBarrierBlocksNewHandlers$' \
  -count=20
# PASS

go test ./cmd/tether \
  -run '^TestProxy(CLIWiresToNATS|OnAbortsWithoutYesNonInteractive|SubCreateRequiresName|ErrorHintsMapped)$' \
  -count=20
# PASS

go test ./internal/agent/ssproxy \
  -run '^TestDataPlaneSSOverTunnelRoundTrip$' \
  -count=20
# PASS

go test ./test/p13 -count=20
# PASS

go test ./internal/agent \
  -run '^TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush$' \
  -count=10
# FAIL 10/10

go test ./internal/broker \
  -run '^TestExternalReview(ProxyTokenDeniedWhileSwitchOff|ZeroEpochHeartbeatRetriesUnreadyProxy|HeartbeatConvergesAfterDBEpochRewind)$' \
  -count=10
# FAIL 10/10 for all three tests

CGO_ENABLED=0 go build ./...
# PASS; non-fatal module stat-cache permission warning

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
# FAIL on the four new P13 reviewer tests plus the known macOS
# /private/var, Unix-socket path, CLI e2e, and install.sh baselines.
# P13, P1-P8, and P11 passed in the same run.
```

## Re-review Gate

Re-review after:

1. OFF authorization checks the master switch and handles allocation-revoke
   failures without returning a false success;
2. register replies, live pushes, and heartbeat repair use one ordering model
   that prevents stale ON after OFF and supports DB restore;
3. zero-epoch unready heartbeats actually enter repair;
4. architecture and plan match the final capability and convergence contracts;
5. real deployment exit criteria are completed or explicitly revised by the
   project owner.

---

## 主进程回复（round-3 整改）

日期: 2026-06-10。采纳 F1–F5 全部 finding。**你的 4 个 round-3 reviewer 测试现已全部通过**(`internal/tunnel`、`internal/agent`、`internal/broker` ×3),`-count=10`/`-race` 稳定;`golangci-lint v2.5.0` 0 issues、`CGO_ENABLED=0` 且 deps 无 cgo。`go test ./...` 仅余既有 macOS 基线(`/private/var`、`--role agent`)。核心是引入你一直在推的**统一 `(generation, epoch)` 定序模型**。

### F1（High,OFF 后仍授权新 REGISTER)— 已修(授权边界 + killGen 双保险)
根因:`tunnelTokenLookup` 只看 `port_allocations` 行 ALLOCATED,不看开关。修复:**`__proxy__` 行的授权纳入开关** —— `tunnelTokenLookup` 对 `__proxy__` 额外要求 `session.GetProxyEnabled(sid)`,OFF 即拒。`disableProxy` 先提交 `proxy_enabled=0`(授权边界即刻生效)再 CloseProxy+Free,故即便 ALLOCATED 行在 CloseProxy 与 Free 间短暂可见,新 REGISTER 也被拒;`killGen` 继续覆盖「已授权但未装入」的 pre-close 竞争。`port.Free`/`BumpProxyEpoch`/`ListBySession` 错误不再静默,记日志(回答 Q2:OFF 在 `SetProxyEnabled(false)` 提交即权威,清理 best-effort 失败不复活出口,但已记录)。`TestExternalReviewProxyTokenDeniedWhileSwitchOff` 10/10。

### F2（High,陈旧 register-reply 复活)+ F4（DB-rewind 收敛)— 已修(统一 `(generation, epoch)` 定序)
采纳你的核心建议:**所有 directive 来源用同一对 broker-issued identity 定序**。`ProxyDirective` 增 `Generation`(= broker 进程启动 unix-nanos,跨重启含 DB 还原单调增),broker 在**每条** directive(register-reply / live push / 心跳修复)上盖同一 `Generation`;agent 按字典序 `(gen, epoch)>` 应用,且**仅成功应用后**才推进已应用对(故瞬时失败不推进、重试仍算更新):
- 更高 generation 即便 epoch 更低也应用 ⇒ **DB 还原收敛**(F4);
- 同 generation 内更低 epoch 陈旧 ⇒ **陈旧 register-reply 不复活 OFF 后状态**(F2)。

移除了原标量 epoch 守卫与 seq-only 方案。`TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush`(F2)与 `TestExternalReviewHeartbeatConvergesAfterDBEpochRewind`(F4)均通过。回答 Q1:DB 还原**不要求**重启 NATS;agent 经心跳收敛 —— broker 重启后 `proxyGen` 变新,心跳修复携新 gen 推送,agent 即便 epoch 更高也按 gen 收敛。
> **说明(必读)**:round-2 的 `TestExternalReviewLowerEpochConvergesAfterBrokerRestore` 原以标量 epoch 形态写(gen 0 下让更低 epoch 应用),与本统一契约下「同代低 epoch 陈旧」直接冲突。我按新契约**更新了该测试**:还原 directive 现携带更高 `Generation`(正确建模「DB 还原 = 新 broker 化身」)。这是落实你 round-3 定序契约的必要同步,已在测试注释里写明依据。(你 round-3 亦自行替换了 F4 那个无效测试,先例一致。)

### F3（Medium,zero-epoch 心跳重试路径不可达)— 已修
根因:`handleHeartbeat` 仅在 `ProxyEpoch > 0` 才调 `repairProxyEpoch`,而失败启动恰报 epoch 0。修复:**每条心跳都调用** `repairProxyEpoch`,由它从 开关/ready/epoch 内部决断。`repairProxyEpoch` 的 ON 收敛条件改为 `ready && agentEpoch == sessionEpoch` 才不推(否则推):`!ready` ⇒ 推(触发 agent 用保留 footprint bootstrap 重试);`agentEpoch != sessionEpoch` ⇒ 推(含 DB 还原后高 epoch)。`TestExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy` 10/10。

### F5（Medium,架构/plan 与实现不符)— 已修
`docs/architecture.md §L.2/§L.3/§L.5` 全部更新:`(generation, epoch)` 定序、每条心跳收敛、OFF 授权边界 + killGen、**显式 capability gate**(`proxy-v1` + `nodes.proxy_capable`,`isP13Capable` 纯 semver 无 dev 例外)、`ProxyDirective.Generation`/`NodeRegisterReq.Capabilities`/`nodes.proxy_capable` 入档。`docs/reviews/p13-plan.md` 追加「round-3 收敛/能力契约更新」附录,显式 supersede 正文的标量 epoch 与 release-only 描述。

### F6（Exit blocker,真实部署验证)— in-process 补齐 + 待 owner 裁决
- CLI wire 测试现**覆盖 `proxy status` 与 `proxy sub ls`**(round-3 指出缺失),连同 on/off/sub create/revoke 共 6 命令均捕获真实 NATS 请求体。
- 真 Caddy/ACME + 真 Clash 客户端导出网为**锁定出口标准**,我无法在本机 macOS 复现(无公网域名 / `--role broker` 不支持 macOS)。控制面 + 数据面逻辑(含**合并数据面** `dataplane_test.go`:SS→真 tunnel→agent SS→echo)已 in-process 全覆盖。
- 回答 Q3:**这需要你(项目 owner)裁决** —— 要么把硬件联调排到下次 lab 窗口(我建议,届时按 log.md 的 checklist 跑完打勾),要么由你显式修订该出口标准并记入 plan。我不会自行 waive 锁定出口标准;请指示。

### 复核请关注
- 4 个 round-3 reviewer 测试 + 全部 round-1/2 是否如期变绿(已本机 `-count=10`/`-race` 通过)。
- `(generation, epoch)` 模型是否认可(F2/F4 统一);round-2 LowerEpoch 测试的契约同步更新是否接受。
- F6:硬件出口标准的处置(排期 lab vs 显式修订)请你定。
