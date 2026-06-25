# B1 review（Stage C 内审 + 主进程采纳）

> Stage C：6×Opus 对抗审查（5 视角只读 + 1 综合）。**Verdict：B1 SOUND，无 BLOCKER、无 must-fix 正确性缺陷**；硬约束全部 held（受保护文件字节未变、无新 subject/ACL/wire-break、schema_version 仍 1、单机字节等价、operator socket 路不变、无安全叙事泄漏）。3 个 MAJOR 全是 doc-drift + test-gap。
> 主进程逐条采纳/驳回如下；改完 `make lint` 0 / `make test` 全绿 / gated d7+d9 -race 通过。

## 驳回（false alarm，综合已判定）
- **ROSTER_UNREACHABLE "BLOCKER"**（review 5）：驳回。该串确实存在（`cluster.go` offline 路 + usage §5.6），doc 与码一致——正是本轮 B1/B2 协调项落实的。
- **gofmt 将 fail `make lint`（review 2/4 MAJOR）**：降级。`golangci-lint v2`（=`make lint`）默认不启 gofmt formatter，实测 0 issues。降为 NIT（F10，已 `gofmt -w` 清理）。

## 采纳（已修）
- **B1-F1 [MAJOR, doc]**：§9.4 误把 `home_catching_up`/`try_again` 列为 ctl-facing expose 码。**核验属实**——三码全在 `tunnelTokenLookup`（agent 反向隧道 REGISTER DENY，agent 自动重试），都不到 ctl `expose`。修：§9.4 移除两行 + 加注解；§9.7.1 改框架（三码全 agent 内部消费）；`error_hints.go` 注释改正（删"try_again DOES reach ctl"的错误声称）。
- **B1-F2 [MAJOR, test]**：`renderClusterStatus` footer（legend+verdict+view）零覆盖。修：加 `TestRenderClusterStatusFooter`（leader / follower / 空 LeaderID→unknown / 空 ViewHost 无 view 行 / 空 Verdict 无 verdict 行 / legend 在场，经 cobra `SetOut`）。
- **B1-F3 [MAJOR, test]**：`IsLeaderView=false` **填充**路径未被执行（只测了字面 false 的序列化）。修：gated `test/d7` 加 `testD7FollowerStatusViewSource`——真 2 节点，在 **follower** 上 `StatusReport` 断言 `IsLeaderView==false`、`ViewHost==self`、`LeaderID==leader`。
- **B1-F4 [MINOR]**：`summarizeClusterHealth` 只从 writable reply 取 LeaderID（follower 也带）。采纳为**有意设计**（不信任 stale follower 报的 leader）——加注释明确 + `TestSummarizeLeaderIDWritableOnly` 钉行为。
- **B1-F5 [MINOR]**：force-single verdict 在 ≥2 broker 应答时仍说"running on one broker"（straggler 残留 marker）= 事实错。修：改为"at least one broker reports emergency mode (force_single_active) … check the leader"（对任意应答数都准确）+ `TestCtlVerdictForceSingleStraggler`。
- **B1-F6 [MINOR]**：`--remote --json`（`ctlClusterSummary`，无 schema_version）与 socket `--json`（`ClusterStatusReport`，有 schema_version）形状不同、无判别符。修：`ctlClusterSummary` 加 `view:"ctl-remote"` 判别符 + §5.6 doc 说明 + `TestRenderCtlStatusJSONDiscriminator`（有 view、无 schema_version）。
- **B1-F7 [MINOR]**：`--remote`+`--offline` 非互斥、offline 静默赢。修：`MarkFlagsMutuallyExclusive("offline","remote")` + `TestClusterStatusRemoteOfflineMutuallyExclusive`。
- **B1-F9 [MINOR]**：ctl verdict 印裸 token "force-single"（gateBlockMessage 刻意 scrub）。采纳——force-single verdict 改用 `force_single_active`（condition 名，非命令）+ `TestCtlVerdictNoNuclearLeak`（六态均不含 `cluster force-single`/`cluster recover` 祈使）。非安全 BLOCKER（描述态、footer 重定向只读 status），但纪律一致。
- **B1-F10 [NIT]**：`protocol.go` gofmt 对齐。修：`gofmt -w`。
- **B1-F11 [NIT]**：`cluster status` Short 宣称所有模式 `1=DEGRADED`，但 `--remote` 不出 1。修：`--remote` flag help 注明"exit 0/2/3 only, never 1/DEGRADED"。
- **B1-F12 [NIT]**：ctl "fewer than 3 visible — not necessarily HA" 可能惊到健康 N=2/3。修：软化为"fewer than 3 answered — re-run on the leader for the exact voter count + HA verdict"。
- **B1-F13 [NIT, 预存]**：`ClusterNodeStatus.ReachSource` 注释 stale。修：注释更新为真实枚举值（B1 触碰了该结构体）。
- **proposed #9/#10**：加 `TestCtlVerdictEmptyNodeIDFloorsToOne`（空 NodeID writable→"1 broker(s)"）+ `clusterVerdict` 表加 {2,HEALTHY_HA}→"NO fault-tolerant writes"（优先级）与 {4}→"survives 1 broker failure"（偶数 voter）。

## 部分采纳 / 记残留
- **B1-F8 [MINOR]**：gated d9 ctl 测试只 1 次 `nc.Request`、未跑 fold/force-single drill。**残留**：fold cores（`summarizeClusterHealth`/`ctlVerdictLine`/`ctlExitCode`）已被 cmd/tether 单测充分覆盖（综合自评"非正确性洞，仅覆盖深度"）；端到端 fold + force-single drill 受 package 边界限制（`probeClusterHealth`/`summarize` 在 package main，test/d9 不可 import），列为后续。d9 现有测试已证 wire 可达。

## 触碰文件（本轮修复）
码：`cmd/tether/{cluster.go, cluster_status_nats.go, error_hints.go}`、`internal/adminsock/protocol.go`。
测：`cmd/tether/cluster_status_nats_test.go`、`internal/broker/clusterstatus_test.go`、`test/d7/integration_test.go`。
档：`docs/usage.md`（§9.4/§9.7.1/§5.6）。

## 出口
内审通过、硬闸全绿。**待用户外审**（§3 step 6）。B1/B2 协调项 ROSTER_UNREACHABLE 已落 B1。
