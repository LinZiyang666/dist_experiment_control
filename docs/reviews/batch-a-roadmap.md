# 批次 A 执行路线图（结构性质量债 · 低风险高收益档）

> 来源：`docs/reviews/quality-audit/2026-07-25-structural/S1-refactor-roadmap.md` §4。
> 本文件是**执行视角**的路线（顺序 / 依赖 / 验证门 / 停止点），不重复 S1 的论证；每项的"为什么"回查 S1。
> 定位：一个叶子增量，走一遍 §3 的 3 阶段 7 步，**在 step 6 外审门停止**。

## 0. 范围与硬边界

**范围**：S1 批次 A 全部 15 项（A1–A15）。总计约 13 人日，净减约 500 行生产代码。

**硬边界（三条，任一被破即中止并回报）**：

1. **零 wire 变更** —— `internal/proto.ProtoVersion` 不动；所有错误码/subject 的**字符串值一个字节不改**。现网混版本 broker/agent/ctl 必须完全无感。
2. **零不变量变更** —— `docs/architecture.md` 与 `distributed-broker-architecture.md` 的不变量不因本增量改变。周围带 `INVARIANT` / `DELIBERATELY` 注释的代码不动。
3. **不碰部署面** —— 不改 `scripts/install.sh` / `nats.conf` 渲染 / systemd unit / 集群生命周期动作。**因此本增量不需要跑 simcluster drill**。

**明确不做**（S1 §7 已裁决，此处复述以防实施时手滑）：
- 不为"包数量好看"合并 import 面为 0 的正当叶子包
- 不删高密度注释（只换寻址锚点）
- 不改测试与 `docs/reviews/` 总量（债在索引不在体量）
- 不做日志级别大调整（228 Warn / 23 Error 是 30 处判断题，A15 只写规约不改级别）
- 不动 `transferRefusalErr` 的输出格式（`test/simcluster/drills/61-transfer-edges.sh` 按 `code=<X>` grep）

## 1. 为什么是这个顺序

15 项里绝大多数彼此独立，但有三处真实耦合，顺序由它们决定：

| 耦合 | 约束 |
|---|---|
| A1 建立错误码 SSOT，A3 要把超时统一到 75，A13 引入一条新的拒绝分支 | **A1 必须最先**——后两者都要往它建立的分类表里加条目 |
| A8 要同步 `docs/usage.md §9.13` 退出码表 | **A8 必须最后**——它同步的是 A1/A3/A13 落定后的结果 |
| A2 是 B1（ingress 准入收口）的可行性样例 | A2 紧跟 A1——它要证明"gate 可提取、错误码逐字不变"这条路走得通 |

其余按"改动面从小到大、验证从易到难"排。

## 2. 四组实施顺序

### 组 1 — 错误面收口（A1 → A2 → A3）

打通"错误码有归类、gate 可提取、`--wait` 可中断"这条线。这是全批次唯一**今天就在现网造成错误行为**的一组。

| 项 | 动什么 | 净行 | 验证门 |
|---|---|---|---|
| **A1** | 新建 `internal/proto/codes.go`；27 个未归类码归到 64/69/70/75/77；新增 `TestErrorCodeCoverage` + `TestErrorCodeCoverageSelfCheck`；消掉 40 处 `"<code>: "+err` 拼接 | ~0 | 守门测试从红转绿；`go build` 后二进制与 Step 1 前**字节等价**（`cmp` 验） |
| **A2** | `internal/broker/transfer.go:1029` 的 `handleCapsReq` 改调同文件上方 60 行的 `transferGate` | -14 | `cmd/tether/g67_caps_test.go` + `test/cli_e2e/transfer_test.go`；错误码字符串**逐字相同** |
| **A3** | 抽 `pollUntil(ctx, cfg, step)`，收口 9 个手写轮询点；`--wait` 响应 Ctrl-C；`cluster reconcile nats --wait` 超时从 70 统一到 75 | -120 | **新增**：每个 `--wait` 命令一条「cancel ctx 后 ≤1 tick 返回 75」表驱动测试 |

**组 1 出口断言**：`TestErrorCodeCoverage` 绿；`cluster join approve --wait` 在 SIGINT 后 ≤1 tick 退出且退 75。

### 组 2 — 删除面（A4 → A6 → A7 → A13 → A11）

全是"删掉或接线活得比用途更久的东西"。**危害排序不是行数排序**——A4 的 `port.Revoke` 是陷阱不是垃圾。

| 项 | 动什么 | 净行 | 关键风险 |
|---|---|---|---|
| **A4** | 删死符号 + 订正误导性 godoc：`port.Revoke`/`PlanRevoke`、`subhttp.LiveProxyNodes`/`Serve`、`proto.RehomeDirective` 的假保证、`xferaudit.TransferReqID` + `plan.go:49` 文档错误、~25 个零引用叶子 | -160 | **三个例外不删**：`clusterupgrade.AgentUnknown`（iota 零值 fail-closed 是载荷）、`ClusterGrowSchemaVersion`（要 stamp，属 B4）、`serveconf.WssListen`/`WSInternal`（`install.sh:547` 真在写）。**`port.Revoke` 是幂等的**（`RowsAffected==0` 回查存在返回 nil），`RevokeAllocation` 直接 `ErrNotFound`——`port_test.go:180/232/342/370` 依赖幂等语义，**不能 sed 全替**；`planPortStateChange` 仍被 `PlanFree` 用，不随 `PlanRevoke` 删 |
| **A6** | `cmd/tether/serve.go:437` 的 `loadAuthCalloutSeeds` 接上 account seed kind 校验 | -95 或 +3 | 唯一行为变化是"错误的 `account.nk` 现在 fail fast"。`test/p1/foundation_risk_test.go:35` 的断言要重定向到真实生产路径 |
| **A7** | 新增 `internal/auth/acl_reconcile_test.go` 双向对账；删 3–5 条死授权；`requirements.md` 把 `kick`/`rotate-pin` 降级为「未实现」 | -21 | 对账测试**今天就会红**。授权变化只体现在新签发 JWT 上；最后一个生产 publisher 在 `55b1451` 被删（早于 v0.1.0），现网无连接依赖 |
| **A13** | 删 `internal/broker/clusterdrain.go:85-206` 的 retire 分支 + `ErrStreamsNotAtTarget`；adminsock 收到 `Retire:true` 返回明确拒绝并指向 `cluster_retire` | -55 | **跨版本行为变化，需写 release note**。对老 CLI 是改善（原来静默走无保护路径）。`test/d7/integration_test.go` 要带 `-tags d7_integration` 跑 |
| **A11** | 抽 `internal/tokenhash` 零依赖叶子包，收口三份 `hashToken` | +2 | `hex(sha256(x))` 不变 ⇒ DB 已有 hash 继续匹配。这是上一轮"只加注释不修"的失败证据——注释生效了，一年后仍长出第三份 |

**组 2 出口断言**：`deadcode` 重跑（**必须带全部 8 个 build tag**）不再命中已删符号；ACL 对账测试绿；`make test` 全绿。

### 组 3 — 结构收口（A9 → A10 → A12 → A14 → A5 → A15）

把"同一份逻辑手抄 N 遍、改一处漏一处编译器不报错"的地方收成单一编辑面。

| 项 | 动什么 | 净行 | 关键风险 |
|---|---|---|---|
| **A9** | `internal/tunnel/tunnel.go` 引入 `fenceSnap` + `fenceChangedLocked`，三条手写 `\|\|` 链坍缩成一次调用 | -15 | 注释记录了 round-2 F1 → round-5 F1 → round-6 F4 每轮外审各加一维。**漏写一项编译器不报错**，后果是已 kill 的公网 exit 在 REGISTER 竞态里复活重新 bind——数据面安全洞 |
| **A10** | 抽 `finalizeTransfer(entry, rec)` 收 `emitTerminal → deleteObject → cancel → remove` 四处 | -45 | watchdog 那份**没有** `entry.cancel()`（正确但代码里看不出为什么）——提取时必须变成**显式参数或显式分支，不能抹掉** |
| **A12** | 抽 `internal/httplisten`：`Bind(addr, requireLoopback)` + `Serve(ctx, ln, handler, name)` | -45 | `clustermanifest` 当前是 `srv.Close()` 硬关 + 用 `!=` 而非 `errors.Is` + watcher goroutine 活过返回。**这是已有漂移不是理论风险**；改成优雅关是行为改进 |
| **A14** | `clusterstatus.go:127` 的 `StatusReport` 拆成 substrate→assemble→health→banner→verdict；`type bannerBuilder []string` | -90 | **banner 文案字节必须逐条保持**（运维可见，`natsconf.DeClusterRemedy*` 是共享 SSOT，不在这步顺手改文案）。**新增**：banner 组合表驱动测试（今天缺） |
| **A5** | 抽 `loopSet{Go, Join}` 自计数，消掉 `clusterwrite.go:434` 的手写 `loopCount=4/5`；4 条游离循环的 `lastTick/runs/lastErr` 进 `RuntimeReport` | -10 | `loopCount` 在 `_test.go` 里 **0 命中**。写小了 `clusterShutdownOrdered` 第 3 步会在循环仍做 JS/raft I/O 时推进、紧接 `nc.Drain`——重演该文件自己写明的「publish-after-Drain 静默丢审计，泄漏门抓不到」。**registry 迁移那一半属 B7，本项不做** |
| **A15** | `hclog.Logger → slog` 适配器（WARN+ 转发）；leadership 当选/下台各一条 Info；三个计数器进 `brokermetrics.Snapshot` | +60 | `node.go:278` 注释写着「D3 wires logging」——**D3 从未接线**。`internal/cluster` 5,300 行只有 5 条日志。**只写规约不改级别** |

**组 3 出口断言**：`-race` + 仓库内建 NumGoroutine/fd 泄漏门全过（A5/A9/A12 触碰并发面）；banner 文案 golden 逐字不变。

### 组 4 — 文档尺订正（A8）

必须最后做，因为它同步的是前三组落定后的结果。**纯文档，零代码风险。**

按性价比排序，**不做全文重写**：
1. （30 分钟拿最大收益）`docs/architecture.md` 顶部加 status banner——标明 §A–§K 是 proto v1 单 broker 历史基线，v2/集群面的绑定契约在 `distributed-broker-architecture.md`。实测文中 **70 处 `tether.v1`** 而 `ProtoVersion = 2`；**72 处 frp** 而 `go.mod` 零 frp 依赖
2. `CLAUDE.md §1` 补两行文档地图（`distributed-broker-architecture.md` 691 行、`deploy-tier-gotchas.md` 618 行，两份被 README 和 runbook 称为绑定契约却在 CLAUDE.md 里 grep 计数为 0）；删 phase 分支条款（与实际全部直接落 main 冲突）；模型约束去掉写死的版本号
3. `docs/reviews/` 一次 `git mv` 分三层 + 一份 `INDEX.md`（引用图证明 **plan 是载荷、review 是残渣**：65 份 plan 只 6 份孤儿；119 份 external-review 里 89 份孤儿）
4. `docs/requirements.md` 顶部加 5 行 banner（`ctl ` 35 处 / frp 19 处 / Relay·Controllerd 28 处是另一个产品的名词）。**不做机械改名**
5. `docs/usage.md §9.13` 退出码表按 A1/A3/A13 的结果同步；**全局扫** `grep -rn "70" docs/*.md | grep -i retry` 确认没有别处还在教「70 可重试」
6. 修 `error_hints.go:24` 的 `tether session list` → `session ls`（实测 `session list` 打 help 退 0——一条跑了不报错也不做事的命令），并扩 `command_tree_inventory_test.go` 断言 `error_hints.go` 里每个反引号命令都能被 cobra 解析到叶子

## 3. 验证门（逐级，不可跳）

| 级别 | 何时跑 | 内容 |
|---|---|---|
| **L0 增量内** | 每项改完 | 该项自带的新测试 + 直接相关的既有测试（例：A2 跑 `g67_caps_test.go`） |
| **L1 组出口** | 每组结束 | 该组涉及包的 `go test ./internal/<pkg>/...`；触并发面的加 `-race` + 泄漏门 |
| **L2 提交前硬闸** | 全部 15 项完成后 | `make test` + `make e2e-parallel`（全矩阵闸门）+ `make lint` 全绿，**任一不过不算 done** |
| **L3 部署面** | **不适用** | 本增量不碰部署面，不跑 simcluster drill |

按 CLAUDE.md §5「按需测试」纪律：开发迭代中只跑碰过的那一块，`make test` / `make e2e-parallel` 只在 L2 硬闸跑一次，**不为验证一处小改反复全量跑**（满负载竞争徒增 flake 噪声）。

## 4. 流程位置与停止点

```
step 1  多专家对抗性草拟 plan（Workflow）
step 2  主进程定稿 → docs/reviews/batch-a-plan.md
step 3  主进程实现 + 测试（本路线图的组 1→4）
step 4  多专家对抗性审查（Workflow）→ docs/reviews/batch-a-review.md
step 5  主进程逐条采纳/驳回 + 整合专家新增测试
step 6  ⛔ 外审门 —— 停在这里，交用户人工外审
step 7  （外审通过后）commit + push
```

**本次推进到 step 5 结束为止。** step 6 的外审报告由用户产出（`docs/reviews/batch-a-external-review.md`），主进程在报告文件内逐条回复并修改后才进 step 7。

按 `feedback_no_git_add_during_external_review`：**外审阶段主进程不 `git add`**，暂存是外部审查者的工作。

## 5. 需要写进 release note 的对外可观察变化

本增量 wire 字节不变，但有三处对外可观察的行为变化：

1. **退出码**：约 27 个错误码从 70 变成 64/75/77。这是目标本身且是严格改善（原状态在指示自动化重试物理上不可能成功的操作），但会改变现有监控脚本的分支行为。
2. **`adminsock` 拒绝 `Retire:true`**（A13）：对老版本 CLI 是改善（原来静默走无保护路径），但属跨版本行为变化。
3. **错误的 `/etc/tether/account.nk` 现在 fail fast**（A6）：原来是 broker 正常启动、每个客户端在 auth_callout 阶段被静默拒绝（「服务起来了但没人能连」）。

## 6. 已知的实施陷阱（实施时逐条对照）

- **A1 Step 2 的分类表是产品决策不是机械工作**——S1 给了建议分类，实施时**逐条再判一次，不要照抄**。
- **A1 Step 1 必须零行为变化**——用 `go build -o /tmp/a && git stash && go build -o /tmp/b && cmp` 验二进制字节等价再往下走。
- **只把跨包共享的码搬进 `proto`**——broker 包内私有的码搬过去会**制造一个新的跨包同步点**，正是要消灭的东西。判据：同时出现在 `internal/broker/*.go` 和 `cmd/tether/error_hints.go`。
- **AST 守门测试必须带自检**（`TestErrorCodeCoverageSelfCheck`）——仿 `test/determinism/lint_skeleton_test.go:262` 的 `TestNoStrayVersionLiteral`。没有自检，扫描器退化成空断言时没人知道。
- **`deadcode` 重跑必须带全部 8 个 build tag**——不带会把 5 个 `*ForTest` 误报成死代码。
- **`go/packages` 引用计数要处理 `pkg [pkg.test]` 变体**——带包内测试的包会生成该变体，`types.Object` 身份不同，按对象身份统计会漏掉全部包内测试引用。
- **banner 文案与错误码字符串一律字节保持**——A14 和 A1 都有"顺手改文案"的诱惑，一律不改。
