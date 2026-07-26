# L05 — 错误处理、日志与可观测性的一致性（结构性质量审计）

> lane key: `error-observability` · 日期 2026-07-25 · 只读审计，未修改任何实现代码
> 上一轮 `quality-audit/01..06` 找的是 bug；本轮找的是**冗余、重复、职责错位、演进阻力**。

---

## 结论

**这条 lane 的问题不是"臃肿"，是"失血"。**

按行数算，tether 的错误/日志/可观测层是**偏瘦**的：68,328 行生产代码里只有 348 条日志调用（每 196 行一条），
`internal/brokermetrics` 只有 173 行、14 个 gauge，`cmd/tether/exitcode.go` 只有 104 行。没有任何一处是
"抽象过度"或"框架自嗨"。真正的债是另一种形状——**同一件事在互不知情的多处各做了一遍，而收口层只覆盖其中一部分**：

- **92 个 wire 错误码没有 SSOT**，散落在 broker/agent 的裸字面量里；ctl 侧的 34 条 hint 表和 45 条 exit-class 表
  与它们**零编译期链接**。61 个码没有 hint，54 个码没有 exit class（默认落到"未分类 70"）。
- **同一个失败（`subject_malformed` / `actor_invalid`）以 7 种不同的应答形状发出**，运维看到提示还是裸串、
  退出 64 还是 70，取决于他跑的是哪个动词。
- **ctl 的错误呈现层分叉成 6 个渲染器 + 2 张 hint 表**，`cmd/tether` 里 332 处裸 `fmt.Errorf` 对 130 处分类构造，
  而 `docs/usage.md §9.13` 明写"70 当可重试"——于是 `--session is required` 这种永不会成功的调用
  会让一个健壮重试脚本无限重试。
- **hashicorp/raft 的日志被 `io.Discard` 吞掉**（`node.go:278` 注释写着 "D3 wires logging"，D3 从未做），
  leadership 转换零日志，`internal/cluster` 5,300 行只有 5 条日志。集群故障事后无法从外部复盘。
- **8 个内部 atomic 计数器里只有 1 个进 `/metrics`**；三个"审计丢失"计数器（`TruncationLossCount` /
  `LagExceededCount` / `DeletedStreamLossCount`）的**生产调用者是零**，只有测试读它们。

**bloat 打分：4 / 10。** 理由：绝对体量正当甚至偏少，没有可以整块删掉的死抽象；扣分来自
"6 个渲染器 / 40 处 concat / 10 处 handler prologue 复制粘贴"这类**横向重复**，以及
"注释承诺了但从未兑现的接线"（raft 日志、ops 计数器）。这不是屎山，是**没收口的正常工程债**——
但对一个**已经在现网跑 1 broker + 6 agent 车队**的运维工具，"运维看不到"比"代码难看"贵得多。

判定：**significant-debt**（不是 structural-problem——没有需要推倒的错误抽象，全部是"补一层收口"能解决的）。

---

## 范围与方法

**范围**：全仓生产代码（241 文件 / 68,328 行，不含 `_test.go` 与 `test/`）的错误构造/包装/传播、
日志调用、wire 错误码的产生与消费；重点文件 `internal/broker/observability.go` (299)、
`internal/broker/audit_publisher.go` (615)、`internal/xferaudit/plan.go` (96)、
`internal/brokermetrics/metrics.go` (173)、`internal/broker/metrics_wire.go` (108)、
`cmd/tether/error_hints.go` (243)、`cmd/tether/exitcode.go` (104)、`internal/broker/audit.go` (264)、
`internal/broker/rehome_events.go` (67)、`internal/broker/incident.go` (204)、
`internal/broker/alert_webhook.go` (192)、`cmd/tether/logging.go` (30)。

**方法**：
1. 逐行读上述 12 个重点文件（≈2,400 行）+ `internal/broker/sessions.go` / `run.go` / `exec.go` / `expose.go`
   的 handler prologue、`internal/agent/transfer.go` 的 `pathErr` 体系、`internal/tunnel/tunnel.go` 的
   DENY 分类路径、`internal/cluster/node.go` 的 raft 配置。
2. 用 `grep`/`python3` 做只读集合运算：把"broker/agent 实际发出的 code 字面量"与
   "`brokerCodeHints` / `brokerCodeExitClasses` 的键"做差集（脚本在 `/home/weiland/.claude/jobs/.../tmp/`，未写入仓库）。
3. 统计学口径：`fmt.Errorf` 1,436 / 其中带 `%w` 942（66%）；`errors.New` 100；`errors.Is` 178 / `errors.As` 19；
   sentinel `Err*` 28 个；自定义 error 类型 12 个；日志调用 348 条（Debug 12 / Info 85 / Warn 228 / Error 23）。
4. **未运行**任何重测试（无 `make test` / `make e2e` / `test/simcluster`）。

---

## Findings

### F1 — [critical] wire 错误码词表没有 SSOT：92 个码散落为裸字面量，与 ctl 的两张表零编译期链接

**证据**
- `cmd/tether/error_hints.go:19` — `brokerCodeHints`，34 个键，全是**字符串字面量**。
- `cmd/tether/error_hints.go:78` — `brokerCodeExitClasses`，45 个键，同样是字面量。
- `cmd/tether/error_hints.go:129-138` — 代码自己承认了这个洞：
  > "The literal is authored in internal/broker (`codeDataplaneNotConverged`); there is no compile-time
  > link across the two packages, so `TestDataplaneNotConvergedCodeIsWireStable` pins it from this side."
  为**一个**码写了一个 pin 测试；另外 91 个没有。
- `internal/adminsock/protocol.go:126-145` — 18 个 `Code*` 常量，是全仓**唯一**被类型化的一段词表。
- 其余 53 个 `Code: "..."` 裸字面量分布在 `internal/broker`（大头）、`internal/agent`、`cmd/tether`。

**量化（集合差集）**
| | 数量 |
|---|---|
| 生产代码实际发出的 code | **92** |
| 其中**没有 hint**（运维只看到 broker 的原始英文串） | **61** |
| 其中**没有 exit class**（一律退 70 = "未分类，tether 侧缺口"） | **54** |
| hint 表里**从未被发出**的键（死条目） | 4（`already_exists` / `home_catching_up` / `proxy_disabled` / `try_again`）※ |
| exit-class 表里从未被发出的键 | 8 |

※ 这 4 条不是"防御性预留"那么无辜：`already_exists` **确实**被发出
（`internal/broker/sessions.go:57`），但它被塞进 `SessionCreateResp.Error` 而不是 `.Code`，
所以 hint 查表的键对不上（见 F2）。

**已经发生的漂移**：同一类失败出现了两种拼写——
`sha_mismatch`（`internal/adminsock/protocol.go:145`、`internal/agent/transfer.go:130,291`）
vs `sha256_mismatch`（`internal/agent/upgrade.go:101,178`）。**只有后者有 hint**。

**为什么这阻碍演进**：加一个新的失败码是**两处无关联的编辑**——在 broker 写一个字面量，在 ctl 加两行 map。
漏掉第二步：编译过、`go vet` 过、`make lint` 过（仓库无 `.golangci.yml`，默认集不含任何 map-key 校验）、
`make test` 过。运维得到的是一个裸 token + 退出码 70，而 `docs/usage.md §9.13` 告诉他的监控"70 要重试"。
今天已经有 61 个码处在这个状态。改一个码的**拼写**同理静默降级。

**建议**：把 code 提升为 `internal/proto` 的常量块（它们**就是** wire surface，按 `CLAUDE.md §5`
wire 的 SSOT 归 proto）；`brokerCodeHints` / `brokerCodeExitClasses` 改用常量做键；加一个表驱动测试断言
"每个 proto code 常量都在 exit-class 表里有条目"。字符串值一个不改 ⇒ 现网无需重装。

**量化收益**：净增 ~100 行常量，减 53 处裸字面量；**换来 92→0 的静默漂移面**。
`estLinesRemovable: 0`（这是结构改造不是删代码）。`changeRisk: low`。`touchesWire: true`（值不变，只加名字）。

---

### F2 — [high] 同一个失败以 7 种应答形状发出，运维看到什么取决于他跑的是哪个动词

**证据**（同一个 `subject_malformed`，7 种编码）：

| 形状 | 位置 | 运维实际看到 |
|---|---|---|
| code 字段 + 空 detail | `internal/broker/expose.go:169` | hint 命中 |
| code 字段 + 人读 detail | `internal/broker/broker.go:1295` | hint 命中（detail 被 hint 覆盖丢弃） |
| **码塞进 Error 字段** | `internal/broker/sessions.go:24` | hint 命中（巧合：无后缀） |
| **码 + ": " + 明细，全塞 Error** | `internal/broker/sessions.go:28,33,39,48` | **hint 落空 → 退 70** |
| **码 + ": " + 明细，塞 reason** | `internal/broker/exec.go:35`、`run.go:37` | run 落空 / exec 命中（见下） |
| code 字段 + 原始 subject 当 detail | `internal/broker/transfer.go:556` | **transfer 路径完全不查 hint** |
| reason-only | `internal/broker/run.go:28`、`proxy.go:43` | 视表而定 |

`actor_invalid` 同样有 3 种（`Code:` 结构化 18 处 / `"actor_invalid: "+err` 拼接 / exec-run 的 reason 变体）。

**拼接形式全仓 40 处**，`internal/broker/run.go` 一个文件占 17 处
（`run.go:37,43,53,68,80,86,96,131,136,145,158,167,173,183`）。

**放大器**：`cmd/tether/error_hints.go:156` 的 `brokerCodeHints[code]` 是**精确查表**，
只对 `agent_rejected:` 前缀做了一次特判（:161）。而 `execFailureMessage`（:228）**会**在 `:` 处切分，
`runFailureMessage`（:216）**不会**。于是 broker 发出的同一个 `json_parse: <detail>`：
- 走 `exec` → 切分成功 → 运维看到 "the agent couldn't parse our run request — tether bug, please report."
- 走 `run` → 切分失败 → 运维看到 "run rejected by agent (json_parse: unexpected end of JSON input)"

**最坏一处**：`internal/broker/sessions.go:60` —— `SessionCreateResp{Error: err.Error()}`，
把一条**原始 Go 错误串**（可能是 SQLite 的文本）当作 code 送上线，ctl 拿它去查 hint 表。

**根因**：`SessionCreateResp`（`internal/proto/messages.go:15-20`）是唯一**没有 `Code` 字段**的应答类型，
所以它的所有失败都得把机器标识符走私进人读字段。`cmd/tether/session.go:62-66` 的注释还写着
"known prefixes like `session_already_exists` still match the hint table"——这句话有两处错：
hint 表里的键是 `already_exists` 不是 `session_already_exists`，而且查表根本不做前缀匹配。

**为什么这阻碍演进**：加一个新动词，作者必须在 8 个 `reply*Err` helper 里挑一个、在 4 种编码里挑一种，
没有任何机制告诉他哪种是"对的"。既有代码给出的示范是互相矛盾的。结果是每加一个动词，
"运维能不能拿到提示"就重掷一次骰子。

**建议**：
1. 给 `SessionCreateResp` 加 `Code string \`json:"code,omitempty"\``——**纯增量**，老 broker 不发该字段时
   ctl 回落到既有的 `Error` 分支，wire 兼容（`omitempty` ⇒ 不改既有字节）。
2. 定义一个 `type Fail struct{ Code, Detail string }` + 一个 `replyFail(msg, resp any, f Fail)`，
   让 8 个 helper 收敛成 1 个；禁掉 `"<code>: "+err.Error()` 这种拼接。
3. `runFailureMessage` 补上 `execFailureMessage` 已有的冒号切分。

**量化**：可净减 ~150 行（40 处拼接 + 8 个 helper 中的 6 个）。
`changeRisk: medium`（`SessionCreateResp` 加字段是加性的；ctl 需保留回落分支）。`touchesWire: true`（加性）。

---

### F3 — [high] raft 日志被丢弃 + leadership 转换零日志：集群故障事后无法从外部复盘

**证据**
- `internal/cluster/node.go:278` — `c.LogOutput = io.Discard // D1: discard raft's internal chatter; D3 wires logging`
  **D3 从未接线**（全仓搜 `LogOutput` / `hclog.` 只有这两处，没有第三处）。
- `internal/cluster/transport.go:135` — `Logger: hclog.NewNullLogger(), // discard raft-net chatter (D1 convention)`
- `internal/cluster` **5,300 行生产代码只有 5 条日志**，其中 3 条在 `fsm.go:120,139,220`（apply 病态），
  2 条在 `offline.go:201,380`（离线重建）。
- `internal/broker/observability.go:231,251-260` — `runObserveLoop` 自己维护 `wasLeader`，
  在 `!wasLeader && isLeader`（当选）和 `wasLeader && !isLeader`（下台）两个 edge 上**都不写日志**，
  只在下游动作失败时才 Warn（:254）。
- `internal/adminsock`（1,245 行，root-only 运维控制面）只有 1 条日志。

**具体的"无法从外部诊断"实例**：现网 racknerd 单 broker 在 2026-07-04 发生过 force-single 后
JetStream 503 烂了 5 天的事故（见 memory/project_racknerd_forcesingle_js_incident）。若在多 broker 场景
重演"03:14 leadership 抖了一下，之后写入开始超时"，运维手上有的是：
- raft 的选举/心跳/快照/`failed to contact` 日志：**全部被 io.Discard 吞掉**；
- broker 侧 leadership 转换日志：**不存在**；
- `tether_broker_is_leader` gauge：需要 Prometheus 当时正在采、且抖动窗口 > 抓取间隔；
- `tether cluster export-incident`（`internal/broker/incident.go`）：打包 alerts + membership + audit 三源，
  **唯独没有 raft 叙事**——因为从未采集，无从打包。

**为什么这阻碍演进**：raft 是这个项目最难的一块，而它是唯一一块**没有运行时可观测叙事**的。
下一次要改 HA 行为（grow/retire/force-single 的任何一处），验证手段只剩 hermetic 测试 + simcluster drill；
现网出问题只能靠复现。这直接抬高了每一次 HA 改动的风险成本。

**建议**：写一个 40 行的 `hclog.Logger` → `slog` 适配器，把 raft 日志按 WARN+ 转发（DEBUG 级别在
`--log-level debug` 下才开）；在 `runObserveLoop` 的两个 edge 上各加一条 `Info("cluster: leadership acquired/lost", "term", ...)`。

**量化**：净增 ~40 行。`estLinesRemovable: 0`。`changeRisk: low`（纯观测，不改控制流）。

---

### F4 — [high] 日志级别分类塌缩：228 Warn / 23 Error / 12 Debug，`--log-level` 不能用于分流

**证据**（全仓生产代码，348 条日志调用）

| 级别 | 条数 | 占比 | `--log-level` 设为该级时保留 |
|---|---|---|---|
| Debug | 12 | 3.4% | 100% |
| Info | 85 | 24.4% | 96.6% |
| **Warn** | **228** | **65.5%** | 72.1% |
| Error | 23 | 6.6% | 6.6% |

Warn 里同时装着这些东西：
- `internal/broker/audit_publisher.go:287` — "audit lost to snapshot truncation (**accepted bounded loss**)"，**审计数据永久丢失**
- `internal/broker/audit_publisher.go:398` — "audit dropped — session + its history stream are gone"，**同上**
- `internal/broker/alert_webhook.go:151` — "queue full, **dropping event**"，**告警丢失**
- `internal/clusteroffline/offline.go:671` — force-single 后 seed 集合只剩死端点（冷启动客户端会连不上）
- `internal/agent/agent.go:1496` — "agent: kill orphan SIGTERM"，**日常清理**
- `internal/broker/audit.go:208` — "orphan stream delete"，**日常清理**

而 Error 只有 23 条，且并不是"更严重的那一批"——`internal/broker/broker.go:899,917,934` 是三个 HTTP
server 的退出，`internal/cluster/fsm.go:139` 是"apply txn failed, **retrying**"（会重试的）。

**运维手册已经受害**：`docs/broker-ops.md:483` 教运维跑
`sudo journalctl -u tether-broker --since '1 hour ago' -p warning`。按上表，`-p warning` 保留 72% 的日志点，
等于没过滤。运维实质上只有两档：全看，或几乎不看。

**为什么这阻碍演进**：没有分级规约，每个新日志点的级别是作者当场拍的，塌缩只会继续。
一旦以后想做"Error 触发 PagerDuty"这类告警接入，现有 23 条 Error 里既有需要叫醒人的
（`cluster_grow_cutover.go:223` "the data plane is DOWN on this broker"）也有不需要的，无法直接用。

**建议**：在 `docs/architecture.md` 里写死三行规约——
**Error = 已发生不可自愈的损失或不可用**（审计丢失、告警丢失、数据面 DOWN、INCONSISTENT）；
**Warn = 降级但会自愈**；**Info = 状态迁移时间线**；**Debug = 稳态重试细节**。
按此把 3 处 accepted-loss（`audit_publisher.go:287,398`、`alert_webhook.go:151`）提到 Error，
把稳态重试类降到 Debug。

**量化**：改动 ~30 处级别（0 净行数）。`estLinesRemovable: 0`。`changeRisk: low`。

---

### F5 — [high] 内部计数器只服务测试不服务运维：8 个 atomic 计数器只有 1 个进 `/metrics`

**证据**

| 计数器 | 定义 | 导出方法 | **生产读者** |
|---|---|---|---|
| `lossCnt`（快照截断丢失的审计条数） | `audit_publisher.go:87` | `TruncationLossCount()` :149 | **0**（仅 3 处测试） |
| `lagCnt`（publish lag 越过 TrailingLogs 的次数） | `audit_publisher.go:88` | `LagExceededCount()` :268 | **0**（仅 2 处测试） |
| `delStreamCnt`（因流被删丢弃的审计条数） | `audit_publisher.go:89` | `DeletedStreamLossCount()` :421 | **0**（仅 3 处测试） |
| `drops`（webhook 队列满丢弃的告警数） | `alert_webhook.go:90` | `Drops()` :192 | **0**（注释写 "test + **ops** counter"，但无 ops 读者） |
| `dedupCount` | `cluster/fsm.go:64` | `Node.DedupCount()` `node.go:422` | **0**（仅 test/d4,d5,d8） |
| `reapplyCount` / `rejectCount` | `cluster/fsm.go:57,70` | 无导出 | **0** |
| `xferUnreapableBuckets` | `broker.go:507` | — | **1**（`metrics_wire.go:52` → `/metrics`）✅ |

这三个 loss 计数器的文档注释自己说明了它们的用途：
> "Tests assert it fired (E-A6) or stayed 0 (steady state)." — `audit_publisher.go:147-148`

也就是说，它们是**为了让测试不 vacuous 而存在的**，从没打算给运维用。

**具体的"无法从外部诊断"实例**：审计记录因 raft 快照截断而永久丢失（`audit_publisher.go:206`
`recordTruncationLoss`）时，运维能拿到的全部是 stderr 上一行 Warn。没有计数、没有 alert 行、
没有 `/metrics` 序列。于是：
- `tether history` 里出现空洞时，**无法从外部判定**这是"当时没发生过操作"还是"审计被截断丢了"；
- 无法对"审计丢失速率"设阈值告警；
- `cluster export-incident` 也拿不到（它只读 alerts / cluster_nodes / history 流本身——而丢的正是 history 流的内容）。

`/metrics` 目前 14 个 gauge 全是 raft/JS 复制姿态（`brokermetrics/metrics.go:65-94`），
**零 counter**：没有 RPC 速率、没有失败计数、没有在线 agent 数、没有 expose 数、没有传输字节/失败数。
运维能看到"raft 健康"，看不到"产品在干活吗"。

**为什么这阻碍演进**：每加一个"接受的有界丢失"（这个系统里有好几处：R-6 截断、M5 流删、webhook 队列满、
`transfer_audit_forward.go:70` 重试放弃），当前范式是"加一个 atomic + 一行 Warn + 一个只给测试用的 getter"。
这个范式**永远不会**长出可告警的可观测性，因为终点是测试而不是 `/metrics`。

**建议**：给 `brokermetrics.Snapshot` 加 3 个 counter 字段，`metrics_wire.go` 各接一行；
`AuditPublisher` 需要一个 `LossCounts()` 聚合 getter（broker 已持有 publisher 引用，`clusterwrite.go:347`）。

**量化**：净增 ~20 行，换来"审计丢失可告警"。`estLinesRemovable: 0`。`changeRisk: low`。

---

### F6 — [medium] ctl 错误呈现层分叉成 6 个渲染器 / 2 张 hint 表，2/3 的 CLI 失败落到"未分类 70"

**证据 —— 6 个并存的渲染器**
1. `brokerErrorMessage(verb, code, errMsg)` — `error_hints.go:154`，格式 `"<verb> failed: <hint|raw> (<code>)"`，
   **带** exit class。16 处调用。
2. `transferRefusalErr(code, format, ...)` — `cmd/tether/transfer.go:863`，**带** class、**不查 hint**。
   注释（:857-862）说明了为什么绕开：
   > "that renders `<verb> failed: <msg> (<code>)`, dropping the literal `code=<X>` token that
   > **drills/61-transfer-edges greps for** (it is GREEN and must stay so)"

   ——**一个 deploy-tier drill 的 grep 模式在决定运维看到的错误格式**，代价是整个 transfer 动词族拿不到任何 hint。
3. `runFailureMessage(reason)` — `error_hints.go:215`，查**另一张表** `runFailureReasons`（:200，13 条），
   **无** exit class（返回裸 `fmt.Errorf` ⇒ 一律 70）。
4. `execFailureMessage(chunkErr)` — `error_hints.go:226`，共用第二张表，会切冒号，**无** exit class。
5. `cmd/tether/cluster.go:997` — 带 class、**不查 hint**。
6. **裸 `Code+Error` 拼接透传**：19 处（`cluster_add_drive.go` 6 处、`cluster_upgrade_drive.go` 6 处、
   `cluster_unlock.go` 2 处、`node.go:355`、`internal/cli/completion_transport.go` 3 处），
   **既无 hint 也无 class**。

**证据 —— 分类覆盖率**

| `cmd/tether` 错误构造 | 数量 |
|---|---|
| `usageErr` / `unavailErr` / `permErr` / `&ExitError{}` | **130** |
| 裸 `fmt.Errorf` + `errors.New` | **334** |

其中 **45 处是形状上明显的 usage 错误**（"is required" / "must be" / "unknown" / "invalid"）却返回裸
`fmt.Errorf`；对照组只有 20 处同形状用了 `usageErr`。举例：
- `cmd/tether/agent_config.go:33` — `"agent config refresh: pass --once (the only supported mode)"`
- `cmd/tether/agent_config.go:36` — `"agent config refresh: --session is required"`
- `cmd/tether/agent_doctor.go:35` — `"agent doctor: --session is required"`
- `cmd/tether/history.go:88` — `"--kind must be one of: call | proc | port | transfer (got %q)"`
- `cmd/tether/proxy.go:236` — `"--name is required"`

**运维侧的实际代价**：`docs/usage.md §9.13` 的健壮重试规约写着
> "把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态。"

于是一个按文档写的监控包装器遇到"忘了传 `--session`"会**无限退避重试**一个永远不会成功的调用。
`run` 动词更彻底：`runFailureMessage` 不带 class，所以**每一种 run 失败都退 70**，
包括 `argv_required`（显然是 64）。

**为什么这阻碍演进**：想统一分类，得同时改 6 个渲染器；想加一条 hint，得先判断它会经过哪个渲染器，
而这不是显式的。已经出现的后果是：功能最新、最需要提示的 transfer 族，反而是唯一完全没有 hint 的族。

**建议**：把 `runFailureReasons` 并入 `brokerCodeHints`（两张表语义相同，都是 code→一句人话）；
`transferRefusalErr` 保持 `code=<X>` 文本格式不变（drill 仍绿），只在末尾追加 hint 句；
45 处 usage 形状换 `usageErr`。

**量化**：净减 ~40 行（合表），改 45 处构造。`estLinesRemovable: 40`。`changeRisk: medium`
（改退出码会影响既有脚本——但 64 比 70 更正确，且 `docs/usage.md` 已经这样承诺）。

---

### F7 — [medium] 日志词表与 wire/event 词表是两个不相交的命名空间，运维手册的排障配方因此是空操作

**证据**
- `docs/broker-ops.md:564` 教运维跑：
  ```
  journalctl -u tether-broker | grep -E '(disk_pressure|store_error|panic)'
  ```
- 全仓生产日志消息里，`disk_pressure` 出现 **0** 次、`store_error` 出现 **0** 次
  （grep `(Logger|logger)\.(Warn|Info|Error|Debug)\("[^"]*(store_error|disk_pressure)` → 空）。
- 原因：`disk_pressure` 是一个 **sys.events kind**（`internal/broker/disk.go:76` → JetStream `events` 流，
  只能用 `tether admin events --kind disk_pressure` 读）；`store_error` 是一个 **wire 应答 code**
  （20 处 `Code: "store_error"`），只走 NATS 应答，从不入日志。

所以同一件事分散在三个互不通名的渠道：**stderr 日志**（消息前缀 `broker:` / `agent:` / `cluster:`）、
**`tether admin events`**（kind 词表）、**RPC 应答**（code 词表）。运维要排一个"存储在闹脾气"，
得同时知道这三套词，而手册把它们混成了一条 `grep`。

**为什么这阻碍演进**：每加一个 code 或 event kind，运维文档要在三处同步，且没有任何机制校验。
`docs/broker-ops.md:564` 就是同步失败的既成事实。

**建议**：`pubSysEvent`（`internal/broker/audit.go:41`）里加一行
`b.cfg.Logger.Info("broker: sys.event", "kind", kind)`，让 `grep <kind>` 与
`admin events --kind <kind>` 至少能对上同一个词。3 行。

**量化**：净增 3 行。`estLinesRemovable: 0`。`changeRisk: low`。

---

### F8 — [medium] tunnel DENY reason 用 Go error 字符串直接上线；四个 reason 只有一个是 proto 常量

**证据**
- 发送侧：`internal/tunnel/tunnel.go:377` —
  ```go
  conn.Write([]byte("DENY " + err.Error() + "\n"))
  ```
  一个任意 Go `error` 的 `.Error()` **逐字**成为 wire 上的 DENY reason。
- 产生侧：`internal/broker/expose.go:83,108` — `return fmt.Errorf("try_again")`；
  `expose.go:85,91,113,128,133,144` — `fmt.Errorf("token_unknown_or_revoked")`；
  `internal/tunnel/tunnel.go:390` — `"DENY public_port_bind_failed\n"`。
- 分类侧：`internal/tunnel/tunnel.go:86` —
  ```go
  case "public_port_bind_failed", "try_again", proto.ReasonHomeCatchingUp:
  ```
  **字符串 switch，`default: return false`（= terminal）**。
- 四个 reason 中只有 `home_catching_up` 被提升为常量（`internal/proto/messages.go:216`）。
  而紧邻的注释（`tunnel.go:88-89`）写着 "The reason string is the proto SSOT shared with the broker
  emit-side (**no duplicated literal**)" ——这句话只对该行三个分支里的一个成立。

**为什么这阻碍演进（且失败模式已经被代码自己点名）**：`internal/broker/expose.go:73-79` 的注释解释了
`try_again` 存在的理由：
> "A TRANSIENT store fault must NOT masquerade as a revocation: a reconnecting agent treats
> `token_unknown_or_revoked` as terminal (proxy off) and stops forever — **the false-online incident**"

而本仓有 **40 处** `"<code>: " + err.Error()` 的既成习惯（F2）。任何一个后来者在 `expose.go:83` 顺手写成
`fmt.Errorf("try_again: %v", err)`，`denyIsTransient` 就落到 `default` → agent 判定 terminal → proxy 永久 off，
**正是这段注释宣称要防的事故**。整条链路没有任何编译期或测试期的保护（`tunnel_reconnect_test.go:131`
测的是分类函数本身，不是 emit↔classify 的一致性）。

**建议**：四个 reason 全部提到 `internal/proto` 常量；`tunnelTokenLookup` 改为返回
`(reason string, err error)`，把 reason 从 error 文本里拿出来，`DENY` 写 reason 而不是 `err.Error()`。

**量化**：净增 ~15 行，减掉 9 处裸字面量。`estLinesRemovable: 0`。`changeRisk: low`（值不变）。
`touchesWire: true`（DENY 行的取值集合不变，只是不再由 error 文本决定）。

---

### F9 — [medium] handler prologue 复制 10 遍，每遍配一种不同的应答形状

**证据**：`(ParseCmdBy → FingerprintFromActor → session.IsActive → session.IsMember)` 这段前置在
10 个 handler 里逐字重复：`expose.go:187,395`、`exec.go:59,219,286`、`proxy.go:337`、
`run.go:51,143`、`transfer.go:976,1057`。单处约 28 行（见 `expose.go:167-195`），合计 ~280 行。
`session.IsActive` 14 处、`auth.FingerprintFromActor` 19 处。

关键不在行数，而在**每一遍都用了不同的 reply 形状**（F2 的直接成因）：expose 用 `replyExposeErr`、
exec 用拼接式 `replyExecErr`、run 用 `replyRunFailed`、transfer 用 `replyPushErr/replyPullErr/replyCommitErr`、
proxy 用 `proxyErr`、sessions 用 `replyJSON` 直塞结构体。

**为什么这阻碍演进**：这 10 段是 **wire 授权边界**（session ACL 隔离，`architecture.md` 的不变量之一）。
加一条前置检查（例如未来要加 role/scope）意味着 10 处同步修改，漏一处就是一个授权洞——
而这正是 hermetic 测试最难覆盖的类别（要给 10 个动词各写一遍否定用例）。

**建议**：抽 `func (b *Broker) authorizeCmd(msg *nats.Msg, wantVerb string) (cmdCtx, *Fail)`，
返回 `(sid, actor, nid, fp)` 或一个 `Fail`；各 handler 用自己的 reply adapter 渲染同一个 `Fail`
（配合 F2 的统一 `replyFail` 则连 adapter 都不需要）。

**量化**：可净减 ~140 行（280 → 10×6 + 一个 ~40 行 helper + 10 个 5 行 adapter）。
`changeRisk: medium`（触碰授权边界，必须逐 handler 对拍字节等价的应答）。

---

### F10 — [low] 审计模型有两套并存：typed re-derivable 四族 vs untyped best-effort sys.events

**证据**
- **typed / 可重放族**：`schema.AuditCall/AuditProc/AuditPort/AuditTransfer`（`internal/schema/audit.go:18,37,52,78`），
  经 `pubAuditCall`（38 处调用）/ `pubAuditProc` / `pubAuditPort` / `emitTransferAudit`。
- **untyped / best-effort 族**：`pubSysEvent(kind string, fields map[string]any)`（`internal/broker/audit.go:41`），
  **28 处调用、~15 个 kind、无 schema**。第二个独立生产者是 `internal/authcallout/handler.go:421` 的
  `h.emit`（`member_joined` / `pin_failed`），走 `EmitEvent` 回调而非 `pubSysEvent`。
- kind 常量化只做了一族：`internal/broker/rehome_events.go:16-20` 定义了 5 个 `ev*` 常量 + 4 个 `reason*` 常量
  （**这是全仓最好的事件词表范式**），其余 ~15 个 kind 是裸字面量，其中一个还是拼出来的
  （`topology_reconcile.go:155` `"nats_topology_"+out.Action`，消费侧 `agent/agent.go:766` 用 `HasPrefix` 反解）。

**"加一个审计事件要改几个地方"的答案取决于走哪一族**：
- sys.events：**1 处**（`pubSysEvent("new_kind", map[string]any{...})`），无 schema、无版本、无重放保证。
- 可重放族（以 `audit.transfer` 为样本追踪）：**13 个生产文件** ——
  `internal/schema/audit.go`、`internal/proto/subjects.go`、`internal/cluster/command.go`（Op 常量 + commandVersion）、
  `internal/cluster/clustermeta.go`、`internal/cluster/offline.go`、`internal/clusteroffline/offline.go`、
  `internal/xferaudit/plan.go`（Plan+Replay 新叶子包）、`internal/broker/cluster_forward.go`、
  `internal/broker/transfer_audit_forward.go`、`internal/broker/audit_publisher.go`、
  `internal/broker/transfer.go`、`internal/broker/xfer_inflight.go`、`internal/broker/broker.go`
  ——**再加 `cmd/tether/history.go` 里 3 处硬编码的 kind 列表**（:87 校验、:187 flag 帮助、:560+ 打印分支）。

两族之间**没有中间档**：想要"有 schema 但不需要重放"，或"需要重放但不想开一个新叶子包"，都无路可走。

**为什么这阻碍演进**：13+3 的成本会把作者推向 sys.events（成本 1），于是运维契约事件持续以
untyped map 增长，`docs/broker-ops.md:430` 那张 kind 清单只能靠人肉同步——它已经和代码漂移过一次
（`audit.go:36-40` 的注释记录了 DOC-12：架构文档曾列出 `rotated_pin`/`kicked`/`agent_unregistered` 三个
**没有生产者**的 kind）。

**建议**：把 `rehome_events.go` 的常量枚举范式推广到全部 sys.events kind（~15 个常量，`internal/proto`）；
`cmd/tether/history.go` 的三处 kind 列表改为遍历同一个常量切片。

**量化**：净增 ~20 行常量，减 3 处硬编码列表。`estLinesRemovable: 0`。`changeRisk: low`。

---

### F11 — [low] `xferaudit.TransferReqID` 全仓零调用者，且 `PlanTransferAudit` 的文档注释仍指向它

**证据**
- `internal/xferaudit/plan.go:30` — `func TransferReqID(transferID, kind string) string`，
  注释自称 "the **legacy** coarse key"。全仓（含所有 `_test.go` 与 `test/`）**零调用者**。
- `internal/xferaudit/plan.go:49` — `PlanTransferAudit` 的文档注释写 "ReqID = `TransferReqID(rec)`"，
  但 :65 实际调用的是 `TransferRecordReqID(rec)`——语义**不同**：
  coarse key 只 hash `(transfer_id, kind)`，record key hash 整条规范化记录，正是为了
  ":37-38 so a later legitimate transfer that reuses the same transfer_id/kind does not get dedup-skipped"。

**为什么这是债**：ReqID 是 D8a 的**幂等键**，是"leader 死后重放不重复发布"这个不变量的载荷
（`audit_publisher.go:428` `auditMsgID` 直接用它做 JetStream dedup id）。文档注释指向一个语义不同的旧函数，
会让下一个改这里的人以为 coarse key 是现行契约——按 coarse key 改会让同 `transfer_id` 的合法重传被静默吞掉。
另外这也说明默认 golangci-lint 集（无 `.golangci.yml`）的 `unused` 检不到导出标识符。

**建议**：删 `TransferReqID`（5 行），订正 :49 注释。
**量化**：`estLinesRemovable: 5`。`changeRisk: low`。

---

## 反证：做得好的地方

不写这一节这份报告就是罪状清单。以下每条都是我实读过代码后确认的。

1. **日志设施唯一且干净**。43 处 import 全是 `log/slog`，**零** stdlib `log`、**零**第三方 logger、
   **零** `*Context` 变体混用。`cmd/tether/logging.go` 只有 30 行，非法 `--log-level` 直接 `usageErr` 退 64
   而不是静默降级（:20），并且用注释钉住了"`newLogger("info", false)` 必须与旧的
   `slog.NewTextHandler(os.Stderr, nil)` 字节等价"（:10-12）。这是很多同规模项目做不到的。

2. **结构化字段命名高度稳定，运维真的能 grep**。`err` 132 处、`sid` 31、`pid` 19、`port` 18、`nid` 13，
   消息前缀 `broker:`(149) / `agent:`(95) / `clusteroffline:`(16) / `tunnel:`(12) / `cluster:`(10) /
   `authcallout:`(7) 覆盖了 92% 的日志点。字段名没有 `session_id` vs `sid` 这种双轨。

3. **`errors.Is/As` 用了 197 处，而字符串嗅探分类全仓只有 5 处**
   （`proxysub.go:224`、`cluster_add.go:154`、`error_hints.go:183`、`incident.go:81`、`doctor.go:212`）。
   对一个 68k 行的 Go 项目这是**很低**的数字——绝大多数同规模项目在这个指标上是两位数以上。

4. **28 个 sentinel 里有相当一部分是范本级的运维错误串**，把"发生了什么 + 该怎么办"都说清楚了：
   - `internal/cluster/offline.go:425` `ErrAtomicExchangeUnsupported` —— 解释了 `RENAME_EXCHANGE` 是什么、
     为什么不做就不 crash-consistent、以及"去一台数据盘是 ext4/xfs/btrfs on Linux ≥3.15 的 broker 上跑"。
   - `internal/clusteroffline/init.go:67` / `offline.go:39` —— 直接给出 `systemctl stop tether-broker` /
     `systemctl mask` 的具体命令。
   - `internal/broker/clusterdrain.go:61` `ErrStreamsNotAtTarget` —— 说明了拒绝的理由（数据尚未冗余）而不只是"拒绝"。
   - `internal/authcallout/handler.go:105,110,117` —— 三个都带 `(retriable)` 标注，把可重试性写进了错误本身。

5. **`%v` 降级不是疏漏而是刻意模式**。21 处 `%v` 里绝大多数是
   `fmt.Errorf("%w: ...: %v", sentinel, cause)`（`clusterroster/invite.go:71,78,145,152`、
   `cluster/membership_ops.go:175,179,197,215`、`cluster/datadirlock.go:95`）——
   保留 sentinel 的可 `errors.Is`，把内层原因降级为文本。这是正确的取舍，不是忘了写 `%w`。

6. **log-then-return 双重上报只有 8 处**（`tunnel.go:1113`、`expose.go:81,88,106`、
   `clusteradmin.go:257,278`、`audit_publisher.go:398`、`clusteroffline/offline.go:142`），
   而且每一处都有明确理由（日志带的上下文比返回的 error 多）。没有"每层都 log 一遍"的常见病。

7. **退出码 taxonomy 是真正为运维设计的**。`cmd/tether/exitcode.go` 只有 104 行却写清了：
   保留区间（`cluster status` 的 0..3、`exec/run` 的透传）不与分类器冲突（:19-24）；
   分类只在 main sink 一处发生（:17）；以及 `ExitError.Quiet`（:42-48）——
   `cluster doctor --json` 的失败不再往 stderr 打 prose，因为 `2>&1 | jq` 会被污染。
   这条是从一次真实的 deploy-tier drill 失败（R11 drill 52 B3/55c）学来的，注释里写明了。

8. **`internal/agent/transfer.go` 的 `pathErr` 体系是全仓最好的错误面**。typed
   `PathValidationError{Code, Msg}`（:557-567）+ 统一构造器 + 每条 Msg 都带具体路径和可执行处方：
   - `:750` `"%s: parent directory does not exist (run \`tether exec ... mkdir -p\` first)"`
   - `:763` `"%s: not under any allow_root (%v)"` —— 把 allow_roots 原样打出来
   - `:777` `"%s: not a regular file (mode=%s)"` —— 把实际 mode 打出来
   这套东西如果推广到 broker 侧，F2 就不存在了。

9. **`brokermetrics` 的"omit-don't-fabricate"立场是正确且罕见的克制**。单机模式不伪造 HA gauge
   （`metrics.go:67-69`，注释："a flat applied_index 0 reads as a stuck raft"）；
   `StreamsTarget==0` 时不发 0（:78-79，"a faked 0 would read as a degraded cluster"）；
   `/metrics` 从 panic 中恢复以免拖垮 broker 进程（:148）；`metrics_wire.go:53-56` 明确让
   **已下台的 leader 不再吐陈旧的 peer gauge**。这些都是被真实事故教出来的判断。

10. **`audit_publisher.go` 615 行里，注释密度极高且全在解释不变量而不是复述代码**。
    R-4 commit 天花板、R-6 有界丢失、R-22 "never advance past an un-ACKed publish"、
    以及 :229-238 那段 self-begetting checkpoint 的解释（"an idle leader would write a fresh raft entry
    every tick FOREVER"）——这是一段**难写对**的代码，而它把为什么这么写留在了原地。
    `publishAudit`（:375-403）对 "no stream" 错误的 ambiguous 分类（永久删除 vs 尚未 ensure）
    是这份审计里我见过最谨慎的错误分类。

11. **`cluster export-incident`（`internal/broker/incident.go`，204 行）是真的运维资产**，
    并且诚实标注了自身局限：":18-21" 明写脱敏是 best-effort、bundle 是 "low-secret" 而非 "secret-free"、
    需要人工过目才能外发。`auditScrubSubstrings`(:31-34) 把 `cmd` 也列进 denylist，
    理由写得很清楚（"an exec/run command line is the MOST likely place a secret appears in cleartext"）。

12. **`rehome_events.go`（67 行）给出了正确的事件词表范式**：kind 常量 + **closed reason enum**，
    并显式说明"raw errors go to the logger only"、payload 是手搭 allow-list 标量 map 永不 struct-splat。
    问题不在这个文件，而在它是唯一这么做的文件（F10）。

13. **`internal/xferaudit` 作为 96 行的独立叶子包是正当的**，不是包爆炸。它的存在理由写在 :6-9：
    让 `schema.AuditTransfer` 不进 `internal/cluster`，保住 L-2 分层（FSM core 不认识审计 schema）。
    这是有意识的分层决策，不是随手拆包。

---

## 本质 vs 偶然复杂度拆解

**lane 范围**：12 个专职文件 ≈ 2,900 行 + 散布在全仓的错误构造/日志语句 ≈ 2,200 物理行（1,436 `fmt.Errorf`
+ 100 `errors.New` + 348 日志调用，多数跨行）。**合计 ≈ 5,100 行**。

### 本质复杂度（问题域强加的，删不掉）—— 我估 ≈ 65%

- **wire 级机器可判别的错误码是必需的**。这是一个跨进程、跨版本、跨 NAT 的控制面，ctl 必须能在不解析英文散文的
  前提下区分"你没权限"和"leader 正在选举"。所以 `Code` 字段和一份词表**必须存在**——问题只在于它没有 SSOT，
  不在于它不该存在。同理 exit-code taxonomy：`docs/usage.md §9.13` 承诺的"哪些可重试"是监控脚本的契约，必需。
- **可重放审计的成本是真的**。`audit_publisher.go` 的 615 行里，我判断 ~75% 是本质：
  R-4 的 commit 天花板、R-6 的快照截断有界丢失、R-8 的跨重试稳定 dedup id、R-22 的 queue-not-drop、
  `ReplicaReport` 的 fail-closed（`AllAtTarget` 在未观测/空集时返回 false，:448-457，
  否则 D7 retire 会在全新集群上假绿 → 数据丢失）。这些不是"实现方式造成的"，是
  "leader 可能在任何时刻死掉且新 leader 只有 raft log"这个前提强加的。
- **两族审计并存有一半是本质的**。`sys.events`（运维广播、订阅者可离线丢消息）和
  `audit.*`（每 session 持久 history，必须重放）确实是**两种不同的耐久度契约**，
  不是同一个东西被做了两遍。
- **分类的多样性有真实来源**：`transfer` 的错误确实和 `expose` 的错误形状不同（前者有 tier A/B、
  有 in-flight ledger、有 finalize 阶段），`run`/`exec` 的失败确实发生在 agent 侧而非 broker 侧。
  hint 表分两张不是纯粹的偶然。

### 偶然复杂度（实现方式造成的，可消除）—— 我估 ≈ 35%

按可消除量排序：

| 项 | 偶然成本 | 可回收 |
|---|---|---|
| 92 个 code 无 SSOT + 两张字面量键的表（F1） | 92 处静默漂移面 | 结构改造，行数不减 |
| 7 种应答形状 / 40 处拼接 / 8 个 reply helper（F2） | ~150 行 + 不可预测的运维体验 | **~150 行** |
| 10 遍 handler prologue（F9） | ~280 行 | **~140 行** |
| 6 个渲染器 + 2 张 hint 表（F6） | 2/3 的 CLI 错误未分类 | **~40 行** |
| raft 日志被丢弃（F3） | 集群故障不可复盘 | 行数负增长（需加 ~40 行） |
| 8 个计数器只喂测试（F5） | 审计丢失不可告警 | 需加 ~20 行 |
| 级别塌缩（F4） | `--log-level` 失效 | 0 行 |
| 日志/event/code 三套词表（F7、F10） | 手册配方是空操作 | 需加 ~23 行 |
| `TransferReqID` 死代码（F11） | 5 行 + 一条误导注释 | **5 行** |

**净行数可回收 ≈ 335 行**（约占 lane 范围的 6.6%），同时需要**新增 ≈ 120 行**（常量块 + raft 日志适配器 +
metrics 接线）。**净减 ≈ 215 行。**

这个数字本身很小，而这恰恰是本 lane 的核心判断：**问题不在体量**。5,100 行支撑一个
NAT 穿透 + Raft HA + auth_callout + 文件传输 + PTY 的系统的错误/可观测层，是**偏少而不是偏多**——
参照物是 `/metrics` 只有 14 个 gauge 零 counter、`internal/cluster` 5,300 行只有 5 条日志。

真正的成本不在行数而在**漂移面**：92 个无保护的字符串耦合点、6 个互不知情的渲染器、
一处"D3 wires logging"的空头支票。这些让**每一次新增动词、新增错误码、新增审计事件**
都在重新掷骰子决定运维能不能看见——而这个项目的用户就是运维，错误串和日志**就是它的产品界面**。

**给用户原始问题的回答（限于本 lane）：不是屎山，也没有臃肿。** 这一层写得挺克制，
局部（`exitcode.go`、`agent/transfer.go` 的 `pathErr`、`brokermetrics` 的 omit-don't-fabricate、
`audit_publisher` 的不变量注释）甚至是同规模项目里的上乘。
它的病是**收口没做完**——每一轮 phase 的多专家流程都在正确地新增能力，但没有任何一轮
的职责是"把上一轮加进来的 code / event kind / 计数器接回运维能看到的地方"。
建议把这件事作为一个显式的 post-1.0 叶子增量（`error-vocabulary-ssot`），
而不是指望它在下一个功能 phase 里被顺手做掉——过去 13 个 phase 已经证明了它不会。
