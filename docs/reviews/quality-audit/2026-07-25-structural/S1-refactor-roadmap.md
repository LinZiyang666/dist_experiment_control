# S1 — 结构性重构路线图

> 综合专家 · 2026-07-25 · 输入 = L01–L12 共 12 条限定范围 lane 的完整报告
> 只读综合。未修改任何实现代码。
> 所有编号（A1/B1/C1…）是本路线图自己的编号，与 lane 内部的 F 编号无关；每项都注明来源 lane。

---

## 0. 一句话结论

**tether 不是屎山，是一个「抽象做了一半」的项目。** 12 条 lane 的 bloat 打分中位数是 4/10，死代码率 0.57%（top decile），
注释密度 29–48% 且绝大多数记录的是「为什么」而非「是什么」。真正的债不在体量，在于**同一个决策被复制到 N 处、
而漏抄第 N+1 处不会编译失败、不会测试失败，只会在现网静默做错事**。

本路线图把 12 lane 的 108 条 finding 去重成 **26 条真实结构债**，砍掉 7 条（驳回或降级），
排成 **A（15 项 / ~13 人日）→ B（8 项 / ~19 人日）→ C（3 项 / ~11 人日 + 部署面代价）** 三批，
并明确列出 **6 类不该做的重构**。

最重要的单一数字（我自己 AST 复核得出，三条 lane 都只说对了一半）：
**生产代码里 39 个 `Code:` 字符串字面量，27 个（69%）没有 exit class 归类 → 全部退 70，
而 `docs/usage.md:1542` 明写「把 69/70/75 当可重试（退避），仅 64/77 当终态」。**
也就是说 `too_large` / `self_path` / `sub_name_taken` / `pid_unknown` / `sha256_required` 这类**绝无可能自愈**的失败，
产品正在指示所有监控脚本对它们无限退避重试。这是全仓唯一一条「今天就在现网造成错误行为」的结构债。

---

## 1. 裁决方法

排序公式：**(演进阻力 × 影响面) / (工作量 × 风险)**，四项各自定义如下。

| 维度 | 打分依据 |
|---|---|
| **演进阻力 E** | 新增一个同类功能要改几处；漏改是编译错误(1) / 测试失败(2) / 运行期报错(3) / **静默做错事(5)** |
| **影响面 B** | 只影响可读性(1) / 运维可见性(2) / 可用性(3) / **正确性或安全(5)** |
| **工作量 W** | 人日（单开发者，含写测试） |
| **风险 R** | 改错的后果：可回滚(1) / 需重跑 e2e(2) / 需跑 simcluster drill(3) / **现网重装(5)** |

**我对 lane 结论的三条不信任原则**：
1. lane 报的重复次数几乎都是**调用点数**而非**决策点数**——同一件事三条 lane 报 13/10/24，实际是 ~12 个 handler。凡是数字打架的我都自己数了。
2. lane 倾向把「本 lane 范围内最大的问题」当成「项目最大的问题」。L12 给 docs 打 bloatScore 6（全场最高），但文档漂移的修复代价是全场最低——**高分不等于高优先级**。
3. lane 会把**故意的设计**读成缺陷。本次至少有两处（force-single 的 N=1 边界、xfer_inflight 的 outbox）是 lane 没读到那段设计论证。这两条我驳回了。

---

## 2. 跨 lane 去重与裁决

### 2.1 三组「同一件事被三条 lane 各报一遍」

| 合并项 | 来源 | 各 lane 的说法 | **我的复核与裁决** |
|---|---|---|---|
| **错误码没有 SSOT** | L05 F1(92 码) / L06 F1(65 码) / L09 F3(43 码) / L05 F6 | 三个数字都不一样 | AST 扫 `Code:` 的 STRING 字面量 = **39 个 distinct**。更高的数字含 `Code: <const>` 与 `"<code>: "+err` 拼接形式。**可操作的数字不是"有多少码"而是"多少码没归类"：27/39 无 exit class、22/39 无 hint、9 个有人写了 hint 却没归类。** 合并为 **A1**，L09 F3 的 transfer 专用方案被 A1 完全覆盖，不单列。 |
| **broker ingress 准入门被手抄** | L01 F2(13 处) / L05 F9(10 个 handler) / L10 F4(24 处) | 13 / 10 / 24 | 实测：`session.IsActive` 14、`IsMember` 10、`IsOwner` 5、`FingerprintFromActor` 19、`ParseCmdBy` 9。三个数字分别是"IsActive 站点""handler 数""IsActive+IsMember 调用数"，**描述的是同一个约 12 个 handler 的前导码**。合并为 **A2（零风险样例）+ B1（完整收口）**。 |
| **周期性收敛调度有两套** | L04 F1 / L11 F1 | L04 从 goroutine/泄漏门角度、L11 从可观测性角度 | 同一件事。`loopCount = 4/5` 手写常量已复核属实（`clusterwrite.go:434`，紧邻恰好 4/5 个 `go func()`，`_test.go` 里 0 命中）。合并为 **A5（loopSet + 可观测性，低风险那一半）+ B7（把 pass 移进 registry，需要执行模型改造那一半）**。 |

### 2.2 其余成对重合（合并，不再单列）

- L02 F2（nats.conf 换装管线抄 5 遍）+ L11 F6（grow cutover 重实现 render）→ **B5**。
- L07 F1（134 文件按轮次命名）+ L12 F6（154 文件）→ 实测 **141/499 = 28%**，两条 lane 只是 regex 宽窄不同 → **B6**。
- L08 F1（`AccountSigner` 死）+ L10 F3（同）→ 两条 lane 独立得出同一结论，且我复核 `grep -v _test` **零生产引用** → **A6**（这是全场少见的"两条 lane 完全一致"，可信度最高）。
- L08 F3（5 条死 ACL 授权）+ L10 F5（4 张 subject 表无对账）+ L06 F6（11 行死 ACL）→ **A7**，一条对账测试同时关掉三条 finding。
- L05 F11 + L09 F8 + L08 F9（死符号清单）→ **A4**，合并成一次扫除。

### 2.3 降级 / 驳回（本节是本路线图最重要的部分之一）

| 原 finding | 原评级 | **我的裁定** | 依据（我实际读了什么） |
|---|---|---|---|
| **L02 F1** force-single online 不做 nats.conf 脱簇「是 racknerd 事故的结构成因」 | **critical** | **降级 medium，且驳回其建议** | lane 没读到这是**故意的两阶段边界**。`cluster_operation_controller.go:1152-1166` 有一整段标题为 `WHAT IS DELIBERATELY *NOT* FIXED (and must not be)` 的论证：孤 voter 的 clustered 渲染无效，必须由运维显式 `--to-standalone` 且**活的 nats 进程证明加载了新 conf** 后 op 才能推进；drill 41 断言的正是 `op_state==NATS_ROLLED_OUT && terminal!=true` 这个形状。产品侧闭合是**真做了**：`natsconf/remedy.go` 是 remedy 文案的共享 SSOT（3 处引用）、`runReconcileToStandalone` 有 5 道 fail-closed 门、`cluster_status_nats.go:168` 有从笔记本可见的 `DATA-PLANE DEGRADED` banner。**lane 建议的"立即给 online 分支补上脱簇 + JS reset"如果照做，会在未经运维 ack 的情况下重置 data-bearing 的 JetStream store —— 静默丢 audit/history。这是本次审计里唯一一条"照做会造成数据损失"的建议。** 幸存的真债是**4 个 raft 提案非原子 + prune 失败留 ghost**，那是 L11 F5 的框架（`OpKindForceSingleFinalize`），按那个框架收进 **C1**。 |
| **L02 F6** 拆分 `internal/cluster`（5300 行 / 243 导出） | medium | **驳回（暂不做）** | lane 自己就写「这条优先级最低……对跑着现网的工具现在不划算」。76 个 import 点 + determinism 白名单复核，收益是"心智模型更清晰"，不产生任何编译期保证。等真需要在 broker 之外复用领域 op 时再说。 |
| **L10 F6** 合并「直写 + Plan 渲染」双路径 | low | **驳回（认同 lane 自己的结论）** | 单模式引 raft 会破 `internal/cluster` 的 L-2 分层约束（`test/determinism/lint_skeleton_test.go:119-146` 强制）。只采纳其**文档侧建议**：往 architecture.md 记一条"新增 auth 相关 mutator 只走 Plan 路径"，让存量 5 对自然收敛。→ 并进 **A8**。 |
| **L09 F7** `xfer_inflight.go` 双目录 outbox「是评审堆出的偶然复杂度」 | medium(risk **high**) | **驳回删除，只采纳提纯函数那半** | lane 自己标 changeRisk=high 且承认"集群模式下确实关闭了真实窗口"。一个 lane 建议动一块它自己评为高风险、且覆盖崩溃恢复路径的代码，这个自相矛盾就是不做的理由。只做 `ledgerRowDisposition(...)` 纯函数提取 + 表驱动测试（把只存在于注释里的 5 行优先级表变成可执行代码）→ 并进 **B8**。 |
| **L03 F6** 把 2987 行搬出 `package main` | medium | **只采纳 1/5（`cluster_secrets.go`）** | lane 自己说不建议搬 `cluster_add_drive.go`/`cluster_upgrade_drive.go`（1339 行、大量依赖 `cmd.OutOrStdout()` 做 halt-and-print）。剩下唯一有**产品收益**（而非"更干净"）的是 `cluster_secrets.go` 217 行——搬出去 broker 的 `StatusReport` 才能真正填 `AccountNkMatch`（现在 ACCT.NK 列硬编码 Y）。→ 并进 **B4**。 |
| **L01 F6** 把 `*Broker` 拆成 5 个包内组件 | medium | **降级为"搭车"，不单独立项** | lane 自己就写"建议搭 F1/F2/F3 的车做，不单独立项"，且它自己的反证证明危害有限（263 个方法只有 6 个碰 ≥6 字段，"改一处炸全身"不成立）。拆分的收益是导航与测试构造，不是正确性。**采纳这个自我克制**：B1/B2/B3 落地后 `Broker` 自然瘦身，不为拆而拆。 |
| **L04 F8** 换掉自建泄漏门 | low | **明确驳回"换 goleak"，采纳"练习 N≥5 次"** | lane 自己论证了自建是明智决策。真正的缺陷是 `TestTunnelServerCloseWithActiveSessionNoLeak` 只开 1 个 session 却用 ±2 绝对容差，per-session ±1 的泄漏结构上不可检测——**换 goleak 修不好这一点**。→ 并进 **B9**。 |

---

## 3. 优先级排序（26 条真实债）

打分后的完整序列。批次 A 取 E×B/(W×R) ≥ 8 且 R=1 的项；批次 B 取需要先补测试网或分步迁移的；批次 C 取碰 wire 或部署面的。

| # | 债 | 来源 | E | B | W(人日) | R | 分 | 批次 |
|---|---|---|---|---|---|---|---|---|
| A1 | 错误码无 SSOT，27/39 退 70 被指示重试 | L05F1+L06F1+L09F3 | 5 | 5 | 2.0 | 1 | 12.5 | **A** |
| A2 | `handleCapsReq` 内联了同文件上方 60 行的 `transferGate` | L10F4 | 5 | 5 | 0.25 | 1 | 100 | **A** |
| A3 | 8 个手写轮询循环；`--wait` 不响应 Ctrl-C | L03F1 | 3 | 3 | 1.5 | 1 | 6.0 | **A** |
| A4 | 死符号 + 误导性 godoc 扫除（含 `port.Revoke` 陷阱） | L08F2/F8/F9/F10+L09F8 | 3 | 4 | 1.0 | 1 | 12.0 | **A** |
| A5 | `loopCount` 手写常量 + 4 条游离循环不可观测 | L04F1+L11F1 | 5 | 3 | 1.0 | 1 | 15.0 | **A** |
| A6 | `auth.AccountSigner` 死；它要守的校验生产真没有 | L08F1+L10F3 | 2 | 4 | 0.5 | 1 | 16.0 | **A** |
| A7 | 5 条死 ACL 授权；4 张 subject 表无对账 | L08F3+L10F5+L06F6 | 4 | 5 | 1.0 | 1 | 20.0 | **A** |
| A8 | architecture.md 40% 反读数；CLAUDE.md 缺两把尺 | L12F1/F4 | 5 | 3 | 1.5 | 1 | 10.0 | **A** |
| A9 | tunnel fence 三元比较链手写 3 遍 | L04F2 | 5 | 5 | 0.5 | 1 | 50.0 | **A** |
| A10 | 传输终态处置手抄 4 遍 | L09F1 | 4 | 3 | 0.5 | 1 | 24.0 | **A** |
| A11 | `hashToken` 三份（上轮"用注释解决"后又长出第三份） | L08F6 | 3 | 5 | 0.25 | 1 | 60.0 | **A** |
| A12 | 3 个 HTTP listener 装配 + shutdown 语义已漂移 | L04F5 | 3 | 2 | 0.75 | 1 | 8.0 | **A** |
| A13 | `DrainNode` retire 分支产品不可达且无 op 机器保护 | L11F2 | 2 | 4 | 0.5 | 1 | 16.0 | **A** |
| A14 | `StatusReport` 尾部 3 段 banner 累积 | L01F7 | 3 | 2 | 0.5 | 1 | 12.0 | **A** |
| A15 | raft 日志被 `io.Discard` 吞；8 个计数器只有 1 个进 /metrics | L05F3+F4 | 2 | 4 | 1.0 | 1 | 8.0 | **A** |
| B1 | ingress 准入门收成 `admit()`（~12 handler） | L01F2+L05F9+L10F4 | 5 | 5 | 3.0 | 2 | 4.2 | **B** |
| B2 | raft 写动词 5 处散弹 + 两份 plan 闭包 | L01F1 | 5 | 5 | 3.0 | 2 | 4.2 | **B** |
| B3 | `b.cfg.DB` 一字段两语义（173 读点） | L01F3 | 5 | 5 | 2.5 | 2 | 5.0 | **B** |
| B4 | join 版本闸挂在已退役 op 上；活路径零版本检查 | L06F3 | 3 | 5 | 1.5 | 2 | 5.0 | **B** |
| B5 | nats.conf 渲染/换装抄 5 遍 | L02F2+L11F6 | 4 | 4 | 3.0 | 3 | 1.8 | **B** |
| B6 | 141/499 测试文件按审查轮次命名 | L07F1+L12F6 | 3 | 2 | 2.0 | 1 | 3.0 | **B** |
| B7 | 收敛调度两套（registry 执行模型改造） | L04F1+L11F1 | 4 | 3 | 2.0 | 2 | 3.0 | **B** |
| B8 | `catchup_deadline` 一列四语义 + outbox 优先级表提纯 | L11F3+L09F7(部分) | 5 | 5 | 1.5 | 2 | 8.3 | **B** |
| C1 | force-single 后 4 步非原子 → ghost roster | L11F5+L02F1(降级后) | 4 | 5 | 5.0 | 3 | 1.3 | **C** |
| C2 | 传输无"预算"抽象：2 GiB 上限与 5min 看门狗不推导 | L09F2 | 3 | 5 | 4.0 | 3 | 1.25 | **C** |
| C3 | topo `Action` 靠 substring 匹配，两端匹配集不一致 | L11F4 | 4 | 3 | 2.0 | 2 | 3.0 | **C** |

**未进表的（明确不做）**：拆 `internal/cluster`、合并双写路径、`cmd/tether` 大搬迁、删 outbox、换 goleak、`adminsock` union 改 typed payload、合并 40 个具名 subject builder。理由见 §7。

---

## 4. 批次 A — 低风险高收益，可立即开工

**总计 ~13 人日，净减约 500 行生产代码，零 wire 变更，零不变量变更。**
建议打包成 **3 个叶子增量**（每个走一遍 plan→实现→内审→外审）：
- **增量 A-I「错误面收口」** = A1 + A2 + A3（~3.75 人日）
- **增量 A-II「死符号与误导文本扫除」** = A4 + A6 + A7 + A8 + A11 + A13（~4.75 人日）
- **增量 A-III「结构性小收口」** = A5 + A9 + A10 + A12 + A14 + A15（~4.25 人日）

---

### ★ A1 — 错误码 exit-class SSOT + AST 守门测试（**第一项，详细到可直接开工**）

**为什么排第一**：这是全仓唯一一条**今天就在现网造成错误行为**的结构债，且修复不碰 wire 字节、不碰不变量、
有一条 ~40 行的守门测试能一次性关掉整个缺陷类。

#### 现状（我自己 AST 复核，不是转述 lane）

```
生产代码里 Code: <STRING 字面量> 的 distinct 值      = 39
  ├─ 有 exit class 归类                            = 12
  └─ 无 exit class → 落 exitInternal(70)           = 27  ← 69%
有人手写了 hint 但没有 exit class                    =  9
```

27 个未归类码全表（`comm -23 prod exitclass` 输出）：

```
actor_invalid  already_deleting  already_revoked  bucket_unknown  download_failed
frpc_failed    home_broker_restart  install_failed  io_error  not_found
object_put_failed  pid_required  pid_unknown  rejected  request_invalid
self_path      sha256_required  signal_failed  state_write_failed  subject_malformed
sub_name_invalid  sub_name_taken  sub_not_found  tier_invalid  too_large
transfer_unknown  verb_mismatch
```

对照 `docs/usage.md:1542`：**「把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态」**。
于是 `too_large`（文件超 2 GiB 硬上限）会被监控无限重试，而每次重试都要重算全文件 SHA-256——
**产品在指示运维烧 CPU 去重试一件物理上不可能成功的事**。`self_path` / `pid_unknown` / `sub_name_taken` 同理。

`cmd/tether/error_hints.go:129-138` 自己写着：*"there is no compile-time link across the two packages; the wire-stability test is the only thing standing between those two literals"* —— 团队知道这个缝，只是只给 1 个码钉了钉子。

#### 目标

1. 27 个未归类码**逐个**归到 64 / 69 / 70 / 75 / 77；
2. 加一条 AST 守门测试，让**下一个新增码如果没归类就编译不过测试**；
3. **wire 上的字符串值一个字节不改** ⇒ 现网零影响，老 ctl / 老 agent 全部照常。

#### 步骤分解（可直接照做）

**Step 1（0.25 天）— 建 code 常量块，不改任何值**
- 新建 `internal/proto/codes.go`。按 `internal/adminsock/protocol.go:126` 已有的 `Code*` 常量做法（那里已经做对了一次，照抄风格）。
- 只放**跨包共享**的码（broker 发、ctl 读）。**不要**把 broker 包内私有的码搬过去——那会制造一个新的跨包同步点，正是我们要消灭的东西。
- 判据：一个码如果同时出现在 `internal/broker/*.go` 和 `cmd/tether/error_hints.go`，它就该是 `proto` 常量。
- 此步**零行为变化**，`go build` 后二进制应该字节等价（可用 `go build -o /tmp/a && git stash && go build -o /tmp/b && cmp` 验）。

**Step 2（0.5 天）— 分类 27 个码**
逐个填表，按 `error_hints.go:74-77` 已写下的分类学（permission→77 / 自愈瞬态→75 / 需人工处置→64 / 我方 bug→70）。
我的建议分类（开工时逐条再判一次，**不要照抄**——分类是产品决策不是机械工作）：

| 码 | 建议 class | 理由 |
|---|---|---|
| `too_large` `self_path` `sha256_required` `tier_invalid` `sub_name_invalid` `sub_name_taken` `pid_required` `request_invalid` `verb_mismatch` | **64** | 纯用户输入错，重试永不成功 |
| `not_found` `sub_not_found` `pid_unknown` `transfer_unknown` `bucket_unknown` `already_revoked` `already_deleting` | **64** | 目标不存在/已终态，重试无意义 |
| `frpc_failed` `install_failed` `download_failed` `state_write_failed` | **64** | 需人工看 agent 日志 |
| `home_broker_restart` | **75** | 名字自陈是瞬态 |
| `io_error` `object_put_failed` | **75** | I/O 多为瞬态；若排查发现多为永久，改 70 |
| `actor_invalid` `subject_malformed` `json_parse`(已有) `rejected` `signal_failed` | **70** | 版本 skew / 我方 bug，70 是对的 |

**Step 3（0.5 天）— 加 AST 守门测试（本项的真正价值）**
在 `cmd/tether/` 加 `error_code_coverage_test.go`。仿 `test/determinism/lint_skeleton_test.go:262` 的 `TestNoStrayVersionLiteral` 写法
（那里已经有一个带**自检**的 AST tripwire，是全仓最好的 lint 范式，连自检都要照抄）：

```
遍历 internal/broker + internal/agent 的非测试 .go
  → go/parser 取 AST
  → 找 *ast.KeyValueExpr，Key 是 Ident "Code"
  → Value 是 *ast.BasicLit(STRING) 或引用 proto.Code* 常量
  → 收集 distinct 值集合 S
断言：S ⊆ keys(brokerCodeExitClasses) ∪ 一份带理由的显式豁免白名单
再加 TestErrorCodeCoverageSelfCheck：合成一个含未归类码的源串，断言扫描器确实抓到 1 个
  （没有这一步，扫描器退化成空断言时没人知道——L06 反证里点名的正是这个）
```

**这条测试今天就会红**，红的正是那 27 个。这是它有效的证明。

**Step 4（0.25 天）— 消掉 `"<code>: " + err.Error()` 拼接**
L05 F2 实测 40 处（`run.go` 一个文件 17 处）。这种拼接让 hint 与 exit class **同时**落空。
改成 `Code: <code>, Error: err.Error()` 两个字段。`runFailureMessage` 补上冒号切分（与 `execFailureMessage` 对齐）。

**Step 5（0.25 天）— 文档同步（按 `feedback-contract-change-sweep`）**
- `docs/usage.md §9.13` 的退出码表：现在 64 那行的例子要补上传输族。
- **全局扫**：`grep -rn "70" docs/*.md | grep -i retry` 确认没有别处还在教"70 可重试"。

**Step 6（0.25 天）— 硬闸**
`make test` + `make lint`；`make e2e` 一次。**不需要 simcluster**（不碰部署面）。

#### 验证方式

| 守什么 | 靠谁 |
|---|---|
| 已归类码不回退 | 新增的 `TestErrorCodeCoverage`（本项产出） |
| 扫描器非空洞 | 新增的 `TestErrorCodeCoverageSelfCheck`（本项产出） |
| `dataplane_not_converged` 跨包字面量稳定 | 既有 `TestDataplaneNotConvergedCodeIsWireStable` |
| CLI 表面无意外变化 | 既有 `cmd/tether/command_tree_inventory_test.go` + golden |
| transfer drill 的 `code=<X>` grep 模式 | 既有 `test/simcluster/drills/61-transfer-edges.sh`（**本项不改 `transferRefusalErr` 的输出格式，drill 不受影响**） |

#### 回滚代价

**近似为零。** 全部改动是 (a) 新增常量、(b) map 加条目、(c) 新增测试文件、(d) 拼接改字段。
wire 字节不变 ⇒ 混版本 broker/agent/ctl 完全无感。`git revert` 单个 commit 即可。
唯一的对外可观察变化是**某些命令的退出码从 70 变成 64/75**——这正是目标，且是严格改善（原状态在指示错误行为）。

---

### A2 — `handleCapsReq` 改调 `transferGate`（0.25 人日，-14 行）

**动什么**：`internal/broker/transfer.go:1029-1064`。
**怎么动**：删掉内联的 `IsActive`/`IsMember` 二连，改成 `if code := b.transferGate(sid, fp, ""); code != "" { b.replyJSON(msg, proto.CapsResp{Code: code}); return }`。
**为什么它值得单列**：`transferGate` 就在同一文件上方 60 行，且它的 doc comment（`transfer.go:965-968`）**逐字写着** *"used for finalize.req / caps.req"* ——
helper 声明了自己服务这个 handler，而这个 handler 没用它。这是 B1 的**零风险样例**：先用它证明"gate 可提取、错误码逐字不变"这条路走得通，再去动其余 11 个。
**验证**：既有 `cmd/tether/g67_caps_test.go`（230 行）+ `test/cli_e2e/transfer_test.go`。错误码字符串必须逐字相同（`error_hints.go:21,80` 按 code 映射退出码）。
**回滚**：单函数改动，revert 即可。

### A3 — `pollUntil` 收口 + `--wait` 响应 Ctrl-C（1.5 人日，-120 行）

**动什么**：`cmd/tether/cluster_join.go:169`(`waitForOp`)、`cluster_reconcile.go:120,155`、`cluster_wait.go:37,82,146`、`cluster_add_drive.go:379,496`、`cluster_lock_keeper.go:90`。
**怎么动**：抽 `pollUntil(ctx, cfg, step func() (done bool, err error)) error`，把「tick 下限 / ctx 取消 / 瞬态重试 vs fail-fast（显式参数）/ 超时→75 / 取消→75」固化成默认。
**为什么优先**：我复核了 `waitForOp`——它是 `time.Sleep(2*time.Second)` 裸循环，**从不读 `cmd.Context()`**。
而 `cmd/tether/main.go:60-70` 的注释写着 *"Audit shard 04 F1: bare Execute() gave subcommands `cmd.Context() == Background()`, so signal handling was a no-op everywhere. The signal-aware ctx is observed by every cobra command"* ——
**这是一条已修复审计发现的静默回归**：`signal.NotifyContext` 接管了 SIGINT（第一个信号只 cancel ctx、不再终止进程），
而 `cluster join approve --wait`（默认 5min）/ `cluster retire --wait`（默认 10min）不读它，于是**按几次 Ctrl-C 都退不出去，只能 SIGKILL**。修复前比修复后更难杀。
**顺带修**：`cluster reconcile nats --wait` 超时退 70，而语义相同的 `transfer-leader --wait` / `join approve --wait` 退 75。统一到 75。
**验证**：需新增——每个 `--wait` 命令一条「cancel ctx 后 ≤1 个 tick 内返回 75」的表驱动测试。`docs/usage.md §9.13` 同步。
**回滚**：中等——9 个调用点，但每处 step 函数是机械提取，revert 干净。

### A4 — 死符号与误导性 godoc 扫除（1.0 人日，-160 行）

合并 L08 F2/F8/F9/F10 + L09 F8。**按危害排序，不是按行数**：

1. **`port.Revoke` / `port.PlanRevoke`**（`internal/port/port.go:399`、`plan.go:74`，-28 行）——**这条是陷阱不是垃圾**。
   它的 godoc 声称 *"used by the broker reconciler"*，而 reconciler 实际用的是 race-safe 的 `RevokeAllocation`（整行身份谓词）。
   下一个要加 revoke 路径的人 grep 到它、照 godoc 选它，就把 port-reuse race 重新引回来——一类只在现网出现的 bug。
   删掉，`test/cluster/equiv_test.go` / `port_test.go` 改指 `RevokeAllocation`。
2. **`subhttp.LiveProxyNodes` / `subhttp.Serve`**（-13 行）——godoc 写 *"kept for back-compat"*。
   `internal/` 是 Go 的封闭世界，**"back-compat" 在这里语义上不可能成立**。删，并把这条判据写进 review checklist。
3. **`proto.RehomeDirective`** —— godoc 承诺 *"a guard test asserts it has no live publisher"`，**该测试不存在**（全仓 3 处命中全在自己的注释里）。
   要么真写那条 AST 断言，要么删掉这句假保证。**假保证比没保证危险**。
4. **`xferaudit.TransferReqID`**（-5 行）+ 订正 `plan.go:49` 指向 `TransferRecordReqID` 的文档错误（coarse key vs 内容寻址 key，是 #57 崩溃恢复设计的基石，指错会让人以为 dedup 是粗粒度的）。
5. 其余 ~25 个零引用叶子按 L08 F9 表逐条删。**三个例外不删**：`clusterupgrade.AgentUnknown`（`iota` 零值 + fail-closed 语义是载荷）、
   `ClusterGrowSchemaVersion`（不删要 **stamp**，见 B4）、`serveconf.WssListen`/`WSInternal`（`install.sh:547` 真的在写，要先决定 install.sh 还写不写）。

**验证**：`go build ./...` + `make test`；`golang.org/x/tools/cmd/deadcode` 重跑**必须带全部 8 个 build tag**（不带会把 5 个 `*ForTest` 误报成死代码——L08 的方法论注记，照做）。
**回滚**：纯删除，revert 即可。

### A5 — `loopSet` + 游离循环可观测性（1.0 人日，-10 行净，+可观测性）

**动什么**：`internal/broker/clusterwrite.go:434-447` 的 `loopCount = 4/5`；`internal/broker/runtime_introspect.go:57-69` 的 `RuntimeReport`。
**怎么动**：抽 ~45 行 `loopSet{Go(name, fn); Join(timeout)}` 自计数，`loopCount` 常量消失；把 4 条游离循环的 `lastTick/runs/lastErr` 也塞进 `RuntimeReport`。
**为什么**：我复核了——`loopCount` 是紧邻 4/5 个 `go func()` 的手写常量，`_test.go` 里 **0 命中**。
写大了每个幽灵槽让 shutdown 阻塞 10s（吵但可见）；**写小了** `clusterShutdownOrdered` 第 3 步在循环仍做 JS/raft I/O 时推进、紧接 `nc.Drain`——
精确重演 `clusterwrite.go:132-137` 自己写明的 *"publish-after-Drain 静默丢审计，a loss the leak gate cannot catch"*。
**这一半是低风险的**（纯计数机制 + 报告字段）；把 pass 移进 registry 那一半有执行模型风险，放 B7。
**验证**：新增「`loopSet.Join` 在所有循环退出后才返回」的测试 + `-race`；既有 `test/concurrency/` 泄漏门。
**回滚**：单文件，revert 即可。

### A6 — 接线 `auth.AccountSigner` 的校验（0.5 人日，-95 行或 +3 行）

**两条 lane 独立得出同一结论，可信度最高。** 我复核：`grep -v _test` 下 `AccountSigner` 零生产引用。
**动什么**：`cmd/tether/serve.go:437` `loadAuthCalloutSeeds`。
**怎么动**：`accountSeed` 过一遍 `auth.LoadAccountSigner`（丢弃 signer、只取校验），或让 `AuthCalloutConfig` 直接持 `*AccountSigner` 并让 `handler.go:427` 的 `uc.Encode` 改调 `IssueUserJWT`（后者顺带消掉 JWT 签发的第二份实现）。
**为什么**：`LoadAccountSigner` 存在的唯一理由是拒绝把 user seed 当 account seed 用（否则静默产出 wrong-kind issuer）。
这条校验**生产路径上不存在**，而 `test/p1/foundation_risk_test.go:35` **正在断言它存在**——review 时看到"有 risk test 覆盖"会以为守住了。
现网后果：`/etc/tether/account.nk` 放错 seed → broker 正常启动 → 每个客户端在 auth_callout 阶段被静默拒绝 →「服务起来了但没人能连」。
**验证**：把 `test/p1/foundation_risk_test.go` 的断言重定向到真实生产路径（`handler_seams_test.go` 已在这么做）。
**回滚**：低——唯一行为变化是"错误的 account.nk 现在 fail fast"。

### A7 — ACL 对账测试 + 删 5 条死授权（1.0 人日，-21 行）

**动什么**：`internal/auth/permissions.go:89,90,93`；新增 `internal/auth/acl_reconcile_test.go`。
**怎么动**：加一条**纯静态对账测试**——把 `auth.PermissionsFor*` 的 ctl-facing pub allow 转成通配形式，
断言每条能被 `broker.go:958` 的订阅表某条 pattern 匹配、**反向亦然**。
**为什么**：这条测试**今天就会红**，红的是那 3–5 条指向不存在订阅者的授权（`kick` / `rotate-pin` / `node.*.tag` / `version.announce` / `unregister`）。
代价有三层：安全审查面比真实协议面大；被授权方现在可以往无消费者 subject 无限 publish；
**最危险的是**——想加"版本广播"功能的人会发现 subject 和授权都已经在了，于是跳过设计直接用，**继承一个从没被有意设计过的授权**。
同时 `docs/requirements.md:193` 与 architecture.md 把 `kick`/`rotate-pin` 写成**已有能力**（实际 `members` 表全仓只有一处 DELETE、`pin_hash` 零处 UPDATE），
必须一并降级为「未实现」——**规格与 ACL 同时撒谎比诚实的缺口危险得多**。
**验证**：本项自带的对账测试；既有 `internal/auth/permissions_test.go` 的 10 条形状不变量。
**回滚**：授权模板变化体现在**新签发**的 JWT 上。历史上从没有 agent/ctl 版本 publish 过这些 subject（最后一个生产调用者在 `55b1451` 被删，早于 v0.1.0），现网不会有连接因此被拒。

### A8 — 三把尺的订正（1.5 人日，纯文档）

**动什么**：`docs/architecture.md`、`CLAUDE.md`、`docs/reviews/` 目录结构、`docs/requirements.md`。
**怎么动**（严格按性价比排序，**不做全文重写**）：
1. **30 分钟拿最大收益**：architecture.md 顶部加 status banner ——「§A–§K 是 proto v1 单 broker 历史基线；v2/集群面的绑定契约在 `distributed-broker-architecture.md`」。
   我复核：文中 **70 处 `tether.v1`** 而 `ProtoVersion = 2`；**72 处 frp** 而 `go.mod` 零 frp 依赖。
   CLAUDE.md §1/§2/§5 三处把这份文件指定为"实现尺"——**一把 40% 给反读数的尺，比没有尺更糟**。
2. `CLAUDE.md §1` 补两行地图（`distributed-broker-architecture.md` 691 行、`deploy-tier-gotchas.md` 618 行——两份被 README 和 runbook 称为绑定契约的文件，CLAUDE.md 里 grep 计数为 0）。
   删 phase 分支条款（与 `feedback_main_only_no_branches` 冲突，实际全部直接落 main）；模型约束去掉写死的版本号。
3. `docs/reviews/` 一次 `git mv` 分三层 + 一份 109 行 `INDEX.md`。引用图证明 **plan 是载荷、review 是残渣**（65 份 plan 只有 6 份孤儿；119 份 external-review 里 89 份孤儿）。
4. `requirements.md` 顶部加 5 行 banner（`ctl ` 35 处 / frp 19 处 / Relay·Controllerd 28 处全是另一个产品的名词）。**不做机械改名**——§1.3 非目标与 §2 北极星仍然有效，banner 足够。
5. 顺带把 §2.3 驳回 L10 F6 时留下的那条约束写进 architecture.md：「新增 auth 相关 mutator 只走 Plan 路径」。
6. 修 `error_hints.go:24` 的 `tether session list` → `session ls`（真实动词；实测 `session list` 打 help 退 0——**给用户一条跑了不报错也不做事的命令**），
   并扩 `command_tree_inventory_test.go` 断言 `error_hints.go` 里每个反引号命令都能被 cobra 解析到叶子。
**验证**：新增的 command-tree 静态断言；其余靠人读。
**回滚**：文档，零风险。

### A9 — `fenceSnap` 收口 tunnel 三元比较链（0.5 人日，-15 行）

**动什么**：`internal/tunnel/tunnel.go:356-358, 405, 441, 640-660`。
**怎么动**：引入 `fenceSnap{port, sess, alloc int64}` + `s.fenceSnapLocked(...)` + `s.fenceChangedLocked(...)`，三条手写 `||` 链坍缩成一次调用。
**为什么高分**：注释本身记录了 **round-2 F1 → round-5 F1 → round-6 F4 每轮外审各加一整个新维度**。
加第 4 维（例如按 nid，多 home 场景完全可能）要改 4 处，其中两条 `||` 链漏写一项**编译器不报错**，
后果是一个已被 kill 的公网 exit 在 REGISTER 竞态里复活并重新 bind 公网端口——**数据面安全洞，且正是过去三轮外审反复踩的同一个坑**。
编辑面从 4 处（2 处静默失败）降到 1 处。
**验证**：round-2/5/6 的 F1/F4 回归测试直接钉住行为；`-race` + 泄漏门。
**回滚**：单文件纯重构，revert 即可。

### A10 — `finalizeTransfer()`（0.5 人日，-45 行）

**动什么**：`internal/broker/transfer.go:429,916,948,1183` + `xfer_inflight.go:511`。
**怎么动**：抽 `func (b *Broker) finalizeTransfer(entry *transferEntry, rec schema.AuditTransfer)` 收 `emitTerminal → deleteObject → cancel → remove`。
**证据强度**：同一句注释 *"F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here."* **字节相同地出现 4 次**——
这就是上一轮修复只能靠手抄、作者只能用注释防止后来者在某一份里退回旧写法的直证。
**注意**：watchdog 那份**没有** `entry.cancel()`（正确，但代码里看不出为什么）——提取时这个差异必须变成显式参数或显式分支，不能抹掉。
**验证**：既有 transfer 族测试 + `test/cli_e2e/transfer_test.go`。
**回滚**：单文件，revert 即可。

### A11 — `internal/tokenhash` 叶子包（0.25 人日，+2 行净）

**动什么**：`internal/port/port.go:466`、`internal/tunnel/tunnel.go:1269`、`internal/proxysub/proxysub.go:204`。
**为什么这条必须做**：这是**上一轮审计"决定不修、只加注释"的失败证据**。
06-deadcode-drift F10 当时评 low、选择"加注释说明是故意的"；结果注释生效了（tunnel 那份写得很清楚），
**但一年后 `proxysub` 又独立写了第三份，而第三份的注释只写 "same scheme as port tokens"，不知道还有第二份**。
三份服务三个 bearer-token 命名空间，必须永远一致——改一份忘一份不是编译错误、不是测试失败，
而是**静默 fail-closed 全网故障**（比如加 pepper：6 台 agent 的 tunnel REGISTER 同时被拒，错误信息指向 token 分发而非 hash 实现）。
`tunnel` 要保持 dep-graph leaf 的诉求成立——`tokenhash` 本身就是零依赖 leaf。
**验证**：`hex(sha256(x))` 不变 ⇒ DB 里已有 hash 全部继续匹配；既有 token 测试。
**回滚**：纯重构，revert 即可。

### A12 — `internal/httplisten`（0.75 人日，-45 行）

**动什么**：`internal/subhttp/subhttp.go:276`、`internal/brokermetrics/metrics.go:114`、`internal/clustermanifest/manifest.go:46`、`internal/broker/broker.go:882-937`。
**怎么动**：抽 `Bind(addr, requireLoopback)` + `Serve(ctx, ln, handler, name)`，单一优雅 shutdown 语义；三个包各留已拆好的 `Handler()`。
**已有的漂移（不是理论风险）**：subhttp 与 brokermetrics 的 `ServeListener` 逐字相同（`AfterFunc` + `Shutdown(3s)` 优雅排空），
`clustermanifest` 却是 `srv.Close()` 硬关（掐断在途请求）、用 `!=` 而非 `errors.Is`、watcher goroutine 活过返回。**没人决定过，这是漂移。**
`broker.go:742` 自己就点名过这个失败模式：*"three hand-copied copies, which is how the late one gets fixed and the early one rots"*——同一诊断换个地方原样复发。
**验证**：三个包的 handler 本就 httptest 可测；`-race` + 泄漏门（clustermanifest 从硬关改优雅关是行为改进）。
**回滚**：三包 + broker.Run 三块，revert 稍大但机械。

### A13 — 删 `DrainNode` 的 retire 分支（0.5 人日，-55 行）

**动什么**：`internal/broker/clusterdrain.go:85-206` 的 retire 分支 + `ErrStreamsNotAtTarget`。
**为什么**：`cmd/tether/cluster.go:524` 把 `Retire` 硬编码成 `false`——**产品无路径可达**，唯二 `Retire:true` 调用方都是测试。
但这段代码执行 `RemoveServer` + roster 删行（**不可逆 raft 成员变更**）却**没有** `opStillLive` 的 TOCTOU 复查、没有可复制 deadline、没有 BLOCKED 逃生口、不可 resume。
它离可达只差一个手写 adminsock 请求（socket 至今接受 `retire:true`）。
更实际的伤害：同一条 retire 安全语义被写了两遍且界限机制不同（op 侧用可复制的 `catchup_deadline` 崩溃可续；同步侧用调用方传的墙钟 deadline 进程一死就没了），**只有一处有测试网**。
**注意契约变更**：adminsock 收到 `Retire:true` 应返回明确拒绝并指向 `cluster_retire`。对老版本 CLI 这是**改善**（原来是静默走无保护路径），但属跨版本行为变化，**需写 release note**。
**验证**：两个测试改指 `StartRetireOperation`；`test/d7/integration_test.go` 需带 `-tags d7_integration` 跑。
**回滚**：纯删除 + 一条拒绝分支，revert 即可。

### A14 — `bannerBuilder`（0.5 人日，-90 行）

**动什么**：`internal/broker/clusterstatus.go:127-402`（216 行代码 / 275 行 span，本包代码密度最高的单函数）。
**怎么动**：`type bannerBuilder []string`（`add(cond, text)` + `String()` 管分隔符），尾部三段追加改成三次 `bb.add`；
`:137-188` 的数据采集抽成 `readStatusSubstrate()`，`StatusReport` 退化为 substrate→assemble→health→banner→verdict 五步。
**为什么**：尾部三个 banner 追加块**各来自一个不同的审查轮次**（computeHealth / G2 #20 / External-review m2 + B5 OPS#7），
每段自带 `if rep.Banner != "" { += " " }`，其中两段是互斥 if/else 且注释写着 *"Emit ONE banner (dedup)"*——**说明它们已经撞过一次**。
加第 4 条 advisory 时必须先读懂前三条之间那个没有名字、没有测试直接覆盖"四条同时成立"的交互。
**硬约束**：banner 文案**字节必须逐条保持**（运维可见，且 `natsconf.DeClusterRemedy*` 是共享 SSOT，不要在这一步顺手改文案）。
**验证**：需新增——banner 组合的表驱动测试（这正是今天缺的）。
**回滚**：单函数，revert 即可。

### A15 — raft 日志 + 计数器接进 /metrics（1.0 人日，+60 行）

**动什么**：`internal/cluster/node.go:278`（`io.Discard`）、`internal/broker/observability.go:251`（`runObserveLoop` 的 leadership edge）、`internal/broker/metrics_wire.go:52`。
**怎么动**：(a) ~40 行 `hclog.Logger → slog` 适配器，WARN+ 转发（DEBUG 只在 `--log-level debug` 开）；
(b) 当选/下台两个 edge 各加一条 `Info("cluster: leadership acquired/lost", "term", …)`；
(c) `TruncationLossCount` / `LagExceededCount` / `DeletedStreamLossCount` 加进 `brokermetrics.Snapshot`。
**为什么**：`node.go:278` 的注释写着 *"D1: discard raft's internal chatter; D3 wires logging"* —— **D3 从未接线**（全仓搜 `LogOutput`/`hclog.` 只有这两处）。
`internal/cluster` 5,300 行只有 5 条日志。现网若重演「leadership 抖一下之后写入超时」，运维手上：
raft 的选举/心跳/快照/failed-to-contact **全部丢弃**、broker 侧转换日志**不存在**、`cluster export-incident` 打包 alerts+membership+audit **唯独没有 raft 叙事**。
**raft 是这个项目最难的一块，也是唯一一块没有运行时可观测叙事的**——每次 HA 行为改动的现网验证成本因此被直接抬高。
审计截断丢失同理：只有 stderr 一行 Warn，无计数无 metric，于是 `tether history` 出现空洞时无法从外部判定是"当时没操作"还是"审计被截断丢了"。
**不做**这条 lane 还建议的日志级别大调整（228 Warn / 23 Error）——那是 30 处判断题，收益是"以后能接 PagerDuty"，
放 §7「先写规约、后改级别」，本项只写规约不改级别。
**验证**：纯观测，不改控制流；`make test` 足够。
**回滚**：revert 即可。

---

## 5. 批次 B — 中风险，需要先补测试网或分步迁移

**总计 ~19 人日。建议每项**单独一个叶子增量**（这些都需要外审）。**

### B1 — `admit()` 收口 broker ingress 准入（3 人日，-200 行）

**动什么**：约 12 个 handler 的前导码（`expose.go:167`、`exec.go:59,219,286`、`run.go:51,143`、`proxy.go:337`、`transfer.go:976,1057`、`upgrade.go:47`、`broker.go:1320`）+ 现有的两个半成品门 `transferGate` / `proxyActiveOwnerGate`。
**怎么动**：`func (b *Broker) admit(msg *nats.Msg, spec verbSpec) (*ingress, string)` 一次性做 `ParseCmdBy → FingerprintFromActor → IsActive → member|owner → 可选 node-online`；
`verbSpec` 里两个 bool（`needOwner`、`needNodeOnline`）即可覆盖现有全部差异。
**为什么中风险而不是低**：这 12 段是 **wire 授权边界**（session ACL 隔离，architecture.md 不变量之一）。
`broker.go:1320` 的注释自陈 register 曾经**没有**这个门（*"Without this, a tombstoned session could get a fresh nodes row"*）——**历史上已经漏过一次**。
JWT 一旦签发就带 24h TTL 且无撤销列表，**这个应用层 gate 是全系统唯一的运行时吊销点**。
**前置条件**：先做 A2（零风险样例，证明路走得通）。
**硬约束**：**每个动词的错误码字符串必须逐字保持**（`error_hints.go` 按 code 映射退出码）。逐 handler 对拍字节等价的应答。
**验证**：需补——每个动词一条否定用例（非成员 / 非 owner / 会话 DELETING / 节点 OFFLINE），这正是今天最缺的一类覆盖。
**回滚**：中等——12 个 handler，但每处是机械替换 + 一行 reply 映射。建议**分 3 个 PR**（transfer 族 / exec-run 族 / expose-proxy-upgrade 族）。

### B2 — `writeVerbs` 表收口 raft 写路径（3 人日，-120 行）

**动什么**：`cluster_forward.go:507,592`、`clusterwrite.go:694,833`、17 个动词的 Verb 常量 / Payload 类型 / 领域 Plan 函数。
**怎么动**：一张 `writeVerbs{verb, decode, plan, allowReqID}` 表；`dispatchForward` 退化为 decode→plan→Propose（约 25 行），10 个路由方法改调同一个泛型 `routeWrite`。
**为什么**：新增一个动词要同时改 5 处，且**同一个 Plan 闭包被写两遍**——leader 本地一份、follower 转发后 leader 侧一份，靠人眼保持逐字语义等价。
今天已有一处**只是巧合正确**：`freePortAllocation` 传完整 `port.Allocation`，而 `dispatchForward` 只从 5 个字段重建，
能对纯因 `planAllocationStateChange` 恰好只读那 5 个字段。一旦给 `PlanFreeAllocation` 加一条 epoch fencing（本项目已在 `PlanAllocate` 做过同类事），
就变成只在「ctl 打到 follower + 该字段非零」才触发的**静默正确性回归**——leader 直连的测试全绿。
**硬约束**：**verb 字符串与 payload JSON 逐字不变**（滚动 broker 升级期间 broker↔broker apply 是跨版本 wire）。
**验证**：需补——「同一输入经 leader 本地路径与 follower 转发路径产出字节相同的 Command」的对拍测试（今天完全没有）。
**回滚**：中等，建议分批（先只收 3–4 个动词证明表结构够用）。

### B3 — `readDB` 类型消歧（2.5 人日，0 净行）

**动什么**：`broker.go:818`（唯一的运行期 `b.cfg.DB = cl.node.RODB()`）+ 173 个 `b.cfg.DB` 读点。
**怎么动**：`type readDB struct{ db *sql.DB }`（只暴露 `Query`/`QueryRow`），`b.read()` 替代读点，`b.liveness()` 语义收窄，`Config.DB` 在 `Run` 之后不再被业务代码直接读。
**为什么**：一个字段两种语义（单机可写池 / 集群只读池），三个 handle 全是 `*sql.DB`，**编译器分不出来**。
`clusterwrite.go:961` 的注释即自陈事故：*"route admin evict through raft (else the direct tx hits the RODB handle and fails)"*。
每次新增数据访问都要回答一个只写在注释里的问题——**答错的代价是运行期 readonly-database，且只在集群模式触发**，
而包内 126 处零值 `&Broker{}` 测试字面量大多走单机路径，**结构上不覆盖**。HA 之后每个叶子增量几乎都要加写路径。
改完「集群模式下误写」从运行期错误变成**编译错误**。
**为什么在 B 不在 A**：173 个调用点的机械改写规模够大，且要逐个判断该走 `read()` 还是 `liveness()`。
**验证**：编译即验证（这正是它的价值）；`make e2e` 的 D4/D5 集群矩阵是最终确认。
**回滚**：大范围但纯机械，revert 干净。

### B4 — join 版本闸 + `cluster_secrets` 下沉（1.5 人日）

**动什么**：`internal/cluster/join_bundle.go`（加 `ProtoVer`/`ReleaseVersion` omitempty 字段）、`DecodeJoinBundle`/`StartJoinOperation`（复用现成的 `versionSkewResponse`）、`cmd/tether/cluster_secrets.go` → `internal/clusterident`。
**为什么**：我复核确认——`versionSkewResponse` 只在 `handleAdd` 里被调用，而 `OpClusterAdd` 在 `adminsock/protocol.go:150` **被刻意不路由**（v0.4.2 关闭直接 AddVoter 路径）。
于是版本闸**产品不可达**，而 `b6_skew_test.go` 直接调它所以**一直绿**——测试覆盖了一条 CLI 到不了的路径。
活的 grow 路径（`OpClusterJoinApprove` → `driveJoin`）我 grep 确认 `JoinBundle` **没有任何 proto/release 字段、全路径零版本检查**。
后果：一个 proto v3 的 broker 被 approve-join 进 v2 集群，raft 复制成功（command 域独立），
但它服务的 agent 全部用 `tether.v3.*` subject 而失联，而 `cluster status` 仍显示 HEALTHY_HA。
同一团队在 `cluster upgrade` 上建了 proto+command+ops 三轴闸——**能力在，只是没接到这条线上**。
顺带把 `ClusterGrowSchemaVersion` 从"定义了但从不 stamp"改成真 stamp（A4 里标记为不删的那条）。
`cluster_secrets.go` 下沉后 broker 的 `StatusReport` 才能真正填 `AccountNkMatch`（现在 `cluster.go:475` 的 ACCT.NK 列硬编码 Y，图例原文 *"currently always Y — per-node verification not yet wired"*）。
**wire 判定**：`JoinBundle` 加 omitempty 字段是 **additive**，老 joiner 不发 ⇒ 落 `JoinerProto == 0` 分支（现有代码已有该分支，走 Warn 放行）。**不动 `ProtoVersion`，不需要重装。**
**验证**：`b6_skew_test.go` 改指新路径（这正是它该测的）；需跑 simcluster **grow drill**（`10-grow-to-3.sh`）。
**回滚**：revert 即可；additive 字段对老端无影响。

### B5 — `natsconf.Swap` / `RenderDesired` 收口（3 人日，-180 行）

合并 L02 F2 + L11 F6。**动什么**：`natsreconcile/reconcile.go:117-190`、`broker/cluster_grow_cutover.go:236-274`、`cmd/tether/cluster_natsconf.go:199,359`、`cmd/tether/cluster_offline.go:96`。
**怎么动**：从 `natsreconcile` 导出 `RenderDesired(in, own, override)`，5 个装配点只提供真正不同的意图；把 `natscluster` + `natsreconcile` 合并进 `natsconf`（否则 `SwapIntent` 会成为第四个跨包同步点）。
**已存在的分歧**：`cluster_grow_cutover.go:233-235` 的注释**声称** *"byte-identical to the one the reconciler DryRun-validated then withheld"*，
而 cutover 强制 `MonitorListen=topoMonitorListen`、reconciler 是从 live conf harvest ——**今天碰巧收敛，但没有任何测试钉住，只有一句注释**。
任何人给 `natsreconcile` 的渲染加一个字段（比如 jetstream `sync_interval`）都会静默打破它，
后果是在一台刚重启完的 broker 上执行一次**计划外的 swap + SIGHUP**。而这正是 racknerd 事故所在的那个文件。
`SynthesizeClusterListen` 被导出的唯一理由就是跨包同步（注释直说 *"Exported so the grow cutover renders the identical listen"*）——**一个函数因跨包同步而被迫导出，就是包边界画错的直证**。
**为什么 R=3**：碰部署面。**必须跑 simcluster drill**：`10-grow-to-3.sh`、`20-forcesingle-natsconf.sh`、`41-shrink-to-standalone.sh`、`43-migrate-live-data.sh`。
**先例证明可做**：`natsconf.MoveAsideJSStore` 已被提取一次给 4 个调用方共享（R16 A0，记在 `cluster_grow_cutover.go:280-282`）——同一动作在同一文件上的再一次应用，不是引入新范式。
**回滚**：中高。建议**先只做 `RenderDesired` 提取**（L11 F6 那半，1 人日），确认 drill 全绿后再做 `SwapIntent`。

### B6 — 测试文件按主题归位（2 人日，-500 行重复头）

**动什么**：实测 **141 / 499（28%）** 测试文件按开发过程事件命名。
**怎么动**：`git mv` 归位。L07 的 AST 集中度分析给出可机械改名的子集（88 个文件有 ≥50% 生产符号引用集中在单一生产文件，如 `jsstream/r16_g67_g69_external_review_test.go` 95%、`clusteroffline/r10_doctor_db_test.go` 100%）。
**最有说服力的单例**：`internal/tunnel/p13_external_review_round{2,5,6}_test.go` 是**同一不变量按审查轮次逐个 verb 复制**——
round2 发现 `CloseProxy` 漏 fence，round5 又发现 `CloseSession` 同样的洞，round6 又发现 `ForgetSession` 同样的洞。
**若 round2 就写成 `{verb, killFn}` 表，round5/round6 这两轮返工在结构上不会发生。** 合并成 `kill_fence_test.go` 一张表。
**硬约束**：**改名时逐字保留原有 doc comment**（如 `js_placement_gate_test.go:20-29` 的 FIXTURE NOTES 预判了自己会如何退化成 no-gate test——这类注释比测试本身更值钱），
每个测试函数上方留一行 `// origin: p13 external review round 6 F2` 保住溯源。
**流程侧**：往 CLAUDE.md §3 加 step 5b「测试归位」——本轮新增测试按被测单元命名，**不允许新建 `*_external_review_*_test.go`**。没有这条，沉积会继续。
**验证**：`make test` 前后测试函数总数与名称集合 diff 为空（除有意合并的）。
**回滚**：`git mv`，零风险。

### B7 — registry 执行模型 + 收敛调度合一（2 人日）

**动什么**：`reconcile_registry.go` 的 `reconcilePass`（加执行模式 inline/own-goroutine-with-timeout + per-pass state slot）；把 `driveProxyReconcile` / `driveLeaderMaintenance` 移进 registry；`leaderOnly bool` → `authority func() bool`。
**为什么不在 A**：`observePollWindow=2s` 阻塞、`waitNatsLoaded` 阻塞 30s、`pub.tick` 做阻塞 JS 发布——
**registry 现在的执行模型（串行 inline + `runMu` 全程持有 + 无 per-pass 超时）结构上装不下慢 pass**。这不是接线问题，是要改执行模型。
**最小闭合（可先只做这个）**：把 4 个游离循环的 `lastTick/runs/lastErr` 塞进 `RuntimeReport`（已在 A5 做）——
单这一项就把「op 驱动器卡住」从**不可观测**变成可观测，而 op controller 是全项目最容易出运维事故的收敛职责。
**明确不做**：把 5 条集群循环整体并进 registry 的单 goroutine（`pub.tick` 串到活性 pass 后面是真回归风险）。
**验证**：`-race` + 泄漏门；registry 的假时钟等价性测试（既有）。
**回滚**：中等。

### B8 — `OpTransitionInput` 三指针 + outbox 优先级表提纯（1.5 人日）

**动什么**：`internal/cluster/operation_ops.go` 的 `PlanClusterOpTransition`（`SetBarrier` 一个 bool 门三列 → 三个指针，nil = 不动）；`internal/broker/xfer_inflight.go` 的 5 行优先级表 → `ledgerRowDisposition(primary, outbox, live, now)` 纯函数 + 表驱动测试。
**为什么**：`catchup_deadline` 一列被**四种语义分时复用**（join catch-up / retire rehome / 拓扑收敛 / JS placement），
靠两段人工"不会重叠"论证维持，且**复用已经咬过一次**（`jsGateExpiryReserve`，作者自述 *"a BLOCKER I shipped and did not see"*）。
现有 API 形状要求 4 处手抄 `in.Barrier=op.Barrier; in.TopoTargetGen=op.TopoTargetGen` 回写，
而**漏抄 `topo_target_gen` 的失败模式是 fail-OPEN**：`topoConvergedForOp` 见 0 直接 `return true`，
即把 SERVING/RETIRED 在拓扑未收敛时宣布出去。**这个 API 形状主动邀请一个会绕过拓扑门的错误。**
改成指针后四处回抄全部消失，`ConfirmOp` 的手动清零变成传 nil。**不动 schema，纯 Go 侧。**
outbox 那半只做提纯（把只存在于注释里的 5 行表变成可执行代码 + 逐行覆盖），**不删 outbox**（见 §7）。
**验证**：需补——`ledgerRowDisposition` 的 5 组合表驱动测试；op 状态机既有测试。
**回滚**：低-中。

### B9 — 泄漏门统一 + 测试脚手架去重（3 人日，-800 行）

**动什么**：`internal/testharness` 扩成两层（现有单机原语 + 新 `testharness/cluster`）；`assertNoGoroutineLeak`×4 → 1；`openDB`×11 / `startNATS`×6 / `silentLog`×13 换成已有导出。
**怎么动**：`test/d3,d4,d5,d8,d9/setup_test.go` 的 1,712 行集群 harness 收成一份。
**顺带修一个真缺陷**：`TestTunnelServerCloseWithActiveSessionNoLeak` 只开 **1 个** tunnel session 却用 ±2 绝对容差，
而 `tunnel.go:470` 的 yamux watcher、`:502` 的 per-conn bridge 正好是 per-session ±1 的形状——**结构上不可检测**。
同文件的 `TestBrokerRepeatedRunNoGoroutineLeak` 跑 5 轮就做对了。**每个泄漏断言把被测对象练习 N≥5 次。**
**明确不做**：换 `goleak`（自建是明智决策，且换了也修不好上面这一点）。
**硬约束**：**一个 lane 一个 PR**，每个 PR 单独跑 `go test -tags dN_integration -race ./test/dN/`，不要一次性全换。
**回滚**：分 PR 后每个都可独立 revert。

---

## 6. 批次 C — 高风险 / 碰 wire 或部署面

### C1 — force-single 后半段收进 op 机器（5 人日 + drills）— **值得做，但排最后**

**动什么**：`internal/broker/force_single_online.go:227-309` 的后 4 步（marker / epoch / prune / seeds）→ 新的 `OpKindForceSingleFinalize`，由 controller 驱到终态。
**为什么值得**：`RecoverToSelfOnline` **之前**确实不能建 op 行（无 quorum 写不进 raft，这是它被排除的正当理由）；
但**之后节点已是可写单 voter**，后 4 步完全可以崩溃可续、prune 失败可重试。
现状代码自陈死路：*"a re-run to retry a failed prune is REFUSED by the dwell gate (CodeQuorumNotLost), so a LOUD fail here would be an unreachable dead-end"* ——
于是 prune 失败 = **永久 ghost roster 行**，正是现网 racknerd 那个删不掉的 `pc732` ghost 的直接来源（项目记忆里已记在案）。
**代价**：
- **不需要现网重装**（不动 `ProtoVersion`）；
- **不需要 agent 侧同步升级**（纯 broker 内部编排）；
- **需要跑 simcluster**：`20-forcesingle-natsconf.sh`、`22-forcesingle-online.sh`、`12-ghost-voter.sh`、`41-shrink-to-standalone.sh`；
- 触碰灾难恢复路径，改错的代价是"紧急恢复时多一个失败模式"。
**明确不做的部分（驳回 L02 F1 的建议）**：**不要**让 online force-single 自动做 nats.conf 脱簇 + JS reset。
`cluster_operation_controller.go:1152-1166` 有整段 `WHAT IS DELIBERATELY *NOT* FIXED (and must not be)` 论证，drill 41 断言这个形状，
且自动 reset 一个 data-bearing 的 JetStream store = **静默丢 audit/history**。运维 ack 是这里的**特性**不是缺口。
**顺带（低风险，可先单独做）**：加一个 registry pass——「某节点 `broker_draining` 已抬起但无非终态 retire op 且 phase 是 VOTER」⇒ 清 marker；
「phase 是 DRAINING 但无 active op」⇒ 记 INCONSISTENT。让 `AbortOp` 注释里那句 *"reconcile/doctor heals"* 从**承诺**变成**事实**（今天那个愈合器不存在）。

### C2 — 传输"预算"抽象（4 人日）— **只做短期那半，中期那半暂缓**

**问题**：`transferTimeoutTierB = 5m` 与 `transferMaxBytes = 2 GiB` **互不推导**。
tier-B 的 5 分钟要覆盖 ctl 上传 2 GiB + agent 下载 2 GiB，100 Mbit/s 链路上**必然超预算**；
看门狗到期后写 `failed/agent_no_responders`（**归因错误**）并 `deleteXferObject` **删掉正在传的对象**，
之后 agent 真落盘发的 `ev.transfer.complete` 因 tracker 已空被静默丢弃——**文件可能落地了而审计说 failed**。
**短期（做，2 人日，零 wire）**：看门狗改成 `max(transferTimeoutTierB, size/minThroughput)`，
并加测试**钉住关系**而非数值——同仓 `xferCrossHomeReapAge = 3 * transferTimeoutTierB` 已经这么做了（`reconcile_passes_test.go:1119`），
说明**纪律存在、只是没用在最该用的那一对上**。同时把三端 tier 常量（8 MiB / 2 GiB，各 3 副本）提到 `internal/proto`。
**中期（暂缓）**：加 `ev.transfer.<id>.progress` 让看门狗按进度续期、`agent_no_responders` 改 `no_progress`。
**代价评估**：additive subject + additive 消息，**不动 `ProtoVersion`、不需要重装**；但**需要 agent 侧升级才生效**
（老 agent 不发 progress ⇒ 落回固定预算，行为退化为今天，是安全方向）。
**判断：暂缓。** 现网 6 个 agent、文件传输不是主用途，短期那半已经把"看门狗删掉在传对象"这个最坏后果消掉了；
中期那半的收益要等到真有慢链路 agent 才兑现，届时再做。

### C3 — topo `Action` 上 wire（2 人日）— **值得做**

**问题**：「拓扑是否收敛」有 **4 个实现、3 种失败极性**，STUCK 分类靠对 `Reason` 字符串 substring 匹配，**且两端匹配集不一致**：
broker 侧 `computeHealth` 匹三个子串（含 `render`），CLI 侧 `topoCell` 只匹两个（**缺 `render`**）。
当前真实后果：render/merge 失败时 broker 侧健康裁决正确标 STUCK，**CLI 的 TOPO 列却渲染「…」（还在追赶）**，
告诉运维去等一个永远不会来的自愈；apply 失败两端都误渲染成"还在追赶"。
**怎么动**：`topoSelfReport` 加 `Action` 字段 → `proto.ClusterHealthResp` / `adminsock.ClusterNodeStatus` 加 `topo_action,omitempty` → 两个渲染器改成 switch 类型值。
**代价**：**additive omitempty，沿用 `TopoReported` 已有的"老 broker 不带此字段"惯例，不需要 `ProtoVersion` 跨版本路径、不需要重装。**
混版窗口内保留 substring 回退。**不需要 agent 侧升级**（这是 broker↔ctl 的面）。
**验证**：需补两端渲染的表驱动测试；`93-metrics-observability.sh` drill。

---

## 7. 不该做的重构

这一节比前面所有批次都重要。以下每一条**看起来都像正经重构**，实际有害或不划算。

### 7.1 不要为了"包数量好看"合并有真实隔离理由的包

L02 复核过 4 个 <250 行的小包，**全部正当，且是编译期证据而非包名幻觉**：
- `clustermanifest`(78 行，internal import = 0) 服务的是**未认证 loopback endpoint**（`manifest.go:18-33` 硬拒非 loopback）。
  塞进 broker 会让「绝不碰 DB/NATS/seed」从**包边界保证**降级为**注释保证**。
- `clusterupgrade`(171 行，internal import = 0) 是从 451 行 orchestrator 抠出的纯决策核心，`AgentPresence` 三态解决了 agentless 主机永远 `AtTarget=false` 的真 bug。
- `internal/xferaudit`(96 行) 把 `schema.AuditTransfer` 挡在 raft FSM 核心之外——**这是 L-2 分层的载体，不是包爆炸**。
- `internal/testharness`(189 行) 被 15 个测试包引用，替代方案是 15 份拷贝。

**判据**：一个小包该不该合并，看的是 `go list -f '{{.Imports}}'` 而不是行数。**import 面为 0 的叶子包几乎永远该保留。**

### 7.2 不要为了消除重复而制造错误抽象

三处具体的：
- **`internal/proc` 与 `internal/spawnsafe` 名字相邻但零重叠**（前者是 processes 表的 SQLite CRUD + raft planner，后者是 mountinfo 分类 + 死挂载探测 + 有界 spawn 窗口）。
  **没有一处调用关系、没有一个共享类型。** 不该合并也不该拆分。
- **40 个具名 subject builder 不要合并成 DSL**。具名可 grep、类型友好，合并只会更糟。L06 自己也这么判。真正要补的是**缺失的 parser**（`ParseCmdForwarded`），不是合并 builder。
- **`adminsock.Request/Response` 不要改成 per-op typed payload**。client/server 是同一个二进制，union 让 `client.go` 保持 56 行。
  符合项目的「安全实用主义：够用就好」。只做低成本部分：改掉过期的契约注释（*"Exactly one of Sessions/Nodes/Audit/Evict is populated"* 是只有 4 个 op 时代写的，今天有 30 个 op / 14 个 payload 槽）+ 删 7 个死字段。

### 7.3 不要动那些「看起来该删但注释解释了为什么不能删」的代码

本仓有一个罕见的优点：**它把"为什么这段代码看起来该删但不能删"写下来了**。至少四处：
- `cluster/fsm.go:80-89` 有 10 行 INVARIANT 注释，**禁止**给 `applied*` 结果类型加 `Error()` 方法，并记录了曾经有人加过、导致 D7 forged-sig poison-skip 路径静默失效。
- `clusterupgrade.AgentUnknown` 零引用但是 `iota` 零值且 **fail-closed 语义是载荷**——删掉会静默改变后面所有枚举值。
- `clusterroster` 的 `CanonicalRosterBytes`/`CanonicalSeedBytes` **逐字不能变**（签名字节）。
- `xfer_inflight.go` 的 outbox（见 §2.3）。

**判据：动一段代码前先 grep 它周围 30 行有没有 INVARIANT / DELIBERATELY / MUST NOT。有就先读完再决定。**

### 7.4 不要删高密度注释

全仓注释密度 29–48%，抽查下来**绝大多数不是重述代码而是记录"为什么是这个数"或"哪次事故改的"**：
`clusterwrite.go:611` 用 30 行论证 caught-up 必须在 raft 域而非 command 域度量（*"SQLite command cursor 在 LogNoop 上不前进、跨域比较结构上永不为真，这曾静默禁用了整个闸门"*）；
`serveconf.go:186-194` 的 `maxReapInterval=24h` 直接点名 racknerd 小盘填满事故。
**在一个「wire 破坏 = 现网必须重装」的生产工具里，这些注释比测试更耐久。任何重构必须整段搬运，不得当作清理删掉。**
唯一该动的是**寻址方案**：288 处 `R7a/C4-M8/G69 (#67 sub-face 4)` 这类私有索引标签换成稳定锚点（`// [inv:topo-fail-closed]`）+ 一页锚点索引，
让锚点可被 grep、可被测试名引用（`TestInvTopoFailClosed`），从而把注释↔行为钉住。**这是 A8 的可选延伸，不是删除。**

### 7.5 不要为了"体量好看"改测试与文档的总量

- **93k 测试 : 68k 生产 = 1.36:1，对这个问题域（Raft + auth_callout + 跨机 mTLS + clustered JS + 隧道 + PTY）是偏低而非偏高**（同类项目 1.5–2.5:1）。
  可净删的只有约 2,100 行（2.3%），债在**索引**不在体量。B6 是改名不是删除。
- 同理 `docs/reviews/` 的 66,844 行不是垃圾——引用图证明 **65 份 plan 只有 6 份孤儿**（92% 被 architecture.md §L / 代码注释持续引用）。
  A8 的建议是**分层 + 索引**（`git mv` + 109 行 INDEX.md），**不是删除**。

### 7.6 不要在 e2e 矩阵上做"看起来明显"的去重（至少不要贸然做）

L07 F5 是本次唯一一条我**降级到需要额外举证**的效率类建议。
我复核确认重复属实（`./internal/cluster/...` 出现在 D1/D2/D3/D4/D5 五个矩阵、`./internal/broker/...` 出现在 4 处）。
但：`-tags dN_integration` **会改变构建集合**，去重前必须逐包核对；且 `make e2e` 是提交前硬闸，
**一次错误的去重会静默降低 release gate 覆盖，而这类降低没有任何信号**。
**若要做，强制前置条件**：做一次「改前/改后跑同一 commit 的包集合 + 测试函数名集合 diff」并证明**覆盖不减**，把该证明写进 PR。
串行决策本身**必须保留**——`Makefile:22-34` + `all_phases_test.go:32-38` 记录了完整的试→测→退（D8 在 2-way 挂、D5 在 4-way 挂、GOMAXPROCS 封顶无效），是有数据的正确决策。

### 7.7 另外三条明确不做

- **不换 `goleak`**（§5 B9 已述）。
- **不做日志级别大调整**（228 Warn / 23 Error）——**先写规约后改级别**。A15 只写规约（Error = 已发生不可自愈的损失；Warn = 降级但会自愈；Info = 状态迁移；Debug = 稳态重试），
  改 30 处级别放到有人真要接 PagerDuty 那天，届时规约已经在了。
- **不做 `cmd/tether` 的大搬迁**（§2.3 已述，只搬 `cluster_secrets.go`）。

---

## 8. 怎么塞进项目的实际工作流

CLAUDE.md §2 规定「新工作一律当作新的叶子 feature 增量」，§3 规定 3 阶段 7 步。本路线图按这个形状切好：

| 增量 | 内容 | 人日 | 外审要点 | 硬闸 |
|---|---|---|---|---|
| **R1「错误面收口」** | A1 + A2 + A3 | 3.75 | 27 个码的分类是**产品决策**，外审必须逐条看 | `make test`/`e2e`/`lint` |
| **R2「死符号与误导文本」** | A4 + A6 + A7 + A8 + A11 + A13 | 4.75 | A7 会改 JWT 模板、A13 有跨版本行为变化，需 release note | 同上 |
| **R3「结构性小收口」** | A5 + A9 + A10 + A12 + A14 + A15 | 4.25 | A9 碰隧道 fence（安全面），必须 `-race` + 泄漏门 | 同上 + `-race` |
| **R4「ingress 准入」** | B1（前置 A2 已在 R1） | 3 | 授权边界，外审重点；分 3 个 PR | 同上 + 每动词否定用例 |
| **R5「raft 写路径」** | B2 + B8 | 4.5 | leader/follower 对拍测试是新增网 | 同上 |
| **R6「DB 类型消歧」** | B3 | 2.5 | 编译即验证；D4/D5 矩阵是最终确认 | 同上 |
| **R7「join 版本闸」** | B4 | 1.5 | additive wire 字段 | 同上 + grow drill |
| **R8「nats.conf 收口」** | B5（先只做 `RenderDesired`） | 1→3 | **部署面**，外审必须看 drill 输出 | 同上 + 4 个 drill |
| **R9「测试归位」** | B6 + B9 | 5 | 改名不改语义；每 lane 一个 PR | 同上 |
| **R10「收敛调度」** | B7 | 2 | 执行模型变更 | 同上 + `-race` |
| **R11「force-single finalize」** | C1 | 5 | 灾难恢复路径，最高外审强度 | 同上 + 4 个 drill |
| **R12「传输预算 + topo Action」** | C2(短期半) + C3 | 4 | additive wire | 同上 + 2 个 drill |

**三条纪律**（对应 CLAUDE.md §5 的「按需测试」）：
1. **A 批全部不需要 simcluster**。只有 B5 / B4 / C1 / C3 碰部署面。别为了 A 批的小改动去起 Docker。
2. **每个增量结束停下等外审**，不要连做（CLAUDE.md §3 的显式要求）。
3. **R1 的 A1 是唯一一条"不做会持续造成现网错误行为"的**。如果只做一件事，做 A1。

---

## 9. 附：本路线图对「是不是屎山」的定量回答

| 维度 | 数字 | 判定 |
|---|---|---|
| 生产代码 | 68,328 行 / 35 个 internal 包 | — |
| 死代码 | 392 行不可达 = **0.57%**（RTA 全程序） | **top decile** |
| 可净删 | ~700 行 = **1.0%** | 极低 |
| 注释密度 | 29–48%，抽查绝大多数记录"为什么" | **资产** |
| TODO/FIXME | 全仓 **1 处** | 极低（但部分待办藏在 `t.Skip` 里，指标略虚高） |
| 未归类错误码 | **27 / 39 = 69%** 退 70 而文档教重试 | **本次唯一在现网造成错误行为的债** |
| 按轮次命名的测试文件 | **141 / 499 = 28%** | 索引债，非体量债 |
| architecture.md 反读数 | 70 处 `tether.v1`（实际 v2）/ 72 处 frp（依赖已删） | **尺歪了** |
| 12 lane bloatScore 中位数 | **4 / 10** | 中等偏低 |
| 12 lane essentialPct 中位数 | **~78%** | 本质复杂度占主导 |

**结论：不是屎山。** 是一个在「删干净」和「写下为什么」两件事上做得比绝大多数同体量 Go 项目好、
但在「把重复的决策收敛成一个决策点」上停在半路的项目。
它的每一处债都有同一个形状——**抽象开了头就停了**（`transferGate` 只服务 2/12 个 handler、
registry 只吃下 9/15 个周期任务、`natsconf.MoveAsideJSStore` 收敛了但 `RenderDesired` 没有、
错误码给 12 个归了类剩 27 个没有）。这是好消息：**每一条都有一个已在本仓证明可行的范式可以照抄，
不需要发明任何新东西。**
