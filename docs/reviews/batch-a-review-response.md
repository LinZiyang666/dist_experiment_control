# 批次 A — 内审处置记录（step 5）

> 对应 `docs/reviews/batch-a-review.md`（综合报告）与反对派报告 `D6-adversary.md`（943 行）。
> 主进程逐条采纳/驳回。**未提交** —— 按流程要在 step 6 外审通过后才 commit。

## 0. 审查规模与结论

| 来源 | 结论 | 数量 |
|---|---|---|
| 综合报告（6 视角 × 对抗复核） | `ship-with-fixes` | 0 blocker / 13 major / 13 minor |
| 反对派（重跑，943 行） | `ship-with-fixes` | 1 blocker / 12 major |
| 独立重跑的硬边界合规视角 | `needs-rework` | 2 blocker / 5 major |

两个视角（硬边界合规、反对派）在 workflow 里因 `server_error` 死亡，均已独立重跑。
反对派原 agent 跑了 28 分 53 秒、60 次工具调用后死亡且**零产出**——因为它把结论留到最后一次性返回。
重跑时强制"边查边落盘"，这是 L08 事故的教训本应在本次 workflow 就应用到所有 lane 的地方。

## 1. 已修复（按严重度）

### blocker

**B1 · A12 反转了 loopback fail-closed 守卫（安全回归，我引入的）**

`internal/httplisten/httplisten.go` 的 `isLoopbackHost` 把空 host 判为 loopback。原 `clustermanifest` 的写法是：

```go
if host == "localhost" || host == "" { return host == "localhost" }   // 空 host → false
```

那行的形态本身就在说"空 host 不算 loopback"，我合并时读成了冗余表达式、简化成 `return true`。
后果：`sub_http_listen: ":8080"`（最自然的写法）从"启动硬失败并解释原因"变成**静默绑 0.0.0.0**，
而 `/sub` 吐 subscriber token + PSK、manifest 吐 account-signed roster，都是明文 HTTP。

我的 `policy_test.go` 抓不到它——它只做 AST 检查 bool 字面量，而回归在**行为侧**；
既有的两条行为测试又只喂 `0.0.0.0:0` / `8.8.8.8:80`，**没有一条覆盖空 host**。

修：恢复空 host 拒绝 + 新增 `internal/httplisten/bind_test.go`（5 个拒绝 case 含 `:0`/`:8080`、
3 个接受 case、以及 `requireLoopback=false` 的反向非空转验证）。

**B2 · `TestWireCodeNamespacesAgree` 从未被写出来**

`internal/proto/codes.go` 两处以现在时宣称它存在。与 A4 刚修掉的 `RehomeDirective` 假保证完全同型。
修：写出来（`cmd/tether/wire_code_namespaces_test.go`），含非空转断言。

**F-12 · A7 的"双向对账"只有一个方向**

四处宣称双向（测试 doc、`permissions.go` 两处、progress），实际只有 grant→subscriber。
修：实现反向 `TestACLSubscribersHaveGrants` + `TestACLExtractorsAreNotVacuous`。

> **差点重蹈覆辙**：第一版反向对账**通过了变异测试**（=空转）。原因是 `ctrl.by.*.>`
> 这类模板级通配授权吞掉了一切——正向对账我排除了 `.>`，反向忘了。
> 若不做变异实验，我会写出第二个"宣称守护实则不守"的闸门，正是 F-12 指控的事本身。
> 现已变异验证：baseline 绿 → 注入无授权订阅 → 精确点名失败 → 恢复回绿。

### major

| # | 问题 | 处置 |
|---|---|---|
| **M1** | A15 raft 日志桥**生产上是黑洞**：`ProductionConfig` 无 Logger 字段 ⇒ `cfg.Logger` 恒 nil ⇒ `emit` 第一行 return。而 `node.go:279` 写着 "finally wired" | 加字段 + `cutover.go:196` 传入 + `raftlog_test.go` 5 测试（此前 167 行零测试） |
| **M7/F-05** | 去重键只有 msg，raft 把 peer 身份放 args ⇒ **第二个失联 peer 被整窗吞掉**，直接损害 A15 立项目的 | 键含字符串 arg（数字排除，否则 term/index 会关掉去重）+ seen 上限 + 测试验证两 peer 都出现 |
| **M6** | `/metrics` goroutine 无同步读 `auditPub`（写在 `wireClusterLate`），真 race | `atomic.Pointer` |
| **F-01** | `loopSet.done` 在锁外初始化，而 4 个测试全串行调 `Go` ⇒ **`-race` 绿是假阴性** | 移进构造函数 + 并发回归测试 |
| **F-03** | `LastErr` 有 reader **零 writer**、`Runs` 恒为 1——报给运维的"liveness"是编造的 | 删掉，只保留真能观察到的 `StartedAt` |
| **M8** | loop 行混进 `Reconcilers` 数组，而该契约里"LastTick 不推进 = stalled" ⇒ 每次 `admin runtime --json` 与 export-incident 稳定出 4-5 条**假 stalled** | 独立 `cluster_loops` 键 + `ClusterLoopInfo` 类型（omitempty，不 bump schema） |
| **M1(Join)** | `loopSet.Join` 首个超时即 `return`，抛弃其余 loop——与旧行为和自己的 godoc 都相反 | 改每 loop 一份预算 + 回归测试 |
| **F-17/D4** | `dataplane_not_converged` 从 2 份声明变成 **3 份**（**反向恶化**，D4 承诺"更强"） | 三份收口成一份，broker 与 cmd/tether 都别名 proto SSOT |
| **F-11/M9** | `codes.go` 自称 SSOT，32 个常量生产**零引用**，改一个值编译通过、测试全绿 | doc 降级为准确描述（registry 非 SSOT）+ `TestEveryDeclaredCodeHasAProductionEmitter` |
| **M13** | A2 立的通则"`store_error` 明细只进 broker 日志"**未落地**——`transferGate` 三处直接丢弃 err，日志里根本没有 | 加 `logStore` 记录 op/sid/nid/err，让通则成真 |
| **M3/m3/m5** | 孤儿函数残留、计数 65 实为 62、`_total` 指标按 gauge 渲染 | 全部已修（见 progress） |

### 反对派的系统性结论 —— 采纳

> 一个把"删假 godoc"当核心工作项（A4，1.0 人日）的批次**新增了 7 条假 godoc**。
> 自我验收对"我刚写下的断言是否为真"是结构性盲区。

七条逐一处置：`bannerbuilder` ×2（随 A14 revert 消失）、`codes.go` "BOTH directions"（改为
说明只有单向被 GUARDED）、`raftlog.go` "identical messages"（改为"共享去重键的消息"）、
`metrics_wire.go` "单机也受益"（`b.cl==nil` 结构上到不了，改为说明为何仍这样放）、
`clusterstatus.go` "see the release note"（**该文档不存在**，改为指向已起草处）、tokenhash 一条。

**并加了元闸门** `test/determinism/promised_guard_test.go`：注释里写 `TestXxx` 则该测试必须存在。
经变异验证有效。它一装上就发现**这不是批次 A 的毛病，是仓库的系统性模式**——
**34 条**既存注释指向不存在的测试（security 测试、D5–D9 回归套件、多个生产文件）。
冻结为基线、只拦新增；排查那 34 条是独立的考古工作。

## 2. 采纳建议：整个否掉 A14

反对派建议 revert `bannerBuilder`。我逐条核实了自己当初的三条论据：

| 我写的 | 实测 |
|---|---|
| "三处 `if != "" { += " " }` dance" | HEAD 里只有 **2 处**（`:358`、`:377`） |
| "'Emit ONE banner' 注释是撞过一次的证据" | 只出现 1 次，讲的是互斥分支，不是碰撞记录 |
| 净减行数 | 实为 **净 +55 行** |

**当初的决策依据不成立**——为消除 2 处分隔符逻辑引入 55 行，这个交易不该做。
而且那两条错误论据是我**写在代码注释里**的，属于上面那 7 条假注释。已 revert。

## 3. 驳回的审查主张

综合报告已裁决掉一批，我复核认同，其中最需要点名的：

- **[伪造证据链]** deletion-safety 用 `grep 'OpStateDrainConfirmed|DRAIN_CONFIRMED'` 的"零命中"论证
  retire 移除步骤无测试——**这两个标识符全仓根本不存在**，真常量在 `operation_ops.go:27-47`；
  且 `grep OpKindRetire --include=*_test.go` 命中 6 个文件。
- **[伪造]** 声称留下孤儿函数 `d7RetireRosterRemovalLegacy`——报告写就时该符号已删除。
- **[因果错误]** "A13 删掉了 §8.1 移除顺序的唯一端到端覆盖"——被删的断言驱动的正是 A13 删掉的
  同步路径自身；删除针对已删代码的测试不可能减少幸存 operation 路径的覆盖。
- **[事实错误]** "A6 漏做 D13-2 且没有登记"——progress 有整段"未做的项（诚实登记）"。
- **[因果系统性错误]** "A1 让监控退避重试一个永不成功的失败"——A1 之前这些码同样落 70，
  前后等价；属**遗漏而非新造**。

## 4. 仍未做（登记，不掩盖）

1. **D13 第 2 步**（删 `IssueUserJWT`+`AccountPublicKey`）—— 理由见 progress，建议进批次 B。
2. **D22 release note** —— 草稿已在 progress 的 A13 段，属发版时动作（本增量未 commit）。
3. **A8 第 3 项**（`docs/reviews/` 335 文件 git mv）—— 净减 0、会淹没外审 diff，建议独立增量。
4. **34 条既存的失效测试引用** —— 已冻结进元闸门基线，排查是独立工作。
6. ~~**`unresolvedCodeSites` stale 检查**~~ —— **最终外审已修复**：禁止文件级豁免，
   每条必须是 `file:line`，并重新扫描确认该 key 仍是实时 unresolved site；移动、已解析或
   已删除都会失败。43 个既有动态点已逐 site 登记理由。
7. **retire 终态 `RETIRED` 的完整 harness**（需真实 topology reconcile）（复审 R4）。
8. ~~**满载并行下 D3/D5/D7 的 flake 根因**（复审 R5 / 并行增量 B1）~~ —— **已解决**
   （2026-07-26）：四类根因，见 `docs/reviews/parallel-flake-rootcause.md`；
   开发者修复三轮 300 单元零失败；最终外审严格 fallback 配置另跑 297 单元零失败。
   教训进 `docs/testing-standards.md` T1–T5，
   raft 计时另有门禁 `TestRaftTimingsUseProductionConstants`（变异验证通过）。

5. **`codes.go` 常量的 emitter 迁移** —— 让 SSOT 名副其实需要跨 broker/agent/spawnsafe 改 32 个码，
   风险与收益都需要单独评估；现已用测试钉住"每个常量都有真实发射点"，doc 也不再夸大。

## 5. 硬闸（修复后重跑）

- `make test` ✅ 0 FAIL
- `make lint` ✅ 0 issues
- `make e2e` ✅ **全绿**（复审后重跑：**1135.935s**，19 个矩阵，0 FAIL——15 个原矩阵
  加上 `test/e2e/parallel` 的 4 个外审反例，后者随 B5 修复进入发布闸门）
  —— 复审前那轮是 1101.166s / 15 矩阵；再前一轮记的是 1092.961s。那个数字来自 d7 raft 超时还是 300ms 的版本，
  而外审 M4 的修复把它改成了 1000ms，闸门却没有重跑。用一个跑的是旧代码的绿灯
  为新代码背书，正是本批次一直在清理的"陈述与事实不符"，只不过对象换成了闸门本身。
  已在 1000ms 版本上重跑，上面是重跑后的真实结果。
- `go test -tags d7_integration ./test/d7/` ✅
- 元闸门 `TestPromisedGuardTestsExist` ✅（变异验证有效）
