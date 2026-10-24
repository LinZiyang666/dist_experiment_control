Pass（内审；stage S0 无外审，见 [stages-plan §-1](hot-update-stages-plan.md) 工作模式）

# S0 · stable-tree 内审报告与主进程处置

日期：2026-09-20。范围：S0 相对接手 index 的全部改动（plan：[hot-update-stable-tree-plan.md](hot-update-stable-tree-plan.md)）。
形式：workflow 固定 4 路审查（并发与资源释放 / 协议两端一致性 / 闸门与变异诚实性 / 文档-台账-行为一致）→ 固定 4 路对抗核验（同视角、以驳倒为目标）。
专家只读实现，新增测试全部写在仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s0-review/<lens>/` 经 `go test -overlay` 运行；原始产出（JSON）同目录。

## 1. 结论

审查者共 16 条 finding（MAJOR 2、MINOR 14；核验者 4/4 视角全部 CONFIRMED，1 条影响段 DOWNGRADE），另有核验者补报 4 条 missed。
**没有一条触及 F1/F2 修法本身的正确性**；两条 MAJOR 是同一件事（evidence 收尾行的 tree hash 指向接手基线树而非候选树）。
主进程全部处置：**采纳 18 条**（含修代码 4 处、加测试 3 个、删死字段 4 个、改文档 9 处），**2 条记入后续 stage**（不属 S0 范围，已写进 stages-plan 对应 stage 的 scope）。
处置后三硬闸 + `make gates` + 单份 `make lint` + Darwin 编译重新全绿（§4）。

## 2. finding 逐条处置

| # | 视角 | 严重度 | 核验 | 内容（摘要） | 处置 |
|---|---|---|---|---|---|
| DOC-1 / G1 / P-1 | docs / gates / protocol | **MAJOR** | CONFIRMED ×3 | evidence §11 收尾行的 `git write-tree = dc4df10c…` 是 S0 开工前的交接 index 树（主进程在 `git add -A` **之前**算的 hash 被写进"收尾"行）；同行「`git status --short` 为空」在暂存树上结构上不可能成立 | **采纳**。§11 该行加订正指针；§11.1 记录最终候选树 hash（在全部 `git add -A` 之后计算）与正确的判据 `git status --short \| grep -v '^[AMDR] '` 为空；stable-tree-plan exit #5 与 stages-plan S0 exit #5 同步改写 |
| C-1 / G2 | concurrency / gates | MINOR | CONFIRMED ×2 | `boundedError` 只钉长度不钉 UTF-8：非法 UTF-8 的 Start 失败文本让 `registry.Complete` 返回 `ErrIdentity`、`finish` 静默 return、task 永停 `starting` 并占着三份 replay 到 Close；且 `>4096` 分支在 S0 测试下是恒等式变异 | **采纳**。`boundedError` 加 `strings.ToValidUTF8`；新增 `TestRuntimeStartFailureTextIsAlwaysAcceptedAsTerminal`（invalid_utf8 / oversize 两臂）钉住两半（变异：去掉任一半 → 红） |
| G3 | gates | MINOR | CONFIRMED（影响 DOWNGRADE：S0 里分类前缀错误与流 abort 结构上不会同时出现） | `SettledError.Error()`「terminal error 首位、原文」的合同无测试 | **采纳**。新增 `TestSettledErrorLeadsWithTerminalErrorVerbatim`（三臂表驱动） |
| G4 | gates | MINOR | CONFIRMED | `outputsEnded` 的 `t.abort[i] != "" → continue` 不可达（abort 只在 setup 失败分支赋值，随后 `observe(t, err)` 已短路）——打不红的死防线 | **采纳**。删除该分支，函数注释写明为何不需要 abort 检查 |
| P-3 | protocol | MINOR | CONFIRMED | `ProcessResumeReply` 顶层 `Stream/Offset/Data/FIN` 四字段零读零写，却被 S0 的 wire 登记钉进只增账本；头注释仍按单流形状描述 | **采纳**。四个死字段在本 verb **首次发布前**删除；账本手改收缩 4 行（`wire_field_inventory.txt`，理由：从未发布、零读写；N-1 规则保护的是已发布 peer，本 verb 没有）；头注释改为双批次形状。这是本增量 commit message 必须写明的账本收缩 |
| DOC-2 | docs | MINOR | CONFIRMED | 两增量边界写「6 共享」是 S0 前数字；S0 往 ccr 冻结清单内的 `wire_field_inventory.txt` 追加后为 7；且 sha256 比对只证明 39 个文件逐字节未动，对共享文件证明不了"ccr 实现没被触碰" | **采纳**。§11.1 改记 39 未动 / 7 共享，并写明共享 7 个文件里 S0 触碰的行只有：agent.go / broker.go 零改动（共享是 ccr 与 hot-update 交接期就有的）、leak_assert_shape_test.go +3 anchor、test_function_inventory.txt 只追加、wire_field_inventory.txt 追加+收缩、architecture.md §22.3 追加、INDEX.md 追加；43 个 ccr 测试回归 rc=0 是行为层证据 |
| DOC-3 / P-2 | docs / protocol | MINOR | CONFIRMED ×2 | §22.3 写「ctl 保留真实 exit/signal」，但 `tether exec` 对任何 `*SettledError` 走 `execFailureMessage` → class 70，真实 exit/signal 只在 `execclient.Result`；usage §9.13 把 70 归可重试 | **采纳**（文档）。§22.3 改为如实：`execclient.Result` 保留，`tether exec` 在 S0 仍退 70；退出码 taxonomy 与 §9.13 的张力**先改文档再改码**，已在 stages-plan S1 scope（"exit/signal/aborted 呈现"）。不在 S0 改 exec.go（plan §0 裁决 1） |
| DOC-4 | docs | MINOR | CONFIRMED | `retireLocked` 注释「the only place a task's resources are released」与代码不符（observe closed 分支、failStart 也释放部分资源） | **采纳**。注释改为「唯一删表处、最后释放处」 |
| DOC-5 | docs | MINOR | CONFIRMED（核验者另补两处） | stages-plan S0 一节仍有被推翻的旧文本：`stream_error`、test inventory +45、「wire 只加 omitempty 字段」、exit #4「45/49/3」；stable-tree-plan §2 承诺的 `runtime_test.go:207` 注释未落地 | **采纳**。四处文本改正；`runtimeClient.accept` 加注释 |
| DOC-6 / G5 | docs / gates | MINOR | CONFIRMED ×2 | `make-test-baseline.log` 不在声明的证据目录（唯一副本在会话 scratchpad） | **采纳**。已复制进 `s0-evidence/`（内容与 §11 描述一致：rc=2，红项恰为 F1/F2 + wire + test inventory + anchor） |
| DOC-7 | docs | MINOR | CONFIRMED | INDEX.md 第 28 行空行把表格切断，S0 行追加在断裂处 | **采纳**（裁决：删空行）。删除后 26–31 行连成一张表 |
| missed（docs 核验者 #1） | docs | MINOR | — | §11 四份硬闸收据取自五份文档最后编辑**之前**的树 | **采纳**。处置后在最终树上重跑 `make gates` / `make test` / `make e2e-parallel` / `make lint`（§4），§11.1 只记这一轮 |
| missed（docs 核验者 #2） | docs | MINOR | — | §11 写「T11/T12/T13 真 runtime 端到端」，只有 T13 起真 runtime | **采纳**。§11.1 订正措辞 |
| missed（gates 核验者） | gates | MINOR | — | T8 对 closewait 变异的杀伤在 `-count=1` 下只有约 50%（select 随机取臂），而 `make test` / 矩阵都是 count=1 | **采纳**。T8 改为测试内循环 20 次（单次运行存活概率 2⁻²⁰）；变异 closewait 在 `-count=1` 下重跑 → 红 |
| missed（protocol 核验者） | protocol | MINOR | — | 未 settle 的 terminal task（ctl 在 terminal-ack 前离开：`--timeout`、Ctrl-C、崩溃）永不回收，两条输出 replay 预算保留到 `Runtime.Close`；按默认每流 8 MiB / 节点 256 MiB，约 15 个被抛弃的 task 后该 agent 上所有后续可靠 exec 都 `ErrBackpressure` | **采纳为后续 stage 项，不在 S0 修**。这是保留期/容量策略（plan §3.1「终态至少保留到租约过期」的"至少"如何封顶），属 stages-plan **S1**（ErrExpired 收敛 / 晚重试窗）与 **S2**（C-6 容量/保留期）；已写进 S1 scope_in（"abandoned-terminal 的有界保留：lease 到期 + grace 后释放 replay、registry 留 tombstone"）。S0 不新增产品能力 |
| C（concurrency 核验者补充） | concurrency | — | — | `Close` 第二段在「pending owner 恰在 ctx 到期同一瞬 conclude」时可能返回 `ctx.Err()`，资源已由 conclude 回收，只多一条 warning | 记为覆盖限制，不改 |

## 3. 审查者独立核验通过的主张（摘）

- F1/F2 原测试与交接原件逐字相同（从 `untracked-files.tar.gz` 抽出比对：runtime_failure_test.go SHA `345dc35b…`）；用交接原 `runtime.go` overlay 跑四条 → T1/T2 FAIL、T3/T4 PASS；S0 树四条 PASS。
- execsession `-race -count=10` 全绿；execclient / workload / procrequest `-race -count=3` 全绿。
- 12 条变异副本逐条 diff 与 plan §3 一致、每条 log FAIL/DATA RACE；核验者自制变异 `abortonly`（只在 setup-failure 路径漏关）→ 五轮门 fd +16 红，证明 `exerciseRuntimeSetupFailure` 轮独立有杀伤。
- 并发核验者自写 10 条交错测试（terminal-draining 下 Close(取消 ctx)、4 客户端 hammer resume/ack 对撞 Close、Exited hook 卡住 finish 与 Close 抢锁、terminal-ack 对撞 Close、adopt 后未建泵时 Close、并发 create 对撞 Close、stdin 泵阻塞在满 pipe 时 Close、setsid 逃逸后代持 stdout、双 Close 一等一取消、Close 对撞 allocate）全绿（含 GOMAXPROCS=2 -count=10）。
- 锁序 r.mu → Output.control → Output.mu → Replay.mu 单向；泵不取 r.mu；happens-before 全序成立；QF1001 两处改写逐项等价。

## 4. 处置后收据（最终候选树）

见 [evidence §11.1](hot-update-evidence.md)。全部 `git add -A` 之后（含 evidence §11.1 与其末行）的 index：`git write-tree` = `650e95abe3c067342ffebd59378b8de27fb8ce83`；本报告写入这一行之后 index 再次变化，S0 结束时的最终 hash 由下一 stage 的 plan 在其「entry」处记录。

## 5. 未覆盖 / 带入后续 stage

- `finish()` 在 `registry.Complete` 返回 `ErrFrozen`（S2 接线后）时的完整处理（plan §7.2）→ S2。
- `AllowReap` 返回 `ErrUnknown` 时 observe 自旋（adapter 违约才可达）→ S1 Started 后置带 PID 时一并处理。
- D-state child 让 SIGKILL 不生效 → 既有边界（gotcha #81 一族）。
- 逃出 pgid 的后代持 stdout 写端 → 既有边界，S5 收口。
- abandoned-terminal 的有界保留 → S1/S2（见上表）。
- 本轮未跑 simcluster drill（S0 明确不跑）；`tether exec` 退出码 taxonomy → S1。
