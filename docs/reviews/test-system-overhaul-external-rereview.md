# Pass — 测试体系革新增量外部复审

> 2026-09-01。复审以首轮外审结束时的 index 为基线；开发者对 F1–F5 与建议项的回复是
> working tree 中 15 个 tracked 文件。本文先记录外审者尚未修改代码时的独立结论。开发者版本
> 随后单独加入暂存；本报告、复审 tasklist 及外审者后续修补始终留在暂存区外。

## 最终结论

**Pass，可以放行当前“staged 开发者版本 + unstaged 外审修补”的组合树。** 开发者回复版单独复审时
仍为 Fail：首轮 F1、F5 和建议项已关闭，但第二轮独立变异又发现 R1–R3 三个 Major 假阴性。
按用户授权冻结开发者版本后，外审者已直接修复三项、补齐红灯回归并完成全量验证；没有遗留已知
Major/Minor finding。以下结论来自实现、真实调用点、独立反例和重跑收据，不采信内审“全绿”。

## 复审 Findings（均已关闭）

### R1 — Major / Closed — leadership gate 把行动循环与同名方法/helper 当成安全等待

`loopBodyActs` 只观察调用表达式，不把 assignment、send、inc/dec 等行动视为行动。因此
`for n.IsLeader() { chosen = n; break }` 会跳过 condition，正是门禁要阻止的 observe-then-act
陈旧窗口。其“安全方法”又只比较 selector 最后一个名字，任意业务对象的 `Add`、`Equal`、`After`
都会被误当成时间运算。最后，polling helper 也只比较 callee 最后一个名字；本文件一次性求值的
`WaitFor(func() bool { ... })` 与真正的 harness primitive 不可区分。

修复：`loopBodyActs` 现在把 assignment/send/inc-dec/go/defer/业务 call/非 continue branch 视为行动，
安全 package 与方法按 import identity 和小集合识别（`leader_premise_test.go:121`）。外部 polling
primitive 按完整 import path 识别；本包 helper 必须由 `verifiedPollingHelpers` 证明是无 break 的
deadline loop，或把原 predicate 转发给已经证明的 primitive（`:226`）。一次性 `WaitFor` shadow、
one-shot loop helper、业务 `Add` 与纯 assignment 四个反例均已钉入 self-check。

### R2 — Major / Closed — sleep barrier gate 把任意普通调用当作 readiness observation

`loopObserves` 遇到任何非 timer call 就返回 true。因此一次性循环中的 `doWork(); time.Sleep(...)`
被当作 polling 而整个 sleep 被跳过；`t.Log`、格式化和任意副作用调用都有同样效果。调用本身不证明
“重复观测被测条件”，这使修复后的门禁仍可一行绕过。另一个保守性疑问是 `RangeStmt` 完全忽略
range source，channel-driven wait 与固定 slice loop 无法区分，可能产生假阳性。

修复：`loopObserves` 只接受 loop/range source、if/switch init+condition、receive 与非 timer select
这些控制流证据（`sleep_barrier_test.go:192`）；普通 `doWork()` 不再豁免固定 sleep。纯
`time.Now().Before(deadline)` 也不算 readiness probe；赋值所得的真实 probe 结果只有进入后续条件
才算证据。range source 会被检查，`readyEvents()` 形状不再误报。timer-only select 报告实际
receive 位置（`:333`），使 `sleep-fixture` 的 same-line 契约真实生效。真假两侧均有回归。

### R3 — Major / Closed — E2E constructor alias 只追踪一层，逃逸可静默少跑

splitter 只收集直接赋值自 `exec.Command`/`CommandContext` 的标识符。`command2 := command` 后调用
`command2(...)` 不计入 target；把 `command` 传参、return 或放入复合字面量也不会设置 escape。
如果同一 Test 另有一条可解析的直接命令，`parsed == targets` 仍成立，whole fallback 不会触发。
dot-import 的裸 `Command(...)` 也不可见，唯一入口门 `forksGoTest` 具有同类的一层 alias 盲区。

修复：splitter 对 Command/CommandContext、import alias 与 dot import 建立构造器身份，别名传播到
不动点（`split.go:108-168`）；传参/return/复合字面量/无法对齐赋值都设置 escape，且 escape 时
文件内每个 Test whole fallback（`:169-213,266-271`）。`forksGoTest` 使用相同保守策略
（`inventory_test.go:426-544`）。transitive alias 与 escaped alias 的最小矩阵先稳定复现 partial
parse，修复后均得到 `units=[]`、`unparsed=[TestHelperMatrix]`；inventory 的 transitive/escape/dot
三形状也全部转绿。

## 已关闭项与复核记录

- F1：release predicate 已覆盖 `always()`、`failure()`、`cancelled()`、`!cancelled()` 与
  `continue-on-error`，并保留普通 tag 条件正控。
- F2/F3/F4：开发者关闭了首轮原始反例；第二轮 R1/R2/R3 已由外审者进一步修复并纳入回归。
- F5：CI 注释已移除易漂移的硬编码 unit 数。
- 建议项：`WithLeader` 契约、`-v` identity、closure stdout hash、B5 cache 分类及 port name/eol
  exact compare 均有对应实现，未发现新的阻断问题。
- 首轮四组外审回归原样运行通过：`test/architecture`、`test/determinism`、`test/e2e/parallel`。

## 验证收据

- 修补前红灯：leader 三个新增形状漏报；sleep 同时漏报 `doWork()+sleep`、误报 range source；
  splitter 两种矩阵仍返回 partial units；inventory transitive/escape/dot 三形状均为 false。
- targeted：修复后的四组 self-check 普通运行通过；`-race -shuffle=on -count=3` 覆盖
  architecture/determinism/parallel 并全部通过。
- `make gates`：tag vet、darwin cross-build、机械门禁、hermetic simcluster 全脚本、固定版本 lint
  全部通过。沙箱内首次因禁止本地 NATS listen 出现同源 panic；原命令在允许监听环境复跑通过。
- `make test`：全树通过；最慢的 `internal/broker` 实跑约 344.8s，非空跑/秒绿。
- `make e2e-parallel`：coverage self-check `17/17`，67 units，`ALL PASS`，wall clock 3m47.712s。
- 最终增量 `gofmt`、`git diff --check`、全量 `test/determinism` 与 `make lint`（0 issues）通过；
  最后加入的 deadline-only 反例另以 `-race -shuffle=on -count=3` 复跑通过。

## 疑惑与建议（不阻断）

1. 这些扫描器承担的是防止未来回归的架构门禁，宁可要求少量显式账本，也不应把“看起来像”等价于证明。
2. 裸 identifier 的 range source 仍无法只靠 AST 区分 channel 与 collection；当前保守报告并允许
   ledger 是正确取舍。如未来误报显著，再引入 `go/types`，不建议用变量名猜测。
3. live simcluster 不能提高本轮 AST 分类器修复的证据质量；最终仍需运行已纳管的 hermetic
   simcluster suite，但没有理由操作共享 live cluster。

## 责任边界与交付状态

开发者回复涉及的 15 个 tracked 文件已在任何外审代码修改前通过 `git add -u` 写入 index；index
还保留首轮外审结束时的完整受审树。外审者后续只修改五个扫描/回归文件，并新建本报告与 tasklist：
五个代码文件的 porcelain 第二列均为 `M`（整体呈 `AM`/`MM`），两个文档为 `??`，全部仍在
暂存区外。复审结束时没有再执行 `git add`，因此开发者与外审者责任边界可由 cached/unstaged
diff 独立复核。
