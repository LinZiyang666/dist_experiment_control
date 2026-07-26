# Fail — e2e 并行化首次外部审查

> 日期：2026-07-26
>
> 基线：`main` / `84bf03049a58f3249cf59f8b62ba9883467675a8`
>
> 范围：`test/e2e/parallel/`、`Makefile`、`test/e2e/all_phases_test.go`、
> `CLAUDE.md` 的并行测试纪律，以及 `docs/reviews/e2e-parallel-plan.md`。
>
> 说明：计划中披露的 7.9× / 漏跑 11 个 phase 仅作为审查线索；本报告的结论来自
> 当前源代码、独立反例和本轮实际运行。

## 结论

**Fail。当前实现可以继续作为实验性快速 runner，但不能与串行 `make e2e` 同权威，
也不能替换提交前串行闸门。**

修复后的 runner 确实不再丢 `TestAllPhases`：本轮实际执行了 107 个 unit，且
`TestAllPhases` 完整通过。但同一轮中三个未被外审测试修改的原有测试在 20-worker 满载下
失败，隔离复跑又全部通过；这直接反证“绑核后并行 flake 已解决”和“两个 runner 可互换”。

此外，独立测试证明 splitter 仍能静默少跑一个矩阵中的动态 command、会丢掉原始 `-run`
过滤条件，runner 的默认非 split 模式永远把成功轮次判为失败，NUMA 分配在总资源充足时
也会因节点不均衡拒跑。新增 `_test.go` 还会被 coverage self-check 当成串行 gate 顶层测试，
使 runner 无法正常自测。

## 阻断问题

### B1 — 实际满载轮次仍有三个原有 flake，不能宣称同权威

命令：

```text
go run ./test/e2e/parallel -split -shards 8 -workers 20 \
  -run '<全部 15 个原始顶层测试的精确 regexp>'
```

结果：107 units，3m28.834s，除新增外审反例外还有三处原有失败：

1. `test/d3.TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review`
   得到本地业务错误 `session does not exist`，而不是预期的 follower `not_leader`；
2. `internal/broker.TestGenericMutatingVerbStillRedirectsOnFollower`
   得到 `no leader (election in progress)`；
3. `test/d7.TestD7Matrix/FollowerStatusViewSource`
   30 秒内做了 58 次 leadership transfer attempt，仍无法让 node 0 恢复领导权，
   最后一次 AddNode 是 `leadership lost while committing log`。

随后把这三个测试并发但隔离地直接运行，全部通过（2.7–3.0s）；完整 D7 race 矩阵隔离运行
也通过（28.868s）。所以失败不是固定产品回归，而是 20-worker 全量负载才触发的 harness
不稳定。

CPU affinity 只隔离了 logical CPU；各进程仍共享调度器、内存带宽、页缓存、文件系统、
网络栈、Go 编译缓存和测试内的选举 deadline。当前证据表明这些共享面仍足以改变测试结果。

**影响**：把并行 runner 升为提交前唯一 e2e 闸门会引入可重复的非产品红灯；更危险的是，
团队可能像这次一样继续放宽测试 timeout/leader 恢复逻辑，把 runner 压力症状改写进
产品级回归 harness。

**建议**：恢复串行版的发布/提交前权威；并行版用于快速反馈。若仍要同权威，先完成多轮
flake hunting，并对 heavy matrix 做权重/并发 admission control，而不是只依赖 CPU 绑核。

### B2 — 同一矩阵“部分可解析、部分动态”时会静默漏 command

位置：`test/e2e/parallel/split.go:54-126`

`found` 是函数级 bool。只要一个顶层测试里有任意 `exec.Command` 被解析，`found=true`；
同函数内另一个动态 command 即使解析失败，也不会触发 whole-matrix fallback。

独立合成矩阵含：

```go
exec.Command("go", "test", "-race", "./first/...")
args := []string{"test", "-race", "./second/..."}
exec.Command("go", args...)
```

splitter 只产出 `./first/...`，`unparsed=[]`，第二条命令静默消失。

现有 `RemoteFS`/`ProxyTunnelReconnect` 恰好全部 command 都是动态形态，所以整体 fallback；
这不能保护未来或重构后的混合形态。

**建议**：按 command 计数；只要发现任何目标 command 不能完整解析，就丢弃该函数已生成的
partial units，并整体 fallback。保留此反例。

### B3 — splitter 丢失原始 `-run`，实际执行的不是同一个矩阵

位置：`test/e2e/parallel/split.go:129-181`

`parseGoTestArgs` 只保留 tags/race/timeout/packages，完全忽略原始 `-run`。独立测试传入
`-run TestOne|TestTwo`，生成 unit 的 `runFilter=""`。

当前树已有两个真实受影响例子：

- `TestTransferDefaultsMatrix` 串行只跑 3 个 cli_e2e 测试；并行版跑整个
  `./test/cli_e2e/...`（本轮 15.217s）；
- `TestPhaseFluidityMatrix` 串行只跑 `TestPhaseFluidityLifecycle`；并行版列出整个
  `internal/broker`，再拆成 8 个 shard（本轮每 shard 30–52s）。

这通常扩大而非缩小覆盖，但它破坏“同一个矩阵”的核心主张，制造大量重复工作与额外 flake
机会；若后续 sharding 直接覆盖 filter，还无法表达“原 filter 与 shard filter 的交集”。

**建议**：完整保存原始 `-run`，shard 时生成 `(?:original)&&(shard)` 的等价选择，或对带
`-run` 的 command 禁止拆分并整体执行。

### B4 — runner 默认非 split 模式永远把成功判为失败

位置：`test/e2e/parallel/main.go:174-190`

结果对账无条件比较 `len(results) != len(units)`。非 split 模式下 work 在 `tests`，
`units` 恒为空。

独立运行一个通过的 `TestTransferDefaultsMatrix`：

```text
[w0 node0] TestTransferDefaultsMatrix PASS
FAILURES: -1 unit(s) produced no result (scheduled 0, got 1)
exit status 1
```

命令行默认 `-split=false`，因此直接 `go run ./test/e2e/parallel` 的默认行为不可用。
Makefile 恰好总传 `-split`，没有覆盖这个公开缺陷。

**建议**：对账使用本轮实际 `workItems`，并比较 result identity multiset，而不只是数量。

### B5 — coverage self-check 混淆递归包，runner 无法正常拥有单元测试

位置：`test/e2e/parallel/main.go:99-123,289-314`

`listTests` 执行：

```text
go test -tags e2e_matrix -list .* ./test/e2e/...
```

然后只按输出行是否以 `Test` 开头收集名字，丢失 package 归属。给 parallel 包添加本轮
四个 `_test.go` 反例后，coverage self-check 把它们也当成串行 `test/e2e` gate 的顶层测试，
因此 `make e2e-parallel -dry-run` 在启动前报 4 个 uncovered test。

这解释了计划 §10 为什么“无 `_test.go`”；但一个复杂到决定发布闸门覆盖面的 parser
恰恰不能被设计成不可测试。

**建议**：只向 `./test/e2e` 这个 package 列顶层 matrix，或使用保留 package identity 的
结构化输出；parallel 自身测试必须进入 `make test`。

## 重要问题

### M1 — NUMA 分配在资源足够时仍会拒跑

位置：`test/e2e/parallel/alloc.go:49-95`

worker 数先平均分给 node，而不看每个 node 的剩余容量。模拟 node0=1 core、node1=3 cores、
workers=3，总容量 4 足够；算法却向 node0 分 2 worker 并失败：

```text
node 0 has 1 cores for 2 workers; reduce -workers
```

`excludeBusy` 还保留零 core 的 node key，使一个被完全排空的 node 更容易触发同类失败。

**建议**：按 node capacity 分配 worker，并在排空后删除 node；保留不均衡/空 node 测试。

### M2 — “50% busy CPU”并未被测量

位置：`test/e2e/parallel/topology.go:163-219`

`busyCPUs(pct, self)` 从未使用 `pct`，也没有两次采样计算利用率。它只看进程累计
`utime+stime > 1000`、当前状态 `R` 和最后运行 CPU。一个长期进程瞬时被调度到某 CPU 会被
视为“>50% busy”，短时高负载进程反而可能被漏掉。

这是 best-effort 优化，不影响覆盖；但 Makefile/CLAUDE/plan 的“自动避开外部负载”表述
过强，应实现真实采样或降级措辞。

### M3 — 文档对权威级别互相矛盾

位置：

- `CLAUDE.md:90,114`、`Makefile:24-65`、plan §8：两个 runner 同权威；
- `test/e2e/all_phases_test.go:87-89`：串行仍是 release gate，并行只是 fast path；
- `test/e2e/parallel/topology.go:27-30`：并行“不替代 make e2e”，串行仍是 release gate。

当前代码自己的 package doc 与调用方文档给出相反发布规则。结合 B1，本轮证据支持后者，
而不支持把 `make e2e-parallel` 写成唯一提交前硬闸。

### M4 — 数量自检不能证明 unit 身份等价

`len(results)==len(units)` 只能抓“少一个结果”。一个 unit 被重复、另一个被替换或 matrix
标签/参数被错误归属时，数量仍相等。B3 就是实际例子：数量 107 完全正确，命令语义已漂移。

**建议**：对 `(matrix, package, tags, race, timeout, run-filter)` 形成 canonical manifest，
串行和并行 runner 从同一结构生成命令；比较 identity set，不再从另一个 runner 的 Go
源代码反向猜命令。

## 独立测试

新增 `test/e2e/parallel/external_review_test.go`：

1. `TestExternalReviewNonSplitModeCanPass` — 当前失败；
2. `TestExternalReviewMixedCommandShapesFallBackWhole` — 当前失败；
3. `TestExternalReviewParserPreservesOriginalRunFilter` — 当前失败；
4. `TestExternalReviewAllocateHandlesUnevenNUMANodes` — 当前失败。

Batch A 复审另新增的 ACL/Raft/error-code 反例也会被并行矩阵如实捕获；这部分失败不是
runner 自身 flake。

## 命令与结果

| 命令 | 结果 |
|---|---|
| `go test -count=1 -v ./test/e2e/parallel` | FAIL，四个独立反例全红 |
| `make e2e-parallel E2EPAR_FLAGS=-dry-run` | FAIL；错误把 parallel 包的四个测试列为 serial gate uncovered |
| 15 个原始矩阵、107-unit 实跑 | FAIL，3m28.834s；9 failures，其中 6 个为 Batch A 新反例重复命中，3 个为 D3/D5/D7 原有 flake |
| 隔离并发复跑失败的 D3/D5/D7 单测 | PASS，均约 2.7–3.0s |
| `go test -race -count=1 -tags d7_integration ./test/d7/` | PASS，28.868s |
| `make lint` | PASS，0 issues |
| `make test` | FAIL，仅新增外审反例 |
| 相关包 `-count=1 -skip ExternalReview` | PASS |

未再运行 18 分钟串行全量：本轮并行 workload 中 `TestAllPhases` 已完整通过，三个并行 flake
也各自隔离转绿；当前 Fail 结论不依赖内部提供的串行绿灯。改动未触及 install/systemd/
nats.conf 或真实部署栈，无相关 simcluster drill。

## 放行条件

1. 修复并保留四个 external-review runner 反例；
2. 使用共同的结构化 matrix manifest，或证明 splitter 对完整 argv 的保真与 fallback 完备性；
3. parallel 包可正常拥有并运行单元测试；
4. 修复 capacity-aware NUMA 分配并订正 busy CPU 声明；
5. 对实际提交配置做足够多轮满载 flake hunting，D3/D5/D7 不再出现本轮失败；
6. 在达到上述条件前，文档恢复“串行发布权威、并行快速反馈”。

---

# 主进程逐条回复（step 6 处置）

> 2026-07-26。**五个 blocker 全部成立，无一驳回。** 四个外审反例保持原样未改，
> 全部由实现侧转绿。

## 结论先行：权威度回退，采纳 B1 与放行条件 6

`make e2e`（串行）恢复为**发布/提交前闸门**，`make e2e-parallel` 降为**快速反馈**。

这推翻了我上一轮写进 `CLAUDE.md`/`Makefile`/plan §8 的"两个 runner 同权威"。
**推翻它的是证据，不是立场**：满载 20-worker 下 `test/d3` 的 follower PIN 写、
`internal/broker` 的 mutating-verb 重定向、`test/d7` 的 `FollowerStatusViewSource`
三个**原有**测试失败，隔离复跑 2.7–3.0s 全绿、完整 D7 race 隔离 28.868s 全绿。

外审对机制的判断我完全接受：**CPU affinity 只隔离 logical CPU**，调度器、内存带宽、
页缓存、文件系统、网络栈、编译缓存、以及测试内部的选举 deadline 全部仍然共享。
我此前把"绑核解决了 flake"当成已证结论，实际只证明了"绑核让 D5/D8 那两个具体 case 转绿"。

外审那句警告是这轮最该记住的：*团队可能像这次一样继续放宽测试 timeout/leader 恢复逻辑，
把 runner 压力症状改写进产品级回归 harness*。d7 从 60ms→300ms→1000ms 的三次调整里，
至少前两次正是这个模式。

已改：`CLAUDE.md` 三处、`Makefile` 两处、`test/e2e/all_phases_test.go` 表格、本 plan §8。

## B2 — 混合命令形态静默漏 command · 采纳并修复

`found` 是函数级 bool，第一个命令解析成功就把整个函数标记为已覆盖。修复改为**计数**：
`targets`（目标命令数）与 `parsed`（成功解析数）相等才接受 units，否则**丢弃已生成的
partial units** 整体 fallback。占位符 argv[0]（无法判断是不是 `go test`）计入 targets 并
必然导致 fallback——不能判断时按"没解析出来"处理。

反例 `TestExternalReviewMixedCommandShapesFallBackWhole` 转绿。

## B3 — 丢失原始 `-run` · 采纳并修复

这条最严重，因为它不是少跑而是**跑了别的东西**，且我的覆盖自检结构上抓不到（数量对、
语义漂移）。`parseGoTestArgs` 现在解析并保留 `-run`；`-run` 值若是占位符则整条命令判为
未解析（触发 fallback），因为无法复现的选择不能假装成功。

`shardable()` 增加 `u.runFilter == ""`：Go 的 `-run` 只接受一个，分片 filter 会**替换**
而非**交集**原 filter，所以带原始 `-run` 的 unit 一律整体跑。

反例 `TestExternalReviewParserPreservesOriginalRunFilter` 转绿。

## B4 — 非 split 模式恒判失败 · 采纳并修复

**这是我上一轮为防"少跑报绿"加的对账自己引入的缺陷**：它比较 `len(units)`，而默认模式的
work 在 `tests` 里、`units` 恒空，于是每一次成功的默认运行都报 `-1 unit(s) produced no
result` 并 exit 1。外审那句"Makefile 恰好总传 `-split`，没有覆盖这个公开缺陷"是准确的——
我只测了自己会走的那条路径。

修复用 `scheduledNames(split, units, tests)` 取本轮真实调度项。反例
`TestExternalReviewNonSplitModeCanPass` 转绿。

## B5 — self-check 混淆递归包，runner 无法自测 · 采纳并修复

`listTests` 用 `./test/e2e/...` 递归列出，而 `go test -list` 输出**不带包归属**，于是
parallel 包自己的 `_test.go` 被当成串行 gate 的顶层测试。改为 `./test/e2e`（非递归）。

外审这句话我原样接受：**"一个复杂到决定发布闸门覆盖面的 parser 恰恰不能被设计成不可测试"**。
plan §10 把"无单元测试"写成一条限制，而真相是自检让它不可能有——限制是我自己造的，
却被记成了外部约束。现在 `test/e2e/parallel` 有 4 个测试并进 `make test`。

## M1 — NUMA 分配在资源足够时拒跑 · 采纳并修复

改为**按容量分配**：每次把 worker 给"核数/已分配 worker 数"最高的节点，且**先删除零核节点**
（`excludeBusy` 排空一个节点后会留下空 key，正是外审指出的放大路径）。
反例 `TestExternalReviewAllocateHandlesUnevenNUMANodes` 转绿。

## M2 — "50% busy" 从未被测量 · 采纳

`busyCPUs(pct, self)` 的函数体里写着 `_ = pct`——代码自己承认了。已重命名为
`runningHeavyCPUs(self)`，去掉假参数，doc 改为诚实描述（"某个占过 CPU 的进程此刻正驻留在
这个核上"，不是利用率），并注明单次 `/proc` 采样为何支撑不了那个说法。
`Makefile`/`CLAUDE.md`/plan 的"自动避开外部负载"表述同步降级。

## M3 — 文档权威级别互相矛盾 · 采纳

矛盾方向与 B1 一致：`all_phases_test.go` 和 `topology.go` 的 package doc 说串行是 release
gate，而我改过的 `CLAUDE.md`/`Makefile`/plan 说同权威。**代码里的那两处是对的**，已把文档
统一到它们那边，而不是反过来。

## M4 — 数量自检不能证明身份等价 · 采纳并修复

采纳"数量相等 ≠ 集合相等"，对账改为按 **multiset 比较 identity**：`reconcile()` 同时报告
`missing`（调度了没结果）与 `extra`（没调度却有结果，或重复）。B3 就是数量 107 完全正确而
命令语义已漂移的实例。

外审建议的完整形态（串行与并行从**同一份结构化 manifest** 生成命令，不再从对方源码反向
猜）**没有做**：那要求改 `all_phases_test.go` 的组织方式，属于产品测试面改动，超出本增量
"不碰产品代码"的边界。**登记为未做**，并且我认为它是让这套东西真正可信的正确方向——
当前实现仍然建立在"解析另一个 runner 的 Go 源码"这一根本脆弱的前提上。

## 放行条件对照

| # | 条件 | 状态 |
|---|---|---|
| 1 | 修复并保留四个 runner 反例 | ✅ 反例未改，实现侧转绿 |
| 2 | 共同结构化 manifest，或证明 splitter 保真与 fallback 完备 | ⚠️ **部分**：fallback 完备性已由 B2 修复 + 反例钉住；共同 manifest **未做**（见 M4） |
| 3 | parallel 包可正常拥有并运行单元测试 | ✅ 4 个测试进 `make test` |
| 4 | capacity-aware NUMA + 订正 busy CPU 声明 | ✅ |
| 5 | 足够多轮满载 flake hunting，D3/D5/D7 不再失败 | ❌ **未做，也不宣称做到** |
| 6 | 达标前文档恢复"串行发布权威、并行快速反馈" | ⚠️ **当时已执行，后被推翻**——见下 |

### 本轮闸门

| 闸门 | 结果 |
|---|---|
| `make lint` | ✅ 0 issues |
| `make test` | ✅ 0 FAIL（四个 runner 反例转绿，未修改反例本身） |
| `make e2e`（串行，已恢复为权威） | ✅ 1135.935s / 19 矩阵 / 0 FAIL |
| `make e2e-parallel` | ⚠️ 修复后未重跑全量——满载 flake 未解决前，它的绿灯不构成论据 |

**条件 5 后续已闭合**（2026-07-26 二次处置）：顺着外审那句"CPU affinity 只隔离了 logical CPU"
查下去，发现的**不是一个 flake，是四类互不相同的根因**——测试自编 raft 计时（17 处，系统性）、
观测分布式状态后假设它不变（d3 与 d7，其中 d7 是我前两次都修错的那个）、
资源分配不均（`TestAllPhases` 只拿到 2 核，把 0.9s 的断言拖成 15s 超时并伪装成产品缺陷）、
以及端口 TOCTOU。全部修复后三轮满载 300 单元零失败。详见
`docs/reviews/parallel-flake-rootcause.md`，四类教训进 `docs/testing-standards.md` T1–T5。

**条件 2 仍只闭合一半**（共同结构化 manifest 未做），本增量仍**不按"已放行"自认**。

**条件 6 已被用户推翻，如实记录**：该条要求"达标前恢复串行发布权威"，其前提是
并行版会引入非产品红灯。前提已不成立——三处 flake 全是**真实的 harness 缺陷**
（错误的 raft 计时、身份竞态、端口 TOCTOU），并行只是把它们压了出来。
用户据此裁定：**`make e2e-parallel` 是唯一闸门，严禁全量跑 `make e2e`**，
串行仅允许在并行报错后用于定位单个测试。

支撑这一裁定的事实：全量串行做了多年闸门，**四类缺陷一个都没抓到**；
并行版另有两道串行版没有的自检（覆盖、identity multiset）。
