# L07 — 9.3 万行测试的真实结构与有效性

> 横切结构审计 lane 7 / lane key `test-suite-organization`
> 日期：2026-07-25 · 范围：全仓 `*_test.go`（93,084 行 / 499 文件）+ `test/` + `Makefile` 测试目标 + `.github/workflows/ci.yml`
> 定位：**不是找 bug**（那是 `quality-audit/01..06` 的活）。找的是冗余、重复、组织混乱、演进阻力。

---

## 结论

**净判断：这 9.3 万行测试不臃肿，但严重误索引（mis-indexed）。断言是真的、对抗性是真的、体量是正当的；坏的是"哪条测试守着哪个不变量"这件事在文件系统层面几乎不可查——134 个文件（18,228 行 / 19.6%）是按*开发过程事件*（审查轮次、gap 编号、phase 号）命名的，而不是按被测单元。**

支撑这个判断的三组硬数据：

| 维度 | 数字 | 判读 |
|---|---|---|
| 断言密度 | 每 100 行测试 8–13 个 `t.Fatal*/t.Error*`，无低洼区（见 §Findings 前的表） | **不是空绿**。2,112 个 `TestXxx` 里只有 29 个零断言，逐个打开后**全部是假阳性**（委托给 `mustNoActiveSession` / `assertNoGoroutineLeak` 等 helper） |
| 组织轴 | 仅 **114 文件 / 24,412 行（26.2%）**的测试文件名与同目录某个生产 `.go` 同名 | 3/4 的测试**不能靠文件名找到** |
| 可净删 | 保守估计 **~2,100 行（2.3%）** | 脂肪很薄。债在**索引**和**脚手架重复**，不在断言量 |

**bloat 打分：4/10。** 理由：把一个跑着 1 broker + 6 agent 现网车队的 NAT 穿透 + Raft HA + auth_callout + JetStream 传输 + PTY 系统测到 1.36:1 的 test:prod 比，是**偏低**而非偏高的比例（基础设施类项目 2:1 常见）。扣分全部来自组织与重复，不来自体量：`internal/tunnel` 里同一个不变量被拆成 4 个按审查轮次命名的文件（§F2），`internal/cluster` 在一次 `make e2e` 里被 `-race` 全量跑 **5 遍**（§F5），30 个 clone group / 750 行逐字重复的 helper（§F3）。

**一句话给用户：不是屎山，是没有目录的图书馆。**

---

## 范围与方法

只读审计。用了：`rg`/`grep`/`wc`、两个一次性 `go/ast` 分析脚本（写在 `/home/weiland/.claude/jobs/cda1899e/tmp/`，未入库）、以及对约 25 个测试文件的**逐行阅读**（不是只看计数）。

未运行 `make test` / `make e2e` / 任何 simcluster drill（12 个 agent 并发，重测试会互相饿死）。运行成本结论（§F5）由 `test/e2e/all_phases_test.go` 的静态包 glob + 各包测试函数数推得，不是实测秒数。

分析脚本口径：
- **组织轴分类**：文件名去掉 `_test.go` 后，先匹配 `(review|round\d|rereview|allgreen|codex|external)` → review-round 命名；否则同目录存在同名 `.go` → unit-adjacent；否则匹配 `^(p|d|r|g|s|c)\d+[_.]` → phase/gap 命名；其余 → thematic。
- **归位可行性**：对每个 process-named 测试文件，收集其引用的所有标识符，与同目录每个生产文件的顶层声明求交，取得分最高者；`>=50%` 视为"可机械改名归位"。
- **克隆检测**：AST 提取所有非 `Test*` 函数体 → `go/printer` 打印 → 空白归一 → SHA256 分组，仅统计 **≥8 行且完全相同** 的组（这是重复量的**下界**，近似重复未计）。

### 组织轴全景（问题 1 的答案）

```
93,084 行 / 499 文件
├─ 与被测生产文件同名（unit-adjacent）  114 文件  24,412 行  26.2%
├─ 按审查轮次命名（review-round）        78 文件   8,471 行   9.1%
├─ 按 phase/gap 编号命名（无同名生产文件）56 文件   9,757 行  10.5%
└─ 其他主题式命名                       251 文件  50,444 行  54.2%
```

按位置再切一刀：

```
内嵌在生产包里（internal/* + cmd/tether）  64,194 行  69.0%
test/ 下 phase 编号目录（p1..p13/d3..d9/c7）17,634 行  18.9%
test/ 下主题目录（chaos/security/concurrency/cli_e2e/determinism/storage/cluster/proxydial/e2e）
                                          11,256 行  12.1%
```

热点包的命名轴构成：

| 包 | 测试文件 | 测试行 | 同名归位 | review 命名 | phase/gap 命名 | 其他 |
|---|---|---|---|---|---|---|
| `internal/broker` | 109 | 19,895 | 26 (6,711) | 22 (3,199) | 23 (4,331) | 38 |
| `cmd/tether` | 85 | 13,075 | 16 (2,911) | 18 (1,331) | 12 (1,956) | 39 |
| `internal/agent` | 21 | 4,464 | **2** (1,256) | 5 (514) | 4 (986) | 10 |
| `internal/cluster` | 36 | 5,239 | 10 (1,785) | 3 (146) | 4 (657) | 19 |
| `internal/clusteroffline` | 21 | 3,553 | 8 (2,013) | 6 (669) | 5 (697) | 2 |

生产文件"有同名测试文件"的比例：`internal/broker` 40%（65 个生产文件里 39 个没有）、`internal/cluster` 32%、`cmd/tether` 31%、`internal/agent` **16%**（12 个生产文件里 10 个没有同名测试）。

---

## Findings

### F1 · HIGH — 134 个测试文件按"开发过程事件"命名，导致"改 X 会挂哪些测试"不可查

**证据**

- `internal/broker/clusterdrain.go`（924 行，drain/retire 的 home 迁移引擎，含 `migrateExposes:722`、`pendingRetireConvergence:881`）**没有 `clusterdrain_test.go`**。
- `internal/broker/home_convergence.go`（`awaitHomeConvergence:122`、`kickHomeDelivery:101`）同样没有同名测试。
- 唯一真正覆盖它们的包内文件是 `internal/broker/r8_home_delivery_test.go`（1,092 行，27 处引用这些符号）。文件名里没有 `drain`、没有 `retire`、没有 `convergence`。

**演示：给"cluster drain 的 home 迁移"找守卫测试**（问题 3）

| 一个开发者会试的路径 | 结果 |
|---|---|
| `ls internal/broker/*drain*test*` | **0 命中** |
| `ls internal/broker/*home*convergence*test*` | **0 命中** |
| `grep -rli drain --include='*_test.go' .` | **62 个文件**（噪声：`proxydial/socks5_test.go`、`pty_test.go`、`spawnsafe_test.go` 里的 io drain） |
| `grep -rli retire --include='*_test.go' .` | **37 个文件** |
| 实际答案 | `internal/broker/r8_home_delivery_test.go` — 只能靠 grep **未导出符号名**（`migrateExposes`/`awaitHomeConvergence`/`pendingRetireConvergence`）找到 |

也就是说：**你必须先读懂实现，才能找到实现的测试。** 这把测试从"改动前的安全网"降级成"改动后的事后惊喜"。

更危险的一层：`test/d7/integration_test.go`（591 行，真 raft 多节点 drain/retire 生命周期）在 `//go:build d7_integration` 后面。改完 `clusterdrain.go` 跑 `go test ./...` **是绿的**——那 591 行根本没被编译。只有 `make e2e` 才会跑。名字不提示、构建标签又隐身，两个隐蔽性叠加。

**为什么是债（它让什么变难）**

1. 任何 `internal/broker` 的行为修改，都无法在改之前确定"哪些不变量在保护这块"。实际做法退化成"改完跑全量，等哪个 `TestExternalReviewRound6...` 红了再回读它的注释"。这正是 `CLAUDE.md` §5 明令要避免的"为验证一处小改反复全量跑"。
2. 新增测试时无处安放，于是继续新开一个按当前轮次命名的文件——**这个模式是自我复制的**，78 个 review 文件就是这么长出来的。
3. 删除生产文件时，无法判断哪些测试随之作废。

**建议**：`git mv` 归位。分析脚本对 123 个 process-named 文件做了"标识符引用集中度"计算，**88 个（71.5%）有 ≥50% 的生产符号引用集中在单一生产文件**，可直接机械改名。样例（分数=集中度）：

| 现名 | 归位目标 | 集中度 |
|---|---|---|
| `internal/broker/r8_home_delivery_test.go` | `home_delivery_test.go` | 118/218 (54%) |
| `internal/tunnel/p13_external_review_round{2,4,5,6}_test.go` | `tunnel_*_test.go` | 各 100% |
| `internal/jsstream/r16_g67_g69_external_review_test.go` | `jsstream_test.go` | 18/19 (95%) |
| `internal/clusteroffline/r10_doctor_db_test.go` | `doctor_test.go` | 22/22 (100%) |
| `cmd/tether/g7_remote_test.go` | `cluster_status_nats_test.go` | 11/12 (92%) |
| `internal/natsconf/g4_harvest_fallback_test.go` | `preflight_test.go` | 22/24 (92%) |
| `internal/broker/g4_adaptive_catchup_test.go` | `cluster_operation_controller_test.go` | 6/7 (86%) |

**量化**：改名本身 0 净减行；合并进已有同名测试文件时省掉重复的 `package`+`import` 头，78 个 review 文件预计合并成 ~30 个 → 约 **−500 行**。真正的收益是 O(1) 的定位成本。
**风险**：**low**。纯 `git mv` + 少量 import 合并，不碰任何实现、不碰 wire、不碰 `architecture.md` 不变量。可拆成每包一个 PR。

---

### F2 · HIGH — 同一个不变量按审查轮次逐个 verb 复制成 4 个文件，而不是一张表；这直接造成了 3 轮返工

**证据**

`internal/tunnel` 有三个 kill 语义 fence（`tunnel.go:132-143`：`killGen[port]` / `killGenAllocation[key]` / `killGenSession[sid]`），4 个 kill verb（`CloseProxy:521`、`CloseProxyIf:545`、`ForgetSession:612`、`CloseSession:673`）。"in-flight REGISTER 已过 token lookup 但尚未装入 `s.sessions` 时，kill verb 必须让它失效"这**一条**不变量，被写成了：

| 文件 | 行数 | verb | 机制 |
|---|---|---|---|
| `p13_external_review_round2_test.go:15` | 80 | `CloseProxy` | 真 Server + 真 REGISTER 竞态（`authorized`/`releaseLookup` channel） |
| `p13_external_review_round5_test.go:11` | 104 | `CloseSession` | **同上，逐字复制** |
| `p13_external_review_round6_test.go:11` | 79 | `ForgetSession` | **同上，逐字复制** |
| `d6_test.go:75` (`TestD9CloseProxyIfFencesPortReuse`) | — | `CloseProxyIf` | **不同机制**：手搓 `Server{}` 字面量 + 直接读 `srv.killGenAllocation` map |

round2 与 round5 的 `diff` 只有 **3 行实质差异**：`srv.CloseProxy(publicPort)` → `srv.CloseSession("lab")`，加一句错误串。round6 再来一次。

**为什么是债**

这不是审美问题，是**已经发生的返工**：外审在 round2 发现 `CloseProxy` 漏 fence，主进程修好加了一个文件；round5 又发现 `CloseSession` 有同样的洞；round6 又发现 `ForgetSession` 有同样的洞。**如果 round2 时把它写成一张 `{verb, killFn, wantErr}` 表，round5 和 round6 这两轮返工在结构上就不会发生**——加第四行的成本是 5 行，而覆盖矩阵一眼可见。

按当前形态，`tunnel.go` 再加第 4 个 fence 维度（代码明显在往这个方向走：已有 3 个 `killGen*` map 用于同一个 `fenced` 判断，`tunnel.go:405`、`:441`）时，你需要知道去改 **4 个文件、2 种断言机制**，而且其中 3 个文件名只告诉你"这是第 2/5/6 轮外审"。

**建议**：合并为单个 `internal/tunnel/kill_fence_test.go`，一张表覆盖全部 4 个 verb，用统一的"真 Server + in-flight REGISTER"机制（`CloseProxyIf` 的白盒 map 探查降级为该表的一行）。
**量化**：263 行（round2/5/6）+ `d6_test.go` 里约 70 行 → 约 140 行。**净 −190 行**，且第 5 个 verb 的边际成本从"新开一个文件"降到"加一行"。
**风险**：**low**。纯测试重构，`internal/tunnel` 无 wire 面。合并时必须保留三段各自的 doc comment（它们解释了三种竞态的因果差异，有真实价值）。

---

### F3 · MEDIUM-HIGH — 750 行逐字重复的测试脚手架；`internal/testharness` 存在但只覆盖了单机原语

**证据**（AST 克隆检测，≥8 行且空白归一后完全相同，30 个 clone group）

| helper | 行数 × 副本 | 位置 |
|---|---|---|
| `startAgent` | 28 × 5 = 112 | `test/p{4,5,7,8,9}/*_e2e_test.go` |
| `openDB` | 8 × 11 = 80 | `internal/{agentprov,broker,clusternodes,port,session}` + `test/{concurrency,d3,d6,p3,security}` |
| `startNATS` | 13 × 6 = 65 | `cmd/tether/cli_failover_external_review_test.go`、`cmd/tether/d8_external_review_test.go`、`internal/agent`、`internal/broker`、`test/concurrency`、`test/p2` |
| `moduleRoot` + `goListDeps` | 27 × 4 = 81 | `test/d{5,6,7,8}/regression_test.go` |
| `waitForCond`/`waitFor` | 9 × 5 = 36 | `internal/cluster/read_test.go`、`test/d{3,4,5,8}/setup_test.go` |
| `startBroker`(两种签名) | 68 | `test/p{4,5}` 一组、`test/p{7,8}` 一组 |
| `startRoutedJS`+`attemptRoutedJS` | 24 × 2 | `test/d5/setup_test.go:83`、`test/d8/setup_test.go:65` |
| `assertNoGoroutineLeak`/`assertNoFDLeak`/`fdCount` | 45 | `test/{concurrency,d4,d5,d8}` + `internal/cluster/wal_concurrency_test.go` |
| ...另 22 组 | | |
| **合计（下界）** | **750** | **30 组** |

近似重复（未计入上表，但更大）：`newRouteCA`+`clusterLeaf` 的 mTLS route CA（`test/d3/route_mtls_test.go:28`、`test/d4/setup_test.go:167`、`test/d5/setup_test.go:189`、`test/d8/setup_test.go:156`，4 份）、`startCluster4`/`startCluster5`/`startD8Cluster`（routed-NATS + mTLS-raft 集群起停，3 份结构同构的 ~120 行）、`silentLog` **13 份**、`jwtToServerPerms` **5 份**。

**关键点**：`internal/testharness` **已经存在**（189 行），导出 `StartNATS`/`StartJSNATS`/`OpenDB`/`SilentLog`/`WaitFor`/`WaitNodeOnline`/`WaitConnect`/`FreshUserPub`，被 36 个文件引用。它的包注释诚实地写着边界：

> Per-phase test files still own the harness pieces that vary by phase … only the truly identical primitives live here.

问题是这条线画在了**单机原语**上，而真正贵的、最该共享的是**多节点 routed-NATS + clustered-JS + mTLS-raft 集群 harness**——那部分在 `test/d{3,4,5,8,9}/setup_test.go` 里写了 5 遍（合计 1,712 行）。上面 `openDB`×11 和 `startNATS`×6 更是**已经有 `testharness.OpenDB` / `testharness.StartNATS` 却没用**。

**为什么是债**

- 改一个 nats-server option / JetStream 配置 / mTLS 曲线，要在 4–5 个 `setup_test.go` 里改同一件事；漏一个 → 那条 lane 的行为与其他 lane 悄悄分叉，且分叉只在 `make e2e` 那 6 分钟里显形。
- 新增一条集群 lane（比如未来的 D10）会**再复制一份 468 行**——这是"数量跟着 phase 走"的结构性增长。
- `CLAUDE.md` 明确的"NumGoroutine/fd 泄漏门"是核心并发纪律，但 `assertNoGoroutineLeak` 有 4 个略有差异的实现（tolerance/poll 参数各写各的），泄漏门的口径其实并不统一。

**建议**：把 `internal/testharness` 扩成两层——`testharness`（现有单机原语）+ `testharness/cluster`（routed CA / `startRoutedJS` / N 节点 mTLS-raft 起停 / leaderIdx / 统一的 leak+fd 门）。同时把 `openDB`×11、`startNATS`×6、`silentLog`×13 换成已有导出。
**量化**：逐字重复 750 行是下界；把 5 份集群 setup 收敛后保守 **−700 ~ −900 行**，泄漏门口径归一（这一条的价值大于行数）。
**风险**：**medium**。纯测试代码，但集群 harness 是最容易引入 flake 的地方——必须一个 lane 一个 PR、每个 PR 单独跑 `go test -tags dN_integration -race ./test/dN/`，不要一次性全换。

---

### F4 · MEDIUM — 35 处把生产源码当字符串 grep 的断言，其中若干对 gofmt 空白敏感

**证据**（`strings.Contains(string(src), …)` 形态，17 个文件 35 处）

最尖锐的两例：

```go
// cmd/tether/d9_external_review_test.go:14
if strings.Contains(string(src), "Peers:         []natscluster.Broker{self}") {
    t.Fatalf("takeover-natsconf renders only the local broker, ...")
}
```
这里的 `Peers:` 后面是 **gofmt 结构体字段对齐产生的 9 个空格**。同结构体里任何一个字段名变长/变短，对齐宽度改变，这条断言就**永久静默通过**——因为它是 `if Contains { fail }`，找不到就是绿。

```go
// test/d7/external_review_test.go:15
if strings.Contains(src, "case adminsock.OpClusterTransfer:\n\t\t\tif err := b.admin.node.TransferLeadership();") {
```
把 `\n\t\t\t` 缩进层数写进了断言。`clusterstatus.go` 里这个 `switch` 只要多包一层 `if`，缩进变 4 个 tab，断言静默失效。

`test/d7/external_review_test.go`（70 行）里 **8 处** 全是这个形态，密度最高；它的文件头自陈：

> contains reviewer regressions for D7 contracts that are **advertised by the architecture/plan but not implemented by the current tree**

——这是一份**被编码成测试的 TODO 清单**。修完之后，它们本该被替换成真行为断言，但留了下来。

`internal/broker/r8_home_delivery_test.go:833-867`（本仓最好的测试文件之一）也有 **7 处**同类断言，说明这个模式已经渗进了主力文件。

**为什么是债**

两个方向的失效都很难察觉：
- `if Contains{fail}` 形态 → 重命名/重排版后**永久假绿**，不变量实际无人守卫。
- `if !Contains{fail}` 形态 → 无害重构触发**假红**，制造"改不动"的心理税，且修复方式是改测试字符串（进一步固化耦合）。

对已发布产品，假绿是真正的风险：`d9_external_review_test.go` 守的是"takeover 渲染的 nats.conf 是否只含本机 broker"——渲染错了直接导致 grow 出来的集群没有 route mesh，这是现网级故障。

**建议**：分三类处置。
1. **能改成行为断言的**（多数）：`d9_external_review_test.go` 应该调用渲染函数、断言输出 conf 的 `routes` 块含 ≥2 个 peer——而不是 grep 渲染器的源码。
2. **真正的分层/接线 pin**（`internal/broker/js_placement_gate_test.go:337` 的 `admin.jsPlaceableFn = b.clusterJSPlaceable`、`g67_wiring_ast_pin_test.go`）：统一走 `go/ast`（后者已经是），不要走字符串。抽一个共享 helper `assertCallSitePresent(pkg, caller, callee)`。
3. **文档漂移守卫**（`test/d7/external_review_test.go:39-64` 扫 `docs/cluster-runbook.md` / `docs/usage.md`）：这类**有真实价值**（`feedback-contract-change-sweep` 记的正是这个教训），但放在 `test/d7/` 是错位——改 `docs/usage.md` 的人不会想到去看 `test/d7`。应移到 `test/docs/cli_doc_drift_test.go`。

**量化**：净减行 ≈ 0（换机制不换数量），消除的是约 **12 处潜在假绿**。
**风险**：**low**（不碰实现）。但改成真行为断言时可能**真的发现红**——那是收益不是代价。

---

### F5 · MEDIUM — `make e2e` 把整包重复跑：`internal/cluster` ×5、`internal/broker` ×2，全部 `-race -count=1`

**证据**（`test/e2e/all_phases_test.go` 的包 glob，逐个数）

| 包 | 出现在 | 次数 | 测试函数数 |
|---|---|---|---|
| `./internal/cluster/...` | D1(:133)、D2(:151)、D3(:170)、D4(:189)、D5(:216) | **5×** | 98（含 `kill9_test.go` 自 fork race 二进制的崩溃一致性矩阵） |
| `./internal/broker/...` | D4(:189)、D5(:216) | **2×** | **489**（全仓最重的包，19,895 行测试） |
| `./internal/proc/...` | D2、D4、D5 | 3× | 20 |
| `./test/cluster/...` | D1、D2 | 2× | 20 |

每次都带 `-count=1`（显式禁用测试缓存，`runPhase` 注释解释了理由：夜跑要真跑不要 cached PASS）+ `-race`。且 D5 的 `-tags d5_integration` 换了 build cache key，即便去掉 `-count=1` 也不会命中 D4 的缓存。

一次 `make e2e` 的冗余执行量：4×98 + 1×489 + 2×20 + 1×20 ≈ **940 次多余的 race-instrumented 测试函数执行**。在一个刻意串行、总预算约 6 分钟的 release gate 里，这是可观的一块。

叠加上 `make test`（`go test ./...`，非 race）已经跑过一遍全部这些包，提交前硬闸里 `internal/cluster` 实际被执行 **6 次**。

**串行决策本身的评估**（问题 6）：**这个决策是对的，且论证是我在本仓见过最扎实的工程注释之一。** `Makefile:22-34` 和 `all_phases_test.go:32-38` 记录了：试过并行、测过、D8 在 2-way 挂、D5 在 4-way 挂、失败模式是 clustered-JS meta-group 形成时的 "routed JS server not ready"、GOMAXPROCS 封顶也无效、因此 revert。**这不是"懒得优化"，是"优化过、有数据、有结论"。** 长期代价（6 分钟 gate，只在 push main + nightly 跑，PR 不跑）也已经被 CI 分层吸收得很好（`ci.yml:53-56`）。

真正的问题不是串行，而是**串行地跑重复的东西**。

**为什么是债**

- 6 分钟的 gate 里有相当一部分是纯重复，这直接抬高"提交前硬闸"的心理门槛，诱导跳过——而 `CLAUDE.md` §5 把它定为**不可跳的硬闸**。
- 每个 D 矩阵的包 glob 是**手写累积**的：D4 加 `./internal/broker/...` 是因为 D4 碰了 broker，D5 也加是因为 D5 也碰了。没人回头删。加 D10 时同样会再追加，重复只增不减。
- 矩阵的语义已经漂移：`TestD1Matrix`..`TestD9Matrix` 名义上是"phase 出口回归"，实际是"一组包 glob"。两者不再对应，导致没人敢删任何一个 glob。

**建议**：把 D 矩阵拆成两层——(a) 每个 D 的**专属重套件**（`./test/dN/...` + 该 phase 真正独占的包，保持 `-race` 独立子进程），(b) **一个** `TestRaceUnitSweep`，跑一次 `-race ./internal/...`。glob 去重后 `internal/cluster` 从 5 次降到 1 次、`internal/broker` 从 2 次降到 1 次。
**量化**：**0 净减行**（甚至 `all_phases_test.go` 会略微变短），预计砍掉 `make e2e` 的 **25–40% 墙钟**（未实测，需要跑一次前后对比确认）。
**风险**：**medium**。这是发布闸门，改错了会漏跑。必须先做一次"改前 / 改后跑同一 commit 的包集合 diff"证明覆盖不减，再合并。**注意 `-tags` 差异**：`-tags d5_integration ./internal/broker/...` 与不带 tag 的构建集合可能不同，去重前必须逐包核对。

---

### F6 · MEDIUM — 3 处死测试（240 行），其中一处每次 `make test` 都真起 NATS + broker 然后 Skip

**证据**

1. **`test/cli_e2e/expose_lifecycle_test.go:227-286`** — `TestExposeDataPlaneTCPEcho`。这个函数**跑完了**全部昂贵 setup（`net.Listen` + echo goroutine :234、`startNATS(t)` :259、`openDB` :260、`seedSession` :262、`tunnelOnRandomPort` :263、`defer startBroker(t, url, db, tunOpts)()` :264、再来一次 `tunnelOnRandomPort` :269），然后：

   ```go
   // Easier path: tear down the convenience helper duplication —
   // we know the broker's listener address came from the first
   // tunnelOnRandomPort call. Let's restructure by inlining:
   _ = pub
   _ = echoAddr
   t.Skip("inlined data-plane wiring covered by dedicated subtests below")
   ```

   这是**开发者的意识流草稿被提交了**（`_ = tunAddr` / `_ = pub` / `_ = echoAddr` 三个丢弃赋值 + 5 段自言自语注释）。紧接着的 `TestExposeDataPlaneTCPEchoInline` 是真正工作的版本。这段死码**在每次 `make test`（提交前硬闸 + 每个 PR 的 CI）里真起一个 NATS server、一个 broker、一个 tunnel server**，然后丢掉。

2. **`test/c7/drill_test.go`** — 27 行，`//go:build c7_integration`，函数体是无条件 `t.Skip`。全仓搜索 `c7_integration`：只在 `docs/reviews/c7-plan.md` / `c7-review.md` 里出现，**`Makefile` 没有、`all_phases_test.go` 没有、CI 没有**。它是一个**永远不会被编译的、内容为 Skip 的文件**——两层死。

3. **`test/d9/grow_migrated_leader_e2e_test.go:48`** — 151 行文件，`t.Skip` 在函数**第一句**（前面 13 行注释解释为什么这个 fixture 建模了一个不真实的 leader）。后面 100+ 行永不执行。

**为什么是债**

- 第 1 项是**真实的重复运行成本**，落在最频繁的闸门上；而且它以 SKIP 的形式出现在输出里，看起来像是"有意为之"，不会有人去查。
- 第 2、3 项是**伪装成测试的 TODO**。本仓 TODO/FIXME 全仓只有 1 处（这是很好的纪律），但代价是待办事项改用 `t.Skip("tracked follow-up …")` 和永不构建的 build tag 来记录——这些**不会出现在任何 TODO 扫描里**，纪律指标是虚高的。
- `t.Skip` 总共 37 处，其余 33 处是**合理**的平台/权限门控（root 绕过 mode bit、Linux-only `/proc/self/fd`、POSIX perm、`-short` 模式），这部分做得好。

**顺带记一条真 bug 风险（不是本 lane 主线）**：`test/chaos/disk_failure_test.go:198` 在 `nats.ErrNoResponders` 时 `t.Skip("agent not yet subscribed; race ok")`。CI 负载高时这个测试会静默变成 no-op，而它守的是 agent 对恶意 upgrade URL 的本地防御（`url_not_allowed_local`）——一个安全相关断言在负载下会自我豁免。应改为轮询等待订阅就绪后再断言。

**建议**：删除 1、2、3；把 2、3 的内容转成 `docs/reviews/` 里的 follow-up 条目（本仓已有 `docs/reviews/` 归档惯例）。修 `disk_failure_test.go:198`。
**量化**：**−240 行** + 每次 `make test` 省一次 NATS/broker 起停。
**风险**：**low**。删的都是不执行的代码。

---

### F7 · LOW-MEDIUM — 一条分层不变量被拆进 4 个 phase 目录，同一规则断言 4 次

**证据**

`test/d{5,6,7,8}/regression_test.go`（84+83+130+83 = 380 行）全部是 import 分层守卫（"L-2 线"）。同一条规则被反复重述：

| 规则 | 断言处 | 次数 |
|---|---|---|
| `internal/cluster` 不得（传递）import nats.go | d5:22、d6:39、d7:39、d8:22 | **4×** |
| `internal/jsstream` 不得 import `internal/cluster` | d5:44、d8:31 | 2× |
| `internal/clusternodes` 保持 pure-SQL leaf | d6:21、d7:24 | 2× |

`d6/regression_test.go:37` 的注释直说了：`// TestD6ClusterStillNoNATS re-asserts the D5 L-2 boundary survives D6.`

外加 `moduleRoot` + `goListDeps` 两个 helper 各 4 份（81 行重复），每次 `make test` 都 shell out `go list -deps` 至少 9 次。

**为什么是债**

分层规则是**架构级、与 phase 无关**的横切不变量。按 phase 切开的结果是：(a) 加一条新规则时不知道该加进哪个 `dN`，只能再开一个；(b) 4 份文件头都是"D9 cutover 删掉了 build-and-prove guard，但下面这些 L-2 guard 是正交的所以留下"——同一段考古注释抄了 4 遍；(c) 规则集合没有单一视图，没人能一眼说出"tether 现在有几条分层规则"。

**建议**：合并成 `test/architecture/layering_test.go`，一张 `{pkg, banned []string, required []string}` 表 + 一份 `goListDeps`。
**量化**：380 → ~130 行，**净 −250 行**，且新规则的边际成本从"新开文件"降到"加一行"。
**风险**：**low**。

---

### F8 · LOW — `test/README.md` 是 8 行过时 stub，28,890 行测试代码没有导航入口

**证据**：`test/README.md` 全文只描述了 `e2e/` 一个子目录，而 `test/` 下实际有 **30 个目录**（p1–p13、d3–d9、c7、chaos、cli_e2e、cluster、concurrency、determinism、proxydial、security、storage、simcluster）。它说"package-local Go unit tests may still live next to implementation files"——而实际上 69% 的测试行都在 `internal/` 里。这份 README 描述的是一个从未存在过的结构。

对比之下 `test/simcluster/README.md` 有完整的 Mandate 和定位铁律（做得非常好，见反证）。

**为什么是债**：新人（和 6 个月后的自己）进入这个测试树时没有地图，只能靠 grep。这直接放大了 F1 的定位成本。

**建议**：重写成一张表——每个目录一行，写清"测什么面 / 有没有 build tag / 什么时候跑 / 谁是它守的不变量"。特别要标出 `d5_integration` 等 8 个构建标签（**`go test ./...` 看不到它们**，这是 F1 里最危险的那一层）。
**量化**：+60 行文档，−(大量重复的定位时间)。
**风险**：**low**。

---

## 反证：做得好的地方

不是客套，是这次审计里实测出来的、明确应该保护的东西。

1. **没有空绿。** 2,112 个 `TestXxx` 里零断言的只有 29 个，逐个打开后**全部是假阳性**——委托给了 `mustNoActiveSession`（`cmd/tether/no_active_session_test.go:24`）、`assertNoGoroutineLeak`、`t.Run` 子测试。断言密度在所有热点区域都是每 100 行 8–13 个（`internal/clusteroffline` 12.8、`internal/broker` 10.8、`internal/cluster` 10.5、`cmd/tether` 9.6、`test/chaos` 6.4），**没有低洼区**。这在 9.3 万行的规模上是很难做到的。

2. **对抗性纪律是真的落地了，不是口号。** 三个最有说服力的实例：
   - `internal/broker/r8_home_delivery_test.go:103-148` 的 `peerSilenceMonitor`——它断言的是**因果**不是结果：注释明说"没有它，测试可能因为 fake agent 碰巧重新注册而通过，bug 看起来修好了但其实没有"。这是 senior 级的测试设计。
   - `internal/broker/g67_review_fixes_test.go:28-33` 的 `blockingJS` 显式**拒绝了更弱的 mock**：`countingJS returns instantly and therefore leaves the entire wall-clock budget dead under the suite — internal review B2`。即"我们审过自己的 mock 会不会让测试变空"。
   - `test/d4/review_fixes_test.go:28-31` 的注释写明了 **RED-on-pre-fix 证据**：`RED on the pre-fix code (ErrKind via %T + no ForwardBusinessError.Is): zero pin_failed + a "provision:" deny`。每条回归钉都记录了它在修复前确实是红的——这是回归测试有效性的唯一硬证据，本仓大量测试都带。

3. **测试注释质量全仓顶尖。** 打开任意一个 review-round 文件，函数头都有 3–15 行说明"这条守的是什么、为什么这个 fixture 是必要的、去掉会怎样悄悄失效"。例如 `internal/broker/js_placement_gate_test.go:20-29` 的 FIXTURE NOTES 明确写了 "TopoTargetGen: 0 so topoConvergedForOp SHORT-CIRCUITS TRUE … if a future change removes the short-circuit, these would silently become no-gate tests"——**作者预判了自己的测试会怎么退化**。**F1/F2 建议改名合并时，这些注释必须逐字保留，它们是本仓最有价值的资产之一。**

4. **`t.Skip` 纪律良好。** 37 处里 33 处是合理门控：root 绕过 mode bits（`datadirlock_round6_test.go:66`、`g4_grow_trigger_test.go:365`、`g1_reconciler_perm_test.go:35`）、Linux-only `/proc/self/fd`（`runtime_introspect_test.go:159`）、POSIX perm 语义（`js_store_test.go:210`）、`-short` 模式跳大数据量（`ps_perf_test.go`）。只有 F6 点名的 3 处是死的。

5. **构建标签分层是正确的成本控制。** 8 个 `dN_integration` / `phasefluidity_integration` 标签把重的 clustered-JS/raft 套件挡在 `make test` 之外，让日常 `go test ./...` 保持快。`all_phases_test.go:211-215` 的注释解释了理由（"~30 concurrent package binaries starve the embedded JS clusters into timeouts"）。这是深思熟虑的，不是随手加的。

6. **e2e 串行是有实测依据的工程决策，不是懒惰。** `Makefile:22-34` + `all_phases_test.go:32-38` 记录了完整的试→测→退过程（D8 在 2-way 挂、D5 在 4-way 挂、GOMAXPROCS 封顶无效）。**保留这个决策**；F5 攻击的是重复，不是串行。

7. **CI 分层合理。** `ci.yml:53-56`：`make test` 每次 push/PR，`make e2e` 只在 push main + nightly，`make lint` 独立 job 且版本通过 Makefile 单一来源强制（`Makefile:37-52` 的版本闸解释了"local-pass-CI-fail 比每次装二进制更糟"——这条推理是对的）。

8. **`internal/testharness` 存在且被用。** 189 行、36 个文件引用、包注释诚实标注了自己的边界。F3 攻击的是这条边界画得太保守，不是它不该存在。

9. **表驱动被真正使用。** 328 个测试函数用了表、216 个用了 subtest。`test/cluster/equiv_test.go` 的 differential 测试（两个独立 `freshDB` 上跑 direct-mutator vs FSM-rendered 两条真实不同的代码路径，逐字段比对）是本仓最漂亮的测试设计之一。

10. **`test/simcluster/README.md` 的 Mandate 是文档典范。**「暴露而非弥补」四条铁律 + `[GAP #N]` 标注机制，把"模拟环境不许替产品把活干完"这条极易滑坡的原则钉死了。这套东西**应该被抄到 `test/README.md`**（见 F8）。

---

## 本质 vs 偶然复杂度拆解

**本 lane 的口径：93,084 行测试代码里，多少是被测问题域强加的（本质），多少是实现/组织方式造成的（偶然、可消除）。**

### 本质部分（估 ~80%，约 74,000 行）

tether 要证明的性质，**没有任何一个可以靠更聪明的写法压缩掉**：

| 面 | 为什么必须真起东西 |
|---|---|
| Raft 共识 + SQLite FSM | `applied_index` 同事务不变量、幂等 re-apply、快照/恢复、真 SIGKILL 崩溃一致性（`internal/cluster/kill9_test.go` 自 fork race 二进制）——这些**只能**用真 raft + 真 kill 证 |
| auth_callout nkey 身份 | 必须真起 nats-server、真签 JWT、真验 ACL；mock 出来的 ACL 决策不证明任何东西 |
| 跨机 route mTLS | 必须真铸 CA、真签 leaf、真跨进程连——这是 `test/d3/d4/d5/d8` 那 1,712 行 setup 的存在理由 |
| clustered JetStream 副本重配 | 必须真起 routed JS 集群等 meta-group 形成 |
| 数据面隧道 + PTY | 必须真开 listener、真 echo、真 attach |
| 分布式竞态（in-flight REGISTER vs kill verb） | 必须用 channel 卡住真实的时序窗口（`internal/tunnel/p13_*` 那一组的做法是对的） |

外加：**1.36:1 的 test:prod 比对基础设施类项目偏低**。作为对照，同类性质的项目（etcd、consul、nats-server 本身）普遍在 1.5:1 ~ 2.5:1。所以从总量上说，**这个测试套件不是写多了，可能还偏少**——`internal/agent` 只有 16% 的生产文件有同名测试，`internal/broker` 40%。

### 偶然部分（估 ~20%，约 19,000 行）——但其中只有约 2,100 行是可净删的

偶然复杂度在这个 lane 里主要**不表现为多余的行**，而表现为**同一件事被复述多次**和**索引缺失**：

| 偶然复杂度来源 | 规模 | 可净删 | 根因 |
|---|---|---|---|
| 脚手架逐字重复 | 750 行（下界）+ 集群 harness 近似重复 ~1,200 行 | −700 ~ −900 | `testharness` 边界画在单机原语上 |
| review-round 文件碎片（头/import/fixture 重复） | 78 文件 8,471 行 | −500 | **工作流产物**：每轮外审新开一个文件 |
| 同一不变量按 verb/phase 复述 | tunnel 263 行、layering 380 行 | −440 | 同上 |
| 死测试 | 240 行 | −240 | 提交了草稿 / TODO 伪装成 Skip |
| **合计** | | **≈ −2,100 行（2.3%）** | |

**关键结论：偶然复杂度在这里的主要成本不是行数，是定位成本。** 18,228 行（19.6%）按过程事件命名意味着——每次要修改 `internal/broker` 的一个行为，"哪些不变量在保护它"这个问题的回答成本是 O(全仓 grep + 读实现)，而不是 O(1)。这个成本每次改动都付一遍，且随文件数增长。删 2,100 行救不了它；**改名归位（F1）才救得了它，而且改名一行不减。**

### 这套工作流是不是在产生垃圾？（用户原问题的子问题）

**不是垃圾，是没归档的好东西。** 78 个 review 文件我抽读了约 20 个，**没有一个是空绿或凑数**——每个都钉着一条真实的、注释解释得很清楚的不变量，很多还记录了 RED-on-pre-fix 证据。「多专家内审 → 用户外审」这套流程**产出的内容质量很高**。

流程的真实缺陷只有一条：**它缺少一个"收尾归档"步骤。** 现在的 step 5「主进程评估审查正确性并修改」把新测试原样落盘就结束了，没有「把本轮新增的测试归位到被测单元旁、看看是否与既有测试构成一张表」这一步。F2 的 tunnel 案例证明了这一步的价值不是整洁——**它能在 round2 就消灭 round5 和 round6 这两轮返工。**

建议往 `CLAUDE.md` §3 step 5 加一句（这是本报告唯一涉及流程的建议）：

> 5b. **测试归位**：本轮新增的测试文件按**被测单元**命名并放到它旁边；若与既有测试守同一条不变量，合并成表而不是新开文件。文件名里不得出现轮次号/gap 号。

---

## 附录：78 个 review 文件的处置清单（问题 7）

分类口径来自 §范围与方法 的集中度分析 + 抽样阅读。

| 类别 | 文件数 | 行数 | 处置 | 净减 |
|---|---|---|---|---|
| **A. 真不变量守卫，可机械改名归位**（集中度 ≥50%） | ~55 | ~6,100 | `git mv` 到 `<prod>_test.go`，或合并进已有同名文件 | −350（头/import 去重） |
| **B. 同一不变量的多轮复述，应合并成表** | ~12 | ~1,400 | 合表：tunnel kill-verb（4→1）、broker p13 proxy 系列（5→1~2）、clusteroffline force_single 系列（4→1） | −450 |
| **C. 跨包/横切不变量，应移到主题目录** | ~6 | ~600 | layering guards → `test/architecture/`；doc-drift guards → `test/docs/` | −250 |
| **D. source-string-grep 断言，应换机制** | ~5 | ~250 | 改真行为断言或统一 AST helper（F4） | 0 |
| **E. 死码** | 3 (含非 review 命名) | 240 | 删 | −240 |
| **F. 保持原样**（集中度低、确实跨多文件的编排级测试，如 `g67_review_fixes_test.go` 的传输生命周期） | ~5 | ~1,200 | 只改名去掉轮次号（如 → `xfer_lifecycle_test.go`），不动内容 | 0 |
| **另：脚手架收敛**（F3，跨 A–F） | — | — | `testharness/cluster` | −700 ~ −900 |
| **合计** | | | | **≈ −2,000 ~ −2,200 行（2.2–2.4%）** |

**没有一条建议触碰 wire 协议、`internal/proto.ProtoVersion`、auth_callout 身份、G.1/G.2 reconcile、session ACL 或 `architecture.md` 的任何不变量。** 全部是测试侧重构，可以逐包渐进，任何一步都能独立验证（改完某包跑 `go test ./internal/<pkg>/`，改完某 lane 跑 `go test -tags dN_integration -race ./test/dN/`）。唯一需要谨慎的是 F5（发布闸门）和 F3 的集群 harness（flake 高发区），两者都标了 medium risk 并给了分步验证路径。

**优先级建议**：F6（删死码，零风险，立即做）→ F1（改名归位，零风险，收益最大）→ F7（layering 合并）→ F2（tunnel 合表）→ F8（README）→ F3（harness 收敛，分 lane）→ F4（换断言机制）→ F5（e2e 去重，最后做，需前后覆盖 diff 证明）。
