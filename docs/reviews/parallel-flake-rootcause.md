# 满载并行 flake — 根因与修复

> 2026-07-26。对应外审 `e2e-parallel-external-review.md` **B1 / 放行条件 5** 与
> `batch-a-external-review.md` **R5**。
>
> 外审的判断是准确的：*"CPU affinity 只隔离了 logical CPU；各进程仍共享调度器、内存带宽、
> 页缓存、文件系统、网络栈、Go 编译缓存和测试内的选举 deadline。"*
> 本文是顺着这句话查下去的结果——**不是一个 flake，是四类互不相同的根因**。

## 结论摘要

| # | 根因类别 | 表现 | 修复 | 门禁 |
|---|---|---|---|---|
| 1 | **测试自编 raft 计时** | d3 / broker / d7 在满载下丢 leader | 17 处对齐生产常量 | `TestRaftTimingsUseProductionConstants`（变异验证通过） |
| 2 | **观测后假设身份不变** | d3 得到 `session does not exist`；d7 58 次 transfer 失败 | 重建前提 / admin 跟随 leader | — |
| 3 | **资源分配不均** | p13 的 0.9s 断言等满 15s | 宽 worker（`reserveHeavyWorker`） | 覆盖自检已有 |
| 4 | **端口 TOCTOU** | tunnel harness 0.17s 硬失败 | 换端口重试 | — |

四类都被写进 `docs/testing-standards.md`（T1–T5），因为它们都不是"这一个测试的 bug"。

---

## 根因 1 · 测试自编 raft 计时（系统性）

外审 M4 在 d7 上指出过一次（60ms → 300ms → 最终 1000ms）。**但没人查过 d7 之外。**

全仓扫描结果：**17 处**硬编码，跨 14 个文件，只有 1 处对齐生产。

```
HeartbeatTimeout:   50 / 60 / 80 / 100 / 150 ms     生产 1000ms
LeaderLeaseTimeout: 25 / 40 / 50 / 75 ms            生产 500ms
```

`LeaderLeaseTimeout: 25ms` 意味着 leader 只要 25ms 没拿到多数派 ack 就下台。
这些套件多数强制带 `-race`（内存访问慢 5–10 倍），再叠加 20 worker 满载——
**掉 leader 是必然而非偶然**。

**修复**：51 处（三字段 × 17）改为引用 `cluster.Multinode{Heartbeat,Election,LeaderLease}Timeout`。
引用常量同时消除了漂移：生产值改了，harness 自动跟随。

**代价为零**：`internal/broker` 对齐前 272s、对齐后 254.9s。这些超时约束的是**故障检测**，
不是 happy path——外审 M4 早就这么说了，这次是实测确认。

**一处必须保留硬编码**（已进门禁豁免表并写明理由）：
`TestD3FailingNewReapsTransportNoLeak` 需要 `lease(2s) > heartbeat(1s)` 这个**非法排序**
让 `raft.ValidateConfig` 拒绝，从而制造"transport 已建立但 New 失败"的路径——
那条路径正是它要验证会被回收的东西。改成合法值后 `New` 成功，测试**什么都不断言**。
批量替换踩到了它，而**它自己把这件事抓住了**（一个断言"必须失败"的 fixture，失效时立刻变红）。

## 根因 2 · 观测分布式状态后假设它不变

### d3

```go
if n.IsLeader() { leaderIdx = i }
followerIdx := 1 - leaderIdx
nodes[followerIdx].Propose(...)   // 它现在可能已经是 leader
```

领导权一旦在这两步之间转移，被测节点就是**真 leader**，于是它真的执行业务逻辑，
发现 session 不存在（seed 只写了原 leader 的 DB）→ `ErrSessionMissing`。
外审看到的 `session does not exist` 正是这个，**不是产品缺陷**。

**修复**：调用前后各确认一次身份，前提失效则重建前提（有限次重试整个观测—操作序列）。
**不是**放宽断言——接受 `ErrSessionMissing`"因为领导权可能变了"等于删掉这个测试。

### d7（这条我前两次都修错了）

失败信息：`node 0 did not hold leadership within 29.68s after 58 transfer attempt(s)`。

harness 的 `c.admin` 绑定 `nodes[0]`，所以 nodes[0] **必须**是 leader。
我的前两次修复都在维护这个假设：先是等更久，然后是**主动请求当前 leader 把领导权转回来**。
后者在满载下输掉了 58 次尝试——**一个刚赢得选举的 voter 没有理由让出来**，
而每次尝试都在和下一次 commit 赛跑。

**假设本身才是缺陷。** AddNode 必须跑在 leader 上，但不必跑在 `nodes[0]` 上。
`nodes[0]` 持有领导权只是 bootstrap 的偶然属性，不是需求。

**修复**：`adminForLeader()` —— 把 orchestrator 绑到**当前**持有领导权的节点，
而不是强迫领导权回到固定节点。3 次连跑 84.6s 全绿。

## 根因 3 · 资源分配不均（外审 B1 建议的 admission control）

`TestAllPhases` 失败时同一行就写着答案：`worker 6, cpus 12,13,56,57` —— **2 个物理核**。
而它内部要**串行**跑 11 个 phase 子进程，每个都是 `-race` 二进制加嵌入式 NATS。
串行 `make e2e` 下同一个 phase 独享整机。

后果：p13 的 `TestProxyFalseOnlineRecoversAfterTunnelDrop` 等满 15s 仍未清除标志，
报出 `Defect B: false-online persists` —— **读起来完全像产品回归**。

实测对照：隔离跑 8 次，**0.81–1.38s**，方差极小。**工作没变慢，是机器变少了。**

> 该测试注释里写着"15s（原为 5s）"——上一次有人遇到同样的问题，
> 用的正是外审警告的那个办法：把超时调大。

**修复**：`reserveHeavyWorker()` 把 3 个同 NUMA 节点的 worker 合并成一个**宽 worker**
（本机 6 物理核 / 12 逻辑 CPU），无法拆分的整矩阵走专用队列调度到它，
跑完自己的队列后再回来帮忙跑轻单元。

效果：`TestAllPhases` 从 2 核 2m6.9s **FAIL** → 6 核 1m49.9s / 1m50.2s **两轮 PASS**，
p13 那个 flake 消失。

## 根因 4 · 端口 TOCTOU

`findFreePort()` 绑定、读端口号、关闭、返回数字。**返回到使用之间任何进程都能抢走它。**

表现：`TestTunnelReconnectTransientDenyRetries` 在满载下 **0.17s** 失败，
报 `broker denied REGISTER: public_port_bind_failed`，而失败行是 `newReconnectHarness`——
**harness 还没搭好**，与"隧道重连"毫无关系。

失败得快是关键线索：该测试的断言预算是 3s/5s，0.17s 说明是硬错误而非超时。

**修复**：换端口重试（最多 8 次）。等待解决不了端口被别人拿走。
4 次连跑 15.1s 全绿。

**未覆盖**：`findFreePort` 在 6 个测试文件里使用，本次只给 reconnect harness 加了重试。
其余调用点同样暴露在这个竞态下——**登记为未做**，见下。

---

## 验证

| 项 | 结果 |
|---|---|
| `internal/broker` + `internal/cluster` | ✅（对齐后 254.9s，未变慢） |
| gated 套件 d3 / d4 / d5 / d7 / d8 | ✅ 22.7s / 72.4s / 82.2s / 28.9s / 21.1s |
| d7 `adminForLeader` 三次连跑 | ✅ 84.6s |
| tunnel reconnect 四次连跑 | ✅ 15.1s |
| 门禁变异验证 | ✅ 注入 → 精确点名 → 恢复转绿 |
| 满载三轮（宽 worker 前） | 300 单元 / 1 失败（p13） |
| 满载三轮（宽 worker 后） | 300 单元 / 2 失败（d7、tunnel，均已修） |
| 满载三轮（全部修复后） | ✅ **300 单元 ALL PASS，零失败**（14m8.9s） |
| `make lint` | ✅ 0 issues |

## 仍未做（登记，不掩盖）

1. **`findFreePort` 的其余 5 个调用点**未加重试，同一 TOCTOU 仍暴露。
2. **宽 worker 的宽度是常数 3**（合并 3 个 slice）。这是按本机 43 核 / 20 worker 定的，
   不是自适应的——换台机器可能需要重新校准。
3. **满载 flake 的样本量仍然小**（三轮 300 单元）。这一条**不构成保留串行闸门的理由**：
   串行那"多年绿色史"恰恰是问题本身——它多年全绿，而这四类缺陷一直在里面。
   用户已裁定 `make e2e-parallel` 为唯一闸门、**严禁全量串行**；样本量继续积累即可。
