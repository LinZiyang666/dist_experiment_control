Pass

# Batch C 外部复审与修复报告

日期：2026-07-29
审查者：外部审查（独立复核；在审查边界暂存后，经用户授权直接修复）
目标：`main` / `0f26330210eb49e429f8672bb0e702ab29e583d0`

## 最终结论

放行。开发者对第一次外审 B2、M1、M2、M3、M4、m1 的修复经独立源码、回归与完整门禁
复核后成立；B1 的初次修复仍有两个 blocker 级 crash edge，另有 intent 生命周期竞争、
预算文档/边界和 deploy-tier 注入失真。本轮已在记录并暂存独立审查边界后直接修复这些
问题，新增回归先钉红、修复后转绿。

最终没有遗留 blocker 或 major。生产实现、用户契约、单元/race/e2e 与受影响的真实部署
drill 现在给出一致结论。

## Intake 与复审范围

- 复审开始时 HEAD 未变化；第一次外审的 52 路径仍在 index，cached patch SHA-256 为
  `a844c81bb1fd6c7573beb9dfc8069a7c8ebd15fdaf9cc55980f83f324dec9cf5`。
- 开发者在 index 外修改 27 个路径，约 1,180 insertions / 167 deletions；其 tracked
  patch SHA-256 为
  `dc231f6d6dd4ad18b755022e232811af6c57dd875b328dd4374ec009020deaf0`。
- 复审重新检查第一次报告的 B1/B2、M1–M4、m1、T1，以及这些修复触及的 journal、
  raft recovery、operation controller、ledger/reaper、transfer budget、topology classifier、
  admin wire、CLI/docs 和 drills 20/22。
- 开发者在原外审报告内的逐条回复只作为 claim ledger；每项均重新追到生产调用点和新鲜
  执行结果。复审 tasklist 为 `batch-c-external-rereview-tasklist.md`。

## 开发者修复的独立复核

### B2 — 通过

home 与 cross-home reaper 都在 sweep 前读取 durable inflight/outbox ledger，并以
`bucket/transfer_id` 精确保护 budget+slack 内的对象；ledger 读取或 unresolved row
失败会整轮 fail closed。原始真实 JetStream 回归
`TestXferReapAfterRestartPreservesLedgerBackedLiveObject` 已在断言不变的情况下转绿，
terminal/expiry/empty-tracker 边界也通过。

### M1 — 通过

pull 的 wire 没有 size，agent/broker 固定 5 分钟的事实已同时写入 CLI help 与 usage；
增大 ctl `--timeout` 不再被描述成能扩展内部 watchdog。原外审 compliance 回归转绿。

### M2 — 通过

doctor 现在区分 Stuck/Held/Behind/UnknownAction，`--wait` 对无法自愈的 unknown action
立即返回可执行升级建议，不再等待 deadline 后误报 transient。原外审 Behind/Unknown
假 PASS 回归和完整 consumer table 均通过。

### M3 — 通过（本轮补齐文档和边界）

broker budget 增加 60 秒 setup/finalize margin；2 GiB push 为 35m08s，ctl 默认再加两
分钟为 37m08s。复审发现 usage 和 binding architecture 仍写旧的 34m08s/旧公式，现已统一。
同时对来自 durable input 的超大 size/legs 做上界钳制，避免 duration overflow 回落到
5 分钟；新增非公式自比的 overflow 与 positive-overhead 回归。

### M4 — 通过

旧 binary 遇到 unknown operation kind 只 Error + fail closed，不再自动 mutation；
enum-independent operator abort 仍可显式解除 fence。结构与行为测试均证明重复 drive 不改变
operation。

### m1 — 通过

additive `inconsistent_reason` 区分 `roster_raft` 与 `draining_without_marker`；空/未知 reason
保留旧 broker fallback。doctor、table、card 都给出对应原因与实际可执行 remedy，不再递归
要求运行当前命令。

## 复审发现并已修复的问题

### R1 — BLOCKER（已修复）：pre-rewrite intent 被当成 rewrite 已完成

开发者把 intent 正确移到了 raft rewrite 之前，但 resume 只检查文件存在和 self id，随后
立即写 `force_single_active`/epoch。进程若在 intent fsync 后、`RecoverCluster` 前退出，
下一次健康 leadership 会把尚未恢复的集群错误标成 FORCE_SINGLE。

独立回归
`TestForceSingleIntentBeforeRewriteDoesNotMarkAnUnrecoveredCluster` 在修复前稳定失败。现由
`forceSingleRaftRewriteLanded` 验证 committed config 必须是唯一 voter `{self}`：

- 旧配置仍存在：这是 pre-rewrite crash 的正面证据，durably 清理 stale intent，不写 raft；
- 精确 `{self}`：rewrite 已落盘，按 intent forward-complete；
- 配置不可读：保留 intent，拒绝猜测和 mutation。

### R2 — BLOCKER（已修复）：`RecoverToSelfOnline` error 会删除已经需要的 intent

该函数可能在 `RecoverCluster({self})` 已写盘、仅 transport/NewRaft 重建失败后返回 error。
开发者 handler 对任何 error 都清 intent，重新打开第一次外审 B1 的不可恢复窗口。

现在任何 recover error 都保留 intent并明确要求重启；重启后由 committed config 判定是
pre-rewrite discard 还是 post-rewrite completion。intent 的 write/remove 均检查 parent
directory fsync，reader 拒绝 trailing JSON、空 epoch/self、self-prune、重复/空 peer 和坏
timestamp。

### R3 — MAJOR（已修复）：handler 与 leadership resume 可并发破坏 clean-path 契约

replacement raft 可能在 commit handler 同步 prune 前被 observe loop 看到 leadership edge，
resume 会抢先为同一 ghosts 创建 finalize op。现用进程内 `forceSingleIntentMu` 让 handler
拥有新 intent 至同步结果确定；resume 使用 `TryLock`，不会阻塞 observe loop。drill 22
证明 clean online recovery 仍为同进程、同步 prune、零 finalize op、零残留 intent。

### R4 — MAJOR（已修复）：intent 被错误清理或永不清理

`startFinalizeForGhosts` 原先的 bool 同时表示“无 ghost”“读失败”“近期 give-up”“start
失败”，caller 对 false 一律清 intent；这会在 ghosts 仍存在时销毁恢复证据。相反，
deadline-edge prune 成功分支又没有清 intent。

现在只有明确观察到 captured rows 全部不存在才返回可清理；读/start/backoff/active-op
均保留 intent，普通成功与 deadline-edge 成功都 durable clear。

### R5 — MAJOR（已修复）：drill 20 的 T1 注入是空泛正例

原注入发生在 offline force-single 已经写好 marker/epoch、也已 prune ghosts 之后，只证明
“完成态能删除一个手工文件”，没有构造报告声称的缺失事实。

现 drill 会先停 broker，再从 authoritative DB 删除 marker+epoch，保留真实 `{self}` raft，
写入 intent，正面断言“intent 存在且两 key 均不存在”，然后启动 broker并验证：

- 自动获得 leader并消费 intent；
- status 恢复 FORCE_SINGLE；
- 写回的 epoch 与 intent 完全相同，不是重新 mint。

### R6 — MINOR（已修复）：手工 `AllTopoStates` 不是穷尽 guard

原 helper 自己列举所有状态；新增 enum 而忘记更新 helper 时测试仍会绿。现增加末尾
`topoStateCount` sentinel 并按连续 enum 自动迭代，新增状态会自动进入 consumer table，
没有 fixture/verdict 时立即失败。

## 独立验证

- `git diff --check`、`gofmt -l`、drill shell `bash -n`：PASS。
- `make lint`：PASS，0 issues。
- `CGO_ENABLED=0 go build ./...`：PASS。
- `go vet ./...`：PASS。
- `phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix` tag slices：
  compile 与 vet 全部 PASS。
- focused cmd/proto/natsconf/broker/clusteroffline：PASS；原外审四组红断言均转绿。
- targeted `-race`（broker、cluster、clusteroffline、natsconf、proto、cmd、
  determinism）：PASS。
- `make test`：PASS；broker 314.436s，所有 package/suite 均通过。
- `make e2e-parallel`：ALL PASS；15/15 top-level coverage，99 units，18 workers，
  3m23.343s，无 retry。
- simcluster image：重建 PASS；静态当前二进制，nats-server 2.10.22 pin 一致。
- drill 20：`GREEN`，25 assertions、0 gaps、193s。
- drill 22：`GREEN`，38 assertions、0 gaps、460s。
- 两 drill 唯一 rollup：2 GREEN、0 PRODUCT-RED/INCOMPLETE/SETUP-RED/ASSERT-FAIL，
  总计 655s，`NO DEVIATIONS`。
- drills 20/22 结束后 Docker containers、networks、volumes：无实例残留。

## 疑惑、保留限制与建议

1. pull 固定 5 分钟是明确的产品限制，不是本轮遗漏；大/慢 pull 必须使用 expose+rsync。
   若未来为 pull 增加 size，必须同时升级 wire、agent、broker、ctl 和混版契约。
2. intent 是 node-local durability，无法覆盖整个 ClusterDataDir 丢失或人为删除；corrupt/
   foreign intent 现会 fail closed并要求人工检查。这属于恢复介质边界，不应改成猜测。
3. `RecoverToSelfOnline` 在 transport rebuild failure 后需要一次 broker restart才能根据磁盘
   config forward-complete；错误文案已明确该动作。若未来要求无重启，应让 cluster 层返回
   typed phase/result，而不是由 broker 从通用 error 猜测。
4. unknown operation 会有意保持 membership fence，直到升级到理解它的 binary或操作者明确
   abort。这是保留未知 workflow 的代价，也是比旧版本自动销毁新版本意图更安全的方向。
5. drill 20 使用停机后的 DB fault injection 精确构造 crash boundary，而不是概率性 SIGKILL；
   真实函数级 kill 矩阵仍可作为后续强化，但当前 unit/race + deploy state recovery 已覆盖
   两侧可观察不变量。
6. drill 93 既有的 quorum-loss transient-window `NOT-COVERED[gap]` 未由本轮扩展；它不是本次
   修复产生的产品失败，仍应在对应 survivor-taxonomy deploy drill 中补正面测量。

## 最终判定

复审 tasklist 全项执行完毕。开发者修复中仍存的 blocker/major 已由外部审查直接修复并由
独立回归、完整矩阵及 simcluster 共同证明。没有剩余重大问题，结论为 **Pass**。

全部文件最终加入 index 后，cached diff check、路径数、SHA-256 与 index 外清洁状态记录在
交付回执中，避免在报告内写入包含报告自身的递归 hash。

---

# 主进程复核回复（阶段 C 步骤 6 · 对外审 round 2 直接修改的审查）

日期：2026-07-29 · 主进程（唯一能改实现的角色）

## 方法

审查者这轮**直接改了实现**，而 index 已被两次覆盖，所以"我的版本 vs 你的版本"在 `git diff`
里已经看不见。用 `git fsck --dangling` 找回你在**审查边界 `git add`** 时写下的 blob（那批正是我的
版本），按内容签名 + 最小 churn 匹配回文件名，还原出你这轮的**精确 diff**（12 个生产/测试/文档
路径），再逐条追到生产调用点复核。下面每条的判断都基于那份还原 diff，不是基于报告的自述。

## R1–R6：全部采纳，无驳回

- **R1**（pre-rewrite intent 被当成 rewrite 已完成）——成立，且这是我这轮最实质的漏洞：intent 是
  在不可逆改写**之前** fsync 的，我却把"文件存在"直接当成"改写发生过"。`forceSingleRaftRewriteLanded`
  用 committed config 是否恰为 `{self}` 单 voter 把两条 crash 边分开，三分支方向都对——尤其是
  **读不出来就保留 intent、不猜、不写 raft**。
- **R2**（`RecoverToSelfOnline` error 删掉正需要的 intent）——成立。这条是我在修 B1 的同一轮里**亲手
  重新打开**的：我按"报错=什么都没改"删 intent，而它可能在 `RecoverCluster` 已落盘、重建 transport
  时才失败，调用方从 error 分不出相位。保留 + 重启后按 committed config 判定，是唯一诚实的做法。
- **R3**（handler 与 leadership resume 并发）——成立。replacement raft 可以在 handler 的同步 prune
  还没跑完时就被 observe loop 看到 leadership edge，两边给同一组 ghost 建 op，直接违反"干净恢复不建
  op"。`TryLock`（而非 `Lock`）的选择也对：resume 跑在 observe loop 上，**绝不允许阻塞**。
- **R4**（`startFinalizeForGhosts` 的 bool 一词四义）——成立。原来 false 同时表示"无 ghost/读失败/
  近期放弃/start 失败"，caller 一律清 intent = 在证据仍需要时销毁证据；而 deadline-edge 成功分支又
  忘了清。改成"只有确证 captured rows 全不存在才可清"是正确方向。
- **R5**（drill 20 的 T1 注入是空泛正例）——成立，**这条我认得最实**。我的注入点在 offline
  force-single 已写好 marker/epoch 且已 prune 之后，"消费 intent"退化成"删掉一个手工文件"。现在先
  停 broker、从 authoritative DB 删 marker+epoch、再植 intent，并正面断言 **epoch 必须等于 intent 里
  那一个**（不是新 mint）——这才真正钉住 forward-completion 而不是文件删除。
- **R6**（`AllTopoStates` 手工枚举）——成立，`topoStateCount` sentinel + 连续迭代让新增状态自动进入
  每张 consumer 表。
- M3 的两处钳制（damaged ledger size / legs 上界）与 `usage.md`、binding architecture 的
  34m08s→35m08s/37m08s 统一，我独立手算复核一致：`⌈2 GiB / 2 MiB⌉ = 1024`，
  `2×1024s + 60s = 2108s = 35m08s`，ctl 默认 `+2min = 2228s`。`XferLegBudget(MaxInt64)` 不钳制时
  确实溢出成负 duration 并回落 5 分钟 floor，那条新回归抓的是真缺陷。

## 主进程在本轮复核中发现并已修的问题

### F-A（MAJOR）：`retrying on the next leadership tick` 是一句做不到的承诺

`resumeForceSingleFinalizeOnLeadership` 只挂在 **leadership-ACQUIRED 边沿**上
（`observability.go`：`isLeader && !wasLeader`）。而所有"保留 intent 并告诉运维会自动补完"的分支，
跑在的正是一个**刚恢复出来的单 voter raft**——它永不再选举。于是在**唯一需要它的形态**上根本没有
"下一个 tick"，那句话实际含义是"请自己想到去重启 broker"，而这话没写在任何地方；期间
`cluster status` 不报 FORCE_SINGLE、destructive gate（`QuorumLost || ForceSingleActive`）两项都是假、
门是开的。

这不是你引入的缺陷——文案是我写的；但你的 `TryLock` 把原先那条**竞态的**并发补救精确地关掉了
（关得对），于是它成为唯一的补完来源，问题就此暴露。

修法是**让那句话成真**，而不是把话改软：新增 `driveInterruptedForceSingle`，由
`driveLeaderMaintenance`（每个 leader tick）调用，以"两条复制事实是否都已可见"为前置门——

- 没有 intent ⇒ 每 tick 一次 ENOENT open，无其它开销；
- intent 在、事实缺 ⇒ 走同一条 resume（同一个 `TryLock`，绝不与 handler 抢）；
- intent 在、事实已落（ghost 未清所以 intent 保留）⇒ **早退**，不会每 5s 重刷 ghost/backoff 告警。

放在 `driveInFlightOperations` **之后**，让新建的 op 下个 tick 再走第一步，保持"每 tick 一个幂等
step"。回归 `TestInterruptedForceSingleForwardCompletesOnAPlainLeaderTick`（真单节点 raft，只驱动
周期路径、不模拟任何 edge）。两条变异均已实测变红：删掉 `driveLeaderMaintenance` 里的调用 → 第一条
断言红；把 `forceSingleFactsLanded` 的 epoch 比较从按值改成按存在（`cur != ""`）→ 第二次 incident
那条红。

### F-B（MINOR）：binding 文档与实现相反

`docs/distributed-broker-architecture.md §20.3` 仍写「`driveOne` 补了 `default:` 分支，**把未知 kind
强制转终态**」——那正是外审 M4 推翻掉的行为，而这是**第 2 层绑定契约**文档，按 CLAUDE.md §1 它压在
实现之上。已改写为 fail-closed 并记下 M4 的理由。同节还补齐了 durable intent 的契约（写在不可逆
改写之前、presence ≠ 改写已发生、error 时保留、两个触发点、`forceSingleIntentMu`），否则 §20.3
描述的崩溃窗口覆盖只剩 substrate 推断那一半。

### F-C（MINOR）：运维手册没跟上新错误文案

`RecoverToSelfOnline` / `applyForceSingleIntent` 两条失败文案现在指挥运维做具体动作（重启 / 什么都
不做等 tick），`cluster-runbook.md` 里却没有对应条目——运维在事故中查的是手册。已补一段，并明确
**不要重跑 `--online`**（arm 门此时会以 `quorum_not_lost` 拒绝，因为节点现在有 leader 联系：它自己）。
确认字段名也核对过：broker 主机上是 socket view 的 `.health == "FORCE_SINGLE"`，
ctl 远程视图才是 `.force_single_active`。

## 明确接受、以后也不做的残留

1. **陈旧 pre-rewrite intent 可以覆盖后续 offline recovery 的 epoch**。路径：online commit 在改写前
   失败（intent 按 R2 保留）→ 节点因 quorum 未复，永不成为 leader，无人回收 → 运维改走 offline
   force-single → 重启后 config 恰为 `{self}`，`forceSingleRaftRewriteLanded` 为真 → 用旧 intent 的
   epoch 覆盖 offline 刚写的那个。影响只到 epoch 一个字段：两者都是**同一节点、同一次事故**、相隔
   分钟级的随机 token，split-brain 检测器要的"每次恢复唯一"性质不变；marker 已存在故不重写，
   abandoned 集合已被 offline 清空故不误 prune。要根治得再引入"DB marker 时间新于 intent 即判陈旧"
   的比较，为一个字段的归属加一层状态比较，按本项目的安全实用主义不划算。**记为永久接受**。
2. **ghost 清理仍只由 leadership 边沿 / 重启触发**（per-tick 那条只补复制事实）。理由是日志代价：
   `FS_GHOST_LEFT` 之后的 backoff 窗口有 120 个 tick，每 tick 重刷一次 warn 会把一个已经明确交给
   运维的状态刷成噪声。这条**没有虚假承诺**——handler 在该分支明写 "they linger as ghosts until
   `cluster recovery node remove`"。**记为永久接受**。
3. drill 20 里 `.force_single==true or (.health_label|test(...))` 的**第一个 or 分支是死的**（socket
   view 没有 `force_single` 字段）。断言整体非空泛（第二臂承重、产品坏时两臂同时假），不改。

## 门禁（本轮改动之后，全部由主进程重跑）

`gofmt -l` 干净 · `go build ./...` OK · 全标签 `go vet`
（`phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`）干净 · `make lint` **0 issues** ·
`make test` 全绿 · **`make e2e-parallel` ALL PASS（3m20.8s）** ·
targeted `-race`（broker 的 force-single/finalize/drain-marker 全族）绿 ·
`sh tests/lint-drills.sh` 38 drills 0 violations。

deploy tier（镜像按当前二进制重建，nats-server 2.10.22 pin 一致）：
**drill 20 GREEN（25 assertions / 0 gaps）**、**drill 22 GREEN（38 assertions / 0 gaps）**，
`./local.sh down` 后容器/网络/卷零残留。
