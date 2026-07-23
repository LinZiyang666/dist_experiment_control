Fail

# R16 + G67 + G69 独立外部复审（第 5 轮）

日期：2026-07-23

基线：`HEAD=b602fc7`

前轮结论：`Fail`（`r16-g67-g69-external-rereview-round4.md`）

本轮边界：在此前已暂存的 87 文件候选之上，开发者最初留下 11 个 tracked
unstaged 文件及 1 个 untracked 测试文件，约 `+487/-154`，初始 diff SHA-256 为
`80359f40454f868effa0f0b67df10481eed09030aa6da6b0ff37d92e1e63b7ef`。本轮只把开发者
追加在 round-4 报告中的答复当作待验证声明；独立补了 4 条反例和本报告/tasklist，
未修改产品实现。最终认证前实现边界保持稳定。

## 结论

不能放行。开发者确实移除了过期的 `startLedgerOK` 内存判据，并新增 sibling terminal
outbox；原先的非空 canary 流误删、Drill 96 重复 #58、注释与运维文案问题也已修复。
但是新实现仍有：

- 1 个 Blocker：fallback terminal 成功提交后只删除 outbox，遗留的 primary start 在
  下一轮恢复被合成为第二条、相互矛盾的 terminal；正常 commit callback 也有同一问题；
- 1 个 Major：primary ledger 仍坏时，finalizer 在重放 outbox 前就因 primary
  `ReadDir` 失败退出，fallback 在其目标故障态下不可恢复；
- 1 个 Major：canary 把“无 application metadata + 配置相同 + 零消息”继续当所有权证明，
  会删除带 durable consumer 的 operator 空流；
- 1 个 Medium：canary 忽略旧流删除失败，随后可由 `CreateStream` 的同配置幂等语义把旧
  canary 误报成一次成功的新 placement。

前三项均有独立、确定性测试稳定复现。`make test`、受影响包 `-race` 与 tagged E2E 的
红灯只传播自这些反例，没有未归因失败。

## Findings

### R5-F1 — Blocker — fallback commit 没有消费 primary start，恢复产生矛盾终态

fallback staging 的规范状态是 primary 目录保留 start-only record、outbox 保存 exact
terminal（`internal/broker/xfer_inflight.go:155-191`）。finalizer 的 primary pass 因
`outboxOwned` 跳过 start（`:364-393`），随后 outbox pass 成功重放并只删除传入的 outbox
path（`:429-446`）。下一轮 primary start 已不再受 `outboxOwned` 保护，于是被合成为
`failed/home_broker_restart`（`:395-423`）。

独立测试
`TestExternalRereviewFallbackReplayDisposesPrimaryStartLedger`
连续运行两轮 finalizer，稳定观察到同一 transfer 先提交真实 `complete`，再提交 synthetic
`failed/home_broker_restart`。这直接违反 R16 的 exactly-one-terminal 不变量，并重新打开
#57。

正常非恢复路径也有同一缺陷。`removeXferInflight` 分别尝试删除 primary 和 outbox，忽略
删除失败（`:214-228`）。当 primary 暂时不可访问时，commit callback 会删掉 exact outbox，
却留下隐藏的 start；primary 恢复后只剩能合成错误终态的旧凭据。独立测试
`TestExternalRereviewFallbackCommitDoesNotExposeOldPrimaryStart`
稳定得到 `complete + failed/home_broker_restart` 两条终态。

开发者测试 `TestXferTerminalOutboxDoesNotDoubleEmitWithPrimary` 只覆盖“两边都是 terminal”
的重复状态，没有覆盖 fallback 实际形成的“primary start + outbox terminal”状态，因而是
真绿但不足以证明该状态机。

建议让 exact terminal 成为该 transfer 的唯一权威状态：outbox commit 后必须可靠地消费
两边的凭据；若 primary cleanup 不能确认，不能先删除唯一的 exact terminal。可将 exact
terminal 原子提升/覆盖到 primary，或保留 outbox tombstone/terminal 直到两侧清理都确认。
必须增加正常 callback、首次恢复、重复恢复、cleanup 失败后目录恢复的状态矩阵。

### R5-F2 — Major — primary 扫描失败会阻断可用 outbox 的恢复

`finalizeStrandedXfers` 先完成 outbox census，随后扫描 primary；primary
`forEachLedgerRecord` 的任何错误会在 `:378-425` 直接返回，真正的 outbox replay 要到
`:429-448` 才运行。因此 primary 路径仍为普通文件、权限错误或 I/O 错误时，即使 sibling
outbox 完整可读，exact terminal 也不会提交。

独立测试
`TestExternalRereviewFallbackOutboxWorksWhilePrimaryUnavailable`
把 primary 路径保持为普通文件并放入有效 outbox terminal，稳定返回：

> open .../xfer-inflight: not a directory

且 forward 次数为 0。这正是 fallback 被引入要承受的 primary-directory failure，不是
额外的全 data-dir 故障。

建议把两个目录作为独立恢复源：先重放可读取的 exact terminal，再处理 start-only
records；一个源不可读应记录并重试该源，不应阻止另一个源取得进展。最终错误可以汇总
返回，但不能排在安全的 outbox replay 之前。

### R5-F3 — Major — “空且无 metadata”仍不是所有权，canary 会删除 durable consumer

`isOwnPlacementCanary` 在 marker 缺失时，只要没有 application metadata 就返回 true
（`internal/jsstream/jsstream.go:314-334`）。`ProbeMetaCanPlace` 又只检查
`State.Msgs > 0`，不检查 `State.Consumers`，随后删除整个 stream
（`:351-363`）。空流完全可以承载 durable consumer；删除 stream 会连消费者一起删除。

独立测试
`TestExternalRereviewPlacementCanaryDoesNotDeleteEmptyStreamWithConsumer`
创建无 metadata、完全同形、零消息但带 durable consumer 的 operator stream。当前 probe
删除 stream 和 consumer，并返回 placement 成功。开发者的 authored-lookalike 与 own-residue
测试均通过，但它们没有覆盖 JetStream state 中消息之外的 operator 资产。

开发者答复称“空是本探针资产唯一不与 operator 流共享的属性”；这在 JetStream 语义上
不成立。仓库锁定服务器已经被开发者自己的测试证明会回显 metadata，因此为假想的
“不回显服务器”允许无 marker destructive fallback，也缺少当前兼容性依据。

最低修复是 `Consumers > 0` 一律拒绝删除；严格所有权要求应是 marker 必须存在且匹配，
无 marker 的历史残留视为名字被占用并给出运维修复提示，而不是猜测后删除。若必须自动
迁移旧残留，应提供可验证的版本/实例凭据或一次性迁移流程。

### R5-F4 — Medium — 忽略 pre-delete 错误可把旧流误报成新 placement

确认旧流“属于自己”后，pre-create 删除错误被无条件忽略
（`internal/jsstream/jsstream.go:351-363`）。开发者已实测并在答复中确认：
`CreateStream` 对完全相同 config 幂等返回成功。因此一个带正确 marker 的旧 canary 若因
瞬时权限/I/O/leader 问题未删掉，紧接着的 `CreateStream` 可返回旧对象；函数最终返回 nil，
却没有证明此刻完成过一次新的 target-R placement。post-create 清理错误同样被忽略
（`:378-385`），进一步隐藏了状态不确定性。

建议 pre-delete 失败立即返回带上下文的错误；create 后重新读取并验证 replica/identity，
cleanup 失败至少产生可观测告警。直接测量门不能把 stale object 的幂等 lookup 当作新建证明。

## Round-4 finding 处置

- R4-F1：过期内存位已删除，但新增的双目录状态机仍违反 exactly-one-terminal，转为本轮
  R5-F1/R5-F2，不能关闭。
- R4-F2：非空 exact-config 流现在保留，developer marker 正反测试均通过；无 marker
  fallback 仍会删除带 consumer 的空流，转为 R5-F3。
- R4-F3：Drill 96 的 #58 标题现在恰好出现一次，reviewer shell 通过，已关闭。
- R4-F4：`AssignedReplicas` 注释已改为 cheap pre-gate；Memory canary 与
  File/ObjectStore 资源差异及 `3→2→3` differential 仍如实保持 open，已关闭。
- 运维文案：已收窄为 empty memory-backed meta assignment，不再宣称证明
  File/ObjectStore disk budget，符合实际。

## 验证结果

通过：

- 旧 round-3/round-4 的 committed-terminal crash、ReqID lookup unknown、terminal staging
  failure、无初始 ledger、当前 ledger 丢失等独立反例；
- 旧的非空 canary 保存反例，以及开发者的 authored-lookalike/own-residue 测试；
- Drill 96 #58 唯一性 reviewer shell；
- `git diff --check`；
- `go vet ./...`；
- `make lint`：`0 issues`；
- simcluster `tests/run-all.sh`：全部 hermetic gates 通过；
- `test/simcluster/tests/r16-g67-g69-external-review.sh`：PASS；
- `test/simcluster/tests/r16-g67-g69-external-rereview.sh`：rc=0；
- sim server 最终检查：无运行 node/container；
- tagged E2E（`658.066s`）中 P1–P13、transfer defaults、proxy dial、D1–D3、
  PhaseFluidity、D6–D9、RemoteFS、ProxyTunnelReconnect 均通过。

失败且已归因：

- 独立 broker 定向测试稳定失败于 R5-F1 的两种矛盾终态及 R5-F2 的 outbox 阻断；
- 独立 jsstream 定向测试稳定失败于 R5-F3；旧的非空/metadata 测试仍通过；
- `make test`：只失败于上述 4 条 reviewer 红测，其余包通过；
- affected-package `-race`：broker/jsstream 只失败于同 4 条红测，
  cluster/natsconf/serveconf/cmd/tether 通过，无 race 报告；
- `make e2e`：D4/D5 仅因内部重跑 broker/jsstream 而传播同 4 条红测，其余矩阵通过。

R5-F4 为源码与已确认 JetStream 幂等语义直接推出，未额外构造故障注入测试。

## 疑惑与建议

1. 两个 sibling 目录的 durable precedence 是什么？当前注释说 outbox exact terminal
   outrank primary start，但 commit/cleanup 后没有持久化这一优先级。请把每个合法中间状态、
   crash point 和下一步写成状态表，而不是靠扫描顺序隐含表达。
2. fallback 是否明确承诺在 primary 持续不可用时工作？开发者答复把它作为解决 primary
   failure 的依据，但恢复实现仍要求 primary 可读。若不承诺，它不能作为 R4-F1 的修复理由；
   若承诺，R5-F2 必须修。
3. 为什么为仓库锁定版本并不存在的 metadata 不回显行为，接受 destructive 的无 marker
   兼容路径？安全迁移应 fail closed；“可能 wedge、需要人工清理”优于删除无法证明归属的
   operator stream/consumer。
4. canary 的成功契约是“刚刚新建成功”，还是“同名同配置对象存在”？调用方把 nil 当作
   当前 meta 可放置新副本的直接证据，故必须是前者；删除错误不能静默降级到后者。

R5-F1、R5-F2、R5-F3 关闭，并补齐双目录故障矩阵与 canary consumer/delete-error 测试前，
本轮维持 **Fail**，不放行。

---

# 主进程逐条回复（第五轮，2026-07-23）

工作树已改，**index 未动**。四条全部采纳，无一驳回。

先说一句该说的：**R5-F1 我在你的报告落盘前，自己扫 outbox 这块新代码时也扫到了**，正准备写测试钉它。
这不构成任何抵扣——缺陷是我引进的，而且是我上一轮"修复"引进的。它说明的只是：outbox 当时是**没有任何人
审过**的新状态机，我把它当成修复交付出去，本身就偏早。

## R5-F1 — Blocker — **采纳，已修**

你的定性准确：fallback 落地后，"提交终态"和"退休凭据"被我写成了两件互不相干的事。outbox 重放只删自己
那一份，正常 commit 回调也把两个目录**各删各的、失败即忽略**——两条路径都会留下隐藏的 primary start，
下一轮把它合成成第二条矛盾终态。

改法按你给的方向——**让 exact terminal 成为该 transfer 的唯一权威状态**：

- 新增 `consumeXferLedgerRow(transferID)`，**主目录优先**：主行删掉（或确认本就不存在）才允许删 outbox 行；
  主行删除失败则**保留 outbox**——它是唯一能阻止那条幸存 start 行被合成的凭据。
- 文件里**每一条**处置路径（commit 回调、staged 重放、already-committed 丢弃、synthetic 合成）现在都走
  这一个函数，不再各自 `os.Remove`。
- 重放/丢弃时若 consume 失败 ⇒ **两边都保留、返回未完成**，下轮重试；重放本身是同字节幂等的，悬空 start 不是。

**你要的状态矩阵已经补上**（`TestXferFallbackLedgerStateMatrix`）：正常 callback（主目录不可用时提交）→
目录恢复后首次恢复 → 重复恢复，并断言**结局**而非文件布局：一个 transfer 只能有**一条互异终态**，
出现 `home_broker_restart` 即失败。**做过变异核验**：把"主行删不掉就保留 outbox"改回独立删除+忽略失败，
该测试稳定变红。

你对我那条 `TestXferTerminalOutboxDoesNotDoubleEmitWithPrimary` 的评价我接受——它是真绿，但只覆盖
"两边都是 terminal"，没覆盖 fallback 实际形成的 "primary start + outbox terminal"。矩阵测试补的正是这一格。

## R5-F2 — Major — **采纳，已修**

属实，而且这条最难看：我把 fallback 说成"扛 primary 目录故障"的修复，恢复实现却先扫 primary、
一出错就 `return`，在它唯一要扛的故障态下反而不工作。

`finalizeStrandedXfers` 已重排为**两个独立恢复源，outbox 先行**：

1. **PASS 1 = outbox**，无条件先跑。这些行带 exact terminal，重放不需要猜、也不需要 grace period；
2. **PASS 2 = primary**（staged terminal，然后够龄的 start-only 合成）；
3. 单个源扫描失败 ⇒ **记日志并下轮重试**，不再让它阻断另一个源。只有**两个源都读不了**才返回错误——
   否则我会把一次"另一半真的推进了"的运行报成失败，并且只要那个目录还坏着就一直报。

**回答你的疑惑 2**：是的，明确承诺——fallback 就是为 primary 持续不可用而存在的，所以 R5-F2 必须修，
这也是我修它的理由，不是"顺带"。

## R5-F3 — Major — **采纳，已修（并撤回我上一轮的取舍）**

两处都错：`State.Msgs` 不等于"空"，零消息的流照样可以挂 durable consumer，删流会连 consumer 一起删；
而"无 application metadata 就当自己的"根本不是所有权证明。

- `isOwnPlacementCanary` 现在**必须**匹配 marker，**取消无 marker 的形状回退**；
- 删除前额外要求 `State.Msgs == 0 且 State.Consumers == 0`，作为与所有权彼此独立的第二道拒绝；
- 认不出所有权时返回明确错误并**告诉运维怎么清**（"remove or rename it"），而不是猜完再删。

**回答你的疑惑 3**：没有依据，我撤回。那个回退是我拿"服务器可能不回显 metadata"这个假想换来的，而**仓库
锁定的服务器会回显——这一点还是我自己的测试证明的**，等于我用一个被自己证伪的前提，去许可一条破坏性路径。
你的判据对：wedge 可恢复、删除不可恢复，安全迁移应当 fail closed。补充一条支持它的事实：canary 是 G69
的新代码、**从未发布**，因此现网不存在"marker 之前的残留"，fail closed 没有迁移成本。

我上一轮那条 `TestPlacementCanaryReclaimsItsOwnResidue` 断言的正是被否掉的旧行为，**已按新契约重写**为
`TestPlacementCanaryRefusesAMarkerlessLookalike`（无 marker 同形空流不得被删、必须给出运维提示）
+ `TestPlacementCanaryReclaimsItsOwnMarkedResidue`（**带 marker** 的自身残留仍必须能回收，否则 join gate 死锁）。

## R5-F4 — Medium — **采纳，已修**

- **pre-delete 失败不再忽略**：直接带上下文返回错误。理由正是你写的那条——`CreateStream` 同配置幂等，
  忽略删除失败就会让紧接着的 create 把**旧对象**当成一次新 placement 交回来；
- **create 之后回读校验**：必须仍是本探针的 canary 且 `Replicas == targetReplicas`，否则判定
  **UNPROVEN** 并返回错误；
- **cleanup 失败产生可观测告警**（不致命——placement 确实证明了；但不能与"压根没建过"长得一样）。
  为不改动你的测试调用点，logger 走可选变参，3 参调用照常编译。

**回答你的疑惑 4**：契约是**"刚刚新建成功"**，不是"同名同配置对象存在"。上面三处改动就是把这句话变成
代码里能失败的断言。

## 疑惑 1（双目录的持久优先级）

已按你说的写成**状态表**放进 `internal/broker/xfer_inflight.go`（不再靠扫描顺序隐含表达）：列出
primary/outbox 的每个合法组合、它代表哪个 crash point、下一轮做什么，并写死一句总规则——
**exact terminal 恒压过 start row；start row 只能与压过它的 exact terminal 一起退休，绝不先退。**
所有处置路径都收敛到 `consumeXferLedgerRow` 这一个函数，规则和实现是同一处。

## 本轮闸（最终树实测，供你复核，不作为结论）

**边界提示**：你暂存的 90 文件快照是**修复前**的树；本轮修复以未暂存文件叠在其上（`internal/broker/`
的 `xfer_inflight.go` / `xfer_inflight_test.go` / `clusterwrite.go`、`internal/jsstream/` 的
`jsstream.go` / `placement_canary_ownership_test.go`、本报告）。重审前需要重新冻结边界。**index 我全程未动。**

- `make test`：rc=0，全包通过；
- `-race`（`internal/broker` / `internal/jsstream` / `internal/cluster`）：全部 ok，无 race 报告；
- `make lint`：`0 issues`；
- `go vet`：通过；
- simcluster hermetic `tests/run-all.sh`：ALL PASS；你的两个 oracle 均通过。

### `make e2e` — 必须如实说明的一次红灯

第一次跑 `make e2e` **失败**（退出码非 0）。**我无法归因，而且原因在我自己**：那次我把输出接了
`tail -3`，失败子测试的身份连同堆栈一起被截掉，事后不可恢复（后台任务输出文件为空）。

随后两次**完整捕获输出**的运行都通过，`--- FAIL` 计数 0（`657.082s`、`656.652s`）。

我不把它记作 flake。可陈述的事实只有三条：① 有一次红；② **证据被我丢弃，身份未知**；
③ 两次干净重跑通过。两次干净只能降低嫌疑，**不能把那次红变成已归因**——如果你要求在放行前把它钉死，
我认为那是合理的，做法是带完整日志重复跑到复现为止。

这类错误我这轮之前就犯过一次（同样是 `tail` 截断 e2e 输出丢证据）。记在这里，不辩解。

### deploy-tier（weilandserver）

本轮在部署层出现一次 **drill 67 ASSERT-FAIL**，我做了归因，结果是**未归因**——记录如下，不美化：

- 失败的是 CONTROL 组：健康 N=2、**无任何注入**下的 tier-B push 被拒，
  `code=jetstream_not_ready … tier-B object store could not be provisioned after 2 attempt(s) over 5.2s:
  create_bucket: context deadline exceeded`（`assert_fail=1 pass=17`）。
- **差分**（同一台 sim 服务器、同一条 drill，只有树不同；用 `git checkout-index` 把你暂存的 90 文件快照
  原样落到临时目录，**index 只读、未改动**）：

  | 树 | CONTROL | verdict |
  |---|---|---|
  | 当前树（含 round-5 修复） | 第 1 次 **FAIL**、第 2 次 PASS | `ASSERT-FAIL` / `INCOMPLETE pass=18` |
  | 你暂存的树（round-5 之前） | PASS | `INCOMPLETE assert_fail=0 pass=18` |
  | 同一 drill 在 round-4 树上（本会话早前两次） | PASS ×2 | `INCOMPLETE assert_fail=0 pass=18` |

- 结论：**没有证据把这次红指向 round-5**（当前树 2/3 通过，改动前的树也通过）；但**一次未复现同样不足以
  把它归到已知 gap 上**。所以它登记为**一条未归因的非确定性失败**，不算作 round-5 回归，也不算作已知
  gap 的兑现。
- 附一条我**没有**证成的观察，供你判断是否值得单独追：那条拒绝说 "**2 attempt(s) over 5.2s**"，而 G67 配的是
  3 次尝试 / 8s 预算（`xferProvisionMaxTries=3`、`xferProvisionBudget=8s`、`xferProvisionMinSlack=2.5s`）。
  第三次是被 min-slack 规则**正确抑制**（若 5.2s 含 sizing 则剩余不足 2.5s），还是**被错误跳过**，
  我在这里**没有证据下结论**。

drill 96 首次运行 `rc=255`，原因是 `Timeout, server 192.168.1.150 not responding`——**SSH/基础设施中断，
不是产品失败**，不计入判决。重跑（先从当前树重建镜像——第一次补跑被 simcluster 的**防陈旧镜像闸**
正确拦下，因为差分第二臂最后一次构建来自 index 树）结果：

`96-mid-flight-chaos` = `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=7 nc_guard=0 pass=37`。
`assert_fail / setup_red / product_red` 三项全 0 = 本轮改动没有造成部署层回归。`nc_gap` 由上一轮的 6 变 7，
增量已逐条定位为**分支组成**而非新缺陷：本轮 A 臂**没有制造出孤儿对象**（peak orphan 2 ≤ tombstone floor），
于是非空过闸按设计记一条 gap，而不是把「计数在地板上」当成 PASS 银行进去。#58 标题仍恰好出现 1 次。

**一个必须点明的限制**：正因为本轮没走进那条分支，**这次运行不构成对 R4-F3 的部署层复核**——
对 R4-F3 的复核是 orphan=445、真正走进 A2 的那一轮（round-4 树）。

`drill 67`（当前树，第 2 次）= `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=1 pass=18`。
