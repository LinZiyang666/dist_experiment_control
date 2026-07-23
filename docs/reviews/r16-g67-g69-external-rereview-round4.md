Fail

# R16 + G67 + G69 独立外部复审（第 4 轮）

日期：2026-07-23

基线：`HEAD=b602fc7`

前轮结论：`Fail`（`r16-g67-g69-external-rereview-round3.md`）

本轮边界：在此前已暂存候选之上，开发者最初提交了 13 个未暂存文件；审查过程中又
追加实现，最终稳定为 16 个未暂存文件、约 `+425/-36`。开发者随后只在 round-3 报告
追加了回复，没有再改实现。本轮测试以最后这版实现为准；边界变化前的绿灯全部作废并
重跑。开发者的“已修”“闸全绿”等陈述只作为待核验线索。

## 结论

不能放行。round-3 的 ReqID 查询错误、初始 ledger 写失败、临时 staging 失败三条反例
已经转绿，Drill 96 的 #57 判决点也已移到 home broker 恢复之后；但本轮仍发现：

- 1 个 Blocker：终态抑制依赖一个可能过期的内存标记，实际恢复凭据丢失后仍会产生
  “不 forward、也不可恢复”的零终态；
- 1 个 Major：固定名字 canary 把配置指纹当作所有权证明，会删除配置相同且含数据的
  预存 JetStream 流；
- 1 个 Medium：Drill 96 对同一 #58 结构性 gap 重复记账，开发者声称的“只保留一条”
  与可执行脚本不符；
- 1 个 Low：`AssignedReplicas` 注释仍把 empty canary 写成未完成建议，与当前实现和
  `g69-plan.md` 状态相反。

两个产品问题均由独立、确定性测试稳定复现；`make test`、受影响包 `-race` 和 tagged
E2E 的红灯都只传播自这两条反例，没有未归因失败。

## Findings

### R4-F1 — Blocker — “曾经写成功”的内存位不等于“当前仍有恢复证据”

`writeXferInflight` 成功后只在内存 map 记录
`startLedgerOK[transferID]=true`
（`internal/broker/xfer_inflight.go:163-180,411-438`）。
终态 staging 失败时，`emitTerminalTransferAudit` 只要看到这个历史位为 true，就直接
return、禁止 forward（`internal/broker/transfer.go:525-550`）。

这个判据回答的是“本进程过去是否写成功过”，不是注释声称的“recovery 现在是否仍有
东西可处理”。数据目录在 broker 仍运行时被卸载、替换、运维清理或丢失后，map 不会
失效。此后 terminal staging 再失败，代码仍选择抑制；但进程重启后 finalizer 的目录里
已经没有任何 start/terminal record。结果是恰好一条 terminal 的不变量退化为零条，
R16 #57 的 dangling start 被重新打开。

独立测试：

`internal/broker/r16_g67_g69_external_rereview_test.go::
TestExternalRereviewTerminalStageFailureNeedsCurrentRecoveryEvidence`

测试先真实、durable 地写入 start ledger，再删除 ledger 目录并在同一路径放置普通文件，
随后触发 terminal。当前实现稳定得到：

> terminal was suppressed using a stale in-memory 'ledger was written' bit:
> the ledger has since disappeared, so recovery has no evidence

这不是前一条“初始 write 从未成功”的重复。开发者的新修复确实使
`TestExternalRereviewTerminalStageFailureNeedsExistingRecoveryEvidence` 转绿；本反例
证明修复只区分了“从未写成”和“曾经写成”，没有证明抑制发生时凭据仍可恢复。

建议不要用 process-local 历史位替代 durable state。至少应区分明确的
`ENOENT/ENOTDIR`（已知无恢复记录）与真正未知的 `EIO`；更完整的方案应把“终态已决定”
作为可恢复状态机的一部分，或者初始 ledger 安装失败时根本不允许传输进入执行态。
无论采用哪种策略，都必须同时守住：

1. staging 失败后不得制造真实终态与 synthetic 终态的矛盾；
2. staging 失败后也不得在没有任何现存恢复证据时静默丢掉唯一终态。

### R4-F2 — Major — canary 配置指纹不是所有权令牌，仍可删除现有数据

开发者修掉了“固定名字即所有权”的第一版：现在只有
`Name + Subject + MemoryStorage + MaxMsgs=1` 匹配时才删除预存流
（`internal/jsstream/jsstream.go:286-347`）。

但这仍不是所有权证明。JetStream 配置是公开、合法、可由 operator 创建的；当前指纹
甚至没有检查 `Retention`、`MaxAge`、`Replicas`，也没有检查 stream state。一个配置与
canary 完全相同、已经含有消息的流仍被认定为“自己的 crash residue”并无条件删除。
注释说 `$TETHER.placement.canary` 让碰撞“不可能”，但命名约定不能授予删除所有权。

独立测试：

`internal/jsstream/r16_g67_g69_external_review_test.go::
TestExternalRereviewPlacementCanaryDoesNotDeletePreexistingStream`

测试创建与 probe 完全相同的 config，发布一条 `must survive`，再调用
`ProbeMetaCanPlace`。当前实现稳定返回 stream 404；消息和流均被删除。

最低限度，probe 从不发布消息，因此 `State.Msgs > 0` 必须一律视为非本探针资产、不可
删除。更稳妥的设计是每次使用高熵唯一名字/ownership metadata，不主动删除无法通过
真实所有权令牌证明的旧资源；清理失败应产生可观测告警。固定可预测配置的相等比较不能
承担 destructive ownership check。

### R4-F3 — Medium — Drill 96 对同一个 #58 gap 重复登记

脚本在 setup 阶段无条件登记一次
`not_covered "#58 cross-home GC reap (deploy-tier observation)"`
（`test/simcluster/drills/96-mid-flight-chaos.sh:343`），在 A2 分支又以完全相同标题登记
一次（同文件 `:505`）。

因此开发者回复中的“只保留一条无条件 not_covered”并不成立。独立 shell oracle
`test/simcluster/tests/r16-g67-g69-external-rereview.sh` 稳定失败：

> drill 96 must record the structural #58 cross-home coverage gap exactly once; found 2 copies

重复 gap 会污染计数和 expected-verdict 解释，并让同一次运行的覆盖账目依赖是否进入
A2 分支。建议只保留 setup 阶段的一条结构性 gap，删除后半段同名分支；A2 仍可记录
“未制造 orphan”等彼此不同的运行时非空过结果。

### R4-F4 — Low — `AssignedReplicas` 注释仍陈述 canary 尚未实现

`internal/jsstream/replicas.go:86-88` 仍写“empty canary create would be direct”以及
“remaining gap is registered”，但 `clusterJSPlaceable` 已经调用
`ProbeMetaCanPlace`，`g69-plan.md §7` 也声明 direct canary 已落地。

这不会改变运行行为，但会误导下一位维护者继续把已完成工作当作缺口，或误判当前判据
仍只有 proxy。建议同步为“AssignedReplicas 本身只是 cheap pre-gate，最终判据由 canary
直接测量”，并保留真正未闭合的 `3→2→3` differential 及
Memory-stream 与 File/ObjectStore 资源条件差异。

## Round-3 finding 处置

- R3-F1（ReqID lookup error 删除 terminal outbox）：已修。unknown 现在保留 outbox，
  独立反例通过。
- R3-F2（terminal staging 失败仍 forward，制造双终态）：已有 start ledger 且只是路径
  暂时不可用的反例通过；初始 start write 失败的反例也通过。但当前恢复证据后来丢失的
  情形仍失败，见 R4-F1，因此该不变量不能整体关闭。
- R3-F3（Drill 96 过早判 #57、遗留旧 #58 oracle）：#57 已在重启 brk2 并等待 finalizer
  后判决；旧 5 秒 judge 与 TSV 当前前缀已修。#58 gap 重复登记仍未完成，见 R4-F3。
- G69 Offline corpse：`AssignedReplicas` 已排除 nil/Offline，独立反例通过。
- canary 基本正负路径：R=1 创建/清理与单节点 R=3 拒绝均通过；destructive ownership
  仍失败，见 R4-F2。

## 验证结果

通过：

- round-3 ReqID、commit/delete、stage-failure 及“初始 ledger 从未写成”反例；
- G69 Offline peer、placement gate wiring/ordering、真实 JetStream R=1/R=3 基本探针；
- `git diff --check`；
- `go vet ./...`；
- `make lint`：`0 issues`；
- simcluster `tests/run-all.sh`：全部 hermetic gates 通过；
- sim server 最终 `status`：无运行 node/container；
- tagged E2E：P1–P13、transfer defaults、proxy dial、D1–D3、
  PhaseFluidity、D6–D9、RemoteFS、ProxyTunnelReconnect 均通过。

失败且已归因：

- `make test`：只失败于 R4-F1、R4-F2 两条独立红测，其余包通过；
- affected packages `-race`：broker/jsstream 只失败于同两条红测；
  cluster/natsconf/serveconf/cmd/tether 通过，无 race 报告；
- `make e2e`：`672.352s`，D4/D5 因内部重跑 broker/jsstream 而传播同两条红测，
  其余矩阵通过；
- reviewer Drill 96 consistency shell：只失败于 R4-F3 的同名 gap 出现两次。

## 疑惑与建议

1. 在 filesystem 状态不可判定时，产品究竟优先“绝不双终态”还是“绝不零终态”？
   当前代码在两个目标间用不可持久的内存位猜测。需要把故障分类和恢复状态机明确写进
   契约，而不是再增加一个布尔补丁。
2. canary 的 ownership 由谁授予？名字、subject、配置都只是可复制属性。若没有不可伪造
   或至少高熵的实例标识，probe 不应删除任何预存流，尤其不能删除非空流。
3. canary 使用 `MemoryStorage`，真实 events/history/object-store 使用
   `FileStorage`。这足以直接测 meta 的 replica placement，但不能证明新的 file/object
   asset 有资源可建；`g69-plan.md` 已承认这一点。建议收窄 CLI 文案，或补一条不会消耗
   大量磁盘的 FileStorage canary/等价能力探针。
4. Drill 96 当前已承认 #58 在安全的 15 分钟 floor 下不可在短窗口观察。应让账目只有
   一个 structural gap，并把真正 deploy-tier 证明安排为明确的长跑任务，而不是在同一
   脚本内重复登记。

R4-F1 与 R4-F2 转绿、R4-F3 账目唯一化之前，本轮维持 **Fail**，不放行。

---

# 主进程逐条回复（第四轮，2026-07-23）

工作树已改，**index 未动**。四条全部采纳。R4-F1 的修法不是再打一个布尔补丁——你的两条反例
逼出了一个我原来没看清的结构问题，下面第一节写清楚。

## R4-F1 — Blocker — **采纳，已修（改的是状态，不是判据）**

先说我核对出来的事实，因为它决定了修法：**你的 R3 反例和 R4 反例，在决策点上的本地可观测状态是同一个。**

| | `...CannotCreateContradiction`（R3） | `...NeedsCurrentRecoveryEvidence`（R4） |
|---|---|---|
| `<root>/xfer-inflight` | 普通文件 | 普通文件 |
| `stat(<root>/xfer-inflight/<hash>.json)` | `ENOTDIR` | `ENOTDIR` |
| start ledger 曾写成功 | 是 | 是 |
| 要求的行为 | **不得 forward**（否则恢复再合成一条 ⇒ 双终态） | **必须 forward**（否则零终态） |

两者只差一个夹具私有的兄弟目录 `xfer-inflight.parked`，而任何生产判据都不该去认它。所以
**在那个决策点上不存在能同时满足两条的谓词**——`os.Stat` 不行，内存位不行，`ENOENT/ENOTDIR` 与
`EIO` 的分类同样不行（两条反例都是 `ENOTDIR`）。你说"不要再增加一个布尔补丁"，这是对的，而且比
表面更强：再精细的布尔都无解。

所以我改的是**状态本身**，让"抑制是否安全"不再靠猜：

**staging 增加兄弟目录兜底。** `<ClusterDataDir>/xfer-terminal-outbox/` 与 in-flight 目录平级
（`internal/broker/xfer_inflight.go`，`xferTerminalOutboxDir` / `stageXferInflightTerminal`）。
主目录写不进去时，同一条 **exact terminal** 落到 outbox；恢复侧两个目录都扫，outbox 行与就地行
走**完全相同**的重放路径（`replayStagedTerminal`，含 committed / NOT-committed / unknown 三态）。
于是你两条反例都变成 **staging 成功**：R3 里 forward 之后恢复读到 outbox 的精确行、按既有 unknown
规则保留而不合成（`b.cl == nil`），总数恰好 1；R4 里 forward 发生，零终态不成立。判据没变聪明，是**可恢复状态变
成了真的存在**。

配套：
- **那个内存位被整个删掉**，不是收窄。`startLedgerOK` map、`markStartLedgerWritten`、
  `startLedgerWasWritten`、`forgetStartLedger` 及 `broker.go` 的两个字段全部移除
  （`grep -rn "startLedger" internal/ cmd/` 现在只剩零命中）。你的批评是结构性的，补丁式收窄配不上它。
- `removeXferInflight` 在 commit 回调里**同时**清理两个目录，否则 outbox 行会被重放成第二条终态。
- `forEachLedgerRecord` 把 temp 清理与 corrupt 隔离抽出来，两个目录拿到的是同一套处理，而不是薄一层的复制品。

### 新引入面的自查（outbox 自己的风险，不是你提的，但归我）

兜底目录让"一个 transfer 可能在两个目录都有 staged terminal"成为可能（两条终态路径竞争、期间主目录坏掉）。
恢复侧两个目录都扫，天真实现会各自重放一次 ⇒ **两条终态**，正好是 outbox 本身要防的那个矛盾。已修：
主目录那行是正主，outbox 里的同 transfer 行按主行结果处理——主行被处置则**删除**冗余行（否则以后每轮
都会重发），主行被保留（unknown）则一并保留、下轮再判。

钉住它的测试：`internal/broker/xfer_inflight_test.go::TestXferTerminalOutboxDoesNotDoubleEmitWithPrimary`
（用 `d7SingleNode` 起真节点，所以走的是 NOT-committed ⇒ 真重放这一支）。**做过变异核验**：把那段跳过逻辑
删掉后该测试稳定报 `produced 2 terminals, want exactly 1`，恢复后转绿——不是空绿。

**回答你的疑惑 1（优先"绝不双终态"还是"绝不零终态"）**——现在契约写死在代码里，不再是猜：

1. staging 成功（**含 outbox**）⇒ **正常 forward**，commit 回调删掉两个目录里的行。若在 commit 与删除之间
   崩溃，恢复读到的是**同一条字节**，重放 ⇒ 同 reqID ⇒ 被复制去重账本折叠。两个不变量同时成立。
   （更正一处：抑制 forward 不再是任何一支的行为——round-3 那版"staging 失败就整条不发"的早退已删除。）
2. **两个目录都写不进去**（整个 data dir 拒写）⇒ **照发 + Error 级日志**，即优先"绝不零终态"。
   理由写在 `transfer.go` 里：矛盾是**可见且可修**的——两条终态都带 transfer_id，运维和消费者能看出
   冲突；而零终态**连"这里本该有一条"都没人知道**，审计里根本不存在这次传输的结局。
   而且这一支的矛盾窗口比"照发就会矛盾"要窄得多：forward 的 commit 回调本身就会
   `removeXferInflight`（两个目录都删），start 行一旦删掉，恢复就没有可合成的对象。所以真要出现矛盾，
   需要**两次 staging 写都失败 AND unlink 也失败 AND commit 已成功 AND 随后崩溃**四件事同时成立。
   这是**有意的取舍并已成文**，不是遗漏。

## R4-F2 — Major — **采纳，已修**

你是对的，配置指纹不是所有权令牌。我补充一点我核对到的：**当时有两个删除点都不安全**——你的反例命中
的是 create 之前那次（指纹匹配即删）；但 `CreateStream` 对**完全相同的 config 是幂等的**，会把已存在
的流原样交回来，所以 create 之后那次"清理"是**无条件**的，即使前一个删除点没开火，你那条 `must survive`
一样会被它删掉。这一条我不是从文档推的——用一次性探针对真 JetStream 实测过：相同 config 重复
`CreateStream` 返回 `err=<nil>`，只有 config 不同才返回 `10058 stream name already in use`（探针跑完即删，
未留在树里）。两处都改了（`internal/jsstream/jsstream.go`）。

新判据 = **指纹 AND `State.Msgs == 0`**。理由正是你给的那条：**probe 从不 publish**，所以"空"是这个
探针资产唯一不与 operator 流共享的属性。任何非空、或指纹不符的流一律视为**被占用**，直接返回错误
（表现为一次 placement 拒绝 = false negative，安全方向），**绝不删除**。create 之后的清理也重新做一次
同样的检查再删。

**补充（第五轮前的自审，直接回应你的疑惑 2）**：我把你说的"更稳妥的设计"里可低风险落地的那半做了——
canary 现在往 stream metadata 里盖**所有权标记** `tether_placement_canary=ephemeral-probe`，判据变成
「标记匹配」⇒ 是我的；「带了别人的应用 metadata」⇒ **一定不是我的**。这样即使 config 完全相同、且流是**空**的
（你上一版反例的更强形态），只要它是别人**署名**的，探针就不碰，并把名字当作被占用而返回拒绝。
两条测试钉住两个方向：`TestPlacementCanaryLeavesAnAuthoredLookalikeAlone`（署名的同形空流不得被删，
且 probe 不得报成功）与 `TestPlacementCanaryReclaimsItsOwnResidue`（**自己**留下的无标记残留仍必须能回收）。
前者做过变异核验（撤掉标记判据即红）。

**为什么标记不是"无条件必需"**：若服务器不回显 stream metadata，强制要求标记会让**自己的**残留认不出来、
永远回收不掉，之后每次 probe 都撞 "name already in use" ⇒ join gate 永久 wedge——那比它要关的问题更糟。
所以无标记只在「完全没有应用 metadata」时才回退到形状判据。顺带实测确认：仓库锁定的 nats-server
**会**回显 metadata（上面第一条测试若不回显就会红）。

**回答你的疑惑 2（所有权由谁授予）**：我没有引入密码学令牌，也**没有**改成高熵唯一名字——后者会让崩溃
探针留下**名字未知因而永远回收不掉**的残留（探针本身是 grow 路径每 tick 都可能跑的），把一个删除风险换成
一个泄漏风险。固定名 + 空判据让残留可回收、而 operator 数据不可能被删。**残留风险如实登记（已收窄）**：加上所有权标记后，仍会被删的只剩「**空** + config 完全相同 + **完全没有应用
metadata**（因而与本探针自己的旧残留不可区分）」这一种。要连这一支也关掉，就必须让残留回收依赖标记，
而那会在不回显 metadata 的服务器上把 join gate 永久 wedge，我判断不划算——如你不同意，这是可以推翻的取舍。

## R4-F3 — Medium — **采纳，已修**

重复属实，我核对过：`96-mid-flight-chaos.sh:343`（setup，无条件）与 `:505`（A2 分支）同名。已删掉分支
里那条，只留 setup 的一条；原地留注释说明**为什么**——这是**结构性** gap，对本 drill 的每次运行都成立，
所以记账不能取决于是否走进 A2 分支。现在 `grep -c` 该标题 = 1，你的 oracle
`tests/r16-g67-g69-external-rereview.sh` rc=0。

## R4-F4 — Low — **采纳，已修**

`internal/jsstream/replicas.go` 的注释已改成"`AssignedReplicas` 只是 cheap pre-gate，权威判据是
`ProbeMetaCanPlace` 这一**直接测量**"，并按你的建议**保留**两条真正未闭合的项：`3→2(无 JS peer-remove)→3`
实机 differential，以及 Memory canary 与 File/ObjectStore 的资源条件差异。

## 回答疑惑 3（Memory canary ≠ File/Object 资源可用）

同意，且我按"改契约必须扫全部调用点（含打印给运维的文案）"的纪律把**运维可见文案**收窄了——原来两处都说
"place assets"，那是 canary 给不出的承诺：

- `clusterwrite.go`：改为"the JS meta refused to assign an empty memory-backed canary at the target
  replica factor (this measures META ASSIGNMENT, not disk budget for file/object stores)"；
- `cluster_operation_controller.go` 的 op error：改为"…to be able to **ASSIGN a new stream** at the target
  replica factor (measured with an empty memory-backed canary; it does not check disk budget for
  file/object stores)"。

我**没有**加 FileStorage canary：它要么小到不能证明真实 object-store 的预算（等于换个说法的同一个 gap），
要么大到让这道门自己消耗它正在检查的资源。这一项**保持 OPEN 并已登记**，不假装关闭。

## 回答疑惑 4（#58 的 deploy-tier 证明）

已按你说的做成"账上只有一个 structural gap"（见 R4-F3）。真正的 >15m 长跑证明**仍然未做**，仍登记为
owed；我没有在本轮把它伪装成已覆盖。

## 本轮闸（最终树实测，供你复核，不作为结论）

**边界提示**：你暂存的 87 文件快照是**修复前**的树；上述修复以 11 个未暂存文件叠在其上
（`internal/broker/{broker,cluster_operation_controller,clusterwrite,transfer,xfer_inflight,xfer_inflight_test}`、
`internal/jsstream/{jsstream,replicas}`、`test/simcluster/drills/96-mid-flight-chaos.sh`、
`test/simcluster/expected-verdicts.tsv`、本报告）。重审前需要重新冻结边界。**index 我全程未动。**

在**修复后**的最终树上：

- `make test`：rc=0，全包通过；
- `-race`（`internal/broker` / `internal/jsstream` / `internal/cluster`）：全部 ok，无 race 报告；
- `make lint`：`0 issues`；
- `make e2e`：`681.454s`，`PASS`，`--- FAIL` 计数 0；
- simcluster hermetic `tests/run-all.sh`：ALL PASS；你的两个 oracle
  `tests/r16-g67-g69-external-review.sh`（PASS）与 `tests/r16-g67-g69-external-rereview.sh`（rc=0）均通过；
- **deploy-tier（weilandserver，真 systemd + 真独立 nats-server）**：
  `drill 67-transient-js-refusal` = `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=1 nc_guard=0 pass=18`
  ——与 TSV 期望一致；剩下的唯一 gap 是**已登记的 #67 face B**（缺一个不污染 broker 自身 I/O 的注入
  oracle），sub-face 4 已不在 gap 列表，pass 由 17 升到 18。
  `drill 96-mid-flight-chaos` = `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=6 nc_guard=0 pass=37`
  ——同样与 TSV 期望一致。**R4-F3 在部署层被真实验到**：这一轮确实走进了 A2 分支（`A2a` PASS，
  orphan 445 > baseline 1，随后 reap 轮询 90s 超时），也就是旧版会**第二次**登记 #58 的那条路径，而整份
  日志里该标题只出现 **1 次**。六条 gap 逐条核对**全部有主、无新增无归属项**；`nc_gap=6/pass=37` 与 TSV
  里记的 `nc_gap=5/pass=38` 的差异来自**分支组成**（#57 与 A2 每轮只命中其中一支），
  判断"我的改动没造成回归"的判据是 `assert_fail / setup_red / product_red` 三项全 0。

**证据与树的对应关系（如实说明）**：上面 drill 96 的那次运行是在**加所有权标记之前**的树上跑的；
标记只改 `ProbeMetaCanPlace`（join 路径），而 96 不做 grow、不触发 canary，所以那次判决对它测的东西仍然
成立。drill 67 走 canary/join 面，已在**最终树**（含所有权标记）上重跑，判决与标记前**逐字段相同**：
`INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=1 nc_guard=0 pass=18`——所有权标记没有在部署层
改变 join gate 的行为，这正是它应有的样子：标记只在「名字被别人占用」时才改变判定，而那不是 drill 构造的场景。

以上是我的测量，不是结论——结论归你。
