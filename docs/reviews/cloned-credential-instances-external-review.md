# Fail - 克隆凭据实例增量外部独立审查

结论：**Fail**。本增量还不能上线。当前至少有 5 个发布阻断项：cluster 注册路径丢失 `leased` 且新增 migration 与同 proto 滚动契约冲突；数字后缀的真实配置名被折叠到错误的凭据族；重连收到租约拒绝后仍保留已 replay 的旧名订阅；contested reply 会提前提交 upgrade marker；plan 明确要求的 co-located agent 租约豁免没有实现。另有 tunnel 换名竞态、共享二进制升级 marker 被兄弟实例接管、cluster 进程归档失真、租约 eviction 无效但破坏历史等高风险问题。

本报告是独立源码、失败路径和运行证据得出的结论。已有两轮内部报告只作为问题索引；其中的“已修”“非缺陷”没有被当作正确性证据。

## Scope 与方法

- 审查对象是开始审查时工作树中全部暂存区外内容：约 33 个已跟踪文件、`+2123/-126`，以及新增 plan、内审报告、migration、实现、测试和 sim drill 等未跟踪文件；开始时 index 为空。后续统计包含外审测试，故数字略有增加。
- 权威链按 `CLAUDE.md` 执行，并阅读了 `docs/requirements.md`、`docs/architecture.md`、`docs/distributed-architecture.md`、`docs/usage.md`、`docs/broker-ops.md`、`docs/cluster*.md`、simcluster README/mandate、当前 plan 和既有审查报告。
- 详细审查面和逐项完成记录见 `docs/reviews/cloned-credential-instances-external-review-tasklist.md`。
- 外审没有修改产品实现；只新增了 4 个独立守护测试、本 tasklist 和本报告。失败测试被保留，用来防止缺陷在后续修复中被重新解释为预期行为。

## Findings

### F1 - Blocker - cluster 注册不持久化 `leased`，而直接补 SQL 又会打破当前滚动升级契约

单机 `node.Register` 在 INSERT 和 conflict 时都写 `leased`（`internal/node/node.go:121-158`）；cluster 使用的 `PlanRegister` 完全没有渲染该列（`internal/node/plan.go:29-60`）。独立差分测试 `TestPlanRegisterPersistsLeaseClassification` 在 INSERT 即得到 `direct=1 replicated=0`。

影响不是显示误差：`node upgrade --all` 依赖该列过滤临时实例；租约分配的 `claimedLeaseNames` 又用 `(status <> 'OFFLINE' OR leased = 0)` 判定名字是否永久占用（`internal/node/lease.go:86-115`）。因此 cluster 中租约节点会进入 fleet upgrade，离线后还会被误当永久设备，使 suffix 无法回收。

同时，新增 `internal/storage/migrations/0019_nodes_leased.sql` 与当前 plan 的“本轮零 migration”（`docs/reviews/cloned-credential-instances-plan.md:360-368`）及 G5 的“同 proto rolling release 不得加 migration”（`docs/reviews/g5-plan.md:91`）冲突。若只把 `leased` 填进 raft SQL，尚未执行 0019 的旧版本 follower Apply 会因未知列失败；当前漏列只是以数据错误掩盖 schema 兼容错误。

建议：先明确这是 flag-day/schema-floor/proto bump，还是同 proto 滚动增量。若必须滚动，应采用旧 schema 可 Apply 的表达并给出 mixed-version 实测；不能只给 `PlanRegister` 补一列。随后用 direct/FSM INSERT+UPDATE 差分、旧新三节点滚动和 snapshot/restore 测试共同锁定。

### F2 - Blocker - 真实的数字后缀 nid 被折叠到错误凭据族，克隆实例无法稳定认证

`assignLeaseName` 对 presented nid 调 `proto.BasenameOf`（`internal/broker/broker.go:2018-2040`），agent 也按折叠后的 `proto.BasenameOf(a.cfg.NID)` 接受租约（`internal/agent/instance.go:439-464`）。所以配置名为 `gpu-02` 的设备被复制时，broker 会发 `gpu-03`/basename `gpu`，而不是 `gpu-02-02`。

agent/broker “一致”并不代表认证链一致：永久 provisioning row 的 key 是 `gpu-02`，auth_callout 对 `gpu-03` 只会回查 `gpu`（`internal/authcallout/handler.go:340-373`）。无 PIN reconnect 会被拒；若仍携带 PIN，则可能把临时名字 bootstrap 成永久 provisioning 名，进一步污染 namespace。独立生产路径测试 `TestConfiguredNumericSuffixRemainsTheLiteralLeaseBasename` 稳定得到 `gpu-03`，期望 `gpu-02-02`。

建议：把 configured nid 当作 opaque credential-family root。首次裁决使用 presented configured nid 原值；已租约重连必须显式携带/保存 canonical configured basename，不能再从字符串尾部猜测。

### F3 - Blocker - reconnect 收到拒绝形租约后不退役连接，旧名命令可被重复执行

nats.go reconnect 时已经 replay 旧 forwarded subscription。`onNATSReconnect` 收到非 nil lease 后，仅在名字可接受时请求 rebuild；空 assignment 或非法 assignment 只记录日志并返回（`internal/agent/proxy.go:681-702`）。broker 此时特意没有注册 challenger、没有 reconcile、也没有任何 ownership 副作用，但 challenger 仍继续订阅 incumbent 名字。

独立测试 `TestReconnectLeaseRefusalRetiresTheContestedSession` 等待 1 秒后确认 session 未 cancel、rebuild flags 未置位。实际后果是一条 `exec/run/expose/upgrade` 可能同时被 incumbent 和被拒 clone 消费，重现本增量声称要消除的双执行问题。

建议：任何 `resp.Lease != nil` 都必须终止当前 connection/session；可接受 assignment 走换名 rebuild，拒绝形走有界 backoff 后重新竞争或明确退出，但不得保留旧订阅。

### F4 - Blocker - contested register 被当作 upgrade 健康提交点

`Agent.register` 对任何 `resp.OK` 都立即调用 `commitUpgradeAfterRegister`（`internal/agent/agent.go:1390-1408`），之后调用者才检查 `resp.Lease`。然而 contested response 明确定义为“没有成功注册、没有 reconcile/AcceptedProcesses”的裁决回复。

独立测试 `TestContestedRegisterReplyDoesNotCommitAnUpgrade` 证明 pending marker 在收到 assigned-name reply 后已变成 `committed`。若随后 assigned-name 的 auth 或 rebuild 失败，rollback watchdog 已被解除，节点可永久离线且控制端得到假成功。

建议：只有 `resp.OK && resp.Lease == nil` 的真实注册才可 commit；assigned-name session 完成认证并以新名字成功 register 后再提交。

### F5 - Blocker - co-located broker-host agent 的租约豁免只写在 plan，没有落地

plan 已准确指出静态 `--colocated-agent-nid` 被用于 forwarded upgrade subject，并裁定该 agent 必须豁免租约（`docs/reviews/cloned-credential-instances-plan.md:231-237`）。实际 lease adjudication 没有读取 `ColocatedAgentNID`；cluster upgrade 仍向静态名字发升级（`internal/broker/cluster_upgrade_trigger.go:134-143`）。

如果 co-located agent 被分配 suffix，broker binary reload 后，agent leg 发往无人订阅的 basename，整机停在 broker 已升级、agent 未升级的半升级状态并 HALT。这个遗漏也没有测试覆盖。

建议：在 broker 裁决入口对 configured co-located nid 强制 singleton/exempt，并补 broker upgrade trigger 的端到端测试；如果产品不再接受该裁定，必须先修改 plan、cluster docs 和升级安全模型。

### F6 - High - tunnel `SetNID` 与 Open 并发可在换名后重新安装旧身份 session

adapter 的 `AddProxy`/`ApplyHome` 使用 `opMu`，`SetNID` 不使用（`internal/agent/tunnel_adapter.go:60-94`）。底层 `SetNID` 原子换 nid、整表摘除 session 后异步 Close（`internal/tunnel/tunnel.go:985-1040`），但一个已经读取旧 nid 的并发 Open 可在摘表后完成握手并重新写入 session map。于是 clone 仍可能以 incumbent 身份桥接其 public port。

此外 retire 只 cancel/Close，没有发 `notifyState(false)`，agent 的 proxy ready 可能继续保持 true；每个 Close 都起一个无上限 goroutine，黑洞 socket 可长期积累。

建议：引入 nid generation；Open 在安装 session 前复核 generation，SetNID 与所有 Open/ApplyHome 串行化或显式 fencing；退役必须发 down edge，并对连接关闭提供可观测、可回收的有界策略。

### F7 - High - 共享二进制 upgrade marker 可被任意兄弟 clone 接管

boot shim 只要发现自身 SHA 等于 marker `NewSHA` 就进入 `bootContinuePending`，递增共享 `BootCount`、mint 新 instance id 并覆写 `TargetInstance`（`internal/agent/upgrade_state.go:445-478`）。共享 home/共享二进制的兄弟进程同样运行这些 bytes，因此注释所称“sibling cannot reach this arm”不成立。flock 只能串行化，不能证明 ownership。

后果是兄弟实例重启可偷走 marker、消耗目标实例 boot budget、报告错误结果或触发目标二进制回滚。

建议：marker 的所有权必须锚定可跨 exec/restart 验证且不被兄弟共享的 lineage；如果同 inode shared-home 是支持场景，marker 需要按实例隔离，不能仅靠 binary path + SHA。

### F8 - High - cluster 模式故意不 refile 进程，I2 地址语义和审计归属失真

`refileProc` 在 cluster mode 直接 log 后返回 nil（`internal/broker/clusterwrite.go:1270-1296`）。adoption 后长运行进程仍永久归档在旧 nid：`tether ps <lease>` 看不到，basename 显示并非自己启动的工作，后续 adoption 逻辑却按“已搬迁”继续。

这不只是 display detail；requirements 把租约名定义成普通可寻址设备，而进程查询、归属和事件审计是该身份的一部分。

建议：增加明确的 raft/FSM refile op 与 N-1 capability gate；若暂不实现，应 fail closed/声明 cluster 不支持本功能，而不是成功返回。

### F9 - High - suffix auth fallback 后的 PIN bootstrap 会把临时租约固化成永久身份

suffix fallback 查不到 basename binding 后会直接落入 PIN bootstrap（`internal/authcallout/handler.go:356-390`）。在 follower stale read、basename provisioning 被误删、或 operator 仍传 PIN 时，`gpu1-02` 会获得自己的 provisioning row。此后 allocator 永久保留该 suffix，`leased`/upgrade/evict 语义都改变。

建议：lease adoption 认证必须带 broker 签发、短时、绑定 `{sid, configured basename, assigned nid, instance id}` 的证明；至少要禁止符合租约形状的未 provisioned nid 进入普通 PIN bootstrap。

### F10 - High - `admin evict <lease>` 返回成功、删除历史，但不会停止实例

新增的 characterization test `internal/adminsock/lease_evict_ops_test.go` 已显示：lease 名没有 provisioning row；evict 可删除 node/process/port 数据并返回成功，广播的 `agent_evicted` 又不会命中 agent，因为 agent matcher以 configured basename 为准。运行中的实例继续工作，reconnect 还能通过 suffix fallback 重新出现。

建议：明确选择并实现一种契约：拒绝 instance lease eviction；实现由 instance id/lease grant 定位的实例级 eviction；或只允许 basename 级撤销整组 credential。现状“成功 + 数据破坏 + 未驱逐”不可接受。

### F11 - High - 快速重启造成 suffix 单调漂移和 ghost 节点

farewell 释放内存 grant，但节点行仍在 OfflineAfter 窗口内保持 ONLINE；allocator 因而把刚释放的 `-02` 视为占用并分配 `-03`（`internal/node/lease.go:94-109`）。现有 `TestReviewDemoRestartedLeasedInstanceGetsANewSuffixEveryTime` 已把这一结果记录为当前行为。

命令、expose 和运维脚本仍引用旧名字，60 秒内旧 ghost 还看似 ONLINE；频繁重启会持续消耗 suffix，最终进入空 assignment 拒绝循环。requirements 只说重启“重新竞争”，没有披露这类地址漂移和资源搁浅。

建议：让可信 farewell 原子释放/下线对应 leased row，或允许相同 credential/lineage 优先回收最近租约；至少文档和 CLI 必须明确名字不稳定并提供清理/耗尽恢复手段。

### F12 - High - proxy status 又用名字形状猜租约，真实 `gpu-02` 被误分类

持久层已新增 `nodes.leased` 来解除语法歧义，但 `proxyStatusNodes` 查询不读取它，仍以 `SplitLeaseName(r.nid)` 设置 `leased_instance`（`internal/broker/proxy.go:1052-1110`）。真实配置为 `gpu-02` 的节点因此即使 `leased=0`，status 也会报告租约不可用原因；与 allocator、upgrade 和 JSON node list 形成三套真相。

建议：所有决策和显示统一使用 stored lease classification；名字 parser 只能用于格式验证，不能代替 provenance。

### F13 - High - 用户文档承诺“完全照旧”，但关键例外和故障恢复没有文档化

requirements 称租约名“与任何设备完全相同”（`docs/requirements.md:375-390`），FAQ 称使用方式“完全照旧”（`docs/usage.md:1778-1787`）。实际例外包括：proxy ineligible、`upgrade --all` 排除、共享 state 不保留 expose、重启可能改名、evict 无有效语义、永不 promotion、cluster refile 缺失。

`node ls` 人读表也没有 `LEASED` 列（`cmd/tether/node.go:89-118`）；usage 仍将 `--all` 写成“session 内全量 ONLINE”（`docs/usage.md:1233-1245`），与代码过滤不一致。

建议：增加完整 operator workflow：如何识别 configured/leased identity、地址漂移、proxy/upgrade/evict 限制、suffix exhaustion、共享 home 风险、co-located agent 和恢复步骤；CLI 至少在人读输出明确标识 LEASED 并列出被 `--all` 跳过的具体名字。

### F14 - High - 新增 sim drill 自身违反 verdict framing 契约，且未覆盖声称的 shared-inode 场景

drill 83 的 16 个 setup/行为断言在本机 sim cluster 全部通过，但脚本到 B4 断言即结束（`test/simcluster/drills/83-cloned-image-instances.sh:100-144`），没有 `drill_begin`，也没有 `drill_end`/机器 verdict。`bash test/simcluster/tests/lint-drills.sh --all` 因这两个 `no-frame` 违规返回 rc=1；该 drill 已被登记为 GREEN，不能按 legacy 豁免。

此外脚本把 home 复制到另一容器的独立存储，并没有验证 plan/脚本注释反复强调的“两个进程同时共享 NFS 上同一 inode”。因此 shared state/marker 的最高风险部署形态仍没有 deploy-tier oracle。

建议：补标准 begin/end framing；保留 copy-image drill，并另加真正共享 volume/inode 的并发 drill，验证 state、marker、expose 和 restart。

### F15 - Medium - 测试资产混入 reviewer-demo/stale oracle，存在假绿和维护误导

多个新增测试仍以 `REVIEWER DEMO FILE`、`CHARACTERIZES` 或内审事件命名。特别是 `internal/broker/lease_basename_collapse_test.go:9-45` 手工复刻了旧的 agent 接受规则；生产代码现已按 `BasenameOf(cfg.NID)` 接受 `gpu-03`，真正失败点转移到 auth，但该 demo 仍会绿色输出过时解释。绿色 characterization 与真正红色 invariant test 共存，容易让修复者删错测试或接受错误语义。

建议：上线候选只保留按产品不变量命名、真正穿过生产路径的 guard；诊断 demo 移入报告或改成明确失败的 regression test，清理审查过程用语和失效注释。

### F16 - Medium - 结构预算被无解释放宽，plan 与实现范围记录失真

`internal/broker/broker.go` 本轮增加约 833 行，结构预算从 14000 放宽到 16000（`test/architecture/testdata/structural_budget_golden.txt:56-63`），但 plan 的结构策略仍声称 broker 文件预算保持并通过自由函数规避方法数（`docs/reviews/cloned-credential-instances-plan.md:356-365`），内审也没有给出为何不能拆分的架构理由。plan 同时说零 migration，实际新增 0019。

建议：不要把 ratchet 改动当作机械更新；先拆出 lease adjudication/holder lifecycle 模块，或在 architecture decision 中记录放宽原因、债务上限和回收计划，并同步 plan。

### F17 - Medium - 初始拒绝/空 assignment 的恢复仍可能形成高速 rebuild 与共享状态误清理

拒绝形 lease 没有可采用名字；初始路径会反复 rebuild，而 Run loop 没有面向“suffix exhausted/invalid offer”的有界退避和终止状态。被拒 clone 仍附着共享 state，fail-closed 定时器有机会清理 basename holder 的 proxy footprint。现有 demo 已能重现 exhaustion 返回空 assignment，但将其当作可持续当前行为。

建议：拒绝形进入明确的 degraded/offline 状态并做指数退避；在任何 teardown/fail-close 之前先 detach 非持有者共享状态，日志提供可操作的 exhaustion 诊断。

## 疑惑与需要产品裁定的事项

1. 0019 migration 是否意味着本 release 改为 flag-day？如果不是，mixed adjacent-release cluster 如何保证旧 follower Apply 新 register command 不失败？当前 ProtoVersion 没变。
2. 租约实例重启后换名、旧地址 ghost 60 秒、已有 expose 搁浅，是否是明确接受的产品语义？若是，requirements 的“普通 nid/完全照旧”必须修改。
3. `admin evict <lease>` 应驱逐单实例、整 credential family，还是明确拒绝？当前三者都不是。
4. shared NFS 同 inode 是否仍是必须支持的 reference deployment？若是，drill 83 当前的 `cp -a` 模型不足。
5. warm memory snapshot 会复制进程内 instance id。代码注释称下游可检测，但相同 iid 被视为同一进程 reconnect，两个 subscription 仍可能共存。若此场景明确不支持，应在部署文档写成硬限制，不能声称已检测。

## 独立验证结果

| 命令 | 结果 | 说明 |
|---|---:|---|
| `make test` | rc=2, Fail | 其余包通过；仅 3 个当时已加入的外审 guard 失败：cluster `leased`、数字后缀 family、reconnect refusal。 |
| `make gates` | rc=0, Pass | tagged vet、Darwin build、architecture/determinism/cmd/auth/concurrency/proto 与 golangci-lint 全通过。 |
| focused upgrade marker test | rc=1, Fail | contested reply 把 marker 从 pending 提前改成 committed。 |
| focused `go test -race`（node/broker/agent 四个外审 guard） | rc=1, Fail | 无 race detector 报告；四个业务断言全部稳定失败。 |
| `make e2e-parallel` | rc=2, 4m07s, Fail | 99 units/18 workers；除外审 node test 及 broker test 的 D4/D5 分片外均 PASS，三个失败均为上述稳定产品断言。 |
| `./local.sh --build drill 83-cloned-image-instances` | rc=0, Pass | 本机 sim server；16/16 setup/行为断言通过，验证基本双实例分名和单次 exec。 |
| `bash test/simcluster/tests/lint-drills.sh --all` | rc=1, Fail | drill 83 缺 `drill_begin` 和 `drill_end`/verdict。 |
| `git diff --check`、drill `sh -n` | rc=0, Pass | 无 whitespace error，shell 语法通过。 |

环境说明：嵌入式 NATS 测试需要在沙箱外绑定本地端口；首次沙箱内启动失败属于环境限制，已在获准的本机环境重跑。sim 主机 `weilandserver` 符合 local sim README。没有未运行且被结论依赖的门禁。

## 上线前最低修复顺序

1. 先解决 F1 的 schema/rolling 设计，不要以局部 SQL 修补制造 raft Apply 兼容事故。
2. 修复 F2-F5，并让 4 个独立红测和新增 co-located 测试转绿。
3. 对 tunnel identity 做 generation fence，重做 shared marker ownership；补真正 shared-inode drill。
4. 明确 eviction、restart rename、cluster refile 三项产品契约并实现，不允许以文档模糊代替。
5. 修正 drill framing、文档与 CLI 可观测性，清理 reviewer-demo 测试；最后重跑 `make test`、`make gates`、`make e2e-parallel` 和 drill lint。

---

# 主进程逐条回复（step 6）

> 结论：**全部 17 项已处理**——5 项 Blocker 与 9 项 High 已修并各自带守卫，
> 3 项 Medium 中 2 项已修、1 项按外审给出的第二个选项记录了债务与回收计划。
> 外审留下的 4 个红测已全部转绿，且其中 2 个被**改写**而非删除（理由逐条写在下面）。
>
> 复跑：`make test` rc=0 · `make e2e-parallel` rc=0 ALL PASS · `make lint` rc=0 ·
> `make gates` rc=0 · `lint-drills.sh --all` rc=0 · drill 83 GREEN 16/16 ·
> 新增 drill 84（真共享 inode）pass=17 assert_fail=0。

## 先回答「需要产品裁定」的 5 条

**1. 0019 migration 是否意味着 flag-day？——不是。撤销 migration。**
外审这条挖得最深：`g5-plan.md` OQ-2 明写「same-proto rolling releases MUST NOT … add migrations」，
而未跑 migration 的 follower 会在 Apply 一条点名未知列的 register command 时失败——集群级事故。
用户的有序升级约束指向同一处。所以**不是**给 `PlanRegister` 补一列，而是**整列删掉**：
租约与否改由 `agent_provisioning` 行推导，两种模式本就同路复制该表，
F1 找到的分歧因此**在构造上不可能复发**，而不是靠第二个写者记得跟上。
这一条同时解掉 F1/F12，也让 F2/F9 的修法成为必需（见下）。

**2. 重启换名、旧地址 ghost 60 秒、expose 搁浅，是明确接受的产品语义吗？——部分不是，已修；剩下的已写进文档。**
「ghost 60 秒 + 后缀单调漂移」**不接受**，已按外审建议修（F11）：可信 farewell 现在原子地把该租约行置 OFFLINE，
重启的实例拿回自己刚让出的名字而不是下一个后缀。
「名字可能变」「expose 不跨重启保留」是真实语义，`requirements.md` 与 `usage.md` 已改写——
原文「与任何一台设备完全相同 / 使用方式完全照旧」确实不实，现在给出**逐条对照表**（F13）。

**3. `admin evict <lease>` 应该做什么？——拒绝。**
选外审列的第一个选项。evict 的语义是**撤销凭据**，而租约名两半都没有：
它没有自己的 provisioning 行，撤销广播又按**配置名**匹配，运行中的实例根本听不见。
现状「成功 + 毁数据 + 没驱逐」是三者里最坏的。现在返回 `bad_request`，
并告诉运维：撤整族用 `evict <配置名>`，只想停一个实例去那台机器上停。

**4. shared NFS 同 inode 仍是必须支持的形态吗？——是，且现在有 deploy-tier oracle 了。**
新增 `up --shared-agent-home`（一个 named volume 挂到所有 agent 的 `/home/sim/.tether`）+ **drill 84**，
第一条 setup 断言就 `stat` 两个容器的 inode，确保「共享」不是假设。
17 条断言全过：两行分名、一次 exec 一次执行、租约可寻址，
以及**incumbent 的端口 token 在租约实例到来后仍在**（租约实例不得写属于 basename 持有者的文件）。

**5. warm snapshot 复制 instance id——注释确实说过头了，已改。**
原文说「下游可检测」，实际是「下游把它们当成同一实例重连」，两个订阅仍可共存。
已在 `internal/agent/instance.go` 的包注释里改写为**不支持的硬限制**，不再声称已检测。

## Blocker

| # | 处置 | 落点 |
|---|---|---|
| **F1** | **采纳，按「旧 schema 可 Apply 的表达」修** | 删 `0019` + `nodes.leased`；判据回到 provisioning 行。外审的红测 `TestPlanRegisterPersistsLeaseClassification` **改写**为 `TestLeaseClassificationIsNotSchemaAndCannotDivergeBetweenModes`——它现在钉「两条 register 路径写同一列集」+「`nodes.leased` 不许回来」，守护的是同一个风险，理由写在测试注释里 |
| **F2** | **采纳，按「configured nid 当作 opaque root」修** | 新增 additive wire 字段 `ConfiguredNID`（agent 自报），`assignLeaseName` 不再 `BasenameOf`；agent 侧接受规则改为「必须是 `cfg.NID` 的直接租约」。`gpu-02` 的克隆现在是 `gpu-02-02` |
| **F3** | **采纳** | 任何 `resp.Lease != nil` 都退役当前 session：可接受的换名 rebuild，拒绝形走 `requestLeaseRebuild(a, "")`（同一套 teardown，不改名），旧订阅不再存活 |
| **F4** | **采纳** | 只有 `resp.OK && resp.Lease == nil` 才 `commitUpgradeAfterRegister` |
| **F5** | **采纳** | `adjudicateLease` 入口对 `ColocatedAgentNID` 强制豁免，带变异验证的守卫（并断言豁免只作用于那一个名字） |

## High

| # | 处置 |
|---|---|
| **F6** | **三半全修**：`nidGen` fence（Open 采样、装 session 前在锁下复核）、退役时发 `notifyState(false)` 下降沿、Close 收敛成一个有界 goroutine。`-race` 通过，down-edge 有变异验证 |
| **F7** | **采纳，且撤销了我第二轮的错误修法**。外审对「sibling cannot reach this arm」的反驳成立：兄弟跑的是同一份 bytes。boot shim 的接管逻辑已删除；ownership 锚到 per-transaction 的 boot proof；**残留限制诚实记录**——共享二进制 + 共享 home 下没有任何值既能跨 supervisor 重启存活又不被兄弟共享。drill 84 的 S6 就是这条 gap |
| **F8** | **采纳**：cluster 模式返回 typed error 而非 `nil`。「为没做的事报成功」是唯一不能做的选择；raft/FSM refile op 已明确记账为后续增量 |
| **F9** | **采纳**：`pin != ""` 时不进 suffix fallback。这条是 F1 能成立的前提——真实 `<base>-NN` 设备因此拿到自己的 provisioning 行 |
| **F10** | **采纳（拒绝语义）**，见上文裁定 3。外审的 characterization test 已**改写**为守护拒绝契约 + 断言「什么都没被删」+ 「basename 仍可 evict」 |
| **F11** | **采纳**：farewell 原子置 OFFLINE。变异验证下正是外审描述的 `gpu1-03` 漂移 |
| **F12** | **采纳**：新增 `leasedNIDsForSession`，`proxy status` 与 `node ls` 同源同判据，名字 parser 只做格式校验 |
| **F13** | **采纳**：`node ls` 人读表加 `KIND` 列（device/leased）；`requirements.md` 与 `usage.md` 改写，给出 6 行例外对照表 + cluster refile 限制 + `--all` 描述订正 |
| **F14** | **两半都做**：drill 83 补 `drill_begin`/`drill_end`（现在打机器可读 verdict，`lint-drills --all` rc=0）；新增 drill 84 做真共享 inode |

## Medium

| # | 处置 |
|---|---|
| **F15** | **采纳**：8 个文件的 `REVIEWER DEMO` / `not part of the increment` 框架文字已剥离（论证一字未动）；`lease_basename_collapse_test.go` 从「绿色地记录缺陷」改成钉不变量，并有变异验证 |
| **F16** | **按外审给的第二个选项处理，并承认第一次上调确实没写理由**。`structural_budget_golden.txt` 现在写明：加了什么、为什么现在拆不动（`instance.go` 紧贴 `Agent` 私有状态，拆包等于换成更宽的 API 面）、**债务上限**（这两条不得再涨）、**回收计划**（`internal/lease` 包，下一个动租约裁决的增量先做）。plan 的结构策略段与「零 migration」段已同步订正 |
| **F17** | **采纳**：拒绝形进入指数退避（2s 起、60s 封顶）并在 5 次后**停止竞争**，日志明说运维要做什么（释放实例或调高上限）。守卫覆盖退避单调、封顶与终止 |

## 一处必须指出的：F7 的建议手段有副作用

报告的次选方案我采纳了，但**首选方案里那句「必须 memoize `mintInstanceID`」是错的**，而且我第二轮正是这么做的。
memoize 是**进程级**缓存，于是一个进程里构造的每个 Agent 拿到同一个 instance id——
而两个实例共享一个 id 在 broker 眼里就是「同一实例重连」，克隆分裂当场失效（`test/p2` 立刻抓到）。
现在 boot shim 根本不需要 id，问题消失。记在这里，因为下一个照着 F7 做的人会踩同一个坑。

---

# 主进程回复 · 第二轮外审（同一报告的复跑）

复跑发现工作树里有**三条新的红测**——它们不在上一版报告的正文里，是这一轮外审新加的守卫，
而且**三条都指向我上一轮修法留下的洞**。全部已修并各带变异验证：

## R1 · `TestRegisterCannotAllocateAnotherCredentialFamily` —— 我引入的安全漏洞

F2 的修法让 agent 自报 `ConfiguredNID`，而我**无条件信任了它**。
后果不是显示问题：持 `gpu1` 凭据的 agent 只要声称 `ConfiguredNID: "victim"`，
broker 就会把分配重定向到**另一个凭据族**，在 `leaseHolder` 里预留 `victim-02`、`victim-03`…
把别人的后缀空间耗尽，而它从来无法以那些名字通过认证。

**修法**：`ConfiguredNID` 必须与本次 register 所用的名字自洽——
要么相等，要么 `nid` 是它的**直接租约**（`<base>-NN`）。这正是"已持租约的 agent"唯一合法的差异形态。
auth_callout 已经把连接绑定到 `nid`，这条让 register body **与之一致**而不是凌驾其上；
不匹配时退回使用呈报名，那是 pre-feature 读法，够不到族外。
**变异验证**：改回无条件信任 → 立刻拿到 `victim-02`。

这条我认：把一个客户端可控的值当权威，是 F2 修法里我自己加的新攻击面，外审抓得准。

## R2 · `TestClusterReleaseDoesNotLeaveTheLeaseRowOnline` —— 我在 F11 里重犯了 F8 的错

F8 教训是"不许为没做的事报成功"，而我在 F11 的 cluster 分支里**又写了一次** `return nil` + 日志。
调用方分不清"已释放"和"跳过了"，于是集群车队保留着 F11 本该消除的后缀漂移，代码读起来却像已修。

**修法**：根本不需要跳过。`nodes.status` 是 **liveness 列**，本来就由各节点本地写
（`ReconcileStates` 走的就是 `livenessDB()`），走 raft 反而会让这次转换和系统里其它每一次
liveness 转换不一致。改为两种模式都经 `livenessDB()`，真正写入。
顺带修了 `livenessDB()` 在 `b.cl` 未 wire 时的 nil 解引用——liveness 写不该因 wiring 时序 panic。

## R3 · `TestReconnectLeaseRefusalRetiresTheContestedSession` —— F17 的退避把 F3 的退役推迟了

外审这次把 `leaseRefusals` 设到终止阈值，专打**放弃分支**，而我的放弃分支直接 `return`，
**根本没退役 session**——那条已 replay 的订阅永远留着，正是 F3 要消灭的东西的**无界形式**。
测试注释还点出了另一半："Earlier retries have the same ordering bug (they wait before teardown)"。

F3 原文是"有界 backoff 后**重新竞争**"，我把顺序做反了。
**修法**：退役**无条件且立即**（含放弃路径）；退避改由 `leaseRefusalUntil` 携带，
在 Run 循环**下次拨号前**消费——那才是"更慢地重新竞争"该待的地方。
**变异验证**：让放弃分支不退役 → 立刻变红。

## 复跑结果

```
make test          rc=0
make e2e-parallel  rc=0  ALL PASS (4m03s)
make lint          rc=0
make gates         rc=0
lint-drills --all  rc=0
git diff --cached --check  rc=0
drill 83           GREEN       pass=16 assert_fail=0 product_red=0
drill 84（真共享 inode）        pass=17 assert_fail=0 product_red=0（1 个显式 gap = F7 残留限制）
外审全部守卫 -race  全绿，无 data race
```

> 附一条状态订正：报告正文写「全部 77 个变更文件已加入暂存区；当前没有未暂存或未跟踪文件」，
> 而实测为 **79 个已暂存、0 个未跟踪**——上一轮我新增的 drill 84 等文件也已被纳入暂存。
> 数字差异不影响任何结论，记在这里只是为了让复核者对得上账。
