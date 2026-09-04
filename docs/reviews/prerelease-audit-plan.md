# 发布前全库审计 · 修复计划（post-1.0 叶子增量）

> Date: 2026-09-02。CLAUDE.md §3 阶段 A 产物，**主进程定稿**。基线 HEAD `021c970`（v0.5.1 之后 3 个 commit）。
> 来源：step 1 的对抗性 workflow（run `wf_2448aa21-2ff`，34 agent 全 Opus，107 min，980 万 subagent token，2540 次工具调用）
> —— 16 条 lane 逐包扫描 → 16 个 verifier 逐条独立核实并**评估修法** → 1 个完整性批评者。
> 综合者（第 34 个 agent）因单条响应超 64k 输出上限而失败；**这不影响定稿**——CLAUDE.md §3 明确
> 「主进程是 plan 的唯一定稿人」，本文由主进程直接从 16 份扫描 + 16 份核实 + 1 份批评者产出裁决而成。
>
> 原始语料：`$CLAUDE_JOB_DIR/tmp/audit_corpus.md`（975 KB，全部 findings + verdicts）、
> `adjudicate_high.md`（51 项高危精简版）、`decisions.md`（**逐条裁决台账，本文的依据**）、
> `main_process_findings.md`（主进程独立取证，与 workflow 并行）。
>
> 用户指令（2026-09-02，覆盖一切"分期"倾向）：**「必须将问题进行彻底地修复，不准将工作推到以后的版本」**。
> 见 memory `feedback-fix-completely-no-deferral`。本文所有修法据此定稿：凡出现「本版做一半、下版再摘」的
> 形态一律回炉重设计。

---

## §0 统计

| | BLOCKER | MAJOR | MINOR | NOTE | 合计 |
|---|---|---|---|---|---|
| 16 lane 扫描（去 1 条 REFUTED） | 5 | 38 | 72 | 27 | 142 |
| verifier 补抓（missed_by_reviewer） | 0 | 8 | 26 | 10 | 44 |
| 主进程独立取证 | 1（≡BDP-F1） | 1 | 2 | — | 4 |
| **主进程上调** | **+2** | −2 | | | |
| **待处理** | **7** | **44** | **98** | **37** | **186** |

verifier 裁决分布：CONFIRMED 108 / DOWNGRADED 13 / REFUTED 1 / UPGRADED 1。
被 REFUTED 的 1 条（`agent-transfer-proxy-upgrade/L3-F7`，前提「archive/tar 会交出负 Size」错误——tar 在 `Next()`
内部就判 `ErrHeader`）**不进本计划**。13 条 DOWNGRADED 按 verifier 的新 severity 归位。

**主进程上调两条**（理由见 `decisions.md`）：
- `broker-proc / V:broker-proc` MAJOR → **BLOCKER**：跨 session 写越界 + 摧毁运行中作业。
- `proto-auth-acl / L1-F3 + V:proto-auth-acl` 合并 MAJOR → **BLOCKER**：未认证方可远程杀光实验负载。

---

## §-1 主进程定稿裁决：三条推翻了 reviewer 的原修法

verifier 系统性地**评估并常常否决**了 reviewer 的修法；下列是我采纳 verifier 而非原稿的三类，
以及一条我推翻了两者的。完整逐条见 `decisions.md`。

**A. 因为原修法会引入新缺陷而否决**（6 条）
- `BT-F4`：`sizeAware false→true` 会翻掉被 `TestHomeReapIsNotSizeAware` AST 钉死的**永久裁决 N15**，
  且让 2 GiB 对象 floor 变 ~32 分钟、把下一次 tier-B 顶成 `too_large`。改用 **uptime 闸**
  `floor = max(minAge, XferTierBMaxBudget − time.Since(bootAt))`：稳态零代价、大小无关、不碰 N15。
- `L3-F2`（交互 run）：`rc.Done() → sess.Hangup()` 把"看不见"换成"跑丢"——每次日常 rebuild 都给前台作业发 SIGHUP。
  改用 **session 级通配订阅**（`startCtlLiveness` 的 `pty.*.ka` 是现成模板，ACL 已允许），rebuild 对交互会话透明。
- `AC-F3`：「连续 N 次失败即 dropLease」在纯网络中断时也触发，而 `dropLease` 会 `stateStore.reattach()`——
  在共享 NFS 上等于把克隆实例放回在位者 state.json 的写权限里，正是 `buildLocalSnapshot` 注释明文要防的事故。
  改用**逐 URL 归因**（只在已失败路径上）。
- `BC-F2`：「把 `b.js` 赋值上移」会让 `EnsureEventsStream` 之前发的 sys.event 拿 `ErrNoStreamResponse`
  而 `pubSysEvent` 只 Warn 不回退 ⇒ 事件**彻底丢失**，窗口从几微秒撑成"整个订阅安装 + 200ms Flush"。
  改用 `atomic.Pointer` + 包级自由函数（不加 *Broker 方法——结构预算棘轮钉方法数）。
- `L3-F3`（upgrade 锁）：把 marker 的 value 从时间戳改成 rollID，会让**混版滚动期**（这把锁恰好只在
  滚动升级时持有！）老 leader 写的值与新 leader 的谓词不可区分 ⇒ release no-op、join/retire 卡到租约到期。
  改用 **additive 独立键** `cluster_upgrade_owner`（与 R7b 拆 lease 同构，向后兼容）。
- `L3-F1`（InstallSnapshot 身份移植）：「装库后补写 + 三条包进 tx」关不上崩溃窗口（重启读盘的是
  `cutover.go:136`，此刻还没有 fsm）。改为**在装库之前改暂存文件** ⇒ 整个安装在 SQLite 语义上原子，代码更短。

**B. 因为站点清单不完整而扩大范围**（4 条）——本仓 `feedback-contract-change-sweep` 的形态
- `L3-F1`（IsNotLeader）：verifier 补出**第二个哨兵** `raft.ErrEnqueueTimeout`，同一后果链，合并一次改完。
- `L-BCO-F1`：只修点名的 3 处 hold 不够，`driveRetire` 里另有 **6 处**同族无界 hold；
  且必须与「退役宕机 voter 结构性不可能」一起修，否则 BLOCKED→confirm→再 BLOCKED 的 10 分钟死循环。
- `CLI-F2`：不止 9 处；`aborted` 家族 10 个站点里**只有 1 个**带 exit class——reviewer 把特例当了通例。
- `L3-F3`（abandoned spawn）：修法必须**镜像到 pty 侧**，否则同一不变量下一轮被重新发现（CLAUDE.md §3 5b 的原话）。

**C. 因为 severity 前提已过期而降级**（我接受对**我自己**发现的修正）
- 我的 `MP-1(c)`（未降频日志 → 磁盘填满）被 verifier 降级：#78 之后 h1 已加进程内 size-cap +
  logrotate 50MB×2，**磁盘填满的前提不再成立**。危害只剩"未降频 + 回显未校验内容"。修法不变（校验 sid/nid 顺带解决）。
- `BDP-F3` 同理：真实伤害不是爆盘，而是 (a) 单核 100% 忙循环饿死 raft/NATS goroutine、
  (b) 秒级把 150MB 日志历史全部轮转掉 ⇒ **事故取证被抹掉**。

**D. 我推翻了 reviewer 与 verifier 双方的一处**
`L3-F2`（init re-run 漏 `nats_route`）的备选「比较前两侧过 `stripScheme`」被 verifier 驳回，我同意并强化理由：
`render.go:145-148` 把该列**逐字**写进 `routes:`，只有 LISTEN 合成才 TrimPrefix ⇒ 两种形态在下游**不等价**，
归一化会把一次真实的形态变更掩盖成"相同身份"。**照原串比较、响亮拒绝**。

---

## §决策A · `_INBOX.>` 凭据泄露：如何在**一个 release 内**闭合（用户指令 vs N-1 窗口）

这是本轮唯一与 `docs/requirements.md §6.7` 的 N-1 兼容窗口正面冲突的一条，也是最严重的一条。

### 缺陷（两条 lane 独立发现 + 主进程逐环自证 + verifier 端到端复现）
`internal/auth/permissions.go:37/174/234` 三个客户端模板的 `Sub.Allow` 都是裸 `"_INBOX.>"`；
全仓 `CustomInboxPrefix` **零命中**（回包一律落默认 `_INBOX.`）；而
`roleCtlUnactivated` 由 `handler.go:226-229` 对**任何自铸 nkey + CONNECT name `tether-cli`** 无条件放行
（零 DB 查询、无 PIN、无成员身份）。三类主体同属一个 NATS account（`handler.go:551-564` 的
`uc.Audience = h.TargetAccount`）。秘密确实走 inbox：register 回包的
`ProxyDirective{Token, Keys[].Secret}`、`ProxySubCreateResp.SubURL`（只打印一次的裸 bearer token）、
以及 **`tether exec` 的全部 stdout/stderr 分块**。
⇒ **任何能连上公网 443 的外部方可跨全部 session 收割凭据与命令输出。**

仓库**自己已经写下过这个事实**（`home_delivery.go:369-370`：「EVERY agent (any session) is granted
`Sub _INBOX.>` … so a token is **DISCLOSED on the shared bus**」），当时选择在局部绕行（每条 directive 一个
一次性 token）而未堵通道；而 `proxy.go:29-32` 与 `docs/architecture.md:346` 两处承重论证至今仍建立在
「inbox 是私密的」这个已被证伪的前提上。critic 指出它**已被推迟过三个增量**。

### 为什么"两步走"不可接受
两条 lane 给的都是「本版加私有子树、下版删裸 `_INBOX.>`」。verifier 说得很直白：
**过渡 release 同时保留两者，本身一分安全都不买**——泄漏在整个过渡期原样存在。
这正是用户指令否决的形态。

### 定稿设计（一个 release 内闭合，零残留）
关键洞察：**匿名攻击面只来自 `PermissionsForUnactivated`**；而对 agent，broker **有持久的版本信息**。

| 主体 | 本版做法 | N-1 影响 |
|---|---|---|
| **unactivated ctl**（匿名入口） | **无条件删除** `Sub "_INBOX.>"`，改授 `_INBOX.<idtok>.>`；**同时删除** `sys.events` 授权（§1 的 L1-F2） | 旧 ctl 的 `session create/list/login` 需升级。**这是唯一的 N-1 例外**，与 §6.7 已有的 `cluster join` release-相等豁免同类，写进 §6.7 |
| **agent**（车队，绝不能断） | **版本感知铸权**：auth_callout 用 `h.DB` 读 `nodes.release_version`（`node.Register` 持久化的列，`PermissionsForAgent(sid,nid)` 已有键）。证明是新版 ⇒ 只授 `_INBOX.<idtok>.>`；否则连裸 `_INBOX.>` 一起授 | **零破坏、零运维介入**：随车队升级自动收敛；回滚同样自动回退。首连无 `nodes` 行时保守授裸通配，下次连接即收敛 |
| **activated member ctl** | 默认**删除**裸 `_INBOX.>`；新增 broker 配置 `broker.auth.legacy_ctl_inbox`（**默认 false = 安全**）作为运维逃生口 | 默认安全（守 S2：安全默认值不用 warning 代替）；需要跑旧 CLI 的运维显式打开，且 `cluster doctor` 报告该项为降级态 |
| **broker** | `PermissionsForBroker` 的 **Pub** 必须加新 reply 子树 | 必需——verifier 指出漏掉它会让 `msg.Respond` 被 NATS 拒绝、**回包静默消失** |

### 实现必须覆盖的 6 处（verifier 逐条验过，漏一处即跑不起来）
1. `PermissionsForBroker` 的 **Pub** 加 `ctrl.by.*.reply.>` 与 `s.*.node.*.reply.>`（当前只有 `_INBOX.>`）。
2. `nats.CustomInboxPrefix` **拒绝结尾带点**（nats.go:1571-1579）⇒ 前缀写 `"_INBOX."+hex16`，**无尾点**。
3. `PermissionsForUnactivated` 也要授新前缀（它同样发 3 个 Request）。
4. `internal/auth/acl_reconcile_test.go:652` 已为 `_INBOX` 登记了「reply subject, not a served endpoint」
   的双向对账豁免 ⇒ 新子树**必须同样登记**，否则 `TestACLGrantsHaveSubscribers` 变红。
5. `cmd/tether/alert_gate.go:31` 用的是**包级** `nats.NewInbox()`（不认连接前缀）⇒ 改 `nc.NewInbox()`，
   否则 destructive gate 静默失效。
6. 换 inbox 前缀会同时改掉 nats.go 的 **JetStream ordered-consumer deliver subject 与 ObjectStore watch subject**
   （`tether history`、push/pull 都走它）⇒ **必须在真 NATS 上跑通**，不能只看单测。

verifier 已在临时模块里实测过目标态两端：`TestFixedPrefixStillAllowsRequestReply` PASS
（`respMux` 订的是 `<prefix>.<nuid>.*`，被 `_INBOX.<tok>.>` 覆盖）、
`TestFixedPrefixBlocksForeignInboxSub` PASS（裸 `_INBOX.>` 订阅被 `Permissions Violation` 拒）。

**驳回**「顺手把 Token/Keys 移出 register 回包」：verifier 证明 `proxyDirectiveForRegister` 是**唯一**
把新分配的 raw tunnel Token 交给 agent 的路径，拿掉会让一次全新分配悬到下一次 reaper tick / 心跳修复。

### 附带必须同批修正的两处已被证伪的文档
`internal/broker/proxy.go:29-32`（把 register-reply `_INBOX` 当成比 `sys.events` 更安全的通道）与
`docs/architecture.md:346` B.4 决策表（inbox「可否被第三方订阅：否」）。不改这两处，下一个人会再次基于
错误前提做设计。

---

## §1 BLOCKER（7 条）

| # | 出处 | 一句话 | 修法要点 |
|---|---|---|---|
| B1 | `broker-proxy-http/L3-F1` ≡ `proto-auth-acl/L1-F1` | `_INBOX.>` 匿名跨 session 收割凭据与 exec 输出 | **§决策A** |
| B2 | `broker-cluster-write/L3-F1` + verifier 补的第二哨兵 | `ErrLeadershipTransferInProgress` / `ErrEnqueueTimeout` 不在 `IsNotLeader` 家族 ⇒ 一条常规 `drain`/`retire` 让全队 agent register 落 `store_error` 并**退进程**（systemd 连坐杀 cgroup 内实验负载） | `IsNotLeader` 加两个 `errors.Is`；godoc 从枚举改写成语义判据并写明为何**不**含 `ErrRaftShutdown`/`ErrAbortedByRestore`；**必须补真集群复现**（谓词单测证明不了 raft 真会返回它，而那是本条全部风险） |
| B3 | `broker-dataplane/BDP-F1` ≡ 主进程 `MP-1` | 公网 :7000 未鉴权 REGISTER 行无字节上限 ⇒ 远程内存耗尽 | `NewReaderSize`+`ReadSlice`；`ErrBufferFull` 交既有 `registerReadFailed`（**不新增 DENY reason**，源码文本闸天然不动）；**在触碰任何 map/日志之前** `ValidateSID`/`ValidateNID`；accept 加并发握手上限 |
| B4 | `deploy-release-docs/DRD-F1` | 重跑 `install.sh --role broker` 静默把已加固 broker 打回**公网无鉴权**（nats.conf 的 authorization 块 + unit 的 `--auth-callout-seeds-dir` 同时被抹），而 §8.5 正推荐这个动作 | 保留既有文件 + 写 `<file>.new` 供 diff + banner 汇总 + `--force-config`；**不是**一律 preserve（那会让下个 release 的 unit 修正静默停在旧机上）。文档半边同批改 |
| B5 | `broker-proc/V` **（主进程上调）** | `ev.proc.exit` 的 pid 不与 subject 的 (sid,nid) 对账 ⇒ 任一 agent 可关掉**别的会话**的 RUNNING 行、任选 rc，随后 G.1 把操作员**仍在跑的作业** SIGKILL | 复用 exit 臂已有的预读做归属检查，**只收紧到 SID**（收到 (SID,NID) 会打断租约改名） |
| B6 | `proto-auth-acl/L1-F3` + `V` **（主进程上调、合并）** | argon2id 跑在 auth_callout **单条串行**回调里（0.15–0.3s/次，`AUTH_TIMEOUT=2s`，限流仅 per-IP 无全局上限）⇒ 未认证方 30 个源地址即认证黑洞；其输出 `Authorization Violation` 与永久拒绝**逐字相同**，`isAuthFailure` 判终局 ⇒ 长命 agent 一次 rebuild 撞上就退进程，systemd 默认 `KillMode=control-group` **杀光 cgroup 内实验负载** | ① 进程级全局 PIN 预算（`rate.Every(1s)`, burst 10），`blocked()` 返回 `perIP \|\| global`；**驳回** goroutine 池（verifier：池满 inline ⇒ 行为与今天逐字相同，是安慰剂且带三样代价）② `everAuthed atomic.Bool`：认证过的进程遇 deny 走瞬时重试；首连行为逐字不变 |
| B7 | `deploy-release-docs/V` | `install.sh --role agent --uninstall` 的 `rm -rf ~/.tether` 一并删掉 **ctl 的私钥身份**（`keys/default.nk`，丢了无法再生成、orphan 掉该身份拥有的每个 session）与其它所有 session 的状态；对照组 `uninstall_ctl` 刻意一个字都不碰该目录 | `--session` 时只删该 session 的 agent 目录；未给时保持整棵删除**但先打印**警告。同步改 `usage.md:170-175` |

---

## §2 MAJOR（44 条）

逐条裁决在 `decisions.md`（含每条采纳/驳回的理由与 verifier 的订正）。按修改面聚类：

- **`internal/tunnel`**（与 B3 同批，同一函数）：BDP-F2 fence map 键永不回收（实测 200 连接留 200 永久键）、
  BDP-F3 accept 忙循环（**必加** `net.ErrClosed` 终止条件）、yamux nil config 直写裸 fd 2、
  server 侧无写截止 + 无 ctx 看门狗。
- **`internal/broker` 租约**：BC-F1 farewell 不带 `LeasedNID`（F11 声称修好的后缀漂移仍活着；现存测试绿是因为
  **helper 自己补了产品不发的字段**）+ BC-F3 farewell 置 OFFLINE 不校验持有者 —— **必须合成一个补丁**。
- **`internal/broker` 并发**：BC-F2 `b.js` data race（`nc`/`tunnelSrv` 早已因同样理由改 `atomic.Pointer`，`js` 被漏掉）。
- **`internal/broker` transfer**：BT-F1 看门狗 cancel 泄漏、BT-F2 tier-A 采信客户端 Bucket/ObjectKey（跨 session 卡死）、
  BT-F3 一条坏 ledger 行永久关停孤儿回收（#58 复发）、BT-F4 单 broker 下 2 分钟删掉在传对象、
  BT-F5 攻击者定尺字符串（真实上界 1024×8MiB 而非注释写的 200 KiB）、#57 单 broker 双重惰性。
- **`internal/broker` 集群运维**：retire ladder 9 处无界 hold + 退役**宕机** voter 结构性不可能（必须同批）、
  drain 收敛闸 fleet-wide（改为把 `setPhase(DRAINING)` 移到收敛门之前，3 行零新键）、
  alert.ack 未校验 dedup_key（member 可无界撑大 raft 复制存储）。
- **`internal/cluster`**：InstallSnapshot 身份移植、roster 准入零校验（准入路径比改写路径**更松**）、
  upgrade 锁无持有者身份（grow 锁修过、upgrade 锁被漏下）。
- **`internal/agent`**：AC-F1/F2 迟到回调未按 conn 身份过滤、AC-F3 多 URL 池下 auth 拒绝被折叠成 `ErrNoServers`、
  exec chunk 绑死 spawn conn（run 侧 h1 已修、exec 侧漏掉）、交互 run rebuild 后哑火、
  abandoned spawn 只 Wait 不 kill、`push --force` 丢权限位（含 owner/group）。
- **`cmd/tether`**：exec/本地拒绝的 exit class 全落 70（而 usage.md §9.13 让自动化重试 70）、
  alert ls/ack 复用 500ms（客户端等得比服务端短 10 倍 ⇒ 已提交的 ack 被报成失败）、
  `agent join --start` 丢掉 10 个配置字段（fail-open 到全盘开放）、
  只读诊断命令消耗升级 boot budget（3 次 `agent doctor` 即回滚整机二进制）。
- **`internal/natsconf`**：自动 reconciler / grow cutover 缺 `ClientListen` fail-closed 守卫
  ⇒ 可在 ~24s 内把 NATS 客户端口从回环静默改成 `0.0.0.0:4222`（三条 CLI 路径都守、两条**自动**路径不守）。
- **发布链**：goreleaser `{{ .Version }}` 剥前导 v（verifier 用**线上 v0.5.1 资产** 200/404 证实）
  ⇒ `--wait` 的精确判据与 same-tag 守卫全是死代码、`cluster upgrade --to-version`（照命令自带 Example 写）必然 HALT。
  修法：新增 `proto.SameRelease`，替换 5 处逐字比较；**驳回**单侧 `TrimPrefix` 归一化。
- **`install.sh`**：agent.yaml 无条件覆盖（verifier 在隔离 HOME 里**实测复现**：两条已收紧的键全部消失）。

---

## §3 MINOR / NOTE（135 条）

按用户指令**全部处理**，不设"下版再说"。执行时按 §4 的批次随主题一并落，逐条结论写进
`docs/reviews/prerelease-audit-review.md` 的附录（第 2 轮内审同批产出）。
其中已知的高价值项：restore staging 缺 `O_NOFOLLOW`（journal.go 已封过的同一提权原语）、
`--dump-divergent` 明文写出 SS PSK、`OfflineBackup` 是唯一不取 data-dir flock 的 offline 手术、
`FetchManifest` 每次新建 `http.Transport` 且从不 `CloseIdleConnections`、
`logrotate.Writer.Write` 违反 `io.Writer` 契约、fd 泄漏闸在 fd 耗尽时报 PASS（**闸门本身**）、
`brokermetrics` 裸 `recover()` 把 snapshot panic 变成 HTTP 200 空 body（fail-open 的监控面）。

---

## §4 批次（每批独立可过三硬闸）

| 批 | 内容 | 主要文件 | 为什么放一起 |
|---|---|---|---|
| **A** | B1（§决策A 全部 6 处）+ L1-F2 删匿名 `sys.events` | `internal/auth/permissions.go`、`internal/authcallout`、`internal/cli`、`internal/agent`、`cmd/tether/alert_gate.go`、`internal/broker` | 同一个授权面；**必须在真 NATS 上验 JetStream/ObjectStore 路径** |
| **B** | B2 + `ErrEnqueueTimeout` | `internal/cluster/node.go` 一处 + 真集群复现测试 | 一行谓词，覆盖 12 个调用点 |
| **C** | B3 + BDP-F2/F3 + yamux config + server 写截止 | `internal/tunnel/tunnel.go` | 全在 `handleAgent`/`acceptLoop` 两个函数 |
| **D** | B6（全局 PIN 预算 + `everAuthed`） | `internal/authcallout/ratelimit.go`、`internal/agent/agent.go` | 攻击链的两端必须同批 |
| **E** | B5 + broker-proc 租约 reconcile + BC-F1/BC-F3 | `internal/broker/{exec,reconcile,broker}.go`、`internal/agent/instance.go` | 都在 register/reconcile 路径 |
| **F** | transfer 全族（BT-F1..F5 + #57） | `internal/broker/{transfer,xfer_inflight,transfer_reconcile}.go` | 同一子系统 |
| **G** | 集群运维（retire 9 处 hold + 退役宕机 voter + drain setPhase + alert.ack） | `internal/broker/cluster_operation_controller.go`、`clusterdrain.go`、`cluster_health.go`、`internal/cluster/alert_ops.go` | retire 两条必须同批 |
| **H** | `internal/cluster`（snapshot 身份、roster 准入校验、upgrade 锁 owner） | `internal/cluster/{snapshot,membership_ops,lock_lease}.go` | 同包，都触 FSM 契约 |
| **I** | agent 侧（AC-F1/F2/F3、exec/run conn、abandoned spawn、push mode） | `internal/agent/*` | 同包 |
| **J** | ctl exit class 全族 + alert 超时 + `agent join` 配置 + boot budget | `cmd/tether/*` | 同包，纯 CLI 侧 |
| **K** | 发布链（`proto.SameRelease` 5 处）+ `install.sh` 守卫（B4/B7/DRD-F5）+ natsconf `ClientListen` 守卫 + 文档 | `internal/proto`、`cmd/tether/node.go`、`scripts/install.sh`、`internal/natsconf/takeover.go`、`docs/*` | 部署/发布面 |
| **L** | §3 的 135 条 MINOR/NOTE，按主题并入上述各批 | — | 不单独成批 |

**顺序**：A → B → C → D → E → F → G → H → I → J → K（L 随批）。A/B/C/D 是 BLOCKER，优先。

---

## §5 测试与变异验证

每条新增守卫按 `feedback-mutation-verify-every-new-guard`：**注入它声称能抓的缺陷 → 确认变红 → 复原**。
verifier 已点名的三条"近乎恒等式"的 proposed_test 必须重写（`alert.ack` 那条注释掉新增三行照样绿）。

- 测试文件/函数按**被测单元**命名（§3 step 5b），禁止 `*_prerelease_audit_*` / `*_round2_*` 这类过程命名；
  溯源写 `// origin: prerelease audit <lane>/<id>` 一行。
- 新增/删除 Test 函数必须同步 `test/determinism/testdata/test_function_inventory.txt`（只增账本，`-update-test-inventory`）。
- **必须在真 NATS 上跑**：批 A（inbox 前缀改动会波及 JetStream ordered consumer 与 ObjectStore watch）。
- **必须真集群复现**：批 B（`LeadershipTransferToServer` 后立刻 Propose，断言 `IsNotLeader(err)`）。
- 已知会被打红、需同批修的既有测试：`internal/broker/force_single_handler_test.go:60`
  （`"127.0.0.1:1"` → `"nats://127.0.0.1:1"`，批 H）。

**收尾四闸**：`make test` + `make e2e-parallel` + `make lint` + `make gates`，
**不得用管道 `| tail` 取结果**（退出码会被吃掉，本仓有过一整轮假全绿）。
干净树基线（021c970 实测）：lint rc=0、test rc=0（1m26s）、gates rc=0（3m37s，simcluster hermetic 全 PASS）。

---

## §6 本轮**不做**（须显式记录，非静默推后）

1. **被 REFUTED 的 1 条**：`archive/tar` 负 Size —— 前提错误，不存在该缺陷。
2. **`sys.events` 载荷脱敏**（把 `owner_fp`/`actor`/`fp` 摘掉或迁到 `s.<sid>.ev.*`）：那是**跨 session 元数据隔离**，
   与本轮的"匿名可读"正交，且会动 agent 的消费路径。verifier 明确建议单独立项。本轮只删匿名读权限。
3. **agent unit 加 `KillMode=mixed`**：能让 agent 退出不牵连 cgroup 负载，但改变既有 teardown 语义，属另一增量。
   本轮用 B6 从根上让 agent 不再因瞬时 auth 失败自杀。
4. **drain 收敛集的 scoping**：本轮用「`setPhase` 移到收敛门之前」这个 3 行修法解决卡死；
   精确 scoping（`drain_origin`）被 verifier 论证为代价最高的一种，单独立项。
5. **已在台账且本轮无新证据的 gotcha**（#29/#34 等）：不复述、不并入。

以上每条都有理由，均非"以后再修"。

---

## §7 覆盖对账

critic 用 16 份 `what_i_checked` 的并集与 `git ls-files 'cmd/*.go' 'internal/*.go'`（去 `_test.go`，**267** 个生产文件）
做差：零 lane 声称读过的只有 **7** 个，且 critic 自己补读了其中最大的 5 个并报出 4 条（U1–U4，已并入 §3）。
主进程另做了一份**跨包**的监听面清单（7 个 `net.Listen`/`tls.Listen` 站点逐个判定），
确认公网可达 + 未认证 + 自写解析器的路径**唯一**就是 :7000（= B3）。

---

## §8 `// origin:` 锚点索引（round 2 · C10）

源码里 175 处 `// origin: prerelease audit <lane>/<id>` 锚点，其 `<id>` 必须在本文件里可查——
否则它就是死指针：读者按图索骥、什么也找不到，还以为是自己漏了。
`test/determinism/origin_line_test.go` 的 `TestPrereleaseAuditAnchorsResolve` 机械守这条。

上文 §1–§3 已逐条列出绝大多数 id。下列 5 条是**实现期新增**的、当时没有回填进 plan 的：

| id | lane | 位置 | 一句话 |
|---|---|---|---|
| `AC-F2` | agent-conn | `internal/agent/agent.go` | `rebuilding` 只在重建**期间**为真，所以清掉之后到达的旧回调看起来和活的一样；用 session 代际号（`sessionGen`）判定归属，而不是留着死连接做指针比较。 |
| `CLI-F1` | cli-ctl | `cmd/tether/error_hints.go` | 该处返回裸 error，`classifyExit` 只能落到未分类的 70；改为携带 exit class 的 `*ExitError`。 |
| `DRD-F2` | deploy-release-docs | `internal/proto/xfer.go` | 五处用 `==`/`!=` 比 release 串，`v` 前缀一差就全判不等；统一走 `SameRelease`（与 `DRD-F3` 同批）。 |
| `L4-F1` | cli-serve-agent-cluster | `internal/natsconf/takeover.go` | 空 `ClientListen` 会渲染出一份语法合法、却谁也连不上的 nats.conf；改为显式拒绝而不是静默接受。 |
| `L-BCO-F2` | broker-cluster-ops | `internal/broker/clusterdrain.go` | 收敛等待用错了判据，drain 会在本可通过时卡住；`setPhase` 移到收敛门之前（见 §6 第 4 条）。 |

**为什么是索引而不是逐条改注释**：锚点的价值在于**扛得住改名**（CLAUDE.md §5「溯源用稳定锚点」）。
把 5 条补进索引，175 处锚点一次全部变成活指针；反过来把 id 从注释里删掉，等于把溯源信息又丢一次。
