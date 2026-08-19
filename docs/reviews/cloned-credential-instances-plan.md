# 同凭证克隆实例 plan（定稿）— instance identity + 租约显示名

> 起草：25 专家对抗性 workflow（12 subsystem lane → 8 critic → 3 候选 plan → judge → completeness critic）。
> 定稿：主进程。逐条采纳/驳回见文内标注。**本文的每条锚点主进程均已独立复核**，
> 与专家转述冲突处以主进程复核为准（已标出三处）。

---

## 0. 问题与证据（取证先行，代码修改之前完成）

镜像里烘焙了 tether agent 与其凭证，镜像被同时启动多份。所有克隆共享
`~/.tether/keys` 的 nkey 与 `agent.yaml` 的 nid，因此**全部通过 auth_callout**
（`ensureAgentProvisioned` 在 fp 匹配时 return，`internal/authcallout/handler.go:308+`），
**这是 D3 的设计意图，不是漏洞**。

### 0.1 生产取证（session=lab，节点 `jupyter-ziyang10`，2026-08-18）

原始数据存 `prefix-baseline/`（`node-ls.txt` / `ps.txt` / `history-port.txt` / `history-proc.txt`）。

| # | 观测 | 状态 |
|---|---|---|
| E1 | 两个活实例，pod IP `10.42.68.20` / `10.42.68.120`；`node ls` 只显示一行 | **直接观测** |
| E2 | 每条 `exec` 两个实例**各执行一次**：审计里两条 start + 两条 exit，退出码旁路信道连续三轮显示 rc=20/rc=120 成对 | **直接观测** |
| E3 | `/etc/machine-id` 为空 | **直接观测** |
| E4 | 端口 14008 的 `revoked`/`reconciled` 翻转 | **⚠ 我最初的解读错误，见 0.2** |
| E5 | G.1 reconcile 跨克隆腐蚀 | **代码确证，生产未触发，见 0.3** |

### 0.2 订正一：14008 的翻转**不是**克隆互相驱逐

`audit.port{kind:"revoked"}` 只有一个发射点（`internal/broker/expose.go:449-450` 的 `reconcilePorts`），
数据源 `port.ListAllocatedForOfflineNodes` 的 SQL 要求 `n.status='OFFLINE'` 且超 `PortRevokeAfter`(15min)。
**两个每 5s 心跳的克隆永远满足不了这个条件。** 实测 14008 全历史仅 4 次 revoked + 4 次 reconciled、
时间戳跨多天 ⇒ 是节点真的离线过 4 次后回归的正常循环。

**仍未解释的线索**：`tether ps` 显示 `__proxy__ jupyter-ziyang10 :14008 created 2h`，
而其余七个节点全是 185–186h ⇒ 它的分配在 2 小时前被**重铸**过。两种机制符合观测且**需要不同的修法**：
(a) `AllocateProxy` re-mint 循环（`proxy.go:515-521` 的 TokenHash 比对每次 register 都 miss）；
(b) tunnel 驱逐 ping-pong。**定案需要 broker 日志里 `broker: proxy allocate on register`（`proxy.go:525`）
与 `tunnel: registered`（`tunnel.go:604`）的到达间隔** ⇒ 列为 **Stage 0 阻塞项 0.3(e)**。
**本 plan 不把未确证的机制写成已确证。**

### 0.3 订正二：reconcile 交叉腐蚀是「已装填未击发」

代码路径确证：`internal/broker/reconcile.go:81-115`，registering agent 未报告的 pid 走 else 分支 →
`ExitMark{-1}` + `reconciled_closed`；再次 register 时这些 pid 已不在 RUNNING 集合 → orphan 分支 →
`killed_orphan` → agent SIGKILL。
但生产审计中该节点 **零条** `reconciled_closed`、**零条** `killed_orphan`（60 start/60 exit 全是探针）。
原因：触发需要「实例 A 有**长驻** RUNNING 进程」且「实例 B 发生 register」，用户当前无长任务。
**正确表述：风险真实、损失不可逆，但不能说它正在发生。**

### 0.4 订正三：驱逐点的代码位置（我此前指错）

驱逐是 `s.sessions[port] = sess`（`internal/tunnel/tunnel.go:594-599`），
map 是 `sessions map[int]*serverSession // public port -> session`（`:131`），**只按 port 定键**
⇒ 两个克隆**即使 tokenHash 不同也互相驱逐**，identical `(sid,nid,tokenHash)` 是巧合不是原因。
`tunnel.go:686` 的 `CloseProxyIf` 是**teardown fence**，与驱逐无关 ——
**给 fence key 加第四维对 install 行为的改变恰好为零**。

### 0.5 judge 独立发现的第三个 flap 引擎（此前无人归因）

`internal/node/node.go:130-137` 的直接 mutator 在 `ON CONFLICT` 时置 **`proxy_ready = 0`**，
携带 round-6 F8 的注释（"(re)register 意味着新 agent 进程尚未重建 SS server + tunnel"）。
⇒ **两个克隆在一次普通 UPSERT 上就把对方踢出 `/sub`**，无需任何驱逐。

**completeness B4 的关键限定（采纳）**：该 clobber **仅存在于单机模式**
（raft 路径的 `plan.go:53-59` 根本不碰 `proxy_ready`/`status`/`last_heartbeat_at`）。
生产车队正是单机模式，故它是 blocker；但**任何断言它的测试必须 mode-aware**，否则在集群 harness 里空过。
**修法绝不能描述为"停止清零 `proxy_ready`"**——那会回归 round-6 F8。
正确框架是 **contested register 短路**：克隆根本到不了 mutator，F8 的本意（*重连中的* agent 清除**自己的**就绪位）逐字保留。

### 0.6 Stage 0 spike 结果 —— 现场形态与预设不同，且改写了一条实现要求

**spike (c) 实测（2026-08-18，`jupyter-ziyang10`）**：

```
HOME=/home/ziyang10/.tether-agent
stat -f ~/.tether           -> fstype=nfs
stat ~/.tether/agent/lab/state.json
   -> ino=1415826711089061659 size=167 mtime=1787101442   （两个实例返回值完全相同）
```

**两个实例共享同一个 NFS 挂载的 `~/.tether`，`state.json` 是同一个 inode。**
所以本场景**不是「镜像克隆出两份独立磁盘副本」，而是「共享一个 home 的两个 agent 进程」**。

四条直接后果：

1. **replay gate 绝不能把丢弃结果写回 `state.json`**（原表述"discard `PortTokens` 与 `Proxy`"
   若实现为写盘，会**直接破坏 basename 持有者的状态**，因为那是同一个文件）。
   ⇒ **修正为：只在内存中跳过 replay，全程不写该文件。** 已并入 §5 的测试要求。
2. **这很可能就是 §0.2 里 14008 未解释机制的答案**：两个 agent 各自持有内存中不同的 proxy token，
   交替写回同一个 `state.json` 互相覆盖 ⇒ broker 侧 `proxy.go:515-521` 的 TokenHash 比对每次 register 都 miss
   ⇒ 走 `AllocateProxy` re-mint（free + 重新分配最低空闲端口，恰好又是 14008）⇒ `created` 被重置为 2h。
   即 critic 列的机制 **(a)**，而非 tunnel 驱逐 ping-pong。**仍需 broker 日志的到达间隔定案**，
   但本 plan 的 Stage 2 工作不依赖该定案（contested 短路让克隆根本到不了 `AllocateProxy`）。
3. **升级 flock 是共享的** ⇒ 两个实例会互相阻塞升级。这实际上是**有利**的（防止并发升级），
   但必须在 Q4 的文档里写明，否则运维会把"另一个实例持锁"误读成死锁。
4. **instance_id 不落盘的决定（D2）在此形态下尤其正确**：任何落盘方案在共享 home 下都会立刻互相覆盖。

**给 plan 的范围影响**：本增量的机制（租约 + contested 短路 + 内存态 replay gate）对
「独立磁盘副本」与「共享 home」**两种形态同样成立**，因为它不依赖任何磁盘状态。
但 simcluster drill 必须**两种形态都覆盖**：`83-cloned-image-instances.sh`（复制整个 `~/.tether`）
**外加**一个共享 home 的变体（bind-mount 同一目录给两个 agent）。

**spike (b) 未取得**（racknerd 上 `/etc/tether/broker.yaml` 读不到）⇒ 仍为 Stage 0 未闭合项。
不阻塞实现：本轮不新增 raft op、不新增复制列，集群与单机两条路径的风险都已在 §6 覆盖。

---

## 1. 不变量、范围、Non-goals

### 1.1 两条不变量（用户的设计哲学，机械可验）

- **I1 单实例不变性**：只有一个实例时，每个功能的行为与今天相同 —— CLI 输出、subject、DB 行、wire。
- **I2 后缀等价性**：多实例时，后缀设备的表现与一台真正独立的新设备不可区分。

**I1 的边界（定稿裁定）**：判据是**用户可观测面 + wire 兼容性**。
唯一的诚实让步见 §4.1（`Capabilities` 数组）——**本 plan 不宣称 register body 逐字节不变**。

**I2 不成立之处必须显式登记**，见 §7 的四条账本（只减不增）。

### 1.2 本轮范围（主进程裁定）

judge 给出 Stage 0–3。**本轮交付 Stage 0 + Stage 1 的 1.1/1.2/1.5 + Stage 2 全部**。

**推迟的，各带理由**：
- **Stage 1.3（单机 GC 有界化）/ 1.4（`adminAuditTail` kind 过滤）**：是**独立的既有缺陷**，
  与克隆无因果关系。捆进来会稀释本增量、并让 CLI golden 因无关原因移动。
- **Stage 3 全部**（`nodes.instance_id`、audit `instance` 键、rename 纠正、运维恢复、`exec` watchdog）：
  是持久化与可观测性债务。**采纳 completeness B1 后 Stage 2 自洽**（见 §3.2），不依赖 Stage 3。
- **`exec --all` fan-out**：修不了任何缺陷，与"使用方式照旧"相悖，且需要 `ExecChunk` 归属字段 +
  重定义 N 元退出码契约。语义写下，推迟。
- **`_INBOX.>` 的 ACL 缺陷**（security critic 发现）：`_INBOX.>` 在**每个**角色（含**未激活** CLI）的
  Pub 与 Sub 白名单里（`internal/auth/permissions.go:25-38`）⇒ **session ACL 隔离已可被匿名 peer 绕过**。
  **爆炸半径不同、需要自己的 plan/四象限/drill，且应排在本增量之前。已单独报告用户。**

### 1.3 Non-goals

- **非并发克隆的检测**（先关原机再开克隆）：语义上是**迁移**。需要单调 epoch 重放检测，
  代价是 agent 崩在落盘窗口即被误判 —— 为一个用户没有的场景付这个代价不值。
- **`ProtoVersion` bump**：纪元更替要求全车队重装，与有序更新冲突。**每个 stage 都保持 2。**
- **既有 OPEN gotcha 的顺带修复**（#29/#34/#57/#71/#73/#74）：本增量会**触碰**这些面，
  必须说明触碰后是否恶化，但不承诺修复。

---

## 2. 决策裁决（Q1–Q5 + D1–D7）

### D1 — 身份**绝不耦合机器信息**（用户硬约束）
不读 machine-id / DMI / MAC / boot_id / IP / hostname。
**已验证（Md2）**：`machine-id`/DMI/`product_uuid` 在生产代码里**零引用** ⇒ 无需改代码。
**真正的反例是 `boot_id`**：tether **确实读**它（`internal/agent/agent.go:1905-1912`），
它是 **per-kernel** —— 同宿主的容器全部相同、agent 重启也不变 —— 却进了 `NodeRegisterReq.BootID`、
被持久化、并出现在 `NodeListEntry.BootID`。
⇒ **`BootID` 必须被排除在任何身份、tiebreak、纪元推导之外**；
且 `ReconcileReqID` 声称 boot_id 是"自然纪元：agent 重启 → 新 bootID"的文档串**是错的，同 commit 订正**。
负向控制机械化：determinism 守卫断言无生产文件读 machine-id/DMI/product_uuid；simcluster 镜像清空 `/etc/machine-id`。

### D2 — instance_id = `crypto/rand` + **进程血统**，绝不落盘
26 字符小写 base32，**独立的 `ValidateInstanceID` regexp，不得复用 `idCharset`**（它同时是 `ValidateSID` 的）。
从 `TETHER_INSTANCE_ID` 读，缺失才生成；在**两个** `syscall.Exec` 站点注入（`upgrade.go:298`、`upgrade_state.go:368`），
**并从子进程环境剥离**（否则用户命令里跑 `tether agent` 会继承并与父进程碰撞）。

**诚实登记两条限制**：
- **（M2）粒度**：它回答的是「哪一次进程运行」，不是「哪一台机器」。一台机器跑 10 次重启会呈现为
  10 个不同"真身份"。**不得把它标为"the true identity"**；D6 的机器级问题需要 D1 禁止的信号。
- **（M2 第二条）传递性继承**：`tether run node -- bash` 后在该 shell 里起 agent 会继承同一个 id ⇒
  broker 读作"同一实例重连" ⇒ 两者共用一个名字 ⇒ 扇出复活。**必须守卫**（已有活会话时拒绝继承来的 id）**并测试**。
- **暖克隆**（RAM 快照 / Firecracker / CRIU / container commit）会逐字节复制 environ ⇒
  **检测得到、防不住**，登记为 detected-not-prevented。

### D3 — 凭证语义不变，绑定在 **basename**
**（G4）一次性、显著地写明**：instance id 是**路由与正确性 token，绝非隔离、绝非认证**
（所有克隆共享一个 nkey → 一个 fp → 一行 `agent_provisioning`；CONNECT name 是客户端控制的）。
**凭证生命周期永久是 basename 粒度。**

### D4 — instance_id **只走 register**，不进 CONNECT name / subject / ACL / `sys.events`
CONNECT name 语法**冻结**：`parseRole` 是 `SplitN(rest,":",2)`，第四段会折进 `parts[1]` 并被
`ValidateNID` 拒绝（`handler.go:312`）⇒ **新 agent × 旧 broker 是硬认证拒绝**；
且 auth_callout 由**集群范围的 queue group** 应答 ⇒ 滚动升级期间拒绝**非确定性**。
subject 语法同样冻结：`dispatchForwarded` 硬断言 10 段，`ParseCmdBy` 11 段，`ParseEvProc`/`ParseEvTransfer` 10 段。

### D5 — **一个标识贯穿到底：租约名**
`nodes.nid` = `port_allocations.nid` = tunnel REGISTER 的 `<nid>` = `/sub` 的 proxy 名 =
subject 的 node token = ACL 字面量 = 运维输入的名字。
**不能分裂**：五个生产查询 + 一个外键把两者焊死（`subhttp.go:153/:182`、`proxy.go:1050`、
`proxy_reconcile.go:114`、`port.go:564`、`0001_init.sql:78`）⇒ 分裂则 `/sub` 静默变空。

**（M3）名字分配是 advisory 的**：所有克隆共享凭证且 CONNECT name 客户端可控，
**broker 无法强制**一个旧的/有 bug 的 agent 接受分配。
**执行边界是「行保护」，不是「路由」** —— contested 短路保护现任的**行**，
但挡不住两个旧克隆都订阅 basename 的 forwarded subject。必须写明。

### D6 — 推迟到后续增量（见 §1.2）
本轮**不加** `nodes.instance_id`、不加审计 `instance` 键。

### D7 — 租约**没有独立存储**：`nodes` 就是租约表
`nodes` 是 PK `(sid,nid)` + `ON CONFLICT DO UPDATE`，一个名字"被持有"当且仅当有活行。
⇒ **不建 lease 表、不加 raft op、不加 lease GC、不加 nodes GC。**
⇒ 连带：`decodeCommand` 永不见未知 op、无副本能 poison-skip、`fsm.Apply` 不会因缺列 panic、
**日志重放式回滚仍然合法**（不扩展 #74 的约束）。

### Q1 — proxy eligibility：**仅 basename 持有者**，折进既有 `proxy_capable`
`ProxyCapable: nodeHasProxyCap(...) && !req.ProxyOptOut && !req.LeasedNID`（`broker.go:1461`，#78 折入点）。
`proxy_capable` 是每个下游门都查的那一列 ⇒ **零新分支**，继承 #78 已发布已 drill 的 N-1 故事。
**用独立的 `ready_reason` 值披露，绝不复用 `OptedOut`**（其注释把它钉在一个烘焙镜像里不存在的 agent.yaml 键上）。
**驳回**「恰好一个活实例才 eligible」（无关 pod 出现会剥夺健康主实例的出口）与「一旦被克隆就永久不 eligible」。
**两条诚实成本**：basename 持有者死亡到下一个新到达者之间**出口是断的**（Q5 禁止提升）；
且出口节点会收到**每个订阅者的 Shadowsocks PSK 明文** ⇒ **克隆镜像应当出厂即 `proxy.participate: false`**。

### Q2 — 既不放宽也不截断：**限定后缀并拒绝**
`ValidateNID` 与 `idCharset` 保持逐字节不变。后缀**零填充两位** `-02`…`-99`；
`MaxInstancesPerBasename` 默认 64、硬上限 99 ⇒ 后缀恒为 3 字符 ⇒ **新** provisioning 绑定上限 **29**；
更长的存量 basename grandfathered 为仅单实例，第二个实例得 typed `nid_lease_unavailable` 拒绝。
**零填充不是美观问题（M4 订正理由）**：`ORDER BY nid` 有 **8 个**生产站点，
最关键的是 **`/sub`（`subhttp.go:163`/`:195`，渲染给每个订阅者的 Clash 列表）**
与 `onlineNIDs`（`proxy.go:922`，驱动分配顺序）；nid 按 **TEXT** 排序，不填充则 `-10` 排在 `-2` 前面。
**租约名必须满足 `ValidateNID`** —— `ParseCmdBy`/`ParseSidNidFromCtrl`/`ParseEvProc`/`ParseEvTransfer` 都会复验。
截断比拒绝更糟：两个 30 字符 basename 会塌成同一前缀，**静默合并两个无关设备族**。

### Q3 — `tether exec <租约名>` 直接命中该实例，**零新代码**
租约名是普通合法 nid，今天就能用；`cmd/tether` 只在一处校验 nid（`transfer.go:738`）。
**`--all` 推迟**（见 §1.2），语义写下。

### Q4 — 升级锚在 **basename**；`--all` 排除租约实例
`a.cfg.NID` **永远**表示 agent.yaml 的 basename（`upgrade_state.go` 就是这么比的）⇒ marker 的四件套提交证明不动。
instance id 与 assigned nid 都经 environ 跨两个 exec 站点传递、**并从子环境剥离**。
`--all` 按新增的 `NodeListEntry.Leased bool`（omitempty，仅为 true 时设 ⇒ multiplicity-1 的 `--json` 逐字节不变）过滤、
打印排除数量，并**在回复被截断时整个拒绝扇出**。显式 `node upgrade <租约名>` 允许并打印一行。
**同时修** `resolveColocatedAgent` 的 absence→`AgentAbsent` **fail-open**（会退回 #19 的半升级主机）⇒ 必须解析为 `AgentUnknown`。

**（B2，completeness 独家发现）`--colocated-agent-nid` 是被当作 NATS subject 用的静态配置值**
（`internal/broker/cluster_upgrade_trigger.go` 的 `reexec-agent` 分支 → `SubjCmdForwarded(sid, agentNID, "upgrade")`；
同值还喂 `cmd/tether/node_versions.go:53,89` 与 `cluster_upgrade_drive.go:284`）。
若 co-located agent 拿到后缀租约，该 forward 谁也到不了 ⇒ `agent_no_responders` ⇒
`cluster upgrade` 在 **broker 已 reload 而 agent 未升**的状态 HALT。
**裁定：co-located broker-host agent 豁免租约**（broker 主机按构造是单实例，不是克隆镜像），
在 broker 侧强制并加测试；同步订正 `docs/cluster.md:308`。

### Q5 — **永不提升**，接管是默认
**结构性强制**：`processes`/`port_allocations` 的 `(sid,nid)` 外键 `ON DELETE CASCADE`、**无 `ON UPDATE`**、
`foreign_keys` 强制 ON ⇒ 有子行时改 PK 直接失败（实测 `FOREIGN KEY constraint failed`）。
替代方案要么 delete+reinsert（级联掉 D6 要保的历史），要么 `ON UPDATE CASCADE`（改写历史行的 nid，即 D6 违规本身）。
wire 上也对：nid 嵌在 CONNECT 时铸的 JWT 权限字面量里，改名只能表达为强制断连。

**运维会看到什么（必须写进文档）**：持有者死后，`node ls` 显示 basename OFFLINE 旁边一个活的 `-02`
（读起来像**错的那台**死了）；之后新到达者让 basename 重新 ONLINE，但那是**另一台机器** ——
本轮无法从审计回答（需 Stage 3 的 `instance` 键）。

**（G6）自愈属性，必须大声写**：后缀是 **session-scoped 且从不持久化**，agent 永远从 agent.yaml 的 basename 启动
⇒ **一个被误后缀的独占 agent 会在下次重启时自动回到 basename**。
残留因此从"永久改名"降为"暂时改名、自我纠正"。写进 plan、`docs/usage.md`、并加测试。

---

## 3. 核心机制

### 3.1 端到端流程

1. agent 启动，取/生成 `instance_id`（纯内存）
2. 用**今天的**三段 CONNECT name 连接：`tether-agent:<sid>:<basename>`
3. register，带 `InstanceID`（additive/omitempty）
4. broker 裁决（见 3.2）
   - **未争用** ⇒ 回复**不含** `Lease` 键 ⇒ agent 什么都不做 ⇒ **与今天逐字节相同**
   - **争用** ⇒ 回复 `Lease{AssignedNID:"…-02"}` 且 **什么都不碰就返回**（G5）
5. agent 见非空 `Lease` ⇒ **整个会话重建**，用同样的 name 形状、nid 换成租约名
   - 第二次 CONNECT 由 auth 的 **suffix fallback**（3.4）放行
6. 此后它就是一台名叫 `foo-02` 的普通设备

**关键既有事实**：`session()` 的顺序是 connect → **register** → **subscribe** → heartbeat
（`internal/agent/agent.go:814` 文档 + `:896`/`:925`）⇒ **被拒的实例从未安装 basename 的 forwarded 订阅**
⇒ 我早先担心的"过渡期两实例共订同一 subject"的窗口**根本不存在**，无需额外 fail-closed 机制。

### 3.2 裁决规则（G1 + completeness B1，主进程采纳 B1 的修正）

judge 的 G1 写作 `contested := … && holder != "" && …`。**completeness B1 指出这有冷注册表洞**：
`holder` 是 leader-local 内存表、只从 register 播种，**broker 重启或 leader 选举后为空**
⇒ `contested` 恒 false ⇒ **两个活克隆依次都被授予 basename，扇出缺陷静默复活**。

**采纳 B1 的修正**——用持久化在 SQLite 的 `nodes.last_heartbeat_at` 驱动，内存 holder 降级为探测避免的快路径：

```
contested := req.InstanceID != "" && heartbeatAge(sid, nid) <= LeaseGrace
             && (holder == "" || holder != req.InstanceID)
if contested {
    // tie-break：现任是不是一个活进程？
    // nc.Request(SubjCmdForwarded(sid, nid, "claim-probe"))，200ms 预算
    //   ErrNoResponders -> 现任 socket 已消失 -> 授予 basename
    //   有响应者         -> 活着的克隆        -> 分配后缀
    //   超时             -> fail-safe        -> 分配后缀
}
```

**为什么探测是 tie-break 而不是裁决器**（judge 对候选 B 的驳回，采纳）：
`handleRegister` 是异步订阅 handler，nats.go 每订阅一个 goroutine ⇒
把阻塞探测放在主路径会**串行化全车队的 register**；且路由网格上的 interest 传播是异步的，
一个活着但远端的现任可能读作已消失。放在歧义窗口内则：真克隆时现任本地 ~1ms 应答；
硬杀重启时 `ErrNoResponders` **瞬时**返回（不是超时）；200ms 预算只在分区时付，而那时答案本来就是"后缀"。

**零 ACL 变更**（已验证）：agent 的 Sub `…cmd.node.<nid>.*.req.forwarded`（`permissions.go:215`）
与 broker 的 Pub `…s.*.cmd.node.*.*.req.forwarded`（`:250`）都是 **full-token 的 verb 通配**
⇒ 新 verb `claim-probe` 两侧都不需要新授权；旧 agent 落 `dispatchForwarded` 的 `default:` 分支（Warn 后返回），无副作用。

**LeaseGrace 的算术（G1 订正 C 的错误）**：心跳在 T，死在 T+ε（ε∈[0,5]），systemd 等 5s，
register 在 T+ε+5+boot。`LeaseGrace` 取 `HeartbeatInterval + 1s = 6s` 时，任何 ε+boot<1s 都落在窗口内
⇒ 硬杀的独占 agent 约 **20%** 概率进入 contested —— 而探测会把它救回来（`ErrNoResponders` 瞬时）。
**这正是探测存在的理由**，不是缺陷。

### 3.3 争用的 register **什么都不碰**（G5，本 plan 最重要的单条排序）

contested 分支必须：**不调 `registerNode`、不调 `reconcileOnRegister`、不发 `pubSysEvent`、
不发 proxy directive、不打 `broker: node registered` 日志**，直接回复 `Lease` 并返回。

这一个排序选择 **by construction** 修好了：
- **`proxy_ready = 0` 的互踢**（§0.5）—— 克隆到不了 mutator，round-6 F8 的本意逐字保留
- **跨克隆 reconcile 湮灭**（§0.3）—— `reconcile.go` **一行都不用改**

⇒ 我在专家产出前起草的"不相交 `LocalProcesses` 检测"止血方案**整条作废**，比它更简单也更彻底。
用 AST 排序守卫钉住（`internal/broker/admit_ordering_test.go` 的形状）。

### 3.4 auth 的 suffix fallback（plan 里价值最高的单处改动）

在 `ensureAgentProvisioned` 的 `ErrNotProvisioned` **分支内**：若 `nid` 可拆出 basename 且
`agentprov.Lookup(sid, basename)` 返回**这一个** fingerprint ⇒ allow 并铸 `PermissionsForAgent(sid, nid)`。
**auth 路径上保持严格的 `ValidateNID`。**

没有它，后缀 agent 被拒，而 `connectNATS` 把**初始** auth 失败当**致命** ⇒ 每个克隆在 `RestartSec=5` 下 crash-loop。
同时改进 `handler.go:347` 的拒绝串——它读起来像"首次启动指引"，对无人值守的克隆是误导。

---

## 4. wire / 闸门 / migration

### 4.1 wire（Stage 2；`ProtoVersion` 保持 2）

| 消息 | 新字段 | 零值语义 |
|---|---|---|
| `NodeRegisterReq` | `InstanceID string`（omitempty，独立 validator） | 缺失 = 前特性 agent |
| `NodeRegisterReq` | `LeasedNID bool`（omitempty） | false = 持 basename |
| `NodeRegisterResp` | `Lease *NodeLease`（**指针** + omitempty，`Proxy`/`Home`/`Roster` 先例） | 缺失 = 未争用 ⇒ 回复无 `lease` 键 |
| `NodeListEntry` | `Leased bool`（omitempty，仅 true 时设） | 缺失 ⇒ multiplicity-1 的 `--json` 逐字节不变 |

**不新增 capability token**：非空 `InstanceID` **本身就是**广告 ——
这正是让每个 agent 的 `capabilities` 保持恰好 `["proxy-v1"]` 的原因。

**⚠ I1 的唯一诚实让步（G7/I1-breaker）**：`internal/agent/agent.go:1215` 是
`Capabilities: []string{proto.CapProxyV1}` 这个**单元素** slice。本设计**不往里加 token**，
因此 register body 的**唯一**变化是新增 `"instance_id"` 键。**本 plan 不宣称"逐字节不变"，
而是声明"delta 恰好是 instance_id 一个键"，并用 §5 的 T1 机械钉住。**

`-update-wire-inventory` 追加；**`internal/proto/testdata/golden/*.json` 必须不变**
（变了就说明字段不是真 omitempty）。手改 `internal/broker/wire_freeze_test.go` 的
`proto.NodeRegisterReq` 键集并写跨版本理由。

### 4.2 闸门预算（Md3 + 主进程实测）

| 闸门 | 影响 |
|---|---|
| `type-methods internal/agent.Agent` **127** / `internal/broker.Broker` **285** | **精确双向** ⇒ 新逻辑用**接受 `*Broker`/`*Agent` 的包级自由函数**，不加方法 |
| `pkg-files internal/broker` **70** | **精确** ⇒ 新文件落 `internal/node/lease.go`，broker 侧逻辑并入既有 `broker.go` |
| `pkg-code-lines internal/broker` | **实测被突破，本轮上调**（broker 14000→16000、agent 4000→6000；agent 那条在外审整改中来回跨过一次边界，两次都手改本文件，这正是棘轮在工作而不是被绕开）。上面那条"用包级自由函数"只挡住了 **type-methods**，挡不住**包体积**——这正是该维度当初被加进来的理由。理由、债务上限与回收计划写在 `structural_budget_golden.txt` 的注释里（外审 F16：第一次上调没写任何解释，是本闸门要防的失败模式本身）|
| `test/cluster/equiv_test.go:312`（DIFF-1） | **B3：本轮不加 `nodes` 列 ⇒ 不动**（Stage 3 才需要） |
| `test/determinism/docs_wire_version_test.go:56` | `"docs/architecture.md": 69` 是逐文件计数，文档改动会移动 |
| `promised_guard_test.go` / `origin_line_test.go` | 注释点名的测试必须存在；`// origin: docs/...` 路径必须存在 ⇒ **plan 文档与代码同一 commit** |
| `gate_registry_test.go` | 新闸门须同时进 CLAUDE.md §5 与 `make gates` |
| `cfgdb_ratchet_test.go` | 钉死 `handleRegister: 1` **双向** ⇒ 裁决必须走 `b.read()`，不是 `b.cfg.DB` |
| CLI 表面 golden | 仅 `--all` 过滤与 `ready_reason` 相关变动 |

### 4.3 migration
**本轮零 migration**（D6 推迟）。

> **外审 F1 的订正记录（保留过程，不覆盖）**：实现期间一度新增了
> `0019_nodes_leased.sql`，直接违反本条。外审同时指出更硬的约束：
> `g5-plan.md` OQ-2 写明「same-proto rolling releases MUST NOT … add migrations」——
> 未执行该 migration 的 follower 会在 Apply 一条点名未知列的 register command 时失败，
> 那是集群级事故，不是显示缺陷。**裁定：撤销 migration**，租约与否改由
> `agent_provisioning` 行推导（两种模式本就同路复制），因此本条恢复为字面真值。

---

## 5. 测试清单（每条附必须变红的变异）

| 测试（按被测不变量命名） | 变异 |
|---|---|
| `TestSingleInstanceRegisterWireIsFrozen` — 一个**真** agent 对 `testharness.StartNATS`，捕获 `SubjNodeRegister` 原始字节，delta 恰为 `instance_id` | 往 `Capabilities` 加第二个 token。**负向对照**：同样改动下确认既有 `proto/golden_test.go` 仍**绿** —— 这正是本 gate 不冗余的证明 |
| `TestAgentAndBrokerPermissionsUnchanged` | 显式加 `claim-probe` 授权，而非依赖已验证的 full-token 通配 |
| `TestSingleInstanceSubjectSetUnchanged` | 强制给独占 agent 分配后缀 |
| `TestLoneAgentRestartKeepsItsName`（N≥5，`-race`，泄漏门） | 去掉探测 tie-break **或** 把 `LeaseGrace` 抬到 `OfflineAfter` ⇒ ε≈0 的硬杀重启被后缀 |
| `TestMisSuffixedAgentRevertsOnNextRestart`（G6） | 把分配的 nid 落盘 ⇒ 回归断言失败 |
| `TestContestedRegisterTouchesNothing` | 让 contested 分支落到 `registerNode` ⇒ `proxy_ready` 被清零、现任 RUNNING 行被 `reconciled_closed` |
| `TestTwoInstancesDoNotAnnihilateEachOthersProcessRows`（**mode-aware**，B4） | 两个 agent 配同一 nid（今天的状态） |
| `TestSingleExecReachesExactlyOneInstance` | 同 nid ⇒ 断言**交错**的 stdout 作为修复前基线（不是"先到先赢"） |
| `TestSuffixedInstanceDiscardsInheritedTokensAndNeverDials` | 去掉 replay gate；**第二个变异**：让丢弃也扔掉 `Roster` |
| `TestBasenameGrantedInstanceReplaysIdentically`（**I1 对照**） | 让丢弃无条件执行 |
| `TestSuffixFallbackAuthorizesOnlyTheBasenameFingerprint` | 去掉 fingerprint 比较；把 `ValidateNID` 换成宽松变体 |
| `TestInstanceIDSurvivesExecAndIsStrippedFromChildren` | 去掉子环境剥离 |
| `TestInheritedInstanceIDIsRefusedWhenSessionLive`（M2 第二条） | 去掉继承守卫 ⇒ 两者共用一个名字、扇出复活 |
| `TestInstanceIDIsNeverPersisted` — **对 `state.go` 序列化结构做 AST**，不是全树 grep | 给 `PortToken` 加一个 `InstanceID` json 字段 |
| `TestGracefulGoodbyeClearsOnlyItsOwnHolder`（`-race`） | 去掉 Release 里的 iid 相等判断 |
| `TestRosterRefreshNeverTouchesLeaseState` | 把裁决移到 `RosterRefreshOnly` 短路之上（3 分钟刷新不带 `InstanceID`，会被读作 legacy claim 约 20 次/小时/agent） |
| `TestClaimPrecedesRegisterNode` — AST，`admit_ordering_test.go` 形状 | 交换两次调用 |
| `TestClaimIsIdempotentAcrossReconnects`（N=20） | 去掉 `(sid, instanceID)` 匹配 ⇒ 每次重连烧一个新后缀 |
| `TestSuffixBudgetAndOrdering` | 不填充（`-10` 排在 `-2` 前）；截断而非拒绝（两个 30 字符 basename 合并）；放宽 `idCharset`（断言 `ValidateSID` 契约破裂） |
| `TestLeasedInstanceIsProxyIneligible` | 去掉 `&& !LeasedNID` 合取项；改用 `OptedOut` 渲染 |
| `TestColocatedAgentIsExemptFromLeasing`（B2） | 去掉豁免 ⇒ `cluster upgrade` 的 reexec forward 无人接收 |
| `TestNodeListCappedAndUpgradeAllFailsClosed` | 无条件 `NodesTotal`（字节等价性死掉）；忽略 `NodesTruncated`（静默的部分车队升级） |
| `TestForcedMultiInstancePathRedensEveryI1Gate`（**G3，全套的验收判据**） | **强制打开**多实例路径跑完整个单实例 gate 集合，断言**每个** I1 golden 变红 —— 保持绿的那个就是恒等式测试 |
| simcluster `83-cloned-image-instances.sh` | agt2 由复制 agt1 **整个** `~/.tether` 造出、`/etc/machine-id` 清空作 D1 常设负向对照。**必须先对修复前代码观察到 PRODUCT-RED 并记录** |

**成本预告（用户应当提前知道）**：新增 1 个 simcluster drill（~5–12 min），
并重跑 `81-admin-evict-session-rm`、`94-agent-reconcile`、`31-node-upgrade-fleet` 记录 verdict 移动。

---

## 6. N-1 四象限（Stage 2；顺序：broker 先，agent 后）

| | 旧 agent | 新 agent |
|---|---|---|
| **旧 broker** | 今天的行为（扇出、交错、reconcile 湮灭、flap 全部存在）—— 这是**修复前基线，不是回归** | agent 用**冻结的**三段 CONNECT name ⇒ auth 与今天逐字节相同；旧 broker 忽略两个未知 JSON 键、不回 `Lease`。**agent 必须把 nil `Lease` 当 legacy 模式且永不重建**（有测试钉住）⇒ 今天的行为。**无认证拒绝、无 crash loop**。broker 回滚同样安全 |
| **新 broker** | contested 要求**两个** id 都非空 ⇒ **永不触发** ⇒ 与今天逐字节相同，**包括克隆扇出**。文档化 **[GAP]**，非回归 | 完整特性 |

**集群子象限**：无新 raft op、无新复制列 ⇒ `decodeCommand` 永不见未知 op、无副本 poison-skip、
`fsm.Apply` 不会因缺列 panic、**日志重放式回滚仍合法**（不扩展 #74）。
holder 注册表是 leader-local 且 `handleRegister` 仅 leader 处理，选举后由各实例下次 register 重新播种。
**tunnel wire 不变**，恒为 6 字段。
**ctl 两个方向都免费**：`jupyter-ziyang10-02` 已是合法 nid，`cmd/tether` 只在一处校验 nid
⇒ **未升级的 v0.5.0 ctl 零改动即可寻址后缀实例**。

**必须写给运维的一句话**：**broker 单独升级阻止不了双执行** ——
扇出是 agent 侧的无条件 `nc.Subscribe`，所以危险窗口由**镜像何时重建**决定，不由 broker 上线决定。

---

## 7. I2 破例账本（只减不增）

1. **`node upgrade --all` 排除租约实例** —— 真设备该被升级；易逝实例重启即回归，且作为字典序 canary 会中止全车队滚动。
2. **proxy eligibility 仅 basename 持有者** —— 一台独立设备 eligible，一个租约实例不 eligible。
3. **凭证生命周期永久 basename 粒度** —— D3 下不可能是别的：一个 nkey、一个 fp、一行 provisioning、一次 PIN 引导，且 PIN 从不持久化。
4. **expose 跨租约交接是拆除重建，不是静默转移**。

---

## 8. 风险与残留

| # | 风险 | 处置 |
|---|---|---|
| R1 | **（M1）gotcha #72「分区但活着」在本车队 LIVE-CONFIRMED** —— weilandserver 2026-08-01 卡了 **10m58s**，systemd `active (running)`、`NRestarts=0`、`:443` ESTABLISHED、数据面活着，而 broker 的 `node ls` 里没有它 | **不是假设，是这个车队的实测。** 必须裁定：一个卡了 11 分钟的 agent 恢复时会遇到什么（改名 / 被拒 / replay 被 gate）。在 gotcha 台账双向交叉引用。注意 #72 的恢复本身已产生 `revoke_ports=2`，新设计不得再叠加一次改名 |
| R2 | **（M3）broker 重启后 basename 归属竞速** | 由 §3.2 采纳 B1（用持久化的 `last_heartbeat_at` 驱动）关闭 |
| R3 | **（M7）运维无法把后缀行映射回 pod** —— 而那一行就是整个交付物 | 唯一可用手段是 `tether exec <租约名> -- hostname`。**必须写进 `docs/usage.md` 的运维流程**，否则多出来的那行不可调试 |
| R4 | **（M6）`agent doctor` 从不连接** —— 它报的是 agent.yaml 的 basename，无法告诉运维自己实际叫 `-02` | **不要让它连接**。最低限度加一行说明：这是**配置的** nid，broker 可能已租出别的显示名 |
| R5 | **（Md5）goodbye 只覆盖 SIGTERM** —— 而 #72 的修复方向恰恰是"卡住时非零退出让 systemd 重启"，那条路径不会发出 goodbye | 明确写出哪些退出路径会发 |
| R6 | **（Md4）14008 的真实机制未定案** | **阻塞 plan 而非实现**：Stage 0 必须先拉 broker 日志的到达间隔 |
| R7 | 旧镜像上的双执行持续存在 | broker 单独升级救不了（见 §6）。窗口由镜像重建决定 |

---

## 9. Stage 0（无代码，阻塞）

- **0.1 文档先行**：`docs/requirements.md` §7.5 与 `docs/usage.md` FAQ ~1775 都断言了一个**没有任何代码实现**的
  duplicate-nid 拒绝（`node.Register` 是 UPSERT）。改写为：nid 是每 session 的**槽位**，占用是租约，真身份是每进程的。
  §6.1 的封套补 `MaxInstancesPerBasename`。**文档先于代码**（CLAUDE.md §2）。
- **0.2 基线取证**（**已完成**，见 §0.1，存 `prefix-baseline/`）。
- **0.3 四个 spike**：(a) `nats stream info history-lab` 的 `State.Bytes` 对 1 GiB `DiscardNew` 上限；
  (b) racknerd 的 `broker.yaml` 是否开 clusterMode（决定 raft 风险是活的还是潜伏的）；
  (c) pod 内 `~/.tether` 是否共享挂载（决定升级 flock 与 `state.json` 互踩）；
  (d) 对 `testharness.StartNATS` 确认 `ErrNoResponders` 行为与 `kill -9` 后订阅消失的延迟。

---

## 10. 后续增量（本轮明确不做）

Stage 1.3/1.4 · Stage 3 全部（`nodes.instance_id`、audit `instance` 键、rename 纠正、
`admin evict` 的租约拒绝与实例级恢复、`tether exec` liveness watchdog、drills 84/85）·
`exec --all` · **`_INBOX.>` ACL 缺陷（应排在本增量之前）**。
