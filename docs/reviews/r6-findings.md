# R6 定案实验 — 裁定记录

Date: 2026-07-19 · 计划：`docs/reviews/r6-plan.md` · **只取证，不改实现**

> 输出契约：每条五元组【假说 → 可证伪预测 → 实测证据 → 裁定 → 归属批】。
> 裁定只有 `CONFIRMED-DEFECT` / `REFUTED` / `AS-DESIGNED`。**禁止「可能/疑似」**。
> 定不了案的写「实验设计 X 已执行、证据不足、需 Y 条件」并归 R14——
> 不得默认为缺陷，也不得默认为非缺陷。

---

## 已裁定

### 95-D（DELETING 会话 boot-resume）→ **REFUTED（假缺口）** · 归属 **R13**

- **假说**：该 not_covered 的自陈理由是「N=2 下 raft 与 JS 共命运、无法解耦，配方前提不成立」，
  即结构性不可构造。
- **可证伪预测**：若真是结构性的，那么判定用的谓词应当**与 arm D 要测的东西相关**；
  若谓词其实是因为**无关原因**失败，则该缺口是谓词过严造出来的假象。
- **实测证据（源码级，无歧义）**：
  - `drills/95-broker-selfheal.sh:80` `_d_raft_ok() { … jq -e '.leader_id=="brk1"' … }`
    ——**硬钉 leader 必须是 brk1**。
  - 同文件 `:70` `_broker_live() { … jq -e '.leader_id != null' … }`
    ——同一个 drill 里**已经存在**正确的宽松写法。
  - 而 arm D 之前，`T1a`（SIGTERM 到 MainPID）与 `T2a`（SIGKILL）**两次干掉 brk1 的 broker**。
    N=2 下两次重启后 leadership **完全可能停在 brk2**。
  ⇒ `_d_raft_ok` 会因为「leader 恰好不是 brk1」而失败，这与 arm D 要测的
  **DELETING 会话 boot-resume** 毫无关系。
- **裁定**：**REFUTED** —— 这不是结构性缺口，是**谓词过严**造成的假缺口。
- **归属批 R13**，且**处置方向锁定**：把谓词收紧到**正确语义**（leader 存在且稳定，
  不要求是 brk1），并**必须在同 drill 内新增一条负向臂证明新谓词能红**——
  谓词只有在能红的时候，它的绿才有意义。**不得**简单删掉这条 not_covered 了事。

### Q2（被分区少数派对客户端表现为连接黑洞）→ 机理已收敛，**待实测确认**

- **已确立的源码事实**：
  - `internal/authcallout/handler.go:102-104` `fenced()` 的注释说「nil predicate => never fenced」，
    但 `internal/broker/authcallout.go` 在集群分支里**确实接线**了
    `h.LeaderContactStale = b.cl.node.LeaderContactStale` ⇒ **「少数派应当明确拒绝」是真设计意图**，
    报告里「设计意图侧」的判断成立。
  - 同文件的 callout 订阅是 **`QueueSubscribe`**，注释明写
    「≥2 节点集群里**恰好一个** broker 应答每次 callout」——即 authcallout 是一个**跨 broker 的队列组**。
- **新假说（比报告的三个候选更具体，且可证伪）**：brk1 被**静默分区**（丢包而非关闭连接）时，
  brk1 的 nats-server 仍认为远端 queue 成员存活，于是把 callout 请求投给**够不到的远端**，
  回复永不返回 ⇒ 客户端超时 = 黑洞，而**不是** brk1 本地 handler 拒绝。
  这对应报告候选机理**①**，而非②（handler 出错时直接 return 不 Respond）。
- **可证伪预测**：分区期间从 ctl 连 brk1:4222，**brk1 的 broker journal 里不会出现任何 authcallout
  处理痕迹**（既无 deny、也无 `authcallout: handle failed`）。若反而出现 handle 记录 ⇒ 本假说被证伪，
  回到候选②。
- **状态**：实测取证进行中。

### #34 的 C-auto 半边（auto-rebalance-on-return 不发火）→ **AS-DESIGNED（门在按设计工作）** · 但**上游成因是 #31** · 归属 **R7**

- **假说**：报告 R7 订正称「C-auto 不发火的实测根因是 fire-gate 看到 in-flight op 后**正确 DEFER**，
  不是自动化失灵」。需源码确证，否则不能据此把它排除出根因族。
- **可证伪预测**：若为真，`gatesClear` 的构成里应当**显式包含**「无在飞 op」这一项；
  若不含，则 DEFER 另有原因，报告的订正不成立。
- **实测证据（源码级）**：`internal/broker/proxy_auto_rebalance.go:118`
  ```go
  gatesClear = len(downNow) == 0 && !forceSingleActive(b.cfg.DB) && b.noInflightOps() && !b.recentProxyRehome()
  ```
  `b.noInflightOps()` **确在其中**；`:122` `if !b.autoRebalanceArm.tick(returned, …, gatesClear) { return }`
  ⇒ 有在飞 op 时 arm 不 fire。**报告的订正成立。**
- **裁定**：**AS-DESIGNED** —— 这半边不是自动化失灵，
  **确认它不属于总纲 §3.4 的根因族**（此前的排除是对的）。
- **但必须同时记住**：实测中那个在飞 op 是 `brk3 join in_progress`，
  而它之所以**滞留**，是 **#31 grow-lock 泄漏**（同族的 retire/upgrade 阻塞根因）。
  ⇒ **门是对的，滞留的 op 才是缺陷**。#31 修好后 auto-rebalance 才谈得上能不能发火。
- **归属批 R7**（#31 所在批）。drill 74 的 Arm C **在 #31 修好之前无法给出有意义的判定**，
  这一点须写进 R7 的出口断言，否则 Arm C 会在 R7 之后仍以「auto 不发火」的面目误报。
- **附带事实**：`autoRebalanceEnabled()` 要求 env `TETHER_AUTO_REBALANCE=on`（`:104`），
  机制默认惰性 —— 这是有意的 opt-in（KD-3b black-hole-bounded invariant），**不是缺陷**。

### #34 的分布漂移半边 → 仍为 **CONFIRMED-DEFECT**（未变）
`cluster rebalance proxy` 能构造出 1/1/1（spread==0），但随即漂回全堆 tunnel/leader broker（实测 3/0/0）。
这是控制面 home 计数直读、无歧义。与上面的 C-auto 半边**是两回事**，不得混为一谈。

### Q2（被分区少数派）→ **CONFIRMED-DEFECT**（黑洞机理）+ 两条对报告的修正 · 归属：产品批 / drill 批 / **R14**

- **实测**：N=3、brk1 静默 DROP 6222+7400、保留 4222，12 次**单发**连接逐次统计三台的 callout 应答归属：
  - **7/12 total=0**（没有任何 broker 应答该 callout）＋ brk1 `/varz http=200`、`MainPID=395 NRestarts=0`、
    `routez.num_routes=0` ⇒ 请求被投给够不到的远端 queue 成员、auth_timeout 到期。**新假说成立**。
    耗时严格量化在 2.1s / 4.1s = 1 或 2 个 `auth_timeout=2`。
  - **2/12 rc=77**，brk1 **本地 handler 确实跑了并拒绝**：
    `authcallout: ctl deny … err="fenced: node lost leader contact (retriable)"`
  - **3/12 rc=0**：brk1 已正确自我 fence，**远端 broker 仍放行、客户端拿到完整授权**。
- **裁定**：
  - `CONFIRMED-DEFECT` —— 黑洞机理 = **跨 broker queue group 把 callout 投给够不到的远端**（报告候选①）。
  - `REFUTED` —— 报告候选②（handler 出错直接 return 不 Respond）：三台 `broker.err` 中
    `authcallout: handle failed` **出现 0 次**，deny 走 `resp.Error` 正常 Respond。
  - `AS-DESIGNED` —— **fenced 拒绝是活的**，以 rc=77 正确呈现给用户。
    ⇒ **报告「少数派完全没有 fail-closed 拒绝、只有黑洞」这一半被证伪。**
  - **新事实（未定性，归 R14）**：3/12 的 rc=0 越权放行是否违反 distributed-broker §3.2/§6.2 的
    fail-closed 不变量，属**设计文档裁决**，不是实验能定的。
- **两条使原 oracle 结构性失效的发现（本身就是产出）**：
  1. `tether-broker` **不向 journald 写应用日志**（unit 是 `StandardError=append:/var/log/tether/broker.err`）
     ⇒ 计划里「grep journal 找 `authcallout: handle failed`」**永远抓不到东西**。
  2. `roleCtlUnactivated`（`session create`/`session ls`）在 `handler.go:162` 直接 `return h.allow(...)`，
     **无任何 Logger 调用** ⇒ 这一路的 allow 永远不可见。探针必须改用 **activated** 命令（`node ls`）。

### Q4（session create 写已提交却报失败）→ **CONFIRMED-DEFECT**，机理① · 归属：产品批

- **实测**：分区中经存活方 brk2 写，14 次全部非零，**第 1 次**就是落地时刻：
  `rc=70 elapsed=1.37s` `error: session create failed: broker: session "canary2" not visible after commit (apply lag)`
  而 `session ls --json` 经 brk3 读回 `"sid":"canary2","state":"ACTIVE"`。
- **裁定**：机理**①成立**（`clusterwrite.go:799` 的 `readCommittedSession` 50×20ms=1s 上限不够，实测 1.37s）；
  机理**②`REFUTED`**（1.37s 远未触及 `session.go:49` 的 5s ctx，且 rc 是 **70** store_error 不是 69）；
  机理**③`REFUTED`**（远未触及 harness 的 `timeout 15`）。
- **复合缺陷（独立成立）**：`session create` **非幂等**——首次超时后每次重试都是 `already_exists`+rc=70，
  `poll_until` **结构上永远转不了绿**；`error_hints.go` 无 `already_exists` 条目。
  ⇒ 这解释了 drill 96 的 D3 为何 60 次全红而 D6 又能读回 canary2。

### Q3（exec rc=0 但进程表不记 RUNNING）→ **REFUTED**（不是产品缺陷，是 drill fixture 缺陷）· 归属：drill 批

- **实测**：`ps -a --json` 30 次/60s 恒定显示该 ULID `status=EXITED`、`started_at`/`ended_at` 相差 **3ms**；
  `history --kind proc` 有成对 start/exit rc=0；agt2 journal `agent: exec` 无 spawn 失败；
  OS 侧 `sleep 9663` 确实活着（已 reparent 到 PID 1）。
- **裁定**：`REFUTED`。tether 跟踪的是它自己 spawn 的**直接子进程** `sh -c …`（`internal/agent/exec.go:192` 的
  `cmd.Wait()`），而 `nohup … &` 把 sleep 后台化后 sh 在 3ms 内正常退出 ⇒ 该行**必然** EXITED，
  **永远不可能** RUNNING。预测的两种缺陷签名一个都没出现。
- **drill 自相矛盾**：同一个 drill 的 `_f3_agt1_exited` 对**完全相同的 argv 模式**断言 `status=="EXITED"`，
  而 `_f0c_capture_agt2_seed`/`_f4_agt2_seed_survived` 却要求同样的行是 `RUNNING`。
  机理与集群规模无关（3ms 子进程退出与 N 无关），故 N=1 证据对 96 的 N=3 场景同样成立。

### OQ-2（真不可中断-D 可行性）→ **T3 填不满 ⇒ 锁定 (c) 分支** · 归属 **R14**

- **已定死的终局结论**：**容器 netns 内构造真 kernel nfsd 不可行（内核级）**。
  `modprobe`/`mount -t nfsd`/`exportfs` 全部 rc=0，但 `rpc.nfsd` 报
  `writing fd to kernel failed: errno 111 (Connection refused)` ⇒ `threads=0`。
  knfsd 的 socket 注册**不是 network-namespace 可移植的**；overlayfs / kmod 缺失 / 版本参数三种可能已逐一排除。
- 余下两条路径本批**不可执行**：host-netns 变体（脚本已写好，启动被环境安全分类器拦下）；
  一次性 VM（`/dev/kvm` 在，但 qemu/virsh 均未安装且 `sudo -n` 需口令）。
- **裁定**：`实验设计已执行、证据不足以填满 T3、需 Y 条件`。按 R6 plan §2.4 →
  **锁定 (c) 分支，收官声明形态定为「36/37 GREEN + 1 条已披露缺口」，本批已定、不再拖到 R15。**
- 解锁条件：① 拿到 sudo 口令装 qemu-kvm 用一次性 VM；**或** ② 显式批准以 `--network host` 起临时 kernel nfsd。

> **须披露的环境残留**：宿主内核的 `nfsd` 模块被 `mount -t nfsd` 自动加载，现为 refcount=0、0 线程、
> 无导出、2049 无监听（完全惰性，重启即消失）。卸载需 root，本批无口令未执行。


---

## 待裁定
`Q1`（account.nk 轮换 A/B/C 三假说）·
`#33` `#35` `#45` `#48` `#49` `#55` `#59` `#63` `#65` · `96:240` arm B 是否 source-closed ·
`OQ-2` 可行性（决定收官声明形态：真跑 HW-1 或锁定「36/37 GREEN + 1 条已披露缺口」）

## 已裁定汇总
| 项 | 裁定 | 归属批 |
|---|---|---|
| 95-D | **REFUTED**（谓词过严造成的假缺口） | R13（收紧谓词 + 必须加负向臂证明能红） |
| #34 C-auto 半边 | **AS-DESIGNED**（fire-gate 正确 DEFER）；上游成因是 #31 滞留 op | R7 |
| #34 分布漂移半边 | **CONFIRMED-DEFECT**（未变） | R8 |
| Q2 黑洞机理 | **CONFIRMED-DEFECT**（跨 broker queue group 投递给够不到的远端） | 产品批 |
| Q2 候选机理② | **REFUTED**（`handle failed` 0 次） | — |
| Q2 fenced 拒绝 | **AS-DESIGNED**（活的，rc=77）⇒ 报告该半边被证伪 | drill 批（`assert_refuses` 钉住） |
| Q2 rc=0 越权放行 3/12 | 事实已钉死，规范定性待设计裁决 | **R14** |
| Q4 | **CONFIRMED-DEFECT** 机理①（apply-lag 1s 窗口不够）+ 非幂等复合缺陷 | 产品批 |
| Q3 | **REFUTED**（drill fixture 自相矛盾，非产品缺陷） | drill 批 |
| OQ-2 | **T3 填不满 ⇒ (c) 分支锁定：36/37 GREEN + 1 条已披露缺口** | R14 |
| **#65** | **REFUTED**（台账 5/6「持久」实为 5 次正确的多数派提交；少数派结构上无法认证新连接） | **不触发 R8x** |

### #65（分区少数派 stale-leader 写有时持久）→ **REFUTED** · **不触发 R8x，不阻断全绿宣布**

- **假说**：N=3 分区少数派 brk1 作为 stale leader 接受的写有时变持久（raft 安全性破损）。
- **可证伪预测**：P1 该写由 **brk1** 提交；P2 分区期该写**在 brk1 本地存在、在 brk2/brk3 不存在**；
  P3 客户端只能够到 brk1 时，少数派**会**接受写。
- **实测证据（10 轮判定组 × 5 扫描点 = 50 次 + 3 轮解释组，solo 与并发两档结果完全一致）**：
  - **P1 证伪**：broker 提交日志（`msg="broker: session created"`）显示所有 rc=0 的写
    **全部由 brk2/brk3 提交，brk1 提交数 0**。更致命的对照——**分区发生之前**经
    `--nats-url nats://brk1:4222` 建的 `canary1` **也是 brk3/brk2 提交的**。
  - **P2 证伪**：分区期直读三台 SQLite，该写是 `brk1=no, brk2=yes, brk3=yes`——**与预测正好相反**，
    它从未落在少数派上。
  - **P3 证伪**：50/50 次钉在 brk1 上的尝试全部 rc=69（TCP 连得上，CONNECT 读超时）；
    brk1 日志在分区后**再无任何 `authcallout:` 行** ⇒ 被隔离的少数派**无法认证新客户端连接**，
    **结构上不可能接受写**。
- **裁定**：**REFUTED**。**无需触发条件批 R8x；#65 不阻断全绿宣布。**

**台账旧记录（6 轮中 5 次持久）的成因 = 两层测量错误叠加，都不在 raft**：
1. **归属错误（主因）**：drill 96 的 D4b/D6b **从未验证是哪台 broker 提交了 canary3**，
   把「经 brk1 拨号」当成「brk1 提交」。而控制面 RPC 是**跨全部 broker 的 NATS queue group**——
   `--nats-url` 只决定 ctl 连哪台**入口服务器**，**不决定哪台 broker 处理请求**。
   ⇒ 台账里那 5 次「持久」，是 5 次**正确的多数派提交**。
2. **H6 的空绿 / rc 误记**（R5 已修）污染了同一批地面真相。

> **与 Q2 的机理汇合**：同一个 queue-group 事实，既解释了 Q2 的黑洞（callout 被投给够不到的远端），
> 也解释了 #65 的误判（写被路由到多数派）。**两条独立结论指向同一个此前没被认识到的架构事实。**

**必须转达的两点**：
- **drill 96 的 D6b 即便在 H6 修好之后，仍不具备判定 #65 的能力**——它缺「提交者归属」这一环，
  只要 ctl 能够到多数派，它就会把**合法写**记成 #65 候选。须改为
  **「提交者归属 + 分区期多数派可见性」双条件**，而非仅凭愈合后可见性。归 **drill 批**。
- **未覆盖的残留变体（保留意见，非裁定的一部分）**：本实验每次 `session create` 都是**新建连接**，
  故只证伪了「新连接经少数派写入」这一条（即 #65 原文场景）。
  **分区前已认证、持有长连接的客户端**在窗口内写入的路径**未被触及**——CLI 是短生命周期进程，
  结构上跑不到。闭合该变体需条件 Y = 一个长连接写入客户端。归 **R14**。

---

## R6 余项裁定（2026-07-19）

### Q1（account.nk 轮换）→ **A/B/C 三假说全部 REFUTED**；真因是 drill 的健康门结构性不可满足

- **实测**（N=2，27+39 次快照，每次**同采**渲染的 `nats.conf` issuer + 磁盘 seed pub + `broker.err`）：
  - gen2 落盘但未重启的 180s 内：conf issuer 保持 **gen1**、磁盘 seed 是 **gen2**、**ctl 认证全程正常**
    ⇒ 换文件在重启前是惰性的（#54 不变）。
  - 恢复 gen1 + 重启双 broker：认证 **+30s 即 OK**，稳定到 +150s ⇒ **A 证伪、B 证伪**。
  - 直接构造 C（带 gen2 重启）：conf issuer 20s 内变 gen2；再换回时**确实出现** C 预测的反向 skew
    （`brk1_iss=gen2 brk1_seed=gen1` + 认证 FAIL），**但 C3 reconciler 在 +20s 内重渲回 gen1、+40s 认证全好**
    ⇒ **C 的状态真实存在，但瞬态自愈，C 的后果被证伪**。
- **真因（CONFIRMED-DEFECT，drill 侧）**：drill 52 的门 `_d_cluster_authworks`（`:459-462`）是三项合取，
  第三项是 brk1 上的 `tether cluster status`。而 `clusterstatus.go:68-79`+`:89-96`：
  `ProjectQuorum(v,false).FaultTolerance==0 ⟺ v<=2` ⇒ **任何 N=2 集群都打印 `NOT-HA (exit 1)`**。
  drill 52 **永远是 N=2**（`grow_to_2`）⇒ **该门与 auth 无关地结构性不可满足**，健康态尾部 400s 全程 FAIL。
  三份独立保留日志（`/tmp/simdrills-{r2,r3,w1}`）出现**完全相同**的 120s 超时与 not_covered 文字 ⇒ 确定性，非 flake。
- **裁定**：**A/B/C 全 REFUTED**；台账那句「account.nk 轮换后无法就地恢复」是**对产品的错误指控**
  ——实测双向就地轮换均 ≤40s 完成。**CONFIRMED-DEFECT（drill）**：一个把**容错度**当**存活性**编码的健康门。
  **本批第三次「把 X 当成 Y」。** 归属：**drill 批**（修门）+ 台账订正 #54 的下游措辞。

### 台账逐条

| 条目 | 裁定 | 要点 | 归属 |
|---|---|---|---|
| **#45** | **CONFIRMED**，但**机理与原始证据都写错了** | 停滞点是 `topoConvergedForOp` 的**拓扑收敛**（fail-closed 于任一不可达 voter、无计数器/无 deadline/无 watchdog），**不是** rehome/migrate（那在 5 个状态之前）。且 **N=2→1 的情形是 BY DESIGN**（drill 41 断言其为 PASS，`--to-standalone` 后达 RETIRED）。#45 系**代码审查登记、从无实跑复现**；41 于 07-18 GREEN、N=3→2 retire 全部 terminal。**保留「无界门」这一半，剥离 N→1 那一半** | R7 |
| **#35** | **REFUTED as stated** | trap 是**仅开机**路径（已在跑的 survivor 不会重入）；`observeLeadership` **不受 leader 门控**故 dwell 可满足；`install.sh:777-778` 保留 `StartLimitBurst=5` ⇒ 崩溃循环 ~10s 即停。drill 22 于 07-18 GREEN。只剩条件句成立 | R14（改措辞） |
| **#48** | **CONFIRMED（后果）/ REFUTED（机理）**，且关键证据**本身就是归属错误** | 确认：retire 不驱逐客户端、agent watchdog 只在 NATS 断连时触发而该断连永不到来。**证伪**：退役 broker **不**供 stale VOTER roster（`isClusterFollower` 门在 `RosterRefreshOnly` 分支**之上**，`ShutdownOnRemove` 默认 true）⇒ agent 是被**饿着**、不是被**投毒**。**归属错误**：「DB 显示 ONLINE」——`nodes.status` 是 per-broker 本地存活、从不复制，而 `node ls` 由入口服务器的 mesh 应答 ⇒ **ONLINE 恰恰证明应答来自那台退役 broker 自己** | R8 |
| **#49** | **ALREADY-FIXED**（缺陷曾真实存在） | `offline.go:270-282` 已有 `previewRecoveredRoster`（在副本 DB + 克隆 raft 树上真跑 recovery 后拒绝）；staged swap 用 `renameat2(RENAME_EXCHANGE)`；已有 RED→GREEN 单测。无源码级缺口残留 | R9（仅 drill 42 复验） |
| **#33** | **CONFIRMED，根因首次归因** | `internal/agent/proxy.go:152-156` rehome 成功分支更新了 `homeAddr/homeEpoch` 却**从不恢复 `a.proxyTunnelUp`**；`ApplyHome→OpenHome` 不调 `notifyState`。于是 home broker 被杀后该标志卡在 `false` 而新隧道其实是活的。两个写者对 `nodes.proxy_ready` 打架（心跳写 false / ACK 路径写 true，均 ~5s）⇒ **这就是所报的非确定性**，且有反常倒置：**抖动的隧道会自愈**（redial 边沿触发 `notifyState(true)`）**而稳定的永不自愈**。`proxy off; proxy on` 走 token 路径故可靠——解释了那个手工绕过 | R8 |
| **#59** | **REFUTED** | `broker.go:956-1000` 在 `Run` 的**冷启动**序列内、不在任何循环里；只读存活是**设计态**且有名字：`proxyStateFrozenReadonly`（"control writes refused, but /sub KEEPS vending"）。Q2 实测的 `MainPID` 稳定 / `/varz` 200 / `fenced` 拒绝**逐点吻合该契约**，落在台账自己的 **GREEN** 分支 | 从台账删除 |
| **#63** | **REFUTED（3/3 实跑）** | 三份保留日志 A7d 全 PASS，含承重双边沿（rc=124 黑洞过 30s yamux keepalive 杀旧会话 → 新会话对轮换后证书吐出精确 sentinel）。前提被正面推翻 ⇒ 应撤销而非当发布闸带着。**残留（非裁定的一部分）**：源码路径预测相反（`tunnel.go:1099-1105` 从**进程内缓存**的 `sess.certPins` 重拨），**agent 如何拿到新 pin 未确立** | 撤销；机理残留→R14 |
| **#55** | **REFUTED（前提为假），且有实测证据** | 「运行中集群 issuer 永不变 NEW」不成立：`topology_reconcile.go:233` 喂的 `AccountIssuer` 由**本进程启动 seed** 实时导出，`natsreconcile/reconcile.go:157` 按**纯内容比对**换、不受 generation 门控、非 leader-gated。**实测**：带 gen2 重启双 broker，`nats.conf` **20s 内**变 NEW issuer。⇒ **NEW-issuer 态由「重启」这一寻常动作即可达**，只重启**一台**就构造出 #55 的确切 skew。且因 auth_callout 是跨 broker 队列组，后果**比 per-broker skew 更糟**：两台上都出现 ~1/N 的授权违规掷硬币 | **R11 重开为「可构造」** |
| **`96:240` arm B** | **AS-DESIGNED（已闭合的那半）**；`not_covered` 合法但**过度扩围** | 机械性主张逐条核实通过（`--nats-url` 钉住是产品侧、心跳极性 agent→ctl、15s watchdog + 合成 `agent unreachable` + 终端恢复 + 非零退出）⇒ 这半该是正向 `assert_ok`。但两件事藏在它后面：watchdog **只在首个心跳后才 arm**（无 HB 则 run 无限挂起，drill 从未确立该运行时属性）；plan 强制的配对臂（`TETHER_RUN_LIVENESS_TIMEOUT=180s`）被**静默丢弃**——那才是唯一真正的运行时部分，也是唯一能钉住 DOC-28 agent 侧语义的部分 | drill 批 + R14 |

### 一处流程澄清（agent 正确提出）
R6 的出口断言写的是「`git diff --stat internal/ cmd/ scripts/` 为空」，但当前工作树**非空**——
那是 **R3（`scripts/install.sh`）与 R4（`cmd/tether/node_versions.go`）的合法产出**，不是 R6 的改动。
**R6 自身零改动**（该 agent 全程无 Edit/Write）。**订正**：该断言应表述为「**本批新增**的 diff 为空」，
而非「工作树为空」——后者在多批次累积下永远不可能成立。
