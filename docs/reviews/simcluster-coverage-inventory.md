# simcluster 覆盖清单（命令树 + 事件/告警面）— 单一真相源附录

Date: 2026-07-10（rev4 建档；rev5 全量生成；rev6 补安全门 flag；**rev7 按外审 round5 R5-F1/F2
用构造后 Cobra 树的完整遍历输出系统性重建 §2**——94 command path 逐行、行为 flag 全列、统一
排除规则显式化，并修正 restore 的 never-escapable 安全模型）
Status: **随 roadmap 演进的受审清单**。本文件是命令/事件覆盖的**唯一清单**：各叶 plan
（s1–s9）**消费并增量更新本文件**，不得各自另生成全量清单；每批 plan 收工闸 = 按 §3 生成法
重新枚举 → 与本文件 diff → **diff 非零则先落行再收工**。引用尽量用**文件/符号**而非行号
（减少行号漂移；如 `internal/broker/roster_stale.go` 的 `maybeEmitRosterStale`）。

## 0. 边界声明（round2 遗留问题 2 的裁定）

「operator-facing 事件/告警面」= 以下四族的**并集**（不止 architecture H.1 契约）：
1. `internal/broker.pubSysEvent` 的**全部现有 kind**（`events` 流，§1.1）；
2. architecture H.1 **承诺**的 kind（含当前无 writer 的——它们是 probe 对象，§1.2）；
3. rehome/drain 专用通道 kind（§1.3）与 proxy-`/sub` 可用性过渡 **event kind**（§1.4——
   亦经 `pubSysEvent` 发射，payload 无 `reason` 字段，断言按 kind 匹配）；
4. store-backed **告警**：kind 枚举 + **dedup key 结构**（两者是不同字段，§1.5）。

## 1. 事件 / 告警清单

### 1.1 `pubSysEvent` 现有发射（源码枚举，2026-07-10；生成法见 §3）

| kind | 发射点（文件/符号） | 归属 | 断言（部署层含义） |
|---|---|---|---|
| `session_created` / `session_destroyed` | broker session 生命周期 | S2-81 | rm 三阶段旅程中两 kind 各现一次（history/`admin audit` 可见） |
| `member_joined` | `internal/authcallout/handler.go`（`h.emit`，PIN 首连两路径） | S2-80 | PIN 首连成功 → 事件含 via=pin/fp |
| `pin_failed` | `internal/authcallout`（同上通道） | S2-80 | 错 PIN → 事件可见（限速探针的独立 oracle） |
| `tetherd_restarted` | broker 启动 | S9-95 | broker 自愈后事件在场 |
| `agent_registered` | register handler | S2-82 + S9-94 | invite 入群后在场；94 以「断连后**新** `agent_registered`」为重注册证据 |
| `agent_evicted` | evict | S2-81 | evict 旅程断言（与 H.1 的 `kicked` 命名漂移，见 §1.2） |
| `agent_roster_stale` | `internal/broker/roster_stale.go`（`maybeEmitRosterStale`，每 (sid,nid,gen) 至多一次） | S2-82 | refresh 前滞后可见、refresh 后消除 |
| `disk_pressure` | store 监控 | S8-90 | 灌盘触发（与同名 alert kind 成对） |
| `grow_cutover_revival_failed` | grow cutover 复活失败路径 | 既有 10/11 + S6-40/41 | **确定的负向断言**：上述 drill 收尾断言 journal/events 中**无**该 kind（出现 = 立即分诊，不入 GREEN） |
| `nats_topology_<action>` | `internal/broker/topology_reconcile.go`（`Noop`/`Unresolvable` 不发射） | S6-40/41 | 合法后缀**闭集**（`internal/natsreconcile/reconcile.go` Action 常量）：`reloaded` / `swapped_reload_pending` / `rejected` / `unknown_directive` / `awaiting_clustered_cutover`。40/41 断言收敛路径出 `reloaded`；`rejected`/`unknown_directive` 为 fail-closed 负例臂；`swapped_reload_pending` 是 #22 史相关的降级态（探索→定格）；`awaiting_clustered_cutover` 由既有 grow（cluster add 协调重启）覆盖 |
| `proxy_enabled` / `proxy_disabled` / `proxy_keyset_changed` | proxy 开关/revoke | S4-72 | on/off/revoke 各现一次 |
| `proxy_node_unready` | proxy 健康（2 发射点） | S4-73 | rehome 期间捕获 |
| `proxy_auto_rebalanced` | 自动再均衡 | S4-74 | `=on` 臂恰一次（g7-plan 案 6） |

### 1.2 architecture H.1 承诺但当前**无 writer / 命名漂移**（probe 对象，不算已覆盖）

| 承诺 kind（architecture.md H.1「消息类型」） | 现状（源码核对 2026-07-10） | 处置 |
|---|---|---|
| `rotated_pin` | 无 writer（产品无改 PIN 动词——PIN 只能 rm 重建） | S2-80 probe → 承诺过时，DOC 候选（改架构或登缺口） |
| `kicked` | 无该 kind；evict 发的是 `agent_evicted` | S2-81 probe → 命名漂移 DOC 候选 |
| `agent_unregistered` | 无 writer（无 unregister 动词） | S2-81 probe → DOC 候选 |
| `session_deleting` 广播（usage.md §5.10 另诺） | 状态码存在、**无 pubSysEvent 广播** | S2-81 **probe**：验证「agent 拒 DELETING session 新调用」的真实机制路径；承诺与实现不符 → gotcha/DOC 候选。**不因 rm 旅程跑通而记 covered** |

### 1.3 rehome / drain 事件（`emitDrainEvent` 通道，`internal/broker/rehome_events.go`）

| kind | 归属 | 断言 |
|---|---|---|
| `expose_rehomed`（back-compat） | S3-71 | rehome 后在场 |
| `home_reassign_started` / `home_reassign_succeeded` / `home_reassign_failed` | S3-71 | 生命周期三态（`failed` 由 S9-96 的中断臂探） |
| `rehome_stalled`（含 `{no_eligible_target}`） | S4-74 / S3-71 | 无 eligible 目标场景钉住 |
| `broker_down_rehome_summary` | S8-90 | 与 `broker_down` alert 成对 |

### 1.4 proxy `/sub` 可用性过渡 event kind（`internal/broker/proxy_cluster.go` 的
`decideProxyEvents` → `emitProxyCountEvents` → `pubSysEvent(kind, fields)`；**是 event kind
而非 reason**，payload 仅 `sid/ready/capable`——叶 plan 勿在 `reason` 字段查找）

| event kind | 归属 | 断言 |
|---|---|---|
| `sub_render_empty` | S4-73 | 全 exit 不可用时该 kind 精确出现 |
| `proxy_no_ready_nodes` | S4-73 | quorum-loss/全 unready 场景 |
| `proxy_partial` | S4-73 | 部分 exit 就绪的过渡窗口 |

### 1.5 store-backed 告警（**kind 与 dedup key 是两个字段，分列**）

**kind 枚举**（`internal/cluster/alert_ops.go` 的 0009 CHECK 闭集，共 7 值）：

| kind | 归属 | 断言 |
|---|---|---|
| `manual` | S8-90 / S7-52 | raise/ack/clear 生命周期（90）；C7 轮换告警（52，见下 dedup key） |
| `broker_down` | S8-90 | 杀 follower 真触发（注入前洁净基线） |
| `disk_pressure` | S8-90 | 灌盘真触发 |
| `broker_draining` | S6-40 | drain 期间在场、abort/完成后清 |
| `replication_degraded` | S5-30 / S8-90 | 滚动重启窗口的瞬态出现与收敛后清除（runbook §6 note） |
| `below_quorum` / `raft_lag` | S8-90 | plan 定造法（杀至无 quorum / 慢 follower）；造不出的显式 NOT-COVERED 附因 |

**dedup key 结构**（同一 kind 下的去重键；`alert ack/clear` 的操作对象是 dedup key）：

| dedup key | 组成 | 归属 | 断言 |
|---|---|---|---|
| `manual` / `manual:<label>` | kind=`manual` + 可选 `--label` | S8-90 | label 变体去重语义 |
| `manual:credrot:<node>` | **kind=`manual` + `AlertLabel="credrot:<node>"`**（`cmd/tether/cluster_rotation.go` 的 `rotationTrackingKey`——**不是独立 kind**，断言时勿按 kind 匹配） | S7-52 | raise→轮换完成→`alert clear manual:credrot:<node>` 生命周期 |
| 系统 kind 的 dedup key | 由各 writer 按 kind+范围生成 | 随各 kind 行 | `alert ack <dedup_key>` 闭环用 `alert ls --json` 的 `dedup_key` 字段取值 |

## 2. 命令树清单（构造后 Cobra 树全量遍历，2026-07-10 重建；`--help` 看不见 Hidden，禁作来源）

> **生成基准（round5 R5-F1 重建）**：临时诊断测试构造 `newRootCmd()`、递归 `Commands()`、
> 采集每命令的 local/persistent/inherited flag 与 command/flag 两级 Hidden 后**逐行转录**
> （测试已删、零残留；round-6 可按 §3 生成法复跑 diff）。共 **94 个 command path**。
>
> **统一排除规则（显式声明，生成器归一化时验证）**：`--home`、`--nats-url`（ctl 侧连接串）、
> `--socket`（admin/cluster 本机 socket 路径，含 inherited 与 doctor/reconcile/alert-raise 的
> local 注册）、`--json`（输出格式）为通用 transport/输出面，**不逐行重复**——生成器须验证
> 被省略的 flag 仅属此四名，任何新 flag 不得静默落入省略。三个例外**保留在行内**：
> `login --broker`（`--nats-url` 别名 + 持久化 `broker_url` 的行为面）、`serve --nats-url`
> 与 `agent --nats-url`（两者是 **daemon 部署 seam**——分别决定 broker→NATS 与 agent 拨入的
> broker/seed list——非 ctl transport；round6 R6-F1 更正）。
> Hidden **command**（8）：`takeover-natsconf`（字面 `Hidden: true`）、`force-single`/
> `recover`/`restore`/`export-incident`/`remove` 五个 cluster 顶层旧拼写
> （`deprecatedClusterAlias`，与 recovery 本体同 RunE + stderr 警告）、`node-pub`/`keygen`
> （`hiddenDebugCmd`）。Hidden **flag**：`drain --retire`（MarkHidden REMOVED-redirect）+
> 七处 `registerYesRejector` 的 Hidden `--yes`。`completion` 由 cobra 运行期注入（构造树不含此节点）——
> **自 S1 外审 MINOR-3 起，命令树门另用第二份 runtime golden** 遍历 `InitDefaultCompletionCmd()` 注入后的树
> （**99 path** = 构造 94 + `completion` + bash/fish/powershell/zsh + `--no-descriptions`），故 completion 面亦入结构门。

### 2.1 使用者 / agent / admin 面

| command path | Hidden | 行为 flag（排除规则外全列） | 归属 | 断言或理由 |
|---|---|---|---|---|
| `version` / `logout` / `ctx` | — | — | S1-60 顺带 | 本地纯逻辑，hermetic 密（logout/ctx 参与 60 的 G.3 臂） |
| `completion <shell>` | —（运行期内建） | `--no-descriptions` | S1-60 顺带 | 同上 |
| `login` | — | `--session/--pin/--broker`（别名+持久化 broker_url） | S1-60 + S2-80 | 激活/CONNECT 拒/G.3 重连（登出→经 broker admin socket 证 agt2 STALE→重登**首个** node ls 即反映=重连读当前态，非 login 取快照） |
| `session create <name>` | — | `--pin` | 既有 10（写提交）+ S2-81 | |
| `session ls` | — | — | S2-81 | STATE/ROLE/active 标记 |
| `session rm <sid>` | — | `--ack-alerts` | S2-81 + S8-92(a) | 三阶段 + 告警态强推臂 |
| `ps` | — | `--all` | S1-60 / S3-70 | LOST 合成、PORTS/HOME 列 |
| `exec <node> …` | — | `--cwd/--timeout/--safe` | S1-60（62 `--safe`） | |
| `run <node> -- …` | — | `--cwd/--safe/--ack-alerts` | S1-60 + S9-96 | 真 PTY / 混沌臂 |
| `expose <node>` | — | `--local/--name/--remote-port/--on-broker`（+ `--no-rebuild`/`--ack-alerts` **NOT-COVERED**——无 G-A drill 实测独立臂，Stage-C ledger-1） | S3-70/71 | 四 flag 独立臂 |
| `expose explain <name>` | — | — | S3-70/71 | home/epoch/moved |
| `expose rm <node>` | — | `--name/--ack-alerts` | S3-70 | 断流 + 端口回收 |
| `push` / `pull` | — | `--force/--timeout/--ack-alerts` | 既有 00 + S1-61 | 双向执法/边界 |
| `history` | — | `--lines/--kind/--follow` | S1-60/61 + S9-94 | 含 G.5 记录 |
| `node ls` | — | `--all/--brokers` | S1-60 + S5-30/31 | skew 视图 |
| `node upgrade <nid>\|--all` | — | `--url/--sha256`（S5-31✓）；**#28 已修**（R15：agent 侧 `upgrade.url_allow` 可配，31 drill 以 #28-FLIPPED 前置验证）；`--all` **enumeration/dispatch/`--timeout` COVERED**（外审 M4 可执行臂：ONLINE 枚举 + OFFLINE 排除 + dispatch-到-online + `url_not_allowed_local` config-abort + `--timeout 0` 金丝雀中止——upgrade-safety 契约改写后该臂由 skip-continue 变为 canary-abort）；**单台 SUCCESS/rollback/`--wait`（真 re-exec / re-register / watchdog 回退 oracle）→ owner = `33-node-upgrade-success`**（upgrade-safety follow-up 落地，INCOMPLETE/1：nc_gap 指 gotcha #73；2026-08-02 首次真跑 INCOMPLETE/1 pass=29 assert_fail=0）；fleet 级 `--all` 金丝雀确认后的全队扇出**仍 NOT-COVERED、无 owner**（31 的残余 nc_gap）；hermetic 面由 `test/p10/upgrade_e2e_test.go` 与 `cmd/tether/node_upgrade_wait_test.go` 覆盖 | S5-31◑+33 | fleet 扇出臂待立项（无 owner）；33 已于 2026-08-02 真跑 |
| `proxy on` | — | `--ha-policy/--yes`（**可见的 Tier-1 确认跳过**——区别于 Tier-2 Hidden rejector） | S4-72/73 | owner-only + --yes 脚本化语义 |
| `proxy off` | — | — | S4-72 | |
| `proxy status` | — | `--cluster` | S4-72/73 | cluster 视图独立臂 |
| `proxy sub create/revoke` | — | `--name` | S4-72 | token/PSK 生命周期 |
| `proxy sub ls` | — | — | S4-72 | 不泄 token |
| `alert raise` | — | `--kind/--severity/--message/--label` | S8-90 | operator socket 面 |
| `alert clear <dedup_key>` / `ls` / `ack <dedup_key>` | — | — | S8-90 | member/operator 两面 |
| `agent`（daemon） | — | `--session/--nid/--pin/--nats-url`（部署 seam 例外，R6-F1）`/--tunnel-addr/--install-user-service/--uninstall/--log-level/--log-json` | S1-60 + S2-82（user-service spike）+ S8-93（log-json）+ S9-95（seed-list 重连即其部署语义面） | |
| `agent join <invite>` | — | `--nid/--pin/--start/--expect-account-pub` | S2-82 | `--nid` 身份必填、`--pin` bootstrap 变体、伪造/篡改负例 |
| `agent config refresh` | — | `--session/--once` | S2-82 | |
| `agent doctor` | — | `--session` | S2-82 | |
| `admin sessions/nodes/evict` | — | — | S2-81 | socket 属主语义 |
| `admin audit <sid>` | — | `-n` | S2-81 | |
| `admin events` | — | `-n/--since/--kind` | **#30 新增；drill 覆盖 owner = 71/73/74 D2**（"raw sys.events have no operator reader"） | operator 读 H.1 `events` 流的 sys.events（session/member/agent/disk/`proxy_*`/`nats_topology_*`/rehome/grow-cutover 各 kind）。走 root-only 0600 admin socket（与 `admin runtime`/`alert raise` 同信任层），读**持久化流**故支持 `--since` 历史 + `--kind` 过滤；**无 `--follow`**（admin socket 一次性设计，运维需求是时点读；live tail 用 poll-loop，见 broker-ops）。载荷**恒无 secret**（生产者手搭 allow-list 标量 map，reader 原样转发）。cluster-mode `proxy sub create/revoke` 现补发 `proxy_keyset_changed`（此前只 bump epoch 不发事件） |
| `serve` | — | `--config/--db/--admin-socket/--nats-url`（部署 seam 例外）`/--nats-conf-path/--nats-server-bin/--auth-callout-seeds-dir/--sub-http-listen/--cluster-manifest-listen/--metrics-listen/--alert-webhook-url/--log-level/--log-json/--upgrade-url-allow/--cluster-data-dir/--cluster-raft-addr/--cluster-secrets-dir/--colocated-agent-nid/--tunnel-addr/--tunnel-public-host/--store-dir/--public-host` | 既有组建 + S8-93（metrics/webhook/log-json）+ S4/S2（两 loopback listener）+ S5-30（colocated）+ 既有 13/doctor（nats-conf-path/nats-server-bin seam） | 两 listener 的 loopback fail-closed 是 S0-ingress 的产品边界断言 |

### 2.2 `cluster` 面（root persistent flag：`--socket`）

| command path | Hidden | 行为 flag（排除规则外全列） | 归属 | 断言或理由 |
|---|---|---|---|---|
| `cluster status` | — | `--card/--homes/--watch/--remote/--offline/--db/`**`--settle`**（R13/D3 新增） | 既有 00/10 + S8-92/93 | exit taxonomy 分立断言。**`--settle <dur>`（R13）**：仅去抖 DEGRADED(1) 的 voter-restart 瞬态；持续降级仍 exit 1；QUORUM_LOST(2)/FORCE_SINGLE(3) **绝不去抖**、立即返回。owner=R13 drill（93/D3） |
| `admin runtime` | — | `--json` | **R13 新增；owner=R13 drill（97）** | 97 结构性缺口的 (a) 解：返回 `goroutines(NumGoroutine 进程内真值)/threads/open_fds/rss/uptime/各 reconciler last_tick`。root-only 0600 admin socket，**禁 pprof**（计数即够、socket 已门控）。drill 97 用它做 goroutine 泄漏门（注入负载→quiesce→回落基线±tol） |
| `admin session-allow` | `<fingerprint>` | `--list/--remove/--note` | **发布前审计 round 2 新增；增量 2 内审后归属改判 → owner=drill 60-user-journey（待补臂）** —— 见右 | 谁可以 `session create` 的准入表。此前该 handler **零准入**:公网上任何持合法 nkey 的连接都能命名一个 session、成为其 owner,并据此铸出 activated-member 与(用它自己刚设的 PIN)agent 两套权限模板 —— 审计中每一处「activated member 是可信主体」的推理因此都在谈论整个互联网。走 root-only 0600 admin socket:准入一个指纹是**运行 broker 的人**的动作。**升级零动作**:broker 启动时的一次性回填(`seedSessionCreators`,经 raft)把每个已拥有 session 的 owner 指纹自动纳入;全新 broker 从**空表**开始,第一个运维在 broker 主机上用这条命令准入自己(指纹由 `tether whoami` 或那条拒绝文案给出)。**归属改判的理由**:这张表本来就走 raft(`OpSessionCreatorSet`),而原行自己写着「若将来准入表进入 raft 复制,这条要改成 owner=某 drill」——该条件当时已经成立而没人回来改。增量 2 内审补了混版能力门与一次性回填后,它有了**跨机语义**:混版集群里旧 voter 会 poison-skip 这条写入,而回填必须只发生一次且不复活被撤销的指纹。hermetic 层已覆盖判定逻辑本身(`internal/session/creators_test.go`、`internal/broker` 的能力门与拒绝路径);deploy-tier 要补的是**滚动升级时序**那一段。 |
| `whoami` | — | `--home/--json` | **增量 2 内审新增；NOT-COVERED(deploy-tier)** | 打印本机 ctl 身份的指纹与公钥。**完全离线**:读本地身份文件后派生,不连 broker、不需要 session —— 因为它存在的意义就是在**还没被准入之前**能用。`admin session-allow --help` 早就让运维去跑它,而它此前不存在;在一个「必须先被准入才能建 session」的 release 里,每个全新部署都是从一条跑不通的指令开始的。**NOT-COVERED 理由**:零网络、零集群语义,一次文件读 + 一次 sha256;hermetic 测试能覆盖它能坏的每一种方式。 |
| `cluster drain <id>` | — | `--now/--abort` + **Hidden `--retire`**（REMOVED-redirect：报错导向 `cluster retire`） | S6-40 | 40 顺带 redirect 报错断言 |
| `cluster retire <id>` | — | `--wait/--timeout/--secrets-dir/--compromised/--require-credential-rotation` | S6-40 + S7-52 | C7 轮换臂 |
| `cluster transfer-leader <id>` | — | `--wait/--timeout` | 既有 10 | |
| `cluster rotate-tunnel-cert <id>` | — | `--cert-fp` | S7-52 | |
| `cluster set-raft-addr <host:port>` | — | `--route/--allow-loopback` | S6-41（rebind 臂） | |
| `cluster backup` | — | `--out/--offline/--db/--data-dir/--secrets-dir/--allow-stale-follower` | S7-50 | follower 默认拒成对臂 |
| `cluster ops ls/show/confirm/abort` | — | — | S6-40 | confirm/abort 由 BLOCKED/STALLED 态驱动 |
| `cluster unlock` | — | `--upgrade/--grow/--force/--dry-run` | **R7b 新增；drill 覆盖 owner = R9**（30 需断言「stale-lock 清除动词被实际调用且锁确已释放」） | R7b 产物：升级/grow 锁改 lease+TTL 后，运维需要一个**不必等 TTL 到期**的出路。默认**拒绝清除仍在续约的 lease**，`--force` 才越过；清除后**再探一次**确认，仅在确认已清才 rc=0 |
| `cluster apply` | — | `--file` | S6-40 顺带 | 仅 plan、零执行断言 |
| `cluster seeds publish` | — | `--bootstrap/--endpoint/--sid` | S2-82（首次）+ S8-91 | |
| `cluster seeds show` | — | `--remote` | S8-91/92 | |
| `cluster reconcile nats` | — | `--all/--wait/--timeout/--plan/--manual` + manual 组（`--secrets-dir/--server-name/--route-url/--account-issuer/--broker-nkey/--peer/--conf/--cluster-listen/--nats-server/--skip-dry-run/--allow-partial-mesh`）+ `--to-standalone/--confirm-single` | 既有 13/init/grow + S6-40（--plan 零写）/41（--to-standalone） | |
| `cluster join prepare` | — | `--node-id/--name/--raft-addr/--nats-route/--tunnel-addr/--public-host/--secrets-dir/--seed/--cert-fp/--nats-server-id` | 既有 grow + S6-42 | `--cert-fp`（tunnel provenance）/`--nats-server-id`/`--name` 是 join bundle 身份面——42 的 rejoin 断言 bundle 携带真值（grow 已日常走） |
| `cluster join approve <bundle>` | — | `--wait/--timeout` | 既有（非阻塞）+ S6-42（--wait/--timeout 落点） | |
| `cluster rebalance proxy` | — | `--dry-run` | S4-74 | |
| `cluster upgrade` | — | `--to-version/--dry-run/--expect-sha256/--account-seed/--backup-taken/--ack-writefence/--notify-webhook` | S5-30 | webhook 臂可并 93 的接收器 |
| `cluster add <joiner>` | — | `--self-id/--name/--raft-addr/--nats-route/--tunnel-addr/--public-host/--node-ident-pub/--secrets-dir/--account-seed/--confirm-node-id/--reset-former-js/--preserve-js-data/--auto-confirm-catchup/--dry-run/--notify-webhook/--data-dir/--db/--config/--timeout` + Hidden `--yes` | 既有（grow=可执行规格）；`--preserve-js-data`（former-N1 JS 保留语义）与 `--auto-confirm-catchup`（BLOCKED op 确认边界）归 S6-40/plan 定臂；`--dry-run` 零改动断言归 40 顺带 | |
| `cluster pin <invite>` | — | `--force` | S8-91 | 首写胜/--force 换簇 |
| `cluster invite` | — | `--account-pub/--seed/--bootstrap` | S2-82 + S8-91 | OOB 铸造 |
| `cluster init` | — | `--from-existing/--from-manifest/--check/--dry-run`（--check 别名）`/--confirm-node-id/--config/`**`--nats-conf`**（N-4c 新增）+ 身份组（`--self-id/--name/--node-ident-pub/--raft-addr/--nats-route/--tunnel-addr/--public-host/--data-dir/--db/--secrets-dir`）+ Hidden `--yes` | 既有 init/grow + S6-42（--from-manifest）/43（带数据 + --check 顺带） | **`--nats-conf`（N-4c）**：init 此前把 `nats_conf_path` 硬编码为默认（`applyClusterSeam` 无该参），自定义 conf 路径部署下 seam 漂移。加 `--nats-conf` 后 seam 记录真实路径、且 `--check` doctor preflight 与打印的身份 cross-check 都用它。**drill 覆盖**：既有 init/13 隐式走默认（与 restore `--config`/force-single `--nats-conf` 同处理——默认路径已被 seam 五字段断言含 `nats_conf_path` 钉住）；显式自定义值的 seam-记录/回读由 hermetic `TestApplyClusterSeamThreadsNatsConfPath` 定臂（no-thrash + stale-seam 硬错），deploy-tier 走默认即可 |
| `cluster doctor` | — | `--offline/--secrets-dir/--db/--conf/--raft-addr/--nats-route` | S7-50（正向 preflight 臂）+ S6-42 | **preflight 输入面**：错 DB/conf/监听地址仍判绿 = 部署风险——50 的 preflight 臂对照真值/错值各一断言（plan 定格） |
| `cluster takeover-natsconf` | **Hidden**（deprecated 别名 → `reconcile nats --manual`） | 同 manual 组 | NOT-COVERED | 与本体同 RunE（字节保留），本体已覆盖；一 release 后删 |
| `cluster recovery diagnose` | — | `--self-id/--db/--offline` | S6-42 顺带 | 与 online `--dry-run` 不同代码路径 |
| `cluster recovery force-single` | — | `--online/--dry-run/--guided/--self-id/--self-addr/--confirm-peers-dead/--data-dir/--db/--nats-conf/--nats-server` + Hidden `--yes` | 既有 12/20（OFFLINE）+ S6-22（ONLINE + 拒绝门五臂 + `--guided`→42 顺带） | `--nats-conf/--nats-server` 决定 OFFLINE 去集群化写哪份 conf/用哪个 binary 做 fail-closed 校验——12/20 隐式走默认，显式/错值负例 plan 定 |
| `cluster recovery resnapshot` | — | `--self-id/--raft-addr/--data-dir/--db/--confirm-node-id/**--accept-audit-loss**` + Hidden `--yes` | S6-42（变体） | **`--accept-audit-loss` 是显式数据损失开关**：42 两臂——有未发布 audit 且不带 → 拒；带 → 截断继续（探索→定格钉真实语义） |
| `cluster recovery rejoin prepare` | — | `--self-id/--dump-divergent/--emit-manifest/--guided/--data-dir/--db/--secrets-dir` + Hidden `--yes` | S6-42 | |
| `cluster recovery restore <bundle>` | — | `--confirm-node-id`（**provenance 锚，typed-confirm**）`/--data-dir/--db/--raft-addr/--secrets-dir/`**`--config`**（R10 新增） + Hidden `--yes` | S7-50/51 + S9-94（orphan 造法） | **never-escapable（R5-F2）**：`confirmTypedNodeID(machineEscapable=false)`——50 三断言：① `--confirm-node-id` 与 manifest/provenance 不符 → 拒；② `--yes` 必拒；③ **flag+`$TETHER_CONFIRM_NODE_ID` 同时正确、非交互执行仍拒**（`TestRestoreNeverEscapableEndToEnd` 的真栈版）。`--raft-addr` = fresh-host IP 变化的恢复逃生 → S7-51 的 DR 臂。**`--config`（R10/P2 新增）**：restore 此前**结构上不可能**写 broker.yaml 的 cluster seam（flag 集里根本没有 config），而 `install.sh` 把 `cluster:` 整段注释掉 ⇒ fresh DR box 启动必 FATAL `data_dir is unset`。加 `--config` 后落地即调现成的 `applyClusterSeam`（自带 fail-closed 的 decode 回读校验）。**drill 覆盖 owner = R10 的 drill 侧**：51 的 DR 尾段须**从订正后的 runbook §5.2 逐字执行**并断言 seam 五字段齐全（含 `nats_conf_path`） |
| `cluster recovery incident export` | — | `--since/--out/--sid/--force` | S7-50 顺带 | `--force` 翻转 O_EXCL 防覆盖——50 顺带负例（不带 --force 时目标已存在 → 拒） |
| `cluster recovery node remove <id>` | — | `--manual/--force/--confirm-node-id` + Hidden `--yes` | 既有 12（--manual 路径）；`--force`（孤儿化语义）+ machine-confirm 负例 → S6-40 的 ops 臂顺带 | ghost passthrough hermetic |
| `cluster force-single`/`recover`/`restore`/`export-incident`/`remove`（顶层旧拼写） | **Hidden** ×5（`deprecatedClusterAlias`，同构 RunE + stderr 警告） | 同各本体 | NOT-COVERED | 与 recovery 本体共享 RunE（安全门字节保留）；一 release 后删 |
| `cluster node-pub` / `cluster keygen` | **Hidden**（debug） | `node-pub --seed`；`keygen --out` | node-pub：NOT-COVERED（纯本地输出）；keygen：S7-52（产品铸钥臂） | |
| **Tier-2 Hidden `--yes` 拒绝面**（`registerYesRejector` ×7：`add`/`init`/`recovery node remove`/`recovery restore`/`recovery force-single`/`recovery resnapshot`/`recovery rejoin prepare`） | **Hidden flag** | `--yes`（接受解析、**必拒**并输出 "no --yes override by design" 类明确报错——B3 设计性无人值守禁区） | 各对应 drill 的 typed-confirm 臂顺带 `assert_refuses --yes`：22（force-single）、42（rejoin/resnapshot）、43（init）、50（restore）、40（node remove）、既有 grow（add） | 安全门 fail-closed 面；hermetic 已测拒绝逻辑，deploy-tier 断言真栈报错可见 |
| **machine-confirm 双因子**（`--confirm-node-id` + `$TETHER_CONFIRM_NODE_ID`，#5 机器逃生） | — | 两者必须同时等于 node-id（缺一即按 TTY 要求拒）。**仅限 `machineEscapable=true` 的命令：`add`/`init`/`recovery node remove`/`recovery resnapshot`**——**`restore` 除外**（其 `--confirm-node-id` 是 provenance 锚、never-escapable，见 restore 行；R5-F2） | 既有 grow（add 全程在用）+ S6-40（node remove）/42（resnapshot）/43（init）各一条「只给 flag 不给 env → 拒」负例 | |

## 3. 生成方法（每批 plan 收工闸重跑；diff 非零先落行）

- 命令树（round5 R5-F1 最终形态）：**对构造后的 Cobra 树递归枚举**——构造 `newRootCmd()`、
  递归 `Commands()`，每命令采集 `LocalNonPersistentFlags()` + `PersistentFlags()` +
  `InheritedFlags()`（三集去重，**persistent 采集使 group/root 级 flag 的定义位置写实**，
  而非只在子命令 inherited 集偶然出现），记录 command 级 Hidden（`Hidden: true`/
  `deprecatedClusterAlias`/`hiddenDebugCmd` 三形态）与 **flag 级 Hidden**（`MarkHidden`、
  `registerYesRejector`），按 §2 的**统一排除规则**归一化后与 §2 **零 diff**（被省略 flag
  仅许属 `--home/--nats-url/--socket/--json` 四名，例外须在行内保留）。结构校验生成器随
  **S0-台账（由工程首开批落地，非绑定 S1）** 提交入仓、每批收工闸执行；归属/断言列仍人审。
- 事件：`grep -rn 'pubSysEvent('`（+ authcallout `h.emit` + `emitDrainEvent` +
  `proxy_cluster.go` 的过渡 **event kind** + `internal/cluster/alert_ops.go` kind 枚举），
  与 §1 diff；动态后缀（如 `nats_topology_<action>`）以其 Action 常量闭集展开。
- architecture H.1 / cluster.md §5.7 的承诺列表与源码枚举**双向对照**：源码有而文档无 →
  DOC 候选；文档诺而源码无 writer → §1.2 probe 行。
- **CA 设施 owner 规则**（round3 建议采纳）：S0-ingress 与 S0-artifact 共用的实例 CA 由
  **首个落地二者之一的批**成为设施 owner；后开批**复用同一实例 CA、绝不重铸**（重铸会使已
  运行容器的 trust 漂移）——写入该批 plan 的生命周期元组。
- 新增项：先在本文件落行（归属或 NOT-COVERED/probe）→ 再收工。

## 4. 批次落地记录（consume/update 日志；每批收工闸落此）

### S1 landing（2026-07-11）

**命令树生成器已落地（S0-台账，test-tier）**：`cmd/tether/command_tree_inventory_test.go`
（`TestCommandTreeInventory`）+ **两份** golden：构造树 `command_tree_golden.txt` + **运行期树**
`command_tree_golden_runtime.txt`（S1 外审 MINOR-3 加：含 cobra 注入的 completion 子树，**99 path = 构造 94 + 5**）。
收工闸重枚举实测 = **94 command path、8 hidden command、11 处 `--yes(hidden)`**（构造树）——与 §2 的
「94 path / 8 hidden」**零 diff 吻合**（`--yes` 的 11 = 7 canonical `registerYesRejector` + 4 个共享 RunE 的
deprecated alias 复制，生成器如实展开）；运行期 golden 另断 99 path（含 completion）。漂移诊断写 `t.TempDir()`
（S1 外审 MINOR-2：不再落源码树 `.actual`）。此生成器接替 §3「临时诊断测试」，每批收工闸
`go test ./cmd/tether -run TestCommandTreeInventory` 断零 diff；产品 CLI 面漂移即 fail、逼迫本附录重对账后才
`-update-command-tree-golden`。

**§2 命令行 S1 勾（臂级，部分勾约定）**：
- `login/logout/ctx` → **S1✓**（激活 + logout 清空 + G.3 重连臂〔60 J1/J-G.3：登出窗口内经 **broker admin
  socket**（无 ctl session）证 agt2 STALE、**重登后首个** node ls 即反映=重连读取当前态；login 本身无 snapshot
  语义〕）· S2☐（CONNECT 拒非成员）。
- `login --broker` 别名 → S2☐（未测）。`completion <shell>` → **S1✓**（`completion bash` 烟测；
  `--no-descriptions` 未测 → 部分）。`version` → **S1✓**（60 J18）。
- `exec` → **S1✓**（`--cwd`〔flag 须在 node 前，SetInterspersed〕、exit 0/精确非零/信号→扁平 128/256 KiB 流〕；
  `--safe` → 62；`--timeout` → 未跑过期臂，**S1◐ 部分**（留 S 后续或 hermetic）。
- `run` → **S1✓**（真 PTY/`stty size`/resize/Ctrl-C→进程组/attach 无孤儿，经 `image/pty-run.py`）；
  `--safe` → 62；`--ack-alerts` → S9☐。
- `ps` `--all` → **S1✓**（RUNNING→EXITED、PORTS 表头〔N=1 无 HOME 列〕）· S3☐（有值 PORTS/HOME）· S9☐（LOST 合成）。
- `push`/`pull` `--force` → **S1✓**（双向码 path_parent_missing〔push〕/path_not_found〔pull〕/path_outside_roots/
  not_a_regular_file/transfer_disabled/dst_exists/too_large〔pull >2 GiB〕、tier 分界、`--force`）；
  `sha_mismatch`/`path_race` **显式 hermetic**（§0.3）；`--timeout` → hermetic/部分。
- `history` `--lines/--kind/--follow` → **S1✓**（`-n` 界、`--kind call/proc/transfer`、非法-kind 拒
  `must be one of`、`--follow` 烟测）。注：history 行首是**时间戳**、KIND 是第二列（`^KIND` 锚点是假绿，S1 已修）。
- `node ls` `--all` → **S1✓**（`-a` OFFLINE/STALE 视图 + PROTO int/RELEASE 非空）；`--brokers` → S5☐。
- `agent`（daemon 注册/心跳/重连）→ **S1✓**（60 setup + G.3）；`agent.yaml` `file_transfer.allow_roots`
  三态 → **S1✓**（61 open/narrow/disabled，经 `drills/lib/agentyaml.sh` 忠实供给〔install.sh 形态、flagless
  unit、0600/0700 sim:sim〕）· `remote_fs` → **S1◐**（62 auto/off+--safe FUSE-approx；真-D NOT-COVERED，OQ-2）·
  `proxy` → S4☐。**`agent join` flag 集 = S2，未勾。**

**§1 事件/告警面 — NULL-DIFF 收工闸（必需步、非跳过）**：S1 的三个 drill 勾 **0 条** §1.1/§1.2 `pubSysEvent`
/alert 行——用户面命令（exec/run/ps/history/node ls/push/pull）**不发** `sys.events` kind；transfer
在独立 subject `audit.transfer` 发、经 `history --kind transfer` 可见（非 pubSysEvent kind，§1.1 已如此登记）。
**`login` 例外须精确（S1-08 内审订正）**：ctl 的 **PIN 首连** *会* 经 authcallout **发** `member_joined`
（`internal/broker/authcallout.go:81` 接 `pubSysEvent` → `internal/authcallout/handler.go:353`；§1.1 已登该
kind、归 **S2-80**）。60-user-journey 的 setup `session … --pin`（60:53，展开 `session create --pin` +
`login -s --pin`）正走此首连路径，**exercise 了但从不 assert** `member_joined`（断言归 S2-80），故 S1 仍勾
**0 event 行**——结论不变、仅理由须精确。（登出后再 `login`〔60:68〕是 already-member 的 steady-state，走
`handler.go:324` 的 if-member 分支、不发；60 的首连才是矛盾所在。）收工时按 §3 生成法重跑（`grep pubSysEvent`
+ authcallout `h.emit` + `emitDrainEvent` + `proxy_cluster.go` + `alert_ops.go`）**记：0 条 S1-引入 kind**
（null-diff——`member_joined` 是既有 kind、非 S1 引入）。

**DOC 缺陷（S1 顺带经命令树 golden 暴露）**：**DOC-3 确认**——`tether agent` 无 `--upgrade-url-allow` flag
（golden 证实），但 `error_hints.go:34` + `usage.md:1443` 指向「agent 的 --upgrade-url-allow」（只 `serve` 有）。
登 `docs/deploy-tier-gotchas.md`，随 S5-31 定格。**DOC-5 非缺陷**（usage.md §669 已正确记扁平-128）。

**保真度债**：FD-1（sim 顶层 `/home/sim/.tether` 0755 vs install.sh 0700）登 gotchas 台账，S1 不依赖、不阻塞。

### S2 landing（2026-07-11）

**drill**（断言数 = live `drill_end` verdict，外审 round1-F4 / round2-R2-F2 订正 42/40/29）：
`80-session-isolation`（GREEN，42）· `81-admin-evict-session-rm`（GREEN，40）·
`82-agent-onboarding-invite`（GREEN，29）。**机械 count 交叉核对**（R2-F2 采纳）：
`grep -cE '^[[:space:]]*assert_(ok|refuses|bug)\b' drills/<name>.sh` = **80→42、81→40**（与 live verdict 一致，
无条件断言）；**82→33 静态 ≠ 29 live**——差 4 = U 臂（U1/U2/U3/U4）在 `systemd --user` NOT-COVERED 路径**不执行**
（else 分支只 `warn`、非 assert），故 82 以 **live drill_end verdict 29 为真值**（静态 grep 对含条件臂的 drill 不适用）。**S0 落地**：S0-隧道（S3→S2）+ S0-ingress（S4→S2；S2=实例-CA
facility owner）。**零产品 Go diff**（只碰 `test/simcluster/` shell + `docs/`）。

**收工闸（必跑必记）**：
- **命令树重枚举**：`go test ./cmd/tether -run TestCommandTreeInventory` → **零 diff**（S2 无 CLI 改；实测 rc=0）。
- **事件生成法**：`git status internal/ cmd/` 零 diff → **0 条 S2-引入 pubSysEvent kind**（80/81/82 只断既有
  kind：member_joined/pin_failed/agent_evicted/session_created/session_destroyed；null-diff）。
- **守恒硬闸**：`make lint`（0 issues）+ `make test`（cached，零 diff）绿；`make e2e` 守恒跑。

**§2.1/§2.2 命令树行 S2 勾（臂级）**：
- `login/logout/ctx` → **S2✓**（I1-neg CONNECT-拒非成员 + I1-persist 非持久 current_session + I1-broker `--broker`
  别名持久 broker_url）。
- `session rm` → **S2✓**（3-phase RESULT E2a/E2b/E2c + non-owner not_owner I3-rm + post-rm CONNECT-拒 E3a/E3b） ·
  S8-92(a)☐（`--ack-alerts` 告警态强推）。`session create/ls` → **S2✓**（setup + E-base）。
- `admin sessions/nodes/audit/evict` → **S2✓**（A1/A2/A3/B1 + 非授权 EACCES A4）。**订正**：`admin nodes` 输出
  `SESSION NODE STATE`（SESSION 首列），非 NODE 首列——匹配须无 `^nid` 锚（sim harness 注记，非产品缺陷）。
- `node upgrade` → **S2✓**（non-owner not_owner I3-up）。`node ls` → 复用（dual-home CTLH + TS 隔离）。
- `exec` → 复用（node_not_found I2c + post-rm CONNECT-拒 E3a）；`run` → **S2✓**（supervised managed child leak 注入
  C-base-proc via backgrounded `tether exec`，探索→定格）。
- `expose` → **S2◐**（active-expose fixture C-base-expose SENTINEL curl + evict 后口命运 C-port，探索→定格 #26）·
  S3-70☐（expose 主旅程/rehome）。
- `agent join` → **S2✓**（--nid/--pin/--expect/伪造 T1/篡改 T3-T5 + 无-expect 残留现实 T2 + --start ONLINE J1）。
  `agent config refresh` → **S2✓**（--once J4 + C1 收敛）。`agent doctor` → **S2✓**（全绿 J5 + FATAL exit77 T2）。
  `agent --install-user-service/--uninstall` → **S2 spike**（U 臂 NOT-COVERED-in-sim：容器无 systemd --user，实测理由）。
- `cluster seeds publish/show` → **S2✓**（P0 首发前空 + P1 首发+MintInvite）。
- **`cluster invite` 不勾 S2**（line 167 remap：**归 S8-91 cli-failover**——它铸 SID-less discovery token 供
  `cluster pin`，`agent join` 用 ParseInvite 要 sid、永不接受；roadmap 82 规格混淆已纠正）。
- **Tier-2 hidden `--yes` rejectors + machine-confirm 面**：**S2 不触**（rm 无 typed-confirm gate；evict 无 --yes）→
  显式 NOT-COVERED-this-batch（owner=后续 destructive-confirm 批）。

**§1.1/§1.2 事件面 S2 勾 + 订正**：
- `member_joined`（row-27）→ **S2-80✓**（E-joined ctl via=pin 经真 sys.events core sub）+ **DOC-9 订正**：只发
  `via=pin`（无 `via=fp`）；fp-reconnect 不发事件。row-27「via=pin/fp」→「via=pin only」。
- `pin_failed`（§1.1）→ **S2-80✓**（E-pinfailed 独立正向 oracle；与 rate-limit probe 分立）。
- `session_created`（row-26）→ **S2-81✓**（setup exercised）+ **DOC-10 订正**：去 `events` 流（tether.v2.sys.events），
  非 history-<sid>；`admin audit` 只 tail history-<sid> 故看不到。
- `session_destroyed`（row-26）→ **S2-81✓**（E2c 直接 sys.events oracle，member observer 真收）。
- `agent_evicted`（§1.2）→ **S2-81✓**（B2a/C-exit 跨进程自退=广播证据）。
- `agent_registered`（row-30）→ **S2-82 exercised**（J1 fresh register）；直断留 S9-94，82 主 oracle=node ls ONLINE。
- `agent_roster_stale`（row-32）→ **S2-82 NOT-COVERED-in-sim**（`rosterStaleGrace=6min` 结构上不触发 + 收敛-agent
  报同 gen；谓词 hermetic）；C1 改用 roster_gen 跳变展示。row-32 处置：「S2-82 断言」→「S2-82 NOT-COVERED + 谓词留 hermetic」。
- H.1 无-writer probe：`kicked`/`agent_unregistered`（81）·`rotated_pin`（80）·`session_deleting`（81）→ 全
  **DOC candidate**（DOC-11/12），不因旅程跑通记 covered。

**台账**：**#25**（PIN 无限速，80 Arm R）· **#26**（evict 不清理 OS 子进程，部署条件式，81 Arm C）·
**#27**（manifest_listen 默认关+未文档化，82 setup）+ **DOC-6…14**（含 O4 裁定两项归 DOC + 注释漂移 DOC-13/14 随批修）。
详见 `docs/deploy-tier-gotchas.md`。

**Stage-B 调试保真度注记（非产品缺陷，避免误归因）**：① post-rm session-scoped 调用是 **CONNECT-deny**（session 已删
→ auth_callout 拒），非 app-layer `session_not_found_or_deleting`——后者需 session 存在但 DELETING（N=1 同步 rm 窗口
不可达，taxonomy 留 hermetic）；E3 按现实断（DOC-11 结论不变）。② manifest 有 30s 重签节流（`manifestRecheckInterval`）
→ seeds publish 后 loopback manifest 至多滞后 30s 反映 seed bundle（缓存设计、非缺陷；onboarding 不依赖即时新鲜=invite
带 inline seed）；M1/M2 poll 过节流。③ `tether admin nodes` 写 STDOUT（fd 分离实测；tether 正确）。

### G-A landing（S3+S4+S5 合并流，2026-07-12）

**drill**（断言数 = live `drill_end` verdict；**权威状态见 `s3-s5-external-review-round3.md`——本表为当前事实、非历史**）：
- **S3**：`70-expose-journey`（GREEN，28，N=1 cluster + agent：expose 数据面全旅程，curl 公网口回真 sentinel）·
  `71-expose-rehome-failover`（N=3：**gotcha #29** = cluster expose 的 home **不可投递到非-tunnel broker**（净效果 tunnel-coupling，机制是 un-homed 回落，R5-M3）：`homeForExpose`(home.go:96-113) 对 `--on-broker <非tunnel>` 返回 nil → agent 回落固定 tunnel → self≠home 拒 → frpc_failed（**不是** round-4 说的"agent 设计上硬编码固定 tunnel"，源码会投递 named home）+ **crash home → 常规 expose 全 voter 搁浅、不自动 rehome**（rehome_events.go:52-53）。**FIXTURE 硬门（R6-M2）**（agt→brk3 间歇——**硬断言 RED 暴露**不建立，不再静默 NOT-COVER-GREEN）→ **combined crash（Arm C rebuild-ON `wstrand` + Arm D rebuild-OFF `wnr` 一次注入）**：都 home brk3 → node_kill → 都全 live voter curl exit-7 + cluster-status reachability 证 leader 见 crash + epoch 不变 + wnr explain 诚实 rebuild:false → node_start → 都同端口恢复；**Arm A** bogus 负例；**非-leader 硬门**。**HARD RED 门——drain-migrate(B)（R6-M2）**：`cluster drain brk3` 必须迁 wstrand 到 survivor 并 serve——被挡即 **RED 暴露**为 release-blocking（**不再**是 round-5 自证的 owner-decision NOT-COVERED，R6-M2 驳回）:(1) homeForExpose un-homed 回落(`--on-broker <非leader>` 从不 serve,180s×2);(2) agent-隧道-到-非-leader 间歇(外审 solo1b 命中为 71 RED);(3) fixture 建成后 `cluster drain brk3` 被 grow 遗留 `NATS_ROLLED_OUT` 挂起 op 拒(#31 家族)。E/G/F 需成功 drain 作前提→同墙阻塞(暴露、非 rescope);raw sys.events 无 reader(D2,仅 raw-event)。grow_to_3 retry=1 nuke+重试(attempt 数作一等证据)兜底间歇 #31 grow-flake）。
- **S4**：`72-proxy-subscription`（GREEN，N=1 cluster + 2 agents：proxy sub 旅程 + 真 SS dual-leg + revoke/off 数据面。**R5-M1 NOW-COVERED**：**字节可观测在飞流强断**（held-open **ThreadingHTTPServer** slow-sink SS 流，revoke 前证 alice+bob 收字节数**严格增长**——stalled/未连接 curl 不能假绿；revoke 后 alice curl 早退+字节冻结=强断 WHILE bob 仍在增长）+ **OFF `__proxy__` 回收**查 OS listener（ss -ltn）**AND** port_allocations 行→0（sqlite3 权威源）**AND** 安全同端口复用（OFF-reuse 经回收端口 serve sentinel）——oracle 若不成立即 RED 暴露）·
  `73-proxy-cluster-ha`（GREEN，40 断言，N=3 + 2 agents：proxy exit **#29-免疫**（steady-state SS 经非-tunnel-homed exit 传字节）。#33 **measure-and-record**（crash-rehome 数据面恢复间歇：AUTO-RECOVERED 记 lag / STRANDED 两种都接受、不 die、不倒置；QUORUM off/on 证手动恢复；撤回 round-3 readiness oracle 与 round-4 240s-倒置门 措辞）+ **R5-M6 因果门控** QUORUM 分离：dead-homed exit 的 /sub-vended server 交叉核对 == 将杀 broker（Q-xcheck）+ 证死 broker down（Q-dead）+ black-hole 作**硬前提**捕获 + SEPARATION 断言是对 black-hole 成立的**复合门**（dead leg 仍传字节 → RED，绝无"DEAD while 200"假合取=solo2 bug）+ 非-black-hole 腿被**诊断**（tunnel-survival vs teardown）+ grow retry=1 nuke重试兜底 + SS-leg re-fetch/per-port）·
  `74-rebalance-on-return`（**RED-EXPOSES**（R6-M1）**proxy 分布不稳定（gotcha #34：1/1/1 不 hold——非-tunnel voter proxy-eligibility 不稳定、exit 重堆 tunnel broker）** + moved-exit 数据面（#33）+ auto-rebalance 缺口为 release-blocking，N=3：rebalance distribution。round-5 的 measure-and-record 把这整条擦成 GREEN——已撤回为硬断言。**RED,per-run(failed 数是 branch-dependent,勿把单个数当稳定判定)**：跨 run 观测 1–7 failed(干净重试 `RED 1/35` 仅 C-auto;噪声 run `3/33`/`5/31`/`7/29` 当瞬态 setup-SS/reconstruct 腿也 RED 时)。**稳定的 release-blocking RED = B-dp(#33)+ C-auto(#31 fire-gate)**;总数随该 run 有多少瞬态 setup/eligibility 腿一起失败而变(每条这种 RED 本身就是 #34 不稳定的暴露)。跑通不中止。**R5-M2+R6-M1**：**FAIL-CLOSED 校验快照**（cmd-rc/JSON-有效/恰 3 **互异 nid**/所有 home 是真 voter；无效→唯一 sentinel）+ **锁定 1/1/1 baseline**（rebalance 构造 spread==0 + skew 前证**全 3 条** SS 腿在传）+ **dry-run/real rebalance 要求命令 rc=0**（堵 dryrun_command_failed 假绿）+ **B-dp 数据面（HARD RED，R6-M1）**（SS 腿必须经 rebalance-MOVED exit 传字节；STRANDED→RED 暴露 #33-family——round-5 measure-and-record accept-both 是擅自 acceptance change,已撤回）+ **B-negctrl 非空硬负控**（reg created+有效 homed+serving 前后,堵 `single` fallback）+ **Arm-C auto EFFECT（HARD RED）**（分布必须 auto-even,不发→RED 暴露 auto 缺口）。**NOT-COVERED（仅 raw EVENT）**：`proxy_auto_rebalanced` count==1 EVENT——sys.events 无 operator reader（D2,仅 raw-event;auto EFFECT 不 carve-out,是硬门）。）。
- **S5**：`31-node-upgrade-fleet`（INCOMPLETE，**#28-FIXED** 前置 + fleet enumeration/dispatch/`--timeout`-金丝雀 control oracle；残余 nc_gap = fleet `--all` 扇出）· `33-node-upgrade-success`（INCOMPLETE/1，单台真 success/rollback/upgrade-domain；nc_gap = gotcha #73；已完成真跑）· `30-rolling-upgrade`（GREEN，13，N=3：**gotcha #31** grow lock 泄漏 + [GAP #31] + upgrade-roll NOT-COVERED）·
  `32-install-lifecycle`（GREEN，fresh 容器 + §8.4 running-broker 升级：install.sh 生命周期 + content-manifest。**R5-M4**：(1) manifest **fail-closed on 部分/权限失败的 find 遍历**（MANIFEST-FIND-ERR——旧 `find|grep|sort` 丢 find rc、接受部分列表）；(2) **单个合并 EXIT trap**（原两个 trap 互相覆盖、早退时泄漏 artifact 容器）；(3) **真 ctl 二进制边界**（自己的 place_binary/run/never-start/uninstall，非"同 agent"）；(4) **§8.4 单机手动升级已实现**：live N=1 broker → stop → 换二进制（特权步）→ `sqlite3 PRAGMA integrity_check==ok`（镜像加 sqlite3）→ start → G.2 业务收敛（原 expose 再 serve sentinel + 新 post-upgrade expose serve=真读写）+ version-flip ∧ MainPID-changed 守卫。§8.4 是**操作员手动路径**(systemctl+install)，**非** #31/#28-阻塞的 cluster/node upgrade 动词。此前 R4 的 content-manifest（数字 %u|%g/symlink 元数据/byte-restore self-test/真 agent 二进制安装）保留）。

**台账新增**（`docs/deploy-tier-gotchas.md`）：**#28**（agent 升级 URL 白名单硬编码不可 operator 配置，31 assert_bug；
拒绝消息实证 DOC-3 指向不存在的 `--upgrade-url-allow` flag）· **#29**（N=3 cluster expose 数据面要求
home==agent-tunnel-broker，否则 `token_unknown_or_revoked` 死，71 assert_bug + BASE）· **#31**（`cluster add` grow
lock best-effort release 泄漏残留 `cluster_grow_active`，阻塞 upgrade/join/retire + 恢复〔re-run cluster add〕也走
同一 best-effort 路径清不掉；30 [GAP #31]；**是连续-grow INCOMPLETE flake 与 upgrade-blocked 的共同根因**）。
**#30**（cluster-mode `sub revoke` 不发 `proxy_keyset_changed`）= code-confirmed gap，73 引用为 NOT-COVERED。

**§1 事件/告警面 — sys.events 无 operator reader（重大发现，2026-07-12）**：`admin` 只有 audit/evict/nodes/sessions、
无 events 读命令；events 不进 broker journal；`nats stream` = Authorization Violation。故 **71/72/73/74 的所有
sys.events 测点 NOT-COVERED**（`proxy_enabled/disabled/keyset_changed` row-36 · `proxy_node_unready` row-37 ·
`proxy_auto_rebalanced` row-38 · `expose_rehomed`/`home_reassign_*` row-53/54 · `rehome_stalled` row-55 ·
`sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial` row-64/65/66）——drill 改用**可读的控制面/数据面 oracle**
钉效果（/sub http_code、`proxy status --cluster`、`cluster status` brkH-reachable、SS-egress 字节、home-count 分布），
事件本身作为**产品/DOC 缺口**登记。§1.3/§1.4 相关行处置：「S3-71/S4-72/73/74 断言」→「NOT-COVERED（no operator
reader），效果由可读 oracle 钉」。

**§2 命令勾（臂级）**：`expose`（--local/--remote-port/--on-broker/--name）→ **S3-70/71✓**（四 flag
独立臂 + 数据面 curl；#29 home-binding；`--no-rebuild`/`--ack-alerts` **NOT-COVERED**，Stage-C ledger-1）· `expose explain`（home/moved，无 epoch）→ S3-70/71✓ · `expose rm` →
S3-70✓ · `ps --all` PORTS/HOME → S3-70✓（N=cluster 有 HOME 列）· `proxy on/off`（--ha-policy/--yes）→ **S4-72/73✓** ·
`proxy status --cluster` → S4-72/73✓ · `proxy sub create/revoke/ls` → S4-72/73✓（**revoke/off 数据面外审 M3 已补**：72 revoke 每-sub PSK 断流〔alice 断 WHILE bob 流〕+ alice2 恢复 + off 断流 + `_off_semantics` HTTP-200 门）· `node upgrade`
（--url/--sha256）→ **S5-31◑+33**（**#28 已修** + `--all` 枚举/dispatch/`--timeout`-金丝雀 control 面 COVERED〔外审 M4 补臂 + upgrade-safety 契约改写〕；单台 SUCCESS/rollback/`--wait` → **33 拥有**；fleet 扇出残余无 owner）· `node ls --brokers` → S5-30✓（broker running version
self-report oracle）· `cluster upgrade`（--to-version/--dry-run/--expect-sha256/--account-seed/--backup-taken）→
**S5-30◐**（staging/broker-login/refuse/dry-run/[GAP #31] 覆盖；**real upgrade-roll 机制 NOT-COVERED**——#31 grow lock
阻断，#31 修复前不可达）· `cluster rebalance proxy`（--dry-run）→ **S4-74✓**（distribution 均衡机制 + returned-voter
eligibility timing）。

**DOC 候选**：**DOC-3**（`--upgrade-url-allow` 只 `serve` 有、hint/手册指向不存在的 agent flag，31 定格）·
**rebalance-on-return timing**（broker restart 后 proxy-eligibility 滞后 raft-VOTER，`cluster rebalance proxy` 需等
voter eligible 才生效，74 记录；文档应说明 proxy 流量回归非即时）。

**诚实注记（mandate 保真度，用户 2026-07-12「你的 green 是真的没问题还是擦屁股」质疑触发的全面审视）**：
① **#29 认知纠正**——spike-proxy3 曾误判「proxy home 追踪 tunnel」（那次 2 exit load-spread 巧落 brk1），真相是
proxy home = load-spread voter + directive 携带 HomeBrokerAddr+CertPins 使 agent 主动拨 home → **结构免疫 #29**
（73 铁证 home!=tunnel + rehome 后 SS 仍活）。② **74 假绿揭穿**——B-settle 隔离出之前 B-real PASS 是 return 后自然
reconcile 漂移伪装成 rebalance 生效；强化 oracle 后真验证因果。③ **#31 grow lock（Stage-C mandate-4 订正）**——#31 是 grow
`releaseGrowLock` best-effort 泄漏 `cluster_grow_active`，由 30 real-roll HALT 铁证钉住（upgrade-blocked，3/3）。
**须区分两类 grow flake**：(i) 我 SOLO server-local 跑遇的 `cluster add HALTED at acquire-lock: grow of brkN
already in progress — serialized`（前一 joiner 的 release 间歇失败挡下一个）确是同一 grow-lock 泄漏，我用一个
**临时 server-local retry 调试脚本（不入 repo）**重试搭起 N=3；(ii) simcluster:223-229 记录的**并发**（7-way
sweep）VOTER-timeout 是 clustered-JS meta-group 形成时序、**另一类 flake**，正式 runner `run-drills.sh` 的
`FLAKE_SIG` 不含它、**不 auto-retry**（surface RED 手动重跑）。#31 只 claim (i)+upgrade-blocked，不 claim (ii)。
「几乎总残留」限定 upgrade 场景（brk3 是最后 grow、其 release 时序最差）。④ **30 假绿主动 suppress**——MainPID-same/write-probe-clean
仅在 upgrade 真执行（MECH=1）时断言，否则它们 PASS 恰因 upgrade 没发生 = 假绿，明确抑制。⑤ **#31 grow-lock 只影响 membership 操作、不干扰 71/73/74**
——它们的测试对象（expose-rehome/proxy-rehome/quorum/rebalance）**都不是 cluster membership 操作**（join/retire/upgrade），
grow lock 残留不影响；只有 30 的 upgrade 是 membership 操作、才撞 #31。**NB（round-4 订正 self-review MAJOR）**：这只说
grow-lock 不干扰，**不等于 73 GREEN**——73 当前是 **RED/NOT-RELEASE 待复验**（数据面 move-lag 非确定，见上表 :324 + 本节
M1+M7），71 的 GREEN 也已从"永久 #29"降级为"observed-unreliable + 记录 split"（见 :322）。此前本条写"71/73/74 的 GREEN
是真的"与 :324/:392 自相矛盾，已订正。

### Stage-C 内审 + 批 B 修复（G-A，2026-07-12）

G-A 8 drill 名义 GREEN 后经 **Stage-C 对抗内审**（6 固定 lane reviewer + 6 verifier + synth，全 Opus；用户「你的 green 是真的没问题还是擦屁股」质疑写进 mandate-fidelity lane 审查焦点）→ **22 findings 全 verifier-CONFIRMED**（11 major 含多个 plan-mandated 假绿/vacuous-oracle），主进程**全采纳无驳回**，分批修复真跑绿化。报告 + 逐条处置见 `s3-s5-review.md`。

**批 A（文档/事实错误）**：#31 confession 订正（删不存在的 `drill-retry.sh` 引用〔临时 server-local retry 脚本、不入 repo〕 + 区分 SOLO-serialized-fence〔grow-lock〕vs 并发-VOTER-timeout〔JS-formation，run-drills 不 auto-retry〕 + 订正「清 lock 后测机制」为「恢复清不掉→机制 NOT-COVERED」）· #30 从 flipping-pin 改 un-pinnable · expose `--no-rebuild`/`--ack-alerts` 从 covered 改 NOT-COVERED（ledger-1）· 71 #29 pin 用词 assert_bug→inverted-assert_ok（ledger-2）· 71 header divergence 措辞收敛（dp-2）。

**批 B（drill 假绿真跑修复，assertion 增量）**：**73 27→34**（quorum 数据面分离：dead-homed SS 黑洞 WHILE survivor-homed SS 传字节 + /sub-200 → 证 /sub-200≠数据面 #20；mandate-1=pin-2=dp-1 + cluster-4）· **30 12→13**（M1 非空 WROTE-guard + roll-order oracle + 30-C 理由；mandate-2=pin-1 + cluster-1/2）· **71 9→10**（#29 signature 收紧 + NONTUN-cert-eligible-VOTER race 判定；mandate-3 + pin-4）· **72 30→32**（TOKa2 主 shell + aead 改 agt1-无字节-纯AEAD〔agt2-journal-区分真跑不可靠〕 + loopback-neg；harness-safety-1/2 + dp-3）· **74 17→23/24（Arm-C auto 非确定，外审 M6：per-run，auto 触发则 24、否则 23）**（SKEW-precond KTGT>0 + no-unhomed + Arm-C auto-rebalance **诚实 NOT-COVERED**〔真跑 `TETHER_AUTO_REBALANCE=on` 在 sim 没触发 auto-even〕；pin-3/6 + cluster-3）。**两处修复假设经真跑二次订正**（72 aead 的 journal-区分、74 auto-path），均真跑验证、绝不弱化 oracle 凑绿——本身即「暴露问题」。

### 外审（G-A/S3–S5）Fail 修复 + 重跑（2026-07-12，7 M + 1 m 全采纳无驳回）

外审从零暂存基线独立复查，判 **Fail**，列 M1–M7 + m1。主进程全采纳并真跑修复（经 `tether exec weilandserver` 服务器本地跑，SSH 直连已断；drill 经 base64-inline 传输 sha256 校验）。**⚠ 下列"2× isolated GREEN"是 round-1 外审修复当时的状态；round-2/3/4 后 73 已翻 RED（不再声称 73 的 2× GREEN，见 :392），71 已降级为 observed-unreliable（见 :322）——保留此段作历史修复记录，当前权威状态见上表 :320-327**。当时（round-1）的 assertion 数（真跑定格）：

- **M2（71 #29）** 10→**12**（2× isolated GREEN）：`cert_fp` 可交付前置 + **agt1 journal 的精确 `token_unknown_or_revoked`**（ctl 只有泛化 frpc_failed）+ settle + 探后复证 → 实测 deliverable home 上仍 deny ⇒ **#29 真缺陷成立**。
- **M3（72 revoke/off 数据面）** 32→**39**：revoke 前后真 SS 腿——alice 断 WHILE bob 流 + alice2 恢复 + off 断流 + `_off_semantics` HTTP-200 门（非 vacuous）+ secret-logging 移除。
- **M4（31 fleet，当时状态）** 15→**26**：`--all` 枚举/OFFLINE-排除/dispatch/config-abort/`--timeout 0` transient-skip；当时 fleet SUCCESS 因 #28 墙而 NOT-COVERED。**当前 #28 已修**，但真 re-exec/re-register/rollback success 臂仍未实现且尚无 owner；见当前权威行。
- **M5（32 install）** 12→**13**：`_snap` 改 content+metadata manifest（sha256/mode/owner/link，非路径名）+ 自测 + ctl/agent dry-run 以 sim；真二进制安装 + §8.4 诚实 NOT-COVERED（--skip-download 跳 place_binary）。
- **M6（74 default-off）** 23/24→**24/25 per-run**：default-off 加 eligibility-证明 + 30s quiet-window 稳定断言；A-elig poll 150s；末尾硬编码文案改条件式。2× isolated GREEN(24)。
- **M1+M7（73）** 34→36：确定 1+1 构造 + 两条必需 baseline + 每 destructive arm 前 fail-fast + QUORUM 数据面分离；真跑**观测到 gotcha #33**（crash-rehome 后 SS **数据面**恢复滞后控制面——控制面 rehomed+ready 但数据面那一刻仍黑洞、恢复 lag 可变 <45s..>150s——**仅观测 + per-run 测量**）。**⚠ round-2 外审 Fail（R2-M1/M2/M3）**：① 73 非确定 + grow/rehome 失败后后续臂继续 → 已加 grow/foundation/control-rehome 的 die 硬门 + off/on 分开断言；② `[GAP #32]`→**`[GAP #33]`**（撞 plan §355 的 #32-candidate），`_ss_no_prompt_recovery` 要求 ss_up 成功（前置缺失不再假 PASS），**删除错误根因**（ApplyHome 实为 OpenHome 原子换 session+重拨，非 re-point 已断 session）+ 删除 >150s/~300s/eventual-auto-recovery 未证声明；③ 73:126 token 日志 + 167 裸反引号命令替换已修。**round-4（self-review）再订正**：#33 从 round-3 错误的 readiness oracle（`_ready_lags_60s`——proxy_ready 恢复快、数据面才 lag，两头 flaky）改回**数据面** observe-measure（rehomed+ready 瞬间 `_ss_deadhole` + 测量恢复 lag + 仅 240s-必恢复 die）；删 ">90s" 自相矛盾数字；三条 destructive baseline die-gate；moved-exit 数据面 poll 放宽 240s；`_pick_nontunnel` 排除 leader 防 spurious die。**仍 RED/NOT-RELEASE 待 strict-serial 重跑复验**，不声称 "2× isolated GREEN"。
- **m1（spike 清理）**：11 个 untracked spike 已删。

**round-5 外审 Fail（R5-M1..M7）全采纳 + 真跑修复（2026-07-14，经 tether exec，SSH 断）——当前权威状态见上表 :320-327**：
- **R5-M1（72）**：held-open 强断改**字节可观测**（ThreadingHTTPServer + 收字节数严格增长 baseline；alice 早退+冻结=强断 WHILE bob 增长）+ OFF 回收查 port_allocations 权威源（sqlite3）+ 安全同端口复用。
- **R5-M2（74）**：snapshot **fail-closed**（cmd-rc/JSON/恰 3 行/真-voter 校验；空/坏→唯一 sentinel，绝不假证 balanced/stable/zero）+ 锁定 **1/1/1 baseline** + **B-dp 数据面**（SS 经 moved exit，本 drill 内证）+ **B-negctrl 负控**；仅 `proxy_auto_rebalanced` EVENT NOT-COVERED（无 reader，owner-decisions D2）。
- **R5-M3（71）**：#29 措辞改**源码准确**（homeForExpose un-homed 回落，非"设计上硬编码固定 tunnel"）+ Arm D **真跨 crash 注入**（rebuild-ON+OFF 一次 crash）+ 非-leader 硬门 + FIXTURE 门（agt→brk3 间歇不建立时 NOT-COVER-THIS-RUN）；B/E/G/F NOT-COVERED 的**三重叠加墙**（un-homed 回落 / agent-隧道-到-非-leader 间歇 / grow 遗留 NATS_ROLLED_OUT op 阻塞 drain）+ **owner decision D1** 记录。
- **R5-M4（32）**：find 遍历 rc 捕获（fail-closed）+ 单合并 EXIT trap + 真 ctl 边界 + **§8.4 实现**（sqlite3 integrity_check，镜像加 sqlite3）。
- **R5-M5（grow）**：`grow_to_3` 加 retry 参数（drill 30 走 no-retry 单次保住 #31 证据）+ attempt 数作一等证据（GROW-ATTEMPTS trailer）+ 删 dead code（`_ensure_grow_lock_released`/`_egl_locked`/`_clear_lingering_ops`）+ 修正 solo/parallel 诊断（VOTER-timeout 不在 FLAKE_SIG、不自动重试）。
- **R5-M6（73）**：QUORUM 分离**因果门控**（vended-server 交叉核对 == 将杀 broker + 证死 broker down + black-hole 作硬前提 + SEPARATION 复合门，消除 solo2 的"DEAD while 200"假合取）。
- **R5-M7（docs）**：#29/#33 gotcha 段 + README + 本 inventory 全对齐可执行事实；撤回 240s-倒置门/固定-lag/2×-strict-GREEN/tunnel-coupled-by-design 措辞；owner 决定写入 `s3-s5-owner-decisions.md`。

**判定反转**：外审比内审更狠地暴露了「非确定拓扑上的 vacuous 继续执行 + secret 日志泄露 + 假合取 + 无 owner 授权记录 + 措辞与源码/可执行事实冲突」，全部真跑修复——**这正是 mandate「暴露问题、绝不擦屁股」的落实**。逐条处置见 `s3-s5-external-review-round5.md`（本文件内的回复段）。

### G-B 开发者原始 landing 快照（S6+S8，2026-07-15；已被 round-4 严格重审取代）

> **非现行验收口径。** 下列“PRODUCT-RED/INCOMPLETE 非阻断、九项预期非 GREEN”的文字保留为被审查
> 快照；round-4 已证明该策略未经 owner 授权且会放行真实缺陷。现行口径见本节末尾的“round-4 严格
> remediation”：所有非 GREEN 默认阻断，只能显式 waiver，waiver 也不得称为 GREEN。

**重大契约变更（外审 round-1/2/3 驱动）——五态 verdict 契约落地，废止「已知缺陷=harness-GREEN 连绿」。**
G-B 是第一批在 `lib/assert.sh` **五态 verdict 契约**下写的 drill：drill 落**唯一 landing verdict**（GREEN /
PRODUCT-RED〔signature-guarded 复现已登记缺陷——harness 按预期工作、**非绿、预期**〕/ INCOMPLETE〔`not_covered`
覆盖缺口〕/ SETUP-RED / ASSERT-FAIL），`drill_end` 发结构化 `DRILL-VERDICT` 行，`run-drills.sh` 按 verdict
分类 + rc 交叉校验（legacy `drill_end;exit N` → VERDICT-RC-MISMATCH blocker）。缺/空签名/缺参 fail-CLOSED。
SSOT = `lib/assert.sh` 头注真值表 + `tests/verdict-contract-test.sh`（三壳 34 断言）+ `tests/lint-drills.sh`
（9 drill 硬闸 0 违规、legacy 出 advisory）。**mandate 校准（用户 2026-07-15「目标是暴露问题、不是全绿」）：
PRODUCT-RED/INCOMPLETE 是暴露-缺陷 harness 的预期产物、owner-tracked、绝不伪装成 GREEN。**

**drill 在契约下的诚实预期落地（per-row disposition；非"全 GREEN"）**：
- **`22-forcesingle-online`** → **INCOMPLETE**（或 PRODUCT-RED if #35）：ONLINE force-single dwell/refusal gates
  + protected-mode + 全函数 Arm-0。**#35 降级 CANDIDATE**（round-3 M5）——仅 PROVEN survivor-restart（MainPID 变）
  + dwell-never-satisfied 才 `assert_bug #35`→PRODUCT-RED，否则 `not_covered`。GATE-d/TAMED = `not_covered`。
- **`40-drain-retire`** → **INCOMPLETE**（或 PRODUCT-RED if #31/#45）：drain 往返 + ops schema + reconcile-plan
  refusal/zero-write + safety negatives。retire 脊 #31-intermittent（`product_red` 阻塞 / `assert_bug #45` stall /
  converged GREEN）。OPS-ABORT/ADD-dryrun = `not_covered`。
- **`41-shrink-to-standalone`** → **GREEN if shrink converges, else PRODUCT-RED**（#31/#45）：peer-present refuse +
  rebind + before/after voter count + raft-replicated session 存活 oracle + JS-reset-broker-active + 3-way to-standalone。
- **`42-rejoin-returning`** → **INCOMPLETE**：diagnose 正负 + rejoin-prepare O_EXCL + resnapshot single-voter +
  Tier-2/machine-confirm。Arm-A journal-catch + DOC-2 + E/F/I = `not_covered`。
- **`43-migrate-live-data`** → **INCOMPLETE**：init `--check` zero-write + `--yes`/machine-confirm 负例 + from-existing
  cutover + **3-way rollback**（DB byte==bak + cluster-off + bootable standalone）。business-survival + E cluster-化
  candidate = `not_covered`（bare P2 不 serve NATS，SB-43）。
- **`90-alerts-lifecycle`** → **INCOMPLETE**：manual raise/ack/clear + 真 broker_down + quorum_lost-ack refuse；absence
  谓词 fail-CLOSED（valid-JSON 门）。M6 disk（#39 5-min 固定）+ below_quorum/raft_lag = `not_covered`。
- **`91-client-converge`** → **INCOMPLETE**（或 PRODUCT-RED if #46）：A1 publish + A2 grow-auto-include + C 幸存者-only。
  **#46**（此前 #G3）= `product_red`（brk3 达 VOTER 却不进 seeds）。D cli-failover + A3 = `not_covered`。
- **`92-js503-remote-alert`** → **INCOMPLETE**（或 PRODUCT-RED if #42）：quorum-loss READ-ONLY 自纠（`#42` `product_red`
  if 不自纠）+ `session rm --ack-alerts` 证到达写路径（非 gate、非 connect/auth）+ 12 MiB tier-B 佐证。leg-b banner +
  recovery = `not_covered`。
- **`93-metrics-observability`** → **INCOMPLETE**：/metrics 真值 + /healthz&/readyz（HTTP status+body，`ready` 排除
  `not ready`）+ webhook（raised+cleared，no-secret）+ --card/JSON。LOGJSON + --watch（需容器 PTY）+ all-down + READYZ-503
  = `not_covered`。

**台账新增/变更**（`docs/deploy-tier-gotchas.md`，与 plan §4 表零漂移）：**#45**（40/41 retire NATS_ROLLED_OUT
收敛停滞，独立号——此前误标 "#37-family"，round-2/3 M6）· **#46**（91 seeds 漏第 3 voter，此前 #G3）· **#35 降级
CANDIDATE**（round-3 M5，未在 sim 确定复现）· **#42** RATIFIED（有界 ~TFence 窗口，#43/#44 折入）· **#39/#36** 沿用。

**§2 命令勾（臂级，G-B 消费）**：`cluster drain/retire/ops`（40）· `reconcile nats --to-standalone/--plan`（40/41）·
`cluster recovery force-single --online / diagnose / rejoin prepare / resnapshot`（22/42）· `cluster init --from-existing`
（43）· `alert raise/ack/clear/ls`（90）· `cluster seeds publish/show`（91）· `cluster status --remote/--card/--watch/--homes`
（92/93）· `/metrics` `/healthz` `/readyz` + alert webhook（93）。逐命令勾入 §2 对应行、无 NULL-diff 遗漏。

**收尾**：地基（`lib/assert.sh` 契约 + `run-drills.sh` 解析）+ 9 drill 迁移 + SSOT 收敛 + hermetic tests 本窗口
一次落地；远端 fail-closed 复跑受影响 drill 后 → **停外审门**（不 commit、外审进行中不 git add）。逐条外审回复见
`s6-s8-external-review.md` / `-round2.md` / `-round3.md` 尾部主进程回复段。

### Round-4 严格单跑结果与修复状态（2026-07-16）

无重试、`-j1` 的九项实跑不是全绿：22/43/92 GREEN；40/90/93 SETUP-RED；41/42/91 ASSERT-FAIL。
其中 90 的 JS mountpoint、93 的非法 alert kind 是 fixture 缺陷；42 同时含一个假 audit oracle 和一个真实
resnapshot/Raft 恢复缺陷。严格分诊还确认了 40 的 grow auth/catch-up fence、41 的连续 retire agent 孤岛、
91 的 terminal-retire seeds 不收敛。修复保留原产品 oracle：不重启 agent、不删 cache、不停 retired broker、
不手动 publish seeds、不重试 drill。独立回归覆盖 auth 重连边界、async join/retire seed terminal 契约、signed
roster event refresh、Raft-tail peer 复活与恢复后真实权威写。

**当前 release 判定仍为 Fail**：本地 contract/lint/目标单测通过不等于 deploy-tier GREEN；平台远端额度在
2026-07-16 阻止 post-fix 重跑，因此 #47/#48/#49 与 40/41/42/90/91/93 的真栈翻转尚未得到证据。不得把
“已实现修复”改写成“已关闭 finding”。详见 `s6-s8-external-review-round4-implementation.md`。

### G-B landing（S6+S8，2026-07-17，外审 8 轮 Pass）

**上一段「当前 release 判定仍为 Fail」已被后续事实推翻，此处为最终状态**（该段保留作 round-4 时点记录）：
round-4 时缺的 deploy-tier 证据已补齐 —— 最终二进制（含全部产品修复）真栈复跑
**`20-forcesingle-natsconf` GREEN(14) · `91-client-converge` GREEN(37)，0 FAIL**（vendor sha == 镜像 sha，
经 `simcluster:528-531` 的 fail-closed 陈旧守卫校验）。外审 **round-8 = Pass**。

**drill（9）**：`22`/`40`/`41`/`42`/`43` · `90`/`91`/`92`/`93`，全部在 `lib/assert.sh` 的**五态 verdict 契约**
下运行（GREEN / PRODUCT-RED / INCOMPLETE / SETUP-RED / ASSERT-FAIL；`DRILL-VERDICT` 结构化行 + runner 严格
grammar 校验 + rc 交叉校验 + **默认 fail-closed，waiver 须显式**）。契约 SSOT = `lib/assert.sh` 头注真值表；
钉子 = `tests/verdict-contract-test.sh`（三壳）+ `tests/lint-drills.sh`（批次硬闸 0 违规）。

**本批最大的教训（记入台账，供后续批次引用）**：
1. **harness 的 oracle 可以制造假的产品故障**。drill 91 长期 ASSERT-FAIL 被判为「真实产品缺陷：seeds 不
   收敛」——实为 drill 自己把 `force-single` 管进 `grep -q`，`grep -q` 命中 `ForceSingle` **内部日志**即退出、
   SIGPIPE 腰斩 CLI，使其后的 nats.conf 去集群化**从未执行** → survivor exit-70 crash-loop → `seeds show`
   连不上。三次受控实验（带/不带管道）定死因果；去掉管道后 91 GREEN(37)。**禁令已入 lint**
   （`sigpipe-truncation`：mutating tether 命令不得管进 `grep -q`）。对照旁证：drill 20 同样管道却长期绿 ——
   因其签名是 CLI **终末行**，彼时工作已完成。
2. **计划外的产品手术风险极高**。S 批原定「零产品 Go diff」，外审者在 round-4 特殊 stage 修了真实 raft
   恢复缺陷；随后的 5 轮外审在这批产品代码上连续发现：darwin 构建断裂（发布线整体产不出物）· root-run
   recovery 把 raft/ 与锁变 root-owned → 唯一幸存 broker 永久拒启 · 目录交换绕过 bolt 锁互锁 · force-single
   非原子无中断标记 · **锁的 chown 跟随符号链接 = 本地提权**。
3. **修复必须接到调用点，且必须扫同类面**。多次出现「改了函数没接线」（属主修复只在
   `AcquireDataDirLock`，五个 offline 入口仍用私有 `acquireFlock`）与「只补被点名那一处」（journal 的符号
   链接洞刚修，同轮又在锁上原样重造）。现以**结构性守卫**钉死（`TestRound6_NoPrivateFlockHelperSurvives`
   禁止私有 flock helper 复活）+ **调用点绑定回归**（删任一修复即精确变红，逐条 mutation 验证）。

**台账新增/变更**：#45（retire NATS_ROLLED_OUT 收敛停滞，独立号）· #46（91 seeds 漏第 3 voter）·
**#35 降级 CANDIDATE**（未在 sim 确定复现）· #42 RATIFIED（有界 TFence 窗口，#43/#44 折入）。

**已知未闭合（不冒充闭合，留给后续批次）**：`InterruptedForceSingle` 无生产调用者（B1 可诊断性半成）·
`blockAfterAttempts` 需 step-aware + `AbortOp`/`ConfirmOp` 守卫 · 原子交换预检的 EBUSY/ENOSPC 残留
（**rename 已崩溃一致、事务尚未**）· prune→exchange 窗口需 phase-aware 跳过。

---

### G-C landing（S7+S9，2026-07-17）+ S1–S9 CLOSURE

**drill（7）**：`50`/`51`/`52`（S7 备份·灾备·凭据轮换）· `94`/`95`/`96`/`97`（S9 混沌对账·长稳），全部在
`lib/assert.sh` 五态 verdict 契约下运行，全部进 `tests/lint-drills.sh` 的 `BATCH` 硬闸（现 16 drill、0 违规）。

**真栈 landing（与 s7-s9-plan §0.1 预期对表，`remote.sh` 全路径）**：
- **50-backup-restore** = **PRODUCT-RED**（68 pass, 0 assert-fail）：#50 · #64（新）· DOC-27。
- **51-full-dr** = **PRODUCT-RED**（45 pass, 0 assert-fail）：#51 + DR-STEP-LEDGER 量化（undoc=2）+ DR-completion NOT-COVERED。
- **52-credential-rotation** = **PRODUCT-RED**（0 assert-fail）：#54 两面 · #56 · DOC-23 + 两处 NOT-COVERED。
- **94-agent-reconcile** = **GREEN**（51 pass）：G.1 missed-exit + orphan（产品路径）+ G.5 审计 + ps LOST。
- **95-broker-selfheal** = GREEN body：#23 判别性（clean-exit T1 / unclean-exit T2 两判别子实测通过）+ DELETING NOT-COVERED。
- **96-mid-flight-chaos** = **PRODUCT-RED**：**#58（LIVE-CONFIRMED，外审 B2 修 oracle 后 7 次复跑一致）**——旧 `grep -c OBJ_xfer` 数 stream 存在性=假阳性，改 /jsz 数对象消息数 + 干净基线差分（baseline=1→orphan=2→重启后仍 2=未回收）；**#65（非确定性候选，外审 B10）**——D6b 逐-broker RAW artifact 显示分区少数派 stale-leader 写 6 轮里 5 轮持久（多数派可见）、1 轮回滚（退化 run），是 raft-safety 疑点、owed 产品侧根因（不记确定性 PRODUCT-RED）。分区旗舰臂 GREEN（rc=124 自证 + brk1 alive/stable + D6 多数派写读回 = 无脑裂-丢失方向）；#57/arm-B/C = NOT-COVERED（in-sim 时序/#29/DOC-28）；double-fault 臂在分区臂未完全恢复时 gate not_covered（跨臂隔离）。
- **97-soak-cycles** = **GREEN**（41 pass）：四型注入 + fd/RSS/Threads leak oracle + panic/FK 完整性 oracle。

**harness 增量**：`lib/vault.sh`（S0-备份库，per-instance host 目录、存活 rm_node --vols、随 nuke 回收）·
`drills/lib/fault.sh`（S0-故障原语，`iptables` 静默 DROP 分区 + SIGSTOP 冻结 + 124/28 判别子）·
`drills/lib/events.sh`（sys.events core-sub 观测，member 可读，推翻 G-A/G-B「无 reader」carve-out）·
`drills/lib/leak.sh`（fd/RSS 泄漏 oracle）· `lib/secrets.sh` 轮换代 · `drills/lib/cluster.sh::grow_to_2` ·
`simcluster::cmd_nuke` 接 vault · `Dockerfile` += iptables · `.gitignore` += backups/。

**lint 新增 3 条结构性守卫**（G-C 开发期反复踩、已 mutation 验证）：`noshc`（harness 函数不得裹 `sh -c`）·
`backtick-in-desc`（断言描述里反引号=命令替换）+ BATCH 含 7 个 G-C drill。

**本批最大的教训（记入台账，供后续批次引用）**：
1. **harness 的 oracle 反复制造假的产品故障**——G-B 是 SIGPIPE，G-C 是 `cluster status` 退出码当存活探针
   （HEALTH 非 LIVENESS，DEGRADED 恒退 1，restore 后 poll 永不成功→差点把自己 bug 报成产品缺陷）。已入
   R-LIVENESS-NOT-HEALTH。同类还有：`node ls` 字段 `nid` 非 `node_id`、audit pid 是 ULID、journal
   `--after-cursor` 静默过滤（改 timestamp gate）、`pull` 语法、前台 exec 才造得出 missed-exit。全部实测钉住。
2. **深水区 drill（51/52/96）须"发现钉住即 gated 收尾"**——DR/轮换的手动恢复剧本在真栈本就不完整（这正是
   #51/#52/#54 想说的），硬跑会级联成十几个 assert-fail 掩盖真发现。正解：核心 gotcha 钉住后，依赖"恢复成功"
   的后半段一律 gated NOT-COVERED（诚实登记"按文档不可完成"），而非 cascade。
3. **零产品 Go diff 守住**——`go test ./cmd/tether -run TestCommandTreeInventory` 通过（零 CLI 变更）。

**台账新增**：#50 · **#51（restore 不 apply cluster seam，LIVE-CONFIRMED）** · **#52（restore 不渲/不提 nats.conf，SOURCE-CONFIRMED）** · **#53（bundle 无 JetStream ⇒ DR 后 audit 全失，SOURCE-CONFIRMED）** · #54 · **#55（account.nk 轮换 auth-rejection 窗口 = #54 下游、in-sim 不可构造，候选）** · #56 · #57 · #58 · #59 · #63 · #64 · **#65（分区少数派 stale-leader 写*有时*持久，raft 安全性，非确定性候选、owed 产品侧根因）** · DOC-23 · DOC-27 · DOC-28 · [#29 续] blast-radius 扩（allocate-time frpc_failed）。

**G-C-SWEEP（收官独占义务，逐行裁）**：
- **row 54/55/56（rehome kind）**：G-A/G-B 以「无 reader」判 NOT-COVERED——**理由已被源码证伪**
  （`permissions.go:36/:147` member 可 core-sub + `rehome_events.go:9-11` 全落 SubjSysEvents）。**结论（deploy-tier
  不单测 rehome 事件）仍成立**，但**理由换为**：rehome/drain 事件的效果面由 71(#29 数据面)/96-C 的真流量 +
  store-backed 告警钉住；raw event 配对不单列，因其为 operator-facing 契约而非一等 CLI reader（DOC-26 登记）。
- **row 78 `replication_degraded`**：S5-30 owns（滚动升级窗口瞬态）；S8-90 不重复（G-B §11-U8 已裁 drop S8-90 腿）。
- **row 123 `expose/expose rm --ack-alerts`**：G-B 指派 92(a)，G-B landing 已兑现（92 GREEN 含 ack-alerts 臂）。
- **row 164 `cluster upgrade --notify-webhook`**：#31-blocked（upgrade-roll 受 grow-lock 阻），NOT-COVERED-tied-#31，
  与 93 的 `alert_webhook_url`（已覆盖）是不同 webhook。
- **broker-ops §7.4 的 JS 快照面（sqlite3 .backup / tar jetstream / nats stream backup）**：非 tether 动词，
  NOT-COVERED（DOC-20 订正 roadmap §4.3 的「§7.4=cluster backup 同机制」事实错误）。
- **永久 NOT-COVERED（各附源码理由，s7-s9-plan §5.1）**：97 goroutine 数（无 pprof/expvar；Threads≠goroutine）·
  P8 24h soak parity（时长/节奏/常驻三 delta）· 94 PID-reuse 支 / agent-exit 支（结构不可造/产品从不发）·
  restore `--raft-addr` 换 IP 动机（sim DNS 寻址）· 跨 proto flag-day（需 v1 车队基线）。

**S1–S9 CLOSURE 断言**：主线 P0–P13 + B/C/D/G 系列 + S1–S9 的每一条使用者面/agent面/broker面/集群面/横切承诺/
事件告警面，已各自归入某个 drill 或显式 NOT-COVERED（附源码理由）。**收官闸**：命令树 golden
（`command_tree_inventory_test.go`）零 diff + `pubSysEvent`/`alert_ops` 重枚举无表外项。**无未勾且未登记的行。**
