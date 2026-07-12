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
| `expose <node>` | — | `--local/--name/--remote-port/--no-rebuild/--on-broker/--ack-alerts` | S3-70/71 | 五 flag 全部独立臂 |
| `expose explain <name>` | — | — | S3-70/71 | home/epoch/moved |
| `expose rm <node>` | — | `--name/--ack-alerts` | S3-70 | 断流 + 端口回收 |
| `push` / `pull` | — | `--force/--timeout/--ack-alerts` | 既有 00 + S1-61 | 双向执法/边界 |
| `history` | — | `--lines/--kind/--follow` | S1-60/61 + S9-94 | 含 G.5 记录 |
| `node ls` | — | `--all/--brokers` | S1-60 + S5-30/31 | skew 视图 |
| `node upgrade <nid>\|--all` | — | `--url/--sha256/--all/--timeout` | S5-31 | 探索→定格（agent 白名单墙） |
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
| `serve` | — | `--config/--db/--admin-socket/--nats-url`（部署 seam 例外）`/--nats-conf-path/--nats-server-bin/--auth-callout-seeds-dir/--sub-http-listen/--cluster-manifest-listen/--metrics-listen/--alert-webhook-url/--log-level/--log-json/--upgrade-url-allow/--cluster-data-dir/--cluster-raft-addr/--cluster-secrets-dir/--colocated-agent-nid/--tunnel-addr/--tunnel-public-host/--store-dir/--public-host` | 既有组建 + S8-93（metrics/webhook/log-json）+ S4/S2（两 loopback listener）+ S5-30（colocated）+ 既有 13/doctor（nats-conf-path/nats-server-bin seam） | 两 listener 的 loopback fail-closed 是 S0-ingress 的产品边界断言 |

### 2.2 `cluster` 面（root persistent flag：`--socket`）

| command path | Hidden | 行为 flag（排除规则外全列） | 归属 | 断言或理由 |
|---|---|---|---|---|
| `cluster status` | — | `--card/--homes/--watch/--remote/--offline/--db` | 既有 00/10 + S8-92/93 | exit taxonomy 分立断言 |
| `cluster drain <id>` | — | `--now/--abort` + **Hidden `--retire`**（REMOVED-redirect：报错导向 `cluster retire`） | S6-40 | 40 顺带 redirect 报错断言 |
| `cluster retire <id>` | — | `--wait/--timeout/--secrets-dir/--compromised/--require-credential-rotation` | S6-40 + S7-52 | C7 轮换臂 |
| `cluster transfer-leader <id>` | — | `--wait/--timeout` | 既有 10 | |
| `cluster rotate-tunnel-cert <id>` | — | `--cert-fp` | S7-52 | |
| `cluster set-raft-addr <host:port>` | — | `--route/--allow-loopback` | S6-41（rebind 臂） | |
| `cluster backup` | — | `--out/--offline/--db/--data-dir/--secrets-dir/--allow-stale-follower` | S7-50 | follower 默认拒成对臂 |
| `cluster ops ls/show/confirm/abort` | — | — | S6-40 | confirm/abort 由 BLOCKED/STALLED 态驱动 |
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
| `cluster init` | — | `--from-existing/--from-manifest/--check/--dry-run`（--check 别名）`/--confirm-node-id/--config` + 身份组（`--self-id/--name/--node-ident-pub/--raft-addr/--nats-route/--tunnel-addr/--public-host/--data-dir/--db/--secrets-dir`）+ Hidden `--yes` | 既有 init/grow + S6-42（--from-manifest）/43（带数据 + --check 顺带） | |
| `cluster doctor` | — | `--offline/--secrets-dir/--db/--conf/--raft-addr/--nats-route` | S7-50（正向 preflight 臂）+ S6-42 | **preflight 输入面**：错 DB/conf/监听地址仍判绿 = 部署风险——50 的 preflight 臂对照真值/错值各一断言（plan 定格） |
| `cluster takeover-natsconf` | **Hidden**（deprecated 别名 → `reconcile nats --manual`） | 同 manual 组 | NOT-COVERED | 与本体同 RunE（字节保留），本体已覆盖；一 release 后删 |
| `cluster recovery diagnose` | — | `--self-id/--db/--offline` | S6-42 顺带 | 与 online `--dry-run` 不同代码路径 |
| `cluster recovery force-single` | — | `--online/--dry-run/--guided/--self-id/--self-addr/--confirm-peers-dead/--data-dir/--db/--nats-conf/--nats-server` + Hidden `--yes` | 既有 12/20（OFFLINE）+ S6-22（ONLINE + 拒绝门五臂 + `--guided`→42 顺带） | `--nats-conf/--nats-server` 决定 OFFLINE 去集群化写哪份 conf/用哪个 binary 做 fail-closed 校验——12/20 隐式走默认，显式/错值负例 plan 定 |
| `cluster recovery resnapshot` | — | `--self-id/--raft-addr/--data-dir/--db/--confirm-node-id/**--accept-audit-loss**` + Hidden `--yes` | S6-42（变体） | **`--accept-audit-loss` 是显式数据损失开关**：42 两臂——有未发布 audit 且不带 → 拒；带 → 截断继续（探索→定格钉真实语义） |
| `cluster recovery rejoin prepare` | — | `--self-id/--dump-divergent/--emit-manifest/--guided/--data-dir/--db/--secrets-dir` + Hidden `--yes` | S6-42 | |
| `cluster recovery restore <bundle>` | — | `--confirm-node-id`（**provenance 锚，typed-confirm**）`/--data-dir/--db/--raft-addr/--secrets-dir` + Hidden `--yes` | S7-50/51 + S9-94（orphan 造法） | **never-escapable（R5-F2）**：`confirmTypedNodeID(machineEscapable=false)`——50 三断言：① `--confirm-node-id` 与 manifest/provenance 不符 → 拒；② `--yes` 必拒；③ **flag+`$TETHER_CONFIRM_NODE_ID` 同时正确、非交互执行仍拒**（`TestRestoreNeverEscapableEndToEnd` 的真栈版）。`--raft-addr` = fresh-host IP 变化的恢复逃生 → S7-51 的 DR 臂 |
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
