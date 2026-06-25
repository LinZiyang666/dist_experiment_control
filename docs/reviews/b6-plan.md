# B6 plan — Ops day-2 重型 / 净新子系统（backup/restore+DR · alert webhook · 滚动升级+版本 skew · incident export · recover→manifest · machine-confirm）

> Stage A：9×Opus 对抗规划（4 drafter backup-restore-dr / upgrade-versionskew-confirm / webhook-incident / scope-sequence-risk → 4 critic wal-owner-restore-safety / webhook-confirm-security / scope-split-correctness / test-adversary → 1 synth）。synth 现场核验全部 load-bearing claim（file:line），抓到一个**真缺陷**（restore-index 腐蚀）。主进程定稿采纳。
>
> **裁定：B6 = 一个批次，一次实现完。** 不拆 B6a/B6b、不在 B6 中途外审——按 program goal「每个 B 完整走流程但删外审环节、外审统一留到 B7 后」。下表 **IN/DEFER** 仅决定**是否进 B6**；**实现优先级列**只是批次内的下手顺序（先做会腐蚀持久状态/削弱安全门的安全核心，再做增量），不是子批次、不带各自外审。

## §0 — 逐项 IN/DEFER 裁定

> "实现优先级" = 批次内下手顺序（不是子批次）：**安全核心**（会腐蚀持久状态或削弱安全门）先于**增量**。

| 项 | 进 B6？ | 实现优先级 | 一句话理由 |
|---|---|---|---|
| OPS#3 backup/restore + DR runbook | **IN** | 安全核心 | 最高腐蚀风险；restore 触 single-WAL-owner 不变量。 |
| OPS#11 recover→manifest（identity-only） | **IN** | 安全核心 | 与 restore 共构；同 offline 纪律。**不重放业务行**（divergent dump 仍"不可自动 merge"）。 |
| OPS#4 skew-**reject**（仅 proto 硬拒） | **IN** | 安全核心 | 坏门砖死滚动升级；安全关键。 |
| OPS#4 version-in-status 渲染 + 升级 runbook §5 | **IN** | 增量 | 增量读面 + 文档。 |
| AUTO#8 machine-confirm escape | **IN** | 安全核心 | 削弱 B3 quorum 门。 |
| OPS#2 alert webhook | **IN** | 增量 | 净新出站 I/O；不腐蚀持久状态。 |
| OPS#12 export-incident | **IN** | 增量 | 只读组装；风险是泄密（scrub）非腐蚀。 |
| B5: disk/ports DEGRADE band | **IN** | 增量 | 值已在线；computeHealth 加 band。 |
| B5: cluster streams_actual/target /metrics gauge | **IN** | 增量 | `streamObserve` 已有；廉价 gauge。 |
| B5: broker.yaml 键（--log-level/--log-json/--metrics-listen） | **IN** | 增量 | 平凡声明式配置。 |
| B5: `add`/`drain --retire --wait` | **DEFER→B7** | `cluster wait <node> --phase` 已覆盖；纯糖。 |
| B5: cordon / agents_repinned / js_store_used_pct | **DEFER→B7** | cordon 是放置子系统；两 gauge 需新探针且无消费者。 |

**MIGRATION 裁定：B6 零新 migration。** 持久化 `join_release_version` 列**无 reader**（skew 门比对 admission 时呈现的 wire 字段 `JoinerProto`、pre-Propose；status 走 live `ClusterHealthResp` 自报）→ DEFER→B7。这砍掉 B6 最重成本项。

---

## 第一组 — 安全核心（批次内先实现）

### A1. OPS#3 — `cluster backup` / `cluster restore`

**backup = 经 `RODB()` 的只读一致拷贝 `backupTo`——绝不 `Node.Snapshot()`、绝不携带 raft log。**（draft-4 的 `Node.Snapshot()`=`raft.Snapshot()` 会截断 raft log = 错；携带外来 raft.db 重引 force-single 要防的脑裂。）bundle = 自描述目录：`state.db`（FSM DB 拷贝、即 committed 状态：roster/alerts/ports/sessions/applied_index）+ `manifest.json`（identity/provenance，§A2）。raft log 是 node-local、restore 时重新 bootstrap。
- **online backup**（`cluster backup`，daemon RUNNING，cluster-mode，leader 或 follower）：新 adminsock op `OpClusterBackup` → handler 调新 `internal/cluster` seam `Node.BackupDBTo(ctx, dstPath)`（包 `backupTo(ctx, n.ro, dstPath)`，snapshot.go:78）。纯 RO 读 `RODB()`→ single-WAL-owner 持有、无 raft 写、无 --yes、任意节点可服务。
- **offline backup**（`cluster backup --offline`，daemon STOPPED）：**非集群字节等价路径**——单 broker 就是 SQLite 文件（`checkpointWALForBackup` 后）包进 `mode:"single-broker"` manifest。

**⚠ 关键缺陷修正（核验、4 draft 都错/含糊的 #1 风险）—— restore-index 腐蚀**：`BootstrapSingleNode`（offline.go:86-94）**要求空 store、不重放 log、不写 index-N 快照**——在 index 1 铺 raft config。FSM 的 `Apply` 跳过 `if l.Index <= applied`（fsm.go:161）。故 restore 一个真集群 bundle（`state.db` 带 `applied_index = N ≫ 0`）后 `BootstrapSingleNode` → 全新 raft log 从 index 1 → **FSM 静默吞掉每个 restore 后的写、直到 raft log 爬过 N**。集群 ACK 并持久丢写。

**修正后的 restore — `cluster restore <bundle>`，OFFLINE-only**，新 `internal/clusteroffline/restore.go` `RestoreFromBackup(opts)`：
1. flock + live-daemon interlock（双探：`RaftStoreLockedByDaemon` + `storage.ProbeWriterLock`），复用 Recover/ForceSingle ladder。
2. **Provenance 门**（§A1-sec）——任何磁盘改动前 FATAL。
3. `verifyIntegrity`（snapshot.go:212）bundle 的 `state.db`，在 **staging 临时拷贝**上。
4. `.bak` 当前 on-disk DB（`backupOnce`）。
5. forward-migrate **staging** 拷贝（`storage.Open`），**再 verifyIntegrity staging**（迁移后），**然后才 swap 就位**——最后才装到 live DB 上（修 draft-1 的"先装后验"顺序：迁移后损坏的 DB 会落到 live、只有 .bak 救你）。镜像 `restoreFrom` 已证顺序（snapshot.go:178/193/198）。
6. **把装好的 `state.db` 的 `applied_index`/`applied_term` 重置为 0**（裁定：reset-to-0，因 bundle 无 raft log 要同步；from-zero 重 bootstrap 覆盖 committed 状态正是 `seedClusterState`/`BootstrapSingleNode` 已证不变量；合成 index-N 快照更优雅但是净新未测 raft 手术）。**DB 的 committed 行不变、只重置 index 游标**，使 index 1 的新 raft log 不被吞。
7. wipe `raft/` 然后 `BootstrapSingleNode(DataDir, selfID, raftAddr)`。raft/ 是最后一步。
8. log "restore complete; single-voter cluster — re-grow with `cluster add`."

**强制测试（无 draft 有的 load-bearing 一个）**：restore 一个 `applied_index=5000` 的 bundle → 重启 daemon → 发权威写 → **断言写持久（未被吞）**。

**CLI**：`cluster backup [--offline] --out <dir>`（无 confirm——RO）。`cluster restore <bundle> --confirm-node-id <id>`——不可逆 + 影响身份 ⇒ Tier-2 typed-confirm、**无 --yes**、经 `confirmTypedNodeID` 且 **`allowMachineEscape=false`**（restore 在 never-escapable 集，§A4）。
**Wire**：additive `OpClusterBackup`（online）；`Request.BackupPath`（server-local 路径）；`Response.Backup`（`{path,bytes,applied_index,self_id}`）。单 broker 返 `cluster_not_enabled`。**不 bump proto、不 migration。**

**A1-sec restore provenance**（裁定：弃 `account_fp` 作主门）：`account_fp = sha256(cluster-ca.pem)` 是 manifest JSON 里 operator-可伪造的明文。**不可伪造锚 = live secrets dir 的 tunnel-cert 指纹**（无集群私 tunnel key 的攻击者造不出匹配的 secrets dir）。门（全 FATAL pre-mutation，同 `seedClusterState` init.go:197-210 的身份等式）：(1) **主（不可伪造）**：`TunnelCertFingerprint(localSecretsDir)`（init.go:233）== `manifest.self_cert_fp` == bundle `state.db` self-row `cert_fp`（三等）；(2) `--confirm-node-id <id>` == `manifest.self_id`；(3) DB-vs-manifest self-row 一致；(4) `account_fp` 仅留作纵深 advisory。→ 无 matching tunnel cert（私钥）+ matching self-id + self-consistent bundle 就不能冒用外来集群。

**A1-sec no-secret-leak**：bundle = `state.db`（存指纹/哈希 `cert_fp`/`pin_hash`，绝无 seed/key/PIN——那些在 secrets dir）+ `manifest.json`（指纹+拓扑）。**scrub 断言**：grep bundle 找 `.nk`/`-----BEGIN`/seed 形/`pin_hash` → 必无。bundle 不是凭据：restore **需要** operator 的 secrets dir。

**A1-tests**：backup→restore round-trip（offline+online，每表字节相同、applied_index 重置 0、raft 重 bootstrap）；**restore-index 持久性（强制）**；**backup-under-concurrent-write（强制，gated -race）**（follower online backup 同时 leader 高写 → offline restore → 断言无事务撕裂：无无父 session 的 port 行、applied_index 与行内一致——`verifyIntegrity` 过 FK-valid 撕裂 DB，故是逻辑一致性断言）；**foreign-cluster 拒绝**（3 个 FATAL，各在 .bak/install 前、on-disk 不动：tunnel-fp 不匹配 / confirm-id≠self_id / bundle-vs-manifest self-row 不符）；restore re-verify 顺序（迁移后损坏 staging 不装到 live）；撕裂/FK 违反 bundle → verifyIntegrity 拒；live-daemon interlock；kill-9 幂等；single-broker mode（offline backup 非集群 DB = SQLite 文件 + `mode:"single-broker"`；单 broker bundle restore 进集群路径被 mode-mismatch 拒）；**DR drill（gated，新 test/d9 或 b6_integration）**：backup 3-node → 全毁 → fresh box `cluster restore` → daemon N=1 → `cluster add` 重长 → agent re-pin、expose re-home。

### A2. OPS#11 — recover→manifest（identity-only）
**成本修正（核验、3 manifest draft 都低估）**：`Recover`（offline.go:215）**不读** self-identity 行（`dumpDivergent`→`wipe`，dumpDivergent 自开自闭 RO handle）；`RecoverOptions` 无 `SelfID`。故 emit 是**净新代码**：先读 `cluster_meta.self_node_id`，再投影 `InitFromExisting` 需的 8 个 identity 列。
**emit（`recover --emit-manifest <file>`）**：加 `RecoverOptions.{SelfID,ManifestPath}` + 新 `readSelfIdentity(dbPath,selfID)` 投影 `{name,node_ident_pub,raft_addr,nats_route,tunnel_addr,public_host,nats_server_id}` + `self_cert_fp` + `account_fp`；O_EXCL 0600 写、同 dumpDivergent 的 fsync-before-wipe。identity-only——divergent 业务行留在 forensic dump。
**consume（`cluster init --from-manifest <file>`）**：`InitFromExisting` 的薄替代入口——从 manifest 读 identity 字段（替 9 flag）。**`cert_fp` 从 local secrets dir LIVE 重导、绝不从 manifest 重放**（recover 节点可能合法呈现轮换后的 cert；manifest 的 `self_cert_fp` 仅 cross-check live 推导、不符则拒）。所有 F4 身份完整性 guard / live-daemon interlock / re-run conflict guard 不变。`DataDir/DBPath/SecretsDir` 留 local flag（绝不信文件里的路径）。
**共享 schema**（与 A1）：backup 的 manifest 与 recover manifest 共用 §A2 `Manifest` 类型，故同一 `init --from-manifest` 消费两者。
**Manifest schema**（`internal/clusteroffline/manifest.go`，versioned，identity-only）：`{schema_version,kind:backup|recover-divergent,created_at,tool_version,mode,self_id,self_cert_fp,account_fp,applied_index,name,node_ident_pub,raft_addr,nats_route,tunnel_addr,public_host,nats_server_id,roster[]}`。`roster[]`（public node_id/name/phase/raft_addr）**必须用与 OPS#12 incident bundle 同一 allowlist 投影 helper**（统一——未来列不能经任一路径泄漏）。

### A3. OPS#4（安全半）— `cluster add` version-skew reject
**裁定（task 点名的 false-positive 风险）：proto 版本不符是唯一硬拒；release 版本 skew 是 advisory（warn+allow）、绝不拒。**（滚动升级 followers-first，leader 暂时比 joiner 旧；回滚时重入的 drained 节点也可能比已升级 leader 旧——硬拒会砖死 OPS#4 要 enable 的滚动升级。）
- **CLI**：`cluster sign-join`（已印可粘 `cluster add` 行）加 `--release-version`/`--proto-ver`，从 *joiner* 的 `proto.ReleaseVersion`/`proto.ProtoVersion` 默认（joiner 二进制是"它将跑什么"的唯一源）；嵌进印出的 add 行。
- **`cluster add`**：`--joiner-release`/`--joiner-proto` flag + additive omitempty `Request.JoinerRelease`/`JoinerProto`。
- **门在 `AddNode`（clusteradmin.go:156），leader-local、phase-1 Propose 前 / `claimJoinNonce` 前**（被拒 joiner 不烧 nonce）：`JoinerProto != proto.ProtoVersion` → `Response{Code:"version_skew"}`（新 const）→ CLI exit 64。release skew → 不持久、**stderr warn**、allow。不可解析 `v0.0.0-dev`（源构建）→ allow+warn。更新 joiner → allow。
- **无持久列、无 migration**——门读呈现的 wire 字段。auth 不变（join-PoP ed25519 仍每 follower Apply 复验；version 是兼容门非 auth 门）。

### A4. AUTO#8 — machine-confirm escape（窄）
**裁定（核心安全发现）**：escape 必须是**逐 call-site 的 `allowMachineEscape bool` 参数**穿进 `confirmTypedNodeID`（默认 false），**不是**共享函数内的无条件分支（draft-2 在函数内查 escape 会泄漏到 force-single/recover/F==0-drain/init——违反硬约束 2）。
核验 5 call site：cluster_offline.go:62（force-single）、:111（recover）、cluster.go:416（F==0-drain）、:459（remove）、:606（init）。
- **NEVER-ESCAPABLE（传 `allowMachineEscape=false`，正确 env 也拒）**：force-single、recover、**F==0-drain**、init、**新 restore**（A1）。不可逆/毁 quorum；systemd unit / CI 里的 env 不是"attended"。
- **ESCAPABLE（传 `allowMachineEscape=true`）**：仅非 F==0 `cluster remove`（reversible typed-confirm 档）。
- **escape（允许时）**：需 **同时** `--confirm-node-id <id>` flag **且** env `TETHER_CONFIRM_NODE_ID` **精确等于** `want`。任一单独（散落 export 的 env / 散落 shell-history flag）→ 拒。错 id → 拒。缺 env（仅 flag）→ 回落既有 TTY 路径（交互或 HARD-REFUSE）。env 是 **node-id 值**（绝非布尔"yes-to-everything"）。**`rejectedUnattendedYes` 不变**——`--yes` 绝不是 escape。

---

## 第二组 — 增量（批次内后实现，同一 B6）

### B1. OPS#2 — alert webhook（leader 侧、transition-gated POST）
**修正——webhook 必须 post-COMMIT、基于 committed 真相，不基于 plan-closure 或 decision-list**：(1) **plan-closure 洞**：`planAlertSignal`（alert_forward.go:44）在 `node.Propose` plan closure 内（applyMu 下、retry 重跑），在那 fire 会 replan 双发——disk/manual seam 必须 **post-commit 在 `dispatchForward` 的 `VerbAlertSignal` case**、`node.Propose` 返 nil 后。(2) **swallowed-not-leader 洞**（核验 alert_reconcile.go:171-183：reconciler propose **吞 `IsNotLeader`**）：在"成功 propose 后" fire 会为一个 raft 写**从未 commit**（pass 中失 leadership）的 alert POST `raised`。**修**：webhook 基于**下一 pass 顶部观测的 committed `ActiveAlertKeys` delta**（committed 真相，alert_reconcile.go:100），非 `raises`/`clears` decision-list。这也免费给 idle-zero-POST + leadership 变更不双发。覆盖 reconciler-owned kind **和** disk_pressure/manual kind（committed-delta observer 看全 `alerts` ACTIVE 集的每个 kind 跃迁）。
**形状**（新 `internal/broker/alert_webhook.go`）：`webhookPoster` 持 bounded 共享 `*http.Client`（`Timeout:5s`）+ 单 drain goroutine 喂 **buffered channel**（cap ~64）；`Post` 非阻塞入队（`select{case ch<-ev:default:drop+Warn+counter}`）。hung/4xx endpoint 只卡那 goroutine 的下个 send、**绝不卡 reconcile pass 或 applyMu 持有的 propose**。有序 shutdown join 既有 `loopDone`（cap-3→cap-4）。`wireClusterLate` **仅在配了 URL 时** wire（否则 nil `OnTransition`）。**字节等价**：SINGLE mode 永不构造 reconciler；未配 URL → nil → cluster mode 也零新行为。（顺手修 alert_forward.go:64-68 的过期注释"Production never calls it"——post-D9 已 live。）
**body**（仅公开拓扑、B5-metrics 级、无密钥）：`{schema:"tether_alert_webhook",schema_version:1,transition:"raised"|"cleared",kind,severity,dedup_key,message,node?,cluster_leader,ts}`。
**SSRF/secret（裁定，务实 v1）**：收 **http 和 https**（拒 `file://`/`gopher://`/非 HTTP——常见部署是 `http://10.x:9093` 内网 alertmanager）；**拒 URL userinfo**（`url.User!=nil`）——load-bearing 守卫、无 secret-in-URL；**不**封私网 IP/metadata（operator 拥 URL，硬约束 2）；v1 无 auth header。wedge 防护（bounded client + per-POST timeout + drop-on-full）远比 scheme-policing 重要。
**B1-tests**（unit）：idle pass→0 POST；raise→恰 1 `raised`；clear→恰 1 `cleared`；二次 idle→仍 1；**leadership-flap→0 POST**；**hung endpoint(30s)→reconcile pass + Propose 在 poll 预算内返回、goroutine 数回基线**（内建 NumGoroutine leak 门）；4xx/5xx→reconciler 不受影响、drop counter++；queue-full→Post 立返、drop>0；`parseWebhookURL` 拒 userinfo+非 HTTP、收 `http://internal:9093`；follower→0 POST。

### B2. OPS#12 — `cluster export-incident`
**只读组装器**（新 `internal/broker/incident.go`，leader-local 读 `RODB()` + JS `history-<sid>`）over 三个已复制源。新 admin verb `OpExportIncident`（需 JS handle、非纯 offline）。CLI `tether cluster export-incident [--out bundle.json --since DUR --sid <sid>...]`。
- alert history：`alerts`+`alert_acks`（0009）全 raise/clear 时间线。
- membership 时间线：`cluster_nodes` roster + `added_at`/`phase_changed_at`/`voter_add_error`（真 append-only membership 事件流是 B7）。
- audit history：per-sid `history-<sid>` 经既有 `adminAuditTail`；`--sid` scope、默认 ACTIVE session。
**secret-scrub（allowlist 投影）**：绝不 `SELECT *`。**必 scrub `cluster_nodes` = `join_nonce`+`join_sig`**（0013 PoP 料）。**公开保留：`cert_fp`/`node_ident_pub`/`nats_server_id`**。开放形 audit `Body`（`map[string]any`）走 key-denylist scrub（`pin`/`token`/`seed`/`sig`/`nonce`/`secret`/`key` 子串）。**与 A2 manifest `roster[]` 共享 allowlist helper**。
**Wire**：additive `OpExportIncident` + `Request.Since/SIDs` + `Response.Incident`。无 migration、无 ACL 改。
**B2-tests**：secret-scrub 断言（seed `join_nonce`/`join_sig` + audit Body `{pin,token}` → bundle 无；有 `cert_fp`/`node_ident_pub`）；allowlist golden；completeness；`--since`/`--sid` 过滤；空集群/无 JS 优雅；leader-only。

### B3. OPS#4（增量半）+ B5 cheap folds
- **version-in-`cluster status`**：`ClusterHealthResp`（alerts.go:11）加 omitempty `ReleaseVersion`/`ProtoVer`（已带 AppliedIndex 经 scatter-gather）、`clusterHealthResponder` 从 `proto.ReleaseVersion`/`ProtoVersion` stamp；`reachOf` 拷进新 omitempty `ClusterNodeStatus.ReleaseVersion`/`ProtoVer`；渲 `VER` 列。**live 自报、无 migration、无 upgrade 后陈旧列**。
- **滚动升级 runbook §5**（`docs/cluster-runbook.md`）：followers-first/leader-last；每节点 `systemctl stop`→swap→`start`→`cluster wait <node> --phase VOTER`→`cluster status` 看新 `VER`；升 leader 前 `cluster transfer-leader <new> --wait` 交接；proto bump（v2→v3）是 flag-day 非滚动升级。
- **disk/ports DEGRADE band**：computeHealth 加 band（disk_free<10% 或 ports>90% → DEGRADED exit 1）；值已在线。**不得**覆盖 FORCE_SINGLE(3)/QUORUM_LOST(2)。
- **cluster streams_actual/target /metrics gauge**：`brokermetrics.Snapshot` 加 `StreamsActual`/`StreamsTarget` + 两 `g(...)` 行（ClusterMode 守卫后）；值从既有 `streamObserve`。
- **broker.yaml 键** for `--log-level`/`--log-json`/`--metrics-listen`（键缺→今天默认、字节等价）。

---

## 实现顺序

> 一个 B6 批次，下列 12 步连续实现 → 一次 Stage-C 内审 → 主进程采纳 → 暂存。**全程零中途外审**（外审统一在 B7 后）。安全核心（1–9）先于增量（10–12）。

**安全核心（先做）**：
1. `internal/clusteroffline/manifest.go` — 共享 `Manifest` + 共享 allowlist 投影 helper（A1+A2+B2 都消费）+ 单测。
2. `internal/cluster` 导出 `Node.BackupDBTo`（薄 `backupTo` 包装、最小 L-2 seam）。
3. `internal/clusteroffline/backup.go`（`OfflineBackup`+`writeBundle`）+ round-trip + scrub 测。
4. `internal/clusteroffline/restore.go`（`RestoreFromBackup`、**applied_index reset-to-0**、staging verify-before-swap、4 层 provenance）+ **restore-index 持久性测** + 拒绝/撕裂/interlock/re-verify-顺序测。
5. A4 machine-confirm：`allowMachineEscape bool` 参数 + 5 call-site bool + restore 经它（false）+ "correct-env-on-never-escapable-still-refuses" 守卫测。
6. A2 OPS#11 emit + consume + manifest round-trip 测。
7. A3 skew-reject（wire 字段 + sign-join/add flag + proto-only 门在 `claimJoinNonce` 前 + `version_skew` code + nonce-not-burned 测）。**无 migration。**
8. adminsock `OpClusterBackup` + broker adapter + `cmd/tether/cluster_backup.go`（online + --offline + restore typed-confirm）。
9. DR + manifest-replay runbook。Gated DR drill + backup-under-write -race 测。

**增量（后做，同一 B6）**：
10. B2 incident（只读、复用 allowlist helper、scrub 测）先——立无险基座。
11. B1 webhook（committed-delta seam、bounded worker、有序 shutdown join `loopDone`）；修过期 `AttachAlertSink` 注释。
12. B3 version-in-status + disk/ports band + streams gauge + broker.yaml 键 + 升级 runbook §5。
→ 12 步全完后 **Stage-C 内审 → 采纳 → 暂存**，B6 结束，直接进 B7。

---

## CONFIRMATIONS

- **single-WAL-owner 安全**：✔ backup 读 RO handle（`Node.RODB()`）经分页 online-backup（`backupTo`）——无第二 writer、FSM 继续经 WAL 写。**绝不 `Node.Snapshot()`**。restore daemon-STOPPED（无 live WAL owner）、verify-on-staging-before-swap、**重置 applied_index 为 0** 使 index 1 新 raft log 不被 FSM index-skip 吞（fsm.go:161 = 核验 #1 缺陷），再重 bootstrap raft。绝无两 writer、绝无外来 raft log。webhook/incident 只读/出站。新 restore 在 machine-confirm never-escapable 集。
- **非集群字节等价**：✔ 每个 B6 面默认 OFF/no-op。单 broker backup = checkpoint 后 SQLite 文件 + `mode:"single-broker"`（无 raft）。webhook 未配 URL → nil（且 SINGLE mode 永不构造 reconciler）。export-incident/skew-reject/status-VER cluster-mode-gated。machine-confirm additive（env 未设 → 今天精确行为）。disk band 仅 `cluster status`；streams gauge 在 ClusterMode 守卫后；broker.yaml 键缺 → 默认。
- **安全/无泄密**：✔ bundle = `state.db`（仅指纹/哈希）+ manifest（指纹+拓扑）；restore provenance 锚在**不可伪造的 live tunnel-cert fp**（非可伪造 account_fp）。incident bundle = allowlist 投影 scrub `join_nonce`/`join_sig` + audit-Body denylist；webhook body = 公开拓扑、URL 守卫 = no-userinfo（+拒非 HTTP）、无 auth secret。machine-confirm env 是 node_id（已 operator-可见）。**A2/B2 共享一个 allowlist helper。** 两 bundle 都有 scrub 断言测。
- **proto 纪律**：✔ `proto.ProtoVersion` 不 bump。**零新 migration**（持久 `join_release_version` 列无 reader → DEFER→B7）。全部 wire 加项 additive omitempty。version-skew 复用 SSOT `proto.ReleaseVersion`/`ProtoVersion`。
- **scope**：✔ B6 单批次一次实现完（backup/restore+manifest+skew-reject+machine-confirm 安全核心先做，webhook+incident+version-surface/runbook+B5 cheap folds 后做），零中途外审、外审统一 B7 后。狠砍：migration 0014（无 reader）、release-串硬拒（砖死滚动升级）、`--retire --wait`（糖）、cordon/agents_repinned/js_store_used_pct——全 DEFER→B7。OPS#11 identity-only（无业务行重放）。
