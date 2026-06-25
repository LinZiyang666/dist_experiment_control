# B7 plan — 文档原 7 条大件（DOC#1–#7 + AUTO#9），最后一批

> Stage A：9×Opus 对抗规划（4 drafter discovery-invite / ops-controller-apply / proxy-events / productization-wizard → 4 critic scope-feasibility / architecture / security / byte-equiv → 1 synth）。synth 全部 IN/DEFER 读码验证。主进程定稿采纳。
>
> **B 程序最后一批**，之后整个 B 暂存区进 ≥20 专家联合内审 + 外审，无 B8。原则：**只 ship 可收口、不自驱危险路径的**——凡"让 tether 自驱 reconnect / 破坏性 drain / 不可逆磁盘手术 / live-NATS 重启"的一律 **DEFER→post-v2**（各留干净 additive seam）。

## §1 VERDICT 表

| 项 | 裁定 | 一句话理由（grounded） |
|---|---|---|
| **DOC#6** `cluster status` 产品化（card + online doctor + incident glue） | **IN** | 纯只读重渲已算好的 `ClusterStatusReport`（B1 verdict + B5 disk/ports/cert/VER + B6 incident）；零 wire/migration、opt-in flag。最低险、最高验收价值 = 程序 capstone。 |
| **DOC#2** broker join/retire OPERATION controller（可恢复 ledger + `cluster ops ls/show`） | **IN** | 两阶段 membership 状态机已 ship（`AddNode`、phase raft-复制、`ReconcileMembershipOnLeadership` 调和）；B7 只加 per-op ledger（唯一 migration）+ 读视图。 |
| **DOC#5** expose/rehome 事件 + 告警 | **IN（trimmed）** | 载体全有（B4 `ExposeResp.HomeBroker/Epoch`、`sys.events`、D5 audit、D8 alert+webhook）。ship `expose_rehomed`（一个发射点）+ `migrate_exposes_stalled` 告警；**砍**原始 per-event flood + 可查历史 RPC。 |
| **DOC#3** signed broker roster + `agent doctor`（verifier core） | **IN（verifier-only）** | signed roster 由 `cluster_nodes` 派生（无 migration），用既有 **account seed** 锚签（`auth.LoadAccountSigner`），骑 `Home`/`Proxy` 指针-omitempty seam。ship account-signed roster 类型 + verifier leaf + `cluster.roster.req` responder + register-reply push + `agent doctor`。 |
| **AUTO#9** `cluster apply -f roster.yaml` | **IN（`--plan` only）** | 纯读+diff（对 `cluster_nodes`/raft config）渲染收敛计划为既有 verb——"到 N=3 还差啥"。零变更/wire/migration。 |
| **DOC#7** recovery/force-single GUIDED | **IN（diagnose + PLAN）** | 只读诊断（自动派生 dead-peer 列、TCP 探每个、渲出真值 `force-single`/`recover --emit-manifest`/`init --from-manifest` 行）over 不变的硬门（`ForceSingle`/`checkPeersDead`）。 |
| **DOC#1** topology auto-restart reconciler | **DEFER→post-v2** | nats.conf identity live 读（`own.AuthIdentity()`）、滚动重启刻意手动"leader LAST"；auto-restart loop 一个时序 bug 能 brick live mesh = 最高 split-brain 险；不能既 OFF-by-default 又有意义。自成 epic + 自外审。 |
| **DOC#4** proxy 集群化 | **DEFER→post-v2（+小 seam）** | proxy fencing（`proxy_epoch`/`proxy_meta.generation`）**零** raft op 覆盖；post-D9 那些 raw 写在 cluster 模式非法。最大新共识面（PSK keyset 撤销），D0–D9 明确切出，gated 于 P13 达无条件 PASS（仍 CONDITIONAL）。**In-B7 seam**：补 `handleProxyStatus` cluster-mode 缺口。 |
| AUTO#9 apply-EXECUTE | **DEFER** | 自驱破坏性 quorum-affecting drain = 自成可恢复状态机 + 自带 split-brain 面；叠在最后一批新 ledger 上 = "sprawling half-built"。 |
| DOC#3 `agent join` invite + agent URL-relearn | **DEFER** | reconnect/dial 是产品最脆路径（v0.3.4/v0.3.6 叶子）；改 agent URL 集 + 新 invite envelope 自成叶子。verifier core 现 ship；persist+reconnect-fold 后落在全测 verifier 上。 |
| DOC#7 auto-execute step-runner + TUI | **DEFER** | 调用不可逆磁盘手术的 wizard = 错/陈旧派生 peer 列经"信任调用者"的门喂 split-brain。ship PRINTER；门保持唯一变更路径。 |

**净：1 migration（`0014_cluster_ops`）、0 proto bump（ProtoVersion 仍 2，`proxy_test.go:15` 钉）、0 新 member-readable NATS subject。** 无新自驱码碰 reconnect / 破坏性 drain / 不可逆手术 / live-NATS 重启。

## §2 IN-B7 各项最小完整形

### DOC#6 — status 产品化
三层只读渲染/编排 over 既有 `ClusterStatusReport`（leader 侧算 `computeHealth`/verdict）：
- **(a) `--card` 渲染模式**（默认表不变）：headline（HEALTHY-HA/DEGRADED:`<top reason>`/READ-ONLY(quorum lost)/FORCE-SINGLE）+ voters `N (tolerates F)` + leader + 紧凑 per-node + "哪坏+怎么办"。**card 不算新 verdict**——重渲 leader 权威 `Health/Verdict`，reason 抽取是纯 CLI helper 镜像 `computeHealth` 的 `degraded=true` 触发。`--offline`/`--remote` 同 `--card`。
- **(b) online `cluster doctor`**（区别于既有 **offline** `clusteroffline.Doctor`，**不重命名/重载**）：`cluster doctor` 自检——admin socket 应答则跑 online 诊断（拉 `ClusterStatusReport`，每 health 条件转 PASS/ADVISORY/FATAL `DoctorCheck`，复用既有 `DoctorCheck`/`renderDoctor`）；否则回落 offline preflight。检查：leader 在、F≥1、每 voter caught-up、每 stream at target、无 INCONSISTENT、disk/ports/cert band、跨节点 version skew。
- **(c) incident export glue**：verdict DEGRADED/QUORUM_LOST/FORCE_SINGLE 时 footer 印准确 `tether cluster export-incident --since <window> --out incident-<date>.json`（真日期）。无新导出逻辑（复用 B6）。
**文件**：`cmd/tether/cluster_status_card.go`、`cmd/tether/cluster_doctor_online.go`、`--card` flag、doctor online-detect 分支。**broker 不动。** 无 wire/migration。opt-in、默认输出不变。只读。

### DOC#2 — ops controller
- **migration `0014_cluster_ops.sql`**：`cluster_ops(op_id PK, kind CHECK('add','drain','retire','rotate_cert'), target_node, state, started_at, updated_at, last_error, requested_by)`。justified：`cluster_nodes.phase` 是 per-node 单行（retire-then-re-add 复用行 → 无 per-op 历史）。骑 `genericExecApplier`（全字面 INSERT/UPDATE，如 alert_ops）——无自定义 applier。
- **ledger 写折进既有 phase-transition Command**（Critique-2 修正）：`setPhase` 是一个 `node.Propose`；ledger UPDATE 必须骑**同一 baked `Command.Body`**（genericExecApplier 每 Apply 执多句），**非**兄弟 `Propose`——否则跨 leader loss 两 raft entry 交错、ledger 与 phase SSOT 背离。**phase 列仍是安全 SSOT、ledger 是人读时间线。**
- **`OpClusterOps` adminsock verb**（只读、leader-agnostic 如 `OpClusterStatus`）→ `cluster ops ls`（表）+ `cluster ops show <op-id>`（时间线 + last_error + resume 提示）。`--json` 带 schema_version。
- **resume = (phase, raft-config) 的纯函数**：`ReconcileMembershipOnLeadership`（fork-safety = 纯由 committed phase + live raft config 驱动）同一 pass 额外把 op 标 `INTERRUPTED`（由它已调和的 phase 推算）。**无 auto-resume driver**——phase 守卫使手动重跑幂等。
**文件**：`0014_cluster_ops.sql`、`internal/cluster/ops_ledger.go`（Plan）、ledger 句折进 `clusteradmin.go`/`clusterdrain.go` phase Command、`internal/broker/clusterops.go`（读）、`adminsock/protocol.go`（+`OpClusterOps`/`ClusterOpEntry`）、`cmd/tether/cluster_ops.go`。无 proto bump。单 broker 空表 → `cluster ops` 返 cluster_not_enabled。

### DOC#5 — rehome 事件 + 告警（trimmed，生产无自治 failover rehome）
- **`expose_rehomed` 事件——唯一发射点**：生产唯一 `PlanReassignHome` 调用者是 `migrateExposes`（`clusterdrain.go:397`，leader 经 `a.node.Propose`）。成功 Propose loop 后发 `pubSysEvent("expose_rehomed",{sid,nid,port,name,from_broker,to_broker})` + `pubPortEvent(...,"rehomed")`。leader 侧单发。
- **`migrate_exposes_stalled` 告警**——第 4 个 `AlertReconciler`-owned kind（继承 transition-gated idle-zero-writes + committed-delta-webhook）。drain 找不到 eligible rehome target（`ErrNoMigrationTarget` 或 rebuild-OFF 拒）时 raise。
- **`expose explain`/`ps` 渲染**：从事件流尾展示 last rehome（`LAST-MOVED <ts> <from→to>`）。单 broker 无 `OpPortReassignHome` → 无事件 → 行省略 → 字节相同。
**砍**：per-reconnect `expose_unhomed`（flood + rate-limiter 是未建态）、可查时间范围历史 RPC。要 "currently un-homed" 用 `/metrics` bounded gauge 免费覆盖。
**文件**：`clusterdrain.go`（migrateExposes 成功 + ErrNoMigrationTarget 路径）、`alert_reconcile.go`（新 kind）、`expose.go`/`ps.go`（渲染）。无 proto bump（open-map type/kind 串）、无 migration（告警复用 0009）。

### DOC#3 — account-signed roster verifier core
- **新 wire 类型 `proto.ClusterRoster`**（additive）：`{schema_version, generation, account_pub, brokers[], issued_at, expires_at, sig}`；`RosterBroker{node_id, nats_route, public_host, tunnel_addr, cert_fp, phase}`。+ `NodeRegisterResp.Roster *ClusterRoster`（omitempty）+ `NodeRegisterReq.RosterGen uint64`（omitempty）。**无 ProtoVersion bump。**
- **用 ACCOUNT SEED 签，非 node-ident key（F1 BLOCKER 修）**：roster 用 `account.nk`（`auth.LoadAccountSigner` over `SA...` seed）签，验对 agent 钉的 `account_pub`（`A...`）。draft 的 "SignerPub ∈ VOTER rows + account-pin" 是**循环**（伪 roster 自证签者）——弃。account.nk 已在每 broker、无新信任。防跨集群来自攻击者没有的 account seed。
- **generation = membership-domain 单调，非 raft commit index（F3 修）**：raft CommitIndex 在 force-single/recover 后会回退、会让 agent 拒合法的 post-recovery roster。用 membership 域单调值：`max(phase_changed_at)`（0013 复制列、每 membership 变更前进），或 `cluster_meta` `roster_generation` 同 Apply UPSERT（无新表）。account 签使回滚可检。
- **新纯 leaf `internal/clusterroster/roster.go`**：`Build`/`CanonicalRosterBytes`/`Verify`/`Select`（import auth+proto+clusternodes；非 raft 非 nats）。`CanonicalRosterBytes` 镜像 `JoinSignBytes` NUL 分隔。
- **pull responder `cluster.roster.req`**（broker-only，既有 `cluster.>` grant 下，模仿 `cluster_health.go`，任意 broker 经 `RODB()` 答）。
- **register reply push**：构造点（`broker.go:1069` `resp.Home` 旁）加 `resp.Roster = b.rosterForRegister(...)`。**attach helper 首行必须 `if b.selfID=="" { return nil }`**（Critique-4——byte-equiv 来自构造点惰性，镜像 `homeForRegister`，**非**指针省 key、**非** `RosterGen`-only 比较）。register 在 cluster 模式 broadcast → **任意应答 broker 都 attach**（replica-identical）。agent 验（account-sig + account-pin + 单调 gen）、持久化 `roster.json`（0600）、更内存 broker 集。
- **`agent doctor`**（IN——廉价零 wire）：只读诊断镜像 `clusteroffline/doctor.go`。检查：identity 在/可读 + 指纹；roster cache 在、**签名验对钉的 account_pub**、未过期、generation；per-broker route TCP 可达 + best-effort NATS connect → unreachable/auth_denied/ok；config sanity。表 + `--json`（B2 taxonomy + exit code）。按需 pull+验 roster——也是 deferred `agent join` 的 seam。
**DEFER（DOC#3 狠裁）**：`agent join <invite>`（bootstrap envelope + mint + agent.yaml 写）**及** agent broker-set 重学 / `nats.Connect` URL-list 变更。碰最脆 reconnect 路径、自成叶子。seam：account-signed roster 对象 + `Verify`/`Select` transport/role-neutral，`agent doctor` 已按需 pull+验。**F2 不变量（即便 agent join defer 也要文档+测）**：invite（落地时）**仅 discovery、零 membership 权威**——节点成 voter 仍只经两阶段 join-PoP over 新 leader nonce；verifier leaf 绝不可当 join 凭证。
**文件**：`internal/clusterroster/roster.go`、`internal/broker/cluster_roster.go`、`internal/agent/roster.go`、`cmd/tether/agent_doctor.go`、`proto/messages.go`（2 字段）+ `proto/cluster_roster.go`。无 proto bump、无 migration。**两个守卫测**：marshal-omits-key + 构造点惰性（`rosterForRegister` selfID=="" 返 nil）。复用既有 account.nk 锚。

### AUTO#9 — `cluster apply -f roster.yaml --plan`（plan-only）
解析期望 roster（node id + raft_addr/nats_route/tunnel_addr/cert_fp + `desired: voter|absent`），diff 对 live `cluster_nodes` + raft config（经 `OpClusterStatus` 读），**印收敛计划为准确既有 verb**：哪些需 `cluster add`（"→ 先在 `<node>` `sign-join`"——admission 需 joiner PoP over leader nonce，YAML 行产不出该签名 → 无人值守 auto-add **code-forced DEFER**）、哪些 `drain --retire`、哪些 `takeover-natsconf` 重跑。differ **拓扑排序**（add/catch-up → transfer-leader-off-retiring-leader → retire 最后、拒 last-voter retire）。`--json` 带 schema_version。**纯读+渲、零变更/wire/migration。** = 头号可用性赢面。
**包名冲突修（Critique-4）**：DOC#3 与 AUTO#9 都想要 `internal/clusterroster`——不同概念：DOC#3 = agent 信任的 signed broker 集；AUTO#9 = operator 期望拓扑 YAML。**AUTO#9 用 `internal/clusterspec/`（desired-state differ）；DOC#3 留 `internal/clusterroster/`（signed wire roster）。**
**DEFER**：apply-EXECUTE。seam：`--plan` 已出拓扑序 verb 列；YAML schema 保 additive（`natscluster.Config` 超集）。
**文件**：`internal/clusterspec/spec.go`（YAML 解析+diff+topo-order 纯 leaf）、`cmd/tether/cluster_apply.go`（`--plan` 渲）。复用 `OpClusterStatus`。无新 adminsock verb、无 migration。

### DOC#7 — recovery/force-single guided（printer 非 runner）
guided 前端 over 不变硬门、**不加自有磁盘变更路径**：
- **`cluster force-single --guided`**（survivor 侧）：只读诊断——读 on-disk roster（`readRoster` RO）、**自动派生全 `--confirm-peers-dead` 列**（#1 手抄 footgun）、TCP 探每 peer raft/nats/tunnel 端口（同 `checkPeersDead` 将硬执行的探）、显 live/dead per peer、任何仍 alive 的 peer 作**阻塞** "CANNOT force-single" 在键入前。然后**印**准确 `force-single --self-id … --self-addr … --confirm-peers-dead <派生列>` 并**退出**（镜像 `init --check`）。
- **`cluster recover --guided`**（returning-node 侧）：印序 `recover --emit-manifest` → `init --from-manifest` → leader 侧 `cluster add`，真 id/addr/pub 读自 manifest。无 9-flag 手抄。
**DEFER**：auto-execute step-runner + TUI。wizard **绝不**对 force-single/recover 设 `allowMachineEscape=true`（never-escapable）。ship PRINTER；不变 `ForceSingle`/`confirmTypedNodeID` 门保持唯一变更路径。seam：diagnose helper（`DeriveDeadPeerList` + per-peer 探）作纯函数。
**文件**：`cmd/tether/cluster_offline_wizard.go`、offline 命令 `--guided`/`--plan` flag、可选 `clusteroffline.DeriveDeadPeerList`（复用 `readRoster`）。无 wire/migration。CLI-only、读 on-disk roster RO。

### DOC#4 seam（折进 DOC#6）
补无守卫 `handleProxyStatus`（`proxy.go:294`）：顶部加 `if b.clusterMode { b.proxyErr(msg,"proxy_unsupported",...); return }` 镜像 `handleProxySet:40`。**必经 `proxyErr` 回复（有 `msg.Reply`）、非静默返回**（否则 ctl 挂到 timeout）。**UX 修非安全修**（post-D9 `b.cfg.DB` 是 RODB、纯 SELECT，今天返空/陈旧、非非法写）。+ DOC#6 doctor/status 行："proxy subscription：cluster 模式不可用（单 broker 特性；追踪未来 proxy-HA 增量）"。无 proto/migration。单 broker 字节相同。

## §3 实现顺序（先父后子）
1. **DOC#6**（status card + online doctor + incident glue）+ **DOC#4 seam**。纯读/渲 over 已 ship `ClusterStatusReport`、零新态。**留**：`clusterDoctorOnline(rep)` helper + 共享只读诊断核（roster+health+per-peer liveness），DOC#7 诊断阶段复用。
2. **DOC#2**（ops ledger，migration 0014）。唯一 migration；建 per-op 时间线，AUTO#9 `--plan` 渲步骤引、DOC#7 hand-off 引。**留**：`cluster_ops` schema + 折进-phase-Command 写法。
3. **DOC#5**（rehome 事件 + stalled 告警）。建于 DOC#2 drain 路径 + 既有 AlertReconciler。**留**：`expose_rehomed` 事件 + deferred 未-homed gauge seam。
4. **DOC#3**（account-signed roster verifier + responder + register push + agent doctor）。独立于 1–3；排在 AUTO#9 前让未来 invite 凭证有处落。**留**：verified roster 对象 + `Verify`/`Select`。
5. **AUTO#9**（`cluster apply --plan`，`internal/clusterspec`）。在 DOC#3 + DOC#2 后。**留**：拓扑序 verb 列 + additive YAML schema。
6. **DOC#7**（recovery/force-single `--guided` printer）。最后；复用 DOC#6 诊断核。**留**：纯 diagnose helper。
→ 6 步全完 **Stage-C 内审 → 采纳 → 暂存**，B7 结束 → 整个 B 暂存区 ≥20 专家联合内审 → 定稿 → 停于外审。

## §4 CROSS-CUTTING 确认
- **single-WAL-owner**：无 IN 项加 raft 外权威写。DOC#2 ledger UPDATE 折进既有 phase-transition Command（一 Apply 一 entry、绝非兄弟 Propose）→ ledger 永不背离 phase SSOT。DOC#5 唯一写是 transition-gated 告警经 Propose。DOC#3/#6/#7/AUTO#9 零写。proxy seam 仅碰 RODB SELECT。
- **byte-equiv**：每新面默认 OFF/no-op，锚 `selfID==""`/`clusterMode==false`/opt-in flag——D6/D8/D9 惰性法，**绝非** generation/argument 锚。唯一 NATS-wire 字段（`NodeRegisterResp.Roster`）配 marshal-omits-key + 构造点惰性双测。migrated-but-empty `cluster_ops` 是公认 byte-equiv 标准（cf. 0009 alerts）。
- **security**：DOC#3 仅用既有 **account seed** 锚（`account.nk` 已在每 broker），验对钉的 `account_pub`——更简（F1）；无新 PKI/key/HTTPS。invite（deferred）仅 discovery、零 membership 权威，join-PoP 仍唯一 admission 锚（F2）。generation membership-域单调（F3，survives recover）。AUTO#9 auto-add code-forced DEFER（同 PoP）。破坏性 op 保 never-escapable typed-confirm + machine-confirm：`cluster apply` 仅 `--plan`；DOC#7 印后退出（无 allowMachineEscape）。
- **proto**：ProtoVersion 仍 2（测钉）。全 wire additive omitempty（`RosterGen`/`Roster`）+ open-map sys.events/ev.port 串。**唯一 1 migration `0014_cluster_ops`**（per-op 历史不能进 per-node phase 列）。无新 member-readable subject family。无 flag-day。
- **scope 狠裁**：DEFER DOC#1（auto-restart 能 brick mesh）、DOC#4（最大共识面、P13-gated）、AUTO#9 execute、DOC#3 agent join、DOC#7 step-runner/TUI。一致原则：*让 tether 自驱 reconnect / 破坏性 drain / 不可逆手术 / live-NATS 重启的一律 DEFER*。ship 的是 signed roster 作 **verifier**、apply/recovery 作**印计划的 planner**、ops controller + status + rehome 事件作真新态——可收口、能过 ≥20 专家审 + 外审，每 deferred 项留干净 additive seam。
