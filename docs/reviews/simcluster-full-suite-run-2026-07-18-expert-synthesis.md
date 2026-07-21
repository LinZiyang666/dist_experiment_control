# tether deploy-tier 全量 drill（37）综合缺陷清单

> 综合 6 lane 分析 + 6 份对抗核验的**去重、排序、可执行**结论。所有条目均已通过核验（upheld / downgraded-but-standing）；被驳回或收窄的主张见 §E。
> **严重度口径归一**：核验者之间对 harness 条目能否评 blocker 存在分歧（L2-1 因"harness 不可能造成部署损失"被降级，而 L1-2/L3-2 保留 blocker）。本报告统一为：**产品条目**的 blocker = 真实部署下数据/可用性损失或运维走不通；**harness 条目**的 blocker = 该 drill 对其目标面的净覆盖为零或发布判定被系统性伪造。按此口径 L2-1 应为 major（它只废掉 96 的一条 oracle，不废整条 drill），维持核验者结论。

---

## §0 发布判定

**不可放行。** 三条独立理由：

1. **存在确证的、未登记的发布级产品缺陷**：`cluster drain` 返回 rc=0「drain ok」、控制面显示 home 已迁移，而公网端口在新 home 上从不监听（P1）。这是"成功退出码 + 数据面静默丢失"这一最危险形态，且运维会据此继续 retire/关机。
2. **灾备面整条不可执行且从未被端到端证明过一次**：runbook §5.2 第 1 步就 permission denied（D3），第 3 步结构性 FATAL（P2/#51），恢复文案指向必然 crash-loop 的下一步（P4/#64），预检动词对"DB 根本不在那里"失明（P5/#50）。而本轮 DR 尾段（#52 nats.conf / #53 JetStream / H2 terminus）因 harness 自身 seam 写不全（H2）**完全没跑**。
3. **本次的 "25 GREEN" 这个数字不可作为放行依据**：至少 4 条 drill 的关键 oracle 是结构性空绿或恒失败（H1/H3/H5/H9），24 处覆盖缺口以 `warn` 绕过 `not_covered` 计数器（H4），静态闸 `lint-drills.sh` 的 BATCH 白名单不覆盖 21/37 个 drill 且对 drill 内嵌 jq 不做可编译性检查（H4/H12）。

**同时必须写明的阴性结论**（避免过度恐慌）：单 drill 的**断言质量本身很高**——抽查的 25 个 GREEN 无一空绿（61/70/72/73/90/91/92/97 等断言密度 28–49 条，普遍带 NON-VACUITY 正控与真实 sha256/sentinel 往返）；50/51/52 三条的落地 verdict 与 README 期望逐条相符、assert_fail=0、无 APPEARS-FIXED 分支触发，排除了"备份/灾备/凭据 family 有缺陷被悄悄修好或悄悄破坏"。**问题集中在元数据层与聚合层，不在断言层。**

---

## §A 产品缺陷

### P1 — `cluster drain` 返回成功，但被迁移的 expose 数据面永不跟随（控制面/数据面静默分裂）
- **类别**：产品缺陷 | **严重度**：blocker | **gotcha**：#29 **族内新面（NEW 子条目）**
- **暴露 drill**：71-expose-rehome-failover（B-migrate）
- **证据**：`logs/71-expose-rehome-failover.log:198` `71: Arm B cluster drain brk3 rc=0 out=[drain brk3 ok]`；:199 `PASS B-cmd … the drain-migrate path is reachable this run`；:200 `FAIL B-migrate [DRAIN-MIGRATE] wstrand migrates to a survivor voter + SERVES within 180s (want exit 0, got 1)`；:201 `poll_until: timed out after 180s`。**台账登记的三堵墙本轮全未命中**：:184 FIXTURE PASS（agt→brk3 隧道已建）、:197 Arm E `will NOT be auto-migrated` 拒绝签名 PASS（证 #31 未拦）、drain rc=0。
- **机理**：`internal/broker/clusterdrain.go:142` DrainNode→`:677` migrateExposes 只做一次 `port.PlanReassignHome` raft 写（改 `port_allocations.home_broker`），`:151-152` `if !retire { return nil }`——不停 nats-server、不断开 agent、不发任何通知。而 home 指令**唯一**投递通道是 register 回复：`internal/broker/home.go:121` homeForRegister（:119 注释自陈 "drives a rehome on the **next reconnect**"）→ `internal/proto/messages.go:142` → `internal/agent/agent.go:1170` applyHomeDirectives。agent 侧 `agent.go:1493-1499` 注释明写 "the agent registers ONCE and heartbeat is fire-and-forget"，只有 `nats.ReconnectHandler` 触发 re-register。**投递通道唯一性已核验**：全仓非测试代码中 `applyHomeDirectives` 仅 agent.go:1170 一处调用，`a.register(` 仅 agent.go:649 与 proxy.go:469(onNATSReconnect) 两处调用者。**rc=0 反证 home 确已改写**：migrateExposes 出错时 DrainNode 必 return err（ErrRebuildOffExposes / ErrNoMigrationTarget）。
- **运维后果**：执行 `tether cluster drain <brk>` 准备下线一台 broker → CLI exit 0 + `expose explain` 显示新 home → 运维继续 retire/关机 → 用户 expose 永久失联。这是 HA 面上**唯一那条"被支持的"计划内迁移路径**，实测不可用。
- **建议动作**：(a) migrateExposes 之后主动把受影响 agent 的 NATS 连接弹开（或新增一条 home-push 通道，复用 proxy_reconcile.go 已有的 push 形态）；(b) 在此之前，`cluster drain` 对含 rebuild-ON expose 的节点应**拒绝返回 0**或至少打印显式警告；(c) 台账在 #29 下开子条目（不要独立新编号）；(d) 回归时必须同时抓 `expose explain --json` 终态与旧 home 的 curl（见 H9）。

### P2 — `recovery restore` 无 `--config`，DR runbook §5.2 逐字执行结构性不可完成
- **类别**：产品缺陷 | **严重度**：blocker | **gotcha**：**#51（live-confirmed）**
- **暴露 drill**：51-full-dr（臂 G1）
- **证据**：`logs/51-full-dr.log:61` `PRODUCT-RED runbook §5.2 step 3 'start the daemon' on a fresh DR box [#51] reproduced for the documented reason (/data_dir is unset|refusing to silently downgrade a cluster DB to single mode/)`；前置 :33 `D-fresh-gate3 broker.yaml has NO cluster seam`。
- **机理**：`cmd/tether/cluster_backup.go` 的 `newClusterRestoreCmd` flag 集实测仅 `--data-dir/--db/--secrets-dir/--confirm-node-id/--raft-addr`，**无 `--config`** → 结构上不可能写 seam；兄弟路径 `cmd/tether/cluster.go:799` → `applyClusterSeam`（定义 :880，5 字段模板 :905-906）自动写全。fresh box 上 `scripts/install.sh:548-556` 把 `cluster:` 整段注释掉 ⇒ 无 seam ⇒ 启动 FATAL。
- **运维后果**：全灭 DR 走到第 3 步必卡死；错误串只说 `data_dir is unset`，不给完整 seam 形状。正确的 5 字段形状只存在于**产品源码**里；runbook 唯一提 seam 的 `docs/cluster-runbook.md:493` 在**另一节**且只列 3 字段、漏 `nats_conf_path`。MTTR 从"照单执行"退化为"读源码逆向"。
- **建议动作**：`recovery restore` 加 `--config`，落地即调现成的 `applyClusterSeam` —— 一次性关掉 #51 且与 init 路径复用同一实现。

### P3 — 「broker 主机不跑同机 agent」的部署上 `cluster upgrade` 结构性不可完成，且 HALT 后保留 `cluster_upgrade_active` 锁 → join/retire 被阻塞
- **类别**：产品缺陷 | **严重度**：major | **gotcha**：**NEW**（与 #31 相邻但不同：#31 卡在 acquire-lock 之前，本条卡在其后的 agent leg）
- **暴露 drill**：30-rolling-upgrade（机理链可由源码单独证成，不依赖本次日志）
- **证据（环境前提）**：`logs/30-rolling-upgrade.log:56` `up: 3 broker(s), 0 agent(s), 1 ctl`；:191 drill 自认 `(a) colocated-agent whole-host leg (OQ-6 — sim brokers run no colocated agent)`。`cmd/tether/serve.go` 的 `--colocated-agent-nid` 默认空、**确为可选**，故 0-agent 是合法生产形态。
- **机理（核验为逐符号闭合）**：`internal/clusterupgrade/plan.go` `AgentVer string // "" = unknown/unpaired → treated as not-at-target` + `AtTarget = target!="" && BrokerVer==target && AgentVer==target` ⇒ AgentVer 恒 "" ⇒ AtTarget 恒 false → `cmd/tether/cluster_upgrade_drive.go` 的 `agentVersionOf` 返回 ok=false → 无条件发 `reexec-agent` → `internal/broker/cluster_upgrade_trigger.go:122-125` `if agentNID == "" { agentNID = b.selfID }` → `Code:"agent_no_responders"` → `haltUpgrade`。锁：`releaseUpgradeLock` **只在干净完成时**调用（drive 顶部注释明写 "a HALT/cancel deliberately LEAVES it held"）；唯一自愈路径 `cmd/tether/cluster_upgrade.go:104-127` 在 `plan.Upgrades()==0` 内，因 AgentVer 恒 "" 而**不可达**。执行面：`upgradeActive` 被 `cluster_operation_controller.go:43/:176/:333` 与 `cluster_grow_trigger.go:73` 用于拒绝成员变更；`grep MetaKeyUpgradeActive` 全仓无独立清锁 CLI 动词。
- **运维后果**：集群停在混合版本的部分升级态；`cluster add`/`cluster retire` 全被拒；重跑 upgrade 重复同一 HALT。**订正 L1-4 的过强表述**：并非绝对无解——在 broker 主机上以正确 SID 起一个 agent 再重跑即可走完并释放锁；缺陷在于这条逃生路径**未文档化，且与"colocated agent 可选"的设计承诺冲突**。`docs/cluster.md:292` 只覆盖了"同机 agent 在**别的 session**"这一变体。
- **零成本加固推论**（核验者补充）：`renderUpgradePlan` 打印的 `(%d host(s) to upgrade)` 是纯文本、不受 P8 的 JSON tag 问题影响；因 AtTarget 要求双到版，**即便三台 broker 全部 re-exec 成功，事后 dry-run 仍必然显示 `3 host(s) to upgrade`** —— 这直接证明"升级永远无法收敛到 `Upgrades()==0`，故 stale-lock 自愈永远不可达"，无需重跑。
- **建议动作**：(a) `AtTarget` 对"该主机无 colocated agent"应短路为 broker-only 判据，而非当作 not-at-target；(b) 新增 `cluster upgrade --abort` / stale-lock 清除动词；(c) 文档补 agentless 部署下的 upgrade 语义。

### P4 — `recovery restore` 剪到单 voter 却不去集群化 nats.conf，完成文案也从不提 → 照它自己印的 NEXT 做必 crash-loop
- **类别**：产品缺陷 | **严重度**：major | **gotcha**：**#64（live-confirmed）**
- **暴露 drill**：50-backup-restore（臂 K）
- **证据**：`logs/50-backup-restore.log:66` `PRODUCT-RED #64 'recovery restore' prunes the roster to a LONE VOTER but leaves nats.conf's cluster{} block in place and never mentions it … Measured: ~4 crash-loops`；:67 诚实记录恢复机理 `the broker recovered because the surviving peer's nats-server let the clustered JS meta re-form, NOT because anything de-clustered the conf`。
- **机理**：`cmd/tether/cluster_backup.go:115-119` 完成文案仅 `NEXT: start tether-broker, then 'cluster join approve' to re-grow to N>=3.`；`internal/clusteroffline/restore.go:159` 剪 roster 到 self、:317 把 applied_index/applied_term/audit_published_index 归零；nats.conf 不在 restore 写入面内（与 #51 同根）。产品在崩溃时刻自己会印出缺的那一步（`internal/broker/clusterstatus.go:354` 的 de-cluster remedy）——**它完全知道该说什么，只是说晚了**。
- **运维后果**：lib 卷丢失后的单节点恢复（比全灭常见得多）照文档做必进 crash-loop；drill 里 ~73s 自愈只因幸存 peer 存在，**真实全灭 DR 无此 peer，届时是永久 crash-loop**。
- **建议动作**：restore 完成文案补两句（lone-voter+clustered conf 需 `reconcile nats --to-standalone --confirm-single`；fresh box 需 `reconcile nats --manual` 渲 auth_callout），复用 clusterstatus.go:354 的现成文案。

### P5 — `cluster doctor --offline --db <不存在路径>` 报 0 fatal 并 exit 0
- **类别**：产品缺陷 | **严重度**：major | **gotcha**：**#50（live-confirmed）**
- **暴露 drill**：50-backup-restore（臂 R3）
- **证据**：`logs/50-backup-restore.log:23`；控制源 :22 `R2 doctor --offline --conf <nonexistent> FAILS` 同轮 PASS，排除"doctor 整体失效"。drill 用 R-EXHAUST 四态（`drills/50-backup-restore.sh:177-187` 含 APPEARS-FIXED / 两个 UNJUDGEABLE 分支），非空绿。
- **机理**：`internal/clusteroffline/doctor.go:81-87` `if db, err := storage.OpenReadOnly("file:"+opts.DBPath); err != nil {…Fatal} else {…Pass, "tether.db opens read-only (migration source reachable)"}`；`internal/storage/storage.go:105-111` OpenReadOnly 只 `sql.Open` 后返回，**从不 Ping**，而 `database/sql` 的 Open 是惰性的。
- **运维后果**：doctor 是不可逆 restore 前的唯一预检动词；运维拿到 0 fatal 绿灯后执行 restore，而该绿灯对"DB 根本不在那里"完全失明。
- **建议动作**：加一行 `db.Ping()`。

### P6 — account.nk 轮换后无任何动词能看见 issuer skew：`reconcile nats --all --wait` 报 false all-clear，`doctor --offline` 报 0 fatal
- **类别**：产品缺陷 | **严重度**：major（**核验者建议重估至接近 blocker**，见下）| **gotcha**：**#54（两 facet 均 live-confirmed）**
- **暴露 drill**：52-credential-rotation（臂 B0/B2/B3）
- **证据**：`logs/52-credential-rotation.log:42`（facet 1，含旧 issuer `AAPQJFXRXLVOBXXZM5SUZQVP5MNJBLBU3GJ5CUBEXFA63OHEA6MJKRTA` + "byte-identical to the pre-rotation baseline"）与 :43（facet 2，doctor 0 fatal）。控制源 :39（B0）证明 reconciler 活着，排除"reconciler 没跑"。
- **机理**：`cmd/tether/cluster_reconcile.go:78-79` 自陈 `It NEVER bumps a generation … this just polls the authoritative status`；`cmd/tether/serve.go:203-218` `loadAuthCalloutSeeds` 只在 serve 启动路径执行一次。唯一能打印该 skew 的 `cluster_secrets.go:46-47` 的调用方只有 `cluster.go:815`(init) 与 `cluster_rotation.go:68`(guide)，`internal/clusteroffline/doctor.go:70-88` 的 check 列表**不含它**。
- **严重度重估依据（L3 核验者 missed 项，本报告采纳）**：`docs/cluster-runbook.md:243-248` §2.1 逐字把 reconcile 指定为轮换的 re-render 动词 —— `Re-render nats.conf across the cluster (\`tether cluster reconcile nats --all --wait\`), then rolling-restart NATS + the broker`。这使 #54 从"一个动词报错绿灯"升格为**文档化的凭据轮换流程整条走不通**；旧 account.nk 一删，下次 broker 重启即全集群认证失败且无回滚材料。
- **建议动作**：(a) doctor 的 check 列表接上已存在的 `readClusterPublicIdentities`（关 facet 2，成本极低）；(b) reconcile 在检出 issuer skew 时必须非零退出，不得打印 converged；(c) 同步修订 runbook §2.1。

### P7 — PIN CONNECT 无 per-IP 限速，architecture §E.6 承诺未实现
- **类别**：产品缺陷（安全面）| **严重度**：major | **gotcha**：**#25（本轮再次实证）**
- **暴露 drill**：80-session-isolation（R 臂三点判别）
- **证据**：`logs/80-session-isolation.log:39/:40/:41`——`R-fails: all 10 same-IP WRONG-PIN attempts REFUSED (reached the Argon2-failure = countable path)` → `R-pinfailed: ≥10 pin_failed sys.events captured (not pre-blocked)` → `R-11th: … the 11th same-IP CORRECT-PIN join STILL SUCCEEDS — PIN rate-limit ABSENT`。三臂正控排除了"被前置拦截所以限速器没触发"。
- **机理**：`grep -rniE 'rate.?limit|per-ip|token.?bucket|throttl' internal/authcallout/*.go`（排除 _test）**零命中**；与失败尝试相关的只有 `handler.go:297`/`:348` 两处 `h.emit("pin_failed")`，无 counter / 滑窗 / per-IP map。承诺见 `docs/architecture.md:825` §E.6 与 :754 E.2 流。
- **运维后果**：公网 broker 上任一源 IP 可对 session PIN 无限爆破，唯一成本是服务端 Argon2 计算（同时是廉价 CPU DoS 面）。与 DOC-6（evict≠revocation：evict 只删 provisioning 行、不封 nkey）叠加时风险升高——重入门槛就是这把无限速的 PIN。
- **当前呈现问题**：因 H4，本缺陷在汇总里显示为 **GREEN**（`product_red=0`）。
- **建议动作**：实现 §E.6 的 per-IP 限速（`client_info.host` 已可得、只是从不读）；在此之前至少改用 `product_red` 记账使其进入发布判定。

### P8 — `node ls --brokers --json` 输出 PascalCase 裸字段名，违反全仓 snake_case + schema/schema_version 约定
- **类别**：产品缺陷 | **严重度**：minor（但**是 H1 的产品侧诱因**）| **gotcha**：**NEW**
- **暴露 drill**：30-rolling-upgrade（消费方）
- **机理**：`cmd/tether/node_versions.go:25-33` `brokerVersionRow` 的 7 个字段 `NodeID/AgentNID/Assumed/BrokerVer/AgentVer/Skew/WholeHostAt` **全无 json tag**；:93-98 `emitJSON` 包装为 `{schema:"node_ls_brokers", schema_version:1, brokers:[…]}`；`cmd/tether/jsonout.go:29` 是裸 `json.MarshalIndent`，键名即 Go 字段名。对照：非 `--brokers` 分支用 `internal/proto/messages.go:379-386` 的 `NodeListEntry`（全带 tag），`cmd/tether/` 下其余 12 处 emitJSON payload 均为带 tag 的 snake_case。
- **运维后果**：`docs/cluster.md:290` 与 `test/simcluster/README.md:294` 都把 `node ls --brokers` 宣传为 G5 #19 whole-host 判据的单一可信来源，而它是全 CLI 唯一键名风格断裂的 JSON 面。**空值静默是最坏失败模式**：jq 不报错、退出 0、返回空串 → oracle 恒真或恒假（本轮就发生了，见 H1）。
- **建议动作**：补 tag（`node_id/agent_nid/assumed/broker_ver/agent_ver/skew/whole_host_at`）+ schema_version bump。

### P9 — install.sh 用未加引号 heredoc 写 nats-server.service，正文反引号 `` `cluster add` `` 被以 root 做命令替换
- **类别**：产品缺陷 | **严重度**：major | **gotcha**：**NEW**
- **暴露 drill**：30-rolling-upgrade（:11/:27/:43，每台 broker 各一次）+ 同签名见 40/41/71/73/74（仅 6 个未抑制 provisioning 输出的 drill 可见 ⇒ 非环境噪声）
- **证据**：`/opt/sim/install.sh: 1: cluster: not found`。**已脱离 Docker 在本机 dash 最小复现**：`cat <<EOF` + 正文含反引号 → 输出同签名，且正文中 `cluster add` 二词**消失**，rc 仍为 0。
- **机理**：`scripts/install.sh:707` `cat > "$sysd/nats-server.service" <<EOF`（未加引号 ⇒ 参数扩展**和命令替换**），正文 `:717` 含 ``# G4 §B / #23: Restart=always (not on-failure). The `cluster add` grow cutover …``。已核验全文件仅此一处反引号落在 heredoc 内。
- **运维后果**：① 每台 broker 安装打印一行伪错误（运维手册无此输出、会淹没真错误）；② 写出的 unit 注释被替换成空，G4 §B/#23 的 Restart=always 依据在实机上读不到；③ **结构性注入面**：install.sh 以 root 跑，若目标机装了提供 `cluster` 可执行文件的包，安装时会以 root 真跑 `cluster add`。
- **未泛化的第二面（核验者补充，应并入同一 gotcha）**：全文 11 处 heredoc 中**只有一处**用了加引号的 `<<'EOF'`（:429），其余（:346 agent.yaml / :526 broker.yaml / :570 Caddyfile / :690 nats.conf / :707 / :730 / :764 …）**全部未加引号**，正文一律做参数扩展。无任何断言保证 nats.conf / Caddyfile 模板里的 `$…` 都是**有意**展开的。
- **建议动作**：把所有不需要展开的 heredoc 改为 `<<'EOF'`；逐个审计需要展开的那几处的 `$` 与反引号；`32-install-lifecycle` 补一条产物**内容**级断言（现有 zero-write manifest 是自比对，对本类缺陷结构性失明）。

### P10 — 非-leader home broker 崩溃后 tier-B transfer 的 orphan object 永不回收
- **类别**：产品缺陷 | **严重度**：major | **gotcha**：**#58（live-confirmed）**
- **暴露 drill**：96-mid-flight-chaos（transfer 臂）
- **证据**：`logs/96-mid-flight-chaos.log:25` PRODUCT-RED #58，含完整机理文本与实测 `OBJ_xfer` 计数停在 2（干净基线为 1）。
- **机理**：brk2 非-leader ⇒ `reaperMayDelete()==false`（`internal/broker/transfer_reconcile.go:34-36` / `clusterwrite.go:478-486`）；boot reconciler 仅跑一次（`internal/broker/broker.go:942`）。
- **运维后果**：orphan object 累积顶满 `cmd/tether/transfer.go:67` 的 8 GiB/session 桶——与 racknerd 现网事故（tier-B 传输坏、8 GiB OBJ_xfer 顶满小盘）同型。
- **建议动作**：reaper 应在 leadership 变更时重跑，或允许非-leader 上报由 leader 代删。

### P11 — `rotate-tunnel-cert` 的 follower 侧 leader-redirect 对 self-only 动词给出误导指引
- **类别**：产品缺陷 | **严重度**：minor | **gotcha**：**#56（结论须收窄）**
- **暴露 drill**：52-credential-rotation（臂 A4）
- **⚠ 收窄**：核验反证了"死循环/永远到不了可执行状态"这一措辞。`internal/broker/clusterdrain.go` 的 `RotateTunnelCert` 在同一处**完整表述了真实要求并给出可执行出路**：`rotate must run on the target broker while it is leader; transfer leadership to %s first so it can hot-swap its live tunnel certificate`。照它做一次即可脱困。
- **实际缺陷**：follower 侧走的是 `internal/broker/clusterstatus.go:649-657` 的**通用** mutating-verb leader-redirect（`not leader; re-run on the leader host: brk1`），对这个 self-only 动词给出错误指引（去 brk1 对 brk2 做 rotate 仍会失败）。
- **建议动作**：为 self-only 动词旁路通用 leader-redirect；**同时订正台账 #56 与 drill:14 的 product_red 文案中的"死循环"措辞**，否则会把 UX 瑕疵夸大成流程死锁。

### P12 — tunnel-cert pin-mismatch 砖化态下，产品建议的补救命令结构上不可达
- **类别**：产品缺陷 | **严重度**：minor | **gotcha**：**DOC-23（live-confirmed）**
- **暴露 drill**：52-credential-rotation（臂 A8）
- **证据**：`logs/52-credential-rotation.log:30`，捕获 `error: admin socket /var/run/tether/admin.sock: … no such file or directory`；前置 :28/:29 已证 broker 确实 fail-closed 且 broker.err 带精确 pin-mismatch 串（排除"unit failed 当绿"）。
- **机理**：`wireClusterEarly` 在 `internal/broker/broker.go:691` 返错即退，admin socket 到 `:1060` 才创建 ⇒ 该态下任何走 admin socket 的 cluster 子命令必 dial 失败。唯一出路是手工恢复旧 cert 文件（A8f 证实有效），产品从不提。
- **建议动作**：pin-mismatch 错误文案改为"还原旧 cert 文件"。

---

## §B 待定 / 开放问题（需定向复跑才能定案，**不得**当作已定结论写入台账）

### Q1 — 恢复 gen-1 account.nk + 重启双 broker 后 120s 内认证仍未恢复，drill 直接归因 #54「unrecoverable」但**零诊断证据**
- **类别**：open-question（若 (A) 成立则为 blocker 级产品缺陷）| **gotcha**：#54 下游
- **暴露 drill**：52-credential-rotation（D-group rebuild gate，`drills/52-credential-rotation.sh:452-465`）
- **证据**：`logs/52-credential-rotation.log:45` 只有 `poll_until: timed out after 120s`；:46 not_covered 文案直接写「a further consequence of #54: no atomic rotation, no in-place recovery」。**全日志无任何 broker.err / journal / doctor 输出**。
- **已排除**：`test/simcluster/lib/secrets.sh:100-103` 拒绝重铸 gen1、gen2 落独立目录，恢复循环拷的确是 gen1 → 材料正确。
- **三个互斥假说**：**(A)** account.nk 一旦换过就无法就地换回（若成立，严重度远超 #54 = 凭据轮换单向砖化）；**(B)** 纯计时——恢复循环对 brk1/brk2 近乎同时 `systemctl restart nats-server tether-broker`（N=2 raft 全停）且全 `|| true` 吞失败，健康门同时要 ctl 认证 + JS meta size 2 + admin socket，120s 可能不够；**(C)**（核验者补充，同样自洽且更可怕）C3 in-broker 拓扑 reconciler 以 User=tether 原子重写 nats.conf，若它在 gen2 落盘后、gen1 恢复前重渲过 conf，则恢复后得到"conf issuer=gen2 vs 磁盘 seed=gen1"的**反向 skew**。
- **建议动作**：定向复跑，**必须同时采 nats.conf 的 issuer 与 broker.err**（仅采 broker.err 无法区分 A/C）。在此之前，台账不得保留"unrecoverable"这条未测量的产品指控。

### Q2 — 被分区的少数派 broker 对客户端表现为连接黑洞，而非设计中的 fail-closed 拒绝
- **类别**：product-defect（机理未归因）| **严重度**：major | **gotcha**：**NEW**
- **暴露 drill**：96-mid-flight-chaos（D1d/D4a/D4b/D4c）
- **证据**：D1d `PASS SELECTIVE CONTROL: ctl->brk1:4222 STILL CONNECTS`（4222 刻意不封）、D4a brk1 的 nats-server 仍应答 /varz、D4c MainPID/NRestarts 未变；但约 5 分钟后 D4b 同路径得 `cannot reach broker at nats://brk1:4222: read tcp …: i/o timeout (verify the broker is running and --nats-url is correct)`。⇒ TCP 通、服务端对 CONNECT 握手**零应答**。
- **设计意图侧已正向核实**：`internal/authcallout/handler.go:102-104` `fenced()` → :264/:328 `ErrFenced` → :167/:175 `h.deny(...)`，**设计上少数派确应明确拒绝**（NATS 端表现为 Authorization Violation，`cmd/tether/error_hints.go:145-153` 有专门文案）。实测走的却是 :155 的 unreachable 分支。
- **候选机理（均未证）**：① auth_callout 是跨 broker queue group（`internal/broker/authcallout.go:99`），静默 DROP 的 route 上 nats-server 仍认为远端 queue 成员存活 → 投递黑洞；② **（核验者补充，最直接）** `authcallout.go:101-104` 的回调在 `h.Handle` 出错时**直接 return 而不 Respond**——若无 quorum 时 Handle 阻塞在 raft 写 seam，客户端只能等超时，且会在 broker journal 留下 `authcallout: handle failed` 警告（可验证预测）；③ brk1 的 tether-broker 进程卡死（D4a 只探了 nats-server 的 8223/varz，**无任何 tether-broker 存活性证据**）。
- **运维后果**：机房间静默丢包时，少数派 broker 对客户端表现为"超时连不上"，且错误提示写着 `verify the broker is running and --nats-url is correct`——而 broker 在跑、URL 也对，运维会去查网络/配置而不是查 quorum。"少数派只读"这一架构声明在真实部署下无法被观察到。
- **建议动作**：定向复跑同时抓 brk1 broker journal（是否有 `authcallout: handle failed`）+ tether-broker 进程存活探针 + nats-server 路由状态快照。

### Q3 — `exec` 报成功、进程表 30s 内从未把该进程记为 RUNNING
- **类别**：open-question（候选产品缺陷）| **gotcha**：**NEW**
- **暴露 drill**：96-mid-flight-chaos（F0b/F0c）
- **证据**：`PASS F0b seed one on agt2 — the CONTROL that must survive`（`exec agt2 -- nohup sleep 9663 &` **返回 0**）紧接 `FAIL F0c … poll_until: timed out after 30s waiting for: agt2's seed is running pre-injection`。谓词 `_f4_agt2_running` 查 `ps -a --json` 里 agt2 上 argv 匹配 `sleep 9663` 且 status==RUNNING。
- **为何值得追**：F 臂前置门 `_f_precond_healthy`（3 VOTER + agt1/agt2 ONLINE，240s）**通过**，"arm-D 残留"这一现成借口已被 drill 自己部分排除。L2 lane 把整段读成"未建立 fixture 的级联"就收工，但"exec 成功而 proc 表不收录"本身是候选产品缺陷（进程登记路径 vs. arm-D 后 agt2 的 home broker brk1 状态）。
- **建议动作**：定向复跑同时抓 `ps -a` 全量 + agt2 journal + brk1 的 exec 登记路径。

### Q4 — `session create` 写已提交却向调用方报失败，且无幂等出口
- **类别**：product-defect（机理三候选未定）| **严重度**：minor | **gotcha**：**NEW**
- **暴露 drill**：96-mid-flight-chaos（D3/D6）
- **可观测事实（成立）**：D6 反证 canary2 已在多数派提交并可从 brk1 读回，而 D3 的 `session create canary2` 无一次返回 0 ⇒ "写落地、客户端未拿到成功"在真实栈上被观测到。
- **⚠ 机理未证**：`_d3_survivor_write` 以 `>/dev/null 2>&1` 丢弃 stdout+stderr 也不 log rc，**本 drill 没有任何关于 CLI 报了什么错的证据**。三候选：① `internal/broker/clusterwrite.go:786-799` `readCommittedSession` 轮询 50×20ms=1s 后返回 `not visible after commit (apply lag)`；② **（更强、L2 lane 完全忽略）** `cmd/tether/session.go:49-50` `context.WithTimeout(…, 5*time.Second)` → :55 `unavailErr("session create: %w (broker unreachable on NATS)")` → rc=69，此路径下 broker 从未"报失败"，是客户端自行放弃（同 run D4b 观测到的正是 rc=69，佐证该候选）；③ harness 自带的 `timeout 15`。
- **产品侧可独立核验成立的部分**：create 非幂等——`internal/broker/clusterwrite.go:521-534` 里 **sid == name**，`session/plan.go:32` 对已存在 sid 直接 `ErrAlreadyExists`，`broker/sessions.go:56-58` 映射为 `already_exists`；无 `--idempotent` 出口；且 `cmd/tether/session.go:64-66` 注释自称 `session_already_exists` 会命中 hint 表，实际 broker 回的是 `already_exists`，`error_hints.go` 里 grep 不到任何 already_exists 条目 ⇒ **连提示都没有**。
- **建议动作**：一次保留 stderr+rc 的定向复跑定案机理；无论机理如何，`session create` 应提供幂等语义或至少补 `already_exists` 的 hint。

---

## §C harness 保真度债（按对发布判定的破坏力排序）

### H1 — drill 30 的版本 oracle `_ver_of` 查了不存在的 JSON schema → 恒返回空串；本次 ASSERT-FAIL 是 oracle bug，而同一 oracle 支撑的 dry-run 断言是**永久空绿**
- **类别**：harness | **严重度**：blocker（该 drill 对 G5 升级机制的净覆盖为零）| **gotcha**：NEW
- **证据**：`drills/30-rolling-upgrade.sh:49` `_ver_of() { CTL node ls --brokers --json | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.release // empty'; }`，而产品输出为 `.brokers[].NodeID/.BrokerVer`（见 P8）⇒ `.nodes` / `.nid` / `.release` 三者皆不存在。失败断言 `logs/30-rolling-upgrade.log:187`+`:188`。
- **双重后果**：(a) `_all_on_next`(:51) 恒 false，无论产品升没升成，断言**永不可能通过**；(b) `_dryrun_no_touch`(:52) 是 `[ "$(_ver_of b)" = NEXTVER ] && return 1`，空串永不等于 NEXTVER ⇒ 恒 return 0 ⇒ 日志 :185 那条"dry-run 没碰任何主机"的 PASS 是**永久空绿**，且在 MECH=0 的历次运行里同样每次都跑 —— **是长期空绿，不止本次**。
- **对照证明 sim 里读版本可行**：`drills/32-install-lifecycle.sh:181` `_ver84()` 走 `cluster status --json | jq '.nodes[]?|select(.node_id=="brk1")|.release_version'`，`logs/32-install-lifecycle.log:39` 实测拿到 `[v0.0.0-simcluster]` 真值、:49 version-flip 断言 PASS。同批作者在 32 里用对了 schema ⇒ 30 是笔误级 bug。
- **为何拖到今天才炸**：`docs/deploy-tier-gotchas.md:166` #31 记录此前 3/3 real roll 都被 grow-lock 挡在 MECH=0 分支，**MECH=1 分支本次是史上第一次真正执行**。
- **建议动作**：jq 改 `.brokers[]|select(.NodeID==$n)|.BrokerVer`（并推动 P8 改 tag 后再改回 snake_case）。

### H2 — drill 51 手写的 cluster seam 只有 3/4 字段（还含一个无效键），DR 尾段被 not_covered 短路；失败被**误标为产品的运维后果**
- **类别**：harness | **严重度**：blocker | **gotcha**：#51/#52/#53
- **证据**：`drills/51-full-dr.sh:325-333` G1-clear 只写 `data_dir/raft_addr/nats_route`；日志随后 40 次刷屏 `error: broker: build cluster runtime: broker: cluster mode requires broker.cluster.secrets_dir`（`internal/broker/cutover.go:181`，这是对**不完整 seam** 的**正确** fail-closed），落到 :375 `NOT-COVERED 51 DR-completion (G3/H/I)`。`nats_route` 经核验**不是** `internal/serveconf/serveconf.go` 的 ClusterConfig 字段（该结构只有 DataDir/RaftAddr/SecretsDir/NatsConfPath/NatsServerBin），是无效键。
- **误归因证据**：`README.md:314` 与 not_covered 文案均称此为「the two [GAP] clears (#51 seam, #52 nats.conf) do not compose」，但 `_dr_gap "#52-natsconf"` 分支（`51-full-dr.sh:358`）**本轮从未执行**（ledger 输出 `undocumented=1 gaps=[#51]`）。
- **影响**：全灭 DR 的三个最重要问题**本轮全部未测**——#52（fresh box stock nats.conf 无 auth_callout）、#53（bundle 不含 JetStream ⇒ audit/history 永久丢失）、H2 terminus（灾前 expose 能否在同一公网端口吐回原 sentinel）。**tether 的全灭 DR 至今从未在部署层被端到端证明过一次**，而这一轮之所以没测到主因是 harness 自身，不是 tether 挡住的 —— 这是 CLAUDE.md §5 铁律②的**反面**（把 harness 缺陷记成产品缺陷），与掩盖缺陷同样有害。
- **建议动作（两层，第二层更根本）**：(a) G1-clear 照抄 `cmd/tether/cluster.go:905-906` 的完整 5 字段模板；(b) **补后置断言**——G1-clear 现在只 assert_ok 了 python heredoc 的退出码，对写出的 broker.yaml **无任何后置检查**；产品自己在 `applyClusterSeam` 里就做了"写完必须能 decode 回 `broker.cluster.raft_addr`"的 fail-closed 验证（有 `cmd/tether/cluster_seam_test.go` 覆盖）。补一行 decode 校验即可在**本轮当场**抓到 3/4 seam。

### H3 — drill 30 丢弃 `_do_roll` 的退出码、roll.log 从不落盘，且终末 warn 硬编码了与本次运行相反的叙事
- **类别**：harness | **严重度**：blocker | **gotcha**：#31（本次未复现）+ NEW
- **证据**：`drills/30-rolling-upgrade.sh:96-99` `_do_roll` 把 stdout+stderr 全量重定向进 `/tmp/roll.log`；`:155` **裸调用**（rc 不取不判）；`grep -n 'roll.log'` 全文仅 3 处命中（:58 grep / :98 写入 / :158 断言描述文字），**无任何 dump 路径**。日志 :186 走 else 分支打印 `grow lock was clean this run — the real roll proceeded directly`（MECH=1）、:190 M1 真跑并 PASS，而 :191（一条位于 `if/else` 块**之外**的无条件 warn）却宣称 `since #31's grow-lock leak blocks the upgrade this run (MECH=0), M1 is SUPPRESSED`。
- **影响**：与 H1 叠加，MECH=1 路径上**没有任何一个能证明升级发生过的断言**：剩余两条 PASF（:189 MainPID UNCHANGED、:190 write-probe 无 not_leader/503）恰恰是该 drill 自己头注 :10-12 点名的假绿 —— `"no upgrade happened" also satisfies zero-disruption + PID-same, so both must hold`，而 both 里的 version 那一半已被 H1 打死。**本次 RED 在事后无法归因**，这也是 P3 只能标 likely 的直接原因。终末 warn 的错误叙事会把读日志的人直接误导到 #31 上。
- **建议动作**：断言 `_do_roll` 的 rc；非 0 时把 roll.log 原样打印进 drill 输出；终末 warn 按 MECH 参数化。

### H4 — 覆盖记账契约被系统性旁路：24 处 `warn "…NOT-COVERED"` 不进 `not_covered` 计数器，静态闸的 BATCH 白名单不覆盖 21/37 个 drill
- **类别**：harness | **严重度**：major | **gotcha**：NEW（关联 OQ-2 / #30 / #25 / #26 / #27）
- **暴露 drill（合并 L1-7 / L4-5 / L5-4 / L6-1 / L6-2）**：30(3) / 31(1) / 62(1) / 71(4) / 73(3) / 74(10) / 82(2) = **24 处**，全部 in-BATCH=NO
- **证据**：`tests/lint-drills.sh:38-39` 自己定义了禁则 `bare-not-covered: a NOT-COVERED note via warn/log — record it with not_covered() so it counts toward INCOMPLETE`，但 `:27` 的 `BATCH="22 40 41 42 43 90 91 92 93 50 51 52 94 95 96 97"` 只有 **16** 个（不是 lane 说的 15），未覆盖 **21/37**；`:14-16` 头注自陈"a drill absent from this list is exempt from every static false-green ban above"。运行侧对照：`logs/73-proxy-cluster-ha.log:214` warn 后 :231 `verdict=GREEN … not_covered=0 pass=42`；82 / 62 同构；对照 96 用真 API 时 `not_covered=3`。
- **四个子面**：
  - **(a) 计数器旁路**（上述 24 处）——run-drills.sh 五列汇总与 `--allow-incomplete` 豁免策略对这些缺口完全失明。
  - **(b) 缺口被写成 PASS**：`drills/62-remote-fs-safe.sh:118-119` `assert_ok "Arm2 true-D + mode:off-without-safe NOT-COVERED (…)" _statfs_healthy` → 日志 :28 `[ ok ] PASS Arm2 … NOT-COVERED …`。契约（`lib/assert.sh:60`/`:174`）两处明写 not_covered "NOT a PASS"。**须关联 `docs/deploy-tier-gotchas.md:643` OQ-2**（该缺口已在人可读台账登记，故准确表述是"机器可读层不可见"，不是"完全不可见"）；且谓词 `_statfs_healthy` 是一次真实 live 测量而非自证 `true`，作者是有意做近似定格。
  - **(c) 已登记缺陷用倒置 assert_ok 钉住 → 计入 GREEN**：`80-session-isolation.sh:179`(#25) / `81-admin-evict-session-rm.sh:167`(#26) / `82-agent-onboarding-invite.sh:104`(#27)，三 drill 末行均 `verdict=GREEN … product_red=0`。**关键强化证据**：`lib/assert.sh:169` 已存在公共记录器 `product_red "<desc>"`，不要求 assert_bug 那种"修好后命令应成功"的形状 ⇒ 「与 assert_bug 语义相反」不构成保持 GREEN 的正当理由，这是**可避免的**假绿，撞 `lib/assert.sh` round-4 policy「A reproduced known defect lands PRODUCT-RED … remains a release blocker by default. The old 连绿 discipline is ABOLISHED」。
  - **(d) 上游成因**：`run-drills.sh:83-84` 的 `--allow-product-red` / `--allow-incomplete` 是**全局布尔**、无 per-drill 粒度。`drills/97-soak-cycles.sh:31-35` 头注**明写据此做了设计决定**：「burning the suite-wide --allow-incomplete waiver on a known permanent gap would trade one drill's honesty for the whole suite's (waivers are GLOBAL, not per-drill)」——因果链由被审对象自己的文字坐实。
- **⚠ 已被证伪的旗舰例证（不得写入报告）**：L6-1 称 `73-proxy-cluster-ha.sh:173` 那条 warn 描述一个 EXPOSED DEFECT 从而"缺陷被 warn 吞掉仍判 GREEN"。实测该 warn 在 **:344**，且上下文明写 `Q-xcheck FAILED above = the RED`——该缺陷已由前一条失败的 `assert_ok` 计入 assert_fail，drill 会落 ASSERT-FAIL。真正带进 GREEN 的未计缺口是 73:271(#30 无 operator reader)、73:347(quorum separation)、82:205/209(无 systemd --user)、62 —— 属**覆盖度低报**，不是**缺陷被掩盖**。
- **另一处已被证伪的安全感**：`lint-drills.sh:18-21` 声称 runner 会 cross-check verdict-line rc vs process rc 兜底，但该校验只比对 rc 一致性，对 `warn "…NOT-COVERED"` 这类**计数器本就为 0、rc 与 verdict 自洽**的情形完全无效。
- **建议动作**：(a) BATCH 改为默认全量（`--all` 已能一次报出全部违规，见 H12）；(b) 24 处 warn 改 `not_covered`；(c) 62:118 改 `not_covered` 并关联 OQ-2；(d) #25/#26/#27 旁加 `product_red` 调用；(e) waiver 改 per-drill 粒度或 waiver 白名单。

### H5 — `poll_until` 不可重入：全局 `_pu_*` 变量被嵌套调用覆写（**两种故障模式，L4 只识别了一种**）
- **类别**：harness | **严重度**：major | **gotcha**：NEW
- **证据**：`lib/log.sh:26-38` 的 `_pu_timeout/_pu_interval/_pu_desc/_pu_end` 是全局（POSIX sh 无 local）；`drills/lib/proxy.sh:51` 的 `ss_up` 内部自带 `poll_until 10 1`。dash 双向复刻均已完成：
  - **模式 A（内层失败）**：外层第 1 次迭代后立刻判超时，并用**内层**的 desc/timeout 报错。现场签名 `logs/74-rebalance-on-return.log:225` `poll_until: timed out after 10s waiting for: ss-local ctl1:1093 SOCKS listener ready`，而该断言写的是 `poll_until 240 5 "recorded moved exit agt3 SS flows"`（`74:325`）。
  - **模式 B（内层成功，L4 完全漏掉、危害更大）**：内层每次成功都把全局 `_pu_end` 重置到 `now+内层timeout` ⇒ **外层 deadline 被无限延后、永不超时 = 无界挂起**。复刻中进程须被 20s `timeout` 强杀。后果是 drill 挂死、整轮 run 被 `-j` 并发档拖垮。
- **⚠ 两条被证伪的 impact 子主张**：(a) 「71/73/74 里所有 240s/180s/120s/90s 判定实际只有 ~10s 窗口」——**对 71 为假**，71 的 `_drain_migrated` 走 `dataplane.sh:26 dp_curl_ok_body`（纯 curl，无嵌套），窗口真实完整（这反而对 P1 有利）；(b) 「#33 在 73 的量化结论被系统性污染」——被日志自证证伪，`logs/73-proxy-cluster-ha.log:208` 记录 `≈19s` > 10s。
- **爆炸半径（核验者系统枚举，跨 lane 价值最高）**：全部嵌套源为 `drills/lib/proxy.sh:51`、`agentyaml.sh:117/121`、`ingress.sh:43/85`、`ident.sh:96`、`cluster.sh:31/71/75`、`lib/tether.sh:42 wait_phase`。其中 **`wait_phase` 被广泛用于集群相位等待**，任何 `poll_until … -- wait_phase …` 形态会触发模式 B 无界挂起，波及 **40/41/42/43 等 grow/retire 家族**，远超 L4 lane 范围。
- **建议动作**：`poll_until` 改用带唯一后缀的变量名或显式保存/恢复；全仓审计上述 8 处嵌套点。

### H6 — 96 D6b 的 GREEN 是空绿：canary3 从未写入却落进"已回滚"分支；日志还把 rc=69 记成 rc=0
- **类别**：harness | **严重度**：major | **gotcha**：**#65（其地面真相被污染）**
- **证据**：`logs/96-mid-flight-chaos.log:40` `D4b diag: minority write via brk1 rc=69 out=error: session create: cannot reach broker at nats://brk1:4222: … i/o timeout`，紧接 :41 `D4b: the minority write via brk1 returned rc=0 (a STALE-LEADER transient accept…)`；随后 `D6b RAW ARTIFACT … brk1=no brk2=no brk3=no` → `PASS D6b NO SPLIT-BRAIN (exclusion half): canary3 was rolled back`。
- **机理**：`_d4_minority_refuses` 中 rc=69 既非 0 也非 124，落到末尾 grep（正则含 `timed out` 但**不含** `i/o timeout` 也不含 `cannot reach`）→ 返回 1 → 调用点走 else 分支，把"因未匹配签名而判否"误叙述成"返回 rc=0 的 stale-leader 接受"。而 D6b 的三分支判别（:414-420）只看 canary3 可见性，**无法区分"被 raft 回滚"与"压根没写进去"**。错误串与 `cmd/tether/error_hints.go:155` 的 unreachable 格式串逐字对应 ⇒ CONNECT 阶段失败 ⇒ canary3 从未抵达 broker。
- **影响**：直接污染 #65 这条 raft-safety 候选缺陷的地面真相。`docs/deploy-tier-gotchas.md:387-407` 的「6 次复跑 5 次持久 / 1 次回滚(run-3)」把回滚计为反例，而 run-3 自述"该轮 D3 幸存写也超时 = 退化 run"与本 run 特征**同型**，高度提示同为空绿。**本 run 对 #65 的正确记法不是 GREEN 而是 `not_covered`**；若照当前逻辑继续累计，分母会被空绿持续稀释。
- **建议动作**：D6b 必须先证明 canary3 曾被接受（前置存在性检查），否则记 not_covered；`_d4_minority_refuses` 正则补 `i/o timeout|cannot reach`（或改为白名单式判别）。

### H7 — drill 40：杀 leader 后用两个**不一致**的 leader 判据，且 `sim_leader` 的 fallback 硬编码到刚被杀的 brk1
- **类别**：harness | **严重度**：major | **gotcha**：**#45/#37（本 run 未触达）**
- **暴露 drill**：40-drain-retire（R-resume）。**合并 L2-5 + L6-5。**
- **证据**：`logs/40-drain-retire.log:255` `+ d kill sim-drill-40-drain-retire-brk1` → :256 `SETUP-FAIL resolve new mid-retire leader` → :257 `verdict=SETUP-RED … pass=32`。
- **机理**：`drills/40-drain-retire.sh:219` 的 poll 用**宽**判据 `jq -r '.leader_id // empty'` 且要求 != OLD_LDR —— **该 poll 通过了**；:220 `LDR=$(sim_leader) || setup_fail` 是**单发无重试**，而 `drills/lib/cluster.sh:103-110` 的 `sim_leader` 主路径用**严**判据 `select(.is_leader_view==true).leader_id`（simcluster:488-497 在 `is_leader_view!=true` 时 fail-closed 打印 error JSON 并 return 1），fallback 写死 `$SIM exec brk1 -- tether cluster status --json` —— brk1 正是上一行被 kill 的容器，**结构上永远不可能救场**。
- **归因订正（L2-5 一处过强）**：触发器**确实是**时序（同一条命令几百毫秒前刚成功）；准确表述是"触发是时序竞态，缺陷在于 harness 在一个已知会抖动的窗口里用了单发解析 + 一个死掉的 fallback"。归类仍为 harness（容错缺失在 harness 侧）。
- **影响**：drill 在 retire 收敛断言之前 abort ⇒ **本轮全量结果里「retire 中途换主后能否收敛」（#45 卡 NATS_ROLLED_OUT、#37）这块覆盖为空白**，不能因 verdict 只是 SETUP-RED 就当作"只差一点"。且 SETUP-RED 在契约优先级（`assert.sh:21` `ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN`）上高于 PRODUCT-RED，掩盖了后续臂本可暴露的 #31/#38 信号。
- **建议动作**：:220 改 poll 重试；`sim_leader` 的 fallback 改为遍历存活节点而非硬编码 brk1。

### H8 — drill 42 漏传 `--ack-alerts`，被 force_single_active 严重告警门确定性拦下
- **类别**：harness | **严重度**：major（**统一为 major**：L2-6 标 minor 与 L2-5 标 major 内部不自洽，影响量级相同）| **gotcha**：NEW
- **暴露 drill**：42-rejoin-returning（步骤 I）。**合并 L2-6 + L6-4。**
- **证据**：`logs/42-rejoin-returning.log:40` `SETUP-FAIL I create real post-force-single transfer-audit Raft entries (want exit 0, got 70)` + 产品原文 `• force_single_active — the cluster is running on one emergency broker with no backup.`
- **机理**：`drills/42-rejoin-returning.sh:151-152` 的 push 无 `--ack-alerts`；产品门是设计内且有官方 override：`cmd/tether/transfer.go:69` 注册 flag、:157-158 `gateDestructive(nc, id.PublicKey, ackAlerts)`，`cmd/tether/d8_alerts.go:70-73` 注释明写 "BLOCKS unless ackAlerts"。drill 前面刚做完 OFFLINE force-single ⇒ 告警必然 active ⇒ 确定性、重跑必现。
- **影响**：G+I 收尾臂（resnapshot audit-window 拒绝 / `--accept-audit-loss` 有界丢失 override / 不复活陈旧 peer / F join approve 并行）**本轮全部未执行**——这正是 rejoin-returning 的核心价值；27 条 PASS 被 SETUP-RED 单桶吞掉。
- **⚠ 附带 open question 须弱化**：L2-6 拿 96 的 B0（`run` 未被拦 rc=0）作对照，但 96-B0 的告警由 kill brk2 产生、**不是** force_single_active，两者条件不同，不构成"门覆盖面不一致"的证据。可留的只是「`transfer.go:158/:419` 让纯数据面 push/pull 也走 gateDestructive」这一事实本身。
- **建议动作**：加 `--ack-alerts`（一个 flag 即恢复整段覆盖）。

### H9 — drill 71 B-migrate 失败后不落任何终态证据，无法区分"home 根本没改"与"home 改了但数据面没跟上"
- **类别**：harness | **严重度**：minor（但**直接决定 P1 的回归可判定性**）| **gotcha**：NEW
- **证据**：`drills/71-expose-rehome-failover.sh:176` 断言失败后直接落到 :183 `cluster drain brk3 --abort`，中间不打印 `expose explain wstrand --json`、也不 curl 旧 home `brk3:$PC`。日志 :200-202 印证。对照 Arm C/D 有显式 epoch/moved/explain 断言（:191/:192）。
- **影响**：P1 的机理是回到产品源码反推出来的，drill 本身给不出。修复产品前后回归时无法证明修的是哪一半。**核验者补充的等价价值**：失败分支同样没 curl **旧 home**，而"端口仍在老家服务"恰是证明控制/数据面分裂（而非彻底不迁移）的最直接观测。
- **建议动作**：失败分支强制 dump `expose explain --json` + 旧 home curl 结果 + agent journalctl。

### H10 — drill 74 把 harness 前置失败叙述成 `#33 族 moved-exit 搁浅、release-blocking`
- **类别**：harness | **严重度**：major | **gotcha**：#33（**本次归因不成立**）
- **证据**：`logs/74-rebalance-on-return.log:224` 断言文本 `RED if it STRANDS = the #33-family moved-exit stranding EXPOSED as release-blocking (R6-M1)`，唯一理由行 :225 是 ss-local 未就绪。该消息来自 `drills/lib/proxy.sh:51-52`，其注释写死语义：`Return 1 (a HARNESS/setup failure — callers die-gate it, NEVER count it as a product black-hole)`。
- **机理**：`74:56-61` `_ss_via_agent` 把 `ss_up … || return 1` 与真实黑洞（`ss_curl_ok` 失败）**折叠成同一 return 1**，抹掉 proxy.sh 精心建立的 R4-M1 分离；H5 让这一次失败没有任何重试；ss-local 的现场诊断写在 ctl1 容器内 `/tmp/sslocal-1093.log`，drill 从不回收，容器已 nuke ⇒ **根因现已不可追**。
- **收窄**：可硬结论的是"失败点落在最初约 10s 的本地客户端/订阅阶段"；"确为 ss-local 未 bind"是高概率而非唯一解（`ss_sub_fetch` 路径同样能产生该签名）。无论哪种，该 RED **不能**作为 #33 证据、更不能当 release blocker。
- **建议动作**：拆分 ss_up 失败与 ss_curl_ok 失败两条返回码；回收 `/tmp/sslocal-1093.log`。

### H11 — drill 74 Arm C 前置门：滚动重启后不 settle，且 `_count_on` 的 -1 fail-closed 让 0-home broker 中选
- **类别**：harness | **严重度**：major | **gotcha**：#34（**产品侧半边如实再现**）
- **证据**：`logs/74-rebalance-on-return.log:232` `post-B distribution: brk1=1 brk2=1 brk3=1` → :233/:234 C-setup/C-verify PASS → :235 `Arm-C KTGT=brk3 (0 homes)` → :236 FAIL C-SKEW-precond → :237 `pre-skew homes (VALIDATED)=[agt1=brk2 agt2=brk1 agt3=brk1]`（brk1=2/brk2=1/brk3=0）。
- **两件事必须分开记**：
  - **(产品，成立)** 「构造好的 1/1/1 经一次滚动重启即退回 2/1/0、重启后的那台 voter 拿 0 个 home」是 **#34**（`docs/deploy-tier-gotchas.md:231`）的又一次现网级复现。运维含义：任何 broker 滚动升级/重启后 proxy 出口会重新堆到少数 broker 上，**必须人工再跑 `cluster rebalance proxy`**，否则 HA 容量分布形同虚设。
  - **(harness，成立)** 「auto-rebalance 开了也不发火」这一推论**不成立**——`74:157-164` `_set_auto_rebalance` 重启完只 poll `_three_voters`、零 dwell（对比 Arm B 有显式 B-settle:281）；`_count_on`(:101) 快照无效时返回 -1，`-1 > -1` 为假 ⇒ brk2 可被静默跳过，让 0-home 的 brk3 以 `0 > -1` 中选。:235 不打印当时 leader，故 leader 漂到 brk2 vs brk2 快照瞬时无效两路不可判（日志此前一路显示 leader=brk1，而 C-setup 重启了每台 broker，leader 漂移至少同样可能）。
- **影响**：Arm C 整条不可跑（`C_EDGE=0`，C-auto/C-dp 本 run 无覆盖），**G7a m11 的 auto 路径至今在部署层无有效验证**。
- **建议动作**：`_set_auto_rebalance` 后加分布 settle；`_count_on` 无效快照改为 die 或重试；KTGT 选择日志打印当时 leader。

### H12 — 静态闸 `lint-drills.sh` 对 drill 内嵌 jq / 描述串完全失明；`--all` advisory 另报出两类更危险的 legacy 债
- **类别**：harness | **严重度**：major | **gotcha**：NEW
- **子面 (a) — jq 无可编译性闸（本轮直接造成一条假 RED）**：93 **在 BATCH 内**、静态闸跑了并 ALL PASS，可 `drills/93-metrics-observability.sh:108`/`:114` 两个**编译不过**的 jq 程序照样溜过去（结尾 `…|sort))))` 多一个右括号；实测喂 jq 得 `syntax error, unexpected INVALID_CHARACTER` / rc=3，去掉多余 `)` 后喂真实 payload 得 `true` / rc=0。本次方向是 fail-closed（假 RED），**镜像方向是 fail-open**：一个恒真过滤器会直接铸出假 GREEN。
- **子面 (b) — 空针假绿路径**：`93:138` 的 `printf '%s' "$0" | grep -qF '$STATUS_HEALTH'`，当 `.health` 缺失/JSON 为空时 STATUS_HEALTH 为空串，`grep -qF ''` **恒返 0**，该子条件被架空。这与 `lib/assert.sh` 文件头明写的「An empty signature is REJECTED (it would `grep -E ''` match ANY failure and forge a false GREEN)」是同一类危害，只是发生在 drill 自拼的 shell 里、绕过了 assert.sh 的 `_as_nonempty` 保护。
- **子面 (c) — 反引号命令替换污染断言描述**：`81-admin-evict-session-rm.sh:139` 的描述里用了未转义的 `` `ps` ``，触发命令替换，把宿主机数十行 ps 输出灌进断言描述（`logs/81-*.log:24` 起可见 `PASS C-base-proc-db broker PID TTY TIME CMD` + 进程表）。断言描述因此非确定性、日志不可 diff。
- **子面 (d) — `--all` 报出的其余两类，危害不低于 bare-not-covered**：实跑 `sh tests/lint-drills.sh --all` 除 24 处 bare-not-covered 外另有：**combined-signal-trap 8 处**（31/32/62/72/73/74/82 …），lint 自己的注释写明后果是「a Ctrl-C mid-drill runs cleanup then **RESUMES** executing kills / iptables / cert overwrites / volume disasters」——**破坏性**风险；**sigpipe-truncation 3 处**（12/20/74），lint 注释记录该模式**曾真实制造过一次栽赃产品的伪 RED**（"drill 91 truncated `force-single` before its nats.conf de-cluster step and then blamed the product"）——正是铁律最忌讳的方向。
- **建议动作**：lint 增加 jq 可编译性检查（`jq -n '<filter>' </dev/null`）、空针检查、描述串反引号检查；BATCH 默认全量；优先清偿 combined-signal-trap 与 sigpipe-truncation。

### H13 — drill 93 三条 RED 零诊断，且 card 诊断的 `head -8` 恰好切掉被 grep 的 `(exit N)` 行
- **类别**：harness | **严重度**：major | **gotcha**：NEW。**合并 L5-1/L5-2 + L6-9。**
- **证据**：两条 webhook FAIL 的全部附加信息只有 `poll_until: timed out after 20s`（谓词内部会 `cat /tmp/hook.log`，但**失败路径从不 dump**）；第三条 CARD/JSON FAIL 无任何 STATUS_RC/STATUS_EXIT/STATUS_HEALTH/CARD_RC 输出。`93:136` `printf '%s\n' "$CARD_OUT" | sed … | head -8`，而 `:138` 要 grep 的 `(exit N)` 由 `cmd/tether/cluster_status_card.go:66` 打在卡片**末尾**，正好在 head -8 之外。`:137-138` 是四段 `&&` 复合断言，失败无法定位是哪一段。
- **反证 webhook 管线可用**：同日志 `PASS WEBHOOK warmup: leader-side reconciler/poster emitted the unique alert`（同为 20s 窗口、同样 cat hook.log；其过滤器 `93:103` 结尾 `))` 平衡）。
- **CARD/JSON 竞态的产品侧解释（likely）**：`93:133-139` 是单发、无 poll、无容差的两次一次性采样，前置只有"brk1 回到 VOTER && reachable"，**不含流副本/拓扑代收敛**；`internal/broker/clusterstatus.go:464/:475/:496` 三条（applied-lag / 拓扑代未追上 / `StreamActual<StreamTarget`）均能在 VOTER&&reachable 已成立后仍判 DEGRADED。刚被连续重启两次的 brk1 会有数秒 `StreamActual<StreamTarget`。而紧随的同一健康面断言 `:141-143` **显式接受 exit 0 或 1** 且用 poll_until，注释自承 "the cluster may show a transient DEGRADED-WRITABLE (topology-observer lag)"——**同一健康面两条断言容差不对称**。已排除产品侧"两种渲染给不同 exit code"的可能：`cmd/tether/cluster.go:156` 的 `os.Exit(rep.ExitCode)` 对 --json 与 --card 同路径。
- **另一条未列举的同类路径**：STATUS_JSON 为空 → STATUS_EXIT 空 → `[ '0' = '' ]` 为假，同样落 harness。
- **⚠ 尚不可定论**：`transition=cleared` 的 POST 本轮**是否真的发出**，日志无法证明——该断言的 `wc -l` 增长前置被 `&&` 串在坏 jq 前面，信号一并丢失。且该 `wc -l < /tmp/hook.log` 前置隐含"receiver 每个 POST 恰写一行 JSON"这一**无任何断言背书**的假设。
- **建议动作**：修 jq 括号；失败路径 dump hook.log；card 诊断去掉 head -8；把复合断言拆成四条；CARD/JSON 改 poll 到两次采样一致并按 `94-agent-reconcile.sh:191-193` 的 R-DIAG-OUTSIDE 纪律打 DIAG 行。**修好后必须重跑 93 才能对 webhook wire schema 定论**（见 D6）。

### H14 — drill 96 F 臂：注释承诺的 F0c 门控在代码里不存在，F0c 与 F4 断言同一谓词 → 一个根因计成两条 ASSERT-FAIL
- **类别**：harness | **严重度**：minor→**major**（统一采纳 L6-6 的 major）| **gotcha**：NEW。**合并 L2-8 + L6-6。**
- **证据**：`drills/96-mid-flight-chaos.sh:436-438` 注释写明「that is an arm-D-residue setup problem, **gated below**, not a G.1xG.2 finding」，但 `:439` 是普通 `assert_ok`（按 `lib/assert.sh` 契约应为 `assert_setup`），`:441-450` 的 F1..F6 无条件顺序执行；`grep _f4_agt2_running` 命中 :184(定义)/:440(F0c)/:446(F4) —— **同一谓词**。日志 `assert_fail=4`，而真实互异信号为 **3**（D3 / F0c / F5）。对照 `74:473-479` 在同类情形下是显式跳过依赖臂 ⇒ 纪律未跨 drill 统一。
- **F5 是独立新信号（不属该级联）**：`PASS F3 G.1xG.2 converge: agt1's processes are reconciled to EXITED(-1)` 与 `FAIL F5 G.5: the audit says kind=reconciled_closed … timed out after 120s` 并存；`internal/broker/reconcile.go:89/:101/:104/:108` 正是在同一调和计划里生成 `Kind:"reconciled_closed"` 的 ProcAudit，:207/:225/:236/:249 走 `pubAuditProc`。**状态翻了、审计行 120s 读不到**。竞争解释未排除（假说）：`_f5_audit_row` 用 `history --kind proc -n 100`，前置产生大量 proc 行可能把目标行挤出窗口。
- **建议动作**：F0c 改 `assert_setup`（或显式 gate F4）；F5 用 `-n 1000` 或按 nid 过滤定向复跑判别——若不是读窗口问题，则是 G.5 审计不变量的真实破口（进程被系统单方面调和关闭却无审计留痕）。

### H15 — 全套 rollup 只走 stdout、无落盘产物，经 `ssh -t`（无超时/keepalive）投递
- **类别**：harness | **严重度**：minor（**降级**：rollup 可从持久产物完全重建）| **gotcha**：NEW
- **证据**：`run-drills.sh:216-257` 的 summary 段全部是 echo/printf，`$LOGDIR` 在该段只被读；ALL GREEN / WAIVED NON-GREEN / `$blockers BLOCKER(S)` 三分支与 `exit "$blockers"` 只存在于 stdout 与退出码。`remote.sh:96` `ssh -t "$SERVER" "$remote_cmd"`，全文无 `ConnectTimeout`/`ServerAliveInterval`/`-n`；`remote.sh:10 set -euo pipefail` 使整链结论依赖这一次 ssh 正常返回。本次症状吻合：服务器端 runner 已退出、`.rc` 全部写好，本地 ssh 仍不返回，summary 一次都没打印。
- **降级理由**：每个 drill 的 `.rc` 与五态一一对应（`assert.sh` 契约 rc 0/1/2/3/4），`.log` 内保有完整 `DRILL-VERDICT` 行 ⇒ 丢失的是**派生视图**而非闸门输入，核验者用一条 `for f in *.rc` 数秒重建全部 37 行并与 25/5/4/2/1 相符。ssh 挂死的确切机理**未归因**（无证据支持"残留进程持有 PTY 从属端"）。
- **建议动作**：summary `tee` 落盘 + ssh 加 `ServerAliveInterval`/`ConnectTimeout`。

### H16 — 其他单点 harness 债（低优先，一并登记）
| # | 内容 | 锚点 |
|---|---|---|
| a | `_dry_run` 是弱到近乎空绿的 oracle：grep 的正则含 `roll`，而 `renderUpgradePlan` 首行无条件打印 `rolling upgrade plan → …`、多条 fail-closed 错误串（`refusing to plan a roll on incomplete data`）同样含 `roll` ⇒ dry-run 失败/计划被 REFUSED/RPC 挂掉**全部照样 PASS** | `drills/30-rolling-upgrade.sh:77` |
| b | 30 的 `_roll_halted_on_growlock` 正则只覆盖 #31 两种拒绝文案中的一种。`growActiveJoiner` 分支文案是 `a \`cluster add\` grow of X **is in progress**`（不含 `in flight`），会被反向标注成"grow lock 干净"。另 `grows are serialized` 出自 `cluster_grow_trigger.go:77`（grow 被 grow 挡），根本不会出现在 upgrade 的 roll.log 里 = 死枝。**本轮无影响**（:138/:168 两处 `cluster add complete.` 前后均无 `⚠ WARNING: grow lock release did NOT confirm`） | `drills/30-rolling-upgrade.sh:58`；`internal/broker/cluster_upgrade_trigger.go:152-162` |
| c | 30 的 whole-host 断言依赖 roadmap **OQ-6** 的同机 agent 供给，而该供给从未实现 ⇒ 即便修好 H1 的 jq，在现有 sim 拓扑下仍不可满足（双重不可满足态）。按铁律②应二选一：真补 OQ-6 供给，或明确记 `not_covered` 并把断言改成只钉 broker 守护腿 | `drills/30-rolling-upgrade.sh:175`；`docs/simcluster-coverage-roadmap.md:448/:905-907` |
| d | 71 的**已登记** #31 阻塞分支用 `assert_ok sh -c false` 落 ASSERT-FAIL（应 `assert_bug`），74 的 C-SKEW-precond 是显式前置门却也落 ASSERT-FAIL（应 `assert_setup`）⇒ 稀释"ASSERT-FAIL = 新信号"语义。**限定**：71 的两条分支本 run 未触发，实际污染只有 74 一条 | `71:179/:182`；`74:384` |
| e | 95 有两条**静默、未被任何 assert 包裹**的 `poll_until: timed out after 20s waiting for: spacing the restart arms so StartLimitBurst=5/10s cannot bite` ⇒ T2a/T3a 在未达成预期间隔下开跑，而同 drill 的 T1h 明确说明 install.sh 故意保留该限制、"一个残留会杀掉后面的臂并被算到产品头上"。#23 系列臂本 run 的 GREEN 是在其自述隔离前提未成立时取得的 | `logs/95-broker-selfheal.log` |
| f | 51 的 `DR-STEP-LEDGER` 在中途 abort 路径上给出误导性分母：`_dr_step` 与 `_dr_gap` **都**递增 `DR_REQUIRED`，本轮 3+1=4 恰等于硬编码的 `DR_DOCUMENTED=4` ⇒ 输出 `documented=4 actually-required=4` 读起来像"文档步数正好够用"，与该 drill 要传达的结论**相反**（真相是 4 步里 1 步永远走不通、还额外需要 ≥2 个未文档化步骤） | `drills/51-full-dr.sh:78-84`；`logs/51-full-dr.log:108` |
| g | 无任何断言保证容器内 `/opt/sim/install.sh` 与 `scripts/install.sh` 字节一致 ⇒ 未来任何 install.sh 类结论都得靠人工另做复现才能站住 | `remote.sh` re-vendor 路径 |
| h | `tests/*.sh` 以 100644 提交。**注意**：全部 37 个 `drills/*.sh`、11 个 `drills/lib/*.sh`、6 个 `lib/*.sh` **也都是** 100644，100755 仅限 4 个真正入口点 ⇒ 这是本树对"非入口脚本"的**一致约定**，`sh tests/lint-drills.sh` 是约定表述而非绕开缺陷。**info 级，非 bug** | `git ls-files -s test/simcluster/` |

---

## §D 文档 / 台账缺陷

### D1 — 缺陷台账三处与本轮实测不一致（台账是发布闸 SSOT）
- **类别**：doc | **严重度**：minor（**优先级不低**）| **gotcha**：#52/#54/#63
- `docs/deploy-tier-gotchas.md` #52 条目（:329-337）两次写 `DR-STEP-LEDGER（undoc=2）`，实测 `undocumented=1 gaps=[#51]`（根因即 H2）→ 会**夸大** runbook 缺口。
- #54 条目（:439）写 `facet 1（reconcile false all-clear）实测中（B2 迭代）`，而已 live-confirmed → **低估**已确证的严重度。
- #63 条目（:468-471）状态 `候选`，其钉住条件明写"自发 45s + 分区强制 redial 90s 双窗口均不 re-pin 才钉"，而 `logs/52-credential-rotation.log:24` 的 A7d 是 **PASS** 且用双边沿证明（旧会话 rc=124 证死 → 新会话吐 sentinel）⇒ 前提被反证，**应撤销而非续挂 CANDIDATE**（否则把一个已证伪的缺陷计入发布阻塞）。
- 另需订正：#56 的"死循环"措辞（见 P11）；50 的 DOC-27 product_red 文案称运维"gets a `store_error`"，实际返回的是 `internal/broker/clusterbackup.go:53` 的 `create bundle dir %q (must not exist)` + `adminsock.CodeBadRequest`。

### D2 — runbook §5.2 缺 seam 步骤、§5 备份示例路径不可写、DR 不提 history/audit 丢失
- **类别**：doc | **严重度**：major（合并 DOC-27 + #51 文档半边 + DOC-19）
- `docs/cluster-runbook.md:524` `tether cluster backup --out /var/backups/tether-$(date +%F)` → `logs/50-backup-restore.log:24` `mkdir /var/backups/tether-…: permission denied`。机理：`scripts/install.sh:491` 只 `install -d -o tether -g tether` 建 LIB_DIR/LOG_DIR，`/var/backups` 从未被建且 root-owned，broker 以 `User=tether` 跑 MkdirAll 撞 EACCES。**这条挡在所有灾备流程最前面**，而 §5 开头就说 "Take backups on a schedule + before any destructive op"。（DOC-27，签名守卫非 parent-dir/perm 原因走 UNJUDGEABLE，非空绿）
- §5.2 需补 seam 步骤并交叉引用 §4:493，同时把 §4:493 的 3 字段补成 5 字段（漏 `nats_conf_path`）。
- 需补「history/audit 不随 bundle 回来」的告警（#53：`internal/clusteroffline/backup.go:87` bundle 只含 state.db，`restore.go:317` 归零 `audit_published_index`）。
- §2.1（:243-248）把 `reconcile nats --all --wait` 指定为轮换的 re-render 动词，需与 P6 一并修订。

### D3 — `cluster status` 的 exit code 在 voter 重启后会抖动数秒，文档未提示需去抖
- **类别**：doc | **严重度**：minor | **gotcha**：NEW
- 机理见 H13 的 clusterstatus.go:464/:475/:496。`cmd/tether/cluster.go:156` 的 §17 契约把 exit code 定义为可被 cron/monitoring 消费，但任何按 exit code 告警的 cron 每次滚动重启都会产生一串 exit=1 噪声。

### D4 — DOC-12：三个 architecture H.1 承诺的事件无 writer，且台账声称的 drill ownership 在 80/81 中零实现
- **类别**：doc | **严重度**：minor | **gotcha**：DOC-12
- 技术结论仍成立：`grep -rnE '"kicked"|agent_unregistered|rotated_pin' internal/ cmd/ --include=*.go` 仅命中 `internal/broker/audit.go:33/:34/:35` 三条**注释**，无 writer；命名漂移属实（`internal/broker/admin.go` 的 `pubAgentEvicted` 发 `agent_evicted`，与承诺的 `kicked` 不同名）。
- **新增问题**：台账（:609-613）写「**80** owns `rotated_pin`；**81** owns `kicked`/`agent_unregistered`」，而 `grep -rnE 'rotated_pin|kicked|agent_unregistered' test/simcluster/drills/` **零命中** ⇒ 无主条目，flip-on-fix 机制形同虚设。
- **订正**：L5-6 称"81 日志中观察到 sys.events{agent_evicted}"略夸张——`81:93-94` 的 B1 断言实际匹配的是 CLI stdout 的 `evicted sid=lab nid=agt1.*broadcast=true`，未读 sys.events 的 type 字段；`agent_evicted` 这个名字来自源码而非日志观测。
- **对照（健康，本轮均仍成立且被实钉）**：DOC-6 由 81 `D1 evicted nkey (byte-identical, md5==D0) re-provisions with same session PIN → ONLINE` 钉住；DOC-8 由 80/81 所有 CONNECT-deny 臂统一用通用串匹配体现；DOC-11 由 81 E3a/E3b/E3c 三条钉住。

### D5 — README 的 drill 表：verdict 漂移 + **scenario 列内容腐化**（后者更危险）
- **类别**：doc | **严重度**：minor（**L5-7 的 90/91/92/93 半边已撤回**，见 §E）
- **verdict 漂移（须改的只有 3 行）**：`README.md:294` 对 30 标 GREEN（实测 ASSERT-FAIL）、:312 对 93 标 INCOMPLETE（实测 ASSERT-FAIL）、:318 对 96 标 PRODUCT-RED（实测 ASSERT-FAIL）。22/43/90/91/92 的 INCOMPLETE→GREEN 属**探索臂真的跑通、缺口已关闭**（各 34–49 条实体断言），是跑前占位符的文档欠账。40/42 的 SETUP-RED 是 H7/H8 的 harness 缺陷，不是期望写错。
- **scenario 列腐化（核验者新发现，比 verdict 漂移更易误导）**：`README.md:309` 声称 90 的 "M6 disk_pressure + below_quorum/raft_lag are `not_covered()`"，但 `grep not_covered drills/90-alerts-lifecycle.sh` **零命中**，而 :153/:157(raft_lag)、:178/:184/:190(disk_pressure)、:202/:204(below_quorum) 全是实体 assert（pass=49）。:312 对 93 的 "READYZ-503 … are not_covered()" 同样与日志矛盾（93 日志有 `PASS READYZ-503`）。⇒ README 在描述**一个已不存在的旧版 drill**，会让下一轮评审去找不存在的缺口。
- **低成本行动项（L6-7 漏掉）**：`README.md:302` 的表已是 `| drill | expected verdict | scenario |` 三列且期望一律加粗 ⇒ run-drills.sh 只需十几行解析器即可与实测 `.rc` 自动差分。"无法自动差分、只能人肉比对"不成立。

### D6 — 覆盖 overclaim：`94` 声称覆盖 `ps` LOST，实际零断言；webhook wire schema 三层全无覆盖
- **类别**：doc + 覆盖清单 | **严重度**：minor
- **94**：`grep -n LOST drills/94-agent-reconcile.sh` 仅命中 :4/:82/:101/:139/:173/:174（全为注释/drill 名），A1 臂 :177-179 只断言 agt1 STALE→OFFLINE，**全文件无任何对 `ps` `status==LOST` 的断言**。且 overclaim 不止在 README 行，也写在 drill **自身**的 `drill_begin` 名（:139）与头注（:4），这两处无免责声明。`ps` 的 LOST 派生态（read-time 计算，`exec.go:337-346`）在 deploy tier 无任何断言。
- **webhook wire schema**：`grep -rn tether_alert_webhook --include=*.go` 全仓仅命中 `alert_webhook.go:126` 一处、无任何测试引用；hermetic `internal/broker/b6_webhook_test.go` 只断言 `WebhookEvent` 结构体字段，**从不读 HTTP body**（其 httptest handler 只用于 hang/queue 语义）。⇒ 安全承诺 `alert_webhook.go:21-26`「Body carries only PUBLIC topology — never a secret」**没有任何一层测试钉住**，deploy tier 的唯一校验就是 H13 那两条坏掉的 jq。

---

## §E 被驳回或收窄的主张（不得写入正式报告的结论）

| 原条目 | 主张 | 裁决 | 反证 |
|---|---|---|---|
| L6-11 | `tests/*.sh` 以 100644 提交是"已知未修的缺陷"、接入自动化会静默失效 | **驳回**（降 info） | 全部 37 drills + 17 lib 脚本也是 100644，100755 仅 4 个入口点 ⇒ 644 是本树一致约定；`Run: sh tests/lint-drills.sh` 是约定表述 |
| L6-12 | Runtime 标称值（~11–22min）虚高、实跑普遍 1–3 分钟、过高标称抑制该跑时跑 | **驳回**（降 info） | 两波均**并行**执行（N=1 全并行 + grow `-j 4`），单 drill 占 36 分钟窗口的 22 分钟毫无矛盾，反而与 96(~22m,08:12 收尾)/97(~16m,08:10) 吻合。存活的只有中性观察：30/37 缺耗时元数据 |
| L5-7 | README 表对 90/91/92/93 的 INCOMPLETE 已过时、"owner 无法判断 GREEN 是进步还是漏跑" | **半边驳回**（整体降 trivial，仅 94 的 ps-LOST overclaim 存活并已并入 D6） | `README.md:296-300` 的免责声明**已预告**该结果：「historical round-3 snapshot, superseded by strict round-4 … **it is not an acceptance target** … A current run is judged solely by its actual unique verdict」。且当前期望表是 :274-294 那张，90-97 不在其中 |
| L6-1 | 旗舰例证 `73:173` 是"真实产品缺陷被 warn 吞掉、drill 仍判 GREEN" | **驳回该例证**（条目主体保留为 H4） | 该 warn 实在 :344，上下文明写 `Q-xcheck FAILED above = the RED`——缺陷已由前一条失败的 assert_ok 计入 assert_fail，drill 会落 ASSERT-FAIL。真实带进 GREEN 的是**覆盖度低报**（73:271/347、82:205/209、62），不是**缺陷被掩盖** |
| L6-7 | 期望值无独立机器可读列 ⇒ run-drills.sh 结构上无法自动差分；`RED-EXPOSES` 是"表外第六态"会误导 | **驳回**（降 minor，见 D5） | `README.md:302` 表头即 `| drill | expected verdict | scenario |` 三列，期望一律加粗；`RED-EXPOSES` 出现在 :288/:291 的**两列旧表 Proves 列**，是用途描述，不占 expected-verdict 列位 |
| L4-2 | "71/73/74 里所有 240s/180s/120s/90s 数据面判定实际只有 ~10s 窗口"；"#33 在 73 的量化结论被系统性污染" | **两条子主张均驳回**（条目主体保留为 H5，并补入被漏掉的模式 B） | 71 的 `_drain_migrated` 走纯 curl 无嵌套，窗口完整（这反而**加强** P1）；`logs/73:208` 实测 `≈19s` > 10s，自证未塌缩。真实语义：内层失败⇒外层即刻放弃并冒名报错；内层成功⇒**外层 deadline 无限延后 = 无界挂起** |
| L3-9 / 台账 #56 | `rotate-tunnel-cert` 给出"循环建议"、运维"被无限来回踢、永远到不了可执行状态" | **收窄**（见 P11） | `internal/broker/clusterdrain.go` 的 `RotateTunnelCert` 在同一处完整表述了真实要求并给出可执行出路 `transfer leadership to %s first so it can hot-swap its live tunnel certificate`。照做一次即可脱困，不存在死锁 |
| L1-4 | 「锁永不释放、没有任何命令能清它」 | **收窄**（见 P3） | 在 broker 主机上以正确 SID 起一个 agent 再重跑 upgrade 即可走完并释放锁。缺陷是该路径未文档化且与"colocated agent 可选"承诺冲突，而非绝对不可解 |
| L2-5 | 40 的 SETUP-RED "非时序" | **收窄**（见 H7） | 触发器**确实是**时序（同命令几百毫秒前刚成功）；准确表述是"时序竞态触发 + harness 在已知抖动窗口用单发解析 + 死掉的 fallback"。归类仍为 harness |
| L2-2 | `session create` 的失败机理是 `clusterwrite.go:799` 的 `not visible after commit (apply lag)` | **收窄为未证**（见 Q4） | 谓词 `>/dev/null 2>&1` 丢弃了所有 stderr 与 rc；存在更强候选 `cmd/tether/session.go:49-50` 的 5s 客户端超时 → rc=69（同 run D4b 正是 rc=69） |
| L2-6 | 96 的 B0（`run` 未被拦）证明 gateDestructive 门覆盖面不一致 | **驳回该对照**（open question 主体保留） | 96-B0 的告警由 kill brk2 产生，**不是** force_single_active，条件不同 |
| L2-7 | 95 的 not_covered reason「N=2 下 raft 与 JS 共命运」 | **驳回该断言**（条目保留为覆盖假缺口） | `scripts/install.sh:733-734` broker unit 对 nats-server 只有 `After=`/`Wants=`（非 BindsTo/PartOf），`systemctl stop nats-server` 不连带停 broker；`internal/broker/authcallout.go:36` `MaxReconnects(-1)` 也不会让 broker 因失联退出。且 `_d_raft_ok` 硬钉 `leader_id=="brk1"`，而 D1 前 brk1 的 broker 已被两次干掉（T1a SIGTERM MainPID 395、T2a SIGKILL MainPID 724），N=2 下 leadership 完全可能停在 brk2，注入前无基线 ⇒ **95-D（DELETING 会话 boot-resume，G.2(1)b）很可能是假缺口**。同 drill `:70` 已存在更宽松的 `_broker_live()` 用 `.leader_id != null` |
| L1-7 | 「全仓有 5 个 drill（51/91/97/95/50）用了正确的 not_covered」 | **数字不实**（结论保留） | 实测 **8 个**：22(1)/50(1)/51(2)/52(4)/91(1)/95(2)/96(10)/97(4) |
| L3-12 | 简报称「52 有 5 处 not_covered」 | **前提校正** | 实为 **4** 处调用点（:327/:440/:464/:493），本轮触发 **2** 处（:440 #55、:464 D-group）。:327 未触发因 A7d 真的证明了 re-pin（正面结果）；:493 因 :464 已 `drill_end; exit` 不可达。⇒ "5 处缺口"应收敛为"1 处高风险缺口(D-group) + 1 处已充分论证的缺口(#55)" |
| L6-1 | BATCH 未覆盖 22/37；"自检工具与要检的东西恰好错位" | **数字与叙事订正**（条目保留为 H4） | BATCH 是 **16** 个 ⇒ 未覆盖 **21/37**；且 `lint-drills.sh:15-24` 头注**明写**豁免并提供 `--all` advisory 模式，实跑 `--all` 能一次列出全部 24 处 + 另三类。工具知道且能报，只是 advisory 不 fail-closed |

---

## §F 最小修复集

### F.1 产品侧 P0（各自都是小改动，产品内已有现成代码可复用）
| # | 动作 | 关闭 | 复用点 |
|---|---|---|---|
| 1 | `recovery restore` 加 `--config`，落地即调 `applyClusterSeam` | **#51 / P2** | `cmd/tether/cluster.go:880`（含 fail-closed decode 校验） |
| 2 | restore 完成文案补两句（lone-voter+clustered conf 需 `reconcile nats --to-standalone --confirm-single`；fresh box 需 `reconcile nats --manual`） | **#64 / P4** + #52 提示半边 | `internal/broker/clusterstatus.go:354` 已有现成 remedy 文案 |
| 3 | doctor 的 DB 检查加一行 `db.Ping()` | **#50 / P5** | `internal/clusteroffline/doctor.go:82` + `storage.go:105-111` |
| 4 | doctor 的 check 列表接上已存在的 `readClusterPublicIdentities` | **#54 facet 2 / P6** | `cmd/tether/cluster_secrets.go:32-47` |
| 5 | `cluster drain` 在 migrateExposes 后主动弹开受影响 agent 连接（或新增 home-push 通道）；在此之前对含 rebuild-ON expose 的节点**不得返回 0** | **P1（发布级）** | `internal/agent/agent.go:1499-1516` ReconnectHandler；`proxy_reconcile.go:360` 已有 push 形态 |
| 6 | `install.sh` 所有不需展开的 heredoc 改 `<<'EOF'`，逐个审计需展开的 `$`/反引号 | **P9** | — |

### F.2 harness P0（不修则本层测试结果不可作为发布依据）
| # | 动作 | 关闭 |
|---|---|---|
| 7 | 30：`_do_roll` 判 rc + 失败时 dump roll.log；`_ver_of` 改 `.brokers[]\|select(.NodeID==$n)\|.BrokerVer`；`_dry_run` 改用 host-count 而非弱 grep；终末 warn 按 MECH 参数化；OQ-6 腿改 `not_covered` 或真补供给 | **H1/H3/H16a/H16c** |
| 8 | 51：G1-clear 照抄 5 字段 seam **并加 decode 后置断言** | **H2**（一次修复解锁 #52/#53/H2-terminus 三块覆盖） |
| 9 | 93：修两处 jq 括号；失败路径 dump hook.log；card 诊断去 `head -8`；拆四段复合断言；CARD/JSON 改 poll+DIAG | **H13**（并使 webhook wire schema 首次获得覆盖） |
| 10 | 42 加 `--ack-alerts`；40 的 :220 改 poll 重试 + `sim_leader` fallback 遍历存活节点 | **H7/H8**（各恢复一整段核心覆盖） |
| 11 | `poll_until` 改唯一变量名或保存/恢复；审计 8 处嵌套点（尤其 `wait_phase`） | **H5**（含 grow/retire 家族的潜在无界挂起） |
| 12 | lint：BATCH 默认全量；增加 jq 可编译性 / 空针 / 描述串反引号三条规则；优先清偿 8 处 combined-signal-trap 与 3 处 sigpipe-truncation | **H4/H12** |
| 13 | 24 处 `warn "…NOT-COVERED"` 改 `not_covered`；62:118 改 `not_covered` 并关联 OQ-2；#25/#26/#27 旁加 `product_red`；waiver 改 per-drill 粒度 | **H4(a)(b)(c)(d)** |

### F.3 定案实验（成本 3 次定向复跑，可一次性关闭 4 条 open question）
| 实验 | 修完哪几条后跑 | 需额外采集 | 定案 |
|---|---|---|---|
| E1：单跑 30 | #7 | roll.log 全文 | **P3** 从 likely → confirmed/refuted（区分"agent leg HALT"与"roll 全成、oracle 坏"）。**注**：P3 的核心主张已可由 `renderUpgradePlan` 的 host-count 静态推论独立证成，E1 只定"本次停在哪一步" |
| E2：单跑 96 D/F 臂 | #9 风格的 DIAG 纪律 | brk1 broker journal（找 `authcallout: handle failed`）+ tether-broker 存活探针 + 路由快照；`ps -a` 全量 + agt2 journal；`history --kind proc -n 1000` | **Q2**（少数派黑洞机理）、**Q3**（exec 成功但 proc 表不收录）、**H14 的 F5 半边**（G.5 审计破口 vs 读窗口）、**H6/#65 取真值** |
| E3：单跑 52 D-group | 恢复循环加诊断 | **nats.conf 的 issuer + broker.err 双采** | **Q1**（三假说 A/B/C 分离；若 A 成立则为 blocker 级新产品缺陷） |

---

## §G 本轮实际未测到的面（不得当作"已覆盖"计入发布判定）

| 面 | 未测原因 | 归属 |
|---|---|---|
| G5 滚动升级机制（版本是否真翻、dry-run 是否真未触碰主机） | H1 oracle 恒空 + H3 rc 丢弃 | **净覆盖为零** |
| whole-host 升级判据（broker+同机 agent 双到版） | OQ-6 供给从未实现（H16c） | 结构性未覆盖 |
| retire 中途换主后的收敛（#45 NATS_ROLLED_OUT / #37） | H7 提前 abort | 空白 |
| rejoin resnapshot audit-window + `--accept-audit-loss` + 不复活陈旧 peer + join approve 并行 | H8 提前 abort | 空白 |
| 全灭 DR 尾段：#52 fresh box nats.conf 无 auth_callout / #53 JetStream 不随 bundle 回来 / H2 DR terminus（灾前 expose 能否吐回原 sentinel） | H2 seam 写不全 | **tether 的全灭 DR 至今从未被端到端证明过一次** |
| 52 D-group（C7 guided rotation 的 retire/alert 生命周期） | Q1 未定案 | 空白 |
| 74 Arm C：auto-rebalance 路径（G7a m11） | H11 前置门失败 | 空白 |
| 95-D：DELETING 会话的 boot-resume（G.2(1)b） | L2-7 谓词过严（**很可能是假缺口**） | 被误记为结构性不可达 |
| 62 Arm 2：true-D + mode:off-without-safe | OQ-2（已登记）+ H4(b) 记成 PASS | 已登记缺口，记账错误 |
| 73：#30 事件的 operator reader + quorum-separation 分支 | H4(a) warn 绕过计数器 | 已叙述、未计数 |
| webhook on-the-wire JSON 契约（schema/schema_version、transition=cleared、no-secret key 白名单） | H13 jq 坏 + hermetic 从不读 body | **三层测试全无覆盖，且触及安全承诺** |
| `ps` 的 LOST 派生态 | D6 overclaim | 声称已覆盖、实为零断言 |
| usage §6.1 systemd --user / linger 部署形态 | 82 环境限制（真实）+ H4(a) 记账错误 | 已叙述、未计数 |
