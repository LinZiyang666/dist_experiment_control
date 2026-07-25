# `docs/reviews/s7-s9-plan.md` — G-C deploy-tier drill plan (S7 + S9)

> Finalized by the main process, 2026-07-17. **7 drills, one merged plan, one external review** (execution group **G-C** per roadmap §2.1 — the last group; S7+S9). Drafted by a 13-agent adversarial Stage-A workflow (6 fixed-lens drafters + 6 fixed adversarial critics + 1 synthesizer, all Opus 4.8, agent count static per CLAUDE.md), then main-process adjudicated. **§11 records the finalized rulings on the 12 open items (A–L) the synthesis surfaced.**
>
> The `critique:feasibility-sim` lens **died mid-response (API error) and produced nothing** — its subject matter (what can actually be injected inside docker/systemd/netns) was the highest spike-density area in this plan. Rather than re-run it, the **main process closed its five named questions by direct measurement on `weilandserver`** before finalizing (§12: SB-FAULT / SB-VAULT / SB-97-1 CLOSED; SB-96-1 / SB-51-UP1 remain Stage-B with documented fallbacks). §11-L records that ruling.
>
> 结构：§0 范围/依赖/环境 · §1 共享 S0 + harness 增量 + 横切 R-规则 · §2 S7(50/51/52) · §3 S9(94/95/96/97) · §4 gotcha ledger(#50+) · §5 NOT-COVERED + 显式裁剪 · §6 OQ 解决 · §7 inventory 消费 + **S1–S9 收官闸** · §8 run-drills/基线 · §9 per-drill false-green 头注 · §10 Stage-B spikes · §11 主进程定稿裁决 · §12 spike log。
>
> 所有 identifier/命令/签名/注释英文；分析中文。破坏性臂逐条给五要素；一切 rehome/失效/撤销/恢复/quorum oracle 收在**真流量恢复**或**对照源对比**，绝不 status 字段。**只测不修：产品缺陷登 gotcha #50+ 并 signature-guarded RED，绝不改产品 Go 代码。**

---

## §0 范围、依赖、环境

### §0.1 Drill roster

| drill | 批 | N | 主题 | 预期 landing | 债 | 预计 | key blocker |
|---|---|---|---|---|---|---|---|
| **50**-backup-restore | S7 | 2 (+agt+ctl) | backup(leader/follower/offline) · restore 门族 · doctor preflight · incident export | **PRODUCT-RED（确定，#50 已实证）** | — | ~15min | — |
| **51**-full-dr | S7 | 3→全灭→1→2 | runbook §5.2 全灭 DR 四步逐条 + 计步呈现 | **PRODUCT-RED（高概率）** #51/#52 | re-grow #31-gated | ~18min | #31 |
| **52**-credential-rotation | S7 | 2 (+agt+ctl) | rotate-tunnel-cert · account.nk+CA 轮换 · keygen · C7 guided rotation | **PRODUCT-RED（必现）** #54 | C7 脊 #31-gated | ~12min | **#31（既有）** |
| **94**-agent-reconcile | S9 | 1 (+2agt+ctl) | G.1 missed-exit + orphan（产品路径造法）+ G.5 proc/port 审计 + `ps` LOST | GREEN（#61 候选） | — | ~11min | SB-94-RESTORE |
| **95**-broker-selfheal | S9 | 2 (+agt+ctl) | G.2 重启对账 + **#23 判别性行为证明** + DELETING 续跑 | GREEN | DELETING 腿 | ~14min | SB-95-DELETING |
| **96**-mid-flight-chaos | S9 | 3 (+agt+ctl) | tier-B/PTY/expose 中断 + 网络分区 + 双故障 | **PRODUCT-RED（源码确证）** #57/#58 | #59/#62 候选 | ~22min | — (SB-FAULT CLOSED) |
| **97**-soak-cycles | S9 | 3 (+2agt+ctl) | 参数化混沌 soak；fd/RSS 泄漏有界性 | GREEN（阈值 UNCALIBRATED） | goroutine 口（批级登记） | ~16min | 阈值校准 |

串行 ~108min；批内 `-j 2` 分波 makespan ≈ 50min（94 是 N=1 族、可与 grow 波并行）。

**G-C 结构上不会 ALL GREEN，也不该。** 50 恒 PRODUCT-RED（#50 已实证）、96 恒 PRODUCT-RED（#57 源码必然）、52 高概率。README drill 表 + roadmap 标签**必须**如实标注（`lib/assert.sh:27-37` 的 OWNER/RELEASE POLICY），**不得写 GREEN 主体**。这直接推动 §11-K-1 的 roadmap 出口措辞订正。

### §0.2 只测不修铁律

同 s6-s8-plan §0.2。三条 G-C 专属加强：

1. **定格证据必须来自 drill 真跑**，不得以 hermetic Go 探针代替。
2. **`lib/assert.sh` 的五态真值表 + `tests/verdict-contract-test.sh` 是外审三轮定的 SSOT，本批不动**（§11-B）。
3. **`image/*` 增量仅限 `iptables` 烘焙一项**（§11-D，SB-FAULT 已实测），且必须带 Mandate ③ 头注。
4. **G-B 血的教训继承**（inventory G-B landing 段）：① harness 的 oracle 能制造假的产品故障（drill 91 把 mutating `force-single` 管进 `grep -q` → SIGPIPE 腰斩 CLI → 被误判为产品缺陷）→ R-SIGPIPE；② **计划外的产品手术风险极高**（G-B 在外审 round-4 改产品代码，随后 5 轮外审在这批新代码上连爆 darwin 构建断裂 / root-owned raft 永久拒启 / 锁 chown 跟随符号链接=本地提权）→ **G-C 严守零产品 Go diff**；③ harness 函数绝不裹进 `sh -c` → R-NOSHC。

### §0.3 环境事实（2026-07-17 主进程亲测）

- `ssh weilandserver`（weiland@192.168.1.150）**可用**：88 核 / 251 GB / inotify=8192 / 0 容器在跑 → **`remote.sh` 全路径（build + rsync + docker build + drill）可用**。**G-B 的 tether-exec-only / 镜像冻结约束已解除** ⇒ 本组允许 `image/*` 增量。镜像已用当前 HEAD 重建（baked nats-server 2.10.22 == install.sh pin，`cmd_drill:526-531` 的 stale-image fail-closed 守卫据此校验）。
- 镜像实测**无** `iptables`、**无** `nft`、**无** `ss`，**有** `/usr/sbin/tc` + `/sbin/ip`（`Dockerfile:18` 只装 `iproute2`）。容器 `--privileged`（`lib/docker.sh:49`）+ 独立 netns。
- **SB-FAULT CLOSED（实测）**：容器内 `apt-get install iptables` → `iptables v1.8.10 (nf_tables)` 可用；`iptables -N SIMFAULT` + INPUT/OUTPUT jump + 双向 `--dport`/`--sport` DROP → 目标端口 `</dev/tcp/…>` 退 **124（挂起=静默丢包）**、同 peer 的非过滤端口退 **0（选择性对照源成立）**、`iptables -F SIMFAULT` 后立即回 0。⇒ **P1 采纳**（§11-D）。
- **SB-VAULT CLOSED（实测）**：`docker cp` 容器(tether-owned)→host ⇒ 落 **`weiland:weiland` 0644**、免 sudo 可读、sha256 可算。host→容器 ⇒ 落 **uid 1000**（**不是** root）⇒ `vault_push` 仍须 chown 到 tether（结论同综合稿，理由订正）。**bind-mount 不需要**（§11-C）。
- **SB-97-1 CLOSED（实测）**：镜像有 `/var/log/journal`（持久 journal）、无 `/run/log/journal`；`node_kill`/`node_start` 保留容器与写层 ⇒ **journal 跨 boot 存活** ⇒ 出口 (a)：`--since` 跨 boot 检查成立。
- 37 drill 全并发 ≈ ~150 容器 ≪ ~600 上限 ⇒ **瓶颈 100% 是 grow-timing，不是资源**。

### §0.4 组内顺序约束

- **G-C 是最后一组** ⇒ 独占「零未勾行」收官义务（§7 的 `G-C-SWEEP` + `S1–S9 CLOSURE`）。
- **S7 软依赖 S6 fixture 经验**（`setup_forcesingle_n2` / `grow_to_3` 的 attempt-accounting）——plan-time 约束，非 runtime 依赖。
- **S9-97 压轴**，且本组另交付**全套并发 3 连基线**（§8）。
- **`SOAK_CYCLES` 默认 = 6**（斜率判据的结构下界，§3.4）。

---

## §1 共享 S0 landing + harness lib 增量 + 横切诚实规则

### §1.1 S0 landing check（读代码核实，不信文档）

**已落地（G-C 不得重复实现）**：

- **S0-pty**（S1）：`image/pty-run.py` + `image/pty-confirm.py` 已烘（`Dockerfile:52-53,57`）→ 50/51 的 typed-confirm、94-B 的 orphan 载体、96-B 的 PTY 混沌。
- **S0-台账**（S1）：`docs/deploy-tier-gotchas.md` + `command_tree_inventory_test.go` + 两份 golden。
- **S0-隧道**（S2）：`drills/lib/agentyaml.sh` 的 `agent_provision_yaml` 写 `tunnel_addr`；`image/provision-node.sh:39-45` 把 install.sh 的 `host: 127.0.0.1` 改写成 `0.0.0.0` ⇒ **51 的 fresh-box 臂只有走 `$SIM up`（跑 provision-node.sh）才成立**，drill 头注须钉这条。
- **S0-ingress**（S2）/ **S0-artifact**（G-A）/ **S0-布局**（G-A）：G-C 全不需要。
  - ⚠ **roadmap §2 的 S0 表状态列对 S0-artifact / S0-布局 陈旧**（仍写「未落地」，实为 G-A 落地：`drills/lib/artifact.sh` 在、`32-install-lifecycle` 已断 agent `~/.local/bin` 布局）→ **本 plan 顺带订正该两行**（文档同步，非新实现），否则下一读者重复实现。§11-K-2。

**本组落地（两项，一次建全）**：S0-备份库（§1.2-1）+ S0-故障原语（§1.2-5）。

### §1.2 harness lib 增量（除 `iptables` 外全 rsync-class）

| # | 增量 | 文件 | 类别 |
|---|---|---|---|
| 1 | **S0-备份库** `vault_init/vault_pull/vault_push/vault_has/vault_sha` | 新 `test/simcluster/lib/vault.sh` | rsync |
| 2 | nuke 接线 | 改 `simcluster` 的 `cmd_nuke`（加 `rm -rf "${BACKUP_VAULT:-$HERE/backups}/$INSTANCE"`）+ `.gitignore` 加 `backups/` | rsync |
| 3 | 轮换代 | 改 `lib/secrets.sh`：`secrets_mint_gen <inst> <gen>` + `secrets_distribute_gen`（写 `_shared-gen<N>/`，**gen1=`_shared` 永不动**）+ `secrets_remint_route_only` + `secrets_mint_tunnel_only` + `secrets_tunnel_fp` + `secrets_push_file` | rsync |
| 4 | `grow_to_2` | 新 `drills/lib/cluster.sh` 函数（`grow_to_3` 的兄弟；含 JS meta `cluster_size==2` **无条件**守卫 + `sim_leader==brk1` 硬断） | rsync |
| 5 | **S0-故障原语** `fault_partition_on/off` · `fault_assert_blackholed/reachable` · `fault_freeze_on/off` · `fault_cleanup_all` · `dp_curl_blackholed` | 新 `drills/lib/fault.sh` | **image**（`Dockerfile:18` += `iptables`） |
| 6 | 事件观测 | 新 `drills/lib/events.sh`：`ev_sub_start/ev_ready/ev_seen/ev_stop`（**提炼 `drills/81-admin-evict-session-rm.sh:62-66` 的活体配方，非新发明**） | rsync |
| 7 | 泄漏 oracle | 新 `drills/lib/leak.sh`：`leak_baseline/leak_sample/leak_verdict` | rsync |
| 8 | **收工闸** | 改 `tests/lint-drills.sh` 的 **`BATCH` 硬编码列表** += 七个 G-C drill（**实读确认现为 `22 40 41 42 43 90 91 92 93`；漏加 = 七个新 drill 完全不受静态假绿闸约束 —— 最容易漏、后果最重的一条**） | rsync |

**(1) S0-备份库 —— 生命周期元组（roadmap §2 强制字段）**

| 字段 | 定稿 |
|---|---|
| 归属批 | **G-C / S7** |
| 消费批 | S7-50（`--out` 目标）、S7-51（**唯一**要求存活于 `rm_node --vols` 的消费者）、S9-94（借用 leader bundle 造 orphan） |
| 实例作用域命名 | `: "${BACKUP_VAULT:=$HERE/backups}"` → `$BACKUP_VAULT/$INSTANCE/<bundle>/` —— **逐字镜像 `lib/secrets.sh:13` 的 `SECRETS_STASH` 形态**（同一生命周期已有完美先例：host 侧、per-instance、gitignored、nuke 回收） |
| 创建预检 | `vault_init`：① `rm -rf "$(vault_dir)"`（存在则先 `warn` 出路径+mtime 作证据）② `mkdir -p -m 0700` ③ **断言空**。**采纳 rm-then-assert-empty、驳回 fail-on-exists**：`INSTANCE` 是确定性的 `drill-<name>`，`cmd_drill` 在 `SIM_KEEP=1` 时跳过 nuke 且其 trap **只捕 INT/TERM 不捕 EXIT** ⇒ 一次人工检视 = 永久 SETUP-RED。fail-closed 用错了地方；③ 才是防「残留 bundle 让灾后可读臂假绿」的那条 |
| 密钥/信任材料 | **零**。bundle 按产品设计不是凭据（`internal/clusteroffline/restore.go:12-13`；manifest 只放指纹 `manifest.go:62-66`）。信任材料的 DR 角色由**既有 `SECRETS_STASH` 承担**（= 运维密钥库），不新建 |
| 健康检查 | `vault_pull` 后：`test -s state.db && test -s manifest.json` + `jq -e .self_id` + **记 sha256 → 灾后重算比对**（= 51-C 的「离簇存活」唯一 oracle）+ 语义检查 `.self_id/.source_role/.roster|length` |
| 最终清理 | **仅 `simcluster nuke`**。`cmd_down -v` 走 `rm_node --vols`（只删 `sim-<i>-<n>-{etc,lib}` 两卷，`lib/docker.sh:93-99`）= 模拟卷灾难，vault **必须存活** |
| 搬运方式 | **`docker cp`（SB-VAULT 已实测，§0.3）**。容器→host 落 `weiland:weiland` 0644、免 sudo 可读可 sha256；host→容器落 uid 1000 ⇒ `vault_push` 须 chown 到 tether。**驳回 bind-mount**（引入宿主目录被容器 uid 写的 R-NO-HOST-LEAK 面）。`secrets_distribute` 已是同一 idiom 的先例；**且 `scp` 备份下机正是真运维** |
| **Mandate ③/④ 说明** | 挂一块备份盘并 chown 给服务用户 = **运维/部署的活**；把 bundle 搬出机器再搬回 = `[operator per runbook §5]`。sim **只提供机器**：`cluster backup` 自己写 bundle、`recovery restore` 自己读 bundle。**且**：50 的臂 C 先按 runbook:524 字面跑一遍并**如实呈现它失败**，vault 才作为 `[env]` 出场 ⇒ **供给没有掩盖任何缺口**（R-SUPPLY-ORDER） |

**(5) S0-故障原语 —— 三件 + 语义表**

- **卷级件（S7）**：`rm_node <n> --vols`（既有，`docker.sh:93-99`）+ in-place `disaster_wipe_data`（50）。**「保留-secrets 变体」无需新代码**：`secrets_mint_node` 在 `route-cert.pem` 存在时短路（`secrets.sh:53`）⇒ `rm_node --vols` + `up` 同 node-id → `secrets_distribute` 重注入**同一** tunnel-cert = runbook §5.2 step 1 的忠实复现（**且这正是 restore 的 provenance 锚所要求的**，`restore.go:237-245`）。
- **弃-secrets（51 的 provenance 负例）**：**不新增 `secrets_forget_node`**（它会删掉正例赖以存在的**唯一**一份原 tunnel-cert = 不可逆顺序炸弹）。改用 `secrets_mint_node "$INSTANCE" brk1-remint` —— **实例命名空间内的新 node-id**，`cmd_nuke` 一并回收，证据强度等价（新铸 = 新 fp = provenance 门必拒）。**驳回把 stash 指到 `$SCRATCH`/`$INST-remint` 的写法**：`$INST-remint ≠ $INSTANCE` ⇒ nuke **永远删不到** ⇒ 每跑一次 51 泄漏一棵密钥树到 weilandserver（R-NO-HOST-LEAK 违反）。
- **分区件（S9）**：`fault_partition_on <node> <port>…` = 目标 netns 内 `SIMFAULT` chain（`-I INPUT/-I OUTPUT` jump），每端口 **`--dport` + `--sport` 双向 DROP**（缺 `--sport` ⇒ outbound-established 的 route/raft 仍收得到 ⇒ 单向「分区」= 更弱的假注入）。**配方已由 SB-FAULT 实测**（§0.3）。
- **冻结件（S9）**：`fault_freeze_on <node> <unit>` = `kill -STOP $(MainPID)` —— **第三种语义**（socket 开着、内核继续 ACK 进填满的接收缓冲、systemd `Restart` 永不触发）= 「卡死但活着」的 broker。

**端口语义表（每臂按需选，绝不多切）**

| 端口 | 面 | 切它 = | 用于 |
|---|---|---|---|
| `6222` | NATS route | mesh 断 | 96-D（与 7400 成对） |
| `7400` | tether raft | raft 复制断 | 96-D |
| `4222` | NATS client | 客户端侧断 | **96-D 刻意不切** = 选择性对照 |
| `7000` | 反向隧道 | 数据面断 | 52-A7 的 redial 触发；96-F 可选 |
| `8223` | NATS monitor（**loopback**） | — | **永不切**（观测源） |

**DROP vs REJECT 的可测判别子（不是口头约定）**：`fault_assert_blackholed` 断 `timeout 3 bash -c '</dev/tcp/h/p'` 退 **124（挂起）**；`lib/docker.sh:88` 的 `tcp_refused` 断的是**立即失败**。两者互斥 ⇒ **「我们注入的是分区不是宕机」本身是一条 GREEN 断言**，且它 fail-closed 地防住了「有人把分区臂改回 `docker network disconnect`」（disconnect 的文档契约只保证摘接口 → 立即 `EHOSTUNREACH` → 必退 1 ≠ 124）。

**`dp_curl_blackholed`（新，承重）**：`dataplane.sh:44-52` 的 `dp_curl_refused` 实现是 `[ "$?" -eq 7 ]`。**DROP 分区下 curl 得到的是 exit 28（`--max-time` 超时），不是 7** ⇒ **armed DROP 窗口内一律禁用 `dp_curl_refused`**，改用 `dp_curl_blackholed`（exit 28）。头注写清 `REFUSED(7)=进程死 / TIMEOUT(28)=分区` 是互斥判别子，与 `fault_assert_blackholed` 的 124 同源。

**Mandate ③/④ 头注模板（`fault.sh` 逐字）**
```
# MANDATE ③/④ — these are INSTRUMENTS (cable-pull equivalents), never accommodations.
# iptables is the containerized equivalent of an operator unplugging a cable or a switch dropping a link.
# Supplying the machine with standard kernel tooling is PROVISIONING (the sim's job), exactly like docker,
# systemd, or --privileged. It works around NO tether defect: no rule exists outside an explicit injection
# window, every rule is removed by an unconditional trap, and tether never reads netfilter.
# MANDATE (4) self-check: a silent partition is HARDER on tether than `docker network disconnect`, not easier —
# it removes tether's ability to notice the failure quickly. If a drill ever needed a rule to make a tether
# command SUCCEED, that is a defect to expose, not a rule to keep.
```

**(3) `secrets_remint_route_only` —— 承重设计订正**

Stage-A 草案原定义为「重铸 route **+ tunnel** leaf」，而臂 B4 随后 `systemctl restart tether-broker`。**新 tunnel leaf 的 fp ≠ `cluster_nodes.cert_fp` ∧ ≠ `cert_fp_prev` ⇒ `wireClusterEarly` 返错（`internal/broker/clusterwrite.go:173-190`，串 `matches neither the pinned …`）⇒ broker 拒启 ⇒ `Restart=always` crash-loop** —— 这正是同一 drill 的臂 A8 刻意造的砖，会在**两个 broker 上同时**无意重造，把 #54 的证据淹没在一个 harness 自造的红里。

**定稿**：函数名 `secrets_remint_route_only`，**只**重铸 route leaf + 替换 `account.nk` + `cluster-ca.pem`，**绝不碰 tunnel-*.pem**。头注逐字：
```
# NEVER touches tunnel-*.pem — the tunnel trust anchor is the cluster_nodes fp PIN, not the CA
# (internal/broker/clusterwrite.go:173-190; tunnel fp = sha256(cert.Raw), issuer-independent, tls.go:91-94).
# Re-minting it bricks the broker on next start. Tunnel rotation is `rotate-tunnel-cert`'s job (arm group A).
```
语义正确：旧 tunnel leaf 在新 CA 下**依然有效**（fp 与签发者无关）；runbook §2.1 的剧本本来就把 tunnel 轮换与 account/CA 轮换分开。

### §1.3 CA gen2 vs 实例-CA facility owner 规则（承重冲突，已解）

inventory §3 立规：实例 CA 由首个落地者（**S2**）成为设施 owner，后开批**复用、绝不重铸**（`drills/lib/ingress.sh:17-18` 写死）。而 52 的存在理由就是重铸 CA。

**解**：① 52 **不得**用 `secrets_ensure_shared`（它在 `cluster-ca.pem` 存在时短路，`secrets.sh:22`）；② `secrets_mint_gen` 写 `_shared-gen<N>/`，**gen1 = `_shared` 永不动** ⇒ owner 规则不破；③ **52 拓扑 MUST NOT 用 ingress/artifact**（N=2、无 `/sub`/manifest 腿）——否则 gen2 轮换会打断 sidecar 的 gen1-CA trust，制造**假 RED**。plan 写死这条排除。

### §1.4 横切诚实规则（引用 s3-s5-plan §1.A / s6-s8-plan §1.3 的 R-系列，仅记 G-C delta）

- **R-EVENTS（推翻 G-B 的 D2 carve-out，G-C 不继承）**：`internal/auth/permissions.go:147`（`PermissionsForActivatedMember`，Sub 块 :135-165）与 `:36`（`PermissionsForUnactivated`）的 **Sub allow 含 `tether.v2.sys.events`（全局 subject、无 sid 通配）** ⇒ **activated member 的 ctl 凭据可 core-sub 全部 sys.events kind**。活体先例：`drills/81-admin-evict-session-rm.sh:63`、`80:42-53`。G-A/G-B 的 D2 前提「无 reader」把 **JS-API 权限**（`nats stream ls` 被拒）错推成 **core-sub 权限**。
  - **G-C 立场（三分框架，不一刀切）**：① 产品**是否发**该 event → **member core-sub 可测 → 断言**；② operator 有无**一等 CLI reader** → **无 → 独立登记 DOC-26**；③ G-A 的 proxy/rehome kind 的 NOT-COVERED **结论可能仍对，但理由「无 reader」源码级为假** → **交 G-C-SWEEP 逐行裁，不越权改 G-A 台账**。
  - **`emitDrainEvent` 通道已定格**：`internal/broker/rehome_events.go:9-11` 逐字「All emits go through **b.pubSysEvent**（stamps onto the existing **proto.SubjSysEvents**）」⇒ `home_reassign_*` / `rehome_stalled` / `broker_down_rehome_summary` **都落 `tether.v2.sys.events`、member 可读** ⇒ **inventory row 54 不得判 NOT-COVERED-as-unreadable**。
- **R-BROKERLOG（一票否决级）**：`scripts/install.sh:756-757` 把 broker 的 stdout/stderr 重定向到 `/var/log/tether/broker.{log,err}`；`image/units/` **只有** `tether-agent.service` ⇒ broker unit 来自真 install.sh ⇒ **`journalctl -u tether-broker` 里没有任何 broker payload**（仓库既有明文：`drills/42-rejoin-returning.sh:31-33`）。
  - **定稿**：一切 **broker 应用层签名** → `dexec <n> -- tail -n <N> /var/log/tether/broker.err`（照抄 42 的 `_rj` idiom）；**systemd 生命周期语义**（`Deactivated successfully` / `Main process exited, code=` / `Scheduled restart job` / `Result=`）才留 journal。**agent 侧不受影响**（sim agent unit 无重定向）。
  - 命中并已修正：50-J1/50-O、51-G1/G2、52-A8、97 的 journal-clean（对 `tether-broker`）。
- **R-SUPPLY-ORDER（新，S0-备份库的落地条件）**：`[env, S0-备份库]` 的每一次出场，drill **必须先有一条断言呈现「不供给时 tether 做不到什么」**。50-C（runbook:524 字面例子）是 vault 的前置证据，不是可选装饰。
- **R-TRAP（新，三条硬规矩）**：
  1. **元素⑤ = 一条 `trap … EXIT INT TERM` 语句，不是臂末代码。** 理由：`lib/assert.sh` 的 `assert_setup`/`setup_fail` **直接 `exit`** ⇒ 注入之后、恢复之前的任何 setup 失败都会让臂末清理永不执行。
  2. **每 drill 恰好一条 trap。** 理由：`drills/32-install-lifecycle.sh:29-34` 的 R5-M4 血泪 —— 两个 `trap … EXIT` **静默互相覆盖**。形如 `_cleanup() { fault_cleanup_all; kill "${BGPID:-0}" 2>/dev/null; …; true; }` + 每步 `|| true`（**清理失败只 warn，绝不改 verdict**）。
  3. **顺序 = `. lib/*.sh` → `drill_begin "<name>"` → `trap '_cleanup' EXIT INT TERM` → SETUP。** 理由：`drill_begin` 在非-`drill-*` instance 上 `exit 2`（trap 先注册 = 在可能是持久实例的上下文里跑清理）；且 `_AS_DRILL` 未设时 `dataplane.sh:15` 的 sentinel token 退化。**每 drill 必须显式列这一行。**
- **R-BOUNDED-PROBE（新）**：**armed 故障窗口内的每个探针必须自带上界**（`timeout N` / `--max-time`）。理由：`run-drills.sh` 的 `run_one` **没有 per-drill timeout**，`cmd_drill` 的 trap 不捕 EXIT ⇒ 一次挂起 = suite 永挂 + 规则永不撤。`poll_until` 自己有 timeout，但**它调用的谓词若永不返回，poll_until 也永不返回**。
- **R-NOSHC**：**harness 函数绝不裹进 `sh -c`** —— 新 shell 不继承函数。反例：`poll_until 30 1 … -- sh -c '! dexec agt1 -- pgrep -f "sleep 9199"'` → `dexec` 找不到 → `!` 取反 → **恒真 → 永久假绿**。改 `_no_orph() { ! dexec agt1 -- pgrep -f 'sleep 9199'; }` + `poll_until … -- _no_orph`。
- **R-SIGPIPE**：mutating tether 命令绝不管进 `grep -q` → 用 `out_matches`（`lib/assert.sh:155-163`）。G-C 的 `cluster backup` / `recovery restore` / `cluster retire --compromised` 全是多步 mutating 命令，**高危**。
- **R-INVERTED**：反极性缺陷（**命令 exit 0 但本该拒/本该写**）一律用既有惯用法 —— `assert_ok`(谓词) + 裸 `product_red`(记账) 的分支写法。仓库内 11 处在用（`40:208`、`80:159`、`22:216`、`90:186`、`91:61,116`、`92:66,125`、`93:185`、`41:212,222`），零漂移。**不加新原语**（§11-B）。
- **R-EXHAUST（新）**：INVERTED 块**四态穷举，绝无 `else` 兜底**（`''` ⇒ UNJUDGEABLE / 已修 ⇒ APPEARS-FIXED / 命中 ⇒ product_red / 其它 ⇒ UNJUDGEABLE）。
- **R-LIVENESS-NOT-HEALTH（新，实测血泪，§12 SB-50-LANDING）**：**`cluster status` 的退出码是 HEALTH 不是
  LIVENESS**（0=healthy / 1=DEGRADED / 2=QUORUM_LOST / 3=FORCE_SINGLE，`clusterstatus.go:66-101`）。restore
  后 / DR 后 / force-single 后的单 voter **恒 DEGRADED** ⇒ 拿退出码当存活探针会对着一个**完全健康**的 broker
  poll 到超时、**伪造出一个产品故障**（本轮真的发生了，50/51/94 三处同款）。**存活 = 答出可解析 JSON**：
  `… cluster status --json 2>/dev/null | jq -e '.leader_id != null'`。
- **R-FIELDNAMES（新，实测）**：**同一个概念在不同 API 用不同键**，查错键 = 静默不匹配 = 假的产品故障：
  `node ls --json` → **`.nodes[].nid`**（`proto/messages.go:379-385`）；`cluster status --json` → `.node_id`；
  `ps --json` → `.processes[].pid` = **ULID**（不是 OS pid！audit 的 `pid` 同此，`exec.go:415`）；
  `expose explain --json` → **`.home_broker`**（不是 `.home`）且 **`epoch` 是 omitempty**（为 0 时**不出现**
  ⇒ 必须 `// 0`）。**一律以源码 struct 为准，不凭记忆、不照抄文档。**
- **R-DIAG-OUTSIDE（新，实测）**：**诊断必须打在 `assert_*` 外层**。`_as_capture` 把 stderr 收进 `_AS_OUT`、
  失败时只回显 `tail -3` ⇒ 写在被断言函数**里面**的诊断会被吞掉（本轮第一版正好藏起了它存在的意义）。
- **R-ASYNC-POLL（新，实测）**：restore/DR/rehome 后的一切读**都是异步收敛**（且会排在 #64 实测 ~73s 的
  crash-loop 后面）⇒ **一次性直读必然与之赛跑**。同一断言、诚实的时间窗：一律 `poll_until`。
- **R-CTX（新，实测）**：`session create` 会**激活**新 session。同一性 oracle 的阴性半边（建完就要被 restore
  回滚掉的那个 session）建完**必须立刻把 ctl 切回主 session**，否则 ctl 停在一个将被销毁的 session 上 ⇒
  后续每条调用 `Authorization Violation` ⇒ **成片的级联假失败**（本轮 4 条）。
- **R-PIN-HOME（新，实测）**：**多 broker 下 `expose` 不带 `--on-broker` 就是抛硬币**（实测 6 跑 3 败）——
  home 落到 agent 的非-tunnel broker 时 allocate 当场失败（`agent_rejected:frpc_failed` = **#29** 的
  allocate-time 面）。50/51/52 一律钉 home，并在头注写明：这**不是**为凑绿松 oracle，而是不让一个**已登记、
  属于 drill 71** 的缺陷在别人的主题上随机开火、制造**假红 + flake**。
- **R-DATAPLANE / R-CONTROLSRC / R-5ELEM / R-SIGGUARD / R-EXPLORE-PIN / R-NO-HOST-LEAK / Mandate ①–④** 全文适用，各臂只引用不重述。

---

## §2 — S7 drills

### §2.1 drill `50-backup-restore`（N=2；**PRODUCT-RED 确定** —— #50 已实证）

**拓扑**：`grow_to_2 1 1`（brk1=leader/VOTER、brk2=follower/VOTER、agt1、ctl1）。**承重守卫**：JS meta `cluster_size==2` **无条件**先断（`setup-forcesingle.sh:14-16` 的形态）+ `sim_leader==brk1` 硬断（漂到 brk2 ⇒ F 臂的 follower 语义变掷硬币）。vault 必须可及两个 broker（I4-foreign 要在**非**产 bundle 的节点上读同一 bundle）。

**用户纪律**：一切 `tether cluster …` 以 **`dexec -u tether`** —— 这是 **`docs/broker-ops.md:621-626` 的 #6 权威配方**（逐字列了 `restore`），**不是** sim 图方便。例外只有臂 J1（故意按 runbook §5.1 字面以 root 跑）。typed-confirm 一律 `python3 /opt/sim/pty-confirm.py <answer> -- tether …`。

**注入法（§11-J）**：**in-place `rm -rf /var/lib/tether/*`**（先 `systemctl stop tether-broker nats-server`，再 rm）。真正的分工线是：**50 = lib-卷灾难（保留 `/etc/tether`：seam + nats.conf + secrets 完好）/ 51 = 整机灾难（fresh box）**。
**代价与实测订正（§12 SB-50-LANDING）**：50 保留 `/etc/tether` ⇒ 看不到 #51（restore 不 apply **seam**）
与 #52 的 **fresh-box 渲染**面 ⇒ 闸门行注明「该两面由 51 独占，50 不计入」。**但 Stage-A 原写的「50 结构上
看不到 #51/#52」被实测推翻**：50 **能**看到同族的**名册剪枝**那一半 ⇒ **#64**（restore 剪到单 voter 却留着
`cluster{}`、完成文案从不提 ⇒ 照文档做必 crash-loop）。**且 50 不得对全灭场景下任何结论**：它的自愈来自
brk2 幸存的 nats-server 让 clustered JS meta 重新形成（实测 ~73s，conf 仍 clustered），全灭无此 peer ⇒ 那是 51。

**臂表（28 臂；裁剪见 §5.3）**

| 臂 | 命令要点 | Oracle / 签名 | 源 |
|---|---|---|---|
| **A** SETUP | `grow_to_2 1 1` | JS `cluster_size==2`；`sim_leader==brk1`；2×VOTER | `cluster.sh:63-71` |
| **B1-B3** SEED X/HIST | `session lab --pin`；`TOKX=$(expose_serve_sentinel agt1 8081)` + `expose … --name live`；`exec agt1 -- echo BACKUP-HISTORY-SENTINEL` | **注入前数据面基线**：`poll_until 30 2 -- dp_curl_ok_body ctl1 "http://brk1:$PX/" "$TOKX"`（真 body，非 status）；`history` 含 SENTINEL | `dataplane.sh:11-30`；`history.go:58-60` |
| **Q1** INC-EXPORT | `dexec -u tether brk1 -- tether cluster recovery incident export --out /tmp/inc.json` | exit 0；`stat -c %a`==600；`jq -e '.schema=="incident" and .schema_version==1 and (.roster\|length)==2'` | `cluster_backup.go:143-166` **[SB-50-1]** |
| **Q2** O_EXCL | 同 `--out` 再跑 | `assert_refuses "refusing to overwrite existing /tmp/inc\.json"` | `cluster_backup.go:189-191` |
| **Q3** FORCE | 加 `--force` | exit 0 = **Q2 的对照源成功**；内容变 | `:183-186` |
| **Q4** NOFOLLOW | `ln -s /etc/tether/secrets/tunnel-key.pem /tmp/inc-link.json`；`--out /tmp/inc-link.json --force` | `assert_refuses "O_EXCL\|O_NOFOLLOW.*symbolic links\|ELOOP"` **且** `tunnel-key.pem` md5 未变（真结果 oracle）。**必须带 `--force`**（否则先撞 O_EXCL = 钉错门） | `:181-192` |
| **Q5** REDACT | 读 Q3 产物 | `jq -e '[…\|.body.actor_nkey]\|length>0 and all(.=="[redacted]")'` **且** `! grep -q "<真 nkey>"`。**`length>0` 是承重**（`all()` 对空集恒真）。**[SB-50-1]** path 未确证 → 先 `jq -e 'paths(scalars)\|join(".")'` 打印真实 path 集再定 oracle | `incident.go:32-35`；`schema/audit.go:22` |
| **R1** DOC-OK | `doctor --offline --secrets-dir … --db … --conf … --json` **以 tether 跑** | exit 0；`jq -e '.summary.fatal==0'`。**必须 `-u tether`**（root 会绕过 `preflight.go:78-85` 的真 `os.Open` 可读性检查） | `doctor.go:44-89` |
| **R2** DOC-CONF | `--conf /nonexistent/nats.conf` | `assert_refuses "natsconf: read .*no such file"` + `1 FATAL` —— **同时是 R3 的对照源**（证明 doctor 能红；R3 的注释必须显式引用 R2，否则「永远不红 ≠ 门存在」） | `natsconf/preflight.go:122-125` |
| **R3** **DOC-DB (#50)** | 同 R1 但 `--db /nonexistent/nope.db` | **INVERTED（R-INVERTED + R-EXHAUST）**：今天 exit 0 + `db PASS` + `0 fatal` ⇒ `product_red "#50 …"`；若判非绿 ⇒ `_as_fail "APPEARS FIXED — promote to assert_refuses"` | **已实证**；`doctor.go:82-87` → `storage.OpenReadOnly` = `storage.go:105-111` 裸 `sql.Open`（**惰性、从不 Ping**） |
| **C** **RUNBOOK-EXAMPLE (DOC-27)** | 逐字跑 runbook:524：`dexec -u tether brk1 -- sh -c 'tether cluster backup --out /var/backups/tether-$(date +%F)-$$'` | 分支 (a) 失败 → 断 `prepare bundle parent`（**`/var/backups` 不存在 ⇒ `MkdirAll` 先跑 ⇒ 撞第一分支、`CodeStoreError`(exit 70)**）→ `product_red`/DOC-27；(b) 成功 → GREEN 如实记。**这是 vault 作为 `[env]` 出场的前置证据（R-SUPPLY-ORDER）** | `clusterbackup.go:49-54`；`install.sh:491` |
| **D** BACKUP-LEADER | `[env, S0-备份库]` → `--out /var/lib/tether/leader-1` → `vault_pull` | stdout 锚 `online backup complete: .*applied_index=[0-9]+, self=brk1, source=leader`；`jq -e '.kind=="backup" and .self_id=="brk1" and .source_role=="leader" and (.roster\|length)==2'`。**`source=self` 不存在**（`sourceRole ∈ {leader,follower}`） | `cluster_backup.go:57-63`；`clusterbackup.go:35-38` |
| **E** MANIFEST-NO-SECRET（**白名单**） | 读 manifest | ① **差集**：`jq -e '[keys[]] - [<allowlist>] \| length==0'`（黑名单会漏未来新增列）② roster 键集 ③ `! grep -qE "PRIVATE KEY\|BEGIN CERTIFICATE\|^S[UAO][A-Z2-7]{20,}"` ④ `.self_cert_fp\|test("^[0-9a-f]{32,}$")` | `manifest.go:51-124` |
| **F1** FOLLOWER-REFUSE | brk2 上 backup | `assert_refuses "online backup must run on the leader .*current leader: brk1\. Re-run there, or pass --allow-stale-follower"` **且** `! test -e <out>`（拒在 Mkdir 之前）。**锚整句**：松签名 `not.*leader` 会同时匹配 `leaderRedirect` 串 = 钉错路径（backup CLI **不调** `leaderRedirect`） | `clusterbackup.go:33-46` |
| **F2** FOLLOWER-ALLOW | `--allow-stale-follower` | stdout `source=FOLLOWER \(possibly stale .* leader: brk1\)`；`jq -e '.source_role=="follower" and .leader_id=="brk1"'`。**头注硬写**：follower bundle **永不**用于同一性 oracle | `cluster_backup.go:57-63` |
| **F3** DOC-17 登记 | — | 无臂。runbook:523「any node, leader OR follower」/:533「ANY node = whole committed state」vs `clusterbackup.go:39-46`；**旁证更狠**：`clusterbackup.go:16-18` 的**函数 docstring 自己也陈旧**、与其下 :30-46 自相矛盾 | — |
| **G1** OFFLINE-REFUSE-LIVE | brk2 daemon 在跑时 `--offline` | `assert_refuses "daemon still running \(raft\.db locked\)"` | `offline.go:39` |
| **G2** OFFLINE-OK | brk2 stop → `--offline` 以 tether 跑 | stdout `offline backup complete: .* mode=cluster, self=brk2`；`jq -e '.account_fp\|test("^[0-9a-f]{64}$")'`（online 无此字段的语义分界）。⑤ 末尾 `start` + poll 回 2 VOTER 才继续 | `backup.go:141-147` |
| **B4** SEED-Z | **备份之后**：`session create zed --pin` | `session ls --json` 同时含 `lab` 与 `zed`（**Z 的存在基线**——没有它 L2 是废话） | 承重顺序 |
| **H1** DISASTER | `systemctl stop tether-broker nats-server` → `rm -rf /var/lib/tether/*` | `! test -e /var/lib/tether/tether.db`；**数据面真死**：`dp_curl_refused ctl1 …`（exit **7**，非 `! curl -sf`） | `dataplane.sh:44-52` |
| **H2** PEER-STOP | brk2 stop，头注标 `[GAP DOC-19]` | 观测 QUORUM_LOST（poll 越过 TFence≈10s，避开 #42 窗口） | **[SB-50-4]** 两分支同权 |
| **I1** RESTORE-YES | `… --confirm-node-id brk1 --yes` | `assert_refuses "cannot run unattended.*NO --yes override"`。**必须同时给 `--confirm-node-id`**（否则先撞门 1） | `cluster_backup.go:90-92` |
| **I2** NEVER-ESCAPABLE | `TETHER_CONFIRM_NODE_ID=brk1 … --confirm-node-id brk1`（**无 pty**） | `assert_refuses "requires an interactive terminal"` + `"aborted \(type the node_id to confirm; --yes is never accepted\)"`。**对照源 = J2（pty 正例成功）**。**禁写「缺 env 拒」型负例**（`allowMachineEscape=false` ⇒ env 根本不被读） | `cluster_backup.go:98-102`（`confirmTypedNodeID(..., false, "")`） |
| **I3** CONFIRM-MISMATCH | pty answer=`brk2`，`--confirm-node-id brk2` | `assert_refuses "--confirm-node-id \"brk2\" does not match the bundle's self_id \"brk1\""`。**daemon 必须已停**（否则先撞门 4） | `restore.go:234-236` |
| **I4** **FOREIGN-BUNDLE** | brk2 上（daemon 停）：`--confirm-node-id brk1`（**bundle 自己的 self_id**） | `assert_refuses "tunnel-cert fingerprint mismatch.*live secrets dir is not this bundle's node.*refusing to adopt a foreign cluster"` **且** brk2 的 db 未变。**门 9(`restore.go:234`) 早于门 10(`:238`) ⇒ 不传 bundle 的 self_id 就钉成门 9 = 假绿。本 drill 唯一致命设计点** | `restore.go:238-245` |
| **J1** **ROOT-RESTORE（#6 首次活体复现；DOC-18）** `[spike]` | `dexec brk1 -- pty-confirm.py brk1 -- tether cluster recovery restore …`（**root**，逐字照 runbook:550-551） | ① 产品**确有** WARN `offline op running as root against a non-root-owned data dir`（KEPT）② restore exit 0 ③ `stat -c %U tether.db`==`root` ④ **不以退出码判**：`poll_until 45 3` broker 达 active **失败** + **`/var/log/tether/broker.err`**（**不是 journal**）命中 `unable to open database file\|permission denied` → `product_red "#6 …"` ⑤ 文档补救 `[operator per broker-ops.md:625]` `chown -R tether:tether /var/lib/tether` → 起 → 健康 | `offline.go:934-949`(仅 WARN)；`install.sh:738,754-755` **[SB-50-2]** |
| **J2** **RESTORE-OK** | `dexec -u tether brk1 -- pty-confirm.py brk1 -- tether cluster recovery restore /var/lib/tether/leader-1 --confirm-node-id brk1 --secrets-dir /etc/tether/secrets` | 三断言：`restore complete: node brk1 is now a single-voter cluster \(pruned 1 stale peers; bundle applied_index [0-9]+ reset to 0\)` + `prior DB preserved at: …` + `NEXT: start tether-broker`。**`pruned 1 stale peers` 是 prune 真发生的唯一 stdout 证据** | `cluster_backup.go:111-119` |
| **K** START + STATUS | `start nats-server tether-broker` → poll leader | `jq -e '.health=="DEGRADED" and .health_label=="NOT-HA" and .exit_code==1 and (…VOTER…\|length)==1'`；**且**真退 1、**非 3**；banner 无 `force_single_active`。**`exit 1` 也是 default 分支的兜底值 ⇒ 必须同断 `NOT-HA` + `voters==1`** | `clusterstatus.go:66-101` |
| **L1+L2** IDENT X 阳/Z 阴 | `session ls --json` **一次读** | `jq -e '([…"lab"]\|length==1) and ([…"zed"]\|length==0)'` —— **阴性必须与阳性同一次读求值**（分开读时「命令报错 ⇒ Z 不在」= 假绿） | 承重 |
| **L3** HIST 阴 + DOC-17 | `history` | ① 进程 rc==0 且输出非空 ② **L1 阳性对照在** ③ 才断 `! grep -q SENTINEL`。单条 `! grep` = 恒真陷阱 | bundle 不含 JS ⇒ 永不回来 |
| **L4** IDENT-Y + 数据面回（**终点**） | poll agt1 ONLINE → `expose explain live --json` | ① `.public_port == $PX`（同号）② **`poll_until 120 3 -- dp_curl_ok_body ctl1 "http://brk1:$PX/" "$TOKX"`** = 原样 sentinel 真流量 ③ `home==brk1` + **epoch 比灾前 +1**（re-pin 的直接钉子）。**绝不以 `expose ls`/status 收口** | `restore.go:352-357` |
| **M1** TORN-BUNDLE | stop broker → `MD5_0` → 改 manifest 的 `public_host` **语义值** → pty restore | `assert_refuses "manifest/state\.db disagree on public_host .*refusing a torn/edited bundle"`；**且** `md5(tether.db)==MD5_0`（拒绝路径永不到 `:175`，md5 安全）；**且** `! ls *.pre-restore*`。改 JSON **结构**字节 → 撞门 5 = 钉错门 | `restore.go:261-274` |
| **O** **INTERRUPTED-RESTORE（确定性变体，§5.3-T6）** | 让 `BootstrapSingleNode` 失败（`raft/` 父目录不可写 或 `--cap-store` 灌满盘 ENOSPC）→ marker 必然滞留 | ① `sqlite3 -readonly` 读 `restore_in_progress`==1（**只读，非篡改**）② `start tether-broker` → **不看退出码**：poll 达 active **必须失败** + **broker.err** 命中 `was interrupted \(restore_in_progress is set\)` + `NRestarts` 递增 ③ 修复 → 重跑 restore → exit 0 + marker 清 ④ broker 起 + L1/L4 复验。**⑤ trap 必须补完 restore**（marker 滞留会让后续臂 SETUP-RED、根因错指） | `restore.go:336`(set)/`:204-209`(clear)；`cutover.go:98-107` **[SB-50-3]** |

**五要素（H1 注入）**：① B1-B3 的真流量/history 基线 ② `dexec`+真 curl+sqlite3 ③ **先停 units 再 rm**（不停 nats 而擦 `jetstream/` 会让 nats-server 半死、污染诊断）④ H1 的 `dp_curl_refused`(7) ⑤ 合并 trap。

**顺序硬约束（违反即假绿）**：① B4(Z) 必须在 D 之后 ② Q/R/C 必须在 H1 之前 ③ I3/I4 必须在 daemon 已停之后 ④ I4 必须传 `--confirm-node-id brk1` ⑤ M1/O 必须在 K 之后**再停一次**（臂表命令栏须显式含那条 `stop` + `poll_until` raft.db 解锁，否则撞门 4）⑥ J1 若走必在 J2 之前，其间只允许 `[operator per broker-ops.md:625]` 的文档补救。

---

### §2.2 drill `51-full-dr`（N=3→全灭→1→2；**PRODUCT-RED 高概率**）

**头等发现（源码确证，两条，是 51 存在的理由）**：
- **#51**：`cluster init` 自 G4 #5 起调 `applyClusterSeam`（`cmd/tether/cluster.go:794-804`）；`newClusterRestoreCmd` 的 flag 集**根本没有 `--config`**（`cluster_backup.go:123-129`）⇒ **restore 结构上不可能 apply seam**。install.sh 把 `cluster:` 整段注释掉（`:548-556`）⇒ 无 seam ⇒ `assertClusterDBConsistent` FATAL（`cutover.go:117-120`：`refusing to silently downgrade a cluster DB to single mode`）。
- **#52**：`init` 打印 NEXT step-1 `reconcile nats --manual …`（`cluster.go:824-826`）；`restore` 的完成文案只有 `NEXT: start tether-broker, then cluster join approve`（`cluster_backup.go:115-119`）。install.sh stock conf 无 authorization/auth_callout/cluster（`:690-704`）；cluster 模式下 auth_callout 自动 ON（`serve.go:203-218`）。runbook §5.2（`:566-574`）通篇不提 nats.conf。

⇒ **runbook §5.2 按字面执行必失败两次。** 必须钉成 `assert_bug`，**绝不用 `[env]` 悄悄补上**。

**拓扑/SETUP**：`grow_to_3 1 1` → `transfer-leader brk1 --wait` → `session lab` → `agent_provision_yaml agt1 lab nats://brk1:4222 open` → `expose … --on-broker brk1`。
**承重前提（写进头注 + `assert_setup`）**：**brk1 = leader ∧ = agt1 的 tunnel broker ∧ = expose home 必须是同一节点** —— `homeForExpose` 对非-tunnel voter 返 nil（`home.go:96-113`，#29）⇒ N=3 下 expose 只有 home==brk1 才服务；online backup 默认只在 leader 放行；restore 把端口 re-home 到 **self**。**这不是便利，是唯一可服务构型。**

**runbook §5.2 四步 → 臂映射 + 标注三分法**

| 类别 | 标签 | 实例 |
|---|---|---|
| sim 供给（Mandate ③） | `[env]` | `up`/install.sh、`enable --now nats-server`、`vault_push` 后 chown、`systemctl reset-failed` |
| 文档剧本明写的 operator 步 | `[operator per runbook §5.2 step N]` | `secrets_distribute`(步1)、pty-fed `restore`(步2)、`start tether-broker`(步3) |
| 产品**明确声明不做**的（documented design boundary，**不是** gap） | `[by design: rejection #N]` | 52 的铸/分发密钥（`cluster_rotation.go:12-20`） |
| 剧本**外**的绕过 | **`[GAP #N]`** | seam 手写(#51)、`reconcile nats --manual` 手跑(#52) |

**判据（可判定，非口头）**：`[operator]` ⇔ 该动作在 runbook §5.1/§5.2 有对应句；否则 `[GAP]`。#51/#52 的源码级判别子：① restore **无 `--config` flag** ⇒ 即便以 root 跑也**永远**无法 apply seam；② 姊妹命令（init）的完成文案对照本身就是缺口证据。

**臂表**

| 臂 | 命令 | Oracle / 签名 | 源 |
|---|---|---|---|
| **A-base** 灾前基线 | — | ① 3 VOTER ② `sim_leader==brk1` ③ `session ls` 含 lab ④ **`dp_curl_ok_body ctl1 http://brk1:$P/ $TOK`**（真隧道字节）⑤ `history --kind proc` 有 exec 行 ⑥ 记 `E0` + host stash tunnel-cert sha | — |
| **B-vault** | `dexec -u tether brk1 -- tether cluster backup --out /var/lib/tether/dr-<uniq>` → `vault_pull` | ① `source=leader` ② vault 有两文件 ③ `jq`: `.self_id=="brk1"`、`.roster\|length==3` 全 VOTER ④ **零密钥**：`! grep -aqE 'PRIVATE KEY\|BEGIN OPENSSH'` + manifest 差集白名单 ⑤ 记 `SHA_STATE`/`SHA_MAN`。**`--out` 落 `/var/lib/tether`**（0750 tether，install.sh:491），**不是** `/var/backups`（50-C 已证那堵墙） | `clusterbackup.go:39-46` |
| **C-disaster** 全灭 | `rm_node brk1 --vols; rm_node brk2 --vols; rm_node brk3 --vols` | ① 三容器 `! node_exists` ② 六卷 `! d volume inspect` ③ **agt1 容器仍 running**（步4 的前提）④ `dp_curl_refused ctl1 …`(7) ⑤ **C-vault-oracle：host 重算 `SHA_STATE`/`SHA_MAN` 不变 ∧ `jq .roster\|length==3` 仍可读** ← **S0-备份库的存在理由，一条断言即证「域外存活」**（**必须 sha256 比对，不是 `test -e`** —— 残留/半写 bundle 会让「灾后可读」因错误原因绿） | `docker.sh:93-99` |
| **D-freshbox** `[operator per §5.2 step 1]` | `$SIM up --brokers 1 --agents 1 --ctl 1` → `secrets_distribute $INST brk1` → `[env] enable --now nats-server` | **fresh 门（缺一即整个 DR 是假的）**：① `! test -e /var/lib/tether/tether.db` ② `! test -e …/raft` ③ `! grep -qE '^    raft_addr:' /etc/tether/broker.yaml` ④ `! grep -q auth_callout /etc/tether/nats.d/nats.conf`。**恢复 oracle**：⑤ in-container `tunnel-cert.pem` **和** `tunnel-key.pem` sha256 == 灾前 stash 值（**两者都要**）⑥ 0600/0700 | `install.sh:548-556,690-704`；`secrets.sh:49-81` |
| **E-remint-neg** | `secrets_mint_node "$INSTANCE" brk1-remint` → `d cp` → `/tmp/remint-secrets` + chown → pty restore `--secrets-dir /tmp/remint-secrets` | `assert_refuses "tunnel-cert fingerprint mismatch\|refusing to adopt a foreign cluster"` **+ 零变更**：`! test -e /var/lib/tether/tether.db`（gate 在任何盘写之前）。**R-CONTROLSRC 对照 = F-b 用原密钥成功**（「全都失败」不是门存在的证据）。**这条同时是 D 的合法性证明**：没有它，「`secrets_distribute` 是恢复不是捷径」只是口号 | `restore.go:237-245` |
| **F-a** `--raft-addr` 畸形 | pty-fed `restore … --raft-addr notahostport` | `assert_refuses -- "--raft-addr .* must be host:port"`（**必须 pty**：confirm 先于校验） | `restore.go:151-155` |
| **F-b** **DR 主 restore** `[operator per §5.2 step 2]` | `dexec -u tether brk1 -- pty-confirm.py brk1 -- tether cluster recovery restore … --raft-addr brk1:7400` | ① `restore complete: … \(pruned 2 stale peers; …\)` ② `prior DB preserved at: \(none — no prior DB on this host\)`（**fresh-box 专属判别子**）③ sqlite3 `count(cluster_nodes)==1`（**镜像 42 的 #49 加固：硬断名册精确 `{self}`**）④ `applied_index==0` ⑤ `restore_in_progress`/`force_single_active` 均 0 行 ⑥ **port re-home**：`home_broker=='brk1' ∧ epoch==E0+1` | `restore.go:161,199-213,317-357` |
| **F-c/F-d** `--raft-addr` 真覆写 | 再 restore `--raft-addr 127.0.0.1:7400` → 再 `brk1:7400` 复位 | **F-c**：sqlite3 `raft_addr=='127.0.0.1:7400'` + stdout `prior DB preserved at: .*tether\.db\.pre-restore\.bak` + **`pruned 2 stale peers`**（**恒 2**：每次在**新拷贝的 staging** 上 prune；判别 F-b/F-c/F-d 的判别子是 **`.bak` 阶梯**，不是 prune 数）。**F-d**：`\.pre-restore\.1\.bak`（证 `.N` 从不覆盖）。**`.bak` 内容等同用 sentinel 行、不用 md5** —— `backupToUnique` 先调 `checkpointWALForBackup` 以**读写 DSN** `PRAGMA wal_checkpoint(TRUNCATE)`，**在拷贝前就改了 tether.db 的字节** ⇒ md5 必假红 | `restore.go:151-157,175`；`init.go:342-375` |
| **G1** **§5.2 步3 逐字 → #51** | `assert_bug "runbook §5.2 step 3 'Start the daemon' on a FRESH DR box" "#51" "<sig>" _dr_start_per_runbook`；helper 失败时 `tail -n 60 /var/log/tether/broker.err` 打 stdout 供匹配 | sig = `broker.cluster.data_dir is unset\|refusing to silently downgrade a cluster DB to single mode`。**FG**：wrong-reason → ASSERT-FAIL；exit 0 → APPEARS FIXED | `cutover.go:117-120`；`cluster_backup.go:123-129` |
| **G1-clear** `[GAP #51]` | root 写 seam（复用 `simcluster` 的既有那行）+ `[env] systemctl reset-failed` | 计入 DR-STEP-LEDGER | — |
| **G2** 再起 → **#52** | `assert_bug … "#52" "<sig>"` | **[SB-51-NATSSIG]** 精确串未知。出口 (a) 抓到 auth/nkey 串 → 钉死、#52 成立；(b) 竟能服务 → **撤 #52**、G2 改 `assert_ok`、降 DOC-19。**绝不臆造签名** | `serve.go:203-218`；`install.sh:690-704` |
| **G2-clear** `[GAP #52]` | `reconcile nats --manual --server-name brk1 --route-url nats://brk1:6222 --account-issuer … --broker-nkey …` → `nats-server -t` → restart | **这条命令行逐字等于 `cluster init` 打印的 NEXT step-1 而 restore 从不打印它** —— 该对照写进 gotcha。零 `--peer` ⇒ Standalone 渲染 ⇒ 这步不可省 | `cluster_natsconf.go:311-315` |
| **G3** 起 + N=1 就绪（**G1/G2 的对照源**） | `assert_ok` start → poll | ① 单 voter ② **`cluster status` exit 1 DEGRADED、非 exit 3**（runbook §5.1 步3 逐字）③ nats.conf 无 `cluster{}` | `cluster-runbook.md:554-555` |
| **H1** 步4 agent 零操作重连 | **禁** restart/re-provision agent（#48 禁令继承） | ① 起 broker **前**先证 `! _apy_online agt1`（心跳残留窗 ≈5s 会假绿）② `poll_until 90 3 -- _apy_online agt1` | **[SB-51-AGENTRECON]** |
| **H2** **步4 数据面收口（终点）** | — | **`poll_until 90 3 -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"`** —— 同一灾前公网端口、灾前那串精确 sentinel、经真反向隧道。**绝不**以 `ps`/`expose explain`/`cluster status` 收口 | `restore.go:355` |
| **I** re-grow（步3b） | `$SIM up --brokers 3 …` → `$SIM grow brk2` | **#31-resilient 双分支**：到 VOTER → ① 2 VOTER ② **H2 的 sentinel 再 curl 一次仍通**；被 #31/#45 挡 → `product_red "[#31] re-grow after DR blocked …"`（DR 主脊已在 N=1 证毕）。**不得用 `grow_to_3` 的 retry 洗掉 #31 证据** | #31/#47 |
| **J** history/audit 全灭（explore→pin） | `history --kind proc` | **[SB-51-HIST]**：机理已核（bundle 只含 `state.db`，`backup.go:87`；`audit_published_index` 重置为 0，`restore.go:317`；raft 从 index 1 重 bootstrap ⇒ 无可回填）。出口 (a) 空/报无流 → **DOC-19** + **#53**；(b) 竟有行 → 撤销。**oracle 纪律**：`history` 进程 rc==0 且对另一 kind 有输出 + 阳性对照在 → 才断缺席。**「命令报错」不是缺口证据** | `backup.go:87`；`restore.go:313-321` |

**计步呈现（Mandate ④ 的可审计化 —— 本组最好的工艺，推广到 50/52）**：
```
DR-STEP-LEDGER: runbook-§5.2-documented=4 actually-required=<N> undocumented=<M> gaps=[#51-seam,#52-natsconf]
```
`<M>` 由 drill 计数（每个 `[GAP #N]` 步 +1）。**M>0 即证 §5.2 不完备** —— 这是「一次操作若靠复杂脚本才成功 = tether 的失败被掩盖」的量化反面。**明令禁止**把剧本包成一个 `dr_restore_all()` 报 GREEN。

---

### §2.3 drill `52-credential-rotation`（N=2；**PRODUCT-RED 必现** —— #54）

**roadmap §3 底稿三处被源码推翻（§11-K-4 顺带订正 roadmap）**：① `cluster keygen` 调 `auth.GenerateUserSeed()`（`cluster_offline.go:480`）铸的是 **`U…` user seed = node-ident**，**不是** account seed（runbook §2.1 step 2 自己写的是 `nk -gen account`；`cluster_rotation.go:87` 逐字「tether NEVER copies private keys for you (rejection #2)」）；② `reconcile nats --all --wait` **纯轮询**（`cluster_reconcile.go:78`「It NEVER bumps a generation」）⇒ **正确顺序是先重启 broker**，runbook 那一步是 false all-clear；③「其余 staging 演练工具面」→ **不存在**（`cluster_rotation.go` 全文 118 行 = printer + banner + alert raise，无 staging/dry-run/verify 动词）→ resolve 为「无此面」，**写进 §7 闸门，不留悬空行**。

**Mandate 正当性（预防外审质疑，写进头注）**：52 会写不少 bash（铸 gen2 CA/account、分发、滚动重启）。**这不是 Mandate ④ 的「靠复杂脚本才成功」** —— 产品**显式、文档化地**把这件事划出自己的范围（`cluster_rotation.go:12-20`：「this is a PRINTER/CHECKLIST … NOT an automator. It NEVER generates or moves private key material (rejection #2)」）。sim 铸密钥 = 扮演 operator 的 PKI，属 `[by design: rejection #2]`。**Mandate ④ 适用于「tether 声称能做却做不到」，不适用于「tether 声明不做」。**
**自设红线**：*若哪天有人想加一个 `secrets_rotate_and_reload()` 一把梭 —— 那正是 Mandate ④ 的判定反转：轮换「成功」只因 sim 写了复杂脚本 = tether 的失败被掩盖。*

**SETUP**：N=2 + agt1 + ctl1；**FG 守卫 1** JS meta `cluster_size==2` 真 FORMED（否则 B7 的掉 1 是「从来没形成」）；**FG 守卫 2** 注入前数据面真通；**FG 守卫 3（承重，`assert_setup` 级）** 同一 ctl 身份经 brk1 与 brk2 **各连一次都成功**（B5d 的对照腿基线；A8 的砖化残留会伪装成 B 组的产品结论 ⇒ 必须 SETUP-RED 而非 assert_ok）。

**臂组 A — `rotate-tunnel-cert`（全 #31-无关）**

| 臂 | oracle / 签名 | 源 |
|---|---|---|
| **A1** no-fp | `assert_refuses "--cert-fp is required"` | `clusterdrain.go:384` |
| **A2** remote-target（on brk1，target=brk2） | `assert_refuses "rotate must run on the target broker while it is leader.*transfer leadership to brk2 first"` | `clusterdrain.go:386-388` |
| **A3** fp-mismatch | `assert_refuses "on-disk tunnel cert fingerprint .* does not match requested --cert-fp"`；**顺带回读 tether 算的 on-disk fp 与 `secrets_tunnel_fp` 交叉核对相等**（`tls.go:91-94` 的 fp = `sha256(cert.Raw)`，Raw=DER ⇒ `openssl x509 -outform DER \| sha256sum` 同构）。不等 = harness bug，**立刻修** | `clusterwrite.go:245` **[SB-52-FP]** |
| **A4** **follower（#56）** | 普通 `assert_refuses "not the leader — re-run on the leader host: brk1"` GREEN。**但摸到的边是真缺陷**：follower 说「re-run on the leader host: brk1」，运维照做后在 brk1 上撞 `clusterdrain.go:386-388`「transfer leadership to **brk2** first」—— **两条建议互相打架，形成环** → **#56 = 成对断言**（follower 上得 leader-redirect ∧ leader 上对同一目标得 self-only 拒绝） | `clusterstatus.go:649-657`；`cluster.go:625` |
| **A5** 正例 | `secrets_mint_tunnel_only` + `secrets_push_file` → `assert_ok` + stdout `cert rotation committed; target broker hot-swapped its live tunnel certificate` | `cluster.go:631` |
| **A6** **pin 窗口可读（roadmap「pin 轮换窗口」的部署面交付）** | `jq -e '.nodes[]\|select(.node_id=="brk1")\|.cert_fp=="<NEW>" and .cert_fp_prev=="<OLD>" and .cert_fp_valid_until!=null'` | `clusterstatus.go:283`；窗口常量 `clusterstatus.go:63`(24h) |
| **A7** **re-pin 真流量（§11-I）** | **两段式**：① 轮换后**先不动 agent**，`poll_until` 有界窗看是否**自发**重拨 ② 不自发 → **`fault_partition_on agt1 7000` 短暂 DROP → 隧道断 → agent 自行 redial**（**注入**，真实网络会发生）→ `poll_until … dp_curl_ok_body ctl1 "http://brk1:$PUB" "$TOK"` ③ 连 redial 后也不 re-pin → 登 gotcha + `[GAP]`。**驳回 `systemctl restart tether-agent`**（§11-I）。**Stage-B 先读 runbook §2.1 的 agent 侧步骤** | `agent.go:1307,1404`；`tls.go:115-134` |
| **A8** **fail-closed 砖化 + DOC-23** | brk2 落盘新 tunnel leaf **但不轮转 pin** → restart。**oracle**：broker **不达 active** **且 `/var/log/tether/broker.err`**（**不是 journal**）命中 `on-disk tunnel cert fingerprint .* matches neither the pinned`（**不是「unit failed」**——那会把任何崩溃吃成绿）。**DOC-23**：该错串第二条补救 `re-run tether cluster rotate-tunnel-cert` **不可达**（`wireClusterEarly` 在 `broker.go:691` 返错即退，adminsock 在 `:1060` 才建）→ 砖化态下跑该命令 `assert_refuses "no such file\|connection refused"` **[SB-52-A8]**。**⑤ trap 必须**：恢复旧 leaf → **`systemctl reset-failed tether-broker`**（`StartLimitBurst=5/10s` 未关，`install.sh:752-753` 逐字「we deliberately do NOT set StartLimitIntervalSec=0」）→ start → poll active + `cluster_size==2` | `clusterwrite.go:173-190`；`broker.go:691,1060` |

**臂组 B — account.nk + CA 轮换（本 drill 重心）**

| 臂 | oracle | 源 |
|---|---|---|
| **B0** **reconciler-liveness 正例（承重）** | B2 的三件齐（收敛串 + md5 不变 + issuer 仍旧）在 **reconciler 死了 / tick 从未触发**时**同样全部成立** ⇒ 三件齐**不判别**。B0 先制造一次 reconciler **确实会**重渲染的变更（拓扑 generation 变动）→ 观测 md5 **变化** → 复位。有了它，B2 的「md5 不变」才等价于「渲染了但比对成 noop」；没有它，#54 的机理是**推断的、不是观测的**，Stage-C 会打掉 | `topology_reconcile.go:29,60-77` |
| **B1** 基线 | 两 broker `issuer:` == `$ACCT_OLD`；md5 记账；FG 守卫 3 双源连通 | `natscluster/config.go:167` |
| **B2** **INVERTED：成功=缺口（#54）** | `secrets_mint_gen $I 2` → `secrets_remint_route_only`（**不碰 tunnel**）→ 推新 `account.nk`+`cluster-ca.pem`+`route-*.pem`（**不重启**）→ leader 上 `reconcile nats --all --wait`。**R-INVERTED + R-EXHAUST**：断三件同时成立 (i) `all voters converged to topology generation [0-9]+` (ii) 两 broker nats.conf **md5 与基线逐字节相同** (iii) `issuer:` 仍 == `$ACCT_OLD` ⇒ `product_red "#54 …"`；任一不成立 ⇒ 分类到 APPEARS-FIXED / UNJUDGEABLE | `cluster_reconcile.go:77-107`；`topology_reconcile.go:233` → `serve.go:203-218`（**启动时一次性 `loadAuthCalloutSeeds` ⇒ 不重启 broker 永不换 issuer = 源码级铁证**） |
| **B3** doctor 失明（#54 第二 facet） | skew 态下 `doctor --offline` 报绿、零 skew 提示 ⇒ `product_red`。唯一能打印该 skew 的 note（`cluster_secrets.go:46-47`）只挂在 rotation guide 与 `init --from-existing` 上 ⇒ **运行中的集群无任何动词可查** | `preflight.go:58-140` |
| **B4** 重启才 render | brk1 restart → `issuer:` == `$ACCT_NEW` ∧ md5 变（证明「正确顺序是先重启」） | `topology_reconcile.go:60-77,233` |
| **B5** skew 探针（**record-only，§5.3-T10**） | **降级为 `log` 打印 `AUTHCALLOUT-SKEW-PROBE: <fail>/20`，不 assert**。理由：`0.5^20≈1e-6` 的判别力假设 nats-server 对跨 route queue group 是 50/50 —— **该模型无任何依据**；真实分布更可能是强本地优先 ⇒ 失败数在 0/1 间抖 ⇒ **臂 verdict 逐轮翻 = flake 定义** | `authcallout.go:95-101` |
| **B5d** **确定性变体 + R-CONTROLSRC（#55 的唯一断言腿）** | `systemctl stop tether-broker` on brk1（= runbook step 4 自己必经的态）⇒ brk1 的 conf issuer=NEW 而**本地无 responder** ⇒ 只剩 brk2 的 OLD-key responder 应答 queue group。**被限方**：经 brk1 `assert_refuses "auth_callout rejected the connection"`；**对照源**：**同一身份**经 brk2 `assert_ok`；**恢复腿**：start brk1 → poll 探针恢复。**三点齐 = §0.4 铁律的样板，定为全组限流/撤销臂模板**。**每次探针成对采样**（brk1 与 brk2 同窗口），只有「brk1 拒 ∧ 同秒 brk2 成功」才计一次命中 | `authcallout.go:99`；`error_hints.go:145-155` |
| **B6** 剧本收口 | brk2 restart → 两 issuer 均 NEW；两源探针均成功；agt1 ONLINE；`dp_curl_ok_body` 仍返回 sentinel ⇒ **车队在新信任锚下全活**（真流量收口） | — |
| **B7** 旧 route leaf 被新 CA 拒（负例 + 对照） | ① 基线 = B6 后 `cluster_size==2` + 两 `nats-server` active（**正向对照腿**）② 观测 = brk1 loopback `curl 127.0.0.1:8223/jsz?meta=1` + `journalctl -u nats-server`（nats-server **不是** tether-broker，journal 有效）③ **只**把 brk2 的 route leaf 换回旧 CA 签的 → restart nats-server ④ brk1 视角 `cluster_size` → **1** ∧ brk1 nats journal 命中 `certificate signed by unknown authority\|unknown certificate authority` **[SB-52-B7]** ∧ **FG 守卫：brk2 的 nats-server is-active ∧ `:4222` 应答**（三者齐才证明「是 route 被拒」而非「brk2 死了」）⑤ trap 恢复 → poll `cluster_size==2` | runbook:245-250 |

**臂组 C — `cluster keygen`**：C1 真盘铸钥（stdout `U…` pub **与独立工具 `nk -inkey … -pubout` 交叉核对相等** —— `secrets.sh:65` 已有同 idiom；**不得**用 tether 自己的 `cluster node-pub` 对照 = 自证循环）+ `stat -c %a`==600 + 属主 tether；C2 Hidden 位（`cluster --help` 不含 `keygen`、`keygen --help` 可跑）；C3 无 `--out` 零落盘。

**臂组 D — C7（`retire --compromised --require-credential-rotation`）**

⚠ **承重风险（roadmap 未预见）**：`--compromised` 只是 retire 的修饰 flag（`cluster_retire.go:140`），guide 在 retire **成功后**才打（`:122`）⇒ 它**走完整 retire op 机器** ⇒ `StartRetireOperation` 的 `growActiveJoiner != "" → 硬拒`（`cluster_operation_controller.go:179-180`）—— **这正是 #31 挡住 40/41 的同一堵墙**。且 `cluster_retire.go:50` 的 `callAdmin` 在 `:47` typed-confirm **之后** ⇒ **#31 在运维已被问过一次之后才拒 = #31 blast-radius 的新面**。

- **D2 非-TTY 真拒**（部署层增量：hermetic `cluster_rotation_admin_test.go:154` 用 `cmd.SetIn` 注入，**永不走** `in == os.Stdin && !term.IsTerminal` 的真 TTY 判定支）：(i) stderr 先命中 WARNING `retire is NOT a credential revocation` (ii) `assert_refuses "requires an interactive terminal"` (iii) **零副作用**：`ops ls` 无新 op、`alert ls --json` 无 `manual:credrot:`。
- **D3 `--yes`**：**[SB-52-D3] 已由源码关闭** —— `cluster_retire.go:138-142` 只注册 5 个 flag，**无** `registerYesRejector`（对比 `cluster_backup.go:128` restore **有**）⇒ `assert_refuses "unknown flag: --yes"` GREEN，**并入 #36 blast-radius，不发新号**。
- **D-spine（#31-gated，单次尝试 + 分支，镜像 `40-drain-retire.sh:193-251`）**：
  - 分支 (i) `grow of .* is in progress` → `product_red "#31 grow-lock leak BLOCKS the C7 guided rotation spine"` + C7 生命周期 `not_covered`（理由=#31-blocked）。**不得**发明新清法凑绿（G-B 实测 canonical 清法也清不掉）。
  - 分支 (ii) op 创建成功 → **D4** stdout `retire operation .* created` + `=== CREDENTIAL ROTATION GUIDE \(compromised node brk2\) ===` + raised 判别子 `severe alert manual:credrot:brk2`（**只断这一个判别子，不复述正文**）；**D5** stderr banner `credentials are NOT yet rotated — brk2 can still authenticate`；**D6（招牌臂）** 从 ctl1 **经真 NATS** `alert ls --json \| jq -e '.alerts[]\|select(.dedup_key=="manual:credrot:brk2")\|.severity=="severe"'` —— hermetic 只对 **stub adminsock** 证了 raise **请求形状**，从未证过落库+可读；**D7** severe banner 上 stdout 仍可解析；**D8** `alert clear manual:credrot:brk2` → 消失 → 再 clear 仍 exit 0（幂等）。
  - **#45 与 52 无关**（alert/guide/banner 全在 op 创建后**立即**发生，先于任何 op 驱动）⇒ **52 不以 terminal RETIRED 收口**（那是 40 的活）⇒ **绝不给 D-spine 加 `--wait`**。这是 52 相对 40 的结构优势。
- **五要素**：① brk2 是 VOTER + `alert ls` **无** `manual:credrot:*`（洁净基线，防 setup 瞬态预拉）+ agt1 ONLINE + 数据面通 ② leader `cluster status --json` + `ops show --json` + **ctl 经真 NATS 的 `alert ls --json`** ③ healthy N=2 + pty typed-confirm（绝无 `--yes`）④ **alert 在 ctl 侧真读到 + clear 后真消失**（不以 `resp.OK` 收口）⑤ 合并 trap。
- **非缺陷、登记备案**：`alert clear` 不校验轮换是否真发生 = `cluster_rotation.go:13-16` 明文的**有意设计**（rejection #5）。头注写明，**不记 gotcha**。

**与 40 的边界（防重复/防抢功）**：正例 retire 的 raft-removal / op ladder / `--wait` / mid-interrupt → **40**；`retire` 的 R-hint（else 支）→ **40**；52 owns 的是 **require 支的 WARNING**（`:44-46`，不同串不同分支）+ C7 wiring。`doctor` 正向 preflight → **50**；52 只加 B3 一条 skew-失明。

---

## §3 — S9 drills

### §3.1 drill `94-agent-reconcile`（N=1 + 2 agent；GREEN）

**头等源码发现（推翻 roadmap 底稿的 orphan 载体暗示）**：`internal/agent/agent.go:1133-1136` 逐字「**Only PTY sessions are reachable from a.procs**；exec children are sync-managed … v1 doesn't track」；`a.procs` 的**唯一**写入点是 `run.go:229` `registerProc`（`exec.go` 无）。⇒ **orphan 臂必须用 `tether run`（真 PTY），`exec` 结构性造不出 orphan。** 这**不是缺陷**（v1 明写的设计）⇒ **不登 gotcha，但必须写进头注** —— 否则未来有人「简化」成 exec 就是结构性假绿。roadmap 首句的 `exec sleep 9999`×N **只适用于另一个方向**（missed-exit → `EXITED(-1)`）。

**拓扑**：N=1 **cluster** 模式（`cluster backup`/`restore` 只吃 cluster bundle，`restore.go:91-93`；SB-43 已证 bare P2 broker 不服务 NATS session）+ 2 agent + 1 ctl。**零新 harness 文件。**

**臂组 A — G.1 missed-exit（`exec` + `docker kill agent`）**

| 臂 | oracle | 源 |
|---|---|---|
| **A0** | `exec agt1 -- sleep 9481` ×2 + `exec agt2 -- sleep 9482`；`ps --json` 里 agt1 两条 `RUNNING` + `dexec agt1 -- pgrep -f 'sleep 9481'` 双证 OS 侧。记 `PIDA1/PIDA2/PIDB` **[SB-94-PSJSON]** | `ps.go:130-153` |
| **A1** LOST | `node_kill agt1` → poll STALE(5s) → poll OFFLINE(60s) → `ps --json` `PIDA1.status=="LOST"`；**判别性守卫**：同一次 `ps --json` 里 agt2 的 `PIDB` **必须仍 RUNNING**（证明 LOST 是 node-scoped 读时派生、不是全表塌陷；LOST **从不落盘**） | `exec.go:337-346` |
| **A2** reconcile + **`agent_registered` event（inventory row-30）** | `node_start agt1` → **等 register 真发生**（cursor 门，非等 ONLINE）→ `ps -a --json` 双双 `EXITED` + `exit_code==-1`；**FG**：agt2 的 `PIDB` 必须**仍 RUNNING**（全表误杀 = HARD FAIL）。**A2 不重启 nats** ⇒ ctl1 的 core-sub 全程存活 ⇒ **`agent_registered` 可确定性捕获** → `ev_seen agent_registered` 计数增。**故 inventory row-30 以清单自己的 oracle 交付，不必改清单措辞** | `reconcile.go:238-250`；`permissions.go:147` |
| **A3** G.5-proc | `history --kind proc \| grep -F "PROC  lab/agt1  pid=$PIDA1  kind=reconciled_closed  rc=-1"`（**两个空格字面量**，`history.go:554,573-576` 直 `Fprintf`、**不过 tabwriter**）+ negative `! grep -F "pid=$PIDB  kind=reconciled_closed"` | `history.go:573-576`；`reconcile.go:249` |

**臂组 B — orphan（产品路径造法，五步）**

① `dexec -u tether brk1 -- tether cluster backup --out /var/lib/tether/bk-94-pre`（N=1 ⇒ self 就是 leader ⇒ `source=leader`；`--out` 必须不存在 ⇒ 唯一名 + trap 清）
② **必须 `run`（PTY）**：后台 `ptyrun … -- tether run agt1 -- sh -c "printf 'ORPHREADY\n'; exec sleep 9199"`（`60-user-journey.sh:139-147` 先例）；基线 = `ps --json` `PIDO.status=="RUNNING"` **AND** `dexec agt1 -- pgrep -f 'sleep 9199'`（broker 视图 + OS 真值双证）
③ `sctl brk1 stop tether-broker`（operator 停 ⇒ `Restart=always` 不救）→ `dexec -u tether brk1 -- pty-confirm.py brk1 -- tether cluster recovery restore /var/lib/tether/bk-94-pre --confirm-node-id brk1 --secrets-dir /etc/tether/secrets` → start → poll ready
 - **必须 `-u tether`**：这是 **`broker-ops.md:621-626` 的 #6 权威配方 + 产品自述**（`offline.go:945-946`），**不是** sim 图方便；root 路径的坑是 **50-J1** 的暴露对象，94 不重复也不代劳（§11-J）
 - **③b 断连未发生的前证（关键 anti-FG）**：restore 期间 nats-server 未动 ⇒ agent 的 nc 未断。断 orphan 仍活 **AND** journal `--after-cursor $CUR0` **无** `agent: re-registered after reconnect` ⇒ **把「broker 重启不打断 agent NATS 连接」从注释变成真断言，同时证明步④不可省**
④ `CUR=$(_jcursor agt1)` → `sctl brk1 restart nats-server`（**绝不** `systemctl restart tether-agent` —— systemd cgroup 会连带杀掉被测子进程）
⑤ oracle：

| 臂 | oracle / 独立超时 | 源 |
|---|---|---|
| **B1** broker-back | `poll_until 60 2 -- _broker_ready` | — |
| **B2** **re-register（PRIMARY）** | `poll_until 60 2 -- _agent_journal_after "$CUR" 'agent: re-registered after reconnect'`（**agent slog 在 journal，不受 R-BROKERLOG 影响**）。**钉 `proxy.go:486` 的专属串，不 grep `agent: registered`**（后者在第三条 register 路径 `roster.go:485-486` 下也会出） | `proxy.go:486` |
| **B2-NOT** | **明令禁用**：`node ls` 回 ONLINE **不是**重注册证据 —— 旧 bundle **含 agt1 的 node 行** ⇒ heartbeat（`broker.go:1289-1302`，5s 一次）**直接写 `livenessDB`** 把它翻 ONLINE，**零 register** | `broker.go:1297-1302` |
| **B3** orphan-killed | `_no_orph() { ! dexec agt1 -- pgrep -f 'sleep 9199'; }` + `poll_until 30 1 -- _no_orph`（**R-NOSHC**）**且** journal `agent: kill orphan` 含 `pid=$PIDO`。**头注写明 slog handler 依赖，别硬编码 `pid=` 格式** | `agent.go:1434` |
| **B4** audit-orphan（**不以进程消失为终点**） | `grep -F "PROC  lab/agt1  pid=$PIDO  kind=killed_orphan  rc=<nil>"` **正反双证**：反面 `grep -E "pid=$PIDO  kind=killed_orphan  rc=(-?[0-9]+)"` 必须**无匹配**。`rc=<nil>` 是产品保证（`exec.go:451-454` 只在 `exit\|reconciled_closed` 设 `RC`；`schema/audit.go:44` `omitempty`） | `exec.go:446-469` |
| **B5** drop-directive | `agent: kill orphan pid=$PIDO` **就是** directive 被返回并执行的证据。**诚实注记 → DOC-25**：`onNATSReconnect` 路径**不打印** `drop_procs=N`（该计数只在初连路径 `agent.go:658-664`）⇒ directive 数组本身无 operator 可读口 → 由 B3+B4 效果面钉住，**不 `not_covered`** | `proxy.go:451-487` |
| **B6** 无关行完好 | ① `session ls \| grep -qw lab` ② **数据面**：`poll_until 120 3 -- dp_curl_ok_body ctl1 "http://brk1:$PX/" "$TOKX"`（**必须 poll** —— `restore.go:355` 把所有 ALLOCATED 端口 re-home 到 self 且 epoch+1，X 的 agent 要重收 directive、重拨号才恢复）③ `ps -a --json` 里 A 组的 EXITED 行仍在 | `restore.go:355` |
| **B7** port-reconcile（G.5 port 面） | **在 backup 之后**建 `web2` expose（`PY`）→ restore 后该 port 行不存在、agent 仍持 token ⇒ register 走 `reconcile.go:280-283` 的 `!ok` 支 → revoke + `pubAuditPort(sid,"reconciled")`。**oracle**：④a `grep -F "PORT  lab/agt1  port=$PY  name=web2  kind=reconciled"` ④b **数据面真断** `dp_curl_refused ctl1 brk1 $PY`(7) ④c **对照源 X 通** —— **顺序硬门**：先 `poll_until 120 3` 证 X 活 → 断 Y 断 → **同窗口再断 X 仍活**（只在 Y 之前证 X 活，不排除两者一起死）。**时序守卫**：`reconcilePorts` 只撤 OFFLINE>15min 的 node（`expose.go:484-485`）⇒ `assert_setup` 断 restore−backup < 300s | `reconcile.go:280-295` |
| **B8** 名册精确（#49 加固继承） | restore 后 sqlite3 硬断 `cluster_nodes` 精确 == `{brk1}`（镜像 42 对 #49 的加固；若复活已剪 peer = restore 侧同构缺陷 ⇒ flag-for-main + 新号） | ledger:333-345 |

**DOC-4 修订（roadmap 预登记，94 兑现，不改 architecture.md 的其它部分）**：`tether run`/`exec` **都是 ctl → broker → agent 的转发**（`broker.go:831-832,837`）⇒ broker 被 SIGSTOP ⇒ 转发不发生 ⇒ **进程根本起不来** ⇒ P8 原型经产品路径不可达。**修订文本**：改为「backup → `tether run` PTY 托管进程 → 停 broker → `recovery restore` 更旧 bundle → 起 broker → `restart nats-server` 制造保留托管进程的断连→重连 → register → orphan → drop directive + `killed_orphan`」，并注明「`exec` 子进程结构上不可能成为 orphan（`agent.go:1133-1136`）」。

**为什么用 `restart nats-server` 而不是分区件**：`systemctl restart` 给的是干净 TCP close ⇒ 秒级重连（`agent.go:1481-1490` `MaxReconnects(-1)`）；DROP 分区需等 nats.go 默认 PingInterval 2min × MaxPingsOut 2 ≈ 4-6min。**写进头注。**

---

### §3.2 drill `95-broker-selfheal`（N=2；GREEN）

**观测通道（R-BROKERLOG 的直接后果，drill 42 血泪）**：broker 应用语义 → `/var/log/tether/broker.err`（`broker: ready` 行数增量作 ready oracle）+ sys.events core-sub + 真读写；systemd 生命周期 → journal + `systemctl show`。

**臂 T0-drift**：`grep -qx 'Restart=always' /etc/systemd/system/tether-broker.service` —— T1 的判别性推理**以 unit 内容为前提，前提必须被断言**。
**臂 T0-ctrl（scratch systemd 对照）→ 砍（§11-H）。**

| 臂 | oracle | 机制 / FG |
|---|---|---|
| **T1** **SIGTERM clean-exit（核心）** | `dexec brk1 -- kill -TERM $PID0`（**绕过 systemctl** —— systemd 知道是自己发起的停止时 `Restart=` 不适用 ⇒ 判别性归零，`install.sh:750-753` 逐字）。**oracle-a**：poll journal `Deactivated successfully` **AND** `! journal 'Main process exited, code='`（后者出现 = 不是 clean exit = HARD FAIL 判别子）**oracle-b**：`NRestarts==NR0+1` ∧ 新 MainPID ∧ active **oracle-c（真 ready）**：`broker.err` 的 `grep -c 'broker: ready'` **> RD0**（**不是** `ActiveState==active` —— `Type=simple` 的 active 在 exec 那刻就成立）**oracle-d（功能真恢复）**：`poll_until 90 2 -- dp_curl_ok_body …`（真流量）**FG**：`Result != start-limit-hit` | `serve.go:225,241,247`（SIGTERM → RunE nil → exit 0）+ `grep -c SuccessExitStatus install.sh` = **0** ⇒ 只有 exit 0 算 success ⇒ `always` 救、`on-failure` 不救 |
| **T2** kill-9（G.2） | **oracle-a（与 T1 互为判别子）**：journal `Main process exited, code=killed, status=9`（T1 断其**不**出现、T2 断其**出现** = 证明我们真造出了两种退出语义）**oracle-c（G.2 SQLite 快照恢复，不以「起来了」为终点）**：① `session ls` 列 lab ② `ps -a --json` 注入前 EXITED 行仍在 ③ **ALLOCATED 端口存活** + `poll_until 90 2 -- dp_curl_ok_body` 恢复 ④ **revoke 计时保留**：`history --kind port` 无新 `kind=revoked`（**absence fail-closed**：先断命令 rc=0 且输出非空）**oracle-d（roadmap 订正）**：roadmap 写「agent 自动重连」——**但 broker 崩不断 agent 的 nats 连接**（两个独立进程）⇒ 改断 **`! _agent_journal_after $CURA 'agent: re-registered after reconnect'`**（agent 侧**零扰动**是 G.2 的一部分）。**该阴性断言必须同窗口配正对照**（`node ls` agt1 仍 ONLINE，证明 journal 读路径与 agent 都活着，缺的只有那一行），否则 T3 若不跑就留下 vacuous PASS | 臂间强制隔 ≥15s 功能收敛 poll |
| **T3** `restart nats-server` → **#23 全链路** | **HARD claim（3 条硬断言）**：① `poll_until 120 2` `ActiveState==active` ∧ `Result==success`（**绝不是 `inactive (dead)` / `failed` —— 那正是 #23 的症状；出现 ⇒ ASSERT-FAIL、G1 的 `Restart=always` 修复回归、release blocker**）② 真读写恢复：`dp_curl_ok_body` ∧ `exec agt1 -- true` ③ agent 真重连：`_agent_journal_after "$CURA" 'agent: re-registered after reconnect'`（**这里**才该重连，与 T2 的 anti-oracle 成对）。**RECORDED-not-asserted（mechanism）**：MainPID/NRestarts/`broker: ready` 行数 → 三态分类打印 `SURVIVED-IN-PLACE` / `REVIVED-BY-UNIT` / `REVIVED-AFTER-CRASH`。**这不是 74-round5 被驳回的 measure-and-record**：claim 本身是硬断言，只有**走哪条路**是记录 —— 理由 = #23 的 clean-exit 触发器**源码不可判**（README「The clean-exit trigger is unlocated」）⇒ 硬断「必须走某一条」会造出无法证成的期望 | — |
| **EV** `tetherd_restarted`（inventory row-29） | `ev_sub_start`（照抄 81:63）→ T1/T2 后 `"type":"tetherd_restarted"` 计数**严格增**。**诚实定位**：core-sub 在 **T3（nats 重启）**期间自身会断连 ⇒ **EV 臂只挂 T1/T2**；T3 的 ready 证据走 `broker.err` 行数（无损）。**EV 是佐证、绝不 die-gate** | `broker.go:1018`；`permissions.go:147` |
| **D** DELETING 断点续跑（G.2 ①b） | 见下 | — |

**臂 D 的三配方**（唯一真难点；hermetic `test/p7/audit_e2e_test.go:323` 用 `session.Tombstone` **直写 SQLite** 造 DELETING —— deploy-tier **不可照抄**：cluster 模式下 `processes`/`sessions` 是 replicated state，raft 外写会 fork leader/follower 内容，`broker.go:1130-1136` 明写机理（**roadmap 引用的 `broker.go:1092-1096` 已漂移**））：

- **RECIPE-A（首选，理论确定性）**：让 **raft 活着而 JS 死掉**（两个 quorum 在 tether 里是分离的：raft 在 `:7400` 自有 TCP，JS meta 在 nats route `:6222`）。`sctl brk2 stop nats-server`（**只停 nats，不碰 brk2 的 tether-broker/raft**）→ **前证三条**：(i) brk1 `cluster status --json .leader_id=="brk1"`（raft quorum 在）(ii) `nats stream ls` 超时/503（JS 死）(iii) `node ls` rc=0（core NATS 活）→ `session rm doomed` → tombstone 经 raft 成功、phase ② 在 5s 内失败（`sessions.go:179-197`「NON-fatal … will resume on next boot」）→ **中间态硬前证**：`sqlite3 -readonly` `state=='DELETING'` ∧ `history-doomed` 流仍在 ∧ **`broker.err`** 出 `session rm finalize failed; will resume on next boot` ∧ 新调用被拒 `session_not_found_or_deleting` → 恢复 brk2 nats → `restart tether-broker` on brk1 → **oracle 三点齐**：流消失 ∧ 行数 0 ∧ **新 `session_destroyed` sys.event**（phase ④ 的专属产物；只断行没会被任何路径满足）→ **无关行完好**（lab + `history-lab` + agt1 + 数据面 X，对照源成功）。
  - **[SB-95-DELETING] 承重未知**：brk2 的 tether-broker 会不会因本机 nats 停而 crash-loop（README 记过「restarting a healthy follower's nats → exit-70」），从而让 raft quorum 抖（撞 #44）。支持存活：broker 用 `MaxReconnects(-1)`（`authcallout.go:34-37`）、无 `ClosedHandler`；README 实验 (b)「stop nats; dwell 8s → broker stays active」。**必须实测**。
- **RECIPE-B（fallback，概率造法 + 硬前证门）**：K≤**4**（**K≤8 叠加 T1/T2/T3 ≈ 12+ 次重启，`StartLimitBurst=5`/`RestartSec=2` 必撞** ⇒ 每轮之间强制 `systemctl reset-failed` 并 `[env: systemd StartLimit，非 tether 面]` 标注）。每轮 `fault_freeze_on`(SIGSTOP) 竞态 → 读 `state`；`DELETING` ∧ 流在 = **硬前证达成** ⇒ 不可能假绿；不中就 INCOMPLETE。
- **RECIPE-C（出口）**：`not_covered "95-D DELETING-resume: 无法在真栈上把 session 稳定停在 DELETING（raft 与 JS quorum 在 N=2 同生共死；cluster 模式禁止 raft 外直写 sessions 行 — broker.go:1130-1136 的 fork 机理）" "hermetic: test/p7/audit_e2e_test.go:323"` → **INCOMPLETE**（诚实登记）。

---

### §3.3 drill `96-mid-flight-chaos`（N=3；**PRODUCT-RED 源码确证**）

**两条推翻 roadmap 底稿的源码事实**：
- **#57**：roadmap 写「bucket watchdog 兜底清理 + audit failed」。**watchdog 随进程死** —— `entry.cancel = b.startTransferWatchdog(b.runCtx, entry)`（`transfer.go:593` push / `:704` pull）挂在 broker 的 runCtx；`transferTracker` 是纯内存 map（`:99-104`），重启 `newTransferTracker()`（`broker.go:602`）为空；`handleEvTransfer` 对 agent 迟到的 finalization 走 `preview == nil → return`（`:816-819`）**静默丢弃**。⇒ **合成 `failed` audit 永不写。roadmap 的 GREEN 期望结构上不可达。**
- **#58**：清理**不是 watchdog 干的**，是 boot reconciler `reconcileXferObjectsOnBoot`（`transfer_reconcile.go:27-94`，**仅由 `broker.go:942` 在启动时调用一次，无周期 pass**），而它**第一道门就是** `if !b.reaperMayDelete() { return }`（`:34-36`），`reaperMayDelete` 在 cluster 模式下 `!IsLeader() → false`（`clusterwrite.go:478-486`）。

**SETUP 硬前提（三条 `assert_setup`）**：`sim_leader == brk1` ∧ `a_non_leader_voter` == brk2 ∧ agt1 的 `tunnel_addr` == `brk2:7000`。**三者是同一个节点是 96 的硬前提，不是巧合** —— 任一漂移 ⇒ `reaperMayDelete()` 从源码保证退化成掷硬币 ⇒ 假 GREEN。

| 臂 | 内容 | 预期 |
|---|---|---|
| **A** tier-B 在飞杀 home（**#57/#58**） | ① 基线：agt1 造 ≥12 MiB（**> `transferTierAMaxBytes` = 8 MiB**，`transfer.go:52`）+ **先跑一次完整 tier-B push 成功** + `history --kind transfer` 抓到 `start`+`complete` **成对**（audit 读路径本来工作的前置阳性对照）+ **该次的 object 事后被回收**（否则「对象仍在」可能只是「tether 从来不回收任何 object」）② 观测：`history --kind transfer`（真读 R=3 复制的 `history-<sid>` 流，**跨 broker 死活可读**）+ `nats … obj ls OBJ_xfer-<sid>` ③ 边界：poll 到 `start` 行出现（tier 字段 == `b`）**且** bucket 对象已出现 → `node_kill brk2`（**固定非-leader voter ⇒ `reaperMayDelete()==false` 由源码保证、非 race**）④ **窗口 = `5min + 90s`**（超过 timeout 后**没有任何代码路径**会再写 ⇒ `2×` 无机理依据；头注写明裁掉的理由）；窗口内每 30s 采样一次打日志 | **PRODUCT-RED** |
| | **A1 的 INVERTED 块必须四态穷举（R-EXHAUST —— `else` 兜底会把「history 读不到」记成一条捏造的 gotcha）**：`_x=$(history --kind transfer -n 200 \| grep -F "$TID")` → `''` ⇒ `_as_fail "#57 UNJUDGEABLE — no audit row at all"`；`*complete*\|*failed*` ⇒ `_as_fail "APPEARS FIXED"`；`*start*` ⇒ `product_red "#57 dangling start …"`；`*` ⇒ `_as_fail "UNJUDGEABLE"`。**A2 同纪律。** **对照源**：注入后新起一次小 tier-A 传输 → 断 start+complete 成对可读 | |
| **B0** `run --ack-alerts`（**inventory:122/226 的 S9☐ 欠账**） | 分区/kill 后必有 severe 告警态 → `tether run --nats-url nats://brk2:4222 agt1 -- true` **被拒**（锚 severe-alert 拒绝串）→ 同命令加 `--ack-alerts` **成功**（对照源）。零额外 fixture，~30s。**若实测 `run` 在 severe 下不拒** → `not_covered` + 与 90 的 severe-banner 语义差异登记，**不得继续留空** | GREEN |
| **B** run-PTY 杀 broker（**[SB-96-3] 已由源码关闭**） | **钉定路径**：`ResolveNATSURLFromHome`（`internal/cli/natsconn.go:86-88`）`if flagChanged { return flagVal }` ⇒ 显式 `--nats-url` **绝对优先、不扩展成多 endpoint、不做 discovery refresh** ⇒ **产品一等的路径钉定手段，不是 harness 造的**。**注入前证明**：`dexec brk2 -- curl -s 127.0.0.1:8223/connz` 的 `connections[].name` 同时含 `tether-cli:<sid>` **和** `tether-agent:<sid>:agt1` **[SB-96-1]**。**极性已定**：`SubjNodeHeartbeat` 的 publisher = **agent**（`agent.go:1456`）、subscriber = **ctl**（`run.go:415`）、无 broker 中转；`runWatchdogTimeout()` **默认 15s**（`run.go:381`）；超时注入 `RunChunk{Kind:"failed", Reason:"agent unreachable: no heartbeat for 15s"}`（`:453-456`）。`node_kill`+`node_start` ≫ 15s ⇒ **会话必死** ⇒ **主设计 = 出口 (b)「优雅带原因终止」**（GREEN by design），(a) 作 APPEARS-FIXED 守卫。**成对臂**：`TETHER_RUN_LIVENESS_TIMEOUT=180s`（产品文档化的 knob，`run.go:377-379`）→ 观察去掉 watchdog 后会话能否续活。**头注必须写明**：用文档化 knob ≠ 改环境规避缺陷 | GREEN + **DOC-28** |
| **C** expose crash → RETURN 窗口（**范围收窄；#29 不重复起诉**） | `rehome_events.go:52-53` —— 常规 expose 在 crash 下**不自动 rehome**、搁浅到 home RETURN（#29 已 LIVE-CONFIRMED 6 次）⇒ roadmap 底稿的「rehome 窗口计量」在 crash 路径**不成立**。**重定义** = RETURN 恢复的**时序形态**（71 断了「RETURN 后同端口/同 epoch」，没断时序）：`dp_curl_ok_body` 基线 → 同秒 `node_kill` → `dp_curl_refused`(7，**佐证不新起诉**) → `node_start` → **唯一硬断言 = 最终恢复**（`poll_until 240`）+ 同端口 + `.moved==false`；**实测耗时 measure-and-record 打日志**（照 #33 在 73 的先例，OQ-7）。**+ `ev_sub_start` 断 `home_reassign_failed`（inventory:54 点名给 96）**：捕不到 → `not_covered` 的理由**必须是**「实测该 kind 在 crash 路径不发火 + `rehome_events.go:52-53` 的源码机理」，**绝不是「无 reader」**（§1.4-R-EVENTS） | GREEN |
| **D** **分区 leader（旗舰臂）** | ① 基线：3 VOTER + leader==brk1 + **经 brk1 真写成功** + `fault_assert_reachable brk2 brk1 6222` ② 观测：`dexec`（不走网络）+ 各节点本机 admin socket（unix socket，分区期间仍答）+ loopback 8223 ③ 边界：`fault_partition_on brk1 6222 7400`（**刻意不切 4222**）→ **三重自证**：`fault_assert_blackholed brk2 brk1 6222`(124) ∧ 同 7400 ∧ **`fault_assert_reachable ctl1 brk1 4222`（选择性对照 —— 全网断的 brk1 当然不能写，那什么也没证明）** ④ **D1** 幸存侧 leader ∈ {brk2,brk3}（**必须从 brk2/brk3 读**）**D2** **新 leader 真写成功**（多数派活着的唯一合法证据）**D3** brk1 admin socket 仍答（anti-vacuous liveness）∧ 经 brk1 的**写**被拒 `not_leader\|ErrNotLeader\|leadership` **D4** brk1 `MainPID==MainPID_0` ∧ `NRestarts==NR0` ∧ `ss -ltn`/`/proc/net/tcp` 仍列 `:7400`/`:4222` ⑤ 愈合：**D5** 全 3 节点 `cluster status` 报同一 leader（`sort -u` 恰 1）**D6 无脑裂（结果级、非状态字段）**：从 **brk1**（前少数派）**读回 D2 在多数派写的那一行**，阴阳成对 ⑥ `fault_cleanup_all` | **总函数三分支**：POSITIVE（MainPID 稳 ∧ NRestarts 不增 ∧ D3 答）→ GREEN；**#59**（NRestarts 增 ∧ crash-loop）→ `product_red`；**INCONCLUSIVE** → `not_covered`，**绝不静默 POSITIVE** |
| | **#59 的引用订正**：草稿的 `broker.go:948-958` 错（现为 JS-probe 的 else 分支 Debug 日志）。正确 = **`broker.go:956-985`**：`:956-971` = lone N=1 + js==nil → 硬 error；`:973-985+` 的 voters>=2 分支注释 `:976-978` 逐字「Ejection is NOT locally provable … emit a **RANKED DIFFERENTIAL** … **never a hard assertion**」⇒ **不得预设它必崩**；签名必须来自 **broker.err**；crash-loop 与否留 Stage-B **[SB-96-B-DISCRIM]** | |
| **F** 双故障（G.1×G.2 交织） | ① 基线：agt1 上 `exec sleep 9999` ×2（`ps` RUNNING×2 + `pgrep` 计数==2）**+ agt2 持有一条 RUNNING 进程（未被杀）** ② 观测：`ps -a` + `history --kind proc` ③ **同时** `node_kill brk2` **且** `node_kill agt1` ④ `node_start` 两者 → **两个子臂（V-first / agt-first）各一次，断两序都收敛**：① agt1 的两条翻 **EXITED(-1)** ② **agt2 的那条必须仍 RUNNING**（唯一能证明 reconcile 是 node-scoped 而非全表误杀的判别子）③ `history --kind proc` 出 **`kind=reconciled_closed`**（**订正**：`schema/audit.go:36` `AuditProc.kind ∈ {start,exit,reconciled_closed,killed_orphan}`；`reconciled` 是 **`AuditPort`** 的 kind，`:52`）④ agt1 回来后**新起**一个进程并跑通（证明 agent 未 wedge） | GREEN；不收敛 → **#62** |
| | **删除 `pgrep -x sleep == 0` 断言**：`node_kill` = `docker kill` 容器 → PID1 被 SIGKILL → 容器内一切进程随之消失 ⇒ **该断言在 tether 一行代码都不跑时必然成立** = 注入本身保证的恒真式。头注逐字：「双故障臂结构上不可能有 OS-truth 腿：注入销毁了被观测的 OS 状态本身；OS-truth 腿只存在于 94-B（那里进程真的活过注入）」 | |
| **E** disconnect 语义对照 | **砍**（§5.3-T8） | — |

---

### §3.4 drill `97-soak-cycles`（参数化；GREEN，阈值 UNCALIBRATED）

**参数**：`: "${SOAK_CYCLES:=6}"`（= 斜率判据的结构下界；取 3 则唯一实质 oracle 每次都走 not_covered 分支）；`: "${SOAK_SETTLE:=25}"`。**[SB-SOAK]** `cmd_drill` 的 `env INSTANCE=… sh "$_script"` 继承外部 env；`remote.sh` 的 ssh 层透传未核 → 出口 (b) = `$HERE/.soak-cycles` 文件（rsync-class）。

**victim 轮转（承重 —— 不这样设计泄漏 oracle 结构性无效）**：每轮都杀被观测进程 ⇒ 它的 fd 每轮归零 ⇒ **永远测不出泄漏**。故 **brk1（leader）与 agt2 永不做 victim**，PID 世代横跨全部 N 轮 = **主判据**。轮转 `cycle % 4`：

| i≡ | 注入 |
|---|---|
| 0 | `node_kill agt1` → `node_start agt1` |
| 1 | `systemctl restart tether-broker` on brk3 |
| 2 | `fault_partition_on brk2 6222 7400` → 愈合 |
| 3 | **传输并发**：后台 tier-B pull（≥12 MiB）与该轮 brk3 restart 并发；非-vacuity = **两半**：① restart 真发生（brk3 MainPID 变，确定性）② 该轮唯一源路径出现 `history --kind transfer` 的 **start 行**（证明 transfer 真进入产品路径）。**外审 round-2 R2-F2**：restart-disrupted 的后台 pull 可能在注册前退出，终态行在 chaos 下不可靠 ⇒ 只要 start 行（进入产品路径）即可，未出现则该半 `not_covered`，绝不用「restart 变了」冒充「四型均自证」 |

**为什么第 4 型不可省**：roadmap §3-97 逐字列了四型（agent kill / broker restart / **分区** / **传输并发**）。且它是 **#57/#58 的累积效应放大器**（soak 下 orphan object 累积 → 撑爆 per-session bucket 8 GiB cap（`transfer.go:67`）= **#21 族复发**）—— 不做它，97 就丢掉了它唯一能对 96-A 贡献的东西。若判定代价过高 → **必须** `not_covered "97 transfer-concurrency injection: <实测理由>"`，绝不静默丢。

**采样 / PID 重解析**：t0 = **warmup 后**（非 boot 时 —— 懒初始化让早期计数虚低 ⇒ 假 RED）；此后每轮稳态 settle 后一次 ⇒ N+1 样本。`soak_pid` 每次**重解析** `systemctl show <unit> -p MainPID --value`，**NEVER 缓存**。
**PID 世代守卫（反-假绿）**：brk1/agt2 的 pid 在全 N 轮**必须不变**；变了 ⇒ 序列被重置 ⇒ `not_covered "brk1 MainPID changed at cycle <i> — the fd/RSS series was reset; leak judgement invalid for this run"`（**不得**用重置后的序列判绿）；该意外重启本身是发现 → 记 `NRestarts` + 分诊。

**统计判据（§11-G：首版 UNCALIBRATED）**

| 指标 | 判据 | 首版处置 |
|---|---|---|
| fd | ① 有界高水位 `max ≤ fd_0 + K_fd` ② 斜率 `mean(last 3) − mean(first 3) ≤ ε_fd × (N−3)` | **K_fd=64 / ε_fd=2 fd/cycle（4× 放宽）**，头注标 `[UNCALIBRATED — widened 4x pending SB-97-2; tighten after baseline]`。理由：ε_fd=0.5 在 N=6 时总容差 **1.5 fd** —— 在一个每轮杀 agent、重启 broker、切分区的系统里几乎必然被正常抖动击穿 = 先射箭再画靶 |
| Threads | `max ≤ thr_0 + 2×nproc + 16` | 同上 |
| RSS | **只用斜率 + 相对上界**（`mean(last3)−mean(first3) ≤ ε_rss×(N−3)` ∧ `mean(last3) ≤ 2×rss_0`）。**绝不用绝对高水位**（Go GC 锯齿 ⇒ 必假 RED） | ε_rss=4 MiB/cycle |
| 样本量门 | `N ≥ 6` 才判斜率；否则 `not_covered "SOAK_CYCLES=<N> < 6: slope judgement is not statistically meaningful"` | — |

**非空性守卫（头等）**：一个什么都没干的循环当然不泄漏 ⇒ **每轮必须断注入真生效 + 恢复真发生**：agent-kill 轮（离开 ONLINE → 回 ONLINE）；broker-restart 轮（`MainPID` **确已改变**）；partition 轮（`fault_assert_blackholed` 退 124 + 愈合后 leader 唯一 + **愈合后的真业务必须经被分区那侧执行**）；每轮末一次真业务（`exec` 返回 sentinel）。任一失败 ⇒ **ASSERT-FAIL**（不是泄漏判据的 not_covered）。

**独立崩溃/完整性 oracle（与泄漏 oracle 正交，绝不互相顶替）**：
```
JOURNAL_BAD_SIG='panic: |goroutine [0-9]+ \[running\]:|FOREIGN KEY constraint failed|WARNING: DATA RACE|database disk image is malformed'
```
- **broker 侧读 `/var/log/tether/broker.err`**（R-BROKERLOG：Go panic 走 stderr → broker.err，**不在 journal**）；agent 侧读 journal。
- **为什么 journal 抓不到慢泄漏**（roadmap 要求论证）：journald 记录**事件**；fd 泄漏在撞 rlimit 之前**零日志**（6 轮泄漏 6×few fd 距阈值差 3–5 个数量级 ⇒ literally 零输出）；RSS 同理（直到 OOM-kill 才有 kernel 一行 —— 那时已不是「检测」而是「事故」）。
- **SB-97-1 CLOSED（主进程实测）**：镜像有 `/var/log/journal`（持久 journal）、`node_kill`/`node_start` 保留容器写层 ⇒ **journal 跨 boot 存活** ⇒ 跨 boot 检查成立（去掉 `-b`；出口 (a)）。

**唯一 landing verdict 的四个陷阱**：T1 参数（env）· T2 断言数 = f(SOAK_CYCLES)（README 不写死）· **T3 一轮失败污染 landing = 设计意图，不是 bug**（禁「只报最后一轮」/「per-cycle 独立 verdict」）· T4 `not_covered` 循环外只调一次。

---

## §4 — Gotcha ledger（**#50+**；现有 max = **#49** 实读确认；DOC **#17+**，**DOC-16 = 保留未用，勿静默复用**）

> **六路撞号已解**：6 份草稿**各自**把自己的头号发现编成 `#50`，`DOC-17` 被 5 份占给 5 件事。以下是主进程 ratify 的全局分配表。**纪律**：号在本表 ratify；**ledger 小节由 Stage-B 真跑后补**；drill 内 `product_red "#N"` 字串与本表**零漂移**（G-B 的 M11 血泪：drill 先写 `#42` 而 ledger 顶 #34 ⇒ 收工闸不可过）。

| # | drill | 一句话 | 机理（file:line） | 钉法 | 默认 & 分支条件 |
|---|---|---|---|---|---|
| **#50** | 50-R3 | `cluster doctor --offline --db <不存在>` 报 `db PASS` + `0 fatal` + **exit 0**；「迁移源可达」承诺结构性为假 | `clusteroffline/doctor.go:82-87` → `storage.OpenReadOnly` = `storage.go:105-111` 裸 `sql.Open`（惰性、无 Ping/PRAGMA）；`cluster_natsconf.go:520-523` | R-INVERTED 四态 | **PRODUCT-RED（已实证）** — roadmap 写「plan 定格」，定格结论就是 RED，不是开放题 |
| **#51** | 51-G1 | `recovery restore` 不 apply（也无法 apply）broker.yaml cluster seam；§5.2 步3 在 fresh DR 箱上必 FATAL | `cluster.go:794-804` vs `cluster_backup.go:123-129`（**无 `--config`**）；`install.sh:548-556`；`cutover.go:117-120` | `assert_bug` + `broker.err` 签名 | 默认 **PRODUCT-RED**。分支：串 actionable ⇒ 主进程可改判 DOC-19（**倾向 gotcha**：init/restore 姊妹对照证明产品自认该自动化） |
| **#52** | 51-G2 | `recovery restore` 不渲染、也不提示 nats.conf；fresh box 的 stock conf 无 auth_callout | `cluster.go:824-826` vs `cluster_backup.go:115-119`；`install.sh:690-704`；`serve.go:203-218` | `assert_bug` **[SB-51-NATSSIG]** | (a) 抓到 auth/nkey 硬墙 ⇒ RED；(b) 竟能服务 ⇒ **撤 #52**、G2 改 `assert_ok`、降 DOC-19 |
| **#53** | 51-J | backup bundle 不含 JetStream → 全灭 DR 后 history/audit 全失且从不告警 | `backup.go:87`；`restore.go:313-321`（`audit_published_index` 归零反使 re-derive 永不回填） | `product_red` / `not_covered` **[SB-51-HIST]** | 默认候选。(a) 空/无流 ⇒ RED + DOC-19；(b) 有行 ⇒ 撤销 |
| **#54** | 52-B2/B3 | account.nk/CA 轮换无产品级 re-render 与 verify；`reconcile nats --all --wait` 报 false all-clear；`doctor` 对 skew 失明 | `cluster_reconcile.go:78`（自陈 NEVER bumps）；`natscluster/config.go:167` ← `topology_reconcile.go:233` ← `serve.go:203-218`（**内存 seed、启动加载**）；`preflight.go:58-140` | R-INVERTED（**exit 0 ⇒ 不可用 `assert_bug`**：会判 APPEARS FIXED → ASSERT-FAIL = verdict 误分类） | **PRODUCT-RED（必现）**。**B0 的 reconciler-liveness 正例是机理成立的前提** |
| **#55** | 52-B5d | 跨 route 的 auth_callout queue group 使 account.nk 滚动轮换存在 auth 拒绝窗口，无原子切换动词、runbook 无警告 | `authcallout.go:95-101`；`error_hints.go:145-155` | B5d 三点齐 **[SB-52-B5]** | (a) 本地优先成立 ⇒ **降级为「仅 broker-down 窗口可达」的有界窗口缺口**（吸取 s6-s8-review M1：有界瞬态 ≠ 永久 bug）；(b) 不成立 ⇒ RED「滚动轮换期约 50% 认证失败」。**B5d 无论如何都跑** |
| **#56** | 52-A4 | `rotate-tunnel-cert` 的**循环建议**：follower 上说「re-run on the leader host: brk1」，leader 上对同一目标说「transfer leadership to brk2 first」 | `clusterstatus.go:649-657` + `cluster.go:625` vs `clusterdrain.go:386-388` | 成对断言 | 候选。**原「裸 raft 错」立项被源码推翻、已废** |
| **#57** | 96-A1 | 在飞 tier-B 传输的 home broker crash 后**终态 audit 永不写**，`history --kind transfer` 留悬空 `start` | `transfer.go:99-104`（纯内存 map）+ `broker.go:602`（重建为空）+ `:593/:704`（watchdog 挂 runCtx）+ `:816-819`（`preview==nil` 静默 return） | R-INVERTED 四态 | **PRODUCT-RED（源码确证）**。FLIP = tracker 可重建 or boot reaper 写终态 |
| **#58** | 96-A2 | cluster 模式下**非-leader** home broker 重启后 orphan xfer object 永不回收 | `broker.go:942`（唯一调用点、无周期 pass）+ `transfer_reconcile.go:34-36` + `clusterwrite.go:478-486` | victim 固定非-leader voter ⇒ **源码保证、非 race** **[SB-96-2]** | (a) 对象仍在 ⇒ RED；(b) 消失 ⇒ 用 `transfer_reconcile.go:90` `"orphan xfer objects reaped"` 判别；非 boot-reap 所为 ⇒ `not_covered` + 根因未定。**运维后果**：反复 crash 累积 → 撑爆 8 GiB bucket cap = **#21 族复发** |
| **#59** | 96-D | 被分区的少数派 broker 无法「只读存活」——restart 撞 voters≥2 && js==nil 分支后 crash-loop | `broker.go:956-985`（**注释 :976-978 明说这里给的是 ranked differential、never a hard assertion ⇒ 不得预设必崩**）；`install.sh:754` | 总函数三分支 **[SB-96-B-DISCRIM]** | 候选。复现 = **#35 CANDIDATE 的分区触发器首证**（与 22 的 peer-kill 互补不重复） |
| **#60** | 97 | broker/agent 的 fd 或 RSS 在 N 轮混沌后越界 | — | 统计判据（首版 UNCALIBRATED） | 默认 GREEN |
| **#61** | 94 | G.5 审计痕迹缺失（`history --kind proc` 无 `reconciled_closed` / orphan kill 无 `killed_orphan`） | — | — | 默认 GREEN |
| **#62** | 96-F | agent + home broker 双亡双回后 G.1×G.2 某一序不收敛 | 待定格 | — | 默认 GREEN |

**复用既有号（blast-radius 扩充，无新号）**

| 号 | G-C 撞点 | 依据 |
|---|---|---|
| **#6** | **root-owned restore**（50-J1 = 首次活体复现） | `docs/broker-ops.md:621-626` **逐字列了 `restore`** ⇒ **三份草稿各自的新号全部并入 #6，不开新号**。**推论**：以 `-u tether` 跑 restore = `[operator per broker-ops #6]`，不是 sim 图方便（§11-J） |
| **#31** | **+52(C7 spine，且在 typed-confirm **之后**才拒 = 新面)** · **+51 的 re-grow** · 96 的 fixture | `cluster_operation_controller.go:175-185` |
| **#45** | +52（retire START 后停滞；**但 52 不以 terminal RETIRED 收口 ⇒ 与 52 的 C7 oracle 无关**） | ledger:286-290 |
| **#23** | **+95（首个行为级 pin）** —— 此前只有 doctor 静态 drift 检查。95 的结论会**反向影响 #35 的 CANDIDATE 状态** | README:352-355 |
| **#29** | +50/51（expose 业务态在 N≥2 时）+96-C | ledger:109-164 |
| **#48** | **+96 的分区臂 = #48 的教科书形态**（agent 与少数派同侧 = 活的 NATS 孤岛）+52 的滚动重启。**禁令继承**：不得 restart agent / 改 env / 删 cache / 停旧 broker（ledger:322） | ledger:313-331 |
| **#36** | +52-D3（`--yes` = cobra unknown flag，与 `recovery node remove` 的 Tier-2 rejector 分歧、同族） | `cluster_retire.go:138-142` |
| **#49** | 94-B8 / 51-F-b 的 restore 后**硬断名册精确 `{self}`**（镜像 42 的加固）；若复活 peer = restore 侧同构 ⇒ flag-for-main | ledger:333-345 |
| **#42** | 95/96 的 oracle **避开 `--remote`**（用 on-broker socket / 数据面），否则重现 TFence 窗口噪声；50-H2 的 poll 越过 TFence≈10s | ledger:275-279 |
| **#47** | `grow_to_3 retry=1` 兜底；但 50/51/52 的 #31-EXPOSURE 臂**不得让 retry 洗掉 leftover-op 态** | ledger:297-311 |

**DOC 缺陷（DOC-17 起；DOC-16 保留未用）**

| DOC | 内容 | 证据 |
|---|---|---|
| **DOC-17** | runbook §5「ONLINE backup … **any node, leader OR follower**」(`:523`) / 「A single backup off **ANY node** is the whole committed state」(`:533`) 与产品 leader-gate + bundle 无 JS 矛盾；**`clusterbackup.go:16-18` 的函数 docstring 自己也陈旧**、与其下 :30-46 自相矛盾 | 50-F1/F3/L3 |
| **DOC-18** | runbook §5.1 `:538`「preserving it at `<db>.bak`」实为 **`<db>.pre-restore[.N].bak`**（CLI 自己印对 `cluster_backup.go:95`）；且 §5.1/§5.2 缺 `sudo -u tether`，与 broker-ops #6 相悖。⚠ **勿与 §4 `cluster init` 的 `tether.db.bak`（`init.go:395`）混淆**（那条是对的、归 43） | 50-J1/51-F-c |
| **DOC-19** | runbook §5.2 缺 seam + nats.conf 两步；从不交代 restore 剪掉的旧 peer 怎么办（它仍持分叉 raft 日志 + clustered nats.conf；§1 join flow 是 **fresh-node** 流程） | 51-G1/G2/H2 |
| **DOC-20** | **`broker-ops.md:479-491` §7.4 通篇不提 `tether cluster backup`**，只教 `sqlite3 .backup` + `tar jetstream/` + `nats stream backup`；roadmap §4.3 的「§7.4 单机备份 = cluster backup 同机制」**是事实错误** | 闸门行订正，§7 |
| **DOC-21** | runbook §2.1 step 4 顺序颠倒 + 「re-render」动词失实 + 未警告轮换期 auth 拒绝窗口 | 52-B2/B4/B5d |
| **DOC-22** | `install.sh:553` 注释样例 `data_dir: $LIB_DIR/raft` 与实写 `$LIB_DIR` 不符（raft/ 是其**子目录**，`restore.go:196`）⇒ 照抄注释 → 找 `/var/lib/tether/raft/raft/` → 永不进 cluster 模式 | 51 头注（**归属**：疑属 S5-32/install.sh 面 → 登记但不在本批修） |
| **DOC-23** | `clusterwrite.go:183-186` 的补救提示 `re-run tether cluster rotate-tunnel-cert` 在砖化态**不可达**（`broker.go:691` ≪ `:1060`）；唯一出路是恢复旧 cert 文件 | 52-A8 |
| **DOC-24** | `StaleAfter`/`OfflineAfter`/`PortRevokeAfter` 无 broker.yaml/flag 暴露（只有 `broker.Config` 字段）⇒ 运维不可调、drill 不可缩 | 94-A1 头注 |
| **DOC-25** | `DropProcesses` 在 reconnect 路径无计数日志（初连路径有 `drop_procs=N`） | 94-B5 头注 |
| **DOC-26** | `sys.events` 有 ACL 可读（`permissions.go:36/:147`）却无一等 operator reader（`cmd/tether/admin.go` 无 events verb）——architecture 把 sys.events 当**运维契约**，operator 只能自己写 `nats sub` | 94/95 头注。**主进程裁定：DOC 不是 gotcha**（机制在、缺的是 UX；架构承诺的是「事件是运维契约」而非「有 CLI 动词」） |
| **DOC-27** | runbook:524 的 `cluster backup --out /var/backups/…` 例子在 stock install 上不可直接跑：`/var/backups` 不存在 ⇒ `MkdirAll` 先跑 ⇒ 撞 `prepare bundle parent` + **`CodeStoreError`(exit 70)** | 50-C **[SB-50-7 已由源码关闭]** |
| **DOC-28** | `docs/usage.md` 未定义 `run` 会话跨 broker 重启的语义（watchdog 15s 合成 failed 终止是设计，但文档没说） | 96-B |
| **DOC-4** | 既有预登记「(S9-94 修订)」→ **94 兑现修订文本**（§3.1），不得再推 | 94 |

---

## §5 — NOT-COVERED（永久 + gated）+ 显式裁剪

### §5.1 永久（各附源码引用的真理由）

| 项 | 理由 |
|---|---|
| **goroutine 数**（97） | ✅ 主进程复核：`grep -rE 'pprof\|expvar' cmd/ internal/` = **零命中**；broker 的三个 HTTP listener 无一服务 `/debug/pprof`。hermetic 用 `runtime.NumGoroutine()` **在进程内**，对跨进程 drill 结构不可用。`/proc/<pid>/status` 的 `Threads` 是 **OS 线程(M)**，不是 goroutine(G) —— 10k goroutine 泄漏可显示**零** Threads 增长 ⇒ **拿它当代理本身就是假绿 oracle**。FLIP = 产品加 pprof/expvar/metrics gauge。**登记形态 = 批级（§11-E 走 (iii)）** |
| **P8 24h soak parity**（97） | 三条 delta：① DURATION（默认 6 轮 ~16min vs 24h：timer wraparound、cert/lease/TTL 到期、日志轮转、JS 保留边界、低于 ε 的慢泄漏结构上够不到）② CADENCE（背靠背 ⇒ 需长静默间隔的缺陷从不被 exercise）③ TENANCY（simcluster 是按需 dev 工具 + throwaway instance，README NON-GOALS 明确不常驻）。MITIGATION（**不是替代**）：`SOAK_CYCLES=48 SOAK_SETTLE=90 ./remote.sh drill 97-soak-cycles` ≈ 24h，发布前手动跑。**P8 24h 出口仍欠 staging/实机** |
| **94 的 PID-reuse 支**（`reconcile.go:185-210`） | 需在 agent 侧确定性造「同 ULID、不同 boot_id/start_time_ticks」——`readBootID()` 读 `/proc/sys/kernel/random/boot_uuid`，容器内无法在不重启内核下改；重启 agent 清空 `a.procs`（ULID 消失）⇒ **结构不可造**。hermetic：`reconcile_marks_test.go` |
| **94 的 `agent exit` 支**（`reconcile.go:213-225`） | `buildLocalSnapshot` **只发 `State:"running"`**（`agent.go:1096` 硬编码）⇒ **产品从不发 `exited`** ⇒ 该支真栈不可达（broker 的防御性代码） |
| **`restore --raft-addr` 的「真实换 IP」动机**（51） | 机制面已由 F-c/F-d 覆盖（值确实变、sqlite3 可观测）；**动机面** sim 不可构造：sim 以稳定 DNS 主机名寻址（README「Addressing = hostname」/ NON-GOALS「Single host」），重建箱子后 `brk1:7400` 依旧正确。hermetic：`internal/clusteroffline/restore_test.go` |
| **broker-ops §7.4 的 jetstream tar / `nats stream backup` 面** | 非 tether 动词、属 nats 生态运维（闸门行拆分，§7 + DOC-20） |
| **`ops confirm` / 多-agent DR 的 #29 后果 / 24h pin 窗口过期 / `--force` 孤儿化 / arm-token replay** | 分别归 G-B-40 / 71 / hermetic（`clusterwrite_test.go:10`、`tunnel/d6_test.go:175`）—— G-C 不重复 |
| **incident export `--sid`** | 纯查询过滤，hermetic 已密（§0.3 深度闸门裁剪） |
| **`cluster doctor --raft-addr`/`--nats-route` 端口 bindability** | 设计给 pre-init 主机；运行中 broker 上语义误导 |

### §5.2 gated / 条件

- **52 的 C7 guide/alert 生命周期** — gated on #31/#45（§2.3-D-spine 分支 (i)）。
- **51 的 re-grow 数据面** — gated on #31/#29。
- **95-D DELETING 续跑** — gated on SB-95-DELETING（三配方全败 ⇒ RECIPE-C）。
- **96-C 的 `home_reassign_failed` event** — **理由必须是实测**（§1.4-R-EVENTS），不得写「无 reader」。
- **50-O 的 [remove … rename] 窗口段** — `restore.go:183-187` **先 `os.Remove`** 再 `:188` `os.Rename` ⇒ 有一段 `tether.db` **根本不存在**；此态下 `assertNoInterruptedRestore`（`cutover.go:151-163`）拿不到文件 ⇒ **fail-closed 契约在这一段完全未被测**。O 臂的命中检测须加第二判据（`! test -e tether.db` 也算一种态）**[Stage-B 分诊]** —— **这是 50 唯一可能藏 fail-open 的地方**。

### §5.3 **显式裁剪**（深度闸门答不出「部署层新增什么」/ 与既有臂重复 —— 记理由，**不记 `not_covered`**，否则白吞 INCOMPLETE）

| # | 对象 | 理由 | 省 |
|---|---|---|---|
| T1 | `assert_bug_refusal` 新原语 | 与 11 处在用的 `assert_ok`+`product_red` 冗余；动外审三轮定的 SSOT（§11-B） | 3 处 SSOT 改动 |
| T2 | 50-臂P（re-grow） | 与 51-臂I 重复；给 50 白加 #31 双稳面（50 的 landing 因此**稳定单值 PRODUCT-RED**，对 §8 manifest 是净收益） | ~4min |
| T3 | 50-臂N（`.pre-restore.bak`） | 被 51-F-c/F-d 严格覆盖且更强（拿到 `.N` 递增证据）；断言并入 51-F-c 并改用 sentinel 行 | ~2min |
| T4 | 50-臂M2（splice-index） | 与 M1 同门族、相邻源码行（`:261-274` vs `:277-280`），零新增部署信息 | ~1min |
| T5 | 50-臂G3（root sidecar spike） | 纯推测；root-owned data dir 已是 **#6** 的地盘，J1 已覆盖 root-op 后果 | ~3min |
| T6 | 50-臂O 的 kill-9 循环 → **改**确定性注入 | 10–100ms 窗、K×~45s ≈ 6min、大概率 `not_covered` ⇒ 最差的收益/成本比。runbook 承诺的是**中断后可续跑**，不是**中断必须由 kill-9 造成**；满盘/权限是更真实的运维故障，同一 fail-closed 契约、~90s、零 INCOMPLETE 风险 | ~5min + 一个 INCOMPLETE |
| T7 | 95-T0-ctrl + `sd_scratch_up/down` | 测 systemd 不测 tether（§11-H） | ~15s + StartLimit 余量 |
| T8 | 96-臂E（disconnect 语义对照） | **自陈「不对 tether 下任何结论」** ⇒ 定义上过不了深度闸门；它想防的（有人把 D 臂改回 disconnect）**已被 `fault_assert_blackholed` 的 124 门 fail-closed 防住** | ~2min |
| T9 | 96-A 的 `2×timeout` 窗口 → `timeout+90s` | 无 late-write 路径（源码级） | ~5min |
| T10 | 52-B5 的 20 探针断言 → record-only | 分布未知、逐轮翻 = flake（§2.3-B5） | 一个高 flake 面 |
| T11 | 52-D1（flag 门）+ advisory `--compromised` 单飞臂 | hermetic 已密（`cluster_rotation_test.go:17,111`）；且 N=2 只有一个 retire 靶 | ~2min |
| T12 | 52 的 24h pin 窗口过期语义 | hermetic（`clusterwrite_test.go:10`、`d6_test.go:175`）；纯 `time.Now()` 逻辑、零部署增量；**不许改钟/改 DB**（Mandate ①）。**但 roadmap 的「pin 轮换窗口」落点句由 A6 交付**（窗口在 `cluster status --json` 上真实存在且带 prev+expiry），§7 闸门行须写死这个拆分 | — |
| — | 50-Q4（symlink O_NOFOLLOW） | **保** —— 真 fs + 真软链指向真 `tunnel-key.pem` + 真「key md5 未变」结果 oracle = 有部署层内容，成本 ≈ 2 条命令 | — |
| — | 94-A1（LOST，65–90s） | **保** —— `ps` LOST 是**读时派生**（`exec.go:337-346`，从不落盘）+ inventory 明列 S9☐；`StaleAfter/OfflineAfter` 无 knob（DOC-24）⇒ 不可压缩，只能等 | — |

---

## §6 — OQ 解决（roadmap §7 + 本组新 OQ）

**OQ-7（探索型 drill 的 oracle 纪律）** → **只钉确定性信号；时序一律宽容 `poll_until`；绝不断精确秒数。**

| 臂 | 钉（确定性） | **明令不钉** |
|---|---|---|
| 96-A | 终态行**缺席**(#57) / bucket 对象**在**(#58) / 对照小传输成对 / journal-clean | 传输失败的具体 error 串；watchdog 触发时刻 |
| 96-B | 新 sentinel 回来 **或** `agent unreachable: no heartbeat` 优雅终止；`/connz` 名字重现 | 重连秒数；「零中断」 |
| 96-C | 最终 `dp_curl_ok_body` 精确 sentinel + 同端口 + `.moved==false`；恢复耗时 **record** | 恢复窗口的秒数上界 |
| 96-D | 124-挂起 / 新 leader ∈ {brk2,brk3} / **新 leader 真写成功** / brk1 写拒 / MainPID+NRestarts 不变 / 愈合后 leader `sort -u` 恰 1 / **brk1 读回多数派那一行** | 选举耗时；term 号；raft 内部状态 |
| 96-F | 双 EXITED(-1) / agt2 仍 RUNNING / `kind=reconciled_closed` / 两序皆收敛 | 收敛耗时；两序快慢 |
| 97 | 统计判据 / 每轮非空性 / 每轮末真业务 / journal-clean / 主观测 PID 世代不变 | 任何绝对 RSS 值；goroutine |
| 95-T3 | **claim 硬断（active ∧ Result=success ∧ 真流量 ∧ agent 重连）**；mechanism **record** | 「必须走某一条路」 |

**「时序不确定 ≠ 覆盖缺口」** —— 防 `not_covered` 泛滥的核心区分。**96 的 `not_covered` 用量上限 = 2**（预登记：rehome-event 腿 + 双故障某不可定子面），冒出第 3 条 → **Stage-C 必须质询**，默认视为 cop-out（G-B 的 M12/M14 就是这么抓出来的）。**且 96 的 spine = 分区臂**（`assert.sh:174-176`：spine 不许 `not_covered`）⇒ 原语建不起来 = **SETUP-RED，不是 INCOMPLETE**（SB-FAULT 已 CLOSED ⇒ 此风险已消）。

**OQ-8（并发/分波）** → §8。**新 OQ-9**：waiver 传染（§8.4 + §11-F）。

---

## §7 — Inventory 行消费（无遗漏闸；收工 stamp「**G-C landing**」+「**S1–S9 CLOSURE**」）

| inventory row | 归属 | 落点 |
|---|---|---|
| 155 `cluster backup` `--out/--offline/--db/--data-dir/--secrets-dir/--allow-stale-follower` | **50** | D/E/F1/F2/G1/G2 + C |
| 175 `cluster recovery restore` `--confirm-node-id/--data-dir/--db/--secrets-dir` + Hidden `--yes` | **50**（三断言）/ **51**（**`--raft-addr` → F-a/F-c/F-d**）/ 94（借用） | I1/I2/I3/I4/J1/J2/M1/O；51-E 的 `--secrets-dir` provenance 面 |
| 176 `cluster recovery incident export` `--since/--out/--sid/--force` | **50** | Q1–Q5。**`--since` 补 1 行烟测**；`--sid` → NOT-COVERED-thin（§5.1） |
| 169 `cluster doctor` `--offline/--secrets-dir/--db/--conf` | **50** | R1/R2/**R3(#50)**。`--raft-addr`/`--nats-route` → NOT-COVERED（§5.1） |
| 151 `cluster retire --compromised/--require-credential-rotation` | **双-consumer 拆分**：40 owns 正例 retire 脊；**52 owns C7 修饰面**（镜像 G-B 的 row151/154 先例） | 52-D2/D4-D8 |
| 153 `cluster rotate-tunnel-cert <id> --cert-fp` | **52** | A1-A8。**闸门行须逐字写**：「**pin 轮换窗口 → 52-A6**（`cluster status --json` 上窗口真实存在且带 prev+expiry）；**过期后拒旧 pin 的时钟语义显式裁剪，hermetic owner = `clusterwrite_test.go:10` + `tunnel/d6_test.go:175`**」 |
| 179 `cluster keygen --out`（Hidden debug） | **52** | C1-C3。**订正**：keygen 铸 **node-ident（`U…` user seed）**、**非 account.nk** ⇒ 同步修 roadmap §3-S7-52 措辞与 §4.4 备注 |
| 180 Tier-2 `--yes` rejector（restore 格） | **50** | I1 |
| 181 machine-confirm 双因子 —— **restore 显式除外** | **50** | I2（钉 never-escapable）。**不得写「缺 env 拒」型假阳性负例** |
| 122 `run --cwd/--safe/--ack-alerts` | S1-60 + **S9-96**（`--ack-alerts` → **S9☐ 欠账**） | **96-B0**（本组必须落地，不进 sweep 兜底） |
| 120 `ps --all`（**LOST 合成**，S9☐） | **94** | A1 |
| 127 `history --kind proc/port/transfer` | **94**（proc/port）+ **96**（transfer） | A3/B4/B7；96-A |
| 137 `agent --nats-url`（seed-list 重连的部署语义面） | **95** | T1-T3 |
| 29 event `tetherd_restarted` | **95** | EV 臂（**member core-sub 可读**，非 NOT-COVERED） |
| 30 event `agent_registered` | **94** | **94-A2**（不重启 nats 的窗口 ⇒ 确定性捕获）⇒ **以清单自己的 oracle 交付，不改清单措辞** |
| 54 `home_reassign_failed`（「由 S9-96 的中断臂探」） | **96-C** | **SB-EV2 已由源码关闭：可读**（`rehome_events.go:9-11` → `pubSysEvent` → `SubjSysEvents`；`permissions.go:147`）⇒ 不得判 NOT-COVERED-as-unreadable |
| 横切 §4.5 | G.1(94) / G.5(94) / G.2 含 DELETING 续跑(95) / **#23 判别性(95)** / 中断+分区+双故障(96) / soak(97) | 全部如上 |
| **roadmap §4.3「§7.4 单机备份 = cluster backup 同机制」→ 50** | **闸门行订正（事实错误）** | **拆行**：「§7.4 的 `cluster backup` 面 → 50」+「§7.4 的 **jetstream tar / `nats stream backup` 面** → **NOT-COVERED**（非 tether 动词）」+ **DOC-20** |
| **50 保留 `/etc/tether` 的代价** | **闸门行注** | 「restore 的 seam/nats.conf 启动路径面由 **51 独占**（fresh box 才暴露），50 不计入」 |

**收工闸（7 项）**

1. `go test ./cmd/tether -run TestCommandTreeInventory` → **零 diff**（S 批零产品 CLI 变更；非零 = 有人碰了产品代码）。
2. 事件重枚举：`grep -rn 'pubSysEvent(' internal/broker/` + authcallout `h.emit` + `emitDrainEvent` + `proxy_cluster.go` + `cluster/alert_ops.go` kind 枚举 → 与 inventory §1 diff。
3. **`tests/lint-drills.sh` 的 `BATCH` 硬编码列表 += G-C 七个**（**漏加 = 七个新 drill 完全不受静态假绿闸约束 —— 最容易漏、后果最重的一条**）。
4. `tests/verdict-contract-test.sh` 守恒跑。
5. README drill 表 + 编号族（`5x`/`9x` 已在册）+ per-drill 时长/资源头注 + **§8 的基线新节**。
6. inventory §4 落 **"G-C landing"** 块。
7. 🔴 **`G-C-SWEEP`（收官独占义务，主进程独占，验收 = orphan 行数 == 0）**：按 inventory §3 的生成法重枚举命令树 + 事件面 → 与全表 diff → 产出 **orphan 行清单**（未勾且未登记）→ 逐行裁 {某 drill 认领 / NOT-COVERED + source-cited 理由 / 移交 H 系列修复批} → 落 inventory 的 "G-C landing" + **"S1–S9 CLOSURE"** 块。已识别候选（**逐行由主进程裁，不 self-authorize**）：
   - **row 54/55/56**（rehome kind）—— G-A/G-B 以「无 reader」判 NOT-COVERED，**理由已被 `permissions.go:36/:147` + `rehome_events.go:9-11` 证伪**；结论可能仍对，**理由必须换**。
   - **row 78** `replication_degraded`（G-B §11-U8 裁「S5-30 cite 或 drop S8-90」）—— 无人复查是否落地。
   - **row 123** `expose/expose rm --ack-alerts`（G-B §11-U8 指派 92(a)）—— 无人复查 G-B landing 是否兑现。
   - **row 164** `cluster upgrade --notify-webhook`（G-B 判「93-if-constructible-else-NOT-COVERED-tied-#31」）—— 无人复查落哪边。
   - **broker-ops §7.4 的 JS 快照面** —— 闸门行映射错（DOC-20）。

---

## §8 — run-drills / 拓扑 / flake / 全套 3 连基线

### §8.1 事实基线（实测 + 代码）

- **drill 总数**：现有 **30** + G-C **7** = **37**。族划分：
  - **grow-family（25）** = 既有 19（`10 11 12 13 20 22 30 40 41 42 43 71 73 74 82 90 91 92 93`）+ G-C 6（`50 51 52 95 96 97`）
  - **N=1-family（12）** = 既有 11（`00 21 31 32 60 61 62 70 72 80 81`）+ G-C 1（`94`）
- **`-j N` 无 family 感知**（纯全局节流 + glob 序）⇒ **`-j 6` 不是「族波」**（README:245-254 已记 S1 内审的订正）。
- **VOTER-timeout 故意不在 `FLAKE_SIG`**（`run-drills.sh:60-69` 的 R2-F1 注释）⇒ grow-timing RED **surface + 手动单跑复核**，绝不 auto-swallow。
- **`FLAKE_SIG` 不扩**：96/97 的分区/kill 是**有意**注入，任何 RED 都是证据不是 flake。**明令禁止**把 `blackholed`/`no terminal audit` 加进去（= retry 洗红）。
- **`-j 2` 是 grow 波的经验上限**（`simcluster:223-241` 的 DO-NOT-RE-INVESTIGATE 注 + 150s VOTER 窗）。G-C 不撞 OQ-8 天花板：50/51 各含 2 个 grow 剧目但在自己时间轴上串行 ⇒ 峰值并发 grow ≤ 2。

### §8.2 批内策略

- 单 drill 迭代：`./remote.sh drill <name>`。
- 批收尾两波：波1 = `./run-drills.sh -j 2 50-… 51-… 52-… 95-… 96-… 97-…`（≈50min）；波2 = `./run-drills.sh 94-agent-reconcile`（可与波1 并行，不 grow）。

### §8.3 全套 ~37 drill × 3 次基线

```sh
# 波 A — grow-family 25 个
./run-drills.sh -j 2 --no-retry --logdir /tmp/simdrills/runN/waveA \
  10-… 11-… 12-… 13-… 20-… 22-… 30-… 40-… 41-… 42-… 43-… \
  50-backup-restore 51-full-dr 52-credential-rotation \
  71-… 73-… 74-… 82-… 90-… 91-… 92-… 93-… \
  95-broker-selfheal 96-mid-flight-chaos 97-soak-cycles
# 波 B — N=1-family 12 个（全并发）
./run-drills.sh --no-retry --logdir /tmp/simdrills/runN/waveB \
  00-… 21-… 31-… 32-… 60-… 61-… 62-… 70-… 72-… 80-… 81-… 94-agent-reconcile
```

| 决策 | 理由 |
|---|---|
| 波 A `-j 2` | 25 个 grow-family ⇒ ~13 串行槽 × ~10min ≈ **120–150min** |
| 波 B 全并发 | 12 个 ≈ 45 容器 ≪ ~600；零 grow ⇒ **~15–20min** |
| **基线第 1 轮波 A/波 B 严格串行；第 2/3 轮再测 overlap，两组数字都进 README** | README:239-243 的 CAVEAT 机理**未被定格**（原文只说「timing-sensitive at **peak concurrency**」，**没说**只对并发 grow 敏感）⇒ 往 grow 窗口塞 ~45 个额外 systemd 容器是那句话没排除的风险区。给一个要冻进 README 的基线注入未验证变量 = 坏实验设计。串行 +10min/轮可接受，且顺带免费回答 SB-BASELINE |
| `--no-retry`（基线轮必带） | 基线的目的是**测真实 flake 率**；auto-retry 污染统计 |
| 不给任何 `--allow-*` waiver | §8.4 |
| 3 连 = 3 次完整两波 | ≈ **7.5h**。`run_in_background` 串跑，`--logdir /tmp/simdrills/run{1,2,3}/` 隔离防覆盖 |
| **97 的基线轮固定 `SOAK_CYCLES=6`** | 恰好满足 N≥6 斜率门；README 必须注明「基线数字对应 `SOAK_CYCLES=6`」 |

### §8.4 waiver 传染 + expected-verdict manifest（新 OQ-9）

**承重事实**：`run-drills.sh:36-37` 的 `--allow-product-red` / `--allow-incomplete` 是 **套件全局开关，不是 per-drill**。G-C 落地后全套 37 个里 ~10 个永久或双稳非绿（既有 31=PRODUCT-RED #28、74=RED #34、40/41/90/91 的 INCOMPLETE/双稳 + G-C 的 50/52/96 恒 RED、51 双稳、97）。要让基线跑出 exit 0 必须同时开两个全局 waiver —— **那一刻，40 的真实覆盖缺口、90 的 not_covered、以及任何新出现的产品红，全部被同一个开关静音**。退出码从「有没有 blocker」退化成「有没有 ASSERT-FAIL / SETUP-RED」。

**定稿（零代码，README 一节 —— 不动 runner 一行，动 runner = 本批范围外）**：

> **基线的判据不是退出码，是 per-drill 的 verdict 对照。** plan 交付一张 **expected-verdict manifest**（每 drill 一个**允许集合**）：
> `00 = {GREEN}` · `31 = {PRODUCT-RED}` · `74 = {RED}` · `40/41/90/91 = {…既有…}` · **`50 = {PRODUCT-RED}`**（T2 砍掉 re-grow 后**稳定单值**）· `51 ∈ {GREEN, PRODUCT-RED}` · `52 ∈ {GREEN, PRODUCT-RED}` · `94 ∈ {GREEN, PRODUCT-RED}` · `95 ∈ {GREEN, INCOMPLETE}` · `96 = {PRODUCT-RED}` · `97 ∈ {GREEN, PRODUCT-RED}`
> README 基线节记录 3 轮实测 × 期望集合的 diff。**新 blocker = 落在集合外**；**基线成立 = 同一 drill 跨 3 轮落同一 verdict ∧ 每个 verdict ∈ 其允许集合**。

**这同时解决 roadmap §3-S9 的出口措辞问题**（§11-K-1）：roadmap 逐字「全套并发 **3 连绿**（已知 RED 除外）」—— 但 31/74/96/50 结构上恒非-GREEN ⇒ **全套永远不会 ALL GREEN，也不该**。一个不可能满足的出口，在 Stage-B 会精确地转化为「把 RED 调成 GREEN」的压力。

### §8.5 README 基线节必写字段

`37 drills = 25 grow-family + 12 N=1-family` · 两波命令（串行版 + overlap 版）· 每轮 wall-clock（3 次 min/median/max）· 每 drill 的 verdict × 3 · **expected-verdict manifest 的 diff** · flake 率 × 签名 · 宿主规格（88c/251G/inotify 8192）· **97 的 `SOAK_CYCLES=6`** · **已知非-GREEN disposition 表**（它们是**预期的诚实 RED，不是基线绿**）。

---

## §9 — Per-drill false-green 风险头注（每 drill 一块；必须活到落地的守卫）

**50**
1. **daemon 未停 ⇒ 所有 restore 负例都回 `daemon still running`** ⇒ 松签名全过。守卫：每条负例锚**专属**串。
2. **门 9 掩盖门 10** ⇒ foreign 臂钉成 confirm-mismatch。守卫：I4 传 **bundle 的 self_id**。（本 drill 唯一致命设计点。）
3. **Z 在备份之前建** ⇒ 同一性对照失效。守卫：B4 在 D 之后 + Z 存在基线。
4. **Z 阴性单读** ⇒ `session ls` 空/报错也「Z 不在」。守卫：L1/L2 **同一次 JSON 求值**。
5. **history 阴性恒真**（L3 单条 `! grep`）。守卫：rc==0 + 输出非空 + L1 阳性对照。
6. **数据面用 status 收口 = #20 复刻。** 守卫：L4 收在 `dp_curl_ok_body` 原 sentinel；H1 收在 `dp_curl_refused`(**7**)。
7. **「永远拒」不是门存在的证据。** 守卫：J2 = I2 的对照源；Q3 = Q2 的对照源；**R2 = R3(#50) 的对照源**（**定稿要求：R3 的注释里必须显式引用 R2**，否则内审会正确地判「永远不红 ≠ 门存在」）。
8. **`exit 1` 无判别力**（`healthExitCode` default 也回 1）。守卫：K 同断 `NOT-HA` + `voters==1` + 无 force_single。
9. **#50 用 `assert_refuses` ⇒ 落 ASSERT-FAIL = verdict 误分类。** 守卫：R-INVERTED。
10. **`.pre-restore.bak` 的 md5 == MD5_0 会假红**（`backupToUnique` → `checkpointWALForBackup` 以读写 DSN `wal_checkpoint(TRUNCATE)` **在拷贝前就改了字节**）⇒ 用 sentinel 行，**不用 md5**。（M1 的 `md5(tether.db)==MD5_0` **安全**：拒绝路径永不到 `:175`。）
11. **J1→J2 之后 `.bak` 名会变**：签名必须 `\.pre-restore(\.[0-9]+)?\.bak` 并**从 stdout 回读实际路径**，不得预设。
12. **broker 应用串不在 journal**（R-BROKERLOG）。

**51**
1. **fresh-box 门是整个 DR 的真伪开关** —— D 的四条 `!` 断言任一缺失，后面全部「恢复」都可能是「箱子根本没坏」。
2. **C-vault-oracle 必须是 sha256 比对**，不是 `test -e`（残留/半写 bundle 会让「灾后仍可读」因错误原因绿；`vault_init` 的 rm-then-assert-empty 是它的孪生守卫）。
3. **E-remint-neg 是 D 的合法性证明** —— 没有它，「`secrets_distribute` 是恢复不是捷径」只是口号；且必须带零变更 oracle。
4. **H2 绝不以 status/explain 收口**；`dp_curl_refused` 用 exit-7 而非 `! curl -sf`。
5. **H1 严禁 restart/re-provision agent**；起 broker 前先证 `! ONLINE`（心跳残留窗 ≈5s 会假绿）。
6. **#51/#52 必须在任何 `[GAP]` 清理之前钉为头等 `assert_bug`**（照抄 40 对 #31 的纪律）；wrong-reason → ASSERT-FAIL，绝不放宽签名。
7. **`--on-broker brk1` + `transfer-leader brk1` 是硬前提，不是调优** —— 缺了 A-base 的 curl 会因 #29 失败 = false-RED 误伤 DR。
8. **`pruned 2 stale peers` 恒为 2** ⇒ 判别 F-b/F-c/F-d 用 **`.bak` 阶梯**，不是 prune 数。
9. **臂 I 不得用 `grow_to_3` 的 retry 洗掉 #31 证据。**
10. **臂 D 的 fresh box 只有走 `$SIM up`（跑 `provision-node.sh:39-45` 改写 `host: 0.0.0.0`）才成立** —— 手工装机会因**环境**假红。
11. **DR-STEP-LEDGER 是一等产出**，禁止 `dr_restore_all()` 一把梭。

**52**
1. **A7 不重启 agent 就 curl ⇒ 绿的是老连接**（`rotate-tunnel-cert` 只 hot-swap 服务端 cert）—— **但也不许 `systemctl restart tether-agent`**（Mandate ②）：用分区件触发 redial。
2. **A8 只断「unit failed」⇒ 任何崩溃都吃成绿。** 守卫：断 **broker.err** 里 `matches neither the pinned` 精确串。
3. **B2 只断 md5 不变 ⇒ reconciler 恰好没跑也会绿。** 守卫：**B0 的 reconciler-liveness 正例** + 三件齐。
4. **B5d 缺对照源 ⇒「经 brk1 连不上」可能是 brk1 网络挂了。** 守卫：同身份经 brk2 成功 + 恢复腿 + 成对采样。
5. **B7 缺 brk2-nats-active 守卫 ⇒「mesh 不形成」可能只是 brk2 死了。**
6. **SETUP 缺 JS-meta-formed 前证 ⇒ B7 的 `cluster_size` 掉 1 可能是它从来没到过 2。**
7. **D6 用 `resp.OK`/stdout 自陈收口 ⇒ 那是 hermetic 已密的请求形状。** 守卫：ctl 经真 NATS `alert ls --json`。
8. **D-spine 洁净基线缺失 ⇒ setup 瞬态预拉的 `manual:*` 会让「retire 后有告警」因错误原因绿。**
9. **inverted 缺陷用 `assert_bug` 包 exit-0 命令 ⇒ APPEARS FIXED → ASSERT-FAIL。**
10. **`secrets_push_file` 漏 chmod 0600 ⇒ `SecretsPreflight` 硬拒 ⇒ 把 #54 的证据洗成假 SETUP-RED。**
11. **`secrets_remint_route_only` 若碰 tunnel leaf ⇒ B4 的重启在两个 broker 上同时造砖** ⇒ #54 的证据被淹没在 harness 自造的红里。
12. **A8 的 trap 必须带 `reset-failed`**（`StartLimitBurst=5/10s` 未关）⇒ 否则 B1 baseline 崩、根因错指 A8。
13. **FG 守卫 3 必须是 `assert_setup` 级**（A8 的砖化残留会伪装成 B 组的产品结论）。

**94**
1. **`exec` 造不出 orphan**（`agent.go:1133-1136` + `run.go:229`）—— orphan 臂**必须** `run`/PTY；不写这条，未来「简化」成 exec 就是静默的结构性假绿。
2. **`node ls` 回 ONLINE 绝不是重注册证据**（旧 bundle 含 agt1 的 node 行 ⇒ heartbeat 直写 livenessDB）。
3. **broker 重启 ≠ agent 重注册**（③b 把它断成正面不变量，同时证明步④不可省）。
4. **三个独立超时**（B1 60s / B2 60s / B3 30s）—— 合并成一个就分不清「broker 没恢复 / agent 没注册 / directive 没执行」。
5. **`killed_orphan` 的 no-RC 双证**（正 `rc=<nil>` 命中；反 `rc=<数字>` 无命中）。
6. **不以进程消失为终点**（B3 之外必须有 B4 审计 + B5 日志）。
7. **对照源必须 poll 且成对**（B7：X 先 poll 到活 → 断 Y 断 → **同窗口再断 X 仍活**）。
8. **cursor 门**（`--after-cursor`）—— 不加会 grep 到早先重连的残行。
9. **backup→restore 窗口 <300s 前证**（防 15min port-revoke 计时污染）。
10. **restore 必须 `-u tether`**（`[operator per broker-ops #6]`）—— root 路径的坑是 50-J1 的暴露对象，94 不越界也不代劳。
11. **`sh -c` 不裹 harness 函数**（R-NOSHC）。

**95**
1. **T1 绝不用 `systemctl restart/stop`** —— systemd 知道是自己发起的 ⇒ `Restart=` 不适用 ⇒ 判别性归零。
2. **T1/T2 的 journal 措辞互为判别子**（`Deactivated successfully` ∧ ¬`Main process exited, code=` vs `code=killed, status=9`）—— 只看 NRestarts 增就分不清 clean/unclean，#23 的整个论点消失。
3. **`ActiveState==active` 绝不是终点**（`Type=simple`）—— 收在 `broker: ready` 行数增（**broker.err**）+ 真流量/真 exec。
4. **T3 是 hard-claim + recorded-mechanism**，不是 accept-both；`inactive/failed` ⇒ ASSERT-FAIL（G1 修复回归）。
5. **T2 的 agent anti-oracle 必须同窗口配正对照**（`node ls` agt1 仍 ONLINE），否则 T3 不跑时留 vacuous PASS。
6. **absence 断言 fail-closed**（先证 rc=0 且非空）。
7. **StartLimit 守卫**（`Result != start-limit-hit`）+ 臂间隔 ≥15s；RECIPE-B 每轮 `reset-failed`。
8. **95-D 的中间态硬前证**（`DELETING` ∧ 流在 ∧ broker.err 出 finalize-failed）—— 没有它，「重启后 session 没了」是 vacuous。
9. **95-D 的三点齐**（流没 ∧ 行没 ∧ **新 `session_destroyed`**）—— 只断行没会被 phase③ 的任何路径满足。
10. **broker 的 slog 不在 journal**（R-BROKERLOG）。

**96**
1. **传输没开始 / 走了 tier-A ⇒「无终态」因错误原因成立。** 守卫：三重基线 + `start` 行的 `tier==b` + bucket 对象已在的双门。
2. **history 流本身坏了 ⇒ vacuous。** 守卫：前置成对 start/complete + 注入后小 tier-A 传输的对照源。
3. **INVERTED 的 `else` 兜底会种一条捏造的 gotcha。** 守卫：R-EXHAUST 四态。
4. **杀错 broker 是自纠正的**（拿到 `complete` → APPEARS-FIXED），但注入前用 `/connz` 确认 home 身份省一轮。
5. **规则没生效 ⇒「一切正常」全绿。** 守卫：`fault_assert_blackholed` **双端口自证**（124），失败即 `setup_fail`。
6. **注入的是 REJECT 不是 DROP ⇒ 语义是「宕机」不是「分区」**（raft 会立刻知道 ⇒ 时序全错但仍可能全绿）。守卫：124 ≠ 1，可测判别子。
7. **切太狠（全端口）⇒ D3「只读存活」vacuous。** 守卫：`fault_assert_reachable ctl1 brk1 4222`。
8. **D1 读的是 brk1 自己的陈旧视图。** 守卫：D1 必须从 brk2/brk3 读；D2 的**真写成功**是多数派活着的唯一合法证据。
9. **D6 用状态字段收口 ⇒ 视图收敛而数据分叉。** 守卫：从前少数派 brk1 **读回多数派写的那一行**。
10. **F 的 `pgrep -x sleep == 0` 是注入自保证的恒真式** —— 已删（`node_kill` 销毁被观测的 OS 状态本身）。
11. **armed 窗口内一律 `dp_curl_blackholed`(28)，禁 `dp_curl_refused`(7)**。
12. **R-BOUNDED-PROBE**：armed 窗口内每个探针必须带 `timeout`（runner 无 per-drill timeout ⇒ 挂起 = suite 永挂 + 规则永不撤）。

**97**
1. **victim 轮转必须排除主观测进程**（brk1/agt2）—— 否则 fd 每轮归零、泄漏 oracle 结构性无效。
2. **PID 世代守卫**：主观测 PID 变了 ⇒ 序列重置 ⇒ `not_covered`，**不得**用重置后的序列判绿。
3. **非空性守卫**：每轮断注入真生效 + 恢复真发生 + 轮末一次真业务；任一失败 ⇒ **ASSERT-FAIL**（不是泄漏判据的 not_covered）。
4. **t0 在 warmup 后**（懒初始化让早期计数虚低 ⇒ 假 RED）。
5. **RSS 绝不用绝对高水位**（Go GC 锯齿 ⇒ 必假 RED）。
6. **`Threads` 不是 goroutine 的代理**（10k goroutine 泄漏可显示零 Threads 增长）—— 拿它当代理本身就是假绿 oracle。
7. **崩溃/完整性 oracle 与泄漏 oracle 正交，绝不互相顶替**；broker 侧读 broker.err（R-BROKERLOG）。
8. **一轮失败污染 landing = 设计意图**，禁「只报最后一轮」/「per-cycle 独立 verdict」。

---

## §10 — Stage-B spikes（全在 `weilandserver` 经 `remote.sh`；pin 前跑；每条两出口）

### §10.1 已 CLOSED（源码 or 主进程实测；省一轮）

| id | 结论 |
|---|---|
| **SB-FAULT** | **CLOSED（主进程实测 2026-07-17，§0.3/§12）**：`iptables v1.8.10 (nf_tables)` 在 `--privileged` 容器内可用；SIMFAULT chain + 双向 dport/sport DROP → 124 挂起 ∧ 非过滤端口 rc=0 ∧ flush 后立即恢复 ⇒ **P1 采纳** |
| **SB-VAULT** | **CLOSED（主进程实测）**：`d cp` 容器→host 落 `weiland:weiland` 0644、免 sudo 可读；host→容器落 uid 1000（**非 root**）⇒ `vault_push` 须 chown。**bind-mount 不需要** |
| **SB-97-1** | **CLOSED（主进程实测）**：镜像有 `/var/log/journal`、无 `/run/log/journal`；`node_kill`/`node_start` 保留容器写层 ⇒ journal 跨 boot 存活 ⇒ 出口 (a) |
| **SB-96-3** | **CLOSED（源码，出口 a）**：`SubjNodeHeartbeat` publisher=agent、subscriber=ctl、无 broker 中转；`runWatchdogTimeout()` 默认 15s（`run.go:381`，`TETHER_RUN_LIVENESS_TIMEOUT` 覆盖）⇒ 96-B 主设计 = 出口 (b) 优雅终止 + knob 成对臂 |
| **SB-52-D3** | **CLOSED（源码，出口 a）**：`cluster_retire.go:138-142` 无 `registerYesRejector` ⇒ `assert_refuses "unknown flag: --yes"` GREEN，并入 **#36** |
| **SB-EV2** | **CLOSED：YES**。`rehome_events.go:9-11` 逐字「All emits go through b.pubSysEvent … onto proto.SubjSysEvents」⇒ rehome kind 全部 member 可读 ⇒ inventory row 54 不得判 unreadable |
| **SB-50-7** | **CLOSED**：`/var/backups` 不存在 ⇒ `MkdirAll` 先跑 ⇒ 撞 `prepare bundle parent` + `CodeStoreError`，**不是**「误导括号」⇒ 降 **DOC-27** |
| **SB-51-SEAMSIG** | **半 CLOSED**：串 = `cutover.go:117-120` 的 `broker.cluster.data_dir is unset (refusing to silently downgrade a cluster DB to single mode)`；**但它不落 journald** → 读 `broker.err`。剩余未知 = cobra `Error:` 前缀是否包裹（低风险） |

### §10.2 待跑

| id | 问题 | 出口 (a) | 出口 (b) |
|---|---|---|---|
| **SB-94-RESTORE**（BLOCKING，94 的脊） | `-u tether` 的 backup+restore 是否过 provenance 门并让 broker 重起？ | B 组全落地 | 分诊：(i) 权限/属主 ⇒ 归 **#6**（50-J1 的地盘），**绝不 chown 环境规避**（Mandate ①）；(ii) fp 不匹配 ⇒ 查 secrets 是否被轮换过；(iii) 结构不可行 ⇒ orphan 臂 `not_covered` + 保 A 组/B7 |
| **SB-94-RECONNECT** | `restart nats-server` 后 60s 内出 `agent: re-registered after reconnect`？被测 `sleep 9199` 存活整窗？ | §3.1 全绿路径 | 换 DROP 分区 + 实测等 PingInterval（~4-6min）；仍不行 ⇒ orphan 臂 `not_covered` |
| **SB-94-PSJSON** | `ps --json` 的实际 schema 字段名 | jq oracle 直用 | 以实测字段名订正（**绝不**回退到列位置 grep） |
| **SB-95-DELETING**（承重） | `stop brk2 nats-server` 60s 窗内：brk2 的 tether-broker `NRestarts` 增否/`MainPID` 稳否？brk1 `leader_id` 稳否？brk1 `nats stream ls` 503/超时否？ | broker 存活 ∧ raft 稳 ∧ JS 死 ⇒ **RECIPE-A**（确定性 GREEN） | brk2 crash-loop ⇒ **RECIPE-B**（K≤4 + 硬前证）；全败 ⇒ **RECIPE-C** |
| **SB-95-SIGTERM** | `kill -TERM $MainPID` 后 journal 真出 `Deactivated successfully` 且**不出** `Main process exited, code=`？`NRestarts` +1？ | T1 判别性成立（源码强支持） | 否 ⇒ `serve.go:241` 的 clean-exit 路径未如预期 ⇒ **新 gotcha 候选**（`Restart=always` 的正当性前提被推翻）；T1 改 `assert_bug`；**且 §11-H 的 T0-ctrl 重新纳入作分诊工具** |
| **SB-95-STARTLIMIT** | T1+T2+T3+RECIPE 连续 restart 是否撞 `StartLimitBurst=5/10s`？ | 直接跑 | 臂间插功能收敛 poll + 断 `Result==success`；仍撞 ⇒ 显式 `reset-failed` 并 labeled `[env: systemd StartLimit，非 tether 面]` |
| **SB-96-1** | cluster 模式下 `dexec <brk> -- curl -s 127.0.0.1:8223/connz` 可用且含 `name`？（`cluster_natsconf.go:326` `MonitorListen: "127.0.0.1:8223"` 由 reconcile cutover 建；`install.sh:549-551` 明写 fresh single-mode 安装没有它） | B/D 臂的钉路径权威源 | 降级 `/proc/net/tcp` established 计数（弱一档：只证有客户端连着、不证是哪个 ctl）+ 该降级登 DOC-26 族 |
| **SB-96-2** | A2 的 boot-reap 是否真跳过 | 对象仍在 ⇒ `product_red #58` | 消失 ⇒ 用 `transfer_reconcile.go:90` `"orphan xfer objects reaped"` 判别；非 boot-reap 所为 ⇒ `not_covered` + 根因未定 |
| **SB-96-B-DISCRIM** | `broker.go:985-1005` 的 voters≥2 分支是否 **return err**（决定 #59 是否可能） | 是 ⇒ #59 可复现 | 否（注释 :976-978 明说 never a hard assertion）⇒ #59 降为纯观测 |
| **SB-50-1** | in-sim 的 `history-lab` 有 `kind=="call"` audit 记录吗？incident JSON 的 `.audit[…]` **path 形状**？ | `actor_nkey == "[redacted]"` 硬断 | 无 call 记录 ⇒ `not_covered "no denylisted-key audit record reachable via the product path in-sim"`；path 未知 ⇒ 先 `jq -e 'paths(scalars)\|join(".")' \| head -50` 打印真实 path 集再定 oracle |
| **SB-50-2** | root-restore 后 broker 起不来的**真 broker.err 串** | 预期 `unable to open database file`/`permission denied` ⇒ signature-guarded `product_red "#6"` | broker 竟能起 ⇒ 记 GREEN 观测；**DOC-18 仍成立**（文本事实） |
| **SB-50-3** | 确定性注入（`raft/` 父目录不可写 / ENOSPC）能否让 `BootstrapSingleNode` 失败并留 marker？ | O 臂 GREEN | 不能 ⇒ 退 kill-9 `[optional, K=2 best-effort]`；仍不中 ⇒ `not_covered`（**禁**伪造 marker） |
| **SB-50-4** | brk2 不停、按 runbook 字面 restore 是否稳定 | 保留 runbook-字面 + 记 brk2 事后态 | 分叉/抖动 ⇒ 停 brk2 + 登 **DOC-19** |
| **SB-51-NATSSIG**（承重） | restored cluster-mode broker 撞 stock nats.conf 的**精确 broker.err 串** | 抓到 nkey/auth 串 ⇒ #52 钉死 | 竟能服务 ⇒ **撤 #52**、G2 改 `assert_ok`、降 DOC-19 |
| **SB-51-AGENTRECON** | agt1 在 brk1 容器被删→重建（docker DNS 条目消失再出现）期间是否自愈 | H1 GREEN | 不自愈 ⇒ **分诊**：agent 侧真缺口（gotcha）还是 docker DNS 缓存（env）—— **绝不用 restart agent 洗绿**（#48 禁令继承） |
| **SB-51-HIST** | 灾后 `history` 真实行为 | 空/无流 ⇒ #53 + DOC-19 | 有行 ⇒ 撤销 |
| **SB-51-UP1** | `up --brokers 1` 后再 `up --brokers 3` 是否幂等且不碰已存在的 agt1/ctl1 | 是（`_bring_up_node` 的 `node_exists` 分支，`simcluster:55-71`） | 否 ⇒ 加 `provision-node <role> <node>` 薄 verb |
| **SB-52-B5** | nats-server 对跨 route queue group 是否本地优先（**nats-server 行为，tether 源码给不出答案**） | 本地优先成立 ⇒ B5 record 全成功、**#55 降级为有界窗口缺口** | 不成立 ⇒ B5 record 出现失败样本。**Stage-C B5 实测订正**：#54 使 issuer 换不到 NEW，B5d 的 skew 前提在运行态无法构造 ⇒ #55 随 B4-B7 一并 NOT-COVERED（源码级：`cluster_reconcile.go:78` + `serve.go:203-218`），非"无论如何都跑" |
| **SB-52-B7** | brk1 nats journal 对旧-CA route leaf 的精确拒绝串 | 命中 `unknown authority` ⇒ 定格 | 无可靠串 ⇒ **降级为纯语义 oracle**（`cluster_size` 2→1→2 + brk2 active + 客户端口应答 + 恢复腿），**不记 `not_covered`** |
| **SB-52-A8** | 砖化态下 `rotate-tunnel-cert` 的确切失败串 | 定格 ⇒ DOC-23 带 signature | socket 仍在 ⇒ **DOC-23 作废**，A8 只留 fail-closed GREEN |
| **SB-52-FP** | `secrets_tunnel_fp` 与 `tunnel.CertFingerprint` 等同 | A3 回读 tether 算的 on-disk fp 比对相等 ⇒ 继续 | 不等 ⇒ **harness bug，随批修** |
| **SB-97-2** | 稳态 fd/RSS 的真实抖动幅度（校准 K_fd/ε_fd/ε_rss） | 阈值确认 ⇒ 收紧并把实测数据写进头注 | 超阈 ⇒ **先分诊是抖动还是泄漏**，再调 |
| **SB-SOAK** | `remote.sh` 是否透传 `SOAK_CYCLES` env | env 参数化 | `$HERE/.soak-cycles` 文件 |
| **SB-BASELINE** | 波 A(`-j2` grow) 与波 B(N=1 全并发) 是否互不干扰 | 第 2/3 轮 overlap 成立，~2.5h/轮 | 波 B 挤掉波 A 的 VOTER 窗 ⇒ 三轮全串行，~3h/轮 |

### §10.3 引用漂移订正表（定稿前全文已替换）

| 处 | 草稿写的 | 实际 |
|---|---|---|
| `permissions.go:86`（三处引作「member Sub-allow sys.events」的决定性证据） | `:86` | **`:147`**（`PermissionsForActivatedMember` Sub 块 :135-165）+ `:36`（unactivated）。`:86` 实为 Pub allow。**结论对、引用错 —— 不修会被内审连结论一起打掉** |
| roadmap 的 `broker.go:1092-1096`（禁 raft 外删行） | 已漂移 | **`broker.go:1130-1136`**（语义是 broker 自身 retention-GC 自禁 + 明写 replicated-state fork 机理；结论不变） |
| `broker.go:948-958`（EJECTED trap） | 现为 JS-probe else 分支 Debug 日志 | **`broker.go:956-985`** |
| `transfer.go:96-104` / `:818-820` | — | struct **`:99-102`**，`newTransferTracker` **`:104`**；`preview==nil` 在 **`:816-819`** |
| 「>1 MiB max_payload ⇒ tier-B」 | — | **`transferTierAMaxBytes = 8 MiB`（`transfer.go:52`）**（结论安全：12 MiB > 8 MiB） |
| 「`source=self`」 | 不存在 | **`source=leader`**（`sourceRole ∈ {leader,follower}`） |
| 「F-c: `pruned 0 stale peers`」 | — | **恒 `pruned 2`**（每次在新拷贝 staging 上 prune） |
| runbook `:522`/`:531`/`:544-546` | — | **`:523`/`:533`/`:550-551`** |
| `install.sh:544-553` / `:545` | — | seam 注释块 **`:548-556`**；`data_dir` 样例行 **`:553`** |
| `install.sh:485` | — | **`:491`** `install -d -o tether -g tether -m 0750 "$LIB_DIR" "$LOG_DIR"` |
| `incident.go:26-34` / `cluster_offline.go:514-536` | — | **`incident.go:32-35`**（`auditScrubSubstrings`）/ **`:524-545`** |
| `cutover.go:97-113` | 函数头 | **`:117-120`** |
| `clusterwrite.go:243`/`:245-248`/`:172-187` | — | **`:245`** / **`:247-250`** / 函数 **`:173-190`**、串 **`:183-186`** |
| `serve.go:245` / `install.sh:757-758` | — | **`:247`** / **`:756-757`** |

---

## §11 — 主进程定稿裁决（开放项 A–L）

> 每项：问题 / 立场 / **裁决 + 反证切换条件**。凡「Stage-B spike」者，立场 = **预期默认 + 反证切换条件**（探索→定格，先真跑、不静态跳过）。

### **A — gotcha 全局分配表 + #6 复用** — 裁决：**采纳 §4 全表**
6 份草稿各自把头号发现编成 `#50`，DOC-17 被 5 份占给 5 件事。裁定：**#50=doctor `--db` 失明（已实证）· #51=restore 不 apply seam · #52=restore 不渲 nats.conf · #53=bundle 无 JS · #54=account/CA 轮换无 re-render · #55=auth_callout 轮换窗口 · #56=rotate 循环建议 · #57=悬空 transfer audit · #58=orphan xfer object · #59=少数派 broker crash-loop · #60=soak 泄漏 · #61=G.5 审计缺失 · #62=双故障不收敛**。**root-restore 复用既有 #6，不开新号**（`docs/broker-ops.md:621-626` 逐字列了 `restore`；反双编号纪律，s6-s8 M11）。号在 §4 表 ratify、ledger 小节 Stage-B 后补、drill 内字串零漂移。
**反证切换**：若 Stage-B 证明 root-restore 有 #6 未覆盖的新 facet（如 `.pre-restore.bak` 本身被建成 root-owned 导致二次 restore 也挂）→ 才开新号。

### **B — `assert_bug_refusal` 新原语** — 裁决：**驳回**
理由：① 该形态有 review-blessed 的既有惯用法，仓库内 **11 处在用且零漂移**；② `product_red()` 本就独立可调用、不绑命令形态；③ 真值表是**外审三轮定的合同**，在「只交付 drill/harness」的批里动它 = 重新开庭，而 G-B 的教训正是「计划外的手术引爆连锁」。**不设 `drills/lib/inverted.sh` 折中体**——11 处内联零漂移已证明不需要抽象。
**反证切换**：若 Stage-C 内审证明内联形态在 G-C 的 7 个 drill 里真的漂移了 → 才抽 `drills/lib/inverted.sh`（**仍不动 `lib/assert.sh`**）。

### **C — S0-备份库形态** — 裁决：**`d cp` + rm-then-mkdir-0700-assert-empty**
**由主进程实测背书**（SB-VAULT，§0.3）：`d cp` 容器→host 落 weiland-owned 0644、免 sudo 可读可 sha256 ⇒ bind-mount 的 uid 映射面根本不必引入。fail-on-exists 会被 `SIM_KEEP=1` / 宿主 OOM / ssh 断**永久毒化**（`cmd_drill` 的 trap 不捕 EXIT）⇒ 用 rm-then-assert-empty。
**反证切换**：无（已实测）。

### **D — S0-故障原语：P1(烘 `iptables`) vs P3(纯 tc)** — 裁决：**P1**，且**订正综合稿对 P3 的错误论断**
**SB-FAULT 已由主进程实测 CLOSED**（§0.3）：`iptables-nft` 在 `--privileged` 容器内可用，双向 DROP → 124 挂起 + 非过滤端口 rc=0 + flush 恢复。Mandate ③ 论证成立（给机器装标准内核工具 = 供给；注入的是**故障**不是 workaround；分区比 `docker network disconnect` 对 tether **更难**，正是 Mandate ④ 的正向自检）。
**订正（重要，防止把假事实写进仓库）**：综合稿称「P3 一旦采纳，96-D 的选择性对照必须 `not_covered`，因为 tc egress-only is device-wide: cannot cut route/raft while leaving the client port」——**这是错的**。主进程实测：`tc qdisc prio + netem loss 100% + u32 filter (match ip dst <IP>/32 match ip dport <PORT>)` **能**精确只丢某 peer 的某端口，同 peer 的 4222 仍 rc=0、ICMP 仍通。故 **P3 是一个语义等价的可用 fallback**（差别只是 egress-only ⇒ 双向需两条 filter），**不需要**降级 D3/D4 的措辞。采 P1 的理由是**表达力与可读性**（`--dport`+`--sport`+INPUT/OUTPUT 四行说清双向），不是 P3 做不到。
**反证切换**：若烘 `iptables` 后镜像出现非预期副作用（如与 systemd 的 nftables 单元冲突）→ 退 P3（配方见 §12，已实测）。

### **E — 97 的 goroutine 口：drill 内 `not_covered()` vs 批级台账** — 裁决：**批级登记（iii）**
理由：① **roadmap 自己把它框成批级**（§3-S9 逐字：「goroutine 数无产品级观测口…，**显式 NOT-COVERED**（将来产品加 metrics 再收）」），97 的 spine 是「泄漏 oracle：fd + RSS」；② plan §5.1 的 NOT-COVERED 台账**就是** first-class 登记（gap 被计数在 plan 里、不是被藏起来），(iii) ≠ 「藏进文档」；③ **waiver 传染是可量化的真实成本**：为一条已知永久缺口开全局 `--allow-incomplete` = 拿一个 drill 的诚实换整个套件的诚实（`run-drills.sh:36-37` 是套件级开关，实读）。97 的 landing 因此是 `{GREEN, PRODUCT-RED}`。**同理 P8 24h 差距也是批级登记，不是 drill 内 gap。**
**反证切换**：若外审判定「drill 内不调 `not_covered()` 就等于藏」→ 走 (i)，但**必须同时采纳 manifest（§11-F）**，否则基线的退出码从第一天起就是噪声。

### **F — expected-verdict manifest 取代「3 连绿」/退出码** — 裁决：**采纳（§8.4）**
零代码、README 一节，**不动 runner**（动 runner 是本批范围外的 harness 叶子增量）。**基线成立 = 同一 drill 跨 3 轮同 verdict ∧ 每个 verdict ∈ 其允许集合。**
**反证切换**：若 owner 愿把「per-drill waiver」列为独立 harness 增量 → manifest 作为它的输入规格。

### **G — 97 首版泄漏判据** — 裁决：**(b) UNCALIBRATED 放宽 4×**
`K_fd=64 / ε_fd=2 fd/cycle / ε_rss=4 MiB/cycle`，头注标 `[UNCALIBRATED — widened 4x pending SB-97-2]`。理由：ε 是 **harness 的参数、不是产品的承诺** ⇒ 校准属 §0.2 允许的「harness bug 随批修」同族；(b) 让 97 从第一跑就有一个真断言（宁可漏报也不制造每轮红一次的假信号），且基线 3 连自然提供收紧依据。
**反证切换**：若基线 3 轮实测抖动**超过** 4× 阈 → 转 (a)：本批把 97 降 record-only 并在 §5 登记「泄漏判据待校准」，下一批翻断言。**任一情况下：出现 RED 先分诊是抖动还是泄漏，绝不因一次 RED 就调阈。**

### **H — 95-T0-ctrl（scratch systemd 语义对照）** — 裁决：**砍**
**深度闸门问的是「部署层新增了什么关于 tether 的信息」—— scratch unit 里一行 tether 都没有 ⇒ 答案是零**，这是决定性的。T1 本身（真 unit + `kill -TERM $MainPID` → `Deactivated successfully` ∧ ¬`code=` ∧ `NRestarts+1` ∧ 新 MainPID ∧ **真流量恢复**）**已在真单元上直接证明了 always+exit0→revived**；`on-failure` 不救是一个**关于 tether 不发布的配置的反事实**。次生收益：95 的 restart 次数 4→3，StartLimit 风险减半。
**反证切换**：若 **SB-95-SIGTERM** 实测出现**非预期退出语义**（如 `code=exited, status=N≠0`）→ `Restart=always` 的正当性前提被推翻 → 那时 T0-ctrl 变成必要的分诊工具，重新纳入。

### **I — 52-A7 的 re-pin 触发法** — 裁决：**两段式（自发 poll → 分区触发 redial → 否则 gotcha）**
**驳回 `systemctl restart tether-agent`**：`rotate-tunnel-cert` 只 hot-swap 服务端 cert，既有连接不受影响 ⇒ **「什么时候重拨」正是被测语义**；由 sim 代为重启 = 认证「re-pin 工作」而隐藏「只有把全车队重启一遍才工作」（Mandate ②）。分区是**注入**（真实网络会发生）；重启 unit 是**运维动作**（真实运维不会为了换证挨个重启）。分区件本组已落地 ⇒ 零额外成本。
**反证切换**：若 **Stage-B 先读 `docs/cluster-runbook.md §2.1` 的 agent 侧步骤**发现文档明写 rolling-restart agents → restart agent 变成 `[operator per runbook §2.1]`，A7 恢复原设计。

### **J — 50 的注入法 + 面归属登记** — 裁决：**保留 in-place，但换论证 + 显式登记代价**
原论证（「`rm_node --vols` 会让 provision sentinel 随 etc 卷亡 ⇒ 保留-secrets 变体必 SETUP-RED」）是**稻草人** —— 它针对的是「保留 etc 卷」的变体，而 roadmap 没要求那个变体。真正的分工线是 **50 = lib-卷灾难（保留 `/etc/tether`）/ 51 = 整机灾难（fresh box）**；且**必须显式登记代价**：50 保留 `/etc/tether` ⇒ 结构上看不到 #51/#52 两个面 ⇒ 闸门行注明「该面由 51 独占，50 不计入」。
**同时裁定**：**94-③ 的 restore 以 `-u tether` 跑不越界** —— 那是 `broker-ops.md:621-626` 的 **#6 权威配方**（产品自述 `offline.go:945-946` 亦然）；root 路径的坑是 **50-J1** 的暴露对象，94 不重复也不代劳。
**反证切换**：若 SB-50-3/SB-94-RESTORE 显示 in-place wipe 后 restore 因 `/etc/tether` 残留态走了与真实灾难不同的路径 → 50 改整机 + 显式区分（但那时 50 与 51 高度重复，应考虑合并）。

### **K — roadmap 文档订正四项** — 裁决：**全部订正**（CLAUDE.md「实现中发现设计问题先改文档再改代码」）
1. **roadmap §3-S9 出口「全套并发 3 连绿（已知 RED 除外）」结构上不可满足**（31/74/96/50 恒非-GREEN）⇒ 改写为 **verdict-stability 基线**（§8.4）。一个不可能满足的出口，在 Stage-B 会精确地转化为「把 RED 调成 GREEN」的压力。
2. **roadmap §2 的 S0 表状态列对 S0-artifact / S0-布局 陈旧**（实为 G-A 落地）⇒ 订正状态列（防重复实现）。
3. **roadmap §4.3「§7.4 单机备份 = cluster backup 同机制」是事实错误**（`broker-ops.md:479-491` 通篇不提 `cluster backup`，教的是 `sqlite3 .backup` + `tar jetstream/`；bundle 只有 `state.db`+`manifest.json`）⇒ **拆行 + DOC-20 + NOT-COVERED（非 tether 动词）**。
4. **roadmap §3-S7-52 的三处措辞**（`keygen` 铸 account.nk / `reconcile nats --all --wait` re-render / 「其余 staging 演练工具面」）被源码推翻 ⇒ 同步修 §3-S7-52 与 §4.4 备注。
**反证切换**：无 —— 四项均为文本事实。

### **L — Stage-A workflow 自身的覆盖缺口（`critique:feasibility-sim` 零产出）** — 裁决：**主进程亲测补齐，不重跑该镜头**
该镜头本应审的正是本 plan 里 spike 密度最高的一类（docker/systemd/netns 能力边界、容器 journal 持久性、`d cp` 属主、`up` 幂等）。其余镜头**部分**吸收了它。剩余的 5 条一面之词中，**主进程已亲手实测关掉 3 条**（SB-FAULT / SB-VAULT / SB-97-1，§0.3+§12）——**且其中 SB-FAULT 的实测同时推翻了综合稿关于 tc 的一条错误论断（§11-D），证明这一层复核是必要的、不是形式**。剩余 2 条（**SB-96-1** 8223 在 cluster 模式可用性、**SB-51-UP1** `up` 幂等）**均有已写死的降级出口**，列为 Stage-B 非-blocking spike。**不重跑镜头**：成本 > 收益。
**反证切换**：若 Stage-C 内审认为可行性面仍有未对抗复核的一面之词 → 补跑单镜头 `critique:feasibility-sim`（Opus 4.8），只审剩余 2 条 + `fault.sh` 的实现。

---

## §12 — Stage-B spike log（live；随实测追加，影响 disposition）

- **SB-GC-DRILL-LANDINGS（2026-07-17，全 7 drill 真栈迭代 DONE）— 与 §0.1 预期对表：**
  - **50** = **PRODUCT-RED**（68 pass, 0 assert-fail）✅ #50 + #64（新，Stage-A 未预见）+ DOC-27。
  - **51** = **PRODUCT-RED**（45 pass, 0 assert-fail）✅ #51 复现 + DR-STEP-LEDGER 量化（documented=4/required=5/undoc=2）
    + DR-completion NOT-COVERED（手动 gap-clear 在真栈不可组成可用 broker = #51/#52 的运维后果，诚实登记）。
  - **52** = **PRODUCT-RED**（0 assert-fail）✅ #54 两面（**比 plan 预期更强**：account.nk 在运行态换 reconcile 直接
    UNREACHABLE，不只是 false all-clear）+ #56 循环建议 + DOC-23 + B4-B7/D-group 两处 NOT-COVERED（轮换在运行态
    不可完成 = #54 后果）。
  - **94** = **GREEN**（51 pass, 0 fail）✅ G.1 missed-exit（前台 exec 造）+ orphan（产品路径 backup→run→restore→
    nats-restart→killed_orphan）+ G.5 审计 + ps LOST 全链路。
  - **95** = **INCOMPLETE（34 pass, 0 assert-fail, 0 setup-red — 干净落地）**：#23 判别性 T1 clean-exit / T2
    unclean-exit 两判别子实测通过（SuccessExitStatus=0 确认）+ T3 nats-restart 硬断（active∧Result=success∧真流量）
    + DELETING boot-resume NOT-COVERED（N=2 raft 与 JS 同生共死，中间态在真栈立不住，诚实登记）。符合 plan 预期。
- **SB-96-DEEPWATER（2026-07-17，drill 96 多轮真栈迭代 + Stage-C B1 修复 + 外审 B2/B10 修复后）— 分区旗舰臂定案：**
  - **#58 = LIVE-CONFIRMED PRODUCT-RED（外审 B2 修 oracle 后 3 次复跑一致）**：旧 `grep -c OBJ_xfer` 数 stream 存在性=假阳性
    （bucket 常驻，重启只删对象留 stream）；改 /jsz 数 `state.messages`（对象数）+ 差分（baseline=1 → orphan=2 → 重启后仍=2 = 未回收）
    才真钉住非-leader home broker 的 orphan xfer object。
  - **#65 = CANDIDATE / 非确定性（外审 B10 修正）**：D6b 加逐-broker RAW ARTIFACT 后，6 次复跑证明该现象**非确定**——5 次持久 run-1/2/6/7/8
    `brk1=yes brk2=yes brk3=yes`（持久/多数派可见）、1 次回滚 run-3 `brk1=no brk2=no brk3=no`（且该轮 D3 亦超时=退化 run）。即
    干净分区下分区少数派 stale-leader 写约 5/6 存活、偶回滚。这正是外审 B10 指出的「旧记录自相矛盾」的真因（现象本身非确定），**不能记确定性 PRODUCT-RED**；
    是 raft-safety 级疑点、owed 产品侧根因确证（gotcha #65）。
  - **旗舰分区臂**：分区件自证 rc=124（D1b/c）+ 选择性对照 4222 通（D1d）+ 幸存侧选举（D2）+ 多数派真写（D3，JS meta 用 2/3
    重组慢、窗口自 150s 放宽到 300s 后稳）+ brk1 alive/stable（D4a 用 `8223/varz`、D4c MainPID 稳）+ 愈合收敛一 leader（D5b）+
    **结果级无脑裂-丢失方向**（D6 多数派写读回，确定性硬断）。
  - **D4b 定案 = 记录式（OQ-7）**：N=3 分区后少数派 brk1 的写**非确定性**——brk1 未及检测失去 quorum 时作 stale-leader 接受写返 rc=0
    （实测多轮 rc=0、偶 rc=70 "apply lag"、可能 124）。D4b 只记录 brk1 瞬态行为、不硬断；**D6b 按每次单次 artifact 落支**（durable→
    product_red、rolled-back→GREEN、brk1-only→not_covered），**绝不跨 run 拼接**（外审 B10 的可追溯性要求）。
  - **#57 = not_covered（in-sim 时序限制，非产品缺陷）**：88 核 loopback 网络上 80 MiB tier-B 传输在 brk2 被杀前就完成，无法稳定在飞行中打断。#57 机理源码确证（watchdog 挂 runCtx 随进程死），hermetic 已覆盖。**关键：complete 落行 ≠ APPEARS-FIXED，是"没抓到 in-flight"= not_covered**（诚实区分）。
  - **arm B/C（run-PTY / expose-crash）+ F（双故障）= not_covered**：B 的 kill-broker-mid-run 是 GREEN-by-design（SB-96-3）、C 属 71/#29、F 的 agt1 跨-broker 恢复（其 tunnel 在被杀的 brk2 上）120s 内未回 ONLINE = arm A 注入的下游后果、gated 不 SETUP-RED 整 drill。
  - **Stage-C B1 是 96 一直没跑通的深层根因**：`timeout N dexec`=127（timeout execvp 看不见 sourced 函数）让 D4a/D4b 恒 ASSERT-FAIL 顶掉 #58；改 `dexec … timeout N tether …` + 加 lint `timeout-fn` 规则后才真跑到底。**96 是唯一没被真跑到底的 drill，这个 latent bug 恰好躲在那里**——印证内审的价值。

  - **96** = PRODUCT-RED 预期（#57/#58 源码确证；**S0-故障原语分区件已实测可用**：`iptables INPUT/OUTPUT --dport/--sport
    DROP` 对端口连接 rc=124 静默挂起、非过滤端口 rc=0 选择性对照、flush 恢复——旗舰分区臂脊成立）。**时长注记**：
    A 臂 transfer window（5min+90s poll #57）+ grow_to_3(2agt) + 分区 + 双故障 ⇒ 单跑需 ≥3600s timeout，
    **不可与他 drill 共用 2400s 预算**（96 必须单独长超时跑；已记入 §8）。
  - **97** = **GREEN**（41 pass, 0 fail）✅ 四型注入（agent kill/broker restart/分区/传输并发）+ fd/RSS/Threads
    leak oracle（全在界内，主观测进程 PID 世代不变）+ panic/FK 完整性 oracle。**S0-故障原语在 soak 下稳定。**
  - **本轮补记的实测规律（全部已入 plan §1.4 R-系列或头注）**：`node ls --json` 用 `nid` 非 `node_id`；
    audit `pid` 是 ULID；journal `--after-cursor` 静默过滤→改 `--since` timestamp gate；`pull` 语法是 `agt:/path /dst`；
    前台 exec 才造得出 missed-exit（nohup& 出 tether 视图）；`fault_partition_on` 用 per-port 对称规则（非 per-peer-IP）；
    vault_init 须在 grow 后（grow_to_3 retry-nuke 会删 vault）；深水区 drill（51/52/96）发现钉住后须 gated 收尾、不 cascade。
  - **command tree inventory test 通过（零产品 CLI 变更）** ⇒ §0.2「零产品 Go diff」铁律守住。

> 每条 spike 是 explore→pin 的实测证据；结论若与 §11 裁决冲突，以实测为准并注明。容器名格式 = `sim-<instance>-<node>`（`lib/docker.sh:15`）。

- **SB-FAULT（2026-07-17，主进程实测，DONE）— 定案 P1。** 镜像 apt 清单无 `iptables`/`nft`、有 `tc`。privileged 容器内 `apt-get install iptables` → `iptables v1.8.10 (nf_tables)`；`iptables -N SIMFAULT` + `-I INPUT/-I OUTPUT -j SIMFAULT` + `-A SIMFAULT -d <peer> -p tcp --dport 6222 -j DROP` + `-A SIMFAULT -s <peer> -p tcp --sport 6222 -j DROP` ⇒ 对端 6222 `</dev/tcp>` 退 **124（挂起=静默丢包）**、同对端 4222 退 **0**、`iptables -F SIMFAULT` 后 6222 立即回 **0**。⇒ **DROP-vs-REJECT 判别子（124 vs 立即失败）与选择性对照源均成立**。
- **SB-FAULT-P3（同上，作为 fallback 一并实测）— 综合稿的 tc 论断被推翻。** `tc qdisc add dev eth0 root handle 1: prio bands 4` + `tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 100%` + `tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst <IP>/32 match ip dport <PORT> 0xffff flowid 1:4` ⇒ 目标端口 rc=**124**、同 peer 非过滤端口 rc=**0**、ICMP 仍通、`tc qdisc del dev eth0 root` 后 rc=0。**故 tc 能做选择性、静默、可清理的定向分区**（差别只是 egress-only ⇒ 双向需两条 filter）。综合稿「P3 ⇒ 96-D 的选择性对照必须 not_covered」**不成立、已删**（§11-D）。
- **SB-VAULT（2026-07-17，主进程实测，DONE）— 定案 `d cp`。** 容器内 tether-owned(uid 1002) 文件 → `docker cp` 到 host ⇒ `weiland:weiland` 0644、`cat` 免 sudo 成功、sha256 可算。host → 容器 ⇒ 落 **uid 1000**（**不是 root**，综合稿的理由订正；结论「`vault_push` 须 chown」不变）。**bind-mount 不必引入。**
- **SB-97-1（2026-07-17，主进程实测，DONE）— 出口 (a)。** 镜像有 `/var/log/journal`（持久 journal Storage）、无 `/run/log/journal`。`node_kill`/`node_start` 保留容器与写层 ⇒ **journal 跨 boot 存活** ⇒ 跨 boot `--since` 检查成立（去掉 `-b`）。
- **SB-50-LANDING（2026-07-17，drill 50 真栈迭代 10+ 轮，DONE）— 定案 `PRODUCT-RED`，与 §0.1 预期一致。**
  最终：`verdict=PRODUCT-RED rc=3 assert_fail=0 setup_red=0 product_red=3 not_covered=0 **pass=68**`。
  三个真缺陷全部 LIVE-CONFIRMED 并入台账：**#50**（doctor `--db` 失明，源码预判 100% 命中）·
  **DOC-27**（runbook:524 示例跑不了，**真串与预判不同**，已按实测订正）· **#64（新，Stage-A 未预见）**。
  **五条 Stage-A 预判被实测推翻，plan 已逐条订正（留着不改就是误导下一个读者）**：
  1. **§2.1 的 SCOPE BOUNDARY 错**：原写「50 保留 `/etc/tether` ⇒ 结构上看不到 #51/#52」。实测 50 **能**看到
     同族的**名册剪枝**那一半 ⇒ 立 **#64**（restore 剪到单 voter 却不去集群化 nats.conf、完成文案从不提，
     照文档做必 crash-loop；产品在崩溃时刻自己印出了缺的那一步）。
  2. **#64 的恢复机理**：drill 50 里 broker ~73s 后自愈，但 **nats.conf 仍 clustered** ⇒ **不是** reconciler
     去集群化，而是 **brk2 的 nats-server 还活着**、clustered JS meta 跨两个 server 重新形成。全灭场景无此
     幸存 peer ⇒ 该结论**只属于 51**。drill 里该臂已从「断言 reconciler 收敛」改为**如实记录实际机理**。
  3. **L3「bundle 不含 JS ⇒ history 必失」在 50 的拓扑下不成立**：lib-wipe 有 brk2 的 JS 副本 ⇒ history
     **存活**（APPEARS-FIXED 门正确开火抓住了我的错误假设）。该断言归 **51-J（全灭）**，50 改为断言
     replicated-JS 存活这一真实行为。
  4. **L4d 的「restore 把端口 re-home 到 self 且 epoch+1」是未经验证的预测、实测为假**：agent 日志
     `rehomed expose name=live port=14000 **epoch=0**`。自洽解释：home 本就钉在 brk1，re-home 到 self 是
     no-op ⇒ 不 bump。断言已**删除**（强行断它 = 误伤 restore 的假红）。
  5. **50 也必须 `--on-broker brk1` 钉 home**（原只给 51/52 写了这条）：实测不钉 = **~50% 抛硬币**
     （6 跑 3 败），真因是 **#29** 的 allocate-time 面 `agent_rejected:frpc_failed` ⇒ 台账扩 #29 blast-radius，
     不发新号；让一个已登记的、属于 drill 71 的缺陷在这里随机开火只会制造**误伤 restore 的假红** + flake。
  **本轮我自己犯并修掉的 harness bug（全部已加结构性守卫或头注钉死）**：
  ① `_broker_ready` 拿 `cluster status` 的**退出码**当存活探针 —— 但它对 DEGRADED 集群**按设计退 1**，
     restore 后 N=1 恒 DEGRADED ⇒ poll 永不成功 ⇒ **差点把自己的 oracle bug 报成产品缺陷**（G-B drill 91 的
     教训重演）。50/51/94 同款一并修，改为「答出可解析 JSON = 活」。
  ② `node ls --json` 的字段是 **`nid`** 不是 `node_id`（后者是 `cluster status` 的字段，两个 API 不同）⇒
     查错键静默匹配不到 ⇒ 一个完全 ONLINE 的 agent 被 poll 到超时、看起来像产品故障。50/51/52/94 全修。
  ③ audit 的 `pid` 是 **ULID** 不是 OS pid（`exec.go:415`）⇒ 94 的 A3/B4 必须用 `ps --json` 的
     `.processes[].pid`，拿 `pgrep` 的 OS pid 去 grep 永远失配。
  ④ **诊断写在 `assert_ok` 里会被 `_as_capture` 吞掉**（只露 `tail -3`）⇒ 一切诊断必须打在 assert **外层**。
  ⑤ **`sh -c` 不继承 harness 函数**（本轮撞 3 次，其中 `sh -c "! node_exists brk1"` 是**永久假绿**）⇒
     已加 **lint 规则 `noshc`**（`tests/lint-drills.sh`，经 mutation 验证：合成违规被抓、既有 9 drill 无回归）。
  ⑥ `session create` 会**激活**新 session ⇒ 建完同一性 oracle 的阴性半边 `zed` 后 ctl 停在 zed 上，restore
     把 zed 回滚 ⇒ 后续每条 ctl 调用 `Authorization Violation` ⇒ **4 条级联假失败**。建完立刻切回 lab。
  ⑦ `expose` 的 `--local` 是**带值 flag**；`$SIM ctl` 已注入 `tether`（不要再写一次）；`expose explain` 是
     `.home_broker`（非 `.home`）且 `epoch` **omitempty**（为 0 时不出现 ⇒ 必须 `// 0`）。

- **环境（2026-07-17，主进程实测）**：`ssh weilandserver` 通（88 核 / 251 GB / inotify 8192 / 0 容器）；`./remote.sh --build build` 全路径成功（baked nats-server 2.10.22 == install.sh pin）⇒ **Stage-B 真栈就绪，且允许 image 增量**。

> **⚠ MANDATE 校准（继承 G-B，用户 2026-07-15 重申）：目标是暴露 tether 缺陷，不是全绿。** 每个 RED 分类为 (a) harness-bug（修 drill 自身机制）或 (b) 真产品/观测缺口（EXPOSE：登 gotcha + signature-guard RED，drill 因暴露真缺陷而 RED = 成功）；**绝不为绿松 oracle**。

---

**相关文件（绝对路径）**

**待建**：`/home/weiland/projects/dist_experiment_control/test/simcluster/drills/{50-backup-restore,51-full-dr,52-credential-rotation,94-agent-reconcile,95-broker-selfheal,96-mid-flight-chaos,97-soak-cycles}.sh`、`.../test/simcluster/lib/vault.sh`、`.../test/simcluster/drills/lib/{fault.sh,events.sh,leak.sh}`

**待改**：`.../test/simcluster/simcluster`（`cmd_nuke`）、`.../test/simcluster/lib/secrets.sh`（`secrets_mint_gen`/`secrets_distribute_gen`/`secrets_remint_route_only`/`secrets_mint_tunnel_only`/`secrets_tunnel_fp`/`secrets_push_file`）、`.../test/simcluster/drills/lib/cluster.sh`（`grow_to_2`）、`.../test/simcluster/Dockerfile`（`+= iptables`）、`.../test/simcluster/tests/lint-drills.sh`（`BATCH`）、`.../test/simcluster/.gitignore`（`backups/`）、`.../test/simcluster/README.md`（drill 表 + **全套并发基线新节** + disposition 表）、`/home/weiland/projects/dist_experiment_control/docs/deploy-tier-gotchas.md`（#50–#62 / DOC-17–DOC-28）、`/home/weiland/projects/dist_experiment_control/docs/reviews/simcluster-coverage-inventory.md`（consume + "G-C landing" + **"S1–S9 CLOSURE"**）、`/home/weiland/projects/dist_experiment_control/docs/reviews/simcluster-coverage-roadmap.md`（§2 S0 表 / §3-S7-52 措辞 / §3-S9 出口 / §4.3 映射 —— §11-K）

**关键复用（勿重造）**：`.../test/simcluster/lib/{assert,docker,log,tether,secrets}.sh`、`.../test/simcluster/drills/lib/{cluster,dataplane,agentyaml,setup-forcesingle}.sh`、`.../test/simcluster/image/{pty-run.py,pty-confirm.py,provision-node.sh}`、`.../test/simcluster/run-drills.sh`
