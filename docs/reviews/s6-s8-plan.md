# `docs/reviews/s6-s8-plan.md` — G-B deploy-tier drill plan (S6 + S8)

> Finalized by the main process, 2026-07-14. 9 drills, one merged plan, one external review (execution group **G-B** per roadmap §2.1). Drafted by a 6-lens adversarial Stage-A workflow (6 draft + 6 critique + 1 synth, all Opus 4.8), then main-process adjudicated — **§11 records the finalized dispositions of the 11 open items (A–K) the synthesis surfaced.** The 6 load-bearing structural claims (#31 retire-gate / online force-single surface / broker_down-needs-N≥3 / 92(a) JS-503 leader-gating / `observability` yaml key / admin-has-no-events-reader) were **independently re-verified in source by main** before finalizing.
>
> 结构：§0 范围/依赖/可行性 · §1 共享 S0+harness+横切规则 · §2 S6(40/41/42/43/22) · §3 S8(90/91/92/93) · §4 gotcha ledger(#35+) · §5 NOT-COVERED(永久+gated) · §6 OQ 解决 · §7 inventory 行消费 · §8 run-drills/拓扑/flake · §9 per-drill false-green · §10 Stage-B spikes · §11 主进程定稿裁决。
>
> 所有 identifier/命令/信号英文；破坏性臂逐条给五要素；一切 rehome/失效/撤销/failover/quorum oracle 收在**真流量恢复**或**对照源对比**，绝不 status 字段。**只测不修：产品缺陷登 gotcha #35+ 并 signature-guarded RED，绝不改产品 Go 代码。零新 baked `image/*` 文件（SSH-down 约束）。**

---

## §0 范围、依赖、可行性

### §0.1 Drill roster（拓扑按源码校正；roadmap prose 被源码推翻处标注）

| drill | 批 | N | 主题 | GREEN/RED 预期 | 债 | 预计 | key blocker |
|---|---|---|---|---|---|---|---|
| **22**-forcesingle-online | S6 | 2 | online force-single dwell + 5 拒绝门 + protected-mode | **explore→pin, DEFAULT EXPOSE**（POSITIVE / #35 RED / tamed→POSITIVE-caveat：total function） | 供 92(b) | ~8–12min | dwell-bounce(#23/#35) |
| **40**-drain-retire | S6 | 3 (+expose fixture) | drain round-trip / retire ladder / ops / reconcile-plan / 安全门负例 | **explore→pin, #31-gated** | — | ~10–12min | **#31（既有）** |
| **41**-shrink-to-standalone | S6 | 3→1 | reconcile `--to-standalone` + set-raft-addr rebind + regrow | GREEN body, #31-gated shrink 脊 | — | ~10min | #31 |
| **42**-rejoin-returning | S6 | 2 (fs fixture) | 弃节点冷启动诊断 / rejoin prepare / init --from-manifest / resnapshot / diagnose | GREEN + **DOC-2 confirm** | — | ~8–12min | — |
| **43**-migrate-live-data | S6 | 1→cluster | init --from-existing WITH 活数据 + rollback（restore `tether.db.bak`） | GREEN（承重 Stage-B spike） | — | ~6–10min | P2-serve spike |
| **90**-alerts-lifecycle | S8 | **3**（roadmap 写 N=2，**主进程采 C1 更正**） | manual raise/ack/clear + broker_down + disk_pressure + quorum_lost-ack refuse | GREEN | — | ~6–9min | broker_down 需 N≥3 |
| **91**-client-converge | S8 | 1→2→3 | G3 A/C/D + cli-failover + pin/anchor | GREEN + candidate | **G3 A/C/D** | ~8–10min | #31（A3/C 的 retire） |
| **92**-js503-remote-alert | S8 | 2 | (a) quorum-loss remote 面 + `--ack-alerts`；(b) online-FS banner | GREEN(a) + (b) gated on 22 | **G7b + G3-B** | ~6–8min | 22 结论（仅 b） |
| **93**-metrics-observability | S8 | 3 | /metrics /healthz /readyz + alert webhook + --card/--watch/--offline/--log-json | GREEN | — | ~8–10min | broker.yaml seam + POST 可达 |

### §0.2 只测不修铁律

S 系列只交付 **DRILL**。tether 缺陷 → gotcha(#35+) + signature-guarded `assert_bug` RED（或 labeled `[GAP #N]`）；修复另立独立叶子增量。仅 harness(sim 脚手架) bug 随批修，**绝不**以 harness 债名义把 tether 需要的 workaround 洗进环境（anti-masking，Mandate ④）。**零产品 Go diff。**

### §0.3 备用计划可行性（tether-exec-only）— 全 9 drill 可活体跑

`weilandserver` 现只经 `tether exec weilandserver -- <cmd>`（user `weiland`）够到；SSH/rsync/`remote.sh build` 不通 → **`tether-sim:dev` 镜像冻结、不能 rebuild。** 全 9 drill 在此约束内可行：

- 一切交付物是 `drills/*.sh` / `drills/lib/*.sh` / `lib/*.sh` / `simcluster` —— rsync-class，base64-arg 经 tether-exec 投递（md5 已实测匹配），落盘即生效（G-A 已证流程）。
- 唯一"新 helper"诱惑 —— 93 的 **webhook 接收器** —— 在**容器运行时**投递（dexec-heredoc / `docker cp` 一个 `python3`-stdlib 脚本进复用的 ctl 容器；`python3` 已烘 `test/simcluster/Dockerfile:14-20`）。**不是** `image/*` 文件。
- 每容器有 `--network-alias == node id`（`lib/docker.sh:34`），故 `alert_webhook_url = http://ctl1:PORT/` broker-resolvable。
- **本组零 `image/*` 增量。** 若 Stage-B 发现某臂非 rebuild 不可 → 该臂改运行时注入或降 NOT-COVERED，绝不阻塞。

### §0.4 组内顺序约束（roadmap §2.1，固化）

**S6-22 的结论先于 S8-92(b) 定稿。** 这是 **plan-time** 约束、非 drill-runtime 依赖：22 的结论（reachable / #35 RED）是 plan §2.5 disposition + gotcha 台账里的记录，92(b) 的臂**据此结论书写**。drill 号序（22 < 92）+ S6-before-S8 实现序在串行或拆分下都满足它。92(a)/91-A/C/D/93 全 **22-无关**。

---

## §1 共享 S0 / harness landing + 横切规则

### §1.1 S0 landing check — G-B 落地零新 S0（roadmap §2.1 成立）

全部所需已由 S1/S2/G-A 落地，主进程逐条确认：

- **S0-tunnel**（S2 落，`drills/lib/agentyaml.sh` 写 `tunnel_addr`；`image/provision-node.sh:27-33` 使 `public_host` docker-resolvable）→ 40 的 expose fixture、43 的 expose 存活。
- **S0-pty**（`image/pty-confirm.py` 已烘，12/20 已用）→ 22/40/41/42/43 的 typed-confirm + machine-confirm 臂。**无需改镜像**（drill 12 已跑 `TETHER_CONFIRM_NODE_ID=… --confirm-node-id …` 模式）。
- **`setup_forcesingle_n2`**（`drills/lib/setup-forcesingle.sh:6-18`，杀前先断 JS `cluster_size==2` + tier-B baseline 的承重假绿守卫）→ 22/42/91-C/92。
- **`grow_to_3` / `sim_leader` / `a_non_leader_voter`**（`drills/lib/cluster.sh`）→ 40/41/43/90/91/93。
- **`node_kill/node_stop/node_start/tcp_refused`**（`lib/docker.sh`，容器+卷保留）→ 一切 kill-then-return 臂。
- **S0-ingress 本组不需**（无 `/sub`/manifest 跨容器腿；93 的 `/metrics` 是 broker-loopback dexec-curl；webhook 是 broker→接收器 POST 走桥）——**例外** = 91-C-anchor 的 foreign-cluster-B 变体（§3.2），降为 Stage-B spike、默认走 torn-anchor（§11-U2）。

### §1.2 harness lib 增量（全 rsync-class；零 `image/*`）

1. **`setup_forcesingle_n2` 参数化** — 线程 `agents`/`ctl`/`retry` 参数（镜像 `grow_to_3` 的 attempt-accounting）、RETURN survivor id 让 *drill* 自己决定 kill + online/offline。两条假绿守卫（JS `cluster_size==2` + tier-B push）保持**无条件**。
2. **`obs_enable <brk> <metrics-addr> <webhook-url>` helper**（新 `drills/lib/obs.sh`）for 93。**[HARD，主进程已核验]**：broker.yaml 键是 **`observability`**（`internal/serveconf/serveconf.go:34` `Obs ObsSection \`yaml:"observability"\``，children `metrics_listen`/`alert_webhook_url`/`log_json`），**不是** `obs`。在既有 `broker:` map 下追加 `  observability:`（2-space 缩进，与 `simcluster:176` cluster-seam 同形），children 4-space；idempotency grep `grep -qE '^    observability:'` 守。yaml.v3 静默忽略未知 `obs:` 键 → listener 不绑 → 93 假绿，**禁用 `obs:`**。install.sh 不烘 `observability:` 块（无重键冲突），唯一 hazard = 错键/错缩进。**Fallback：seam 若不 round-trip，改 systemd drop-in 覆盖 ExecStart 加 flag（同 rsync-class）**。*Stage-B 确认 YAML 经 install.sh 烘出的 `broker.yaml` round-trip。*
3. **webhook 接收器** — 运行时注入 python3-stdlib 进 ctl 容器（§0.3）。这偏离 roadmap "webhook 接收器角色容器"表述——**主进程 ratify 此偏离**（§11-U8），更新 roadmap harness 注以免无遗漏闸把角色容器读成缺失。
4. **无新 shrink/return fixture 文件** — 40/41 从 `grow_to_3` 起；41/42 经 `$SIM exec <brk> -- tether …` 直调（12/20 先例）。online force-single 路径已存在为 `cmd_force_single`（`simcluster:392-421`）；22 演练/裁决它，不书写它。

### §1.3 横切诚实规则（引用 s3-s5-plan §1.A 的 R-系列，仅记 delta）

- **R-DATAPLANE / #20 假绿**：一切 rehome/failover/撤销/quorum oracle 收在真 curl/SS/数据面恢复 **或** 独立对照源在受害期成功。视图在死数据面上收敛 = #20。
- **R-CONTROLSRC**：安全/撤销/门类臂必须带一个 SUCCEEDS 的对照（"全部都失败"永远不是门存在的证据）。
- **R-5ELEM / R-SIGGUARD / R-EXPLORE-PIN / R-CLEANUP-机制 / R-NO-HOST-LEAK / Mandate ①–④** 全文适用，各臂只引用不重述。
- **事件-reader-absence（主进程已核验 `cmd/tether/admin.go` 仅 sessions/nodes/audit/evict）**：`admin` 无 raw `sys.events` reader。一切 raw `pubSysEvent`/rehome/drain 事件测点 = **NOT-COVERED-as-event**，效果由 READABLE 源钉（cluster status 可达性、/sub http_code、真流量、`alert ls`/webhook）。**store-backed 告警是可读的（`alert ls`+webhook POST）→ 覆盖它们**；raw-event 配对不断言。

---

## §2 — S6 drills

### §2.1 drill `40-drain-retire`（N=3 + expose fixture；GREEN body + #31-gated retire 脊）

**承重门 — #31 阻塞 online retire（主进程已核验源码）。** healthy `grow_to_3` 后，`StartRetireOperation`（`internal/broker/cluster_operation_controller.go:179-180`）在 `growActiveJoiner != ""` 时硬拒 `cluster retire`，串 *"a `cluster add` grow of %q is in progress — retry after it completes (G4 §B)"*；`releaseGrowLock` best-effort、几乎总残留（`cmd/tether/cluster_add_drive.go:494-506`），fixture 有意保留该 leak（`drills/lib/cluster.sh`）。**成功 grow 后 join op 是 terminal SERVING**，`ops ls` 也不显示非-terminal op → **唯一 operator-observable 信号 = `cluster retire` 拒绝串本身**。**DELETE** 综合稿早期的 `cluster status --json | jq .GrowLockActive` 预读支路（`GrowLockActive` 只在 broker health-probe 应答上、不在 adminsock `ClusterStatusReport`；且 jq 路径 PascalCase vs snake_case tag 错）。retire 脊结构：(1) 尝 `cluster retire`，**无条件** `assert_bug "online shrink blocked by grow-lock leak" "#31" "grow of .* is in progress|already in flight"` 作头等 gap pin；(2) 再 labeled `[GAP #31]` 清（re-run `cluster add … --account-seed`，唯一 tether-native 清法——`ops abort` `AbortOp` `:268-280` **从不碰** `cluster_meta` marker、是已证死路）；(3) 再 GREEN 脊。**#31 blast-radius 扩 +retire(40)+shrink(41)**（无新号，§11-U4）。

**拓扑**：N=3（`grow_to_3 1 1 1`，retire 需 survivor voter 集）；expose home 钉在**非-leader voter**（leader 存活作 op driver）。admin verb 在 broker 内以 tether 跑（`$SIM exec <leader> -- runuser -u tether -- tether cluster …`），typed-confirm 喂 baked `pty-confirm.py`。

**SETUP**：`grow_to_3 1 1 1` → `session lab --pin` → `agent-join agt1` → `agent_provision_yaml agt1 lab nats://<agt1-tunnel-broker>:4222 open`（S0-tunnel）。`LDR=$(sim_leader)`。

**#31-无关臂（无论 #31 都成立）：**

| 臂 | oracle / 签名 | 源 |
|---|---|---|
| **D-drain** round-trip | `cluster drain T` → poll `ops show T` State=`draining` + phase `DRAINING`；`--abort` → poll 回 `VOTER`/`done`。**FG 守卫**：断中间 DRAINING 被 OBSERVED 才 abort（同 tick abort 假绿）。plain drain 只查 `assertNoActiveOp`、**不查** `growActiveJoiner` → 非 #31-blocked | `clusterdrain.go:85-89,146-149,359-370`；`cluster.go:476-526` |
| **RED drain --retire redirect** | `cluster drain T --retire` → `assert_refuses "is removed.*cluster retire\|run .*cluster retire"` | `cluster.go:485-490,521-524` |
| **OPS-OBS** `ops ls/show` | 表头 `NODE OP STATE PHASE UPDATED LAST_ERROR`；`--json` schema `cluster_ops` v1 / `cluster_op` v1 带 timeline | `cluster_ops.go:90-160`；`clusterops.go:21-95` |
| **OPS-ABORT** `ops abort` 释放槽 | ① 非-terminal op 在 ② `ops ls` State 非-terminal ③ `cluster ops abort <id>` ④ 语义：State→terminal + stderr *"active slot freed; membership unchanged"* + 后续同-target op 可入 ⑤ nuke | `AbortOp cluster_operation_controller.go:268-280` |
| **RECONCILE-plan** `reconcile nats --manual --plan` 零写 | footer **`# NOTHING WAS WRITTEN`**（仅 `--manual --plan`）；零写 oracle = `md5sum` + `.bak` mtime 字节等同 | `renderTakeoverPlan cluster_natsconf.go:564` |
| **APPLY-plan** `cluster apply -f` plan-only | 有序 plan，roster/status 前后字节等同；非-leader socket → `assert_refuses "authoritative LEADER view\|non-leader view"`；partial status → refuse | `cluster_apply.go:11-13,21-50` |
| **ADD-dryrun** `cluster add --dry-run` | 解析+打印 grow plan，零改动。*Stage-B SB-4：确认 dry-run target 无真 joiner 可解析* | `cluster_add.go:124` |

**安全门负例（#31-无关）：**

| 臂 | oracle | 源 |
|---|---|---|
| `recovery node remove --yes` 拒 | `assert_refuses "There is NO --yes override\|cannot run unattended"` | `cluster.go:548-575,973` |
| machine-confirm 双因子 | 非-TTY `recovery node remove <n> --manual --confirm-node-id <n>`、`$TETHER_CONFIRM_NODE_ID` **unset** → `assert_refuses "type the node_id.*confirm-node-id\|aborted"`；正例对照两者都 set → proceeds | `cluster.go:551-555` |
| `--manual` required | `recovery node remove <n>`（无 --manual）→ `assert_refuses "requires --manual\|routine path is .*cluster retire"` | `cluster.go:548-550` |
| `--force` 孤儿化 | **NOT-COVERED-hard-to-manufacture**（需卡住的 VOTER_ADD_FAILED node homing exposes）；ghost-passthrough `--force` hermetic(drill 12) | `clusterdrain.go:241-280` |

**retire 脊（#31-gated；explore→pin）。** op-machine `driveRetire` 在 `cluster_operation_controller.go:662-780`（**非** `clusterdrain.go`；legacy 一次性 `DrainNode(retire=true)` `clusterdrain.go:158` 是**不同**路径，R-retire 走 op machine）。

| 臂 | oracle / 五要素 | 源 |
|---|---|---|
| **R-retire**（N=3→2，F==0 pty-confirm） | ① 3 VOTER，`T=$(a_non_leader_voter)`，home-owned expose 在 ② leader `cluster status --json` + `ops show T --json` ③ pty 喂 `cluster retire T`（F==0 typed-confirm 先触，`cluster_retire.go:104-117`，绝无 `--yes`） ④ **raft-config 腿无 operator reader → 绝不以 "T 不在 committed raft config" 收口**。raft-removal **间接**证：surviving 2-node 上一个独立对照源 WRITE 成功（`session --pin` create 或 `nats kv put` 真 COMMIT——仅当 raft config 相干、无幻影 voter、quorum 活才可能）。roster-delete（`status` 显 T 消失）作**佐证判别子**。#31-gated ⑤ nuke | `cluster_retire.go:95-135`；`cluster_operation_controller.go:753,758-765` |
| **R-streams** | best-effort 佐证、非硬门（streams 已达 target 时 `streamsReadyFn` 立即 ready）；硬 oracle = terminal RETIRED gated by convergence | `cluster_operation_controller.go:697-707,736` |
| **R-wait** | `cluster retire T --wait --timeout 10m` 阻塞到 RETIRED exit 0；非-`--wait` 打印 `watch: cluster ops show <op>` 立返（对照断言） | `cluster_retire.go:80-86,131-134` |
| **R-mid-interrupt**（leader-kill mid-retire → resume；#31-gated，见 §11-U6 定稿裁决） | N=3 下杀 retiring T 期间的 leader L → {L(dead),V}；resume 驱到 RAFT_REMOVED 后 roster-delete 需 quorum-of-{L,V}=2、只 V 活 → **BLOCKS**（L 死时永达不到 RETIRED）。**oracle 重定义**：在 **pre-RAFT_REMOVED** 态（`ops show T` ∈ {DRAIN_REQUESTED, REHOME_EXPOSES, STREAMS_AT_TARGET}）杀 L；断新 leader 的 controller **resume + dwell(BLOCKED/holding)、无 double-apply/panic/FK**；`node_start(L)` 复 quorum（收敛的**前提**、非清理）→ 推进到 RETIRED。explore→pin：若 resume wedge/panic/orphan → **#37**（§4）。全 kill 用 `node_kill`(容器) | controller docstring `:287-293` |
| **R-re-retire**（N=2→1，F==0） | R-retire 后 `cluster retire <剩余非-leader>` → 1 voter/quorum 1/F=0 typed-confirm；允许（非 last-voter refuse `Voters<1`）。同 control-source-write oracle。#31 复检 | `cluster_operation_controller.go:195-205` |
| **R-hint**（retire ≠ 撤销） | NOTE 打到 **stderr** → `assert_ok … 2>&1 \| grep "NOT a credential revocation"`。`--compromised`/`--require-credential-rotation` 是 **S7-52**、非 40 | `cluster_retire.go:122-130` |

**OPS-CONFIRM 臂（需真 BLOCKED op — Stage-B SB-2）**：N=3 造真 BLOCKED retire 不可达（retire→立即 F=0→pre-start typed-confirm、从无未确认 op；`driveRetire` 用 plain `recordOpError`，只 JOIN 用 `blockAfterAttempts`→BLOCKED `:638-651`）。**SB-2**：(a) JOIN-BLOCKED via 停止的 joiner 超 `opCatchupTimeout`(~2m) → `cluster ops confirm` 重入 CATCHING_UP；或 (b) `ops confirm` **NOT-COVERED-in-sim**（ConfirmOp/AbortOp hermetic）。绝不 fake BLOCKED via synthetic state write。主进程裁 §11-U8。

**#29 诚实（retire→expose-rehome）**：rehomed expose 受 #29-stranded（cluster expose 仅当 `home == agent 自己的 tunnel broker` 才服务，`home.go:96-113`；`migrateExposes` 把 home 迁到非-tunnel survivor）。40 只在 **CONTROL plane** 断 retire 的 `migrateExposes`（roster `home_broker` 列迁离 T）；DATA-plane crash-strand 交 **71/#29**（gap-exposure-first，不在此断真流量正例除非 Stage-B 证 drain-migrate 可达）。

---

### §2.2 drill `41-shrink-to-standalone`（N=3→1；GREEN body + #31-gated shrink 脊）

**拓扑**：N=3 via `grow_to_3 0 1 1`；种一个 tier-B 业务行（session `lab --pin` + 一个 **command-domain / raft-replicated** 行，**非** JetStream stream——见 S-restart oracle）。记初始 per-broker `nats.conf` md5。

**Peer-present 硬拒（#31-无关，负例先行）：**

| 臂 | oracle | 源 |
|---|---|---|
| **S-refuse-peers** | 2+ voter：`reconcile nats --to-standalone --confirm-single …` → `assert_refuses "cluster has [0-9]+ voters .*need EXACTLY 1\|retire.*down to N=1"`（`--plan` 也触） | `runReconcileToStandalone cluster_natsconf.go:143-171` |
| **S-refuse-no-confirm** | 无 `--confirm-single` → `assert_refuses "FINAL N=1 step\|re-run with --confirm-single"` | `:126-128` |
| **S-refuse-already-standalone**（可选） | already-standalone → `assert_refuses "already standalone\|nothing to de-cluster"`；standalone conf 不易 stage 则 defer | `:123-125` |

**shrink 脊（#31-gated，同 40 的墙；若 blocked → to-standalone happy-path NOT-COVERED-blocked-by-#31，41 落负例 + set-raft-addr；OFFLINE de-cluster(drill 20) 仍覆盖机制）：**

| 臂 | oracle / 五要素 | 源 |
|---|---|---|
| **S-retire×2 → N=1** | 同 40 R-retire/R-re-retire（F==0，control-source-write oracle）。#31-gated | 同 40 |
| **S-tostandalone** | ① 恰 1 voter ② survivor `nats.conf` 字节 ③ `reconcile nats --to-standalone --confirm-single --server-name <s>` → conf 无 `cluster{}`、pristine `.bak`、re-parse standalone-JS + 同 `store_dir`、打印 "FULL restart REQUIRED … RESET the JS store" ④ 无 `cluster{}` 块 + `.bak` 保留 ⑤ nuke。空 store_dir → refuse | `:118-239` |
| **S-jsreset** `[operator per runbook §2.2]` | JS-store reset（`systemctl stop nats-server && rm -rf <store> && start`）是 **runbook 强制 operator 步、非 gap**（NATS 无法原地迁 clustered-JS→standalone；`warnClusteredJSShrink` 逐字打印它）。断 drill **执行**该打印过程。**合法手动 → 无 gotcha**（S6 铁律边界的答案） | `:92-110,235-237` |
| **S-restart-tierB** | `systemctl restart nats-server`（FULL、非 SIGHUP）→ poll standalone HEALTHY → 存活 oracle 读 **command-domain / raft-replicated** 行（种下的 `session --pin` 或 port allocation，真扛过 JS wipe）。**DROP 任何 "JetStream stream 存活" 断言**——S-jsreset `rm -rf`'d JS store，stream 写按设计被毁；断它存活会 false-RED（或更糟：只在 reset 静默没跑时过、掩盖文档化数据损失） | `:235-238` |
| **S-r3-persist** | 再 restart `tether-broker` → `nats.conf` 仍 standalone（reconciler 收敛到当前 desired gen=standalone）。FG 守卫：re-add `cluster{}` → **#38**（§4） | `cluster_reconcile.go:80-132` |

**§1.0 rebind 臂（`set-raft-addr` online rebind）：**

| 臂 | oracle | 源 |
|---|---|---|
| **S-rebind** | N=1 survivor advertising loopback raft addr → `cluster set-raft-addr <public:7400> --route nats://<public>:6222` → "raft advertise address rebound …" + "run `cluster reconcile nats` …"；`grow brk2` → regrow N=2、模式保留、roster `raft_addr` 显新 addr + brk2 VOTER。**DROP "self-only guard" 腿**（`set-raft-addr` 取 `<host:port>` 无 node-id arg，peer rebind 非 CLI-reachable，self-only refuse broker-internal、hermetic-owned） | `cluster.go:639-693` |

**SB-3（Stage-B）**：loopback-advertise 前提。sim broker advertise DNS `brkN:7400`、非 `127.0.0.1`。确认 N=1 survivor 能否带到 loopback（OFFLINE force-single fixture / `init --allow-loopback`）使 rebind→public 有意义；否则断 any→any 原地 addr *change* + regrow（loopback→public 留 hermetic d9）。此臂独立于 to-standalone 脊（落在任一 N=1）→ #31 blocked 时仍存活。

---

### §2.3 drill `42-rejoin-returning`（N=2 force-single fixture；GREEN body + DOC-2 confirm）

**SETUP**：`setup_forcesingle_n2` → healthy N=2 clustered-JS + agt1 + ctl（fixture 杀前断 JS `cluster_size==2` + tier-B baseline）。按 drill 20（`20:26-45`）造弃节点：`node_kill brk2`(保容器) → brk1 `systemctl stop tether-broker` → **OFFLINE** `recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2`(pty) → JS-store reset + restart → brk1 lone N=1 leader；brk2 = 弃容器（on-disk raft config 仍列 {brk1,brk2}）。OFFLINE（非 online——online dwell 是 22 的活）。

| 臂 | oracle / 签名 | 源 |
|---|---|---|
| **A** 弃节点冷启动出 ACTIONABLE rejoin 诊断（G2 #15 回归） | `node_start brk2` → 其 broker 见 voters≥2 + peer 在 on-disk raft config 但无 clustered-JS quorum → 出 ranked EJECTED-vs-transient 微分。**Oracle = journal MESSAGE CONTENT**（`$SIM logs brk2 tether-broker` 匹配 `recovery rejoin prepare` AND (`EJECTED`\|`raft config still lists peer`)），**绝不** broker liveness（fix 是 message；`broker.go:941` 仍 `return` error → exits 70、`Restart=always` 续 bounce）。**FG 守卫**：必是 voters≥2 支（`broker.go:941-958`）、非 voters≤1 de-cluster 支（`:926-933`）。*Stage-B（M-10）：`$SIM logs`=`journalctl -n 200`，crash-loop 里 EJECTED 行每 boot 打一次、可能滚过 200 → poll 须抓 fresh boot；确认 error 串真落 journalctl* | `broker.go:941-958` |
| **B+C** `diagnose` / `force-single --guided`（同打印 `DiagnosePeers`/`forceSingleGuided`） | **两个真出口成对（消除 "全部 probe 都失败" 假绿）**：(i) **dead-peer 路径**：roster 仍列 dead peer 处运行 → 打印可粘贴 `force-single … --confirm-peers-dead brk2` 命令、exit 0、执行 nothing（断 leader/roster 不变）；(ii) **alive-peer 路径**：`diagnose --self-id brk2`、brk1 是运行的 N=1 leader → 探 brk1 ALIVE → `usageErr` **非零** + `assert_refuses "a peer is still reachable\|ALIVE"`。read-only 路径的零-mutation 断言（raft/+tether.db 字节不变）= FG 守卫。N=1 退化 "already single-node" 支（`wizard.go:26-29`）由签名 EXCLUDE | `cluster_recovery.go:41-43`；`cluster_offline_wizard.go:16,26-49` |
| **D** `rejoin prepare` dump 0600 O_EXCL + wipe（破坏性，五要素见下） | never-escapable typed-confirm | `offline.go:528-566` |
| **E** `init --from-manifest` re-seed | wiped brk2(daemon 停)：`init --from-manifest <manifest>`(pty) → "…complete — brk2 is now a single-voter cluster"；fresh raft/ 从 manifest identity seed。依赖 D 的 `--emit-manifest` | `cluster.go:745-751,782-783`；`wizard.go:70-71` |
| **F** `join approve --wait`（returning-node 入群） | brk2(clean single-voter) `join prepare`；brk1 `join approve <bundle> --wait`。**绝不以 op `"done"` + VOTER roster count 收口**（收敛视图；`init.go:236` 自名 "hollow voter" hazard）。收在**真 op**：经重收敛的 2-node clustered JS + brk2 在场跑 `session`/`expose`/`history` round-trip（或 JS meta 报 brk2 为 current peer + 真 workload 成功）；VOTER count 佐证。`--wait` 阻塞由 stdout 判别子证（"operation … reached …" `:187` vs 非-wait 的 "created (driving to SERVING) … watch:" `:154-158`）、非 timing。**Stage-B（SB-42F）**：regrow 到 N=1-standalone brk1 是 first-grow → #3/#10 mesh-render workaround；(a) 驱 mesh render 为 labeled `[env]`/`[workaround #3/#10]` 步（runbook 逐字，首选）vs (b) 复用 `cluster add` + targeted `--wait` sub-call | `cluster_join.go:143-160,167-174,186-187` |
| **G** `recovery resnapshot` happy path | clean single-voter、daemon 停：`TETHER_CONFIRM_NODE_ID=brk1 … resnapshot --self-id brk1 --raft-addr brk1:7400 --confirm-node-id brk1` → "resnapshot complete: brk1 is now grow-ready" | `cluster_offline.go:327-366`；`offline.go:142-231` |
| **H** resnapshot SINGLE-VOTER-only 拒 | 需 daemon-STOPPED **且** roster 仍 {brk1,brk2}（在 force-single prune **之前**跑；healthy N=2 里 daemon RUNNING → `ErrDaemonRunning`、非此消息）。`assert_refuses "SINGLE-VOTER only .* roster has .* non-self node"` | `offline.go:172-198` |
| **I** resnapshot `--accept-audit-loss` 两臂 | (i) unpublished-audit 在、flag ABSENT → `assert_refuses "would truncate .* UNPUBLISHED audit entr"`；(ii) + `--accept-audit-loss` → truncate-continue。**explore→pin（SB-42I）**：unpublished audit 不能确定性构造 → **NOT-COVERED-with-reason**（hermetic `offline_test.go`）；保 G+H | `offline.go:216-224` |
| **J** typed-confirm `--yes` 负例 | `assert_refuses "cannot run unattended\|NO --yes override"`（rejoin-prepare AND resnapshot 各一） | `cluster.go:967-975` |
| **K** machine-confirm-missing-env（仅 resnapshot）+ CONTRAST | resnapshot `--confirm-node-id brk1` 无 env、非-TTY → refuse；CONTRAST：rejoin-prepare 无 env 逃生（never-escapable `machineEscapable=false`）→ 因 TTY 理由拒。**分类已核（resnapshot/init `machineEscapable=true` `cluster_offline.go:354`；recover/rejoin-prepare `false` `:407`；never-escapable 集 `:504`）** | `cluster_offline.go:354,407,497-534` |

**臂 D — 五要素**：① brk2 daemon 停、非空 `tether.db` + raft/ 在（`test -s`/`test -d`） ② 盘上 dump 文件(0600) + raft/+tether.db 在场 ③ **以 ROOT 跑**（匹配 runbook `sudo`；`runuser -u tether` → `/root` EACCES）——`pty-confirm.py brk2 -- tether cluster recovery rejoin prepare --self-id brk2 --dump-divergent /root/div-brk2.json --emit-manifest /root/rejoin-brk2.json`，期 `warnRootDataDirOwner` WARN 作佐证 ④ dump 0600+非空 AND raft/+tether.db GONE AND manifest 0600 携 brk2 identity（`offline.go:549-566`；O_EXCL `:636-637`）。**O_EXCL 子臂**：镜像 `offline_test.go:190`——在 DB **完好**的节点上**预建** dump 文件、跑 rejoin-prepare、断 refuse + **raft/ 与 tether.db 仍在**（WIPE REFUSED）。签名 `"dump failed, WIPE REFUSED.*(file exists|exists)"`（**非** "must not pre-exist"=flag help）。成功 wipe 后**不**再跑（会在 `OpenReadOnly` "DB gone" 处先失败于 O_EXCL open） ⑤ fixture nuke。

---

### §2.4 drill `43-migrate-live-data`（N=1→cluster；GREEN body；承重 Stage-B spike）

**承重可行性事实**：无 `--auth-callout-seeds-dir` 的单 broker 跑 **P2 dev mode——无 NATS 层身份执法、但仍 SERVES**（`broker-ops.md:292`；install.sh standalone nats.conf 无 auth_callout 块 `:690-704`；`serve.go:204-216`）。这使 runbook §4 pre-migration "v2 SINGLE broker" = 一个服务真 session/expose/history ROW 的 P2 broker。

**SB-1（BLOCKING）**：无既有 drill 在 pre-`init` P2 broker 上建过 session/agent/expose。Stage-B 须证：`cmd_start_broker` 上，`session create --pin` + `agent join --pin`→ONLINE + `expose`→跨容器 curl-sentinel 全成？三出口驱 43 形态（§5-gated）：
- **(a) P2 serves**（likely）→ 全 A→G 复用 `cmd_session`/`cmd_agent_join`/`expose`。
- **(b) P2 仅带 labeled `[env]` auth_callout provisioning 才 serve**（铸 broker.nk/account.nk + 渲 single-mode `authorization.auth_callout` per `broker-ops.md §3.4`）→ distinct `[env]` 步（Mandate ③，忠实文档剧本）。**masking 边界**：(b) 严格作*文档化剧本 provisioning*；若把 "dev serves" 读法拉伸到 expose 且 P2 做不到，登 DOC/gotcha、绝不静默升级到 (b) 强行绿。
- **(c) P2 建不出服务行** → business-survival NOT-COVERED-in-sim；43 保 B/C/D/E-mechanical/G-on-minimal-DB。

**SETUP**：`up --brokers 1 --agents 1 --ctl 1` → `cmd_start_broker brk1`（P2 standalone，**非** `simcluster init`）→ 建 LIVE 行（session `lab --pin`、agt1 ONLINE、`expose_serve_sentinel agt1` + `expose --local` 记 port+token、≥1 `exec`）。baseline：`session ls`/`ps -a`/`dp_curl_body`(sentinel)/`history`。

| 臂 | oracle | 源 |
|---|---|---|
| **A** baseline LIVE 行 | 真读证行 + expose DATA PLANE 在 P2 broker 上活（migration 前） | — |
| **B** `init --check`/`--dry-run` 零改动 | read-only doctor preflight、不 mutate；**oracle = `.bak` ABSENCE + 字节等同**（exit 0 不足；short-circuit 于 `backupOnce init.go:210` 前） | `cluster.go:736-741,861-862` |
| **C** `init --yes` 拒 | `assert_refuses "cannot run unattended\|NO --yes override"` | `cluster.go:725,967-975` |
| **D** init machine-confirm-missing-env | 负：`--confirm-node-id brk1` 无 env、非-TTY → refuse。正例对照**不**单列——驱臂 E 的 cutover **非交互**（`--confirm-node-id` + `$TETHER_CONFIRM_NODE_ID`），它就是正例对照 | `cluster.go:772-779` |
| **E** `init --from-existing` cutover WITH LIVE DATA（非交互=D 的正例对照） | daemon 停 → `init --from-existing` → 打印 step-1 `reconcile nats --manual` → `nats-server -t` → restart → cluster seam → start cluster-mode → N=1 leader。Migration 0008-0013 + self VOTER + **home_broker backfill**（`init.go:302-304`）原地跑；pristine DB 在 `tether.db.bak`（O_EXCL 0600，`init.go:395`）。**Oracle = 臂 F**、非 init exit 0；另断 `tether.db.bak` 存在 0600 | `init.go:110-304`；runbook `:448-503` |
| **F** post-cutover SURVIVAL（真读） | auth_callout-ON broker 上：(1) `session ls` 列 lab；(2) agt1 重连 ONLINE（**explore→pin，#40**：pre-migration P2-connected agent 须在 auth_callout 下重连；需 re-provision 即 finding）；(3) **expose DATA PLANE 恢复**——`dp_curl_ok_body ctl1 http://brk1:<same-P>/… <sentinel>` 返回精确 sentinel（`grep -qF`；TCP-connect/4xx=假绿）；(4) `history` 显 exec 行。R-DATAPLANE | `init.go:302-304` |
| **G** ROLLBACK（restore `.bak` + de-cluster）— 五要素，`[operator per runbook §4]` | ① post-cutover live leader + `.bak` 在 ② `session ls`+`dp_curl` sentinel AFTER rollback + `DetectClusterMode` OFF ③ `systemctl stop tether-broker` → `cp tether.db.bak tether.db` + `rm -f *-wal *-shm` → restore `nats.conf.bak.<ts>` → 移 `broker.cluster.*` from broker.yaml → start（各 `[operator per runbook §4]`） ④ P2 standalone + 行完好 ⑤ nuke | runbook `:505-508`；`init.go:379-395` |

---

### §2.5 drill `22-forcesingle-online`（N=2；explore→pin, DEFAULT EXPOSE — 最难的 drill）

**机制（主进程已核验源码）**：online force-single = arm→confirm→commit 经本地 root admin socket（`cluster_offline.go:220` `--online` flag "the preferred path"；`force_single_online.go` 实现；in-process `RecoverToSelfOnline` 不停进程）。四门，arm 时按序评（`fsArmVerdict`），commit 时复检：

| 门 | code | reason 串 | 源 |
|---|---|---|---|
| dwell — 尚有 leader 联系 | `quorum_not_lost` | `node still has leader contact` | `force_single_online.go:170` |
| dwell — 15s 窗内 | `quorum_not_lost` | `not yet quorum-lost … wait <remaining>` + `DwellRemaining` set | `:173-174` |
| peer-liveness 硬拒 | `peer_alive` | `HARD-REFUSE — peer %q accepted a TCP connection on its %s port … ALIVE, force-single would split-brain` | `offline.go:276` |
| arm-token 单用/TTL | `arm_expired` | `no fresh arm token …` | `:237-239` |

dwell **in-memory、15s-continuous**（`leaderlessSince`），由 5s 非-leader-gated observe tick 喂；fresh arm token 每 broker start 铸 → **任何 restart 把 `leaderlessSince` 归零**。commit 真 in-process（`RecoverToSelfOnline` inmem transport、无网络 IO、`:7400` listener 保绑 → **MainPID 保留**）；持 `force_single_active` + recovery epoch，且 **R3** 不 de-cluster nats.conf（offline-only），打印 `reconcile nats --to-standalone` remedy。

**Dwell-bounce（OQ-4）— DEFAULT DISPOSITION = EXPOSE（§11-U2）。** repo 已记 bounce：`12-ghost-voter.sh:6` / `20-forcesingle-natsconf.sh:8-10` 记 online-FS dwell 被 #23 `Restart=always` bounce 重置（这正是二者用 OFFLINE 的原因），且 `cmd_force_single` 的 online 路径（`simcluster:402-409` `die`s "dwell gate never satisfied within 60s"）**至今无 drill 驱动**——22 是首次真跑一条 repo 已标不可达的路径。重置 dwell 的 crash-loop 是真产品行为（boot `b.js==nil` EJECTED/TRANSIENT 微分 `broker.go:941-961` → 非-`context.Canceled` return → 非零 exit `serve.go:239` → `Restart=always`/`RestartSec=2` `install.sh:722/754`），但 **首次 restart 触发器非源码可判**（js_health detection-only；`LeaderContactStale` 从不 brick；MaxReconnects(-1)；仅 nats-loss 上模糊 clean-exit(0)）→ 杀 peer 是否触发 survivor 首次 restart = Stage-B 实测定夺。

**拓扑**：N=2（最小 quorum-loss）。**pin brk1 = raft LEADER pre-kill**（`cluster transfer-leader brk1` + assert），因 PROT control-source write 与 POS 的 `RecoverToSelfOnline` 都要 brk1 是 surviving leader（`node.ProposeWithReqID` 在 `state != Leader → ErrNotLeader` node.go:401-404 于跑 plan **之前** gate）。所有 sampler 在容器内（R-NO-HOST-LEAK）。

**共享-fixture 增量**（roadmap 522-524；12/20/22 共享）：给 `setup_forcesingle_n2` 加一条杀前 OFFLINE peer-alive 拒——两 peer 皆活时 survivor 上 stop→refuse→start（daemon-lock 互锁先触 `RaftStoreLockedByDaemon→ErrDaemonRunning`）：`systemctl stop tether-broker` on brk1 → pty `recovery force-single … --confirm-peers-dead brk2`(brk2 活) → `assert_refuses "ALIVE, force-single would split-brain\|accepted a TCP connection on its .* port"` → `systemctl start`（干净 rejoin）。**回归门**：落地前重跑 12 + 20 GREEN。

**ARMS（顺序承重）：**

*杀前（healthy，每次可达）：*
- **DRY** `--online --dry-run` → OK、`WouldProceed=false`、`Reason="node still has leader contact"`、stdout `dry-run: gates evaluated, NO changes made.`；**零-mutation oracle** = MainPID 不变 + 无 `force_single_active` + 无 token 铸。
- **GATE-a** healthy REAL arm → `assert_refuses "quorum_not_lost"` on `node still has leader contact`。FG 守卫拒 `peer_alive`/`arm_expired`/socket-down（wrong-reason HARD FAIL）。
- **YES-off** offline `--yes` → `assert_refuses "cannot run unattended.*NO --yes override"`（稳健 always-reachable (e) 锚；offline 无 dwell 门）。
- **PALIVE-full** 共享-fixture 杀前 offline peer-alive 拒（上）。

*Arm 0 — DIAGNOSIS SPIKE（破坏性，REAL unit 绝不 mask，分支决定者）。五要素*：① baseline healthy N=2、记 MainPID_0 + NRestarts_0、DRY 证 leader 联系 ② `systemctl show -p MainPID -p NRestarts -p ActiveEnterTimestampMonotonic` + `broker.err` 首次退出 + DRY `WouldProceed` ③ `node_kill brk2`，`Restart=always` INTACT（mask=Mandate ① 违反）；`tcp_refused brk2 7400` ④ ≥60s 内：NRestarts 增否？MainPID 变否？有 ≥30s 连续 uptime 否？DRY 达 `WouldProceed=true` 否？分类 `broker.err` 首退 ⑤ nuke。

**TOTAL-FUNCTION 分支（无假绿隐藏区）：**
- **POSITIVE** iff NRestarts==0 ∧ MainPID 稳 ∧ DRY 达 WouldProceed=true → 跑正路径。
- **GOTCHA-production-real** iff NRestarts 增 AND DRY 从不 WouldProceed=true AND `broker.err` 显 startup JS-unavailable crash-loop 微分 → **#35 assert_bug RED**（production-real；生产 survivor 因任何原因 restart 且 dead peer 仍在其 raft config → 撞同一不可达-dwell 死锁）。
- **GOTCHA-container-tamed→POSITIVE-with-fidelity-caveat** iff NRestarts>0 但 DRY 最终 WouldProceed=true AND 首退是可证容器 artifact → harness 驯服 per OQ-4（杀→poll-wait 级联平息→加宽 dwell poll），labeled harness accommodation + production-fidelity NOT-COVERED 注，落正臂。
- **任何未分类观测 → INCONCLUSIVE/RED**，绝不静默 POSITIVE。**APPEARS-FIXED 守卫**：bounce 消失则 re-triage 为 POSITIVE、绝不过 #35 RED。

*杀后窗（两分支）：*
- **GATE-b** within-dwell REAL arm → `assert_refuses "quorum_not_lost"` **AND `DwellRemaining` 非空**（判别 Phase 2 vs Phase 1，后者返同 code 但 DwellRemaining 空）。加 socket-reachability 前提（`cluster status` 应答）——socket-unreachable 当 "phase unobservable this cycle"、非拒绝（bounce 支里 `callAdmin` unreachable 打 "broker admin socket unreachable"=wrong-reason）。
- **PROT** protected-mode：quorum-lost routine op 拒。机制：set-raft-addr → `Propose(PlanClusterNodeReaddr)` + `AddVoter` 直调（`clusterdrain.go:517,525`）；retire → `RemoveServer` 直调（`clusterdrain.go:158`）；均 surface raw wrapped `raft.ErrNotLeader`（node.go:403）。**R-CONTROLSRC 三元**：control = 杀前 `set-raft-addr brk1:7400` on LEADER SUCCEEDS（需 brk1=leader，§拓扑）；anti-vacuous liveness = brk1 admin socket 在 quorum loss 期答 `cluster status`（*Stage-B 确认；支撑 `read.go:17,23`*）；victim = quorum-lost retire/set-raft-addr 拒 ∧ 未 commit。

*仅 POSITIVE 分支：*
- **POS** 正路径（五要素）：③ pty 喂 `recovery force-single --online --self-id brk1 --confirm-peers-dead brk2` ④ **PRIMARY control-source oracle**：先前被拒的 PROT `set-raft-addr` 现 **SUCCEEDS**（raft writable at quorum-of-1）。KEY in-process 判别子：MainPID_post == MainPID_pre（FG：变了=restart→HARD FAIL）。佐证：`cluster status` exit 3 + `force_single_active` banner；stdout "…writable WITHOUT a broker restart"。**R3**：nats.conf 仍 clustered（`grep -qE '^cluster'` 仍匹配）+ stdout 携 `reconcile nats --to-standalone` remedy。**显式绝不断 tier-B 恢复**（数据面留 503 直到 operator `--to-standalone`=92(b)/offline scope；断此=over-reach 假绿）。
- **GATE-c-online** peer-alive online → `assert_refuses "peer_alive"`（分区 brk2 raft/tunnel 但留 nats up）。timing-fragile；不可靠 stage 则**折进 branch-独立的 OFFLINE nats-port 变体**（§11-U2）。
- **GATE-d** arm-token TTL-expired → `arm_expired`。CLI 每 run 铸 fresh token → 纯 replay 非 CLI-reachable。**采 (d-ii) NOT-COVERED-in-sim**（单用/TTL 负例 hermetic `force_single_online_test.go:36-61`；POS 已 round-trip token on 真 socket）（§11-U2）。
- **YES-online** FINDING：online `--yes` **非** Tier-2-rejected——`runForceSingleOnline` 在 `cluster_offline.go:155-158` 早返、绕过 `:165` rejector，故 `--online --yes` 非-TTY 因 TTY-required 消息拒（`:523-525`）、**非** offline `NO --yes override` 串。`assert_refuses "requires an interactive terminal.*no --yes"` + labeled 注 → **#36**（DOC vs gotcha，低 sev；TTY 保护完好）。这使 inventory row 180 对 force-single 的 blanket claim 失效 → 拆行（offline=Tier-2；online=TTY-only）。

*GOTCHA 分支：*
- **GOTCHA** `assert_bug "#35 online force-single dwell structurally unreachable …"` 由 CONJUNCTION 钉（NRestarts 在 [t_kill, t_kill+dwell+margin] 内增 ∧ DRY 从不 WouldProceed=true ∧ `broker.err` startup-JS-crash-loop）。FLIP：durable dwell OR quorum-loss 上 dead-peer raft-config prune。

---

## §3 — S8 drills

### §3.1 drill `90-alerts-lifecycle`（N=3 — 主进程采 C1 更正；GREEN）

**C1 更正（主进程已核验源码）**：roadmap 写 90 为 **N=2**，但 `broker_down:<v>` 是 store-backed raft-written 告警（leader-gated observe loop `observability.go` → `node.Propose`）。N=2 杀任一 voter 掉 committable quorum → survivor 失 leadership → observe loop no-op → 告警**永不写**。**只有 N≥3 能 raise 它**。故 90 = N=3（杀 follower 留 2/3=quorum；不触 `below_quorum`，后者仅 `nv==2` 触 `alert_reconcile.go:253`）。

**C2（severity，塑 oracle）**：`broker_down`/`disk_pressure` 是 **INFO**（`observability.go:208` 硬编码 `AlertSeverityInfo`；`alert_reconcile.go:256`），`renderBanner` 只 filter `severe`（`d8_alerts.go:132-145`）→ 二者只在 `alert ls` 现、**从不**在 ps/node banner。banner 只由 `manual --severity severe`（或 `replication_degraded`、唯一 severe reconciler kind——S5-30）可证。

**SETUP**：`grow_to_3 1 1`（retry=1；90 非 #31-pinning drill）→ session lab + agt1。`LEADER=$(sim_leader)`；两 distinct follower `FOLL_DISK`(回 VOTER) 与 `FOLL_DOWN`(留 down 到 cleanup)。**Clean-baseline 门**：`alert ls --json` 显无 active `broker_down`、无 `manual`（任何臂前）。

| 臂 | oracle / 签名 | 源 |
|---|---|---|
| **M1** raise | `dexec $LEADER -- tether alert raise --kind manual --severity severe --message "SENTINEL90_$$"` → member `alert ls --json` 见 dedup_key `manual`、severity `severe`、message 含 `SENTINEL90_$$`。FG：per-run sentinel | `alert.go:54-78`；reader `internal/broker/cluster_health.go:133-155` |
| **M2** severe banner on ps/node ls **stderr**，stdout parseable | `sim ctl -- ps 1>out 2>err`：`err` 有 `‼ ALERT [manual] …SENTINEL90_`，`out` 干净 parseable（无 `‼`）；`--json` → banner 两流皆无。FG：`‼` 绝不在 stdout | `ps.go:128,165`；`d8_alerts.go:135-145` |
| **M3** ack = store-backed team-ack | `alert ack manual` → committed `acked_by`（fresh re-read `.acked_by\|length>0`）。**FG(微妙)**：ack 后 severe banner **仍** render on `ps` stderr（ack 只抑制 inline prompt；只 `clear` 移除——断 "ack 后 banner 消失" 测假承诺） | `alert.go:200-206`；`d8_alerts.go:168-170` |
| **M4** clear idempotent | `alert clear manual` → "cleared … idempotent"；`alert ls` 不再列 manual；二次 clear → exit 0；post-clear `ps` stderr 无 `‼ ALERT [manual]` | `alert.go:109`；`alertadmin.go:90-109` |
| **M5** kill follower → `broker_down`（五要素） | ① clean-baseline + `$FOLL_DOWN` live VOTER；baseline liveness 用 `reachable:true` 且 `ReachSource=="nats-health"`（`broker_down` keys on 的同信号 `:50-56`）、非 raft 口 ② member `alert ls --json` ③ `node_kill $FOLL_DOWN`；证死 ④ poll ≤30s：`alert ls` 显 `kind=="broker_down"`、`dedup_key=="broker_down:$FOLL_DOWN"`、`severity=="info"`；佐证 leader `status` `reachable:false` ⑤ `node_start` → poll VOTER + `broker_down` clears；nuke。`broker_down_rehome_summary` 配对 → **NOT-COVERED**（raw event，无 reader） | `observability.go:206-213`；reader `cluster_health.go:142-146` |
| **M6** fill JS store → `disk_pressure`（五要素） | ① capped store <80% + 无 `disk_pressure:$FOLL_DISK` ② `alert ls` ③ ballast-dd（或 drill-21 near-ceiling push）过 0.80，THEN **bounce `$FOLL_DISK`**（`[env]` accelerant——告警 5min 内自触，bounce 只 re-sample now） ④ poll ≤30s：`disk_pressure:$FOLL_DISK` info；FG：dedup_key node-scoped ⑤ 移 ballast → clears；nuke。**M-7 注**：bounce 触 **startup tick**（`disk.go:102`）、非 periodic ticker → 加一条 no-bounce 腿等 interval 证 periodic 路径自触（budget 允 ~6-9min），或 scope 到 startup-tick surfacing（periodic hermetic-only）→ 这是 **#39** 的真主题 | `disk.go:23-24,73-93,102` |
| **M7** `alert ack quorum_lost` interpret-REFUSED（B3） | pre-NATS-dial arg short-circuit → `assert_refuses "LIVE cluster-health condition\|NOT a store-backed alert\|cannot ack it"`（无需真 quorum loss）；repeat for `force_single_active`；断 `--ack-alerts` disambiguation 行在。FG：精确 live-condition 签名 | `alert.go:179-184` |

**`below_quorum`/`raft_lag`（inventory SSOT row 79）→ signature-guarded NOT-COVERED-with-reason**（§11-U5）：`below_quorum` 需 `nv==2`（=一次 retire-to-N=2、破坏性、#31-gated 且与 N=3 broker_down 冲突）；`raft_lag` 需 follower 滞后 >64 entry（不可确定性诱发）。

---

### §3.2 drill `91-client-converge`（N=1→2→3；GREEN body + candidate；臂间隔离，D 先于破坏性 C）

**承重模板约束（已核）**：`DeriveSeedEndpoints` 对空/仅-`nats://` `prev` 返 `nil`（`seed_converge.go:29-33,44-57`——拒 auto-fan-out cleartext-PIN `nats://` seed）。故 A1 首 publish 必用 `tls://`/`wss://` endpoint、否则 change-gated auto-converge 永久 no-op。A2 oracle 是 `seeds show` 内容（非真 dial），故 `tls://<brk>:443` 忠实。

**SETUP**：`grow_to_3` 增量建（A2 观测每 grow）；session `lab --pin`；1 agent；ctl。

**Arm A — change-gated auto-publish（无手动 publish）：**

| 臂 | oracle | 源 |
|---|---|---|
| **A1** 首 publish（唯一允许的手动） | `cluster seeds publish --bootstrap <https-manifest> --endpoint tls://brk1:443 --sid lab` → `seeds published (seed_generation=1)` + 铸 invite；`seeds show` 恰列 `tls://brk1:443` | `cluster_seeds.go:38-70` |
| **A2** grow → auto-include（无 publish） | `grow brk2`, `grow brk3`；poll `seeds show` 至 endpoints == {tls://brk1/brk2/brk3:443} 且 seed_generation 严格越过 1。FG：gen 严格增（stale 读留 gen=1）；新 host roster-derived、非重打 | `seed_converge.go:39-96,160-201` |
| **A3** retire → 死端点 auto-disappear（无 publish） | **retire 可能因 #31 从不 commit**：要求 A3 收敛在 grow-lock **先可靠清**（labeled `[GAP #31]` re-run-`cluster add`，per §2.1）的拓扑上，且**断 retire 真 commit**（roster 行消失）BEFORE 断 seed-drop。若 Stage-B 发现 #31 不能可靠清 → A3 的 G3-retire-converge **自身 NOT-COVERED-with-reason**（不藏在 #31 label 后）。drop-only oracle：仅 brk3 掉 | `clusterdrain.go:174,262,336`；`seed_converge.go:210-215` |

**Arm D — cli broker auto-failover + #17 roster refresh via NON-floor survivor（healthy N=2/N=3 上、破坏性 C 之前跑）。五要素**：① 冷-`cluster pin` fresh ctl home 仅 **floor**——source from `cluster invite --bootstrap` **无** `--seed`（A1 的 `MintInvite` 总 stamp `Seed: endpoints[0]`，故 A1 invite 非 seedless），断 `jq '.invite_seeds==null'` on fresh cache before kill。`node ls` #1 经 floor 连、`refreshCtlEndpoints` 填 `ce.Roster`（`jq .roster_gen > 0`） ② ctl-container `cluster_endpoints.json` jq + `node ls` 真 reach + `tcp_refused` ③ `node_kill <floor>`；证 `tcp_refused floor 4222` ④ `node ls` #2 成功(≥1 broker) 而 `broker_url == 死 floor` → 只经非-floor survivor 成功（`DialFor` `nats://` roster survivors、floor-last `cluster_endpoints.go:176-192`）；R-DATAPLANE。FG：`broker_url` 必 == 被杀 floor ⑤ `node_start` floor；nuke。**#17 收敛子-oracle**：10-min `ctlRefreshTTL` 意味 `node ls` #2 用 cached roster；证从 survivor refresh（roster_gen 在 floor 死时前进）用 **fresh-ctl 变体**（第二 fresh ctl pin floor + 一 `--seed`=survivor、floor 已死 → 立即 refresh）——推荐、无 cache 戳。

**Arm C — offline force-single → seeds 收敛 survivor-ONLY + trust-anchor 负例（LAST，独立重建 fixture）：**
- **C-seeds**（五要素）：① N=2、`seeds show` 列两、JS FORMED ② brk1 socket ③ `docker kill brk2` + OFFLINE `recovery force-single --confirm-peers-dead brk2`(pty) + restart ④ poll 至 endpoints == {tls://brk1:443} only（纯 drop）。**触发诚实（已核）**：收敛触 post-restart leadership-startup backstop（`observability.go:253`→`clusteradmin.go:290,356`）或 ghost `recovery node remove` 尾——**皆非手动 publish**（G3 精髓）。若 survivor-only 收敛需显式 `recovery node remove`（single-voter 持 leadership、无 edge），该 node-remove 是 labeled 合法清理 verb、**非**手动 publish——记录哪个触发 ⑤ nuke。
- **C-anchor**（wrong-anchor roster 拒、ctl cache 不被毒；五要素）：**10-min TTL throttle 必须 un-throttle 否则 refresh 从不触**——加同 91-D 的 un-throttle（fresh ctl 空 `FetchedAt`），④ `fetched_at`-advanced 作 HARD pre-oracle（throttle-swallowed run 读成 arm-没跑、非 pass）。**默认走 torn-anchor**：serve A 真签 manifest 的 byte-flipped 副本于 `bootstrap_url` → `VerifyAt` 破签失败 → `AdoptDecision(pin=A)` reject → cache `roster_gen`/`roster` 不变==R0 + ctl 仍 dial A。忠实 **foreign-cluster-B** 变体（第二 `init`'d broker + 第二 ingress vhost）是 **Stage-B spike，contingent on 运行时 ingress multi-vhost**（SSH-down ⇒ 未证，§11-U2）、非默认。"online force-single 留 nats.conf clustered(R3)" 引 **product source** `force_single_online.go`（prune roster + 收敛 seeds、从不碰 nats.conf）、非 sibling drill 注。

---

### §3.3 drill `92-js503-remote-alert`（N=2；G7b；两 leg 拆；leg-b soft-depends on 22）

**Headline finding（主进程已核验源码）— 92(a) 是 MISFRAME：N=2-kill-peer 不达 JS-503 banner。** sustained-503 信号 leader-observed：`ReconcileAlertsOnce` 在 `!IsLeader() || LeaderContactStale(now)` 时早返丢信号（`alert_reconcile.go:176-183`）；需 60s 持续 leader 观测（`jsDownThreshold=60s :78`）。N=2 quorum loss 里 survivor 下台（`MultinodeLeaderLeaseTimeout=500ms`）→ 既非 `IsLeader()` 又非 `!LeaderContactStale` → reconciler 关 → `JetStreamUnavailable` 留 false → `--remote` 无 banner。**正确 frame**：N=2 gap 是 survivor **根本不是 leader**（非 voters:1）。

**SETUP**：`setup_forcesingle_n2`（JS FORMED 守卫）。ctl 登入 `lab`。

- **BASE**（FIRST）healthy `--remote` → 无 `DATA-PLANE DEGRADED` + writable-leader verdict + exit 0。
- **Leg (a) — 22-独立（N=2 kill peer，quorum loss）：**
  - **(a-GREEN，reachable)**：poll `--remote` → **exit 2 + "no writable leader … READ-ONLY"**（`cluster_status_nats.go:116-117,136`；`EvalDestructiveGate.QuorumLost`）。再 `session rm <sid>`(无 flag) → `assert_refuses "BLOCKED"` + `"--ack-alerts"` + `"quorum_lost"`；`session rm <sid> --ack-alerts` → 断命令**越过** advisory gate via downstream 判别子（broker 的 quorum-unable write rejection，区别于 "BLOCKED" gate 文本）——裸 absence-of-"BLOCKED" 若命令在 gate 前先错则假绿。断 **gate-bypassed**、非 command-success。R-CONTROLSRC：survivor 答 `LeaderContactStale=true, WritableLeaderConfirmed=false`（非 "all failed"）。
  - **(a-EXPLORE→PIN — JS-503 banner)**：预测 `DATA-PLANE DEGRADED` ABSENT。**PRIMARY DISPOSITION = by-design NOT-COVERED、非 #35**（§11-U3）：JS-503 banner 针对 *control-plane-healthy / data-wedged* 情形（`cluster_status_nats.go:162-168`，racknerd force-single-clustered rot）；自然 N=2 quorum loss 是**反面**（control plane 真死、已由 exit-2 READ-ONLY surface）——此处 banner 冗余、非缺信号。为 correct-by-design 行为登 assert_bug RED = Mandate 反转。**仅当 OQ-D4-2 retire-to-N=1 spike 显真正 control-healthy 单 leader 仍不 banner 才保 #41**（§4 RESERVED）。
- **Leg (b) — #20③ 专属 leg（SOFT-DEPENDS on 22）：**
  - 若 22 判 online force-single **production-UNREACHABLE** → leg (b) 登 **NOT-COVERED-in-sim**（banner 需合法 single-voter leader + clustered conf，sim 中只经 online force-single 可达；banner 逻辑 hermetic `g7_js_health_test.go`）。**不阻塞 leg (a)。** 91-B（seeds 收敛 survivor-only）也 ride 22 → 也 NOT-COVERED then（不宣称 91 的 G3-B clean）。
  - 若 22 驱通 online force-single → 破坏臂：online force-single on brk1（conf 留 clustered，R3）→ poll `--remote` 至 **`DATA-PLANE DEGRADED — … JetStream UNAVAILABLE (sustained 503)`** + `reconcile nats --to-standalone` remedy + **exit 3**（`ForceSingleActive→3`），≥60s sustained（poll ≥120s tolerant，OQ-7）；佐证 JS 503 via 数据面探针失败。Recovery：operator `reconcile nats --to-standalone --confirm-single` `[operator per runbook §2.2]` + JS reset + restart → `--remote` DATA-PLANE 行 clear；**终点 oracle = tier-B push 又通**（R-DATAPLANE）。

**OQ-D4-2（Stage-B，可能 mooting）**：一个 **retire-to-N=1** survivor 是合法 1/1 leader（quorum=1、从不 demote）带仍-clustered conf → sustained JS-503 → banner FIRES（exit 0 + DATA-PLANE-DEGRADED）——这可能是 roadmap 想要的真 22-独立 floor，把 G7b-banner 从 22 依赖 de-risk。确认 retire-to-N=1 留 conf clustered 且 JS meta 返 503。

**Remote 命令面**：`--homes` PROXY_HOMES 表；`seeds show --remote`（无 bundle → exit 69）；`--remote`+`--offline`/`--watch` 互斥 → `assert_refuses`。

---

### §3.4 drill `93-metrics-observability`（N=3；GREEN）

**SETUP**：`grow_to_3 1 1` → session lab + ctl。**Obs-enable `[env]` 步**：`obs_enable`（§1.2，**`observability:` 键 2-space 缩进**）每 broker + rolling restart（poll VOTER、保 quorum）。**webhook 接收器**运行时投递进 ctl 容器（§0.3）。**pin leader 稳定跨 raise→capture 窗**（或 pin leader）——`fireWebhookDelta` 只在首次 leader pass seeds；leader 在 raise 与 capture 间移动会把 `raised` POST 掉进 baseline。

| 臂 | oracle / 签名 | 源 |
|---|---|---|
| **MET** `/metrics` 真值（loopback dexec-curl） | `$LEADER`：`cluster_mode 1`、`is_leader 1`、`voters 3`、`quorum_margin 1`（`ProjectQuorum(v,false).FaultTolerance`）、`peer_applied_lag{node=…}` + `peer_reachable{node=…} 1` per peer。`$FOLL`：`is_leader 0` + 断**无行匹配 `^tether_broker_peer_applied_lag{node=`**（HELP/TYPE 头行**无条件**发 `metrics.go:81,85`；只 `{node=…}` data 行 leader-gated——允头、禁数据行）。FG：follower 有 `cluster_mode 1`+`voters 3`(真 broker) 但无 peer data 行。POST → 405 | `metrics_wire.go:16-67`；`metrics.go:56-89` |
| **MET-degraded**（五要素；kill `$FOLL`） | `quorum_margin`/`peer_reachable` 读 leader 的 **cached** observe tick（`lastObserve`、5s+2s 刷）→ `poll_until quorum_margin==0`（budget ≥10s；即时 scrape 读 stale margin=1）。CARD/EXIT 读 LIVE `StatusReport` scatter-gather(~2s) → 与 MET 的 ~7s-lagged cache **非**同时（注两钟）。**DROP "一注入服务三"** | `metrics_wire.go:33-45`；`observability.go:219-220` |
| **HEALTH** `/healthz` 200；`/readyz` 200 serving VOTER | 常量 200 `ok`；`/readyz` 200 `ready: leader=… self=VOTER` | `metrics.go:146-165`；`metrics_wire.go:75-107` |
| **READYZ-503** 非-serving → 503（explore→pin） | primary：joiner 跨 grow-boundary 窗（`simcluster:188-216`）→ `/readyz` 503 命名非-VOTER phase。triage：(a) 非缺陷（hermetic `b7_readyz_test.go`）→ NOT-COVERED-in-sim（timing）for CATCHING_UP cell、用稳定非-VOTER phase 钉 removal-property（`cluster drain <foll>` → DRAINING → 503 `phase=DRAINING`——*Stage-B 确认 DRAINING dwell 是否够久 vs 径直 RETIRING，与 S6-40 协调*）。FG：503 body 命名非-VOTER phase（排 `no raft leader` 503 / connection-refused）+ 同 run 一个 sibling VOTER 答 200 | `metrics_wire.go:81-83,89-91` |
| **WEBHOOK** alert transition POST captured、no-secret（五要素） | ① 接收器 up、`/tmp/hook.log` 空、无 `manual` ② ctl-container capture 文件 ③ `dexec $LEADER -- tether alert raise --kind manual --severity severe --message "HOOK90_$$"` ④ poll ≤10s：JSON 行 `transition:"raised"`、dedup_key `manual`、kind/severity、message 含 `HOOK90_`、`cluster_leader`；再 `alert clear manual` → `transition:"cleared"`。**NO-SECRET 白名单（精确键）**：`["cluster_leader","dedup_key","kind","message","schema","schema_version","severity","transition","ts"]`(+`node` for node-scoped)——多键=RED；+ `grep -vq` PIN/nkey/seed ⑤ `pkill` 接收器 in-container；`alert clear`；nuke | `alert_webhook.go:124-139`；`alert_reconcile.go:116-141` |
| **WEBHOOK-URL-neg**（五要素） | poster 只在 cluster mode wired（`wireClusterLate` clusterwrite.go:320-330；single-mode `serve` 从不 parse URL → **绝不经 single-mode serve 断**）——经 cluster-startup：① unit healthy N=3 ② 注坏 `broker.yaml`（userinfo `http://u:p@h/` 或非-http scheme）+ restart 一 broker ③ oracle：`systemctl is-failed` AND journal 携 `parseWebhookURL` 串（`alert webhook url … not allowed`/`must not contain userinfo` `alert_webhook.go:57-64`） ④ 恢复 + restart + repoll VOTER。private-IP/metadata blocking by-design absent（`:26`）→ NOT-COVERED（security-pragmatic） | `alert_webhook.go:57-65`；`clusterwrite.go:320-330` |
| **CARD** `--card` glance；JSON 镜像 | healthy：`CLUSTER HEALTHY (HA)` + `HEALTHY_HA (exit 0)`。Degraded（复用 MET kill）：`what's wrong`/`what to do` + `DEGRADED-WRITABLE: voter <foll> unreachable`。**同-report 镜像**（非 cross-time byte-diff——index 会前进）：断 card 的 `Health`/`(exit N)` == JSON 的 `.health`/`.exit_code`（card 不算新 verdict，`cluster_status_card.go:11-15`） | `cluster_status_card.go:19-114` |
| **WATCH** smoke | `cluster status --watch 2s` in-container、~5s 内捕 ≥2 repaint、干净 SIGINT；`--watch --offline`/`--watch --remote` → usage error | `cluster.go:146-149,175-176` |
| **EXIT** taxonomy（B2） | online healthy → 0/`HEALTHY_HA`；MET kill 后 → 1/`DEGRADED`；offline（全 broker 停、`:7400` 死——`:7400` 属 tether-broker、非 nats-server）→ 2/`ROSTER_UNREACHABLE`。**B2 判别子**：`DEGRADED`(1) ≠ `ROSTER_UNREACHABLE`(2) 精确串（两断）。exit-2 QUORUM_LOST / exit-3 FORCE_SINGLE → cross-ref NOT-COVERED-here（S6-22/12/20 + hermetic） | `clusterstatus.go:68-81,467-470` |
| **LOGJSON** | `log_json:true` 下 tail journal、断 broker-emitted 行 parse 为 JSON 带 `.msg`/`time`/`level`。FG：broker structured slog 行、非 systemd/nats 前缀 | `logging.go:14`；`serve.go:278,96-98` |

**inventory gap（D6 M2）**：`cluster upgrade --notify-webhook`（`cluster_upgrade.go:158`；+ `cluster add --notify-webhook` `cluster_add.go:126`）由 s3-s5-plan:376 继承给 S8-93。它与 `alert_webhook_url` 是**不同** webhook。因真 upgrade-roll 受 #31-blocked → §7 记 **93 若可构造 upgrade/grow-milestone POST 则消费，否则 NOT-COVERED-with-reason tied to #31**（§11-U8）。

---

## §4 — Gotcha ledger（#35+；现有 max = #34，6 lens 皆核）

**主进程 ratify 的编号（§11-U1）：**

| # | drill | status | 机制（file:line） | flip |
|---|---|---|---|---|
| **#35** | 22 | **CANDIDATE，外审 round-3 M5 降级为「未在 sim 复现」**；PRODUCT-RED only if 22 Arm 证 PROVEN survivor-restart(MainPID 变)+dwell-never-satisfied，否则该 run 记 INCOMPLETE | in-memory `leaderlessSince`(`force_single_online.go:41`) 每 `newForceSingleArm` 归零 + startup-JS-crash-loop(`broker.go:941-961`→`serve.go:239`→`install.sh:754`) 阻止 ≥25-30s 连续 uptime。**根因 = #23 restart-bounce 族**（cross-ref；manifestation-pin，见 §11-U1） | durable dwell OR quorum-loss 上 dead-peer raft-config prune |
| **#36** | 22 | CANDIDATE(DOC vs gotcha，低 sev) | online `--yes` 静默接受、非 Tier-2-rejected（`cluster_offline.go:155-158` 早返绕过 `:165` rejector）；TTY 保护完好 | route online `--yes` 经 Tier-2 rejector |
| **#37** | 40 | CANDIDATE, spike-decided, **default GREEN** | mid-retire leader-kill resume 不收敛（wedge/FK/orphan）——无当前代码证据（controller 声称 clean re-derive `:287-293`） | resume 收敛 → GREEN |
| **#38** | 41 | CANDIDATE, spike-decided, **default GREEN** | reconciler 在 de-cluster 后 restart 重加 `cluster{}`（R3）——无代码证据（`cluster_reconcile.go:80-132`） | 保持 standalone → GREEN |
| **#39** | 90 | CANDIDATE(gotcha vs by-design vs DOC) | `disk_pressure` 延迟不可配：5-min 固定 monitor interval、无 broker.yaml/flag 旋钮（`disk.go:23-24`；`serve.go:175-201` 从不 wire 它） | 加 `disk_check_interval` 旋钮 |
| **#40** | 43 | CANDIDATE, spike-decided, **default GREEN** | P2-built agent 在 `init --from-existing` cutover(auth_callout 上)后 fails to reconnect ONLINE + re-serve expose | migration re-provision pre-existing agent |
| **#41** | 92(a) | RESERVED, contingent on OQ-D4-2 | JS-503 banner 在真正 control-healthy retire-to-N=1 单 leader 上不 fire（**仅此**才真缺陷；N=2 non-firing by-design） | banner fire on N=1 leader |
| **#42** | 92(a) | RATIFIED（Stage-C M1 订正为**有界 ~TFence(10s) 窗口**，非永久） | quorum-loss 后 ~10s 内 `--remote` 误报 transient + `session rm` 栽 raw store_error；TFence 后自我纠正（`cluster_status_nats.go:116-136`）。**原 #43/#44「永久」断法是误分类 RED、已废并折入本条**（`--remote` remedy 与 banner 同受 `JetStreamUnavailable` gate） | 窗口内即给 quorum-lost verdict |
| **#45** | 40/41 | REGISTERED（Stage-C 41-M15，PRODUCT-RED on repro） | retire op 卡 `NATS_ROLLED_OUT`、永不达 terminal RETIRED，下一 retire 被 `already in flight` 拒（`cluster_operation_controller.go:33`）。**独立于 #31（START 前 grow-lock 拒）与 #37（mid-retire-resume）**——此前误标 "#37-family"，round-2 M6/round-3 M6 给独立号 #45 | retire 在 rehome/migrate 完成后可靠推进到 RETIRED |
| **#46** | 91 | CANDIDATE（91-A2，PRODUCT-RED on repro） | seeds change-gated auto-publish 纳入第 2 broker 但**漏第 3 voter**：brk3 达 VOTER 却从不进 `seeds show` endpoints（`seed_converge.go` 根因待定，SB-91）；G3 client-converge 债的 manifestation | 第 3+ endpoint 也被 derive |

**复用（不重编号）**：**#29**（expose home/crash-strand——40 defer 数据面到 71）；**#31**（grow-lock leak——blast-radius 扩 +retire(40)+shrink(41)+91-A3-retire；在 40/41/91 头等钉）；**#23**（restart-bounce 族——**#35 是其 manifestation，非另立根因**，§11-U1）。

**编号 reconciliation（外审 round-2 M6 / round-3 M6，跨文档零漂移）**：#37=40-mid-retire-resume（candidate default-GREEN，**非** stall）；#38=41-R3-reconciler-recluster（candidate default-GREEN，**非** stall）；**#45=retire NATS_ROLLED_OUT 收敛停滞**（独立号，此前 ledger 误标 "#37-family"）；#42=有界 `--remote` 窗口（#43/#44 折入）；#46=91 seeds 漏第 3 voter。ledger `docs/deploy-tier-gotchas.md` 与本表一致。

**DOC candidates**：**DOC-2** 由 42 confirm（`recovery diagnose`/`resnapshot` 缺 `cluster.md`/`runbook`）。新 G-B DOC → **DOC-17+**（DOC-16 被 s3-s5-plan 的 #29-reclass 占）。

---

## §5 — NOT-COVERED（永久 + gated）

**永久（附理由）：**
- **Raw rehome/drain/seed `sys.events`**（`home_reassign_*`, `broker_down_rehome_summary`, `expose_rehomed`, `rehome_stalled`, `seed_*`, `grow_cutover_revival_failed`, `nats_topology_*`）——无 operator reader（`admin.go:33-36`）；效果由可读 control/data-plane 钉。store-backed 告警（`alert ls`/webhook）**已覆盖**。
- **Expose-rehome DATA plane on retire(40)** — #29-blocked；control-plane `migrateExposes` only；数据面 owner 71/#29。
- **`recovery node remove --force` 孤儿化(40)** — 需卡住的 VOTER_ADD_FAILED node homing exposes；不 fake（Mandate ①）；ghost-passthrough hermetic(12)。
- **Arm-token 纯 replay(22)** — 无 CLI/admin 二次 commit affordance；hermetic（`force_single_online_test.go`）。
- **AdoptDecision wrong-signer 决策表(91)** — hermetic；91-C-anchor 的部署价值 = on-disk-cache-不被毒属性。
- **`/metrics` public-interface 暴露 / webhook SSRF blocking(93)** — 有意 by-design（security-pragmatic）。
- **exit-2 QUORUM_LOST / exit-3 FORCE_SINGLE cells in 93** — cross-ref S6-22/12/20 + hermetic。
- **`recovery restore <bundle>`（never-escapable）** — S7-50/51、非 42。

**Gated（仅在 Stage-B 出口时才 NOT-COVERED）：**
- **Retire 脊(40) / to-standalone 脊(41) 若 SB-1=(c)** — NOT-COVERED-blocked-by-#31；OFFLINE de-cluster(20) 仍覆盖机制。
- **`ops confirm`(40) 若 SB-2=(b)** — ConfirmOp hermetic。
- **resnapshot `--accept-audit-loss`(42) 若 unpublished-audit 不可构造** — hermetic。
- **43 business-survival（F/G 行）若 SB-1(P2-serve)=(c)** — 保 B/C/D/E-mechanical/G-on-minimal-DB。
- **92(b) 专属 banner + 91-B seeds-converge 若 22 判 online-FS unreachable** — hermetic `g7_js_health_test.go`；诚实记录、G3-B 不宣称 clean。
- **92(a) JS-503 banner** — primary **by-design NOT-COVERED**（与 exit-2 READ-ONLY 冗余）；#41 only if OQ-D4-2 显 control-healthy N=1 失败。
- **90 `below_quorum`/`raft_lag`** — signature-guarded NOT-COVERED（nv→2 retire #31-gated / >64-entry lag 不可诱发）。
- **93 READYZ-503 CATCHING_UP cell 若 grow-window 不可采样** — predicate + removal-property 由稳定 phase + hermetic 钉。
- **`cluster upgrade --notify-webhook`(93)** — NOT-COVERED-with-reason tied to #31 若无 upgrade/grow-milestone POST 可构造。

---

## §6 — OQ 解决（roadmap §7）

- **OQ-4（online force-single dwell-bounce，22）**：DEFAULT EXPOSE（§2.5）。total-function Arm-0 三终态；production-real crash-loop=#35 RED；仅**确证**容器-timing artifact 才 license taming。无 default-to-tame。
- **OQ-7（60s sustained JS-503 poll）**：92(b) poll ≥120s tolerant；*Stage-B 确认 sim 未缩短 `jsDownThreshold`*。
- **OQ-8（并发）**：G-B grow-bound——见 §8。
- **OQ-5（`--cap-store`）**：Stage-B S1 确认 capped N=3 grow 可行 + 安全 fill（ballast-dd vs tier-B push）不 wedge JS meta；否则拆 90 disk 臂到 N=2-capped。

---

## §7 — Inventory 行消费（无遗漏闸；收工 stamp "G-B landing" 块进 `simcluster-coverage-inventory.md §4`，镜像 G-A landing）

**命令树行（§2.2）**：`cluster drain`→40 · `cluster retire`(`--wait/--timeout`；`--compromised`=S7-52)→40 · `cluster ops ls/show/abort`→40（`confirm` SB-2-gated） · `cluster apply --file`→40 · `cluster reconcile nats --plan`→40 / `--to-standalone/--confirm-single`→41 · `cluster set-raft-addr`(`--route/--allow-loopback`)→41 · `cluster recovery force-single --online/--dry-run/--self-*/--confirm-peers-dead`→22（`--guided`→42；OFFLINE=12/20） · `cluster recovery rejoin prepare`→42 · `cluster recovery resnapshot`→42 · `cluster recovery diagnose`→42（confirm DOC-2） · `cluster recovery node remove`(`--force` orphan NOT-COVERED；machine-confirm neg)→40 · `cluster init --from-existing`→43 / `--from-manifest`→42 / `--check/--dry-run`→43 · `cluster join prepare/approve --wait`→42 · `cluster add --dry-run/--auto-confirm-catchup`→40（`--preserve-js-data`→**out-of-40，§11-U8**） · `cluster seeds publish/show (--remote)`→91（+92(b)-B riding 22） · `cluster pin --force`/`cluster invite --bootstrap/--seed`→91 · `cluster status --card/--homes/--watch/--remote/--offline` + exit taxonomy→92/93 · `alert raise/clear/ls/ack`→90 · `session rm --ack-alerts`→92(a) · **`expose --ack-alerts`/`expose rm --ack-alerts`**→**92(a)（G-A 继承义务，authority=`s3-s5-plan.md:375` "owner S8-92(a)"）** · `serve --metrics-listen/--alert-webhook-url/--log-json`→93 · **`cluster upgrade --notify-webhook`/`cluster add --notify-webhook`→93 或 NOT-COVERED-tied-#31** · Tier-2 `--yes` rejectors→22(offline)/40/42/43（**row 180 SPLIT for force-single：offline=Tier-2，online=TTY-only，#36**） · machine-confirm 双因子→40/42/43。

**双-consumer 行**：row 151(`retire`)→40 owns 正例、22 owns protected-mode 拒；row 154(`set-raft-addr`)→41 owns 正例 rebind、22 owns protected-mode 拒——写进 inventory 拆分。

**store-backed ALERTS（§1.5，可读→覆盖）**：`manual`/`broker_down`/`disk_pressure`→90；`broker_draining`→40（drain 期在）；`below_quorum`/`raft_lag`→90（§3.1 显式 NOT-COVERED disposition）；`replication_degraded`→S5-30（cite；SSOT row 78 drop S8-90 或声一 severe-system 腿）。

**Raw EVENTS（§1.1/§1.3）**：全 NOT-COVERED-as-event；`broker_down_rehome_summary` SSOT row 56 downgrade → **flag for main ratification（§11-U8 已 ratify）**。

**收工闸**：`go test ./cmd/tether -run TestCommandTreeInventory`（零 diff——无 CLI 变更）+ `grep pubSysEvent`/`alert_ops.go` 重枚举；stamp "G-B landing" 块。**绝不**在 `--notify-webhook` 未 disposition 前让 "No unmapped row" 立。

---

## §8 — run-drills / 拓扑 / flake（§9 analogue）

- **编号族**：2x=22；4x=40/41/42/43；9x=90/91/92/93。N：22/42/92=N=2；40/41/91/93=N=3；90=N=3(broker_down，C1)；43=N=1→cluster。
- **Grow-concurrency（OQ-8）— G-B 是 GROW-BOUND（主 wall-clock）。** 每 G-B drill 组建 cluster → 全 9 落 `-j 2` grow-wave（VOTER-promotion 破 >~2，`simcluster:229-241`）。加 G-A 4 + 既有 10/11/13/82，grow-wave ~16-17 drill → budget **~60-75min 仅 grow-wave**。批头注。
- **#31 × concurrency**：setup grow 用 `grow_to_3` retry=1（attempt-count 记）。**例外**：40/41/91 的 #31-EXPOSURE 臂**绝不**让 retry 洗掉 leftover-op 态——断 as-built fixture 上的拒绝（勿照抄 drill-30 retry=0）。
- VOTER-timeout 故意非 `FLAKE_SIG`（`run-drills.sh:57-65`）→ grow timeout=RED、operator 单跑复核、绝不 auto-swallow。Preflight `fs.inotify.max_user_instances ≥ 2048`（已持久 8192）。
- Auto-discovery glob `drills/*.sh`；更新 README drill 表 + 编号族 + per-drill timing 头注。

---

## §9 — Per-drill false-green 风险头注（每 drill 一块；必须活到落地的守卫）

- **22**：REAL `Restart=always` unit 绝不 mask；Arm-0 是 TOTAL function（未分类→RED、APPEARS-FIXED→re-triage、非静默 POSITIVE）；每拒绝门 signature-guarded（wrong-reason/socket-down HARD FAIL）；GATE-b 需 `DwellRemaining` 非空(Phase-1-vs-2) + socket-liveness 前提；PROT control-source 需 brk1=leader(否则 vacuous)；POS PRIMARY oracle=被拒的 write 现成功 + MainPID 不变(变=restart=HARD FAIL)；绝不断 tier-B 恢复(R3 over-reach)。
- **40**：#31 拒绝在任何 labeled 清之前钉为头等 RED(无静默 `ops abort`)；retire double-removal 由独立 control-source WRITE 证(raft-config 无 reader)；mid-interrupt resume oracle 为 N=3 重定义(dwell/BLOCKED、node_start 是前提非清理)；retire-rehome frame gap-exposure-first(#29)；NOTE grep stderr；retire 脊 cite `cluster_operation_controller.go` 非 `clusterdrain.go`。
- **41**：to-standalone GREEN 由真 de-cluster 证(无 `cluster{}` + tier-B works at N=1 + R3 persist across restart)；存活 oracle 读 raft-replicated 行**非** JetStream stream(reset 抹 JS)；`--to-standalone --plan` 无 `# NOTHING WAS WRITTEN` claim(md5/.bak)；无 CLI-unreachable self-only rebind 腿。
- **42**：Arm-A oracle=journal message content、绝非 liveness(fix 是 message；broker 仍 exit 70)；diagnose/guided 覆两真出口(dead-peer→pasteable+exit0；alive-peer→非零+"peer reachable")、非假 exit-0；join-approve 收在真 clustered-JS op(无 hollow-voter 视图假绿)；O_EXCL 子臂 on INTACT DB(WIPE REFUSED、raft/db 仍在)；resnapshot single-voter 拒需 daemon-stopped + roster {brk1,brk2}；rejoin-prepare 以 root 跑(perms)。
- **43**：`--check` 由 `.bak`-absence + 字节等同证；survival/rollback 收在 `session ls` + `dp_curl` sentinel BODY(`grep -qF`)；machine-confirm 负例与非交互 cutover 配为正例对照；rollback 步 labeled `[operator per runbook §4]`；P2-serve gated on SB-1(绝不 fake auth_callout 强绿)。
- **90**：clean-baseline 门在每臂前(无预拉 broker_down/manual)；severe banner 仅经 manual-severe(broker_down/disk 是 INFO→`alert ls`)；ack **不** clear banner；store-backed ack 由 committed re-read `acked_by`；broker_down baseline liveness via `ReachSource=="nats-health"`(非 raft 口)；disk_pressure dedup_key node-scoped + no-bounce periodic-path 腿(或 scope 到 startup-tick)；`alert ack quorum_lost` 精确 live-condition 签名；N=3(C1)。（90 是破坏性 drill——此块为其 FG 审计。）
- **91**：全 A/C/D 收敛在首 publish(A1) 后**无**中间 publish；A2 gen 严格增；A3 断 retire COMMITTED before seed-drop(不藏在 #31 后)；D 钉 pre-failure 路径(ctl 可证达 floor) + seedless invite(`invite_seeds==null`) + floor 可证死 + `broker_url==floor`；C-anchor un-throttle 10-min TTL(fetched_at HARD pre-oracle) + torn-anchor 默认；R3-clustered claim cite product source。
- **92**：survivor 可证 alive/serving while JS-meta 1/2(非 "all failed")；`--ack-alerts` 断 gate-BYPASSED(downstream 判别子)、非 command-success；JS-503 banner primary=by-design NOT-COVERED(不为 correct code 登 assert_bug RED)；leg-b 若 22 unreachable 诚实 NOT-COVERED(91-B 同)；test-citation frame 修正(survivor 非-leader、非 voters:1)。
- **93**：follower peer-series 守卫只禁 DATA ROW(HELP/TYPE 无条件)；MET-degraded `poll_until` cached observe tick(drop "一注入服务三")；webhook no-secret=精确键白名单(多键=RED) + leader pinned；URL-neg 经 cluster-startup(single-mode 从不 parse)；CARD=同-report Health/exit 镜像(非 cross-time byte-diff)；READYZ-503 body 命名非-VOTER phase；EXIT 断(health-string, exit-code) 对 + DEGRADED≠ROSTER_UNREACHABLE；log-json 行是 broker-emitted slog；全后台探针 in-container。

---

## §10 — Stage-B spikes（全在 `weilandserver` 经 tether-exec；pin 前跑）

SB-1 #31-clear（40/41/91 承重）· SB-2 BLOCKED-op 制造（40 ops confirm）· SB-3 loopback-advertise N=1（41）· P2-serve（43，BLOCKING）· unpublished-audit（42-I）· manual join-approve-wait 驱动路径（42-F）· agent-survival across cutover（43-F/#40）· obs-seam YAML round-trip（93）· broker→ctl webhook POST 可达 + leader 稳定（93）· READYZ-503 DRAINING dwell（93）· retire-to-N=1 banner floor（92 OQ-D4-2）· `--cap-store` N=3 grow + 安全 fill（90）· foreign-cluster-B ingress multi-vhost（91-C-anchor，默认 torn-anchor）。

---

## §11 — 主进程定稿裁决（§10 A–K 待决项的最终立场）

> Stage-A 综合把 11 项标注"交主进程"；以下为定稿裁决。凡"Stage-B spike"者，立场 = **预期默认 + 反证切换条件**（探索→定格，先真跑、不静态跳过）。6 个承重结构断言（#31 retire-gate / online-FS 面 / broker_down-需-N≥3 / 92(a) leader-gating / `observability` key / admin-无-events-reader）已由主进程独立核验源码，见对应节。

- **U1（gotcha 编号去重，OQ-NUM——最高优先）— 裁决：ratify 规范映射** #35=22-dwell-unreachable / #36=22-online-`--yes`-not-Tier2 / #37=40-mid-retire-resume / #38=41-R3-reconciler-recluster / #39=90-disk-latency / #40=43-P2-agent-no-reconnect / #41(RESERVED)=92(a)-JS503-N1-banner。**#35-vs-#23 子裁决：#35 是 DISTINCT gotcha，manifestation-pin。** online force-single verb（`cluster_offline.go:220` 自述 "the preferred path"）在真 quorum-loss 里结构性不可完成 = 一个 #23 未钉的**新 operator-facing 能力缺口**；但其**根因 = #23 restart-bounce 族**（台账 cross-ref #23 为机制）。同 #29/#31 之与他缺陷共根因的先例——按 manifestation 钉使 operator 缺口可见 + 独立 flippable（durable-dwell OR dead-peer-prune 落地时翻）。

- **U2（22 disposition）— 裁决：DEFAULT EXPOSE**（total-function Arm-0，§2.5）。理由：repo 自身（`12:6`/`20:8-10` + `cmd_force_single` online 路径无 drill 驱动）已把该路径标为不可达；探索→定格的默认立场应是暴露它、而非预设可驱动。**OQ-D3-1 → (d-ii) NOT-COVERED** for arm-token 纯 replay（hermetic 已覆盖单用/TTL；POS 已在真 socket round-trip token）。**OQ-D3-3 → OFFLINE-nats-port peer-alive 锚为 primary**（online 分区 timing-fragile；OFFLINE 变体可靠）；online 分区变体仅 Stage-B 证可稳定 stage 才补。

- **U3（92(a) JS-503 banner）— 裁决：by-design NOT-COVERED 为 primary。** 为 correct-by-design 行为（N=2 quorum loss 已由 exit-2 READ-ONLY surface、banner 冗余）登 assert_bug RED = Mandate 反转。**#41 仅 RESERVED**，contingent on OQ-D4-2 retire-to-N=1 spike 显真正 control-healthy 单 leader 仍不 banner。**关键 debt 结论：G7b 的核心（exit-2 READ-ONLY / gate / `--ack-alerts` / session-rm-refuse）经 92(a) 22-INDEPENDENT 闭合**；只有 JS-503 banner 子面是 by-design NOT-COVERED，92(b)/G3-B ride 22。

- **U4（#31 fallback 政策，S6 blocker）— 裁决：若 #31 不可恢复地阻塞 online retire，40/41 落负例 + control-plane + set-raft-addr，retire/shrink 脊 = NOT-COVERED-blocked-by-#31**（与 drill 30/74 一致）。canonical `[GAP #31]` 清 = **re-run `cluster add … --account-seed`**（唯一 tether-native 清法；`ops abort` `AbortOp:268-280` 从不碰 `cluster_meta` marker = 已证死路）。#31 blast-radius 扩 +retire(40)+shrink(41)+91-A3，**无新号**。#31 拒绝在 40 里钉为**头等 assert_bug RED**（operator-observable 信号），在任何 labeled 清之前。

- **U5（90 拓扑 + below_quorum/raft_lag）— 裁决：采 N=3**（broker_down 在 N=2 结构性不可 raise，已核 `alert_reconcile.go`/`observability.go` leader-gating）。`below_quorum` + `raft_lag` → **signature-guarded NOT-COVERED-with-reason**（below_quorum 需 nv==2=一次 #31-gated retire 且与 N=3 broker_down 冲突；raft_lag 需确定性 >64-entry lag 不可诱发）。90 保统一 N=3（不拆 disk 臂，除非 Stage-B S1 发现 capped-N=3-grow 不可行 → 则拆 disk 到 N=2-capped）。

- **U6（R-mid-interrupt 拓扑，40）— 裁决：N=3 重定义 oracle**（pre-RAFT_REMOVED 态杀 leader → 新 leader resume + dwell/BLOCKED、无 double-apply/panic/FK → `node_start(killed-leader)` 复 quorum 作前提 → 推进 RETIRED），非 N=5。此臂本身 #31-gated（retire 须先启动）。理由：保拓扑最小、重定义 oracle 已 source-sound。

- **U7（Merge-vs-split）— 裁决：KEEP MERGED**（roadmap 默认 + 用户以"G2"单一单元框定），带**预注册 SPLIT 触发器**：仅当 Stage-B #31 spike 确认 S6-40 首个 drain/retire 被 #31-hard-blocked 成 release-blocking RED 战、膨胀 S6 外审面时，才拆 {S6}/{S8}。22→92(b) 耦合在拆分下存活（S6-before-S8 序先落 22 结论）；拆分唯一代价=多一次外审。Merged-by-default、split-on-#31-confirmation。

- **U8（ownership + ratification）— 裁决：全采纳。** `--preserve-js-data` out-of-40（grow-semantics）；`ops confirm` + `--auto-confirm-catchup` → SB-2 JOIN-BLOCKED gated、否则 NOT-COVERED-in-sim。inventory ratify：row 56（`broker_down_rehome_summary`→NOT-COVERED）；row 78（`replication_degraded`→S5-30、drop S8-90 或加 severe-system 腿）；row 180 SPLIT（force-single offline=Tier-2 / online=TTY-only per #36）；webhook-接收器 ctl-容器偏离 roadmap 角色容器（更新 roadmap harness 注）；`--notify-webhook`（93-if-constructible-else-NOT-COVERED-tied-#31）；DOC-17+ 起号；`expose`/`expose rm --ack-alerts`→92(a)（G-A 继承义务，authority=`s3-s5-plan.md:375`）。

---

## §12 — Stage-B spike log（live，weilandserver via tether-exec；随实测追加，影响 disposition）

> **SB-40-#31-INTERMITTENT（2026-07-14，drill 40 首跑 7/18）— 修正 §11-U4：#31 是 INTERMITTENT 泄漏、非"总阻塞"。** drill 40 的 grow_to_3(retry=1，两次 grow brk2/brk3) 后，`cluster retire brk2`（pty typed-confirm）**成功启动了 retire op**（stdout `watch: tether cluster ops show op-…` + stderr NOT-a-credential-revocation NOTE，exit 0）——即本 run 的 grow-lock **释放成功、retire 未被 #31 阻塞**。这推翻 drill 30 注释的"releaseGrowLock almost always fails"强假设：#31 的 best-effort 释放**间歇成功**。**修正 disposition**：40/41/91-A3 的 retire/shrink 脊**不应假设 #31 恒阻塞**——须**捕获 retire 实际输出并分支**：refused-with-grow-in-flight → 记 #31-leaked（本 run 泄漏）+ clear-attempt；op-started(`watch: ops show`) → **#31-released 本 run → GREEN 脊可达**（poll op 到 RETIRED + control-source-write oracle）。retire 是 **NON-BLOCKING**（须 `--wait` 或 poll op 到终态；成功启动 ≠ 完成）。#31 仍是真缺陷（间歇泄漏、泄漏时阻塞），但 drill 须两outcome-resilient、不能硬编码 assert_bug 恒 RED。**另**：drain/ops-ls/reconcile-plan 的实际 stdout 与静态 grep 所报不同（ops ls 无 active op 时可能不印表头；drain 成功串待实测）→ drill 40 加诊断 dump 实测定稿。


> 每条 spike 是 explore→pin 的实测证据；结论若与 §11 裁决冲突，以实测为准并注明。容器名格式 = `sim-<instance>-<node>`（`lib/docker.sh:15`）。

> **⚠ MANDATE 校准（用户重申 2026-07-15）：目标是暴露 tether 缺陷，不是全绿。** 每个 RED 分类为 (a) harness-bug（修 drill 自身机制）或 (b) 真产品/观测缺口（EXPOSE：登 gotcha + signature-guard RED，drill 因暴露真缺陷而 RED = 成功）；**绝不为绿松 oracle**。差点犯错：92 leg(a) 我险些把签名松成匹配"electing a leader"就算绿——已纠正为暴露。

> **SB-92-REMOTE-OBSERVABILITY（2026-07-15，drill 92 暴露，#42/#43 候选）— 真观测缺口，EXPOSE 非修签名：**
> ① **#42 候选：`cluster status --remote` 在真实永久 quorum-loss（N=2 杀 1 voter、1/2、非 force-single 不可恢复）下 VERDICT = "the cluster is electing a leader (transient) — re-run shortly"** —— 误导：把不可恢复的 quorum-loss 报成"暂时选举、稍后重试"（重试永不恢复）。--remote 无法区分 minority-reachable 下的暂时选举 vs 永久 quorum-loss。（view 行诚实标"leader unknown / run on a broker host"，但 verdict 行误导。）**推翻 plan §11-U3 的"92(a) exit-2 READ-ONLY 闭合 G7b"假设** —— --remote 根本不给 READ-ONLY verdict。
> ② **#43 候选：force-single `--remote` VERDICT 显 emergency mode(force_single_active) 但缺 `reconcile nats --to-standalone` remedy 提示**（socket 侧 banner 有、--remote 聚合面无）。
> ③ **待查：force-single 后 operator to-standalone 恢复 tier-B 仍 exit 70**（恢复不全 or 真 gap，下一窗口 triage）。
> **drill 92 leg(a) 须 reframe**：assert_bug/signature-guard 暴露 ①（钉"electing a leader.*transient"），非松签名匹配它算绿。session-rm 默认拒串、--homes、--remote+--offline mutex 串待抓（可能 harness 签名 or 真面差异，逐一分类）。

> **SB-92/93 续（2026-07-15，#42 已暴露✅ + 新发现）：**
> - **#42 EXPOSED ✅**（drill 92 assert_bug reproduced："electing a leader.*transient"）。
> - **#44 候选（EXPOSE，drill 92 session-rm）**：真 quorum-loss（N=2 杀 1，无 store-backed alert）下 `session rm` 默认给 **raw `SQLite error (store_error)`**、非优雅 advisory gate（"BLOCKED needs --ack-alerts" 仅当有 store-backed alert 才触发；quorum_lost 是 LIVE 条件非 store-backed → 命令栽在写层 store_error，operator 见困惑的 SQLite 错）。我原 session-rm 两臂前提错（假设 advisory gate 触发）→ 下一窗口 reframe 为暴露 #44 + 修前提。
> - **#43 候选（EXPOSE）**：force-single `--remote` VERDICT 缺 `to-standalone` remedy（socket banner 有、--remote 聚合面无）。
> - **drill 93 obs-seam 成功 ✅**：`observability:` yaml seam round-trip 通，/metrics 绑定，MET(is_leader/voters/peer_reachable{node=}/POST-405) 全过。CARD/EXIT **exit 69 = harness bug**（`cluster status`/`--card` 是 admin-socket 命令，我从 ctl〔无 admin socket〕跑了→改从 broker `dexec -u tether $LDR` 或 `--remote`）。LOGJSON 待查（log_json seam 生效否 / systemd 前缀）。
> - **待查（下一窗口）**：92 b-TERMINUS(to-standalone 后 tier-B 仍 exit70)、REMOTE-homes(疑需 proxy)、REMOTE-mutex 串；43-E(疑我 pre-mint/负例序列污染 cmd_init=harness 结构 bug，因 cmd_init 在 90/40/41 正常)；91-A2-brk3(brk3 是否 VOTER→真 G3 缺口 or grow flake)；91-D(暴露"单端点未 roster-warm ctl 无 failover"?)。

> **待分类 RED（下一窗口，暴露优先）**：见上 SB-92/93 续。分类原则：真缺口→EXPOSE(gotcha+signature-guard)；harness bug(如从错节点跑 admin 命令、签名串错)→修。

> **Stage-B drill 落地状态（2026-07-15，滚动更新）**：全 9 drill 已写。**CONFIRMED GREEN**：90-alerts(21, +M6 待测)、22-forcesingle(20)、40-drain-retire(16)、41-shrink(3 + shrink 脊 NOT-COVERED-blocked-#31)、42-rejoin(18, 修 K CONTRAST=rejoin-prepare 无 `--confirm-node-id` flag=never-escapable 证据 + DOC-2 反引号)。**待跑/重跑**：91-client-converge(A1 修 dexec-in-sh-c + 时间戳 gen；A2/D/C 待验)、92-js503(已投递)、93-metrics(已投递)、43-migrate(已写，outcome c)。**反复根治的 harness bug**：harness 函数（dexec/poll_until/tcp_refused/A/S/CTL）绝不裹进 `sh -c`（新 sh 不继承函数，只 `$(...)` 继承）——已全面 grep-扫描确认 9 drill 无残留。**#31 第二症状（drill 41）**：retire 可卡 `NATS_ROLLED_OUT after 6m`（非启动即拒），drill NOT-COVERED-blocked 兜底正确；`--wait --timeout` 已降 3m 加速。**下一步**：重跑 91 + 跑 43/92/93/90-M6 → Stage-C 对抗内审。

- **SB-43-P2-serve（2026-07-14，DONE）— outcome (b)/(c) 定案。** `up --brokers 1 --agents 1 --ctl 1` + `start-broker brk1`（un-init P2 standalone）→ tether-broker + nats-server 皆 `active`，但：① broker admin socket 不存在（`/var/run/tether/admin.sock: no such file`——P2 未起 admin 面）；② `session create lab --pin … --nats-url nats://brk1:4222` → **`nats: nkeys not supported by the server`**（standalone nats.conf 无 auth_callout/nkey，install.sh:690-704 已预期）。⇒ **bare P2 broker 不服务 NATS session/agent/expose**。43 的 pre-cutover serving 单 broker 必须经 **outcome (b)**：labeled `[env]` 单模 auth_callout provisioning（broker-ops §3.4 文档剧本，Mandate ③），或若不可行 → **outcome (c)** business-survival NOT-COVERED、保 B/C/D/E-mechanical/G-on-minimal-DB。建 43 时按 (b) 尝试单模 auth_callout 渲染；给不出即 (c)。

- **SB-22-forcesingle-Arm0（2026-07-14，DONE）— 修正 §11-U2：正路径 POSITIVE 可达（≥极简 N=2）。** 极简 N=2（up2+init+grow，无 tier-B 活动）+ `transfer-leader brk1` + `docker kill sim-<inst>-brk2`（正确容器名）后观测 120s：**brk1 MainPID 全程稳定（NRestarts=0，未 restart）**，`--online --dry-run` 门在 **t≈70s 达 `WouldProceed=true`** 并稳定保持。⇒ 杀 brk2 容器后 brk1 自身 nats-server 仍在、broker 不 clean-exit-on-nats-loss（`MaxReconnects(-1)`）、不 restart → `leaderlessSince` dwell 连续累积 → 满足。**online force-single 正路径在此可达 = POSITIVE 分支**，#35 "dwell 结构性不可达" 在极简 fixture **未成立**。**修正 disposition**：22 的 Arm-0 默认预期改为 **explore→pin、预期 POSITIVE（dwell 可达）**，#35 RED 仅作 fallback。§2.5 total-function Arm-0 已两分支覆盖，无需改结构，只改默认预期。**副产品（门 reason 串已实测核实）**：peer-alive="ALIVE on raft brk2:7400 (HARD-REFUSE: would split-brain)"；quorum_not_lost Phase-1="node still has leader contact; force-single is a quorum-loss escape only"。

- **SB-22-FULL-FIXTURE（2026-07-14，drill 22 首跑 18/20，DONE）— 定案：22 = GREEN POSITIVE，#35 在 sim 中非真 gotcha。** 全 `setup_forcesingle_n2`（N=2 + agent + **tier-B baseline push 的真 JS 活动** = drill 20:8-13 所述 bounce 语境）下真跑 drill 22：**Arm-0 verdict = BRANCH=positive**（NR0=0 NR1=0 PID 397 稳定、dwell 在 **t=25s** 达 WouldProceed=true）→ POS commit（pty typed-confirm）成功 + **PRIMARY control-source oracle**（先前 quorum-loss 下被拒的 `set-raft-addr` commit 后 SUCCEEDS）+ MainPID 不变 + force_single_active exit-3 banner + **R3 nats.conf 仍 clustered**——全绿。⇒ **杀 brk2 容器后 brk1 自身 nats-server 仍在、broker 不 restart、dwell 连续满足**，即便带 tier-B JS 活动亦然（bounce 未触发）。**#35 → 降级为 NOT-observed（default GREEN candidate，仅在将来某 run 真触发 survivor bounce 时才 assert_bug）**；22 Arm-0 的 gotcha 分支保留作 total-function 兜底但预期不走。**连带定案 de-risk**：92(b) online-FS banner leg + 91-B seeds-converge **可达（GREEN 预期，非 NOT-COVERED-blocked-by-22）**。**首跑 2 处 harness 签名 bug 已修**：PROT 拒绝串实为 `no leader (election in progress)`（非 `not the leader`）；YES-online(#36) 须在**杀前 healthy 态**测（POS commit 后 brk1 复得 leader 联系 → `--online --yes` 撞 `leader contact` 门而非 TTY/--yes 面）——#36 的实证 = online `--yes` 拒绝串（"leader contact"）与 offline `--yes`（"NO --yes override"）**不同** = online 路径不走 Tier-2 rejector（`cluster_offline.go:155-158` 早返）。

---

**相关文件（绝对路径）**：候选 drill `/home/weiland/projects/dist_experiment_control/test/simcluster/drills/{40,41,42,43,22,90,91,92,93}-*.sh`；harness 增量 `.../drills/lib/{setup-forcesingle,cluster,obs}.sh` + `.../drills/lib/agentyaml.sh`（复用）+ webhook 接收器（运行时注入 ctl 容器，非 `image/*`）；台账 `/home/weiland/projects/dist_experiment_control/docs/deploy-tier-gotchas.md`（#35+ + DOC-2/17+）；清单 `/home/weiland/projects/dist_experiment_control/docs/reviews/simcluster-coverage-inventory.md`（消费 + stamp G-B landing）；格式基准 `/home/weiland/projects/dist_experiment_control/docs/reviews/s3-s5-plan.md`。
