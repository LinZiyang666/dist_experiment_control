# Deploy-tier gotchas — 已了结条目归档（#25+ 台账）

Date: 2026-07-25（从 `docs/deploy-tier-gotchas.md` 剥离）。

> **这是什么**：`docs/deploy-tier-gotchas.md` 里**已了结**（CLOSED / FIXED / REFUTED /
> WONTFIX-BY-DESIGN）的缺陷条目全文归档。活台账只留仍未了结的条目 + 一张已了结索引表。
>
> **编号仍全局唯一且连续**——`assert_bug` 的 gotcha token 与 `[GAP #N]` 标注跨
> `deploy-tier-gotchas.md` / 本文件 / `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`（#1–#24）三处唯一。
> 查任一编号：先看活台账末尾的索引表，再到这里看全文。
>
> **同一编号的多条记录**（定案头 + 原始详细记录）一并保留，顺序与原台账一致。

## 已了结条目

### #25 — PIN CONNECT 无 per-IP 限速（架构 §E.6 承诺未实现）

> ****CLOSED（2026-07-19，R12/P7）**：per-IP PIN 限速已实现（`authcallout/ratelimit.go`，token bucket burst=10 refill=10/min，本地 in-memory 无分布式状态无新依赖）。错误 PIN 分支计数、超 10/IP/min 后连正确 PIN 也拒。`client_info.host` 由 nats-server 填、callout 层不可伪造；共享 NAT 是有意接受的 v1 权衡（宁误伤不留 PIN oracle）。本地 auth_callout e2e 真跑通（`test/p3` 12 错 PIN 后正确 PIN 被拒）。**
- **现象**：单源 IP 可在任一 60s 窗口内**无限次** PIN 首连尝试，无任何节流。`architecture.md:825` §E.6「未处理
  的威胁」表承诺「PIN 暴力：速率限制：每 IP 每分钟 ≤ 10 次尝试」，且 E.2 流（`architecture.md:754`）明确**在
  失败（Argon2 校验失败）分支** `拒绝 connection + 写 pin_failed + 按 E.6 速率限制计数`——即限速器的触发是**错误
  PIN 尝试**。实测（`80` Arm R）：**10 个同源错误-PIN 尝试全被处理（各发 pin_failed）后，第 11 个同源 correct-PIN
  首连仍成功**（若有 ≤10/IP/min 限速器,10 次失败后该源应被封,连正确 PIN 也拒）。
- **机理**：`internal/authcallout/handler.go`（`ensureMember`/`ensureAgentProvisioned`）+
  `internal/broker/authcallout.go` **无任何** counter / token-bucket / window / per-IP map /
  `golang.org/x/time/rate`；client IP `jwt.AuthorizationRequestClaims.ClientInformation.Host`（`client_info.host`）
  **可得但从不读**；每个 wrong PIN 只 `h.emit("pin_failed", …)`（`handler.go:297,348`）、无 attempt accounting。
  hermetic 对应件：`test/security/auth_bypass_test.go:200-226`（`TestPINBruteForceNoLockout5Tries`，5 错+1 对仍成功）。
- **怎么自动化或修**：加一个 per-IP（读 `client_info.host`）滑窗**失败**计数器，超阈返一个**区分于 wrong-PIN** 的
  rate-limit 拒绝 kind + 事件；或正式退役 §E.6 承诺（显式架构编辑，非静默 waiver）。
- **钉住它的（外审 F1 订正为错误-PIN 模型）**：`drills/80-session-isolation.sh` **Arm R**——(a) 10 个同源
  **错误-PIN** 尝试全被拒（reached auth path），(b) 捕获 **≥10 个 pin_failed** 事件（限速器真触发点 fired 10×），
  (c) **倒置 `assert_ok`：10 次失败后第 11 个同源 correct-PIN 仍成功** = 缺失的 §E.6 per-IP 限速。**flip**：限速
  落地后 (c) 翻 `assert_refuses <rate-limit-sig>`（10 次失败后源被封、correct PIN 亦拒），并加 post-fix 全形
  （**second-source-IP 控 in-window 成** + **window-reset 后原源恢复成**）成 GREEN 回归。

### #26 — evict 不清理 agent 的 managed OS 子进程（部署条件式；公网口随隧道掉线被清）

> ****CLOSED（2026-07-19，R12）**：evict 现在 agent 侧收割 managed 子进程——exec children 起 `Setpgid` 并登记，`agent_evicted` 事件到达后 SIGKILL 整个进程组（含 PTY run 会话）。仅 evict 触发，正常 restart/upgrade 保留子进程供 G.1 reconcile。变异验证：去掉收割 → 宿主进程表仍有该 pid。**
- **现象**：`admin evict <sid> <nid>` 删 `agent_provisioning` + `nodes` 行（FK cascade 移
  `processes`/`port_allocations` 行），broker DB 视图里 proc/port **都没了**，但 agent 主机上**真实运行的
  managed OS 子进程**（如 `tether run`/`tether exec` 起的进程）与 broker DB 视图 **DIVERGE**。活体 spike
  实测（2026-07-11）：setsid-nohup 部署下 evict 后 → agent daemon 退（clean exit 0）、broker DB 行没、
  **但 `sleep 999999` 子进程仍活（reparent PID1）= 真泄漏**。**公网 expose 口**则 evict 后 curl 拒
  （connection refused）= 被清——但机制是 **agent 退出→反向隧道掉线的副作用**，**非** evict 的显式
  listener-teardown。**部署条件式**：systemd unit（默认 `KillMode=control-group`）下 evict → cgroup teardown
  **顺带回收**子进程（**tether 仍啥没做**，实测反证 `C-GAP-proc-sysd`）；`setsid nohup`
  （`install.sh:371` 头号手动启动路径）下子进程真泄漏。
- **机理**：`internal/adminsock/server.go` 的 `handleEvict`（只 `DELETE FROM agent_provisioning/nodes`）+
  `internal/node/plan.go:63-77` `PlanEvict`——**无任何码 kill agent 的 OS 子进程或关反向-TCP expose
  listener**；agent 侧 evict 只走 `cancelRun()`（`internal/agent/agent.go:711-728`），无 evict-specific
  SIGTERM/SIGKILL；唯一 child-kill 路径 `killOrphanProcess`（`agent.go:1404-1435`）仅经 `DropProcesses`
  reconcile 可达、**不**由 evict 广播触发；exec 子进程经 plain `exec.Command`（`exec.go:325`，无 `Setsid`）起。
- **怎么自动化或修**：agent 在收到 `agent_evicted` 时 kill 名下 managed 子进程（对齐 reconcile 的
  `DropProcesses` 路径）；broker 在 row-delete 时显式撤反向-TCP 公网 listener（不依赖隧道掉线副作用）。
- **钉住它的**：`drills/81-admin-evict-session-rm.sh` 的 **C-GAP-proc**（倒置 `assert_ok`：复合谓词 daemon
  退 **且** 子进程仍活 = 泄漏，setsid-nohup 部署）+ **C-GAP-proc-sysd**（systemd 下 cgroup 回收反证=非
  tether 之功）+ **C-port**（公网口被清 GREEN，注机制=隧道掉线副作用）。**flip**：evict-cleanup 修落地后
  C-GAP-proc 翻断「子进程被收」。

### #27 — C2 well-known discovery 在 `cluster init` 后不 serve-ready（`manifest_listen` 被 seam 省略 + 未文档化）

> ****CLOSED（2026-07-19，R12）**：`manifest_listen` 在 cluster 模式下默认 `127.0.0.1:7480`（照 `nats_conf_path` 同款默认），init 后 discovery listener 已 bound、curl 可达。**
- **现象**：bare `cluster init` / `cluster add` 后 broker **不 bind** well-known manifest listener，C2
  bootstrap-URL onboarding 腿默认**死**（curl `http://127.0.0.1:7480/.well-known/tether/cluster.json` →
  connection refused，实测 `82` SETUP-27）；operator 必须**手加** `manifest_listen` 才 serve-ready，而
  该步骤**在任何用户文档里都没有**。
- **机理**：`internal/broker/broker.go:753` gate `if b.selfID != "" && b.cfg.ManifestAddr != ""` 才
  `clustermanifest.Bind`——默认 `ManifestAddr==""` 不 bind；`applyClusterSeam`（定义 `cmd/tether/cluster.go:880`，
  调用点 `:799`；R25 订正行号）
  只写 `data_dir/raft_addr/secrets_dir/nats_conf_path`、**不含** `manifest_listen`；install.sh 的整个
  `cluster:` 块含 `# manifest_listen`（注释掉）；serve unit 无 `--cluster-manifest-listen`；且
  `docs/usage.md`/`docs/cluster.md`/`docs/broker-ops.md` **零** `manifest_listen`/`well-known` 命中。
- **怎么自动化或修**：`applyClusterSeam` 自动加 `manifest_listen: "127.0.0.1:7480"`（对齐生产 Caddy 拓扑）；
  或在 `docs/usage §6`/`cluster.md` 文档化「启用 well-known discovery」的必需步骤。
- **钉住它的**：`drills/82-agent-onboarding-invite.sh` 的 **SETUP-27**（`assert_ok` 记默认关 gap：bare init
  后 loopback curl → http_code 000/connection refused），随后 `ingress_enable_manifest` 是**可见的 labeled
  operator 供给步**（gotcha #27 的 workaround；也正因此 S0-ingress 的 manifest_listen 启用是忠实的、非省事）。
  **flip**：文档化启用步 OR seam 自动加 `manifest_listen`。

> **G-A（S3+S4+S5）登记 #28/#29**（活体 server spike 实测 2026-07-12/13/14）。#28 = agent 升级白名单不可配（DOC-3 并发）；
> #29 = cluster expose 的 home **不可投递到非-tunnel broker**（`homeForExpose` un-homed 回落 → agent 回落固定 tunnel →
> self≠home 拒；净效果 tunnel-coupling，机制非"设计上硬编码"，R5-M3）+ **crash home → 常规 expose 全 voter 搁浅、不自动
> rehome**（drill 71 钉 crash-strand+return + rebuild-off crash；drain-migrate = **HARD RED 失败门**——`cluster drain`
> 被三重叠加墙挡即 RED 暴露为 release-blocking，非自证的 owner-decision NOT-COVERED，R6-M2）。旧「home-binding 永久死 / ≥1/4 探针 /
> 2× isolated GREEN（round-1..3）/ 必须-tunnel-coupled-by-design（round-4）」框架均已撤回（R5-M3）。

### #28 — agent 侧升级 URL 白名单硬编码、不可 operator 配置（DOC-3 是并发文档缺陷）· ✅ FIXED (R15, 2026-07-20)
> **状态：已修复**。产品加了 agent 侧可配白名单：`cmd/tether/agent.go` `resolveAgentUpgradeAllow`（优先级 `--upgrade-url-allow` flag > agent.yaml `upgrade.url_allow` > 内建默认），`internal/agent/upgrade.go:76` 消费 `Config.UpgradeURLAllowlist`；`agent_upgrade_allow_test.go` 证半接线闭合（daemon→resolver→agent.Config）。DOC-3 的错误 hint 也随之成真。drill 31 已翻（`_allow_artifact_agent` 配 agent 白名单后：CONFIGURED URL 越过 agent 门、卡在 `sha256_mismatch`；OFF-allowlist URL 仍 `url_not_allowed_local`=白名单仍强制）。drill 落 INCOMPLETE（保留 success-re-exec 的诚实 not_covered，归专属 success drill），#28 缺陷本身 CLOSED。下文为历史记录。
- **现象**：broker 放行（`broker.upgrade.url_allow`）的自托管镜像 URL，被 **agent 本地白名单**拒
  `url_not_allowed_local`——即使 broker 已 whitelist、agent 能 fetch+trust 该 URL、agent ONLINE+owner。
  自托管/私有 release 镜像升级在 agent 侧不可达。
- **机理**：`internal/agent/upgrade.go` `defaultAgentURLAllowlist` 硬编码 GitHub release 前缀；`tether agent`
  daemon 的 flag 集**无** `--upgrade-url-allow`、agent.yaml **无**对应键、**无** env——agent 白名单恒等于硬编码
  前缀。`serve.go` 的 broker 侧 `--upgrade-url-allow` 只配 broker 白名单、不下发 agent。并发 **DOC-3**：
  error_hints + usage 指向一个 agent 上不存在的 `--upgrade-url-allow` flag。
- **怎么自动化或修**：给 agent 加真的 `--upgrade-url-allow` / agent.yaml 键 / env 接线；或改 hint/手册指向 broker 侧。
- **钉住它的**：`drills/31-node-upgrade-fleet.sh` Arm #28（3 点判别子前证 agent-可 fetch+trust / broker-放行 /
  agt1-ONLINE-owner，使 `url_not_allowed_local` 是唯一可能墙 → `assert_bug` 钉之；假绿守卫：签名含 `_local`，
  broker `url_not_allowed`/`sha256_invalid`/`not_owner` 均触 guard）。**flip**：agent 白名单可配后 assert_bug 检出
  exit 0 → 真升级成功。**（活体签名以 31 最终定格为准）**

### #35 — [CANDIDATE，未在 sim 复现] online force-single 的 dwell 在 quorum-loss 期 survivor RESTART 时结构性不可达（"preferred path"仅 OFFLINE 逃生；根因 = #23 restart-bounce 族）

> **R6 定案（2026-07-19）：REFUTED as stated** —— trap 仅开机路径（已跑的 survivor 不重入）；`observeLeadership` 不受 leader 门控故 dwell 可满足；`install.sh:777-778` 保留 `StartLimitBurst=5` ⇒ 崩溃循环 ~10s 即停。drill 22 于 07-18 GREEN（未复现）。只剩条件句成立、措辞降级，不占发布闸。

- **状态（外审 round-3 M5 降级）**：**CANDIDATE，机理 source-cited 但尚未在 sim 确定复现**。此前把 #35 当已复现 gotcha 是过度声称——peer-kill fixture 达 22 POSITIVE（survivor 不 restart），#35 的真触发（survivor RESTART）在 22 的专属臂里**只有当一次 PROVEN 的 MainPID 变更（≠ 仅 NRestarts）叠加 dwell-never-satisfied 才判 PRODUCT-RED**；否则该 run 记 INCOMPLETE（`not_covered`），#35 保持 CANDIDATE。
- **现象/机理**：`cluster recovery force-single --online`（`cluster_offline.go:220` 自述"the preferred path"）需连续 `forceSingleDwell=15s`（`force_single_online.go:27`）的 quorum-loss。但 quorum-loss 期若 survivor 因任何原因 `systemctl restart tether-broker`（boot 撞 `b.js==nil` EJECTED trap `broker.go:948-958` → 非零 exit `serve.go:240` → `Restart=always`/`RestartSec=2` on **tether-broker.service** `install.sh:754-755`），每 ~2s 生命 < 15s dwell，且 `newForceSingleArm()` 每 boot 归零 `leaderlessSince`（`force_single_online.go:41,46`）→ dwell 永不满足 → online force-single **结构性不可达**，只剩 OFFLINE。
- **钉（22 专属臂，round-3 M5 硬化）**：`node_kill brk2` 后 survivor `systemctl restart tether-broker`；capture MainPID before/after——**若 MainPID 未变（restart 未生效）→ fixture 无效 → `not_covered`**（非 #35 结论）；MainPID 已变（restart 已证）且 DRY-never-WouldProceed → `assert_bug #35`（PRODUCT-RED）；MainPID 已变但 dwell 仍满足 → `not_covered`（#35 未复现，保持 CANDIDATE）。签名不再含 `socket`。**peer-kill fixture 不触发 survivor restart** → 正路径**可达**（22 POSITIVE 分支）。
- **flip**：durable dwell（跨 restart 保 `leaderlessSince`）OR quorum-loss 上 dead-peer raft-config prune。#35 是 #23 的 manifestation-pin（plan §11-U1）。复现后翻普通 PRODUCT-RED 回归；产品修复后 `assert_bug` 见 exit0 → APPEARS-FIXED → 提 `assert_ok`。

### #36 — online force-single 的 `--yes` 不走 Tier-2 rejector（与 offline 分歧；TTY 保护完好）

> ****CLOSED（2026-07-20，R14）**：online force-single 的 `--yes` 现在与 offline 一致走 Tier-2 rejector（`cluster_offline.go:171` 的 `rejectedUnattendedYes` 在 admin socket 之前）⇒ `--online --yes` 被拒 exit 64。drill 22 翻正为 `assert_refuses`，GREEN。**

- **现象/机理**：`runForceSingleOnline` 在 `cluster_offline.go:155-158` 早返、绕过 `:165` 的 Tier-2 `--yes` rejector → `--online --yes` 非-TTY 因 online-gate（leader contact / TTY-required）拒、**非** offline 的 `NO --yes override` 串。TTY 保护仍在（真 commit 需 typed confirm）。
- **钉**：`22` YES-online 臂（杀前 healthy 态，签名 = online-gate 串 ≠ offline `NO --yes override`）。DOC vs gotcha，低 sev。
- **flip**：online `--yes` 路由经 Tier-2 rejector。
- **产品修复（R14，2026-07-19）**：`newClusterForceSingleCmd` 的 `if online` 分支现在**先**调
  `rejectedUnattendedYes(cmd, "force-single", selfID)`，再进 `runForceSingleOnline`——与 offline 分支同一个 Tier-2
  gate。故 `force-single --online --yes` 现返 offline 一致的 `cannot run unattended … NO --yes override`（exit 64），
  **在触到 admin socket 之前**。TTY-typed node_id confirm（`confirmTypedNodeID(..., false, "")`）原样保留。
  测试 `TestOnlineForceSingleRejectsUnattendedYes`（+ `WithoutYesReachesSocket` 反向守卫）；变异（去掉 rejector）
  实测退化成 socket dial error。**drill 22 YES-online 臂签名待主进程翻正**（现应断 offline 同串）。

### #39 — `disk_pressure` monitor interval 固定 5-min、无 operator knob（90-M6 确认）

> **CLOSED（2026-07-19，R13）**：disk_pressure 间隔加了 operator 旋钮——`broker.observability.disk_check_interval`（yaml）+ `--disk-check-interval`（flag），优先级 flag>yaml>内建 5m 默认，亚秒/负值 Load 时拒。默认保持 5m。drill 覆盖 owner=R13（drill 90 待翻正为正向回归）。

- **现象/机理**：`disk.go:23` 硬编码 5-min monitor interval，`serve.go:175-201` 从不 wire 任何 `disk_check_interval`/阈值旋钮 → disk_pressure 自动检测最慢滞后 5-min，operator 不可调。90-M6④ 的"45s 内 raise"实为从 operator `systemctl restart`（startup re-sample `disk.go:99-102`）量、非 auto-detect。
- **钉**：`90` M6④ relabel（45s 从 bounce 量）+ 一条 no-bounce periodic leg（或 scope startup-tick，periodic hermetic）。**flip**：加 `disk_check_interval` broker.yaml/flag knob。

### #46 —  seeds change-gated auto-publish 漏第 3 voter（91 暴露；G3 client-converge 债的 manifestation） — **CONFIRMED（drill 91 product_red 钉住，owner R12）**

> ****CLOSED（2026-07-19，R12）**——**台账机理假说被推翻**：`DeriveSeedEndpoints` 本就正确处理 3 voter，真缺陷在**触发架构**（per-grow converge 只在 leadership-acquired 边沿重触发，稳定单 leader 集群永不再触发 ⇒ 最后一个 voter brk3 无后继 grow）。修：leader 每 observe tick 用 `seedSetEqual` change-gate 幂等 re-converge。**又一次「证据真、归因错」。****

- **现象/机理**（91-A2）：`cluster grow` 后 seeds 的 change-gated auto-publish 纳入第 2 broker（brk2 进 `seeds show` endpoints），但第 3 个 grow 后 **brk3 达 VOTER 却从不进 endpoints**（120s 内）。`seed_converge.go` 的 DeriveSeedEndpoints 为何不 derive 第 3 endpoint 待根因（SB-91）。
- **钉**：`91` A2——`if brk3 in endpoints → GREEN`；`elif brk3 IS VOTER but not in endpoints → product_red #46`（签名 = VOTER present ∧ endpoint absent，brk2 已收敛作对照）；`else 未达 VOTER（grow flake）→ not_covered`。**flip**：第 3+ endpoint 也被 derive。

---

### #51 — `recovery restore` 结构上不能 apply broker.yaml cluster seam ⇒ fresh DR box 上「start the daemon」必 FATAL

> ****CLOSED（2026-07-19，R10/P2）**：`recovery restore` 加 `--config`，落地即调 `applyClusterSeam` 写全五字段（含 `nats_conf_path`）。drill 51 F-b8 实测：seam 五字段齐全、无效键 `nats_route` 消除、fresh box 不再 boot FATAL。** 详见 `docs/reviews/r2-plan.md` §15。
- **状态**：**LIVE-CONFIRMED（2026-07-17，drill 51 臂 G1）**。
- **现象**：全灭 DR 后在 fresh box 上 restore 成功 → 照 runbook §5.2 step 3 启动 broker → FATAL
  `broker.cluster.data_dir is unset ... refusing to silently downgrade a cluster DB to single mode`。
- **机理**：`cluster init` 自 G4 #5 起调 `applyClusterSeam`（`cluster.go:794-804`），但 `newClusterRestoreCmd`
  的 flag 集**根本没有 `--config`**（`cluster_backup.go:123-129`）⇒ restore 结构上不可能 apply seam；
  install.sh 把 `cluster:` 整段注释掉（`:548-556`）⇒ 无 seam ⇒ `assertClusterDBConsistent` FATAL（`cutover.go:117-120`）。
- **钉住它的**：drill 51 臂 **G1**（`assert_bug`，签名 `data_dir is unset|refusing to silently downgrade`）+ DR-STEP-LEDGER 计入。

### #52 — `recovery restore` 既不渲染也不提示 nats.conf ⇒ fresh box 的 stock conf 无 auth_callout、broker 服务不了

> ****部分 CLOSED（2026-07-19，R10/P4）**：*silence 半* CLOSED——restore 完成文案印出有序 `reconcile nats --manual` 下一步，drill 51 G-nats **逐字执行 restore 自己印的命令**跑通。*auto-render 半*（restore 自动渲 nats.conf）降为已文档化的手工步骤，owner 决定 close vs. reduce-to-note。** 详见 `docs/reviews/r2-plan.md` §15。
- **状态**：**SOURCE-CONFIRMED**（静态源码事实：restore 无 `--config`、完成文案不提 nats.conf；`assert_bug "#52"`
  的直证 auth/nkey 签名分支本轮**未触发**——broker 在真实栈上根本没起来（seam 不完整 = #51/#52 自身运维后果），
  故经 **DR-completion NOT-COVERED** 门 + DR-STEP-LEDGER（undoc=2）捕获，而非直接 auth 签名；直证签名 owed to a
  run where the fresh DR box actually starts）。
- **机理**：`init` 打印 NEXT step-1 `reconcile nats --manual …`（`cluster.go:824-826`），`restore` 的完成文案只有
  `NEXT: start tether-broker, then cluster join approve`（`cluster_backup.go:115-119`）；stock nats.conf 无
  authorization/auth_callout（`install.sh:690-704`），cluster 模式 auth_callout 自动 ON（`serve.go:203-218`）。
- **钉住它的**：drill 51 臂 **G2**（三出口探索→定格；抓到 auth/nkey 硬墙 → `product_red`，seam 不完整 → 记 DR-completion NOT-COVERED）+ DR-STEP-LEDGER 计入（undoc=2）。

### #53 — **已拆分（2026-07-19，R10）**：`#53-silence` **CLOSED** / `#53-scope` **WONTFIX-BY-DESIGN**

> **拆分理由（主进程批准）**：原条目把两件事捆在一起——「**丢**」与「**不说**」。R10 只能、也只应修后者。
>
> - **`#53-silence`（静默）→ CLOSED**：backup 与 restore **两端都明确告警**
>   （`BUNDLE SCOPE` / `HISTORY/AUDIT NOT RESTORED`），runbook §5 亦补上。变异验证：抽掉任一端的告警，
>   对应测试即红。**「静默丢 history/audit」这条底线已达成。**
> - **`#53-scope`（bundle 不含 JetStream）→ WONTFIX-BY-DESIGN**：把 JetStream 拉进 bundle 会**强迫
>   `backup --offline`（其前提正是「守护进程已停」）去跟一个活的 nats-server 说话**，从而让 offline 与 online
>   两种 bundle **在范围上悄悄不同**——**那正是 #53 本身所属的那一类谎言**。故保持 bundle 为 state.db-only。
>
> **⚠ 对 drill 的后果（必须同批处理）**：`51-full-dr.sh:409` 的「已修」判据写的是
> *「灾前 history 行在 DR 后幸存；bundle 现已携带 JetStream 状态」* ——按 (b) 方案它**永远不会成立**，
> 会让 #53 永久报 PRODUCT-RED。drill 侧须改判为：**两端告警存在 = `#53-silence` GREEN**，
> 而 history 不可回填改记为 `#53-scope` 的**已登记设计边界**（`not_covered` class=gap 或正向断言「告警确实出现」）。

<details><summary>原条目（已拆分，存档）</summary>

### #53 — backup bundle 不含 JetStream ⇒ 全灭 DR 后 history/audit 全失且从不告警
- **状态**：**SOURCE-CONFIRMED**（静态源码事实：bundle 只含 `state.db`、restore 归零 `audit_published_index`；
  `product_red "#53"` 的 live-reader 分支本轮**未跑**——它坐落在 tail（G3/H/H2/J），而 tail 被 **DR-completion
  NOT-COVERED** 门短路（broker 从未起来）；live-reader 确证 owed to a completing DR）。
- **机理**：bundle 只含 `state.db`（`backup.go:87`），`audit_published_index` restore 时重置为 0（`restore.go:317`），
  raft 从 index 1 重 bootstrap ⇒ 无可回填。runbook §5 从不告知 bundle 不含 JS（DOC-19）。
- **钉住它的**：drill 51 臂 **J**（history rc≠0/无流 → `product_red`/DOC-19；有行 → 撤销）。


</details>

### #58 — cluster 模式非-leader home broker 重启后 orphan xfer object 永不回收

- **R16 状态（2026-07-22）：产品修复已落地，deploy-tier 证明仍欠 —— 本条保持 OPEN。** Lane C 实现 leader 跨-home GC：`homeOwnsXferBucket` 为假且 `xferBucketOrphanedEverywhere` 为真（split-home / 零节点会话，即**没有任何 home 能回收**的 bucket）时，由 caught-up **LEADER**（唯一，避免多 broker 竞争）回收年龄超过`xferCrossHomeReapAge`（派生自 3×tier-B 超时=15m，比 per-home grace 长，护住另一 home 上仍在飞的传输，跨节点时钟偏斜留足余量）的对象；内审 M6 补回 busy-bucket 合取（leader 自己持活 tracker 条目的 bucket 绝不 GC）。新增 serveconf seam `xfer_cross_home_reap_age` —— **外审 F2 后已收窄为生产安全旋钮：只能调高、不能低于 15m**（3× tier-B）。低于该下限会让 leader 删掉另一 home 上仍在用的对象，而 leader 看不见别人的 tracker。**因此它不再是 drill 的压缩接缝**；drill 96 的 #58 臂已改为无条件 `not_covered`（结构上不可在窗口内观察），**没有为此在产品里保留测试后门**。hermetic 钉：`TestXferCrossHomeGCReapsSplitHome` / `TestXferCrossHomeGCSkipsBusyBucket` / `TestXferCrossHomeReapAgeDerivation` / `TestXferUnreapableBucketCounter`。**未闭合的原因**：drill 96 本次未产生 orphan 集（峰值 2 ≤ tombstone floor 6，同 #57 的中断 gap），GC 无物可收；R16 因此给该臂加了**非空过闸**——峰值不超过 floor 时记 not_covered，绝不把「计数本就在 floor」当作 FIXED 空过。
- **状态**：**LIVE-CONFIRMED（外审 B2 修 oracle 后 3 次 fresh-instance 复跑一致）**。victim 固定非-leader ⇒
  `reaperMayDelete()==false` 源码保证、非 race。
- **机理**：`reconcileXferObjectsOnBoot`（`transfer_reconcile.go:27-94`）仅 `broker.go:942` 启动时调一次、
  无周期 pass，首门 `if !b.reaperMayDelete() { return }`（`:34-36`），cluster 模式非-leader = false
  （`clusterwrite.go:478-486`）。运维后果：反复 crash 累积 → 撑爆 per-session 8 GiB bucket cap = #21 族复发。
- **oracle（外审 B2 订正）**：旧臂 `grep -c OBJ_xfer` 数的是 **stream 名存在性**——但 `OBJ_xfer-<sid>` bucket 常驻到
  session 删（`transfer.go:189-193`），重启只删 stale **对象**、留 stream（`transfer_reconcile.go:18-22`），故 stream 存在
  ≠ orphan 存在 = **假阳性**。改为经 /jsz 数 OBJ_xfer* 的 `state.messages`（**对象数**）+ 差分：干净传输后基线（对象被
  deleteXferObject 回收，floor）→ 打断后 orphan 计数须 **> 基线** 才判（先证 orphan 存在再判是否被 reap）。
- **钉住它的（3 次复跑一致）**：drill 96 臂 A2——`baseline=1 → brk2 宕时 orphan=2（>1）→ brk2 重启 boot-reconciler 跑后仍=2`
  ⇒ 未回收 = product_red。签名守卫：仅当 `orphan-probe > baseline ∧ post-restart > baseline` 才钉；读不出对象数 → not_covered。

### #68 — R16 A4 的 `--reset-js` 确认门只更新了一半 remedy ⇒ JS-503 横幅让运维跑一条必被拒的命令

- **状态：✅ FIXED（G67 修产品侧；G69 补完部署层的第三个调用点）。**
- **⚠ 范围更正（G69，2026-07-22）——本条最初被我记小了。** 我当初把它写成「漏了一处 SSOT 副本」。
  真实形状是：**R16 A4 改变了 `reconcile nats --to-standalone` 这个动词的契约**（数据面 JS store 上必须
  带 `--reset-js`），而部署层有**三个**调用点，R16 只更新了其中一个（drill 42）：
  | # | 调用点 | 谁发现 | 怎么发现的 |
  |---|---|---|---|
  | 1 | `drills/42-rejoin-returning` | R16 自己 | R16 期间跑过 |
  | 2 | `drills/92-js503-remote-alert` | **G67 回归清扫** | R16 期间**没重跑** 92 |
  | 3 | `drills/41-shrink-to-standalone` | **G69 验收跑** | R16 与 G67 **都没重跑** 41 |
  | 4 | `internal/natsconf/remedy.go` 的 SSOT（JS-503 横幅） | **G67 内审** | 横幅让运维跑一条必被拒的命令 |
  | 5 | `cmd/tether/cluster_offline.go`（online force-single 完成文案，**手抄字面量**）+ `cluster_operation_controller.go` 的 N=1 边界下一步文案 | **G69 内审（G-7）** | 手抄字面量**结构上绕过了** SSOT 的 pin——SSOT 化本身正是为防这个而做的，却漏了这两处 |
  **同一个缺陷被独立发现了五次。** 第 5 处尤其讽刺：`remedy.go` 的文档注释开宗明义写着「三份手抄副本迟早烂掉一份」，
  而它自己的 SSOT 化并没有收编所有副本，于是**留下的手抄件正好绕过了为它设的 pin**。
  **教训（比缺陷本身更重要）**：改变一个动词的契约时，只更新「手头正在跑的那个 drill」是不够的——
  必须**全局扫所有调用点，包括产品自己打印给运维的文案**。我在修 92 时只扫了 drills 目录，没扫产品侧的
  手抄字面量；G69 内审才把第 4/5 处翻出来。**"改契约 ⇒ 扫调用点"必须包含：drills、lib、docs/runbook、
  以及产品里所有会把该命令打印给人看的地方**——尤其是**没走 SSOT 的手抄件**，因为 SSOT 的 pin 看不见它们。三个 drill 里有两个还各自留着一份**手工 `mv` JS store** 的 Mandate-② 遮掩，
  A4 之后它们不仅多余而且是**坏的**（拒绝 ⇒ conf 没换 ⇒ 手工挪走 store ⇒ 孤 voter 带 clustered conf 启动
  ⇒ `n1ClusteredJetStreamFatal` ⇒ 后续断言连锁失败）。三处现已统一改为**一条产品动词**
  `--to-standalone --reset-js`，并**先钉住确认门本身**（不带 flag 必须拒绝且点名 flag 与数据影响）。
- **机理**：R16 A4 给 `reconcile nats --to-standalone` 加了**确认门**：JS store 有数据时**拒绝**，要求 `--reset-js`。
  R16 更新了 grow-cutover 那处 remedy（`cluster_grow_cutover.go` 明写 `--reset-js`），却**没更新 SSOT**
  `internal/natsconf/remedy.go` 的 `DeClusterRemedyCmd`。而该 SSOT 正是 **DATA-PLANE-DEGRADED / JetStream
  UNAVAILABLE 横幅**的补救文案——那个横幅**恰恰**在 JetStream 一直在服务、因而 store **必然有数据**时才亮。
  于是：横幅让你跑 X，X 必拒。`remedy.go` 自己的文档注释早写了「三份手抄副本迟早烂掉一份」——这次烂的就是它预言的那份。
- **连锁**：drill 92 的恢复腿因此整条塌掉——`--to-standalone` 被拒 ⇒ conf 没换 ⇒ 而 drill 又用**手工 `mv`**
  把 store 挪走并重启 ⇒ 孤 voter 带着 **clustered conf** 启动 ⇒ `n1ClusteredJetStreamFatal` ⇒ auth responder
  起不来 ⇒ ctl re-login / terminus / REMOTE-homes 连挂 4 条。**表面 4 个失败，根只有 1 个。**
- **修复**：新增 `DeClusterRemedyResetJSNote` 并接入横幅。**命令本身不动**——在横幅里无条件推荐一个会丢
  audit/history 的破坏性 flag 更糟；拒绝本身是**护栏而非死路**（它已点名 flag、数据影响、以及先 `nats stream backup`）。
  这条 note 只是消除意外。
- **同时退役 drill 92 的遮掩**：第 134 行的手工 `mv /var/lib/tether/jetstream …` 是 Mandate-② 违规
  （R16 刚在 drill 42 里退役过同一形态），且 A4 之后它还是**坏的**。改为一条产品动词
  `--to-standalone --reset-js`（conf 交换 + store 移置一步到位），并**先钉住护栏**：不带 flag 必须拒绝且
  文案点名 `--reset-js` 与数据影响——只断言 happy path 会让日后有人悄悄摘掉破坏性操作的确认门。

### #59 — ~~被分区少数派 broker 无法「只读存活」~~ → **REFUTED（R6 定案，2026-07-19）**
> 只读存活是**设计态**、有名字：`proxyStateFrozenReadonly`（"control writes refused, but /sub KEEPS vending"，`proxy_cluster.go:43-45`）；`read.go:14-17` 明写 fence 存在正是为了「quorum-lost 节点继续供本地读而非砖化（§6.2）」。R6 的 Q2 实测（`MainPID` 稳定 / `/varz` 200 / `fenced` 拒绝）逐点吻合该契约、落在台账自己的 GREEN 分支。`broker.go:956-985` 是 `Run` 的**冷启动**序列、不在任何循环里——把它当稳态机理是又一次「把 X 当成 Y」。**不占发布闸。** 详见 `docs/reviews/r6-findings.md`。
- **状态**：候选，96 旗舰分区臂探索→定格。
- **机理**：`broker.go:956-985`（注释 :976-978 明说给的是 ranked differential、never a hard assertion ⇒
  不预设必崩）；总函数三分支（POSITIVE=MainPID 稳/NRestarts 不增 → GREEN；crash-loop → #59；否则 not_covered）。
  复现 = #35 CANDIDATE 的分区触发器首证。

### #50 — `cluster doctor --offline --db <不存在的路径>` 报 `0 fatal` 且 exit 0（「迁移源可达」预检承诺结构性为假）

> ****CLOSED（2026-07-19，R10/P5）**：`OpenReadOnly` 后加 `Ping()`+`PRAGMA quick_check`；db 格失败计入 fatal、exit 64。**六态**（不存在/空/截断/目录/权限拒绝/损坏页）drill 50 R3 臂逐一 `assert_refuses` 实测变红，对照 live DB 仍绿。** 详见 `docs/reviews/r2-plan.md` §15。
- **状态**：**LIVE-CONFIRMED（2026-07-17，drill 50 首跑即复现）**。
- **现象**：`tether cluster doctor --offline --secrets-dir … --db /nonexistent/nope.db --conf …` **exit 0**、
  `--json` 的 `.summary.fatal == 0`、db 那格报 PASS。运维据此认为「备份/迁移源可达」，实际那个路径根本不存在。
- **机理**：`internal/clusteroffline/doctor.go:82-87` 用 `storage.OpenReadOnly`；`internal/storage/storage.go:105-111`
  是**裸 `sql.Open`** —— database/sql 的 `Open` **惰性、从不建连、从不 Ping**，故一个不存在的文件也「打开成功」。
  `cmd/tether/cluster_natsconf.go:520-523` 只在 `fatal>0` 时 usageErr ⇒ 整条链路静默放行。
- **对照证据（同一 drill 内）**：`--conf /nonexistent/nats.conf` **确实**报 FATAL 并非零退出
  （`internal/natsconf/preflight.go:122-125` 做真读）⇒ **doctor 能红**，问题特定于 db 那格，不是「doctor 从不红」。
- **怎么修**：`OpenReadOnly` 后 `Ping()`（或 `PRAGMA quick_check` / `os.Stat` 预检）；db 格失败必须计入 `fatal`。
- **钉住它的**：`drills/50-backup-restore.sh` 臂 **R3**（**INVERTED**：命令 exit 0 **就是**缺陷 ⇒ 用
  `assert_ok`+`product_red` 而非 `assert_bug`——后者会把 exit 0 判成 APPEARS-FIXED → ASSERT-FAIL = verdict 误分类）。
  四态穷举（R-EXHAUST），R2 是其「doctor 能红」的对照源。**FLIP**：产品修好后 R3 翻 `assert_refuses`。

### #65 — ~~[CANDIDATE] 分区少数派的 stale-leader 写有时变持久~~ → **REFUTED（2026-07-19，R6 定案批）**

> **本条已证伪，不是产品缺陷，不占发布闸。** 详见 `docs/reviews/r6-findings.md`。
>
> **旧记录「6 轮中 5 次持久」的成因 = 归属错误，不在 raft 层**：控制面 RPC 是**跨全部 broker 的
> NATS queue group**，`--nats-url` 只决定 ctl 连哪台**入口服务器**，**不决定哪台 broker 处理请求**。
> 10 轮判定组 × 5 窗口点 = 50 次少数派写尝试，支③ **0 次**（solo 与并发两档一致）：
> - broker 提交日志显示所有 rc=0 的写**全由 brk2/brk3 提交，brk1 提交数 0**；
>   **分区之前**经 `--nats-url nats://brk1:4222` 建的记录**也是** brk3/brk2 提交的。
> - 分区期直读三台 SQLite：`brk1=no, brk2=yes, brk3=yes` —— 与「少数派本地有、多数派没有」的预测**正好相反**。
> - 50/50 次钉在 brk1 的尝试全部 rc=69，brk1 分区后**再无任何 `authcallout:` 行**
>   ⇒ 被隔离的少数派**无法认证新客户端连接**，结构上不可能接受写。
> ⇒ 台账那 5 次「持久」是 **5 次正确的多数派提交**。
>
> **残留（归 R14，非本条）**：本实验每次都是**新建连接**，故只证伪了 #65 原文场景；
> **分区前已认证的长连接**在窗口内写入的路径未被触及（CLI 短生命周期，结构上跑不到），需条件 Y = 长连接写入客户端。
> **另**：drill 96 的 D6b 判别子**即便修好 H6 后仍不能判定 #65**——缺「提交者归属」这一环，
> 只要 ctl 够得到多数派就会把**合法写**记成 #65 候选。须改为「提交者归属 + 分区期多数派可见性」双条件。

<details><summary>原条目（已证伪，存档）</summary>

### #65 — [CANDIDATE / 非确定性] 分区少数派的 stale-leader 写**有时**变持久（raft 安全性，需产品侧根因确证）
- **状态**：**CANDIDATE — 非确定性复现（外审 B10 后 6 次 fresh-instance 复跑取地面真相）**。此前记 LIVE-CONFIRMED 是
  **过度声称**；外审 B10 正确指出旧记录**自相矛盾**（同时称「持久/多数派可见」与「被回滚」）。**6 次复跑证明矛盾的
  真因就是这个现象本身非确定**：
  - **5 次持久**（run-1/2/6/7/8）：愈合后 canary3 `brk1=yes brk2=yes brk3=yes` ⇒ **持久/多数派可见**（支③，product_red #65）。
    其中 run-7 的 D4b 甚至报 `refused/blocked（CLI 未拿到 majority ack）`，该写**仍**持久——一个更强的 raft-safety 信号
    （CLI 以为失败、写却存活）。
  - **1 次回滚**（run-3）：愈合后 canary3 `brk1=no brk2=no brk3=no` ⇒ **正确回滚**（支①，无脑裂）；且该轮 D3 幸存写也超时 =
    **退化 run**，其回滚可能在非干净分区下发生。
  即：干净分区下分区少数派的 stale-leader 写**约 5/6 次存活、偶回滚**。一个本该**永不**存活的写有时存活 = 真 raft-safety 疑点，
  但**非确定**，不能记为确定性 PRODUCT-RED。
- **现象**：N=3、brk1 为 leader，静默分区 brk1 的 route(6222)+raft(7400)（4222 留通）后，经 brk1 跑 `session create canary3`
  返 rc=0（brk1 未及检测失去 quorum、作 stale-leader 接受）；愈合后该写**有时**经多数派 brk2/brk3 仍可见。
- **判别子（drill 96 D6b，RAW ARTIFACT 逐 broker readback + D4b rc 一并 log — EXT-REVIEW-B10）**：① 三处都没 =
  **GREEN**（正确回滚）；② 仅 brk1 本地有 = **not_covered**（本地视图 artifact）；③ 经**多数派**可见 = **product_red #65**。
  每次 run 按其单次 artifact 落支，绝不跨 run 拼接。
- **对照**：同 drill D6（多数派分区期写 canary2 愈合后可读）+ D5b（三节点收敛一 leader）为确定性硬断，证明分区生效、
  无脑裂-**丢失**方向正常。#65 只针对脑裂-**多余写存活**方向。
- **owed / FLIP**：这是 raft-safety 级疑点，需**产品侧**专门查「`session create` 经 stale-leader minority 的写路径为何有时
  durable」（是否 ack 了未 majority-commit 的本地 append；`internal/cluster` 写路径 vs raft commit 语义）。产品确证并修复后
  → 若消除该窗口则 #65 记 GREEN 回归；若确证为真缺陷则升 PRODUCT-RED（需带确定性触发器，非当前非确定 chaos 臂）。
  「tether 是否 ack 未 commit 的本地 append」。
- **待深查**：canary3 是否真被 majority commit（若是，机理是什么——stale leader 的 entry 如何被新 leader 采纳？），
  还是新 leader 选举时把 brk1 的未提交 entry 当成已提交并复制。需产品侧 raft 层专项分诊。**这是候选严重缺陷，drill
  只负责暴露 + 用 signature-guarded product_red 钉住，不负责归因。**
- **钉住它的**：drill 96 臂 **D6b**（多数派可见性判别子；signature-guarded product_red）。


</details>

### #64 — `recovery restore` 剪到单 voter 却不去集群化 nats.conf、也从不提示 ⇒ 照文档做必 crash-loop

> ****CLOSED（2026-07-19，R10/P4）**：完成文案改为**领以 de-cluster 步骤**并点名「REFUSE to start」，复用 `clusterstatus.go` 的 remedy SSOT、带真实 nkey。drill 50 K 臂翻正为正向断言，实测通过。** 详见 `docs/reviews/r2-plan.md` §15。
- **状态**：**LIVE-CONFIRMED（2026-07-17，drill 50 臂 K，稳定复现）**。
- **现象**：N=2 集群上 `recovery restore` 成功（`pruned 1 stale peers`）→ 照它自己印的 NEXT
  （`NEXT: start tether-broker, then cluster join approve`）启动 → broker **crash-loop ~4 轮**，broker.err：
  `error: broker: cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node — a single node cannot form the clustered JetStream meta quorum. The nats.conf almost certainly still has a `cluster{}` block: de-cluster it to standalone JS with `tether cluster reconcile nats --to-standalone --confirm-single --server-name <self-server-name> --broker-nkey <self-bus-nkey>` …`
- **机理**：restore 把 `cluster_nodes` 剪成 `{self}`（`restore.go` 的 normalizeRestoreStaging）⇒ 名册 N=1；
  但 nats.conf 的 `cluster{}` 块**原样不动**（restore 无 `--config`、也不渲 conf ⇒ #51/#52 同族），而
  N=1 + clustered-JS ⇒ meta quorum 不可能形成 ⇒ `serve` 返错 ⇒ `Restart=always` 拉起 ⇒ 循环。
  **产品在崩溃时刻自己印出了缺的那一步**（上面那条 remedy）—— 证明 restore **完全有能力**在完成文案里
  提前说，却没有说。
- **与 #51/#52 的关系**：#51=restore 不 apply broker.yaml seam（需 fresh box 才看得到）；#52=restore 不渲
  nats.conf（fresh box）；**#64 = 同族的「名册剪枝」那一半，在 lib-卷灾难下就能看到**（`/etc/tether` 完好）。
  ⇒ **plan §2.1 原写的「50 结构上看不到 #51/#52 面」被实测推翻**，已在 plan 订正。
- **恢复机理（实测订正，重要）**：drill 50 里 broker 在 **~73s** 后自行恢复，但 **nats.conf 仍是 clustered**
  ⇒ **不是** in-broker reconciler 去集群化（drill 20 的 #20 路径），而是 **brk2 的 nats-server 仍活着**、
  clustered JS meta 跨两个 nats-server 重新形成了。**真正的全灭 DR 没有这个幸存 peer**（那是 drill 51 的
  地盘）⇒ 本条**不对全灭场景下结论**。drill 里该臂已从「断言 reconciler 收敛」改为**如实记录实际机理**。
- **怎么修**：restore 完成文案加上「若 restore 后是 lone voter 且 conf 仍 clustered，先跑
  `reconcile nats --to-standalone --confirm-single …`」；或让 restore 自己渲染（同 `cluster init` 的
  step-1 打印）；最彻底=restore 带 `--config` 并 apply seam + 渲 conf（与 #51/#52 一并修）。
- **钉住它的**：`drills/50-backup-restore.sh` 臂 **K**（四态穷举 R-EXHAUST；signature 锚
  `cluster mode requires JetStream, but it is UNAVAILABLE on a lone N=1 node`；恢复机理只**记录**不断言）。


### #54 — account.nk / CA 轮换无产品级 re-render 与 verify；`reconcile nats --all --wait` 报 false all-clear；`doctor` 对 skew 失明

> **R11 修复（2026-07-19）**：doctor 接 `readClusterPublicIdentities`，issuer skew 计入 **FATAL**；`reconcile nats` 检出 skew **非零退出**（不再 false all-clear）。runbook §2.1 订正：reconcile 是 restart **之后**的 fail-closed VERIFY 步，不 re-render。**#55 一并解决**（重启即换 issuer，故 #55「不可构造」前提为假、无需新动词）。CLOSED。
- **状态**：**产品侧 FIXED（R11 P6，2026-07-19）**——facet 1 + facet 2 均已修，drill 52 臂 B2/B3 待主进程翻正为回归。
- **facet 2 现象（已修）**：换了 account.nk（issuer 源）后、broker 未重启前，on-disk account.nk 与 nats.conf 渲染的
  auth_callout issuer 已 skew，但 `cluster doctor --offline` 曾报 **0 fatal、零 skew 提示**。
- **机理**：auth_callout seeds 在**启动时一次性加载**（topology 对账从**进程启动 seed** 渲染，非按需重读
  account.nk），`reconcile nats` 是 pure polling（`cluster_reconcile.go:78` 自陈「It NEVER bumps a generation」）
  ⇒ 不重启 broker 永不换 issuer。唯一能打印该 skew 的 note 只挂在 rotation guide 与 `init --from-existing` 上
  ⇒ 运行中集群曾无任何动词可查。
- **修法（R11 P6）**：
  - **facet 2**：`cluster doctor` 的 check 列表接上 `readClusterPublicIdentities` 的结构化 skew 信号
    （`cmd/tether/cluster_secrets.go` 新增 `IssuerSkew/BrokerSkew` + `clusterAuthIssuerSkewChecks`）——on-disk
    account.nk/broker.nk 与渲染的 auth_callout issuer/nkey 不符即 **FATAL**（`auth-issuer-skew` /
    `auth-broker-nkey-skew`）。测试 `TestClusterDoctorReportsIssuerSkew`（端到端命令 + 变异对照）。
  - **facet 1**：`runReconcileNatsAuto` 在打印 converged 前后做同一 skew 交叉核对，检出即**非零退出**
    （`clusterAuthIssuerSkewError`），不再打印 false all-clear。测试 `TestReconcileNatsFailsClosedOnIssuerSkew`。
  - runbook §2.1 + `cluster_rotation.go` 轮换指引同步订正：reconcile 不 re-render，RESTART 才 re-render；
    reconcile 现为 VERIFY 步（fail-closed on skew），须在 restart 之后跑。
  - **deploy-tier 复现暴露的第二个产品 bug（drill 52 B3/55c，hermetic 漏掉）**：facet 2 的**检测**在真实栈上
    是对的（reconcile B2 用同一 `readClusterPublicIdentities` 已 fail-closed），但 `cluster doctor --json` 在检出
    FATAL 后，`main.go` 会往 **stderr** 打一行 `error: ...`；drill 用 `... --json 2>&1 | jq` 抓取 ⇒ JSON 尾部被
    prose 污染 ⇒ `jq: parse error` ⇒ 谓词失败（55b 收敛态无 FATAL、nil error、JSON 干净故 PASS——正是这一半掩盖
    了它）。**修**：`ExitError` 加 `Quiet` 位；`renderDoctor` 在 `--json` + FATAL 时返回 quiet 非零，`main.go` 经
    `renderTerminalError` 对 quiet error 不打 stderr（JSON 的 `summary.fatal` + 退出码已足够传达失败）。测试
    `TestClusterDoctorJSONStreamStaysParseableOnFatal`（真复现 `2>&1` 合流 + 双向变异）。**教训**：hermetic 只测了
    cmd 自身的 out/err，从没合流过 `main.go` 的 stderr 打印——机器输出命令的 `2>&1` 洁净度必须在合流后测。
- **钉住它的**：drill 52 臂 **B3 + 55c**（doctor FATAL auth-issuer-skew，`2>&1|jq` 可解析）+ **B2**（facet 1）——
  **deploy-tier r11e 实测 B2/B3/55a/55b/55c 全 PASS**（pass 57→59）。

### #55 — account.nk 轮换的 auth-rejection skew 窗口 ⇒ 与 #54 合并 → **CLOSED（R11，随 #54 一并修复）**
> R6 定案：**可构造**（重启一台 broker 即在 20s 内换 issuer），非「结构性不可构造」⇒ 原「加原子 switch-over 动词」的 (a) 裁决作废。真实问题（skew 无人可见 + reconcile false all-clear）已并入 #54，由 R11 修复。
- **状态**：**前提被 R6 证伪、真实问题并入 #54（R11 P6 已修可见性）**。**不加原子 switch-over 动词**（总纲 §2 原
  裁决据 R6 撤销）。
- **R6 定案**：原前提「运行中集群的 issuer **永不**变 NEW」为假——`topology_reconcile.go:233` 喂的
  `AccountIssuer` 由**本进程启动 seed** 实时导出，`natsreconcile/reconcile.go:157` 按**纯内容比对**换、不受
  generation 门控 ⇒ **带新 seed 重启一台 broker，nats.conf 20s 内就变 NEW issuer**，只重启一台即构造出 #55 的
  确切 skew。且因 auth_callout 是**跨 broker 队列组**，后果**比 per-broker skew 更糟**：两台上都出现 ~1/N 的
  授权违规掷硬币。
- **结论**：#55 的真实缺陷不是「不可构造」而是「运行中集群的 issuer skew 无人可见、且 reconcile 报 false
  all-clear」——即 **#54**。R11 P6 已给 doctor + reconcile 加上 skew 可见性/fail-closed（见 #54）。**不新增动词**：
  一个原子 switch-over 动词对「重启即换 issuer」的现实是多余的（铁律④：drill 越省事越可疑）。
- **钉住它的**：随 #54 的 drill 52 臂 B* 一并翻正；#55 不再作为独立 not_covered。

### #56 — `rotate-tunnel-cert` 的 follower 侧误导指引（**UX 误导，非流程死锁** — R6 收窄）

> **R11 收窄（2026-07-19）**：R6 已核实**不是死锁**——产品在 `RotateTunnelCert` 同一处就给了可执行出路（transfer leadership 后再做）。真实缺陷是 follower 侧走了 `clusterstatus.go:649-657` 的**通用** mutating-verb leader-redirect、对这个 **self-only 动词**给了错指引。R11 已修：self-only 动词旁路通用 redirect，直接指向「target 须 BE leader」。**「死循环」措辞降为「UX 误导」。CLOSED。**
- **状态**：**产品侧 FIXED（R11 P11，2026-07-19）**。**「死循环」措辞按 R6 收窄为 UX 误导**——`clusterdrain.go` 的
  `RotateTunnelCert` 在同一处**完整表述了真实要求并给出可执行出路**（transfer leadership 后再做），**不存在死锁**：
  照它做一次即可脱困。
- **现象（已修）**：在 follower 上跑 `rotate-tunnel-cert <self>` → 曾报「not leader — re-run on the leader host」；
  运维照做、在 leader 上对同一 target 跑 → 报「transfer leadership to <target> first」。第一条是**错指引**：去
  leader 对 follower 做仍会失败，因为真实要求是「**target 必须 BE leader**」。
- **机理**：follower 侧走的是 `clusterstatus.go` 的**通用** mutating-verb leader-redirect，对这个 **self-only 动词**
  给了错指引。
- **修法（R11 P11）**：为 self-only 动词 `OpClusterRotateCrt` **旁路通用 leader-redirect**（在 leader 门之前 dispatch，
  与 force-single/broker-reload 同列），直接进 `RotateTunnelCert`；后者现区分**错主机**（nodeID≠self：run ON the
  target）与**对主机但非 leader**（nodeID==self ∧ !IsLeader：transfer leadership to it），两者都指向「target 须 BE
  leader」、绝不再说「re-run on the leader host」。测试 `TestRotateCertOnFollowerGivesSelfOnlyGuidance` +
  对照 `TestGenericMutatingVerbStillRedirectsOnFollower`（drain 仍走通用 redirect，证明旁路是 self-only 专属）。
- **钉住它的**：drill 52 臂 **A4**（成对断言）——产品已修，主进程翻正为 GREEN 回归 + 收窄 drill 文案里的「死循环」措辞。

### #63 — rotate-tunnel-cert 在线轮换后 re-pin（R6 REFUTED：车队确实 re-pin；机理已在 R11 立源、残留归 R14）

> **R11 归因（2026-07-19）**：re-pin 真实路径已从源码立住——`rotate-tunnel-cert` 更新 roster `cert_fp` 但不 bump epoch，载体是**完整 register 回包**（`homeForRegister` 从 roster 实时读 pins），任何 NATS 重连/boot 触发 ⇒ 解释 R6 的 3/3 实跑。**残留→R14**：R8 的 active push 按 epoch 判收敛，裸 cert 轮换不改 epoch ⇒ 静默 agent 不被推送；已用 `TestActivePushIsBlindToBareCertRotation` 钉在代码里。CLOSED（残留 R14）。
- **状态**：**REFUTED（R6 3/3 实跑，后果被证伪）**——车队在线轮换后**确实 re-pin**。R11 从源码立住机理并加测试；
  一条 active-push 残留归 **R14**。
- **R6 定案**：三份保留日志 A7d 全 PASS，含承重双边沿（rc=124 黑洞过 30s yamux keepalive 杀旧会话 → 新会话对轮换后
  证书吐出精确 sentinel）。
- **机理（R11 从源码立住）**：`rotate-tunnel-cert` 更新 roster 的 `cert_fp` 但**不 bump expose epoch**。真实 re-pin
  载体是**完整 register 回包**：`homeForRegister` 从 roster **实时读** cert pins，故轮换后**下一次完整 register**
  即携带新 pin（同 epoch），agent 经 `ApplyHome` **原地**更新 `sess.certPins`（`TestD6ReviewSameEpochPinUpdateQueuedDuringLoop`
  钉住 agent 半边）。该 register 在任何 NATS 重连/boot 时发生 ⇒ 在线轮换在实践中 re-pin，正是 R6 所见。
  测试 `TestRotatedCertPinRidesTheRegisterReply`（投递载体携带轮换后 pin、epoch 不变）。
- **残留 → R14**：R8 的 **active home-delivery push** 不覆盖「静默 agent 的裸 cert 轮换」——其收敛判据
  `homeAssignmentApplied` **按 epoch 判**，轮换不改 epoch ⇒ pass 视作已收敛、不推送。闭合需给 applied-ack 带上
  pin 指纹（wire 变更）以让 pass 检出 pin skew，超出 R11 身份/凭据面范围。`TestActivePushIsBlindToBareCertRotation`
  把该残留**钉在代码里**（若未来 pass 变 pin-aware，该测试翻红、强制复议本残留）。

### #47 — `cluster add` 可把可达 joiner 永久留在 CATCHING_UP，后续 grow 被串行锁阻断

- **状态：✅ CLOSED / FIXED（R16，2026-07-22 deploy-tier 复验）** —— 本条此前唯一未闭合的原因是「修复后远端
  待复验」（2026-07-16 额度受限）。R16 在 weilandserver 用本条自己规定的 oracle 重跑 `drills/42-rejoin-returning`
  并取得 **verdict=GREEN（pass=48, assert_fail=0, product_red=0, not_covered=0）**：`cluster add` invocation-2
  **rc=0**、`✓ brk2 is now a VOTER`、权威 join op 达 **terminal SERVING**、`cluster add complete.`——即本条
  "修复后的远端结论必须以该原 oracle 重跑为准" 的三项硬断全部满足，fixture 仍是 retry=0 的严格版。
  R16 另外补了两处使该 oracle 可稳定达成的产品修复（见 42 行 tsv 证据）：A1 把 **returning joiner 自身**
  的 dead-epoch clustered JS store 在 grow P5 移置（rejoin-prepare 只擦 raft/+tether.db、不擦 JS store，
  joiner 因而 fail-stop 于 n1ClusteredJetStreamFatal），以及把 start-joiner 就绪检查从**一次性探测**改为
  **有界轮询**（一次性探测会撞上 joiner 重启后的正常启动窗口，把正确的 grow 判成失败）。
- **原状态（外审 round-4，2026-07-16，保留作轨迹）**：**RATIFIED / intermittent PRODUCT defect，修复后远端待复验**。
  当前保留的严格 40 单次日志中，brk2 invocation-1 精确到达 rc=75 start-joiner 边界；fixture 已证明
  ctl/auth-callout ready，但 invocation-2 随即收到 `Authorization Violation`（rc=77），权威 join op 留在
  `CATCHING_UP`（lag=21），后续 grow 被串行锁阻断。另一次较早保留运行命中独立表现：invocation-2
  rc=69、joiner 可达但 learner/CATCHING_UP 不收敛。不得把两份日志拼成“本次等待完整 4m”。
- **区别**：#47 是 joiner 已启动后的 Raft learner/catch-up 不收敛；#31 是 grow 已完成后
  `cluster_grow_active` 泄漏；#45 是 retire 的 `NATS_ROLLED_OUT` 停滞。不得再用 “#31-family flake” 合并。
- **修复边界**：产品端仅对 cutover 时 NATS 的 `Authorization Violation` 做 30s 有界重连；其他网络/TLS
  错误不重试。join 状态机在 terminal `SERVING` 前硬收敛 seeds 并按 joiner 值谓词释放 grow lock，避免最终
  reply 丢失留下永久 fence。严格 fixture 仍保持 retry=0，并硬断 invocation-2 rc=0、VOTER 与对应 op
  terminal SERVING；修复后的远端结论必须以该原 oracle 重跑为准。
- **证据**：修复前 `/tmp/s6-s8-external-round4-final-strict/40-drain-retire.log`（远端保留）；本地认证
  重连边界与异步 join/seed 单测通过。2026-07-16 平台远端额度阻止 post-fix 重跑，故本条尚不可 CLOSED。

### #48 — 连续 shrink 时 agent 可黏在已退役 broker 的 NATS 孤岛，ONLINE 行与真实命令路径分裂

> ****CLOSED（2026-07-20，R14）**：R6 机理订正（被饿着非投毒）落地——agent 侧 `rosterRefreshLoop` 数连续静默刷新，持续静默 + 缓存 roster 仍有可拨 voter ⇒ `rebuildOnBrokerSilence`（roster.go:473）关闭静默连接、重建到幸存者。复用 R8 的 rebuild-onto-voter 机制、只加静默触发器。drill 41 翻正为正向回归（窗口 210→300s 适配 stock 3-min SLA），GREEN。**

- **状态（外审 round-4，2026-07-16）**：**RATIFIED / release blocker**。41 先用单 seed + connz 强制
  agt1 直连将退役的 brk2；brk2 退役后产品正确读取 leaving roster、主动 rebuild 到剩余 VOTER，connz 与真实
  `exec` 均通过。agent 随机落到 brk3 后再执行最终 2→1：brk3 达 RETIRED、brk1 完成显式 standalone + JS
  reset/restart，但 agt1 在完整 210s signed-roster refresh SLA 内仍直连 brk3，brk1 上真实 `exec` 无回包；数据库
  ONLINE 不能作为替代证据。
- **根因边界**：现有 agent 修复能处理“当前 broker 返回 leaving/removed signed roster”，却不能处理已退出
  Raft/NATS mesh 的旧 broker 持有 stale VOTER roster、继续保持本地 client connection 的孤岛。测试没有在退役后
  restart agent、重写 `agent.env`、删 cache 或停旧 broker；这些都会替 tether 擦屁股，明确禁止。
- **证据**：`/tmp/s6-s8-external-round4-final-strict/41-shrink-to-standalone.log`；agent journal 仅有第一次
  `current broker is leaving ... rebuilding`（brk2），第二次无该事件，connz 显示 brk3=1、brk1=0。
- **flip**：退休协议在移除前可靠驱逐/迁移 client（或 agent 使用不依赖当前孤岛 broker 的权威发现通道），并
  保持 41 的 adversarial origin、双次 connz 与真实 post-restart exec/Tier-B oracle 全部 GREEN。

**修复候选（远端待复验）**：拓扑 `sys.events` 只作为唤醒提示，agent 立即走原有 account-signed、generation-
monotone roster refresh；timer 仍作丢事件兜底。只有签名 roster 证明当前 broker leaving/removed 且另有可拨
VOTER 时才重建 session。独立测试把周期设为 1h，证明 event 会触发 signed refresh；它不替代 41 的真实孤岛
oracle，41 必须保持不 restart agent、不改 env/cache、不停旧 broker。

- **R6 机理订正（2026-07-19，必读）**：机理是「**被饿着**」不是「被投毒」——退役 broker 的 `handleRegister`
  在 `isClusterFollower` 门（在 `RosterRefreshOnly` 分支**之上**）早返 + `ShutdownOnRemove` 默认 true ⇒ 它
  **什么都不答**，并非供 stale VOTER roster。上面「旧 broker 持有 stale VOTER roster」这半是被 R6 证伪的。
  「DB 显示 ONLINE」本身是**归属错误**（`nodes.status` per-broker 本地存活、从不复制 ⇒ ONLINE 恰证明应答来自
  那台退役 broker 自己）。
- **产品修复（R14，2026-07-19）**：走 flip 的**后半**——「agent 使用不依赖当前孤岛 broker 的权威发现通道」。
  R8 的 broker→agent 主动投递**不可复用**（孤岛 agent 在退役 broker 的 NATS 上、幸存集群够不到它）；可复用的是
  R8 期新增的 **agent 侧 rebuild-onto-voter 机制**（`requestRosterReconnect`）。新增：`rosterRefreshLoop` 计
  **连续静默**次数（`maxSilentRosterRefreshes=3`），达阈且 `hasOtherDialableVoter`（缓存 roster 里另有可拨 VOTER）
  为真时，`rebuildOnBrokerSilence` 把当前(静默)broker 的 host 标为一次性 `avoidHost` 并触发 rebuild——`connectNATS`
  首拨据 `nats.DontRandomize` 排除该 host、落到幸存 VOTER（幸存瞬时不可达则回退全池，无永久锁死）。既有
  `rosterRequiresReconnect`（拉得回新 roster 才发火）在孤岛下永不发火，这条正是补它的缺。测试
  `TestBrokerSilenceEscapesToVoter`（真 NATS，broker 建连后转静默 ⇒ agent 逃到 voter）+ 三条单测；变异（关静默
  触发）实测 agent 6s 内不逃、复现孤岛。**drill 41 真实孤岛 oracle 待主进程复验**（不 restart agent / 不改
  env/cache / 不停旧 broker）。

### #49 — ~~resnapshot 的 SQLite preflight 与 RecoverCluster 实际 FSM 不一致~~ → **CLOSED / ALREADY-FIXED（2026-07-19）**

> **已修复并复验，不再占发布闸。**
> **R6 定案（源码级）**：`internal/clusteroffline/offline.go:270-282` 现已跑 `previewRecoveredRoster`
> ——在**副本 DB + 克隆 raft 树**上真跑一次 recovery，若该 recovery 会复活 peer 则**拒绝**；
> force-single 的 prune 是硬步骤（`:169-176`）；staged swap 用 `unix.Renameat2(RENAME_EXCHANGE)`
> （`exchangedir_linux.go:26`，能力先证后变更、无 fallback）。已有 RED→GREEN 单测
> （`s6_s8_resnapshot_external_review_test.go:25,74`）。**无源码级缺口残留。**
> **R9 复验（deploy-tier）**：drill 42 的 rejoin/resnapshot 主干**首次端到端执行并全绿**（42 条断言），
> 其中「resnapshot 不复活陈旧 peer」一条实测通过。

<details><summary>原条目（已闭合，存档）</summary>

### #49 — resnapshot 的 SQLite preflight 与 RecoverCluster 实际 FSM 不一致，可复活已剪 peer

- **状态（外审 round-4，2026-07-16）**：**RATIFIED，产品修复已被独立 RED→GREEN 单测钉住；远端 42 待复验**。
- **现象/机理**：`recovery resnapshot` 原先只检查当前 SQLite 为 self-only，随后 `RecoverCluster` 却先恢复旧
  snapshot 并 replay Raft tail；旧 snapshot/tail 中的非 self peer 因而可在 preflight 后复活，同时 Raft config
  被重写成 `{self}`。旧 force-single 也会先 snapshot、再 direct-SQL prune，留下同一枚延迟炸弹。
- **钉**：独立测试先在 Raft tail 填入 peer、保持 SQLite self-only，旧实现真实 RED（resnapshot 接受）；修复后
  preflight 在副本上执行同一 recovery 并拒绝。第二条测试构造旧 snapshot 含 stale peer，要求 force-single
  计入 confirmed-dead、剪名册、以高于旧 applied_index 的 fresh `{self}` snapshot 原子换入，随后启动真实 Raft
  并提交一条权威写，防止只看 metadata 的假绿。
- **修复边界**：force-single 的 prune 改为 hard step；staged Raft 完整构建后用 Linux
  `renameat2(RENAME_EXCHANGE)` 交换，避免 live `raft/` 缺失窗口。42 额外硬断 resnapshot 后名册精确 `{brk1}`，
  并用真实 `OpTransferAudit` 制造 audit-bearing log，不再靠把 cursor 置零伪造风险。

> **候选 #38/#40/#41（spike-decided，plan §4）**：#38(41 R3-recluster，default-GREEN)/#40(43 P2-agent-reconnect，default-GREEN)/#41(92 JS-503 banner 不发 on control-healthy N=1，RESERVED contingent on retire-to-N=1 spike OQ-D4-2)——各批 spike 定格。
> **编号 reconciliation（round-2 M6 / round-3 M6）**：#37=plan §4 的 40-mid-retire-resume（**非** stall）；#45=retire NATS_ROLLED_OUT 收敛停滞（独立号，此前误标 "#37-family"）；#42=有界 `--remote` 窗口（#43/#44 折入）；#46=91 seeds 漏第 3 voter。本 ledger 与 `docs/reviews/s6-s8-plan.md §4` 表一致。

---

## g75-g78 部署层默认值（#75/#76/#77）— FIXED 全文归档（2026-08-11）

> 增量 = `docs/reviews/g75-g78-deploy-defaults-plan.md` + `-review.md`（内审）+ `-external-review.md`（外审）。
> #78 仍为 OPEN（product 已修，broker-WARN-damping 的 deploy-tier 面不可构造，由 drill 78 的 INCOMPLETE
> not_covered 拥有），留在 active ledger。#75/#76/#77 已由 hermetic + live drill(32 GREEN / 93 #75 五臂 PASS)
> 双证闭合，全文归档于此。

### #75 — broker 日志无默认封顶；`observability.log_file` 实测不写文件 → `broker.err` 可无界爆盘 —— **FIXED**

- **状态：✅ FIXED（g75-g78，2026-08-11；hermetic 全绿 + deploy-tier drill 93 的 #75 五臂 live PASS、product_red=0；外审 F1 config-check 提前成功已修）。**
- **两半治**：① 静默吞根因——`internal/serveconf.Load` 改 `KnownFields(true)` 严格解析（镜像 agent 侧
  `loadAgentYAML`）：错嵌套/键名 typo 现在**拒启**并报键名行号。官方模板自 P10 起带 `broker.tls.acme.email`，
  裸严格化会 brick 每台官方装机——故加 inert `TLSSection` stub 吸收它，`TestInstallShTemplateParsesStrict`
  （从真 heredoc 提取、含注释态 cluster 块变体 Mi12）长效钉模板↔schema 配对。`cmd/tether/cluster.go` 的
  seam-probe 站吞错改传播。② sink 生效自证——`resolveLogSink` 打 `tether: log sink <path>` breadcrumb。
  **unit→journal + in-process cap 早在 h1/v0.5.0 已落地**（② 面当时已修），本轮补静默吞与可见性两半。
- **外审整合（F1）**：`tether serve --config-check` 曾在 strict decode 后即退出、漏 newLoggerTo(log level)/
  auth seeds/webhook/listen 校验 → 假 "config OK"。已改为退出点后移到**全部纯校验器之后、任何副作用之前**，
  并抽 `broker.ValidateConfig`（webhook URL + listen 地址，sub/manifest 经共享 `httplisten.CheckLoopback`）供
  config-check 与真实 serve 共用；storage.Open 在 config-check 下跳过、logger 用 discard sink（不建日志文件）。
- **现象（历史）**：手写/精简 broker.yaml 不含 `observability` 段 → slog 落 stderr → unit
  `StandardError=append:<file>`（无上限），被 #78 的每 5s WARN 灌到 1.1 GB。补配 `observability.log_file` 后
  broker.log 仍 0 字节（slog 走了 journal，in-process cap 没接管）。

### #76 — install.sh 生成 broker/nats/caddy unit 但**不 enable** → 开机不自启（单 broker 是生产命脉）—— **FIXED**

- **状态：✅ FIXED（g75-g78，2026-08-11；drill 32 live GREEN 54 assert、#76 四断言全过；外审 F2/M4 已修）。**
- **修**：install.sh broker 角色装完**默认 `systemctl enable`**（不带 `--now`——仅 symlink，`pgrep tether`
  必空的 K.0 §2 never-start 不变量原样保留）；`--no-enable` 退出口 + uninstall 对称 disable + 悬空 symlink
  清理。K.0 §2 契约字句 sweep 全引用面。**banner 三分支**（外审 M4：无 systemd 宿主不再假称 ENABLED——
  `ENABLED_UNITS` 只在 enable 真跑时置位）。存量装机 retrofit 见 broker-ops §2。
- **外审整合（F2）**：journald drop-in 的**未引用 heredoc** 正文含反引号 `systemctl restart systemd-journald`
  = command substitution，装机时以 root 执行——改**引用 heredoc + printf** 只展开 cap 值；`lint-install.sh`
  纳入验证；`daemon-reload` 改 best-effort。
- **现象（历史）**：racknerd 的 `tether-broker`/`nats-server` 竟是 disabled，一旦重启整车队失联。

### #77 — journald 无默认 `SystemMaxUse` → 小盘 broker 的 journal 可无界增长 —— **FIXED**

- **状态：✅ FIXED（g75-g78，2026-08-11；drill 32 live GREEN、#77 断言 drop-in 值独立复算 + operator 尊重 + uninstall 净全过；外审 F3 已修）。**
- **修**：install.sh broker 角色**条件写** drop-in `/etc/systemd/journald.conf.d/60-tether.conf`，`SystemMaxUse`
  按 `/var/log` 三档（<10G→200M / <40G→500M / ≥40G→1024M）。任何位置已有未注释 `SystemMaxUse=`（含 spaced
  `=`，Mi6）则跳过；df 非数字向最小档（Mi9）。**台账"无界"订正**：journald 默认 = min(fs 10%, 4G)，非无界、对小盘太宽。
- **外审整合（F3）**：drop-in 曾**仅凭路径**认领所有权——覆盖/删除同名 operator 文件。改为**首行 ownership
  marker**（`# managed-by: tether-install.sh (#77 journald cap)`）证明所有权：无 marker 的同名文件视为
  operator/site-policy 文件,写入跳过、uninstall 保留。foreign 文件(含无 SystemMaxUse 的)survives 均有 p10 钉。

### #45 — retire op 卡 `NATS_ROLLED_OUT`、永不达 terminal RETIRED —— **FIXED**

- **状态：✅ FIXED（2026-08-11 实测复核 drill 40 GREEN、`product_red=0 not_covered=0`）。** `cluster retire brk2`
  顺利到 terminal RETIRED（drill 自述 `grow-lock released + converged this run`），不再卡 NATS_ROLLED_OUT；
  同一 drill 名含 "#31 retire gate"，印证 #31 修复也解除了它对 retire 的阻塞。归档系 g75-g78 外审 F6 顺手
  清偿的既有 bookkeeping 缺口（此前 prose 标"已修"但未迁 closed，导致 ledger-crosscheck 计其为 UNOWNED）。
- **编号（外审 round-2/3 M6）**：本停滞此前误标 "#37-family"，与 plan §4 的 #37（mid-retire-resume）冲突——
  故给独立 #45；#45 = retire START 后停滞；#31 = retire START 前被 grow-lock 拒；#37 = mid-retire-resume,三者相异。
- **现象/机理**（41 M15）：一次 `cluster retire` 可 STALL 在 `NATS_ROLLED_OUT`（rehome/migrate 未完成）、
  永不收敛到 terminal RETIRED；随后下一个 retire 被 `already in flight` 拒。
- **钉**：`40` R-retire 要求 terminal RETIRED op_state；`41` shrink else-branch 区分 #31 grow-lock vs #45 stall。
