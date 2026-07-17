# Deploy-tier gotchas — simcluster S-series 新台账（#25+）

Date: 2026-07-11（建档：S 系列首开批 S1 落地，roadmap `docs/simcluster-coverage-roadmap.md` §5）。

> **这是什么**：`test/simcluster/` deploy-tier drill 在**真实部署栈**（真 systemd + 真独立 nats-server +
> 真 install.sh 路径 + 真持久盘）上跑真实使用/运维旅程时，新发现的 tether 缺陷台账。编号 **#25 起、全局
> 连续**，接续 `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`（**#1–#24 是那段的 SSOT**，两文件互链）。全局
> 连续保证 `assert_bug` 的 gotcha token 与 `[GAP #N]` 标注跨两文件唯一。
>
> **只测不修**（roadmap §0.2）：S 系列 drill 只**暴露**缺陷（登记此处 + `assert_bug` 签名钉 RED），
> **不**交付产品修复；修复另立独立叶子增量（如 G 系列先例）。某 gotcha 修好后 → 对应 `assert_bug` 翻
> `assert_ok`、trailer token 移除，由修复批负责。
>
> 每条模板：**现象 / 机理(file:line) / 怎么自动化或修 / 钉住它的 drill + 签名**。

## `#I*` 过渡族收编（关族）

历史上 sim 用 `#I1` 记「cluster-mode `serve` fail-closed 拒无 raft state 的 fresh joiner」——这是 tether
**有意保留**的 fail-closed 不变量（不是缺陷），由 `drills/11-grow-gaps.sh` 的 `assert_refuses`
（`no raft state exists|never auto-bootstraps`）钉住。**自本台账起 `#I*` 族关闭**，不再新增 `#I*` 号，
一律用 `#25+`。`#I1` 的语义并入 drill 11 的头注与断言，此处仅登记其归属，不占 gotcha 号。

## 产品缺陷台账（#25+）

> **S1 现实 gotcha 数 = 0**（`60`/`61` GREEN 旅程、`62` FUSE-approx spike，未暴露新缺陷）。**S2 登记
> #25/#26/#27**（三条均由「探索→定格」臂以**倒置 `assert_ok`**〔缺陷=本该拒却成功/本该清却泄漏，与
> `assert_bug` 语义相反，见 §0.2 注〕钉住，附 flip 条件）。三条均经**活体 server spike 实测**（2026-07-11）。

### #25 — PIN CONNECT 无 per-IP 限速（架构 §E.6 承诺未实现）
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

### #28 — agent 侧升级 URL 白名单硬编码、不可 operator 配置（DOC-3 是并发文档缺陷）
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

### #29 — cluster expose 的 home 不可投递到非-tunnel broker（un-homed 回落）；crash home → 常规 expose **全 voter 搁浅**（不自动 rehome），直到 home RETURN
- **现象（LIVE-CONFIRMED，6 次 sim run 2026-07-12/13/14）**：
  - **home 不可投递到非-tunnel broker（净效果 = tunnel-coupling，但机制是 un-homed 回落）**：N=3 cluster 里
    `expose --on-broker <非-tunnel voter>`（agent 隧道在别处）→ `homeForExpose`（`internal/broker/home.go:96-113`）
    对该 home 返回 **nil（un-homed）**（home.go:105 日志「home not deliverable on expose; leaving un-homed」）→
    agent 的 AddProxy（`tunnel_adapter.go:76-77`，空 BrokerAddr ⇒ fallback）拨它**固定的** tunnel broker → 该 broker
    因 committed 行的 home_broker != self **拒 REGISTER** `token_unknown_or_revoked` → `agent_rejected:frpc_failed`
    + rollback（实测 exit 70；probe-onbroker + probe-drain P4，180s×2 严格，agent journal 抓证）。**净可观测**：
    cluster expose 的数据面只在 home == agent 自己的 tunnel broker 时起来。**注意这不是"agent 设计上硬编码固定
    tunnel"**（源码若投递 named home，agent 会拨它——见 #33 的 proxy exit 就成功拨非-tunnel home）；是
    **un-homed 回落 → self≠home 拒** 这条路径。N=1（home_broker=''）不受影响（drill 70 GREEN）。
  - **crash-stranding（#29 核心）**：一个 home 在 brkH 的常规（rebuild-ON）expose，brkH 被 **crash**（node_kill）后
    **不自动 rehome**——`internal/broker/rehome_events.go:52-53`「regular exposes are NOT auto-rehomed on a crash —
    stranded until a drain/return」。其公网口只由 home 服务，故 crash 后**全 live voter 上 curl exit-7 搁浅** +
    epoch 不变，直到 home **RETURN**（agent 重拨返回的 home，同端口/同 epoch 恢复）。
  - **rebuild-OFF**：`--no-rebuild` expose crash 后随 home 一起 down——**与 rebuild-ON 在 crash 下行为完全相同**
    （都搁浅、都在 RETURN 后同端口恢复）；rebuild 区别**只在 drain 路径**（rebuild-ON 迁 / rebuild-OFF 拒），
    而 drain 路径 NOT-COVERED。
- **round-5 订正（external-review R5-M3/M7，2026-07-14，撤回 round-4 的"必须 tunnel-coupled"措辞）**：round-1..4 曾
  写「home!=tunnel 初始交付**永久死** / `≥1/4` 倒置探针 / **2× isolated GREEN**」（round-1..3），round-4 又写
  「expose **必须** tunnel-coupled（agent 拨固定 tunnel by design）」——**后者被源码证伪**：`homeForExpose` 确实会为
  eligible 的 home 投递 named directive（BrokerAddr=home.TunnelAddr），agent 会拨它。真正的行为是 **homeForExpose
  对刚 grow 的非-leader 返回 nil（不可投递）→ 回落固定 tunnel → self≠home 拒**（un-homed 回落，非硬编码）。
- **机理**：`home.go:96-113` homeForExpose 对 not-eligible / `CertFP==""` / 空-TunnelAddr 的 home **返回 nil（un-homed）**；
  agent `tunnel_adapter.go:76-77` OpenHome 在 directive BrokerAddr 空时 fallback 固定 tunnel_addr；broker
  tunnelTokenLookup 在 token 的 home_broker != self 时拒 `token_unknown_or_revoked`；crash 后常规 expose 由 reaper
  **不 rehome**（仅 `__proxy__` home 会 rehome，regular expose 搁浅——与 #33 的 proxy-exit 行为相反）。
- **钉住它的**：`drills/71-expose-rehome-failover.sh`——**FIXTURE 门**（agt→brk3 间歇不建立时诚实 NOT-COVER-THIS-RUN、
  不在未建立 fixture 上跑 crash 臂）→ **combined crash（Arm C rebuild-ON + Arm D rebuild-OFF 一次注入）**：agent 隧道
  指向 brk3、rebuild-ON `wstrand` + rebuild-OFF `wnr` 都 home 在 brk3 → node_kill brk3 → **两个都全 live voter
  curl exit-7 搁浅** + `cluster status` reachability 证 leader 见 crash + epoch 不变 + wnr explain 诚实 rebuild:false/
  未 moved → node_start brk3 → **两个都同端口恢复**；**Arm A（--on-broker bogus 负例）**；**非-leader 硬门**（brk3 必是
  live 非-leader）。
- **drain-migrate = HARD RED 失败门（R6-M2，非 owner-decision NOT-COVERED）**：
  SUPPORTED-rehome-via-DRAIN（B）是 drill 71 的**硬断言**——`cluster drain brk3` 必须迁 rebuild-ON expose 到 survivor
  voter 并 serve；被**三重叠加**产品行为挡即 **RED 暴露**为 release-blocking:(1) homeForExpose 不投递非-tunnel home
  （un-homed 回落，`--on-broker <非-leader>` 从不 serve）；(2) 唯一替代——把 **agent 隧道**指向非-leader——本身
  **间歇**（agt→brk3 有的 run 建、有的 200s 重试仍不建；外审 solo1b 亦命中为 71 RED，drill 用 HARD FIXTURE 断言暴露）；
  (3) 即便 fixture 建成，`cluster drain brk3` 被 grow 遗留的 `NATS_ROLLED_OUT` 挂起 op 拒（"already in flight for brk3"，
  需手动 `cluster ops abort`——#31 grow-op 家族）。**Arm E（rebuild-OFF drain 拒绝）现为直接执行**（R7-M2）：wnr
  （rebuild:false）+ wstrand 在 return 后**都 live** 时直接 `cluster drain brk3`，要求 clusterdrain.go:665 的 rebuild-OFF
  拒绝签名；若 #31 挂起 op 先拦（NATS_ROLLED_OUT）则该签名不可达 → RED 暴露（不再与 G/F 并列为"需成功 drain 作前提"）。
  粘性（G）/ rehome_stalled（F）仍需**成功 drain** 作前提→同墙阻塞（暴露、非 rescope）。**hermetic 覆盖（R8-M3 订正 R7 的 scoping 错误）**：
  `cluster drain` 的 **marker + phase-advance + rehome-target-exclusion** 有 hermetic 测（clusteradmin_test、g1g7 A9），
  **且 rebuild-OFF drain 拒绝也有 hermetic 测**——`test/d7/integration_test.go testD7DrainRefusesRebuildOff` 断言
  `errors.As ErrRebuildOffExposes` + 枚举的拒绝端口 + home 未被静默改（R7 说"无 `_test.go` 引用 `ErrRebuildOffExposes`"是
  **事实错误**——那次 grep 只搜 `internal/`、漏了 `test/d7/`）。drill 71 Arm E 在其上**追加** CLI / 真栈 / #31-交互覆盖,
  **非唯一**覆盖。drill 71 **唯一独有**的是**端到端 drain-migrate 数据面**（rebuild-ON expose 真迁到 survivor 并经真隧道
  serve）——D7 的 `DrainRetireFollower` 对 `migrateExposes` no-op（无 expose homed）,该数据面路径**无 hermetic 覆盖**,
  drill 71 Arm B 是它唯一的覆盖。home_reassign_*/broker_down_rehome_summary/expose_rehomed/rehome_stalled **raw**
  sys.events 无 operator reader（owner-decisions D2，仅 raw-event carve-out）——这些 EVENT 本身 NOT-COVERED，但
  drain-migrate 的 EFFECT 是硬门。
- **怎么修**：让 homeForExpose 对任一 eligible home 可靠投递 directive（含 BrokerAddr+CertPins）+ agent 对 home
  directive **真正重拨新 home**（而非 fallback 固定 tunnel）；crash 后让 reaper **也 rehome regular exposes**（或明确
  文档化「crash 搁浅、需 drain/return」）。**flip**：crash 后 expose 自动 rehome 到幸存 voter + 数据面恢复 → Arm C
  的搁浅断言翻 → 翻 GREEN 回归。

### #31 — `cluster add` 的 grow lock（`cluster_grow_active`）best-effort release 几乎总失败、残留、阻塞 upgrade（dry-run 看不见）
- **现象**（`30-rolling-upgrade`，2026-07-12，3/3 live real roll 命中）：`cluster add`（grow）完成、joiner 达
  VOTER、命令 **exit 0 报成功**后，`cluster_grow_active` marker **几乎总残留**。后果：（a）`cluster upgrade` 的
  **real roll** 撞 `acquire upgrade lock refused: a cluster membership operation (join/retire) is in flight`
  （exit 69），HALT 于 safe partial state（无 host re-exec）；（b）同一残留让连续 grow 的下一个 `cluster add`
  撞 serialized fence → 该 joiner INCOMPLETE（**SOLO server-local 跑时的 serialized-fence grow flake 根因**——
  区分 simcluster:223-229 记录的**并发** 7-way-sweep VOTER-timeout，后者是 clustered-JS 形成时序、非 grow-lock）。
  「几乎总残留」的实证限定 (a) upgrade 场景（brk3 是最后 grow、release 时序最差，3/3 命中）；(b) 是间歇的。
  **关键隐蔽性**：
  `cluster upgrade --dry-run` 与缺 `--account-seed`/`--backup-taken` 的 refuse 都**不 acquire upgrade lock**
  （只预览 / 只做参数前置校验），所以它们**全 PASS**——残留只在 real roll 的 acquire-lock 步骤显形。这也是
  为何我最初的 dry-run 探测法探不到、险些把缺陷绕过成假绿。
- **机理**：`cmd/tether/cluster_add_drive.go:494-506` `releaseGrowLock` 自述「best-effort clears THIS joiner's
  grow marker via the leader」。两条失败路径（`could not resolve the leader` / `release-lock trigger` not OK）
  都只 `⚠ WARNING`（非 fatal），`cluster add` 仍 exit 0。**VOTER 达成 ≠ SERVING ≠ lock released**。3/3 live
  命中说明 release 在 sim 的 cutover-后 leader 时序下**几乎总失败**（非罕见边角）。
- **运维含义**：grow 后**无法直接 upgrade / join / retire**；需手动 re-run `cluster add --account-seed` 清
  marker 才能继续——一个 tether 本应自动完成、现实要人工绕过的步骤（tether 自己的 WARNING 就这么说）。
- **怎么自动化或修**：`releaseGrowLock` 失败应**重试到确认**（或 block grow 完成直到 lock released），而非
  best-effort 静默放过；或让 upgrade/join/retire 遇陈旧 marker 时能自愈清理。
- **钉住它的**：`drills/30-rolling-upgrade.sh`——real roll attempt-1 用其**自身 HALT**（`membership operation
  in flight`，`_roll_halted_on_growlock`）作 **[GAP #31]** 暴露证据，`assert_ok` 钉住「upgrade 被残留 lock
  阻塞」这一缺陷现象；再重试 tether 自己的恢复动词（re-run `cluster add`）清 lock——**实测恢复走同一
  best-effort release 路径、也清不掉**（30 real-roll #2 仍 HALT），故 upgrade-roll 机制（re-exec /
  write-continuity / PID-same）**NOT-COVERED**（#31 阻断，像 #29；假绿 MainPID-same/write-probe-clean 主动
  suppress，因它们 PASS 恰因 upgrade 没发生）。**坦白（Stage-C mandate-4 订正）**：我这次 SOLO server-local 跑
  遇的连续-grow `HALTED at acquire-lock: grow of brkN already in progress — serialized`（前一 joiner 的 release
  间歇失败挡下一个）确是同一 grow-lock 泄漏，我用一个**临时 server-local retry 调试脚本（不入 repo、非正式
  harness）**重试搭起 N=3；须与 simcluster:223-229 记录的**并发**（7-way sweep）VOTER-timeout 区分——后者是
  clustered-JS meta-group 形成时序、另一类 flake，正式 runner `run-drills.sh` 的 `FLAKE_SIG` 不含它、**不
  auto-retry**（surface RED 手动重跑）。#31 只 claim serialized-fence + upgrade-blocked，不 claim 并发
  VOTER-timeout。**flip**：release 变可靠（grow 完成即 lock released）后，30 的 real roll attempt-1 不再 HALT →
  [GAP] 臂走 else（clean）→ upgrade 机制变 COVERED、翻成普通 GREEN 回归。

### #33 — proxy exit crash-rehome 后 SS **数据面**恢复滞后控制面（**仅观测 + 测量，根因未归因**）

> 号码取 **#33**：`docs/reviews/s3-s5-plan.md:355` 已占 **#32（CANDIDATE）= rehomed-then-returned 泄漏 stale
> public listener（double-bind）**，不同假设，避免撞号（外审 R2-M2）。

- **现象（reproduced across valid runs）**：一个**已建立在传字节**的 proxy exit，其 HOME broker 被**杀**（quorum 保住
  2/3）后，**控制面立即恢复**——`proxy status` 的 home 从死 broker 移走 + 该 exit 达 `ready=true`——**但 SS 数据面在
  那一刻仍是黑洞**，之后 **per-run 或自动恢复（AUTO-RECOVERED，测量 lag、不声称固定值）或该 run 内不自动恢复
  （STRANDED，需手动 `proxy off; proxy on`）**——两种都如实记录。即"控制面报 rehomed+ready ≠ 数据面已活"。
- **round-5 订正（external-review R5-M6/M7，2026-07-14，撤回旧倒置-断言 + 240s 门 的措辞）**：早先的 round-3
  readiness-based（`_ready_lags_60s`）与 round-4 "rehomed+ready 瞬间倒置-assert_ok 数据面黑洞 + 240s 内必恢复 die"
  **两版措辞都撤回**——`proxy_ready` 恢复很快、`ApplyHome→OpenHome`（tunnel.go:937-964）是原子换 session + 重拨，
  没有可靠的"ready-but-black-hole"瞬间可倒置断言。当前 drill **不做倒置断言、不设 240s 恢复 die**，改为
  **measure-and-record**：
  - `[#33-a]` crash **确定性**切断到死 home 的隧道 → 那条 pre-crash 已验证在传的 SS leg **黑洞**（确定，非 flaky）；
  - `[CONTROL]` home 离开死 broker（die 门）+ exit 达 ready（die 门，宽容）；
  - `[#33]` **poll 180s 测量数据面是否自动恢复**，如实记 **AUTO-RECOVERED（记 lag）或 STRANDED**——**两种都接受**
    （既非 die、也非 flaky 倒置 pin）；STRANDED 时 QUORUM 臂的手动 `proxy off; proxy on` heal 证明可恢复。
- **未确立（不声称）**：① 任何**固定** lag 数字——lag 非确定，只 per-run 测量并 log；② **根因归因**——第一轮
  「ApplyHome 原地 re-point 已断 session」**是错的**（ApplyHome = 原子换 session + 重拨新 home；SS server 独立于
  tunnel）。**根因待查**。
- **运维含义（谨慎表述）**：承载 proxy exit 的 broker 崩溃后，即使 `proxy status` 显示 exit 已 rehome + ready，其实际
  egress 有时仍需数十秒才恢复、有时该 run 内根本不自动恢复（需手动 `proxy off; proxy on`）——一个值得进一步诊断的
  恢复缺口，非已归因的确定缺陷。
- **钉住它的**：`drills/73-proxy-cluster-ha.sh` Arm REHOME——baseline SS leg 传字节（die 硬门）→ kill →
  `[#33-a]` 同一已验证 client 黑洞（确定）→ `[CONTROL]` home 离开死 broker + 达 ready（die 硬门）→
  `[#33]` **测量并记 AUTO-RECOVERED/STRANDED（两种都接受，不 die、不倒置）**。QUORUM 数据面分离臂用
  `proxy off; proxy on` heal 在 2/3 fresh-establish 解耦，**不依赖** crash-rehome 恢复时序。
  **flip**：crash-rehome 后数据面稳定在 rehomed+ready 瞬间即活（不再 STRANDED）→ 该臂转成对"prompt 自动恢复"的
  正向 GREEN 断言。

### #34 — proxy home 分布无法稳定保持 one-per-voter；非-tunnel voter 的 proxy-eligibility 不稳定；auto-rebalance-on-return 不发火（external-review round-6 硬化 74 时暴露）
- **证据分级（R7 订正——区分"已证"与"观测但未独立归因"）**：
  - **已证 A（control-plane home 计数直读，无歧义）**：`cluster rebalance proxy` **能**把 3 个 __proxy__ exit 构造成
    one-per-voter（1/1/1，spread==0），但**随即又漂移回全堆 tunnel/leader broker brk1**（实测 1/1/1 → brk1=3/brk2=0/
    brk3=0）。这是**分布不稳定**的直接证据——`home_broker` 计数是控制面直读，不经数据面，无歧义。
  - **已证 B（运行时诊断）**：C-auto 窗口时刻 leader `cluster ops ls` 有**非终态 in-flight op**（`brk3 join in_progress`），
    关掉 auto 的 `no in-flight op` fire-gate（proxy_auto_rebalance.go:57）→ auto 正确 DEFER 不发火。
  - **观测但未独立归因**：74 的 SS 腿（`_ss_via_home`）**间歇超时**。**⚠ 单条 SS 超时不足以证明"eligibility loss"**——一次
    `_ss_via_home` 超时无法区分 missing-home / `/sub` 渲染 / ss-local readiness / sink 失败这几层。所以 SS 超时**仅佐证**"数据面
    有问题"，**不单独钉死具体层**；真正钉住 eligibility 不稳定的是上面**已证 A 的分布漂移**（home 计数），不是 SS 超时。
- **合起来的推断（非独立证明）**：分布漂移（已证 A）**符合**"非-tunnel voter（brk2/brk3）的 proxy-home-eligibility 不稳定
  ——拿到 eligibility → 接一个 exit → 又丢 → exit 重 home 回 brk1"这一机制，但该机制本身是**从 home 计数推断**的、未经源码/事件独立
  确认。伴随两个已知面：① 非-tunnel exit 的 SS 数据面间歇搁浅（#33 家族，观测——见上，不单独归因到具体层）；
  ② `TETHER_AUTO_REBALANCE=on` 的 auto-rebalance-on-return **在 sim 的 kill+return 场景不发火**（180s 锁定窗内，
  round-5/6 一致观测）。**根因确认（源码 + 运行时双证据）**：auto 的 fire-gates（proxy_auto_rebalance.go:57-58）=
  `downNow empty ∧ **no in-flight op** ∧ !force-single ∧ no recent proxy rehome`；74 的 C-auto FIRE-GATE 诊断**实测**
  在 auto 窗口时刻 leader `cluster ops ls` 有非终态 op：`brk3 join in_progress`（"topology convergence: voter brk1
  at gen 0 < …"）——即 return（rejoin）留下一个 **in-flight join op**（= 挡 71 drain、挡 30 upgrade 的**同一个 #31
  grow-op 家族**），关掉 `no in-flight op` gate → auto **DEFER 不发火**。**非 auto 逻辑本身坏**（hermetic
  `g7_auto_rebalance_test` 已密其 flap/gate/cooldown）。**#31 挂起-op 影响面广：挡 drain(71) + upgrade(30) +
  auto-rebalance(74) 三样。** 合起来：proxy home 分布 / rebalance / 数据面**整个子系统在部署环境下不稳定**，且与 #31 深度交织。
- **round-5 的掩盖（订正）**：round-5 的 74 用 measure-and-record（AUTO-SERVED/STRANDED 都接受）+ warn-only NOT-COVERED
  把上述**整条不稳定链全擦成 GREEN**——外审 round-6 R6-M1 驳回为"假 release 判定"。已撤回：B-dp（moved-exit 数据面
  闭合）+ C-auto（auto EFFECT）改**硬断言**，drill 诚实 RED 暴露。
- **钉住它的**：`drills/74-rebalance-on-return.sh`——`SETUP-ss-brkN`（每 voter 一条 SS 腿，非-tunnel exit 搁浅即 RED）
  + `SKEW-reconstruct`（SKEW 前重建 1/1/1，漂移即需重建，暴露不 hold）+ `B-dp`（moved-exit 数据面必须闭合，硬 RED）
  + `C-auto`（auto EFFECT 必须发生，硬 RED）。实测 `74 RED (3 failed, 33 passed)`。
- **怎么修**：让非-tunnel voter 的 proxy-home-eligibility 稳定（reconcile 后不丢），使 1/1/1 可保持；让 moved/rehomed
  exit 的 SS 数据面在 home 变更后可靠闭合（同 #33）；让 auto-rebalance-on-return 在 return edge 可靠触发。**flip**：
  三者稳定后 74 的对应硬断言转正向 GREEN 回归。

### #35 — [CANDIDATE，未在 sim 复现] online force-single 的 dwell 在 quorum-loss 期 survivor RESTART 时结构性不可达（"preferred path"仅 OFFLINE 逃生；根因 = #23 restart-bounce 族）

- **状态（外审 round-3 M5 降级）**：**CANDIDATE，机理 source-cited 但尚未在 sim 确定复现**。此前把 #35 当已复现 gotcha 是过度声称——peer-kill fixture 达 22 POSITIVE（survivor 不 restart），#35 的真触发（survivor RESTART）在 22 的专属臂里**只有当一次 PROVEN 的 MainPID 变更（≠ 仅 NRestarts）叠加 dwell-never-satisfied 才判 PRODUCT-RED**；否则该 run 记 INCOMPLETE（`not_covered`），#35 保持 CANDIDATE。
- **现象/机理**：`cluster recovery force-single --online`（`cluster_offline.go:220` 自述"the preferred path"）需连续 `forceSingleDwell=15s`（`force_single_online.go:27`）的 quorum-loss。但 quorum-loss 期若 survivor 因任何原因 `systemctl restart tether-broker`（boot 撞 `b.js==nil` EJECTED trap `broker.go:948-958` → 非零 exit `serve.go:240` → `Restart=always`/`RestartSec=2` on **tether-broker.service** `install.sh:754-755`），每 ~2s 生命 < 15s dwell，且 `newForceSingleArm()` 每 boot 归零 `leaderlessSince`（`force_single_online.go:41,46`）→ dwell 永不满足 → online force-single **结构性不可达**，只剩 OFFLINE。
- **钉（22 专属臂，round-3 M5 硬化）**：`node_kill brk2` 后 survivor `systemctl restart tether-broker`；capture MainPID before/after——**若 MainPID 未变（restart 未生效）→ fixture 无效 → `not_covered`**（非 #35 结论）；MainPID 已变（restart 已证）且 DRY-never-WouldProceed → `assert_bug #35`（PRODUCT-RED）；MainPID 已变但 dwell 仍满足 → `not_covered`（#35 未复现，保持 CANDIDATE）。签名不再含 `socket`。**peer-kill fixture 不触发 survivor restart** → 正路径**可达**（22 POSITIVE 分支）。
- **flip**：durable dwell（跨 restart 保 `leaderlessSince`）OR quorum-loss 上 dead-peer raft-config prune。#35 是 #23 的 manifestation-pin（plan §11-U1）。复现后翻普通 PRODUCT-RED 回归；产品修复后 `assert_bug` 见 exit0 → APPEARS-FIXED → 提 `assert_ok`。

### #36 — online force-single 的 `--yes` 不走 Tier-2 rejector（与 offline 分歧；TTY 保护完好）

- **现象/机理**：`runForceSingleOnline` 在 `cluster_offline.go:155-158` 早返、绕过 `:165` 的 Tier-2 `--yes` rejector → `--online --yes` 非-TTY 因 online-gate（leader contact / TTY-required）拒、**非** offline 的 `NO --yes override` 串。TTY 保护仍在（真 commit 需 typed confirm）。
- **钉**：`22` YES-online 臂（杀前 healthy 态，签名 = online-gate 串 ≠ offline `NO --yes override`）。DOC vs gotcha，低 sev。
- **flip**：online `--yes` 路由经 Tier-2 rejector。

### #42 — quorum-loss 后 ~TFence(10s) 内 `cluster status --remote` 误报 transient + `session rm` 栽 raw store_error（有界窗口观测缺口）

- **现象/机理**（Stage-C M1 订正——**非永久，是有界 ~10s 窗口**）：N=2 杀 1 voter 后 survivor 降 leadership，`LeaderContactStale` 在 `TFence=10s`（`internal/cluster/read.go:18`）+ leader-lease 后才翻 true。**窗口内**（~0-11s）：① `--remote` VERDICT = "electing a leader (transient) — re-run shortly"（误导：不可恢复却报"稍后重试"）；② `session rm` 默认（无 store-backed alert 时）栽 raw `SQLite error (store_error)`，因 `EvalDestructiveGate.QuorumLost` 走同一 TFence LIVE-probe 谓词（`alerts.go:144-163` / `d8_alerts.go:71-98`）、窗口内尚未判 quorum-lost → 不给优雅 `--ack-alerts` advisory。**TFence 后二者都自我纠正**（`--remote`→READ-ONLY/exit2 `cluster_status_nats.go:116-136`；session rm→优雅 advisory）。所以这是**短暂误导窗口**，非永久缺口。**订正**：plan §12 "推翻 §11-U3" 为**假**——§11-U3 的 exit-2 READ-ONLY 正确，只是 TFence-delayed。
- **钉**：`92` leg-a——**fence-aware 对照**：探 on-broker socket（~1s 后翻 quorum-lost）vs `--remote`（窗口内仍 transient）→ 断二者在窗口内**不一致**（socket 已判 quorum-lost、--remote 仍 transient）；或 TFence 后断 `--remote` 自我纠正为 READ-ONLY(exit2)=GREEN。**（原 #42/#44 "永久"断法是误分类 RED、已废。）**
- **flip**：`--remote`/destructive-gate 在窗口内即给 quorum-lost verdict（缩短或消除 TFence 误导）。#43/#44 折入本条与 #41（`--remote` remedy 与 banner 同受 `JetStreamUnavailable` gate `cluster_status_nats.go:161-168`，非独立缺陷）。

### #39 — `disk_pressure` monitor interval 固定 5-min、无 operator knob（90-M6 确认）

- **现象/机理**：`disk.go:23` 硬编码 5-min monitor interval，`serve.go:175-201` 从不 wire 任何 `disk_check_interval`/阈值旋钮 → disk_pressure 自动检测最慢滞后 5-min，operator 不可调。90-M6④ 的"45s 内 raise"实为从 operator `systemctl restart`（startup re-sample `disk.go:99-102`）量、非 auto-detect。
- **钉**：`90` M6④ relabel（45s 从 bounce 量）+ 一条 no-bounce periodic leg（或 scope startup-tick，periodic hermetic）。**flip**：加 `disk_check_interval` broker.yaml/flag knob。

### #45 — retire op 卡 `NATS_ROLLED_OUT`、永不达 terminal RETIRED（40/41 暴露；与 #31 grow-lock、与 plan §4 #37 mid-retire-resume 均相异）

- **编号（外审 round-2 M6 / round-3 M6）**：本停滞此前误标 "#37-family"，与 **plan §4 的 #37（mid-retire-resume）语义冲突**——故给它**独立干净的 #45**（#38/#40/#41 已被候选占、#43/#44 已折入 #42，见下）。#45 = retire START 后停滞；#31 = retire START 前被 grow-lock 拒；#37(plan §4) = mid-retire-resume；三者相异。
- **现象/机理**（41 M15）：一次 `cluster retire` 可 STALL 在 `NATS_ROLLED_OUT` 阶段（rehome/migrate 未完成）、永不收敛到 terminal RETIRED；随后下一个 retire 被 `already in flight` 拒（`cluster_operation_controller.go:33`）。
- **钉**：`40` R-retire 要求 terminal RETIRED op_state（非仅 roster-absence；stall 则 `assert_bug #45` → PRODUCT-RED）；`41` shrink else-branch 区分二因（#31 grow-lock vs #45 stall）、`product_red` EXPOSE 本停滞。**flip**：retire op 在 rehome/migrate 完成后可靠推进到 RETIRED。

### #46 — [CANDIDATE] seeds change-gated auto-publish 漏第 3 voter（91 暴露；G3 client-converge 债的 manifestation）

- **现象/机理**（91-A2）：`cluster grow` 后 seeds 的 change-gated auto-publish 纳入第 2 broker（brk2 进 `seeds show` endpoints），但第 3 个 grow 后 **brk3 达 VOTER 却从不进 endpoints**（120s 内）。`seed_converge.go` 的 DeriveSeedEndpoints 为何不 derive 第 3 endpoint 待根因（SB-91）。
- **钉**：`91` A2——`if brk3 in endpoints → GREEN`；`elif brk3 IS VOTER but not in endpoints → product_red #46`（签名 = VOTER present ∧ endpoint absent，brk2 已收敛作对照）；`else 未达 VOTER（grow flake）→ not_covered`。**flip**：第 3+ endpoint 也被 derive。

### #47 — `cluster add` 可把可达 joiner 永久留在 CATCHING_UP，后续 grow 被串行锁阻断

- **状态（外审 round-4，2026-07-16）**：**RATIFIED / intermittent PRODUCT defect，修复后远端待复验**。
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

## 文档缺陷（DOC-n，不占 gotcha 号）

产品**文档**缺陷单列于此（roadmap §5：批量修文档是独立小增量，S 系列只登记不顺手改）。

- **DOC-3（确认，S1 顺带经命令树生成器暴露；随 S5-31 一并定格/修）**：`error_hints.go:34`
  （`url_not_allowed_local` 提示）与 `docs/usage.md:1443` 都让用户「检查 **agent 的**
  `--upgrade-url-allow` flag」，但 **agent daemon 根本没有该 flag**——它只存在于 `serve`（broker 侧，
  `cmd/tether/serve.go:267`）。命令树 golden（`cmd/tether/testdata/command_tree_golden.txt`）证实
  `tether agent` 的 flag 集不含 `--upgrade-url-allow`。**机理**：agent 侧升级 URL 白名单**硬编码、不可
  operator 配置**，而 hint/手册指向一个不存在的旋钮。**修**：要么给 agent 加真的 `--upgrade-url-allow`
  接线，要么改 hint/手册指向 broker 侧配置。**钉住它的**：S5-31 的 `31-node-upgrade-fleet.sh`（未开工）。

- **DOC-5（S1 核实为「非缺陷」，登记备案）**：`exec` 远端进程被信号杀时 CLI 退**扁平 128**（无 signal
  号）——agent 返回 `ExitCode()=-1`（`internal/agent/exec.go:202-208`），ctl 塌成 `os.Exit(128)`
  （`cmd/tether/exec.go:131-135`）。**核实结论**：`docs/usage.md:669-670` **已正确**记载「信号杀变 128…
  当前不传具体信号号」，故**不是文档缺陷**；「128+signo」是**有意**延后的 v2 wire-proto 能力。由
  `60-user-journey` 的 J7（`rc==128` 且 stderr `terminated by signal`）钉住这个扁平-128 契约。

**S2 登记（主进程裁定：以下为**有意设计**或**文档/命名漂移**，非 gotcha；drill 中立钉现实）：**

- **DOC-6（O4 裁定：eviction ≠ ban，有意）**：被踢 agent 的 nkey 用**仍有效的 session PIN** 可**重新
  provision** 回来（`80`/`81` D1 实测 re-join → ONLINE）。**机理**：`internal/authcallout/handler.go:246-289`
  evict 后 `Lookup`→`ErrNotProvisioned`，`ProvisionWithPIN` 用不变 PIN 成功——**无 nkey/fp denylist**。
  **语义澄清**（这才是 DOC 而非缺陷的理由）：evict 删的是 provisioning 行、不封 nkey；「nkey 泄漏立即吊销」
  用例中，**仅 nkey 泄漏（session PIN 未泄）时重入仍需 PIN = 实际已吊销**；若 PIN 亦泄，正确工具是
  rotate-PIN / `session rm`，evict 从来不是。**修（若将来判为安全缺口 → 升 gotcha #28）**：evict 加 nkey
  denylist 或强制 PIN 轮换才算真吊销。
- **DOC-7（O4 裁定：有意纵深防御，非 gotcha）**：`agent join <forged-invite>` 在**无 `--expect-account-pub`
  且 invite 带 inline `seed=`** 时，会**写下 agent.yaml 残留**（`82` T2 实测 yaml 存在）。**机理**：
  `cmd/tether/agent_join.go:59-73` `brokerURL=inv.Seed` 非空 → `writeAgentConfig` 被调（先于 roster-cache
  预热）。**为何是 DOC 非缺陷**：roster-cache 由 `AdoptDecision` **签名验证门控**（验签失败→`!ok`→不写
  cache，`agent_join.go:75-83`），残留 agent.yaml **无信任价值**、`agent doctor` FATAL 兜底；pin=OOB 信任锚，
  用户不传 `--expect` 即视为信任该 OOB invite。`docs/simcluster-coverage-roadmap.md` line 322「无半写
  agent.yaml」是 **overclaim**（应订正）。**修（若将来判缺陷）**：join 在 manifest-verify-against-pin 通过
  前不落 agent.yaml。
- **DOC-8（§3.0 承重发现）**：auth_callout CONNECT-deny 的**具体 reason 对 client 不可见**。tether handler
  拒（`internal/authcallout/handler.go:400-405` `resp.Error=reason`）→ nats-server 发**通用**
  `Authorization Violation`（nats-server `client.go:2434`）；tether reason（`not a member of session` /
  `invalid PIN for session` / `not provisioned…` / `session not active`）**只 server 侧 log + `$SYS…AUTH.ERR`**。
  `handler.go:117-118` docstring「the client sees a clear auth error」**误导**。非缺陷（有意 info-hiding），
  记之使未来读者**不在 client 断 reason 串**误报 bug——所有 CONNECT-deny drill 臂必配 server-side 判别子。
- **DOC-9（inventory §1.1 row-27 订正）**：`member_joined` 事件**只发 `via:"pin"`**（`handler.go:286` agent、
  `:353` ctl）；已 member 的 **fp-reconnect 路径不发任何事件**（`handler.go:320-330` 返 nil）。清单 row-27
  「事件含 via=pin/fp」不精确 → 订正为「via=pin only；fp-reconnect emits no event」。**80** owns。
- **DOC-10（inventory §1.1 row-26 订正）**：`session_created`/`session_destroyed` 去 **`events` 流**（subject
  `tether.v2.sys.events`，`internal/broker/audit.go:36-48`），**非** `history-<sid>`；`admin audit` 只 tail
  `history-<sid>`（`admin.go:36`），`session_destroyed` 且**后于** history-<sid> 删除（phase ②）→ **`admin
  audit` 看不到它**。row-26「history/admin audit 可见」订正为「events 流可见（member sys.events sub）」。**81** owns。
- **DOC-11（承诺未实现 → probe 钉住）**：`docs/usage.md §5.10`（`:578-583`）承诺 `session rm` 会 broadcast
  `sys.events{type:session_deleting}` 使 agent 主动拒该 sid 新调用——**无 `pubSysEvent("session_deleting")`
  writer**（唯一字面 `internal/broker/cluster_forward.go:255-262` 是 wire error-kind classifier，非广播）。
  **真机制（deploy-tier 实测，R9 订正）**：refusal 全在 **broker 侧**、agent 从不参与——但**分两条路径按
  session 状态**：① session 存在但 **DELETING** → 应用层 `session.IsActive` gate 返 `session_not_found_or_deleting`
  （`exec.go:49-55`/`run.go:41-47`/`expose.go:178-184`/`broker.go:1145-1154`）——N=1 **同步** rm 窗口不可达 →
  **hermetic-only**；② session **已删**（rm 完成后，deploy 实况）→ 下一次 session-scoped CONNECT 在 auth_callout
  `ensureMember` 处被拒（session-not-active → 通用 `Authorization Violation`，`handler.go:317-319`）——**从不到达
  应用层 gate**。**钉住它的**：`81` E3a/E3b/E3c（验 ② 的 CONNECT-deny broker 强制路径；DOC-11 核心「无 agent
  广播」不变）。**修**：正 usage §5.10 / architecture H.1，或加 writer。**次要 drift**：usage §5.10 说「H.5」，
  code 注为「H.3」（`audit.go:53-72`）。
- **DOC-12（architecture H.1 无 writer / 命名漂移）**：H.1 承诺的 `kicked` / `agent_unregistered` /
  `rotated_pin` **均无 writer**——`kicked` 实际发的是 `agent_evicted`（`internal/broker/admin.go:93`，命名
  drift）；`agent_unregistered` 无 unregister verb；`rotated_pin` 无改-PIN verb（PIN 只能 rm 重建）。**修**：
  正 H.1 或加 writer（另立增量）。**80** owns `rotated_pin`；**81** owns `kicked`/`agent_unregistered`。

**harness 注释漂移（sim 脚手架，非产品文档；S2 随批修一次）：**
- **DOC-13**：`image/provision-node.sh` agent 角色注释宣称「Agents onboard via `tether agent join <invite>`」，
  但 sim `cmd_agent_join`（`simcluster:332-343`）实走 `--pin` 首连 + 手写 system unit。S2 已修注释如实说明两条
  onboarding 路径（`--pin` 首连保留 + 真 C2 `agent join` 由 drill 82 演练）。
- **DOC-14**：`drills/lib/agentyaml.sh:14-16` 注释「S0-隧道… is S3's job」——S0-隧道 已在 S2 落地，S2 已改注释
  为「landed in S2」。

**S2 内审顺带发现（R24；产品文档缺陷，非 gotcha）：**
- **DOC-15**：well-known manifest 相对 `cluster seeds publish` 有**至多 30s 新鲜度滞后**——broker 缓存 manifest
  bytes 到 `nextCheckAt = signedAt + manifestRecheckInterval`（`internal/broker/cluster_manifest.go:22`，30s），
  一个在 30s 窗口内的请求会拿到**发布前的 seed-less** manifest，30s 后的请求才重签含 seed bundle。**非安全洞**
  （AdoptDecision 的 generation 单调性使陈旧 manifest 只是「暂无更新」，onboarding 亦不依赖即时新鲜=invite 带
  inline seed）；但 `usage.md`/`cluster.md`/`broker-ops.md` **零 user-facing 说明**（与 #27 同族的 doc 缺口）。
  `82` M1/M2/C1 poll 过该窗口（诚实注记，非静默）。**修**：usage 文档化该滞后，或把它变成 82 的显式 labeled probe。

**预登记指针（roadmap 研究期发现，未在 S1/S2 核实立项）**：
- **DOC-1**：`usage.md §5.15` 尾段「cluster 不支持 proxy」与 C5 现实相悖的残留旧文（S4 核实）。
- **DOC-2**：`recovery diagnose`/`resnapshot` 未入 cluster.md/runbook 命令文档（S6 核实）。
- **DOC-4**：architecture P8 测试原型的「SIGSTOP broker 期间 agent 起进程」经产品路径不可达（S9-94 修订）。

## harness 保真度债（sim 脚手架，非产品缺陷；随批清偿或登记）

- **FD-1（S1 登记，不阻塞）**：sim 烘焙的顶层 `/home/sim/.tether` 是 **0755**，而真 install.sh 建的是
  **0700**（`image/provision-node.sh:65` vs `scripts/install.sh:315-317`）。S1 的传输策略面（allow_roots
  三态）**不读该目录 mode**，故 61/62 不依赖它，S1 不阻塞。`drills/lib/agentyaml.sh` 写的 `agent/<sid>`
  子树是 0700、`agent.yaml` 是 0600 sim:sim（镜像 install.sh 真实形态，Mandate ③）；仅顶层目录 mode
  偏离。**处置**：登记为 §1.4-邻接的保真度债，将来若某批断言依赖顶层目录 mode 再对齐（绝不静默 chown
  烘焙镜像掩盖之，Mandate ①）。

## OQ-2 — `62-remote-fs-safe` 的 NOT-COVERED 定格（feasibility spike 结论）

`62-remote-fs-safe` 是 roadmap OQ-2 的 feasibility spike。**实测结论（2026-07-11，weilandserver）**：

- **可行的部分（GREEN，triage 分支 b = FUSE-approx）**：用一个 `fuse.hangfs` FUSE 挂载（`image/hangfs.py`）
  忠实复现「挂死的网络挂载」——它被 `spawnsafe.classifyFstype`（前缀 `fuse.`）判为 hangable；SIGSTOP 其
  daemon 后 statfs 阻塞，agent 的**有界探针如实快速失败、且不 wedge agent**（实测）：`exec --cwd <死挂载>`
  → `remote_fs_unsafe_cwd`；`exec <死挂载>/abs-argv0` → `remote_fs_unhealthy`；`exec --safe --cwd`
  （mode:off 下 per-call 升级）→ `remote_fs_unsafe_cwd`。三臂 + alive-control（wedge 后普通 exec 仍工作）
  全 GREEN。
- **NOT-COVERED（Arm 2，实测理由）**：**真不可中断-D** 需 kernel nfsd + hard mount；而观察
  **mode:off-WITHOUT-safe 的遗留裸挂死**会驱使 agent 对死挂载做**无界** chdir/exec。两者都会 wedge
  agent / 共享的 weilandserver（实测：wedged FUSE 编排即便有 timeout 也曾挂死宿主 5 min）。本 drill 的
  FUSE daemon 是 **T/S 态、kill-9 可收割**的**近似**（drill 的 D-判别子实测钉住），把它等同真-D 即
  Mandate-① false-GREEN。**留给专用硬件/隔离宿主**，不在共享 sim 宿主上跑。remote_fs 的判定逻辑本身
  hermetic-密（`internal/spawnsafe` 单测），此处部署增量 = 真 FUSE 挂载 + 真 bootHangable 扫描 + 真有界 statfs。
