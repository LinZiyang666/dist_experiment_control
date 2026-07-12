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
