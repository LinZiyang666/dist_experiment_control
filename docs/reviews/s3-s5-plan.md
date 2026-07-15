# `docs/reviews/s3-s5-plan.md` — G-A deploy-tier drill plan (S3 + S4 + S5)

> Finalized by the main process, 2026-07-12. 8 drills, one merged plan, one external review (execution group G-A per roadmap §2.1). Drafted by a 6-lens adversarial Stage-A workflow (6 draft + 6 critique + 1 synth, all Opus 4.8), then main-process adjudicated — §11 records the finalized dispositions of the 5 open items the synthesis surfaced.
> 结构：§0 范围/依赖 · §1 共享 S0+harness+横切规则 · §2 S3(70/71) · §3 S4(72/73/74) · §4 S5(31/30/32) · §5 gotcha ledger · §6 NOT-COVERED · §7 OQ · §8 inventory · §9 run-drills/拓扑/flake · §10 per-drill false-green · §11 主进程定稿裁决。
> 所有 identifier/命令/信号英文；破坏性臂逐条给五要素；一切 rehome/撤销/failover/限流 oracle 收在**真流量恢复**或**对照源对比**，绝不 status 字段。**只测不修：产品缺陷登 gotcha #28+ 并 signature-guarded RED，绝不改产品 Go 代码。**

---

## §0 范围与依赖

**范围 = 执行组 G-A = S3 + S4 + S5，共 8 drill：**

| drill | 批 | N | 主题 | GREEN / EXPECTED-RED | 债 | 预计时长 |
|---|---|---|---|---|---|---|
| 70-expose-journey | S3 | 1 (+1agt+ctl) | expose 数据面全旅程 | GREEN | — | ~3-5min |
| 71-expose-rehome-failover | S3 | 3 (+1agt) | drain-rehome + crash-stranding | GREEN 主体 + **#29 gotcha** | — | ~8-10min |
| 72-proxy-subscription | S4 | 1 (+2agt) | proxy on/off/sub/revoke + SS 双腿 | GREEN (+可能 #30 fallback) | — | ~5-8min |
| 73-proxy-cluster-ha | S4 | 3 (+2agt) | exit rehome + quorum-loss 策略 + cluster revoke | GREEN + **#30 gotcha** | — | ~8-10min |
| 74-rebalance-on-return | S4 | 3 | skew + auto-rebalance | GREEN | G7a m11 (sim 面) | ~5-8min (含 ~180s poll) |
| 31-node-upgrade-fleet | S5 | 1 (+2agt) | 升级白名单/sha/--all | **#28 EXPECTED-RED** + GREEN 负例 | — | ~3-5min |
| 30-rolling-upgrade | S5 | 3 | 滚动升级 followers-first | GREEN (explore→pin whole-host) | G5 #8 | ~8-10min (最重) |
| 32-install-lifecycle | S5 | 1 | install.sh 生命周期 + §8.4 单机升级 | GREEN | — | ~3-5min |

**已落地、复用不重造（S2）：**
- **S0-tunnel**：`agentyaml.sh` 写 `tunnel_addr: <broker>:7000`；`provision-node.sh --domain <node>` 使 `public_host` docker-resolvable。70/71/72/73 一切正向数据面依赖它（无它 agent 空拨 127.0.0.1:7000、curl 必死）。
- **S0-ingress**：per-broker 同-netns python-stdlib TLS 反代 sidecar（`--network container:<brk>`）+ 实例 CA（`drills/lib/ingress.sh`，S2 = mint owner）。72/73 跨容器 `/sub` 走 `https://<brk>/sub/…`；产品 loopback listener **绝不改绑、绝无 `TETHER_DEV_NO_AUTH`**。
- **S0-pty**：`image/pty-run.py`（本组不新用）。

**本组落地（S5 owns）：** S0-artifact、S0-layout、`remote.sh` 双版本构建、若干 harness lib 增量（§1.B）。**零产品 Go diff**（S 批铁律）。

---

## §1 共享 S0 / harness landing + 横切规则

### §1.A 横切诚实规则（绑每一条臂；各 drill 小节只引用、不重述）

- **R-DATAPLANE（#20 铁律）**：rehome / failover / 撤销 / quorum-policy oracle 一律收在**真 curl/SS 字节流恢复**或**对照源在受害期成功**；status 字段（`epoch++`、`explain=moved`、`ps HOME`、`proxy status: ACTIVE`、rehome 事件）**只作佐证判别子，绝不作终点**。视图在死数据面上收敛 = #20 假绿。
- **R-5ELEM**：每条破坏性/混沌/failover/撤销臂五要素齐：① 命名的注入前基线（先证目标活/在场）② 权威观测源 ③ 精确注入边界（打什么、在哪个状态点）④ 语义 post-injection oracle（结果，非退出码）⑤ 无条件清理。
- **R-CONTROLSRC（撤销/限流/quorum）**：拒绝/限制只由「受害者失败 ∧ 独立对照源仍成功 ∧ 窗口/拓扑恢复」三点齐证。「全部都失败」永远不是门存在的证据（与 broker 宕机不可分）。
- **R-SIGGUARD**：本组每条 RED 都是 signature-guarded `assert_bug`/`assert_refuses`，**只为文档化的那个原因通过**；wrong-reason 失败触发 guard → HARD FAIL。`assert_bug` 带 gotcha 号 + flip 条件；exit-0 时 harness 报 "APPEARS FIXED → promote to assert_ok"。
- **R-EXPLORE-PIN**：每个新面断言写成产品的文档化承诺 → 真服务器跑 → GREEN 回归；失败三门：(a) 产品缺陷 → gotcha #28+ + assert_bug RED，(b) harness bug → 本批修（anti-masking，绝不把 workaround 走私进环境），(c) 不可行 → 显式 NOT-COVERED + 理由。**绝无第四扇门「改环境让 tether 过」**。
- **R-CLEANUP-机制（C.0 事实，纠 Draft 6 用词）**：`cmd_drill`（`simcluster:476-494`）是 **INT/TERM trap + 收尾无条件 nuke**（RED/`die`/非零退出照 nuke，仅 `SIM_KEEP=1` 例外），`docker rm -f` 对容器内一切进程（含 SIGSTOP 冻住的）发 SIGKILL。⇒ **容器内故障进程由收尾 nuke 兜底，不需每 drill 显式 EXIT trap**。唯二需显式 trap：(a) 故障卡住容器 teardown 本身（如 62 的 FUSE——S3–S5 无此类）；(b) 臂间需就地回退以免污染后续臂（如 SIGSTOP 后 SIGCONT，correctness 非泄漏）。
- **R-NO-HOST-LEAK（要素⑤唯一真实泄漏口）**：一切后台探针/故障进程（curl 环、nats sub、`ss-local`、background writer、被 SIGSTOP 的 agent）**必须跑在容器内（ctl/agent 容器），绝不在宿主 `weilandserver` 上起**——宿主进程逃过 nuke、污染共享服务器（62 FUSE 挂死宿主 5min 教训）。**写进每条含后台探针的 drill 头注。**
- **Mandate ①–④**：sim 只 provision + expose，绝不 chown/patch/fake 环境迁就 tether；每处不得不手动的特权绕过显式标注为 distinct 步骤并断言其为缺口；一次操作若靠复杂脚本才「成功」= tether 失败被掩盖 = inverted-success 缺陷、应改成暴露。

### §1.B harness lib 增量（放 `drills/lib/`，各 batch 只引用）

1. **`node_kill` / `node_stop` / `node_start`（C.0 事实-3，多 draft 漏）**：`lib/docker.sh` 现只有 `rm_node`（`rm -f`，**销毁容器+卷**）和 `tcp_refused`，**无「杀而保留容器供 `docker start`」原语**。但 **71-C-return / 71-D-return / 74-return** 都要 `kill brkH → 观测 → start brkH`（同容器带持久卷、raft 状态在 `vol_lib`、回来 catch-up 到 VOTER）。误用 `rm_node -f` 则容器销毁、无法 `start`、注入边界不成立。**新增** `node_kill`(=`d kill <ctr>`) / `node_stop`(=`d stop`) 保容器+卷 + `node_start`(=`d start`)；只在「永久杀不回归」的臂（73-B quorum-loss 的两台）才用 `rm_node`。
2. **`ingress_up` 去 manifest 硬编码（E5）**：现 `ingress.sh:70-72` 把 manifest 路由 `--route /.well-known/tether/=…` 写死；72/73 只要 `/sub/` 路由、不做 C2 onboarding、不需 manifest_listen。**重构 `ingress_up` 接受任意 route 集**，72/73 只挂 `--route /sub/=127.0.0.1:8090`。`secrets_mint_ingress` + `ingress_trust_inject` 原样复用。
3. **`proxy.sh`（SS 客户端 helper，OQ-1）**：解析 `curl https://<brk>/sub/<token>` 的 **Clash YAML**（非 SIP008——E2 纠 Draft 6）抽 per-agent-exit host/port/cipher/psk，起 `ss-local -s <exit-host> -p <exit-port> -k <psk> -m chacha20-ietf-poly1305 -l <local-socks>` 后台（**容器内**，R-NO-HOST-LEAK）；trap `pkill -f ss-local`。**依赖两个未烘镜像项，须加 Dockerfile + Stage-B 门**（§7 OQ-1）。
4. **`expose_serve_sentinel`（共享 fixture，CF-5）**：agent 起 `python3 -m http.server` bind 127.0.0.1 服**每-run 唯一 sentinel** `SENTINEL_<drill>_$$`；跨容器 curl body 收口。被 70/71 + 30 写探针共用 → 放 §1 共享 lib，不放 S3 小节（免 S5 耦合到 S3）。
5. **`grow_to_3`（共享 fixture）**：`up --brokers 3 → init brk1 → grow brk2 → grow brk3`（复用 82 建簇路径）。71/73/74/30 共用 → §1 共享 lib，使 §9 concurrent-grow flake 记账准确。

### §1.C S0-artifact（HTTPS 制品库；owner S5；consumer 30/31）

**复用 ingress-proxy.py，不新写 `artifact-server.py`（E4）：** artifact 容器内跑 `python3 -m http.server <port> --bind 127.0.0.1 --directory /artifacts` 作后端，配一个 ingress sidecar（`--network container:<artifact-http>`）+ `--route /=127.0.0.1:<port>` = **零新 TLS 代码**，完全照搬 broker+ingress 模式（ingress-proxy.py 自述 bounded GET-only，30MB tarball GET 落在 scope 内）。

| 生命周期要素 | spec |
|---|---|
| owner / consumer | owner S5；consumer 30（staged 二进制 download+SHA256SUMS verify）、31（broker `upgrade.url_allow` target + 后-#28 agent fetch target） |
| 实例作用域命名 | `ctr_name "$INSTANCE-artifact"` + `-http` 后端，label `sim.instance=$INSTANCE sim.role=artifact`；hostname bridge-resolvable → `https://<inst>-artifact/…` 跨容器 fetch |
| CA / trust | **复用实例 CA**（`secrets_ensure_shared "$INSTANCE"` + `secrets_mint_ingress` 镜像铸 SAN=DNS:`<inst>-artifact`），`ingress_trust_inject` 注入节点系统 trust。**零新 mint/inject 代码**（inventory §3 CA-owner 规则：S2 owns mint，S0-artifact 仅 consume） |
| fresh/perms 预检 | served dir 起空；`docker cp` staged tarball + host-computed SHA256SUMS 入内；**预检断言**：容器内重算 sha256 == SHA256SUMS（torn/stale tarball fail-loud）；只读 |
| health | `-k` readiness poll（错-SAN 负例 front 仍是 running front）+ **另一条 CA-validated（无 `-k`）200 正例** + **un-injected 节点 validated fetch 失败**负例（证 CA 注入而非 `-k` bypass 使其通） |
| cleanup | per-instance 收尾 nuke 按 label 回收；无宿主级外部态（纯容器 teardown，R-CLEANUP-机制兜底） |

**OQ-3 分离（本组 owns，§7）**：CA 解决 **TLS 层**；agent 侧升级白名单（`upgrade.go:76-83`）是**正交的第二堵墙**——即便 CA 可信，agent 仍拒自托管 URL（`url_not_allowed_local`，#28）。CA **绝不**被用来让 31 升级变绿（agent 在 `upgrade.go:83` 拒**先于** `fetchURL`，根本不到 artifact TLS 层）；CA 只让 30 的 staging download 与 31 的 broker 侧 forward 忠实。

### §1.D S0-layout（agent 二进制对齐 install.sh `~/.local/bin`；owner S5；consumer 31）

**已证实载重（E6；实现修正）**：agent unit **四处**（baked `image/units/tether-agent.service:17`、`ident.sh:88`、`agentyaml.sh:101`、**`simcluster` cmd_agent_join:355** — 实现时 grep 全树发现，plan 原记「三处」遗漏了 cmd_agent_join，已补）都 `User=sim` + `ExecStart=/usr/local/bin/tether`（root-owned，Dockerfile:37,42）。`installNewBinary` 做 `MkdirTemp(dirOf(dst))+Rename` 需**二进制父目录写权**；`User=sim` 对 root-owned `/usr/local/bin` 无写权 → **EACCES = 真实部署不存在的假墙**（真实 install.sh agent 装在用户可写 `~/.local/bin`，`install.sh:303 BIN_DIR=$HOME/.local/bin`）。
- **落地**：`provision-node.sh` agent 分支把二进制 copy 到 `/home/sim/.local/bin/tether`(`sim:sim`,0755)，四处 agent-unit `ExecStart` 指向它（使 `os.Executable()` 返回可写路径、原子替换命中真布局）。
- **回归门（E6 已证正确）**：改 ExecStart 触及 `ident.sh`/`agentyaml.sh`/baked unit 三个共用模板 → **落地前重跑 61/62/80/82 验 verdict 不变**（layout-agnostic 但 unit 路径变了）。
- **边界（勿统一套用）**：30 的 colocated agent 保持 broker 的 **root-owned `/usr/local/bin/tether`** + re-exec-only（`syscall.Exec` 不需目录写权，§7 OQ-6）；**不要**把 `~/.local/bin` 套到 30。

### §1.E `remote.sh` 双版本构建（`vendor/tether-next` / `SIM_VERSION_NEXT`）

现 `remote.sh:27-55 stage_binaries` 只 build 一份。**加** `SIM_VERSION_NEXT="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}"` + 第二次 `make build VERSION=$SIM_VERSION_NEXT` → `vendor/tether-next`（**同源、同 proto/commandVersion/schema，只 bump version 字符串**——g5 rolling-safe 硬前提）；打 tarball + 算真 sha256 写 S0-artifact SHA256SUMS（须加进 `stage_binaries`，现只 build+cp、未打包）。version-string-only delta 足以驱动 `node ls --brokers` skew（渲染 `ReleaseVersion`）+ whole-host + mid-roll skew + PID-preserving re-exec。

### §1.F 复用确认

- `/sub` 反代路由：`ingress-proxy.py:25` 已预留 `--route`；`/sub` 默认 serve-ready = **GREEN 回归**（`install.sh:544 listen:"127.0.0.1:8090"` active，非 #27-sibling——E5 裁 Draft 2 对），但保留 Stage-B 门：证 sim broker unit 未传覆盖性 `--sub-http-listen ""`（`serve.go:263`）。
- proxy exit 端口跨容器可达继承 S0-tunnel，但**只 expose 数据面被 81 活体证过、proxy exit 端口没有** → 列 S4 Stage-B 显式前证（E8），别当既成事实。

---

## §2 S3 — expose 数据面 + rehome/failover

### drill 70-expose-journey（GREEN，N=1 + 1 agent + ctl）

SETUP：`up --brokers 1 --agents 1 --ctl 1 → init brk1 → session lab --pin → agent-join agt1 → agent_provision_yaml agt1 lab nats://brk1:4222 open`；`expose_serve_sentinel`（每-run 唯一 `SENTINEL70_$$`）。

| 臂 | oracle / 签名 | 注 |
|---|---|---|
| **J1 data-plane 铁证** | ctl 容器 `curl http://brk1:<port>/SENTINEL70_$$` 返回**精确 sentinel 体**（端到端穿隧道，`grep -q "$TOKEN"`，非 TCP-connect）；port 从 `--json \| jq -r .port`（R2，绝不 grep `2>&1` 合并行） | — |
| **J2 ps 无 HOME 列** | `ps -a` PORTS 表头 = `NAME NODE LOCAL PUBLIC STATE CREATED`（6 列、**无 HOME**，单 broker `HomeBroker==""` → showHome=false）；**假绿守卫**：断言 HOME 列存在即错 | — |
| **J3 explain N=1 形态** | `expose explain web` → `home: (single broker / un-homed)`、`rebuild: on`、epoch 0、footer `rehome events/last_error/…: not yet recorded (planned B5)`；**断 footer 在场、不断值**（B5 deferred，NOT-COVERED-in-product） | — |
| **J4 `--remote-port` 成功** | `--remote-port <band 内 free>` → curl 该精确端口返回 sentinel | band 默认 14000-14999 |
| **J5 port_taken** | 对已占端口再 `--remote-port <same>` → `assert_refuses "port_taken"` | hard fail 无 fallback |
| **J6 port_out_of_band** | `--remote-port 15500`（band 外）→ `assert_refuses "port_out_of_band"` | round-trip 回 broker 校验 |
| **J7 name_taken** | 同 `--name web` 二次 → `assert_refuses "name_taken"`（short-circuit 于 port 检查前） | — |
| **J8 on_broker_single_mode** | `expose --on-broker brk1` → `assert_refuses "on_broker_single_mode\|single-node"` | N=1 专属负例；71 结构不可达（§6 归属） |

**J9（破坏性五要素）`expose rm` → 断流 + 端口即复用**
- ① 基线：curl `brk1:P` 回 sentinel + `ps --json` 有 `web` 行（先证数据面活）
- ② 观测源：ctl 容器 curl（真隧道字节）+ `ps --json` port-alloc 行
- ③ 注入边界：`expose rm agt1 --name web`，注入点={expose live, curl 通, 行在}
- ④ 语义 oracle：`curl brk1:P` → **connection-refused（curl exit 7，`_c_port_refused` 式，非 `! curl -sf`——R1 半开 listener 4xx/5xx 会骗过 `! curl -sf`）** + alloc 行消失；随后 `--remote-port P` re-expose 后 **curl `brk1:P` 返回 sentinel（F6 纠：收在真流量，非 expose exit-0——bound≠forwarding）**
- ⑤ 清理：新 expose 由 nuke 收
- 控制对比：受害=旧 curl exit 7；恢复=同号口 re-expose 后 curl 回 sentinel

**J10（破坏性五要素）agent restart → state.json token 重连（P6 真栈）**
- ① 基线：curl 回 sentinel + 记 epoch E0
- ② 观测源：ctl curl + `expose explain --json`
- ③ 注入边界：`systemctl stop tether-agent`（**精确=agent unit，非 broker**）→ **断言 curl exit 7**（证隧道 agent-driven）→ `systemctl start`
- ④ 语义 oracle：curl 回**同一 sentinel** ∧ port==P ∧ **epoch==E0**（证经 state.json token **重连**同一 expose、无 re-expose；**假绿守卫**：若 agent 重注册后新建 expose 会有新 port/新 epoch，curl 也通但非 P6 语义——必须断 port==P ∧ epoch==E0）
- ⑤ 清理：unit 自恢复；nuke
- **假绿注**：裸 `systemctl restart` 可能快到 curl 从不掉线（frps 公网行残留 ~5s）→ 必须 STOP→refused→START 三段。

### drill 71-expose-rehome-failover（GREEN 主体 + **gotcha #29**，N=3 + 1 agent + ctl）

> **承重重构（CF-2 / C1 / F1 / MF-5，源码定案，Draft 1 对、Draft 4/5 错）**：roadmap §3 prose 与 Draft 4/5 把「docker kill home → 常规 expose 自动 rehome 到 survivor」当 GREEN——**源码反驳**：`rehome_events.go:52-53` 逐字「regular exposes are NOT auto-rehomed on a crash — stranded until a drain/return」；`PlanReassignHome` 对常规 expose 唯一调用点 = `clusterdrain.go:742 migrateExposes`（graceful drain，`:670` 注「rebuild-ON exposes」）+ retire op controller；crash-reaper `proxy_reconcile.go:288` 只 rehome `__proxy__`。⇒ **crash 下常规 expose（含 rebuild-ON）被 STRANDED，正向 rehome 只发生在 `cluster drain`**。承诺 crash-rehome 的产品文案确在：`expose.go:136`（`--no-rebuild` help "default: it auto-rehomes"）、`expose.go:271`（explain 运行时输出 "rebuild: on — auto-rehomes to a survivor broker if the home dies"）、`usage.md:792/820` → **假承诺出现在 CLI 自身运行时输出 → 倾向 gotcha #29**（主进程可改判 DOC-n）。**废弃 Draft 4 FG-71a/c、Draft 5 71-A/71-D-baseline/71-B-#29候选**（后者是伪命题：crash 下连 rebuild-ON 都不迁，`--no-rebuild` 不迁毫不特殊）。

SETUP：`grow_to_3` → session/agent-join/`agent_provision_yaml` + `expose_serve_sentinel`。**关键拓扑决策：expose 的 home 一律 `--on-broker <非-leader voter>` 钉住**，使 leader 存活作 `broker_down_rehome_summary` 的 leader-gated 权威发射者（`observability.go:167`）。

| 臂 | oracle / 签名 | 五要素 |
|---|---|---|
| **A `--on-broker` 负例** | `expose --on-broker <bogus>` → `assert_refuses "on_broker_unknown"` **且事后 `ps -a` 无该 name 行**（不止错误串） | — |
| **B SUPPORTED rehome via DRAIN（正向旅程，锚 drain）** | pin home=非leader brkH，curl brkH:P 通 → `cluster drain brkH` → rebuild-ON expose 迁到另一 eligible VOTER brkT：**终点 = curl brkT:P 返回精确 sentinel** ∧ curl brkH:P exit 7；epoch++/`home_reassign_started→succeeded`/`expose_rehomed`(单 JSON 行 bind kind:expose+name+from/to) **仅佐证** | ①drain 前 curl brkH 通+explain epoch0 ②leader `expose explain --json`+sys.events ③`cluster drain brkH`(graceful,brkH 存活,非 kill) ④curl brkT 返回 sentinel ⑤nuke+杀后台 sub |
| **C CRASH-STRANDING（#29，INVERTED assert_ok）** | fresh re-expose pin home=非leader brkH → **`node_kill brkH`**（保容器）→ 复合 gap 谓词：**curl EVERY 幸存 voter 的 :P → 全 exit 7（数据面 cluster-wide 死，F5 纠：不止 curl 死 home）** AND epoch 未变 AND 无 `home_reassign_succeeded` AND **`broker_down_rehome_summary{exposes_stranded>=1}`**（anti-vacuous 硬绑：证 leader 已 SEE crash，否则「未 rehome」与「crash 未检测」不可分）→ return-recovery：`node_start brkH` → agent 重拨**同一** home（state.json HomeBrokerAddr）→ curl brkH:P **原端口/原 epoch** 恢复 sentinel | ①kill 前 curl brkH 通+stranded=0 ②leader sys.events + explain --json ③`node_kill brkH`（先 `tcp_refused brkH 7400` 证死）④复合 gap 谓词 ⑤`node_start brkH`+nuke |
| **D rebuild-OFF + crash：随 home 一起 DOWN** | fresh `--no-rebuild` pin home=brkH → `node_kill brkH` → curl **任一 voter** exit 7 + explain 诚实仍 `rebuild:off`/home=brkH(死)/无 moved + 无 `home_reassign_started`(该 name) | GREEN（匹配代码：rebuild-off 从不迁）；①curl brkH 通 ②explain --json ③node_kill ④curl 全 refused+未 moved ⑤node_start+nuke |
| **E DRAIN 拒静默迁移 rebuild-OFF** | live brkH 上 rebuild-OFF expose → `cluster drain brkH` → `assert_refuses "rebuild-OFF .* NOT be auto-migrated\|will NOT"`（`clusterdrain.go:665-667`）；**对照**：另建 rebuild-ON expose homed brk3 → `cluster drain brk3` 正常迁（证「rebuild-OFF 触发的拒绝」而非「drain 全局坏」——F1/C.1 纠 Draft 4 FG-71c：crash-path 对照源无效，唯一有效对照 = drain 路径拒绝对比） | 控制对比臂 |
| **F rehome_stalled{no_eligible_target}（共享 74，可选）** | 降级拓扑（retire/kill 其余 voter）使无 eligible VOTER → `cluster drain brkH` → `rehome_stalled{kind:expose,reason:no_eligible_target}`（`clusterdrain.go:727-735`）+ `assert_refuses "no other eligible VOTER"`；**若 N=3 干净造不出 → NOT-COVERED（附理由，非 gotcha）** | compound 注入 |
| **G home 回归后粘性（#18 默认关）** | 承 **B（drain-rehome，非 crash——纠 Draft 5 71-D baseline）**：expose 已在 brkT → `node_start` 被 drain 的 brkH → expose **保持** brkT + curl 仍 brkT 通 + 无回迁事件 | GREEN；post-**drain** 粘性 |

`broker_down_rehome_summary` 归属 = **S8-90**（71 仅 observe-only，其 `exposes_stranded` 为 C 臂 anti-vacuous 提供权威证据）。

---

## §3 S4 — proxy subscription + HA + rebalance

### §3.0 Keystone（**E1 / D-load-bearing 未闭合，见 §11-U1**）

**KD-72-topology**：`simcluster init` 把 N=1 也切成 1-voter **cluster** raft（`clusterMode=true`），而 `repairProxy`（`proxy_keyset_changed` 唯一 writer，`proxy.go:665`）在 `if b.clusterMode { return }`（`proxy.go:688`）后——即 **init'd N=1 上 single-mode `proxy_keyset_changed` 永不触达**。Draft 2 主张 72 跑 true single mode（`up`，NO `init`）以覆盖它；**但 critic E 证据倾向证伪**：现有 12 drill 无一跑 un-init broker，`init` 正是给单机 broker bootstrap account/auth_callout/JS 的那步，un-init broker 大概率连 `session create` 都起不来。
- **处置**：列为 **S4 Stage-B 头号 spike**——先证 un-init broker 能否 serve-ready（session create + agent join --pin 到 ONLINE）。
  - 若能 → 72 = true single mode，覆盖 single-mode `proxy_keyset_changed`（GREEN）。
  - 若不能（预期）→ 72 = init'd cluster-mode N=1，single-mode `proxy_keyset_changed` 行 → **NOT-COVERED-in-sim（hermetic 已覆盖）**，且「cluster-mode revoke 不发 `proxy_keyset_changed`」= **#30**（inverted assert_ok），落在 73（cluster）或 72 的 cluster-fallback。
- **两个数据面别混**：`/sub` **config fetch**（GET Clash YAML）走 loopback `sub.listen` + S0-ingress HTTPS；**SS 出口流量**走 broker frp public port（`14000-14999`）→ yamux → agent SS server(127.0.0.1) → target。SS 流量**不**走 ingress TLS sidecar。

### drill 72-proxy-subscription（GREEN，N=1 + 2 agent + ctl）

SETUP：见 §3.0（模式待 Stage-B）；agent-A `agent.yaml` 显式 `proxy.allow_private_destinations: true`（高危 config，`cmd/tether/agent.go:73`，头注抄高危说明），agent-B 默认（`DenyPrivateDestinations`）；RFC1918 sink = 在**已有** agent/ctl 容器桥 IP 上跑 `python3 -m http.server`（桥 IP 天然 RFC1918，**免新容器**——E4）。

**Arm O — owner-only + member-readable（无泄漏）**
- owner `proxy on --yes` → `PROXY: ON` + `proxy_enabled` 事件；member T `proxy on --yes` → `assert_refuses "not_owner"`（`proxy.go:568`）；member T `proxy status` 可读（member-gated）+ **payload 无 PSK/token**（`redactSubs`）。
- **G3 补**：非-PTY `proxy on` **无 `--yes`** → `assert_refuses "aborted.*--yes"`（`proxy.go:108-110` `confirmProxyOn` 真交互门）。

**Arm C — sub create 生命周期 + 同名冲突（explore→pin）**
- `proxy sub create --name alice` → SubURL 打印**一次**；`sub ls` alice ACTIVE、无 token。
- 重复 `--name alice` → **explore→pin**：`assert_refuses "sub_name_taken"` AND 第一 token 仍 `/sub`-200（无静默 rotate）；若静默 mint 第二 token/rotate PSK → finding（pin RED）。

**Arm SUB — `/sub` body + 伪 token 负例**
- loopback handler：`curl http://127.0.0.1:8090/sub/<alice-token>` → 200 + Clash YAML **恰 2 个 `ss` proxy**（agent-A/B），各 `server:/port:` == 真 `EXIT host:port`（jq/yaml 断节点数 + host/port 相等，cross-check `proxy status` NODES 与实际可 curl 的 frp public port）。
- 跨容器 ingress：ctl `curl --cacert <inst-CA> https://brk1/sub/<alice-token>` → 同一签名 body。
- **伪 token 负例**：`curl https://brk1/sub/deadbeef…` → **精确 404 `not found`**（`subhttp.go:87-96`），与 **revoked** token 的 404 **字节等同**（无 existence oracle）；control：valid token → 200。

**Arm SS — 真 SS 流量双腿（OQ-1）**
- **SS-pos（agent-A，allow_private=true）**：`ss-local`→agent-A exit → `curl --socks5-hostname` RFC1918 sink → **200 + body bytes**（正向数据面；桥内目标全 RFC1918，故正向腿**必须**骑 allow_private=true agent，此分工由 mandate 钉死、非过度供给）。
- **SS-neg-privdest（agent-B，默认拒）**：同一有效 PSK → agent-B exit → 同 sink → curl **失败**，权威 oracle = agent-B **时间窗内**（F8：`journalctl --since "$T_inject"`/cursor，防匹配 stale 行）journald `ssproxy: blocked non-public destination target=<ip>`（`server.go:521`）——证 AEAD 成功、block 是 dest-policy（隔离唯一变量 = allow_private）。
- **AEAD-neg-wrongpsk（agent-B）**：错 PSK → curl 失败 AND **无** blocked-dest 日志（trial-decrypt 全败 → drop，`server.go:485`，在 destAllowed 之前）——blocked-dest 日志的**在/不在**判别 AEAD-failure vs dest-policy（假绿防线）。

**Arm REV — `sub revoke alice`（撤销，破坏性五要素 + 控制三元）**
- ① 基线：alice（agent-A）+ bob（第二 sub）**两条持久 SS 会话在飞传字节**（先证两活），`sub ls` 均 ACTIVE、`/sub/<alice>`/`/sub/<bob>` 均 200
- ② 观测源：SS 客户端字节流状态 + 新连接 AEAD 门 + `sub ls` state + `/sub` http_code；`proxy_keyset_changed` **佐证**（single-mode writer；cluster mode 见 #30）
- ③ 注入边界：`proxy sub revoke alice`（两连接在飞时）
- ④ 语义 oracle（poll_until，async 经 reaper re-push directive→SetKeys→force-close）：alice 在飞连接**被断** + alice 撤销-PSK **新连接被拒**(AEAD 门) + `/sub/<alice>`→404 —— **WHILE** bob 在飞**不受扰** + bob 新连接仍 relay + `/sub/<bob>`→200
- ⑤ 清理：trap `pkill ss-local`（容器内）+ nuke
- **控制三元**：受害=alice 断+新连拒；**独立对照源=bob 全程传字节**（证按-token 作用域撤销、非 proxy 整崩——「全断」是此处最危险假绿）；**恢复=`sub create alice2` 新 token 新连接能连**（证撤销未误伤 proxy，Draft 5 补，采纳）
- **OQ-1 降级风险**：若 OQ-1 落「AEAD 门+TCP 连通」无常驻 SS 客户端档 → 「在飞断」无从观测 → **降 NOT-COVERED（hermetic 已测撤销传播），只保「新连拒+bob 不受扰」两点**；先定 OQ-1 档再定本臂。

**Arm OFF — `proxy off` 全停 + 端口回收（破坏性五要素）**
- ① 基线：proxy ON，两 agent serving，`/sub` 2 节点，SS 在传，frp 公网口 relay
- ② 观测源：`/sub` 节点数 + frp 公网口可达（R1 exit 7）+ `proxy status` NODES + port_allocations
- ③ 注入边界：`proxy off`
- ④ 语义 oracle（poll_until）：每 agent SS server 停 → frp 公网口 **不再 relay（curl exit 7，非 `! curl -sf`）** + `/sub` 渲染 **0 个 ss 节点（DIRECT fallback，E3——非空 body/非 404）** + `proxy status: OFF` + `__proxy__` 端口回收 + `proxy_disabled` 恰一次
- ⑤ 清理：nuke

### drill 73-proxy-cluster-ha（GREEN + **gotcha #30**，N=3 + 2 agent + ctl）

SETUP：`grow_to_3` → S0-ingress on each broker（mint leaf + `/sub` route + trust inject）→ `proxy on --ha-policy freeze-on-quorum-loss`（session lab）；跨 voter 布置 distinct homes。

**Arm REHOME — kill exit 的 home broker → auto-rehome（C5；破坏性五要素）**
- ① 基线：agent-X 的 `__proxy__` home = 特定 voter brkH；`proxy status --cluster` X home=brkH reason=ready；`/sub` 渲染 X `server=brkH.public_host`；ss-local→`brkH:portX`→RFC1918 sink 200
- ② 观测源：`proxy status --cluster`（per-agent HOME/REASON，`proxy.go:405-425`）+ `/sub` `server` 字段 + **真 ss curl** + sys.events；**C.2 补：post-kill `/sub` refetch 必须经 survivor broker 的 ingress**（死 brkH 的 ingress 同-netns 已亡，走它会 000/refused=假 RED）
- ③ 注入边界：`node_kill brkH`（先 `tcp_refused` 证死）
- ④ 语义 oracle（poll_until）：X 迁到 survivor brkS——`proxy status --cluster` X home=brkS reason=ready **且标 brkS 自己的 public_host**（G7#2；**从 query-broker≠brkS 处查**，使「总显示被查 broker host」回归 RED）；`/sub server`=brkS.public_host；**终点 = ss-local 重指 `brkS:<newExit>` curl sink 成功（真流量恢复）**；捕 `proxy_node_unready`(`proxy_reconcile.go:303`) + `home_reassign_started/succeeded` + no-ready 窗内 `sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial`（**按 kind 匹配、payload 无 reason 字段**，inventory §1.4 警告）
- ⑤ 清理：被 kill 容器 nuke 收；`pkill` ss-local；`ingress_down`；trap

**Arm CLUSTERVIEW — `proxy status --cluster`**
- 渲染 `NID STATUS REASON HOME EXIT`（reason ∈ {ready,keyset_stale,catching_up,tunnel_down,no_home}）+ `CLUSTER: <state> ha-policy=… writable=…`；member-readable、无泄漏。

**Arm REVOKE-cluster（#30，MF-4 补，INVERTED assert_ok）**
- cluster-mode `proxy sub revoke <name>` → revoke **有效**（epoch bumped / 在飞连接 force-close / `sub ls` REVOKED）**AND** 时间窗内 sys.events **无 `proxy_keyset_changed`**（cluster 路径 `handleProxySubRevokeCluster`(`proxy_cluster_wire.go:92-104`) 从不 emit；唯一 writer 是 single-mode `proxy.go:665`）→ **gap = #30**。**FLIP**：cluster revoke 补 emit 后翻 `assert_ok "…proxy_keyset_changed present"`。

**Arm QUORUM — quorum-loss freeze vs disable（破坏性五要素；最刁钻控制纪律；采 Draft 5 分离断言、弃 Draft 2 optional）**
- ① 基线：两 session——lab(freeze-on-quorum-loss)、ops(disable-on-quorum-loss)，各 live token；**至少一个 exit 显式 homed on 将存活的 survivor**（否则 freeze 无可服务子集，Draft 5 前提，Draft 2 缺）；**先证 survivor tether-broker 杀后仍 active**（要素①硬前证——否则 disable→404 可能是 broker 死而非策略，假绿；README #23 `MaxReconnects(-1)` 留活）；两 `/sub` 200、survivor-homed exit SS 腿在传
- ② 观测源：`/sub/<token>` http_code + body + survivor-homed exit SS 字节 + lab 的 control **write**（`proxy sub create`）
- ③ 注入边界：`node_kill` **两台 voter**（杀至 1/3，失 committable quorum；`tcp_refused` 证两死；**别杀 survivor**）
- ④ 语义 oracle（同一存活 broker 上）：
  - **freeze(lab)**：`/sub/<lab-token>` **仍 200 + frozen 快照**（`FROZEN_READONLY`,Vendable=true,`proxy_cluster.go:45`）AND **control write（lab `proxy sub create`）被拒 `proxy_frozen_readonly`/no committable quorum**（write-refused-while-read-serves 证真 quorum-lost、非 healthy）；**且 R-DATAPLANE 强制分离断言（F3/MF-3）**：**survivor-homed exit SS 腿仍传字节** WHILE **死-homed exit SS 腿黑洞**（quorum 失 → `PlanReassignHome` 无法 commit → 死-homed 无法 rehome；frozen body 仍列死-homed exit，故「/sub 200」≠「数据面活」——绝不以 `/sub` 200 单独收口，那正是 `Vendable`=「/sub 读门」定义、非数据面）
  - **disable(ops)**：`/sub/<ops-token>` → **404**（`DISABLED_NO_QUORUM`,Vendable=false,`subhttp.go:106-108`）；control = freeze-session 仍 200（证 404 是 HA 策略、非 broker 死/网络故障）
- ⑤ 清理：两 kill 容器 nuke 收；`pkill` ss；`ingress_down`；trap
- **控制三元**：freeze 受害=死-homed 腿黑洞；对照源=survivor 自身 `/sub` 200 + survivor-homed 腿传字节；对照臂=disable 404

### drill 74-rebalance-on-return（GREEN，N=3；G7a m11 sim 面）

> **D2 债说明**：g7-review m11 有三交付——(1) env 入运维文档 (2) hermetic code↔doc drift guard (3) 本 sim drill。**74 只闭 (3)**；(1)/(2) 是 G7 的活（`7a16f72` 已 land 或欠 doc 增量，S 批不改 doc）。74 不是 (1)/(2) 的 fix。

SETUP：`grow_to_3`，proxy on，≥3 exit homed **one-per-voter**（max−min=0），`cluster status --homes` 记初始分布。

**Arm SKEW — build skew（破坏性五要素；break-before-make）**
- ① 基线：homes one-per-voter + 各 SS 腿在传
- ② 观测源：per-voter HOME counts + **每 exit `/sub` 指真 home 的 curl**（g7-plan L133 硬要求，数据面真相，非 status 计数）
- ③ 注入边界：`node_kill brkA`（≥1 exit 的 home）→ poll exits rehome to survivors → **`node_start brkA`**（保容器）→ poll ONLINE+VOTER
- ④ 语义 oracle：分布**倾斜**（brkA 载 0，brkB/C 载 rehomed，max−min≥2）+ rehomed exits `/sub server`=新 home + SS 通（stickiness=好性质，reaper 只 DOWN 时 rehome、不 return 回迁）
- ⑤ 清理：nuke；trap（break-before-make 黑洞窗 poll 宽容，OQ-D）

**Arm A — default-off 保持 skew**
- `TETHER_AUTO_REBALANCE` unset（`autoRebalanceEnabled()` false）→ poll ≥ dwell+quiet-window → skew **持续**（max−min≥2 不变）AND sys.events **无 `proxy_auto_rebalanced`**（断绝对缺席）

**Arm B — `cluster rebalance proxy` manual verb**
- `--dry-run`（admin socket on leader，`sudo -u tether`）→ 打印 `rebalance proxy (would move N of M … (dry-run))` AND **零改动**（`cluster status --homes` 前后字节相同、`/sub` homes 不变、无 `proxy_auto_rebalanced`/`home_reassign_*`——zero-write oracle）
- 实跑 → **均摊 max−min≤1** + per-move `home_reassign_started/succeeded`+`proxy_node_unready`（`proxy_rebalance.go:181`）+ **NOT** `proxy_auto_rebalanced`（manual≠auto）+ **数据面 close**：ss-local 重指各 moved exit 新 home curl sink 成功

**Arm C — `TETHER_AUTO_REBALANCE=on` auto-rebalance-on-return（破坏性五要素）**
- ① 基线：skew（max−min≥2）+ env=on **live 验证**（`systemctl show tether-broker -p Environment` 含 `TETHER_AUTO_REBALANCE=on`；ENV 入每 broker unit，**不入 broker.yaml**，KD-3b；labeled operator 步）+ 各 SS 腿在传
- ② 观测源：per-voter HOME counts + **`proxy_auto_rebalanced` 精确计数** + `/sub servers` + ss curl
- ③ 注入边界：brkA 的 return edge（broker_down→clear、仍 current voter）
- ④ 语义 oracle（poll_until，宽容——见 timing）：分布**均摊 max−min≤1** AND **恰一次 `proxy_auto_rebalanced`**（count==1，非 0 非 ≥2——cooldown 防 re-fire，F9 反 flap）AND **数据面 close**：ss curl 经均摊后新 home 恢复
- ⑤ 清理：`unset`/重启回 default + nuke；trap
- **Timing 注（OQ-7，宽 poll，无精确秒）**：auto-fire 等 `autoRebalanceReturnDwellTicks=6`≈30s **加** `autoRebalanceQuietWindow=60s`（因 brkA 死刚 rehome 盖 `last_rehome_at`）→ 实测 ~60-90s，**poll≥180s**（74 = S4 最长 drill，头注标 timing budget）
- **控制对比（inv-5）**：加一个 co-homed 普通 expose，断它**不被** auto-rebalance 选中（`__proxy__`-only）

---

## §4 S5 — 升级与版本运维

### drill 31-node-upgrade-fleet（**#28 EXPECTED-RED** + GREEN 负例，N=1 + 2 agent + ctl）

SETUP：N=1 broker（init）+ 2 agent（`bind_agent` 入同一 session lab，**S0-layout 后二进制在 `~/.local/bin`**）+ ctl；env 起 S0-artifact；**broker.yaml `upgrade.url_allow` 含 `https://<inst>-artifact/`**；`ART_URL=https://<inst>-artifact/tether-<ver>.tar.gz`，`ART_SHA=<tether-next 真 sha256>`。

**Arm #28（assert_bug，钉 agent 白名单不可配）**
- **前置正例（FG-31b 三点判别子，使 `url_not_allowed_local` 是唯一可能墙）**：(i) agent 容器 `curl --cacert <inst-CA> $ART_URL` → 200 + sha 正确（证 reachable+trusted，排除 download_failed/CA）；(ii) 断 broker `upgrade.url_allow` 含该 URL（排除 broker `url_not_allowed`）；(iii) `node ls` agt1 ONLINE+owner=caller（排除 node_offline/not_owner）
- **RED**：`assert_bug "self-hosted mirror upgrade (broker allowlisted) 被 agent 本地白名单拒" "#28" "url_not_allowed_local" -- CTL node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA"`
- 机理（源码）：`agent/upgrade.go:48` `defaultAgentURLAllowlist=["https://github.com/LinZiyang666/…/releases/"]`；`:76-83` 空则回退默认 + `url_not_allowed_local`；`agent/agent.go:113` `UpgradeURLAllowlist` **唯一 writer = `serve.go:186`（broker 侧）**，`tether agent` daemon flag 集无 `--upgrade-url-allow`、无 agent.yaml 键、无 env → agent 白名单**恒等于**硬编码 GitHub 前缀
- **假绿守卫**：签名 `url_not_allowed_local` **含 `_local`**；broker 侧 `url_not_allowed`(无 `_local`)、`download_failed`、`sha256_mismatch`、`not_owner` 均触 guard → HARD FAIL（wrong-reason 不假过）
- **FLIP**：agent 白名单可配落地后 → assert_bug 检出 exit 0 → APPEARS FIXED → promote `assert_ok`（真升级成功），并解锁 §6-B 的 gated 臂
- **DOC-3（H1，纠 Draft 3）**：`error_hints.go:34` + `usage.md:1443` 指向不存在的 agent `--upgrade-url-allow`——**DOC-3 已由 S1 command-tree golden hermetic 钉住**；31 **不重测 flag 缺席**（hermetic-owned，§0.3 depth gate），只在 #28 RED 头注一行 cross-ref。**禁**用 `/etc/hosts` 伪 github + 自签强推绿（Mandate ①）。

**GREEN 负例（与 #28 独立可达）**
- **broker `url_not_allowed`**：`https://evil.invalid/x.tgz` + **合法 64-hex sha** → `assert_refuses "url_not_allowed([^_]|$)"`（F7：锚定非 `_local`；且用合法 sha 防更早的 `sha256_invalid` gate 抢答）
- **broker `sha256_invalid`**：任意 URL + 畸形 sha（非 64-lowercase-hex）→ `assert_refuses "sha256_invalid"`（`broker/upgrade.go:98`，**先于** url 检查 `:103`）
- **not_owner**：非 owner member `node upgrade` → `assert_refuses "not_owner"`（`:64`，先于 url/sha）

**Arm `--all` A-dispatch-skip（GREEN，破坏性五要素；采 Draft 3 冻**全部**——纠 Draft 4/5/6 冻一个）**
- **为何冻全部（F2/C4/MF-7 源码定案）**：`node.go:322 isConfigError` 以 `strings.Contains(msg,"url_not_allowed")` 匹配，而 `url_not_allowed_local` **含该子串** → 任一**存活** agent 命中 #28 → `isConfigError` true → `node.go:221 return err` **直接 abort `--all`**，`(N node(s) skipped…)` 汇总（`:237`）**永不打印**。只冻一个则另一存活者触 abort = 假 RED。
- ① 基线：agt1/agt2 均 ONLINE（`node ls`）+ 证可 dispatch
- ② 观测源：ctl stdout/stderr（`skipped (transient)` 行 + `(N node(s) skipped…)` 汇总）+ `node ls` 心跳态
- ③ 注入边界：`SIGSTOP` **两个** agent（容器内 `pkill -STOP -x tether`，**在 5s ONLINE 心跳窗内**，freeze 后立刻 `--all`——`listOnlineNIDs` 仍枚举二者）→ dispatch 期无应答 → broker `nc.Request` 超时 → `agent_no_responders` → client `isTransientError` → transient skip
- ④ 语义 oracle：stderr 恰 2 条 skip 行 + `(2 node(s) skipped due to transient errors)` + 命令**退 0**
- ⑤ 清理：**trap 无条件 `SIGCONT` 两 agent**（`pkill -CONT -x tether`，臂间 correctness——冻住的进程虽 nuke 会收，但跨臂停住会污染后续 `node ls`）

**Arm `--all` A-enum-exclude（GREEN）—— 枚举期排除边界**
- 停 agt2 unit + poll 至 broker 视图 agt2 OFFLINE；agt1 ONLINE → `--all` + **broker-allowlist 外** URL → `listOnlineNIDs` 只枚举 [agt1] → agt1 → broker `url_not_allowed`(正确行为、#28-无关) → abort
- oracle：refuses 且 stderr/stdout **通篇无 agt2**（枚举期排除，非 dispatch-time skip；对照 A-dispatch-skip 钉「enumerate exclude vs dispatch skip」语义分界）

**Arm `--timeout`（G2 补，thin）**：freeze 一 agent（仍 ONLINE 窗内）→ `node upgrade <nid> --timeout 2s` → client 侧 deadline 触发（区别于 broker no-responders）；或若不可稳定区分 → NOT-COVERED（hermetic parse-only）+ 理由。

### drill 30-rolling-upgrade（GREEN，N=3；G5 #8 债）

> **状态翻转声明（Draft 4 结尾，采纳）**：G5 `cluster upgrade` verb 已 land（`7a16f72`：`cmd/tether/cluster_upgrade.go`/`cluster_upgrade_drive.go`/`internal/broker/reexec.go`/`node_versions.go` 在树）→ 30 = **real-GREEN 回归**（把 g5-plan §3.3 的 signature-guarded RED 依 g5-review #8「once B1 lands, flip to GREEN」翻绿），**非** RED-for-missing-verb。

SETUP：`grow_to_3`；**每 broker host 一个 colocated agent**（OQ-6，§7）；`remote.sh` 双版本 current=`v-cur`/staged=`v-next`（同 proto/command）。

**特权 staging（MF-2 铁律：显式 distinct 步、绝不显得更自动；采 Draft 6 install.sh 路径）**
- **`[env: PRIVILEGED PRECONDITION — operator stages new binary; cluster upgrade does NOT do this]`**：经**真 `install.sh --url <artifact> …` root download+verify**（consume S0-artifact）**或** labeled operator 步（g5-plan §0.1）把 `v-next` 落到每 broker host root-owned `/usr/local/bin/tether`（真实布局：broker+colocated agent 均 `User=tether`，都写不了 root-owned bin dir = keystone 特权墙）；此步单独成步 + 断 `sha256sum /usr/local/bin/tether == $EXPECT_SHA`
- **反「过度自动」断言**（证 `cluster upgrade` **不**替你 stage/backup）：无 `--account-seed` → `assert_refuses "--account-seed .* required"`；无 `--expect-sha256` → 拒；无 `--backup-taken` → `assert_refuses "take a verified .* backup .* FIRST"`

**Arm dry-run（GREEN）**
- `cluster upgrade --to-version v-next --dry-run` → 有序计划（followers 先、leader 末）+ 每 host `TRANSFER/UPGRADE/SKIP` + `(dry-run — no host was touched)` + **零 host 触碰**断言（跑后 `node ls --brokers` 三 host 仍 v-cur）

**Arm 30-B roll 全程数据面连续性（破坏性五要素；F4 补 version-readback）**
- ① 基线：**容器内**（R-NO-HOST-LEAK）两 probe——(A) 数据面 `expose_serve_sentinel` → ctl 连续 curl 真 body；(B) 写面后台 `session create p$i --pin && session rm p$i` 循环——先证均活、初始零 `not_leader`
- ② 观测源：两 probe 落盘日志（curl code 序列 / write-loop stderr）+ `node ls --brokers`（每 host broker VER + colocated agent RELEASE）+ `systemctl show -p MainPID`
- ③ 注入边界：roll（followers-first/leader-last，transfer-leader-gated，逐 broker **self-re-exec reload**——`syscall.Exec` PID-preserving，**非 kill**）
- ④ 语义 oracle：整个 N=3 roll 期间写面**零 `not_leader`、零 503/JS unavailable/no responders** + 数据面 curl **全 200**（真流量，#20 收口，非 status）；**且每 host `node ls --brokers` VER 翻 `v-cur→v-next` WHILE `tether-broker` MainPID 不变 WHILE `nats-server.service` MainPID 不变（F4 关键：version-changed ∧ PID-same 一起断——否则「没升级发生」也满足零-disruption+PID-same = 假绿）**
- ⑤ 清理：trap `pkill` 两 probe + `expose rm`
- **非-vacuous 依赖（F4/C.2）**：零-`not_leader` 单看可能因 probe 没在写 → **必须由 30-C（N=2 fence）做负对照**；缺 30-C 则 30-B 空转

**Arm 30-B-whole-host（explore→pin，纠 Draft 3/4/6 assumed-GREEN——D1）**
- roll 后 `node ls --brokers` 三 host **broker VER 与 colocated agent RELEASE 均==v-next、无 skew**（`correlateBrokerVersions`）+ mid-roll skew：某 host「broker 已 reload、colocated agent 未 re-exec」窗口 `node ls --brokers` 显 SKEW
- **explore→pin（不 hard-code GREEN）**：g5-review:150 flag 潜在 colocated-agent bug（`handleReExecOnly` + `ExpectSHA256` → `os.Open("…(deleted)")` ENOENT → 回 `self_path` 永不 re-exec，skew 不清）；4 轮 Pass review 可能已修但源码无法确认（`agent/upgrade.go:156/174` 仍 emit `self_path`）→ **真跑；若 colocated agent 停在旧二进制 = NEW gotcha（非 harness bug）**

**Arm 30-C N=2 write-fence 负对照（GREEN）**
- 降 N=2（retire 一台或 `up --brokers 2`）→ 同一 writer 在流 → N=2 下 `cluster upgrade`（**先 `assert_refuses "--ack-writefence"`**，带则跑）→ writer **确观测到 `not_leader` fence**（N=2 F=0，`invariant 13`）——证 30-B 零-fence 是 N≥3 专属真性质、非 writer 没跑

**Arm 30-A mid-run HALT（破坏性五要素）**
- ① 基线：N=3 全 VOTER + writer 零 `not_leader` + `cluster upgrade` 跑到 canary 升完、host-2 reloading
- ② 观测源：`node ls --brokers` + `cluster ops ls`/stale-lock 串 + writer `not_leader` 计数
- ③ 注入边界：`SIGKILL` orchestrator PID（容器内 ctl 进程），注入点={canary 升完、host-2 未完}
- ④ 语义 oracle：**重跑同一** `cluster upgrade` → 检出部分态 + 清 stale `OpClusterOps` 锁（`"cleared a stale upgrade lock"`）+ 续跑三 host 全收敛无 skew + writer 续零 `not_leader`；**锁互斥（Draft 5 控制纪律）**：持锁期 `cluster join/retire/add` 被拒（`assert_refuses "cluster_upgrade_active\|BLOCKED"`）**且先证注入前同 op 无锁时可发起**（受害=锁下拒 + control=无锁成功）
- ⑤ 清理：trap kill 后台 orchestrator/probe

**Arm 30-D synthetic-skew HALT → NOT-COVERED-in-sim（MF-1/CF-3，纠 Draft 3）**
- 造真 commandVersion-skew 二进制须改 `internal/cluster/command.go:211 const commandVersion=2`（**const 不可 ldflags 覆盖**）= 产品 Go diff，**违反 S 批「零产品 diff」+ Mandate ④**（config/env 伪造 skew = inverted-success 假象）→ **NOT-COVERED-in-sim**（skew-HALT 逻辑 hermetic 已密：`decodeCommand` poison proof + `node_versions_test` skew 分类）

### drill 32-install-lifecycle（GREEN，N=1）

**fresh throwaway 容器**（不复用 provisioned 节点——install.sh 幂等/属主要在真处女盘跑）。

| 臂 | oracle |
|---|---|
| **`--dry-run` 零写** | `install.sh --role <r> --dry-run` → `find`+stat install 树 前后**字节等同**（非 exit-0）；三角色各一 |
| **永不启动** | 装完（非 dry-run）→ `! pgrep -x tether`（broker 角色 `systemctl enable --now` 只是 `cat` 打印的操作员指引 `install.sh:592-600`、不执行）；三角色皆然，在 sim 控制脚本 enable **之前**断 |
| **`--uninstall` 清单归零 + 重装幂等** | 装 → 记文件清单 → `--uninstall` → 清单**归零**（`rm -f $BIN_DIR/tether` + `~/.tether` 移）→ 再装 → 幂等 |
| **属主 vs 真 install.sh 表（doctor 复检）** | `/etc/tether` root-owned、`nats.d/` tether-owned、`secrets/` 0700 tether、**agent `~/.local/bin` sim-owned（S0-layout）**——`doctor` 全绿；**FD-1 caveat**：baked 顶层 `/home/sim/.tether` 0755 vs install.sh 0700，若断顶层 mode 须先 align real（不 chown 掩盖，Mandate ①） |
| **§8.4 单机 broker 手动升级** | `stop tether-broker` → **`[env: 特权 staging]` 换二进制**（显式分立，断 sha 落地）→ `integrity_check`（有则跑，否则 `tether version` 断 v-next+sha）→ `start` → **G.2 收敛**：oracle 收在「重启后 **tier-B 通 + session/expose 业务行完好（真读写）**」（Draft 3 arm-5，非 `health==HEALTHY` 字段；health 降佐证） |

**32 破坏性-臂 = N/A**（显式登记，免主进程误判漏 kill 臂；`--uninstall`/换二进制是运维步非故障注入）。

---

## §5 合并 gotcha ledger（#28+，signature-guarded）

> **编号去重（CF-1/MF-0/C2/F11 定案）**：三个 code-confirmed 缺陷各占低号，candidate 靠后。**主进程可在三个 confirmed 间调换 #28/#29 序**（drill 71 先于 31 → C2 建议 #28=stranding；4 critic 多数 → #28=allowlist；本稿取多数、**§11-U2 flag**）。

**#28 — agent 侧升级 URL 白名单硬编码、无操作员接线（CONFIRMED，EXPECTED to land）**
- 现象/机理：见 §4-31 Arm #28。broker.yaml allowlist 放行的自托管 URL 被 agent `url_not_allowed_local` 拒；agent 白名单恒为硬编码 GitHub 前缀、无 flag/yaml/env。
- 钉：`31` Arm #28 `assert_bug … "url_not_allowed_local"`（三点判别子前置）。**FLIP**：agent 白名单可配后翻 `assert_ok`（真升级），解锁 §6-B gated 臂。
- 伴随 DOC-3（已 S1 hermetic 钉，31 仅 cross-ref）。

**#29 — 常规 expose 承诺「home 死自动 rehome」但 crash 下 STRANDED（只有 drain rehome）（CONFIRMED）**
- 现象/机理：见 §2-71 重构。假承诺在 `expose.go:136/271`(CLI 运行时输出)+`usage.md:792/820`；`PlanReassignHome` 常规 expose 仅 `clusterdrain.go:742`(drain)；crash-reaper 只 rehome `__proxy__`；`rehome_events.go:52-53` 明写 stranded。
- 钉：`71` Arm C（INVERTED assert_ok，复合 gap 谓词 guarded by `exposes_stranded>=1` + cluster-wide curl-exit7）。**FLIP**：加 crash-rehome reaper 后 → Arm C(iii) 翻 `assert_ok` on curl-brkT-sentinel。
- 主进程可改判 DOC-16（若判 stranding 为有意设计、仅 CLI 措辞 overclaim）；现实钉法不变。

**#30 — cluster-mode `proxy sub revoke` 不发 `proxy_keyset_changed`（CONFIRMED）**
- 现象/机理：`proxy_keyset_changed` 全仓唯一 writer=`proxy.go:665`(single-mode)；cluster `handleProxySubRevokeCluster`(`proxy_cluster_wire.go:92-104`) 与 reaper 从不 emit → clustered broker 上 revoke 对 audit 不可见（安全敏感 op 的观测缺口）。
- 钉（Stage-C pin-5 订正）：sys.events **无 operator reader**（admin 无 events 读命令），故 `proxy_keyset_changed` ABSENT **无法可读断言**——`73` REVOKE arm 只 `warn` NOT-COVERED，data-effect（carol /sub 404 WHILE alice 200）由可读 oracle 钉。#30 是 **code-confirmed 但 un-pinnable**（非 flipping pin；无 reader 时无法钉「ABSENT」）。**FLIP 前提**：需先有 operator-readable events 面，再补 emit 断言。

**#31（CANDIDATE，spike-decided，73）— cluster `/sub` 全 exit online 却渲染 0 节点**
- **无当前代码证据**（C5 hermetic 已测；`repairProxy` cluster no-op 是设计，directive 走 `proxy_cluster.go`）→ pre-armed 仅在 73 spike 显示 `proxy on` 成功但 `/sub`(经 ingress)**解析出 ss proxy 数==0**（E3 纠：非 HTTP 空/404——renderClash 回落 DIRECT-only 合法 200 doc）时登。
- pre-armed sig：`assert_bug … "parsed ss-proxy count==0 with all exits ready"`，guarded by **all-exits-`ready` sustained 跨 poll 窗 ∧ 无 `proxy_node_unready`/`home_reassign_*` in flight**（F9：排除合法 rehome/catch-up 瞬态）。**else**：GREEN + **DOC-1** logged。

**#32（CANDIDATE，spike-decided，71/73）— rehomed-then-returned home 泄漏 stale public listener（double-bind）**
- 无直接代码证据（类比 #26）→ pre-armed 仅在 home-return（#18 sticky）后 **curl returned-home 的口成功服务 rehomed body（本应 sticky on survivor）**时登。**else**：GREEN（sticky 由 71-G/74-B 验）。

**DOC-1 — usage §5.15 尾「cluster 不支持 proxy / proxy_unsupported」陈旧（CERTAIN，无代码残留）**
- `proxy_unsupported` 全仓 **零** Go 出现（唯 `usage.md:888`）；`usage.md:315` 已记 cluster proxy(C5) 可用；`proxy_cluster.go` 实现之。73 跑 cluster proxy end-to-end → confirm DOC-1（register only，S 批不改 doc）。`proxy.go:686` 注释「P13 OUT of v1 HA」是 comment-drift（行为对），note-only。

---

## §6 NOT-COVERED（两栏：永久 + gated）

**A. 永久 NOT-COVERED（附理由）**
- **成功升级/MainPID-不变/agent 侧 `sha256_mismatch`/randomly-kill-mid-upgrade** — 见 B 栏（这些是 gated，非永久，勿混）。
- **default-allow-public SS egress（72）** — hermetic net 无 public sink（桥 IP 全 RFC1918）；negative 腿只证 agent-B 拒 private，**显式 NOT-COVERED 使其无歧义是策略证明、非数据面失败**（F10；不伪造「public」桥目标 = Mandate ①）。
- **真 Caddy/ACME/wss public ingress（72/73）** — README NON-GOAL；S0-ingress test-TLS 忠实于 loopback+反代 posture（「测试 TLS ≠ 免 TLS」），真 ACME 面属 staging/实机。
- **single-mode `proxy_keyset_changed`（72，若 Stage-B 落 init'd cluster-mode）** — init'd N=1 = cluster mode，single-mode writer(`proxy.go:665`)不可达；hermetic 已覆盖（§3.0/§11-U1）。
- **synthetic proto/command-skew HALT（30-D）** — 需产品 Go diff 造真 skew（`commandVersion` const）、违反 S 批；hermetic 已密（§4-30-D）。
- **`nats-server` binary 升级（30）** — G5 scope 外（独立 unit 生命周期；G5 不重启 nats-server）；混 nats-version route mesh 是 doc-noted 未测轴。
- **cross-proto flag-day reinstall（v1→v2）** — 需 v1-fleet baseline + dual-proto 矩阵（roadmap §4.4）；需时再登。
- **`home_reassign_failed`（71）** — mid-flight kill-during-rehome 探，owner **S9-96**。
- **`rehome_stalled{no_eligible_target}`（71-F，若 N=3 干净造不出）** — compound 降级拓扑不可行时；74 覆盖该场景。
- **`expose/expose rm --ack-alerts`** — severe-alert 态与 71 rehome 拓扑冲突（造严重告警需杀至失 quorum、与 rehome 臂互扰）；owner **S8-92(a)**（同门覆盖 `session rm --ack-alerts`）。**inventory 记账（G1）**：line 123/125 从 S3-70/71 **改归 S8-92(a)**（附理由），并修 line-123 「五 flag」vs 六-flag（已含 `--ack-alerts`）计数不一致。
- **`cluster upgrade --notify-webhook`（30）** — receiver 容器是 S8-93 harness 增量；无 receiver 则 NOT-COVERED-in-batch，owner **S8-93**（或 piggyback S8-93 容器）。
- **`proto_bump_requires_reinstall`（31）** — ctl proto==broker proto（同源），deploy-tier 造不出跨 proto 请求；hermetic 已密。

**B. #28-GATED（非永久缺口，随 #28 修复翻绿——flip 条件附）**
- **成功升级 + MainPID 不变（syscall.Exec 同 PID，`agent/upgrade.go:191-205`）** — 今日 agent 拒自托管 URL（禁伪 github）；#28 修复后真升级并断 MainPID 跨升级不变。
- **agent 侧 `sha256_mismatch`（digest，`agent/upgrade.go:97`）** — digest 检查在 agent **接受 URL 之后**，自托管 URL 先被 `url_not_allowed_local` 拒（未下载）；#28 后可达。**（纠 Draft 3 稿内矛盾：以 gated 为准，非「仍可 GREEN」——只 broker 侧 `sha256_invalid` 格式今日 GREEN）**
- **randomly-kill-mid-upgrade 崩溃一致性（原子 rename 保旧二进制）** — **要素③结构性不可满足**（agent 白名单在下载前拒 → 无 in-flight 可 kill）；#28 后建完整五要素臂（需 S0-layout 使原子 install 命中真 `~/.local/bin`）。
- **`--all` 成功升级尾 + 单-freeze-skip 变体** — 存活 agent 命中 #28 config-abort；#28 后「一台宕机、余台成功升级、被冻者 transient skip、汇总打印」才是现实场景。

---

## §7 OQ 解决

**OQ-1（S4 SS 客户端）— RESOLVED：apt-bake `shadowsocks-libev` `ss-local`，附两个未烘依赖 + Stage-B 门**
- crypto interop **已核可行**：server 标准 SS AEAD `chacha20-ietf-poly1305`(`crypto.go:32`) + `evpBytesToKey`(MD5) + `HKDF-SHA1("ss-subkey")`(`crypto.go:60-69`)，stock `ss-local` 可互通；`password`=base64 PSK 字面串。
- **两个未烘镜像项（E2，必加 Dockerfile + Stage-B interop gate）**：(1) `shadowsocks-libev`（现 Dockerfile:14-20 apt 列表无它；ubuntu:24.04 universe 可得性须验）；(2) **YAML 解析器**（`/sub` body 是 **Clash YAML** `application/yaml`+`yaml.Marshal`，**非 SIP008 JSON**——纠 Draft 6；镜像有 jq(JSON-only)、无 yq/python3-yaml）→ 加 `yq` 或 `python3-yaml` 抽 server/port/psk（别 grep 合并行，违 R2）。
- 正向腿 agent-A(allow_private=true)、负向腿 agent-B(默认拒)、AEAD-neg 错 PSK；`ss-local` 后台**容器内**、trap `pkill -f ss-local`。
- **fallback ladder**：(1) apt ss-local（首选）；(2) **降级「AEAD 门 + TCP 连通」**（labeled partial NOT-COVERED，全 egress 腿显式登缺、绝非静默等价；blocked-dest journald 判别子仍区分 dest-policy vs AEAD）。**vendored-Go-SS-client（Draft 2 ladder 第 2 档）删除**——无 build/bake 路径（remote.sh 只 build tether），若主进程要保则须显式接 remote.sh（§11-U3 flag）。

**OQ-3（S5 artifact CA）— RESOLVED**：复用 S2 实例 CA（`secrets_ensure_shared`/inventory §3 CA-owner 规则，**绝不重铸**），注入 trust anchor（部署职责）；持 **TLS 层 ≠ agent-白名单墙** 分离，CA 绝不 paper over #28（agent 在 `upgrade.go:83` 拒**先于** TLS 层）。见 §1.C。

**OQ-6（S5 colocated agent）— RESOLVED**：每 broker 容器跑 colocated `tether-agent` unit + `broker.cluster.colocated_agent_nid`/`--colocated-agent-nid`（`node ls --brokers` 关联 broker↔agent 版本）；**共享 broker root-owned `/usr/local/bin/tether`** 走 **ReExecOnly 路径**（`agent/upgrade.go:71 handleReExecOnly`；drive `cluster_upgrade_drive.go:197`，execute-only 无目录写权——**NOT** S0-layout 的 `~/.local/bin`，别统一套用）。`ColocatedAgentNID` 仅驱动 display/skew；orchestrator 按当前操作的 host 定位、非 advertised nid。staging honesty 见 §4-30。

---

## §8 inventory 行消费（§4 无遗漏闸；收工 diff 命令树 golden 零变更）

- **line 120** `ps --all` → **70-J2**（真 PORTS 表头 N=1 无 HOME）；71-B/G 补有值 HOME+(moved)。
- **line 123** `expose`（六 flag）→ 70(`--local`J1/`--remote-port`J4-6/`--name`J7)、70-J8(`--on-broker`→single_mode)、71-A(`--on-broker`→unknown)、71-D(`--no-rebuild`)；**`--ack-alerts` 改归 S8-92(a)**（G1，修「五 flag」计数）。
- **line 124** `expose explain` → 70-J3 + 71-B/C。**line 125** `expose rm` → 70-J9（**`--ack-alerts` 同改归 S8-92(a)**）。
- **line 129** `node upgrade`（`--url/--sha256/--all/--timeout`）→ 31（#28 + broker url/sha GREEN + `--all` dispatch-skip/enum-exclude + `--timeout` thin）；成功升级/sha256_mismatch/mid-kill 登 §6-B gated；proto_bump §6-A。
- **line 130** `proxy on`（`--ha-policy`/`--yes`）→ 72(O：owner-only+`--yes` 正/负)+73(`--ha-policy`)。`proxy off`→72-OFF。`proxy status --cluster`→72(plain)/73(`--cluster`)。`proxy sub create/revoke --name`→72-C/REV+73-REVOKE。`proxy sub ls`→72。
- **§1.1** `proxy_enabled/disabled`→72（both modes）；`proxy_keyset_changed`→72(single) **或 NOT-COVERED-in-sim + #30(cluster)**（待 Stage-B，§3.0/§11-U1）；`proxy_node_unready`→73+74；`proxy_auto_rebalanced`→74-C(count==1)。
- **§1.3** `expose_rehomed`/`home_reassign_started/succeeded`→71-B（佐证，终点真 curl）；`home_reassign_failed`→S9-96；`rehome_stalled{no_eligible_target}`→74/71-F。
- **§1.4** `sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial`→73（按 kind、无 reason 字段）。
- **§1.5** `replication_degraded`→30（roll 窗瞬态 + 收敛清除，runbook §6；佐证）。
- **line 56** `broker_down_rehome_summary`→**owner S8-90**，71-C 仅 observe（为 stranding gap 提供 `exposes_stranded>=1`）。
- **§2.2** `cluster upgrade`（全 flag）→30；`cluster rebalance proxy --dry-run`→74-B；`cluster init/join/add/retire` 锁-BLOCKED 面→30-A（stale-lock 持锁 membership 被拒）；`cluster drain [--abort]`→71-B/E（与 S6-40 主 drain 分工，71 只取「拒静默迁移 rebuild-OFF」腿）。
- **§4.3** `install.sh`（三角色/uninstall/dry-run/never-start）+ §8.4 单机手动升级→32。
- **serve** `--upgrade-url-allow`(broker 侧 allowlist target=artifact URL，#28 判别子)/`--colocated-agent-nid`(OQ-6)→31/30。
- **收工 null-diff**：re-enumerate `grep pubSysEvent` + channels，记 delta（S1/S2 同款）；命令树 golden `TestCommandTreeInventory` 零 diff（S 批无 CLI 变更）。

---

## §9 run-drills 集成 + 拓扑最小化 + 命名 + flake

**拓扑最小化（Draft 6 §6.6，逐条核实成立）**：70 N=1（无集群语义）；71 N=3（rehome 需 kill home 后仍有存活 voter，N=2 F=0 失 quorum）；72 N=1（proxy 单 broker，2 agent 仅 allow_private 分工）；73 N=3；74 N=3（有意义分布 + restart 保 quorum）；31 N=1+2agt（per-agent 升级 + `--all` 分类）；30 N=3；32 N=1。**无过/欠供给**。

**实例命名/隔离**：`cmd_drill` 注入 `INSTANCE=drill-<name>`，`drill_begin` 拒非 `drill-*`；每 S0 容器实例作用域（`$INSTANCE-artifact`、ingress sidecar、SS sink）+ label `sim.instance=$INSTANCE` → 收尾 nuke 按 label 回收。零跨-drill 泄漏（+ R-NO-HOST-LEAK：探针一律容器内）。

**并发 flake（§1.1 + OQ-8；E8 纠：`-j` 是全局 cap → 分波=分次调用）**：G-A 新增 4 个 N=3（71/73/74/30）。`run-drills.sh -j` 单一全局 cap（`:104/:152`），**不能一次调用里 grow 族 -j2 + N=1 族全并行** → **分波 = 分次 `run-drills.sh`**：
1. **Wave 1（grow/cluster 族，`-j 2`）**：`./run-drills.sh -j 2 10 11 13 71 73 74 82`（并发 VOTER 窗 ≤2，远低于 5 的破窗阈）
2. **30 SOLO**：`./run-drills.sh 30-rolling-upgrade`（formed-N=3 滚动 restart + self-re-exec + colocated agent 扰动 JS-meta，最重最 timing-sensitive，不与任何 grow/JS-formation 撞；`-j2` 下容器数非约束，约束是并发 grow≤2）
3. **Wave 3（N=1 族，全并行）**：`./run-drills.sh 00 21 60 61 62 70 72 80 81 31 32`
- VOTER-promotion timeout **故意非 `FLAKE_SIG`**（`run-drills.sh:57-65`）→ grow-timing RED 显 RED、operator 单跑复核，**不自动 swallow**；仅 inotify/systemd-PID1 签名 auto-retry。
- Preflight `fs.inotify.max_user_instances ≥ 2048`（已 8752afe 持久到 8192）。
- **S0-layout 回归门**：改 agent ExecStart 后先重跑 61/62/80/82 验 verdict 不变（§1.D）。

auto-discovery：`run-drills.sh:92-97` glob `drills/*.sh`，新 drill 自动 enroll；`drills/lib/*` helper 不被当 drill。README drill 表 + 编号族补 70/71/72/73/74/30/31/32 + 头注预计时长/资源。

---

## §10 per-drill false-green 风险（头注，每 drill 一块）

- **70**：正向一律 curl 精确 sentinel 体（端到端，非 TCP-connect）；断流 curl exit 7（非 `! curl -sf`——半开 listener 4xx/5xx 骗过）；J10 STOP→refused→START 三段（裸 restart 残留骗「重连」）；port 恒 `--json|jq`。
- **71**：rehome 正向收在「新 home curl 真流量」（#20，epoch 佐证）；旧 home 必 exit 7（stale 200=cutover 未完）；C 臂 INVERTED 硬绑 `exposes_stranded>=1`（anti-vacuous）+ cluster-wide curl-exit7（F5）；home 一律 pin 非-leader（保 leader 观测源）；rebuild-OFF 对照走 drain 拒绝（非 crash-path 活性对照，那会 vacuous）。
- **72**：`/sub` 跨容器经 ingress（loopback 不改绑、无 dev-no-auth）；SS 正/负腿**同 run** 断（「全通/全断」皆假绿）；wrong-PSK 无 blocked-dest 日志（判别 AEAD vs dest-policy）；journald grep **时间窗内**（F8）；伪 token 404 与 revoked 404 字节等同（无 existence oracle）；revoke 收「新连拒 + bob 传字节 + alice2 恢复」（非「全断」）。
- **73**：`repairProxy` cluster no-op → 一切收在 `/sub`(经 survivor ingress)+真 SS 字节、**绝不 `proxy status: ACTIVE`**；rehome 从 query≠brkS 处查 home host（G7#2）；quorum-freeze **分离断言** survivor-homed 传字节 ∧ 死-homed 黑洞（`/sub` 200 是读门≠数据面）；freeze/disable 对照证 404 是策略非死 broker；freeze 前证 survivor 仍 active；transition kind 按 kind 匹配。
- **74**：分布收在每 exit `/sub` 指真 home curl（非 `--homes` 计数——#2 原始假绿）；auto count==1（反 flap）；env=on `systemctl show` live 验；manual≠auto（无 `proxy_auto_rebalanced`）；`__proxy__`-only（co-homed 普通 expose 不被选）。
- **31**：#28 签名 `url_not_allowed_local`（含 `_local`）三点判别子（vs broker `url_not_allowed([^_]|$)`）；broker `url_not_allowed` 臂用合法 64-hex sha（防 sha256_invalid 抢答）；`--all` 冻**全部** ONLINE agent（单冻→config-abort→汇总不出=假 RED）；gated 臂显式登（禁伪 github）；agent 二进制在 `~/.local/bin`（否则撞假权限墙，码变 install_failed）。
- **30**：staging 显式 `[env]` distinct + 断 sha 落地（sim 静默 staging=inverted-success）；30-B version-readback ∧ MainPID-same **一起**断（F4：没升级也满足零-disruption+PID-same）；30-B 非-vacuous **依赖 30-C** N=2 fence 负对照同批在场；whole-host 走 explore→pin（D1 潜在 self_path bug 未确认修）；writer/curl probe **容器内、连续跨整 roll**（非末尾采样）；staged next 同 proto（误带 skew 会把 GREEN 测成 HALT）。
- **32**：fresh throwaway 容器（provisioned 节点 sentinel 让「零写/幂等」假绿）；属主镜像 install.sh 真布局不预置 chown（FD-1：顶层 mode 先 align real）；§8.4 收「重启后真读写恢复」非 health 字段；staging 显式 `[env]`。

---

## §11 主进程定稿裁决（Stage-A 未闭合项的最终立场）

> Stage-A 综合把 5 项标注「交主进程」；以下为定稿裁决。凡「Stage-B spike」者，立场 = **预期默认 + 反证切换条件**（探索→定格，先真跑、不静态跳过）。

- **U1（72 single-vs-cluster 模式）— 裁决：默认 init'd cluster-mode N=1；true single-mode 仅在 Stage-B spike 反证时切换。** 现存 12 drill 无一跑 un-init broker，`init` 正是单机 broker 的 auth_callout/JS bootstrap 步——un-init broker serve-ready 无先例、预期 INFEASIBLE。**S4 头号 Stage-B spike 先证 un-init broker 能否 `session create` + `agent join --pin` 到 ONLINE**：能 → 72 切 true single-mode，覆盖 single-mode `proxy_keyset_changed`(GREEN)；不能（预期）→ 72 = init'd cluster-mode N=1，single-mode `proxy_keyset_changed` 行 = NOT-COVERED-in-sim（hermetic 已覆盖），且 #30（cluster revoke 不发该事件）成真正被 drill 的 gotcha。inventory §1.1 该行判定随 spike 结论统一。
- **U2（三 confirmed gotcha 编号序）— 裁决：#28 = agent-allowlist、#29 = expose-crash-stranding、#30 = cluster-revoke-no-keyset-event（采 4-critic 多数，固定，不再调换）。** 理由：#28 是 roadmap §3 S5-31 唯一在总纲层就点名预判的产品缺陷（「预期落 gotcha」），给它头号符合「最先明确预期」语义。每 drill 头注 + `assert_bug` token 按此 1:1 引号锚 flip。
- **U3（OQ-1 vendored-Go-SS-client fallback）— 裁决：删除，不保留。** fallback ladder 定为 (1) apt `shadowsocks-libev` `ss-local`（首选）→ (2) 降级「AEAD 门 + TCP 连通」labeled partial NOT-COVERED（egress 腿显式登缺、非静默等价）。vendored-Go-SS 无 build/bake 路径、不值得为它增编 `remote.sh`。
- **U4（30 whole-host self_path bug）— 裁决：explore→pin 真跑，不预判 GREEN。** g5-review:150 的 colocated-agent `self_path` 回退 bug 是否已被 G5 四轮 Pass review 修复，源码无法确认（`agent/upgrade.go:156/174` 仍 emit `self_path`）。30-B-whole-host 按 explore→pin 真跑；若 colocated agent 停在旧二进制 = NEW gotcha（占 #31/#32 candidate 之后的下一号），非 harness bug、绝不 harness-around。
- **U5（30 staging 精确机制）— 裁决：Stage-B 先核 install.sh 是否有「升级既存二进制」的 `--url` 模式；有 → 用真 `install.sh --url <artifact>` root download+verify（consume S0-artifact）；无 → 退为 labeled 特权 operator 步。** 两路径都必须 **显式 distinct 步 + 断 `sha256sum == $EXPECT_SHA` 落地**（MF-2 铁律：绝不让 sim staging 显得比 `cluster upgrade` 实际更自动）。

---

**相关文件（绝对路径）**：候选 drill `/home/weiland/projects/dist_experiment_control/test/simcluster/drills/{70,71,72,73,74,31,30,32}-*.sh`；harness 增量 `.../drills/lib/{docker,ingress,proxy,secrets,ident,agentyaml}.sh`、`.../image/{ingress-proxy.py,provision-node.sh,Dockerfile}`、`.../remote.sh`、`.../run-drills.sh`；台账 `/home/weiland/projects/dist_experiment_control/docs/deploy-tier-gotchas.md`（#28-#30 + DOC-1 confirm）；清单 `/home/weiland/projects/dist_experiment_control/docs/reviews/simcluster-coverage-inventory.md`；plan 落点 `/home/weiland/projects/dist_experiment_control/docs/reviews/s3-s5-plan.md`。
