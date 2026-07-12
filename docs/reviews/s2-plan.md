# S2 Plan — session · 多租户安全 · admin · agent onboarding（C1/C2）

Date: 2026-07-11. Batch: **S2**（S 系列第二批）. Flow: CLAUDE.md §3（3 阶段 7 步）.
Status: **主进程定稿**（Stage A step 2）. Roadmap 总纲：`docs/simcluster-coverage-roadmap.md` §3-S2；
无遗漏闸清单：`docs/reviews/simcluster-coverage-inventory.md`；质量尺模板：`docs/reviews/s1-plan.md`.

> 草拟法：本 plan 由 Stage A 对抗草拟工作流（5 drafter〔80/81/82/S0-隧道/S0-ingress〕→ 5 对抗 critic
> 〔false-green / anti-masking / source-fidelity / §0.4-五要素+清单闸 / feasibility〕→ 1 synth，全部
> Opus 4.8、静态数量）产出候选综合稿（`scratchpad/s2-synth.md`），主进程逐条核实源码 + **4 次活体
> server spike** 后定稿。**验收目标**：`80-session-isolation` GREEN、`81-admin-evict-session-rm` GREEN、
> `82-agent-onboarding-invite` GREEN（三者各含探索→定格预期-gotcha 臂）+ 两个 S0 底座（S0-隧道 / S0-ingress）
> 落地并活体验证。**零产品 Go diff**（S1 已落命令树校验器，S2 只复跑它作闸）。

> **使命一句话**（逐字约束每断言）：像真实团队那样「用」一个真实部署的 tether 集群，让缺陷**暴露**——
> 不是让操作「跑通」，而是让问题「露出来」（README Mandate ①–④）。测试基础设施的目的是**暴露问题**，
> 不是攒一堆 GREEN。

---

## 主进程定稿说明（vs 工作流综合稿）

综合稿质量极高、源码有据、已直接并入全部 5 位 critic 的结论（17 条，见综合稿 §A）。主进程全盘核实后
**采纳其结构与断言表**，并对综合稿抛出的 5 个 OPEN 项（O1–O5）与散落 FLAG 逐一**定稿裁定**，其中两项
（O1/O2）由**活体 server spike** 实测定夺：

### 活体 spike 实测（4 次，均在 weilandserver throwaway 实例，2026-07-11；无泄漏，trap nuke 已清）

1. **S0-隧道 数据面 = PASS（铁证）**：`public_host=<容器hostname>`（Option A：`--domain <node>`）+ agent
   `tunnel_addr=<broker>:7000` 后，`expose agt1 --local 18080 --name X` → ctl 容器 `curl http://brk1:14000/marker`
   返真 body（frp 反向隧道跨容器转发真流量）。→ S0-隧道 两处 provisioning delta 成立；81 活跃-expose 臂可行。
2. **S0-ingress netns-share = PASS**：普通 bridge sidecar **够不到**他容器 `127.0.0.1`（REFUSED，证 loopback 边界
   真）；`--network container:<brk>` sidecar 共享 netns → 达 `127.0.0.1:<port>` 取回 body（REACHED）；`openssl
   3.0.13` 可 TLS-terminate。→ 同 netns TLS 反代方案的传输机制确立。
3. **O1 evict-leak = PASS（定 Arm C + gotcha #26 形态）**：setsid-nohup 部署下 `tether exec agt1 -- sleep 999999 &`
   进 `tether ps` 管理表（无需 PTY/tether run）；evict 后 → **DAEMON-GONE + CHILD-LEAKED（sleep 仍活、reparent
   PID1）+ PS-ROW-GONE（FK cascade——divergence 坐实）+ NODE-GONE + expose 口 curl RC=7（connection refused）=
   公网口被清**（随 agent 退出→隧道掉线的副作用，**非显式 evict teardown**）；systemd 反证 = **CHILD-REAPED-by-cgroup**
   （非 tether 之功）。→ **gotcha #26 精化 = 只 OS 子进程泄漏（部署条件式：setsid-nohup 真泄漏、systemd 下 cgroup 顺带回收）**；
   公网口如实钉「被清但机制是隧道掉线副作用、非 evict 显式拆除」。
4. **O2 nats_as = PASS（定 Oracle ②）**：`nats --nkey <home>/keys/default.nk --connection-name tether-cli:lab`
   假冒成功（A 自己 lab pub 通=JWT 有效）；A(lab)→ops **pub DENIED + sub DENIED**（皆 "Permissions Violation"）。
   → **Oracle ② protocol-layer ACL 拒可行、无需退 fallback**；Oracle ②b（app-layer node_not_found）仍作补充臂保留。

### O1–O5 定稿裁定

- **O1**（evict-leak cgroup）→ **采纳** setsid-nohup 部署 + backgrounded `tether exec` 注入 + 部署条件式 gotcha #26
  + systemd cgroup 反证探针；Arm C 最终形按 spike#3 落（child-only leak；port cleaned-incidentally）。
- **O2**（nats_as 假冒）→ **采纳** Oracle ② protocol-layer（spike#4 PASS）；②b/TS 作补充 app-layer 臂（非 fallback，同批一起断）。
- **O3**（编号）→ 线性化：**#25** PIN 无限速（80）· **#26** evict 不清理 OS 子进程（81，部署条件式）· **#27**
  manifest_listen 默认关+未文档化（82）。DOC 续 S1 的 DOC-5 → **DOC-6+**。
- **O4**（归类）→ 两项判 **DOC（非 gotcha）**，drill 仍中立钉现实：**DOC-6** = 被踢 nkey 用新 PIN 重入（eviction≠ban，
  有意；仅 nkey 泄漏时重入仍需 session PIN=实际已吊销，符合安全实用主义）；**DOC-7** = 伪造-pin 无 `--expect`+inline
  seed → agent.yaml 残留（有意纵深防御：pin=OOB 信任锚，roster-cache 由签名验证门控——残留 yaml 无信任价值）。
- **O5**（manifest_listen 默认关）→ 判 **gotcha #27**（坐实：`broker.go:753` gate `ManifestAddr==""` 默认不 bind；
  `applyClusterSeam cluster.go:799` 只写 data_dir/raft_addr/secrets_dir/nats_conf_path 无 manifest_listen；
  `usage.md`/`cluster.md`/`broker-ops.md` 零 `manifest_listen`/`well-known` 命中）。drill 82 setup 先断默认关再启用。

### harness 放置 / 机制裁定

- `CTLH`/`CTLHS`（多身份 TETHER_HOME）、`nats_as`（bare-nats 假冒）、`bind_agent`（broker-admin ONLINE poll）→
  落 **`drills/lib/`**（如 `agentyaml.sh` 先例；不进 operator-facing `simcluster` brain）。
- **S0-隧道 broker 侧 = Option A**：`image/provision-node.sh` broker 角色 `--domain "sim-${NODE}.tether.test"` →
  `--domain "${NODE}"`（install.sh 无 domain 点校验，`install.sh:460` 仅 `[ -n "$DOMAIN" ]`，spike 已用 `brk1` 验通）→
  install.sh 写 `public_host: <node>`（`install.sh:529`）= 单源忠实。烘焙 delta，需 `remote.sh build` + 重验既有 10 drill 绿。
- `ingress_enable_manifest` = drill helper（可见 labeled operator 步，gated on gotcha #27 先断）。

### 采纳的 critic 源码纠偏（要点，全 17 条见综合稿 §A；主进程复核通过）

A1 CONNECT-deny client 只见通用 `Authorization Violation`（§3.0 承重发现，塑造每个 deny 臂）；A4 删「去 --cacert」
假负例、改 wrong-SAN / 未注入-CA 真负例；A6 session_destroyed 用直接 sys.events oracle（member JWT **有** sys.events
sub，permissions.go:147）；A7 events 臂 fp-filter 排除 Arm R 污染；两 invite 路径区分（`cluster invite` 归 S8-91）；
`agent_roster_stale` 6-min grace → NOT-COVERED-in-sim；roadmap line 322「无半写 agent.yaml」是 overclaim（T2 钉残留现实）。

---

## §0. 范围与边界

- **交付**：3 个 drill（`80-session-isolation` GREEN、`81-admin-evict-session-rm` GREEN、`82-agent-onboarding-invite`
  GREEN，均含探索→定格预期-gotcha 臂）+ 两个 S0 底座（**S0-隧道** + **S0-ingress**，见 §1/§2）。**S2 是 S0-ingress
  的实例-CA facility owner**（首开需之批，inventory §3 CA-owner 规则）。
- **拓扑（§0.4 最小化）**：**80 = N=1 + 2 agent + ctl**（双 session 双 identity 隔离需两租户 node 做 app-layer 边界 +
  `TETHER_SESSION` no-crosstalk）；**81 = N=1 + 1 agent + ctl**（evict/rm broker-local，admin socket per-broker）；
  **82 = N=2 cluster + fresh agent + ctl**（grow/roster 收敛需集群语义）。
- **只测不修（§0.2）**：S2 交付 drill + 暴露缺陷，**零产品修复**。任何 tether 缺陷 → 台账 `#25+` + signature-guarded
  `assert_bug`（或倒置 `assert_ok`）或 DOC-n；修复另立叶子。harness/保真度债随批修，但任何超「真实生产供给」的环境新增必带
  **Mandate-④** 说明（§2）。**唯一的 harness 行为新增 = S0-ingress 的 `manifest_listen` 启用**——其忠实性**门控于 drill 82
  先断「默认关」（gotcha #27），否则即省事-for-tether**。
- **深度闸门（§0.3）——逐 drill 部署层增量**：
  - **80**：真 `auth_callout` 在真独立 nats-server 上按发的 JWT 做 **protocol-layer ACL denial（`Permissions Violation`，
    spike#4 实证）** + 真 `sys.events`（`pin_failed`/`member_joined`）+ 真 per-session node 表 + 经 distinct `TETHER_HOME`
    的 distinct nkey 双 identity。hermetic-密（不重断）：ACL 模板逻辑、connectError 分类器、cobra arg arity。
  - **81**：真 broker admin unix socket 0600/0700 **OS EACCES**（无应用层串）+ 真 sys.events 广播 → 真跨进程 agent 自退
    + 真 FK cascade on 真 SQLite + 真 JS history stream 删除 + **真孤儿 OS 进程**（in-process 套件造不出，spike#3 实证）+
    真 error taxonomy 过 socket。hermetic：evict arg arity、IsActive 状态机、`already_deleting` 瞬态。
  - **82**：真 `cluster seeds publish` MintInvite + 真 fresh-agent auth_callout 首连 provision（`ProvisionWithPIN`）+ 真
    invite parse + 真 agent.yaml 落真 install.sh 路径 0600/0700 + 真 HTTPS well-known manifest 经真 TLS front→loopback +
    真 Go x509 验（系统信任库）+ 真 account-signed manifest + AdoptDecision + 真 raft grow→新签名 roster→agent adopt
    （roster_gen 跳变）。hermetic：ParseInvite allowlist/scheme/version、roster_stale 谓词（6-min grace）、AdoptDecision 算术。
- **无产品 diff**：三 drill 全是 `drills/` 下 shell；S2 **零 Go 增量**。两处**烘焙 delta**（`provision-node.sh --domain <node>`
  + 新 `image/ingress-proxy.py`）→ 一次 `remote.sh build` + 重验既有 10 drill 仍 GREEN。`make test`/`make e2e`/`make lint`
  为一次性守恒闸（无产品 diff 理应零变化）。

## §1. 依赖与 S0 落地

- **上游依赖**：无产品前序（S2 叶子增量，不在线性 P 序）。
- **S2 落地的 S0 项（唯一归属规则：首开批落它需要的未落 S0 项；先于 S3/S4 开）**：
  - **S0-隧道**（agent 反向隧道 wiring，使 expose 数据面真 curl-able）——**81 的活跃-expose 臂消费**。roadmap S0 表默认
    落地批 = S3，但唯一归属规则使早开的 S2 落它；落地时翻 S0 表状态列 `未落地`→`已落地（S2；commit …）`并注 S3→S2 转移。
    **活体 spike#1 PASS**。
  - **S0-ingress**（per-broker 同 netns HTTPS 反代 sidecar + 实例测试 CA + 信任注入 + 证书正/负）——**82 的 well-known
    manifest 跨容器 bootstrap-URL 腿消费**。**S2 = 实例-CA facility owner**（首开需之批 + 建 CA 设施；复用 `lib/secrets.sh`
    铸造设施、绝不另铸 CA）。**活体 spike#2 PASS**。
  - **S0-隧道 无需任何 CA**（隧道 self-sign + agent InsecureSkipVerify + per-expose bearer token）——**与 S0-ingress 的
    CA 设施明确分立**，勿混。
- **S0 状态台账**：S2 后 S0-隧道 + S0-ingress = 已落地；S0-布局/artifact/备份库/故障原语仍未落地（各自后续批）。

## §2. Harness 增量（每项带 7 字段生命周期元组 + Mandate-④）

每断言约定（贯穿 §3）：**名 · 精确命令 · 期望 · sig-regex(file:line) · false-green 备注 · Mandate**。
`assert_ok "<d>" cmd…` / `assert_refuses "<d>" "<sig>" cmd…` / `assert_bug "<d>" "<gotcha>" "<sig>" cmd…`
（`lib/assert.sh`，regex 匹配 combined stdout+stderr）。**倒置语义臂**（缺陷=成功，如限速缺失/proc-leak/re-join）
用 `assert_ok`+头注+台账 flip，**非** `assert_bug`（其 exit 0 = "APPEARS FIXED"）。`poll_until` 不用固定 sleep；异步验
RESULT 非退出码；`drill_begin` throwaway 门；`trap … nuke`。

### 2.1 S0-隧道（agent 反向隧道 wiring）— 复用 S1 fixture，S2 点火

**两处 provisioning delta（Mandate ③ 忠实供给，非替 tether 弥补；活体 spike#1 PASS）：**
- **BROKER 侧** `public_host = 可解析容器 hostname`（**Option A，定稿**）：`image/provision-node.sh:27-28` broker 角色
  `--domain "sim-${NODE}.tether.test"`（不可解析 dummy）→ `--domain "${NODE}"` → install.sh 写 `domain: <node>` +
  `public_host: <node>`（`install.sh:528-529`）→ `serve --config broker.yaml` 经 `pickPublicHost`（`serve.go:79,321-330`）
  读 → `expose` 广告 `<node>`（`expose.go:49-51,326`）= 每容器可解析。**烘焙**（`remote.sh build`）。ripple：`provision-node.sh:28`
  是全 sim 树唯一 `sim-…tether.test` 命中；无 drill 断域名串；Caddy 从不在 sim 启（`simcluster:97-101`）故 `<node>:443` block
  惰性；route/tunnel SAN 独立铸于 `<node>`（`secrets.sh:35-40`）故 mTLS 不受影响。install.sh 无 domain 点校验（`install.sh:460`
  仅 `[ -n ]`，spike#1 用 `brk1` 验通）。**否决 `/etc/hosts` 假冒**（=造 DNS，mask）。
- **AGENT 侧** `tunnel_addr = <broker>:7000`。**无新 fixture**——复用 S1 `drills/lib/agentyaml.sh::agent_provision_yaml`
  已写 `tunnel_addr: <broker>:7000`（`agentyaml.sh:37-38,77`，从 broker_url 剥 host 派生）+ flagless unit（yaml 权威）。
  S1 显式**未**断可达性（`agentyaml.sh:14-16` 承诺 fail-loud），S0-隧道 = **点火 S1 wired 未 fire 的断言**。**落地时改**
  `agentyaml.sh:14-16` 注释「S3's job」→「S2's job（landed here）」。

**§2.1 活体验证（T0-T6，折入 81 setup 前置 or 独立 `00-tunnel-feasibility.sh`；活体 spike#1 PASS，作回归门）**：
T0 topology；T1 真 tunnel_addr（`agent_provision_yaml agt1 lab nats://brk1:4222 open`，grep `tunnel_addr: brk1:7000`，
STOP→leave-ONLINE→START 守卫 `agentyaml.sh:107-115`）；T2 agt1 本地 server 服 `SENTINEL-$RANDOM`（注入前基线①）；
T3 `tether expose agt1 --local 8888 --name tunnelprobe --json` → `jq -e '.public_host=="brk1" and (.port>=14000 and
.port<=14999)'`（`expose.go:109`）；**T4 数据面铁证** ctl `curl -fsS --max-time 10 http://brk1:<port>/probe.txt` → body
**字节等于 `$TOKEN`**（非「curl 退 0」非状态字段）；T5 负控 邻港未分配 → refused；T6 cleanup（`expose rm --name tunnelprobe`
+ `pkill http.server` + trap nuke）。

**生命周期元组**：归属批 **S2**（首个数据面批，唯一归属规则）· 消费批 S2-81 · S3-70/71 · S4 全数据面 · S6-40 · S7-50/51 ·
S9-94/95/96 · 实例作用域 = 无新 host-side 态（broker 侧在 per-container `/etc/tether/broker.yaml`；agent 侧在
`/home/sim/.tether/agent/<sid>/agent.yaml`；均随实例卷 nuke）· 创建预检 = broker.yaml 由真 install.sh 铸、agent.yaml 需先
`cmd_agent_join`（nkey 已 bound），T4 curl=预检 · **密钥/信任材料 = 无**（隧道 self-sign + bearer token）· 健康检查 = T4 真
curl-body + agentyaml STOP→leave-ONLINE→START 守卫 · 最终清理 = `expose rm` + `pkill` under trap，实例卷随 nuke。
**Mandate-④**：operator 本就把 public_host 设成可解析域名 + install.sh agent 本就写 tunnel_addr；sim 供给这两值再 curl 真
body——两配置值 + 一 curl，无精巧 bash 掩盖破 op。

### 2.2 S0-ingress（per-broker 同 netns HTTPS 反代 sidecar + 实例测试 CA + 信任注入）

**产品两 loopback listener 永不改绑/削弱（OQ-5 已裁定 roadmap:851-858；`TETHER_DEV_NO_AUTH` 永不设）**：well-known
manifest（`manifest.go:19-42` Bind 拒非 loopback）+ /sub proxy（`subhttp.go:34-46` requireLoopback）。S0-ingress = 同 netns
反代**替 Caddy**（`install.sh:575-582` 生产 Caddy 做的事）。

- **反代工具 = 烘焙 `image/ingress-proxy.py`（Python stdlib HTTPS 路径路由反代，~80-100 行）**。理由：python3 已烘焙
  （Dockerfile:17），零 apt；path-route 如生产 Caddy（`/.well-known/tether/*`→127.0.0.1:7480 [S2]；预留 `/sub/*`→127.0.0.1:8090
  for S4）；`ssl.SSLContext(PROTOCOL_TLS_SERVER).load_cert_chain`（ed25519 leaf，openssl 3.0.13 支持，spike#2 已验）;
  `ThreadingHTTPServer`；fail-loud（bind/cert-load 失败非零退，使健康 poll 露死 front）；作 sidecar PID1
  （`--stop-signal SIGTERM`）。**拒** socat（需 apt）/ nginx-caddy（重、Caddy 默认 ACME=sim 非目标）。烘焙 delta：Dockerfile
  `COPY image/ingress-proxy.py` + 并入 chmod 行；无新 apt。
- **同 netns sidecar**：`d run -d --network container:$(ctr_name <brk>) --stop-signal SIGTERM --label sim.instance=$INSTANCE
  --label sim.role=ingress --label sim.nodeid=<brk>-ingress -v <secrets>:ro $IMAGE python3 /opt/sim/ingress-proxy.py --listen
  :443 --cert … --key … --route /.well-known/tether/=127.0.0.1:7480`。共享 broker netns（同 lo/eth0/IP/DNS 别名 `<brk>`）→ peer
  容器 `https://<brk>:443` 可达 + sidecar 达 broker 的 `127.0.0.1:7480`（spike#2 验证）。`:443` sim 内空闲（Caddy 不启，nats
  ws=127.0.0.1:8222）。cert 材料**只读 bind-mount** 自 per-instance secrets stash（无 docker-cp race）。选独立 sidecar 容器
  （非 broker 容器内进程）保 broker 容器**纯产品**（Mandate ③ 硬界）。
- **实例 CA + leaf（复用 `lib/secrets.sh`，绝不另铸）**：CA = `secrets_ensure_shared` 铸的 ed25519 `cluster-ca.pem`（每实例一次、
  幂等 `secrets.sh:19-31`）——**复用不重铸**（重铸会漂移在跑容器信任，inventory §3）。per-broker `ingress` leaf 由 `_mint_leaf`
  （`secrets.sh:33-47`，已发 `SAN=DNS:<cn>,DNS:localhost,IP:127.0.0.1`）；新幂等 wrapper `secrets_mint_ingress <inst> <node>
  [leaf=ingress] [san_cn=$node]`（放 `drills/lib/ingress.sh`）——`san_cn` 参使 wrong-SAN 负例可铸 `cn=notabroker`。
- **信任注入（部署职责，Mandate ③）**：`ingress_trust_inject <node> <inst>` = `d cp <shared>/cluster-ca.pem
  <ctr>:/usr/local/share/ca-certificates/tether-sim-ca.crt`（`.crt` 后缀承重）+ `dexec <node> -- update-ca-certificates`
  → Go 默认 TLS（`fetch.go:36-45` 无自定义 RootCAs，用系统信任库）信任 sim chain；无它则 `x509: certificate signed by unknown
  authority`（**真负控杠杆**，fetch 路径无 `InsecureSkipVerify`）。
- **`manifest_listen` 忠实启用（唯一 Mandate-④ hinge，门控于 gotcha #27 先断）**：helper `ingress_enable_manifest <brk>`
  （root，如 install.sh 写 broker.yaml=provisioning）插 `manifest_listen: "127.0.0.1:7480"` 进 `/etc/tether/broker.yaml`
  cluster: 块 + `systemctl restart tether-broker` + poll admin socket 复健。**visible labeled operator step**，drill 82 显式调
  （非烘焙进共享 seam）。**共享 seam 默认不含 manifest_listen**（忠于 `applyClusterSeam cluster.go:799` / sim seam
  `simcluster:292,176` 的省略，坐实 gotcha #27），使「默认不 serve-ready」事实可暴露。

**证书正/负断言（fixture 定义于此，断言跑在 drill 82；见 §3.3 J4/I-NEG-*）**：load-bearing oracle = **agent 真 Go-x509 fetch**
（`agent config refresh --once`，硬失败于 TLS 错，`agent_config.go:25-81`），非 curl OpenSSL 路径（curl `--cacert` 仅次级
sanity）。I-POS=J4（post-J1-pin）；I-NEG-CA=fresh 未注入容器 refresh→`x509: certificate signed by unknown authority`
（`fetch.go:52`）；I-NEG-SAN=CA 已信但 wrong-SAN 证书于 alt `:8444`（2nd sidecar）refresh→`x509: certificate is valid for …
not <brk>`。

**生命周期元组**：归属批 **S2**（实例-CA facility owner）· 消费批 S2-82 · S4-72/73（/sub，复用同 sidecar path-router+CA+信任
设施，加 `--route /sub/=127.0.0.1:8090`）· S5-artifact（复用实例 CA+trust-inject，不重铸）· 实例作用域 = sidecar
`sim-<inst>-<brk>-ingress` + labels（`list_nodes` 枚举 → `cmd_down`/`cmd_nuke` 反射式回收，零 simcluster brain 改，
`docker.sh:65-71`/`simcluster:468-473` 已验）· 创建预检 = `secrets_ensure_shared` 幂等 + `secrets_mint_ingress` 幂等 + broker
在跑 + `manifest_listen` 已启（poll loopback curl 非-503，需先 `seeds publish`）+ sidecar 名不占用 + 烘焙脚本在 · **密钥/信任
材料 = 实例 ed25519 CA（复用）→ 签 per-broker ingress leaf（SAN=DNS:<brk>）；CA 注入 ctl/agent 系统信任库；产品 loopback listener
永不改绑，`TETHER_DEV_NO_AUTH` 永不设** · 健康检查 = peer 容器 `curl -sf --cacert <ca> https://<brk>/.well-known/…` → 200 +
可解析 JSON（fail-loud）· 最终清理 = label 回收 + drill trap 先 `ingress_down <brk>` + per-instance secrets stash 随 nuke。
**Mandate-④**：供给 = 生产 Caddy 拓扑（同机 TLS→loopback + operator CA 入系统根，OQ-3），供 TLS **非** bypass TLS（loopback
listener 永不改绑；agent 真 Go-x509 验；invite forces https `invite.go:186`）；sidecar 只 relay 字节到产品自己 loopback
listener，manifest 仍产品构建/签、agent 自己 fetcher 抓、AdoptDecision 验——任一破即 RED。**忠实性门控于 drill 82 先断 gotcha #27**。

### 2.3 多身份 `TETHER_HOME`（drill 80/82 用；`drills/lib/ident.sh`，无新烘焙）

tether `DefaultHome()` 认 `$TETHER_HOME`（`identity.go:137-142`），每 home = 完整 identity 根（nkey `keys/default.nk` +
`current_session` + `broker_url`）→ distinct `$TETHER_HOME` = distinct nkey（distinct fp）+ distinct session + distinct
broker_url = 真双 identity 隔离（纯 ctl 侧）。helper：`CTLH <tag> <args…>` = `$SIM exec ctl1 -- runuser -u sim -- env
HOME=/home/sim TETHER_HOME=/home/sim/.tether-<tag> tether "$@"`；`CTLHS <tag> <TETHER_SESSION> <args…>` = 同上加 per-shell
`TETHER_SESSION`。tether 首 `EnsureIdentity` 自建 `.tether-<tag>/keys` 0700（`identity.go:83`），无需预建。**Mandate-④**：ctl
operator 本就可有任意多 `$TETHER_HOME`；只**供给** per-identity home，零产品行为改。

### 2.4 `nats_as`（bare-nats auth_callout 假冒，drill 80 Oracle ②；`drills/lib/ident.sh`；spike#4 PASS）

vendored `nats`+`nk` 烘焙于 `/usr/local/bin`（Dockerfile:39-40）。`nats_as <tag> <sid> <nats 子命令…>` = `$SIM exec ctl1 --
runuser -u sim -- env HOME=/home/sim nats --server nats://brk1:4222 --no-context --nkey /home/sim/.tether-<tag>/keys/default.nk
--connection-name "tether-cli:<sid>" "$@"`。`--nkey <seed>`（keys/default.nk 首字节 SU=user seed，spike#4 验）→ nats-server
验签 → callout 请求带 `ConnectOptions.Nkey`=identity pub（`handler.go:146`）；`--connection-name tether-cli:<sid>`
（`CtlNameForSession natsconn.go:105`）→ `parseRole`=`roleCtlActivated` sid=<sid>（`handler.go:197-206`）；无 token（已 member）
→ `ensureMember` nil → 发 `PermissionsForActivatedMember(actor,sid)`（`handler.go:164-170`）。**pub-deny 可靠**（spike#4 实证
非零退 + `Permissions Violation`）；**sub-deny 验输出**（grep `Permissions Violation for Subscription`，非退出码——sub async
-ERR 可能不在 --timeout 前非零退）。**Mandate-④**：`nats`/`nk` 预烘焙（非为此 drill 加）；假冒用 operator 本就持有的 real
identity nkey 证 JWT 边界，不造特权（任何持 seed 者皆可为此）。

### 2.5 `bind_agent`（session-home-agnostic agent onboarding + broker-admin ONLINE poll，drill 80；`drills/lib/ident.sh`）

sim `cmd_agent_join` 的 ONLINE 自检走 default-home ctl 的 `node ls`（`simcluster:372-376`），与双 home 不容（default home 未登
lab/ops）。故各 agent 经既有 `--pin` 首连 + 持久 unit bind（同 cmd_agent_join），但 ONLINE poll 走 **broker admin socket**
（session-agnostic 权威，`60-drill:31` 惯例）：`_agent_online_admin(){ $SIM exec brk1 -- runuser -u tether -- tether admin
nodes 2>/dev/null | grep -qE "$1[[:space:]]+.*ONLINE"; }`。**Mandate-④**：绑真 agent nkey 经真 `--pin` 首连、经 broker 自己
admin socket 观 ONLINE（ground truth），不填 tether gap。**`tether agent join <invite>` 是 drill 82 的**；80 只用经典 `agent
--pin` bind。

## §3. 逐 drill 规格

### 3.0 一处贯穿全批的承重源发现（塑造每个 CONNECT-deny 臂）

**auth_callout CONNECT-deny 的 reason 对 client 不可见——client 恒见通用 `Authorization Violation`。** tether handler 拒
（`h.deny`→`resp.Error=reason` `handler.go:400-405`）→ nats-server 走 callout-error 路径 → `c.sendErr("Authorization
Violation")`（nats-server `client.go:2434`），tether reason（`not a member of session %q` `handler.go:333` / `invalid PIN for
session %q` `:349` / `not provisioned; first connect must supply --pin` `:274` …）**只 server 侧 log + `$SYS…AUTH.ERR`**，
**从不**在 client -ERR。ctl 侧 `connectError`（`error_hints.go:144-157`）匹配该通用串渲染 `broker auth_callout rejected the
connection` exit 77；agent 侧 `agent: NATS auth_callout rejected`（`agent.go:858-869`，仅 `isAuthFailure`=Authorization
Violation 时打 `agent.go:1538-1553`；unreachable 走静默 retry `agent.go:871`）。**后果**：**任何 CONNECT-deny 臂必配判别子**
（正控 + server-side sys.event/log），否则通用串使坏 broker/网络故障同样拒=false-green。→ **DOC**（`handler.go:117-118`
docstring「client sees a clear auth error」误导）。

### 3.1 `80-session-isolation`（GREEN，N=1 + 2 agent + ctl）

**头注 false-green 横幅（必写入 drill 顶部）**：80 可能因错误原因变绿——
(a) CONNECT-deny 臂：client 串是通用 `Authorization Violation`，网络故障/坏 broker 同样拒 → 每 deny 配正控（同 identity 激活
自己 session）+ server-side 判别子（wrong-PIN→`pin_failed` sys.event；非成员→`ctx` 非持久化）。
(b) bare-NATS 跨 session ACL：deny 可能是断连非 subject-scoped ACL → pub-正控证 JWT 有效；deny sig 钉 `Permissions Violation
for (Publish|Subscription) to.*s\.<其他sid>`（**实测精确串 spike#4**：pub=`Permissions Violation for Publish to "..."`，sub=`...for
Subscription to "..."`；connect-deny 说 `Authorization Violation` 绝不 `Permissions Violation`）。
(c) `not_owner`：可能 CONNECT-deny 假冒 → `not_owner` 是成功 member CONNECT 后才可达的 app-layer 码 + member 正控。
(d) PIN 限速 probe：第 11 同源 correct-PIN join **成功=被记录的缺陷**用 `assert_ok`（倒置）；若某维护者见此臂 FAIL（第 11 被拒）
= 限速落地 → FLIP 为 `assert_refuses` + 升 #25 为 GREEN 回归。
(e) `sys.events` oracle：`pin_failed`/`member_joined` 可能是 setup 陈旧事件 → background sub 在 setup **后**起；只断 W 触发；
capture 每 drill fresh；Arm R 在 events 臂完成 + E-clean 后跑；E-joined grep W 的 fp。
(f) `member_joined` 专项：owner 自 `login` **不发**（已 member via session create→fp-reconnect 无 emit `handler.go:320-330`）→
用 fresh 非成员 W。
(g) `TETHER_SESSION` no-crosstalk：`node ls` 可能无视 `$TETHER_SESSION` → agt1 只在 lab、agt2 只在 ops，每 shell 只显自己
session 的 node（双向交叉核）。

**拓扑 + identity/session roster**：`up --brokers 1 --agents 2 --ctl 1` → `init brk1`（N=1）。identity/session 表：

| tag | `$TETHER_HOME` | 角色 | 建立 |
|---|---|---|---|
| **A** | `.tether-A` | lab owner，仅 lab member | `CTLH A session create lab --pin $PIN_A`（自加 owner `session.go:68-96`）+ `CTLH A login -s lab --pin $PIN_A --broker $NURL` |
| **B** | `.tether-B` | ops owner，仅 ops member | `CTLH B session create ops --pin $PIN_B` + `CTLH B login -s ops --pin $PIN_B --broker $NURL` |
| **T** | `.tether-T` | 两 session 的**非 owner** member | `CTLH T login -s lab --pin $PIN_A` 后 `CTLH T login -s ops --pin $PIN_B` |
| **W** | `.tether-W` | fresh 非成员（neg→pos + events） | events 臂内 live 建 |
| **r1…r11** | `.tether-r*` | fresh（限速 probe） | Arm R 内 live 建 |
| agt1 / agt2 | (agent 节点) | lab / ops 的 node | `bind_agent agt1 lab $PIN_A` / `bind_agent agt2 ops $PIN_B` |

常量 `SID_A=lab PIN_A=135790`；`SID_B=ops PIN_B=246810`；`NURL=nats://brk1:4222`。

**Oracle ① — 非成员激活 CONNECT 拒 + 不持久化 `current_session`**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| I1-pos | 正控：同 id 激活自己 session | `CTLH A login -s lab --pin $PIN_A` | exit 0 | `assert_ok`（`activated session` login.go:96） | 失败则 deny 故事是 infra 非 policy | ③ |
| I1-neg | 跨租户 no-PIN 激活 CONNECT 拒 | `CTLH A login -s ops`（A 非 ops，无 --pin） | 拒 exit 77 | `auth_callout rejected the connection`（error_hints.go:145；server reason `not a member of session` handler.go:333 非 client-visible §3.0） | 通用串→坏 broker 同拒→由 I1-pos + I1-persist 守卫 | ③ 真 auth_callout on 真 nats-server |
| I1-persist | 失败激活无痕 | `CTLH A ctx` | 打印 `lab` 非 `ops` | `lab` 且 `! ops`（ReadCurrentSession identity.go:151-165；write-only-after-connect login.go:78-84） | ctx 打 ops = 失败 CONNECT 上持久化=真 bug→RED | ③ |
| I1-broker | `--broker` 别名持久化 `broker_url`（闭 §2.1 login 行） | setup 用 `login … --broker $NURL`；随一条无 `--nats-url` 的 `CTLH A node ls` 成功 | exit 0 | `assert_ok`（`--broker`=`--nats-url` 别名，持久化 broker_url） | 无持久则后续 die「no broker」→证别名真持久 | ③ |

**Oracle ② — bare-NATS 跨 session ACL：双向、pub+sub（spike#4 PASS，protocol-layer）**

member JWT 无 `s.*` 通配（仅 PermissionsForBroker `permissions.go:221-263`）→ 跨 session subject 匹配无 allow → nats-server
protocol 层拒 `Permissions Violation`。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| I2-posA | A pub 自己 session（控） | `nats_as A lab pub 'tether.v2.s.lab.pty.x.in' hi` | exit 0 | `assert_ok`（permissions.go:107） | 证 A JWT 有效故 I2-AB* 是 ACL 非死连 | ③ |
| I2-posB | B pub 自己 session（控） | `nats_as B ops pub 'tether.v2.s.ops.pty.x.in' hi` | exit 0 | `assert_ok` | B 对称控 | ③ |
| I2-AB-pub | A(lab)→ops **pub** 拒 | `nats_as A lab pub 'tether.v2.s.ops.pty.x.in' hi` | 拒（非零退） | `Permissions Violation for Publish to.*s\.ops`（spike#4 实测精确串 `nats: permissions violation: Permissions Violation for Publish to "tether.v2.s.ops.pty.x.in"`） | connect-deny 说 `Authorization Violation` 非 `Permissions Violation`→sig 区分 | ③ 真 nats-server ACL |
| I2-AB-sub | A(lab)→ops **sub** 拒（验输出非退出码） | `nats_as A lab sub 'tether.v2.s.ops.audit.>' --timeout 3s --count 1` | 输出含拒串 | grep `Permissions Violation for Subscription to.*s\.ops`（A sub allow 仅 `s.lab.audit.>` permissions.go:138）；加正 sub 控（A sub `s.lab.audit.>` 无 error） | sub async -ERR 可能不在 --timeout 前非零退→grep 输出 | ③ |
| I2-BA-pub | B(ops)→lab **pub** 拒（对称） | `nats_as B ops pub 'tether.v2.s.lab.pty.x.in' hi` | 拒 | `Permissions Violation for Publish to.*s\.lab`（spike#4 串式） | 对称抓单向 ACL 洞 | ③ |
| I2-BA-sub | B(ops)→lab **sub** 拒（对称，验输出） | `nats_as B ops sub 'tether.v2.s.lab.audit.>' --timeout 3s --count 1` | 输出含拒串 | grep `Permissions Violation for Subscription to.*s\.lab` | 对称 both ops | ③ |

**Oracle ②b — app-layer 跨租户 NODE 隔离（用 2 agent；补充臂，与 ② 一起断）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| I2c-posA | A 见自己租户 node（控） | `CTLH A node ls` | agt1 ONLINE | `agt1[[:space:]].*ONLINE`（node.go:105） | 证 A 是活 lab session | ③ |
| I2c-AB | A(lab) 够不到 agt2（在 ops） | `CTLH A exec agt2 -- true` | 拒 `node_not_found` | `node_not_found`（exec.go:77——IsActive gate exec.go:49 先于 node lookup） | agt2 由 bind_agent 证 ONLINE-in-ops 故非空洞 | ③ deploy 真 per-session node 表 |
| I2c-BA | B(ops) 够不到 agt1（对称） | `CTLH B exec agt1 -- true` | 拒 `node_not_found` | `node_not_found`（exec.go:77） | 对称 | ③ |

**Oracle ③ — in-session 非 owner 触达 broker app-layer `not_owner`**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| I3-pos | T 是活 lab MEMBER（控） | `CTLH T node ls` | exit 0 列 node | `assert_ok` | 证 T 以 member CONNECT（故 not_owner 是 app-layer 非 connect 拒） | ③ |
| I3-rm | 非 owner `session rm`→`not_owner` | `CTLH T session rm lab` | 拒 `not_owner` | `not_owner`（sessions.go:148-155；rm 直发 RPC 无 typed-confirm） | not_owner 无成功 member CONNECT 不可达→非 connect-deny 假冒；owner rm 不跑（破坏性；hermetic 已覆） | ③ |
| I3-up | 非 owner `node upgrade`→`not_owner` | `CTLH T node upgrade agt1 --url https://x/x.tgz --sha256 <64个0>` | 拒 `not_owner` | `not_owner`（upgrade.go:57-66；owner 检查先于 url/sha 校验） | `--url`+`--sha256` CLI 必填（node.go:166）故 RPC 真发；broker 在校验前返 not_owner→dummy 值不 matter | ③ |

**Neg→pos + `sys.events` oracle（wrong-PIN→correct-PIN；`pin_failed`；`member_joined`）**

一个 background `sys.events` 订阅（as A，lab member，Sub allow `tether.v2.sys.events` `permissions.go:147`；subject SSOT
`proto.SubjSysEvents=tether.v2.sys.events` `subjects.go:11`）+ fresh identity **W**。sys.events 全局非 sid-scoped → 每事件断言
**必带 `"sid":"lab"` filter** + E-joined 带 W 的 fp。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| E-sub | 起权威 sys.events 观测 | `nats_as A lab sub 'tether.v2.sys.events' > $CAP 2>&1 &` 后 poll `Subscribing` in `$CAP` | sub live | readiness marker | 须在任何 triggered join **前** live | ③ |
| E-wfp | 捕 W 的 fp（A7 前置） | `CTLH W`（首 EnsureIdentity）后从 `keys/default.nk` pub 算 fp → `$WFP` | fp 记录 | — | 使 E-joined 只匹配 W、非 Arm R | ③ |
| E-neg | wrong-PIN join CONNECT 拒 | `CTLH W login -s lab --pin 999999` | 拒 exit 77 | `auth_callout rejected the connection`（server `invalid PIN for session` handler.go:349 非 client-visible） | 通用串→配 E-pinfailed + E-pos | ③ |
| E-pinfailed | `pin_failed` sys.event（精确 reason） | `poll_until 15 1 -- grep '"type":"pin_failed"' + '"sid":"lab"' + '"role":"ctl"' in $CAP` | 事件捕获 | `"type":"pin_failed"` 且 `"sid":"lab"` 且 `"role":"ctl"`（emit handler.go:348；pubSysEvent audit.go:36-51） | 须 W（post-sub）触发非陈旧 | ③ 真 broker sys.events |
| E-pos | 同 id correct-PIN 立即成功 | `CTLH W login -s lab --pin $PIN_A` | exit 0 | `activated session`（login.go:96） | 证 wrong-PIN deny 是 per-attempt 非 lockout（与 Arm R 限速缺失一致） | ③ |
| E-joined | `member_joined` sys.event（ctl PIN 首连，W 的 fp） | `poll_until 15 1 -- grep '"type":"member_joined"' + '"sid":"lab"' + '"via":"pin"' + '"fp":"'$WFP'"' in $CAP` | 事件捕获 | 四字段齐（emit handler.go:353） | owner login 不发（已 member）→用 fresh W；fp filter 排除 Arm R 污染 | ③ |
| E-clean | 收观测（**在 Arm R 前**） | `kill $SUBPID`（亦在 trap） | — | — | 序守卫：Arm R 在此后跑 | ③ |

**Arm R — PIN 限速黑盒 probe（探索→定格 预期-gotcha #25；在 E-clean 后跑）**

> **⚑ 外审 F1 订正（本表下方 R-warm/R-11th 的「correct-PIN」模型被此取代）**：§E.6 限速器的触发是**错误 PIN**
> （架构 §E.2 明确「失败分支 → 拒绝 + pin_failed + 按 E.6 计数」；hermetic `TestPINBruteForceNoLockout5Tries`
> 亦为「N 错 + 1 对」）。用 correct-PIN 数不触发失败计数，即使限速器只封失败也会永绿、不 pin #25。**实现改**：
> (a) 10 个同源**错误-PIN** 尝试全被拒（reached auth path）；(b) 捕获 **≥10 个 pin_failed** 事件（限速器真触发
> 点 fired 10×）；(c) 倒置 `assert_ok`：10 次失败后第 11 个同源 **correct-PIN 仍成功** = 缺失的 §E.6 per-IP 封锁。
> flip：限速落地后 (c) 翻 `assert_refuses`，加 second-source-in-window 成 + window-reset 原源恢复 成 GREEN 回归。

源码定夺（穷尽 + spike）：§E.6 限速器 **CONFIRMED ABSENT**（`architecture.md:825`；`internal/authcallout/handler.go` +
`internal/broker/authcallout.go` 无 counter/bucket/window/per-IP map/`rate.Limiter`；client IP `jwt ClientInformation.Host`
可得但从不读；每 wrong PIN 只发 `pin_failed`）。**assert 语义**：缺陷=「本该拒却成功」→ **`assert_ok`**。

| # | 名 | 命令 | 期望 | sig | false-green | M |
|---|---|---|---|---|---|---|
| R-sub | 起 fresh sys.events 观测（计 pin_failed） | `ev_sub_start`（truncate $EVCAP）→ poll `ev_ready` | sub live | readiness marker | 与 E-arm capture 隔离；只计 Arm R 的 pin_failed | ③ |
| R-fails | 10 同源**错误-PIN** 尝试全被拒（reached auth path） | `for i in 1..10: CTLH rf$i login -s lab --pin 111111`（错 PIN） | 各失败（RF==10） | `sh -c "[ $RF -eq 10 ]"` | 错 PIN 是 §E.6 限速器的**真触发**（架构 §E.2 失败分支计数）；correct-PIN 数不触发失败计数=假绿（外审 F1） | ③ |
| R-pinfailed | ≥10 个 `pin_failed`（sid=lab+role=ctl 单行绑定）被捕获 | `poll_until -- _ev_pinfailed_ge10` | ≥10 | grep 单行绑定 type+sid+role 计数 ≥10 | 证 10 次尝试真到失败分支（非被预先封锁） | ③ 真 broker sys.events |
| R-11th | **10 次失败后**第 11 同源 **correct-PIN** join **仍成功** | `CTLH rok login -s lab --pin $PIN_A` | exit 0 | `assert_ok "…after 10 same-IP wrong-PIN failures, the 11th CORRECT-PIN join STILL SUCCEEDS — PIN rate-limit ABSENT (§E.6; #25; FLIP when the per-IP limiter lands)"` | 若有 ≤10/IP/min 限速器,10 次失败后源被封、correct PIN 亦拒→FLIP；post-fix 加 second-source-in-window + window-reset 恢复 | ③ 真 per-IP path |

post-fix GREEN 会取的形（诚实记于 gotcha #25，限速缺席时 moot、不现加）：second-source-IP 控 + window-reset 控。11 join 皆源
ctl1=broker 见一个 `ClientInformation.Host`；11 顺序 CONNECT ~2-3s ≪ 60s。**pin_failed 审计（E-pinfailed）是独立正向 oracle**。

**Arm TS — `TETHER_SESSION` 双 shell no-crosstalk**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| TS-lab | shell pin lab 只见 lab node | `CTLHS T lab node ls` | agt1 在、agt2 缺 | `agt1[[:space:]].*ONLINE` 且 `! agt2` | 两 node 皆显=路由无视 session→与 TS-ops 交叉核 | ③ |
| TS-ops | shell pin ops 只见 ops node | `CTLHS T ops node ls` | agt2 在、agt1 缺 | `agt2[[:space:]].*ONLINE` 且 `! agt1` | 对称——证 per-shell 选择无 crosstalk | ③ deploy 真 per-session roster |
| TS-file | env unset→回落 file（末次 login=ops） | `CTLH T ctx` | 打印 `ops` | `ops`（identity.go:157-164） | 证 env-over-file precedence | ③ |

**§0.4 五要素**（Oracle ② bare-NATS ACL 隔离臂需异源成功控 / Neg→pos+events 臂 / Arm R 限速臂）见综合稿 §3.1 五要素段
（已并入）：Oracle ② = I2-posA/posB 异源成功控排除「everything fails」；events 臂 = live background sub + fresh W 注入 +
poll capture 验 RESULT + `kill $SUBPID` trap；Arm R = 限速缺席故 second-source/window 控 moot（记于 #25 作 post-fix 形），
单断言同 IP 第 11 不被拒 + trap nuke。

### 3.2 `81-admin-evict-session-rm`（GREEN，N=1）

**头注 false-green 横幅（必写）**：
(a) admin-owner 臂：`nobody` 的「permission denied」可能来自 binary 找不到/坏 --socket/broker down → sig 钉 `connect:
permission denied`（unix-dial EACCES）+ 正控（同 binary 同 socket、user tether→OK）。
(b) evict self-exit：「agent 离 roster」可能是心跳过期非 evict → oracle 是 `pgrep -x tether` on agt1 **空**（daemon PROCESS
退），在 P9 ~1s 预算内非 5-s StaleAfter timeout。
(c) reconnect-denied（A1 修正）：post-evict reconnect 可能因「broker unreachable」→ client sig 钉 **`NATS auth_callout
rejected`**（agent.go:859）+ server-side 判别子 grep brk1 `broker.err` `not provisioned`（handler.go:173）+ 正控 B0/D1。
**绝不**断 client 侧 `not provisioned…`（server-only §3.0）。
(d) evict-cleanup GAP（探索→定格，倒置）：leaked-proc oracle 倒置——leaked child=`pgrep` 命中=exit 0；用 `assert_bug` 会误读
leak 为「APPEARS FIXED」→ 用 `assert_ok`。oracle 是**复合谓词**：agent daemon EXITED **且** managed child STILL ALIVE。
**部署条件式（spike#3）**：leak 只在 **setsid-nohup** 部署可见（systemd cgroup 顺带回收 child）。
(e) session-rm：「rm 旅程跑了」非 phase 覆盖——每 phase 需自己 RESULT oracle（history stream 没/SQLite 残留没），poll。
(f) session_deleting probe：因 rm 成功记 covered = inventory §1.2 禁——probe 须验真拒机制 + 登 promise-vs-impl 差。
(g) re-join 臂（探索→定格，倒置）：re-join 用同 session PIN **成功**；守卫是**同一被踢 nkey**（fp 相等 oracle）非 fresh identity。

**依赖 + 拓扑 + 原语**：**N=1** `up --brokers 1 --agents 1 --ctl 1`。Arm C 的 active-expose 腿需 **S0-隧道**（§2.1，spike#1
PASS）；proc-leak 腿独立。helper：`CTL()`=`$SIM ctl -- …`；`BADMIN()`=`$SIM exec brk1 -- runuser -u tether -- tether admin …`
（授权 admin 路径，`60-drill:31` 惯例）；`AGT(agt)`=`$SIM exec <agt> --`。

**Setup**：`up 1b1a1c` → `init brk1`（N=1 leader）→ N=1 floor 健康 → `session lab --pin 135790`（ctl=**owner**，Arm E rm 需之）
→ `agent-join agt1 --session lab --pin`（bind agt1 nkey）→ `agent_provision_yaml agt1 lab nats://brk1:4222 open`（flagless
unit + tunnel_addr wired→poll ONLINE）。`session create lab` 发 `session_created`（sessions.go:79）。

**Arm A — admin socket owner 语义（broker-local，N=1）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| A1 | admin sessions（授权） | `BADMIN sessions` | OK 列 lab ACTIVE | `lab` + `ACTIVE`（server.go:269-291；admin.go:57-66） | A4 正控 | ③ 真 euid=tether socket |
| A2 | admin nodes（授权） | `BADMIN nodes` | OK `agt1 … ONLINE` | `agt1[[:space:]]+.*ONLINE`（server.go:293-324；**Stage-B 核 spacing**，60-drill 已用 ONLINE\|OFFLINE\|STALE） | — | ③ |
| A3 | admin audit（授权，默认 n=50） | `BADMIN audit lab -n 5` | OK ≥1 audit 行 | `#[0-9]+`（server.go:326-343） | 立 history-lab EXISTS（Arm E baseline） | ③ 真 JS history tail |
| A4 | **非授权 OS user→OS EACCES（承重 deploy oracle）** | `$SIM exec brk1 -- runuser -u nobody -- tether admin sessions` | 拒 `connect: permission denied` | `connect: permission denied`（server.go:62-68,136 socket 0600/0700；wrapped admin.go:214-216；broker-ops.md:649-657） | 承重：binary 找不到/坏 path/broker down 必不过——sig 钉 unix-dial EACCES；A1 证同 cmd+socket 对 tether 成功 | **deploy 真 fs-perm 墙，无 app 串** |

`nobody` 恒存、可 exec world-readable tether；`admin evict <sid> <nid>` 是 `cobra.ExactArgs(2)`（admin.go:164）——hermetic-密，仅 prose 注。

**Arm B — evict 在线 agent → ~1s 自退 + reconnect 拒**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| B0 | baseline agt1 ONLINE | `BADMIN nodes` | `agt1 … ONLINE` | `agt1[[:space:]]+.*ONLINE` | 破前证活（五要素①） | — |
| B1 | admin evict（2-arg） | `BADMIN evict lab agt1` | OK `evicted sid=lab nid=agt1 (…broadcast=true)` | `evicted sid=lab nid=agt1` + `broadcast=true`（admin.go:190-192；broker/admin.go:92-96） | no-op evict 打 `nothing to evict`+`not_found`（admin.go:184-188）——sig 钉真删+广播 | ③ 真 socket→真 DB 删+真 sys.events pub |
| B2a | agent daemon 自退 ≤~1s（RESULT） | poll_until ~6s：`! AGT(agt1) pgrep -x tether` | agt1 daemon PROCESS 没 | pgrep 空（poll RESULT；agent.go:711-728 `agent_evicted`→cancelRun） | (b) 非 5-s StaleAfter；断 PROCESS 退于 P9 ~1s | **deploy 真 sys.events 广播→真跨进程 shutdown** |
| B2b | roster row 删（FK） | poll_until：`BADMIN nodes` 无 agt1 | agt1 缺 | `! agt1`（DELETE nodes row server.go:391-398 / plan.go:67-77） | — | ③ |
| B3 | reconnect 拒（provisioning 没）—— A1 修正 | `$SIM exec -u sim agt1 -- env HOME=/home/sim timeout 8 tether agent --session lab --nid agt1`（flagless 读 yaml，无 --pin） | 拒 | client：`NATS auth_callout rejected`（agent.go:859）+ **server 判别子** `poll_until $SIM exec brk1 -- grep -q 'not provisioned' <broker.err>`（handler.go:173,267-275） | (c) 非「broker unreachable」；client 侧绝不断 `not provisioned…`（server-only §3.0）；正控 B0/D1 | **deploy 真 auth_callout CONNECT 拒** |

B 注：持久 unit `Restart=on-failure`（agentyaml.sh:101）+ evict→clean exit(0)→systemd 不 auto-restart。`agent_evicted` filter
`SID==lab && NID==agt1`（agent.go:723）故只被踢者自退=真定向 revoke。

**Arm D — 被踢 nkey re-join（探索→定格，倒置；为 Arm C re-provision agt1；DOC-6）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| D0 | evict 前捕 agt1 fp（五要素①） | 从 `agent_provisioning.fp`（broker probe）或 agt1 `keys/agent.nk` pub 算 → `$FP0` | fp 记录 | — | 使 D1 能证「同一 nkey」非 fresh mint | ③ |
| D1 | 被踢 nkey 用仍有效 session PIN re-join→ONLINE 且 fp==FP0（eviction≠revocation） | re-run `$SIM agent-join agt1 --session lab --pin 135790`（binds 同持久 nkey）+ `agent_provision_yaml agt1 lab nats://brk1:4222 open`；oracle：`BADMIN nodes` `agt1 … ONLINE` **且** 新 provisioning fp `== $FP0` | re-provision 成功、同 fp | `agt1[[:space:]]+.*ONLINE`（handler.go:246-289：evict 后 `Lookup`→`ErrNotProvisioned`，`ProvisionWithPIN` 用不变 PIN 成功；无 denylist/ban）**且** fp 相等断言 | (g) key 是 agt1 原被踢 nkey（非 fresh）——fp 相等证 byte-identical 重入；无此臂 vacuously 真于任何 key | **deploy 真 auth_callout re-provision——证 evict 删行但不吊销 identity** |

D 注：倒置语义→`assert_ok`（re-join FAILURE 会 fail loud=「APPEARS ban，re-triage」）。**归类 DOC-6**（eviction≠ban，有意；仅 nkey
泄漏时重入仍需 session PIN=实际已吊销）。

**Arm C — evict-with-RUNNING-proc + ACTIVE-EXPOSE cleanup GAP（探索→定格；spike#3 定形）**

**部署改写（spike#3）**：Arm C 的 agent 用 **`setsid nohup` 部署**（`install.sh:371` 头号手动启动，非 systemd unit），使 daemon +
managed child **不在一个 cgroup**、post-evict 命运可独立观测。序：D1 后（agt1 ONLINE under systemd）→ `systemctl stop tether-agent`
（留 yaml）→ `setsid nohup runuser -u sim -- env HOME=/home/sim tether agent --session lab --nid agt1 --nats-url $NURL
--tunnel-addr brk1:7000 >/tmp/agtC.log 2>&1 &`（读 yaml tunnel_addr）→ poll ONLINE（ppid=1 verify detach）。注入用 **backgrounded
`tether exec`**（spike#3 实证：进 processes 表、真 OS 子进程；无需 PTY/tether run）。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| C-base-proc | managed child RUNNING（五要素①） | `setsid tether exec agt1 -- sleep 999999 &`（ctl 后台，spike#3）；poll `AGT(agt1) pgrep -f 'sleep 999999'` 非空 + `CTL(ps)` RUNNING | child 活于注入前 | pgrep 非空（poll RESULT；exec 进 ps 表 spike#3 实证）；`sleep 999999.*RUNNING` in ps | 证 OS proc 活 | — |
| C-base-expose | 活 expose curl-able（五要素①；SENTINEL） | agt1 detached server 服 `SENTINEL-$RANDOM`→`/probe.txt`；`CTL(expose agt1 --local 8080 --name web)`；poll `CTL(exec ctl1 -- curl -sf http://brk1:<port>/probe.txt)` **body 字节等于 `$TOKEN`** | 数据面活于注入前 | body **string-equals `$TOKEN`**（承 tunnel T4；spike#1/#3 PASS） | 「curl exit 0」不够 | ④ 真反向-TCP 数据面 |
| C1 | admin evict 于此态（注入） | `BADMIN evict lab agt1` | OK evicted | `evicted sid=lab nid=agt1` + `broadcast=true` | — | ③ |
| C-brk | broker 视图：proc+node 行没（FK cascade——evict 契约，GREEN） | poll：`CTL(ps -a)` 无 `sleep 999999` **且** `BADMIN nodes` 无 agt1 | broker-权威行移除 | `! sleep 999999` in ps-a；`! agt1`（FK ON DELETE CASCADE，audit.go:99-104 + plan.go:63-77；spike#3 PS-ROW-GONE） | 证 broker DB 清——设立与 OS 现实的 DIVERGENCE | ③ 真 FK cascade on 真 SQLite |
| C-exit | agent daemon 退 | poll：`! AGT(agt1) pgrep -x tether` | daemon 没 | pgrep 空（agent.go:711-728；spike#3 DAEMON-GONE） | leak 签名前提 | — |
| C-GAP-proc | **LEAKED OS child（探索→定格，#26 oracle，setsid-nohup）** | C-exit 后：`AGT(agt1) pgrep -f 'sleep 999999'` **仍非空** | child 挺过 evict（reparent PID1；无码 kill） | **复合谓词**：`pgrep -x tether` 空 **且** `pgrep -f 'sleep 999999'` 非空（anchor：handleEvict / PlanEvict 无 proc-kill，只 row DELETE；spike#3 CHILD-LEAKED） | (d) 倒置：pgrep 命中=leak=exit 0→**`assert_ok`**；复合谓词排除「agent 还活故 child 活」 | **deploy 真孤儿 OS 进程（in-process 套件造不出）** |
| C-GAP-proc-sysd | 反证探针：systemd cgroup 顺带回收（记录=非 tether 之功，spike#3） | 另在 systemd unit 部署下重跑 C1；poll `AGT(agt1) pgrep -f 'sleep 888888'` **空** | cgroup 回收 child | pgrep 空（KillMode=control-group 默认拆 cgroup；spike#3 SYSTEMD-CHILD-REAPED） | 记录「回收是 systemd 之功、tether 仍啥没做」——防 gotcha #26 over-claim | ④ 部署条件式诚实 |
| C-port | **公网 expose 端口 evict 后被清（GREEN，机制=隧道掉线副作用非显式 teardown，spike#3）** | C1 后 poll：`CTL(exec ctl1 -- curl -sS --max-time 5 http://brk1:<port>/probe.txt)` → connection refused | 公网口关（RC=7） | curl exit 7 / `Connection refused`（spike#3 RC=7；机制=agent 退→反向隧道掉→broker frp 撤该转发，**非** evict 显式 listener-teardown） | **诚实框**：与 proc-leak 对比——口被清但因隧道掉线；若将来 evict 加显式 listener-teardown 亦 GREEN（此臂不 flip，注机制变化） | ④ 真反向-TCP listener 命运 |

C 注（centerpiece GAP，spike#3 定形）：**divergence**——broker DB 行没（C-brk）yet 真 OS proc 活（C-GAP-proc）=gotcha #26
（**只子进程泄漏、部署条件式**）；公网口如实钉「被清但机制是隧道掉线副作用」（C-port，非泄漏、非 evict 显式拆除）。

**Arm E — `session rm` 3-phase + `session_deleting` probe（session-level；agt1 无需在线）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| E-base | 存在 baseline（五要素①） | `CTL(session ls)` `lab ACTIVE`；`BADMIN sessions` lab；`BADMIN audit lab -n 5` OK | lab + history 存在 | `lab.*ACTIVE`；`#[0-9]+` | 删前证目标存在 | — |
| E-sub2 | 起 sys.events 观测（A6：直接 oracle） | `nats_as <owner> lab sub 'tether.v2.sys.events' > $CAP2 2>&1 &`（member JWT 有 sys.events sub permissions.go:147；24h JWT 不因 session 销毁吊销故存活收事件） | sub live | readiness marker | 修正 81 原 F3「member 无 sys.events sub」误判（A6） | ③ |
| E1 | session rm（owner） | `CTL(session rm lab)` | OK | success（sessions.go:130-200→`SessionRmResp{OK:true}`） | rm OK ≠ phase 完成——E2* 验 RESULT | ③ owner-only tombstone |
| E2a | phase ②——history-lab stream 删（RESULT） | poll：`BADMIN audit lab -n 5`→error | `history_unavailable` | `history_unavailable`（admin.go:37-39；audit.go:73-77 DeleteHistoryStream） | (e) 验 STREAM 没 | ③ 真 JS stream 删 |
| E2b | phase ③——SQLite 残留没（RESULT） | poll：`BADMIN sessions` 无 lab **且** `BADMIN nodes` 无 lab 行 | 7-表 cascade drop | `! lab` in sessions；`! agt1` in nodes（audit.go:99-138 dropSessionRows） | E-base 证 lab 在→absence=drop 成 | ③ 真 cascade |
| E2c | phase ④——`session_destroyed` 直接 sys.events oracle | poll：`grep '"type":"session_destroyed"' + '"sid":"lab"' in $CAP2` | 事件捕获 | `"type":"session_destroyed"` 且 `"sid":"lab"`（audit.go:94 pubSysEvent；events 流 tether.v2.sys.events 非 history-<sid>） | 修正原「mechanism TBD」——member observer 真收 | ③ |
| E3a | DELETING/absent 拒 on exec = broker IsActive gate（probe） | `CTL(exec agt1 -- true)` on 移除 sid | 拒 | `session_not_found_or_deleting`（exec.go:49-55；IsActive false session.go:133-140） | IsActive gate 先于 node lookup | **deploy broker 同步 gate 是真拒路径——非 agent 侧广播** |
| E3b | 同拒 on expose（独立路径） | `CTL(expose agt1 --local 8080 --name x)` on 移除 sid | 拒 | `session_not_found_or_deleting`（expose.go:178-184） | 第 2 独立码路径证是 broker gate | deploy 同 gate 异 verb |
| E3c | rm API 自己码保持 DISTINCT | `CTL(session rm lab)` 再（post-完成） | 拒 `not_found` | `not_found`（sessions.go:167-168）——**非** `session_not_found_or_deleting`、**非** `already_deleting` | 保持 anchor 区分：session-scoped→`session_not_found_or_deleting`；rm API on absent→`not_found`；DELETING 瞬态→`already_deleting`（hermetic-only） | deploy 真 error taxonomy |

E 注（DELETING 窗口探索→定格诚实）：N=1 单模式 `handleSessionRm` 同步跑 phase ②③④ **在回 OK 前**（sessions.go:196 finalizeSessionRm
inside handler），故 live `state=DELETING` 瞬态不可靠观测——rm 返时行已删。

> **⚑ 实现订正（Stage-B 实测 + 内审 R9/R17 确认；本段 above 的 IsActive-gate 框架被此取代）**：rm 完成后 session **已删**，
> 下一次 session-scoped 调用（exec/expose/2nd-rm，均以 `tether-cli:<sid>` activated 连接 session.go:163）在 **auth_callout
> CONNECT 处**被拒（`ensureMember`→session-not-active→通用 `Authorization Violation`），**从不到达**应用层 `IsActive` gate。
> 故 `session_not_found_or_deleting`（gate 码）+ `not_found`/`already_deleting`（rm-API 码）在 deploy-tier N=1 **均不可达 =
> hermetic-only**。drill E3a/E3b/E3c 均断 **CONNECT-deny**（`auth_callout rejected|Authorization Violation`）。DOC-11 核心
> 「broker 侧强制、无 agent 广播」不变。（inventory S2 landing + 台账 DOC-11 已按此订正；line 454/462/626/656 的 IsActive-gate
> 措辞以本段为准。）

**session_deleting 广播 probe（inventory §1.2）**：usage §5.10 承诺 broadcast，无 pubSysEvent writer——真机制=E3a/E3b/E3c 的
broker CONNECT-deny（见上订正）；登 DOC-11（§4.2），不因 rm 旅程跑通记 covered。

**§0.4 五要素**（Arm C evict-with-proc+expose / Arm D re-join 撤销臂 / Arm E session rm）：已并入综合稿 §3.2 五要素段——Arm C
基线=agt1 ONLINE+child RUNNING+expose curl-live（三者破前皆活）、观测=agt1 pgrep（OS ground truth）+跨容器 curl+BADMIN nodes/ps-a
（broker DB 视图，刻意对比暴露 divergence）、注入=admin evict、oracle=C-brk/C-exit/C-GAP-proc/C-GAP-proc-sysd/C-port（验结果）、
cleanup=trap `pkill -f 'sleep 999999'` + tear server + `expose rm` + nuke；Arm D 有 fp 相等异源成功控；Arm E 有 E-base 基线 +
poll async RESULT oracle + `kill $SUBPID` trap。

### 3.3 `82-agent-onboarding-invite`（GREEN，N=2 cluster + fresh agent；消费 S0-ingress）

**一句话**：真 C2 onboarding——leader `cluster seeds publish --sid` 铸 agent-join invite → **fresh agent 容器** `tether agent
join '<invite>' --start` → ONLINE；+ well-known manifest loopback + 跨容器（经 S0-ingress）两腿、C1 grow 收敛、
`agent config refresh`/`doctor`、信任锚负例、user-service spike。

**头注 false-green 横幅（必写）**：
(a) invite「铸出来了」只因回显——只 grep 真 `tether-invite:v1?` token 行（cluster_seeds.go:66），捕获 token 直接喂 `agent join`
真解析（P1→J1 端到端）。
(b) agent「ONLINE」只因残留旧行——agt1 **fresh 容器、从未 ONLINE 过**，故 J1 首个 ONLINE 只能是本次 join 后真注册。
(c) manifest 腿「验签通过」只因 curl 拿 200 却是 503/空——断 body 是签名 JSON（`jq -e '.roster and .seeds'`、非 503 manifest.go:71）。
(d) 跨容器 https「可达」只因 S0-ingress 明文兜底或改绑 loopback——正例经 agent 真 Go-x509 fetch（J4），配不受信/wrong-SAN 负例；
产品 loopback listener 保持 loopback-only（manifest.go:26-27，OQ-5，永不改绑、永不 `TETHER_DEV_NO_AUTH`）。
(e) 伪造 invite「被拒」只因 nid 缺失/环境墙而非 pin 校验——每负例 sig 钉精确文案，且先满足 `--nid`（否则先撞 `--nid is required`）。
(f) refresh「收敛了」只因退出码 0 却 roster_gen 没动——C1 验 RESULT（refresh 打印 `roster_gen=N` 严格 > grow 前 G1）。
(g) doctor「全绿」只因把 FATAL 当 WARN——FATAL 变体臂（T2）独立断 exit 77。
(h) 「无残留」被高声宣称却其实写了 agent.yaml——T2 钉「精确残留现实」而非空喊零残留。
(i) manifest 腿「serve-ready」只因 sim 手启 manifest_listen——**先断默认关**（gotcha #27）再启用。

**拓扑 + setup + 依赖**：`up --brokers 2 --agents 1 --ctl 1`（**agt1 fresh**——provision-node.sh agent 角色只建 sim 用户 +
`/home/sim/.tether` + 拷入未 enable 的 baked unit，`provision-node.sh:60-70`，**从未绑 nkey / 从未 join**）。82 **直接驱**
`tether agent join`（不经 sim `cmd_agent_join --pin`）——正是 §1.4 债要补的 C2 真路径。**只依赖 S0-ingress**（ONLINE 主旅程走
client :4222 不拨反向隧道，expose 归 81）。setup 序：① `[env] up`；② `[tether] init brk1`（N=1）；③ **[env] gotcha #27 先断**：
bare init 后 `dexec brk1 -- curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:7480/.well-known/tether/cluster.json` →
**connection refused / 无 listener**（`assert_ok` documenting gap，sig `Connection refused`/curl exit 7）；④ **[env]
`ingress_enable_manifest brk1`**（labeled operator step）+ poll leader 复健；⑤ **[env] S0-ingress bring-up on brk1**
（`secrets_mint_ingress` + `ingress_up brk1` + `ingress_trust_inject agt1`）；⑥ `[tether] session lab --pin 135790` → ctl1 login。
helper：`CTL(...)`=`dexec -u sim ctl1 -- env HOME=/home/sim tether ...`；`AJOIN(...)`=`dexec -u sim agt1 -- env HOME=/home/sim
tether ...`；`AJOIN_H(<home> ...)`=同 + `TETHER_HOME=<home>`（负例隔离）；`BROK(...)`=`dexec -u tether brk1 -- tether ...`。

**Arm P — leader 铸 invite（C2 leader 侧；首次 publish = 产品语义）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| P0 | 首次 publish 前无 seed bundle | `BROK(cluster seeds show)` | endpoints 空/未发布 | `endpoints:[[:space:]]*$`（cluster_seeds.go:104） | 证首发是手动产品语义（seed_converge.go:55-56）——先立注入前基线 | ③ |
| P1 | seeds publish 铸 agent-join invite | `BROK(cluster seeds publish --bootstrap https://brk1/.well-known/tether/cluster.json --endpoint nats://brk1:4222 --sid lab)` | 打印 `seeds published` + `Agent invite:` 含 `tether-invite:v1?...&sid=lab&...` | `Agent invite`（cluster_seeds.go:66）**且** `tether-invite:v1\?`（invite.go:63）**且** `sid=lab` | 必须 `--sid`+`--bootstrap` 才走 MintInvite（cluster_seeds.go:59-62）；捕获 token 喂 J1 | ③ MintInvite SID-必填、pin=account pub、seed=endpoints[0] |

**[纠正·已采纳]** `cluster invite`（cluster_pin.go:97-125，MintDiscoveryInvite）铸 SID-LESS discovery token 供 `cluster pin`
（cli-failover，S8-91）——它故意 FAIL `ParseInvite`（invite.go:139-141），`agent join` 用 `ParseInvite` 要 sid（invite.go:93-94），
**永不接受 discovery token**。S2-82 唯一 agent-join invite 来源 = `cluster seeds publish --sid`。→ drill 82 用 P1，**删 `cluster
invite`**；inventory line 167 的 S2-82 映射改**仅** S8-91。

**Arm M — well-known manifest 两腿（loopback 验签 + 跨容器经 S0-ingress）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| M1 | broker 本机 loopback 验签 | `dexec brk1 -- curl -s http://127.0.0.1:7480/.well-known/tether/cluster.json` | 200 + 签名 JSON | `jq -e '.roster and .seeds'` 真（manifest.go:17 ManifestPath）**且** `! grep 'not available'`（manifest.go:71 503） | curl 拿 200 却 503 空 body→jq 门 + 反 503 守卫 | deploy 真 loopback HTTP 服签名 manifest |
| M2 | 跨容器 https 经 S0-ingress + 正向证书校验（curl 次级 sanity，load-bearing 是 J4） | `dexec agt1 -- curl -s --cacert /usr/local/share/ca-certificates/tether-sim-ca.crt https://brk1/.well-known/tether/cluster.json` | 200 + 同签名 JSON | `jq -e '.roster and .seeds'` 真；curl exit 0（SAN=brk1） | curl 是 OpenSSL sanity，非 agent Go-x509 oracle（J4） | deploy 跨容器 HTTPS→同 netns 反代→loopback（OQ-5 边界不改绑） |

**Arm J — join 旅程（fresh agent → ONLINE + on-disk 真相 + refresh + doctor + TLS 正/负）**

Setup 后捕 P1 invite token 到 `$INV`。**TLS oracle 序**：J1（join pin account，manifest fetch best-effort）→ J4/I-POS（refresh
over trusted front = 硬 TLS 正）→ I-NEG-SAN（refresh over :8444 wrong-SAN）→ I-NEG-CA（fresh 未注入容器）。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| J1 | fresh agent join --start → ONLINE | 后台 `AJOIN(agent join "$INV" --nid agt1 --pin 135790 --start)`（setsid/nohup detach，捕 PID，user sim）；`poll_until CTL(node ls)→agt1 ONLINE` | agt1 ctl roster ONLINE | `^agt1[[:space:]]+ONLINE`（poll_until 验 RESULT） | agt1 fresh 从未 ONLINE→首 ONLINE 真；`--start` 前台在进程内跑→detach + trap-kill（PIN 仅首连 argv 不落 unit，agent_join.go:100,144-182） | **deploy 真 auth_callout 首连 provision（`ProvisionWithPIN` handler.go:281）+ 真 invite parse + 真 agent.yaml 落真路径** |
| J2 | agent.yaml 落真路径 + 0600 + 字段 | `AJOIN(stat -c %a /home/sim/.tether/agent/lab/agent.yaml)` + grep 内容 | mode 600；含 `broker_url: nats://brk1:4222`、`account_pub: <REAL>`、`bootstrap_url: https://brk1/...`、`session: lab`、`nid: agt1` | mode `^600$`（agent_join.go:136）；on-disk yaml tag `broker_url:`/`account_pub:`/`bootstrap_url:`/`session:`/`nid:`（agentYAML struct tag agent.go:27-36；子树 0700 agent_join.go:130-131） | 只断存在会漏错字段；account_pub 必须 == P1 真 account pub | deploy 真 install.sh 路径 0600/0700 on-disk 配置 |
| J3 | roster cache 预热成功 | `AJOIN(test -f /home/sim/.tether/agent/lab/roster_cache.json)`（roster_cache.go:26）**且** J1 stdout 无 `manifest pre-warm skipped` | 预热 cache 在场 | cache 文件存在（agent_join.go:78-81）**且** `! grep 'pre-warm skipped'`（agent_join.go:90） | 预热仅当 fetchErr==nil（S0-ingress 通）**且** AdoptDecision ok——钉住 S0-ingress 真通 + 真验签；挂则 J1 打 skipped→RED（诚实暴露非 mask） | deploy join 期真 HTTPS manifest fetch + AdoptDecision |
| J4 | config refresh --once = 硬 TLS 正 oracle（I-POS，post-J1-pin） | `AJOIN(agent config refresh --once --session lab)` | `roster refreshed (roster_gen=<N> seed_gen=<M>)` | `roster refreshed`（agent_config.go:74） | 必须 J1 先 pin account（否则 refuses TOFU-from-HTTP :52）；需 `--once`+`--session`；抓 roster_gen 供 C1 基线；经 trusted front 真 Go-x509 验（fetch.go 无 InsecureSkipVerify） | deploy 真 well-known 签名 bundle fetch + 对已 pin account 验签 |
| I-NEG-SAN | wrong-SAN 证书拒（CA 已信，证 hostname 验，主负例） | 铸 `secrets_mint_ingress <inst> brk1 ingress-wrongsan notabroker`（SAN=notabroker）+ `ingress_up brk1 8444 ingress-wrongsan`（2nd sidecar）；`AJOIN(agent config refresh --once --session lab)` bootstrap 指 `https://brk1:8444/…` | 拒，hostname mismatch | `x509: certificate is valid for .*not brk1`（fetch.go:52 wrap） | CA 已信→唯一拒因是 SAN；skip-verify build 会 PASS；post-pin 跑 | ③ 负控 |
| I-NEG-CA | 不受信 CA 拒（fresh 未注入容器） | 在未 `ingress_trust_inject` 的容器（或 J 前）`agent config refresh --once` 打 `https://brk1/…` | 拒，unknown authority | `x509: certificate signed by unknown authority`（fetch.go:52） | order-fragile：agt1 系统信任已装 CA→须用 fresh 容器；I-POS(J4) 证同 fetch CA 信任后成功（不同态控） | ③ 负控 |
| J5 | doctor 全绿 exit 0 | `AJOIN(agent doctor --session lab)` | 退 0；无 FATAL | 退 0（assert_ok）**且** `well-known manifest verifies against the pin`（agent_doctor.go:134，非 `manifest.*OK` 不匹配）**且** `! grep FATAL` | doctor 只读绝不 mint/CONNECT/写；manifest 抓失败降 WARN（:123 非 FATAL）故 exit 0 稳；FATAL 变体见 T2 | deploy 真栈只读诊断 |

**Arm C1 — grow 收敛（状态变更臂）**

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| C1 | grow 加 broker→agent roster 收敛（roster_gen 跳变，不声称 stale 事件） | 基线 `G1=J4 的 roster_gen`；`[tether] grow brk2`（cmd_grow，N=2）；`AJOIN(agent config refresh --once --session lab)`→`roster_gen=G2` | **G2 > G1**（严格增） | 抽两次 `roster_gen=([0-9]+)`（agent_config.go:74），断 `G2 > G1`（AdoptDecision 只在 gen 更高才 adopt :64-67） | 只断「refresh exit 0」→gen 没动也绿；比数值。**不断 `agent_roster_stale`**（6-min grace 结构上不触发） | **deploy 真 raft grow→新签名 roster→agent 真验签 adopt** |

**[纠正·已采纳]** `agent_roster_stale` 6-min grace（roster_stale.go:24 `rosterStaleGrace=6min`；`rosterStaleShouldEmit :76-81` 要
`agentGen≠0 && agentGen<currentGen && now-lastChange>grace`）→ grow 刚发生 lastChange 很新→6-min 内必不发；且 fresh agent join
后 cache 已新 gen（收敛态）→报 agentGen==currentGen→永不 flag。→ **`agent_roster_stale` 记 NOT-COVERED-in-sim（实测理由=6-min
grace + 收敛-agent 报同 gen；固定 sleep 6-min 死等违工艺）**，C1 用 roster_gen 跳变展示、不声称该事件；谓词 hermetic 覆盖。inventory
line 32 处置从「S2-82 断言」改「S2-82 NOT-COVERED + 谓词留 hermetic」。

**Arm T — 信任锚负例（TETHER_HOME 隔离，绝不污染 J 的好 agent.yaml）**

负例用独立 `TETHER_HOME`，残留断言打 scratch home。**伪造 pin = valid-但-非本簇 account pub**（MintInvite/ParseInvite 都要
`nkeys.IsValidPublicAccountKey` invite.go:42,87）：`DECOY=$(dexec agt1 -- sh -c 'nk -gen account | nk -inkey /dev/stdin -pubout')`
（baked nk），P1 invite 的 `pin=` 换 `$DECOY` 成 `$FORGED`；`REAL=$(secrets_account_pub $INSTANCE)`（secrets.sh:66）。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| T1 | 伪造 pin + `--expect-account-pub` → 写前即拒 + 无残留 | `AJOIN_H(/home/sim/.tether-forge, agent join "$FORGED" --nid agt1 --expect-account-pub "$REAL" --pin 135790)` | 拒；agent.yaml 未写 | `does not match the invite's pin`（agent_join.go:41）**且** `! test -e /home/sim/.tether-forge/agent/lab/agent.yaml`（拒在 :40-42 早于 writeAgentConfig :71） | expect-check 先于 nid-check；残留断言证「写前拒」 | 防御纵深：pin 校验先于任何持久化 |
| T2 | 伪造 pin + 无 `--expect` + inline seed → agent.yaml 残留 / cache 缺 / doctor FATAL | `AJOIN_H(/home/sim/.tether-forge2, agent join "$FORGED" --nid agt1 --pin 135790)`（无 --start/--expect）；再 `AJOIN_H(/home/sim/.tether-forge2, agent doctor --session lab)` | (a) agent.yaml **存在**；(b) roster cache **不存在**；(c) doctor **exit 77 + manifest FATAL** | (a) `test -f .tether-forge2/agent/lab/agent.yaml`（inline seed 非空→brokerURL 非空→writeAgentConfig agent_join.go:59-73）；(b) `! test -e .../roster_cache.json`（AdoptDecision 验签失败→!ok→不写 :75-83）；(c) exit `77`（exitcode.go:34）+ `roster does NOT verify against the pinned account`（agent_doctor.go:125） | 勿空喊零残留——现实 yaml 有、cache 无、doctor FATAL 兜底；三点齐钉。注 identity-FATAL 共驱（.tether-forge2 无 --start→无 agent.nk→identity 检查也 FATAL agent_doctor.go:80-85）；manifest-FATAL 串门控在 ingress up | **归类 DOC-7（有意纵深防御）** |
| T3 | 篡改：未知参数→parse 前拒 | `AJOIN_H(/home/sim/.tether-t3, agent join "tether-invite:v1?pin=$REAL&url=https://brk1/.well-known/tether/cluster.json&sid=lab&seed=nats://brk1:4222&bogus=1" --nid agt1 --pin 135790)` | 拒；无 agent.yaml | `unknown param`（invite.go:83）+ `! test -e .../agent/lab/agent.yaml`（ParseInvite RunE 首行 :36） | 严格 allowlist（invite.go:80-85） | parse 门在写前 |
| T4 | 篡改：错 scheme/version→parse 前拒 | `AJOIN_H(/home/sim/.tether-t4, agent join "tether-invite:v2?pin=$REAL&url=https://brk1/...&sid=lab" --nid agt1 --pin 135790)` | 拒；无 agent.yaml | `expected scheme .*version`（invite.go:74）+ 无残留 | v2 被拒（invite.go:73） | 同上 |
| T5 | 篡改：非 https bootstrap→parse 前拒 | `AJOIN_H(/home/sim/.tether-t5, agent join "tether-invite:v1?pin=$REAL&url=http://brk1/.well-known/tether/cluster.json&sid=lab&seed=nats://brk1:4222" --nid agt1 --pin 135790)` | 拒；无 agent.yaml | `bootstrap url must be https`（invite.go:192）+ 无残留 | `TETHER_DEV_NO_AUTH` 必须未设（=1 放行 http invite.go:189）——drill 断 env 不含它 | parse 门 + 明文拒绝 |

**Arm U — user-service spike（install ≠ start；usage §6.1 步骤 2 首日主路径；三选一定格如 62）**

依赖 PID1 容器内 `systemctl --user` 可用（user@UID.service/dbus/linger）。须先停 J1 前台 daemon（同 nid 同 nkey 两 daemon 争）。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | M |
|---|---|---|---|---|---|---|
| U1 | linger 起 user manager | `dexec agt1 -- loginctl enable-linger sim`；poll `user@$(id -u sim).service` active | user manager 起 | `systemctl is-active user@<uid>`==active | 容器无 user-manager 能力→整 U NOT-COVERED（实测理由） | ③ |
| U2 | `--install-user-service` 写 unit（install≠start） | `AJOIN(agent --install-user-service --session lab --nid agt1)` | 写 `~/.config/systemd/user/tether-agent@lab.service`（0644）+ 打印 next-step，**不启动** | `wrote .*tether-agent@lab.service`（agent.go:344）+ `test -f ~sim/.config/systemd/user/tether-agent@lab.service`（0644 :341）+ `loginctl enable-linger`（:348） | install 只写不启（:303 K.0）——断「文件在且此刻 agent 未 ONLINE-经-user-unit」 | ③ 真 --user template unit 落盘 |
| U3 | user-unit enable --now → 同 agent 回 ONLINE（spike 判决） | 停 J1 前台 daemon→poll OFFLINE（基线）；`dexec -u sim agt1 -- env XDG_RUNTIME_DIR=/run/user/$(id -u sim) systemctl --user daemon-reload && … enable --now tether-agent@lab.service`；poll ONLINE | 同 agent 经 user-service ONLINE | `^agt1[[:space:]]+ONLINE`（poll_until） | 三选一：(a) 全通→GREEN；(b) U2 成功但 `systemctl --user` 容器不可行→U2 GREEN + U3 NOT-COVERED（实测）；(c) linger 不行→整 U NOT-COVERED。判决须 live 断言（同 62） | deploy 真 systemd --user 生命周期 |
| U4 | `--uninstall` 卸 unit | U2 后 `AJOIN(agent --uninstall --session lab --nid agt1)`（Stage-B 核确切 flag 名/行为 agent.go:352-368）；`! test -f ~sim/.config/systemd/user/tether-agent@lab.service` | 文件没 | unit 文件被删（agent.go:362）；`! test -f …` | 写→卸→文件没；若 flag 名/语义不符则 NOT-COVERED（实测理由） | ③ 真 unit 卸载 |

**§0.4 五要素**（C1 grow 收敛 / T2 伪造-pin 残留 / U3 daemon→user-unit handoff）：已并入综合稿 §3.3 五要素段——C1 基线=J4 的
G1 + agt1 ONLINE、观测=refresh stdout roster_gen（经 AdoptDecision 验签后写）、注入=grow brk2、oracle=G2>G1 数值比较、cleanup=trap
nuke + J1 后台 daemon/U3 user-unit trap kill；T2 基线=scratch home 干净、oracle=yaml 在/cache 不在/doctor exit77 三点；U3 基线=
J1-ONLINE 证过、注入=stop J1→user-unit enable、oracle=同 agent 经 user-unit ONLINE、cleanup=trap kill。

## §4. 台账 + inventory + README 同步

### 4.1 `docs/deploy-tier-gotchas.md`（#25+ 全局连续；S1 建档、#25+ 前空落）

**编号线性化（O3；确认与 `v0.4.5-ha-grow-ops-gotchas.md` #1-24 连续）。每条模板：现象 / 机理 file:line / 怎么自动化或修 /
钉住它的 drill+签名 / flip 条件。**

- **#25 — PIN CONNECT 无 per-IP 限速（§E.6 未实现）**。现象：单源 IP 可在任一 60s 窗无限次 PIN 首连尝试，无节流。机理：
  `internal/authcallout/handler.go`（ensureMember/ensureAgentProvisioned）+ `internal/broker/authcallout.go` 无
  counter/bucket/window/per-IP map/`rate.Limiter`；client IP `jwt ClientInformation.Host` 从不读；每 wrong PIN 只发 `pin_failed`
  （handler.go:297,348）无 accounting；architecture.md:825 §E.6 承诺「≤10/IP/min」。钉：drill 80 Arm R-11th `assert_ok`（倒置预期-gotcha）。
  flip：限速落地→Arm R 翻 `assert_refuses <rate-limit-sig>` over 全 §0.4 形（同 IP 11 correct-PIN：≤10 成 + 第 11 拒 +
  second-source-IP 控 + window-reset 控）成 GREEN。修=另立叶子（或正式退役 §E.6 承诺的显式架构编辑，非静默 waiver）。
- **#26 — evict 不清理 managed OS 子进程（部署条件式；spike#3 实证）**。现象：`admin evict` 删 `agent_provisioning`+`nodes` 行
  （FK cascade 移 processes/port_allocations），broker DB 显 proc 没，yet 真 OS 子进程命运 **DIVERGE**。机理：`internal/adminsock/server.go
  handleEvict`（只 DELETE）+ `internal/node/plan.go:63-77 PlanEvict`——**无码 kill OS proc**；exec 子进程无 kill 路径（`killOrphanProcess`
  仅 PTY 可达 agent.go:1404-1435；exec.go:325 plain exec.Command 无 Setsid）。**部署条件式（spike#3 实证）**：`setsid nohup`
  （install.sh:371 头号手动启动）下子进程 reparent PID1 **SURVIVE（真 leak）**；systemd unit（默认 KillMode=control-group）下 cgroup
  teardown **顺带回收**（tether 仍啥没做）。**公网 expose 口 evict 后被清（spike#3 curl RC=7）——机制是 agent 退出→反向隧道掉线的
  副作用，非 evict 显式 listener-teardown**（如实钉，非泄漏）。钉：drill 81 C-GAP-proc（复合谓词 `pgrep -x tether` 空 AND `pgrep -f
  'sleep 999999'` 非空，setsid-nohup，`assert_ok`）+ C-GAP-proc-sysd（systemd cgroup 回收反证）+ C-port（口被清 GREEN，注机制）。倒置→
  assert_ok。flip：evict-cleanup 修落地（agent 在 `agent_evicted` kill managed proc）→ C-GAP-proc 翻断子进程被收。
- **#27 — C2 well-known discovery 在 `cluster init` 后不 serve-ready（`manifest_listen` 被 applyClusterSeam 省略 + 未文档化）**。现象：
  bare `cluster init`/`cluster add` 后 broker 不 bind manifest listener（broker.go:753 gate `ManifestAddr==""` false），C2 bootstrap-URL
  onboarding 腿默认死，operator 须手加 `manifest_listen`。机理：install.sh 整 `cluster:` 块注释含 `# manifest_listen`；`applyClusterSeam`
  （cluster.go:799）只写 data_dir/raft_addr/secrets_dir/nats_conf_path；serve unit 无 `--cluster-manifest-listen`；**`docs/usage.md`/
  `docs/cluster.md`/`docs/broker-ops.md` 零 `manifest_listen`/`well-known` 命中**。钉：drill 82 setup 步③默认关探针（bare init 后 loopback
  curl→Connection refused，`assert_ok` 记 gap），再 labeled `ingress_enable_manifest`。flip：`docs/usage §6` 文档化启用步 OR seam
  自动加 `manifest_listen`。**注**：此 gotcha 使 S0-ingress 的 `manifest_listen` 启用忠实（否则=省事-for-tether）。

### 4.2 DOC 节（续 S1 DOC-5；不占 gotcha 号；主进程分配 DOC-6+）

- **DOC-6 — 被踢 nkey 用新 session PIN 可重入（eviction ≠ revocation）**（O4 裁定：有意，非 gotcha）。机理 handler.go:246-289 无
  denylist。**语义**：evict 删 provisioning 行、不封 nkey；仅 nkey 泄漏（PIN 未泄）时重入仍需 session PIN=实际已吊销；PIN 亦泄则应
  rotate-PIN / session rm。钉 drill 81 D1（`assert_ok` + fp 相等）。修（若将来判缺口）=evict 加 denylist / 强制 PIN 轮换。
- **DOC-7 — 伪造-pin 无 `--expect` + inline seed → agent.yaml 残留**（O4 裁定：有意纵深防御，非 gotcha）。机理 agent_join.go:59-73
  brokerURL=inv.Seed 非空→writeAgentConfig 被调；但 roster-cache 由 AdoptDecision 签名验证门控（!ok 不写），残留 yaml 无信任价值、
  doctor FATAL 兜底。roadmap line 322「无半写 agent.yaml」是 overclaim。钉 drill 82 T2（三点残留 oracle）。修（若将来判缺陷）=join 在
  manifest-verify-against-pin 通过前不落 agent.yaml。
- **DOC-8 — auth_callout deny REASON 架构上 client 不可见**（§3.0）。nats-server 发通用 `Authorization Violation`（client.go:2434）；
  tether reason 只 server log + `$SYS…AUTH.ERR`。`handler.go:117-118` docstring「client sees a clear auth error」误导。非缺陷（有意
  info-hiding），记之使未来读者不在 client 断 reason 串误报 bug。
- **DOC-9 — `member_joined` 只发 `via:"pin"`；fp-reconnect 无 emit**（inventory §1.1 row-27 订正）。code emit only via=pin
  （handler.go:286 agent, :353 ctl）；already-member fp-reconnect 返 nil 无 emit（:320-330）。row-27「via=pin/fp」订正为「via=pin only；
  fp-reconnect emits no event」。80 owns。
- **DOC-10 — inventory §1.1 row-26 `session_created`/`session_destroyed` 可见性订正**。去 **`events` 流**（subject
  `tether.v2.sys.events`，audit.go:36-48），**非** `history-<sid>`；`admin audit` 只 tail `history-<sid>`（admin.go:36）；
  `session_destroyed` 且**后于** history-<sid> 删除。row-26「history/admin audit 可见」订正为「events 流可见」。81 owns。
- **DOC-11 — `session_deleting` 广播承诺但未实现**（usage.md §5.10:578-583）。承诺 broadcast `sys.events{type:session_deleting}`；
  **无 `pubSysEvent("session_deleting")` writer**（唯一字面 cluster_forward.go:255-262 是 wire error-kind classifier）。真机制=broker
  侧同步 `IsActive` gate → `session_not_found_or_deleting`。钉 drill 81 E3a/E3b。修=正 usage §5.10 / architecture H.1。**次要**：usage
  §5.10 说「H.5」，code 注为「H.3」（audit.go:53-72）——drift。
- **DOC-12 — architecture H.1 `kicked`/`agent_unregistered`/`rotated_pin` 无 writer**。`kicked`=emit 的 `agent_evicted` 命名 drift
  （broker/admin.go:93）；`agent_unregistered` 无 unregister verb；`rotated_pin` 无改-PIN verb（PIN 只能 rm 重建）。register DOC
  （修 H.1 或加 writer=另立增量）。80 owns rotated_pin；81 owns kicked/agent_unregistered。
- **DOC-13 — provision-node.sh:60-63 注释漂移**（**只 82 修一次**）：注释宣称「Agents onboard via `tether agent join <invite>`」但 sim
  `cmd_agent_join`（simcluster:332-343）实走 `--pin` 首连 + 手写 system unit。修：如实说明「sim `cmd_agent_join` 用 `--pin` 首连（usage
  §5.8 一等路径，保留）；真 C2 `agent join <invite>` 由 drill 82 演练」。
- **DOC-14 — agentyaml.sh:14-16 注释「S3's job」漂移**：S0-隧道 落 S2 → 改「S2's job（landed here）」。

### 4.3 README 重构 + 编号族（S0-台账续 S1）

- **加行**（表按号排序，每行头注记时长/资源）：`80-session-isolation`（GREEN，N=1+2agent，~3-5min 无 grow）·
  `81-admin-evict-session-rm`（GREEN，N=1，~4-6min，含 setsid-nohup 部署臂）· `82-agent-onboarding-invite`（GREEN，N=2+fresh
  agent+S0-ingress，~5-9min，含 init+grow+ingress bring-up）。三者归 **`8x` session·安全·入群族**（S1 §4.2 已立；82 带 grow →
  drill-all 分波归 grow 族波，OQ-8）。
- **doctor 漂移**：无新 doctor 检查漂移（82 J5/T2 用既有检查）。
- **S0 表**：翻 S0-隧道 + S0-ingress 状态 `未落地`→`已落地（S2；commit …）`；注 S0-隧道 默认落地批 S3→S2 转移；S2 记为 S0-ingress
  实例-CA facility owner。
- **烘焙 delta 重验**：`provision-node.sh --domain <node>` + 新 `image/ingress-proxy.py` → `remote.sh build` 后**重验既有 10 drill
  （00/10/11/12/13/20/21 + 60/61/62）仍 GREEN**（预期全绿：无 drill 断域名串、无 drill curl expose、Caddy 惰性）。

## §5. inventory 消费 / 收工闸（部分勾约定 `S2✓(<臂>) · S8☐(<臂>)`）

**§2.1/§2.2 命令树行 S2 勾（臂级）：**
- `login/logout/ctx` → S2✓(CONNECT-拒非成员 I1-neg + 非持久 I1-persist + `--broker` 别名持久 I1-broker)。
- `session rm` → S2✓(3-phase RESULT E2a/E2b/E2c + non-owner not_owner I3-rm + 二次 not_found E3c) · **S8-92(a)☐(`--ack-alerts`)**。
  `session ls/create` → S2✓(setup + E-base)。
- `admin sessions/nodes/audit/evict` → S2✓(A1/A2/A3/B1 + 非授权 EACCES A4)。
- `node upgrade` → S2✓(non-owner not_owner I3-up；owner 升级 = S5)。`node ls` → 复用(dual-home + TS)。
- `exec` → 复用(node_not_found I2c + IsActive gate E3a)；`run` → S2✓(supervised managed child leak 注入 C-base-proc，探索→定格)。
- `expose` → S2◐(active-expose fixture C-base-expose + evict 后口命运 C-port，探索→定格) · **S3-70☐(expose 主旅程/rehome)**。
- `agent join` → S2✓(nid/pin/expect/伪造/篡改 T1-T5 + 无-expect 残留现实 T2 + --start ONLINE J1)。`agent config refresh` → S2✓(J4 +
  C1)。`agent doctor` → S2✓(全绿 J5 + FATAL exit77 T2)。`agent --install-user-service/--uninstall` → S2 spike(U2✓ + U3/U4 按实测)。
  `cluster seeds publish/show` → S2✓(P0 + P1 首发+invite)。
- **`cluster invite` 不勾 S2**（归 S8-91 cli-failover，纠正 inventory line 167）。
- Tier-2 hidden `--yes` rejectors + machine-confirm 面：**S2 不触**（rm 无 typed-confirm；evict 无 --yes）——显式登 NOT-COVERED-this-batch。

**§1.1/§1.2 事件面 S2 勾（臂级）：**
- `member_joined`（row-27）→ **S2-80✓**(E-joined ctl via=pin 经真 sys.events) + DOC-9 订正。
- `pin_failed` → **S2-80✓**(E-pinfailed 独立正向 oracle)。
- `session_created`（row-26）→ **S2-81✓**(setup + E-base 行为投影) + DOC-10 订正。
- `session_destroyed`（row-26）→ **S2-81✓**(E2c 直接 sys.events oracle)。
- `agent_evicted` → **S2-81✓**(B2a/C-exit 行为 oracle=跨进程自退即广播证据)。
- `agent_registered`（row-30）→ **S2-82 exercised**(J1 fresh register)，直断留 S9-94；82 主 oracle=node ls ONLINE。
- `agent_roster_stale`（row-32）→ **S2-82 NOT-COVERED-in-sim**(6-min grace；谓词 hermetic)；C1 用 roster_gen 跳变。
- H.1 无-writer probe：`kicked`/`agent_unregistered`（81）·`rotated_pin`（80）·`session_deleting`（81）→ 全 DOC candidate（§4.2），不因
  旅程跑通记 covered。

**收工闸 checklist（S2 零产品 Go diff → 命令树 + 事件生成法皆 trivially 零-diff 但必跑必记）：**
1. `make test` + `make e2e` + `make lint` 绿（守恒；命令树校验器须绿）。
2. **命令树重枚举** via S1 落的 `TestCommandTreeInventory`（`go test ./cmd/tether -run TestCommandTreeInventory`）→ **断零 diff** vs
   清单 §2（S2 无 CLI 改）。收工重推真实 path 数、勿把「94」当真相；非零 diff=调查勿脏收。
3. **事件生成法**重跑（`grep pubSysEvent` + authcallout `h.emit` + `emitDrainEvent` + `proxy_cluster.go` + `alert_ops.go`）→ **记 0 条
   S2-引入 kind**（80/81/82 只断既有 kind；null-diff 非跳过）。
4. inventory §1/§2 hand-edit **先落行再收工**：row-26/27/32 订正 + line 167 remap + 臂级勾/NOT-COVERED/probe/DOC。
5. 台账 #25/#26/#27 + DOC-6…14 落地；README 8x 族 + 80/81/82 行 + S0 表翻状态；烘焙重验既有 10 drill 绿。
6. `80`/`81`/`82` 在 run-drills 套件绿（落盘自动发现）；tunnel T0-T6（spike#1 PASS）+ nats_as（spike#4 PASS）+ evict-leak 定格
   （spike#3）皆 live 判决入盘。

## §6. OQ 裁定

- **OQ-5（S0-ingress loopback 边界）——已裁定**（roadmap:851-858）。S0-ingress = 同 netns TLS 反代，产品两 loopback listener 永不改绑、
  `TETHER_DEV_NO_AUTH` 永不设（manifest.go:19-27 / subhttp.go:34-46）。活体 netns-share spike#2 PASS 确认机制。
- **OQ-8（横切分族两 pass，S1 立制）**：S2 加 80/81(N=1 无 grow)→归 N=1 族波全并行；82(N=2 含 grow)→归 grow 族波 serial 或 `-j 2`。
  81 的 setsid-nohup 部署臂 + C-GAP-proc-sysd systemd 反证在同 drill 内两 sub-pass（顺序两 agent 部署），不加新 grow 负载。记
  wall-clock 为 OQ-8 数据点。
- **S2 提出（已裁）**：OQ-nats_as（O2）= spike#4 PASS，Oracle ② 采纳；OQ-evict-leak（O1）= spike#3 定 gotcha #26 部署条件式 +
  port-cleaned-incidentally；OQ-manifest-listen（O5）= gotcha #27，drill 82 先断默认关。

## §7. 验收出口

1. `80` GREEN + `81` GREEN + `82` GREEN 在 `run-drills.sh` 套件；每 GREEN 含其探索→定格预期-gotcha 臂（80 Arm R #25 / 81 Arm C #26 /
   82 setup #27）已 live 判决。
2. **S0-隧道 + S0-ingress 落地 + 已验**：S0-隧道 T0-T6 数据面铁证（spike#1 PASS，作回归门）；S0-ingress netns-share + TLS 正（J4/M2）+
   负（I-NEG-SAN/I-NEG-CA）；S0 表翻状态；S2 记 S0-ingress 实例-CA facility owner。
3. harness 增量落地：`provision-node.sh --domain <node>`（烘焙）+ `image/ingress-proxy.py`（烘焙）+ `drills/lib/ingress.sh`
   （secrets_mint_ingress/ingress_enable_manifest/ingress_up/ingress_down/ingress_trust_inject）+ `drills/lib/ident.sh`
   （CTLH/CTLHS/nats_as/bind_agent）——各带 Mandate-④ 注 + 7 字段生命周期元组（§2）；agentyaml.sh:14-16 + provision-node.sh:60-63
   注释漂移修（§4.2）；烘焙重验既有 10 drill 绿。
4. 台账 #25/#26/#27（含 flip 条件）+ DOC-6…14 落地；O3 编号线性化已落。
5. 收工闸过：命令树重枚举=零 diff（数重推非假设）；事件生成法=0 条 S2-引入 kind（null-diff 记）；inventory §1/§2 订正（row-26/27/32 +
   line 167 remap）+ 臂级部分勾 + NOT-COVERED（`agent_roster_stale` 6-min-grace / Tier-2 --yes / U3/U4 按实测）带实测理由；无 S2-owned
   行未触及-且-未登记。
6. 每个倒置臂 signature-guarded（倒置 `assert_ok` + 台账 flip）；每个 S0-隧道/ingress-gated 腿 live 或 NOT-COVERED 带诚实理由；每处不得已
   手动步（`ingress_enable_manifest` = gotcha #27 gap 的可见 operator 供给）显式标；无 green-for-the-wrong-reason（三 drill 头注 banner 守卫全 hold）。
7. `make test` + `make e2e` + `make lint` 绿；drill-all 按族分波两 pass（80/81 N=1 族并行 + 82 grow 族 serial/`-j 2`，OQ-8）。
   **外审不过不算 done**——本 plan 定稿后走对抗内审 → 用户外审 → 才 commit；**停在外审，不自 commit**。
