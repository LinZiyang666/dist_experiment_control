# B5 plan — Ops day-2 可观测性（轻量）

> Stage A：9×Opus 对抗规划（4 drafter metrics-health / logs-watch-wait / cert-capacity-cordon / natsconf-plan-scope-risk → 4 critic byte-equiv-security / scope-creep / correctness-reuse / test-adversary → 1 synth）。synth 现场核验全部 load-bearing claim（file:line）。主进程定稿采纳。
>
> **范围**：轻量地把集群**已经算好**的值暴露出来——零新 migration、零新 replicated op、零新 agent→broker 通道。重项（cordon 全功能、js_store_used_pct、agents_repinned、rotate-wait、跨 peer 容量聚合）**全部 DEFER 到 B6**。**非集群字节等价 + proto 不 bump** 是硬约束。

## §0 — 8 项 IN-vs-DEFER 裁定（定稿）

| # | item | 裁定 | 一句话理由（已核验） |
|---|---|---|---|
| **OPS#8** | `--log-level`/`--log-json` + key-path attrs | **IN** | 单一 handler 点：`serve.go:145`、`agent.go:213`（都是 `slog.New(slog.NewTextHandler(os.Stderr,nil))`）。off-by-default = 字节等价。 |
| **OPS#5** | `cluster status --watch` | **IN** | CLI 重绘循环，复用 `callAdmin(OpClusterStatus)`+render。**但**每帧不可调 2s-blocking 的 `StatusReport`（§1.3）。 |
| **OPS#13/AUTO#5/#6** | `--wait`/`--timeout` + `cluster wait` | **IN（4 verb 取 3）** | poll 既有 phase RPC。**`rotate-tunnel-cert --wait` = DROP**（no-op）。timeout→exit 75。 |
| **OPS#7** | cert-rotation 可见性 | **PARTIAL IN** | cert_fp/prev/valid 暴露 = IN（additive omitempty、公开 fp）。**`agents_repinned N/M` = DEFER-B6**（无 per-agent repin 计数）。near-expiry = **advisory only，绝不 DEGRADE**。 |
| **OPS#9** | 容量信号 | **PARTIAL IN** | `disk_free_pct` + `ports_used/total` = IN（self-row only）。**`js_store_used_pct` = DEFER-B6**（需新 `js.AccountInfo` 往返；`ReplicaReport` 无 store-bytes 字段）。 |
| **OPS#6** | maintenance / cordon | **DEFER-B6（全）** | 真 cordon = "停新放置、保留现有 expose"。`OpClusterDrain` 会**迁走** expose（语义错）。`Eligible()==Phase==VOTER` 无 cordon 维度 → 需持久化 flag + 改 Eligible() = 重。 |
| **OPS#1** | `/metrics` + `/healthz` + `/readyz` | **IN（最重，放最后）** | 值都经 raft 直接 accessor 取（廉价）。**绝不读 `StatusReport`**（每次 scrape 阻塞 2s + NATS scatter + JS fan-out）。prometheus 非直接依赖 → 手写 text exposition。 |
| **OPS#10/AUTO#10** | `takeover-natsconf --plan` | **IN** | 纯 dry-run，helper 都在，在 `Apply` 前分支。从渲染好的 `merged` 读路由，不碰 `Render`。 |

**B5 净新代码预算（"轻量"判据）**：一个 HTTP listener（metrics）+ CLI 循环/flag + ~5 个 additive omitempty socket 字段 + Broker 上一个 cached-observe-snapshot 字段。**零 migration、零 replicated op、零 agent→broker 通道。**

### 已解决的分歧（逐条）

1. **Scrape 成本（test-adversary CRITICAL — 采纳）**：drafts 误称 `StatusReport` 被 5s observe loop 缓存。**核验为假**：`StatusReport`（clusterstatus.go:90）同步调 `a.healthPoll()`（:173-174）= live `pollClusterHealth` scatter-gather 阻塞 `observePollWindow=2s`，外加 `a.streamObserve()`（:160）= O(streams) JS-API 调用；observe loop 的 `observeOnce` 结果**无可读缓存**。→ **`/metrics` 直接读 raft accessor；`--watch` 保留 `StatusReport`（人在 2s 容忍）但 interval clamp ≥2s 并文档化"每帧最多 ~2s"**。metrics peer-lag 用一个新的 cached snapshot（§1.1，唯一净新 broker 侧件）。
2. **`js_store_used_pct`（DEFER）**：`ReplicaReport{Streams,Observed}` 无 store bytes（audit_publisher.go:440-442）；只有 `js.AccountInfo`（broker.go:705-712）有，是读路径上新往返。`disk_free_pct` 已覆盖"store 目录涨满"。→ **DEFER-B6**。
3. **Cordon（DEFER 全）**：cert-capacity 的 "cordon = `OpClusterDrain{Retire:false}`" 会 `migrateExposes` 迁走 expose——那是 drain 不是 cordon。→ **DEFER-B6**，不发错名 drain 别名。computeHealth "(planned drain)" page-suppression 也 cut（会改既有 draining-node 输出、违字节等价；phaseDraining 已 `degraded=true` 于 :300，broker_draining 告警已 `AlertSeverityInfo`，误 page 已在下层解决）。
4. **`rotate-tunnel-cert --wait`（DROP）**：`RotateTunnelCert` 要求 `nodeID==SelfID()` 且 leader（clusterdrain.go:274），fp 在同一 `Propose` **同步**写（:291）→ verb 返回成功时 fp 已本地 committed → poll 自己的行第一轮即收敛 = no-op。真正 async 的是 DEFER-B6 的 `agents_repinned`。→ **`--wait` 只用于 `add`/`drain`/`transfer-leader`**。（顺带消除跨 lens cert 依赖。）
5. **`term` attr（IN 但不在任何路径）**：无 `AppliedTerm()` accessor，且 applied/command-domain term ≠ operator 关联 raft 日志要的 leadership term（后者在 `raft.Stats()["term"]`）。→ **log attr 发 `applied_index`；leadership-acquired 行读 `raft.Stats()` 的 raft term**，不建 `AppliedTerm()`。不在任何 `--wait` 收敛路径。
6. **`/readyz` band（correctness-reuse — 采纳，反转 metrics draft）**：metrics draft 说 ready ⟺ `health==HEALTHY_HA`。**错**：computeHealth 对良性态返 DEGRADED——phaseDraining/Retiring/CatchingUp（:300）和 F==0 即健康 2-voter（:322-325）。那样 `/readyz`→503 会在维护中把 serving broker 从 LB 摘掉。→ **Ready ⟺ `health != QUORUM_LOST` 且本节点 `Phase==VOTER` 且 not inconsistent**。DEGRADED-but-serving = ready(200)。2-voter 显式 pin 200。
7. **Near-expiry 窗口**：7-day 与 `certRotationWindow=24h`（:61）不自洽（CertValid≤now+24h，7d 永远亮）。→ **窗口 = 实际 rotation 窗口的一个分数（render 时读 `CertValid.Sub(now) < window/8`，24h 默认下 ~3h），派生非硬编码**。**advisory only**。
8. **ports SQL scoping**：`port_allocations` 是复制表；裸 `COUNT(*) WHERE state='ALLOCATED'` 会把整集群计到一个 broker 的 band。→ **`COUNT(*) WHERE home_broker=<selfID> AND state='ALLOCATED'`**（`countOwnedExposes` 既有 pattern，clusterdrain.go:230）。
9. **disk 阈值**：不硬编码 `diskFreeDegradePct=10`；从 operator 可配的 `cfg.DiskPressureThreshold`（disk.go:40）派生，使 DEGRADE band 严格紧于既有告警、永不漂移。disk 探测 **self/online-only，绝不在 offline disk-roster 路径**（stale/unmounted statfs 会挂，v0.3.3 remote-fs 隐患）。
10. **StoreDir/disk 访问路径**：`StatusReport` 是 `*ClusterAdmin` 方法（持 `a.node`，无 `cfg.StoreDir`）。→ **把 `StoreDir`/`selfID`/`portBandLow/High` 在构造时 thread 进 `ClusterAdmin`**（NewClusterAdmin, clusterwrite.go:290）。
11. **subhttp.Bind 复用**：`subhttp.Bind` 硬拒非 loopback（subhttp.go:218 requireLoopback）。metrics 需私网 bind。→ **`brokermetrics.Bind` = 裸 `net.Listen`，无 loopback guard**；只复用 `ServeListener`/`Shutdown`/`ReadHeaderTimeout=5s` lifecycle 形（subhttp.go:229-238）。

---

## §1 — IN 项的具体形状

### §1.1 OPS#1 — `/metrics` + `/healthz` + `/readyz`

新建 `internal/brokermetrics/metrics.go`(+`_test.go`) — leaf，只 import `net/http`/`io`/`context`，不 import `internal/cluster`、不碰 NATS。
```go
type Snapshot struct {
    ClusterMode bool
    IsLeader    bool
    Voters      int
    FaultTol    int     // quorum_margin
    AppliedIndex, CommitIndex uint64
    ForceSingle bool
    AlertsActive int
    Health      int     // 0-3, B2 exit-code SSOT
    Peers       []PeerSnap // node_id, applied_lag, reachable, stream_actual/target
}
func Render(w io.Writer, s Snapshot)
func Bind(addr string) (net.Listener, error)  // 裸 net.Listen，无 requireLoopback
func ServeListener(ctx context.Context, ln net.Listener, snap func() Snapshot, ready func() (bool,string)) error
```
**数据源（廉价、in-process、非 StatusReport）**：is_leader/voters/applied_index/commit_index = `node.IsLeader()`(node.go:403)/`NumVoters()`(read.go:173)/`AppliedIndex()`(read.go:57)/`CommitIndex()`(read.go:99)（全非阻塞）；quorum_margin = `ProjectQuorum` fault-tol（clusterdrain.go:42）；force_single = `forceSingleActive(b.cfg.DB)`（cluster_health.go:127）；alerts_active = `cluster.ActiveAlertKeys(b.cfg.DB)`（cluster_health.go:75）；**peer_*（lag/reachable/stream actual/target）来自新 cached snapshot**：`b.observeOnce` 每 5s 已算 peer health（observability.go:111），把它最后结果存在 atomic/mutex（`b.lastObserve`），metrics snapshot 读那个——scrape **永不**调 `pollClusterHealth`/`StatusReport`。`# HELP` 注明 "peer gauges up to 5s stale"。

**wiring**：`Config.MetricsAddr string`（broker.go:89 SubHTTPAddr 后）。gated block 紧跟 subhttp block（broker.go:579，在 `cfg.DB=RODB()`:516 之后→request-time 读安全），镜像 `Bind`→`go ServeListener`→ctx-cancel-`Shutdown`。同步 bind、**失败从 Run 传播**（坏/占用 addr 让启动失败、绝不留健康 broker 带死 metrics 口）。snapshot closure **request-time/lazy**。

**HTTP**：`GET /metrics` text exposition，每 gauge 恒在（发 0 不省）、label escape（`\ " \n`）、cluster gauge（voters/quorum_margin/applied_index/commit_index/peer_*）**单模整段省略**（不假成 0）；cert_fp 不是 metric。`GET /healthz` listener 起即 200、不读集群（snapshot panic 也活、recover）。`GET /readyz` 单模 `200 single`；集群 `200` iff `health!=QUORUM_LOST && self.Phase==VOTER && !self.Inconsistent` 否则 503+body。非 GET→405、未知 path→404。
**flag**：`serve --metrics-listen`（默认 `""`），`pickFlagOrYaml`，yaml `broker.metrics.listen`。
**单模诚实退化**：`cluster_mode 0`、无 cluster/peer gauge、/readyz→200、/healthz→200。单模 `b.cl==nil` → snapshot 复刻 observability.go:191 的 nil-guard。

### §1.2 OPS#8 — 结构化日志
新建 `cmd/tether/logging.go` `newLogger(level string, jsonOut bool) (*slog.Logger, error)`：`level.UnmarshalText`（大小写不敏 debug/info/warn/error，非法→`usageErr`/exit 64、绝不静默默认）；`&slog.HandlerOptions{Level:lv}`；JSON vs Text 分支。
改 `serve.go:145` + `agent.go:213` → `newLogger(logLevel, logJSON)`。两 cmd 加 `--log-level "info"` / `--log-json false`；可选 `broker.log.{level,json}` yaml。
> **字节等价锚（golden）**：`newLogger("info",false)` ≡ `slog.NewTextHandler(os.Stderr,nil)`（slog 默认 level 即 Info）。golden 对比 pin。

**key-path attrs — 4 锚点，仅 cluster-mode child logger**（`b.clusterLog = cfg.Logger.With("node_id", selfID)` 一次；单模不动）：(1) leadership-acquired edge（observability.go:191-197）：`applied_index` + **raft.Stats() 的 raft term**（非 command-domain term）；(2) observe-loop lag/down 决策（:122-125）：Active 翻转时 node_id+applied_index+lag；(3) forward 路（clusterwrite.go proposeOrForward）：Debug reqID+verb，`ErrForwardNotLeader` Warn reqID；(4) FSM poison-skip（fsm.go:215）：broker 把 `.With("node_id",…)` logger 传入 `cluster.NewProduction` 即免费继承、零新 FSM 代码。

### §1.3 OPS#5 — `cluster status --watch`
**Step 0（共享前置，§3 唯一所有者）**：把 `cluster status` RunE 重构成 `runStatusOnce(...) (*ClusterStatusReport, error)`（无 `os.Exit`）；remote（cluster_status_nats.go:180）与 offline（cluster.go:218/237）同。
`newClusterStatusCmd`：`--watch <dur>`（默认 0=one-shot 字节等价）。拒 `0<watch<2s`（clamp ≥2s，每帧经阻塞 `StatusReport` 可达 ~2s）。`MarkFlagsMutuallyExclusive("offline","watch")`。`--watch --remote` **B5 拒**（远端每 interval 轰 member cluster-health broadcast 放大 ACL subject，价值低 → remote-watch DEFER-B6）。
**循环**：`signal.NotifyContext(SIGINT,SIGTERM)` → Ctrl-C 干净 exit 0。`for { clear; runStatusOnce→render(无 exit); select ticker/ctx }`。watch 中 transient `callAdmin` 错 → stderr + **继续循环**（监控非门）。TTY→ANSI `\033[H\033[2J`；非 TTY→`---`+时间戳分隔。`--json --watch`→JSONL（每行一对象、无 ANSI）。

### §1.4 OPS#13/AUTO#5/#6 — `--wait`/`--timeout` + `cluster wait`
共享 `waitForConverge(socketPath, node, predicate, timeout, interval)` poll `OpClusterStatus`（leader-agnostic, clusterstatus.go:355），读目标的 `Phase`/`Role`（roster 恒有、off-leader 可用）。`--interval` 默认 2s（min-clamp）；`--timeout` 默认 2m。

| verb | 收敛 | failure-terminal |
|---|---|---|
| `add` | 行在 ∧ `Phase==VOTER` ∧ Role∈{voter,leader} | `Phase==VOTER_ADD_FAILED` → 立即退出（不烧 timeout） |
| `drain`（无 --retire） | `Phase==VOTER` 又 ∧ homes 0 个 rebuild-ON expose | —（可 `--abort` 逆） |
| `drain --retire` | `Phase==RETIRING` ∧ streams `AllAtTarget` | streams 卡过 timeout。**非"行消失"**（retire≠remove） |
| `transfer-leader` | `LeaderID==<node>`；**`LeaderID==""`=继续等**（选举中非失败） | timeout |
| ~~`rotate-tunnel-cert`~~ | **DROP**（§0.4） | — |

**`cluster wait <node> --phase <PHASE>`**（新子命令）：`--phase`（clusteradmin.go:30-34 串，或伪 `GONE`=不在 roster），`--timeout`/`--interval`。命中→0；failure-terminal→立即非零；timeout→**75**（exitTransient, exitcode.go:33）；Ctrl-C/cancel→75（exitcode.go:76）。只读/幂等。跑在 NumGoroutine/fd leak gate 下。
**`add --wait` 两阶段**：`add` 首调返 nonce（cluster.go:325-342）；`--wait` 在 `resp.Nonce!=""` 时 **no-op**（只在 admit 后等）。roster 可能含 node 两次（roster 行 + orphan-voter append, clusterstatus.go:213-219）→ predicate 取第一个匹配、绝不 panic。

### §1.5 OPS#7 — cert 可见性（fp 暴露 + advisory）
`adminsock.ClusterNodeStatus` additive omitempty（host-local、schema_version 仍 1）：
```go
CertFP        string `json:"cert_fp,omitempty"`
CertFPPrev    string `json:"cert_fp_prev,omitempty"`
CertValidSecs int64  `json:"cert_valid_secs,omitempty"` // now→valid_until 派生（clock-honest，非 RFC3339）
```
**producer**：扩 `StatusReport` 既有单 roster SELECT（clusterstatus.go:96，现 `node_id,name,phase`）拉 `cert_fp,cert_fp_prev,cert_fp_valid_until`——一条 SELECT、无嵌套（守 D6 单 conn）。值是公开 fp（read.go:36-39）。
**near-expiry advisory** 于 computeHealth（:280）：`0 < CertValidSecs < certRotationWindow/8`（24h 默认 ~3h、派生）→ 追加 advisory banner（"node X tunnel-cert window closes in Nm — confirm agents repinned"）。**绝不改 health/ExitCode**（窗内 rotation 是健康；即便过 valid_until 也只是 *previous* pin 失效、cert 仍服务）→ 永不 DEGRADE。
**`agents_repinned N/M` = DEFER-B6**。

### §1.6 OPS#9 — 容量（disk + ports，self-row only）
thread `StoreDir`/`selfID`/`PortBandLow/High` 进 `ClusterAdmin`（NewClusterAdmin, clusterwrite.go:290）。`ClusterNodeStatus` additive omitempty，**仅 `node_id==a.node.SelfID()` 时填**（镜像既有 selfLag/reachOf("self") 规则, clusterstatus.go:177-178）：
```go
DiskFreePct int `json:"disk_free_pct,omitempty"`
PortsUsed   int `json:"ports_used,omitempty"`
PortsTotal  int `json:"ports_total,omitempty"`
```
disk：`diskUsage(b.cfg.StoreDir)`(disk.go:127, Statfs)→`round((1-used/total)*100)`，self/online-only、**绝不 offline 路径**，gate `StoreDir!="" && total!=0`。ports：`PortsTotal=PortBandHigh-Low+1`（默认 1000）；`PortsUsed = COUNT(*) WHERE home_broker=<selfID> AND state='ALLOCATED'`（countOwnedExposes pattern）。单 COUNT、rows closed 后（单 conn 安全）。
**health-degrade band**（computeHealth，全部"值已知才 degrade、unknown 不 degrade"fail-open，镜像 StreamTarget==0 skip :318）：`disk_free_pct < degradeFreeFrac`（从 `cfg.DiskPressureThreshold` 派生、严格紧于既有告警，如 `(1-threshold)/2`，默认 80%-used 下 ~10% free）→ **DEGRADE**；`ports_used/total ≥ 0.95` → **advisory banner，非 degrade**（满 band 停新 expose 但不威胁 quorum/data）。

> **⚠ 实现收敛（Stage-C M1 采纳 → DEFER-B6）**：上面这个 disk/ports **DEGRADE health-band 未在 B5 落地**——值在 `cluster status --json` + `/metrics` 暴露，但**不改 `computeHealth` 的 health/exit`**。理由：`disk_pressure` 已是 replicated 告警 + 进 `/metrics` 的 `alerts_active`，degrade-band 近冗余，且它是 B5 唯一会改活集群 `computeHealth` 输出的项（唯一字节等价风险）。监控**勿**把 `cluster status` exit 0 读成"磁盘 OK"。band 留 B6。详见 `b5-review.md`。
**`js_store_used_pct` = DEFER-B6**。

### §1.7 OPS#10/AUTO#10 — `takeover-natsconf --plan`
`cmd/tether/cluster_natsconf.go` 给 `newClusterTakeoverNatsconfCmd` 加 `--plan` + `--json`。走相同 identity-resolve（`AuthIdentity`，错误必须 surface——`--plan` 对真 takeover 会拒的 conf 也须 fail-closed）→ `Preflight`（include/未知 directive fail-closed）→ `BuildMergedConf`，然后 **在 `Apply` 前分支**（唯一 mutator, takeover.go:144）：
```go
if plan {
    dryRunErr := ""
    if !skipDryRun { if e := natsconf.DryRun(natsServerBin, merged); e!=nil { dryRunErr=e.Error() } }
    return renderTakeoverPlan(cmd, own, cfg, merged, dryRunErr, asJSON) // 绝不到 Apply
}
```
新 leaf `internal/natsconf/plan.go` `DiffPlan(own, cfg, merged string) PlanResult` — PURE（无 I/O）。auth-users diff 经 `auth["users"][].nkey`（AuthIdentity 已走, preflight.go:204-207）。**routes diff：从渲染好的 `merged` 文本解析**（Apply 写的真源），**不**从 `own.Parsed["cluster"]["routes"]`（嵌套数组在 Parsed 的存活未验）、**不**重构冻结的 `natscluster.Render`（over-engineering、动安全敏感渲染器）。无 `cluster{}` block 的 conf（首次 takeover）→ all-added、不 nil-map panic。
**`--json` schema**（CLI-local DTO，`{schema,schema_version}`）：`{schema:"takeover_natsconf_plan",schema_version:1,changed,server_name,client_listen,routes_added/removed,auth_users_added/removed,ownership_regenerated,preserved,js_store_dir,dry_run,restart_order_hint}`。`changed`/`schema`/`schema_version` 非 omitempty。restart-order hint 复用既有 post-Apply next-steps 文本（plan 与 apply 一致）。

---

## §2 — 对抗 TEST 矩阵

**廉价单元（`make test`）**：
1. metrics off-by-default 字节等价 — `MetricsAddr==""`→无 listener/goroutine/fd delta（NumGoroutine+fd leak gate）；单模 Run 不变。
2. 单模 `b.cl==nil` snapshot — 不 panic、诚实退化（cluster_mode 0、无 cluster/peer gauge、无假 `applied_index 0`）。
3. Render escape + 每 gauge 在场 — node_id 含 `" \ \n`/unicode escape；零值不省 series。
4. `/readyz` bands 表 — HEALTHY_HA→200；DEGRADED-but-VOTER→200；**2-voter F==0→200**（pin）；QUORUM_LOST→503；self 非 VOTER→503；single→200。
5. 非 loopback metrics bind 成功（subhttp loopback-refuse 的逆）。
6. method/path guard — POST /metrics→405；GET /nope→404；/healthz→200 即便 snapshot panic（recover）。
7. bind 失败传播 — 占用口→`Run` 错（无静默死 endpoint）。
8. logs golden 字节等价 — `newLogger("info",false)` ≡ 今；`"bogus"`/`""`→usageErr(64)；`--log-json` 解析为 JSON；debug 出 info 抑制的。
9. slog attrs — leadership 行带 node_id+applied_index+**raft term**（capture handler）；forward Warn 带 reqID。
10. `cluster wait` predicate 纯度（表）— add→VOTER、add→VOTER_ADD_FAILED(terminal)、retire→RETIRING∧AllAtTarget、transfer→LeaderID 匹配、**transfer LeaderID==""→继续等**、node-absent（GONE 收敛/VOTER 继续等）、**双 append node_id 取第一确定**、空 Nodes。
11. `add --wait` 在 nonce(phase-1) 调时**不进 wait loop、不挂**。
12. `--wait` timeout→75（fake clock）；ctx-cancel mid-wait→75 及时（leak gate）；VOTER_ADD_FAILED 短路（不烧 timeout）。
13. watch shaping — 非 TTY→JSONL 无 ANSI；TTY→clear-escape；`--watch 1s` 拒；`--watch --offline` 拒；`--watch --remote` 拒；watch 中 transient 错继续循环。
14. cert near-expiry 边界在**真 24h 窗** — `CertValid=now+(window-ε)`→advisory；`now+window+ε`(fresh)→静默；NULL→无字段；**全程 health/ExitCode 不变**。
15. disk band — free=9/10/11% 在（派生）边 → degrade/edge/healthy；`total==0`/`StoreDir==""`→absent 不 degrade；**阈值跟随 DiskPressureThreshold**（设 0.85 断 band 移）。
16. ports band — `home_broker=selfID` scoping（插另一 broker 的 ALLOCATED 行、断不计）；满 band→advisory 非 degrade；越界不 panic。
17. 容量字节等价 — `StatusReport` nil seam + 单模 `b.cl==nil` → 新字段 absent、computeHealth health/exit 同 pre-B5。**强制：单 broker(`b.cl==nil`) 触发零新 disk/port syscall**。
18. `takeover-natsconf --plan` 无 mutation（**BLOCKER**）— 快照字节+mtime+`.bak` glob；断字节相同、无 `.bak`。
19. `--plan` fail-closed — 对 include/未知 directive/JS-disable 拒；identity-resolve 错 surface；no-op `changed:false`；grow/shrink routes/auth diff 正确；nkey dedup；**无 `cluster{}` block→all-added 不 nil-map panic**；`--json` 合法、`changed` 在场即便 false；`nats-server -t` 失败→`dry_run:"<err>"` 仍 exit 0 仍无 mutation。

**Gated（`dN_integration`，真集群，出 `make test`）**：
20. scrape/watch **不**阻塞 ~2s、**不**发 `SubjClusterCursor` NATS 请求（§0.1 ship-blocker）— 断 `/metrics` 读 cached/raft-accessor 非 `StatusReport`。
21. 真 3-node `/metrics` — leader scrape `is_leader 1`/`voters 3`/`quorum_margin 1`/`peer_applied_lag{} 0`；杀 follower → 5s observe tick 后 `peer_reachable{} 0`、`/readyz`→503。
22. `add --wait` 真 2→3 grow 仅 VOTER 后返 0；`drain --retire --wait` 仅 RETIRING∧AllAtTarget；`transfer-leader --wait` 仅 LeaderID 翻转后。
23. leadership-acquired 日志每 acquire edge 恰一次（真 runObserveLoop capture）、非每 tick。

---

## §3 — 实现顺序

0. **共享 `runStatusOnce` 去 `os.Exit` 重构**（唯一所有者）— cluster.go:111/218/237 + cluster_status_nats.go:180。watch + 容量 render 的前置。
1. **OPS#8 log flags** + raft-term leadership 行 + node-scoped child logger — 最低风险、无集群、golden 字节等价测。
2. **OPS#10 `takeover-natsconf --plan`** — 纯 dry-run、helper 都在、无 live wiring。
3. **OPS#5 `--watch`**（在 §0 重构上）。
4. **OPS#13 `--wait`/`cluster wait`**（3 verb，rotate 丢）。
5. **OPS#7 cert-fp 字段 + OPS#9 disk/ports 字段 + health bands + advisory** — 一次 computeHealth pass；thread StoreDir/selfID/band 进 ClusterAdmin。
6. **OPS#1 metrics/healthz/readyz 放最后** — 新 HTTP listener + cached-`observeOnce`-snapshot 字段，在稳定基座上。

**强制序**：§0 重构在 watch + 容量 render 前。无跨 lens cert 依赖（rotate-wait 丢）。不建 `AppliedTerm()`；leadership 日志用 `raft.Stats()` term。

---

## §4 — CONFIRMATIONS

- **非集群字节等价**：每 flag off-by-default。`--metrics-listen ""`→gated 同 `SubHTTPAddr!=""`（broker.go:579），热路径无 listener/goroutine/`net/http`、无 eager DB 读（snapshot request-time、在 `cfg.DB=RODB()`:516 后）。`newLogger("info",false)` ≡ `slog.NewTextHandler(os.Stderr,nil)`（golden）。`--watch`/`--wait`/`cluster wait`/`--plan` 是新 flag/子命令；absent ⇒ 既有 RunE 不动。容量/cert 字段 additive omitempty 于 `ClusterStatusReport`/`ClusterNodeStatus`（host-local）；单 broker `b.cl==nil` ⇒ ClusterAdmin/StatusReport/disk/port 探测结构上不可达、零新 syscall（test #17/#20）。computeHealth bands unknown fail-open → 单 broker health/exit 相同。不改既有 draining-node health 输出（cordon health-tweak 砍）。单 broker `/metrics` 诚实退化、绝不假 HA。
- **安全（无泄密）**：只暴露公开值——node_id、cert *fingerprint*（`cert_fp` 今已在线 omitempty）、计数、index、lag enum。无 nkey/seed（serve.go:315）/join nonce/token/cert 私钥。Renderer **whitelist** 字段（绝不反射 struct）。metrics bind 默认空；设则裸 `net.Listen`（不 forced loopback——它不 vend 密钥）但**文档化**：`/metrics` 公布集群 *topology*（node_id、voter 数、leader、per-peer lag）——低敏侦察、operator 绑私网口。`--plan` 只读（无 Apply/.bak/rename）、经 Preflight+identity-resolve fail-closed、只印公开 directive 名 + nkey pubkey。无新 NATS subject、无 member-ACL 拓宽——全 host-local（admin socket/files）或本地 HTTP。
- **不 proto bump**：`proto.ProtoVersion` 与 `proto.ClusterHealthResp`（per-peer、versioned）冻结。全部经 additive omitempty 于 `adminsock.ClusterStatusReport`/`ClusterNodeStatus`（schema_version 仍 1）或新本地 HTTP。per-peer 容量聚合 DEFER-B6 **正是为**避免拓宽 `ClusterHealthResp`。无新直接依赖（prometheus 仅 transitive、text exposition 手写→go.mod 不变、Go 1.25 锁不破）。
- **scope 纪律**：**IN** = OPS#8 / OPS#5 / OPS#13(3 verb) / OPS#10 / OPS#1 / OPS#7(fp+advisory) / OPS#9(disk+ports)。**DEFER-B6** = OPS#6 cordon（全，需持久化 flag + 改 Eligible()；drain≠cordon）、OPS#7 `agents_repinned N/M`（per-agent telemetry）、OPS#9 `js_store_used_pct`（读路径新 js.AccountInfo 往返）、`rotate-tunnel-cert --wait`（no-op）、跨 peer 容量聚合（proto bump）、metrics auth/TLS（config 子系统）、remote `--watch`（ACL 放大）。**B5 净新 = 零 migration、零 replicated op、零 agent→broker 通道**。
