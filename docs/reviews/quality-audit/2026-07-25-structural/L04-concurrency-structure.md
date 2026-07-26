# L04 — 并发结构与生命周期编排（结构性审计）

- lane key: `concurrency-structure`
- 日期：2026-07-25
- 范围：全仓 goroutine 启动点 / ctx 传播 / shutdown 路径的**组织结构**（不是找 race —— 缺陷面见 `docs/reviews/quality-audit/01-concurrency.md`）
- 审计对象生产行数：**16,862**（下面 §范围与方法 列出具体文件）
- 只读审计：未修改任何 `.go`/`.sh`/`Makefile`/配置；`go build ./...` 绿（仅作工具链 sanity）

---

## 结论

**净判断：并发面不是屎山。** 68,328 行生产代码里只有 **76 个 `go` 语句**、**42 把 mutex**、**0 个 `context.TODO()`** —— 对一个同时负责 NAT 反向隧道、Raft HA、auth_callout、文件传输和 PTY 的系统来说，这是**偏低**的并发密度。而且项目已经为周期性收敛这一类生命周期建成了一等公民抽象 `reconcileRegistry`（零 goroutine、零 timer、假时钟可证等价、`admin runtime` 可观测），它的设计质量高于绝大多数同规模 Go 项目。

**真正的债是可点名、可局部化的，共 7 条**，核心一条是：**这个抽象只吃下了 15 个周期性任务中的 9 个**，剩下 6 个各自手写 `ticker + select + go func`，并靠 `clusterwrite.go:434` 一个**手工维护、零测试覆盖**的常量 `loopCount := 4 / 5` 来完成 join —— 这个常量写错时重新引入的，正好是 `clusterShutdownOrdered` 存在的唯一理由（drain 前 publish 丢审计记录），而那类失败该文件自己写明「the leak gate cannot catch」。

其余的债是**同一个并发协议被抄第二遍**的三处（bounded-spawn、drain barrier、ServeListener），以及 `tunnel.Server` 里三个平行 fence 维度的手写比较链。全部合计**净可删约 200–260 行**，风险从 low 到 medium，**没有一条触碰 wire 协议或 `architecture.md` 的不变量**。

**bloat 打分：3 / 10。** 理由：本 lane 的臃肿不是"写多了"，是"同一个难写对的协议写了两遍"。判据是重复的**协议密度**而非行数 —— 76 个 go 语句里，真正需要人类推理才能写对的（abandon-and-reap、drain-before-close、fence-snapshot-compare）只有 5 处，其中 3 处有第二份拷贝。这是 3 分的形状，不是 7 分的形状。7 分应该长成"goroutine 满天飞、ctx 到处丢、shutdown 靠 sleep"，本仓完全不是。

---

## 范围与方法

### 读进去的文件（生产，共 16,862 行）

`internal/broker/`：`broker.go`(1485)、`clusterwrite.go`(1010)、`transfer.go`(1210)、`transfer_audit_forward.go`、`reconcile_registry.go`(341)、`reconcile_passes.go`(225)、`observability.go`(299)、`topology_reconcile.go`(366)、`disk.go`、`audit_publisher.go`(615)、`alert_reconcile.go`(321)、`reexec.go`、`runtime_introspect.go`
`internal/agent/`：`agent.go`(1681)、`exec.go`(473)、`run.go`(550)、`roster.go`(642)、`proxy.go`、`transfer.go`、`ssproxy/server.go`(619)
其他：`internal/tunnel/tunnel.go`(1277)、`internal/adminsock/server.go`(488)、`internal/spawnsafe/spawnsafe.go`(1015)、`internal/proc/{proc,plan}.go`(580)、`internal/subhttp`、`internal/brokermetrics`、`internal/clustermanifest`、`internal/pty/pty.go`、`cmd/tether/{run,cluster_lock_keeper}.go`
测试侧：`test/concurrency/*`、`test/d4|d5|d8/setup_test.go` 的泄漏门实现

### 量化口径

- goroutine 启动点：`rg '^\s*go\s+(func|[A-Za-z_])'`，排除 `*_test.go` 与 `test/`
- 生产 76 处 / 测试 212 处
- mutex 42 把、`atomic.*` 38 处、`make(chan ` 25 处、`sync.WaitGroup` 8 处、`errgroup` **0 处**
- `context.Background()` 39 处、`context.TODO()` **0 处**

### 生产 goroutine 启动点分布（Top）

| 文件 | 数量 | 形态 |
|---|---|---|
| `internal/agent/exec.go` | 15 | 11 个 `go a.handleXForwarded`（每 verb 一个）+ streamPipe×2 + bounded-spawn×2 |
| `internal/tunnel/tunnel.go` | 12 | accept loop / ctx-watch / per-conn bridge / supervisor |
| `internal/agent/agent.go` | 7 | bootstrap / roster refresh / rehome / reconnect single-flight |
| `internal/spawnsafe/spawnsafe.go` | 5 | bounded probe / bounded resolve / abandon-and-reap |
| `internal/broker/clusterwrite.go` | 5 | 5 条 leader-gated 长循环 |
| `internal/agent/ssproxy/server.go` | 5 | accept loop / ctx-watch / per-conn relay |
| `cmd/tether/run.go` | 5 | stdin/WINCH/SIGINT pump + 出站 drain |
| 其余 18 个文件 | 22 | 各 1–3 |

**关键观察：76 个启动点分散在 21 个文件，最大簇 15 个，且这 15 个中 11 个是同一个 `switch verb` 的分支。没有 goroutine 泛滥。**

---

## Findings

### F1 — [high] 15 个周期性任务，3 套机制；为此而生的 registry 只吃下 9 个，join 靠一个无测试的手写常量

**证据**

- 抽象本体：`internal/broker/reconcile_registry.go:1-80`（写明「The registry starts NO goroutines and owns NO timers」+ one-vote-veto 不变量 + 假时钟等价证明），`reconcile_passes.go:57,82,96,116,162,177,193,201,224` = **9 个 pass**
- 落在抽象外的 **5 条集群循环**：`internal/broker/clusterwrite.go:439-447`
  ```
  loopCount := 4
  if poster != nil { loopCount = 5 }
  b.cl.loopDone = make(chan struct{}, loopCount)
  go func() { defer func() { b.cl.loopDone <- struct{}{} }(); pub.Run(loopCtx) }()
  ... ×5
  ```
  join：`clusterwrite.go:157` `for i := 0; i < cap(b.cl.loopDone); i++`
- 落在抽象外的**第 6 个**：`internal/broker/disk.go:58` 磁盘压力监视器，自带 ticker、自带 `emitted` 状态、永不 join
- 这 6 条的循环体**逐字就是 registry pass 的形状**：
  - `audit_publisher.go:119-130` = `ticker + select{ctx.Done, t.C} + p.tick(ctx)`，且 `tick` 第一行 `audit_publisher.go:135` 自己做 leader gate —— 正是 `register(name, interval, leaderOnly=true, fn)` 免费提供的
  - `alert_reconcile.go:101-114` 同形
  - `observability.go:228-236` 同形（多一个 `wasLeader` 边沿）
  - `topology_reconcile.go:61-79` 同形
- 观测面只覆盖 registry：`runtime_introspect.go:57-68` —— `tether admin runtime` 的 `Reconcilers[]{LastTick,Runs,Skips,LastErr}` **只从 `b.reconcilers.status()` 来**。那 6 条循环在 `admin runtime` 里完全不可见。
- `loopCount` **零测试覆盖**：`rg 'loopCount|loopDone' --glob '*_test.go'` = 0 命中。

**为什么它让未来改动变难/变危险**

1. 「加一个周期性任务」现在有两个答案，且两个答案的 shutdown 纪律、可测试性（假时钟 vs 真 ticker）、可观测性（`admin runtime` 有/无）**互不相同**。新人（或半年后的自己）选错就静默失去后三样。
2. `loopCount` 是**手工维护的、必须与其下方 `go` 语句数一致的常量**。写大了：shutdown 每个幽灵槽阻塞 10s（`clusterwrite.go:161` 的 `time.After(10*time.Second)`），吵但可见。写小了：**静默** —— `clusterShutdownOrdered` 第 3 步（unsubscribe responders）在一条循环仍在跑 JS/raft I/O 时就推进，紧接着 `nc.Drain` 这个 defer 执行。而 `clusterwrite.go:132-137` 自己写明了这个顺序为什么 load-bearing：「a publish-after-Drain would SILENTLY DROP an audit record — **a loss the leak gate cannot catch**」。也就是说，写小 `loopCount` 精确地重新引入了这段有序 shutdown 唯一存在的理由，且是本仓泄漏门明确无法检测的那一类失败。
3. `topology_reconcile.go:81` 的签名 `reconcileTopologyOnce(ctx, lastApplied, lastObserved, lastReloadMtime, lastRestartMtime, lastEventKey) (uint64, uint64, int64, int64, string)` —— 5 进 5 出地穿线循环局部状态，纯粹因为「手写循环没地方放 per-pass 状态」。registry 的 pass 是闭包，这 10 个参数/返回值会直接消失。

**建议（分两步，不要一步到位）**

- **不要**把 5 条集群循环整体并进 registry 的单 goroutine —— `pub.tick` 做阻塞 JS 发布，串到 node-states 活性 pass 后面是真实的回归风险。
- 第一步（低风险，先做）：把 **`disk` 与 `topology`** 两条注册进 registry。二者都是 non-leader-gated、level-triggered、幂等，完全符合 one-vote-veto；`reconcileTopologyOnce` 的 5 进 5 出签名坍缩成 `func(ctx, now) error`，`emitted`/`lastEventKey` 等状态进闭包。**净减 2 个 goroutine、约 30 行**，并且这两条立刻进入 `admin runtime`。
- 第二步：为剩下 3 条（pub/rec/observe，都需要独立 goroutine 以免阻塞）抽一个 ~45 行的 `loopSet{Go(name, fn); Join(timeout)}`，**自计数**（`loopCount` 消失），并把它的 last-tick 并进 `reconcilers.status()` 的输出。净行数约持平，但手工常量的隐患和观测盲区一起消失。

**量化**：净减约 30 行（第一步）；`admin runtime` 覆盖的周期任务从 9/15 升到 15/15；`loopCount` 这个静默失败点归零。
**风险**：medium（改动 shutdown join 语义；需保持 pass 返回 nil 以免 registry 的指数退避改变 cadence）。不触 wire、不触 `architecture.md` 不变量。

---

### F2 — [high] `tunnel.Server` 的 fence 状态：3 个平行维度 × 3 组 map 压在一把锁下，比较链手写 3 遍

**证据**

- `internal/tunnel/tunnel.go:130-160`：`s.mu` 保护 **8 个 map + closed + ln**
  ```
  sessions, killGen, killGenAllocation, killGenSession,
  inflightBySID, forgotten, inflightByAllocation, closedAllocation
  ```
- 同一个三元比较**写了 3 遍**：
  - 快照 `tunnel.go:356-358`：`gen := s.killGen[port]; sessGen := s.killGenSession[sid]; allocGen := s.killGenAllocation[fenceKey]`
  - pre-fence `tunnel.go:405`：`s.closed || s.killGen[port] != gen || s.killGenSession[sid] != sessGen || s.killGenAllocation[fenceKey] != allocGen`
  - install-fence `tunnel.go:441`：同上，逐字重复
  - 加上 in-flight 计数的 defer `tunnel.go:362-372`（两个 map 各 ++/--，两个 prune helper）
- 三个平行 prune helper：`tunnel.go:640 maybePruneSessionLocked` / `:648 ensureAllocationFenceMapsLocked` / `:660 maybePruneAllocationLocked`
- 这些维度的**来源清清楚楚写在注释里**，每一轮外审加一整个新维度：`round-2 F1`（port 级）→ `round-5 F1`（session 级）→ `round-6 F4`（in-flight/forgotten）→ `CloseProxyIf`（allocation 级）。

**为什么它让未来改动变难/变危险**

加第 4 个 fence 维度（例如按 nid fence，在多 home 场景下完全可能）需要改 **4 处**：快照、pre-fence 链、install-fence 链、prune defer。其中**两处 `||` 链漏写一项编译器不会报错**，而后果不是崩溃 —— 是一个已被 kill 的公网 exit 在 REGISTER 竞态里复活并重新 bind 公网端口。这是数据面安全洞，且正是过去三轮外审反复踩到的同一个坑。换句话说：**这个结构保证了同一类 bug 会以固定节奏再来一次**。

**建议**

```go
type fenceSnap struct{ port, sess, alloc int64 }
func (s *Server) fenceSnapLocked(port int, sid string, k sessionFenceKey) fenceSnap
func (s *Server) fenceChangedLocked(port int, sid string, k sessionFenceKey, snap fenceSnap) bool
```
三条手写链坍缩成 `if s.closed || s.fenceChangedLocked(port, sid, fenceKey, snap)`。加维度的改动面从 4 处（2 处静默失败）降到 **1 处**。

**量化**：净减约 15 行；新增 fence 维度的编辑点 4 → 1。
**风险**：low —— 纯重构，round-2/5/6 的 F1/F4 回归测试直接钉住行为，无 wire 变更。

---

### F3 — [medium] `Agent.startBounded` 是 `spawnsafe.RunStartWithCleanup` 的逐行重写，而同包的兄弟调用点已经在用库版本

**证据**

- `internal/agent/exec.go:403-422`（20 行）与 `internal/spawnsafe/spawnsafe.go:917-947` 结构完全一致：同样的 `TryAcquireSpawnSlot` → `done chan` → `select{done, time.After}` → abandon 分支起第二个 goroutine 等 `<-done` 再 `ReleaseSpawnSlot`。差异只有两点：`startBounded` 不做回调 nil 检查、timeout 用 `a.spawnTimeout()` 而非入参。
- **同一个 package 的兄弟调用点已经用库版本**：`internal/agent/run.go:188-199` = `a.spawnPolicy.RunStartWithCleanup(startFn, a.spawnTimeout(), onClose, onReap)`。
- 唯一调用点 `internal/agent/exec.go:174-182` 两个回调都非 nil，因此 `startBounded` 与 `RunStartWithCleanup(cmd.Start, a.spawnTimeout(), onAbandon, reapOnReturn)` **行为等价**。
- 库侧的注释仍在为这份拷贝辩护，且理由已过期 —— `spawnsafe.go:952-955`：「Callers that must clean up resources on abandon ... manage their own goroutine with these primitives instead of RunStart (review M4: RunStart cannot close a caller's StdoutPipe/StderrPipe)」。可 `RunStartWithCleanup` 收的就是 `onAbandon`/`reapOnReturn`，M4 早已修掉这个理由。这是典型的「外审加了库能力 → 局部修好 → 没有回扫既有拷贝」。

**为什么它是债**

wedge slot 的配对（`TryAcquire` 与跨越"被放弃的 goroutine"的 `Release`）是这个仓里最难写对的会计之一 —— `wedgeCeiling` 的语义、以及"放弃后 slot 要一直占着直到 execve 恢复"这条规则，本来由 `internal/spawnsafe` **独占拥有**。包外存在第二份实现直接废掉这个所有权：给 spawnsafe 加限流指标、给被放弃的 reaper 加上限、或者改 slot 释放时机时，`exec.go` 的这份会被静默漏掉，而症状是"agent 在 NFS 挂死时 fd/线程无上限增长"这种只在现网出现的形态。

**建议**：删掉 `startBounded`，`exec.go:174` 直接调 `a.spawnPolicy.RunStartWithCleanup(cmd.Start, a.spawnTimeout(), onAbandon, reapOnReturn)`；`internal/agent/remotefs_test.go:276` 的断言搬到 `spawnsafe_test.go`（那里已有 `TestRunStartWithCleanup_reapsSuccessfulLateStart`）；顺手订正 `spawnsafe.go:952-955` 那段过期注释。

**量化**：净减 **32 行**（20 行实现 + 12 行文档注释）。
**风险**：low。

---

### F4 — [medium] 「drain barrier」协议独立实现两遍，且 draining 分支语义相反、无处成文

**证据**

- 实现 A（broker，transfer audit）：状态 `broker.go:474-490`（`transferAuditWG` / `transferAuditDraining` / `transferAuditMu`）；进入 `transfer_audit_forward.go:84-98`；封口 `clusterwrite.go:143-152`
- 实现 B（agent，proxy-keys）：状态 `agent.go:315-323`（`proxyHandlerWG` / `proxyDraining` / `proxyDrainMu`）；进入 `exec.go:71-85`；封口 `agent.go:683-690`
- 两处**各自写了一整段注释重新推导同一条正确性论证**（「Add 不能落在 Wait 看到 0 之后」）：`transfer_audit_forward.go:85-91` vs `exec.go:69-71`。两处都是靠一轮外审才写对的（`audit M7 / xx-concurrency F1` 与 `round-2 F4`）。
- **draining 分支语义相反且无处成文**：broker 在 draining 时**同步就地执行**（`transfer_audit_forward.go:90-93`，因为审计记录不能丢）；agent 在 draining 时**直接丢弃**（`exec.go:73-75`，因为迟到的 proxy-keys 推送可以丢）。两者各自都对，但"什么时候该 inline、什么时候该 drop"没有任何地方写下来。
- 附带的注释漂移证据：`transfer_audit_forward.go:104-106` 的 `WaitTransferAudit` 文档写着「**TEST-ONLY**: the d8 harness calls it before its ... leak assertion」，而 `clusterwrite.go:152` 就在生产 shutdown 路径上调它。恰恰是顺序 load-bearing 的那个函数，文档说反了。

**为什么它是债**

第三个需要异步 handler + 有序 drain 的子系统（很可能出现 —— 例如未来的 home-directive 主动推送、或 proxy 订阅事件）会**第三次重新推导**这段论证。以本仓的历史命中率看，第三次同样需要一轮外审才写对。而 inline-vs-drop 的选择没有 checklist，第三个实现只能靠抄其中一份 —— 抄错那份就是"关机窗口静默丢审计"。

**建议**：抽一个 ~35 行的 `internal/drain.Barrier`：
```go
func (b *Barrier) TryEnter() bool   // 原子的 {检查 sealed + WaitGroup.Add(1)}
func (b *Barrier) Done()
func (b *Barrier) Seal()            // 原子的 {置 sealed}；随后 Wait 是可靠屏障
func (b *Barrier) Sealed() bool
```
两处调用点各降到 5 行；inline-vs-drop 变成调用点上一个具名的显式分支而非埋在注释里的默契。顺手订正 `WaitTransferAudit` 的 TEST-ONLY 文档。

**量化**：净减约 40 行（含两份重复注释）；协议实现从 2 份降到 1 份。
**风险**：low-medium —— 触碰审计路径的 shutdown 顺序，必须保留 broker 侧的同步 fallback；需过 `-race` + 泄漏门。

---

### F5 — [medium] `Bind` + `ServeListener` 在三个包各抄一份，shutdown 语义已经漂移；`Broker.Run` 里另有三块几乎逐字相同的 12 行接线

**证据**

- `internal/subhttp/subhttp.go:276-302` 与 `internal/brokermetrics/metrics.go:114-138`：**函数体逐字相同**，只差错误前缀与 handler ——
  `context.AfterFunc(ctx, func(){ shCtx,_ := context.WithTimeout(context.Background(), 3*time.Second); srv.Shutdown(shCtx) })` = **优雅 shutdown，排空在途请求**
- `internal/clustermanifest/manifest.go:46-56`：**三处漂移**
  - `go func(){ <-ctx.Done(); srv.Close() }()` = **硬关，掐断在途请求**（与上面两个不一致）
  - `err != http.ErrServerClosed` 用 `!=` 而非 `errors.Is`
  - 错误不 wrap（另两个都 `fmt.Errorf("%s: serve: %w")`）
  - 这个 watcher goroutine 在 `Serve` 因监听器被外部关闭而返回时，会**活过 `ServeListener` 的返回**直到 ctx 取消（不是真泄漏，但是三份实现里唯一有这个形状的）
- `internal/broker/broker.go:882-902`（subhttp）、`:909-920`（metrics）、`:926-937`（manifest）：三块 `Bind → 失败即 return → Info 日志 → go Serve → Error 日志` 的 12 行接线，逐字同构。

**为什么它是债**

一个在途的 `/sub` 请求被优雅排空、一个在途的 manifest GET 被掐断 —— **没人决定过这件事，这是漂移**。加第 4 个 loopback HTTP 面（很可能：pprof、debug 端点）就是第 4 次复制 + 第 4 次重新决定语义；而把 3s 宽限期改掉需要同时改 3 个地方。这个失败模式本仓自己在 `broker.go:742` 就点名过：「Those were three hand-copied copies, which is how the late one gets fixed and the early one rots.」—— 同一个诊断，换个地方原样复发。

**建议**：抽 `internal/httplisten`，`Bind(addr string, requireLoopback bool) (net.Listener, error)` + `Serve(ctx context.Context, ln net.Listener, h http.Handler, name string) error`（单一优雅 shutdown 语义）。三个包各留自己的 `Handler()`（本来就已经拆好、httptest 可测）。`Broker.Run` 的三块坍缩成一张 3 行表 + 一个循环。

**量化**：净减约 **45 行**（三份 ServeListener 归一 ~20 行 + Run 里三块归一 ~25 行）；三种 shutdown 语义收敛为一种。
**风险**：low（`clustermanifest` 从硬关改优雅关是行为改进，且该端点只 vend 已签名的公开发现信息）。

---

### F6 — [medium] 「teardown 用哪个 ctx」没有成文规则：`deleteXferObject` 8 个调用点里 4 个用 `context.Background()`，其中一个就在手里已有 ctx 的 goroutine 内

**证据**

- 同一函数 `internal/broker/transfer.go:379 deleteXferObject(ctx, sid, transferID)` 的 8 个生产调用点分成两派：
  - 传真 ctx：`xfer_inflight.go:416`、`:432`、`:516`
  - 传 `context.Background()`：`transfer.go:438`、`:929`、`:956`、`:1196` —— **四处都没有一行注释解释为什么**
- 最刺眼的一处：`transfer.go:402-403` `startTransferWatchdog(parent, e)` 内部 `ctx, cancel := context.WithCancel(parent)`（parent 来自 `transfer.go:654` 的 `b.runCtx`），goroutine 里 `transfer.go:412` 正在 `select { case <-ctx.Done(): ... }` —— **手里明明有 ctx**，到了 `transfer.go:438` 却传 `context.Background()`。
- 而同一子系统的兄弟函数带着一段 6 行的反向强制说明：`xfer_provision.go:154-156`「parent **MUST be b.runCtx, not context.Background()**: with Background() the shutdown branch is dead code AND ordinary budget exhaustion would be reported as "the broker is shutting down" — a brand new false statement of exactly the species #67 exists to remove.」
- 更上一层：**「在 NATS 回调里取到环境 ctx」有三套并存机制**
  1. `b.runCtx` —— 裸结构体字段存 context（`broker.go:371` 声明，`:763` 赋值）
  2. `a.runCtx` + `setRunCtx/loadRunCtx` —— mutex 装箱（`roster.go:617-630`），因为 C1 的 session 重建循环会跨 session 改写它
  3. 裸 `context.Background()` —— 上述 4 处 + `agent/proxy.go:449,484` 的 nil fallback + `cluster_grow_cutover.go:344`（这处**有**注释说明：「No ctx flows through the async NATS callback chain」）

**为什么它是债**

根因是结构性的、且不是本仓造成的：`nats.MsgHandler` 签名里没有 ctx，所以**每个 handler 侧的生命周期决定都只能就地拍**。目前的后果是传输路径上关机行为不一致；未来的后果更硬 —— 当有人缩短 shutdown 宽限（或加一个「关机时必须完成 X」的要求）时，一半清理路径会观察到关机、一半不会，**而 code review 时没有任何规则可以指着说"这条违反了策略"**。

**建议**（这条不减行，减的是决策熵）

在 `architecture.md` 写一条规则并在每个 `context.Background()` 站点加一行注释引用它。建议规则：**获取/推进类工作（provision、forward、apply）derive 自 runCtx；释放/清理类工作（delete、unsubscribe、reap）用新的 `context.WithTimeout(context.Background(), N)`，以便在关机中仍能完成。** 这条规则恰好能同时解释 `xfer_provision.go` 的强制和 `deleteXferObject` 的 Background —— 即现状**大概率本来就是对的，只是没人写下来**，所以第 9 个调用点会是掷硬币。

**量化**：0 行净减；新增 1 条规则 + 8 行注释。价值在于把"8 次独立猜测"变成"1 条可引用的策略"。
**风险**：low（纯文档 + 注释；若顺带统一某处实际 ctx，需单独评估）。

---

### F7 — [low] 自建泄漏门：机制选型是对的，但复制了 4 份，且 ±2 容差被用在只运行 1 次的测试上

**这是任务点 6 的正面回答。**

**证据**

- 实现本体 `test/concurrency/helpers_test.go:136-153` —— **18 行**：`for i<50 { Gosched; last=NumGoroutine(); if last-before<=2 {return}; sleep 20ms }`，失败时 `runtime.Stack(buf, true)` 全量 dump
- **同一份被复制 4 遍**：`test/concurrency/helpers_test.go:136`、`test/d4/setup_test.go:423`、`test/d5/setup_test.go:430`、`test/d8/setup_test.go:313`。已经开始漂移（变量名 `n` vs `nb`；d5/d8 把说明注释删了）。`fdCount` 复制 3 遍。
- fd 基线：`test/concurrency/fd_leak_test.go:114,154,199,228` 四处 + 各 `setup_test.go`
- **容差 × 运行次数的盲区**：
  - `goroutine_leak_test.go:168-198 TestTunnelServerCloseWithActiveSessionNoLeak` —— 只开 **1** 个 tunnel session 就断言 `delta <= 2`。**每 session 泄漏 1–2 个 goroutine 在结构上不可检测。**（对照：`tunnel.go:470` 的 yamux CloseChan watcher、`:502` 的 per-conn bridge 正好是 per-session ±1 的形状）
  - `goroutine_leak_test.go:242-254 TestBrokerRepeatedRunNoGoroutineLeak` —— 跑 **5** 轮，per-run 泄漏 1 个就是 delta 5 > 2，**这个形状是对的**。同一个文件里两种做法并存。
- 依赖论据的现状核实：`golang.org/x/sync v0.20.0` 已在 `go.mod`（indirect），所以"引入新依赖"这条对 errgroup 类工具已不成立；但 `go.uber.org/goleak` 确实是独立 module，那条论据对 goleak 本身仍然成立。

**评估：自建是明智的自主决策，不是重复造轮子。**

- **维护成本**：18 行，写完至今零维护。这不是"造轮子"该有的成本曲线。
- **goleak 相对它的真实增量**只有两项：按栈**归因**（哪条 goroutine 泄漏）与按函数名**过滤**已知良性 goroutine。前者本仓用"失败时全量 dump 64KiB 栈"替代了 —— 更吵，但信息不缺；后者用"相对 delta + 容差"替代 —— 更粗，但对本仓的形状够用。
- **真正的弱点不在选型，在容差与运行次数的交互**：±2 是绝对值，所以泄漏信号必须被放大到 >2 才可见，而这需要**测试把被测对象跑 N≥3 次**。仓库里已经有做对的样板（repeated-run），也有做错的（single-session）。换成 goleak **不会**修好这一点 —— goleak 同样会把 1 个 per-session 泄漏归到"测试结束时还活着的 goroutine"里，只是它能报出名字；真正的修法是把练习次数抬上去，跟库无关。
- 覆盖面另有缺口：`CLAUDE.md §5` 要求"隧道/PTY/reconcile/传输/Raft 并发面必须带 leak 门"，而实际断言点只有 14 处，覆盖 broker run/agent run/tunnel close/adminsock close/spawnsafe/d4-forward/d5-publisher。**PTY（`internal/pty`）、transfer watchdog、5 条集群循环、3 个 HTTP 监听器都没有 leak 断言。** 政策比门宽。

**建议**

1. 把 `assertNoGoroutineLeak` + `fdCount` 搬进 `internal/testharness`（非 test 包、已存在 189 行、每个 `test/dN` 都能 import）。**净减约 45 行**，4 份变 1 份。
2. 给每个泄漏断言配一个"练习 N≥5 次"的循环 —— 特别是 `TestTunnelServerCloseWithActiveSessionNoLeak`，把 1 个 session 改成 5 个，±2 容差立刻低于信号。
3. 不要换 goleak。

**量化**：净减 45 行；泄漏门的最小可检测信号从"每对象 ≥3 个 goroutine"降到"每对象 ≥1 个"。
**风险**：low（纯测试侧）。

---

## 反证：做得好的地方

1. **`internal/broker/reconcile_registry.go` 是本仓最好的代码之一。** 341 行里有 ~70 行是**为什么这样设计**的论证（`reconcile_registry.go:11-72`），包括：为什么 tuple 必须是 `(name, interval, leaderOnly, lastTick, fn)`（"retrofitting an observability field into a scheduler after four batches have registered passes against it is how interface freezes get broken"）、one-vote-veto 不变量（"a one-shot action that is wrong destroys state once; the same action on a 30s cadence destroys state 2880 times a day"）、以及为什么锚定 deadline 而不重采样墙钟（这是前后等价性证明的基础）。**它启动 0 个 goroutine、拥有 0 个 timer**，因此完全不干扰泄漏门。9 个周期任务、1 个驱动 ticker。这是教科书级的抽象。

2. **`clusterShutdownOrdered`（`clusterwrite.go:132-165`）是显式编号的有序关闭，不是 defer-LIFO 猜测。** 三步各带"为什么这一步必须在这个位置"的理由，且是从事故里学到的（drain 前 publish 会静默丢审计）。绝大多数 Go 服务的关机就是一串 defer + 祈祷。

3. **并发密度低得意外。** 68,328 行生产代码 / **76 个 `go` 语句**。而且分布健康：最大的簇（`agent/exec.go` 15 个）里 11 个是同一个 `switch verb` 的分支，属于"每 verb 一个 handler goroutine"这一条被明确论证过的设计（`exec.go:51-56`：不这么做则 `tether run sleep 60` 期间的 kill.req 会被 head-of-line 阻塞）。

4. **`context.TODO()` 零处。** 39 个 `context.Background()` 里，多数是正当的**无环境 ctx**场景：CLI 入口（`cmd/tether/main.go:68` 且是 `signal.NotifyContext`）、离线恢复工具（`clusteroffline/*` 根本没有 daemon ctx）、HTTP 优雅关闭的独立超时。真正可疑的只有 F6 点出的那一簇。

5. **`internal/proc` 与 `internal/spawnsafe` 没有任何重叠**（任务点 5 的直接回答）。`proc` = `processes` 表的 SQLite CRUD + raft 命令 planner（`proc.go:90 Insert` / `:145 MarkExited` / `plan.go:36 PlanInsert` / `:163 PlanReconcileBatch`）；`spawnsafe` = mountinfo 分类 + 死挂载探测 + 有界 spawn 窗口（`spawnsafe.go:431 parseMountinfo` / `:532 mountHealthy` / `:686 Prepare`）。二者**没有一处调用关系、没有一个共享类型**。名字相邻造成的误会而已 —— **不该合并，也不该拆分**。

6. **锁的粒度是细的，不是粗的**（任务点 4 的直接回答）。42 把 mutex / 68k 行。两个最大的类型都是**细分锁域**而非一把大锁：`Agent`（`agent.go:220-438`，55 个字段）有 13 把各自命名、各自注释了保护范围的锁（`procsMu` / `execChildrenMu` / `pushCacheMu` / `rehomeMu` / `rosterMu` / `redialMu` / `flcMu` …）；`Broker`（`broker.go:336-566`，45 个字段）有 8 把。「一把大锁保护半个结构体」**在 broker/agent 里不存在** —— 唯一符合这个描述的是 `tunnel.Server.mu`，已作为 F2 单列。

7. **atomic 用在了该用的地方，且每处都写了为什么锁是错的。** `b.nc atomic.Pointer[nats.Conn]`（`broker.go:339-344`：drain-before-clear 的顺序化，锁做不到）、`b.tunnelSrv`、`a.ncBox`（`agent.go:330-335`：tunnel hook 在 supervisor goroutine 上跑，取 `p.mu` 会死锁，锁序是 `p.mu → c.mu`）、`cl.topoSelf`、`afterRehomeWantSettledHook atomic.Pointer[func]`（`agent.go:460-463`：连测试 seam 都不用裸 var，因为 `-race` 会报）。这不是 cargo-cult 的 atomic 滥用。

8. **`sync.Map` 只用在真正的 leader-local 缓存上**（`proxyDwell` / `rehomeEvt` / `proxyEvtCounts`，`broker.go:435-448`），且每处都写明了 leadership 翻转时重置的语义是**期望行为**而非漏洞。

9. **agent 的 session 关闭虽是 defer-LIFO，但每一条顺序依赖都有就地注释指名它必须先于谁**（`agent.go:674-690`：「Set proxyDraining FIRST (under the lock) so dispatch cannot Add a new handler after this point, THEN Wait — a sound barrier... Runs just before nc.Drain (registered right after it)」）。

---

## 本质 vs 偶然复杂度拆解

本 lane 范围 **16,862 行**生产代码。估算：**本质 ≈ 80%，偶然 ≈ 20%（约 3,300 行），其中真正可净删的约 200–260 行。**

### 本质的（问题域强加，删不掉）

| 复杂度来源 | 为什么是本质 | 大致体量 |
|---|---|---|
| 反向隧道的并发关闭 fence | `expose` 的关闭真有 **3 种粒度**：按 port（`CloseProxy`）、按 allocation 身份（`CloseProxyIf`，token_hash 定身份以免端口复用误杀）、按 session（`CloseSession`/`ForgetSession`）。而 REGISTER 授权与 listener 安装之间必然存在窗口。**维度是本质的，只有它们的编码方式是偶然的（F2）** | ~450 |
| yamux 重拨 + generation fencing | 每个 expose 一条独立 supervisor，重拨时必须防止陈旧 supervisor 覆盖新 session。gen 戳 + 值参快照（`tunnel.go:915-918`）是这个问题的最小解 | ~350 |
| D-state execve 放弃与回收 | **内核层面无法 kill 一个 D-state 进程**。因此"超时后放弃 goroutine、等它自然返回再释放槽位、期间限制并发放弃数"不是设计选择，是唯一可行解 | ~200 |
| 有序关闭（drain-before-close） | 审计是 at-least-once 语义，`nc.Drain` 之后 publish 会静默丢。跨 5 条 leader-gated 循环 + 异步 forward + responder 订阅的顺序是真依赖 | ~150 |
| Raft leader 门控循环 | leadership 会翻转，且翻转必须不改变 goroutine 数（否则泄漏门噪声），所以是"常驻循环内轮询 IsLeader"而非"leader 时起 goroutine" | ~400 |
| NATS 回调无 ctx | 上游 API 约束。所有 handler 侧的生命周期决定都必须自己接线（F6 的根因） | 弥散 |
| PTY / 双向 copy / stdin pump | 每条数据流一个 goroutine，io.Copy 语义决定的下限 | ~250 |

### 偶然的（实现方式造成，可消除）

| 项 | 可净减 | Finding |
|---|---|---|
| 第 6 套周期任务机制 + 手写 `loopCount` + 5 进 5 出状态穿线 | ~30 | F1 |
| 三条手写 fence 比较链 + 三个平行 prune helper | ~15 | F2 |
| `startBounded` 整份重复（实现 + 文档） | ~32 | F3 |
| drain barrier 第二份实现 + 第二份论证注释 | ~40 | F4 |
| ServeListener ×3 + Run 里的接线 ×3 | ~45 | F5 |
| 泄漏门 ×4 + fdCount ×3 | ~45 | F7 |
| **合计** | **~207** | |

另有约 3,000 行属于"偶然但不该动"的类别：大量**推导过程注释**（`broker.go:474-490` 的 17 行注释支撑 3 个字段；`reconcile_registry.go` 70 行论证；`clusterwrite.go:132-137` 的顺序理由）。按行数算它们是"冗余"，但对一个**外审驱动、每条不变量都是从事故里学来的**生产工具，这些注释是防止同一个 bug 复发的主要机制 —— 本审计明确**不建议删**。它们是本项目为"没有第二个维护者"付的合理保费。

### 一句话

本 lane 的 16,862 行里，**约 207 行是可以直接删掉且删了更安全的**（1.2%）。剩下的要么是问题域本身要求的，要么是把问题域的教训写下来的成本。**「过于臃肿 / 屎山」这个假设在并发结构这一面不成立。**
