# Pass — simcluster-speed 外部复审（round 2）

> 2026-09-20。角色：独立复审（CLAUDE.md §4.1）。基线：HEAD `a3431a1`；index = 首轮外审结束时的受审树（107 文件）；
> 复审范围 = index 之外的全部工作树修改（22 个已跟踪文件）与 2 个未跟踪测试文件。tasklist：
> `simcluster-speed-external-rereview-tasklist.md`。复审期间未修改实现；下面每条结论来自实现、真实调用点、独立反例、
> 复审自造变异与重跑收据，不采信主进程回复里的"已修 / 全绿"。

## 最终结论

**Pass，可以放行当前工作树。** 首轮 F1–F5 五条 MAJOR 全部**独立核验关闭**：首轮两个 overlay probe 原样转绿、首轮 F3/F4/F5
的形状转绿，主进程报告的每条变异我都换了注入点再做一遍（R-M1…R-M7）仍全部红。复审新发现 **2 条 Minor（R1 测试时序裕量、
R2 一个理论上可达的非确定性）+ 1 条既有面的 Note（R3）**，无 Major，无阻断；R1/R2 建议在 commit 前顺手收掉（修法都在
十行以内，不改行为），已在文末"主进程回复"登记处置。

## 首轮 finding 逐条关闭

### F1 — Closed · expose 重试预算贯穿整条梯子

- **边界（A1）**：`ExposeAdapter.AddProxy(ctx, p)` 的 13 个实现（1 生产 + 12 fake）全部改签；四个生产调用点的 ctx 与回复一致：
  `handleExposeForwarded` → `exposeOpenContext(a.loadRunCtx())` = run ctx 的 3 s 子 ctx；`replayPortsFromState(ctx)` /
  `openHomeFromState(ctx, d)` = run ctx（`session()` 的 `runCtx` / `applyOneHome` 的 ctx）；`proxyStartLocked` =
  `context.Background()`（行内 `ctx-none:`，函数按 dataplane_lifetime 门不收 ctx，行为与改前一致）。
- **迟到安装（A2）**：`OpenHome` 先 `ctx.Err()` 短路（不 dial）、握手 ctx = `c.ctx` 子 ctx + `context.AfterFunc(ctx, cancelHS)`、
  `c.mu` 下安装前再查 `ctx.Err()`。fence 之后、返回之前 ctx 到期的窗口：session 已装、handler 答 OK——此时距 handler 开始 ≤ 3 s
  + state.json 写，broker 的 5 s 窗还有 ≥ 2 s 裕量，两端一致；不是 F1 的形状。
- **生命周期（A3）**：`sessCtx` 仍派生自 `c.ctx`；信号量三条路径 acquire/release 配对正确，`acquireOp` 超时不 release，
  `defer` 覆盖 panic 路径；ctx 已 done 且锁空闲时 select 随机命中锁的分支也无害（`OpenHome` 第一行即返回，defer 释放）。
- **独立反例**：首轮 probe `TestExposeRetriesFinishBeforeForwardExpires`（fake 按新接口遵守 ctx）`elapsed=3.05s calls=2
  persisted_ports=0` PASS。复审变异 **R-M1**（切断后不再返回先例 transient deny）→ 梯子表行 + handler 测试 `got "frpc_failed"`
  双红；**R-M2**（caller ctx 不 relay 进握手）→ `OpenHome took 612ms` 红。
- **900 次顺序 open 的漂移探测**（见 R2）：0 次 spurious down-edge、REGISTER 数 == open 数。

### F2 — Closed · tier-A push 的终结权

- `claimAbandonedPush` 在 `tracker.mu` 下读 **tracked** `e.tier`；push.req 在创建 entry 之前已把 tier 限定为 {a, b}
  （`tier_invalid`），所以 `!= "b"` 恰等于 tier-A。handler 对 `abandonRefusal` 穷举无 `default:`；audit 的 tier 覆盖只对 pull 生效
  （`entry.verb == "pull" && …`）。`abandonNoEntry` / `abandonFinalized` → 幂等 OK，与修前 "!claimed → OK" 语义一致。
- 枚举登记是覆盖扩大：`enumFamilies` +1、`familiesWithSwitches` +1；全仓以 `abandon` 开头的常量只有这 6 个（`abandoned` 是
  变量，不进 case 表达式）。
- **独立反例**：首轮 probe（未改一字）`{OK:false Code:verb_mismatch …} tracker_present=true` PASS。复审变异 **R-M4**（去掉 tracked
  tier 检查、改在 handler 里信 `fin.Tier=="a"`）→ 表测 `push-tier-a = 0, want 5` + Arm 4 `body Tier="b" … got ok=true` 双红——
  正是首轮"不能信任请求体 Tier"的那一刀。

### F3 — Closed · replay 的 legacy 资格按 drill

- 扫描面是 `manifest_arms` 的全部 arm，与选中的 unit 无关。复审自造三种形状：post-split **完整**归档 + 残留旧父日志 → 三个臂行、
  父日志被忽略、无 LEGACY；同一归档显式点 `f-split.D` → 只有 D 行；纯旧归档 → LEGACY-UNIT 一行、按父行 MATCH（UNIT_* 表填充正确）。
- 复审变异 **R-M5**（只扫选中臂）→ H-8b（混合归档显式点缺失臂）红——这正是"按选中臂判"会漏的那种形状。

### F4 — Closed · grow fixture 在 post-check 后一次写 lane 标记

- `SIM_GROW_DONE_FILE= "$SIM" grow brkN` 是 POSIX 的单命令 env 前缀，dash 下有效；96 的调用形状（`assert_setup` →
  `_as_capture` → `$("$@" 2>&1)` 命令替换）下复审自跑：cmd_grow 协议的 stub **零** PERGROW 行、fixture 两行落盘、
  `GROW-ATTEMPTS` 进 `_AS_OUT`（与 74 把 grow_to_3 放在 assert_ok 外的既有原因一致）。`grow_to_2` 的写点在两道 guard 之后；
  `setup_forcesingle_n2` 未改（单次 grow、无重试，每-grow 行语义仍正确）。
- 复审变异 **R-M6**（grows 之后、post-check 之前就写）→ E-3 `sidecar holds 1 line(s) mid-retry` 红。
- 真栈：核对了主进程 74.SRAB（镜像 #15）的原始文件——`.grow-done` 两行同一 epoch 秒、`GROW-ATTEMPTS: 1`、`regime.tsv`
  `REGIME default 5 0`、rollup MATCH。没有再跑 96：A8 的命令替换探测已覆盖它与 74 的唯一差别（assert_setup 包裹），再跑一个
  333 s 的单元不会增加信息。

### F5 — Closed · REGIME 是归档的事实

- `regime.tsv` 在 live 清理之后、任何 launch 之前写；replay 的 `rm` 列表只有 rollup.*；REGIME 行只从文件读；缺失 / 形状坏 →
  `unknown - -` 并在 summary 明说；三个 regime flag 与 `--replay` 互斥（rc=2）。同一 live run 的 attribution pass 不经过写点。
- 复审变异 **R-M7**（replay 连 regime.tsv 一起删）→ H-9 `REGIME after replay: unknown - -` 红。

## 复审 Findings

### R1 — Minor · 新增 F1 测试的时序裕量偏紧，-race 并行矩阵下有假红风险

- `internal/agent/expose_test.go` 梯子表 "F1: a slow retry is cut at the deadline…"：60 ms 预算、20 ms 步、每次调用 25 ms，
  断言 `calls == 2` 要求第一次调用在 40 ms 内返回（剩余 > 一步才会重试）——`time.After(25ms)` 之上再加 15 ms 的调度延迟就
  变成 `calls=1` 红。`open_deadline_test.go` 的 `took ≤ 450 ms`（200 ms deadline）与 `tunnel_adapter_test.go` 的 `took ≤ 400 ms`
  （150 ms 预算）也是 2–3× 的裕量。两遍 e2e-parallel 都绿，但样本只有两个，而 LeakRisk 分片本身刚证明过负载会把 3 s 窗口
  撑破（R3）。
- **建议**：预算 / 步 / 单次时长按 300 / 50 / 150 ms 重排（要求第一次调用在 250 ms 内返回，裕量 100 ms）、`took` 上界放到
  ≥ 2× deadline + 300 ms；tunnel 侧 deadline 300 ms / hold 1500 ms / took ≤ 900 ms、迟到窗口等待 1700 ms；adapter 侧 hold
  1200 ms / 预算 200 ms / took ≤ 700 ms。全是测试常量，不触及产品。
- **不阻断**：假红方向是"门更严"，不是漏检。

### R2 — Minor（理论可达）· 每次 open 都取消握手 ctx，让 `dialAndRegister` 的 watcher 有了一个随机分支

- `dialAndRegister` 的 watcher goroutine `select { case <-ctx.Done(): conn.Close(); case <-hsDone: }`。修前 ctx 是 `c.ctx`，
  只在进程退出时取消，随机性无害；修后每个 `OpenHome` 返回都 `cancelHS()`。若 watcher **在整个握手期间都没被调度**、首次运行时
  两个 channel 都已关闭，Go 随机选分支，50% 会关掉刚安装的 session 的 conn → 一次 spurious drop + supervisor 重拨。
- **实测**：300 次顺序 open × 3（`-race`）= 900 次，0 次 down-edge、REGISTER 数恒等于 open 数——握手至少含一次阻塞读，
  watcher 实际上总在 hsDone 关闭前就阻塞在 select 上。所以是理论可达、未观测到，且后果是自愈的一次重连。
- **建议**：改成确定形式 `stop := context.AfterFunc(ctx, func() { _ = conn.Close() }); defer stop()`——`stop()` 返回 true
  表示回调尚未启动，握手完成后就不会再有人关这个 conn；或在 `ctx.Done` 分支里再查一次 `hsDone` 再关。两行的改动。

### R3 — Note（既有面，非本轮）· `TestBrokerSilenceEscapesToVoter` 的 3 s 退出窗 vs 一次 DNS 名字拨号

- e2e-parallel 首遍的唯一红。该测试逃逸后 rebuild 到 `survivor.example.com:4222`，cancel 时 agent 正在 nats.go 的拨号里，
  而那次拨号先要解析名字：本机 `getent hosts survivor.example.com` 实测 **3.40 / 1.14 / 0.37 / 0.41 s**（systemd-resolved 上游
  NXDOMAIN），并行矩阵下再慢一点就越过 3 s。路径不经过本轮任何改动（该 agent 无 ExposeAdapter、无 state.json）。
- **建议**（下一个增量）：把 survivor 的 host 换成不可路由的字面量（203.0.113.1）——测试要的是"避开静默 broker 的 host"，
  不是 DNS。本轮不动它：不在范围内，且改一个既有测试的语义需要它自己的审查。

## 验证记录

| 验证 | 结果 |
|---|---|
| 首轮 overlay probe ×2（F1 按新接口适配 fake、F2 原样） | PASS |
| 复审自造变异 R-M1 / R-M2 / R-M4（Go）、R-M5 / R-M6 / R-M7（harness） | 全部红，cp/cmp 还原 |
| `internal/tunnel` 全包 `-race`、`internal/agent` 全包 `-race`、`internal/broker` transfer 家族 `-race` | ok（agent 53.6 s、tunnel 7.4 s） |
| `internal/broker` 全包（无 -race，make test 形状） | ok 361.8 s（带 -race 超过 go test 默认 10 min，不是挂死——分片矩阵负责它） |
| `go test -tags d6_integration -race ./test/d6/`、`make vet-tags` | ok |
| `go test ./test/architecture ./test/determinism`（含结构预算、ctx 账本、inventory 只增、enum 家族对账） | ok |
| `sh test/simcluster/tests/run-all.sh`（hermetic 闸集）、`ledger-crosscheck.sh`、`lint-drills.sh` | ALL PASS / OK / 0 violations |
| A2 漂移探测（900 次 open）、B3 交错分布探测（fence 6–11 / 20 ×3） | 0 spurious drop；两种交错都被覆盖 |
| A8 命令替换探测（96 的 assert_setup 形状，dash） | 零每-grow 行、fixture 两行 |
| `gofmt -l`、`git diff --check`、index 未被本轮触碰（`git diff --cached --stat` 仍是首轮快照 + 用户暂存的 CLAUDE.md） | 干净 |
| 主进程四硬闸数字（test 384 s / lint / gates 300 s / e2e 首遍 rc=2 → e2e-one PASS → 重跑 ALL PASS 3m52s） | rc 文件核对一致；首遍红归因见 R3 |

未执行：没有重跑全部 43 个 drill（本轮改动不触及它们的断言；F4 的真栈样本是 74.SRAB，96 的调用形状由 A8 覆盖）。

## 主进程回复（2026-09-20，复审之后、暂存之前）

- **R1 ACCEPTED**：按建议重排三处测试常量（只改测试）。
- **R2 ACCEPTED**：`dialAndRegister` 的 watcher 改为 `context.AfterFunc(ctx, conn.Close)` + `defer stop()`，随机分支不复存在；
  `open_deadline_test.go` 与 `tunnel_reconnect_test.go` 复跑。
- **R3 NOTED**：不在范围；已在 plan §7 "交给其他增量" 登记一行。

### 处置后收据（2026-09-20 13:2x–13:50）

- R2 修后：漂移探测 300 次 open ×2 仍 0 drop；变异"去掉 AfterFunc watcher" → `OpenHome took 1.52s` 红；`internal/tunnel`
  全包 `-race` ok。R1 修后：tunnel open/reconnect 家族与 agent ladder/adapter 家族各 `-race -count=3` ok。
- **四硬闸（最终树，串行，rc 落文件）**：`make test` rc=0 384 s；`make lint` rc=0 13 s；`make gates` rc=0 298 s（hermetic 闸集
  ALL PASS）；`make e2e-parallel` rc=0 **ALL PASS 4m01s，一遍过**（R3 的既有面这次没有出现——它是概率性的，仍按 R3 记）。
- 暂存：按 §4.1 step 5 `git add -A`（被审代码、测试、文档、首轮报告的回复、复审 tasklist 与本报告）；未 commit、未 push。
