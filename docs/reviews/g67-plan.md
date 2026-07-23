# G67 — transient tier-B refusal reported as permanent, with zero retry — PLAN

> Status: **Stage A finalized** (main process is the sole finalizer). Drafted by an 11-agent Opus 4.8
> adversarial workflow (5 lenses draft → 5 adversarial critics, one per draft → 1 synthesizer); the main
> process then **re-verified every load-bearing claim against source** and made the §0 decisions.
> Leaf increment, not a linear P-phase. Follows CLAUDE.md §3 (Stage B implement → Stage C internal
> review → external review gate).
>
> Base: main HEAD `b602fc7` **plus the uncommitted R16 working tree** (R16 is parked at its external
> review gate; this increment lands on top of it and shares that gate).
> Defect SSOT: `docs/deploy-tier-gotchas.md` `### #67`. Pin: `test/simcluster/drills/67-transient-js-refusal.sh`.
> Discovery lineage: found by drill 42 repeat run 3 during R16 deploy-tier validation, then root-caused
> with two targeted probes on `weilandserver`.

---

## 0. 定稿人决定（binding；实现以本节为准）

1. **Q1 — 落地测量，不留假设。** provisioning 失败时**永久性**记录 `sizing_ms` / `create_ms` 两个字段。
   理由：本缺陷的调查过程里我已经被自己未测量的假设打回过一次（"5s 预算偏紧" → 实测建桶 ~57 ms、100× 余量）。
   台账里关于"哪一段吃掉了预算"的机理描述，在这两个数落地前**只能写成待证假设**。
2. **Q3 — 重试 3 次，但把单次超时与总预算压紧：`xferCreateAttemptTO = 2.5s`、`xferProvisionBudget = 8s`
   （create 循环墙钟）、`xferSizingTimeout = 1.5s` 独立。** 综合稿给的是 2 或 3 次的二选一（3 次最坏 ~11.5s、
   2 次 ~8s）。我两个都不取：**R1（head-of-line blocking）是我亲自验证过的头号风险**——`.push.req` 在
   `isBroadcastClusterSubject`（`clusterwrite.go:59-61`）里，走普通 `nc.Subscribe`（`broker.go:1025-1028`），
   handler 直接作为 nats.go 异步回调注册、**无 goroutine 包装**，因此**全 broker 的 push.req 串行在一条投递
   goroutine 上**；但 2 次只覆盖 ~3s 停顿，对一次 meta 选主偏薄。压紧后 in-handler 最坏 ≈ 1.5 + 2.5×3 + ~0.9
   → 受 8s 预算钳制 ⇒ **≈ 9.5s（今日 5s 的 1.9×）**，仍远小于 `transferTimeoutTierA = 30s`（`transfer.go:50`）。
3. **Q2 — 不做**：本增量不再把 `bucket_create_failed` 拆出 small-disk 专用码。瞬时态一旦被 `jetstream_not_ready`
   切走，`bucket_create_failed` 本身已无歧义。
4. **Q4 — 不做**：drill 不对被拒 push 断言墙钟下限（本注入下每次尝试都快速失败，可观测差值只有 ~1s backoff，
   立不稳的断言比不立更坏）。墙钟只记日志。
5. **Q5 — 推迟并登记**：`b.js` 的 boot 探测是一次性的 1s `AccountInfo`（`broker.go:1057-1059`），失败后永不重探。
   懒重探是正解，但 `b.js` 是被大量点直接读的裸字段，需要 `atomic.Value`/`RWMutex` 改造 + 独立 `-race` 过一遍
   ⇒ **不进本增量**，作为 open sub-face 登记；本增量改为把 nil-JS 拒绝的文案写成**同时点名两种成因**。
6. **Q6 — 做**：`too_many_in_flight` / `transfer_id_in_flight` 一并进 exit-class 映射（两行 map，零文案改动）。
   它们本就被 broker 写成 `retry shortly`（`transfer.go:600`/`:730`），却落在未分类的 70。
7. **同 PR 更正一条既有外审记录。** `docs/reviews/g1-g7-audit-external-review.md:76` 当初放行 A11
   （把准入与建桶的两次 `AccountInfo` 合成一次）的理由之一写着「客户端本就对整次 push 重试」——
   **该前提为假**，树内不存在任何客户端重试（`runPush`/`runPull` 皆无）。面 A 因此很可能是一次**建立在错误事实上
   被接受的回归**。必须原地更正，否则同一个错误前提还会再放行下一次。
8. **范围外，明确不做**（每条都有硬证据，见 §2）：把 transfer 拒绝改走 `brokerErrorMessage`；客户端对 prepare
   做任何重试；签名 (ii) rc=75 的根（`transferHomeGate`）；签名 (iv) 连接层失败。

---

## 1. 根因

一个缺陷、两张面孔，同一个成因：**用户面 tier-B 路径上根本不存在"瞬时"这个概念**。两端都把临时状况转成终态，
而客户端还额外**编造了一个没人报告过的成因**。

### 面 A — broker 侧（drill 已确定性复现）

`handlePushReq`（`internal/broker/transfer.go:555-579`）与 `handlePullReq`（`:691-699`）把**两次独立的
JetStream 往返塞进同一个 5s context、各跑一次、任何错误一律转成终态 `bucket_create_failed`**：

- 往返 1 — `xferBucketMaxBytes` → `jsStoreCeiling` → `b.js.AccountInfo(ctx)`。**其错误被吞掉**
  （`transfer.go:207-215`：`if info, err := …; err == nil && …`，失败即 fall through 到 statfs）。
  所以一次停住的 `AccountInfo` 本身不报错，只是**静默吃掉共享预算**。
- 往返 2 — `ensureXferBucketSized` → `js.CreateObjectStore(ctx, cfg)`，拿到的是同一个 ctx 剩下的部分。

nats-server 对两个 endpoint 的把关是同构的：`isLeaderless()` → 快速 503/10008；否则 `!JetStreamIsLeader()`
→ **直接不回复**。于是每次往返要么快速失败、要么静默烧光 deadline ⇒ 两个窗口、两种底层错误、一个共享预算：

| 窗口 | AccountInfo | CreateObjectStore | 运维看到的 |
|---|---|---|---|
| meta leaderless（drill 的 clean stop） | 快速 503/10008（被吞 → statfs） | 快速 503/10008 | `create_bucket: … err_code=10008` |
| meta 正在重新指派、尚无 leader（**未注入的 grow 后窗口**） | 静默丢弃，吃掉 ~5s | 立即过期 | `create_bucket: context deadline exceeded` ← 签名 (i) |

这解释了「实测 ~57 ms 的建桶为何产出 5s deadline 错误」。**注意：上表两行中"究竟哪一段吃掉预算"目前是两个
同等成立的假设，不是已证事实**——§0 决定 1 的 `sizing_ms`/`create_ms` 就是为了在第一次部署层运行时把它定死。
设计对两种情形都成立（拆开 deadline 两者都修），但台账机理措辞必须等数据。

这条路径上**没有任何分类**。`jsstream.IsMetaGroupNotReady` 存在且正确，但其消费者只有 reconcile 循环与
`raiseXferReplicas`——而后者算出的"可重试"判定，转手就被扔进同一个硬拒绝。

### 面 B — client 侧（人工探针复现过一次；clean-stop 注入下不复现）

`cmd/tether/transfer.go:162`（push）与 `:423`（pull）：`caps, _ := probeCaps(...)` **丢弃错误**；
`probeCaps`（`:714`）在传输**与**解析失败时都返回零值 `proto.CapsResp{}`，且**从不读 `resp.OK`**。
而 `handleCapsReq`（`internal/broker/transfer.go:987-1013`）对 `not_a_member` /
`session_not_found_or_deleting` / `store_error` / `actor_invalid` 都回 `OK:false` 且 `JetStreamReady` 留零。
于是**四种截然不同的状态坍缩成同一个字节模式**，`chooseTier:761` 据此**制造**出一个 broker 从未做过的永久能力断言，
外加与故障无关的 `max_payload` 建议。`MaxPayload=0` 还顺带跳过 inline 钳制，把 tier-A/B 分界**悄悄抬到** 8 MiB 默认值。

### 明确**不是**本缺陷 —— 不要"修"这些

- **quorum 缺失时拒绝 tier-B 写**：R=2 资产确实写不了，拒绝是正确行为。
- **5s 预算太小**：健康建桶有 ~100× 余量。**调大它是最差选项**——push.req 串行（见 §0 决定 2），
  加长天花板只会拉长 head-of-line 阻塞而不增加任何一次尝试。
- **`IsMetaGroupNotReady` 太窄**：它**对它的消费者而言是正确的**。`audit_publisher.go:141/481/585` 的用法是
  `err != nil && !errors.Is(err, ErrMetaGroupNotReady)`，即**命中即让错误消失**。放宽它会把硬失败变成
  `Degraded()` 背后看不见的 no-op。**本增量一个字节都不动它。**
- **签名 (iv)** `cannot reach broker … i/o timeout`（rc=69）：连接层，已被 drill 自己的签名闸拒绝过一次。
- **签名 (ii)** `push (tier B prepare): context deadline exceeded`（rc=75）：其退出码已由
  `classifyExit` 的 `context.DeadlineExceeded` 臂正确给出；**根**在 `transferHomeGate` 对未解析 home 的静默
  （`transfer_home.go:54-63`），属集群路由议题 ⇒ 范围外，登记为 open sub-face。

---

## 2. 范围

### IN

| # | 项 | 面 |
|---|---|---|
| 1 | 把 sizing 往返与 create 往返拆到**独立 deadline** | A |
| 2 | **有界、带分类的 broker 侧重试**，只重试 create 段，`handlePushReq` + `handlePullReq` 两处，整段活在 `transfers.put()` **之前** | A |
| 3 | 新增瞬时码 `jetstream_not_ready` + 按分类给出的诚实文案；`bucket_create_failed` 的永久文案**逐字不变** | A |
| 4 | 给 broker 的 nil-JS 拒绝写上真正的文案（今天 `Error` 是**空串**，`transfer.go:557`） | A/B |
| 5 | 六个 transfer 拒绝点挂 exit class，**人类可读文本逐字节不变** | A |
| 6 | 面 B 全量：保留探测错误、按 `caps.OK` 分流、删掉那句编造的断言、tier-A 上限改由真实测量推导 | B |
| 7 | pull 复用同一个上限 helper（删掉 `:424-430` 的重复算术） | B |
| 8 | drill 67 改写 + `expected-verdicts.tsv` + 台账拆分 | — |

### OUT（每条都有硬证据）

- **把 transfer 拒绝改走 `brokerErrorMessage`**：`test/simcluster/drills/61-transfer-edges.sh:32` 的
  `refuse_clean` grep 的是**字面 token `code=<X>`**，其 11 处用法中 10 处是 push/pull 拒绝；61 在
  `expected-verdicts.tsv` 里是 **GREEN**。而 `brokerErrorMessage` 输出的是 `<verb> failed: <msg> (<code>)`
  ——没有 `code=`；它还会在存在 hint 时**丢弃原始 broker 错误**（`error_hints.go:149-153`，`msg = hint`）。
  落它会把一个现役 GREEN drill 打红，并毁掉瞬时/永久判别器。**exit class 改为不碰文本地挂载。**
- **客户端对 prepare 做任何重试**：pull 上不安全、push 上不必要。
  - **pull-prepare 就是数据面**：agent 在回复**之前**把整个文件 `store.Put` 进 bucket
    （`internal/agent/transfer.go`：tier-B 分支上传完成后才 `replyPull(... OK:true ...)`）。重试它 = 成倍的
    并发多 GB 上传与 bucket 占用。
  - `runPull` 对非 `transfer_id_in_flight` 的拒绝会发 `sendFinalize`（`cmd/tether/transfer.go:458`），
    而 `finalize.req` 与 `pull.req` 是**不同的 subscription**（`broker.go:993` vs `:986`）、无顺序保证 ⇒
    同 id 重试会让第 1 次的 finalize 认领并终结第 2 次的在飞传输。
  - `transfer_id` 是在 prepare **之前**打印的（`:170`/`:434`）；每次重试换新 id 会让打印出来的 id 变成谎话。
- 签名 (ii) 的根、签名 (iv)：见 §1。

---

## 3. 设计

### 3.1 `internal/jsstream/transient.go`（NEW）

`IsTransientProvisionErr(err error) bool` —— **刻意独立于** `IsMetaGroupNotReady`（后者喂 reconcile 循环，
命中即让错误消失并无限重试，放宽会把永久错配拖成几天的静默空转）。求值顺序**永久优先**：

| 序 | 规则 | 判定 |
|---|---|---|
| 0 | `err == nil` | false |
| 1 | `errors.Is(err, jetstream.ErrJetStreamNotEnabled)` / `…ForAccount` | 永久 |
| 2 | `*jetstream.APIError` 且 `ErrorCode ∈ {10076, 10039, 10047}` | 永久 |
| 3 | 子串 `non-clustered` / `not supported` / `insufficient storage` | 永久 |
| 4 | `errors.Is(err, context.Canceled)` | 永久（停止） |
| 5 | `errors.Is(err, context.DeadlineExceeded)` | 瞬时 |
| 6 | `errors.Is(err, nats.ErrNoResponders)` | 瞬时 |
| 7 | `*jetstream.APIError` 且 `ErrorCode ∈ {10008, 10004, 10023, 10005, 10202}` | 瞬时 |
| 8 | `errors.Is(err, jetstream.ErrStreamNotFound)` / `ErrBucketNotFound` | 瞬时 |
| 9 | `errors.Is(err, ErrMetaGroupNotReady) \|\| IsMetaGroupNotReady(err)` | 瞬时 |
| 10 | 其它 | **永久（默认）** |

承重注记（已由主进程对 `nats-server@v2.14.0/server/jetstream_errors_generated.go` 复核）：

- **绝不能用笼统的 `Code == 400` 规则。** 实测该文件：`JSClusterNoPeersErrF = 10005`（:60/:694，Code 400）与
  `JSClusterServerMemberChangeInflightErr = 10202`（:81/:701，Code 400，描述字面是
  `cluster member change is in progress`）**两者都是 400 且都是瞬时**——10202 就是"grow 正在进行"，
  即 #67 的头号窗口。笼统 400 规则会让这个修复**对它自己的严重度场景失效**。
- `10047`（insufficient storage resources available，Code 500）是永久，必须**排在** `insufficient` 子串规则之前
  ——与 `IsMetaGroupNotReady` 现有做法一致。
- nats.go v1.52.0 只导出了其中 `10039`/`10076` 的常量；其余以 `jetstream.ErrorCode(10008)` 等写在一个 `const`
  块里并注明来源文件。注释里要老实写"半结构化"，别把它吹成纯结构化判定。
- 规则 8 之所以存在，是**因为重试本身让这个竞态变得可达**：一次已在服务端提交但回复丢失的 create 会在第 2 次
  返回 `10058`（`JSStreamNameExistErr`，:591/:871），从而拐进 `raiseXferReplicas`（`transfer.go:294-302`），
  而其 `js.ObjectStore` 可能在 stream assignment 传播期间 404。该函数用 `%w` 包装，`errors.Is` 看得穿。

### 3.2 `internal/broker/transfer.go` — 永久 sizing 拒绝的 sentinel

`xferMaxBytesForCeiling`（`:236-238`）现在返回裸 `fmt.Errorf`。引入
`var errXferStoreTooSmall = errors.New("js store too small for tier-B")` 并 `%w` 包装，**文本不变**，只为让
`errors.Is` 可用。没有它，"永久集"测试在**整个永久分支被删掉**时依然通过（默认本来就是 false）。

### 3.3 `internal/broker/xfer_provision.go`（NEW）

常量（§0 决定 2 已压紧）：

```
xferSizingTimeout     = 1500 * time.Millisecond  // best-effort；jsStoreCeiling 本就吞错误
xferCreateAttemptTO   = 2500 * time.Millisecond  // 每次尝试，独立 ctx
xferProvisionMaxTries = 3
xferProvisionBackoff  = 300 * time.Millisecond   // ×3 增长，±25% 抖动
xferProvisionBudget   = 8 * time.Second          // create 循环墙钟上限
xferProvisionMinSlack = 1 * time.Second          // 预算不足以完整跑一次就不再起新尝试
```

`provisionXferBucket(parent, sid, size) (bucket string, tooLarge *xferTooLarge, perr *xferProvisionErr)`：

1. **sizing 独立 deadline** → `xferBucketMaxBytes`，立刻 `cancel()`。**这是首要修复**，重试是次要的。
2. **准入语义不变**：`sizeErr == nil && size > maxBytes` → 返回 `tooLarge`，调用方回**现有的 `too_large` 码与
   现有文案逐字**。必须是**带类型的返回值**而非普通 error，否则 G6 #21 的准入拒绝会退化成 `bucket_create_failed`。
3. **`sizeErr != nil` ⇒ 零次 create 尝试**，`Transient=false`。盘太小不是停顿。
4. **create 循环**：每次尝试 `context.WithTimeout(parent, min(xferCreateAttemptTO, remaining))`、用后立即
   `cancel()`（循环里**不用 defer**）；出错后在下列任一成立时停止——`parent.Err() != nil`、
   已达 `xferProvisionMaxTries`、`!jsstream.IsTransientProvisionErr(err)`、`remaining < wait + xferProvisionMinSlack`；
   backoff 用 `time.NewTimer` + `select { <-t.C; <-parent.Done() }` + 显式 `Stop()`（**不用 `time.After`**，
   它会把定时器钉满整个时长，触怒仓库自建的泄漏门）。
5. 返回 `xferProvisionErr{Err, Transient, Attempts, Elapsed, SizingMs, CreateMs}`。

**`parent` 必须是 `b.runCtx` 而非 `context.Background()`**：用 `Background()` 的话 shutdown 分支是死代码，
且普通的预算耗尽（`DeadlineExceeded`）会被报成"broker 正在关闭"——那正是 #67 登记的那类新的假陈述。
按 `errors.Is(parent.Err(), context.Canceled)` 与 `DeadlineExceeded` 分流。

**日志（§0 决定 1；这是部署层的反空过 oracle，不是装饰）**：每次重试 `Warn`；重试后成功 `Info` 带 `attempts`
与 `elapsed_ms`；放弃 `Warn` 带 `attempts`/`elapsed_ms`/`transient`/`err`。失败路径**恒带** `sizing_ms` 与 `create_ms`。

### 3.4 `internal/broker/transfer.go` — 调用点与确切文案

两个 handler 均变为：nil-JS 检查 → `provisionXferBucket` → `tooLarge` 回复（仅 push）→ 错误回复。
其后的 `req.Bucket` / `transfers.put` / `writeXferInflight` / watchdog / audit start / **R7a 顺序守卫
（`transfer.go:617`，注释明写 do-not-remove）** / 转发**全部不变、顺序不变**。

**`ensureXferBucket` 删除**——其唯一非测试调用者就是 `handlePullReq:694`；树内**没有任何 `.golangci.yml`**，
golangci-lint v2 默认跑 `unused`，留一个孤儿未导出方法会让 `make lint`（硬闸）失败。

新码常量 `codeXferJSNotReady = "jetstream_not_ready"`。文案：

- **瞬时**（`jetstream_not_ready`）：说明尝试次数与耗时、点名**三种**常见成因（broker 重启 / 选主 / grow 进行中）、
  明确 `retry the same command shortly`、并给出持续失败时的升级路径（`tether cluster status`）。
  **绝不**把 quorum 缺失断言为事实——一个永久离队的 peer 会产生一模一样的 `DeadlineExceeded`。原始成因用 `%v`
  原样带出，`create_bucket:` 等前缀保持可 grep。
- **永久**（`bucket_create_failed`）：**文本逐字不变**，`err.Error()` 原样。它**刻意不含** `retry|transient|temporar`,
  这样永久态永远不可能与瞬时态混淆。
- **中止**（broker 关闭中）：单独措辞，仍用 `bucket_create_failed`。
- **nil-JS**（替换今天 `:557` 的空 `Error`）：必须**同时点名两种成因**——nats.conf 里禁用了 JetStream，
  **或** broker 启动时那次一次性探测失败（`broker.go:1057-1059`，此后永不重探），并给出"重启即重探"的补救。
  只说"JetStream 被禁用"是 broker 根本无权做出的断言。

### 3.5 `cmd/tether` — 只挂 exit class，**不动文本**

`transferRefusalErr` + 映射：`jetstream_not_ready` → 75（瞬时/可重试），`bucket_create_failed` → 70，
`too_many_in_flight` / `transfer_id_in_flight` → 75（§0 决定 6）。**人类可读文本逐字节不变**，
`code=<X>` token 保留 ⇒ drill 61 无需改动。

### 3.6 `cmd/tether/transfer.go` — 面 B

**(a) 带类型的探测结果**：`capsUndetermined`（传输/解析失败，**没有可用答案**）/ `capsRefused`（broker 答了但拒绝，
`OK=false`）/ `capsOK`（权威）。`probeCaps` 改用 `cmd.Context()` 以便 Ctrl-C 生效。**不重试探测**——
一旦"探测失败 = 交给 broker 裁决"，重试只买来每次 push 至多 3s 额外延迟。

**(b) tier-A 上限不再依赖一个可能不存在的测量**：`tierAInlineCeiling(connMaxPayload, caps)` 从
`cliTierAMaxBytes` 起步，对**每一个已知的** `p > 0` 取 `min(cur, p/2-1024)`，**绝不**因为某个测量缺失而放宽。
`nc.MaxPayload()` 是"本客户端能发多大"的地面真值。push 与 pull 共用它，删掉 `:424-430` 的重复算术。

**(c) `chooseTier` 决策表**：`size <= ceiling` → tier a（探测失败绝不阻断 tier-A）；`capsOK && JetStreamReady`
→ tier b；`capsOK && !JetStreamReady` → **拒绝**（此时才是真·永久态，`usageErr` ⇒ 64），
文案按"文件是否本来就大于 tier-A 天花板"分两版——**只有在提高 max_payload 确实是一条出路时才提它**；
`capsUndetermined` / `capsRefused` → **乐观走 tier b + stderr 警告**，让 broker 裁决。

后两行是修复的要害：**权威答案就在一次 RPC 之外**，本地拒绝等于再编造一个断言。这一行为**只有在 §3.4 给了
broker 的 nil-JS 拒绝真文案之后才可接受**——否则只是把"编造的消息"换成"空消息"。
`broker has no JetStream` 与那句无条件的 `bump nats max_payload` 建议**从树中删除**。

---

## 4. wire / 兼容性裁决

**纯增量，不触发 v1→v2。** 新增的只是 `Code` 字段的一个**新字符串值**，`PushPrepareResp` / `PullPrepareResp`
的结构不变、无新字段。旧 ctl 遇到未知 code 会走默认分支照常打印（`Code` 本就是自由字符串）；新 ctl 连旧 broker
时永远收不到该值，行为与今天一致。`internal/proto.ProtoVersion` **不变**。`messages.go` 只改 `Code` 的文档注释。

---

## 5. 风险与守卫（节选，完整表见综合稿）

| # | 风险 | 守卫 |
|---|---|---|
| **R1** | **head-of-line blocking（头号风险，已由主进程独立验证）**：push.req 走普通 `Subscribe`、handler 无 goroutine 包装 ⇒ 全 broker 串行；该 subscription 还承载至多 8 MiB 的 tier-A `InlineData`，阻塞会顶 pending-bytes 上限触发 slow-consumer 丢弃 | §0 决定 2 的压紧参数（in-handler 最坏 ≈9.5s = 1.9×，且 < 30s tier-A 超时，由常量关系测试钉住）；**永久分类恰好一次尝试**即返回；sizing 拆分让常见路径**不变慢**；并发 pin。**不**用"派发到 goroutine"解决——那会在 R7a 守卫正要保护的路径上引入无界 goroutine 与重排序风险 |
| **R2** | 重试永久条件空转（尤其 10047、G6 #21 小盘拒绝） | 永久规则**先行**；未知 ⇒ 永久；`sizeErr != nil` ⇒ **零**次尝试；`errXferStoreTooSmall` sentinel 让"永久集"测试结构性成立 |
| **R3** | 重试 `CreateObjectStore` 产生重复/孤儿 stream | 结构上不可能：名字确定（`OBJ_xfer-<sid>`，每会话一个）。回复丢失的已提交 create 在第 2 次得 `10058` → 落入 `raiseXferReplicas`（raise-only、`cur >= target` 返回 nil、**保留**既有 `MaxBytes`）⇒ 部分创建收敛为成功 |
| **R4** | 重试与 tracker / #57 ledger / audit start / watchdog / R7a reaper 相撞 | 架构性而非防御性：整段活在 `transfers.put()`（`:596`/`:726`）**之前**。不占 tracker 槽位 ⇒ 1024 上限不受影响；`startedAt` 仍在 accept 时取 ⇒ provisioning 不吃传输自己的 watchdog 预算；provisioning 中崩溃**不留** ledger 文件、不留 start 行 |
| **R5** | **最可能的"虚假修复"**：把 `retry shortly` 喷到每一条拒绝上，让这个词失去意义 | 永久文案**逐字节不变**且不含 `retry\|transient\|temporar`；测试**双向**断言；drill 的反空过牙齿是**证明重试真的跑过的日志行**，不是文本 grep |
| **R6** | 新的瞬时措辞像面 B 一样过度承诺（永久离队的 peer 会产生同样的 `DeadlineExceeded`——参见 racknerd 事故：滞留的 clustered nats.conf 静默 503 了五天） | 措辞写「**通常**是瞬时」、点名三种具体成因、结尾给升级路径；绝不把 quorum 缺失断言为事实 |
| **R10/R11** | drill 61（GREEN）与 drill 92 回归 | §3.5 保留 `code=` token 与逐字节文本 ⇒ 61 无需改动；92 的正则仍匹配新瞬时文案，新增延迟 ≤ ~7s，远在其 90s poll 内。两者都要重跑 |

---

## 6. 测试与部署层证据

**基线已核实**：`grep -rn "broker has no JetStream\|bump nats max_payload\|bucket_create_failed\|create_bucket\|chooseTier\|probeCaps" --include=*_test.go .` **返回空**
⇒ 下列每一条都是净新增覆盖，不会打破既有测试。

每条 pin 必须与"删掉它就变红"的**具体突变**配对（R16 内审 M7 抓过我这个毛病）。要害几条：

- **T4 `TestSizingStallDoesNotConsumeCreateBudget`**（**Step 0，RED-first**）：对**今天的代码**先写，
  必须因为"create 拿到一个已过期的 ctx"而红。这是根因的裁决。
- **T5**：两个 verb 各一行，断言瞬时态得到 `jetstream_not_ready` 且文案含重试线索、永久态得到
  `bucket_create_failed` 且**不含**任何重试词。**双向**。
- **T2**：对**未被修改的** `IsMetaGroupNotReady` 做两侧钉——把"刻意不放宽"这个决定写进代码。
- **T10**：常量关系（预算 < `transferTimeoutTierA`）。
- **T9**：并发 + `-race` + 仓库自建的 `runtime.NumGoroutine` / fd 门（**非 goleak**）。

**drill 67 的翻转**：`_g67_tierb_face` 需容纳新码；注入臂从"期望 PRODUCT-RED"改为 **GREEN 回归**，
其**反空过牙齿是日志行**（`broker: tier-B bucket provisioning retried` / `… provisioned after retry`）——
证明重试**真的跑过**，而不是"这次恰好没撞上窗口"。**注入前的控制臂**（曾自发复现面 A）改为：
不再需要重试即应成功；若仍需重试，则该 drill 仍记 product_red（缺陷未消）。
`expected-verdicts.tsv` **只有在部署层跑绿之后**才翻。

**台账**：`### #67` 拆成 `#67-A`（本增量修复，具名 oracle 达成后 CLOSED）/ `#67-B`（面 B 的 CLI 部分修复；
但 face-B 无 drill 级 oracle 这一点必须保留为 OPEN 记录）/ 三条 open sub-face（一次性 boot 探测、
`transferHomeGate` 静默、面 B 的 drill oracle 缺失）。**绝不因为"改了代码"就整条 CLOSED。**

---

## 7. 实现顺序

0. **RED-first**：T4 对今日代码写，必须因**指定理由**变红。此前不写任何修复。
   > **实施记录（偏差，如实登记）**：T4 已 RED，理由精确——`CreateObjectStore` 拿到的 ctx 剩余
   > **-4.25 ms**（已过期），即停住的 sizing 探测把共享预算吃光后，**承重的 create 连一次尝试都没得到**。
   > 这比"预算不足"更强，直接坐实机理。**T5 的瞬时行未在 step 0 写**：它断言的"瞬时 vs 永久选不同的码与文案"
   > 这个判别 seam 今天不存在，对今日代码写出来只会编译不过——那是无信息量的 RED，不是根因裁决。
   > T5 改在 step 4 落地，并以**当场突变检查**（还原两个 handler 块 ⇒ T5 必红）给出同等强度的保证。
1. `internal/jsstream/transient.go` + T1/T2（纯函数，无依赖；T2 把"不放宽"钉在代码里）。
2. `errXferStoreTooSmall` sentinel（一行，文本不变）。
3. `internal/broker/xfer_provision.go` + 三条日志 ⇒ T3/T4 转绿，T10 落地。
4. 改接两个 handler、删 `ensureXferBucket`、新文案与新码、nil-JS 文案 ⇒ T5 转绿。**当场做突变检查并记录**：
   把两个块还原 ⇒ T5 必须红。
5. `internal/proto/messages.go` 文档注释（仅注释）。
6. `cmd/tether` exit class（文本逐字节不变）⇒ T8。
7. `cmd/tether` 面 B（`capsProbe`/`tierAInlineCeiling`/`chooseTier` 改写、两个调用点、pull 去重）⇒ T6/T7。
   grep 复核两条被删字面量确实消失。
8. T9 并发 + `-race` + 泄漏门。
9. 文档：`usage.md`、**`g1-g7-audit-external-review.md:76` 更正（§0 决定 7）**、台账 `#67` 拆分改写。
10. drill 改写；tsv **最后**才翻。
11. 硬闸：`make test` + `make e2e` + `make lint`；部署层 `67-transient-js-refusal` ×2 独立运行，
    外加 `61-transfer-edges` 与 `92-js503-remote-alert` 各自的期望判定。

**若必须裁剪**：1–4 一刀不能少；先砍 6（exit class），再砍重试（保留 deadline 拆分 + 分类 + 文案），
**deadline 拆分是最后才能砍的东西**。
