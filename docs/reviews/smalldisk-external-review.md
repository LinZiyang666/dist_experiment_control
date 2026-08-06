PASS

# Small-Disk Broker Transfer External Review

日期：2026-08-06
角色：独立外部审查者
基线：`HEAD=71e494338e3ed2a928cf8dfeea9d421e66414da7`

## 结论

**PASS / 放行。**

开发者候选不能原样放行：独立反例稳定确认 6 组 Major、1 组 Medium 与若干 Low
问题。按用户授权，审查者先将开发者候选、tasklist 和 RED 测试冻结到暂存区，暂存
diff 指纹为 `b208a10a3ae410ce849766990d48551b9038f859`；随后直接修复。修复后未发现
剩余 High/Major blocker，全部项目硬闸与指定 sim-cluster 场景通过。

冻结后的 reviewer 生产修复、测试更新、tasklist 收尾和本报告均保持在暂存区外，便于与
冻结候选逐项比较；审查结束后没有再次执行 `git add`。

## 审查范围

- 重读 `CLAUDE.md`、传输/容量设计、部署与 usage 契约、历史外审流程。
- 逐行审查 sizing、existing-bucket resolve/convergence、provision retry、push/pull
  admission、Object Store 配置、CLI 文案、JetStream 常量与 structural budget。
- 直接核对生产固定版 nats-server v2.10.22 与 nats.go 行为，不采信 fake 自证：
  account reservation 按 replicas 倍增，server `storeReserved` 不倍增；stream shrink
  只计算负增量；`UpdateObjectStore` 的零值字段会覆盖现有配置。
- 覆盖 N=1/N=3、有限/无限 account、server 更紧、现有大/小桶、残留对象、并发同桶、
  chunk overhead、极端整数与极小 `max_payload`。

## Findings 与直接修复

### F1 — Major：account/server 容量单位混用，且未取两级限制的更紧者

候选把 account 与 statfs-derived server ceiling 合为一个数，再一律按 replicas 倍增
events/history reserve。N=3 的 server 分支因此过度扣减；有限但宽松的 account 又会
提前返回，完全忽略更小的 server 磁盘限制。

已拆为两个有量纲的约束：account 分支使用 replica-weighted reserve，server 分支使用
单份 reserve，各自计算可用桶上限后取更紧者。补充饱和算术，极端 session/replica 数
不会回绕为小正数或 unlimited。

### F2 — Major：existing-bucket convergence 会误收缩并覆盖运维配置

`ReservedStore` 已包含当前桶，候选却判断 `reserved + target > ceiling`，等价于询问一个
并不存在的第二份桶能否放下。生产无限 account 下又把 replica-weighted
`ReservedStore` 与 statfs server estimate 比较，量纲和时间点均不一致。收缩时重建的
`ObjectStoreConfig` 还会清空 Description、TTL、Placement、Compression、Metadata；
replica raise 具有相同覆盖风险。

已改为仅在有限 account 的精确同量纲数据证明**当前** `max(Store, ReservedStore) >
MaxStore` 时允许 best-effort shrink；无限 account/statfs 只用于保守 sizing，不授权改写
operator-owned 配置。所有 update 均从现有 StreamConfig 完整映射并只改目标字段。

### F3 — Major：一个 session bucket 可同时接纳多个最大对象

桶上限只容纳一个 2 GiB payload 加开销，但 tracker 原先允许同一 session 的多个 Tier-B
transfer ID 并发。两个请求会分别通过准入，其中一个在 `DiscardNew` 中途失败。

已在 tracker 锁内按非空 bucket 串行化 Tier-B；Tier-A 与不同 session 仍可并发，拒绝沿用
既有瞬态 `too_many_in_flight` 语义。

### F4 — Major：disk-bound admission 未从 stream MaxBytes 扣除 chunk 开销

候选只在全局 cap 上增加 64 MiB margin。磁盘约束将桶压到较小值时，请求 payload 可等于
stream MaxBytes，实际 128 KiB chunks 与消息元数据会让 Put 中途触顶。

已引入 backing-limit → payload-limit 的单一换算：扣除现存 `State.Bytes` 与 64 MiB
margin，再受全局 2 GiB 上限约束。错误响应报告可用 payload，而不是 backing stream
reservation。

### F5 — Major：准入忽略现有桶的真实 MaxBytes 与残留字节

候选按新计算 target 做 push 准入，即使 operator 已把桶设为 512 MiB，或 crash 遗留对象
已占 200 MiB，仍可接受必然在 Put 中途失败的请求。

ensure 路径现在返回 resolve/create 后的真实 payload capacity；provision 先用 proposed
capacity 快速拒绝，再用现有 StreamInfo 的 `MaxBytes/State.Bytes` 做第二次权威准入。非
not-found 的 lookup 错误不再伪装成缺桶 create，而进入有界瞬态重试。

### F6 — Major：pull 只有 agent 知道大小，无法执行小盘 payload 上限

`PullPrepareReq` 无 size，候选只在 agent 端检查全局 2 GiB。1.5 GiB 小盘桶上的 1.8 GiB
pull 会在 ObjectStore.Put 中途失败。

broker 在匿名 forwarded payload 中增加可忽略的
`bucket_payload_max_bytes`（pointer 区分“旧 broker 未发送”与“容量为 0”）；新 agent 在
stat 后和实际 reader `limit+1` 两处执行该上限。字段为 additive/omitempty，ProtoVersion
保持 2；旧 peer 会忽略未知字段。

### F7 — Medium：session count 查询不受 sizing timeout 控制

新增路径使用 `QueryRow`，SQLite pool 等待可突破 1.5 秒 sizing budget。`readDB` 现增加只读
`QueryRowContext`，查询继承 sizing context；错误仍保守回退为 1，不扩大数据库写权限。

### F8 — Low：CLI/docs 与结构账本的边界问题

- `usage.md` 的 `> 2 GiB` 行被放到 blockquote 后，已移回 Markdown table。
- 已知极小 `max_payload` 使 `max_payload/2-1KiB <= 0` 时，旧实现忽略该测量并放宽到
  8 MiB；现按文档公式将 tier-A 归零。
- 删除多余 Broker wrapper 后方法数由 286 降到 285，structural budget 已同步收紧，
  没有留下可回涨额度。
- plan 的“草稿骨架”和重复待办标题已收口为实现/外审完成状态。

## 疑惑、剩余风险与建议

1. **其他 session 的既有 xfer bucket reservation 未进入 sizing。** 当前实现会安全地在
   create 时收到 10047 并拒绝，不会中途损坏传输，但小盘多 session 下可能出现“第一个
   session 可用、后续 session 不可用”。建议后续通过同量纲 stream inventory 计入已有
   OBJ_xfer reservation；不要直接把 account `ReservedStore` 套进 server 算式。
2. **statfs 是保守估计，不是 JetStream 启动时固定的精确 server MaxStore。** 因此本轮明确
   禁止它触发 operator 配置收缩，只用于新桶 sizing。若将来需要精确 server admission，
   应增加受支持的 server-side limit 查询，而不是从当前 free space 反推。
3. **混合版本 rollout。** 新 broker → 旧 agent 时，旧 agent 会忽略 pull payload 字段，
   因而在 agent 升级前仍保留旧的 mid-Put 风险；协议不破坏、行为不比旧版更差，但建议
   broker 与承担大文件 pull 的 agent 在同一维护窗口升级。新 agent → 旧 broker 会按全局
   2 GiB 兼容回退。
4. 每 session 串行 Tier-B 是安全优先的吞吐取舍。若未来需要同 session 并发，应改为 tracker
   锁内原子累计“payload + per-object overhead”，并同时处理 pull 的未知 size，不能只提高桶
   cap 或放开当前 guard。

## Verification

- 冻结候选 reviewer regressions：8 个 FAIL，逐项复现 F1–F6。
- 修复后核心回归（12 项，`-count=10`）：PASS。
- `go test ./internal/broker ./internal/agent ./cmd/tether -count=1`：PASS
  （broker 318s、agent 15.6s、CLI 80.7s；后续重跑同样通过）。
- `go test ./...`：PASS；`make test` 的 tagged vet、Darwin build 与全包测试三个组成命令
  均最终 PASS。一次沙箱内复跑因禁止本地 bind 失败，已在有端口权限环境复验，不计产品失败。
- `go test -race ./internal/broker ./internal/agent ./cmd/tether ./test/concurrency -count=1`：
  PASS（broker 567s、agent 21s、CLI 134s、concurrency/leak 46s）。
- `make gates`：PASS；golangci-lint v2：0 issues。
- `make e2e-parallel`：ALL PASS，99/99 units、15/15 top-level matrices，3m45.76s。
- `test/simcluster/local.sh --build build`：PASS，镜像固定 nats-server 2.10.22。
- `test/simcluster/local.sh drill 21-smalldisk-tierb`：GREEN，8/8 assertions；候选冻结前与
  reviewer 修复后各执行一次。
- `git diff --check`：PASS；最终暂存 diff 指纹保持
  `b208a10a3ae410ce849766990d48551b9038f859`。

## 最终暂存边界

- 暂存区：冻结的开发者候选、外审 tasklist、RED 回归测试。
- 暂存区外：reviewer 的生产修复、测试更新、文档修正、本报告。
- 未执行 commit/push，未在修复后再次暂存。

---

## 主进程回复（2026-08-06）

F1–F8 **全部独立复核后采纳**——逐条对着源码与实跑核对，不因其来自外审而照单全收。
F3 我先怀疑「同 session 全量串行」过于粗暴（两个小文件也会被拒），查证后采纳：
pull 腿的 `entry.size` 是空的（broker 只从 agent 才知道大小），所以「按在途字节累计」
在 pull 上无法实现，报告 §4 已把这条取舍写明。

### 补了三处

1. **F3 与 F4 的修复没有守卫。** 变异验证：删掉同桶串行化、去掉 chunk margin、
   去掉已用字节扣减——**三个变异全部零失败**。修了真缺陷却没留下能变红的测试，
   而这两处恰好长得像「多余的循环」和「多余的减法」，一次化简重构就会把它们撤销。
   已补 `TestTierBIsSerializedPerSessionBucket` /
   `TestPayloadLimitSubtractsChunkOverheadAndUsedBytes`，三条变异各自精确变红。

   **一处自我更正**：我最初也判 F2 无守卫，那是**我变异打错了位点**——
   `TestConvergenceDoesNotGuessFromStatfsWhenTheAccountIsUnlimited` 在正确位点
   （去掉 `info.Limits.MaxStore <= 0` 那道守卫）**确实会红**。该判断已撤回。

2. **`xferStoreCannotFit(ctx, _ int64)` 的参数已成死参数，且函数名与行为不符**——
   它不再回答「放不放得下 want」，而是「账户是否已过量预留」。
   重命名为 `xferAccountIsOverCommitted(ctx)` 并删掉死参数。

3. **一个报告未讲明的后果**（详见 `smalldisk-plan.md` §10）：F2 把收敛限制在有限 account
   限额上，而 tether 不渲染 `accounts{}`，生产账户限额恒为 -1 ⇒
   **收敛在每台真实 broker 上都不会触发**。

   这不使增量失效——racknerd 的 push 靠 **resolve-before-create** 就能用了——
   但它推翻了本增量 plan §1 的中心论断「收敛是必需项」。真实情况是：push 可用，
   但那个 8 GiB 空桶的预留**仍在**，该 broker 上新建会话的 history 流仍会撞 10047
   （即报告残留 §1）。

   外审的安全立场我认同（用 statfs 估算去改写运维所有的配置，风险大于收益）。
   **因此现网仍需一次运维动作**缩掉那个空桶，命令见 plan §3；
   这一条必须写进发版说明，不能让人以为升级即自愈。

### 复核所跑的闸门

`gofmt` clean · `go build ./...` · `go test ./...` rc=0 · 三条新守卫的变异各自精确变红。
完整硬闸结果见下方提交前的最终记录。
