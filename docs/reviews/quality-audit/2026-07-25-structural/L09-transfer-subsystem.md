# L09 — 文件传输子系统：结构性质量审计

> lane key: `transfer-subsystem` ｜ 2026-07-25 ｜ 只读审计，未修改任何实现代码
> 本轮找的不是 bug，是**冗余 / 重复 / 抽象错位 / 演进阻力**。

---

## 结论

**净判断：这 5,611 行不臃肿，也不是屎山。lane 简报里"broker 侧 1210 行和 agent 侧 1114 行是同一份逻辑写了两遍"的假设是错的——两侧职责正交（broker = 准入/审计/桶生命周期，agent = 路径安全/落盘），真正重复的只有 4 处终态处置手抄和 6 个跨包常量副本，合计约 105 行。本 lane 真正要紧的结构债只有两条：(1) 传输缺"预算"这个抽象——声称的 2 GiB tier-B 上限和 5 分钟看门狗是互不推导的两个字面量，broker 会主动删掉还在传的对象；(2) 43 个错误码里 29 个没有退出码归类，全部落到 exit 70，而 `docs/usage.md:1542` 告诉自动化 70 要退避重试。**

**bloat 打分：4 / 10**（1=精炼，5=正常工程债，10=屎山）

理由：

- **没有屎山特征**：没有重复实现、没有为一个实现造的 interface+factory+registry、没有上帝对象、没有死抽象层。`internal/storage` 是 269 行生产码直接返回 `*sql.DB`，是"抽象不足但诚实"而不是过度抽象。
- **扣分点 1**：终态处置序列（claim → emit terminal → delete object → cancel watchdog → tracker.remove）被手抄 4 遍，连同一句警告注释一起（`transfer.go:440/934/961/1201` 四行字节相同）。这是本 lane 唯一称得上"复制粘贴"的地方。
- **扣分点 2**：`cmd/tether/transfer.go` 880 行里约 570 行是协议客户端逻辑而非 CLI（17/26 个函数完全不依赖 cobra）。不是冗余，是**错位**——净行数不会减，但任何第二个传输客户端都复用不到一行。
- **扣分点 3**：注释里约 12% 是"评审考古"（"以前的版本怎么错的"）。9 个主文件共 1427 行注释，其中 672 行落在含评审标记（`ROUND-4 RE-REVIEW` / `external review F1` / `R5-F1` / `G67 m14` / `audit shard P11 F2`）的注释块里。其中相当一部分是决策依据（该留），另一部分是历史（该进 `docs/reviews/`）。
- **不扣分的地方**：`xfer_provision.go` 的常量全部带实测依据（"create 本身 ~57ms"、head-of-line blocking 的推理），`agent/transfer.go` 后半段 560 行的 openat 加固每一步都对应一个具体攻击。这些体量是**正当**的。

---

## 范围与方法

| 文件 | 总行 | 注释 | 空行 | 代码 | 注释占比 |
|---|---:|---:|---:|---:|---:|
| `internal/broker/transfer.go` | 1210 | 344 | 62 | 804 | 28% |
| `internal/agent/transfer.go` | 1114 | 209 | 55 | 850 | 18% |
| `cmd/tether/transfer.go` | 880 | 131 | 60 | 689 | 14% |
| `internal/broker/home_delivery.go` | 676 | 247 | 42 | 387 | 36% |
| `internal/broker/xfer_inflight.go` | 607 | 195 | 27 | 385 | 32% |
| `internal/broker/xfer_provision.go` | 284 | 103 | 23 | 158 | 36% |
| `internal/storage/storage.go` | 269 | 84 | 18 | 167 | 31% |
| `internal/broker/transfer_reconcile.go` | 212 | 90 | 7 | 115 | 42% |
| `internal/broker/transfer_home.go` | 157 | 66 | 6 | 85 | 42% |
| `internal/broker/transfer_audit_forward.go` | 106 | 42 | 6 | 58 | 39% |
| `internal/xferaudit/plan.go` | 96 | 28 | 7 | 61 | 29% |
| **合计** | **5611** | **1539** | **313** | **3759** | **27%** |

方法：全文读完上述 11 个文件 + `internal/proto/messages.go:756-940`（传输 wire）+ `internal/proto/subjects.go:107-181` + `internal/jsstream/jsstream.go:86-136` + `cmd/tether/error_hints.go:73-146`。辅以 grep/awk 做常量副本、注释块分类、错误码枚举的量化。未运行任何测试。

**两条范围更正**（写给主进程，不算 finding）：

1. **`internal/broker/home_delivery.go` 与文件传输零耦合**。`grep -cin "xfer\|transfer" home_delivery.go` = **0**。它是 expose 端口的 home-broker 指令投递 reconcile pass（`port_allocations.home_broker` + `proto.HomeAssignment`），被"delivery"这个词误归到本 lane。它应该属于 cluster/expose lane。本报告仍按分派审了它（见反证 7）。
2. **`internal/storage` 不是 1317 行**。生产码 269 行（`storage.go`），其余 1048 行是三个测试文件。lane 简报的行数把测试算进去了。

---

## 传输状态机（必答问题 1）

### 结论先说：**状态机是隐式的，散在 4 种互不相干的表示里，没有 state enum、没有 transition 表。**

四种表示：

| 载体 | 位置 | 实际状态数 | 转移原语 |
|---|---|---|---|
| broker 内存 | `transferTracker.entries` + `transferEntry.finalized` | 3（不存在 / in-flight / finalized） | `put` / `claimFinalize` / `remove` |
| agent 内存 | `Agent.pushCommitCache` | 2（有 prep / 无 prep） | `rememberPushCommit` / `purgePushCommitCache` |
| ctl | 无 —— `runPush`/`runPull` 是直线控制流 | 0 | — |
| broker 磁盘 | `xferInflightRecord.Terminal` 是否为 nil × 两个目录 | 5（见下） | `writeLedgerRecord` / `consumeXferLedgerRow` |

唯一被显式建模的"转移表"是 `xfer_inflight.go:79-92` 的 5 行 ASCII 注释表格：

```
primary        outbox        means                                  next pass
-----------    ----------    ------------------------------------   ----------------------------
start          -             in flight / crashed pre-terminal        synthesize after timeout+slack
terminal       -             terminal staged in place                replay exact row, consume both
start          terminal      primary unwritable when decided         replay OUTBOX row (pass 1)
-              terminal      as above, start row already retired     replay the outbox row
terminal       terminal      two terminal paths raced                pass 1 replays once, consumes both
```

**这张表没有任何一个函数是它的执行者**——它的执行分散在 `replayStagedTerminal` + `finalizeStrandedXfers` 的两趟扫描 + `consumeXferLedgerRow` 的顺序约束里。这是本 lane 认知负担最高的一处（见 F7）。

### Push 状态机（两 tier）

```
                                    ┌─── tier A ───────────────────────────────────┐
ctl: stat + caps probe + chooseTier │ read+SHA(inline)                             │
        │                           │      │                                       │
        ▼                           │      ▼                                       │
   PushPrepareReq{tier} ────────────┤  broker: gate(session/member/node/home)      │
        │                           │         tracker.put() ── R7a 顺序守卫        │
        │                           │         writeXferInflight()  [仅 cluster 模式]│
        │                           │         watchdog(30s) ARM                    │
        │                           │         audit{start}                         │
        │                           │         forward ──► agent                    │
        │                           │  agent: ValidateForWrite → SHA verify        │
        │                           │         OpenForWriteAtomic → write → fsync   │
        │                           │         RenameForWriteAtomic(linkat/renameat)│
        │                           │         reply OK  +  pub ev.transfer.complete│
        │                           └──────────────────────────────────────────────┘
        │
        └─── tier B ────────────────────────────────────────────────────────────────
             ctl: SHA 全文件（整读一遍）
             PushPrepareReq{tier:b} ──► broker: provisionXferBucket(sizing+3 次重试)
                                              tracker.put / ledger / watchdog(5m) ARM
                                              audit{start} / forward
                                        agent: ValidateForWrite → rememberPushCommit(TTL 6m)
                                               reply OK（此时字节还没动）
             ctl: js.ObjectStore.Put(bucket=xfer-<sid>, key=transferID)   ◄── 整读第二遍
             ctl: SubscribeSync(ev.transfer.<id>.*) + Flush(2s)
             ctl: TransferCommitReq ──► broker（tracker 查表 + gate）──► agent
                                        agent: reply OK 后**异步** goroutine：
                                               ObjectStore.Get(5m ctx) → LimitedReader → tee sha
                                               → size 校验 → sha 校验 → RenameForWriteAtomic
                                               → pub ev.transfer.{complete,failed}
             broker: handleEvTransfer → claimFinalize → 终态处置
             ctl: NextMsg(ev) → 打印 OK / 报错；收不到就打印 "commit acked"
```

### Pull 状态机

```
ctl: caps probe → tierAInlineCeiling
PullPrepareReq ──► broker: gate → **无条件** provisionXferBucket（即使最终走 tier A）
                          tracker.put(tier="b" 乐观) / ledger / watchdog(5m)
                          audit{start, tier:"?"} / forward
                   agent: ValidateForRead → OpenForReadAtomic(openat pin + dev/ino)
                          size <= maxInline ?
                            ├─ 是 → 读入内存（LimitReader maxInline+1）→ reply{tier:a, inline}
                            └─ 否 → ObjectStore.Put(5m ctx) → reply{tier:b, bucket, key}
ctl: tier A → 校验 size/sha → writeLocalAtomic(tmp+link/rename)
     tier B → js Get → io.Copy(MultiWriter(f,h)) → size/sha → commitLocalTemp
ctl: sendFinalize{complete|failed} ──► broker handleFinalizeReq
                                        → 4 层校验 → claimFinalize → 终态处置
```

### 超时 / 重试 / 取消路径

| 预算 | 值 | 定义处 | 起算点 |
|---|---|---|---|
| broker tier-A 看门狗 | 30s | `transfer.go:50` | prepare 被接受时 |
| broker tier-B 看门狗 | 5m | `transfer.go:51` | prepare 被接受时（**ctl 还没开始上传**） |
| broker 桶 provisioning 预算 | 8s（+1.5s sizing） | `xfer_provision.go:35-46` | 每次 prepare |
| broker stranded slack | 60s | `xfer_inflight.go:35` | ledger 记录的 startedAt |
| broker 孤儿对象宽限 | 2m | `transfer_reconcile.go:18` | 对象 ModTime |
| broker 跨 home GC 底线 | 15m（`3*tierB`，**推导且被测试钉死**） | `transfer.go:62` | 对象 ModTime |
| agent commit Get 上下文 | 5m 硬编码 | `agent/transfer.go:186` | push-commit 到达时 |
| agent pull Put 上下文 | 5m 硬编码 | `agent/transfer.go:408` | pull prepare 到达时 |
| agent prep 缓存 TTL | 6m | `agent/transfer.go:54` | prepare 时 |
| ctl 整体超时 | 10m（可 `--timeout`） | `cmd/tether/transfer.go:71,385` | 各阶段各自起算 |
| ctl caps 探测 | 3s | `cmd/tether/transfer.go:166,436` | — |
| ctl finalize 请求 | 3s / 5s | `cmd/tether/transfer.go:472,510,591,606` | — |

**取消路径**：wire 上**没有 cancel 动词**。用户 Ctrl-C 只中断 ctl 自己的 context；broker 的 tracker entry 和 ledger 记录要等看门狗（30s/5m）才被回收，期间该 `transfer_id` 被 `transfer_id_in_flight` 占位。

**重试路径**：只有 broker 的桶 provisioning 有重试（3 次，指数退避 +25% 抖动）。**数据面零重试、零续传**——任何一次断连都是从头再来（tier B 意味着重新读整个文件算 SHA + 重新上传）。

---

## Findings

### F1（high）终态处置序列被手抄 4 遍，没有 `finalizeTransfer()` 这一个函数

**证据**

四处结构相同的 5 步序列：

- `internal/broker/transfer.go:429-443`（watchdog 到期）
- `internal/broker/transfer.go:916-936`（`handleEvTransfer`，push 收方终结）
- `internal/broker/transfer.go:948-961`（`cleanupEntry`，forward 失败）
- `internal/broker/transfer.go:1183-1205`（`handleFinalizeReq`，pull 收方终结）
- 第五个变体：`internal/broker/xfer_inflight.go:511-522`（崩溃恢复合成终态）

同一句警告注释字节相同地出现 **4 次**：

```
internal/broker/transfer.go:440:  // F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here.
internal/broker/transfer.go:934:  // F1: ...（同上）
internal/broker/transfer.go:961:  // F1: ...（同上）
internal/broker/transfer.go:1201: // F1: ...（同上）
```

这句注释本身就是证据：外审 F1 那次修复是被迫在四个位置逐一手抄的，作者只能靠留下四份相同注释来防止后来者在某一份里退回旧写法。

四份副本已经出现分叉：watchdog 那份**没有** `entry.cancel()`（因为它自己就是 watchdog，正确），但这个"为什么少一步"在代码里看不出来。

**为什么是债 / 阻碍什么演进**

- 加任何新终态都要改 5 处：`tether push --cancel` 动词、"部分成功"（大文件断点）、看门狗按进度续期（见 F2）、传输配额超限的主动终止。
- `emitTerminalTransferAudit`（`transfer.go:499-545`）已经是"唯一写终态审计的地方"，但**对象删除 / watchdog 取消 / tracker 摘除**这三步没被一起收进去，所以"终态"这个概念只完成了 1/4 的收敛。

**建议**

抽 `func (b *Broker) finalizeTransfer(entry *transferEntry, rec schema.AuditTransfer)`：内部做 `emitTerminalTransferAudit` → `deleteXferObject` → `cancel()` → `remove()`；四个调用点只负责**构造 rec** 和决定要不要回复。watchdog 那份传 `cancel=nil` 或干脆复用（重复 cancel 是幂等的）。

**量化**：净减约 45 行；把 4 份 5 步序列压成 4 次 1 行调用。
**风险**：low（纯内聚重构，不碰 wire、不碰 ledger 语义；`claimFinalize` 的 validate-then-claim 契约不变）。

---

### F2（high）传输没有"预算"这个抽象：2 GiB 上限和 5 分钟看门狗互不推导，broker 会主动删掉还在传的对象

**证据**

- `internal/broker/transfer.go:51` — `transferTimeoutTierB = 5 * time.Minute`
- `internal/broker/transfer.go:67` — `transferMaxBytes = 2 * 1024 * 1024 * 1024`（2 GiB，"全局硬上限"）
- `internal/broker/transfer.go:654` — 看门狗在 `handlePushReq` **转发给 agent 之前**就 ARM 了
- `cmd/tether/transfer.go:245-291` — ctl 的 `ObjectStore.Put` 发生在 prepare **之后**
- `internal/broker/transfer.go:438` — 看门狗到期时执行 `deleteXferObject(...)`，**主动删除正在传输的对象**
- `internal/proto/messages.go` 全文 — 传输 wire 里**没有任何 progress / heartbeat 消息**（grep 结果 0）

推论：tier-B push 的 5 分钟预算要覆盖 `ctl 上传 2 GiB + commit + agent 下载 2 GiB + 落盘`。在 100 Mbit/s 的对称链路上，单向 2 GiB 就要约 3 分钟，双向必然超预算。看门狗到期 → `claimFinalize` 成功 → 写 `failed / agent_no_responders` 审计（**归因错误**：agent 好好的，只是用户还在上传）→ 删除对象 → 摘除 tracker。之后 agent 真把文件落盘并发 `ev.transfer.complete`，`handleEvTransfer` 因 `transfers.get()` 返回 nil 而静默丢弃。**结果：文件可能真的落地了，审计说 failed。**

**为什么是债 / 阻碍什么演进**

- 想提高文件上限、支持慢链路 agent、或加断点续传，都必须同时改三个包里 6 个常量并重新推理它们的关系——而这个关系**今天没有任何地方写下来，也没有任何测试钉住**。
- 反证这套纪律是存在的：`transfer.go:62` 的 `xferCrossHomeReapAge = 3 * transferTimeoutTierB` 就是推导出来的，并被 `reconcile_passes_test.go:1119-1121` 显式钉死："the cross-home GC floor's cross-node/clock-skew margin depends on this relation"。**同一个仓库知道怎么做常量推导，只是没用在最关键的那一对上。**
- 缺失的抽象是一个 `transferBudget`：由 size × 期望吞吐推导 deadline，或者让收/发方周期性发 progress 来续期看门狗（后者是 additive wire，不动 `ProtoVersion`）。

**建议**

1. 短期低风险：把 tier-B 看门狗改成 `max(transferTimeoutTierB, size/minThroughput)`，并加一条测试钉住"看门狗预算 ≥ 在 X Mbit/s 下传完 transferMaxBytes 所需时间"。
2. 中期：加 `ev.transfer.<id>.progress`（additive，`Code` 一样是 free-form 扩展的先例见 `xfer_provision.go:56-65`），看门狗按最后进度续期；顺带解决"看门狗归因错误"（`agent_no_responders` 该改成 `no_progress`）。
3. 把三个包的 tier 常量提到 `internal/proto`（它已经是 wire SSOT）。

**量化**：约 +40 / −20 行；消除 6 个常量副本中的 4 个。
**风险**：medium（改看门狗行为会影响 `test/cli_e2e/transfer_test.go` 与 d8 的时序断言；progress 消息是 additive，不破坏兼容、不需要重装）。

---

### F3（high）错误码 taxonomy 无 SSOT：43 个码里 29 个未归类退出码，被文档指示为"可重试"

**证据**

- 传输路径实际使用的 `Code` 字符串共 **43 个**（grep 全仓生产码枚举），全部是**裸字符串字面量**，散在 `internal/broker`、`internal/agent`、`cmd/tether` 三个包 + `internal/proto/messages.go` 的文档注释里。只有最新加的 3 个有具名常量：`xfer_provision.go:65,78,84`（`codeXferJSNotReady` / `codeXferStoreTooSmall` / `codeXferBrokerRestarting`）。
- 退出码分类表 `cmd/tether/error_hints.go:78-133` 只覆盖了其中 **14 个**；`brokerCodeExitClass`（`:140-146`）默认落 `exitInternal = 70`。
- 落到 70 的传输码包括：`dst_exists`、`too_large`、`path_outside_roots`、`transfer_disabled`、`not_a_regular_file`、`path_parent_missing`、`path_not_absolute`、`path_not_found`、`sha_mismatch`、`size_mismatch`、`tier_invalid`、`object_get_failed`、`object_put_failed`、`bucket_unknown`、`transfer_unknown`、`verb_mismatch`、`path_race`、`io_error` ……共 **29 个**。
- `docs/usage.md:1542`："**健壮重试规则**：把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态。"

于是 `tether push a.bin node:/existing-file`（忘了 `--force`）退 70，文档指示监控**退避重试**——每次重试都要重算一遍全文件 SHA-256，永远不会成功。

**这个缺陷类已经被识别并修过一次，但只修了实例没修类。** `xfer_provision.go:67-78` 的注释原文：

> 「docs/usage.md §9.13 tells automation to treat 70 as RETRYABLE with backoff and only 64/77 as terminal, while bucket_create_failed maps to 70 … A monitor following the documented rule would retry a too-small disk forever, each attempt paying a full-file SHA-256.」

这段推理逐字适用于上面 29 个码中至少 8 个（`dst_exists` / `too_large` / `path_outside_roots` / `transfer_disabled` / `not_a_regular_file` / `path_parent_missing` / `path_not_absolute` / `path_not_found` 都是"人必须动手，重试无用"= 64）。当时只给 `tier_b_store_too_small` 和 `jetstream_unavailable` 补了行。

**为什么是债 / 阻碍什么演进**

- 每加一个错误码都要人工记得去**另一个包**补一行，没有编译期约束。仓库自己承认这一点：`error_hints.go:129-131`「there is no compile-time link across the two packages, so TestDataplaneNotConvergedCodeIsWireStable pins it from this side」——即"靠一个测试盯住一个字符串"，而这套办法只被用在 1 个码上，剩下 42 个没有。
- 演进后果具体化：加 `--json` 输出（`usage.md:1548` 说 transfer 的 `--json` 在 B2.1 补）时，机器可读的 `code` 字段和退出码会给出**互相矛盾**的可重试性判断。

**建议**

在 `internal/proto` 建一个 `transfer_codes.go`：具名常量 + `func CodeClass(code string) Class`（terminal / transient / usage / perm）。broker 与 agent 用常量而非字面量；`cmd/tether` 的表从这里读，`brokerCodeExitClasses` 退化为"覆盖 proto 未定义的非传输码"。补齐上面 8 个明显的 usage 码。

**量化**：+50 行（新文件）/ −0 行；把 29 个未分类降到 0，把 43 个裸字符串收敛到 1 处定义。
**风险**：low（只改退出码与常量引用，wire 上的字符串值一字不动，不影响协议兼容；`drills/61-transfer-edges` grep 的 `code=<X>` 文案由 `transferRefusalErr` 保留，不受影响）。

---

### F4（medium）`cmd/tether/transfer.go` 880 行里约 570 行是协议客户端，不是 CLI

**证据**

- 26 个函数中 **17 个完全不依赖 cobra**（`grep -n "^func " | grep -v cobra`）：`sendFinalize` / `failAndFinalize` / `writeLocalAtomic` / `commitLocalTemp` / `parseRemoteSpec` / `probeCapsCtx` / `probeCapsClassified` / `classifyCapsResp` / `tierAInlineCeiling` / `chooseTier` / `transferRefusalErr` / `newTransferID` / `hexSHA256` / `firstErr` / `pullPrepareFailureNeedsFinalize` / `capsProbe.warning` / `indexByte`，共约 296 行。
- 另有 4 个（`pushTierA` `:186-222`、`pushTierB` `:224-350`、`finishPullTierA` `:483-514`、`finishPullTierB` `:516-595`，共 276 行）只通过 `cmd.Context()` / `cmd.OutOrStdout()` 用到 cobra——换成 `context.Context + io.Writer` 即可脱钩。
- 真正的 CLI 部分只有约 180 行：`newPushCmd`/`newPullCmd` 的 cobra 定义 + help 长文本 + `completePushTarget`。
- **作者已经感到这个压力，但用错了解法**：`cmd/tether/transfer.go:780-784` 明确写「`classifyCapsResp` is the pure half of the probe, **split out so the three-way classification is directly testable**」——把函数拆细放在 `package main` 里，而不是下沉到 `internal/`。结果是 `cmd/tether/g67_caps_test.go`（230 行）在测 `package main` 的内部函数。

**为什么是债 / 阻碍什么演进**

任何第二个传输客户端都**拿不到这 570 行的任何一行**，只能第三次实现"选 tier / 分块 / 校验 / finalize"：

- agent↔agent 直接拷贝（`tether cp node-a:/x node-b:/y`）
- daemon 内部触发的传输（实验数据自动回收——这正是 `experiment-lifecycle` skill 里"raw data pull-back"要做的事）
- 目录 / 递归传输（`push -r`）
- `--json` 结构化输出（`usage.md:1548` 已登记待补）

**建议**

新建 `internal/xfer`，把 client 半边搬进去：`TierChooser`（`chooseTier` + `tierAInlineCeiling` + `capsProbe`）、`PushClient` / `PullClient`（4 个 tier 函数，签名换成 `(ctx, nc, io.Writer)`）、`VerifiedCopy`、`Finalizer`。`cmd/tether/transfer.go` 只留 cobra 壳 + 打印，约 200 行。

**量化**：cmd 减约 570 行，internal 增约 500 行（净 −70，但**主要收益是可复用 + 可测**，不是减行）。
**风险**：medium（纯搬移，零 wire 变化；但要同步搬 `g67_caps_test.go` 与 `cli_ux_test.go` 里相关断言，改动面 ~1200 行测试）。

---

### F5（medium）三个包各写一份 tier 常量与小工具，6 个 wire 耦合常量存在三副本

**证据**

```
8 MiB tier-A 上限（3 副本）
  internal/broker/transfer.go:52   transferTierAMaxBytes = 8 * 1024 * 1024
  internal/agent/transfer.go:47    agentTierAMaxBytes    = 8 * 1024 * 1024
  cmd/tether/transfer.go:682       cliTierAMaxBytes      = 8 * 1024 * 1024

2 GiB 硬上限（3 副本）
  internal/broker/transfer.go:67   transferMaxBytes      = 2 * 1024 * 1024 * 1024
  internal/agent/transfer.go:51    agentTransferMaxBytes = 2 * 1024 * 1024 * 1024
  cmd/tether/transfer.go:689       cliMaxBytes           = 2 * 1024 * 1024 * 1024

hexSHA256（2 份生产 + 2 份测试，函数体逐字相同）
  internal/agent/transfer.go:534 / cmd/tether/transfer.go:877
  test/security/transfer_authcallout_test.go:606 (hexSHA256ForTransferTest)
  test/cli_e2e/transfer_test.go:507 (hexSHA256ForTest)

firstErr / firstNonNilErr（3 份，函数体逐字相同、只是名字不同）
  cmd/tether/transfer.go:669 / internal/agent/transfer.go:545 / internal/cluster/membership_ops.go:220
```

**为什么是债 / 阻碍什么演进**

调 tier 边界必须同时改三处，且**没有任何编译期约束**。改漏一处的表现不是编译失败，而是"某一端静默拒绝"——例如把 broker 的 8 MiB 提到 16 MiB 而忘了 agent，则所有 8–16 MiB 的 tier-A push 会被 agent 端拒绝，broker 侧却已经写了 `audit{start}` 并 ARM 了看门狗。这类 bug 只会在集成测试里露头。

**建议**

常量提到 `internal/proto`（`TierAMaxBytes` / `TransferMaxBytes`）——它已经是 wire SSOT，三端 import 即可；helper 进 `internal/xfer`（配合 F4 一次性做完）。

**量化**：净减约 60 行；消除 6 个副本中的 4 个 + 4 份 `hexSHA256` 里的 3 份。
**风险**：low（常量值不变，纯引用替换）。

---

### F6（medium）`xferEventsHistoryReserve` 硬抄了 jsstream 的常量，并把"每 session 1 GiB"当成全局 1 GiB

**证据**

- `internal/broker/transfer.go:74` — `xferEventsHistoryReserve = 2 * 1024 * 1024 * 1024 // events(1 GiB)+history(1 GiB) — the other JS reservations`
- `internal/jsstream/jsstream.go:97` — events 流 `MaxBytes: 1 << 30`（**全局一条流**）
- `internal/jsstream/jsstream.go:121` — `historyMaxBytesPerSession = 1 << 30`（**每 session 一条流**，常量名自己写着 PerSession）

`xferMaxBytesForCeiling`（`transfer.go:240-267`）用 `ceiling - 2 GiB` 算可用空间。当 broker 有 N 个 session 时，真实预留是 `1 + N` GiB，公式却恒定减 2 GiB。N=5 时低估 4 GiB。

两个包之间**没有 import 关系**，只有一句注释在同步语义。

**为什么是债 / 阻碍什么演进**

- 会话数一多，磁盘预留系统性低估 → tier-B bucket 被算得偏大 → 正是小盘 broker 上 tier-B 反复出问题的那一族（项目记忆里 racknerd 的 tier-B 传输故障）。注意这**不是**同一个 bug（那个是 `avail < floor` 直接拒绝），而是相邻的第二个。
- 改 `internal/jsstream` 里任何一个流的 MaxBytes，**不会**传导到 xfer sizing——sizing 会静默偏移，且没有任何测试钉住这个关系。

**建议**

`internal/jsstream` 导出 `func ReservedBytes(nSessions int) int64`，`xferBucketMaxBytes` 调用它并传入实际会话数（`session.Count(b.cfg.DB)`）。加一条测试钉住"reserve 必须 ≥ events + N×history"。

**量化**：+20 / −5 行。
**风险**：low（只影响 bucket sizing，不动 wire；但会让小盘 broker 上更多场景落到 `tier_b_store_too_small`——那是**正确**的行为，不是回归）。

---

### F7（medium）`xfer_inflight.go` 的双目录 outbox 是评审逐轮升级堆出的偶然复杂度，且在默认部署形态下整体 inert

**证据**

- 归属于"兄弟目录 fallback"的增量代码约 **180 行 / 607 行**：
  - `xfer_inflight.go:71-112` — 42 行优先级表格注释 + `xferTerminalOutboxDir`
  - `xfer_inflight.go:201-219` — `stageXferInflightTerminal` 的 fallback 分支
  - `xfer_inflight.go:242-299` — `consumeXferLedgerRow` + `removeLedgerRowDurably` 的"主目录优先、失败则保留 outbox"顺序不变量
  - `xfer_inflight.go:450-495` — PASS 1 / `outboxOwned` / `obErr` 的跨趟 ownership 逻辑
- 它覆盖的场景很窄：`writeLedgerRecord` 已经 `os.MkdirAll`，所以"目录被删"能自愈；剩下的只有"`xfer-inflight` 被替换成非目录 / 被 chmod 掉"，而**同父目录下的兄弟目录**在这些情形下往往同样受影响。
- 整个文件在**单 broker 部署下完全 inert**：`xfer_inflight.go:64-69` 的 `xferInflightDir()` 在 `ClusterDataDir == ""` 时返回 `""`；而 `cmd/tether/serve.go:308` 里 `--cluster-data-dir` 默认为 `""`。即：`docs/broker-ops.md` 描述的默认单 broker 安装下，broker 崩溃仍会留下永久悬空的 `audit{start}` 行——正是 #57 要修的缺陷，在更常见的部署形态里按设计被接受。
- 代码自己承认这一点：`:30` 「Cluster mode only (ClusterDataDir set); single-broker audit publish is best-effort by design.」

评审升级的痕迹在注释里连续可见：`external RE-REVIEW F1` → `ROUND-4 RE-REVIEW` → `ROUND-5 R5-F1` → `R5-F2` → `R6-F1` → `round-3 R3-F1/R3-F2`。每一轮都在前一轮的机制上再加一层保险。

**为什么是债 / 阻碍什么演进**

- 任何触碰终态路径的改动（F1 的重构、F2 的看门狗续期、加 cancel 动词）都必须**重新证明那张 5 行优先级表仍然成立**——而这张表只存在于注释里，没有任何函数是它的执行者，没有表驱动测试逐行覆盖 5 种状态组合。这是本 lane 单位行数改动成本最高的一块。
- `finalizeStrandedXfers` 的两趟扫描 + `outboxOwned` 跨趟传递，使得"某一趟扫描失败"要分三种情况处理（`obErr` / `prErr` / 两者都失败），这三种情况的正确性论证也只在注释里。

**建议**

**不建议直接删**——它在集群模式下确实关闭了一个真实窗口。但建议两步：

1. **把优先级表变成代码**：抽 `func ledgerRowDisposition(primary, outbox *xferInflightRecord, live bool, now time.Time) disposition`（纯函数），用表驱动测试逐行覆盖那 5 种组合。当前对它的验证散在 `xfer_inflight_test.go`（410 行）+ `r16_g67_g69_external_rereview_test.go`（616 行）里，两个文件加起来只有 17 个 `func Test`。
2. **评估降级 fallback**：把 outbox 从"兄弟目录"降级为"同目录不同前缀的文件名"（`<hash>.terminal.json`），可以直接去掉 PASS 1、`outboxOwned`、`obErr` 三段跨趟逻辑与那张表的后 3 行。

**量化**：方案 1 约 +30 / −20 行但显著降低改动成本；方案 2 净减 80–120 行。
**风险**：high（碰的是崩溃恢复正确性，且 `r16_g67_g69_external_rereview_test.go` 616 行全部钉在当前形状上）。

---

### F8（low）死代码与只被测试读的字段

**证据**

| 符号 | 位置 | 情况 |
|---|---|---|
| `IsPathValidationError` | `internal/agent/transfer.go:569-574` | **零调用方**（生产 + 测试全仓 grep 只命中定义本身）。exported，所以 `unused` linter 不报。 |
| `TransferReqID` | `internal/xferaudit/plan.go:29-33` | 注释自称 "legacy coarse key"，**零调用方**。且 `:49` 的文档注释还写着「ReqID = TransferReqID(rec)」，而代码用的是 `TransferRecordReqID` —— **文档与实现不符**。 |
| `xferProvisionErr.SizingMs` / `.CreateMs` | `internal/broker/xfer_provision.go:139-140` | 4 个写入点（`:181,229,247,259`），**零读取点**。注释说 "measure, never assume — these two settle which round trip stalls"，但没人读它们，日志里另有 `sizing_ms`/`create_ms` 键。 |
| `ValidatedPath.AllowRoot` | `internal/agent/transfer.go:665-669` | 注释自承「it has zero readers today」，只有 `transfer_test.go:146,216` 在断言它。 |

**为什么是债**

单个都不痛，合起来是"审计留下的观测字段没有消费者"的模式——它们让读者以为存在某个观测/归因链路，实际没有。`TransferReqID` 的过时文档更会误导下一个人以为 dedup key 是粗粒度的（实际是内容寻址的细粒度 key，这是 #57 整套设计的基石）。

**建议**：删 `IsPathValidationError`、`TransferReqID`（+ 修正 `:49` 文档）、`SizingMs`/`CreateMs`；`AllowRoot` 保留但把注释从"zero readers"改成"reserved for audit attribution"或干脆接进 `pubAuditTransfer`。

**量化**：净减约 25 行。**风险**：low。

---

## 反证：做得好的地方

1. **`internal/storage` 不是过度抽象——它是"抽象不足但诚实"的正面样本。** 269 行生产码，**没有 interface、没有 factory、没有 registry**，`Open` / `OpenWAL` / `OpenReadOnly` 直接返回 `*sql.DB`。三个变体各自对应一个真实需求（P0–P13 冻结 DB / cluster FSM 的 WAL+synchronous=FULL / 快照的只读源），`storage.go:22-44` 用 25 行注释讲清了 `SetMaxOpenConns(1)` 为什么 load-bearing、哪些 allocator 依赖它。lane 简报里"为一个实现写了接口+工厂+注册表"的假设在这里**被证伪**。唯一可挑的是 3 个 DSN 拼接函数有约 10 行重复——不值得动。

2. **`xfer_provision.go` 是全 lane 最干净的一块。** 单一职责（provisioning 的超时/重试/分类）、typed error（`xferTooLarge` 刻意做成**值**而不是 error，`:126-129` 解释了为什么："so it cannot silently degrade into a provisioning failure"）、纯函数 `xferMaxBytesForCeiling` 可独立测试、每个常量都带**实测依据**而不是拍脑袋（`:13-29`：create 本身 ~57ms、`.push.req` 走 `isBroadcastClusterSubject` 因而是 head-of-line blocking、所以预算刻意收紧到 ~9.5s）。这是"注释解释决策而不是解释历史"的样板。

3. **`agent/transfer.go:554-1114` 的 560 行路径加固是真正的本质复杂度，不该被"简化"。** `openDirNoSymlinks` 逐段 `O_NOFOLLOW` 打开父目录 → `Fstatat(AT_SYMLINK_NOFOLLOW)` 预检 → `Openat(O_NOFOLLOW)` → `Fstat` 复核 dev/ino → `dirFDStillNamesPath` 再验父目录未被换 → 提交时用 `Linkat`（无 `--force`，原子 create-if-absent）或 `Renameat`。每一步都对应一个具体攻击（父目录换成 symlink、leaf 在 lstat 与 open 之间被换、check-then-rename 竞态覆盖）。`transferMode`（`:681-713`）的三态 open/narrow/disabled 与 `resolveTransferMode` 坚持从**原始配置**而非 canonical 列表长度推导（`:700-704`：一个条目全部失效的 narrow 配置必须留在 narrow 并拒绝一切，绝不塌回 open），这是安全设计里最容易写错的一处，这里写对了并解释清楚了。

4. **broker/agent "同一逻辑写两遍"的假设不成立。** 逐行对读后：broker 侧做的是准入门（`transferGate` 会话/成员/节点在线 + `transferHomeGate` 归属路由）、in-flight 台账、审计三元组、JS 桶生命周期；agent 侧做的是路径校验 + 原子落盘 + SHA。两者**没有一个函数是对方的翻版**。真正重复的只有 F5 里那些常量和 4 行的小工具。这一点值得写进结论，因为它推翻了 lane 的原始假设。

5. **`transferTracker` 的 validate-then-claim 契约是并发正确性收敛的正面例子。** `transfer.go:149-178` 把"谁有权写终态"收敛成一个原语 `claimFinalize`，四个终态路径共享它而不是各自加锁；注释还写明了为什么前一版的 `markFinalized + 失败后 unclaim` 是竞态的（unclaim 发生在 `tracker.mu` 之外）。`put()` 拒绝覆盖同 id 也有明确理由（`:123-127`：旧 entry 的 watchdog 还挂在这个 id 上，覆盖会让它在中途收割替身）。

6. **常量推导 + 测试钉死的纪律在这个仓库是存在的。** `transfer.go:62` 的 `xferCrossHomeReapAge = 3 * transferTimeoutTierB` 不是字面量而是推导式，并被 `reconcile_passes_test.go:1119-1121` 钉死关系而非钉死数值。这正好说明 F2 不是"团队不懂"，而是"这套纪律没被用在最该用的那一对上"。

7. **`home_delivery.go`（虽然被误归到本 lane）自身结构良好。** 严格的 reconcile pass 三段式（expected = `homeForRegister`——刻意复用 register 回复的**同一个** builder，所以不可能漂移；actual = agent 确认 applied 的 epoch；idempotent apply = `applyHomeDirectives`），收敛时**零副作用**。三个 prune 函数（`pruneHomeApplied` / `pruneHomeOutstanding` / `pruneHomeAttempts`）各自解释了为什么用 live-set 而不是 TTL（`:596-604`：TTL 会把长时间掉线但活着的节点从 max backoff 里踢出来，反而破坏 storm control）。per-directive 单次令牌的设计（`:347-363`）把"令牌必然在共享 `_INBOX` 上泄露"这一事实变成了无害的：泄露时它已被消费且只指向一个已收敛的端口。

8. **`internal/xferaudit` 只有 96 行，边界正确。** 它把 `schema.AuditTransfer` 挡在 `internal/cluster` FSM 核心之外（`:6-9` 说明了为什么：FSM 核心不该认识审计 schema），`ReplayTransferAudit` 只读 `cmd.Aux`、不查 DB、不发请求——两个 leader 重放同样字节得同样记录。这是"叶子包只做一件事"的正面样本。

---

## 本质 vs 偶然复杂度拆解

**总量：5,611 行（含注释）/ 3,759 行纯代码。**

### 本质复杂度（问题域强加，删不掉）— 约 3,200 行 / 57%

| 部分 | 行数 | 为什么是本质的 |
|---|---:|---|
| agent 侧路径安全（openat pin / O_NOFOLLOW / dev-ino 复核 / linkat 原子提交 / 三态 mode） | ~560 | agent 主机上可能有敌意的非成员本地进程；每一步对应一个具体攻击。这与"成员有没有权限"正交（成员本来就有 run/exec 全权）。 |
| 双 tier wire + 4 个 verb + 收方终结不变量 | ~900 | NAT 后 agent 只能反连，broker 又必须不在数据面上——"审计由收字节的那一方触发"是这个约束的直接后果，不是设计花哨。 |
| JS 对象存储生命周期（provision / 磁盘感知 sizing / replica raise / 孤儿回收 / 跨 home GC） | ~700 | JetStream 的对象存储没有"per-transfer 桶 + 自动过期"这种原语（`subjects.go:134-148` 记录了为什么不能用 per-transfer 桶：ACL 里的字面 `*` 不是通配符）。回收必须自己做。 |
| broker 侧准入门 + home 路由 | ~250 | 集群模式下每个 broker 都收到广播 subject，没有 home gate 就是 N 路扇出。 |
| 集群崩溃恢复核心（#57 单目录版） | ~250 | "审计恰好一个终态"在 broker 会崩的前提下确实需要一个 outbox 语义。 |
| CLI 壳 + help + completion | ~180 | 用户界面。 |
| `internal/storage` + `internal/xferaudit` | ~365 | 迁移 runner + 三种 DB 打开姿势 + 一个 re-derivable op 的渲染/重放。 |

### 偶然复杂度（实现方式造成，可消除）— 约 1,000–1,200 行 / 18–21%

| 部分 | 行数 | 归属 finding |
|---|---:|---|
| 终态处置四抄 | ~45 | F1 |
| 三包常量 / helper 重复 | ~60 | F5 |
| 双目录 outbox 增量（表格 + 两趟扫描 + 跨趟 ownership + 顺序不变量） | ~180 | F7 |
| 评审考古注释（"以前的版本怎么错的"，可迁 `docs/reviews/`） | ~300–400 | 见下 |
| 死代码 / 无消费者的观测字段 | ~25 | F8 |
| reserve 常量抄写 + per-session 语义错配 | ~15 | F6 |
| 错误码分类表要人工同步（不是行数问题，是**缺失**的 50 行 SSOT） | −50 | F3 |

### 职责错位（不减行，但让 570 行无法复用）

`cmd/tether/transfer.go` 的 570 行协议客户端逻辑（F4）。它既不是本质冗余也不是偶然冗余——它是**放错了地方**。修它不会让代码变短，会让第二个客户端成为可能。

### 关于那 27% 的注释

9 个主文件共 1,427 行注释，其中 **672 行**落在含评审标记的注释块里（`ROUND-4 RE-REVIEW` / `external review F1` / `R5-F1` / `R6-F1` / `G67 m14` / `audit shard P11 F2` / `内部 review B1`…）。这**不全是浪费**：

- **该留的**（约一半）：解释"为什么这个看起来多余的分支不能删"的部分。例如 `messages.go:820-837` 明确写「These two are the TRANSIENT/PERMANENT split … **do not collapse them again**」；`transfer.go:665-679` 的 R7a 顺序守卫把一个"靠约定维持"的前提变成了运行时前置条件。删掉这些注释，下一个人一定会把它们改回去。
- **该走的**（约一半）：叙述"上一版怎么写的、为什么错、评审第几轮发现的"的部分。这些属于 `docs/reviews/`——那里已经有 66,836 行评审记录了。它们留在源码里的代价是：读者必须先把三轮评审的历史读一遍才敢改一行。

**这也是对用户元问题的直接回答之一**：这套「多专家草拟 → 实现 → 多专家内审 → 用户外审」流程确实在制造一类结构债，但形态不是"屎山"，而是三种更细的东西——(a) 修复被应用到**实例**而不是**类**（F3 最典型：同一段推理写在 `xfer_provision.go:67-78`，只给 3 个码补了行，剩下 29 个照旧）；(b) 每轮评审在前一轮的机制上再加一层保险，而没人有权回头问"上一层还需要吗"（F7）；(c) 修复被迫手抄 N 份时，注释成了唯一的防退化手段（F1 的四行相同注释）。

---

## 附：本 lane 未发现的问题（明确说"没有"）

- **没有**为一个实现造的 interface + factory + registry（`internal/storage` 被证伪）。
- **没有**broker/agent 的状态机重复实现。
- **没有**上帝函数：本 lane 最长的函数是 `finalizeStrandedXfers`（103 行）和 `handlePushReq`（146 行），后者是一条直线的校验管线，抽象层次单一。
- **没有**循环依赖：`proto ← broker/agent/cmd`、`xferaudit → cluster+schema`、`jsstream` 被 broker 单向依赖，方向都对。
- **没有**遗留的 TODO/FIXME（全仓仅 1 处，不在本 lane）。
