# smalldisk — 让小盘 broker 上的 tier-B 传输可用

> 叶子增量（CLAUDE.md §2），不在线性 P 序内。
> 状态：**已实现并完成独立外审**（最终裁决与 reviewer 修复见
> `docs/reviews/smalldisk-external-review.md`；本文保留设计推导与历史裁决）。

日期：2026-08-06 · 基线：`71e4943`

## 0. 事实（现网实测，非推断）

broker `racknerd`（唯一 broker，force-single N=1，19G 盘）。

### 症状

```
tether push <4.4MB> timan107:/tmp/x --ack-alerts
→ tier=b, 4387000 bytes
→ error: push (tier B) refused at prepare: code=bucket_create_failed
         create_bucket: nats API 500 err_code=10047 insufficient storage resources available
```

### JetStream 账目（`/jsz`）

| 项 | 值 |
|---|---|
| `max_storage` | 10.33 GiB（**nats 按空闲磁盘自动推导**；`nats.conf` 没写 `max_file_store`）|
| `reserved_storage` | **10.00 GiB** |
| `storage`（实际占用）| 47 MiB |

预留去向：

| 流 | MaxBytes | 实际 | 来源 |
|---|---|---|---|
| `events` | 1 GiB | 9.5 MiB | `internal/jsstream/jsstream.go:97`（全局一个）|
| `history-lab` | 1 GiB | 38 MiB | `internal/jsstream/jsstream.go:121`（**每会话一个**）|
| `OBJ_xfer-lab` | **8 GiB** | **0 字节**，2026-07-04 建、从未用过 | legacy cap |

### nats-server 的判定公式（源码核对，v2.14.0）

10047 = **`JSStorageResourcesExceededErr`**（`jetstream_errors_generated.go:844`），
是**账户/服务器存储额度**检查，不是 peer 选择用的 `JSInsufficientResourcesErr`。
出处 `jetstream.go:2469-2477` `checkBytesLimits`：

```go
totalBytes = addBytes + maxBytesOffset          // addBytes = 新 config 的 MaxBytes
case FileStorage:
    // ① 账户级
    if selectedLimits.MaxStore >= 0 &&
       (currentRes > selectedLimits.MaxStore || totalBytes > selectedLimits.MaxStore-currentRes) {
        return NewJSStorageResourcesExceededError()
    }
    // ② 服务器级
    if checkServer && (js.storeReserved > js.config.MaxStore ||
                       totalBytes > js.config.MaxStore-js.storeReserved) {
        return NewJSStorageResourcesExceededError()
    }
```

**两级的口径不同，这是整件事的关键：**

`currentRes = jsa.tieredReservation(tier, cfg)`，而它**显式排除正在改的这个流**
（`jetstream_api.go:1356`：`// Don't count the stream toward the limit if it already exists.`）。
所以①对我们是**通过**的（扣掉自己那 8 GiB 后 `currentRes=2 GiB`，`6.25 < 10.33−2`）。

而②的 `js.storeReserved` 是**服务器全局预留，不排除自己**。
racknerd：`js.storeReserved = 10 GiB`（含我们自己的桶），`MaxStore = 10.33 GiB`
⇒ 可用 `0.33 GiB`，索要 `6.25 GiB` ⇒ **必然 10047**。

**推论（设计前提，源码已核）**：**创建路径缩不了自己的桶，更新路径可以。**

`stream.go:2341-2380` 的更新路径：

```go
cfg.MaxBytes = maxBytesDiff                        // 只把「差值」计入额度
reserved = jsa.tieredReservation(tier, &cfg)       // 排除本流
if old.MaxBytes > 0 { reserved += old.MaxBytes }   // 再把旧值显式加回
checkAllLimits(&selected, &cfg, reserved, maxBytesOffset)
```

缩容时差值被夹成 1 字节，于是账户级与服务器级检查**都通过**。代入 racknerd：
`reserved = 2 GiB(others) + 8 GiB(old) = 10 GiB`，`totalBytes = 1`，
`1 > 10.33 − 10 = 0.33 GiB`？否 ⇒ **通过**。

⇒ **tether 可以用 `UpdateObjectStore` 把现存的 8 GiB 桶就地收敛到目标值，无需任何运维介入。**
这让修复能**自愈存量 broker**（racknerd 就是），而不只是让新装的 broker 正确。

#### 收敛不是可选项：只修 sizing 救不了存量 broker

服务器级检查用的是 `js.storeReserved`，**它包含我们自己那个桶**。代入 racknerd
（`MaxStore=10.33 GiB, storeReserved=10.0 GiB ⇒ 服务器级可用 0.33 GiB`）：

| create 索要 | 结果 |
|---|---|
| 6.25 GiB（今天的算法）| **10047 拒绝** |
| **2.00 GiB（sizing 修好之后）** | **仍然 10047 拒绝** |
| 0.25 GiB（floor）| 通过 |

**所以把 D1/D2/D4 全部修对之后，racknerd 上的 `create` 依然会被拒。**
唯一能在带内解开死锁的是走**更新路径**收敛现存桶（更新只计差值、并把旧值显式加回）。
⇒ 收敛是本增量的**必需**部分，不是锦上添花；没有它，这次修复对现网**零效果**。

安全边界：收敛**不得缩到已用字节以下**（`UpdateObjectStore` 会拒绝，并可能把
`replication_degraded` 锁死——`raiseXferReplicas` 的注释已记录这个坑）。
实现必须先读 `info.State.Bytes` 再决定目标值：`target = max(want, usedBytes + 余量)`。
（racknerd 现状 used=0，所以第一次收敛必然安全。）

#### 与既有原则的冲突，及主进程的裁决

仓库有一条**明写的载重原则**（`internal/jsstream/jsstream.go:249-253`）：

> raise ONLY the replica factor — start from the LIVE config and change just Replicas, so an
> operator's `nats stream edit` limits (MaxBytes/retention/…) survive. … **the HA replica factor is
> cluster-owned; retention/limits are operator-owned.**

即 **MaxBytes 是运维所有的**。所以「每次 push 都把桶归一化到 tether 算出的值」会**直接违反**
这条契约：一个大盘运维故意把桶设成 4 GiB 换并发余量，会被 tether 悄悄改回 2 GiB。

**裁决：收敛必须是「按已证实的失败修复」，不是「总是归一化」。**

只有当**当前配置本身已不可满足**时才向下收敛——即按上面的服务器级公式，
以现有 MaxBytes 根本无法通过 `checkBytesLimits`，因而 push 是**硬阻塞**的。
在那个状态下运维的意图**在物理上无法被满足**，尊重它就等于让系统一直坏着；修复严格更优。

配套要求（缺一不可）：
1. 收敛必须**大声记日志**，含 before/after 与「为什么现有值不可满足」的算式，
   让运维看得见 tether 动了它的东西；
2. 健康状态下**绝不**触碰 MaxBytes（哪怕它与 tether 的算法不一致）——测试必须钉住这一点，
   否则这条裁决就退化成了「总是归一化」；
3. 向上永不自动扩（扩容是运维决策，且可能砸盘）。

## 1. 缺陷（D3 已被源码订正，见下）

**D1（确认）—— `jsStoreCeiling` 用错了量纲。**
`internal/broker/transfer.go:294` 返回 `info.Limits.MaxStore`（天花板），而 nats 判定用的是
`MaxStore − max(ReservedStore, Store)`。tether 索要的数从来就不是 nats 会批的数。

**D2（确认）—— 桶按磁盘比例定尺，而非按需要。**
`maxBytes = avail × 0.75`，上限 8 GiB。但：
- 单次传输硬上限 `XferMaxBytes = 2 GiB`（`internal/proto/xfer.go:43`）；
- 传输完成后对象由**两端都删**（`cmd/tether/transfer.go:290`、`internal/agent/transfer.go:490`），
  另有 orphan reaper 兜底；
- 桶的 backing stream 是 **`DiscardNew`**（nats.go 对象存储默认），满了**干净拒绝新写入**，
  不会挤掉在途对象。

⇒ 桶只需容纳**在途**传输；为一个 4.4 MB 的 push 预留 6.25 GiB 是砸死小盘的根源。

**D3（原假设已推翻，订正如下）。**
原假设「nats 先查存储额度、后查重名，所以现存桶被误判」**是错的**：`stream.go:775` 的
`addStream` **先**查重名（配置一致直接返回成功、不一致回 `JSStreamNameExistErr`），
`checkAllLimits` 在其后。10047 的真实来源是**集群 peer 选择**（上面的 `selectPeerGroup`），
它在流被分配之前就跑，所以重名短路根本没机会发生。
**可观察后果仍然成立**（一个现存、空的、可用的桶被回 10047），但机制不同——
而且这意味着：**修好 D1/D2 让索要的数落进 available，10047 自然消失**，
不需要单独做「先查存在」的改动。

**D4（确认）—— `xferEventsHistoryReserve = 2 GiB` 是常量，却假设只有一个会话。**
`history` 是**每会话 1 GiB**，真实预留是 `1 + N×1 + N×桶`。N=1 时才刚好对上。

**放大器（非本增量必修，但必须记录）—— tier-A 的实际上限是 511 KiB，不是 8 MiB。**
`cmd/tether/transfer.go:826` `tierAInlineCeiling` 把上限夹到 `max_payload/2 − 1024`；
默认 `max_payload` 1 MiB ⇒ **511 KiB**。而 `tether push --help` 写的是 "Up to 8 MiB: tier A"。
后果：现网几乎所有文件都掉进 tier B、都依赖 JetStream，上面的缺陷因此每次都命中。

## 2. 设计要点（主进程视角，待与专家产出对齐）

### ⚠️ 一个必须处理的相互作用

若把 `jsStoreCeiling` 改成返回「可用量」，则 `xferMaxBytesForCeiling` 里**再减一次**
`xferEventsHistoryReserve` 就是**重复扣减**——racknerd 会从「10047 报错」变成「**永久拒绝**」
（`avail` 变负 ⇒ `errXferStoreTooSmall`）。更诚实，但仍然不可用。

同理：`ReservedStore` **包含我们自己那个桶**的预留，重算时必须**加回**，
否则每次重算都会把自己的额度算成别人的。

### 候选公式（已按时间预算简化）

```
R              = b.xferTargetReplicas()                                   // 见 §7 A2
expectedOthers = R × (eventsMaxBytes + nSessions × historyMaxBytesPerSession)
headroom       = MaxStore − expectedOthers
size           = min(XferMaxBytes + margin, headroom × 3/4)
if size < xferBucketFloor → 拒绝（保留 errXferStoreTooSmall 语义）
```

`R ×` 是 §7 A2 采纳的修正：账户级 `tieredReservation` 按 `Replicas × MaxBytes` 计预留
（`jetstream_api.go:1343`），而服务器级不乘（`jetstream.go:2425`）——两级口径不同，取更紧的。
racknerd 与 drill 21 都是 `R=1`，所以现网观察不出差别，但 N≥2 会低估 3 倍。

代入：
- racknerd：`expectedOthers=2G, headroom=8.33G, size=min(2.06G, 6.25G) = **2.06 GiB**`
- drill 21（4 GiB tmpfs）：`expectedOthers=2G, headroom=2G, size=min(2.06G, 1.5G) = **1.5 GiB**` ⇒ 保持 GREEN

**为什么不在这里用 `ReservedStore`（一次重要的简化）**

第一版公式写的是 `provision = max(othersReserved, expectedOthers)`，其中
`othersReserved = max(Store, ReservedStore) − ourBucketMaxBytes`。它要拿到「我们自己那个桶的
MaxBytes」，就得**再加一次 JS 往返**去读 backing stream——而 sizing 的预算只有
**`xferSizingTimeout = 1500ms`**（`xfer_provision.go:35`），且这段代码已经有
「AccountInfo 卡死会吃光整个 deadline」的防御（`TestSizingStallDoesNotConsumeCreateBudget`）。
往里塞第二次往返是拿一个**已知的脆弱点**去换一点精度。

改成只用 `MaxStore − expectedOthers` 之后：
- sizing 保持**单次** AccountInfo 往返，预算不动；
- 「现存桶占了多少」这个信息**只在决定要不要收敛时才需要**，而那发生在
  `ensureXferBucketSized` 内部——那里本来就要读 backing stream（`raiseXferReplicas` 已经在读），
  且跑在 `xferCreateAttemptTO = 2500ms` 的预算下。信息在需要它的地方获取，不提前。

**已知的取舍（诚实记录）**：若运维额外建了 tether 不知道的流，`expectedOthers` 会**低估**，
于是 size 偏大、create 撞 10047 —— 落回现有的有界重试/拒绝路径，并由 C5 的错误串说明
「谁占着」。这比为了覆盖这种情形而把一次脆弱的往返塞进 sizing 更好。

`nSessions` 可得：`b.activeProxySessions()` 已有同类查询（`proxy_reconcile.go:41`）。

**`expectedOthers` 的输入今天没有单一真相源**（这正是 D4 的根）：
- `events` 的 1 GiB 是 `jsstream.go:97` 的**裸字面量**（无名字）；
- `historyMaxBytesPerSession` 在 `jsstream.go:121`，**未导出**；
- `xferEventsHistoryReserve = 2 GiB` 是这两者的**手抄和**，没有任何东西钉住它们一致。

⇒ 给 events 的字面量命名、并把两个常量**导出**（`internal/broker` 本就 import `internal/jsstream`，
见 `jsstream.ErrMetaGroupNotReady` 的用法），预留math 从此有单一真相源；
再加一条守卫：手抄和若与导出常量之和不符即变红。

### 可测性（已确认，无需新基础设施）

- `xferMaxBytesForCeiling` 是**纯函数**，现有表测在 `internal/broker/capacity_test.go`。
- `AccountInfo` 已有可注入的假实现：`xfer_provision_test.go` 的 `countingJS.AccountInfo` 与
  `stallJS.AccountInfo`；`countingJS` 已有 `maxStore` 字段驱动表测，只需再加
  `Store` / `ReservedStore` 两个字段即可覆盖新公式的全部分支。

### ⚠️ 边界陷阱：桶上限不能正好等于 `XferMaxBytes`

nats 对象存储按 **128 KiB** 分块（`nats.go/jetstream/object.go:486`，tether 不覆盖），
2 GiB 会拆成 **16384 块**，每块是一条带头部的消息；而 `MaxBytes` 统计的是**含每消息开销的
存储字节**，桶又是 `DiscardNew`。

于是若把桶上限设成正好 `XferMaxBytes`：一个 2 GiB 的文件会**通过准入**
（`xfer_provision.go:171` 判的是 `size > maxBytes`，相等不算超），然后在 put 中途撞上
`DiscardNew` 失败——比现在的「建桶就拒」更糟，因为它在传了一半之后才失败。

⇒ 桶上限必须是 `XferMaxBytes + 余量`（或准入改成对 `maxBytes − 余量` 比较）。
取前者更简单。余量按最坏分块数估：`16384 × ~200B ≈ 3.3 MB`，取 **64 MiB** 有充分裕度且
相对 2 GiB 可忽略。**这条必须有测试钉住**：`bucket >= XferMaxBytes + margin`。

### 不变量（任何方案都必须保住）

1. **绝不发 `MaxBytes <= 0`** —— nats 视其为 UNLIMITED，是更坏的静默砸盘。
2. `errXferStoreTooSmall` 是**永久**条件的哨兵，#67 的有界 provisioning 重试据此**零次**尝试；
   不得把瞬态错误归进来，也不得让永久条件漏出去。
3. `raiseXferReplicas` 刻意**保留**现存桶的 MaxBytes（缩到已用量以下会让 `UpdateObjectStore`
   拒绝并把 `replication_degraded` 锁死）。
4. wire 保持 N-1 窗口：additive/omitempty，不 bump ProtoVersion。
5. 代码必须对 N≥2 仍然正确（现网虽是 force-single N=1）。

## 3. 免升级的运维处置（现网止血，与代码修复正交）

那个 8 GiB 空桶缩到 2 GiB 即可腾出额度，**不损失任何能力**（单次传输本来最大 2 GiB）：

```
tether exec racknerd -- /tmp/natscli/nats-0.1.5-linux-amd64/nats \
  -s nats://127.0.0.1:4222 --nkey /etc/tether/secrets/broker.nk \
  stream edit OBJ_xfer-lab --max-bytes=2GB -f
```

（`nats` CLI 已放在 racknerd 的 `/tmp/natscli/`。）
另注：`push` 当前被 `force_single_active` 门挡着（幽灵 pc732 造成，**与磁盘无关**），
需要 `--ack-alerts`；那是另一件事。

## 4. 变更清单（主进程草案，待与专家产出对齐）

按「解锁力 / 风险」排序。

### C1 — 桶按需定尺，并留分块余量（D2 + 边界陷阱）

`internal/broker/transfer.go`：`xferBucketCap` 从 8 GiB 改为 `XferMaxBytes + xferBucketChunkMargin`
（2 GiB + 64 MiB）。理由与余量推导见 §2。

### C2 — 用「可用量」而非「天花板」（D1 + D4）

`jsStoreCeiling` / `xferMaxBytesForCeiling` 按 §2 的候选公式重写，
并**去掉重复扣减**（`xferEventsHistoryReserve` 不再在返回可用量之后二次扣）。
`expectedOthers` 由**导出的**常量算出（C3）。

### C3 — 预留量有单一真相源（D4 的根）

`internal/jsstream`：给 events 的 `1 << 30` 字面量命名并导出，导出
`HistoryMaxBytesPerSession`；`internal/broker` 改为引用它们。
守卫：手抄和若与导出常量之和不符即变红。

### C4 — 现存过大桶的带内收敛（**必需**，见 §1 的算术）

`ensureXferBucketSized`：当桶已存在、且其 MaxBytes **按服务器级公式已不可满足**时，
走 `UpdateObjectStore` 向下收敛到目标值；健康状态下**绝不触碰**（§2 的裁决）。
不得缩到 `info.State.Bytes + 余量` 以下。`ErrMetaGroupNotReady` 按现有可重试通道处理。

### C5 — 拒绝时说人话

sizing 拒绝与 10047 的错误串必须给出算式与出处：`MaxStore`、`storeReserved`、
各流各预留了多少、以及「谁占着」。今天运维只看到
`insufficient storage resources available`，无从知道一个**空的** 8 GiB 桶占了 77%。

### C6 —（文档）tier-A 上限的实际值

`tether push --help` 写 "Up to 8 MiB: tier A"，实际是 `max_payload/2 − 1024`
（默认 `max_payload` 1 MiB ⇒ **511 KiB**）。这条**照着它判断必然出错**，属诚实性缺陷。
本增量只改文案与帮助文本；是否要动那个夹取逻辑**不在本增量范围**。

## 5. 测试与变异（每条守卫都必须能变红）

| 守卫 | 变异（必须变红）|
|---|---|
| 桶上限 ≥ `XferMaxBytes + margin` | 把 margin 去掉 ⇒ 满尺寸传输在 put 中途溢出 |
| 可用量公式用 `MaxStore − max(Store, ReservedStore)` | 改回只用 `MaxStore` |
| 不重复扣减预留 | 在返回可用量后再减一次 `xferEventsHistoryReserve` ⇒ racknerd 形态变永久拒绝 |
| 预留量单一真相源 | 改动导出常量之一而不改手抄和 |
| 健康时不动 MaxBytes | 让收敛无条件执行 ⇒ 运维设定被 clobber 的用例变红 |
| 不可满足时收敛 | 删掉收敛分支 ⇒ racknerd 形态（8 GiB 空桶）仍被拒 |
| 永不缩到已用字节以下 | 去掉 `State.Bytes` 下限 |
| `errXferStoreTooSmall` 仍是永久条件 | 让新路径把瞬态错误裹进该哨兵 ⇒ 零重试断言变红 |

`countingJS` 加 `Store` / `ReservedStore` 两个字段即可覆盖全部分支（§2 已确认）。

## 6. 非目标（明确排除）

- **不动** `tierAInlineCeiling` 的夹取逻辑（C6 只改文案）。
- **不动** `XferMaxBytes = 2 GiB` 这个单次传输硬上限。
- **不**为小盘引入新的传输层（`expose` + rsync 仍是超大文件的答案）。
- **不**碰 `force_single_active` 告警门（那是幽灵 pc732 的事，与磁盘无关）。
- **不**在本增量里改 `events` / `history` 各自的 1 GiB（只是把它们变成有名字、可引用的常量）。

## 10. 外审后的主进程复核（阶段 C 步骤 6）

外审判定 **PASS**，并按授权直接修了 6 个 Major + 1 Medium。逐条独立复核（不采信报告，自己跑）：

### 采纳 —— 六条都是真缺陷，修法也对

- **F1**（account 与 server 两个量纲混用）：拆成各自扣减、取更紧者，并加饱和算术。核对无误。
- **F2**（收敛误缩 + `UpdateObjectStore` 会清空 `Placement`/`TTL`/`Metadata`）：改为**只在有限
  account 的精确同量纲证据**下才收缩，且 update 从现存 `StreamConfig` 完整映射。
- **F3**（同一 session 的多个 tier-B 会撞 `DiscardNew`）：tracker 锁内按桶串行。
  我先怀疑这条过于粗暴（同 session 两个小文件也会被拒），查证后**采纳**：pull 腿的 `entry.size`
  是空的（broker 只从 agent 才知道大小），所以「按字节累计」在 pull 上无法实现——
  外审在报告 §4 已把这条取舍写明。
- **F4/F5**（payload 上限要扣 chunk 开销与已用字节；准入要用现存桶的真实容量）：核对无误。
- **F6**（pull 的大小只有 agent 知道）：新增 additive/omitempty 的 `bucket_payload_max_bytes`
  指针字段，两端对称，`nil` 回落到全局上限（= 旧 broker 行为），并在 stat 与 `LimitedReader`
  **两处**执行。ProtoVersion 不变。
- **F7**（session 计数不受 sizing 预算约束）：`readDB` 增只读 `QueryRowContext`。

### 主进程补的三处

1. **F3 / F4 的修复没有守卫。** 我用变异验证过：删掉同桶串行化、去掉 chunk margin、
   去掉已用字节扣减——**三个变异全部零失败**。修了真缺陷却没留下能变红的测试，
   一次「化简」重构就会把它们悄悄撤销，而这两处恰好长得像「多余的循环」和「多余的减法」。
   已补 `TestTierBIsSerializedPerSessionBucket` 与
   `TestPayloadLimitSubtractsChunkOverheadAndUsedBytes`，三条变异各自精确变红。
   （F2 的守卫**是有效的**——我第一次变异打错了位点，据此说它无守卫是我的误判，已撤回。）
2. **`xferStoreCannotFit(ctx, _ int64)` 的参数已成死参数，函数名与行为不符**
   （它不再回答「放不放得下 want」，而是「账户是否已过量预留」）。
   重命名为 `xferAccountIsOverCommitted(ctx)` 并删掉死参数。
3. **一个报告未讲明的后果，必须写在这里。**

### ⚠️ 后果：收敛在**每台 tether 渲染的 broker 上都不会触发**

F2 把收敛限制在「有限 account 限额」上。而 §7 已经查实：tether 不渲染 `accounts{}`，
所以生产账户限额恒为 **-1**。⇒ **`convergeOversizedXferBucket` 在现网永不触发。**

这不等于本增量失效，但它**推翻了本 plan §1 的一个中心论断**（「收敛是必需项，
没有它对现网零效果」）。真实情况是：

| | 状态 |
|---|---|
| racknerd 的 push | **可用** —— 靠 **resolve-before-create**（现存桶不再走 create，因而不再撞 10047），不靠收敛 |
| 那个 8 GiB 空桶的预留 | **仍在**，不会被自动回收 |
| 后果 | 该 broker 上**新建会话**的 history 流仍会撞 10047（外审报告残留 §1 已列） |

外审的安全立场我认同：statfs 是**当下**的估算，而 nats 的真实限额定在 **JetStream 启用时**，
用一个估算去改写运维所有的配置，风险大于收益。

**因此现网仍需要一次运维动作**（缩掉那个空桶），命令在 §3；
`nats` CLI 已放在 racknerd 的 `/tmp/natscli/`。这条要在发版说明里写明，
不能让人以为升级即自愈。

## 9. 内审记录（阶段 C 步骤 4–5）

5 个视角的 reviewer + 1 个对抗性 verifier。verifier 的价值在于它**驳回**了相当一部分 finding
（含被标为 BLOCKER 的若干条），这让剩下的 CONFIRMED 项更可信。

### 已修（按影响排序）

| # | 级别 | 内容 | 变异 |
|---|---|---|---|
| IR-1 | BLOCKER | C4/C5 gate 在 `Limits.MaxStore > 0` 上 ⇒ 生产死代码（详见 §7 已撤销的 A5）| 改回 gate 账户限额 ⇒ 红 |
| IR-2 | MAJOR | 收敛在健康 store 上误触发（`ReservedStore` 已含本桶的双重计数）| 退回 `reserved+cur` ⇒ 红 |
| IR-3 | MAJOR | 收敛静默把 Replicas 3 降到 1 | 写回 `targetReplicas` ⇒ 红 |
| IR-4 | MAJOR | 收敛腰斩在途上传（`State.Bytes` 不含"已准入还要来的"）| 删在途检查 ⇒ 红 |
| IR-5 | MAJOR | 判据最终取 `reserved + target > ceiling` —— 比 `reserved > ceiling` 更稳健：nats 的限额定在 **JetStream 启用时**，`jsStoreCeiling` 读的是**当下** statfs，两者随磁盘漂移 | 见 IR-2 |
| IR-6 | MINOR | `TestHealthyBucketIsNeverResized` 原用 500 GiB／10 GiB —— **50 倍余量下错误判据与正确判据给出相同答案**，分辨不出缺陷。收紧到 10 GiB／6.5 GiB／4.5 GiB（桶大于剩余余量，正是双重计数会误触发的常态）| 退回双重计数 ⇒ 现在也红 |
| IR-7 | MINOR | 三处**注释说了假话**：godoc 仍描述已删的 `sizeErr`；`capacity_test` 命名了不存在的 `b.cfg.DB == nil` 守卫；`TestXferReserveTracksTheStreamConstants` 声称的变异其实不会红（`want` 由同一批常量算出）。逐条改成真话，最后一条改为说明**它真正钉的是"来源"而非数值** | — |
| IR-8 | MINOR | **契约扫描**（仓库自有规则）：C6 只改了 `push --help`，`docs/usage.md` 三处旧边界没跟着改 | — |
| IR-9 | MINOR | 我引入的 off-by-2KiB：「max_payload ≥ 16 MiB 即可达 8 MiB tier-A」是错的，恰好 16 MiB 时 `16777216/2−1024 = 8387584`，还差 1 KiB。正确门槛 **16779264** | — |

promised-guard 闸门另外抓到我改名时留下的注释／函数名不一致——**正是它存在的理由**。

### 一次 e2e flake，以及为什么它没有被当成 flake 处理掉

带改动的第一轮 `make e2e-parallel` 报了 4 个失败，全在 `TestHomeDelivery*`
（`home_delivery_test.go` 的 `waitFor(2*time.Second, …)`），与 xfer 路径没有交集。
诱人的结论是「并行 flake」，但**基线对照必须先做**：

| 运行 | 结果 |
|---|---|
| 改动 · 第 1 轮 | 4 FAIL（HomeDelivery ×4）|
| **基线（改动 stash 掉）** | **ALL PASS** |
| 改动 · 第 2 轮 | ALL PASS |
| 改动 · 第 3 轮 | ALL PASS |

单独跑（含 `-tags d5_integration`、并 `taskset` 限到 2 核跑三次）全部通过。
结论：**不可复现的并行 flake**，2 秒墙钟预算在 20 worker × 2 cpu 下偶发不足，
与本增量无关。

记在这里而不是略过，理由有两条：其一，基线只跑了**一次** ALL PASS，
它不能证明这条 flake 在基线上不存在——只能证明我这轮没有稳定复现的回归；
其二，`docs/reviews/parallel-flake-rootcause.md` 记的正是这一族，
而 `TestHomeDelivery*` 目前不在其中，下一个撞上它的人应当能查到这条记录。

### 未做（记录而非静默）

- **m1 · `xferReserveFor` 不计其他会话的 xfer 桶。** 仓库在 `clusterwrite.go` 已就同一盲点做过裁定。
  非回归（严格优于 8 GiB 前身），但 racknerd 在 **N=4 会话**时会再次不足。列为残留。
- **m2 · 往返数从 4 增到 7**（`Info(ctx)` 重取、`raiseXferReplicas` 内部两次、
  `xferStoreCannotFit` 两次 `AccountInfo`）。在 push 这条队头阻塞的订阅上是实打实的浪费。
  修法明确（用 `CachedInfo()`、把已取的 `*StreamInfo` 往下传），但属性能而非正确性，
  且会动到既有函数签名——留给外审裁决是否本轮做。
- **m6 · `QueryRow` 而非 `QueryRowContext`**，未继承 `xferSizingTimeout`。同为既有形态。
- **既有残留**：准入用算出的 `maxBytes`，而生效的可能是现存桶更小的值（§8 已记）。
- **`prepareObjectStoreConfig` 会清空 `Placement`/`Metadata` 等**：`UpdateObjectStore` 用的是
  完整重建的 config，所以收敛会丢掉运维的 `nats stream edit --tag`。
  `raiseXferReplicas` 有**同样**的形状（既有），诚实的修法是抽一个从 `si.Config` 起手的共用 helper。
  本轮未做，因为它同时改既有路径；已在此记录。

## 7. 定稿记录（阶段 A 步骤 2，主进程逐条裁决）

多专家 workflow：4 份 draft（minimal / correctness / failure-mode / operator 四个视角）
+ 4 份对抗性 critique。以下只记**改变了 plan 的**裁决；每条都由主进程重新对源码核对，
不因其来自专家而照单全收。

### 采纳 —— 而且是我自己的方法论错误

**A1（r8#2，FATAL）：我读错了 nats-server 的版本。**
我通篇读的是 `nats-server/v2@v2.14.0`——那是 `go.mod` 里的**嵌入式测试** server。
**生产装的是 v2.10.22**（`scripts/install.sh:659` `NATS_SERVER_VERSION="${TETHER_NATS_SERVER_VERSION:-v2.10.22}"`；
racknerd 上 `nats-server --version` 亦为 v2.10.22——这个数我本会话早先还亲眼看到过）。

已在 **v2.10.22** 上把全部结论重新核对（下面 A2/A3 即重核结果）。教训记在这里：
**读依赖源码之前先确认「生产跑的是哪一个」**，`go.mod` 的版本可能只服务于测试。

**A2（r8#3，FATAL）：副本会成倍放大预留，我的公式漏了。**
v2.10.22 的两级口径**不同**，这比 critique 说的更微妙：

```go
// 账户级 tieredReservation (jetstream_api.go:1343):  乘副本
reservation += (int64(sa.cfg.Replicas) * sa.cfg.MaxBytes)
// 服务器级 js.storeReserved (jetstream.go:2425):     不乘
js.storeReserved += cfg.MaxBytes
```

racknerd 全是 `Replicas=1`，两者都是 10 GiB，所以现网观察不出差别；但 **N≥2 时账户级看到 3 倍**。
⇒ 公式改为 `expectedOthers = R × (eventsMaxBytes + nSessions × historyMaxBytesPerSession)`，
`R = b.xferTargetReplicas()`。取两级中更紧的那个。

**A3（r5#3）：`21-smalldisk-tierb` 是本增量的既有 deploy-tier 证明，必须不被弄红。**
我此前不知道它存在。它用 **4 GiB tmpfs** 限住 broker 的 JS 文件系统，断言
「tier-B push 成功」+「文件真的落到 agt1」，现在是 GREEN，正是 #21 修复的回归。
代入我的公式：`expectedOthers=2G, headroom=4−2=2G, size=min(2.06G, 1.5G)=1.5G` ⇒ **保持通过**。
⇒ 列为本增量的**必跑 drill**（阶段 C），并且它天然就是「重复扣减」这个坑的活体检测器。

**A4（r5#1，与我独立发现互相印证）**：把 `jsStoreCeiling` 改成返回可用量、而 `xferMaxBytesForCeiling`
里仍减一次 `xferEventsHistoryReserve`，会把小盘从「报错」变成「**永久拒绝**」。
我在 §2 已独立识别并规避（不二次扣减）；专家从相反方向撞到同一处，加强了这条的置信度。

### 驳回 —— 结论错了，尽管前提对

**R1（r8#1，被标为 FATAL）：「两条 create 路径都先查重名，所以 10047 说明桶当时不存在，
因此 D1 单独就能解释事故」——驳回。**

前提对（`addStream` 确实先查重名：v2.10.22 `stream.go:446/462`；集群 `jetstream_cluster.go:6102`），
但**漏了 API 入口那一层**：`jsStreamCreateRequest` 在调用 `addStream` **之前**先跑
`acc.jsNonClusteredStreamLimitsCheck(&cfg)`（`jetstream_api.go:3293`），而它内部就是
`tieredReservation` + `checkAllLimits`。所以额度检查**确实先于**重名检查发生，
`addStream` 里的顺序根本没被走到。

在 v2.10.22 上把 racknerd 的数代进 `checkBytesLimits`（`jetstream.go:2262-2269`）：

```go
case FileStorage:
    if currentRes+totalBytes > selectedLimits.MaxStore { → 10047 }       // 账户级(排除本流)
    if checkServer && js.storeReserved+addBytes > js.config.MaxStore { → 10047 }  // 服务器级(含本流)
```

`js.storeReserved(10 GiB) + addBytes(6.25 GiB) = 16.25 GiB > MaxStore(10.33 GiB)` ⇒ **10047** ✓
与实测完全一致。⇒ 「桶当时不存在」的推论不成立，`OBJ_xfer-lab` 确实在（`nats stream ls --all`
与 `/jsz` 双重确认，0 消息 0 字节）。

**并且这条驳回保住了本增量的核心**：把 sizing 修成 2 GiB 之后，
`10 + 2 = 12 GiB > 10.33 GiB` ⇒ **仍然 10047**。所以「带内收敛」不是可选项。
若采纳 R1，本增量对现网将是零效果。

**R2：收敛可行性也在 v2.10.22 上重核过。** `stream.go` 更新路径的注释与代码：
`// If we're updating to a lower MaxBytes (maxBytesDiff is negative)` → `maxBytesDiff = 0`，
缩容只计 0 字节 ⇒ 服务器级 `10 GiB + 0 > 10.33 GiB` 为假 ⇒ **通过**。收敛设计成立。

### ❌ 已撤销 —— A5 是**我判错的**，原仓库注释一直是对的

> **这一条整节保留但作废，因为它比删掉更有用**：内审的三个 reviewer 独立指出它是 BLOCKER，
> 而按它做出来的代码在生产上是死的。留着，是为了让下一个人不要再走同一条弯路。

**我当时的结论（错的）**：`TestG67SizingTimeoutCannotMoveTheAdmissionDecision` 驳回一条外审
finding 所依据的「AccountInfo 报 MaxStore = -1，`jsStoreCeiling` 总走 statfs」是假前提，
理由是 racknerd 的 `/jsz` 报 `max_storage = 10.33 GiB`（有限值）。

**为什么错**：`/jsz` 的 `max_storage` 是 **`JetStreamConfig.MaxStore`（服务器级）**，
而 `jsStoreCeiling` 读的是 **`AccountInfo.Limits.MaxStore`（账户级）**——**两个不同的字段，
JSON 名字相同**。tether 不渲染 `accounts{}`，nats 于是给全局账户装上
`defaultJSAccountTiers`（`server/opts.go`），账户级限额是 **-1**。

实测坐实（`nats account info` + 原始 JS INFO 回包）：

```
Storage: 47 MiB of Unlimited
"storage":49485651  "reserved_storage":10737418240  "max_storage":-1
```

**代价**：我据此把收敛触发器 gate 在 `Limits.MaxStore > 0` 上——**在每台真实 broker 上都是死代码**。
C4/C5 都不会跑。三个 reviewer 独立抓到。

**已做的处置**：撤销对该测试注释的"订正"，恢复 `MaxStore = -1` 这个真相并把这次弯路写进注释；
把触发器改成读 `jsStoreCeiling`（账户有限则用它，否则 statfs），并新增
`TestConvergenceFiresWhenTheACCOUNTLimitIsUnlimited` /
`TestStorageAccountingSurvivesAnUnlimitedAccountLimit` 钉住生产形态。

**教训（比结论更值钱）**：这是本轮**第二次**踩同一个坑——先是读错 nats-server 版本（§7 A1），
再是读错「哪一个 MaxStore」。两次的共同点都是**拿一个名字相同的数当成了另一个**。
下次要断言某个字段的生产取值，先问「这个值是谁产生的、我读的是不是同一个产生者」。

### 采纳 —— 内审确认并已修复的四条

**IR-1（BLOCKER，三个 reviewer 独立提出）**：见上，C4/C5 曾是生产死代码。

**IR-2（MAJOR）—— 收敛在健康 store 上误触发（又一次双重计数）。**
`ReservedStore` 已含本桶，而我问的是「还能再放下一个 `cur` 吗」。实测：16 GiB store、
10 GiB 预留（其中 8 GiB 是自己）、6 GiB 真正空闲、什么都没被挡，收敛仍把运维的 8 GiB 改成 2 GiB。
判据最终取内审建议的 `reserved + target > ceiling`——它同时避开双重计数，
又比 `reserved > ceiling` 更稳健：nats 的限额定在 JetStream **启用时**，
而 `jsStoreCeiling` 读的是**当下**的 statfs，两者会随磁盘变化漂移。

**IR-3（MAJOR）—— 收敛静默降副本。** 现存 Replicas=3 被写成 targetReplicas=1。
复制是 raise-only、集群所有；容量修复无权顺手削掉冗余。改为保留现存副本数。

**IR-4（MAJOR）—— 收敛会腰斩在途上传。** `State.Bytes` 只报**已存**字节，不报一个**已准入**的
传输还要送多少。1.4 GiB 对象传到 600 MiB 时看起来只占 600 MiB，会被放行缩到 768 MiB，
然后撞 `DiscardNew` 死在半路——**由修复本身造成**的、正是本增量要消灭的那种失败。
改为查 `b.transfers.activeOBJStreams()`；`b.transfers` 为 nil 时同样不动
（读不到就不碰，绝不"假定安全"）。

四条各自有测试，四条变异各自**精确红对应的那一条**。

### 待办（历史记录）

synth 的综合结论尚未返回；其余 critique 里若干条（`GrowthReserve` 会 re-bricks、
10023 语义、MB/MiB 算术、`CapsResp` 新字段重蹈 #67 face B、若干测试是恒等式）
属于**专家 draft 之间**的争议，不直接冲击本 plan 的 §2/§4，将在 synth 返回后
一并过一遍，只把改变本 plan 的记进来。
