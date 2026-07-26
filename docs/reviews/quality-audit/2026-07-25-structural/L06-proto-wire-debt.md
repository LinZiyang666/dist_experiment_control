# L06 — wire 协议与消息模型的演化债

> 结构性质量审计 · lane key = `proto-wire-debt` · 2026-07-25
> 范围：`internal/proto/`（非测试 2,042 行）、`internal/adminsock/`（1,245 行）、`internal/schema/`（97 行），
> 以及全仓的消息编解码点、subject 构造/解析点、error-code 生产/消费点。
> **只读审计**：未修改任何实现代码。

---

## 结论

**净判断：这个 lane 不臃肿——它的问题是"契约写在注释里而不是类型里"。**

proto+adminsock+schema 三个包合计 3,384 物理行，去掉注释和空行只剩 **1,778 行真代码**，
承载 **86 个 wire 类型 / 61 个 subject API / 5 套独立传输面**（NATS 控制面、adminsock Unix
socket、tunnel 行协议、raft command、签名 roster/seeds/manifest）。`messages.go` 1,095 行里
**只有 465 行代码，48% 是注释**——平均每个消息类型 5.4 行代码。没有 getter/setter 层、没有
DTO 转换层、零手写 `MarshalJSON`、全部走 `encoding/json` 反射。**按体量算这是一个精瘦的 wire 层，
"16 万行屎山"的怀疑在这个 lane 上不成立。**

真正的债全部是同一种形状：**"某个跨包契约只存在于注释和人工维护的列表里，没有编译期或测试期的强制"**。
具体表现为 4 类：

1. **动词与错误码没有 SSOT**。10 个转发动词是裸字符串，散在 broker 订阅表 / agent dispatch switch /
   CLI 三处；≥65 个 error code 是裸字面量，对着 CLI 侧两张手写 map。**已经漂移了**：8 个码有人工撰写的
   user-facing hint（说明有人认真想过它们）却没进 exit-class map，运行时静默落到 exit 70
   ("tether 内部 bug，无法分类")——而 hint 文本写的是"换个 --name 重试"。
2. **cluster 路由策略与订阅声明分处两个包**，靠 `strings.HasSuffix` 字符串对齐；这个裂缝**已经产出过一次
   线上级 bug**（`broker.go:1004` 的注释亲口记录：`.proxy.sub.*.req` 通配符匹不上 per-leaf 后缀检查 →
   leader-only 命令被 queue-group 到静默 follower → ctl 超时 (N-1)/N 的概率）。27 条订阅里只有 16 条被 guard test 钉住。
3. **版本机制大半是仪式**。9 个版本/schema 计数器里只有 4 个有读者；`ClusterHealthSchemaVersion`
   已经 bump 到 **5** 而全仓无任何消费者（它自己的注释坦白 "a documentation ledger, not a compat switch"）；
   `ClusterGrowSchemaVersion` 连对应的 wire 字段都不存在。与此同时 **architecture.md J.2 承诺的
   CLI↔broker proto 握手从未实现**，而唯一的 join 版本闸挂在 v0.4.2 已停用的 `OpClusterAdd` 上——
   **现网 `cluster add` 走的活路径没有任何 proto/release skew 检查**。
4. **前向兼容不变量只覆盖一半**。整个项目的兼容策略就是 "additive omitempty，老端忽略未知键"，
   而验证这条性质的 `TestUnknownFieldsIgnored` 只覆盖 **36/86** 个类型；golden 字节只钉住 8 个
   （全是 P4–P6 时代的），P11 transfer / P13 proxy / 全部 cluster 类型一个字节都没钉。

**bloat 打分：3/10**（1=精炼，5=正常工程债）。行数正当、密度高、无重复实现；扣分全在"手工维护的
平行列表"数量上——它们不占行数，但每一条都是未来改动的绊线。

**verdict：minor-debt。** 建议的修复总量约 300 行删除 + 3 个生成式 guard test + 1 个版本闸补位，
都是包内局部改动，不触 wire、不需要现网重装。唯一需要尽快处理的是 Finding 3（活路径缺版本闸），
它是正确性问题不是风格问题。

---

## 范围与方法

**读进去的文件**（全文，非 grep 抽样）：
`internal/proto/{version,subjects,messages,identifiers,alerts,cluster_roster,cluster_grow,cluster_upgrade}.go`、
`internal/proto/proto_invariants_test.go`（不变量 harness）、
`internal/adminsock/protocol.go`、`internal/schema/audit.go`、
`internal/auth/permissions.go`、`cmd/tether/{error_hints,exitcode}.go`、
`internal/broker/broker.go`（订阅表段）、`internal/broker/clusterwrite.go`（路由策略段）、
`internal/agent/exec.go`（dispatchForwarded）、`internal/cluster/join_bundle.go`、
`internal/tunnel/tunnel.go`（包 doc + `parseRegisterLine`）、
`test/determinism/lint_skeleton_test.go`（版本字面量 tripwire）。

**机械分析**（只读脚本，临时文件写在仓库外）：
- 对 proto 的 86 个导出类型逐个统计 生产引用 / 测试引用 / 包内引用；
- 对 61 个 subject builder/const/parser 同样统计；
- 从 broker/agent 生产代码机械抽取 error-code 字面量集合 P，与 `brokerCodeHints`（集合 H）、
  `brokerCodeExitClasses`（集合 E）做三向差集；
- 统计 lane 内每个文件的 注释/空行/代码 行数比；
- `go list -deps` / `go list -f {{.TestImports}}` 验证 auth↔proto 的"import cycle"说法；
- `go build ./...` 通过（未跑任何测试，遵守 lane 约束）。

**没做**：`make test`/`make e2e`/simcluster（按任务约束）。

---

## Findings

### F1 — 命令动词与错误码没有 SSOT，且已实测漂移 · severity: high

**证据**

动词侧（10 个转发动词，全是裸字符串，三处独立维护）：
- `internal/broker/broker.go:962-1013` — 订阅表，每条手写 `proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.<verb>.req"`
- `internal/agent/exec.go:57-100` — `dispatchForwarded` 的 `switch verb` 十个 case
- `cmd/tether/exec.go:89`、`expose.go:96,330`、`run.go:122,469`、`node.go:277`、`transfer.go:209,252,313,455` —
  CLI 侧 `proto.SubjCmdBy(sid, actor, nid, "expose-rm")` 等 10 处裸字面量
- `internal/proto/subjects.go:57,65` — `SubjCmdBy(sid, actor, nid, verb string)` / `SubjCmdForwarded(...)`
  签名收的是 `verb string`，proto 里**没有任何动词常量**

唯一的反例是 `internal/broker/home_delivery.go:87 const homeDeliveryVerb = "home"`——说明作者知道该怎么做，
只是只做了一处。

错误码侧：
- 机械抽取出 **≥65 个** 生产侧 code 字面量（`Code: "..."`、`replyXxxErr(msg, "...")`、`proxyErr(msg, "...")`）
- `cmd/tether/error_hints.go:19-71` — `brokerCodeHints`，34 个键
- `cmd/tether/error_hints.go:78-133` — `brokerCodeExitClasses`，45 个键
- `internal/proto/messages.go` 里另有 24 个 `// Code string \`json:"code,omitempty"\`` 的行内注释
  各自列一份码表（第三份副本，且已过期——例如 `ExposeResp` 的注释列了 `on_broker_single_mode` 但
  该码不在任何一张 CLI map 里）

**三向差集实测结果**：

| 集合关系 | 数量 | 后果 |
|---|---|---|
| 有 hint、无 exit class、且**确实被生产代码发出** | **8** | 运行时落 exit 70 |
| 有 exit class、无 hint | 20 | 用户看到裸 code |
| 有生产者、两张表都没有 | ~36 | 裸 code + exit 70 |
| hint 里注册但全仓无生产者 | 5 | 死条目 |

那 8 个"有人认真写了 hint 却拿不到 exit class"的码是最刺眼的证据：
`actor_invalid` `already_revoked` `frpc_failed` `name_reserved` `subject_malformed`
`sub_name_invalid` `sub_name_taken` `sub_not_found`。
其中 `sub_name_taken` 的 hint 原文是 *"an active subscriber already uses that name; pick another"*
——明确的用户可操作错误，却退出 70，而 `exitcode.go:31` 把 70 定义成
*"EX_SOFTWARE: malformed reply / decode error / UNCLASSIFIED"*。
`docs/usage.md §9.13` 告诉自动化对未分类错误做 backoff 重试——于是一个"改个名字就好"的错误
会被脚本无限重试。

`error_hints.go:135-138` 自己把这个问题写成了注释：
> `dataplaneNotConvergedCode` mirrors internal/broker's `codeDataplaneNotConverged`.
> Renaming either side silently downgrades a terminal "not converged" signal to exit 70;
> **the wire-stability test is the only thing standing between those two literals.**

**为什么是债（它让什么未来改动变难/变危险）**
- 加一个新动词：改错一个字符（`expose_rm` vs `expose-rm`）编译通过、单测通过，只在真跑起来时表现为
  "agent 收到 unknown forwarded verb" 的 warn 日志 + ctl 超时。三处字面量没有任何编译期关联。
- 加一个新错误码：默认行为是"用户看到裸 token + 退出 70 + 被监控脚本无限重试"。开发者必须**记得**
  去改 `cmd/tether/error_hints.go` 的两张 map——没有任何机制提醒。实测已有 36 个码走了这条路。
- 改一个已有码的语义：三份副本（broker 字面量 / proto 注释 / CLI 两张 map）必须手工同步，
  漏一处不报错。这正是 `feedback-contract-change-sweep` 记录过的复发缺陷模式。

**建议**
1. 在 `internal/proto` 建 `verbs.go`：`const VerbExec = "exec"` … 10 个常量；把 broker 订阅表、
   agent switch、CLI 三处改成引用常量。改动机械、零 wire 变化。
2. 在 `internal/proto` 建 `codes.go`，把 error code 提升为常量（`adminsock` 已经这么做了，18 个
   `Code*` 常量——照抄那个做法即可）。
3. **关键一步**：加一个 AST 驱动的 guard test（仿 `test/determinism/lint_skeleton_test.go` 的
   `TestNoStrayVersionLiteral`），扫描 `internal/broker`/`internal/agent` 里赋给 `Code:` 字段的字符串
   字面量集合，断言它 ⊆ `brokerCodeHints ∪ brokerCodeExitClasses` 的键集。~40 行，一次性关掉整个缺陷类。

**量化**：净删行 ≈ 0（常量化是等量替换），但消除 3 份平行码表中的 2 份的**漂移可能性**；
新增 ~60 行常量 + ~40 行 guard test。已知漂移实例 8 + 36 = 44 个码需要一次性归位。

**风险**：low。不触 wire（常量值 = 现有字面量），不触现网。

---

### F2 — cluster 路由策略与订阅声明分处两包，靠字符串后缀对齐；已产出过线上级 bug · severity: high

**证据**
- `internal/broker/broker.go:958-1013` — 27 条订阅的**声明处**（subject → handler）
- `internal/broker/clusterwrite.go:59-79` — `isBroadcastClusterSubject(subj string) bool`，
  对 14 个 leaf 做 `strings.HasSuffix`，**决定**每条订阅在 cluster 模式下是 broadcast 还是 queue-group
- `internal/broker/broker.go:1026-1031` — 消费处：
  `if b.clusterMode && !isBroadcastClusterSubject(ss.subj) { QueueSubscribe } else { Subscribe }`
- `internal/broker/subjects_cluster_test.go:18-48` — guard test，**第三份**手写 subject 列表
  （11 broadcast + 5 queued = 16 条，覆盖 27 条订阅中的 16 条）

已发生的 bug，代码里有第一手记录（`broker.go:1004-1008`）：
> Mega-audit MAJ-2: register the CONCRETE create/revoke/list leaves, NOT the wildcard
> `.proxy.sub.*.req`. **The wildcard never matched isBroadcastClusterSubject's per-leaf HasSuffix
> check, so it was queue-grouped and the leader-only create/revoke landed on a silent follower
> ~(N-1)/N of the time → ctl timeout.**

注意这个匹配是对 **subject 模式串**（含字面 `*`）做后缀匹配，不是对真实 subject——
`".transfer.*.complete"` 这条 leaf 只有在订阅串**恰好**写成同样的通配形状时才命中。

**为什么是债**
- 新增一条 ctl 命令订阅时，**默认行为是 queue-group**。如果这条命令需要 leader 视角（几乎所有写路径都需要），
  开发者必须记得去**另一个文件**的字符串列表里加一行；忘了就是一个只在 N≥2 集群下、概率性
  ((N-1)/N)、且只在真集群里才复现的静默丢命令 bug——hermetic Go 测试结构上抓不到（这正是
  `test/simcluster/` 存在的理由，但 simcluster 的 drill 覆盖面又不含每个新动词）。
- guard test 是第三份手写列表，只迭代自己的数组，**新订阅不进列表就不会被检查**。今天 27 条里
  11 条（`upgrade.req`、`proxy.status.req`、`proxy.sub.list.req`、`node.list.req`、
  `ev.node.*.proxy.*`、`pty.*.failed`、`session.create/list.req`、`ev.node.*.proc.*.started`、
  `ev.node.*.transfer.*` 中的部分…）没有被任一断言覆盖。

**建议**
把路由策略**移进订阅表本身**——把 `{subj, handler}` 结构体加一个 `broadcast bool` 字段，
删掉 `isBroadcastClusterSubject`。这样"新增订阅"和"选择路由策略"变成同一处的同一个决定，
编译器强制你填（Go 结构体字面量虽不强制，但 review diff 里一目了然），并且 guard test 可以直接
**从表里读**而不是维护第四份列表。

**量化**：删 21 行（`isBroadcastClusterSubject`）+ 简化 guard test 约 30 行；订阅表加 27 个布尔字段。
净删约 20 行，消除的是"两处不同步"这个缺陷类。

**风险**：medium。要逐条核对现有 14 个 broadcast leaf 对应到哪几条订阅（`.transfer.*.finalize.req`
这类模式匹配要人工确认），核对错了会改变集群路由行为。建议连同 `subjects_cluster_test.go` 的
断言一起改，并在 simcluster 跑一次 N=3 的 drill 验证。**不触 wire。**

---

### F3 — 版本握手：架构承诺的 CLI 侧从未实现；唯一的 join 版本闸挂在已停用的 op 上 · severity: high

**证据**

(a) `docs/architecture.md:1591-1606` (J.2) 明确规定：
> **CLI → tetherd**：… CLI 在 **NATS CONNECT 阶段**附带 `proto_version`（通过 `client.Opts.Name` 或
> `ConnectionInfo.Jwt.Claims.Aud` 等字段），由 `auth_callout` 在签 JWT 之前比对：不等 → 直接拒绝 CONNECT …
> CLI 解码出相同的 `proto_mismatch` 文案。

实测：`internal/broker/authcallout.go` + `internal/auth/*` 全文 **`ProtoVersion` / `proto_version` 零命中**。
生产侧只有 3 个 ProtoVersion 强制点：
- `internal/broker/broker.go:1314` — agent register 握手（真实、生效）
- `internal/broker/upgrade.go:91` — `UpgradeReq.ProtoVersion`（真实、生效）
- `internal/broker/clusterstatus.go:880` — join skew 闸（**死路径，见 (b)**）

`CapsResp.BrokerProto`（`messages.go:944`）由 broker 填，但**唯一的读者是 `test/cli_e2e/transfer_test.go:246`**
——生产 CLI 从不读它。

实际保护 CLI 的是 subject 前缀（`tether.v2.*`）+ JWT ACL 模板由同一个 `subjectPrefix` 渲染：
一个 v1 CLI 的 pub 会被 NATS 权限拒绝。这在**安全上**是够的，但**UX 上**给出的是
"Authorization Violation" 或 no-responders 超时，而不是 J.2 承诺的
`✖ proto mismatch: your tether CLI is … (proto=2), server is … (proto=1)`。

(b) **join 路径的版本闸是死的**：
- `internal/broker/clusterstatus.go:879-902` — `versionSkewResponse`（B6 A3 闸：joiner proto ≠ cluster proto
  硬拒，release skew 警告）
- 唯一调用点 `clusterstatus.go:831`，在 `handleAdd`（807-878）内
- `handleAdd` 唯一调用点 `clusterstatus.go:695-696` — `case adminsock.OpClusterAdd:`
- `internal/adminsock/protocol.go:150-154`：
  > v0.4.2: **OpClusterAdd is deliberately NOT routed.** … The `cluster add` CLI was deleted in C8;
  > grows now go through `OpClusterJoinApprove` → `driveJoin`
- 活路径 `OpClusterJoinApprove → StartJoinOperation → cluster.DecodeJoinBundle`：
  `internal/cluster/join_bundle.go:20-36` 的 `JoinBundle` 结构体 **没有 proto/release 字段**；
  `cmd/tether/cluster_add_drive.go`（886 行的 grow 编排器）里 `ProtoVer|ReleaseVersion|CommandVer` **零命中**。

结论：**现网 `tether cluster add` 把一个任意 proto 版本的 broker 加进集群，没有任何检查。**
`b6_skew_test.go` 全绿，因为它直接调 `versionSkewResponse`——测试覆盖了一条 CLI 到不了的路径。

(c) 相比之下 `cluster upgrade` 做对了：`cmd/tether/cluster_upgrade_drive.go:337` 通过
`ClusterHealthResp.CommandVer` 做 canary 比对并在 unverifiable 时保守处理。**同一个团队在
upgrade 上建了三轴闸（proto+command+ops），在 grow 上一个都没有**——因为 grow 的闸被绑在了
transport op（`OpClusterAdd`）上而不是绑在 join bundle 上，op 一退役闸就跟着掉了。

**为什么是债**
- 结构成因清晰且会复发：**版本闸挂在"哪个 socket op"这一层，而不是挂在"哪个数据结构"这一层**。
  任何一次"我们换个更好的入口 op"都会静默丢掉挂在旧 op 上的所有校验。C8 那次换入口就是这么丢的。
- 后果具体：一个 proto v3 的 broker 被 approve-join 进 v2 集群，raft 会复制成功（command 域独立），
  但它服务的 agent 会用 `tether.v3.*` subject——一整个 broker 的 agent 全部失联，且
  `cluster status` 看起来 HEALTHY_HA。
- J.2 未实现这件事本身危害有限（前缀已经隔离了），但它让 architecture.md 作为"实现的尺"失真：
  下一个人读 J.2 会以为 CLI 侧有闸。

**建议**
1. 把版本字段搬进 `cluster.JoinBundle`（加 `ProtoVersion int` / `ReleaseVersion string`），
   在 `DecodeJoinBundle` 或 `StartJoinOperation` 里做闸。**注意 bundle 前缀已经是 `tether-join:v1:`，
   加 omitempty 字段不破坏旧 bundle**（旧 bundle 解出 0 → 保持今天的 allow+warn 语义）。
2. `versionSkewResponse` 的逻辑直接复用，把它从 `clusterAdminBackend` 提到一个纯函数上（它已经
   被注释标为 "extracted so the ALLOW paths are unit-testable"，只差换个调用点）。
3. 要么实现 J.2 的 CONNECT 期 proto 比对，要么**改 architecture.md** 记录现状
   （"CLI 侧由 subject 前缀 + JWT 模板隐式隔离，不做显式握手；代价是错误文案为 auth violation"）。
   项目规矩是"实现中发现设计问题先改文档"——这条已经欠了很久。

**量化**：新增 ~20 行（bundle 两个字段 + 闸调用）；顺带让 `OpClusterAdd` 那 ~225 行死路径
（见 F6）可以整体删除而不丢失校验能力。

**风险**：medium。改 `JoinBundle` 属于 wire 变更，但是 additive/omitempty 且 bundle 是操作员 OOB
携带的一次性串，不涉及现网长连接，**不需要重装**。

---

### F4 — 前向兼容不变量只覆盖 36/86 类型，catalogue 手工维护 · severity: medium

**证据**
- `internal/proto/proto_invariants_test.go:28-33`（作者自己写的警告）：
  > **Keep this list in sync with the type catalogue in messages.go — adding a new public message type
  > without an entry here will silently skip roundtrip coverage.**
- `allRoundtripCases()` 实际条目：**36**；proto 导出类型：**86**
- `TestUnknownFieldsIgnored`（`:176-215`）——**整个项目兼容策略的验证器**（"additive omitempty，
  老端忽略未知键"）——迭代的正是这 36 条
- `internal/proto/testdata/golden/` 只有 **8 个** golden 文件，全部是 P4–P6 时代类型：
  `node_register_{req,resp,resp_empty_slices,resp_proto_mismatch}` / `heartbeat` /
  `exec_chunk_binary` / `expose_forwarded_req` / `port_event`
- fuzz 目标（`proto_fuzz_test.go`）**21** 个，同样偏 P4–P6
- **25 个类型在 `internal/proto/*_test.go` 里零出现**：
  `AlertAckReq AlertLsResp AlertView CertPins ClusterManifest ClusterRoster ClusterUpgradeResp
  DestructiveGate HomeAssignment ProxyNodeEntry ProxySetReq ProxySetResp ProxyStatusReq
  ProxyStatusResp ProxySubCreateReq ProxySubCreateResp ProxySubEntry ProxySubListReq ProxySubListResp
  ProxySubRevokeReq ProxySubRevokeResp PsReq RehomeDirective RosterBroker SeedBundle`
  （其中 `ClusterRoster`/`SeedBundle`/`RosterBroker` 在 `internal/clusterroster` 有真实的 sign/verify
  测试，不算裸奔；其余的 unknown-field 容忍性**无人验证**）
- 补充覆盖是**按 feature 分散的**：`transfer_test.go` 手工 roundtrip 11 个 transfer 类型、
  `proxy_test.go` 4 个、`b4_expose_alert_test.go` 3 个、`cluster_grow_test.go` 3 个——
  即同一条不变量在 5 个文件里各实现了一遍局部版本

**为什么是债**
- `ProxyDirective`（`messages.go:971-997`）是全仓最复杂的兼容契约之一——(Generation, Epoch)
  字典序排序 + `Home *HomeDirective` 指针 nil 保持 byte-identical——**它的 unknown-field 容忍性
  没有任何断言**。P13 那 8 轮外审（`p13_external_review_round8_test.go`）加的是行为测试，
  不是 wire 不变量测试。
- 后果是"下一次 additive 字段加错位置"没人拦：例如把一个字段写成非 omitempty，或给 `time.Time`
  加 omitempty（Go 的 `time.Time` omitempty 不生效，零值仍会序列化），roundtrip 断言会抓，
  但只对在册的 36 个类型有效。
- 这也是 CLAUDE.md 那套"每 phase 多专家审查"流程的一个副产品：每个 feature 自己带一套测试文件，
  没有人负责**把新类型登记进全局不变量表**。

**建议**
加一个反射/AST 驱动的完备性测试（~30 行）：枚举 `internal/proto` 包内所有导出 struct 类型，
断言每个都出现在 `allRoundtripCases()` 里（或在一个显式的 `skip` 白名单里，白名单每项要写理由）。
这样"忘记登记"从静默变成红灯。

**量化**：新增 ~30 行；需要为约 50 个类型补 catalogue 条目（每条 2–4 行，共 ~150 行测试代码）。
可以分批做——先补 P11/P13/cluster 三个族。

**风险**：low。纯测试新增。

---

### F5 — 9 个版本计数器只有 4 个有读者；其中一个已 bump 到 5 · severity: medium

**证据**

| 计数器 | 值 | 写 | 读/闸 | 判定 |
|---|---|---|---|---|
| `proto.ProtoVersion` | 2 | ✓ | ✓ ×2 活（register 握手、node upgrade） | **真** |
| `proto.SubjectVersionToken` / `SubjectPrefix` | v2 | ✓ | ✓ 结构性 + AST tripwire + auth 交叉断言 | **真** |
| `cluster.CommandVersion()` | — | ✓ | ✓（`cluster upgrade` canary 闸） | **真** |
| `proto.ClusterRosterSchemaVersion` | 1 | ✓ | ✓ `clusterroster/roster.go:130` `>` 拒绝 | **真** |
| `proto.SeedBundleSchemaVersion` | 1 | ✓ | ✓ `clusterroster/seeds.go:97` `>` 拒绝 | **真** |
| `proto.ClusterHealthSchemaVersion` | **5** | ✓ | ✗ 全仓零读者 | 仪式 |
| `proto.ClusterManifestSchemaVersion` | 1 | ✓ | ✗ | 仪式 |
| `proto.ClusterGrowSchemaVersion` | 1 | **✗**（`ClusterGrowReq/Resp` 根本没有 SchemaVersion 字段） | ✗ | **纯死常量** |
| `schema.AuditSchemaVersion` | 1 | ✓（每条 audit 的 `v`） | ✗ 无 decoder dispatch | 仪式（append-only 契约，尚可） |
| adminsock 4 个报告的 `SchemaVersion` + `cmd/tether` 若干 JSON 信封 | 1 | ✓（**硬编码字面量 `1`，连常量都没有**：`broker/incident.go:97`、`broker/homes.go:17`、`cmd/tether/node.go:97`、`cmd/tether/cluster_status_nats.go:56`） | ✗ 仓内无读者 | 见下 |

`ClusterHealthSchemaVersion` 的注释（`alerts.go:6-11`）自己交代得很清楚：
> v2 (C3) adds … v3 (G5/G7b) adds … v4 (G4) adds … v5 (R7b) adds …
> **No consumer gates on this value — decoding is omitempty-additive — so it is a documentation
> ledger, not a compat switch.**

即：**bump 了 4 次、写了 6 行版本沿革注释、每个 broker 每次 health tick 都把这个数字塞上线，
而没有任何代码分支依赖它。**

adminsock 那批是**合理的**：`docs/usage.md:1550`、`docs/broker-ops.md:438,448`、`docs/cluster.md:160`
把 `(schema, schema_version)` 写成对外部监控的稳定契约，所以"只写不读"是设计意图（读者在仓外）。
但它们全是硬编码 `1`，没有常量、没有测试断言"删字段必须 bump"——bump 政策只存在于 `usage.md` 的散文里。

**为什么是债**
- 主要危害是**误导**而非体积：`ClusterHealthSchemaVersion` 的注释读起来像一份兼容契约，
  下一个人 bump 到 6 会以为自己保护了兼容性，实际什么都没发生。真正保护兼容的是
  `omitempty` + `TopoReported`/`ProxyHomeReported` 这类"我报告过没有"的布尔哨兵
  （`alerts.go:44,100` 那套才是真机制）。
- `ClusterGrowSchemaVersion` 是纯噪声：一个既不上线也不被读的常量，只因为"新 wire 文件都要有一个
  SchemaVersion"这个仪式而存在。

**建议**
- 删 `ClusterGrowSchemaVersion`（3 行）。
- `ClusterHealthSchemaVersion`：要么删（连同 `ClusterHealthResp.SchemaVersion` 字段——它每个
  health tick 都上线），要么把注释改成一句话说明它是纯 ledger、不要再 bump。倾向后者（字段已在
  wire 上，删它虽然 additive-safe 但没收益）。
- adminsock/CLI 的硬编码 `1` 提成命名常量（`incidentSchemaVersion` 等），并加一条 golden 测试：
  对外契约的 JSON 键集变了就红灯（现在删一个键不会有任何测试反对）。

**量化**：删 ~10 行常量；改 ~15 行注释；新增 ~40 行 golden 键集断言。

**风险**：low。删 `ClusterGrowSchemaVersion` 零风险（无引用）。

---

### F6 — 明确的死 wire 面：3 个类型、5 个 subject、11 行 ACL、1 条 ~225 行的 op 路径 · severity: medium

**证据**（全部经过 全仓 grep + 排除测试 验证）

**死类型（生产引用 = 0）**
| 类型 | 位置 | 状态 |
|---|---|---|
| `ErrorReply` | `messages.go:255-261` | 声称是 "the canonical shape for any req that responds with an error"，**零生产者、零消费者**，只被 proto 自己的 roundtrip 测试引用。实际上 24 个响应类型各自内联 `Code`+`Error` 字段。这是"抽象声明了但没人采纳"的教科书例子。 |
| `RehomeDirective` | `messages.go:192-206` | **全仓零引用**（含测试）。注释写 "NOT YET WIRED (D7) … **a guard test asserts it has no live publisher** so a half-wiring is caught (review A5 M5)" ——搜遍全仓**不存在这样的 guard test**。D7 早已完成、项目已到 v0.4.7、G 系列 epic 都做完了，这个"为 D7 预留 wire 稳定性"的类型从未被使用。 |
| `ProxySubListReq` | `messages.go:1077` | 零引用（含测试）。CLI 走 `proxy.go:74` 直接发空 body。 |

**死 subject API（生产引用 = 0）**
| 符号 | 说明 |
|---|---|
| `SubjCtrlBy` (`subjects.go:52`) | 通用构造器，被 9 个专用构造器完全取代 |
| `SubjNodeUnregister` (`subjects.go:74`) | `unregister.req` 无发布者、无订阅者、无消息类型。`broker/audit.go:37` 亲口写 `agent_unregistered` "have NO producer in v1" |
| `SubjEvNodeState` (`subjects.go:82`) | `ev.node.<nid>.state` 从未发布 |
| `SubjPtyReady` (`subjects.go:203`) | ready 实际走 `RunChunk{Kind:"ready"}` 的 reply inbox |
| `SubjVersionAnnounce` (`subjects.go:10`) | 从未发布、从未订阅 |

**死 ACL grant（11 行，分布在 4 个 JWT 模板里）**
| 授权 | 位置 | 对应实现 |
|---|---|---|
| `session.<sid>.kick.req` | `auth/permissions.go:89` | 全仓无 |
| `session.<sid>.rotate-pin.req` | `auth/permissions.go:90` | 全仓无 |
| `s.<sid>.node.*.tag.req` | `auth/permissions.go:93` | 全仓无 |
| `ctrl.version.announce` | `permissions.go:35,136,227` + broker pub | 全仓无 |
| `node.<nid>.unregister.req` | `permissions.go:170`（agent pub）、`:250`（broker sub） | 全仓无 |
| `pty.*.ready` | `permissions.go:139`（member sub）、`:175`（agent pub） | 全仓无 |

成因清楚：`docs/architecture.md:118-135` 的规范 subject 树里就列着 `kick` / `rotate-pin` / `tag` /
`unregister` / `version.announce`——**ACL 模板照着这棵树写全了，动词从来没实现，两边都没清理**。

**死 op 路径（~225 生产行）**
`adminsock.OpClusterAdd` 自 v0.4.2 起被**故意不路由**（`protocol.go:150-154`），CLI 在 C8 已删除。
但它整条实现还在：
- `broker/clusterstatus.go:695-696` case → `handleAdd` (807-878, 72 行)
- `versionSkewResponse` (879-902, 24 行) — 即 F3 里那个失效的版本闸
- `splitJoinToken` (903-913, 11 行)
- `broker/clusteradmin.go:227-304` `AddNode` (78 行) + `IssueJoinNonce`/`claimJoinNonce`/
  `releaseJoinNonce` + `issuedNonces` map (~40 行)
- adminsock wire 字段：`Request.{NodePub, JoinToken, TunnelAddr, PublicHost, JoinerProto, JoinerRelease}`
  6 个 + `Response.Nonce` 1 个——**逐个验证过，唯一读者都在 `handleAdd`/`versionSkewResponse` 内**
- 保活它们的是 `b6_skew_test.go` / `cluster_c8_hints_test.go` / `d9` 集成 harness

**为什么是债**
- ACL 那 11 行是**最小权限原则的漂移**：今天不可利用（没人服务那些 subject），但每张签发的 JWT 都
  多带三个不存在动词的 pub 权限。真正的成本是**可读性**：想回答"一个 member 能干什么"必须先
  grep 每条 grant 有没有对应实现——审计者做不到"读模板即读规范"。
- `OpClusterAdd` 路径的成本不是 225 行，是它**藏着一个看起来在工作的版本闸**（F3）。有测试、绿的、
  永远不执行。这比没有闸更糟：它让人以为有闸。
- `RehomeDirective` 的注释宣称有 guard test 而实际没有——同样是"以为有保护"。

**建议**
1. 删 `ErrorReply` / `RehomeDirective` / `ProxySubListReq` / 5 个死 subject builder / 11 行死 ACL。
   同步删 `architecture.md` subject 树里的 `kick`/`rotate-pin`/`tag`/`unregister`/`version.announce`
   **或**加上 "(planned, not implemented)" 标注——两边必须一致。
2. `OpClusterAdd` 整条路径：先做 F3（把版本闸搬到 `JoinBundle`），然后删。删之前跑一次
   `test/d9` 确认 harness 改用 `OpClusterJoinApprove`。
3. 加一条 guard test：断言每个 JWT 模板里的每条非 `$JS`/`$O`/`_INBOX` grant，其 leaf 都能在
   proto 的 subject builder 集合里找到对应项。~50 行，一次性关掉"ACL 领先实现"这个类。

**量化**：净删 ≈ **280 行生产代码**（proto ~30 + auth 11 + broker ~225 + adminsock 7 字段）
+ ~120 行测试。占 lane 生产代码的 ~8%。

**风险**：low（proto/auth/subject 部分，无引用）；medium（`OpClusterAdd` 路径，需先补 F3 且要动 d9 harness）。

---

### F7 — `adminsock.Request/Response` 是 40+30 字段的 god-union，且契约注释已过期 · severity: medium

**证据**
- `internal/adminsock/protocol.go:173-259` — `Request`，**40 个字段**，30 个 Op 共用；除 `Op` 外全部 omitempty
- `:265-337` — `Response`，**30 个字段**，其中 14 个是 per-op 报告的指针/切片槽
  （`Evict *EvictResult`、`Runtime *RuntimeReport`、`Alert *AlertResult`、`Cluster *ClusterStatusReport`、
  `Homes`、`ProxyRebalance`、`ForceSingle`、`QuorumProj`、`Backup`、`Incident`、`Ops`、
  `Sessions`、`Nodes`、`Audit`/`Events`）
- **契约注释已过期**（`:261-264`）：
  > Response is the on-wire reply. **Exactly one of Sessions / Nodes / Audit / Evict is populated**
  > based on the request op.

  写于只有 4 个 op 的时代；今天有 30 个 op / 14 个 payload 槽。
- 字段↔op 的对应关系**只存在于注释分组**（`// Audit args` / `// Evict args` / `// D7 cluster args` …），
  没有任何编译期或运行期关联；`server.go` 按 `req.Op` switch 后自行读它需要的字段。

**为什么是债**
- 加一个 admin verb 需要：Op 常量 + `clusterOps` map 一行 + Request 加若干字段 + Response 加一个槽 +
  backend 方法 + CLI 命令 + 可能的 Code 常量 = **6–7 处**。字段永远只增不减（谁敢确定某个字段没人用？
  ——本次审计花了单独一轮 grep 才证明 6 个字段是死的，见 F6）。
- Response 的 14 个槽让客户端无法从类型上知道哪个被填了；`cmd/tether` 侧每个命令自己 nil-check
  自己关心的那个槽。加一个新槽时忘记在某条路径上填，表现为客户端拿到全零报告而不是错误。

**为什么我不建议大改**
这是 root-only 的本地 Unix socket，客户端和服务端**是同一个二进制**（`tether admin` 跑在 broker 主机上）。
union 让 client/server 的编解码保持极简（`client.go` 只有 56 行）。改成 `Op + json.RawMessage` 的
per-op typed payload 会增加代码量、增加一层 marshal，收益主要是编译期关联——对一个每天调用几十次的
运维接口不划算。这符合项目"安全实用主义"的取向。

**建议**（低成本部分）
1. **改掉过期的契约注释**（`:261-264`）——现在它主动误导。
2. 删掉 F6 里那 6 个死字段 + `Response.Nonce`。
3. 若要进一步：给 Response 加一个 `Payload string` 判别符（写哪个槽就写哪个名字），
   ~10 行，让客户端能断言"我拿到的是我要的那种报告"。这是可选的。

**量化**：删 7 个字段（~12 行）+ 改 4 行注释。结构不动。

**风险**：low。字段是 omitempty，删除对同版本 client/server 无影响。

---

### F8 — `internal/auth` 里的 `subjectPrefix` 重复，其"import cycle"理由经验证不存在 · severity: low

**证据**
- `internal/auth/permissions.go:5-11`：
  > subjectPrefix is duplicated from internal/proto.SubjectPrefix **to avoid pulling proto into
  > internal/auth (and through it the ed25519 / jwt chain into proto's identifier validation)**.
  > `const subjectPrefix = "tether.v2"`
- 实测 `go list -f '{{.Imports}}' ./internal/proto` → `[fmt regexp strings time]`。
  **proto 零 module-internal 依赖，零 ed25519/jwt 依赖。**
- `go list -f '{{.TestImports}}' ./internal/proto` → 同样不含 auth。测试图里也没有环。
- 更有意思的是**这个包自己的 guard test 注释已经承认了**（`permissions_test.go:183-186`）：
  > The non-test permissions.go deliberately duplicates `subjectPrefix` to avoid importing proto
  > (cycle through proto's ed25519/jwt identifier validation), but this _test.go is free to import
  > proto (**proto does NOT depend on internal/auth — verified**).
- 为维持这个不必要的重复，代价是 3 个工件：
  (1) `permissions.go` 的常量 + 6 行解释注释；
  (2) `permissions_test.go:180-199` 的 `TestSubjectPrefixInSyncWithProto`；
  (3) `test/determinism/lint_skeleton_test.go:264-266` 白名单里的一条豁免。

**为什么是债**
不是体积问题（~20 行），是**信号问题**：仓库里唯一被允许偏离 SSOT 的地方，理由是错的。
下一个人读到"proto 会拉进 ed25519 链"会以为 proto 是重包，可能因此在别处也复制常量。
而且白名单一开，`TestNoStrayVersionLiteral` 对 `permissions.go` 就完全失效——那个文件里
`tether.v2` 出现在第 11 行和第 73 行的注释里，将来有人在那儿硬编码第二个前缀不会被抓。

**建议**
`internal/auth` 直接 `import "…/internal/proto"`，`const subjectPrefix = proto.SubjectPrefix`
（或全部替换为 `proto.SubjectPrefix`），删 guard test，删 determinism 白名单条目。

**量化**：删 ~25 行（常量注释 20 + guard test 20 - 新增 import 1），并让 `TestNoStrayVersionLiteral`
的覆盖率从 "internal+cmd 减一个文件" 变成 "internal+cmd 全覆盖"。

**风险**：low。需要确认 `internal/proto` 不会未来引入 auth（加一条 `go list -deps` 断言即可）。

---

### F9 — subject 语法在 builder / subscriber / parser / test 四处各写一遍；两个 parser 在 proto 之外 · severity: low

**证据**
以 `cmd.by.<A>.node.<N>.<verb>.req` 这一条语法为例，它被独立表达了 4 次：
1. 构造：`proto/subjects.go:57` `fmt.Sprintf("%s.s.%s.cmd.by.%s.node.%s.%s.req", …)`
2. 订阅：`broker/broker.go:965` 手工拼 `proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.exec.req"`（×10）
3. 解析：`proto/subjects.go:236` `ParseCmdBy`，位置索引 `parts[0..10]` 逐个断言
4. 测试：`proto/proto_test.go` 的字面量期望串

`ctrl.by.<A>.…` 同理（builder 9 个 + 订阅 8 条 + `ParseCtrlBy`/`ParseCtrlProxy`）。

**更值得注意的是 proto 之外的两个 parser**（`strings.Split(msg.Subject, ".")` 全仓仅 3 处，
一处是 `cmd/tether/history.go:605` 的 `lastSubjectToken`，无害；另两处是真解析）：
- `internal/agent/exec.go:45-51` — `dispatchForwarded` 手写解析 `req.forwarded` 语法
  （`len(parts)!=10 || parts[8]!="req" || parts[9]!="forwarded"`，`verb = parts[7]`）。
  proto 有 `SubjCmdForwarded` **构造器但没有对应的 parser**，所以消费方只能手写。
  而且这个手写解析**不校验版本段**（不检查 `parts[1] == SubjectVersionToken`），
  也不校验 sid/nid 语法——与 proto 里 7 个 parser 一致做 `ValidateSID/NID/ActorToken` 的纪律相悖。
- `internal/broker/run.go:202-206` — `handlePtyFailed` 手写解析 `s.<sid>.pty.<pid>.failed`
  （这个倒是校验了 `parts[1] != proto.SubjectVersionToken`）

**为什么是债**
- 改一条 subject 布局（例如给 forwarded 树加一段），proto 的构造器 + parser + 测试会一起红，
  但 `agent/exec.go` 的手写索引**不会**——它只在真跑起来时表现为 "unknown forwarded verb" 警告。
  这是 agent 控制面最热的那条路径。
- 我**不**建议把 40 个具名 builder 合并成通用 builder：具名 builder 是可 grep 的、类型友好的，
  合并成 `SubjCtrlBy(actor, "s."+sid+".ps.req")` 只会更糟。这一处的设计是对的。

**建议**
在 proto 加 `ParseCmdForwarded(subject) (sid, nid, verb string, ok bool)`（~15 行，与
`ParseCmdBy` 同形），`agent/exec.go` 改调它。顺带获得版本段和 sid/nid 校验。

**量化**：proto +15 行，agent -8 行。净 +7 行，换来 forwarded 语法的单一真相 + 版本段校验。

**风险**：low。行为等价或更严格（更严格意味着一个畸形 subject 从"警告"变成"丢弃"——
但订阅本身已经限定了前缀，实际不可能收到）。

---

### F10 — 文档与实现的 wire 层漂移（tunnel 包注释、architecture.md subject 树） · severity: low

**证据**
- `internal/tunnel/tunnel.go:17` 包 doc 写：
  `2. agent writes:  REGISTER <sid> <nid> <port> <token>\n`（**5 字段**）
  而 `tunnel.go:1245-1251` 的 `parseRegisterLine` 自 D6 起要求**恰好 6 字段**
  （多一个 epoch），并显式拒绝 5 字段：
  > D6 §7.2(b): the v2 REGISTER grammar is EXACTLY 6 fields … A 5-field (pre-D6) or 7-field line is rejected
  包 doc 描述的是被代码明确拒绝的格式。
- `docs/architecture.md` 里 **70 处** `tether.v1`，包括 B.1 那棵"顶层四段分层"的规范 subject 树
  （`:118-135` 及后续），而 wire 自 D0 起是 `tether.v2`。
  讽刺的是仓库有一条 AST tripwire（`TestNoStrayVersionLiteral`）**禁止生产代码出现游离的
  `tether.vN` 字面量**，却对作为"实现的尺"的架构文档没有任何约束。
- 同一棵树还列着 3 个从未实现的动词（`kick`/`rotate-pin`/`tag`）和 1 个从未实现的 subject
  （`version.announce`、`unregister.req`）——这正是 F6 里那 11 行死 ACL 的来源。

**为什么是债**
CLAUDE.md 把 `architecture.md` 定为"实现的尺"，每进入新 phase 要过它的 checklist。
一把刻度写着 v1、且列着 5 个不存在动词的尺子，会持续制造 F6 那类"ACL/常量领先实现"的偏差——
本次审计发现的死 ACL 就是上一轮照着这棵树写出来的。

**建议**
- 把 `architecture.md` 的 subject 树整体改成 `tether.v2`（或改成 `tether.<SubjectVersionToken>`
  这样的占位，避免下次 bump 又要全文替换），未实现动词标 `(planned)` 或删除。
- 修 `tunnel.go:17` 的包 doc 为 6 字段。
- 可选：把 `TestNoStrayVersionLiteral` 扩展到扫 `docs/*.md`（把当前版本以外的 `tether.vN` 报红）。

**量化**：文档改动，代码 1 行注释。

**风险**：low。纯文档。

---

## 反证：做得好的地方

1. **体量本身完全正当。** proto 非测试 2,042 物理行里只有 **956 行代码**（53% 是注释）；
   `messages.go` 1,095 行 → **465 行代码**。86 个 wire 类型 ÷ 465 行 ≈ **5.4 行/类型**——
   这已经接近 Go struct 定义的物理下限（`type X struct {` + 字段 + `}`）。**没有一行可以靠
   "写得更紧凑"省下来。** 三个包合计 1,778 行代码覆盖 5 套传输面。这个 lane 与"屎山"无关。

2. **版本前缀的 SSOT 是真做到了，而且是全仓最扎实的一处工程。**
   `SubjectVersionToken`/`SubjectPrefix` 单点定义 → 40 个 builder + 7 个 parser 全部派生 →
   `TestProtoVersionStillPositive`（`proto_invariants_test.go:393`）断言
   `SubjectVersionToken == "v"+itoa(ProtoVersion)` 且 `SubjectPrefix == "tether."+token` →
   `TestNoStrayVersionLiteral` 用 AST（不是 grep，扫的是 `*ast.BasicLit` STRING 节点，
   所以注释里的 `tether.v1` 不会误报）扫 `internal/`+`cmd/` 禁止游离字面量 →
   **还有 `TestNoStrayVersionLiteralSelfCheck` 证明这个扫描非空洞**。
   带自检的 lint tripwire 在任何代码库里都是少数派做法。

3. **零手写编解码，零重复序列化实现。** 全仓 **0 个** `MarshalJSON`/`UnmarshalJSON`，
   全部走 `encoding/json` 反射。没有"同一结构手写一遍 + 反射一遍"的经典债。
   （唯二的手写编码是 `CanonicalUpgradeReqBytes`/`CanonicalGrowReqBytes` 的签名字节串——
   那是**必须**手写的：签名需要确定性字节序，Go map 迭代不确定，用 JSON 签名是经典漏洞。
   而且两者都带域分隔前缀 `tether-cluster-grow-v2\n` 防跨类型签名重放。这是正确做法。）

4. **所有 proto parser 都 fail-closed 且做语法校验。** `ParseCmdBy`/`ParseCtrlBy`/`ParseCtrlProxy`/
   `ParseEvProc`/`ParseEvTransfer`/`ParseTransferFinalize`/`ParseSidNidFromCtrl` 一律先断言
   **精确 token 数** + 固定段字面量，再跑 `ValidateSID`/`ValidateNID`/`ValidateActorToken`，
   **不把 malformed token 当 opaque 字符串放行给 handler**（`subjects.go:236-242` 注释明说这是
   "audit shard 03 F5 — defense in depth"）。`ValidateClusterNodeID`（`identifiers.go:78-88`）
   更进一步拒绝 `-` 开头的 option-like id，理由是它会被渲染进操作员复制粘贴的命令行——
   这是把威胁模型想到位了才会写的校验。

5. **additive/omitempty 的加字段纪律高度一致，而且是"指针 + omitempty 保证字节不变"这个正确做法。**
   `NodeRegisterResp.Proxy/Home/Roster` 全是 `*T` + omitempty，注释逐个说明
   "so a single-node broker produces a NodeRegisterResp **byte-identical** to today"
   （`messages.go:130-148`）。`ExposeReq.RebuildOff` 甚至**刻意把布尔取反**
   （编码"非默认"的那一侧），让"字段缺失"解码成今天的行为，并且明说
   "a dropped --no-rebuild fails toward ON — the SAFE direction"（`messages.go:581-592`）。
   这种"默认值方向要选安全的那侧"的思考在很多项目里是没有的。

6. **`ReasonHomeCatchingUp` / `CodeLeaderUnavailable` 提到 proto 做跨包共享常量是范例。**
   `messages.go:208-227` 两段注释解释得很到位：这两个字符串一侧是 broker emit、
   一侧是 agent classifier，"a duplicated literal would let one side drift and
   **permanently brick a fleet**"。作者显然知道字符串契约该怎么治——F1 说的正是
   这个正确做法没有系统化推广到另外 65 个 code。

7. **`adminsock` 刻意保持 leaf 包，用依赖倒置接住 broker。**
   `ClusterAdminBackend interface { HandleCluster(Request) Response }`（`protocol.go:645-647`）+
   注释 "adminsock stays a leaf (it imports neither internal/cluster nor internal/broker —
   the adapter translates to these wire types)"。30 个 op 的复杂后端没有污染 wire 包。

8. **`clusterroster` 的 schema 版本是唯一闭环的一处，而且做对了方向。**
   `roster.go:130` / `seeds.go:97` 是 `if r.SchemaVersion > ProtoConst { reject }`——
   **拒绝更新的、接受更旧的**，这是签名 artifact 的正确方向（旧 roster 仍可验、
   未来 roster 不敢猜）。

9. **`OpClusterAdd` 的退役方式是负责任的。** `protocol.go:150-154` 没有偷偷留一条后门，
   而是把它从 `clusterOps` 路由表里摘掉并写明理由（"Its backend (AddNode) does a DIRECT AddVoter,
   which can wedge an N=1 cluster … Dropping OpClusterAdd here closes the last reachable
   direct-AddVoter admission path"）。F6 说的是"摘掉之后代码没删干净"，
   而**这个决定本身以及对它的记录**是这个仓库文化里好的那一面。

10. **proto 包 0 处 TODO/FIXME**，且注释不是废话——它们记录的是**为什么这么设计**
    （NATS `*` 不能匹配部分 token 所以 bucket 必须 per-session、
    epoch 单标量无法区分"陈旧的低值"和"权威的 restore" 所以需要 (Generation, Epoch) 对……）。
    48% 的注释率在别的项目里是坏味道，在这里是资产：这些是审计中最难自己推导出来的信息，
    而且大部分附了具体的失败模式（"round-1 BLOCKER: empty replies ⇒ a false broker_down
    for every voter every tick"）。**如果按"可执行代码行"重新统计全仓，
    68,328 行生产代码的真实规模会显著低于表面数字。**

---

## 本质 vs 偶然复杂度拆解

**本质（约 80%，~1,420 / 1,778 代码行）**

这个 wire 层要同时服务 5 个互不兼容的传输面，每一个都是产品需求逼出来的，不是设计选择：

| 传输面 | 为什么必须存在 | 代码占比（估） |
|---|---|---|
| NATS 控制面（proto，86 类型） | NAT 穿透的全部意义就是"agent 反连、ctl 经同一 bus 路由"——subject 树 + JSON 消息是这个架构的直接产物 | ~54% |
| adminsock（Unix socket） | 集群运维必须在 broker 主机上、root-only、**不经 NATS**（NATS 本身可能就是坏的那个东西）。用同一套 NATS RPC 做运维会在 quorum 丢失时无法自救 | ~23% |
| tunnel 行协议 | 数据面必须与控制面分离（架构不变量），公网 TCP → agent 本地端口不能走 NATS | ~7%（在 lane 外） |
| raft command 编码 | 复制日志的编码版本与 NATS wire 版本**必须解耦**（`command.go:203` 说明得很清楚：bump command 版本会 per-replica 毒化日志，只能 flag-day 重装；bump proto 只影响 subject 语法） | 在 lane 外 |
| 签名 roster/seeds/manifest | agent 冷启动发现必须不依赖静态 broker_url，且必须可验证——账户签名 + 独立 generation 计数器是最小方案 | ~9% |

再往下拆 86 个类型：session 生命周期 7、node 生命周期 6、exec/run/PTY 11、expose 8、
node upgrade 4、文件传输双 tier 11、proxy 订阅 13、集群 HA（roster/seeds/manifest/health/
grow/upgrade/alerts/home）20、其余 6。**每一族都对应一个用户可见的命令族**。
删掉任何一族等于删掉一个产品能力。

指出一个**边界情况**：cluster HA 那 20 个类型 + adminsock 30 个 op 中的 22 个 cluster op，
合计约占 lane 的 40%，服务的是一个当前实际拓扑为 **1 broker + 6 agent、且长期处于
force-single N=1** 的车队（见 `project-racknerd-forcesingle-js-incident`）。
这是**范围选择**问题而非代码质量问题——HA 能力一旦决定要做，这 40% 就是它的合理成本。
本审计不建议删，但值得记录：这个 lane 里"最贵的一半代码"服务的是一个至今没有真正跑过 N≥3 的能力。

**偶然（约 20%，~360 行）**

| 项 | 行数 | 可消除性 |
|---|---|---|
| `OpClusterAdd` 死路径（含它藏着的失效版本闸） | ~225 | 可删（需先做 F3） |
| 死类型 / 死 subject builder / 死 ACL / 死 schema 常量 | ~55 | 直接可删 |
| adminsock 6 个死 Request 字段 + `Response.Nonce` | ~12 | 直接可删 |
| 版本仪式（`ClusterGrowSchemaVersion` + 无读者的 SchemaVersion 字段与沿革注释） | ~40 | 部分可删 |
| `auth` 的 `subjectPrefix` 重复及其护栏（常量+注释+guard test+lint 白名单） | ~25 | 直接可删 |

**关键判断：偶然复杂度的主体不是"多写的行"，而是"没建立的关联"。**
把 F1（动词/错误码 SSOT）、F2（路由策略与订阅声明合并）、F4（不变量完备性测试）这三条做掉，
新增代码约 130 行、删除约 300 行——**净减 170 行，同时把三类目前只靠人工纪律维持的契约
变成机器强制**。这是这个 lane 投入产出比最高的三件事。

对已发布生产工具的风险提示：**本报告的全部建议里，只有 F3 触碰 wire**
（给 `cluster.JoinBundle` 加两个 omitempty 字段）。`JoinBundle` 是操作员 OOB 携带的一次性
base64 串，不涉及任何长连接或持久化格式，**旧 bundle 解码成 0/"" 仍走今天的 allow+warn 语义**，
因此 **不需要现网重装、不需要 ProtoVersion bump**。其余 9 条全部是删死码、改注释、加测试。
