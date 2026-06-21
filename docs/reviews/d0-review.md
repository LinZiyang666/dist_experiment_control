# D0 内审报告（distributed-broker HA, proto v2 地基）

> 来源 = 6-agent 对抗审查 workflow（5 维度审查 → 1 综合）。维度：deps+PreVote / proto flip 完整性 / migrations DDL / lint+scope+文档保真 / 测试严谨性。专家**只读实现、可建议新增测试，绝不改实现**；主进程逐条处置并落盘。
>
> **整体结论：pass-with-fixes**。D0「零运行期行为变化」目标证据充分且正确（deps CGO-free 且承重、PreVote 门非 flaky 且有真判别对照、proto v2 flip 产品代码零 stray `tether.v1`、migrations 契约保真且无 D1+ scope creep）。**全部 finding 为测试质量 / 提交卫生，无需改实现**。专家独立对 HEAD 复核每条 finding，无误报。
>
> 一位 reviewer（lint-scope-doc）中途 API 连接中断，但其 finding 已被综合者纳入（M1/n2 等）。

---

## 专家 findings 与主进程处置

### Major

| ID | finding | 主进程处置 |
|---|---|---|
| **M1** | golden fixtures `internal/proto/testdata/golden/` 未入 git → 干净 checkout 必红 | **采纳**：`git add internal/proto/testdata/golden/`（已纳入工作区索引；提交时随 D0 一并落盘）。 |
| **M2** | `TestSubjectPrefixInSyncWithProto` 是同义反复（`subjectPrefix != "tether.v2"` 字面比字面），非对 proto SSOT 的交叉校验——plan §4 明确要求 `subjectPrefix == proto.SubjectPrefix` | **采纳**：测试文件内 import proto，改断言为 `subjectPrefix != proto.SubjectPrefix`（proto 不依赖 auth，_test.go 无环）。 |
| **M3** | 7 个被 sweep 的 parser 中，`ParseTransferFinalize`/`ParseEvTransfer`/`ParseCtrlProxy` 缺「v1 前缀 wrong-version」负向用例，其 `parts[1]!=SubjectVersionToken` 分支从未被测 | **采纳**：三者各加一条结构合法但 v1 前缀的 `ok==false` 负向（T1）；并加全 7 parser 的表驱动 v1-reject（T3）。 |

### Minor（migrations 覆盖硬化，趁表新一并补）

| ID | finding | 主进程处置 |
|---|---|---|
| **m1** | 0010 ALTER 后 FK `ON DELETE CASCADE` 仅靠 `foreign_key_check` 断言，未真触发级联 | **采纳**：T4——删父 `nodes` 行，断言 `port_allocations` 子行被级联删。 |
| **m2** | `cluster_meta` KV 约束（`value NOT NULL`、`key PK`）完全无测试 | **采纳**：T5——NULL value 拒、dup key 冲突、upsert 往返。 |
| **m3** | `idx_alerts_dedup_active` partial-WHERE 未对「两条 CLEARED 同 dedup_key」加测 | **采纳**：T6——两条 CLEARED 同 key 均成功。 |
| **m4** | `home_broker` 默认证了 `==''` 但未证 `!= NULL`（Scan 进 string 会把 NULL 也读成 ""） | **采纳**：T7——`COUNT(*) WHERE home_broker IS NULL == 0`。 |
| **m5** | `idx_port_alloc_home_active` 仅按名断言存在，未证非唯一 | **采纳**：T8——两条 ALLOCATED 同 home 不同 port 均成功 + DDL 无 `UNIQUE`。 |

### Nits

| ID | finding | 主进程处置 |
|---|---|---|
| **n1** | `FileSnapshotStore` 按类型 `_ *raft.FileSnapshotStore` 钉，非 §19 点名的构造器 `NewFileSnapshotStore`（在 deps_test.go 非 doc.go） | **采纳（benign deviation + belt）**：保留类型钉（D1 消费的就是该 struct，更durable），**另加** `var _ = raft.NewFileSnapshotStore` 满足 §19 字面点名；登记偏离（raft 为 direct require 是 deps_test.go 直接 import 的必然结果，更新 plan §2 注）。 |
| **n2** | L-2 raft-confinement 白名单在 D0 是死分支（`walkGoFiles` 已跳 `_test.go`，唯二 raft importer 都是测试文件）；白名单为 D1 前向 | **采纳（注释澄清）**：白名单分支加注「D1 前向、对 D0 无效；当前是 `_test.go` 排除允许 cluster 测试 import raft」。 |
| **n3** | `node_register_resp_empty_slices` golden 注释声称锁「[] vs null」，但字段都 `omitempty`，`[]int{}` 与 nil 编码一致（都省略）——注释过度声称 | **采纳（注释修正）**：改注释为真实保证「锁这些 omitempty 字段省略空值；捕获 omitempty 被误删」。 |
| **n4** | `TestNoStrayVersionLiteral` 只扫单 string-literal，抓不到拼接/格式化的版本串（`"tether.v"+"1"`、`Sprintf`） | **采纳（接受局限 + 自校验）**：D0 接受（flip 后零 off-SSOT 拼接）；加 T11 正向自校验证 tripwire 非 vacuous；拼接检测留 D2。 |
| **n5** | phase/kind CHECK 负向表漏空白近似值（`' VOTER'`/`'VOTER '`/`'VOTER\t'`） | **采纳**：T10——加空白近似负向。 |
| **n6** | `applyThrough` 硬编码 `0007_proxy_generation.sql` 边界，未来插入 0007.5 会静默改基线 | **采纳**：T12——断言 migration 序连续 `[0001..0010]`。 |

### 可选硬化（采纳的）

- **T13** PreVote DISABLED 对照 term 抬升有意义裕度（`>= termBefore+3`，实测达 21）——**采纳**。
- **T14** 钉 raft v1.7.3 依赖行为：Disconnect 的 InmemTransport `RequestPreVote` 返回的 err **不含** `unexpected command`（askPeer 把该 sentinel 当 GRANTED）——**采纳**（门静默依赖此行为）。
- **T15** heal 后断言**同一节点**仍是 leader（`LeaderWithID` 身份连续），非仅 `State()==Leader`——**采纳**。
- **T16** file-backed 重启幂等（Open→close→reopen→`applyMigrations` no-op）——**采纳**（覆盖内存测试漏的 close/reopen 路径）。
- **T18** 钉 `proto.SubjCmdForwarded(sid,nid,"*")` == `tether.v2.s.<sid>.cmd.node.<nid>.*.req.forwarded` + 匹配 broker 发布通配——**采纳**（钉 agent.go:477 swap）。
- **T17**（go mod tidy 后 raft-boltdb 仍在 require 的 CI/make 守卫）——**记录、暂不实现**（deps_test.go 的编译引用已是机制；CI 守卫为额外保险，避免 Makefile churn，留作后续）。

**驳回**：无。所有 finding 均为有效的测试质量改进，全部采纳（仅 T17 记录不实现、n1/n4 采「接受+补强」）。

---

## 结论

D0 交付了正确、运行期等价的 proto v2 地基，**无 D1+ scope creep**（无产品 FSM / Apply 路径 / BoltStore 接线 / 6-field REGISTER / cert_pins wire 破坏 / cluster_revoked_identities；leader-baked 时间戳；phase 全大写枚举）。运行期契约成立。所需修复全在**测试 + 提交 `git add`**，不触实现。主进程已逐条整合（见上）。整合后复跑 `make test` + lint + PreVote `-race` 全绿，再交外审。
