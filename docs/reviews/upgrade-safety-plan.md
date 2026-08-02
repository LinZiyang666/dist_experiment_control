# upgrade-safety 叶子增量 · plan（定稿）

> 2026-08-01。阶段 A 产物：3 drafter（safety / wire / ops 视角）→ 3 critic（失败闭合 / 简单性 / 可测试性）→
> 1 synth 的对抗性 workflow 综合稿，经主进程核实事实并定稿。
> 用户裁定的范围：①更新规范入 doc（N-1 窗口、只向前、不追溯）；②让 N-1 从下一版起可执行的最小落点；
> ③升级安全全套现在做。安全实用主义：够用就好，绝不过度设计。
>
> **定稿修订**（主进程对综合稿的三处改动，理由见各节）：
> R1 `decideBoot` 只挂 agent 守护进程启动路径；R2 boot_budget 的论证按本仓真实 unit 参数改写
> （综合稿基于 systemd 默认 RestartSec=100ms 的"烧穿 start-limit"论证与本仓 RestartSec=2/5 不符）；
> R3 commit 与 watchdog 回退显式互斥。

## 0. 范围与刻意不做的事

**做**：
① `docs/requirements.md §6.7` 整节替换 + `docs/distributed-broker-architecture.md` 新增「版本兼容窗口与升级安全」节；
② additive 纪律的机械闸门（并入既有 proto golden 体系）+ 三处版本站点钉子测试；
③ agent 升级安全状态机（冒烟门 / prev 槽 / marker / 自回退）；
④ ctl `--wait` + `--all` 金丝雀；
⑤ 两个新 wire 错误码 + 两个 additive register 字段。

**刻意不做**（每条一句理由）：
- **不做任何运行时版本窗口判定**：subject 前缀内嵌 `v<ProtoVersion>`（`internal/proto/version.go`
  `SubjectVersionToken`），N-1 纪元的端发布在 `tether.v(N-1).*` 主题树上，双订阅落地前"接受集"分支不可达，
  是死代码假广告。三处精确相等站点（`broker.go:1375`、`broker/upgrade.go:63`、`clusterstatus.go:1179`）
  **一行不改**，只加 origin 注释钉住"为何是相等而非区间"。
- 不做 hello [min,max] 协商；不做 release 层运行时拒绝（advisory 字段也不做——无今日消费者）。
- 不接受 proto 1（追溯兼容被用户明令禁止，且 v1 端到不了 v2 主题树，纯死代码）。
- **不做 broker 持久化/schema 变更**：nodes 写入走 raft apply，`node.RegisterInput` 是
  `internal/broker/wire_freeze_test.go` 冻结的跨版本载荷，混版集群下新字段静默丢失 → FSM 发散；
  `ReleaseVersion` 已存在，ctl 轮询 `node.list` 即可观测升级结果。
- 不做 boot 期之外的行为级健康判定：**register 成功 = 提交点**是承诺边界；"起来连上但行为异常"
  的运维手段是金丝雀 + 手动指旧版本 URL 再升一次（回滚 = 一次普通 upgrade）。
- 不改三种 supervisor 单元、不为 setsid-nohup 造 watchdog（GAP 如实标注，见 §3.2 F4）；
  不动 `ReExecOnly`（co-located agent）路径；不动 broker `cluster upgrade`（已有 staging+sha+quorum）。
- 不做 `--canary <nid>` flag、逐台 wait、deadline 可调 flag、多代 prev、prev GC、失败自动重试
  （均无今日用例）。

## 1. 更新规范条文

**落点**：WHAT → `docs/requirements.md §6.7` 整节替换；HOW → `docs/distributed-broker-architecture.md`
新增「版本兼容窗口与升级安全」节（第 2 层绑定契约）。现行 §6.7"三端 major.minor.patch 全一致"
**从未被实现**（register 只查过 proto），本改写是让规范追认并收紧现实。

**§6.7 替换全文**：

> ### 6.7 版本兼容（N-1 窗口）
> - 兼容单位是 **release（git tag）**。每个新 release 必须与其**直接前一个 release** 在线互通：
>   ctl / broker / agent 任意两两混配，覆盖滚动升级与回滚的全部中间态。
> - **基线**：本条生效后发布的第一个 release 为兼容基线；不为更早历史版本提供任何追溯兼容。
> - **升级顺序恒为 broker 先、agent/ctl 后**；回滚顺序相反。
> - **wire 纪元（ProtoVersion）在窗口内冻结**：相邻 release 间一切 wire 变更必须 additive，
>   新增字段的**缺省零值必须是合法语义**；新增错误码必须能被 N-1 端的 default 分支优雅呈现。
> - **ProtoVersion bump = 纪元更替**，是唯一被许可的兼容断裂：必须重装，不得走 `node upgrade`
>   （`proto_bump_requires_reinstall` 是其执行点）。bump 时新 broker 必须同时订阅
>   `tether.v(N)` 与 `tether.v(N-1)` 两棵 subject 树并在请求方前缀上应答
>   （对未来 bump 者的义务，由钉子测试的失败信息指路）。
> - 握手仍以 ProtoVersion 精确匹配拒绝；release 偏差不做运行时拒绝——**回滚是一等公民**：
>   升级失败退回前一 release 的节点必须仍能 register 并工作，任何 release 层拒绝都会卡死回滚路径。

HOW 节内容：版本检查站点盘点表、additive 纪律与闸门位置、agent 升级状态机全图（§3）、
marker 文件格式、bump 者双前缀义务展开。

## 2. N-1 窗口机制（全在开发期，零运行时件）

1. **wire 字段清单闸门**：不新建平行 golden（`golden_test.go` 与 `wire_freeze_test.go` 已各守一段，
   第三套必漂移）。在 `internal/proto/golden_test.go` 同族新增 `TestWireFieldInventoryAppendOnly`：
   testdata 记录每个导出消息结构的 {字段名, json tag, 类型}，规则**只增不删不改**
   （删 / 改名 / 改 tag / 改类型即红）；updater 拒绝删除条目——与结构预算棘轮同一摩擦哲学。
   **不检查 omitempty**：JSON 未知字段本就被旧端忽略，真约束是"零值必须是合法语义"，
   那是条文 + 审查纪律，不假装机械。入 CLAUDE.md 闸门表 + `gate_registry_test.go` 登记。
2. **钉子测试**：三处精确相等站点表驱动钉死（接受 N、拒绝 N±1）；失败信息指向 HOW 契约的
   bump 者双前缀义务。
3. `xfer_provision.go` 已示范的"新码走旧端 default 分支"个案升格为 §1 条文。

## 3. 升级安全状态机

### 3.1 机制

- **冒烟门（rename 前）**：对 tmp 路径跑 `<tmp> version`（5s 超时），exec 失败或首行解析不出
  release tag → 拒绝，回 `smoke_failed`，磁盘零变更。首行格式 `tether v0.4.7 (proto v2)`
  （`cmd/tether/main.go` newVersionCmd），提取 `v0.4.7` 作为 `NewVersion`——**归一化为与 register
  上报的 `ReleaseVersion` 同一格式**，并用测试钉住该等式（`--wait` 判据依赖它）。
  `readVersionString` 从 best-effort 升格为门。
- **prev 槽**：`os.Remove(prev)` 清旧 → `os.Link(dst, prev)` 硬链接（同目录同 fs，零拷贝）→
  仍是**单次** `rename(tmp, dst)` 原子进位。**dst 在任意断点都在位**——否决三份草案初版的
  双 rename 方案（两次 rename 之间断电 → ExecStart ENOENT 永久失联，比今天更糟）。
  link 失败（个别 fs）降级为 copy。prev 路径 = `dst + ".prev"`。
- **install 入口门**：marker 处于 `pending` → 拒绝，回 `upgrade_in_progress`；
  防止 exec 前窗口内二次 upgrade 把唯一好二进制 clobber 成 prev。
- **marker**：`<dst 同目录>/.tether-upgrade.json`（tmp+rename 原子写），字段
  `{state: pending|committed|rolled_back|rollback_failed, prev_sha, new_sha, prev_version,
  new_version, deadline(绝对墙钟), boot_count, boot_budget:3}`。JSON 损坏 → 视为 idle 并告警。
- **exec 失败就地恢复**：`reExecInPlace` 失败分支**删掉 `os.Exit(1)`**——exec 失败瞬间旧进程映像
  仍在且是唯一存活的旧代码：`rename(prev, dst)` 恢复、写 `rolled_back`、旧进程继续跑不退出。
  这同时闭合 setsid-nohup 路径（"os.Exit + 下次启动接手"被评审证伪：回退代码住在起不来的二进制里）。
- **启动检查（纯函数，定稿修订 R1）**：抽成 `decideBoot(marker, selfSHA, now) → action`，
  **只挂在 agent 守护进程启动路径**（`tether agent` 的 RunE 进入 `Agent.Run` 之前）——
  绝不挂根 main：冒烟用的 `<tmp> version`、运维手跑的任何子命令都不得碰 marker，
  否则冒烟本身会消耗 boot 预算。分支：
  - `pending` ∧ self==new_sha ∧ boot_count<budget ∧ now<deadline → boot_count++ 落盘，继续启动，武装 watchdog；
  - `pending` ∧ (boot_count≥budget ∨ now≥deadline) → 校验 prev_sha 后 `rename(prev, dst)`、
    写 `rolled_back`、exec dst；
  - `pending` ∧ self==prev_sha（回退被打断后 supervisor 拉起的是已恢复的 dst）→ 收敛写 `rolled_back`，正常跑；
  - prev 缺失 / sha 不符 → 写 `rollback_failed`、响亮日志、以现状继续跑（不制造新循环）。
- **boot_budget=3 的论证（定稿修订 R2）**：本仓两个 unit 是 `Restart=always RestartSec=2`
  （`scripts/install.sh` system 层）与 `Restart=on-failure RestartSec=5`（`cmd/tether/agent.go`
  user 层）；按 systemd 默认 `StartLimitIntervalSec=10s / StartLimitBurst=5`，2s/5s 的重启间隔
  **不会烧穿 start-limit**——起来即崩的新二进制会无限循环。boot_budget=3 是 F4 的**真正闭合者**
  （第三次拉起时启动代码自回退，~15s 内收敛），deadline 是后备而非主闭合。
- **健康签到（定稿修订 R3）**：register 成功（`agent.go:688` 返回处）→ marker 写 `committed`。
  marker 为 pending 时武装 ctx 挂接的 register-deadline watchdog（默认 120s，常量不设 flag）：
  到期未 commit → 校验 prev_sha、恢复 prev、写 `rolled_back`、exec dst。
  **commit 与回退互斥**：marker 状态转换收敛到单一 owner（mutex + 当前态检查）——
  watchdog 触发后 register 迟到不再写 committed；已 committed 后 watchdog no-op。
- `ReExecOnly` 不进状态机（bin 目录 root-owned，cluster upgrade 已有自己的保护）。

### 3.2 失败模式 → 机制闭合表

| # | 失败模式 | 闭合机制 | 回退执行者 | systemd×2 / nohup |
|---|---|---|---|---|
| F1 | 下载坏（sha / 超大 / 404） | 既有 sha + size cap，安装前拒 | 旧进程（盘未动） | 三者皆闭合 |
| F2 | 装坏（垃圾 ELF / 坏架构 / 截断） | 冒烟门，rename 前拒 | 旧进程（dst 未动） | 三者皆闭合 |
| F3 | `syscall.Exec` 失败 | 就地恢复 prev、不退出 | 旧进程（唯一存活点） | 三者皆闭合 |
| F4 | 新二进制起来即崩 | supervisor 循环 + boot_budget=3 启动期自回退 | 新二进制启动代码 | systemd 闭合；**nohup GAP**（进程死无人拉起，与今日同险，文档标注不弥补） |
| F5 | 起来但 deadline 内 register 不成 | pending watchdog → 自回退 exec prev | 新二进制自身（不依赖 supervisor） | 三者皆闭合 |
| F6 | pending 期二次 upgrade | install 入口门 `upgrade_in_progress` | —（拒于入口） | 三者皆闭合 |
| F7 | 回退本身失败（prev 丢 / 坏） | prev_sha 校验失败 → `rollback_failed` 终态、停止循环、继续现状 | 当前进程 | 三者皆闭合 |
| F8 | 断电打断任意步骤 | dst 全程在位（硬链接方案）+ marker 原子写 + 启动 self-sha 对账；跨过 deadline → 回退（安全方向） | 启动代码 | 三者皆闭合 |
| F9 | 崩得早于启动检查 | 残余缺口：静态 Go 二进制过真实 exec 冒烟后窗口极窄，接受 | — | 文档记录 |
| F10 | register 成功但行为异常 | 非目标（承诺边界）；金丝雀 + 手动降级 | — | 文档记录 |

## 4. wire 增量（全部 additive/omitempty，ProtoVersion 恒 2）

- `NodeRegisterReq` += `UpgradeState string`（"committed"/"rolled_back"/"rollback_failed"）、
  `UpgradeDetail string`（原因串）。broker 仅记一条日志行，**不持久化**——没有它 broker 侧无法区分
  "回退"与"普通重启"，成本是两个 omitempty 字段。
- `UpgradeForwardedResp.NewVersion` 语义升级：冒烟保证非空且为归一化 release tag（字段不新增）。
- 新错误码入 codes registry + `cmd/tether/` 4 处 exit-class 双向表：
  - `smoke_failed` → **config 类（abort 车队）**：`--all` 单 URL 单 sha 发全队、预设同构车队，
    冒烟失败即产物对全队皆坏；abort 是安全方向，金丝雀先行使此裁决几乎不被触发。
  - `upgrade_in_progress` → transient 类（该节点稍后重试）。
- 全部字段进 §2 清单闸门 + 既有 golden fixture 更新。

## 5. ctl 可观测与金丝雀

目标输出时序（写进 usage.md 并被 e2e 断言）：

```
✔ nid1: staged (v0.4.7 → v0.5.0, smoke ok); agent re-exec in progress
  waiting for re-register (deadline 120s) …
✔ nid1: registered as v0.5.0 — upgrade COMMITTED
✗ nid1: still v0.4.7 after deadline — likely ROLLED BACK (check agent log / broker log)
```

- `--wait`（单目标默认开，`--wait=false` 可关）：dispatch OK 后每 3s 轮询既有 `node.list`
  （零 schema、零订阅、ACL 面不动），判据 `ReleaseVersion == resp.NewVersion` ∧ ONLINE；
  deadline+30s 超时仍旧版本 → 报"可能已回退"，exit 非零。
- `--all`：nid 字典序，**第一台为金丝雀强制 --wait 全确认**，失败 / 回退 / 超时 → abort 其余并明说
  "canary failed, N nodes untouched"；确认后其余沿既有 sequential + transient/config 分类扇出，
  不逐台 wait（6 台 ×120s 串行等待无收益），收尾提示 `node ls` 复核。

## 6. 改动文件清单

- `internal/proto/`：`messages.go`（2 字段）、`codes.go`（2 码）、golden 族（inventory 闸门 + fixture）、钉子测试。
- `internal/agent/`：`upgrade.go`（冒烟门、硬链接 prev、入口门、exec 失败就地恢复）；
  新文件 `upgrade_state.go`（marker 状态机 + `decideBoot` 纯函数 + watchdog——按职责命名）；
  `agent.go`（register→commit、watchdog 武装、`execFn`/`nowFn` 测试钩子）。
- `cmd/tether/agent.go`（守护路径挂 decideBoot）。
- `internal/broker/broker.go`（register 处记 upgrade_state 日志行，一处）。
- `cmd/tether/node.go`（--wait、金丝雀、两码分类）+ exit-class 4 处 + CLI golden 更新。
- `test/`：`gate_registry_test.go` 登记；p10 fixture 迁移（§7）。

## 7. 测试与变异验证计划

- **存量 fixture 迁移（P3 第一步而非扫尾）**：`test/p10/upgrade_e2e_test.go` 的
  `makeTarball(t, []byte("payload"))` 类文本 fixture 在冒烟门落地后全红——迁移为可执行假脚本
  （shell 脚本打印 `tether vX.Y.Z (proto v2)`；`exec.Command` 起子进程，不碰 go-test 二进制）。
- **注入点**：`execFn`（可观察 fake，断言"以 prev 路径被调用过"）、`nowFn`（deadline 边界）。
- **表驱动**：`decideBoot` 全组合（marker 状态 × self-sha × 时钟 × boot_count 边界）；对抗用例：
  打印版本后 exit 1 的脚本、prev 被人为删、deadline=0、双重回退、marker JSON 损坏、pending 期二次 install、
  commit 与 watchdog 竞态（R3）。
- **e2e（进 e2e-parallel，文件在 `test/p10/`）**：commit 路径往返、F5 回退路径（register 阻断 + 钩子）、
  `--wait` / 金丝雀输出断言、`upgrade_in_progress` 拒绝。**诚实边界**：真 exec + supervisor 崩溃循环
  （F3/F4）在 go test 结构上不可达，由纯函数表驱动覆盖，不假装 e2e 覆盖；本增量不动部署栈，不跑 simcluster drill。
- **变异验证**（每守卫一条，隔离工作树逐个注入，预告成本 ≈8 变异 × 相关包测试时长）：
  删冒烟调用→F2 红；恢复 os.Exit(1)→F3 红；删 boot 预算比较→F4 红；删 deadline 检查→F5 红；
  删入口门→F6 红；删 prev_sha 校验→F7 红；inventory 闸门删一字段/改一 tag→红；金丝雀门删除→e2e 红。
  三站点精确相等在今日集成面是恒等式，变异由钉子单测抓，不宣称集成覆盖存在。
- `-race` + 内建 NumGoroutine/fd 泄漏门（watchdog goroutine 挂 ctx 必须归零）；命名按被测单元；
  溯源 `// origin: upgrade-safety plan §3`。

## 8. 文档改动清单

- `docs/requirements.md §6.7`（§1 全文）。
- `docs/distributed-broker-architecture.md` 新节（站点表 / additive 纪律 / 状态机 / marker 格式 / bump 者双前缀义务）。
- `docs/usage.md`（node upgrade 新输出、--wait、金丝雀语义、验收时序样例）。
- `docs/broker-ops.md`：§8 头部加"broker 先、agent/ctl 后"滚动顺序；新增 §8.7 运维须知
  （marker 与 prev 路径、nohup GAP、首跳无保护、racknerd 小盘一句话、NAT 弱网假回退）。
  **落点修订**：综合稿原定这批运维事实进 `deploy-tier-gotchas.md`，实施时判定该文档是
  simcluster drill 的缺陷台账（S 系列编号制），运维须知放 broker-ops 更贴职责，不硬塞。
- `CLAUDE.md`：闸门表加 inventory 闸门一行（过 gate_registry 自对账）；§5"不兼容则必须重装"条款
  与 `codes.go` 注释同步改写为纪元语义。

## 9. 实施顺序

1. **P1 docs 先行**（先改文档再改代码）。
2. **P2 proto 面** ∥ **P3 agent 状态机**（P3 第一步迁 p10 fixture）。
3. **P4 ctl**（依赖 P2 的码与 P3 的 NewVersion 语义）。
4. **P5 变异批 + e2e + `make gates`**（动了闸门自身）→ 内审 → 外审。

## 10. 遗留风险与外审重点

1. **首跳无保护**：把本设施滚上现网的那一次 upgrade 仍由旧 agent 代码执行（无冒烟/无 prev/无回退），
   风险与今天等同。缓解：手工金丝雀先升 1 台 + `node ls` 确认。
2. **硬链接方案**请专门审：prev=hardlink 后 `rename(tmp,dst)` 是否在所有断点保持 dst 在位与
   prev 指向旧 inode；link 失败降级 copy 的分支是否被测试覆盖。
3. **boot_budget 与 supervisor 参数的交互**（R2 论证依赖 unit 的 RestartSec/StartLimit 实际取值，
   已核对本仓两个 unit；第三方自定义 unit 下机制仍成立、只是收敛时间不同）。
4. **F9 残余窗口**与 **nohup GAP**（F4）：如实标注不弥补，外审确认接受。
5. **`smoke_failed`=config 类全队 abort** 是对评审分歧的裁决（同构车队预设 + 安全方向），外审可复核。
6. **register 字段保留**（broker 侧区分回退与普通重启的唯一信号）——若外审认为日志行不值两个字段，
   删之不影响骨架。
7. deadline=120s 常量：NAT 弱网假回退无害但浪费一轮；现网首验后若偏紧再议 flag。
