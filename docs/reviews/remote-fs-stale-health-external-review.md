Pass

# remote-fs stale-health 独立外部审查报告

> 日期：2026-08-31
>
> 身份与边界：本报告审查外审介入时全部暂存区外内容。plan、内审报告及其绿色结果只用于建立待证伪假说，没有被当作产品证据。外审未修改生产实现；只新增了 tasklist 和两个可重复的架构门反例测试。

## 结论

当前增量不能上线。健康判定的 epoch replacement、healthy-only TTL、四条 evidence-driven invalidation 接线、`--safe` 顺序和 exec watchdog 收敛，在源码、定向测试与 race 检查中没有发现新的确定性生产实现缺陷；但发布所依赖的两层证据都未闭合：

1. 从当前源码构建的 simcluster drill 62，在同一外审中两次隔离运行一红一绿。红灯正落在本批新增的核心 Arm 1S-3：第一次 stale spawn 已给出证据后，第二次相同命令没有在预期约 2 秒内重探并快速失败，45 秒外层预算仍以 `rc=124` 杀死。这个结果未归因，不能用随后一次绿色运行覆盖。
2. 新增 `spawn_stall_evidence` 架构门宣称逐铸造点、路径精确并检查锁支配，实际只扫描整个 deadline arm 是否“出现过”作废调用，并按源码顺序计数 Lock/Unlock。提前 return 与条件 Unlock 两个反例都被错误接受；后者允许一个确定性自死锁形态绕过门禁。

因此 `make test` 和 `make gates` 也被外审反例正确拉红。完整 E2E、lint、build 和受影响路径 race 为绿，不足以推翻以上两项直接红证据。

## 范围与权威契约

- 外审开始时暂存区为空；候选面为 14 个 tracked 修改和 4 个既有 untracked 文件，约 1,552 行增加、62 行删除。外审随后增加本报告、tasklist，以及 `test/architecture/spawn_stall_evidence_test.go` 中两项 reviewer 测试。
- 已按 `CLAUDE.md` 的权威链核对 requirements、`distributed-broker-architecture.md`、`deploy-tier-gotchas.md`、`usage.md`、测试标准、历史 architecture 的有效章节、simcluster Mandate、cluster/device/local 运维信息，以及本批 plan/内审和 remote-fs 原始 plan/review。
- 本批没有 wire/proto/schema/config 键变化；CLI 仅补充 `--safe` 说明和错误提示，agent/ctl N-1 组合不依赖新字段。`HealthTTL` 是进程内默认值/测试 seam，不是 `agent.yaml` 旋钮；回滚仍是纯二进制回滚。
- 必须维持的核心不变量是：dead verdict 绝对 sticky；healthy verdict 可由证据或 TTL 换代；旧 launcher 只能写旧 epoch；每个 mount-generation 至多一个 probe；任何作废不得在 `p.mu` 持有时重入；spawn/resolve/Home watchdog 必须先同步作废再返回；门禁必须逐铸造点验证实际路径，而不是函数或语句块内“曾出现过”。

## Findings

### F1 — Major — 本批核心 deploy drill 在 45 秒预算下仍复现红灯，#81 不能标为已验证 FIXED

drill 62 的 Arm 1S 先在健康挂载上播种 verdict，再 SIGSTOP FUSE daemon。第一条绝对 argv 命令应付出 30 秒 execve watchdog 并返回 `remote_fs_spawn_timeout`；该超时必须同步作废 healthy verdict，因此第二条相同命令应在约 2 秒探针预算内返回 `remote_fs_unhealthy`（`test/simcluster/drills/62-remote-fs-safe.sh:121-145`）。

外审从当前源码执行 `./local.sh --build build` 后，两次隔离运行结果为：

- 第一次：**ASSERT-FAIL**，`assert_fail=1 pass=34 not_covered=1`。Arm 1S-3 的 `RFS40 agt1 -- /mnt/hung/probe` 在 45.010 秒被外层 `timeout` 杀死，`rc=124`、stderr 为空；失败时 loadavg 约 10.27/10.56/10.01，4 KiB fsync 约 15 ms，3 个容器正常存活。其他 arm 继续运行，`--safe` 半边通过。
- 第二次（保留实例取证）：**INCOMPLETE**，`assert_fail=0 pass=35 not_covered=1 nc_gap=1`；同一 Arm 1S-3 约 2 秒通过。agent 日志显示首条命令 30 秒超时，第二条在约 2 秒内结束。取证后实例、容器、卷、network 与 secrets stash 已 `nuke` 清理。

这不是一个可以靠“重跑变绿”关闭的结果。内审自己已记录该 arm 在 25 秒预算下出现过一次 `rc=124`，并明确写明加宽到 45 秒不是调绿手段、若再红应立案而不是重跑（`docs/reviews/remote-fs-stale-health-review.md:56-65`）。本次恰是在 45 秒下再次红，而且 drill 的注释所称“45 秒会给出诊断码”也被事实证伪（`test/simcluster/drills/62-remote-fs-safe.sh:132-142`）。

影响：尚不能证明这是 verdict 没被作废、mountinfo/FUSE 时序、ctl/NATS 响应丢失还是 harness 观测问题；但它直接落在本批修复的唯一 deploy 级 healthy→dead 恢复边。当前把 #81 写为 `FIXED`（`docs/deploy-tier-gotchas.md:886-900`）过早，上线后可能仍间歇付出超过两层 watchdog 的不可分类停滞。

建议：先把 #81 改为 candidate/verification-blocked。让 Arm 1S-3 在命令前后采集 mount generation、health epoch/state、invalidation sequence、mountinfo signature、agent request ID 与 ctl transport 终态；外层超时必须保留并打印 agent/ctl stderr，而不是空证据。定位后以多次独立实例运行建立稳定性，不能只取最后一次绿色。若 45 秒仍可能被基础设施吞掉，应把“产品码”和“控制面没收到终态”拆成两个独立 oracle。

### F2 — Major — mint-site 门禁按整个 arm 扫描，不能证明作废支配每个 timeout return

`mintSitesIn` 给一个 deadline arm 中的每个 mint site 复用“整个 arm 是否包含 invalidation”的布尔值。以下形态中，第一条 return 永远不会执行后面的作废，但当前 gate 把两条都记为 wired：

```go
case <-time.After(timeout):
    if skip {
        return ErrSpawnTimeout
    }
    p.invalidateHealthy()
    return ErrSpawnTimeout
```

独立测试 `TestSpawnStallEvidenceGateRejectsReturnBeforeInvalidation`（`test/architecture/spawn_stall_evidence_test.go:278-313`）稳定失败：gate 报 2/2 wired，真实应为 1/2。这个反例不依赖生产源码恰好已有这种分支；它证明 `CLAUDE.md:154` 和 gotcha #81 所登记的“铸造点精确账本、同一死线臂内”不足以推出作废一定先发生。

影响：未来在现有 timeout arm 前半段新增早退，或把错误构造抽进条件分支，`make gates` 会保持绿色，却重新制造“本次 timeout 不作废 stale healthy”的原事故。

建议：以每个 return/mint expression 为分析终点，验证从 deadline case 入口到该点的所有可达控制流都先经过同步 invalidation；最稳妥的是把 timeout 终态收敛到一个不可绕过的 helper/API，让产品结构保证顺序，门禁只核对唯一入口。若继续 AST gate，需要真正的 CFG/dominance，而不是 arm-level `ast.Inspect`。

### F3 — Major — 所谓锁支配检查只是源码顺序计数，条件 Unlock 可让确定性死锁静默过门

`lockedInvalidateIn` 按遍历源码时遇到的 `p.mu.Lock()`/`Unlock()` 加减计数，没有控制流语义；文件自己的注释也承认 branch-dependent locking 可以骗过它（`test/architecture/spawn_stall_evidence_test.go:338-344`），但 `CLAUDE.md:154` 和 gotcha #81 却把它登记成“`p.mu` 持有期间一律红”的锁支配检查。

独立测试 `TestSpawnStallEvidenceGateRejectsConditionalUnlockBeforeInvalidation`（`test/architecture/spawn_stall_evidence_test.go:315-335`）构造：

```go
p.mu.Lock()
if unlock {
    p.mu.Unlock()
}
p.invalidateHealthy()
```

当前 gate 接受该函数；`unlock=false` 时 `invalidateHealthy` 重入同一把非重入 mutex，确定性永久死锁。由于 gate 还递归传播“哪些 helper 会作废”，这个假阴性不限于直接调用。

影响：架构文档声称已经机械封死一个比 #81 更严重的 agent-wide spawn hang，实际并没有。门禁的 false-green 会给后续重构提供错误安全感。

建议：不要用线性 token 计数冒充 dominance。优先让 invalidation 接受已经解析出的 epoch/action，并在锁外统一执行，或将锁内状态转换与锁外调用拆成类型/函数边界；门禁再禁止锁内 helper。若必须静态分析，至少建立基本块 CFG，逐路径传播 lockset，并把未知/不平衡分支判为失败。

### F4 — Minor — `too_many_wedged_spawns` 的源码行号注释已经失真

`cmd/tether/error_hints.go:229` 仍把 slot exhaustion 的来源写成 `spawnsafe.go:812-814`；本批扩展文件后，该位置已经落在 `invalidateHealthy` 注释/实现附近，ceiling 铸造点在更后的 resolver/start 路径。该注释不会改变运行时，但会误导事故定位，并会在每次代码插入后继续腐烂。

建议：使用稳定的函数锚（例如 `boundedResolveInDirs` / `RunStartWithCleanup` 的 wedge-ceiling 分支），不要记录易漂移的绝对行号。

## 生产实现审查结果

- `mountHealth` 的 `state/launched/result/done/decidedAt` 组合、晚到结果 drain、healthy TTL 边界、dead sticky、applyMounts signature 继承和换指针 epoch 隔离，在当前路径上相互一致。作废只替换 `stHealthy` 指针，不会原地清空仍被 launcher 持有的对象；旧 launcher 的写入不会污染新 generation。
- `HealthTTL==0` 使用 5 分钟默认，负值拒绝；相等边界用 `>=` 到期。注入时钟回拨会延长一次 verdict 寿命，但不会将 dead 复活或破坏资源上界；这是 wall-clock seam 的残余语义，不是本次阻断项。
- `Prepare(--safe)` 在 cwd 快速检查之前刷新 mount table 并作废 healthy；PATH、显式 argv0、cwd 与 autofs 的既有分类没有发现兼容性漂移。全局作废会使多个健康远端挂载同批重探，属于已登记的抖动/墙钟风险，当前没有新配置面可缓解。
- `boundedResolveInDirs` 与 `RunStartWithCleanup` 当前 timeout 分支先同步作废再返回；start watchdog 保持 reap-before-release，pipe/PTY/session cleanup 没有发现 double-close 或 slot 提前释放。agent `boundedHomeRead` 的 timeout 同样在返回前调用导出作废 API。
- ctl exec/run 的 `--safe` help 与错误提示对称，错误码和退出分类未变；提示正确说明“重跑只会让下一次重探”，也保留“payload/数据本身位于死挂载时重试无用”的边界。

## 独立验证

| 验证 | 结果 | 说明 |
|---|---|---|
| reviewer gate 反例 | **FAIL（发现缺陷）** | 两项测试均稳定红：arm-level false wired、conditional Unlock false safe |
| `make test` | **FAIL（仅 reviewer 反例）** | vet、Darwin cluster build完成；全仓其余包通过，包括 cmd、agent、spawnsafe、concurrency、determinism 与 E2E 包 |
| `make gates` | **FAIL（仅 reviewer 反例）** | architecture 两项红；determinism/cmd/auth/concurrency/proto 均通过 |
| `make e2e-parallel` | **PASS** | coverage self-check 15/15，99 个执行单元全部报告，wall clock 3m47.602s |
| `make lint` | **PASS** | 0 issues |
| affected race | **PASS** | `go test -race ./internal/spawnsafe ./internal/agent ./test/concurrency -count=1` |
| focused repeat | **PASS** | `go test ./internal/spawnsafe -count=10`；未见 race/goroutine/slot 泄漏 |
| `make build` | **PASS** | 当前源码生成 `bin/tether` |
| `git diff --check` | **PASS** | 无 whitespace error |
| simcluster build | **PASS** | 当前静态 tether/tether-next 构建为 `tether-sim:dev`，nats-server pin 2.10.22 |
| drill 62 run 1 | **ASSERT-FAIL** | 34 pass、1 assert_fail、1 known gap；Arm 1S-3 在 45.010s 得到 rc124 |
| drill 62 run 2 | **INCOMPLETE** | 35 pass、0 assert_fail、1 known gap；保留实例取证后已完整 nuke |
| simcluster hermetic tests | **FAIL（既有 ledger 项）** | harness/verdict/poll/lint/kept-sites 等通过；`ledger-crosscheck` 因既有 open #80 没有 non-GREEN owner 而红，另标 #82 为 R6 candidate；不是本批代码导致，但说明仓库总门仍非绿 |

沙箱内 Go cache trim 和本地 listener 权限曾造成环境错误；相关命令均在允许 host listener/cache 的执行环境重跑，报告没有把沙箱拒绝记为产品失败。

## NOT-COVERED、疑惑与残余风险

- drill 62 使用可回收的 FUSE T/S-state 近似，不是真实不可杀的 D-state NFS/CIFS。true-D 下 kernel thread、reaper 与 shutdown 上界仍为 **NOT-COVERED**；本次 drill 的 `nc_gap=1` 正是该事实，不能转换成绿色保证。
- `outage=true` 的完整部署链（child PATH/PWD/cwd rewrite、fallback PATH 与 dropped banner）仍没有 deploy-tier 端到端覆盖；gotcha 已登记 gap。本批会让该路径在现网更常被触发，上线前至少应在独立增量补可控 fixture。
- 疑问 1：第一次 drill 红灯时，agent 是否已执行 invalidation、但第二个 ctl response 丢失；还是 mount generation 被重新继承为 stale healthy？现有日志与空 stderr 无法区分。没有这个答案，不应把失败归咎于“机器慢”。
- 疑问 2：全局 invalidation 是有意的 conservative fallback，还是未来应携带 implicated mountpoint？当前注释曾使用 `targeted`，实现会同时推翻所有 healthy remote mounts；多挂载 fleet 的 TTL/证据同刻重探可能形成延迟尖峰。
- 疑问 3：5 分钟 TTL 是不可运维的编译期默认。若 fleet 出现 false-demotion/churn，plan 所写“调大 TTL”实际意味着改常量发版；runbook 应明确这一点，避免 operator 寻找不存在的 YAML 键。
- `smokeVersion` 的升级 execve 仍不在有效 watchdog 下；本批没有扩大到该路径，建议保留为独立 gotcha，而不是从 spawn evidence 的穷尽性语气中消失。

## 上线前必须完成

1. 归因并修复/硬化 F1，使当前源码的 Arm 1S-3 在多次独立实例上稳定给出产品终态；在此之前撤回 #81 的 `FIXED` 状态。
2. 修复 F2/F3 的架构门，保持两项 reviewer 反例原样并转绿；同步收窄 `CLAUDE.md` 与 gotcha 的覆盖声明，直到真正 CFG/path-sensitive 检查落地。
3. 修正 F4 的失真 provenance；补充并闭合 simcluster ledger #80 owner（虽不是本批引入，仓库总门当前仍红）。
4. 重跑 `make gates`、`make test`、affected race、`make e2e-parallel`、`make lint` 和当前镜像 drill 62；不得用最后一次绿色覆盖中间红灯。

---

## 主进程逐条回复（2026-08-31，step 6 返修）

> 四条全部**采纳**，无驳回。外审留在 `spawn_stall_evidence_test.go` 的两个反例测试**原样保留**——
> 它们现在是这次修复的变异验证收据。

### F1 — 采纳。撤回 `FIXED`

> **标题历史修订（复审 RR-F5）**：本节标题原为「取证落地后结论变了：红的是 1S-2，不是 1S-3」。
> **那个结论也是错的**，它来自只装了一臂仪表的单次运行；下文保留完整的推翻过程作为审计轨迹。

> **先说一条我自己的错**：第一版取证代码是坏的，它产出了一轮 **5/5 假红**。原因两条：
> `assert_ok` 用 `$(...)` 捕获输出，predicate 跑在子 shell 里，我用全局变量传 rc/输出 ⇒ 两个 oracle
> 读到的都是空串（oracle A 还因此"空过"，因为 `"" != "124"`）；另外 dash 的 `printf` 把以 `--`
> 开头的 format 当成选项（`printf: Illegal option --`），把证据自己截断了。
> 已改为**走文件**，并让 oracle A 在 **rc 缺失时也红** —— harness 坏掉必须自曝，不能冒充产品判决。
> 那一轮 5/5 红**不是产品信号**，特此更正。

**取证修好后（累计 11 次独立运行，两版仪表）**。中途我下过一个结论说"红的是 1S-2 不是 1S-3"——
**那个结论也是错的**，它来自只装了 1S-3 仪表的那一轮的单次运行。把 **1S-2 也拆成 A/B 之后**再跑三次，
位置又变回 1S-3。所以唯一站得住的表述是：

> **两条连续的挂死挂载 `exec` 中，间歇性地有一条在 45s 内拿不到终态。位置在 1S-2 与 1S-3 之间游移，不固定。**

硬证据（不同运行各一条）：

```
# 装了 1S-3 仪表那轮：红在 1S-2
desc: 1S control: first abs-argv0 ...   argv: RFS40 agt1 -- /mnt/hung/probe   rc: 124  took: 45009 ms

# 两臂都装仪表后：1S-2 两个 oracle 全绿，红在 1S-3
desc: 1S-3 oracle B ★ ...   argv: _1s_code second remote_fs_unhealthy
     second rc=124        --- ctl stdout+stderr ---  (空)
     --- agent 'exec' lines after this command's cursor ---  1
```

**最后一行是本轮最有价值的一条事实**：`agent 'exec' lines after cursor = 1` ——
**agent 收到了这次请求并记了日志，而 ctl 在 45s 内什么都没收到**。

**这仍然推翻了把 F1 描述成"healthy→dead 恢复路径复现红灯"的框架**（外审的与我自己的都一样）：
一条命令只有一个断言时，"控制面没交出终态"与"产品码不对"在观测上不可分，于是历次归因都在猜。
拆成 A/B 之后，**每一次红都明确是 oracle A**，即控制面侧。

**现在精确的未决问题（独立于 #81 的修复本身）**：
*对挂死挂载发起的 ctl `exec`，agent 已记录收到请求，但 ctl 有时在 45s 内拿不到任何终态，
尽管 agent 自己的 execve 看门狗只有 30s。* 归属未定（agent 回包路径 / broker / ctl / harness）。
**我没有猜，也没有据此改任何产品代码。**

**仪表本身的一个已知弱点，一并说明**：证据里的 agent slog dump 之前是 `tail 60`、不按游标锚定，
而 agent 在 Arm3 会被重新 provision（重启后追加同一文件），所以短 tail 可能整段都是重启之后的行、
恰好错过要诊断的那 45s 窗口——实测发生过。已改为 `tail 200`。**要真正闭合这条，下一步应该在
`drills/lib/logs.sh` 里加一个按游标截断的 dump helper**，而不是靠加大 tail；我没有在本轮动那个共享 lib。

**本轮已做的**：
- **状态已撤回**：gotcha #81 由 `🟢 FIXED` 改为 **`🟡 FIX LANDED，DEPLOY-TIER 验证未闭合`**，并写明
  转 FIXED 的条件（多次独立实例稳定绿，或红一次并由证据定位）。
- **承认被证伪的那句话**：drill 注释里"45s 会给出诊断码"是我写的，外审在 45s 下复现 `rc=124` 直接推翻了它。
  该注释已删除并改写为"两次红都是空证据、归属未定"。
- **拆 oracle + 强制取证**（`62-remote-fs-safe.sh` Arm 1S-3）：命令改为先捕获 rc 与输出，然后两条独立断言——
  **oracle A**「控制面在 45s 内交出了终态（rc≠124）」、**oracle B**「产品码是 `remote_fs_unhealthy`」。
  **两条失败时都打印** ctl stdout+stderr、agent slog tail、以及"agent 到底有没有收到这次请求"
  （`sim_agent_slog_count 'agent: exec'`，按 cursor 只看本次之后）。日志读取经 `drills/lib/logs.sh`
  唯一映射并已 `source`（CLAUDE.md 的 simcluster 日志 oracle 铁律 + lint-drills 通过）。
  下一次复现将直接把"产品回归"与"控制面没送达"分开。
- **我没有做的事，明说**：我**没有**归因这两次红。按外审建议做的是让它可归因，而不是宣称它已解决。

### F2 — 采纳。arm-level 布尔换成路径敏感支配分析

`mintSitesIn` 原本把"这个 deadline 臂里出现过作废"这一个布尔复用给臂内每个 mint，于是臂内提前 return
被记成 wired。现改为在臂内按顺序走语句、携带"本路径上是否已作废"的标志，**只有 if 的两个分支都作废
才把标志传播到 if 之后**；loop/switch 保守处理（可能零次执行 / 可能走不作废的臂），宁可少判 wired
（响亮失败）也不多判（静默放行）。外审反例 `TestSpawnStallEvidenceGateRejectsReturnBeforeInvalidation`
现在得到 1/2 而不是 2/2，转绿。

### F3 — 采纳。线性计数换成三值 lockset，分支不一致按"持有"处理

`lockedInvalidateIn` 原本按源码顺序加减 Lock/Unlock——那不是支配检查，条件 Unlock 会替一条根本没走的
路径把计数减掉。现改为路径敏感的三值状态（free / held / unknown）：if 的两分支**求交（join）**，不一致
即 `unknown`，而 `unknown` **按持有处理**；`defer Unlock` 不算解锁；`for/range` 若改变锁态则之后为
`unknown`；func literal 不计入本路径。外审反例
`TestSpawnStallEvidenceGateRejectsConditionalUnlockBeforeInvalidation` 转绿。

**同时收窄了声明**：该检查现**限定在 `internal/spawnsafe`**——`Policy.mu` 是 `invalidateHealthy` 实际要取的
那把锁，别的包持不到它；全树匹配任何名为 `mu` 的字段会误报 `internal/agent` 里若干无关临界区，而一个爱哭狼的
门会被删掉而不是被遵守。这条 scope 已同时写进 `CLAUDE.md` 闸门行、gotcha #81 与闸门文件头。

**回归验证**：重写后把此前整套闸门变异重跑，全部维持原判——删作废/藏进 `go func(){}`/`%w` 包装的新铸造点/
给 ceiling 接线/抽 helper 锁内调用/`Prepare` 锁内作废 六条 RED，`time.NewTimer` 重构正控 GREEN。

### F4 — 采纳。改用函数锚

`cmd/tether/error_hints.go` 的 `too_many_wedged_spawns` 注释改为点名
`boundedResolveInDirs` / `RunStartWithCleanup` 的 wedge-ceiling 早退分支，不再记绝对行号。
另外扫了本批引入的其它 provenance：`docs/deploy-tier-gotchas.md` #81 **诊断段**里的 `spawnsafe.go:NNN`
是修复**前**（2026-08-29）那棵树的取证，已在条目开头加一行说明其指向 pre-fix 树、不追溯改。

### 关于 "上线前必须完成" 第 3 条后半（simcluster ledger #80 owner）

**不在本批做，登记理由**：`ledger-crosscheck` 红是因为既有 open #80 没有 non-GREEN owner，外审自己也标注
"不是本批代码导致"。#80 是另一个会话的在飞 proxy-lifecycle 增量的条目，改它会跨到别人的工作面上。
应由该增量收尾时闭合，或单开一个 ledger 整理增量。

## 交付状态（首轮 · 已被后续章节取代）

> **过期声明（复审 RR-F5 指出）**：本节写于首轮外审当时，称「两项 reviewer 测试刻意保持红色」。
> 那两项**已在首轮返修中转绿**；此后复审又新增三项并保持红色，它们也已在本轮返修中转绿。
> **当前交付状态以文末「主进程对复审的逐条回复」为准**，本节仅作历史留存。

首轮 tasklist 已逐项执行；暂存不代表审查通过。

---

# 独立外部复审结论（2026-08-31）

## Verdict

**Fail。** 首轮 F4 已闭合，首轮 F2/F3 的两个具体反例也转绿；#81 从 `FIXED` 降为 `FIX LANDED，DEPLOY-TIER 验证未闭合` 是诚实且必要的。但返修仍不能上线：所谓路径敏感 gate 还有三个确定性假阴性；当前源码构建的 drill 62 再次 ASSERT-FAIL，并证明新增 A/B oracle 不能按其注释所写区分控制面与 agent owner；#81 状态降级又使它成为 ledger 中无人负责的 open defect。

复审只把首轮已暂存树当作基线，审查其后的 6 文件 developer delta（474 additions / 125 deletions），没有直接采信开发者报告的三条绿色门禁或 11 次 drill 归纳。复审没有修改生产实现；新增一个复审 tasklist 和三个确定性的 gate 反例测试。

## 首轮 findings 复核

- **F1 未闭合，证据更具体但仍是发布阻断项。** 撤回 `FIXED` 正确；新 oracle 确实不再吞 rc/output，也成功留下当前红灯证据。但其 owner 分层不成立，见 RR-F3。
- **F2 部分闭合。** 首轮“arm 内提前 return”反例连续 10 次通过；但新 walker 仍不是完整控制流分析，`go` 与 `goto` 均可绕过，见 RR-F1。
- **F3 部分闭合。** 首轮“条件 Unlock”反例连续 10 次通过；但三值 walker 跳过语句 initializer，仍可静默接受锁内重入，见 RR-F2。
- **F4 闭合。** `too_many_wedged_spawns` 改用 `boundedResolveInDirs` / `RunStartWithCleanup` 函数锚，避免行号漂移；历史诊断行号也明确标注属于 pre-fix tree。
- **#80 ledger 裁决接受。** #80 属另一个在飞增量，本批不应越权修改。但返修自己打开的 #81 不能套用这个豁免，见 RR-F4。

## 复审 findings

### RR-F1 — Major — mint-site walker 仍把异步调用和源码顺序误当作路径支配

`mintSitesIn` 把任何 default statement 交给 `callsInvalidateDirectly`；该 helper 只在遇到 `FuncLit` 时停止，因此 `go p.invalidateHealthy()` 会被当成同步作废（`test/architecture/spawn_stall_evidence_test.go:715-719`）。watchdog 可以立即 return，而 goroutine 尚未运行，下一条命令仍能读到 stale generation。返修声称重跑过“藏进 `go func(){}`”变异，但那只是异步调用的一种语法；最短的直接 `go` 形态没有被覆盖。

同一个 walker 也没有建立 branch/goto CFG。以下合法形态中，`goto done` 跳过作废，但线性 scanner 在看到 label 前已经把 `seen` 置为 true：

```go
if skip {
    goto done
}
p.invalidateHealthy()
done:
return ErrSpawnTimeout
```

独立测试 `TestSpawnStallEvidenceGateRejectsAsyncDirectInvalidation` 与 `TestSpawnStallEvidenceGateRejectsGotoAroundInvalidation`（`:346-371,394-423`）均稳定失败，`-count=10` 每轮都红。

影响：CLAUDE 和 gotcha 宣称“到达 return 的每条路径都已执行”仍是假保证；一个机械重构即可让 timeout 返回先于 evidence invalidation，而 `make gates` 在没有 reviewer 反例时保持绿色。

建议：先把 `GoStmt` 明确判为异步、绝不传播 seen；对 `BranchStmt`/label 建真实 CFG，或禁止 timeout arm 中出现 goto/复杂控制流。更稳妥的是让 timeout return 只能经一个同步 invalidation helper 退出，再对唯一 helper 做结构门，而不是继续扩写半个 Go 控制流解释器。

### RR-F2 — Major — 三值 lockset 跳过 statement initializer，仍可接受确定性自死锁

`lockedInvalidateIn` 的 `IfStmt` 只分类 `Cond`，完全不访问 `Init`（`:513-531`）；`ForStmt` 也跳过 `Init`/`Cond`/`Post`，switch/type-switch 同样没有先处理 initializer。以下合法 Go 代码在所有路径都持有 `p.mu`，但 gate 返回 safe：

```go
if p.mu.Lock(); ok {
}
p.invalidateHealthy()
```

独立测试 `TestSpawnStallEvidenceGateRejectsLockInIfInitializer`（`:373-392`）稳定失败，`-count=10` 每轮都红。把 scope 收窄到 `internal/spawnsafe` 是合理的——包外确实不能持有 `Policy.mu`——但 scope 正确不能补偿包内分析漏掉真实语句。

影响：这仍是 agent-wide spawn 的确定性永久死锁，严重度与首轮 F3 相同。当前“能带着 `p.mu` 到达作废即红”的注册声明不成立。

建议：所有 compound statement 必须按 Go 执行顺序处理 init/condition/body/post，并对 return/break/continue/goto 做可达性建模；未知语法直接 fail closed。至少为 if/for/switch initializer 各保留一个反例，避免修一处漏两处。

### RR-F3 — Major — 新 A/B oracle 不能区分 broker refusal 与 agent 产品码；当前运行再次暴露超过 30 秒的真实失联窗口

drill 注释把 oracle A 定义为“ctl/NATS/harness 是否交出终态”，oracle B 定义为“agent behaviour 产品码”（`test/simcluster/drills/62-remote-fs-safe.sh:130-132`）。但 A 实际只拒绝 rc 缺失或 124（`:160-165`），所以 broker 在请求到达 agent 前返回 `node_offline`/rc70 时 A 会绿色；B 随后因没有 `remote_fs_unhealthy` 而红，并被错误归到 agent behaviour。

复审从当前源码重建 `tether-sim:dev` 后，单次隔离运行稳定展示了这个缺口：

1. 15:49:31.360，1S-2 在 agent 记录 `agent: exec argv=[/mnt/hung/probe]`；
2. 预期 30 秒 watchdog 没有交付终态，broker 在 15:49:47 记录 node state transition；外层 45 秒到 15:50:16 返回 rc124，oracle A/B 均红，游标后 exec count=1；
3. 紧接着的 1S-3 得到 `error: exec: node_offline: status=STALE`、rc70。oracle A **错误通过**，oracle B 红；游标后 exec count=0，证明这次请求根本没到 agent；
4. `_heal` 后，第一条请求直到 15:50:17.286 才在 agent 写出 `remote_fs_spawn_timeout`，距接收约 45.926 秒；随后 agent 才恢复处理新 exec。

最终 verdict：`ASSERT-FAIL assert_fail=3 pass=36 not_covered=1`。这不是开发者报告里“每一次红都落在 oracle A、agent exec lines=1”的同一形状；它证明两条连续命令的状态相互影响，而简单 A/B 二分会把 broker 拒绝误写成产品码失败。报告中“拆成 A/B 之后，每一次红都明确是 oracle A”的结论（`:168-170`）已被本次运行直接证伪。

影响：仍不能断言根因在 spawnsafe、reply path、heartbeat、broker 或 harness；但可以断言 agent 的 30 秒 watchdog 没有在 45 秒外层预算内形成 ctl 可见终态，并伴随节点 STALE。这个窗口会让后续命令在 broker 被拒绝，已超出“单条命令只是没回包”的描述。

建议：oracle 至少拆成三层并由同一 request ID 关联：(A) ctl 是否超时/调用失败；(B) broker 是否 forward 且 agent 是否收到该 request ID；(C) 只有 B 成立时才验证 agent 产品码。`node_offline`、认证/usage/transport 错误必须归入 B 前置失败，不能让 A 绿后落到产品码 oracle。日志 dump 应使用 cursor-bounded helper而非 tail 200。随后定位为什么 `RunStartWithCleanup` 的 30 秒 timer 路径要等 FUSE heal 才完成；优先检查 timeout arm 中同步 `invalidateHealthy`/`onAbandon` 的阻塞点及 heartbeat state transition。

### RR-F4 — Major — #81 降为 open 后没有加入 ledger owner，返修使 simcluster hermetic gate 新增红灯

`docs/deploy-tier-gotchas.md` 将 #81 从 FIXED 改为开放的 `FIX LANDED，DEPLOY-TIER 验证未闭合`，但 `test/simcluster/expected-verdicts.tsv` 的 drill 62 owner 仍只有 `OQ-2`。因此 `bash test/simcluster/tests/run-all.sh` 的 `ledger-crosscheck` 当前报告：

```
UNOWNED #80
UNOWNED #81
R6-CAND #82
```

#80 是既有跨会话问题；#81 是本返修状态变化直接产生的新 unowned defect。仓库规则对 open defect 的要求正是“必须有 non-GREEN owner，保证某次 run 能抓到”，不能只在 prose 写转 FIXED 条件。

建议：由 drill 62 owner 明确钉住 #81，并为当前可观察的 exact signature 建 band/log 条目；band 仍必须阻断，不能把间歇 ASSERT-FAIL 折成 INCOMPLETE 或 GREEN。保持 OQ-2 的 true-D gap 为独立 owner，二者不能互相替代。

### RR-F5 — Minor — 返修回复保留自相矛盾标题、重复段落和过期交付说明

外审报告的主进程回复标题仍写“红的是 1S-2，不是 1S-3”（`:138`），正文紧接着承认这句话错误并说位置游移（`:147-151`）；“本轮已做的”列表又在 `:182-199` 重复两遍，第二遍仍只称 Arm 1S-3 拆 oracle。文件末尾交付状态还说“两项 reviewer 测试刻意保持红色”，但那两项已转绿，当前红的是复审新增三项。

这些不改变运行时，却破坏报告作为审计轨迹的可读性。建议保留历史错误事实，但将标题标为“历史错误，已撤回”，删除重复列表，并让最终交付状态由本复审章节统一覆盖。

## 其它复审观察

- `_1S_DIR=${TMPDIR:-/tmp}/tether-1s-$$` 每次运行都会创建目录，但 trap 只清理 FUSE，不删除该目录；本机已经积累多份 `tether-1s-*`。PID 命名避免旧文件误读，但应在统一 EXIT cleanup 中删除，或把证据搬入 harness-owned 临时目录。
- `_1s_terminal` 把 125/126/127/信号退出也视为“终态”；oracle B 最终通常会红，但 owner 会被错标。建议 A 只接受 ctl 的已定义业务退出类，并把 timeout utility 自身错误单列 harness failure。
- `sim_agent_slog_count 'agent: exec'` 是有用 discriminator，但没有 request ID；异步写入、相同 argv 并发或重试时无法证明是哪一条请求。当前顺序运行降低风险，不等于建立身份契约。
- true uninterruptible-D 与完整 outage rewrite 仍 NOT-COVERED；当前 drill 的 FUSE T/S-state 近似不能升级为真实 D-state 保证。

## 独立验证结果

| 验证 | 结果 | 说明 |
|---|---|---|
| 首轮两个 reviewer 反例 + 主 gate，`-count=10` | **PASS** | arm 提前 return、条件 Unlock 已闭合；当前生产 mint ledger 通过 |
| 三个复审 reviewer 反例，`-count=10` | **FAIL（发现缺陷）** | async direct invalidation、if initializer Lock、goto bypass 每轮均红 |
| `make test` | **FAIL（仅三个复审反例）** | vet/Darwin build及全仓其余包通过 |
| `make gates` | **FAIL（仅三个复审反例）** | determinism/cmd/auth/concurrency/proto 均通过 |
| `make e2e-parallel` | **PASS** | coverage 15/15，99/99，wall clock 3m49.201s |
| `make lint` | **PASS** | 0 issues |
| affected race | **PASS** | spawnsafe、agent、concurrency，未见 race detector 报告 |
| `make build` / `bash -n` / `git diff --check` | **PASS** | 当前源码构建及 shell/whitespace 检查通过 |
| simcluster current-image build | **PASS** | 当前返修源码，nats-server 2.10.22 pin 一致 |
| drill 62 | **ASSERT-FAIL** | 3 assert_fail、36 pass、1 known gap；1S-2 rc124，1S-3 node_offline/STALE |
| simcluster hermetic tests | **FAIL** | 其余 harness tests 通过；ledger-crosscheck 因 #80/#81 unowned 红，其中 #81 为本返修新增 |

## 复审上线条件

1. 修复 RR-F1/RR-F2，使三个 reviewer 反例原样转绿，并为同族 init/branch/async 控制流补正负控制；在此之前收窄“每条路径/能带锁到达”的文档声明。
2. 按 RR-F3 重做可关联 request ID 的分层 oracle，归因并闭合“30 秒 watchdog 超过 45 秒且节点 STALE”的路径；保留本次红灯，不得由后续绿色覆盖。
3. 为开放 #81 补 ledger owner 与 exact signature，保持 true-D OQ-2 独立；#80 可继续由其原会话负责。
4. 清理 RR-F5 报告矛盾/重复与 drill 临时目录，再重跑全部门禁、simcluster hermetic tests 和多次隔离 drill 62。

## 复审交付状态

复审 tasklist 全部执行；三个 reviewer 反例作为可重复收据保留为红。最终一致性检查后，复审 delta 与原有 staged 基线将按用户要求统一 `git add -A`；暂存不改变本节 **Fail** 结论。

---

## 主进程对复审的逐条回复（2026-08-31 第二轮返修）

> 五条全部**采纳**，无驳回。复审留在 `spawn_stall_evidence_test.go` 的三个反例**原样保留**，
> 现在是本轮修复的变异收据。**本节取代此前所有交付状态声明。**

### RR-F1 — 采纳。异步判定 + 对未建模控制流 fail-closed

两个洞我都确认了：`callsInvalidateDirectly` 只在 `FuncLit` 停下，所以裸 `go p.invalidateHealthy()`
被当成同步；线性扫描在看到 label 之前就把 `seen` 置真，`goto` 能跳过作废。

修法按复审自己的建议——**不再扩写半个 Go 控制流解释器**：

- `GoStmt` / `DeferStmt` 与 `FuncLit` 一样**永不计为同步作废**。异步形态无论写法都不算证据接线。
- **`BranchStmt` / `LabeledStmt` 一律 fail-closed**：同一语句列表里出现跳转或标签，`seen` 归零，
  该臂之后的 mint 全部记为未接线。理由写在代码里：建模 label 与跳转＝在闸门里写 CFG 解释器，
  而那正是这个检查不断长出新盲区的原因。**拒绝分析是诚实的答案，而且是响亮的**——账本里 wired 的
  站点会红，直到那个臂被写成没有跳转的形状。

### RR-F2 — 采纳。lockset 按 Go 执行顺序处理 initializer

`IfStmt.Init` 完全没被访问，`ForStmt` 的 `Init`/`Cond`/`Post`、switch/type-switch 的 `Init`/`Assign`
同样被跳过。现在每个复合语句都按执行顺序先过 initializer，再进条件、body、post；
`switch`/`select` 的各 clause 从**构造处**的状态出发并 join。

### RR-F1 / RR-F2 的验证

- 三个复审反例 + 首轮两个反例 + 主闸门：`-count=10` **全绿**。
- 新增 `TestSpawnStallEvidenceGateControlFlowFamilies`：**15 个同族用例，正负控制成对**——
  顺序作废/两分支都作废（wired）、单分支/`go`/`defer`/`goto`/循环体（**not** wired）；
  未加锁/先解锁（安全）、直接锁内/if-init/for-init/switch-init/条件 Unlock/`defer Unlock`（**违规**）。
- 旧变异 battery 全部复跑并维持原判，另加两条复审形态：
  裸 `go` 语句 ⇒ RED、`if p.mu.Lock(); requestedSafe {` ⇒ RED；`time.NewTimer` 正控仍 GREEN。

### RR-F3 — 采纳。改成三层 oracle，并补 cursor-bounded 日志 helper

你的实测把两层拆分的缺陷钉死了：broker 在请求到达 agent 前回 `node_offline`/rc70 时，
oracle A **会绿**（那确实是一个终态），然后 B 因没有产品码而红——**把路由问题记成了 agent 行为缺陷**。

现在每条命令三层，且 **C 只在 B 成立时才有意义**：

| oracle | 问的问题 | owner |
|---|---|---|
| **A 传输** | ctl 在 wrapper 内拿到终态了吗 | ctl / NATS / harness |
| **B 投递** | broker 转发了、且 agent 收到了吗 | broker / 路由 |
| **C 产品** | agent 的码对不对 | agent 行为 |

- **A** 现在拒绝 `""` / 124 / 125 / 126 / 127 / ≥128（信号）——按你的指正，`timeout` 工具自身的失败
  是 harness 故障，不是 ctl 的业务退出。
- **B** 两半都查：回包里出现 `node_offline|not_a_member|node_not_found|no_responders` 即判前置失败；
  并要求本次 cursor 之后 agent slog 真有 `agent: exec`。
- 日志 dump 改用**新加的 `sim_agent_slog_since`**（`drills/lib/logs.sh` 唯一映射内），按本命令游标截断，
  不再是 `tail 200`。
- `_1S_DIR` 已挂进 `drill_install_traps` 的清理链（此前只清 FUSE，目录会累积）。

**身份契约的诚实说明**：owner 判定用的是 cursor-bounded 的 agent slog，**不是 request ID**——
并发同 argv 无法区分。本臂严格顺序执行所以成立，这一点已写进 drill 注释而不是默认为真。
**真正的 request-ID 契约是后续项**，我没有在本轮改 ctl/agent 的 wire。

**我仍然没有归因那条超时**（agent 收到请求、ctl 45s 内拿不到终态、伴随节点 STALE）。
本轮做的是让它可归因并**阻断**，不是宣称它已解决。

### RR-F4 — 采纳。#81 已进 ledger，并且是**阻断**的 band

`expected-verdicts.tsv` 的 drill 62 行 owner 补上 #81，band 为
`ASSERT-FAIL@#81@sig:1s-no-terminal-state`；signature 定义与完整背景写进 `expected-verdicts-log.md`。

**band 只钉 oracle A**（传输层那一半）。**oracle C 刻意不 band**：若证据驱动重探本身回归，
那必须以**未 banded 的红**出现。banded 红仍然阻断，没有折成 INCOMPLETE 或 GREEN。
OQ-2 的 true-D gap 保持独立 owner，两者不互相替代。

验证：`ledger-crosscheck` 现在 `ok #81`，只剩既有的 `UNOWNED #80`（跨会话增量，维持不越权裁决）。
`validate-verdicts.sh`：OK（43 行，band 合法）。

### RR-F5 — 采纳

- F1 节标题改为「撤回 `FIXED`」，并在其下用一行标注**原标题是历史错误、已撤回**，保留推翻过程作审计轨迹。
- 删掉重复的第二份「本轮已做的」列表。
- 首轮「交付状态」标为**已被取代**，明说那两项 reviewer 测试早已转绿；当前状态以本节为准。

### 其它复审观察

- **临时目录**：已随 trap 清理（见 RR-F3）。
- **rc 语义**：125/126/127/信号已从"终态"里剔除（见 RR-F3）。
- **request ID**：如实登记为后续项，未在本轮伪造身份契约。
- **true-D / outage 全链路**：仍 NOT-COVERED，未做任何升级声明。

## 第二次独立复审与外审修复（2026-09-01，最终结论）

**Pass。** 本节取代前文所有阶段性 verdict。编码者返修解决了首轮门禁反例，但独立复审随后在真实 drill 中
定位到 agent runtime freeze，并在外审修复第一版中引入过一次严重的跨进程内存泄漏事故。事故后重新从
kernel OOM、测试二进制入口、helper 生命周期和 deploy drill 四层审起；最终树已封住该失控路径，并通过
全仓门禁与 deploy-tier 复验。这里的 Pass 只覆盖 remote-fs stale-health / spawn runtime 增量；不把既有 #80
或 OQ-2 true-D 硬件缺口洗成已解决。

### 范围与 index 边界

- 编码者第二次返修的 8 个文件已在外审开始修代码前精确加入 index；其结果没有被当作可信证据，全部重新走查和复跑。
- 本节新增的生产修复、回归、tasklist、报告及门禁接线均保持 **unstaged**，以满足“编码者修改已暂存、外审修改留在暂存区外”的区分要求；没有用 `git add -A` 混合边界。
- reviewer 生产面主要是 `internal/spawnexec`、agent exec、PTY start、`Makefile` gates 接线；harness 面是 drill 62 三层 oracle、cursor 日志 helper 与 hermetic contract。

### 最终 findings 与处置

#### ER2-F1 — Major，已修：路径门仍漏 deferred invalidation 与 goto 绕 Unlock

编码者的路径敏感改写关闭了提前 return、条件 Unlock 和 initializer 等已知反例，但复审继续构造出两种假阴性：
`defer p.invalidateHealthy()` 被误当同步证据；`goto` 可跳过 Unlock 后在持锁状态调用 invalidation。门禁现对含
Policy lock+invalidation 的函数遇 defer-invalidation 或 goto/label 一律 fail-closed；正负控制与既有 mutation
battery 均保持原判。

#### ER2-F2 — Major，已修：drill 的 A/B/C 实际无短路，且日志身份可误配

返修注释称 C 只在 B 成立时判产品，脚本却无条件执行三个 `assert_ok`；一次 transport 红会复制成 product 红。
B 的宽泛 `agent: exec` 又能匹配前一请求的晚到 watchdog warning，坏 cursor 还可能退到文件开头。现由
`.terminal`/`.delivered` 标记控制 C，只接受精确 request-start `msg="agent: exec" ... pid=` 且必须恰好一条；
cursor 非数字/读取失败是 rc125 harness 红，cleanup 在 trap 安装前定义。新增 hermetic contract 钉住这些关系。

#### ER2-F3 — Critical，已修：直接 `cmd.Start` 超时并没有隔离 Go runtime

当前镜像首次 drill 仍稳定得到 1S transport timeout。对隔离容器 agent 发 SIGQUIT 后，dump 显示第一条风险
`cmd.Start` 卡在 execve 路径，下一条 reply 的 JSON 分配触发 stop-the-world，GC 等不到该线程安全点，进而冻结
heartbeat、timer 和 NATS 回包。这解释了“agent 已记 request-start、ctl 却 45s 无终态”。safe exec 与 PTY run
现跨本地 re-exec helper 边界，agent runtime 只等待可取消 pipe，不再直接触碰风险 execve。

#### ER2-F4 — Critical，已修且事故登记：第一版 helper 形成跨重启进程/内存泄漏

第一版外审修复在 helper 内再次 `exec.Cmd.Start`：每个风险启动保留一个完整 Go helper，再 fork 一个目标；D-state
恢复前两者不能回收，agent 重启又会重置进程内 64-slot 计数。裸跑测试/模拟最终留下 5,789 个 `exe`，合计约
307 GiB RSS，耗尽 251 GiB RAM 与 8 GiB swap，触发 global OOM；共享 tmux、推理 replica 与 `agent.test` 被连锁杀死。
这是外审引入并造成的事故，不归因给编码者。

最终 helper **自身原地 `syscall.Exec`**，目标 PID 必须与 helper PID 相同；不再二次 fork。抽象 AF_UNIX 的
CLOEXEC 租约把同 UID、跨 agent 重启仍存活的 pre-exec helper 全局限制为 64，且在潜在卡住的 `chdir` 前取得；
成功 exec 自动释放，卡住则一直占槽。环境私有 marker 从继承 env 与显式 RPC env 双向剥离。

#### ER2-F5 — Critical，已修：`/proc/self/exe` 在未接线的 `*.test` 中递归执行整套测试

第一版只给三个已知包手写 `TestMain`。但 `/proc/self/exe` 在 CLI/P4/P5 等测试里是各自的 `*.test`；未接线
二进制收到 helper env 后会重新跑整套测试，递归产生进程。这正是事故的另一放大器，也在修后受限复验中被
TasksMax=512 再次安全拦住。分派现由 `internal/spawnexec.init` 对所有链接者统一执行，入口保持 package-private；
架构门禁止把责任重新散回 main/TestMain。`test/cli_e2e` 从多项 20–30s timeout 恢复为约 14s 全绿。

#### ER2-F6 — Minor，已修：私有 spec fd 会穿过成功 exec 泄漏给目标

成功 `syscall.Exec` 不运行 Go defer；仅 defer-close 的 fd 3 因此会进入长寿命目标。helper 现于 decode 完成立即
显式关闭 spec fd，测试同时验证目标看不到 helper marker 与 `/proc/self/fd/3`。

### 最终 deploy 证据

从最终源码重建 `tether-sim:dev`（baked nats-server 2.10.22 与 pin 一致）后，单跑 drill 62：

```text
DRILL-VERDICT verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0
not_covered=1 nc_gap=1 nc_guard=0 pass=41
```

1S-2 与 1S-3 的 transport/delivery/product 六个 oracle 全 PASS；第一条返回
`remote_fs_spawn_timeout`，第二条返回 `remote_fs_unhealthy`，证明 stall evidence 同步作废旧 healthy verdict。
唯一 gap 是既有 OQ-2：FUSE 只能构造可恢复 T/S-state，不能在共享主机安全构造 true uninterruptible-D。
drill trap 后无实例/容器残留。#81 因此关闭并删除临时 band；contract 反向要求任何新 A/B/C 红都裸露为 deviation。

### 最终验证矩阵

| 验证 | 最终结果 |
|---|---|
| focused spawnexec / architecture / CLI E2E | PASS；含同 PID 原地 exec、显式空 env、marker/fd 隔离、跨生命周期全局 ceiling |
| affected race：spawnexec / PTY / agent | PASS |
| `make test` | PASS；含 all tags vet、Darwin cluster build、全仓 Go tests |
| `make gates` | PASS；spawnexec 已加入 recipe，lint `0 issues` |
| `make e2e-parallel E2EPAR_FLAGS=-workers=8` | ALL PASS；15/15 顶层、99 units，RemoteFSMatrix PASS |
| simcluster hermetic | 目标 oracle contract PASS；全套唯一红为既有越界 `UNOWNED #80` |
| 当前源码 image + drill 62 | 预期 INCOMPLETE，41 pass、0 产品/assert/setup 红、唯一 OQ-2 gap |
| shell syntax / gofmt / `git diff --check` | PASS |

所有可能扩张进程的复验均运行在 `systemd-run --user` 临时 scope 中，设置 MemoryMax、MemorySwapMax=0、TasksMax、
RuntimeMaxSec、OOMPolicy=kill 与 KillMode=control-group；未再裸跑。

### 仍有疑惑、NOT-COVERED 与建议

1. drill 的 delivery 身份仍是严格串行窗口内的 cursor + request-start，不是 wire request ID；本 drill 足够，但未来若允许并发同 argv，应增加 request ID，不能扩大当前 oracle 的适用范围。
2. true-D 与 mode:off-without-safe 仍需专用硬件，不能从 FUSE T/S-state 结果外推；OQ-2 保持唯一 gap。
3. 全局 64 槽使用 Linux 抽象 AF_UNIX 语义，符合当前 agent 部署面；若未来声明非 Linux agent 支持，必须提供等价的跨进程租约实现和平台测试。
4. 全局 ceiling 目前只通过启动错误暴露，没有独立 metric/admin 观测。建议后续增加 active lease/ceiling rejection 指标，但这不是本批正确性阻断项。
5. #80 ledger owner 仍缺失，属于另一批 proxy lifecycle 工作面；本批没有修改或豁免它。#82 的剩余现场归因也未借 #81 修复宣称关闭。
