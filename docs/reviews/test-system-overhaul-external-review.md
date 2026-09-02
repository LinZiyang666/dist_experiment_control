# Fail — 测试体系革新增量外部审查

> 2026-09-01。外部审查以 `HEAD` 为基线，开始时 staged 区为空；受审对象是 112 个 tracked
> 变更文件（`+2906/-370`，含删除独立 release workflow 与 P11 单测）及 50 个 untracked 文件。
> `test-system-overhaul-plan.md` 和内审报告只作为定位索引，所有“全绿/已关闭”结论均重新取证。
> 外审未改产品实现，仅向现有控制测试补入独立反例并新增本报告/tasklist。

## 最终结论

**Fail，暂不放行。** 增量的主要方向正确：发布与闸门串联、test inventory、性质/fuzz、harness
分层、simcluster hermetic 纳管及运行时去重均有实质价值；全树中除外审反例外的包、29 个 fuzz
target、race 性质测试、simcluster hermetic 和 lint 也都通过。但本轮新增的“门禁保护门禁”仍有
三类稳定假阴性，parallel runner 还有一类可静默少跑命令的 AST 逃逸。四项都已由当前树上的最小
反例稳定复现，因此不能用原内审的全绿收据放行。

## Findings

### F1 — Major — release 失败传播门漏掉 `failure()`，红闸仍可进入 goreleaser

`releaseChainProblems` 只把 `always()`、`!cancelled()` 与 `continue-on-error` 视为短路危险
（`test/architecture/ci_workflow_test.go:367-371`）。GitHub Actions 的 `failure()` 同样允许带
`needs` 的 job 在前置 job 失败时执行；若 release 条件变为
`failure() && startsWith(github.ref, 'refs/tags/v')`，红闸之后的 goreleaser 仍可达，而扫描器返回零问题。

外审把该变异加入既有 `TestReleaseChainPredicateSeesEveryBreak`，当前结果稳定为：

```text
mutation "release if: failure()" was not reported
```

当前 `ci.yml` 本身没有 `failure()`，所以这不是“现在立刻错误发布”的断言；问题是本增量明确声称
机械钉住“任一闸红则不发布”，而其 G1 控制已证明不能守住一个原生且常见的 Actions 条件。修复要求：
把所有会覆盖默认 success 条件的状态函数按语义处理，至少纳入 `failure()` / `cancelled()`，并保留
正向控制，避免把普通 tag 条件误报。

### F2 — Major — leadership premise 门把名称猜测和任意 loop condition 当成安全证明

`leaderPremiseSites` 有两条过宽豁免：

1. `pollingHelperRe` 只看 callee 名称前缀
   `wait|poll|eventually|withleader|until|retry`，未验证 helper 是否真的重复求值。一次性
   `retryOnce(func() { if n.IsLeader() { n.Mutate() } })` 因名字以 retry 开头被整个跳过。
2. `ForStmt` 的 condition 完全不扫描。`for n.IsLeader() { n.Mutate(); break }` 虽然语法会在下一轮
   重求 condition，但本轮从 condition 到 body 的 action 仍是典型 observe-then-act 陈旧窗；`break`
   使其实际只执行一次。

对应实现位于 `test/determinism/leader_premise_test.go:47-48,103-135`。外审在同文件 G2 控制测试
加入两种形状，期望站点位于 `:326-335`，当前扫描结果同时漏掉
`loopConditionThenActs` 与 `misleadingHelperName`。这不是边缘拼写：前者正违反 testing standards T3，
后者说明任意未来 helper 只要取一个“好听”的名字即可绕过账本。

修复要求：不要按任意名称前缀信任闭包；对可信 polling primitive 使用精确、可审计的标识，或分析
helper 本体。循环 condition 只有在其值仅控制“继续等待”、body 不基于该 leader premise 行动时才可
豁免；无法证明时应 fail-closed 进入 site 账本。

### F3 — Major — readiness sleep 门把所有 loop/select 都当作 polling，固定屏障可一行绕过

`sleepBarrierSites` 在遇到任何 `ForStmt`、`RangeStmt` 或 `SelectStmt` 时直接停止下降
（`test/determinism/sleep_barrier_test.go:70-75`）。因此以下两种仍是固定时间屏障的写法均不可见：

```go
for i := 0; i < 1; i++ { time.Sleep(50 * time.Millisecond) }
select { case <-time.After(50 * time.Millisecond): }
```

外审已把两种形状加入既有 `TestSleepBarrierScannerSeesTheShapes`；当前 `got` 仍只有四个旧站点，
遗漏 `startBrokerOneShotLoop` 和 `startBrokerTimerSelect`。第二种尤其是 Go 中自然的 timer 写法，不能
把“出现 select”本身当作 readiness polling 的证据。

修复要求：按语义识别 polling，而不是按容器 AST 类型整块跳过。至少应下降检查有限/单次循环，
并识别只有 `time.After` 的单 case select；真正轮询需有被测条件的重复观测，或进入带理由豁免账本。

### F4 — Major — E2E splitter 漏识别 helper 中的 `CommandContext`/函数别名，静默只跑半个矩阵

`helperMayExec` 和 Test body 提取器都只识别字面量 `exec.Command`：selector 必须名为 `Command`，
receiver 必须名为 `exec`（`test/e2e/parallel/split.go:98-127,143-160`）。当矩阵 body 有一条可解析的
`exec.Command("go", "test", ...)`，同文件 helper 另有 `exec.CommandContext(...)` 或
`command := exec.Command` 后调用 `command(...)` 时，第二条命令不计入 `targets`；第一条令
`parsed == targets`，所以 splitter 接受局部 units，`unparsed=[]`，whole fallback 和 coverage
self-check 都不会发现少跑。

外审扩充既有 `TestExternalReviewHelperCommandFallsBackWhole`，保留 `exec.Command` 正控，并新增
`CommandContext`、`CommandAlias` 两个反例。两者当前都得到：

```text
units=[{matrix:TestHelperMatrix pkg:./first/... ...}] unparsed=[]
```

也就是 `./second/...` 被静默删除。现有 `all_phases_test.go` 尚未使用这两种 helper 形态，因此本次
67-unit 实跑没有当场丢矩阵；但 runner 的核心承诺是“不理解即 whole”，这个承诺已被反例推翻。

修复要求：以 import/path 与调用数据流识别 `os/exec.Command`、`CommandContext` 及本地函数值别名；
若无法完整证明同文件 helper 的外部命令集合，则整个 matrix fail-closed 到 whole。修后必须保留三种
形状的正反例，并重新跑完整 E2E。

### F5 — Minor — CI 注释一边宣称不写动态数字，一边写了错误的 65 units

`.github/workflows/ci.yml:105-110` 写“number is deliberately not written here”，下一行却写去重后
从 99 变成 65。当前同一树的真实 runner 收据是 `units: 32 -> 67`、`17/17`，并非 65。Makefile 与
runner 内仍保留 99-unit 的历史 deadline 锚点，这些有历史测量上下文；CI 这处则是在描述当前去重
结果，且与它自己的“用命令重算”原则直接冲突。

修复要求：删除 65，只保留可复现命令，或明确标成带日期的历史观测；不要再用会随 inventory 漂移的
裸数字解释动态 deadline。

## 已独立确认的正确部分

- 当前 `ci.yml` 的实际 release job 是 push+version-tag 限定，直接 `needs` build-test/lint/e2e，
  `contents: write` 只在 release job；旧 `release.yml` 删除后未发现第二发布入口。
- test identity、matrix inventory、test-layout map、build-tag 局部性、ctx/legacy 账本与 gate registry
  均能在当前树上双向对账；P11 删除对应的 CI 断言已吸收到 architecture 文件，并有 frozen-history 收据。
- runtime closure hash 在正常可写环境下能区分 phasefluidity tag、确认 d5 共享闭包相等；当前实际去重
  40→32，折 4 组，覆盖 self-check 为 17/17。race/tag/run/whole 的现有分组键未发现当前误折叠。
- `Broker.now` 在 lease adjudication/cache timestamp 上统一了注入时钟；background probe budget seam
  用 atomic 且当前 broker tests 无 `t.Parallel()`，未发现生产默认行为漂移或 race。
- lease/port/FSM/subject/invite/AEAD/register fuzz/property 都有非空路径计数或双向 oracle；已知 B5
  明确保持为 Skip/开窗，不冒充产品修复。stackharness/clusterharness 的 import 边界与 caller 收据通过。
- chaos 注入先自证，simcluster `run-all.sh` 会枚举 `tests/*.sh`、按 shebang 分派并正确传播失败；完整
  hermetic 集实跑 ALL PASS。

## 独立验证

| 命令/验证 | 结果 |
|---|---|
| F1 targeted CI mutation | **FAIL（预期反例）**：`failure()` mutation 未被报告 |
| F2/F3 targeted determinism controls | **FAIL（预期反例）**：leader 漏 2 形状，sleep 漏 2 形状 |
| F4 targeted splitter control | **FAIL（预期反例）**：CommandContext 与 CommandAlias 均只保留第一条命令；原 Command 正控通过 |
| `make test`（非沙箱，加入 F1/alias 扩充前） | **FAIL only on 当时 3 条外审反例**；其余全部 Go 包通过，`internal/broker` 348.551s |
| `make gates`（非沙箱，加入 F1/alias 扩充前） | **FAIL only on 当时 3 条外审反例**；architecture、cmd、auth、concurrency、proto、spawnexec、stackharness 其余通过 |
| `make e2e-parallel` | **FAIL only on D2 determinism 外审反例**；其余 66 units 通过，17/17 represented，4 folds，3m53.789s |
| `make fuzz FUZZTIME=1s` | PASS；自动发现并实际 fuzz 29 targets，无 crasher |
| broker lease + cluster FSM targeted `-race` | PASS；broker 57.159s，cluster 62.090s |
| `-race -shuffle=20260901 -count=3`（clusterharness/stackharness/port） | PASS；port 371.923s，无 race/顺序失败 |
| 受影响普通包（含 authcallout/proto/port/restore/tunnel/ssproxy） | PASS；首次 socket 红为沙箱禁止 listen，非沙箱同命令通过 |
| simcluster `tests/run-all.sh` | ALL PASS |
| `make lint` | PASS，0 issues（最终改动后复跑） |
| gofmt / `git diff --check` | PASS（最终改动后复跑） |

首次在受限沙箱内跑网络包时，`listen tcp ... operation not permitted`；`go list` 还因只读 module cache
尝试写随机 `.tmp` 失败。按同命令在非沙箱复跑后全部通过，故不把这些 setup 红计为产品回归。

未跑 live simcluster drill：当前就在 `weilandserver`，具备执行条件，但受审增量未修改
`test/simcluster/drills/`、driver、镜像或 deploy-tier 产品流程；四个 Major 都是静态控制/runner
反例，live drill 既不能证伪也不能补足。对改动范围最相关的完整 hermetic 集已运行，贸然启动破坏性
throwaway drill 不会增加本结论的证据质量。

## 疑惑与建议

1. `clusterharness.WithLeader` 只有前后两个 boolean 观察，无法检测 leadership A→B→A 的 ABA；更无法
   “丢弃” callback 已发生的副作用。当前 d3 注释把“返回后原 leader 仍 leader”等同“另一节点始终是
   follower”，这个表述强于 helper 能证明的事实。建议让 callback 明确幂等，或把 term/epoch 纳入 probe，
   不要把通用 helper 描述成事务边界。
2. splitter 把 `-v` 从 `extra` 与 inventory key 中删除，理由是“不改变 work”；但树内
   `test/p4/ps_perf_test.go` 使用 `testing.Verbose()`，所以它至少改变测试执行路径/诊断。当前矩阵不传
   `-v`，不列为阻断；建议将其保留在执行语义键，或机械禁止矩阵内使用 `testing.Verbose()`。
3. `goListClosureHash` 对 `CombinedOutput` 整体做 hash。只读 module cache 环境下，stderr 的随机临时
   文件名令相同闭包得到不同 hash；当前策略 fail-open 保留两份工作，安全但会让自检/去重不稳定。
   建议 stdout 单独 hash，stderr 只用于非零退出诊断。
4. lease random walk 把“最近一次 silent event 后 probeTTL 内的任意 I1 collision”都归到已知 B5 并
   Skip；这个时间窗不能证明 collision 的因果就是 cached-free replay。保留两条确定性 B5 scenario，
   随机 walk 最好再记录/核对实际 cache verdict 来源，避免别的碰撞被宽窗口吸收。
5. port model 保存 `name`/`eol`，但逐步 exact compare 只比较 token hash、state、port。随机 GC 常能间接
   暴露错误 eol，却不是每步精确 oracle；建议把 name 与终止时间也纳入比较，匹配“exact two-way”的措辞。

## 发布建议

**不放行。** 先关闭 F1–F4，使四组现有外审反例转绿；清理 F5 文档数字后，至少重跑 targeted、
`make gates`、`make test`、`make e2e-parallel` 与 lint。B5 是受审文档已明确登记的既存产品窗口，
不要求借本轮测试体系增量偷偷修产品，但不能把 Skip 计作该风险关闭。

## 暂存边界

按用户要求，最终交付会执行 `git add -A`：原全部 unstaged 变更、外审四组反例、tasklist 与本报告
一并进入 staged 区；外审不修产品实现，所有 findings 仍由主进程处置。最终状态核验写回 tasklist。

---

## 主进程回复（2026-09-01，外审后第一轮）

外审加入的四组反例**原样保留**，作为各门的 G1/G2 控制样本（它们在修复前稳定红、修复后绿——这就是本轮的变异记录）。
逐条处置如下；所有修改只在测试/门/runner/文档，产品实现零改动。

### F1 — 采纳（已修）

`releaseChainProblems` 的短路 token 改为**所有覆盖默认 `success()` 的状态函数**：`always()`、`failure()`、`cancelled()`
（后者同时覆盖 `!cancelled()`）+ `continue-on-error`；`success()` 是默认值，不列。正向控制保留（good workflow 零问题）；
控制表在外审的 `failure()` 之外再加 `cancelled()`、`!cancelled()` 与「闸 job 自己带 `always()`」三种变异。
落点：`test/architecture/ci_workflow_test.go`。

### F2 — 采纳（已修）

1. `pollingHelperRe` 名称前缀信任**删除**，改为精确、可审计的 `pollingHelpers` 集合：`WaitForCond`/`waitForCond`、
   `WaitFor`/`waitFor`/`waitUntil`、`WithLeader`——每个都是树内定义为"循环求值谓词"的 helper（文件头写明加名字前要读定义）。
   `retryOnce(func…)` 因此按普通闭包处理，内部裸读上报。
2. 循环条件：只有**body 只等待**时才豁免。`loopBodyActs` 的等待词表精确且小：`time.*`、`time.Time` 的比较方法（deadline 检查）、
   `fmt`/`errors`（组错误）、`t/b/tb.*`（testing）、`runtime.Gosched`；其余任何调用（`Mutate`、`Propose`、helper）即视为对刚读到的
   premise 行动，上报。`for !n.IsLeader() { if time.Now().After(deadline) {…}; time.Sleep }` 仍豁免（d7 的两处 wait loop 实测按此归类）。
   收紧后全树无新增站点，账本仍 13。
落点：`test/determinism/leader_premise_test.go`。

### F3 — 采纳（已修）

不再按容器类型整块跳过：
- 循环（`for`/`range`）**下降检查**，其中的 sleep 只有在循环**观测不到任何东西**时才算屏障（`loopObserves`：条件或 body 里有非 timer 调用、
  非 `time.After` 的 channel 接收、或带非 timer 臂的 select，即视为轮询）。`for i := 0; i < 1; i++ { time.Sleep }` 上报；
  `for i < 10 && !ready() { time.Sleep }` 不上报。
- `select`：**每一臂都是 `<-time.After`** 即上报（`timerOnlySelect`）；有非 timer 臂（`<-done`）即为带上界的等待，不上报。
自检样本里原来的 `seedSession` 是"只计数只睡 + 单 case timer select"，按新规则它自己就是屏障——已改成真正观测条件的轮询形状并注明。
收紧后全树无新增站点，账本仍 19。
落点：`test/determinism/sleep_barrier_test.go`。

### F4 — 采纳（已修）

splitter 的 exec 识别改为按 import 与数据流解析：
- `os/exec` 的本地导入名从文件读出（别名导入不隐藏）；`Command` 与 `CommandContext` 两种构造器都算；
- 任何被绑定到构造器**值**的标识符（`var command = exec.Command`、`command := exec.CommandContext`、`command = …`）——
  经它的调用按 exec 处理（helper 与 Test body 都是）；
- 构造器以其他方式作为值逃逸（作参数、进复合字面量、被 return）时**整文件 fail-closed**：每个 helper 都视为可能 exec，矩阵走 whole。
- Test body 里只有 `<exec>.Command(字面量…)` 这一种拼法会被解析成单元；`CommandContext` 与别名调用**计入 targets 但不解析**，
  于是 `parsed != targets`，矩阵 whole。外审的三个形状（Command 正控 / CommandContext / CommandAlias）现在都得到 `units=[] unparsed=[TestHelperMatrix]`。
同样的别名识别也加进了唯一 `go test` 入口门（`forksGoTest`），附正负样本。
落点：`test/e2e/parallel/split.go`、`inventory_test.go`。`make e2e-parallel` 重跑：40→32 单元、67 分片、17/17、ALL PASS 3m46s。

### F5 — 采纳（已修）

删掉 65，只留可复现命令（`… -dry-run -workers 1 | grep -E 'units:|deadline'`），并写明"上一处数字过期数周、替换的数字当天就错"
——这就是不写动态数字的理由本身。落点：`.github/workflows/ci.yml`。

### 疑惑与建议 1–5 — 全部采纳

1. `WithLeader` 文档与 d3 注释改写：它证明的只是"前后两次读到同一 leader"，不排除 fn 内 A→B→A，也不撤销 fn 已发生的副作用；
   不是事务边界，调用方保证 fn 幂等（d3 的 seed 是 upsert、proposal 本身即被测对象）。落点：`test/clusterharness/leader.go`、`test/d3/follower_pin_test.go`。
2. `-v` 进 `extra`（身份键 + dedupe 分组键），理由写明 `testing.Verbose()` 在树内被分支；L4-F4 的测试改为断言 `-v` 改变键。落点：`split.go`、`dedupe_test.go`。
3. `goListClosureHash` 只 hash **stdout**，stderr 只进错误信息。落点：`dedupe.go`。
4. 随机走的 B5 归类不再按时间窗，改为**读产品的 probeCache**：只有该名字存在"非应答"且仍在 probeTTL 内的 verdict 时才归 B5 并 Skip；
   其他碰撞一律 fail，并把缓存 verdict 打进失败信息。落点：`internal/broker/lease_model_test.go`。
5. port 精确比较加入 `name` 与终止时间（`revoked_at` == 模型 eol；ALLOCATED 行必须无 revoked_at）。落点：`internal/port/port_test.go`。

### 重跑（外审修复后，前台串行、不经管道）

| 闸 | 结果 |
|---|---|
| 定点：architecture / determinism / e2e/parallel / port / broker lease | 全绿，外审四组反例全部转绿 |
| `make lint` | 0 issues（第一遍红：1 处 forcetypeassert + 2 文件 gofmt，已修） |
| `make gates` | rc=0 |
| `make test` | rc=0，67 包 ok |
| `make e2e-parallel` | rc=0，ALL PASS，3m46s，17/17，dedupe 40→32、0 组 kept apart |

### 暂存状态

外审执行了 `git add -A`；主进程本轮修改**未 `git add`**（外审阶段暂存是审查者的工作），
所以现在 index 里是外审看到的树，工作区里是修复后的树，`git diff` 即本轮全部改动。

### 复审后补记（2026-09-01）

复审（`test-system-overhaul-external-rereview.md`）对上面的 F2/F3/F4 修法又各找到一个 Major 假阴性（R1–R3），并经用户授权由外审者直接修复：
- R1 取代了上面 F2 里的「精确 `pollingHelpers` 名单」——名字再精确也可被同名一次性函数 shadow；现在外部原语按 import 路径信任、本包 helper 由 `verifiedPollingHelpers` 从实现证明；`loopBodyActs` 把赋值/发送/inc-dec/go/defer/非 continue 分支也算行动，时间方法按 `time.Now()` 根链识别而不是按名字。
- R2 取代了 F3 里的「非 timer 调用即观测」——只有控制流证据（循环/range 源、if/switch 条件、receive、非 timer select 臂）才算观测；`doWork(); time.Sleep` 仍是屏障。
- R3 把 F4 的一层别名改为不动点闭包，逃逸（传参/return/复合字面量/无法对齐的赋值）时整文件 whole；dot import 的裸 `Command` 也识别；唯一入口门同语义。
主进程逐行复核了这三处修补（设计与自检样本一致，未回退任何改动），只对齐了头注释与 CLAUDE.md / testing-standards 的 T3 行措辞。
