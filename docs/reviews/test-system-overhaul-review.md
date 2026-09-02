# 测试体系革新增量 — 内审报告（CLAUDE.md §3 step 4–5）

- 日期：2026-09-01
- plan：`docs/reviews/test-system-overhaul-plan.md`（§-1～§8 定稿文字不改；落地偏离登记在 **§9**）
- 基线：`docs/reviews/test-system-overhaul-baseline.md`
- 审查形式：Workflow `wf_41250fdd-5ae`，13 个 agent（6 条 lane 独立审查 → 6 个 verifier 逐条对抗核实 → 1 个 synth 综合）。
  第一次运行被会话额度中断（2 lane 完成、11 失败），resume 后全部完成；docs-truth lane 因缓存未命中跑了两次，
  两份结论一致，本报告以第二份（verifier 看到的那份）为准。synth 的返回文本在 workflow 结果里**头部被截断**
  （从 §6.1 中段开始），所以下面的逐条处置以 6 条 lane 的原始 finding + verifier 裁决为底稿，synth 只取其 §6.2 偏离表与 §7 结论。
- 审查者规则：只读实现、可提 proposed_test、**不改实现**；变异只在 cp→sed→test→复原 的循环里做，事后 md5 核对。
- 硬闸状态见 §5；三硬闸全绿后停在 step 6 等外审，**不 commit、不 push、不 git add**。

## §0 结论

- **内审裁决（synth）：Fail，不 block。** 67 条 finding 无一指向产品在生产上的错误行为；所有 MAJOR 都是「门/文档说的比做的多」
  与「plan 点名未做且未登记」。Fail 的理由：一条 G1 记录为假（B3 ②）、两份收据各含谎言（身份 golden 7 条幻影、矩阵 golden 对 5 个 whole 矩阵不透明）、
  三道门名不副实（-race 归属、唯一 `go test` 入口、发布链）、`make fuzz` fail-open、四处活文档与树相悖、偏离清单 6 条 vs 实际 19 条。
- **主进程处置：全部 67 条 finding 逐条处置（§2），synth §7.1 的 12 项「外审前必须处理」全部完成，§7.2 的「同批捎带」全部完成，
  §7.3 建议「留到下一增量」的 20 项中 16 项也在本轮完成**（它们多是几十行的谓词收紧，等下一增量只会再被发现一次）。
  未做的 4 项与新登记的偏离见 §3。
- verifier 对 67 条的裁决：**66 条 CONFIRMED、1 条 DOWNGRADED**（L4-F6：`-race` 不进闭包 hash，事实成立但今日无 race 门控文件；仍采纳，零成本）。
  verifier 另补 6 条「审查者漏看」（§2.7），其中 5 条采纳、1 条登记。
- 每条新增/收紧的守卫都做了 G1 变异（§4）：**15 条变异 15 红**，另有 1 条（B3 ②）如实登记为「不可达、G1 未通过」。

## §1 审查面（各 lane 声明的检查范围）

| lane | 看了什么 | 没看什么 |
|---|---|---|
| L1 gates | 本增量 12 道新/改门的谓词与自检，逐条静态论证 + 注入 | 未跑三硬闸；proposed_test 未编译 |
| L2 properties | 4 个性质测试的模型/oracle 与被测实现逐行对照；6 个 Fuzz 目标的 oracle；chaos 9 处证明 | 未施变异；未 -fuzz |
| L3 product-ci | 唯一生产改动（broker.go）等价性；ci.yml 逐行；run-all.sh；Makefile fuzz 转义 | GitHub Actions 语义来自文档记忆 |
| L4 runner | dedupe/split/main/inventory 逐行；dry-run 实跑；`-deps -test` 闭包语义实证 | 未在树上变异 |
| L5 docs-truth | CLAUDE.md / testing-standards / README / 59 个文件头 / plan §-1 与 §5.1 逐条对账 | 未跑硬闸 |
| L6 scope | 132 项改动逐项 vs plan §3/§4/§5.1；重做 5 条 G1 变异 | 未跑硬闸；B2 其余 3 条 G1 未重做 |

## §2 逐条处置

记号：**采纳** = 已按建议修改（可能有形式差异，注明）；**部分** = 采纳其中可执行的一半，其余登记；**登记** = 不改代码，写进树内注释或 §9 偏离表；**驳回** = 附理由。

### 2.1 L1 gates（13 条）

| # | 严重度 | verifier | 处置 | 落点 |
|---|---|---|---|---|
| L1-F1 身份 golden 7 条幻影（raw string 里的 `func Test…`） | MAJOR | CONFIRMED | **采纳**：提取器改 go/parser（`testFuncDecls`：顶层无 receiver FuncDecl + cmd/go isTest 规则），命名冻结共用同一提取器；golden **手删 7 行**；自检加 raw/interpreted 字符串幻影与 `Testify` 负样本；解析失败即错不静默 | `test/determinism/test_inventory_test.go`、`test_naming_test.go`、`testdata/test_function_inventory.txt` |
| L1-F2 -race 归属只读手写 map，agent 泄漏门从未在 -race 下跑 | MAJOR | CONFIRMED | **采纳**：新增 `TestLeakRiskMatrix`（`-race ./internal/agent/... ./internal/tunnel/...` 整包，实测 26s）；`raceCoveredByWholeMatrix` **删除**，门只认「可解析单元 + -race + 无 -run」 | `test/e2e/all_phases_test.go`、`test/e2e/parallel/inventory_test.go` |
| L1-F3 `make fuzz` 无下限、吞 stderr | MAJOR | CONFIRMED | **采纳**：`-list` 失败即 exit 1，计数 n，`FUZZ_MIN`（默认 28 / 收窄时 1）；G1 f 变红 | `Makefile` |
| L1-F4 `always()`/`continue-on-error` 绕过发布门 | MAJOR | CONFIRMED | **采纳**：并入 `releaseChainProblems`（release + 三闸 job 禁三种 token），控制样本各一 | `test/architecture/ci_workflow_test.go` |
| L1-F5 唯一入口谓词看不见 `append(base…)`/`CommandContext` | MINOR | CONFIRMED | **采纳**（与 L4-F3 合并）：fail-closed 谓词 + 别名解析 + 扫描面扩到 `test/**/*.go`、`internal/testharness/`；`layering_test.go` 的 `exec.Command("go", args...)`（go list）进带 cap 的例外表 | `inventory_test.go` |
| L1-F6 两道 sleep 门可用 `<-time.After`/IIFE 绕过 | MINOR | CONFIRMED | **采纳**：`isBareSleepStmt` 认两种拼法、IIFE 体视为本体；产品计时门认 `time.After`；自检各加样本；G1 g 变红 | `sleep_barrier_test.go`、`raft_timing_guard_test.go` |
| L1-F7 leader_premise 把任何 func literal / 循环当安全 | MINOR | CONFIRMED | **采纳**（与 L6-F9 合并）：只有轮询 helper 实参的 func literal 与 for 条件本身安全；循环体、IIFE、select 臂、`.State()==raft.Leader` 都报；账本 5→**13**；G1 h 变红 | `leader_premise_test.go`、`legacy_leader_premise_sites.go` |
| L1-F8 六本例外表无 cap、四本逃过键单位登记 | MINOR | CONFIRMED | **采纳**：`simGateSetExclusions`(0)、`leakCoverageExceptions`(0)、`integrationTagExceptions`(1)、`legacyProductTimingSleeps`(0)、raft `exempt`(7)、`goForkExceptions`(1) 各加 `!=` cap；登记正则扩到 `*Exceptions/Exclusions/Exemptions`，键单位新增 `package` | `gate_standards_test.go` 等 |
| L1-F9 语料目录不核对 `func FuzzX` 存在 | MINOR | CONFIRMED | **采纳**：`orphanedCorpora` + 自检样本；G1 j 变红 | `fuzz_corpus_test.go` |
| L1-F10 泄漏覆盖把死 helper 里的调用算覆盖 | MINOR | CONFIRMED | **采纳**：只认 `Test*` 函数体内（含闭包）；负样本两条；G1 i 变红 | `leak_assert_shape_test.go` |
| L1-F11 gate_standards 只读首个锚点 | NOTE | CONFIRMED | **登记**：键单位是 file，与声明一致；第二锚点由 promised-guard 兜底 | — |
| L1-F12 `TestNoWorkflowPublishesOutsideCI` 词表窄 | NOTE | CONFIRMED | **部分**：词表加 `gh release`/`action-gh-release`/`upload-release-asset`/`/releases`；「on.push 无过滤即报」不做（grep 门固有近似，注释已说） | `ci_workflow_test.go` |
| L1-F13 ctx 门不覆盖 `context.TODO()`/别名 | NOTE | CONFIRMED | **登记**：今日树三者均为 0；下一增量 | — |

### 2.2 L2 properties（12 条）

| # | 严重度 | verifier | 处置 | 落点 |
|---|---|---|---|---|
| L2-F1 租约随机走无 silent 事件 → 后台探针/缓存/silence rule 0 次 | MAJOR | CONFIRMED | **采纳（plan 的 fallback）**：加 `backgroundProbeBudgetOverride` 测试接缝（atomic，仅随机走用，两个 B5 场景仍用真预算）；走加 `silentThenRegister`（每序列 ≤1）；I1 改比较实际订阅 subject（`liveName`）；规模恢复 60×20（~6s）；实测 silent 生效 44 次。**登记不建模**：以后缀名再注册、`ReconcileStates`（头注释） | `internal/broker/broker.go`（seam）、`lease_model_test.go` |
| L2-F2 端口模型按 port 键，历史行被遮蔽 | MAJOR | CONFIRMED | **采纳**：模型改 token-hash 键，`compare` 精确双向（模型⊆DB 且 DB⊆模型）；G1 l 变红于 compare | `internal/port/port_test.go` |
| L2-F3 FSM 重放只有 1 个 op 族、投影 5 键 | MAJOR | CONFIRMED | **采纳**：三族（set/ReqID dedup/poison）、投影含 `cluster_reqid_ledger`、恢复后再投切点前 ReqID 断言 `appliedDedup`、族地板 ≥20；G1 m 变红 | `internal/cluster/crash_invariant_test.go` |
| L2-F4 B5 Skip 无过期机制 | MAJOR | CONFIRMED | **采纳**：`b5ReplayWindowOpen` 常量 + `expectB5` 双向过期；AfterABackgroundProbe 改为证明前提（subscribe 落在预算内）+ 失败即 fail | `lease_model_test.go` |
| L2-F5 `agentHeartbeatFrozen` 相等性证明会 flake | MINOR | CONFIRMED | **采纳**：改为年龄式（`node.HeartbeatAge` > 3 个间隔）+ `WaitFor` | `test/chaos/chaos_harness_test.go` |
| L2-F6 subject 不动点对删 Validate 全盲 | MINOR | CONFIRMED | **采纳**：`noWild` 第二 oracle（只对已校验的 sid/nid/actor）+ 两条通配符种子；G1 n 变红 | `internal/proto/subjects_boundary_test.go` |
| L2-F7 两个 G2 自检是影子 RNG | MINOR | CONFIRMED | **采纳**：port/lease 走自己计「生效」次数并在末尾断言；两个影子测试**删除**（golden 手删，理由见此表） | `port_test.go`、`lease_model_test.go` |
| L2-F8 I2 shield 基准与产品分叉 | MINOR | CONFIRMED | **采纳**：`shieldExpired()` 读 `b.leaseHolder`，在 register 前取 | `lease_model_test.go` |
| L2-F9 AEAD 注释承诺「分配前」未测；往返无独立 oracle | MINOR | CONFIRMED | **采纳**：头注释改真值；往返加帧长算术 + `firstLength` oracle | `internal/agent/ssproxy/crypto_test.go` |
| L2-F10 `check()` 无反向包含 | MINOR | CONFIRMED | **随 F2 消失** | — |
| L2-F11 `parseRegisterLine` 不钉 port 范围 | NOTE | CONFIRMED | **登记**：fuzz 里写明范围由 tokenLookup 兜底、加范围检查是产品决定 | `internal/tunnel/register_and_fence_test.go` |
| L2-F12 `FuzzParseInvite` 接受集依赖环境变量 | NOTE | CONFIRMED | **采纳**：`f.Setenv(devNoAuthEnv, "")` | `internal/clusterroster/invite_test.go` |

### 2.3 L3 product-ci（7 条）

| # | 严重度 | verifier | 处置 | 落点 |
|---|---|---|---|---|
| L3-F1 发布链可达性只钉 release 自身（needs 的 job 丢 tag 臂 / on.push 加过滤） | MAJOR | CONFIRMED | **采纳**：`releaseChainProblems` 纯谓词读整条链 + `TestReleaseChainIsReachableOnATagPush` + 控制样本 6 种变异；G1 e 变红 | `ci_workflow_test.go` |
| L3-F2 usage.md 指向已删 release.yml | MINOR | CONFIRMED | **采纳**：附录 C 改写；加 `TestLiveDocsReferenceOnlyExistingWorkflowFiles` | `docs/usage.md`、`ci_workflow_test.go` |
| L3-F3 `brokerNow` 与 `(*Broker).now()` 重复 | MINOR | CONFIRMED | **采纳**：删 `brokerNow`，两处改 `b.now().UTC()`，注释整段搬到站点 | `internal/broker/broker.go` |
| L3-F4 `make fuzz` fail-open | MINOR | CONFIRMED | 同 L1-F3 | — |
| L3-F5 ci.yml「99 units / 1h39m」过时 | NOTE | CONFIRMED | **采纳**：数字换成可复现命令 | `.github/workflows/ci.yml` |
| L3-F6 对 tag 手动 dispatch 会再跑 goreleaser | NOTE | CONFIRMED | **采纳**：release `if` 加 `github.event_name == 'push'`，门断言之 | `ci.yml`、`ci_workflow_test.go` |
| L3-F7 artifact 名与内容不符 | NOTE | CONFIRMED | **采纳**：改名 `fuzz-testdata` 并注释 | `ci.yml` |

### 2.4 L4 runner（9 条）

| # | 严重度 | verifier | 处置 | 落点 |
|---|---|---|---|---|
| L4-F1 矩阵 golden 对 5/16 个 whole 矩阵不透明 | MAJOR | CONFIRMED | **采纳**：`wholeMatrixLiterals`（AST 收集 `./` 包字面量、`-run` 值、allPhases 元素，含同文件被调函数）→ `Matrix\|<whole>\|lit=…` 行；非空地板；golden 38→**74 行**（旧 38 行 1:1 保留，已逐行核对）；G1 b 变红 | `inventory_test.go`、`testdata/matrix_units_golden.txt` |
| L4-F2 -race 归属被矩阵名满足 | MAJOR | CONFIRMED | 同 L1-F2 | — |
| L4-F3 唯一入口谓词只认双字面量 | MAJOR | CONFIRMED | 同 L1-F5；G1 c 变红 | — |
| L4-F4 透传旗（-short/-failfast/-count）不进键 | MINOR | CONFIRMED | **采纳**：`unit.extra`（规范化）进 `unitKey` 与 dedupe 分组键；`-v` 明确不进；测试 `TestDedupeAndInventoryKeySeePassThroughFlags` | `split.go`、`dedupe.go`、`inventory_test.go` |
| L4-F5 `-shuffle` 对 whole 单元是空操作 | MINOR | CONFIRMED | **采纳**：`wholeUnitEnv` 经 `GOFLAGS` 合并透传；文档改写 | `dedupe.go`、`main.go`、`docs/testing-standards.md` T4 |
| L4-F6 闭包 hasher 不带 -race | MINOR | **DOWNGRADED** | **采纳**（零成本）：`goListArgs(pkg, tags, race)`；测试钉住 | `dedupe.go` |
| L4-F7 fail-open 静默 | MINOR | CONFIRMED | **采纳**：`foldNote.reason`，kept-apart 也打印；全部出错时 WARNING | `dedupe.go`、`main.go` |
| L4-F8 被整体折掉的矩阵让 self-check 误报 | NOTE | CONFIRMED | **采纳**：`droppedMatrices` + `coveredMatrices` | `dedupe.go`、`main.go` |
| L4-F9 模板/旗名与 plan 文本不一致 | NOTE | CONFIRMED | **采纳**：`dedupe.go` 头注释 + §9 D-13/D-20 | — |

### 2.5 L5 docs-truth（13 条）

| # | 严重度 | verifier | 处置 |
|---|---|---|---|
| L5-F1 golden 幻影（5 条 + verifier 补 2 条 = 7） | MAJOR | CONFIRMED | 同 L1-F1；`go test -list` 对账：见 §4 收据 |
| L5-F2 CLAUDE.md 缺三行 | MAJOR | CONFIRMED | **采纳**：T3 前提 / 泄漏门（覆盖闸）/ build-tag（局部性）三行 + 「seedSession 收据」行 + 确定性 lint 行点名 raft 文件；反向门 `TestEveryAnchoredGateIsNamedInCLAUDEMd` |
| L5-F3 §0.3 T3/T6/「4 条」自相矛盾 | MAJOR | CONFIRMED | **采纳**：三处改写，不再写数字 |
| L5-F4 usage.md | MAJOR | CONFIRMED | 同 L3-F2 |
| L5-F5 §七指向不存在的 harness.go 环规则 | MAJOR | CONFIRMED | **采纳**：`harness.go` 头注释加环规则；`layering_test.go` 加 `internal/testharness` 行（`originalUnion` 登记空继承）；§七 该行指向两处 |
| L5-F6 README「38 个 drill」 | MINOR | CONFIRMED | **采纳**：改为命令 |
| L5-F7 README 门列表过时、「三本账本」、漏 WithLeader | MINOR | CONFIRMED | **采纳**：architecture/determinism 行改为「以 CLAUDE.md §5 为准」的摘要；两本递减 + 零账本；clusterharness 行加 WithLeader；stackharness 行写明收据在 gates 里跑 |
| L5-F8 all_phases 注释「every tag」自相矛盾 | MINOR | CONFIRMED | **采纳**：改「every d*_integration tag」，复现命令补全 d5–d9 |
| L5-F9 simcluster_log_oracle 未锚定 | MINOR | CONFIRMED | **采纳**：纯谓词 `simCallsHelperWithoutSource`/`simInlinedStreamLines` + 控制 + 锚点；账本 13→12，头注释如实；G1 o 变红 |
| L5-F10 hermetic 行措辞 | NOTE | CONFIRMED | **采纳** |
| L5-F11 staleness 测试头注释仍宣称守 younger 守卫 | NOTE | CONFIRMED | **采纳**：头注释写明它钉的是 recheck、不是 younger 守卫；broker.go 守卫处写明不可达 |
| L5-F12 WithLeader 零采用 | NOTE | CONFIRMED | **采纳**：d3 follower PIN 写迁到 `WithLeader`；d7 三处登记（§9 D-1）；`TestWithLeaderHasARealSuiteCaller` |
| L5-F13 floor 注释 2880 vs 2953 | NOTE | CONFIRMED | **采纳** |

### 2.6 L6 scope（13 条）

| # | 严重度 | verifier | 处置 |
|---|---|---|---|
| L6-F1 G1 B3 ② 未被任何测试抓到、简报记「3 条」 | MAJOR | CONFIRMED | **采纳（如实登记）**：守卫在单飞 probeInFlight 下不可达（唯一写者是后台探针自己、inline 定论不缓存、第二个后台探针要等第一个 Store 完），`broker.go` 与 `lease_probe_staleness_test.go` 写明；G1 计数 **B3 2/3**（§9 D-9）。未做 (b)「让模型到达它」：需要第二个缓存写者，那是产品改动 |
| L6-F2 WithLeader 零调用方 | MAJOR | CONFIRMED | 同 L5-F12 |
| L6-F3 -race 归属假收据 | MAJOR | CONFIRMED | 同 L1-F2 |
| L6-F4 usage.md | MAJOR | CONFIRMED | 同 L3-F2 |
| L6-F5 CLAUDE.md 三行；「确定性 lint」按目录点名 | MAJOR | CONFIRMED | 同 L5-F2；目录行保留（`TestEveryAnchoredGateIsNamedInCLAUDEMd` 认目录点名），时序守卫文件另点名 |
| L6-F6 §0.3 / §七 | MAJOR | CONFIRMED | 同 L5-F3 / L5-F5 |
| L6-F7 I3 消失、无 reason 断言、40×12 | MAJOR | CONFIRMED | **采纳**：I3 在头注释写明为何不是本产品的不变量；reason 不可表达（登记 §9 D-3）；60×20 |
| L6-F8 账本头「Four」记假 | MAJOR | CONFIRMED | 同 L5-F9 |
| L6-F9 leader_premise 循环体 | MINOR | CONFIRMED | 同 L1-F7；账本 13 |
| L6-F10 生成器订阅间隔与被变异常量同增同减 | MINOR | CONFIRMED | **采纳**：`modelSubscribeTurnaround = 3s` 固定；G1 k：`leaseGrantWindow=1s` 现在随机走也红（seed 27） |
| L6-F11 「矩阵源一字不改」为假 | MINOR | CONFIRMED | **采纳**：CLAUDE.md 改「只为归属加包，不为去重改」；D1 注释写 clusteroffline -race 实测 ~81s 与 300s 推导 |
| L6-F12 六个数字不符 | NOTE | CONFIRMED | **采纳**：§9 D-18 登记；文档正文不再写这些数 |
| L6-F13 stackharness 收据不在 gates | NOTE | CONFIRMED | **采纳**：gates recipe 加 `./test/stackharness/`，CLAUDE.md 加行 |

### 2.7 verifier 补充（「审查者漏看」）

| 来源 | 内容 | 处置 |
|---|---|---|
| runner verifier | `riskyPackages` 扫描面只有 cmd/+internal/；`test/concurrency` 直接 import tunnel，只在 RemoteFS 矩阵的 `-run Spawnsafe\|Leak\|FDStable` 子集下跑 -race（29 个 Test 中 14 个被排除，含 4 个隧道测试） | **登记**（§9 D-23）：`go test -race ./test/concurrency/` 整包实测 48s；该包含 T5 端口 TOCTOU 的已知瞬时 flake（本轮 `make gates` 曾红一次），整包进矩阵前先修 T5 |
| gates verifier | `build_tags_test.go` 无锚点且因 CLAUDE.md 行不写 `_test.go` 路径而对 gate_standards 不可见 | **采纳**：CLAUDE.md 行点名文件；加 `// gate-control: TestBuildTagsReconcilerIsNonVacuous` |
| product-ci verifier M1 | `ciJobBlocks` 把注释计入 job body，且 job 上方的说明注释落到前一个 job | **采纳**：`splitJobBlocks` 剥注释；控制样本加注释外溢用例（本轮先被它抓到：release 注释里的 `refs/tags/v` 让 fuzz 门变红） |
| docs-truth verifier | 幻影共 7 条（`mixed_command_back_test.go` 2 条首快照就收了） | **采纳**（已删 7） |
| properties verifier | 模型对 SUFFIXED 授予立即写 nodes 行，产品 contested register 写零行；`plan_test.go`/`PlanGCTerminated` 在模型外 | **部分**：改成只对裸名写行后随机走出现假 I1（后缀实例 live 于 gpu1-03 而无行认领，下一个克隆被再次提供同一后缀），于是**恢复写行并在头注释写明**它代表实例以新名的第二次注册；`PlanGCTerminated` 登记 §9 D-11 |
| scope verifier | §5.3 边际成本仍是估值；`make gates` 由 1m31s 变 ≈3.5 min | **采纳**：§5.3 与 CLAUDE.md 写实测（§5） |

## §3 未做与新登记

- 未做（都留给下一增量，理由见 §9）：L1-F11（第二锚点）、L1-F13（`context.TODO()`/别名）、L1-F12 的「on.push 无过滤即报」、
  d7 三处 `adminForLeader` 迁 `WithLeader`（D-1）、`test/concurrency` 整包 -race（D-23）、5 处内联 NumGoroutine + `resnapshot_test.go`（D-15）。
- 新登记的偏离：§9 D-1～D-23（synth §6.2 的 19 条全部登记 + 本轮新增 4 条）。

## §4 G1 变异记录（内审后新增/收紧的守卫）

脚本 `g1_mutations.sh`（job tmp）：cp 备份 → sed 注入 → 单包 `go test` → cp 复原 → md5 核对；`git status` 项数前后一致。

| # | 守卫 | 注入 | 结果 |
|---|---|---|---|
| a | 身份提取器 | 去掉 isTest 过滤（`Testify`/方法被计） | 红 |
| b | whole 矩阵字面量 | 删 `run("pty", …)` | 红（golden missing） |
| c | 唯一入口谓词 | argv[1] 非字面量视为非 test | 红 |
| d | -race 归属 | `TestLeakRiskMatrix` 删 agent | 红 |
| e | 发布链可达性 | e2e `if` 删 tag 臂 | 红 |
| f | `make fuzz` 地板 | `FUZZ='^FuzzNope$'` | 红（rc=2） |
| g | 就绪 sleep 门 | 去掉 `time.After` 臂 | 红 |
| h | leader_premise | 所有 func literal 视为安全 | 红 |
| i | 泄漏覆盖 | 任何函数内的调用都算 | 红 |
| j | 语料孤儿 | `FuzzSOCKS5Reply` 改名 | 红 |
| k | 租约模型 | `leaseGrantWindow = 1s` | 红：CloneInsideTheSettleWindow + SubscribesLate + **随机走 seed 27**（L6-F10 修复前随机走全绿） |
| l | 端口模型 | GC 无视 cutoff 删 FREED 行 | 红：`compare` 在 step 22 gc 报「model row … vs DB (present=false)」 |
| m | FSM 重放 | `reqIDSeenTx` 恒 false | 红 |
| n | subject noWild | `ParseEvProc` 不校验 nid | 红（种子 `*`） |
| o | 日志 oracle 谓词 | 注释行计数 | 红 |
| — | B3 ② younger 观测守卫 | 恒 false | **绿（G1 未通过，如实登记：不可达）** |
| — | chaos `agentHeartbeatFrozen` 改年龄式 | 整包非 -race 一轮 | 绿（11s） |

**身份 golden 与 `go test -list` 对账收据**（L5-F1 要求）：`test/determinism`、`test/architecture`、`test/e2e/parallel` 三包
`go test -list '.*'` 与 golden 的差集在幻影删除后为空（提取器改 parser 后，`TestTestFunctionInventoryOnlyGrows` 的 missing 列表恰为那 7 条，无多无少）。

**golden 手改清单**（commit message 需要写的理由）：
- `test_function_inventory.txt`：删 7 条幻影（L1-F1）；删 `TestDedupeNeverFoldsAcrossRaceRunFilterOrWholeUnits` / `TestDedupeFailsOpenOnHashError`（改名为 `…ExtraOrWholeUnits` / `…AndSaysWhy`，L4-F4/L4-F7）；删 `TestAllocationPropertyReachesEveryPath` / `TestLeaseModelGeneratorCoversTheAlphabet`（影子 RNG 自检并入走本身，L2-F7）；其余为追加。
- `matrix_units_golden.txt`：**重生成**（键格式加 `extra=`、whole 矩阵加 `lit=` 行）；旧 38 行按 1:1 映射全部保留（脚本核对无 MISSING），新增 `TestLeakRiskMatrix` 2 行 + 34 行 `lit=`。
- `legacy_leader_premise_sites.go`：5→13（扫描器收紧；这是同一 commit 内的初值，不是放宽）。
- `legacy_gates_without_controls.go`：13→12（simcluster_log_oracle 锚定）。

## §5 三硬闸（内审修复后，串行、不经管道、退出码落盘）

| 闸 | 结果 | 实测 | 备注 |
|---|---|---|---|
| `make gates` | **rc=0** | 3m32s（212s） | 第一轮红在 lint（9 文件 gofmt、3 处 `seed := seed` copyloopvar、1 处德摩根），修后单独 `make lint` 0 issues，再跑全绿。含 run-all.sh ≈2 min |
| `make test` | **rc=0** | ≈6 min，67 包 ok | 前台跑。两次后台（nohup）运行红在同一个 SIGHUP 测试，见 §6 运行方式陷阱 |
| `make e2e-parallel` | **rc=0，ALL PASS** | 3m52s | dedupe **40 → 32** 单元（`TestLeakRiskMatrix` +2）、4 折、**0 组 kept apart**、self-check 17/17、分片后 67 单元、18 worker + 1 wide；deadline 25m |

`make lint` 是 `make gates` 的最后一步，随 gates 绿。

**外审第一轮（Fail：F1–F5）修复后重跑**（同样前台串行）：`make gates` rc=0、`make test` rc=0（67 包）、
`make e2e-parallel` ALL PASS 3m46s（17/17，40→32、0 组 kept apart）、`make lint` 0 issues。处置明细在
`docs/reviews/test-system-overhaul-external-review.md` 末尾「主进程回复」。

**外审复审（Pass）**：`docs/reviews/test-system-overhaul-external-rereview.md`——复审又找到 R1–R3 三个 Major 假阴性（leader 门的
同名 helper shadow / 无调用的行动；sleep 门把任意调用当观测；splitter 与入口门只追一层别名），经用户授权由外审者直接修复
（5 个扫描/回归文件），主进程逐行复核后只对齐注释与文档；提交前三硬闸在合并树上再跑一遍（见 commit）。

## §6 留给产品增量 / 外审要看的

- **B5 复现**（`TestLeaseModelReplayWindowWhenTheIncumbentSubscribesLate`，Skip + trace；`b5ReplayWindowOpen` 双向过期）。随机走 60×20 含 44 次 silent 步**未**再现 B5（它需要"后台探针写完 nobody 之后再订阅"的时序，走的 silent 步在 60ms 后撤掉 interest、注册随即以 inline 定论授予）。
- younger 观测守卫不可达（B3 ② G1 未通过）——不是缺陷，是防御性代码；已写明。
- `ParseDiscoveryInvite` 收 `sid=` 而 Mint 不出；`parseRegisterLine` 不钉 port 范围（tokenLookup 兜底）；僵尸迟订阅无 broker 侧防御；Clash 仍开；`tether ps` 1MiB 已修（71e4943）。
- `test/concurrency` 的 T5 端口 TOCTOU 瞬时 flake（`make gates` 本轮红过一次、复跑绿）——修它是整包进 -race 矩阵的前提（§9 D-23）。
- **运行方式陷阱（不是缺陷，登记给下一个跑闸门的人）**：`internal/agent` 的 `TestCtlLivenessReaperHangsUpSilentRun`（本增量未碰，
  上次改动 71e4943）靠 SIGHUP 挂断一个 PTY 子进程。把 `make test` 放在 `nohup` / 工具的后台任务里跑时，父进程把 SIGHUP 设成 SIG_IGN，
  子进程一路继承到 `sleep 30`，SIGHUP 于是什么也不做——测试报「armed silent run was never hung up」，两次都在同一处红；
  同一棵树前台跑 `make test` 绿，单包 `-count=5` 绿，并发负载下 `-count=3` 也绿（0.94s）。**三硬闸必须在前台跑**（或至少不在忽略 SIGHUP 的父进程下），
  否则这条红会被误读成 flake 或误归因给本增量。
