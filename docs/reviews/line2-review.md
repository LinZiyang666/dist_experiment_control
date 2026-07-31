# 线二 · 质量闸门加固 — 内部审查报告

> 阶段 C step 4 产物。8 条 reviewer lane × 3 个 verifier 镜头（可复现性 / 修复方案 / 范围与授权）× 2 位审计员（完成判据机械核验 / 拖延与逃避猎手）。
> **专家只读实现、只建议测试，未修改任何实现代码。** 采纳与驳回由主进程在本文件内逐条回复（step 5）。

## 0. 结论

### 两位审计员的 verdict（原样呈现，未缓和）

| 审计员 | verdict |
|---|---|
| **完成判据机械核验员** | **NOT-DONE** —— 27 条判据逐条实跑：18 条满足、8 条不满足、1 条部分（`make e2e-parallel` 由主进程负责）。「实现本身基本是硬的」（7 个新闸门的咬合力全部独立复现），但 §14 这份「唯一凭据」自身漏在它最引以为傲的地方：判据 4 的 fail-open 实测不成立；判据 11/13/14/16 在它们声称会变红的变异下全绿；`nolint_directive_test.go` 不存在而 `.golangci.yml:157` 声称它存在；判据 22 靠一个 git 装不下的空目录变绿；TLS 配对门可一行绕过。 |
| **拖延与逃避猎手** | **NOT-DONE** —— 措辞扫描全清（判据 27 通过，本次未新增任何延后措辞），但另外三项各有实证问题：自述诚实度 CRITICAL（E5/E6 各自一行未做却标「✅ 已完成」，line 772「22 个 DO 项至此全部落地，无延后项」至少五处为假）；两条 REJECT-FOREVER 不成立（X2 的重开条件在写下它的同一次改动里两半都已满足；X10 的重开条件列字面写「无」且其支撑机制实测为假）；五处静默缩水。「树是连贯的，阻塞项是完成声明的真实性，不是坏树。」 |

**任一为 NOT-DONE 即不算 done。两位都是 NOT-DONE。**

### 存活 finding 计数

- reviewer 原始 finding **89 条**（8 lane）。
- 按综合规则 1（≥2 个镜头 refuted 才删）：**0 条被删除**。7 条各被**单个**镜头驳回，未达删除门槛，全部保留并在 §8 逐条记录驳回理由，供主进程复核。
- 合并同一缺陷的重复报告后，**存活条目 59 条**：**CRITICAL 6 / MAJOR 26 / MINOR 27**。
- 六条 lane 独立报出同一条 CRITICAL（`make lint` fail-open），两位审计员亦各自列为 unmet —— 九路独立复现。

### 严重度分歧（三处，主进程需自行裁定，未替任何一方缓和）

| finding | reviewer 定级 | 镜头意见 |
|---|---|---|
| PC-2（PTY ENOSPC） | MAJOR | 范围镜头实测 `usage.md:1559` 把 69 定义为**可重试**，因此 PC-2 的 impact 链（「监控停止重试」）不成立，建议降 MINOR；同时 PC-2 与 F2[deletions]/D5 对同一张表给出**相反读法** |
| F8[lint]（noctx `^test/`） | MINOR | 范围镜头据 plan §15 仲裁（T1 只豁免 `_test\.go`）主张上调：这是对本次改动自己的仲裁记录的违反 |
| PI-9（main-noncli 无阈值） | MAJOR | 修复镜头与范围镜头均指出它反对的是 plan §4 的**明文裁决**（「只登记，无绝对阈值」），应读成「摩擦频率被低估」而非「实现走偏」 |

另有两条 MINOR 被审计员上调：del-F5（健康度 sweep 过度声称）与 PF-13（X10 重开条件为「无」），拖延审计员均判 MAJOR。

### lane 覆盖

8 条 reviewer lane 全部产出，3 个 verifier 镜头全部产出，2 位审计员全部产出。**无死掉的 lane。**

需要说明的两处口径问题：

1. reviewer 的 finding id 有冲突 —— `.golangci.yml` 配置审查 lane 与「删除的东西」lane 都用了 `F1..F8/F9`。本文统一写作 `F<n>[lint]` 与 `F<n>[deletions]`。
2. **`make e2e-parallel` 无第三方证据**：本轮所有专家被明令禁止跑它（3.5 min + 互相打架）。主进程声称 ALL PASS 3m30s，本报告不为该声称背书。`make lint` / `make gates` 同理未由专家重跑（golangci-lint 全局锁），以主进程预生成的 `review-lint.json`（Issues=0、22 linter 生效）为据。
3. 全程仓库零改动：审查开始与结束两次 `git status --porcelain | wc -l` 均为 **86**，所有变异实验在 `/home/weiland/.claude/jobs/cda1899e/tmp/<lane>/` 的副本内进行。

---

## 1. CRITICAL

### C1 · `make lint` 至今 fail-open —— plan §6 明令的接线未落地
**id**：IDG-1 / F1[lint] / PI-1 / PF-1 / D1 / GI-1（六 lane 独立复现）+ 两位审计员均列为 unmet
**文件**：`Makefile:160`

**缺陷**：lint 目标仍是裸 `"$$LINT" run;`，不带 `-c .golangci.yml`。`git diff Makefile` 的 hunk 只覆盖 `.PHONY` / `ALL_TEST_TAGS` / `vet-tags` / `test:` 依赖 / `gates`，lint 配方**零改动**。plan:323-325 明写「**`make lint` 今天是 fail-open 的**…→ 改成 `golangci-lint run -c .golangci.yml`，配置缺失即硬失败」；判据 #4 明写期望 rc≠0。

**实测证据**（副本内唯一一次串行 lint）：
```
$ mv .golangci.yml /tmp/ && make lint
0 issues.
MAKE_LINT_RC=0
$ golangci-lint linters | awk '/^Enabled/,/^Disabled/' | grep -cE '^[a-z0-9]+'
5          # 有配置时 22
```
`gofmt -l ./cmd ./internal ./test` 同样干净，所以整条 target 绿。修复可行性已实测：空目录内 `golangci-lint run -c .golangci.yml` → `can't load config: can't read viper config`，**rc=3**（v2 对缺失的 `-c` 路径硬失败）。加 `-c` 不改变行为：带与不带 `-c` 的 enabled 计数都是 22。

**失败场景**：任何让 `.golangci.yml` 消失或改名的动作 —— merge 冲突解错、`.gitignore` 误伤、从子目录跑 lint、CI 换工作目录 —— 都会让 `make lint`、`make gates`（以 `$(MAKE) lint` 收尾）、CI 的 lint job 全部保持绿，而 18 个新启用的 linter（含 T1 全部零基线正确性检测器 forcetypeassert/nilerr/bodyclose/noctx/exhaustive/errorlint）、7 条 dupl 行区间豁免、maintidx 登记表静默停跑。**「lint 0 issues」这句证据与「配置整个没生效」在 `make lint` 下的输出和 rc 完全相同** —— 正是本仓明令禁止的恒等式形态。

**修法**：`Makefile:160` → `"$$LINT" run -c .golangci.yml;`。并在 `test/architecture/` 加一条常驻断言（读 Makefile，断言 lint 配方含 `-c .golangci.yml`；断言 `.golangci.yml` 存在且能解析出非空 `linters.enable`）—— 修复镜头提醒：**不要**在 go test 里调 golangci-lint 来做这条断言，`make gates` 以 `make lint` 收尾会抢全局锁；用纯 YAML/文本解析。
**注**：`?? .golangci.yml` 的未跟踪状态是外审阶段不 git add 的约定造成的（本轮 9 个新文件全是 `??`），且 `git check-ignore` 返回 rc=1（未被忽略）—— PI-1 的「`git clean -xdf` 会删掉它」这条 sub-argument 不构成常驻风险，核心缺陷不依赖它。

---

### C2 · 纯 lint 整改破坏 `cluster status --watch --json` 的 JSONL 契约（B5 已修过的 BLOCKER 重新引入）
**id**：PC-1
**文件**：`cmd/tether/cluster_wait.go:41`

**缺陷**：`revive: empty-block` 整改把三分支塌成两分支。原式 `if asJSON { /*空*/ } else if isTTY { clear } else { 分隔行 }`；新式 `if !asJSON && isTTY { clear } else { 分隔行 }` —— `asJSON == true` 从此落进打印 `--- <ts> ---` 的 else 支。

**实测证据**（副本内加探针，ctx 预 cancel 只跑一帧）：
```
RAW OUTPUT: "--- 2026-07-29T16:57:17-05:00 ---\n{\"schema_version\":1,…}\n"
JSONL contract broken: line "--- …" is not JSON: invalid character '-' in numeric literal
--- FAIL: TestLaneProbeWatchJSONLIsPureJSON
```
回归证明：同一探针在 `git checkout HEAD -- cmd/tether/cluster_wait.go` 后 → `ok`。**HEAD 绿 / line-2 红**，可复现性镜头独立复现。契约来源：函数自身 doc（`cluster_wait.go:28`）「--json emits JSONL (one object per line, no ANSI)」；`docs/reviews/b5-review.md:8` 记录这正是 B5 的 **BLOCKER**（当时是 MarshalIndent 多行）。新写的注释「JSONL deliberately does NOT clear the screen: one object per line」在它守的那个条件下恰好是假的。

**失败场景**：`tether cluster status --watch 5s --json | jq -c` 从第一帧起 mis-parse（`jq: invalid numeric literal`）。`--watch` 只与 offline/remote/settle/homes 互斥（`cluster.go:195-214`），**与 `--json` 不互斥**。`grep -rn watchClusterStatus --include=*_test.go` 零命中 —— `--watch` 全仓零测试，所以 `make test` / `make e2e-parallel` 全绿也发现不了。本次改动**动过** `cluster_wait_test.go`（revive context-as-argument 重排参数）却仍未调用该函数。

**修法**：asJSON 提到外层、彻底不做帧装饰（已在副本验证：探针转 PASS，且不含空 block 故不触发 revive empty-block）：
```go
// JSONL deliberately gets NO frame decoration at all -- one object per line, nothing else.
if !asJSON {
	if isTTY {
		_, _ = fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J")
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "--- %s ---\n", time.Now().Format(time.RFC3339))
	}
}
```
**顺手关掉同根因的第二个后果**（见 §6 集体盲区 B2）：装饰目前在 fetch **之前**执行，所以 `--watch --json` 遇到瞬时 socket 错误时 stdout 只得到一行 `--- <ts> ---` 而没有任何 JSON 对象。把装饰整段移到 `rep` 成功之后可一并解决。

---

### C3 · `test/architecture/nolint_directive_test.go` 缺失，而 `.golangci.yml:157` 声称它存在
**id**：PF-4（CRITICAL）/ IDG-8 / F4[lint] / GI-3 / D9 + 完成判据审计员（CRITICAL）
**文件**：`.golangci.yml:155-157`；应存在而不存在的 `test/architecture/nolint_directive_test.go`

**缺陷**：plan:446（§8 C4）把该守卫列为 **DO**（「断言每条 `//nolint:X` 的 X 都在 `.golangci.yml` 的 enable 集合里。约 20 行」），§15 仲裁也把「nolint 指令守卫」派给 `test/architecture/`。文件不存在，而已落盘的配置在 KNOWN LIMIT 段写着「`test/architecture/nolint_directive_test.go` covers that gap」。plan 末尾同时写着「22 个 DO 项至此全部落地，无延后项」。

**实测证据**：
```
$ ls test/architecture/
build_tags_test.go  layering_test.go  structural_budget_test.go  testdata  tls_verify_pairing_test.go
$ grep -rn nolint_directive .
.golangci.yml:157:      # test/architecture/nolint_directive_test.go covers that gap.
docs/reviews/line2-plan.md:446:→ `test/architecture/nolint_directive_test.go` …约 20 行。
```
缺口实测仍在（enable 17 条 + standard 5 = 22 个名字，不含 gosec / unconvert）：
```
internal/tunnel/tls.go:65                        //nolint:gosec
internal/tunnel/tls.go:121                       //nolint:gosec
internal/tunnel/register_and_fence_test.go:290   //nolint:gosec
internal/broker/disk.go:132                      //nolint:unconvert,gosec
```
即 5 个 (linter, 站点) 对永久不受 nolintlint 巡查，与 plan §8「仓内今天已有 5 条这种永久不受巡查的僵尸指令」逐条吻合 —— **一条都没动**。反向对照：写一条 `//nolint:bodyclose`（已启用）立刻被报 ``directive … is unused for linter "bodyclose"``。

**失败场景**：两层。① `internal/tunnel/tls.go:65/121` 的两处 `InsecureSkipVerify: true` 正是本次同批新建的 TLS 配对门在盯的对象，它们各挂一条**永远不会被任何 linter 消费**的 `//nolint:gosec`；哪天有人删掉 `VerifyConnection` 里的 pin 检查，那行注释仍在原地宣称「pin check is done in VerifyConnection below」而无人报错。② 配置用这段 KNOWN LIMIT 向读者保证缺口已被别处补上，读者据此**停止怀疑** —— 一条指向不存在文件的注释比没有注释更坏，它把「已知缺口」伪装成「已关闭缺口」。而 T2 整层「豁免自带过期压力」的叙事全靠 nolintlint，这个盲区正是它的地板。CLAUDE.md:125 新写的「问题修好后悬空的指令会报错」对未启用 linter 名同样不成立（= D9）。

**修法**：二选一，**不能两者都不做**。(a) 写那 20 行：解析 `linters.default`（`standard` 展开为 errcheck/govet/ineffassign/staticcheck/unused）+ `linters.enable`，扫全仓 `//nolint:a,b,c`，断言每个名字都在集合内 —— **预期第一次跑就红**（5 条），那正是它的咬合力证明，不需要额外注入缺陷；随后逐条处置（改用已启用 linter / 删指令但**保留后面的论证散文**）。(b) 删掉 `.golangci.yml:155-157` 的最后一句并把 KNOWN LIMIT 写成「无守卫」，同时改 CLAUDE.md:125 的措辞为「仅限已启用的 linter」。
**排除项**：第三条路「把 gosec/unconvert 真的启用」会反转 plan §11 X7 的永久裁决（gosec 全量 111 条 REJECT-FOREVER，只采纳人工复核的 6 条），不改 §11 不得照做。
**抗腐化建议**：把 `promised_guard_test.go` 的采集面从「Go 注释里的 `Test…` 标识符」扩到「`.golangci.yml` / Makefile / 活文档里形如 `\S+_test\.go` 的文件引用」—— 这次漏网正是因为承诺的载体从 `.go` 换成了 `.yml`。

---

### C4 · 结构棘轮用 `strings.Contains(src, "cobra")` 分类，注释里出现 cobra 即整文件豁免（单向棘轮，且存量已有一例）
**id**：PI-2
**文件**：`test/architecture/structural_budget_test.go:188`

**缺陷**：`main-noncli-code-lines` 维度判定一个文件是不是 CLI 文件靠**全文子串**，不看 import。注释里写 cobra 就整文件不计入，而且这个豁免是**单向棘轮**：加注释 → 报「债务缩小」→ 跑 `-update` 锁住；想撤回 → 报 BUDGET EXCEEDED 要求手改 + 写论证。

**实测证据**（三步 + 存量实例，可复现性镜头独立复现）：
```
(a) 往 cmd/tether/error_hints.go 顶部加一行 "// … does not depend on cobra …"
    → DEBT SHRANK … main-noncli-code-lines cmd/tether = 899, ledger still says 1116
(b) 按门自己的提示跑 -update-structural-budget → 接受，golden 写成 899，重跑绿
(c) 删掉那行注释 → BUDGET EXCEEDED … = 1116, ledger says 899 (+217) 「change the line BY HAND and justify it」

存量：cmd/tether/poll.go:22 的注释含 `every cobra command`（引用 main.go 原话）。
把它改成 `every CLI` → BUDGET EXCEEDED … = 1160, ledger says 1116 (+44)
```
我独立核实：`cmd/tether` 下**只有** `poll.go` 命中「注释含 cobra 但不 import cobra」。

**失败场景**：① **golden 的 1116 今天已经是错的**，至少少算 poll.go 的 44 行；② 这一维的全部目的（拦「不该待在 package main 里的编排逻辑」）可以被一句注释关掉，而这句注释恰恰是本仓文化最会写的那种（「本文件故意不依赖 cobra，所以可以直接单测」正是这一维想奖励的事实）；③ 门的失败文案主动教人走这条路（DEBT SHRANK → 跑 `-update`）；④ 单向性使任何人事后编辑那句注释都会打红构建并被要求「论证为什么这个预算该放宽」—— 又一次给注释编辑收税，与 CLAUDE.md 同一次改动新写的「注释是资产」正面冲突。

**修法**：改用 AST 判定（扫 `f.Imports` 里是否有 `github.com/spf13/cobra`），而不是全文子串。修完 poll.go 会归位，golden 需在**同 commit** 手改到 1160 一线（`-update` 会拒绝放宽 —— 这是设计好的摩擦，不是 fix 的缺陷）。并在 `TestStructuralBudgetMeasurementIsNonVacuous` 里加一条分类器反向探针：合成一个「`package main` + 一行含 cobra 的**注释** + 若干代码行」的文件，断言它仍被计入。

---

### C5 · E5 一行未做，却被进度表标为「✅ 已完成」，并计入「22 个 DO 项全部落地」
**id**：PF-2 + 拖延审计员（CRITICAL）
**文件**：`docs/reviews/line2-plan.md:508`（需求）、`:688`（「已完成」标签）、`:772`（完成声明）；应被修改而未被触碰的 `test/e2e/all_phases_test.go`

**缺陷**：E5 = 「在 `test/e2e/all_phases_test.go` 顶部注释登记 L07-F5 的约 940 次冗余测试函数执行（S3 §9.6 的执行动作，六条 lane 集体零覆盖）」。零交付。

**实测证据**：
```
$ git status --porcelain test/e2e/all_phases_test.go
（空 —— 文件未被本次改动碰过）
$ head -30 test/e2e/all_phases_test.go     # 仍是原 e2e_matrix 说明，无任何冗余登记
穷举（tracked diff + 全部 9 个未跟踪新文件全文）：
$ grep -inE '940|redundant execut|L07-F5|重复执行|冗余'
→ 仅命中 plan 自身陈述需求的 4 行，交付侧零命中
```
而 plan:688 写 `~~E1–E6 G5~~ ✅ **已完成**`（其说明列只枚举 E1–E4 的内容：修订 2/3/4/5a/5b/5d + step 5b —— **标签比它自己的证据宽**），plan:772 写「线二 §2 归宿总表的 22 个 DO 项至此全部落地，无延后项」。

**失败场景**：S3 §9.6 唯一仍然有效的执行动作丢失，约 940 次冗余执行在每次 `make e2e-parallel` 里继续付出成本且无人知道它存在 —— 正是 plan §0「不许延后、只有 DO 或 REJECT-FOREVER」要消灭的形态，而它以「已完成」的伪装通过了。更严重的是完成声明本身：一个会写「已完成」而实际为零的进度表，让 §14「流程结束时这份清单是唯一凭据」失效。

**修法**：要么真在 `all_phases_test.go` 顶部落这段登记（并写清测算方法），要么在 §2 把 E5 显式改判 REJECT-FOREVER 并补论证 + 重开条件。无论哪条，**先把 plan 里「E1–E6 ✅ 已完成」和 line 772 的「全部落地」改成事实**。

---

### C6 · E6 一行未做，同样被标为「✅ 已完成」；plan 自己诊断过的失败模式被逐字重演
**id**：PF-3 + 拖延审计员（CRITICAL）
**文件**：`docs/reviews/line2-plan.md:509-511`（需求）、`:688`、`:772`；应被修改而只改了 1 行的 `docs/architecture.md`

**缺陷**：E6 = S3 裁决 5 **采纳的那一半**：architecture.md 写一条两行 ctx 规则 + 每个生产 `context.Background()` 站点加一行引用注释。零交付。

**实测证据**：
```
$ git diff --numstat docs/architecture.md
1	1        # 唯一那行是 E4 的分支删除：「新开分支 phase/<N>-<slug>」→「直接在 main 上工作」
$ grep -nE 'context\.Background|ctx 规则|contextcheck|ctx rule' docs/architecture.md
（零命中）
$ grep -rn 'context.Background()' --include=*.go cmd internal | grep -v _test.go | wc -l
39          # 其中带规则引用注释的：0
```
diff 里唯一的 `context.Background` 触碰是测试文件的形参重排（revive context-as-argument）。

**失败场景**：plan §10 E6 的原文诊断是「六条 lane 只继承了『驳回』，丢了『采纳』」—— 这句诊断在实现阶段被逐字重演：contextcheck 的驳回落实了（配置里没启用），配套要求的规则与站点注释一条没落。39 个生产站点仍然没有任何文档规则可引用，下一个人加第 40 个时同样无据可依。它是第二个被标成「已完成」的空项。

**修法**：同 C5 —— 落地或显式改判 REJECT-FOREVER（带重开条件），并先修正进度表与完成声明。

---

## 2. MAJOR

### M1 · TLS 配对门只看复合字面量：赋值式与变量值完全逃逸；下限 `found == 0` 可被测试代码单独满足
**id**：IDG-2 / GI-11 / IDG-9（原 MINOR，同文件同族，合并）+ 完成判据审计员（独立以第三种形态复现）
**文件**：`test/architecture/tls_verify_pairing_test.go:96`（扫描面）、`:181`（下限）

**缺陷**：门只匹配 `*ast.CompositeLit` 里 `*ast.KeyValueExpr` 且值为 Ident `true` 的形态。三种逃逸形态实测全绿：
```
(A) cfg := &tls.Config{MinVersion: …}; cfg.InsecureSkipVerify = true   → 门 ok exit=0
(B) &tls.Config{…, InsecureSkipVerify: alwaysSkip}（包级 var = true）  → 门 ok exit=0
(C) InsecureSkipVerify: skipHostname（局部 var，并删掉 VerifyPeerCertificate）
    → go build OK / go vet OK / 门 PASS / test/architecture + test/determinism 全绿
```
而 `gosec` 不在 enable 列表（且 X7 判其 REJECT-FOREVER），所以这三种写法**没有任何工具覆盖**。
下限侧：`found == 0` 太松。实有 4 个字面量站点（`internal/tunnel/tls.go:65`、`:121`、`internal/cluster/transport.go:99`、`internal/tunnel/register_and_fence_test.go:290`），把 walker 的 skip 列表加上 `"cluster"`（丢掉唯一那个 configured-pair 站点）→ 门仍全绿。修复镜头另指出：walker **不排除 `_test.go`**，所以 `register_and_fence_test.go` 一个测试文件就能单独满足下限 —— 生产站点全部消失时门依然绿（这也使 GI-11 建议的 `found < 3` 数字写错了，实测是 4）。

**失败场景**：明天有人写一个用赋值形态构造 `tls.Config` 的新 dialer（agent doctor 探测、升级下载、任何一处 http.Transport），它**接受任何证书**、编译通过、所有功能测试通过、这条为记录 G402 复核而写的门一声不吭、golangci 也不报。门的文件头承诺「the two fields live and die together」在这两种写法下不成立；而门存在的全部理由（把那次一次性 G402 复核变成永久断言）所指向的 `internal/cluster/transport.go` 可以整个掉出扫描面而不留痕迹。

**修法**：① 除 CompositeLit 外再扫 `*ast.AssignStmt`（左侧 SelectorExpr `.Sel.Name == "InsecureSkipVerify"`、右侧非 `false`），并把「值不是字面量 `false`」也当 skipsHostname（保守方向：宁可要求作者显式登记；实测今天全仓零赋值式站点，故不会新增 offender）。② 下限从计数改成**点名**：断言 `seen` 至少包含 `internal/cluster/transport.go` 与 `internal/tunnel/tls.go` 的站点（照 build_tags 门「d5_integration 必须命中、7 个文件」的形状）。③ 配一个 synthesized 源码样本的自检，证明扩展后的扫描器真能看见赋值式与变量式。

---

### M2 · 枚举 `default:` 门只登记 3/15 个自有枚举，补齐后实测仍有 5 个生产 switch 靠 `default:` 永久瞎掉 exhaustive；且是本组唯一没有覆盖度反向断言的账本
**id**：IDG-3 / PI-13
**文件**：`test/determinism/enum_switch_default_test.go:50`（`enumFamilies`）、`:147`（未注册即跳过）、`:196`（非空泛下限）

**缺陷**：`enumFamilies` 只有 `natsconf.TopoState` / `agent.openOutcome` / `broker.ledgerDisposition` 三族，而 AST 扫 `cmd/`+`internal/` 得 `type X <整型>` + iota 封闭枚举共 **15** 族。未登记族在 `:147-149` 直接 `return true` 静默跳过。plan §7 承诺的是「AST 扫 cmd/ + internal/ 里**所有**以本仓自有枚举类型为 tag 的 switch」。

**实测证据**（在门内加 `t.Logf` 测得，可复现性镜头逐位复现）：
```
基线：  guarded=11  offenders=0
把 12 个未登记家族加进 enumFamilies（passAuthority / ledgerSite / cutoverAction /
spawnsafe.Mode / fsKind / admitRole / RenderIntent / cli.Source / authcallout.role /
clusterupgrade.AgentPresence / agent.transferMode / main.capsStatus）：
        guarded=17  offenders=5
  internal/natsconf/render_desired.go:77   (RenderIntent，default: 就是 IntentPreserve)
  internal/natsconf/render_desired.go:155  (同上)
  internal/broker/admit.go:283             (admitRole，default: // admitOwner — also the zero value)
  internal/authcallout/handler.go:227      (role)
  cmd/tether/transfer.go:782               (capsStatus)
```
逐个读码确认都是真的以该枚举为 tag 的 switch，非前缀误报。非空泛自检只有 `guarded >= 5`，在漏登记 12 个家族时 `guarded=11` 仍然绿；对照本组其他五本账本（`unverifiedTLSFallbacks` / `allowedEnumDefaults` / `legacyProcessNamedTestFuncs` / `historicalWireDocs` / `staleClassificationKeys`）全都有陈旧条目反向断言，**只有 enumFamilies 的失效方向是「不在表里」而不是「在表里但过时」**。

**失败场景**：给 `RenderIntent` 加第 5 个常量，`String()` 与 `standalone()` 会静默把它当 `IntentPreserve` 处理 —— exhaustive 因 `default:` 不报、新门因家族未登记不报。这与 plan §1 订正 2 要拦的批 C 外审 M2（doctor 漏 case、把不收敛报成 PASS）是同一缺陷形状，只是换了枚举。

**修法**：补齐 enumFamilies 到全部自有枚举，并对新暴露的 5 个站点逐条判「改成全 case」还是「进 `allowedEnumDefaults` 带理由」；补一条 `TestEnumFamiliesCoverEveryRepoEnum`（扫 `type X int` + iota 声明，每个类型要么在 enumFamilies 里、要么在一份带理由的 `notAnEnumLedger` 里）；把下限从 `guarded < 5` 抬到与登记家族数挂钩。
**两条落地提醒（修复镜头）**：① 该门按**常量名前缀**匹配，一次性登记 12 个家族要挑不与他族共享的前缀，否则误归属；② `allowedEnumDefaults` 今天是**空 map**，处置那 5 个 offender 会首次填充它，而它用 `file:line` 做 key —— 会立刻继承 M15 的注释位移税，必须与 M15 同批修（详见 §6 集体盲区 B4）。

---

### M3 · `-update-structural-budget` 静默删掉 golden 里 13 行手写放宽论证（rc=0，闸门自己在债务收缩时主动指引跑它）
**id**：IDG-4 / PF-5 / D3 / GI-5（四 lane）
**文件**：`test/architecture/structural_budget_test.go:299-317`（`renderGolden`）、`:407`（写盘）、`:352-355`（失败文案）；`testdata/structural_budget_golden.txt:12-24`

**缺陷**：`renderGolden()` 从固定 11 行模板重建整个文件，`readGolden` 读时对 `#` 行一律 continue —— 手写注释不进数据模型也不回写。

**实测证据**：对**未改动**的树跑一次例行 `-update` → `ok` rc=0，`diff` 前后 golden：
```
12,24d11
< # Raised 1102 -> 1106 -> 1116 by hand, in two steps (the -update flag refuses to widen, which is the
< # point — each raise had to be typed and justified).
< # +4  the missing natsconf.TopoBehind branch in cluster_status_card.go …
< # +10 the line-2 §12 Y2 error-code split: hints and exit classes for pty_unavailable …
< # Both raises buy an operator a stated cause where they previously got silence. …
```
共 13 行。制造 DEBT SHRANK（golden 279→280）后跑 `-update` 同样 rc=0 + 同样 13 行消失。对照：拒绝**放宽**是真的（golden 279→278 → `REFUSES to widen 1 budget(s)` rc=1 且不落盘）。

**失败场景**：任意一次债务收缩（重构成功、删了一个方法、cmd/tether 少几行非 CLI 代码）都会让闸门打红，红字**指定的修法就是跑 `-update`**（`:352-355` 逐字如此），一跑就把此前所有放宽理由抹掉、退出 0、diff 里看起来只是几个数字变了。下一个看 golden 的人只看到裸数字 1116，看不到那 `+10` 是为了让 `tether node upgrade` 不再对着错 URL 无限重试。CLAUDE.md 新条款要求「动任何 golden 等同于改不变量，必须写明为什么这个预算该放宽」—— 论证写在 golden 里，而 golden 会被例行动作清掉。本仓「注释是资产、让人删注释才能变绿的闸门是负收益」在这里被闸门自己的补救动作触发，且**连人都不用动手**。

**修法**：`updateGoldenTightenOnly` 先读原文件，把所有非样板 `#` 行原样保留后再拼 entries。
**排除项（修复镜头）**：不要采纳「`-update` 只做原地数值替换、完全不重建文件」这条备选 —— tighten-only 更新同时负责删掉已不超预算的陈旧行（主 test 的 stale 分支要求账本排空），纯正则改数字会让 `-update` 不再排空、把排空动作退回手工。
**必配守卫**：`TestUpdateFlagPreservesLedgerJustifications` —— 在 `t.TempDir()` 造一份带 `#` 理由行的 golden，跑 writer，断言理由行逐字保留。这是唯一能防复发的形状。

---

### M4 · `-update-structural-budget` 对「账本里还没有的新超预算实体」无摩擦收录 rc=0 —— 正是 plan 称为「本项最重要的实现约束」所要避免的形态
**id**：GI-4 + 拖延审计员
**文件**：`test/architecture/structural_budget_test.go:389`（`updateGoldenTightenOnly` 的判断）、`:341`（update 分支直接 return）

**缺陷**：判断是 `if was, ok := prev[e.key()]; ok && e.value > was` —— `ok == false` 的新实体直接落进 `out` 并写盘。

**实测证据**：
```
新建 internal/probepkg/（21 个生产文件）
$ go test ./test/architecture/ -run TestStructuralBudget
  FAIL: NEW over-budget entity: pkg-files internal/probepkg = 21 … add the line and say WHY in the commit message
$ go test ./test/architecture/ -run TestStructuralBudget -update-structural-budget
  ok   (rc=0)
$ grep -n probepkg testdata/structural_budget_golden.txt
  16:pkg-files internal/probepkg 21        ← 自动落盘
```
对已存在 key 的拒绝是真的（rc=1、不落盘）。

**失败场景**：文件头 `:53-57` 自称「because the flag can only tighten, running it can never smuggle unrelated growth past review」—— 这句话对新实体是假的。可达路径不需要恶意：任一维度收紧就会红，并**由失败文案主动建议**跑 `-update`；若同一次改动里又诞生了第 4 个 god type 或第 5 个 >20 文件包，那次 `-update` 会把它一起吞掉并 rc=0，摩擦点归零。plan §4 逐字警告过：「`command_tree_inventory_test.go` 的更新 flag 是无条件重写，照抄它会做出一个假闸门」。

**修法**：把「prev 中不存在的 key」也归入 refused（新债务只能手写 + commit message 说明），或至少要求一个额外的 `-allow-new-budget-entry` 显式 flag。同时把 update 分支改成先跑完整比对、有 NEW 实体就 Fatal，而不是 `return` 掉全部断言。

---

### M5 · §14 判据 #11/#13/#14/#16 的 `-run` 正则选不到被测闸门 —— 在带缺陷的树上照样 exit 0
**id**：IDG-5 / D7 + 完成判据审计员（四条判据各自实跑）
**文件**：`docs/reviews/line2-plan.md:607` 起的 §14 判据表

**缺陷**：判据写的函数名不存在。真名是 `TestPackageLayering` / `TestDocsUseCurrentWireVersion` / `TestNoDefaultOnRepoEnumSwitch`。

**实测证据**：
```
#11  注入 internal/cluster → internal/jsstream 违规后
     -run TestLayering                    → ok  rc=0（只命中 TestLayeringRulesAreWellFormed）
     -run TestPackageLayering             → FAIL rc=1
#13  往 docs/usage.md 写 tether.v1.session.foo 后
     -run TestDocsWireVersion             → ok  rc=0（只命中 …ScannerIsNonVacuous）
     -run TestDocsUseCurrentWireVersion   → FAIL rc=1
#14  ProtoVersion 2→3 后：同上，判据命令 rc=0、真门 rc=1
#16  给 TopoState switch 加 default: 后
     -run TestEnumSwitchNoDefault         → ok … [no tests to run]  rc=0（真名一个都不匹配）
     -run TestNoDefaultOnRepoEnumSwitch   → FAIL rc=1
```

**失败场景**：§14 自定原则「一条判据如果在『偷偷跳过这一项』时仍然是绿的，它就不合格」—— 这四条自己犯了这个错，且**恰好是判据 7 要堵的「空包也绿」在 `-run` 层原样复发**。`[no tests to run]` + PASS + rc=0 与「守卫通过」在退出码上完全不可分。#16 尤其危险，它是 plan §1 订正 2 整条价值论证的落点。任何按 §14 照抄命令做验收的人（包括未来的会话与外审）都会得到「加了 `default:` 也绿」的结论。
> 口径分歧：D7 认为 #15 与 #13/#14 共用同一条空转命令；完成判据审计员逐条实跑后只把 #13/#14 判不满足（#15 双向变红）。本报告 §4 采信逐条实跑的结果并在此记录分歧。

**修法**：判据里的 `-run` 换成真实函数名，或（更抗腐）**判据里禁用 `-run`、改整包** —— `-run` 打错字永远是绿的，整包不会。并加一条元判据：判据表里每条 `-run` 的正则至少选到 1 个测试（`grep -c '^=== RUN'` ≥1）。

---

### M6 · 436 条函数名账本没有已发布计数守卫 —— 加一行即静默豁免，「只减不增」在这本账本上是散文而非机械
**id**：IDG-6 / GI-9（+ PF-11 的后半，见 m16）
**文件**：`test/determinism/legacy_process_named_funcs.go:11`（文件头声称「an entry is only ever REMOVED, never added」）；缺失的断言应落在 `test/determinism/test_naming_test.go`

**缺陷**：只有「陈旧条目变红」的反向断言，没有任何机制阻止新增条目。300 行外的兄弟账本（文件名）有 `const published = 0` 强制（`test_naming_test.go:343`），这本没有。

**实测证据**：
```
往 internal/port/port_test.go 追加 func TestB99BrandNewProcessNamedTest
  → go test ./test/determinism/ -run TestNoNewProcessNamed  → FAIL（门有咬合力）
再往 legacyProcessNamedTestFuncs 加一行 "TestB99BrandNewProcessNamedTest": true
  → go test ./test/determinism/  → ok，条目数 436→437，无任何断言反对
对照：文件账本加一条 → TestLegacyLedgerCountMatchesThePublishedNumber 立刻红
```
账本条目数实测正好 436。

**失败场景**：违反本仓「豁免必须自带过期压力，只增不减的 allow-list 最终允许一切」。下一个想写 `TestB5Xxx` 的人（包括未来的审查 agent）遇到红门，最省力的合规动作就是往账本加一行。CLAUDE.md §3 step 5b 刚被订正成「三样、两本账本」，其中一本从落地第一天起就没有收缩压力。**连带后果**：plan §11 X10 的 REJECT-FOREVER 论证「账本机制保证只减不增，存量会自然消解」因此为假（见 m17）。

**修法**：照文件账本形态加 `TestLegacyFuncLedgerCountMatchesThePublishedNumber`：`const published = 436`，条目数 ≠ published 即红，失败文案写明「这是递减账本，降低这个数字要和改名同 commit；**加条目不是豁免申请**」。约 8 行。顺带核一下 `promised_guard_test.go:16` 的「There are 34 of them」—— 实测账本 33 条（同类已发布数字漂移，见 m27）。

---

### M7 · 函数名账本按**裸函数名**做 key（类别级豁免），而同一次改动的文件名账本按路径（站点级）—— 两本账本 key 语义不一致
**id**：PI-7
**文件**：`test/determinism/test_naming_test.go:194-195`（`seen[name]` / `legacyProcessNamedTestFuncs[name]`）；对照文件门 `:92-93` 用 `seen[rel]`

**缺陷**：账本里的每个名字对**全仓任何位置**的新测试函数永久有效。

**实测证据**：
```
新建 test/zzprobe/probe_test.go 只含 func TestB1ClusterVerdict（该名在 436 条账本里）
  → go test -run TestNoNewProcessNamed → 绿
改成 func TestB99BrandNewName → 红并点名文件
```

**失败场景**：账本等于发了 436 张**可复用**的通行证：任何人想写 `TestB4ExposeReqFlagsSerialize` 风格的新测试，只要撞上账本里已有的名字就静默放行 —— 正是本仓尺子禁止的形态（「豁免必须站点级，不能类别级：覆盖被检查过的站点，不覆盖以后出现的」）。第二个后果：若两个包各有一个同名 process-named 函数，改掉其中一个不会让条目变陈旧（另一个还在把 `seen[name]` 置 true），于是那次改名的账本行留了下来、并继续为第三处放行 —— 排空能力受损。

**修法**：key 改成 `rel + ": " + name`（offender 文案已经在用这个形状），436 行同步加路径前缀（可用现有 offender 输出机械生成）。与 M6 的 published 计数可同时落地（条目数不变）。代价要说清：以后移动一个 legacy 命名的测试文件会让条目陈旧变红、需同 commit 改账本 —— 这正是站点级豁免应有的摩擦，不是缺陷。

---

### M8 · 命名门的批次前缀字符类漏掉字母 A，存量已有一个门看不见且不在账本里的活违规
**id**：PF-7
**文件**：`test/determinism/test_naming_test.go:157`（`(R|P|D|B|C|G|S|F|M)[0-9]+[A-Z].*`）

**缺陷**：**线一第一批就是批次 A**，而字符类不含 A（也不含 E/H/L/N/…）。

**实测证据**：用门的正则与更宽的 `^(Test|Benchmark|Fuzz)([A-Z][0-9]+[A-Z].*)$` 对全仓 2474 个测试函数做差集：
```
total test funcs: 2474
matched by the gate: 439
MISSED by the gate: 1
  TestA9RebalanceTargetsExcludeDraining  (internal/broker/audit_test.go)
$ grep -c TestA9RebalanceTargetsExcludeDraining test/determinism/legacy_process_named_funcs.go
0        ← 也不在 436 条账本里
```
`make test` 全绿，证明这个存量违规确实被静默放过。

**失败场景**：`TestA9…`、`TestE3…`、`TestL7…`、`TestH2…` 都能自由新增。因为门是绿的，下一轮审查会以「函数名已冻结」为前提，这个洞会一直在。

**修法**：补 A（必要）与 E/H/L（存量为零、纯预防）。
**警告（修复镜头）**：**不要**把字符类整体开成 `[A-Z]` —— 在一个真有 v1/v2 wire 版本的仓库里，那会把 `TestV2SessionAttach`、`TestH2ListenerBinds`、`TestP99Latency` 这类版本/协议形状的正当名字也判成 process-named。改完须同 commit 重算 436，并同时落 m16 的 mustMatch/mustNotMatch 伴生表。

---

### M9 · layering 合表本身无丢失，但防止未来丢失的守卫太松：下限 `< 5` 而表有 6 行、并集完整性只对 1/6 行断言、plan 明写要落的测试名对照表不存在
**id**：IDG-7 / PF-12（+ 拖延审计员把「对照表缺失」列为 MINOR unmet）
**文件**：`test/architecture/layering_test.go:191`（下限）、`:222-243`（只覆盖 cluster 一行的全等比对）、`:216`（注释写「held 5 distinct boundaries」）

**缺陷**：三处。① 下限 `if len(layerRules) < 5`，实有 6 行 → 整行可被删除而门不响；② 并集完整性只对 `internal/cluster` 一行做**字符串全等**；③ plan §5 明写「逐条核对四份原文的每个断言在新表里都有对应行（**用测试名对照表落在文件注释里**）」—— `grep -nE 'TestD5|TestD6|TestD7|TestD8' layering_test.go` 零命中。

**实测证据**（两条变异均全绿）：
```
(a) 删掉整个 internal/xferaudit 行（原 d8 TestD8XferauditIsLeaf 的全部内容）
    → 剩 5 行 → -run 'TestPackageLayering|TestLayeringRulesAreWellFormed' → ok exit=0
(b) 从 internal/clusternodes 行删掉 "github.com/nats-io/nats.go/jetstream"
    （逐字来自 d6 的 TestD6ClusternodesNoNATSNoCluster）→ 两个测试全 PASS
```
**合表本身经三条 lane 独立逐子句核对：四份原文的 10 个 Test 函数的全部子句在新表中都有对应，零丢失**（d7 的 clusternodes 行还补了原来缺失的 database/sql self-check，比原来强），判据 #12 满足。原文覆盖的 distinct 包为 cluster/jsstream/clusternodes/xferaudit/clusteroffline/broker = **6** 个，注释少算一个。

**失败场景**：这次合并是干净的，但它把「四份文件各自守着自己那条」换成了「一张表 + 一条只覆盖 1/6 的完整性断言」。下一个动这张表的人删错一行或删错一个子句，10 个原断言里少掉的那条不会有任何声音 —— 而合表的全部理由就是「narrow restatements cannot mislead anyone again」。没有对照表，下一个人要机械核验只能重做一次本轮的手工逐条比对。

**修法**：把「四份原文的并集」做成数据表 `originalUnion map[string][]string`（6 行），断言每行的 bannedTransitive/bannedDirect **⊇** 对应并集（⊇ 而非全等：全等会让以后新增一条 ban 也变红）；下限从 `< 5` 改成 `!= len(originalUnion)`；文件头补 10 行 `原测试名 → 新 layerRules 行` 对照表；注释 5 → 6。

---

### M10 · layering 表里 `"github.com/nats-io/nats-server"` 这条禁令永远不可能命中，而新写的 well-formed 测试把它冻结成「union 已完整保留」的证明
**id**：F1[deletions]
**文件**：`test/architecture/layering_test.go:60`（ban 项）、`:170-175`（`deps[banned]` 精确键查）、`:232`+`:242`（第二份硬编码 want 列表做字符串全等）

**缺陷**：模块真实路径带 `/v2`，全仓没有任何包的 import path **等于**那个裸串，而匹配是 map 精确键查。该子句是从被删的 `test/d5/regression_test.go` 原样搬过来的 —— 合表动作把一个死子句原封不动保留**并加盖了印章**。

**实测证据**：
```
$ cat > internal/cluster/zz_mut.go   # import _ "github.com/nats-io/nats-server/v2/server"
$ go list -deps … internal/cluster | grep -c nats-server
16
$ go list -deps … | grep -x 'github.com/nats-io/nats-server' | wc -l
0
$ go test -count=1 ./test/architecture/ -run TestPackageLayering
ok  (0.710s)
```
对照：同法注入的另外 4 个变异（jsstream / clusternodes / 两个 bannedDirect raft）全部正确变红 —— 6 条子句里 **5 条有咬合力、这 1 条没有**。

**失败场景**：把整个内嵌 NATS server（16 个包）import 进 `internal/cluster`，L-2「raft 保持 NATS-free」的门依然报绿；而下一个人读到 `TestLayeringRulesAreWellFormed` 的「union 已完整保留」断言会以为这条边界被守着。同根因的未来隐患：`internal/broker`、`internal/cluster` 等 ban 项也是精确键查，今天没子包所以侥幸正确，一旦出现 `internal/broker/<sub>` 就能绕过（仓内已有 `internal/agent/ssproxy` 这种子包先例）。

**修法**：改成 `dep == banned || strings.HasPrefix(dep, banned+"/")`。**那个斜杠是必需的**（修复镜头）—— 写成裸 HasPrefix 会让 jsstream 行的 `internal/cluster` ban 误命中 `internal/clusternodes`，制造假红。并把 well-formed 的「union 保留」证明从「与第二份硬编码列表字符串全等」换成「每条 ban 至少在当前模块图里对应一个真实可 import 的包路径（否则该子句不可能命中）」—— 那样今天就会红在 nats-server 这条上；判据里对「尚非依赖的预防性 ban」需显式标注 preemptive。

---

### M11 · layering 门在 `-run` 限定下会吃 Go 测试缓存、对真实分层违规返回陈旧 PASS
**id**：F4[deletions]
**文件**：`test/architecture/layering_test.go:141-160`（判据全部来自 `exec.Command("go","list",...)`）

**缺陷**：exec 的结果不进 Go 的 testlog，不参与测试缓存键；该测试自身只 `os.Getwd` + `os.Stat(go.mod)`。现在不出事**只是因为**同包 `build_tags_test.go` 碰巧 `os.ReadFile` 了全树每个 `.go` 文件 —— 偶然耦合，不是设计。

**实测证据**（与 finding 相同的命令热过缓存后逐位复现）：
```
$ go test ./test/architecture/ -run TestPackageLayering        # 打热缓存
ok  0.625s
$ cat > internal/cluster/zz_mut.go   # import _ ".../internal/jsstream"  ← 真实 L-2 违规
$ go test ./test/architecture/ -run TestPackageLayering
ok  (cached)          ← 陈旧绿
$ go test -count=1 ./test/architecture/ -run TestPackageLayering
--- FAIL: TestPackageLayering/internal/cluster
```
不加 `-run`（跑全包）时会正确失效 —— 失效能力来自另一个测试文件，不来自 layering 门自己。

**失败场景**：开发者最自然的自查方式 `go test ./test/architecture/ -run TestPackageLayering` 会对真实的分层违规**永久报绿**；一旦 `build_tags_test.go` 被移走/拆包，或 `make gates` 改成带 `-run`，全包路径也一起失去失效能力。

**修法**：让门自己登记输入 —— 在 `TestPackageLayering` 里对每条规则的包目录 `os.ReadDir` + 对其 `.go` 文件 `os.ReadFile`（结果丢弃，目的是进 testlog），并加注释说明原因。
**排除项（修复镜头）**：不要给 gates 加无条件 `-count=1` —— gates 含 `./cmd/tether/`（冷跑约 71s），会把 14s 的集中自检变成 80s+，反而降低「改闸门就跑 gates」的依从性。

---

### M12 · maintidx 登记表里的裸 `Run` 是仓库级**名字**豁免（无 `path:`），配置自称的「第 11 个 god function 不可能不经手改出现」不成立
**id**：F3[lint] / PI-4
**文件**：`.golangci.yml:365`（maintidx exclusion 只有 `text: "Function name: (…|Run|…),"`，无 `path:`）

**缺陷**：maintidx 的消息不含 receiver（实测 `internal/broker/broker.go:815:1: Function name: Run, Cyclomatic Complexity: 70, … Maintainability Index: 1`），而规则没有 path，所以它匹配全仓**所有**叫 `Run` 的函数。plan §8 登记的只有 `Broker.Run`。

**实测证据**：
```
$ grep -rnE '^func \(.*\) Run\(' internal/ cmd/     → 5 处定义
  broker/broker.go:815 (MI 1)  agent/agent.go:615 (MI 49)
  broker/alert_reconcile.go:104 (MI 53)  broker/audit_publisher.go:123 (MI 56)
  broker/alert_webhook.go:207 (MI 60)
A/B 变异：在 internal/broker 造两个逐字节相同的巨兽（70 个 if，CC=211/MI=0），
一个叫 Run、一个叫 RunProbeAlpha，用真实配置跑 →
  只报 RunProbeAlpha，Run 一声不吭
把 under:20 改成 under:55（模拟退化穿过选择器）→ 653 条 maintidx，
  其中 grep -c "Function name: Run," = 0（该门槛下无豁免本应报 3 个 Run）
同 MI 区间邻居（agent.go:1061 loadStateBounded MI 53）正常报出 → 门本身是活的
```
同一份配置在 unparam 段（`:319-325`）自己写着反对名字级豁免的论证：「An earlier revision matched on the PARAMETER name … This is the same defect as a file-wide dupl exemption」—— 逐字适用于 `Run`。

**失败场景**：`Run` 是 Go 里最常见的方法名。`Agent.Run`（MI 49）是 `Broker.Run` 的对称对手方，agent 侧 reconcile/home/proxy 逻辑往它里长是本仓最可能的退化路径之一 —— 而它**已经免检**。未来任何新类型的 `Run` 方法天生免检。违反「豁免必须站点级，不能类别级」。

**修法**：给这条 exclusion 补 `path:`，把每个名字钉到它所在的文件（`Run` → `internal/broker/broker\.go`；`StatusReport` → `internal/broker/clusterstatus\.go`；等）。备选是改用行内 `//nolint:maintidx // <理由>` 挂在那十个函数上（站点级 + 自动过期，但会把 nolint 指令从 14 抬到 24，判据 #3 的预算需同改）。
**与 m3 的交互（修复镜头）**：F7[lint] 建议的对账门判据「每个名字在仓内**恰好一处**同名定义」与本条 fix **直接冲突** —— 补 path 后 `Run` 仍有 5 处定义，那条门会永久红。对账门必须按 `(rule.path, name)` 配对判定，见 m3。

---

### M13 · maintidx `under: 20` 对**未登记**函数仍然注释敏感，四个函数 MI 恰好压在线上（`driveRetire` 加 6 行纯注释即 lint 红）
**id**：PI-3
**文件**：`.golangci.yml:365` 附近（maintidx `under: 20`）

**缺陷**：plan §8 声称「丢掉的只有带内退化检测」，实际丢掉的是「对未登记函数的注释免疫」，而边界上正好站着四个。

**实测证据**（MI 普查：临时禁用 maintidx 豁免 + `under:100`，串行单次）：
```
MI == 20（压线）：driveRetire (cluster_operation_controller.go:951, CC 38)
                 clusterAdminBackend.HandleCluster (CC 42)
                 RestoreFromBackup (CC 38)  runReconcileToStandalone (CC 37)
往 driveRetire 体内插 6 行 // INCIDENT NOTE
  → Function name: driveRetire, CC 38, Halstead 4120.23, MI 19 (maintidx)  ← 红
插 12 行 → 同一条报告，CC 与 Halstead 与 6 行时逐字节相同（只有 LOC 项在动）
四个边界函数各插 1 行 → 全绿（税的粒度是几行，不是一行）
```
配置头部约束 1（maintidx 对注释敏感）我方独立复现为真：`handleGrowTrigger` 基线 CC 52 / HV 7314.54 / **MI 17**，体内插 20 行纯注释 → CC 52 / HV 7314.54 / **MI 16**。plan §1 订正 1 成立。

**失败场景**：CLAUDE.md:98-103 新写下的「注释是资产，`.golangci.yml` 故意不启用 funlen/lll/gocyclo，就是为了不让行数闸门反向激励删注释」在这四个函数上是假的：往 `driveRetire`（集群 retire 的 waypoint 驱动循环，最需要写事故注释的地方之一）加一段 6 行的顺序不可换说明 → `make lint` 红。三条合规出路中最便宜的是**删注释**。

**修法（两条镜头都对原 fix 提出了限制，主进程需注意）**：
- 原 fix (a)「换 gocyclo 做选择器」**踩 X7**：plan §11 X7 把 S3 §4 的 13 个被拒 linter（含 gocyclo/funlen）判 REJECT-FOREVER。不先重开 X7 不得照做。而 X3 的重开条件恰恰指向「换用注释免疫的量」= gocyclo —— X3 与 X7 构成循环（见 m5 与拖延审计 unmet）。
- 原 fix (b)「把 `under` 降到 15」**修复镜头判为有害**：会让 10 个已登记 god function 中的 6 个（MI 17,17,17,18,18,19）掉出报告范围、register 大半变成死豁免，第 11 个 MI 16–19 的 god function 从此静默出现，而边界税只是搬到 MI 15。
- **可采纳的中间路**：保留 `under:20`，把这四个边界函数的实测 MI 值与「它们只差几行注释」写进 register 注释（让触发读作「来看一眼」而不是「删注释」）；若要根治则郑重走 (a) 并同时重开 X3/X7。

---

### M14 · 7 条 dupl 豁免全部按**行区间**钉死 —— 在被钉区间上方增删任何一行注释都会让 `make lint` 变红（覆盖 10 个生产文件）
**id**：PI-5
**文件**：`.golangci.yml:259 / :271 / :278 / :285 / :292 / :303 / :310`

**实测证据**：
```
往 cmd/tether/cluster_ops.go 顶部加 5 行注释（代码整体下移 5 行）
$ golangci-lint run -c .golangci.yml ./cmd/tether/...
cmd/tether/cluster_ops.go:36: 36-57 lines are duplicate of cmd/tether/cluster_ops.go:61-82 (dupl)
+ 反向一条，共 2 issues 红      （豁免钉的是 31-52|56-77）
```
受影响文件：`internal/proto/subjects.go`、`cmd/tether/cluster_ops.go`、`internal/cluster/snapshot.go`、`internal/port/port.go`、`cmd/tether/cluster_add.go`、`cmd/tether/cluster_upgrade.go`、`internal/broker/exec.go`、`internal/broker/run.go`、`internal/clusterroster/roster.go`、`internal/clusterroster/seeds.go`。
附带独立核实：主进程「已豁免文件内追加第三份副本仍会报出新配对」的声称**为真**（在 cluster_ops.go 追加第三份骨架 → 报出 `56-77 lines are duplicate of …:171-192`），行区间钉死的设计确实成立。

**失败场景**：这 10 个文件里包括 `internal/port/port.go`（批 A A4 刻意隔离的 Free↔Revoke footgun，最需要写「为什么不许合并」注释的地方）和 `internal/broker/exec.go`/`run.go`（broker 热路径，注释里记着 subject-shape 检查与 follower short-circuit 的顺序论证）。在这些文件顶部写一段事故注释 → lint 红，三条合规出路分别是：不写注释、手改 `.golangci.yml` 里的行号、或者合并那对重复（而这 7 条注释每一条都在论证不该合并）。plan §8 自己提出过更抗腐的替代（行内 `//nolint:dupl` + `allow-unused:false` 在配对消失时报悬空），实现选了脆的那个。**这条陷阱目前没有任何文字记录。**

**修法**：按 plan §8 自己的建议改成行内 `//nolint:dupl // <理由>` 挂在具体函数上。三点必须一起做（修复镜头）：① dupl 在**两个** partner 处各报一次，两边都要挂；② 会新增约 14 条指令，判据 #3 的预算要同步；③ 精度从「被审查过的那一对」降到「这个函数」—— 第三份副本仍会在自己那一侧报出，可接受。若坚持配置侧，至少在 CLAUDE.md「注释是资产」条款旁明写这 10 个文件的陷阱。

---

### M15 · TLS 账本用 `file:line` 做 key —— 在被钉行上方加注释会同时触发一条安全级告警和一句可验证为假的文案
**id**：PI-6
**文件**：`test/architecture/tls_verify_pairing_test.go:50`（ledger key `"internal/tunnel/tls.go:63"`）、`:129`（offender key = `rel + ":" + line`）

**实测证据**（代码一个字节没动）：
```
在 internal/tunnel/tls.go 的 clientTLSConfig doc 上方插 2 行注释 → 同时红两条：
  1 tls.Config literal(s) set InsecureSkipVerify:true with NEITHER VerifyPeerCertificate
    NOR VerifyConnection: internal/tunnel/tls.go:65 … That combination accepts a certificate from anybody
  1 unverifiedTLSFallbacks entr(y/ies) no longer name an InsecureSkipVerify:true site:
    internal/tunnel/tls.go:63 … The code moved or was fixed — re-read the site
```

**失败场景**：三重反向激励。① 对本仓最敏感的那个文件（TLS fallback）单独加了一条注释税。② 变红后最省力的绿化路径是 `git checkout internal/tunnel/tls.go`（一个文件、一条命令），而正确路径是去改测试文件的字面量 —— 梯度指向「撤回注释」。③ **狼来了**：本仓自己在 `promised_guard_test.go:142` 写下「a gate that cries wolf gets muted — which is the failure mode this whole file is about」；这个门每加一次注释就喊一次「接受任何证书」，第三次之后没人会再认真读它，而真正删掉 `VerifyPeerCertificate` 时喊的是同一句话。第二条的文案「The code moved or was fixed」在最常见的触发原因下就是假的。

**修法**：key 改成 `file:enclosingFuncName`（`internal/tunnel/tls.go:clientTLSConfig`）—— AST 已在手，取包含该 CompositeLit 的最近 FuncDecl 名即可；函数改名/删除仍会失配（保住原设计意图），加注释不会。修复镜头核实两个站点分属 `clientTLSConfig` 与 `clientTLSConfigPinned`，不会撞 key。**顺手**：账本条目的 reason 字符串里自己嵌了一个会漂移的行号引用（`the pinned path (tls.go:121)`），门只校验 KEY 不校验 reason，改 key 时应一并换成函数名（见 §6 集体盲区 B3）。同时把 stale 分支文案改成「moved, fixed, or a line was inserted above it」。

---

### M16 · noctx 的四文件豁免只有 `path:` 没有 `text:` —— 是**整文件级** noctx 免疫，而配置注释声称「Listed individually so that a fifth one cannot join them silently」
**id**：F2[lint]
**文件**：`.golangci.yml:219`

**实测证据**（A/B 同一次运行，用**未改动的真实配置**）：
```
向 internal/agent/upgrade.go（豁免名单内）追加包级 http.Get（默认 client：无 Timeout、无 ctx）
向 internal/agent/roster_cache.go（同包、名单外）追加逐字相同的违规
$ golangci-lint run -c .golangci.yml ./...
internal/agent/roster_cache.go:119:23: net/http.Get must not be called. … (noctx)
* noctx: 1
→ upgrade.go 里的新违规 0 报告
```
规则原文 `path: (internal/proxydial/httpconnect|internal/agent/upgrade|internal/broker/cluster_grow_cutover|internal/natsconf/takeover)\.go`。对比同文件 `:250-310` 的 dupl（每条都钉 partner 行区间，并写明「a file-wide exemption would silently absorb it」）—— 这里恰好用的就是它自己批判的形态。今天实际遮住 4 个站点：`upgrade.go:276` `http.Client.Get`、`cluster_grow_cutover.go:384` `http.Client.Get`、`httpconnect.go:39` `tls.Conn.Handshake`、`takeover.go:38` `os/exec.Command`。

**失败场景**：`internal/agent/upgrade.go` 正是 agent 自升级下载二进制的路径。今天那处之所以可接受，理由（写在配置注释里）是「The download is bounded by the client Timeout」—— 这是**站点性质**的论证，但豁免是文件级的。以后有人在同一文件里写一个用默认 client 的 `http.Get`（无任何超时）或换成 `http.Post` 上报，noctx 一声不吭。同理 `takeover.go` 里新增的 `exec.Command`、`cluster_grow_cutover.go` 里新增的裸 HTTP 探针都免检。这与 racknerd 那类「远端不响应就永久挂住」的事故同族，而闸门的设计意图恰恰是拦它。

**修法**：四条各自拆成带 `text:` 的独立规则把签名钉死，例如
```yaml
- linters: [noctx]
  path: internal/agent/upgrade\.go
  text: "\\(\\*net/http\\.Client\\)\\.Get must not be called"
- linters: [noctx]
  path: internal/natsconf/takeover\.go
  text: "os/exec\\.Command must not be called"
```
这样新增的 `http.Get` / `http.Post` / `net.Dial` 都会报出。text 与 golangci 消息格式耦合，但失效方向是 fail-closed（重新报出、有人来看），且 `make lint` 已 pin 版本（v2.5.0），风险受控。顺带同族的轻量版：`path: internal/(pty|agent)/` + `text: "os/exec.Command must not be called"`（`:178-180`）对整个 `internal/agent/**`（含 ssproxy 子包）的所有未来 `exec.Command` 免检，建议收窄到具体文件。

---

### M17 · PTY 失败的 errno 判据不全（devpts 耗尽是 ENOSPC）+ `pty_unavailable` 的 exit class 与 usage.md 的重试规则冲突 —— **两条 lane 对同一张表给出相反读法，主进程必须先裁定**
**id**：PC-2（MAJOR，impact 链被范围镜头推翻）/ F2[deletions]（MAJOR）/ D5（MAJOR）
**文件**：`internal/agent/run.go:115`（errno 判据）；`cmd/tether/error_hints.go:154`+`:336`（分类与 hint）；`docs/usage.md:1549`+`:1559`（重试规则）

**缺陷（两半，都成立）**：
① **errno 判据不完整**：`run.go:115` 只判 `errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE)`，其余全落终态支。而 PTY **耗尽**在 Linux 上返回的是 **ENOSPC**（`open("/dev/ptmx")` → `ptmx_open` → `devpts_new_index`，超出 `max` / `pty_limit - pty_reserve` 时返回 `-ENOSPC`）。`internal/pty/pty.go:65` 的 `s.SetSize()`（ioctl TIOCSWINSZ）失败也走同一 else 支，被报成 `pty_unavailable` —— 它根本不是 ptmx 开不开的问题。
② **分类与文档冲突**：`pty_unavailable` → `exitUnavailable`(69)，hint 写「Retrying will not help until the host changes」，而 `docs/usage.md:1559` 逐字写「**健壮重试规则**：把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态」。同一次 Y2 拆分把 `download_http_status`/`download_too_large` 正确送去 **64**（注释理由「the operator has to fix the argument」）。仓内既有约定也是「需运维动手 → 64」（`node_offline`/`port_exhausted`/`name_taken`/`jetstream_unavailable` 全给 64），本仓刻意不用 69 表达这件事。

**实测证据**（privileged 容器挂 `devpts newinstance,max=1`，CGO_ENABLED=0 探针连开两次，可复现性镜头独立复现）：
```
open #0 ok
open #1 FAILED: open /mnt/pts/ptmx: no space left on device
  EMFILE=false ENFILE=false ENOSPC=true ENOMEM=false EAGAIN=false EIO=false
  => tether new code reports: pty_unavailable (TERMINAL, exit 69)
本机 /proc/sys/kernel/pty/max = 4096（非 root 再减 pty_reserve 1024 → 约 3072）
```
错误链可 `errors.Is`：creack/pty 用 `os.OpenFile` → `*fs.PathError` 包 `syscall.Errno`；`internal/pty/pty.go:63` 用 `%w` 包装。

**读法冲突（必须裁定）**：PC-2 写「监控按 §9.13 把 69 当终态、停止重试」；F2[deletions] 与 D5 写「69 属文档定义的**可重试**类，于是永久故障被无限退避重试」。范围镜头实测 `usage.md:1559` 支持后者，并据此判 PC-2 的 impact 链不成立（改动前 `pty_alloc_failed` 无 exit class → 落 70，同样可重试；改动后 ENOSPC → 69，同样可重试 ⇒ **自动化行为零变化**，实际损害只剩 hint 对 ENOSPC 说了假话），建议 PC-2 降 MINOR。**但 ① 的技术事实与 ② 的不一致本身不受影响。**

**修法**：
```go
// ptyTransientErrnos: allocation failures that clear on their own as sessions close.
// ENOSPC is the devpts index exhaustion errno (/proc/sys/kernel/pty/max, or a devpts
// `max=` mount option) -- the single most likely transient PTY failure on a busy host.
var ptyTransientErrnos = []syscall.Errno{
	syscall.EMFILE, syscall.ENFILE, syscall.ENOSPC, syscall.ENOMEM, syscall.EAGAIN,
}
```
`run.go:115` 改成 `if ptyFailureIsTransient(err) {`。**errno 列表可以是变量，分支体内的 `Reason` 字面量必须保留**（`run.go:100-114` 的注释解释了为什么：wire-code 覆盖门静态解析字面量）。同时：把 `pty_alloc_failed` 的 hint 从「ran out of file descriptors」改成「fd 或 pty 上限耗尽（/proc/sys/kernel/pty/max）」—— **注意** `cmd/tether/error_hints_test.go:146` 把该 hint 钉在子串「file descriptors」上，需同 commit 改。② 侧：要么把 `pty_unavailable` 归到 64（与 download_* 及既有约定一致），要么在 usage.md §9.13 给 69 加显式例外并同步「健壮重试规则」—— **无论哪条，本轮都必须动 usage.md**（它本轮一字未改，运维拿不到反证）。

---

### M18 · Y2 四个新错误码**发射侧零测试** —— 两次变异证明分支被打断后全仓相关包依然全绿
**id**：PC-3
**文件**：`internal/agent/upgrade.go:282`（sentinel 包装）、`internal/agent/run.go:115`（瞬态判据）

**实测证据**：
```
变异 A：upgrade.go:282 把 %w 改成 %v（切断 sentinel 链 → 每个 404 都退回
        download_failed = exitTransient = 永久重试，正是 Y2 要治的病）
  go test ./internal/agent/ ./cmd/tether/ ./test/architecture/... ./test/determinism/...
  → 四包全部 ok，未变红
变异 B：run.go:115 条件改成 if false（瞬态支永久不可达）
  → ./cmd/tether/ ok 71.5s，./internal/agent/ ok 7.0s，未变红
对照：连字面量一起删掉整个 case errors.Is(err, ErrUpgradeHTTPStatus) 分支
  → TestClassificationTableKeysStillHaveEmitters 变红（4 死键）
$ grep -rn 'ErrUpgradeHTTPStatus|ErrUpgradeTooLarge' --include=*_test.go .
（无命中）
```

**失败场景**：Y2 的全部价值在「码 → 重试语义」的对应关系上，而这层对应关系没有任何咬合力：任何人把 `%w` 写成 `%v`、把 errno 判据写错、或把 `fetchURL` 换成别的实现，都会静默退回「一个 catch-all + 永久重试」，而 CI 全绿。D1 反向门只保护「字面量存在」，不保护「分支可达 / 判据正确」—— 正是本仓禁止的恒等式守卫形态。

**修法**：补三组表驱动测试（落点见 §7），并把 `error_code_coverage_test.go` 的反向门从「字面量存在」升级成「每个 Y2 码必须有一条断言其**触发条件**的测试」的登记表。

---

### M19 · `already_voter` 从 wire 删除，但活文档 `docs/usage.md:1596` 仍把它列为 adminsock 的「稳定 `code`」
**id**：PC-5 / F3[deletions] / D4
**文件**：`docs/usage.md:1596`（本轮一字未改）

**缺陷**：本次删掉了 `internal/adminsock.CodeAlreadyVoter` 与 `brokerCodeExitClasses` 的对应键，手册没跟着扫。

**实测证据**：
```
$ git diff --stat docs/     → 只有 architecture.md (1/1) 与 deploy-tier-gotchas.md (1/1)
$ git status --porcelain docs/usage.md   → 空
$ grep -n already_voter docs/usage.md
1596: adminsock cluster admin 回复带稳定 `code`（`not_leader`/`already_voter`/`not_a_voter`/…），CLI 据此映射退出类 + 提示。
$ grep -rn 'usage.md' --include=*_test.go .   → 无任何测试对账这份枚举
```
**删除动作本身安全**（三条独立证据链）：`git log -S already_voter --all` 只有引入它的 `f1a2bde`；同 commit 的 `b2-review.md:11` 自陈「CodeAlreadyVoter/CodeNodeUnknown 暂未接」；L08 死代码审计用 go/packages（Tests:true + 全部 8 tag）实测引用计数 0 ⇒ 不存在「旧 broker 发它、新 ctl 退 70」的路径。

**失败场景**：运维/自动化按手册写 `case already_voter:`，该分支永远不会命中，且 CLI 已无映射（落到默认 70）。反过来，下一个人 grep 会同时读到「稳定 wire 码」和「已删除」两个互相矛盾的权威说法。D1 反向门治的是「表里有死键」，这里剩下的是「文档里有死键」—— 同一缺陷换载体，正是 memory 里 contract-change-sweep 记过的复发形态。

**修法**：删掉 `usage.md:1596` 的 `already_voter`（或改写成「历史码，vX 起不再发出」），并在同 commit 说明这次 wire 表面收缩为什么无需 ProtoVersion 动作（CLAUDE.md §5 把破坏性 wire 变更定为要走跨版本路径 —— 实测该码从未被任何 binary 发出，正是那句论证的内容）。抗腐化：把这份枚举与 `internal/adminsock` 的 `Code*` 常量集合做双向对账（D1 反向门的文档侧对偶）。

---

### M20 · `main-noncli-code-lines` 无阈值：10 个非 cobra 文件里任何 ±1 行代码都是红构建
**id**：PI-9（**修复镜头与范围镜头均指出它反对的是 plan §4 的明文裁决**「只登记，无绝对阈值」，应读成「摩擦频率被低估」）
**文件**：`test/architecture/structural_budget_test.go:193`（无条件返回 entry；另两维在 `:119`/`:156` 有 `> threshold` 过滤）

**实测证据**：
```
往 cmd/tether/exitcode.go 追加 var probeOneLine = 1（1 行）
  → BUDGET EXCEEDED: main-noncli-code-lines cmd/tether = 1117, ledger says 1116 (+1)
  → -update-structural-budget → REFUSES to widen
受管文件实测 10 个：alert_gate / cluster_doctor_online / cluster_lock_keeper / cluster_rotation /
cluster_secrets / cluster_status_card / error_hints / exitcode / jsonout / logging（cmd/tether 共 52 个生产文件）
```

**失败场景**：这 10 个文件是 exit code 分类、错误提示、告警门、集群状态卡片 —— 最常因运维反馈而增删一两行的地方。每次这种改动都要跨树改 golden + 写 commit 论证；跌了也红（要跑 `-update`，而那会触发 M3）。省掉通行费的三条路径是「把修复写得更密」「把逻辑放进一个 cobra 文件里（正好是这一维想阻止的事）」「往文件里写 cobra（C4）」—— 三条都让代码更差。频率上：这一维的摩擦发生在**每一次净改动**上，另两维只在跨过 40/20 边界时发生。

**修法**：给这一维也加阈值语义（带余量的上限，如「不许超过 1200」，余量用完才手改），或按量子（如 50 行一档）而不是逐行棘轮。代价要认：余量会让「跌了也红」的锁定改善能力弱一档 —— 这是显式取舍。若主进程坚持 §4 原裁决，请在 golden 头部补一句频率论证。

---

### M21 · 三维预算的盲区正好是最省力的动作（往既有文件塞代码全绿），而 X2 判 REJECT-FOREVER 的重开条件在同一次改动里已被自己满足
**id**：PI-8（**索要动作触及 plan §11 X2，见修法**）+ 拖延审计员（列为「伪装的 REJECT-FOREVER」）
**文件**：`test/architecture/structural_budget_test.go:26`（三维定义）、`:196-218`（`countCodeLines`）；`docs/reviews/line2-plan.md:521`（X2）

**实测证据**：
```
往 internal/broker/loopset.go 追加 120 个自由函数（约 1000–1082 行真代码，文件 219/300 → 1301/1420 行，
无新文件、无 >40 方法的新类型）
  → go build ./internal/broker/ OK
  → go test ./test/architecture/... ./test/determinism/...  → 两包全 ok
对照：同样代码挂成 60 方法的新类型 → NEW over-budget entity: type-methods … = 60（红）
      新建一个 4 行的职责命名文件 → BUDGET EXCEEDED: pkg-files internal/broker = 71（红）
```
X2 的重开条件原文是「找到一个注释免疫的包体量度量，**且** pkg-files 被证明漏掉了真实增长」—— 半 1：`countCodeLines`（go/scanner mode 0，注释免疫）是**本次新写**并已用于 main-noncli 维度（往 cmd/tether 加 500 行注释 golden 不变，已实测）；半 2：上述变异即证明。**两半都已满足。** 另外 X2 的三条支撑里，「当前只剩 133 行余量而闸门会被本流程自己打红」是本流程的**时机异议**，不构成永久性论证。

**失败场景**：两个已装的维度把「拆文件」和「给既有类型加方法」都变成需要带论证手改预算的红构建，而唯一没被度量的方向是「往既有文件里塞代码」—— 同时 CLAUDE.md 新增的文件纪律又写着「新代码优先并入职责匹配的既有文件」。两条约束叠加，指向的是 **god file**：`internal/broker` 现在 70 个文件，任意一个都可以无限长而闸门无声。

**修法（必须先动 §11，不得直接加维度）**：
- 主进程的正确处置只有两种：**按 plan 自己写下的重开条件走重开流程**，或**改写 X2 那一行**（因为它引用的重开条件已被满足，论证已被自己推翻）。
- **若选择加维度**，修复镜头警告：PI-8 原建议的阈值「只圈住 internal/broker / cmd/tether / internal/cluster」等于给全仓最热的三个包装上**逐行棘轮**（internal/broker 70 个文件，任何一次加三行的提交都要手改 golden）—— 那正是 M20 指控的缺陷从 10 个文件扩到 70 个。要做就必须用带余量的上限或量子记法。
- 变异验证必须成对：「往 internal/broker 加 500 行纯注释保持绿」+「往既有文件追加 1000 行真代码变红」。

---

### M22 · `make lint` 不传 `--build-tags`，23 个 tag 后的文件一条都不 lint —— lint 的覆盖面严格小于本轮新装的 `vet-tags`
**id**：GI-2（**范围镜头 low confidence：这是存量状态、plan 未承诺改，且其头号证据已被本次改动修掉；修法会与 plan §13 冲突**）
**文件**：`Makefile:148-172`（lint 配方）、`.golangci.yml:43-44`（`run:` 段只有 `timeout`）

**实测证据**：主进程自己的两份产物给出直接对照 —— `g1cfg-alltags.json` 比 `g1cfg-notags.json` 多出
```
unused:      func assertNoGoroutineLeak is unused @ test/d8/setup_test.go:273
copyloopvar: internal/agent/transfer_test.go:673
noctx:       internal/adminsock/client.go:29 / internal/agent/exec.go:339 / internal/agent/upgrade.go:386
```
tag 后文件实测 23 个。`ALL_TEST_TAGS` 变量已存在（`Makefile:32`），只接给了 `go vet`。安全性旁证：`go vet -tags '<全部 7 个>' ./...` 已 rc=0，说明同时打开不会有符号冲突。

**范围镜头的两点订正（主进程需知）**：① 这是**存量**状态（改动前后都不传），plan §6 从未承诺改；② finding 的头号证据 `assertNoGoroutineLeak is unused` 恰好已被本次改动修掉（d8 泄漏门接线），那个具体例子今天是空的。留在范围内的只有一句：新配置宣告「T1 零基线」而该基线从未在 23 个 tag 后文件上测量过。

**失败场景**：`vet-tags` 的整个立论是「这 23 个文件此刻正没人编译」，本轮把编译补上了，但 lint 依旧看不见它们：这些文件里的 forcetypeassert/nilerr/bodyclose/unused 永远不会被报出。具体后果已真实发生过一次（`assertNoGoroutineLeak` 定义了却零调用点，默认 lint 口径完全沉默，靠人读代码才发现）。

**修法（按 §0 定性为新工作，需主进程给出 DO / REJECT-FOREVER）**：lint 配方加 `--build-tags '$(ALL_TEST_TAGS)'`，或在 `.golangci.yml` 的 `run:` 里写 `build-tags:` **并让 `build_tags_test.go` 把它也纳入对账**（否则第三方 lint 配置又是一份手抄清单）。落地前须重测基线（约 4 条新 issue，其中 `_test.go` 里的会被 part-1 豁免吃掉，剩下的逐条判）—— 这与 plan §13「永远不提交一个红的 lint 配置」冲突，故不应算进本次交付的验收清单。

---

### M23 · CLAUDE.md 闸门表把结构棘轮写成 `test/architecture/budgets` —— 该路径不存在；plan §1 订正 5 亲手判过的错误类型在新文本里复发
**id**：D2 / PI-11 / PF-10 / GI-6 + 完成判据审计员
**文件**：`CLAUDE.md:112`

**实测证据**：
```
$ ls -d test/architecture/budgets      → No such file or directory
$ ls test/architecture/
build_tags_test.go  layering_test.go  structural_budget_test.go  testdata  tls_verify_pairing_test.go
$ grep -rn 'test/architecture/budgets' .
CLAUDE.md:112 与 S3 原稿（S3 写的是 budgets_test.go + testdata/budgets_golden.txt + -update-budgets）
```
真实载体是 `structural_budget_test.go` + `testdata/structural_budget_golden.txt`。同表其余 9 行位置列逐个 `ls` 全部存在。**这一行是照抄 S3 未订正的残留。**（GI-6 附带称 `-update-budgets` flag 名也不存在于 CLAUDE.md —— 可复现性镜头核实 CLAUDE.md:123 用的是正确的 `-update-structural-budget`，该 sub-claim 不成立。）

**失败场景**：CLAUDE.md 每会话加载，位置列的唯一用途就是让下一个会话直接够到文件。一个 `ls`/`grep` 不到的路径会让读者以为闸门被删了，或者去新建一个 `budgets` 包 —— 本仓 `internal/tunnel` 四份 fence 测试就是这么长出来的。plan §1 订正 5 花整节论证「照抄不存在的路径会把过时信息写进 CLAUDE.md」，然后新表自己写了一个。

**修法**：改成 `test/architecture/structural_budget_test.go`（golden 在 `testdata/structural_budget_golden.txt`）。同表第 7 行「wire 错误码 ↔ exit class | `cmd/tether/` 4 处」也应换成可 `ls` 的文件名（实测是 `error_code_coverage_test.go` / `exitcode_test.go` / `error_class_test.go` / `wire_code_namespaces_test.go`）；「存量 436 个」这种会漂移的数字改成不含数字的表述（权威处是 `legacy_process_named_funcs.go` 头部 + M6 建议的 published 常量）。抗腐化：加一条门断言 CLAUDE.md 闸门表里每个反引号路径都在文件系统里存在（可与 C3 的路径存在性扫描合成同一条实现）。

---

### M24 · 归档基础设施半落地：`docs/reviews/archive/` 是 git 装不下的空目录（判据 22 只在本机为真），且 plan 点名要判定的 `cluster-ha-realmachine-test-plan.md` 无任何裁决
**id**：PF-6 / F8[deletions] / D8 + 完成判据审计员
**文件**：`docs/reviews/archive/`（空）、`docs/reviews/INDEX.md`、`docs/cluster-ha-realmachine-test-plan.md`

**实测证据**：
```
$ ls -A docs/reviews/archive | wc -l          → 0
$ git ls-files docs/reviews/archive | wc -l   → 0     ← git 不携带空目录
$ git status --porcelain | grep archive        → （无输出）
$ git status --porcelain | grep batch-a
R  docs/batch-a-roadmap.md -> docs/reviews/batch-a-roadmap.md      ← 去的是 docs/reviews/，不是 archive/
$ grep -n batch-a docs/reviews/INDEX.md        → （无 —— 第一次实施就没登记）
$ ls docs/cluster-ha-realmachine-test-plan.md  → 仍在 docs/ 顶层（命中新条款自己的 *-plan.md glob）
$ git check-ignore -v docs/cluster-ha-realmachine-test-plan.md
.gitignore:67:/docs/cluster-ha-realmachine-test-plan.md            ← 本地 scratch，未被跟踪
$ grep -rn 'cluster-ha-realmachine' docs/reviews/INDEX.md CLAUDE.md   → 零命中
```
INDEX.md 的「存量 389 份不回填」实测准确（`find docs/reviews -name '*.md' | wc -l` = 391 − 本轮 2）。

**失败场景**：① 判据 22 是「本机文件系统为真、仓库为假」—— commit + fresh clone 后 `test -d docs/reviews/archive` 失败，27 条凭据之一翻红；② `archive/` 是**无条款指向的孤儿**（CLAUDE.md step 7 与 INDEX.md 都写「`git mv` 进 `docs/reviews/`」），下一个人不知道该往哪放，两种做法都能引用原文之一；③ plan §10 E1 点名要判定的第一个适用对象被跳过 —— 它继续以「活基线」身份躺在 docs/ 顶层（钉在 v0.4.2，现网已 v0.4.7，3 处引用）。

**修法**：二选一并让条款与判据一致 ——(a) 放弃 `archive/`，判据 22 改成 `test -f docs/reviews/INDEX.md && test ! -f docs/batch-a-roadmap.md`（这就是当前实现的语义）；(b) 若 `archive/` 真要用来放更老的冻结件，放一份被跟踪的 `archive/README.md` 说明收什么，并把条款目标路径改过去。另外对 `cluster-ha-realmachine-test-plan.md` 出明确判定（归档 / 保留为活文档并更新到 v0.4.7）并写进 INDEX.md —— 注意它被 .gitignore 忽略，「归档」对它意味着的动作与对已跟踪文档不同，M25 那条新门也必须显式处理这一类。

---

### M25 · step 7 的归档判据自称「机械，不靠临场判断」，但可操作形式是一句对假想未来读者的反事实提问，且整条**没有任何闸门**
**id**：D6
**文件**：`CLAUDE.md:58-61`（条款）、`:64-66`（自陈失败机理）

**缺陷**：plan §10 E1 明确要求「条款必须带**机械判据**」。条款里唯一真正机械的部分（`*-plan.md` / `*-review*.md` / `*-tasklist.md` / 已收尾的 `*-roadmap.md` 不得留在 `docs/` 顶层）被降级成括号里的举例，且「已收尾的」四个字又把主观判断塞回来；给出的「可操作形式」是**问「下一次改代码的人需要读它吗」**——与 `03ff578` 那次「判断正确但没成条款」失败时用的是同一把尺。

**实测证据**：条款自己在 `:64-66` 记录了失败机理（「这条必须是条款而不是一次清扫：`03ff578` 做过一次正确的归档，但没成条款」）；落地当天就被自己的 glob 违反（`ls docs/*.md` 含 `cluster-ha-realmachine-test-plan.md`，且无裁决）；`grep -rn 'docs/reviews' --include='*_test.go' .` 只命中 docs_wire_version 的排除逻辑 —— 全仓无任何闸门管这件事。

**失败场景**：下一个 phase 结束时，判断题的答案又只取决于当时那个人心里怎么想；沉积会在同一位置第三次长出来（第一次 `03ff578` 之后、第二次 `batch-a-roadmap.md`）。而在一个把「机械可判 + 变异验证」当尺子的仓库里，一条自称机械却无闸门的条款还会给读者错误的严格度印象。

**修法**：把机械那半升格为闸门（扫 `docs/*.md` 顶层，断言无 basename 命中 `-(plan|review|review-round[0-9]+|tasklist|roadmap)\.md$`），配一份可排空的具名豁免账本（每条一行理由 + 反向断言）；**必须显式排除 gitignored 的本地 scratch 文档**，否则这道门会在开发者本地工作树上永久红（`cluster-ha-realmachine-test-plan.md` 就是这一类）。CLAUDE.md 保留那句提问作为**判断新形态文件**的补充启发，但删掉「机械，不靠临场判断」这个当前不成立的自我标签。

---

### M26 · 判据 #20 按写法不可满足 —— 实现选了比 plan 更强的路径（删 test/c7 + 删死 tag），而「唯一凭据」没跟着订正
**id**：PF-8 / D13 / IDG-11（#20 部分）+ 完成判据审计员
**文件**：`docs/reviews/line2-plan.md:616`

**实测证据**：
```
$ grep -c c7_integration Makefile        → 0        （判据要求 ≥1）
$ ls test/c7                             → No such file（第二个 grep 无文件可 grep）
$ grep -n '^ALL_TEST_TAGS' Makefile      → 7 个 tag，无 c7_integration
plan 进度节：「删 test/c7/ 与死 tag c7_integration」  ← 与 plan:616 直接冲突
```
实质交付确认到位且更强：`internal/broker/cluster_health_monotonicity_test.go` 的 `TestClusterExitCodeStaysNonZeroThroughRecovery`（7 个 waypoint，含 N=2-stable credrot 陷阱的反面检查 + 时序乱序自检）+ `TestClusterExitZeroImpliesRealHA`（穷举 forceSingle×leader×voters0-7×streamShort，含 greens==0 非空泛断言），我方实跑 rc=0，M8/M9 变异能红。附带：`build_tags_test.go:239` 的非空泛文案仍写「the repo has 8」，实测自定义 tag 只剩 **7**。

**失败场景**：§14 被 plan 定义为「流程结束时这份清单是唯一凭据」，现在有一条永远变红的行。外审若逐条机械跑这张表会得到假红，把注意力耗在一个已解决（而且解得更好）的问题上，进而降低对整张表的信任度 —— 而这张表里 #4/#11/#13/#16 是真的需要被当真的。

**修法**：#20 改写成与裁决一致的形状，例如「`test ! -d test/c7` 且 `grep -c c7_integration Makefile` = 0 且 `go test ./internal/broker/ -run TestClusterExitZeroImpliesRealHA` 绿」，并在 §2 归宿总表 A3 行注明裁决从「实现它」改成「删占位 + 用更强的穷举扫描替代」。顺手把 `build_tags_test.go:239` 的 8 改 7。

---

## 3. MINOR

### m1 · unparam 两条豁免的函数名列表左侧未加锚，后缀同名新函数被静默吸收
**id**：F5[lint] · **文件**：`.golangci.yml:346`
正则形如 `(verifyClusterSeam|…|mint|pollClusterHealth|envGet)(\$[0-9]+)? - \w+ always receives`，左侧完全开放。实测（用配置里逐字取出的正则跑 grep -E 比对构造消息）：`mint - ttl always receives …` SUPPRESSED（预期）、`remint - ttl …` SUPPRESSED（**不该**）、`spawnsafeenvGet - key …` SUPPRESSED（**不该**）、`spawnsafeEnvGet …` reported（仅靠大小写幸免）。unparam 消息含 receiver 前缀（`(*forceSingleArm).mint - …`），所以左侧必须开放 —— 但要有边界。今日两份名单**无失效条目**（13 个 `is unused` 名字 ↔ 14 条实测报告、9 个 `always receives` 名字 ↔ 9 条，一一对上）。场景：`mint`（4 字符）与 `envGet` 出现同后缀新函数不难想象（`remint`/`premint` 之于凭据轮转相当自然），这份名单声称的「新 seam 必须被人看过一遍」被削弱一档。修法：左侧包成 `(^|\.)(name1|name2|…)`（裸函数走 `^`、方法走 `\.`，两种形态都命中）；并建议补 `path:`。

### m2 · 7 条 dupl 豁免里唯一一条没钉全行区间，与紧随其后的自述注释不一致
**id**：F6[lint] · **文件**：`.golangci.yml:259`
`text: "duplicate of .internal/proto/subjects\\.go:(175|298)"` 只钉起始行，其余六条都是 `:(31-52|56-77)` 形态；而注释写「Every dupl exemption below pins the PARTNER'S LINE RANGE」。实测吸收宽度：`…:298-310` SUPPRESSED（预期）、`…:175-999` SUPPRESSED（区间任意扩大仍被吸收）、`…:1750-1762` SUPPRESSED（前缀匹配；`wc -l subjects.go` = 434 故今天非活跃风险）。场景：`subjects.go:175` 那段 builder 若被编辑得更长（175-188 → 175-210），豁免仍生效，而这已不是被审查过的那一对。修法：改成 `:(175-188|298-310)`。

### m3 · `.golangci.yml` 的三份账本（7 对 dupl 行区间 / 22 个 unparam 站点名 / 10 个 maintidx 名）是本次改动里**唯一没有反向断言**的豁免集合
**id**：F7[lint]（修复镜头驳回其判据措辞，见 §8） · **文件**：`.golangci.yml:159` 附近
`grep -rn 'golangci' --include='*.go' test/ cmd/ internal/` 唯一命中 `enum_switch_default_test.go:16` 的一句注释 —— **全仓没有任何 Go 代码解析 .golangci.yml**。对照：`legacy_process_named_list.go`（published 计数）、`promised_guard_test.go`（legacyMissingGuards 反向断言）、`unverifiedTLSFallbacks`、`allowedEnumDefaults`、`historicalWireDocs` 全都有。今日三份名单我方核对为干净（无失效条目）。失效方向性也清楚：改名与行号偏移是 **fail-closed**（重新报出 → 有人来看），**删除函数是静默腐化**。场景：这是「豁免必须自带过期压力」在本次改动里唯一没落实的地方，也正是 M12 那个 `Run` 缺陷能存在并未被发现的机制原因。修法：加一条纯 AST/文本对账测试（不需要跑 golangci-lint，可进 `make test`）：对 unparam/maintidx 名单里的每个函数名断言仓内存在同名定义，对 dupl 名单里每条 `path` 断言文件存在。**判据必须按 `(rule.path, name)` 配对判定「该规则覆盖的站点恰好一个」，不能写成「全仓恰好一处同名定义」** —— 后者与 M12 的 fix 冲突（补 path 后 `Run` 仍有 5 处定义，门会永久红）。

### m4 · noctx 的 `path: ^test/`（无 `text:`）把整个 test 树含 harness 非测试文件一起免检，与同文件 T1 段自己立下的原则矛盾
**id**：F8[lint]（**范围镜头据 plan §15 仲裁主张上调为 MAJOR**） · **文件**：`.golangci.yml:184-185`
同文件 `:226-229` 专门论证 T1 类豁免**不能**写成 `^test/`：「Widening it to the whole test tree would also silence test/clusterharness/clusterharness.go:138 … Harness code is not test code just because it lives under test/.」而 noctx 落在 **T1 块**（`:66`），plan §15 仲裁明文裁决「T1 正确性类只豁免 `_test\.go`；T2 结构类豁免 `(_test\.go|^test/)`」—— 故这是对本次改动自己的仲裁记录的违反，不只是同文件两处原则打脸。今天实际吸收的 harness 站点：`clusterharness.go:134` net.Listen（另有全局 text 规则覆盖）、`test/e2e/parallel/main.go:442` 与 `shard.go:98` 的 exec.Command（符合注释里「the parallel e2e runner reaps its own workers」的论证）—— 即实际损失接近零，风险在未来：`test/clusterharness/` 是 e2e 全矩阵共用的 harness，若出现无超时无 ctx 的 `http.Get`/`net.Dial`（例如探测 broker readyz），noctx 不会报。修法：收窄成 `path: ^test/` + `text: "os/exec.Command must not be called"`，或改成 `(_test\.go|^test/e2e/)`；若保留现状则须在 `:226-229` 旁写明 noctx 为何是例外。

### m5 · `//nolint` 真实指令 14 条 > 判据 #3 的预算 12；4 条 gocritic 是 exitAfterDefer 的**全部**站点，即该 linter 在本树今天没有任何在线检测量
**id**：F9[lint] · **文件**：`.golangci.yml:63`、`:127-128`
`git grep` HEAD 基线 10 条（非 `_test.go` 5）；工作树 16 行 grep 命中，扣掉 2 行叙述 = **14** 条真指令（新增 6：`gocritic exitAfterDefer` ×4 于 `cmd/tether/cluster.go:411`、`cluster_status_nats.go:204`、`main.go:81`、`test/e2e/parallel/main.go:232`；`nilerr` ×2 于 `internal/agent/roster_cache.go:101`、`internal/cli/cluster_endpoints.go:81`；删除 2 条悬空）。plan §7 只点名两条 nilerr ⇒ 预算 12。`grep -c "nolint:gocritic"` = 4 且 review-lint.json 里 gocritic 报告数 0 ⇒ **该检查在本树的每个命中点都被抑制**，唯一剩下的价值是「第 5 个站点必须新写一条带理由的 nolint」（这确实是真实过期压力，但与配置注释「finds real resource leaks」给读者的印象不符）。所有 14 条都指名 linter 且带理由（nolintlint 三项均开）。修法：把判据 #3 的预算改成实测值并逐条列出新增 6 条；在 `:127-128` 补一句实测事实。**排除项（修复镜头）**：不要为一个被全部抑制的检查去重排 os.Exit 路径 —— `main.go:78-81` 那几条 nolint 的散文正是「defer 已在上一行显式执行」的论证，重排是纯 churn 且会删掉论证。

### m6 · docs 版本棘轮的计数不分版本：追加一处**正确的** `tether.v2.*` 会被报成「carries 70 stale wire-subject path(s)」，而 v1→v2 现代化对棘轮不可见
**id**：IDG-10 / PI-10 · **文件**：`test/determinism/docs_wire_version_test.go:55`（`wireSubjectPathRe` 不带版本条件）、`:116-131`（只比总数）
实测：往 `docs/architecture.md` 追加 `` `tether.v2.session.attach` `` → 红，`carries 70 stale wire-subject path(s), the ledger says 69 (grew to, by 1)` + `GREW: a passage was added or edited that states a subject path this code no longer speaks. Fix the passage — do not raise the number` —— **与事实相反**；把一处 `tether.v1.s` 改写成 v2 → 计数仍 69，改善不可见；破坏掉任一处 v1 使计数回到 69 → 绿（可对冲）。该文件实测 42×`tether.v1.s` + 27×`tether.v1.c` = 69，零 v2。危险方向（新增 v1）仍被拦住，故是精度问题而非漏洞。场景：合规且最省力的动作变成「不在 architecture.md 里写当前 subject」或「删掉一条正确的 v1 引用把计数拉回」—— 指向删文档，而「记录 v1→v2 迁移在哪个 subject 上发生」正是第 3 层该干的事。修法：计数只统计 `version != proto.ProtoVersion` 的匹配（把 historicalWireDocs 的语义写实成 stale-only），失败文案分开 GREW/SHRANK 归因。修复镜头核实该改法不打坏同包非空泛伴生（它用未过滤的 `found[rel]`）也不打坏判据 #14（其红来自非历史文档），且今天数字不变。

### m7 · §14 判据的账面数字与落地不符（#20 / #3；#1 的那一半已被驳回）
**id**：IDG-11（修复镜头驳回其 #1 部分，见 §8） · **文件**：`docs/reviews/line2-plan.md:616` 等
#20 见 M26；#3 见 m5。**#1 不成立**：判据自己的命令（`golangci-lint linters | awk … | grep -c`）实测就是 22，与判据期望一致；23 只出现在 `review-lint.json` 的 `Report.Linters`（含 typecheck 伪 linter）—— 两种测法之差，不应改判据期望值。影响：外审逐条机械跑这张表会得到 2 个假异常，降低对整张表的信任度。修法：只改 #20 与 #3。

### m8 · `hd.acks` 成了只写字段（锁内永久自增、全仓零读取），且注释所称的「R13 status」核实为早已过期
**id**：PC-4 · **文件**：`internal/broker/home_delivery.go:141`（声明）、`:322`（自增）、`:339`（doc）
```
$ grep -rn "homeDeliveryStats|hd.acks" --include=*.go .
home_delivery.go:322  hd.acks++
home_delivery.go:343  func (b *Broker) homeDeliveryStats() (pushes uint64)
home_delivery_test.go:443,451,555   ← 仅 3 处，全在测试
$ grep -rn "Pushes" --include=*.go internal/ cmd/
proxy_reconnect_test.go:140   ← 只是个测试函数名
```
`internal/adminsock/` 与 `clusterstatus.go` 里没有任何 push/ack 计数字段 ⇒ status 面**连 pushes 都不读**，`:339` 的「(tests + R13 status)」对两者都为假。场景：diff 自己给 `transferTracker.remove` 写的理由是「a returned value nobody reads reads like a handle somebody is holding」—— 按同一把尺，一个在互斥锁内永久自增却无途径可读的计数器读起来就像一个已被暴露的指标；下一个想观测 home-delivery ack 收敛的人会以为它已接出去。修法：二选一别停在中间态 ——(a) 一并删掉 `acks` 字段与 `:322` 的自增；(b) 保留双返回值并真的接进 R13 status/metrics。无论哪条，把 `:339` 的 doc 订正为「pushes only; tests only」。

### m9 · `js_store.go` 新增的 stat 失败分支自带不了补救办法，而三个调用点会给它套上只对「非空未 ack」成立的 `--reset-js` 建议
**id**：PC-6 · **文件**：`internal/natsconf/js_store.go:141`；wrapper 在 `cmd/tether/cluster_natsconf.go:258` / `cmd/tether/cluster_offline.go:316` / `cmd/tether/cluster_add_drive.go:810`
三分支完整、无 nil 解引用（`serr != nil` 在 `!fi.IsDir()` 之前，故 fi 必非 nil），且**没有正常流程会新开始报错**（joiner 路径先过 `JSStoreHasData` 双重前置；其余路径 stat 只要父目录 +x 而后续 `os.Rename` 要父目录可写 —— 能 rename 必能 stat）⇒ 判 MINOR 而非 CRITICAL。缺口：同文件 `:160` 的 rename-EACCES 分支带了 broker-ops #6 的 `chown -R tether:tether` 提示，新 stat 分支没有；而三处 wrapper 无条件在 `%w` 后拼 `--reset-js`/`--reset-former-js`。场景：EACCES/ESTALE/ELOOP 触发时运维看到「加上 --reset-js 重跑」，照做后一模一样地再失败一次 —— 本仓明令禁止的「hint 指错补救办法」。修法：新分支里插一条 `errors.Is(serr, os.ErrPermission)` case 带上 chown 话术（插在 `os.IsNotExist` 之后、`serr != nil` 之前，ENOENT 不是 ErrPermission，不会误吞）；更彻底的做法是给「需要 ack」的失败定义 `ErrJSStoreNeedsAck` sentinel，让三处 wrapper 只在 `errors.Is` 命中时才追加建议。

### m10 · ssproxy 的新类型断言放在 `s.ln = ln` **之后**，失败路径留下半构造的 Server
**id**：PC-7 · **文件**：`internal/agent/ssproxy/server.go:264`
`s.ln = ln` → 断言 → 失败时 `ln.Close()` + return，而 `s.localPort` / `s.allConns` / `s.keyConns` 在 `:270-272` 才初始化 ⇒ `s.ln` 持着一个已 Close 的 listener 而两个 map 仍是 nil。今天不可达（`net.Listen("tcp", …)` 必返 `*net.TCPAddr`）。场景：若该防御分支哪天真被触发（换 Unix socket / 注入自定义 Listener），后续任何往两个 map 写入的方法都会 panic on nil map write，而不是干净失败 —— 一个 fail-closed 守卫反而制造更难查的崩溃。修法：把断言提到赋值之前（`s.ln` 保持 nil、`s.closed` 仍 false，Start 可干净重试）。

### m11 · `scrubAuditBody` 的 fail-closed 把 nil 静默写进 bundle，既不置 `Partial` 也不追加 `Errors`
**id**：PC-8 · **文件**：`internal/broker/incident.go:88`
方向是对的（丢 body 优于导出未脱敏），但同一函数 `:80-84` 对 audit 拉取失败已有 `partial = true; errs = append(...)` 机制正用于别的缺失情形 —— 新代码没接上身边现成的痕迹机制。今天不可达（`scrubAny` 对 `map[string]any` 输入的第一个 case 必返 map）。场景：取证读者看到一条 Body 为空、`Partial:false` 的 audit 记录 ——「完整」的假象，本仓反复标记的 false-green 方向。修法（`partial`/`errs` 就在同作用域，inline 可行）：
```go
scrubbed, ok := scrubAny(entries[i].Body).(map[string]any)
if !ok {
	partial = true
	errs = append(errs, "audit for "+sid+": body dropped (scrubber returned a non-map; failing closed rather than exporting it unscrubbed)")
}
entries[i].Body = scrubbed // nil on failure
```

### m12 · 健康度单调门的文件头声称「EVERY reachable combination」「no input at all can produce a green exit」，实测 sweep 只变 4 个维度中的 4/18
**id**：F5[deletions]（**拖延审计员判 MAJOR**） · **文件**：`internal/broker/cluster_health_monotonicity_test.go:26`
`computeHealth`（`clusterstatus.go:616-708`）读 18 个输入维度；sweep 只变 forceSingle / leaderID / voters 0..7 / streamShort（64 个输入），topoDesired 恒 0、节点行恒为 `{phaseVoter, ReachSource:"self", Reachable:true, TopoReported:true}`。三条真实降级支逐条短路后**两个测试全绿**：`n.ReachSource=="nats-health" && !n.Reachable` → ok；`ClassifyTopo` 的 `st.Degrades()` → ok；`n.DiskFreePct>0 && <10` → ok。对照（证明它在自己声明的四维上确有咬合力）：短路 `proj.FaultTolerance == 0` → 两个测试都红（N=1/N=2 waypoint + 乱序自检同时报警）；短路 stream-below-target → 红并逐个报「exit 0 with a stream below its target replica count」。另：c7 原承诺里的 AlertReconciler 那一支被静默丢掉（坦白节只写了 StatusReport 填充）—— 该丢弃客观上无害（`healthExitCode(rep.Health)` 与 alert 无关），但坦白不完整。场景：把 ClassifyTopo 降级支（C3 验收「任一 broker 未完成 topology apply 时不显 HEALTHY-HA」）、低磁盘降级、不可达 voter 降级中任意一条删掉，这个自称穷举的门全绿；读到「no input at all」的人会据此认为无需再补维度 —— 注释在这里起了负作用。修法：把文件头改成实说的四维，或把 sweep 真的扩到 topoDesired∈{0,7} × TopoReported∈{T,F} × ReachSource×Reachable × DiskFreePct∈{0,5} × Ports∈{0/0,95/100}（每维两三个取值仍是毫秒级）；把 AlertReconciler 明确写进 WHAT THIS DOES NOT COVER 并说明理由。

### m13 · `d8_alerts.go → alert_gate.go` 改名漏了 3 处活引用，其中一处是 drill 运行时打给运维看的 assert 标签
**id**：F6[deletions] · **文件**：`test/simcluster/drills/42-rejoin-returning.sh:164`（运行时文案）、`:160`、`test/simcluster/drills/90-alerts-lifecycle.sh:13`（注释）
`ls cmd/tether/d8_alerts.go` → 不存在。`docs/reviews/**` 下 15 处旧名不改是**对的**（冻结记录），`deploy-tier-gotchas.md` 也改了；漏的是 `test/simcluster/drills/`（活脚本）。场景：跑 drill 42 的人拿到一条指向不存在文件的断言说明，去 grep `d8_alerts.go:71` 找不到那个 gate —— 正是 CLAUDE.md §5b 说「文件名扛不住改名」要解决的问题，只是落在 shell 里。修法：三处改成 `alert_gate.go`；防复发可加一条门扫 `test/simcluster/**.sh` 与非 reviews 文档里形如 `<path>.go:<line>` 的引用并断言文件存在（可与 C3/M23 的路径存在性扫描合成同一条实现）。

### m14 · `gates` 目标自称「re-runs every mechanical guard in one shot」但不含 `vet-tags`
**id**：F7[deletions] · **文件**：`Makefile:209`
`gates:` = `go test ./test/architecture/... ./test/determinism/... ./cmd/tether/ ./internal/auth/` + `$(MAKE) lint`，无 vet-tags（`test: vet-tags` 在 `:37`）。实测二者不可互替：往 test/d6、test/d8 注入编译错误 → `go vet ./...` rc=0、`go build ./...` rc=0（看不见），`make vet-tags` rc=1 并精确点名两文件；而 `TestBuildTagsAreReconciled` 在两种情况下都绿（它只读 `//go:build` 行，不做编译）。场景：改了 `ALL_TEST_TAGS` 的人最自然只跑 `gates`，会得到「列表已核对」的绿而没有「文件能编译」的证据 —— 与 `build_tags_test.go` 头里自陈的「六个名字里有六个不存在」是同一类盲区。影响有限（`make test` 是提交前硬闸且带 vet-tags）。修法：`gates: vet-tags`（热跑约 +2s），并把 gates 注释的清单补上这一项。

### m15 · 判据 #3 / #24 被主进程自己新写的**注释散文**触发假红
**id**：PF-9 · **文件**：`internal/broker/loopset.go:200`、`internal/broker/reconcile_registry.go:449`
```
$ grep -n 'nolint:unused' internal/broker/loopset.go
200:// (There used to be a //nolint:unused here. nolintlint's allow-unused:false found it dangling: the
```
悬空指令确已删除（`git diff` 证实），p13 那条也真删（grep rc=1）；散文命中 grep → 判据 #24 按字面失败。判据 #3 同理把 2 行叙述算成指令（16 vs 真实 14）。场景：27 条判据里有 2 条给出错误结论；一条要靠人工减掉散文才能读的机械判据不再是机械判据。这是 plan 自己在「实施进度」里记录过三次的同一形状（说明文字重新制造检查对象）的第四、第五次发作。修法：grep 改成只匹配真指令，例如 `grep -rnE '^[^/]*//nolint:' --include=*.go .` —— 修复镜头核实 `[^/]*` 不能跨过 `/`，故以 `// (There used to be a //nolint:` 开头的散文行匹配不上，而独立成行的真指令与行尾挂在代码后的指令都能匹配。并在 plan §7/§8 补记那 4 条 gocritic。

### m16 · `processNamedFuncRe`（本次最易错的一块手搓正则）没有合成样本的非空泛伴生测试
**id**：PF-11（其 published 计数部分见 M6） · **文件**：`test/determinism/test_naming_test.go:150`
`grep -rn 'isProcessNamedFunc|processNamedFuncRe'` 只有门自己在用，没有任何测试喂样本；对照同文件 `:290 TestProcessNamePatternRecognisesTheRealShapes`（13 mustMatch + 18 mustNotMatch，全针对**文件名**）。场景：M8 那个漏字母 A 的洞正是这种缺失的直接产物 —— 没有 mustMatch 样本，就没有任何东西会问「`TestA9…` 应该被抓到吗」；正则若某天被改窄，门会继续绿，因为它的成功状态就是「没有违规」。修法：加 `TestProcessNamedFuncPatternRecognisesTheRealShapes`（mustMatch / mustNotMatch 见 §7）。修复镜头提醒：样本写在**代码字符串**里，不受 promised_guard 的注释采集影响，可以放心写全。

### m17 · X10 的「重开需要的新证据」列字面为「无」，且其括号内的支撑机制实测为假
**id**：PF-13（**拖延审计员判 MAJOR**，列为「伪装的 REJECT-FOREVER」） · **文件**：`docs/reviews/line2-plan.md:528`
原文：`| X10 | 529 个测试函数改名（作为闸门前置条件） | …冻结即止血… | 无（账本机制保证只减不增，存量会自然消解） |`。其余 9 条都给了可判定事件（X1「出现第二个需要历史豁免的文件」、X8「一次因缺 `t.Helper()` 误判失败位置的真实排查」等）。支撑机制实测为假：见 M6（加一行账本即静默豁免，436→437 全绿）。场景：判据 #26 只 grep `REJECT-FOREVER` 的条数（22 ≥ 10 通过），检查不到这一栏是空的；§0 自定「每一条都必须写明未来若要重开需要什么新证据」在此失效。修法：写成可判定事件（如「某次真实排查因为某个 `TestX<N>…` 名字无法 grep 而走错方向」，与 X8 同形），并删掉那句已被证伪的机制断言 —— 或在补上 M6 的 published 计数后让它成真。

### m18 · promised_guard 的孤儿反向断言把「删掉一条假承诺注释」变成红构建，并逼出三处「此处故意不写出那个标识符」的自我封口注释
**id**：PI-12（修复镜头驳回其主 fix，见 §8） · **文件**：`test/determinism/promised_guard_test.go:191`；自我封口在 `:42-45`、`:187-190`、`test_naming_test.go:155-156`
实测：把 `internal/brokermetrics/metrics_test.go:61` 的 `// TestReadyzBands (B5 plan §F.4):` 改写成 `// origin: B5 plan §F.4 —`（该测试真名是 `TestEndpointBands`，旧名是假承诺，删掉它**正是该门想要的结果**）→ `1 legacyMissingGuards entr(y/ies) are no longer named by ANY comment: TestReadyzBands` 红。**范围镜头的订正**：条目变孤儿时门的设计意图正是「去账本删掉那一行」，失败文案也这么说 —— 这不是「让人删注释才能变绿」，而是「删注释后必须同步排空账本」，方向与本仓尺子一致；但**梯度确实反了**（变红后最省力的绿化动作是 `git checkout` 把假承诺放回去）。真正有价值的那半是第二点：门迫使本仓写出三处信息量更低的注释，而这三段注释的读者恰恰需要知道是谁。修法：**只取原 fix 的后半** —— 把 stale 分支的失败文案写清是哪个文件哪一行的账本条目该删（现在只说 remove those lines，没说在哪个文件）。**排除原 fix 的前半**（`// no-promise: TestX` 转义标记）：按它自己的限定「该标记只用于已在账本里/已删除的名字」，一条命名不存在且不在账本里的 Test 可以被「已删除」这半句合法化，等于任何人加一个标记就能写假承诺 —— 重开了这道门存在的唯一理由。

### m19 · CLAUDE.md 的并发锁警告把因果写反了（漏掉「不是排队而是直接失败」）
**id**：D10 · **文件**：`CLAUDE.md:128`
`grep -n '全局锁' CLAUDE.md Makefile` → CLAUDE.md 只写「而 golangci-lint 有全局锁，**不得与另一个 lint 并行**」；`Makefile:205-207` 写对了：「golangci-lint takes a global lock and a second same-argument invocation **exits rc=3 rather than waiting**」。本轮审查中真实撞上：`Error: parallel golangci-lint is running` + 立即非零退出、没有等待（一条 lane 因此损失一次运行）。场景：熟悉锁语义的读者会推断「有锁 = 会自动串行化 = 并行是安全的」，正好得出相反结论；本轮的审查 workflow 正是靠这条约束才把 lint 收敛到单 agent。修法：补齐后半句并把作用域从 `make gates` 提到「任何 lint 调用（含 `make lint` × 2）」。

### m20 · 「本仓 29–33% 的行是注释」缺一个作用域词
**id**：D11 · **文件**：`CLAUDE.md:101`，同句逐字出现在 `.golangci.yml` 设计约束 1
独立测量：`cmd internal test` 全仓行首 `//` 占 **21.1%**（剔空行 22.6%、含行尾 `//` 上界 24.1%）；仅生产非测试文件 **29.7%**（剔空行 31.6%）；测试树单独 17.0%。场景：这个数字是「不启用按物理行计数的 linter」这条纪律的全部论据，一个能被 `wc` 在 30 秒内否掉的比例会让下一个人怀疑整条论证 —— 而论证是对的，被收税的正是那 29.7%。修法：两处都改成「本仓**生产代码**约 30% 的行是注释（含测试的全仓约 21%）」，并把口径（行首 `//` 计数）写在旁边。

### m21 · `batch-a-roadmap.md` 移动后仍有 3 处带路径的旧引用；而条款字面「把所有引用一并重指」与实际执行（只重指活文档）不一致
**id**：D12 · **文件**：`docs/reviews/batch-a-plan.md:4`、`docs/reviews/batch-c-review.md:406` 与 `:1945`；条款在 `CLAUDE.md:60`
`ls docs/batch-a-roadmap.md` 已不存在（`git status` 显示 `R docs/batch-a-roadmap.md -> docs/reviews/batch-a-roadmap.md`）。对照组：`d8_alerts.go` 改名在 `docs/reviews/` 下留了 15 处旧名，未改 —— **那才是合理的规则**（冻结记录不追溯改），但不是条款写的那条。场景：条款字面要求与实际执行不一致，下一个人要么去批量改 391 份冻结报告（把历史记录改成不再是当时看到的样子），要么违反条款 —— 两种都让条款失去约束力。修法：条款改成「把**活文档**（`docs/` 顶层 + `CLAUDE.md` + `test/simcluster/README.md`）里的引用重指；`docs/reviews/` 下的冻结记录不追溯改，与『存量的轮次标签不追溯改』同一原则」。

### m22 · CLAUDE.md 闸门表漏掉本轮新造的两道门，其中一道是全仓唯一的 TLS 安全闸门
**id**：D14（与 M23 同一张表，但缺陷不同） · **文件**：`CLAUDE.md:107-120`
`grep -in 'TLS|InsecureSkipVerify|enum|default:' CLAUDE.md` 在闸门表范围内**零命中**。`test/architecture/tls_verify_pairing_test.go`（`TestInsecureSkipVerifyIsAlwaysPairedWithChainVerification`）与 `test/determinism/enum_switch_default_test.go`（`TestNoDefaultOnRepoEnumSwitch`）都真实存在且在 gates 覆盖范围内（我方 `go test ./test/architecture/... ./test/determinism/...` 两包 ok）；后者只被「确定性 lint | `test/determinism/`」这行的目录指针间接盖住，而那行的「管什么」列列的是另外四件事。场景：下一个人给 `InsecureSkipVerify` 加第三个站点、或给枚举 switch 加 `default:` 被拦下时，会在每会话加载的文件里找不到这道门的任何解释，只能靠读失败文案自行猜规矩 —— 而 TLS 那条是安全不变量，最需要常驻可见。修法：表里加两行（TLS 验证配对 / 枚举 switch 禁 default），并把「确定性 lint」行的「管什么」补上这一项。

### m23 · `deploy-tier-gotchas.md` 的 `alerts.go:144-163` 行号已陈旧（**范围镜头判定越界：存量缺陷，属别的增量**）
**id**：D15 · **文件**：`docs/deploy-tier-gotchas.md:186`
改名本身是对的（`git show HEAD:cmd/tether/d8_alerts.go | diff - cmd/tether/alert_gate.go` 无差异，`:71-98` 确实覆盖 `gateDestructive`+`gateBlockMessage` 开头）。但同一括号里的 `alerts.go:144-163` 陈旧：`EvalDestructiveGate` 在 `internal/proto/alerts.go:201`，`:140-166` 现在是 `AccountNkPub`/`AlertView`/`AlertLsResp`；全仓只有一个 `alerts.go`。**范围镜头**：`git log -1 -S "alerts.go:144" -- docs/deploy-tier-gotchas.md` → `02913d9`（早于本次），本次在那一行做的唯一事情是改名扫描，既没引入也没使它更错 ⇒ 属「批评本来就存在、与本次改动无关的问题」，**不应计入本轮验收**；正确归宿是独立订正或由主进程明确判为顺手采纳而不占 line-2 的账。修法（若顺手采纳）：改成 `internal/proto/alerts.go:201-222`（带包路径，`cmd/tether` 下另有 `alert_gate.go`/`alert.go`，写全更抗混淆）。

### m24 · `gates` 的包清单是手抄常量、无任何反向对账；3 个门在名单外（其中一个还被 CLAUDE.md 表列为闸门）
**id**：GI-7 · **文件**：`Makefile:209`（清单）、`:203`（自称「re-runs every mechanical guard in one shot」）
逐条比对 CLAUDE.md 的 10 行表：lint / 结构棘轮 / 分层 / 命名冻结 / 确定性 lint / wire 错误码 / ACL / CLI golden 都在；**「泄漏门 `test/concurrency/helpers_test.go`」不在**；build-tag 门只跑自检不跑 `vet-tags`（见 m14）。本轮新增的 `internal/broker/cluster_health_monotonicity_test.go` 与 `test/d8/forward_churn_leak_test.go` 既不在 gates 也不在那张表里。`grep -rn '"gates"' --include=*.go .` 无命中 —— 没有任何测试对账这份清单，而同一次改动专门给 `ALL_TEST_TAGS` 配了双向对账门，说明这个腐化模式团队是认识的。场景：加一个新守卫包或改一个不在名单里的门，`make gates` 静默少跑、全绿 —— 与 `build_tags_test.go` 头部那句话逐字同形，而且不是「将来会有」而是「此刻正开着」。修法（**修复镜头推荐取 (b)**）：(a) 给 gates 配双向对账门 —— 修复镜头反对：「扫全仓找出含 golden/账本/反向断言形状的守卫包」是个模糊分类器，其自身正确性无法机械验证，会做出一个既漏又误报的门；(b) 显式补进清单（+ `gates: vet-tags`），并把 CLAUDE.md 那句「改了闸门自身时跑 make gates 做一次集中自检」改成「跑 make gates 快检，收尾仍以 make test + make lint 为准」—— 实测 gates 的 go-test 半边是 `make test` 的**真子集**（13.9s vs 冷跑 328s），唯一增量价值是速度而非覆盖。

### m25 · `itoaDeterminism` 是同包 `itoa` 的纯冗余复制；合掉 4 份 helper 的同一次改动净新增 3 份重复，而 dupl 对 `^test/` 整树豁免 —— 永远不会有门报出来
**id**：GI-8 · **文件**：`test/determinism/enum_switch_default_test.go:205`（同包已有 `promised_guard_test.go:257 func itoa`）
实测：把唯一调用点换成 `itoa` 并删掉 `itoaDeterminism` → `go test ./test/determinism/ -run TestNoDefaultOnRepoEnumSwitch` → ok。本轮 helper 账：合掉 4 份 `goListDeps`+`moduleRoot`（B2）；新增 `test/architecture/build_tags_test.go:81 repoRoot`、`:256 itoa`（手搓十进制循环，非 `strconv.Itoa`）、`itoaDeterminism`。仓内 `itoa` 家族现共 11 份、`repoRoot` 家族 9 份，其中 `test/determinism` 一个包里 `repoRoot`(lint_skeleton_test.go:22) 与 `repoRootForGuards`(promised_guard_test.go:244) 并存。`.golangci.yml:242-248` 把 dupl/maintidx/unparam 对 `(_test\.go|^test/)` 整树豁免。场景：读者在同一个包里看到 `itoa` 和 `itoaDeterminism` 会以为语义不同（实测完全相同），下一个门作者会继续造第 12 份；更值得记的结构性事实是**测试树的 helper 重复在本仓没有任何门管**，所以「合表消重」这类收益只能靠人记得。修法：删 `itoaDeterminism` 改调同包 `itoa`；`test/architecture` 那份改用 `strconv.Itoa`（`cmd/tether/error_code_coverage_test.go:623` 已是这个写法）；`repoRoot`/`repoRootForGuards` 二合一时顺手删掉 `test_naming_test.go:364` 的死行（见 m27）。

### m26 · `test/architecture` 的 package doc 发布的分工规则与实际不符（**可复现性镜头 refuted，未达删除门槛，保留供复核**）
**id**：GI-10 · **文件**：`test/architecture/build_tags_test.go:1`
GI-10 称 package doc 写「架构不变量放这里、determinism/SSOT 放那边」而它自己就是一份 Makefile↔源码的 SSOT 对账。**可复现性镜头驳回**：该 package doc 第一段逐字把「**which build tags exist**」列为架构不变量，所以 build_tags 门恰好在它自己声明的范围内；plan §15 仲裁也**同时**写下了两条判据（budgets/layering/nolint → architecture；G3.6 与枚举门 → determinism，理由是「与既有 `TestNoStrayVersionLiteral` 是同一件事的两半」），`docs_wire_version` 的落位与之一致。**唯一存活的事实**：`tls_verify_pairing_test.go` 全文没写放置论证（它守的是 `internal/cluster/transport.go` 与 `internal/tunnel/tls.go` 的构造形态，属结构面）。后果有限（两个目录都在 `make test` 与 `gates` 里）。修法：给 `tls_verify_pairing_test.go` 补一句放置论证；若主进程认可镜头的驳回，则 package doc 无需改。

### m27 · 三处已发布数字/死代码过时，落在本仓「已发布数字必须可复现」这条纪律上
**id**：GI-12 · **文件**：`test/architecture/build_tags_test.go:239`、`test/determinism/test_naming_test.go:364`、`test/determinism/promised_guard_test.go:16`
① `:239` 的失败文案写「the repo has 8」，实测自定义 tag 只剩 **7**（本次删 `c7_integration` 造成；`ALL_TEST_TAGS` 也是 7 个）—— 读者若照文案去找第 8 个 tag 会白找一轮，而非空泛检查只断言 `len(inTree) == 0`。② `test_naming_test.go:364` 的 `var _ = os.Stat` 是死行（`os` 在 `:185` 的 `os.ReadFile` 已被真实使用）。③ `promised_guard_test.go:16` 写「There are 34 of them」，实测账本 **33** 条（Y3 删了一条）。修法：8→7、34→33、删死行（连同上一行那条已失真的「defined in promised_guard_test.go」注释一起处理）。**排除项（修复镜头）**：不要给 tag 数加 `len(inTree) < 7` 下限 —— 非空泛测试已有点名断言（d5_integration 必须命中、7 个文件），而 tag 数是本仓**刻意在减少**的量，加下限等于每次整合套件都要手工下调一次数字，摩擦换不到检测力。

---

## 4. 完成判据核验（plan §14 · 27 条逐条）

由**完成判据机械核验员**实跑，多条 lane 独立复现。审计员自报 **18 满足 / 8 不满足 / 1 部分**；下表逐条列出时其 unmet 列表里出现 **9** 条判据行（#3 #4 #11 #13 #14 #16 #20 #22 #24），算术上与 18/8/1 差 1（可能把 #24「实质完成、仅 grep 假红」计入满足）—— **主进程复核时以逐条为准，不以计数为准**。

| # | 判据 | 结果 | 实测 |
|---|---|---|---|
| 1 | enabled linter = 22（无配置时 5） | ✅ 满足 | `golangci-lint linters \| awk … \| grep -c` = 22（带与不带 `-c` 都是 22）；移走配置 = 5。**注**：`review-lint.json` 的 `Report.Linters` 是 23（含 typecheck 伪 linter），是另一种测法，判据本身无需改（见 m7） |
| 2 | lint 0 issues | ✅ 满足 | `review-lint.json` → `Issues: []` |
| 3 | `//nolint` 计数 ≤ 基线 10 + plan 明确列出 | ❌ **不满足** | grep 得 16（真指令 14），预算 = 10 + plan §7 点名的 2 = 12；超出的 4 条 gocritic exitAfterDefer 在 plan 全文无记录。另有 2 行叙述被 grep 误计（m5 / m15） |
| 4 | `mv .golangci.yml /tmp/ && make lint` → rc≠0 | ❌ **不满足** | rc=**0**、输出 `0 issues.`、enabled 22→5、gofmt 干净 ⇒ 整条 target 绿（**C1**） |
| 5 | （文本见 plan §14） | ✅ 满足 | 实测 = 2 |
| 6 | 从 ALL_TEST_TAGS 删 `d5_integration` → 红 | ✅ 满足 | 点名「d5_integration (7 file(s), e.g. test/d5/fanout_test.go)」；反方向（树里新增未登记 tag）也红 |
| 7 | `test/architecture -v` 的 `=== RUN` ≥ 8 | ✅ 满足 | 13 |
| 8 | Broker +1 方法 → 红 | ✅ 满足 | `BUDGET EXCEEDED: type-methods internal/broker.Broker = 280, ledger says 279 (+1)` |
| 9 | 加 500 行纯注释 → 绿 | ✅ 满足 | 往 `internal/broker/disk.go` 与**计入维度**的 `cmd/tether/error_hints.go` 各加 500 行纯注释均绿；加 3 行真代码 → 红（1119/1116）。**注释税已消除** |
| 10 | 超预算树上跑 `-update` → 非零且拒写 | ✅ 满足 | rc=1、`REFUSES to widen 1 budget(s)`、golden 逐字节未变。**但对账本里还没有的新实体 rc=0 静默收录 → M4；且一次例行 `-update` 会删掉 13 行手写论证 → M3** |
| 11 | `-run TestLayering` 在注入违规后 → 红 | ❌ **不满足** | 正则只命中 `TestLayeringRulesAreWellFormed`，注入违规后 rc=0；真名 `TestPackageLayering` 才红。且判据点名的 `internal/proto → internal/broker` 探针是靠 import cycle 让 `go list` 失败而红，验证不到规则内容（**M5 / M9**） |
| 12 | 四份 `test/d{5,6,7,8}/regression_test.go` = 0 | ✅ 满足 | 全部删除；并集经三条 lane 独立逐子句核对**零丢失**（10 个原 Test 函数的全部子句都在新表中） |
| 13 | 往 usage.md 写 `tether.v1.*` 后 `-run TestDocsWireVersion` → 红 | ❌ **不满足** | 只命中 `…ScannerIsNonVacuous`，rc=0；真名 `TestDocsUseCurrentWireVersion` 才红（**M5**） |
| 14 | `ProtoVersion=3` 后同上 → 红（证明跟 SSOT） | ❌ **不满足** | 同一命名缺陷，判据命令 rc=0。**尺子本身是对的**：真名下 3 个文件同时红 |
| 15 | architecture.md 计数双向（70 与 68 都红） | ✅ 满足 | 两次都红。口径分歧：D7 认为它与 #13/#14 共用空转命令，逐条实跑不支持（见 M5 脚注）。另有精度问题 → m6 |
| 16 | 加 `default:` 后 `-run TestEnumSwitchNoDefault` → 红 | ❌ **不满足** | 真名一个都不匹配 → `ok … [no tests to run]` rc=0，**变异前后两次都绿**；真名 `TestNoDefaultOnRepoEnumSwitch` 才红。这是 plan §1 订正 2 整条价值论证的落点（**M5**） |
| 17 | G3.1 反向对账三步（注入死键 / 加白名单 / 删表键） | ✅ 满足 | 三步全过，含反向断言「is not a key of any classification table」。首次运行即抓到真死键 `already_voter`（独立复现） |
| 18 | 新增 process-named 测试函数 → 红 | ✅ 满足 | `TestB99…` 点名文件与函数。**但加一行账本即静默豁免 → M6** |
| 19 | `cmd/tether/d8_alerts.go` 已改名 | ✅ 满足 | 已改为 `alert_gate.go`（纯改名，字节相同）。**3 处 shell 活引用漏改 → m13** |
| 20 | `grep -c c7_integration Makefile` ≥1 且 `test/c7` 无 `t.Skip` | ❌ **不满足** | = 0 且 `test/c7` 已删。实现选了更强路径，判据未订正（**M26**） |
| 21 | `phase/<N>` 与 `Opus 4.8` 计数 = 0 | ✅ 满足 | 两项在 CLAUDE.md 与 docs/architecture.md 均 0 |
| 22 | `archive/` 存在 且 `INDEX.md` 存在 且 `batch-a-roadmap.md` 不在 docs/ 顶层 | ❌ **不满足** | 本机 rc=0，但 `archive/` 是空目录、`git ls-files` = 0 ⇒ **commit 后 fresh clone 即红**（**M24**） |
| 23 | `assertNoGoroutineLeak` 调用点 ≥2 | ✅ 满足 | 6（定义 + d8 调用点）；`test/d8/integration_test.go:32` 确认挂进 `TestD8Matrix`；`-tags d8_integration -race` 实跑 PASS 10.1s，两半变异（泄漏 goroutine / 泄漏 fd）都能红 |
| 24 | 两处悬空 `//nolint` 均不存在 | ❌ **不满足**（实质完成） | 真指令确已删；`loopset.go:200` 的**叙述注释**命中 grep（**m15**） |
| 25 | `make test` / `make lint` / `make e2e-parallel` 全绿 | ⚠️ **部分：e2e-parallel 由主进程负责** | `make test` 专家实跑 **rc=0**（FAIL 行数 0）；gofmt 全仓干净；`make lint` 以 `review-lint.json`（0 issues）为据、未重跑（全局锁）；**`make e2e-parallel` 无第三方证据** |
| 26 | §11 REJECT-FOREVER 条数 ≥10 且每条有重开证据 | ✅ 满足（条数） | 22 条。**但 X10 的重开条件列字面为「无」→ m17；X2 的重开条件已被自己满足 → M21。判据只 grep 条数，检查不到这一栏的质量** |
| 27 | 无「延后 / 下个增量」类措辞 | ✅ 满足 | 措辞扫描全清；命中的 5 处全是「引述前人逃避以便当场关闭它」或豁免自带的复查指针 |

### 判据表本身的两个结构性问题

1. **「空包也绿」在 `-run` 层复发四次**：判据 7 是专为堵这个通道设计的，而 #11/#13/#14/#16 自己踩了进去。`[no tests to run]` + PASS + rc=0 与「守卫通过」在退出码上完全不可分 —— §14 自定原则「一条判据如果在偷偷跳过这一项时仍然是绿的，它就不合格」在这四条上自指失效。
2. **§14 写在阶段 C 之前，凭据面小于交付面**：27 行覆盖不到 TLS 验证配对门、promised_guard 反向断言（Y3）、Y2 六个新码的正反对账、健康度时序单调（#20 只查 `t.Skip` 是否消失）、以及枚举门的**存在性**。可检验的后果：**唯一被静默丢掉的项（`nolint_directive_test.go`）和唯一被实测出漏洞的门（TLS 配对门）都落在没有判据行的那一区** —— 这不是巧合，是覆盖面缺口的直接表现。建议判据表随交付演进，否则「27 条全过」这句话的强度由表的年龄决定，而不是由实现决定。

---

## 5. 拖延与逃避审计

> 本节是两位审计员的独立产出，性质与 §1–§3 的 finding 不同：它们审的是**流程与自述**，不是代码。verdict 已在 §0 原样呈现（两者均 **NOT-DONE**）。

### 5.1 拖延与逃避猎手 · evasions（原样列出）

1. **结构性逃避而非措辞逃避**：本次没有新增任何「延后 / 推迟 / 下个增量」字样（判据 27 通过），逃避全部以「**声称完成**」的形式出现 —— 这比措辞逃避更难发现，因为 grep 抓不到。
2. `plan:688` `~~E1–E6 G5~~ ✅ **已完成**`：标签覆盖 E1–E6，说明列只枚举 E1–E4 的内容（修订 2/3/4/5a/5b/5d + step 5b），E5/E6 实测各自一行未做 —— **用一个复合标签把两个零交付项裹进四个真交付项里**。
3. `plan:772`「线二 §2 归宿总表的 22 个 DO 项至此全部落地，无延后项」：至少五处为假（E5、E6、C1 的 §6 接线、C4 的守卫、E1 的 archive/ 与 cluster-ha 裁决）。这句话是全文最后一句，位置上承担「交外审」的通行证作用。
4. **X2 的 REJECT-FOREVER 论证里混入「闸门会被本流程自己打红」** —— 这是「现在不方便」，被用来支撑「以后永不做」。REJECT-FOREVER = 永不做，时机论证不能出现在这一栏。
5. **X10 的重开条件列写「无」**，并用一句实测为假的机制断言（「账本机制保证只减不增」）替代重开条件。判据 26 只 grep 条数，检查不到这一栏是空的。
6. **A3 把形态替换包装成严格升级**：「stronger statement」「EVERY reachable combination」「no input at all」。坦白节存在且诚实，但只覆盖了 StatusReport 填充这一轴，没说 sweep 只变 18 个维度中的 4 个；三条真实降级支可删而测试全绿。
7. **D2 用「以闸门正则为准并记录差异」把 plan 的 529 条目标重定义为「我的正则匹配到的」**，差额 93 归因于正则精度，而未检查正则是否覆盖本仓真实用过的批次字母（缺 A，线一第一批就是批次 A）。
8. `.golangci.yml:157`「test/architecture/nolint_directive_test.go covers that gap」—— 该文件不存在。配置向读者保证一个缺口已被别处补上，读者据此**停止怀疑**。
9. `plan:676`「豁免分三种…**都做了不会过宽的验证**」：dupl（行区间钉死）与 unparam 确实做了，noctx 的四站点规则是 `path:` 无 `text:`，同文件新增违规会被吸收 ——「都」不成立。
10. 进度表「未完成」节仍留着 `§12 线一欠账 Y2/Y3 + d8 泄漏门 + gosec 6 条复核 | 全部 | 未开始` 与 `阶段 C 审查 workflow | 未开始` 未划掉，被 15 行后的小节推翻。属被取代而非虚假，但使进度表无法自上而下阅读。

### 5.2 拖延与逃避猎手 · unmet 中未被 §1–§3 覆盖的两条

| 项 | 期望 | 实测 | 定级 |
|---|---|---|---|
| **X3 与 X7 构成循环** | 两条 REJECT-FOREVER 的重开条件不应互相封死；配置的设计约束应内部一致 | X3 重开条件 = 「maintidx 上游提供 `ignore-comments`，**或换用注释免疫的量**」，而 plan §1 订正 1 与配置都把那个量点名为 **gocyclo**；X7 = 「S3 §4 的 13 个被拒 linter」REJECT-FOREVER，**含 gocyclo**。`.golangci.yml` 第 12 行写「funlen / gocyclo / gocognit / cyclop / lll are deliberately NOT enabled」，第 19 行写「gocyclo and funlen{ignore-comments:true} are **the comment-immune ones**」—— 同一段内同一个 linter 被同时否决与推荐（这条同时是 M13 修法受阻的根因，也是 §6 B10） | MINOR |
| **X5 的重开阈值已被本次改动推到 92%** | X5（CLAUDE.md 精简）的重开条件是「CLAUDE.md 重新膨胀到 10,000+ 字符」 | 本次把它从 **6325 → 9178** 字符（+45%）。附带订正：X5 引用的 S3「19%」是拿**字符除字节**算出来的（`git show 84bf030:CLAUDE.md \| wc -m -c` = 7686 字符 / 13029 字节），按一致口径应为 32%（字节 33%）—— 结论 REJECT-FOREVER 仍成立且比 plan 写的更强 | MINOR |

### 5.3 完成判据机械核验员 · evasions（原样列出，去重后）

1. **「空包也绿」在 `-run` 层原样复发四次**（详见 §4 结构性问题 1）。
2. **判据 4 声称堵住的 fail-open 通道现在仍完全敞开** —— 仓内没有任何机械断言「配置在生效」（判据 1 的 22 这个数没有任何测试钉住），所以「lint 0 issues」这句话至今无法区分「139 条整改完成」与「配置从未落盘 / 已被删除」。
3. **判据 22 被一个 git 装不下的空目录满足** —— 产物的生命周期短于判据的生命周期。
4. **判据 20 / 24 的凭据是文本而非行为，且两条都因文本而失真** —— 同一形状（说明文字重新制造检查对象）plan 自己承认发作过三次，这里是第四、第五次，说明这不是偶发，而是「用 grep 当验收」这个方法本身的系统性副作用。
5. **活文档声称的覆盖大于实际覆盖**：`.golangci.yml:157` 指向不存在的文件；CLAUDE.md 闸门表指向不存在的 `test/architecture/budgets`。两处都能被 `test -e` 一秒判伪，而 `promised_guard_test.go` 只扫注释里的 `Test…` 标识符，抓不到「承诺一个文件名」这种形态。
6. **§14 的凭据面小于交付面**（详见 §4 结构性问题 2）。

### 5.4 完成判据机械核验员 · unmet 中未被 §1–§3 覆盖的一条

| 项 | 实测 | 定级 |
|---|---|---|
| `test/d8/forward_churn_leak_test.go` 头注释对「为何做成子测试」的机制陈述不准确 | 注释写「the e2e runner invokes this suite with `-run TestD8Matrix`, so a new top-level function here would compile, pass locally, and never once be executed by the gate」。实际 `test/e2e/all_phases_test.go:375` 是 `exec.Command("go","test","-race","-count=1","-tags","d8_integration","-timeout","300s","./test/d8/...")` —— **没有 `-run` 过滤**（`-run TestD8Matrix` 作用在外层 test/e2e 包上），所以 test/d8 里的顶层函数是会被执行的。接线本身正确、实跑 PASS。仅论证失真 —— 但按「注释是资产」的标准，一个会让下一个人得出错误结论的机制陈述值得订正 | MINOR |

---

## 6. 集体盲区（verifier 的 `missedByAll`）

> 三个 verifier 镜头各自在「8 条 lane 全都没看的地方」找到的东西。**性质与 finding 不同**：它们不是对已有 finding 的评判，而是覆盖面缺口。B1 / B5 我方独立读码确认。

### B1 · `cmd/tether/node.go` 的 fleet 分类器没跟着 Y2 拆分更新 —— 而 `node upgrade` 正是 Y2 自己点名要救的那个命令（**最实质的一条**）
镜头 3；我方独立核实。`dispatchUpgrade`（`node.go:266-291`）把 `resp.Code` 交给 `brokerErrorMessage`，`node upgrade --all` 的循环（`node.go:215-236`）再用两张**按字符串 needle 匹配**的清单分流：
```go
// node.go:298  isTransientError → 跳过并继续车队
{"node_offline","node_not_found","agent_no_responders","agent_malformed_resp","deadline exceeded","context canceled"}
// node.go:318  isConfigError    → 中止整个车队
{"not_owner","url_not_allowed","sha256_invalid","proto_bump_requires_reinstall","actor_invalid","session_not_found_or_deleting"}
```
**四个新码一个都不在里面**（我方 grep 确认）。后果两个方向都错：
- `download_http_status`（打错 `--url`）既非 config 也非 transient → 落进「✗ failed」桶而循环**继续把已知坏 URL 扇出给车队里剩下的每一个节点** —— 而 `isConfigError` 存在的全部理由，逐字写在 `node.go:207-210`：「everything else (not_owner / url_not_allowed / proto_bump / sha256_invalid): config bug, **abort the rest of the fleet so we don't fan-out a known-bad request**」。
- `download_failed`（`error_hints.go:162` → `exitTransient` 75，真瞬态）不在 `isTransientError` 里 → 被记成硬失败而不是「skipped (transient)」。

改动把 exit class 修对了（→64/75）却在**同一个包里**留下第三份互相矛盾的分类，而 D1 反向门结构上看不到它（只对账 `brokerCodeExitClasses` 这类**表**，不看 ad-hoc substring 清单）。讽刺的是 `error_hints.go:341` 还专门为「下一个加 Y2 码的人」写了一条注释警告「A hint filed in the wrong map is never printed」—— 作者扫了 hint 映射表，但没扫 node.go。8 条 lane 全都在谈这四个码，没有一条打开 node.go。这正是 memory 里 contract-change-sweep 那条教训的第二次发作，只是载体从文案换成了另一份 Go 清单。
**注**：`cmd/tether/node_classify_test.go` 已存在（表驱动覆盖两个分类器）—— 即有一张表可以扩，没扩。

### B2 · `--json` 的 watch 帧在 fetch 失败时只吐分隔行、不吐任何 JSON（C2 的同一根因下第二个后果）
镜头 1。`cluster_wait.go` 的循环把帧装饰放在 fetch **之前**：先执行装饰，然后 `rep, err := fetchClusterStatusReport(...)`；err 非空时只往 stderr 打「watch: %v (retrying)」并 continue。所以 `--watch --json` 下一次瞬时 socket 错误会让 stdout 得到一行 `--- <ts> ---` 而**没有对应的 JSON 对象** —— 行读监控不仅 mis-parse（C2 已覆盖），还会在错误帧上完全丢失该帧的存在信号。而 `usage.md §9.14` 明写 `cluster status --json` 「不再吞 broker 错误」（`errors[]` / `partial:true`）。修 C2 时把装饰整段移到 `rep` 成功之后可一并关掉。

### B3 · 本轮新写的 TLS 账本，其 reason 字符串里自己嵌了一个会漂移的行号引用
镜头 2。`tls_verify_pairing_test.go:52` 的 reason 含「…the pinned path (**tls.go:121**) verifies via VerifyConnection.」，而门的反向断言只校验 map 的 **KEY**、从不校验 reason 里引用的行号。这与 m23 抓到的 `deploy-tier-gotchas.md` 的 `alerts.go:144-163` 是同一缺陷类别，只是这次是**本轮新造**的，而且出现在那份专门论证「行号漂移是 deliberate、就是要逼人重读」的文件里。M15 改 key 的 fix 也不覆盖它 —— 改 key 时应一并换成函数名。

### B4 · `allowedEnumDefaults` 预装了 M15 刚被发现的同一个缺陷，且它今天是空 map 所以从未被行使
镜头 2 + 镜头 3。`enum_switch_default_test.go:56` 的注释逐字写「keyed by `file:line` of the switch」，与 `unverifiedTLSFallbacks` 用的是**逐字相同**的 key 方案 —— 在 switch 上方插一行注释即同时触发「新违规」与「陈旧条目」两条红。**关键的 fix 交互**：M2 建议补齐 12 个枚举家族，实测会暴露 5 个必须处置的生产 switch，其中判「default 是对的」的那些**正好会首次填充这张表**，于是这条从未被触发的缺陷会随那条 fix 一起落地。没有一条 lane 把两者连起来。修 M15 时必须两处一起改，否则同一缺陷会在下一轮审查里被重新发现一次 —— 正是 CLAUDE.md §3 step 5b 用 `internal/tunnel` 四份 fence 测试举例的那个循环。

### B5 · 第三份未对账的 build-tag SSOT：`test/e2e/all_phases_test.go` 把 6 个 tag 硬编码成字符串字面量
镜头 1；我方独立核实（`grep -n '\-tags' test/e2e/all_phases_test.go` → `:290 d5_integration`、`:310 phasefluidity_integration`、`:329 d6_integration`、`:351 d7_integration`、`:375 d8_integration`、`:399 d9_integration`，全部在 `exec.Command("go","test",…,"-tags","<字面量>",…)` 里）。而 `TestBuildTagsAreReconciled` 只对账 **Makefile `ALL_TEST_TAGS` ↔ 树里的 `//go:build`** 两方。镜头 1 实测这个缺口：
```
把 all_phases_test.go 里的 d5_integration 改成 d5_integratoin
$ go test -count=1 -tags d5_integratoin ./test/d5/   → ok 0.008s（零测试、exit 0）
$ go test ./test/architecture/                        → 绿
$ go test ./test/determinism/                         → 绿
$ make vet-tags                                       → rc=0（它用 ALL_TEST_TAGS，看不到那些字面量）
```
即整个 d5 重型 clustered-JetStream 套件可以静默不编译不运行，而 `make e2e-parallel` 会把该 shard 报成通过 —— 并行 runner 的 coverage self-check（`test/e2e/parallel/main.go:104-127`）只核对「`-tags e2e_matrix` 下声明的每个顶层 `TestXMatrix` 都有一个 unit」，它看不见**子进程内部**跑了零个测试（子进程 exit 0）。这正是 `build_tags_test.go` 自己头部写下的那句话：「Go does not error on an unknown build tag, it simply builds nothing for it. **A typo does not fail — it disables.**」新闸门把对账做到了 Makefile，却停在了 tag 真正被 e2e 闸门消费的那一跳之前。多条 lane 长篇讨论过这个门（IDG 的三向变异、F7[deletions]、M22、m24），全部只在「Makefile ↔ 树」和「lint 不传 build-tags」两个维度上打转。修法与现有门同形：把 `all_phases_test.go` 里 `-tags` 后的字面量也纳入三方对账（约 15 行），或让它从一个共享常量取 tag。

### B6 · build-tag 提取器对 `&&` 约束的右侧 tag 完全不可见，且其注释把自己的取证前提说反了
镜头 3。`build_tags_test.go:161-165` 用 `constraint.Expr.Eval` 配一个恒返回 false 的 oracle 来枚举 tag。Go 的 `&&` 短路：对 `//go:build a && b`，`a` 求值 false 后 `b.Eval` **根本不会被调用**，oracle 永远不会被问到 `b`。今天全仓没有 `&&` 约束（`grep -rn '^//go:build .*&&'` 为空），所以是潜伏的；但这个门存在的唯一理由就是「一个 tag 下的文件静默不被编译」，而这恰好是同一方向的洞。附带：`:167` 的注释写「Eval short-circuits on `||`」—— 在恒 false 的 oracle 下短路的是 `&&`，`||` 反而会访问两侧。

### B7 · docs wire 版本闸的扫描面漏掉 CLAUDE.md §1 明列的一份权威活文档
镜头 1 + 镜头 2。`docsWireScanFiles`（`docs_wire_version_test.go:63-88`）只收「仓根一层 `*.md`」+「`docs/*.md` 一层」，因此 `test/simcluster/README.md`（CLAUDE.md §1 文档表把它列为 deploy-tier drill 的 Mandate 与用法权威）不在扫描面内；同理 `docs/reviews/` 以外的任何子目录活文档也不在。这不是「docs/reviews 刻意排除」那条已论证的决定，而是**未被任何 lane 提到的覆盖缺口**。今天该文件零条 subject 路径，所以是范围洞而非活缺陷。

### B8 · TLS 门的非空泛下限把 `_test.go` 站点也计入 —— 一个测试文件就能单独满足它
镜头 2。已并入 **M1** 的证据与修法（下限改成点名断言）。要点：全部生产 `InsecureSkipVerify` 字面量被删掉、或按赋值形态重写之后，这条「扫描器不是空转」的守卫仍然绿，而它的失败文案指着 `internal/cluster/transport.go` —— 恰恰是它不要求命中的那个站点。这也使 GI-11 建议的 `found < 3` 把实测的 4 写错了。

### B9 · `vet-tags` 的粒度没人质疑
镜头 3。`go vet -tags '$(ALL_TEST_TAGS)'` 把 7 个 tag **同时**打开，而任何真实调用都是一次一个（CLAUDE.md §5 的 `go test -tags dN_integration -race ./test/dN/`）。两个后果没被提：① 它照不到**负向约束**（`//go:build !x`）后面的文件 —— 本仓已在用这个形式（`internal/cluster/exchangedir_other.go:1 //go:build !linux`），所以「每个 tag 后的文件都还能编译」这个承诺对负向约束不成立；② 若将来两个互斥套件在同一个包里定义同名 helper，`vet-tags` 会红而**没有任何真实命令能复现** —— 一个没有真实对应场景的红，正是最容易被 muted 的那种。

### B10 · `.golangci.yml` 设计约束 1 自相矛盾，8 条 lane 全部把它当「实测为真」放过
镜头 2。第 9-13 行把 gocyclo 列进「计数 PHYSICAL LINES 所以刻意不启用」的名单，而同一条约束第 18-19 行又写「gocyclo and funlen{ignore-comments:true} are the comment-immune ones」—— 同一个 linter 在同一段里被同时否决和推荐。这不是文字瑕疵：X3 的重开条件逐字是「换用注释免疫的量」，而配置自己已经点名了那个量，于是 M13 发现的 MI=20 边界注释税在文档上**没有任何归属人**（谁想修都会先读到「gocyclo 不许启用」）。lint-config lane 明确宣称「头部四条设计约束逐条实测、全部为真」。（与 §5.2 的 X3/X7 循环是同一件事的两个侧面。）

### B11 · fail-open CRITICAL 的最便宜核验没人做
镜头 3。六条 lane 独立报了 `make lint` fail-open，其中 PI-1 把 impact 建立在「`.golangci.yml` 当前 untracked，一次 `git clean -xdf` 就删掉它」上，但没人查它是否被 `.gitignore` 命中 —— `git check-ignore -v .golangci.yml` 返回 rc=1，**未被忽略**，文件是可提交的，untracked 只是外审阶段的正常状态（用户明令「外审阶段主进程不要 git add」）。这不改变 C1 的结论（无 `-c` → 22→5 且 rc=0 是独立成立的），但把该 finding 里唯一一条会误导优先级的论证剔掉了：真正的修法是 Makefile 加 `-c` + 一条常驻断言，不是「记得 git add」。

---

## 7. 建议新增的测试

> 专家不写进仓库（CLAUDE.md §4）。每条都按「被测单元命名」（step 5b），finding 出处写成函数上方的一行 `// origin: line-2 internal review <ID>`。**按优先级排序。**

### 7.1 直接堵 CRITICAL

1. **`test/architecture/lint_config_test.go`**（C1，最高优先）—— 读 `Makefile`，断言 lint 配方含 `-c .golangci.yml`；断言 `.golangci.yml` 存在、`version: "2"`、能解析出非空 `linters.enable`。**纯文本/YAML 解析，不得调 golangci-lint**（gates 以 `make lint` 收尾会抢全局锁）。变异验证：删掉 `-c` 必须红；`mv .golangci.yml /tmp` 后 `make test` 必须红（今天三件套全绿）。这是 C1 修复后唯一能防那一行再被删掉的东西。
2. **`cmd/tether/cluster_watch_jsonl_test.go`**（C2）—— 表驱动 (asJSON, isTTY) 四组：ctx 预 cancel 只跑一帧，stub fetch 返一份报告，断言 (a) asJSON=true 时**每一行**都能 `json.Unmarshal` 成功（禁 `---` 分隔行、禁 ANSI），(b) asJSON=false && !isTTY 必须有 `--- ` 分隔行，(c) asJSON=false && isTTY 必须有 `\033[H\033[2J`，(d)（B2）fetch 失败帧不得产生任何 stdout 输出。`--watch` 目前全仓零覆盖 —— C2 正因此溜进来。（isTTY 现从 `os.Stdout` 直接取，需提成可注入字段，或先只测 asJSON 两组。）
3. **`test/architecture/nolint_directive_test.go`**（C3，plan §8 C4 已承诺）—— 解析 `linters.default`（`standard` 展开为 errcheck/govet/ineffassign/staticcheck/unused）+ `linters.enable`，扫全仓 `//nolint:a,b,c`，断言每个名字都在集合内。**预期第一次跑就红（gosec ×3 站点 + unconvert ×1）**，这就是它的咬合力证明，不需额外注入。配非空泛自检（合成一条 `//nolint:notalinter` 必须被识别）。
4. **`TestStructuralBudgetMeasurementIsNonVacuous` 补分类器探针**（C4）—— 合成一个「`package main` + 一行含 `cobra` 的**注释** + 若干代码行」的文件，断言它仍被计入 main-noncli；并断言 `cmd/tether` 里被跳过的文件数 == 真正 import cobra 的文件数。

### 7.2 直接堵 MAJOR

5. **`tls_verify_pairing_test.go` 扫描面扩展 + 点名下限**（M1 / B8）—— 除 CompositeLit 外扫 `*ast.AssignStmt`（`X.InsecureSkipVerify = <非 false>`），并把 `InsecureSkipVerify: <非字面量>` 也当 skipsHostname；下限从 `found == 0` 改成点名断言（`seen` 必须含 `internal/cluster/transport.go` 与 `internal/tunnel/tls.go` 的站点）。配 synthesized 源码样本自检（赋值式 / 变量式各一）。另配（M15）：同一份源码整体下移 N 行后 ledger 仍命中（证明 key 对注释位移免疫），而删掉 `VerifyPeerCertificate` 那一行后 offender 立刻出现（证明它仍抓真缺陷）—— 现在这两件事的信号完全一样。
6. **`TestEnumFamiliesCoverEveryRepoEnum`**（M2 / B4）—— AST 扫 `internal/`+`cmd/` 的 `type X <整型>` + iota 声明（及自有 string 枚举），每个类型必须在 `enumFamilies` 或一份带理由的 `notAnEnumLedger` 里；并把非空泛下限从 `guarded < 5` 升级成「每个已登记家族至少命中 1 个 switch」。
7. **`TestUpdateFlagPreservesLedgerJustifications`**（M3）—— 在 `t.TempDir()` 造一份带 `#` 理由行的 golden，跑 `updateGoldenTightenOnly`，断言理由行**逐字**保留。唯一能防 M3 复发的形状。
8. **`TestUpdateFlagRefusesNewOverBudgetEntity`**（M4）—— 假树含新超阈值实体 + 一份不含该 key 的 golden，断言 writer Fatal 且不写盘。
9. **`TestGateRunSelectorsSelectSomething`**（M5）—— 解析 plan §14 判据表里每条 `-run <re>`，断言该正则在对应包里至少匹配 1 个测试函数名，且匹配集合包含该判据要验的那个门。落盘当天就会抓到 #11/#13/#16。
10. **`TestLegacyFuncLedgerCountMatchesThePublishedNumber`**（M6）—— `const published = 436`，条目数 ≠ published 即红；文案写明「这是递减账本，**加条目不是豁免申请**」。与文件账本的同名守卫对称。
11. **函数账本 key 站点化的合成自检**（M7）—— 「在一个全新包里复用 436 条账本里的名字」写成合成自检（按 `docs/testing-standards.md` G2 的范式，不扫活树），断言同名不同路径必须分别登记。
12. **`TestProcessNamedFuncPatternRecognisesTheRealShapes`**（M8 / m16）—— mustMatch = `{TestB4ExposeExplainJSONNoDeferredKeys, TestExternalReviewProtoMismatchClassesAreTerminal, TestF9IncidentWriteRefusesSymlinkAndClobber, TestA9RebalanceTargetsExcludeDraining, TestP10Round2Foo}`；mustNotMatch = `{TestExposeExplainOmitsDeferredKeys, TestSha256HelperRoundTrips, TestHTTP2ListenerBinds, TestPortRangeWraps, TestV2SessionAttach}`（最后一个专门钉住「不要把字符类整体开成 `[A-Z]`」）。
13. **`layering_test.go` 的 `originalUnion` 数据表**（M9 / M10）—— 把四份已删 `regression_test.go` 的 10 个测试名 → 新表行的对照关系写成 `{origTestName, pkg, clause}` 表，断言每行的 banned 集合 **⊇** 对应并集（⊇ 而非全等）；下限从 `< 5` 改成 `!= len(originalUnion)`；再加一条「每条 ban 必须可命中」（等于某个真实 import path，或是其前缀祖先）—— 今天就会红在 `github.com/nats-io/nats-server` 上。变异验证：临时从任一行删一个子句必须红；新建 `internal/broker/subpkg` 并让 `internal/cluster` import 它必须红。
14. **layering 门登记 testlog**（M11）—— 在 `TestPackageLayering` 里对每条规则的包目录 `os.ReadDir` + `.go` 文件 `os.ReadFile`（结果丢弃）并加注释说明原因。验证：打热缓存 → 加违规文件 → 不带 `-count=1` 必须红。
15. **`test/architecture/lint_exemption_ledger_test.go`**（M12 / m3）—— 对 `.golangci.yml` 的 maintidx / unparam 名单，按 **`(rule.path, name)` 配对**断言该规则覆盖的站点恰好一个；对 dupl 名单断言 `path` 文件存在且行区间与当前内容对应。**判据不得写成「全仓恰好一处同名定义」**（与 M12 的 fix 冲突）。变异验证：把 `handleGrowTrigger` 改名必须红。
16. **`test/architecture/noctx_exemption_shape_test.go`**（M16 / m4）—— 断言 `.golangci.yml` 里 `linters: [noctx]` 的每条 exclusion 都同时带 `text:`（禁止只有 `path:` 的整文件级豁免）。变异验证：删掉任一条的 `text:` 必须红。可推广成对 dupl/unparam/maintidx 也要求「path 与 text 至少有一个把范围钉到站点」。
17. **`internal/agent/run_pty_failure_codes_test.go`**（M17 / M18）—— 把 errno→Reason 的判据抽成可测纯函数（`ptyFailureIsTransient(err) bool`），表驱动覆盖 `{EMFILE, ENFILE, ENOSPC, ENOMEM, EAGAIN}` → transient/`pty_alloc_failed`，`{ENOENT, EACCES, EPERM, ENODEV, EIO}` → `pty_unavailable`，并把 err 包一层 `fmt.Errorf("pty: open: %w", &fs.PathError{Err: errno})` 以同时钉住 `errors.Is` 穿透 PathError 这条链。**ENOSPC 那一行就是 M17 的红灯。**
18. **`internal/agent/upgrade_download_codes_test.go`**（M18）—— httptest 造三种响应（404 / 超 `upgradeMaxTarballBytes` / 中途断连），断言两层：(1) `errors.Is(err, ErrUpgradeHTTPStatus)` / `ErrUpgradeTooLarge` / 二者皆不满足 —— **这一层专杀 `%w`→`%v` 变异**；(2) 走 `handleUpgradeForwarded` 得到的 Code 分别是 download_http_status / download_too_large / download_failed。
19. **`internal/agent/run_attach_failure_test.go`**（M18）—— 注入 SubscribeSync 会失败的 nats.Conn，断言 (a) Reason 是 `attach_subscribe_failed` 而非 `pty_alloc_failed`，(b) PTY session 已被 Close（不泄 fd）。
20. **`cmd/tether/error_code_coverage_test.go` 增强**（M18）—— 补一张 `codesRequiringBehaviouralTest` 登记表（先放 4 个 Y2 码），断言每个码在 `internal/agent/*_test.go` 里至少有一处**以该码为期望值的断言**。这样「字面量在、分支死」不再能全绿溜过。
21. **`cmd/tether/node_upgrade_fleet_classify_test.go`**（**B1**，扩 `node_classify_test.go`）—— 表驱动断言每个 `brokerCodeExitClasses` 里的码在 fleet 分流上有确定归属：`exitUsage` 类必须 `isConfigError`（中止车队）、`exitTransient` 类必须 `isTransientError`（跳过并继续）；并补一条反向断言「exit class 表里的每个码要么被两个分类器之一命中，要么在一份带理由的 `deliberatelyUnclassifiedForFleet` 账本里」。今天会红在 download_http_status / download_too_large / download_failed 上。
22. **`test/determinism/docs_wire_codes_test.go`**（M19）—— 解析 `docs/usage.md` §9.14 的 adminsock「稳定 code」枚举，与 `internal/adminsock` 的 `Code*` 常量集合**双向**对账（手册有而代码无 → 红；代码有而手册无 → 进具名账本）。今天会红在 `already_voter` 上。这是 D1 反向门的文档侧对偶。
23. **`cmd/tether/error_hints_test.go` 补 retry-semantics 断言**（M17 / m4 冲突的机械化）—— 从 `usage.md §9.13` 解析可重试集合 {69,70,75} 与终态集合 {64,77}，对每个 hint 文案含「Retrying will not help / will do so on every retry / same size next time」等终态措辞的码，断言其 exit class ∈ 终态集合。今天会红在 `pty_unavailable` 上。它把手册的「健壮重试规则」变成可执行契约。
24. **`test/architecture/build_tags_test.go` 三方对账**（**B5**）—— 把 `test/e2e/all_phases_test.go` 里 `-tags` 后的字面量也纳入 `TestBuildTagsAreReconciled`。变异验证：把 `d5_integration` 拼错必须红（今天全绿且 d5 套件静默不运行）。顺手修 B6（`&&` 右侧不可见）与 `:239` 的「8」→7。
25. **`test/architecture/doc_path_liveness_test.go`**（M23 / C3 / m13）—— 扫 `CLAUDE.md`、`.golangci.yml`、`docs/` 下除 `reviews/` 外的文档、`test/simcluster/**/*.sh` 里出现的仓内路径字面量（含 `<path>.go:<line>` 形式），断言每个都存在。今天会红在 `test/architecture/budgets`、`nolint_directive_test.go`、两个 drill 的 `d8_alerts.go` 上。**一条实现同时关掉三处同形缺陷。**
26. **`test/determinism/docs_archive_layout_test.go`**（M25）—— 扫 `docs/*.md` 顶层，断言无 basename 命中 `-(plan|review|review-round[0-9]+|tasklist|roadmap)\.md$`；配可排空的具名豁免账本（每条一行理由 + 反向断言）+ 合成名非空泛自检；**必须显式排除 gitignored 的本地 scratch 文档**。变异验证：`touch docs/foo-plan.md` → 红；账本留一条指向已归档文件的条目 → 红。今天会先报出 `docs/cluster-ha-realmachine-test-plan.md`，正好逼出 M24 那条缺失的裁决。
27. **健康度 sweep 扩维**（m12）—— 把 `TestClusterExitZeroImpliesRealHA` 扩到 `topoDesired ∈ {0,7}` × `TopoReported ∈ {T,F}` × `ReachSource ∈ {self,nats-health}` × `Reachable ∈ {T,F}` × `DiskFreePct ∈ {0,5}` × `PortsUsed/Total ∈ {0/0, 95/100}`。变异验证：三条降级支短路必须各自变红（当前全绿）。
28. **`internal/natsconf/js_store_failclosed_test.go` 补一条**（m9）—— 父目录 chmod 0000 的 storeDir，断言 error 文本里同时出现「cannot stat」与 broker-ops #6 的 `chown` 话术（fail-closed 的同时必须给出**正确的**下一步）。
29. **`internal/broker/home_delivery_test.go`**（m8，仅在选择「保留并接出 acks」时）—— 多 expose 的一次 push、agent 逐端口 ack 后 `acks` 必须等于端口数（钉住 per-port token 消费的不变量）。
30. **维持型（非 `go test`）**：把 lint-config lane 的 `maintidx-all.yml`（`default: none`、仅 maintidx、`under: 100`、无 exclusions）存进 `test/architecture/testdata/` 作为**手动**对账脚本 + 一行 README（用法 + 并发锁约束）。它是唯一能看出「豁免遮住了什么」的视角 —— M12 就是靠它发现的。

---

## 8. 已驳回的 finding

**按综合规则 1（≥2 个镜头 refuted 才删）：0 条被删除。** 下列 7 条各被**单个**镜头驳回，未达门槛，已全部保留在 §1–§3，此处逐条记录驳回理由供主进程复核：

| id | 驳回镜头 | 一句话理由 |
|---|---|---|
| **GI-10** | 可复现性 | 它指控 `test/architecture` 的 package doc 分工规则被 build_tags 门自己违反 —— 但该 doc 第一段**逐字**把「which build tags exist」列为架构不变量，且 plan §15 仲裁同时写下了两条判据，`docs_wire_version` 的落位与之一致；唯一存活的事实是 `tls_verify_pairing_test.go` 没写放置论证（远弱于原表述）。见 m26。 |
| **IDG-11** | 修复方案 | 三个 sub-claim 里 **#1 那一半为假**：判据 #1 自己的命令（`golangci-lint linters`）实测就是 22，23 只出现在 JSON 产物（含 typecheck 伪 linter）—— 按它的建议把期望值改成 23 会让一条**现在通过**的判据永久失败。#20 与 #3 两半成立。见 m7。 |
| **F7[lint]** | 修复方案 | finding（三份名单无反向断言）成立，但其 **fix 判据**「每个函数名在仓内恰好一处同名定义」与同一 lane 的 F3/M12 fix 直接冲突：补 `path:` 后 `Run` 仍有 5 处定义，该门会永久红，照做只有「给 `Broker.Run` 改名」或「删 register（lint 立刻永久红）」两条出路。判据须改成按 `(rule.path, name)` 配对。见 m3。 |
| **PI-3** | 修复方案 | finding（MI=20 边界注释税）成立，但 **fix (b)「把 `under` 降到 15」有害且不解决问题类别**：会让 10 个已登记 god function 中的 6 个掉出报告范围、register 大半变死豁免，边界税只是搬到 MI 15；fix (a)「换 gocyclo」则触及 X7 的永久裁决。见 M13 的修法分段。 |
| **PI-8** | 修复方案 | finding（三维盲区 + X2 重开条件已被自己满足）成立，但 **fix 的阈值建议「只圈住 internal/broker / cmd/tether / internal/cluster」会把同一 lane 自己判为反向激励的形状放大十倍**（逐行棘轮从 10 个文件扩到 70 个），正是 PI-9 指控的缺陷。见 M21 的修法分段。 |
| **PI-12** | 修复方案 | finding（孤儿断言的梯度反了 + 三处自我封口注释）成立，但其 **fix 主体（`// no-promise: TestX` 转义标记）会重开这道门存在的唯一理由**：按它自己的限定「该标记只用于已在账本里/已删除的名字」，一条命名不存在且不在账本里的 Test 可以被「已删除」这半句合法化。只取 fix 的后半（把失败文案写清该删哪个文件的哪一行）。见 m18。 |
| **D15** | 范围与授权 | **唯一一条被判越界**：`deploy-tier-gotchas.md` 的 `alerts.go:144-163` 行号陈旧是**存量缺陷**（`git log -1 -S "alerts.go:144"` → `02913d9`，早于本次），本次在那一行只做了改名扫描，既没引入也没使它更错 ⇒ 不应计入本轮验收；正确归宿是独立订正，或由主进程明确判为顺手采纳而不占 line-2 的账。见 m23。 |

另有两条**未被驳回但被镜头实质性修正**，主进程需注意（已写入对应 finding）：
- **PC-2**：claim 与 errno 事实成立，但 impact 链（「监控停止重试」）被 `usage.md:1559` 实测推翻，且与 F2[deletions]/D5 对同一张表给出相反读法 —— 见 **M17**，主进程须先裁定 69 的语义。
- **GI-2**：范围镜头判 low confidence —— lint 不传 build-tags 是**存量**状态、plan 未承诺改，且其头号证据（`assertNoGoroutineLeak is unused`）恰好已被本次改动修掉；修法与 plan §13 冲突 ⇒ 属需要主进程给出 DO / REJECT-FOREVER 的**新工作**，不应算进本次交付的验收清单。见 **M22**。
