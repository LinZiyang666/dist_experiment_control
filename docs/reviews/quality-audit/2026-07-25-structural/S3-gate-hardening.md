# 综合 3 — 质量闸门加固方案（防止债务再生）

> 结构性质量审计 · 2026-07-25 · synthesis lane = `gate-hardening`
> 输入：L01–L12 十二份 lane 报告全文 + 本轮在仓库上的真实测量
> 只读综合。**未修改任何实现代码、未写入 `.golangci.yml`**——本文给出可直接落盘的全文，落盘由主进程执行。

---

## 0. 结论（先给数字）

我把整套候选 linter 真跑了一遍。**"没装探测器"这个起点判断成立；但"装上探测器会看到一座山"这个隐含预期是错的。**

| 度量 | 数值 | 来源 |
|---|---|---|
| 当前 `make lint` 结果 | **0 issues / 1.7s** | 实跑 |
| 当前启用的 linter | errcheck, govet, ineffassign, staticcheck, unused（golangci-lint v2 `standard` 默认集） | 无 `.golangci.yml`，走默认 |
| 候选普查（30 个 linter，阈值放到最松） | **2,821 issues**（prod 1,532 / test 1,289） | 实跑，14s |
| **本文推荐配置的基线** | **128 issues（prod 95 / test 33）** | 实跑，冷 14s / 热 **2.7s** |
| 其中"结构债"三件套 | dupl 14 + maintidx 11 + unparam 26 = **51 prod** | 实跑 |

**一句话结论：这个仓库不需要"收紧"，它需要"接线"。** 2,821 → 128 这个落差不是我在放水，而是普查里 2,693 条集中在 5 个与本仓工程约定正面冲突的 linter 上（`wrapcheck` 643、`noctx` 的 database/sql 分支 830、`gosec` 的 G304/G115/G204/G101 共 97、`revive` 默认集的 `exported`/`unused-parameter` 149、`perfsprint` 172）。把这 5 个按本仓的既有约定裁掉之后，剩下的 128 条**几乎每一条都指向 L01–L12 已经独立发现的东西**——这是配置正确的最强证据，不是配置宽松的证据。

**最重要的一条设计约束（也是最容易做错的地方）：本仓 29–33% 的行是注释，且 L01/L02/L05/L06/L11 五个 lane 一致认定这些注释是全仓最有价值的资产。任何按"行数"计的闸门都会把它们变成违规。** 实测：`funlen` 按物理行 span>60 报 159 条，按代码行（排除注释）>60 报 114 条——差 39%；span>120 报 50 条 vs code>120 报 19 条——差 163%。**一个配错的 `funlen` 会直接激励删注释。所以本文不启用 `funlen`/`gocyclo`/`gocognit`/`cyclop` 中的任何一个**，改用对注释免疫的 `maintidx`（Halstead 体积 + 圈复杂度，只吃 token，不吃注释）。

---

## 1. 度量方法（数字可复现）

三条独立证据链，全部在 `/home/weiland/dist_experiment_control` 上真跑：

**(A) golangci-lint 普查** — v2.5.0（`make lint` 已钉死的版本，`$(go env GOPATH)/bin/golangci-lint`）。
配置 `default: none` + 30 个候选 linter，`--max-issues-per-linter=0 --max-same-issues=0`，JSON 输出后按
`FromLinter` × `是否 _test.go` 二维统计。全仓 `./...`，**14 秒**。

> ⚠️ 复现时的两个坑（我踩了）：① `--output.json.path` 与 `--output.text.path` 必须同时给，否则 JSON 里
> `Issues` 为空；② `--uniq-by-line`（默认 true）会让**同一行上不同 linter 的报告互相吞掉**——
> 本次 `unparam` 从 13 跳到 26，唯一原因是关掉了 `revive.unused-parameter` 之后 `unparam` 的同行报告不再被吞。
> 任何"关掉 A 之后 B 的数量变了"的观察都要先排除这一条。

**(B) 自写 AST 度量工具** — `go/ast` 遍历全仓 5,003 个顶层 func/method，逐个算
`(物理行 span, 排除注释的代码行, 语句数, gocyclo 圈复杂度, 最大嵌套深度)`，
再对任意阈值做违规计数。这一步是为了**在选阈值之前就知道每个阈值的代价**，而不是先开再数。

**(C) 定向 grep / `go list` / `go vet -tags`** — 用于 linter 够不到的量（wire code 表差集、
测试文件命名、build-tag 隐身面、docs 版本字面量）。

分母：生产 68,328 行（`cmd` 14,611 + `internal` 53,717），测试 93,084 行，63 个包，1,884 个生产函数。

### 1.1 复杂度分布（这是选阈值的依据，不是拍的）

生产函数（1,884 个），违规数 vs 阈值：

| 阈值 | 违规数 | | 阈值 | 违规数 |
|---|---|---|---|---|
| gocyclo > 15 | 99 | | funlen span > 60 | 159 |
| gocyclo > 20 | 52 | | funlen span > 120 | 50 |
| gocyclo > 25 | 32 | | funlen span > 200 | 6 |
| gocyclo > 30 | **18** | | funlen **code** > 60 | 114 |
| gocyclo > 40 | 5 | | funlen **code** > 120 | **19** |
| nestif > 3 | 39 | | funlen **code** > 200 | 4 |
| nestif > 4 | **8** | | maintidx < 20 | **11** |
| nestif > 5 | 1 | | maintidx < 25（试算） | ~20 |

最深嵌套只有 6 层且全仓仅 1 处 >5——**"深嵌套"在本仓不是问题**，`nestif` 不值得开。
`maintidx under:20` 的 11 条命中的正是各 lane 点名的那批函数：
`Broker.Run`(cyclo 70)、`ClusterAdmin.StatusReport`(65)、`handleGrowTrigger`(52)、`handleUpgradeTrigger`(46)、
`dispatchForward`(39)、`newRunCmd`(38)、`driveAdd`(36)、`handleExposeReq`(32)、`newSessionCmd`(31)、
`newServeCmd`(29)、`handleRunForwarded`(24)。
**一个阈值、一个 linter，精确复现了 L01-F8 / L01-F7 / L03-F6 / L11 的全部函数级点名。** 这就是我选它的理由。

---

## 2. 跨 lane 裁决

lane 是限定范围的，以下六处我核实后与 lane 判断不同或需要收窄。

### 裁决 1 — "linter 抓不到结构问题"只对了一半，`dupl` 的召回率我实测是 3/11

L01/L02/L03/L04/L05/L09 六个 lane 合计点名了 11 处"手抄 N 遍"。我用 `dupl threshold:100` 实跑，
prod 命中 7 个唯一重复对：

| dupl 命中 | 对应 lane finding | 判定 |
|---|---|---|
| `clusterroster/roster.go:126` ↔ `seeds.go:93` | **L02-F7**（roster/seeds 平行继承线） | ✅ 精确命中 |
| `port/port.go:340` ↔ `:404` | **L08-F2**（`Revoke`/`RevokeAllocation` 旧孪生） | ✅ 精确命中 |
| `cluster_add.go:194/211` ↔ `cluster_upgrade.go:415/430` | **L03-F10**（callAdmin 四连样板） | ✅ 精确命中 |
| `cluster_ops.go:31` ↔ `:56` | 未被任何 lane 点名 | ✅ 新增发现 |
| `cluster/snapshot.go:78` ↔ `:128` | — | ⚪ save/restore 对称，正当 |
| `proto/subjects.go:175` ↔ `:298` | L06 counterEvidence 明确说**不要合并** | ⚪ 正当，需豁免 |

**但它一条都没抓到**：L01-F1（raft 写动词 5 处散弹）、L01-F2（13 处 ingress 门）、L03-F1（8 个轮询循环）、
L03-F2（22 份 ctl 前导码）、L04-F5（3 份 ServeListener）、L09-F1（4 份终态处置）。
原因是这些副本单块 10–28 行、token 级已分叉（不同的 reply 类型、不同的错误码），
低于任何可用阈值——把 dupl 降到 50 会在测试里炸出几百条。

> **我的裁决：`dupl` 值得开（成本 6 处豁免/修复，收益是 3 条已确认 finding 的复发防线），
> 但必须明确它不是 L01-F1/F2 的闸门。** 那两条只能靠第 5 节的自定义 AST 闸门。
> 任何"开了 dupl 就防住了复制粘贴"的说法在本仓是假的。

### 裁决 2 — L12 的 bloatScore 6 / essential 40% 与其余 11 个 lane 的"注释是资产"不矛盾，但闸门设计必须区分

L12 说 docs 40% 是反向读数；L01/L02/L05/L06/L11 说代码注释是全仓最有价值的资产。
两者分别针对 `docs/*.md` 和 `*.go` 内注释，不冲突。**但闸门层面这是同一件事的两面**：

- 对 `*.go` 注释：**任何按行计数的闸门都必须排除注释**（否则激励删注释）→ 本文不开 `funlen`。
- 对 `docs/*.md`：既有的 `TestNoStrayVersionLiteral`（AST 扫 `*ast.BasicLit`，注释不误报）**只扫 `.go`**，
  实测 `docs/` 里有 **120 处 `tether.v1`** vs 81 处 `tether.v2`——尺子自己是 v1 的。
  这条扩到 docs 的成本是 ~15 行测试代码。

### 裁决 3 — L05-F1(critical) / L06-F1(high) / L09-F3(high) 是同一条，我实测的差集比 L05 报的小但方向一致

三个 lane 独立发现"wire error code 无 SSOT"，严重度给了 critical/high/high。我做了机械对账：

```
prod 里 `Code: "literal"` 的不同码            : 39
brokerCodeHints 键                          : 34   （其中 17 个键没有任何 emitter → 陈旧）
brokerCodeExitClasses 键                    : 45
runFailureReasons 键                        : 10
─────────────────────────────────────────────────
无 exit class 的码                          : 27 / 39   (69%)
无 hint 的码                                : 22 / 39   (56%)
两张表都没有的码                             : 20 / 39   (51%)
```

L05 报的是"92 个码 / 61 无 hint / 54 无 exit class"。差异原因：L05 把 `adminsock.Code*` 常量族
（34+22+17+12 处引用）、tunnel DENY reason、`"<code>: "+err.Error()` 拼接（broker 内 31 处）
都算进了词表，而我的 grep 只覆盖 `Code:` 字段字面量。**方向完全一致、量级 L05 更全**。

> **我的裁决：这是本次审计里唯一一条"闸门可以一次性关掉整个缺陷类"的 finding，
> 且它同时是三个 lane 的最高严重度项。它排在所有闸门建议的第一位（G3.1）。**
> 我额外发现一个 lane 都没报的方向：**17 个 hint 键没有任何 emitter**——
> 表在两个方向上都漂了，所以闸门必须是双向的（码 ⊆ 表 **且** 表 ⊆ 码 ∪ 显式白名单）。

### 裁决 4 — L03 说 `newServeCmd` 的 297 行"不是债"，`maintidx` 说 MI=14。我判定两者都对，闸门形态因此必须是"具名豁免"而非"硬失败"

L03-F9 明确写："`newServeCmd` 的 297 行是 23 个 flag 变量 + 20 次 `pickFlagOrYaml` + 一个 26 字段 Config 字面量，
层次是齐的，我不认为它是债。" L01-F8 对 `Broker.Run` 给了同样判断（"20 段启动 DAG，拆成 20 个小函数只会把
唯一重要的信息藏起来"）。

我核实了 `Broker.Run`：358 代码行 / 529 物理行 span（31% 注释），三处 `PLACEMENT IS LOAD-BEARING` 注释各钉一次真实故障。
**L01/L03 的判断是对的，`maintidx` 的判断也是对的**——它没说"必须拆"，它说的是"这 11 个函数的可维护性指标已经见底"。

> **我的裁决：`maintidx` 开，但基线以 11 条带理由的 `//nolint:maintidx // <一句话:为什么这个函数必须长>` 落地，
> 而不是修掉。这 11 条豁免本身就是全仓"god function 登记表"——今天它不存在，
> 而它一旦存在，第 12 个就必须由作者手写一句理由才能加进去。** 这正是闸门该起的作用：
> 不阻止，但要求陈述。

### 裁决 5 — L04 把 `context.Background()` 的规则问题定为 medium 并建议"写进 architecture.md"，我实测 `contextcheck` 能抓 39 条，但我**不建议**开

L04-F6 说 `deleteXferObject` 8 个调用点里 4 个用 `context.Background()`，且"在 NATS 回调里取环境 ctx"有三套机制。
`contextcheck` 实跑 prod 39 条，包括 `Run$9->handleExecReq->pubAuditCall->publishAudit should pass the context parameter`
这一族——**它抓的是 `nats.MsgHandler` 签名没有 ctx 这个结构性事实，而这个事实 L04 自己说"非本仓造成"**。
39 条里绝大多数的正确响应是"不改"，改了反而会把可取消性引进审计发布路径。

> **我的裁决：驳回"开 `contextcheck`"。** 采纳 L04 的原建议（在 `architecture.md` 写一条两行规则 +
> 在每个 `context.Background()` 站点加一行引用注释）。理由：39 条里正确率我估 <20%，
> 一个正确率 20% 的闸门在单开发者项目上必然被 `//nolint` 批量绕过，绕过之后它就永久失效了。
> **闸门的第一属性是可信度，不是覆盖率。**

### 裁决 6 — L07-F1（134 个 process-named 测试文件）与 L12-F6（154 个）数字不一致；我实测 155

L07 报 134 / 18,228 行，L12 报 154 / 20,710 行。我用统一正则
`(_|/)((p|d|c|g|r|s|b)[0-9]+|external_review|round[0-9]+|allgreen|codex|megaaudit|review|fixes)[_.]`
实测 **155 个文件**（总测试文件 499，占 31%），分布：`internal/broker` 58、`cmd/tether` 34、
`internal/clusteroffline` 11、`internal/agent` 9、`internal/cluster` 8。

> **我的裁决：两个 lane 都低估了，取 155。** 更关键的是**它们的建议不一样**：
> L07 主张 `git mv` 机械改名（88 个可自动归位），L12 主张改名 + 合并（154 → 约 40）。
> **我判定 L07 的方案可执行、L12 的方案不可执行**——L12 的"合并"要求判断两个 round 文件是不是同一组不变量的两半，
> 这是逐文件的语义工作，154 个文件的语义合并在单开发者项目上必然半途而废，留下一个改了一半的树。
> **闸门层面我只要一件事：止血（新文件不许这么命名），存量进 golden allowlist 冻结。**
> 存量改名是可选的清理动作，不是闸门。

---

## 3. G1 — 可直接落盘的 `.golangci.yml`

以下为全文。落到仓库根目录 `.golangci.yml`。**实测：冷跑 14s / 热跑 2.7s，128 issues（prod 95 / test 33）。**

```yaml
# golangci-lint v2 configuration — tether
#
# 版本由 Makefile.GOLANGCI_VERSION 单点钉死（v2.5.0）并在 `make lint` 里强制校验；
# 本文件只负责"跑哪些检查"。改本文件 = 改闸门语义，必须同步 CLAUDE.md §5。
#
# 设计约束（读之前先看这三条，否则会把配置改坏）：
#   1) 本仓 29–33% 的行是注释，且注释记录的是"为什么"（事故、外审轮次、不变量论证），
#      是全仓最有价值的资产。任何按物理行计数的 linter（funlen / lll / gocyclo 的行数变体）
#      都会把它们变成违规，从而激励删注释。**故意不启用 funlen / gocyclo / gocognit / cyclop。**
#      函数级复杂度统一交给 maintidx —— 它只吃 token，对注释免疫。
#   2) 本仓的错误约定是 `fmt.Errorf("%w: ...: %v", sentinel, cause)`：sentinel 可 errors.Is，
#      内层原因降级为文本。errorlint.errorf / wrapcheck 会与这条约定正面冲突（实测 wrapcheck
#      在 prod 报 570 条），**故意不启用 wrapcheck，且 errorlint.errorf 关闭。**
#   3) 本仓所有 SQL 走 SQLite + SetMaxOpenConns(1)，且 raft FSM Apply 路径必须确定性、
#      不可被 ctx 取消打断。noctx 对 database/sql 的 244 条报告若照做会把可取消性引进 FSM，
#      **故意豁免 noctx 的 database/sql 分支**，只保留 http / exec / tls / dial 分支。

version: "2"

run:
  timeout: 10m

linters:
  # standard = errcheck, govet, ineffassign, staticcheck, unused（今天 make lint 跑的就是它，0 issues）
  default: standard
  enable:
    # ── T1：零基线正确性探测器（修完必须保持 0）─────────────────────
    - bodyclose        # HTTP body 未关闭
    - copyloopvar      # Go 1.22 之后多余的 `x := x`
    - durationcheck    # time.Duration 相乘（单位错误的经典来源）
    - errorlint        # 见 settings：只留 asserts + comparison
    - exhaustive       # 具名类型 switch 漏 case
    - forcetypeassert  # 未检查的类型断言（本仓 5 处，其中 3 处在 raft/JS 回调里）
    - gocritic         # 见 settings：主要为 exitAfterDefer
    - makezero         # append 到非零长度 make() 切片
    - nilerr           # err != nil 却 return nil
    - noctx            # 见 exclusions：只留 http / exec / tls / dial
    - revive           # 见 settings：显式小规则集，不用默认集
    - usestdlibvars    # 硬编码 "GET" / "200" 等
    - wastedassign     # 被覆盖的赋值

    # ── T2：结构棘轮（基线以具名豁免落地，不许增长）─────────────────
    - dupl             # 复制粘贴（阈值 100 行 token）
    - maintidx         # 可维护性指数（god function 探测器）
    - unparam          # 恒定参数 / 未使用返回值（死接口的最强信号）

    # ── T3：闸门自身的闸门 ────────────────────────────────────────
    - nolintlint       # //nolint 必须带原因、必须指名 linter、不许悬空

  settings:
    errorlint:
      # errorf:false —— 本仓 `%w: ...: %v` 是刻意约定（见文件头约束 2）。
      # 打开它会在 prod 报 25 条全部是"故意的"，闸门会立刻失去可信度。
      errorf: false
      # 这两条相反，是真 bug 探测器：实测 prod 5 条，其中
      # internal/cluster/operation_read.go:81/99/112 三处 `== ErrX` 会在错误被包装后静默失效。
      asserts: true
      comparison: true

    exhaustive:
      # 有 default 分支即视为穷尽。关掉这条会把每个带 default 的 switch 都报出来（噪音）。
      # 打开后 prod 仅剩 4 条，全部是"有 default 但漏了一个语义上必须显式处理的 case"。
      default-signifies-exhaustive: true

    dupl:
      # 100 是实测选出来的：
      #   100 → prod 7 个唯一重复对，其中 3 对精确命中 L02-F7 / L08-F2 / L03-F10；
      #   120 → 掉到 3 对，L02-F7 和 L08-F2 两条已确认 finding 都漏掉；
      #   <80 → 测试树里炸出上百条（测试本来就该有平行结构）。
      threshold: 100

    maintidx:
      # under:20 → prod 11 条，精确等于各 lane 点名的 god function 集合
      #（Broker.Run / StatusReport / handleGrowTrigger / handleUpgradeTrigger /
      #  dispatchForward / newRunCmd / driveAdd / handleExposeReq / newSessionCmd /
      #  newServeCmd / handleRunForwarded）。under:25 会涨到约 20 条并开始误伤。
      # 这 11 条以 //nolint:maintidx + 一句理由落地，构成全仓 god function 登记表。
      under: 20

    gocritic:
      disabled-checks:
        # ifElseChain 是纯风格偏好，本仓 if/else 链都很短。
        - ifElseChain
      # 保留默认 checks 的价值在 exitAfterDefer：实测 prod 3 条
      #（cluster.go:404 / cluster_status_nats.go:200 / main.go:77），
      # 每一条都是 os.Exit 跳过 defer 的真实资源泄漏点。

    revive:
      # 不用 revive 默认集：实测默认集在 prod 报 179 条，其中 80 条是 `exported`
      #（要求每个导出符号有 doc comment）。本仓 34/35 个包是 internal/，
      # 没有外部消费者，这 80 条是纯噪音。unused-parameter 69 条同理——
      # 本仓大量 seam 签名（nats handler、reconcile pass）参数必须保留形状。
      # 故只启用真 bug 类的小规则集，实测 prod 3 条 / test 5 条。
      rules:
        - name: indent-error-flow
        - name: empty-block
        - name: context-as-argument
        - name: range-val-address
        - name: unreachable-code
        - name: waitgroup-by-value      # 本仓有 4 处 WaitGroup barrier，值传递是致命的
        - name: bool-literal-in-expr
        - name: identical-branches
        - name: modifies-value-receiver
      # 故意不启用 redefines-builtin-id：实测 36 条全是把 `clear` / `any` / `max` / `min`
      # 当局部变量名（`clear` = "清锁", `any` = "任一节点"）。36 处重命名换不来任何东西。

    nolintlint:
      # 这三条让 T2 的"具名豁免"机制不会退化成 `//nolint` 一把梭：
      require-explanation: true   # 必须写为什么
      require-specific: true      # 必须写 //nolint:maintidx 而不是裸 //nolint
      allow-unused: false         # 问题修好后悬空的 nolint 会报错，豁免不会永久沉积

  exclusions:
    generated: lax
    rules:
      # ── noctx：只留真正需要 ctx 的那半 ──────────────────────────
      # database/sql：244 条。本仓 SQLite 单连接 + raft FSM Apply 必须确定性不可取消，
      # 把 ctx 传进去等于允许一个被取消的 ctx 从中间打断 FSM 事务 —— 方向是错的。
      - linters: [noctx]
        text: "database/sql"
      # net.Listen：8 条。ListenConfig 只在"绑定期间可取消"时有意义，本仓三处
      # loopback listener 都是 bind-then-serve，换 ListenConfig 换不来任何行为。
      #（这三处的真正债是 L04-F5 说的"三份逐字同构的 ServeListener"，那是 G3 的活。）
      - linters: [noctx]
        text: "net.Listen must not be called"

      # ── 测试树豁免 ──────────────────────────────────────────────
      # 测试本来就应该有平行结构（表驱动）、长函数（场景搭建）、未检查断言（t.Fatal 兜底）。
      # 把这几个 linter 放进测试树只会产生要求"让测试更像生产代码"的压力，方向是反的。
      - path: _test\.go
        linters:
          - dupl
          - maintidx
          - unparam
          - forcetypeassert
          - bodyclose
          - noctx
          - errorlint
          - gocritic
```

### 3.1 实测基线（这是"整改工作量"的准确定义）

| linter | prod | test | 处置 | 一次性成本 |
|---|---:|---:|---|---|
| `unparam` | **26** | 0 | 修（删参数/删返回值） | ~2h |
| `noctx` | **17** | 0 | 修 4 条（`http.Client.Get` ×3 在 reconcile 循环里、`exec.Command` ×4）+ 13 条 `//nolint` | ~1.5h |
| `dupl` | **14**(=7 对) | 0 | 修 3 对（= L02-F7 / L08-F2 / L03-F10）+ 3 对豁免 | ~3h |
| `maintidx` | **11** | 0 | 全部 `//nolint:maintidx` + 一句理由 | ~40min |
| `errorlint` | 5 | 0 | 修（`operation_read.go:81/99/112` 是真 bug 候选） | ~30min |
| `forcetypeassert` | 5 | 0 | 修 | ~30min |
| `exhaustive` | 4 | 0 | 修或豁免 | ~20min |
| `gocritic` | 3 | 0 | 修（3 处 `os.Exit` 跳过 defer） | ~20min |
| `nilerr` | 3 | 0 | 逐条判定（可能是刻意 fail-open） | ~20min |
| `copyloopvar` | 2 | 16 | `--fix` 自动改 | ~2min |
| `bodyclose` / `durationcheck` | 1 / 1 | 0 | 修 | ~10min |
| `revive` | 3 | 5 | 修 | ~20min |
| `usestdlibvars` / `makezero` | 0 | 8 / 3 | `--fix` / 修 | ~10min |
| `nolintlint` | 0 | 1 | 给 `test/p13/proxy_e2e_test.go:227` 的 `//nolint:noctx // test` 补一句像样的原因 | 2min |
| **合计** | **95** | **33** | | **约 10 小时** |

**长期负担：`make lint` 从 1.7s 变成 2.7s（热）/ 14s（冷）。CI 的 lint job 增加约 12 秒。**
每次改动的额外心智负担：T1 是零（都是真 bug，本来就该修）；T2 是"新增一个 god function / 新增一段
重复块 / 新增一个恒定参数时要么改、要么写一句理由"——这正是想要的。

---

## 4. G2 — 明确拒绝的 linter（附实测拒绝理由）

**这一节和第 3 节同等重要。** 单开发者项目上，一个噪音闸门的代价不是"多花时间"，
而是"养成 `//nolint` 一把梭的习惯，从而让全部闸门失效"。

| linter | 实测 prod | 拒绝理由 |
|---|---:|---|
| `wrapcheck` | **570** | 与 L05-counterEvidence 记录的既有约定正面冲突：本仓 21 处 `%w: ...: %v` 是刻意设计（保 sentinel 可 `errors.Is`，内层原因降级为文本）。570 条里绝大多数的"正确修法"是把一个想清楚过的约定改成另一个。**最高优先级拒绝。** |
| `noctx`（sql 分支） | 244 | 见配置注释：把 ctx 传进 raft FSM 的 SQL 事务是引进可取消性，方向错误。 |
| `gosec`（全量） | 111 | G304（变量文件路径）54 条——本仓是运维工具，读操作员指定的路径是产品语义；G115（整数溢出）26 条全是 DB 列的 int↔uint64；G204（子进程）10 条是 `nats-server -t` / `systemctl`，是产品功能；G101（硬编码凭据）7 条是 `tokenHash` 之类常量名误判。**剩下真正值得看的只有 G402(2) / G202(1) / G404(3)，共 6 条——为 6 条真信号引入 105 条噪音不划算。** 建议改为一次性人工复核这 6 条（约 30min），不接进闸门。 |
| `revive`（默认集） | 179 | `exported` 80 条要求 internal 包的导出符号写 doc comment（无外部消费者）；`unused-parameter` 69 条打的是 seam 签名。已改为显式 9 条小规则集。 |
| `perfsprint` | 167 | 161 条是 `fmt.Errorf`（无格式参数）→ `errors.New`。纯风格，本仓这些点都不在热路径。 |
| `intrange` | 18 + 173 test | Go 1.22 `for i := range n` 现代化。可用 `--fix` 一次改完，但 173 处测试改动会和第 5 节的测试文件改名撞车，且零正确性收益。**建议：不开；若将来要开，和测试树重组一起做。** |
| `funlen` / `gocyclo` / `gocognit` / `cyclop` | 159 / 99 / — / — | 与注释密度正面冲突（见 §0 与 §1.1）；且与 `maintidx` 高度重叠但精度更差。`maintidx` 的 11 条是 `gocyclo>30` 的 18 条的高精度子集。 |
| `nestif` | 39(>3) / 8(>4) / 1(>5) | 最深嵌套 6 层、>5 的只有 1 处。本仓根本没有深嵌套问题。 |
| `contextcheck` | 39 | 见裁决 5：抓的是 `nats.MsgHandler` 无 ctx 这一结构性事实，正确率估 <20%。 |
| `predeclared` / `revive.redefines-builtin-id` | 36 | 全是 `clear` / `any` / `max` / `min` 作局部变量名。36 处重命名，零收益。 |
| `thelper` | 0 + 86 test | 要求 helper 调 `t.Helper()`（改善失败行号归属）。**唯一一个我犹豫的**：86 条、约 1.5h、对 93k 行测试树的调试体验有真实改善。**判定：可选，放 T4；不进硬闸。** 理由是它不防任何债务再生，只改善体验。 |
| `goconst` | 7 | L03-F5 的路径字面量问题（6 处绕过 `defaultDataDir` 等常量）用 `min-occurrences:5` 只能抓到 7 条中的一部分，且抓不到那 6 处（每个字面量出现 2–3 次）。**这条改用第 5 节 G3.2 的自定义闸门更准。** |
| `godox`（TODO 检查） | — | 全仓 TODO/FIXME 仅 1 处。**但 L07-F6 指出真正的待办藏在 `t.Skip("tracked follow-up")` 和永不构建的 build tag 里**——所以纪律指标虚高，而 `godox` 对此完全无效。改用 G3.4。 |

---

## 5. G3 — linter 抓不到的闸门（**载体：Go 测试，不是 shell 脚本**）

### 5.0 载体选型（这是本节最重要的决定）

本仓已有三个成功先例，全部是 **Go 测试**：
- `test/determinism/lint_skeleton_test.go` — AST 扫 `*ast.BasicLit` 禁游离 `tether.vN` 字面量，**并带 `TestNoStrayVersionLiteralSelfCheck` 自检证明扫描非空洞**；
- `cmd/tether/command_tree_inventory_test.go` — 整棵 CLI 表面对 golden 断言，带 `-update-command-tree-golden`；
- `test/dN/regression_test.go` — `go list -deps` 分层规则（L07-F7 指出被拆成 4 份）。

> **裁决：所有自定义闸门一律写成 `test/architecture/` 下的 Go 测试，不写 shell 脚本、不加 CI step、不引新依赖。**
> 理由：① `make test` 已经是不可跳的硬闸，闸门自动进 CI 且自动进"提交前硬闸"，零接线成本；
> ② Go 测试免费拿到 `go/ast` 和 `go/packages`，shell 脚本做不到 AST 级判断（L07-F4 已经证明
> "grep 生产源码字符串"这条路会因 gofmt 空白而静默假绿）；
> ③ 团队已有三次成功经验和一套写法（含自检），复制模式比引入新范式便宜。

---

### G3.1 — wire error code 双向对账（**最高优先级**）

**关闭**：L05-F1(critical) + L06-F1(high) + L09-F3(high) + L05-F6 的一半。

**机制**：`test/architecture/wire_codes_test.go`，用 `go/ast` 扫 `internal/` + `cmd/`：

1. 采集 emitter 集合 `E`：所有赋给 `Code:` 字段的字符串字面量、所有 `"<code>: " + err` 形式的前缀
   （broker 内 31 处）、`adminsock.Code*` 常量的值。
2. 采集 classifier 集合 `C`：`cmd/tether/error_hints.go` 的 `brokerCodeHints` / `brokerCodeExitClasses` /
   `runFailureReasons` 三张表的键（用 AST 读 `*ast.CompositeLit` 的键，不用 grep）。
3. 断言 `E ⊆ C_exitclass ∪ 白名单`（**这是防止"新码静默退 70"的那一半**）；
4. 断言 `C ⊆ E ∪ 白名单`（**这是防止陈旧键沉积的那一半，实测今天有 17 个**）；
5. 白名单是文件内的具名 `var`，每条带一行理由（例：`"bucket_create_failed": "G67 M5 — 故意留在 70，是同一分裂的永久半"`）。
6. **自检**：合成一个含未登记码的假源文件，断言扫描恰好抓到 1 条（照抄 `TestNoStrayVersionLiteralSelfCheck`）。

**当前违规量**：27 个码无 exit class、22 个无 hint、17 个表键无 emitter。
**分阶段**：初次落地时把这 66 条差集全部写进白名单（机械，~40min），
然后**按 L05-F6 建议逐批清空**（先补那 8 个明显的 usage 错误码：`dst_exists` / `too_large` /
`path_outside_roots` / `transfer_disabled` / `not_a_regular_file` / `path_parent_missing` /
`path_not_absolute` / `path_not_found`——它们今天退 70，而 `docs/usage.md:1542` 指示监控对 70 退避重试，
每次重试都要重算全文件 SHA-256）。

**成本**：写 ~120 行测试 + 白名单 66 条 ≈ **3h 一次性**；长期每加一个错误码 **+1 行表项**（本来就该写）。
**这是全部建议里投入产出比最高的一条。**

---

### G3.2 — 结构预算棘轮（method 数 / 包文件数 / 包行数 / cmd-main 非 CLI 行数）

**关闭**：L01-F6（Broker 45 字段 263 方法）、L01-F5（65 个按 phase 切的文件）、L03-F6（2,987 行编排锁在 `package main`）、L02-F3/F6（包职责漂移）。

**机制**：`test/architecture/budgets_test.go` + `testdata/budgets_golden.txt`，带 `-update-budgets` flag
（照抄 `command_tree_inventory_test.go` 的 golden 更新模式）。**棘轮语义：当前值 ≤ golden 值即通过；
只允许把 golden 往下调，往上调必须显式改文件并在 commit message 里说明。**

golden 内容（今日实测值）：

```
# type-methods  <pkg>.<Type>  <count>
internal/broker.Broker            263
internal/agent.Agent              106
internal/broker.ClusterAdmin       67
internal/cluster.Node              36
internal/spawnsafe.Policy          23
# 阈值：新类型不得超过 40 个方法（未登记的类型超 40 直接失败）

# pkg-files  <pkg>  <prod-files>
internal/broker                    65
cmd/tether                         51
internal/cluster                   31
# 阈值：未登记的包不得超过 20 个生产文件

# pkg-lines  <pkg>  <prod-lines>
internal/broker                 21618
internal/cluster                 5300
...
# 阈值：未登记的包不得超过 3000 行

# main-noncli-lines  cmd/tether  3673
# = package main 里"零 cobra 引用"文件的总行数（L03-F6 的度量）。
# 只许降不许升 —— 直接把"别往 CLI 层塞业务逻辑"变成机械可判。
```

**当前违规量**：0（golden 就是今日快照）。**这是一个零基线闸门**——它不要求任何整改，
只要求"不再恶化"。这正是单开发者项目上唯一能长期存活的结构闸门形态。

**成本**：~150 行测试 + golden ≈ **2.5h 一次性**；长期 **0**（只在真的越界时才需要人做决定）。

> **为什么棘轮而不是绝对阈值**：L01 明确论证了 `*Broker` 是"命名空间型"而非"共享状态型" God Object
> （263 个方法里只有 6 个碰 ≥6 个字段），危害有限。设一个"必须 ≤40"的绝对阈值等于要求一次
> 大重构，在这个项目上等于被绕过。棘轮把成本降到零，同时保住"不再长"这个真正重要的性质。

---

### G3.3 — 测试文件命名规约（止血，不追溯）

**关闭**：L07-F1/F2（155 个 process-named 文件）+ L12-F6。

**机制**：`test/architecture/test_naming_test.go` + `testdata/legacy_test_names.txt`：

```go
// 禁止的形状：文件名含 external_review / round<N> / <phase-letter><digits>_ / allgreen /
// codex / megaaudit / fixes_ / _review。
// 存量 155 个文件全部列进 legacy_test_names.txt（冻结）。
// 新增：不在冻结表里且匹配禁止形状 → 失败，错误信息给出替代建议：
//   "按被测单元命名（proxy_generation_fencing_test.go），把溯源写成函数上方一行
//    // origin: p13 external review round 6 F2"
// 反向断言（防止冻结表变空洞）：冻结表里已经不存在的文件名必须被删掉 —— 存量改名后自动收紧。
```

**当前违规量**：0（155 个全部冻结）。**成本**：~60 行测试 + 155 行 golden ≈ **1h**；长期 **0**。

存量的 155 个改名是**可选清理**（L07 估算 88 个可机械改名），**不进闸门**——
见裁决 6：把它做成闸门会立刻卡住所有工作。

---

### G3.4 — build-tag 编译闸（隐身套件不许烂掉）

**关闭**：L07-F8（8 个 build tag / 24 个文件在 `go test ./...` 下完全不编译）+ L07-F6 的一半
（`//go:build c7_integration` 的死文件——全仓无任何地方定义这个 tag）。

**机制**：Makefile 里加一行，`make test` 依赖它：

```makefile
ALL_TEST_TAGS := d5_integration,d6_integration,d7_integration,d8_integration,d9_integration,c7_integration,phasefluidity_integration,e2e_matrix

# 8 个 build tag 下有 24 个测试文件，`go test ./...` 一个都不编译。一次 vet 就能证明它们
# 还跟得上生产代码；不加这一条，改完某个面跑 go test ./... 是绿的，而真正的守卫套件根本没被构建。
vet-tags:
	go vet -tags '$(ALL_TEST_TAGS)' ./...
```

**实测：8.7 秒，当前通过。** **成本：3 行 Makefile ≈ 5min；长期 +8.7s / 次 `make test`。**

> 附带收益：`c7_integration` 这个 tag 在 Makefile / CI / `all_phases_test.go` 里都不存在
> （L07-F6 的死测试），一旦进了 `ALL_TEST_TAGS` 就会被强制回答"它到底还要不要"。

---

### G3.5 — 分层规则合并成一张表

**关闭**：L07-F7（同一条架构级分层不变量拆进 4 个 phase 目录，helper 各 4 份，81 行重复）。

**机制**：`test/architecture/layering_test.go`，一张 `{pkg, banned []string, required []string, why string}` 表
+ 一份 `goListDeps`。把 `test/d5|d6|d7|d8/regression_test.go` 里的 4 组规则搬过来（`internal/cluster` 不得传递
import `nats.go` ×4、`jsstream` 不得 import `cluster` ×2、`clusternodes` 保持 pure-SQL leaf ×2）。

**并入 L02 的既有规则**（`test/determinism/lint_skeleton_test.go` 的 raft 禁闭 + 白名单自检）时**不要搬**——
那份已经在正确的位置且带自检，只在 layering 表里加一行指针即可。

**成本**：`git mv` + 合表 ≈ **1.5h**；净减约 250 行；长期新增一条规则从"新开文件"降到"加一行"。
**收益**：让"tether 现在有几条分层规则"这个问题有单一答案（今天没有）。

---

### G3.6 — docs 版本字面量扫描（把尺子本身钉住）

**关闭**：L12-F1(critical) 的机械部分、L06-F11。

**机制**：扩 `TestNoStrayVersionLiteral`：加一个 `TestDocsUseCurrentWireVersion`，扫 `docs/*.md` 与 `*.md`，
断言 `tether.v<N>` 中的 `N` 恒等于 `proto.ProtoVersion`，除非该行在一个显式的"历史基线"围栏内
（例如以 `<!-- wire-version-frozen: v1 -->` 开头的块）。

**当前违规量**：**120 处 `tether.v1`**（vs 81 处 `tether.v2`、1 处 `v3`）。

**分阶段**：这条**不能一次性开成硬闸**（120 条要么全改要么全围栏）。
建议：先做 L12-F1 的第一刀（`architecture.md` 顶部加 status banner + 把 §A–§K 整体围进
`wire-version-frozen: v1` 块，约 10 行编辑），此时违规量降到 0，再把闸门打开。
**成本：banner + 围栏 ≈ 1h；测试 ≈ 20 行 / 30min；长期 0。**

---

### G3.7 — ACL ↔ 订阅表对账

**关闭**：L10-F5①（"若只能落地一条建议，选①"）+ L06-F2 + L06-F6 的死 ACL 部分。

**机制**：`test/architecture/acl_subscription_test.go`：把 `auth.PermissionsForCtl/ForAgent/ForBroker` 里
面向 ctl 的 pub allow 转成通配形式，断言每条能被 `broker.go` 的 27 条订阅表某条 pattern 匹配、反向亦然。

**当前违规量**：**3 条死授权**（`kick` / `rotate-pin` / `node.*.tag`，实测 `permissions.go` 里
`kick`×2 / `rotate-pin`×2 / `.tag`×1 / `unregister`×2 / `version.announce`×3 的字面量，
对应订阅侧零订阅者）。**这条测试今天就会红——这正是它的作用。**

**成本**：需要先把 `broker.go:958` 的订阅表从匿名 struct 提成包级 `var`（L06-F2 / L10-F5② 本来就建议
给它加 `broadcast bool` 字段并删掉 `clusterwrite.go:59` 的第 4 张表）。
合并做：**~4h**（含删 3 条死 ACL + 消掉一张表）；长期每加一个 verb **-1 处编辑**（4 张表变 3 张）。

> **注意 wire 影响**：删 ACL grant 会让已签发的 24h JWT 与新模板不一致。但这 3 条 grant
> 指向的 subject **没有任何订阅者**，删掉在现网不改变任何可观察行为，不触 `ProtoVersion`。

---

## 6. G4 — Makefile / CI 接线（完整 diff 级建议）

```makefile
# 新增：
ALL_TEST_TAGS := d5_integration,d6_integration,d7_integration,d8_integration,d9_integration,c7_integration,phasefluidity_integration,e2e_matrix

vet-tags:
	go vet -tags '$(ALL_TEST_TAGS)' ./...

# 改：把 vet-tags 挂进 test（G3.4）
test: vet-tags
	go test ./...

# 新增：一条 "闸门自检" 目标，专给"改了闸门本身"时用
gates:
	go test ./test/architecture/... ./test/determinism/...
```

**CI 不需要改任何一行**：`build-test` job 跑 `make test`（自动带上 `vet-tags` 和
`test/architecture/...`），`lint` job 跑 `make lint`（自动读新的 `.golangci.yml`）。
**这是选 Go 测试作载体的直接红利。**

`.PHONY` 补 `vet-tags gates`。

---

## 7. G5 — `CLAUDE.md` 流程修订（可直接替换的条款文本）

L07/L11/L12 一致指出：这套 7 步流程本身是沉积物的生成器。实测沉积：
**155 个 process-named 测试文件**、**`docs/reviews/` 335 文件其中 140 份零引用（17,656 行）**、
**`internal/broker` 一个包 288 处 review 轮次标签**。

L12-F4 另外查出 CLAUDE.md 自身有 3 条已过时指令。以下是可直接替换的条款。

### 修订 1 — §3 阶段 C 增加 step 4b「归位」（**这是止血的关键一条**）

> **4b. 归位（专家产出落盘规约）**：step 4 专家新增的测试**必须按被测单元命名并放在被测单元旁边**
> （`proxy_generation_fencing_test.go`，不是 `p13_external_review_round6_test.go`）；
> 若与既有测试守同一条不变量，**合并成表驱动的一个测试**而不是新开文件。
> 溯源写在测试函数上方一行注释：`// origin: p13 external review round 6 F2`。
> **禁止新建任何 `*_external_review*_test.go` / `*_round<N>_test.go` / `*_allgreen_*_test.go`。**
> 该规约由 `test/architecture/test_naming_test.go` 机械强制，存量 155 个文件已冻结在
> `testdata/legacy_test_names.txt`，只许减不许增。

### 修订 2 — §3 step 7 增加「归档」动作

> **7. phase 结束**：① 把本轮的 `*-review*.md` / `*-round*.md` / tasklist **移入
> `docs/reviews/archive/<stem>/`**，只把最终裁定回写进 `docs/reviews/<stem>-plan.md` 的
> 「落地结论」小节；② 在 `docs/reviews/INDEX.md` 追加一行（版本 / 日期 / 一句结论 / plan 路径）；
> ③ `git commit` + `git push`（见 §6）。
> **不做 ①② 不算 phase 结束**——沉积的成因就是"review 报告和 plan 平铺同级、无人敢批量归档"。

### 修订 3 — §5 增加「文件与注释纪律」三条

> - **新文件必须能用一个名词短语说清职责**；不得以 phase / review 轮次命名
>   （`clusterwrite.go` 装了 4 个不相干职责就是反例）。新代码优先并入职责匹配的既有文件。
> - **注释里的 review 轮次标签改用稳定锚点**：写 `// [inv:topo-fail-closed]` 而不是
>   `// round-5 S5-15`，锚点在 `docs/reviews/quality-audit/invariant-index.md` 里映射到
>   建立它的那次 review。锚点可 grep、可被测试名引用（`TestInvTopoFailClosed`），
>   从而把注释与行为钉在一起。**存量 288 处标签不追溯改。**
> - **注释是资产，任何重构必须整段搬运**。`.golangci.yml` 故意不启用 `funlen`/`lll`
>   就是为了不让行数闸门反向激励删注释——改配置前先读该文件头部的三条设计约束。

### 修订 4 — §5 增加「闸门」小节

> - **提交前硬闸**：`make test`（含 `vet-tags` + `test/architecture/...`）+ `make e2e` + `make lint` 全绿。
> - **闸门本身的变更走同一流程**：改 `.golangci.yml` / `test/architecture/*` 的 golden
>   等同于改不变量，必须在 commit message 里写明"为什么这个预算该放宽"。
>   golden 只许往收紧方向自动更新（`-update-budgets` 拒绝写入比现有值更宽的数）。
> - **`//nolint` 必须指名 linter 并带一句理由**（`nolintlint` 强制）；
>   问题修好后悬空的 `//nolint` 会报错，豁免不会永久沉积。

### 修订 5 — 三条过时指令的订正（L12-F4，机械）

> - §6 与 `docs/architecture.md:2303`：**删掉 `phase/<N>-<slug>` 开分支的要求**
>   （项目实际全部直接落 `main`，这与用户记忆 `feedback_main_only_no_branches` 一致）。
> - §3 的 Workflow 模型硬约束：把「不得低于 **Opus 4.8**」改成
>   「**省略 `model` 以继承会话主模型**；不得使用 Haiku / Sonnet」——去掉版本号，避免每次升级都要改文档。
> - §1 文档地图：补 `docs/distributed-broker-architecture.md`（691 行，README 与 runbook 都称其为绑定契约）
>   与 `docs/deploy-tier-gotchas.md`（618 行活账本、12 条 OPEN）；
>   把 `architecture.md` 那行改为「**proto v1 单 broker 历史基线**；v2/集群面的绑定契约见 distributed-broker-architecture.md」。
>   —— 不改这条，新会话会拿一把 v1 的尺去量 v2 的集群面（L12-F1 与 L12-F4 的复合伤害）。

### 修订 6 — §5 simcluster 两段长文压缩（可选，纯瘦身）

L12-F4 实测：CLAUDE.md 第 65–66 行合计 2,472 字符 = 全文（13,029 字符）的 **19%**，
内容已被 `test/simcluster/README.md` 完整覆盖。压成「定位一句 + 宿主判断三档 + 指向 README Mandate」
约 3 行。**收益是每个会话的上下文预算，不是磁盘。成本 20min。**

---

## 8. 成本与优先级汇总

| # | 闸门 | 当前违规 | 一次性成本 | 长期负担 | 关闭的 lane finding | 优先级 |
|---|---|---:|---:|---|---|---|
| G3.1 | wire code 双向对账 | 66 条差集 | **3h** | +1 行/新码 | L05-F1(crit), L06-F1, L09-F3 | **1** |
| G1-T1 | `.golangci.yml` 零基线层 | 41 prod | **4h** | +1s/lint | L08 全部, L11-F4 若干 | **2** |
| G3.2 | 结构预算棘轮 | **0** | 2.5h | **0** | L01-F5/F6, L03-F6, L02-F3/F6 | **3** |
| G3.3 | 测试命名规约 | **0** | 1h | **0** | L07-F1/F2, L12-F6 | **3** |
| G3.4 | build-tag 编译闸 | **0** | 5min | +8.7s/test | L07-F8, L07-F6 半 | **3** |
| G1-T2 | dupl / maintidx / unparam | 51 prod | **6h** | 加 god fn 需写理由 | L02-F7, L08-F2, L03-F10 | 4 |
| G3.7 | ACL ↔ 订阅对账 | **3 死授权** | 4h | -1 处编辑/verb | L10-F5①, L06-F2/F6 | 4 |
| G3.5 | 分层规则合表 | 0 | 1.5h | -250 行 | L07-F7 | 5 |
| G3.6 | docs 版本扫描 | **120** | 1h(围栏)+30min | 0 | L12-F1 机械部分 | 5 |
| G5 | CLAUDE.md 六条修订 | — | 1.5h | **0** | L07-F1, L12-F4/F5, L11-F8 | **2** |
| — | `thelper`（可选） | 86 test | 1.5h | 0 | — | 9 |

**总计：约 26 小时一次性；长期每次 `make test` +8.7s、`make lint` +1s、CI +约 20s。**

**如果只做三件事**：G3.1（wire code 对账）+ G1-T1（零基线 linter）+ G5（CLAUDE.md 止血条款）。
共约 8.5 小时，关掉 3 条 critical/high 并止住最大的两条沉积流（测试文件命名、review 归档）。

---

## 9. 明确不建议做的事（避免闸门反噬）

1. **不要引入 baseline 文件机制**（`golangci-lint` v2 无内置支持，社区方案要么是 `--new-from-rev`
   要么是外部 diff 工具）。本仓 T2 的基线只有 51 条，用**带理由的 `//nolint` + `nolintlint` 强制**
   更好：豁免落在出问题的那一行、自带理由、修好后自动报错清理。
   opaque baseline 文件的最大问题是**它会静默吸收新违规**——正是要防的东西。

2. **不要用 `--new-from-rev` / `--new-from-merge-base` 做主闸门。** 本仓直接落 `main`
   （`feedback_main_only_no_branches`），没有稳定的 diff base；且 `make lint` 是本地硬闸，
   不是 PR 检查。可以在 CI 的 `lint` job 里**额外**加一个 `--new-from-rev=HEAD~1` 的软报告 step，
   但主闸门必须是全量+基线豁免。

3. **不要为了让 `dupl` 归零而合并 `proto/subjects.go` 的 40 个具名 builder。**
   L06-counterEvidence 明确论证过具名 builder 可 grep、类型友好，合并只会更糟。
   给那一对加 `exclusions.rules`（`path: internal/proto/subjects\.go` + `linters: [dupl]`）并写明理由。

4. **不要把 155 个测试文件改名做成闸门的前置条件。** 见裁决 6。冻结即止血，改名是可选清理。

5. **不要开 `gosec` 全量、`wrapcheck`、`revive` 默认集中的任何一个。** 三者合计 860 条 prod 违规，
   全部与本仓已经想清楚的约定冲突。在单开发者项目上，一次"批量 `//nolint`"就会让整套闸门失去可信度。

6. **不要动 `make e2e` 的串行决策。** `Makefile:22-34` 与 `all_phases_test.go:32-38` 记录了完整的
   试→测→退过程（D8 在 2-way 挂、D5 在 4-way 挂、GOMAXPROCS 封顶无效）。
   L07-F5 攻击的是**重复**（`internal/cluster` 在 5 个 D 矩阵里各跑一次、`internal/broker` 跑 2 次，
   约 940 次冗余测试函数执行）不是串行——但去重要求逐包核对 `-tags` 造成的构建集合差异，
   **风险高于收益，本文不建议现在做**，只建议在 `all_phases_test.go` 顶部记一条注释登记这个已知冗余。

---

## 10. 附录：本文所有数字的复现命令

```bash
export PATH=$PATH:/usr/local/go/bin:$(go env GOPATH)/bin

# 当前基线（0 issues / 1.7s）
make lint

# 推荐配置的基线（128 issues / 冷 14s / 热 2.7s）
golangci-lint run --config .golangci.yml \
  --max-issues-per-linter=0 --max-same-issues=0 \
  --output.text.path=/dev/null --output.json.path=/tmp/g.json ./...
python3 -c "
import json,collections
d=json.load(open('/tmp/g.json'))
c=collections.Counter((i['FromLinter'], not i['Pos']['Filename'].endswith('_test.go')) for i in d['Issues'])
for l in sorted({k[0] for k in c}): print(f'{l:<18}{c[(l,True)]:>5}{c[(l,False)]:>5}')"

# build-tag 编译闸（8.7s）
go vet -tags 'd5_integration,d6_integration,d7_integration,d8_integration,d9_integration,c7_integration,phasefluidity_integration,e2e_matrix' ./...

# wire code 差集
python3 - <<'PY'
import re,os
codes=set()
for r,_,fs in os.walk('.'):
    if '.git' in r: continue
    for f in fs:
        p=os.path.join(r,f)
        if f.endswith('.go') and not f.endswith('_test.go') and (p.startswith('./internal') or p.startswith('./cmd')):
            codes |= set(re.findall(r'Code:\s*"([a-z0-9_]+)"', open(p).read()))
s=open('cmd/tether/error_hints.go').read()
K=lambda v:set(re.findall(r'"([a-z0-9_]+)"\s*:', s[s.index(v):s.index('\n}\n',s.index(v))]))
h,e,rr=K('brokerCodeHints'),K('brokerCodeExitClasses'),K('runFailureReasons')
print('codes',len(codes),'no-exitclass',len(codes-e),'no-hint',len(codes-h-rr),'stale-hint-keys',len(h-codes))
PY

# 测试文件命名违规（155）
find . -name '*_test.go' | grep -Ec '(_|/)((p|d|c|g|r|s|b)[0-9]+|external_review|round[0-9]+|allgreen|codex|megaaudit|review|fixes)[_.]'

# docs 版本字面量（120 × v1）
grep -roE 'tether\.v[0-9]' docs/ *.md | sed 's/.*:\(.*\)/\1/' | sort | uniq -c
```

---

*本文只读产出。`.golangci.yml`、`test/architecture/*`、`Makefile`、`CLAUDE.md` 的实际落盘由主进程执行。*
