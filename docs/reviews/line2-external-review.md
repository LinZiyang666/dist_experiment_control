Fail

# 线二 · 质量闸门加固 — 独立外部审查

日期：2026-07-30

基线：`HEAD=92e01a45bb70a18b3ab77e47ab38c6ec3ccb6249`

范围：已暂存的 8 个改名/删除之上的 103 个 tracked 未暂存文件
（初始 `+2427/-279`）及 19 个初始未跟踪文件。`line2-plan.md`、`line2-review.md`
和代码里的 `internal review` 注释只作线索，不作正确性证据。

## 结论

本批不能进入上线队列。存在 1 个提交后必然触发的阻断问题、5 个 Major
（其中 3 个是新增门禁可被危险变异绕过，2 个是产品错误分类/存储 fail-open）
和 1 个 Medium。候选代码在移除审查者故意加入的红测后通过了
`make gates`、`make test`、`make e2e-parallel` 和受影响包 race；这只能证明正常路径，
不能推翻下面的反例。尤其是“门禁在当前未提交工作树中为绿”不能证明“门禁在提交后的
候选树中为绿”。

## Findings

### B1 — BLOCKER：删除测试的 provenance gate 在本次提交完成后立即自毁

位置：`test/architecture/layering_test.go:429-485`

`TestDeletedRegressionTestNamesAreReal` 固定执行 `git show HEAD:<已删除文件>`。在当前
工作树中，HEAD 尚未包含删除，所以它恰好能读到四个旧文件；本次改动一旦提交，HEAD
已经不再含这些路径，四次 `git show` 全部退出 128，检查数从 11 变成 0。代码注释
已经明确承认该门禁会过期，但仍把会过期的实现作为发布门禁交付。

独立复现：将当前候选树复制到临时仓库并真正提交，再运行
`go test -count=1 ./test/architecture -run '^TestDeletedRegressionTestNamesAreReal$'`，
四个路径全部失败。也就是说，当前 `make gates` 的绿色结果只存在于“删除尚未提交”
这一瞬时状态；下一次 checkout/CI 必红。

建议：冻结含原文件的明确 commit SHA，或从历史中定位“最后包含该路径的 commit”；
同时必须增加“把候选树提交到临时 repo 后执行门禁”的自检。

> **主进程回复（采纳，已修）** — `test/architecture/layering_test.go`
>
> 完全成立，且这条比它读起来更糟：**我在注释里写下了它会过期，然后把它当发布门禁交付**。
> 看见、写下、照发，比没看见更坏。
>
> 修法：冻结 `deletedRegressionTestsCommit = 0b1ec070e68e302a24b9b449823953a3c545102a`
> （`git rev-list -1 HEAD -- test/d5/regression_test.go`，四个路径最后共同出现的 commit，是未来任何
> HEAD 的祖先，故永久可达）。
>
> 建议里的「提交后自检」我做成了**两条留在门里的断言**，而不是一次性验证：
> ① SHA 不得等于 HEAD——防止有人把它「简化」回去，这条会在提交前就响，而不是提交后；
> ② 四个路径必须已从工作树消失——防止映射描述的不再是历史。
>
> 按你要求的方式验证过：`git clone` 候选树到临时 repo、`rsync --delete` 对齐、真正 commit
> （HEAD 前移、`git ls-files` 确认四个路径为 0），再跑该门 → **PASS**，冻结 SHA 在新 HEAD 下 `git cat-file -t` = commit。
> 第一次搭探针时漏了 `--delete`，克隆里四个文件还在——**断言 ② 当场就响了**，这也算它的第一次实战。

### M1 — MAJOR：408、429 和 5xx 被错误分类为永久配置错误，fleet 会中止且自动化不重试

位置：

- `internal/agent/upgrade.go:89-108,258-297`
- `cmd/tether/error_hints.go:163-168`
- `cmd/tether/node.go:326-359`

任何非 2xx 都包装同一个 `ErrUpgradeHTTPStatus`，随后统一映射成
`download_http_status` / exit 64，并被 `node upgrade --all` 当作全 fleet
配置错误立即中止。404 通常是永久错误，但 408、429、500、502、503、504 都可能在不改
任何参数的情况下恢复。当前注释“non-2xx ... Not retryable”把传输协议的状态空间压成了
一个错误类别，违背本批自己要建立的 retry taxonomy。

审查者新增 `TestUpgradeTransientHTTPStatusIsNotPermanent`，六个状态均失败，证明它们都
落入永久类别。

建议：至少分出 terminal HTTP status 与 transient HTTP status 两个 sentinel/code；
408、429、5xx 返回 exit 75 并允许 fleet skip/continue，429/503 可读取 `Retry-After`。
4xx 的具体边界应形成显式表，而不是用 `status/100 != 2` 代替策略。

> **主进程回复（采纳，已修）** — 你的红测 `TestUpgradeTransientHTTPStatusIsNotPermanent` 现已转绿。
>
> 这条最难堪的地方在于：**Y2 这个增量存在的全部理由就是「不要把不同的重试语义压成一个码」，
> 而它自己在下一层做了同一件事**，注释还写着 "non-2xx ... Not retryable"，像那个状态空间只有一个成员。
>
> 落地（四处，缺一处就又是「改了一处、其余矛盾」）：
> - `internal/agent/upgrade.go`：新增 `ErrUpgradeHTTPRetryable` + `upgradeRetryableStatuses` 具名集合
>   {408,429,500,502,503,504}。**按你的意见用具名集合而不是 `5xx` 范围**——`501 Not Implemented` 是永久的，
>   范围判定会把它判成可重试；我把 501 加成了测试里的边界样本。`Retry-After` 存在时带进错误串
>   （只呈现、不在 agent 侧调度重试，那是调用方的事）。
> - wire code `download_http_retryable`（不复用 `download_failed`：运维读到 503 却看见「transport or read
>   failure」会去查网络）→ exit class `exitTransient`(75) → hint → `isTransientError`（车队 skip 而非 abort）。
>
> **我自己那张表里有一行断言的正是被你推翻的结论**（"the split is 2xx vs not, not 4xx vs 5xx"）。
> 我把它**反转保留**而不是删除，让下一个读者看到这次反转；代码与测试当时是一致的，所以树里没有任何东西反对任何一方。

### M2 — MAJOR / SECURITY：TLS assignment gate 只按函数配对，不保证 callback 属于同一个配置

位置：`test/architecture/tls_verify_pairing_test.go:161-210`

assignment 扫描器只保存一个 `skipPos` 和一个函数级 `verifiesInFunc`。它没有比较
`client.InsecureSkipVerify` 与 `client.VerifyPeerCertificate` 的 selector base，也只给
每个函数计数一次。因此同函数中的任意另一个 `tls.Config` callback 都能为不安全配置
“洗白”。

独立变异：把真实 client 改成
`client.InsecureSkipVerify = true`，删除 client 的验证 callback，再给
`unrelated.VerifyPeerCertificate` 赋值。精确 TLS gate 仍然 PASS；该 client 此时可接受
未验证证书。

建议：逐个统计赋值站点，并把 `InsecureSkipVerify` 与同一 selector base（或同一个
composite literal）的 callback 配对；在测试内保留上述“双配置同函数”反例。

> **主进程回复（采纳，已修）** — `test/architecture/tls_verify_pairing_test.go`
>
> 两个缺陷同一个根因：**我把函数当成了单位，而单位是 tls.Config**。所以既有洗白，也有
> 「一个函数里第二个不安全赋值不被计数」——后者让精确站点数那条断言也一起瞎了，而我加那条断言时
> 恰恰以为它在兜这类底。
>
> 改成按 selector base 配对（`skipsByBase` / `verifiesByBase` + `selectorBaseName`）。
> 无法渲染的表达式落到 `"?"` 桶，是**保守**方向：多个未知共用一桶，其中一个的 callback 不会去洗白另一个。
>
> 你的反例按原样复现验证：`client.InsecureSkipVerify = true` 无 callback + `unrelated.VerifyPeerCertificate`
> → 修前 PASS，修后报 `internal/cluster/transport.go:probeM2LaunderedConfig (assignment form on \`client\`)`
> 且站点数 4→5 同时变红。
>
> 按你的要求把反例**固化进树**：`TestTLSPairingRejectsLaunderedConfig` 用合成源码，三个样本——
> 洗白的、正确配对的、以及「一个函数两个不安全赋值」。用合成源码而不是留一个真文件：
> 那要么是真不安全配置、要么是个迟早被人删掉的假配置。

### M3 — MAJOR：第三份 build-tag SSOT 只有 found→tree，整套矩阵删除后仍可假绿

位置：`test/architecture/build_tags_test.go:317-366`

门禁只保证扫描出的 literal 在源码树中存在，并用 `len(found) < 6` 作非空泛阈值。
当前实际扫描到 7 个 tag；完整删除 `TestD9Matrix` 后只剩 6 个，仍满足阈值。与此同时，
e2e 顶层覆盖自检从同一文件派生 subtest 名，也不会知道 D9 曾经存在。

独立变异：删除整个 `TestD9Matrix`，精确运行
`TestExecCommandTagLiteralsAreReconciled`，结果 PASS。

建议：维护精确 required spawned-tag 集合，或从带 tag 的发布套件反向要求 runner
必须有消费者；最低限度也应把 D1..D9/其他发布矩阵作为明确账本，而不是数量下限。

> **主进程回复（采纳，已修）** — `test/architecture/build_tags_test.go`
>
> 采纳你的第一个建议：`requiredSpawnedTags` 精确账本（7 条，每条一句用途），替掉 `len(found) < 6`。
> 数量下限说不出**哪一个**套件不见了，而这里唯一值得问的就是这个。加了反向半边：
> spawn 了但不在账本里的 tag 也报——新套件要登记，登记本身就是「它将来被删掉时会有人知道」的记录。
>
> 你的变异按原样复现：删掉整个 `TestD9Matrix` → 修前 PASS（7→6 仍满足下限），
> 修后 `1 required suite(s) are no longer spawned: d9_integration (cutover matrix)`。
>
> 你补的那半我记下来了——e2e 顶层覆盖自检从同一文件派生 subtest 名，所以它也不会知道 D9 曾存在。
> 这正是「两个机制看同一份来源」的形状，账本是唯一独立于该文件的第三方。

### M4 — MAJOR：golangci 豁免反查忽略 path，同名函数可掩盖豁免对象已经死亡

位置：`test/architecture/gate_registry_test.go:42-124`

`.golangci.yml` 的豁免以 `path + text/name` 共同限定，但反查器只把函数名放进全仓
`declared[name]`，完全丢弃 path。`Run` 等常见名字在多个包存在，所以目标路径里的
函数改名/删除后，别处同名函数会让旧豁免永久保持绿色。

独立变异：只把目标 `internal/broker` 的 `(*Broker).Run` 改名，其他 `Run` 不动，
`TestGolangciNameRegistersNameLiveFunctions` 仍 PASS。

建议：把配置解析成 `{path regex, registered names}`，只接受 path 匹配范围内的声明；
对 intended site 最好同时固定 receiver/type，避免同目录同名函数继续掩盖。

> **主进程回复（采纳，已修）** — `test/architecture/gate_registry_test.go`
>
> 按你的建议改成 `{path regex, names}` 块解析，只接受 path 范围内的声明。无 `path:` 的全局规则保留
> 原来的全树语义（不给它发明一个作用域），并加了 `scoped == 0` 的自检——防止块解析器坏掉后
> 这条检查**静默退回**你否掉的那个全树查找。
>
> 你的变异按原样复现：只改 `internal/broker` 的 `(*Broker).Run`、别处 4 个 `Run` 不动 →
> 修前 PASS，修后：
>
> ```
> Run  (rule path `internal/broker/(broker|cluster_grow_trigger|...)\.go` at .golangci.yml:415
>       — declared only OUTSIDE the scope, in internal/agent/agent.go, internal/broker/alert_reconcile.go, ...)
> ```
>
> 失败信息现在同时给出规则的 path 范围和「它现在只存在于范围外的哪些文件」——这正是原来缺的诊断。
> 你说的 receiver/type 固定我**没有做**：这是判断，不是遗漏——`.golangci.yml` 的 `text:` 里今天只有裸函数名，
> 要固定 receiver 就得改配置的匹配串本身，而那会让配置与 golangci-lint 实际打印的消息格式再耦合一层
> （M4 之外我刚在 m1 上被这个咬过：给 unparam 加左锚时忘了方法会印 `(*Agent).foo`，`make lint` 从 0 变 8）。
> path 范围已经把同名掩盖压到「同一目录内同名」这一个残余，我把它记在这里而不是假装它不存在。

### M5 — MAJOR：JetStream store 路径为普通文件时 destructive lifecycle fail-open

位置：

- `internal/natsconf/js_store.go:149-176`
- `internal/natsconf/js_store_failclosed_test.go:142-160`

`MoveAsideJSStore` 对 `!fi.IsDir()` 返回 `("", nil)`。配置的 JetStream store 路径存在
但类型错误，是损坏或配置错误，不是“store 不存在”。静默 no-op 会让 force-single /
grow 等破坏性生命周期继续执行，却没有检查、移动或保护该路径；这也与相邻
`JSStoreHasData` 的非目录报错策略不一致。

审查者新增 `TestMoveAsideJSStoreRejectsNonDirectoryStorePath`，当前稳定失败：
普通文件路径返回 `("", nil)`。

建议：仅 `ENOENT` 可作为 no-op；非目录、权限、I/O、broken symlink 都应带路径和补救
信息 fail-closed。

> **主进程回复（采纳，已修）** — `internal/natsconf/js_store.go`。你的红测
> `TestMoveAsideJSStoreRejectsNonDirectoryStorePath` 现已转绿。
>
> 这条我认得最痛快：**函数里唯一剩下的那个 fail-open，恰好是唯一没有测试的那个分支**。
> 相邻的 sentinel 分支、m4 的 ReadDir 分支、隔壁的 `JSStoreHasData` 全都 fail-closed，
> 只有它返回 `("", nil)`——而它的三个调用方是 force-single、grow cutover、reconcile --to-standalone，
> 都会带着「那里什么都没有」的结论继续做破坏性动作。
>
> 现在非目录带 mode 与补救信息 fail-closed（区分「散落的文件」与「store_dir 指错地方」两种处置）。
> 权限那条在本轮更早已按 m9 加了 `os.ErrPermission` 专门分支；I/O 与 broken symlink 走通用 `serr != nil`
> 分支，都已 fail-closed。**只有 ENOENT 是 no-op**，与你的建议一致。

### M6 — MEDIUM：structural golden 的“首次生成”说明不可执行，sentinel 自检没有调用被测 guard

位置：

- `test/architecture/structural_budget_test.go:541-545,585-623`
- `test/architecture/structural_budget_test.go:764-785`

缺少 golden 时，错误消息要求运行 `-update-structural-budget` 生成；更新函数却把
`prev` 设为空表，再把每个 current entry 都判成“NEW over-budget entity”拒绝。因此
文档化的恢复命令不可能成功。独立变异中移走 golden，照错误消息执行命令，结果拒绝
12 个新条目。

此外，名为“refuses to regenerate a golden whose sentinel is gone”的 subtest 从未调用
`preservedComments`，只验证 fixture 不含 sentinel 和无注释 render 的前提。删除真实
`Fatalf` guard 后该 subtest 仍可绿；日志把证明责任转交给计划中的人工 mutation。

建议：要么提供显式、审计友好的 bootstrap 模式，要么把提示改为“从 git 恢复/人工建立
账本”；用 subprocess 执行真实 update path，断言缺 sentinel 时非零退出且 golden
未改。

> **主进程回复（采纳，已修）** — `test/architecture/structural_budget_test.go`
>
> **上半**：采纳「显式 bootstrap 模式」。`firstGeneration` 时 `!known` 不再拒绝——两半本来不冲突：
> `!known` 存在是为了防止新实体被塞进**已有**账本而不经审查；没有账本时，整份文件就是被审查的对象，
> 没有什么可以被塞过去。写入时 `t.Logf` 明说产出是**提案不是已接受的基线**。
> 按你的方式验证：移走 golden → 照错误信息执行 → 生成 12 条（此前是拒绝 12 条），原 golden 未被破坏。
>
> **下半**：完全成立，而且这条是**我这轮反复在批评的形状，出现在我自己写的、专门用来防这个形状的测试里**。
> 那个 subtest 断言的两件事对任何输入都成立，它的 `t.Log` 还把举证责任转交给一次没人排期的人工 mutation——
> 我甚至在注释里写了「它不覆盖 Fatalf 那条分支」，然后就当覆盖过了。
>
> 按你的建议改成 **subprocess**：父进程种一份被扰动 sentinel 的 golden，
> 子进程（`TETHER_BUDGET_SENTINEL_CHILD=1`）真跑 `preservedComments` → 断言子进程**非零退出**、
> 失败信息含 `contains no boundary line`、且**文件逐字节未变**——最后这条是关键，
> 一个「拒绝了但仍然写入」的拒绝不是拒绝。

## 疑惑与未闭环项

1. `cluster status --watch --json` 的错误行使用顶层 `{"error": ...}`，正常行是 status
   schema。代码注释要求消费者先判断 `error`，但公开用法应明确这是异构 JSONL，
   以及错误对象是否也需要 `schema_version`；否则严格 decoder 会在暂态错误时退出。

> **主进程回复 · 采纳，已改**（`cmd/tether/cluster_wait.go`、`cluster_wait_test.go`、`docs/usage.md`）
>
> 需要 `schema_version`，而且它是 CLI 里**唯一**没有的一个机器 JSON 载荷——`jsonout.go` 开头写着
> 「monitor keys on (schema, schema_version)」，这一行自己违反了自家契约。`watchFrameError` 现在带
> `schema:"cluster_status_watch_error"` + `schema_version:1`，两个键都**非 omitempty**（在场即判据），
> marshal 失败时的手搓兜底行也带上——一个版本化 decoder 分不了类的兜底行，等于多绕几步的丢帧。
>
> 判据写进注释与文档：带 `schema` 的是没到达的帧，带 `view` 的是真报告。
>
> 另外我在核这条时发现一个**你没提、但同源且更安静**的坑：到达的 status 帧带 `errors`（**复数**，
> broker 侧问题），没到达的帧带 `error`（**单数**）。搞反的监控会把「降级但活着」读成「socket 死了」，
> 或者反过来对真故障保持沉默。`TestWatchJSONLDiscriminatorSeparatesFrameErrorsFromDegradedFrames`
> 把两个方向都钉死（错误行不许长出 `errors`、报告不许长出 `error`）。
>
> 变异验证两条：① 发射端不填 schema/schema_version → 红；② 错误行长出 `errors` 键 → 红（就是上面那条 trap）。

2. `isTransientError` / `isConfigError` 仍通过 `strings.Contains(err.Error(), code)` 分类。
   这允许人类错误文本偶然包含某个 code 后改变 fleet 控制流。建议让 wire code 作为
   结构化字段贯穿到 CLI 决策。

> **主进程回复 · 采纳，按你的建议做了结构化贯穿**（`cmd/tether/exitcode.go`、`error_hints.go`、
> `node.go`、`node_classify_test.go`）
>
> 你这条不只是「理论上可能」——同一个仓库的 `exitcode.go` 就写着
> 「The classifier never string-sniffs prose for a class — that would make a reworded message silently
> change a script's exit code」，这两个函数正是它禁止的那件事，而且它们读的 prose 是
> `brokerCodeHints` 的**运维可读文案**，本来就是会被改写的东西。
>
> 改法就是你说的：`ExitError` 加 `Code` 字段，由 `brokerErrorMessage` 在源头填（填的是
> **剥掉 `agent_rejected:` 前缀后**的形式，调用方不必知道哪些 code 是包着来的），两个分类器改为
> **精确匹配**结构化 code。字符串兜底**只留两条 Go transport/context prose**
> （`deadline exceeded` / `context canceled`）——那类错误压根没有 wire code，不给兜底就是行为倒退；
> 且只在「没有结构化 code」时才咨询，**永不覆盖**结构化判定。`isConfigError` 没有兜底。
>
> **测试也一并纠正了**：原来三个用例用 `errors.New("…code…")` 手搓错误，那实际上是在断言 **prose**，
> 对一个真实错误全错的分类器也照样绿。现在统一走生产构造器 `brokerErrorMessage`。
>
> 新增 `TestFleetClassifiersIgnoreCodeNamesAppearingInProse`，四个用例都是「prose 里种一个 code、
> 结构上是另一个（或没有）」。**这里我第一版写砸了并被自己的非空洞检查抓住**：`brokerErrorMessage`
> 在有 hint 时会用 hint **替换** detail，所以我种进去的 code 名被丢掉了，两个"对抗"用例什么都没测。
> 现在前两例改成**临时改写 hint 文案**（这才是你说的那个真实机制），并加了 `plantedCode` 非空洞断言——
> 样本里必须真的出现那个外来 code 名，否则 `t.Fatalf`。
>
> 变异验证两条：两个分类器分别退回 prose 匹配 → 分别红在不同的子用例上。

3. `nolint_directive_test.go` 对标准 linter 集合的约束也是手工集合；若配置改成
   `default: none` 或用另一个 linter 替换，精确数量未必能证明安全强度没有下降。

> **主进程回复 · 采纳前半（是个真洞，已修），后半我同意你的判断并把话说明白**
> （`test/architecture/nolint_directive_test.go`）
>
> **前半是真洞，而且原注释的方向判断是反的。** 那五个标准 linter 名原来**无条件注入**，注释还写着
> 改成 `default: none` 只会「错在**安全**方向」。恰恰相反：这个门的职责是**拒绝**指名未启用 linter 的
> 指令，多信五个 linter 是启用的会让它更**宽松**——正是不安全的方向。`default: none` 之下，
> 五条陈旧的 `//nolint:staticcheck` 会继续读作「已知情的例外」而无人报警。
>
> 现在 `default:` 从配置里**读**而不是假设（只认 `linters:` 那一层的两空格缩进，避免误读嵌套 settings）：
> `standard` 注入五个、`none` 不注入、`all` **硬失败**（这个门枚举不了 golangci-lint 的全集，
> 把 `all` 当 `standard` 只是换个马甲再犯一次同样的宽松错误）、无法识别也硬失败。
>
> **变异验证**：把 `.golangci.yml` 改成 `default: none` → `TestEnabledLinterSetIsParsedNotAssumed` 红，
> **且** `TestNolintDirectivesNameEnabledLinters` 直接点出 `internal/broker/wire_freeze_test.go` 的
> `//nolint:unused` 指名了未启用的 linter——**这正是你描述的那个洞，原实现会静默放行**。
> 还原后 `git diff -- .golangci.yml` 为空，字节一致。
>
> **后半我不打算假装机械可判。** 你说「精确数量未必能证明安全强度没有下降」——对的，一换一的替换
> 数字不动。我加了 `expectedEnabled = 22` 的计数，但在错误文案里**明写**它证明的是**可见性**不是强度：
> 改 `.golangci.yml` 必须在同一个 commit 里改这个常量，并在 message 里说明新集合为什么不更弱。
> 这跟本仓所有递减账本是同一笔交易；把它吹成「证明了强度」比留着这个洞更糟。
4. 部署级 drill 42 在重建候选镜像后连续两次得到同一
   `DRILL-VERDICT verdict=ASSERT-FAIL`（`pass=26 assert_fail=5 setup_red=1`），没有到达
   README 声明的预期 `PRODUCT-RED #47`。两次均先证明 brk2:7400 connection-refused，
   随后的产品 diagnose/force-single 却又报告 brk2:7400 接受 TCP，并在后续造成 session
   auth failure。本批只改了该 drill 中的文件名注释，`clusteroffline` 与 docker fault
   primitive 也无 diff，因此现有证据不足以把根因归给本批；但它确实意味着本批
   `MoveAsideJSStore` 的真实 force-single 部署链没有得到 green 验证。不得把这次 drill
   记录成已知 #47 的预期红。

> **主进程回复** — 先订正一处事实，再说处置。
>
> **`internal/clusteroffline` 有 diff，不是无 diff**：`init.go` 10 行、`restore.go` 6 行
> （`git diff HEAD -- internal/clusteroffline/`）。这条重要，因为 drill 42 走的正是 force-single/rejoin 这条链，
> 而你的归因推理把「无 diff」当作前提之一。
>
> 我逐个核过：两处都是 `revive: indent-error-flow` 的 else 提升
> （`if x, err := f(); err != nil {…} else {…}` → 先赋值再判 err），作用域与执行顺序均未变，**语义等价**。
> 所以你的结论方向不变——它们解释不了 ASSERT-FAIL。但**我不敢把「等价」当成不必核**：
> 本轮内审的唯一 CRITICAL（PC-1）就是一次 `revive: empty-block` 清理静默改掉了 `--json` 的分支归属，
> 形状与这两处完全一样。核过、等价、留证据，比「没 diff 所以不用看」可靠。
>
> **处置**：本机即 weilandserver，已复现并**定位到根因**。
>
> 第三次复现得到与你完全相同的 verdict（`ASSERT-FAIL pass=26 assert_fail=5 setup_red=1`），
> 5 个 assert-fail 中后 4 个都是第一个（ord 12）的下游。用 `SIM_KEEP=1` 保住环境、手工杀掉 brk2 后
> 从 **brk1 容器内部**实测：
>
> ```
> getent hosts brk2                        ->  198.18.0.58   brk2.lan
> </dev/tcp/brk2/7400                      ->  CONNECTED
> getent hosts <绝不存在的名字>             ->  198.18.0.59   <名字>.lan      ← 宿主上任何名字都解析
> pgrep -a -x mihomo                       ->  1077492 /usr/local/bin/mihomo …
> /etc/resolv.conf                         ->  search lan
> ```
>
> **根因：宿主的 mihomo fake-IP DNS。** `198.18.0.0/15` 是 fake-IP 段。brk2 容器消失后 docker 内嵌 DNS
> 不再认识该名字，查询转发给宿主 resolver，回来一个合成地址，其 TUN 设备**完成 TCP 握手**——
> 于是「已杀死的节点」在任何基于 TCP 的判活里都读作 ALIVE。你两次、我一次，三次 ASSERT-FAIL 同一个成因。
>
> 这**确认了你的归因判断**：与本批无关，也不是 tether 的逻辑缺陷。你原话「现有证据不足以把根因归给本批」是对的，
> 现在有证据了，结论一致。
>
> **处置（sim 侧，已做）**：按 Mandate「暴露而非弥补」，`lib/docker.sh` 的 `ensure_net` 前置加
> `assert_host_dns_says_no`——探一个绝不存在的名字，能解析就**拒绝开跑**并说明宿主该怎么修
> （停 mihomo / 换 redir-host / 换回会返回 NXDOMAIN 的 resolver）。`SIM_ALLOW_FAKE_DNS=1` 可越过，
> 但注释里写明：任何断言节点已死的 drill 都不该用它，那样的绿什么也没测。
> 我**没有**去钉容器 IP 或改成按地址探测——那会让 drill 42 变绿，同时让它失去测量能力，是弥补不是暴露。
>
> **同一根因下的一条产品发现（新）——已裁定并做完，不留待决**：`internal/clusteroffline/offline.go`
> 的 `probePeer` 把「TCP 连接成功」当作存活证据。在任何 fake-IP / 通配 / captive DNS 网络里，这会让
> **HARD-REFUSE 永久阻断一次合法的 force-single**——运维手里那台机器确实死了，产品却坚持它活着，
> 而且报错让人去找一台不存在的机器。
>
> **我没有把探测改"聪明"，这是刻意的裁定。** 要求 raft/TLS 握手才认活，会把失败翻到**危险**方向：
> 一个证书过期或版本 skew 的**活** peer 会被读成死的，force-single 就真脑裂了——那正是 B-8 与
> audit CC-2 建这道闸要防的事故。**拒绝是安全裁决，保持不变。**
>
> 补的是缺的另一半：**说清这次判定可能没有意义**。`untrustworthyProbeAdvice` 在**拒绝路径上**追加诊断——
> ① peer 地址解析进 `198.18.0.0/15`（RFC 2544 基准段，clash/mihomo 的 fake-IP 段，真 peer 不可能在里面）；
> ② RFC 2606 保留的 `tether-liveness-canary.invalid`（**必须**解析失败）居然解析成功 ⇒ 本机会给不存在的名字
> 编地址，此机上任何 TCP 判活都恒为 ALIVE。两个信号都是关于**宿主**的观察，**都不能把拒绝变成放行**。
>
> `TestHardRefuseNamesAnUntrustworthyResolver` 四个子用例，第二条最重要：**拒绝仍然是拒绝**；
> 第四条防的是这个诊断自己制造新的拒绝（说谎宿主上，探到死的 peer 必须照样放行）。
> 变异验证两条：C 断开 advice → 红；D 把 advice 提到判活之前当新拒绝理由 → 第四条子用例红。
>
> 至于 drill 42 本身：宿主 DNS 修好之前它测不了。所以按你第 5 条，我**不声称** JetStream reset/rejoin
> 的部署级验证已完成，也没有把这次记成已知 #47 的预期红。

## 验证记录

| 验证 | 结果 |
|---|---|
| `git diff --check` | PASS |
| `make gates`（开发者候选、加入审查红测前） | PASS；architecture、determinism、cmd/tether、auth/concurrency、lint 均绿 |
| `make test`（隔离候选副本，不含两条审查者故意红测） | PASS；含 vet-tags、Darwin build 与全包测试 |
| `make e2e-parallel`（同一隔离候选副本） | PASS；coverage 15/15，test units 36→99 |
| `go test -race -count=1 ./internal/agent ./internal/natsconf ./internal/tunnel ./internal/pty` | PASS |
| `test/simcluster/tests/run-all.sh` | PASS；hermetic/oracle/ledger/R9D 全部通过 |
| post-commit provenance mutation | FAIL，确认 B1 |
| TLS unrelated-config callback mutation | 门禁错误 PASS，确认 M2 |
| 删除 `TestD9Matrix` mutation | 门禁错误 PASS，确认 M3 |
| 目标路径 `Broker.Run` 改名 mutation | 门禁错误 PASS，确认 M4 |
| 缺 structural golden 后执行提示命令 | FAIL，确认 M6 |
| `local.sh drill 42-rejoin-returning`，重建镜像后连续两次 | 两次均 ASSERT-FAIL，未到达预期 #47 |

## 审查者新增测试

- `internal/agent/upgrade_retryability_test.go`：固定 transient HTTP status 不得进入永久
  `ErrUpgradeHTTPStatus` 分类；当前故意为红。
- `internal/natsconf/js_store_failclosed_test.go`：
  `TestMoveAsideJSStoreRejectsNonDirectoryStorePath`；当前故意为红。

这两条测试没有修改产品实现，也没有把失败隐藏进 skip/build tag。修复 M1/M5 后它们应
成为常绿回归测试。

## 上线前建议顺序

1. 先修 B1，并在“真正提交后的临时仓库”跑完整 gates。
2. 修 M1、M5，使两条独立红测转绿。
3. 修 M2-M4 的门禁假绿并把三个 mutation 固化为自检。
4. 修 M6 或纠正文档化维护契约。
5. 单独定位 drill 42 的 peer 判活矛盾；直到恢复主链能到达其声明的唯一预期 verdict，
   不应声称 JetStream reset/rejoin 的部署级验证已经完成。

---

## 主进程处置汇总（回复外审）

按你给的顺序做完，逐条回复写在各 finding 下方。**没有一条延后**。

| 项 | 处置 | 变异验证 |
|---|---|---|
| B1 | 采纳，已修：provenance 锚定到具名 commit SHA，并加两条闸内断言（SHA≠HEAD、四个路径必须已从工作树消失） | 在**真正提交后**的临时仓库里复现原缺陷 → 红；修复后绿。第二条断言在我自己一次 `rsync` 漏 `--delete` 的探针里首次真实触发 |
| M1 | 采纳，已修：408/429/5xx 走 `ErrUpgradeHTTPRetryable`，透传 `Retry-After`；exit class + fleet 分类 + hint 三处同步 | 你的 `TestUpgradeTransientHTTPStatusIsNotPermanent` 转绿 |
| M2 | 采纳，已修：按 **selector base** 配对而非按函数；`InsecureSkipVerify` 只有字面量 `false` 才算安全；站点数精确钉死 4 | 你的 unrelated-config 变异 → 红；新增 3 个合成样本 |
| M3 | 采纳，已修：`requiredSpawnedTags` 具名 7 条 + 反向半边；结构化 walk 取代 `Eval` | 删 `TestD9Matrix` → 红 |
| M4 | 采纳，已修：`{path regex, names}` 块解析 + path 限定反查 + `scoped==0` 自检 | `Broker.Run` 改名 → 红 |
| M5 | 采纳，已修：非目录 → 带 mode 与补救的报错；`os.ErrPermission` 单独分支给 chown 建议 | 你的 `TestMoveAsideJSStoreRejectsNonDirectoryStorePath` 转绿 |
| M6 | 采纳，已修：`firstGeneration` 引导模式（输出明写是 PROPOSAL）；sentinel 缺失即拒写；自检改为**子进程真跑 guard**，断言非零退出 **且** 文件字节不变 | 缺 golden 后按提示执行 → 现在可执行 |
| 疑惑 1 | 采纳，已改：错误行补 `schema`+`schema_version`；**并发现你没提的单复数陷阱**（`errors` vs `error`）一并钉死；usage.md 补异构 JSONL 判别表 | 2 条 |
| 疑惑 2 | 采纳，按你的建议结构化贯穿：`ExitError.Code` 源头填、分类器精确匹配、**测试也从断言 prose 改成走生产构造器** | 2 条；且第一版对抗用例被自己的非空洞断言抓出是空的，已重做 |
| 疑惑 3 | 前半是真洞（原注释方向判反了），已修：`default:` 从配置读，`all`/无法识别硬失败。后半我同意你——计数证明的是**可见性**不是强度，已在错误文案里明写 | 改 `default: none` → 两个门都红，且直接点出 `wire_freeze_test.go` 的 `//nolint:unused`（原实现静默放行） |
| 疑惑 4 / drill 42 | 根因定位完毕：**宿主 mihomo fake-IP DNS**，与本批无关（确认你的归因）。sim 侧按 Mandate 加**拒绝开跑**的前置守卫而非弥补；产品侧 `probePeer` 的同源盲点**已裁定并做完**——不改判活标准（改了会翻到危险方向），只在拒绝路径上说清「这次判定可能没有意义」 | 2 条 |

**四道硬闸复跑（本轮全部改动落盘之后）**：`make test` PASS · `make gates` PASS（含 vet-tags、Darwin build、lint 0 issues）·
`make e2e-parallel` **ALL PASS**（3m27s）· `go test -race ./internal/clusteroffline/` PASS。

**我仍不声称的事**：JetStream reset/rejoin 的**部署级**验证。宿主 DNS 修好之前 drill 42 测不了——
这与你第 5 条一致，我没有拿 hermetic 绿去顶部署级绿。
