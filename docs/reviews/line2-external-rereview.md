Pass

# 线二 · 质量闸门加固 — 独立外部复审最终报告

日期：2026-07-30

## 结论

**放行当前完整工作树**，即“已暂存的开发者候选快照 + 暂存区外的审查者修复”。
没有剩余 Blocker / Major。开发者候选单独仍是 Fail，证据冻结在
`docs/reviews/line2-external-rereview-candidate.md`；若只提交暂存区、漏掉当前
`git diff`，B1/B2/M1 会重新出现，本 Pass 不成立。

上一轮 M3/M4/M5/M6 的开发者修复经独立变异成立；本轮发现的 2 个 Blocker、
1 个安全 Major 和 2 个 Medium 已直接修复并由职责命名测试/变异闭环。

## 审查者修复

### 1. CI provenance 历史深度

- `.github/workflows/ci.yml` 的 `build-test` checkout 增加 `fetch-depth: 0`。
- `TestCIProvidesHistoryForDeletedRegressionProvenance` 固定该前提；删除配置后精确门禁转红。
- 在真正提交后的完整临时仓库中，历史 provenance gate PASS。

这修复了真实 GitHub Actions 默认 depth=1 下冻结 commit 不可达、四次 `git show`
必然失败的问题。

### 2. simcluster fake-DNS preflight

- NXDOMAIN 的 `getent` 非零退出显式 `|| true`，避免 `set -euo pipefail` 在读取空输出前终止。
- 非 DNS 职责的 hermetic `cmd_up` 测试显式使用 override，避免真实宿主 DNS 污染
  signal/temp-cleanup 测试。
- 新增 `dns-preflight-test.sh`，固定三向契约：
  honest NXDOMAIN→PASS、fabricated answer→REFUSE、显式 override→PASS。
- 新测试接入 `tests/run-all.sh`。

真实宿主仍返回 `198.18.0.0/15` fake-IP；零节点 `local.sh up` 正确以 rc=1
fail-closed。没有为了得到 drill 绿色而绕过守卫。

### 3. TLS 门禁安全配对

- callback 字面量 `nil` 不再被视为验证；composite literal 与 assignment 两条扫描路径一致。
- selector base 改为完整 AST 渲染，保留 index 和 call arguments；
  无法渲染的表达式按位置形成独立 bucket，不再共享 `"?"`。
- synthetic self-check 新增：
  nil callback、`configs[0]`/`configs[1]`、`getConfig(0)`/`getConfig(1)`。

修复后，把真实 raft client callback 改成 nil，门禁转红；把 callback 洗到另一个
indexed config，门禁也转红。精确生产站点数仍为 4。

### 4. HTTP retry taxonomy

421 `Misdirected Request` 与 425 `Too Early` 加入 `ErrUpgradeHTTPRetryable` 具名集合、
审查者状态表与 operator hint。501 保持永久边界。

421 的标准语义允许换连接重试；425 的标准用途就是触发重试：
[RFC 9110 §15.5.20](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.20)、
[RFC 8470 §5.2](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.2)。

### 5. offline liveness advice DNS budget

- 两次诊断性 lookup 改用带 context 的 resolver。
- 每次 lookup 具名预算 250ms；超时/错误只取消附加 advice，不改变原
  `HARD-REFUSE`。
- 新测试让 resolver 等待 `ctx.Done()`，证明安全拒绝在有限时间内返回。

### 6. structural golden 二次读取 fail-closed

`preservedComments` 现在只有 ENOENT 可作为首次生成；权限、I/O、wrong-shape
等读取错误直接拒绝。它不再可能在未读取手写 justification 的情况下按空账本继续写。

## 独立确认成立的开发者修复

- required build-tag 账本：删除整个 D9 runner 后转红。
- golangci path-scoped registry：只改名目标 `Broker.Run` 后转红。
- JetStream 非目录 store：fail-closed 红测转绿，只有 ENOENT no-op。
- structural golden bootstrap：缺文件时可生成 proposal；缺 sentinel 子进程拒写。
- watch JSONL：错误帧有 schema/version，`error` 与状态帧 `errors` 不混淆。
- fleet 分类：`ExitError.Code` 结构化传播，prose 中出现其他 code 不改变控制流。
- nolint 默认模式：从配置读取，`default:none/all/unknown` 不再按错误安全方向猜测。

## 验证结果

| 验证 | 最终结果 |
|---|---|
| `git diff --check` / gofmt / shell `bash -n` | PASS |
| focused architecture / CLI / agent / natsconf / clusteroffline | PASS |
| `make gates` | PASS；lint 0 issues |
| `make test` | PASS；含 310s broker 长测 |
| affected packages `go test -race -count=1` | PASS |
| `make e2e-parallel` | ALL PASS；15/15、99 units、3m23s |
| simcluster `tests/run-all.sh` | ALL PASS；含新增 DNS preflight |
| post-commit provenance gate | PASS |
| 删除 CI `fetch-depth:0` mutation | 正确 FAIL |
| TLS callback=nil mutation | 正确 FAIL |
| TLS indexed-object laundering mutation | 正确 FAIL |
| D9 runner 删除 / scoped `Broker.Run` 改名 mutation | 正确 FAIL |
| 当前宿主真实 fake-DNS preflight | 正确 REFUSE，rc=1 |

## 疑惑与限制

唯一未宣称的证据仍是 drill 42 的部署级绿色：当前宿主 resolver 会为不存在的名字生成
`198.18.0.0/15` fake-IP，前置守卫因此正确拒绝运行。使用
`SIM_ALLOW_FAKE_DNS=1` 强跑会让“节点死亡”断言失去意义，所以没有这样做。

这不是本次放行中被隐藏的产品失败：此前 drill 的矛盾已定位到宿主 resolver，
产品侧仍保持保守 HARD-REFUSE，并在 resolver 不可信时给出有预算的诊断。待宿主恢复
NXDOMAIN 语义后，建议补跑 drill 42，作为部署证据增强；不应把当前 hermetic 绿色冒充
该部署证据。

## 暂存边界

- **已暂存**：开发者候选、复审 tasklist 的候选阶段、候选 Fail 报告。
- **暂存区外**：本报告、上述全部审查者代码修复及其新增回归测试。

该边界是有意保留，便于直接用 `git diff` 审阅审查者修复。对比完成后，应把暂存区外
内容一并纳入最终提交；否则最终结论退回候选报告的 Fail。

---

## 主进程回复 — 逐条复核审查者的 6 项修复

按你留出的 `git diff` 边界逐条读完并**独立复验**（不采信报告里的结论，自己注入变异）。
**6 项全部采纳**，其中 3 项修的是我代码里的真实缺陷，我在下面点名；另外我在其上**改了 2 处**。

| 你的修复 | 我的复核 |
|---|---|
| 1 · CI `fetch-depth: 0` | **真实缺陷，我的。** B1 用 `git show <frozen-sha>` 取历史，`actions/checkout` 默认 depth=1 下那个对象不可达，CI 必然红——而我只在本地验过，本地永远是全历史。**我在其上改强了**，见下。 |
| 2 · simcluster `\|\| true` | **真实缺陷，我的，而且比报告写的更严重。** NXDOMAIN 时 `getent` rc=2，`set -euo pipefail` 下赋值就终止——也就是说我那道守卫在**诚实的宿主上会让每个 drill 当场死掉**，恰好是它该放行的那些机器。hermetic `cmd_up` 三处显式 override 也对：那些测试测的是 temp 清理与信号，不该依赖宿主 resolver。`dns-preflight-test.sh` 三向契约（NXDOMAIN→PASS / 伪造应答→REFUSE / override→PASS）已接入 `run-all.sh`，我跑了全套 15 项 ALL PASS。 |
| 3 · TLS 门禁配对 | 采纳。字面量 `nil` 当验证是我的漏，`selectorBaseName` 共享 `"?"` 桶让两个不可渲染表达式互相洗白也是。**我独立变异复验**：把真实的 `internal/cluster/transport.go:100` 的 `VerifyPeerCertificate` 改成 `nil` → 门精确点名 `clusterTLSConfigs (line 96)` 变红，还原后绿。 |
| 4 · 421 / 425 | 采纳。RFC 9110 §15.5.20（421 可换连接重试）、RFC 8470 §5.2（425 的存在就是为了触发重试）都站得住；501 留在永久侧是对的。 |
| 5 · DNS 预算 | **真实缺陷，我的。** 我用了裸 `net.LookupHost`，一个黑洞 resolver 能把这条**安全拒绝**吊在 OS 解析超时上（数十秒）。250ms × 2 的具名预算 + 让 resolver 卡在 `ctx.Done()` 的新用例是对的。我在 16 个 CPU 打满的负载下连跑 14 次无 flake。**我在其上补了一处**，见下。 |
| 6 · structural golden 二次读取 | 采纳。我把**任何**读错误都当"首次生成"，权限/IO 错会让它在没读过手写 justification 的情况下覆写。只放行 ENOENT 是对的。 |

### 我在你的修复之上改的 2 处

**a) `internal/clusteroffline/offline.go:553` 补 `// ctx-none:` 标注。**
CLAUDE.md §5 规定新增的 `context.Background()` 站点必须在那一行写明落在哪一类（`contextcheck` 被判
REJECT-FOREVER，注释就是唯一载体，存量 39 处不追溯）。你新加的这个站点没标。它属第 2 类——
`ForceSingleOptions` 结构上没有 ctx、offline 工具在 daemon 停机时运行，所以那条预算是它唯一的界，
这也正是它该是具名常量而非继承 deadline 的原因；已把这句写在站点上。

**b) `TestCIProvidesHistoryForDeletedRegressionProvenance` 改为按「谁真的跑测试」判定，而不是按 job 叫什么。**
你的版本断言**名叫 `build-test`** 的 job 带 `fetch-depth: 0`——要求是对的，但挂错了主语。
我实测了这个缺口：把 `make test` 原样搬到 `lint` job（`build-test` 仍带 `fetch-depth: 0`），
**你的版本全绿，CI 会红**。现在改成解析 `jobs:` 下所有 job，凡是跑
`make test` / `make gates` / `go test ./...` 的都必须带 `fetch-depth: 0`；
并加了 `checked == 0` 反向断言——CI 里没有任何 job 跑全量测试时硬失败，
因为那意味着 provenance 闸压根没在 CI 里跑，而这条检查是唯一会说出这件事的东西。
（作用域仍是对的：`make lint` 与 `make e2e-parallel` 都够不到 `test/architecture`，
`test/e2e/parallel` 显式枚举各 phase 套件，不该被要求深历史。）

三条变异全部确认：删 `fetch-depth` → 红；把 `make test` 搬到别的 job → 红（**旧版会绿**）；
CI 里彻底不跑 `make test` → 红。`ci.yml` 每次变异后按备份还原，`git diff --stat` 确认字节一致。

### 复核后的四道硬闸

`make test` PASS · `make gates` PASS（lint 0 issues）· `make e2e-parallel` ALL PASS ·
`go test -race ./internal/clusteroffline/` PASS · simcluster hermetic `tests/run-all.sh` **ALL PASS（15 项）**。

同意你「唯一未宣称的证据是 drill 42 的部署级绿色」这一句，原样保留：宿主 resolver 恢复 NXDOMAIN 语义后再补跑，
不拿 hermetic 绿冒充部署证据。
