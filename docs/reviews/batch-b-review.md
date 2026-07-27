# 批次 B — 内审报告（step 4）

> **流程位置**：CLAUDE.md §3 step 4。本报告由多专家对抗性 workflow 产出，主进程未修改其内容。
> **workflow**：`wf_b5651e67-932` —— 12 条 review lane + 8 条对抗性 verify lane + 1 条 synth（21 agent）。
> **模型**：全部继承会话主模型（Opus，无 `model` 覆盖），符合 CLAUDE.md §3 的模型硬约束。
> **agent 数**：每阶段静态固定（12 / 8 / 1），无一由上一阶段产出内容决定。
> **专家边界**：只读实现、可提出新增测试条目，**未修改任何实现代码**（CLAUDE.md §4）。
>
> **主进程对本报告的逐条处置写在 `batch-b-plan.md` §10（blocker + 前 18 条）与 §11（synth 的四条 major）**，
> 不写在本文件内——本文件保持专家原文，便于外审对比"专家说了什么"与"主进程做了什么"。
>
> ⚠ 报告开头专家自述的门禁状态是**他复核时刻**的树。此后主进程又依据 §11 做了三处修改，
> 最终门禁状态见 `batch-b-plan.md` §9.5（订正版）。

---

`make test` = 0, `make lint` = 0 issues, `go test ./cmd/tether/` = ok, `go test ./test/determinism/` = ok — I re-ran all four on the tree as of 14:2x. Report below.

---

# batch-B 内审报告（step 4 综合）

> 12 条 review lane + 8 条对抗性 verify lane。原始 finding 共 78 条（含重复），去重后 **41 个不同缺陷**。
> **本报告的所有裁决都由我在当前工作树上亲自复验过**（跑门、读源、AST/行号核对），不转述 lane 的结论。

## 0. 结论

**这个增量在功能与门禁上是干净的：0 blocker。** 四条 blocker 级 finding（`-race` 下审计多重集竞争、`allocIdentity` 回归测试是恒等式、`promised_guard` 变红、`make lint` QF1001）**全部在 review 进行期间已被主进程修掉并经我复验**。残留的是**四条 major 的"注释/守卫说了但没做到"**，以及约二十条 minor 的注释失真与记账缺口。

| 级别 | 数量 | 说明 |
|---|---:|---|
| **BLOCKER** | **0** | 四条原始 blocker 已修并复验；无新增 |
| **MAJOR** | **4** | 3 条只有 1–2 条 lane 发现（M-A / M-B 是 verify 阶段的新发现），1 条 6 条 lane 一致 |
| **MINOR** | **21** | 见 §3，可合并处理 |
| 被驳回 | 6 | 见 §4 |

值得单独说一句：**§10 的自纠效率是这次审查最强的信号**。M18（同一句豁免理由连错两次）是主进程自己在报告里写下的方法论订正——"我在用推断代替验证"。这句话恰好也是本报告残留 major 的共同根因。

---

## 1. BLOCKER 逐条

**无。**

四条曾经成立的 blocker 及其当前状态（我逐条复验，不是采信自述）：

| 原始 finding | 当时的证据 | 当前状态（我的复验） |
|---|---|---|
| `audit-multiset-races-handler-goroutine` | `ingress_characterization_test.go:376` 写 / `:433` 读跨 goroutine；`-race -count=20` 20 次里红 10 次 | **已修**：`:391-408` 加 `auftMu sync.Mutex`，`:415-419` 在订阅回调 `defer` 里关 `handled` channel，`:434-438` 等待，`:489-491` 持锁快照。我跑 `go test ./internal/broker/ -run 'TestIngress…'` 全绿；v1/v3/v4/v6 各自跑过 `-race` 无竞争报告 |
| `alloc-identity-regression-test-is-a-tautology`（6 条 lane 重复上报） | `leaderSide := allocIdentity(a).allocation()` 与 `forwardSide := ...` 是**同一个表达式**；revert 修复后全包仍绿（v2/v6/v7 各自在 /tmp 副本上验证过） | **已修**：`alloc_identity_test.go:152` `TestAllocIdentitySurvivesTheWireRoundTrip` 真走 `json.Marshal`→`Unmarshal`→`.allocation()`；`:212` 新增 `TestAllocationCallSitesPassTheNarrowedValue` AST 守 call site。**但守卫本身有残留缺陷，见 §2 M-B** |
| `promised-guard-red`（v1/v5/v7/v8 四条 lane 独立报，判 BLOCKER） | `alloc_identity_test.go:38` 仍点名已删的 `TestAllocIdentityIsTheOnlyProjection` → `make test` 红 | **已修**：`grep -rn TestAllocIdentityIsTheOnlyProjection --include=*.go .` 无命中；`go test ./test/determinism/ -count=1` = `ok 8.111s`；plan §10.6 记账 |
| `lint-red-admit-ordering` | `admit_ordering_test.go:96` `case !(iSubject < iGuard && iGuard < iACL):` → staticcheck QF1001 | **已修**：现为 `case iSubject >= iGuard \|\| iGuard >= iACL:`；`make lint` = `0 issues.` |

---

## 2. MAJOR 逐条

### M-A · `admit_test.go:47-51` 的注释是假的，而它守的正是 B1 的头号安全要求

**证据（我亲自核对，非转述）**

```go
// 1b. Internal review raised the obvious attack on 1: if some subject parses to an EMPTY
// verb token, the zero spec's `verb == ""` would MATCH it and the "zero denies" property
// would be accidental rather than real. It does not — proto.ParseCmdBy rejects both shapes
// below — but that is another package's behaviour, so pin it here.
for _, malformed := range []string{
    proto.SubjectPrefix + ".s.lab.cmd.by." + owner + ".node.lab-1..req", // empty verb token
    proto.SubjectPrefix + ".s.lab.cmd.by." + owner + ".node.lab-1.req",  // verb token absent
} {
    if _, _, ok := b.admit(malformed, verbSpec{}); ok { ... }
}
```

三处硬事实：

1. **`ParseCmdBy` 不拒绝第一种。** `internal/proto/subjects.go:324-334` 要求 `len(parts) == 11` 并只校验 `parts[3]`(sid) / `parts[6]`(actor) / `parts[8]`(nid)，**`parts[9]`（verb）原样返回、不校验**。`…node.lab-1..req` 恰好切成 11 段，`ok=true, verb=""`。
2. **同一个 package 的生产注释说了相反的正确的话。** `internal/broker/admit.go:140-145`：*"proto.ParseCmdBy **accepts** a well-formed subject whose verb token is EMPTY … and against such a subject the comparison `"" == ""` succeeds."* 两句话直接打架，错的那句在**认证这个属性的测试里**。
3. **这个断言不可能因它宣称的原因失败。** 循环传的是 `verbSpec{}`，`admit.go:150-152` 的 `if spec.verb == ""` 在 `ParseCmdBy` 被调用**之前**就返回了 `subject_malformed`。就算 `ParseCmdBy` 明天被放宽，这个循环照样绿。真正守线的 `admit.go:154` 的 `verb == ""` 子句**没有任何测试触及**——我 grep 过整仓：带空 verb token 的 subject 只在 `admit_test.go:53` 出现一次，且配的是零 spec；`ingress_characterization_test.go:527` 用的是 `"not-"+verb.name`，永远非空。

**失败场景**：有人读 `admit_test.go:49` 得出"空 verb 由 proto 层挡住"，于是删掉 `admit.go:154` 的 `verb == ""`（看起来是与 `spec.verb == ""` 重复的冗余）。此后一个带非空 verb 的 spec 遇到 `…node.<nid>..req` 时，`verb != spec.verb` 因 `"" != "exec"` 仍然拒绝——**这一条今天恰好安全**。但 `admit.go:150` 那条被删才是致命的，而两条守卫在注释里被描述成"由另一个 package 保证"的同一件事，删掉哪一条都会被这段注释背书。plan §4-B1 把零值-DENY 称为「B1 的头号安全要求」，`admit_test.go:23-24` 称它是「the single most important assertion about admit()」——认证它的那段话是假的。

**建议修法**：把循环改成用**非空 spec** 打空 verb 的 subject，让 `admit.go:154` 真正进入被测面：

```go
if _, den, ok := b.admit(
    proto.SubjectPrefix+".s.lab.cmd.by."+owner+".node.lab-1..req",
    verbSpec{verb: "exec"},
); ok || den.code != "subject_malformed" { t.Error(...) }
```

保留零 spec 那条作为独立断言。注释改成实话：`ParseCmdBy` **接受**空 verb token（`subjects.go:334` 原样返回 `parts[9]`），因此 `admit.go` 有**两道各自独立的守卫**（`spec.verb == ""` 与 `verb == ""`），**各自需要自己的测试**。

**发现者**：v8（唯一）。**我独立复验**：读 `subjects.go:321-335` 与 `admit.go:139-161` 的控制流，确认切分为 11 段、`parts[9]` 无校验、`spec.verb == ""` 先返回。

---

### M-B · `TestAllocationCallSitesPassTheNarrowedValue` 守的是**标识符名字**，不是推导关系

**证据**（`internal/broker/alloc_identity_test.go:212-282`）

```go
const narrowedIdent = "narrowed"
...
arg, ok := call.Args[idx].(*ast.Ident)
if arg.Name != narrowedIdent {
    t.Errorf("%s passes %q to %s, not %q.\nThat is the original defect: ...", ...)
}
```

整个判定就是一次字符串比较。测试**从不引用** `allocIdentity`、`PortFreeAllocationPayload` 或 `.allocation()`；非空洞下限（`:267` `total < 4`）数的是**被守的调用数**，不是推导数。

**失败场景**：把 `clusterwrite.go:843-844`

```go
id := allocIdentity(a)
narrowed := id.allocation()
```

改写成

```go
narrowed := a
payload, err := json.Marshal(allocIdentity(a))
```

——线上载 5 个字段、leader-local Plan 闭包与单机直写路径拿全结构体，**正是 revert 前的分歧**，而四个调用点仍然传一个叫 `narrowed` 的标识符。这条守卫、`TestAllocIdentityDropsEverythingBeyondTheFence`、`TestAllocIdentitySurvivesTheWireRoundTrip`、`TestPortFenceColumnsAreStillFive`、以及 §10.9 新加的 `TestDirectMutatorsAcceptTheNarrowedAllocation`（它用完整值与窄化值各跑一遍直写 mutator，两侧都成功即绿——`narrowed := a` 下两侧都是完整值，照样绿）**全部保持绿**。

**诚实的减轻因素，必须一起说**：真正会发生的失败（`git revert`、合并冲突解决成 `port.PlanFreeAllocation(db, a, ...)`）**是被抓住的**——那才是 plan §10.2 变异验证跑的那种。绕过需要作者**主动改绑定**。所以这不是"守卫无效"，是"守卫用名字代理了性质"。但它守的是 plan §8「永不砍」清单上**本增量唯一的真 bug 修复**，而后果（follower 路由的请求对零值做 plan）在 N=1 车队上要到扩容之后才显形。

**建议修法**：断言**推导**而非名字——在两个被守函数内遍历 `AssignStmt`，要求传给 port mutator/planner 的标识符绑定到 `allocIdentity(...)` 或其 `.allocation()` 结果；并额外要求两个函数都**不把自己的 `a` 参数传给 `allocIdentity` 以外的任何调用**。保留 `total < 4` 下限。

**发现者**：v8（唯一）。**我独立复验**：读 `alloc_identity_test.go:212-282` 全文 + `clusterwrite.go:842-856`，并核对 §10.9 新增的直写守卫同样过不了这个绕法。

---

### M-C · `wire_freeze_test.go` 的 "SCOPE, stated honestly" 段落仍然是假的，而 plan §10.5 M12 记为"已订正"

**证据**（`internal/broker/wire_freeze_test.go:51-55`，我在 14:1x 重读）

```
// SCOPE, stated honestly (testing-standards A3): this freezes the 17 verbs and the
// top-level payload types dispatchForward decodes. `proto.NodeRegisterReq` is nested
// inside ReconcilePayload and is frozen here too, because it rides the same envelope.
// Types reachable only through those (none today: every field is a scalar, a slice of
// scalars, or time.Time) are NOT walked recursively — ...
```

「reachable only through those」的类型**今天就有两个**：`internal/proto/messages.go:45-46` 的 `LocalProcesses []LocalProcess` / `LocalPorts []LocalPort` 是**结构体切片**，而 `proto.LocalProcess` / `proto.LocalPort` 正是因为只能经 `NodeRegisterReq` 到达才被冻在 `wire_freeze_test.go:121-122`、specimen 在 `:156-157`。**同一个文件 `:508-509` 亲口讲了这段历史**："This is the check that caught proto.NodeRegisterReq's own LocalProcesses/LocalPorts nesting when this file was first written; the first draft froze NodeRegisterReq and stopped, silently leaving nine keys uncovered."

**为什么算 major 而不是 minor**：plan `§10.5 M12` 写着「文件头「SCOPE, stated honestly」段落的说法与实现不符 → **随 M10/M11 一并订正**」。**没有订正。** 这落在本次审查明确列出的"§9 声称已验证但实际没有"这一类里，而且这是**同一个增量里第三次**同类事件（第一次是豁免理由 "scanned there"，第二次是 "all eight codes still emitted"，都记在 §10.10）。代码危害有限——`TestFrozenWireTypesHaveNoUnfrozenNesting` 现在机械强制这条规则、失败信息会告诉作者加什么——但记账危害是实的：外审拿 §10.5 逐条核会核到一个假的"已修"。

**失败场景**：下一个加嵌套 payload 类型的作者读文件头，得出"这里结构上不可能有嵌套"，不去找冻结项。**这正是 LocalProcess/LocalPort 第一次被漏掉的方式**，`:508-509` 自己写着。

**建议修法**：改成实话——根集合是 17 个 verb 的顶层 payload + `proto.NodeRegisterReq`（嵌在 `ReconcilePayload`）+ `proto.LocalProcess` / `proto.LocalPort`（嵌在 `NodeRegisterReq`）；`jsonKeys` 只看一层，传递性由 `TestFrozenWireTypesHaveNoUnfrozenNesting` 要求"每个可达结构体自身必须是根"来强制，`time.Time` 是唯一豁免。同时把 §10.5 M12 那行从"已订正"改成实际状态。

**发现者**：v1/v2/v3/v4/v6/v7/v8 七条 lane 一致判 stands；**v2 额外指出 §10.5 M12 的假记账**。我复验了注释原文、`messages.go:45-46`、`wire_freeze_test.go:121-122/:156-157/:508-509`。

---

### M-D · §9 偏离 2 的 T9 预算数字是**非 `-race` 的**，而 T9 守的是 `-race` 那根杆

**证据**

- `docs/reviews/batch-b-plan.md:454`：*"实测：`internal/broker` 基线 **260s**，加上两张新网后**增加约 1.5s = 0.6%**"*，结论 *"T9 的预算约束按其字面执行并通过"*。
- 但被守的命令带 `-race`：`test/e2e/all_phases_test.go:252`（D4）与 `:278`（D5）都跑 `./internal/broker/...`，且 `Makefile:64` 注释自陈 *"internal/broker is 4m37s of D4's 4m45s"*，D4/D5 传 `-timeout 300s`。
- 我实测（非 `-race`）本批次新测试合计 ~6s（`TestIngressRefusalSurface` 1.42s + `TestIngressPreDBRefusals` 0.50s + 其余）。多条 lane 在 `-race` 下实测同一组为 **26–32s**（v3 23.4s、v4 31.7s、v6 32.5s、v8 26.8s，四条独立测量互相吻合）。**即 §9 记录的数字比被守路径上的真实开销小约 20 倍。**

**失败场景**：`make e2e-parallel` **不受影响**——`test/e2e/parallel` 用 `-split -shards 8` 对 `internal/broker` 做名字分片，§9.5 记录 ALL PASS。受影响的是 **`make e2e-one T=TestD4Matrix`**，即 `CLAUDE.md §5` 指定的**唯一合法的串行定位路径**：4m37s 基线 + ~30s 对 `-timeout 300s` 已无余量。也就是说，并行门报出某个 broker 测试红时，用来定位它的那条路可能自己先超时。

**诚实的界定**：我**没有**重跑 `-race` 全包基线（每侧约 7 分钟×2），所以 "+6.9%" 这个比例是 lane 的数字，我只复验了分子（新测试的 `-race` 耗时）与分母来源（`Makefile:64` 的 4m37s）。另外 v5 有理由地把它降为 minor：§9 确实白纸黑字写了"按其字面执行"，所以这不是谎报，是 T9 自己的验收行与其论证段互相矛盾。**我仍判 major**，理由是可操作的那一半——§9 里那个数字会被下一个增量当基线用，而它错了一个数量级。

**建议修法**：用门实际跑的命令重测一次（`go test -race ./internal/broker/`），把 §9 的 0.6% 换成 `-race` 数字；并二选一：(a) 抬高 D4/D5 的 `-timeout`，(b) 采纳下面 §3 第 21 条把 48 次 `openDB` 降到 ~18 次，(c) 在 §9 写明 `make e2e-one T=TestD4Matrix` 对本包不再适用、定位改用 `-run` 过滤分片。顺带把 T9 的验收行文字改成带 `-race`，它的论证段本来就是这么说的。

**发现者**：v3/v4/v5/v6/v7/v8 六条 lane。

---

## 3. MINOR 汇总

以下 21 条我都在当前树上核对过 file:line，全部**成立**，全部**不影响任何门**。按类合并：

**(a) 注释点名不全 / 指错地方（7 条）**

1. `internal/broker/admit.go:164-165` — *"admit runs the whole prologue in one call, for the handler (upgrade)"*，但 `expose.go:169` 也调 `b.admit(msg.Subject, exposeSpec)`，而 `admit_ordering_test.go:52-59` 的 `combinedAdmitAllowed` **两个都列了**并写明 expose 的理由。同一 package 两处说法打架，错的那处正是这次拆分的安全说明。
2. `internal/broker/clusterwrite.go:839` — *"freePortAllocation routes an expose-rm free"*，实际两个调用点：`expose.go:396`（expose-rm）与 `expose.go:317`（`rollbackExposeAllocation` 的补偿性 free）。旁边 `:904-907` 那条对 `revokePortAllocation` 的说明是准确的，反衬更明显。
3. `internal/broker/admit.go:20-21` — *"broker.go:1320 records that a session-scoped ingress has already shipped MISSING it once"*。`broker.go:1320-1326` 是一段 C.1 §6 的规则陈述 + 反事实（*"Without this, a tombstoned session could get a fresh nodes row…"*），**没有日期、没有事件、没有归属**。这是 admit.go 给出的**唯一**存在理由（plan §1.1 同句）。v3 找到了真出处：`git log -S` → commit `16a8530 "Address P8 review: register DELETING gate (F1)"`——改成引这个即可，论据反而更硬。
4. `internal/broker/admit_ordering_test.go:41` — *"broker.go:1005-1013 records the incident behind that"*。MAJ-2 写在 `broker.go:1001-1005`，`:1006-1012` 是三条 proxy.sub 订阅项，且 MAJ-2 讲的是 `.proxy.sub.*` 通配，与 upgrade 无关。**同源的 `upgrade.go:37` 已经修对了**（现在写 `broker.go:1001-1006` 并明说 *"that one was about the `.proxy.sub.*` wildcard, not about upgrade"`），漏了这一处。
5. 四处行距全错：`run.go:94` 说 kill 在 run "forty lines" 处、`admit.go:31` 说 kill 在 exec "twenty lines below it in the same file"、`ingress_characterization_test.go:184/:192` 说 "twenty lines"。实测 `handleRunReq` = `run.go:21`、`handleKillReq` = `run.go:84`（63 行），`handleExecReq` = `exec.go:32`（**不同文件**），run 的拒绝审计在 `run.go:34`（距 kill 注释 60 行）。四处无一正确。建议**直接删掉距离**、改引标识符+文件，这类声明每次编辑都会腐烂。
6. `alloc_identity_test.go:27 / :205 / :259` 仍写 "13-field"，而**同一文件** `TestFixtureIsActuallyFullyPopulated` 断言 `NumField() >= 14` 且失败信息说 *"the fixture and allocIdentity's godoc both assume 14"*。`cluster_forward.go:158` 已改成 "fourteen"。文件自相矛盾。
7. `internal/broker/clusterstatus.go:979-982` — logSkew 的 godoc 说 *"several unit fixtures construct one without it"*。`grep -rn 'ClusterAdmin{'` 全仓恰好 5 个复合字面量（`r8_home_delivery_test.go:642/:677`、`b6_skew_test.go:38`、`join_version_gate_test.go:287/:331`），**五个都设了 logger**；唯一的生产构造 `clusteradmin.go:154 NewClusterAdmin` 在 `:151-153` 做 nil 兜底。`join_version_gate_test.go:87-90` 复述同一假说法。**注意：v3 指出"删掉 logSkew 直接 log.Warn"这条建议是错的**——`join_version_gate_test.go:92/:95` 显式传 nil 给 `versionSkewRefusal`，删了会 panic。只能改注释，不能删分支。

**(b) 运维可见输出未记账（1 条）**

8. `clusterstatus.go:963` / `:973` 两条 `logSkew` 仍以 `"cluster add:"` 开头、并写 *"(older `cluster add`?)"*，而 `cluster_operation_controller.go:60-62` 现在从 **`cluster join approve`** 路径调 `versionSkewRefusal`。当前车队上每个 pre-B4 二进制铸的 bundle 都是 `ProtoVer==0`，**每次 approve 都会打这一行**，把运维指向一个不可能产生它的命令（今天的 `cluster add` 总是 shell 出当前二进制的 `join prepare`）。§9 没有把这两行列为新增输出。`ErrJoinVersionSkew.Error()` 被 `TestJoinVersionSkewMessageIsUnchanged` 逐字节冻住，**不要动**。

**(c) 测试的自我描述大于其实际断言（6 条）**

9. `ingress_characterization_test.go:52-56` 的 SCOPE 只按 **verb** 划界。六个已转换 verb 内部未覆盖的码：`store_error`（`admit.go:275 storeErrDenial`，admitACL 里三个调用点）、`json_parse`、`marshal`、`forward_failed`、expose-rm 的 creator 分支；且全部行都是单机模式，没有任何 cluster-role 短路。这些界限写在了 `admit.go:264-274` 与 plan §9，**不在读者会打开的那个文件里**。
10. plan §4-B1（`batch-b-plan.md:207`）要求非逐字节 verb 钉 *"code + audit 多重集 + **日志行**"*。文件里每个 broker 都是 `Logger: silentLogger()`（`:373/:523/:556/:717`），无任何 slog 断言。§9 的两条偏离与 §10 都没记这一条。实话可能是"这六个 handler 拒绝时不打日志，所以没有行可钉"——**那句话本身值得写进 §9**，因为 plan §5 决策 #1 一旦落地就会创造出这条日志行。
11. `wire_freeze_test.go:461-470` 的信封冻结**只有单向**（遍历 `envelopeSpecimens()`），没有对 `frozenEnvelopeKeys` 的反向环——删掉一个 specimen 会让它的 golden 静默孤立。payload 那半边是双向的。另外 `:512 roots := payloadSpecimens()`，**信封被排除在嵌套遍历之外**，而它包着全部 17 个 verb。
12. `wire_freeze_test.go:636-680` 的 `TestWireFreezeScannerSelfCheck` 自称证明 *"the two AST scanners"* 非空洞：const 那半边正确地调共享谓词 `verbConstsFromAST`，**arm 那半边是 inline 重写的 `ast.Inspect`**，从不调 `scanDispatchArms`——`scanDispatchArms` 独有的 `fd.Name.Name != "dispatchForward"` 过滤与 `return false` 停止下降**都没被自检**。同一文件 `:275-277` 明写 *"a self-check against a re-implementation proves nothing"*。M10 把这条纪律施加给了嵌套遍历，漏了 arm 扫描器。风险由 `len(arms) < 10` 下限兜住，所以是文档准确性问题。
13. `wire_freeze_test.go:171-192` 的 `jsonKeys` 没有 `f.Anonymous` 分支：匿名嵌入结构体会被记成**类型名**，而 `encoding/json` 会提升其字段。`TestJSONKeyDeriverSelfCheck` 覆盖 tag / 无 tag / `-` / opts / 未导出，**不覆盖嵌入**。今天没有冻结类型用嵌入，潜伏。
14. `join_version_gate_test.go:337-345` 把任何非 `*ErrJoinVersionSkew` 的失败降级成 `t.Logf`，而它的 doc（`:327-328`）声称能挡住"拒绝一切"的门。今天确实在 admit（我跑过，无 fallback 行），但任何新增的前置条件在 `d7SingleNode` fixture 上开始失败，就会把文件里唯一的 ALLOW 路径断言变成静默 no-op 且仍报 PASS。

**(d) 结构性潜伏（2 条）**

15. `admit.go:161` 的 `admitSubject` 在**只解析、零授权**之后返回一个填满的 `ingress` + `ok=true`，而 `admit.go:85-86` 仍写 *"The gate is the `ok` return, NEVER this struct"*——现在有两个 `ok`，只有一个是门。唯一的结构保护是 `admit_ordering_test.go:43-48` 的四个硬编码 handler 名 × `:62` 的四个硬编码文件；`:58` 的 `"admit": true` 是**死项**（admit.go 不在 `files` 里）。六个现有调用点我逐一核过全对，所以是潜伏而非活缺陷；但 commit 9 的目标 `sessions.go`/`proxy.go` 不在 `files` 里。**注意 v7 的正确警告**：把 `admitSubject` 改成返回不透明 `parsedSubject` 的方案会撞上 `admit.go:79-86` 与 `TestAdmitReturnsIdentityOnLateRefusal` 要求的"拒绝时也要有 populated ingress"契约。低风险做法是把 `files` 扩到所有含 `handle*Req` 的文件、并对任何不在 `shortCircuitGuarded` 里的 `admitSubject` 调用报错。
16. `admit.go:93-95` 的 `ingress.status` 是**只写字段**：`grep '\.status'` 在 admit/exec/run/expose/upgrade 五个生产文件里只命中 `admit.go:231 ing.status = status`。它的 doc 面向"设置了 skipNodeCheck 的调用者"——那类读者不存在——且**在每一次拒绝上它都是空的**（包括 node_offline，那时状态只活在 `den.detail` 里），doc 没说。

**(e) plan 记账（4 条）**

17. `batch-b-plan.md:91`（§2 边界 8）与 `:380`（§6 T4）都写 `legacyMissingGuards`「今天 33」；`test/determinism/promised_guard_test.go:16` 自陈 *"There are 34 of them"*，字面量 34 条，门跑起来也打印 34。门不断言计数所以不红，但这是**要求外审去数**的硬边界。
18. `batch-b-plan.md:428` 写「✅ **4 个变异**」后面只列了三个（verb 值改名 / 新增未冻结 verb / 给未打 tag 的结构加 json tag），`×2` 数的是"同时变红的测试数"不是第四个变异。§10.7 M13 之后确实又多了一个真变异，可能改写这一行比改数字更合适。
19. `batch-b-plan.md:454` 的 T9 数字（见 §2 M-D）。
20. `broker.go:545-548` 的假注释（*"so broker.go itself does not import internal/cluster"*，而 `broker.go:32` 就 import 了）仍在树里。plan §0.2 说本批次"顺手修掉"，§9 只在「3 / 13-15 未开工」那行里隐式处置。**v3 的反驳有道理**：plan §3.2 把它明确编进了 commit 13，交叉引用能查到，所以这是记账不显眼而非丢失。改法是一行注释删除，不碰任何门（我确认 T3 的禁词是另一个串 `PlanAllocateProxy(b.cfg.DB`）。

**(f) 成本（1 条）**

21. `ingress_characterization_test.go:372` 在 6×6 循环里每个 subtest 开一个 SQLite（36 次），`:523`/`:556` 再各 6 次，共 **48 次 `openDB`**。其中 `TestIngressPreDBRefusals` 的 12 次是**纯浪费**——`subject_malformed` 在 `admit.go:151/155` 返回、`actor_invalid` 在 `:183` 返回，都在第一次 `session.IsActive`（`:189`）之前。这是 §2 M-D 的可操作解药。**采纳时必须一并改 `:394` 的审计 tap subject 过滤**（它闭包捕获了包级 `const sid = "lab"`）——v7 明确警告过：漏改会让每个 audit 多重集变空、所有 `audit: nil` 期望空洞通过，**静默退回 §9.4 刚修好的状态**。

---

## 4. 被驳回的 finding（这一节和 §1 同等重要）

### 4.1 `charnet-never-crosses-standing-with-node-state` —— **不成立**

finding 说"没有任何东西钉住 admitACL 里 standing-before-node 的相邻顺序"。**假的。** `internal/broker/admit_test.go:157` 把 `lab-1` 种成 `STALE`，`:188-192` 随后用一个全新的非成员 actor 走 `b.admit(..., execSpec)` 并断言 `auditRefusal(d2) == "not_a_member"`——**这一行恰好交叉了"standing 失败 × node 失败"**。v3 做了决定性验证：在 `/tmp` 副本里把 `node.LookupStatus` 提到 role switch 之前，`admit_test.go:192` 立刻红，打印 `non-node audit rendering changed: "node_offline:STALE"`。

**lane 分歧与我的取舍**：v2/v5/v6/v7 判 stands，但**四条都只查了 `ingress_characterization_test.go`**；v1/v3 去找了覆盖并找到了，v3 还跑了变异。**有证据的一侧胜过缺证据的一侧**，驳回。窄化后仍成立的那句——"表征网自己不覆盖这条相邻关系"——是对的，但性质已被覆盖，且加一行 scenario 的收益不大（还要一并设 `rawGolden`、抬 `:682` 的 `n < 6` 下限并改它的失败文案，否则消息与门不一致）。**列入"可以不修"。**

### 4.2 `single-mode-direct-path-narrowed-with-no-tripwire` 的**后果描述**不成立

finding 声称给 `updateAllocationState` 加第六个 fence 列会"全仓无一测试变红"。v3 施加了 finding 自己给的变异（`AND local_port=?`），结果 **`go test ./test/p6/...` 大声变红 4 条**（`expose rm: free_failed port: row not found`、rollback 未回滚、`port revoke event never arrived`）。所以那条路径不是"覆盖最差"，而是被 p6 端到端覆盖着的。

底层的覆盖缺口（绊线只跑两个 `Plan*` 入口）是真的，**且主进程已按 §10.9 M15 加了 `TestDirectMutatorsAcceptTheNarrowedAllocation`**（我确认存在于 `alloc_identity_test.go:350`）。另外 finding 给的备选修法"把单机窄化改回 `a`"**今天会打红** `TestAllocationCallSitesPassTheNarrowedValue`——不能采纳。

### 4.3 `s9-omits-a1-gate-edit` / `section9-inventory-stale` —— 陈述的危害不成立

finding 说"从 §9 出发的审查者不会被提示去打开 `error_code_coverage_test.go`"。§9.1 的 T1 行**整行就是讲这个文件**（12 个键 +8、理由串一字不改、6 个新豁免），§9.2 点名 `admit_ordering_test.go` 并解释它为什么存在。产出文件列确实还没同步，但那是格式问题不是隐藏变更。v3/v5/v6 一致 drop。

### 4.4 `b4-drill-not-recorded` / `b4-drill-not-accounted` —— 不成立

§9.5 已记录：`DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=49`，`S6-42 rejoin-returning`，本机直跑（符合 CLAUDE.md §5 的"本机就是 weilandserver 就别 ssh"），并写明 B1/B2 不起 drill 的理由。八条 verify lane 一致 drop。

### 4.5 `tripwire-guidance-unreachable-on-the-natural-mutation` 的**副命题**不成立

主命题成立（见 §3 未列——实为 minor：`alloc_identity_test.go:302-309` 的四个 `err != nil` 分支都是裸 `t.Fatalf`，`:311-317` 的决策指引够不到）。但 finding 附带说 "plan §9 记录的变异结果我复现不出来" ——v3 证明：**只改 baked UPDATE**（不改 SELECT）时两条 SQL 都会打印、完整指引都会出现，这大概率就是 §9 记录的那次。**§9 的记录是可信的**，只有指引可达性这一半成立。

### 4.6 关于 `admit-codes-invisible-to-a1-gate` 家族的一点澄清

这一家六条 finding 在 review 阶段全部成立，之后被修了**两次**——第二版又是假的，由 verify 阶段三条 lane 各自用变异独立证伪（§10.10 M18 已记账）。**当前第三版是对的**：我核对 `cmd/tether/error_code_coverage_test.go:190-215` 明写 *"node_offline and node_not_found have NO scanner-visible emitter left anywhere"*，并核对 `grep` 结果确认——`transfer.go:1021/1027` 的裸 `return` 是 form 5，扫描器自陈不覆盖。补偿断言 `TestAdmitRefusalCodesAreClassified`（`error_code_coverage_test.go:714`）**直接从码表断言 exit class，不依赖扫描器找到发射点**，这是正确的关闭方式。**不再是 finding。**

---

## 5. 已核查且干净的面（界定本次审查的边界）

以下面**被至少两条独立 lane 或我本人**核过，当前树上干净：

**门禁（我在 14:2x 亲自跑，非转述）**
- `make test` → **EXIT=0**（含 `./test/determinism/`、`./test/p6/`、`./internal/port/`）
- `make lint` → **`0 issues.`**（golangci-lint v2）
- `go test ./cmd/tether/ -count=1` → **ok 69.6s**（`TestErrorCodeCoverage` / `TestAllowlistEntriesStillHaveEmitters` / `TestExternalReviewUnresolvedCodeExemptionsAreSiteScopedAndLive` / `TestCodeCarryingHelperListIsComplete` 全绿）
- `go test ./test/determinism/ -count=1` → **ok 8.1s**
- `internal/broker` 本批次新测试 37 条全 PASS（我用 `-run` 过滤跑，5.8s）

**review 期间被修好并复验的面（原 finding 成立，现已闭合）**
- `admit()` 三态渲染：`admit_test.go:147-194` 现在真驱动 `b.admit` 打 STALE 节点，逐字节断言 `code`/`detail`/`reason`/`auditRefusal` **并检查三者互不相等**——旧版是"断言 `deny()` 会赋自己的参数"。
- exec/run 逐字节 golden：`rawGolden` 已实现（`:245-255` 声明、六个 scenario 全设、`:479-487` 读），`node_offline: status=OFFLINE` / `...=STALE` 现在被钉死。多条 lane 的变异（去掉 `status=`）现在会红。
- `TestAdmitRefusalCodeSet`（`admit_test.go:218`）：AST 重推导 `admit.go` 的 `deny()` 首参、`calls < 8` 下限、双向集合相等。plan §5 决策 #7 的要求已兑现。
- 零值 verbSpec 现在真 DENY（`admit.go:150-152` + `:154`）。**注意 §2 M-A 是关于认证它的注释，不是关于这个修复本身。**
- 嵌套遍历：`unfrozenNestedFields` / `reachableStructTypes` 抽成共享谓词，live check 与 self-check 调同一函数；Map（键与值）与 interface 都覆盖了。
- payload 冻结完整性：`TestEveryDispatchedPayloadTypeIsFrozen`（`wire_freeze_test.go:369`）从 `dispatchForward` 的 AST 重推导每个 arm 解码的类型，含 `< 10` 下限与 import 别名映射。**六条 lane 判它 stands 是因为它们读树时它还没落地。**
- `roleGate ↔ broker.go 订阅表`对账：`admit_subscription_reconcile_test.go` 已落地（`TestEverySubscribedVerbIsGatedOrExempt`，`< 12` 下限，双向反向检查，14 条带理由豁免，另有提取器自检 `TestSubscriptionScanSelfCheck`）。**同样是六条 lane 读树时还没有的东西。**
- `b6_skew_test.go`：已加 21 行文件头说明它驱动的是产品不可达的 `handleAdd`、活路径覆盖在哪、为什么保留（`TestVersionSkewRejectBeforeNonceBurn` 的"拒绝不烧 nonce"是新文件没有的性质）、以及"绿了不能推出 grow 路径有版本闸"。§10.7 M14 记为对 plan 字面的**有意偏离**并给了理由。
- `cluster join prepare` 的铸造半边：`TestJoinPrepareStampsTheDeclaredVersion`（`join_version_gate_test.go:256`）+ 非空洞检查。
- `fullyPopulatedAllocation` 的 `RebuildOff` 盲区：已设 `true`，并加 `TestFixtureIsActuallyFullyPopulated` 反射断言每个字段非零 + `NumField() >= 14`。
- "PlanAllocate 已 fence 在 Epoch" 假注释：`cluster_forward.go` 与 `alloc_identity_test.go` **两处都已撤回**（后者是主进程按线索自己找出的同源第二处）。
- MAJ-2 引用：`upgrade.go:37` 已改对（见 §3 第 4 条，只剩测试文件那一处）。

**我核过且本来就干净的面**
- `internal/port/updateAllocationState`（`port.go:381-387`）与 `planAllocationStateChange`（`plan.go:45-70`）今天 fence 在**完全相同的五列**，窄化在行为上是安全的。
- `ErrJoinVersionSkew` 的拒绝串被 `TestJoinVersionSkewMessageIsUnchanged` 逐字节冻住，本次未动。
- `error_code_coverage_test.go` 的 12 个 clusterstatus 键整体 +8，**理由串逐字节未变**（多条 lane diff 过）；upgrade.go 键已重算到 `:45` 并与活站点一致。

---

## 6. 覆盖缺口

按重要性排序。**第 1 条是本轮最大的结构性缺口。**

1. **约六个新文件/新测试在所有 12 条 review lane 与多数 verify lane 之后才落地，因此几乎没有被对抗性审过。** 具体是：`admit_subscription_reconcile_test.go`（全新文件，含 14 条豁免理由）、`TestDirectMutatorsAcceptTheNarrowedAllocation`、`TestAdmitRefusalCodesAreClassified`、`TestEveryDispatchedPayloadTypeIsFrozen`、`b6_skew_test.go` 的新文件头、`error_code_coverage_test.go` 的第三版豁免理由。我只做了浅读（读了订阅提取器与豁免表全文、确认下限与双向反向检查存在、跑绿），**没有对它们做变异**。而这个增量的历史记录是：同一处解释性文字连错两次。**这些新增守卫应当进下一轮，不能算已审。**
2. **`ungatedSubscriptionLeaves` 的 14 条豁免理由无人核实。** 每一条都是"这个 verb 不走 admit() 是安全的，因为 X"。X 有没有一条是假的——尤其 `register`（*"IsActive 是第五个检查，在 proto_mismatch 之后"*）与 `session.rm`（*"故意无 IsActive"*）——**没人查过**。这正是 B1 存在的那条边界。
3. **`make e2e-parallel` 在当前树上没人跑过。** §9.5 记录 ALL PASS，但那是在上述六项落地**之前**。我跑了 `make test` + `make lint`，**没跑并行全矩阵**。这是 CLAUDE.md §5 唯一的全矩阵闸门。
4. **`-race` 下的真实包耗时无人在最终树上测过。** lane 的 26–32s 是在少两三个测试文件时测的；`admit_subscription_reconcile_test.go` 与三个新守卫是 AST/内存的（我实测 `TestSubscriptionScanSelfCheck` 0.00s），影响应当可忽略，但没验证。
5. **cluster 模式的拒绝面完全没人看。** 表征网 48 行**全部单机**，`admit.go:136-138` 自陈 follower 短路"没有测试能看到它"。`admit_ordering_test.go` 只从 AST 证明顺序，不证明行为。若 commit 9 或 transfer/proxy 族后续进来，这块仍是空白。
6. **B4 的门与真实滚动升级的交互只被一次 drill 覆盖**（`42-rejoin-returning`，已记录 GREEN）。该 drill 的两个容器跑同一二进制，所以走的是 happy path；**真正的偏斜路径（老 `join prepare` 铸的 bundle 打新 leader）在部署层从未被跑过**——只有 hermetic 覆盖。考虑到 racknerd 是**唯一 broker**，一次错误的 approve 拒绝没有第二个投票者可以 failover，这是有意识承担的风险，值得在 §9 写明是"有意识承担"而不是"已覆盖"。
7. **没人审 `docs/` 的用户可见面。** 本增量改了 `cluster join prepare` 的 bundle 内容（多两个字段）与 `cluster join approve` 的拒绝语义，`usage.md` / `cluster-runbook.md` 有没有需要跟着动，12 条 lane 里没有一条的维度是文档同步。

---

## 7. 给主进程的处置建议

### 必须修（外审之前）

| # | 项 | 理由 |
|---|---|---|
| **M-A** | `admit_test.go:47-51` 的注释 + 把空 verb 子句真正纳入测试 | 假注释直接坐在授权边界上，且与同 package 的 `admit.go:140-145` 互相矛盾；`admit.go:154` 目前**零覆盖**。修法是加一条断言、改一段话，无风险 |
| **M-C** | `wire_freeze_test.go:51-55` 的 SCOPE 段落 + 把 plan §10.5 M12 的"已订正"改成实况 | 这是"§10 声称已验证但没有"，外审逐条核会核到假记账。纯文本编辑，不碰任何门 |
| §3-8 | `clusterstatus.go:963/:973` 的 `"cluster add:"` 前缀 + 在 §9 记为新增运维输出 | 当前车队上**每次 `cluster join approve` 都会打**，指向一个不可能产生它的命令。给 `versionSkewRefusal` 加一个 op 标签即可（三个调用点，编译期检查）。**不要动 `ErrJoinVersionSkew.Error()`** |
| §3-17/18 | plan §2.8 与 §6 T4 的 33 → 34；§9 line 428 的「4 个变异」 | 都是外审被要求"自己数一遍"的硬边界与可审计记录，数字对不上会让整张表失信。各一个字符 |

### 应该修（这一轮内，但不阻塞外审）

| # | 项 | 理由 |
|---|---|---|
| **M-B** | `TestAllocationCallSitesPassTheNarrowedValue` 改成断言推导而非名字 | 守的是 plan §8「永不砍」清单上唯一的真 bug 修复。承认减轻因素：**真实的 revert 是被抓住的**，绕过需要主动改绑定。所以是"应该"不是"必须" |
| **M-D** | 用 `go test -race ./internal/broker/` 重测并订正 §9 的 0.6%；同时处理 `make e2e-one T=TestD4Matrix` 的余量 | 数字错了一个数量级、会被下个增量当基线。最省事的组合是"重测 + 抬 D4/D5 的 `-timeout`"；若愿意花力气，§3-21 的 SQLite hoist 能真正买回余量（**务必一并改 `:394` 的 tap subject 过滤**） |
| §3-1/2/3/4/5/6/7 | 七条注释失真（admit 的 upgrade-only、freePortAllocation 的单调用者、broker.go:1320 的伪归属、admit_ordering 的 MAJ-2 引用、四处行距、三处 13-field、logSkew 的假 fixture 说法） | 单独看每条都无害，**合起来是 batch-A 的复发模式**：本批次的 stated goal 之一就是删假注释。全部是文本编辑，一次扫完。logSkew **只改注释不改分支**（`join_version_gate_test.go:92/:95` 显式传 nil） |
| §3-9/10 | 表征网 SCOPE 加"码级"边界；§9 加一行"日志行未钉及原因" | §9 自己的规则是「凡是 plan 里写了而本节标为未做的，都必须给出理由」，这是第三条未记的偏离 |
| §6-1 | **把 review 之后新落地的六项交给下一轮对抗性审查** | 本报告明确不覆盖它们；考虑到"同一句话连错两次"的历史，让它们直接进外审是这轮最大的敞口 |
| §6-3 | 外审前跑一次 `make e2e-parallel` | §9.5 的 ALL PASS 是六个文件之前的记录 |

### 可以不修（写下理由即可）

- **§4.1 的 not_a_member × OFFLINE 新 scenario**：性质已被 `admit_test.go:188-192` 覆盖（v3 变异验证）。加行的收益是"让表征网自身可读"，代价是要同时设 `rawGolden`、抬 `:682` 下限并改失败文案。commit 9 扩表时顺手加即可。
- **§3-11/12/13（信封反向环、arm 扫描器自检、jsonKeys 嵌入）**：三条都潜伏、都有下限兜底、都无现网可达路径。建议**合并成 wire freeze 的一条后续叶子**，不塞进本增量。
- **§3-14/15/16（soft-pass、admitSubject 非门、ingress.status 死字段）**：全部潜伏。第 15 条的正确修法是扩 `admit_ordering_test.go` 的 `files` 与 `shortCircuitGuarded` 判据，**不是** v1 提的 `parsedSubject` 重构——那会撞 `TestAdmitReturnsIdentityOnLateRefusal` 的契约。第 16 条如果是留给 commit 9 的，注释里说一句就行。
- **§3-20（`broker.go:546` 假注释）**：plan §3.2 已把它编进 commit 13，交叉引用可达。若愿意，这是一行删除、零门风险，顺手做也无妨。
- **§3-所有 tripwire 指引可达性**（`alloc_identity_test.go:302-309` 的裸 Fatalf）：绊线仍然会红，只是消息质量下降。把四个错误分支路由到同一条指引是一个 helper 的事，但不紧急。

---

**一句话给外审**：这个增量的实现面是干净的（0 blocker、四个门全绿），残留问题全部集中在**"注释/守卫宣称的东西比它实际做到的多"**这一个类别上——而这恰好是本增量自己宣称要消灭的类别。§10 的自纠记录（尤其 M18 那句「我在用推断代替验证」）说明主进程已经认出了这个模式；§2 的四条 major 是同一模式的最后四个实例。