Pass

# Batch B independent external re-review

Date: 2026-07-27
Reviewer: independent external reviewer
Base: `main`, `HEAD=52d3b80`
Boundary: the staged Batch B tree plus the developer's 16-file response delta. This review did not
trust the response's conclusions: each claimed disposition was checked against the requirements,
production call graph, behavioral tests, and the prescribed gates before any reviewer repair.

## Verdict

**PASS — the final working tree is approved for release, provided the unstaged reviewer repairs
listed below are included.**

The developer correctly removed `tools/rescue.py` from the release, repaired the direct network
forward observations, made declared release mismatches fail closed, fixed the B4 response-driven
retry fixture, formatted the tree, and materially strengthened the wire-scanner tests. Those changes
resolved most of the first review. The independent re-review then found two reachable contract
bypasses and one load-sensitive D7 harness failure. Per the user's explicit authorization, those
were repaired only after this review had concluded and the developer snapshot plus the original
`Fail` form of this report had been staged. Focused, race, full unit, lint, and 99-unit E2E gates are
now green.

The index intentionally still represents the pre-repair developer snapshot and therefore contains
the `Fail` form of this report. The `Pass` applies to the current working tree; promoting the staged
snapshot without the unstaged repairs would reintroduce R1–R3.

## Findings resolved after review

### R1 — MAJOR — leader-local provision and join writes bypass the outcome counter

The response defines the metric denominator as every network forward plus every leader-local
`Propose`. `Forwarder.Forward` now observes all network sends, and `proposeOrForward` observes its
leader-local branch. However, `NewProvisionSeam` and `NewJoinSeam` call `node.Propose` directly on a
leader and return without either boundary. Therefore `provision/*` and `join/*` remain absent for
leader-local attempts while the new tests exercise only their network paths.

Required fix: report the result of both direct leader-local `Propose` calls through the same
single-outcome observer and add a real-leader behavioral test that proves exactly one outcome per
attempt.

Resolution: `Forwarder.observeOutcome` is now the common observer for network forwards and the
provision/join leader-local branches. A real single-node leader test drives both seams and requires
exactly one classified result for each attempt. PASS.

### R2 — MAJOR — omitted version declarations still bypass the mandatory exact-version gate

Declared protocol or release mismatches now return typed `version_skew`, but protocol `0` and an
empty release are still warned and admitted. `docs/requirements.md §6.7` requires exact
major.minor.patch equality, and `docs/testing-standards.md §S1` requires missing or unparseable
compatibility evidence to fail closed. A legacy or stripped bundle supplies no evidence of equality;
accepting it preserves the most direct bypass of the new gate.

The response's compatibility rationale does not override the WHAT authority. An old joiner must be
upgraded and its bundle regenerated before admission, just as an explicitly mismatched joiner must.

Required fix: return typed `version_skew` for both missing declarations before nonce/op/raft writes,
retain decoding compatibility, and cover the zero-value decisions and live no-write path.

Resolution: missing protocol or release declarations now return typed, actionable
`ErrJoinVersionSkew`; old bundles still decode, then fail closed. Decision-table and real-raft tests
cover both omission and mismatch before operation/roster writes. Unrelated join-controller fixtures
were updated to mint exact current versions rather than weakening the gate. PASS.

### R3 — GATE — loaded D7 force-single setup can lose leadership between seed operations

`make e2e-parallel` failed `D7:test/d7/ForceSingleRecoverRestart`: after adding peers, the fixture
writes its premark through `nodes[0]` without re-establishing that node as leader and received
`node is not the leader`. The targeted subtest passed under `-race -count=10`, so this is
load-sensitive, but a required full gate is red and the failure is consistent with the fixture's
known leadership-movement model.

Required fix: use the response-derived/current-leader retry discipline for this seed write (or
explicitly re-establish the fixture invariant), then rerun the full matrix.

Resolution: the premark is committed through whichever node currently leads and the fixture waits
until the force-single survivor has applied it. The subtest passed under `-race -count=20` and in the
subsequent loaded 99-unit E2E matrix. PASS.

## Minor observations

- `.gitignore` ignores the entire `/tools/` directory to exclude one unsafe local rescue script.
  This can silently hide future product or security-sensitive tools; ignore `/tools/rescue.py`
  specifically.
- The wrapper inventory scanner discovers future structs only when their names end in `Envelope` or
  `Reply`. That makes its “every wrapper” claim naming-convention dependent. Inventory all relevant
  unexported wire structs or maintain an explicit, completeness-checked exemption mechanism.
- The Makefile's formatting probe redirects `gofmt` diagnostics and reasons only about non-empty
  stdout. A formatter execution/parse failure should be reported as a gate failure in its own right.

Resolution: the ignore was narrowed to `/tools/rescue.py`; wrapper discovery now inventories every
unexported struct in the forwarding source rather than suffixes; and the Makefile now captures and
reports `gofmt` execution failures separately. PASS.

## Final verification

Developer snapshot evidence retained:

- Focused broker regression suite: PASS.
- Relevant broker `-race -count=10`: PASS.
- `make lint`: PASS, `0 issues`.
- `make test`: PASS.
- `make e2e-parallel`: **FAIL**, 98/99 units passed; the sole failure was
  `D7:test/d7/ForceSingleRecoverRestart`.
- Isolated `go test -race -tags d7_integration ./test/d7
  -run 'TestD7Matrix/ForceSingleRecoverRestart$' -count=10`: PASS.
- Deploy tier was not repeated before this verdict: the preceding independent review built the
  current-source image and passed `10-grow-to-3` with 19 assertions; this response did not alter the
  matching-version join path.

Post-repair evidence:

- Leader-local metrics, strict version decisions/live no-write, wrapper inventory, and affected
  join-controller fixtures: PASS.
- Relevant broker paths under `-race -count=10`: PASS.
- D7 `ForceSingleRecoverRestart` under `-race -count=20`: PASS.
- `make lint`: PASS, `0 issues`.
- `make test`: PASS; `internal/broker` completed in 272.028s.
- `make e2e-parallel`: PASS, 99/99 units, 15/15 top-level matrices represented, 2m54.243s.
- Current-working-tree simcluster image build: PASS; pinned `nats-server` 2.10.22.
- Simcluster `10-grow-to-3`: GREEN, 19 assertions, zero gaps. Production `cluster add`
  successfully reached N=3, then retained quorum reads and writes after killing one follower.
- `git diff --check` and staged-diff check: PASS.

## Doubts and recommendations

- Release-version fields remain unsigned. That is acceptable only as an operator-error preflight,
  not an authenticated compatibility/security statement; no later feature should treat them as
  adversary-resistant evidence.
- The broad generic retry fixture may temporarily classify any non-`NotLeader` response as
  leadership movement. Keep retries limited to explicit transient leadership outcomes so genuine
  operation failures remain visible.
- The release-version declarations remain an operator-error guard, not adversary-resistant evidence.
  If authenticated compatibility becomes a security premise, introduce a versioned signed-bundle
  format rather than silently changing `JoinSignBytes` and invalidating existing DR bundles.
- Release only after the unstaged reviewer repair set receives the intended next-stage treatment;
  the index alone is deliberately the pre-repair audit snapshot.

---

## 主进程对外审修复的审核（step 5，第二轮）

外审这次**直接改了实现**（在暂存区外）。按流程我逐条核实其正确性，而不是因为结论是 Pass 就照收。

### R1–R3：三条都成立，实现正确，采纳

| # | 我核实了什么 | 结论 |
|---|---|---|
| R1 | `NewProvisionSeam` / `NewJoinSeam` 的 leader 分支直接调 `node.Propose`，**既不过 `Forward` 也不过 `proposeOrForward`** —— 读代码确认，这是我上一轮的真实遗漏：我把"五个直接发送方"数成了五个 `fwd.Forward` 调用点，漏了两个 `node.Propose` 调用点。`observeOutcome` 作为共同观察者是对的形状（nil 接收者/nil observer 都安全），且不会双计（leader 分支走 `observeOutcome`，follower 分支走 `Forward` 内的同一个钩子） | **正确** |
| R2 | 查了 `docs/testing-standards.md §S1` 的原文：**"fail-closed：空值、缺失、无法解析，一律按'不安全'处理"** —— 引用准确。这意味着我上一轮那句"未声明 ≠ 不匹配，把缺少声明当成检测到不匹配是伪造信号"**与项目自己的安全规范直接冲突**：§S1 说的正是缺失即按不安全处理。我的"旧 joiner 无法遵守"也站不住——遵守方式就是升级 joiner 并重新生成 bundle，而这恰好是 §6.7 要求的 | **我错，外审对** |
| R3 | `ForceSingleRecoverRestart` 的 premark 通过 `nodes[survivorIdx]` 直写，与 B4 完全同类（观测后假设仍成立）。修法沿用同一条纪律，且额外要求**survivor 真的 applied** ——那一步是对的：这个 seed 的用途是证明 restart 后状态被精确保留，在别的 leader 上提交成功对 survivor 会重放什么毫无证明力 | **正确** |

**R2 的落地前提我另外单独验了**，因为它现在承载整个 grow 流程：`cmd/tether/cluster_join.go:117` 确实
stamp 了两个字段，且有 `TestJoinPrepareStampsTheDeclaredVersion` 守住 mint 侧（含非空洞检查）。
没有这个 stamp，门是对的但**现行工具永远无法满足它**，grow 直接不可用。
外审的 post-repair simcluster `10-grow-to-3` GREEN 是端到端证据。

### 我在外审的修复里改了三处

1. **`TestEveryForwardWrapperTypeHasAnEnvelopeSpecimen` 的豁免机制原本只存在于注释里。**
   注释写着"add an explicit reviewed exemption rather than weakening discovery"，但**没有那个机制**——
   下一个人在 `cluster_forward.go` 里加一个纯实现用的私有 struct，会拿到一条没有正当解法的失败，
   只能去改测试。这正是 §A4「注释里承诺的东西必须存在」的形状。补上 `offWireForwardStructs`
   （带理由字符串），并加**反向检查**：stale 条目、以及"声明为 off-wire 却又有 specimen"两种情况都要红。
   **变异验证**：把 `forwardReply` 错误地豁免 + 加一个不存在的 `goneAway` → 两条各自精确变红。
   今天这张表**是空的**，那是诚实的状态：该文件里两个非导出 struct 都确实在 wire 上。
2. **`name[0] >= 'a' && name[0] <= 'z'` 换成 `ts.Name.IsExported()`。** 手写的大小写判断对
   `_foo` 或 Unicode 小写名会漏；标准库已经有这个谓词。
3. **d7 helper 硬编码 `c.nodes[0]`，而调用方传的是 `survivorIdx`。** 今天 `survivorIdx := 0` 所以巧合正确，
   但 helper 的 godoc 自己说"waits until nodes[0] has applied"——一旦调用方换 survivor，
   它会静默地等错节点。改成参数，并把"为什么是参数"写进 godoc。

### 一处报红：无法归因，如实记录，未声称修好

`make test` 的**第一次**运行里 `test/p13/TestProxyFalseOnlineRecoversAfterTunnelDrop` 失败一次。
之后**没能复现**：
- 干净 `make test` 一次 → PASS（exit 0）；
- 隔离 `-count=3` → PASS，`-count=12` **在 `make e2e-parallel` 满载同时** → PASS；
- 整包 `-count=8`（同样满载中）→ PASS；
- 整棵 `go test ./test/...`（同样满载中）→ PASS。

第一次的失败输出没有捕获到（第二次就绿了），所以**失败在哪一步未确认**。
机制上与本轮改动无关：p13 是单 broker phase 测试，不走 forward 计数器、不走版本门。

**做了一处有据的加固，但明确不是"已修"**：该测试的第 (1) 步注释里明确记着超时从 5s 提到 15s
是为了负载，而第 (2) 步留在 10s、`waitTCP` 留在 8s —— 而第 (2) 步等的活**严格更多**
（agent 重连 backoff + tunnel 握手 + REGISTER 往返 + broker 写 proxy_ready），第 (1) 步只等**掉线检测**。
把便宜的等待按满载机器定尺、把更贵的按空闲机器定尺，是反的（§T1）。两处都提到 15s。
它**不可能掩盖产品缺陷**：断言是"最终恢复"，永不自愈仍然会红，只是晚一点。

### 最终门禁（当前工作树，含我这三处修改）

| 闸 | 结果 |
|---|---|
| `make lint` | **0 issues**（含 gofmt 闸） |
| `gofmt -l ./cmd ./internal ./test` | 空 |
| `make test` | **全绿** |
| `make e2e-parallel` | **ALL PASS**（2m56s） |
| 另有两次满载 e2e（作为 p13 归因的负载源） | 均 ALL PASS |

未重跑 simcluster：我这三处改动只碰测试守卫与一个 d7 fixture 参数，未触真实部署栈；
外审的 post-repair `10-grow-to-3` GREEN 仍然适用。
