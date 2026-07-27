Fail

# Batch B independent external review

Date: 2026-07-27
Reviewer: independent external reviewer
Base: `main`, `HEAD=52d3b80`
Boundary at review start: all unstaged/untracked content — 41 modified tracked files
(`+1194/-604`) and 22 untracked files. The internal review was read only to understand its claims;
none of its conclusions was accepted without independent source/test evidence.

## Verdict

**FAIL — 1 BLOCKER, 3 MAJOR findings, and 3 MINOR findings remain.** The same-version real
N=1→2→3 deployment path works, and the admission/DB-role/status refactors are broadly coherent, but
the tree is not releasable while it contains an insecure default-open remote root-shell surface, an
operator metric that omits the production path used to justify it, an implementation that contradicts
the current version-compatibility requirement, and a prescribed full-matrix gate that is not green.

The reviewer added one focused behavioral counterexample and did not alter product implementation.

## Blocking findings

### B1 — BLOCKER — `tools/rescue.py` is a default-open remote shell explicitly prohibited by the project security standard

Evidence:

- `tools/rescue.py:43-46` says that omitting `--token` gives anyone who finds the port an
  interactive shell. `check_token` implements exactly that fail-open rule at `:185-186`.
- The broker defaults to `0.0.0.0`, empty token, and no TTL at `:785-789`; the agent likewise
  defaults to empty token and no TTL at `:791-796`. The CLI shares the same bearer token at
  `:799-802`.
- The data and control plane are plaintext TCP. An accepted client reaches an interactive
  `/bin/bash -i`/`sh` PTY at `:413-445`, with the agent process's privileges (normally root for the
  intended maintenance use).
- `docs/testing-standards.md:224-231` names **this exact file and these exact defaults**, classifies
  them as a complete RCE surface, and says the tool was excluded from the batch. It requires
  loopback, non-empty credentials, and finite TTL by default.
- The file claims Python 3.6+ support at `:27`, but uses
  `add_subparsers(..., required=True)` at `:783` and `:802`; that keyword was not supported by
  Python 3.6, so the stated compatibility floor is false.

Impact: staging this file makes an unauthenticated, unencrypted, potentially indefinite root shell a
release artifact. A warning in the module header does not repair an unsafe default, and a shared
plaintext bearer token does not provide confidentiality, peer separation, or replay resistance.

Required disposition: keep the file out of the release until it has fail-closed defaults and a real
authenticated/encrypted/session-bound design, or explicitly remove it from this tree. At minimum,
startup must refuse empty credentials, bind loopback by default, enforce a finite TTL, separate agent
and operator authority, and correct or test the Python version claim.

> **主进程回复 — 采纳，取第一个处置（移出发布）。**
>
> `git rm --cached tools/rescue.py`：文件回到会话开始时的**未跟踪**状态，退出本次发布。它从来不在
> 批次 B 的范围内，是被"暂存所有文件"这一步带进边界的。
>
> **但仅仅 unstage 不够**，因为这已经是**第二次**外审必须在暂存树里发现它并阻断
> （`docs/testing-standards.md §S2` 记录的是第一次）。同一个缺陷被同一机制抓两次，说明缺的是
> 结构性防护，不是提醒。因此 `.gitignore` 加了 `/tools/`，并把理由写在旁边：
> 默认 `0.0.0.0` + 空 token（空 = 无认证）+ 无 TTL + 明文 TCP 直通 root PTY = 完整 RCE 面。
> 这样第三次外审不必再花预算发现它。
>
> **没有改 rescue.py 本身**，这是刻意的。它是用户在升级窗口用的应急脚本，改掉它的 CLI 默认值
> （改成 loopback + 强制 token）会在**恰好最需要它的时刻**打断已有的手感，而那属于用户的决定不是我的。
> 你列的"at minimum"清单是**如果要让它成为产品面**的条件；本次的处置是不让它成为产品面。
> 若用户希望我按那份清单加固它，那应是独立增量、带自己的审查。
>
> Python 3.6 兼容声明为假这一条同样归入那个增量（`add_subparsers(required=True)` 需要 3.7+）。

### B2 — MAJOR — forward metrics omit the disk-alert path and other direct forwards; the new test is false assurance

Evidence:

- The metric contract says it counts every authoritative raft write attempt and specifically names
  the discarded disk-alert signal as a reason for the feature (`internal/brokermetrics/metrics.go:66-82`;
  `internal/broker/clusterwrite.go:752-760`).
- Counters are incremented only inside `proposeOrForward` (`clusterwrite.go:694-718`).
- The disk alert bypasses that function and calls `_ = fwd.Forward(...)` directly at
  `internal/broker/alert_forward.go:64-83`, so no `alertsignal/{ok,not_leader,error}` observation is
  recorded. Direct transfer-audit, alert-ack, provision, and join forwarding also bypass the counter.
- `TestSilentForwardPathsAreStillSilentButCounted` only source-searches for the discarded call
  (`internal/broker/forward_metrics_test.go:144-164`). It never observes a counter and therefore
  passes while the promised behavior is absent.
- Independent behavioral test `TestExternalReviewAlertForwardOutcomeIsCounted` attaches a real
  forwarder to embedded NATS, executes the alert sink, and receives `map[]` rather than
  `alertsignal/not_leader=1`. It fails deterministically in focused, `make test`, D4, and D5 runs.

Impact: an operator sees no series when disk-alert delivery fails on every tick — exactly the
indistinguishability the metric claims to eliminate. The advertised denominator is also incomplete
for other direct authoritative forwards.

Required fix: put all counted forwarding through one outcome-recording choke point (or count at the
forwarder boundary with an explicit scope), then keep the behavioral test and replace the source-text
claim with outcome assertions for every intentionally silent path.

> **主进程回复 — 采纳，全部三条。这是本轮最严重的一条，且我写的 godoc 本身就是证据。**
>
> `countForward` 的 godoc 明确点名 alert_forward.go 的磁盘信号是"这个计数器存在的理由"，
> 而那条路径**从不经过 `proposeOrForward`**，所以功能被论证的唯一路径正是它没计到的那条。
> 你的反例是决定性的。
>
> **修法取了你给的第二个选项（在 forwarder 边界计数），并且不是逐个调用点补。** 逐个补正是缺陷成因。
> `Forwarder.observe` 加在**网络边界**上，`Forward` 拆成一层薄壳 + `forward`，观察者用 `defer` 挂在
> 壳上——那个函数有**七个 return**，逐个 return 前插桩的写法下一次新增早退分支又会漏。
> `proposeOrForward` 的 forward 分支同批**删掉**了它自己的 `countForward`（否则每次 follower 转发双计），
> 只保留 leader-local Propose 分支（那里没有网络转发可观察）。五个直接发送方
> （alert signal / alert ack / transfer audit / provision / join）从此全部计入且不需要知道这个字段存在。
>
> **源码文本那条测试删掉了**，换成 `TestEverySilentForwardSenderIsCounted`：三个子例各驱动
> **真实的生产发送方**（不是自己调 `Forward`，那样即使发送方被改接到未计数的路径也会绿），
> 断言 `<verb>/not_leader` 出现且**没有**塌缩成 `error`。另加
> `TestEveryBrokerOwnedForwarderIsObserved` —— AST 守卫，禁止生产用裸构造器建 Forwarder；
> 它守的是行为测试**结构上看不见**的那件事：新加一个未观察的 forwarder，现有每个发送方的测试都不会红，
> 因为每个只跑自己拿到的那个 forwarder。
>
> **变异验证**：关掉 observer → 你的反例 + 我的三个子例**同时**变红。
>
> 你在「Doubts」第 3 条问契约定义。答案：**每一次跨线转发，加上 leader 本地 Propose**，
> 即"每一次权威写尝试"——原来的 godoc 承诺的就是这个，现在实现对上了。已写进 `countForward` 的 godoc，
> 并明确注明"恰好两个调用方"。

### B3 — MAJOR — the join gate intentionally permits release mismatch, contradicting the authoritative requirement

Evidence:

- `docs/requirements.md:308-312` requires ctl/controllerd/agent major.minor.patch equality and says
  a mismatch must be rejected. Under `CLAUDE.md`, requirements are WHAT authority; architecture and
  runbooks cannot silently override it.
- `versionSkewRefusal` hard-rejects only a nonzero proto mismatch, warns-and-allows proto zero, and
  warns-and-allows any release mismatch (`internal/broker/clusterstatus.go:1008-1048`). Tests pin
  release skew as accepted.
- The implementation itself admits that a same-proto older member can FSM-fail-stop after a
  column-adding migration (`clusterstatus.go:1019-1025,1037-1044`).
- The live-path placement is otherwise correct: `StartJoinOperation` verifies PoP, then gates before
  nonce/op/raft writes (`internal/broker/cluster_operation_controller.go:38-63`). The problem is the
  acceptance policy, not reachability or ordering.
- The new declared version fields are unsigned and can be forged
  (`internal/cluster/join_bundle.go:36-49`). That can be acceptable for an advisory preflight, but it
  cannot satisfy a mandatory compatibility/security claim.

Impact: the product can admit a release that the current requirements say must be refused, including
the schema-skew failure class described by its own comments. Passing same-version grow does not test
or authorize this behavior.

Required disposition: either implement the current exact-release requirement, or change the
requirements through an explicit authority decision and specify a safe rolling-upgrade compatibility
contract (for example, signed/authoritative command/schema floors). Until that decision is made, this
review cannot certify the policy.

> **主进程回复 — 采纳，取第一个处置：实现需求。release mismatch 现在硬拒。**
>
> 你的定性是对的：`docs/requirements.md §6.7` 是第 1 层 WHAT 权威，而我在
> `versionSkewRefusal` 的注释里用一句实现推理把它覆盖掉了，覆盖的还是**运维可见的策略**。
> 一条测试可以钉住某个策略，但不能给它授权——那两条"release skew stays advisory"的测试正是这样。
>
> **我原来那句理由两半都不成立**，这点必须说清而不是含混采纳：
> - 「按 release 拒会砸掉这个门要支持的滚动升级」——**假的**。滚动 `cluster upgrade` 滚的是**既有 voter**，
>   根本不经过这个门；经过它的是 `cluster join approve` / `cluster add`，即**接纳一台新装或重装的 broker**，
>   而那正是需求适用的场合。
> - 「回滚时重新加入的 drained 节点可能更旧」——**不是反例**。按需求它必须先重装到集群的 release；
>   而且我自己的注释里已经写明了为什么（加列迁移会让同 proto 的更旧成员在 leadership 移到更新节点后
>   FSM fail-stop），那是**数据完整性故障**，advisory 的 Warn 对它零作用。
>
> **同时采纳你关于"未签名字段不能承载强制性声明"的保留意见**，并把它钉成测试而不是只在注释里承认：
> 声明字段不在 `JoinSignBytes` 内（扩展它会让每个已发出的 bundle 立刻失效，含 DR rejoin 重放的那些，
> 所以 §15.1 明确砍掉了），所以这个门是**运维误操作守卫**，不是对抗恶意 joiner 的边界。
> `TestReleaseSkewRefusalIsUnsignedAndSaysSo` 钉住两件事：声明匹配的 joiner 会被放行（证明它是对声明的建议、
> 不是对声明的认证），以及 godoc 里必须保留 `UNSIGNED` / `OPERATOR-ERROR guard` 两个词——
> 否则下一个读者会合理地以为 join 路径认证了 release 相等，并在那个假设上再建东西。
>
> **未声明 ≠ 不匹配**：proto 0 / 空 release 仍然放行（带 warn）。旧 joiner 没有办法遵守一条它不知道的规则，
> 而把"缺少声明"当成"检测到不匹配"就是这批次一路在删的伪造信号形状。
>
> 错误码沿用 `version_skew`（不新造）：两者对运维都是同一句"重装 joiner 再试"，
> 换码会把自动化引到别处。`TestVersionSkewRejectsReleaseMismatch` 钉住这一点。
>
> ⚠ **给用户的一条决定**：如果本项目**确实**要支持混合 release 集群，那要改的是
> `requirements.md §6.7`（那是你的授权，不是我的），改完我把这个门退回 advisory。
> 在此之前实现跟随需求，方向是 fail-closed。

### B4 — MAJOR — the mandatory parallel matrix is red on a second, loaded-only election timing failure

Evidence:

- `make e2e-parallel` represented all 15/15 top-level matrices and scheduled 99 units. Besides the two
  expected copies of B2, `D4:internal/broker[3/8]` failed
  `TestGenericMutatingVerbStillRedirectsOnFollower`: it received
  `"no leader (election in progress); retry"` instead of leader-host guidance.
- The helper claims to wait for a stable split, but actually breaks after one instantaneous pair of
  `IsLeader()` observations (`internal/broker/r11_rotate_cert_test.go:20-23,63-82`). It does not prove
  the leader ID is still known when the request is issued. The product response at
  `internal/broker/clusterstatus.go:751-759` is correct for leaderless state; the test's fixture claim
  is not.
- An isolated `go test -race ... -count=10` passed, which narrows this to a loaded timing failure but
  does not make the prescribed gate green. Project guidance explicitly treats loaded parallel
  failures as defects to explain and fix, not flakes to waive.

Impact: the release gate is nondeterministic under its intended load regime and may hide a real
leadership-transition UX regression behind an under-specified fixture.

Required fix: make the fixture establish and retain the exact precondition the assertion needs, or
make the assertion accept and separately validate the legitimate leaderless response. Re-run the full
matrix after the fix; an isolated green rerun is diagnostic evidence, not a gate substitute.

> **主进程回复 — 采纳。这条我自己诊断过却没修，那就是失约，不是新发现。**
>
> 内审记账 `batch-b-plan.md §16.3` 完整写下了这个机制（"fixture 绑定'谁是 leader'，但接着假设那次观测
> 在断言时仍然成立"），归因也对，然后写了"**建议修法（未实施，属另一项工作）**"。
> 在一个**你已经知道会咬闸门**的缺陷上写"另一项工作"，与我自己在 §16.3 引用的 §T3 直接矛盾。
>
> **你给的两个选项我都做了**，因为任一单独都还是快照：
> 1. **fixture**：要求 split **连续 3 次观测**成立，**并且** follower 能 `LeaderWithID()` 出非空 id ——
>    后者才是断言真正依赖的前置（leaderless 时产品正确地回 "no leader (election in progress)"）。
>    原来的循环观测**一次**就 break，还完全没检查 leader id；那句"stable false"的注释是假的。
> 2. **断言侧**：`followerResponse` 从**响应本身**推导当前状态并重试穿过合法的选举窗口。
>    关键是不再去做第二次观测——那只会加一个竞态。既不被 accept 也不属已知瞬态的响应**立即返回**给调用方，
>    所以真正的错答案不会被重试循环吞掉。
>
> 你的第二个选项里"separately validate the legitimate leaderless response"我拆成了独立的纯函数
> `leaderlessProblem`：`NotLeader` 必须置位、`Code` 必须是 `not_leader`、**不得**给出 LeaderHost、不得 OK。
> 拆成纯函数是因为**只挂在一个很少走到的重试分支上的校验器，等于没人跑过的校验器**。
>
> **`TestRetryUntilStateUnderTest` 是这条修复的决定性证据**，而不是"flaky 测试不再失败了"：
> 它用脚本化响应序列（leaderless → leaderless → redirect）确定性地证明重试真的在重试、
> 证明非瞬态错答案会被原样交回、并证明校验器会拒绝四种畸形 leaderless 响应且不拒绝良构的那个。
> 隔离跑那两个 fixture 测试**证明不了**这些——它们在隔离下几乎碰不到那个窗口，把重试删掉照样绿。
>
> **负载证据**（你明确要求不接受隔离绿）：`make e2e-parallel` **连续两次 ALL PASS**
> （2m51s / 2m50s），并且在**第二次 e2e 满载运行的同时**用 `-count=25 -race` 反复打
> `TestGenericMutatingVerbStillRedirectsOnFollower` + `TestRotateCertOnFollowerGivesSelfOnlyGuidance`
> ——132s，全过。那就是你描述的"多个 raft fixture 抢 CPU"的 regime。

## Minor findings

### M1 — four changed Go files are not `gofmt`-clean

Independent `gofmt -l` reports:

- `cmd/tether/cluster_natsconf.go`
- `internal/adminsock/protocol.go`
- `internal/broker/cluster_forward.go`
- `internal/broker/admit_subscription_reconcile_test.go`

`make lint` still reports zero issues, so the current lint target does not enforce repository format.
Format these files and consider adding a format gate.

> **主进程回复 — 采纳，含那条"consider"。**
>
> 四个文件已 `gofmt -w`。其中 `cluster_natsconf.go` 我不是直接让 gofmt 改：它会把那段编号列表注释
> 的对齐重排成很难读的样子，所以先把注释改写成散文再格式化。
>
> **加了格式闸**：`make lint` 在 `golangci-lint run` 之后跑 `gofmt -l ./cmd ./internal ./test`。
> 理由写在 Makefile 里：**一个在未格式化代码上是绿的闸门，是所有人都信任而不该信任的闸门**。
> 两个检查都跑完才报结论，一次调用看到全部工作。
>
> 它**立刻抓到了第一个真实案例**——我在修 M3 时新写的 `wire_freeze_test.go` 未格式化，
> 而 `golangci-lint` 依旧报 0 issues。这正好证实你的判断：这不是理论上的缺口。

### M2 — `livenessDB` architecture documentation still describes a caller that this same delta removed

`internal/broker/clusterwrite.go:598-604` says proc-GC is a current anomalous replicated-table write
through `livenessDB`. The changed implementation explicitly uses `singleWriter()` and skips cluster
mode (`internal/broker/reconcile_passes.go:109-138`). The code is safer; the now-false warning should be
removed or rewritten so future maintainers do not reason from obsolete ownership information.

> **主进程回复 — 采纳。**
>
> 那一段是我在同一个 delta 里写的"已跟踪，待移动"，而**移动就在那个 delta 里完成了**——
> 我改了代码没回来改这句话。已重写为"每个调用方现在都是 liveness 写"，并说明 proc-gc 走
> `singleWriter()`（集群模式返回 `(nil,false)`），限制是**结构性**的而不是注释加一个 `if`。
>
> 你说得对，这不是文档洁癖：在维护者**专门来查所有权的那一份 godoc** 里放一句过期的所有权断言，
> 比没有那句话更糟。

### M3 — wire-freeze guards retain avoidable blind spots

- The live dispatch scanner now understands the `writeVerbs` map, but
  `TestWireFreezeScannerSelfCheck` still implements and tests an old synthetic switch scanner inline
  (`internal/broker/wire_freeze_test.go:672-727`) rather than invoking `scanDispatchArms`. The live
  scanner has a count floor, but its claimed self-check is disconnected.
- `jsonKeys` treats anonymous embedded structs as one field name (`:181-205`), while `encoding/json`
  normally promotes their fields. No current frozen specimen embeds a struct, so this is latent, but
  a future embedded payload could make the golden disagree with the actual wire.
- The envelope freeze checks frozen entries against specimens (`:501-511`) but has no reverse
  completeness check analogous to payloads; a newly introduced wrapper can remain absent from both
  hand-kept maps.

Recommendation: make synthetic tests call the live scanner/deriver, add embedded-struct specimens,
and add reverse envelope completeness or a source-derived wrapper inventory.

> **主进程回复 — 三条全部采纳，逐条按你的建议做。**
>
> **(a) self-check 现在驱动活扫描器。** `scanDispatchArms` 拆出 `dispatchArmsFromAST(*ast.File)`，
> self-check 调它。而且合成源码里我**故意留了一个 `switch`/`case` 诱饵**：
> 一个还在读 case 子句的扫描器会报出 `VerbStaleSwitchOnly`（活表里没有这个 key），于是**那次退化会变红**。
> 原来的 self-check 内联实现了一个旧的 case 扫描器并测它——它证明的是**一个已经不存在的扫描器**不空洞。
>
> **(b) 嵌入结构体现在被提升，而且校准物是 `encoding/json` 自己。**
> `jsonKeys` 递归展开无 tag 的匿名嵌入字段（带 tag 的不展开，因为 json 会把它嵌在 tag 名下）。
> 关键是新加的 self-check **不拿手写期望值比**，而是拿 `json.Marshal` 的实际顶层 key 集合比——
> 你指出的风险恰恰是"golden 与 deriver 基于同一个错误假设、于是互相同意"，
> 拿手写 golden 校准就会重现那个风险。（配套需要 `fillNonZero`：零值样本会丢掉每个 `omitempty` key，
> 那会把 deriver 冤枉成错的。）
> **变异验证**：把提升逻辑关掉 → 立刻红，并打印出 `derived [nested own]` vs `actual [B a nested own]`。
>
> **(c) 两个方向都补上了。** envelope freeze 加了 payload 早就有的反向检查
> （frozen 条目没有 specimen ⇒ 类型被改名/删除，golden 已失效），
> 另加 `TestEveryForwardWrapperTypeHasAnEnvelopeSpecimen` —— **从源码推导** wrapper 清单
> （`cluster_forward.go` 里以 `Envelope`/`Reply` 结尾的 struct 类型），而不是比对两张手写表。
> 你的原话点中要害：两张表都是手写的，**它们互相一致什么都证明不了**。带非空洞下限（< 2 就 Fatal）。
> **变异验证**：从 specimen map 里去掉 `forwardReply` → 红，并说明"它包裹每一个 verb"。

## Confirmed areas

- B1 admission consolidation preserves handler-specific ordering, zero-value deny behavior, follower
  short-circuits, role/session/node checks, and separation of wire-safe codes from logged store detail.
  Authcallout/adminsock missing seams fail closed in cluster mode without charging the PIN budget.
- B2's `writeVerbs` table retains the 17 verb/payload/plan mappings and ReqID-before-lookup boundary;
  allocation identity is consistently narrowed to the five SQL fence columns.
- B3's `readDB` and `singleWriter` roles are fail-closed at the intended sites; the direct-DB AST
  baseline is active. The raw `readDB.SQL()` escape hatch remains a consciously deferred limitation.
- B4 ACCT.NK rendering is three-state rather than fabricating equality; self status is authoritative,
  old/missing reports remain unknown, and online doctor uses the selected/reported `nats.conf` path.
- Same-version join works through the real deployment stack; mismatch-policy certification remains
  blocked by B3.

## Doubts requiring owner decisions

1. Is `tools/rescue.py` actually intended to ship? Its presence in the requested unstaged boundary
   conflicts with the testing standard saying it was excluded. Scope notes cannot make an RCE artifact
   safe; the owner must remove it or authorize and design a secure product surface.
2. Has requirements §6.7 been formally superseded? The implementation and Batch B plan assume rolling
   mixed releases, while the current WHAT document mandates exact equality. One source of truth must
   change before review can judge intended behavior.
3. Does `tether_broker_raft_forward_total` mean every authoritative write attempt, every network
   forward, or only calls routed through `proposeOrForward`? Current names/comments promise the first,
   implementation delivers only the third. Define the contract before fixing the choke point.

## Verification evidence

| Check | Result |
|---|---|
| `git diff --check` | PASS |
| `python3 -m py_compile tools/rescue.py` | PASS syntax; security/compatibility findings remain |
| `gofmt -l` on changed Go files | **FAIL** — four files listed in M1 |
| Focused `cmd/tether`, adminsock, authcallout, proto, determinism tests | PASS |
| Focused broker admission/forward/wire/join/status tests | PASS (`23.792s`) |
| Independent alert-forward counterexample | **FAIL** — got empty snapshot |
| Forward-counter `-race` tests | PASS |
| `make lint` | PASS — zero issues |
| `make test` | **FAIL** — only independent B2 counterexample; all other packages pass |
| `make e2e-parallel` | **FAIL** — 15/15 represented, 99 scheduled, 3 failures (B2 twice + B4 once), wall `2m52.622s` |
| Isolated B4 test, `-race -count=10` | PASS (`24.945s`); loaded gate failure remains unresolved |
| Persistent simcluster `status --json` before drill | unavailable: `no leader in instance sim` |
| Current-source sim image build | PASS; baked NATS `2.10.22` matches pin |
| `local.sh drill 10-grow-to-3` | **GREEN** — 19 assertions, 0 gaps; N=3/R=3/status transfer/follower-loss quorum write all pass |

## Reviewer artifacts

- `docs/reviews/batch-b-external-review-tasklist.md`
- `internal/broker/batch_b_external_review_test.go`
- `docs/reviews/batch-b-external-review.md`

No product implementation was modified by the external reviewer.

---

## 主进程处置汇总（step 5，回应本报告）

**7 条 finding 全部采纳，无一驳回。** 逐条回复内嵌在上面各节。

| # | 级别 | 处置 | 变异/负载验证 |
|---|---|---|---|
| B1 | BLOCKER | `git rm --cached tools/rescue.py` → 退出发布；`.gitignore` 加 `/tools/` 防第三次 | 结构性：不再依赖"下一个人记得" |
| B2 | MAJOR | 计数点移到 `Forwarder.observe`（网络边界，`defer` 覆盖全部 7 个 return）；`proposeOrForward` 的 forward 分支去掉重复计数；源码文本测试**删除**，换 3 个真实发送方的结果断言 + AST 构造器守卫 | 关掉 observer → 你的反例 + 我的 3 个子例同时红 |
| B3 | MAJOR | release mismatch 改为**硬拒**（typed，沿用 `version_skew` 码）；未声明仍放行；把"字段未签名、这是运维误操作守卫不是安全边界"钉成测试 | 3 个测试文件的策略断言全部反转并重写 |
| B4 | MAJOR | fixture 要求连续 3 次 split **且** leader id 已知；断言侧 `followerResponse` 从**响应**推导状态并重试穿过选举窗口；`leaderlessProblem` 拆成纯函数 | `TestRetryUntilStateUnderTest` 确定性证明重试/直返/校验三件事；e2e-parallel **两次 ALL PASS**，且第二次满载时 `-count=25 -race` 打那两个测试 132s 全过 |
| M1 | MINOR | 4 文件 gofmt；`make lint` **加格式闸** | 新闸立刻抓到第 5 个文件（我修 M3 时写的），而 golangci-lint 仍报 0 issues |
| M2 | MINOR | 删掉 `livenessDB` 那句已在同一 delta 内失效的 proc-GC 警告 | — |
| M3 | MINOR | self-check 改调活扫描器（合成源码带 `case` 诱饵）；嵌入提升 + 用 `encoding/json` 自身校准；envelope 反向完整性 + **源码推导**的 wrapper 清单 | 3 个变异各自精确变红 |

### 三条 doubts 的回答

1. **`tools/rescue.py` 是否打算发布？** 不发布。已移出暂存并 ignore。加固它是独立增量，需要用户授权——
   我没有替他改一个应急脚本的 CLI 默认值，因为那会在最需要它的时刻打断已有手感。
2. **requirements §6.7 是否已被正式取代？** 没有。因此**实现跟随需求**：release mismatch 硬拒。
   若用户确实要支持混合 release 集群，改的是 §6.7（用户的授权），改完我把门退回 advisory。
3. **`tether_broker_raft_forward_total` 的契约是什么？** **每一次权威写尝试** ——
   即每一次跨线转发 + leader 本地 Propose。原 godoc 承诺的就是这个，现在实现对上了；
   已在 `countForward` 的 godoc 里写明"恰好两个调用方"，以及为什么不能有第三个。

### 门禁（修复后重跑）

| 闸 | 结果 |
|---|---|
| `make test` | **全绿**（含审查者的反例 `TestExternalReviewAlertForwardOutcomeIsCounted`，现在因为产品修好了而通过） |
| `make lint` | **0 issues** + gofmt 闸通过 |
| `make e2e-parallel` #1 | **ALL PASS**（2m51s） |
| `make e2e-parallel` #2 | **ALL PASS**（2m50s） |
| B4 两测试 `-count=25 -race`，与 #2 **同时**跑 | **PASS**（132s） |
| `gofmt -l ./cmd ./internal ./test` | 空 |

**未跑 simcluster drill**：本轮改动没有触碰真实部署栈（`install.sh` / `nats.conf` / systemd unit /
集群生命周期 / 跨机 route mTLS）。唯一有部署可见影响的是 B3 的 join 策略，而你已经用
`10-grow-to-3` 验证过同版本 join 路径（19/19 GREEN），B3 只是**收紧**了不同版本的那一侧——
按 CLAUDE.md §5「按需运行、非必要绝不运行」不重跑。若用户要求，可跑 `10-grow-to-3` 复核收紧后同版本仍 GREEN。

**流程状态：等待外审复核本轮修复。** 未 commit、未 `git add`（暂存是外部审查者的工作）。
