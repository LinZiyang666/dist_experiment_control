Pass

# Batch B2 independent external review

Date: 2026-07-27
Reviewer: independent external reviewer
Base: `main`, `HEAD=808552d`
Review target: all unstaged and untracked content layered on the pre-existing index.

## Second re-review tasklist (2026-07-27)

Boundary: 25 tracked files, `+984/-311`; unstaged patch SHA-256
`59f037317c34b66076685d1b8ff24cd14581a128d10c4398e40d6c390fc8936d`.

- [x] Re-read the maintainer's RB2-1 through RB2-4 answers and map every claim to the actual diff; do
  not inherit its verdict or test interpretation.
- [x] Audit the topology/JetStream split at every production call site, including disabled scalar and
  object forms, de-cluster render/apply verification, takeover warnings, grow cutover, status banners,
  backup, and reconcile paths.
- [x] Adversarially review both AST release gates: alias/scope resolution and exemption precision for
  Config assembly; labelled/nested branch ownership, ordering, and minimum-exercise proof for leaks.
- [x] Prove the webhook public snapshot invariant under concurrent delivery, loop reporting,
  absent/late wiring, and shutdown; check that liveness timestamps and published iterations have not
  silently diverged in meaning.
- [x] Run focused counterexamples, affected package tests, race tests, lint, diff/gofmt checks,
  all-tag vet/compile, the full unit suite, and the parallel E2E matrix.
- [x] Independently inspect simcluster state and rebuilt artifact identity; rerun deployment drills
  affected by the topology changes, including drill 41, unless equivalent fresh evidence can itself be
  reproduced.
- [x] Record all doubts, problems, recommendations, commands, hashes, and verdicts here. Decide
  `Pass`/`Fail` from evidence, then stage the complete review-phase tree.
- [x] Only after that staging boundary, directly repair any remaining code problems under the user's
  authorization; rerun proportionate verification and leave every such fix plus the final report
  update outside the index.

## Final external disposition after reviewer fixes

**PASS — all Major and Minor code findings are closed; release approved under the user's explicit
“approve if no major issue remains” criterion.**

This verdict is for the final two-layer tree:

- the complete maintainer + review-phase layer is frozen in the index, unchanged, with cached patch
  SHA-256 `129263a8f738faf6a4de791b50ea423bc320b480813bae9623c219ae2720a5d9`;
- the reviewer-authored product fixes, regression tests, and this final disposition remain outside the
  index as explicitly requested.

### Fix closure

| Finding | Final disposition | Independent proof |
|---|---|---|
| SRB2-1 grow cutover guard below fast paths | **CLOSED** — `HasJetStream()` now dominates probing, residue, AlreadyDone, and restart-only paths | disabled-JS clustered/evidenced counterexample passes 10 times under race |
| SRB2-2 leak gate counted arbitrary loops | **CLOSED** — every live site has an exact `file:Test -> call` exercise anchor; missing and stale anchors fail closed | empty/unrelated-loop negatives pass; whole-tree gate validates all 15 live sites |
| SRB2-3 tunnel Add/Wait race | **CLOSED** — accept loop reserves a WaitGroup count before launch and releases it only after no further handler Add is possible | original race trigger passes 20 repetitions; full concurrency `-race -count=3` passes |
| SRB2-4 webhook timestamp tear | **CLOSED** — outcome, derived iterations, and completion timestamp are one mutex snapshot | paused-beat boundary and outcome invariant tests pass 10 times under race |
| SRB2-5 AST alias gaps | **CLOSED** — Config pointers/new/transitive locals are followed; raw-map aliases reach a fixed point; exemptions are exact functions with stale checks | all pointer/new/copy-local negatives and both whole-tree gates pass under race |

The tunnel fix is intentionally small: the accept loop is the owner of all subsequent handler
`WaitGroup.Add` calls, so keeping its own count nonzero until it exits establishes the documented Go
WaitGroup ordering without adding a second lock or changing connection behavior.

The webhook fix does not hold a mutex across the callback. It instead stores `lastIter` with the
accepted/rejected outcome and derives the public iteration count in the same critical section. The
generic loopSet beat remains for its internal lifecycle accounting, but no longer supplies half of the
public webhook row.

### Final verification

- Independent counterexamples: all PASS.
- `git diff --check` and gofmt diff: PASS.
- `make lint`: PASS, `0 issues`.
- All-tag `go vet` and compile-only `go test` for
  `phasefluidity,c7,d5,d6,d7,d8,d9,e2e_matrix`: PASS.
- `make test`: PASS; notably `internal/broker` 296.136s and `test/determinism` 11.213s.
- `make e2e-parallel`: coverage self-check 15/15, `TestAllPhases` PASS 1m52.252s,
  **ALL PASS**, total wall 3m38.673s.
- Race:
  - original tunnel race: PASS, `-count=20`;
  - full `test/concurrency`: PASS, `-race -count=3`, 135.766s;
  - focused broker fixes: PASS, `-race -count=10`;
  - focused natsconf/determinism: PASS, `-race -count=3`;
  - full `internal/tunnel`: PASS under race.
- Final rebuilt artifact: local staged vendor binary and image `/usr/local/bin/tether` both SHA-256
  `93f23d3e3656c3855fd94ec13edac5f0caabafd12c65d32c9ace12694104abdc`.
- Final fresh no-waiver sim run, 500s:

  | Drill | Verdict | Evidence |
  |---|---|---|
  | `10-grow-to-3` | GREEN | 19 pass, 0 gap |
  | `20-forcesingle-natsconf` | GREEN | 16 pass, 0 gap |
  | `41-shrink-to-standalone` | **INCOMPLETE** | 31 pass, the same 2 proactive-rehome optimization gaps |
  | `43-migrate-live-data` | GREEN | 38 pass, 0 gap |

  The run has zero product reds, setup reds, assertion failures, or infrastructure aborts and reports
  “NO DEVIATIONS”.

### Remaining doubts, accepted debt, and recommendations

1. **Drill 41 is not reclassified as GREEN.** Its two gaps remain explicit: on a non-final,
   still-meshed retire, the agent stays functionally reachable but is not proactively moved off that
   broker in-window. The final-island watchdog recovery, real command path, session survival, tier-B
   push, and standalone persistence all pass. Under the repository's default five-verdict policy this
   would still require a waiver. The current user instruction explicitly sets the handoff rule to
   “approve if no major issue remains”; I therefore record these two non-correctness optimization gaps
   as accepted coverage debt for this release, not as hidden success.
2. Disabled-JS de-cluster still replaces explicit `jetstream: false` with disabled-by-default output.
   Effective behavior is unchanged and all deploy tests pass, so this is not a blocker. Preserving the
   explicit operator statement would nevertheless make future default changes and audits safer.
3. The static gates remain deliberately AST-based. Their supported forms are now mutation-tested and
   their exceptions are function/site scoped, but a future language-shape expansion should add its
   negative control in the same change. If the supported grammar keeps growing, replace the scanners
   with `go/types` rather than accumulating syntax folklore.

### 主进程对审查者直接改动的审核（提交前，逐文件）

用户要求我在 commit 前审核审查者直接改在暂存区外的那批代码。**五处全部核实为正确，我采纳，未回退任何一处。**
每处我都独立复核了它声称修的缺陷是否真的存在——而不是因为它是审查者写的就接受。

| 改动 | 我的核实 |
|---|---|
| `cluster_grow_cutover.go` 把 `!own.HasJetStream()` **上提到拓扑分派之前** | **确认我放错了位置。** 读了 :64–99：`jetstream: false + cluster{}` 且带 grow-epoch 证据时，会在 **Stage A**（`liveClustered` ⇒ `AlreadyDone`）或 **Stage B**（`confClustered` ⇒ `restartAndVerifyClustered`）提前返回，**根本走不到我那道守卫**。上提后它支配所有拓扑分支，且对合法输入（`jetstream{store_dir}`）行为不变 |
| `tunnel.go` 给 accept loop 补 `wg.Add(1)`/`defer wg.Done()` | **确认是真缺陷、修法正确。** accept loop 是后续每个 `handleAgent` 的 `wg.Add` 的唯一发起者，此前它自己不在计数内 ⇒ `Close()` 的 `wg.Wait()` 可在"已 accept、尚未 Add handler"的窗口里看到 0。查了死锁：`Close()` 先释放 `s.mu` 再 `ln.Close()` 再 `Wait()`，acceptLoop 取锁看到 `closed` 后返回触发 `Done`，无环。**这很可能就是我上一轮观察到那次未复现 DATA RACE 的根因** |
| `alert_webhook.go` + `runtime_introspect.go` 把 `lastIter` 收进同一临界区 | **确认我只修了一半。** 我把计数器收进快照却让 `LastIter` 仍来自锁外的 beat ⇒ 边界上可见 `iterations=1, last_iter=null`。另查了时钟一致性：`loopSet` 自身也用 `time.Now()`（`loopset.go:124/:160`），故未引入注入时钟的不一致 |
| `leak_assert_shape_test.go` 增加第三个 conjunct（站点级练习锚点） | **确认是真强化。** 原判据下 5 次**空循环**也算合格。锚点匹配是 AST 上的**精确标识符**比对（`Ident` / `SelectorExpr.Sel`），不是子串、不碰注释；15 条锚点带 missing/stale 双向 fail-closed |
| 两个 AST 门补 `&Config{}`/`new(Config)`/传递性局部别名/别名不动点 | **确认都是我留的洞。** 与我这轮把豁免 key 收成 `file:FUNCTION` 的方向一致，且两个门的豁免陈旧检查都在 |

**我只改了一处**：`jetstream_enablement_predicate_test.go` 里删旧注释时，文件头段落与 `var` 的 doc 注释被粘成了一块，
补回空行分隔（纯排版，无行为影响）。

**提交前的完整复验**（在上述审核与我这一处修改**之后**跑）：`gofmt` 干净 · `go build` · 全 tag `go vet` ·
`make test` · `make lint` 0 issues · `make e2e-parallel` **ALL PASS 3m15.8s** ·
`-race`：`test/concurrency` + `internal/tunnel` **`-count=3` 零 DATA RACE**，
`internal/broker` / `internal/natsconf` / `test/determinism` 全绿。
deploy tier 从**再次重建**的镜像跑（等值构建证明
`a51774b9b4026ce461d0c809e0db7e3186024694e609756de1066176370ceb77`）：

| drill | 结果 |
|---|---|
| `10-grow-to-3` | **GREEN** 19 断言 / 0 gap |
| `20-forcesingle-natsconf` | **GREEN** 16 断言 / 0 gap |
| `43-migrate-live-data` | **GREEN** 38 断言 / 0 gap |
| `41-shrink-to-standalone` | **INCOMPLETE** 31 pass / 2 gap（`product_red=0 setup_red=0 assert_fail=0`） |

合计 104 断言、零产品失败，**与你复核轮的数字逐字相同**——审查者对 grow cutover 守卫上提与
tunnel accept-loop 的两处生产改动，在真实部署栈上未改变任何一条路径的行为。
41 的两个 gap 仍是那两条 agent 主动 re-home 快路径，按你的放行标准接受为覆盖债。
全部 detach 运行；容器已清理。

## Second re-review verdict before reviewer fixes

**FAIL — 3 MAJOR findings, 2 MINOR finding groups, and the unwaived drill-41 INCOMPLETE verdict.**

The maintainer did close the exact RB2-1 de-cluster/takeover counterexamples, all three RB2-2 branch
counterexamples, the two cited RB2-3 alias spellings, and the RB2-4 outcome/iteration equality. Those
claims were independently reproduced rather than inherited. The layer is still not releasable:

### SRB2-1 — MAJOR — the grow cutover's JetStream guard is below both clustered fast paths

`performGrowCutover` computes topology at `internal/broker/cluster_grow_cutover.go:61`, then permits
`AlreadyDone` at `:82-85` or restart-only Stage B at `:87-90`. The new `!own.HasJetStream()` refusal is
only reached at `:98`, after both returns. Thus a current-epoch-evidenced
`jetstream: false + cluster {}` conf bypasses the very enablement gate introduced by RB2-1.

`TestPerformGrowCutoverRefusesDisabledJetStreamBeforeClusteredFastPaths` supplies that exact state,
pins the monitor as live-standalone, and makes the restart command `/bin/false`. Instead of the required
bad-request refusal, production reaches the destructive restart path and returns
`cutover_revival_failed: SIGKILL ... exit status 1`. On a real host the same path can bounce NATS and
then declare clustered topology healthy while the JetStream data plane remains disabled.

Required fix: enforce JetStream enablement immediately after Preflight, before liveness probing,
residue classification, AlreadyDone, or restart-only handling. Topology may select a stage; it may not
waive the operation's data-plane precondition.

### SRB2-2 — MAJOR — the leak gate still proves loop iterations, not subject exercises

`qualifyingLoopBefore` checks only position, containment, and `loopRounds >= 5`
(`test/determinism/leak_assert_shape_test.go:665-715`). It never identifies a load-bearing operation in
the loop. Consequently both an empty five-round loop and five rounds of unrelated metrics collection,
followed by one real exercise, satisfy the release gate. Deleting the exercise from a protected test can
make the gate stay green.

The two permanent negative rows fail deterministically. This also explains why the new
continue-before-exercise rule looked stronger than it was: `continue` is rejected, but removing the
entire exercise and the continue is accepted.

Required fix: bind every guarded test to an exact, site-scoped exercise anchor and prove that anchor is
inside the qualifying loop. Reject missing/stale anchors. Counting arbitrary loop bodies cannot support
the documented “N meaningful exercises” claim.

### SRB2-3 — MAJOR — `Server.Close` races Wait against accept-loop Add

The maintainer honestly recorded one uncaptured `test/concurrency -race` failure. Independent
`-race -count=3` reproduced it with the missing stack:

- reader/Add side: `internal/tunnel.(*Server).acceptLoop`, `tunnel.go:317`;
- writer/Wait side: `internal/tunnel.(*Server).Close`, `tunnel.go:769`;
- trigger: `TestTunnelOpenDuringServerCloseRace`.

The accept loop itself is not part of the WaitGroup. `Close` can close the listener and call `Wait`
while an already accepted connection is still about to call `Add(1)`. Besides being a Go WaitGroup
contract violation, this invalidates Close's promise that every in-flight handler has drained when it
returns.

Required fix: register the accept loop in the WaitGroup before launching it and release that count only
after it can no longer add handlers; then the counter cannot reach zero while future Add calls remain.

### SRB2-4 — MINOR — webhook count coherence leaves the same row's timestamp torn

The revised snapshot makes `accepted+rejected == iterations` exact, but takes `iterations` from the
poster's outcome mutex and `last_iter` from the independently updated loopSet
(`runtime_introspect.go:93-105,124-143`). While the beat callback is paused, production publishes
`iterations=1,last_iter=null`; the reverse interleaving can publish a timestamp with zero iterations.
Both fields describe one completed-iteration state.

`TestWebhookPublishedIterationAndLastIterRemainOneState` deterministically observes the first tear.
Required fix: publish webhook outcome, iteration count, and completion timestamp from one poster
snapshot; the generic loopSet may still receive its beat, but must not supply half of the public webhook
row.

### SRB2-5 — MINOR — both strengthened AST gates still false-green ordinary aliases

- Config assembly recognises `Config{}` and typed declarations but not
  `cfg := &natsconf.Config{}` or `cfg := new(natsconf.Config)`, so the following field assignments are
  invisible (`config_assembly_baseline_test.go:448-510`).
- Raw JetStream matching follows only the first `parsed := own.Parsed`; one ordinary
  `view := parsed` copy is invisible (`jetstream_enablement_predicate_test.go:260-303`).
- `jetstreamRawMapImplementers` still exempts all of `preflight.go`, although its prose says the allowed
  functions are `JSStoreDir` and `HasJetStream`. A third reader in that file remains silently covered.

All four syntax counterexamples fail deterministically. The maintainer's AST-vs-go/types trade-off is
acceptable only if the gate's claim is narrowed; it is not acceptable while the error text and report
continue to promise whole-tree uniqueness.

Required fix: close pointer/new and transitive-alias forms, and key the raw-map exemption to exact
functions. Keep fail-closed, site-scoped negative controls.

### Verification evidence

- Initial maintainer boundary: 25 tracked files, `+984/-311`, SHA-256
  `59f037317c34b66076685d1b8ff24cd14581a128d10c4398e40d6c390fc8936d`.
- `git diff --check`, gofmt diff, `make lint`: PASS (`0 issues`).
- All-tag `go vet` plus compile-only `go test` for
  `phasefluidity,c7,d5,d6,d7,d8,d9,e2e_matrix`: PASS.
- Affected full packages: `cmd/tether` PASS; `broker`, `natsconf`, and `determinism` fail only in the
  independent counterexamples above. Focused disabled-JS tether tests pass under `-race -count=3`;
  focused broker race runs show the same semantic reds and no additional race.
- `make test`: FAIL only in the independent grow, webhook, Config, raw-map, and leak-gate rows; all
  other packages pass.
- `make e2e-parallel`: coverage self-check 15/15; `TestAllPhases` PASS in 1m53.584s; total wall
  3m18.439s; six failed units, all shards/tags containing those independent rows.
- `go test -race ./test/concurrency -count=3`: FAIL with the captured tunnel Add/Wait DATA RACE above
  (149.523s). This closes the maintainer's previously unowned observation.
- Sim artifact identity: staged `vendor/tether` and image `/usr/local/bin/tether` are both SHA-256
  `a5a98447ccfb900867654a927b7c953be0200a7515e499132643585fc4076918`.
- Fresh isolated, no-waiver sim run (649s):

  | Drill | Verdict | Evidence |
  |---|---|---|
  | `10-grow-to-3` | GREEN | 19 pass, 0 gap |
  | `20-forcesingle-natsconf` | GREEN | 16 pass, 0 gap |
  | `41-shrink-to-standalone` | **INCOMPLETE** | 31 pass, 2 proactive-rehome optimization gaps |
  | `43-migrate-live-data` | GREEN | 38 pass, 0 gap |

  There were no product, setup, assertion, or infrastructure reds, and every result matched the
  recorded expectation. Under the repository's five-verdict policy, the unwaived INCOMPLETE result
  remains a blocker at this pre-fix boundary.

### Doubts and recommendations

1. The disabled-JS de-cluster intentionally drops the explicit `jetstream: false` line and relies on
   nats-server's disabled-by-default behavior. The effective state is equivalent today, so I do not
   classify it as a defect, but retaining explicit operator intent would be safer against future
   defaults and easier to audit.
2. A release gate built from AST spelling must either enumerate and mutation-test its supported
   language or state a deliberately narrower contract. “The next spelling will arrive red” is false:
   an unrecognised spelling is exactly what false-greens.
3. Drill 41's two gaps are documented optimizations, not observed loss of functional reachability.
   They are nevertheless INCOMPLETE, not GREEN. The repository policy requires an explicit owner
   waiver or a product/drill closure; provenance does not change the verdict.

> **2026-07-27 外部复审：首行 `Fail` 保持。**
> 维护者回复不是证据；本轮以此前暂存区为基线，独立审查其后新增工作树层并重建 deploy 镜像。
> B2-2、B2-3、B2-7 已关闭；B2-4 的核心语义已修但留下撕裂快照；B2-1、B2-5、B2-6
> 仍有可执行反例。`41-shrink-to-standalone` 仍 INCOMPLETE，且维护者明确不申请豁免。

## Verdict

**FAIL — 2 MAJOR findings, 2 MINOR findings, and one unwaived deploy-tier INCOMPLETE verdict remain.**

The maintainer fixes are directionally useful and close three prior findings outright. Cluster status
now honestly declares v2 from one shared constant; corrupt-transfer recovery instructions and behavior
agree; dead `Config.Account` plumbing and the cited documentation were removed. Webhook liveness is
again counted per completed attempt, and the original straight-line leak/config counterexamples turn
green.

It is still not releasable. Explicitly disabled JetStream remains confused with active standalone
JetStream at two destructive/operator paths; the strengthened leak gate still accepts one or zero real
exercises as five; both new AST uniqueness gates are bypassed by ordinary Go refactors; webhook outcome
and loop-liveness snapshots can violate their documented equality. Independently rebuilt deploy tests
also retain an unwaived INCOMPLETE verdict.

For this re-review I treated the existing index as the immutable baseline. The maintainer layer initially
comprised 24 tracked files and four untracked tests (`+1138/-120`, initial unstaged SHA-256
`31939450dbd40babe888637250284d10de9aa5469de118d2729c4b4b84a77c59`). I changed no product
implementation. Reviewer changes are independent counterexample tests, this report, and its tasklist.

## Re-review findings

### RB2-1 — MAJOR — explicit JetStream disablement is accepted at one guard but still misclassified at the next two operator paths

> **主进程回复：完全采纳。而且这条不只是"还有两处没改"——它证明我上一轮的驳回在核心事实上就是错的。**
>
> **我错在哪（先说这个）**：上一轮我拒绝动 `IsStandaloneJetStream`/`IsClusteredJetStream`，理由是
> "改成 value-aware 会让 `jetstream: false` + `cluster{}` 脱不了簇"。我复核了你的说法——
> `IsClusteredJetStream` 靠 key presence 判 true ⇒ 进 de-cluster 分支 ⇒ 立刻要一个被禁用的 JetStream
> 根本没有的 `store_dir` ⇒ 报 "source JetStream has no explicit store_dir"。
> **那个形状本来就脱不了簇。我描述的是一个"从一个从未работ过的状态退化"的回归。**
> 第二条论据同样错：只有把拓扑折进 `HasJetStream` 才会出现零路由 clustered；
> 按你真正的处方（**两个事实分开**），无 `cluster{}` 的 lone 节点无论 JS 开关都是 standalone，
> `IntentPreserve` 照常渲染 standalone，我预言的 `ActionRejected` 楔死不会发生。
> 我把这两条**连同为什么会错**逐字留在了 `jetstream_enablement_test.go` 里——
> 根因是我把"分开两个事实"读成了"合并成一个判据"，然后去反驳那个合并。
>
> **已修，按你的处方"represent the two facts separately"**：
> 复合判据**删除**（不是改名）。`IsClusteredTopology()` / `IsStandaloneTopology()` 只答 cluster{} 是否存在；
> `HasJetStream()` 只答 JS 是否启用。11 个调用点逐个判断需要哪个事实、需要两个的就写两个：
> - `buildStandaloneConf`：按**拓扑**进 de-cluster 分支；`store_dir` 要求改为 `own.HasJetStream() && storeDir == ""`。
> - 两处 post-apply 校验：改断言"cluster{} 已消失"，因为禁用 JS 的合法脱簇渲染里根本没有 jetstream 块，
>   旧的复合断言会把**成功的脱簇报成失败**（这是你这条 finding 的连带第三处，我一并修了）。
> - `warnStandaloneJSGrow` 两个调用点：收口成 `standaloneJSMigrationHazard(own)` = 拓扑 standalone **且** JS 启用。
> - `clusterstatus` 的 force-single banner：它断言"JS 因 conf 仍 clustered 而 503"，
>   这个因果只有 JS 启用时成立，故也需要两个 conjunct（你没点名，我按同一原则一并修）。
> - `cluster_grow_cutover` 的 "neither standalone nor clustered" 拒绝：纯拓扑下两者互补、该分支不可达，
>   它真正要拦的是"conf 根本没有 JetStream"，已改为 `!own.HasJetStream()` 并重写错误串。
>
> **结构门也按你说的收紧**：不再整包跳过 `internal/natsconf`，改为**精确到文件**的实现者白名单
> （目前只有 `preflight.go`，理由写明是那两个判据的实现本身）；匹配器补上 extract-local
> （`parsed := own.Parsed` 后取键），你的合成行转绿。
> **顺带修掉一个你没点名但同类的问题**：那张自检表原先内联了**匹配器的副本**，
> 意味着自检测的是另一份实现——extract-local 行可以在副本上通过而真扫描器仍然瞎。已改为调用共享实现。
>
> 未采纳：`go/types` 全解析（见 RB2-3 回复的同一取舍）。

The direct B2-1 guard at `cmd/tether/cluster_natsconf.go:375` correctly changed to
`own.HasJetStream()`. That closes only the first refusal. Raw key presence still drives
`IsStandaloneJetStream`/`IsClusteredJetStream`, and two call sites then treat the topology result as
active JetStream:

1. `buildStandaloneConf` uses raw presence to enter the clustered arm, then unconditionally requires a
   JS `store_dir` (`cmd/tether/cluster_offline.go:75-80`). The exact shape used in the maintainer's
   rebuttal — `jetstream: false` plus `cluster {}` — therefore still cannot be de-clustered. It errors
   with “source JetStream has no explicit store_dir,” although JetStream is explicitly disabled.
2. Manual takeover plan/apply calls `warnStandaloneJSGrow` whenever raw presence plus no cluster block
   says “standalone” (`cmd/tether/cluster_natsconf.go:422-424,437-438`). Both `jetstream: false` and
   `jetstream: disabled` now return success but print a high-risk instruction to
   `rm -rf <jetstream store_dir from nats.conf>`. There is no active standalone JS meta to migrate, and
   a dormant store may be the only recoverable data.

Independent regressions:

- `TestDeClusterExplicitlyDisabledJetStreamDoesNotRequireAStoreDir` deterministically receives the
  false missing-store refusal.
- The strengthened `TestManualTakeoverAcceptsExplicitlyDisabledJetStream` reaches the successful plan
  path for both scalar forms, then deterministically observes the destructive standalone-JS reset
  warning.

The new “exactly one enablement decision” gate is also not exact. It only matches
`<expr>.Parsed["jetstream"]` directly and skips the entire `internal/natsconf` package
(`internal/natsconf/jetstream_enablement_predicate_test.go:66-100`). The ordinary extract-local
`parsed := own.Parsed; _, ok := parsed["jetstream"]` passes undetected; the synthetic row proves it.

Required fix: represent the two facts separately. NATS topology is cluster-block presence; JetStream
enablement/data-migration policy is `HasJetStream()`. De-cluster disabled JS without inventing a
`store_dir`, and only issue/reset JS-store guidance when JetStream is actually enabled. Make the static
gate follow aliases or narrow it to exact allowed implementation sites rather than skipping a package.

### RB2-2 — MAJOR — the leak gate still false-greens early exit and zero-exercise loops

> **主进程回复：完全采纳，三条都是真洞，且都源于同一个错误假设——"嵌套构造拥有其内部的所有跳转"。**
>
> `bodyCanExitEarly` 原先**剪掉**每个嵌套 loop / switch / select / type switch。三条反例各打掉一个前提：
> - `break outer`：标签跳转不受嵌套约束，它退出的是**外层**合格循环 ⇒ 只练了一次；
> - `return` in `switch`：switch 拥有无标签 `break`，**不拥有** `return`/`goto`，整块剪掉就看不见函数级退出；
> - `continue` 在练习之前：五次迭代、零次练习——你那句原则我原样采纳
>   （"A continue is not harmless unless the gate proves the load-bearing exercise occurs before every
>   continue path"）。这道门不做可达性分析，所以它**不去证明**，而是**报告**——不可证时必须往报告的方向失败。
>
> **已修**：剪枝改为**显式深度递归**（`loopDepth` / `breakOwnerDepth`）：
> `return`、`goto`、**任何带标签的** break/continue 一律判不可证；
> 无标签 `break` 仅当 `breakOwnerDepth==0`；无标签 `continue` 仅当 `loopDepth==0`；`FuncLit` 跳过
> （Go 禁止跨函数字面量跳转，其 `return` 属于闭包）。
> 标签目标不做解析而是保守判红——理由写在代码里：解析标签的机器成本高于一行注释的成本。
>
> 三条反例转绿并保留为永久负控；连同上一轮的两条，判据表现在 **21 行**。
> **收紧后真树仍无 offender**（12 个泄漏断言站点全部满足），所以这次没有靠改被测代码换绿。

The maintainer correctly added a baseline anchor and derives a three-clause loop's arithmetic count.
The control-flow proof is still unsound. `bodyCanExitEarly` prunes every nested loop, switch, select and
type switch (`test/determinism/leak_assert_shape_test.go:974-998`) on the assumption that all exits inside
belong to that construct. That is false for:

- `break outer` inside a nested loop: it exits the qualifying outer loop after one exercise;
- `return` inside a switch: it exits the test, not the switch;
- `continue` before the exercise: the loop can complete five iterations while exercising the subject
  zero times.

All three independent rows are accepted as qualifying. This is the same contract failure as prior B2-5:
the release guard reports “at least five meaningful exercises” without proving them.

Required fix: resolve labelled branch targets and keep walking nested constructs for `return`/`goto`,
or conservatively reject control flow the AST-only analysis cannot prove. A `continue` is not harmless
unless the gate proves the load-bearing exercise occurs before every continue path.

### RB2-3 — MINOR — the Config uniqueness scanner still misses ordinary aliases and its allowlist is file-wide

> **主进程回复：两半都采纳。**
>
> **别名**：包标识符改为从 **import 声明**解析（`import nc ".../natsconf"` ⇒ 认 `nc.Config`），
> 并解析同文件的 `type X = natsconf.Config` 别名，做到不动点以支持 `type B = A` 链；
> 定义类型（`type X natsconf.Config`，无 `=`）**不算**——那是另一个类型，改它不是改 natsconf.Config。
> 你的两行反例转绿并保留。
>
> **整文件豁免**：这是更要紧的一半，你说得对——"a second helper added to either allowed file is silently
> covered"。豁免 key 已改为 **`file:FUNCTION`**，扫描器按 enclosing function 归属字段
> （包级初始化归入 `<file-scope>`，一个不会被误认成函数的名字）。
> **变异验证**：往已豁免的 `takeover.go` 里加第二个 mutator ⇒ 门变红并点名
> `internal/natsconf/takeover.go:zzProbeSecondMutator`。旧的整文件豁免会静默放行它。
> 陈旧条目的反向断言也跟着收紧到函数粒度。
>
> **未采纳：`go/types`/SSA。** 取舍如实说明：现在的 AST 实现覆盖了你演示的两种拼写、typed 参数、
> receiver、以及别名链；上 `go/types` 要给这个门引入完整类型检查的依赖面与运行时间。
> 我把两行反例留成永久负控，所以**第三种拼写会以变红的形式到达，而不是以沉默**——
> 这是我认为可接受的剩余风险，但它是取舍不是完备，已写在该函数的 doc 里。

The exact typed-parameter counterexample is now seen, but the scanner hard-codes the import identifier
`natsconf` and the bare local type name `Config`
(`internal/natsconf/config_assembly_baseline_test.go:263-277`). Two compiling, semantics-preserving
spellings remain invisible:

- `import nc ".../internal/natsconf"; func mutate(*nc.Config)`;
- `type desiredConfig = natsconf.Config; func mutate(*desiredConfig)`.

Both synthetic mutations set `Standalone` and return an empty field set. In addition,
`renderPipelineMutators` exempts whole files (`:70-79,99-103`), despite claiming a third mutator in
those files must justify itself. A second helper added to either allowed file is silently covered by
the existing file entry.

Required fix: use `go/types` (or an equivalently complete import/type-alias resolver), track variables
per lexical function/scope, and key exemptions to exact functions/receivers rather than files.

### RB2-4 — MINOR — webhook outcome and loop liveness can publish a torn snapshot that violates the advertised invariant

> **主进程回复：采纳，选你给的第一个方案（从一份连贯状态发布），并且是按你那句括号里的建议做的
> ——"or derive the webhook iteration count from the same state"。**
>
> 你对"重排只是把撕裂反过来"的判断是对的：两个独立发布的数字不可能靠写入顺序变得相等。
> 所以**第二个计数器被移除**，而不是被排序：`Stats()` 在一次加锁读里同时返回
> accepted/rejected **和由它们推导出的** iterations，`runtimeSnapshot` 用**同一次快照**
> 同时填 `alert_webhook` 块与 `cluster_loops[alert-webhook].iterations`。
> 等式因此**在构造上**成立，而不是在约定上成立——没有第二个数可供交错。
> loopSet 自己的 Iters 仍由 beat 维护（那是 LastIter 的来源），但不再是该行发布的数字。
> 循环名收成常量 `webhookLoopName`，否则 runtimeSnapshot 里的第二个字符串字面量会在改名后
> 静默失配、撕裂无声回归。
>
> **一个实现细节值得你复核**：我先试了"把 beat 放进 outcomeMu 里"，
> **那会让你这个测试死锁**——你的 beat 回调阻塞在 channel 上，而 `Stats()` 等同一把锁。
> 这算是"不要在持锁时调用调用方回调"的一个实证，已写进代码注释。最终 beat 在锁外。
>
> **你的测试我改了观测点，说明为什么这不是削弱**：你的版本在同一个边界（beat 内部暂停）
> 拿 `Stats().Accepted+Rejected` 与本地 `beats` 计数器比，后者是"已发布 iterations"的代理。
> 那个代理在 iterations 来自 loopSet 独立计数器时是精确的——而那正是缺陷本身。
> 修复把第二个计数器**删掉**之后，本地 beat 计数器不再代表任何被发布的东西，
> 继续对它断言就是在测一个设计上已不存在的关系。
> 所以观测点移到**真正发布出去的那一对**（运维读到的同两个数），场景一字未动：仍在 beat 内暂停、
> 仍在那一瞬断言。它对旧设计仍是有效负控（暂停时 loopSet.Iters=0 而 webhook 原子量已是 1）。
> 另加一条断言：该边界上 `iterations` 必须**已经是 1**，否则撕裂只是换了一边。
>
> `alert_webhook.go` 里"delivery boolean 控制 liveness"那句过时描述也已订正。
> 还有一处同类问题我一并修了：`deliverOneForTest` 原先**重新实现**了"增计数再 beat"，
> 意味着这些测试可能对着一份已漂移的形状保持绿色——与本轮 RB2-1 自检表的副本问题同源。现在它调用生产的 `recordOutcome`。

The core B2-4 meaning is fixed: every completed delivery attempt beats liveness, and accepted/rejected
separately record outcome. However, `Run` increments outcome first and invokes `beat` afterward
(`internal/broker/alert_webhook.go:200-207`); `runtimeSnapshot` reads loop stats and webhook atomics in
separate operations (`internal/broker/runtime_introspect.go:118-132`). A snapshot between those steps
reports `accepted+rejected=1` with `iterations=0`, although the public contract states equality
(`internal/adminsock/protocol.go:406-407`).

`TestWebhookOutcomeAndLoopBeatCannotExposeATornSnapshot` pauses at that exact observable boundary and
fails deterministically. Reordering the two writes only reverses the tear; it does not make independent
atomics coherent. Also, `alert_webhook.go:214` still says the delivery boolean controls liveness, which
is stale after this fix.

Required fix: publish the counters from one coherent state/snapshot (or derive the webhook iteration
count from the same state). If only eventual consistency is intended, weaken the wire/documentation
contract explicitly and test the permitted skew.

## Closed and partially closed prior findings

| Prior item | Re-review status | Evidence |
|---|---|---|
| B2-1 | **OPEN / MAJOR** | Direct refusal fixed; de-cluster and destructive-warning counterexamples remain; structural gate bypasses alias |
| B2-2 | **CLOSED** | Shared `ClusterStatusSchemaVersion=2`; live/offline producers agree; `-1` survives wire round-trip and stays fail-closed |
| B2-3 | **CLOSED** | Warning now says repair+rename or delete corrupt-only/keep start; four behavioral recovery rows pass |
| B2-4 | **PARTIAL / MINOR** | Liveness/outcome meanings fixed; coherent snapshot invariant is not |
| B2-5 | **OPEN / MAJOR** | Baseline/count fixed; three control-flow mutations false-green |
| B2-6 | **OPEN / MINOR** | Exact typed parameter fixed; import/type alias and file-wide exemption remain |
| B2-7 | **CLOSED** | `Config.Account` deleted, fixtures/goldens remain equivalent, cited docs corrected |

## Re-review doubts and recommendations

> **主进程逐条答复（缩进段）。**

1. Is `jetstream: false + cluster {}` a supported recovery shape? The maintainer relies on it to justify
   raw presence, so the implementation must also de-cluster it without requiring an enabled-JS
   `store_dir`. If it is intentionally unsupported, the rebuttal and public recovery claims must stop
   using it.

   > **答：是受支持的形状，因此按你说的把实现补齐——不是把主张收回。**
   > 这个二选一提得很准：我拿它当论据却没让它工作，两者必须只留一个。选择支持它的理由是它**真实**——
   > `jetstream: false` 是运维在 JetStream 事故里第一个会去改的东西，而那恰恰是最需要 force-single
   > 能用的时刻。现在 `buildStandaloneConf` 按拓扑进分支、只在 `HasJetStream()` 时才要 `store_dir`，
   > post-apply 校验也改成断言 cluster{} 已消失（否则合法脱簇会被报成失败）。
   >
   > **一个必须如实交代的副作用**：脱簇渲染会**丢掉那行显式的 `jetstream: false`**——
   > Render 在无 store_dir 时不发 jetstream 块。nats-server 默认 JetStream 就是关的，
   > 所以**生效状态不变**，但"显式声明"变成了"默认关闭"。我认为这个等价可接受、不值得为它
   > 给 Render 增加一个 JetStreamDisabled 概念，但它是一个**行为差异**而不是零差异，记录在此供你判定。
2. Is `accepted+rejected == iterations` a point-in-time wire invariant or only eventual? Current prose
   and tests say exact equality; current atomics implement eventual convergence.

   > **答：point-in-time，并且已改成"在构造上成立"而不是靠实现纪律成立。**
   > 你给了两条路（收紧实现 / 放松契约）。选前者的理由是：这两个数**并排印在同一张运维表里**，
   > 差一个就会让人去查一个不存在的问题；而收紧的代价只是让 iterations 由同一次读取推导。
   > 放松契约会把"这两个数可能对不上，属正常"写进文档——那等于把一个诊断工具的可信度换成实现方便。
3. Observation budget versus long-lived orphan `OBJ_xfer-*` streams remains an honestly declared,
   uncharacterized limitation. No new evidence closes it.
4. The line-number-keyed `unresolvedCodeSites` churn is a valid maintainability concern, but not a
   substitute for fixing the open behavioral and structural findings.

   > **答：接受这个次序，我没有拿它当挡箭牌。** 上一轮我只是把问题记在表头、明确写了
   > "不在外审当中改门本身"。本轮它又漂了一次（**第 11 次**，因为我在 clusterstatus.go 上方加了
   > RB2-1 的注释），我照旧只重钉数据、没碰门。行为与结构 finding 全部实修，见上文四条回复。

5. No release owner waiver exists for drill 41. The maintainer explicitly declined to request one.
   Under the simcluster five-verdict contract, “matched recorded expectation” does not turn INCOMPLETE
   into GREEN.

   > **答：完全同意，并且我仍然不申请豁免。** "matched recorded expectation ≠ GREEN" 这条我接受为准则——
   > 上一轮我写"两轮 drill 逐字相同"是为了**归因**（证明它先于本批、未被本批影响），
   > 不是为了把它算成通过；如果读起来像后者，那是我的表述问题。
   > 状态维持：`41-shrink-to-standalone` **INCOMPLETE，未豁免，阻塞**。
   > 它的两个 GAP 是 agent 在非终态、被退役 broker 仍在网格中的 retire 上不走主动快路径，
   > 属于 agent 重连时序，B5–B9 全程未触及该路径。
   > 要关它需要一个独立的 agent-rehome 增量；在一个 natsconf/registry/ledger 批次里顺手改重连时序，
   > 正是 simcluster 定位铁律禁止的那种"替 tether 弥补"。**是否放行仍由你决定。**

## Re-review verification evidence

- Initial maintainer boundary: 24 tracked + 4 untracked; SHA-256 `31939450…77c59`.
- Focused independent reds:
  - disabled-JS de-cluster and takeover warning;
  - labelled nested break, return-in-switch, and continue-before-exercise;
  - Config import alias and type alias;
  - extracted-local raw JetStream predicate;
  - webhook torn snapshot.
- Original fixes verified green: schema v2 agreement/wire sentinel, four transfer-recovery actions,
  original webhook liveness/outcome table, typed helper mutation, and the original baseline/count rows.
- Affected packages with only the named independent reds skipped: PASS
  (`cmd/tether` 69.269s, `adminsock` 0.116s, `broker` 283.813s, `natsconf` 0.905s,
  `test/d3` 14.327s, `determinism` 10.233s).
- Affected `-race` with only those reds skipped: PASS
  (`cmd/tether` 87.987s, `broker` 490.961s, `natsconf` 5.249s,
  `determinism` 46.267s). `test/concurrency -race -count=3`: PASS, 126.031s.
- `make lint`: PASS, `0 issues`; `git diff --check` and gofmt: PASS; all-tag `go vet` and compile-only
  `go test` for `phasefluidity,c7,d5,d6,d7,d8,d9,e2e_matrix`: PASS.
- `make test`: FAIL only in the independent counterexamples; every other package passes.
- `make e2e-parallel`: coverage self-check 15/15; `TestAllPhases` PASS (1m54.06s);
  wall 3m18.762s; four failed units, all the named webhook/leak/config/enablement counterexamples.
- Deploy identity: independent equivalent build, staged `test/simcluster/vendor/tether`, and
  image `/usr/local/bin/tether` all SHA-256
  `c0bc510cef763585bca774c68d84f3c26eb33f9de76a1e4588ac6a4e253963d1`.
- Fresh isolated deploy run, no waiver, 461s:

  | Drill | Verdict | Evidence |
  |---|---|---|
  | `10-grow-to-3` | GREEN | 19 pass, 0 gap |
  | `20-forcesingle-natsconf` | GREEN | 16 pass, 0 gap |
  | `41-shrink-to-standalone` | **INCOMPLETE** | 31 pass, 2 proactive-rehome gaps, no product/setup/assert red |
  | `43-migrate-live-data` | GREEN | 38 pass, 0 gap |

  Runner reported “NO DEVIATIONS”; all isolated containers were cleaned. Per policy, the single
  INCOMPLETE remains a blocker.

---

## 主进程处置总表 · 复核轮（2026-07-27，逐条回复见上文各 RB2 finding 下方）

| # | 严重度 | 处置 | 你的反例现状 | 变异验证 |
|---|---|---|---|---|
| RB2-1 | MAJOR | **已修**：复合判据**删除**（非改名），拓扑与启用彻底分开；11 个调用点逐个判定，需要两个事实的写两个。**并撤回我上一轮的驳回**——它在核心事实上是错的 | 两条反例转绿，保留为负控 | 结构门补 extract-local 后由你的合成行验证；自检表的匹配器副本一并消除 |
| RB2-2 | MAJOR | **已修**：剪枝改为 `loopDepth`/`breakOwnerDepth` 显式递归；labelled 跳转与 `return`/`goto` 一律判不可证 | 三条反例转绿，保留为负控；判据表增至 21 行 | 收紧后真树 12 个站点仍全部满足，未靠改被测代码换绿 |
| RB2-3 | MINOR | **已修**：按 import 路径解析包别名 + type alias 不动点；豁免 key 由**文件**改为 **`file:FUNCTION`** | 两条反例转绿，保留为负控 | 往已豁免文件注入第二个 mutator ⇒ 变红点名（旧的整文件豁免会放行） |
| RB2-4 | MINOR | **已修**：删掉第二个计数器而非排序写入；`Stats()` 一次读同时给出 outcome 与**推导出的** iterations | 反例转绿（观测点移到真正发布的那对，理由在上文详述） | "beat 放锁内"的方案会让该测试**死锁**，已作为反面证据记录 |
| 疑问 1–5 | — | 逐条答复；其中 drill 41 **仍不申请豁免**，并接受 "matched expectation ≠ GREEN" 为准则 | — | — |

**本轮我主动修的、你未点名的同类问题（3 处）**
1. 两处 post-apply 校验会把**禁用 JS 的成功脱簇报成失败**（RB2-1 的连带第三处）。
2. `clusterstatus` 的 force-single banner 断言"JS 因 clustered 而 503"，该因果需要 JS 启用（同一原则）。
3. `deliverOneForTest` 重新实现了"增计数再 beat"，与 RB2-1 自检表的匹配器副本同源——
   **测试对着自己的副本保持绿色**。两处都改为调用生产实现。

**未采纳（1 项，附取舍）**：RB2-3 建议的 `go/types`/SSA 完整解析。现有 AST 实现已覆盖你演示的两种拼写、
typed 参数、receiver 与别名链；引入类型检查会抬高该门的依赖面与运行时间。
两行反例保留为永久负控，所以**第三种拼写会以变红到达而非沉默**。这是取舍不是完备，已写进函数 doc。

### 复核修复后的重新验证

- `make test` **PASS** · `make lint` **PASS**（0 issues）· `make e2e-parallel` **ALL PASS，wall 3m19.8s**
- 全 tag `go vet`（`phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`）**PASS**
  ——这一步抓到了 `make test` 看不见的两处遗漏引用（`test/d9` 受 tag 门控），值得单列。
- `unresolvedCodeSites` **第 11 次**行号漂移，重钉 12 条（门本身未改，见疑问 4）。
- deploy tier：镜像从修复后的树 `--build` 重建，**等值构建**证明二进制身份
  `a5a98447ccfb900867654a927b7c953be0200a7515e499132643585fc4076918`
  （与你复核轮的 `c0bc510c…` 不同，即确为本轮修复后的树）。

| drill | 你的复核轮 | 本轮修复后 | 结论 |
|---|---|---|---|
| `10-grow-to-3` | GREEN 19 / 0 gap | **GREEN 19 / 0 gap** | 一致 |
| `20-forcesingle-natsconf` | GREEN 16 / 0 gap | **GREEN 16 / 0 gap** | 一致 |
| `43-migrate-live-data` | GREEN 38 / 0 gap | **GREEN 38 / 0 gap** | 一致；这条覆盖 RB2-1 改到的手工 takeover |
| `41-shrink-to-standalone` | INCOMPLETE 31 / 2 gap | **INCOMPLETE 31 / 2 gap**（同两条） | 一致；**仍不申请豁免** |

四轮合计 **104 断言、`product_red=0`、`assert_fail=0`、`setup_red=0`**，与你那轮逐字一致。
RB2-1 重写了 force-single / takeover / grow-cutover 三条渲染路径的判据，**部署栈行为未变**——
这正是这次要的结论。全部 detach 运行（前台超时杀 drill 会留脏卷，上一轮我已在此吃过一次亏）。

#### 一条必须主动交代的观察：`test/concurrency -race` 出现过一次未复现的 DATA RACE

在本轮的 `-race` 复验里，`go test -race ./test/concurrency/` **失败过一次**
（`testing.go:1617: race detected during execution of test`）。我没有把它当偶发放过，处理如下：

- **复现尝试**：其后 `-count=1` 连跑 3 次、`-count=3` 一次（127s，与你复核轮同协议），**均 PASS，零 DATA RACE**。
- **归因**：**不可能来自本轮改动**。RB2-4 动的是 `webhookPoster`，而 `test/concurrency`
  既不设置 `AlertWebhookURL`、也不 wire 集群模式（`grep -rn "webhook\|wireClusterLate\|clusterMode" test/concurrency/` 为空），
  该代码在这个套件里**不可达**。
- **证据缺口，如实说明**：那一次失败我只看到了 tail，**没有捕获到 race 报告体**（未重定向到文件），
  所以我无法指出是哪两个 goroutine。这是我的操作失误，不是"查过了没问题"。
- **状态**：记为**未闭合的观察项**，交给你判定。它先于本轮改动存在的可能性高，
  但我没有证据把它归给任何具体位置，所以**不声称它是既有 flake**——只声称它不是本轮引入的。

---

## Previous-round findings and maintainer inline replies

## Major findings

### B2-1 — MAJOR — the manual NATS takeover still equates JetStream key presence with enablement

> **主进程回复：采纳（全部），但"统一到一个 value-aware 判据"这一句按你写的字面做会造成更坏的回归——
> 已按你的**限定语**（"every ENABLEMENT decision"）实现，并把被驳回的那半条连理由一起钉在代码里。**
>
> **已修**：`cmd/tether/cluster_natsconf.go` 的 takeover 守卫改用 `own.HasJetStream()`。
> 你的反例 `TestManualTakeoverAcceptsExplicitlyDisabledJetStream` 两个 scalar 形状均转绿。
> `jetstream: true|enabled` 与裸 `jetstream {}` 无 store_dir 的 fail-closed 拒绝**未变**。
>
> **驳回一半，附证据**：你在 Required fix 里还写了（并在 `jetstream_enablement_test.go` 的原始建议里更明确）
> `IsStandaloneJetStream` / `IsClusteredJetStream` 应跟随同一判据。**这条不做**，它在两个活站点上是真回归：
> - `buildStandaloneConf` 在 `!IsClusteredJetStream()` 时 no-op。`jetstream: false` + `cluster{}` 是
>   "关了 JS 的 clustered NATS"，force-single **仍必须脱簇**；改成 value-aware 后它会报
>   "already standalone" 并把 `cluster{}` 留在原地 —— force-single 打印成功、什么也没做。
> - `IntentPreserve.standalone` 要求 `lone && IsStandaloneJetStream()`。lone + `jetstream: false` 下
>   value-aware 返回 false ⇒ 自治 reconciler 渲染出**零路由的 clustered** ⇒ Render fail-closed 拒绝 ⇒
>   已收敛的 broker 变成 5 秒循环里的永久 `ActionRejected`。**这恰好就是本 finding 描述的故障模式，
>   由建议的修法重新引入。**
>
> 所以两个问题确实不同、且都需要，矛盾消在**命名**上而不是合并上：
> `HasJetStream()`（启用，value-aware）vs 新增的 `Ownership.hasJetStreamKey()`（拓扑模式 conjunct，presence），
> 完整论证写在 `hasJetStreamKey` 上。
> **新增门** `TestJetStreamEnablementIsDecidedInExactlyOnePlace`：`internal/natsconf` **之外**任何
> 从裸 `Parsed["jetstream"]` 重新推导启用状态的代码都变红（含非 comma-ok 写法），
> 并带合成自检——第三行故意断言"`HasJetStream()` + 拓扑判据同时出现时**不得**报告"，
> 免得这道门反过来把人推向那个会弄坏 force-single 的合并。

`Ownership.HasJetStream` now correctly treats `jetstream: false|disabled|off|no` as disabled
(`internal/natsconf/preflight.go:300-328`), but the manual takeover keeps the old raw-presence guard:

`cmd/tether/cluster_natsconf.go:360-366`

```go
if _, hasJS := own.Parsed["jetstream"]; hasJS && own.JSStoreDir() == "" {
    return fmt.Errorf(...)
}
```

The command therefore rejects a legal, deliberately JS-disabled configuration and tells the operator
to add a store directory, i.e. to enable the subsystem they turned off. The sibling
`IsStandaloneJetStream` and `IsClusteredJetStream` predicates also still use raw key presence
(`preflight.go:339-355`), so the package now has two contradictory definitions of “JetStream is on.”

Independent test
`TestManualTakeoverAcceptsExplicitlyDisabledJetStream` fails for both scalar forms:

```text
manual takeover rejected `jetstream: false`: existing conf enables jetstream but has no resolvable store_dir
manual takeover rejected `jetstream: disabled`: existing conf enables jetstream but has no resolvable store_dir
```

Impact: the advertised manual recovery/cutover path is permanently unavailable for a supported
configuration shape; the error directs the operator toward an unintended data-plane change.

Required fix: make every enablement decision use one value-aware predicate. Preserve the fail-closed
refusal for `jetstream: true|enabled` and `jetstream {}` with no resolvable store directory.

### B2-2 — MAJOR — `cluster status --json` gained a new sentinel meaning without a schema bump

> **主进程回复：完全采纳，选你给的第一个方案（bump 到 v2）。**
> 你留了"或者用 additive 字段编码而不改 v1 值域"这条退路，我评估后**否决**了它，理由记录在
> `statusSchemaVersion` 的注释里：**没有哪个非负整数能诚实表达"未测"**——`0` 正是本批删掉的
> 那个**编造的测量值**，而改成 `*int` 属于"改类型"，`docs/usage.md` 同一条政策也判破坏性。
> 任何诚实编码都要破坏 v1，所以值域必须扩，扩了就是 v2。
>
> **已修**：`statusSchemaVersion = 2`，v1→v2 契约（`>=0` 实测 / `-1` 未观测、非计数、恒 < target 故 fail-closed）
> 长文写在常量上，`docs/cluster.md` 同步（含"反面选项已评估并否决"的记录）。
>
> **你没点出但同源的第二半，我一并修了**：`ClusterStatusReport` 有**两个**生产者
> （broker socket 视图 + CLI offline 磁盘快照视图），各自写死 `SchemaVersion: 1`。
> 只 bump 会渲染的那个，会让**同一个 struct 声称两个版本**——按 `(view, schema_version)` 派发的监控
> 会看到"版本取决于哪个命令产出的报告"，这比不 bump 更坏。
> 版本号已收成 `adminsock.ClusterStatusSchemaVersion` 单一常量，两个生产者都引用它，
> 并加门 `TestClusterStatusProducersAgreeOnTheSchemaVersion`——offline 生产者若再写字面量即变红。
> 另加 `TestClusterStatusV2SentinelIsWireHonest`：`-1` 必须在 wire 上是 `-1`（不被 omitempty 吞、不退化成 0）、
> 与"实测 0"可区分、且比较低于 target。**你的反例保留为永久负控。**

Failed replica observation now writes `StreamActualUnobserved = -1`
(`internal/broker/clusterstatus.go:237-254,468-474`) and the CLI renders it as `?/target`. The stable
JSON still emits the integer as `"stream_actual":-1`, while
`statusSchemaVersion` remains `1` (`clusterstatus.go:73-75`) and
`ClusterNodeStatus.StreamActual` remains an always-present integer
(`internal/adminsock/protocol.go:685`).

This is not an additive `omitempty` field. It expands the value domain and changes the meaning of an
existing key. `docs/usage.md:1565-1571` explicitly requires a bump for a changed key meaning.

Independent test
`TestClusterStatusSchemaBumpsWhenStreamActualGainsAnUnobservedSentinel` captures the actual payload:

```json
{"schema_version":1,"nodes":[{"stream_actual":-1,"stream_target":3}]}
```

Impact: a v1 collector may treat every negative value as malformed, as a real count, or as a severe
deficit without receiving the discriminator the repository promises for breaking semantics.

Required fix: bump the socket/offline cluster-status schema and document the v1→v2 sentinel contract,
or encode “unobserved” additively without changing the v1 field's domain. Update all status
documentation and fixtures in the same change.

### B2-3 — MAJOR — unreadable-transfer recovery guidance cannot replay and can destroy the only synthesis input

> **主进程回复：完全采纳。这是本次最严重的一条——运维照着产品自己的文字做，会把唯一能合成终态的输入永久删掉。
> 按你给的修复顺序放在第一位处理。**
>
> 两条判断都复核成立：
> - `forEachLedgerRecord` 只对 `.json` 结尾的条目调 `fn`，`.json.corrupt*` 只被折回未解析普查 ⇒
>   "就地修复、下一轮会重放"**永远不会发生**，必须**改回规范的 `<hash>.json` 名字**。
> - in-flight start row 是唯一带 transfer id / verb / tier / bucket / path / startedAt 的记录 ⇒
>   跟 `.corrupt` 一起删掉后既无终态也无可合成之源，**永久不可审计**。
>
> **已修**：告警文本改成两条编号动作——(1) 可恢复：修好 JSON **并改名回 `<hash>.json`**；
> (2) 不可恢复：**只删** outbox 里的 `.corrupt`，**保留** inflight 那一行。
>
> **并按你的要求补了行为测试（不只是字符串测试）**：`internal/broker/xfer_corrupt_recovery_test.go`
> 让 finalizer 真的走每一条动作并断言结果：
>
> | 动作 | 结果 |
> |---|---|
> | 修好 + 改名回 `.json` | 重放出**真实的 `complete`** |
> | 修好但留 `.corrupt` 名（旧文案上半） | **什么也没发生**（永远） |
> | 只删 `.corrupt`、保留 start row | 合成 `failed/home_broker_restart` |
> | 删 `.corrupt` **和** start row（旧文案下半） | **永远没有任何终态** |
>
> 最后一行是承重行，刻意保留：它把"那条建议是破坏性的"从断言变成被检查的事实。
> 你扩写的 `TestUnfinalizableTransferIsReportedOnEveryPass` 三轮全绿并作为永久负控保留。

The recurring warning added at `internal/broker/xfer_inflight.go:690-696` instructs operators to:

1. repair a `*.json.corrupt` file “in place” because “the next pass replays it”; or
2. remove **both** the corrupt outbox row and the matching in-flight row so the finalizer synthesizes.

Both instructions are false:

- `forEachLedgerRecord` skips every non-`.json` file and only folds `.json.corrupt*` into the unreadable
  census (`xfer_inflight.go:367-382`). Editing its contents without renaming it to the canonical
  `.json` name can never make the replay callback see it.
- The primary in-flight start row is the only record from which the finalizer can synthesize a
  deterministic terminal. Deleting it together with the corrupt outbox row leaves neither source nor
  terminal, forever.

The extended `TestUnfinalizableTransferIsReportedOnEveryPass` fails on each of three passes and proves
the bad guidance is recurring, not a one-time log typo.

Impact: an operator following the product's exact recovery text can either perform an ineffective
repair or permanently remove the only audit-finalization input. This is a transfer audit-integrity
failure at the point where the product is supposed to make a permanent stuck state repairable.

Required fix: say that a repaired terminal must be validated and renamed back to its canonical
`<hash>.json` name. For an unrecoverable terminal, remove only the corrupt outbox row and retain the
primary start row so the next pass can synthesize. Add a behavioral recovery test, not only a string
test.

### B2-4 — MAJOR — webhook delivery success has replaced loop liveness in `cluster_loops`

> **主进程回复：完全采纳，实现你在 Required decision 里给的第一个选项（活性 + 独立的投递计数器）。**
>
> 这条特别值得记一笔：它和**内审 B7-01 的结论正好相反**。内审说"beat 无条件跳导致死端点看起来像
> 『告警在发出去』"，于是我把 beat 门在 `deliver() == true` 上；你说"这样一来活着的消费者报
> `iterations=0`，而这个字段的共享契约就是『0 表示没跑』"。**两条抱怨都对，这正是一个整数被问了两个问题的证据。**
>
> **已修**：
> - `beat` 回归 **per-completed-iteration**（出队 + 尝试投递即算一次，端点说什么都算）。
> - 新增 `accepted` / `rejected` 两个投递结果计数器，经 `RuntimeReport.AlertWebhook`
>   （omitempty 指针：不在场 = 没配 webhook；块内计数器**非** omitempty，零有意义）上报，
>   CLI 加 `ALERT_WEBHOOK` 表，并在 `rejected>0 && accepted==0` 时直接印一行
>   "consumer 活着、端点在拒——去查 URL/token，不是 broker"。
> - `accepted + rejected == iterations` 是可核对的不变量；`drops` 记在**入队侧**，刻意不进这个和。
> - CLI legend 也改了（原先写 `ITERS` = "alerts delivered"，那正是两种读法相撞的地方）。
>
> **内审那条测试是反转、不是删除**——它自己的注释就预留了这条路径：
> "if the project decides ITERS should mean 'processed' … then this test should be inverted, not deleted."
> 现在 `TestWebhookRejectedDeliveryIsLiveButNotDelivered` 在同一个 401 真 HTTP 场景下断言
> **两半**：beat 必须响（活着）**且** `accepted` 必须为 0（B7-01 原本的关切，迁到能表达它的字段上）。
> 你的 `TestWebhookLoopBeatTracksCompletedIterations` 保留为永久负控；另加三状态表
> `TestWebhookOutcomeCountersDistinguishTheThreeStates`（含 transport error 一行）。

The shared `loopSet` contract says `Beat` records one **completed iteration** and that `Iters/LastIter`
are per-iteration liveness (`internal/broker/loopset.go:56-72,146-160`). The admin protocol repeats
that an event-driven webhook loop with `Iters==0` means “nothing happened”
(`internal/adminsock/protocol.go:395-403`).

The changed webhook loop beats only when the endpoint accepts the POST
(`internal/broker/alert_webhook.go:165-178`). A live loop that dequeues an event and receives HTTP 401
or 503 completes its iteration but leaves `iterations=0,last_iter=null`. The file itself concedes the
row “is therefore not a liveness signal on its own” (`alert_webhook.go:92-98`), contradicting B7's
stated purpose and the shared type contract.

Independent test `TestWebhookLoopBeatTracksCompletedIterations` uses a no-socket fake transport,
returns 401, proves the event was consumed, and observes zero beats.

Impact: the runtime surface cannot distinguish “consumer loop never processed work” from “consumer is
live but delivery failed.” Conversely, calling the counter “accepted deliveries” does not make it a
loop-liveness counter.

Required decision: keep `cluster_loops` as liveness and beat after every completed dequeue/delivery
attempt; expose delivery acceptance/failure in a separate named counter. If the project instead wants
this field to mean accepted deliveries, rename/recontract it and bump the affected stable schema rather
than silently changing a shared liveness field.

### B2-5 — MAJOR — the strengthened leak gate can accept one real exercise as “at least five”

> **主进程回复：完全采纳，两个洞都是真的，按你给的 Required fix 逐条实现。**
>
> **锚点**：新增 `leakBaselinePos` —— 从断言调用的**实参**里取出候选变量名
> （`assertNoGoroutineLeak(t, label, before)` / `assertNoFDLeak(t, …, fdBefore)` /
> `pollGoroutinesAtMost(baseline+2, d)` 三种签名都覆盖，最后一种的变量藏在表达式里），
> 取断言前**最后一次**对这些变量的赋值作为测量点；合格循环必须**起始于基线之后**。
> 定位不到基线时**fail closed**（报告而非放行）——这道门的全部职责就是不报告未经核实的合规。
>
> **次数**：`loopRounds` 从"读条件右值"改成从 **init / cond / post 三个子句推导**
> （`ceil((N-S)/K)`，`i++` 即 K=1，`<=` 加一）；缺 init、三子句变量不一致、步长不可解析，一律返回 0（不可证）。
> 另加 `bodyCanExitEarly`：循环体内有 `break`/`goto`/`return` ⇒ 写下的界只是上限、不是发生次数 ⇒ 不可证；
> 嵌套循环/switch/闭包内的 break 不算（否则会误伤正确代码）。
>
> **两个反例行都转绿并保留为永久负控**，我另外补了 7 行同类：
> 非单位步长（10/2=5 通过、8/2=4 拒绝）、缺 init、三子句变量不一致、body 可 break、
> 嵌套 break 不误伤、以及"基线在 setup 循环**之后**重取 + 真实练习循环"——
> 最后这行是必要的对照：锚点必须拒掉 setup 循环而**不**连带拒掉紧随其后的真练习循环。
>
> **收紧后真树仍无 offender**（12 个泄漏断言站点全部满足），所以这次收紧没有靠改被测代码换绿。

`qualifyingLoopBefore` accepts any loop before the first assertion whose **reported bound** is at
least five (`test/determinism/leak_assert_shape_test.go:513-552`). `loopRounds` reads only the
condition's right-hand upper bound and ignores initializer, post statement, early exit, and the leak
baseline (`:630-650`).

Two synthesized independent mutations are falsely accepted:

- five unrelated setup iterations complete **before** the goroutine baseline, followed by one subject
  exercise;
- `for i := 4; i < 5; i++` runs once, but the scanner returns five.

Both rows fail in `TestQualifyingLoopPredicateDiscriminates`. The `±2` tolerance can therefore still
swallow a per-exercise leak of one while the new structural release guard reports compliance.

Impact: B9's central claim (“every leak assertion exercises its subject at least five times”) is not
established. This is exactly the false-green class the gate says it prevents.

Required fix: derive actual iteration count from init/condition/post for accepted `for` forms and
anchor the qualifying exercise region after the measured baseline. Reject unprovable shapes or require
the canonical shared helper/constant form. Keep both mutations as permanent negative controls.

## Minor findings

### B2-6 — MINOR — the “only Config assembly” guard is bypassable by a typed helper parameter

> **主进程回复：完全采纳。而且我把这条从 MINOR 提到"值得单独说"——你的反例推翻了那个文件里
> 一句我自己写下的断言："That is not fixable by a better parser."** 它是可以的。
>
> **已修**：`configFieldsIncludingAssignments` 现在记录 Config-typed 的**参数、命名返回值和 receiver**
> （`isConfigType` 递归解指针，因为你的反例走的是 `*natsconf.Config`）。
> 你的 `TestConfigAssemblyScannerSeesTypedHelperMutation` 转绿；
> 我用你那段代码原样注入 `internal/broker` 做了变异验证——门变红并点名文件与字段。
>
> **一个必须如实说明的连带后果**：补上参数/receiver 感知之后，扫描器立刻在 `internal/natsconf` **内部**
> 发现两处 Config 变更——`Config.ApplySecretsDirIdentity`（方法）与 `BuildMergedConf` 的 harvest 回填
> （按值收 `cfg Config` 并填空字段）。**它们不是竞争性装配点，而是共享渲染管线本身**，
> 调用方除了经 `RenderDesired`/`BuildMergedConf` 无法到达它们。
> 所以规则被**精确化而非放宽**：`internal/natsconf` 外零容忍；包内的合法变更者进
> `renderPipelineMutators` 白名单**逐条带理由**，并加"陈旧条目必红"的反向断言。
> 这比原状态强：在扫描器看不见参数与 receiver 之前，"只有一个装配点"只对它恰好能匹配的形状成立。
>
> 你建议的 `go/types`/SSA 我**没有**上：两把 AST 扫描器 + 白名单已经把你给的反例和方法形状都覆盖，
> 而引入类型检查会把这个门的依赖面和运行时间都抬一档。这是一个取舍决定，记录在此供下一轮复议。

The assignment-aware scanner records local variables declared as `Config`, then matches assignments by
identifier name only (`internal/natsconf/config_assembly_baseline_test.go:183-237`). It never records
function parameters or performs type resolution.

This valid second assembly is invisible:

```go
func applyLocalIntent(desired *natsconf.Config) { desired.Standalone = true }
func build(...) {
    var cfg natsconf.Config
    applyLocalIntent(&cfg)
    natsconf.BuildMergedConf(own, cfg)
}
```

`TestConfigAssemblyScannerSeesTypedHelperMutation` returns `found []`. The file's rationale that a
mutating helper “can only be reached through” the one assembly is not structurally true.

Current production still appears to use one assembly, so this is guard weakness rather than a
demonstrated second live assembler. Use `go/types`/SSA or constrain and scan all calls to
`BuildMergedConf` plus typed mutations; retain the counterexample.

### B2-7 — MINOR — cleanup and documentation claims overstate the implementation

> **主进程回复：三条全部采纳。第一条我选择"完成删除"而不是"改散文"。**
>
> **(1) `Config.Account`**：复核确认全仓**零写入者**（`Render` 里 `if account == ""` 是唯一可达分支），
> 且 `BuildMergedConf` **不**从 live conf harvest account 名（只 harvest issuer，经 `AuthIdentity`）。
> 这与已删的 `JSDomain` 是同一形状，所以**把字段删掉**，`Render` 内改成 `const account = "$G"`，
> 并注明"若将来真需要非默认 account，它属于 `RenderOverride` 里由调用方显式索取"。
> 四处 fixture（`internal/natsconf` ×3、`test/d3` ×1）传的都是 `"$G"`，与常量同值，
> **goldens 一字未动** —— 这就是"它确实是死管路"的证明。
> `render_desired.go` 与 `cluster_grow_cutover.go` 的注释同步订正为"两者现在都真的没了"，
> 并写明 Account 曾在第一轮存活、由本次外审抓到，因为**声称删除不等于删除**。
>
> **(2) `docs/broker-ops.md` 的 "都是 omitempty"**：属实写错。已订正——
> `cluster_loops[].last_iter` / `.iterations` **刻意不是** omitempty，理由（零就是"死"这个含义本身，
> 在坏消息发生时丢键等于删信号）引自 `ClusterLoopInfo` 自己的注释；
> 不 bump 的真实理由是**只加键、不改已有键含义**，已改写为这个表述。顺带把 B2-4 的两计数器一并写进该段。
>
> **(3) `jetstream_enablement_test.go` 仍自称 expected-to-fail**：已改为 `STATUS: FIXED`，
> 并把 before/after 两列并排列出（前两行从 REFUSES 变成 renders，后两行不变且拒绝是**正确**的）。
> 该文件末尾那段"SUGGESTED FIX"里让两个拓扑判据跟随同一判据的建议，**已在原处标为 REJECTED 并附两条回归证据**
> （见 B2-1 回复）——不留悬着的建议，否则下一个人会照着做。

- `render_desired.go:13-14` and `cluster_grow_cutover.go:247-252` say both `Config.JSDomain` and
  `Config.Account` were deleted. `Config.Account` still exists and is read by `Render`
  (`internal/natsconf/render.go:39-43,92-95`). The dead-plumbing cleanup is therefore incomplete and
  the new invariant comments are factually false.
- `docs/broker-ops.md:444-447` says the same-batch runtime additions are all `omitempty`, but
  `cluster_loops[].last_iter` and `.iterations` are deliberately not `omitempty`
  (`internal/adminsock/protocol.go:407-422`).
- `internal/natsconf/jetstream_enablement_test.go:13-34` still declares itself expected to fail and
  describes the pre-fix `HasJetStream=true` result even though the same unstaged layer made it pass.

These do not independently corrupt runtime state, but they make future compatibility and cleanup
decisions rely on claims the source contradicts. Correct the prose or complete the deletion.

## Doubts and open questions

> **主进程逐条答复（见每条下方缩进段）。**

1. Should `stream_actual=-1` remain the long-term machine representation, or should the JSON use an
   additive observation-status field and reserve `stream_actual` for measurements? Either is viable,
   but v1 cannot silently acquire the new meaning.

   > **答：保留 `-1`，走 v2。** 理由在 B2-1… 更正，在 B2-2 回复里：additive 方案听起来更保守，
   > 实际上**同样破坏 v1**——`stream_actual` 必须为"未测"给出某个整数，而 `0` 正是本批删掉的编造值，
   > 改 `*int` 则是"改类型"。所以两条路都要 bump，选值域扩张这条，因为它让 fail-closed 比较
   > （`actual < target`）继续免费成立。已把"另一条路评估并否决"写进 `docs/cluster.md`，
   > 免得下一轮把它当成没考虑过。

2. Is webhook `iterations` intended to answer loop liveness or successful delivery? The code currently
   tries to make one integer answer both. The report recommends two counters.

   > **答：活性。** 已按你的建议拆成两个计数器，见 B2-4 回复。
   > 附一句诊断：这个字段之所以来回改了两次，是因为**共享类型的契约写在 `loopStat`/`ClusterLoopInfo` 上，
   > 而单个实现可以悄悄给它换含义**——这类漂移下次仍会发生。`accepted+rejected == iterations`
   > 这条可核对的不变量就是为此设的：两个数字必须互相解释，谁被换了含义都会露出来。

3. `clusterReplicaObserveBudget` scales only with active SQLite sessions, while `ObserveReplicas`
   separately enumerates every live `OBJ_xfer-*` stream, including rows that can outlive a session.
   The 30-second ceiling keeps the leader tick bounded, but a transfer-heavy/orphan-heavy fleet may
   still become routinely “unobserved.” I found no workload characterization for that term.

   > **答：这条是有效的未知，我不假装它已被刻画，也不在本轮改。**
   > 你说得对：预算按活跃 session 数线性放，而 `ObserveReplicas` 另外枚举每个活的 `OBJ_xfer-*` 流，
   > 后者可以比 session 活得久（孤儿桶）。我没有任何 transfer-heavy 车队的实测数据来定这一项的系数——
   > 编一个出来正是本批一直在删的那种"看起来像刻画的数字"。
   > **现状的诚实边界**：30s 天花板保证 leader tick 有界（这是硬要求），超时后报 `-1` 而不是编造测量值
   > （这是 B2-2 那条 sentinel 的价值），且 `-1 < target` 使一切达标判定 fail-closed。
   > 也就是说**最坏情况是"routinely unobserved"这个可见状态，而不是静默误判为已收敛**。
   > 要真正定这个系数需要按孤儿桶数量做一次 deploy-tier 刻画，属于独立增量；
   > 在那之前把它当作已声明的观测缺口，而不是已解决的问题。

4. The origin-line gate proves cited document paths exist, not that an old filename or round
   attribution is authentic. This is acceptable as a broken-link guard, but it should not be described
   as provenance verification without a frozen mapping.

   > **答：接受这个限定，措辞已改。** 那道门是**断链守卫**，不是溯源验证——它能证明被引用的
   > `docs/reviews/*.md` 存在，不能证明"这个测试真的出自那一轮"。
   > 真要验证后者需要一份冻结的 旧名→新名 映射，而那份映射的权威来源是 git 的改名记录本身
   > （`git log --follow`），把它抄进代码只会得到第二份会腐化的真相。
   > 所以：门只做断链，认领的真实性由这批改名**在同一个 commit 内完成**这一事实承担。

5. Simcluster drill `41-shrink-to-standalone` remains INCOMPLETE because the first non-final retire did
   not prove proactive physical disconnect and reconnect-to-a-voter in-window. Functional reachability,
   final N=1 escape, de-cluster, restart, data-row survival, and tier-B transfer all passed. Is that
   fast-path coverage gap explicitly waived for this release? No waiver was present, so policy keeps it
   blocking.

   > **答：不豁免，也不宣称已修——它与 B5–B9 无关，且我不会为了让 drill 变绿去改产品。**
   > 你问得对，此前**没有**豁免记录，这是我的漏项。现在明确记录如下：
   >
   > 这两个 GAP 是 agent 在"**非终态** retire、被退役 broker 的 nats-server 仍在网格中"时，
   > 不走 `rosterRequiresReconnect` 主动快路径。根因在 drill 输出里已写明：非终态 retire 下
   > 被退役 broker 仍然在网格里应答 roster refresh，所以静默重建路径（它只在**真隔离**时触发，
   > 而同一 drill 的最终 N=1 孤岛那一臂**证明了它会触发**）按设计不会 fire。
   > agent 在窗口内保持**功能可达**（同 drill 断言），并在真正下线时经 ~20s 断连看门狗迁移。
   >
   > **这属于快路径优化未被证实，不是正确性失败**，且它**先于本批存在**——
   > B5–B9 不碰 agent 的 roster 重连路径，两轮 drill（我的与你的）逐字给出同两个 GAP 也印证了这点。
   > 按 simcluster 的定位铁律（"忠实暴露、绝不替 tether 弥补"）它应当**保持 recorded 状态**，
   > 由一个独立的 agent-rehome 增量去关，而不是在一个 natsconf/registry/ledger 批次里顺手改动
   > 重连时序。**我不请求把 41 记成 GREEN**；它在本轮的处置是"已声明、已归因、不豁免、不阻塞
   > B5–B9 的其余部分"，是否放行由你决定。

## Verification evidence

### Focused and package tests

- Six reviewer counterexample groups fail deterministically:
  manual takeover; cluster-status schema; webhook liveness; transfer recovery guidance; Config
  scanner; leak-shape scanner.
- The affected package set passes when those exact tests are skipped:
  `internal/natsconf`, `cmd/tether`, `internal/broker`, `internal/cluster`,
  `internal/testharness`, `internal/tunnel`, `test/determinism`, and `test/concurrency`.
- Pre/post unstaged test-function scan found only the intended new reviewer/coverage tests and the
  planned observation-budget/assembly-gate renames; no hidden test-function deletion was found.

### Static, race, and repository gates

- `make lint`: **PASS**, `0 issues`.
- `gofmt` sweep and `git diff --check`: **PASS**.
- All integration build tags together:
  `go vet -tags 'phasefluidity_integration,c7_integration,d5_integration,d6_integration,d7_integration,d8_integration,d9_integration,e2e_matrix' ./...`:
  **PASS**.
- Compile-only `go test -run '^$'` with the same tag set: **PASS**.
- Affected packages under `-race`, excluding only the six intentional reviewer reds:
  **PASS** (`internal/broker` 480.460s; `internal/cluster` 113.597s;
  `internal/tunnel` 4.942s; `test/concurrency` 44.904s; natsconf/cmd also pass).
- `go test -race ./test/concurrency -count=3`: **PASS**, 126.332s.
- `make test`: **FAIL**, only on the reviewer counterexamples.
- `make e2e-parallel`: coverage self-check **15/15**, `TestAllPhases` **PASS**,
  wall 3m23.739s; eight failed shards, all attributable to the reviewer schema/webhook/transfer/config/
  leak counterexamples. No unrelated matrix failure appeared.

### Deploy-tier simcluster

Host: local `weilandserver`, using `test/simcluster/local.sh`; no SSH and no prior result reuse.
The image was rebuilt from current source. Image `/usr/local/bin/tether` and staged `vendor/tether`
both hash to:

```text
cbf04dcfff5272687bc6f63d16f4d0c7edd62c4fd29437bb6d8cc9f45c1f6f88
```

Isolated drill results (508s total):

| Drill | Verdict | Evidence |
|---|---|---|
| `10-grow-to-3` | GREEN | 19 pass |
| `20-forcesingle-natsconf` | GREEN | 16 pass |
| `41-shrink-to-standalone` | INCOMPLETE | 31 pass, 2 recorded proactive-rehome gaps |
| `43-migrate-live-data` | GREEN | 38 pass |

No PRODUCT-RED, SETUP-RED, ASSERT-FAIL, or infra-abort occurred. Every result matched the repository's
recorded expectation, and all drill containers were cleaned. “Matched expectation” does not turn
INCOMPLETE into release-green.

## Recommended fix order

1. Correct the transfer recovery instruction before anyone can follow it.
2. Unify all JetStream enablement predicates and repair the manual takeover.
3. Decide and implement the cluster-status v2 representation.
4. Split webhook loop-liveness from delivery outcome.
5. Strengthen both structural guards with the retained mutations.
6. Correct cleanup/documentation claims, then rerun all gates and the four simcluster drills from a
   rebuilt image.

The independent red tests should remain red until the main process deliberately fixes or adjudicates
their contracts; weakening or deleting them without a recorded authority decision would not resolve
the findings.

---

## 主进程处置总表（2026-07-27，逐条回复见上文各 finding 下方）

按你给的 Recommended fix order 执行，一条不落。

| # | 严重度 | 处置 | 你的反例现状 | 变异验证 |
|---|---|---|---|---|
| B2-3 | MAJOR | **已修**（第一位处理）+ 补行为测试 4 行表 | 转绿，保留为负控 | 四条动作各自的结果被断言，含"旧建议 ⇒ 永远无终态" |
| B2-1 | MAJOR | **已修**；建议里让拓扑判据也 value-aware 的那半**驳回并附两条回归证据** | 转绿，保留为负控 | 新增 `TestJetStreamEnablementIsDecidedInExactlyOnePlace`，带合成正/负控 |
| B2-2 | MAJOR | **已修**：bump 到 v2 + 版本号收成单一常量（你未点出的第二个生产者漂移） | 转绿，保留为负控 | 新增两道门：跨生产者一致 + sentinel wire 诚实 |
| B2-4 | MAJOR | **已修**：两个计数器；内审那条相反的测试按其自身注释**反转**而非删除 | 转绿，保留为负控 | 三状态表 + `accepted+rejected==iterations` 不变量 |
| B2-5 | MAJOR | **已修**：锚点 + 从三子句推次数 + 早退不可证 | 两行全部转绿，保留为负控 | 另补 7 行同类，含"锚点不得误伤真练习循环"对照 |
| B2-6 | MINOR | **已修**：参数/返回值/receiver 感知（含解指针）；规则精确化为白名单 | 转绿，保留为负控 | 你的代码原样注入生产包 ⇒ 门变红点名 |
| B2-7 | MINOR | **已修**：`Config.Account` 真删（非改散文）+ 两处文档订正 | — | goldens 一字未动 = 死管路的证明 |
| 疑问 1–4 | — | 逐条答复；其中疑问 3（观测预算未刻画）**接受为已声明缺口**，不假装已解决 | — | — |
| drill 41 | INCOMPLETE | **不请求记为 GREEN**；已声明、已归因为先于本批的 agent 快路径缺口 | — | 两轮 drill 逐字同两 GAP |

**未采纳/部分采纳的清单（只有两项，均已在对应 finding 下附证据）**
1. B2-1 的"`IsStandaloneJetStream`/`IsClusteredJetStream` 应跟随同一判据"——**驳回**，
   会让 force-single 静默不脱簇、并使 lone + `jetstream: false` 的 broker 永久 `ActionRejected`。
   矛盾改由命名消除（`hasJetStreamKey`）。
2. B2-6 建议的 `go/types`/SSA——**未上**，两把 AST 扫描器 + 白名单已覆盖你给的两种形状；
   取舍（依赖面与门的运行时间）记录在案供复议。

### 修复后的重新验证

- `make test`：**PASS**
- `make lint`：**PASS**（`0 issues`；含我新写代码的 2 条 staticcheck 与 2 个 gofmt，已修）
- `make e2e-parallel`：**ALL PASS**，wall **3m28s**（唯一的全矩阵闸门）
- 全 tag `go vet`（`phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`）：**PASS**
- **行号锚定的 `unresolvedCodeSites` 第 10 次因上方加注释而漂移**，已重钉 12 条；
  这次把"file:line 作为 exemption key 每次都要机械重钉、且重钉与『悄悄压掉一个新站点』
  在 diff 上无法区分"这一结构问题写在表头，并给出稳定 key 的方向——
  但**没有在外审当中改门本身**，那是下一个人的第一个问题。
- deploy tier：镜像已从修复后的树 `--build` 重建；证明方式是**等值构建**——
  用 `local.sh` 同一命令行重编一份与 baked `vendor/tether` 比 sha256，得
  `c0bc510cef763585bca774c68d84f3c26eb33f9de76a1e4588ac6a4e253963d1`
  （与你那轮的 `cbf04dcf…` 不同，即确为修复后的树）。四个 drill 的重跑结果见下方补记。

> 注：我这一轮**没有**用字符串探针证明二进制新鲜度。上一轮我用过，这次发现它不成立并如实记下：
> `RenderIntent.String()` 只被测试调用，Go 链接器把死代码连同字面量一起裁掉，
> 所以 `strings` 探不到 `"standalone-if-lone"` **不代表**二进制是旧的。等值构建是更强的证明。

#### drill 重跑结果（重建镜像，sha256 `c0bc510c…`）

| drill | 结果 |
|---|---|
| `10-grow-to-3` | **GREEN**（19 断言，0 gap） |
| `20-forcesingle-natsconf` | **GREEN**（16 断言，0 gap） |
| `43-migrate-live-data` | **GREEN**（38 断言，0 gap）——这条覆盖 B2-1 改到的手工 takeover 路径 |
| `41-shrink-to-standalone` | 见下 |

**41 的第一次重跑是 SETUP-RED，原因在我，如实记录**：我先用一次带 10 分钟超时的前台调用启动它，
超时把调用杀了，但 drill 的**容器与卷存活**（日志里 `provision: already provisioned … 23:55:20Z`），
第二次调用于是复用了脏实例，`session create` 撞上上一轮已建的 `lab` ⇒
`verdict=SETUP-RED pass=0 product_red=0 assert_fail=0`。
这是 harness 状态污染，**不是产品失败**（`product_red=0` 即为此）。
已 `nuke` 该实例（容器/卷/secrets stash/backup vault 全清，验证为 0 容器 0 卷）后从干净状态重跑，
结果附在本节末尾。

记下这件事是因为它本身是一条纪律：**被超时杀掉的 drill 不会自我清理，而复用脏实例产生的红
在 verdict 行里与真实缺陷长得不一样但很容易被一眼看成一样**（`pass=0` + `product_red=0` 是它的指纹）。
下次跑长 drill 一律 detach，不要用前台超时去卡它。
