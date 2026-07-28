Pass

# batch B2 debt cleanup — external review

> Reviewer: independent external reviewer
> Date: 2026-07-28
> Intake: empty index; 14 modified + 2 untracked developer paths; intake unstaged patch SHA-256
> `ae4cf0c9f328609ea3b4a471d50a743b7db9bea6096493f472204693dc3b8fd5`.
> Tasklist: `docs/reviews/batch-b2-debt-external-review-tasklist.md`

## Final verdict

**PASS.** The reviewed developer layer was first frozen and staged with the pre-fix Fail verdict. After
that boundary, the reviewer used the user's explicit authorization to close F1–F3. The final worktree
passes the full unit, lint, race, all-tag compile/vet and sole parallel E2E gates. No release-blocking
finding remains.

## 1. Pre-fix verdict

**FAIL.** The stable-key migrations, agent wiring coverage, drill-41 ledger rewrite and most of the
internal-review responses are sound, but the stream-budget change contains one operational regression.
An independent regression test fails in both `make test` and the D4/D5 broker shards of
`make e2e-parallel`. Two smaller issues also need correction before release: the raft timing exemption
set is larger than the actual exception, and the newly documented loopback asymmetry has a safe local
fix rather than requiring an unresolved policy choice.

This verdict does not trust `docs/reviews/batch-b2-debt-review.md` as authority. I reconstructed each
claim from current source, the pre-change tree, executable guards, the binding documentation and a fresh
simcluster run.

## 2. Findings

### F1 — MAJOR / release-blocking: stale stream cache can permanently under-budget after session growth

`observeStreamCountForBudget` returns `lastReplicaStreams` whenever it is non-zero and never consults the
live `ACTIVE` session count in that state. That makes the new predictor strictly worse than the old one
after growth:

- cached observation: 1 stream;
- current DB: 100 active sessions;
- new budget: `3s + 1×250ms = 3.25s`;
- old session floor: `3s + 101×250ms = 28.25s`.

The comments acknowledge the smaller first budget but claim the next observation self-corrects. It
cannot. `ObserveReplicas` returns an empty report plus an error on the first failed stream collection;
`observeAndCache` only calls `cacheReplicaSnapshot` when `err == nil`; and the cache rejects unobserved
reports anyway. A timeout therefore preserves the stale count and can repeat forever. On the leader this
can keep status/retire convergence observations permanently UNOBSERVED after an otherwise healthy
growth burst.

Independent proof:

- added `TestObserveBudgetDoesNotRegressBelowLiveSessionFloor`;
- focused result: `budget input after session growth = 1, want 13`;
- `make test`: all other packages passed; this test was the only failure;
- `make e2e-parallel`: the same test was the only failure, independently in
  `D4:internal/broker[6/8]` and `D5:internal/broker[6/8]`; 97 other units passed.

Required correction: combine the last enumerated stream count and current `sessions + events` floor;
make failed serial collection return the complete work-set count without presenting partial states as an
observation; and bound the alert reconciler's third observation call as well. Also remove the false
“next tick self-corrects” and “never worse than old behavior” claims.

### F2 — MINOR: two of three raft timing exemptions are unnecessary and their reasons are false

The re-key correctly discovered three syntactic non-conforming fields on one source line, but “three
sites” does not imply “three justified exemptions”. Production values are:

- `MultinodeHeartbeatTimeout = 1s`;
- `MultinodeElectionTimeout = 1s`;
- `MultinodeLeaderLeaseTimeout = 500ms`.

The failure fixture uses `1s / 1s / 2s`. Only the `2s` lease must differ. Replacing heartbeat and
election with their production constants preserves the exact intended `LeaderLeaseTimeout >
HeartbeatTimeout` `raft.ValidateConfig` failure. The current #2 reason additionally suggests election
must be moved with heartbeat to preserve the failure, but both production constants are already 1s and
the election relation is valid.

Required correction: reference production constants for heartbeat/election and keep one site-scoped
exemption for the 2s leader lease. Update the gate narrative and ordinal accordingly.

### F3 — MINOR: unchanged loopback roster churn has a determinate symmetric fix

`rosterRequiresReconnect` currently filters the current roster entry before testing whether it names the
connected broker, while `rosterContainsHost(previous, host)` does not filter. The new test correctly
shows that a byte-identical loopback roster is read as “present before, removed now”, rebuilding on every
refresh.

The response says fixing it requires choosing which side should filter. The safer invariant is more
specific and removes that ambiguity: identify the current broker before applying dialability filtering;
apply the filter only when deciding whether another voter is a usable destination. Then:

- an unchanged loopback current broker remains present and does not churn;
- the same broker becoming RETIRING still triggers re-home when a dialable other voter exists;
- undialable peers are still not counted as destinations.

Required correction: reorder the current-entry match ahead of the undialable-host filter and flip the
recorded-asymmetry test into positive behavior, including a RETIRING control.

## 3. Accepted changes

- The cmd/tether, auth and raft scanners now use receiver-qualified
  `file:FUNCTION#ordinal` keys with per-file counters and live-entry checks. Synthetic collision rows
  cover same function, different functions, same-named methods on different receivers and file-scope
  sites separated by declarations. The documented same-count replacement blind spot remains real and
  honestly stated.
- The 51 error-code exemption migration is site-injective in the current tree; no new unresolved site is
  silently absorbed. Dynamic ACL scanning now covers both source trees used by its forward extractor.
- The new roster refresh wiring test reaches REMOVED, RETIRING, unchanged, unrelated and rejected
  generation paths through a real request/reply connection. `-race -count=10` passed.
- The string-identity blind spot is real and the limitation test self-destructs if fixed. A complete fix
  needs a stable broker identity in the signed roster (or another binding contract); I do not infer a
  safe wire change in this debt patch.
- Drill 41, TSV, verdict log and coverage-boundary prose agree on `INCOMPLETE / 2 gaps`. The new gap
  reasons distinguish measured movement from unproven candidate mechanisms.

## 4. Fresh simcluster evidence

Fresh local run on `weilandserver`:

```text
DRILL-VERDICT verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0
not_covered=2 nc_gap=2 nc_guard=0 pass=31
```

All grow, retire, de-cluster, store move-aside, restart, #48 escape, session-survival and tier-B hard
assertions passed. Before teardown I independently captured this run's agent journal and `/connz`:

- registered through brk2 at `13:15:56.350Z`;
- proactive decision at `13:16:54.929Z`;
- registered again at `13:16:54.972Z` (43ms after the decision);
- brk2 `/connz`: agt1 present before the move and absent afterward;
- brk1 `/connz`: agt1 present afterward.

This is a sixth observed run and a successful sample at roughly 58.6s. The two unconditional gaps still
represent the historical reliability claim rather than a failure observed in this particular run. That
is honest but not self-closing: once the product supplies a retryable/bounded wake-up contract, these
sites should become hard assertions rather than permanent historical annotations. The isolated
simcluster instance was clean after teardown.

## 5. Pre-fix verification

- `git diff --check`, gofmt: PASS.
- `make lint`: PASS, `0 issues`.
- all-tag `go vet` and compile-only `go test` for
  `phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`: PASS.
- changed stable-key guards: PASS, repeated three times.
- agent proactive re-home focused `-race -count=10`: PASS.
- broker budget tests excluding the new regression, `-race -count=3`: PASS.
- simcluster hermetic `tests/run-all.sh`: ALL PASS.
- drill 41: expected INCOMPLETE, 31 pass + 2 gap, no other red; cleanup PASS.
- `make test`: FAIL only F1's reviewer regression.
- `make e2e-parallel`: FAIL only F1's reviewer regression in two tagged broker shards; scheduled
  coverage self-check 15/15 and all other units passed.

## 6. Residual doubts and recommendations

- The 250ms coefficient and 30s ceiling remain uncharacterized; the ceiling binds at 108 streams. The
  proposed F1 fix prevents a regression below the old session floor but does not prove that
  transfer-heavy fleets below/above the cap complete. Keep this as explicit operational debt.
- The proactive re-home path still identifies the current broker by hostname string identity. A vanity
  name, CNAME or discovered IP not present in the roster can silence both arms. Do not relabel drill 41
  GREEN until a stable identity and retry/bound contract are delivered and tested.
- Ordinal exemption keys deliberately cannot detect a same-function remove+add replacement that keeps
  the unresolved count constant. Reasons must be re-read whenever such a function changes.

## 7. Staging and authorized-fix boundary

The complete developer + external-review layer (including the failing regression, tasklist and pre-fix
Fail report) was staged first:

- cached patch SHA-256:
  `ceb85f78d06317db06badc4f80be40882ec8ebd1d6f90786eddad473524973ed`;
- `git diff --cached --check`: PASS;
- `git diff --quiet`: PASS at the boundary.

Only after those checks did the reviewer edit implementation. No `git add` was run afterward. The
cached patch hash remained byte-identical through final verification.

## 8. Authorized fixes and dispositions

### F1 — fixed

- `observeStreamCountForBudget` now takes `max(last enumerated stream count, current active sessions +
  events)`, so a stale cache can never regress below the old session floor.
- `ObserveReplicas` lists SIDs and live `OBJ_xfer-*` names before serial state collection and returns the
  complete `StreamCount` even when collection later times out.
- `cacheReplicaSnapshot` may use that failed-pass count only for the next deadline; it still refuses to
  update replica posture/gauges from an unobserved pass.
- The alert reconciler's observe wrapper now owns the same derived timeout as the other two
  cluster-maintenance call sites. The structural guard pins exactly three observation calls and three
  derived deadline contexts.

This closes both permanent-underbudget shapes: session growth and transfer-only growth.

### F2 — fixed

`TestD3FailingNewReapsTransportNoLeak` now references production heartbeat/election constants and keeps
only the deliberate 2s leader-lease literal. The stable-key exemption map shrank from three entries to
one and its reason names the exact `LeaderLeaseTimeout > HeartbeatTimeout` failure.

### F3 — fixed

`rosterRequiresReconnect` now identifies the connected broker before filtering undialable *destination*
entries. Tests prove both directions: an unchanged loopback roster does not churn, while a loopback
current broker becoming RETIRING still re-homes to a dialable peer.

## 9. Final verification

- `make test`: PASS; broker package `281.098s` on the uncached post-F1 run. An intermediate run correctly
  caught one stale promised-test-name comment after the structural guard rename; the comment was fixed,
  the promise gate repeated three times, and the final complete run passed.
- `make e2e-parallel`: **ALL PASS**, 15/15 top-level coverage, 99/99 units,
  `3m27.964s`; `TestAllPhases` `1m55.802s`.
- `make lint`: PASS, `0 issues`; gofmt and `git diff --check`: PASS.
- `CGO_ENABLED=0 go build ./...`: PASS.
- all-tag `go vet` and compile-only `go test` for
  `phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`: PASS.
- broker budget/report/structural tests: `-race -count=3` PASS.
- agent roster tests: `-race -count=10` PASS.
- cluster failure-cleanup fixture: `-race -count=5` PASS.
- raft timing and promise guards: repeated PASS.
- simcluster hermetic gates: ALL PASS; drill 41 fresh run remained the expected
  `INCOMPLETE / pass=31 / gap=2 / all other red=0`, with clean teardown.

## 10. Final residuals

Two explicitly non-blocking debts remain:

- the 250ms per-stream coefficient and 30s ceiling are still not fleet-characterized;
- proactive re-home still cannot identify an alias/IP absent from the signed roster. It remains an
  optimization gap with correctness fallback, and a complete fix requires a stable signed broker
  identity/binding rather than another hostname guess.

Neither is hidden or relabelled GREEN.

---

## 11. 主进程对本轮外审直接改动的审核（2026-07-28）

外审者本轮**直接修改了实现**（10 个文件，全部保留在暂存区外）。按 §4 的角色边界，实现的最终责任在主进程，
所以我把这 10 个文件当作"待审的补丁"逐条核验，而不是当作既成事实接受。结论：**F1/F2/F3 三条 finding 全部成立、
三份修复全部采纳**，另**补一处外审自己漏掉的测试层**。

### 11.1 逐条核验

**F1（MAJOR）—— 成立，修复正确。** 我复现了它指出的形态：缓存 1 条流 + 现网 100 个 ACTIVE session 时，
新预测器给 3.25s、旧的 session 项给 28.25s，**我的改动在这个形态下严格劣于它替换掉的东西**。
`max(上次枚举的流数, 当前 sessions+events)` 是对的：两个下界各自看得见对方看不见的东西
（前者看得见 OBJ_xfer，后者看得见上次观测之后的 session 增长），取大保留覆盖而不可能低于旧地板。
把工作集枚举提到逐流采集**之前**、并在失败路径上返回 `StreamCount`，也正确地拆开了两件被我混在一起的事：
「这次没测到」（`Observed=false`，量表/退休闸继续 fail-closed）与「这次有多少活」（下次预算的输入）。
第三处 `ObserveReplicas` 确实存在且确实吃进程级 context——我的 §11.2 M7 段落只谈了 leader/follower，
**没发现这个调用点根本没有 deadline**。变异验证：还原 `max()` ⇒ 新增的 live-floor 测试红；
去掉第三处 `WithTimeout` ⇒ 结构门报 `3 个调用 / 2 个 deadline`。

**F2（MINOR）—— 成立，而且比我的处理更准。** 我把「重键发现一行上有 3 个语法站点」直接当成
「需要 3 条豁免」，这是一步没走完的推理。生产常量 `MultinodeHeartbeatTimeout` / `MultinodeElectionTimeout`
**本来就是 1000ms**，与 fixture 里的字面量逐位相同，所以把这两个换成常量后 fixture 语义**完全不变**，
而豁免从 3 条缩到 1 条——只剩真正制造 `LeaderLeaseTimeout > HeartbeatTimeout` 的那个 2s。
我写的 #2 理由（「election 必须跟着 heartbeat 动」）**是错的**：两个生产常量相等，election 关系本来就合法。
变异验证：把 heartbeat 改回字面量 1s ⇒ 门红并报出新的 `#2` 站点。

**F3（MINOR）—— 成立，我当初的"不修"判断是错的。** 我把它记成「要决定哪一侧过滤」的开放策略题并拒绝在发布后拍脑袋。
外审给出的不变量比"选一侧"更具体、也确实消解了那个两难：**可拨号性过滤的是"目的地"，不是"身份"**。
先认出当前 broker、再对**其他**条目做过滤，三个方向同时正确（未变更不抖动、RETIRING 仍疏散、不可拨号的 peer 仍不算目的地）。
这不是在两个坏选项里挑一个，是我没找到第三个。原来那条自毁式的"记录局限"测试也被正确地翻成了正向双向断言。
变异验证：把过滤挪回匹配之前 ⇒ 未变更 roster 立刻被读成移除、测试红。

### 11.2 我在外审修复中发现并补上的一处：F1 的修复自己没有测试

`ObserveReplicas` 是 F1 的核心改动（枚举提前 + 失败路径带 `StreamCount`），但它在 `internal/broker` 里
**零测试调用者**。新增的那条断言是手工造一个 `ReplicaReport{StreamCount: 17}` 直接喂 `cacheReplicaSnapshot`——
它证明了**缓存**会采纳 `StreamCount`，没有证明**产出方**会给出 `StreamCount`。
我做了变异确认：把 `failed()` 还原成 `return ReplicaReport{}, err`（即 F1 修复的逐字节回退），
**`internal/broker` 整包仍然全绿**。

这与本轮内审提给我的 **M3 是同一个形状**——「被改的函数只经由它的消费者被测到」——出现在 M3 的修复本身里。
新增 `internal/broker/audit_publisher_replicas_test.go` 直接驱动真函数（真 JetStream、不存在的流让采集必然失败），
钉住三件事：`Observed=false`、`Streams` 为空（半份状态不得让 `AllAtTarget` 作答）、
`StreamCount` 等于完整工作集（events + 每个 active session + 每个 OBJ_xfer）。
另一条钉**顺序**：`ListSIDs` 在采集之前恰好被调用一次——枚举放在采集后面，就恰好在最需要它的那一趟贡献不了任何东西。
上述变异现在会让这两个测试红。

顺带修掉重命名留下的两处文字不一致（结构门注释仍说"两个调用点"、错误串仍说 "on the leader tick"），
以及 `batch-b2-plan.md §8` 里一个指向已改名测试的引用。

### 11.3 遗留（同意，不在本次关闭）

§10 的两条非阻塞债我全部同意并原样保留：250ms 系数与 30s 上限仍未按车队刻画；
主动疏散仍靠主机名字符串同一性认人。**drill 41 不因本轮改动翻 GREEN**——它仍是 `INCOMPLETE / 2 gaps`。
外审第六次运行（43ms、约 58.6s）是又一个成功样本，但它不改变"延迟无界"这个结论，
两条 gap 记录的是可靠性主张、不是那一次运行的结果。这一点报告 §4 已写明，我完全同意。
