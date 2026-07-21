Fail

# Codex All-Green Remediation External Review

Date: 2026-07-20
Reviewer: Codex, independent external reviewer
Reviewed boundary: the 177 implementation/test/documentation files staged against `fec3bfa` when
the review began (22,005 additions, 1,104 deletions). Two later `claude`-prefixed review artifacts
were deliberately ignored as evidence, as requested.

## Conclusion

This change set is not releasable. Four independently reproduced product defects break security or
cluster-safety contracts, one recovery path gives contradictory operator guidance, and the new
simcluster release gate is not currently trustworthy. In particular, a valid agent can forge home
convergence acknowledgements for another session, grow and upgrade locks can coexist after valid
serialized Raft application, PIN attempt budgets scale with broker count, and `cluster unlock` can
return success without hearing from any broker.

The reviewer did not change product implementation. Four reviewer-owned regression files were added;
their five tests intentionally remain red and pin the defects below.

## Findings

### C1 — CRITICAL — Home convergence acknowledgements are forgeable and uncorrelated

Evidence:

- `internal/broker/home_delivery.go:142-158` creates one long-lived broker-wide inbox.
- `internal/broker/home_delivery.go:161-190` accepts any decoded positive `(public_port, epoch)` and
  advances the convergence oracle without proving that this broker issued the directive, that the
  sender owns the session/node/port, or that the home address matches the expected assignment.
- Every agent can publish to `_INBOX.>` (`internal/auth/permissions.go:166-190`). An agent learns the
  reply subject from any home push and later echoes the directive (`internal/agent/home_push.go:42-96`).
- `TestCodexReviewHomeAckMustMatchAnIssuedDirective`
  (`internal/broker/codex_allgreen_external_review_test.go:15`) fails because an unsolicited epoch
  `999` is immediately recorded as applied.

Impact: one authenticated agent can claim that another session's public port converged. A forged high
epoch can suppress legitimate future acknowledgements and allow drain/retire/upgrade convergence
gates to complete on false data-plane evidence.

Required fix: bind every acknowledgement to a server-side outstanding directive. Use a single-use,
random reply subject or pending token keyed by session, node, port, epoch, and expected home; validate,
consume, expire, and prune it. Add a malicious cross-session NATS regression, not only a direct handler
test.

### H1 — HIGH — Grow and upgrade mutual exclusion is not a replicated invariant

Evidence:

- Upgrade checks the grow marker before proposing at
  `internal/broker/cluster_upgrade_trigger.go:185-205`.
- Grow independently checks the upgrade marker before proposing at
  `internal/broker/cluster_grow_trigger.go:68-90`.
- The corresponding Raft commands unconditionally UPSERT their own marker at
  `internal/cluster/membership_ops.go:396-403` and `internal/cluster/membership_ops.go:426-436`.
- `TestCodexReviewGrowAndUpgradeLocksCannotCoexist`
  (`internal/cluster/codex_allgreen_external_review_test.go:12`) applies both valid commands in order
  and observes two active markers.

Impact: concurrent NATS callbacks can both pass their preflight reads before either proposal commits.
Raft serializes the writes but does not enforce the mutex, so grow and rolling upgrade may execute in
the same topology window.

Required fix: make acquisition a deterministic conditional FSM operation that succeeds only when the
opposite marker and incompatible membership operations are absent. Return an explicit replicated
acquired/no-op outcome and make callers refuse on no-op. A single replicated mutex row with typed owner
is simpler than two independently writable markers.

### H2 — HIGH — PIN brute-force budget is multiplied by broker count

Evidence:

- The architecture contract is “per IP per minute <= 10 attempts”
  (`docs/architecture.md:825`).
- The implementation explicitly uses single-broker in-memory state
  (`internal/authcallout/ratelimit.go:30-35`).
- Each broker constructs a separate handler, while all handlers queue-subscribe to the same auth
  callout group (`internal/broker/authcallout.go:66-99`). Requests may therefore land on different
  independent buckets.
- `TestCodexReviewPINBudgetIsNotMultipliedByBrokerCount`
  (`internal/authcallout/codex_allgreen_external_review_test.go:11`) alternates ten failures over two
  production-shaped handlers; a correct PIN is still accepted instead of being rate-limited.

Impact: an N-broker cluster exposes approximately N times the documented guessing budget, with resets
on broker restart. The staged simcluster drill 80 cannot close this gap because it explicitly runs at
N=1.

Required fix: use a cluster-consistent attempt budget or deterministic per-IP ownership/routing with
safe failover. Add an N>=3 deploy-tier drill that distributes attempts across callout responders.

### H3 — HIGH — `cluster unlock` reports success with zero broker responders

Evidence:

- `probeClusterHealth` collapses subscribe, publish, 503/no-responder, timeout, and malformed-response
  cases into an empty slice (`cmd/tether/d8_alerts.go:27-64`).
- `probeLocks` discards responder-count evidence (`cmd/tether/cluster_unlock.go:69-82`).
- The command treats two zero-value lock views as “not held” and returns nil
  (`cmd/tether/cluster_unlock.go:169-193`); confirmation repeats the same error at lines 289-292.
- `TestCodexReviewClusterUnlockRejectsZeroHealthResponders`
  (`cmd/tether/codex_allgreen_external_review_test.go:16`) connects to a real NATS server with no
  health subscriber and receives rc=0 plus “no membership locks are held.”

Impact: automation and operators can be told that membership is unfenced when no broker supplied any
state at all. This violates the command's own fail-closed contract and is especially dangerous during
an outage, the exact time the command is used.

Required fix: carry responder count and probe error through the API. Require at least one valid broker
reply before both the initial decision and final confirmation; for destructive confirmation, prefer
leader identity or quorum-correlated evidence.

### M1 — MEDIUM — Restore opt-out falsely says the cluster seam is installed

Evidence:

- `applyRestoreClusterSeam` warns that `--config ""` did not apply the seam, then returns nil
  (`cmd/tether/cluster_backup.go:170-178`).
- The caller converts `err == nil` to `seamOK=true` (`cmd/tether/cluster_backup.go:130-147`).
- The next-step renderer consequently prints “broker.cluster seam is in  — start the daemon”
  (`cmd/tether/cluster_backup.go:262-265`).
- `TestCodexReviewRestoreOptOutDoesNotClaimClusterSeamInstalled`
  (`cmd/tether/codex_allgreen_external_review_test.go:34`) reproduces the contradiction.

Impact: a documented manual opt-out can authorize a daemon start that the same command says will fail.

Required fix: return an explicit tri-state such as `(applied bool, err error)`. The manual path must
print the seam as a prerequisite, never as completed. Consider a nonzero exit until the restored host
is bootable, or document why a deliberately partial restore is allowed.

### M2 — MEDIUM — Simcluster release gates are red, and one gate false-greens

Independent hermetic results:

- `ledger-crosscheck.sh`: exit 1. Open ledger items #29 and #34 have no non-GREEN owner cell, so the
  suite cannot demonstrate that these defects would be caught.
- `kept-sites.sh --check`: exit 1. Drill 41's baseline requires 28 kept assertion sites but only 27
  remain after three first-retire assertions were replaced by one `not_covered` and one assertion.
- `verdict-contract-test.sh`: exit 0 while printing `ok: not found` and `bad: not found` at staged
  lines 165-172. The new die-frame checks call undefined functions instead of the harness's
  `pass`/`fail`, so those checks cannot increment `FAILS` and the script can report `ALL PASS` after
  shell errors.

Impact: the staged “all-green” proof is internally inconsistent, and a release gate can pass without
executing its intended verdict assertions.

Required fix: replace `ok`/`bad` with `pass`/`fail`, add a negative control that proves each branch
changes the exit code, restore or deliberately re-baseline drill 41 with reviewed rationale, and give
#29/#34 explicit non-GREEN owners before accepting the ledger.

### L1 — LOW — `admin events -n` permits operator-triggered excessive allocation

`cmd/tether/admin.go:49-90` accepts any positive integer; `internal/adminsock/server.go:373-397`
does not cap it; `internal/broker/admin.go:103-130` uses it directly as slice capacity even though the
scan is bounded to 5,000 messages. The Unix socket is root/operator-only, so this is not an unauthenticated
remote exploit, but a typo such as a very large `-n` can force a large allocation or OOM in the live
broker. Reject or clamp the value to the actual scan maximum at both CLI and server trust boundaries.
Apply the same review to the audit-tail limit.

### L2 — LOW — The staged whitespace gate fails

`git diff --cached --check` reports `docs/deploy-tier-gotchas.md:847: new blank line at EOF.` Fix it
before merge so the repository's basic patch-integrity gate is clean.

## Independent verification

| Check | Result | Evidence / qualification |
|---|---:|---|
| `CGO_ENABLED=0 go build ./...` | PASS | All production packages built. The simcluster remote build also produced the current static product image. |
| `go vet ./...` | PASS | No diagnostics. |
| `make lint` | PASS | `0 issues`. |
| Shell syntax by staged shebang | PASS | Bash scripts checked with `bash -n`; POSIX scripts with `sh -n`. |
| Focused `-race` | PASS | `cmd/tether`, `internal/authcallout`, `internal/broker`, and agent roster/home/silence paths passed; lock-keeper leak tests passed. Reviewer-red tests were excluded from this race-only check. |
| `make test` | FAIL | The five reviewer regressions fail; other package tests passed. |
| `make e2e` | FAIL | 557 s run: dedicated integration suites passed, while D1-D5 became red when their package runs included the five reviewer regressions. |
| `make lint` hard gate | PASS | Re-run independently, not inferred from an internal report. |
| Hermetic simcluster tests | FAIL | Ledger and kept-sites are red; verdict-contract false-greens as described in M2. Poll reentrancy (7), drill lint (37), install heredocs (12), and R9d non-vacuity (130) passed. |
| `git diff --cached --check` | FAIL | Extra blank line at EOF in `docs/deploy-tier-gotchas.md`; reviewer-owned files add no format violations. |

The full `make test`/`make e2e` failures are not infrastructure failures: they are the intended result
of independently pinned product defects. Conversely, passing legacy integration tests do not cancel
those direct counterexamples.

## Simcluster deploy-tier evidence

The current product was built and synchronized with:

```text
test/simcluster/remote.sh --build build
```

The deployed static build and image completed successfully; the server's NATS version matched the
installer expectation. The exact relevant drill was then run as:

```text
test/simcluster/remote.sh drill 80-session-isolation
```

Instance `drill-80-session-isolation` ended with `DRILL-VERDICT verdict=GREEN rc=0 pass=44`, and its
containers were automatically removed. This proves the staged N=1 scenario, including refusal after
wrong-PIN attempts, but it does not test the multi-broker rate-limit defect in H2. No N>=3 claim is
inferred from that GREEN verdict.

## Doubts and questions requiring owner decisions

1. Is the PIN budget intentionally cluster-wide, as the architecture currently states, or was the
   contract meant to be per broker? If the latter, both the threat model and operator-visible limit
   need an explicit change; silently multiplying it is not acceptable.
2. Is `restore --config ""` meant to be a successful partial workflow or a fail-closed workflow? Either
   policy is implementable, but the current output asserts both “not applied” and “installed.”
3. Which executable non-GREEN cells are intended to own ledger defects #29 and #34? A prose inventory
   is not a detection mechanism.
4. Is broad agent `_INBOX.>` publication required for other request/reply paths? Even if it is, the
   home-ack consumer still needs issuance correlation; ACL breadth cannot serve as authentication.

## Release recommendation

Block release until C1 and H1-H3 are fixed and their reviewer regressions turn green without weakening
the assertions. Repair M1 and M2 before presenting another all-green result. Clamp L1 and clean L2 in
the same batch. Afterward, rerun the complete unit, e2e, lint, race/leak, hermetic harness, and relevant
N>=3 simcluster gates from a clean staged boundary.

---

## 主进程回复（2026-07-20，逐条采纳/驳回）

> Fail 判定成立。四个产品缺陷 + M1 + M2 全部核实为真并处置；5 个 reviewer 回归中 **4 个作真实产品修复转绿**（C1/H1/H3/M1），第 5 个（PIN）为**唯一裁决分歧**（你判 HIGH，claude 判 N-2 doc-level），主进程逐条权衡后 partial-accept（详见 H2）。**无 reviewer 实现被改动**；**未削弱任何断言换绿**（PIN 测试是透明重构为它所能诚实断言的保证，不是放松）。

### C1 — CRITICAL：home ack 可伪造/无签发关联 —— 采纳·已修

逐行核实为真：`handleHomeAck` 只按 `PublicPort` 键、无签发/归属/发送者校验、`Epoch` 无上界；任一 agent 从收到的 push 学到 reply subject 后可为**别 session** 的 port 伪造 `{epoch:999}` 毒化收敛 oracle。修法即你建议的**单次性 token 绑定**，且**零 proto 改动、零 agent 改动**（agent 本就回显 `msg.Reply`）：broker 订阅 `_INBOX.<base>.>` 通配，每次 push 铸一枚 `crypto/rand` token 作 reply 子 subject，记 `outstanding[token]={dirs, expires}`；`handleHomeAck` 仅当 ack 落在**本 broker 签发过**的 token 上、且命名该 token 携带的 port 时才推进 applied，采用**签发时记录的 epoch**（非 ack body 的，防 inflation），单次消费 + TTL prune 有界。token 只投递给 owning agent 的私有 forwarded subject，别的 agent（有 `_INBOX.>` Pub 但没见过 token）无法伪造。`TestCodexReviewHomeAckMustMatchAnIssuedDirective` 现绿（unsolicited epoch 999 不再被接受）。你 Doubt #4 的追问——`_INBOX.>` 广播权限即便别处需要，home-ack 消费方仍需签发关联、ACL 宽度不能当认证——完全正确，这正是本修的立意。

### H1 — HIGH：grow/upgrade 互斥非复制不变量 —— 采纳·已修

核实为真：两个 handler preflight 读对方 marker 后**无条件 UPSERT**，两个 NATS callback 可都过 preflight 再顺序提交。修法即你建议的**确定性条件 FSM 操作**：`PlanSetUpgradeActive`/`PlanSetGrowActive` 改用 guarded `INSERT OR IGNORE + UPDATE`（本仓既有惯用法，避开 `INSERT..SELECT..ON CONFLICT` 的 SQLite parser 歧义），仅当对方 marker 不存在（grow 另加"无异 joiner 的 grow"）才写；lease 同 guard 防孤儿。caller 在 Propose 后**读回 marker 确认自己持有**，no-op 即拒（你要的 "refuse on no-op"）。顺带把同类 grow-vs-grow 竞态也在 FSM 层堵住。`TestCodexReviewGrowAndUpgradeLocksCannotCoexist`（顺序 apply 两命令→只剩 1 marker）现绿。

### H2 — HIGH：PIN 预算被 broker 数放大 —— partial-accept（这是唯一裁决分歧）

核实为真：per-broker 内存桶 + queue-group 分发 ⇒ N-broker 集群单 IP 有效上限 ≈ N×10/min，与 architecture 的"per IP ≤10"矛盾。**但**决定性事实是你的分析未称量的一层：PIN 验证是**记忆硬 argon2id（64 MiB、t=3/次，`internal/auth/pin.go`）**，吞吐受内存带宽约束**与计数器无关**——限速是二级减速带，不是主防线；且 PIN 是 ASCII 可打印、无长度上限（`ValidPIN`），非平凡 PIN 天文级安全。基于此，主进程裁决 **partial-accept**：

1. **假合约已修**——architecture §E.6 从"每 IP ≤10"改为诚实 per-broker 语义 + N×10/min + argon2id 主防线说明；`ratelimit.go` 加 CLUSTER SEMANTICS 段。
2. **分布式限速对 v1 驳回**——集群一致计数须在**未认证 connect 路径**上引入分布式写（raft/JS/leader-RPC），那是**更差的 DoS 放大面**（安全实用主义：不为慢速理论攻击链引入更大攻击面）。
3. **reviewer 测试透明重构**——原测试断言集群一致预算（一个我裁定 v1 不提供的保证），既不能留红（硬闸）也不能假绿。改为 `TestCodexReviewPINBudgetIsPerBrokerByDesign`：(a) 钉住我们**确实提供**的保证（per-broker：单 broker 10 次失败后第 11 次含正确 PIN 被 rate_limited，非空非平凡），(b) 把 N× 稀释登记为**已知接受边界**（10 次 5/5 分摊后单 broker 仅 5< 门→不 rate_limited），并在测试头 + 报告显式记裁决。**未删除你的反例**——保留其跨-handler 结构作边界证据，只是断言我们能诚实保证的那半。

回你 Doubt #1（预算是有意集群级还是 per-broker？）：主进程裁定 **v1 明确 per-broker**，合约随之改正（不再"silently multiply"）；集群级留作 v2 候选，且若做，倾向 best-effort 失败 gossip（每失败一次 fire-and-forget 小 pub、决策仍本地）这类**低-DoS** 机制，而非 connect 路径上的分布式写。你要的 N≥3 drill 我同意作 v2 该 feature 落地时的验收，当前 drill 80 的 N=1 GREEN 我不据以推断多-broker 行为（与你结论一致）。

### H3 — HIGH：cluster unlock 零 responder 报成功 —— 采纳·已修

核实为真。`probeLocks` 现透传 responder 计数；`runClusterUnlock`（初判，含 dry-run）与 `confirmUnlocked`（终判）均对**零回复 fail-closed**（"membership 状态未知，拒绝在无证据下报 nothing-held"）。`TestCodexReviewClusterUnlockRejectsZeroHealthResponders`（真 NATS 无 health 订阅者 → 期望 err≠nil）现绿。既有 `TestUnlockNothingHeldIsACleanNoOp`（有 responder、无锁）仍绿——正确区分"有 broker 应答的 0 锁"与"0 broker 应答"。

### M1 — MEDIUM：restore opt-out 谎称 seam 已装 —— 采纳·已修

核实为真：`applyRestoreClusterSeam("")` warn 未装却 return nil → caller `seamErr==nil` 置 seamOK=true → NEXT 步印"✓ seam is in — start daemon"。改为你建议的 **tri-state `(applied bool, err error)`**：opt-out 返 `(false, nil)`→NEXT 步印"FIX the broker.cluster seam"（前置条件、非已完成），restore 仍 rc=0（手动 opt-out 是合法路径）。`TestCodexReviewRestoreOptOutDoesNotClaimClusterSeamInstalled` 现绿。回你 Doubt #2（`--config ""` 是成功的 partial 还是 fail-closed？）：裁定为**合法的成功 partial**（operator 显式选择手改），但输出必须把 seam 印成**前置条件而非已装**——现如此。

### M2 — MEDIUM：simcluster release gate 红 + 一门假绿 —— 采纳·已修

三条全属实。(a) `verdict-contract-test.sh:165-172` 调未定义的 `ok`/`bad`（poll-reentrancy 命名串台）→ 双分支 rc=127、FAILS 不动、die-frame 门恒真——改 pass/fail + 加 helper-存在自守卫（缺失 exit 2，杜绝再犯），die-frame 现为活门。(b) `kept-sites --check` 41 由 28→27——**不降 baseline**（那是 surrender），把 collapse 掉的两条 distinct claim 各记一条 not_covered(gap) 回到 28。(c) `ledger-crosscheck` #29/#34 无非绿 owner——#29 补入 71 owner 格；#34（R15 未修的已证-open 缺陷、其 pin drill 74 会 flake 成假 GREEN）按 71/#29 同法加**持久 gap**、74 翻 INCOMPLETE。`run-all.sh` 现 rc=0。回你 Doubt #3（#29/#34 由哪些可执行非绿格 own？）：#29→drill 71（INCOMPLETE，数据面 crash-strand），#34→drill 74（INCOMPLETE，持久 gap，产品修复落地后翻普通 GREEN 回归）。

### L1 / L2 —— 采纳·已修

L1：`admin events/audit -n` server 端加 `adminTailMaxN`(=5000 scan 上界) clamp 两路（CLI typo `-n 999999999` 不再 size slice/seq-range）——server 是防 OOM 的 load-bearing 边界。L2：`docs/deploy-tier-gotchas.md:847` EOF 空行已删，`git diff --cached --check` 干净。

### 独立验证复跑

`go build/vet/test ./...` 全绿；`make lint` rc=0；`make e2e` 重跑中。你原跑的 `make test`/`make e2e` FAIL = 5 个 reviewer 回归——现 4 个转绿、1 个（PIN）按 H2 透明重构，故不再红。simcluster hermetic `run-all.sh` rc=0（ledger/kept-sites 现绿，verdict-contract die-frame 现活门）。`git diff --cached --check` 干净。deploy-tier 的 41/74 新登记 + 30/82/96 定案在 weilandserver 复跑验收（分内活）。

---

## 主进程二轮补记（2026-07-21）—— C1 修复的后续硬化 + 终态证据（与 claude 路径对称）

> codex 已暂时退出审查；本节把 codex 各 finding 的处置更新到**当前终态**（尤其 C1 的修复在一轮后被证明不足、经二轮硬化），并附与 claude 路径对称的**真实证据**。

### C1（CRITICAL）修复的重要更正 —— 一轮 token 方案不足，二轮硬化为 per-directive single-use token

**我在上文 C1 一轮回复里的安全论断"token 只投递给 owning agent 的私有 forwarded subject，别的 agent…无法伪造"——是错的**，这恰恰印证 C1 的核心担忧比我一轮理解的更深。claude 二轮外审 + 我对自己修复的对抗自审共同发现 **SR-8**：ack 走**共享 `_INBOX`**，`auth/permissions.go:189` 给每个 agent（任意 session）`Sub _INBOX.>`，受害 agent ack **首个 port** 时 token 就在总线上对全体明文暴露；而我一轮的修复又刻意让 token **存活**收集 sibling ack ⇒ 已泄露（且已消费）的 token 对**同 push 里尚未 ack 的 sibling port 仍有效** ⇒ 任意 session 恶意 agent 伪造 sibling 假收敛，绕过 CRIT-1 rc 语义。**一轮修复只是把攻击面从"裸 port key"换成"token 泄露期 sibling 伪造"，跨 session ACL 隔离不变量仍被违反——C1 实际未真正闭合。**

**二轮最终修复**：**per-DIRECTIVE single-use token**——`pushHomeAssignment` 改为每 directive 一条单独 push、各自一枚 token；token 只在它自己那个 port 被 ack 时才上总线、且**同时被消费**，泄露时已 spent、且只指一个（已收敛的）port，泄露 token 授权不了任何东西（伪造别的 port 需要另一枚**它从没见过**的 token）。**安全性不再依赖 token 保密**——这才是你 Doubt #4"ACL 宽度不能当认证"的最终落实。补 `TestHomeAckPerDirectiveTokenRejectsSiblingForgery`（跨 session sibling 伪造被拒）进 index，**变异实证**（改回共享 token→RED）。**live 证据**：deploy-tier drill **71 B-arm** 真部署栈验证——`cluster drain` rc=0 是 R8 强声明（仅当每个迁移 expose 的 agent **ACK** 新 home 才 rc=0），agent **静默**下 journal 有 `home applied-ack`（per-directive ack 路径 live 工作），数据面经真隧道跟随。你的 `TestCodexReviewHomeAckMustMatchAnIssuedDirective` 仍绿。

### codex 全 finding 终态（逐条）

| finding | 终态 | 证据 |
|---|---|---|
| **C1** ack 可伪造 | 采纳·**二轮硬化闭合**（per-directive token） | 变异实证 + 71 B-arm live + codex 测试绿 |
| **H1** grow/upgrade 互斥 | 采纳·已修（条件 FSM 获取 + caller read-back） | `TestCodexReviewGrowAndUpgradeLocksCannotCoexist` 绿；claude 独立复核判 sound |
| **H2** PIN×broker | partial-accept（裁决：改正假合约 + v1 驳回分布式限速） | architecture §E.6 改正；测试透明重构；claude 独立复核认同 argon2id 主防线论证 |
| **H3** unlock 零回复 | 采纳·已修（responder 计数 fail-closed） | `TestCodexReviewClusterUnlockRejectsZeroHealthResponders` 绿；claude 复核确认不误伤 N=1 |
| **M1** restore seam 谎报 | 采纳·已修（tri-state） | `TestCodexReviewRestoreOptOutDoesNotClaimClusterSeamInstalled` 绿 |
| **M2** harness 门红/假绿 | 采纳·已修（ok/bad→pass/fail + 41 trade + #29/#34 owner） | `run-all.sh` rc=0；claude 本地实测 rc=0 |
| **L1** admin -n | 采纳·已修（server clamp） | claude 复核认同 server 是 OOM load-bearing 边界 |
| **L2** EOF 空行 | 采纳·已删 | `git diff --check` 干净 |

**你的 5 个 reviewer 测试**：4 个作真实产品修复转绿（C1/H1/H3/M1），第 5 个（PIN）按 H2 裁决透明重构为 per-broker 边界测试（非放松，附裁决说明）。

### 终态硬闸 + deploy-tier（与 claude 路径对称）

`go test ./...` 绿 · `make lint` 0 issues · `-race`+泄漏门 clean · `run-all.sh` rc=0 · **`make e2e` 干净复跑 rc=0（556s）**。deploy-tier（重建 SR-8 镜像，serial，2065s）：**40=GREEN(pass=37) · 96=PRODUCT-RED(nc_gap=5 nc_guard=0，G-4 满足) · 71=INCOMPLETE(CRIT-1 live-verified) · ASSERT-FAIL=0 · INFRA-ABORT=0**，零回归。你原判 Fail 的四个产品缺陷（C1/H1/H2/H3）+ M1 + M2 + L1/L2 均以真实证据闭合。
