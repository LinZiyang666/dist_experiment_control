FAIL — *主进程已整改 + 自审,见文末「## 主进程回复（round-5 整改 + 多专家自审）」。F1–F3 全部采纳并修复,你的 3 个 round-5 reviewer 测试通过(`-count=10`/`-race`);随后跑了一轮 6 维度多专家对抗自审(确认 7/14 finding),又修了 OFF-repair 未升代、升代失败仍硬推、killGenSession 无界增长三处,驳回了 1 处会引入回归的建议。F4 真硬件需 owner 裁决。*

# P13 External Review Round 5

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 is not approved.

The round-4 remediation fixes several real paths:

- a healthy `proxy_meta` counter advances across ordinary restart and wall-clock
  rollback;
- heartbeat now compares the complete `(generation, epoch)` pair;
- installed listeners can be closed by session without querying the allocation
  store;
- allocation-list failures are reported, false `freed` events are suppressed,
  and a healthy stale allocation is rotated on the next ON.

Three production paths remain open. Session-level shutdown does not fence an
in-flight REGISTER, generation persistence failure silently falls back to the
unsafe wall clock, and a broker restored behind a connected agent cannot raise
its generation and therefore cannot converge. The locked real-deployment exit
criteria also remain incomplete.

## Findings

### F1 - High: CloseSession does not invalidate an in-flight REGISTER

`handleAgent` snapshots only `killGen[port]` before token authorization
(`internal/tunnel/tunnel.go:197-206`) and checks that value before installing
the listener (`internal/tunnel/tunnel.go:250-268`).

`CloseSession` only iterates already-installed entries in `s.sessions`
(`internal/tunnel/tunnel.go:352-376`). If a REGISTER has passed authorization
but has not yet inserted its `serverSession`, the session is invisible to
`CloseSession`, so no kill generation advances.

The resulting OFF race is:

1. REGISTER snapshots the port generation and passes token authorization while
   the switch is ON;
2. OFF commits the switch and calls `CloseSession(sid)`;
3. the REGISTER is not yet in `s.sessions`, so `CloseSession` does nothing;
4. the REGISTER resumes, sees the unchanged port generation, installs the
   public listener, and reopens the exit after OFF returned.

The independent regression fails 10/10:

```text
go test ./internal/tunnel \
  -run '^TestExternalReviewCloseSessionInvalidatesInFlightRegister$' \
  -count=10
# FAIL: public proxy listener appeared after CloseSession returned
```

Test: `internal/tunnel/p13_external_review_round5_test.go:11-77`.

Recommendation: add a session-level kill generation/tombstone. `handleAgent`
must snapshot both the port and session generations after parsing `sid`, then
verify both before installation. `CloseSession(sid)` must increment the session
generation even when no listener is currently installed, then close installed
victims. Tracking in-flight registrations explicitly is another valid design.

Keep the existing port generation for `CloseProxy(port)` and add a test where
the session shutdown lands before, during, and after token lookup.

### F2 - High: generation persistence failure fails open to an unsafe wall clock

`Broker.New` logs an `advanceProxyGeneration` error and continues with
`cfg.Now().UnixNano()` (`internal/broker/broker.go:345-358`).

This directly restores the round-4 fencing failure. A missing/corrupt
`proxy_meta`, a migration mismatch, or a persistence error can start a later
broker with a lower generation, allowing older ON directives to outrank the new
broker's OFF.

The independent regression fails 10/10:

```text
go test ./internal/broker \
  -run '^TestExternalReviewBrokerRefusesUnpersistedGeneration$' \
  -count=10
# FAIL:
# broker started without durable fencing:
# prior generation=200000000000 fallback generation=100000000000
```

Test: `internal/broker/p13_external_review_round5_test.go:14-40`.

The migration change makes this more likely. `proxy_meta` was appended to
`0006_proxy.sql`, but the migration engine skips files already recorded by
filename (`internal/storage/storage.go:116-123`). Any DB that applied the prior
round's 0006 will not receive the new table, and the unsafe fallback hides the
schema mismatch.

Recommendation:

1. make inability to persist a fencing generation a fatal `Broker.New` error;
2. add `proxy_meta` in a new migration such as `0007_proxy_generation.sql`
   instead of amending an already-applied migration;
3. add an upgrade test starting from a DB whose `schema_migrations` already
   contains `0006_proxy.sql`;
4. check generation overflow rather than wrapping `stored+1`.

There is no safe pre-0006 fallback: the rest of P13 also requires the P13
schema.

### F3 - High: restored broker generation cannot self-bootstrap past agents

The new documentation explicitly acknowledges that restoring an older DB also
rewinds `proxy_meta`, then delegates correctness to a future runbook
(`docs/architecture.md:1894`, `docs/reviews/p13-plan.md:309`).

No executable runbook or command exists, and the suggested
`now_unixnano` value is not guaranteed to exceed a generation previously
issued by a host with a forward-skewed clock.

More importantly, the broker now receives exactly the information needed to
detect this condition, but does not act on it. When `agentGen > b.proxyGen`,
`repairProxy` merely publishes another directive stamped with the lower broker
generation (`internal/broker/proxy.go:497-543`). The agent correctly drops that
directive forever.

The independent regression fails 10/10:

```text
go test ./internal/broker \
  -run '^TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration$' \
  -count=10
# FAIL:
# restored broker pushed generation=100
# behind applied agent generation=200
```

Test: `internal/broker/p13_external_review_round5_test.go:42-106`.

This can retain an old subscriber keyset after restore, including keys revoked
in the restored database.

Recommendation: when an authenticated capable agent reports
`agentGen >= brokerGen`, atomically raise the durable generation above
`agentGen`, update the in-memory generation with synchronization, and only then
push the authoritative directive. Multiple agents must converge through an
atomic max operation. Reporting the last applied generation in registration as
well as heartbeat would reduce convergence latency.

An external generation store is also valid. If automatic DB-restore convergence
is intentionally removed, that is an owner-level requirements change and needs
a concrete, tested restore command that proves the chosen value exceeds all
agents; comments in a migration are not an operational control.

### F4 - Exit blocker: real Caddy/WSS and Clash validation remains pending

The in-process CLI and combined data-plane coverage passes, including P13 in the
e2e matrix. The locked exit criteria still require:

- real Caddy + ACME with `/sub/*` and NATS WSS coexisting;
- a real Clash client importing the subscription and exiting through an agent.

Both remain unchecked in `log.md:381-386`. Complete them in the lab or have the
project owner explicitly revise the phase exit criteria.

## Accepted Round-4 Fixes

- `TestExternalReviewBrokerGenerationSurvivesClockRollback`: PASS, 20 runs.
- `TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch`: PASS,
  20 runs.
- `TestExternalReviewCloseSessionKillsListenersWithoutDB`: PASS, 20 runs for
  already-installed listeners.
- `TestProxyOffReportsErrorWhenAllocStoreFails`: PASS, 20 runs.
- `TestProxyOnRotatesStaleNotReadyAllocation`: PASS, 20 runs.
- Build, lint, CGO-free dependency check, P13 e2e, and unaffected P13 race
  packages pass.

## Questions

1. Does P13 still promise automatic convergence after an older DB restore? The
   round-3 locked contract says yes; the round-4 appendix silently changes this
   to a manual prerequisite.
2. Is the owner willing to make generation persistence failure fatal? Continuing
   without fencing is incompatible with the stale-directive security model.
3. Will the real Caddy/Clash exit checks be scheduled or explicitly removed?

## Verification

Passed:

```text
CGO_ENABLED=0 go build ./...
go list -deps -f '{{if .CgoFiles}}{{.ImportPath}} {{.CgoFiles}}{{end}}' ./...
# no output

golangci-lint run
# 0 issues

git diff --check
git diff --cached --check

go test -count=1 -tags e2e_matrix -v ./test/e2e/...
# P1-P8 and P13 pass; P9/P10 hit the recorded macOS baselines
```

`go test ./... -count=1` fails on the three new round-5 reviewer tests plus the
already recorded macOS `/private/var`, Unix-socket path, CLI `exit 64`, and
agent-install baselines. `test/p13` passes.

The focused race run fails on the same three new reviewer tests plus the
recorded agent `/private/var` tests. `internal/subhttp`, `internal/proto`,
`internal/proxysub`, `internal/port`, and `test/p13` pass under `-race`.

## Re-review Gate

Re-review after:

1. session-level OFF fences both installed and in-flight REGISTER operations;
2. broker startup fails closed when a durable generation cannot be obtained;
3. generation schema is delivered through a forward migration;
4. a restored broker can obtain a generation greater than connected agents, or
   the owner explicitly revises the restore contract and supplies a tested
   operational mechanism;
5. all three round-5 reviewer tests pass repeatedly and under `-race`;
6. the real deployment exit criteria are completed or explicitly revised.

---

## 主进程回复（round-5 整改 + 多专家自审）

日期: 2026-06-10。采纳 F1–F3 全部 finding;**3 个 round-5 reviewer 测试通过**(`-count=10`/`-race`)。随后按用户要求跑了一轮**多专家对抗自审**(6 维度 fan-out → 对抗复核 → 综合,确认 7/14),据此又改了 3 处、驳回 1 处会引入回归的建议。`golangci-lint v2.5.0` 0 issues、`CGO_ENABLED=0` deps 无 cgo。`go test ./...` 仅余既有 macOS 基线。

### F1（High,CloseSession 不 fence 在飞 REGISTER)— 已修(session 级 kill 代)
tunnel 加 `killGenSession[sid]`;`handleAgent` 授权前同时快照 `killGen[port]`+`killGenSession[sid]`,装入前两者都校验;`CloseSession(sid)` **无条件** bump `killGenSession[sid]`(即便此刻无已装入监听)。故 OFF 按 sid 杀也能让已授权未装入的 REGISTER 在装入处放弃。`TestExternalReviewCloseSessionInvalidatesInFlightRegister` 10/10。

### F2（High,generation 持久失败 fail-open + 迁移 append bug)— 已修
- **fail-closed**:`advanceProxyGeneration` 取不到持久 generation ⇒ `broker.New` **返回 error 拒启**(不退化到 wall clock)。`TestExternalReviewBrokerRefusesUnpersistedGeneration` 10/10。
- **forward migration**:`proxy_meta` 从 0006 移到**新 `0007_proxy_generation.sql`**(改已应用的 0006 会被按文件名跳过);`CREATE TABLE IF NOT EXISTS`+`INSERT OR IGNORE` 幂等。`TestProxyGenerationForwardMigrationAppliesOnExisting0006DB`(模拟仅有 0006 的 DB)通过。
- **溢出守卫**:`advanceProxyGeneration` 加 `stored+1` 上限 `maxProxyGeneration=1<<62`。

### F3（High,restored broker 无法越过 agent generation 自举)— 已修(自动升代,作废手动 runbook)
broker 在心跳见 `agentGen >= brokerGen` 时 `escalateProxyGen` 原子地把持久 generation 抬到 `agentGen+1` 之上(`advanceProxyGeneration` 的 floor + 事务内 max),随后推送压过 agent。**round-4 的「手动 runbook」punt 作废**,改为自动收敛。多 agent 经事务 max 收敛。`TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration` 10/10。回答 Q1:**仍承诺还原后自动收敛**(round-4 附录改成手动是退步,已撤回);Q2:**接受持久失败即拒启**(已实现)。

### 多专家自审(6 维度,确认 7/14)— 据此追加修复
1. **[High] OFF-repair 路径未升代**:`repairProxy` 的 `!on` 分支推 disable 前没升代,restored broker 推的 disable 被 agent 丢弃 ⇒ 杀不掉。**已修**:把升代移到 on/off 分叉**之上**,两路都覆盖。新增 `TestProxyOffRepairEscalatesPastRestoredAgentGeneration`。
2. **[High] 升代失败仍硬推**:`escalateProxyGen` DB 失败时返回早退但仍推一条必被丢弃的 directive(livelock)。**已修**:`escalateProxyGen` 返回 bool,失败则 `repairProxy` 跳过推送、靠下次心跳重试。
3. **[Medium/High] killGenSession 无界增长**:每个 session 生命周期留一条 entry 永不回收。**已修**:加 `tunnel.ForgetSession(sid)` 在 `finalizeSessionRm`(`dropSessionRows` 后,此时 token 查询必拒该 sid)安全剪除;`killGen` 本就被端口段(≤1000)有界。新增 `TestForgetSessionPrunesKillGen`。
4. **[High] 「失败后 p.gen/p.epoch 未重置」— 驳回**:自审建议「重置为 (0,0)」会**重新打开陈旧 directive 复活洞**(applied=(0,0) 则任何旧 enable 都 >,会复活已撤代理)。当前「保留上次成功对」才正确(失败后保留较低对,重试携更新对仍 > 故仍能重试,`TestExternalReviewTransientProxyStartFailureCanRecover` 已证)。加注释固化决策,并新增 `TestStaleEnableDroppedAfterDisableTeardown` 防回归。
5. 测试补强:`TestProxyNewerLexicographic`(定序契约)、`TestProxyGenerationEscalationConvergesUnderConcurrency`(并发升代经事务 max 收敛 + 持久一致,`-race`)。
- 其余 refuted 项:`killGen` 无界(实为端口段有界)、proto omitempty 歧义(generation≥1,0 仅表 legacy,无歧义)等。

### F4（Exit blocker,真 Caddy/WSS + 真 Clash)— 仍需 owner 裁决(Q3)
in-process 覆盖已齐。真硬件本机 macOS 无法复现。回答 Q3:请你裁决排期 lab 或显式修订出口标准记入 plan;我不自行 waive。

### 复核请关注
- 3 个 round-5 reviewer 测试 + 自审新增 5 个测试是否如期绿(本机 `-count=10`/`-race` 已过)。
- 自动升代(F3)是否认可为 DB-还原收敛机制(取代手动 runbook)。
- F4 出口标准处置请你定。
