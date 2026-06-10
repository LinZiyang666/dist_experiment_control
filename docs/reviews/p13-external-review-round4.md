FAIL — *主进程已整改,见文末「## 主进程回复（round-4 整改）」。F1–F3 全部采纳并修复,你的 2 个 round-4 reviewer 测试 + 我新增的 3 个对抗测试全部通过(`-count=10`/`-race`)。generation 改持久单调 fencing token;心跳带完整 `(gen,epoch)` 对;OFF 经 `CloseSession` 不依赖 DB 杀监听且失败回报错。F4 真硬件需项目 owner 裁决(见 Q3)。*

# P13 External Review Round 4

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 is not approved.

The round-3 remediation is real: all four round-3 reviewer regressions now
pass, the switch is part of `__proxy__` authorization, zero-epoch heartbeat
repair is reachable, and all directive sources carry `(generation, epoch)`.

The new ordering model is not yet a valid cross-restart fencing protocol,
however. Its generation is derived from a fallible wall clock, while heartbeat
reports only epoch and therefore cannot verify the pair that the agent actually
applied. The OFF cleanup path also still claims success after failures that can
leave an existing listener alive or make the next ON unrecoverable. The locked
real-deployment exit criteria remain incomplete.

## Findings

### F1 - High: wall-clock UnixNano is not a monotonic broker generation

`Broker.New` sets `proxyGen = cfg.Now().UnixNano()`
(`internal/broker/broker.go:345-354`), while protocol and architecture comments
claim that this value increases across every restart, including DB restore
(`internal/proto/messages.go:765-775`,
`docs/architecture.md:1890-1893`).

That assumption is false. NTP/manual correction, VM restore, host migration,
RTC problems, or an injected clock can make a later process observe an equal or
lower wall time.

This is a fencing failure, not just an availability edge:

1. old broker generation 200 computes or emits an enabled directive;
2. broker restarts after a clock rollback and gets generation 100;
3. the new broker emits an authoritative OFF at generation 100;
4. the agent either drops the new OFF behind its applied generation 200, or a
   delayed old generation-200 ON arrives and wins over the new OFF.

The independent reviewer regression fails deterministically:

```text
go test ./internal/broker \
  -run '^TestExternalReviewBrokerGenerationSurvivesClockRollback$' \
  -count=1
# FAIL:
# later broker generation=100000000000 did not advance past
# prior generation=200000000000
```

Test: `internal/broker/p13_external_review_round4_test.go:14-42`.

Recommendation: use a real fencing source, not wall time. One conservative
design is an atomically incremented incarnation counter stored outside the
session DB restore boundary and preserved during broker migration. Another is
an explicit startup/takeover protocol in which agents report their last applied
generation and the broker obtains a value greater than every observed value
before issuing directives. A counter inside the same rewound DB is insufficient
unless restore explicitly preserves or advances it.

Add tests for backward/equal clocks, DB snapshot restore, host migration, and a
delayed old register reply racing a new OFF.

### F2 - High: heartbeat cannot detect generation mismatch at the same epoch

The agent's ordering state is a pair, but heartbeat still contains only
`ProxyEpoch` (`internal/proto/messages.go:121-130`,
`internal/agent/agent.go:698`). The broker passes only that scalar into
`repairProxyEpoch` (`internal/broker/broker.go:737-741`), and a ready node with
an equal epoch is considered converged (`internal/broker/proxy.go:481-493`).

Two DB snapshots can have the same numeric epoch but different subscriber
keysets. After restore, a ready agent from the prior broker generation can
therefore keep accepting a revoked key indefinitely: heartbeat says the epoch
matches, so the current generation/keyset is never pushed.

The independent reviewer regression sends an older generation with the same
epoch and receives no repair:

```text
go test ./internal/broker \
  -run '^TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch$' \
  -count=1
# FAIL:
# same-epoch heartbeat from an older generation was treated as converged
```

Test: `internal/broker/p13_external_review_round4_test.go:44-103`.

Recommendation:

1. add `HeartbeatPayload.ProxyGeneration`;
2. have the agent report its successfully applied `(generation, epoch)` pair;
3. change repair to compare both fields;
4. consider ON converged only when `ready && agentPair == brokerPair`;
5. push the current directive on any generation mismatch, including equal
   epochs.

If F1's generation is learned from agents during takeover, the register request
also needs the agent's last applied generation.

### F3 - High: OFF cleanup failures can still leave an existing exit alive

`disableProxy` treats everything after `proxy_enabled=0` as best effort
(`internal/broker/proxy.go:124-157`).

The switch check prevents new `__proxy__` REGISTER authorization, but it does
not close an already-installed public listener:

- if `port.ListBySession` fails, no ports are passed to `CloseProxy`, yet OFF
  returns `OK`;
- if `BumpProxyEpoch` fails, `epoch` remains zero and a same-generation agent
  that already applied a higher epoch drops the disable directive as stale;
- if `port.Free` fails, the code still publishes a `freed` event and returns
  success. The next ON sees the stale ALLOCATED row and sends a tokenless
  keyset-only directive, while the prior OFF cleared the agent footprint. That
  node can then remain unready with no path to receive a fresh token.

The first two failures together leave the existing data plane dependent on
agent/NATS behavior, contradicting the documented broker-side immediate kill.
Logging does not restore the security property.

Recommendation: make active proxy listeners discoverable and closable by
session in `tunnel.Server`, independent of a post-switch DB query, then close
them synchronously before replying success. Treat epoch/allocation cleanup as a
durable reconciliation operation. Publish `freed` only after `port.Free`
succeeds, and ensure a later ON rotates a stale allocation and sends a full
token-bearing directive when no valid footprint exists. Add injected
`BumpProxyEpoch`/`ListBySession`/`Free` failure tests.

### F4 - Exit blocker: real Caddy/WSS and Clash validation is still pending

The CLI wire test now includes `proxy status` and `proxy sub ls`, closing the
round-3 test-coverage comment. The combined in-process SS-over-tunnel path also
passes.

The locked exit criteria still require:

- real Caddy + ACME with `/sub/*` and the NATS WSS catch-all coexisting;
- a real Clash client importing the subscription and reaching the internet
  through an agent.

They remain unchecked in `log.md:375-380`. This requires either completion in
the lab or an explicit owner revision of the phase exit criteria. A reviewer
cannot silently waive it.

## Accepted Round-3 Fixes

- `TestExternalReviewProxyTokenDeniedWhileSwitchOff`: PASS.
- `TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush`: PASS.
- `TestExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy`: PASS.
- `TestExternalReviewHeartbeatConvergesAfterDBEpochRewind`: PASS.
- The four tests also passed repeated and race runs before the round-4 tests
  were added.
- CLI wire coverage now includes all six proxy commands.
- Architecture and plan describe the submitted implementation, although F1/F2
  show that the submitted convergence contract itself still needs revision.

## Questions

1. What state is guaranteed to survive a DB restore and broker host migration?
   The fencing design needs an explicit persistence boundary.
2. Is `proxy off` allowed to mean only "deny new registrations", or must the
   command guarantee that existing public listeners are closed before success?
   The locked architecture currently says the latter.
3. Will the owner schedule the real Caddy/Clash lab validation, or explicitly
   revise the locked exit criteria?

## Verification

Passed:

```text
CGO_ENABLED=0 go build ./...
go list -deps -f '{{if .CgoFiles}}{{.ImportPath}} {{.CgoFiles}}{{end}}' ./...
# no output

golangci-lint run
# 0 issues

git diff --check

go test -count=1 -tags e2e_matrix -v ./test/e2e/...
# P1-P8 and P13 pass; P9/P10 hit the recorded macOS baselines
```

`go test ./... -count=1` fails on the two new round-4 reviewer tests plus the
already recorded macOS `/private/var`, Unix-socket path, CLI `exit 64`, and
agent-install baselines. `test/p13` and the other P13 packages pass.

The focused race run fails only on the two new broker regressions and the
recorded agent `/private/var` tests; `internal/tunnel`, `internal/subhttp`,
`internal/proto`, `internal/proxysub`, `internal/port`, and `test/p13` pass
under `-race`.

## Re-review Gate

Re-review after:

1. broker generation is a real monotonic fencing token across the supported
   restart/restore/migration model;
2. heartbeat convergence compares the complete generation/epoch pair;
3. OFF closes existing public listeners despite allocation-store cleanup
   failures and does not report false lifecycle events/success;
4. the two round-4 reviewer tests pass repeatedly and under `-race`;
5. the real deployment exit criteria are completed or explicitly revised by
   the project owner.

---

## 主进程回复（round-4 整改）

日期: 2026-06-10。采纳 F1–F3 全部 finding。**你的 2 个 round-4 reviewer 测试现已通过**,并新增 3 个对抗测试覆盖你点名的注入失败/会话级杀监听场景;`-count=10`/`-race` 稳定;`golangci-lint v2.5.0` 0 issues、`CGO_ENABLED=0` deps 无 cgo。`go test ./...` 仅余既有 macOS 基线。

### F1（High,wall-clock 不是单调 fencing token)— 已修(持久单调计数器)
采纳。`Generation` 不再是裸 `now.UnixNano()`,改为**持久化** `proxy_meta.generation`(migration 0006 新增一行表),每次 `broker.New` 取 `advanceProxyGeneration = max(persisted+1, now_unixnano)` 并写回。故时钟回拨(NTP/手动/VM 还原)后启的进程仍得严格更大值。`TestExternalReviewBrokerGenerationSurvivesClockRollback` 通过(200→回拨到 100,新 gen=200000000001 > 200000000000)。
- 回答 Q1 + 你的「DB 内计数器不足以 fence 还原」:对。**运维契约已显式落档**(migration 0006 注释 + architecture §L.3 + plan 附录):从更旧备份 DB 还原会一并回退该行,还原 runbook 必须把它推进到超过任何 agent 可能已应用的值(例如设为还原时刻 now_unixnano)。这是 fencing 的持久边界。我没有引入独立的 DB-外计数器(会牵出新的持久化/迁移面);持久 `max(+1, now)` + 显式还原 runbook 在本项目部署模型(单 broker + SQLite crown-jewel)下足以满足重启/回拨,DB 还原由 runbook 兜。若你要求强制 DB-外 fencing(独立文件/外部 KV),请指示,我评估为后续增量。

### F2（High,心跳无法在同 epoch 检出 generation 不符)— 已修(心跳带完整对)
采纳全部 5 条建议。`HeartbeatPayload` 增 `ProxyGeneration`;agent 用 `proxyGenEpoch()` 上报已成功应用的 `(gen, epoch)`;`handleHeartbeat` 把整对传给新 `repairProxy(sid,nid,agentGen,agentEpoch)`;**仅当 `ready && agentGen==brokerGen && agentEpoch==sessionEpoch` 才算收敛**,任何 generation 不符(含同 epoch)都补推当前 directive。`TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch`(gen-1、同 epoch ⇒ 收到补推)通过;round-3 的 `repairProxyEpoch(sid,nid,epoch)` 保留为薄封装(agentGen=0)供其直调测试,行为一致(0 != brokerGen ⇒ 必推)。

### F3（High,OFF 清理失败仍可能留活监听)— 已修(CloseSession 不依赖 DB + 失败回报错 + 轮换)
采纳全部建议:
- **`tunnel.Server.CloseSession(sid)`**:serverSession 记 `sid`,OFF 提交开关后**立即同步关该 session 全部已建公网监听 + bump 各 port killGen,完全不查 DB**。故即便随后 `ListBySession` 失败,数据面已死。`TestExternalReviewCloseSessionKillsListenersWithoutDB`(双端口、跨 session 隔离)通过。
- **失败不谎报成功**:`BumpProxyEpoch` 或 `ListBySession` 失败 ⇒ OFF 回 `store_error`(不再 best-effort 吞错回 OK),因为 epoch 不 bump 会让同代高 epoch agent 丢弃 disable。`TestProxyOffReportsErrorWhenAllocStoreFails`(DROP port_allocations 注入)通过 —— 且开关仍权威 OFF。
- **`freed` 只在 `port.Free` 成功后发**;失败留 stale ALLOCATED 行。
- **下次 ON 轮换**:`enableProxy` keep-vs-replace 改为**仅当节点 `proxy_ready=1` 才 token-less 保留**;否则 Free stale 行 + 重铸 port+token 发完整 directive。`TestProxyOnRotatesStaleNotReadyAllocation`(stale 行 + not-ready ⇒ 收到带 token 的新 directive、token hash 变更)通过。
- 回答 Q2:`proxy off` 现在**两者都保证** —— 既拒新 REGISTER(开关授权边界),又同步关已建监听(CloseSession)。与锁定架构「broker 侧即时 kill」一致。

### F4（Exit blocker,真 Caddy/WSS + 真 Clash)— 仍需 owner 裁决(Q3)
in-process 覆盖已齐(6 条 CLI wire + 合并数据面)。真 ACME-Caddy `/sub/*`×WSS 共存、真 Clash 导入出网为**锁定出口标准**,本机 macOS 无法复现(无公网域名 / `--role broker` 不支持)。回答 Q3:**请你(项目 owner)定** —— 排到下次 lab 窗口按 log.md checklist 跑完打勾,或显式修订该出口标准并记入 plan。我不自行 waive。

### F5 文档 — 已同步
architecture §L.2/§L.3/§L.5 + plan 附录 + log.md + migration 0006 注释全部更新到 round-4 契约(持久 generation fencing + 运维还原 runbook、心跳完整对、CloseSession 不依赖 DB、OFF 失败语义、stale 轮换)。

### 复核请关注
- 2 个 round-4 reviewer 测试 + 3 个新对抗测试是否如期绿(本机 `-count=10`/`-race` 已过)。
- F1 的「持久 `max(+1,now)` + 还原 runbook」是否被接受为本项目部署模型下的 fencing 边界,还是要求强制 DB-外 fencing(我可评估为后续增量)。
- F4 出口标准处置(排期 lab vs 显式修订)请你定。
