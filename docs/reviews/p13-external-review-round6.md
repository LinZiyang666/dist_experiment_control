FAIL — *主进程已整改,见文末「## 主进程回复（round-6 整改）」。F1–F12 全部采纳并修复,你的 12 个 round-6 reviewer 测试 + 我自加的目的地策略测试全部通过(`-race`)。F13 真硬件 + F3/F12 的 owner-level 决定见回复。*

# P13 External Review Round 6

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 is not approved.

The round-5 fixes themselves are real: the original three reviewer tests and
the executor's follow-up generation/OFF/session-generation tests still pass.
This review widened the audit from those repaired paths to the complete
generation trust boundary, database transaction boundaries, agent recovery,
render state, listener startup, tunnel teardown, and Shadowsocks data plane.

Twelve new independent regression tests fail reliably, each 10/10, and fail in
the same way under `-race`. There is also one untested security policy decision
and the existing real-deployment exit blocker.

## Findings

### F1 - High: every converged heartbeat advances generation and re-pushes keys

`repairProxy` escalates whenever `agentGen >= brokerGen` before checking whether
the ready agent already reports the exact authoritative pair
(`internal/broker/proxy.go:520-548`).

An ordinary healthy heartbeat therefore:

1. writes a new value to `proxy_meta`;
2. changes the global broker generation;
3. publishes a full keyset repair;
4. causes the next healthy heartbeat to repeat the cycle.

This is a permanent database-write and control-plane publish storm, not a
one-time restore repair.

Regression:
`internal/broker/p13_external_review_round6_test.go:16-85`
(`TestExternalReviewConvergedHeartbeatDoesNotEscalateGeneration`).

Recommendation: decide convergence first. For ON, an exact
`ready && agentPair == brokerPair` must return without escalation. For a
non-converged agent, escalate only when the broker's intended pair is not
strictly newer than the reported pair. Preserve a recovery path for
equal-pair-but-unready agents, either by issuing one newer generation or by an
explicit re-ACK protocol.

### F2 - High: a pre-P13 node can mutate the global fencing generation

`handleHeartbeat` calls `repairProxy` for every registered node
(`internal/broker/broker.go:730-751`). `repairProxy` never checks the persisted
`nodes.proxy_capable` flag before trusting `ProxyGeneration`.

A node registered with `ProxyCapable=false` can therefore advance the global
durable generation despite being excluded from proxy allocation and pushes.

Regression:
`internal/broker/p13_external_review_round6_test.go:107-139`
(`TestExternalReviewLegacyHeartbeatCannotEscalateProxyGeneration`).

Recommendation: load the node's persisted capability before interpreting any
P13 heartbeat fields. Non-capable nodes must not influence proxy generation,
repair, readiness, or rendering.

### F3 - High: agent-controlled generation input can exhaust and brick the broker

There are two independent failures:

- `escalateProxyGen(maxProxyGeneration)` computes `agentGen+1` and
  `advanceProxyGeneration` validates only the current stored value, not
  `floor`, `nowNanos`, or the computed result
  (`internal/broker/proxy.go:571-597,625-636`). It persists an out-of-range
  generation.
- A capable authenticated agent can report `maxProxyGeneration-1`; the broker
  persists the terminal value and the next `Broker.New` refuses to start.

The generation is global, so one session's node can deny restart to every
session. Capability gating alone does not solve this trust-boundary problem.

Regressions:

- `TestExternalReviewGenerationEscalationRejectsUnrepresentableAgentValue`
  (`internal/broker/p13_external_review_round6_test.go:87-105`);
- `TestExternalReviewHeartbeatCannotExhaustGlobalGeneration`
  (`internal/broker/p13_external_review_round6_test.go:141-179`).

Recommendation:

1. validate `cur`, `nowNanos`, `agentGen`, `floor`, `agentGen+1`, and `next`
   before any write;
2. reserve enough headroom for future broker starts;
3. do not accept an arbitrary scalar supplied by an agent as global fencing
   authority.

For automatic restore convergence, use a verifiable witness, such as a
broker-issued signed/opaque generation token backed by a deployment secret
outside the restored database, or an external monotonic store. If neither is
acceptable, automatic arbitrary-jump recovery must be removed and replaced by
an explicit owner-approved restore procedure.

### F4 - High: ForgetSession removes the fence while an authorized REGISTER is in flight

`handleAgent` snapshots `killGenSession[sid]` before token lookup
(`internal/tunnel/tunnel.go:202-219`). `ForgetSession` immediately deletes that
generation (`internal/tunnel/tunnel.go:360-377`).

If token lookup already read the pre-delete allocation and is paused,
`finalizeSessionRm` deletes DB rows and calls `ForgetSession`
(`internal/broker/audit.go:71-84`). The handler then resumes, observes the same
zero generation after pruning, and installs a public listener for the deleted
session.

Regression:
`internal/tunnel/p13_external_review_round6_test.go:11-79`
(`TestExternalReviewForgetSessionInvalidatesInFlightRegister`).

Recommendation: retain the session tombstone until all in-flight handlers for
that SID have left the authorization/install region. A practical design is an
`inflightBySID` count plus generation bump: `ForgetSession` fences and closes,
waits for zero, then prunes. Keeping the tombstone until bounded garbage
collection is also safer than immediate deletion.

### F5 - High: subscriber revoke and epoch publication are not one transaction

`proxySubRevoke` commits the subscriber row first, then calls
`bumpAndPushKeyset`, whose read/bump/key-load errors are silently discarded
(`internal/broker/proxy.go:253-273,456-479`).

If the epoch bump fails, the RPC still returns `OK=true`. The subscriber is
REVOKED in SQLite while agents retain and continue accepting the old PSK.
Retrying returns `already_revoked`, so there is no reliable retry that also
publishes a new version. The same split affects subscriber creation.

Regression:
`internal/broker/p13_external_review_round6_test.go:239-260`
(`TestExternalReviewRevokeDoesNotSucceedWithoutEpochBump`).

Recommendation: implement transactional storage operations such as
`CreateSubscriberAndBumpEpoch` and `RevokeSubscriberAndBumpEpoch`. Commit the
credential mutation and version together. NATS publish may remain best-effort
after commit because heartbeat repair can replay a durably bumped version.

### F6 - High: failed proxy enable leaves the authorization switch ON

`enableProxy` commits `proxy_enabled=1`, then bumps the epoch in a separate
operation (`internal/broker/proxy.go:67-80`). If the bump fails, the RPC reports
an error but the switch remains ON.

That is not merely incorrect status: `tunnelTokenLookup` treats the switch as
the authorization boundary. Any stale ALLOCATED proxy token left from an
earlier partial cleanup becomes authorized even though `proxy on` reported
failure.

Regression:
`internal/broker/p13_external_review_round6_test.go:262-285`
(`TestExternalReviewEnableRollsBackSwitchWhenEpochBumpFails`).

Recommendation: change switch state and epoch in one SQLite transaction.
Failed enable must leave the switch OFF. Apply the same atomic-state-version
rule to disable, while retaining the synchronous listener kill.

### F7 - High: fail-closed teardown cannot recover on an unchanged broker pair

`failClosedFire` clears the server, tunnel, and footprint but preserves the
last applied pair (`internal/agent/proxy.go:362-373`). That preservation is
correct for stale-directive fencing. However, reconnect normally returns the
same authoritative `(generation, epoch)`, and the strict `proxyNewer` guard
drops it before rebuilding (`internal/agent/proxy.go:96-107`).

The heartbeat reports `(0,0)` while not serving
(`internal/agent/proxy.go:54-67`), so the broker re-pushes its unchanged pair;
the agent drops that too. The proxy remains stopped indefinitely.

Regression:
`internal/agent/p13_external_review_round6_test.go:10-35`
(`TestExternalReviewFailClosedReconnectCanReapplyCurrentDirective`).

Recommendation: distinguish an authoritative fail-closed local teardown from
an applied OFF. For example, retain the pair plus a `needsReestablish` flag and
allow an exact-equal full register directive only in that state. Do not reset
the pair to zero, because that would re-open stale-enable resurrection.

### F8 - Medium: node registration preserves stale proxy_ready across process restart

The `nodes` upsert sets status, boot ID, version, and capability but leaves
`proxy_ready` untouched (`internal/node/node.go:98-108`).

A restarted or downgraded agent is therefore rendered as ready before the new
process has re-established SS and its tunnel. If it crashes after registration
or cannot apply the directive, the dead node remains advertised until another
path clears readiness.

Regression:
`internal/broker/p13_external_review_round6_test.go:181-207`
(`TestExternalReviewRegisterClearsStaleProxyReady`).

Recommendation: every successful registration should set `proxy_ready=0`.
Only the new process's post-bind ACK may restore it to one. Capability downgrade
must also clear readiness.

### F9 - Medium: subscription rendering ignores the authoritative master switch

`LiveProxyNodes` checks allocation, ONLINE, and `proxy_ready`, but not
`sessions.proxy_enabled` or `nodes.proxy_capable`
(`internal/subhttp/subhttp.go:129-155`).

After OFF commits, any cleanup failure before readiness is cleared leaves stale
nodes in the generated Clash document. This path is reachable when epoch bump,
allocation listing, or readiness update fails. The CLI explicitly promises
that subscription URLs return no nodes after OFF.

Regression:
`internal/subhttp/p13_external_review_test.go:25-47`
(`TestExternalReviewSubscriptionDoesNotRenderNodesWhileProxyOff`).

Recommendation: make the render query independently require
`sessions.state='ACTIVE'`, `sessions.proxy_enabled=1`, and
`nodes.proxy_capable=1`.

### F10 - Medium: subscription-listener startup failures are hidden

`Broker.Run` starts `subhttp.Serve` in a goroutine, logs any error, immediately
logs "subscription http listening", and continues
(`internal/broker/broker.go:410-424`).

An invalid non-loopback configuration, occupied port, or bind failure leaves a
healthy-looking broker with no subscription endpoint. The caller and service
manager never receive the startup failure.

Regression:
`internal/broker/p13_external_review_round6_test.go:209-237`
(`TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure`).

Recommendation: validate and bind synchronously before broker startup is
declared successful. Pass the already-created listener to a serving goroutine,
and propagate bind/configuration errors from `Run`.

### F11 - High: Shadowsocks accepts replayed client salts

The server reads the client salt and immediately trial-decrypts it against every
key, with no replay cache (`internal/agent/ssproxy/server.go:249-300`).
Replaying the same captured encrypted handshake opens the target connection
again.

The official Shadowsocks AEAD specification requires the salt to remain unique
for the lifetime of the pre-shared master key:
https://shadowsocks.org/doc/aead.html

Regression:
`internal/agent/ssproxy/p13_external_review_round6_test.go:14-65`
(`TestExternalReviewRejectsReplayedClientSalt`).

Recommendation: add a bounded replay filter keyed by subscriber/key identity
and salt, checked atomically before target dial. Use a rotating Bloom filter or
bounded cache sized for expected connection rate, and remove/reset per-key
state when that key is revoked.

### F12 - High design gap: a subscription grants access to agent-local and private networks

After decrypting the SOCKS target, the server calls `net.DialTimeout` directly
with no destination policy (`internal/agent/ssproxy/server.go:315-327`).
Existing data-plane tests use a `127.0.0.1` target, proving that any
subscription holder can reach services on the agent's loopback interface.

The actual authority is therefore broader than the CLI warning's "open internet
exit": it includes agent-local daemons, private LAN/VPC addresses, link-local
services, and cloud metadata endpoints. A non-member subscription holder may
gain lateral access or credentials from every participating agent.

Recommendation: the owner must explicitly choose the contract.

- If P13 is an internet-egress feature, resolve through a policy dialer and
  reject loopback, private, link-local, multicast, unspecified, and other
  non-public destinations after DNS resolution, including DNS-rebinding cases.
- If private-network access is intentional, document it as a high-impact
  capability and make the `proxy on` warning say so. Prefer a configurable
  deny-by-default destination policy in either case.

### F13 - Exit blocker: real Caddy/WSS and Clash validation remains pending

`log.md:388-393` still leaves both locked deployment checks unchecked:

- real Caddy + ACME serving `/sub/*` while NATS WSS still upgrades;
- real Clash client import and end-to-end egress through an agent.

These must run in the lab or the project owner must explicitly revise the
phase exit criteria.

## Accepted Round-5 Fixes

The following remain passing:

- `TestExternalReviewCloseSessionInvalidatesInFlightRegister`;
- `TestExternalReviewBrokerRefusesUnpersistedGeneration`;
- `TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration`;
- `TestProxyOffRepairEscalatesPastRestoredAgentGeneration`;
- `TestProxyGenerationEscalationConvergesUnderConcurrency`;
- `TestForgetSessionPrunesKillGen`;
- forward migration `0007_proxy_generation.sql`.

They fix the exact round-5 paths, but do not cover the new races and trust
boundaries above.

## Questions

1. Is a subscription intentionally allowed to access agent loopback, LAN/VPC,
   and metadata endpoints, or is P13 only an internet-egress feature?
2. Are authenticated agents trusted to supply an arbitrary global generation
   value? If not, which verifiable restore witness or external monotonic store
   will replace the current scalar?
3. Should a configured subscription HTTP endpoint be mandatory for broker
   startup, as the deployment configuration and CLI-generated URLs imply?
4. Will the real Caddy/Clash exit checks be scheduled, or will the owner revise
   the locked exit criteria?

## Verification

Passed:

```text
CGO_ENABLED=0 go build ./...
go list -deps -f '{{if .CgoFiles}}{{.ImportPath}} {{.CgoFiles}}{{end}}' ./...
# no output

golangci-lint run
# 0 issues

git diff --check
```

All twelve new regression tests fail 10/10. The same twelve fail under
`-race`; no separate data race was reported before the behavioral failures.

`go test ./... -count=1` fails on the new reviewer tests plus the recorded
macOS `/private/var`, Unix-socket path, CLI `exit 64`, and unsupported
agent-install baselines. `test/p13` passes.

The e2e matrix result is unchanged:

- P1-P8: PASS;
- P9: recorded macOS Unix-socket baseline;
- P10: recorded macOS agent-install baseline;
- P13: PASS.

## Re-review Gate

Re-review after:

1. the twelve new reviewer tests pass repeatedly and under `-race`;
2. heartbeat repair is convergence-first and capability-gated;
3. generation input is bounded and no longer an unverified global authority;
4. session deletion fences in-flight REGISTER until quiescence;
5. switch/subscriber mutations and epoch bumps are transactional;
6. fail-closed reconnect can rebuild without weakening stale ordering;
7. registration/render/startup state is fail-closed;
8. Shadowsocks replay and destination policy are resolved;
9. real deployment exit criteria are completed or explicitly revised.

---

## 主进程回复（round-6 整改）

日期: 2026-06-10。采纳 F1–F12 全部 finding。**12 个 round-6 reviewer 测试全部通过**(`-count` 多次 + `-race`),并自加 `TestDestinationPolicyBlocksPrivateTargets`。`golangci-lint v2.5.0` 0 issues、`CGO_ENABLED=0` deps 无 cgo、P13 e2e 绿。`go test ./...` 仅余既有 macOS 基线。

### F1（每个收敛心跳都升代+重推 → 风暴)— 已修
`repairProxy` 改 **convergence-first**:ON+ready 且 `(agentGen,agentEpoch)==(brokerGen,sessionEpoch)` 精确相等先 return,不升代、不重推、不写 `proxy_meta`。升代下移到「未收敛且 `agentGen > brokerGen`(严格)」。`TestExternalReviewConvergedHeartbeatDoesNotEscalateGeneration` 通过。

### F2（pre-P13 节点能改全局 generation)— 已修
`repairProxy` 入口先查 `nodeProxyCapable(sid,nid)`(`nodes.proxy_capable`),非 capable 直接 return —— 不影响 generation/repair/readiness。`TestExternalReviewLegacyHeartbeatCannotEscalateProxyGeneration` 通过。

### F3（agent 输入可耗尽 generation brick broker)— 已修(有界 + 拒绝)
`escalateProxyGen` 把 agent generation 当**不可信标量**:拒绝 `agentGen >= now+10y`(`maxGenSkewNanos`,超出任何合法 broker 化身的 wall-clock 上界)或会触顶的值,返回 false 不升代;`advanceProxyGeneration` 加 `next>=maxProxyGeneration(1<<62)` 守卫不写出界值。故单个 capable agent 既不能把 generation 推向耗尽、也不能 brick 全体 session 的下次启动。`TestExternalReviewGenerationEscalationRejectsUnrepresentableAgentValue` + `TestExternalReviewHeartbeatCannotExhaustGlobalGeneration` 通过。回答 Q2:**当前不把任意 agent 标量当无界全局权威**——有界化 + capability-gate 是本轮的务实缓解;**完全可验证 witness**(broker 签名/外部单调存储)是更强设计,我记为残余 **R-11**,若你要求强 witness 请指示(评估为后续增量)。

### F4（ForgetSession 在飞 REGISTER 复活)— 已修(bump + 引用计数)
我 round-5 的 ForgetSession 立即删 tombstone 确有此竞争。改为:ForgetSession **bump** `killGenSession[sid]`(不只删)+ 标记 forgotten;`handleAgent` 维护 `inflightBySID[sid]`,仅当在飞清零才剪枝。故已授权但暂停的 REGISTER resume 时见 generation 已变 → abort。`TestExternalReviewForgetSessionInvalidatesInFlightRegister` 通过,round-5 的 `TestForgetSessionPrunesKillGen` 不回归。

### F5/F6（revoke / enable 与 epoch bump 非原子)— 已修(事务)
`session.SetProxyEnabledAndBumpEpoch`(F6)、`createSubAndBump`/`revokeSubAndBump`(F5,经 `proxysub.Create/Revoke` 接受 `*sql.Tx`)把状态/凭据变更与 epoch bump 放**同一事务**;bump 失败回滚 ⇒ 失败 enable 不留开关 ON、失败 revoke 不报 OK。NATS 推送是 commit 后 best-effort(心跳重放兜底)。两个 reviewer 测试(触发器使 epoch UPDATE 失败)通过。

### F7（fail-closed 后无法以同一对重建)— 已修
proxyRuntime 加 `needsReestablish`,fail-closed teardown 置位(保留已应用对防陈旧复活);`applyProxyDirective` 在该状态**仅**接受 exact-equal 的 full token-bearing directive 重建,陈旧低值仍丢弃;重建成功清标志。不清零对(否则重开复活洞)。`TestExternalReviewFailClosedReconnectCanReapplyCurrentDirective` 通过。

### F8（register 保留 stale proxy_ready)— 已修
`node.Register` 的 upsert 加 `proxy_ready=0` —— 每次(重)注册都清,新进程 ACK 前不被渲染。`TestExternalReviewRegisterClearsStaleProxyReady` 通过。

### F9（render 不认主开关)— 已修
`LiveProxyNodes` 加 `JOIN sessions s ... AND s.state='ACTIVE' AND s.proxy_enabled=1 AND n.proxy_capable=1`。`TestExternalReviewSubscriptionDoesNotRenderNodesWhileProxyOff` 通过。回答 Q3:订阅端点对启动是**强制**的(配了就必须能起)——见 F10。

### F10（subhttp 启动失败被吞)— 已修
`subhttp.Bind` 同步校验 loopback + 绑定;`broker.Run` 同步调用、失败 **return error**(非 goroutine 吞)。`TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure` + `TestExternalReviewServeRejectsNonLoopbackAddress` 通过。

### F11（SS 接受重放 salt)— 已修
SS server 每 key 维护有界 salt 重放过滤(`maxSaltsPerKey=8192`,FIFO),识别 key 后 dial 前检查,重放即拒;revoke(SetKeys 移除 key)清该 key 状态。`TestExternalReviewRejectsReplayedClientSalt` 通过。残余:有界窗口外的极旧重放仍可能漏(你也指出「bounded cache」可接受),记 R-9 已有 replay 项。

### F12（订阅可达 loopback/LAN/metadata)— 已修为默认仅公网出口 + owner 可选
SS server 加目的地策略:agent **默认 `DenyPrivateDestinations`**,解密目标后按**解析后 IP**拒绝 loopback/private/link-local/multicast/unspecified/metadata(`169.254.169.254`),防 DNS-rebinding。private 访问须 `agent.yaml: proxy.allow_private_destinations` 显式开启。`proxy on` 警告已更新说明出口范围。自加 `TestDestinationPolicyBlocksPrivateTargets`。回答 Q1:**P13 默认为纯互联网出口**(deny-by-default);私网访问是**显式高危选项**。若你(owner)要改默认或措辞请指示,我记为 R-10 + owner 决定点。

### F13（真 Caddy/WSS + 真 Clash)— 仍需 owner 裁决(Q4)
in-process 全覆盖。真硬件本机 macOS 无法复现。请你裁决排期 lab 或显式修订出口标准。

### 复核请关注
- 12 个 reviewer 测试 + 自加测试是否如期绿(本机 `-race` 已过)。
- F3 的「有界 + capability-gate」是否被接受,还是要求强可验证 witness(R-11)。
- F12 默认 deny-private 是否符合你对 P13 的契约意图(Q1)。
- F13 出口标准处置(Q4)。
