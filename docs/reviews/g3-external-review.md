# Fail - G3 external review

结论：Fail。G3 的 seed 自动收敛和 ctl live roster-pull 主体实现基本成形，PIN/no-TOFU 门、actor-scoped subject、ACL carve-out、online/offline seed drop 的核心不变量没有发现越权或直接数据破坏。但 ctl refresh 的新 NATS-primary 路径会把“合法但旧”的 roster-pull 响应当成成功，跳过可用的 HTTP bootstrap 新 manifest，并把下一次刷新推迟 `ctlRefreshTTL`（10 分钟）。这会让 G3 声称的“连上任一幸存者后名册收敛”在成员变化后的 manifest cache 窗口里失效。

本次只审查未暂存/未跟踪内容；开始时 staged baseline 为空。内部 G3 review 只作为风险索引，不作为结论依据。审查清单已单独落盘：`docs/reviews/g3-external-review-tasklist.md`。

## Tasklist / review surface

- [x] 重建未暂存 tracked diff、未跟踪测试/文档、空 staged baseline 的审查边界。
- [x] 复读 `CLAUDE.md`、architecture/cluster 文档、G3 plan/review、近期外审格式。
- [x] 审查 seed derive/host-match/VIP/custom endpoint/first-publish/manual seed 语义。
- [x] 审查 join/retire/online force-single/recovery remove/leadership backstop 触发点。
- [x] 审查 offline force-single drop-only、事务边界、generation bump、empty-set floor。
- [x] 审查 ctl roster-pull、权限、actor scoping、PIN gate、fallback、TTL/no-poison。
- [x] 审查 subject namespace 是否越过 broker-only `cluster.*` 边界。
- [x] 审查文档诚实性与测试覆盖，并添加外审回归测试。
- [x] 运行聚焦测试、静态检查和编译门；记录 simcluster 判定。

## Findings

### F1 - Major - stale NATS roster-pull can mask a fresher HTTP manifest and throttle convergence for 10 minutes

`cmd/tether/ctl_connect.go:103-114` makes NATS roster-pull primary and only uses HTTP bootstrap when the NATS request returns `nil`. If NATS returns a syntactically valid, account-signed manifest, `refreshCtlEndpoints` never checks whether it advanced `RosterGen` / `SeedGen` before skipping HTTP and writing `FetchedAt`.

This matters because `internal/broker/cluster_manifest.go:43-50` serves cached manifest bytes for up to `manifestRecheckInterval` (30s) without checking DB generations. Right after `join approve`, `retire`, or force-single seed convergence, a broker can still answer roster-pull with the previous signed manifest. `AdoptDecision` accepts equal-generation manifests (`>=`), so the stale response is treated as a successful refresh; then `FetchedAt` suppresses the next refresh for `ctlRefreshTTL` (10 minutes).

Impact: a ctl that connects during the 30s manifest cache window can miss fresh endpoints even when its HTTP bootstrap would already return the newer manifest. In a failover/grow incident, this leaves `cluster_endpoints.json` stale for up to 10 minutes after a command that was supposed to converge it.

Independent reviewer regression added: `cmd/tether/g3_external_review_test.go`.

Failing command:

```text
GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP -count=1
```

Observed failure:

```text
RosterGen=1, want 7 (FetchedAt="2026-07-07T11:18:40Z")
```

Recommendation: after a NATS manifest is parsed, compare the adopted result against the previous `RosterGen`/`SeedGen`. If it makes no generation progress and `BootstrapURL` exists, try HTTP before writing `FetchedAt`; alternatively do not advance `FetchedAt` for non-advancing manifests, or add a short retry TTL for “valid but no progress” responses. The fix should preserve the existing no-poison rule: foreign/rollback manifests must not mutate roster/seed/FloorURL.

### F2 - Minor - docs overstate automatic seed convergence for dead-peer-only seed sets

`internal/broker/seed_converge.go:177-178` treats any stored seed set whose hosts match no current broker as operator-curated VIP/LB and leaves it untouched. The added test itself documents the residual gap at `internal/broker/g3_seed_converge_test.go:196-201`: a seed set containing only departed peer endpoints is indistinguishable from a pure VIP set, so the dead endpoint lingers.

That tradeoff may be acceptable, but `docs/cluster.md:259-264` currently says post-join/retire/force-single client rosters automatically converge and `cluster seeds show` reflects true members, while only warning about pure VIP and mixed VIP sets. It does not mention the dead-peer-only exception.

Recommendation: either implement a survivor-anchored fallback for “all stored broker-like hosts are departed” or document this as an explicit residual GAP with the expected recovery path (`cluster seeds publish` or `cluster pin` InviteSeeds).

## Doubts / residual risk

- I did not run a deploy-tier simcluster drill. This change touches cluster lifecycle semantics but not `install.sh`, systemd, nats.conf rendering, or simcluster scripts. There is also no G3 drill in `test/simcluster/` to run. A future deploy-tier drill should cover grow seed auto-publish, force-single seed shrink, and ctl failover refresh from a survivor.
- The roster-pull responder is broadcast first-reply-wins. This is probably acceptable under monotone verification, but combined with cache windows it increases the chance of receiving a non-advancing manifest first.

## Verification

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./internal/cluster ./internal/clusteroffline -run 'TestSeedEndpointsDropHosts|TestPruneRosterPeers' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestDeriveSeedEndpoints|TestSeedSetEqual|TestSeedHostsMatchAnyBroker|TestG3Seed|TestForceSingleOnlineConvergesSeeds|TestG3RemoveGhostConvergesSeeds' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestG3FetchManifestOverNATS|TestRefreshCtlEndpointsGateAHoldsOverLiveNATS|TestRefreshCtlEndpointsAdoptsOverNATSPrimary|TestRefreshCtlEndpointsNoHTTPTOFU|TestRefreshCtlEndpointsSelfHealFloorAfterRepoint|TestRefreshCtlEndpointsRejectLeavesCacheAndFloor' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestG3ClusterRosterPull|TestG3RosterPullDelegatesToManifestFn|TestManifest' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/auth -run 'TestG3RosterPullGrantedBothTemplates|TestD8bMemberAlertACLCarveOut|TestAgentCannotPublishAudit' -count=1`
- `git diff --check`
- `GOCACHE=/tmp/tether-gocache go test ./... -run '^$'`

Failing as intended:

- `GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP -count=1`

Note: embedded NATS tests could not start inside the managed sandbox; the NATS-dependent focused runs above were rerun outside the sandbox with approval.

---

## 主进程回复（外审后修复，2026-07-07）

外审结论 Fail 采纳。F1（真 bug）+ F2（doc）**全修**，外审新增探针已转 GREEN；no-poison / 门A / 改法二 primary / 改法一 self-heal 不变量全保持。

### F1 (Major) — stale NATS mask fresh HTTP + 10min throttle → **已修**
根因确认（外审诊断完全正确）：`refreshCtlEndpoints` 只在 NATS 返回 `nil` 时才 fallback HTTP；NATS 回一个"签名合法但 generation 未前进"的缓存 manifest（`manifestBytes()` 30s 缓存窗口）时被当成成功，跳过更新的 HTTP bootstrap manifest、写 `FetchedAt` 抑制刷新 10min。

**修复**（`cmd/tether/ctl_connect.go`）：改为把 **NATS + HTTP 两个 candidate 都过同一个 `AdoptDecision` monotone-gate**（累积、取更高 generation、rollback/foreign 仍拒）。触发 HTTP fallback 的条件从"NATS 返回 nil"改为"**NATS 未前进 roster 或 seed 任一 generation**"（`next.RosterGen <= prev.RosterGen || next.SeedGen <= prev.SeedGen`）——注意 roster/seed gen 独立前进，故一个 manifest 可能 bump seed 却不 bump roster（正是外审探针的场景：NATS gen1 让 seed 0→1 但 roster 1==1）。只有 NATS **两个 gen 都前进**（完全 fresh）才跳过 HTTP，保留改法二"不依赖 bootstrap"的性质；HTTP 死时 NATS 仍救场（改法二意义不变）。**no-poison 严格保持**：reject/no-progress 只推进 `FetchedAt`、绝不动 roster/seed/FloorURL。

**验证**：外审探针 `TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP` **转 GREEN**（RosterGen 收敛到 7）；`TestRefreshCtlEndpointsAdoptsOverNATSPrimary`（NATS-primary、bootstrap 不可达仍成功）、`...GateAHoldsOverLiveNATS`（门A）、`...RejectLeavesCacheAndFloor`（no-poison）、`...SelfHealFloorAfterRepoint`（改法一）均保持 PASS。

### F2 (Minor) — docs 未提 dead-peer-only 收敛例外 → **已修（doc）**
`docs/cluster.md` §5.6.9 补一条明确的残留 GAP：published seeds 恰好**只含已被弃 broker 端点**（survivor 从未 publish 进 seeds）时，host-match 无法与纯 VIP 集区分 → 不自动收敛（死端点滞留、活 survivor 缺席）；degraded-not-fatal（HTTP bootstrap / `broker_url` floor 兜底），恢复靠一次手动 `cluster seeds publish <survivor>` 或 `InviteSeeds`。并注明稳态 seeds 含所有 broker、此例不现实。与 Stage-C 内审 m7 裁决一致（该局限已由 `g3_seed_converge_test.go` 的 GAP 测试钉住），现补齐用户面 doc。

### Doubts / residual — 回应
- **sim drill**：同意 G3 触碰 lifecycle 语义但不改 install.sh/systemd/nats.conf 渲染/simcluster 脚本；核心已 hermetic 覆盖。真集群 grow 自动 publish / force-single 收缩 / ctl failover 刷新验证列为 **deploy-tier follow-up**（`docs/reviews/g3-review.md` 已记，plan §3 A/B/C/D 场景待 deploy-tier 门）。
- **broadcast first-reply-wins + cache window**：F1 修复正好缓解这一点——即便先收到一个 non-advancing 的 roster-pull 应答，只要它未推进 gen 就会继续咨询 HTTP bootstrap，不再被缓存窗口内的陈旧应答 mask。

### 提交前硬闸（外审修复后）
`make lint` = 0 · `make test` 全包绿 · `make e2e` 全矩阵绿 · ctl-refresh 面 `-race` 绿。（详见文末补充验证。）

### 补充验证（外审修复后硬闸详情）
- `make lint` = **0 issues**。
- `make test`（全包，嵌入式 nats）= 全绿。
- `cmd/tether` ctl-refresh + G3 面 `-race` = 绿。
- `make e2e`（全矩阵，串行）：一次 `TestD9Matrix/TwoBrokerJoinReplicates` FAIL（`phase->CATCHING_UP: leadership lost while committing log`）——**坐实为 pre-existing flake、非 G3 回归**：① G3 对 `clusteradmin.go` 仅加尾部 best-effort seed-helper 调用，未碰 `AddNode` 的 phase 转换逻辑，错误发生在 `setPhase(CATCHING_UP)`（seed-helper 之前）；② backstop 在该测试（不 publish seeds）下 change-gate no-op、不增 raft 负载；③ 单独 `go test -tags d9_integration ./test/d9 -run TwoBrokerJoinReplicates` 连跑 2 次均绿；④ 属 CLAUDE.md 记录的 N=1→2 grow raft leadership 时序脆弱类（同 #14）。
