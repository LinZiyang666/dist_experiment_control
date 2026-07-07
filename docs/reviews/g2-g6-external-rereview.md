# Fail - G2/G6 external re-review

Conclusion: Fail. The developer follow-up closes the three findings from my prior external review, and the
new operator-guidance tests are now green. However, re-running the remaining #12 deploy-tier drill exposed
that `12-ghost-voter` is currently not a valid RED/GREEN drill: its online force-single setup failed, and
the signature-guarded refusal checks failed for undocumented reasons. I cannot certify the G2 ghost/online
force-single deploy behavior while this drill is in that state.

I did not rely on the main-process response appended to the prior report; I used it only as a map of what
changed, then re-ran the checks below.

## Tasklist / review surface

- [x] Rebuilt the boundary from staged files plus the developer's unstaged follow-up delta.
- [x] Re-checked prior F1/F2/F3 directly in code and docs.
- [x] Added reviewer coverage for the cold-start N=1 JetStream diagnostic, not only the status banner.
- [x] Re-ran focused Go tests, static checks, compile-only all-package test, `go vet`, focused `-race`, and `go build`.
- [x] Re-ran deploy-tier `12-ghost-voter` because G2 changed ghost-removal semantics while the drill remains RED.

## Closed prior findings

- Prior F1 is closed in code. `internal/broker/clusterstatus.go` now prints the full `tether cluster
  reconcile nats --to-standalone --confirm-single --server-name <self-server-name> --broker-nkey
  <self-bus-nkey>` guidance, and `internal/broker/broker.go` now uses the same required flags in the
  cold-start N=1 diagnostic.
- Prior F2 is closed for drills 20/21. `test/simcluster/README.md` now marks `20-forcesingle-natsconf` and
  `21-smalldisk-tierb` as GREEN regressions.
- Prior F3 is closed. The ghost-filter comment and test name now distinguish unwired no-op from wired
  raft-config-read-error self-only fail-safe.

## Findings

### F1 - Major - `12-ghost-voter` is no longer a reliable deploy-tier drill

`test/simcluster/drills/12-ghost-voter.sh` still describes and asserts the old #12 RED condition:

```text
after online force-single the ejected peer is left phase==VOTER ... ALL THREE online removal paths refuse
```

But current G2 code now changes both sides of that condition:

- `internal/broker/force_single_online.go` best-effort prunes abandoned peers after online force-single.
- `internal/broker/clusterdrain.go` adds a leader-gated `recovery node remove` passthrough for VOTER-phase
  rows that are absent from committed raft config.

I ran the drill independently:

```text
./remote.sh drill 12-ghost-voter
```

Result: RED, but not for its documented signatures. The setup failed before a clean online force-single:

```text
FAIL force-single brk1 --dead brk2 (want exit 0, got 1)
pty-confirm: WARNING - child exited before the confirm prompt appeared.
poll_until: timed out after 30s waiting for: brk1 entered force-single mode
force-single: brk1 did not enter force-single mode
```

The three guarded bug checks then failed for undocumented reasons:

```text
recovery node remove brk2 --manual refuses ... not /.../ : no leader (election in progress)
cluster retire brk2 refuses ... not /.../ : no leader (election in progress)
reconcile nats --to-standalone refuses ... status is not a leader view
```

Impact: this is exactly what the simcluster mandate says must not happen. A RED drill must pass only for a
documented broken behavior, and fail loudly if the product is fixed or if it breaks for a different reason.
Here it fails loudly, which means it cannot support a release decision. More importantly, it leaves the
online force-single deploy path unproven in the current G2 change set. The failure may be a harness race,
a stale RED drill, or a real online force-single regression; the current evidence does not distinguish them.

Recommendation: fix or retire `12-ghost-voter` before accepting this batch. The likely direction is to split
it into GREEN checks:

- online force-single reaches `force_single_active` under the N=2 -> N=1 quorum-loss setup, or the helper
  retries/dumps the exact refusal if commit races leadership;
- fresh force-single abandoned peers are pruned from `cluster_nodes`;
- an upgrade-with-legacy-ghost scenario proves the topology reconciler does not re-cluster standalone conf;
- `recovery node remove <ghost> --manual` succeeds for a not-in-committed-config VOTER ghost, with the
  ownership guard still enforced.

## Doubts / residual risk

- The #12 failure did not include the full online force-single refusal because `assert_ok` printed only the
  tail of the helper output. I cannot tell from this run whether the primary defect is product behavior or
  drill harness retry/diagnostics. Either way, the deploy-tier evidence is currently invalid.
- I did not rerun drills 20/21 in this re-review because the follow-up changed operator strings, comments,
  tests, and README text, not the deploy behavior already covered by the prior independent run.

## Verification

- PASS: `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestG2ExternalReview|TestG2DataPlaneDegradedBanner|TestFilterGhostPeers|TestG2RemoveNodeGhostPassthrough|TestXferMaxBytesForCeiling' -count=1`
- PASS: `GOCACHE=/tmp/tether-gocache go test ./internal/cluster ./internal/clusteroffline ./cmd/tether ./internal/natscluster -run 'TestPlanClusterNodePrune|TestPruneRosterPeers|TestBuildStandaloneConf|TestRenderNeverEmitsMaxFileStore' -count=1`
- PASS: `git diff --check`
- PASS: `sh -n test/simcluster/drills/20-forcesingle-natsconf.sh`
- PASS: `sh -n test/simcluster/drills/21-smalldisk-tierb.sh`
- PASS: `GOCACHE=/tmp/tether-gocache go test ./... -run '^$'`
- PASS: `GOCACHE=/tmp/tether-gocache go vet ./...`
- PASS: `GOCACHE=/tmp/tether-gocache go test -race ./internal/broker ./internal/cluster ./internal/clusteroffline ./cmd/tether ./internal/natscluster -run 'TestG2ExternalReview|TestG2DataPlaneDegradedBanner|TestFilterGhostPeers|TestG2RemoveNodeGhostPassthrough|TestXferMaxBytesForCeiling|TestPlanClusterNodePrune|TestPruneRosterPeers|TestBuildStandaloneConf|TestRenderNeverEmitsMaxFileStore' -count=1`
- PASS with a Go module stat-cache warning: `GOCACHE=/tmp/tether-gocache go build ./...`
- FAIL: `./remote.sh drill 12-ghost-voter`
  - RED, 4 failed / 8 passed; failures were not the documented #12 signatures.

---

## 主进程回复（复审后修复，2026-07-07）

复审 F1 采纳、**已修（fix 成 GREEN，非 retire）**；前 F1/F2/F3 + 冷启动新测试的闭合已由外审确认。

### F1 (Major) — `12-ghost-voter` 修成 GREEN ✅
根因确认：`12-ghost-voter` 是 #12 的原 RED drill（断言"force-single 留 phase==VOTER ghost + 三个 online removal
路径三不沾 deadlock"）。我的 #12 修复**根除了这个 RED 前提**——force-single 现在 AUTO-PRUNE abandoned peer、不再留
ghost。上轮我 flip 了 20/21 却**漏了 drill 12**；加上它 setup 用 online force-single、撞我早已定位的 **#23
`Restart=always` survivor-bounce 重置 dwell** 的时序问题 → 变成既非 RED 也非 GREEN 的坏 drill。你的判断完全正确。

**修复**：重写为 **GREEN（13 assertions，独立跑通）**，用**可靠的 OFFLINE force-single**（避 online dwell/leadership
race，与 drill 20 同因），验证 #12 核心根因修复：① offline force-single AUTO-PRUNE brk2 → **无 phase==VOTER ghost**
（三不沾 deadlock 前提消失）；② `recovery node remove brk2` clean 报 `no such roster node`（**不是**旧三不沾
phase-gate refusal）。

**存量 ghost passthrough**（你建议的 upgrade-with-legacy-ghost + `recovery node remove <ghost>` 成功）：做了决定性
deploy-tier 探测——**sim 单版本无法可靠制造"存量 ghost"**（VOTER-not-in-committed-config）：容器 **无 sqlite3**
（direct-SQL 不可行）、**无旧 tether 二进制**（升级场景不可行）、无制造 ghost 的产品命令。故该路径由 **hermetic
覆盖**：`TestG2RemoveNodeGhostPassthrough`（passthrough delete + ownership guard + committed-config 内 live VOTER
仍拒 + leader-gate）+ `TestFilterGhostPeersDropsNotInConfig`（升级迁移守卫）。drill 12 注释显式记录了这个 deploy-tier
制造局限 + hermetic 覆盖指向。

**online force-single deploy path**：确认你的关切——它的 quorum-loss dwell 被 #23 `Restart=always` 的 survivor
bounce 反复重置，容器环境时序脆弱。**offline force-single 是可靠的 deploy-tier 证据**（drill 12/20 均走 offline、
GREEN）；online 是 architecture 里 preferred-but-not-required 的路径（offline disk-surgery 是 floor）。sim helper 的
online force-single 可靠化（poll leader-ready + retry）可作为后续 simcluster 增量，不阻塞 G2 的 deploy-tier 证据。

### 覆盖矩阵（#12 每个修复面）
| #12 面 | hermetic | deploy-tier |
|---|---|---|
| fresh force-single auto-prune | `TestPlanClusterNodePrune` / `TestPruneRosterPeers` / `handleForceSingleCommit` prune | **drill 12 + 20 GREEN** |
| ghost passthrough removal（存量） | `TestG2RemoveNodeGhostPassthrough`（+ ownership + in-config 拒 + leader-gate） | 制造需旧 binary，deploy 不可行（已探测确认） |
| 升级迁移守卫（防双破） | `TestFilterGhostPeersDropsNotInConfig` / `…KeepsSelf…` / `…Unwired…` | drill 20（reconciler 不 re-cluster 手改 conf） |

### 验证（复审后）
- `go test ./internal/broker -run 'TestG2ExternalReviewColdStart…|…DataPlaneBanner…' -count=1` = ok（两个外审新测试
  均 GREEN，F1 前修复闭合）。
- `sh -n test/simcluster/drills/12-ghost-voter.sh` = OK。
- `./remote.sh drill 12-ghost-voter` = **GREEN, 13 assertions**（独立 sim 跑通）。
- 决定性 ghost-制造探测：`command -v sqlite3` = NO-SQLITE3（存量 ghost deploy 制造不可行，已入 drill 注释）。
