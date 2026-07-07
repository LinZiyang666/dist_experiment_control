# Pass - G3 external review round 2

结论：Pass。开发者对上一轮外审的两个 finding 都做了针对性修复；我重新审查了未暂存修复 diff，并重新运行了上一轮失败探针和相关回归。没有发现新的阻塞问题。

## Tasklist / review surface

- [x] 重建 re-review 边界：当前暂存区仍是上一轮 G3 改动，开发者追加的未暂存修复集中在 `cmd/tether/ctl_connect.go`、`docs/cluster.md`、上一轮外审报告回复。
- [x] 复读开发者写在 `docs/reviews/g3-external-review.md` 的回复。
- [x] 复核 F1 修复：NATS/HTTP 两个 manifest candidate 均走 `AdoptDecision`，NATS 未推进任一 generation 时继续查 HTTP，rollback/foreign/no-poison 语义保持。
- [x] 复核 F2 文档：`docs/cluster.md` 现在明确 dead-peer-only seed set 是残留 GAP，并给出恢复路径。
- [x] 重新运行外审失败探针和 G3 聚焦回归。

## Findings

No blocking findings.

### F1 previous - stale NATS manifest masking fresh HTTP

Resolved. `refreshCtlEndpoints` now accumulates accepted candidates through the same monotone `AdoptDecision` state, first from live NATS, then from HTTP when the NATS candidate does not advance roster or seed generation. The external reviewer regression `TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP` now passes and proves the fresh HTTP manifest can override a non-advancing NATS response.

I specifically checked that the change still preserves the important safety properties:

- Unpinned ctl returns before consuming NATS/HTTP manifests.
- Foreign or rollback manifests still cannot mutate roster/seed/FloorURL.
- NATS-primary still works when bootstrap is unreachable.
- FloorURL self-heal remains on the accepted path.

### F2 previous - docs omitted dead-peer-only residual

Resolved. `docs/cluster.md` now documents that a seed set containing only departed broker endpoints is protected like a pure VIP/LB set and will not auto-converge, with manual `cluster seeds publish` / `InviteSeeds` recovery guidance.

## Doubts / residual risk

- I did not run simcluster. The re-review diff does not touch deployment rendering, systemd, install scripts, or simcluster tooling. A future deploy-tier G3 drill is still useful for grow seed auto-publish, force-single seed shrink, and ctl survivor refresh.
- The current fallback rule can consult HTTP more often when one generation advances and the other does not. That is a bounded latency/cost tradeoff, not a correctness bug, and it is preferable to masking a fresher manifest.

## Verification

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP|TestG3FetchManifestOverNATS|TestRefreshCtlEndpointsGateAHoldsOverLiveNATS|TestRefreshCtlEndpointsAdoptsOverNATSPrimary|TestRefreshCtlEndpointsNoHTTPTOFU|TestRefreshCtlEndpointsSelfHealFloorAfterRepoint|TestRefreshCtlEndpointsRejectLeavesCacheAndFloor' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestG3ClusterRosterPull|TestG3RosterPullDelegatesToManifestFn|TestManifest|TestDeriveSeedEndpoints|TestSeedSetEqual|TestSeedHostsMatchAnyBroker|TestG3Seed|TestForceSingleOnlineConvergesSeeds|TestG3RemoveGhostConvergesSeeds' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/auth ./internal/cluster ./internal/clusteroffline -run 'TestG3RosterPullGrantedBothTemplates|TestD8bMemberAlertACLCarveOut|TestAgentCannotPublishAudit|TestSeedEndpointsDropHosts|TestPruneRosterPeers' -count=1`
- `git diff --check`
- `GOCACHE=/tmp/tether-gocache go test ./... -run '^$'`
- `GOCACHE=/tmp/tether-gocache go test ./cmd/tether -count=1`

Note: NATS-dependent tests were run outside the managed sandbox with approval because embedded NATS cannot bind/start inside the sandbox.
