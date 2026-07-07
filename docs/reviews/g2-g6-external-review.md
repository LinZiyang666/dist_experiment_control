# Fail - G2/G6 external review

Conclusion: Fail. The G2/G6 implementation is largely coherent, and both relevant deploy-tier simcluster
drills passed in my independent run. However, the persistent operator recovery guidance still contains an
incomplete `reconcile nats --to-standalone` command in two high-visibility paths. An operator following
that guidance after online force-single can remain stuck with JetStream returning 503.

I reviewed the unstaged tracked changes plus untracked tests/docs as an external reviewer. The staged
baseline was empty when I started. I used the internal G2/G6 plan and review only as context, not as
authority.

## Tasklist / review surface

- [x] Rebuilt the boundary from `git status`, tracked diffs, untracked files, and the staged baseline.
- [x] Re-read `CLAUDE.md`, cluster architecture/runbook docs, simcluster mandate, and prior review style.
- [x] Reviewed online/offline force-single roster pruning, generation bumps, idempotency, and convergence.
- [x] Reviewed ghost voter removal, topology ghost filtering, and render safety.
- [x] Reviewed offline de-cluster rendering, identity selection, validation/swap path, and operator output.
- [x] Reviewed DATA-PLANE-DEGRADED status and cold-start diagnostics.
- [x] Reviewed G6 OBJ_xfer capacity sizing, transfer admission, and nats.conf preflight.
- [x] Reviewed changed simcluster drills and docs against RED/GREEN semantics.
- [x] Added an independent reviewer regression test for the high-risk operator-guidance path.
- [x] Ran focused Go tests, static checks, compile-only all-package test, and simcluster drills 20/21.

## Findings

### F1 - Major - `status` and cold-start recovery guidance omit required standalone flags

`internal/broker/clusterstatus.go:335` appends this persistent force-single data-plane banner:

```text
cluster reconcile nats --to-standalone --confirm-single
```

`internal/broker/broker.go:897-901` repeats the same incomplete command in the N=1 JetStream-unavailable
cold-start diagnostic.

That command is not the command required by the current implementation for a still-clustered multi-broker
conf. `cmd/tether/cluster_natsconf.go:181-185` refuses without `--server-name` and the broker nkey when
the existing auth_callout cannot unambiguously identify the lone broker. The online force-single success
path already prints the correct command in `cmd/tether/cluster_offline.go:287-292`, and the runbook does
the same in `docs/cluster-runbook.md:383-387`:

```text
tether cluster reconcile nats --to-standalone --confirm-single --server-name <self-server-name> --broker-nkey <self-bus-nkey>
```

Impact: `cluster status` is the long-lived prompt an operator sees until the data plane is repaired. If
they copy the banner after online force-single, the command can refuse with missing identity/nkey, leaving
file transfers, history, and audit on JetStream 503. The cold-start diagnostic has the same problem for a
crash-looping N=1 node.

Independent test added: `internal/broker/g2_external_review_test.go` pins that the DATA-PLANE-DEGRADED
banner must name `--server-name` and `--broker-nkey`. It currently fails with the existing banner.

Recommendation: update both the status banner and cold-start diagnostic to print the full command, including
`tether`, `--server-name <self-server-name>`, and `--broker-nkey <self-bus-nkey>`, with the same short nkey
derivation hint used by the online force-single completion text. Keep the new regression, or fold the same
assertion into `g2_banner_test.go`.

### F2 - Minor - simcluster README still lists fixed drills 20/21 as RED backlog drills

`test/simcluster/drills/20-forcesingle-natsconf.sh` and `21-smalldisk-tierb.sh` are now GREEN regression
drills, and both passed independently. But `test/simcluster/README.md:207-208` still says:

```text
20-forcesingle-natsconf | RED (#20)
21-smalldisk-tierb     | RED (#21)
```

Impact: this does not break product behavior, but it violates the simcluster documentation contract and can
mislead future reviewers about whether those defects are still expected-open backlog items.

Recommendation: update the drills table to match the scripts' GREEN regression semantics.

### F3 - Minor - ghost-filter comments/tests still say fail-open while the error path is fail-safe self-only

`internal/broker/topology_reconcile.go:158-174` has an outer comment that says unreadable raft config returns
peers unchanged, but the implementation now logs and returns only self on `RaftConfiguration()` error. The
test name/comment in `internal/broker/g2_ghostfilter_test.go:79-82` also uses fail-open language, though it
only covers the nil/unwired early-return path, not a real raft-config read error.

Impact: I agree with the self-only fail-safe direction for the G2 double-break, and I did not find a runtime
bug here. The risk is maintainability: a future change could "fix" the implementation back to the stale
comment.

Recommendation: update the comments/test name to distinguish "unwired returns unchanged" from
"config-read error returns self-only", and add an explicit config-error test if a small fake node hook is
available.

## Doubts / residual risk

- Online force-single intentionally treats abandoned-roster pruning as best-effort. The code logs a warning
  and suggests `cluster recovery node remove <ghost>` if the prune proposal fails. I did not reproduce that
  failure mode in simcluster; the happy path was covered by drill 20.
- Drill 21 proves the small-disk tier-B round-trip and absence of forbidden `max_file_store`, but it does
  not directly inspect the rendered OBJ_xfer bucket `MaxBytes`. The unit tests cover the sizing function;
  a future deploy drill assertion on the actual stream config would tighten the end-to-end proof.

## Verification

- PASS: `GOCACHE=/tmp/tether-gocache go test ./internal/cluster ./internal/clusteroffline ./cmd/tether ./internal/natscluster -run 'TestPlanClusterNodePrune|TestPruneRosterPeers|TestBuildStandaloneConf|TestRenderNeverEmitsMaxFileStore' -count=1`
- PASS: `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestG2RemoveNodeGhostPassthrough|TestFilterGhostPeersDropsNotInConfig|TestFilterGhostPeersKeepsSelfEvenIfNotInConfig|TestFilterGhostPeersFailOpen|TestG2DataPlaneDegradedBanner|TestXferMaxBytesForCeiling' -count=1`
- FAIL as intended: `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run TestG2ExternalReviewDataPlaneBannerNamesRequiredStandaloneFlags -count=1`
  - Failure: banner does not include `--server-name` / `--broker-nkey`.
- PASS: `git diff --check`
- PASS: `sh -n test/simcluster/drills/20-forcesingle-natsconf.sh`
- PASS: `sh -n test/simcluster/drills/21-smalldisk-tierb.sh`
- PASS: `GOCACHE=/tmp/tether-gocache go test ./... -run '^$'`
- PASS: `./remote.sh --build drill 20-forcesingle-natsconf`
  - GREEN, 14 assertions.
- PASS: `./remote.sh drill 21-smalldisk-tierb`
  - GREEN, 8 assertions.

---

## 主进程回复（外审后修复，2026-07-07）

外审 3 个 finding 全部采纳并已修；`go build ./...` + `go vet` = 0，外审者的回归测试转 GREEN，touched 包 `-race` 全绿。

### F1 (Major) — 采纳，已修 ✅
根因确认：修内审 MAJOR#2（to-standalone 命令缺 `--server-name`/`--broker-nkey`）时只补了 online 指引
（`cmd/tether/cluster_offline.go:287`）+ runbook §2.2/§3.2，**漏了同一命令的另两处**——`clusterstatus.go:335` 的
DATA-PLANE-DEGRADED banner + `broker.go:897-901` 的 N=1 冷启动诊断。两处均补全为
`tether cluster reconcile nats --to-standalone --confirm-single --server-name <self-server-name> --broker-nkey
<self-bus-nkey>` + 与 online 指引一致的来源提示（server-name = conf 的 server_name；broker-nkey = broker.nk seed /
cluster_nodes.bus_nkey_pub，**不在 broker.yaml**）。**外审者新增的
`TestG2ExternalReviewDataPlaneBannerNamesRequiredStandaloneFlags` 予以保留、现已 GREEN**。（注：`broker.go` 我加的
voters>=2 差分诊断分支指向 rejoin runbook、不含 to-standalone，无此问题。）

### F2 (Minor) — 采纳，已修 ✅
`test/simcluster/README.md` 的 drill 表把 `20-forcesingle-natsconf` / `21-smalldisk-tierb` 从 `RED` 更新为 GREEN
regression（含各自 14 / 8 assertions 与修复摘要）。（`12-ghost-voter` 仍标 RED——本轮 flip 范围只含 20/21；#12 的
ghost-removal 已被 hermetic `TestG2RemoveNodeGhostPassthrough` + drill 20 的 prune 断言覆盖，drill 12 脚本 flip
留后续叶子增量。）

### F3 (Minor) — 采纳，已修 ✅
`topology_reconcile.go:154` 的 `filterGhostPeers` docstring 重写为区分两个 distinct fallback：(1) 未 wired
（nil cl/node）→ peers UNCHANGED（无 raft、无 ghost 风险）；(2) wired 节点的 raft-config READ ERROR → SELF-ONLY
（fail-SAFE，非 fail-open，防 #12 双破）。测试 `TestFilterGhostPeersFailOpen` 改名
`TestFilterGhostPeersUnwiredReturnsUnchanged` + doc-comment 明确它只覆盖 unwired 路径；config-error self-only 路径
需 fake-node hook（harness 缺，真 node 的 `RaftConfiguration()` 不 error），靠推理 + downstream Render zero-routes
fail-closed 验证（记录为后续可加测试，需 node interface 重构）。

### Doubts / residual risk — 确认
- online prune best-effort 失败：发现路径已由闭合核验验证——status `Inconsistent=true`(phaseVoter && !inCfg) +
  DATA-PLANE-DEGRADED banner + operator `recovery node remove <ghost>`(leader-gated passthrough,已测)。
- drill 21 不直接 inspect OBJ_xfer `MaxBytes`：单元 `TestXferMaxBytesForCeiling` 覆盖 sizing 硬不变量；同意后续
  deploy drill 加一条 jsz `max_bytes` 断言收紧端到端证明（记录为后续）。
