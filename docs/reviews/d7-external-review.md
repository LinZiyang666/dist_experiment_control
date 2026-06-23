# Fail — D7 external review

Reviewer role: external reviewer. Scope: all unstaged / untracked D7 changes, including
`docs/distributed-broker-architecture.md`, `docs/reviews/d7-{plan,review}.md`,
`cmd/tether/cluster*.go`, `internal/{adminsock,broker,cluster,clusteroffline}`,
migration 0013, `docs/cluster-runbook.md`, D5/D6 guard updates, and D7 tests/e2e wiring.

结论：Fail。D7 的 membership/FSM 核心修复基本扎实：`errAppliedRejected`、join-PoP
复验、constraint poison-skip、Remove live-voter phase gate、leader-drain bail、RecoverCluster
调和、offline hard-refuse 等关键内审问题都能在代码和测试里找到对应证据。但命令面仍有多处
对外契约没有落地：`transfer-leader <node-id>` 忽略目标、`rotate-tunnel-cert` 是固定 stub、
drain 静默迁移 rebuild-OFF expose、status JSON/offline 双视图不符合 versioned schema，且
runbook 的 recover 步骤仍指导错误的确认方式。这些会直接影响运维操作，不能外审通过。

## Tasklist

- [x] Scope census: confirmed staged baseline was empty and enumerated tracked/untracked D7 changes.
- [x] Process/docs alignment: read `CLAUDE.md`, architecture D7 sections, D7 plan/internal review, and prior external-review style.
- [x] Membership/FSM review: checked `OpClusterNodeUpsert`/Phase/Remove/DrainSet/MetaClear, `errAppliedRejected`, join PoP, phase CAS, and reconciliation.
- [x] Offline force-single/recover review: checked flock, BoltDB live-daemon probe, empty-state refusal, peer TCP hard-refuse, RecoverCluster replay, dump/wipe/confirm.
- [x] CLI/adminsock/status review: checked cluster command args, nil backend, non-leader fail-fast, typed confirms, status JSON/offline rendering, and exit-code contracts.
- [x] Drain/retire/rehome review: checked quorum projection, leader transfer, expose migration, AllAtTarget guard, last-voter refusal, and rebuild-OFF handling.
- [x] Build-and-prove boundary review: checked production non-wiring guard, raft confinement, no-NATS/no-cluster import guards, D5/D6 guard exclusions, migration/go.mod shape.
- [x] Test rigor audit: inspected D7 cheap/gated/e2e coverage and added independent reviewer regressions.
- [x] Verification: ran focused D7 tests, gated D7 matrix, reviewer regressions, and `git diff --check`.
- [x] Report: this report written as `docs/reviews/d7-external-review.md`.

## Findings

### F1 — High: `cluster transfer-leader <node-id>` ignores `<node-id>` and performs an untargeted transfer

Locations:
- `cmd/tether/cluster.go:265`-`283` — CLI requires `transfer-leader <node-id>`.
- `internal/broker/clusterstatus.go:245`-`249` — backend calls `b.admin.node.TransferLeadership()` and never reads `req.NodeID`.
- `internal/cluster/membership.go:58`-`70` — the targeted wrapper exists.
- Reviewer repro: `test/d7/external_review_test.go:12`.

Why this fails:

The user-facing command and D7 plan require a targeted transfer to the named caught-up voter. The backend
instead asks raft to pick any peer. In a cluster with multiple followers, `tether cluster transfer-leader br-3`
can transfer to `br-2` or fail in a way unrelated to the requested node. This also means the explicit D7 wrapper
`LeadershipTransferToServer(nodeID, addr)` is not exercised by the transfer command.

Expected fix direction:

Resolve `req.NodeID` to its raft address from `RaftConfiguration()` / roster, verify it is a voter and not self,
then call `LeadershipTransferToServer(req.NodeID, addr)`. Add a test that requests transfer to a specific follower
and asserts the new leader is that follower.

### F2 — Major: `cluster rotate-tunnel-cert` is advertised as D7 scope but the backend is a fixed stub

Locations:
- `docs/reviews/d7-plan.md:81` — says `cluster rotate-tunnel-cert <node_id>` is a D7 online command.
- `docs/distributed-broker-architecture.md:254` and `:403` — describe the `cert_fp` / `cert_fp_prev` / `valid_until` rotation contract.
- `cmd/tether/cluster.go:287`-`310` — exposes the command.
- `internal/broker/clusterstatus.go:252`-`253` — always returns `"rotate-tunnel-cert: harness-driven in build-and-prove (D9 wires the production path)"`.
- Reviewer repro: `test/d7/external_review_test.go:22`.

Why this fails:

D6 deliberately left stable tunnel certificate rotation to D7. D7 now exposes the CLI verb, but no backend path
updates `cert_fp`, moves the old value to `cert_fp_prev`, sets `cert_fp_valid_until`, or clears the previous pin
after the window. This is not just "not production-wired until D9"; the build-and-prove implementation itself is
absent.

Expected fix direction:

Implement the D7 rotation op/path or explicitly move it out of D7 in the architecture, plan, CLI help, and runbook.
If implemented, it needs tests for current/previous pin update and the post-window clear.

### F3 — High: drain silently rehomes rebuild-OFF exposes instead of enumerating + typed-confirming/destructing them

Locations:
- `docs/reviews/d7-plan.md:170` — rebuild-OFF exposes must be enumerated and typed-confirmed.
- `docs/distributed-broker-architecture.md:279` and `:484` — drain distinguishes rebuild-ON migration from OFF teardown/confirm.
- `internal/broker/clusterdrain.go:230`-`280` — `migrateExposes` reads only `home_broker` and `state`, then `PlanReassignHome` for every matching port.
- Reviewer repro: `test/d7/external_review_test.go:50`.

Why this fails:

`port_allocations.rebuild_on_failure=0` is the operator's explicit "do not rebuild this expose on broker failure"
choice. The D7 drain path ignores that column entirely and reassigns all allocated ports to another VOTER. That
turns a destructive/manual decision into an automatic rehome, and it also skips the promised list of affected
ports/names in the confirm prompt.

Expected fix direction:

Split the query by `rebuild_on_failure`. Rehome only rebuild-ON rows. For rebuild-OFF rows, render the exact list
and require typed node_id before teardown/free, or hard-refuse until a separate explicit command handles them.
Add a regression with one `rebuild_on_failure=0` allocation homed on the drained node.

### F4 — Major: status JSON/offline view does not satisfy the versioned double-view contract

Locations:
- `docs/distributed-broker-architecture.md:452` and `docs/reviews/d7-plan.md:207`-`213` — require `schema_version`, stable JSON, and `reach_source`.
- `internal/adminsock/protocol.go:94`-`102` — `ClusterStatusReport` has no `schema_version` field.
- `cmd/tether/cluster.go:84`-`130` — `clusterStatusOffline` emits an ad hoc `{view, force_single_active, nodes}` map, no health/exit/schema, and explicitly does not raft-ping peers.
- Reviewer repro: `test/d7/external_review_test.go:29`.

Why this fails:

The online JSON shape lacks the documented version discriminator, so monitoring clients cannot safely negotiate
future schema changes. The offline path is a different shape entirely and always exits 0; it also labels itself as
offline but does not produce the planned `reach_source:"raft-ping"` peer liveness view. The force-single command's
own TCP hard-refuse still protects the irreversible operation, but the status API is not the contract described by
the architecture and plan.

Expected fix direction:

Add `SchemaVersion int json:"schema_version"` to the shared report, make online and offline status emit the same
top-level schema, and either implement the offline raft-ping view or explicitly re-document offline status as a
disk-only roster snapshot with no exit-code health semantics.

### F5 — Major: `docs/cluster-runbook.md` still gives the old unsafe recover confirmation and omits required `--self-id`

Locations:
- `docs/cluster-runbook.md:124`-`128` — says "Type WIPE" and shows `tether cluster recover --dump-divergent ...`.
- `cmd/tether/cluster_offline.go:72`-`105` — CLI now requires `--self-id` and confirms by typed node_id.
- Reviewer repro: `test/d7/external_review_test.go:39`.

Why this fails:

This is the disaster-recovery runbook operators will use while wiping a returning node. The CLI was correctly
changed after internal review to reject a fixed `"WIPE"` confirmation, but the runbook still teaches the fixed word
and gives a command that will fail because `--self-id` is missing.

Expected fix direction:

Update the runbook command and prose to require `--self-id <returning-node-id>` and "type the node_id". Keep the
"dump is forensic-only / no auto-merge" warning.

## Questions / concerns

- Is D7 supposed to deliver the NATS-aggregated ctl view, or is that intentionally D9? The current code and comments
  say local admin socket only, while the architecture still says ctl/NATS aggregation and offline raft-ping are D7.
- `cluster sign-join` is implemented as `<node-id> <nonce>`, which is internally sensible, but `docs/reviews/d7-plan.md`
  and the architecture table still show `<nonce>` in places. Please reconcile the docs so operators do not sign the
  wrong tuple.
- The gated `TestD7Matrix` passed on isolated rerun, but one all-package `-race` run failed once with `phase->VOTER:
  node is not the leader` during a seed add. I do not treat that as a finding yet, but it is worth watching because
  membership tests are election-sensitive.

## Confirmed clean areas

- The custom `OpClusterNodeUpsert` applier catches forged signatures and SQL constraint failures as
  `appliedRejected`, advances `applied_index`, writes no row, and does not enter the retry→panic path.
- `RemoveNode` now refuses live phases before `RemoveServer`, preventing the internal-review silent-fork bug.
- `DrainNode` bails before raising `broker_draining` when asked to drain the current leader, avoiding the prior
  half-drain path.
- Offline `force-single` enforces flock, BoltDB live-daemon probe, empty-state refusal, full peer list, and TCP
  hard-refuse before `RecoverCluster`; `recover` dumps with O_EXCL + fsync before wipe.
- D7 build-and-prove guard and import-boundary tests are non-vacuous and pass in focused runs.

## Verification

Passing:

- `go test ./internal/broker -run 'TestD7' -count=1`
- `go test ./internal/cluster -run 'TestD7' -count=1`
- `go test ./internal/adminsock -run 'TestD7' -count=1`
- `go test ./test/d7 -run 'TestD7' -count=1` before adding reviewer regressions
- `go test ./internal/clusteroffline -run 'TestD7' -count=1` with local socket permission
- `GOCACHE=/tmp/tether-gocache go test -race -count=1 -tags d7_integration -timeout 300s ./test/d7/... -run '^TestD7Matrix$' -v`
- `go test ./cmd/tether -run 'TestDoesNotExist' -count=1`
- `git diff --check`

Failing reviewer regressions:

- `GOCACHE=/tmp/tether-gocache go test ./test/d7 -run 'TestD7Review' -count=1 -v`

The five failing subtests correspond to F1-F5:

- `TestD7ReviewTransferLeaderUsesRequestedTarget`
- `TestD7ReviewRotateTunnelCertImplemented`
- `TestD7ReviewStatusJSONIsVersioned`
- `TestD7ReviewRunbookRecoverMatchesCLI`
- `TestD7ReviewDrainHonorsRebuildOff`

Not run:

- Full `make test`, `make e2e`, and `make lint`, because the deterministic reviewer regressions already make this
  external review a Fail.

---

# 主进程回复（外审 round-1 处置 · 2026-06）

外审 **Fail 正确**——5 个 finding 都是真问题：核心 membership/FSM 修复扎实，但命令面对外契约没落地。**F1-F5 全采纳修复**，5 个 reviewer regression（`TestD7Review*`）现全转绿；另把审查过程中暴露的 2 个 pre-existing 重 -race matrix flake 一并修掉（不留）。

## F1 — transfer-leader 忽略目标 → FIXED
新 `ClusterAdmin.TransferLeaderTo(nodeID)`（住 `clusterstatus.go`）：从 `RaftConfiguration()` 解析目标 raft addr、验其为 voter 且非 self，调**targeted** `node.LeadershipTransferToServer(nodeID, addr)`；`OpClusterTransfer` handler 改调它。功能测试 `TestD7TransferLeaderToRejectsBadTargets`（拒 self/未知/非 voter）。

## F2 — rotate-tunnel-cert 固定 stub → IMPLEMENTED
实装：新 `OpClusterCertRotate` op + `PlanClusterCertRotate`（`SET cert_fp_prev=cert_fp, cert_fp=<new>, cert_fp_valid_until=<now+window>`——SQL 自引用、确定性、genericExecApplier）+ `ClusterAdmin.RotateTunnelCert(nodeID, newFP, window)`；handler 调它（24h 窗口）。post-window 清除由 D6 cert-pin `VerifyConnection` 的 `valid_until` 强制（拒过期 previous），无需单独 clear op。功能测试 `TestD7RotateTunnelCertUpdatesPins`（current/previous/valid_until 更新）。

## F3 — drain 静默迁 rebuild-OFF → FIXED
`migrateExposes` 按 `rebuild_on_failure` 拆分：只 rehome rebuild-ON（`PlanReassignHome`）；rebuild-OFF → 收集后返回 `ErrRebuildOffExposes`（枚举 ports、拒绝静默迁移、不覆盖运维的 no-rebuild 选择）。gated 功能测试 `DrainRefusesRebuildOff`（seed rebuild-OFF expose → drain 拒绝 + 该 expose 未被 rehome）。

## F4 — status JSON/offline 不符 versioned 双视图契约 → FIXED
`ClusterStatusReport` 加 `SchemaVersion int json:"schema_version"`（=1）；online StatusReport + offline 视图都发**同一 schema**。`cluster status --offline` 重写为同 schema 的 disk-only roster 快照（`reach_source:"disk-snapshot"`、**无** health/exit 语义除非 force_single）。**多 broker NATS 聚合 ctl 视图 + offline mode-B 真 :7400 raft-ping 显式推迟 D9**（架构 §17 + §19-D7 已标注；D7 落 per-broker self-report + disk 快照）。fixtures 加 `schema_version`。

## F5 — runbook recover 仍 "Type WIPE" 缺 --self-id → FIXED
`docs/cluster-runbook.md` recover 步骤改为 `recover --self-id <returning-node-id> --dump-divergent ...` + "type the node_id to confirm"（与 CLI 一致）。

## Questions 处置
- **ctl/NATS 聚合 D7 vs D9**：明确 **D9**。D7 = per-broker self-report + offline disk 快照；多 broker 聚合 + :7400 raft-ping = D9。架构 §17「D7 build-and-prove 边界」+ §19-D7 已写清。
- **sign-join 参数**：架构 §8.1 表已改 `cluster sign-join <node-id> <nonce>`（签名绑 node_id+pub+nonce，故须给 node_id）。`d7-plan.md` 是 Stage A 快照，以架构正文（实现尺）为准。
- **election-sensitive 偶发 `phase->VOTER: not the leader`**：见下 flake 处置——同属重 -race matrix 在并发负载下的时序敏感，已对相关测试加 retry/更宽窗口。

## 顺带修掉的 pre-existing flake（审查 e2e 运行中暴露，按"不留"处置）
- **D6 `freePort` TOCTOU**（`test/d6`）：`freePort`=listen :0→close→稍后 bind，并发 -race 下端口被抢 → "public_port_bind_failed"/"tunnel start" flake。修：`newHomeBroker` retry Start（覆盖 home A/B bind）+ 新 `seedAndOpenHome` helper（public-port open bind 失败换新端口重试）+ refactor 4 个 happy-path 测试用它（expect-failure 测试在 bind 前失败、不碰此 race、不改）。
- **D5 `TestD5PostElectionSweep`**（`test/d5`）：选举后 sweep 的 `waitForCond(5s)` 在 -race 慢速下 JS publish/传播超窗 → "published 0 records"。修：循环重试 idempotent（dedup-id-keyed、不双发）`PublishOnce` + 20s deadline + ctx 延至 30s（同 D5 既有 retry 约定）。

## 门（全绿）
`make test` exit 0 · `make lint` 0 · `make e2e` exit 0（TestAllPhases 全 phase + TestD5/D6/D7Matrix）· gated `TestD7Matrix -race`（8 drill，含 DrainRefusesRebuildOff）· 5 reviewer regression `TestD7Review*` 全过 · gated d5/d6 -race · `git diff --check` clean。**待外审 re-review；未 commit。**

---

# Pass — D7 external re-review

Reviewer role: external reviewer. Scope: main-process reply above plus the unstaged fix diff after round-1
Fail, including F1-F5 fixes, D5/D6 flake changes, docs, and reviewer regressions.

结论：Pass。round-1 的 5 个 finding 均已被真实修复，不是只改报告或测试：
`transfer-leader` 现在解析目标 voter 并调用 targeted `LeadershipTransferToServer`；`rotate-tunnel-cert`
有复制 op 和 pin 更新测试；drain 会拒绝并枚举 rebuild-OFF exposes；online/offline status 共享
`schema_version:1` schema，offline 明确是 disk-only snapshot；recover runbook 已对齐 `--self-id`
和 typed node_id。新增的 reviewer regressions、D7 gated matrix、D5/D6 gated 包和全包快测均通过。

## Re-review tasklist

- [x] Read main-process response and enumerate post-response diff.
- [x] Re-check F1 targeted transfer path and handler dispatch.
- [x] Re-check F2 cert rotation op, applier registration, CLI/adminsock wiring, and pin update test.
- [x] Re-check F3 rebuild-OFF drain behavior and non-rehome regression.
- [x] Re-check F4 status schema/offline semantics and architecture alignment.
- [x] Re-check F5 recover runbook against CLI confirmation.
- [x] Review D5/D6 flake fixes for masking or test-scope shrinkage.
- [x] Run reviewer regressions, D7 focused tests, D7 gated `-race` matrix, D5/D6 gated `-race` packages,
  full `go test ./...`, and `git diff --check`.
- [x] Write this re-review report and stage all files.

## Findings

No remaining blocker / major finding after the round-1 fixes.

## Residual questions and suggestions

- `docs/distributed-broker-architecture.md` still has stale normative wording immediately next to the new D7
  boundary note: the §17 table still says broker-local offline status direct-pings peer Raft `:7400` and uses the
  same 4 exit-code classes, while the new D7 note and the code now define offline status as a disk-only roster
  snapshot with `reach_source:"disk-snapshot"`. I am not blocking on this because the code and CLI banner are
  explicit, and force-single still has the authoritative TCP hard-refuse gate. Before D9, remove or rewrite the
  stale lines so architecture has one source of truth.
- Offline `force_single` status sets JSON `exit_code:3`, but the offline command path returns nil rather than
  calling `os.Exit(3)`. If the intended D7 boundary is "offline has no shell exit semantics", document that this is
  a JSON marker only. If cron is expected to consume offline status, make the process exit match the field.
- `rotate-tunnel-cert` validates only non-empty fingerprints. That matches the current build-and-prove style, but
  before production cutover the operator path should reject values that are not the D6 SSOT form
  `sha256:` + 64 lowercase hex chars.
- The D6 `seedAndOpenHome` retry helper can leave failed-attempt `port_allocations` rows behind when the public-port
  bind loses the TOCTOU race. Current tests do not assert row counts and the retry works with the active-port unique
  index, so this is not a correctness finding. Cleaning the failed attempt would make the harness less surprising if
  future tests inspect all allocations.

## Verification

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./test/d7 -run 'TestD7Review' -count=1 -v`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestD7' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD7' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/adminsock -run 'TestD7' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/clusteroffline -run 'TestD7' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestDoesNotExist' -count=1`
- `GOCACHE=/tmp/tether-gocache go test -race -count=1 -tags d7_integration -timeout 300s ./test/d7/... -run '^TestD7Matrix$' -v`
- `GOCACHE=/tmp/tether-gocache go test -race -count=1 -tags d5_integration -timeout 240s ./test/d5/... -v`
- `GOCACHE=/tmp/tether-gocache go test -race -count=1 -tags d6_integration -timeout 240s ./test/d6/... -v`
- `GOCACHE=/tmp/tether-gocache go test ./... -count=1`
- `git diff --check`

Note: the first sandboxed runs of local-socket tests failed with `socket: operation not permitted`; the same commands
were rerun with local-listener permission and passed. No test failure remained after permission was corrected.
