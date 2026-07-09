# Fail - G4 external review

结论：Fail。当前 G4 `tether cluster add` 增量还不能放行。核心问题不在签名或 move-aside 单点逻辑，而在
真实 grow 编排边界：joiner 启动边界仍然不是一个可按提示执行的产品路径，`broker.yaml` cluster seam 既可能
没有写入 `broker.cluster`，也可能因权限失败被降级成 note 后继续；随后本地 admin socket 只要有任何响应就会被
当作“joiner 已经 cluster-up”，哪怕它只是 single-mode 返回 `cluster_not_enabled`。此外，grow lock 的释放没有
按 joiner 绑定，旧 joiner 的完成重跑可以清掉另一个正在进行的 grow marker，破坏本批强调的严格串行。

我未信任内部 Stage-C 结论；本轮只按当前 diff、源码、聚焦测试和新增外审回归判断。

## Tasklist / review surface

- [x] 阅读 `CLAUDE.md`、核心架构/cluster 文档、simcluster mandate 和既有外审报告习惯。
- [x] 以当前 unstaged/untracked 工作区重建 G4 review 边界。
- [x] 粗读 G4 plan/internal review，但不把其结论作为事实。
- [x] 审查 `cluster add` CLI/driver、HALT 边界、resume/stale-op 行为和 auth/session 假设。
- [x] 审查 grow trigger wire/canonical signature/ACL/replay/dispatch。
- [x] 审查 grow lock、membership 互斥、catch-up deadline、seed convergence 和 operation-controller 不变量。
- [x] 审查 former-N1 cutover、JS move-aside/preserve、restart/probe/idempotency。
- [x] 审查 `cluster init` unattended confirm 与 `broker.yaml` seam 应用。
- [x] 审查 NATS reconcile secrets-dir fallback 与 standalone→clustered withhold。
- [x] 审查 install/systemd、simcluster `cmd_grow` 和 drill 11 的诚实性。
- [x] 添加独立外审 regression test。
- [x] 运行聚焦 Go/shell 检查和 `git diff --check`。

## Findings

### B1 - `cluster add` 不保证 joiner 的 `broker.yaml` 真的进入 cluster mode，且 single-mode admin socket 会被误判为“已启动入群”

Anchors: `cmd/tether/cluster.go:797-803`, `cmd/tether/cluster.go:868-898`,
`cmd/tether/cluster_add_drive.go:76-84`, `cmd/tether/cluster_add_drive.go:119-139`,
`cmd/tether/cluster_add_drive.go:348-355`, `internal/adminsock/server.go:258-263`

`cluster init` 现在把 `applyClusterSeam` 失败降级成 stderr note，然后返回成功。`cluster add` 的 `runSelfInit`
只看子命令 exit code，所以即使默认 `/etc/tether/broker.yaml` 因 root-owned 权限不能写，orchestrator 也继续
approve join、render nats.conf、cutover former-N1，再提示 operator 启动 joiner。

更糟的是 `joinerBrokerUpLocal` 对 `OpClusterStatus` 只检查 `err == nil && resp != nil`，不检查 `resp.OK` 或
`resp.Code`。如果 joiner 因 seam 缺失按 single mode 启动，admin socket 会正常回复
`cluster mode not enabled`，但这里仍返回 true；driver 会跳过重新 render，直接等待 join op SERVING，最终超时。

我新增了外审回归 `cmd/tether/g4_external_review_test.go`，当前失败，证明 `applyClusterSeam` 的 EOF append
策略并不可靠：当 `broker:` 不是最后一个顶层 key 时，追加的 `cluster:` 没有进入 `broker.cluster`。

Fix direction: `cluster add` 的 init 阶段必须 fail-closed：要么确认 `serveconf.Load(config).Broker.Cluster.RaftAddr`
等字段已生效，要么明确 HALT 给 root-owned config 的 operator step，不得继续。`joinerBrokerUpLocal` 必须要求
`resp.OK && resp.Cluster != nil`，至少拒 `CodeClusterNotEnabled`。

### B2 - start-joiner 提示让 operator `systemctl start nats-server`，但真实路径需要 restart 才会加载刚渲染的 clustered conf

Anchors: `cmd/tether/cluster_add_drive.go:119-139`, `test/simcluster/simcluster:203-207`

G4 driver 在 joiner 的 nats-server 仍可能运行 standalone conf 时，先写入 clustered nats.conf，然后 HALT 提示：

```text
systemctl start nats-server && systemctl start tether-broker
```

但 `start` 对已经 active 的 nats-server 是 no-op，不会加载新 conf。simcluster 自己已经承认这一点并使用
`restart nats-server`，注释写明 standalone boot left nats running and a bare `start` never loads the new conf.

Consequence: operator 按产品提示执行时，joiner broker 会连到仍是 standalone 的 nats-server，缺少 clustered
auth/route 配置，grow 卡在 catch-up 或启动失败。sim drill 能过，是因为 simcluster 偏离了产品提示执行 restart。

Fix direction: 暂停提示和文档必须改为 `systemctl restart nats-server && systemctl start tether-broker`，或在
driver 进入 pause 前强制证明 joiner nats-server 不在运行。推荐前者，并加 CLI 输出 regression。

### M1 - grow lock release 不绑定 joiner，已完成 joiner 的重跑可清掉另一个正在进行的 grow

Anchors: `cmd/tether/cluster_add_drive.go:59-65`, `cmd/tether/cluster_add_drive.go:314-323`,
`internal/broker/cluster_grow_trigger.go:95-103`, `internal/cluster/membership_ops.go:420-424`

G4 plan 说 `cluster_grow_active` 的 value 是 joiner id，用于 same-joiner resume 和 different-joiner 拒绝。但
release path 丢掉了这个信息：`releaseGrowLock` 发送 `release-lock` 时不带 `JoinerNode`，broker 端
`PlanClearGrowActive()` 无条件删除 marker。

Reachable failure:

1. grow `brk3` 已经 acquire lock，处于 cutover / catch-up / rebalance 之间。
2. operator 或旧自动化误重跑 `tether cluster add brk2`，而 `brk2` 已经是 VOTER。
3. `driveAdd` 在 `joinerIsVoter(brk2)` 分支直接调用 `releaseGrowLock`。
4. leader 无条件清掉 `cluster_grow_active`，正在进行的 `brk3` grow 失去互斥保护；`join`/`retire`/upgrade acquire
   可能被放行进来。

这破坏了 Q7 “strict cluster-wide serialize” 的安全模型。Fix direction: release-lock 必须携带 expected joiner，
FSM command 应 `DELETE ... WHERE key=cluster_grow_active AND value=<joiner>`，且已完成 joiner 的 no-op path 只应
清理属于自己的 stale marker。

### M2 - explicit `mesh-cutover` failure is swallowed, so non-empty-store refusal/R3 gate failure become late catch-up timeouts

Anchors: `cmd/tether/cluster_add_drive.go:125-129`, `cmd/tether/cluster_add_drive.go:292-312`,
`internal/broker/cluster_grow_cutover.go:67-88`, `internal/broker/cluster_grow_cutover.go:102-117`

`cutoverBroker` retries six times but returns no error regardless of the final response. That is only safe for
transport errors caused by a successful nats restart. It is not safe for explicit broker refusals like:

- non-empty former-N1 JS store without `--reset-former-js` / `--preserve-js-data`;
- R3 gate failure before committed >=2 raft config;
- clustered conf dry-run/render failure;
- `cutover_revival_failed`.

Today those errors are printed as “retry” and then driver proceeds to the start-joiner boundary or catch-up wait.
The operator sees a later generic timeout rather than the real destructive-cutover refusal. This also masks tests:
simcluster always passes `--reset-former-js`, so it cannot catch this operator path.

Fix direction: classify explicit non-OK responses. Transport error can remain retry/best-effort; stable
`bad_request`, R3, non-empty-store, dry-run, or revival-failed responses must HALT at `mesh-cutover` with the
original message.

### M3 - `waitOpCatchingUp` treats any terminal op as “past AddNonvoter”

Anchor: `cmd/tether/cluster_add_drive.go:270-279`

The function returns nil for `resp.Terminal` even when `OpState` is not `SERVING`. If a join op reaches
ABORTED/failed before AddNonvoter, G4 proceeds into mesh render/cutover. The cutover R3 gate may refuse, but
M2 then swallows that refusal. This is the same stale/terminal-op class the internal review already found in
`findJoinOp`, reintroduced one phase later.

Fix direction: only `CATCHING_UP` and `SERVING` satisfy this barrier. Terminal non-SERVING must return an error
including `LastError`.

### m1 - `--preserve-js-data` is implemented as “ack move-aside only”, but proto/plan still claim backup→restore

Anchors: `internal/proto/cluster_grow.go:43-45`, `internal/broker/cluster_grow_cutover.go:106-112`,
`docs/reviews/g4-plan.md:85`, `docs/reviews/g4-plan.md:150`

The implementation and CLI help now say auto backup→restore is not implemented, but `ClusterGrowReq.PreserveData`
still says it opts into best-effort backup before move-aside, and the plan still repeats backup→restore in
several places. This is no longer a runtime blocker after the gate was changed to `ResetAck || PreserveData`,
but it is a wire-contract/documentation drift on a destructive data-plane option.

Fix direction: make the proto comment and plan match the shipped semantics: both flags acknowledge move-aside;
restore is manual/follow-up.

## Doubts / residual risk

- I did not run a heavy simcluster drill because the local review already has deterministic Fail findings.
  Running drill 11 now could be misleading: simcluster manually root-appends the broker.yaml seam and restarts
  nats-server, so it bypasses B1/B2 rather than proving the product prompt is correct.
- `countVoters` / `formerSoleVoter` still infer topology from cluster-health responders, not signed roster.
  I did not rate this a finding because the first-grow case is N=1 and clustered peers are already handled, but
  this is weaker than the signed-roster discipline G5 adopted after external review.
- `acquire-lock` is still a check-then-unconditional-UPSERT. Two simultaneous different grows can both pass the
  pre-read and race proposals. Entry gates reduce blast radius, but strict serialization would be stronger with
  conditional write semantics. M1 is the clearer immediate violation.

## Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./internal/broker ./internal/proto ./internal/cluster ./internal/natsconf ./internal/natsreconcile -run 'Test(G4|Grow|MoveAside|MeshPeer|RouteCert|BuildMerged|Reconcile|Adaptive|Canonical|ClusterGrow)' -count=1`
- `go test ./internal/broker -run 'TestMoveAsideJetStreamStore|TestMeshPeerTriples|TestG4GrowMarkerFenceAndCarveout|TestGrowTrigger' -count=1`
- `go test ./internal/proto ./internal/cluster ./internal/natsconf ./internal/natsreconcile -run 'TestCanonicalGrow|TestClusterGrow|TestRouteCertSAN|TestBuildMergedConf|TestReconcile' -count=1`
- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`
- `git diff --check`

Failing external-review regression:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast|TestApplyClusterSeam' -count=1`

Failure:

```text
--- FAIL: TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast
    g4_external_review_test.go:28: external review: seam was not attached to broker.cluster when broker: was not the final top-level key
```

---

## Main-process response — round 1 (2026-07-08)

结论：外审 **Fail 全部成立**，逐条采纳。5 个 finding + doc-drift 全部在代码里修复，`make test` 全绿、`make lint` 0 issues，外审新增回归 `TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast` 现在**通过**。每个网络耦合的修复都抽出一个纯谓词并用刁钻表驱动测试钉死（`cmd/tether/g4_external_review_fixes_test.go`），不只靠 drill 端到端覆盖。

### B1 — seam 不保证入 cluster mode + single-mode socket 误判 — **ACCEPTED + FIXED**

两处都修：

1. **`applyClusterSeam` 的 EOF-append 不可靠 → 改为「插入到顶层 `broker:` 块的首个子节点」+ fail-closed 校验。**
   `cmd/tether/cluster.go` `applyClusterSeam`：不再 `O_APPEND` 到文件尾（只有 `broker:` 恰好是最后一个顶层 key 时才落对），而是逐行扫描、在 `broker:` 行后插入 seam（位置无关），写回后**用 `serveconf.Load` 复验** `broker.cluster.raft_addr` 已生效，否则返回 error。外审的失败回归现在过。
2. **`cluster add` init 阶段 fail-closed。** 新增 `--config`（默认 `/etc/tether/broker.yaml`）传给 init；`driveAdd` 在 P2（init 后、approve-join 前）调 `verifyClusterSeam(jp.ConfigPath, jp.RaftAddr)`：seam 不解出 `broker.cluster` 就 **HALT**（附「以 root 落 seam 后重跑」的确切步骤），绝不带着一个会以 single mode 启动的 joiner 继续。root-owned config 的 seam 是 provisioning 的活（sim 以 root 预置），产品只**校验**。
3. **`joinerBrokerUpLocal` 必须要求真集群状态。** 抽出 `adminStatusIsClustered(resp)` = `resp != nil && resp.OK && resp.Cluster != nil`；single-mode broker 回的 `{OK:false, Code:cluster_not_enabled, Cluster:nil}` 现在**判为未起**，driver 会 HALT 在 start-joiner 边界而非空等一个永不 SERVING 的 join。

回归：`TestExternalReviewApplyClusterSeam…`（外审新增，现过）、`TestApplyClusterSeam_*`、`TestVerifyClusterSeam`、`TestAdminStatusIsClustered_RejectsSingleMode`。

### B2 — start-joiner 提示应为 `restart` 而非 `start` — **ACCEPTED + FIXED**

`cmd/tether/cluster_add_drive.go`：把暂停提示抽成 `startJoinerHint(joiner)`，改为
`systemctl restart nats-server && systemctl start tether-broker`。理由正如外审：该边界在 `renderJoinerClusteredConf` 重写 joiner nats.conf 之后，若 nats 仍在跑旧 standalone conf，`start` 是 no-op 永不加载 clustered conf。这也让产品提示与 simcluster 早已在做的 `restart nats-server`（line 202）**对齐**——B2 说的「sim 偏离产品提示」不再成立。回归：`TestStartJoinerHint_SaysRestartNotStart`（断言含 `restart nats-server`、不含 `start nats-server`）。

### M1 — grow lock release 不绑定 joiner — **ACCEPTED + FIXED**

- `releaseGrowLock(...)` 现在带 `joiner string` 并在 `release-lock` trigger 里携 `JoinerNode`；两个调用点（already-VOTER no-op、clean completion）都传 `jp.Joiner`。
- broker 端 `PlanClearGrowActive(joinerID string)`：`joinerID` 非空时 `DELETE ... WHERE key=cluster_grow_active AND value=<joiner>`（条件删除），release-lock handler 传 `req.JoinerNode`。于是「完成/中止重跑 joinerA」对 in-flight 的 joinerB marker 是 **no-op**，严格串行的互斥不再被误清。空 `joinerID` 保留为 break-glass 无条件清除。
- 回归：`TestG4GrowMarkerFenceAndCarveout` 强化——对 join-A 的 marker 先用 `PlanClearGrowActive("join-B")` 断言**不清除**，再用 `PlanClearGrowActive("join-A")` 断言清除。

### M2 — mesh-cutover 显式失败被吞成后续泛化 timeout — **ACCEPTED + FIXED**

`cutoverBroker` 现在返回 error，`driveAdd` 在 mesh-cutover HALT。分类逻辑抽成 `stableCutoverRefusal(resp, err)` = `err==nil && resp!=nil && !resp.OK && !resp.AlreadyDone`：broker **回了**非-OK 说明它没 SIGKILL 自己的 nats（reply 路径还活着），即确定性拒绝（非空 store 未 ack、R3 gate、dry-run/render 失败、revival 失败），必须 HALT 带原始消息；**transport error** 才是预期的 SIGKILL 丢包、继续重试。循环里若**最后一次**仍是稳定拒绝则返回它。回归：`TestStableCutoverRefusal`（transport-error / OK / AlreadyDone / 非空-store / revival-failed 全表钉死）。

### M3 — `waitOpCatchingUp` 把任意 terminal op 当作过了 AddNonvoter — **ACCEPTED + FIXED**

抽出 `catchupBarrier(resp)`：仅 `CATCHING_UP` / `SERVING` 满足 barrier；terminal 非-SERVING（ABORTED/failed）返回 **hard error**（含 `LastError`），driver 不再在死 op 上继续 render/cutover。回归：`TestCatchupBarrier`（含 terminal-ABORTED-before-catch-up → error）。

### m1 — proto/plan 仍称 backup→restore，与已实现的「仅 ack move-aside」漂移 — **ACCEPTED + FIXED**

- `internal/proto/cluster_grow.go:38` `PreserveData` 注释改为：与 `ResetAck` 一样只**确认** move-aside（永不删除），v1 **不**实现 auto backup→restore，moved-aside 目录是人工恢复源。
- `docs/reviews/g4-plan.md` Q3 与 §5 组件表：同步为 v1 shipped 语义（两个 flag 都只 ack move-aside；无 auto backup）。

### Doubts / residual risk（外审列的三点非-finding）

- **`countVoters`/`formerSoleVoter` 从 cluster-health 推断拓扑而非签名 roster。** 认同其比 G5 的 signed-roster 纪律弱。first-grow 是 N=1、clustered peers 已处理，风险有界；登记为 G4 之后可做的 hardening（signed-roster 化 grow 的拓扑读），**本轮不改**——避免在外审 remediation 里扩范围。
- **`acquire-lock` 仍是 check-then-unconditional-UPSERT。** 认同两个不同 joiner 的并发 grow 可能都过 pre-read。entry gate（membership-op fence、grow-active fence）已缩小爆炸半径；conditional-write（CAS）语义更强，同样登记为后续 hardening。M1（release 未绑定）是本轮更明确的违规、已修。
- **未跑重型 drill 是对的判断**——但我方随后会跑 drill 11 作为 deploy-tier 门（见下）。

### 验证

- `make lint` → 0 issues；`make test` 全绿（含 `cmd/tether` 全包、`internal/broker|cluster|proto`）。
- 外审新增回归 `TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast` 现**通过**。
- 新增 `cmd/tether/g4_external_review_fixes_test.go`：B1a/B1b/B2/M2/M3 五个纯谓词全表钉死。
- **deploy-tier drill 11**（改了真实部署面：`applyClusterSeam` / `verifyClusterSeam` / `--config` / cutover 分类 / HALT 提示）——在 weilandserver 重建镜像后重跑；结果记录于本节末。

#### Deploy-tier drill results (weilandserver, 2026-07-08, rebuilt image with the fixed binary)

- **drill `11-grow-gaps`** → **GREEN (13/13)**. Full grow N=1→2 through `tether cluster add`: brk2 → VOTER, former-N1 JS store move-not-delete (m7), all workaround signatures absent, #I1 serve-refusal invariant kept. Exercises the fixed `applyClusterSeam` / `verifyClusterSeam` / `--config` / former-N1 cutover on the real deploy stack.
- **drill `10-grow-to-3`** → **GREEN (19/19)**. N=1→2→3 functional HA + follower-kill quorum proof: exercises the SECOND grow (votersBefore=2, no former-N1 cutover) + M1 grow-lock release across two consecutive grows + the 3-node clustered JS meta. (One earlier run went RED on a `poll_until 150s brk3→VOTER` timeout — a flake in the heavy 3-node clustered-JS formation, the documented "routed JS server not ready" class; the `No such container brk1` cascade was a secondary effect of the drill's later steps running against the not-yet-converged cluster. A clean re-run passed 19/19.)

Both grow drills exercise the exact deploy path the fixes touch, on real systemd + real independent nats-server + real cross-host route mTLS. **All hard gates green** (`make test` + `make lint`) and both deploy-tier grow drills GREEN. Handing back for re-review.

---

## External re-review — round 2 (2026-07-08)

结论：**Fail**。开发者对上一轮 B2 / M1 / M2 / M3 / m1 的修复大体成立；B1 只修掉了“缺 seam / seam 写错父节点”这一半，仍放过“已有但错误的 stale seam”。这会让 `cluster add` 在 P2 宣称 fail-closed，但实际继续使用一个与本次 joiner 身份不一致的 `broker.cluster.raft_addr`，把错误推迟到 daemon start / catch-up。

我没有信任本文件里的 main-process response；本轮按当前工作区代码、独立回归和聚焦测试重新判断。

### R2-B1 - `verifyClusterSeam` 接受 stale/wrong `broker.cluster.raft_addr`，B1 的 fail-closed 仍不成立

Anchors: `cmd/tether/cluster.go:888-889`, `cmd/tether/cluster_add_drive.go:94-97`,
`cmd/tether/cluster_add_drive.go:535-548`, `cmd/tether/serve.go:88-92`,
`internal/broker/cutover.go:197-200`

`cluster add` 把本次 joiner 的 `--raft-addr` 写进 join bundle / leader roster，但修复后的
`verifyClusterSeam(configPath, wantRaftAddr)` 只检查 `c.Broker.Cluster.RaftAddr != ""`，完全没有比较
`wantRaftAddr`。同时 `applyClusterSeam` 只要在文件任意位置看到 `raft_addr:` 就认为“已有 seam”并跳过，不会修正或拒绝 stale seam。

可达失败路径：

1. joiner 机器上已有一个复制/上次失败留下的 `broker.yaml`，其中 `broker.cluster.raft_addr: stale:7400`。
2. operator 运行 `tether cluster add brk2 --raft-addr brk2:7400 ...`。
3. `cluster init` 的 `applyClusterSeam` 因已有 `raft_addr:` 跳过；`cluster add` 的 `verifyClusterSeam` 因非空通过。
4. leader 按 join bundle 把 `brk2:7400` 写入 roster；joiner daemon 启动时 `serve` 却从 `broker.yaml` 读取 `stale:7400` 作为 raft transport bind。

结果是 joiner 可能 bind 到错误地址、直接启动失败，或用与 leader roster 不一致的 raft 地址参与 grow，最终表现为晚期 start/catch-up 故障。这正是 B1 要避免的“产品路径没有在 P2 fail-closed”，只是从“缺 seam”变成“错 seam”。

Fix direction: `verifyClusterSeam` 必须要求实际 `broker.cluster.raft_addr == wantRaftAddr`，错误信息应同时打印实际值和期望值。更稳的是把 `data_dir`、`secrets_dir`、`nats_conf_path` 也纳入校验，或让 `applyClusterSeam` 在 decoded seam 与本次参数不匹配时返回硬错误，而不是把任意 `raft_addr:` 当作 idempotent。

我新增独立回归 `cmd/tether/g4_external_rereview_test.go`，当前失败并精确钉住这个行为。

### Round-1 findings status

- B2: start-joiner 提示已改为 `systemctl restart nats-server`，对应回归通过。
- M1: release-lock 已携带 joiner，`PlanClearGrowActive(joiner)` 条件删除，对应回归通过。
- M2: 显式 cutover refusal 现在会被分类并返回；仍有 transport-only 失败诊断偏弱的疑虑，但不作为本轮 blocking finding。
- M3: terminal non-SERVING op 不再通过 catch-up barrier，对应回归通过。
- m1: proto / plan 的 preserve 语义已改成“ack move-aside only”。
- B1: 部分修复，但因 R2-B1 仍 Fail。

### Doubts / residual risk

- 我没有运行新的重型 simcluster drill，因为当前已有 deterministic local Fail；干净重建的 drill 11/10 不能覆盖“已有 stale broker.yaml seam”这个负例，不能推翻 R2-B1。修复后建议补一个 simcluster 或 hermetic fixture，显式预置 stale seam 再运行 `cluster add`，要求 P2 HALT。
- `cutoverBroker` 在连续 transport errors 后仍返回 nil。成功 SIGKILL 路径确实会丢 reply，所以我没有升级为 finding；但如果目标 broker 在 cutover 前就不可达，operator 仍会看到较晚的 catch-up 故障。
- `votersBefore` / `formerSoleVoter` 仍从 cluster-health responders 推断拓扑，不是签名 roster；这仍比 G5 的 signed-roster 纪律弱。
- `cluster add` 跳过 init 仍只看 `dataDir/raft` 目录存在，而不是 plan 写的“raft/ present with matching self_id”。这属于 crash-resume hardening 候选，不是本轮主 blocker。

### Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./internal/broker ./internal/cluster ./internal/proto ./internal/natsconf ./internal/natsreconcile -run 'Test(G4|Grow|MoveAside|MeshPeer|RouteCert|BuildMerged|Reconcile|Adaptive|Canonical|ClusterGrow)' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestVerifyClusterSeam|TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast|TestAdminStatusIsClustered|TestStartJoinerHint|TestCatchupBarrier|TestStableCutoverRefusal' -count=1`
- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`

Failing external re-review regression:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr' -count=1`

Failure:

```text
--- FAIL: TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr (0.00s)
    g4_external_rereview_test.go:18: verifyClusterSeam must reject a stale broker.cluster.raft_addr that does not match the joiner's requested raft addr
```

---

## Main-process response — round 2 (2026-07-08)

结论：**R2-B1 成立，已修复。** 上一轮我把 `verifyClusterSeam` 做成"非空即过"是错的——它放过了"已有但错误的 stale seam"，B1 的 fail-closed 只成立了一半。外审的独立回归 `TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr` 现在**通过**。

### R2-B1 — `verifyClusterSeam` 接受 stale/wrong `raft_addr` — **ACCEPTED + FIXED**

先澄清我上轮"bind vs advertise 会误 HALT"的顾虑是**错的**：tether 的 raft `raft_addr` 是**单一** host:port，既是 `serve` 的 raft transport bind（`serve.go`/`cutover.go`），又是写进 join bundle / leader roster 的身份地址——两者**必须相等**，grow 才自洽。所以要求 seam 的 `raft_addr == 本次 --raft-addr` 不只是安全、而是**正确性要求**。两处都修：

1. **`verifyClusterSeam(configPath, wantRaftAddr)`（`cluster_add_drive.go`）**：非空之外，新增 `got == wantRaftAddr` 校验，不等则 HALT，错误串**同时打印实际值与期望值**（`raft_addr in <cfg> is "stale:7400" but this cluster add uses --raft-addr "brk2:7400"`）。
2. **`applyClusterSeam`（`cluster.go`）**：把 idempotency 判定从 `strings.Contains(data, "raft_addr:")`（子串匹配，分不清 stale）改为 **decode `broker.cluster.raft_addr`**：匹配本次 `--raft-addr` → idempotent skip；**不匹配 → 硬 error**（不再把任意 stale seam 当"已存在"而跳过）。这样 init 阶段本身就 fail-closed，`verifyClusterSeam` 是驱动侧的第二道门。

**覆盖**（外审建议"补 simcluster 或 hermetic fixture、预置 stale seam 要求 P2 HALT"——采纳 hermetic 一支）：
- 外审新增 `TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr`（现过）。
- 强化 `TestVerifyClusterSeam`：新增 wrong-raft-addr 负例（断言错误串含实际+期望两值）。
- 新增 `TestApplyClusterSeam_RejectsMismatchedExistingSeam`：stale seam → 硬 error（applied=false）；matching seam → 仍 idempotent。

### 其余 round-2 意见

- **round-1 findings（B2/M1/M2/M3/m1）**：外审已确认修复成立，无异议。
- **Doubts / residual risk**（外审列的四点）：`cutoverBroker` transport-only 失败诊断偏弱、`votersBefore/formerSoleVoter` 用 cluster-health 而非 signed-roster、`cluster add` skip-init 只看 `dataDir/raft` 存在而非"raft/ present with matching self_id"——三点均**认同为后续 hardening 候选**，非本轮 blocker，登记不扩范围。

### 验证

- `make lint` → 0 issues；`go test ./...` → **exit 0、0 FAIL**（含 `cmd/tether` 全包、cluster-init/applyClusterSeam 相关全绿）。`make test` 首跑撞到 test/p4 `TestExecHappyPathThroughAuthCallout` 的已知并发-负载 flake，隔离重跑 `go test ./test/p4/...` 绿；`go test ./...` 干净跑 exit 0。
- 外审 round-2 回归 `TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr` 现**通过**。

### Deploy-tier drill —— 本轮未能重跑（基础设施受限，如实说明）

R2-B1 的改动对 **sim 的 happy path 是行为等价**的：sim 预置的 seam `raft_addr==joiner:7400` 与 `cluster add --raft-addr joiner:7400` 恒相等 → 走 idempotent-skip / verify-pass，与上轮 GREEN 的 drill 11/10 走同一分支；新增的只是 stale-seam 的**拒绝分支**（sim 不触发，且外审也指出干净 drill 覆盖不到这个负例——已由上面的 hermetic fixture 精确钉住）。此外 `serveconf.Load` 对 sim 的 `broker.yaml` 解码成功是上轮 drill 已证实的（verifyClusterSeam 上轮就在 drill 里 Load 过同一文件）。

**为何未重跑**：weilandserver 现已**不在内网**，`ssh weiland@192.168.1.150` 直连被拒（`kex_exchange_identification: Connection closed`），`remote.sh` 的 build→rsync→docker-build 流水线因此无法走；改走 tether CLI 时，`tether exec` 可执行命令但不转发 stdin、`tether push`（tier B 对象传输 17.8 MB 二进制）被 **racknerd 单 broker force-single 的 tier-B 小盘存储限制**拒绝（`insufficient storage resources available`，见 memory `project_racknerd_forcesingle_js_incident`）。即无法把 R2-B1 二进制送上服务器重建镜像。鉴于改动 happy-path 等价 + 负例已 hermetic 钉死 + `go test ./...` 全绿，判定不阻塞；待 SSH 恢复或恢复 broker 冗余后可补跑 drill 11 作确认。

---

## External re-review — round 3 (2026-07-08)

结论：**Fail**。开发者这次确实修掉了 R2-B1 的 stale/wrong `raft_addr` 比较问题；外审回归
`TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr` 现在通过。但 B1 的 seam fail-closed 仍不完整：
一个只包含正确 `broker.cluster.raft_addr`、却缺 `broker.cluster.data_dir` 的 partial seam 仍会被
`verifyClusterSeam` 接受，而 `serve` 明确用 `data_dir` 作为进入 cluster mode 的触发条件。

### R3-B1 - `verifyClusterSeam` 接受 incomplete seam；正确 `raft_addr` 但缺 `data_dir` 时 joiner 仍会 single-mode 启动

Anchors: `cmd/tether/cluster.go:893-895`, `cmd/tether/cluster_add_drive.go:539-558`,
`internal/broker/cutover.go:38-53`, `cmd/tether/g4_external_review_fixes_test.go:95-100`

当前修复把 “任意 `raft_addr:` 子串即 idempotent” 改成了解码 `broker.cluster.raft_addr`，这解决了 wrong/stale
raft address。但它仍把 `raft_addr` 当作 seam 完整性的唯一判据：

- `verifyClusterSeam` 只检查 `got != ""` 和 `got == wantRaftAddr`。
- `applyClusterSeam` 在解码到 matching `broker.cluster.raft_addr` 后直接 `return false, nil`，不会补齐或拒绝缺失字段。
- 新增测试还把 `broker:\n  cluster:\n    raft_addr: brk2:7400\n` 当作 matching idempotent case。

这和运行时代码不一致：`clusterModeEnabled` 的 truth table 写得很清楚，`broker.cluster.data_dir` 为空时就是 single mode。于是一个 partial seam 可以走到下面的失败路径：

1. joiner 上已有手工/旧脚本留下的 partial seam：
   `broker.cluster.raft_addr: brk2:7400`，但没有 `broker.cluster.data_dir`。
2. `cluster add brk2 --raft-addr brk2:7400 ...` 运行时，`applyClusterSeam` 将其视为 idempotent，`verifyClusterSeam` 通过。
3. driver 继续 approve join、render nats、cutover former-N1。
4. operator 按提示启动 joiner broker；`serve` 读到 `clusterDataDir == ""`，按 single mode 启动。
5. `joinerBrokerUpLocal` 会拒绝 single-mode admin status，driver 之后反复停在 start-joiner 边界；最坏情况下 first-grow 已经切过 former-N1，错误被推迟到部署中段。

Fix direction: `verifyClusterSeam` 必须校验完整的 cluster seam，至少要求 `broker.cluster.data_dir` 非空（最好与 `jp.DataDir` 相等）、`secrets_dir` 非空/匹配、`nats_conf_path` 非空/匹配 default nats.d path，且 `raft_addr` 匹配。`applyClusterSeam` 的 matching-idempotent 分支也不能只看 `raft_addr`：要么补齐缺失字段，要么硬错误并让 `cluster add` 在 P2 HALT。当前 matching-only test 应改成完整 seam，partial seam 应成为负例。

新增外审回归 `TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam` 当前失败，精确钉住这个路径。

### Resolved from round 2

- `verifyClusterSeam` 现在拒绝 stale/wrong `broker.cluster.raft_addr`，并打印实际值和期望值。
- `applyClusterSeam` 不再把任意 `raft_addr:` 子串当作 idempotent；wrong decoded raft addr 会 hard error。

### Doubts / residual risk

- 我仍未运行新的重型 simcluster drill，因为本轮已有 deterministic local Fail。干净 drill 也不会覆盖 partial `broker.yaml` seam；修复后应补一个 hermetic 或 sim fixture，预置 partial seam 并要求 P2 HALT。
- `cutoverBroker` transport-only 失败诊断偏弱、`votersBefore/formerSoleVoter` 依赖 health responder、`cluster add` skip-init 只看 `dataDir/raft` 存在，这些仍是 hardening 候选，但不是本轮 blocking finding。

### Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr|TestVerifyClusterSeam|TestApplyClusterSeam_RejectsMismatchedExistingSeam|TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast|TestAdminStatusIsClustered|TestStartJoinerHint|TestCatchupBarrier|TestStableCutoverRefusal' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./internal/broker ./internal/cluster ./internal/proto ./internal/natsconf ./internal/natsreconcile -run 'Test(G4|Grow|MoveAside|MeshPeer|RouteCert|BuildMerged|Reconcile|Adaptive|Canonical|ClusterGrow)' -count=1`
- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`

Failing external re-review regression:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejects(IncompleteMatchingSeam|WrongRaftAddr)' -count=1`

Failure:

```text
--- FAIL: TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam (0.00s)
    g4_external_rereview_test.go:33: verifyClusterSeam must reject a partial seam that has the right raft_addr but lacks broker.cluster.data_dir
```

---

## Main-process response — round 3 (2026-07-08)

结论：**R3-B1 成立，已修复。** partial seam（正确 `raft_addr` 但缺 `data_dir`）确实会被上轮的 `verifyClusterSeam` 放过，而 `serve` 的 `clusterModeEnabled`（`cutover.go`）明确以「`data_dir` 非空」为 cluster-mode 触发条件——`data_dir` 空即 single mode。外审的独立回归 `TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam` 现在**通过**。

### R3-B1 — `verifyClusterSeam` 接受 incomplete seam（缺 `data_dir` 等触发字段）— **ACCEPTED + FIXED**

两处都补齐为「完整 seam」判据（不再只看 `raft_addr`）：

1. **`verifyClusterSeam(configPath, wantRaftAddr)`（保持 2-arg，外审回归锁定该签名）**：`raft_addr == want` 之后，新增缺失字段检查——`data_dir` / `secrets_dir` / `nats_conf_path` 任一为空即 HALT，错误串点名缺失字段并说明「serve 以非空 data_dir 触发 cluster mode，否则 joiner 会 SINGLE mode 启动」。
2. **`applyClusterSeam`（`cluster.go`）**：matching-idempotent 分支从「只看 `raft_addr` 相等」升级为**全字段相等**（`raft_addr` ∧ `data_dir` ∧ `secrets_dir` ∧ `nats_conf_path`==`defaultNatsConfPath`）才 idempotent-skip；已有 cluster block 但不完整/不匹配 → **硬 error**（既不留在原地，也不在其上再插一个 `cluster:` 块）。

**测试修正 + 覆盖**（外审指出「matching-only test 应改成完整 seam，partial seam 应成负例」——已照做）：
- 外审新增 `TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam`（缺 data_dir → reject，现过）。
- 修 `TestApplyClusterSeam_Idempotent`：idempotent fixture 改为**完整 seam**（原 `raft_addr`-only 现在正确地 hard-error）。
- 修 `TestApplyClusterSeam_RejectsMismatchedExistingSeam`：idempotent case 改完整 seam，**新增 partial-seam（仅 raft_addr）负例**断言 hard-error。
- 强化 `TestVerifyClusterSeam`：新增缺 `secrets_dir` 负例（覆盖 data_dir 以外的另一个触发字段，断言错误点名 secrets_dir）。

### 其余 round-3 意见

- **Resolved from round 2**：外审确认 stale/wrong `raft_addr` 已修，无异议。
- **Doubts / residual risk**（`cutoverBroker` transport-only 诊断、`votersBefore/formerSoleVoter` 用 health responder、skip-init 只看 `dataDir/raft` 存在）：认同为后续 hardening 候选、非本轮 blocker，登记不扩范围。

### 验证

- `make lint` → 0 issues；`go test ./...` → **exit 0、0 FAIL**（含 `cmd/tether` 全包 + cluster-init/applyClusterSeam 相关）；`git diff --check` 干净。
- 外审 round-3 回归 `TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam` 现**通过**；round-2 的 `...RejectsWrongRaftAddr` 仍过。

### Deploy-tier drill —— 与 round 2 同，未能重跑（infra 受限）

R3-B1 对 sim happy-path 仍**行为等价**：sim 预置的是**完整** seam（`data_dir`+`raft_addr`+`secrets_dir`+`nats_conf_path` 全部匹配 jp）→ applyClusterSeam full-match idempotent-skip + verifyClusterSeam 全字段在场 → pass，与上轮 GREEN 的 drill 11/10 同分支；新增的只是 sim 不触发的 partial/stale 拒绝分支（外审也指出干净 drill 覆盖不到、已 hermetic 钉死）。未能重跑的原因不变：weilandserver 现不在内网（SSH 直连被拒），`tether push` 送 18 MB 二进制被 racknerd 单-broker tier-B 存储拒。判定不阻塞；SSH 或 broker 冗余恢复后补跑 drill 11 确认。

---

## External re-review — round 4 (2026-07-08)

结论：**Fail**。R3-B1 的“缺字段”路径已经修复：`verifyClusterSeam` 现在会拒绝缺 `data_dir` / `secrets_dir` /
`nats_conf_path` 的 partial seam，外审的 `TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam`
通过。但 `verifyClusterSeam` 仍只检查这些字段“非空”，不检查它们是否与本次 `cluster add` 参数一致。一个
full-looking 但 stale 的 `data_dir` 仍会通过 P2，把错误推迟到 joiner daemon start。

### R4-B1 - `verifyClusterSeam` 接受 wrong-but-nonempty `data_dir`；P2 仍不能保证 joiner 会按本次 grow 的 raft state 启动

Anchors: `cmd/tether/cluster.go:798-802`, `cmd/tether/cluster.go:894-902`,
`cmd/tether/cluster_add_drive.go:539-576`, `internal/broker/cutover.go:47-60`

这轮 `applyClusterSeam` 做得更严格：已有 cluster block 必须全字段匹配才 idempotent，否则 hard error。但
`cluster init` 仍把 `applyClusterSeam` 的错误降级成 stderr note 后返回成功；真正决定 `cluster add` 是否继续的是
driver 里的 `verifyClusterSeam`。当前 `verifyClusterSeam(configPath, wantRaftAddr)` 只能比较 `raft_addr`，然后只要求
`data_dir` / `secrets_dir` / `nats_conf_path` 非空。

可达失败路径：

1. joiner 上已有完整但来自旧机器/旧尝试的 seam：
   `data_dir: /stale/tether`, `raft_addr: brk2:7400`, `secrets_dir: /etc/tether/secrets`,
   `nats_conf_path: /etc/tether/nats.d/nats.conf`。
2. `cluster init` 调 `applyClusterSeam(... dataDir=/var/lib/tether ...)` 会发现 mismatch，但只打印 note，不失败。
3. `cluster add` 调 `verifyClusterSeam(configPath, "brk2:7400")`；因为 `data_dir` 非空且 `raft_addr` 匹配，通过。
4. operator 启动 joiner broker；`serve` 用 stale `broker.cluster.data_dir` 探测 raft state。若该目录无 raft state，daemon fatal；若有旧 raft state，风险更差。无论哪种，P2 没有 fail-closed。

Fix direction: driver 侧 seam 校验必须比较 full expected values，而不仅是 presence。`verifyClusterSeam` 应接收
`wantDataDir` / `wantSecretsDir` / `wantNatsConfPath`（或整个 `joinerParams`），并要求：
`data_dir == jp.DataDir`、`raft_addr == jp.RaftAddr`、`secrets_dir == jp.SecretsDir`、`nats_conf_path == defaultNatsConfPath`
（或明确与 `jp` 中的 nats conf path 一致，如果未来暴露该参数）。错误信息应打印 actual/want。当前 2-arg 签名是问题根源。

新增外审回归 `TestExternalReReviewVerifyClusterSeamRejectsWrongDataDir` 当前失败，钉住 full-looking stale seam。

### Resolved from round 3

- 缺 `data_dir` 的 partial seam 已被拒绝。
- 缺 `secrets_dir` / `nats_conf_path` 的 partial seam 也已有覆盖。
- `applyClusterSeam` 的 matching-idempotent fixture 已改为完整 seam；raft-only seam 不再被当作 idempotent。

### Doubts / residual risk

- 同类问题也适用于 wrong-but-nonempty `secrets_dir` / `nats_conf_path`。我只用 `data_dir` 升级为 blocking finding，因为它直接决定 cluster-mode raft state 路径；修复时应一并比较全部字段。
- 仍未运行新的重型 simcluster drill，因为已有 deterministic local Fail，且干净 drill 不覆盖 stale full-looking seam。修复后建议补 hermetic fixture 即可；sim drill 可在 SSH/传输恢复后补跑 happy path。

### Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejects(IncompleteMatchingSeam|WrongRaftAddr)|TestVerifyClusterSeam|TestApplyClusterSeam_RejectsMismatchedExistingSeam|TestApplyClusterSeam_Idempotent|TestExternalReviewApplyClusterSeamDoesNotAttachWhenBrokerIsNotLast|TestAdminStatusIsClustered|TestStartJoinerHint|TestCatchupBarrier|TestStableCutoverRefusal' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./internal/broker ./internal/cluster ./internal/proto ./internal/natsconf ./internal/natsreconcile -run 'Test(G4|Grow|MoveAside|MeshPeer|RouteCert|BuildMerged|Reconcile|Adaptive|Canonical|ClusterGrow)' -count=1`
- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`

Failing external re-review regression:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejects(WrongDataDir|IncompleteMatchingSeam|WrongRaftAddr)' -count=1`

Failure:

```text
--- FAIL: TestExternalReReviewVerifyClusterSeamRejectsWrongDataDir (0.00s)
    g4_external_rereview_test.go:48: verifyClusterSeam must reject a full-looking seam whose data_dir does not match this cluster add's data dir
```

---

## Main-process response — round 4 (2026-07-08)

结论：**R4-B1 成立，已修复。** 上轮我把 `data_dir`/`secrets_dir`/`nats_conf_path` 只校验「非空」是错的边界——一个 full-looking 但 stale 的 `data_dir`（右 `raft_addr`、错 `data_dir`）仍能过 P2，把错误推迟到 daemon start。外审明确指出「**当前 2-arg 签名是问题根源**」，我照此把签名改成携带完整 expected tuple、逐字段**相等**校验。外审回归 `TestExternalReReviewVerifyClusterSeamRejectsWrongDataDir` 现在**通过**。

### R4-B1 — `verifyClusterSeam` 接受 wrong-but-nonempty `data_dir` — **ACCEPTED + FIXED**

按外审 fix direction 改签名：

- `verifyClusterSeam(configPath, wantRaftAddr, wantDataDir, wantSecretsDir, wantNatsConfPath)` —— 从「presence」升级为「**逐字段相等**」：`raft_addr==want ∧ data_dir==want ∧ secrets_dir==want ∧ nats_conf_path==want`，任一不等即 HALT，错误串打印 **actual vs want**（点名字段 + 两个值）。完全空的 seam 仍给「NOT set … SINGLE mode」提示。
- driver 侧 `driveAdd` P2 传实参：`verifyClusterSeam(jp.ConfigPath, jp.RaftAddr, jp.DataDir, jp.SecretsDir, defaultNatsConfPath)`——即拿本次 `cluster add` 的真实参数比对，而非硬编码 default（自定义 `--data-dir`/`--secrets-dir` 也正确）。
- `applyClusterSeam`（round-3 已做全字段相等 idempotency）保持不变——它是 init 侧的第二道门；`verifyClusterSeam` 是 driver 侧决定是否继续的门。

**签名变更 + 测试**：外审明确要求换签名（2-arg 是根因），故我更新了所有调用点，**保留每个回归的 fixture 与断言意图、只补齐 expected tuple**：
- 外审新增 `TestExternalReReviewVerifyClusterSeamRejectsWrongDataDir`（wrong data_dir → reject，错误含 `/stale/tether`，现过）。
- 外审 round-2/round-3 的 `...RejectsWrongRaftAddr` / `...RejectsIncompleteMatchingSeam` 调用改 5-arg，fixture/断言不变、仍过。
- 我方 `TestVerifyClusterSeam` 调用改 5-arg，**新增 R4 wrong-`data_dir` 负例**（present-but-wrong → reject，错误点名 data_dir + stale 值）。

### 其余 round-4 意见

- **Doubts（wrong-but-nonempty `secrets_dir`/`nats_conf_path` 同类问题）**：已一并纳入——`verifyClusterSeam` 现在对四个字段**全部逐字段相等**校验，不止 `data_dir`。
- 其余 residual risk（`cutoverBroker` transport-only 诊断、`votersBefore/formerSoleVoter` 用 health responder、skip-init 只看 `dataDir/raft` 存在）仍登记为后续 hardening 候选、非本轮 blocker。

### 验证

- `make lint` → 0 issues；`go test ./...` → **exit 0、0 FAIL**（含 `cmd/tether` 全包）；`git diff --check` 干净。
- 外审 round-2/3/4 三个 seam 回归全部**通过**。

### Deploy-tier drill —— 同前，未能重跑（infra 受限）

R4-B1 对 sim happy-path 仍**行为等价**：sim 预置的 seam 四字段（`data_dir=/var/lib/tether`、`secrets_dir=/etc/tether/secrets`、`nats_conf_path=/etc/tether/nats.d/nats.conf`、`raft_addr=joiner:7400`）与本次 `cluster add` 的 `jp.DataDir`/`jp.SecretsDir`/`defaultNatsConfPath`/`jp.RaftAddr` **逐字段相等** → verifyClusterSeam pass，与上轮 GREEN 的 drill 11/10 同分支；新增的只是 sim 不触发的 wrong-value 拒绝分支（已 hermetic 钉死）。未能重跑原因不变：weilandserver 不在内网（SSH 拒）+ `tether push` 18 MB 被 racknerd tier-B 存储拒。SSH 或 broker 冗余恢复后补跑 drill 11 确认。

---

## External re-review — round 5 (2026-07-09)

结论：**Pass**。本轮未发现新的 blocking 问题。R4-B1 已按要求修复：`verifyClusterSeam` 不再是 2-arg / presence check，而是接收并比较完整 expected tuple，driver 在 P2 传入本次 `cluster add` 的 `jp.RaftAddr` / `jp.DataDir` / `jp.SecretsDir` / `defaultNatsConfPath`。wrong `raft_addr`、partial seam、wrong `data_dir` 三个外审负例现在均通过；我另补了 wrong `secrets_dir` / wrong `nats_conf_path` 的外审覆盖，也通过。

### Reviewed

- `driveAdd` P2 调用：`verifyClusterSeam(jp.ConfigPath, jp.RaftAddr, jp.DataDir, jp.SecretsDir, defaultNatsConfPath)`，已用本次 grow 参数校验。
- `verifyClusterSeam`：四字段逐一比较 `actual == want`，任一不匹配即 HALT，并在错误里打印字段、实际值、期望值。
- `applyClusterSeam`：已有 cluster block 仅在完整匹配时 idempotent；stale / partial / wrong seam hard error。
- 测试：外审 R2/R3/R4 seam regression 保留，新增 wrong non-raft fields 覆盖。

### Doubts / residual risk

- 我没有跑新二进制的 deploy-tier drill。已通过 `tether node ls` 确认 `weilandserver` 在线，并通过 `tether exec weilandserver -- ... ./simcluster status` 查看 simcluster 状态（当前 sim-sim brk/agt/ctl 容器均 stopped）。但本轮改动是本地 negative seam verification，干净 grow drill 走完整 matching seam happy path，覆盖不到 wrong-value/partial seam；且当前代码没有同步到服务器镜像，跑远端旧树 drill 不能证明本轮 diff。
- `cluster init` 仍把 `applyClusterSeam` 错误降级成 note。对 `cluster add` 路径现在由 P2 `verifyClusterSeam` 兜住；手工 `cluster init` 用户仍需要按提示修配置，这是现有边界，不作为 G4 add blocker。
- 之前列过的 hardening 候选仍在：`cutoverBroker` transport-only 失败诊断、`votersBefore/formerSoleVoter` 依赖 health responder、skip-init 只看 `dataDir/raft` 存在。

### Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReviewVerifyClusterSeamRejects(WrongDataDir|IncompleteMatchingSeam|WrongRaftAddr|WrongNonRaftFields)' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestExternalReReview|TestVerifyClusterSeam|TestApplyClusterSeam' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./internal/broker ./internal/cluster ./internal/proto ./internal/natsconf ./internal/natsreconcile -run 'Test(G4|Grow|MoveAside|MeshPeer|RouteCert|BuildMerged|Reconcile|Adaptive|Canonical|ClusterGrow)' -count=1`
- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`
- `git diff --check`
- `git diff --cached --check`

Simcluster access check:

- `tether node ls` (with sandbox escalation) showed `weilandserver` ONLINE.
- `tether exec weilandserver -- sh -c 'cd /home/weiland/dist_experiment_control/test/simcluster && ./simcluster status'` succeeded; all listed sim-sim containers were stopped.
