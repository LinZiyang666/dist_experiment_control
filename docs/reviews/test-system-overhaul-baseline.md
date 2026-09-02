# 测试体系革新 · 基线测量（过程产物）

> Date: 2026-09-01，树 `0be2365`，主机 weilandserver（44 核）。
> 对应 `docs/reviews/test-system-overhaul-plan.md` B0「基线记录」行。这些数字的用途是**给新闸门的边际成本记账**
> 与**回答 A8（component 标记）做不做**；不是性能目标。命令写在每个数字旁边，以便重新导出。

## 1. `make gates` 墙钟（新闸门的边际成本基线）

```
$ time make gates        # vet-tags → 6 个包的 go test → make lint
real    1m31.427s
user    2m07.244s
sys     0m24.096s
```

plan §-1 F15：本增量新增闸门的总增量上限 10s（≈11%）。

## 2. `internal/broker` 逐测试耗时（唯一全矩阵闸的 ceiling 在哪）

```
$ go test -json -count=1 ./internal/broker/ > broker_timing.jsonl      # 无 -race
```

| 指标 | 值 |
|---|---|
| 包墙钟 | 332.1s |
| 顶层 Test 数 | 743 |
| 逐测试 Elapsed 之和 | 332.1s（= 包墙钟：包内串行，与 e2e-parallel-plan §6「只用 0.7 核」一致） |

**Top-20 测试**（秒 / 名 / 文件）：

```
10.61  TestG67HeadOfLineLatencyIsBounded                                   every_started_attempt_test.go
 8.24  TestCorruptOutboxRecoveryActionsBehaveAsDocumented                  xfer_corrupt_recovery_test.go
 7.41  TestTopoAdvanceBoundsTheUnboundedGate                               topo_advance_test.go
 6.00  TestG67SizingTimeoutCannotMoveTheAdmissionDecision                  every_started_attempt_test.go
 5.85  TestCatchingUpIsAlwaysBounded                                       topo_advance_test.go
 5.36  TestG67EveryStartedAttemptGetsAFullBudget                           every_started_attempt_test.go
 5.29  TestLeadershipEdgeCreatesFinalizeOpOnlyForTheGhostShape             force_single_finalize_drive_test.go
 4.12  TestStatusReportGivesNoAcctVerdictWhenTheViewCannotNameItsOwnKey    acct_nk_honesty_test.go
 3.89  TestSessionCreateReportsSuccessWhenCommittedButNotYetVisible        session_idempotent_test.go
 3.85  TestJoinVersionGateRefusesBeforeAnyStateIsCreated                   join_version_gate_test.go
 3.65  TestForceSingleHandlerArmCommitPersistsMarkerAndEpoch               force_single_handler_test.go
 3.62  TestForceSingleOnlineConvergesSeeds                                 forcesingle_converge_test.go
 3.37  TestBackgroundProbeMustNotOverwriteANewerObservation                lease_probe_staleness_test.go
 3.12  TestContestedProbeCostsTheFullBudgetAgainstASilentSubscriber        instance_lease_probe_test.go
 2.94  TestPerformGrowCutoverRefusesRecoveredResidue                       grow_trigger_test.go
 2.81  TestIngressRefusalSurface                                           ingress_characterization_test.go
 2.64  TestRegisterDuringLeadershipLossIsRetriableNotTerminal              register_leadership_loss_test.go
 2.58  TestSizingStallDoesNotConsumeCreateBudget                           xfer_provision_test.go
 2.35  TestJSPlacementRunsAfterTopologyConvergence                         js_placement_gate_test.go
 2.34  TestExternalFailedNonvoterJoinCanBeRemovedOnline                    cluster_phase_fluidity_external_test.go
```

**Top-10 文件**（秒）：

```
26.9  force_single_finalize_drive_test.go
26.3  every_started_attempt_test.go
21.5  clusteradmin_test.go
17.9  js_placement_gate_test.go
16.7  topo_advance_test.go
16.3  seed_helper_test.go
13.8  cluster_operation_controller_test.go
12.6  force_single_handler_test.go
 8.7  home_delivery_test.go
 8.7  terminal_failure_outbox_test.go
```

**读法**：大头是**真实时间预算**的测试（head-of-line 延迟上界、sizing 超时、topo advance 上界、
force-single 驱动）——它们等的是产品的 `time.After`，不是 NATS 启动。这与 e2e-parallel-plan §6
「包内串行、0.7 核」的诊断一致：砍墙钟的正确杠杆是给这些预算加注入时钟接缝（B3 给 lease 裁决接的那种），
不是拆包或减测试。**本增量不做**——它属于下一次「broker 时间预算接缝」增量的输入。

## 3. A8 裁决：起嵌入式 NATS 的同包测试占比

```
$ grep -lE 'natstest\.RunServer|testharness\.Start(JS)?NATS' internal/broker/*_test.go | wc -l   # 13
```

| 指标 | 值 |
|---|---|
| 起嵌入式 NATS 的测试文件 | 13 / 157 |
| 这 13 个文件里的测试耗时占包耗时 | **9.5%** |

plan §6「arch A8 component 标记 `-short`」的判据是 <30% 永久登记不做。**9.5% ⇒ 永久不做。**
"慢的是不是那 30 个文件"——不是。同包 L1/L2 混居的代价在这个包上不存在，标记只会制造 13 处 `if testing.Short()` 的噪声。

## 4. 940 冗余的 build-identity 收据

```
$ for pkg in ./internal/cluster ./internal/broker; do
    for t in "" d5_integration d9_integration; do
      go list ${t:+-tags $t} -f '{{join .GoFiles " "}} {{join .Imports " "}}' $pkg | tr ' ' '\n' | sort | md5sum
    done; done
internal/cluster   notag=ff4e4637  d5=ff4e4637  d9=ff4e4637
internal/broker    notag=62a28e76  d5=62a28e76  d9=62a28e76
$ grep -rl 'go:build.*_integration' internal/ cmd/ | grep -v _test.go | wc -l
0
```

`phasefluidity_integration` 是唯一改变共享包文件集的 tag（broker 多出 `phasefluidity_lifecycle_test.go`，
TestGoFiles md5 `d26922aa` ≠ `593f6ff7`）——所以 B6 的判据必须是运行时闭包 hash，不能是静态表。
