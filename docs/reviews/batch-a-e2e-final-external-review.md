# Pass — 批次 A + e2e 并行化最终外部审查

> 日期：2026-07-26
> 审查者：Codex（外部审查；不采信内部绿灯）
> 范围：批次 A 复审整改、e2e 并行化整改、满载 flake 根因修复，以及最终外审者直接修复。

## 结论

**Pass，可以放行本次批次 A 与 e2e 并行化增量。**

这个结论是在外审者新增反例、直接修复新发现问题，并对最终代码执行完整硬闸之后给出的，
不是对开发者“三轮全绿”记录的转述。

最终提交配置连续三轮执行完整并行矩阵：每轮 15/15 顶层矩阵、99 个调度单元，
合计 **297 units / 0 failure**。上一轮阻断的 D3 follower PIN、broker mutating redirect、
D7 `FollowerStatusViewSource` 均未复现；`TestAllPhases` 每轮真实执行并通过。

## 一、开发者整改的独立复验

以下上一轮 finding 已确认闭合：

1. 非 split 模式不再按空 `units` 对账，成功运行可正常返回 `ALL PASS`。
2. 同一函数内“一个可解析命令 + 一个动态命令”会整矩阵 fallback，不再只跑一半。
3. 原始 `-run` 被保留；带原始 filter 的 unit 不再被名称分片覆盖。
4. parallel 包可正常拥有并执行单元测试，不再污染顶层矩阵 coverage list。
5. NUMA worker 按容量分配，资源总量足够时不会因小节点被过量分配而拒跑。
6. `runningHeavyCPUs` 已诚实描述为瞬时启发式，不再宣称测量“>50% 利用率”。
7. ACL 普通 keyed `subj` 字段不再伪装成活订阅。
8. Raft 日志去重只接受 string-kind 身份；`time.Duration` 等 numeric Stringer 不再进入 key。
9. D7 orchestrator 改为绑定当前 leader，不再强迫 leadership 回到 node 0。
10. 权威文档链中 requirements/current architecture/historical architecture 的直接矛盾已清理。
11. retire 测试已准确降级表述为“不可逆步骤集成覆盖”，没有再声称到达 `RETIRED` 终态。

## 二、最终外审新增发现及直接修复

### F1 · CI 固定 20 workers 会在小型 runner 启动前失败

CI 已改为运行 `make e2e-parallel`，但 Makefile 仍固定传 `-workers 20`。可用物理核少于 20 时，
`allocate` 会按设计直接拒绝，因此典型小型 CI 连一个测试都不会执行。

修复：

- Makefile 使用 runner 自动并行度；
- 自动值为 `min(work items, physical cores/2, 20)`，最少 1；
- 2/4 核环境分别使用 1/2 worker，44 核开发机保持 20；
- `make e2e-one` 收窄到 `./test/e2e` 并使用精确 test-name anchor。

### F2 · 错误码豁免仍可整文件覆盖，且 stale site 不会失败

开发者把 scanner 输出改成 site key，但报告端仍接受 10 个文件级 exemption。未来在这些热文件
新增任何动态 code 都会被整文件理由吞掉。三个已有 `file:line` 条目已经被 form 9 解析，
却仍能留在清单中。

修复：

- 禁止全部文件级豁免；
- 43 个实时动态点逐一改为 `file:line` 并写明理由；
- 每次重新扫描，豁免必须仍精确命中 unresolved site；
- site 移动、删除或变为可解析都会使门禁失败。

### F3 · ACL guard 把所有 BinaryExpr 当作“已解析”

`Subscribe(SubjectPrefix + suffix, ...)` 中 `suffix` 是运行时参数时，原 guard 仍因表达式属于
`BinaryExpr` 而直接放过，subscriber→grant 方向继续存在静默漏检。

修复：

- 对 literal、已解析 local、proto subject selector 和递归常量拼接做实际可解析性判断；
- 动态拼接进入显式 exemption gate；
- 新暴露的 `home_delivery.go` broker-owned `_INBOX` ack subscription 逐 site 登记理由；
- dynamic subscription exemption 同样增加 stale 检查。

### F4 · splitter 仍会漏 helper command，并会丢未知 flags

一个 Test 同时包含直接 `exec.Command` 和同文件 helper 内的命令时，原计数只看 Test body，
仍会接受直接命令并漏 helper。`parseGoTestArgs` 对未知 flag 和动态 timeout 也会静默忽略，
从而执行不同命令。

修复：

- 建立同文件 helper call 检查；helper 可达命令时整矩阵 fallback；
- 非 `go test`、动态参数、未知 flag 一律 fail-closed；
- 已支持参数逐字保留，不再重新拼一个“近似命令”；
- `-race` 同步用于 `go test -list`，避免漏掉 race-build-tag 测试；
- 多个 whole fallback 使用缓冲 heavy queue，避免 light workers 被发布端阻塞。

共同结构化 manifest 仍是更优的长期方案；当前 parser 对已知与未知命令形态已改为保守 fallback，
所以该结构风险降为中风险维护项，不再阻断当前提交。

### F5 · Raft timing guard 接受本地硬编码别名

开发者报告称 17 处均引用生产常量，但 D7 仍定义：

```go
d7HeartbeatTimeout = 1000 * time.Millisecond
d7LeaderLeaseTimeout = 500 * time.Millisecond
```

数值恰好相等不等于绑定生产常量；生产值变化后 D7 仍会漂移。原 guard 只拒绝字段内的 literal，
所以别名稳定绕过。

修复：

- D7 两个 Config site 直接引用 `cluster.Multinode*Timeout`；
- 删除硬编码别名及仍声称“300ms 快速选举”的矛盾注释；
- timing guard 只接受字段对应的精确生产常量引用；
- alias、算术缩放和自编 duration 均被独立反例拒绝；
- 文件遍历和解析错误不再被静默忽略。

### F6 · heavy worker 只查看最后一个 NUMA 节点

最后节点不足三个 slice 时，旧实现直接放弃宽 worker，即使其他节点完全有能力提供。
修复为在所有节点中选择可提供目标 slice 且合并后核心数最多的节点。

## 三、独立验证证据

| 验证 | 结果 |
|---|---|
| 上一轮 7 个 external-review 反例 | 全部 PASS |
| 本轮新增 scanner/ACL/parser/CI/NUMA/timing 反例 | 修复后全部 PASS |
| `go test -count=1 ./cmd/tether ./internal/auth ./internal/cluster ./test/determinism` | PASS |
| `go test -race -count=1 -tags d7_integration ./test/d7/` | PASS |
| `make test` | PASS，全包 0 failure |
| `make lint` | PASS，0 issues |
| `make e2e-parallel` | PASS，99 units，2m53.609s |
| `make e2e-parallel E2EPAR_FLAGS=-repeat=2` | PASS，198 units，5m23.398s |
| 三轮累计 | **297 units / 0 failure** |
| coverage self-check | 每轮 15/15 |
| `git diff --check` | PASS |

未运行 simcluster：本轮没有修改 install、systemd、nats.conf、真实 route mTLS 或部署生命周期，
不满足 deploy-tier drill 的触发条件。

## 四、保留疑惑与非阻断建议

1. **共享 matrix manifest 仍未实现。** 当前源码解析已经 fail-closed 覆盖已知风险，但未来出现
   新型间接命令仍需先补 parser 反例。建议后续让测试入口与 runner 共用结构化命令定义。
2. **retire 完整 `RETIRED` 终态 harness 仍未实现。** 当前证明到 RemoveServer→roster delete
   的不可逆安全顺序；报告措辞已经准确，因此不阻断本批。
3. **其他 5 个 `findFreePort` 调用点仍有 TOCTOU。** 本轮只修了实际复现的 reconnect harness。
4. **宽 worker 合并数仍为 3。** 调度节点选择已自适应，但宽度是本机校准值；应继续积累其他
   机器数据。
5. 批次 A 既有后续项仍按原登记处理：D13 第 2 步、D22 release note、A8 文档归档、
   34 条既存失效测试引用、codes.go emitter 迁移。

这些项目没有被包装成“已完成”，也没有观察到会阻断当前代码上线的确定性产品缺陷。
