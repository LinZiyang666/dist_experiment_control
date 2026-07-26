# e2e 并行化与 Batch A 复审 — 外审 tasklist

> 日期：2026-07-26
>
> 原则：内部回复与已有绿灯只作线索；覆盖等价、失败传播与 Batch A finding
> 均由当前代码、变异测试或本轮命令重新证明。

## 1. 范围与暂存边界

- [x] 核对已有 staged 基线与本轮 unstaged 文件归属。
- [x] 阅读 Batch A 外审逐条回复、内审处置和 e2e 并行化计划。
- [x] 阅读 CLAUDE 新文档权威链、并行测试纪律和提交前硬闸。
- [x] 确认 `tools/` 始终排除，不被本轮任何增补暂存命令带入。
- [x] 记录最终 staged/unstaged/untracked 边界与 diff 统计。

## 2. Batch A 重新外审

- [x] B2：真实 `raft.ServerAddress` / `raft.ServerID` peer 去重反例转绿且不过度纳入数值噪声。
- [x] B3：helper 的变量、函数结果、参数转发均进入 unresolved 或显式 exemption，门禁自检可翻红。
- [x] B4：ACL 只把真实 Subscribe 路径当订阅；死声明、wrapper、动态 subject 与 wildcard 变异均有诚实行为。
- [x] B5/M4：真实三节点 operation-retire 成功路径恢复，legacy refusal 独立保留；leader flake 修复不靠放宽断言。
- [x] M1/M2：proto mismatch 同义码一致；混合永久/瞬时错误码已拆分或有保守且文档一致的分类。
- [x] M3：CLAUDE/requirements/architecture 权威链无环且措辞一致。
- [x] 元门禁：codes registry 排除消费侧自证；HTTP listener 扫描仓库级；runtime JSON 排序与 metrics contract 有测试。
- [x] 核实 review response §4 未做项仍诚实登记，没有被 Pass 结论误写成已完成。

## 3. e2e 并行化语义等价

- [x] 枚举串行 gate 的每个顶层测试及其真实 subprocess 命令。
- [x] 比较并行 unit 的 packages、build tags、`-race`、timeout、`-run`、`-count` 与 working directory。
- [x] 验证 `TestAllPhases` 及 p1–p10/p13 不再丢失，并对 fallback 做变异测试。
- [x] 验证一个矩阵含多个 command、部分可解析/部分不可解析时不静默少跑。
- [x] 验证 package splitting 与 name sharding 不漏 TestMain、子测试、build-tag tests 或原始过滤条件。
- [x] 验证 full run 与 `-run` partial run 的选择语义、标识和退出状态。
- [x] 验证 scheduled/result 对账不会在非 split 模式、repeat、deadline、duplicate/missing substitution 下误判。
- [x] 验证任一 compile/test/numactl/topology/worker 失败都产生非零退出且保留足够诊断。

## 4. 拓扑、资源隔离与可移植性

- [x] 审查 allowed CPU、SMT sibling、NUMA node 发现与 cpuset 限制。
- [x] 审查 busy CPU 采样的真实性、空 NUMA node、不均衡 node 与 worker 分配。
- [x] 证明 worker CPU 不重叠、不跨 node，且不足资源 fail-closed。
- [x] 验证 numactl 缺失、非 Linux/sysfs 缺失、单 NUMA、无 SMT 与受限 affinity 的行为。
- [x] 检查 context deadline、子进程终止、输出截断和 goroutine 生命周期。

## 5. 测试与报告

- [x] 为 splitter/result/allocation 的独立反例添加测试，不修改被审实现。
- [x] 运行并行 runner dry-run、定向矩阵、全量矩阵及必要 repeat/flake 检查。
- [x] 运行 Batch A 定向测试、D7 race、`make test`、`make lint`。
- [x] 根据改动面判断 simcluster；不需要时写明依据。
- [x] 形成 Batch A 复审结论，并新增 e2e 并行化外审报告（Fail/Pass 开头）。
- [x] 按用户文件归属增补暂存；`CLAUDE.md` 精确暂存全部两个归属增量；不 commit、不 push。

## 完成说明

- Batch A 各项均已复验；勾选表示审查动作完成，不表示实现通过。R1–R6 见复审报告。
- 并行 full workload 实跑一次；由于已稳定发现三处原有满载 flake 与四个结构反例，
  未用重复跑更多失败轮次消耗资源。
- 非 Linux / 无 numactl 无可用宿主实测；已通过代码路径审查确认当前行为是显式失败，
  并在报告列为已知可移植性边界。
- 改动不触及 deploy-tier 产物，无相关 simcluster drill。
