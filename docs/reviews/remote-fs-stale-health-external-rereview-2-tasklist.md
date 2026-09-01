# remote-fs stale-health 独立外部复审（二）tasklist

> 基线：2026-08-31 外审复审已暂存的 21 文件。对象：其后 8 个未暂存返修文件；编码者回复与绿色结果均为待证伪陈述。

## A. 范围与逐项对账

- [x] A1. 粗读 staged/unstaged、stat/numstat、diff-check，确认第二次返修 delta 为 8 文件，无生产 Go 实现变化。
- [x] A2. 完整阅读相对 index 的 8 文件 diff；对账 RR-F1～F5、#81 ledger、drill 62 oracle 和共享 logs helper。
- [x] A3. 核验 CLAUDE/gotcha/report 的声明没有超过实际 gate 与 deploy 证据；确认 #80 仍不越界。

## B. 架构门复核

- [x] B1. 验证 async direct invalidation、if initializer Lock、goto bypass 三项原样转绿，且不是通过放宽断言或删测试。
- [x] B2. 深审 CFG/flow 实现：init/cond/post、return/break/continue/goto/label、loop/switch/select、go/defer/func literal、fallthrough、unreachable path 和 unknown 方向。
- [x] B3. 添加必要 reviewer 正负反例，特别检查 label 作用域、跨嵌套 goto、switch/for initializer、defer 同步语义、helper 调用身份和 false-positive 可维护性。
- [x] B4. 重放现有 gate ledger 与开发者宣称的变异族，确认新算法既不 silent-pass 也不 vacuous-red。

## C. drill 62 / oracle / ledger

- [x] C1. 审查三层 oracle 的执行顺序、短路关系、request identity、broker-forward/agent-receive区分、rc 分类和产品码断言。
- [x] C2. 审查 cursor-bounded logs helper：字节偏移、截断/轮转/重启、缺文件、注入/quoting、读取失败与 shell portability。
- [x] C3. 审查临时目录 cleanup/trap、并发/旧证据、失败输出及 harness verdict 是否可能 false-green/false-red。
- [x] C4. 审查 expected-verdict owner/band/signature 与 log；#81 在根因闭合和三次 deploy 绿后已关闭并删除 band，新增 contract 反向保证 A/B/C 新红不被吞；#80 维持越界。
- [x] C5. 从当前源码重建并运行 drill 62；最终 `INCOMPLETE rc=4 pass=41`，产品/assert/setup 红均 0，唯一 gap 为 OQ-2；trap 后无残留实例。

## D. 门禁与交付

- [x] D1. 运行 focused/repeat、architecture/determinism、affected race、sim hermetic、make gates/test/e2e-parallel/lint、build、bash syntax 和 diff-check。sim hermetic 目标测试均绿，整套唯一红为既有越界 `UNOWNED #80`。
- [x] D2. 在外审报告追加第二次独立复审 verdict、findings、疑惑、NOT-COVERED 和验证矩阵；首行反映最新结论。
- [x] D3. gofmt reviewer Go 测试，全部 tasklist 打勾；编码者返修文件已精确加入 index，审查者后续实现/测试/报告保持 unstaged，未使用 `git add -A` 混淆两者。
