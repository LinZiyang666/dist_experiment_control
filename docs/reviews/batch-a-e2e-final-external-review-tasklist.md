# 批次 A + e2e 并行化最终外审 tasklist

> 外部审查者：Codex
> 日期：2026-07-26
> 原则：不采信内部绿灯；先复现上一轮反例，再检查大改引入的新边界。确认的问题由外审者直接修复。

## 1. 范围与证据链

- [x] 核对暂存区、未暂存区、新文件和生成物边界，保持 `tools/` 排除。
- [x] 重读 `CLAUDE.md`、权威需求/架构文档、两份原外审及开发者回复。
- [x] 将 47 个未暂存文件按 runner、flake、门禁、文档/CI 四类归档审查。
- [x] 检查所有批量替换，特别是 Raft timeout、deadline、等待预算和测试 fixture 语义。

## 2. 批次 A 阻断项复验

- [x] 复验错误码扫描器：同文件多个动态 site、参数转发、site 级豁免和 stale 指纹。
- [x] 复验 ACL 双向对账：普通 keyed struct 不得误判，动态订阅不得漏报。
- [x] 复验 Raft 日志去重：具名字符串纳入身份，数字及 numeric Stringer 排除。
- [x] 复验 D7 retire 不可逆步骤；确认报告没有夸大为终态 `RETIRED`。
- [x] 检查需求/当前架构/历史架构的权威链在 CLAUDE、README 和 docs 中一致。
- [x] 核对批次 A 未完成项仍被诚实登记，未以措辞宣称完成。

## 3. e2e runner 功能与覆盖等价性

- [x] 复验非 split 模式结果计数。
- [x] 复验混合静态/动态 `exec.Command` 必须整测试 fallback，不得部分解析。
- [x] 复验原始 `-run`、tags、包路径与其余 argv 的语义保真。
- [x] 复验 parallel 包可以拥有、运行单元测试且不污染矩阵覆盖集合。
- [x] 复验 coverage identity multiset；数量相等不得替代身份相等。
- [x] 检查 parser 对别名、变量、拼接、循环、helper 和多命令形态的 fail-closed 行为。

## 4. 调度、资源与可移植性

- [x] 复验不均匀 NUMA 拓扑下 capacity-aware 分配。
- [x] 检查 heavy worker 预留是否自适应、是否可能饿死普通队列或降低实际并行度。
- [x] 检查无 NUMA、单节点、空节点、busy CPU、worker > core 等退化路径。
- [x] 核实 busy CPU 实现与文档声明一致；不能用名称暗示不存在的利用率采样。
- [x] 检查 worker affinity、`GOMAXPROCS`、子进程环境和清理/信号路径。

## 5. flake 修复真实性

- [x] 审阅四类根因证据，区分产品缺陷、fixture 缺陷、资源饥饿和端口 TOCTOU。
- [x] 检查测试 timeout 是否引用生产常量；逐项审计例外是否仍保留原 fixture 目的。
- [x] 检查 D3 观测—使用重试是否真正只在 follower 前提成立时断言。
- [x] 检查 D7 `adminForLeader` 在换主、无主和关闭路径下是否安全。
- [x] 检查端口重试是否只重试明确的 bind 冲突，避免吞掉鉴权/协议等真实错误。
- [x] 检查新增 timing guard 是否能变异失败且不会误伤合理 fixture。

## 6. 文档、CI 与发布闸门

- [x] 对齐 `CLAUDE.md`、Makefile、CI、README、testing standards 与 runner 实际行为。
- [x] 判断删除串行全矩阵 target 是否保留足够的单矩阵诊断能力。
- [x] 检查唯一发布闸门是否确实包含所有原矩阵、phase 子套件和 runner 自测。
- [x] 检查报告中的耗时、轮数、unit 数和“等权/唯一权威”结论有可复验证据。
- [x] 运行 `git diff --check`、格式化检查及文档引用检查。

## 7. 独立验证、修复与收尾

- [x] 先运行上一轮 7 个 external-review 反例，确认修复而非删除/放宽断言。
- [x] 添加或保留必要的独立边界测试，并直接修复复现的问题。
- [x] 运行相关包单测、`-race`、`make test` 与 `make lint`。
- [x] 运行 dry-run 覆盖自检和实际全矩阵 `make e2e-parallel`。
- [x] 对满载偶现风险进行重复验证，并记录样本量及剩余不确定性。
- [x] 形成以 `Pass` 或 `Fail` 开头的最终报告，列明疑惑、问题、修复和建议。
- [x] 完成全部 tasklist，暂存全部审查范围文件；确认仅 `tools/` 保持排除。
