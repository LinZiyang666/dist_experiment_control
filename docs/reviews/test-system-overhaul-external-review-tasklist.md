# 测试体系革新增量外部审查 tasklist

> 2026-09-01。对象为当前全部 unstaged 变更；staged 区为空时以 `HEAD` 为基线。内部 plan、内审报告和
> “全绿”记录只作为索引，不作为外审证据。本轮只允许新增独立测试/诊断与审查文档，不修改产品实现；
> 完成后按用户要求执行 `git add -A`，因此最终报告必须明确所有问题仍由主进程处置。

## A. 边界、契约与收据

- [x] A1. 固化 `HEAD` / staged / unstaged 边界，枚举新增、删除、改名与生产代码改动；确认没有把既有用户改动误当外审产物。
- [x] A2. 通读 `CLAUDE.md`、WHAT/HOW 权威文档、测试规范、overhaul baseline/plan/内审报告及相邻外审样例，建立需求—设计—实现映射。
- [x] A3. 核对 B0–B6、内审 L1–L6、未做项 D-1–D-23 与实际 diff；凡“已关闭/已验证”均独立重建证据。
- [x] A4. 审查测试身份 golden、矩阵 golden、递减账本、例外表、删除的 release/P11 门与文档归档，证明没有靠删测试、放宽阈值或转移扫描面制造绿色。

## B. CI、发布与执行器

- [x] B1. 审查 `.github/workflows/ci.yml`、删除的 `release.yml`、Makefile 与文档，验证唯一发布入口、tag 可达链、`needs`/`if`/权限、失败传播、artifact 和本地/CI 命令一致性。
- [x] B2. 对 CI workflow 静态门做独立反例：注释污染、映射/inline YAML、`always()`/`continue-on-error`、workflow_call/dispatch、tag/branch 过滤和依赖链绕过。
- [x] B3. 审查 parallel runner 的 AST 提取、whole/parsed unit、分片、筛选、超时和覆盖 self-check；对 argv 变体、变量/append、`CommandContext`、`-run`、tag、race 与 pass-through flag 做差分。
- [x] B4. 审查运行时 closure-hash 去重的等价关系、键、错误路径、fold note、矩阵归属与 GOFLAGS shuffle；确认不跨 race/tag/run/whole/环境语义错误折叠，不会漏跑或假 self-check。

## C. 新机械门禁的可靠性

- [x] C1. 审查 test inventory / matrix inventory / test-layout map：解析范围、身份唯一性、只增不减语义、重命名/删除流程、whole matrix 字面量和目录双向对账。
- [x] C2. 审查命名、ctx-background、build-tag、layering、gate-standards 与 simcluster gate-set：正反向集合、账本键粒度、cap、注释/字符串假命中、别名和新增文件逃逸。
- [x] C3. 审查 determinism 门：leader premise、sleep barrier、产品计时、raft timing、泄漏覆盖、fuzz corpus；重点验证 AST 祖先关系、IIFE/闭包/循环/select、方法别名、死 helper 与非空控制。
- [x] C4. 对每个新门检查 G1/G2/G2b/G3/G4/G6：同文件控制测试是否真实调用生产谓词，负控是否会红，正控是否排除合法写法，目标清空时不制造错误地板。
- [x] C5. 独立运行代表性变异或最小反例测试；发现扫描器缺陷时优先添加职责命名的 regression test，并用 `// origin:` 记录本报告 finding。

## D. 性质测试、harness 与少量产品接缝

- [x] D1. 审查 `Broker.now`、probe cache 时间域、background probe budget atomic seam 与 `LeaseGrantWindow` 导出：生产路径零行为漂移、测试并行隔离、goroutine 清理和 race 安全。
- [x] D2. 审查 lease/port/FSM/subject/AEAD/invite/restore/tunnel 等 property/fuzz 测试的模型独立性、操作族覆盖、双向比较、随机路径生效计数、种子语料和错误 oracle。
- [x] D3. 审查 clusterharness/stackharness/internal testharness 的 import 边界、真实调用方、leader 重试语义、端口/进程/环境生命周期与 cleanup；检查批量 `t.Parallel()` 是否引入共享全局、固定端口、env 或信号冲突。
- [x] D4. 审查 chaos/E2E 中 sleep 替换、心跳年龄、资源分配、超时和 flake 分类；确认修复的是被测前提而非仅延长等待。

## E. 文档真值、simcluster 与验证

- [x] E1. 对照 requirements、分布式架构、deploy gotchas、usage、test README、testing standards 与 CLAUDE 门表；核验所有命令、数字、路径、层级和“保证/未覆盖”措辞。
- [x] E2. 审查 simcluster `run-all.sh` 的 shebang 分派、完整集合、文件分类与失败传播；运行 hermetic gate，并检查是否需要/适合使用 sim cluster server 的 live drill。
- [x] E3. 运行 targeted 普通/race/fuzz seed 测试、`go test` 受影响包、`make gates`、`make test`、`make e2e-parallel`、lint、gofmt 与 diff checks；每个红区分代码缺陷、测试缺陷、环境/setup 和已登记 flake。
- [x] E4. 若资源允许，至少复跑一组同命令多次/带 `-shuffle`/race 的非确定性压力；记录 seed、次数、耗时与任何不稳定。

## F. 报告与交付

- [x] F1. 写独立外审报告，首行/标题为 Fail 或 Pass；按严重度给出可复现路径、影响、证据、修复要求，并单列疑惑、建议、未覆盖面和发布建议。
- [x] F2. 逐项回读 tasklist 与报告，确保勾选只代表真实完成；运行 `git diff --check`、检查最终状态与 staged 边界。
- [x] F3. 按用户要求执行 `git add -A`，确认所有文件已暂存、工作区无 unstaged 内容，并在报告记录该事实。

## G. 主进程处置（2026-09-01，外审后）

- [x] G1. F1–F5 全部采纳并修复；外审四组反例原样保留为控制样本，修复后全部转绿（回复见报告末「主进程回复」）。
- [x] G2. 疑惑与建议 1–5 全部采纳（WithLeader 措辞、`-v` 进键、闭包 hash 只取 stdout、B5 按缓存 verdict 归类、port 比较含 name/eol）。
- [x] G3. 三硬闸前台重跑：`make gates` rc=0、`make test` rc=0（67 包）、`make e2e-parallel` ALL PASS（3m46s，17/17）；`make lint` 0 issues。
- [ ] G4. 等待外审复审；主进程未 `git add`（工作区 = 修复后，index = 外审看到的树）。
