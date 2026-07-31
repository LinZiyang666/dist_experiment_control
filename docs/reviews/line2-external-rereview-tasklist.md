# 线二 · 质量闸门加固 — 独立外部复审 tasklist

日期：2026-07-30

基线：上一轮外审后已全部暂存的候选树；`HEAD=92e01a45bb70a18b3ab77e47ab38c6ec3ccb6249`。

开发者本轮边界：20 个初始未暂存文件，`+1511/-163`；初始 diff SHA-256：
`08df0e660fd9583059c2d942daf0727b2714eee03b94fc54b755d546dfe0d059`。
开发者写入旧外审报告的回复只作实现索引，不作为修复成立的证据。

## A. 边界与旧 finding 闭环

- [x] 核对 B1 的历史 commit 锚点真实、永久可达、提交后可运行，且不存在 shallow clone/无 git 环境退化。
- [x] 核对 M1 HTTP 状态表、wire code、exit class、fleet 行为、Retry-After 与文档完整闭环。
- [x] 核对 M2 TLS selector-base 配对对多赋值、复杂表达式、alias/重赋值和 unknown bucket 的安全方向。
- [x] 核对 M3 required build-tag 账本双向对账、组合 tag、runner consumer 与 suite 删除变异。
- [x] 核对 M4 golangci path regex 解析与 Go declaration 路径匹配同 golangci 语义一致，避免解析器假绿/假红。
- [x] 核对 M5 非目录 store fail-closed，并复查 symlink、权限、TOCTOU 和三个 destructive caller。
- [x] 核对 M6 golden bootstrap、tighten-only、sentinel 子进程、原子/失败不写和维护提示。

## B. 本轮新增生产行为

- [x] 审查 watch JSONL error/report discriminator、schema/version、marshal fallback、字段冲突和兼容性。
- [x] 审查 `ExitError.Code` 从 wire 到 fleet classifier 的结构化传播，前缀剥离、未知码、transport fallback 与 exit code。
- [x] 审查 cluster wait 状态分类、超时/永久错误和 human/JSON 两条路径的一致性。
- [x] 审查 offline peer liveness advice 的 DNS/IP 解析预算、保守性、错误吞吐、IPv4/IPv6 和安全拒绝不变式。
- [x] 审查 simcluster fake-DNS preflight 的执行位置、网络创建/清理、并发实例、override 语义及无泄漏。

## C. 门禁抗逃逸与独立反例

- [x] 逐条复现开发者声明的 mutation，不独信报告文字；对未覆盖边界增加独立变异或测试。
- [x] 检查所有新增测试的非空泛性、生产调用路径、测试间全局状态恢复、缓存隔离与平台稳定性。
- [x] 检查新增 regex/AST/YAML 手写解析器的 grammar 边界、错误时 fail-closed 与精确账本漂移。
- [x] 检查文档、注释、错误提示是否夸大验证范围或与真实行为/命令不一致。

## D. 验证与部署

- [x] 运行 `git diff --check`、格式、focused tests、上一轮两条红测和所有新增 gate 自检。
- [x] 运行 `make gates`、`make test`、受影响包 `-race` 与 `make e2e-parallel`。
- [x] 运行 simcluster hermetic harness；检查当前宿主 fake-DNS 前置守卫能否稳定 fail-closed。
- [x] 判断 drill 42 是否具备可信运行前提；不以 override 制造无意义绿色。

## E. 两阶段交付

- [x] 形成开发者候选复审结论；若仍有问题，按严重度和证据记录。
- [x] 将开发者候选与本 tasklist/阶段报告全部加入暂存，确认该快照无未暂存残留。
- [x] 在暂存快照之上直接修复仍存在的代码问题，并增加职责命名回归测试。
- [x] 修复后重新运行相称验证，形成最终报告。
- [x] 最终确认审查者修复和最终报告留在暂存区外，无未跟踪文件，便于 `git diff` 对比。
