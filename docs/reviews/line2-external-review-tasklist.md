# 线二 · 质量闸门加固 — 独立外部审查 tasklist

日期：2026-07-30

审查者：独立外部审查者

基线：`HEAD=92e01a45bb70a18b3ab77e47ab38c6ec3ccb6249`

边界：以已暂存的 8 个改名/删除为候选基座，独立审查其上的 103 个 tracked
未暂存文件（`+2427/-279`）和 19 个初始未跟踪文件。初始 tracked diff SHA-256：
`0e8f6a87710332841f8504537e3e0c8217464e95fba9aa4db14b8d41720fe717`。
`line2-plan.md` 与 `line2-review.md` 只作索引和待证声明，不作为正确性证据。

## A. 项目契约、范围与可追溯性

- [x] 读取 `CLAUDE.md`、WHAT/HOW 权威文档、测试规范、simcluster Mandate、设备/宿主说明及历史外审样例。
- [x] 将 plan 的 DO / REJECT-FOREVER、内部审查 59 个编号块、额外 §5/§6 finding 与实际 diff 双向对账。
- [x] 核对审查边界在结束前是否漂移；区分开发者初始改动、审查者新增测试与报告。
- [x] 检查删除/改名是否完整保留行为、注释、测试断言与活文档引用；冻结报告不得被追溯篡改。

## B. 新质量闸门的真实性与抗逃逸性

- [x] 审查 `.golangci.yml` 三层策略、`make lint` fail-closed、build-tag 覆盖、豁免粒度、陈旧豁免反查及注释税。
- [x] 审查 `vet-tags`、`gates`、tag SSOT 对账、跨平台负 build constraint，并用变异证明不能空转或吃缓存。
- [x] 审查结构预算四维度、量化语义、注释剥离、cobra 分类、golden 收紧更新、理由保留、幂等与新债务拒收。
- [x] 审查 layering 合表是否逐子句保留四份删除测试，传递/直接依赖语义、模块版本前缀及 cache ballast 是否可靠。
- [x] 审查 TLS 配对门能覆盖复合字面量、赋值式、变量值、生产站点精确集合、函数级账本与同函数新增站点。
- [x] 审查 enum switch 门的枚举家族发现、成员完备性、default 规则、函数级豁免与所有自有 iota 枚举反向覆盖。
- [x] 审查 docs wire、docs layout、nolint 指令、gate registry、error-code 双向、命名冻结和 promised-guard 的非空泛/反向断言。
- [x] 对每道新增或加固闸门至少设计一个“实现被删/扫描面缩小/账本放宽仍应变红”的独立反例；必要时新增职责命名测试。

## C. 生产行为变化

- [x] 审查 `cluster status --watch --json` 的成功、fetch-error、marshal-error、换行、JSONL 与 human separator 契约。
- [x] 审查 Y2 错误码拆分的真实发射条件、wire 文案、exit taxonomy、fleet upgrade continue/stop 分类及文档一致性。
- [x] 审查 PTY errno 分类（含 ENOSPC/ENOMEM/EAGAIN）、SetSize 上下文、终态/瞬态边界和平台差异。
- [x] 审查 upgrade 下载的 HTTP 状态、大小上限、流读取/关闭、原子替换与重试分类。
- [x] 审查 attach subscribe 失败路径、资源清理和错误码位置门。
- [x] 审查 `js_store` stat/read-dir fail-closed，symlink/权限/非目录/TOCTOU 及三个调用方补救提示是否真实。
- [x] 审查 incident bundle 脱敏失败是否 fail-closed 且显式标记 partial/errors，防止泄密或静默丢证据。
- [x] 审查 ssproxy listener 类型断言的赋值顺序、失败清理和半构造状态。
- [x] 审查 node classification、cluster wait/settle、topology/health switch 新 case 是否保持完整枚举语义。

## D. 并发、资源生命周期与分布式不变量

- [x] 审查 broker/agent/tunnel/reconcile/transfer/cluster 路径中的 context、goroutine、channel、timer、body/listener/file 生命周期。
- [x] 审查 `home_delivery` ack/状态修改，避免数据竞争、重复终态、无读者状态或锁内副作用。
- [x] 审查 d8 forward churn 泄漏门是否真的接入被发布套件、基线稳定、失败后仍清理、不会把正常后台 goroutine 当泄漏。
- [x] 审查 cluster health “绿蕴含真实 HA”覆盖面与 spoiler 自检，防止穷举声明大于实际输入空间。
- [x] 审查 wire/ACL/subject/role/port/raft Apply 的确定性、身份隔离、版本 SSOT 与 fail-closed 边界。

## E. 测试删改、文档与架构一致性

- [x] 对四份删除的 regression 测试逐个测试名/断言/依赖规则建立收据，确认 replacement 不弱化。
- [x] 核对所有测试改名/函数账本的路径级唯一性、published cap、产品名例外与误报/漏报。
- [x] 检查测试中删除断言、放宽等待、移除边界检查或忽略错误的每一处 `-` 行是否有正当理由。
- [x] 核对 `CLAUDE.md`、usage、architecture、deploy-tier gotchas、reviews INDEX 与真实命令、路径、错误码、wire 版本一致。
- [x] 检查 process artifact 归档、跟踪状态、`.gitignore` 例外与活文档引用策略。

## F. 独立验证

- [x] 运行 `git diff --check`、格式、构建、vet 与适当的跨平台 build-tag 验证。
- [x] 运行新增闸门及其独立变异测试；确认不依赖 Go test cache 的陈旧结果。
- [x] 运行受影响包的 focused tests；并发/隧道/PTY/reconcile/传输/Raft 相关包运行 `-race` 与泄漏门。
- [x] 运行 `make gates`、`make test`、`make lint`，保留失败身份并判断是否为产品、测试、环境或审查反例。
- [x] 运行唯一全矩阵发布闸 `make e2e-parallel`；只在其报错时按规范串行定位单项。
- [x] simcluster 决策：先证明这批改动是否触及真实部署栈；若只改 drill 文案且无部署行为变化，跑 hermetic
  harness/oracle 即可；若触及集群生命周期或需要验证 drill 90/42 的真实契约，则在本机用
  `test/simcluster/local.sh` 跑相关单 drill，并核对起止清洁状态。

## G. 报告与收尾

- [x] 报告第一行写 `Fail` 或 `Pass`，按严重度列 finding、可复现证据、疑惑、建议与验证结果。
- [x] 每个 finding 精确到职责/符号/行，区分阻断问题、测试基础设施问题、文档问题和非阻断建议。
- [x] 全部 tasklist 完成后重新计算边界与状态，确认报告没有把内部结论当证据。
- [x] 将所有文件加入暂存，确认无未暂存或未跟踪文件，并停止。
