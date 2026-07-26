# 批次 A — 外部审查 tasklist

> 日期：2026-07-26
>
> 角色：外部独立审查者。内部 plan、progress、review 与 response 仅作为线索，
> 不作为正确性证据；所有关键结论须由当前工作树、独立测试或可复现命令支持。
>
> 范围：审查开始时 `git diff` 与 `git ls-files --others --exclude-standard`
> 所列的全部暂存区外内容。开始时暂存区为空。

## 0. 基线、流程与范围

- [x] 完整阅读 `CLAUDE.md`，确认角色边界、文档权威层级、测试门与 simcluster 定位。
- [x] 粗读 `docs/requirements.md`、`docs/architecture.md`、
  `docs/distributed-broker-architecture.md`、`docs/usage.md` 的目标、硬不变量与本批次相关章节。
- [x] 阅读 Batch A roadmap、定稿 plan、progress、内审报告与处置记录，了解此前审查工作流，
  但不继承其结论。
- [x] 枚举已修改与新增文件，确认无既有 staged diff；建立 A1–A15 到文件/测试的初步映射。
- [x] 核实所有新增质量审计文档与 `tools/rescue.py` 是否属于本批次有意交付，
  是否含临时路径、凭据、主机私密信息、伪造数据或不可复现结论。
- [x] 记录审查基线（分支、HEAD、Go/tool 版本、主机身份、simcluster 可用性）和最终差异统计。

## 1. 硬边界与架构契约

- [x] 验证 `internal/proto.ProtoVersion`、cluster command version、schema version 未被意外改变。
- [x] 对比错误码、subject、JSON/wire 字段的字符串与形态，确认没有未登记的 wire 变化。
- [x] 检查所有 `INVARIANT` / `DELIBERATELY` 邻接改动，确认状态机、确定性、fence、
  session ACL、home ownership、port allocation、transfer terminal ordering 未被破坏。
- [x] 确认未修改 install/systemd/nats.conf 等部署产物；若行为影响 deploy-tier 断言，
  精确识别并运行相关 simcluster drill。
- [x] 检查文档权威层级与现实现一致：历史 v1 文档 banner 不得掩盖当前 v2 契约，
  使用手册不得把内部实现愿望写成已交付能力。

## 2. A1–A3：错误语义与轮询

- [x] A1：独立枚举错误码发射形态，验证 coverage gate 不空转、不漏局部变量/helper/
  selector/动态形态；做针对性变异测试。
- [x] A1：核对 62 个分类、`codes.go` registry、CLI classifier、admin namespace 与真实 emitter；
  检查 64/69/70/75/77 的产品语义及文档重试建议是否自洽。
- [x] A1：扫描 e2e/simcluster 对旧数值退出类与 `code=<X>` 的断言，决定是否需要相关 drill。
- [x] A2：逐调用点审查 `transferGate`，确认 `store_error` 细节只进日志、wire 文案不泄漏、
  caps/push/pull/commit/finalize 行为一致。
- [x] A3：枚举全部 `--wait` 路径，验证 `pollUntil` 的取消、deadline、tick、attempt、
  final observation、错误包装和退出类；检查遗漏的手写轮询。
- [x] A3：独立测试“取消后有界返回”、首次成功、永久 pending、step error 与 timer 泄漏。

## 3. A4/A6/A7/A13：删除、鉴权与 retire

- [x] A4：对每个被删符号做全 build-tag 引用核查；重点验证 `port.Revoke` 幂等对照语义、
  `subhttp` loopback 护栏与 `RehomeDirective` 文档/测试替代物。
- [x] A6：验证 account seed wrong-kind fail-fast 走真实启动路径，合法 seed 与 malformed seed
  行为无回归，签发 claims/audience/clock 路径未被改变。
- [x] A7：双向 ACL 对账做变异测试，验证 wildcard 不吞掉缺失授权；核实被删 subject
  没有 producer、subscriber、JetStream 捕获或仍在文档/CLI 暗示可用。
- [x] A7：评估已签 JWT 与长连接的撤回/回滚语义，确认删 grant 的安全与发布说明充分。
- [x] A13：验证 legacy `DrainNode(..., Retire:true)` 明确拒绝、活的 operation retire 路径仍受
  last-voter/catch-up/roster/raft 顺序保护，D7 gated 集成测试真实覆盖幸存路径。

## 4. A9/A10/A11/A12：并发、终态、哈希与 HTTP

- [x] A9：核对 tunnel fence snapshot 的每一维、锁域与所有比较点，做字段扰动/竞态测试，
  防止已撤销 tunnel 在 REGISTER 竞态中复活。
- [x] A10：逐路径证明 `finalizeTransfer` 的 emit→delete→cancel→remove 顺序与旧行为等价，
  watchdog owner-cancel 差异显式且错误路径不会泄漏对象、entry 或 goroutine。
- [x] A11：确认四处 token/content hash 的字节兼容、依赖方向与用途边界；测试已有 DB hash
  与固定向量。
- [x] A12：验证 loopback 策略对空 host、IPv4/IPv6、localhost、解析异常、端口 0 均 fail-closed；
  检查 bool policy 参数误用面和仓库内所有 HTTP listener 是否真正收口。
- [x] A12：验证 `Serve` 的 ctx 关闭、优雅 shutdown、错误分类、listener 关闭与 goroutine 生命周期，
  并运行 race/leak 相关测试。

## 5. A5/A14/A15：循环生命周期、状态输出与可观测性

- [x] A5：审查 `loopSet` 注册/启动/关闭/Join 的并发协议、late Go、panic、重复 shutdown、
  per-loop budget 和 `nc.Drain` 顺序；执行 race 与 goroutine/fd 泄漏验证。
- [x] A5：核对 `cluster_loops` JSON 为 additive `omitempty` 字段，不污染 `Reconcilers`
  语义，不制造假 stalled，文本/JSON 输出兼容。
- [x] A14：确认已整体撤销无收益的 bannerBuilder，现有 banner 字节与互斥/组合行为未漂移。
- [x] A15：验证 raft logger 在 production 配置中真正接线，级别映射、重复 key、peer identity、
  限速/去重边界、内存上限与线程安全。
- [x] A15：验证 leadership edge 日志不刷屏，三个 audit failure counter 的更新、并发读取、
  Prometheus 类型/命名与 snapshot/export 路径一致。

## 6. 文档、测试门与仓库卫生

- [x] A8：核对 CLAUDE/architecture/requirements/usage 的每条新增声明与代码事实，
  检查失效命令、错误码数量、退出码重试建议、release-note 提醒和文档交叉引用。
- [x] 审查新增元测试自身：非空转、自检可翻红、路径/包列表不会静默漏新增面、
  不依赖脆弱行号或本机环境。
- [x] 审查既有测试被修改/删除是否降低覆盖；独立检查 test-only helper 是否误当生产证据。
- [x] 执行相关包测试、`-race` 并发测试、D7 gated test、静态扫描与定向变异测试。
- [x] 执行提交前硬闸：`make test`、`make lint`、`make e2e`；记录耗时和任何 flake/重跑。
- [x] 根据退出码/HTTP/部署断言扫描结果决定 simcluster：不需要则写出可核验证据；
  需要则只跑相关 drill 并保存结果。
- [x] 检查 `go test` 缓存影响，关键门至少一次 `-count=1`；检查生成物、临时文件和格式化状态。

## 7. 结论与收尾

- [x] 报告以 `Fail` 或 `Pass` 开头；findings 按严重度列出文件/行号、触发条件、影响、
  证据与建议修法。
- [x] 单列疑惑、非阻塞建议、未覆盖风险、所有命令与结果；不得用内部审查结论代替证据。
- [x] 对每个 tasklist 项标记完成或说明阻塞理由，复核报告与当前最终工作树一致。
- [x] 将全部文件加入暂存区，确认 `git diff` 为空且 staged 内容完整；不 commit、不 push。

## 完成说明

- simcluster：按 V1 静态扫描确认没有命中本批次 exit-class 变化的数值断言，故没有运行无关 drill；
  证据与 sandbox Docker 限制均记录在正式报告。
- HTTP Serve：现有测试和 race 通过；直接 listener 门只覆盖硬编码三包、缺少仓库级保证，已列入报告。
- A13：legacy refusal 通过，但真实三节点 operation-retire 成功路径已丢失，判为阻断问题而非
  把 tasklist 留作“未做”。
- A15：接线与 race 通过；typed peer 反例稳定失败，metrics/JSON contract test 缺口已记录。
- 全部文件已按用户要求加入暂存；未 commit、未 push。
