# g75-g78 部署默认值外部审查 tasklist

> 2026-08-11。审查对象是本轮开始时 `HEAD` 之外的全部暂存区外内容：39 个已修改文件、9 个未跟踪文件；开始时暂存区为空。
> 外审不采信 `g75-g78-deploy-defaults-review.md` 的结论，只把其中的实现主张和未完成项当作待证伪假设。外审只可新增独立测试与报告，不修改生产实现。

## A. 基线、契约与变更面

- [x] A1. 完整读取 `CLAUDE.md`、权威链（requirements / distributed-broker-architecture / deploy-tier-gotchas）、使用与运维文档、simcluster mandate、测试规范和相关历史外审习惯。
- [x] A2. 固化 `git status`、tracked/untracked diff、协议/CLI/golden/台账变化，确认没有把既有暂存内容或无关用户修改混入审查。
- [x] A3. 从 plan、文档和代码分别重建 #75–#78 的需求矩阵；逐项核对升级顺序、N-1 四象限、默认值、回滚边界和 non-goals 是否一致。

## B. #75：严格配置、配置检查与日志可见性

- [x] B1. 审查 `serveconf.Load` 的 strict YAML 语义：空文档、多文档、未知/错嵌套键、类型错误、路径包装、TLS inert stub 与全部调用点。
- [x] B2. 审查 install.sh 真模板与 schema 的双向配对测试，特别是注释态 cluster 块、shell 展开、默认 observability sink 和未来模板漂移的 vacuity 风险。
- [x] B3. 审查 `tether serve --config-check` 是否覆盖实际启动前的全部配置校验且严格零副作用；核对 cluster seam probe 的错误传播和错误分类。
- [x] B4. 审查 file sink breadcrumb 的成功/失败、stderr 可见性、敏感信息、并发/轮转和 agent/serve 共用路径。

## C. #76/#77：install.sh 生命周期与 journald 默认

- [x] C1. 逐分支审查 broker/agent、dry-run、`--no-enable`、无 systemd、enable 失败、重复安装、卸载、离线卸载；确认 never-start、失败传播、banner 与真实状态一致。
- [x] C2. 审查 journald 显式设置扫描面（优先级目录、空白/注释/重复键、自家 stale 文件）、`df` 失败与容量边界、原子写/权限、幂等、dry-run、卸载所有权和路径穿越面。
- [x] C3. 独立运行 shell 静态检查与 p10 hermetic 测试；补反例覆盖内审未证实或仅靠字符串断言的行为。
- [x] C4. 审查文档中的新装、存量 retrofit、配置预检、启停/卸载和 journald 生效说明，避免会覆盖现网配置或触发副作用的指引。

## D. #78：agent 首拨退避

- [x] D1. 重建 `proxyStartLocked` 状态机和所有入口/出口；核对锁、时钟、Tracker class、失败计时、成功/teardown/reconnect reset，以及窗内零 teardown/零 ACK/零拨号副作用。
- [x] D2. 验证 dial identity 的 gen/epoch/port/token/homeEpoch 每一维 bypass 与清零；覆盖 keyset-only directive、token rotation、rehome、off/on、长拨号和时钟跳变。
- [x] D3. 审查错误分类和日志语义（WARN/Debug/announce-once/recovered suppressed），确认配置故障不会 5s 刷屏、瞬时故障不会永久静默、并发访问无 race。

## E. #78：broker REGISTER 日志抑制

- [x] E1. 审查 TLS 后 read-REGISTER 的真实调用链、鉴权成功点与 remote 属性，确保未鉴权输入不能伪造 recovery。
- [x] E2. 验证 per-class Tracker 在交错类别、Cap 到期、成功恢复、持续攻击、多连接并发和关闭路径下有界且不会被攻击者重武装。
- [x] E3. 检查锁粒度、Tracker 非并发安全约束、日志字段基数与内存/CPU DoS 面；运行 race 和独立行为反例。

## F. #78：proxy opt-out 与 N-1

- [x] F1. 核对 agent YAML 三态、wire additive/omitempty/零值字节等价、register 单载以及 raft payload 未扩展；执行 wire inventory 与跨版本思维矩阵。
- [x] F2. 单机路径：register fold、既存 `__proxy__` 释放、`proxyOpMu` 锁序、ready 清理、事件真实性、status hint 生命周期和 opt-in 恢复。
- [x] F3. cluster 路径：capability 查询、reconcile teardown/PlanFree 失败重试、rotation/reaper、leader 变更、旧 leader/旧 broker 混版残余及 status 整行缺席是否被诚实呈现。
- [x] F4. agent 对旧 broker 的 belt：启动 footprint 清理、directive gate、一次性日志、零拨号与恢复；检查写入新 YAML 键后的自动回滚砖风险和文档警告。

## G. 测试质量、门禁与变异

- [x] G1. 阅读所有新增/修改测试，检查是否只证明复制实现、是否存在恒等式/空扫描/错误 oracle、时间脆弱、平台依赖、泄漏、过程命名或遗漏负例。
- [x] G2. 运行受影响包、`-race`、`make test`、`make gates`、`make e2e-parallel`、lint 与 `git diff --check`；失败逐一归因，禁止把 setup failure 记为产品 verdict。
- [x] G3. 对关键新守卫做独立反例/敏感性审查；baseline 已由 config-check、journald ownership、REGISTER 计数和 drill shell-scope 四个反例打红。因外审角色不修改生产实现，且 drill 78 连合法 baseline verdict 都不存在，没有把内部报告中的 mutation 声明冒充独立证据；完整实现 mutation 列为修复后的放行门。

## H. simcluster / deploy-tier

- [x] H1. 阅读 sim cluster server 资源说明和 drill mandate，确认目标服务器状态、版本/容器残留、DNS/日志 oracle 前置条件。
- [x] H2. 在 simcluster 运行 drills 32、93、78；保存每臂 verdict 和证据，确认 arm C 的 TLS 构造边界没有被 raw TCP 假覆盖。
- [x] H3. 评估可恢复变异轮的前置条件：32 baseline GREEN，但 78 缺 `drill_begin/end` 且 D4 已证假绿，无法产生可比较 verdict；在无效 baseline 上做 mutation 不构成敏感性证据，故不伪造“真实红”。将 32/78 mutation 明确列为修复后放行条件；最终 simcluster status 为空，无隔离实例残留。

## I. 收口

- [x] I1. 逐项复核代码—测试—文档—台账四方一致，整理按严重度排序、可定位且可复现的 findings、疑惑与建议。
- [x] I2. 写外审报告，第一行给出 `Pass` 或 `Fail`；记录所有命令、退出码、sim verdict、未覆盖边界与放行条件。
- [x] I3. 更新本 tasklist 为真实完成状态，复查 diff/报告引用，然后执行 `git add -A`，确认所有文件均已暂存且工作区无未暂存内容。

## 执行摘要

- 静态/契约：完成 A–F 全面逐行复核；四个独立反例文件覆盖 config-check 双向 parity/纯格式漏验、foreign journald ownership、REGISTER `suppressed_since_last` 与 drill fresh-shell 假绿。审查期间 F2–F7 及 F9 均被主实现修复并经原反例/门禁转绿；F1 仍漏 malformed NATS URL 与非法 metrics DNS host。
- 门禁：最终 simcluster `run-all.sh`、结构预算、diff/gofmt PASS；`make gates` 仅由 F1 两个差分阻断，`make e2e-parallel` 为 97/99（F10 同一 broker shard 在 D4/D5 红），全新 cache 的 `make lint` 仅 F11 QF1011 红；受影响包 race 未发现 data race。
- deploy tier：当前源码重建镜像后，32=GREEN(54 pass)，93=INCOMPLETE(65 pass/1 gap)，修补后 78=INCOMPLETE(22 pass/1 gap，与台账一致，A=6/4/3、D4 真实读取状态)；结束后无隔离容器残留。
- 最终结论与完整命令证据见 `docs/reviews/g75-g78-deploy-defaults-external-review.md`。
