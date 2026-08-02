# upgrade-safety 独立外部审查 tasklist

日期：2026-08-01

审查者：独立外部审查者（不采信内部审查结论，仅把它作为反例索引）

基线：`HEAD=a31634814aff`；审查对象为当时完整暂存区外工作树：29 个 tracked 修改、
7 个 untracked 文件，约 `+1279/-131`；tracked diff 初始 SHA-256：
`55413bfe04d46bd48af061292106214e8f6299b06a8751446d266a1ab2209f76`。

通过标准：需求、绑定架构、实现、用户文档和测试证据相互一致；升级状态机在并发、崩溃、
断电、旧版本互操作和运维误用下 fail-safe；所有可执行硬闸通过；不存在未处置的 Blocker/Major。

## A. 边界、权威文档与既有证据

- [x] 冻结并复核 staged/unstaged/untracked 边界，确认审查期间没有混入未知实现变更。
- [x] 通读 `CLAUDE.md`、requirements、当前绑定架构、deploy-tier gotchas、testing standards，
  提取本增量必须满足的需求、不变量、门禁和 simcluster 运行条件。
- [x] 阅读 upgrade-safety plan、完整内审及处置记录；逐条映射承诺到实际 diff，不把“已采纳”
  或“变异已红”当作事实。
- [x] 对照近期 external review/tasklist 的报告习惯，保证最终报告以 `Fail`/`Pass` 开头，
  区分产品缺陷、测试缺陷、文档偏差和审查疑惑。

## B. 需求、架构与 wire 兼容契约

- [x] 核对 N-1 release 窗口、ProtoVersion 纪元、broker-first 顺序、join-gate 豁免、回滚顺序，
  检查 requirements/HOW/代码错误信息/usage/ops 是否一致且无不可达承诺。
- [x] 盘点所有版本比较与 subject-version 站点，验证精确 proto 相等只在双 subject tree 未实现前成立，
  release 检查不会阻塞已部署节点回滚。
- [x] 审计 `NodeRegisterReq` 新字段、两个错误码、`UpgradeForwardedResp.NewVersion` 语义及 N-1
  旧端 default 行为；确认所有发射点、分类点、ACL/订阅和 forward-envelope 冻结面闭合。
- [x] 独立审计 wire inventory 的发现范围、类型/tag 稳定性、append-only 更新器、bootstrap/收缩绕过、
  嵌套/别名/泛型/匿名字段等边界；以真实变异或独立测试验证它会因破坏性变化失败。

## C. agent 安装事务、文件系统与供应链安全

- [x] 逐语句审计下载、限额、SHA、tar 路径/多 entry/权限、冒烟解析、临时文件清理及 URL allowlist；
  检查坏 ELF、错误架构、非零退出、恶意输出、超时与子进程清理。
- [x] 验证 prev 硬链接与 copy fallback、tmp→dst rename、marker 写入顺序的断点矩阵；在每个失败点保证
  `dst` 可用、marker 与 prev 一致、旧版本不被覆盖、错误不会误报 staged。
- [x] 检查文件权限/owner、symlink/hardlink/目录替换、同路径 alias、跨设备、磁盘满、fsync 与目录
  durability；区分“原子命名空间变化”和“掉电后持久化”。
- [x] 验证 install 互斥覆盖 HA 双 broker 并发转发、重复/迟到消息、pending/stale/corrupt/terminal marker；
  锁粒度与失败路径不得死锁、长时间阻塞或留下不可恢复状态。
- [x] 独立测试至少一个内审未充分证明的安装/文件系统反例，并核对现有测试是否因 mock 绕过生产路径。

## D. boot / commit / rollback 状态机

- [x] 枚举 `state × self SHA × prev SHA × boot_count × deadline × marker parse/read/write error`，
  检查 `decideBoot` 动作、落盘和调用方是否一致，所有终态是否可收敛且不形成 exec/restart loop。
- [x] 核对启动检查只挂 agent daemon，`version` 冒烟及其他子命令不消费预算；确认 boot budget 的
  off-by-one、deadline 边界、墙钟跳变和默认 supervisor 参数下的真实收敛时间。
- [x] 验证 register commit 只能由真正启动的新进程触发；旧进程 flip→exec 窗口、旧在途 register、
  重复 register、marker 被替换/损坏均不能伪 commit。
- [x] 验证 watchdog arm/cancel/rollback/exec 与 commit 的真实并发互斥；检查 timer/goroutine 生命周期、
  ctx 取消、迟到回调、exec 失败和 marker 写失败，不得 data race、泄漏或错误退出。
- [x] 验证 boot-time 与 watchdog 两条 rollback 路径都先校验 prev SHA，恢复后调用正确路径；无 supervisor
  的 nohup 边界、rollback_failed 观测和 committed marker 生命周期与文档一致。
- [x] 增加独立状态机/并发测试，优先覆盖现有表中缺失的真实生产调用链而非直接调用内部 helper。

## E. ctl `node upgrade` 与 fleet/canary 行为

- [x] 审计单节点默认 `--wait`、显式关闭、timeout、poll interval、baseline 捕获与 ONLINE 判据；
  NATS/JSON/node-list 错误必须有清晰且安全的处置及稳定 exit class。
- [x] 验证新 agent 规范化版本、旧 agent 完整首行、空 NewVersion、same-tag 重装、release 先变后离线、
  stale pre-dispatch row、节点消失/重现等时序不会假 COMMITTED 或假 ROLLED BACK。
- [x] 审计 `--all` 空车队、排序、第一台强制 canary、canary 失败后 untouched 计数、后续 transient/config/
  unknown 分类、上下文取消和部分成功输出；确认所有 broker 包裹错误形状均被正确归类。
- [x] 对照 command-tree golden、usage 输出样例和 error hints；检查 flags 的组合约束、默认值、脚本兼容、
  退出码语义与文档是否真实可由代码产生。
- [x] 增加独立 CLI 反例测试，覆盖已有测试未表达的轮询或 fleet 时序。

## F. broker、集群与可观测性

- [x] 审计 broker register 对 UpgradeState/Detail 的输入信任、日志注入/敏感信息/长度、旧端缺字段、
  重放与 agent-reported 措辞；确认不意外进入 raft 持久状态或造成 FSM 分歧。
- [x] 审计 node upgrade forward 的版本门、错误包装、超时、响应 schema 与 HA 多 broker 路由，确认
  ProtoVersion bump 拒绝路径给出可执行的 reinstall 指引。
- [x] 复核 join cluster release 精确门的豁免边界与 node upgrade N-1 回滚契约不矛盾；运行相关 broker/
  cluster 测试及必要变异。

## G. 测试质量、文档与门禁

- [x] 审查所有新增/修改测试的 oracle、同步、超时、共享全局变量恢复、并行安全、平台依赖和假绿风险；
  确保所谓 e2e 不绕过关键生产入口。
- [x] 检查新增源文件/测试命名、origin 注释、`context.Background()` 站点、结构预算放宽、golden/账本
  修改是否符合项目纪律且理由充分。
- [x] 核对 usage/broker-ops/architecture/requirements/CLAUDE 的命令、路径、空间估算、时序、已知 GAP、
  首跳风险和 rollback 操作；检查章节位置与交叉引用。
- [x] 运行 `git diff --check`、受影响包单测、独立 reviewer tests、`-race` 与泄漏相关门。
- [x] 因本轮改动 wire/闸门，运行 `make gates`；随后依项目发布标准分别运行 `make test`、
  `make e2e-parallel`、`make lint`，保存真实退出码，不以管道尾命令掩盖失败。
- [x] 阅读 simcluster mandate/inventory 与 server 信息，判断是否存在匹配本增量的 deploy-tier drill；
  若存在则只跑相关 drill 并检查前后残留，若不存在则在报告中给出不运行的契约依据。

## H. 结论与交付

- [x] 将每个发现按严重级别、位置、失败场景、证据和建议修法写入独立外审报告；明确疑惑与建议。
- [x] 回填本 tasklist 全部项目；复核最终 diff 与审查边界变化，列明审查者仅新增的测试/文档。
- [x] 报告首行写 `Fail` 或 `Pass`；结论以未关闭的最高级问题和硬闸结果为准。
- [x] 将所有文件加入暂存区，确认没有 unstaged/untracked 内容，并记录最终 staged 范围。
