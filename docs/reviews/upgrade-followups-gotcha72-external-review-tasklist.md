# upgrade follow-ups + gotcha #72 独立外部审查 tasklist

日期：2026-08-02

基线：`HEAD`（开始审查时 index 为空；边界为全部未暂存及未跟踪候选文件）

角色边界：本审查不信任内部 plan/review/绿测结论；它们只提供待验证的主张。审查者不修改产品实现，
但可添加职责命名的独立测试、运行本地和 simcluster 验证，并产出本 tasklist 与最终报告。

## 范围建立与权威契约

- [x] 核对 `git status`、`git diff`、`git diff --cached`，确认 index 为空，并枚举 18 个已跟踪修改与
  9 个未跟踪候选文件；粗分 #72 teardown、升级 drill、simcluster fault/ledger、manifest 闸门和文档面。
- [x] 完整阅读 `CLAUDE.md` 的工作流、角色、闸门、测试和 simcluster 约束；确认不存在更深层
  `AGENTS.md`，并以 `requirements.md` → `distributed-broker-architecture.md` /
  `deploy-tier-gotchas.md` → 历史 `architecture.md` 的权威链校准结论。
- [x] 阅读相关 requirements、当前架构、usage/broker ops、testing standards、simcluster README 与
  既往外审 tasklist/report，提取连接恢复、N-1、升级、退出码、部署层真实性和报告格式契约。
- [x] 独立核对三个候选 plan/review 的范围、明确不做、验收条件、变异承诺和已承认 gap；任何
  `FIXED`/`COVERED`/`GREEN` 均重新取证。

## #72 teardown 产品状态机

- [x] 从 `Run → session → connectNATS → NATS callbacks → fireRedial/rebuildOntoVoter → finalizer`
  逐帧追踪正常关停、父 ctx 取消、主动 rehome、broker silence、stuck reconnect、注册失败和 successor
  session；核对每条入口的 intent、rebuild 标志、cancel、cleanup、close、poison、escalate 顺序。
- [x] 审计 `sessionFinalizer` 的发布/清除与 `sync.Once` 屏障：并发 callback、session defer、迟到 reconnect、
  successor connect window、无 finalizer fallback，以及 cleanup 注册与 finalizer 启动之间的竞态。
- [x] 全仓搜索 `nats.Conn` teardown/订阅清理站点，确认所有可能拿 `nc.mu` 的 `Close`、`Drain`、
  `Unsubscribe`、flush/wait 路径都进入预算，不存在 LIFO defer 在 ladder 之前重新制造无界阻塞。
- [x] 审计 `connTracker`：现有 CustomDialer 包装、默认 dial 语义、代理/TLS 可选接口、context/timeout、
  连接登记容量、旧连接淘汰、late dial、sticky poison、nil/typed-nil conn、并发 Dial/poison 与资源回收。
- [x] 对照 vendored/pinned nats.go 真实代码复核 `nc.mu`、DNS/TLS/WS、CustomDialer timeout 与
  reconnect 行为；不接受仅来自候选注释的第三方库事实。
- [x] 审计 S5 escalation 与升级状态机交互：`upgradeExePath`、deleted inode、pending marker、
  `recoverFromFailedExec`、self-exec 参数/环境/PID、setsid/systemd、受管子进程、shutdown exit 91、
  测试 seam 返回后的活 goroutine，以及“≤60s”承诺是否对所有路径成立。
- [x] 审计共享可变预算 seam、并行测试与 `-race` 安全；检查日志/退出码是否真正接入 CLI exit taxonomy、
  监控和结构闸门，文档是否夸大检测时延、恢复边界或 WSS 覆盖。

## #72 测试强度与独立反例

- [x] 逐个审阅 `conn_teardown_test.go` 与 leak test 的非空性、两侧性、真实帧保真度、时间断言、
  goroutine/fd 基线和清理；核对 plan A–F、M1–M7 的承诺与实际测试一一对应。
- [x] 检查现有 agent/roster/proxy/reconnect 测试是否因新 finalizer seam 改变语义或漏设 live-session
  fixture；覆盖 clean shutdown、register/subscribe 中途失败、evict、handler drain 和 repeated rebuild。
- [x] 添加 reviewer-owned 职责命名测试，优先钉住 cleanup-defer、cleanup-registration race、intent 竞争、
  tracker 淘汰/代理接口、escalation 返回等可疑高风险分支；测试失败作为 finding 证据，不改产品代码。
- [x] 运行 agent 聚焦测试、重复 timing-sensitive 测试、`-race` 和泄漏门；做小型 mutation/counterexample
  验证，区分真实红、环境红和测试夹具红。

## 升级 follow-up 与 drill 31/33

- [x] 对照产品 `node upgrade` 实现逐条验证 drill 33 的 B rollback、C upgrade-domain、A success、C2
  release 证据链；核对 marker 状态、boot_count、SHA、PID/ExecMainStartTimestamp、register outcome、
  `.prev` 生命周期、ctl 等待与错误文本均不能被错误分支或旧日志满足。
- [x] 审计双 unit/共享二进制拓扑是否真实建立同一 upgrade domain：HOME、SID、nkey、marker/path、systemd
  restart、允许列表、同 inode 与 A/C2 之间的不可逆共享副作用；检查 cleanup 是否恢复宿主状态。
- [x] 审计 SYN-only fault 的方向/协议/conntrack 语义和 self-proof，确认 established 注册确实存活、所有新
  agent 连接确实被切断，且不是 DNS、broker、artifact 或 ctl 旁路决定结果。
- [x] 核对 drill 31 从 skip-continue 改为 canary-abort 是否与当前产品契约一致；验证 untouched oracle、
  enumeration 顺序、timeout=0、配置错误与 transient 分类，不把不存在的 per-node 行当作“未 dispatch”证明。
- [x] 审计共享 `upgradecfg.sh` 的 shell quoting、YAML 层级、幂等性、权限、严格 loader、不同 HOME/SID/unit
  参数和 readiness 函数作用域；确认抽取没有改变 drill 31 行为。
- [x] 核对 gotcha #73 的威胁模型、architecture/usage/coverage inventory 与 expected verdict owner；判断
  `INCOMPLETE` 是否诚实，是否遗漏更严重的产品防御或可恢复性问题。
- [x] 审阅 r9d non-vacuity 新增 oracle 的 extract/stub 保真度和 mutation 强度；补充独立 shell 静态/动态
  测试，覆盖 shell 函数作用域、后台 rc 文件、空变量、日志陈旧匹配和 false-green 反例。

## drill 98、fault helper 与 simcluster 台账

- [x] 依据 simcluster Mandate 审计 drill 98：基线 live edge、单 peer 注入、黑洞/存活 self-proof、impact、
  heartbeat watermark、另一 voter landing、PID/escalation 分类、heal 与 WSS `not_covered`，禁止把普通
  nats:// failover 冒充 #72 原始 WSS close wedge 的产品修复证明。
- [x] 核对 `RECOVERY_BUDGET=330` 的库默认、检测起点和两次顺序 poll 是否造成最多 660s 或错失同一 SLA；
  检查“conn 离开旧 broker”是否可能由服务端观测过期/监控失败假满足。
- [x] 审计 `fault_partition_peer_on` / `fault_synblock_on` 的链生命周期、双向规则、IPv4/IPv6、重复调用、
  中途失败原子性、off/cleanup、多个并存 fault、shell 注入与 docker DNS/IP 解析。
- [x] 核对 README、coverage inventory、drill-costs、expected-verdicts/log 和 gotcha ledger 的计数、owner、
  状态与实际脚本完全一致；运行 ledger crosscheck、shell syntax、Mandate/nonvacuity 自测。
- [x] 查看 simcluster server/宿主状态与资源；在可行范围运行最小相关 drill（优先 98 和争议最大的 33/31），
  记录 run id、断言计数、耗时、判决和 nc_gap；不以内部历史 run 代替独立证据。

## manifest、架构闸门、兼容性与代码卫生

- [x] 审计 `TestManifestNoSecrets` 的新 token 扫描是否仍能检出全部合法 nkey seed 类型、短/嵌套/转义值、
  key 泄漏和非 JSON 表示；验证排除 account public key 不会变成通用绕过。
- [x] 审计 enum family、leak anchor 与 structural budget golden：新增方法数来源、预算放宽合理性、更新方向、
  注释承诺和 gate 自对账；确认修改 gate 没有为了候选实现而降低不变量。
- [x] 静态搜索 touched call graph 中的 ignored errors、裸 ctx roots、time.After/timer 泄漏、锁顺序、数据竞态、
  typed nil、死 helper、重复注释/陈旧行号、未覆盖 exit code、非 POSIX shell 与 unsafe quoting。
- [x] 检查 gofmt、shell syntax、`git diff --check`、go vet/build、聚焦 package tests、`make gates`/`make test`/
  `make lint`；按项目规则评估是否需要一次 `make e2e-parallel`，不并发运行 golangci-lint。

## 报告与收口

- [x] 对每个 finding 复核可复现性、上线影响、严重级别、精确文件/行号和最小修复建议；把疑惑、问题、
  建议与 residual coverage 明确分开，避免把未证实推理写成事实。
- [x] 生成 `docs/reviews/upgrade-followups-gotcha72-external-review.md`，首行严格为 `Fail` 或 `Pass`，包含
  边界、结论、严重度排序 findings、独立测试、simcluster 证据、疑惑、建议和未覆盖面。
- [x] 完成全部 tasklist；若受环境/权限/时长阻塞，记录已尝试证据与对结论的影响。最终确认审查者只新增
  tasklist/report/独立测试，没有修改候选产品实现，也没有把工作区用户改动误纳入审查者产物。

## 大改后重新审查与授权修复（2026-08-02）

边界：先对开发者大改后的全部未暂存内容独立复审；完成复审后把该候选基线加入
index。根据用户的明确授权，审查者可直接修复本轮发现，但所有修复、新回归测试和最终报告必须
留在 index 之外，便于与开发者基线直接对比。

- [x] 重建 `HEAD / index / worktree` 三层快照，逐条复验原 F1–F8 的开发者回应，不信任报告中自报的绿测。
- [x] 重跑 reviewer Go/shell 反例、`git diff --check`、agent/broker `-race`、simcluster hermetic suite、
  `make gates`、`make test` 和 `make e2e-parallel`，并独立核对输出。
- [x] 追踪初连成功到 finalizer 发布之间的窗口，核对 `ConnectedUrl`/`nc.mu`、立即断连
  callback、watchdog 清理与父 ctx 关停的全部竞态排列。
- [x] 复核 S5 与升级 marker 共享域：同 host sibling 身份、父关停/rebuild intent 竞争、
  self-exec 二次失败和 systemd/setsid 终态。
- [x] 复核 drill 31/98 的 false-green 面，包括 untouched oracle、impact 后水位非空性、共享 deadline
  和 connz 三态观测；复核台账与文档的陈旧结论。
- [x] 候选复审完成后执行 `git add -A`，使开发者大改后基线成为唯一 staged 层。
- [x] 在 unstaged 层修复 finalizer/父关停可达性、新 watchdog 误清、sibling marker 误回滚、
  shutdown intent 优先级、drill 空水位和三处文档漂移；为每个逻辑问题添加独立反例。
- [x] 对 unstaged 修复重跑定向测试、agent `-race`、全量 gates/test/e2e、simcluster hermetic 和真实 drill 98；
  复核 staged/unstaged 分层与工作树卫生。
- [x] 将报告首行更新为最终 `Pass`/`Fail`，记录本轮 findings、修复、测试证据、疑惑、
  residual gaps 和放行条件；确认报告本身仍未暂存。
