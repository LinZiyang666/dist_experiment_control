# 发布前全库审计 · 外部审查 tasklist

> 日期：2026-09-03。角色：外部审查者；只审查，不修改生产实现。
> 审查基线：`HEAD=021c970`；对象为 `HEAD..index` 的全部变更（当前 200 files，`+17154/-719`）。
> 审查启动时工作区与 index 一致，没有额外 unstaged diff；因此本文中的“暂存区外审”统一指对 index 内容做独立外审。之后的 unstaged 内容仅为本审查新增的 tasklist、报告与独立测试。
> `prerelease-audit-plan.md` 与 `prerelease-audit-review.md` 只作索引，不继承其 Pass/修复结论。

## A. 边界、契约与前序工作流

- [x] 读取 `CLAUDE.md`，确认权威链、外审角色边界、测试/变异纪律、N-1 与收尾要求。
- [x] 用 `git status`、`git diff --cached --stat/--numstat/--dirstat/--check` 重建审查边界；确认无 unstaged diff。
- [x] 审查中检测并接纳 6 个并发写入的 simcluster unstaged 文件；逐行复核 admission helper/call sites 与 node-list oracle 修订，并在其稳定后重跑 shell syntax、43-drill lint 与 hermetic gates。
- [x] 粗读本轮 plan/内审报告，提取原始 7 Blocker、44 Major、增量 2 的 B1–B3 与主进程裁决；不信任其闭合声明。
- [x] 读取需求、当前架构、部署 gotchas、测试标准及本轮修改的用户/运维文档；建立需求→实现→测试映射。
- [x] 阅读近期 external-review/tasklist 的体例，固定 finding 严重度、确定度、机理、证据、建议与阴性结论格式。

## B. 生产代码审查面

- [x] **Auth / inbox 隔离**：验证 `_TINBOX` 独立根、CONNECT capability marker、modern/legacy 四象限 fallback、broker reply grants、unactivated/member/agent 权限边界；重点尝试匿名窃听 modern/legacy reply、伪造 marker、跨 clone-family 订阅、半接线与 async permission error。结论：modern 根隔离成立，但 legacy 匿名读取长期凭据构成 B-1。
- [x] **CONNECT 认证与限流**：验证 nonce/signature 真校验、全局+per-IP PIN budget、长期 agent 的 transient/permanent denial 分类、multi-URL probe 不复用 live callbacks、回调按 generation/connection 归属。结论：cluster verifier 四处绕过 global charge（M-1）。
- [x] **session create 准入**：验证 migration/一次性 raft seed、admin allow/list/remove、混版 capability gate fail-closed、leader/follower 读写语义、撤销不被 seed 复活、空表 first-user 流程、fingerprint 规范化与 `whoami`。结论：标准 rolling 时序漏 seed（M-2），并有 non-canonical fingerprint（m-1）。
- [x] **tunnel :7000 intake**：验证 REGISTER 字节上限、SID/NID 校验发生在 map/log 前、握手并发 ceiling、fence/key 回收、accept close/backoff、read/write deadline、watchdog/yamux 生命周期及资源泄漏。未发现新的独立阻塞项；长期 token 的泄漏归 B-1。
- [x] **broker lease / proc / home delivery**：验证 farewell 的 leased-NID/SID ownership、exit/ack subject-payload 对账、session fence、reconcile ownership、attempt map prune、审计发布与 `b.js` 并发安全。结论：N-1 proc exit 越过 session fence（B-2）。
- [x] **transfer 全族**：验证 tier-A 不信任客户端 bucket/key、budget/size 上界、watchdog cancel、orphan reap uptime 门、single/multi-broker home 判定、tmp mode/atomic commit、push/pull mode 与连接重建。结论：single staged terminal 的已知 OPEN 仍是 M-3。
- [x] **cluster online ops / FSM**：验证 retire/drain/force-single/upgrade/leadership-transfer 的 phase 顺序、有限 hold、宕机 voter 语义、marker owner、`IsNotLeader` 家族、request id 幂等、alert ack key/subject 对账。除与 M-2 组合的 leadership-transfer 时序外，未发现新的独立阻塞项。
- [x] **offline backup/restore/config**：验证 flock、symlink/O_NOFOLLOW、临时 journal/DB 原子性、snapshot 身份移植、restore identity、nats.conf ClientListen/JetStream fail-closed、配置保留与权限。抽样与门禁未发现新的独立阻塞项。
- [x] **CLI / exit taxonomy / PTY**：验证 exec/run 本地与 broker 拒绝码、alert deadline、destructive gate fail-closed、`agent join` 配置透传、boot budget、PTY intake 寻址、whoami 与 transfer 模式。抽样与全量基线未发现新的独立阻塞项。
- [x] **install / release / upgrade**：验证 broker/agent 配置 preserve + `.new` + `--force-config`、uninstall 数据边界、systemd 行为、nats-server 三处 pin、SameRelease 五处接线、release URL/版本形态、脚本 quoting/幂等/权限。结论：redirected root 仍操作 host systemctl（M-4）。
- [x] **wire / ACL / N-1**：验证所有 proto 改动 additive/omitempty/零值合法，wire inventory 同步；新 raft op 在混版下不 poison-skip；权限授予与真实 subscribers 双向闭合。B-1/B-2 都是跨 N-1 组合失败。
- [x] **并发、锁、生命周期横切**：审查新 atomic/goroutine/timer/context/锁序，运行 `-race` 与仓库 leak/fd 门；检查 error path 是否 release slot/cancel/wait/close。普通包基线通过；race 首轮 broker 超时、其余目标包通过，broker 20m 独立重跑结果写入最终报告。

## C. 测试与证据审查面

- [x] 对新增/修改测试逐项判断：测的是 production wiring 还是仅 helper；是否存在 identity assertion、手工补产品字段、源码字符串 pin、错误 fixture、遗漏第二次 tick/reconnect/follower 等空守卫。
- [x] 对高风险新守卫做独立变异或反例实验；每个实验单独运行，防止分组 mutation 互相掩蔽。新增四个相互独立的 production/接线反例，均稳定红。
- [x] 运行 `git diff --cached --check`、`go test` 触碰面、`go test -race` 并发面、`make test`、`make gates`、`make lint`；按 CLAUDE 只在确有部署栈必要时决定是否跑 targeted simcluster，不用硬闸绿替代代码审查。结果：diff check 红、reviewer tests 红；普通 baseline/lint/vet-tags/hermetic simcluster 绿；aggregate gates 不以排除红测伪造绿结论。
- [x] 核验 test inventory、origin ledger、结构预算、docs layout、build tags、NATS pin、inbox pairing 等新增/更新 golden 是否来自真实变化而非放宽。新增 reviewer functions 已通过仓库 updater 写入 inventory，定向 determinism/origin 复跑通过。

## D. 报告与收尾

- [x] 将所有 finding 用“严重度 / 确定度 / 文件 / 机理 / 独立证据 / 建议 / 边界”写入 `docs/reviews/prerelease-audit-external-review.md`；首行必须是 `Fail` 或 `Pass`。
- [x] 报告中列出实际运行命令、真实退出码、未运行项及理由；明确区分产品缺陷、测试缺口、文档失实和 pre-existing/open 项。
- [x] 完成报告后更新本 tasklist；运行格式/状态复核，并把工作区所有文件加入暂存。

## E. 开发者修订后二轮重审（2026-09-03）

> 增量边界：第一轮 209 个已暂存文件保持为基线；开发者在其上新增 49 个 tracked unstaged 修改（约 `+1520/-247`）和 3 个 untracked broker 测试。二轮先审该增量，再用定向反例与全量门禁检查它和原基线的组合结果。

- [x] 完整读取开发者在外审报告中的逐条回复、mutation 记录与 deploy-tier 记录；把每项声明映射到实际 production call path、独立测试和文档，不以回复或自测结果代替复核。
- [x] **B-1 legacy inbox 密钥栅栏**：复核私有 reply 判定边界、register 与 proxy `/sub` 两条泄密路径、老 agent 对 `OK/code/Proxy` 的真实兼容行为、资源副作用，以及升级前已泄漏 token/PSK 是否有可执行的轮换闭环。结论：cluster-only create 遗漏栅栏，轮换次序也会重新启用旧 keyset，R2-B1。
- [x] **B-2 proc exit fence**：复核 planner 与 SQL 双层拒绝、所有本地/转发/N-1 调用者、错误分类与 reconcile 收敛；重跑独立 legacy empty-SID 反例并检查新旧 wire 组合。结论：CLOSED。
- [x] **M-1 cluster PIN budget**：复核 follower per-IP 与 leader global 的 check-and-charge 原子性、所有四种写路径、leader late wiring/fail-closed 窗口、错误传播和并发 ceiling；做独立 wiring/并发反例。结论：接线已闭合，但 split check/charge 可让 burst=1 并发放行 64 次，R2-M1。
- [x] **M-2 session creator seed**：复核 boot + leader reconciliation 的 level-trigger、capability gate、marker/owner 兼容语义、撤销不可复活、领导权切换和滚动升级时旧 broker 仍可创建 owner 的安全窗口。结论：retry/grandfather CLOSED；leader dispatch 不重检 admission，N-1 可绕过，R2-B2。
- [x] **M-3 single-broker terminal audit**：复核正常发布与 staged replay 的 payload/Msg-Id 恒等、unlink 时机、publish-ack crash window，以及 JetStream duplicate-window 过期后的重放语义；必要时增加跨窗口反例。结论：过 2m dedup window 后真实 JetStream 接收第二条 terminal，R2-M2。
- [x] **M-4 redirected install root**：复核 install/uninstall 共用 host-systemd predicate、所有 `systemctl` call sites、dry-run seam 与 shell quoting；重跑独立 redirected-root 反例。结论：CLOSED。
- [x] **m-1/m-2/m-3**：复核 fingerprint 严格 base64 round-trip、需求文档与 migration 真相一致、全增量 `diff --check` 清洁。结论：m-1/m-3 CLOSED；m-2 的 seed 描述已订正，但 leader-authoritative admission 仍缺文档和实现。
- [x] **新增 deploy-tier 修订**：审查 JetStream boot wait 的就绪定义、超时/关闭/诊断/测试 seam；审查 NATS 2.14.5→2.10.22 回退的功能与安全依据、三处 pin 一致性，以及 simcluster/admission/expected-verdict 修改是否掩盖失败。结论：2.10.22 落入多个官方 affected ranges（R2-B3）；ensure failure 不进入 wait（R2-M3）。
- [x] **其余增量与回复外改动**：逐文件粗读 49+3 的 diff，重点复核 alert ack、forward metrics、reconcile registry、agent compatibility、wire inventory、测试 expected 值和文档承诺；记录任何新回归。除报告列项外未确认新的独立发布阻断。
- [x] 先运行第一轮四个 reviewer guards，再运行开发者新增测试和触碰包；对可疑修复执行独立 mutation/行为实验，并确保 reviewer test 不依赖 helper-only/self-fulfilling assertion。新增 5 个 production-path 红 guard，分布于 3 个 reviewer test 文件。
- [x] 运行 `git diff --check`、触碰包、`-race`、`make lint`、`make test`、`make gates`；deploy-tier 只在代码审查需要时运行可控的 targeted drill，并如实区分产品失败、环境失败和非复现结果。完整命令证据见报告 §8.5；本轮未伪装 live deploy 复跑。
- [x] 在外审报告追加“二轮重审”章节并把首行更新为最终 `Fail` 或 `Pass`；逐条给出 CLOSED/OPEN/REGRESSED、新发现、命令证据与剩余风险，随后更新 tasklist并将所有文件加入暂存。

## F. 开发者修订后第三轮重审（2026-09-04）

> 增量边界：二轮末 227 个文件已全部暂存；开发者在其上修改 22 个 tracked 文件，初始 unstaged
> 统计为 `+785/-145`。本轮先审 §9 的逐条回复和这 22 个文件，再验证它们与全部 staged 候选组合后的
> 发布结论。审查者只新增独立测试、tasklist 与报告，不修改生产实现。

- [x] 完整读取当前 `CLAUDE.md`、§9 开发者回复及相关 requirements/architecture/broker-ops 约束；把每个“已转绿”声明映射到实际实现和测试，不继承 rc=0 或 deploy GREEN 结论。
- [x] **R2-B1 cluster secret fence**：验证 single/cluster handler 确实只能经统一 reply helper 设置 `SubURL`，private inbox 判定 fail-closed，legacy 请求的 create side effect/审计/code 契约一致；审查 AST guard 是否会被别名、位置字面量或 helper rename 绕过；核对 OFF→revoke-all→ON→private recreate 在 single/cluster 下不会重启旧 PSK/bearer，文档命令真实可执行。代码 fence 关闭；运维命令与版本说明仍红，见报告 R3-M3。
- [x] **R2-B2 leader admission**：验证 `MayCreateSession` 在 leader committed DB、同一 authoritative propose closure 内先于 `PlanCreate`，读错误 fail-closed；覆盖 N-1 old origin、stale follower after revoke、pre-marker stranger、admitted owner、duplicate/error mapping 与混版 capability 行为。forwarded closure 已补，leader-local closure 遗漏并被独立测试复现。
- [x] **R2-M1 PIN atomic budget**：验证所有 single/leader-local/forwarded verifier 都只调用 atomic try-take，一次验证恰好消费一个 token，失败/正确 PIN 都计费；检查 fake clock、nil limiter、锁范围、并发公平性及删旧入口后无旁路。实现、定向测试与 race 均通过。
- [x] **R2-M2 durable terminal detection**：审查 `TransferTerminalCommitted` 的 stream-by-subject 查找、scan 起点、payload identity、三态错误语义、consumer 生命周期与复杂度；重点反例为 retention/limits 已清除旧 terminal、同 transfer ID 跨 session/verb、malformed audit、context timeout、stream rename/restore，以及查询与 replay 间竞态是否仍能重复。相反 kind 重复、context/clock false-negative 与 consumer 泄漏均确认。
- [x] **R2-M3 JS boot wait**：验证首次 probe 和后续 ensure 错误确实共享单一 90s deadline、取消及时、最终错误可诊断；区分永久配置冲突与瞬态 meta failure，检查测试 seam 是否真正走 `Run`/production helper，避免每次 5s 子超时突破总预算。核心 crash-loop 缺陷关闭；“共享单一预算”表述不实但当前 30s+子调用上界仍留出 readiness margin。
- [x] **R2-B3 dependency/toolchain floor**：验证 NATS 三处均为安全且受支持的 2.14.6，版本 floor guard 对旧/畸形/未来 major 均 fail-safe；核对 `go`/`toolchain go1.26.8` 的实际 Go 语义、CI/install/release 是否真的使用该 toolchain，x/crypto 升级和 go.sum 无意外漂移，并复核开发者引用的 govulncheck 可达性结论。当前 pin 与 0 reachable 均成立；patch floor、setup-go v5 与 tidy 留有缺口。
- [x] **CLAUDE / 运维文档**：核对版本变更规则没有把“最低版本”误作实际构建证明；核对 broker-ops 章节顺序、轮换停机窗口、全部 subscriber 枚举/撤销/重建步骤和 N-1 限制与 CLI 实际行为一致。章节顺序关闭；`rm` 不存在及 v2.10.22 降级说明确认。
- [x] **剩余增量横切**：逐文件审阅 22-file diff，检查新的 imports/API、error taxonomy、context/timer/goroutine/consumer cleanup、JetStream 扫描成本、test inventory 与 golden 修改是否合理；记录回复以外的回归。
- [x] 先复跑二轮 5 个 reviewer guards和开发者新增定向测试；对每个安全边界做独立 mutation/反例，必要时新增 reviewer tests并同步 determinism inventory。新增 6 个独立 guard；其中 5 个针对当前候选稳定红，patch-level 工具链 guard 绿。
- [x] 运行 combined `diff --check`、触碰包、相关 `-race`、`make lint`、`make test`、`make gates`；按证据需要决定是否复跑本机可控 simcluster，明确区分开发者 deploy 记录、审查者实跑和环境限制。完整结果见报告 §10.6；本轮不重复冒充开发者的 live deploy 记录。
- [x] 在外审报告追加第三轮章节，逐条给出 CLOSED/OPEN/REGRESSED、新 finding、阴性结论、命令与未运行项；更新首行最终结论和本 tasklist，最后把工作区全部文件加入暂存并确认 unstaged=0。

## G. 审查者获授权直接修复与放行复核（2026-09-04）

> 边界：F 完成后已将开发者全部修改、第三轮 reviewer guards、tasklist 和 `Fail` 报告加入暂存；随后
> 用户授权审查者直接修改到可放行，并要求这些修改留在暂存区外。本节所有动作均在该 index 快照之上
> 进行，完成后不得再次 `git add`。

- [x] 修复 leader-local session create 的 authoritative admission：本地/转发共用 committed-DB planner；补齐已授权 idempotency 测试前置条件，保持撤销 guard fail-closed。
- [x] 重写 durable terminal 查询：以 session+transfer ID 为恒等式、反向读取 raw stream、跨 terminal kind 去重、context/损坏/读取错误返回 unknown；不创建 consumer。
- [x] 修正 proxy credential rotation 命令、broker reply 提示和当前 NATS 运维版本说明；确认不存在现行 `proxy sub rm`/2.10.22 推荐。
- [x] 将 CI 升级到 `actions/setup-go@v6`，增加 toolchain-directive 结构守卫；运行 `go mod tidy`、module verify 并更新 test inventory。
- [x] 修复全仓测试暴露的 spawnsafe churn 测试同步缺陷；普通 100 次、race 20 次稳定通过且保留 no-op mutation 检出能力。
- [x] 复跑 reviewer guards、触碰路径 race、`make lint`、`make gates`、无排除 `make test`、module/shell/diff/inventory 检查，全部通过。
- [x] 运行 `make e2e-parallel`：首次仅 `t.TempDir` cleanup 瞬时失败；目标 p2 50/50 通过，第二次完整矩阵 `ALL PASS`，据证据归类为非阻塞偶发并如实写入报告。
- [x] 将工作树报告首行更新为 `Pass` 并追加 §11；确认 index 报告仍为第三轮 `Fail`，所有审查者修复均 unstaged，未再次加入暂存。
