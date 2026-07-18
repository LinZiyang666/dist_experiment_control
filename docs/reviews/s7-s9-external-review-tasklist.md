# S7–S9 外部审查 tasklist

> 角色：独立外部审查者。内部 plan、自审报告和既有 gotcha 仅作为线索；结论必须由当前 diff、产品源码、独立测试或 simcluster 活体证据支持。
>
> 审查对象：暂存区外的 S7/S9（G-C）改动；审查期间只允许补充独立测试/审查文档，不修改被审实现。

## 0. 范围与基线

- [x] 读取 `CLAUDE.md`、架构/集群/运维/simcluster 文档，提取本批适用的不变量和 simcluster Mandate。
- [x] 核对 `git status`、`git diff`、未跟踪文件、暂存区，建立完整文件清单；确认没有把既有用户改动误当成审查者改动。
- [x] 粗读 `s7-s9-plan.md`、内部 `s7-s9-review.md`、coverage inventory、roadmap 和同类历史外审，理解预期 verdict、已知 RED、NOT-COVERED 与报告格式，但不采信其结论。
- [x] 将 plan 的每个承诺映射到具体脚本/辅助库/文档行，检查实现遗漏、静默降级和范围漂移。

## 1. 通用脚本正确性与安全

- [x] 对全部新增/修改 shell 文件运行语法检查、仓库 lint、`git diff --check`；检查 shellcheck（若项目支持）结果并人工复核误报/漏报。
- [x] 审计 `set -eu`、管道退出码、命令替换、后台任务、trap 覆盖、临时文件、引用/分词、正则签名、超时与轮询边界。
- [x] 审计所有破坏性动作（卷灾难、容器删除、iptables、进程 kill、密钥覆盖）：目标必须精确、fixture 必须自证、cleanup 在成功/失败/信号路径均执行，不能影响并行实例或宿主机。
- [x] 审计跨容器/宿主机复制的 owner/mode/原子性/完整性；秘密不得进日志、world-readable 文件、构建层或 git。
- [x] 检查并行隔离：`INSTANCE`、Docker 网络/卷/容器名、iptables 规则、vault 路径、artifact 名和事件游标不能跨 drill 污染。
- [x] 检查断言原语语义：`assert_ok`/`assert_bug`/`product_red`/`not_covered` 不得倒置、吞掉错误、用宽泛签名误认、或让命令根本没走到目标路径仍假绿。

## 2. 公共库与运行器

- [x] `lib/events.sh`：事件读取权限、JSON 有效性、游标边界、精确过滤、超时、历史事件污染和 fail-closed 行为。
- [x] `lib/fault.sh`：分区方向/端口/链选择正确，注入有独立证据，撤销幂等且 trap-safe；保留控制通道的假设真实成立。
- [x] `lib/leak.sh`：PID generation、进程重启、采样字段、整数/单位、斜率与高水位算法、样本下界、受害者排除和缺失样本均 fail-closed。
- [x] `lib/vault.sh` 与 `lib/secrets.sh`：备份库为宿主独立持久面，push/pull 做 SHA-256 校验，权限不掩盖产品缺陷，凭据轮换/恢复不泄密。
- [x] `drills/lib/cluster.sh`：新增 helper 的参数、节点身份、leader/follower 解析、错误传播和重复调用语义。
- [x] `simcluster`、Dockerfile、`.gitignore`、lint-drills：依赖完整、远端 rsync 可达、环境变量透传明确、lint 确实枚举所有新文件且无危险豁免。

## 3. S7：备份、恢复、全量 DR、凭据轮换

- [x] `50-backup-restore`：逐项验证 leader/follower/offline backup、bundle provenance/完整性、13 个 restore gate 的专属签名、foreign bundle gate 顺序、daemon 停止边界、root/tether 权限差异、`.pre-restore` 唯一备份、doctor/incident 行为。
- [x] 检查 restore 的数据后置条件：节点名册、raft 地址、secrets、DB、nats.conf/systemd seam、服务恢复、数据面与审计；不得只以 CLI exit 0 认定成功。
- [x] 独立复核 #50、#64、DOC-27 的源码机理和活体断言；验证已知缺陷被诚实标 RED，而非 inverted GREEN 或不相关先行错误。
- [x] `51-full-dr`：逐字对照 runbook §5.2；灾前 bundle 必须在独立 vault；全卷损毁必须真实；DR step ledger 必须列出每个未文档化/特权/人工步骤。
- [x] 检查 fresh-box 恢复不偷带旧 `/etc/tether`、nats.conf、secrets、volume 或容器状态；所有 workaround 必须明确标 gap，不能代产品完成恢复。
- [x] 独立复核 #51（及相关 #52/#53 的覆盖/未覆盖边界）和 DR 尾部 NOT-COVERED 的真实性；不能在尚未恢复的栈上声称后续保障。
- [x] `52-credential-rotation`：分别审查 tunnel cert、account seed/CA、node identity、guided compromised-retire；验证轮换前后指纹、旧凭据拒绝、新凭据成功、节点/数据面重新收敛。
- [x] 检查轮换窗口、leader/follower 指令、doctor 可见性和 C7 流程；独立复核 #54/#56/DOC-23，防止把 auth 断裂、缓存连接或旧进程存活误判成轮换成功。

## 4. S9：对账、自愈、混沌与长稳态

- [x] `94-agent-reconcile`：验证 missed-exit 与 orphan 两个方向均由产品路径制造；PID/进程确实存在或消失；audit node scope、rc 字段、`ps` LOST、port-reconcile 的数据面与对照源均准确。
- [x] 检查 restore→reconnect 时序、15 分钟 port-reconcile 条件、旧状态与新状态的唯一性；防止 systemd/cgroup 清理替产品完成 orphan kill。
- [x] `95-broker-selfheal`：区分 SIGTERM clean exit 与 SIGKILL failure，证明 `Restart=always` 而不是偶然重启；检查 G.2、nats restart、T3 和 DELETING NOT-COVERED 的边界。
- [x] `96-mid-flight-chaos`：每个 mid-flight 注入必须先证明操作正在飞行，再证明 fault 生效；检查 leader partition、majority/minority 写、transfer orphan、heal 后一致性及双故障裁剪。
- [x] 独立复核 #58/#65 的测量对象、leader 身份、多数派可见性和持久性证据；尤其排除请求路由到多数派、旧输出污染、未真正分区、或 heal 后重试造成的假象。
- [x] 核查 `run --ack-alerts` inventory 债是否兑现；所有计划要求但无法确定制造的臂必须显式 NOT-COVERED，不能静默删除。
- [x] `97-soak-cycles`：四类注入都被执行且自证；样本数与 `SOAK_CYCLES` 参数一致；被测进程不作为 fault victim；重启后 PID generation 正确处理。
- [x] 复算 fd/RSS/Threads 高水位和斜率 oracle，检查阈值、整数截断、短样本、单调趋势/锯齿泄漏、失败命令和缺样本；panic/FK/corruption 扫描不能被日志轮转或 grep 语义绕过。
- [x] 核对“GREEN（阈值未校准）”与 24h parity 欠账的表述，不能把短 soak 推广成发布级长稳态证明。

## 5. 文档、台账与可追溯性

- [x] 核对 README、roadmap、inventory、gotcha ledger、内部 review 的 verdict/断言数/覆盖状态一致；自动统计与手工描述相互印证。
- [x] 核对 gotcha 编号无碰撞、现象/机制/证据/修复建议相符，SOURCE-CONFIRMED 与 LIVE-CONFIRMED 分级诚实。
- [x] 核对每个 NOT-COVERED 都有具体源码或环境理由，且没有可合理补测却被裁掉的高风险路径。
- [x] 核对文档命令可直接执行、权限/路径/拓扑正确；安全敏感信息只引用本地 ops 文档，不进入公共报告。

## 6. 独立验证

- [x] 运行本地快速门：语法、lint-drills、相关单测/静态检查；失败逐条定因。
- [x] 阅读 `docs/devices-ops.local.md §6`，只在专用 server 上通过 `remote.sh` 查看 simcluster 状态并运行相关 drill。
- [x] 先跑针对高风险发现的最小活体复现；再根据成本/信号运行 50/51/52/94/95/96/97，记录命令、时间、实例、退出码、关键日志和 cleanup 状态。
- [x] 必要时添加只针对审查假设的独立测试；测试不得修改产品实现或用环境 workaround 掩盖产品问题。
- [x] 若声称批次完成，验证 expected-verdict manifest/runner 对预期 RED、INCOMPLETE、infra flake 的处理，避免“全套绿”语义造假；按计划要求检查全套基线证据是否真实存在。

## 7. 收尾与报告

- [x] 对每个 finding 给出严重度、文件/行号、触发条件、实际后果、证据和可执行建议；疑惑单列，不用猜测填补证据缺口。
- [x] 报告首行明确 `Pass` 或 `Fail`；只要存在发布阻断缺陷、测试假绿、安全/清理风险或关键承诺未验证，结论为 Fail。
- [x] 报告区分：本轮实现缺陷、产品既有缺陷、文档缺陷、测试债/NOT-COVERED、基础设施问题。
- [x] 运行完成性审计：逐项勾完 tasklist，复查 diff、测试证据和报告引用；最后将工作树全部文件加入暂存并核验 `git status`。

## 完成记录

- 本地快速门、源码/脚本合同复核、plan-to-code 映射和独立负向测试均已完成；正式结论见 `s7-s9-external-review.md`。
- shellcheck 未安装；仓库 `make lint` 在 golangci-lint package-loading 阶段以 `no go files to analyze` 退出，均已作为工具/基础设施限制记录，而非误记为通过。
- 已按 `docs/devices-ops.local.md §6` 核对专用 server 路径；只读 SSH/status 与会先 rsync 的 `remote.sh status` 均未获当前执行审批，因此没有伪造独立 live 结果，也没有采信内部历史 run 作为替代。
- 独立测试 `test/simcluster/tests/s7-s9-external-review.sh` 稳定暴露 10 个当前 contract 失败；这些本地可证阻断项已经足够作出 Fail 判定。

## Round 2 复审记录（2026-07-18）

- [x] 逐条核对开发者对 B1–B10 的实现、回复与文档同步；确认八项完整闭合，B10 已合理降级并加单次 artifact。
- [x] 重跑独立审查测试、lint-drills、verdict-contract、相关 Go package 测试、shell syntax 与 diff whitespace 门。
- [x] 复核 A7 的真实连接生命周期，而非采信“短 DROP 等于 fresh TLS handshake”的回复表述。
- [x] 复核 97 四类 injection 的非空自证与 plan/README 口径。
- [x] 补强独立测试并在正式报告追加 round-2 裁决、最小放行条件与非阻断项边界。

## Round 3 窄复审记录（2026-07-18）

- [x] 确认 97 transfer-concurrency half 已用本 cycle history `start` 自证，失败路径诚实 NOT-COVERED；R2-F2 闭合。
- [x] 对照 `fault_partition_on` 的 netns/INPUT-dport/OUTPUT-sport 契约，核验 A7 fault 的节点与流向。
- [x] 补独立静态回归，确认当前只剩 `fault_partition_on agt1 7000 4222` 的目标节点错误。
- [x] 在正式报告追加唯一最小放行条件；不重开已闭合项或风格争议。

## Round 4 最终复审记录（2026-07-18）

- [x] 核验 A7 fault 已移动到 brk1 tunnel listener 端，block/down/heal/up 四段证据闭合。
- [x] 复核 fault helper 双 hook + dport/sport 语义，接受开发者对旧接线的实测澄清。
- [x] 重跑独立回归、16-drill lint、verdict contract、shell syntax 与 diff whitespace 门，全部通过。
- [x] 将报告首行和最新裁决更新为 Pass；保留历史轮次供追溯。
