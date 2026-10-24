# 全栈热更新证据索引

日期：2026-09-20。**已有显式资源交接基础包的测试证据；完整热更新尚未接线/通过，真实simcluster未运行。** 对照 [plan](hot-update-plan.md) 和 [tasklist](hot-update-tasklist.md) 使用。

## 1. 起点与证据归属

- HEAD：`cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`。
- 起点包含control-connection-recovery的46文件未暂存候选，逐文件SHA清单：`/tmp/tether-control-recovery/final-candidate-sha256.txt`；原修复证明见[第二轮报告](control-connection-recovery-review-round2.md)。这份快照不因后续热更新实现而覆盖。
- 热更新设计：[agent](hot-update-design-agent.md)、[transport](hot-update-design-transport.md)、[system](hot-update-design-system.md)。它们是独立建议/静态分析，不能算测试结果。
- 本功能实施期间必须另存候选HEAD、dirty diff及未跟踪文件摘要、构建/运行身份；不能仅记未变化的HEAD掩盖不同工作树。

## 2. 只读环境检查

系统lane在规划时检查本机为weilandserver，88逻辑CPU，内存约252GiB（约226GiB可用），仓库所在盘约3.5TiB可用，/tmp约806GiB可用；docker当时没有运行容器，有既有sim镜像及vendor二进制。上述是**一次资源读数，非长期预约或部署成功证明**。启动drill前重新检查、分配独立instance/卷/端口并冻结镜像身份。

现有 `test/simcluster/drills/lib/ingress.sh` 的Python入口及当前Dockerfile不能证明真实Caddy升级；真实Caddy/TLS/WSS供给是前置工作。未启动新服务、未触碰现网。

## 3. 逐能力证据账本

未执行记 `NOT_RUN`；实际执行状态与runner原生verdict分别记，保留 `GREEN / PRODUCT-RED / SETUP-RED / ASSERT-FAIL / INCOMPLETE` 等真实判词，不用新词表吞掉失败。新增自动测试通过与真实drill通过分别记，任何一栏没有证据不得把整行标GREEN。

| 能力 | 自动测试/真实进程/race资源 | simcluster臂与运行身份 | 文档/限制 | 当前结论 |
| --- | --- | --- | --- | --- |
| 唯一owner/请求幂等/manager | 待实现、未运行 | 待分配、未运行 | plan §3.1–3.2 | NOT_RUN |
| ctl输出续接/能力协商 | 待实现、未运行 | 待分配、未运行 | plan §3.1、§3.6 | NOT_RUN |
| 普通进程/exec/PTY/退出码 | exec 域：S1–S3 hermetic + 真二进制收据（§12–§14）；PTY 待实现（S4） | exec 域：34 七臂 GREEN、35 三臂 GREEN、33 INCOMPLETE（#73）、39 X GREEN / Y PRODUCT-RED（§14.4）；PTY：drill 36 待分配 | plan §1、§3.1；architecture §22.4–§22.6 | exec 域 GREEN（2026-10-02）；PTY NOT_RUN |
| SSH/TCP逻辑流/原socket | 待实现、未运行 | 待分配、未运行 | plan §3.3 | NOT_RUN |
| SS AEAD状态/权限撤销 | 待实现、未运行 | 待分配、未运行 | plan §3.3 | NOT_RUN |
| tier A/B push/pull/commit | 待实现、未运行 | 待分配、未运行 | plan §3.4 | NOT_RUN |
| remote-fs/控制任务 | 待实现、未运行 | 待分配、未运行 | plan §5 | NOT_RUN |
| broker tracker/Raft/SQLite | 待实现、未运行 | 待分配、未运行 | plan §3.4 | NOT_RUN |
| NATS/JS升级/回滚 | 待实现、未运行 | 待分配、未运行 | plan §3.5 | NOT_RUN |
| Caddy配置/二进制/入口 | 待实现、未运行 | 真Caddy供给待补 | plan §3.5、§6 | NOT_RUN |
| N=1/N≥3/容量拒绝 | 待实现、未运行 | 待分配、未运行 | plan §3.5 | NOT_RUN |
| N-1/首次迁入/回滚 | 待实现、未运行 | 待分配、未运行 | plan §3.6 | NOT_RUN |
| 全栈/多代/全旧映像退场 | 待实现、未运行 | 待分配、未运行 | plan §1.2、§7 | NOT_RUN |

## 4. 每次执行的必填收据

1. 对应能力/场景/故障窗，预先声明的预期结论、oracle与预算。
2. 候选HEAD+dirty摘要、旧/新artifact SHA、实际运行映像SHA与进程身份、构建参数。
3. 命令、开始/结束UTC、退出码、instance/拓扑/副本/权限/配置摘要。
4. 原始日志和probe记录的归档路径/校验和；原PID/start ticks/nonce、原连接与双向序号、transferID/SHA/唯一终态、每新请求结果/延迟。
5. 正/负控制与red→green收据；修复前后究竟改了哪些变量；禁止仅保留最后一次绿。
6. 旧映像/FD/进程/consumer/临时文件/锁的回收证据，至少五轮生命周期资源读数。
7. verdict、未覆盖/环境限制、活文档更新路径与tasklist对应项。

## 5. 最终结论规则

必要能力所有自动测试、真实drill、文档映射均完成，再记录同一候选的全量闸门与内审结论。`make gates`的hermetic自检不是deploy-tier证据。源码检查、旧修复通过、磁盘版本变化、升级后重新连通、单纯安全defer都不能替代热更新成功臂。

## 6. H1 显式资源包的当前收据（部分实现）

生产位置：`internal/handoff/{handoff.go,image_linux.go,image_unsupported.go}`。此时没有agent/broker/ctl调用点；支持的是调用者已安全停读写后，同PIDexec保显式FD/元数据。12个Test及1个Fuzz覆盖sealed manifest、身份/schema/文件指纹拒绝、CLOEXEC输入、借用/转移/释放、真实原child/pipe/TCP、exec失败恢复与五轮资源。

| 验证 | 实际结果与原始收据 |
| --- | --- |
| root状态/边界定向race | PASS 1.055s；`/tmp/tether-control-recovery/handoff-state-race.log` |
| 专家真实exec/资源及两个变异 | 整包race PASS 28.232s；错误开放非白名单FD与遗漏import CLOEXEC两个overlay都被抓红；[测试报告](hot-update-handoff-test-report.md)，原始日志`/tmp/tether-control-recovery/handoff-exec-*.log` |
| root增加非CLOEXEC输入拒绝/纯解析器后的整包race | PASS 28.219s；`go test -race ./internal/handoff -count=1`，`/tmp/tether-control-recovery/handoff-package-current-race.log` |
| 自写dispatch解析fuzz | `GOMAXPROCS=4 make fuzz FUZZ='^FuzzHandoffDispatch$' FUZZTIME=30s` PASS；364469次执行、6个种子、11个新增interesting输入；`handoff-dispatch-fuzz.log`。interesting缓存不作为已提交crash语料 |
| 独立原socket/Go flags探针 | `-race ×3` PASS 5.801s；File/Fd负例确实改变原socket阻塞状态；raw duplicate正例保持deadline；[传输核查](hot-update-handoff-transport-review.md)及`handoff-transport-socket-probe.log` |
| 测试/矩阵登记 | 新13个函数追加test inventory，新增handoff整包race矩阵单元，两个泄漏anchor追加；原有清单未删除 |
| Darwin arm64 交叉编译 | `GOOS=darwin GOARCH=arm64 go build ./internal/handoff` exit 0；只证明unsupported边界可编译 |
| 首次make gates | FAIL：泄漏形状门无法解析外包常量循环上界。将测试循环改为可识别的5次literal，定向门PASS；未改门、未加豁免；`handoff-foundation-gates.log`保留，完整复验见后续记录 |

局限：不证明上层freeze/父Wait协调、PTY、TLS/yamux/SS、可靠ctl/传输/持久化、四层升级/回滚、N-1或真实部署。子进程测试的30s等待只是测试预算，未证明产品500msfreeze/5s控制目标；五轮原语exercise也不等于同一个业务跨五代的完整运行层验收。

基础包后续闸门：`handoff-foundation-gates-current.log` 中Go闸门与simcluster hermetic全部通过，lint唯一QF1001（名称字符检查的否定布尔写法）。改为正向判定后，单独`make lint` exit 0 / `0 issues`，`handoff-foundation-lint-final.log`。这是该基础包候选的闭合证据；后续workload改动仍须自己的验证，最终完整候选仍须全量闸门。

## 7. H1 进程所有权的首轮接线（部分实现）

`internal/workload`新增进程表和显式stdio；agent普通exec不再依赖`cmd.Wait`的隐式copy/pipe清理。PTY成功启动后也把wait/signal交给同一manager，master/KA和启动失败清理由原Session持有。针对登记child的WNOHANG reap与Signal/Freeze同锁，保真实status/rc/rusage，终态转交courier前不删任务。G.1联合快照新增普通exec并合并PTY/退出记录；kill已支持普通exec。

| 验证 | 实际结果与原始收据 |
| --- | --- |
| 独立process表测试 | 8项，`go test -race ./internal/workload -count=3` PASS 1.846s；`workload-process-race-final.log`；[独立报告](hot-update-workload-test-report.md)。其快照早于后续Usage字段，当前候选须再验证 |
| process表负控制 | `/tmp` overlay忽略freeze、删snapshot深拷贝均预期FAIL；未改仓库生产；原始`workload-process-*-negative.log`及独立报告 |
| 既有agent定向兼容 | PASS 2.777s；`workload-agent-compatibility.log`；环境/隔离helper/courier/遗弃启动清理 |
| 新agent字节与管理回归 | 三项race PASS 1.578s；`workload-agent-ownership-current.log`；普通/隔离spawn × 空/大块二进制stdin、两路输出、真实rc、活任务snapshot、真实NATS kill响应、退出重叠合并 |
| CLI/broker/evict定向集成 | `go test -race ./test/cli_e2e ./test/p4 ./test/p9 -run 'Test(Exec|PsListsExitedProcess|AdminEvict)' -count=1` PASS，分别10.363s/10.549s/3.096s；`workload-exec-integration.log` |
| PTY既有兼容首轮 | PTY/agent/P5定向race PASS，2.102s/5.455s/11.638s；`workload-pty-compatibility.log`；后续独立接管测试仍待补 |

首个编译尝试因LocalProcess.EndedAt为指针而非值失败，修正后重跑；新测试初次误用了WaitFor签名，改为已完成handler后的直接所有权断言后重跑。失败日志仍保留，不计为产品行为失败。

此阶段没有state import、I/O安全点/ACK重放、可靠ctl、upgrade/bootstrap接线或真实热更新drill；starting/abandoned helper完整census也仍待做，不能开放活跃业务热升级。PTY原200ms尾部等待尚待替换为可靠输出状态。兼容核查另发现既有`session_destroyed`事件未触发agent业务清场，详见[核对记录](hot-update-workload-compatibility.md)，纳入后续撤权/删除与切换竞争的实现及测试。

PTY新增独立接管测试：3项（含3拒绝子例），`-race ×3` PASS 1.155s，`/tmp/tether-control-recovery/pty-process-owner-race.log`；[独立报告](hot-update-pty-owner-test-report.md)。覆盖真实输入/resize、process freeze下Hangup拒绝且master保留、thaw准确rc、Process.Release后OSPID不跟随-1、重复/过晚Adopt拒绝。仍未验证PTY I/O冻结或真实exec迁移。

root临时overlay删除普通exec→G.1快照接线，`TestExecOwnershipSnapshotAndControlKill`预期FAIL 0.120s，明确报活任务快照为空；`workload-agent-snapshot-negative.log`。所有负控制均不修改生产树。`GOOS=darwin GOARCH=arm64 go build ./internal/workload ./internal/pty ./internal/agent` exit 0，`workload-darwin-build.log`。阶段静态检查`make lint` exit 0 / 0 issues，`workload-ownership-lint.log`。

## 8. H0真实进程探针接线（真实drill待运行）

新增34-hot-update-process，先供给一个broker/agent/ctl并完成所有fixture重启，再以原ctl执行独立Python业务；按原PID/boot/startticks/nonce、原stdout/stderr序号、RUNNING状态建立正向基线；真实CLI node upgrade换代后核验同agent PID与新运行SHA/committed marker，再核验原业务持续、管理及准确退出码。记录source ledger只作为期望值，绝不回填用户输出。当前旧升级入口的预期是严格signature匹配的PRODUCT-RED（双pipe EPIPE、原child仍活且推进、管理EXITED(-1)），不是热更新通过；修复后须保相同正向oracle并改为普通assert_ok。

13文件harness patch归档SHA `4499ed3f2023a7e137eeb2ce948d2626231d3b57abb35f44d78acb00e3174f9f`；root已审完整probe/清理并整合。专家完整hermetic ALL PASS，`/tmp/tether-control-recovery/hot-update-h0-patch/hermetic-gates.log`；root当前树独立探针自检exit 0，`hot-update-h0-root-probe.log`（28个synthetic正反例、真实healthy/EPIPE pipes、pidfd清理、统一四日志映射）。它们不启动Docker，**不算真实34已运行**。供给真实Caddy草案另待审合并，34仅宣称进程升级基线，不宣称Caddy/WSS或全栈热更新。

## 9. H0真实基线和有界接线复核

34真实演练已执行，61.900s、原runner rc=3 / PRODUCT-RED，17 setup通过且assert_fail=0。实际agent同PID1223换到新SHA并committed，原child1279继续产出，但两管道EPIPE、管理错误EXITED(-1)，原ctl未交付业务退出码23；stdout/stderr分别缺92/93记录。完整冻结身份、原日志及teardown见[真实基线报告](hot-update-process-baseline.md)，原始目录`/tmp/tether-control-recovery/hot-update-process-real/`。旧/新二进制来自同源不同release标识，仅证明机制基线，不算真实N-1覆盖。

W1/W2有界复核及独立测试见[接线报告](hot-update-workload-wiring-review.md)。root回复：W1采纳，AdoptDraining+AllowReap保留原leader直到输出EOF；独立race×3 PASS8.278s，撤hold变异如期红。W2采纳，共用按逻辑任务ID的Signal；root `TestReconciliationDropsOrdinaryExecWithBoundedEscalation`覆盖真实忽略TERM后升级SIGKILL和ctx取消，连同stdio/kill测试race PASS12.120s，`workload-drop-processes-race.log`。新状态import另在实现，不借上述报告声称已通过。

真实Caddy供给及地址补丁已整合：官方artifact校验、镜像内复核、真实installer unit、显式profile与独立WSS/tunnel地址。地址反例修正后，root hermetic12项PASS，`caddy-artifact-root.log`；生产`loadAgentYAML`读取实际shell生成文档及裸IPv6反例定向PASS，`caddy-agent-yaml-root.log`。此时真实Caddy下载/build/profile smoke仍在执行，不能把hermetic当成部署成功。

进程域state恢复：6个独立Test，最终race×3 PASS4.861s；真实同PID exec携原live child、ReapHeld zombie、真实终态和显式WaitError，保原退出权；17个非法state臂验证无部分安装/不杀不reap，五轮资源收据与两个反例均落[进程状态报告](hot-update-process-state-test-report.md)。它不包含IO或agent升级闭环。

注册结算：5个独立Test，race×3 PASS2.152s，真实旧register回复晚于新child exit23时，原退出仍经courier送达；恢复旧全队列清理的overlay按预期FAIL4.125s。[报告](hot-update-register-receipt-test-report.md)。root采纳：每attempt捕获事件身份和新的实际快照；成功回复在register内只settle该请求携带的事件；courier直接ACK也只作用于当次entry身份。未宣称解决新任务准入与broker G.1旧快照的全部并发。

真实Caddy供给：2.7.6官方archive下载/校验与镜像构建通过，真实authenticated WSS、运行SHA、原production unit和冷重启后自启/WSS通过；plain profile确认Caddy前后disabled/inactive/从未启动。额外“原Python sidecar冷return仍可达”检查FAIL，两份原rc1保留；独立诊断证实sidecar仍持旧netns，而broker换了netns，新增Caddy未运行。这是已观察fixture边界，不归因Caddy也不静默放宽断言；不在热更新窗口用重挂sidecar补救。[完整报告](hot-update-caddy-profile-report.md)，所有三实例资源已清空。

## 10. 用户要求停止实施、转交 Claude 的收尾

最新状态统一在[主交接](hot-update-handoff.md)：原 plan/全部范围不变，普通进程、Replay/Input/Output、创建 registry、可靠 Exec 三端已有部分实现和独立测试；Runtime 两条真实失败未修，没有完整 freeze/import 或实际热升级闭环。各历史原语报告保留自己的源码快照，不能视为最后树已验证。

- [Input](hot-update-input-test-report.md)、[Output](hot-update-output-test-report.md)、[Replay](hot-update-replay-test-report.md) 的真实原 pipe/部分写/背压/exec/五轮资源收据已归档；最新 Output prefix 接线晚于原测试收据。
- [Registry](hot-update-procrequest-test-report.md) 11 Test race×3通过；[Runtime成功主线](hot-update-execsession-test-report.md)4 Test race×3通过，但[失败路径报告](hot-update-execsession-failure-review.md)两红两绿、整体Fail；均保留负控制和原日志。
- [broker权限报告](hot-update-process-permissions-test-report.md)8 Test race×3与30s fuzz通过；新增CallID错误关联未复验。client首批8 Test通过，后3个Test入口（含helper）尚未运行，详见[协议交接](hot-update-handoff-protocol.md)。
- [PTY master](hot-update-pty-master-test-report.md)和startExecPipes提案已root审核应用，历史overlay收据与后续适配增量分开。
- [最后一次集中gates](hot-update-stream-gates-report.md)原rc2；循环写法已修，但此后新增测试/字段/矩阵未登记，没有新的完整gates/test/e2e/lint收据。
- `reliable-exec-cli-first.log`的10.162s PASS只覆盖`test/cli_e2e/exec_io_test.go`中**旧exec wire替身**；不能宣称新CLI端到端已通过。

完整`/tmp/tether-control-recovery/`及额外顶层笔记备份到仓库忽略目录`tmp/hot-update-handoff-20260920T2356Z/`，最终`source-manifest.json`、`tracked.diff`、未跟踪文件tar和`archive-receipt.json`绑定交接树。没有暂存/commit/push/现网部署；用户停止指令后只进行文档与已有命令收据收尾。

## 11. S0 稳定交接树（Claude 接手后第一个 stage；plan：[hot-update-stable-tree-plan.md](hot-update-stable-tree-plan.md)）

基线 HEAD `cab90d1`；接手时 169 文件已在 index。原始日志目录：仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s0-evidence/`（`tmp/` 内的证据副本因污染源码扫描门已整体搬出）。

**接手基线**：`make test` 6m32s rc=2，红项恰为 execsession F1/F2 + wire/test inventory + leak anchor（`make-test-baseline.log`）；单份 `make lint` rc=2，交接树自带 2 条 `QF1001`（`internal/procrequest/registry.go:346`、`internal/broker/exec.go:631`；`lint-baseline.log` 里第三条 `unused` 是主进程当时已开始编辑的 `task.abort` 字段，属于 S0 引入的中间态，不是交接树的红）。

**修法**：唯一最终清场 owner（`conclude`/`retireLocked`，closed runtime 下迟到 Start 自行回收；`Close` 两段式，已发布 task 不等 ctx）；setup 失败不再 `closeTask`——child SIGKILL 后已建泵 drain 到真 EOF，未建泵的流记 `task.abort`（aborted 合同：`ProcessOutput.Error` 与 `FIN` 同位、只在终批、互斥；request 侧 FIN 只回显真 EOF；terminal-ack 逐流 `(FIN && ack==End) || (aborted && ack==End)`）；execclient 把 aborted 作为流终结继续 settle，返回 `*SettledError`；零新 wire 字段；两处 QF1001 改写。

| 收据 | 命令 / 结果 |
|---|---|
| 四条定向 F1/F2 测试 | `go test -race -count=3 ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure\|PumpSetupFailureRetainsObservableTerminal\|PartialAllocationFailureSettlesAndReleasesBudget\|CloseReapsChildWithUnacknowledgedFullOutput)$'` PASS 1.196s；断言未改（T3 追加 Error=="" 断言） |
| execsession 全包 | `-race -count=3` PASS 11.647s；`TestRuntimeCloseRetiresUnsettledTerminalWithoutWaiting -count=20` PASS |
| execclient 全包 | `-race -count=1` PASS 6.014s（含 T11/T12/T13 真 runtime 端到端） |
| 变异（overlay 于仓库外副本，`mut/*.log`） | f1 rc=1「retained 1 completed startup owner」；f2 rc=1「file already closed」；fakefin rc=1「must be aborted, not FIN」；lastbatch rc=1「non-final batch carries an end marker」；closewait(-count=20) rc=1「left published work」；closelate rc=1「timed out waiting for late successful startup」；noclose rc=1「leak: +28」；lockfree rc=1「DATA RACE」；client rc=1「expected terminal-ack, got op=ack」；clientfin rc=1（永不 terminal-ack，超时）；clientcross rc=1（fin_then_error / error_then_fin）；proto rc=1「encodes a FIN key」。**12/12 红** |
| 登记 | leak anchor +3（`anchor.log` PASS）；matrix `appended 3 unit(s)`（golden +3/-0）；wire `wrote (496 entries)`（golden +49/-0，新增行全是 `Process*`）；test inventory `appended 54 function(s)`（golden +54/-0 = 45 交接 + 9 新增） |
| 门 | `go test ./test/determinism ./test/architecture ./test/e2e/parallel ./cmd/tether ./internal/proto` rc=0；`make gates` rc=0 5m36s；`make test` rc=0 6m24s；`make e2e-parallel` rc=0 5m07s **ALL PASS**（dedupe 45→37，units 37→72）；单份 `make lint` rc=0；`GOOS=darwin GOARCH=arm64 go build ./...` rc=0 |
| 两增量边界 | control-connection-recovery 冻结清单 46 文件对 index：40 未动、6 共享（architecture.md / INDEX.md / agent.go / broker.go / leak_assert_shape_test.go / test_function_inventory.txt）；其 43 个测试回归 `ccr-regress.log` rc=0（cmd/tether、agent、broker、jsstream、tunnel、test/p13） |
| 收尾（**本行数据有误，内审 DOC-1/G1/P-1 抓出：hash 是接手基线树而非候选树、"status 为空"在暂存树上不可能成立——订正见 §11.1**） | `git add -A` 后 `git status --short` 为空；`git write-tree` = `dc4df10c0b184949b3bf8b9cc2a59cb3a5ad5715`（S0 内审前的候选树；内审修改后另记） |

**覆盖限制**（plan §7）：setup 失败后若有逃出 pgid 的后代持 stdout 写端，stdout 永不 FIN、task 保留到 Close——与正常 exec 后代持 pipe 的既有边界同类，S5 收口；`finish()` 在 `registry.Complete` 失败时不 Exited/不 Forget 的缺口留 S1/S2。

### 11.1 S0 内审后订正与最终候选树收据

内审报告：[hot-update-stable-tree-review.md](hot-update-stable-tree-review.md)（16 finding + 4 missed，全部 CONFIRMED；18 采纳、2 记入 S1/S2）。以下**取代** §11 中被订正的行；未提及的行仍有效。

- **接手基线树**：`git write-tree` = `dc4df10c0b184949b3bf8b9cc2a59cb3a5ad5715`（169 文件交接 index，F1/F2 仍红；§11 误把它记成"S0 候选树"）。
- **两增量边界**（订正 DOC-2）：ccr 冻结清单 46 文件对最终 index：**39 未动 / 7 共享**（agent.go、broker.go 在 S0 零改动——共享来自交接期；leak_assert_shape_test.go +3 anchor；test_function_inventory.txt 只追加；wire_field_inventory.txt 追加 49 − 收缩 4；architecture.md §22.3 追加；INDEX.md 追加 + 删一空行）；行为层证据 = 43 个 ccr 测试回归 rc=0（`ccr-regress.log`）。
- **execclient**（订正 missed #2）：T11/T12 是 stub relay 形状，只有 T13 起真 `execsession.Runtime`。
- **make-test-baseline.log** 已归档进 `s0-evidence/`（DOC-6/G5）。
- **内审修改**：`boundedError` 加 UTF-8 半（C-1/G2）；`outputsEnded` 删不可达 abort 分支（G4）；`ProcessResumeReply` 删 `Stream/Offset/Data/FIN` 四个死字段并手改 wire 账本收缩 4 行（P-3；本 verb 从未发布、零读写——**commit message 须写明此账本收缩**）；T8 改为测试内 20 次尝试（gates missed）；新增 `TestRuntimeStartFailureTextIsAlwaysAcceptedAsTerminal`（C-1/G2）、`TestSettledErrorLeadsWithTerminalErrorVerbatim`（G3）；文档 9 处（DOC-1/3/4/5/7、§22.3、stages-plan S0、plan exit #5）。
- **新增变异**（`mut/*.log`）：closewait2（现 runtime.go + 新 T8，`-count=1`）rc=1「left published work」；noutf8 rc=1「did not become a terminal」；nolen rc=1「did not become a terminal」；settledorder rc=1「SettledError.Error() = …stderr aborted…; remote_fs_spawn_timeout…」。**16/16 红**（12 + 4）。
- **登记**：test inventory 再 +2（`appended 2 function(s)`，vs HEAD +170）；wire vs HEAD +46（= +50 − 4）；matrix +3、anchor +3 不变。

| 最终候选树收据 | 命令 / 结果 |
|---|---|
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./...` rc=0 |
| 扫描门 | `go test ./test/determinism ./test/architecture ./test/e2e/parallel ./internal/proto` rc=0（`gates-go-r2.log`） |
| `make gates` | rc=0 5m38s（`gates-r2.log`） |
| `make test` | rc=0 6m31s（`test-r2.log`） |
| `make e2e-parallel` | rc=0 4m52s **ALL PASS**（`e2e-r2.log`） |
| `make lint` | rc=0（`lint-r2.log`，与 gates 内那份串行） |
| 判据 | `git status --short \| grep -v '^[AMDR] '` 为空；`git ls-files \| grep -c __pycache__` = 0 |
| **S0 候选树** | `git write-tree` = 见本节末行（在全部 `git add -A` 之后计算；不含写入该行本身之后的 evidence 变更） |
- **S0 候选树** `git write-tree` = `69789773909545fd9d1d09f64dd4306043a99ffc`（含本节至上一行；写入本行后 evidence 自身再次变化，最终 index 的 hash 由内审报告 §4 记录）

## 12. S1 可靠 exec（plan：[hot-update-reliable-exec-plan.md](hot-update-reliable-exec-plan.md)）

基线 = S0 最终候选树（§11.1）。原始日志：仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s1-plan/`（4 路草拟 + 4 路批评 + 综合）、`s1-e2e-race.log`；本节的三硬闸/变异日志在会话工作目录 `~/.claude/jobs/5a375dad/tmp/`（`s1-gates.log`、`s1-test.log`、`s1-e2e.log`、`mut/*.out`、`runall.log`）——它们不进仓库，本表是它们的摘录。

**范围（plan §0 六出口）**：① 真二进制 e2e 可靠 exec 全绿；② callout 黑窗连接级有界重试；③ 撤权（member 撤销 → 77 不降级；`session_destroyed` → Revoke + tombstone，daemon 不退）；④ 任务表合一的时序半（单一 started 站点 + `StartedUnacked` + G.1 两处 skip）；⑤ 退出码 taxonomy（74）；⑥ 部署层：`vendor/tether-prev` 供给 + drill 35 三臂 + drill 34 重钉。**deploy 实跑（step 12）需用户同意，本节记 NOT_RUN 与已备条件。**

| 收据 | 命令 / 结果 |
|---|---|
| 真二进制 e2e（Linux） | `go test -race -count=1 -timeout 600s -run '^(TestExecReliable\|TestCtlConnect)' ./test/cli_e2e/` PASS **102.2s**（14 条：RealBinaryStreamsAndExitCode / AgentConnKickMidStream / BrokerRestartMidStream / NATSRestartMidStream / CalloutBlackWindowMidStream / LostReplyRunsOnceAndSettlesOnce ×2 / TimeoutAbandonsWithoutRemoteKill / PsShowsTaskRunningThenExited / RevokedMemberMidStreamExitsNoPerm / SessionRmMidStreamKillsChild / LegacyFallbackOnOldAgent / **LegacyFallbackOnOldBroker**（server 端 deny 今天 broker 的 `process-resume.req` 订阅 = v0.6.0 订阅面；自证读 server 连接表）/ StdoutClosedEarlyKeepsRemoteTruthful / NodeOfflineAfterCreateIsUnsettled / CtlConnectRetriesThroughCalloutBlackWindow）；矩阵单元 `TestReliableExecMatrix`（golden +1） |
| 定向包测试 | `go test -race -count=3 ./internal/procrequest ./internal/execsession ./internal/workload ./internal/execclient ./internal/procforward` PASS；`./internal/broker ./internal/agent ./cmd/tether ./internal/cli` 在 `make test` 内 |
| simcluster hermetic 闸集（内审前的数字；内审 D10 订正后的最终数在 §12.1） | `sh test/simcluster/tests/run-all.sh` **34/34 PASS**（新增 `prev-supply-test` 24 控制；probe selftest **61** 控制（S0 的 selftest 打 28 个控制 → +33：34 新签名正例 + 旧 `{finished:false}` 负例 + 「rc 74 但输出完整」负例、`carrier` 1 正 + 15 负、`carrier --path legacy` 1 正 + 2 负、`legacy-carrier` 4 正 + 7 负——每条负例的失败原因逐条打印核对，无一因无关缺文件而红）；r9d-nonvacuity **199 proved**（drill 35 三臂各一节：`_final_ps_exited` 7、`_stream_advancing/_advanced_by` 8、`_agt1_at_prev/_rebuild_seen` 8）；lint-drills 45 drills 0 violation；arm-manifest-lint 3 arm-split OK；validate-verdicts 53 rows OK；ledger-crosscheck 16 open all pinned；kept-sites / assert-identity 基线 +72 rows（35）后 OK） |
| 变异（`go test -overlay`，仓库树不动；`mut/M*.out`、`mut/summary.log`） | 见下表：M1–M8 **10/10 go 变异全红**（M4/M5 第一版 overlay 是编译错误，改成保留 import 的形式后为真断言红）+ M9–M12 |
| fuzz 收据 | `go test -run '^$' -fuzz '^FuzzParseStamp$' -fuzztime 30s ./internal/procforward/` PASS（execs 2,154,781，new interesting 51 / total 322）；`-fuzz '^FuzzPrivateProcessReply$' -fuzztime 30s ./internal/broker/` PASS（execs 408,032，new interesting 16 / total 58）；两者 `testdata/fuzz` 无新 crasher（`fuzz-*.log`） |
| 登记 | wire inventory +2（S1 步 2：`ProcessResumeReply.Code`、`LocalProcess.StartedUnacked`）；test inventory `appended 43 function(s)`（本轮）+ 之前 S1 各步；matrix +1；structural budget Agent 126→125（收紧方向自动更新）；CLI golden `--stdin`；wire 错误码账本 +5 码 / +1 unresolved site / `processResumeReject`=3 |
| 门（第一轮，修 5 条 lint + 1 条 wire-freeze 之前） | `make gates` rc=2 5m38s——hermetic 闸集与全部 Go 门绿，只有收尾的 `make lint` 5 条（maintidx `agent.session`、revive bool-literal ×2、staticcheck QF1001 `admit.go`、unused `restartBroker`）；`make test` rc=2 6m27s，唯一红 `TestForwardPayloadKeysAreWireFrozen`（`LocalProcess` 新增 `started_unacked` 未登记）；`make e2e-parallel` rc=2 4m35s，同一条红、其余 ALL PASS |
| 修法 | `session()` 的 sys.events 闭包提成 `handleSysEvent`（注释整段搬运；不登记第十一个 god function）；两处 `!= true` 改类型断言；`admit.go` 条件改写为 `status != ONLINE && (!allowStaleNode || status != STALE)`（M1 变异按新形式重做）；删未用 `restartBroker`；`wire_freeze_test.go` 登记 `started_unacked`（additive，freeze-on-entry 注释）。单份 `make lint` 0 issues |
| 门（第二轮） | `make gates` rc=2 1m45s——唯一红 `TestStructuralBudget`：`handleSysEvent` 作为方法让 `Agent` 方法数 126 > 账本 125（账本是棘轮，不放宽）；`make test` rc=2 同一条；`make e2e-parallel` rc=0 4m46s **ALL PASS**。修：`handleSysEvent` 改为包级函数（与 `revokeSessionWorkloads` 同理由），单份 `make lint` 0 issues |
| 门（第三轮，最终树；`s1-*-r3.log`） | `make gates` rc=0 5m31s；`make test` rc=0 1m53s（73 个 `ok`）；`make e2e-parallel` rc=0 4m26s **ALL PASS**（units 38→73）；`GOOS=darwin GOARCH=arm64 go build ./...` rc=0 |
| 部署层供给（未实跑） | `sim_prev_tag` = `v0.6.0`（HEAD `cab90d1`+index 不在 tag 上 → describe）；`vendor/tether-prev` 构建/`tether-prev-source.json`/`tether-source.json` 只在 `./local.sh --build build` 时产生；drill `35-hot-update-exec-carrier` A/B/C 与重钉的 34 **均 NOT_RUN**（成本预告 ≈12–15 min，见 plan 头部；期间不跑 go test/lint/gates）；expected-verdicts 已登记 35 父行 + 三子行 GREEN、34 注记 S1 re-pin；drill-costs 34/35 行待实跑后补 |

**变异表**（每条守卫注入它声称能抓的缺陷）：

| # | 变异 | 守卫 | 结果 |
|---|---|---|---|
| M1 | `admit.go` 去掉 STALE 放行 | `TestProcessResumeAttachOpsTolerateStaleNodeButNotOffline` / `TestConvertedVerbSpecsMatchTheirHandlers` | RED `TestProcessResumeAttachOpsTolerateStaleNodeButNotOffline` |
| M2 | `reconcile.go` 两处 `StartedUnacked` skip 改 `if false &&` | `TestReconcileDefersOrphanKillForStartedUnackedRow` | RED `TestReconcileDefersOrphanKillForStartedUnackedRow` |
| M3 | `registry.validate` 异实例改报 `ErrIdentity` | `TestRegistryRestoreOwnsCopiesAndRequiresExplicitActivation`；`TestUnadoptedStartAndForeignInstanceAreRefusedDeterministically` | RED 两包（registry：`TestRegistryRestoreOwnsCopiesAndRequiresExplicitActivation`；execsession：`TestUnadoptedStartAndForeignInstanceAreRefusedDeterministically`） |
| M4 | `Revoke` 不 SIGKILL | `TestRevokeKillsLiveTasksAndRefusesPreRevocationLeases` | RED（5.02s：撤权后 child 仍活） |
| M5 | `exec.go` create 后一律 70 而非 74 | `TestReliableExecFailureClassTable` / `TestExitUnsettledIsPresentationOnly` | RED `TestReliableExecFailureClassTable` |
| M6 | `transientConnectErr` 恒 false | `TestConnectWithAuthRetryPolicy` | RED `TestConnectWithAuthRetryPolicy` |
| M7 | `Stamp` 只删精确键、保留大小写变体 | `TestStampPreservesUnknownFieldsAndOwnsTheActorKey`；`TestProcessResumeForwardPreservesUnknownFieldsAndStripsForgedActor` | RED 两包（`TestStampPreservesUnknownFieldsAndOwnsTheActorKey`；`TestProcessResumeForwardPreservesUnknownFieldsAndStripsForgedActor`） |
| M8 | `ctlConnOptions` 去掉 `IgnoreAuthErrorAbort` | `TestCtlConnOptionsIgnoreAuthErrorAbort` | RED |
| M9 | 真二进制：old-broker 用普通 broker（订阅面未装） | `TestExecReliableLegacyFallbackOnOldBroker` 自证 | RED「the broker still subscribes to process-resume.req」（`mut-oldbroker.log`） |
| M10 | probe.py 26 条合成负例 | `carrier`/`legacy-carrier`/`legacy` | 26/26 红，原因逐条核对（`probe_reasons.py` 输出） |
| M11 | prev-supply 8 负例（错 tag / 换 binary / 不可执行 / 无 source.json / 缺 sha / 构建失败三态 / 缓存 commit 不符） | `sim_prev_source_ok` / `sim_stage_prev` | 8/8 红 |
| M12 | r9d 23 条 drill 35 谓词负例 | `_final_ps_exited` 等 | 23/23 红 |

**覆盖限制**：running-abandoned（无人 ACK 的 running 任务 8 MiB 后背压）留 S2；`finish` 遇 `ErrFrozen` 留 S2；broker 重启窗内新请求静默降级 legacy（只记路径，S6 覆盖）；deploy 实跑 NOT_RUN（同意后补 §12.1）。

### 12.1 S1 内审后处置与最终候选树收据

内审报告：[hot-update-reliable-exec-review.md](hot-update-reliable-exec-review.md)（Fail → 处置 → 本节收据）。4 路审查 45 条 + 4 路核验 16 条；
处置摘要在报告 §2 逐条「主进程处置」。本节只记**收据**。

**真二进制 e2e 的变异要经 `GOFLAGS=-overlay=<json>`**（核验 e2e-V3）：harness 嵌套 `go build` 出 ctl/agent 二进制，`go test -overlay`
只改 harness 进程；本节 EA–EI / EC / M15 一律以 `GOFLAGS` 施加（`mut/run3.sh`）。`realstack_race_test.go` 哨兵让 `-race` 下的二进制同样插桩。

| 收据 | 命令 / 结果 |
|---|---|
| 修后定向包测试 | `go test -race` `./internal/execsession ./internal/execclient ./internal/procforward ./internal/procrequest ./internal/workload ./internal/cli` 全 ok；`./internal/agent`（74.1s）、`./cmd/tether`（129.0s）ok；`-run '^(TestExecReliable\|TestCtlConnect\|TestRealStack)' ./test/cli_e2e/` **-race ok 186.8s**（黑窗窗口延到 6.5 s、二进制带 -race 后从 102 s 增长） |
| 审查者新增测试 | 采纳 12 个（`revoke_inflight_start_test.go` ×2、`reconcile_started_unacked_test.go`、`forward_envelope_test.go`（按 protocol-V2 的新规则改写：路由键变体一律拒绝、actor 键变体仍剥离）、`forward_container_test.go`、`exec_guidance_test.go`、`exec_exit_class_admission_test.go`、`exec_stdin_test.go` ×2、`client_reject_callid_test.go`、`exec_interrupt_message_test.go`、`agent_sys_event_test.go`）；`tests/hot_update_process_probe_legacy_carrier_test.py` 折进 probe selftest（`loss before the first fault is not attributable`）后删除 |
| 主进程新增测试 | `TestConnectRetryEligibility`（表：login 三种动词、未激活、他 key、无收据）、`TestRunIdleAcksBackOffAndBytesResetTheCadence`、`TestRuntimeFinishDeliversExitWhenCompleteFails`、`TestReliableTaskSurvivesSessionRebuild`（进程内 agent + fireRedial 真重建，同 PID 续字节、exit 23）、`TestRegisterRequestCarriesStartedUnackedWhileStartedIsPending`（真 register.req 线上带 `started_unacked`）、`TestRealStackBuildPassesRaceThrough`；`TestConnectWithAuthRetryPolicy` 随 `timeoutErr` 搬到 `ctl_connect_test.go`（golden 手删 2 行：`exec_exit_class_test.go: TestAuthReceiptGate`（并入 `TestConnectRetryEligibility`）与 `…: TestConnectWithAuthRetryPolicy`（搬移）；`-update-test-inventory` 追加 20） |
| 三本清单 | test inventory −2/+20（上行）；wire inventory 不变（`ProcessRequestMaxBytes` 是常量不是字段）；matrix golden 不变；structural budget 不变（`handleSysEvent` 是包级函数） |
| simcluster hermetic（内审后） | probe selftest **65** 控制（`continuity` 1+21、`legacy` 1+7、`carrier` 1+16、`carrier --path legacy` 1+2、`legacy-carrier` 5+9，+1 空地板；D1 归因子句正负例各一）；r9d-nonvacuity **205 proved**（35 臂 B 加 ctl 侧停滞负例 ×2、臂 C 加 rc 2/rc 1 语义 ×2）；prev-supply **29 proved**（D5 脏树、更近更老可达 tag、deploy-V4 失败原因两条）；`sh tests/run-all.sh` 在 `make gates` 内 |

**变异表（内审后补录；`mut/summary2.log`、`mut/summary3.log`、`mut/M13.out`、`mut/M14.out`、`mut/M15.out`、`mut/M8-e2e.out`）**：

| # | 变异 | 守卫 | 结果 |
|---|---|---|---|
| M13 | execclient `call` 去掉 `IsReconnecting` 等待 | `TestRunKeepsAttachEpochAcrossReconnect`（E14 后：风暴断言本身） | RED「republished 4 times across the reconnect」 |
| M14 | `startExecPipes` 先 adopt 后 enqueue（E3） | `TestExecStartedEnqueuedBeforeAdoptAndSnapshotFlagsIt` 缝隙观测 | RED「pending=true, child adopted=true」 |
| M15 | `session()` 随会话 `stopProcessRuntime`（E5a） | `TestReliableTaskSurvivesSessionRebuild` | RED「op ack refused: process_runtime_closed」 |
| M8-e2e | `ctlConnOptions` 去 `IgnoreAuthErrorAbort`，经 GOFLAGS 进真二进制（E2） | `TestExecReliableCalloutBlackWindowMidStream` | RED rc=74（连接被永久关闭） |
| N1 | create 永不置 `created` | `TestRunReportsCreateSentOnlyAfterPublish` | RED |
| N2 | `Executed` 无证据置 true | `TestRunMarksExecutedOnlyAfterEvidence` | RED |
| N3 | `ProtocolError` 丢 `AfterCreate` | `TestRunSurfacesTypedRefusalWithPhase` / `TestRunNeverDowngradesAfterCreate` | RED |
| N4 | `isUnsupported` 恒 true | `TestRunNeverDowngradesAfterCreate` / `TestRunProbeAuthorizationErrorCannotDowngrade` | RED |
| N5 | 流中收到 `process_expired` 当作完成 | `TestRunConvergesOnExpiredSettlementOnlyWhenHoldingBothEnds/during_resume` | RED |
| N6 / N7 | `Code` / `started_unacked` 去 omitempty | `TestProcessOutputCodeAndStartedUnackedAreOmitEmpty` | RED ×2 |
| N8 | `Parse` 接受引号 call_id | `TestParseRefusesMalformedEnvelopes` | RED |
| N9 | broker version 检查先于 ACL | `TestProcessResumeRefusalOrderAndUnreadableCallID` | RED |
| N10 | `ErrInstance` 映到 `process_identity` | `TestProcessRefusalCodeTable` | RED |
| N11 | `revokeSessionWorkloads` 不 reap legacy | `TestSessionDestroyedRevokesReliableAndLegacyChildren` | RED |
| N12 | sweep 的 grace 无穷 | `TestSweepDiscardsAbandonedTerminalAndKeepsTombstone` | RED |
| N13 | 收据不看 key | `TestAuthReceiptIsPerSessionAndPerKey` | RED |
| EA | 重发的 create 铸新 RequestID | `TestExecReliableLostReplyRunsOnceAndSettlesOnce` | RED |
| EB | create 后 ctx 错误归 75 | `TestExecReliableTimeoutAbandonsWithoutRemoteKill` | RED |
| EC | `handleSysEvent` 不分派 session_destroyed | `TestExecReliableSessionRmMidStreamKillsChild` + `TestHandleSysEventRoutesSessionDestroyedToRevocationAndKeepsTheRun` | RED ×2 |
| ED | create 后 77 伪装 74 | `TestExecReliableRevokedMemberMidStreamExitsNoPerm` | RED |
| EE | attach spec 也放行 OFFLINE | `TestExecReliableNodeOfflineAfterCreateIsUnsettled` | RED |
| EF | probe 超时不映射 `ErrUnsupported` | `TestExecReliableLegacyFallbackOnOldAgent` | RED |
| EH | 信号死亡退 128 不加 sig | `TestExecReliableRealBinaryStreamsAndExitCode` | RED |
| EI | started 永不入队 | `TestExecReliablePsShowsTaskRunningThenExited` | RED |
| M1-e2e / M6-e2e | 去 STALE 放行 / `transientConnectErr` 恒 false（真二进制） | `TestExecReliableBrokerRestartMidStream` / `TestCtlConnectRetriesThroughCalloutBlackWindow` | RED ×2 |
| 未变异（如实） | `TestExecReliableAgentConnKickMidStream`、`TestExecReliableNATSRestartMidStream`（守的是 nats.go 重连 + 非空性 `NATS disconnected` 计数，产品侧没有一处「只影响重连续接」的单行变异；它们的失败态由 EE/EA 这类同族变异间接覆盖）、`TestExecReliableStdoutClosedEarlyKeepsRemoteTruthful`（「断流即重 Start」的变异需要改 attach 语义本身，等价于 EA）、`TestExecReliableLegacyFallbackOnOldBroker`（M9 自证已红） | — |

**内审新增守卫的变异**：`TestExecFailureGuidanceNamesExistingCommands`（把指引改回 `tether kill` → 红，审查者已验）、
`TestReliableExecAdmissionRefusalKeepsRetryableClass`（删 `createNeverAdmitted` 条目 → 74 → 红，表测试 `refusal_after_create_capacity_is_retryable` 同）、
`TestRevokeReachesChildAdoptedAfterRevocation` / `TestRevokeOfBackpressuredTaskStillReachesTerminal`（去掉 `start()` 的 revoked 重查 / `abortStalledRevokedLocked` → 红，即修前状态）、
`TestRunSurfacesCallIDZeroCreateRefusalInsteadOfResendingUntilTimeout`（修前红 → 修后绿）、`TestConnectRetryEligibility`（`verb != "login"` 字面比对 → 激活动词臂红）、
`TestStampKeepsEnvelopeFieldsAuthoritativeOverCaseVariants`（去掉 Parse 的变体拒绝 → 红）。

| 门（内审处置后，`s1-*-r4/r5/r6.log`） | r4：`make gates` rc=2（唯一红 `TestOnlyTheMatrixForksGoTest`，E4 重构让 `go build` 字面量不可证明——修法见报告 §6）、`make test` 同一条、`make e2e-parallel` rc=0 ALL PASS 4m42s；r5：`make gates` rc=2（唯一红 `assert-identity --check`：drill 35 断言文本 −1/+5，基线随改并注记）、**`make test` rc=0 1m52s（73 ok）**、**`make e2e-parallel` rc=0 ALL PASS 4m23s**；r6（只动了两份 hermetic .tsv 后重跑 gates）：**`make gates` rc=0 5m42s**（hermetic 34/34 ALL PASS、`make lint` 0 issues）；`GOOS=darwin GOARCH=arm64 go build ./...` rc=0 |
| 收尾 | `git add -A` → `git status --short \| grep -v '^[AMDR]  '` 为空；`git write-tree`（含报告 §6 之前）= `7164bde28af1e82eb2763fa06a223c6b862960e0`；**最终 index**（含本行之前的 evidence/报告，写入本行后不再变）= 见本节末行 |
| deploy 实跑 | **NOT_RUN**（drill 35 A/B/C + 重钉后的 34）：需用户同意，成本预告见 plan 头部（≈12–15 min，期间不跑 go test/lint/gates）；实跑后补 verdict 行 / `carrier.json` / `legacy-outcome.json` / 镜像 ID / vendor 三元组 / `drill-costs.tsv` 34、35 行 |
- **S1 候选树** `git write-tree` = `81e83ad5dcfc123c7ff5b8573211e56846309b88`——这是写入本行**之前**的 index（S0 内审 DOC-1 的教训：一行自引用的 hash 永远不能等于含它的树）。含本行的最终 index 由外审在暂存区上自己 `git write-tree` 取得；此后 evidence 不再改动直到 deploy 实跑或 S2。

### 12.2 S1 deploy-tier 实跑收据（2026-09-21，weilandserver 本机，期间不跑 go test/lint/gates）

原始目录：`~/tether-hot-update-handoff-20260920T2356Z/s1-evidence/`（`build.log`、`drill35.log`、`drill34.log`、`drill35-evidence/`（每臂每腿的 `carrier.json` / `legacy-outcome.json` / `calibration.jsonl` / before-post-final 全套）、`drill34-evidence/`、runner 的 `rollup.*` 与三臂 log、`mut/`、六轮 `s1-*.log`）。

| 收据 | 命令 / 结果 |
|---|---|
| 供给 | `./local.sh --build build` 首跑 rc=1：`tether-prev` 的 worktree 构建被 **fail-closed 拒绝**——`error obtaining VCS status: exit status 128`。根因：worktree 的 `.git` 是文件，go 的 VCS 根只认目录，于是一路向上撞到一个**空的 `/tmp/.git`**（2026-09-20 16:07 留下的杂物），在 `/tmp` 里跑 `git status` 失败。修：`sim_stage_prev` 的构建加 `GOFLAGS=-buildvcs=false`（worktree 构建本就不该带别的 checkout 的 VCS 戳；二进制身份 = VERSION ldflag + source.json 的 commit），并删除 `/tmp/.git`。重跑 rc=0 22 s：`tether-prev-source.json` = `{tag: v0.6.0, commit: a3431a12be3cf947cc797c28fea93382fa68472d, binary_sha256: 1cfd3ec6…0780, toolchain: go1.26.8}`；`tether-source.json` = `{head: cab90d1…, dirty_files: 215, tether_sha256: 6b180be0…1e5, tether_next_sha256: 08400b03…3fac, tether_prev_tag: v0.6.0}`；`vendor/tether-prev version` → `tether v0.6.0 (proto v2)`；镜像 `tether-sim:dev` = `sha256:ea43e4788dd1338d4dcbc0a25cb4aa83fe621327a62c09091b2a750d31865cb2` |
| drill 35 | `./run-drills.sh 35-hot-update-exec-carrier`（-j3）**92.7 s** rc=0：`GREEN 35….A rc=0 MATCH`（29 断言，90 s）、`GREEN 35….B rc=0 MATCH`（21，51 s）、`GREEN 35….C rc=0 MATCH`（40，84 s）、ARMS 行 `GREEN,GREEN,GREEN pass=90`，**NO DEVIATIONS**；无残留容器 |
| 35.A 记录 | `carrier.json`：`exec_start_lines=1 reliable_lines=1 rebuild_lines=1`，agent MainPID 296→296、ExecMainStartTimestamp 不变；faults `nats-restart` 源 52→152（7 s，ctl 侧注入时 54/54）、`nats-outage` 源 167→626（25 s + 恢复），ctl rc 23，final ps 恰一行 EXITED(23)；calibration post-A：10/10 rc 0、10/10 reliable、wall 227–246 ms |
| 35.B 记录 | `broker-restart` 源 53→131（5 s），rc 23；**B-new**（restart 发出后立刻起的新 `exec agt1 -- true`）rc 0、reliable、wall 2012 ms（连接级重试穿过 callout 黑窗，只记录） |
| 35.C1 记录 | v0.6.0 ctl：`reliable_lines=0 exec_start_lines=1 rebuild_lines=1`；`legacy-outcome.json`：`lost {stdout:450, stderr:450} / source_count 522`，`first_lost_seq 55/55 ≥ ctl_seq_before 54/54`（归因成立），ctl outcome `{finished:true, exit_code:23}`（legacy exec 的 exit chunk 经 liveConn 仍送达，丢的是字节）；今天的 agent 保持 RUNNING、最终 EXITED(23) |
| 35.C2 记录 | v0.6.0 agent（agt1 换 binary 后 MainPID 494，ONLINE at v0.6.0）：`probe_reached_lines=4`（`unknown forwarded verb … process-resume`）、`reliable_lines=0 exec_start_lines=1`、transcript 完整、rc 23 |
| drill 34 | `./local.sh drill 34-hot-update-process` **55.1 s** rc=3 **PRODUCT-RED**（pass=17 product_red=1 assert_fail=0 setup_red=0）：签名 `H0_LEGACY_EXEC_INTERRUPTED output=epipe management=exited_minus_one ctl=owner_lost_rc74 …` 匹配；`final/ctl-outcome.json` = `{finished:true, exit_code:74}`，ctl stderr `error: exec: process request: lease was issued by another runtime instance; task continuity lost — the remote task was accepted and its result is unknown (task 01m3297q… on agt1)`；正 oracle `_continuity` 如实拒绝（`original live task lost management`）。与 2026-09-20 的 pre-S1 运行（62 s，ctl `{finished:false}`）相比，ctl 半从「等到 360 s」变成「数秒内 74」，其余两半不变 |
| 基线/账本 | `drill-costs.tsv` +4 行（34：55 s；35.A/B/C：90/51/84 s）；README 行 34/35、expected-verdicts-log §34/§35、coverage-inventory、gotcha #90、§22.4 item 12 的 NOT_RUN 全部翻成观测；`sh tests/run-all.sh` 复跑见下 |

## 13. S2 agent 运行层 image ABI（plan：[hot-update-agent-image-abi-plan.md](hot-update-agent-image-abi-plan.md)）

基线 index tree `b342e0c1d0c6a693b9f5283210d042a09de29944`（S1 结束时）；本 stage 无 drill、不接产品路径。原始日志：`~/tether-hot-update-handoff-20260920T2356Z/s2-evidence/`。

### 13.1 出口逐条

| # | 出口 | 收据 |
|---|---|---|
| 1 | 五包 `-race -count=3` | `go test -race -count=3 ./internal/execsession ./internal/agentimage ./internal/workload ./internal/procrequest ./internal/spawnsafe` **rc=0**（38.1 s / 34.1 s / 35.5 s / 4.5 s / 3.2 s；`s2-exit1.log`） |
| 2 | 真实同 PID exec，execclient 得精确字节/顺序/rc | `TestRuntimeImagePreservesLiveChildAcrossExec` PASS（-race，2.3 s）：父进程真 `execclient.Run`（嵌入 NATS）跨 exec 收到 stdout `out:first\nsecond` + `descendant-tail`（顺序精确：后代在 exec 之后放行）、stderr `degraded-path\nerr:x` + `err-tail\n`、rc 23、`Executed`；receipt：OwnerPID==after PID、A/B/C 的 OS pid 与 start ticks 前后相等、`stdout_base_at_freeze=10 end=16`、`stderr 14/19`、`input_base<end=196608`、`blocked_requests≥1`、预算守恒、后代在 boot 时活、二次 Restore 为 nil、`memfd:tether-handoff`=0、每条业务 pipe 恰 1 引用、`spawned_after=0`、`TargetInstance=image-original`、B（ReapHeld 僵尸+后代持 stdout）`heldheld-tail` rc 37、C（冻结期 raw SIGKILL 僵尸）Signal 9、三任务 Exited 各恰 1；freeze→thaw 跨 exec **163 ms**（-race，校准样本非承诺）。负控制 `TestRuntimeImageOracleRejectsMissingStream` PASS（after 半 `IMAGE_ORACLE_REJECT`、非零退出、ctl 无结果）。Bundle 级 `TestBundleExecRestoresEveryDomain` PASS（四域，43 ms）。 |
| 3 | starting/in-flight 时 ErrBusy 或有界 defer；导入拒绝表无部分安装 | `TestRuntimeImageFreezeRefusesWhileStarting`（200 ms deadline → `procrequest.ErrBusy`，registry/泵未冻、并发 create 被 park、child 未死）、`TestSpawnInFlightDefersFreeze`（slot / probe 在飞 / 超时后 sticky-dead 保留 / Busy 钩子 / runtime in-flight Start → 各自 reason、零域冻结）、`TestFreezeDefersOnForeignProcessRows`；`TestRuntimeImageImportRejectsInvalidBundle` **40 行**（内审后：预算行改为「导入方上限」两行，+M28）（schema/instance/boot ID/secret/重复 task/无 record/handle/重复身份/预算三形/流形态七形/tombstone 三形/时钟两形/revocation/文件多少/声明缺席/plain file/互换/同形 memfd 互换/写端入源槽/外来行/终态行/缺行/重复行）每行：`ErrImage`、salvage 文件仍开且 CLOEXEC、fd 回基线、child 活、进程表不变、goroutine 不增；`TestBundleRestoreRejectsPartialBundleWithoutInstalling` 13 行（含未知 JSON 键两处、trailing data） |
| 4 | 四条必做变异各自红 | M1 漏 stderr replay → exec harness `IMAGE_ORACLE_REJECT`（标记行自证）rc=1；M2 courier 漏 exit → `TestCourierOutboxSurvivesImage` rc=1；M3 导入后重复预留 → `…RestoresPartiallyAckedOutputs`/`…RoundTripReleasesResources` rc=1；M4 漏 tombstone → `…PreservesTombstonesAndRevocations` rc=1。全表见 §13.2 |
| 5 | 五轮资源门；三硬闸；Darwin；登记 | `TestRuntimeImageRoundTripReleasesResources`（进程内，fd 容差 0）、`TestBundleFreezeRollbackReleasesResources`、`TestBundleRepeatedExecReleasesResources`（父进程基线，5 次真 exec）PASS；`GOOS=darwin GOARCH=arm64 go build ./...` rc=0；inventory +35、matrix golden +1（`TestLeakRiskMatrix` + `./internal/agentimage/...`）、leak anchors +3、enum 家族 +1（`spawnsafe.probeState`）、layering +1 行、ctx-none 站点 1（账本不增）、CLAUDE.md 闸门表 +1 行 +1 条；三硬闸与 `make gates` 见 §13.3 |
| 6 | dispatch 形状 | 单测试函数 + mode env + `-test.run=^Name$ -test.count=1`；`runtimeChildEnvironment` 双向剥离（业务 child 打印 `env::` 自证、after 半二次 `handoff.Restore()` 为 nil）；helper 只 `Prepared.Exec`（`spawned_after=0`） |

### 13.2 变异收据（`go test -overlay`，`-race -count=1`；驱动 `s2-evidence/mut/run.py`，每条 rc≠0）

| 变异 | 守卫 | rc |
|---|---|---|
| M1 Export 漏 stderr replay/source | exec harness（`IMAGE_MUTATION=M1` 标记行 + `IMAGE_ORACLE_REJECT`）| 1 |
| M2 ExportOutbox 跳过 exit | `TestCourierOutboxSurvivesImage` | 1 |
| M3 阶段③每份 log 二次 reserve | `…RestoresPartiallyAckedOutputs` / `…RoundTripReleasesResources` | 1 |
| M4 Export 不写 tombstones | `…PreservesTombstonesAndRevocations` | 1 |
| M5 Import 铸新 instance | exec harness（标记行；ctl 得 `ErrInstance`）| 1 |
| M6 registry.Freeze 忽略 starting | `…FreezeRefusesWhileStarting`（child 被 SIGKILL）| 1 |
| M7 Thaw 对终态任务也起 observer | `…RestoresPartiallyAckedOutputs`（Exited 2 次）| 1 |
| M8 去 observe 的 frozen gate | `…AccountsExitDuringFreeze`（第一版两臂 child 没关 stdio、泵冻结后永不 ready，变异存活；改为 child 先关 stdio 让 observe 在 fence 前已 ready → 变异释放 stdin 被直接 oracle 抓住）| 1 |
| M9 Output.Freeze 按 paused 拒绝 | `TestOutputFreezeAfterAbortExportsEndedStream` | 1 |
| M10 Handle 冻结期拒绝而非阻塞 | `TestRuntimeFrozenFenceBlocksUntilThaw` | 1 |
| M11 spawnsafe.Export 忽略 probesInFlight | `TestPolicyExportRefusesInFlightSpawnAndProbe` | 1 |
| M12 Thaw 吞掉 Manager 冻结 | `TestRuntimeFrozenFenceBlocksUntilThaw` | 1 |
| M13 Revoke 冻结期仍 Signal | 同上（第一版用 `kill(pid,0)` 判活——僵尸也成功，变异存活；改为 `/proc/<pid>/stat` 非 Z）| 1 |
| M14 phase 改由 done/terminal_at 派生 | `…ExportsTaskBetweenFinishAndConclude` | 1 |
| M15 Discard 不关原件 | `…RoundTripReleasesResources`（fd 门）| 1 |
| M16 courier 在 fence 前导出 | `TestBundleRoundTrip`（stub 记录 fence 状态）| 1 |
| M17 Freeze 跳过 Busy 钩子 | `TestSpawnInFlightDefersFreeze` | 1 |
| M18 去外来行只读预检 | `TestFreezeDefersOnForeignProcessRows` | 1 |
| M19 Frozen.Thaw 漏 Manager.Thaw | `TestBundleFreezeRollbackReleasesResources` | 1 |
| M20 删 Secret.Format | `TestRegistryRestoreWithClockAndRedactedSecret` | 1 |
| M21 去 running-abandoned 臂 | `TestSweepDiscardsAbandonedRunningOutput` | 1 |
| M22 Discard 改成 Abort | 同上（child 再写不成功）| 1 |
| M23 Import 接受外来行 | `…ImportRejectsInvalidBundle` | 1 |
| M24 Import 跳过 dev/ino 指纹 | 同上 | 1 |
| M25 spawnsafe.Import 把 dead 装成 unprobed | `TestPolicyImportPreservesVerdictsAndSkipsForeignGeneration` | 1 |
| M26 courier 导入不对账 payload | `TestCourierImportRejectsInconsistentEntries` | 1 |
| M27 Thaw 丢掉补杀 | `TestRuntimeFrozenFenceBlocksUntilThaw` | 1 |
| G1 execsession 新增带 ctx 的导出方法（真改磁盘再还原；overlay 对扫源码的门不可见）| `TestAgentImageEntryPointsAcceptNoContext` | 1 |
| G2 `TaskImage.LastTouch` 改 tag | `TestImageFieldInventoryAppendOnly`（dangling）| 1 |
| G3 账本删一行 → `-update` 只增回；改一行 → `-update` 仍拒（rc=1）| 同上 | 0 / 1 |
| **内审后新增（R1 / 处置守卫）** | | |
| M28 `closeTask` 空操作（Close 后什么都不关） | exec harness 的按类别 fd 收据（`Pipes:6 ReplayMemfds:6` 留存 → `descriptors left after the after half closed everything`）；**修 R1 前** harness 用 `FDAfterClose > FDAfterImport` 对此绿 | 1 |
| M29 `salvageOf` 丢掉 bundle | `TestBundleRestoreRejectsPartialBundleWithoutInstalling`（每行 `salvage.Bundle()` 非空 + `salvage.Prepare()` 可执行）| 1 |
| M30 `outputsEnded` 忽略 `Discarding`（F1 回退）| `TestFenceDiscardingStreamsReleaseReapHoldWhileDescendantLives` | 1 |
| M31 Close 在 fence 下既不 thawLocal 也不拒绝（F4 簇回退）| `TestFenceCloseUnderFrozenTableLeavesChildAlive` / `TestFenceCloseOnFrozenUnexportedRuntimeKeepsExitAccounting` | 1 |
| M32 revocation 时刻界回退到 `FrozenAtBootNS`（protocol-V1 回退）| `TestRuntimeImageRevocationUnderFenceIsCarriedOutAfterImport` | 1 |
| M33 Detach 忽略 `thawed`（F3/R2 回退）| `TestFenceDetachAcceptedAfterThaw` | 1 |
| M34 删 `Salvage.Format` | `TestSalvageRedispatchesTheSameBundleWithoutRenderingTheKey`——**第一次 rc=0**：子串 oracle 看不见 `%#v` 把 raw 印成十六进制字节；加「渲染 ≤256 字节才是摘要」后红，同时暴露 `%+v` 对**解引用值**仍会把 raw 按十进制字节整段印出（String/Format 是指针接收者）→ 改为值接收者 | 0→1 |
| M35 Freeze 最终 ErrBusy 不 Warn（F5 回退）| `TestRuntimeImageFreezeRefusesWhileStarting` | 1 |
| M36 阶段 ① 采用 image 的 `budget_limit`（I2/R6 回退）| `TestRuntimeImageImportRejectsInvalidBundle`——**第一次 rc=0**：预算三行在阶段 ③ 的 reserve 失败上照样红，分不出「阶段 ① 拒」与「阶段 ③ 拒」；给三行加 `pure: true`（拒绝时 taken 必须为 0）后红 | 0→1 |

### 13.3 硬闸与候选树收据（内审前）

| 闸 | 命令 | 结果 |
|---|---|---|
| lint | `make lint` | 第一轮 6 条（`validateImage` maintidx MI 8 → 拆成 validateTaskImage/validateOutputImage/validateImageBudget/validateImageOwners/validateImageRows；revive bool 字面量；staticcheck SA4006；unused 字段；wastedassign；ineffassign）→ 第二轮 **0 issues**（`s2-lint-r1.log`/`s2-lint-r2.log`） |
| make test | `make test; echo rc=$?` | **rc=0**，74 个包 ok（`s2-make-test-r1.log`） |
| make e2e-parallel | `make e2e-parallel; echo rc=$?` | **ALL PASS rc=0**，74 单元，wall **4m16.7s**（`s2-e2e-r1.log`；`TestLeakRiskMatrix` 含 `./internal/agentimage/...`） |
| make gates | `make gates; echo rc=$?` | **rc=0**（vet-tags → architecture/determinism/… → run-all.sh hermetic 闸集 → lint 0；`s2-gates-r1.log`） |
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./... && go vet <五包+agent>` | rc=0 |
| 五包 race×3 | §13.1 #1 | rc=0 |

归档：`~/tether-hot-update-handoff-20260920T2356Z/s2-evidence/tether-gen1`（`CGO_ENABLED=0 go build -trimpath ./cmd/tether`，SHA-256 `8571c4428b1c8a475969bac90c26e4877e35f25839e5530ac90df17c54057af4`，README 标「ABI 代际机制证明（dev gen-1），非 release 级 N-1」）；同目录：六份闸门日志、`mut/`（27 条 overlay 变异的副本/overlay/输出 + G1–G3）、`plan-synthesis.md`（workflow 综合稿原文）。

候选树（内审前）：index 仍是 S1 的 `b342e0c1d0c6a693b9f5283210d042a09de29944`——S2 全部未暂存/未跟踪，内审处置后 `git add -A` 再记最终树。

### 13.4 内审处置后的收据（报告：[hot-update-agent-image-abi-review.md](hot-update-agent-image-abi-review.md)）

**与 plan 的差异**（实现优于 plan 的字面或 plan 写错的地方，按 §3 step 5 记录而不是回改 plan 正文；plan 已加「定稿修订」与内审订正）：

| # | plan 写的 | 实际交付 | 为什么 |
|---|---|---|---|
| 1 | §4 item 6「`outputsEnded` 无需改（ReadError 已非空）」 | `outputsEnded` 把 `Discarding` 视为仍活 | 内审 F1：Discard 后泵还在读活管道，按 ReadError 判 ended 会在 child 活着时 AllowReap，AdoptDraining 的 hold 丢失、后代逃过撤销 |
| 2 | §1.5 Close 行「thawLocked 后 teardown」（所有 frozen 列） | 表仍冻结 → **拒绝** `workload.ErrFrozen`；表已 thaw → thawLocal 后 teardown；exported → Discard | 内审 F4/V1/R3/P6：在冻结的表下 Signal 是静默空操作，「先 thaw 再 teardown」在那一格做不到，拒绝比留下活 child 诚实 |
| 3 | §3.6 `Census{Wedged, ProbesPending, ProbesRetained}` + Gen 整表门 | `Census{Wedged, ProbesInFlight, ProbesRetained}`；**无 Gen 门**，`Import` 按每个 mount 的签名逐条匹配并返回 installed 计数 | 内审 I3：Gen 在任何 mount 表变化时把全部判定静默丢掉，测试只能靠伪造 Gen 才走到逐条路径 |
| 4 | §3.5 `ImportOutbox` 前提「`len(pending)==0` 且 run 未起」 | 只检查空表 | courier 的 loop 在 S3 bootstrap 里由同一路径在 Import 之后才起；一个只能在本进程被违反的前提是死代码 |
| 5 | §6 账本门在 `internal/agentimage/image_inventory_test.go` + Makefile gates 行 | `test/architecture/image_field_inventory_test.go`（`make gates` 按目录前缀直接跑） | 与其它源扫描门同处；账本 reflect 走查对未导出嵌入结构体做提升（内审 I4） |
| 6 | §5.1 `TestRuntimeImageHelperEnvironmentIsStripped` 单列 | 折进 exec harness（child 打印 `env::` 自证 + after 半二次 `Restore()` 为 nil） | 定稿修订 #6；plan §5.1 那一行是残留 |
| 7 | §2.5 阶段 ② 含「SetRead\|WriteDeadline(zero)==nil 可轮询证明」 | 可轮询证明在阶段 ③（`os.NewFile` 进 poller）；阶段 ① 补 `validReplayState`（Version/Base≤End/Capacity 界/End−Base≤Capacity）| 内审 I7：只有构造才能证明可轮询；数据形状的检查提前到 ① 让 malformed log 在取任何 fd 前被拒 |
| 8 | §2.1 `budget_limit`「Import 用新二进制的 limit」 | 第一版实现**采用**了 image 的 limit；处置后按 plan：用导入方 `Config.ReplayBudget` 校验 `used ≤ limit`，image 字段只作信息 | 内审 I2/R6——这是实现偏离 plan，不是 plan 错 |
| 9 | §1.5 Export×frozen+imported「允许」/ Detach 无「未 Thaw」前提 / Discard×closed = nil | Export 允许（`buildTask` 填 `t.frozen`）；Detach 要求 `!thawed && !exported`；Discard×closed = `ErrClosed` | 内审 F2/F3/F7/P7/R2——前两条是实现补上 plan 的承诺，第三条是 plan 表格写错 |
| 10 | §10 风险 9 / §22.5 item 12「`exported` 标志拒绝 Thaw」 | Thaw 不看 `exported`（exec 失败后的回滚就是 Thaw 一个 exported 的 runtime）；导入成功后只 Discard 是调用纪律 | 内审 F6/P2/import-V2 |
| 11 | §3.4「RestoreProcesses 是最后一个可失败步骤」 | 预检（`Courier.Check` / `Spawn.ImportReady`）先于任何安装；RestoreProcesses 是最后一个**内核校验**步骤；其后 courier/spawn 安装失败经 Detach + Discard 回滚 | 内审 I5/protocol-V3 |
| 12 | §2.3 拒绝表「42 行」 | **40 行**（R14 由阶段 ② `CheckReplayFile` 覆盖、R17 由外来行/记录不符覆盖，不单列；预算行拆两行；加 role 行） | 内审 P3 |

**处置后的验证**（每条 rc 单独取，不经管道；日志在 `s2-evidence/` 同名 `-r2` 文件）：

| 项 | 命令 | 结果 |
|---|---|---|
| 变异复跑 | `run.py`（M1–M36 全表；`s2-mut-r2.log`）| **36/36 rc=1**（M6 第一次 EDIT-MISS——F5 改了 `Registry.Freeze` 的源码文本，驱动的 exact-match 落空并**照跑了未变异的源**得 rc=0；驱动改为 EDIT-MISS 即中止，M6 按新文本单独复跑 rc=1）；G1–G3 未重跑（门源码未再改） |
| 五包 race×3 | `go test -race -count=3 ./internal/execsession ./internal/agentimage ./internal/workload ./internal/procrequest ./internal/spawnsafe` | **rc=0**（42.8 s / 36.1 s / 37.6 s / 4.4 s / 3.1 s；`s2-exit1-r2.log`） |
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./...` | **rc=0** |
| lint | `make lint` | **rc=0**，0 issues（`s2-lint-r3.log`） |
| make test | `make test` | **rc=0**，74 个包 ok（`s2-make-test-r2.log`） |
| make e2e-parallel | `make e2e-parallel` | **ALL PASS rc=0**，74 单元，wall **4m5.0s**（`s2-e2e-r2.log`） |
| make gates | `make gates` | **rc=0**（`s2-gates-r2.log`） |
| 测试身份清单 | `go test ./test/determinism/ -run '^TestTestFunctionInventoryOnlyGrows$' -update-test-inventory` | 相对 S1 index **+52 行**（内审前 +35，内审后 +17：审查者 9 + 主进程 8）；`TestFenceCloseOnFrozenUnexportedRuntimeLosesExitAccounting` 在登记前改名为 `…KeepsExitAccounting`（未进过 index，golden 直接改） |
| 序列 | `s2-final.sh`（变异 → race×3 → Darwin → lint → test → e2e → gates，串行、每步独立 rc）| 2026-09-21 15:02:21 → 15:17:08（`s2-final-rc.txt`） |
| 收尾 | `git add -A` → `git status --porcelain \| grep -v '^[AMDR]  '` 为空 | 通过 |

- **S2 候选树** `git write-tree` = `e6815c90229c115c116b2c51788cb012e3a1534a`——这是写入本行**之前**的 index（S0 内审 DOC-1 / S1 §12.1 的同一条规矩：自引用的 hash 永远不能等于含它的树）。含本行的最终 index 由外审在暂存区上自己 `git write-tree` 取得。

## 14. S3 agent bootstrap 与 node upgrade 接线（plan：[hot-update-agent-bootstrap-plan.md](hot-update-agent-bootstrap-plan.md)；内审：[hot-update-agent-bootstrap-review.md](hot-update-agent-bootstrap-review.md)）

基线 = S2 最终 index 树 `ddd301c878841631bab63e283cd3ac66cdff0904`（HEAD 仍是 `cab90d1`，S0–S3 均未 commit）。原始日志与 Astra 报告在仓库外
`~/tether-hot-update-handoff-20260920T2356Z/s3-review/`（prompts、五路审查 + 五路核验报告、每个 Astra 作业的最终回复）与
`s3-evidence/`（`gates-run*`、`drill-run*`、`mut/`）；本节是它们的摘录。

**开发方式（本 stage 起）**：用户 2026-10-02 裁定改为 GPT-6-Astra（Codex `--model gpt-6-astra --effort xhigh`）并行、不设上限；每个作业一个
从未引用快照 commit 派生的 detached worktree，文件所有权互斥，主进程收 `git diff --cached --binary` 后 `git apply --3way` 进集成树；
每个后台作业由 Monitor 盯住。实现 8 包（boot / cut / ctlcodes / gates / drill / mixedprobe / chain / e2e）+ docs；内审 5 路审查 + 5 路核验
（pipeline，不设屏障）；处置 7 包（fix-cut〔+ followup〕/ fix-courier / fix-gate / fix-res / fix-misc / fix-drills / fix-docs）+ fix-lint。

### 14.1 出口逐条（plan §0）

| # | 出口 | 收据 | 结论 |
|---|---|---|---|
| 1 | drill 34 GREEN，父行 DERIVED GREEN | §14.4：七臂 GREEN ×3 次扫（最终镜像上 2 次 + 首扫 6/7），`validate-verdicts` NO DEVIATIONS；`expected-verdicts-log.md §34` S1 段落 verbatim + S3 实跑段 | **达成** |
| 2 | 新臂 + 负控制 + 导入失败臂 | 34 A/G/U/N/P/W/F GREEN；39 X GREEN（`image_abi_unsupported` 64、盘面不动、原 ctl rc23）、39 Y PRODUCT-RED（预测签名逐字命中，无需重钉）；导入失败回滚 = hermetic 真二进制 `TestBundlePresentAndBudgetExhaustedRedispatchesToPrev`、`TestImportFailureNeverRegistersEmptyTable` + 34.W 真实再派发 | **达成** |
| 3 | drill 33 不劣化；B 臂 watchdog 有 bundle 时行为明确 | 首跑 **ASSERT-FAIL**（C2，产品缺陷，见 §14.3）→ 修复后 INCOMPLETE rc=4、pass=29、nc_gap=1（#73）、B4 合取成立。plan 写的「agent slog 多出 image cut / re-dispatched / activated 各一行」**未被 33 断言**（plan 同时写「33 不改脚本」，容器随 teardown 销毁）；同一组日志行由 34.W 断言（generation 2、两次 cut 收据、`resource_phase=redispatched`） | **达成（附注）** |
| 4 | hermetic 命名测试 race×3 | `go test -race -count=3 -timeout 60m ./internal/agent ./internal/agentboot ./internal/agentimage ./internal/handoff ./internal/upgradebudget` rc=0（§14.5 序列 2）；首跑两处红已处置（超时 + S2 测试清场 flake，内审报告 §3） | **达成** |
| 5 | freeze→新泵可用 ≤500 ms 有样本（校准数据，不设断言） | §14.4 表：最终镜像上 9 个 activation 样本 `freeze_window_ms` 107–141 | **达成** |
| 6 | 三硬闸 + gates + Darwin；结构预算 | §14.5 序列 2：Darwin / vet-tags / lint / make test / e2e-parallel / make gates 全 rc=0；`type-methods internal/agent.Agent 125` 不动 | **达成** |

### 14.2 新增测试与变异

测试身份清单相对 S2 index **+74 行**（S3 实现 + 内审处置 + §14.3 的 1 条）。内审处置的变异收据见内审报告 §5（FIX-CUT 17 + 11、FIX-COURIER 1、
FIX-GATE 7、FIX-RES 8、FIX-MISC 20、FIX-DRILLS 48、FIX-LINT 2，均回注原缺陷后变红；主进程复核 M9 rc=1）。

### 14.3 deploy-tier 首跑暴露、hermetic 与内审都没覆盖的问题

| # | 现象 | 归属 | 处置 |
|---|---|---|---|
| D1 | 34.G ASSERT-FAIL `HOT_UPDATE_EVIDENCE_INVALID: empty activation import_ms` | **drill oracle**：`shim_ms`/`import_ms` 是 `Duration.Milliseconds()` 截断，<1 ms 的步骤合法为 0（gen-2 hop 导入 <1 ms） | judge 只对 `generation`/`tasks`/`freeze_window_ms` 要求 >0；selftest 加 sub-ms 正例与 generation=0/tasks=0 负例；变异（恢复全正规则）→ probe-test rc=1，还原 rc=0；34.G 复跑 GREEN |
| D2 | 33 C2 ASSERT-FAIL：`agent_rejected:install_failed` "running image differs from the on-disk binary" | **产品缺陷**（plan §2.1 第 3 条与 §8「33 C2 不变」自相矛盾）：共享二进制 sibling 的 dst 已是同一候选，被永久拒绝 | 不变量改为 prev ∈ {运行映像, 候选}，判定移到 extract 后（`installPolicy.runningSHA`）；`TestInstallPrevSlotAcceptsOnlyRollbackCapableImages` 四行；变异 M1 恢复 plan 原文 → sibling 行红、M2 去门 → unrelated 行红、M3 忽略哈希错 → unhashable 行红，还原后绿；`go test -race ./internal/agent ./test/p10 ./test/security ./cmd/tether` rc=0；33 复跑 INCOMPLETE（预期） |

### 14.4 deploy-tier 实跑（weilandserver 本机；期间不跑 go test/lint/gates，结束无残留容器）

最终身份：tree `7094d4bb7823c4f63c35729d5468f4e5363e0f6b`（D2 修复后、文档收尾前），镜像 `sha256:4fe3646bf38eb8b611032213ce82106b127dbf0e56cc2479eb1e884da662ead0`，
`vendor/tether` `81eb1d769c4772a8f355d4d7ebfd51f8c42858ecc7a5435d681ea09d883b124b`、`tether-next` `73415cded09e6d7cef8a93a79821d31561f3d88cdf0b1978452622da736647ba`、
`tether-prev`（v0.6.0）`1cfd3ec693b211bfd2844f7fea219f1e10b7b8dffc9185e558bcdbdf16230780`，go1.26.8。首扫镜像 `c346a962…`（tree `122ffb53`，D1/D2 修复前）。

| 时间 | 命令 | 结果 |
|---|---|---|
| 14:04–14:08 | `run-drills.sh -j4 --no-attribute 34-hot-update-process`（首扫，`c346a962`） | 6/7 GREEN，**G ASSERT-FAIL**（D1） |
| 14:10–14:12 | `run-drills.sh --no-attribute 34-hot-update-process.G`（D1 修复后） | GREEN，72 s，NO DEVIATIONS |
| 14:12–14:14 | `run-drills.sh -j2 --no-attribute --allow-product-red 39-hot-update-legacy-n1` | X GREEN / Y PRODUCT-RED（签名命中），119 s，NO DEVIATIONS |
| 14:14–14:17 | `./local.sh drill 33-node-upgrade-success` | **ASSERT-FAIL** rc=1（C2，D2） |
| 14:17–14:19 | `run-drills.sh -j3 --no-attribute 35-hot-update-exec-carrier` | GREEN ×3，93 s |
| 14:31–14:42 | `./local.sh --build build` → 33 → 34 → 39 → 35（`4fe3646b`） | 33 **INCOMPLETE rc=4**（pass=29、nc_gap=1、206 s）；34 **GREEN ×7**（218 s）；39 X GREEN / Y PRODUCT-RED（119 s）；35 **GREEN ×3**（A 94 / B 55 / C 87 s）——全部 NO DEVIATIONS |
| 14:43–14:49 | 34、39 再扫（只为留每臂秒数） | 34 GREEN ×7（220 s：A 63 / G 77 / U 68 / N 72 / P 102 / W 217 / F 81 s）；39 X 64 / Y 117 s |
| 14:51–14:55 | 34 再扫（留全部 evidence 目录） | GREEN ×7，218 s，NO DEVIATIONS |

**校准数据（不是预算断言；最终镜像，`agent: image activated` 行）**：

| 臂 | generation | tasks | freeze_window_ms | shim_ms | import_ms |
|---|---|---|---|---|---|
| A | 1 | 1 | 109 | 97 | 0 |
| G | 1 / 2 | 1 | 113 / 114 | 101 / 102 | 0 / 1 |
| U | 1 | 1 | 110 | 97 | 1 |
| N | 1 | 1 | 111 | 98 | 1 |
| P | 1 | 3 | 141 | 116 | 8 |
| W | 1 / 2（watchdog 再派发进 prev） | 1 | 107 / 110 | 94 / 4 | 0 / 0 |
| F | 1 | 1 | 110 | 97 | 1 |

`LocalFreezeWindow` 预算 500 ms，最大样本 141 ms（P，3 个 task）。shim 的 ~100 ms 是 20 MiB sha + 两次 fsync；W 的 gen-2 shim 4 ms 是回滚进
prev 的那一跳：marker 已是 `rolled_back`，`bootUpgradeCheckLocked` 在非 pending 时直接返回（`upgrade_state.go` 的
`m.State != upgradeStatePending` 早退），不哈希、不 fsync；G 的 gen-2 是第二次真升级（marker pending），shim 仍 ~100 ms。34.P 混合流量（P1，10 req/s，`until_mark=activated` + 10 s 尾）：124 个请求（read 42 / short 41 /
write 41）零失败、零 `agent_upgrading`、零重复/无主副作用；cut→activated 203 ms，窗口内 8 个请求、5 个写；write p99 1.58 s（P0 校准 1.08 s）、
short max 0.59 s；`exec-paths`：reliable 82 / legacy 0 / restored 1 / cut 1。

### 14.5 硬闸与候选树收据

| 项 | 命令 | 结果 |
|---|---|---|
| 序列 1（D1/D2 修复后、收尾文档前；tree `996a5018f11f49ccc72ec0dad16a2437ea703b7a`） | `s3-gates2.sh`：Darwin → vet-tags → lint → test → race×3 → e2e → gates，串行、每步独立 rc | 14:55:54 → 15:23:04 |
| Darwin | `GOOS=darwin GOARCH=arm64 go build ./...` | **rc=0** |
| vet-tags | `make vet-tags` | **rc=0** |
| lint | `make lint` | **rc=0**，0 issues |
| make test | `make test` | **rc=0**（结构预算门含 `type-methods internal/agent.Agent 125` 不动） |
| 出口 4 race×3（首跑） | `go test -race -count=3 ./internal/agent ./internal/agentboot ./internal/agentimage ./internal/handoff ./internal/upgradebudget` | **rc=1**：agent 撞默认 10 min 超时（×3 实需 877 s）；agentimage 描述符普查「20 vs 21」——内审报告 §3 最后一条（S2 两个测试的清场缺陷，非产品） |
| 出口 4 race×3（复跑） | 同上去掉 agentimage、加 `-timeout 60m` | **rc=0**（agent 877.1 s / agentboot 65.9 s / handoff 85.3 s / upgradebudget 1.0 s） |
| 出口 4 agentimage（测试修复后） | `go test -race -count=3 ./internal/agentimage` | **rc=0** ×2（主进程）+ ×5 与 `GOGC=1 -count=5`（Astra，`s3-evidence/fdflake-verification.txt`） |
| make e2e-parallel | `make e2e-parallel` | **ALL PASS rc=0**，74 单元，wall **8m2.2s** |
| make gates | `make gates` | **rc=0**（hermetic 闸集 ALL PASS，lint 0） |
| **序列 2（最终树：含描述符 flake 的测试修复与全部收尾文档；tree `b16a4d5f1e43d892feae052312819cc7613eab96`）** | `s3-gates3.sh`：同序列 1，race×3 含 agentimage 且带 `-timeout 60m` | 15:52:29 → 16:23:15 |
| Darwin / vet-tags / lint | 同上 | **rc=0 / rc=0 / rc=0**（0 issues） |
| make test | `make test` | **rc=0**（76 个包 ok） |
| 出口 4 race×3 | `go test -race -count=3 -timeout 60m ./internal/agent ./internal/agentboot ./internal/agentimage ./internal/handoff ./internal/upgradebudget` | **rc=0**（agent 878.1 s / agentboot 65.7 s / agentimage 61.4 s / handoff 85.2 s / upgradebudget 1.0 s） |
| make e2e-parallel | `make e2e-parallel` | **ALL PASS rc=0**，74 单元，wall **8m6.3s** |
| make gates | `make gates` | **rc=0**（hermetic 闸集 ALL PASS，lint 0） |

出口 4 的命令须带 `-timeout 60m`：`internal/agent` ×3 实需约 880 s，plan §0 原文照抄会在默认 10 min 处 panic（不是测试失败）。

序列 2 之后只改了文档（本节最终行、§14.1 出口 4/6 结论、`INDEX.md` S3 行）；`go test ./test/architecture/ ./test/determinism/` rc=0、
`sh test/simcluster/tests/run-all.sh` rc=0 复核。收尾 `git add -A` 后 `git status --porcelain | grep -v '^[AMDR]  '` 为空。

- **S3 候选树** `git write-tree` = `f366a046b56ed827f6dcd7e7ef19c28df88c28e1`——写入本行**之前**的 index（自引用的 hash 永远不能等于含它的树）。含本行的最终 index 由外审在暂存区上自己 `git write-tree` 取得。


## 15. S4 pty-terminal 证据骨架

2026-10-03，S4-EVIDENCE 按冻结 plan、43 份 package/fix 报告及原始 artifact 填写。**T01–T48：47 行收据完整、0 行部分、T44 一行 NOT_RUN（无 Darwin runner）；补充 T49/T50 两行完整。** 本次核验保留 321 条独立 mutation 收据，其中 270 条来自原包原始日志，51 条由本 worktree 补跑；本次 51 条全部击红，源文件全部恢复。规定缺陷分别签收，额外变异也逐条列出；一条红灯不代签整组。

“完整”指本行有实际 baseline、逐条 mutation 的命令/独立 rc/断言/身份及 raw log、实际 restore。**不表示把历史 mutation 重跑到当前树，也不表示全栈出口已签同候选。** 历史命令保留原工作目录/overlay 路径；270 条历史 mutation 的原始 source/driver、日志、JSON 已复制并计算 SHA256。T47 的历史 on-disk driver 没保存改后源文件，采用已归档生成器 SHA + 精确 case + 单条日志，不虚构原始源文件 SHA。当前 baseline/restore 是新的独立收据。

全部长日志、逐行命令和 mutation 身份位于 `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/`（本节缩写，绝对根路径见下）；入口为 [逐行索引](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/row-summary.json)、[逐条 mutation](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/mutations-by-row.json)、[历史 artifact SHA 清单](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive-manifest.json)和[报告来源清单](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/report-references.json)。原始 result-p2/p7/p13 独立报告不在给定目录；其职责由保存的 core/P6/P9/P10 集成收据覆盖，未把缺失报告当作运行证据。S0–S3 原文不改。

### 15.1 环境与候选身份

| receipt | 实际身份与范围 | 状态 / raw receipt |
|---|---|---|
| 环境 | 2026-10-03；`weilandserver`，Linux `6.8.0-139-generic`、x86_64；`go1.26.8 linux/amd64`。本次命令最多四路并发；负载以 `uptime` 原样存档，不假设机器空闲。Go 测试均 `-race -count=1`，另有 ptysession 整包 `-count=3`；Python 用真实本地 PTY/fake fixture，仅离线判官重放 | [candidate.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/candidate.json)，environment 命令各 rc=0 |
| 给定候选 | HEAD `deab3c074739cce501e8b8f189c83caf7a11ac29`；HEAD tree 与 `git write-tree` 均 `4295f498af8a1cd931dca52f48818ba1402e4ee4`；起始 status/diff 为空，空 diff SHA256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`。本次不暂存；以上 index tree 不包含随后补写的两份文档 | `git rev-parse HEAD HEAD^{tree}`、`git write-tree`、`git status --porcelain=v1`、`git diff --binary` 各 rc=0；[原始命令](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/candidate.json) |
| 当前构建 | `go build -o /home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/tether-candidate ./cmd/tether`，rc=0；binary SHA256 `c368185d2f15cff5a952734a54e34f1053c2fcf54a9de95d7e1db08c87ffb1cb`。未运行 installed tether，未读写生产 HOME | [build receipt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/candidate-build.json)；真 CLI 测试自己的 sandbox/race binaries 与此非 race artifact 分列 |
| wire diff | 相对 S3 tree `f366a046b56ed827f6dcd7e7ef19c28df88c28e1` 的 inventory：新增 **18** 行（request 8/reply 10），零删除/旧行改变；`ProtoVersion=2`、`ProcessResumeVersion=1` 不变 | `git diff f366a046 HEAD -- internal/proto/testdata/wire_field_inventory.txt test/architecture/testdata/image_field_inventory.txt` rc=0；[wire diff/两侧 SHA](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/wire-inventory-diff.json)；T45 正反收据 |
| image diff | ABI1；inventory 实数 **58** 新行、零删除，覆盖 Kind/PTY/ClockUnknown/SessionMember/terminal identity/control/tombstone。P16 当时“57”不代签当前 58。纯 exec 的新增外层字段 omitempty；PTY 内部责任字段按 schema 保留；kind-aware master/input/output 文件表由 T19/T23/T24/T34 验证 | [image diff/两侧 SHA](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/image-inventory-diff.json)；T46 真 S3 reader 来源是 `0ec45bfeab0e614c1f900e8b29cb87e0091de981`（不是冒称 final S3 f366），5 次同 PID Decode/Restore/Thaw |
| 预算 pin | Go↔shell 全量对账：PTYRecovery=180s、PTYRecoveryOpportunity=5s、PTYInputBytes=262144、PTYBatchBytes=32768；ComponentHandover=120s、RequestLatency/LiveFlowStall/SafePointWait=5s、LocalFreezeWindow=500ms 保持。后者仅校准；结构 golden 未放宽、Agent=125 | T47 `TestUpgradeBudgetsAgreeAcrossGoAndDrill` / `TestUpgradeBudgetPinControls`、`TestStructuralBudget`；独立删除/漂移变异见 [T47](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T47.md) |
| drill 候选 | run5 镜像 `78f2c62d`（review §4 与成本账本）；`tether-source.json` 写 HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025` / dirty_files=427。old running SHA `41c57cafa361b2c5d8f15cbea650c37b58a3cd9c8b2bc8d54b35d5a703306e9f`，new running SHA `e6cc5643cf5d9281c3d3a77071ca4a2fb5750539ef735d13f79838f758552c5c`。各臂 tarball/served SHA 独立 | [A source](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/tether-source.json)、[A supply](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/supply.json)、[A committed marker](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/hop-1.json)。**没有 run5 dirty patch/tree 的可复算收据，不能声称它等于当前 index tree** |
| N-1 来源 | `v0.6.0`，commit `a3431a12be3cf947cc797c28fea93382fa68472d`；linux/amd64、go1.26.8；binary SHA `1cfd3ec693b211bfd2844f7fea219f1e10b7b8dffc9185e558bcdbdf16230780`；build timestamp `2026-09-21T15:23:53Z` | [P tether-prev-source.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.P.XYokrS/tether-prev-source.json)。旧 broker/agent/ctl 的 endpoint SHA/version 与对应 fixture 分存；不把 simulator 叫真实旧二进制 |

### 15.2 测试与逐条变异

当前 baseline 与 restore 的精确 argv、rc、耗时、JSON raw log 分别在 [baseline-commands.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/baseline-commands.json) 与 [restore-commands.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/restore-commands.json)。表中每个 `Tnn` 链接包含该行的完整命令与每个 Mnn 的**单独** rc、失败断言、mutation diff/源 SHA（或明确标注的历史生成器身份）、raw log。baseline/restore 的同包批次共享一次命令，但每个实际测试都有独立 `Action=pass`；没有把编译成功/空匹配当 PASS。

真实 binary mutations 的 GOFLAGS overlay 环境、outer/inner mutation marker、race build 驱动及副本均归档在 `archive/tmp/p10-integration-mutations-7dwdkh_r/`；T35–T39 的副测试 oracle（byte/relay/resize）在各行明确列名，未冒充主测试。T43 按 L5：regular file、/dev/null 成功，socket pre-create legacy fallback。T44 按 L6 保持 NOT_RUN。

| ID / 计划名称 | 当前真实 `path: Name` | baseline / restore | 独立 mutation 收据 |
|---|---|---|---|
| T01 `TestPtyEIOOnlyInStartedDomain` | `internal/pty/terminal_test.go: TestPtyEIOOnlyInStartedDomain` | rc=0 / rc=0 | 3 条各 rc=1；[T01 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T01.md) |
| T02 `TestPtyTailOutlivesLeaderExit` | `internal/ptysession/output_test.go: TestPtyTailOutlivesLeaderExit` | rc=0 / rc=0 | 3 条各 rc=1；[T02 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T02.md) |
| T03 `TestPtyResizePreservesPollability` | `internal/pty/master_poll_test.go: TestPtyResizePreservesPollability` | rc=0 / rc=0 | 3 条各 rc=1；[T03 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T03.md) |
| T04 `TestPtyActorSurvivesSamePIDExecWithBlockedSlave` | `internal/ptysession/image_exec_test.go: TestPtyActorSurvivesSamePIDExecWithBlockedSlave` | rc=0 / rc=0 | 4 条各 rc=1；[T04 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T04.md) |
| T05 `TestPtyLogicalFINIsNotTerminalEOF` | `internal/ptysession/input_test.go: TestPtyLogicalFINIsNotTerminalEOF` | rc=0 / rc=0 | 4 条各 rc=1；[T05 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T05.md) |
| T06 `TestPtyDuplexDeadlineIsolation` | `internal/ptysession/fence_test.go: TestPtyDuplexDeadlineIsolation` | rc=0 / rc=0 | 4 条各 rc=1；[T06 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T06.md) |
| T07 `TestPtyFreezeDeadlineIncludesControlAdmission` | `internal/ptysession/fence_test.go: TestPtyFreezeDeadlineIncludesControlAdmission` | rc=0 / rc=0 | 3 条各 rc=1；[T07 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T07.md) |
| T08 `TestHangupSequenceNeverSplitsAcrossSnapshot` | `internal/ptysession/control_test.go: TestHangupSequenceNeverSplitsAcrossSnapshot` | rc=0 / rc=0 | 4 条各 rc=1；[T08 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T08.md) |
| T09 `TestPtyHangupFailureReceipts` | `internal/ptysession/control_test.go: TestPtyHangupFailureReceipts` | rc=0 / rc=0 | 4 条各 rc=1；[T09 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T09.md) |
| T10 `TestPtyMasterTransferAndWrongDescriptorRefusal` | `internal/pty/terminal_test.go: TestPtyMasterTransferAndWrongDescriptorRefusal` | rc=0 / rc=0 | 4 条各 rc=1；[T10 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T10.md) |
| T11 `TestPtyInputCreditAndOverlap` | `internal/ptysession/input_test.go: TestPtyInputCreditAndOverlap` | rc=0 / rc=0 | 5 条各 rc=1；[T11 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T11.md) |
| T12 `TestPtyInputAbortStillSettles` | `internal/execsession/pty_test.go: TestPtyInputAbortStillSettles` | rc=0 / rc=0 | 3 条各 rc=1；[T12 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T12.md) |
| T13 `TestPtyOutputAbortFreezesFinalEnd` | `internal/ptysession/output_test.go: TestPtyOutputAbortFreezesFinalEnd` | rc=0 / rc=0 | 3 条各 rc=1；[T13 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T13.md) |
| T14 `TestPtyControlReplayDoesNotRepeatSideEffects` | `internal/ptysession/control_test.go: TestPtyControlReplayDoesNotRepeatSideEffects` | rc=0 / rc=0 | 4 条各 rc=1；[T14 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T14.md) |
| T15 `TestProcessControlGuardAndSessionCleanup` | `internal/workload/control_test.go: TestProcessControlGuardAndSessionCleanup` | rc=0 / rc=0 | 3 条各 rc=1；[T15 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T15.md) |
| T16 `TestSessionCleanupSurvivesSamePIDExec` | `internal/workload/control_exec_test.go: TestSessionCleanupSurvivesSamePIDExec` | rc=0 / rc=0 | 2 条各 rc=1；[T16 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T16.md) |
| T17 `TestPTYIntentAndSettledKindSurviveImage` | `internal/procrequest/pty_exec_test.go: TestPTYIntentAndSettledKindSurviveImage` | rc=0 / rc=0 | 41 条各 rc=1；[T17 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T17.md) |
| T18 `TestPtyRepeatedLifecycleReleasesResources` | `internal/ptysession/lifecycle_test.go: TestPtyRepeatedLifecycleReleasesResources` | rc=0 / rc=0 | 3 条各 rc=1；[T18 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T18.md) |
| T19 `TestPTYImageClosedMasterAndTombstones` | `internal/execsession/pty_image_test.go: TestPTYImageClosedMasterAndTombstones` | rc=0 / rc=0 | 4 条各 rc=1；[T19 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T19.md) |
| T20 `TestPTYRevokeBeforeQueuedInputActivation` | `internal/execsession/pty_revoke_test.go: TestPTYRevokeBeforeQueuedInputActivation` | rc=0 / rc=0 | 2 条各 rc=1；[T20 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T20.md) |
| T21 `TestPTYRevokeClockFailureRetainsResponsibility` | `internal/execsession/pty_revoke_test.go: TestPTYRevokeClockFailureRetainsResponsibility` | rc=0 / rc=0 | 3 条各 rc=1；[T21 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T21.md) |
| T22 `TestPTYKAAbsoluteDeadlinesAcrossGenerations` | `internal/ptysession/control_test.go: TestPTYKAAbsoluteDeadlinesAcrossGenerations` | rc=0 / rc=0 | 7 条各 rc=1；[T22 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T22.md) |
| T23 `TestPTYPartialActivationNeverDetachesInitialImage` | `internal/agentboot/pty_activation_test.go: TestPTYPartialActivationNeverDetachesInitialImage` | rc=0 / rc=0 | 3 条各 rc=1；[T23 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T23.md) |
| T24 `TestPTYBundleRollbackCarriesCurrentState` | `internal/agentimage/pty_exec_test.go: TestPTYBundleRollbackCarriesCurrentState` | rc=0 / rc=0 | 3 条各 rc=1；[T24 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T24.md) |
| T25 `TestPTYSpawnBudgetAndFailureOwnership` | `internal/execsession/pty_test.go: TestPTYSpawnBudgetAndFailureOwnership` | rc=0 / rc=0 | 3 条各 rc=1；[T25 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T25.md) |
| T26 `TestSessionDestroyedRevokesPTYAcrossCut` | `internal/agent/session_destroyed_pty_test.go: TestSessionDestroyedRevokesPTYAcrossCut` | rc=0 / rc=0 | 5 条各 rc=1；[T26 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T26.md) |
| T27 `TestProcessResumePTYAuthorizationMatrix` | `internal/broker/process_resume_pty_test.go: TestProcessResumePTYAuthorizationMatrix` | rc=0 / rc=0 | 9 条各 rc=1；[T27 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T27.md) |
| T28 `TestPTYScopeAndActorStamp` | `internal/agent/run_reliable_test.go: TestPTYScopeAndActorStamp` | rc=0 / rc=0 | 3 条各 rc=1；[T28 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T28.md) |
| T29 `TestPreflightDefersOnEachReasonCode` | `internal/agent/upgrade_image_test.go: TestPreflightDefersOnEachReasonCode` | rc=0 / rc=0 | 3 条各 rc=1；[T29 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T29.md) |
| T30 `TestPTYCandidateDomainsAndSiblingPrev` | `internal/agent/upgrade_domain_test.go: TestPTYCandidateDomainsAndSiblingPrev` | rc=0 / rc=0 | 5 条各 rc=1；[T30 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T30.md) |
| T31 `TestPTYPendingAdmissionCannotFallBack` | `internal/agent/upgrade_domain_test.go: TestPTYPendingAdmissionCannotFallBack` | rc=0 / rc=0 | 4 条各 rc=1；[T31 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T31.md) |
| T32 `TestPTYFinalPrepareRechecksDomains` | `internal/agent/upgrade_domain_test.go: TestPTYFinalPrepareRechecksDomains` | rc=0 / rc=0 | 3 条各 rc=1；[T32 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T32.md) |
| T33 `TestPTYUpgradeExecFailureRestoresBothDirections` | `internal/agent/upgrade_image_test.go: TestPTYUpgradeExecFailureRestoresBothDirections` | rc=0 / rc=0 | 3 条各 rc=1；[T33 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T33.md) |
| T34 `TestPTYRuntimeFiveRoundImportFailures` | `internal/execsession/pty_image_test.go: TestPTYRuntimeFiveRoundImportFailures` | rc=0 / rc=0 | 6 条各 rc=1；[T34 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T34.md) |
| T35 `TestRunReliableDuplexAcrossUpgradeAndRollback` | `test/cli_e2e/run_reliable_test.go: TestRunReliableDuplexAcrossUpgradeAndRollback` | rc=0 / rc=0 | 3 条各 rc=1；[T35 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T35.md) |
| T36 `TestRunReliableLostRepliesDoNotDuplicateEffects` | `test/cli_e2e/run_reliable_test.go: TestRunReliableLostRepliesDoNotDuplicateEffects` | rc=0 / rc=0 | 3 条各 rc=1；[T36 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T36.md) |
| T37 `TestRunReliableTerminalAndAbortSettlement` | `test/cli_e2e/run_reliable_test.go: TestRunReliableTerminalAndAbortSettlement` | rc=0 / rc=0 | 3 条各 rc=1；[T37 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T37.md) |
| T38 `TestRunReliableNMinusOneQuadrants` | `test/cli_e2e/run_reliable_test.go: TestRunReliableNMinusOneQuadrants` | rc=0 / rc=0 | 4 条各 rc=1；[T38 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T38.md) |
| T39 `TestRunReliableLocalCancellationRestoresTerminal` | `test/cli_e2e/run_reliable_test.go: TestRunReliableLocalCancellationRestoresTerminal` | rc=0 / rc=0 | 4 条各 rc=1；[T39 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T39.md) |
| T40 `TestPTYClientSchedulesKAWhileOutputBlocked` | `internal/execclient/pty_test.go: TestPTYClientSchedulesKAWhileOutputBlocked` | rc=0 / rc=0 | 3 条各 rc=1；[T40 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T40.md) |
| T41 `TestPTYClientRecoveryDeadlineIsAbsolute` | `internal/execclient/pty_test.go: TestPTYClientRecoveryDeadlineIsAbsolute` | rc=0 / rc=0 | 3 条各 rc=1；[T41 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T41.md) |
| T42 `TestLegacyPTYInputDoesNotBlockOtherSessions` | `internal/agent/run_legacy_test.go: TestLegacyPTYInputDoesNotBlockOtherSessions` | rc=0 / rc=0 | 3 条各 rc=1；[T42 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T42.md) |
| T43 `TestTerminalIOCancellationAndDescriptorOwnership` | `internal/terminalio/io_test.go: TestTerminalIOCancellationAndDescriptorOwnership` | rc=0 / rc=0 | 9 条各 rc=1；[T43 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T43.md) |
| T44 `TestLegacyRunDarwinTerminalLifecycle` | `test/cli_e2e/run_darwin_test.go: TestLegacyRunDarwinTerminalLifecycle` | NOT_RUN：Linux，无 Darwin runner | NOT_RUN；未在 Darwin 实跑 |
| T45 `TestPTYWireShapeAndZeroValueCompatibility` | `internal/proto/process_resume_pty_test.go: TestPTYWireShapeAndZeroValueCompatibility` | rc=0 / rc=0 | 4 条各 rc=1；[T45 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T45.md) |
| T46 `TestPTYDomainVersionProbeAndPureExecReadability` | `internal/agent/upgrade_domain_test.go: TestPTYDomainVersionProbeAndPureExecReadability` | rc=0 / rc=0 | 3 条各 rc=1；[T46 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T46.md) |
| T47 `TestPTYGateControls` | 集合索引；实际 gate 名与路径见下表 | rc=0 / rc=0 | 67 条各 rc=1；[T47 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T47.md) |
| T48 `test_complete_duplex_trace_passes` | `test/simcluster/tests/hot-update-pty-probe-test.py: PTYProbeTests.test_complete_duplex_trace_passes` + `test_discontinuities_and_invalid_evidence_fail_closed` 的 40 个独立 arm | rc=0 / rc=0 | 40 条各 rc=1；[T48 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T48.md) |
| T49 `TestRunReliableErrorExitClasses` | `cmd/tether/run_reliable_test.go: TestRunReliableErrorExitClasses` | rc=0 / rc=0 | 4 条各 rc=1；[T49 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T49.md) |
| T50 `TestPTYRuntimeAdmissionLockDeadline` | `internal/execsession/activation_test.go: TestPTYRuntimeAdmissionLockDeadline` | rc=0 / rc=0 | 2 条各 rc=1；[T50 逐条命令/断言/SHA/raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T50.md) |

T01–T46、T49/T50 的计划名字均真实存在（T44 仅 Darwin build-tagged 函数），未发现删名或合并成空壳；T47 从来只是责任集合，不存在名为 `TestPTYGateControls` 的 Go 函数。T48 是 Python，不伪造 Go inventory 行。

| T47 实际门控身份（同文件控制或本次对生产源补缺） | 覆盖 / 收据 |
|---|---|
| `test/architecture/layering_test.go: TestPTYLayeringControls` / `TestLayeringRulesAreWellFormed` / `TestPackageLayering` | missing row、reverse dependency、subpackage prefix 独立红 |
| `test/architecture/dataplane_lifetime_test.go: TestAgentImageContextImportControls` / `TestAgentImageEntryPointsAcceptNoContext` | 三个 owner 的 membership/exported context/floor 独立红 |
| `test/architecture/upgrade_budget_pin_test.go: TestUpgradeBudgetPinControls` / `TestUpgradeBudgetsAgreeAcrossGoAndDrill` | 五个预算各 delete shell / delete Go mapping / drift 独立红 |
| `test/architecture/image_field_inventory_test.go: TestImageFieldInventoryAppendOnly`；`internal/proto/wire_inventory_test.go: TestWireFieldInventoryAppendOnly`；`test/determinism/test_inventory_test.go: TestTestFunctionInventoryOnlyGrows` | image/wire/test 删除行分别红；机械 ledger 只引用原包 receipt，本 worktree 未改 ledger |
| `test/determinism/leak_assert_shape_test.go: TestEveryLeakAssertionExercisesItsSubjectRepeatedly`；`test/determinism/enum_switch_default_test.go: TestNoDefaultOnRepoEnumSwitch` / `TestEnumFamiliesCoverEveryIotaEnum` | 11 anchor 删除 + dead anchor、三 enum 登记删除 + 三 bare-default scanner fixture 独立红 |
| `test/e2e/parallel/inventory_test.go: TestEveryRiskyPackageRunsUnderRaceInSomeMatrix` / `TestMatrixUnitInventoryOnlyGrows` | whole-package/race/package/matrix row 独立红 |
| `test/architecture/structural_budget_test.go: TestStructuralBudget` | 本次 on-disk Agent126；rc=1，`current=126 > golden=125`；恢复后 rc=0 |
| `test/architecture/gate_standards_test.go: TestEveryGateNamesItsControl`；`test/architecture/ctx_background_ledger_test.go: TestCtxBackgroundSitesAreAnnotatedOrLedgered`；`test/architecture/release_comparison_test.go: TestReleaseStringsAreComparedWithSameRelease` | 控制豁免、stale ctx ledger、raw release comparisons 分别红；精确实际路径以 [T47 JSON](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/mutations-by-row.json) 和源码为准 |

本次补跑：T27 的六个新 op 各绕 ACL、probe/pty-probe 各放宽 STALE、unknown default allow（9）；T39 第二 external signal handler（1，on-disk）；T47 Agent126（1，on-disk）；T48 每个 negative 单独删其对应 judge 分支（40，on-disk）。T48 正例保持 rc0，负例被错判 rc0 后原有断言变红 rc1，restore 原负例恢复 rc1/2、unittest rc0；每臂都是 0/1/0，不能用四十次同一失败签收。

**历史未击红也保留，不抹成“从未有 survivor”。** 本次新增 mutation survivor=0。以下是资料中可定位的旧 survivor/无效注入及处置；它们不是当前未关闭的产品 finding。

| 原始 survivor / 无效注入 | 观察与最终处置 / 原始证据 |
|---|---|
| P3 未完成 write 记账、early reap、blocked-write admission、close/join 单点 | 初稿测试未抓住，P3-INTEG 加强后 core 63 条分别 rc1。报告未给这几条初次 command/rc，保持“报告记载、初次 receipt 不全”，不编造数值；[report](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/reports/result-p3-integ.md) |
| P9 T33 open-before-thaw、T26 leader-PGID-only | 初次 rc0；改 earliest-open 断言及 HUP-immune stopped descendant 后分别 rc1；[ordering rerun](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/p9-mutations-yiq314y5/results.json)、[final-results](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/p9-integrated-mutations-k6nrcfbj/final-results.json) |
| P9 T31 source-order overlay | source scanner 看不到 overlay，初次 rc0 是无效注入；on-disk retry rc1，不计 overlay 作负控 |
| P10 T36 forward-then-close / repeat-control-effect，T39 dup/nonblock | 初稿 `p10-integration-mutations-uyvzjbw5/results.json` 各 rc0；加强 reply socket boundary、signal/resize handshake、SyscallConn flags oracle 后 `7dwdkh_r` 各 rc1；中间 `0d9ih1_5` 的 dup 也 rc0，保留原始 JSON |
| P10 late-input source-end 转换 | 初次现有 race 用例存活；新增 deterministic late-input adversary 后 rc1；[原始 rc0 result](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/p10-input-ended-mutation-vw7bupom/result.json)、[新增 adversary 的独立 FAIL log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/p10-input-ended-mutation-vw7bupom/adversarial.log) |
| P15b draining-tail threshold | 冗余检查下单点存活，整理为单一责任检查后 rc1；[result-p15b](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/reports/result-p15b.md)，初次独立 raw command 未提供 |
| AGENT-SLIM-2 `install-running-sha-admission` / `leaf-symlink-validation` / `open-parent-current-identity` / `open-symlink-guard` | **测试覆盖 finding：四个孤立 mutation 各 rc0，仍存活**；组合冗余防线 mutation 分别 rc1，**不能称这四个孤立 mutation 已击红**；[完整原始表](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/agentslim-mutation-receipt.txt) |
| rv-drill-V1 / DRILL-8：向旧 T35 fixture 注入真实 HUP | 内审独立运行 rc0，证明 oracle 漏洞；FX-D 加回 signal receipt 断言后注入 HUP rc1；删两处断言再注入 HUP 又 rc0，正是三向控制的预期结果；[review 原始 log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-review/rv-drill-verify-hup.log)、[FX-D report](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/reports/result-fxd.md) |

| 补充 receipt | 实际边界 / 状态 |
|---|---|
| `TestPTYSessionRecreationBoundary` | 本次 `go test -race -count=1 -json ./internal/agent -run '^TestPTYSessionRecreationBoundary$'` rc0；[raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/boundary.log)。确认 L1 不可判别窗口，**不是 C-10 通过** |
| matrix 实际地图 | `test/e2e/all_phases_test.go: TestReliableRunMatrix` → `test/cli_e2e` 的 `^TestRunReliable` 真 CLI 覆盖；证据为 T35–T39 的逐个实际 pass event，matrix 名是并行 suite join，不冒称重复执行。 |
| P17 文档负控 | matrix 地图删行、Darwin runtime 假 PASS、39.P 假 GREEN/隐藏 native3，独立 receipt audit 正反/恢复，见 §15.3；与 Go docs 门区分 |

### 15.3 race、五轮资源与门禁

| receipt | 实际命令与结果 | raw receipt / 边界 |
|---|---|---|
| ptysession 整包 race×3 | `go test -race -count=3 -timeout=10m -json ./internal/ptysession` rc0，142.43s | [command](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/race-ptysession-count3.json)、[raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/race-ptysession-count3.log) |
| 其它风险包整包 race | `go test -race -count=1 -timeout=10m -json ./internal/pty ./internal/workload ./internal/procrequest ./internal/execsession ./internal/execclient ./internal/agentimage ./internal/agentboot ./internal/terminalio` rc0，71.79s；agent 另 `go test -race -count=1 -timeout=15m -json ./internal/agent` rc0，158.10s | [risk packages](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/race-risk-packages.json)、[agent](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/race-agent.json)。CLI 本次是 T35–T39 定向批次，不称整包 |
| ≥5轮资源 anchors | T18/T34/T43、same-PID/control/auth/cancel 等实际 Test body 的共享 goroutine/FD assertions 与 exercise shape 门均通过；保留分类 FD/master/replay/pidfd/child/zombie 原有断言 | T18 3、T34 6、T43 9 独立 leak/ownership mutations；[T47](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/rows/T47.md) anchor 删除/死 anchor 控制；不能由父进程退出清理代签 |
| 机械门 / 预算 / 清单 | 本次 architecture/determinism/parallel 精确 gate selections baseline、restore 及有效 gate-name 补跑均 rc0；Agent126 已补红，golden 未改；18 wire/58 image append-only diff 另存 | [baseline gate supplement](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/baseline-gates.json)、T47。首轮三个不实际存在的辅助 filter 名没计覆盖，真实名均另有实际 pass event |
| docs 门 | `go test -race -count=1 -json ./test/architecture ./test/determinism -run '^(TestDocsTopLevelHoldsNoProcessArtifacts\|TestTestLayoutMapIsReconciledWithTheTree\|TestOriginLinesPointAtDocumentsThatExist\|TestDocsUseCurrentWireVersion)$'`：见最终 docs 验证 JSON | 最终文档编辑后另存 `final-doc-checks.json`；不存在的 filter 不签运行，按实际 test event核验 |
| make test / lint / vet-tags / gates | 用户给定的 lead 状态：当前树 green；review §3.3 记 `make lint`/`make vet-tags` rc0，较早 `/tmp/gates-s4.log` 与 `/tmp/gates-fxdm.log` 有 package绿日志 | **LEAD-REPORTED**，本 worktree 未运行；当前候选全套命令的独立 rc/完整 raw log 未提供，不能把历史两行 package PASS 代签当前全门。补齐请求见 §15.7 |
| make e2e-parallel | 用户给定：除既存 D7 leadership flake 外 green，D7 另包修复 | **LEAD-REPORTED / PARTIAL**；不是 S4 新失败，也不能写 ALL PASS；缺当前全矩阵原始命令/rc/log，本次禁止重跑 |
| Darwin cross-build | `GOOS=darwin GOARCH=arm64 go build ./...` rc0；Darwin test binary compile：`GOOS=darwin GOARCH=arm64 go test -c -o /home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/darwin-tests ./internal/pty ./internal/terminalio ./test/cli_e2e` rc0 | [build](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/darwin-cross-build.json)、[compile receipt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra/darwin-test-compile.json)。terminalio 显示 `[no test files]`，不能声称 Darwin io unit/runtime 执行 |
| Darwin 实跑 T44 | **NOT_RUN：未在 Darwin 实跑**，Linux 主机无 runner，按 L6 不阻塞 S4 | 不用 cross-build 或 compile 冒称实际 run/resize/cancel/tail/termios |
| P17 receipt audit | 本次独立审核 baseline rc0；matrix 删行、Darwin 假 PASS、39.P 假 GREEN、隐藏 native3 四个变异各 rc1；restore rc0 | [receipt-audit.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/receipt-audit.json)。这是收据审核控制，不冒称新的仓库 Go gate |

### 15.4 drill artifacts、身份与原生判词

**本包没有跑 drill/docker/deploy。** 下表是 lead 2026-10-03 run5 原始 receipts。每个 fixture 的 nonce、原 ctl waitpid、leader/foreground identity、images/marker、原始双向 bytes、A/Q/Base/End/control/KA、四日志 cursors 与分类 census 在对应 evidence 目录，摘要见 [drill-run5-summary.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/drill-run5-summary.json)。本次在 receipts 中复制原始 fixture 后离线调用真实 judge，七个正例 rc0，P rc1；复制的 artifact 不回写归档。

| 臂 | native rc / verdict | 原始 artifact / 本次离线 replay |
|---|---|---|
| 36.A | 0 / GREEN，30 pass | [hot-update-pty.A.YfNyPy](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/evidence.json)；replay rc0；ctl PID386/start_ticks126694335/wait_status5888→exit23；same agent PID1224，committed generation1 |
| 36.W | 0 / GREEN，32 pass | [hot-update-pty.W.knsWxT](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.W.knsWxT/evidence.json)；replay rc0；ctl364→23；真实 blackhole **126.12861955s**；原 120s watchdog、当前态 rollback、原 KA 期限与 measured backlog收据齐全 |
| 36.C1 / C2 | 0 / GREEN，C父35 pass | [C1](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.C.Yls1CG/evidence.json)、[C2](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.C.Yls1CG/c2/evidence.json)各 replay rc0；原 ctl387/798 均23；C1 carrier pair独立于 image generations；C2 tail=0.20559417s >200ms、canonical Ctrl-D/Ctrl-C/logical FIN |
| 36.L | 0 / GREEN，26 pass | [L](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.L.NAGf2C/evidence.json) replay rc0；原旧 ctl389→23；legacy gate command独立rc75、零spawn/effect/盘面不动；old-agent/old-broker fallback两个 fixture 各离线 compatibility rc0、原 ctl363/350→23 |
| 36 父 | GREEN，123 pass | [rollup.tsv](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/rollup.tsv) 的 ARMS 行为 A/W/C/L join，不跳失败臂；聚合行 rc0 |
| 39.X / 39.Y | 0 / GREEN；3 / PRODUCT-RED | [X raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/39-hot-update-legacy-n1.X.log)、[Y raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/39-hot-update-legacy-n1.Y.log)；各独立 `.rc`，保 H0 分类 |
| 39.P | **oracle_rc=1；native_rc=3；PRODUCT-RED**，23 pass | [P legacy_exporter.rc](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.P.XYokrS/legacy_exporter.rc)、[exact signature](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.P.XYokrS/legacy-exporter.out)、[native rc](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/39-hot-update-legacy-n1.P.rc)、[raw log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/39-hot-update-legacy-n1.P.log)。本次 offline legacy-exporter rc1；原 ctl446 measured timeout300.005886s 后 fixture SIGKILL，waitpid rc=-9，不能称产品自主exit |
| 39 父 | PRODUCT-RED，63 pass | X/Y/P join，product_red=2，H0/H1/S8完整 owner保留。ARMS汇总列rc0属于聚合操作，不是子臂native rc；waiver没有把native3变绿 |
| 34 A/G/U/N/P/W/F | 每臂 rc0/GREEN，父162 pass | run5 `34-hot-update-process.<arm>.log/.rc` 与各独立 evidence目录；[rollup](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/rollup.tsv) |
| 33 A/C/C2 | A/C/C2实际断言 PASS；全 drill **rc4/INCOMPLETE**，29 pass | [raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/33-node-upgrade-success.log) 的 A1–A4/C/C2与共享 sibling/marker断言；已有 gotcha #73 NON-tether artifact/shim self-attestation 缺口保持 NOT-COVERED，不能改GREEN |
| 35 A/B/C；94 | 35各 rc0/GREEN，父90 pass；94 rc0/GREEN，54 pass | run5各 `.log/.rc`；[rollup](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/rollup.tsv) |
| sweep | 19 units，16 GREEN / 2 PRODUCT-RED / 1 INCOMPLETE；wall352s；jobs10/stagger0/retry1；`--allow-product-red`；**NO DEVIATIONS** | [s4-drills5.log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/s4-drills5.log)、[rollup.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/rollup.txt)。sweep 的独立 process rc文件未保存：**NOT_RECORDED**；33为blocker，不能由allow-product-red推断sweep=0 |

39.P精确观测：`output=interrupted hup=0 management_terminal=0 ctl=-9 pid=1396 nonce=899c0f6fa5ad225fe8c9bf1b89d09767`；reason 原文逐字保留在 signature文件。原生 `assert_fail=setup_red=not_covered=nc_gap=0`、`product_red=1`；trace/log/供给有效才计产品负控。同 oracle 三向（正输入通过/旧输入失败/删对应检查旧输入错过）由本次T48每臂的positive/negative/mutation/restore证明；实际39.P签名仍以run5实物为准，不把 syntheticfixture叫deploy。

run1–4不丢弃：原镜像595ae2bb/ae082fc7/ab9568ac与每轮红灯/修复分类见 review §4、reports DRILL-SUBSHELL/R3/R4、SPAWN-INFLIGHT、PRODUCT-R4、N1-REFUSAL；[五轮原始rollup副本](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/drill-rollups/)。run5取代其出口结果，不能抹掉它们暴露的产品/driver缺陷。

### 15.5 延迟、冻结分布与成本

| receipt | 实测值 / 口径 | raw receipt |
|---|---|---|
| run5 PTY全冻结窗 | 四条实际 activation记录：**213/428/448/470ms**；nearest-rank p50=428、p95=p99=max=470ms；无>500ms。对应shim/import逐样本保留；只统计这四条，不能冒充全阶段总体分布 | [完整samples及raw line](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/drill-run5-summary.json) |
| 当前race真CLI | T35 baseline generation1/2 = **1080/444ms**，restore = **1031/468ms**；T38另 **642/627ms**。六样本p50=627、p95=p99=max=1080ms；>500为1080/1031/642/627。**超标明确保留**，L4仅校准，不删最大值。较早CLI-LATENCY曾记录3218ms，不被这次较小最大值抹去 | [baseline raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/baseline/test-cli_e2e.log)、[restore raw](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/restore/test-cli_e2e.log)、[CLI-LATENCY完整report](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/clilat-results/report.json) |
| 正常新请求/业务停顿 | run5 A strict mixed rate=**10.02678293/s**，configured10；read/write/short=44/43/43、window writes6、new run22、starts_in_window5；connect_failures/refusals/request_failures/over_5s/started_missing/skipped/dup_side_effects均0；max request2.086674s，新run started max1.144688s，均≤5s | [mixed judge](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/mixed/judge.json)、[samples](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/mixed/samples.jsonl)、[marks](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/evidence/hot-update-pty.A.YfNyPy/mixed/marks.json) |
| PTY资源 / 内存 | 各阶段原对象身份、master/replay/pidfd/handoff、wrongfd/zombie分类及settled退场有实际census。A/Q与output/checkpoints有实际offset；**未采独立RSS或replay+accepted+Pending合计瞬时峰值，NOT_MEASURED**；256KiB/32KiB限制测试不代替内存峰值测量 | [census/offset摘要](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/drill-run5-summary.json)与每个fixture原始resources/agent/*.json |
| 实际drill成本 | run5并发10，wall352s；36A/W/C/L=104/216/112/193s；39X/Y/P=68/119/**348s**；34七臂与35/33/94秒数各原样存 `.secs`。39P实际348s **超过plan的300s worst estimate**，包含原ctl300s超时，不能隐藏或改称300以内 | [rollup实数](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run5/rollup.tsv)、[各臂cost](../../test/simcluster/drill-costs.tsv)。五轮失败/restage成本与run5分列；没有从estimate倒推actual |
| 本次验证成本 | baseline CLI batch140.31s、restore135.86s；agent整包158.10s、ptysession race×3 142.43s、风险包批次71.79s。与CLI-LATENCY前691.4/690.5s、后135.6/155.9s（unrestricted/2CPU）不同执行条件分列 | [baseline/restore/extra命令JSON](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/extra-commands.json)、[latency原report](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s4-evidence/receipts/evidence-20261003/archive/tmp/clilat-results/report.json)。四路worker实测、不把重叠命令秒数相加当wall |

plan预算算术仍为估计：36Σworst1380s/串行ceiling2760s；39Σworst1020s/ceiling2040s；34Σworst2940s/ceiling5880s，三组无并发178min。不是本次耗时承诺。

### 15.6 N-1 与 rollback 域矩阵

| 路径 | 实际receipt / 限定 |
|---|---|
| new ctl / new agent | T35–T39 current race baseline/restore；run5 36A/W/C原可靠handle、原PTY/foreground/双向raw bytes；old process-resume broker第二probe fallback由T38真实wire拒绝/前创建oracle覆盖 |
| old ctl / new agent | run5 L真实v0.6.0 ctl→当前agent；legacy可用、gate拒绝rc75/零spawn/effect/盘面不动；T42双发布只注入一次。此格不宣称legacy连续热交接 |
| new ctl / old agent；old broker | T38 source `0ec45...` 真S3 reader/agent与旧订阅面分别列明；run5 L/old-agent、L/old-broker的真实v0.6.0供应、endpointversion/SHA、pre-createfallback与ctlwaitpid均在fixture；两格离线replayrc0 |
| old ctl / old agent | run5 39P真实oldexporter+oldctl基线、同fixture原始双向bytes；故障后PRODUCT-RED，不冒称续接 |
| create后降级/撤权 | T37/T38/T40/T41及P10额外guard收据：仅原handle恢复或74/77、零legacy重建；每错误不能重授180s期限，旧CallID不能推进ACK |
| S4→S3 exec-only | T46五次真S3 `Decode/Restore/Thaw` + same-PID资源oracle；纯exec外层S4字段零值省略；reader来源0ec45固定，不偷换成当前新reader |
| S4→S3 retained PTY | T30/T32保留live/terminal/tombstone/settled/ClockUnknown域，target/actualprev SHA-bound能力；Tasks-only、ABI-only、running=disk、忽略prevdomain等独立红，拒绝时盘面不动 |
| S3→S4 pending | T31/T38完整cap而createbusy、legacy75；MarkReady或commit持久化失败不能开放；N1-REFUSAL真实v0.6.0的try_again分类由run5 L原ctl实际证明，不只引用hermetic表 |
| S4→S4两代/rollback/sibling | T24/T33/T35当前A/Q/output/controlreceipt、generation+1；T30actualprev==candidate真实siblinginstall；W当前候选重新cut绝对KA不变。本次T24/T35baseline/restore与旧bundlemutation分列，不能把W已接纳输入说成黑洞期间新网络送达 |

### 15.7 出口与未覆盖边界

| 项 | 已有receipt与未签部分 | 状态 |
|---|---|---|
| C-4 PTY | T11/T12/T35–T41和run5 36A/C/L、39P；两步capability、byteACK/credit、mergedstdout/virtualstderr、terminal-ack、四象限 | 有实际receipt；tasklist只勾PTY子项 |
| C-8 PTY | T08/T15/T16/T26/T28/T42与36A/W/L；控制/Hangup、同SID多PGID/stopped清场、courier真exit/management | 有实际receipt；不扩大为任意子树containment |
| C-10 PTY | 四事件窗口/register-gone/tombstone/clockunknown及复建boundary有收据；finalPrepare后丢事件且同SID重建仍不可判 | **整体不勾**；L1归S8 authority incarnation；L2主动setsid后代不覆盖 |
| C-11 | 行级收据、风险包race、≥5轮与docs审计已具备；**§15.8 主进程最终同候选收据补齐**：全门原始 rc/log、D7 全矩阵（D7-LEADER 修后 ALL PASS）、drill 与门禁的同一 index tree 绑定 | **已签**（见 §15.8） |
| B-2 PTY子项 | T35/run5 36A/W/C/39P原terminal、rawbyteoracle、原ctlwaitpid | PTY子项可勾；SSH/TCP/SS及B-2总项仍S5 |
| H1六缺口 | EIO/FIN/controlfence/尾流队列/kerneldeadline/真出口分别有正receipt及独立规定mutation红 | 已有receipt；历史单点survivor按§15.2另列 |
| Darwin | cross-build/testcompile已有；T44未在Darwin实跑 | NOT_RUN，按L6不阻塞S4 |

Requests for other packages / lead receipts（本包没有越界修实现）：

- 补当前最终candidate的 `make lint`、`make vet-tags`、`make test`、`make gates` 精确command/独立rc/完整rawlog与source/tree绑定。用户给定green状态已经记LEAD-REPORTED，不把claim变receipt。
- D7既存leadershipflake由其owner闭合后，提供当前`make e2e-parallel`完整矩阵receipt；保留旧flake原始失败，不标ALLPASS。
- 若要签“同最终候选drill”，补run5 dirty427files的准确patch/tree或其它可复算source绑定；保存sweep独立processrc。现有running/artifact SHA和NO DEVIATIONS已签，缺项不伪造。
- T44需Darwinrunner才可实跑；内存瞬时峰值如需要须另采，当前NOT_MEASURED；33gotcha#73与S8 L1/legacyexporter缺口保持其原owner，不由本文勾掉。

最终source恢复与写入范围：本次所有临时on-disk变异都在finally恢复；对起始trackedfile逐个SHA256比较，差异仅本证据§15和tasklist的S4节。证明见 `scope-proof.json`、`scope-diff-stat.log`（最终审核落盘），未改mechanicalledgers，未暂存/提交。

### 15.8 主进程最终同候选收据（2026-10-03，补齐 §15.7 的 lead requests）

S4-EVIDENCE 之后又合入两个包：

- **SURVIVORS**：AGENT-SLIM-2 的 4 条孤立 survivor 各补了单点测试，单点变异全部 rc=1、组合变异仍 rc=1；
- **D7-LEADER**：既存的领导权前提 flake，修 3 处，T3 账本 13→10，18 条变异 rc=1。

另由主进程修了两处测试侧的异步计数时序：`TestExecReliableAgentConnKickMidStream` 改为轮询断开证据（去掉第二次 kick 的变异 rc=1），以及 spawnsafe 扰动守卫（S3、S4 两树同为约 1.5%，改后 300 次 0 失败）。

随后由脚本 `s4-final.sh` 在**同一 index tree** 上依次执行，原始日志在仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s4-evidence/final-candidate/`：

| 步骤 | 身份 | rc / 结果 |
|---|---|---|
| 候选 | HEAD `cab90d1d…`（S0–S4 均未 commit）；index tree `0ad5612000fc188f63e7791d1c5108a1fe2bd3c8`；unstaged 0、untracked 0；go1.26.8；Linux 6.8.0-139；nproc 88（`identity.txt`） | — |
| 构建 | `./local.sh --build build`，镜像 `sha256:50613d397eee…`（`build.log`） | build=0 |
| drill sweep | `run-drills.sh -j 10 --no-attribute --allow-product-red 36 39 34 35 33 94`（`drills.log` + `simdrills/`）：<br>• 36 A/W/C/L GREEN（123 pass）<br>• 39 X GREEN、Y PRODUCT-RED、P PRODUCT-RED（钉判命中）<br>• 34 七臂 GREEN，35 三臂 GREEN，94 GREEN<br>• 33 INCOMPLETE（既存 #73）<br>**NO DEVIATIONS** | drills=1：只来自 33 INCOMPLETE 的 BLOCKER 语义（未加 `--allow-incomplete`），其余判词全部与预期一致 |
| drill 后 tree | `index_tree_after_drills` 仍为 `0ad56120…` | — |
| `make test` | 同一 tree（`make-test.log`） | make_test=0 |
| `make e2e-parallel` | 同一 tree，ALL PASS，wall 5m53s（`e2e-parallel.log`） | e2e_parallel=0 |
| `make gates` | 同一 tree：lint 报 1 条 staticcheck QF1003（`install_admission_test.go` 的 if 链），其余全过（`make-gates.log`） | make_gates=2 |
| lint 修 | 改为 tagged switch。tree 变为 `9c22ec37b475793839d86165f54d9c42f068e1d7`，与 `0ad56120` 只差这**一个单元测试文件**，3+/2-（`tree-delta-after-drills.txt`），产品代码及 drill 所用的全部二进制字节相同 | — |
| 最终 tree 复跑 | `9c22ec37…` 上：<br>• `make gates`：0 issues（`make-gates-2.log`）<br>• `make test`（`make-test-final.log`）<br>• `make e2e-parallel`：ALL PASS，5m25s（`e2e-parallel-final.log`） | make_gates_2=0；make_test_final=0；e2e_parallel_final=0 |

**仍未签、且不阻塞的项**（如实保留）：

- T44 的 Darwin 实跑没有 runner，按 L6 处理；
- 内存瞬时峰值 NOT_MEASURED，plan 不把它列为出口要求，PTY 资源由各阶段 census 与预算测试覆盖；
- C-10 中 finalPrepare 之后的事件丢失与同 SID 复建归 S8（L1）；
- 33 的 gotcha #73 与 legacy exporter（H1）保持原 owner。

## 16 S5 flow-carrier-ss receipt index

本节及 §16.1 保留 P28 当时的未运行账本，不代表 2026-10-11 当前执行状态；最终回执与用户收尾裁决见 §17。124 行仍未逐项转写为 P29 validation，不能将其批量改为 COMPLETE。

2026-10-03: P28 documentation package on the P0 integrated contracts/stub baseline.
The [binding plan](hot-update-flow-carrier-plan.md) is frozen; all product receipts below are
**NOT_RUN pending P29**, not failed product assertions or completed exits. The P29-owned
`docs/reviews/hot-update-flow-carrier-ss-validation.md` is absent here. P0's contract receipts and
S3/S4 dated receipts above retain their original candidate identities and cannot sign S5.
P28's local documentation/build checks will be recorded separately in §16.1.

Each final receipt must retain source_head, dirty_patch_sha (actual dirty/untracked content), build_flags,
artifact_sha, running_sha, role, command, native_rc, verdict, wall_s, oracle_inputs_sha, evidence_path and status.
Record original byte/AEAD ledgers, target Accept/application identity, SQL/allocation/fences/AppliedRevision,
per-domain FD/replay capacity, source diff and the failing assertion for each independent mutation.
Tests use separate baseline/mutation/restored receipts; real artifact, same-test-binary kernel and independent
reference results cannot substitute for each other. Publish raw artifact references, not only GREEN prose.
Every COMPLETE row requires P29 validation and an existing raw artifact; link paths resolve from this file.
Native Darwin remains NOT_RUN without a runner, separate from cross-build and nonblocking under S4 L6.

<!-- s5-receipts:start -->
| Receipt | Status | Artifact | Required evidence |
|---|---|---|---|
| exit-1 | NOT_RUN | - | Frozen §0 exit 1; P29 final candidate |
| exit-2 | NOT_RUN | - | Frozen §0 exit 2; P29 final candidate |
| exit-3 | NOT_RUN | - | Frozen §0 exit 3; P29 final candidate |
| exit-4 | NOT_RUN | - | Frozen §0 exit 4; P29 final candidate |
| exit-5 | NOT_RUN | - | Frozen §0 exit 5; P29 final candidate |
| exit-6 | NOT_RUN | - | Frozen §0 exit 6; P29 final candidate |
| T01 | NOT_RUN | - | TestFlowFrameGoldenAndMalformed; P2 frame_test.go; baseline, each mutation and restored result |
| T02 | NOT_RUN | - | TestFlowACKUsesAppliedNotCarrierSent; P1 manager_test.go; baseline, each mutation and restored result |
| T03 | NOT_RUN | - | TestFlowDedupeBeforePartialWrite; P1 manager_test.go; baseline, each mutation and restored result |
| T04 | NOT_RUN | - | TestFlowOpenReconciliationKeepsOriginalDial; P1 open_test.go; baseline, each mutation and restored result |
| T05 | NOT_RUN | - | TestRetiredOpenCannotDialAfterReceiptGC; P1 retirement_test.go; baseline, each mutation and restored result |
| T06 | NOT_RUN | - | TestFlowFINCloseWritePreservesReverseTail; P1 manager_test.go; baseline, each mutation and restored result |
| T07 | NOT_RUN | - | TestFlowAbortKeepsFinalTailReceipt; P1 manager_test.go; baseline, each mutation and restored result |
| T08 | NOT_RUN | - | TestRawSocketPartialWriteAndDescriptorIdentity; P1 socket_linux_test.go; baseline, each mutation and restored result |
| T09 | NOT_RUN | - | TestConnectSOErrorHasOneConsumerAcrossCut; P1 connect_test.go; baseline, each mutation and restored result |
| T10 | NOT_RUN | - | TestFlowFreezeJoinsWithoutClosingBusiness; P1 fence_test.go; baseline, each mutation and restored result |
| T11 | NOT_RUN | - | TestYamuxSharedWriterCancellationBounds; P1 flow_carrier_test.go; baseline, each mutation and restored result |
| T12 | NOT_RUN | - | TestEndpointSurvivesSessionLossAndResumes; P1 flow_endpoint_test.go; baseline, each mutation and restored result |
| T13 | NOT_RUN | - | TestResumeReRunsTokenLookupAndFence; P1 flow_resume_test.go; baseline, each mutation and restored result |
| T14 | NOT_RUN | - | TestReaperDoesNotRehomeDuringPlannedCarrierRebuild; P13 flow_rehome_test.go; baseline, each mutation and restored result |
| T15 | NOT_RUN | - | TestFlowALPNNMinusOneQuadrants; P3 flow_alpn_test.go; baseline, each mutation and restored result |
| T16 | NOT_RUN | - | TestRegisterReaderRetainsYamuxPrefix; P3 register_buffer_test.go; baseline, each mutation and restored result |
| T17 | NOT_RUN | - | TestSSActorEveryByteCutAndTargetParser; P8 actor_test.go; baseline, each mutation and restored result |
| T18 | NOT_RUN | - | TestSSNonceContinuityAndNoReseal; P8 crypto_nonce_test.go; baseline, each mutation and restored result |
| T19 | NOT_RUN | - | TestSaltHistoryAliasesRotationAndResponseCollision; P8 salt_history_test.go; baseline, each mutation and restored result |
| T20 | NOT_RUN | - | TestSSNewAuthRechecksActiveMaterial; P8 actor_test.go; baseline, each mutation and restored result |
| T21 | NOT_RUN | - | TestSSSilentRevokeClosesBoundFlow; P8 revoke_flow_test.go; baseline, each mutation and restored result |
| T22 | NOT_RUN | - | TestSSFreezeAndCleanupJoinOutsideLocks; P8 fence_test.go; baseline, each mutation and restored result |
| T23 | NOT_RUN | - | TestSSPrivateCodecGoldenAndRequiredFields; P9 state_codec_test.go; baseline, each mutation and restored result |
| T24 | NOT_RUN | - | FuzzSSFrame; P9 state_fuzz_test.go; baseline, each mutation and restored result |
| T25 | NOT_RUN | - | FuzzFlowFrame; P2 frame_fuzz_test.go; baseline, each mutation and restored result |
| T26 | NOT_RUN | - | TestSSSecretsNeverEnterPublicCarriers; P10 image_test.go; baseline, each mutation and restored result |
| T27 | NOT_RUN | - | TestSSFiveRoundImageOwnershipFailures; P10 image_exec_test.go; baseline, each mutation and restored result |
| T28 | NOT_RUN | - | TestProxyAuthoritySnapshotNeverMixesKeysAndEpoch; P11 proxy_authority_test.go; baseline, each mutation and restored result |
| T29 | NOT_RUN | - | TestResumeAuthorityLookupStopsAndRejectsKnownLag; P12 tunnel_authority_test.go; baseline, each mutation and restored result |
| T30 | NOT_RUN | - | TestSharedReplayBudgetCountsOwnedDomains; P4 shared_budget_test.go; baseline, each mutation and restored result |
| T31 | NOT_RUN | - | TestFlowImagePendingAndRetirementSurvive; P6 image_test.go（flow）; baseline, each mutation and restored result |
| T32 | NOT_RUN | - | TestClientImagePreparePermitAndFDClosure; P7 flow_image_test.go; baseline, each mutation and restored result |
| T33 | NOT_RUN | - | TestBundleFinalReexportIncludesFlowSSRevoke; P17 flow_ss_image_test.go; baseline, each mutation and restored result |
| T34 | NOT_RUN | - | TestFlowSSPartialActivationUsesCurrentCut; P18 flow_ss_activation_test.go; baseline, each mutation and restored result |
| T35 | NOT_RUN | - | TestFlowSSCandidateAndActualPrevDomains; P16 upgrade_domain_flow_test.go; baseline, each mutation and restored result |
| T36 | NOT_RUN | - | TestPendingOldPrevRejectsNewFlowSSAdmission; P16 upgrade_domain_flow_test.go; baseline, each mutation and restored result |
| T37 | NOT_RUN | - | TestPreflightDefersOnEachReasonCode（扩展原名）; P16 upgrade_image_test.go; baseline, each mutation and restored result |
| T38 | NOT_RUN | - | TestAgentRestoresRawThenResumeThenRegister; P14 flow_restore_test.go; baseline, each mutation and restored result |
| T39 | NOT_RUN | - | TestAgentSamePIDExecKeepsFourTCPFlows; P20 flow_exec_test.go; baseline, each mutation and restored result |
| T40 | NOT_RUN | - | TestFlowCarrierRebuildLifecycleMatrix; P20 carrier_test.go; baseline, each mutation and restored result |
| T41 | NOT_RUN | - | TestHalfCloseTailAcrossRealExec; P20 flow_lifecycle_test.go; baseline, each mutation and restored result |
| T42 | NOT_RUN | - | TestSSRealExecKeepsIndependentCipherTrace; P21 ss_exec_test.go; baseline, each mutation and restored result |
| T43 | NOT_RUN | - | TestSSRevokedDuringCutNeverWritesAfterGrant; P21 ss_revoke_test.go; baseline, each mutation and restored result |
| T44 | NOT_RUN | - | TestActiveRollbackExportsCurrentFlowSSState; P22 rollback_test.go; baseline, each mutation and restored result |
| T45 | NOT_RUN | - | TestReadyPendingWatchdogTakesSecondCurrentCut; P22 partial_activation_test.go; baseline, each mutation and restored result |
| T46 | NOT_RUN | - | TestLegacyExposeDarwinLifecycle / TestLegacySSDarwinLifecycle; P23 legacy_darwin_test.go（各包）; baseline, each mutation and restored result |
| T47 | NOT_RUN | - | test_complete_duplex_trace_passes及负例组; P24 hot-update-flow-probe-test.py; baseline, each mutation and restored result |
| T48 | NOT_RUN | - | hot-update-flow-drill-test.sh; P25 同名文件; baseline, each mutation and restored result |
| T49 | NOT_RUN | - | 各gate的同文件Controls; P26 §10所列文件; baseline, each mutation and restored result |
| T50 | NOT_RUN | - | TestHotUpdateFlowMatrix; P27 flow_matrix_test.go; baseline, each mutation and restored result |
| T51 | NOT_RUN | - | TestRawSocketFiveRoundLifecycle / TestFlowFiveRoundLifecycle / TestTunnelFlowFiveRoundLifecycle / TestSealedStateFiveRoundLifecycle; P1 socket_linux_test.go/manager_test.go/flow_endpoint_test.go；P5 state_linux_test.go; baseline, each mutation and restored result |
| 75 | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.A | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.C | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.R | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.B | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.W | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 75.L | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| early-close | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| wrong-offset | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 39.F | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 36.A | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 36.W | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 36.C | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 36.L | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 39.P | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 34-all | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 35-all | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 33-boundary | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 70 | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| 72 | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| fuzz-exact-two | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| whole-race | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| five-round-leaks | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| public-inventory | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| private-codec-inventory | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| gate-controls | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| domain-fd-budget | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| alpn-four-quadrants | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| freeze-full-window | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| latency-max | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| native-verdicts | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| source-artifact-running | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| make-test | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| make-build | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| make-e2e-parallel | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| make-lint | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| make-gates | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| vet-tags | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| darwin-cross-build | NOT_RUN | - | No P29 final receipt; retain native command/rc and raw evidence |
| darwin-native | NOT_RUN | - | No P29 final receipt; no Darwin runner, nonblocking under S4 L6 |
| named:TestBootstrapFlowRestoreStartsNoWorkers | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestCurrentOwnerTicketCoversLongIdleAndFirstDetach | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestDomainCompleteActivationWindow | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowFrameInventoryControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSBudgetControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSContractControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSDataplaneLifetimeControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSEnumControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSExecFailureThawsBothDirections | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSGateRegistryControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSImageInventoryControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSLayeringControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSLeakRiskControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSMatrixControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestFlowSSRedispatchUsesCurrentState | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestPrepareActivateLostReplyHasOneOwner | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestProxyApplyEntrypointsAreSerialized | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestProxyFiredCleanupSurvivesCut | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestProxyPairDigestIsNotSuppressed | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestRawImageSalvageKeepsAllTakenFiles | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestRawProbeDetectsDroppedAndDuplicatedBytes | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestRawSocketDarwinImageUnsupported | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestReplayACKKeepsCapacityReservation | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestSSCapacityFailureStillRevokesRemovedKeys | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestSSReferenceRejectsNonceReuse | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestSSStateInventoryControls | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
| named:TestSealedStateRejectsUnsealedDescriptor | NOT_RUN | - | Frozen §8/§10/§12 supplemental identity; baseline, each mutation and restored receipt |
<!-- s5-receipts:end -->

B-2 requires original SSH/TCP/SS and S4 PTY on the same final candidate. D-1/D-3/D-5 may be signed only
with their full tests/drills/mutations; D-2/D-4/D-6 agent subitems do not sign broker exec/exports (S6).
Re-run S4 C-4/C-8/C-10/C-11/B-2 and failures §4's six responsibilities on this S5 candidate, including
36.A/W/C/L and39.P. C-10's lost-event plus same-SID recreation remains S8; existing S4 C-11 historical
completion does not waive these S5 regressions. S9 infrastructure and S10 final rollout remain uncompleted.

### 16.1 P28 local documentation checks (not product exits)

Scope: exactly the 17 P28-owned Markdown files, on the P0 contract/stub baseline.
Source HEAD is recorded in [/tmp/tether-s5-p28/base-head.txt](/tmp/tether-s5-p28/base-head.txt).
The [local receipt](/tmp/tether-s5-p28/package-receipt.json) binds final owned file digests and dirty patch;
no staged/installed/running product artifact is claimed. Initial build/vet command wall was not measured;
Go test logs retain package wall, mutation drivers record per-operation wall. Product receipts above remain NOT_RUN.

| Check / exact command | Native rc / outcome | Raw receipt |
|---|---|---|
| `go build ./... && GOOS=darwin GOARCH=arm64 go build ./...` | 0, Linux build and Darwin arm64 cross-build; no native Darwin run | [Linux](/tmp/tether-s5-p28/build-linux.log), [Darwin](/tmp/tether-s5-p28/build-darwin.log) |
| `go vet ./test/architecture ./test/determinism` | 0 | [vet](/tmp/tether-s5-p28/vet.log) |
| `go test -race -count=1 -timeout 30m ./test/architecture ./test/determinism` | 1 overall; determinism PASS; architecture has exactly two pending integration failures below | [whole race](/tmp/tether-s5-p28/whole-race.log) |
| `go test -race -count=1 -timeout 30m ./test/architecture -run '^(TestDocsTopLevelHoldsNoProcessArtifacts\|TestProcessArtifactPatternRecognisesTheRealShapes\|TestGatesTargetCoversEveryGateCLAUDEMdNames\|TestGolangciNameRegistersNameLiveFunctions)$'` | 0 | [docs gates](/tmp/tether-s5-p28/docs-gates.log) |
| `go test -race -count=1 -timeout 30m ./test/determinism -run '^(TestPromisedGuardTestsExist\|TestPromisedGuardScannerIsNotVacuous)$'` | 0 | [promise gates](/tmp/tether-s5-p28/promised-gates.log) |
| `go test -race -count=1 -timeout 30m ./test/determinism -run` with selectors `^TestDocsUseCurrentWireVersion$` and `^TestDocsWireVersion` (two commands) | 0 each, after updating version output example | [current wire](/tmp/tether-s5-p28/docs-current-wire.log), [scanner control](/tmp/tether-s5-p28/docs-wire-version.log) |
| `python3 /tmp/tether-s5-p28/docs_audit.py` (extracted canonical README fence) | 0; 17 docs,3 live guards,124 required product receipt rows,123 required rows pending plus native Darwin pending | [audit](/tmp/tether-s5-p28/audit-final.log) |
| `python3 /tmp/tether-s5-p28/mutations.py` | 0 driver;14 independent on-disk mutations each native1, restored audit0 | [commands/assertions/diff hashes/wall](/tmp/tether-s5-p28/mutation-report.json) |
| `python3 /tmp/tether-s5-p28/raw_reference_controls.py` | 0 driver;2 independent standalone Markdown fixture controls native1/restored0; no fake P29 result entered this tree | [raw-reference controls](/tmp/tether-s5-p28/raw-reference-controls.json) |
| `python3 /tmp/tether-s5-p28/gate_path_mutation.py` | 0 driver; wrong CLAUDE gate path native1/restored0 using the actual Go gate | [gate-path mutation](/tmp/tether-s5-p28/gate-path-mutation.json) |
| `rg --files -g '*.go'` piped to `xargs gofmt -l`; `git diff --check` | 0, empty output | [gofmt](/tmp/tether-s5-p28/gofmt.log), [diff check](/tmp/tether-s5-p28/diff-check.log) |

The Markdown audit supplements Go gates: promised-guard scans Go comments, not Markdown. No Go guard is
weakened or replaced. The audit checks raw-reference existence only; P29/lead still must review actual contents.
The mandatory false-all-check, wrong-path and dead-test-promise mutations each failed at the named guard,
then passed after exact byte restoration. Additional controls cover missing/duplicate registry/receipt rows,
frozen copy/digest, invalid/false COMPLETE status, unexecuted artifact, tasklist loss and fake Darwin execution;
raw reference absent/malformed controls use an isolated synthetic documentation fixture. There are17 red controls total.

**Observed pending dependencies (not hidden or waived):**

- `test/architecture/gate_standards_test.go: TestEveryGateNamesItsControl`: P26's new gate source files are absent.
  The first reported path is `test/architecture/flow_frame_inventory_test.go`; also land
  `ss_state_inventory_test.go` and `flow_ss_contract_test.go` with their real same-file gate-control anchors.
- `test/architecture/test_layout_map_test.go: TestTestLayoutMapIsReconciledWithTheTree`: P19 has not landed
  `test/hotupdate` Go files. P28 registers the final required directory row; no placeholder .go file masks this failure.
- `go test ./test/hotupdate/` and product tests/drills/fuzz/matrix/native Darwin are NOT_RUN pending their
  owners/P29/runner. No Docker, installed tether, production state, deploy/global-lock targets or git mutations ran.

**Landing requests:** P19/P26 resolve the two real whole-package failures; P27 supplies the final whole-race
matrix/ledgers. P29 fills raw receipt references on one final candidate, preserving native verdicts and the
Darwin distinction; then P28/lead can sign eligible tasklist subitems. No new Go Test/Fuzz, inventory or leak-anchor
rows originate in P28. S6/S8/S9/S10 and top-level D-2/D-4/D-6 remain open.

## 17. S5 flow-carrier-ss 最终收尾收据（2026-10-11）

用户于 2026-10-11 裁决 S5 关闭，剩余一项间歇 e2e 失败带入 S6，登记为 **ENDPOINT-RESUME**。本节从回执签收，不把原生红灯、校准超标、未运行项或既有边界改成 GREEN；[内审处置](hot-update-flow-carrier-review.md) / [tasklist](hot-update-tasklist.md) 与本节同口径。所有路径均为已保存的回执；本 docs 包没有重跑产品、镜像或 deploy。

### 17.1 候选树、镜像与身份

| 候选 | 身份与用途 | 回执 |
|---|---|---|
| gates-3 | HEAD cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025；tree ee2f1c2814e30d0c4b0996389940a17e25a4f550；2026-10-04 四闸全绿 | [gates-3/identity.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-3/identity.txt) |
| gates-4 | 同 HEAD；tree 4c55cb832e9ac8405f6c3d5926dc449992ea77f0；DRILL-RUN merge 后 2026-10-11 候选 | [gates-4/identity.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-4/identity.txt) |
| gates-5 | 同 HEAD；tree f4309233ddb85a76aa35ee38fb55f40208ca045a；GATES-FINAL-3 合入后最新硬闸 | [gates-5/identity.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/identity.txt) |
| 最终 deploy | image sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c；HEAD d2ba694b4907ab9fa09d25579b5b0b4c742caf23；base tree ee2f1c2814e30d0c4b0996389940a17e25a4f550 | [HANDOFF.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md) |
| deploy dirty source | product build digest 09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336；final reviewable source digest 9ec4c8d2752cc7299c82d13b9d104ddf14c83102947f637f169e5c7c25ece657；427 production/build inputs、binary/vendor 与每个 drill/test input 对账同一 rebuilt tree，acceptance 后仅 measured ledger docs 变化 | [FINAL-DEPLOY-SUMMARY.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-DEPLOY-SUMMARY.md) |

以上 tree、HEAD、patch digest 和 image digest 各有不同含义，不将 gates-5 tree 冒充 deploy build tree。每臂 running SHA、原始字节/nonce/Accept/身份 ledger、command/load/native_rc 由 [FINAL-ACCEPTANCE-ARMS.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json) 和各 run receipt 保存。早期 b7f5ce43 等尝试及 previous-final-image049/ 中的 049b3c3b 只作历史记录；run262 deadline 产品红导致修复、run270 新镜像与 271–278 整轮重验，未用旧镜像成功拼接最终验收。

### 17.2 硬闸与候选树

数字是 rc.txt 的原生 shell rc，不将 runner rc2 写成单条测试 rc1。

| receipt / tree | make lint | make test | make e2e-parallel | make gates | 处置 |
|---|---|---|---|---|---|
| [gates-3/rc.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-3/rc.txt) / ee2f1c28 | 0 / 17s | 0 / 519s | 0 / 850s | 0 / 504s | 合入 drill 前四闸全绿 |
| [gates-4/rc.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-4/rc.txt) / 4c55cb83 | 2 / 22s | 0 / 621s | 2 / 1023s | 2 / 562s | 转 GATES-FINAL-3；[报告](/home/weiland/s5wt/prompts/result-gates-final3.md) 记录 fixture Hook race、closed-transport retry 与 ledger/fixture 修复 |
| [gates-5/rc.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/rc.txt) / f4309233 | 0 / 17s | 0 / 587s | **2 / 892s** | 0 / 550s | 一个单元仍红，ENDPOINT-RESUME 转 S6 |

[gates-5/e2e-parallel.log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/e2e-parallel.log) 保存 `HotUpdateFlow:internal/tunnel` 的唯一失败：`TestEndpointSurvivesSessionLossAndResumes/wire`、`flow_endpoint_test.go:164`、`tunnel: invalid flow owner state`。GATES-FINAL-3 的 endpoint + lifecycle 在 `GOMAXPROCS=2 -race -count=50` 中 rc0（198.813s），只能证明该重复运行通过；不证明并行矩阵失败已修或是环境原因。S6 / ENDPOINT-RESUME 必须保留 gates-5 raw log 与 tree，在相同矩阵压力下复现并确认 owner 状态根因，再用回归及矩阵复跑关闭。用户裁决允许 S5 携此项收尾，没有撤销红灯。

### 17.3 drill 原生判词与完整回归

权威索引：[RECEIPTS.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/RECEIPTS.md)、[HANDOFF.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md)、[FINAL-DEPLOY-SUMMARY.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-DEPLOY-SUMMARY.md)；下列均为最终 image 9d8a232c 的串行实跑，不重命名 parent policy verdict。

| 最终 run | 原生臂结果 | 回执 |
|---|---|---|
| 271 / parent75 | A/C/R/B/W/L 全 GREEN0；原 SSH/TCP/SS、EINPROGRESS/half-close、撤权 survivor、10/s 新请求、原 socket/target Accept=1 ledger | [271](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json) |
| 273 / parent39 | X GREEN0；Y/P/F PRODUCT-RED3；精确 X0/Y3/P3/F3 符合 manifest 与冻结 P pin | [273](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/39-parent/receipt.json) |
| 274 / regression | 36 A/W/C/L、34 A/G/U/N/P/W/F、35 A/B/C、94 全 GREEN0；33 INCOMPLETE4 | [274](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/regression-sweep/receipt.json) |
| 275 / full smoke | 70 GREEN0（53s）、72 GREEN0（90s）；保留完整历史断言，不是 shortened subset | [275](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-275/smoke/receipt.json) |
| 276/277 / negatives | 实际关闭原 ss-K socket；真实 wire ACK A+1；共同 oracle 各 ASSERT-FAIL1、outer control0 | [early-close native](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-276/early-close/drill/native.rc)、[wrong-offset native](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-277/wrong-offset/drill/native.rc)、[restore proof](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-277/RESTORED-PROOF.json) |
| 278 / restored original | 恢复源码/镜像后原 75.A GREEN0 | [278/75-A-restored native](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-278/75-A-restored/native.rc) |

33 的 INCOMPLETE4 是预登记 #73 NC_GAP / frozen §9.5 边界，不是成功。39.F unpinned run272 SETUP-RED2 为刻意控制，随后原 reset 与 reviewed201 pin 重核；不能将其覆盖为 GREEN。历史 35.A stale ONLINE 判据、35.B 错套 fresh registration 的 SETUP-RED2 都留据；最终 guard 仅 NATS restart/outage 要求 fault cursor 后的原生注册事件，broker-only restart 保留 ONLINE authority，恢复预算不变。原失败及九条 on-disk mutants 见 run263/MUTATIONS-SCOPED.json。

### 17.4 真实 drill 暴露的产品缺陷与回归

DRILL-RUN 报告及 [ROOT-FIXES-CONSOLIDATED.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/ROOT-FIXES-CONSOLIDATED.md) 逐项登记以下 roots（原 positive rc0 / 注入缺陷 assertion rc1 保存在对应 run；无效 selectors、编译/fixture 失败和 survived mutations 单列，不充当检出）。它们解释为何 hermetic/内审通过不能替代原协议与真实部署。观测实现的缺陷也列在表内，但不冒充业务断链；fixture/judge 缺陷另列于表后。

| root / 漏洞与修复 | 回归 | 回执 run |
|---|---|---|
| pending socket observation：pump 保留原 binding 与 committed offsets 的 native pending observer | TestFlowObservationKeepsOriginalBindingAndCommittedOffsets | 12 |
| CONTROL / DATA credit：DATA 背压不应触发 CONTROL watchdog fence；有界共享 writer cancel 与锁外 transport close | TestYamuxSharedWriterCancellationBounds | 23 |
| PREPARE stale attachment / transient REGISTER：recovery 共用 current attachment，避免旧 exchange，暂拒作 bounded retry | TestConcurrentCarrierRecoverySharesOneAttachment | 32/35 |
| restored supervisor readiness：初始 up、lost down、恢复 up 必须真实发布 | TestCarrierRecoveryWaitsForSupervisorPublication | 61 |
| zero-ID legacy pre-SYN custody：connect 前绑定 tuple 与 reservation，普通 expose 不受 proxy flow reservation 污染，silent revoke 可清场 | TestSSSilentRevokeClosesBoundFlow/legacy；TestProxyFlowReservationKeepsOrdinaryExposeIndependent | 87/88 |
| terminal receipt resurrection：验证 exact receipt/ownership，不能以历史终态重建 live flow | TestTerminalFlowReceiptsDoNotBreakCarrierResume/round/detached_revoke | 133/135 |
| initial typed v1 home lag：原 attempt 内按 typed transient/refusal 分类，不能乱作 legacy fallback 或延长 deadline | TestConcurrentCarrierRecoverySharesOneAttachment | 146/148 |
| kernel-ready 原 socket 与 deadline：先处理已就绪 connect/SO_ERROR custody，保留 Stop 与原 absolute deadline，不重 Dial | TestConnectSOErrorHasOneConsumerAcrossCut | 156/157/160 |
| committed directive height：proxy dispatch 使用 committed home apply height，并保原 authority deadline | TestProxyReaperReadinessControls/round-[0-9]+/initial-home-apply | 165/166 |
| observer EAGAIN/batch/poison：写不进时保留同一 batch/sequence，dead peer/overflow 必须 poison，不能丢观测再称零丢包 | TestCustodyOverflowAndWriteFailurePoisonTheStream | 173/174 |
| native v0.6 known-home-lag：bounded legacy token lookup 保留 typed lag，拒撤权、不能续预算 | TestResumeAuthorityLookupStopsAndRejectsKnownLag/round-.*/known-lag | 212/214 |
| S3 reader/calibration：FrozenAtBootNS 保原 accounting；admission telemetry 使用兼容 opaque sealed descriptor custody，删除被拒 StateCutAtBootNS JSON 方案，pristine S3 Borrow/Repack 后仍可读 | TestPTYDomainVersionProbeAndPureExecReadability/pure_exec_s3_reader；TestBundleFreezeWindowStartsAtAdmission | 109/110/112 |
| C1 native capacity/binding：typed/countable refusal、reservation 与 original inode/tuple 不能错报或双反转 | TestTunnelFlowFiveRoundLifecycle；CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners | 247/252 |
| fresh registration heartbeat：首次 tick 即发布 reaped corpse 状态 | TestHeartbeatPublishesReapedCorpseStateOnItsFirstTick | 91 |
| committed subscriber revoke：立即发送 keyset_changed，区分合法无 subscription 与空 subscription | TestClusterProxyRevokeEmitsKeysetChanged | 102 |
| accepted RESUME 后 transient DATA bind：有界重试，避免 supervisor 看似恢复但 data bind 永久缺失 | TestCarrierRecoveryWaitsForSupervisorPublication | 91 |
| initial closed transport：closed session 有界 retry，malformed supply 仍拒 | TestConcurrentCarrierRecoverySharesOneAttachment | 102 |
| post-drill legacy deadline provenance：最后 SQL deadline expiry 不得丢既有真实 lag，也不得凭空造 lag；private snapshotAttempt 保留 expiry provenance，public snapshot 仍映射 try_again；store fault/Stop/revoke/earlier deadline 各保原语义 | TestResumeAuthorityLookupStopsAndRejectsKnownLag / exerciseLegacyAuthorityExpiry | 262 red；267 red→green；268 八条 on-disk mutants rc1/restored0 |

每行 exact command、positive receipt、mutation 由 [ROOT-FIXES-CONSOLIDATED.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/ROOT-FIXES-CONSOLIDATED.md) 直接链接。最后一行在 whole broker race 才暴露，促使 270 重建及 271–278 全新验收；不是归因于环境的 flake。完整产品 files/symbols 见 [HANDOFF.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md) 与 FINAL-SOURCE.patch。

fixture/judge 修复包括独立 draining keeper、有限 SS records 与真实 partial writes、原 OFD provenance、39.P 真 128KiB output pressure（原 pin 未改）、36/34 fresh generation provenance、C1 saturation→自然 FIN→recovery→normal、35 scoped fresh registration；均在 RECEIPTS.md/HANDOFF.md 中保留原失败、root controls 与 mutants。peer-close FD fixture 通过真实 retirement barrier 修复，TestBundleFinalReexportIncludesFlowSSRevoke 的删 barrier 五轮均红、refused snapshot leak 也红；见 run117 与 FD-INVESTIGATION.md。另一历史 run112 FD 19→20 transient 未能归因，quiet passes 不构成环境根因证明。

### 17.5 资源、校准、变异与未覆盖边界

[HANDOFF.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md) 的 run279：agentimage `-race -count=5`，agent/ssproxy/agentboot/execsession/broker/flow/tunnel/rawsocket/drillobserve whole-package `-race -count=1`，structural、Darwin arm64 cross-build 共 12 checks native0；S3 prior-reader 同 PID exec 五轮随 agent race 运行。完整 commands/rc/wall 在原表保存，不由本 docs 包复跑。QUIET-WINDOW-CLOSURE.json / FINAL-CENSUS.json / FINAL-PID1-CENSUS.json 证明 scoped container/network/process 清空；ACCEPTANCE-DONE 的 UTC 时刻 2026-10-12T00:23:40.285395+00:00 对应当地 2026-10-11。

| 75 arm | max stall s | requests / terminal failures | full-freeze max s |
|---|---:|---|---:|
| A | 3.013146 | 435 / 0 | 1.063 |
| C | 3.023064 | 403 / 0 | 0.974 |
| R | 3.038600 | 513 / 0 | 1.048 |
| B | 3.008034 | 447 / 0 | 0.991 |
| W | 3.006737 | 1584 / 0 | 1.041 |
| L | 3.033160 | 307 / 0 | 0 |

实测值来自 [FINAL-DEPLOY-SUMMARY.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-DEPLOY-SUMMARY.md) / [FINAL-ACCEPTANCE-ARMS.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json)。500ms 完整冻结窗为 L16/S4 L4 校准；如实保留超标，没有提高期限。正常 max5s、10/s、新请求零 terminal failures、原 identity/Accept=1 仍为硬判。C1 32 originals 占 256MiB，typed/countable capacity refusal 保已有 owner；32 natural FIN、64 direction ledgers 后释放全部 budget，恢复原 socket/新 exec，再以 ≤224MiB normal profile 作真实 cut。

| 变异族 | 回执所示结果 | 证据 |
|---|---|---|
| FIX-IMAGE / FIX-FLOW / FIX-BROKER / FIX-SS | 最终 named defect mutants assertion rc1；原 overlap/T26 弱 oracle rc0 单列 | [四包处置](hot-update-flow-carrier-review.md#2-finding-与处置) |
| INTEG-VERIFY | 148 intended defect receipts 编译后 assertion-red；四个弱 oracle 与两个原 HOTUPDATE green controls 单列 | [result-integ-verify](/home/weiland/s5wt/prompts/result-integ-verify.md) |
| P13-REGRESS | 79 executions assertion-red；premature-adapter-start 因 idle compatibility 改为正控 | [result-p13-regress](/home/weiland/s5wt/prompts/result-p13-regress.md) |
| DRILL-RUN | 八 deadline、32 C1、七 capacity/binding、九 35 recovery、11 S3/accounting/custody guards 均 assertion1/restored0；其他 roots 个别结果查原表，无效控制不计 | [HANDOFF.md](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md) |
| deploy negatives | 两条真实缺陷各 arm1/control0；恢复原 A0 | run276/277/278，[FINAL-ACCEPTANCE-ARMS.json](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json) |
| GATES-FINAL-3 | 25 mutation receipts；同步 teardown 回退报 Hook race，restored0 | [result-gates-final3](/home/weiland/s5wt/prompts/result-gates-final3.md) |

本节不将不同包重复复核的 receipts 相加当作独立缺陷总数。原生 Darwin runtime **NOT_RUN**（Linux host；S4 L6 非阻断），cross-build0 单列。现有 supplied receipts 无两目标联合 `FuzzFlowFrame` + `FuzzSSFrame` final live invocation / 精确集合结果；P2/P9 分别运行成功不是联合收据。P29 指定 validation 文件缺失，§16 的 124 行未逐项完成签录，six exits 整表不勾。用户关闭 S5 并未把这些文书/覆盖缺口变成运行成功。S6 broker exec、S8 incarnation/legacy migration、S9 NATS/Caddy、S10 全栈混载继续开放；ENDPOINT-RESUME 的最新矩阵红单独转 S6。

### 17.6 S5-DOCS 本地检查与 audit 缺口

本包仅改 evidence/review/tasklist/INDEX 四份 Markdown，无新增 Go guard/test 或产品执行。`go test -count=1 ./test/architecture/ -run 'Docs|Layout'` rc0；`go test -count=1 ./test/determinism/ -run 'Origin|Promised|Version'` rc0；新增引用路径存在性检查与 `git diff --check` rc0。

按 `test/hotupdate/README.md` 的 canonical extraction block 执行 Python audit，**rc1：frozen plan changed**。其期望 digest 为 `5a1fe8f6bf430b1949eaec676f0b40bc7a8ecbbb9c676bdf5ee7a950bb321f66`，当前 plan（按 audit 去除允许的 pointer 行后）为 `dd47cadc45f229b133822b29fde016958465388e556973efc66b437b4285d135`；本包没有修改 plan 或 audit。失败发生在 completion 检查之前，不能声称后面的检查已经执行。另经直接读取确认指定 `docs/reviews/hot-update-flow-carrier-ss-validation.md` 缺失；当前 audit 全部非 Darwin 行 COMPLETE 的规则也不能表达部分子项签收与 ENDPOINT-RESUME 携带裁决。请求其 owner 核对 lead 最新冻结裁决后更新 digest，并以逐项 receipt dependencies / 显式 carried disposition 核验，保留无回执不可勾的负控制。没有用假 COMPLETE 或替代 validation 绕过 guard。

## 18. S6 broker-image interim receipts (2026-10-12)

**S6 closes now under the user ruling: Pass（处置后；4 项测试失败与 drill 37/39.BR 实跑转入 S7 收口窗口）.** This section follows §17's source/receipt/native-result separation. It records package receipts and all three lead gate rounds; four gates-3 test failures belong to S6-TESTFIX-2 and the deploy run is NOT_RUN, scheduled in the combined S7-close quiet window. It does not certify all product exits or the whole stack. The [S6 internal review](hot-update-broker-image-review.md) preserves all 45 Opus findings, D1–D3, L1–L7 and later integration dispositions. The [frozen plan](hot-update-broker-image-plan.md) §0/§14 requires six AND exits and final same-candidate identities, resource/attempt oracles, mutations and costs. Earlier green package runs and later red merged gates are separate observations.

### 18.1 Candidate snapshots and contents

Each SHA below comes from the linked snapshot file. Contents use the snapshot commit title, package reports and adjacent snapshot file diffs; a snapshot is source identity, not a running-artifact receipt. `p00base`/`s6base*` are intermediate engineering snapshots, not final acceptance candidates.

| Snapshot receipt | Commit | What it contains / boundary |
|---|---|---|
| [p00base](/home/weiland/s6wt/snapshot-p00base.txt) | `0184b3b7108c60c239736c75202f811d7b8072b1` | S5 integrated base used for P00 drafting; no S6 implementation completion claim. |
| [s6base](/home/weiland/s6wt/snapshot-s6base.txt) | `04febd9f0cb888b3218f2487f601f18b8c70f916` | S5 candidate plus P00 frozen contracts/compile adapters. |
| [s6base2](/home/weiland/s6wt/snapshot-s6base2.txt) | `4d47339d40926bb56463fa0dbcfdd5e7c6a311ae` | S5 merged with P00/P32/P25/P08: contracts, inventory gates, wire schema and listener keeper. |
| [s6base3](/home/weiland/s6wt/snapshot-s6base3.txt) | `1b5268866d61feeaba0508907fc8c6304e572499` | Most implementation waves 1–3, per commit title; incomplete assembly. |
| [s6base4](/home/weiland/s6wt/snapshot-s6base4.txt) | `71bfda041648125622ca93a73f51c78cca1fc443` | 29-file continuation from base3: brokerimage cut/runtime/resources, tunnel pause/fences/image, P34 docs/audit and P29 flow fixture/broker collector. Presence does not establish production callers or continuity. |
| [s6base5](/home/weiland/s6wt/snapshot-s6base5.txt) | `46038c7fc39dd8e1bb03af1273408ba8f7fe3f31` | 12-file broker Run/reexec/local status/transfer/loop and single-store/reload wiring continuation. |
| [s6base6](/home/weiland/s6wt/snapshot-s6base6.txt) | `cf56958e6896266159089cb3243a5f5eea51adb3` | All implementation packages; four Opus reviews target this source. Missing production assembly and ordinary-operation regressions are confirmed by their reproducers. |
| [s6fix](/home/weiland/s6wt/snapshot-s6fix.txt) | `d84f693e4cfe46a7407d192bbab29930ed7df126` | base6 plus reviewers' reproducer tests; fix-package base. |
| [s6base7](/home/weiland/s6wt/snapshot-s6base7.txt) | `db8a018fb4d54c5e17fb0cddcc545026a4e32c8c` | Integration plus four fix packages, per commit title; D2's real cut base. Held maintenance admission hunk still requires reconciliation. |
| [s6base8](/home/weiland/s6wt/snapshot-s6base8.txt) | `652ed508676b69063a5ceb94e33f48eec7adac7b` | D2 working real single/N1 cuts, current rollback and lifecycle fixtures; complete stage exits still unsigned. |
| [s6base9](/home/weiland/s6wt/snapshot-s6base9.txt) | `33cb561b658ebf3562a2bbd07ec6e8cfca6854e6` | N3 maintenance/admission/current history integration and real three-voter original-TCP test. |
| [s6base10](/home/weiland/s6wt/snapshot-s6base10.txt) | `96e223e9f5fa8bef0967110040a31a53cbdf5a7b` | COHORT collector/admission/full-flow fixture changes; its real T48/P27 failures remain red at that receipt. EXITS-2-3 uses this as its base and supplies later positives. |
| [s6base11](/home/weiland/s6wt/snapshot-s6base11.txt) | `7345c94cfa1e589fc9e94d0bc524a11b078ed83c` | Slim integration. BROKER-SLIM and EXITS-2-3 reports describe the budget relocation and retained StartBusiness/heartbeat behavior; source presence is distinct from final gates. |
| [s6base12](/home/weiland/s6wt/snapshot-s6base12.txt) | `7cc5a293bdb54f41c91d2c1c7a8f82c2f497557c` | GATES-S6-B repairs; cluster/tunnel budgets and StatusReport relocation still require later owners. |
| [s6base13](/home/weiland/s6wt/snapshot-s6base13.txt) | `fdc0a5b34853dbfa1dc1192c1233c19811602858` | Cluster/tunnel slimming into leaf owners; scanners, observation overlays and ledgers must follow moves. |
| [s6base14](/home/weiland/s6wt/snapshot-s6base14.txt) | `d8214bf073da7b9ce9564b5d376a80822b0133d2` | GATES-S6-C identity/enum/leak and CLI split integration. |
| [s6base15](/home/weiland/s6wt/snapshot-s6base15.txt) | `f2c9f54dc089beda13395c5cc3ab10815cc55988` | GATES-S6-D source pins, private inventory/layering/TLS repairs and cluster-add split. |
| [s6cand1](/home/weiland/s6wt/snapshot-s6cand1.txt) | `2742ccf0c7c65530483529bdde8584ac0d3cad65` | Commit title: “s6 candidate f00adb03 + domain test”; actual tree is `f00adb03139ad2ad1c554af2ea9607fee1a036f4`. Includes GATES-S6-E legacy-file ratchet/new-file splits, CLI fixtures/resolver, and explicit S5/S6 domain cases. This is the tree of gates-1; the title alone is not a test receipt. |
| [s6cand2](/home/weiland/s6wt/snapshot-s6cand2.txt) | `0c7642e932432cad019e41f62cc8a24459158417` | cand1 plus HARDGATE-A compile/scanner repairs; tree `b402d810f5216148fa613bb7e26a807e7c09e3c1`. Historical DOCS-1 worktree base; superseded by the gates-2/3 identities below, not a drill receipt. |

The lead gates-1 identity additionally records working source tree `f00adb03` over committed HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`, unchanged after execution. Do not substitute snapshot commit IDs for that recorded tree or infer a deployment/running SHA from either.

### 18.2 Real hot-cut and lifecycle package receipts

`G` in this subsection expands to `go test -race -count=1 -timeout 30m`. The commands below are **reported executions**, not reruns by this docs package. Their reports retain exact commands, raw logs, native rc, mutation digests, nested artifact identities and cleanup observations. Base commit identifies the package's starting source; final changed-byte/overlay identity must be read from the corresponding report. Final deploy source/artifact/running-SHA triple is NOT_RUN and carried into the combined S7-close quiet window.

| Package / base / report | Real result and raw receipt | Limits / mutations |
|---|---|---|
| [D2](/home/weiland/s6wt/prompts/s6-d2-complete-report.md), base `db8a018f` | `G ./internal/broker -run '^TestBroker(ExecStaysInsideRun\|ActiveImportFailureUsesCurrentCut\|RunningSHAIgnoresStagedReplacement)$'` rc0; real single/N1 five rounds, Run-internal exec, current-store rollback, continuous canonical flock, ACK before replay, actual closed driver/work census and original TCP/SS. [Core cut log](/tmp/s6-d2-real-cut-final-proof.log), 237.617s. SHA-bound incoming previous reader regression five rounds rc0, [log](/tmp/s6-d2-reader-binding-final.log), 120.967s. | 37 distinct accepted mutations native1, [machine receipts](/tmp/s6-d2-mutations/all-results.json); unlock/old-snapshot/old-Run-unwind/current-direction/late-revoke/reader-binding defects fail. Full T48/P27/N3 were still pending or red at this report. |
| D2 P26/P28 lifecycle suite | `G -tags hotupdate_integration ./test/hotupdate -run '^Test(CurrentRuntimeStopAfterExecError\|BrokerPreparedListenerRevokeCannotResurrectEndpoint\|BrokerActiveRollbackKeepsNewACK\|BrokerCurrentRollbackAndNormalStop\|BrokerLifecycleSeamLeakCycles\|SoleVoterReloadWithinBudget)$'` rc0; [five-round receipt](/tmp/s6-d2-p28-p26-final.log), 731.057s. Includes current reopen/new ACK/terminal cleanup, ACTIVE current-cut rollback and prepared listener revocation. Remaining store/exec/snapshot consumers rc0 in [component log](/tmp/s6-d2-p26-component-complete.log). | Whole brokerimage/flow/clusterharness races and targeted tunnel/serve checks rc0; global gates native1. These lifecycle checks do not replace native 37.R/T/39.BR. |
| [N3-MAINT](/home/weiland/s6wt/prompts/s6-n3-maint-report.md), base `652ed508` | `G ./test/clusterharness -run '^TestBrokerN3MaintenancePreservesHomeAcrossExec$'` rc0, five real three-process serve/SQLite/Raft rounds with routed authenticated NATS, actual agent capability/ACK, signed driver, original TCP/target socket and home/epoch continuity. [N3 log](/tmp/s6-n3-maint-n3-proof-final.log), 117.427s. Whole [clusterharness](/tmp/s6-n3-maint-clusterharness-final-receipt.log) rc0, 188.981s; brokerimage/CLI/brokermaint/clusterupgrade races rc0. | 16 intended mutants assertion1, [unit](/tmp/s6-n3-maint-mutations/unit-receipts.json) and [product](/tmp/s6-n3-maint-mutations/product-receipts.json). Wrong role/deadline, missing applied cursor/cache/history/CutID, self-blocked issuer and peer-only auth proof fail. Extra self-subject routing hypothesis survived rc0 and was removed, not counted as killed. Original TCP proof is narrower than complete T48 SSH/SS/FIN. |
| [COHORT-COLLECTOR](/tmp/s6-cohort-validation/REPORT.md), base `652ed508` | Real agent/SSH foreground child, original TCP/SS, pending OPEN, independent hash/FIN/accept/socket census, producer-clock phase and true-start watcher; busy transfers typed-defer and preflight/freeze deadlines separated. Whole brokerimage/CLI/clusterharness and S1 N-1 controls rc0. Full T48/P27 **native1**, including N1 reverse gaps 5.126209564/5.606104101s and mixed deadline/replay exhaustion; quorum initially unsupported. | 19 accepted mutants native1; late AEAD injection initially survived and was corrected/excluded before the accepted run. These reds remain historical observations; the later EXITS receipt below supplies actual final package positives. |
| [EXITS-2-3](/tmp/s6-exits23-receipts/REPORT.md), base `96e223e9` | `G -tags hotupdate ./test/p13 -run '^TestBrokerExecOriginalFlowCohort$' -v` rc0, 818.856s, [final T48 log](/tmp/s6-exits23-t48-release.log): five single + five sole-voter + five quorum rounds, 126,510 exact ledger records/FIN observations, original SSH child/SS/TCP sockets, connecting OPEN, >one-ring tail, fresh connects and shared leaks. Changed-byte digest `765fa3527a1e62b0e535465c259c9354d151734e0ee5f6d5cd5f5fbfa3c6c4ce` is reported unchanged after this run. | Nine accepted mutants native1; stronger replacements exclude earlier green/ineffective drafts. Normal replay budget and five-second assertions unchanged. Final merged gates and deploy drills still pending. |
| EXITS-2-3 control/N-1/current-write suites | `G ./test/cli_e2e -run '^(TestControlAdmissionAbsoluteBudget\|TestBrokerControlAuthBlackoutKeepsAttemptsAndOneEffect\|TestBrokerControlBlackoutMixed\|TestBrokerControlIntegrationLeakCycles\|TestBrokerControlEvidenceRejectsCompensation\|TestBrokerMaintenanceDeliveryUsesInstalledCapability)$' -v` rc0, [P27 log](/tmp/s6-exits23-p27-complete.log), 99.273s. S1 old-broker/old-agent/run quadrants rc0, [N-1 log](/tmp/s6-exits23-nminus-stable.log), 141.240s; sole-voter five current-write rounds rc0, [log](/tmp/s6-exits23-sole-writes-final.log), 54.23s. | Sole-self-voter normal TimeoutNow, actual heartbeat refresh and result-report reserve remove real readiness/admission gaps. Two real control targets retain normal replay headroom. Whole CLI/cluster/clusterharness races rc0; architecture/determinism native1 on then-unmerged owner seams. Hermetic old subscription/version evidence is separate from a real old-artifact deploy roll. |

The EXITS final flow/calibration observations are:

| Topology | Complete rounds | Max original-flow gap, seconds | Max cut-to-control-ready, seconds | Native rc |
|---|---:|---:|---:|---:|
| single | 5 | 1.988396690 | 1.138657566 | 0 |
| sole voter | 5 | 2.725405073 | 2.938346187 | 0 |
| N=3 quorum | 5 | 2.592179509 | 3.295549448 | 0 |

All fifteen cut-to-ready intervals exceed the 500ms calibration target. The report preserves those excesses; the hard flow budget remains 5s. These are package observations, not final deploy costs or final full-freeze distribution. Final deploy latency/freeze/resources and baseline/after marginal costs are NOT_RUN, carried into S7; gate elapsed seconds below are measured wall times only.

Later slimming receipts also preserve real cut checks: [BROKER-SLIM](/home/weiland/s6wt/prompts/s6-slim-report.txt) reports an initial aggregate 600s timeout, retained native1, followed by explicitly bounded 30m broker hotupdate rc0 (868.357s); [CLUSTER-SLIM](/home/weiland/s5wt/prompts/result-s6-slim-cluster.md) reports clusterharness/hotupdate rc0 (178s/811s); [TUNNEL-SLIM](/home/weiland/s5wt/prompts/result-s6-slim-tunnel.md) reports unmodified observation harness build red, then narrow harness-only overlay rc0 (183.237s/785.624s). The latter is not an unmodified-harness pass. Gates-1 confirms stale overlay failures still present on the merged candidate.

### 18.3 First lead hard-gate run: gates-1

Raw root: [/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/). [Identity](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/identity.txt): source tree `f00adb03139ad2ad1c554af2ea9607fee1a036f4`, committed HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`, start `2026-10-12T10:54:29-05:00`, same tree afterwards. [Native rc/wall](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/rc.txt) records all four runs red:

| Command | Native rc | Recorded seconds | What the receipt actually shows |
|---|---:|---:|---|
| `make test` | 2 | 4 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/test.log): prerequisite vet-tags fails `phasefluidity_lifecycle_test.go` against moved private controller member `caughtUpFn`; test target never reaches a clean full execution. |
| `make e2e-parallel` | 2 | 1126 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/e2e-parallel.log): `FAILURES: 37`; native failed units retained, including build/fixture/scanner/assertion/timeout failures. Some units pass, including ReliableExec/ReliableRun and the watchdog; that does not make the matrix green. |
| `make lint` | 2 | 15 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/lint.log): tagged typecheck uses removed private controller members; also `internal/broker/clusterdrain.go` not gofmt-clean. No clean lint result. |
| `make gates` (separate centralized check) | 2 | 3 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-1/gates.log): same prerequisite vet-tags compile failure; no clean centralized run. |

The three hard gates are test/e2e-parallel/lint; gates is a separate centralized check. No rc2 above is converted into a pass. Failure categories, with attribution limited to the recorded assertions:

| Category | Recorded examples / disposition |
|---|---|
| Slim relocation/tag and scanner seams | Phasefluidity private names; auth ACL extractor misses moved/named subscription tables; broker admission/C5 pins; proto emitter/type-alias/budget-literal location; flow parent/BR registration. HARDGATE-A supplies scoped repairs and red controls; later gates-3 gates rc0 is recorded below. |
| Stale real-product observation overlays | Many broker control/clusterharness/hotupdate/lifecycle/P13 tests fail nested product build: injected code lost incidental `os` import and still uses lowercase fields on relocated `resumeRequest` (`Scope/Mode/RequestID/Endpoint` are now exported). Earlier overlay-assisted package passes do not close the unmodified merged failure. HARDGATE-B repairs the overlays and supplies real reruns; gates-3 no longer fails their nested build. The historical build reds remain unchanged. |
| Ordinary behavior/fixture boundaries | Six port missing-proof expectations; shutdown cancellation comparisons, home-expiry dwell expectation, positive final seal without journal; unbound HTTP store fixture and landed contractstubs; D7 and broker source/behavior tests. Each needs its owner disposition against frozen semantics, not an all-green assumption. HARDGATE-B/TESTFIX dispositions and later gate rounds are recorded below and in review §3; this row describes the original red observations. |
| Inventories and scheduling/resource probes | Missing wire inventory rows; TestAllPhases/P13 90s alarm; RemoteFS unit; raw artifact domain/ABI probe and other hotupdate failures. Root causes not established merely by the matrix log. Later TESTFIX/LEDGERS dispositions and gates-3 matrix results follow; no root cause is inferred solely from this original log. |

[HARDGATE-A report](/home/weiland/s5wt/prompts/result-s6-hard-a.md) records tagged vet/build, targeted scanner races and determinism/architecture rc0; 18 assertion-red controls and one intended compile-red control. It also records whole proto native1 (308 wire rows), port native1 (six proof expectations), p4 native1 (forwarding/readiness) and P13 scheduling still unresolved at that round. This is a repair receipt, not a second complete hard-gate run. GATES-S6-B/C/D/E's earlier local green checks likewise do not override gates-1.

### 18.3.1 Second lead run: gates-2

Raw root: [gates-2](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/). [Identity](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/identity.txt) records source tree `acc3b273957d7b7f7439e9009640386c3710e6f3`, HEAD `54b0c9eedd09247680bb626c516058a8d9f30341`, start `2026-10-12T14:30:21-05:00`, identical tree after execution. All four commands remain red in [rc.txt](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/rc.txt):

| Command | Native rc | Recorded seconds | Receipt / reason |
|---|---:|---:|---|
| `make lint` | 2 | 34 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/lint.log): substantive lint findings across cluster, broker/image, tunnel/flow, HTTP/inbox/storage, CLI/agent and fixtures, plus `clusterdrain.go` gofmt. LINT-A..F later supply scoped fixes/controls; their reports are not themselves a global lint run. |
| `make test` | 2 | 615 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/test.log): broker/P13 cumulative 600s timeouts, moved schema/observation scanners, pristine repack bind collision, stale-legacy response ordering, absent-proof expectations, wire inventory and unsupervised excluded CLI launches, plus p4/p5/p6 startup/admission failures. TESTFIX root causes and final controls are in the linked report below. |
| `make e2e-parallel` | 2 | 1163 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/e2e-parallel.log): **nine failed units**: two P13 cohorts (600s), CLI reliable black-window/discovery, AllPhases (90s child budget), port proof expectations, D4 broker shard8, RemoteFS reliable exec/run, rollback private target traffic and proto inventory. TESTFIX’s rollback SS rejection did not reproduce in three pinned runs; its cause remains unconfirmed rather than reclassified. |
| `make gates` | 2 | 4 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-2/gates.log): prerequisite vet-tags clears, proto append-only wire inventory fails on missing rows. |

[Gate-repair dispositions](hot-update-broker-image-review.md#3-post-review-integration-findings) preserve HARDGATE-A/B, FIX-BLACKOUT/-2, FIX-ROLL, FIX-ORPHAN, FIX-MATRIX, SUPERVISE-CLI and LINT-A..F. [TESTFIX](/home/weiland/s5wt/prompts/result-s6-testfix.md) / [full report](/tmp/s6-testfix-evidence/report.md) supply per-failure causes, exact commands and restored controls; [archived test result index](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/testfix/test-results-index.json) and [mutation history](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/testfix/all-mutation-results.json) retain initial ineffective controls as well as final negatives. [LEDGERS](/home/weiland/s5wt/prompts/result-s6-ledgers.md) reconciles moved tests/leak anchors/tags and ceilings. Do not convert nine original matrix reds into passes from these scoped reports.

### 18.3.2 Closure candidate run: gates-3

Raw root: [gates-3](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/). [Identity](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/identity.txt) records source tree `3a9ec85a133da6573b099c7a91dd6649009b06c8`, HEAD `54b0c9eedd09247680bb626c516058a8d9f30341`, start `2026-10-12T16:08:34-05:00`, identical tree afterwards. Per the user handoff, this candidate lives separately at `/home/weiland/s6cand` and its patches were mirrored into the integration tree. It is a source-tree identity, not an artifact/running-SHA receipt.

| Command | Native rc | Recorded seconds | Exact result |
|---|---:|---:|---|
| `make lint` | 0 | 2 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/lint.log): `0 issues.` |
| `make gates` | 0 | 539 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/gates.log): vet-tags, proto, deterministic/architecture/parallel checks and hermetic gates pass; `simcluster hermetic gates: ALL PASS`. This is a hermetic result, not a deploy drill. |
| `make test` | 2 | 585 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/test.log): **one failure**, `internal/execsession: TestFreezeRollbackBoundsRuntimeAccountingLock/4`, `freeze never closed admission`. |
| `make e2e-parallel` | 2 | 1129 | [Log](/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/e2e-parallel.log): **three failed units**, detailed below; footer `FAILURES: 3`. |

| Failed e2e unit / test | Observed failure / S7 owner |
|---|---|
| `HotUpdateIntegration:test/hotupdate[TestBrokerLifecycleSeamLeakCycles]` | Round0 `prepared-revoke-barrier` REGISTER denied `session_closed`; S6-TESTFIX-2. |
| `HotUpdateIntegration:test/hotupdate[TestHalfCloseTailAcrossRealExec]` | Round0 **`real expose: exit status 64`**, `agent_rejected:frpc_failed`; agent REGISTER denied `session_closed`; S6-TESTFIX-2. |
| `HotUpdateBroker:test/p13[^TestProxyDisableDuringTunnelDropStaysDown$]` | `127.0.0.1:14000` became connectable in the no-resurrect window; S6-TESTFIX-2. |

The four latest failures are carried, not fixed or waived by a green assertion. Their root causes are not yet established by a repair receipt. The user ruling closes S6 now, with S6-TESTFIX-2 continuing in S7’s close flow and the deploy-tier run scheduled for the combined S7-close quiet window. Earlier fixed package evidence is retained alongside these latest reds. No separate open S5 carry is identified in the current handoff.

### 18.4 Closure receipt status and S7 carry

Final deploy receipts must retain exact command/native verdict, source and previous/candidate artifact/running SHA, PID/startticks, every child/parent arm, independent raw oracles/counters, phase intersections, log mapping, mutation/restore and wall/cost. The user’s administrative stage closure does not fill missing product receipts. **No S6 deploy-tier drill has run yet.** All drill rows below are NOT_RUN and scheduled in the combined S7-close quiet window; tests and deploy drills will run serially. 37 still requires native GREEN0 and genuine old N3 Run-unwind 39.BR PRODUCT-RED3; INCOMPLETE stays native4.

| Required final receipt | Actual result / carry disposition |
|---|---|
| Final candidate/identity and all six exit rows | Source candidate `/home/weiland/s6cand`, tree `3a9ec85a133da6573b099c7a91dd6649009b06c8`, patches mirrored into integration; gates-3 identity above. Deploy source/artifact/running triple and row-by-row six-exit certification NOT_RUN, carried into S7. |
| Final `make test` | Gates-3 native2, 585s, one execsession failure; S6-TESTFIX-2 in S7. |
| Final `make e2e-parallel` | Gates-3 native2, 1129s, three units; S6-TESTFIX-2 in S7. No complete-green matrix or all-process census inferred from this run. |
| Final `make lint` | Gates-3 native0, 2s, zero issues. |
| Final separate `make gates` | Gates-3 native0, 539s, tagged vet/proto/determinism/architecture/parallel/hermetic gates. |
| Whole-risk-package race, inventories, leak anchors and resource caps | Scoped race/mutation/census receipts in package reports; gates-3 centralized checks native0. Full matrix remains red on three units; final combined acceptance/census carried into S7. |
| Linux/Darwin build and platform behavior | HARDGATE-B, FIX-ROLL, TESTFIX and LINT-A..F report Linux/Darwin builds rc0. Native Darwin lifecycle NOT_RUN without a runner receipt, carried into S7; cross-build never implies native pass. |
| 37.A — single | NOT_RUN; combined S7-close quiet window: original cohort/control/store oracle, child/parent verdicts. |
| 37.A1 — actual sole voter | NOT_RUN; same window: production timers, leader upgrade and ≤5s first write. |
| 37.B — N=3 broker-only | NOT_RUN; same window: every-hop transfer/config/observer ACK/home history and unchanged co-located agent identities. |
| 37.T — transfers | NOT_RUN; same window: S6 busy/defer baseline and S7-capable transfer continuation under the frozen S7 contract, with legacy/unknown defer separately retained. |
| 37.R — rollback | NOT_RUN; same window: legal held after-Take and ACTIVE faults, current new ACK/offset/generation/previous reader. |
| 39.BR | NOT_RUN; same window: actual previous N3 follower/home Run-unwind barrier, native PRODUCT-RED and original-cohort failure. |
| Regression 75 | NOT_RUN; same window: A/C/R/B/W/L child/parent, original flow/SS identities and negatives. |
| Regression 36 | NOT_RUN; same window: A/W/C/L, original PTY/ctl lifecycle and strict mixed requests. |
| Regression 39 | NOT_RUN; same window: X/Y/P/F plus BR manifest pins and parent aggregation; known native reds retain their meaning. |
| Regression 34 | NOT_RUN; same window: seven-arm lifecycle and original child/ctl identity/exit receipts. |
| Regression 35 | NOT_RUN; same window: three-arm real-old supply/N-1 receipts. |
| Regression 33 | NOT_RUN; same window: preserve any native INCOMPLETE4 and gotcha #73 boundary. |
| Regression 94 | NOT_RUN; same window: complete native regression receipt. |
| Regressions 70 / 72 | NOT_RUN; same window: complete scoped native/oracle/cleanup receipts. |
| Cost/freeze/latency/resources | Gate seconds and package calibration are measured above; final matched baseline/after marginal cost, build/test separation and deploy memory/FD/replay/max-freeze observations NOT_RUN, carried into S7. |
| ENDPOINT-RESUME historical receipt | Current user handoff: no open S5 carry; gates-3 does not list the old invalid-flow-owner-state failure. Dedicated mechanism/mutation closure receipt absent, final aggregate row unsigned and carried for S7 reconciliation. Historical §17 native red stays red. |
| Supplemental receipt gaps | Stronger after-Prepare stop barrier, domain-specific mutation and schema-framing disposition have no located final receipts; NOT_RUN / carried into S7 reconciliation, without inferring additional current product failures. |
| Review verdict / INDEX | User ruling: Pass（处置后；4 项测试失败与 drill 37/39.BR 实跑转入 S7 收口窗口）; [review](hot-update-broker-image-review.md) and [INDEX](INDEX.md) record 2026-10-12 closure. |

### 18.5 Documentation-only verification

S6-DOCS-1 changes only the review, this evidence section and receipt-based tasklist subitems; no product, test, mechanical ledger or INDEX row changes. These checks validate documentation boundaries only, never the stage exits above. [Local command/rc capture](/tmp/s6-docs1-checks.json):

| Local command | Native rc / result |
|---|---|
| `go test -count=1 ./test/architecture/ -run 'Docs\|Layout'` | 0; [raw log](/tmp/s6-docs1-architecture.log); docs top-level layout, layout-map reconciliation/controls and live workflow references |
| `go test -count=1 ./test/determinism/ -run 'Origin\|Promised\|Version'` | 0; [raw log](/tmp/s6-docs1-determinism.log); origin references/controls, promised guards/controls and current/stray wire-version checks |
| `python3 /tmp/s6-docs1-audit.py` (exact S6 Python fence extracted from test/hotupdate/README.md) | 0; 11 documents, 7 landed source/test promises, 8 unchecked-or-evidenced aggregate claims |
| Finding/test/link reconciliation | 0; exact 45 identities, original severity 9/20/16, all referenced Test identifiers declared in the current tree, new S6 local/receipt links exist |
| `git diff --check` | 0 |

Exact matched Go test identities:

- `test/architecture/docs_layout_test.go: TestDocsTopLevelHoldsNoProcessArtifacts`; `test_layout_map_test.go: TestTestLayoutMapIsReconciledWithTheTree`, `TestLayoutMapParserSeesTheShapes`; `ci_workflow_test.go: TestLiveDocsReferenceOnlyExistingWorkflowFiles`.
- `test/determinism/origin_line_test.go: TestOriginLinesPointAtDocumentsThatExist`, `TestOriginDocRefRecognisesTheRealShapes`; `promised_guard_test.go: TestPromisedGuardTestsExist`, `TestPromisedGuardScannerIsNotVacuous`; `docs_wire_version_test.go: TestDocsUseCurrentWireVersion`, `TestDocsWireVersionScannerIsNonVacuous`; `lint_skeleton_test.go: TestNoStrayVersionLiteral`, `TestNoStrayVersionLiteralSelfCheck`.

[Negative-control receipts](/tmp/s6-docs1-audit-mutations.json) retain exact commands, native rc and assertion text; the two tasklist controls edit the owned file on disk and restore in finally. Three audit registry/CLI variants run as on-disk extracted-fence copies in /tmp; canonical README stays unchanged under docs-only ownership. These exercise the existing audit and add no new guard or repository test.

| Independent defect | Observed native rc / intended oracle |
|---|---|
| Check all eight final claims without receipts | 1 / premature S6 completion: E-4 |
| Mark E-4 COMPLETE without evidence | 1 / missing final source SHA: E-4 |
| Replace one audit document with a missing path | 1 / missing S6 documentation |
| Replace a promised Test symbol with a nonexistent one | 1 / dead S6 source/test promise |
| Require a nonexistent broker CLI verb | 1 / missing broker CLI verb |
| Restore tasklist and exact canonical fence | 0 / S6_DOC_AUDIT_OK |

This package creates no test-inventory or leak-anchor rows. Product/drill/global gate runs were NOT_RUN by DOCS-1; its historical local checks do not supersede the later lead receipts and S7 carry in §18.4. The canonical S5 audit is a separate historical check; it was not rerun here.

### 18.6 S6-DOCS-2 closure-document verification

This worker changed only the four owned files: review, evidence §18, tasklist and INDEX. The review’s first line and INDEX now record the user’s S6 closure; all eight final aggregate claims stay unchecked/NOT_RUN. The lead’s archived gates above were read, not rerun by this worker.

| Local command / scope | Native rc / evidence |
|---|---|
| `go test -count=1 ./test/architecture/ -run 'Docs\|Layout'` | **1**; [raw log](/tmp/s6-docs2-architecture.log). Only `TestTestLayoutMapIsReconciledWithTheTree` fails: existing `fullstackharness`, `infraharness`, `transferharness` directories absent from `test/README.md` table. These are later-stage S10/S9/S7 harnesses in the integration tree, not additional gates-3 candidate failures. |
| `go test -count=1 ./test/determinism/ -run 'Origin\|Promised\|Version'` | **0**; [raw log](/tmp/s6-docs2-determinism.log). Origin references/controls, promised guards/controls, current/stray wire-version checks. Exact test paths/names remain those listed in §18.5. |
| `python3 /tmp/s6-docs2-audit.py` — exact bounded S6 Python fence from `test/hotupdate/README.md` | **0**; 11 documents, 7 landed promises, 8 unchecked-or-evidenced aggregate rows; restored canonical audit also rc0. |
| `python3 /tmp/s6-docs2-reconcile.py` — receipt/link/finding reconciliation | **0**; [result](/tmp/s6-docs2-reconciliation.json): all added local/receipt links exist, 45 finding IDs retained once, all S6 open slots given an actual receipt or explicit carry, three exact gate identity trees checked. |
| `git diff --check` | **0**. |

[Five independent negative receipts](/tmp/s6-docs2-audit-mutations.json) retain command, native rc, assertion and extracted-source digest. Tasklist mutations use on-disk edit/run/restore in finally. The three audit document/promise/CLI mutations use on-disk extracted-fence copies under `/tmp`; canonical README remains untouched under four-doc ownership.

| Independent mutation | Observed native rc / oracle |
|---|---|
| Check all eight aggregate boxes without receipts | 1 / `premature S6 completion: E-4` |
| Mark E-4 COMPLETE without evidence | 1 / `missing final source SHA: E-4` |
| Audit references nonexistent document | 1 / `missing S6 documentation` |
| Audit promises nonexistent Test name | 1 / `dead S6 source/test promise` |
| Audit requires nonexistent broker CLI verb | 1 / `missing broker CLI verb` |
| Restore owned tasklist and exact canonical fence | 0 / `S6_DOC_AUDIT_OK` |

**Request to the later-stage docs/map owner:** append these exact rows within the existing five-column `test/README.md` directory table, then rerun the unchanged Docs/Layout command. That file is outside this worker’s ownership; no assertion, row parser or ledger was changed.

| 目录 | 层 | build tag | 怎么跑 | 守什么 |
|---|---|---|---|---|
| `fullstackharness/` | helper + receipts | — | `go test -race ./test/fullstackharness/` | S10 shared supervision, owner-death cleanup and executable/OFD identity witnesses |
| `infraharness/` | helper + receipts | — | `go test -race ./test/infraharness/` | S9 infrastructure process lifetime and private observation custody |
| `transferharness/` | helper + receipts | — | `go test -race ./test/transferharness/` | S7 real-binary launches, native exits and artifact/history receipt checks |

No test-inventory/leak-anchor rows arise from this docs-only work. Product tests, global make/lint targets, deploy drills and native Darwin execution are NOT_RUN by this worker under the package restrictions. All requested local commands were executed; the layout-map failure is reported rather than waived. Remaining gate/drill/product carries are listed in review §4 and §18.4 above.
