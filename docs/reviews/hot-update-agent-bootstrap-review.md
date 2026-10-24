Pass（处置后）—— 内审原判 **Fail**（核验后 BLOCKER 5 条〔去重后 3 个缺陷〕+ MAJOR 15 条，见 §1）；全部 33 条 finding + 4 条核验补报按 §2 逐条处置（采纳并修复 33 / 部分采纳 1 / 驳回 1 子项 / 记录 2 NOTE），处置后的最终候选树收据见 §6（三硬闸 + `make gates`）；deploy-tier drill 在 §7（实跑结果单列，不以内审结论代替）。

内审原判：Fail —— 五路审查的共同结论是：**S3 的主路径（install → cut → 同 PID exec → 导入 → thaw → register → commit）是对的，错在它的每一条「出口」上**：首次 register 被权威拒绝时，回滚判断读的是 session 已取消的子 ctx，于是根本不回滚、`defer` 的 Close 直接杀掉携带任务（RB-1/CC-6）；commit 写盘失败后映像处于 ready 而 marker 仍 pending，watchdog 只认 active，于是对携带任务的 runtime 裸 exec（RB-2/CC-1）；watchdog 回滚导出 generation 2 却在 marker 里留下 generation 1，drill 34.W 的判据因此不可能成立（RB-3/DR-3/TD-3）；而 drill 34 的 activation judge 连真实的 slog 记录都解析不了，七臂里六臂会把健康产品报红（DR-1）。另有混合探针可以在「零写入」「切换窗口与真实切换无关」时判绿（DR-4/DR-5），expose replay 与 home 推送绕过 cut 的准入（CC-4），以及一组 admission / courier / 资源寿命缺陷。S3 在处置前不能收口。

# S3 · agent bootstrap 与 node upgrade 接线 —— 内审报告与主进程处置

日期：2026-10-02。范围：S3 相对 S2 最终 index 树 `ddd301c878841631bab63e283cd3ac66cdff0904` 的全部改动（plan：[hot-update-agent-bootstrap-plan.md](hot-update-agent-bootstrap-plan.md)；证据：[hot-update-evidence.md §14](hot-update-evidence.md)）。审查对象为集成快照 `a9dc5bf910b73f14b121f344deec1c59798495b4`（不含 T16 e2e；T16 在审查期间合入，其覆盖面由主进程在 §2 TD-1 处置中实测）。

**形式（本 stage 起的开发方式变更）**：用户 2026-10-02 裁定开发改为 **GPT-6-Astra（Codex，`--model gpt-6-astra --effort xhigh`）并行开发、不设 agent 数量上限**。本次内审因此为 **5 路审查（rollback / concurrency / resources / drills / testsdocs）→ 每路 1 个对抗核验者（共 5 个，审查一结束即派发，pipeline 不设屏障）**，每个 agent 一个独立 git worktree（从集成快照 `--detach`，不动任何 ref），全程 Monitor 盯后台 job。审查者只读实现、可加复现测试（全部保持失败作为证据）；处置同样由 6 个 Astra 修复包并行完成（文件归属不相交），主进程逐包审 diff、三方合并、登记、复跑。原始报告：`/home/weiland/s3wt/reviews/{lane}.md` 与 `{lane}-verify.md`，归档于 `~/tether-hot-update-handoff-20260920T2356Z/s3-review/`。

## 1. 结论

审查者共 **33 条 finding**（BLOCKER 5、MAJOR 14、MINOR 12、NOTE 2），核验者对 **全部 33 条给出 CONFIRMED**（TD-7 的 6 个子项中 1 个被驳回为「非合同违背」），严重度**零调整**；核验者另补报 **4 条**（CC-V1 MAJOR、DRILLS-V1 / RESOURCES-V1 / TESTSDOCS-V1 MINOR）。

跨 lane 的独立重复发现（同一缺陷，按一处修）：**RB-1 = CC-6**、**RB-2 = CC-1**、**RB-3 = DR-3 = TD-3**、**RS-2 = TD-2**、**RB-5 ≈ RS-5**（NOTE）。去重后阻断缺陷为 3 个 BLOCKER（Run 恢复读错 ctx、ready 映像被裸 exec、activation judge 解析不了真实日志）与 11 个 MAJOR。

横贯的根因有两条：(a) **「被测的入口」与「生产的入口」不一致**——T12 直接调 `runFailedBeforeControlReady`，绕开了必然取消子 ctx 的 `session()`；T11 用 `UpgradeExecFn` 只看 bundle generation、不看 marker；drill judge 的正例是裸 message 字符串而不是 slog 记录。每一处都让测试绿着而生产路径坏着。(b) **只认「刚导入、还没 register」的状态机太窄**——watchdog、Run 失败、install 守卫都以 `PhaseActive` / 「deadline 未过」为「还持有事务」的判据，而 ready + pending（commit 写盘失败）、过期但仍被本进程持有的 pending（CC-V1）都是真实可达的「仍持有」。

主进程处置：**全部处置**，见 §2；修复后 33 + 4 条的复现测试全部转绿、每条修复都有回注原缺陷的变异收据（§5）。

## 2. finding 逐条处置

严重度为核验后结论；「核验」列给出 verdict。报告原文（含证据、触发条件、建议）见 `s3-review/` 归档；此处只列定位与处置。

### 2.1 BLOCKER

| # | lane | 核验 | 内容 | 主进程处置 |
|---|---|---|---|---|
| RB-1 / CC-6 | rollback / concurrency | CONFIRMED（两路独立） | `runFailedBeforeControlReady` 读 `a.loadRunCtx()`（per-session 子 ctx），`session()` 返回前必 `cancelRun()`，首次 register 权威拒绝（如 `proto_mismatch`）时恢复路径永远视为「运维停止」而跳过，`defer stopProcessRuntime` 杀掉携带任务 | **采纳，已修（FIX-CUT）**：签名改为 `runFailedBeforeControlReady(ctx, a, err)`，Run 传父 ctx，持锁后再查一次；保留真实父 ctx 取消与 `errSessionGone` 的排除。回归 `TestRunAuthoritativeRefusalRedispatchesImportedTasks` 走真实 Run→session→register 拒绝（嵌入 NATS + 真携带 child），`TestRunFailureRechecksParentAfterLock`；变异 M1/M2/M14 红 |
| RB-2 / CC-1 | rollback / concurrency | CONFIRMED（两路独立） | 首个 register 成功但 commit 的 marker fsync 失败（ENOSPC/EIO）→ marker pending、映像 ready；watchdog 的携带判断只认 `PhaseActive`，ready 走裸 `upgradeExec`，业务 pipe/replay 随 CLOEXEC 关闭 | **采纳，已修（FIX-CUT）**：新增 `ownsUpgradeImage`（active **或** ready），watchdog 据此选择「切当前态再派发」；「register 成功」（MarkReady）与「commit 已落盘」分开。回归 `TestWatchdogReadyImageRetainsHandoffAfterCommitWriteFailure`；变异 M3 红 |
| DR-1 | drills | CONFIRMED | `freeze_windows` 要求数字后跟空格/逗号/行尾，而生产 activation 行的字段在 slog msg 里，text 与 JSON 两种格式都以引号收尾 → judge rc 2，34.A/G/U/N/W/F 六臂的最终 image 判据必红 | **采纳，已修（FIX-DRILLS）**：judge 先按 text（shlex）/ JSON 解析出 msg 再取字段，缺失/畸形/零值/重复仍拒绝。**跨语言接缝**用双侧 golden 钉住：`cmd/tether/agent_log_format_test.go`（真实 daemon logger 渲染 activation 行 == `test/simcluster/tests/testdata/agent-activation-records.txt`）+ Python hermetic 测试把同一 golden 喂真实 judge；改 logger 或改解析任一侧即红 |

### 2.2 MAJOR

| # | lane | 核验 | 内容 | 主进程处置 |
|---|---|---|---|---|
| RB-3 / DR-3 / TD-3 | rollback / drills / testsdocs | CONFIRMED（三路独立） | watchdog / Run 失败的「切当前态」导出 generation 2，但 `executeRollback` 前只写 `ResourcePhase`，marker 留 generation 1，且没有回滚 cut 的 `agent: image cut` 行；接收方的 `stampMarkerPhase` 只写 pending，无法补救 → 34.W 判据不可能成立 | **采纳，已修（FIX-CUT）**：落盘前写 `m.Generation = cut.generation` 与 `redispatched`，并发出与 install 同形的 `agent: image cut` 行。回归在 exec hook 边界比对 marker generation == Prepared manifest generation（`TestWatchdogMarkerGenerationMatchesRedispatchedBundle`）；变异 M4/M5 红。drill W 判据**未放宽** |
| CC-2 | concurrency | CONFIRMED | watchdog defer 置 `rollbackInFlight`，随后 commit 成功并停掉 watchdog，flag 永不清 → 此后 ReExecOnly 永远 `upgrade_in_progress` | **采纳，已修**：commit、terminal 报告送达、watchdog 遇非 pending、回滚完成、exec 失败恢复五处清 flag。`TestSuccessfulCommitClearsDeferredRollbackAdmission`；变异 M7/M8/M9/M16 红 |
| CC-3 | concurrency | CONFIRMED | P9 用「当前」pending 表判断 started 是否在途；register 在途时 started ACK 到达会抹掉这一判据，迟到的 reply 删掉唯一一份 exit | **采纳，已修（FIX-COURIER）**：改用 register **请求时**的 receipt。`TestRegisterReceiptRetainsExitWhenStartAckArrivesAfterSnapshot`（真实 NATS ACK 时序，两种 settle 路径各 5 轮）；回退到当前表判定 → 10 次 exercise 全红 |
| CC-4 | concurrency | CONFIRMED | expose replay（`replayPortsFromState`）与 home 推送（`applyHomeDirectives` 的异步 opener）不经 cut 的准入；`SessionCount` 只数已装好的会话，握手中的看不见 → 被判 safe 的升级会关掉新建的 expose | **采纳，已修（FIX-GATE）**：两条路径在拨号前取 `gateExpose` 准入，异步重试与接力 worker 持有到会话装好/尝试结束；关门时跳过并由 broker 重推；`flow_busy` 先读准入再读已装会话（反序会在交接瞬间两边都看不见）。5 个回归测试（真实 TLS/yamux tunnel server），7 条变异红 |
| CC-5 | concurrency | CONFIRMED | ReExecOnly 的 handler 返回即释放 `upgradeInstallMu`，exec goroutine 仍持 cutMu；普通 install 不查 gate → 可在冻结中下载并替换已被选中的可执行文件 | **采纳，已修（FIX-CUT）**：准入（`upgradeInstallMu`）随 `imageCut.releaseAdmission` 一直持有到 exec/abandon/恢复完成；install 入口先查 gate 与 `rollbackInFlight`。`TestInstallRefusesWhileReExecOnlyOwnsFrozenImage`、`TestUpgradeAdmissionSpansInstallRecovery`、`TestInstallRefusesFrozenCutBeforeDownload`；变异 M10/M11/M15 红；锁序（upgradeMu → host flock，cutMu 先于 upgradeMu 释放）未变 |
| CC-V1 | concurrency（核验补报） | CONFIRMED | 已导入、register 成功、commit 落盘失败的进程（pending + ready、flag 未置）过了 deadline，普通 install 抢先拿到 host flock，把「过期 pending」当孤儿，删掉 `.prev` 写新事务 | **采纳，已修（FIX-CUT 续跑）**：`ownsPendingUpgrade`：marker 指向本实例 ∧（本进程持有 boot proof ∨ 持有导入映像）即视为活的持有者，install 拒绝 `upgrade_in_progress`；真正的孤儿过期 marker 保持旧行为。`TestExpiredLiveUpgradeKeepsItsPreviousImageUntilResolution`、`TestExpiredPendingAdmissionRequiresLiveOwnership`；变异 F1–F6 红 |
| DR-2 | drills | CONFIRMED | 34.F 的 agent 在 hangfs 挂载**之前**启动，`exec --cwd` 不带 `--safe`，`Policy.Prepare` 对本地 boot 直接返回 → 根本不起 probe，臂会停在 `_probe_wedged` | **采纳，已修（FIX-DRILLS）**：先挂健康 hangfs，再做最后一次 setup 重启，再起 producer；断言 probe 真的在飞（拒绝原因 + 日志/census 证据）后才升级；绝不在业务开始后重启 agent |
| DR-4 / DRILLS-V1 | drills | CONFIRMED | 混合探针 upgrade 模式没有工作量下限：一次读 + 399 个跳过、全部 exec 被 `agent_upgrading` 拒、拒绝后仍有副作用都判 0；ledger 里未发出的 token 也不对账 | **采纳，已修（FIX-DRILLS）**：全程速率下限、读/写/短三类各自非空成功、至少一个成功写跨越观测到的切换区间、`effect_without_rc0 == 0`、`agent_upgrading` 只允许落在 cut 窗口内、ledger 逐 token 对账。删除原自测里「拒绝伴随副作用也接受」的正例；每条都有负控制 |
| DR-5 | drills | CONFIRMED | P 的窗口是预设的 `upgrade@10`..`end@40`，与真实切换无关（下载/staging 慢时切换可以落在流量之后仍判绿） | **采纳，已修（FIX-DRILLS）**：窗口改为**观测到的** `cut`..`activated`（drill 按 logs.sh 游标看到两条日志即写 `marks.d`，与探针同一单调时基）；探针 `--until-mark activated --tail` 持续发流量直到观测到激活；以探针的启动握手代替固定 `sleep`；记录 reliable/legacy 路径计数（只记录） |
| RS-1 | resources | CONFIRMED | 激活成功后 `handoff.Restored` 里未被取走的文件从不关闭（只有 Abandon 关），辅助 pipe 的写端永远拿不到 EPIPE | **采纳，已修（FIX-RES）**：`Activate` 成功后（同 bundle 回滚已不可能）关闭 remainder 并清引用；held/refused/imported 仍保留供 Repack。`TestImageCompletionClosesUntakenPipeBeforeExit` |
| TD-1 | testsdocs | CONFIRMED（针对审查快照） | 审查快照里没有 T16 的 4 个真二进制测试；变异 M9（`prepared.Exec` → 裸 `realExec`）在组件测试下存活 | **采纳，已在审查期间交付**：E2E 包合入 `test/cli_e2e/node_upgrade_image_test.go` 的 4 个 `TestNodeUpgrade*`（真 ctl/agent 二进制、同 PID、SHA 换代、rc 23、marker、FD 审计），矩阵 `TestReliableExecMatrix` 的 `-run` 与 golden 已加 `TestNodeUpgrade`。**主进程实测 M9**：见 §5（GOFLAGS overlay 进嵌套 build） |

### 2.3 MINOR

| # | lane | 核验 | 内容 | 主进程处置 |
|---|---|---|---|---|
| RB-4 | rollback | CONFIRMED | 回滚 exec 失败、业务已 thaw，marker 仍写 `redispatched` | **采纳，已修**：确认 exec 返回且 thaw 完成后写 `resource_phase=thawed`，Detail 字节不变。`TestWatchdogExecFailureRecordsThawedResources`；变异 M6 红 |
| RS-2 / TD-2 | resources / testsdocs | CONFIRMED | W1b 的 fd 释放 oracle 在进程退出之后才检查，内核已代为清理 | **采纳，已修（FIX-RES）**：真实 tether 在 Begin/Abandon 后被 hold 住，存活期间检查 fd / 父端 EPIPE；去掉 `Abandon` 的 Close 即红 |
| RS-3 | resources | CONFIRMED | candidate `version` 探测只等直接子进程，后台 fork 的后代存活 | **采纳，已修（FIX-RES）**：新增 `agentboot.RunVersionProbe`：经 `/proc/self/exe` 起一个**独立 helper 进程**，只在 helper 里开 `PR_SET_CHILD_SUBREAPER`（agent 本身绝不开，否则 `wait4(-1)` 会吃掉业务 child 的状态），成功/失败/超时都杀整组并回收；私有 mode env 双向剥离。`TestCandidateABIProbeLeavesNoDescendant` |
| RS-4 | resources | CONFIRMED | S3 的 fd 审计没有实例化真实 logrotate 写端，清掉其 CLOEXEC 的变异存活 | **采纳，已修（FIX-RES）**：审计实例化 logrotate、listener、upgrade/data-dir 锁、PTY、活任务；log CLOEXEC 变异红 |
| RESOURCES-V1 | resources（核验补报） | CONFIRMED | `/proc/self/fd` 打不开（EMFILE）时 W1 静默跳过清理 | **采纳，已修（FIX-RES）**：退回到 `fcntl(F_GETFD)` 逐号扫描到 RLIMIT_NOFILE 硬上限；仍失败则在崩溃窗日志里写 `fd_cleanup_error`；关闭前复核 dev/ino（防 fd 号复用，同时覆盖 RB-5 的一半） |
| TD-4 | testsdocs | CONFIRMED | `rollback blocked` 是 Warn，文档承诺 Error | **采纳，已修**：Error 级、限频不变；测试断言 slog level。变异 M12 红 |
| TD-5 | testsdocs | CONFIRMED | 计划承诺的 `TestBudgetConstantsAgreeWithTheirOwners` 不存在，agent deadline/boot budget 与 ctl wait 预算漂移无人察觉 | **采纳，已修（FIX-MISC）**：agent 与 cmd/tether 两侧的预算对账测试，既比数值也解析源码确认是**直接引用**（等值字面量同样红）。20 条变异红 |
| TD-6 | testsdocs | CONFIRMED | `image_abi_unsupported` 的 hint 只描述「无 ABI 行 + 有活任务」，且覆盖掉了「candidate v2 / agent v1」的具体诊断 | **采纳，已修（FIX-MISC）**：hint 对两种原因都成立并保留具体诊断；T17 加 future-ABI 用例 |
| TD-7 | testsdocs | CONFIRMED IN PART | (a) ABI 行读取位置描述不实；(b) 晚到的 ABI 拒绝与 `freeze failed: <class>` 未实现/未记录；(c) converge 日志短语不符；(d) watchdog 路径没有 `image re-dispatched`；(e) preflight「全部命中」**驳回**；(f) usage 缺 `resource_phase` / `rollback_blocked` | **(b)(d) 代码已修（FIX-CUT）**：非 deferred 的 post-flip 失败写 `freeze failed: <class>`（固定词表 `frozen_table/abi_late/bundle/image_cut`，绝不写原始错误文本；`TestCutFailureClassesExcludePrivateErrorText`，变异 F7–F11 红）；`agent: image re-dispatched` 覆盖 boot-budget / watchdog / Run 失败 / 早期 bootstrap 四条路径（M13/M17 红）。**(a)(c)(f) 文档已修（FIX-DOCS）**。(e) 按核验结论驳回 |
| TESTSDOCS-V1 | testsdocs（核验补报） | CONFIRMED | dataplane ④ 门按标识符拼写 `context` 匹配，别名/点导入可逃逸 | **采纳，已修（FIX-MISC）**：按每个文件的 import 路径解析别名与点导入；正负控制在同文件（`TestAgentImageContextImportControls`），因此 `legacy_gates_without_controls` 账本删去该文件、cap 12→11（只减） |

### 2.4 NOTE

| # | 内容 | 主进程处置 |
|---|---|---|
| RB-5 / RS-5 | W1 的 fd 扫描以「fd≥3 且非 CLOEXEC」为所有权依据，不证明 handoff 归属；关闭用旧 fd 号 | **记录 + 部分加固**：启动排他前提（main 第一条语句之前不存在其他继承 fd，socket activation 不受支持）写进代码注释与 §22.6；关闭前复核 dev/ino（RESOURCES-V1 的修复），复用的 fd 号不再被误关 |

## 3. 主进程在合并时自己发现并修的问题

- 34.P 与混合探针的衔接（两个包互相看不到对方）：探针按同目录读 `hot_update_budget.sh` 而 drill 只拷了探针本身；agent 上没有预建 ledger 目录导致每个写请求都失败——合并时修（drill 把两者拷进同一私有目录、P0/P1 前建 ledger 目录）。
- drill 34.N 内联写了 agent panic 流路径（simcluster 日志 oracle 门红）——在 `drills/lib/logs.sh` 增加 `sim_agent_panic_sink_path`，drill 改用它，不扩充豁免账本。
- `cmd/tether` 覆盖门把 `agentimage.DeferredError{Reason:}` 当成 wire 码——在 `unresolvedCodeSites` 登记两条带理由的条目（deferral 原因只出现在 `upgrade_deferred` 的 Error 文本里）。
- 探针对 Go 源码的日志短语钉子要求完整字面量 `"agent: image activated"`，而 activation 行是 `fmt.Sprintf("agent: image activated …")`（为 JSON 格式下的可解析性故意如此）——钉子改为「以该短语开头的 Go 字符串字面量」，改名控制保留。
- E2E 的泄漏测试循环上界用跨包常量，leak-shape 门不可解析——改为函数内 `const rounds = 5`。
- **drill 33 C2 ASSERT-FAIL（首次实跑 2026-10-02 才暴露；五路审查与全部 hermetic 层都没有覆盖）**：plan §2.1 第 3 条的
  「运行映像 == 磁盘二进制」门在**下载前**判定，而 plan §8 同时承诺「33 A/C/C2 不变」——两条自相矛盾。C2 的共享二进制
  sibling `agt1b` 在 A 提交后处于 dst == 候选 ≠ 运行映像，被以 `install_failed: running image differs from the on-disk
  binary` 永久拒绝（每次重试看到的是同一张盘，只剩重启一条路）。修法：不变量改为 **prev ∈ {运行映像, 候选}**——候选逐字节
  相同时，boot 回滚再派发的 bundle 候选已按 `checkCandidateABI` 收过、watchdog 回滚的 bundle 由候选自己重导出，二者都接得住；
  判定移到 `installNewBinary` 的 extract 后、smoke 前（`installPolicy.runningSHA`），下载前只保留「运行映像可哈希」。
  守卫 `TestInstallPrevSlotAcceptsOnlyRollbackCapableImages`（exporter / shared_sibling_candidate / unrelated_disk /
  unhashable_running 四行）；变异 M1 恢复 plan 原文 → sibling 行红、M2 去门 → unrelated 行红、M3 忽略哈希错 → unhashable 行红，
  还原后绿。原 `TestUpgradeCandidateABIProtectsEveryRetainedDomain/disk_mismatch` 断言「未下载」，与新判定位置矛盾，
  该行并入新测试（同一不变量一处守）。plan 正文加「实跑订正」、architecture §22.6 第 6 条同步。重跑见 §7。
- **出口 4 的 race×3 首跑两处红（2026-10-02 收尾时）**：① `internal/agent` ×3 = 877 s，撞 `go test` 默认 10 min 超时——命令问题，
  以 `-timeout 60m` 重跑 rc=0；② `internal/agentimage` `TestBundleRestoreRejectsPartialBundleWithoutInstalling` 的描述符普查
  报「20 vs 21」——**少了**一个描述符，即别人的描述符在本子测试期间被关。根因（Astra 复现为 21 vs 25，描述符身份追踪 + finalizer
  追踪定位）是 S2 的两个测试清场缺陷：`TestSpawnInFlightDefersFreeze` 的 cleanup 从不 join 阻塞中的 Start、也不 Close 它的 runtime，
  泄漏的 pipe 与 replay memfd 之后被 GC finalizer 关掉；`TestFreezeDefersOnForeignProcessRows` 成功的 Freeze 一直处于 exported，
  runtime teardown 只 Discard、不释放进程行的 reap hold 与 pidfd。修：前者 cleanup 释放 Start 并以有界
  `context.WithoutCancel(t.Context())` Close runtime，后者 cleanup 先 Thaw；两者各加 cleanup 后的资源归属与精确描述符计数断言
  （`// origin: docs/testing-standards.md §三 A1`）。变异：去掉 `slowRT.Close` → rc=1（replay 字节残留、22 vs 13）；去掉 Thaw → rc=1
  （行/pidfd 残留、8 vs 7）；还原 → rc=0。`go test -race -count=3 ./internal/agentimage` 修后 7 次 rc=0（含 `GOGC=1 -count=5`）。
  产品代码无改动；原 rejection 普查一字未放宽。

## 4. 审查覆盖到与没覆盖到的

覆盖：boot 序全部出口（Cobra/YAML/identity/New 失败、导入拒绝、thaw 失败、预算耗尽回滚、两跳上界）、cut 路径全部并发窗口（latch、gate、watchdog vs commit vs Run 失败 vs 第二次 install、ReExecOnly）、FD/secret/资源寿命、drill 判据的空转与 Mandate、计划履约与文档对账。
未覆盖（如实）：真实的成功同 PID exec 与 SIGTERM 落在 latch 内（计划 §13.9 已登记）、内核/掉电故障、D-state 文件系统的真实挂起、生产网络故障、穷举锁调度；deploy-tier 只能在 §7 的真实 drill 里证明。

## 5. 处置后的变异收据（主进程复核）

各修复包的变异收据（全部回注原缺陷后变红）：FIX-CUT 17 + 11、FIX-COURIER 1（10 次 exercise 全红）、FIX-GATE 7、FIX-RES 8、FIX-MISC 20、FIX-DRILLS 48，原始收据路径见各包报告（归档于 `s3-review/fix-*`）。主进程另行复核：

| 变异 | 测试 | 结果 |
|---|---|---|
| M9：`reExecInPlace` 用裸 `realExec` 代替 `prepared.Exec`（经 `GOFLAGS=-overlay` 进 e2e 嵌套 build） | `test/cli_e2e TestNodeUpgradeCarriesLiveExecAcrossExec` | **rc=1**（37.8 s）：新映像 COMMITTED 但原 ctl 得 `lease was issued by another runtime instance; task continuity lost`（= H0/S1 签名）——TD-1 的「裸 exec 存活」在合入 T16 后关闭 |

## 6. 处置后的最终候选树收据

见 [hot-update-evidence.md §14](hot-update-evidence.md)。

## 7. deploy-tier drill

见 [hot-update-evidence.md §14](hot-update-evidence.md)（实跑序列、镜像身份、每臂判词与原始证据路径）。
