# workload 迁入的最小兼容核对清单

2026-09-20；只读实际 API/调用链与测试，未修改生产、未运行新实验/测试。本文是接线清单，不是能力 Pass。
范围仅 internal/proc、spawnexec、spawnsafe、pty、agent exec/run/courier 与直接消费者；不重述总体热更新架构。
源码随 root 实现可能移动，引用同时给符号和当时行号。`[ ]` 是迁入时待核对，并非现有测试失败。

## 1. 接线前先固定的产品语义

| 核对 | 现有行为、具体依赖与迁入要求 |
| --- | --- |
| [ ] exec/run 仍各有启动语义 | agent/exec.go:132–142 在 runChild/真实fork前发 started 和 proc.started，失败仍 exit=-1；run.go:275–281 只在真实 Start 成功后发 started，attach/alloc/start失败只发 PtyFailed。不得无意合并，不能把旧 exec started 当真实 PID/started 验收证据 |
| [ ] argv 不经 shell 重写 | proc.NewPID 返回小写 ULID；resolved Path 与原始 Args[0] 分离（exec.go:371–374、pty.go:114–118）。Unicode/元字符和 multicall argv[0]保持 |
| [ ] exec Env 是替换 | exec.go:354–389、:410：空 override 保 Cmd.Env=nil；非空 req.Env替换进程环境；不要套用 PTY mergeChildEnv |
| [ ] run Env 是合并 | run.go:187–217、exec.go:512：从 agent 环境叠加 req.Env，缺 TERM 默认 xterm-256color；safe outage 才用决议环境/cwd，warning只进日志，不能污染raw PTY |
| [ ] nil Env 不等于空 Env | spawnexec/helper.go:42–44、:235–239：nil继承，空slice完全空环境；新的 task spec/JSON 不能 omitempty 抹平 |
| [ ] cwd/PWD和环境清理 | os/exec 在 Env=nil时为Cmd.Dir处理PWD；显式Env不能继承错PWD。保 lineage/routing私有变量不进入业务child；新handoff私有env同理。测试必须走 /usr/bin/env，shell会修正PWD掩盖问题 |
| [ ] 非零/信号退出是结果 | runChild / pty.Session.Wait 非零退出返回 rc,nil；信号死 legacy rc=-1，不变成128+sig或setup error。CLI exec将负rc映射退出128并打印提示（cmd/tether/exec.go:124–138） |
| [ ] 等待超时不是终止 | pty.Wait自身无超时；ctl exec --timeout 是整个等待ctx，默认10min，超时/取消本地返回，不发送远端kill。未来 Manager.Wait(ctx)不能因调用方放弃便杀任务；取消任务必须是显式操作 |
| [ ] 标准流完整性 | exec stdout/stderr分流，4KiB chunk，先读尾部再exit；不承诺两路总序。PTY是raw master，4KiB/50ms刷新；现在exit后最多等200ms泵，新增可靠输出不能沿用该截尾窗口假称完整 |
| [ ] 正常长期运行不吃spawn预算 | defaultRemoteFSSpawnTimeout=30s只约束execve启动，不能给业务Wait套该deadline或500ms freeze预算（agent.go:876–887） |

关键回归：
- `internal/agent/exec_child_env_test.go:27 TestExecChildEnvCarriesTheRequestedCwdAsPWD`，以及 `instance_lineage_test.go`、`routing_nid_adoption_test.go:252 TestExecChildrenNeverInheritTheInstanceLineage`。
- `test/cli_e2e/exec_io_test.go`：TestExecLargeStdoutDelivered、TestExecBothStreamsDeliveredFully、TestExecArbitraryExitCodePropagates、TestExecChildKilledBySignal、TestExecCtlContextCancelReturnsLocally、TestExecPreservesUnicodeAndShellMeta。
- `test/p4/exec_e2e_test.go`：HelloWorld/FalseExitCode/StderrCapture/PsListsExitedProcess；`exec_risk_test.go:82 TestExecRecordsStartedByFingerprint`。

## 2. remote FS 与 spawn lease：不可绕过的具体 API

路径：`Policy.Prepare(argv,cwd,lookupPATH,childEnv,requestedSafe)` → Decision → 安全模式 `spawnexec.Prepare(cmd)` → `Handshake.Start(cmd)` → `RunStartWithCleanup`。

- [ ] **lookupPATH来自agent进程PATH，不能来自子环境PATH**（exec.go:348–350、run.go:200–201）。active路径直接`&exec.Cmd{Path:d.Path,Args:原argv}`，不能再次`exec.Command`触发危险LookPath。
- [ ] 无hangable/off/健康hangable保原env/cwd；outage决议才sanitize/substitute。保 `remote_fs_unhealthy`、unsafe_cwd、not_found、spawn_timeout、too_many_wedged_spawns 的分类和现有Safe参数转发。
- [ ] bounded Start不能变成goroutine包直接target cmd.Start。helper先快速exec本机 `/proc/self/exe`，仅独立进程触碰危险Chdir/execve；这是避免Go STW被D-state exec挂死的现有隔离。
- [ ] helper `init()` 私有分流不能挪成main/TestMain义务；新bootstrap须不干扰helper模式。spec/status用child fd3/4，Ready之后**status CLOEXEC EOF**才是成功证据；不能仅收到Ready就当target已启动。
- [ ] helper直接syscall.Exec成为target、PID不变，禁止第二次fork；target继承stdio，spec pipe立即关，private mode不可递归泄漏（helper.go:202–245）。
- [ ] **两层quota不同**：Policy内wedge计数；helper内UID+abstract-UNIX内核lease全局64个，跨agent重启仍受限。helper在Chdir前取lease，成功target exec用CLOEXEC释放；真正卡住的进程存在期间不能伪释放（helper.go:209–217、:269–294）。
- [ ] 超时严格顺序：invalidateHealthy → onAbandon同步关等待/pipe → late start返回后reap → 最后ReleaseSpawnSlot（spawnsafe.go:1142–1168）。新的Wait owner不能令slot在真正reap前归还。
- [ ] `Handshake.Cancel`只中断spec/status等待，不杀helper/target（helper.go:155–164）；因此 late cleanup仍必须负责kill process group并Wait。
- [ ] `reapAbandonedExecChild`不能因startErr非nil就跳过：Cancel通常使晚返回为error，但child仍存在。只用实际process/reaped状态判定（exec.go:475–487）；PTY成功晚返回用KillAndWaitAfterAbandonedStart。
- [ ] Manager统一wait后要移除/适配helper失败分支和pty关闭中Start分支中的内部cmd.Wait，避免两个reaper争同一个child；先明确所有权再替换，不是只替成功路径。
- [ ] 在途helper/abandoned child仍是资源，不能因为尚未registerExecChild就漏出清单；不可导出时保业务defer，不能清表后再接纳64个新wedges。

必保测试：
- `internal/spawnexec/helper_test.go`全部6项：StartsTargetAndPreservesOutputAndExit、ReportsTargetStartFailure、DoesNotLeakPrivateModeIntoTarget、ExecsTargetInPlace、PreservesExplicitEmptyEnvironment、GlobalWedgeCeilingSurvivesProcessLifetimeBoundaries。
- `internal/agent/spawnexec_test.go:9 TestRunChildActivePathUsesSpawnHelper`；注意此测试`runChild(nil,"",...)`合法，空reply应不触发NATS访问。
- `internal/agent/remotefs_test.go`：BuildExecCmd inert/active/healthy/PWD/off、StartBounded_abandonClosesPipesAndReaps。
- `internal/spawnsafe/spawnsafe_test.go`：RunStart_abandonsAndCeiling、RunStartWithCleanup_reapsSuccessfulLateStart、RunStartWithCleanup_reapsBeforeReleasingTheWedgeSlot、Prepare_resolvesAgainstLookupPATHNotChildEnv，以及stale health/TTL/ceiling正反例。
- `internal/agent/session_generation_test.go:358 TestAnAbandonedExecChildIsKilledNotJustWaitedOn`。

## 3. PTY、Kill、Wait、attach 与连接生命周期

- [ ] Allocate与Start分离；订阅attach并Flush先于ready，收到attach才启动child；默认attach窗口15s，可由TETHER_AGENT_ATTACH_DEADLINE覆盖。超时/JSON错误关闭未启动PTY，不生成proc.started（run.go:136–178、:348–370）。
- [ ] agent返回初始size，合法attach size再应用；PTY Setsid+Setctty，exec Setpgid；Signal向负pgid整组发送，不能只杀shell leader（pty.go:139–142、:267–282；exec.go:394–403）。
- [ ] 当前kill默认SIGINT，missing PID→pid_required、未知→pid_unknown、失败→signal_failed。当前lookupProc只覆盖PTY；扩至exec是显式功能补齐，不可假称旧路径已有（run.go:469–491）。
- [ ] `Close`幂等、Start二次拒绝；Start取走slave所有权，不持mutex做危险Start；并发Close后晚成功须kill整组并唯一Wait（pty.go:93–191）。
- [ ] `Hangup`是SIGHUP+master关闭，**不升级SIGKILL**；HUP-immune child继续运行并仍RUNNING。不要与显式Kill或evict共用同一种stop策略（pty.go:315–344）。
- [ ] `WriteInput`取指针后释放锁再Write；写满PTY不能占住manager全局锁，让resize/cancel/freeze都卡死。SetSize非法尺寸回80x24。
- [ ] Wait返回立即设置waitDone/reaped身份屏障，之后不能对可能复用pgid发reaper信号。新sole-wait替代Cmd.Wait时保rc=-1、rusage/exit-pending、tail ownership。
- [ ] session重建不能Hangup活PTY；`.in/.resize/.ka`是每NATS session的wildcard intake，按活PID路由；输出每flush取当前nc。
- [ ] 新node-scoped `.in/.resize`与legacy session-scoped双发布：legacy带`PtyNodeHeader`必须丢弃，否则每次键盘输入两遍（run.go:875–940）。新manager不得重复注册每task订阅。
- [ ] KAIntervalMS=0的旧ctl永不reap；其余interval夹[1s,60s]，grace=max(6×interval,3min)。仅超过grace且本tickPONG证明conn健康、连续第二strike、未waitDone才Hangup。
- [ ] 断链、probe失败、session重建会restamp并清第二strike；这是既有“agent失聪不罚ctl”策略，区别于安全授权绝对deadline，不能把二者混成同一不准重置规则（run.go:675–752、:760–817、:982–1015）。

必保测试：
- `internal/pty/pty_test.go`：StartCapturesEcho、StartResolvedPathUsesIsolatedHelper、SignalDeliversSIGINTToProcessGroup、StartTwiceRejected、Session_concurrentStartClose_noRace、Session_closeBeforeStart、KillAndWaitAfterAbandonedStart_reapsProcessGroup、Hangup三项、WriteInput两项。
- `test/p5/run_e2e_test.go`：RunHelloWorld、RunExitCodePropagates、AttachTimeout、KillSendsSIGINT、RunRejectedForDeletingSession、RunRejectedForMissingNode；`run_risk_test.go:13 TestRunRecordsProcessLifecycleForPs`。
- `test/cli_e2e/run_modes_test.go`：RunResizePropagatesViaSIGWINCH、RunFastExitNoAttachTimeout、RunNonexistentCommandFails。
- `internal/agent/ctl_liveness_test.go`4项table/probe/Hangup；`session_generation_test.go:285 TestPtyIntakeIsSessionScopedNotPerRun`、:311 TestExecChunksRideTheCurrentConnection。

## 4. courier、proc DB、lease rename 与 G.1 的兼容缝

- [ ] 保StartedByFP、Argv、原started/ended时间、logical PID、boot/startticks；payload描述事件发生时刻，不是回放时刻（proc_delivery.go:108–132）。
- [ ] 每PID started先于exit；started永不因3次timeout而park，只有exit可park给register replay；N-1无reply仍是旧publish语义，不能导致无限重复审计。
- [ ] positive ACK才settle；negative ACK/store/leader错误重试。register仅接受真正处理过snapshot的应答，Lease争议回复绝不清退出队列（proc_delivery.go:261–315、:374–404）。
- [ ] register清理：started出现在AcceptedProcesses才确认；exit不在AcceptedProcesses才确认，不能要求必须在ReconciledProcesses。保broker reconcile失败不伪成功的处理。
- [ ] `drainStarted`先于register，当前预算为一次5s；迁入新5s端到端路径需重新设计预算，不能叠加一次5s启动drain后还称请求≤5s。
- [ ] 联合running/exit-pending snapshot必须消除unregister→enqueueExit暂时缺席；当前source明确承认该gap（proc_delivery.go:119–123）。加普通exec进G.1时还要处理旧exec started bootID=""/ticks=0，不能与新真实triple产生误判。
- [ ] proc.MarkExited/PlanMarkExited是**sid+pid**安全围栏，不加会随lease改变的nid；empty sid和foreign sid拒绝，readback也按sid。Refile返回bool必须是真RowsAffected，不能以nil error当采用证明。
- [ ] 旧courier上限4096、24h回收、优先淘汰started，是当前有界策略；新完整可靠任务表不可直接沿用掉事件策略，需要显式背压/水位/新版测试，不能删旧测试后无替代。

必保测试：
- `internal/agent/proc_delivery_test.go`全组（current conn/order/old broker park/started never park/TTL/register clearance/snapshot）；contested register的`instance_lease_reconnect_test.go`与`session_contested_reply_test.go`。
- `internal/broker/proc_event_ack_test.go`全组，包括TestRegisterReconcileFailureCannotLookSettledToExitCourier、TestRegisterReconcileReadFailureRetainsExitAndIssuesNoDirectives、foreign PID indistinguishability。
- `internal/proc/exit_session_fence_test.go`4项两writer/empty sid；`proc_test.go:522 TestRefileReportsWhetherARowActuallyMoved`；`reconcile_replay_test.go`审计顺序/内容。
- 注意`proc.go:188–198`还留着“PlanMarkExited empty-sid escape hatch”过时注释；真实plan.go:80–98和拒绝测试已经禁止，不能照注释恢复漏洞。

## 5. teardown 与 session destroy：一项真实既有需求缺口

**evict 已有明确行为**：agent.go:1153–1164只接受匹配SID/NID的agent_evicted，先reapManagedChildren再cancelRun；run.go:554–575对exec整组SIGKILL（必要时pid fallback）、PTY整组SIGKILL。
- [ ] 保`test/p9/admin_e2e_test.go:548 TestAdminEvictReapsManagedExecChild`、:494 TestAdminEvictTriggersAgentShutdown；不能让manager独立生命周期漏掉真实驱逐清场，nohup无cgroup尤其依赖它。
- [ ] 普通carrier rebuild与evict分开；现agent Run取消并不等于kill全部业务，不能在公共defer盲加manager.Close(Kill)。`TestAgentDispatchAfterRunCancel`和既有重复Run泄漏门仍须通过。

**session rm需求/实现不一致（源码确认，未运行现场实验）**：
- requirements.md:597要求各agent kill全部托管进程。
- broker/sessions.go:202–245只tombstone并finalize；audit.go:87–108删除history/xfer/DB、ForgetSession并发布session_destroyed。
- agent.go:1153明确忽略非agent_evicted；全internal搜索未发现session_destroyed消费者，不能把现有删除事件误当已接管资源终止。
- `test/p7/audit_e2e_test.go:229 TestSessionRmDeletesStreamAndCascades`覆盖存储级联/事件，未证明真实exec/PTY子树被终止。
- [ ] 接线时显式记录该待补能力，session authority/tombstone不能被导入旧manager状态复活；新增长exec+PTY+后代+删除与freeze竞争的真实回归。不能只把DB行删掉便报告清场。
- [ ] 仍保DELETING拒新exec/run、resume删除、先history/xfer再DB的幂等次序：TestExecOnDeletingSessionRejected、TestRunRejectedForDeletingSession、TestDeletingSessionResumesOnBrokerRestart。

## 6. 会因搬代码变红的硬门：迁门，不删责任

| 门 | 具体耦合与处理 |
| --- | --- |
| TestRemoteFSSafeSpawnUsesProcessIsolation | test/architecture/spawn_exec_isolation_test.go硬读agent/exec.go、pty/pty.go中的Prepare/Start/Cancel及helper init/in-place exec/lease；迁入workload须改真实路径和对应反例，不能删字符串后放空 |
| TestSpawnTimeoutMintSitesNoteEvidence | spawn_stall_evidence_test.go精确4个spawnsafe mint site，timeout须invalidate、ceiling不得invalidate，且不可持policy.mu；复写watchdog会新增站点/丢证据 |
| wire/error门 | 保ExecChunk/RunChunk/KillResp、legacy subject/字段和reason零值兼容；错误reason变量化会触发cmd/tether的wire-code静态覆盖门，不能用豁免掩盖丢分类 |
| structural预算 | Agent方法数已钉，源码数处特意用package函数；资源职责迁出可降Agent负担，但不能为过门删除历史承重注释 |
| 测试矩阵/泄漏 | 新workload若import pty，必须整包-race矩阵、直接调用共享leak helper且≥5真实exercise、FD/goroutine基线；新test登记inventory和anchor，由root统一整合 |
| helper独占Wait | grep所有cmd.Wait/Session.Wait/Process.Wait和abandoned-start回收位，适配sole owner时保每个失败路径对应测试，不能只改正常runChild |

## 7. 最小迁移顺序与完成核对

1. [ ] 先固定task spec/env/stdio/started协议差异；保持buildExecCmd与spawnsafe决策为唯一策略入口。
2. [ ] 统一活exec/PTY/starting/abandoned/exit-pending ownership；先接Signal/Wait/Close与helper失败回收，再切handlers。
3. [ ] 保原NATS/PTY adapter和courier协议，联合snapshot消gap；处理lease/triple后再开放新代恢复。
4. [ ] 每次小迁移跑直接相关单包/定向真实child测试；不以只编译作为行为验证，不在本轮建议重跑重型全矩阵。
5. [ ] 上述baseline行为通过后，才接exec/handoff；新continuity/reliable ctl/drill要求仍另验，当前清单未声称这些已实现。

已向root先行报告：helper隔离/内核lease、env差异、started早于fork、Wait超时不杀、session_destroyed未消费的需求缺口。
