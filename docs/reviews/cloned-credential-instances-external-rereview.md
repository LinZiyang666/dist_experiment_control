# Fail - 克隆凭据实例增量外部复审

结论：**Fail**。主进程的整改解决了上轮多项问题，但“全部 17 项已处理、5 Blocker 与 9 High 已修”的回复不能成立。复审确认 1 个 Blocker、4 个 High 仍未闭合，其中 3 个有新增独立红测：terminal lease refusal 会永久保留 contested subscription；cluster farewell 仍不释放 ONLINE lease row；新增的 `ConfiguredNID` 可把租约分配重定向到另一个 credential family。共享二进制 upgrade ownership 仍被代码明确承认为 gap，而 drill 84 没有构造共享 binary/marker；F8 所称 typed error 也没有出现在源码中。

## 复审范围与独立性

- 复核 `docs/reviews/cloned-credential-instances-external-review.md` 末尾主进程对 F1-F17 的逐条回复，以及本轮 44 个文件约 `+1126/-304` 的未暂存整改。
- 不以回复中的 `make test/e2e/gates` 结果为证据；逐项回到生产路径、测试 oracle 和 sim topology 核验。
- 外审没有修改产品实现，只强化/新增 3 个独立守护测试并写本报告。

## Findings

### R1 - Blocker - terminal refusal 仍保留旧订阅；退避发生在 teardown 之前，F3/F17 实际互相抵消

`onNATSReconnect` 收到 unusable lease 后先递增 refusal、计算退避（`internal/agent/proxy.go:695-723`）。前 4 次在调用 `requestLeaseRebuild` **之前**等待 2/4/8/16 秒（`:734-741`）；第 5 次 `giveUp` 直接 return（`:724-733`），根本不 teardown。

nats.go 在 reconnect callback 前已经 replay 旧 forwarded subscription。于是前 4 次等待期间 clone 仍会接收 incumbent 的命令；第 5 次则永久保留 contested connection。这正是 F3 的双执行 Blocker，只是从“所有拒绝永久保留”变成“第 5 次永久保留”。

我把原外审 guard 强化到 terminal 分支：`TestReconnectLeaseRefusalRetiresTheContestedSession` 稳定失败，1 秒内 session 未 cancel。`-race` 下同样失败，无 race detector 噪声。

F17 也没有覆盖初始 session：`applyLeaseVerdict` 仍只等待 `RegisterRetryInitial` 后 rebuild（`internal/agent/instance.go:356-385`），完全不读取 `leaseRefusals`、`leaseRefusalBackoff` 或 `maxLeaseRefusals`。初始 suffix exhaustion 仍可无限快速循环。

建议：收到非 nil lease 后先同步 latch rebuild 并立即退役当前 session；退避必须发生在旧 connection 已不可服务之后。terminal 状态也必须 close/cancel，不能用“停止竞争”保留被判无所有权的订阅。初始与 reconnect 两条 register 路径共用同一 refusal policy/state machine。

### R2 - High - F11 只修单机；cluster 仍返回成功并留下 ONLINE row

回复称“可信 farewell 现在原子地把该租约行置 OFFLINE”，但 `markNodeOfflineOnRelease` 在 cluster mode 只写日志并返回 nil（`internal/broker/clusterwrite.go:1309-1318`）。其注释也明确说仍等待普通 OfflineAfter，和回复相反。

独立测试 `TestClusterReleaseDoesNotLeaveTheLeaseRowOnline` 得到：`cluster release returned success but left the lease row ONLINE`。allocator 因而继续把 `gpu1-02` 当占用，重启得到 `gpu1-03`，原 F11 的 ghost/suffix drift 在 cluster 中完整保留。

建议：复用已有 replicated liveness/status op，或新增被旧版本 capability-gate 的 raft write；不能以 nil 表示已完成。测试必须覆盖真实 cluster writer/FSM，而不是当前只覆盖 single-mode fixture。

### R3 - High - F7 没有修复；drill 84 没有共享 binary/marker，且实际 verdict 是 INCOMPLETE

生产代码仍允许 sibling 获取同一个 boot proof：任何进程在 shared staged binary 上运行 `BootUpgradeCheck` 都从 `bootContinuePending` 返回 marker `UpgradeID`（`internal/agent/upgrade_state.go:430-491`）；`markerTargetsThisAgent` 又以该 proof 绕过不同 `TargetInstance`（`:566-587`）。代码自己承认 sibling 可到达，`upgrade_state.go:479-483` 却还保留“sibling cannot reach”这一相反注释。

drill 84 只把一个 volume 挂到 `/home/sim/.tether`（`test/simcluster/lib/docker.sh:93-113`、drill `:24`），agent binary/marker 路径是 `/home/sim/.local/bin`（drill `:72,146`），仍是两个容器各自的 inode。它只 `stat` `.tether` 目录，没有断言 binary 或 marker 同 inode，因此不能作为 shared-binary ownership oracle。

独立运行当前镜像得到 17 条行为 PASS，但 S6 是 `NOT-COVERED[gap]`，最终为 `DRILL-VERDICT verdict=INCOMPLETE rc=4`，不是回复顶部容易让人理解的 pass。`expected-verdicts.tsv` 也把它登记为 INCOMPLETE。

建议：使用现有 `upgrade-success-drill-plan.md` 描述的正确拓扑——同一容器两个 unit，独立 agent state/credential，但 `ExecStart` 指向同一 binary inode——实际触发 pending marker，并断言 sibling 不得 commit/rollback/消耗预算。在闭合前，至少禁止 shared-binary clone 上的显式 leased-instance upgrade；不能同时在 usage 中推荐显式升级又把 ownership 留作 High gap。

### R4 - High - F8 回复称返回 typed error，源码仍原样返回 nil

回复表写“cluster 模式返回 typed error 而非 nil”，但 `refileProc` 在 `b.clusterMode` 时仍 log 后 `return nil`（`internal/broker/clusterwrite.go:1290-1296`）。调用方因此递增 `adopted` 并从 agent snapshot 删除 PID，仿佛行已经迁移（`internal/broker/reconcile.go:484-505`）。文档虽新增“ps 可能看不到”的限制，代码和回复仍不是同一契约。

除了显示失真，离线期间积压的 exit 若依赖 register reconciliation，old-nid RUNNING row 不会进入 new-nid 的主循环；agent 的 exited entry 又不进 orphan loop，存在 courier 结算而 DB row 长期 RUNNING 的风险。

建议：真正返回 typed error 并让调用方不要报告 adopted，或实现 raft/FSM refile。若选择“cluster 不支持迁移”，需有 pending-exit/re-register 测试证明不会丢退出状态，而不是只在 usage 中降低 `ps` 预期。

### R5 - High - `ConfiguredNID` 是未验证的网络输入，可跨 credential family 占用租约空间

F2 的 honest-agent 修法正确保留了 `gpu-02` 这个 opaque root，但 broker 直接信任 `req.ConfiguredNID`：`configuredBasename` 非空即返回（`internal/broker/broker.go:2663-2676`）。`handleRegister` 只验证 body NID 等于 subject NID（`:2187-2207`），没有验证 ConfiguredNID 合法，也没有验证 presented nid 是它本身或它的直接 lease。

结果是一个只获准以 `gpu1` 注册的 agent，可以在 contested request 中声称 `ConfiguredNID=victim`。`assignLeaseName` 会发 `victim-02` 并在 `leaseHolder` 留 offer（`:2054-2104`）；重复请求可占用 `victim-02`、`victim-03`……直至真实 victim clone 被临时拒绝。

独立测试 `TestRegisterCannotAllocateAnotherCredentialFamily` 实际得到 `AssignedNID:victim-02 Basename:victim`。

建议：在任何 lookup/offer 前验证 ConfiguredNID 满足 `ValidateNID`，并要求 `nid == ConfiguredNID` 或 `nid` 是 `ConfiguredNID` 的直接 lease；违反者返回稳定错误且不得写 holder/probe cache。还应验证该 root 的 provisioning fingerprint 与当前连接 credential 一致，不能把 agent 自报字段当 authority。

## 已确认闭合的上轮事项

- F1：撤销 0019/`nodes.leased`，避免 same-proto rolling migration；direct/FSM guard 通过。
- F2 的 honest path：配置名 `gpu-02` 的 clone 得到 `gpu-02-02`；但需补 R5 的 trust-boundary 校验。
- F4/F5：contested reply 不再提交 marker，co-located configured nid 被豁免；focused guards 通过。
- F6：nid generation fence 与 down edge 已加入，相关测试通过；Close 的长期回收仍建议后续收敛，但不单独阻断本轮。
- F9/F10/F12/F13：PIN/bootstrap 顺序、lease eviction 拒绝、proxy/node 同源分类、CLI/文档可见性已按回复落地。
- F14 的 framing 半项：drill 83/84 均通过 lint；shared-binary fidelity 见 R3。
- F15/F16：reviewer-demo 清理与结构预算债务说明已补。

## 独立验证

| 命令 | 结果 | 说明 |
|---|---:|---|
| 整改自带 focused regression set | rc=0 | F1/F2/F4/F5/F6/F10/F11(single mode) 均通过。 |
| 3 个复审 guard | rc=1 | terminal refusal、cluster release、cross-family ConfiguredNID 全部稳定失败。 |
| 两个关键 guard 的 `go test -race` | rc=1 | 两个业务断言失败，无 race report。 |
| `make test` | rc=2 | 除当时已加入的两个复审 guard 外，其余 package 通过；第三个 R5 guard 随后单独验证失败。 |
| `make gates` | rc=0 | vet、Darwin build、architecture/determinism/cmd/auth/concurrency/proto 与 lint 通过。 |
| `bash test/simcluster/tests/lint-drills.sh --all` | rc=0 | 43 个 contract-enforced drill 无 framing 违规。 |
| `./local.sh --build drill 84-shared-home-instances` | rc=4, INCOMPLETE | 17 pass、0 assertion fail、1 NOT-COVERED gap；未共享 binary/marker。 |
| `git diff --check`、83/84 `bash -n` | rc=0 | 通过。 |

未重复运行 `make e2e-parallel`：`make test` 已有确定性 Blocker/High 红测，完整 E2E 只会重复收集同一 package failure；主进程声称的旧测试全绿已由 focused set 与其余 package 结果支持，但不能覆盖上述新分支。

## 复审结论

不能通过。最低放行条件是 R1/R2/R5 三个红测转绿，F8 回复与实际代码契约一致，并对 R3 选择“真正修复”或“明确禁止该部署下显式 lease upgrade”之一；单纯保留 `NOT-COVERED` 不能同时宣称 High 已修。
