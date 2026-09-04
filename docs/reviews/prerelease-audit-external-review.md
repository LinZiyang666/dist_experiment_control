Pass

# 发布前全库审计 · 外部审查报告

> 日期：2026-09-03。审查者：Codex 外部审查者。审查对象为 `HEAD=021c970` 之上的 index 候选：初始状态 200 个文件、`+17154/-719`，初始无额外 unstaged diff。审查中另有 6 个 simcluster 文件在工作区并发写入（`lib/tether.sh`、`simcluster`、drill 00/30/60/80），也已追加纳入代码审查与复跑边界。
> 输入索引：`docs/reviews/prerelease-audit-plan.md` 与 `docs/reviews/prerelease-audit-review.md`。两者只用于重建范围和裁决历史，本文没有继承其中的 Pass/“已修”结论。
> 方法：按 `CLAUDE.md` 的 WHAT/HOW 权威链重建需求映射，逐面阅读 auth、cluster/FSM、session admission、proc、transfer、install、wire/N-1 与测试资产；再以四个独立失败测试、全量基线、race、lint 和 simcluster hermetic 门交叉验证。

---

## 0. 结论

**发布判定：Fail。** 当前候选至少有两项 Blocker、四项 Major；其中任一 Blocker 都足以否决发布：

1. `_TINBOX` 只保护升级后的回复。任何匿名合法 nkey 仍可自报 legacy 并获得 `Sub _INBOX.>`，窃听尚未升级 agent 的 register reply；其中 tunnel token 与全部 subscriber PSK 没有轮换机制，泄漏不会随滚动升级结束。需求文档自己承认该长期凭据泄漏并把真正修复推到后续 release，却同时声明“本 release 没有 N-1 例外”，与本轮“不得把问题推到以后版本”的裁决正面冲突。
2. N-1 broker 转发的 `proc.exit` 没有 SID。新 leader 把缺失 SID 当作可信 peer 的兼容豁免，按 PID 无范围更新；但旧 broker 的输入仍来自 agent，PID 可由 agent 指定。旧 broker 因而成为 confused deputy，可替一个 session 把另一 session 的运行中进程标为 EXITED。独立生产路径测试稳定复现。

此外，cluster 模式绕过进程级 Argon2 ceiling；正常 followers-first/leader-last 滚动升级会让一次性 session creator seed 在所有节点上都被跳过；single-broker transfer 的 staged terminal 恢复被代码明确保留为 OPEN；安装测试根目录仍能操作真实宿主机 systemd。这些都不是“补一行文档即可放行”的收尾问题。

**必须同时写明的阴性结论**：本批工作并非整体失效。nonce signature、现代 inbox 独立根、tunnel REGISTER 上界与 fencing、cluster/FSM 多数 fail-closed 路径、离线文件安全、CLI exit taxonomy、配置 preserve，以及大量测试/结构门的实现经抽样和全量基线没有发现新的系统性问题。排除本文四个刻意失败的 reviewer guard 后，全仓普通 Go 测试包均通过；lint 与 simcluster hermetic 自检也通过。Fail 集中在 rollout/security/recovery 的几条跨组件组合路径，而恰好是单包绿色最难覆盖的部分。

---

## 1. Blocker

### B-1 — legacy inbox 仍允许匿名窃取长期 tunnel 凭据，且升级后不失效

- **严重度**：Blocker（认证边界、长期凭据外泄）· **确定度**：已确认（实现与 requirements 自证，且候选已有真实 NATS 四象限测试证明 legacy grant 确实工作）
- **文件**：`internal/authcallout/handler.go:234-253`；`internal/auth/permissions.go:130-148,486-509`；`docs/requirements.md:344-374`；register reply 的 secret 形态见 `internal/auth/permissions.go:17-23`。
- **机理**：控制面允许任何能证明自己持有合法 user nkey 的未激活客户端连接。是否支持 modern inbox 只看客户端自报的 CONNECT username；不报 marker 即 `legacyInbox=true`，随后未激活模板也获得完整 `Sub _INBOX.>`。独立 `_TINBOX` 根确实阻止 legacy grant 读取现代回复，但滚动窗口内旧 ctl/agent 仍把回复放在共享 `_INBOX`。因此攻击者无需 session 成员资格或既有凭据，只需新建一个 nkey、声称 legacy 并订阅 `_INBOX.>`。
- **为何不是“暴露只持续到客户端升级”**：旧 agent 的 register reply 含 tunnel token 和全部 subscriber PSK。requirements `:367-374` 明确承认这些是长寿凭据、全仓没有轮换，且已泄漏凭据在客户端升级后仍有效。删除下一 release 的 legacy grant 只能阻止继续窃听，不能撤销已经拿走的 secret。
- **与本轮裁决冲突**：同一节 `:344` 写“本 release 没有 N-1 例外”，`:373-374` 却把真正安全动作登记为尚未实现的后续项；审计 plan 又明确禁止把问题推到以后版本。内审把主题空间隔离当作 B1 已闭合，实际只闭合了 modern/modern 象限。
- **建议**：不能再以 marker 自报维持安全结论。若本 release 必须彻底关闭该缺陷，应把它做成明确的协议/发布纪元切换，拒绝共享 inbox 客户端，并提供重装/回滚路径；若产品坚持 N-1 服务，则至少必须在窗口收口时自动轮换全部 tunnel token/PSK、使旧值失效并提供可验证的 fleet 收敛门。但后者仍不能保护窗口内流过的普通 reply 内容，必须在 release 契约里作为真实安全例外裁决，不能写成“没有例外”。

### B-2 — N-1 `proc.exit` 空 SID 豁免重新打开跨 session 写入

- **严重度**：Blocker（跨租户进程状态写入，可诱发错误 reconcile/kill）· **确定度**：已确认（真实 leader dispatch + FSM 写入的独立失败测试）
- **文件**：`internal/proc/plan.go:69-106`；`internal/broker/cluster_forward.go` 的 `VerbProcMarkExited` dispatch；独立测试 `internal/broker/clusterwrite_test.go` 的 `TestLegacyForwardedProcExitCannotBypassTheSessionFence`。
- **机理**：`PlanMarkExited` 对非空 SID 同时约束存在性查询和 UPDATE，但 `sid==""` 时刻意恢复旧的 `WHERE pid=?`。注释把安全性建立在“只有 mTLS/system account 内的 peer 能传空 SID”上；这遗漏了数据来源：N-1 broker 收到 agent 在自己 session subject 上发布的 exit，agent 仍可选择 payload 中的 PID，然后旧 broker 丢失 subject SID、以合法 peer 身份转发旧 payload。新 leader无法区分这个 confused-deputy 请求与可信无范围写入。
- **独立证据**：测试创建 victim session/node/RUNNING process，向真实 `dispatchForward` 送入 N-1 形态（无 SID）的 `ProcMarkExitedPayload`。当前实现返回 nil 并把 victim 行改成 `EXITED`；测试以 `legacy payload crossed the session fence` 稳定失败。
- **建议**：新 leader 必须拒绝空 SID，不能从 transport 身份推导业务授权。旧版本兼容若必须保留，需要旧 peer 传递原始 subject 中可验证的 SID；既然旧 binary 做不到，安全选项是对该 verb 结束 N-1 兼容（显式 proto/release 门），或拒绝 legacy exit 并依靠注册/reconcile 恢复状态。不得保留无范围 UPDATE。

---

## 2. Major

### M-1 — cluster PIN 验证绕过进程级 Argon2 ceiling

- **严重度**：Major（公网认证面 CPU/serialized callout DoS）· **确定度**：已确认（四个生产接线点 + AST guard）
- **文件**：`internal/authcallout/handler.go:438-469,523-544,727-748`；`internal/broker/authcallout.go:116-129`；`internal/broker/cluster_forward.go:663-669,830-870`；独立测试 `internal/broker/authcallout_wiring_test.go` 的 `TestClusterPINPlansDoNotBypassTheArgon2Budget`。
- **机理**：handler 在验证前检查本进程的 global/per-IP limiter，真正的 global charge 只发生在 `h.chargedVerifyPIN`。单机 fallback 正确把该 verifier 传给 `ProvisionWithPIN`/`JoinWithPIN`；cluster 模式却安装 `NewProvisionSeam`/`NewJoinSeam`，leader-local 和 forwarded dispatch 的四个 Plan 调用全部直接传 `auth.VerifyPIN`。于是发起请求的 follower 检查但不消费 global budget，实际执行 Argon2 的 leader既不检查也不消费它。分散 IP/多个入口可持续把昂贵验证压到 leader，注释 `handler.go:743-745` 所称“在 leader 上 charge”并未发生。
- **独立证据**：AST guard 枚举所有 `PlanJoinWithPIN`/`PlanProvisionWithPIN` 的 raw verifier，当前稳定报出 `cluster_forward.go:665,668,838,870` 四处并失败。
- **建议**：让 cluster seam 显式接收并使用由 leader auth handler 持有的 budgeted verifier，或把 process-wide limiter 移到所有 Plan 路径共享的 broker 组件；补 leader-local 与 follower-forward 两条行为测试，断言第 N+1 次请求在 Argon2 前被拒且不存在 session 的请求不消费 budget。

### M-2 — 一次性 session creator seed 会被标准滚动升级时序全部跳过

- **严重度**：Major（升级后现有 owner 无法创建 session，且错误永久化到下次 leader 重启）· **确定度**：已确认（启动/选主/升级时序组合证明）
- **文件**：`internal/broker/clusterwrite.go:1379-1443`；`internal/broker/broker.go:1480-1494`；`internal/clusterupgrade/plan.go:114-177`；`docs/distributed-broker-architecture.md:164-169`。
- **机理**：migration 0019 只建空表。seed 只在 `Broker.Run` 启动阶段调用一次；cluster follower 在 `clusterwrite.go:1421-1422` 直接返回，错误仅 WARN 后继续 ready，既没有 leadership-acquired hook，也没有周期 retry。标准升级恰好是 followers-first/leader-last：所有新 follower 重启时旧 leader 仍在，因而全部跳过；最后一步先把 leadership 转给已经运行的新 follower，再重启旧 leader，旧 leader回来时也只是 follower，再次跳过。最终 marker 与 allow rows 都不存在，而且没有任何节点会自动重试。
- **附加窗口**：即使最终补一次 leadership hook，混版 capability gate 提交 seed 之前，新 follower 已开始执行空表 admission，会随机拒绝既有 owner 的 `session create`。因此“升级零动作”还需要 marker 未提交期间的明确兼容语义，而不只是 eventually retry。
- **追加 simcluster 变更没有覆盖该缺陷**：审查中写入的 `admit_creator` helper 与四个 call site 对 fresh-broker fixture 是合理修复，shell/lint/hermetic 门也全绿；但 drill 30 先由 `$SIM session` 显式 allow 主身份，又对 isolated write-probe 身份显式 `session-allow`。它验证的是“已 allow 身份在 roll 中持续写”，没有构造“旧版已有 owner、升级时依靠自动 seed”的状态；staged `tether-next` 也只是同一代码的 next-version 构建，不是缺少 op/migration 的真实 N-1 binary。因此不能用该 drill 的绿色反驳本 finding。
- **建议**：把 seed 做成 leader-acquired/周期 reconciliation，直到 marker 与数据经 raft 同条提交；并在 marker 未完成时 fail-safe 地 grandfather 本地已有 session owner（只允许已有 owner，不允许任意新身份），待 replicated marker 生效后切换到 allow table。增加三 voter followers-first/transfer-leader/leader-last 的真实时序测试以及一次瞬态 Propose 失败后的无重启自愈测试。

### M-3 — single-broker staged terminal 恢复被明确保留为永久 OPEN

- **严重度**：Major（transfer 审计缺 terminal、恢复 ledger 永久累积）· **确定度**：已确认（代码显式分支和内审 OPEN 台账）
- **文件**：`internal/broker/xfer_inflight.go:482-559,936-979`；正常 terminal staging/publish 路径见 `internal/broker/transfer.go`；`docs/reviews/prerelease-audit-review.md` 的 F-T1。
- **机理**：single broker 现在也启用 durable transfer ledger。若进程在“terminal 已 stage、尚未 publish/unlink”窗口崩溃，重启会进入 `replayStagedTerminal`；`b.cl==nil` 分支明确 WARN、永久 retain、返回 false。正常 periodic finalize 仍走同一分支，故 audit stream 可永久只剩 start。代码注释 `:539-555` 与内审都把它登记为 OPEN。
- **为什么已有 synthetic 修复不能替代**：`commitSyntheticTransferTerminal` 的 single-mode 分支已经使用 content-derived `Nats-Msg-Id`，但正常 terminal publish 没有同一 dedup id。直接 replay 有 post-publish/pre-unlink 重复窗口，所以当前选择“不恢复”；这证明需要统一 publish identity，而不是证明可以发布。
- **建议**：正常 single-mode terminal 首发和 recovery replay 必须使用同一 content-derived `Nats-Msg-Id`，成功后再 unlink；测试分别 SIGKILL/故障注入在 stage-before-publish 与 publish-before-unlink 两个窗口，要求最终恰好一个真实 terminal 且 ledger 清空。

### M-4 — `TETHER_INSTALL_ROOT` 在 systemd 主机上仍可修改宿主服务状态

- **严重度**：Major（测试隔离/宿主机运维状态破坏）· **确定度**：已确认（脚本控制流直达真实命令）
- **文件**：`scripts/install.sh:664-693,803-826,1264-1320`；现有 real redirected install helper 固定附加 `--no-enable`；独立测试 `test/p10/config_preservation_test.go` 的 `TestRedirectedBrokerLifecycleDoesNotTargetHostSystemd`。
- **机理**：脚本把 `TETHER_INSTALL_ROOT` 描述为把 system paths 全部重定向、使 whole broker install hermetic。文件路径确实被改到临时根，但 `enable_broker_units` 仍只检查真实 `/run/systemd/system`，随后调用真实 `systemctl daemon-reload/enable`；`uninstall_broker` 同样会调用真实 `systemctl disable` 与 daemon-reload。现有 real-install 测试总是隐式附加 `--no-enable`，因此绕开了 install 半边；uninstall 测试主要是 dry-run/源码路径检查，没有覆盖 live-systemd host。
- **后果**：在开发者或 CI 的真实 systemd 主机上，以临时 root 做“hermetic”安装可启用宿主已有同名 units，重定向卸载可禁用真实 nats-server/tether-broker/caddy 的开机启动。文件不越界并不能使 systemctl side effect 变成 hermetic。
- **独立证据**：安全地以 dry-run 记录对应 real-run 命令；install 子测试捕获 `systemctl daemon-reload/enable`，uninstall 子测试捕获 `systemctl disable/daemon-reload`，两臂均稳定失败。该测试不实际调用 systemctl，也不依赖当前主机是否运行 systemd。
- **建议**：设置 `TETHER_INSTALL_ROOT` 时完全禁止真实 systemctl（记录 would-run 即可），或引入显式、可注入的 systemctl root/runner；install 与 uninstall 必须对称。新增 fake-systemctl 行为测试，验证 redirected real run 的调用计数为 0，且测试本身不依赖宿主是否运行 systemd。

---

## 3. Minor / 文档与流程

### m-1 — `ValidFingerprint` 接受生成器永远不会产生的非 canonical base64

- **严重度**：Minor · **确定度**：已确认（独立失败测试）
- **文件**：`internal/auth/fingerprint.go:41-65`；独立测试 `internal/auth/permissions_test.go` 的 `TestValidFingerprintRejectsNonCanonicalBase64`。
- **机理**：`base64.RawStdEncoding.DecodeString` 默认不检查最后字符未使用的低位。32-byte digest 的 canonical 尾字符可由 `A` 改成 `B`，仍 decode 为同一 32 bytes；当前函数只看 decode 成功与长度，所以 admin 报“admitted”，但表中字符串永远不可能等于生成器输出。
- **建议**：用 `base64.RawStdEncoding.Strict()`，并最好 re-encode 后要求与输入完全相等。

### m-2 — requirements 对 migration 0019 的描述与实现相反

- **严重度**：Minor（文档失实，掩盖 M-2）· **确定度**：已确认。
- **文件**：`docs/requirements.md:205-208` 对比 `docs/distributed-broker-architecture.md:164-169`、`internal/broker/clusterwrite.go:1379-1401`。
- **机理**：requirements 声称 migration 0019 自动纳入已有 owner；实际 migration 只建空表，架构和代码都明确说明 seed 必须由 broker 启动后经 raft 完成。运维按 requirements 无法判断 seed 是否发生，也看不出标准 rolling order 会漏掉它。
- **建议**：改成真实的 seed/marker/retry/可观测契约，并提供检查与恢复命令；不能只把“migration”替换成“boot”而不解决 M-2。

### m-3 — 候选自身未通过 whitespace diff gate

- **严重度**：Low（过程/可维护性）· **确定度**：已确认。
- **文件**：`docs/reviews/prerelease-audit-review.md`，至少数十处 added line 带 trailing whitespace。
- **证据**：`git diff --cached --check` 返回 rc=2，首批位置包括 `:18,72,74,92,103,113,114,143`。
- **建议**：只清理行尾空白并重跑 diff check；本文遵守 reviewer 边界，未替生产方改写其内审报告。

---

## 4. 独立测试与门禁结果

| 检查 | 结果 | 说明 |
|---|---:|---|
| 四个 reviewer guards 单独运行 | **rc=1 / Fail** | legacy proc.exit 将 victim 改为 EXITED；cluster 四处 raw `auth.VerifyPIN`；non-canonical fingerprint 被接受；redirected install/uninstall 两臂仍输出 host systemctl。均命中 production 行为/接线，不是 helper 自证。 |
| 全仓 `go test -count=1 ./...`（排除四个 reviewer guards） | **rc=0 / Pass** | 追加 simcluster 变更与第四条 guard 后完整复跑；所有包通过（broker 362.737s、cluster 111.033s）。更早一轮仅因新测试函数尚未写入 determinism inventory 而 rc=1；运行仓库 updater 后 inventory/origin 定向复跑 rc=0。 |
| 初次 `make test`（受限 sandbox） | **rc=2 / 环境失败** | NATS/PTY 本地 bind 被 sandbox 拒绝且 Go cache 只读；因此不把该次结果归因于产品，随后在获准本机环境完成上述全仓基线。 |
| `go test -race`：agent/authcallout/tunnel/cmd/concurrency | **rc=0 / Pass** | 未发现 race。 |
| `go test -race`：broker 首轮 | **rc=1 / timeout** | 默认 10m 处仍在 SQLite migration setup；无 race detector 报告。 |
| `go test -race -timeout=20m ./internal/broker`（排除两条 broker reviewer guards） | **rc=0 / Pass** | 710.451s；未发现 race。 |
| `make e2e-parallel` | **rc=2 / Fail** | 17/17 top-level、67 units、3m57s。四个失败单元分别命中四条 reviewer guard；另有 D4 broker 6/8 在负载轮无具名 assertion 地退出 1，但以完全相同 98-test shard + `-race` 隔离复跑 123.229s rc=0，完整 broker race 与最终普通基线也通过。记录为单轮未复现信号，不把它包装成 Pass，也不在没有稳定机理时制造 finding。 |
| `make lint` | **rc=0 / Pass** | `0 issues.`，独立 cache 路径。 |
| `sh test/simcluster/tests/run-all.sh` | **rc=0 / Pass** | 初始与并发 simcluster 文件写入期间各完整运行一次；6 文件最终稳定后又完整复跑一次，均以 `simcluster hermetic gates: ALL PASS` 结束。 |
| shell syntax + `lint-drills.sh --all` | **rc=0 / Pass** | install.sh 与追加 6 文件均通过 `sh -n`；最终 43 个 contract-enforced drills、0 violations。语法通过不证明 M-4 的宿主副作用安全。 |
| `git diff --cached --check` | **rc=2 / Fail** | 内审报告存在 trailing whitespace，见 m-3。 |
| `make gates` | **未作为绿证据运行** | 当前四个 reviewer guards 会确定性使 aggregate gate 变红；其组成中的全仓 baseline、determinism/origin、lint、simcluster 已分别核验。 |
| live deploy-tier simcluster | **未运行** | 没有在本轮上下文中提供目标主机/凭据；结论不依赖 live 远端。M-2 仍应在修复后增加三节点真实滚动 drill。 |

> 所有本报告列为 Pass/Fail 的命令都已取得最终退出码；没有把“已启动”或“隔离复跑通过”冒充原始并行门通过。

本审查新增并保留四条最小、相互独立的红测；没有修改任何生产实现。测试函数 inventory 已用仓库自己的 updater 增长，避免绕过 determinism gate。

---

## 5. 上线前最低闭合顺序

1. 先裁决 N-1 安全边界：关闭 B-1 的匿名 shared inbox 与长期 credential 残留，同时让 B-2 的空 SID 写入 fail closed。两者都不能靠“peer trusted”或“下一 release 清理”结案。
2. 修复 M-1 leader-side budget 接线，让两个独立 auth/proc 红测转绿；再补真实 forwarded 行为覆盖。
3. 重做 M-2 的 rollout state machine：mixed window、leadership transfer、seed retry、marker 生效必须有一套一致契约和三节点 oracle。
4. 统一 single-mode terminal 的首发/replay dedup identity，关闭内审明列的 F-T1 OPEN。
5. 让 redirected install/uninstall 对 host systemctl 真正 hermetic，收紧 fingerprint canonical 校验，修正文档与 whitespace。
6. 最后无排除项重跑 `make test`、`make gates`、`make e2e-parallel`、完整 touched-package race 和所需 live rolling drill；只有所有 reviewer guards 转绿且报告不再含 OPEN，才可改判 Pass。

---

## 6. 主进程逐条回复（2026-09-03）

> 按 `CLAUDE.md` §3 step 6，回复写在本报告内、逐条对应。**九条全部采纳，无驳回。**
> 每条新守卫都做了变异验证——注入它声称能抓的缺陷并确认变红，结果记在 §6.10。

### B-1 — 采纳（设计裁决由用户做出）

结论正确，且我确认了它最关键的一半：**这个缺陷无法用 ACL 收窄**。老客户端在连上**之后**才自选随机回复主题，
而 per-connection 权限在 CONNECT 时就发完了，没有任何静态授权能表达「只准收你自己那条」。
`_INBOX.*` + `_INBOX.*.*` 那版收窄也不行——服务端把订阅自身的 `>` 当字面 token 匹配，
`_INBOX.>` 因此仍被 `_INBOX.*` 准入。所以共享空间**做不成私有的**。

穷尽之后把三条路（彻底废弃 legacy grant／保留窗口+到期轮换／秘密不进共享空间）提交用户裁定，
用户选**第三条**。落地：

- `auth.IsPrivateInboxSubject`（**白名单**，只认 `_TINBOX.` 根——判错方向的代价是把 tunnel token
  发进一个读者集合未知的空间，所以不能写成 `!HasPrefix(s, "_INBOX.")`）。
- `handleRegister`：回复主题不在私有根 → `NodeRegisterResp.Proxy` 整个省略，
  回 `OK=true` + `Code=legacy_inbox_no_secrets`，broker 侧 **WARN** 并打印 reply subject 作为证据。
  **注册仍然成功**是这个设计的承重半边：拒绝注册更简单，但节点永不 ONLINE，
  `tether node upgrade` 这条唯一的修复路径就断了，整队 agent 失去带内升级——与
  `project-orderly-update-from-v050` 直接冲突。
- `proxySubCreate`：同判据withhold `/sub` URL（bearer token），并明说要升级 ctl 后重建——
  **已经在共享总线上出现过的 token 不该再发一次**。
- 需求文档 §6.7 按报告要求改写：原「本 release 没有 N-1 例外」与它自己末尾登记的长寿凭据泄漏
  自相矛盾（这正是你判 Fail 的依据之一），现改为**一条明确的、内容级而非凭据级的 N-1 例外**，
  并写明存量 v0.5.0 部署升级后应轮换一次 token/PSK（运维动作，不再登记成"未实现的后续项"）。

测试：`internal/broker/inbox_secret_fence_test.go`（正负控制成对：私有 inbox 仍拿到 directive，
共享 inbox 拿不到且 `OK=true`；`/sub` 同构；`IsPrivateInboxSubject` 的白名单方向单独钉）。

**报告未点、但同源必须一起改的**：`internal/broker/proxy_test.go` 的 `proxyTestBroker` 原先用
`nats.Connect(url)`（默认 `_INBOX`）。不改的话 12 个测试文件都会在**降级路径**上做断言却自称在测正常路径。
已改为携带 per-identity 前缀，与生产客户端一致。

### B-2 — 采纳

结论与机理都成立。我确认了原注释**押错了哪一跳**：`exec.go` 钉住的是 agent→接收方 broker 那一跳，
空 SID 出现在**下一跳**——N-1 broker 转发时旧 payload 没有 `Sid` 字段可承载它刚才知道的东西，
而它的输入仍是 agent 选的 PID。

按报告建议取「拒绝 + 依赖 reconcile 恢复」：`proc.ErrUnscopedExit`，`PlanMarkExited` 与共享渲染器
`markExitedSQL` **各自独立拒绝**（后者今天不可达——正因如此才是第三个调用方会重新引入通配 UPDATE 的地方）。
代价有界且自愈：`reconcileOnRegister` 每次注册都会把 broker 侧 RUNNING 而 agent 未上报的 pid 判为孤儿收敛掉，
所以丢的是延迟不是退出码。

存量测试 `TestMarkExitedAcceptsEmptySidForTheNMinusOneWindow` 把这个漏洞**写成了需求**，已反转为
`TestMarkExitedRefusesAnUnscopedExitOnBothWriters`（测试身份 golden 需手改，理由进 commit message）。

### M-1 — 采纳

四个站点与 `handler.go:743-745` 那句「在 leader 上 charge」的失实描述都属实。落地：

- `Handler.VerifyPINWithBudget` —— **既检查又计费**（`chargedVerifyPIN` 只计费）。这一点报告没点破但很关键：
  forwarded 写从未经过本 broker 的 `Handle`，若只计费不检查，经 follower 扇入仍能打爆 leader 的天花板。
- `forwardDeps` 显式依赖穿过 dispatch 表；`proposeWithPIN` 是独立 builder，**让"哪些 verb 花预算"在表里可见**。
- 未接线时 `errPINVerifierUnwired` **失败关闭**，绝不回退 `auth.VerifyPIN`——否则缺陷会在下一个忘记传参的
  调用方那里无声复活。
- handler 在 `SubscribeClusterApply` **之后**才构造，故 forwarded 路径经 `atomic.Pointer` 晚绑定。

按报告要求补了行为测试（`TestClusteredPINWritesRunThroughTheSuppliedVerifier`）：两条 clustered 路径都真的
调用所供 verifier、**不存在的 session 不消耗预算**（round 2 A-F3 那条性质）、无 verifier 时不验证。

### M-2 — 采纳（两半都修）

时序论证成立。原注释「tries again on its next boot」正是漏洞——**滚动升级没有下一次 boot**。

1. seed 注册为 `authorityLeader` 的 reconcile pass（level-triggered），顺带覆盖报告要求的
   「一次瞬态 Propose 失败后无需重启自愈」。它出现在 `TestCoreReconcilePassesAreRegisteredAsSpecified`
   的清单里，就是「不再是 boot-only」的机械形式。
2. 报告点名的**附加窗口**（marker 未提交期间新 follower 用空表拒绝既有 owner）按建议做 fail-safe
   grandfather：只放行**已经拥有 session** 的指纹，陌生身份照常拒绝。

这里有一条报告没点、但差点做错的负向：撤销**刻意不删** owner 的既有 session，所以「owns a session」
在撤销后永远为真——grandfather 若活过 marker 就会**静默撤销掉运维的每一次撤销**。已单独钉住。

**关于报告 §M-2「追加 simcluster 变更没有覆盖该缺陷」**：接受，且判断准确。drill 30 验证的是
「已 allow 身份在 roll 中持续写」，`tether-next` 也只是同代码的 next-version 构建，不是缺少 op/migration
的真实 N-1 binary。那批 `admit_creator` 改动的目的仅是让 fresh-broker fixture 能跑起来（43/43 全红→19 GREEN），
从未用于反驳本 finding。

### M-3 — 采纳（此前记为 OPEN，现关闭）

报告的诊断是对的，而且**代码自己已经写出了正解**（`xfer_inflight.go` 那段注释：给 audit publish
一个 content-derived `Nats-Msg-Id`，把 `PubAck.Duplicate` 当作"已提交"）。此前没做是因为它被判为
"a real change to the audit publish path"。现按报告要求统一：

- `pubAuditTransfer`（正常首发）改用 `xferaudit.TransferRecordReqID` 派生的 id；
- `replayStagedTerminal` 的 single-mode 分支不再永久 RETAIN，改为发布**逐字相同**的 staged bytes、
  **发布成功之后**才 unlink；
- `publishAuditDeduped` 从 `js.Publish + WithMsgID` 改为 `PublishMsg` + 显式 header——
  option 形式是对未导出结构的闭包，测试替身**看不见**它携带的 id，而「首发与 replay 用同一个 id」
  正是这里的全部正确性论证，得让它可检验而不只是被断言。

两个窗口按报告要求各自成测（`xfer_terminal_dedup_test.go`）：crash-before-publish 由 replay 真的发出来覆盖，
crash-after-publish 由两个 id 相等覆盖；另加负向控制（同一 transfer 的 complete 与 failed 必须**不同** id，
否则第二个会被当重复丢弃）与一条 ledger 落盘往返后 id 不变的守卫。

### M-4 — 采纳

「文件不越界并不能使 systemctl side effect 变成 hermetic」这句话是对的。落地：`host_systemd_is_the_target`
单一判据，install 与 uninstall **对称**使用（报告特别强调对称，而不对称正是 install 半边曾被修好、
uninstall 半边继续写宿主的原因）；被跳过的调用以 `would have run: …` 记录，**故意不写成 `(dry-run) systemctl`**
——那个前缀是真实预览的形状，seam 运行必须可区分。你的 `TestRedirectedBrokerLifecycleDoesNotTargetHostSystemd`
两臂现均绿。

### m-1 — 采纳

`base64.RawStdEncoding.Strict()` **加上**重新编码比对（`EncodeToString(raw) == body`）。只加 Strict 也能过你的测试，
但两层一起才把"未来某次编码行为变化重新放进一个永不匹配的条目"也挡住——变异验证时这一点被证实：
只拆一层另一层仍然拦得住，必须两层同时退回修复前形态才变红（见 §6.10）。

### m-2 — 采纳

`docs/requirements.md` 已改写为真实契约：leader 经 raft 一次性回填、marker 与数据行同一 entry、
level-triggered 重试、marker 前的 grandfather 语义、marker 后表是唯一权威。并按报告要求写明这不是措辞问题——
按原文运维无从判断回填是否发生过，也看不出标准滚动顺序会漏掉它。

### m-3 — 采纳

`docs/reviews/prerelease-audit-review.md` 行尾空白已清（30 行）。**先验证过没有任何一行以恰好两个空格结尾**
（Markdown 硬换行），再做纯空白剥离，并以「忽略空白后逐字节相同」核验未改动任何语义。

### 6.10 变异验证结果

| 守卫 | 注入的缺陷 | 结果 |
|---|---|---|
| B-2 `TestMarkExitedRefusesAnUnscopedExit…` | 两层同时退回无范围 `WHERE pid=?` | **RED ✓** |
| m-1 `TestValidFingerprintRejectsNonCanonical…` | 退回 decode+长度校验 | **RED ✓** |
| M-2 grandfather | 拿掉 marker 前的 owner 放行 | **RED ✓** |
| M-2 seed 重试 pass | 取消 pass 注册 | **RED ✓** |
| M-1 行为测试 | seam 改回未计费 verifier | **RED ✓** |
| B-1 register fence | 去掉共享 inbox 判据 | **RED ✓** |
| B-1 `/sub` fence | 同上 | **RED ✓** |
| M-3 id 一致性 | 首发改回未 dedup | **RED ✓** |
| M-4 install.sh | uninstall 半边改回触碰宿主 systemd | **RED ✓** |

⚠ B-2 与 m-1 **第一轮变异是 GREEN**——不是漏洞，是双层防御：只拆一层另一层仍然拦着。
两层同时退回修复前形态才变红。这条记在这里，因为「变异没红」的第一反应本该是怀疑守卫，
而这次正确答案是怀疑变异是否真的到达了被测对象。

---

## 7. 外审后的 deploy-tier 工作（主进程，2026-09-03 续）

> 本报告 §4 记录 live deploy-tier simcluster **未运行**（"没有在本轮上下文中提供目标主机/凭据"），
> 并要求 M-2 修复后补三节点真实滚动 drill。这一节是补跑的结果。它**推翻了本报告没有覆盖到的两件事**，
> 并因此产生了两处新的产品改动——都不在 §1–§3 的九条里。

### 7.1 起点：drill 全红，根因是九条之外的东西

第一次全量 sweep：**GREEN=0 / 43**。43 条全部死在同一个根因——全新 broker 的 `session_creators`
表是空的，`MayCreateSession` 对空表返回 false，于是每个 drill 的第一步 `session create` 就被拒。
drill 写于准入需求存在之前，`broker-ops.md §5.20` 记的运维步骤（`tether whoami` → `admin
session-allow`）没有对应的 fixture。

修法是让 fixture 照做那两条命令（`admit_creator`，`lib/tether.sh`）。两个不显然的点：

- **身份是 `(user, HOME)` 二元组**：`cli.DefaultHome()` 认 `TETHER_HOME`，`EnsureIdentity` 按 home
  各铸一把 nkey，所以 ctl1 上的 `sim` 用户**每个 CTLH tag 都是不同指纹**。admit 错 home 会放行一个
  没人用的指纹而 create 照样被拒——**长得跟原 bug 一模一样**。
- **先 `--list` 再写**：`--list` 是本地读，而 `session-allow` 是走 raft 的复制写、要过混版能力门
  （任一 voter 联系不上即拒）。`90-alerts-lifecycle` 会在故意宕了一个 broker 的 N=2 臂里再次调用
  `$SIM session`，那里**唯一会失败的就是这个 no-op 重复 admit**。

同时拆掉一处伪装：`simcluster:281` 的 `|| true` 把产品拒绝吞掉，失败晚一行以 **"auth setup failed"**
的名义冒出来——运维会去查 auth 配置，而问题在准入表。现在只容忍 `already_exists`，其余原样报出。

**顺带查出一处运维文档缺口**（不在九条内）：`cluster add` / `unlock` / `upgrade` /
`seeds show --remote` / `status --remote` 六个操作都需要 ctl 身份，因而都在准入这道门后面，
而 `cluster-runbook.md` 对此只字未提。已补进 §1 的前置块。

### 7.2 把「不是我改的」与「确实是回归」分开

drill 修好后仍有 8 条偏离 `expected-verdicts.tsv`。定性用了三步，每步都是可证伪的：

1. **本轮修复不背这个锅**：含九条修复的 sweep，其偏离集是**不含修复那轮的严格子集**，
   且逐条命中**同一条断言**。
2. **登记表本身过期**：`expected-verdicts.tsv` 上次校准是 2026-08-19 的
   `cloned-credential-instances` 批次，早于本增量。于是建了 **HEAD 的 git worktree**
   （工作树一行未动）、用 HEAD 的源码烘镜像（nats-server 自动烘成 HEAD 的 pin **2.10.22**）、
   用 HEAD 的 drill 跑那 8 条——那正是该表被校准时的状态。
3. **结果**：6 条在 HEAD 上就是 ASSERT-FAIL / INFRA-ABORT（登记表过期，非回归）；
   **2 条在 HEAD 上是 GREEN**：`50-backup-restore`、`95-broker-selfheal`。

### 7.3 两条回归的唯一根因：nats-server pin 升级

只动 nats-server 版本这一个变量（tether 代码逐字相同）：

| 组合 | drill 95 | drill 50 |
|---|---|---|
| HEAD tether + 2.10.22 | GREEN | GREEN |
| 本轮 tether + **2.14.5** | ASSERT-FAIL（`assert_fail=6`，T2c 起级联） | ASSERT-FAIL |
| 本轮 tether + **2.10.22** | `assert_fail=1`，**T2c PASS** | **GREEN**（pass=87） |

**tether 自己的代码造成零条部署层回归。** 两条全部来自增量 2 内审时把三处 pin 对齐到 v2.14.5。

机理是两层的，第二层是**与版本无关的真缺陷**：`internal/broker/broker.go` 的 boot 路径对 JetStream
只探**一次**（1s 超时），而 cluster 模式把探测落空当作 FATAL，于是 exit 70 → `Restart=always` →
死循环。drill 95 实测 `NRestarts` 到 **137**、ready 行始终不增。而那条 fatal 自己的文案写着
「(1) **TRANSIENT** … JS **self-heals** once quorum returns」，同时建议运维
「**STOP the crash-loop now: systemctl stop tether-broker**」——**产品能说出自己的瞬时态，却选择
立刻退出，并把「关掉它」当成诊断**。2.14.5 下 N=2 的 JS meta quorum 在 broker 被 SIGKILL 后重组更慢，
单次探测抓不到；但任何一次主机负载高、路由慢形成、滚动重启，都能在**任何版本**上触发同一条死循环。

### 7.4 两处新的产品改动（用户裁定「两件都做」）

1. **pin 三处降回 v2.10.22**（`go.mod` / `install.sh` / `Makefile`，由 `nats_server_pin_test.go` 守）。
   升级的**原始理由已作废**：它是为了让 `deny _INBOX.*.*.>` 装载（仅 ≥2.14），而设计随后改成
   `_TINBOX` 独立根、根本没有 deny。剩下的约束只有「测的服务端 == 发的服务端」，两个方向都能满足。
   降级后 `go` directive 仍是 1.25.0，全仓编译通过。
2. **boot 期 JS 探测改为有界轮询**（`waitForClusteredJetStream`，90s 预算 / 2s 重试）。
   **有界而非无限**：被 force-single 踢出去的节点永远不会恢复，必须仍能走到那条可操作的 fatal——
   一个永远挂着、看起来活着却什么都不服务的进程比崩溃循环更糟。
   并且把**可操作诊断挪到等待开始时就打印**（`clusteredJetStreamDiagnostic`，WARN），
   而不是只在 90 秒后随 fatal 出现：那段文字的作用正是告诉运维**等待是否可能有用**
   （mesh 没形成会自愈 / 被踢出去不会），而只在 fatal 时给出，等于在它失去意义的那一刻才送达。
   诊断函数做了 nil 防护——一个能 panic 的诊断会把它要诊断的进程带下去，比它描述的故障更糟。

**复验**：`50-backup-restore` **GREEN**（pass=87，K-#64c 的 reality-tie 因诊断提前打印而**重新变回
确定性**）；`95-broker-selfheal` **GREEN**（pass=44）。

**变异验证**（新守卫各注入其声称能抓的缺陷）：退回单次探测 → 正控制 **RED**；去掉预算上限 →
边界测试 **RED**。

### 7.5 本节暴露的一条方法论

**四道硬闸对这两条回归完全无感**，而且不是巧合：它们需要一个**真实的、慢的、集群化的 JetStream**，
而没有任何单元测试构造得出，也没有任何 e2e 矩阵含有它。这与本报告 §0 的结论同构——
"Fail 集中在 rollout/security/recovery 的几条跨组件组合路径，而恰好是单包绿色最难覆盖的部分"。
补充一句本轮学到的：**`expected-verdicts.tsv` 会过期，而过期的登记表长得跟回归一模一样**；
区分二者的唯一办法是拿基线 commit 建 worktree 重跑，而不是读表。

---

## 8. 开发者修订后二轮外部重审（2026-09-03）

> 范围：以第一轮末尾 209 个已暂存文件为基线，完整审阅开发者追加的 49 个 tracked unstaged
> 修改（初始约 `+1520/-247`）与 3 个 broker 测试；随后把本轮 tasklist、3 个独立 reviewer
> guard 及其 determinism inventory 一并纳入组合复验。以下结论覆盖 §6 的逐条回复和 §7 的
> deploy-tier 追加工作，不继承其中的“关闭”判断。

### 8.1 最终结论

**发布判定仍为 Fail。** 第一轮的 B-2、M-4、m-1、m-3 已关闭；M-2 的 level-triggered seed
与 marker 前 grandfather 两半也分别成立。但同一功能在 cluster-only 分支、leader 最终提交边界和
长期恢复窗口仍有三个确定性穿透；deploy drill 为恢复绿色而回退的 nats-server 版本又新增一个公开攻击面
Blocker。二轮共确认 **3 个 Blocker、3 个 Major、1 个 Minor**：

| 第一轮项 / 二轮项 | 状态 | 二轮裁决 |
|---|---:|---|
| B-1 legacy inbox | **OPEN / Blocker** | register 与 single `/sub` 已栅栏；cluster `/sub` 独立 handler 仍回传 bearer URL，且迁移手册轮换次序会短暂重新启用旧 PSK。 |
| B-2 empty-SID proc exit | **CLOSED** | planner 与 SQL renderer 均拒绝空 SID；旧消息只等待 scoped reconcile，不再越租户写。 |
| M-1 cluster PIN budget | **OPEN / Major** | 四个 raw verifier 接线已换掉；新增的 check 与 charge 分离，64 个并发请求可共同越过 burst=1。 |
| M-2 creator seed | **PARTIAL → 新 Blocker** | seed retry/grandfather 已关闭原可用性问题；origin-only admission 可被 N-1 broker 或 stale follower 转发绕过。 |
| M-3 single terminal replay | **OPEN / Major** | 首发/replay 的 Msg-Id 已一致，但只在 2 分钟 dedup window 内幂等；长停机后仍写出第二条 terminal。 |
| M-4 redirected systemd | **CLOSED** | install/uninstall 的真实 `systemctl` 调用均受同一 host-target 判据保护。 |
| m-1 / m-2 / m-3 | **CLOSED / PARTIAL / CLOSED** | canonical fingerprint 与 whitespace 已闭合；creator 文档仍遗漏 leader admission 这一安全语义。 |
| deploy NATS pin | **新增 Blocker** | 三处一致回退到已知受多个上游安全通告影响、且已不在支持分支的 v2.10.22。 |
| deploy JS boot wait | **新增 Major** | 只重试 `AccountInfo` 不可用；events stream ensure 的瞬态失败仍立即退出。 |

阴性结论也应保留：proc 的双层 scope fence、creator 的 leader retry、register secret strip、single-mode
`/sub` strip、strict base64、redirected-root systemd、三处 pin 一致性、diagnostic nil guard 均经代码与定向
测试成立。二轮 Fail 不是把所有修订都判作无效，而是剩余路径仍直接跨越认证、授权或审计完整性边界。

### 8.2 Blocker

#### R2-B1 — cluster `/sub` 路径遗漏 shared-inbox secret fence

- **严重度**：Blocker（长期 bearer token 与 Shadowsocks PSK 泄漏）· **确定度**：已确认（真实 NATS 请求复现）。
- **文件**：`internal/broker/proxy.go:240-258,280-325`；`internal/broker/proxy_cluster_wire.go:119-156`；
  `docs/broker-ops.md:846-867`；独立测试
  `internal/broker/inbox_session_xfer_guards_test.go` 的
  `TestClusterProxyCreateAlsoWithholdsSecretsFromLegacyInbox`。
- **机理**：single mode 的 `proxySubCreate` 在 `auth.IsPrivateInboxSubject(msg.Reply)` 为 false 时已正确
  创建但不返回 URL。cluster mode 在分派点提前改走 `handleProxySubCreateCluster`；该函数 mint/commit 后
  无条件以 `SubURL: b.subURL(rawToken)` 回复，完全没有相同栅栏。开发者新增的
  `inbox_secret_fence_test.go` 只构造 single-mode broker，因此没有执行这个生产分支。
- **独立证据**：用 embedded NATS 的默认 request inbox 调用 cluster handler，返回
  `https://localhost/sub/<raw-token>`，guard rc=1。也就是说 §6 所称“本版起这些秘密不再进入共享空间”
  对 cluster 部署仍为假。
- **迁移闭环也有次序错误**：手册要求 `proxy off` → `proxy on` → 再逐个 revoke/recreate subscriber。
  `proxy off/on` 会轮换 tunnel allocation token，但 `enableProxy`/cluster reaper 读取
  `activeProxyKeys`，仍把原有 subscriber PSK 作为当前 keyset；`/sub` bearer row 也只有 revoke 才失效。
  因此 `on` 到最后一条 revoke 之间，已泄漏旧 bearer/PSK 被重新启用。标题编号还把 8.9 放在 8.8 前。
- **闭合要求**：复用一个 fail-closed helper 覆盖 single 与 cluster 两个 create handler，并新增真实
  cluster reply 四象限测试。轮换流程须在 proxy 保持 OFF 时先 revoke 全部旧 subscriber，再 ON，最后只从
  private inbox 重建；或实现原子 rotate-all。不得宣称 off/on 会重铸 subscriber keyset。

#### R2-B2 — session creator admission 未在 leader commit 边界重检

- **严重度**：Blocker（公网匿名身份可经混版 peer 创建 session 并成为 owner）· **确定度**：已确认
  （真实 leader dispatch + Raft/FSM commit 复现）。
- **文件**：`internal/broker/sessions.go:45-75`；`internal/broker/cluster_forward.go:795-800`；
  `internal/session/creators.go:26-83`；独立测试
  `TestForwardedSessionCreateRechecksAdmissionOnTheLeader`。
- **机理**：`MayCreateSession` 只在接收客户端请求的 origin broker handler 执行；
  `VerbSessionCreate` 到 leader 后直接 `session.PlanCreate`。标准 N-1 窗口中的旧 broker binary 根本没有
  admission check，却能以受信 peer 转发攻击者 payload；当前 follower 的 policy view 在 revoke 后短暂落后时
  也有同一问题。leader 把 transport 身份误当作已完成的业务授权。
- **独立证据**：测试先提交 seed marker、allow 后 revoke 指纹，再把该指纹的 forwarded create 送入
  `dispatchForward`。当前实现返回成功并提交 `rogue` session，guard rc=1。marker 前还更危险：攻击者写出的
  owner 可被后续 grandfather。
- **闭合要求**：在 leader `Propose` closure / authoritative dispatch 内、`PlanCreate` 之前按 leader committed
  DB 重检 `MayCreateSession`，并保持 origin 的廉价早拒绝。增加 N-1 old-origin、stale-follower-revoke、
  pre-marker stranger 与 admitted owner 四条测试；文档需明确授权以 leader 决策为准。

#### R2-B3 — deploy drill 回退到已知不安全且已退出支持窗口的 nats-server v2.10.22

- **严重度**：Blocker（公开 WSS 的 pre-auth remote DoS / crash，并含 JetStream 授权缺陷）·
  **确定度**：已确认（版本 pin 与上游官方 advisories 直接比对）。
- **文件**：`go.mod:12`；`Makefile:260`；`scripts/install.sh:1079`；公开 WSS 契约见
  `docs/architecture.md:40-74,113-120`；独立 guard
  `test/architecture/nats_server_security_floor_test.go`。
- **机理**：三处 pin 的一致性是对的，但值从 2.14.5 降到 2.10.22。上游 release policy 只维护当前与上一
  minor；2.10 已不在该集合。更直接地，2.10.22 落在以下官方 affected ranges：
  [GHSA-pq2q-rcw4-3hr6](https://github.com/nats-io/nats-server/security/advisories/GHSA-pq2q-rcw4-3hr6)
  的未认证 WebSocket remote crash、
  [GHSA-qrvq-68c2-7grw](https://github.com/nats-io/nats-server/security/advisories/GHSA-qrvq-68c2-7grw)
  的 WebSocket compression memory DoS，以及
  [GHSA-fhg8-qxh5-7q3w](https://github.com/nats-io/nats-server/security/advisories/GHSA-fhg8-qxh5-7q3w)
  的 JetStream admin API authorization bypass。上游支持策略见
  [RELEASES.md](https://github.com/nats-io/nats-server/blob/main/RELEASES.md)。本项目经 Caddy 公开转发 NATS
  WebSocket，前两条可在认证前到达，不能以 deploy drill 绿色接受。
- **独立证据**：security-floor guard 在 `go.mod` 发现 2.10.22 后稳定 rc=1；既有 pin-pairing test 只证明
  三处相等，无法判断所选版本是否安全。
- **闭合要求**：在仍受支持且不落入上述 affected range 的版本上解决/隔离 rollout 回归；把安全下限或
  denylist 纳入架构 guard，并对目标版本重跑 50/95 和全量 deploy sweep。不能把“升级旧理由已作废”推导成
  “任意旧版都可发布”。

### 8.3 Major

#### R2-M1 — leader Argon2 budget 的 check/charge 存在 TOCTOU

- **严重度**：Major（昂贵 Argon2 工作的进程上限可被并发放大）· **确定度**：已确认（确定性并发反例）。
- **文件**：`internal/authcallout/handler.go:745-776`；`internal/authcallout/ratelimit.go:182-219`；独立测试
  `internal/authcallout/pin_budget_atomicity_test.go`。
- **机理**：`VerifyPINWithBudget` 先在一次锁内用 `TokensAt` 纯读，再解锁；随后
  `chargedVerifyPIN` 通过另一次锁内的 `AllowN` 消费。两个 cluster subscriptions/多个 goroutine 可全部看见
  最后一个 token 后再分别进入 Argon2；`AllowN` 的 false 结果又被丢弃。接线替换关闭了上一轮 raw verifier
  漏计费，却没有实现“检查与占位是同一个原子动作”。
- **独立证据**：固定 rate=0、burst=1，用 barrier 让 64 个调用者先完成 check，再允许 charge；当前
  **64/64** 都进入 verifier，而期望只有 1 个，guard rc=1。
- **闭合要求**：提供一个在同一临界区/同一 `rate.Limiter.AllowN(now,1)` 中返回 bool 的 try-spend，并只让
  成功者调用 Argon2；保留 single path 恰好消费一次的行为测试和 concurrent burst test。

#### R2-M2 — content Msg-Id 只提供有限时间去重，不能证明 terminal 恰好一次

- **严重度**：Major（审计历史出现重复 terminal）· **确定度**：已确认（真实 JetStream 过窗复现）。
- **文件**：`internal/broker/transfer.go:980-1020`；`internal/broker/xfer_inflight.go:482-570`；
  `internal/broker/broker.go:807-839`；`internal/jsstream/replicas.go:16-25`；独立测试
  `TestSingleBrokerTerminalReplayAfterDedupWindowDoesNotDuplicate`。
- **机理**：正常首发与 crash replay 现在确实产生相同 `Nats-Msg-Id`，但 JetStream 只在 stream 的
  `Duplicates` window 内记住它；仓库配置为 2 分钟。进程在 publish-ack 后、ledger unlink 前崩溃并停机超过
  2 分钟，replay 会被当作新消息接收。开发者测试只比较两个 header 相等，没有让真实 stream 的窗口过期。
- **独立证据**：真实 memory JetStream 设 100ms duplicate window，首发后等待 300ms 再走生产 replay；
  stream 最终有 2 条 terminal，guard rc=1。
- **闭合要求**：以不随 broker downtime 过期的 durable 事实判重，例如持久 commit ledger / audit stream
  identity lookup，并在确认已存在后再 unlink；测试必须覆盖超过 configured window 的 post-publish crash。
  单纯扩大 window 只移动故障边界。

#### R2-M3 — clustered JetStream boot wait 遇到 transient ensure failure 仍立即退出

- **严重度**：Major（cluster broker 仍可进入 systemd crash loop）· **确定度**：代码路径已确认；本轮未在
  live deploy 注入该特定瞬态。
- **文件**：`internal/broker/broker.go:1291-1308,3027-3083,3107-3137`；开发者测试
  `internal/broker/jetstream_boot_wait_test.go`。
- **机理**：首次 `enableJetStream` 只要 `AccountInfo` 成功而 `EnsureEventsStream` 因 5s deadline、meta
  placement 或短暂 server error 失败，`Run` 在进入 wait 前就 `return err`。在 wait 内同一错误也会
  `return` 而不是消耗剩余 90s 预算。故有界轮询只修了“JS 完全不回答”的一种 transient；
  “JS 已回答但 events stream 暂不可 ensure”仍是立即退出。当前 tests 只有 no-JS 与 fully-healthy 两极。
- **闭合要求**：cluster mode 将所有可判为 transient 的 probe/ensure 错误纳入同一 deadline loop，保存最后错误
  用于最终诊断；single mode 的现有降级契约保持不变。增加 `AccountInfo` 成功、ensure 前几次失败后恢复的行为测。

### 8.4 Minor / 文档

#### R2-m1 — broker-ops 迁移章节编号倒序

`docs/broker-ops.md` 依次为 8.4、8.5、8.6、8.7、**8.9**、**8.8**。这不会独立阻断发布，但会破坏章节引用；
修复 R2-B1 的轮换步骤时应一并重排，且不得保留“off/on 作废当前 keyset”的失实表述。

### 8.5 独立测试与门禁证据

| 检查 | 结果 | 说明 |
|---|---:|---|
| 第一轮四个 reviewer guards + 开发者定向修复测试 | **Pass** | canonical base64、empty-SID proc、PIN wiring、redirected systemd 及 single/register secret fence 等目标测试均绿。embedded NATS 测试在获准本机环境运行；一次 sandbox bind 拒绝只属环境，不计产品失败。 |
| 二轮 5 个 reviewer guards | **Fail（预期红）** | PIN 原子性报 burst=1 admitted 64；cluster `/sub` 返回完整 bearer URL；leader 提交 revoked creator；过 dedup window 后 history=2；NATS 2.10.22 security floor 失败。均走 production path。 |
| `go test ./... -skip <5 reviewer guards> -count=1` | **除一次 inventory 命名问题外通过** | 所有产品包通过（broker 365.505s、cluster 109.689s）；reviewer 文件按仓库命名规则重命名并更新 inventory 后，determinism 定向复跑通过。后续未排除 guards 的 `make test` 也只报告这 5 条红测。 |
| `go test -race ./internal/broker -run <修复触碰测试>` | **Pass** | 7.363s，无 race。 |
| `go test -race ./internal/authcallout -skip TestVerifyPINWithBudgetCheckAndChargeIsAtomic` | **Pass** | 97.473s，无 race；逻辑 TOCTOU 不会由 race detector 报告。 |
| `make lint` | **Pass** | `0 issues.`，独立 cache。 |
| `make gates` | **Fail** | vet-tags 与 Darwin cluster build 通过；Go test 阶段命中 NATS security-floor guard 后停止，未伪造 aggregate green。 |
| `make test` | **Fail** | vet-tags/Darwin build 通过；全包测试恰好由上述 5 个 reviewer guards 失败，其余列出的包通过。 |
| shell / diff | **Pass** | `bash -n scripts/install.sh` 与最终 `git diff HEAD --check` 通过。 |
| deploy drill 50 / 95 | **开发者记录 Pass，本轮未重跑** | §7 记录 pass=87 / pass=44；二轮代码审查接受其对旧 pin 的功能复现，不接受用它覆盖安全版本下限，也未在本机伪装 live deploy 证据。 |

本轮 reviewer 新增的三个测试文件是：

- `internal/authcallout/pin_budget_atomicity_test.go`
- `internal/broker/inbox_session_xfer_guards_test.go`
- `test/architecture/nats_server_security_floor_test.go`

### 8.6 发布前最低闭合顺序

1. 先关闭 R2-B1 与 R2-B2：两个 cluster/leader authoritative 边界必须复用同一安全判据，并让新增真实
   NATS/Raft guards 转绿；修正 legacy credential rotation 的顺序和措辞。
2. 选择受支持且不在官方 affected ranges 的 nats-server，保留三处 pin pairing，修复该版本上的 JS
   rollout 行为并重跑 50/95 与全 deploy sweep。
3. 将 global PIN budget 改成 atomic try-spend；用 durable evidence 关闭超过 JS dedup window 的 terminal
   replay；把 events-stream ensure 的 transient 纳入 90s boot budget。
4. 重跑二轮 5 个 guards、全仓普通测试、相关 race、lint、`make test`、`make gates`、diff check；所有硬闸与
   deploy tier 同时绿色后再进行下一轮外审。当前没有可接受的“文档豁免”路径。

---

## 9. 二轮外审的逐条回复（主进程，2026-09-03）

> 按 `CLAUDE.md` §3 step 6 写在本报告内。**六条全部采纳，无驳回。** 每条对应的 reviewer guard 均已转绿。

### R2-B1 — 采纳

结论与机理都成立，而且它指出的**测试盲区**比缺陷本身更值得记：我上一轮写的
`inbox_secret_fence_test.go` 只构造 single-mode broker，所以它从未执行 `handleProxySubCreateCluster`
这条生产分支——**栅栏覆盖了爆炸半径较小的那种部署形态，漏掉了较大的那种**。

修法按报告要求做成两个 handler 共用的 fail-closed helper（`proxySubCreateReply`），
并**额外加了结构守卫** `TestOnlyOneFunctionEverSetsTheSubURL`：`ProxySubCreateResp.SubURL`
只允许在那一个函数里被赋值（AST 扫描，含 helper 存在性的非空性控制）。理由是行为测试只能覆盖
**有人想到要构造**的路径，而这次的洞正是「没想到要构造 cluster 分支」——所以除了补那条路径，
还要让「第三个 handler 忘记栅栏」变成机械可检测。

轮换次序按报告订正：必须 **OFF 时先 revoke 全部旧 subscriber，再 ON，最后只从 private inbox 重建**。
报告对机理的判断准确——`enableProxy` 与 cluster reaper 读的是 `activeProxyKeys`（当前仍存在的
subscriber 行），先 `on` 会把那批旧 PSK 当作新 keyset 重新下发一遍。「off/on 作废当前 keyset」
的失实表述已删除，8.9/8.8 编号已归位（R2-m1）。

### R2-B2 — 采纳

与第一轮 B-2 是**同一个错误的第二次出现**：把**传输身份**（mTLS/system-account 内的对等 broker）
读成**业务授权**。第一轮是 `proc.exit` 的空 SID，这一轮是 forwarded session create。

在 `VerbSessionCreate` 的 propose 闭包内按 **leader 已提交视图**重检 `MayCreateSession`，
读错误按 origin handler 同一规则拒绝而非当作空表。origin 侧的检查保留为廉价早拒绝。

### R2-M1 — 采纳

`tryTakeGlobal`：一次临界区、一次 `AllowN`、一个 bool，只有拿到 token 的调用者进 Argon2。

**并且把 `chargedVerifyPIN` 与 `spendPINBudget` 整个删掉了**，而不是修好它们。理由：「只检查」与
「只计费」这个**区分本身**就是缺陷的来源——留着两个入口，就还能被再拆开一次。删掉之后
`VerifyPINWithBudget` 是唯一的验证器，没有哪个调用方能只做一半。lint 的 `unused` 随后抓到
`spendGlobal` 变成孤儿：一个失去最后调用方却依然活着、注释依然正确的方法，是下一个人照着注释
把它接回去的完美诱饵，所以它也被删除、论证搬进 `tryTakeGlobal`。

### R2-M2 — 采纳

报告的判断是对的：content Msg-Id 只提供**有限时间**去重，而「已经发过没有」这个问题没有时间边界。

新增 `jsstream.TransferTerminalCommitted`——**问审计流本身，不问时钟**。两个实现细节值得记：

- **按 subject 反查拥有它的流**（`StreamNameBySubject`），不按流名。你那条测试的流叫
  `history-lab-review` 而非 `history-<sid>`，我第一版按名查因此找不到——这恰好证明按名查是脆的：
  一次 restore、改名或运维手工建的流都会让它答出「未提交」，而那是**唯一会产生重复的**错误方向。
- 扫描从 transfer 自身起点（减 slack）开始，成本被一个 session 自该 transfer 以来的审计量界定，
  不被 retention 界定。

三态纪律与该文件既有约定一致：已提交→丢弃 / 未提交→重发 / **查不出来→保留**。

### R2-M3 — 采纳

`Run` 与 wait 循环内的**所有**瞬态都纳入同一 90s 预算，最后一个错误随 fatal 一起报出——
「mesh 没形成」和「JS 应答了但建不了流」是两种不同的运维动作，而单靠 ranked differential 分不出来。

补了报告要求的行为测试，失败是**确定性构造**的而非计时的：先建一个占住 events subject 的流，
JetStream 拒绝跨流重叠 subject，于是 `AccountInfo` 成功而 ensure 必然失败，正确实现会跑满预算，
旧实现第一次就返回。

### R2-B3 — 采纳，且我上一轮的因果归错了

报告是对的，而且不只在安全面上对。**我把两个变量一起动了**（降 nats-server 版本 + 加 boot-wait），
然后把 drill 转绿的功劳全记在版本上，据此写下「pin 降级是必要的」。本轮在 **v2.14.6** 上复跑：

```
50-backup-restore   GREEN  assert_fail=0
95-broker-selfheal  GREEN  assert_fail=0
```

**那两条 drill 的红从来不需要降版本来修**；真正的机理是 boot 期单次探测把瞬时可自愈状态变成
死循环，而那条已由 R2-M3 的前身（有界轮询）独立修掉。报告那句「不能把『升级旧理由已作废』
推导成『任意旧版都可发布』」击中的是同一个推理缺陷。

升级链最终为：**nats-server v2.14.6**（最新稳定、受支持）· **Go toolchain 显式钉在 `go1.26.8`** ·
**x/crypto v0.56.0**。

**顺带查出一条独立于本次外审的问题**：`govulncheck` 显示发布二进制此前是用 **`go1.25.0`**
（该 minor 的首发版、零补丁）编译的，标准库有 **31 条可达**漏洞（`net/http`、`crypto/tls`、
`net/url`、`crypto/x509` 全在公网面上），而 `go.mod` 看上去毫无问题——因为 `go` directive 是
**最低要求**，不是编译器。加上 `toolchain` 行后 **31 → 0**（剩 1 条不可达且上游无补丁）。

security-floor guard 已从**字面量黑名单**改写为**真正的版本下限**：原实现只拒绝 `v2.10.22`，
降到 `v2.10.21`（更旧、同样在 affected range 内）会照常通过。现比较 major/minor，另加一条
**工具链下限**与一条非空性控制（正则失配时不得静默通过）。`CLAUDE.md` 的 Go 版本行同步改写，
写明它是**变更控制**而非版本上限，并记下 `go get x/crypto@v0.56.0` 当场静默抬 directive
**并删掉 `toolchain` 行**的实测。

### 门禁结果

`make test` rc=0 · `make gates` rc=0 · `make lint` **0 issues** · `make e2e-parallel` **ALL PASS**（4m2s）。
deploy tier：50/95 在 **v2.14.6** 上 GREEN，全量 sweep 见下一节。

一次 `make test` 曾红在 `test/p2` 的 `TestOneContestedProbeDoesNotStallEveryOtherNodesRegister`
（阈值 100ms、实测 106ms），单独复跑 3/3 通过 0.7s——负载敏感的 flake，如实记录而不当作通过。

---

## 10. 开发者返修后的第三轮外部重审（2026-09-04）

### 10.1 结论

**发布判定仍为 Fail。** 本轮确认 **1 Blocker、3 Major、2 Minor**。开发者确实关闭了 cluster
`/sub` 直接泄密、PIN budget TOCTOU、boot ensure 首错即退，以及受影响 NATS pin 四条核心问题；但
session admission 只修了 forwarded closure，leader-local authoritative closure 仍可在撤销后提交。
新的 durable audit scan 也没有实现它注释声明的“每个 transfer 恰好一个 terminal”：已有 `complete`
不会阻止 staged `failed`，且每次查找残留一个 ordered consumer。升级手册则同时给出了不存在的撤销
命令和已被本次安全升级淘汰的 v2.10.22 降级理由。五条 production/runbook 独立 guard 稳定变红。

必须同时写明阴性结论：排除这五条刻意失败的 guard 后，`go test ./... -count=1` 全包通过；定向
`-race`、lint、module verify、shell syntax、diff check 通过；独立 `govulncheck ./cmd/tether` 确认
**0 reachable vulnerabilities**（另有 1 条 required module 漏洞不可达）。所以当前 Fail 集中在上述
authoritative admission、audit exactly-once 与可执行运维边界，不代表返修整体无效。

### 10.2 上轮 finding 逐条状态

| 上轮项 | 状态 | 第三轮证据 |
|---|---|---|
| R2-B1 cluster secret fence | **PARTIAL** | single/cluster 均调用 `proxySubCreateReply`，真实 cluster legacy-inbox 测试通过；但泄漏后轮换 runbook 的 `proxy sub rm` 不存在，不能完成凭据撤销。 |
| R2-B2 leader admission | **OPEN** | forwarded `writeVerbs` closure 已正确重检；`createSession` 的 leader-local closure 仍直接 `PlanCreate`，独立真实 raft 测试在 committed revoke 后得到 `err=nil` 且写入成功。 |
| R2-M1 PIN atomic budget | **CLOSED** | `tryTakeGlobal` 在单临界区消费并返回判定，旧 check/spend 入口删除；并发 guard、wiring、定向 race 均绿。 |
| R2-M2 durable terminal detection | **OPEN / REGRESSED** | 超过 duplicate window 的“相同记录”测试转绿，但相反 terminal kind 可形成两条互相矛盾的历史；扫描还泄漏 consumer 并含 fail-open EOF 判定。 |
| R2-M3 JS boot wait | **CLOSED（有低风险边注）** | AccountInfo 成功但 ensure 失败会在有界循环重试，取消与最终诊断测试通过。第一 probe 在 deadline 建立前、最后一次 5s ensure 在 deadline 检查前，故回复所称“所有尝试共享同一预算”并不精确；当前 30s 配置仍未耗尽 90s readiness window，不单独阻断。 |
| R2-B3 dependency/security floor | **核心 CLOSED，守卫/文档未收口** | 三处 pin 均为 v2.14.6，工具链实际选择 go1.26.8，独立 govulncheck 为 0 reachable；但运维主文仍推荐 v2.10.22，原 floor guard 又只比较 Go major/minor。 |
| R2-m1 章节编号 | **CLOSED** | 8.8/8.9 顺序已恢复。 |

### 10.3 Blocker

#### R3-B1 — leader-local session create 仍绕过提交边界的 creator admission

- **严重度**：Blocker（已撤销身份仍能创建 session）· **确定度**：已由真实 single-voter raft production path 复现。
- **位置**：`internal/broker/clusterwrite.go:1003-1013`；对照已修的 forwarded closure
  `internal/broker/cluster_forward.go:815-825`。
- **机理**：origin handler 的早期 `MayCreateSession` 不是 authoritative boundary；其后还有 PIN hash 和
  调度，operator revocation 可以先提交。forwarded 请求最终进入的新 closure 会在 leader committed DB
  重检，但一个本来就在 leader 上执行的请求进入 `createSession` 后走另一条 closure，仍直接
  `session.PlanCreate`。因此“旧早检通过 → revoke commit → create proposal”照常提交。
- **独立证据**：`TestLeaderLocalSessionCreateRechecksAdmissionAtTheCommitBoundary` 先 seed/allow/revoke，
  再调用真实 `createSession`；当前返回 `<nil>` 而非 `ErrNotAllowedToCreate`，且 rogue session 行存在。
- **最低修复**：leader-local 与 forwarded 两条 propose closure 必须复用同一个 plan helper，在传入的
  committed `*sql.DB` 上先 fail-closed `MayCreateSession`，再 `PlanCreate`；不能只补 origin handler。

### 10.4 Major

#### R3-M1 — durable terminal 查询按 staged kind 匹配，会追加互相矛盾的第二终态

- **位置**：`internal/jsstream/jsstream.go:469-519`；调用方
  `internal/broker/xfer_inflight.go` 的 single-mode replay 分支。
- **机理**：代码只在 `TransferID == transferID && Kind == kind` 时返回 committed。但本文件自己声明的
  invariant 是“一个 transfer 恰好一个 terminal”，不是“每种 kind 各一个”。如果流里已经有
  `complete`、ledger 留下 `failed`，查询答 false；两个内容派生 Msg-Id 又必然不同，publish 后 history
  同时包含 complete/failed，消费者无法判断真结果。
- **独立证据**：`TestSingleBrokerRecoveryDoesNotAppendAContradictoryTerminal` 先发布 complete，再恢复同 TID
  的 staged failed；stream `Msgs=2`，稳定复现。
- **同 helper 的额外 false-negative**：它把 parent `context.DeadlineExceeded` 与正常 scan EOF 合并为
  false/nil；从客户端 `StartedAt-1m` 的 wall-clock 起扫也把 correctness 依赖于 broker/NATS 时钟偏差。
  注释甚至明确承认 idle 判短会“产生 duplicate”，这与 unknown 必须 retain 的三态纪律冲突。
- **最低修复**：identity 只应是 session+transfer ID，任何 terminal kind 都算已存在；parent ctx 取消/超时
  必须返回 unknown error；不能用有限 clock slack 裁掉可能的 durable evidence。

#### R3-M2 — 每条 staged ledger 查询遗留一个 JetStream ordered consumer

- **位置**：`internal/jsstream/jsstream.go:492-519`。
- **机理**：`stream.OrderedConsumer` 创建 server consumer 后，所有 found/EOF/error return 都没有
  `DeleteConsumer`。恢复按 staged row 调用一次，批量恢复会在 inactive cleanup 前累积同等数量 consumer，
  撞到 stream/operator `MaxConsumers` 后后续查询变 unknown、ledger 永久滞留。
- **独立证据**：`TestTransferTerminalCommittedCleansUpItsScanConsumer` 查到一条真实 terminal 后读取
  `stream.Info`，`State.Consumers=1`，期望 0。
- **最低修复**：取得 consumer name 后在独立、短时、不可被 parent cancellation 取消的 cleanup context
  中删除；cleanup 失败需可观测，不能静默宣称资源已释放。

#### R3-M3 — 泄漏凭据轮换手册不可执行，且同页仍指导部署受影响旧 NATS

- **位置**：`docs/broker-ops.md:54-69,903-918`；实际 CLI
  `cmd/tether/proxy.go:295-306`；同一错误还在 `docs/requirements.md` 与 broker reply 文案。
- **机理**：安全次序 OFF→revoke-all→ON 已写对，但关键第二步调用 `tether proxy sub rm`；CLI 只有
  `revoke`，无 `rm` alias。operator 会在 proxy 已 OFF 时中断，泄漏过的 bearer/PSK 行仍 ACTIVE；以后
  再 ON 就复活旧 keyset。同页安装段还写“nats-server v2.10.22”并长篇论证为何应停在旧版，与实际
  install/Makefile/go.mod 的 v2.14.6 及本报告 R2-B3 安全裁决正面冲突。
- **独立证据**：`TestProxyCredentialRotationRunbookUsesARealCommand` 与
  `TestTheOperatorRunbookDoesNotRecommendAnAffectedServerLine` 均稳定失败。
- **最低修复**：所有可执行文案统一为 `proxy sub revoke`；安装段改成 v2.14.6，并说明 50/95 已在 boot
  wait 修复后对该版本转绿，删除任何把旧 affected/unsupported pin 描述为可接受目标的现行指令。

### 10.5 Minor / 守卫与候选整洁度

#### R3-m1 — 工具链安全 floor 原守卫不比较 patch，CI 仍使用不识别 toolchain directive 的 setup-go v5

原 `TestTheBuildToolchainIsPinnedAtOrAboveTheSecurityFloor` 解析 patch 却只比较 major/minor，因此
`toolchain go1.26.0` 也通过；而 Go 1.26.4、.5、.6 的官方 release notes 都含标准库安全修复。审查者补的
`TestTheBuildToolchainIncludesTheSecurityPatchFloor` 以 1.26.6 为最低 patch，当前 1.26.8 通过。

另外 `.github/workflows/ci.yml` 五处仍是 `actions/setup-go@v5`。setup-go 官方说明从 v6 才原生读取
`toolchain` directive；v5 只解析 `go` 行，所以先装 1.26.0。标准 `GOTOOLCHAIN=auto` 后续通常会切到
1.26.8，因此当前 GitHub hosted release 不是已证实的漏洞构建；但这不是“action 精确钉住 1.26.8”的
机械证明，且 `GOTOOLCHAIN=local` 会忽略 suggested toolchain。本项按守卫/可复现性缺口列 Minor，建议
升级 setup-go 并显式断言 release 使用的 `go version`。

#### R3-m2 — module tidy 与开发者门禁收据不一致

`go mod tidy -diff` 非空：`go.mod` 仍含未使用 `go.uber.org/automaxprocs`，`go.sum` 仍含旧
nats-server 2.10.22/2.14.5、x/crypto 0.55.0 等可删除项。第三轮开始时
`TestTestFunctionInventoryOnlyGrows` 还因漏登
`TestTheBootWaitLeavesRoomForWhoeverIsWatching` 失败；审查者在加入独立测试时一并更新 inventory。
因此 §9 的“当前 make test rc=0 / make gates rc=0”不是从提交给本轮的完整 diff 得出的可复现收据。

### 10.6 独立测试与门禁证据

| 检查 | 结果 | 说明 |
|---|---:|---|
| 开发者定向修复集 | **Pass** | PIN atomic、cluster secret fence、forwarded admission、same-kind durable terminal、四个 boot wait、NATS/toolchain 当前 pin 均通过。 |
| 第三轮 5 个当前候选 reviewer guards | **Fail（预期红）** | leader-local admission、contradictory terminal、consumer cleanup、rotation CLI、runbook NATS floor 均命中上述 finding。另增 patch-floor guard为 Pass。 |
| 排除 5 个红 guard 的 `go test ./... -count=1` | **Pass** | 全包通过；broker 372.814s、cluster 106.403s、clusteroffline 59.127s，其余完整输出均绿。 |
| 触碰包定向 `-race` | **Pass** | authcallout、broker、jsstream、cmd/tether 均通过，无 race。 |
| `make lint` | **Pass** | `0 issues.`。 |
| `make gates` | **Fail** | vet-tags、Darwin cluster build 先通过；architecture runbook floor 与 CLI rotation guard 失败，aggregate rc=2。 |
| `make test` | **Fail（首个 reviewer guard 已输出后会话被中断）** | vet-tags/Darwin build 通过；CLI rotation guard 已红。随后用“排除五 guard 的全仓命令”完整证明其余包，不把中断冒充完整 rc。 |
| `govulncheck ./cmd/tether` | **Pass** | `0 vulnerabilities` reachable；0 imported package vulnerabilities；1 required-module finding unreachable。 |
| module / shell / diff | **Mixed** | `go mod verify`、`bash -n scripts/install.sh`、`git diff HEAD --check` 通过；`go mod tidy -diff` 非空。 |
| deploy drills 50/95 / full sweep | **未由第三轮 reviewer 重跑** | 接受为开发者 §9 的记录，不把它改写成审查者实跑；本轮 finding 均由本机 production-path/静态 gate 独立复现，不依赖 live deploy。 |

本轮外审只新增测试、inventory、tasklist 与本节报告；在审查结论冻结前没有修改生产实现。新增的六个
guard 分别覆盖 leader-local commit、opposite-kind terminal、consumer cleanup、真实 CLI runbook、Go patch
floor 与 NATS runbook floor。

### 10.7 放行前闭合条件

1. 在 leader-local authoritative closure 重检 creator admission，让 R3-B1 guard 转绿。
2. 把 terminal durable identity 改为 transfer 级、unknown fail-closed，并删除 scan consumer，让 R3-M1/M2
   三条运行时 guard 转绿。
3. 修正全部 `proxy sub rm` 与 broker-ops 当前 NATS 版本/裁决，清理 module graph；强化 CI toolchain
   选择证明。
4. 重跑全部 reviewer guards、无排除全仓、相关 race、lint、`make test`、`make gates` 与 diff/tidy。
   live deploy sweep 若未重跑必须保留为开发者证据，不能写成外审实跑。

---

## 11. 审查者获授权修复后的放行复核（2026-09-04）

### 11.1 最终结论

**发布判定：Pass。** 用户在第三轮 `Fail` 结论及开发者修改全部进入 index 后，授权审查者直接修复，
并要求审查者修改全部留在暂存区外。第 10 节的一项 Blocker、三项 Major、两项 Minor 已全部闭合；独立
红守卫全部转绿，无排除的全仓测试、race、lint、结构门和并行端到端矩阵均已通过。没有剩余的已知
发布阻断项。

这一结论只适用于“当前 index + 当前 unstaged 修复”的组合工作树。为了保留责任边界，index 中冻结的
第三轮报告首行仍为 `Fail`，开发者候选及审查资产也仍全部在 index；本节、首行 `Pass` 和下述代码/测试/
文档修复均只存在于 unstaged diff。放行或提交时必须同时纳入这批 unstaged 修复，不能只发布 index。

### 11.2 Finding 闭合

- **R3-B1 CLOSED**：leader-local 与 forwarded session create 现在共用
  `planAuthorizedSessionCreate`，在 Raft proposal 的 committed DB 上先执行 `MayCreateSession`，读失败与
  未授权都 fail-closed，再调用 `PlanCreate`。旧 idempotency 测试补上显式 creator admission 前置条件，
  没有为了迁就测试放松生产检查。
- **R3-M1 CLOSED**：durable terminal identity 改正为 session + transfer ID；从审计流最新 raw sequence
  反向扫描，遇到任一 `complete`/`failed` 都视为已提交，遇到该 transfer 的 start 才确认未提交。扫描不再
  依赖客户端 wall clock、固定 slack 或 staged kind；context、读取和 JSON 错误均返回 unknown，让 ledger
  保留而不是冒险重发。
- **R3-M2 CLOSED**：查询改用 stream raw-message API，不再创建 ordered consumer，因此没有 consumer
  删除窗口、inactive cleanup 延迟或 `MaxConsumers` 累积。新增 unreadable-audit 反例确认损坏证据也按
  fail-closed 三态处理。
- **R3-M3 CLOSED**：当前运维文档、requirements 和 broker 提示全部改为真实存在的
  `tether proxy sub revoke`；broker-ops 当前部署线恢复为 nats-server v2.14.6，并记录 boot-wait 修复后
  50/95 drill 在该版本转绿，不再推荐 2.10.22。
- **R3-m1 CLOSED**：五处 CI 均升级为 `actions/setup-go@v6`，新增结构守卫要求 CI action 能读取
  `toolchain` directive；现有 patch floor 继续钉住至少 Go 1.26.6，实际 toolchain 为 1.26.8。
- **R3-m2 CLOSED**：`go mod tidy` 移除未使用的 automaxprocs 和旧依赖 sums；test inventory 已由仓库
  updater 同步，`go mod tidy -diff`、module verify 与 determinism inventory 均通过。

此外，首次无排除 `make test` 暴露两个只验证 session idempotency/read-back 的旧测试没有建立 creator
授权前置条件，已改为在测试内显式 admission。它还暴露一个既存 spawnsafe 压测的同步缺陷：所谓“慢探测”
使用 512 容量通道，实际被 feeder 预填后即时返回；失效计数又把未健康代际上的 no-op 调用算作 re-arm。
测试现使用真正延迟的无缓冲 probe，只统计已确认健康代际的替换，并以新 probe 数交叉证明替换发生；普通
100 次与 race 20 次压力复跑均通过，同时仍会在 `InvalidateHealthy` 退化为 no-op 时失败。

### 11.3 最终验证证据

| 检查 | 结果 | 说明 |
|---|---:|---|
| 第三轮 reviewer guards + 新 fail-closed/CI guards | **Pass** | broker、jsstream、cmd/tether、architecture 全绿。 |
| 触碰路径 `go test -race` | **Pass** | authcallout、broker、jsstream、cmd/tether；另对 session admission 和 spawnsafe churn 定向复跑。 |
| spawnsafe churn 压测 | **Pass** | 普通 `-count=100`；race `-count=20`。 |
| `make test`（无排除） | **Pass** | rc=0；broker 365.750s，其余全部包、vet tags 与 Darwin cluster build 均绿。 |
| `make gates` | **Pass** | rc=0；结构、确定性、命令树、simcluster hermetic 等全部通过。 |
| `make lint` | **Pass** | `0 issues.`。 |
| `make e2e-parallel` | **Pass** | 最终完整复跑 `ALL PASS`，3m53.202s，17/17 top-level tests represented。 |
| module / inventory / shell / diff | **Pass** | tidy diff 空、module verify、inventory、`bash -n`、`git diff --check` 均绿。 |

端到端矩阵首次运行的产品断言全部通过，但 Go 测试框架在 p2 的 `t.TempDir` cleanup 收到一次
`directory not empty`。对应测试独立 `-count=50` 为 50/50 通过，第二次完整并行矩阵同一
`TestAllPhases/p2` 也通过；没有遗留文件或可复现的退出泄漏证据，因此记录为高负载下测试框架/临时目录
清理偶发，不作为发布阻断，也没有通过放宽产品断言掩盖它。

第三轮 reviewer 没有冒充重跑开发者的 live deploy 50/95；其在 v2.14.6 上的 GREEN 仍明确属于开发者
第 9 节证据。本节的 Pass 建立在审查者实际完成的 hermetic 全仓、race、结构与并行端到端门禁，以及对
每个第三轮 finding 的 production-path 独立守卫之上。

---

## 12. 主进程对「审查者获授权修复」的复核（2026-09-04）

> 按 `CLAUDE.md` §4 的角色边界：审查者这一轮获授权直接改了实现，但**能改实现的仍然只有主进程**，
> 所以他的修改必须经过与外审 finding 同样的逐条评估。本节记录我的裁决、我在其之上做的修正，
> 以及一处**必须订正的证据归属**。

### 12.1 六条修复的逐条裁决

| 修复 | 裁决 | 我核实了什么 |
|---|---|---|
| R3-B1 leader-local session create 授权 | **采纳** | 我上一轮只在 `writeVerbs` 的转发闭包里加了 `MayCreateSession`，`createSession` 自己的 leader-local 闭包仍直接调 `PlanCreate`——同一个不变量有两个入口而只守住一个。`planAuthorizedSessionCreate` 把两条路收敛到一个 planner，这正是我在 R2-M1 里对 `chargedVerifyPIN` 用过的同一条论证（**区分本身就是缺陷的来源**），他用在了我漏掉的地方。单机模式仍走 `sessions.go:57` 的 origin 检查、行为不变 |
| R3-M1/M2 durable terminal 查询 | **采纳机理，重写实现**（见 12.2） | 结论正确：身份该是 session+transfer ID、不该依赖 wall clock、不该留 ordered consumer（默认 `InactiveThreshold` 是 5 分钟，批量恢复时会累积）。但他的**扫描形状**引入了一个更坏的退化，已重写 |
| R3-M3 `proxy sub rm` / NATS 版本 | **采纳，但闸门不够**（见 12.3） | `proxy sub revoke` 确为真实动词（`cmd/tether/proxy.go:298`），三处文案都改对了。broker-ops 恢复 v2.14.6 也对 |
| R3-m1 `actions/setup-go@v6` | **采纳** | 事实核实通过：v6 起 `go-version-file` 才读 `toolchain` 指令。本仓 `go 1.26.0` 是**显式零补丁**，所以 v5 装的恰好是 go1.26.0——正是本仓栽过的那个坑（go1.25.0 / 31 条可达 stdlib 漏洞）。守卫写成 `< 6` 即红的**下限**而不是等值，v7 也放行 |
| R3-m2 `go mod tidy` | **采纳** | `go mod tidy -diff` 干净、`go mod verify` 全部通过、`automaxprocs` 全仓零引用 |
| spawnsafe churn 测试 | **采纳** | 512 容量的 `slow` 通道被 feeder 预填后探测**根本没慢过**，而该测试整个存在意义就是「并发唤醒下的慢探测」；失效计数又把未健康代际上的 no-op 调用算成 re-arm。新版用无缓冲通道 + 真实延迟，只统计**已确认健康**代际的替换，并以 probe 数交叉证明。我复核了它在 `InvalidateHealthy` 退化为 no-op 时仍会红：healthy 恒为真 ⇒ `rearms` 暴涨而 probe 数不变 ⇒ `c < n` 触发 |

### 12.2 R3-M1/M2 的修法我没有采纳：它把一个泄漏换成了一次 reconciler 停摆

他把扫描改成**按原始序列号反向走查**（`stream.GetMsg(seq)`，seq 从 `LastSeq` 递减）。
无 consumer、无时钟这两点是对的，我保留。但成本论证不成立：

- 每个 session 的 history 流装的是 `audit.>`——**call / proc / port 与 transfer 共用一条流**。
  按原始序列反向走查会把**每一条消息的消息体**都取回来，一个序列一次 round trip。
- 它唯一的提前退出是「走到本 transfer 自己的 start 行」。而 `pubAuditTransfer` 是 best-effort，
  **start 行缺失的场景恰恰就是审计 publish 正在失败的场景**，那也正是 ledger 行留存不掉的原因。
- 于是在退化态下，每 5 分钟一次的 reap pass 都会重走**整条 1 GiB 流**；而这个 pass 是
  **同步跑在 reconcile goroutine 上、没有自己的 deadline** 的，node-states（1s 周期）、端口回收
  全部排在它后面。**一个 consumer 泄漏换来了一次 reconciler 停摆，而且发生在最需要 reconciler 的那一刻。**

改成**服务端按主题向前走查**（`jetstream.WithGetMsgSubject`，nats.go v1.52.0 提供 `next_by_subj`）：
只访问 `audit.transfer` 行，不取任何外来消息体；成本从「整条流的字节数」降到「该 session 的 transfer 条数」。
无 consumer、无时钟两条性质原样保留，并且**递减的无符号计数器整个消失**——反向版本靠三处重复的
`if seq == first { break }` 来避免下溢，向前版本的 `seq` 只增不减，结构上不可能下溢。
另加 `transferScanBudget = 30s`：这是**活性**边界不是正确性边界，超时返回 error ⇒ 上游按 unknown **保留**
ledger 行，而不是拿停摆去换一条重复终态。

测试补了三条，并做了变异验证：
- `TestTransferTerminalCommittedReadsOnlyTransferAudit` 钉的是**成本**而不只是答案——它在同一条流里放
  60 条噪声审计 + 4 条 transfer 行 + 5 个已删除序列造洞，然后**数服务端收到多少次 `$JS.API.STREAM.MSG.GET`**。
  变异（退回不带主题过滤的逐序列走查）实测 **200 次 vs 阈值 8**，如实变红。只断言答案的测试看不见这个退化。
- `TestTransferTerminalCommittedTreatsEitherTerminalAsCommitted`：两个方向都测，钉住「任一终态都算已提交」
  ——这是他那条改动里**正确但没有测试**的语义。
- 旧的 `...CleansUpItsScanConsumer` 更名为 `...CreatesNoConsumer`：函数已经不再创建那个需要清理的东西，
  旧名字描述的是一个不再发生的动作。改名让身份 golden 拒绝了自动更新（只增不删），已按规程手改。

### 12.3 R3-M3 的修法我采纳了，但他留下的闸门只覆盖三分之一

缺陷在**三处**：`broker-ops.md` 的轮换 runbook、`requirements.md` 的 N-1 例外、以及
`internal/broker/proxy.go` **broker 自己打给运维的回复串**。三处都是手工改的，而他留下的守卫是
`strings.Contains(broker-ops.md, "tether proxy sub rm")`——**一条单串黑名单，只扫三处中的一处**。

黑名单只回答已经有人问过的那个问题：把同一个错误拼成 `proxy sub delete`、或写进 `usage.md`、
或写进产品字符串，它一个字都不会响。而这三处需要三次手工修复，原因正是**没有任何机械的东西把它们关联起来**
（这也是我记忆里那条反复复发的教训：改一个命令的契约后必须全局扫所有调用点，含产品打印给运维的手抄文案）。

改成从 cobra 命令树**推导**合法动词的闸门 `TestEveryProxyCommandShownToOperatorsExists`：
扫描面 = 被 git 跟踪的 `docs/` 顶层活文档（`docs/reviews/` 的冻结报告合法地引用错误命令作为证据，
`cluster-ha-realmachine-test-plan.md` 未被跟踪，两者按 `docs_layout_test.go` 的同一条规则排除）
+ `cmd/tether/` 与 `internal/broker/` 的非测试 Go 源。别名（`ls`/`list`）与叶子命令的位置参数
（`proxy sub revoke alice`）都不算缺失动词，否则闸门会在正确文本上变红、然后被人删掉。

**变异验证**：把缺陷注回他那条黑名单**够不到**的站点（broker 回复串 `revoke`→`rm`），
新闸门如实变红并点名文件与真实动词——
`internal/broker/proxy.go cites 'proxy sub rm', but "rm" is not a subcommand there; the CLI offers [create ls revoke]`；
**同一次变异下他的黑名单守卫是绿的**。他那条我保留了，它是那次事故确切拼写的廉价回归钉。

### 12.4 一处必须订正的证据归属，和一个我自己搞错的因果

§9 记录的 `50-backup-restore GREEN` / `95-broker-selfheal GREEN` 是在 **boot wait = 90s** 时测的。
其后我在一次全量 sweep 里看到 95 的 `T2c`（poll 90s 等新的 `broker: ready` 行）超时 91762ms，
判断是「**我的实现会安静地等最多 90 秒且不打印 ready**，两个 90 秒正面相撞、余量为零」，
据此把预算改成 30s。**这个因果是错的**，而且我在没有任何 drill 证据的情况下就把它写进了代码注释。

本轮取证（drill 运行中直接读 brk1 的 journal 与 slog）：

```
16:27:43  systemd: Main process exited, code=killed, status=9/KILL      <- T2a 的 SIGKILL
16:27:45  systemd: Scheduled restart job, restart counter is at 2
16:27:45.787  level=INFO msg="broker: ready"                             <- 端到端 2.8s
整轮日志中 "still waiting for JetStream" 出现次数：0
```

**drill 95 杀的是 tether-broker，不是 nats-server**，所以 JetStream 从未缺席，这条 wait 在该 drill 里
**一次都没进过**——90s 也好 30s 也好，它对 T2c 不可能有任何影响。那次红的真实原因是我在同一台机器上
与 drill 并发跑了完整 `go test ./cmd/tether/`（81s，自身还起 NATS）并且留着三个 11 小时的
`drill-40-drain-retire` 残留实例。清干净后同一份代码 `T2c` PASS。

**处置**：预算改回 **90s**（R2-M3 当初的论证 + §9 的 drill 证据都指向这个值），
保留逐次进度日志（那才是真正的改进：让「在等」与「挂死」可区分），
并把那条基于假前提的比例守卫 `TestTheBootWaitLeavesRoomForWhoeverIsWatching` **换掉**——
**一条理由是假的守卫比没有守卫更坏：它靠常绿活下来，还会把假话教给下一个读它的人。**
新守卫 `TestTheBootWaitStaysObservableWhileItWaits` 断言的是真实成立的那条性质：
等待期间**必须持续出声**（行为断言，直接数真实日志行）。变异验证：删掉那行进度日志 ⇒
`emitted 0 progress lines` 如实变红。

**本轮 deploy-tier 实测（30s 预算、并发污染下）**：

| drill | verdict | 备注 |
|---|---|---|
| `50-backup-restore` | **GREEN** | assert_fail=0, pass=87 |
| `95-broker-selfheal` | ASSERT-FAIL → **GREEN** | 并发污染下 `T2c`/`T2f` 红（assert_fail=2, pass=34）；清干净机器后同码复跑 **assert_fail=0, pass=44, not_covered=0** |

最终发布的是 90s 版本，与 §9 GREEN 所用的值一致，且已证明该常量不在这两条 drill 的路径上。

### 12.5 最终门禁与两条如实登记的负载敏感红

| 闸门 | 结果 | 证据 |
|---|---:|---|
| `make test` | **Pass** | rc=0（首轮 rc=2，见下） |
| `make gates` | **Pass** | rc=0；结构预算、分层、命名冻结、身份 golden、simcluster hermetic 闸集全绿 |
| `make lint` | **Pass** | `0 issues.` |
| `make e2e-parallel` | **Pass** | `ALL PASS`，3m51.386s（首轮 rc=2，见下） |
| deploy tier `50-backup-restore` | **GREEN** | assert_fail=0, pass=87 |
| deploy tier `95-broker-selfheal` | **GREEN** | assert_fail=0, pass=44, not_covered=0 |
| `go mod tidy -diff` / `go mod verify` | **Pass** | tidy 空、all modules verified |
| `git diff --check` | **Pass** | — |

**首轮两条红，都不是本增量的回归，都已定性、不掩盖**：

1. **`test/p2` `TestOneContestedProbeDoesNotStallEveryOtherNodesRegister`** —
   `an unrelated node's register waited 105.140275ms`，阈值 100ms。
   同一份代码空闲机器 **30/30 通过**。我实测了它的构成：contested probe **确实发生**
   （每轮 2 次真的投递到静默 incumbent），bystander 在空闲机上约 **21ms**，
   所以 100ms 只有约 5 倍余量，而整树 `make test` 的排队延迟就够吃掉它。
   我**尝试过**把它改成与负载无关的「排序」判据（bystander 必须在 contested 仍在飞时被应答），
   实测推翻了该前提——contested 请求 **17ms 就返回**，并非它自己注释假设的 200ms 预算，
   于是排序判据不成立。**已还原，未改这条测试**：与其在发布前夜按一个我刚证伪的前提重塑一条
   不属于本增量的测试，不如把测量结果留在这里。后续增量应重新审视该测试的判别力
   （200ms 预算的注释与 17ms 的实测不符）。

2. **`test/p13` `TestProxyDisableDuringTunnelDropStaysDown`** —
   `agent lab/lab-1 never reached ONLINE within 3s`。同一份代码单跑 **15/15 通过**。
   这正是 `docs/reviews/parallel-flake-rootcause.md` 已登记的**根因 3（资源分配不均）**，
   且该文「仍未做」第 2 条明写宽 worker 宽度是按 **43 核 / 20 worker** 校准的常数——
   本机是 **88 核**，校准并未重做。`testharness.WaitNodeOnline` 自己的注释也写着
   「busy CI runner 上可能要 1–2s」，而调用方给的是 3s。

两轮 `make test` / `make e2e-parallel` 跑的是**同一棵树**（中间那次 p2 实验在第二轮前已完整还原，
`git status` 为空），所以差异纯粹来自运行间的负载，而不是任何代码改动。

---

## 13. deploy tier 全量复跑（43 条 drill，2026-09-04）

> 用户要求在发布判定前跑完整套 deploy-tier drill，而不是只跑相关的几条。`-j6` 并发，
> 镜像从当前树重烘。**这一节的结论不改变第 12 节的代码裁定，但它推翻了我在 §12.1 里对一条
> finding 的归类**——见 13.3。

### 13.1 首轮结果

| verdict | 条数 |
|---|---:|
| GREEN | 23 |
| INCOMPLETE（只有已声明的 not_covered，assert/setup/product 全 0） | 11 |
| ASSERT-FAIL | 7 |
| SETUP-RED | 1 |
| INFRA-ABORT（rc=124 超时，无 verdict 行） | 1 |

**43 条里 `product_red` 全部为 0**——没有任何一条被判定为产品红。suite `rc=20`。

### 13.2 十条偏离的逐条归因

`run-drills.sh` 在首轮之后自带一个**归因 pass**：把每条偏离**单独重跑**，并且明确
「第一次的结论仍是记录在案的判决，复跑只能追加标签，永远不改 verdict 或退出码」。
预算 3600s 内跑完 8 条，剩 2 条我补跑。

| drill | 首轮 | 归因 | 裁定 |
|---|---|---|---|
| `52-credential-rotation` | ASSERT-FAIL ×2 | REGRESSION | **登记表过期**：首个失败 `A8d` 与 `expected-verdicts-log.md` 2026-09-03 记录的 HEAD 基线签名逐字相同 |
| `60-user-journey` | ASSERT-FAIL ×1 | REGRESSION | **登记表过期**：`J-G.3c-2 FIRST post-login node ls -a`，签名相同 |
| `81-admin-evict-session-rm` | ASSERT-FAIL ×1 | REGRESSION | **登记表过期**：`B3 重连被拒但不是期望的理由`，签名相同 |
| `94-agent-reconcile` | ASSERT-FAIL ×3 | REGRESSION | **登记表过期**：`B3-timeout 孤儿未被 KILL`，签名相同 |
| `67-transient-js-refusal` | INFRA-ABORT | REGRESSION（仅 verdict） | **登记表过期**：与记录同为无签名的 abort |
| `30-rolling-upgrade` | ASSERT-FAIL ×1 | REGRESSION | ⚠ **不是过期，见 13.3** |
| `40-drain-retire` | SETUP-RED | LOAD-SENSITIVE | 单跑 **GREEN(37 pass)**；见 13.4 |
| `74-rebalance-on-return` | ASSERT-FAIL ×2 | （预算耗尽，我补跑） | 单跑 **INCOMPLETE/assert_fail=0/pass=53**，与登记一致 → 负载敏感 |
| `95-broker-selfheal` | ASSERT-FAIL ×1 | UNSTABLE | 三次三个样子；见 13.4 |
| `96-mid-flight-chaos` | INCOMPLETE nc=6 | （预算耗尽，我补跑） | 单跑同为 INCOMPLETE/assert_fail=0/nc=6；登记 nc=5，见 13.5 |

### 13.3 `30-rolling-upgrade`：唯一一条不能用「登记表过期」打发的

它在那六条已登记的「HEAD 就红」名单里，但**这次的首个失败签名不同**——记录是
`PHASE-1 CONTINUITY`，实测是 `UNLOCK-safety`。签名变了就必须重新分诊，读表不算数。

**基线对照**（`git worktree` @ `021c970`、独立镜像 `tether-sim:base021`、单跑）：

```
DRILL-VERDICT verdict=INCOMPLETE rc=4 assert_fail=0 ... pass=53   ← 基线：干净，与登记一致
DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 ... pass=52   ← 当前树：UNLOCK-safety 红
```

所以它**确实是本增量带来的差异**。追下去发现是**有意的**：本轮 H-2 给
`internal/cluster/lock_lease.go` 加了 `PlanExpireUpgradeLease`——HALT 的编排者在退出时
**主动把自己的租约盖成已过期**，同时**保留 marker**。这个拆分正是 H-2 的全部意图：

- marker 留着 ⇒ `join`/`retire` 在半滚状态下继续被围栏（drill 的 `UNLOCK-precond` 依然 PASS，就是这一半）
- 租约释放 ⇒ HALT 提示语承诺的「修好后重跑」当场成立，而不是要等满 `LockLeaseTTL`(15 min) 加一个
  回收周期；同一个拒绝此前也堵死了紧急 `--to-version <older>` 回滚

而那条断言的前提——「编排者刚死几秒，可能还有人在续租」——正是被这次变更**有意作废**的。
**安全性没有丢**：活着的持有者每 `LockLeaseRenewInterval` 续租一次，租约在它驱动期间永不落到过去，
默认 clear 依然拒绝它。

**drill 已更正，但不是为了变绿**：那条断言想保护的性质被拆成两半——

- 能在这条 drill 上构造的那半改为断言新行为（`UNLOCK-halt-admits`：HALT 后默认 clear 成功，
  且 rc=0 仍要求 post-clear 复探），注释里写明它此前断言的是相反的事、以及那时为何是对的；
- **不能构造的那半**（活租约的拒绝、`--force` 的覆盖）用 `not_covered ... gap` **明确登记为缺口**，
  并指名它搬去了哪里：`cmd/tether/cluster_unlock_test.go` 的 `TestUnlockRefusesALiveLease`
  与 `TestUnlockForceOverridesALiveLease`（两者均已确认存在）。

复跑证明：`INCOMPLETE rc=4 assert_fail=0 product_red=0 pass=52`，三条 unlock 断言全 PASS。

### 13.4 `40` 与 `95` 的书面裁定（harness 明确要求，不能拿「单跑绿了」当免死金牌）

**`40-drain-retire`（LOAD-SENSITIVE）**：首轮 `SETUP-FAIL grow_to_3 failed`，单跑 GREEN(37 pass)；
同一轮 `10-grow-to-3` 首轮就是 GREEN，说明 grow 本身没坏。**裁定：环境，不阻断发布。**
依据是 `-j6` 下 6 条 drill 的容器同时做 raft+SQLite 写。

**`95-broker-selfheal`（UNSTABLE）**：三次跑出三个样子——首轮红在 `D6b`（boot resume 后 DELETING
会话未消失，poll 90s 超时），harness 单跑红在 `T2c`+`T2f`+`D0`，而我当天在**清空容器后**单跑是
**GREEN(pass=44, not_covered=0)**。**裁定：wall-clock 轮询对磁盘延迟敏感，不是产品回归。**
证据有两条，缺一不可：

1. 失败证据里的宿主机读数 `fsync_4k_ms: 15.321`——4K fsync 慢到 15ms，raft 每次提交都要付这个代价，
   「boot resume 完成一次删除」这种多提交操作因此撑爆 90s 轮询；
2. 我在 drill 运行中直读 brk1 的 journal 与 slog 拿到的时序：`SIGKILL 16:27:43 → broker: ready
   16:27:45.787`，**端到端 2.8 秒**，全程零条 `still waiting for JetStream`。产品自愈本身是秒级的。

**这两条都不是「重跑就绿了」**。它们仍作为 DEVIATION 响着，不写进 `expected-verdicts.tsv`——
一条登记为 expected 的红就不再是信号。

### 13.5 `96-mid-flight-chaos` 的缺口计数漂移

单跑与首轮一致（INCOMPLETE / assert_fail=0 / nc=6），登记为 5。六条声明式缺口逐条为：
`#58 cross-home GC reap`、`arm B + arm C`、`96-A #57 in-flight interruption`、
`96-B0 run --ack-alerts gate`、`96-D6b canary3 durable majority commit`、`96-F double fault`。
登记表的注记把多出来的那条归因于「`-j6` 下的 #71」，但它**单跑同样出现**——该注记的措辞不准确，
在此如实订正，留给下次校准。

### 13.6 这一节对发布判定的意义

- **没有任何一条 drill 判定为产品红**（43/43 `product_red=0`）。
- 六条 REGRESSION 里五条经签名比对确认是**登记表过期**（表上次校准是 2026-08-19，此后无人跑过全量）。
- 第六条经**基线 commit 复跑**确认是本增量的**有意行为变更**，drill 已按变更更正并把不可构造的
  半边登记为 gap。
- 两条负载敏感、一条不稳定，均已给出书面裁定与证据，且**没有**被写进期望表。
- 升级族四条 `30`/`31`/`32`/`33`：`31`/`33` 的 not_covered 与登记数逐条相等，`32` GREEN，
  `30` 见 13.3。
