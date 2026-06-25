# B3 plan（定稿）— 迁移/扩容/确认人机工程

> Stage-A：9×Opus 对抗 workflow。主进程采纳综合 plan（已据实核验 + 采纳 critic 修正）。完整细节见
> workflow 输出（`tasks/wzayo7pal.output`）；本文件记录绑定决策 + 结构 + 测试 + 顺序。
> **硬约束确认**：无 proto wire 改、无 ACL 改、非集群 broker 字节等价；唯一 wire 增量是 adminsock（本地 socket）`Request.Force` + `CodeRemoveOwnsResources` 常量；不弱化任何现有硬门（typed-confirm / --confirm-peers-dead / F==0 / RemoveNode phase-gate / natsconf preflight）。

## 范围决策（采纳 critic）
- **item7 重定为仅 `VOTER_ADD_FAILED`**：`RemoveNode` 只接受 `RETIRING`/`VOTER_ADD_FAILED`，而 RETIRING 已被 `DrainNode→migrateExposes` 迁走 expose，故 ownership 拒绝只对 VOTER_ADD_FAILED 有意义 + 现有拒绝改措辞。`--force` 只绕新 ownership 探测、**绝不**绕 phase-gate（D7 silent-fork 不动）。
- **item2 砍成只读 doctor**：**无** DB-copy 迁移 dry-run（mutation 风险 → B6）、**无** doctor 内二次 `nats-server -t` 重建（takeover 已 swap 前校验）、**无** 总在 FDE advisory 上 fail 的 `--strict`、**无** init 外层 preflight 拒绝（内层 `InitFromExisting` 已 fail-closed）。
- **item3 alias 不改名**：`init` 仍主、加 `migrate-to-cluster` 别名（零 D9-doc 分歧）+ `--check`/`--dry-run`（廉价、零 mutation）。
- **item1 conf 交叉校验**：文件派生 nkey 与 `nats.conf` `own.AuthIdentity()` 不符 → 不替换、回 placeholder + 响亮 NOTE（唯一会印错命令的角）。
- **item4**：plain `drain` **不**加 `--yes` 拒绝器（可逆/分层）；只在恒 Tier-2 的 `remove`/`force-single`/`recover`/`init` 拒 `--yes`。`alert ack` synthetic 守卫做 RunE 第一句（先于 session/connect）。
- **`confirmTypedNodeID` 重构读 `cmd.InOrStdin()`**（正路可单测 + `--yes`-拒绝负断言非 TTY-拒绝串），签名加可选 consequence；6 个 caller 同改。`TestAlertAckRequiresActiveSession` 改用 store-backed key。

## Item 1 — init/add/sign-join 印真值命令
- 新 `cmd/tether/cluster_secrets.go`（纯 read+render helper）。
- **(1a) init NEXT 块**（在迁移节点本机，secrets 全本地）：`auth.PublicKeyFromSeed(account.nk/broker.nk)`→`A…`/`U…`（用现有原语、不加 auth API）；**KIND 校验**（`nkeys.IsValidPublicAccountKey/IsValidPublicUserKey`）；**conf 交叉校验**（`natsconf.Preflight(conf)→own.AuthIdentity()`）不符→placeholder+NOTE；任何读/解析/perm 错或不符→该值留 `<…>`+NOTE；**print 在 success 后、错误绝不令命令失败**（exit 仍 0）；任一值是 placeholder 则 header 说"填下面 `<…>`"。
- **(1b) sign-join 印完整 re-run 行**（在 joiner）：stdout **唯一**内容 = 完整可贴 `cluster add … --join-token <nonce>:<sig> …`，人类框架（`Re-run on LEADER:`/node-pub/cert-fp）移 stderr；可派生 reals（node-pub、cert-fp via 导出 `clusteroffline.TunnelCertFingerprint`、nonce:sig）+ addr 来自**显式 flag**（`--raft-addr` 必填、否则拒发行；无 `broker.yaml` 自读）；cert 读失败→token 仍发 + `--cert-fp <sha256:…>` placeholder + WARN（绝不半个 fp）；私钥永不印。
- **(1c) add step-1 nonce reprint**：leader 无 joiner secrets，改为指向 `sign-join`（删 Draft4 幻觉的 `cluster show-join-identity`）。

## Item 2 — `cluster doctor` 廉价只读可见 preflight
- 新 `internal/clusteroffline/doctor.go`（leaf：read-only storage/natsconf/auth/net；无 raft、无 live NATS）。检查→`Check{Name,Status:PASS|ADVISORY|FATAL,Detail}`，全非变更：secrets+perms（SecretsPreflight）/ FDE advisory（**可见 stdout 行**，修 stderr/stdout 分裂）/ identity 完整性（复用 `missingClusterInitFields`+`net.SplitHostPort`）/ cert-fp 一致（vs seeded self row）/ **端口可绑分类**（raft-addr 占用=FATAL；nats-route :6222 占用=ADVISORY「将停的 broker 持有，预期」；非 EADDRINUSE=FATAL）/ natsconf unknown-directive 拒（`natsconf.Preflight`）/ DB **OpenReadOnly + schema pre-cutover 校验**（无迁移、无 copy）。tabwriter `CHECK STATUS DETAIL` + summary；FATAL→非零（B2 exit class）。`--json{checks,summary}`。**`--strict` DEFERRED**（FDE 总 advisory 会恒非零）。**init 不外层拒绝**，只印 `# tip: run tether cluster doctor first`。

## Item 3 — 命名 + dry-run（不改名）
- `init` 主 + `Aliases:["migrate-to-cluster"]`；Short/Long 改述「ONE-TIME 把本机活单 broker 迁成单 voter（6 步迁移第 2 步、先跑 doctor）」；`--check`(别名 `--dry-run`) 跑 item2 引擎 + 只读 DB 校验、**不 mutate**（无 flock-mutate/.bak/迁移/bootstrap）退出。

## Item 4 — 两档确认 + 一致错误
- 两档表：Tier-1 可逆（plain drain@F>0 / transfer-leader / rotate-tunnel-cert，无 confirm、无 `--yes`）；Tier-2 不可逆/影响 quorum（remove / drain --retire@F==0 / force-single / recover / init，TTY 输 node-id、**拒 `--yes`**）。
- **(4a)** `confirmTypedNodeID(cmd, want, consequence)` 读 `cmd.InOrStdin()`、consequence 在 prompt 前印。
- **(4b)** 恒 Tier-2 加 hidden `--yes`（仅作拒绝、RunE 第一句）：`usageErr("cluster X cannot run unattended: 不可逆/影响 quorum，需人 TTY 输 node id；设计上无 --yes 覆盖…")`。
- **(4c)** `alert ack` 守卫（RunE 第一句、先于 session/connect）：`quorum_lost`/`force_single_active` → `usageErr("…是实时合成的 cluster-health 条件、非 store-backed dedup_key；不能 ack；--ack-alerts 也不 ack/不修、只强推一条命令；真修是恢复 quorum，runbook §3")`。collateral：`TestAlertAckRequiresActiveSession` 改 store-backed key。
- **(4d)** usage §5.6.x「确认机制如何工作」单节 + §5.7 交叉链。

## Item 5 — Example 块 + 上下文分组
- `newClusterCmd` 加 4 个 cobra Group（ASCII 危险标记、无 Unicode）：`online`{status,add,drain,remove,transfer-leader,rotate-tunnel-cert} / `migrate`{init,takeover-natsconf,doctor} / `escape`{force-single,recover}（标 DANGER） / `local`{sign-join,node-pub,keygen}。14 命令全加 `Example:`，load-bearing（add 含 node-pub 前置 + 两次 leader call + takeover 收尾；force-single 含 --confirm-peers-dead）端到端完整。

## Item 6 — add success 可执行「NOT DONE YET」提醒
- 成功后 stdout 印：raft 改了、NATS mesh 没；列**真实节点表**（标 leader、leader 最后 + 理由）+ 印 `--peer` 三元组（每 peer `name,nats_route,bus_nkey`，leader 上 `cluster_nodes` 可得；不可得则 `<…>`+note、绝不编造）+ `cluster status` 验证 + runbook §1。

## Item 7 — remove refuse-by-default + --force（重定）
- `RemoveNode(nodeID, force)`：`phase==phaseAddFailed && !force` → `countOwnedExposes`（`BoundedStaleRead` 单 `SELECT COUNT(*) FROM port_allocations WHERE home_broker=? AND state='ALLOCATED'`）`n>0` → `ErrRemoveOwnsResources` 指向 drain --retire 或 --force。现有 `:186` 拒绝串改响亮（REFUSED + prefer drain --retire）。`adminsock.Request.Force`(omitempty,本地)；dispatch 传 `req.Force`；`clusterCodeFor` 加 `"still HOMES"`→新 `CodeRemoveOwnsResources`（**字面串 author 一次**、错误串+clusterCodeFor+D7 pin 共用），映 B2 precondition/usage exit class；CLI `--force`。typed-confirm 不受 `--force` 影响。

## Item 8 — 安全语义可见（additive、不碰门）
- **(8a)** `drain --retire` success stderr：`NOTE: retire 是拓扑改、非凭据撤销；退役机仍持 account.nk+CA（共享、未轮换）可继续鉴权；疑似泄漏请立即轮换（runbook §2.1）`。plain drain 无。
- **(8b)** `force-single` Short 前置劈脑裂；consequence 经 `confirmTypedNodeID` 在 prompt 前印；`checkPeersDead` 不动。
- **(8c)** `takeover-natsconf` Short 前置安全网（`nats-server -t` 校验 + .bak + 拒 unknown directive）。
- **(8d)** **先**写 `docs/cluster-runbook.md` `### 2.1` 锚（所有 §2.1 交叉引用解析前置）。

## 测试（表驱动，未注明=unit/make test）
- item1：`cluster_secrets_test.go`（happy 派生；**全 stdout+stderr 扫不含任何 seed token**；garbled/wrong-KIND/perm-denied→placeholder+NOTE+exit0；**file/conf 不符**→placeholder+reconcile NOTE；header 不说 copy-paste-ready 当有 placeholder）；`cluster_signjoin_test.go`（全 flag→stdout 唯一一行；garbled cert→token+WARN+`<sha256:…>`；缺 --raft-addr→拒发；缺 node-ident.nk→先于 print 报错）。
- item2：`internal/clusteroffline/doctor_test.go`（all-clean 全 PASS+FDE 可见 ADVISORY；FATAL 不早退仍评全部；**端口两分支**+非 EADDRINUSE FATAL；identity 缺/坏→FATAL；cert-fp 不符→FATAL；**DB 读前后 hash(db+-wal+-shm) 不变**证零 mutation；unknown directive→FATAL；--json 形状）。
- item3：`cluster_init_test.go`（init 与 alias 同 RunE；`--check` 零 mutation[无 .bak/raft/、DB hash 不变]）。
- item4：`cluster_confirm_test.go`（4 个 Tier-2 拒 `--yes`→usageErr/exit64/不到 prompt/非 TTY-拒绝串；**plain drain --yes@F>0 不拒**回归门；typed-confirm 正路 `cmd.SetIn`；`alert ack quorum_lost`→explain、**无 session 读/无 NATS dial**；`--force`→`Request.Force`）。
- item5/6/8：`cluster_help_test.go`（每子命令非空 Example；add success 含 "NOT DONE YET"+"leader LAST"+节点表+`--peer ` 三元组；Short：force-single 劈脑裂 / takeover 安全网 / drain --retire stderr "NOT a credential revocation"+§2.1，plain drain 无；4 组 ASCII 标题渲染）。
- item7：`clusterdrain_test.go`（VOTER_ADD_FAILED+N>0 manufacture+断言前置→`ErrRemoveOwnsResources`；force=true 过；N==0 过；非 ALLOCATED 不计；live VOTER 先撞 phase-gate 无视 --force；`clusterCodeFor`→`CodeRemoveOwnsResources` 钉 exit class）。
- gated `test/d9`（1 drill）：活单 broker → doctor PASS → `init --check` PASS → 真 init → **断言 NEXT 块印替换后（非 `<placeholder>`）的 account/broker pubkey 且经 conf 校验** → takeover 经 `nats-server -t` 校验。`nats-server` 缺则 `exec.LookPath` self-skip。

## 顺序
1. 前置（叶子、零行为改）：导出 `clusteroffline.TunnelCertFingerprint`；**runbook §2.1 锚**（先于任何引用串）。2. `confirmTypedNodeID` 重构 + 6 caller。3. item1 helper（`cluster_secrets.go`）。4. item1 wiring（init NEXT / sign-join / add step-1）。5. item2 doctor 引擎 → item3 `--check`/alias 复用。6. item7 broker core→wire→CLI。7. item4 机制（4 Tier-2 拒 --yes；alert ack 守卫 + collateral 测试）。8. item6 + item8 wording。9. item5 组 + Example（最后、碰每 constructor）。10. usage §5.6.x + §5.7 + runbook §1 mesh lockstep。11. 测试随码。
- **lockstep**：`takeover-natsconf --peer` full-mesh 形态 author 一次、item1 NEXT/item5 add Example/item6 add-success/runbook §1 共用，与 D9 runbook §1 真形态一致。

## DEFERRED → B4/B5/B6
DB-copy 迁移 dry-run（B6）；`doctor --strict`（B5）；doctor 内二次 nats-server -t；migrate-to-cluster 真改名；init 外层 preflight 拒绝；mutating verb 的 --json；CLI 轮换 account.nk/CA（8a 仅警告）；Tier-1 加 `--yes` no-op。

## 出口标准
内审通过 + 硬闸（lint/make test，触碰 cluster 接缝跑 gated d7/d9）；外审统一留最后。
