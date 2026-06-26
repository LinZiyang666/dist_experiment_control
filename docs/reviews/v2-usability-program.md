# v2 易用性改造程序（program backlog + 分批序列）

> 触发：外部用户反馈"v2 cluster 太麻烦"（`../v2-usability-proposals.md`，7 条建议）。
> 本轮在那 7 条之外，另用 4 个独立视角 agent（产品经理 / 非专家新用户 / Ops-SRE / 集成自动化）对 v2（distributed-broker HA, D0–D9, proto v2）做易用性审视，合并去重后得 ~57 条 finding。
> 用户决策：**全部落实**。本文件把全集分解为可独立交付的叶子批次 B1–B7，逐批走 CLAUDE.md §3 的 3 阶段（workflow 起草 plan → 主进程实现+测试 → 独立 agent 闭合核验 + 用户外审），一次一批、先父后子。
>
> finding 溯源：`PM#n`=产品经理视角，`NEW#n`=新用户视角，`OPS#n`=Ops/SRE 视角，`AUTO#n`=自动化视角，`DOC#n`=`../v2-usability-proposals.md` 原 7 条。

## 0. 跨视角主线（多个 agent 独立撞到 = 最强信号）

1. **`cluster status` 对真正受影响的人不可见 / 全黑话 / 无白话判定 / exit code 串味**（PM#1·#9, NEW#5·#15, OPS#5, AUTO#2·#7·#15）。
2. **错误信息把惊慌的终端用户指向"核按钮"**（`force-single`/`recover`）+ 内部 token（`home_catching_up`）裸漏（PM#1·#14, NEW#4·#2）。
3. **几乎没有机器消费面**：15 个命令仅 2 个 `--json`；adminsock 响应无错误 `Code`；退出码 1 bit；无 metrics/health；告警纯 pull（AUTO#1·#2·#3·#13·#14, OPS#1·#2）。
4. **迁移/扩容是"占位符模板"仪式，无 dry-run、无 resume**（PM#2·#3·#11, NEW#7·#8, AUTO#4）。
5. **确认机制四套并存、互相打架**（`--yes` / 输入 node-id / `--ack-alerts` / `alert ack`）（PM#8, NEW#9）。

已核验的需求一致性缺口：`expose --no-rebuild`/`--on-broker` 不存在（ExposeReq 无字段）；`ExposeResp` 无 `home_broker`/`epoch`；`tether alert` 仅 `ls`/`ack`（无 `raise`/`clear`）。

## 1. 分批序列（一次一批，先父后子）

### B1 — 用户可见的集群状态 + 安全的错误叙事（主线①②，Tier-1 核心）
- ctl 经 NATS 可达的 `cluster status`（复用 D8 `ctrl.by.<actor>.cluster-health` 响应器），返回**用户级**摘要而非操作员逃生命令（PM#1）。
- `cluster status` 列图例 + `LAG/ACCT.NK/STREAMS x/y/REACH` 单位说明；`--explain`/legend（NEW#5）。
- 按 voter 数的**白话判定行**：N=1「无冗余」/ N=2「无容错写」/ N≥3+streams-at-target「HA active」（NEW#15）。
- `view_host` + `is_leader_view` 标注；非 leader 上提示"重跑于 leader 拿权威判定"（OPS#5）。
- `gateDestructive` 改指 `status`+runbook，**不再**对普通 ctl 用户喊 `force-single`/`recover`（NEW#4, PM#14）。
- `home_catching_up`/`leader_unavailable` 上 usage §9.4/§9.7 词条 + ctl 友好渲染（NEW#2）。
- 单 broker 仍是默认/受支持路径的显著 callout（usage §1/§2.3）（NEW#1）。
- 平面语言"什么是集群/quorum"小节（NEW#13）。

### B2 — 机器消费面：`--json` + exit-code taxonomy + 错误码（主线③ CLI 部分）
- `--json` 补齐：`node ls`、`ps`、`session ls`、`alert ls`（必带 `dedup_key`，与 `alert ack` 闭环）、`expose`、`admin sessions/nodes/audit`、`transfer`、`node upgrade --all`（每节点结果 map）（AUTO#1·#12·#14）。
- 统一 `schema_version` 于所有 `--json` 载体（AUTO#13）。
- 进程级 exit-code taxonomy（usage/bad-arg、service-unavailable、permission、transient…），与 `cluster status` 的 0/1/2/3 健康码分离；transport-down 不再和 DEGRADED 都 exit 1（AUTO#2, PM#9）。
- adminsock `Response` 加稳定 `Code` 枚举（not_leader/already_voter/catch_up_stalled/…）；CLI 映射 code→exit、code→hint（AUTO#3）。
- `cluster status --json` 不再吞 `Error`；加 `Errors[]`/`Partial`（AUTO#7）。
- offline/online exit-2 语义统一或显式标 `view`（AUTO#15）。

### B3 — 迁移/扩容/确认人机工程（主线④⑤）
- `init --from-existing`/`cluster add`/`sign-join` 打印**真值替换**的下一步命令（读真实 account/broker nkey、cert-fp、本机 addr），而非 `<placeholder>`（PM#2·#11, NEW#8）。
- `cluster doctor`/`cluster preflight`：在不可逆的 `systemctl stop` 之前跑**全量非变更检查**（端口可绑、route 可达、DB 迁移 dry-run、cert-fp 一致、nats.conf `-t`）；advisory 可见、`--strict`（PM#3, NEW#6）。
- `init` 改名反映"migration"语义（保留 `init` 隐藏别名）；`--check`/dry-run（PM#2, NEW#7）。
- 确认机制统一：文档化两档策略（可逆 fleet → `--yes`+预览；不可逆/影响 quorum → 输入 node-id，永不 `--yes`）；`--yes` 传给 typed-confirm 命令时给有用报错；`alert ack` 作用于 synthetic gate 时解释"不能 ack"（PM#8, NEW#9）。
- 每个 `cluster` 子命令加 `Example:` 块 + 按上下文分组（online/offline/migration/local-crypto）（PM#7）。
- `cluster add` 成功后打印"尚未完成：还需在所有节点重跑 takeover-natsconf（leader last）"（NEW#8）。
- `cluster remove` 在仍持 expose/stream 时 refuse-by-default，导向 `drain --retire`（NEW#14, PM#8）。
- 安全语义可见：`drain --retire` 成功后提示"retire ≠ 凭据撤销"（NEW#10）；`force-single` Short 前置劈脑裂风险 + 内联后果（NEW#3）；`takeover-natsconf` Short 前置安全网（NEW#12）。

### B4 — 需求一致性补缺（叶子功能补全）
- `expose --no-rebuild`（默认 rebuild ON）+ `--on-broker <node>`：ExposeReq 加字段 + agent 透传 + 落 home pin（PM#4）。
- `ExposeResp` 加 `home_broker`/`epoch`；`ps` 行展示；`expose explain <name>`（DOC#5 的结构载体 + 命令）（PM#5）。
- `tether alert raise --kind manual --severity … --message`/`alert clear <dedup_key>`：操作员本机 admin-socket verb（PM#6）。

### B5 — Ops day-2 可观测性（轻量，多为接已算好的值）
- Prometheus metrics 端点 `--metrics-listen`（leader/voters/quorum-margin/applied-index/peer-lag/stream-replicas/alerts-active/force-single）+ `/healthz`+`/readyz`（OPS#1）。
- 结构化/JSON 日志 + `--log-level`/`--log-json` + 关键路径 slog 属性（node_id/term/applied_index/reqID）（OPS#8）。
- `cluster status --watch`（OPS#5）。
- async 命令 `--wait`/`--timeout` 收敛（`add`/`drain`/`transfer-leader`/`rotate-tunnel-cert`）+ `cluster wait <node> --phase`（OPS#13, AUTO#5·#6）。
- cert 轮换可见性：`cert_fp_current/prev/valid_until` + `agents_repinned N/M` + 临期告警（OPS#7）。
- 容量信号：`disk_free_pct`/`ports_used/total`/`js_store_used_pct` 进 status + health 降级带（OPS#9）。
- maintenance/cordon 模式：抑制计划内重启的误 page + 停新放置（OPS#6）。
- `takeover-natsconf --plan`（dry-run 打印 ownership diff + leader/quorum/agent 影响 + 重启顺序提示）+ `--json{changed,...}`（OPS#10, AUTO#10）。

### B6 — Ops day-2 重型 / 净新子系统
- `cluster backup`/`restore` + 多节点 DR runbook（OPS#3）。
- 告警 push/webhook（leader 侧 transition-gated POST）（OPS#2）。
- 集群滚动升级路径 + 节点 binary/proto 版本进 status + `cluster add` 版本 skew 拒绝 + runbook §5（OPS#4）。
- `cluster export-incident`（audit/alert-history/membership timeline bundle）（OPS#12, AUTO 对 DOC#6 的补充）。
- recover→init→re-add 身份 manifest 重放（`recover --dump-divergent` 同时出 `init --from-manifest` 清单）（OPS#11）。
- 破坏性命令的可审计机器确认逃生口（`--confirm-node-id` + env gate，仅 attended automation）（AUTO#8）。

### B7 — 文档原 7 条大件（DOC#1–#7，最大、最后）
- DOC#3 signed broker roster + agent 自动发现（bootstrap URL 取代静态 broker_url）+ `agent join <invite>`+`agent doctor`。
- DOC#1 topology reconciler（Raft 记期望拓扑 + 每 broker 本机 reconciler 渲染 nats.conf 滚动重启）。
- DOC#2 broker join/retire operation controller（可恢复 operation + 状态机 + `cluster ops ls/show`）。
- DOC#4 proxy cluster 化（proxy 作系统管理的 expose，home/epoch + freeze-on-quorum-loss 降级语义）。
- 声明式 `cluster apply -f roster.yaml`（AUTO#9，与 DOC#1/#2 合流）。
- DOC#7 recovery/force-single guided 向导产品化。
- DOC#5 expose/rehome 可观测性事件 + 告警（B4 落结构载体后，这里补事件流）。
- DOC#6 cluster status 产品化收口（状态卡 + `doctor` + `incident export`，与 B1/B5 合流验收）。

## 2. 验收（每批一致）
- CLAUDE.md §3 3 阶段：现场 workflow 起草 plan（多视角对抗，全 Opus 4.8、静态 agent 数、省略 model）→ 主进程定稿 `docs/reviews/<batch>-plan.md` → 主进程实现+测试 → workflow 对抗审查 `docs/reviews/<batch>-review.md` → 用户外审 `docs/reviews/<batch>-external-review.md`。
- 不变量以 `architecture.md`/`distributed-broker-architecture.md` 为尺；**非集群 broker 全程字节等价**仍是硬约束；任何 wire/ACL 变更走 proto SSOT。
- 提交前硬闸 `make test`+`make lint`（并发改动加 `-race`+内建 leak 门）；触碰 cluster 接缝跑相关 gated 套件（`-tags dN_integration`）。
- 一次一批、停下等用户外审/确认再进下一批。

## 3. 当前进度
> 注：本轮 goal = 一个一个 B 完整走流程（plan→实现→Stage-C 审查→采纳），**删每批外审**；外审统一留到全部 B 实现完之后。
- [x] B1 用户可见状态 + 安全错误叙事 — **完整流程完成**（plan+impl+Stage-C 审查+采纳+硬闸全绿）。报告 `b1-{plan,review}.md`。
- [x] B2 机器消费面（--json + exit codes + 错误码） — **完整流程完成**（plan→实现→Stage-C 审查→采纳 2 MAJOR+8 MINOR/NIT→硬闸绿；d7 gated flake 经 clean-HEAD 验证非回归）。报告 `b2-{plan,review}.md`。实现：exitcode taxonomy(64/69/70/75/77) + `--json`×8 + adminsock `Code`(+clusterCodeFor) + Item5 不吞 Error + usageErr。**B2.1 延迟**：upgrade-all/transfer --json、自由文本 cluster 错误 sentinel 拓宽、session create Code 字段、golden 默认文本锁。
- [x] B3 迁移/扩容/确认人机工程 — **完整流程完成**（8 item 实现 → Stage-C 审查 → 采纳 1 MAJOR 代码缺陷 + 4 MAJOR 测试缺口 + MINOR/NIT → lint 0 / make test 绿）。报告 `b3-{plan,review}.md`。
- [x] B4 需求一致性补缺（expose flags / alert raise-clear / ExposeResp 字段） — **完整流程完成**（plan→实现→Stage-C 审查→采纳 D1 真缺陷[drain-marker 窗口]+J1 MAJOR[explain not-found exit 64]+m1/m5/m6/m2-b MINOR/test→lint 0 / make test 全包 + gated d6/d8(-race) 绿）。报告 `b4-{plan,review}.md`。实现：ExposeReq +rebuild_off/on_broker、ExposeResp +home_broker/epoch、PsPortEntry +3（全 additive omitempty、ProtoVersion 不 bump）；port Allocate/PlanAllocate 条件列保字节等价；`--on-broker` 校验（Eligible+CertFP+非 draining、拒绝不写行）；`expose explain`（复用 ps RPC、诚实 footer）；ps HOME 列（单 broker 字节不变）；operator `alert raise/clear`（admin socket、poison-safe、manual-only、零 ACL 改）。**残留**：m2-a/m3 正向 gated 测试（需真 node→d6/d8）记 refinement；gated d7(-race) 既有失败经 clean-HEAD 验证非回归。
- [x] B5 Ops day-2 可观测性（轻量） — **完整流程完成**（plan→实现 7 IN 项→Stage-C 审查→采纳 2 BLOCKER[--watch --json JSONL；/readyz self-phase]+测试缺口[wait/watch/--plan]+MINOR/NIT+5 deviation 裁定→lint 0 / make test 全包绿）。报告 `b5-{plan,review}.md`。实现：OPS#8 `--log-level/--log-json`(newLogger)；OPS#1 `internal/brokermetrics` /metrics+/healthz+/readyz(`--metrics-listen` off 默认零 wiring、cheap accessor+lastObserve 缓存、不碰阻塞 StatusReport、/readyz self-VOTER+raft-config 一致性)；OPS#5 `cluster status --watch`(≥2s、SIGINT、JSONL)；OPS#13 `cluster wait <node> --phase`(GONE、timeout→75)+`transfer-leader --wait`；OPS#7 cert 可见性(CertFP/Prev/ValidSecs omitempty + near-expiry advisory，不改 health)；OPS#9 disk/ports self-row(`home_broker=selfID` scoping)；OPS#10 `takeover-natsconf --plan`(零 mutation+--json)。**DEFER-B6**：disk degrade-band(值已暴露、disk_pressure 告警覆盖)、cluster-level stream gauge、add/drain --wait(cluster wait 覆盖)、3 flag 的 yaml 键。审过 → git add 暂存。
- [ ] B6 Ops day-2 重型 / 净新子系统 ← **Stage-A 定稿（`b6-plan.md`），单批次一次做完（不拆，零中途外审）**。安全核心先做（backup/restore + recover→manifest + version-skew-reject + machine-confirm），增量后做（alert webhook + export-incident + version-in-status/升级 runbook + B5 cheap folds: disk/ports band、streams gauge、broker.yaml 键）。规划抓到真缺陷：restore-index 腐蚀（FSM index-skip 吞 restore 后写 → restore 必重置 applied_index=0）。零新 migration。**Stage-B 实现完成（12 步全做完）+ make test/lint 绿**；待 Stage-C 对抗审查。
  - 安全核心：`internal/clusteroffline/{manifest,backup,restore}.go`（共享 Manifest + allowlist helper、offline backup、restore[index-reset=0 + staging verify→migrate→verify→normalize + 4 层 provenance + 清非 self roster]）；`cluster.Node.BackupDBTo` + `cluster.{BackupDBFile,VerifyIntegrity}` seam；machine-confirm `allowMachineEscape` per-call-site（仅非 F==0 remove 可逃、flag+env 双钥）；OPS#11 recover `--emit-manifest` + `init --from-manifest`（cert_fp live 重导）；A3 version-skew（proto 硬拒/release advisory，gate 在 claimJoinNonce 前、`version_skew`→exit 64）。
  - 增量：online backup（adminsock `OpClusterBackup`、leader gate 前、任意节点）+ CLI `cluster backup [--offline] / restore`（typed-confirm）；`export-incident`（只读组装、allowlist + denylist scrub）；alert webhook（committed-delta seam、bounded worker、loopDone cap 3→4、http/https+拒 userinfo、`broker.observability.alert_webhook_url`）；version-in-status VER 列；disk/ports DEGRADE band；streams gauge；broker.yaml observability keys；runbook §5 DR + §6 滚动升级。
  - 活证（make test，-race）：restore index-durability 真 raft 节点 write-not-swallowed + backup-under-concurrent-write 一致性。
  - **Stage-C 完成**（6×Opus 对抗审查 → synth：2 BLOCKER + 7 MAJOR + 13 minor，synth 复现 BLOCKER-1）。主进程**全采纳 2 BLOCKER + 7 MAJOR、全修+全测** + high-value minor：BLOCKER-1 orphan-wal kill-9 腐蚀（清 sidecar + checkpoint TRUNCATE）、BLOCKER-2 post-migration verify 测（test seam）、MAJOR-1/2 incident scrub 漏 cmd + 不递归（denylist 扩 + scrubAny 递归）、MAJOR-3 self_node_id 重写、MAJOR-4 port home_broker rehome+epoch、MAJOR-5 webhook hung-endpoint -race+leak gate、MAJOR-6 skew gate 抽 versionSkewResponse 可测 + never-escapable e2e + sign-join embed、MAJOR-7 restore interlock/kill-9 测；5 minor 带理由 defer。报告 `docs/reviews/b6-review.md`。**make test + lint + -race 全绿 = Stage-C PASS**。**B6 DONE（暂存，未 commit）**。
- [x] B7 文档原 7 条大件（DOC#1–#7 + AUTO#9，最后一批）← **DONE**（Stage-A→B→C：6×Opus 审查 0 BLOCKER + 5 MAJOR + 11 minor，3 范围 deviation 全裁 JUSTIFIED；主进程全修 M1–M5 + 廉价 minor + 全测 → `make test`/`make lint`(0)/gofmt 全绿 → PASS）。报告 `b7-review.md`。**暂存（未 commit）**。M1：roster generation recover/retire 回退 → rosterForRegister 去 gen 门始终发（gen advisory）；M2/M3/M4：clusterspec 退非-voter 误拒 + transfer-leader 矛盾步 + 多退役 quorum-unsafe → isVoter-keyed floor + 先判 refusal + 不计 pending add；M5：doctor daemon-down 静默 green-lie → 响亮警告。
  - **IN-B7 ship**：DOC#6（`cluster status --card` 速览 + online `cluster doctor` 自检 + incident glue）；DOC#4 seam（`handleProxyStatus` cluster-mode 守卫经 proxyErr 回复）；DOC#2（`cluster ops ls/show` 只读派生视图）；DOC#5（`expose_rehomed` sys 事件）；DOC#3（`internal/clusterroster` account-签名 roster verifier core + proto 类型 + broker register-push 投递 + byte-equiv 双守卫）；AUTO#9（`cluster apply -f --plan` quorum-safe 拓扑序差分，`internal/clusterspec`）；DOC#7（`force-single/recover --guided` 诊断+印命令 printer）。
  - **净**：0 migration、0 proto bump（ProtoVersion 仍 2）、0 新 member subject。每新面 selfID==""/clusterMode==false/opt-in 惰性。
  - **DEFER→post-v2**：DOC#1（auto-restart reconciler，最高 split-brain）、DOC#4 全（proxy 集群化，最大共识面、P13-gated）、AUTO#9 apply-execute、DOC#3 agent join + URL-relearn、DOC#7 step-runner/TUI。
  - **主进程 3 个范围 deviation（review 记理由）**：DOC#2 派生视图（非持久 ledger 表，零 migration/零 Command 改/零 divergence）；DOC#5 仅事件（DEFER stall 告警[需 CHECK migration]+ ps 渲染）；DOC#3 agent 侧 DEFER（agent 无 account_pub pin，加它+碰 register-reply apply = plan 已 defer 的 fragile leaf）。

## ≥20 专家大规模联合内审（B1–B7 全程，外审前最后一道内部门）

- 22 专家 lens 并行审计整个暂存区（使用者/agent/运维/PM/SRE + 安全/并发/错误/数据完整/wire/解耦/简洁/CLI/测试/泄漏/quorum/可观测/升级/输入/DR/文档/命名）→ synth：87 raw → **0 BLOCKER + 18 MAJOR + ~50 minor**，verdict FIX-THEN-READY。核心不变量审计全 hold（无 proto bump / 无 single-WAL-owner 违反 / 无 byte-equiv 破 / confirm 门完好）。
- 主进程**全采纳 18 MAJOR、全修 + 全测**：DI×4（restore 重置 audit_published_index + 清 reqid ledger；restore_in_progress fail-closed torn-window 守卫；online backup manifest 从 copy；init --from-manifest 拒 backup-kind）、QS×1（apply 两 voter 都退役无占位符）、SEC×1（node_id charset fail-closed）、EH×1（incident Partial/Errors）、UX×2（4 emitter schema 判别符；no-active-session→exit64）、OBS×2（删假 last_contact_secs；quorum_margin 改 live）、DOCS×3（restore exit-3→1；broker.go 矛盾注释；apply/ops 入 usage.md）、TEST×2（metricsReady cluster bands；InitFromExisting refuse-before-mutate）+ downgrade-doc。余 minor 带理由 defer。
- 三个 Stage-C 范围 deviation 经独立审计**再确认 JUSTIFIED**。`make test`/`make lint`(0)/gofmt/-race 全绿。报告 `docs/reviews/b-mega-audit.md`。
- **B 程序（B1–B7）全部完成 + 全暂存 → READY FOR EXTERNAL REVIEW。** 等用户本人外审。

## 外部审查（用户本人）：Fail → 主进程逐条修复

- 用户外审结论 **Fail**（报告 `b-external-review.md`，含新增回归 `cmd/tether/b_external_review_test.go`）。14 findings（2 Blocker / 7 High / 5 Major）+ 3 Questions。
- 主进程**全采纳、逐条修复 + 加回归测试**，在报告内逐条回复：
  - **Blockers**：F1 restore preflight 移到 raft 构造前（fail-closed）；F2 移除暂存的 24M 动态二进制 + `/tether` gitignore。
  - **High**：F3 online backup 默认 leader-only + freshness provenance；F4 restore 唯一 `.pre-restore.bak` + 真实路径；F5 manifest/state applied_index 交叉校验；F6 apply 拒非 leader-view/partial status；F7 doctor 在线失败注入 FATAL 非零退出；F8 takeover-natsconf 跨校验 voter 全覆盖；F9 incident --out O_EXCL+O_NOFOLLOW。
  - **Major**：F10 scrub 文案降级 best-effort + 限制测试；F11 bad_request/restore-abort → exit 64（用户回归转绿）；F12 apply 非 voter remove 按 phase 分流诊断；F13 offline 统一 `ValidateClusterNodeID` + NUL/UTF-8 拒；F14 roster `VerifyAt`（过期/未知 schema 拒）。
  - **Questions**：Q1 restore `--raft-addr` override；Q2 webhook 拒跨 host redirect；Q3 ctl summary 加 (schema, schema_version)。
- 硬闸全绿（`make test`/`make lint`(0)/gofmt/-race）。**待用户复审**（外审不过不算 done）。
