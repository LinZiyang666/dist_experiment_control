# 发布就绪评估 + 剩余工作交接（allgreen 外审 remediation 之后）

> 日期：2026-07-21 · 当前 main HEAD：`55b1451`（"fix(cluster): close external-review release blockers + simcluster all-green remediation"，已 push）
> 用途：新 session 接手用。本文件自包含——记录**已达标的部分**、**发布就绪的分层判定**、**发版前需你决策/我来做的事**、以及**逐项 follow-up 技术债**（含文件位置、严重度、外审 finding 号、怎么做）。
> 关联真相源：外审报告 `docs/reviews/{claude,codex}-allgreen-external-review.md`（含一/二/三轮 + 逐条举证回复）；remediation 总纲 `docs/allgreen-remediation-roadmap.md`。

---

## 0. 一句话结论

**审查阻塞层已达发布线**（两位外审 claude+codex × 三轮 → Pass，CRIT-1/P1 头号目标 hermetic+live 双验证，已 merge 到 main）。
**能否"打 tag 对外发版"取决于两个决策**：(a) 目标部署是否触发 `grow-onto-recovered` 深层缺陷——是则先做 R16；(b) 是否需要 macOS 门。**发版机制本身（tag→goreleaser→车队升级）尚未执行。**

---

## 1. 已达标（本批闭合、已合入）

- **CRIT-1/P1**：drain/retire 数据面收敛门不再自毁（durable `pendingRetireConvergence` 取代 `migrateExposes`-返回值 oracle）。silent agent 的 drain 不再假 rc=0、retire 不再带着搁浅隧道走向 RemoveServer。**live 验证**（drill 71 Arm B：rc=0 仅当每个迁移 expose 的 agent ACK 新 home）+ hermetic durability + 变异。
- **ack 真实性（C1/M-1 → SR-8）**：home applied-ack 改 **per-directive single-use token**，即使 ack 走共享 `_INBOX` 总线也跨 session 不可伪造（token 只在自己 port 被 ack 时才泄露、且同时消费）。
- **grow/upgrade 互斥**：复制层条件获取 FSM op + caller read-back（no-op 即拒）。
- **cluster unlock**：零 health responder fail-closed（初判 + confirm 双probe）。
- **restore `--config ""`**：不再谎称 seam 已装（tri-state）。
- **锁 reap**：加 `reaperCaughtUp()` catch-up 门；xfer-orphan reap 每 bucket 重取 in-flight 集 + ModTime grace（不删新对象）。
- **retire REHOME_EXPOSES hold**：有界 → BLOCKED（`boundRehomeConvergence`）；F2 confirm 重置窗；门用**固定 origin `op.CreatedAt`** 作用域（无 confirm-retry 漂移 fail-open）。
- **PIN 限速合约**：改正为诚实 per-broker 语义（argon2id 主防线，architecture §E.6）。
- **simcluster harness**：verdict-contract die-frame 活门（ok/bad→pass/fail + 自守卫）、kept-sites 41 诚实 trade（→28）、ledger #29/#34 owner、lint-install heredoc 盲区、admin `-n` clamp、96 #58 split-home runtime-guard→gap（nc_guard=0）。
- **两处 Low 已随手清理（外审三轮建议）**：
  - `RESIDUAL-2`：`internal/port/d6_plan_test.go::TestReassignHomeStampsLastRehomeAtomically` 钉住 `PlanReassignHome` 的 UPDATE 同含 `epoch=` 与 `last_rehome_at=`（防未来重构静默 fail-open）。**已完成。**
  - `SR-8 注释漂移`：`internal/broker/home_delivery.go` 的 `pushes` 计数注释 + Transport 头两处已订正为 per-directive。**已完成。**

**硬闸状态**：`go test ./...`（`internal/tunnel` 的 `TestTunnelConcurrentClientOpens`/`...ReconnectFires...` 是**既有并发 stress flake**、未碰该包、隔离复跑过）· `make lint` 0 issues · `-race`+泄漏门 clean · `make e2e` rc=0（556s 干净复跑）· `run-all.sh` rc=0。
**deploy-tier（SR-8 镜像，serial）**：40=GREEN(pass=37) · 96=PRODUCT-RED(nc_gap=5 **nc_guard=0**) · 71=INCOMPLETE(CRIT-1 live) · ASSERT-FAIL=0 · INFRA-ABORT=0。

---

## 2. 发布就绪的分层判定

| 层 | 状态 |
|---|---|
| 外审阻塞层（发布级缺陷） | ✅ 全闭合，两位外审三轮 Pass，"可放行" |
| 头号目标 CRIT-1/P1 | ✅ 真闭合 + hermetic + live 双验证 |
| 代码质量（经得起对抗测试、非快乐测试） | ✅ 达标 |
| **grow-onto-recovered 深层缺陷** | ⚠️ **开放**（见 §3）——是否阻塞取决于目标场景 |
| 发版机制（tag/产物/车队） | ❌ 未执行（见 §4） |
| macOS 门 | ❌ 未跑（见 §5） |
| Follow-up 技术债 | ◻ 开放但不阻塞（见 §6） |

---

## 3. 发版前的关键决策：grow-onto-recovered 深层缺陷（可能阻塞，取决于场景）

- **是什么**：force-single 恢复（或某 broker 崩溃恢复成 survivor）后，**再扩容回 N=2** 时，clustered-JetStream 的 meta 从 1→2 形成期**死锁**。真实生产事故同型（racknerd/pc732，见 `project_racknerd_forcesingle_js_incident` / `project_cluster_ha_realmachine_test` memory）。
- **影响面**：drill **42 / 51 / 22 / 82**（本批 deploy-tier 里 42=PRODUCT-RED、51=PRODUCT-RED 即此族）。
- **状态**：已在外审中**诚实披露为深层 PRODUCT-RED**，**不在本 allgreen 批次范围**，留给专属 **R16**（HA 关键路径，值得单独 plan→实现→内审→外审，不 rush）。
- **决策树**：
  - 目标部署**不涉及"恢复后再扩容"**（单 broker / 稳定 N≥3 不重扩）→ **可直接发版**，本缺陷不触发。
  - 目标部署**会做恢复后 re-grow**（HA 运维演练/车队重组）→ **建议先做 R16 再发**，否则用户会踩死锁。
- **相邻已披露的深层项**（同样非本批、可与 R16 一起消化）：`#57`（home-broker 崩溃时 in-flight transfer 审计悬挂）、`#58-split-home` residual（`homeOwnsXferBucket` 需精化到 per-transfer-owner——见 96-A2 的 gap + `internal/broker/transfer_reconcile.go`）。

---

## 4. 发版机制（未执行，按 `project_release_fleet_upgrade` memory）

流程 = `tag → goreleaser → 车队升级`。当前**只 commit 到 main（`55b1451`），未打 tag、未构建产物、未推车队**。

发版时要做：
1. **打 version tag**（下一版号：主线已到 v0.4.7，按 leaf 增量走 v0.4.8 或按你判 v0.5.0）。
2. **goreleaser 构建**静态产物（`CGO_ENABLED=0`、Go 1.25 锁；升级依赖前必验 go directive）。
3. **车队升级**（见 `project_live_fleet_v2_ops` / `project_release_lines_v1_v2` memory）：
   - 现网活跃 v2 车队（pc732 broker + timan1/107/108）已全 proto-v2 v0.4.x，patch 从 main 发。
   - node upgrade 需**显式 `--url + --sha256`**；broker 经 pc732 NOPASSWD sudo 远程更新。
   - 休眠 v1 节点（a100/jupyter-*/optiplex）仍搁浅、连不上 v2 broker——不在本次升级面。
   - **proto wire 未变**（本批无破坏性 wire 变更）→ 是 upgrade 不是重装。

---

## 5. macOS 门（未跑，按 `project_macos_test_gates` memory）

- 本次 remediation **全在 WSL/Linux** 跑。
- darwin-only 失败会被**缓存掩盖**（`go test` 需 `-count=1` + `set -o pipefail`）：`sun_path` 长度限制、`/var` 软链、`install.sh` 角色门。
- **若 release 含 macOS 客户端**：在你的 mac 上 `go test -count=1 ./...`（重点 tunnel/adminsock/clusteroffline/install.sh 路径），或至少跑触碰面的包。本批新增的 broker/cluster/port 测试都是纯 Go 逻辑、无 darwin 特异性，风险低但需确认。

---

## 6. Follow-up 技术债（外审明确**不阻塞放行**，按 roadmap 消化）

按优先级 / 关联：

### 6.1 deploy-tier 登记落地（task 26 残项）
- **96 的 `runtime-guard→gap` tsv/finalization 登记**：drill 96 的 A2 分类已改（`runtime-guard→gap`，server 实测 nc_guard=0），但 `expected-verdicts.tsv` / `finalization` 的 96 行**归因文字**可能仍需同步更新为"split-home #58 = defect-tied gap、nc_guard=0"。核对 `test/simcluster/expected-verdicts.tsv` 的 96 行 + `docs/reviews/r15-finalization.md`。
- **30 HALT 窗定案实验**：drill 30 的 `PHASE-1 CONTINUITY` write-probe 在 agentless HALT / broker reload 窗看到 `not_leader/503`——**是产品缺陷（滚动升级不保持写可用性）还是 drill 谓词过严**未定案。需一次保留现场的逐样本尸检（write-probe 命中时刻的 broker 状态）。定案后如实登记 owner。文件 `test/simcluster/drills/30-rolling-upgrade.sh`。
- **30/82 归因**：已从"稳定 ASSERT-FAIL"改为"间歇 flake band"（外审第 3 次复跑二者转 INCOMPLETE）。若要真 37/37 需消除间歇源或如实登记为无主非绿（roadmap N1）。

### 6.2 并发/锁硬化（Medium 残留）
- **锁 reap catch-up 硬化**：`reaperCaughtUp()`（`internal/broker/clusterwrite.go`）在 boot/岛化 follower 上 `applied>=commit` 可能平凡为真（commitIndex volatile）。外审建议加 **`CommitIndex()>0`**（曾与 leader 同步过的新鲜度界）。影响 `reconcile_upgrade_lock.go` / `reconcile_grow_lock.go` / `transfer_reconcile.go` 的 reap 门。
- **M-2 门行为测试**：锁 reap 的 catch-up 门目前是 source-pin（`TestMembershipLockReapsGateOnCatchUp`）。behavioral（lagging leader 不 reap 活锁）需多节点 lag fixture——补一个。
- **M-3 grace 接线测试**：xfer reap 的 `xferReapMinAge` 已有 `TestXferReapShieldsFreshObjects`；`reaperCaughtUp` 的 boot/岛化平凡真与上条同源。

### 6.3 F3 / RESIDUAL-1（理论、非回归，仅记录）
- **RESIDUAL-1**：F3 用固定 origin `op.CreatedAt`。理论上跨-leader 且新 leader 时钟**回拨 > 创建→migrate 实耗时**（NTP 级 >10–15s 偏差）时，本 op 行可能 `last_rehome_at < op.CreatedAt` → fail-open。**极难触发，固定原点严格优于弃用的滑动窗（残留非回归），符合安全实用主义，外审判可仅记录。** 若要顺手收：给 `since` 减一个小 skew 容差（`op.CreatedAt - clockSkewTolerance`，方向 fail-CLOSED）。文件 `internal/broker/cluster_operation_controller.go`（driveRetire）+ `clusterdrain.go`（`pendingRetireConvergence`）。

### 6.4 一轮 N-* / 文档项（Low）
- **N-3（安全）**：`reconcile nats` issuer-skew 检测在**检测本身出错时 fail-open**（`internal/broker/cluster_secrets.go:44-52`，conf 读不出→confIssuer=""→不判→放行）。建议 `acct!="" 但 confIssuer==""` 时升 loud note/拒。需已 root 才可利用 + doctor 独立 FATAL 兜底。
- **DOC-27（N-4a）**：`docs/cluster-runbook.md §5` + backup 命令示例仍全用 `/var/backups/...`，而在线备份由 `User=tether` 创建、stock 装机不可写（drill 50 实测 permission denied，51 已改 `/var/lib/tether`）。改 runbook 文案。
- **N-4b/c**：`cluster_backup.go:248` fresh-box 缺 conf 分支印不可执行的 `reconcile nats --conf <缺失>`；`applyClusterSeam` 的 `nats_conf_path` 硬编码默认、不与 `--nats-conf` 联动。
- **N-5**：`adminEventsTail` 的 `--since`/`--kind`/`eventsMaxScan=5000` 截断 / ctx 中途失败**无 truncated 标记**（静默返部分结果）；`xfer_reap_interval`/`disk_check_interval` 无上界（`10000h` 等效静默关停）。加 truncated flag + 上界钳。文件 `internal/broker/admin.go` / `internal/serveconf/serveconf.go`。
- **N-6**：`runtime_introspect.go` 的 `Threads` 用 pprof threadcreate `Count()`（创建过的 M 数、不回落）注释略过头，可订正；split-home/零节点 session bucket 集群模式永不可 reap，建议加**告警计数**防小盘顶满（racknerd 事故同型）。
- **N-7**：`kept-sites.sh`/`lint-install.sh` tokenizer 不感知引号（当前 37 drill 无真误计，未来漂移通道）；`kept-sites.baseline.tsv` 无 provenance header、总和三个数字互不等。补 header。
- **N-9**：`home_delivery.go` 的 `attempts` map（键 `sid|nid`）全释放后条目永久残留——慢泄漏，NumGoroutine/fd 门抓不到。本批 C1 的 `outstanding` 已加 TTL prune；`attempts` 的 prune 补一下。
- **M-5**：反注水闸 D（INVERTED 块须同块配对 `product_red|assert_bug`）的 **lint 未实现**（只有描述注释）。block 级配对需块作用域解析（line-based lint 做不干净），ledger-crosscheck 已接替"已注册缺陷坐绿 drill"这一半，"未注册倒置假绿"留 follow-up。 **[WITHDRAWN 2026-07-21 → `test/simcluster/tests/lint-drills.sh` gate-D 注释；任何可实现的代理都 key 在注释里的 "INVERTED" 一词=prose-triggered lint（正是 kept-sites 剥注释要避免的），且本仓有已量测撤回先例（ledger-crosscheck 10 命中 3 真）。两半接替=ledger-crosscheck（已注册缺陷坐绿）+ Stage-C non-vacuity 要求（未注册倒置=接受残留）。]**
- **H2 的 N≥3 drill**：若将来真做集群一致 PIN 限速（倾向 best-effort 失败 gossip、非 connect 路径分布式写），补一条 N≥3 分布 callout 的 deploy-tier drill 作验收。当前 v1 明确 per-broker、不做（architecture §E.6 已改正）。 **[RECORDED → docs/reviews/simcluster-coverage-boundary.md §3（deferred v2 验收门）]**

---

## 7. 关键引用

- **外审报告**（含一/二/三轮 + 逐条举证回复、顶部最新结论指引）：
  - `docs/reviews/claude-allgreen-external-review.md`
  - `docs/reviews/codex-allgreen-external-review.md`
- **remediation 总纲**：`docs/allgreen-remediation-roadmap.md`
- **本次 commit**：`55b1451`（189 files，作者 LinZiyang666、无 AI 署名）
- **本批新增的关键回归测试**（防未来 revert）：
  - `internal/broker/r8_home_delivery_test.go`：`TestPendingRetireConvergenceIsDurable`（CRIT-1 durable）、`TestRetireRehomeHoldIsBoundedToBlocked`（bound→BLOCKED + F2 confirm 重窗）、`TestHomeDeliveryConvergesEveryPortOfAMultiExposeAgent`（多-port 收敛）、`TestHomeAckPerDirectiveTokenRejectsSiblingForgery`（SR-8 跨-session 伪造被拒，变异实证）、`TestPendingRetireConvergenceRecencyWindow`（F3 固定 origin 不龄出 vs 滑动窗 fail-open 对比）、`TestDrainConsultsTheDurableConvergenceGate` + `TestRetireConsultsTheConvergenceGate`（drain/retire 双 source-pin）
  - `internal/port/d6_plan_test.go`：`TestReassignHomeStampsLastRehomeAtomically`（RESIDUAL-2）
  - `internal/broker/reaper_gate_test.go`：`TestMembershipLockReapsGateOnCatchUp`（M-2 catch-up 门）
  - `internal/broker/reconcile_passes_test.go`：`TestXferReapShieldsFreshObjects`（M-3 ModTime grace）
  - `internal/cluster/codex_...` / `internal/broker/codex_...` / `cmd/tether/codex_...` / `internal/authcallout/codex_...`：codex reviewer 回归（4 转绿 + PIN 透明重构）
- **相关 memory**（新 session 会自动加载 MEMORY.md，重点看）：
  - `project_release_fleet_upgrade`（发版 + 车队升级）
  - `project_macos_test_gates`（macOS 测试门）
  - `project_racknerd_forcesingle_js_incident` / `project_cluster_ha_realmachine_test`（grow-onto-recovered 生产事故）
  - `project_live_fleet_v2_ops` / `project_release_lines_v1_v2`（现网车队/发版线）
  - `feedback_commit_authorship`（无 AI 署名）/ `feedback_main_only_no_branches`（直提 main）

---

## 8. 下一步（新 session 可选）

1. **发版路径 A（场景不踩 grow-onto-recovered）**：打 tag → goreleaser 出产物 →（可选）车队升级。
2. **发版路径 B（涉及恢复后 re-grow）**：先开 **R16** 收 grow-onto-recovered 深层缺陷族（42/51/22/82 + #57 + #58-split-home），走 §3 三阶段（对抗草拟 plan → 实现 → 内审 → 外审），再发版。
3. **收尾 follow-up**：§6.1（96 tsv 登记 + 30 定案）先做——它们让 deploy-tier 台账真正收敛；再 §6.2 锁硬化；§6.4 文档/N-* 项随缘。
4. **macOS 门**：若含 mac 客户端，先在 mac 上 `go test -count=1 ./...` 复跑触碰面。
