# Deploy-tier gotchas — simcluster S-series 新台账（#25+）

Date: 2026-07-11（建档：S 系列首开批 S1 落地，roadmap `docs/simcluster-coverage-roadmap.md` §5）。

> **这是什么**：`test/simcluster/` deploy-tier drill 在**真实部署栈**（真 systemd + 真独立 nats-server +
> 真 install.sh 路径 + 真持久盘）上跑真实使用/运维旅程时，新发现的 tether 缺陷台账。编号 **#25 起、全局
> 连续**，接续 `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`（**#1–#24 是那段的 SSOT**，两文件互链）。全局
> 连续保证 `assert_bug` 的 gotcha token 与 `[GAP #N]` 标注跨两文件唯一。
>
> **只测不修**（roadmap §0.2）：S 系列 drill 只**暴露**缺陷（登记此处 + `assert_bug` 签名钉 RED），
> **不**交付产品修复；修复另立独立叶子增量（如 G 系列先例）。某 gotcha 修好后 → 对应 `assert_bug` 翻
> `assert_ok`、trailer token 移除，由修复批负责。
>
> 每条模板：**现象 / 机理(file:line) / 怎么自动化或修 / 钉住它的 drill + 签名**。
>
> **已了结条目已剥离**：CLOSED / FIXED / REFUTED / WONTFIX 的条目全文移到
> `docs/reviews/deploy-tier-gotchas-closed.md`，本文件末尾留索引表。正文只留**仍未了结**的条目。

## `#I*` 过渡族收编（关族）

历史上 sim 用 `#I1` 记「cluster-mode `serve` fail-closed 拒无 raft state 的 fresh joiner」——这是 tether
**有意保留**的 fail-closed 不变量（不是缺陷），由 `drills/11-grow-gaps.sh` 的 `assert_refuses`
（`no raft state exists|never auto-bootstraps`）钉住。**自本台账起 `#I*` 族关闭**，不再新增 `#I*` 号，
一律用 `#25+`。`#I1` 的语义并入 drill 11 的头注与断言，此处仅登记其归属，不占 gotcha 号。

## 产品缺陷台账（#25+）

> **S1 现实 gotcha 数 = 0**（`60`/`61` GREEN 旅程、`62` FUSE-approx spike，未暴露新缺陷）。**S2 登记
> #25/#26/#27**（三条均由「探索→定格」臂以**倒置 `assert_ok`**〔缺陷=本该拒却成功/本该清却泄漏，与
> `assert_bug` 语义相反，见 §0.2 注〕钉住，附 flip 条件）。三条均经**活体 server spike 实测**（2026-07-11）。

### #29 — cluster expose 的 home 不可投递到非-tunnel broker（un-homed 回落）；crash home → 常规 expose **全 voter 搁浅**（不自动 rehome），直到 home RETURN
- **现象（LIVE-CONFIRMED，6 次 sim run 2026-07-12/13/14）**：
  - **home 不可投递到非-tunnel broker（净效果 = tunnel-coupling，但机制是 un-homed 回落）**：N=3 cluster 里
    `expose --on-broker <非-tunnel voter>`（agent 隧道在别处）→ `homeForExpose`（`internal/broker/home.go:96-113`）
    对该 home 返回 **nil（un-homed）**（home.go:105 日志「home not deliverable on expose; leaving un-homed」）→
    agent 的 AddProxy（`tunnel_adapter.go:76-77`，空 BrokerAddr ⇒ fallback）拨它**固定的** tunnel broker → 该 broker
    因 committed 行的 home_broker != self **拒 REGISTER** `token_unknown_or_revoked` → `agent_rejected:frpc_failed`
    + rollback（实测 exit 70；probe-onbroker + probe-drain P4，180s×2 严格，agent journal 抓证）。**净可观测**：
    cluster expose 的数据面只在 home == agent 自己的 tunnel broker 时起来。**注意这不是"agent 设计上硬编码固定
    tunnel"**（源码若投递 named home，agent 会拨它——见 #33 的 proxy exit 就成功拨非-tunnel home）；是
    **un-homed 回落 → self≠home 拒** 这条路径。N=1（home_broker=''）不受影响（drill 70 GREEN）。
  - **crash-stranding（#29 核心）**：一个 home 在 brkH 的常规（rebuild-ON）expose，brkH 被 **crash**（node_kill）后
    **不自动 rehome**——`internal/broker/rehome_events.go:52-53`「regular exposes are NOT auto-rehomed on a crash —
    stranded until a drain/return」。其公网口只由 home 服务，故 crash 后**全 live voter 上 curl exit-7 搁浅** +
    epoch 不变，直到 home **RETURN**（agent 重拨返回的 home，同端口/同 epoch 恢复）。
  - **rebuild-OFF**：`--no-rebuild` expose crash 后随 home 一起 down——**与 rebuild-ON 在 crash 下行为完全相同**
    （都搁浅、都在 RETURN 后同端口恢复）；rebuild 区别**只在 drain 路径**（rebuild-ON 迁 / rebuild-OFF 拒），
    而 drain 路径 NOT-COVERED。
- **round-5 订正（external-review R5-M3/M7，2026-07-14，撤回 round-4 的"必须 tunnel-coupled"措辞）**：round-1..4 曾
  写「home!=tunnel 初始交付**永久死** / `≥1/4` 倒置探针 / **2× isolated GREEN**」（round-1..3），round-4 又写
  「expose **必须** tunnel-coupled（agent 拨固定 tunnel by design）」——**后者被源码证伪**：`homeForExpose` 确实会为
  eligible 的 home 投递 named directive（BrokerAddr=home.TunnelAddr），agent 会拨它。真正的行为是 **homeForExpose
  对刚 grow 的非-leader 返回 nil（不可投递）→ 回落固定 tunnel → self≠home 拒**（un-homed 回落，非硬编码）。
- **机理**：`home.go:96-113` homeForExpose 对 not-eligible / `CertFP==""` / 空-TunnelAddr 的 home **返回 nil（un-homed）**；
  agent `tunnel_adapter.go:76-77` OpenHome 在 directive BrokerAddr 空时 fallback 固定 tunnel_addr；broker
  tunnelTokenLookup 在 token 的 home_broker != self 时拒 `token_unknown_or_revoked`；crash 后常规 expose 由 reaper
  **不 rehome**（仅 `__proxy__` home 会 rehome，regular expose 搁浅——与 #33 的 proxy-exit 行为相反）。
- **钉住它的**：`drills/71-expose-rehome-failover.sh`——**FIXTURE 门**（agt→brk3 间歇不建立时诚实 NOT-COVER-THIS-RUN、
  不在未建立 fixture 上跑 crash 臂）→ **combined crash（Arm C rebuild-ON + Arm D rebuild-OFF 一次注入）**：agent 隧道
  指向 brk3、rebuild-ON `wstrand` + rebuild-OFF `wnr` 都 home 在 brk3 → node_kill brk3 → **两个都全 live voter
  curl exit-7 搁浅** + `cluster status` reachability 证 leader 见 crash + epoch 不变 + wnr explain 诚实 rebuild:false/
  未 moved → node_start brk3 → **两个都同端口恢复**；**Arm A（--on-broker bogus 负例）**；**非-leader 硬门**（brk3 必是
  live 非-leader）。
- **drain-migrate = HARD RED 失败门（R6-M2，非 owner-decision NOT-COVERED）**：
  SUPPORTED-rehome-via-DRAIN（B）是 drill 71 的**硬断言**——`cluster drain brk3` 必须迁 rebuild-ON expose 到 survivor
  voter 并 serve；被**三重叠加**产品行为挡即 **RED 暴露**为 release-blocking:(1) homeForExpose 不投递非-tunnel home
  （un-homed 回落，`--on-broker <非-leader>` 从不 serve）；(2) 唯一替代——把 **agent 隧道**指向非-leader——本身
  **间歇**（agt→brk3 有的 run 建、有的 200s 重试仍不建；外审 solo1b 亦命中为 71 RED，drill 用 HARD FIXTURE 断言暴露）；
  (3) 即便 fixture 建成，`cluster drain brk3` 被 grow 遗留的 `NATS_ROLLED_OUT` 挂起 op 拒（"already in flight for brk3"，
  需手动 `cluster ops abort`——#31 grow-op 家族）。**Arm E（rebuild-OFF drain 拒绝）现为直接执行**（R7-M2）：wnr
  （rebuild:false）+ wstrand 在 return 后**都 live** 时直接 `cluster drain brk3`，要求 clusterdrain.go:665 的 rebuild-OFF
  拒绝签名；若 #31 挂起 op 先拦（NATS_ROLLED_OUT）则该签名不可达 → RED 暴露（不再与 G/F 并列为"需成功 drain 作前提"）。
  粘性（G）/ rehome_stalled（F）仍需**成功 drain** 作前提→同墙阻塞（暴露、非 rescope）。**hermetic 覆盖（R8-M3 订正 R7 的 scoping 错误）**：
  `cluster drain` 的 **marker + phase-advance + rehome-target-exclusion** 有 hermetic 测（clusteradmin_test、g1g7 A9），
  **且 rebuild-OFF drain 拒绝也有 hermetic 测**——`test/d7/integration_test.go testD7DrainRefusesRebuildOff` 断言
  `errors.As ErrRebuildOffExposes` + 枚举的拒绝端口 + home 未被静默改（R7 说"无 `_test.go` 引用 `ErrRebuildOffExposes`"是
  **事实错误**——那次 grep 只搜 `internal/`、漏了 `test/d7/`）。drill 71 Arm E 在其上**追加** CLI / 真栈 / #31-交互覆盖,
  **非唯一**覆盖。drill 71 **唯一独有**的是**端到端 drain-migrate 数据面**（rebuild-ON expose 真迁到 survivor 并经真隧道
  serve）——D7 的 `DrainRetireFollower` 对 `migrateExposes` no-op（无 expose homed）,该数据面路径**无 hermetic 覆盖**,
  drill 71 Arm B 是它唯一的覆盖。home_reassign_*/broker_down_rehome_summary/expose_rehomed/rehome_stalled **raw**
  sys.events 无 operator reader（owner-decisions D2，仅 raw-event carve-out）——这些 EVENT 本身 NOT-COVERED，但
  drain-migrate 的 EFFECT 是硬门。
- **怎么修**：让 homeForExpose 对任一 eligible home 可靠投递 directive（含 BrokerAddr+CertPins）+ agent 对 home
  directive **真正重拨新 home**（而非 fallback 固定 tunnel）；crash 后让 reaper **也 rehome regular exposes**（或明确
  文档化「crash 搁浅、需 drain/return」）。**flip**：crash 后 expose 自动 rehome 到幸存 voter + 数据面恢复 → Arm C
  的搁浅断言翻 → 翻 GREEN 回归。

### #31 — `cluster add` 的 grow lock（`cluster_grow_active`）best-effort release 几乎总失败、残留、阻塞 upgrade（dry-run 看不见）
- **现象**（`30-rolling-upgrade`，2026-07-12，3/3 live real roll 命中）：`cluster add`（grow）完成、joiner 达
  VOTER、命令 **exit 0 报成功**后，`cluster_grow_active` marker **几乎总残留**。后果：（a）`cluster upgrade` 的
  **real roll** 撞 `acquire upgrade lock refused: a cluster membership operation (join/retire) is in flight`
  （exit 69），HALT 于 safe partial state（无 host re-exec）；（b）同一残留让连续 grow 的下一个 `cluster add`
  撞 serialized fence → 该 joiner INCOMPLETE（**SOLO server-local 跑时的 serialized-fence grow flake 根因**——
  区分 simcluster:223-229 记录的**并发** 7-way-sweep VOTER-timeout，后者是 clustered-JS 形成时序、非 grow-lock）。
  「几乎总残留」的实证限定 (a) upgrade 场景（brk3 是最后 grow、release 时序最差，3/3 命中）；(b) 是间歇的。
  **关键隐蔽性**：
  `cluster upgrade --dry-run` 与缺 `--account-seed`/`--backup-taken` 的 refuse 都**不 acquire upgrade lock**
  （只预览 / 只做参数前置校验），所以它们**全 PASS**——残留只在 real roll 的 acquire-lock 步骤显形。这也是
  为何我最初的 dry-run 探测法探不到、险些把缺陷绕过成假绿。
- **机理**：`cmd/tether/cluster_add_drive.go:494-506` `releaseGrowLock` 自述「best-effort clears THIS joiner's
  grow marker via the leader」。两条失败路径（`could not resolve the leader` / `release-lock trigger` not OK）
  都只 `⚠ WARNING`（非 fatal），`cluster add` 仍 exit 0。**VOTER 达成 ≠ SERVING ≠ lock released**。3/3 live
  命中说明 release 在 sim 的 cutover-后 leader 时序下**几乎总失败**（非罕见边角）。
- **运维含义**：grow 后**无法直接 upgrade / join / retire**；需手动 re-run `cluster add --account-seed` 清
  marker 才能继续——一个 tether 本应自动完成、现实要人工绕过的步骤（tether 自己的 WARNING 就这么说）。
- **怎么自动化或修**：`releaseGrowLock` 失败应**重试到确认**（或 block grow 完成直到 lock released），而非
  best-effort 静默放过；或让 upgrade/join/retire 遇陈旧 marker 时能自愈清理。
- **钉住它的**：`drills/30-rolling-upgrade.sh`——real roll attempt-1 用其**自身 HALT**（`membership operation
  in flight`，`_roll_halted_on_growlock`）作 **[GAP #31]** 暴露证据，`assert_ok` 钉住「upgrade 被残留 lock
  阻塞」这一缺陷现象；再重试 tether 自己的恢复动词（re-run `cluster add`）清 lock——**实测恢复走同一
  best-effort release 路径、也清不掉**（30 real-roll #2 仍 HALT），故 upgrade-roll 机制（re-exec /
  write-continuity / PID-same）**NOT-COVERED**（#31 阻断，像 #29；假绿 MainPID-same/write-probe-clean 主动
  suppress，因它们 PASS 恰因 upgrade 没发生）。**坦白（Stage-C mandate-4 订正）**：我这次 SOLO server-local 跑
  遇的连续-grow `HALTED at acquire-lock: grow of brkN already in progress — serialized`（前一 joiner 的 release
  间歇失败挡下一个）确是同一 grow-lock 泄漏，我用一个**临时 server-local retry 调试脚本（不入 repo、非正式
  harness）**重试搭起 N=3；须与 simcluster:223-229 记录的**并发**（7-way sweep）VOTER-timeout 区分——后者是
  clustered-JS meta-group 形成时序、另一类 flake，正式 runner `run-drills.sh` 的 `FLAKE_SIG` 不含它、**不
  auto-retry**（surface RED 手动重跑）。#31 只 claim serialized-fence + upgrade-blocked，不 claim 并发
  VOTER-timeout。**flip**：release 变可靠（grow 完成即 lock released）后，30 的 real roll attempt-1 不再 HALT →
  [GAP] 臂走 else（clean）→ upgrade 机制变 COVERED、翻成普通 GREEN 回归。

### #33 — proxy exit crash-rehome 后 SS **数据面**恢复滞后控制面（**仅观测 + 测量，根因未归因**）

> 号码取 **#33**：`docs/reviews/s3-s5-plan.md:355` 已占 **#32（CANDIDATE）= rehomed-then-returned 泄漏 stale
> public listener（double-bind）**，不同假设，避免撞号（外审 R2-M2）。

- **现象（reproduced across valid runs）**：一个**已建立在传字节**的 proxy exit，其 HOME broker 被**杀**（quorum 保住
  2/3）后，**控制面立即恢复**——`proxy status` 的 home 从死 broker 移走 + 该 exit 达 `ready=true`——**但 SS 数据面在
  那一刻仍是黑洞**，之后 **per-run 或自动恢复（AUTO-RECOVERED，测量 lag、不声称固定值）或该 run 内不自动恢复
  （STRANDED，需手动 `proxy off; proxy on`）**——两种都如实记录。即"控制面报 rehomed+ready ≠ 数据面已活"。
- **round-5 订正（external-review R5-M6/M7，2026-07-14，撤回旧倒置-断言 + 240s 门 的措辞）**：早先的 round-3
  readiness-based（`_ready_lags_60s`）与 round-4 "rehomed+ready 瞬间倒置-assert_ok 数据面黑洞 + 240s 内必恢复 die"
  **两版措辞都撤回**——`proxy_ready` 恢复很快、`ApplyHome→OpenHome`（tunnel.go:937-964）是原子换 session + 重拨，
  没有可靠的"ready-but-black-hole"瞬间可倒置断言。当前 drill **不做倒置断言、不设 240s 恢复 die**，改为
  **measure-and-record**：
  - `[#33-a]` crash **确定性**切断到死 home 的隧道 → 那条 pre-crash 已验证在传的 SS leg **黑洞**（确定，非 flaky）；
  - `[CONTROL]` home 离开死 broker（die 门）+ exit 达 ready（die 门，宽容）；
  - `[#33]` **poll 180s 测量数据面是否自动恢复**，如实记 **AUTO-RECOVERED（记 lag）或 STRANDED**——**两种都接受**
    （既非 die、也非 flaky 倒置 pin）；STRANDED 时 QUORUM 臂的手动 `proxy off; proxy on` heal 证明可恢复。
- **未确立（不声称）**：① 任何**固定** lag 数字——lag 非确定，只 per-run 测量并 log；② **根因归因**——第一轮
  「ApplyHome 原地 re-point 已断 session」**是错的**（ApplyHome = 原子换 session + 重拨新 home；SS server 独立于
  tunnel）。**根因待查**。
- **运维含义（谨慎表述）**：承载 proxy exit 的 broker 崩溃后，即使 `proxy status` 显示 exit 已 rehome + ready，其实际
  egress 有时仍需数十秒才恢复、有时该 run 内根本不自动恢复（需手动 `proxy off; proxy on`）——一个值得进一步诊断的
  恢复缺口，非已归因的确定缺陷。
- **钉住它的**：`drills/73-proxy-cluster-ha.sh` Arm REHOME——baseline SS leg 传字节（die 硬门）→ kill →
  `[#33-a]` 同一已验证 client 黑洞（确定）→ `[CONTROL]` home 离开死 broker + 达 ready（die 硬门）→
  `[#33]` **测量并记 AUTO-RECOVERED/STRANDED（两种都接受，不 die、不倒置）**。QUORUM 数据面分离臂用
  `proxy off; proxy on` heal 在 2/3 fresh-establish 解耦，**不依赖** crash-rehome 恢复时序。
  **flip**：crash-rehome 后数据面稳定在 rehomed+ready 瞬间即活（不再 STRANDED）→ 该臂转成对"prompt 自动恢复"的
  正向 GREEN 断言。

### #34 — proxy home 分布无法稳定保持 one-per-voter；非-tunnel voter 的 proxy-eligibility 不稳定；auto-rebalance-on-return 不发火（external-review round-6 硬化 74 时暴露）
- **证据分级（R7 订正——区分"已证"与"观测但未独立归因"）**：
  - **已证 A（control-plane home 计数直读，无歧义）**：`cluster rebalance proxy` **能**把 3 个 __proxy__ exit 构造成
    one-per-voter（1/1/1，spread==0），但**随即又漂移回全堆 tunnel/leader broker brk1**（实测 1/1/1 → brk1=3/brk2=0/
    brk3=0）。这是**分布不稳定**的直接证据——`home_broker` 计数是控制面直读，不经数据面，无歧义。
  - **已证 B（运行时诊断）**：C-auto 窗口时刻 leader `cluster ops ls` 有**非终态 in-flight op**（`brk3 join in_progress`），
    关掉 auto 的 `no in-flight op` fire-gate（proxy_auto_rebalance.go:57）→ auto 正确 DEFER 不发火。
  - **观测但未独立归因**：74 的 SS 腿（`_ss_via_home`）**间歇超时**。**⚠ 单条 SS 超时不足以证明"eligibility loss"**——一次
    `_ss_via_home` 超时无法区分 missing-home / `/sub` 渲染 / ss-local readiness / sink 失败这几层。所以 SS 超时**仅佐证**"数据面
    有问题"，**不单独钉死具体层**；真正钉住 eligibility 不稳定的是上面**已证 A 的分布漂移**（home 计数），不是 SS 超时。
- **合起来的推断（非独立证明）**：分布漂移（已证 A）**符合**"非-tunnel voter（brk2/brk3）的 proxy-home-eligibility 不稳定
  ——拿到 eligibility → 接一个 exit → 又丢 → exit 重 home 回 brk1"这一机制，但该机制本身是**从 home 计数推断**的、未经源码/事件独立
  确认。伴随两个已知面：① 非-tunnel exit 的 SS 数据面间歇搁浅（#33 家族，观测——见上，不单独归因到具体层）；
  ② `TETHER_AUTO_REBALANCE=on` 的 auto-rebalance-on-return **在 sim 的 kill+return 场景不发火**（180s 锁定窗内，
  round-5/6 一致观测）。**根因确认（源码 + 运行时双证据）**：auto 的 fire-gates（proxy_auto_rebalance.go:57-58）=
  `downNow empty ∧ **no in-flight op** ∧ !force-single ∧ no recent proxy rehome`；74 的 C-auto FIRE-GATE 诊断**实测**
  在 auto 窗口时刻 leader `cluster ops ls` 有非终态 op：`brk3 join in_progress`（"topology convergence: voter brk1
  at gen 0 < …"）——即 return（rejoin）留下一个 **in-flight join op**（= 挡 71 drain、挡 30 upgrade 的**同一个 #31
  grow-op 家族**），关掉 `no in-flight op` gate → auto **DEFER 不发火**。**非 auto 逻辑本身坏**（hermetic
  `g7_auto_rebalance_test` 已密其 flap/gate/cooldown）。**#31 挂起-op 影响面广：挡 drain(71) + upgrade(30) +
  auto-rebalance(74) 三样。** 合起来：proxy home 分布 / rebalance / 数据面**整个子系统在部署环境下不稳定**，且与 #31 深度交织。
- **round-5 的掩盖（订正）**：round-5 的 74 用 measure-and-record（AUTO-SERVED/STRANDED 都接受）+ warn-only NOT-COVERED
  把上述**整条不稳定链全擦成 GREEN**——外审 round-6 R6-M1 驳回为"假 release 判定"。已撤回：B-dp（moved-exit 数据面
  闭合）+ C-auto（auto EFFECT）改**硬断言**，drill 诚实 RED 暴露。
- **钉住它的**：`drills/74-rebalance-on-return.sh`——`SETUP-ss-brkN`（每 voter 一条 SS 腿，非-tunnel exit 搁浅即 RED）
  + `SKEW-reconstruct`（SKEW 前重建 1/1/1，漂移即需重建，暴露不 hold）+ `B-dp`（moved-exit 数据面必须闭合，硬 RED）
  + `C-auto`（auto EFFECT 必须发生，硬 RED）。实测 `74 RED (3 failed, 33 passed)`。
- **怎么修**：让非-tunnel voter 的 proxy-home-eligibility 稳定（reconcile 后不丢），使 1/1/1 可保持；让 moved/rehomed
  exit 的 SS 数据面在 home 变更后可靠闭合（同 #33）；让 auto-rebalance-on-return 在 return edge 可靠触发。**flip**：
  三者稳定后 74 的对应硬断言转正向 GREEN 回归。

### #42 — quorum-loss 后 ~TFence(10s) 内 `cluster status --remote` 误报 transient + `session rm` 栽 raw store_error（有界窗口观测缺口）

- **现象/机理**（Stage-C M1 订正——**非永久，是有界 ~10s 窗口**）：N=2 杀 1 voter 后 survivor 降 leadership，`LeaderContactStale` 在 `TFence=10s`（`internal/cluster/read.go:18`）+ leader-lease 后才翻 true。**窗口内**（~0-11s）：① `--remote` VERDICT = "electing a leader (transient) — re-run shortly"（误导：不可恢复却报"稍后重试"）；② `session rm` 默认（无 store-backed alert 时）栽 raw `SQLite error (store_error)`，因 `EvalDestructiveGate.QuorumLost` 走同一 TFence LIVE-probe 谓词（`cmd/tether/alert_gate.go` 的 `EvalDestructiveGate`／`gateDestructive`；
文件此前叫 `d8_alerts.go`，线二按「文件按职责命名」改名，故旧文里的 `alerts.go:144-163` 已失效——**引用改成符号名而不是行号**，行号会随任何编辑作废而符号名不会）、窗口内尚未判 quorum-lost → 不给优雅 `--ack-alerts` advisory。**TFence 后二者都自我纠正**（`--remote`→READ-ONLY/exit2 `cluster_status_nats.go:116-136`；session rm→优雅 advisory）。所以这是**短暂误导窗口**，非永久缺口。**订正**：plan §12 "推翻 §11-U3" 为**假**——§11-U3 的 exit-2 READ-ONLY 正确，只是 TFence-delayed。
- **钉**：`92` leg-a——**fence-aware 对照**：探 on-broker socket（~1s 后翻 quorum-lost）vs `--remote`（窗口内仍 transient）→ 断二者在窗口内**不一致**（socket 已判 quorum-lost、--remote 仍 transient）；或 TFence 后断 `--remote` 自我纠正为 READ-ONLY(exit2)=GREEN。**（原 #42/#44 "永久"断法是误分类 RED、已废。）**
- **flip**：`--remote`/destructive-gate 在窗口内即给 quorum-lost verdict（缩短或消除 TFence 误导）。#43/#44 折入本条与 #41（`--remote` remedy 与 banner 同受 `JetStreamUnavailable` gate `cluster_status_nats.go:161-168`，非独立缺陷）。

### #45 — retire op 卡 `NATS_ROLLED_OUT`、永不达 terminal RETIRED（40/41 暴露；与 #31 grow-lock、与 plan §4 #37 mid-retire-resume 均相异）

- **编号（外审 round-2 M6 / round-3 M6）**：本停滞此前误标 "#37-family"，与 **plan §4 的 #37（mid-retire-resume）语义冲突**——故给它**独立干净的 #45**（#38/#40/#41 已被候选占、#43/#44 已折入 #42，见下）。#45 = retire START 后停滞；#31 = retire START 前被 grow-lock 拒；#37(plan §4) = mid-retire-resume；三者相异。
- **现象/机理**（41 M15）：一次 `cluster retire` 可 STALL 在 `NATS_ROLLED_OUT` 阶段（rehome/migrate 未完成）、永不收敛到 terminal RETIRED；随后下一个 retire 被 `already in flight` 拒（`cluster_operation_controller.go:33`）。
- **钉**：`40` R-retire 要求 terminal RETIRED op_state（非仅 roster-absence；stall 则 `assert_bug #45` → PRODUCT-RED）；`41` shrink else-branch 区分二因（#31 grow-lock vs #45 stall）、`product_red` EXPOSE 本停滞。**flip**：retire op 在 rehome/migrate 完成后可靠推进到 RETIRED。

## G-C（S7+S9）登记（#50+ / DOC-17+）— **实测中，随 Stage-B 真跑滚动补全**

> **G-C = roadmap 的最后一组**（S7 备份/灾备/凭据轮换 + S9 混沌对账/长稳）。plan = `docs/reviews/s7-s9-plan.md`。
> 编号从 **#50** 起（#25–#49 已用）；DOC 从 **DOC-17** 起（DOC-16 保留未用，勿静默复用）。
> 每条的 default 与两出口分支条件见 plan §4；**drill 内 `product_red "#N"` 字串与 plan §4 表零漂移**
> （G-B 的 M11 教训：drill 先写 `#42` 而 ledger 顶 #34 ⇒ 收工闸不可过）。

### [#29 续] blast-radius 扩充（G-C 实测，**不发新号**；正条见上文 `### #29`）— allocate-time 的 `agent_rejected:frpc_failed` 面
- **状态**：**LIVE-CONFIRMED（2026-07-17，drill 50 开发期 6 连跑：3 pass / 3 fail，~50%）**。
- **新面（既有登记只写了 crash-strand，这是 allocate 时刻的另一张脸）**：N=2 集群、agt1 的 `tunnel_addr`
  指向 brk1、`expose` **不带 `--on-broker`** 时，broker 会把 home 任意分给 brk1 或 brk2；落到 brk2（agt1 的
  **非**-tunnel broker）时 **expose 当场分配失败**：
  `error: expose failed: the agent couldn't start the local proxy; check the agent log … (agent_rejected:frpc_failed)`
- **机理**：同 #29 主条（`home.go:96-113` 对非-tunnel voter 返 nil / 不可投递）。**运维后果**：多 broker 下
  `expose` 不带 `--on-broker` 就是抛硬币，且错误串指向 agent 日志（`frpc_failed`），把运维引向 agent 侧，
  而真因在 broker 的 home 分配 —— **误导性归因**。
- **钉住它的**：drill **71**（#29 的 owner，数据面）。**50/51/52 一律 `--on-broker brk1` 钉死 home**，
  并在头注写明理由：它们的主题分别是 backup/restore 同一性、DR、凭据轮换 —— 让一个已登记的、属于别人的
  缺陷在这里随机开火，只会制造**误伤 restore/DR 的假红**，并把 drill 变成 flake 源。


### #57 — 在飞 tier-B 传输的 home broker crash 后终态 audit 永不写（悬空 start 行）

- **R16 状态（2026-07-22）：产品修复已落地，deploy-tier 证明仍欠 —— 本条保持 OPEN。** Lane B 实现「finalize-on-recovery」：在 `<ClusterDataDir>/xfer-inflight/<hash>.json` 落一份 node-local 持久 in-flight 账（put() 后、转发前写，四条 terminal 路径各自删），重启后的周期 pass 对「超过 tier 超时+slack 且无活 tracker 条目」的账目补发一条**确定性**终态（`Kind=failed, Code=home_broker_restart, Ts=startedAt+timeout`），其内容寻址 reqID 使任何重发在复制 ledger 去重。内审 M1 修正了致命一点：终态**确认提交后**才删账目，leaderless 时保留并下轮重试（否则恰在 #57 的旗舰窗口把证据也毁掉）。hermetic 钉：`TestXferInflightFinalizeOnRecovery` / `...RetainsLedgerWhenUncommittable` / `...DedupReqIDStable`。**未闭合的原因**：drill 96 的 1 GiB tier-B 上传这次仍在 docker kill 前抵达终态（本条自身记录的 in-sim 中断不可构造 gap），故悬空 start 行从未在部署层产生，补发路径未获实测演示。
- **状态**：源码确证；96 分区件已验证可注入（rc=124 静默丢包实测通）。
- **机理**：watchdog 挂 broker runCtx（`transfer.go:593`/`:704`）随进程死；`transferTracker` 内存 map
  （`:99-104`）重启 `newTransferTracker()`（`broker.go:602`）为空；`handleEvTransfer` 对迟到 finalization
  走 `preview==nil → return`（`:816-819`）静默丢弃 ⇒ 合成 `failed` audit 永不写。
- **钉住它的**：drill 96 臂 A（R-EXHAUST 四态；`start` 悬空无终态 → product_red）。

### #67 — 瞬时 JetStream 不可用被 tier-B push 当作**永久能力缺失**上报（零重试 + 错误指引）

- **状态（2026-07-22，G67）：拆分登记。`#67-A` 产品修复已落地并经 deploy-tier 复验；`#67-B` CLI 侧已修但
  **无 drill 级 oracle**，保持 OPEN；另登记三条 open sub-face。** 发现于 R16 部署层验证（drill 42 复跑 3），
  非 R16 回归——`git diff` 证明 R16 对 `internal/broker/transfer.go` 的改动全在 bucket-create **之后**，
  缺陷存在于已发布的 v0.4.7。修复方案见 `docs/reviews/g67-plan.md`（11 agent 对抗性 workflow 起草、主进程逐条验证定稿）。
- **缺陷说的到底是什么（勿扩大）**：quorum 缺失期间**拒绝** tier-B push 本身**不算错**——R=2 资产那时确实写不了。
  错的是**告诉运维的话**：拒绝文案**不含任何「瞬时、稍后重试」线索**，而同一条 push 几秒后零操作即成功；
  最坏那张面孔更断言了一个 broker 从未报告过的**永久能力缺失**并给出与故障无关的建议。
  **tether 在同一条路径上本就有这套词汇**（`transfer %s rejected (%s); retry shortly`）却不用；
  而 broker 自己的 reconcile 循环同一时刻正把同一状况**正确地当作瞬时**并每 ~100ms 重试
  （`d5: replica reconcile … 503 err_code=10008`）——**可重试通道存在，用户面 push 够不到**。

#### `#67-A` — broker 侧共享 deadline + 单次尝试　→　**FIXED（G67）**

- **机理（已由 RED-first 测试坐实）**：`handlePushReq`/`handlePullReq` 把**两次独立 JetStream 往返**
  （`xferBucketMaxBytes`→`jsStoreCeiling`→`AccountInfo`，与 `CreateObjectStore`）塞进**同一个 5s ctx、各一次**。
  而 `jsStoreCeiling` **吞掉** AccountInfo 的错误回退 statfs ⇒ 停住的 sizing 探测**不报错、只静默吃光预算**。
  `TestSizingStallDoesNotConsumeCreateBudget` 对修复前代码测得：承重的 create 拿到的 ctx 剩余 **-4.25 ms**
  ——**它连一次尝试都没得到**。这解释了为何实测 ~57 ms 的建桶会产出 5s deadline 错误（≈100× 余量）。
- **修复**：`internal/broker/xfer_provision.go` 新增 `provisionXferBucket`——sizing 独立 1.5s deadline；
  create 每次尝试独立 2.5s、最多 3 次、总墙钟 8s、`±25%` 抖动退避；分类由**独立的**
  `jsstream.IsTransientProvisionErr` 负责（**`IsMetaGroupNotReady` 一个字节没动**，它喂的是 reconcile 的
  永久循环，放宽会把永久错配拖成几天静默）。永久分类**恰好一次尝试**即返回。
  新增瞬时码 `jetstream_not_ready`（纯增量，`ProtoVersion` 不变）；`bucket_create_failed` 文案**逐字节不变**
  且**不含**任何重试词，两者不可混淆。nil-JS 拒绝从**空串**改为同时点名两种成因的真文案。
- **预算为何压紧**：`.push.req`/`.pull.req` 在 `isBroadcastClusterSubject` 里、走普通 `nc.Subscribe`，
  handler 直接作为 nats.go 异步回调注册**无 goroutine 包装** ⇒ **全 broker 的 push 串行在一条投递 goroutine 上**。
  in-handler 最坏 ≈9.5s（今日 5s 的 1.9×），仍远小于 `transferTimeoutTierA=30s`。
- **hermetic 钉**：`TestSizingStallDoesNotConsumeCreateBudget`（RED→GREEN 的根因裁决）、
  `TestPushHandlerRepliesTransientCodeOnStall`（**接线**——把 handler 还原即精确变红成
  `bucket_create_failed: create_bucket: context deadline exceeded`，突变检查实测过）、
  `TestXferProvisionRefusalDistinguishesTransientFromPermanent`（**双向**）、
  `TestXferProvisionPermanentMakesExactlyOneAttempt`、`...RetriesTransientWithinBudget`、
  `...SucceedsOnRetry`、`...SizingRefusalMakesZeroAttempts`、`TestIsTransientProvisionErr`、
  `TestIsMetaGroupNotReadyDeliberatelyNotWidened`。
- **deploy-tier oracle**：`drills/67-transient-js-refusal` 翻为 **GREEN 回归**——注入后的拒绝必须
  ① 带 `code=jetstream_not_ready`、② 含重试指引、③ **不**含 `broker has no JetStream`/`bump nats max_payload`，
  ④ **非空过牙齿**：brk1 的 `broker.err` 里必须读到 `tier-B bucket provisioning retried|gave up`
  ——**证明有界重试真的跑过，而不是只把文案改漂亮**。
  **内审更正**：这颗牙齿是 `not_covered` 而非 `_as_fail`，所以删掉重试循环是把 `nc_gap` 从 1 顶到 2、**不是**把 drill 打红；
  因此健康运行的 **nc_gap 基线=1**（face B）已写进 tsv，跑出 2 就意味着重试没在跑。先前台账说它「变红」是**说过头了**。
  该牙齿**刻意不接受** `gave up` 那一行：它在**永久单次尝试**时同样会打，用它当牙齿等于把重试循环删掉也能过。
- **deploy-tier 实测（2026-07-22，weilandserver）**：面 A 臂 **GREEN**，实际拒绝为
  `code=jetstream_not_ready tier-B object store could not be provisioned after 3 attempt(s) over 8s:
  create_bucket: context deadline exceeded — … usually transient …`，恢复 peer 后同一条 push 零操作成功。
  修复前同一注入下是 `code=bucket_create_failed create_bucket: context deadline exceeded`，无任何重试线索。
- **跑这次复验又抓出两个 drill 自身的 bug（留痕）**：① 断言写成 `sh -c "… \$_G67_OUT …"`——子 shell **不继承**
  该变量，于是三条断言实际在测**空串**，产出**两个假 FAIL + 一个空过 PASS**；已全部改走函数。
  ② 第一版非空过牙齿接受 `retried|gave up`（见上）。**两处都是"测试自己坏了"，不是产品问题——但如果不查清，
  第一条会被误读成"修复没生效"，第三条会被误读成"已验证不含误导文案"。**

#### 覆盖缺口（drill 自报，verdict 因此是 INCOMPLETE 而非 GREEN）

drill 在面 A 全绿之后**无条件**记一条 `not_covered[gap]` 声明面 B 未被本 drill 覆盖。
若不记，这个 drill 会报出一个干净的 GREEN，从而**暗示 #67 整条已闭**——那正是 ledger-crosscheck 存在的理由所指的
假完整形态。

#### `#67-B` — client 侧吞掉 caps 探测错误、据零值编造永久断言　→　代码已修，**但保持 OPEN（无 drill oracle）**

- **机理**：`cmd/tether/transfer.go` 的 `caps, _ := probeCaps(...)`（push 与 pull 两处）**丢弃错误**，
  `probeCaps` 失败即返回零值 `CapsResp{}` ⇒ `JetStreamReady=false` 且 `MaxPayload=0`；而 `handleCapsReq`
  对 `not_a_member`/`session_not_found_or_deleting`/`store_error`/`actor_invalid` 也回 `OK:false`+零值
  ⇒ **四种状态坍缩成同一字节模式**。`chooseTier` 据此编造出永久断言。broker **从未**这么说过：
  `JetStreamReady` 取自**静态** `b.js != nil`，与临时 503 无关。`MaxPayload=0` 还顺带把 tier-A/B 分界悄悄抬回 8 MiB 默认值。
- **修复**：引入带类型的 `capsProbe{capsUndetermined|capsRefused|capsOK}`；**非权威**答案一律**乐观走 tier-B
  并在 stderr 打 warning，交由 broker 裁决**（权威答案就在一次 RPC 之外，本地拒绝等于再编造一个断言）；
  只有 `capsOK && !JetStreamReady` 才拒绝，且 `max_payload` 建议**仅在提高它确实能让文件走 inline 时**才出现。
  `tierAInlineCeiling` 对**每一个已知**测量取小、**绝不因测量缺失而放宽**。两条误导字面量已从 CLI 路径删除。
  钉：`TestChooseTierNeverInventsACapabilityClaim`、`TestTierAInlineCeilingNeverWidenedByAMissingMeasurement`、
  `TestCapsProbeWarningsAreHonest`。
- **为何仍 OPEN**：面 B 只有**一次人工探针**复现（SIGSTOP 冻结 peer），而 SIGSTOP 因会毒化 brk1 的 route I/O
  已被弃用；clean-stop 注入**不复现**它。**没有 drill 级 oracle ⇒ 不允许标 CLOSED。**

#### 残留限制（G67 **没有**声称修掉的东西 —— 实测数据在此，勿在验收时误读）

- **G67 的契约是「诚实 + 可重试」，不是「第一次必成」。** 有界重试的墙钟预算是 **8s**（受 head-of-line
  blocking 反向约束，见上），超过它仍会拒——只是现在拒得诚实：`code=jetstream_not_ready` + 「稍后重试同一条命令」。
- **实测（2026-07-22，weilandserver）**：
  - **空载**部署层：grow 后**第一次** tier-B push **1.66s 成功、零重试**（`provisioning gave up` 计数 0）
    ⇒ 常态下 8s 预算绰绰有余。
  - **多 drill 并发**（一台机上 6–9 个 clustered-JS 集群）：3 次尝试跨 8s **全部超时** ⇒ 该负载下
    post-grow 的 meta 窗口 **> 8s**，运维需要按提示重试一次。
- **更深的正解在 grow 侧、不在 push 侧**：`cluster add` 在 JS meta 尚不能放置资产时就报成功。真正消除
  「grow 完立刻传文件会失败」应当是**让 grow 不要过早宣告成功**，而不是让 push 无限等。**本增量不做**，登记为
  sub-face 4。
- **因此 drill 断言形状也改了**（`drills/lib/setup-forcesingle.sh` 的 baseline 与 drill 67 的 CONTROL(before)）：
  契约断言为「要么一次成功，**要么**被 `jetstream_not_ready` 拒且**紧接着的重试成功**」。
  **这不是放水**——修复前的**终态** `bucket_create_failed` 仍然硬失败；只有产品**明确文档化并指示**的那条路径被接受；
  且若瞬时拒绝之后的重试**也**失败，记 product_red（说明文档化的补救根本不work）。

#### open sub-faces（本增量明确未做，登记以免遗忘）

1. **broker 的 JetStream 探测是一次性的**：`b.js` 由启动时一次 1s `AccountInfo` 确定，失败后**永不重探**
   ⇒ 忙主机上探测超时会让该 broker 一直拒绝 tier-B 直到重启。懒重探是正解，但 `b.js` 是被大量点直接读的
   裸字段，需 `atomic.Value`/`RWMutex` 改造 + 独立 `-race`，不进本增量；已改为拒绝文案**同时点名两种成因**。
2. **签名 (ii)** `push (tier B prepare): context deadline exceeded`（rc=75）的根在 `transferHomeGate`
   对未解析 home 的静默，属集群路由议题。
3. **面 B 的 drill oracle 缺失**（见上）。
4. **`cluster add` 过早宣告成功** —— **G69：产品修复已落地（hermetic 钉 P1–P10），deploy-tier 证明仍欠。**
   grow 返回时 JS meta 可能尚不能放置 R=N 资产，于是「grow 完立刻传文件」在负载下会撞上一次瞬时拒绝。
   G69 在 join 终态门加了一个合取项：`events` 流的 `Configured` 与 `Assigned` 都达到目标副本数才进 SERVING；
   到期则**降级推进**（绝不 BLOCK）并在 op timeline 留 `WITHOUT proving JetStream placement`。
   **为什么仍欠证明（内审 G-3 更正了主进程的说法）**：drill 67 那条 sub-face-4 gap **不是无条件的**——
   它只在「首推失败 ∧ 重试成功」时触发，而**空载主机产生不了这个前提**（实测空载 1.66s、零重试）。
   更要命的是 tsv 里记录的**修复前**基线本来就是 `nc_gap=1 pass=17`，所以「修复后 nc_gap=1」与修复前
   **逐字节相同、零判别力**。主进程一度把它当作验收证据，**那是把巧合当证据**。
   现已改为**正向** oracle（grow 后断言任何 op timeline 里都**没有** `WITHOUT proving` 降级条目），
   它每次运行都可判、不依赖负载。
   **饱和实测（2026-07-22，7 路并发 = 当初产生原始失败的同一 regime）**：drill 67 两条判据**均成立**——
   正向 oracle PASS（grow 真的证明了放置、未降级），且 sub-face-4 gap **未触发**（首推不再需要文档化重试）；
   同批的 grow 家族 10/11/42/92 全 GREEN，这同时也是 `jsGateExpiryReserve` 那个 BLOCKER 修复在真实压力下的证据。
   **限度（勿读过头）**：这是**单臂**证据，不是差分——跑修复前那一臂需要在 51 改+20 新的未提交树上做 stash 构建，
   风险不对等，故未做。所以它证明「修复在原始失败的负载下成立」，**不能排除**「这次恰好没触发」。
- **同一次饱和跑暴露的残留（如实登记，非吸收）**：drill 12 的共享 fixture 命中 `#67 residual` 护栏——
  首推被判瞬时拒绝后，**紧接着那一次重试也失败**。即 **G69 收窄了 post-grow 窗口但在极端负载下没有关闭它**。
  产品文案承诺的是「retry the same command **shortly**. If it **persists**, …」——一个**短窗口**而非「一次必成」，
  所以 fixture 原先断言「恰好一次重试成功」是**相对产品自己的承诺过度规定**，已改为**有界多次**（5 次 / ~25s）
  并保留原有牙齿（窗口内始终不成功仍记 product_red），且把成功时的**尝试次数打进日志**，使恶化表现为数字而非静默通过。
- **两次七路饱和跑的实数据（这就是"收窄但未完全关闭"的量化依据）**：
  | 跑次 | 首推一次成功的 fixture | 需要重试的 | 结果 |
  |---|---|---|---|
  | 第 1 次 | 2/3 | drill 12（首推被拒 **且紧接的那次重试也失败**） | PRODUCT-RED（护栏正确触发） |
  | 第 2 次 | **3/3**（12/42/92 全部 first attempt） | 0 | 7 drill 全部回到期望值，0 product_red |
  ⇒ 残留是**间歇性**的：G69 把 post-grow 窗口收窄到「多数情况下首推即成」，但在极端并发下**仍可能**需要
  产品文案所说的那个短重试窗口。**未声称已关闭。**
   **已获得可计数的 owner**（内审 M-honesty）：drill 67 的 CONTROL(before) 在「首推被判瞬时 + 重试成功」时记一条
   `not_covered … sub-face 4` gap。此前该分支只 `log`，导致 DRILL-VERDICT 与「一次成功」**逐字节相同**——
   于是最初发现 #67 的那个探测器（drill 42 复跑 3 挂在这条 baseline）变得**不可达**，而成因仍未修。

### #66 — 滚动升级 phase-2（leader 自身 reload）有一个有界的 leader-hop 写不可用窗口

- **状态**：**LIVE-CONFIRMED（2026-07-21，drill 30 臂 PHASE-2 CONTINUITY，scene 实证）**。deploy-tier 定案实验（`release-readiness-followups.md §6.1` 的「30 HALT 窗定案」）：serial ×2 + -j3 ×1 = **1 次 fire / 2 次 clean（间歇）**。
- **现象**：`30-rolling-upgrade.sh` 的 phase-2 写探针（`ctl1` 每 0.3s 一发 `session create` = 一次 raft 写）在**完成滚动**（含 leader 主机自身 reload）期间**偶尔**记到 `WRITEFAIL`。scene（本会话 serial #1 首次命中即抓）显示失败瞬间：leader **已从 brk1 转到 brk2**（发生 leader 换届）、三 broker 全 VOTER / LAG=0 / STREAMS 3/3 / TOPO 收敛✓ / 全可达——**集群健康、已重收敛，不是 wedged、不是 IO 停滞、不是 no_responders**。
- **机理（scene 定案，非预判）**：roll 是 leader-last，leader 主机用 PID-preserving `syscall.Exec` 原地 re-exec；但 raft 节点随之重启、in-memory raft 态丢失并从盘重读 ⇒ leader lease 过期 ⇒ 重选举（brk1→brk2）。这 ~亚秒窗口内一发 `session create` 无 leader 可达即返非零（`WRITEFAIL`）。**CLI 命令级不自动重试**（architecture C.3：`run/exec/session.* 超时即失败、由发起方决定重试`），所以恰落在该窗口的写就暴露为失败。**间歇**：探针 0.3s 节拍是否恰好命中亚秒重选窗决定命中与否；serial 也会 fire（**非并行专属**——`-j3` 并未明显放大，本次 -j3 反而 clean）。
- **#66 只是 roll-窗口写 blip 的一种确证形态，不排他**（外审 M-1 收窄，2026-07-21）：主进程登记时的证据是 **1 个 phase-2 换届 scene**（样本量 serial×2+-j3×1），据此写下的「phase-1 CONTINUITY 恒 clean」是超出证据的过强概括。外审独立 serial 复跑的**样本 #4 已证伪它**：一次 **PHASE-1 CONTINUITY 命中**，scene 显示命中瞬间 **leader 未换届（brk1 保持 leader、HEALTHY-HA、view authoritative）、roll 期无 systemd 重启（MainPID-UNCHANGED 亦 PASS）**——**不符合 #66 的 leader-hop 机理**（无重选举换届），失败签名行未存档故形态**未定性**（更接近 branch-3 暂态/no-responder 类或一个未定型形态，不硬归）。所以：#66 = phase-2 leader-hop 命中的确证缺陷（其换届 scene 是真证据）；phase-1 命中是**另一种未定性形态**，不属 #66，watcher 常驻正为继续自捕这类样本以待定因（样本 #4 即其自捕能力的实证）。
- **不是什么**：#66 本身不是 branch-3 的 host IO-contention infra-flake（其 scene 集群健康、有明确 leader 换届）。
- **候选修**（产品，留后续 phase）：在 CLI 的 raft-write 路径（`session create` 等）加**有界 not_leader/no-responder 重试**以骑过 leader-hop 窗；或显式把「滚动升级 leader-hop 有界写窗口」文档化为可接受语义。二者皆非本 phase 范围。
- **钉住它的**：`drills/30-rolling-upgrade.sh` 的 **PHASE-2 CONTINUITY** 断言（谓词**保持严格**，不放宽 grep——mandate：真相由本 gotcha + tsv owner 承载，不由弱化谓词掩盖）+ **scene-capture watcher**（观测-only，命中瞬间抓 leader/status/journal，已 committed 为常驻取证器）。间歇命中时 drill 落 ASSERT-FAIL（否则 INCOMPLETE，仅 b/c 两结构 gap）；该 ASSERT-FAIL 现**可归因 #66**，非无主 flake。

### Q4 — 分区中 `session create` 写**已提交**却报失败 + 非幂等（R6 CONFIRMED-DEFECT，机理①；产品批）— **产品修复 R14（2026-07-19）**

- **现象（R6 实测，drill 96 canary2）**：分区中经存活 follower 写 `session create canary2`，14 次全非零，**第 1 次**就是落地时刻：
  `rc=70 elapsed=1.37s error: session create failed: broker: session "canary2" not visible after commit (apply lag)`；
  而经另一 broker `session ls --json` 读得回 `state=ACTIVE`。
- **机理**：`internal/broker/clusterwrite.go` 的 `readCommittedSession` 原 50×20ms=**1s** 上限不够——forward 在 **leader commit**
  返回、follower 本地 Apply 滞后（实测 1.37s）⇒ read-back 超时报「committed-but-reported-failed」。**复合缺陷**：`session create`
  **非幂等**——首超时后每次重试 leader 的 `PlanCreate` 存在性复查返 `already_exists`+rc=70 ⇒ `poll_until` **结构上永远转不了绿**；
  `error_hints.go` 无 `already_exists` 条目。
- **产品修复（R14）——为何不用 owner-fp 幂等**：初版对 `ErrAlreadyExists` 走「同 owner ⇒ 视作本次创建返成功」的幂等出口，
  被 `make e2e-parallel` 的 `test/d9`（`SessionCreateRoutesThroughRaft`/`TwoBrokerJoinReplicates`）打回——那两条**同 actor 连做两次
  create**、以「第二次被拒」证明第一次已 commit 到复制 FSM。**一次超时重试与一次全新同名 create 在 broker 侧字节无别**
  （无跨进程幂等 token），故任何让「同 owner 第二次 create」成功的方案都**必然**破坏该 D9 契约。改走正确得多的一招：
  - **(核心) read-back 超时非致命 ⇒ 首次即报成功**：`proposeOrForward` 返 nil **就已经意味着写 commit 到 raft**（leader 路径
    Propose 等本地 Apply；forward 路径在 leader commit 返回）。故 `createSession` 先试 read-back 取权威 `created_at`；**超时不再当
    失败**，而是**返 best-effort success**（`{SID,OwnerFP,State=ACTIVE,CreatedAt=now}`，权威 created_at 下次 `session ls` 收敛）。
    ⇒ 首次尝试即 rc=0，**根本不进重试循环**，`already_exists` 死结不复存在。**绝不造假成功**——只有 `proposeOrForward` 已
    commit 才走到这条。**重名（任何 owner）仍返 `already_exists`**，D9 契约原样保留。
  - **(a) 放宽 read-back 窗口**：`sessionReadBackAttempts` 50→**150**（3s）——覆盖 R6 的 1.37s apply-lag，常态仍取到权威行；
    3s 之内落在 5s ctl deadline 内，broker 回包来得及；超过则走 best-effort。
  - **(c) 补 hint**：`error_hints.go` 加 `already_exists`（若上次 create 请求本身超时无回包，写仍可能已 commit ⇒ `session ls` 确认）
    + exit class 64（真冲突=取名冲突、运维可动，非 tether bug 的 70）。
- **改的是 raft 写路径**：`-race` 全绿；success 只在 commit 已确立后返，不引入假成功。
- **测试**：`TestSessionCreateSucceedsAndDuplicateStillRejected`（钉住 D9 依赖的重名拒绝契约）· `TestSessionCreateReportsSuccessWhen
  CommittedButNotYetVisible`（把 broker 读 DB 指向另一空 DB、FSM commit 到 node 自身 DB ⇒ read-back 超时但写真在 ⇒ 断言返成功
  且 `n.RODB()` 确有该行=非假成功）· `TestSessionReadBackWindowCoversMeasuredApplyLag`。变异（去 best-effort）实测复现 R6 的
  `not visible after commit (apply lag)`。
- **单模式不变**：`session create` 单模式仍走原子 `session.Create`、重名仍 `already_exists`（无 read-back、无此失败模式）。
- **drill 96 D3/D6 侧翻正待主进程**（首次即成功后 poll_until 应转绿）。

### DOC-28 — `docs/usage.md` 未定义 `run` 会话跨 broker 重启的语义
- **状态**：登记（drill 96 臂 B 的 NOT-COVERED 理由，源码 SB-96-3 已闭合行为面）。
- **现象**：`run` 会话经显式 `--nats-url` 连到被杀 broker 时，watchdog 15s 合成 `agent unreachable: no heartbeat` 优雅终止（`run.go:453-456`）——这是**有意设计**（GREEN），但 usage 文档未说明。
- **修法**：usage 补一句说明 run 跨 broker 重启 = liveness watchdog 优雅终止（可用 `TETHER_RUN_LIVENESS_TIMEOUT` 调）。

### DOC-23 — 砖化态下 `rotate-tunnel-cert` 的补救提示不可达

> **R11 修复（2026-07-19）**：pin-mismatch 文案去掉指向连不上的 `rotate-tunnel-cert`，改为 FILE-level 恢复（还原 tunnel-cert.pem/tunnel-key.pem 再重启）。CLOSED。
- **状态**：**产品侧 FIXED（R11 P12，2026-07-19）**——错误文案已改为可达的 FILE-level 恢复。
- **现象（已修）**：tunnel cert pin-mismatch 使 broker 拒启（fail-closed，正确）；旧错误串的第二条补救「re-run
  `tether cluster rotate-tunnel-cert`」在该态**不可达**——`wireClusterEarly`（`clusterwrite.go`）在 admin socket
  建立**之前**返错即退，命令连不上。
- **修法（R11 P12）**：pin-mismatch 错误串（`tunnelCertPinMismatchError`）**去掉指向 `rotate-tunnel-cert` 的补救**，
  改为明确的**文件级恢复**：把 `<secrets>/tunnel-cert.pem` + `tunnel-key.pem` 还原成 pinned 的那对、再重启。
  测试 `TestTunnelCertPinMismatchErrorPointsAtFileRestore`（断言不再提 rotate-tunnel-cert、且指明还原文件+重启）。
- **钉住它的**：drill 52 臂 **A8**（砖化态跑该命令 → assert_refuses「no such file|connection refused」）——产品已修，
  drill 可另加断言：错误文案指向文件恢复而非该命令。

### DOC-27 — runbook §5:524 的 `cluster backup --out /var/backups/…` 示例在 stock 装机上跑不了
- **状态**：**CLOSED（2026-07-21）**——runbook §5（9 处）+ `cmd/tether/cluster_backup.go` 的 `--help` 示例改用 `/var/lib/tether/backups/…`（install.sh 已建 `LIB_DIR` 且 tether 可写）+ 加 off-node caveat；drill 50 臂 C 翻为正向回归，**deploy-tier 实测 drill-50 GREEN（pass=87，0 gaps）on weilandserver**、`DOC-27 CLOSED …runs as User=tether` 断言 PASS。tsv row 50 已同步 GREEN。下文为历史立项记录（原 LIVE-CONFIRMED 2026-07-17，drill 50 臂 C）。
- **现象**：逐字照抄 runbook `:524` 的示例 → 失败。**实测真串**（非预判）：
  `error: cluster backup: create bundle dir "/var/backups/tether-2026-07-17-799" (must not exist): mkdir /var/backups/tether-2026-07-17-799: permission denied`
- **机理**：`install.sh:491` 只 `install -d -o tether -g tether` 建 `LIB_DIR`/`LOG_DIR`；**`/var/backups` 从未被建**
  且 root-owned ⇒ broker 以 `User=tether` 跑 `MkdirAll` 撞 EACCES。（Stage-A 曾预判会撞「误导的括号提示」，
  **实测推翻**：真正撞的是 `create bundle dir … permission denied`，故原 gotcha 立项降为 DOC。）
- **怎么修**：文档改用 `/var/lib/tether/backups/…`（install.sh 已建且 tether 可写），或 install.sh 建 `/var/backups/tether`
  并 chown tether，或让 backup 的报错直接给出可执行的补救命令。
- **钉住它的**：`drills/50-backup-restore.sh` 臂 **C**。
  本臂同时是 **R-SUPPLY-ORDER** 的前置证据：它先证明「不供给备份库时 tether 做不到什么」，S0-备份库才作为 `[env]` 出场。
- **修复轨迹（2026-07-21，两阶段）**：runbook §5（9 处）+ `cmd/tether/cluster_backup.go` 的 `--help` 示例订正为
  `/var/lib/tether/backups/…`（install.sh 已建 `LIB_DIR` 且 tether 可写）并加 off-node caveat；臂 C 翻为**正向回归**——
  成功=`assert_ok "DOC-27 …runs as User=tether"`、意外 perm 失败=`product_red "DOC-27 REGRESSION"`、其它=`_as_fail`。
  第一阶段落地时 weilandserver 一度不可达（no route）、tsv row 50 暂留 PRODUCT-RED；**同日晚些时候 weilandserver 恢复，
  `./remote.sh drill 50` 独立复跑 GREEN（pass=87，0 gaps）后已翻结**——本条状态行 CLOSED、tsv row 50 转 GREEN（见顶部状态行；已无「drill 码期望绿 vs tsv 期望红」的待验证差）。保留本段仅为演进轨迹，不含现在时的 OPEN/PRODUCT-RED 断言。

## 文档缺陷（DOC-n，不占 gotcha 号）

产品**文档**缺陷单列于此（roadmap §5：批量修文档是独立小增量，S 系列只登记不顺手改）。

- **DOC-3（确认，S1 顺带经命令树生成器暴露；随 S5-31 一并定格/修）**：`error_hints.go:34`
  （`url_not_allowed_local` 提示）与 `docs/usage.md:1443` 都让用户「检查 **agent 的**
  `--upgrade-url-allow` flag」，但 **agent daemon 根本没有该 flag**——它只存在于 `serve`（broker 侧，
  `cmd/tether/serve.go:267`）。命令树 golden（`cmd/tether/testdata/command_tree_golden.txt`）证实
  `tether agent` 的 flag 集不含 `--upgrade-url-allow`。**机理**：agent 侧升级 URL 白名单**硬编码、不可
  operator 配置**，而 hint/手册指向一个不存在的旋钮。**修**：要么给 agent 加真的 `--upgrade-url-allow`
  接线，要么改 hint/手册指向 broker 侧配置。**钉住它的**：S5-31 的 `31-node-upgrade-fleet.sh`（未开工）。

- **DOC-5（S1 核实为「非缺陷」，登记备案）**：`exec` 远端进程被信号杀时 CLI 退**扁平 128**（无 signal
  号）——agent 返回 `ExitCode()=-1`（`internal/agent/exec.go:202-208`），ctl 塌成 `os.Exit(128)`
  （`cmd/tether/exec.go:131-135`）。**核实结论**：`docs/usage.md:669-670` **已正确**记载「信号杀变 128…
  当前不传具体信号号」，故**不是文档缺陷**；「128+signo」是**有意**延后的 v2 wire-proto 能力。由
  `60-user-journey` 的 J7（`rc==128` 且 stderr `terminated by signal`）钉住这个扁平-128 契约。

**S2 登记（主进程裁定：以下为**有意设计**或**文档/命名漂移**，非 gotcha；drill 中立钉现实）：**

- **DOC-6（O4 裁定：eviction ≠ ban，有意）**：被踢 agent 的 nkey 用**仍有效的 session PIN** 可**重新
  provision** 回来（`80`/`81` D1 实测 re-join → ONLINE）。**机理**：`internal/authcallout/handler.go:246-289`
  evict 后 `Lookup`→`ErrNotProvisioned`，`ProvisionWithPIN` 用不变 PIN 成功——**无 nkey/fp denylist**。
  **语义澄清**（这才是 DOC 而非缺陷的理由）：evict 删的是 provisioning 行、不封 nkey；「nkey 泄漏立即吊销」
  用例中，**仅 nkey 泄漏（session PIN 未泄）时重入仍需 PIN = 实际已吊销**；若 PIN 亦泄，正确工具是
  rotate-PIN / `session rm`，evict 从来不是。**修（若将来判为安全缺口 → 升 gotcha #28）**：evict 加 nkey
  denylist 或强制 PIN 轮换才算真吊销。
- **DOC-7（O4 裁定：有意纵深防御，非 gotcha）**：`agent join <forged-invite>` 在**无 `--expect-account-pub`
  且 invite 带 inline `seed=`** 时，会**写下 agent.yaml 残留**（`82` T2 实测 yaml 存在）。**机理**：
  `cmd/tether/agent_join.go:59-73` `brokerURL=inv.Seed` 非空 → `writeAgentConfig` 被调（先于 roster-cache
  预热）。**为何是 DOC 非缺陷**：roster-cache 由 `AdoptDecision` **签名验证门控**（验签失败→`!ok`→不写
  cache，`agent_join.go:75-83`），残留 agent.yaml **无信任价值**、`agent doctor` FATAL 兜底；pin=OOB 信任锚，
  用户不传 `--expect` 即视为信任该 OOB invite。`docs/simcluster-coverage-roadmap.md` line 322「无半写
  agent.yaml」是 **overclaim**（应订正）。**修（若将来判缺陷）**：join 在 manifest-verify-against-pin 通过
  前不落 agent.yaml。
- **DOC-8（§3.0 承重发现）**：auth_callout CONNECT-deny 的**具体 reason 对 client 不可见**。tether handler
  拒（`internal/authcallout/handler.go:400-405` `resp.Error=reason`）→ nats-server 发**通用**
  `Authorization Violation`（nats-server `client.go:2434`）；tether reason（`not a member of session` /
  `invalid PIN for session` / `not provisioned…` / `session not active`）**只 server 侧 log + `$SYS…AUTH.ERR`**。
  `handler.go:117-118` docstring「the client sees a clear auth error」**误导**。非缺陷（有意 info-hiding），
  记之使未来读者**不在 client 断 reason 串**误报 bug——所有 CONNECT-deny drill 臂必配 server-side 判别子。
- **DOC-9（inventory §1.1 row-27 订正）**：`member_joined` 事件**只发 `via:"pin"`**（`handler.go:286` agent、
  `:353` ctl）；已 member 的 **fp-reconnect 路径不发任何事件**（`handler.go:320-330` 返 nil）。清单 row-27
  「事件含 via=pin/fp」不精确 → 订正为「via=pin only；fp-reconnect emits no event」。**80** owns。
- **DOC-10（inventory §1.1 row-26 订正）**：`session_created`/`session_destroyed` 去 **`events` 流**（subject
  `tether.v2.sys.events`，`internal/broker/audit.go:36-48`），**非** `history-<sid>`；`admin audit` 只 tail
  `history-<sid>`（`admin.go:36`），`session_destroyed` 且**后于** history-<sid> 删除（phase ②）→ **`admin
  audit` 看不到它**。row-26「history/admin audit 可见」订正为「events 流可见（member sys.events sub）」。**81** owns。
- **DOC-11（承诺未实现 → probe 钉住）**：`docs/usage.md §5.10`（`:578-583`）承诺 `session rm` 会 broadcast
  `sys.events{type:session_deleting}` 使 agent 主动拒该 sid 新调用——**无 `pubSysEvent("session_deleting")`
  writer**（唯一字面 `internal/broker/cluster_forward.go:255-262` 是 wire error-kind classifier，非广播）。
  **真机制（deploy-tier 实测，R9 订正）**：refusal 全在 **broker 侧**、agent 从不参与——但**分两条路径按
  session 状态**：① session 存在但 **DELETING** → 应用层 `session.IsActive` gate 返 `session_not_found_or_deleting`
  （`exec.go:49-55`/`run.go:41-47`/`expose.go:178-184`/`broker.go:1145-1154`）——N=1 **同步** rm 窗口不可达 →
  **hermetic-only**；② session **已删**（rm 完成后，deploy 实况）→ 下一次 session-scoped CONNECT 在 auth_callout
  `ensureMember` 处被拒（session-not-active → 通用 `Authorization Violation`，`handler.go:317-319`）——**从不到达
  应用层 gate**。**钉住它的**：`81` E3a/E3b/E3c（验 ② 的 CONNECT-deny broker 强制路径；DOC-11 核心「无 agent
  广播」不变）。**修**：正 usage §5.10 / architecture H.1，或加 writer。**次要 drift**：usage §5.10 说「H.5」，
  code 注为「H.3」（`audit.go:53-72`）。
- **DOC-12（architecture H.1 无 writer / 命名漂移）**：H.1 承诺的 `kicked` / `agent_unregistered` /
  `rotated_pin` **均无 writer**——`kicked` 实际发的是 `agent_evicted`（`internal/broker/admin.go:93`，命名
  drift）；`agent_unregistered` 无 unregister verb；`rotated_pin` 无改-PIN verb（PIN 只能 rm 重建）。**修**：
  正 H.1 或加 writer（另立增量）。**80** owns `rotated_pin`；**81** owns `kicked`/`agent_unregistered`。

**harness 注释漂移（sim 脚手架，非产品文档；S2 随批修一次）：**
- **DOC-13**：`image/provision-node.sh` agent 角色注释宣称「Agents onboard via `tether agent join <invite>`」，
  但 sim `cmd_agent_join`（`simcluster:332-343`）实走 `--pin` 首连 + 手写 system unit。S2 已修注释如实说明两条
  onboarding 路径（`--pin` 首连保留 + 真 C2 `agent join` 由 drill 82 演练）。
- **DOC-14**：`drills/lib/agentyaml.sh:14-16` 注释「S0-隧道… is S3's job」——S0-隧道 已在 S2 落地，S2 已改注释
  为「landed in S2」。

**S2 内审顺带发现（R24；产品文档缺陷，非 gotcha）：**
- **DOC-15**：well-known manifest 相对 `cluster seeds publish` 有**至多 30s 新鲜度滞后**——broker 缓存 manifest
  bytes 到 `nextCheckAt = signedAt + manifestRecheckInterval`（`internal/broker/cluster_manifest.go:22`，30s），
  一个在 30s 窗口内的请求会拿到**发布前的 seed-less** manifest，30s 后的请求才重签含 seed bundle。**非安全洞**
  （AdoptDecision 的 generation 单调性使陈旧 manifest 只是「暂无更新」，onboarding 亦不依赖即时新鲜=invite 带
  inline seed）；但 `usage.md`/`cluster.md`/`broker-ops.md` **零 user-facing 说明**（与 #27 同族的 doc 缺口）。
  `82` M1/M2/C1 poll 过该窗口（诚实注记，非静默）。**修**：usage 文档化该滞后，或把它变成 82 的显式 labeled probe。

**预登记指针（roadmap 研究期发现，未在 S1/S2 核实立项）**：
- **DOC-1**：`usage.md §5.15` 尾段「cluster 不支持 proxy」与 C5 现实相悖的残留旧文（S4 核实）。
- **DOC-2**：`recovery diagnose`/`resnapshot` 未入 cluster.md/runbook 命令文档（S6 核实）。
- **DOC-4**：architecture P8 测试原型的「SIGSTOP broker 期间 agent 起进程」经产品路径不可达（S9-94 修订）。

## harness 保真度债（sim 脚手架，非产品缺陷；随批清偿或登记）

- **FD-1（S1 登记，不阻塞）**：sim 烘焙的顶层 `/home/sim/.tether` 是 **0755**，而真 install.sh 建的是
  **0700**（`image/provision-node.sh:65` vs `scripts/install.sh:315-317`）。S1 的传输策略面（allow_roots
  三态）**不读该目录 mode**，故 61/62 不依赖它，S1 不阻塞。`drills/lib/agentyaml.sh` 写的 `agent/<sid>`
  子树是 0700、`agent.yaml` 是 0600 sim:sim（镜像 install.sh 真实形态，Mandate ③）；仅顶层目录 mode
  偏离。**处置**：登记为 §1.4-邻接的保真度债，将来若某批断言依赖顶层目录 mode 再对齐（绝不静默 chown
  烘焙镜像掩盖之，Mandate ①）。

## OQ-2 — `62-remote-fs-safe` 的 NOT-COVERED 定格（feasibility spike 结论）

`62-remote-fs-safe` 是 roadmap OQ-2 的 feasibility spike。**实测结论（2026-07-11，weilandserver）**：

- **可行的部分（GREEN，triage 分支 b = FUSE-approx）**：用一个 `fuse.hangfs` FUSE 挂载（`image/hangfs.py`）
  忠实复现「挂死的网络挂载」——它被 `spawnsafe.classifyFstype`（前缀 `fuse.`）判为 hangable；SIGSTOP 其
  daemon 后 statfs 阻塞，agent 的**有界探针如实快速失败、且不 wedge agent**（实测）：`exec --cwd <死挂载>`
  → `remote_fs_unsafe_cwd`；`exec <死挂载>/abs-argv0` → `remote_fs_unhealthy`；`exec --safe --cwd`
  （mode:off 下 per-call 升级）→ `remote_fs_unsafe_cwd`。三臂 + alive-control（wedge 后普通 exec 仍工作）
  全 GREEN。
- **NOT-COVERED（Arm 2，实测理由）**：**真不可中断-D** 需 kernel nfsd + hard mount；而观察
  **mode:off-WITHOUT-safe 的遗留裸挂死**会驱使 agent 对死挂载做**无界** chdir/exec。两者都会 wedge
  agent / 共享的 weilandserver（实测：wedged FUSE 编排即便有 timeout 也曾挂死宿主 5 min）。本 drill 的
  FUSE daemon 是 **T/S 态、kill-9 可收割**的**近似**（drill 的 D-判别子实测钉住），把它等同真-D 即
  Mandate-① false-GREEN。**留给专用硬件/隔离宿主**，不在共享 sim 宿主上跑。remote_fs 的判定逻辑本身
  hermetic-密（`internal/spawnsafe` 单测），此处部署增量 = 真 FUSE 挂载 + 真 bootHangable 扫描 + 真有界 statfs。

</details>

### #69 — `retire --compromised --require-credential-rotation` 不跟随 leader，把 `not leader` 直接抛给运维

- **状态：🔴 OPEN（候选产品 UX 缺陷；simcluster-accel D3 于 -j6/-j12 并发下暴露）。**
- **现象**：并发/换届期间对**非 leader** broker 发 `tether cluster retire --compromised
  --require-credential-rotation`，CLI 返回 `error: not leader`（rc=77），要求运维自己去找 leader 重发。
  drill 52 D-spine 两轮 -j6 + 一轮 -j12 均复现此签名（`D-spine UNJUDGEABLE — retire --compromised
  --require-credential-rotation failed for an unclassified reason (rc=77): error: not leader`）。
- **归因**：M4 判为 LOAD-SENSITIVE——单 leader 稳定（solo）时永不触发，独跑转 GREEN。所以不是回归，是并发暴露的
  产品行为：这条破坏性运维动词没有透明地路由到 leader、也没有 retry-with-redirect。
- **产品修复方向（交产品增量，不在 simcluster-accel 范围）**：`retire --compromised` 应跟随 leader 或在
  `not leader` 时带重定向重试，而不是把 leader-hunting 甩给运维。**禁止在 drill 侧对产品动词加重试循环**
  （Mandate ②）——simcluster 的职责是暴露+钉住，不是代劳。
- **部署层钉住**：drill 52 以签名绑定 band `ASSERT-FAIL@#69@sig:retire-not-leader` 记录，红时归 MATCH-BAND
  （仍阻断、已归因），签名变了则回落 DEVIATION。

### #70 — grow_to_3（N=3 HA 形成）在高 -j 并发下时序不稳（DELIBERATELY NOT banded）

- **状态：🟡 OPEN（已知 sim/product 并发时序特性；simcluster-accel B1/D2 于 -j6 暴露）。**
- **现象**：`grow_to_3` 的 N=3 集群形成是 SINGLE no-retry 尝试（drill 30 owns #31，故不得靠重试洗掉一次
  grow 失败）。-j6 满并发时，多个 drill 同时 grow 会饿死 raft VOTER 晋升，`grow_to_3` 偶发 RED（drill 30
  run-1、drill 96 run-2 均命中签名 `grow_to_3 (N=3 HA …`）。独跑/低 -j 时 grow 顺利完成。
- **归因**：M4 判为 LOAD-SENSITIVE（独跑转 GREEN/INCOMPLETE 的预期态）。这是 README「CAVEAT (grow-
  concurrency)」+ OQ-8 记录的 grow-并发 flake，不是 lever 引入的回归。
- **⚠ 为何 DELIBERATELY NOT banded**：真 grow 回归也会失败在**同一个 `grow_to_3` 断言**上——签名无法把
  flake 与回归区分开。给它签名绑定 band 会让**真 grow 回归被当成已知 flake 吞掉**（正是 simcluster-accel
  round-2 MAJOR-2 的洗白类）。所以它**故意保持 first-class DEVIATION**，由 M4 每次判 LOAD-SENSITIVE、运维
  独跑复核。代价是 -j6 的偏离集因此不稳定（30/96 时红时不红）。
- **真正的修复方向（另一增量）**：OQ-8 的 two-wave split——grow-heavy drills 走低 -j（serial/-j2），其余
  走高 -j。那能消除 grow flake 并让偏离集稳定，但属独立的调度增量、需自己的评审与验证。

### #71 — drill 96 于 -j6 的 heal 后 brk1 canary3 commit-success 行（OPEN，边界时序未定根）

- **状态：🔴 OPEN。** 旧归档在 heal 后看到 brk1 自己的 `broker: session created … canary3`，但修正树
  两次 `-j6` 没有复现同一世界：到达 D 臂的 solo run 是 pre-heal=no、post-heal 也无 brk1 行，走的是已知的
  “queue group 路由到多数派”分支。因此新运行只能证明**本次**没有少数派提交，不能解释旧归档。
- **已增加的证据**：drill 96 在 partition 仍 armed 时记录 `_C3_COMMIT_PREHEAL`。若为 `yes`，brk1 的请求
  handler 在与多数派完全隔离时从 committed create 返回，仍直接触发 `#65` PRODUCT-RED；这是强证据。
- **为何 `no` 不能关闭旧现象**：pre-heal grep 与随后 `iptables -F` 不是原子边界。若该行在 snapshot 后、
  heal 前落盘，它仍可能是真少数派安全问题；若在 heal 后落盘，则可能是恢复 quorum 后的合法延迟完成。没有
  product 时间戳/边界 artifact 时，两者不能区分。源码说明“少数派按设计不能 commit”是预期不变量，不是对
  一次违反该不变量的实测归因。
- **处置**：不 band；pre-heal `yes` 保持 PRODUCT-RED，pre-heal `no` + post-heal `yes` 保持
  `NOT-COVERED[gap] #71 AMBIGUOUS`，直到专用复现提供可与 heal 边界排序的 product artifact。结构不可达的
  长连接 condition Y 仍是相关覆盖缺口。

## 已了结条目索引（全文见 `docs/reviews/deploy-tier-gotchas-closed.md`）

编号保持全局连续；下列条目已了结，正文已剥离归档。

| 编号 | 结论 | 条目 |
|---|---|---|
| #25 | CLOSED | PIN CONNECT 无 per-IP 限速（架构 §E.6 承诺未实现） |
| #26 | CLOSED | evict 不清理 agent 的 managed OS 子进程（部署条件式；公网口随隧道掉线被清） |
| #27 | CLOSED | C2 well-known discovery 在 cluster init 后不 serve-ready（manifest |
| #28 | FIXED | agent 侧升级 URL 白名单硬编码、不可 operator 配置（DOC-3 是并发文档缺陷）· ✅ FIXED (R |
| #35 | REFUTED（证伪，非缺陷） | [CANDIDATE，未在 sim 复现] online force-single 的 dwell 在 quorum-los |
| #36 | CLOSED | online force-single 的 --yes 不走 Tier-2 rejector（与 offline 分歧；TT |
| #39 | CLOSED | disk_pressure monitor interval 固定 5-min、无 operator knob（90-M6  |
| #46 | CLOSED | seeds change-gated auto-publish 漏第 3 voter（91 暴露；G3 client-con |
| #47 | CLOSED | cluster add 可把可达 joiner 永久留在 CATCHING_UP，后续 grow 被串行锁阻断 |
| #48 | CLOSED | 连续 shrink 时 agent 可黏在已退役 broker 的 NATS 孤岛，ONLINE 行与真实命令路径分裂 |
| #49 | CLOSED / ALREADY-FIXED | resnapshot 的 SQLite preflight 与 RecoverCluster 实际 FSM 不一致 |
| #50 | CLOSED | cluster doctor --offline --db <不存在的路径> 报 0 fatal 且 exit 0（「迁移源 |
| #51 | CLOSED | recovery restore 结构上不能 apply broker.yaml cluster seam ⇒ fresh  |
| #52 | WONTFIX-BY-DESIGN | recovery restore 既不渲染也不提示 nats.conf ⇒ fresh box 的 stock conf 无 |
| #53 | WONTFIX-BY-DESIGN | 已拆分（2026-07-19，R10）：#53-silence CLOSED / #53-scope WONTFIX-BY- |
| #54 | CLOSED | account.nk / CA 轮换无产品级 re-render 与 verify；reconcile nats --all |
| #55 | CLOSED | account.nk 轮换的 auth-rejection skew 窗口 ⇒ 与 #54 合并 |
| #56 | CLOSED | rotate-tunnel-cert 的 follower 侧误导指引（UX 误导，非流程死锁 — R6 收窄） |
| #58 | FIXED | cluster 模式非-leader home broker 重启后 orphan xfer object 永不回收 |
| #59 | REFUTED（证伪，非缺陷） | 被分区少数派 broker 无法「只读存活」 |
| #63 | REFUTED（证伪，非缺陷） | rotate-tunnel-cert 在线轮换后 re-pin（R6 REFUTED：车队确实 re-pin；机理已在 R1 |
| #64 | CLOSED | recovery restore 剪到单 voter 却不去集群化 nats.conf、也从不提示 ⇒ 照文档做必 cras |
| #65 | REFUTED（证伪，非缺陷） | [CANDIDATE] 分区少数派的 stale-leader 写有时变持久 |
| #68 | FIXED | R16 A4 的 --reset-js 确认门只更新了一半 remedy ⇒ JS-503 横幅让运维跑一条必被拒的命令 |
