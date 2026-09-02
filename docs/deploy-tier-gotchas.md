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

> **⬆ 2026-08-11 实测复核（drill 71 on v0.5.0 源码）→ 部分改善**（`product_red=0`）：**drain-migrate 门已通**
> ——`cluster drain brk3` rc=0 + migrated expose 被 agent ACK 新 home（`B-cmd [R8]` PASS；之前被 #31 grow-lock 挡的
> 那一重已随 #31 修复解除，Arm E rebuild-OFF 拒绝签名也 PASS）。**crash 后不自动 rehome 确认是设计行为**
> （C/D-strand PASS、epoch 不变——非 bug，要不要改是产品决策）。**唯一 open 面**：普通 `expose`（无 --on-broker）在
> rebalance/kill 动态窗口 create `rc=64`（drill 74 B-negctrl 实测复现）——homeForExpose 动态 eligibility 不稳定这一面未闭合。
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

> **⬆ 2026-08-11 实测复核（drill 30 on v0.5.0 源码）→ 已修**：grow brk2/brk3→N=3 后 rolling upgrade
> **顺利过 acquire-lock，全程无 `membership operation in flight`**（output 零 `[GAP #31]`），`product_red=0`。
> 旧版 3/3 在此 HALT——grow-lock best-effort release 现已可靠。verdict 仍是 INCOMPLETE，但只因 2 个**与
> #31 无关**的覆盖缺口（N=2 负控制作者自证冗余 + leader-hop sim 结构不可达）。**产品行为已对，台账挂 OPEN 仅是 drill 翻不了绿。**
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

> **⬆ 2026-08-11 实测复核（drill 74 on v0.5.0 源码）→ 核心多数 PASS，产品债门仍在**（`product_red=0`）：
> `SKEW-reconstruct` 把分布重建成 1/1/1（spread==0）、**auto-rebalance-on-return 在 return edge 发火**
> （`C-dp` 数据面 flow bytes + `C-event` `proxy_auto_rebalanced` count==1，均 PASS）——比台账原始描述改善明显。
> 但 **`#34 REGISTERED-OPEN` 产品债门仍挂着**（drill 自述"非确定性复现、R15 did not fix #34、未 verified-fixed"）；
> 本 run 的 `assert_fail=2` 实为 #29 家族的 expose-create `rc=64`（见 #29），不是 #34 本体。
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

> **#45 已 FIXED 并归档**（2026-08-11 实测复核 drill 40 GREEN、`product_red=0 not_covered=0`；g75-g78
> 外审 F6 顺手清偿的既有 bookkeeping 缺口——此前 prose 标"已修"但未迁 closed）——全文见
> `docs/reviews/deploy-tier-gotchas-closed.md`，下方索引表亦记。

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

### #72 — agent 的 stuck-disconnect watchdog 在取消 session 前同步 `nc.Close()`；WSS close 卡住时 systemd 假活、节点 OFFLINE、数据面暂存

- **状态：🟡 OPEN — 产品修复已落地（2026-08-01，见文末「修复」段），但 flip 条件未满足**：
  真 WSS 黑洞 deploy-tier drill 尚不可构造（simcluster 无 wss 前端），故不移入 closed。
  原始记录（2026-08-01 裸机 LIVE-CONFIRMED；症状与阻塞区间已实证，缺触发时 goroutine dump，
  故最内层阻塞点标为高置信归因而非伪装成已逐帧证明）如下，**机理段的主嫌疑已被修复轮的逐帧核对订正**
  （见文末）。 发现环境不是 simcluster：真实 `systemd --user` agent，已发布
  `tether 0.4.7 (proto v2)`；本条仍登记在 deploy-tier 台账，因为只有长驻进程 + 真 WSS/NAT + 真双平面隧道的部署栈
  才呈现该故障。尚无 drill / `assert_bug`，不能据此声称已被自动化钉住。

- **用户可见现象**：RackNerd 一侧的 ctl 长时间把本机 agent `weilandserver` 看成 OFFLINE / 不列出，但本机
  `tether-agent@lab.service` 始终是 `active (running)`；先前建立的 expose 在一段时间内仍能工作。这不是观测矛盾：
  NATS WSS `:443` 是注册/心跳/命令的**控制面**，yamux-over-TCP `:7000` 是 expose 的**数据面**，两条连接独立。
  控制面已经失活时，旧数据面可以继续传字节，直到自己的 keepalive、broker fence 或 allocation revoke 使它关闭。

- **2026-08-01 CDT 现场时间线（取自 `~/.tether/agent/lab/agent.log` + 同机 ctl/systemd 探针）**：
  1. `09:00:40`：agent 最后一次记录 `re-registered after reconnect`，旧 expose 为 `__proxy__:14006` 与
     `ssh:14022`；随后两条隧道分别 reset / keepalive timeout。
  2. `09:08:59.453`：明确记录 `agent: NATS stuck-disconnected; rebuilding session on the freshest roster`。
     这行位于 `fireRedial` 设置 `rebuildRequested=true` 后、同步 `nc.Close()` **之前**。
  3. 约 `09:19` 的首次 `tether node ls --json` 经 `linziyang.top`（roster node `racknerd`）成功返回；`racknerd`
     与其余节点均 ONLINE，唯独 `weilandserver` 整行不存在。故不是 ctl 全局连不上、不是 RackNerd broker 整体停机，
     而是该 agent 没有注册/心跳。同期 `tether ps --json` 等待 15s 后超时。
  4. 全窗口 systemd 的 `MainPID=618208`、`NRestarts=0`、`active/running`，进程从 2026-07-29 起未退出；宿主还看到
     tether 保有一条 ESTABLISHED `:443` socket。故「PID 存活 / TCP 表面 ESTABLISHED」均不是 agent 就绪判据。
  5. `09:17:53`：旧 14022/14006 数据面才收到 EOF；14022 下一次 REGISTER 被权威拒绝
     `token_unknown_or_revoked`。这解释了为何控制面 OFFLINE 窗口内 expose 看起来仍正常，也证明数据面最终不会永久
     越过 broker revoke fence。
  6. `09:19:57.953`：直到 watchdog 日志后 **10m58.500s**，才出现 heartbeatLoop 的
     `agent: shutting down`，同一时刻 Run loop 记录 `rebuilding NATS session on the freshest roster`。
  7. `09:20:03.254`：**同一 PID、无 systemd restart** 重新注册成功，broker 回复 `revoke_ports=2`；
     `09:20:06.593` 新 `__proxy__` tunnel 在 14004 打开。其后间隔 6s 的两次 `node ls` 均显示
     `weilandserver ONLINE`，5s 心跳持续推进；`racknerd` 也一直 ONLINE。

- **排除项 / 伴随噪声**：
  - ctl floor 是 `wss://linziyang.top:443`，agent YAML floor 是 `wss://weiland.top:443`，但 agent 的有效签名 roster
    同时含 `pc732/weiland.top` 与 `racknerd/linziyang.top` 两个 `VOTER`，缓存当时新鲜且在恢复后继续更新；所以「没有
    可切换 endpoint」不是本次近因。不同 floor 在 HA 中本身合法，不应把它当配置错误修掉。
  - 日志中长期出现 `dial tcp 127.0.0.1:40243: connect: connection refused`，说明旧 `ssh` expose 的本地服务未监听；
    它只在数据面收到入站 stream 时发生，与 NATS 心跳连接独立，是需要另行清理的 stale expose / 本地服务问题，
    不是本次控制面 teardown 停顿的根因。
  - 没有在故障窗口抓 goroutine dump，因此本文不声称已经证明 nats.go 的哪一个具体 syscall 阻塞了 10m58s；实证能
    钉死的是 `fireRedial` 已进入而 session cancel 长时间没有生效。

- **高置信机理定位（源码顺序 + 日志相邻点）**：
  1. `internal/agent/roster.go:603-617` 的 `fireRedial` 先置 `rebuilding/rebuildRequested` 并写上述 watchdog 日志，
     然后在 `:609-611` **同步**调用 `nc.Close()`；只有它返回后，`:612-617` 才取得 `sessCancel` 并 `cancel()`。
  2. `internal/agent/agent.go:1570-1578` 的 heartbeatLoop 只在 session ctx 被 cancel 后写
     `agent: shutting down`。两条日志夹住的 10m58.500s 因而发生在 `nc.Close()` + 后续极短的
     `sessCancelMu` 临界区之间；`setSessionCancel/clearSessionCancel` 仅做一次指针赋值，长期持锁的结构证据不存在，
     所以阻塞高度集中到 `nc.Close()`。
  3. 本仓锁定的 `nats.go v1.52.0` 中，`Conn.Close()` 对 WebSocket 先调用 `wsClose()`；后者持 `nc.mu` 组 close frame
     并同步 `nc.bw.flush()`，之后才进入通用 `close()` 关闭 transport。对已经 stuck-disconnected 的 WSS，close-frame
     flush 位于「真正强制关 socket」之前，具备被坏链路/代理写路径拖住的结构条件。这与现场使用 WSS、日志区间以及
     最终自行解卡吻合；但在补充 goroutine dump 或确定性复现前，具体卡在 flush / conn mutex / 底层 write 中哪一层仍
     保留为待证。
  4. 现有 watchdog 的意图是 disconnected 超过 `redialAfter=20s` 后取消 heartbeatLoop，让 `session()` 返回
     `rebuild=true` 并用最新 roster 重拨；但取消动作排在一个**无 agent 侧 deadline 的同步 close 后面**，所以 20s
     watchdog 并没有形成恢复时延上界。最坏情况下主进程永久存活、systemd 永远不拉起新实例。

- **影响与安全边界**：
  - ctl 的 node/exec/ps 视图与 agent 真实进程分裂：node 行 OFFLINE/消失，agent PID 与 TCP socket 看起来仍健康；仅用
    `systemctl is-active` 的监控会假绿。
  - agent 不接收新命令、注册回复、reconcile 或新的 home directive；控制面恢复时间从设计的约 20s 退化为此次近 11min，
    结构上无硬上界。
  - 已建立 expose 可在窗口前半继续服务，随后按独立数据面机制失效；因此「expose 还能用」既不能证明 agent ONLINE，
    也不表示安全 fence 失效。本次旧 token 最终被 broker 拒绝，未观察到 revoke 后复活或双绑。
  - 恢复时 broker 撤销两个旧 port、重建 proxy 到新端口，普通 expose 的公网端口/可用性可能变化；不能把自行恢复视为
    无扰动恢复。

- **立即运维处置**：先用 `tether node ls --json` 看 `last_heartbeat_at` 是否推进，而非只看 systemd；若日志已经出现
  `NATS stuck-disconnected`，经过一个明确的小预算仍无 `agent: rebuilding NATS session` / `agent: registered`，执行
  `systemctl --user restart tether-agent@lab.service`。这会中断现存数据面，且可能触发 allocation revoke/replay；先记录
  日志与 `node ls`，有条件时在 restart 前抓 `SIGQUIT` goroutine dump，才能把上面的最内层待证点闭合。不要删除
  `state.json` / keys，也不要用重新 expose 掩盖控制面故障。

- **产品修复方向（尚未定案，不把候选写成契约）**：
  1. teardown 顺序必须让 session cancel **先于任何可能阻塞的 WSS close/flush**，使 heartbeat、roster refresh 与 handler
     生产者先停；但仅把 `nc.Close()` 丢进无界 goroutine 会把一次假死变成 goroutine/FD 泄漏，不是合格修复。
  2. 为 NATS session teardown 定义产品级有界预算；预算耗尽时宁可让 agent 非零退出交给 systemd `Restart=on-failure`，
     也不能保持 `active/running` 的不可用进程。setsid/nohup 路径没有 systemd，需同时定义其退出与外部拉起责任，不能只
     修 systemd 场景。
  3. 核对/上游化 `nats.go` WSS `Close → wsClose → bw.flush` 的无 deadline 行为，评估能否在不碰私有 transport 的前提下
     强制打断；若必须引入 seam，须同时守住 callback drain、subscription teardown、no-double-subscribe 与 FD/goroutine
     回收。
  4. 增加 agent readiness/health 信号：至少区分「process active」「NATS connected」「registered + heartbeat recent」，
     避免 systemd/监控继续把半死态显示为健康。此项是可观测性补强，不能替代恢复路径本身的有界修复。

- **钉住它的测试 / drill（当前缺口与 flip）**：
  - **确定性包内回归**：给连接 teardown 引入最窄可注入 seam，使 `Close` 永久阻塞；触发 redial watchdog 后断言 session ctx
    在恢复预算内先被 cancel，且主循环不会在 `Close` 前永久停住。变异验证：恢复成「Close 后 cancel」，测试必须精确红。
  - **并发/泄漏门**：同一故障注入下重复 disconnect/rebuild，断言至多一个 successor session、无 double subscription，
    `-race` + fd/NumGoroutine 门不增长；禁止以 timeout 后遗弃一个永不返回的 close goroutine 来骗绿。
  - **deploy-tier WSS drill**：在 agent 已注册且 expose 正在传字节时，黑洞当前 NATS WSS 的写路径（不是 clean FIN/RST），
    保持另一 voter 可达；断言 agent 在书面预算内出现在另一 voter 的 `node ls` 且心跳推进，同时记录旧数据面的独立存活/
    fence 时序。要检查 `MainPID`：同进程 rebuild 与超时后由 systemd restart 两条允许恢复路径都必须明确归类。
  - **可观测性回归**：故障窗口内 health/readiness 必须 RED，即使 PID 存活、socket ESTABLISHED、旧 expose 仍传字节。
  - **flip**：上述 package 回归 + 真 WSS deploy-tier drill 都 GREEN，且多轮运行中 OFFLINE 窗口有硬上界、无资源泄漏、
    revoke/home fencing 不回退，方可将 #72 移入 closed 台账；单次人工 `systemctl restart` 或本次自行恢复不算关闭。

- **修复（2026-08-01 落地，plan：`docs/reviews/gotcha72-teardown-plan.md`）**：
  - **机理订正**：修复轮逐帧核对 nats.go v1.52.0 后，主嫌疑**从 close-frame flush 改为
    `doReconnect` 持 `nc.mu` 跨 `createConn`/`processConnectInit`** —— 其中 `net.LookupHost` 无 ctx、
    `makeTLSConn` 的 `Handshake()` 在 `processConnectInit` 设 deadline **之前**、ws `req.Write` 无写
    deadline。close-frame flush 反而受 `FlusherTimeout` 约束、且 RECONNECTING 时直接跳过。
    仍**不升格**为逐帧证明（无现场 dump），但修复覆盖全部三层 + 兜底，不依赖归因收窄。
  - **产品修复**：teardown 改为状态机——**先 cancel**（纯指针/chan，不碰 nats.go 锁）→ close 放
    closer goroutine 并限时 **10s** → 超时**粘性毒化**底层 socket（`SetDeadline(过去)+Close`，且此后
    新拨号一律拒绝）再等 **10s** → 仍卡则升级：**rebuild 场景原地 self-exec**（PID 不变，setsid 也活）、
    **关停场景 exit 91**。S3/S4 正常返回前 closer 必须 join；S5 以 process replacement/exit
    终结当前进程，不会把活 closer 带回 `Run`（timeout 后遗弃 goroutine 仍是台账点名的假修复）。
    恢复硬上界 ≈ redialAfter 20s + 10s + 10s + 重连 ≈ **≤60s**（替换原文的"结构上无硬上界"）。
    同型缺陷 `rebuildOntoVoter` 一并修复——**外审 F3 订正**：它原先在 cancel 之前调用
    `nc.ConnectedUrl()`（该 observer 取 `nc.mu.RLock`，正是卡死的那把锁），修复后改读健康期抓取的
    无锁快照，teardown 路径上不再有任何 `*nats.Conn` observer。
  - **落点**：`internal/agent/conn_teardown.go`（新）、`roster.go` 两处、`agent.go`（tracker dialer /
    session finalizer / `rebuilding` 复位顺序）、`usage.md §9.9`。
  - **钉住**：`conn_teardown_test.go`（顺序/有界/单飞/毒化粘性/escalate 双意图）+
    `conn_teardown_leak_test.go`（20 轮楔死 teardown 后 goroutine+fd 回基线，用仓库内建泄漏门）+
    `test/simcluster/drills/98-stuck-redial-recovery.sh`（nats:// 面的恢复回归，ledger owner）。
  - **残余**：WSS 面在 simcluster 不可构造（98 的 `[GAP #72]`）；DNS 层（`LookupHost` 持锁、无 fd 可毒）
    是 escalate 存在的理由，如实写在代码与本条。
  - **外审轮补修（2026-08-02，F1–F4）**：首版仍有三条可绕过状态机的可达路径——`subEvict.Unsubscribe`
    留在有界 finalizer 之前（defer LIFO，`fin.Do` 根本启动不了）、register 循环用父 ctx 而非 runCtx
    （连接已 finalize 仍无限重试，session 永不返回）、`rebuildOntoVoter` 的 observer 调用（见上）；
    另有 escalate 误用普通升级的"旧进程存活"契约（teardown 时本进程可能是 NEW 映像）。四条均已修并有
    外审提供的反例钉住。**本条在这些修复通过复审前，不应被读作"#72 已闭合"**。

### #73 — 能骗过冒烟门 version 串的非 tether 产物 ⇒ 无 boot shim ⇒ 升级永 pending、NAT 后节点永久失联

- **状态：🔴 OPEN（upgrade-safety success-drill 规划轮推理定格；33 drill 以 `not_covered[gap]` 指认，
  尚无自动化复现——登记先于 drill 实测，威胁形态本身不依赖实测）。**
- **形态**：`node upgrade` 的冒烟门只验证候选产物 exec 后按冻结格式回答 `tether <release> (proto vN)`
  且纪元相等（architecture §21.3）。一个**不是 tether 的产物**（如打印该行的脚本/别的程序——真实事故
  形态是 CI 打包错产物，sha 由 operator 自己背书所以 sha 门不设防）能通过冒烟并被 flip + exec。
  此后三层收敛器**全部旁路**：产物没有 Go boot shim ⇒ boot 预算永不消耗；watchdog 活在被 exec 掉的
  旧进程里 ⇒ 随之消亡；marker 永 pending ⇒ 后续 `node upgrade` 重试被入口门拒到 deadline、之后的
  重试又对**同一坏产物域**反复 staging。supervisor 拉起的永远是坏产物，节点在 NAT 后永久失联，
  `.prev` 里的好二进制在盘上无人问津。
- **与 §21.3 已接受窗口的边界**：架构接受的"崩得早于启动检查"窗口，前提是**产物是 tether 二进制**
  （静态 Go、shim 在 main() 最前）。非 tether 产物令该前提整体失效——这不在已接受窗口内，是真 gap。
- **运维处置**：只用 goreleaser 产物 + `SHA256SUMS` 的哈希（照 broker-ops §8.6/8.7 的发布流程走，
  别给手工拼的 tarball 背书）；中招后需带外访问：恢复 `<二进制>.prev` 覆盖 dst 并重启 unit。
- **产品修复方向（未定案）**：冒烟门加"产物自证 shim"——例如候选以专用参数运行时必须回显 marker
  相关的自证信息（仅新产物会实现；对老产物退化为现状，不破坏 N-1 窗口）；或 `version` 输出加入可验证
  的构建自证。任何方案都要过"不把冒烟门变成假安全感"的外审。
- **钉住它的**：`test/simcluster/drills/33-node-upgrade-success.sh` 的 `not_covered[gap]` 指认本条；
  hermetic 面 `TestSmokeVersionTable` 已钉冒烟门对格式/纪元的拒绝（但对"格式全对的非 tether 产物"
  结构上不可判——这正是 gap 所在）。owner 待定：34 号探针 drill 或产品防御，二选一后 flip。

### #74 — h1 引入的新 FSM op 与新告警种类要求 broker 锁步升级；日志重放式回滚被封死

- **状态：🟡 OPEN-BY-DESIGN（h1 明确取舍，非缺陷；记在这里是因为它约束运维动作）。**
- **形态**：h1 给 raft 词汇表加了两个 op（`ProcGC` / `PortGC`，存储保留期 GC）和一个
  `alerts.kind` 枚举成员（`proxy_bind_stalled`，migration 0018 重建表）。三者都是**新 broker 才认识**
  的东西：
  - 一个**旧 broker**收到 `ProcGC`/`PortGC` 的 committed entry 会走 unknown-op 路径；
  - `proxy_bind_stalled` 的行在 0009 的 CHECK 下非法，**从零重放日志**会 fail-stop。
- **约束（两条，都是硬的）**：
  1. **多 broker 集群必须锁步升级**——所有 broker 装上 h1 之后，新词汇才可能开始流动。
     现网是 force-single N=1，天然满足；将来长到 N≥2 之前必须先把这条写进 grow 流程。
  2. **回滚到 h1 之前只支持 snapshot-restore**，一旦 `proxy_bind_stalled` 被 raise 过就
     **不能**用日志重放（`fsm.Restore` 的前向迁移让 snapshot 路径两个方向都安全）。
- **为什么不做能力门**：h1 plan X-1 的裁决——production 是 force-single N=1，任何第二个 broker
  都会在**它自己**那一跳装上新二进制，能力门防的是一个当下不存在的拓扑，代价是一个 wire 字段
  + health 面板管道 + 一套测试。反转条件已记在 plan 的 Q6：一旦滚动多 broker 升级成为支持路径，
  B 和 E 两个 workstream 都要补门。
- **运维处置**：见 `broker-ops.md §8.8`（升级顺序 broker→agent、迁移步骤、冒烟检查）。

## 真实车队 v0.5.0 全删重装发现（#75+；来源=生产车队运维，非 simcluster drill）

> 2026-08-11 全车队 v0.5.0 从零重装（racknerd 洗成纯 single broker + 8 agent 重 join）过程中，
> 在**真实生产栈**上暴露的一批**系统性短板**——共性是「本该是默认/傻瓜式的东西却要运维手动补，
> 漏一步就出事」。区别于 S 系列（那是 simcluster drill 自动钉红）：这几条**尚无 drill oracle**，
> 修复批落地时应顺手补 simcluster 覆盖（#75/#76/#77 都可在 deploy-tier 复现）。

> **#75/#76/#77 已 FIXED 并归档**（2026-08-11，g75-g78）——全文见
> `docs/reviews/deploy-tier-gotchas-closed.md`「g75-g78 部署层默认值」段，下方索引表亦记。
> hermetic 全绿 + live drill(32 GREEN / 93 #75 五臂 PASS, product_red=0) 双证闭合；外审 F1/F2/F3 已整合。

### #78 — agent 拨隧道失败无退避、broker WARN 无降频、且无 per-agent proxy opt-out → 拨不通的节点每 5s 刷屏

> **⬆ 2026-08-11 修复（g75-g78）**：三条都做。**① agent 首拨指数退避**——`proxyStartLocked` 单咽喉
> （broker 每 5s keyset repair 推送不变，agent 在退避窗内不拨）：5s→…→封顶 5min，key =
> `{gen,epoch,port,token,homeEpoch}`，任一分量变化（proxy off/on、rotation、rehome）或 NATS 重连立即重试、清零
> ——运维显式操作零延迟。复用 `internal/backoff.Tracker`，零新 timer/goroutine。**② broker WARN 降频**——
> `tunnel handleAgent` 的 `read REGISTER` 站点接 **per-class 三个固定 `backoff.Tracker`**（class ∈
> {eof,timeout,other}，带 `class`+`remote` 主机 + `suppressed_since_last`;内审 M1:单共享 Tracker 会被
> 多源类别交替击穿）：同类风暴只打首条 + **Due 到点每 Cap(5min) 重申一条**(内审 Mi5) +
> **已鉴权** REGISTER(内审 M2:过 tokenLookup 之后,垃圾行伪造不了 recovery)补 `recovered suppressed=N`。
> **③ per-agent opt-out**——`agent.yaml proxy.participate: false`（`*bool` nil=参与，防零值灾难）→ register
> 带 additive `proxy_opt_out`，broker 折入既有 `nodes.proxy_capable`（**不加 RegisterInput 字段**——那是 raft
> payload；折入既有列即复制到位），停 mint/停推/停渲染 + 释放存量 __proxy__ 行；agent 侧 belt 对旧 broker
> 也拒拨；`proxy status` 标 `opted-out`。wire +2 additive（N-1 四象限过）。hermetic 钉（各带变异验证）：
> 退避表测 + opt-out 三态 + broker 闭环 + WARN 降频 + wire 零值字节等价。deploy-tier：新 `drills/78-proxy-dial-backoff.sh`
> 四臂（退避 cadence 用 netfilter 计数器、operator bypass、WARN 降频、opt-out 释放+恢复）。
> **⚠ 回滚砖**：agent.yaml 严格解析，写 `participate` 后回滚到旧二进制会拒启——仅在不再回滚到 <本版本时才写。
- **状态：🟢 FIXED（g75-g78，2026-08-11；hermetic 已绿，drill 78 待实跑复核）。环境侧残余**：wsl 宿主
  Windows 放行出站 :7000 不在产品面（opt-out + 退避已把"刷屏爆盘"这一半消掉）；**N-1 [GAP]**：旧 broker 仍会
  给 opted-out 节点推 directive（agent 本地拒拨止血，但 broker 渲染该节点 never-ready）。
- **现象（历史）**：weiland-wsl（WSL2 出站只放行 :443、隧道 :7000 半通——TCP 建立即 EOF）参与 session proxy，
  **每 5 秒**拨隧道一次，broker 每次记 `tunnel server: read REGISTER err=EOF` WARN，**永不停**。
- **机理**：① agent proxy 隧道重试是**固定 ~5s、无指数退避**；② broker 对同源同因 WARN **无 dedup/rate-limit**；
  ③ proxy 是 **session 级**开关，拨不通的单个 agent **无法退出出口**（agent.yaml 无 proxy opt-out）——
  只要 `proxy on` + 该 agent 在线，就无限刷。
- **怎么修（该改进）**：失败重试**指数退避**（封顶如 5min）；broker 重复 WARN **降频/合并计数**；
  agent.yaml 增 `proxy.participate: false`（或 broker 侧按连续拨失败自动摘除该节点出口）。
  彻底根治本例还需在 wsl 宿主 Windows 放行出站 :7000——但那是环境侧，产品这三条能把「刷屏爆盘」这一半消掉。

### #79 — [更正：非 v0.5.0 短板] force-single 的 roster prune 失败无重试 → 永久 ghost VOTER —— **0.4.7 死结，v0.5.0 batch C 已修 ✅**

- **状态：✅ FIXED（v0.5.0 / batch C·C1，commit `92e01a4` `internal/broker/force_single_finalize.go`）。
  本条最初误登为 v0.5.0 open 短板——经代码核实是 0.4.7 的死结、v0.5.0 已修，更正如下。**
- **0.4.7 的病**：online force-single 同步 prune abandoned roster row 失败后，dwell gate
  （`CodeQuorumNotLost`——该节点此刻自己就是 leader、有 leader contact）**拒绝 re-run** → 失败的 prune
  永无重试 → ghost roster row（phase=VOTER 但**不在 raft config**、`cluster status` role 为空）永久残留 →
  `reconcile nats --to-standalone` 因 "cannot prove N=1" 拒 → **降不回纯 single 的死结**。
  `force_single_finalize.go:44–49` 自述：这正是现网 **pc732 permanent ghost VOTER 的直接成因**。
- **v0.5.0 的修（batch C）**：加 `OpForceSingleFinalize` **自驱动 retry op**——replicated deadline
  budget=`12 * observeTickInterval`（carried in `catchup_deadline` 列，**非 leader-local 计数器**，重启/换届/
  no-op propose 都不丢），在同步 prune 失败时自动创建并重试 prune，直到 abandoned ghost roster row 被清、
  to-standalone 的 N=1 tally 通过、降级解锁。deterministic finalizer 仍是 `cluster recovery node remove <ghost>`。
- **本轮运维教训（非产品缺陷，记此备忘）**：这次清 pc732 时 racknerd broker **仍是 0.4.7**，所以只能走
  推倒重建（删 `raft/`+`tether.db`）。**v0.5.0 下的正解是：先把 broker 升到 0.5.0 → 跑 `cluster recovery
  force-single`（finalize retry 自动清 ghost）→ `reconcile nats --to-standalone` 降回纯 single，无需推倒重建。**
  推倒重建同样达成了目的（且符合本次"全删重装"意图），但不是 v0.5.0 下的最优降级路径。
- **对「双向切换」结论的修正**：分析里曾说"降下来最硬的缺口是 N=1 带死 voter 无在线清理"——**在 v0.5.0
  这已不成立**，force-single finalize retry 补上了这条在线路径。

### #80 — agent 的 SS proxy server 锚在 per-session `runCtx` 上：一次 NATS session 重建即杀死数据面，且**永不重建**（真实生产事故，已修复）

> **来源 = 生产车队，非 simcluster drill。** 2026-08-21 weilandserver 现场诊断。
> plan 见 `docs/reviews/proxy-lifecycle-plan.md`，内审见 `-review.md`，外审见 `-external-review.md`。
> 本条同时是 **#33 的候选归因**（见下）。
>
> **结论口径（外审 F7 订正）**：首版本条把结论写成"已修，次生缺陷一并修复"，当时**外审的四个 Major 尚未闭合**，
> 那个措辞越界了。现在的准确表述是：**原始 session-context 根因已修**（去 ctx 锚 + corpse 收割 + 停止者闭集），
> **外审 F1–F4 已在同一增量内修复并各有反例测试转绿**：
> F1 = `Stop` 无法取消 in-flight DNS（server 自有 `stopCtx`，见下）；F2 = hard revoke 漏关 upstream 半边；
> F3 = 持久化 `LocalPort` 冲突使自愈永久变暗；F4 = single-broker 非重连 corpse 无自愈边。
> **仍未闭合**：`Stop` 在 `p.mu` 下的**总**上界（取消已到位，但 teardown 预算未做成显式契约）；
> 以及 #33 的归因仍是 **CANDIDATE**——转 FIXED 需重跑 drill 73 并见 STRANDED 臂消失。

- **现象（生产实测）**：`tether proxy status` 报 `weilandserver ONLINE READY=true`、对外广告出口 `linziyang.top:14004`，
  而该主机 **`ss -tlnp` 下 agent 进程零 listener**，持续 **7 小时 40 分**。每个被 `/sub` 导到该出口的订阅者必然连接失败。
  `agent.log` 里 `agent: proxy SetKeys err="ssproxy: server stopped"` **5416 条**（每 5s 一条，4.3 MB），从未自愈。
  唯一恢复手段是 session 级 `tether proxy off && tether proxy on --yes`——它**重排了全部 8 个节点的公网端口**并强制
  订阅者重拉配置。对"一个节点的 SS 死了"而言这个爆炸半径是荒谬的。
- **根因链（逐条对照源码验证）**：
  1. `proxyStartLocked` 把调用方 ctx 交给 SS server（`internal/agent/proxy.go` 的 `srv.Start`），该 ctx 一路来自
     `applyProxyDirective` 的参数 = `session()` 的 **`runCtx`**，而 `internal/agent/agent.go` 的注释本身就写着
     *"the C1 session loop **REWRITES runCtx every session**"*。
  2. NATS stuck-disconnect → `fireRedial`（`internal/agent/roster.go`，`redialAfter = 20s`）→ session 重建 → `cancelRun()`。
  3. ssproxy 的 ctx-watch goroutine 收到 `ctx.Done()` → `shutdown()` → `closed = true`，listener 关闭。
  4. agent 侧 `p.srv` **仍指向这具尸体**（`proxyRuntime` 跨 session 存活，无人清理）。
  5. 新 session 的 register 回包是 **keyset-only（无 Token）**——这是**正确的 broker 行为**
     （`internal/broker/proxy.go`：*"Token empty → agent reuses its persisted token"*）。broker 没有做错任何事。
  6. `applyProxyDirective` 因 `p.srv != nil` 走 `default:` 分支 → `SetKeys` 撞 `closed` → **只 WARN 然后 return**。
     且 exact-equal re-ACK 分支（`d.Enabled && p.srv != nil && 同 pair`）会**主动 `pubProxyReady(nc, true)`**
     ——**"假 READY" 是 agent 亲口宣告的**，不是 broker 猜的。
- **两条独立的次生缺陷（同一诊断中发现，一并修复）**：
  (a) `ssproxy.Server.Start` 的 `if s.ln != nil` 早退排在 `if s.closed` **之前**，而 `shutdown()` 关闭 listener 却**不置 nil**
  → 对已停止 server 调 `Start` 返回 `(oldPort, nil)`，**假成功 + 死 listener**；
  (b) `Stop()` **无界**：`allConns` 只含 accepted 连接，upstream 不在其中，而 `relay` 的 `wg.Wait()` 需双向 copy 都结束
  → upstream 不响应 half-close 即无限期挂住，**且 `Stop()` 是持 `p.mu` 调用的**（与 gotcha #72 同族）。
- **策略层面的意义**：`armFailClosed` 的 `ProxyFailClosedGrace`（默认 15 min）本是 SS server 的**既定** teardown 策略
  （分区超过宽限才停，防被吊销订阅者继续 egress）。runCtx 耦合让一次普通 session 重建在 ~20s 内就杀掉 SS——
  **这条 15 分钟宽限在该路径上等同死代码**。控制面事件在拆毁策略明确规定它不该拆的数据面。
- **修法**：SS server **不接受任何 ctx**（移除 `Start` 的参数），使停止者集合成为闭合可 grep 的四项清单
  （权威 OFF / fail-closed / teardown-then-rebuild / agent 退出）；corpse 在 directive 入口被收割并从持久 footprint 重建；
  `SetKeys` 的 `ErrStopped` 从"可忽略瞬时错误"改判为终态信号。闸门见 `test/architecture/dataplane_lifetime_test.go`。
- **flip**：本条已修；回归由 `internal/agent/proxy_lifetime_test.go` 的停止者闭集表钉住。

> **#33 的候选归因（不宣称闭合）**：#33 记录的是"proxy exit 的 home broker 被杀后，控制面报 rehomed+ready 但 SS 数据面
> 是黑洞，**per-run 或自动恢复或需手动 `proxy off; proxy on`**，根因未归因"。本条的机制与之**逐项吻合**：home broker 被杀
> → 其同机 nats-server 一并死 → agent 的 NATS 连接断 → session 重建 → runCtx cancel → SS 成 corpse → `proxy off/on`
> 是唯一 mint token-bearing directive 的路径（故恰是那个 workaround）；而"有的 run 自动恢复、有的不恢复"也解释得通——
> 取决于该 run 里 agent 是否真的经历了 session 重建（连在别的 broker 上的 agent 不会）。
> **标 CANDIDATE 而非 FIXED**：#33 的**首个**根因假说已被撤回过一次（round-5 订正），且 #33 的观测里含 SS 腿超时等
> 无法单独归因到具体层的现象。要转 FIXED 需在修复后重跑 `drills/73-proxy-cluster-ha.sh` 并见到 STRANDED 臂消失。

### #81 — spawnsafe 的 mount health `stHealthy` 无 TTL、永不失效 ⇒「先健康、后挂掉」的 NFS 永远不被剔出 `$PATH`，整套 remote-fs 保护静默退化成"只剩有界超时"

> **来源 = 生产车队，非 simcluster drill。** 2026-08-29 timan107 现场诊断（agent 0.5.0，UIUC CS 机房）。
> **✅ FIXED（2026-09-01 第二次外审：根因闭合 + deploy-tier 3 次独立实例稳定）**；plan
> `docs/reviews/remote-fs-stale-health-plan.md`，内审 `-review.md`，外审 `-external-review.md`。
>
> 历史上曾因 drill 62 Arm 1S 的间歇 `rc=124` 降回 verification-blocked；以下保留归因过程。
>
> **2026-08-31 取证后的订正。** 把每条命令拆成 **oracle A（控制面是否交出终态）** 与
> **oracle B（产品码对不对）** 之后，11 次独立运行的结论是：
> **两条连续的挂死挂载 `exec` 中，间歇性地有一条在 45s 内拿不到终态，位置在 1S-2 与 1S-3 之间游移。**
> 每一次红都落在 **oracle A**（控制面侧），而证据显示
> 「游标之后 agent 记录的 exec 行数 = 1」——**agent 收到了请求并记了日志，ctl 什么都没收到**。
> 此前所有把它描述成「healthy→dead 恢复路径复现红灯」的归因**都指错了地方**：一条命令只有一个断言时，
> 「控制面没交出终态」与「产品码不对」在观测上不可分。（中途我一度断言「红的是 1S-2」——那也是从单次
> 运行过度概括，装上两臂仪表后位置又变回 1S-3；一并记在这里，因为这正是**样本量不足就下结论**的复发形态。）
>
> **现在精确的未决问题（独立于 #81 的修复本身）**：*对挂死挂载发起的 ctl `exec`，agent 已记录收到请求，
> 但 ctl 有时 45s 内拿不到任何终态，尽管 agent 自己的 execve 看门狗只有 30s。*
> 归属未定（agent 回包路径 / broker / ctl / harness），**未猜、未改产品码**。
>
> **2026-09-01 第二次外审订正。** 三层仪表的首次独立运行再次在 1S-3 得到 rc=124；agent 游标后有
> 一条 request-start 记录。返修脚本虽声称「C 只在 B 成立时断言」，实际 A/B/C 三个 `assert_ok` 无条件
> 顺序执行，因此同一次 transport 红又制造了一条 C 产品红；B 的宽泛 `agent: exec` 还会匹配前一请求的
> 延迟 watchdog warning。现已改为：A/B 分别落盘成功标记，C 只有在 A+B 均成立时才执行；B 只匹配
> `msg="agent: exec" ... pid=` 的 request-start 且必须恰好一条。ledger signature 也已与实际断言标题校准。
>
> **最终产品根因与处置。** 在第二条请求挂起窗口向 agent 发 SIGQUIT，goroutine dump 显示：第一条被 watchdog
> 「放弃」的直接 `cmd.Start` 仍停在 Go runtime 无法完成 stop-the-world 的位置；第二条在编码 error reply 时
> 触发 GC，GC 等该线程，遂把 heartbeat、timer 与 NATS 回包一起冻住，直到 FUSE 恢复。`go cmd.Start()` 加
> timeout 只界定调用者等待，**不等于隔离 Go runtime**。现由 `internal/spawnexec` re-exec 本地当前二进制；
> agent/PTY owner 等待可取消 pipe 握手，exec 与 run 共用此边界，helper 私有环境标记不会传给目标。
>
> **2026-09-01 外审事故与二次订正。** 第一版 helper 又在 helper 内调用 `exec.Cmd.Start`，即每次风险启动保留
> 一个完整 Go helper，再 fork 一个可能卡在 execve 的子进程；agent 重启会重置进程内 wedge 计数，却不会清掉
> D-state 孤儿。外审裸跑测试/模拟时最终遗留 **5,789 个 `exe`、合计约 307 GiB RSS**，耗尽 251 GiB 主机内存与
> 8 GiB swap，触发 global OOM，连锁杀死共享 tmux、推理 replica 与 `agent.test`。这不是单进程 heap leak，而是
> **跨 agent 生命周期的进程/内存泄漏**。修正后 helper 自身直接 `syscall.Exec`（目标 PID 必须等于 helper PID，
> 回归测试钉住），不再二次 fork；status fd 与抽象 AF_UNIX 槽均为 CLOEXEC，成功 exec 即释放，卡在 cwd/execve
> 则继续占槽。64 个内核槽跨 agent 重启生效，封住进程内 Policy ceiling 无法覆盖的孤儿累积；继承环境和显式
> RPC env 都剥离私有 mode key。另一个放大器是 `/proc/self/exe` 在 Go 测试里指向当前 `*.test`：第一版只给
> 三个已知包手写 `TestMain`，未接线的 CLI/E2E 测试会递归执行整套测试。现由 `internal/spawnexec.init` 对所有
> 链接者统一、早期分派，门禁反向禁止把责任重新散回各包的 `TestMain`；`make gates` 直接运行该包。
>
> 当前源码重建镜像后，drill 62 三次全新隔离实例均为
> `INCOMPLETE pass=41 assert_fail=0 product_red=0 not_covered=1`；唯一 gap 是独立 OQ-2 true-D 专用硬件臂。
> #81 临时 band 已删除，后续任何 A/B/C 红均重新成为无条件 deviation。故转 FIXED。
>
> **方法论教训（第二条，与 (d) 同族）**：第一版取证代码自己是坏的——用全局变量在 `assert_ok` 的
> 子 shell 里传状态（丢失）、dash 的 `printf` 把 `--` 开头的 format 当选项（截断证据），产出过一轮
> **5/5 假红**。现已改为走文件，且 **oracle A 在 rc 缺失时也红**：harness 坏掉必须自曝，不能冒充产品判决。
>
> ⚠ 下文"机理"三条里的 `spawnsafe.go:NNN` 行号指的是**修复前**（2026-08-29）的树，属事故取证，不追溯改。
> 这是 v0.3.3 `remote-fs-resilience` 增量在**真实长寿命 agent** 上的结构性缺口：该增量的全部 hermetic
> 单测与 OQ-2 spike 都只覆盖**「挂载在第一次探测时就已经死」**，从未覆盖**「探测时活、之后才死」**。

> **修法摘要**：健康判定不再是终态。两条失效路径——**证据驱动**（`boundedResolveInDirs` 与
> `RunStartWithCleanup` 的超时分支、`--safe`、agent 的 Home 有界读超时，共 4 个调用点）+ **惰性 TTL**
> （`DefaultHealthTTL = 5 min`）；作废动作是**换一个全新的 `mountHealth` 指针**（不是原地重置——原地重置
> 会 close-of-closed panic，且掉队 launcher 的 `state == stUnprobed` 守卫会拒绝降级新纪元）。
> **dead 仍绝对 sticky**。零 wire 改动、零新 `agent.yaml` 键 ⇒ **回滚是纯二进制回滚**。
> 闸门：`test/architecture/spawn_stall_evidence_test.go`（铸造点精确账本**双向**enforce + 必须在同一死线臂内
> + **路径敏感**的支配检查：作废必须在到达每个 return 的每条路径上都已**同步**执行（`go`/`defer` 不算，
> 遇 `goto`/label 直接 fail-closed），且能带着 `p.mu` 到达作废调用即红（分支不一致按持有处理，
> 复合语句 initializer 按执行顺序处理，限 `internal/spawnsafe`）；五个外审/复审反例 + 15 例同族正负控制钉住）；行为守卫在
> `internal/spawnsafe/spawnsafe_test.go` 与 `internal/agent/remotefs_test.go`。
>
> **变异验证的诚实说明（内审 F-2 订正）**：初稿写「13 条…每条都做过变异验证」，这句话当时**不成立**。
> 内审查实三件事：(i) 变异是按 `-run` **正则分组**跑的，`TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup`
> 的红其实来自同组的另一条；(ii) 该测试的 `close(stop)` 排在 worker 之前，invalidator 只跑 0–3 圈
> （113µs–1.39ms vs worker 的 12.5–15.6ms），几乎没有 churn 可言；(iii) 它对「`invalidateHealthy` 整体
> 变空操作」也绿——即对它命名的那件事零断言。三条都已修（**逐条单测**、把 `close(stop)` 移到 worker 之后、
> 按**探测次数**断言"重新武装真的发生了"），并补了确定性白盒守卫
> `TestMountHealthy_reArmReplacesGenerationPointer` 把这个设计决策从并发调度里解耦出来。
> **现在的记录是：每条守卫都用它自己声称能抓的那个缺陷、逐条单独跑过，红绿两向都验过。**
> 教训写在这里而不是只写在 review 里：**按正则分组跑变异会互相掩蔽**，这是一种可复发的方法论错误。

> **正文订正（本轮查实，写在这里而不是改冻结的 plan）**：
> - **(a) 当年否掉 TTL 的推理没错，错在它的适用范围没被继续追问。** `remote-fs-resilience-plan.md` §B
>   的原话是：「rejects a plain ~5s TTL, which re-issues a fresh `statfs` against a **still-dead** mount
>   every window and leaks one D-state goroutine **per window**」。注意 **still-dead** 这个限定词——
>   那句话对它的指称对象（一个**连 dead 判定也重探**的 plain TTL）**是真的**，本批并不推翻它。
>   本批做的是换一个指称对象：TTL **只作用于 healthy 判定**，dead 依旧绝对 sticky。在这个约束下泄漏画像
>   完全不同——每个 (挂载, 代) 最多泄一个：重探要么在时返回（零泄漏），要么超时 ⇒ 判死 ⇒ 永不重探；
>   要泄第二个必须先回到 healthy，而那要求前一个被放弃的 statfs 已经返回。
>   **教训不是"当年推理错了"，而是"一条正确的否决被当成了对整个方案族的否决"。**
>   （本条初稿写成「当年否掉 TTL 的理由是假命题」，删掉了 still-dead 这个承重限定词，属歪曲转述；
>   内审 F-22 订正。）
> - **(b) 单靠 spawn-timeout 证据修不了两类零证据负载**，所以必须叠 TTL：① **显式 argv[0] + 本地 cwd**
>   （`exec -- /bin/bash -c …`）不进 `boundedResolveInDirs`、execve 秒成，agent 侧零超时，卡死的是它拉起来的
>   **子进程**——这正是 107 上那几个 D 态 bash 的形态；② **wedge ceiling 打满**时两个看门狗都在 select
>   **之前**早退，任何超时分支都不执行。
> - **(c) `[GAP]` 登记**：`outage=true` 全链路（childEnv PATH 重写 / PWD 注入 / cwd→safe_dir / fallback PATH /
>   dropped 横幅）在部署层**从未跑过一次**——现有 62 的断言全部发生在 `sanitizePATH` **之前**。要覆盖它得给
>   `drills/lib/agentyaml.sh` 加 `remotefspath:` token，而 `agent_provision_yaml` 有 **36 处调用 / 20 个文件**
>   的契约面，该走独立增量。**本次修复会让这条码路第一次在现网点亮**，上线按 plan §7 R1 分级（weilandserver
>   → 单台 timan → 全车队）。
> - **(d) 变异轮的实测教训（三条恒等式，两轮才抓完）**：`TestApplyMounts_carryOverPreservesHealthyFreshness`
>   与 `TestPrepare_slowMountFalseDemotionSelfHealsWithinOneCommand` 初版都是**恒等式**——前者因 churn 期间
>   时钟不走、"刷新 decidedAt"写进去的是同一个值；后者因 `decidedAt` 已被判死那一步盖成非零值，而测试全程
>   冻结时钟。这两条由主进程的变异轮抓出。第三条 `TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup`
>   **躲过了那一轮**，由内审抓出——因为主进程按 `-run` 正则**分组**跑变异，它的"红"来自同组的另一条测试。
>   与 memory 里「批次 B 真实翻车」同形，但多了一条新教训：**分组跑变异会互相掩蔽，必须逐条单测**。

- **现象（生产实测）**：timan107 的 `/shared`（autofs direct 之上的 nfs4 overmount，
  `czhai-storage-01.cs.illinois.edu` / `128.174.136.29`）挂死后——
  - `tether exec timan107 nvidia-smi` → `remote_fs_spawn_timeout`（2.1s）；`tether run` 同；
  - **`--safe` 同样无效**：`tether exec --safe timan107 -- nvidia-smi` 仍是 `remote_fs_spawn_timeout`；
  - 绝对路径**照常可用**：`tether exec timan107 -- /usr/bin/nvidia-smi` 正常出结果；
  - **一条 `[tether agent] remote-fs: dropped N unresponsive network $PATH dir(s)` 警告都没有**
    ⇒ `sanitizePATH` 一个目录都没剔 ⇒ `outage=false`，子进程继承的仍是中毒 PATH（远端 `bash -c` 自己
    查 `timeout` 就 D 住，实测在该机留下多个 D 态 bash）。
  - agent 的 `$PATH` 前两项正是 `/shared/nas/data/m1/zixuans8/miniconda3/{bin,condabin}`。
- **判定归属的实测三角（均在活体上跑）**：

  | 命令 | 耗时 | 说明 |
  |---|---|---|
  | `exec -- echo ok` | **2.14s** → `remote_fs_spawn_timeout` | 命中 `boundedResolveInDirs` 的 `probeTimeout`(2s)——它**真的**去 stat 了 `/shared/.../echo` |
  | `exec -- /shared/.../python -V` | **30.18s** → `remote_fs_spawn_timeout` | 走到 `startBounded` 的 30s execve 看门狗 ⇒ `pathOnDeadMount()` **零延迟返回 false**（没有那 2s 探测）⇒ `/shared` 被判**活的** |
  | 并发 `stat`/`statfs` 逐个 PATH 目录（6s 后看谁还活着） | 只有 `/shared*` 两项 HANG | 死的确实是 `/shared`；`/home/zixuans8`（另一台 NFS）健康 |

- **机理（逐条对源码核实）**：
  1. **分类是对的**：`/proc/self/mountinfo` 第 55 行 autofs `/shared`、第 58 行 nfs4 `/shared`（overmount），
     `mountForPath`（`internal/spawnsafe/spawnsafe.go:486-500`）的 `>=`（外审 F11）正确取到**后者**
     ⇒ `kindRemoteProbe`，不是 autofs 那条已知边界。
  2. **洞在 health 状态机是单向的**：`mountHealthy`（`spawnsafe.go:552-554`）
     `if h.state == stHealthy { return true }` —— **无 TTL、无再验证**。函数注释自述
     "sticky（dead 永不重探）+ self-healing（迟到的成功翻回 healthy）"，**只设计了 dead→healthy**；
     `stHealthy` 同样是**终态**。`spawnsafe.go:540` 的 self-heal drain 也被 `h.state != stHealthy` 挡在门外。
  3. `applyMounts`（`spawnsafe.go:374-405`）在 mountinfo 变动时按 **signature 相同则原样继承**旧判定；
     `/shared` 恰因挂死而**无法被 autofs umount**，signature 恒定 ⇒ 继承链永不断。
  4. 于是：agent 进程活了很久（PID 984969，比当日新 PID 小约 10^6），期间**每一次** exec 都探测过
     `/shared` 并缓存 **healthy**；NFS 是**后来**才死的 ⇒ 该判定在进程生命周期内**永不重探**。
  5. `--safe`（`requestedSafe=true`）只绕过 `bootHangable` 短路、强制 `refreshIfChanged()` 重读挂载表，
     **不作废已缓存的 health 判定** ⇒ `docs/usage.md §7.7`「手动强制」承诺的逃生口在本场景下**结构上无效**。
- **净效果**：保护静默退化成"只剩有界超时"——不剔 PATH、不走 local fallback、不发 `Warn`、不给
  `remote_fs_unhealthy` / `_unsafe_cwd` 的**快速失败**（见 #82），只剩 2s / 30s 两条死线把"永久卡"换成
  "必然失败"。**用户视角就是"那个 NFS 修复没起效"。**
- **为什么现有测试与 drill 全都够不到**：`internal/spawnsafe/spawnsafe_test.go` 的用例全是
  **"探测即死"**（fake probe 首次即返回 false）；OQ-2 的 `62-remote-fs-safe` FUSE spike 同样是
  **先挂载 → 再 SIGSTOP → 才 exec**。**没有任何一条覆盖 healthy→dead 的时序**——这个转换从未被测过。
- **怎么修（建议，未实施）**：
  - **最省成本且自纠正**：`remote_fs_spawn_timeout` 本身就是"某条 healthy 判定已过期"的**证据**——
    在发生 spawn timeout（`boundedResolveInDirs` 或 `startBounded` 任一）时把所有 hangable mount 的
    `stHealthy` 降回 `stUnprobed`，下一次 spawn 自动重探即自愈。**稳态零开销**（健康机器永不触发）。
  - 可选叠加：healthy 判定加 TTL（如 30–60s），仍走既有 single-flight + 有界探针；dead 保持 sticky
    （重探死挂载 = 再泄一个 D 态线程，那条设计是对的）。
  - `--safe` 应额外**强制作废 healthy 判定**，否则文档承诺的手动逃生口名不副实。
  - `docs/usage.md §7.7`「已知边界」补一条：现在只写了"启动后**新挂**的网络盘不检测"，
    没写"启动时健康、**之后才挂掉**的旧挂载同样不检测"——后者才是生产上真正会发生的那个。
- **现场绕过（不改代码即可用）**：绝对路径 `tether exec <node> -- /usr/bin/nvidia-smi`；需要 shell 时
  `-- /usr/bin/env PATH=/usr/bin:/bin /bin/bash -c '...'`（否则子进程继承中毒 PATH）；
  **重启 agent 才是真正恢复**（新进程首探即命中死挂载 → 2s 判死 → 剔出 PATH → 相对命令恢复可用）。
- **钉住它的测试（待补，均需变异验证）**：
  hermetic —— `spawnsafe` 单测加一条 fake probe **首次 true、之后 false** 的用例，断言一次 spawn timeout
  之后的下一次 `Prepare` 会 `Outage=true` 且剔除该目录（不加修复必须红）。
  deploy-tier —— `62-remote-fs-safe` 增一臂：**先跑一次成功的 exec 把 healthy 灌进缓存**，再 SIGSTOP
  hangfs daemon，然后断言相对 argv[0] 的 exec 仍能成功，而不是 `remote_fs_spawn_timeout`。

### #82 — `exec --cwd <死挂载>` 在 #81 的 stale-healthy 态下不快速失败，且其后 agent 失联（CANDIDATE，未归因）

> **来源 = 生产车队。** 2026-08-29 timan107，与 #81 同一次诊断。**仍 OPEN，根因未归因**——本条只登记可复现的
> 观测与时间相关性，**不宣称因果**。

> **⬆ 2026-08-30 更新（#81 修复批）**：**前半段的频率从 O(命令数) 降到 O(1)，但没有被消灭。**
> 精确说法：在同一个 healthy→dead 窗口里，**第一条** `exec --cwd <死挂载>` 仍会被 stale-healthy 放行、
> 仍付一次 30s execve 看门狗、仍 fork 出一个 pre-execve D 态子进程；此后该判定被证据/TTL/`--safe` 作废，
> 后续命令才在 `Prepare` 的 lexical 检查处快速失败为 `remote_fs_unsafe_cwd`。
> 两半都钉住了：`TestPrepare_cwdOnStaleHealthyMountFailsFastOnceInvalidated` 的**控制断言**明确断言
> stale-healthy 期间那次放行仍然发生，`TestPrepare_safeInvalidatesBeforeCwdCheck` 断言 `--safe` 能让
> **第一条**就快速失败。
> （早先这里写的是"前半段已消失"——那与它自己援引的那条测试的控制断言矛盾，也违反本批 plan §7 R13
> 「不要在 gotcha 里宣称 #82 已解决」。内审 F-10 订正。）
> **后半段（agent 转 S 态停止处理消息）仍 OPEN、不修**，理由与新证据如下。
>
> **本轮排除的三条假说（都由读源码证伪，写在这里让下一个人不必重走）**：
> - **假说 A「`--cwd` 路径没接上 30s 看门狗」— 证伪。** `internal/agent/exec.go` 的 `startBounded` 在
>   `decision.Active` 下包住 `cmd.Start`，与是否设 `cmd.Dir` 无关；`cmd.Dir` 在更早的 `buildExecCmd` 就已设好；
>   `run.go` 的 `RunStartWithCleanup` 同理。（本轮还把 exec 侧的 `startBounded` 收敛进
>   `spawnsafe.RunStartWithCleanup`，两条 spawn 路径现在共用同一个看门狗。）
> - **假说 B「一条卡死的 exec head-of-line block 了订阅的消息循环」— 证伪。** `exec.go` 每个 forwarded verb
>   都以 `go a.handleXForwarded(...)` 独立派发。
> - **假说 D「wedge slot 耗尽」— 弱，否决。** 槽位耗尽在两处都是**第一行 return**，产生的是立即返回的
>   `too_many_wedged_spawns`，不是沉默；解释不了「没有任何返回」。
>
> **假说 C（本轮新查出，最强的一条，只登记不修）**：`internal/agent/state.go` 的**写**路径无死线且**持锁**——
> `AddPort` / `UpdatePortHome` / `RemovePort` / `SetProxy` / `SetRosterCache` 全在 `s.mu` 下调用无界的
> `saveLocked`；只有**读**被做成有界 + 单飞。Home 落在死挂载上的一次写会**永久持有 `s.mu`**，此后每个
> `load()` / `GetProxy()` / `GetRosterCache()` 全部堵在它后面——这与「原 agent 进程 S 态活着但停止处理消息」
> 比任何其它候选都吻合，且 `roster.go` 的 `SetRosterCache` 提供了一条不需要用户操作就能触发的路径。
> **这是假说不是结论**：timan107 的 Home 是 `/home/zixuans8`，当时实测**健康**，所以要成立需要该 NFS 后来也
> 出问题、或另有写路径落在 `/shared`。**下一个增量必须先验证再动手。** 这条源码事实（与 #82 是否归因无关）
> 已补进 `docs/usage.md §7.7` 的已知边界。
>
> **两个 pre-execve D 态 fork 子进程**（cmdline 仍是父进程 argv、etime 与那条命令吻合）依然**无解释、无修复**。
> 复现定格不变：隔离宿主 + hangfs，不要在共享/生产机器上跑。

- **现象**：在 #81 的 stale-healthy 态下跑 `tether exec --cwd /shared/nas timan107 -- /bin/echo hi`：
  - **没有**返回 `remote_fs_unsafe_cwd`——`Prepare` 的 lexical 快速失败因 `pathOnDeadMount(cwd)` 拿到
    stale-healthy 而不触发。**这一半是 #81 的直接后果，机理清楚。**
  - 也**没有**在 `startBounded` 的 30s 看门狗回 `remote_fs_spawn_timeout`；而同一台机同一批实验里，
    `exec -- /shared/.../python` **确实**在 30.18s 正常回了该错，所以看门狗本身当时是活的。
  - ctl 侧 90s 超时后，该节点在 broker 上转 **OFFLINE 并持续未回**（timan107 无 sudo / 无 systemd 自启，
    靠 NFS `.bashrc` 登录自启，故不会自愈，需跳板重拉）。
- **未归因的部分**：`cmd.Dir` 落在死挂载上时 chdir 发生在 **fork 之后的子进程**里，父进程阻塞在读
  status pipe 上，`startBounded` 本应 30s 后放弃并回 `ErrSpawnTimeout`——这次没回。是看门狗在 `--cwd`
  路径上没接上、还是 agent 另有他因退出（该机在诊断**之前**就已积压 `find` 与多个 `-bash` 的 D 态进程），
  **现有证据不足以断言**。
- **值得注意的先例**：OQ-2 的 spike 结论已写明「**mode:off-WITHOUT-safe 的遗留裸挂死**会驱使 agent 对死挂载做
  **无界** chdir/exec……会 wedge agent」，并据此把该臂定格 NOT-COVERED、留给隔离宿主。本次是**同一风险在
  生产上被观察到**，而且**不是** mode:off ——是 #81 让 `auto` 模式退化成了等价于 off。
- **复现前提**：必须先造出 #81 的 stale-healthy 态（长寿命 agent + 探测时健康、之后才挂掉的网络盘），
  所以它**不能**在现有 `62-remote-fs-safe`（先挂载后 SIGSTOP）里复现——同 #81 的"为什么现有测试够不到"。
- **下一步**：先修 #81；修完后本条前一半会自动消失，再单独复核 `--cwd` 路径的 30s 看门狗是否真的接上。
  **复现请用隔离宿主 + hangfs，不要在共享/生产机器上跑**（OQ-2 的定格理由依然成立）。

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
| #45 | FIXED | retire op 卡 NATS_ROLLED_OUT 永不达 terminal RETIRED；drill 40 GREEN 确证（#31 修复连带解除） |
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
| #75 | FIXED | broker.yaml 静默失效（serveconf 非严格解析）+ log-sink 无可见性；g75-g78 严格化 + inert TLS stub + config-check + breadcrumb（外审 F1 整合） |
| #76 | FIXED | install.sh broker unit 不 enable → 开机不自启；g75-g78 默认 enable + --no-enable + 三分支 banner（外审 F2/M4 整合） |
| #77 | FIXED | journald 无默认 SystemMaxUse；g75-g78 条件写三档 drop-in + ownership marker（外审 F3 整合） |
