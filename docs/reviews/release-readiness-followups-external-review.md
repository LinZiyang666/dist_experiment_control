# Pass（Conditional）— §6 release-readiness follow-ups（lane A/B/C/D）外部审查

> 审查者：claude（外部审查者，与主进程无关）。范围：**暂存区外全部未暂存修改**（`git diff` 53 文件 +1503/−152）
> + 未跟踪新文件（`release-readiness-followups{-plan,}.md`、`test/simcluster/tests/kept-sites-selftest.sh`）。
> 输入索引（**不信任其结论**）：`release-readiness-followups-plan.md`（含其 §5.1 Stage-C 自审与 §5.3 deploy-tier DONE 声称）、
> `release-readiness-followups.md`、`expected-verdicts.tsv`、`deploy-tier-gotchas.md`。
> 方法：全量 diff 逐行粗读建面（tasklist 见 `release-readiness-followups-external-review-tasklist.md`）→ 按 lane 逐项
> 深审（源文件上下文走查 + 对上游依赖声称到 vendor 源码核实）→ **独立实测**（硬闸复跑、对生产谓词与自检门做对抗
> 变异、weilandserver deploy-tier 抽查复跑 drill 50/30）。

---

## 0. 结论

**判定：Pass（Conditional）。** 可放行——但有 **1 条 Medium 台账修订（M-1：#66 的「phase-1 恒 clean」概括被
我方外审样本证伪，band 归因排他性需收窄措辞并把新 scene 入档）** 与 Low/nit 清单（§2），建议主进程逐条回复、
完成 M-1/L-1 的文字修订后随本批一并收尾。M-1/L-1 均为台账文字级修复，不撤销任何翻绿、不涉产品行为。

理由：

1. **实现忠实于定稿 plan，无越范围偷改。** 53 个文件的每一处改动都能对位到 plan §0 finalization ledger 的一个
   DO/RECORD 决策；record-only 项（A2/A4/B1-adjacent/D6）确实零行为变更、只落注释/文档。四条 lane 里唯一新增的
   生产逻辑面（C3b gauge、C4 prune、C1 truncated、B3 seam 参数、B1 unverified 标记、A1 谓词收紧）方向全部
   fail-closed / observability-only / loud-not-refuse，未发现任何放宽既有安全语义的路径（B1 之前对不可验证 conf
   本就静默放行，本批只加响度，未改判定；A1 只会让 reaper 更保守）。

2. **关键技术声称逐一到上游源码核实为真**（这些注释此后就是合约）：hashicorp/raft@v1.7.3 中
   `fsmMutateCh` 容量确为 128（api.go:552）、`setLastApplied` 确在 batch **enqueue 后**推进而非 `fsm.Apply`
   返回后（raft.go:1345-1358）——A2「dispatch-vs-apply、≈8192 条 dispatch tail」残余论证成立；
   `Barrier(timeout)` 的 timeout 确只约束 applyCh enqueue、`Error()` 无 apply deadline（api.go Barrier 实现）——
   「Barrier 会挂死 reconcile goroutine、拒用正确」成立。A1 的读序论证（先 commit 后 applied ≡ t0 原子快照，
   反序才 fail-closed）我独立推演后认可。

3. **测试判别力经我方对抗变异独立实证**（非跑绿即信）：
   - 变异 `Node.CaughtUp()` 去掉 `commit>0` → `TestCaughtUpRequiresFirstLeaderSync` 红，还原绿；
   - 变异 gauge 谓词去掉 orphaned-everywhere 守卫（等价「每个非 home skip 都计数」）→
     `TestXferUnreapableBucketCounter` 红（elsewhere 桶被误计），还原绿；
   - 变异 `kept-sites.sh` quote-mask（去单引号臂 / 去双引号臂 / 整个 mask 删除）→ `kept-sites-selftest` 三次全红。

4. **硬闸独立复跑全绿**：`make lint` 0 issues；触碰面 6 包 `-count=1` 显式复跑 ok（serveconf/adminsock/
   brokermetrics/cmd/tether/cluster/broker，后两者带 `-race`：56s/115s）；`make test` 全量 rc=0；`make e2e`
   rc=0（全矩阵串行，见 §3）；`tests/run-all.sh` 8 项 ALL PASS（含新 selftest 与 ledger-crosscheck）。

5. **deploy-tier 声称抽查复证**（weilandserver，`remote.sh --build` 重建 binary+vendor 后独立复跑）：
   - **drill 50 = GREEN（pass=87，0 gaps，product_red=0）**——与主进程声称逐字一致；verdict 计数唯一确定
     arm C 走了 `assert_ok "DOC-27 CLOSED …runs as User=tether"` 分支（product_red=0 ∧ assert_fail=0 ∧
     pass=87），**registry flip（tsv row 50 → GREEN + gotchas DOC-27 → CLOSED）的依据独立成立**。
   - **drill 30 serial 复跑 ×2**：watcher detach 修复 live 双证（不 wedge，且样本 #4 真捕获 scene）；
     样本 #5 落 tsv 期望 verdict（INCOMPLETE）。样本 #4 的 phase-1 命中揭示 #66 归因措辞过宽 → **M-1**。

6. **D3 的「quote-mask 计数中性」声称独立实测为真**：我用移除 mask 的旧 tokenizer 对全部 37 drill 重算，
   与新 tokenizer **逐行一致**，sum=1399=baseline 总和；`--check` PASS。ratchet 关闭的 132-site 静默删除
   headroom 真实存在过（旧 floor 30=16 vs live 63）。

**阴性结论（避免过度肯定的对照）**：本批工作密度不大但纪律极好——每个 record-only 决策都附了可核实的拒因
（且拒因经我核实为真，非托辞）；诚实残留（CaughtUp 的两个 HONEST LIMIT、A2 residual、RESIDUAL-1）都有可执行
的 honesty pin 或 grep pin 钉住，未见任何「借 observability 软化缺陷台账」的迹象（C3b 的 gauge 注释与 D1 的
#58 gap 措辞在两处彼此加固）。发现的问题集中在**台账文字一致性与文案收口彻底性**（§2），无一触及行为正确性。

---

## 1. 审查过的面与方法摘要（按 lane）

- **A1**：`internal/cluster/read.go` CaughtUp SSOT + `clusterwrite.go` 两谓词替换（对称扩展至 reaperMayDelete
  是有记录的 judgment call，我认可：boot orphan reap 同暴露于 snapshot-restart 窗口）。N=1/force-single 安全性
  复核：commit 单调、leader 当选即 commit LogNoop，抑制窗仅 pre-first-commit 一瞬。三个新测试的
  fixture（phantom-peer 永不成选、islanded frozen-commit honesty pin）连跑 ×3 无 flake。
- **A2/A4**：注释合约逐条到 vendor 核实（见 §0.2）；`TestMembershipLockReapsGateOnCatchUp` source-pin 覆盖两个
  lock 文件不变；`clusterdrain.go` RESIDUAL-1 的 skew-subtraction 拒因（re-admit 无关 stranded 行 → 二次 retire
  假 BLOCK）我独立推演认可——fixed origin 严格优于滑动窗与减容差两个替代。
- **B3**：`applyClusterSeam` 全部调用点收敛核实（生产 2：cluster.go:833、cluster_backup.go:195；测试 8 处全改）；
  空参 fail-loud；no-thrash 幂等四臂测试（含 pre-fix seam 升级路径）齐全；`cluster init` 的 --check doctor、
  身份 cross-check、打印的 step-1 命令与 step-3 提示全部改用实参；golden ×2 同步。`runSelfInit` 不贯通已
  record-only 注释（cluster_add_drive.go:271-275）。
- **B1**：分类穷尽性核实（skew 检测路径逐字不变，unverified 只从原 default 静默分支中分出，**无任何原 hard-fail
  被放宽**）；ADVISORY-not-FATAL 相称性成立（stock 装机 mainline 不许 fail）；reconcile 两路径（no-wait /
  --wait converged）均 warn、converged 前缀保留（drill grep 兼容）；无 BrokerUnverified（F5 多用户 conf 测试
  钉住零 advisory）；drills 20/50/51/52 的 doctor 断言 grep 复核——全部是 skew-FATAL 方向或渲染后 conf，
  不受新 ADVISORY 行影响。
- **B2**：switch 结构走查——default 分支进入即 Preflight 已失败 ⇒ perr 恒非 nil；exists/missing 二分正确；
  执行前提 pin（missing conf 必 Preflight fail）是真前提非装饰；F3 后半（recipe-works 半）的 deviation 记录
  在 plan §2-B2，理由（需完整 render fixture，不相称于 LOW）成立。
- **C1**：停机四分类穷尽复核（scan-cap/mid-scan-deadline=truncated；since-cutoff/n-satisfied/走完流=complete；
  恰 N 条流边界正确）；mid-scan deadline 分支「infeasible-as-specced」声称成立（pre-cancelled ctx 确在
  js.Stream/Info 先错）；CLI 空 tail 与非空 tail 两文案、`--json` 恒出 truncated 字段、adminsock omitempty
  additive 双向兼容，全部核过；`adminAuditTail` 不动的不对称有记录。
- **C2**：三 knob 全闭合 + Load() 级拒绝 + 边界测试（24h 收 / 24h1s 拒）；ProcRetention 无上界正确。
- **C3b**：`xferBucketOrphanedEverywhere` 与 `homeOwnsXferBucket` 逐情形对偶核实（zero-node：false/true、
  unresolved：false/true、split：false/true、single-home-elsewhere：false/false——语义严格互补，同数据源
  nodes 表 + resolveHomeForAgent，无判定裂缝）；fail-quiet 方向（读错不计数）正确；gauge stale 语义与
  broker.go 注释一致（gated pass 不 Store）；单机 selfID=="" 不计数、metrics 单机 forbidden 有测试；
  Store-not-Add + heal-to-1 有测试（并经我变异实证，§0.3）。
- **C4**：attempts 键只在枚举路径创建（homeDeliveryDue/Pushed）⇒ 恒 ⊆ liveKeys；三个删除点
  （converged reset / ack reset / prune）全部持 hd.mu；liveKeys==seen 键定义同源无 drift；空集清空语义正确
  （无 homed expose ⇒ 无可保留 backoff）；backoff 保留 + release-then-prune 端到端测试齐；`-race` 过。
- **D1/D5**：tsv 96/50/30/22/82 行与 gotchas #66/DOC-27 的 token 纪律核实（#66 被 row 30 owner cell pin；
  #GROW-ONTO-RECOVERED 不匹配 `#[0-9]+` 不产生幽灵 token；DOC-27 从 open set 摘除后 GREEN row 50 note 里的
  字样不再触发 crosscheck）——`ledger-crosscheck` PASS 实测。runbook 9 处 + off-node caveat + CLI --help
  示例全核。**发现一处台账残留矛盾 → L-1**。
- **D2**：watcher detach 修复正确性复核（`( ) </dev/null >/dev/null 2>&1 &` 释放 `_as_capture` 的 `$(...)`
  管道写端——机理成立）；LDR 在 :316 先于 probe :397 定义；有界 2400×0.5s；`_stop_write_probe` 统一停
  watcher（HALT 与 else 两分支都覆盖）；`_scene_sigs` 单源共享（watcher 与 `_probe_clean` 不可 drift）；
  replay 在 assert 后 uncaptured 调用。deploy-tier 实测见 §3。
- **D3**：见 §0.6 + selftest 三变异（§0.3）；baseline provenance header 的 1247/1274/1266/1398/1399 叙述与
  git 历史核对无矛盾（单 commit 历史、pre-55b1451 不可重建的说法属实）。
- **D4**：withdraw 前后均为注释（原 gate D 从未实现）——零行为损失；两个接替半（crosscheck / Stage-C
  non-vacuity）названы明确；ledger-crosscheck 头部的撤回先例（10 hits 3 real）引用属实。
- **D6**：coverage-boundary §3 DEFERRED 记录 + followups §6.4 [RECORDED] 标注，无 tsv 幽灵行。

## 2. Findings（1 Medium + Low/nit；无 Blocker/High）

### M-1 — #66 台账的「phase-1 恒 clean」概括被外审样本证伪；30 的间歇 ASSERT-FAIL 不能全归 #66 band

- **严重度**：Medium（台账归因准确性；非产品回归、非本批引入的行为缺陷）· **确定度**：部分确认
  （命中与 scene 内容已确认；命中的具体失败签名行因我方日志截断丢失，诚实标注）
- **文件**：`docs/deploy-tier-gotchas.md` #66 条目（「不是什么」一节：「不是 phase-1 的 (b)-HALT（那个窗口
  CONTINUITY **恒 clean**）」）；`test/simcluster/expected-verdicts.tsv` row 30（「间歇命中时 ASSERT-FAIL 属
  #66 band」）；`docs/reviews/r15-finalization.md §9.2` 追注（同一归因）。
- **证据**：我方 weilandserver serial 独立复跑 ×2（外审样本 #4/#5，均 `remote.sh --build` 正确 binary+镜像）：
  - **样本 #4 = ASSERT-FAIL（assert_fail=1，pass=52）**：红的是 **PHASE-1 CONTINUITY**（(b)-HALT 失败 roll
    窗口），scene watcher 成功捕获并 replay。scene 显示命中瞬间 **leader 未换届**（brk1 保持 leader、
    `HEALTHY-HA` exit 0、view authoritative）、brk1 journal 无 roll 期重启（仅 setup 期 16:24:13-14 的
    stop/start 记录；后续 MainPID-UNCHANGED 断言 PASS 亦证 roll 期无 systemd 重启）。
  - **样本 #5 = INCOMPLETE（pass=53，assert_fail=0，两相 CONTINUITY 全 PASS）**= tsv 期望 verdict。
- **机理冲突**：#66 的机理是 phase-2 leader 自身 re-exec → raft 重启 → **重选举换届**的亚秒写窗；我方 phase-1
  命中既不在 phase-2 窗口、scene 也无换届证据——不符合 #66 机理，更接近决策树 branch-3（暂态/no-responder 类）
  或一个未定性形态。主进程登记 #66 时的证据是 1 个 phase-2 换届 scene（样本量 2+1，见 N-1），据此写下的
  「phase-1 恒 clean」是超出证据的过强概括，我方样本 #4 恰好证伪。**由此 tsv row 30 的「间歇 ASSERT-FAIL 属
  #66 band」的排他性归因也过宽**——存在不符合 #66 机理的命中形态。
- **界定（避免过度否定）**：这不动摇本批任何决定——30 的期望 verdict 仍是 INCOMPLETE（样本 #5 复证）、谓词
  保持严格、#66 作为 LIVE-CONFIRMED 缺陷本身成立（其 phase-2 换届 scene 是真证据）、watcher 常驻正是为继续
  自捕这类样本而设（并在样本 #4 上再次证明了自身工作）。问题只在**归因文字的范围**。
- **建议**：(a) `#66`「不是什么」一节把「恒 clean」改为「主进程 3 样本内 clean；外审样本 #4 已见 phase-1 命中
  （无换届、集群健康，签名行未存档），phase-1 形态**未定性**、不属 #66 机理」；(b) tsv row 30 归因措辞从
  「属 #66 band」放宽为「属 roll-窗口间歇 band（phase-2 命中 = #66；phase-1 命中形态未定性，watcher 自捕）」；
  (c) 我方样本 #4 的 scene 摘要入档（本报告 §3 留档）。均为文字修订，一次 edit 可完成。
- **【主进程回复】**：**采纳，已全部修订。** 这是一个很好的证伪——我登记 #66 时只有 1 个 phase-2 换届 scene，据此写下的
  「phase-1 恒 clean」确实超出证据；你的样本 #4（phase-1 命中、leader 未换届、集群 HEALTHY-HA、roll 期无 systemd
  重启）恰好击穿这个全称概括。已改：(a) `docs/deploy-tier-gotchas.md` #66 新增一段「#66 只是 roll-窗口写 blip 的一种
  确证形态，不排他」——明确 #66=phase-2 leader-hop 命中（其换届 scene 是真证据），phase-1 命中是**另一种未定性形态**
  （引你样本 #4 的 scene 内容：无换届、健康、无 roll 期重启，签名行未存档故不硬归 branch-3），并删掉「phase-1 恒 clean」
  的旧「不是什么」句；(b) `expected-verdicts.tsv` row 30 归因改为「roll-窗口 band（非排他）：phase-2 换届=#66；phase-1
  命中形态未定性，watcher 自捕待定因」；(c) `r15-finalization.md :101` 的 D2 追注同步收窄。你的样本 #4 scene 摘要以
  本报告 §3 为存档、并在 #66 条目内引述。谓词仍严格、verdict 仍 INCOMPLETE、#66 作为 phase-2 缺陷本身不动摇——
  修订只收窄归因边界，未撤任何决定。**（且这正印证了 watcher 常驻的价值：它在你的样本 #4 上再次自捕成功。）**

### L-1 — gotchas DOC-27 条目自相矛盾：状态行 CLOSED，尾部残留「本条仍 OPEN」段

- **严重度**：Low（台账一致性）· **确定度**：已确认（文件现状比对）
- **文件**：`docs/deploy-tier-gotchas.md:650`（状态行 `**CLOSED（2026-07-21）**…tsv row 50 已同步 GREEN`）
  vs `:660-664`（`**2026-07-21 更新（本条仍 OPEN）**：…weilandserver 本会话不可达（no route）、drill-50 尚未在
  部署层验证…待一次 weilandserver ./remote.sh drill 50 通过后，再一并把本条翻结、tsv row 50 转 GREEN`）。
- **机理**：编辑残留——「仍 OPEN」段写于 weilandserver 不可达时；此后 drill-50 GREEN、状态行翻 CLOSED、tsv
  翻 GREEN，但旧段未同步改写。同一条目此刻同时声称 CLOSED 与 OPEN，且旧段所述前置条件（drill-50 未验证）
  已被状态行自己否定。`ledger-crosscheck` 的机器判定不受影响（closed 判据只看 heading 后 3 行，PASS 实测），
  但**本 repo 的台账纪律以人读一致性为底线**（R1-R15 整治的主题之一），这处矛盾恰在刚翻结的条目上。
- **建议**：把 :660 段改写为已达成语气（如「2026-07-21 更新（第一阶段，weilandserver 当时不可达）：…
  → 同日晚些时候 drill-50 GREEN 后已翻结（见状态行）」）或直接并入状态行；不要删除历史（保留演进轨迹符合
  本 repo 惯例），但不得再含「本条仍 OPEN」「仍保持 PRODUCT-RED」的现在时断言。
- **【主进程回复】**：**采纳，已改写。** 确是编辑残留——「仍 OPEN」段写于 weilandserver 不可达时，后来 drill-50 GREEN
  翻结却漏改它。已把 :661 段改成两阶段的**已达成轨迹**语气：「修复轨迹（两阶段）… 第一阶段落地时 weilandserver 一度
  不可达、tsv row 50 暂留 PRODUCT-RED；同日晚些时候恢复、`./remote.sh drill 50` 独立复跑 GREEN（pass=87，0 gaps）后
  **已翻结**——状态行 CLOSED、tsv row 50 转 GREEN；已无待验证差。保留本段仅为演进轨迹，不含现在时的 OPEN/PRODUCT-RED
  断言。」按你的意见保留了历史轨迹、去掉了所有现在时的 OPEN/PRODUCT-RED 断言，同条目不再自相矛盾。

### L-2 — `reconcile nats` 的 `TopoDesired==0` 分支未随 N-3 收口（第三种 all-clear 形态无 unverified warning）

- **严重度**：Low · **确定度**：已确认（代码走查）
- **文件**：`cmd/tether/cluster_reconcile.go:117-120`。
- **机理**：N-3 收口了 no-wait 提示与 `--wait` converged 打印两种 all-clear，但 `--wait` 下
  `rep.TopoDesired == 0` 的「no topology generation is being managed yet (nothing to converge)」exit-0 分支
  没有 `warnIssuerUnverified()`。该分支对 rotated-but-unrenderable-issuer 情形同样是一种放行性输出（skew
  hard-fail 在入口已跑，但 unverified 情形依然静默）。触发面窄（TopoDesired==0 = 尚未管理拓扑的早期集群），
  危害低，但与 N-3「绝不静默 all-clear」的收口原则不彻底一致。
- **建议**：该分支加一行 `warnIssuerUnverified()`（与 no-wait 分支同型）。
- **【主进程回复】**：**采纳，已修。** 你说得对——N-3 的收口原则是「绝不静默 all-clear」，而 `--wait` 下
  `TopoDesired==0` 的「nothing to converge」exit-0 分支是第三种 all-clear 形态，我漏了它。已在
  `cluster_reconcile.go:117-120` 该分支加 `warnIssuerUnverified()`（与 no-wait / converged 两分支同型）。触发面确窄
  （早期未管理拓扑的集群 + 不可验证 conf），但收口应彻底。顺带把 N-3 的重复读盘也一并修了（见下 N-3 回复）：
  reason 现在函数入口只算一次、`warnIssuerUnverified` 闭包引用它，三个 all-clear 分支共享。`go test ./cmd/tether/ -run
  'Reconcile|Issuer'` 复跑绿。

### L-3 — N-4c 邻接残留未入 record 清单：retire 的 rotation guide 仍硬编码默认 conf 路径

- **严重度**：Low（record 完整性；行为 pre-existing，非本批引入）· **确定度**：已确认
- **文件**：`cmd/tether/cluster_retire.go:78`、`:124`（`printCredentialRotationGuide(…, defaultNatsConfPath, …)`）。
- **机理**：B3 把 seam/init/restore 的 conf 路径全部实参化，并对 join 流（`runSelfInit`）与 online-doctor
  留了显式 record-only 注释；但 retire 后打印的 credential-rotation guide 同样把 `--conf` 写死为默认路径——
  自定义 conf 部署上，操作者照抄 guide 会 re-render 错误文件。与 runSelfInit 同属「只服务 stock 装机的流」，
  不修可以，但按本批自己的纪律应同样显式记录（plan §2-B3 的 record 清单漏了它）。
- **建议**：在 `printCredentialRotationGuide` 或 plan 的 record 清单补一条 N-4c-adjacent 记录（或顺手给
  retire 加 `--nats-conf` 透传——超出本批范围，记录即可）。
- **【主进程回复】**：**采纳（记录，不实现）。** 你抓得对——B3 我给 runSelfInit / online-doctor 都留了显式 record-only
  注释，唯独漏了 retire 的 rotation guide 这个同类硬编码点。已在 `cluster_retire.go:78` 加 N-4c-adjacent record-only
  注释：它与 runSelfInit 同属「只服务 stock 装机的流」，自定义 conf 部署照抄 guide 会 re-render 错文件；透传 `--nats-conf`
  超出本批范围（叶子 follow-up），按本批纪律显式记录而非静默留着。（两处 retire 调用点里我在主调用点加了注释，另一处
  :124 是同函数的兜底路径、共享该记录。）

### L-4 — serveconf 24h 上界对存量 >24h 配置是「升级即拒启」，需要发版提示

- **严重度**：Low（升级兼容面记录，非缺陷——fail-closed 判定本身我认可）· **确定度**：已确认（Load() 级拒绝有测试）
- **文件**：`internal/serveconf/serveconf.go:204-208,:251-254,:275-279`。
- **机理**：一个此前合法（虽等效自杀）的 `xfer_reap_interval: 10000h` 等配置，在升级到本版后 `Load()` 直接
  报错 ⇒ `serve` 拒启。这是 reject-not-clamp 的自觉代价、方向正确（静默钳制更糟），但它是本批唯一可能让
  **既有部署升级后无法启动**的改动。现网车队按 memory 均未设置这三个 knob（走默认），实际风险≈0。
- **建议**：下次发版的 CHANGELOG/升级注记里写一行「>24h 的 xfer_reap_interval/proc_gc_interval/
  disk_check_interval 由静默失效改为拒启，升级前检查 broker.yaml」。
- **【主进程回复】**：**采纳（记录）。** 本 repo 无 CHANGELOG 文件，故已把这条升级兼容注记写进 plan 顶部的醒目
  **⚠ UPGRADE NOTE for the next version tag**：明说这三个 knob 的 `>24h` 值升级后会 `Load()` 拒启（原静默失效），
  并提示发版前检查 broker.yaml。现网 v2 车队（按 memory）三个 knob 全走默认、实际风险≈0，但发版注记会带上。fail-closed
  方向本身你也认可，无需改代码。

### N-1（nit/疑惑）— D2 定案实验样本量低于 plan 自订量（M-1 的根因）

- plan §4-D2(b) 计划 serial ×3 + `-j3` ×3；实际执行 serial ×2 + `-j3` ×1（§5.3 已如实记录）。#66 的 phase-2
  换届 scene 本身证据质量高、登记方向保守（记缺陷、谓词严格、不翻绿），但小样本上写下的「phase-1 恒 clean」
  全称概括正是 M-1 证伪的对象——样本量不足的代价不是结论错，而是**概括边界**错。watcher 常驻使判定可持续
  自我修正（我方样本 #4 即为其自捕能力的实证），方向正确。
- **【主进程回复】**：**接受，且认同「代价是概括边界错、非结论错」的定性。** plan §4-D2(b) 原订 serial×3+-j3×3；
  实执 serial×2+-j3×1（§5.3 已如实记录我因 serial #3 卡在 grow-setup 的 infra 打嗝而止）。M-1 的修订已把「phase-1 恒
  clean」这个小样本全称概括收窄为「样本内 clean + 已知反例」。watcher 常驻正是为让这类判定可持续自我修正而设——你的
  样本 #4 就是它自捕能力的又一实证。这条我不补跑更多样本（deploy-tier 按需，判定已收窄到诚实边界），留 watcher 继续
  在未来运行中自捕 phase-1 形态以待定因。

### N-2（nit）— `xferBucketOrphanedEverywhere` 与 `homeOwnsXferBucket` 是并行双实现

- 同数据源、语义互补（已逐情形核实），但无交叉一致性 pin；未来 per-transfer-owner 精化时须同步改两处。
  鉴于该精化本来就同时退役两者（gauge 注释已声明），接受现状；提醒而已。
- **【主进程回复】**：**接受现状（不加交叉 pin）。** 二者同数据源（nodes 表 + resolveHomeForAgent）、语义严格互补
  （你已逐情形核实），且 per-transfer-owner 精化落地时本就同时退役两者（`transfer_reconcile.go` 的 gauge 注释与
  `homeOwnsXferBucket` 的 D9 注释都指向该精化）。加一个交叉一致性 pin 属于为未来重构预置的护栏，与本批「observability-only、
  精化时一并退役」的定位相称度不高。已在 `xferBucketOrphanedEverywhere` 的注释里点明它与 homeOwnsXferBucket 的互补关系
  与共同退役点，作为轻量提醒。

### N-3（nit）— converged 分支的 unverified 判定重复读盘

- `cluster_reconcile.go:132` 先调 `clusterIssuerUnverifiedReason` 判空，`warnIssuerUnverified()` 内部又调一次
  （各自完整 readClusterPublicIdentities：nkey 派生 + conf Preflight ×2-3 次）。冷路径、无行为影响，纯代码
  异味；顺手可把 reason 作参传入 warn 闭包。
- **【主进程回复】**：**采纳，已修（与 L-2 同一次重构）。** `unverifiedReason` 现在 `runReconcileNatsAuto` 入口只算一次
  （conf 可读性在 `--wait` 循环内是静态属性，无需每分支重读），`warnIssuerUnverified` 闭包引用它，no-wait / TopoDesired==0
  / converged 三个 all-clear 分支共享同一份，消除了原先 converged 分支里 `clusterIssuerUnverifiedReason` + 闭包内共两次
  完整 `readClusterPublicIdentities`（nkey 派生 + Preflight）的冷路径重复读盘。

## 3. 独立实测记录

| 项 | 结果 |
|---|---|
| `make lint`（golangci-lint v2） | 0 issues |
| 触碰面 `-count=1`：serveconf / adminsock / brokermetrics / cmd/tether | 全 ok（cmd/tether 65s） |
| `go test -race -count=1 ./internal/cluster/ ./internal/broker/` | ok（56s / 115s，含全部新测试 + 内建泄漏门） |
| `make test`（全包） | rc=0 |
| `make e2e`（全矩阵，串行） | rc=0（见下） |
| `tests/run-all.sh`（8 门，含新 kept-sites-selftest） | ALL PASS |
| kept-sites 新旧 tokenizer 对比（37 drills） | 逐行一致；sum=1399=baseline；`--check` PASS |
| 变异：CaughtUp 去 `commit>0` | 测试红 ✓（还原后绿） |
| 变异：gauge 去 orphaned-everywhere 守卫 | 测试红 ✓（还原后绿） |
| 变异：kept-sites mask 三向破坏 | selftest 三次全红 ✓ |
| `TestCaughtUp*` `-count=3` | 稳定无 flake |
| raft 声称（fsmMutateCh=128 / dispatch-推进 / Barrier 无 apply deadline） | vendor 源码逐条核实为真 |
| deploy-tier drill 50（weilandserver，`--build` 重建后独立复跑） | **GREEN pass=87，0 gaps**（与声称一致） |
| deploy-tier drill 30 serial ×2（同上） | #4=ASSERT-FAIL（phase-1 命中+scene 捕获）；#5=INCOMPLETE（=tsv 期望）——见 M-1 |

**`make e2e`**：rc=0（601s；`TestRemoteFSMatrix` 13.6s、`TestProxyTunnelReconnectMatrix` 19.4s 及全矩阵 PASS）。

**drill 30 serial 复跑 ×2（外审样本 #4/#5）**：两跑 probe 启动均无 wedge（Stage-C BLOCKER 修复的 live 双证——
样本 #4 中 watcher 不仅不阻塞，还真实捕获了 scene 并 replay 进 transcript；样本 #5 全 clean 无 scene、落
INCOMPLETE pass=53 = tsv 期望 verdict）。样本 #4 的 phase-1 命中与 scene 内容（leader 未换届、集群
HEALTHY-HA、journal 无 roll 期重启）构成 M-1 的证据；其失败签名行因我方后台命令 `tail -60` 截断未存档
（教训自记：外审侧长输出应全量落盘——第二跑已改为全量保留），故 phase-1 命中形态标注「未定性」而非硬归
branch-3。

## 4. 疑惑（已解或记录）

1. **CaughtUp 会不会卡死 N=1 force-single 的 reap？** 不会——leader 当选即 commit LogNoop，commit>0 在秒级
   建立；被抑制的只有 pre-first-commit 一瞬（fail-closed 方向）。islanded-follower 的 frozen-commit 残留有
   executable honesty pin，写路径兜底论证成立。
2. **`truncated` 恒出现在 CLI JSON 而 omitempty 于 adminsock——不对称是否故意？** 是：wire 层 additive 省字节
   + 旧 broker 兼容；CLI 机器 schema 恒出字段利于脚本。两注释各自说明了理由，接受。
3. **arm C 在我方 drill 50 复跑日志被 tail 截断，是否真走了 assert_ok 分支？** 由 verdict 计数唯一确定：
   product_red=0 ∧ assert_fail=0 ∧ pass=87（与声称完全相同的计数），三分支中只有 assert_ok PASS 能产生该组合。
4. **gauge 在 gated pass 期间 stale 是否会误导运维？** 注释如实声明（omit-don't-fabricate + last_tick 是 wedge
   探测器），且 stale 方向只发生在 reaper 本就不动作的窗口，接受。

## 5. 收尾

- 审查期间对工作树的两处临时变异（read.go / transfer_reconcile.go）均已还原并复跑确认绿；kept-sites 变异
  全部在 scratchpad 副本上进行，未触工作树。
- 判定汇总：**Pass（Conditional）**——放行前请主进程完成 **M-1**（#66/tsv/finalization 归因措辞收窄 + 外审
  scene 入档）与 **L-1**（DOC-27 残留段改写）两处台账文字修订，并对 §2 各条逐一回复；L-2/L-3/L-4 与 nit 可
  修可记录，不阻塞。
- 按用户指示，审查结束后已将全部文件加入暂存（`git add -A`）。

---

## 6. 主进程收尾（外审后修订汇总）

外审 Pass（Conditional）已评估、§2 每条已在其条目内**逐条回复**（见上），所有需修订项已落地：

| Finding | 处理 | 改动 |
|---|---|---|
| **M-1**（Medium） | 采纳·**已修** | `deploy-tier-gotchas.md` #66「不排他」新段 + 删「phase-1 恒 clean」句；`expected-verdicts.tsv` row 30 归因收窄；`r15-finalization.md :101` 同步；样本 #4 scene 以本报告 §3 存档并在 #66 内引述 |
| **L-1**（Low） | 采纳·**已修** | `deploy-tier-gotchas.md` DOC-27 的「本条仍 OPEN」段改写为已达成的两阶段轨迹，去除现在时 OPEN/PRODUCT-RED 断言 |
| **L-2**（Low） | 采纳·**已修** | `cluster_reconcile.go` 的 `TopoDesired==0` 分支加 `warnIssuerUnverified()`（第三种 all-clear 收口） |
| **L-3**（Low） | 采纳·**已记录** | `cluster_retire.go:78` 加 N-4c-adjacent record-only 注释（retire rotation guide 硬编码 conf 路径） |
| **L-4**（Low） | 采纳·**已记录** | plan 顶部加 **⚠ UPGRADE NOTE**：>24h reap/gc/disk 区间升级后 `Load()` 拒启（无 CHANGELOG 文件，故入 plan 供发版注记） |
| **N-1** | 接受·定性认同 | — |
| **N-2** | 接受现状 | `xferBucketOrphanedEverywhere` 注释点明与 homeOwnsXferBucket 共同退役点 |
| **N-3** | 采纳·**已修** | 与 L-2 同一次重构：`unverifiedReason` 入口只算一次、三分支共享，消除冷路径重复读盘 |

**修订后复验**：`make lint` 0 issues；触碰面 `cluster_reconcile` 所在 `cmd/tether` + 全部改动包 `-count=1` 复跑绿；
`tests/run-all.sh` 8 门 ALL PASS（含 ledger-crosscheck：#66 pinned、DOC-27 CLOSED、10 open）。M-1/L-1 台账收窄不撤任何翻绿；
L-2/N-3 是收口性代码改动（新增一处 warning + 去重读盘，无行为放宽）。

**判定接受**：外审 Pass（Conditional）的两条前置（M-1 + L-1）已完成，可按流程收尾 commit/push。
