我将先核验最承重的几条 finding 的源码证据，再综合。先并行读取关键实现文件。
`dexec` is confirmed a shell function (docker.sh:59). The critical `timeout N dexec` lines are actually at 96:104 and 96:106 (reviewer 1 cited 98/100 — the substance is right, line numbers slightly off). Let me empirically verify the 127 claim and read the verdict contract + plan landing.
Empirically confirmed: `timeout <shell-function>` returns **127** (`timeout: failed to run command 'myfn': No such file or directory`), and the precedence is ASSERT-FAIL(1) > SETUP-RED(2) > PRODUCT-RED(3) > INCOMPLETE(4). This is the crux BLOCKER. Now I must resolve the reviewer conflict: reviewer 1 says 96 was "never truly run"; reviewer 7 says "measured 轮里 96=PRODUCT-RED". Let me check plan §12 landing.
Let me read the crucial landing records — SB-GC-DRILL-LANDINGS (line 933) and the §0.1 expected-landing table (line 17), to resolve whether 96 truly landed and as what.
Decisive confirmation. Plan §12 records **every drill except 96** with a concrete measured "N pass, 0 assert-fail" (50=68pass, 51=45pass, 52, 94=51pass, 95=34pass, 97=41pass). **96 has only "PRODUCT-RED 预期"** + a note that it needs ≥3600s and "必须单独长超时跑" — i.e. it was NEVER run to completion. This confirms reviewer 1's F1 and **refutes reviewer 7's claim that "measured 轮里 96=PRODUCT-RED (该臂过了)"**. The `timeout dexec`→127 bug is latent precisely because 96 never truly ran.

Now let me verify the second cluster of findings: `secrets_remint_route_only` dead code, 52-B5d gating, events.sh ev_count bug, and 52 keygen.
Now let me verify the dead-code / consumption claims with grep, and read the 52 drill to check B5d gating + keygen ordering + the #63/#54 evidence text.
Confirmed by grep: `secrets_remint_route_only` is **dead code** (only the 52:51 comment references it); `ev_count`/`ev_seen_field` have **zero consumption**; `dp_curl_blackholed` **never wired** (only headnote); 96's `ev_sub_start` observer has **no `ev_seen` consumer**; and `_leader_is_brk1_now` is **undefined in 96/97** (only defined in 51). Now let me read drill 52 to verify the B2 evidence-overstatement, B5d gating, keygen ordering, and #63.
Let me read the B-group asserts (330-395), the keygen C-group placement, and the #63 line.
Confirmed: `_b2_push_gen2` (52:149-156) only swaps account.nk, yet B2b's assert text (52:333) and the not-hit #54 branch (52:342) claim "route leaves + cluster-ca.pem". Also confirmed keygen (ARM GROUP C, 52:396) sits AFTER the #54-poisoned auth gate (52:391-393 `drill_end; exit`), and #63 appears at 52:290. Let me verify the plan ledger, gotchas registry, B5d mandate, and the 51 DR-STEP-LEDGER + lint cross-checks.
Let me verify the 51 DR-STEP-LEDGER gap count and the §8.4 verdict manifest for 95.
Let me verify the 50 DOC-27 signature (reviewer 1 F4) and the 96-D #59 branch (reviewer 6 F8) quickly, then I have enough to synthesize.
我已对最承重的 finding 逐条做了源码级核验(含本地 shell 语义实测 + plan §12 landing 对表)。综合报告如下。

---

# S7–S9 (G-C) 对抗审查综合报告 — 7 drill + harness

> 综合者裁决稿。7 份镜头审查(false-green / mandate / source-truth / harness-libs / verdict-gating[空] / coverage / robustness)综合为一份排序 finding,交主进程逐条裁决。所有 BLOCKER 均经源码 + 实测复核;被证伪的删,reviewer 冲突以源码证据裁。分析中文,标识符/签名英文。

## §0 一句话结论

**不可提交外审。存在一条源码确证的承重 BLOCKER 必须先修**:drill **96 结构上恒落 ASSERT-FAIL(非 PRODUCT-RED)**——`timeout N dexec …`(96:104/106)因 `dexec` 是 sourced shell 函数、`timeout` 直接 execvp 找不到它而恒退 **127**,使旗舰分区臂 D4a/D4b 恒 FAIL,按 verdict 优先级 `ASSERT-FAIL > PRODUCT-RED` **顶掉 96 存在的唯一理由(钉 #57/#58)**;且此 latent bug 从未被抓到,因为**96 是 7 个 drill 里唯一没被真跑到底的**(plan §12 只给它"PRODUCT-RED 预期" + "须 ≥3600s 单独长超时跑",其余 6 个都有 `N pass, 0 assert-fail` 实测数)。此外还有 (B2) #58 的 oracle 无法鉴权读对象仓、(B3) 52 的 PRODUCT-RED/PASS 证据夸大了从未执行的 route+CA 轮换(+死代码)、(B4) 三处 orphan 使 "S1–S9 CLOSURE orphan==0" 结构性为假、(B5) plan 两次明令"B5d 无论如何都跑"却被 gated 掉。这些修完再走外审。

承重工艺本身**扎实**(fault.sh 判别子、restore 门族、R-LIVENESS-NOT-HEALTH、leak oracle、secrets_tunnel_fp、#29 诚实登记——见 §4),问题集中在 96 全臂 + 52 的证据忠实性 + 收官闸的 orphan 记账。

---

## §1 承重 finding(BLOCKER,按严重度排序)

### B1 — 96 `timeout N dexec` 恒退 127 ⇒ D4a/D4b 恒 ASSERT-FAIL,顶掉 #57/#58 的 PRODUCT-RED(且从未被真跑抓到)
- **[drills/96-mid-flight-chaos.sh:104, :106]** → verdict 误分类 + 掩盖真产品缺陷(假红盖真红)。reviewer 1 F1(主),reviewer 7 F4(被本条取代)。
- **证据(源码 + 实测双证)**:`dexec` 是 sourced 函数(`lib/docker.sh:59` `dexec() {`);`timeout` 是 coreutils 二进制,`execvp` 自己的第一个参数,不经 shell、看不到函数。本地实测复现(含 `export -f`):`timeout 3 myfn …` → `timeout: failed to run command 'myfn': No such file or directory`,**rc=127**。verdict 优先级 `ASSERT-FAIL(1) > SETUP-RED(2) > PRODUCT-RED(3)`(`lib/assert.sh:216-219`)。两处:`_d4_brk1_answers`(96:104)恒 127 ⇒ `assert_ok "D4a"`(96:270)FAIL;`_d4_minority_refuses`(96:106)`_o` 收到 "timeout: failed to run command 'dexec'"、`_r=127`(≠0 故不 `return 1`)⇒ grep 不中 `not the leader|…|deadline` ⇒ `assert_ok "D4b"`(96:272)FAIL。arm A 已记 `product_red "#57"`(96:205)/`#58`(96:219),但 `_AS_FAIL≥1` ⇒ 整 drill 落 **ASSERT-FAIL**,且是"为错误原因红"。
- **失败场景**:任何人第一次用足够长超时把 96 真跑到底(plan §12 明记这从未发生),它必落 ASSERT-FAIL,被 §8.4 manifest `96={PRODUCT-RED}` 判为 out-of-set 新 blocker,#57/#58 被误当 harness bug/回归。**§12 对表铁证**:50=68pass、51=45pass、94=51pass、95=34pass、97=41pass 全有实测计数,唯 96 只有"预期"+"必须单独长超时跑"——这个 latent bug 恰好躲过了唯一没跑的那个 drill。
- **修法**:把上界推进被调命令内部——`_d4_brk1_answers() { dexec -u tether brk1 -- timeout 8 tether cluster status …; }`(timeout 在容器内跑,看得见 tether 二进制);`_d4_minority_refuses` 同理把 `timeout 15` 塞进容器内 `env HOME=… tether session create` 前;或改用自带上界的 `poll_until`。**并补 lint**:`tests/lint-drills.sh` 的 `noshc`(:60)只抓 `sh -c "…<fn>…"`,漏 `timeout <fn>` 同族陷阱——加一条 `timeout [0-9]+ (<harness-fn-alternation>)` 静态守卫(见 §3)。

### B2 — 96 `_a2_objects_present` 用无凭据 `nats obj ls` 探对象仓 ⇒ cluster auth_callout 下几乎必 "unreadable" ⇒ #58 悄悄退化成 not_covered(即便修了 B1 仍钉不住)
- **[drills/96-mid-flight-chaos.sh:95]** → oracle 无法鉴权 ⇒ gated NOT-COVERED 掩盖源码确证缺陷。reviewer 1 F2。
- **证据**:`_o=$(dexec brk1 -- sh -c "nats --server nats://127.0.0.1:4222 obj ls …")`——**裸连、无 nkey、以 root**。本仓所有其它 `nats` 客户端调用(`drills/lib/events.sh:41`、80/81)**全部**带 `--nkey /home/sim/.tether/keys/default.nk`。cluster 模式 auth_callout 默认 ON(drill 自己在 96:16 引 `serve.go:203-218`);events.sh:11-12 自陈 member 连 `nats stream ls`(JS-API)都被拒。匿名 `obj ls` ⇒ 空输出 ⇒ `_a2_objects_present` 返 `unreadable`(96:96)⇒ 走 96:227 `not_covered`。
- **失败场景**:#58(非-leader home broker 上 orphan xfer object 永不回收)是源码必现缺陷,却因读对象仓的 oracle 连不上而每跑都 not_covered/INCOMPLETE,永远钉不成 PRODUCT-RED。叠加 B1:96 既盖住 #58 又误分类;即便修了 B1,#58 仍钉不住。
- **修法**:像 events.sh 一样给 `nats` 带有对象仓读权的身份;member nkey 大概率不够(JS-API 被拒),评估用 `$SYS`/operator 凭据,或直接读 nats-server 监控口 8223 的 `/jsz`(drill 已多处免鉴权用它)来数 `OBJ_xfer`,而非 client `obj ls`。**须在真栈实测确认能读到对象后再定 present/gone/unreadable 三态**(此条唯一未在本机实测的假设,主进程在 weilandserver 验一次)。

### B3 — 52 的 #54 PRODUCT-RED / B2b PASS 证据夸大了从未执行的 route+CA 轮换;`secrets_remint_route_only` 是死代码
- **[drills/52-credential-rotation.sh:333, :342, :149-156, :51 + lib/secrets.sh:124-135]** → 不忠实 Mandate ②(暴露的缺陷证据与实际注入不符)+ 承重 harness 函数死代码。reviewer 2 F1 + reviewer 4 H1(一致)。
- **证据(grep 确证)**:`_b2_push_gen2`(52:149-156)**只** `d cp` gen2 的 `account.nk` 到两 broker,不换 route leaf、不换 `cluster-ca.pem`。但 B2b 的 `assert_ok` 描述(52:333,PASS 时照样打印)写「re-mint ONLY the **route leaves** + swap account.nk/**cluster-ca.pem** on BOTH brokers」;#54 的 false-all-clear 分支文本(52:342)写「rotating account.nk **+ the cluster CA**…」——两样都没做。紧邻注释(52:145-148)自陈「Swap **ONLY** the account.nk … Leaving the route alone keeps the mesh up」,与 52:333 **自相矛盾**。plan §1.2-(3) 把 `secrets_remint_route_only` 定为「承重设计订正」,§332 映射 B2 用它,但 `grep` 全仓仅 `52:51` 一条注释引用它——**从未被调用**,`lib/secrets.sh:124-135` 整段是死代码,FG-guard 11(52:51-53)在守一个不存在的调用点。
- **失败场景**:52 正确落 PRODUCT-RED #54(account.nk-skew 分支源码为真,`cluster_reconcile.go:78` + `serve.go:203-218` 已核),verdict 不错;**但审计者/未来修复者读 PRODUCT-RED 与 B2b PASS 会去找一个 CA+route 轮换的复现,而 drill 从未做过**。plan §11 精心设计的 route+CA 路径沦为死代码 = #54 的"CA 轮换"半边在部署层从未被 exercise。违反 Mandate ①/②"如实呈现、绝不夸大"。
- **修法**(二选一,推荐 b):(a) B2b 真调 `secrets_remint_route_only`(推 route-cert/key + cluster-ca.pem,D-group 重建同步还原三者);或 (b) 保留 account.nk-only(更干净,保 mesh 存活),把 52:333/52:342 文本改成「只换 **account.nk (the issuer)**」,删「+ cluster CA」「route leaves」措辞,删死函数 `secrets_remint_route_only` + 空守卫 FG-guard 11,并订正 plan §1.2-(3)/§332。**注**:该函数的防砖化推理本身正确(tunnel fp = `sha256(cert.Raw)`,issuer-independent,`tls.go:91-93` + `clusterwrite.go:173-190`),问题只在于它服务的是一个没人调用的函数。

### B4 — 三处 orphan 使「S1–S9 CLOSURE orphan==0」结构性为假(G-C 独占的收官义务)
- **问题类型**:漏覆盖 + 收官闸假成立 + 台账指向幽灵臂 + 死 setup。reviewer 6 F1/F2/F4 + reviewer 4 M1。
- **(a) 96 臂 C 缺失 ⇒ inventory row 54 `home_reassign_failed` 被幽灵 96-C 认领 + observer 建了无人读**。plan §3.3(line 442)定义臂 C(expose crash → RETURN 窗口 + `ev_sub_start` 断 `home_reassign_failed`,plan 明令捕不到必须 `not_covered` 且理由「实测该 kind 在 crash 路径不发火 + `rehome_events.go:52-53`」、**绝不写"无 reader"**)。实测 96 无任何 `expose`/`home_reassign`/`RETURN` 断言(grep 0)。`ev_sub_start ctl1 … EVCAP`(96:174)+ `ev_ready`(96:175)建了观测器,但**全 drill 无一处 `ev_seen`/`ev_count`**(grep 确认)——死 setup,其唯一预期消费者正是缺失的臂 C。G-C-SWEEP 若如 plan §7 所述把 row 54 判「96-C 的真流量」认领,即用幽灵臂作证。
- **(b) 96 臂 B 缺失 ⇒ DOC-28 孤儿**。plan §3.3(line 441)定义臂 B(run-PTY 杀 broker,GREEN + `TETHER_RUN_LIVENESS_TIMEOUT` 成对臂 + **DOC-28**)。实测 96 无 watchdog/liveness/no-heartbeat 任何痕迹。DOC-28 在 plan §4(line 542)登记指向 96-B,但 `docs/deploy-tier-gotchas.md` **无 DOC-28**(实测最高 G-C 号 #64 + DOC-27),`simcluster-coverage-inventory.md` 亦无 ⇒ 登记在 plan、无 owner 臂、无 ledger 条目。
- **(c) 52 keygen(inventory row 179)被 #54 毒化的 auth gate 锁死、静默 orphan**。keygen(C1-C3,52:396-404)是**纯离线**动作(`tether cluster keygen --out` + 独立 `nk -pubout` 交叉核对),但被排在 D-group 的 `_d_cluster_authworks` gate(52:391)**之后**;该 gate 失败即 `not_covered "52 D-group" + drill_end; exit`(52:392-393)。§12 landing 明记「B4-B7/D-group **两处 NOT-COVERED**（轮换在运行态不可完成 = #54 后果）」⇒ D-group gate 在实测里确实失败、于 393 exit ⇒ **keygen(52:396)从未运行**,row 179 既无 assert 又无 not_covered = 静默 orphan。
- **失败场景**:§7 item 7 收官闸的验收 = 「orphan 行数 == 0」;row 54(幽灵认领)、DOC-28(孤儿号)、row 179(静默跳过)三处都不满足「归入某 drill 或显式 NOT-COVERED + source 理由」⇒ S1–S9 CLOSURE 的核心断言对这三行为假,且 G-C 是最后一组、独占该义务。
- **修法**(每处二选一,主进程逐行裁):补回臂(96-B/96-C 或至少 knob 成对臂 + expose-crash 观测器消费者;keygen 移到 B-group 破坏**之前**紧跟 SETUP)**或**显式 `not_covered` + source-cited 理由 + 订正 inventory row 54/123 owner、gotchas.md 补 DOC-28、删 96:174-175 死 observer、plan §7 删「96-C 的真流量」幽灵引用。keygen 无论如何应前移(它 auth-无关,不该做 #54 的连带牺牲品)。

### B5 — 52 B5d(#55 唯一断言腿)被 gated 掉、永不运行,与 plan 两次明令「B5d 无论如何都跑」冲突
- **[drills/52-credential-rotation.sh:375-378]** → 漏覆盖 spine + Mandate ⑤(gated NOT-COVERED 理由 over-subsume)。reviewer 2 F2。
- **证据**:实现把 B4/B5/B5d/B6/B7 **整块**折进单条 `not_covered "52 B4-B7 …"`(52:376),gate 在 `B2_ROTATION_LIVE=0`——而 #54 必现故该 flag 恒 0,**B5d 永不运行**。plan **两处**明令无条件跑:§4 ledger #55(line 503)「**B5d 无论如何都跑**」+ §10 SB-52-B5(line 843)「**无论如何 B5d 都跑**」;§2.3(line 336)称 B5d 是「#55 的唯一断言腿」,基座 `_b5d_recover`(52:160)+ FG-guard 3(52:226-227,注释「B5d's control baseline」)都建好了、唯一消费者 B5d 被 gated ⇒ 成孤儿。`lib/assert.sh:174-176` 自陈「a topic SPINE(roadmap locked cell)must be a hard assert or product-RED, NEVER not_covered」。
- **失败场景**:#55(跨 route account.nk 滚动轮换的 ~50% auth 拒绝窗口)在部署层零覆盖;not_covered 理由「need a staged out-of-band re-mint; owed to staging」把一个 sim 有能力做的**有界 broker-down 窗口探针**(`stop tether-broker brk1` = runbook step 4 必经态,drill 自己在 A8f/52:313 与 D-group/52:385 反复重启 broker)一并推给 staging = over-subsume。
- **修法**(需主进程实测裁):把 B5d 从 B4-B7 块**拆出无条件执行**(它只依赖 FG-guard 3 已建的对照基线);把 not_covered 收窄为仅 B4/B6/B7,理由改源码级「完整 account 轮换需重签全部 member 凭据,非 sim 可在不 re-provision 全队下完成」。**若 Stage-B 实测证明 B5d 的前提(brk1 conf issuer=NEW)本身因 #54 的"reconcile 不 re-render"而无法在 running cluster 构造**,则反过来订正 plan §4/§10 的"无论如何都跑"措辞 + 把 not_covered 理由做成源码级——两者必居其一,当前 plan 与 impl 直接矛盾,不能带进外审。

### B6 — 51 DR-STEP-LEDGER(一等产出)欠计 #52:代码只能产出 undoc=1,§12/plan 却记 undoc=2
- **[drills/51-full-dr.sh:292, :319, :337]** → Mandate ④ 一等产出不忠实(auditable 量化与代码/自身认定矛盾)。reviewer 2 F3 + reviewer 6 F6(同一根因,两角度)。
- **证据(grep 确证)**:`_dr_gap`(累加 `DR_UNDOCUMENTED`)全 drill **只调一次**(51:292,`#51` seam)。#52 在 51:319 已 `product_red`(证明它是一条必需、未文档化的 gap:restore 不渲 nats.conf),但**从未作为 `_dr_gap` 计入**。预期 RED 路径(#52 挡住 broker)走 DR-COMPLETION gate,在 51:337 打印 `DR-STEP-LEDGER: … undocumented=1 gaps=[#51]` 后 `drill_end; exit`,永不到达假想的第二 gap。而 §12(line 935)记「documented=4/required=5/**undoc=2**」、plan §2.2 headnote(line 298)示例 `gaps=[#51-seam,#52-natsconf]`(两条)。⇒ 任何执行路径都只能 undoc=1,记录却宣称 2。
- **失败场景**:DR-STEP-LEDGER 被 plan 定为「一等产出 / 本组最好的工艺」,量化 runbook §5.2 有多不完备。审计者读 ledger 得 1 个未文档步骤,读 drill body 得 2 个(#51 seam + 有 product_red 背书的 #52),且 §12 记录方向是**低估**——恰恰弱化了 ledger 存在的意义。非假绿(51 仍 PRODUCT-RED),但招牌工艺失真。
- **修法**(二选一):#52 被 product_red 钉住时即计一条 gap(51:319 后加 `DR_UNDOCUMENTED=$((DR_UNDOCUMENTED+1)); DR_GAPS="$DR_GAPS #52"`,不新增 `_dr_step`——该步未真执行);**或**订正 §12/inventory 的 `undoc=2 → 1`、`gaps=[#51]`,如实反映现码只做一次 gap-clear。别让记录与代码继续背离。

---

## §2 次要 finding(应修,不阻塞)

1. **[events.sh:51-53] `ev_count` 的 `grep -c … || echo 0` 双零 bug**(reviewer 4 H2):`grep -c` 零匹配时既打印 `0` 又 exit 1,`|| echo 0` 追加第二个 `0` ⇒ 输出 `"0\n0"`(本机实测 `[ "0\n0" -gt 0 ]` 报 integer expression expected)。头注(:48-50)恰恰力荐「用严格递增 count 而非裸 presence」的增量/缺席用法会踩它。当前零消费(94/95 只用 `ev_seen`),不炸,但是交给下一读者的地雷。修:去掉 `|| echo 0`(`grep -c` 本就总打印数字),同理复核 `ev_seen_field`。
2. **[plan §8.4 line 701] 95 的 expected-verdict manifest 写 `{GREEN, PRODUCT-RED}`,实测 §12 落 INCOMPLETE**(reviewer 2 F6 + reviewer 7 F7):95-D DELETING 在 N=2 raft/JS 同生共死时诚实 `not_covered`(95:232/251,`broker.go:1130-1136` 机理忠实)→ INCOMPLETE,不在允许集内 ⇒ 基线会周期性把 95 的诚实 INCOMPLETE 误判为 out-of-set 新 blocker。纯 manifest 笔误,修 `95 ∈ {GREEN, INCOMPLETE}`,同步 95 drill 头注「Expected landing」。不动实现。
3. **[50:193] DOC-27 只凭 `rc != 0` 触发,未做签名守卫**(reviewer 1 F4):`product_red "DOC-27"` 前不 grep `prepare bundle parent|store_error|no such file`,只把 `tail -1` 塞进消息。若 `cluster backup --out /var/backups/…` 因别的原因(leader 漂移/瞬态/权限)非 0 退出会误钉 DOC-27。§12 实测该轮为对的原因红,但守卫松。修:product_red 前加签名 grep,不匹配则 `_as_fail` UNJUDGEABLE。
4. **[52:290] `product_red "#63"` 号漂移**(reviewer 6 F5):plan §4 ratify 到 #62(line 877 列表止于 #62),gotchas.md 无 #63 heading(#64 有、是 Stage-A 未预见的新号),违反本组自定「drill 内 `#N` 字串与 §4 表零漂移」(§4 line 494 M11 血泪)。`lint-drills.sh` 无 ledger 交叉检查故静态闸抓不到。修:在 gotchas.md/plan §4 补 #63(rotate-tunnel-cert 在线轮换后车队不 re-pin 候选)或改用既有号。
5. **[96:141 / 97:108] `_ensure_leader_brk1` 调未定义的 `_leader_is_brk1_now`**(reviewer 7 F3,grep 确证):该函数只在 51:100 定义;96/97 里命中 command-not-found(127)被 `2>/dev/null` 吞掉,恒走 `||` 后 jq 兜底(功能等价)。死首子句 + 与 51 不一致,日后若有人"简化"删 jq 兜底会塌。次生 TOCTOU:check-then-`transfer-leader` 之间 leadership 漂回会 exit 70 → SETUP-RED。修:改用已定义的 `_leader_is_brk1`,transfer 步吞 exit 70(`grep -qiE 'already the leader' && return 0`)。
6. **[96:273 D4c] 96-D 无 `#59` 的 signature-guarded 路径**(reviewer 6 F8):plan §3.3(line 443)要求 96-D 三分支总函数(POSITIVE→GREEN / crash-loop→`product_red "#59"` / INCONCLUSIVE→not_covered,「绝不静默 POSITIVE」);实测 D4c 是 `_d4_brk1_stable` 硬 assert,无 #59 路径 ⇒ 少数派 broker 真 crash-loop 会落 ASSERT-FAIL(误判 harness/回归)而非 #59 首证。SB-96-B-DISCRIM 把 #59 降为纯观测有据,但须在头注/ledger 明记「#59 无 drill pin 路径、真复现落 ASSERT-FAIL 需人工分诊」。修 B1 重做 D 臂时一并处理。
7. **[97:88 `_xfer_terminal`] 未 scoped 到本 cycle**(reviewer 1 F5):`grep -qE 'complete|failed'` 数全部 transfer 历史;`SOAK_CYCLES=48` 深跑时 cycle 8 起会命中 cycle 4 遗留的 `complete` 行 ⇒ type-3 非空性守卫 vacuous。97 spine 是 leak oracle 不依赖此,故非承重,但违反"每 cycle 证明注入真发生"。修:给每 cycle 唯一 dst 文件名过滤,或记基线 terminal 行数断"新增"。
8. **[96:96-97] D2/D3 探针缺 R-BOUNDED-PROBE 要求的显式 timeout**(reviewer 7 F5):`_d2_new_leader`/`_d3_survivor_write` 在 armed 分区窗口内无 timeout,而同臂 D4a/D4b 有——不对称暴露漏包。`cmd_drill` trap 不捕 EXIT、`run_one` 无 per-drill timeout ⇒ 真挂住 = suite wedge + SIMFAULT 残留。修 B1 时给 D2/D3 内的 tether 调用一并包 `timeout N`(容器内)。
9. **[51:380-388 arm I] 裸 `$SIM grow`(无 retry)把 VOTER-timeout flake 变假红**(reviewer 7 F2):DR-completion 通过分支里 arm I 的 re-grow 绕过 `grow_to_2/3` 的 retry 封装,else 分支 `_as_fail`(ASSERT-FAIL)会盖过 #51 的 PRODUCT-RED;VOTER-timeout 被 runner 刻意排除出 FLAKE_SIG(不 auto-retry)。修:else 分支里匹配 VOTER-timeout 签名的降为 `not_covered`,只有真无法归类才 `_as_fail`。
10. **[50:359-375] 50-K 无兄弟 drill 都有的 `reset-failed` 安全网**(reviewer 7 F6):#64 crash-loop 若快于 measured ~16-18s/loop 撞 `StartLimitBurst=5/10s` → broker 进 failed 不自愈 → K1/K2 ASSERT-FAIL 盖过预期 PRODUCT-RED。低概率(空载 10+ 轮未触发),但唯独 50-K 无安全网。修:K 轮 poll 补 `systemctl reset-failed tether-broker`,`_cleanup` 同。
11. **[94:113-114/:133/:165-167] `_A0_PIDS` trap 回收是死代码**(reviewer 7 F1,本机实测):`_a0_start` 经 `assert_ok` → `_as_capture` 在命令替换子 shell 里跑,`_A0_PIDS` 赋值对父 shell 不可见 ⇒ trap 遍历空集。非假绿,但 `SIM_KEEP=1` 下泄漏宿主 docker exec client。修:pid 写文件由 trap 读,或把 side-effect 与断言分开。
12. **[94:208 A2d] 裸 `ev_seen agent_registered` 只证 row-30 可读性、不构成 reconciliation 证据**(reviewer 1 F6):setup 期 capture 内几乎必有该 kind(events.sh:48-50 自陈裸 presence 会为错误原因过);真正的 re-register 主 oracle 是 B2-timeout(带 journal cursor 门,做对了),spine 不塌。修:改 `ev_count` 前后取差,或删 A2d 的"reconciliation"暗示只留可读性。
13. **[harness 死代码/覆盖高估](reviewer 4 M1,grep 确证)**:`dp_curl_blackholed`(plan §1.4 标"新,承重" R-DATAPLANE)、`fault_freeze_on/off`(plan §1.2-(5) 第三种语义)、`node_ip`、`ev_seen_field` 全**零消费**;96-D 整臂无一处真 curl 数据面(只用 `fault_assert_blackholed` TCP 挂起)。不是运行期假绿,但让 README/plan 显得"数据面分区 oracle、freeze 语义"已交付、实则没接线。修:每个函数决定接线或删 + 改 plan 的"承重"措辞。
14. **[fault.sh:100-108] `fault_cleanup_all` 只 SIGCONT `pgrep -x tether`,冻结的 nats-server 不解冻**(reviewer 4 L1):freeze 当前零消费故无污染;一旦有臂冻 nats-server 靠 trap 解冻会留 SIGSTOP 的 nats。随 #13 删 freeze 时一并简化,或 `pkill -CONT -x nats-server` 并列。
15. **源码引用行号漂移(全部 trivial,不触断言,reviewer 3 F1/F2/F3)**:94:294/96:295 的 `schema/audit.go:52` 应为 `:51`(AuditPort kind);52:20 `cluster_offline.go:480`→`:481`、52:429 `cluster_retire.go:47`→`:48`;95:219 注释把 `broker.go:1130-1136` 说成 sessions、实为 processes(兄弟表,机理相同)。断言判据都不依赖行号,改注释即可。

---

## §3 建议新增测试条目

1. **lint:`timeout <harness-fn>` 静态守卫**——扩 `tests/lint-drills.sh` 的 `noshc` 规则族,把 `timeout [0-9]+ (dexec|_bt|<全部 sourced 谓词名交替)` 列为 HARD 违规(B1 正是这个结构缺口:noshc 只抓 `sh -c`,漏了 `timeout <fn>`)。合成违规注入 96:104 验证被抓、既有 drill 无回归。
2. **lint:ledger 零漂移交叉检查**——扫每个 BATCH drill 的 `product_red "#N"`/`assert_bug … "#N"` 字串,断 N ∈ {gotchas.md 已登记号 ∪ plan §4 ratified 号},否则 HARD 失败(B6/#63 漂移 static 闸现在抓不到;这正是 §4 line 494 的 M11 血泪要防的)。
3. **96 补臂或显式弃权测试条目**:臂 B(`run --nats-url` 杀 home broker → 出口(b)优雅 `no heartbeat` + DOC-28 + `TETHER_RUN_LIVENESS_TIMEOUT=180s` 成对臂)、臂 C(expose crash → RETURN + `poll_until` 恢复 + `.moved==false` + `ev_seen home_reassign_failed`,捕不到时 not_covered 理由写 `rehome_events.go:52-53` 机理)。补齐则消费 96:174 的 observer;不补则删 observer + not_covered。
4. **52 B5d 独立条目**:broker-down 窗口成对采样(`stop tether-broker brk1` → 经 brk1 `assert_refuses "auth_callout rejected"` ∧ 同窗口经 brk2 `assert_ok` → `_b5d_recover`),无条件执行(不依赖 rotation-completion)。
5. **51 补 A0d 对照 / 96 补 A0d**:断 A0b 那次成功传输的 object 已回收(正常路径 present→gone),给 #58 的 present 判据排他力(reviewer 1 F3:现 A0b/A0c 只断 start+complete 成对,未断控制传输 object 事后回收,#58 present 判据未 scoped 到被杀传输)。

---

## §4 确证做对的承重点(点名,供取信、避免过度改动)

- **`drills/lib/fault.sh` = Mandate ③ 范本**:iptables 头注明确定位 cable-pull instrument;`fault_assert_blackholed` 的 **124(挂起)vs 1(refused)vs 28(curl blackhole)判别子** fail-closed 防住有人把分区臂偷换回 `docker network disconnect`(那会把静默分区变即时 outage);`fault_partition_on` 单 SIMFAULT 链 INPUT+OUTPUT 双向 `--dport`+`--sport` DROP 对称隔离——SB-FAULT 已在 weilandserver 实测(plan §12 line 959)。**armed 窗口探针骨架正确**。
- **restore 门族(50/51)源码逐字对**(reviewer 3 全过):门序 `restore.go:234`(confirm-node-id)早于 `:238`(tunnel-fp);50-I4 传 bundle 自身 self_id(brk1)在 brk2 上跑逼 `:242-244` "tunnel-cert fingerprint mismatch … refusing to adopt a foreign cluster"——本 drill 唯一致命设计点处理正确;L4d 实测 restore re-home 不 bump epoch 并主动 DROP 该断言(`restore.go:355` WHERE 排除已 pin 在 self 的端口)= 漂亮的源码对账,plan 预测才是错的。
- **R-LIVENESS-NOT-HEALTH** 贯穿 50/51/94/95(`_broker_ready` = 答出可解析 JSON 而非拿 `cluster status` 退出码当存活探针)——直接堵掉"对着 DEGRADED 健康 broker poll 到超时伪造产品故障"(§12 SB-50-LANDING 实测血泪,G-B drill 91 教训)。
- **`secrets_tunnel_fp`(secrets.sh:153-156)指纹算法正确**:`openssl x509 -outform DER | sha256sum` == 产品 `sha256(cert.Raw)`(`tls.go:91-93`);52-A3 `_a3_fp_agrees` 剥前缀等值自检 ⇒ 口径分歧会被响亮抓到、不会静默假绿。
- **#29 `--on-broker brk1` home-pin 处理 = Mandate 范本**(50/51/52/95):不松 oracle 凑绿,而把一个已登记(属 drill 71)的缺陷如实登记为 blast-radius sighting(附 live-measured ~50% 抛硬币证据),避免它在别人主题上制造假红/flake。
- **95-T1/T2 clean-vs-unclean journal 措辞互为判别子** + 阳性对照先于绝对断言 + `SuccessExitStatus=0` 前提被 T0-drift 显式断言——#23 行为学证明构造正确;DELETING not_covered 理由源码级(`broker.go:1130-1136` fork 机理)。
- **97 leak oracle 时序设计**:被测进程(brk1/agt2)永不做 victim + 每轮 PID 世代守卫(变则 not_covered)+ 采样在 `poll_until … false`(先等终态)+ RSS 只用斜率+相对天花板——结构性排除 GC 锯齿假 RED;goroutine 永久 NOT-COVERED 理由源码为真(`grep -rE 'pprof|expvar' cmd/ internal/` 实测 0 命中,Threads≠goroutine)。
- **`vault_init` rm-then-assert-empty**(vault.sh)把 fail-closed 放 postcondition;51 的 sha256 存活 oracle 非空守卫(51:191-193);`secrets_mint_gen` 拒 gen1 守住设施-owner 规则;`grow_to_2/3` 驱动真 `tether cluster add` 不手工组集群 = 忠实 Mandate ③。
- **events.sh 核心前提源码为真**(reviewer 3/4 双证):`permissions.go:147/:36` Sub allow 含 `tether.v2.sys.events`,member ctl 可 core-sub 每个 kind;推翻了 G-A/G-B 的"admin 无 events reader"碳-out。
- **收官闸最重一条已落实**:`tests/lint-drills.sh:27` 的 `BATCH` 已含全部 7 个 G-C drill(plan §7 判"最易漏、后果最重")。
- **Mandate ③ 擅改环境规避缺陷 = 零命中**(reviewer 2):所有 chown/chmod 都是把 operator 该放的密钥/bundle 以产品要求属主/权限就位(`SecretsPreflight` 硬拒 0077);`/etc/tether` root-owned 在 51 fresh-box 门如实断言、未 chown 规避 #51/#52。

---

## §5 reviewer 冲突(源码证据裁)

1. **reviewer 1 F1(96 never ran,timeout-dexec 127 latent)vs reviewer 7 F4(「measured 轮里 96=PRODUCT-RED,该臂过了」)** — **裁:reviewer 1 正确,reviewer 7 F4 的前提被证伪**。plan §12 SB-GC-DRILL-LANDINGS 铁证:96 是 7 个 drill 里**唯一没有 `N pass, 0 assert-fail` 实测计数**的,§12 明记它需 ≥3600s、"必须单独长超时跑"、只给"PRODUCT-RED **预期**"。50/51/94/95/97 全有具体 pass 数。所以"measured 96=PRODUCT-RED"无据——96 从未真跑到底,D4a/D4b 从未被 exercise。本机又实测 `timeout <shell-function>` 恒 127。⇒ B1 成立为 top BLOCKER;reviewer 7 F4 的具体竞态(timeout 先于 tether 打印拒绝串触发)**moot 且被 B1 取代**:timeout 根本没 exec 到 tether(在 exec `dexec` 就 127 了),不是赛跑而是恒败。
2. **reviewer 3(「承重断言零 WRONG,只有 trivial 行号漂移」)vs reviewer 1/2/4/6/7 的一批 BLOCKER** — **无冲突,不同镜头**。reviewer 3 校验的是**已存在的签名/门序/字段名**是否与产品输出逐字一致(结论:是,零 WRONG,承重正确点见 §4);B1-B6 是**结构缺陷/覆盖缺口/harness-bug/证据夸大**,与"已写出的签名是否对源码"正交。两者都成立:签名对,但(B1)被套在 timeout 里跑不到、(B2)oracle 连不上、(B3)证据描述夸大注入、(B4)整臂缺失。
3. **reviewer 5(verdict-and-gating 镜头)无输出** — **该维度是覆盖空洞**,主进程需自查或补跑。部分由他镜头兜住:precedence 语义(reviewer 1 F1 引 assert.sh:216-219)、manifest gating(reviewer 2 F6 / reviewer 7 F7,§8.4 vs §12 的 95/96 出集问题)。但"每条 not_covered/product_red 是否落对 counter、drill_end 分支是否穷尽"这类 gating 正确性建议主进程针对 96(改后)+ 52(B4/D-group gate 链)复核一遍。

---

**核验方式说明**:B1 经本地 shell 实测(`timeout <fn>` → 127)+ 源码(docker.sh:59 / assert.sh:216-219)+ plan §12 landing 对表三证;B2/B3/B4/B5/B6 及 §2 的死代码/consumption/行号类均经 `grep` 全仓确证 + 读实现 + 对照 plan §3.3/§4/§7/§8.4/§12。唯一未在本机实测、需主进程在 weilandserver 验一次的假设:B2 中"匿名 `nats obj ls` 在 cluster auth_callout 下是否必然读不到对象仓"(源码强烈支持,但建议真栈确认三态)。

**相关绝对路径**:
- `/home/weiland/projects/dist_experiment_control/test/simcluster/drills/96-mid-flight-chaos.sh`(:95,:104,:106,:174,:270-273)
- `/home/weiland/projects/dist_experiment_control/test/simcluster/drills/52-credential-rotation.sh`(:149-156,:290,:333,:342,:376,:391-396)
- `/home/weiland/projects/dist_experiment_control/test/simcluster/drills/51-full-dr.sh`(:292,:319,:337)
- `/home/weiland/projects/dist_experiment_control/test/simcluster/lib/secrets.sh`(:124-135)、`drills/lib/events.sh`(:51-53)、`lib/docker.sh`(:59)、`lib/assert.sh`(:216-219)、`tests/lint-drills.sh`(:60)
- `/home/weiland/projects/dist_experiment_control/docs/reviews/s7-s9-plan.md`(§3.3 L441-443、§4 L503/542、§7 L644-647、§8.4 L701、§12 L933-948)
- `/home/weiland/projects/dist_experiment_control/docs/deploy-tier-gotchas.md`(无 #63/DOC-28)、`docs/reviews/simcluster-coverage-inventory.md`(row 54/78/123/179 + sweep)
---

## §6 主进程采纳裁决（2026-07-17）

逐条裁决，**全部采纳**（无驳回——每条 finding 都经源码/实测确证）。

**BLOCKER（6/6 采纳）**：
- **B1（96 `timeout N dexec`=127 恒 ASSERT-FAIL）** ✅ 采纳。改 `dexec … timeout N tether …`（timeout 推进容器内、看得见二进制）。**并加 lint 规则 `timeout-fn`**（mutation 验证：合成违规被抓、既有 drill 无回归）——这是 R-NOSHC 的结构性同族缺口，钉死。
- **B2（96 `nats obj ls` 无鉴权）** ✅ 采纳。改用 loopback 监控口 `8223 /jsz?streams=1` 数 OBJ_xfer（免鉴权，drill 已多处用于 cluster_size）。三态判定需真栈验证（reviewer 已标注）。
- **B3（52 B2b 证据夸大 + 死代码）** ✅ 采纳方案 (b)。B2b/#54 文本改为「只换 account.nk（issuer）」，删死函数 `secrets_remint_route_only` + 空 FG-guard 11，订正 plan §1.2-(3)。account.nk-skew 的 #54 源码为真、verdict 不变。
- **B4（三处 orphan）** ✅ 采纳。(a)/(b) 删 96 死 observer + 显式 `not_covered` 记 96 臂 B/C 的 NOT-COVERED（source-cited）+ DOC-28 入台账；(c) keygen 前移到 B-group 破坏之前（auth-无关的离线动作，不做 #54 连带牺牲品）。
- **B5（52 B5d 被 gated 与 plan 矛盾）** ✅ 采纳第二出口。#54 使 issuer 换不到 NEW ⇒ B5d 的 skew 前提运行态无法构造 ⇒ #55 是 #54 下游后果、无独立可复现窗口。收窄 not_covered 为源码级理由，订正 plan §4/§10 的「无论如何都跑」措辞（消除 plan/impl 矛盾）。
- **B6（51 DR-LEDGER 欠计 #52）** ✅ 采纳方案一。#52 被 product_red 钉住时即 `DR_UNDOCUMENTED+1`（不加 _dr_step，该步未真执行）⇒ ledger 如实 undoc=2 gaps=[#51,#52]。

**次要（采纳 1/2/3/5/7/9/10/15，其余登记）**：
- 1 ev_count 双零 bug ✅ · 2 plan §8.4 `95∈{GREEN,INCOMPLETE}` ✅ · 3 DOC-27 签名守卫 ✅ · 5 `_leader_is_brk1_now` 未定义→内联+吞 exit-70 ✅ · 7 97 `_xfer_terminal` scoped 到 cycle ✅ · 9 51 arm I VOTER-timeout→not_covered ✅ · 10 50-K reset-failed 安全网 ✅ · 15 行号漂移注释 ✅。
- 次要 4（#63 号）✅ 补 #63 + DOC-28 入台账消漂移 · 6（96-D #59 无 pin 路径）已在头注记「真复现落 ASSERT-FAIL 需人工分诊」，SB-96-B-DISCRIM 有据 · 8（D2/D3 timeout）随 B1 一并（D2/D3 的 tether 调用容器内已带上界）· 11（`_A0_PIDS` trap 死代码）低优先，SIM_KEEP 才泄漏 · 13/14（harness 死代码 dp_curl_blackholed/fault_freeze）保留（未来臂消费）· 12（A2d 可读性）保留。

**§4 确证做对的承重点全部保留**（fault.sh 判别子、restore 门族、R-LIVENESS-NOT-HEALTH、leak oracle、secrets_tunnel_fp、#29 诚实登记、events.sh 源码前提、BATCH 已含 7 drill）——不过度改动。

**§5 冲突裁决采纳**：reviewer 1（96 从未真跑、B1 latent）胜 reviewer 7 F4（源码+实测+§12 三证）；reviewer 3（签名零 WRONG）与 BLOCKER 正交、都成立；reviewer 5（verdict 维度空洞）由主进程对 96/52 gate 链自查覆盖。

**采纳后硬闸**：sh+dash 全清 · lint 16 drill 0 违规（含新 timeout-fn）· verdict 契约 ALL PASS · 零产品 Go diff · command tree golden 零 diff。

**受影响 drill 真栈复跑（修复后落地已确证）**：B1 修复后 **96** 真栈复跑 = **PRODUCT-RED（32 pass）**，D4a/D4b 不再 127、旗舰分区臂 GREEN，且 **#65（分区少数派 stale-leader 写持久）正是 B1 修复后 96 首次真跑经 D6b majority-visibility 判别子发现的新 raft-safety finding**（若 B1 未修，96 永远顶在 ASSERT-FAIL、#65 无从暴露）；**52** = PRODUCT-RED（#54/#56，33 pass）；**51** = PRODUCT-RED（#51，45 pass、DR-completion NOT-COVERED）；**50** = PRODUCT-RED（#50/#64，68 pass）。7 drill 全部 0 ASSERT-FAIL 干净落地：50/51/52/96 = PRODUCT-RED · 94/97 = GREEN · 95 = INCOMPLETE。

> **⚠ 本 §6 是 Stage-C 内审快照（2026-07-17，其「B1」=timeout-fn latent bug、pass 数=Stage-B 原始落地）。随后的独立外审（`s7-s9-external-review.md`）判 **Fail** 并抓出 10 项阻断（oracle 有效性、安全、恢复代际、覆盖、证据矛盾），本节「全部 0 ASSERT-FAIL 干净落地」由此被推翻。外审整改后的**权威**最终落地见 `s7-s9-external-review.md`「主进程回复与整改」的复跑落地表（51=53 · 52=37 · 96=32 · 97=41，全部 0 assert-fail；#58 7 轮一致 RED；#65 非确定性候选）。**



**收尾台账零漂移（外审门前最终核）**：drill 承重 `product_red/assert_bug/not_covered "#N"` 字串 ↔ `deploy-tier-gotchas.md` heading **零漂移（缺 0、无重复号）**；新登记 #51/#52/#53/#55 补齐，inventory「台账新增」清单同步。#51=LIVE-CONFIRMED、#52/#53=SOURCE-CONFIRMED（live 直证分支本轮经 DR-completion gate 折叠，owed to a completing DR）、#55=候选（#54 下游、in-sim 不可构造）。
