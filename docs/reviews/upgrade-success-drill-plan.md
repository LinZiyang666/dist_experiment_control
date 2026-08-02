# upgrade-success drill · plan（定稿）

> 2026-08-01。阶段 A：3 drafter（design/rollback/guards）→ 3 critic（mandate/falsegreen/feasible）→ 1 synth
> 对抗性 workflow 综合，主进程定稿。upgrade-safety 外审遗留三件中的第 2 件（dedicated success drill 立项）。
> **定稿批注**：综合稿全部裁决采纳，无修订。实施顺序：先做 §9 的两个 spike（agt1b 双 unit 供给、
> nf_tables `--syn` 匹配），任一失败按 plan 既定退路降级；随后按 §8 清单实现。§5 的新 gotcha 登记
> 与 architecture §21.3 GAP 列表补句按"文档先行"原则在实现前落。drill 真跑依赖宿主 DNS fidelity
> 修复（upgrade-safety 遗留件 ①，resolved drop-in 待用户执行）。

已核实关键争议事实（`node.go:386` `upgradeWaitBudget=150s`；`node.go:450-458` wait 循环对 OFFLINE/查询失败一律 `continue`，到点必打 `likely ROLLED BACK`——G2 的 FIN 穿透不破坏 ctl sig；`upgrade.go:85/106/128` 三条 `upgrade_in_progress` 文案互异属实）。以下为候选 plan。

---

# 33-node-upgrade-success.sh 候选 plan（综合稿）

## 0. 范围与刻意不做

**范围**：单台 node upgrade 在真实部署栈（真 systemd、真 https+CA、真网络故障、真共享二进制双实例）上的四条腿：A 真成功、B 真 watchdog 回退、C 升级域拒绝、C2 域释放后 sibling 成功。

**刻意不做**（写头注，不进 not_covered——均为"机制已被 hermetic 逐字钉死、deploy 层无新增事实"或用户已裁定）：
- D 首跳语义（用户裁定）；
- 同 tag 重推 UNCONFIRMED（cmd 层逐字已钉，deploy 徒增一轮 re-exec）；
- `--all` 金丝雀成功扇出（fleet 语义按 M4 属 31，见 §6 的 31 收窄）；
- rollback_failed 终态（需 harness 篡改 `.prev`，对产品状态文件下手越线；`upgrade_state_test.go` 表驱动已覆盖）；
- 断电耐久（`upgrade_durability_test.go` 注入点专属，容器演不了真掉电）；
- boot-budget 崩溃循环回退腿（`agent_boot_shim_test.go` 真实构建二进制黑盒已钉；B 臂 Detail sig 钉明只认 watchdog 腿，**不冒充**覆盖此腿）；
- E 探针臂实跑（裁决见 §5：不进 33）。

## 1. 臂清单与编排

拓扑：`up --brokers 1 --agents 1 --ctl 1`（brk1+agt1+ctl）。agt1 容器内供给**第二个 systemd unit `agt1b`**：独立 HOME（`/home/sim/.tether-agt1b`）、独立 nkey pin-bind + 自己的 agent.yaml url_allow（C2 用），`ExecStart` 与 agt1 **同一路径** `~/.local/bin/tether`。die-gate：两 unit 的 ExecStart 二进制路径逐字相同 ∧ 同 inode（升级域前提非空）。**agt1b 的 join 必须在布防之前**（bind 需新建连接）。

**顺序：setup → B（回退，C 嵌套其 pending 窗口）→ 愈合 → A（成功）→ C2（域释放）**。理由：A 的 re-exec 是不可逆终态（dst=vNEXT），排最后才能让 B 用同一 tarball、且 B 回退后恢复基线正好给 A 干净起点；C 只能落在真实 pending 窗口里，B 的 120s 天然提供；C2 需域已释放且 sibling 仍跑旧映像，只能压轴。

产物：**单 tarball 复用**（`vendor/tether-next`，stage.sh 已铸造，S0-artifact 零改动）。B 的输入是完全健康的真产物，失败由环境注入（§2）。版本捕获不写死字面量：`OLD_RELEASE` 取升级前 `node ls --json`；`NEXT_RELEASE` host 侧 `vendor/tether-next version` 按冻结格式解析；`NEXTBIN_SHA=sha256(二进制)` **≠** `ART_SHA=sha256(tarball)`（喂 `/proc/exe` oracle 必须用前者）。开场 die-gate：`OLD_RELEASE != NEXT_RELEASE`（同版会让 A/B 全塌成 UNCONFIRMED）。

## 2. 坏产物裁决（B 臂输入）

**采纳候选三：真 tether-next 产物 + SYN-only 网络断供**（agt1 netns 内 SIMFAULT 链新增规则：出向 TCP `--syn --dport 4222` DROP；established 存活、新连接黑洞）。理由：broker-ops §8.7 白纸黑字的真实场景（NAT 弱网假回退）；fault.sh 头部已冻结 iptables=拔线级仪器的 Mandate 先例；watchdog 在首次拨号前武装（agent.go:672）保证收敛；回退后撤除 fault 即纯产品自愈 re-register，零运维干预——三候选中唯一让终态全程产品自驱的构造。

**驳回候选二（改坏 agent.yaml broker 地址）**：sim 默认 unit 是 flagful（`--nats-url ${NATS_URL}`，argv 跨 `syscall.Exec` 保留，agentyaml.sh:5 明言 flag-shadow）——yaml 地址**结构性不在 register 路径上**，注错无效；即便换 flagless 形态，回退后旧二进制读同一份坏 yaml 同样连不上，"以旧版本重新 ONLINE"必须靠 harness 修配置才走通，正是 Mandate 禁止的"替 tether 弥补"。

**驳回候选一（假 version 脚本）作臂输入**：无 boot shim ⇒ 预算永不消耗、watchdog 随旧映像消亡、marker 永 pending——它驱动不了回退路径，只测出"另一种行为"（永久变砖）。其诚实去向见 §5/§6。

## 3. 每臂 oracle + 假绿防护表

**⚠ 全局事实（G2，FIN 穿透）**：exec 关闭旧 socket 时 FIN 不是 SYN、会穿出去 ⇒ **staged 后窗口内 agt1 即转 OFFLINE，窗口内不得断言 agt1 ONLINE**；C 臂用 agt1b（established 连接不受影响）。ctl wait 循环对 OFFLINE 一律 continue（node.go:457），到点必打 ROLLED BACK——sig 已核实稳。

**时序（订正 design 的方向性错误）**：watchdog 于 **flip+120s** 先火；ctl 于 **staged 回执+150s**（node.go:386）后超时。后台收 ctl rc 的外层超时给 ≥210s。

| 步 | 断言 | 假绿防护（反向论证） |
|---|---|---|
| B1 前置 | 捕 OLD_SHA/PID0/ExecMainStartTimestamp；`fault_synblock_on agt1 4222` | 双自证：agt1 仍 ONLINE（established 活）∧ 容器内新 TCP 连 brk1:4222 timeout rc=124（规则真生效；rc=1 拒绝≠黑洞） |
| B2 dispatch | 后台 `node upgrade agt1 --url --sha256`，输出落文件；**闸=输出现出逐字 staged 行**（node.go:373） | staged 行只能在 broker/agent 双 url 门+下载+sha+冒烟+flip 全过后发出，排除"从未升级"蹭后续旧 sha 断言 |
| B3 窗口 | **poll-until 整体合取**：marker `pending ∧ boot_count>=1` ∧ dst sha==NEXTBIN_SHA ∧ MainPID==PID0 ∧ `/proc/$PID0/exe` sha==NEXTBIN_SHA | boot_count：install 恒写 0、只有新映像真跑过 shim 才 >0——分区下"新进程活到过 shim"的唯一窗口内直接证据；PID 不变单独是弱判别（systemd 重启会换），与 exe-sha 合取后只有 in-place exec 同时满足；poll-until 消解 exec 前 boot_count==0 瞬窗 |
| C 嵌套 | 前置 agt1b ONLINE → `node upgrade agt1b` 同 URL/sha → `assert_refuses` sig=`upgrade_in_progress` ∧ `pending its register deadline` | 三条同码文案互异（upgrade.go:86/107/129）：钉 marker 入口门专属短语，排除 TryLock/flock 撞锁蹭绿；marker 门先于 allowlist（:128 vs :142 代码序即隔离论证）⇒ agt1b 拒绝不依赖其 url_allow 配置；若 B 早失败，C 落到别的 code 响亮红 |
| B4 回退 | **poll-until 整体合取**（fault 未撤 ⇒ marker 静止零竞态）：marker `rolled_back` ∧ **Detail 含 `register deadline exceeded`** ∧ dst sha==OLD_SHA ∧ `.prev` 消失（被 rename 消耗）∧ MainPID==PID0 ∧ exe sha==OLD_SHA ∧ ExecMainStartTimestamp 不变 | Detail 钉死 watchdog 腿，排除 exec-fail（`syscall.Exec …`）与 boot 预算（`boot: budget/deadline exhausted`）两兄弟蹭绿——falsegreen 对 design 的缺陷 1 闭合；poll 整体合取消解 rename/marker 两步中间态竞态 |
| B5 ctl | 收 rc≠0 ∧ 输出同时含 staged 行与 `likely ROLLED BACK`（含 OLD_RELEASE 字面量） | 前者排除 config 拒绝提前退出满足后者；诚实标注：此行按 F7 是**推断** oracle，真相由 B4 磁盘/进程 oracle 承担 |
| B6 愈合 | `fault_synblock_off` → poll（预算 **120s**，旧进程重连走退避）：agt1 ONLINE ∧ RELEASE==OLD_RELEASE；brk1 journal `agent-reported upgrade outcome` 含 rolled_back（broker.go:1468）；随后 marker 文件**被删除** | outcome 行仅由携带 upgrade_state 且 markerTargetsThisAgent 的 register 触发，sibling 普通重连不能冒发；marker 删除=终态一次性投递的送达证明（S24 不对称） |
| A1 | 前台 `node upgrade agt1` → rc=0 ∧ 含 staged 行、`waiting for re-register (deadline 120s)`、`upgrade COMMITTED`（钉公共子串+NEXT_RELEASE，**不钉整行**——node.go:463/475 两变体） | 短语 grep 不整行 golden：避免与 hermetic 双钉同一文案造成漂移双红 |
| A2 进程级 | MainPID==PID0 ∧ ExecMainStartTimestamp 不变 ∧ exe sha==NEXTBIN_SHA | B 全程未重启 ⇒ 同一 PID 贯穿三次 exec；dst flip 后磁盘对全宿主都是新 sha（§21.3），只有真 re-exec 的进程运行它；timestamp 封死 PID 复用残洞 |
| A3 登记/磁盘级 | `node ls --json` RELEASE==NEXT ∧ ONLINE；dst sha==NEXTBIN_SHA；`.prev` sha==OLD_SHA；marker `committed ∧ boot_count==1 ∧ new_sha==NEXTBIN_SHA ∧ target_nid==agt1`；journal outcome 含 committed | 四件套互锁：盘翻进程退时③红、缓存旧行时②④红、flip-未-exec 时 exe-sha 红；committed 面包屑按 S24 保留 |
| A4 偏斜钉 | agt1b 仍以 OLD_RELEASE ONLINE（旧映像继续跑） | 共享二进制版本偏斜的正面事实：四件套阻 sibling 冒领 commit |
| C2 域释放 | `node upgrade agt1b` → COMMITTED ∧ agt1b ONLINE@NEXT ∧ agt1b 自己的 MainPID 不变 ∧ marker target_nid==agt1b | 证明 C 的拒绝是窗口性非永久，且 sibling 走一遍自己的四件套。**硬条款：A 的全部 marker 断言必须在 C2 之前完成；C2 整体覆写共享 marker（S24 wholesale replace），C2 后不得再断言 agt1 marker**（falsegreen 对 guards 缺陷 1 的闭合）。驳回 feasible"砍 C2"：拒绝文案时戳只证窗口存在，不证释放后 sibling 能成功；代价 ~40s |

## 4. S0 设施改动与新增

- `drills/lib/fault.sh`：新增 `fault_synblock_on/off <node> <port>`——**必须走 SIMFAULT 链**（驳回 rollback 草案的裸 iptables，单点 flush 清理纪律不破）；`--syn` 若 nf_tables 实测不支持则退 `-m conntrack --ctstate NEW`（落地实测定格）；头部语义表补第四条"有状态防火墙切新连"。iptables 已在 Dockerfile:18，无需 spike。
- `drills/lib/upgradecfg.sh`（新）：从 31 抽 `_allow_artifact`/`_allow_artifact_agent` 共源，31 行为零变化。
- agt1b 供给函数（drill 内联 ~25 行，第二 systemd unit 形态）：**落地前置 spike**（同容器双 unit 无先例）。注意 design 的降级方案（窗口内对 agt1 自身重发）被 G2 击毙——agt1 窗口内 OFFLINE，dispatch 撞 node_offline。若 spike 失败：C/C2 转 not_covered[gap] 并如实说明。
- `artifact.sh`、`ingress_trust_inject`、`drill_install_traps`（cleanup=artifact_down+fault_synblock_off+agt1b unit 清理）原样复用。

## 5. 预期新 [GAP]/gotcha 登记

- **新 gotcha（候选一的诚实去向）**："能骗过 version-串冒烟门的非 tether 产物 ⇒ 无 boot shim ⇒ 预算永不消耗、watchdog 随旧映像消亡、marker 永 pending、NAT 后节点永久失联，`.prev` 在盘上无人问津"。**定性采 rollback 草案 §3**（三评审一致裁定为最准）：§21.3 接受窗口的前提是"产物是 tether 二进制、crash-early"，非 tether 产物令前提失效、三层收敛器全旁路——**不在已接受窗口内**，是真 gap。登记 `docs/deploy-tier-gotchas.md`（威胁模型注明：sha 由 operator 背书，属"CI 打包错产物"级真实事故形态）；建议主进程在 §21.3 GAP 列表补一句（文档先行）。
- **E 探针臂不进 33**（裁决）：其 NRestarts/"死循环烧 CPU"断言已被证伪（exit 0 不重启、exit≠0 几秒进 start-limit failed）、脚本 exec 可行性未 spike、真 wedge agt1、+3.5min——最投机的臂不该拖累整卷。是否独立成 34 实跑（含 labeled 手工恢复顺带钉 `bootConvergeRolledBack`）交主进程/外审裁决。
- FIN 穿透（窗口内 agt1 必 OFFLINE）：真实行为，drill 注释如实呈现，非 GAP。
- 潜在：若实测 outcome 行时序/文案与文档不符，如实暴露不弥补，按探索→定格转 gotcha。

## 6. not_covered 诚实清单

- **nc_gap ×1**：无 shim 骗冒烟产物场景（指向 §5 gotcha #N；owner 待定：34 探针 drill 或产品加"产物自证 shim"防御）。**驳回 feasible"避免 INCOMPLETE 故不进 not_covered"**：那是 verdict-gaming——assert.sh 自述真实 debt 必须 not_covered[gap]，头注 gotcha 无 nc_gap 压力会腐化；#71 OPEN 是既有先例，"成功路径 drill 带一条诚实 debt"比"假 GREEN"符合本仓账本纪律。33 在 expected-verdicts.tsv 钉 **INCOMPLETE/1**。
- **31 收窄不摘除**（驳回 rollback/guards 的"31 转 GREEN"——M4 点名 fleet 成功即 two-node summary，33 只覆盖单台）：31 的 not_covered 改述为"`--all` 金丝雀 fleet 全成功扇出（单台 success/rollback/--wait 已由 33 拥有）"，31 仍 INCOMPLETE/1，reason/slug 同步。

## 7. 时长预算

up+init+joins(含 agt1b) ≈ 2.5min；artifact+allow 配置 ≈ 30s；B+C ≈ 3min（staged ~10s + watchdog 120s + ctl 150s 收尾）；愈合 poll ≤ 2min；A ≈ 40s；C2 ≈ 40s。**合计 ≈ 7.5–9min**，一个 120s 刚性窗口（砍 E 后从两个减到一个），低于 96 的 22min，run-drills 并行预算内。

## 8. 改动文件清单

1. `test/simcluster/drills/33-node-upgrade-success.sh`（新）
2. `test/simcluster/drills/lib/fault.sh`（+synblock，SIMFAULT 链内）
3. `test/simcluster/drills/lib/upgradecfg.sh`（新，31/33 共源）+ `31-node-upgrade-fleet.sh`（改引用 + not_covered 收窄）
4. `test/simcluster/expected-verdicts.tsv`：+33 行（INCOMPLETE/1）、31 行 reason 改述；`expected-verdicts-log.md` 两条登记（三草案共同漏项 G1，validate-verdicts 门会红）
5. `test/simcluster/drill-costs.tsv`：+33 行
6. `test/simcluster/README.md`：drills 表 +33、31 行改述、wall-time 注明
7. `tests/r9d-nonvacuity.sh`：按现约定登记 33 的可提取 oracle（落地时核对登记方式）
8. `docs/deploy-tier-gotchas.md`：§5 gotcha 一条（主进程定夺措辞与是否升格 [GAP #N]）
9. `docs/reviews/simcluster-coverage-inventory.md` + final-review F13 台账：NOT-COVERED 项落 owner=33（单台）/31（fleet 残余）

## 9. 风险与外审重点

- **评审致命缺陷处置**：design 时序反向→已订正（§3 时序段）；design gotcha 压 not_covered→改判进 nc（§6）；design 候选二论证→换 flagful 论证（§2）；rollback 裸 iptables→SIMFAULT 链（§4）；rollback E 臂→砍出 33（§5）；rollback/guards "31 转 GREEN"→收窄（§6）；guards C 闸 flake→双闸（B2 staged 行 + B3 poll marker）；guards C2 clobber→硬条款（§3）；guards 愈合 60s→120s；共同 G1→§8 条 4；共同 G2→§3 全局事实。
- **落地前置 spike（仅 2 项）**：① agt1b 同容器双 unit 供给（失败则 C/C2 转 nc_gap）；② nf_tables 下 `--syn` 匹配（退路已定）。
- **flake 面**：愈合后重连退避上界（预算 120s，仍不足则按实测放宽并注明依据）；窗口内 agt1b 自发掉线重连撞 node_offline（sig 响亮红非假绿，重跑即可，不掩护）。
- **外审重点**：① §6 的 INCOMPLETE/1 裁决是否接受（verdict 语义之争的最终定夺）；② E 是否升格独立 34；③ C2 的 marker clobber 条款是否够硬（后续 soak 类 drill 不得读 agt1 marker）；④ B4 Detail 短语与 upgrade.go 文案的钉法宽度（子串 vs 整句）。
