# simcluster-coverage-roadmap — 多专家对抗审查报告 + 主进程裁决

Date: 2026-07-10
对象: `docs/simcluster-coverage-roadmap.md`（rev1 → 本轮裁决后修订为 rev2）
Status: **内审完成，roadmap 已按裁决修订（rev2），停在外审门。**

## 工艺

- Workflow：6 个固定视角专家并行审查 → **每个视角固定 1 个对抗核验者**逐条到仓库独立验证
  （**Review=6 / Verify=6，两阶段 agent 数均为静态常量**，核验者无条件 spawn、与 findings
  多少无关——遵守全局编排规则）。全部 12 agent 继承会话主模型（≥ Opus 4.8 约束满足）。
- 视角：mandate-fidelity（定位铁律）/ coverage-completeness（覆盖完备）/ factual-accuracy
  （事实核查）/ engineering-feasibility（工程可行性）/ adversarial-testing（测试对抗性）/
  process-conventions（流程约定）。
- 产出：**61 findings**（BLOCK 6 / MAJOR 18 / MINOR 23 / NIT 14）；核验裁决 CONFIRM 48 /
  ADJUST 12（按核验者修正稿采纳）/ REFUTE 1。
- 主进程（唯一定稿人）裁决：**采纳 60（其中 12 条按核验修正稿）、驳回 1**（ENG-F7，核验者
  依据 file:line 证伪）。roadmap rev2 已整合全部采纳项。

## BLOCK 级（6 条，全采纳）

| ID | 内容 | 裁决与落点（rev2） |
|---|---|---|
| MAN-F1 / FAC-F1(ADJUST) | 「无遗漏闸」总表漏 `ops confirm/abort`、`recovery diagnose`、`recovery resnapshot`、`rebalance proxy`（手动 verb 行）、`node-pub`、broker-ops §8.4/§8.5/§8.6——表外行永不被勾，击穿「一行不漏」 | 采纳：§4.3/§4.4 补齐全部行（confirm/abort→40、diagnose/resnapshot→42、rebalance→74、§8.4→32 新臂、§8.5/§8.6→NOT-COVERED+理由、node-pub→NOT-COVERED）；§4 闸门规则加 **`--help` 命令树 diff**；§0 真相源同步扩展；DOC-2 预登记（diagnose/resnapshot 未入手册）。FAC-F1 的 evidence 修正（resnapshot 在 §8.6 顺带提及、node-pub 三册记为 hidden）一并采信 |
| COV-C1 | §4.3 整段漏 broker-ops §8（§8.4 单机手动升级=单机现网唯一升级路径；§8.5/§8.6 迁移剧本） | 采纳：§8.4 归 S5-32 新臂（stop→换二进制→integrity_check→start→G.2）；§8.5/§8.6 NOT-COVERED（需旧版布局基线，与 flag-day 同类） |
| ENG-F1 | `31-node-upgrade` 的 GREEN 中心断言（升级成功+MainPID 不变）**结构性不可达**：agent 侧白名单硬编码 GitHub 前缀、无任何操作员接线（usage §9.3/error_hints 却声称有 `--upgrade-url-allow` agent flag）——roadmap 自建的 artifact server 必撞 `url_not_allowed_local`；/etc/hosts 假冒 github 被 Mandate ① 明禁 | 采纳：31 重构为**探索→定格、预期落 gotcha #25 候选**（+DOC-3 并发文档缺陷）；负例臂（broker 侧拒/sha 拒）保持 GREEN；升级成功臂与 mid-upgrade kill 臂标注「随修复翻绿」；§4.1/§4.2 行加限定。核验者补强一并采纳：agent 二进制布局失真（root-owned /usr/local/bin vs install.sh 的用户可写 ~/.local/bin）登 §1.4 保真度债、S5 harness 对齐 |
| ADV-A1(ADJUST) | force-single 的防脑裂安全闸（peer-alive HARD-REFUSE / dwell / typed-confirm）在既有 drill 与 roadmap 全程**零拒绝臂**——闸门 no-op 化时 22 依然全绿（安全性质 false-green）。核验者修正了签名钉法：健康集群真 arm 的拒因是 quorum-not-lost（fsArmVerdict 按 dwell→peer-alive 顺序返回**首个**失败门），peer-alive 拒须构造「broker 停但端口仍应答」的 peer | 按修正稿采纳：22 增**四拒臂**（quorum-not-lost / dwell-remaining / peer-alive〔停 broker 留端口应答〕/ arm token 单发重放）+ 保护模式臂（COV-C4 合并）；12/20 共享 setup 加杀-peer-前 OFFLINE assert_refuses；§4.4 新增「force-single 拒绝门」行 |
| PRO-P1 | 三处「既有覆盖」记错：`cluster doctor`/`--check` 全 sim 零调用（init 不带 --check）；`join approve --wait` G4 后**有意不用**（drill 11 反而断言非阻塞）；`keygen` 零调用（secrets.sh 用 host openssl+nk） | 采纳：§4.4 三行改写——doctor→S7-50 正向 preflight 臂（并注明先前记「既有」有误）；approve `--wait`→42 rejoin 收尾落点 + 注明 grow 路径有意非阻塞；keygen→S7-52 产品铸钥臂 |
| COV-C4（原 MAJOR，与 A1 合并后按 BLOCK 域处理） | 保护模式（quorum-lost 下 routine 命令全拒、唯 force-single 可动，runbook §2.3 整节语义）无表行无 drill 臂 | 采纳：§4.4 新增行；22 的 dwell 窗口内加 retire/set-raft-addr 拒绝臂（成本≈0，fixture 现成） |

## MAJOR 级（18 条，全采纳；摘要）

- **MAN-F2 + COV-C2**（`--install-user-service` NOT-COVERED 是借口式+引证失实）：降为 S2-82 的
  **user-service spike 臂**（enable-linger + install.sh --role agent 真路径）；引用漂移改为实测
  结论驱动。
- **MAN-F3 + PRO-P3 + FAC-F6(NIT)**（范围声明自相矛盾 + f460148 日期错）：范围句改「已发布至
  v0.4.7 的全部产品功能面中缺 deploy-tier 覆盖者（含 G 欠账）」；日期改 2026-07-05。
- **MAN-F4**（72 正向 SS 腿与私网默认拒结构性冲突——桥内全是 RFC1918）：双 agent 分工
  （agent-A 显式 `allow_private_destinations: true` 承载正向、agent-B 默认承载负例）；OQ-1 补前提。
- **COV-C3**（runbook §4 记「既有」高估——既有只割接过空库；带活数据割接+回滚无归宿）：新增
  **43-migrate-live-data**（业务行存活断言 + restore tether.db.bak 回滚臂）；§4.4 拆行。
- **COV-C5 + ADV-A8(MINOR)**（91 自称清 G3 债实则砍掉 B/C 两臂）：C 臂（offline FS prune 后
  seeds drop-only 收敛）进 91（复用 12/20 fixture）；B 臂（online FS 后收敛仅 survivor）骑
  92(b) leg、软依赖 22 结论；S8 验收出口改为按臂如实清账。
- **FAC-F2**（95 的 kill -9 是 #23 伪证明——SIGKILL 属 unclean，on-failure 同救）：改为
  **对 MainPID 直发 SIGTERM**（NotifyContext 优雅退出 exit 0 = clean-exit，唯 Restart=always
  拉起）作判别性证明；kill -9 保留为独立 G.2 崩溃恢复臂；§4.5 行同步改写。
- **ENG-F2**（S3「harness 增量：无」为假——sim agent 从未接线 tunnel_addr，expose 数据面必死）：
  S3 harness 增量改为按 install.sh 形态写 agent.yaml `tunnel_addr`（Mandate ③ 正当供给）；
  §1.4 登保真度债。
- **ENG-F3**（「~35 drill 全并发无压力」与自家 grow-concurrency flake 记载矛盾，并发 grow ~×3）：
  §1.1 改为校准表述；OQ-8 重写（从 S1 起分波/`-j`）；S9 基线固化推荐参数。
- **ENG-F4(ADJUST)**（OQ-4 的「等稳定后再杀」与 bounce-on-kill 机理自相矛盾）：按核验修正稿
  重写 OQ-4——顺序约束（先判 bounce 生产可达性→gotcha；确证容器特异→驯服〔杀后等级联平息+
  poll 窗参数化〕），两出口同权；MAN-F10(MINOR ADJUST) 一并落实（该条目移出「保真度债」、
  改列「覆盖债（分诊未定）」）。
- **ADV-A2/A3**（50 无内容同一性对照；torn-bundle 与 kill-9 restore_in_progress 两条文档化
  fail-closed 承诺零覆盖）：50 增 X/Y/Z 阳性+阴性对照（恢复=备份时刻而非空库）、torn-bundle 拒
  （断言原 DB 字节未动）、kill-9 中断→daemon 拒启→重跑续完；51 以灾前 expose 的 curl 真流量收口。
- **ADV-A4**（S9 名不副实：无 soak、无分区、无双故障）：96 增网络分区臂（docker network
  disconnect，静默丢包≠RST）与双故障臂（G.1×G.2 交织）；新增 **97-soak-cycles**（P8 24h 原型的
  参数化缩放替身，差距显式登记）。
- **ADV-A5**（G.5 对账审计承诺无行无臂）：§4.5 增 G.5 行；94 两臂各加 history
  `kind=reconciled/reconciled_closed` 断言；G.4 注明为元表。
- **ADV-A6**（三处可恢复/中途语义无中断臂）：40 增 mid-retire kill-leader→resume 臂；30 增
  B2 锁真互斥断言（持锁期 join/retire 被拒）+ stale-lock 主动清除臂 + `--ack-writefence` N=2 臂
  显式入规格；31 的 mid-upgrade kill 臂如实标注「随 #25 候选修复解锁」。
- **ADV-A7**（信任锚链全正向臂）：82 增伪造 invite（错 pin，端到端拒+无半 onboard 残留）与
  篡改 invite 负例；91 增错锚 roster 拒+缓存不毒化臂。
- **PRO-P2 + MAN-F7(MINOR) + COV-C6(MINOR ADJUST)**（§4↔§3 六处失配、幻影 43 号）：每处
  「顺带/併入」在 §3 对应 drill 规格补落点句（rebind→41 臂、--plan 零写→40、incident export→50、
  --ack-alerts 复验→92(a)、G.3/logout→60、--ack-writefence→30）；43 号改实体化为
  migrate-live-data（同时解决 COV-C3）；§3 头注明确「§4 只是核对清单」。

## MINOR / NIT（37 条，36 采纳、1 驳回；按主题归并）

- **92 配方修正**（MAN-F6 + FAC-F4 + ENG-F5(ADJUST)）：「N=1 停 nats」备选**双重不可行**
  （JS-503 分类器只认 10008、连接类错误刻意排除；且 N=1 唯一 NATS 停掉后 `--remote` 观测面
  自身不可达）→ 92 拆两 leg：(a) N=2 杀 peer（不 force-single）→ 泛化 sustained-503 banner
  （22-独立，保底 G7b）；(b) #20③ 专属 banner+`--to-standalone` auto-clear（产品门在
  `force_single_active`+conf 滞留上，G2 后原生只剩 ONLINE 可达）→ 软依赖 22，不可达成则该 leg
  如实 NOT-COVERED-in-sim。§1.3 注明 g7-plan 原配方已被 G2 修复淘汰。
- **31 `--all` 分类臂按实现校准**（FAC-F3）：已 OFFLINE 的 agent 在枚举期被静默排除、不产生
  skip 行——臂改为「枚举后失能」（SIGSTOP/docker kill 在心跳窗内）→ dispatch 期
  agent_no_responders → skip+汇总；另一臂钉「已 OFFLINE 者静默排除」的语义分界。
- **编号约定措辞**（MAN-F8 + COV-C11 + FAC-F7(ADJUST) + PRO-P9）：「沿既有」改「自本 roadmap
  起确立」；12/13 历史例外注记（12/20/21 来自 gotcha 号、13 为顺延号——FAC-F7 核验者对 13 的
  勘误一并采信）。
- **S1 PTY 风险点反写**（ENG-F6 ADJUST）：容器恒 --privileged 且 ptmx 是文档化前提——风险点
  改为「烘焙布局偏离 install.sh 的双向失真」；§0.3-1 加 caveat。核验者对原 finding「特权掩盖
  权限缺陷」机理错误的驳正一并采信（特权容器内非 root uid 仍受完整 DAC 约束）。
- **90/92 洁净基线**（ADV-A11）：注入前先断言无 active broker_down / 无 DEGRADED banner。
- **61 双向执法+软链臂**（ADV-A9）：push 写侧/pull 读侧各一臂、not_a_regular_file 双向、
  path_not_found 补列；sha_mismatch/path_race 显式裁剪留 hermetic。
- **72 撤销与伪 token 负例**（ADV-A10）：revoked-PSK 新连接拒（salt 失效承诺）、/sub/<junk>
  负例、重复 create 定格。
- **80/81 对抗加厚**（ADV-A12）：错 PIN 负-正对照 + 失败尝试可见性探针（不可见→观测性 gotcha
  候选，不断言不存在的限速承诺）；evict 时 RUNNING 进程/活跃 expose 归宿 + 被踢 nkey 重入
  语义定格。
- **映射勘误**（COV-C7/C9 + FAC-F5 + PRO-P8）：rejoin 行改 42 单独承载；remote_fs 补 62；
  flag-day 行引注改 usage §8.3（+runbook §6/§4）。
- **flag-day 理由改写 + 60 拓扑**（COV-C8）：NOT-COVERED 理由改为真实成本（v1 二进制 Release
  可下、成本在车队基线+双 proto 矩阵）；60 按自立的拓扑最小化原则改 N=1。
- **README 11 号行漂移**（MAN-F9）：登 §1.4 债、S1 清偿。
- **make lint**（MAN-F11 + COV-C10 + PRO-P4）：§6 守恒闸补第三件套。
- **三分诊反掩盖细则**（MAN-F5 ADJUST）：§0.2 增判定程序（harness 修复新增生产供给之外行为
  须过 Mandate ④ 说明，给不出改登 gotcha）。
- **流程杂项**（PRO-P5/P6/P7/P10/P11/P12 + FAC-F8）：每批补「依赖：」行；重排继承条款；
  #I\* 族收编关族；Workflow 模型地板改绝对表述「≥ Opus 4.8」；-round/-tasklist 惯例注记；
  proxy-aware dial 行删去仓外 memory 引注（核验者指出改引外审文件同为不实引用，径直删除）；
  「同 G-roadmap」改「仿 G-roadmap 的三准则结构」。

## 驳回（1 条）

- **ENG-F7**（OQ-6 低估 colocated-agent 供给：broker 容器无 sim 用户/无 home/无身份目录）——
  **REFUTE 成立，驳回**：Dockerfile 镜像层为**所有角色**烘了 `useradd -m sim`（provision 的
  useradd 本就带 `id -u sim ||` 守卫）；agent 身份目录由 `EnsureAgentIdentity` 在首连时
  `MkdirAll` 自建；`cmd_agent_join` 角色无关。OQ-6 维持原状（供给方式与 session 归属仍是
  plan 题，但无缺失前置）。

## 结论

roadmap rev2 已整合 60 项采纳（含 12 项按核验修正稿）。批次结构不变（S1–S9），drill 提案数
27→**30**（新增 43-migrate-live-data、97-soak-cycles；22/31 重定性为探索→定格），全套预计
~37 个。**停在外审门**：未 commit、未 git add，待用户外审
`docs/simcluster-coverage-roadmap.md`（本报告随同）。
