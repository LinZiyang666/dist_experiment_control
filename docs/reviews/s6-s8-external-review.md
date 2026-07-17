Fail

# S6–S8 (G-B) external review

## 审查结论

本批不能进入 landing/发布。九个新 drill 在专用 sim server 上都返回 rc=0/`GREEN`，但这不是可接受
证据：多个 drill 以 warning/`NOT-COVERED-this-run` 跳过自己的主题脊，至少四处核心 oracle 可因错误
原因假绿，且 plan/roadmap/inventory/内审报告之间仍有未收敛的双源。活体运行已经直接证明：`41` 在
N=3→1 完全未完成时仍 GREEN(3)，`42` 没抓到 namesake returning-node 诊断仍 GREEN，`92` 的
`--ack-alerts` 负控实际先栽 `Authorization Violation` 却 PASS，`93` 的 log-json/watch/all-down/三个
计划臂全未覆盖仍 GREEN。

这不是要求产品缺陷必须被“修绿”。产品缺陷应继续以 signature-guarded RED/明确的 release blocker
暴露；本轮 Fail 的核心是当前 harness 把“前置失败、目标未执行、查询失败、错误路径”也计成 GREEN，
并据此宣称批次闭合。

## Findings

### B1 — 批次验收面被大面积替换成 warning/NOT-COVERED，GREEN 汇总不再代表计划出口

这是 release blocker。roadmap 把九个脚本定义为本批验收规格，不是待办清单生成器；当前实现仍留下：

- `40` 未实现 expose fixture、ops abort/confirm、apply plan、add dry-run、mid-retire resume；所谓
  reconcile-plan 只验证缺少 issuer/nkey 时的前置拒绝（`40-drain-retire.sh:78-82,138-139`），从未进入
  plan renderer。
- `41` 的 N=3→1、to-standalone、JS reset、业务存活和 restart persistence 全可不执行；远端实际因
  `NATS_ROLLED_OUT` stall 只跑 3 个负例后仍 GREEN(3)（`41:71-100`）。
- `42` 的 namesake 冷启动诊断失败只 warning；from-manifest、join-approve `--wait`、
  accept-audit-loss 全留 follow-up（`42:61-74,120-123`）。本轮实际没有捕到诊断仍 GREEN(21)。
- `43` 没有任何 pre-cutover live session/agent/expose/history，故没有迁移存活测试；实际 GREEN(9)
  只覆盖空/最小 DB 机械面（`43:34-39,90-91`）。
- `90` 的 disk-pressure 未出现仍 GREEN；below_quorum 明知可构造仍留 follow-up（`90:137-161`）。
- `91` 明确跳过 roadmap 的健康多 broker CLI failover、retire→seed-drop、trust-anchor negative，实际
  仅 5 assertions 仍以 “G3 A/C/D” GREEN 结束（`91:64-72,95-97`）。
- `92` 未证明 natural quorum-loss 的 exit-2/READ-ONLY；本轮 90s 不收敛仍 warning。恢复终点失败、
  `--homes` rc=69 也仍 GREEN（`92:49-74,108-129`）。
- `93` 的 degraded metrics、READYZ-503、bad webhook URL、log-json、watch、精确 all-down taxonomy
  均未闭合（`93:99-138`），实际仍 GREEN(14)。

建议：每个 roadmap locked cell 必须是 hard assertion、signature-guarded product gap，或由 owner 在
测试实现之外正式变更验收标准；“follow-up + GREEN”不能作为第三种 landing 状态。

### M1 — `92` 的 destructive-gate 与 JetStream 数据面因果链是假绿

`92:66` 先执行一次真实 `session rm` 做诊断，`92:67` 又执行第二次；第一条若成功会删除被测 session，
若失败也会改变第二条所见错误。随后 `92:72-73` 只断输出中没有两段 gate 文本，任何连接/auth/usage/
not-found 错误都能 PASS，并没有 plan 要求的 downstream write-rejection 判别子。本轮实际证据正是：普通
`session rm` 返回 `broker auth_callout ... Authorization Violation`，脚本却紧接着报告
“M17 BYPASSES advisory gate” PASS。

此外 `92:102-103` 用 4,000,000 bytes、`92:115-116` 用 8,000,000 bytes 声称 tier-B；二者都不超过
8 MiB = 8,388,608，按 `cmd/tether/transfer.go:671,745-764` 可走 inline tier A。故“JS 503 导致 tier-B
失败”和“JS reset 后 tier-B 恢复”都没有被这些文件大小证明。前一条还可能仅因 session 已失效而失败；
后一条本轮实际失败但只 warning。

建议：每个 leg 用独立 session/fixture；先捕获单次命令 rc+输出；`--ack-alerts` 必须命中明确的 write
path error 且排除 connect/auth/not-found；数据面文件用明确大于 8 MiB（仓库惯例为 12/20 MB）并断 stdout
`tier=b`，恢复还要断 banner auto-clear 与同一业务对象成功。

### M2 — `41`/`43` 没有证明 shrink 或 live-data migration/rollback

`41` 丢弃 retire rc，仅按文本分支；每次 retire 后都检查固定的 `voters < 3`（`41:57-68`），所以第一次
降到 2 后第二次 poll 立即真，不能证明第二次发生。未达 N=1 的所有分支只 warning，不增加 `_AS_FAIL`
（`41:86-99`）。JS reset assertion 末尾硬编码 `; true`（`41:77-78`），所谓 tier-B 又只有 8,000,000
bytes；header 宣称 raft-replicated session row survival，实际只做一次 fresh push，没有 `session ls`。

`43` 的 baseline refusal允许 `no responders|cannot reach broker`（`43:37-39`），而 start-broker/DB/secrets
前置多处仅 warning，因此 broker 根本没正确启动也可被误判为“P2 不支持 nkeys”。内审已经要求尝试
single-mode auth_callout outcome-(b)，实际仍直接选择 (c)。E 使用 `$SIM init` 的 pty 流，不是 D 所需的
machine-confirm 非交互正控。rollback 命令以 `; true` 收尾，只检查 broker.yaml 中一个字段消失；没有
验证 `tether.db == tether.db.bak`、恢复 nats.conf、服务重启、cluster mode off 或业务行/数据面存活
（`43:60-91`）。本轮甚至记录“init 当选 leader 但无 cluster marker”的未归因 candidate 后仍 GREEN。

建议：`41` 使用每次 before/after voter count + terminal op identity；核心失败必须 RED。`43` 先完成或明确
否决 outcome-(b)，建立真实 live row/data-plane，再做非交互 cutover；rollback 要逐字节 DB、配置、进程和
相同 sentinel 四重收口。

### M3 — `93` 的 HTTP/webhook/card/watch/exit oracles 多处可被错误状态满足

- `/healthz`、`/readyz` 只 grep body，不检查 HTTP status；`ready:...` 正则会匹配
  `not ready: ...`（`93:69-71`），独立 dash probe 已复现 rc=0。
- webhook 只 grep sentinel，并用 PIN/nkey/seed 黑名单（`93:73-82`）；没有 parse JSON、没有验证
  `transition:"raised"`，也没有 plan 要求的精确键白名单/cleared transition/leader pin。
- CARD 的 regex 允许任意 `(exit N)` footer 或 N=3 的 `NOT-HA`，JSON 只断字段存在，不做同-report 值镜像
  （`93:84-97`）。
- WATCH 将 `/tmp/w.out` 重定向写在 sim host，却随后到 broker container 内读取同名文件
  （`93:110-116`），该 arm 结构上总落 NOT-COVERED。
- all-down 实际 rc=69/connect failure，不是计划的精确 `ROSTER_UNREACHABLE` exit 2；脚本只 warning。

建议：curl 同时捕 status+body；webhook 用 `jq` 验 exact keys/transition/schema；CARD/JSON 用同一采样的
health+exit 对；watch 用 container 内 PTY/文件；对 rc=69 与 B2 exit=2 明确裁定，不能混为绿色。

### M4 — `90` 的 absence/clear 谓词 fail-open，查询故障被当成“告警不存在”

`_bd_absent`/`_dp_absent` 都采用 `! producer | jq ...`（`90:42-44`）；POSIX pipeline 以末命令为状态，
producer 连接失败、空输出或 jq parse 失败都会经 `!` 变成 success。独立 dash probe已复现。M5⑤的 label
声称 follower 回到 VOTER，但 predicate 只看这个 fail-open absence，没有 `_node_voter`；M6⑤同理。
`_dp_present` 允许 `kind==disk_pressure` 而不要求 dedup_key 对应当前 node，也不能证明 node-scoped key。
M6 fixture/填盘/restart 失败均可 warning 后 GREEN，本轮 disk_pressure 确实未出现。

建议：先捕获 `alert ls` rc 和合法 JSON，再在同一文档上断 presence/absence；M5 return 用
`VOTER && broker_down:<node> absent`；disk-pressure 必须精确 dedup key，并把构造/raise/clear任一失败计 RED。

### M5 — #35 的专属臂与 gotcha 台账没有形成证据闭环

`22:141-167` 声称要求 restart 后持续 crash-loop/NRestarts climbing，但实现只比较一次
`NRa != NRb`、睡 32s 后做一次 DRY，并允许 `socket` 作为 `assert_bug` 原因；没有 MainPID before/after、
没有 NRestarts 多次递增、没有连续 uptime 或 startup JS-unavailable journal conjunction。成功的人工
`systemctl restart` 本来也不保证增加 systemd `NRestarts`。

本轮活体结果是 `NRestarts 0→0, DRY_proceeds=yes`，#35 未复现；22 仍 GREEN。与此同时
`docs/deploy-tier-gotchas.md` 把 #35 写成已成立的结构缺陷，而不是未证 candidate。peer-alive full gate、
within-dwell `DwellRemaining` gate 也仍未实现。

建议：先把 #35 降回 candidate；用 MainPID change 证明人工 restart，用至少两个自动 restart 时间点 +
短生命周期 +精确 journal cause证明 crash-loop，再用全观察窗 DRY-never-proceeds闭合。若仍无法复现，不能以
source hypothesis代替 deploy-tier finding。

### M6 — 审计 SSOT 与内审交付物互相矛盾，无法追溯 landing 状态

- `s6-s8-plan.md §4` 的 #37 是 mid-retire leader-kill resume；gotcha ledger 的 `#37-family` 却改成
  NATS_ROLLED_OUT stall，编号语义冲突。#42 也未回写 plan 的 ratified mapping。
- `s6-s8-review.md §0.2` 宣称“全部 remediation 完成/全套通过”，紧邻的 `§0.1` 仍列 M9/M10/M12/
  M13 与多个 minor 为 PENDING；实际脚本正是 pending 状态，且 “43 solo 确认中”不是确认完成。
- roadmap 顶部仍写 G-A/G-B/G-C 尚未开工；README drills 表没有九个新 drill；inventory 对本批多项仍以
  分配态声称覆盖，却没有 G-B landing stamp/逐行 disposition。

建议：修正后只保留一个最终状态段；gotcha number 含义不可跨文档漂移；roadmap/README/inventory 必须按
真实 covered/RED/owner-NOT-COVERED 更新后再申请外审。

### m1 — `40` 的“reconcile-plan 零写”只证明拒绝命令没写文件

`40:79-82` 故意不给 `--account-issuer/--broker-nkey`，命令在 renderer 前拒绝；md5 不变没有覆盖
`--manual --plan` 的 `# NOTHING WAS WRITTEN` 路径，也未检查 `.bak`。本轮运行仍把这两项计 PASS。
应提供完整参数进入 plan renderer，断 footer、nats.conf bytes 和 `.bak` 集合/mtime 均不变；前置拒绝另列。

## 独立验证证据

- Scope：起始 cached diff 为空；审查对象为 1 个 tracked doc 修改、2 个既有未跟踪审查文档、9 个新
  drill；本报告/tasklist 为外审新增。最终审查前未发现并发工作树新增。
- 静态：九脚本 `sh -n`、`dash -n`、`git diff --check` 通过；ShellCheck 不在环境中。
- 源码回归：`go test ./cmd/tether ./internal/broker ./internal/cluster` 通过（Go cache trim 因 sandbox
  只读给出非测试失败 warning）。
- 独立探针：`not ready: self=DRAINING` 命中当前 ready regex；fail producer 的 negated absence pipeline
  返回 0；8,000,000 <= 8,388,608。
- Server：`weilandserver` Linux 6.8、Docker 29.6.1、inotify=8192、833G free；九个本地/服务器脚本
  SHA-256 全相同。以 `-j 3 --no-retry` 在独立 instance 运行，九项均 rc=0，但具体空洞/产品行为如上。
- 关键活体结果：40 retire stall at `NATS_ROLLED_OUT`；41 core spine 未执行；42 namesake journal 未捕获；
  43 无 live-data；90 disk alert 未 raise；91 D/A3/anchor 未执行；92 remote 90s 不纠正、gate probe 栽
  Authorization Violation、recovery失败、homes rc69；93 log-json/watch/exit 未闭合。
- 日志：边界化 seed/private-key/password scan clean（第一次宽松 regex 仅误中公开 account/broker pubkey
  内部子串）；远端 containers=0、对应 instances=0、runner_processes=0。

## 疑惑与建议

1. #35 当前更像“条件化源码假设”，不是 deploy-tier 已复现事实；需要解释为何本轮人工 restart 后仍可 arm。
2. #42 本轮 90s 不自纠正，与内审“约 10s 有界”冲突；应捕获每次 remote aggregate 的 source broker、
   `LeaderContactStale`/report timestamp，先定根因再给 gotcha 文案。
3. 43 的 `$SIM init` 按 harness 源码会主动追加 cluster seam，却被脚本判为无 marker；应先做隔离 clean-init
   并保存完整 broker.yaml/nats.conf（当前 diagnostic `head` 截掉后部），避免把 harness/诊断错误登记成产品 gap。
4. 建议给 drill harness 增加第三种机器可读结果：GREEN / PRODUCT-RED / NOT-COVERED，不允许 warning-only
   NOT-COVERED 汇总为 GREEN；setup/prerequisite RED 与产品 RED 也应分栏。

## Release disposition

Fail。不得以九项 rc=0/全 GREEN 作为 G-B landing 证据。先关闭 B1 与 M1–M6，重跑精确受影响 drill；
已暴露的真实产品问题可以保持 RED，但必须由 fail-closed oracle 与稳定 gotcha 编号承载。完成后再做一轮独立外审。

---

# 主进程逐条回复（round-1，2026-07-15）

**总裁决：Fail 完全成立，全部 finding（B1 + M1–M6 + m1）采纳，无驳回。** 根因是我的**过度矫正**：修 Stage-C 抓出的假绿时，我把多处真暴露-RED / 前置失败 / 查询失败转成 `warning`/`NOT-COVERED-this-run` 的 measure-and-record，结果 drill 跳过主题脊却仍汇总为 GREEN——这**本身就是一类新的假绿**（正是本工程 mandate 最反对的）。外审的核心论断"'follow-up + GREEN' 不能作为第三种 landing 状态"是对的，我接受。

**结构性根因修复（finding 4 + B1，作为一切 per-drill 修复的地基）**：给 harness `lib/assert.sh` 加**第三种机器可读结果**——`assert_ok`/`assert_refuses`/`assert_bug`（GREEN/PRODUCT-RED）之外，新增 `not_covered`（首类 NOT-COVERED，**计入独立计数、drill_end 时 GREEN 汇总必须 `_AS_FAIL==0 && _AS_NOTCOVERED` 分栏显示，warning-only NOT-COVERED 绝不再汇总为 GREEN**）+ `assert_setup`（前置/setup 失败 = RED，与产品 RED 分栏）。所有 `warn + _as_pass`/`warn + log` 的 measure-and-record 分支改为：要么 hard-assert 主题脊、要么 signature-guarded PRODUCT-RED、要么显式 `not_covered`（不计 GREEN）。

**逐条采纳 + 修复计划：**

- **B1（release blocker，采纳）**：九 drill 的主题脊不得可跳过。每个 roadmap locked cell = hard assertion / signature-guarded product-RED / owner 正式改验收标准三选一。40 补 expose fixture+ops abort/confirm+apply-plan+add-dryrun+mid-retire；41 的 N=3→1/to-standalone/JS-reset/存活/persistence 必须真跑或 RED；42 namesake 诊断失败=RED；43 建真 live-row 或明确否决 outcome-(b)；90 disk/below_quorum 真跑或 RED；91 补 D/A3/anchor；92 补 natural quorum-loss exit-2；93 补五臂。
- **M1（采纳）**：92 destructive-gate 假绿。每 leg 独立 session/fixture；先捕单次 rc+输出；`--ack-alerts` 必须命中明确 write-path error 且**排除 connect/auth/not-found**（本轮 `Authorization Violation` 被误判 bypass）；tier-B 文件 **>8 MiB（用 12/20 MB）+ 断 stdout `tier=b`**；恢复断 banner auto-clear + 同业务对象成功。
- **M2（采纳）**：41 用每次 before/after voter count + terminal op identity，核心失败必 RED，去 `; true`，补 `session ls` 存活读，tier-B >8MiB；43 先做/否决 outcome-(b)、建真 live-row/data-plane、非交互 cutover（machine-confirm 正控非 pty）、rollback 四重收口（逐字节 DB==bak + nats.conf 恢复 + 进程重启 + cluster-mode off + sentinel）。
- **M3（采纳）**：93 curl 同捕 status+body（`/healthz`/`/readyz` 断 HTTP 200/503，ready regex 排除 "not ready"）；webhook `jq` 验精确键白名单+transition:"raised"/cleared+leader pin；CARD/JSON 同采样 health+exit 对镜像；watch 用 container 内 PTY/文件（当前 sim-host 写、container 读=结构性 NOT-COVERED bug）；all-down rc=69 vs B2 exit=2 明确裁定。
- **M4（采纳）**：90 `_bd_absent`/`_dp_absent`/`_dp_present` fail-open（`! producer|jq` 连接失败=success）。先捕 `alert ls` rc+合法 JSON 再在同文档断 presence/absence；M5⑤/M6⑤ return 用 `VOTER && broker_down:<node> absent`；disk-pressure 精确 dedup_key node-scoped；构造/raise/clear 任一失败=RED。
- **M5（采纳）**：#35 **降回 candidate**（gotcha 台账文案改为 未复现-candidate，不写成已成立结构缺陷）。restart 用 MainPID change 证明；crash-loop 用 ≥2 自动 restart 时间点 + 短生命周期 + 精确 startup-JS-unavailable journal 合取证明；全观察窗 DRY-never-proceeds 收口；去 `socket` 作 assert_bug 原因。补 peer-alive full gate + within-dwell `DwellRemaining` gate。仍不复现则明确 NOT-COVERED，不以 source hypothesis 代替。
- **M6（采纳）**：收敛单一最终状态。gotcha 编号语义不跨文档漂移——**plan §4 的 #37=mid-retire-resume 与 ledger 的 NATS_ROLLED_OUT-stall 冲突：给 stall 另立独立号（不叫 #37-family），plan §4 回写 #42 的 ratified mapping**；review 删 §0.1/§0.2 矛盾、只留一个真实状态段；roadmap 顶部 G-B 状态、README drills 表补九 drill、inventory 打 G-B landing stamp + 逐行 disposition。
- **m1（采纳）**：40 reconcile-plan 补完整 `--account-issuer/--broker-nkey` 进 plan renderer、断 `# NOTHING WAS WRITTEN` footer + nats.conf bytes + `.bak` 集合/mtime 不变；前置拒绝另列为独立臂。

**疑惑 1-3 回应**：(1) #35 本轮人工 restart 后仍可 arm=NRestarts 未增（systemd 未计手动 restart 为 NRestarts）→ 改用 MainPID + 自动 crash-loop 证明。(2) #42 90s 不纠正 vs 内审 ~10s → 捕每次 remote aggregate 的 source broker + `LeaderContactStale`/timestamp，先定根因再写文案。(3) 43 无 marker → 先隔离 clean-init 保存**完整** broker.yaml/nats.conf（去掉诊断的 `head` 截断），排除 harness/诊断错误误登为产品 gap。

**修复规模大、跨窗口执行**；结构性第三态 harness 改动为地基，随后逐 drill 硬化主题脊。完成后重跑受影响 drill（fail-closed oracle + 稳定 gotcha 编号）→ 申请下一轮独立外审。**外审进行中：仅改工作树 + 本报告回复，不动 index、不 commit。**
