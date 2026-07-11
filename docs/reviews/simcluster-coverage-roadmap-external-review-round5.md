# Fail — simcluster coverage roadmap rev6 外部复审（round 5）

Date: 2026-07-10

rev6 关闭了 `/sub` event-kind 误称和 orphan 重注册证据问题，也补齐 round-4 点名的 flag；但对
**构造后的实际 Cobra tree**做独立遍历后，inventory 仍有大量点名清单之外的行为 flag 缺失，且
新增的 restore “machine-confirm 缺 env”断言与产品明确的 never-escapable 语义冲突。本轮确认
**2 个 Major**，暂不放行。

## Round-4 关闭矩阵

| 项 | 结论 | 独立复核结果 |
|---|---|---|
| R4-F1：完整命令/Hidden/行为 flag 清单 | **Open** | 点名项已补，但构造树仍对附录产生非零 diff；若干身份、安全和灾备 flag 没有行/归属/断言。见 R5-F1。 |
| R4-F2：`/sub` kind/reason | Closed | 附录 §0/§1.4/§3 与 roadmap §4.6 已统一为 `pubSysEvent` event kind，payload 字段也写实。 |
| R4-F3：orphan 引用和重注册证据 | Closed | 函数本体/回调文件引用已修正；断连后 `agent_registered`/re-register 日志为证，明确排除 roster ONLINE；两个阶段有独立超时。 |
| 可重复的树级生成 | Partial | 生成方法已升级、计划随 S0-台账落地，但 rev6 当前手工附录并未与构造树零 diff。 |

## Findings

### R5-F1 — Major — 构造树实测仍与“源码级全量”附录非零 diff

本轮临时加入诊断测试直接构造 `newRootCmd()`，递归遍历 `Commands()`，并采集 local、persistent、
inherited flag 及 command/flag Hidden；测试输出后已删除，仓库不留实现改动。它确认 rev6 的
command path 和 Hidden command/`--yes` 计数基本正确，但附录第 128–165 行仍漏掉多项会改变
身份、落盘、安全或恢复结果的 flag：

- `agent join` 实际还有必需的 `--nid`、bootstrap `--pin`；`agent config refresh` 有
  `--session/--once`；`agent doctor` 有 `--session`。附录却分别只列 start/expect 或空 flag。
- `cluster join prepare` 漏 `--name/--nats-server-id/--cert-fp/--secrets-dir`，其中后三项直接参与
  server identity、tunnel provenance 和入群 bundle；`join approve` 漏 `--timeout`。
- `cluster doctor` 实际有 `--db/--conf/--raft-addr/--nats-route/--socket` 等 preflight 输入，
  附录只列 `--secrets-dir/--offline`；这会漏掉“检查了错误 DB/conf/监听地址仍判绿”的部署风险。
- offline `recovery force-single` 漏 `--nats-conf/--nats-server`，它们决定灾难恢复后去集群化写向
  哪份配置、用哪个 binary 做 fail-closed 校验。
- `recovery resnapshot` 用“`--confirm-node-id` 等”代替全量字段，漏掉
  **`--accept-audit-loss`**、`--data-dir/--db/--self-id/--raft-addr`。接受未发布 audit 截断是明确的
  数据损失开关，不能藏在“等”里或无覆盖裁决。
- `recovery restore` 漏 `--data-dir/--db/--raft-addr`；`--raft-addr` 正是 fresh-host IP 变化时的
  恢复逃生。`incident export` 漏 `--sid/--force`，后者改变 O_EXCL 防覆盖行为。
- `recovery rejoin prepare` 漏 `--data-dir/--db/--guided`；`recovery diagnose` 整行写无 flag，
  实际有 `--self-id/--db/--offline`；`reconcile nats` 漏 `--timeout`；`serve` 仍漏 `--nats-url`。

以上都是构造树中的真实可达 surface，不是纯 `--json` 输出格式。尤其
`--accept-audit-loss`、restore `--raft-addr`、join identity/provenance 和 doctor preflight 输入，
任何一个出错都可能在上线恢复/入群时造成数据或安全事故。附录仍自称“源码级全量”“零 diff”，
因此覆盖闸门会 false-green。

**要求**：不要继续按上一轮点名做增量手补。先落一个临时或正式的树遍历器，把每个完整 command
path 的 command Hidden、persistent/inherited/local flags、flag Hidden 生成规范化清单，再逐行做
“部署行为 / 纯输出 / 通用 transport”分类。所有部署/安全行为必须有 drill 或 NOT-COVERED；即使
决定不在表中重复通用 `--home/--nats-url`，也要写明统一排除规则并让生成器验证，而不是静默省略。

### R5-F2 — Major — restore 被错误归入 machine-confirm，负例会因无 TTY 假阳性

附录第 170 行把 machine-confirm 双因子归到 S7-50，并要求“只给 flag 不给 env → 拒”；roadmap
第 513–515 行也给 restore 加了“machine-confirm 缺 env 拒”。实际 `newClusterRestoreCmd` 的
`--confirm-node-id` 是**bundle/目标身份锚**，随后调用
`confirmTypedNodeID(..., machineEscapable=false, "")`：restore 明确不可用
`$TETHER_CONFIRM_NODE_ID` 绕过人工输入。已有 `TestRestoreNeverEscapableEndToEnd` 也钉住这一点。

因此当前 S7-50 负例在无 TTY harness 里会“拒绝”，但原因只是缺少人工 typed-confirm，并不能证明
env 双因子；给正确 env 也仍应拒/要求输入。这个 oracle 会把错误的安全模型测成绿色。

**要求**：从 machine-confirm 行和 S7-50 删除 restore。restore 分别断言：

1. `--confirm-node-id` 必须匹配 manifest/provenance；
2. `--yes` 必拒；
3. 即使 `--confirm-node-id` 与 `$TETHER_CONFIRM_NODE_ID` 同时正确，非交互执行仍不可绕过 typed
   confirm（never-escapable）。

machine-confirm 缺一拒只归属真正传 `machineEscapable=true` 的命令，如 init/raw remove/resnapshot
及由 grow 驱动的 init 路径。

## Doubts and recommendations

- 附录 §3 的生成法只写 `LocalNonPersistentFlags()` + `InheritedFlags()`。为了把 group/root
  persistent flag 的定义位置也写实，生成器还应采集 `PersistentFlags()`（或等价去重集合）；否则
  `cluster/session/proxy/admin` 根命令的 persistent flags 只能偶然在子命令 inherited 集合中出现。
- 结构生成器被写成“随 S0-台账（S1）提交”。若工程首开批不是 S1，按 S0 唯一归属规则应由首开批
  一并落地，避免 S4/S6 先开工时仍使用手工且不完整的基线。

## Independent verification

已完成：

- 重建 staged rev5→unstaged rev6 边界，通读 roadmap、inventory 和 round-4 回复；回复仅作索引。
- 临时构造 Cobra root，递归输出全部 command path、local/persistent/inherited flags、command/
  flag Hidden；诊断测试通过后已用 patch 删除，最终工作树无该测试。
- 对照 agent/join/doctor/recovery/restore/resnapshot/incident/serve 的实际 flag 注册和安全语义。
- 聚焦测试通过：C8 recovery tree、raw remove manual gate、machine-confirm 双因子、restore
  never-escapable、proxy event、G.1 reconcile、restore reset/idempotence。
- `git diff --check` 通过。

未运行 live simcluster：rev6 没有实现 S0-S9，当前两个 Major 是静态构造树/安全控制流差异，现有
drill 无法为未来 inventory gate 或 S7-50 新负例提供证据。

## Release recommendation

**不放行 rev6。** 先用一次完整树输出系统性重建 §2，修正 restore 安全模型，再做 round-6。
若生成结果与受审表零 diff、每个行为 flag 有归属/裁决，且没有新的可执行性问题，即可放行。

---

# 主进程逐条回复（2026-07-10，roadmap/inventory rev7 落点）

两条 findings + 两条 recommendation 全部采纳。**按你的要求放弃增量手补**：本轮落了同款临时
诊断测试（构造 `newRootCmd()`、递归 `Commands()`、采集 local/persistent/inherited 三集与
command/flag 两级 Hidden），以其完整输出（94 command path、523 行）**逐行转录重建**附录 §2，
测试已删除、`cmd/` 零残留（与你的做法一致，工作树无实现改动）。

- **R5-F1（Major，采纳）**：§2 已按构造树全量重建。你点名的每一项都已成行并给归属/断言：
  `agent join --nid/--pin`、`agent config refresh --session/--once`、`agent doctor --session`；
  `join prepare --name/--nats-server-id/--cert-fp/--secrets-dir`（bundle 身份/provenance 面，
  42 断言 bundle 携真值）、`join approve --timeout`；`cluster doctor --db/--conf/--raft-addr/
  --nats-route`（**preflight 输入面**：50 的 preflight 臂加「错值指向不得判绿」对照）；
  offline force-single `--nats-conf/--nats-server`（决定去集群化写哪份 conf/用哪个 binary
  校验——12/20 隐式走默认，显式/错值负例 plan 定）；resnapshot **`--accept-audit-loss`**
  （显式数据损失开关——42 两臂：有未发布 audit 且不带 → 拒；带 → 截断继续，绝不藏进「等」）
  及 `--data-dir/--db/--self-id/--raft-addr` 全列；restore `--data-dir/--db/--raft-addr`
  （`--raft-addr` = fresh-host IP 变化逃生 → 51 的 DR 臂）；`incident export --sid/--force`
  （`--force` 翻转 O_EXCL——50 顺带「不带时已存在 → 拒」负例）；`rejoin prepare --data-dir/
  --db/--guided`；`diagnose --self-id/--db/--offline`；`reconcile nats --timeout`；
  `serve --nats-url`（部署 seam 例外，行内保留）。**统一排除规则显式化**：仅
  `--home/--nats-url/--socket/--json` 四名可省略、生成器验证任何新 flag 不得静默落入省略，
  两个例外（`login --broker`、`serve --nats-url`）行内保留。§2 头注不再宣称「89 处 Use」，
  改为构造树遍历实录 + round-6 可复跑 diff。
- **R5-F2（Major，采纳）**：restore 已从 machine-confirm 行**移除**——其 `--confirm-node-id`
  是 bundle/目标身份的 **provenance 锚**，`confirmTypedNodeID(machineEscapable=false)` 明确
  不可用 `$TETHER_CONFIRM_NODE_ID` 绕过（`TestRestoreNeverEscapableEndToEnd` 钉住）。rev6 的
  「缺 env 拒」负例确实会因无 TTY 假阳性、把错误安全模型测成绿——已删。S7-50 改为你给的
  **三断言**：① `--confirm-node-id` 与 manifest/provenance 不符 → 拒；② `--yes` 必拒；
  ③ flag+env 同时正确、非交互执行**仍拒**（never-escapable 真栈版）。machine-confirm 双因子
  面限定为 `machineEscapable=true` 的 `add`/`init`/`recovery node remove`/`recovery
  resnapshot`（rejoin prepare 亦无 env 逃生，已从该面排除），负例归 grow（add，日常在用）/
  40（node remove）/42（resnapshot）/43（init）。
- **建议 1（采纳）**：§3 生成法加 `PersistentFlags()` 三集去重采集（group/root persistent
  flag 的定义位置写实，不再只靠子命令 inherited 集偶现）。
- **建议 2（采纳）**：结构生成器归属改为「随 S0-台账**由工程首开批**落地（非绑定 S1）」，
  与 S0 唯一归属规则一致——S4/S6 先开工时不会再用手工基线。

## 复审请求

roadmap rev7 + 清单附录 rev7 + 本回复提交 round-6 复审。round-6 可按 §3 生成法复跑树级
diff（预期为零）。本阶段无产品/脚手架实现变更（临时诊断测试已删，`cmd/` 干净）；未触碰暂存区。
