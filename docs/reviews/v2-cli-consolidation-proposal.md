# v2 CLI 精简提案——cluster 命令面收敛（随 C1–C7 边建边收）

> 输入：现状 `tether cluster` 命令面（20 子命令 / 4 组）+ `v2-automation-program.md`（C1–C7 会新增的命令）。
> 目标：C1–C7 落实**所有非拒绝需求**的同时，让 cluster CLI **净命令数下降、day-2 路径变短**，而不是膨胀。
> 边界：精简**不弱化任何安全护栏**（typed-confirm、副本 target、fail-closed、no-auto-elect）；被删的自动化命令保留 `--manual`/`recovery` 逃生。

## 1. 动机

C1–C7 若**只加不减**，会把命令面从 20 个推到 30+，"cluster 命令太复杂"会更严重。但今天命令多，**恰恰是因为缺 operation / reconciler 层**——底层手动原语（两段式 `add` + `sign-join`/`node-pub`/`keygen`、每台 `takeover-natsconf`）被平铺到顶层让人手敲。C3（reconciler）+ C4（operation controller）补上自动化层后，这些原语应当**折叠进 operation 动词**。

**核心判断：实现 C1–C7 本身就是最大的精简杠杆。只要"边建新层边删旧原语"（不新旧并存），净命令数能降不升。** cluster CLI 是 **v2-only**（不发 v1 车队），删命令**零兼容负担**。

## 2. 现状盘点：`tether cluster` 已有 20 子命令（4 组）

| 组 | 子命令 |
|---|---|
| online | `status` `add` `drain` `remove` `transfer-leader` `rotate-tunnel-cert` `wait` `backup` `export-incident` `ops`(ls/show) `apply` |
| migrate（一次性） | `init` `takeover-natsconf` `doctor` |
| escape（危险） | `force-single` `recover` `restore` |
| local（crypto） | `sign-join` `node-pub` `keygen` |

（`proxy` on/off/status/sub、`expose`、`agent` 另计。`cluster_secrets.go` 是内部 helper，无面向用户子命令。）

## 3. 三类精简

### A. 删除 / 降为内部步骤（被 operation+reconciler 替代）

| 现命令 | 被谁替代 | 处置 | 落在哪阶段 |
|---|---|---|---|
| `cluster add`（无 token→拿 nonce→re-run 两段式手动舞） | C4 `join prepare`/`approve` + C2 invite | **删顶层**，逻辑进 operation | C4 |
| `cluster sign-join` | 变成 `join prepare` 内部一步 | **删**（`local` 组消失） | C4 |
| `cluster node-pub` / `keygen` | `join prepare` 自动生成/读取 | **降为 hidden debug**（`--advanced`） | C4 |
| `cluster takeover-natsconf`（每台手动渲 nats.conf） | C3 reconciler 自动渲 + C4 `reconcile nats` | **删 routine**，留 `reconcile nats --manual` 逃生 | C3 |
| `cluster remove`（文档已写"prefer drain --retire"） | C4 `cluster retire` operation | **降为 `recovery node remove --manual` raw 逃生** | C4 |
| `cluster wait <node>`（独立命令） | C4 各 operation 的 `--wait` + `ops show` | **折叠**进 `--wait`，删独立命令 | C4 |

**净效果：6 个顶层命令消失/降级，整个 `local` 组（3 个）清空。**

### B. 合并（重叠的多个入口收成一个）

| 现状（多入口） | 合并为 | 落在哪阶段 |
|---|---|---|
| `apply -f roster.yaml`（plan-only 不执行）+ C4 `plan add`/`apply <plan-id>` | **一对** `cluster plan <…>` + `cluster apply <plan-id> --wait`（不留 roster-diff 和 plan-id 两套） | C4 |
| `reconcile nats` + `takeover-natsconf` | **一个** `cluster reconcile nats`（自动），手动版降 flag | C3 |
| `drain --retire`（retire 寄生在 drain 的 flag 上） | 拆成 `cluster drain`（迁走 expose、保留 voter）+ `cluster retire`（完整退役 operation）两个清晰动词 | C4 |
| 散在顶层的 `force-single` `recover` `restore` `export-incident` + C6 提议的 `recovery diagnose/force-single/rejoin` 别名 | **收进一个 `cluster recovery` 子命名空间**（别"顶层 4 个 + 再加 4 个别名"） | C6 |
| `status` + proxy/expose 的 home 视图 + `--homes`/`--cluster`/`--explain` | **一个 `cluster status` 带 flag**，不为每个视图开新命令 | C5/C6 |

## 4. 目标命令树（C1–C7 全做完后）

```
tether cluster
├─ status [--homes|--cluster|--explain|--json]   ← 唯一健康/拓扑视图
├─ doctor                                          ← 在线诊断 / init 前 preflight
├─ membership（operation 化，可恢复、带 --wait）
│   ├─ join prepare        （吃掉 add/sign-join/node-pub/keygen）
│   ├─ join approve --wait
│   ├─ drain <node>        （迁 expose、保留）
│   ├─ retire <node> --wait [--compromised]   （吃掉 drain --retire；C7 接 rotation）
│   └─ transfer-leader <node>
├─ plan <add|retire|…>     ＋  apply <plan-id> --wait   （吃掉 apply -f roster）
├─ reconcile nats --all --wait      （吃掉 takeover-natsconf 的 routine 用法）
├─ ops ls | show <id>      （真 operation 日志）
├─ backup                  （DR）
├─ recovery               ← 把所有 escape 收进来
│   ├─ diagnose --offline
│   ├─ force-single --confirm-peers-dead
│   ├─ rejoin prepare --dump-divergent   （= 旧 recover）
│   ├─ restore <bundle>
│   ├─ incident export                    （= 旧 export-incident）
│   └─ node remove <id> --manual          （= 旧 remove，raw 逃生）
└─ init --from-existing    （一次性迁移）
```

**顶层从 20 个平铺命令 → 约 8 个动词 + 2 个子命名空间（membership / recovery）。** 日常 day-2 路径从"敲一串手动命令"缩成 `join prepare`/`approve` 和 `retire --wait`。

agent 侧 C1/C2 新增的 `agent join` / `agent config refresh` / `agent doctor` 是净新增、必要，不在删减范围。

## 5. 阶段集成（精简动作绑进各阶段验收，边建边收）

| 阶段 | 新增 | **同阶段必须删/合并（验收项）** |
|---|---|---|
| C3 Topology reconciler | `reconcile nats` 自动渲 | `takeover-natsconf` 降为 `reconcile nats --manual`（不再 routine） |
| C4 Operation controller | `join prepare/approve`、`retire`、`plan/apply`、`ops show` | **删** `add`、`sign-join`；**降** `node-pub`/`keygen` 为 hidden；`drain --retire` flag 拆出 `retire`；`apply -f roster` 并进 `plan/apply`；删独立 `wait`；`remove`→`recovery node remove --manual` |
| C5 Proxy cluster 化 | `proxy status --cluster`、`status --cluster` | proxy 视图走 `status` flag，不新开顶层命令 |
| C6 可观测 + 命名 | `recovery` 子命名空间 | `force-single`/`recover`/`restore`/`export-incident` **迁入** `cluster recovery *`（顶层保留一轮 deprecated 别名提示，下个 tag 删） |

## 6. 落地纪律（不然会更复杂）

1. **折叠随阶段同步做，绝不新旧并存**：C4 建 `join prepare/approve` 的**同一 PR 内删 `add`/`sign-join`**——否则用户面对"两套加入方式"反而更乱。
2. **每个被删的自动化命令保留一条逃生**（`reconcile nats --manual`、`recovery node remove --manual`、`transfer-leader`）：happy path 干净，reconciler 失灵时人能接管——符合"自动化必须有手动兜底"。
3. **安全护栏一字不弱化**：合并后的 `retire`/`force-single`/`recovery *` 继续 typed-confirm、副本 target、rebuild-OFF 枚举、no-auto-elect、fail-closed。
4. **顶层迁移留一轮 deprecated 别名**：被迁进 `recovery` 的命令在顶层保留一个 tag 周期的 hidden 别名 + stderr deprecation 提示，再删——给运维肌肉记忆缓冲（runbook 同步更新）。

## 7. 完成定义（DoD）

C1–C7 全部完成后：
- `tether cluster --help` 顶层 ≤ ~10 项（含 2 个子命名空间），与 §4 目标树一致。
- 加 broker 的 day-2 路径 = `join prepare`（新机）+ `join approve --wait`（leader），**无 `add`/`sign-join`/`node-pub`/`keygen`/`takeover-natsconf` 手敲**。
- 所有 escape 在 `cluster recovery *` 下；顶层无散落 escape。
- gap 文件需求全 ✅（本提案不改 `v2-automation-program.md` 的需求覆盖，只规定命令面如何收敛）。

> 本文件是 `v2-automation-program.md` 的**配套约束**：自动化计划定义"做什么需求"，本提案定义"命令面怎么同步收敛"。两份一起执行。
