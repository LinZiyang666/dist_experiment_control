# S1 内审报告 — 用户平面核心旅程（Stage C step 4–5）

Date: 2026-07-11. Flow: CLAUDE.md §3（3 阶段 7 步）step 4（多专家对抗审查）+ step 5（主进程逐条裁决 + 修复 + 集成测试）.
Plan: `docs/reviews/s1-plan.md`. 覆盖清单 SSOT: `docs/reviews/simcluster-coverage-inventory.md`.

> **补档说明（外审 MAJOR-3）**：本报告在 S1 外审阶段补齐——内审 workflow **确已按 CLAUDE.md §3 step 4 运行**，
> 但当时未落盘成 `s1-review.md`，导致审计链不完整。其 **13 条原始产出已逐字入库** `s1-review-raw-outputs.md`
> （§0 Provenance 标注了「可入库复核」与「转录」的诚实边界）。本文件据这 13 条如实重建：完整 finding 集、
> 6 维度专家 + 恒等对抗核验的裁决、主进程逐条采纳/驳回与严重度校准、内审新增的测试、以及一次跨会话接手中
> 发现并修正的「报告 ✅ 与代码不符」偏差。**非事后追记——findings 逐条对应入库的原始产出。**

## 0. Provenance（诚实边界，外审 round2 R2-F3 订正）

**可入库复核者 vs 转录者，分明标注**：

- **可入库复核**：内审 workflow 的 **13 条 subagent 原始产出**（6 reviewer + 6 verifier + 1 synth）已**逐字入库**
  于 `docs/reviews/s1-review-raw-outputs.md`——**清点即得 6+6+1 = 13**（回应「13-agent 数量」）、每条 finding 与
  对抗裁决可直接溯源。**恒等映射**：每 reviewer 一个对抗 verifier。脚本用固定宽度 `parallel`/`pipeline`，6→6→1
  三阶段 agent 数均编译期常量（满足 CLAUDE.md「每阶段 agent 数静态固定」）。
- **转录（非独立可再验，诚实标注）**：Workflow 记录 `wf_6ba03f33-99e.json`（runId、`agentCount=13`、
  `defaultModel=claude-opus-4-8[1m]`、`status=completed`、脚本无 `model:` 覆写 → 全继承会话 Opus 4.8）与原始
  `journal.jsonl` **都是那个已损坏会话的会话本地 Claude Code 工件，不在仓库、当前不可独立读取**。上述 runId /
  模型 / 数量元数据系**接手会话在其失效前读出后转录**，非独立可再验；本报告与入库产出即据此重建。**不把已失效
  的会话路径当作可复核 provenance**——可复核的是上一条的入库 13 条产出。

## 1. 审查结构（step 4）

| Phase | Agent 数 | 职责 |
|---|---|---|
| Review | 6 | 6 个独立维度各 1 reviewer，只读实现、可自行新增测试条目、**绝不改实现** |
| Verify | 6 | 恒等映射，每 reviewer 一个对抗 verifier，独立复核源码、判 CONFIRMED/REFUTED/NEEDS-OWNER |
| Synthesize | 1 | 汇总 CONFIRMED findings、去重、most-severe-first |

**6 个 reviewer 维度**：① coverage/inventory/ledger 真相；② 并发/泄漏/host-safety（`-j` 并行 drill-all）；
③ Mandate ①–④ + 保真度；④ product-truth（反码/退出码/列/flag/阈值）；⑤ harness-engineering + robustness；
⑥ false-green/vacuous-pass 全局猎捕。

## 2. 核验裁决汇总（step 4 verify）

**12/12 CONFIRMED，0 REFUTED，0 NEEDS-OWNER。** 每条 finding 由其维度的对抗 verifier 独立复核源码后确认；
无一被驳回。S1-01（62 头注反码）被 4 个 reviewer（③④⑤⑥）独立发现。核验另修正一处 reviewer 过度声称
（「`remote_fs_not_found` 全仓只这两行」实为 ~8 处，准确说法是「仅在 simcluster drill 树内」——核心矛盾不受影响）。

## 3. 12 条 CONFIRMED finding + 主进程逐条裁决（step 5）

**全部 12 条采纳（都是真缺陷）。** 严重度经主进程校准（见 §4）。most-severe-first：

| ID | 严重度 | 位置 | 缺陷一句话 | 主进程修复 |
|---|---|---|---|---|
| S1-02 | major | `drills/lib/agentyaml.sh` | ONLINE poll 蒙旧进程残留 ONLINE 行（心跳驱动、StaleAfter=5s），无法证明新 yaml 生效 | STOP→poll 老行离 ONLINE→START→poll 新 ONLINE；加 `badkey` 负向自测 + 61 G0 断言 |
| S1-01 | major | `drills/62-remote-fs-safe.sh:13,15` | 头注 reason 写错 `remote_fs_not_found`（实际断言 `remote_fs_unsafe_cwd`），重记已修的假绿机理 | 头注改 `remote_fs_unsafe_cwd` + 真机理（lexical cwd fail-fast，argv[0] 解析前） |
| S1-04 | major | `README.md` OQ-8 | `-j 6`「family-wave cap」误述——`-j` 无 family 感知、反把 5 个 grow 前载到 wave 1 | 订正 README + 改按族两 pass；同步 `s1-plan.md` §6/§7 |
| S1-03 | moderate | `drills/61-transfer-edges.sh` C1 | tier-A 断言 grep 预传输 banner、丢弃 push rc（管道退出=grep），被拒也绿 | 捕获 push rc + 加 C1b `_landed` 落地锚 |
| S1-05 | minor | `drills/62-remote-fs-safe.sh` | `_mount_hangfs`/`_heal` 用固定 sleep 作 readiness（违反 poll_until 铁律、flake） | 改 `poll_until`（mount / statfs healthy） |
| S1-06 | minor | `drills/62-remote-fs-safe.sh` | 3 处 alive-control 未包 timeout（真 wedge 时 exec 默认 10min 挂死，占 slot） | 3 处包 `timeout 25` |
| S1-07 | minor | `image/pty-run.py` | drain-idle 路径落到阻塞 `os.waitpid`，fail-loud 自保不完整 | drain-idle break 时 SIGKILL+reap 子进程 |
| S1-08 | minor | `simcluster-coverage-inventory.md:234` | null-diff 文案误称 `login` 不发 sys.events；实际 PIN 首连发 `member_joined` | 移出「不发」列、注明 exercised-not-asserted、归 S2-80 |
| S1-09 | nit | `drills/60-user-journey.sh` J5 | 描述称「ordered」但只 grep 成员（非位置），tail-first 也过 | 改 `HEADx+TAILxyz` 断言有序+连续 |
| S1-10 | nit | `drills/62-remote-fs-safe.sh` | `assert_ok "…NOT-COVERED…" true` 自声明 GREEN、测不了什么 | 改为 live 测量 `_statfs_healthy`（wedge 经 SIGCONT 可逆=非真-D 的实证） |
| S1-11 | nit | `image/pty-run.py` | eof 后触 fd 的 step 崩在 fd=-1（traceback 而非 clean FAIL） | eof 后 `master<0` guard → clean FAIL(3) |
| S1-12 | nit | `drills/62-remote-fs-safe.sh:33` | `_cleanup_hang` 的 `pkill -9 -f hangfs.py` 自匹配 wrapper 自身 cmdline | 改 `[h]angfs.py` 经典自排除技巧 |

（每条 finding 的完整源码级论证见原始 journal 的 synth result；本表为主进程定稿摘要。）

## 4. 严重度校准裁定（主进程对 verify 阶段的回复）

- **N-1（S1-01 major vs verifier 的 minor）**：主进程定 **major**——它在「verified live」时间戳下重记了刚修的
  假绿，未来一次「订正」就会把 :13/:15 改回 `remote_fs_not_found` 重开假绿，正中 Mandate「断言 DOCUMENTED cause」。
- **N-2（S1-02）**：**major**（fixture 层）——守卫不健全且注释是虚假保证；虽当前 blast radius 被下游 live-daemon
  断言兜住，但守卫本身必须真能抓 bad reprovision（故加 `badkey` 负向自测钉死）。
- **N-3（S1-03）**：**moderate**（verifier 与主进程一致，从 reviewer 的 major 下调——live 暴露有界）。
- **N-4（S1-06 scoping）**：alive-control 的职责就是探 wedge，头注又是无条件「every wedge-touching command
  bounded」，故一律包 timeout。
- **N-5（S1-04 remedy）**：两者都做——README 误述改掉 + 建议按族分 pass（VOTER-timeout 不进 `FLAKE_SIG` 是有意，
  故 `-j 6` 反而放大需手动单跑的 timeout）。

## 5. 内审新增/强化的测试（step 5 集成）

- **S1-02**：`agentyaml.sh` 的 restart-took-effect 门（STOP→离线沿→START→ONLINE）+ **`badkey` policy 负向自测**
  + `61` 的 **G0** 断言（malformed yaml 必使 fixture 返非零）。
- **S1-03**：`61` 的 **C1b**（`_landed /home/sim/u.bin`）+ C1 单独捕 push rc。
- **S1-09**：`60` J5 改 `HEADx+TAILxyz`（断有序+连续）。
- **S1-05/06/10/12**：`62` 改 poll_until readiness、alive-control 包 timeout、NOT-COVERED 标记改 live `_statfs_healthy`、
  `[h]angfs.py` 自排除。

## 6. 跨会话接手 + 一处「报告 ✅ 与代码不符」的修正

内审 step 5 的修复由前一会话开始、中途会话损坏；后继会话接手时**逐条对照代码现状 vs 交接报告**，发现：

> **S1-10**：交接报告标 ✅「已改为 `_statfs_healthy`」，但 `62-remote-fs-safe.sh` 代码**仍是 `assert_ok "…" true`**
> ——主进程决策未真正落地（reviewer ③ 与 verifier ⑤ 的原始产出也记录当时仍是 `true`）。接手会话已补落地为
> `_statfs_healthy`（wedge 经 SIGCONT 可逆的 live 实证）。

其余 11 条经代码核实确已落地。此偏差印证了「审计链留档 + 逐条对照」的必要性（正是外审 MAJOR-3 的关切）。

## 7. 修复后验证（step 5 收尾）

- **三 drill 在 weilandserver 实跑 GREEN**（重建镜像烘焙新 `pty-run.py` 后）：`60`/`61`/`62` 全 GREEN；服务器日志
  逐条确认 `61` G0 负向自测 PASS、`62` 全臂（含 `_statfs_healthy` live 标记）PASS。
- **`make test` GREEN**（含命令树守恒 golden 零 diff）。
- **`pty-run.py` 两 latent 守卫本地主动烟测**（drill 不触发 eof/drain-idle）：eof-after-step → clean FAIL(3) 无 traceback；
  silent-but-alive 子进程 → SIGKILL、wall≈idle-timeout（非挂死）。

> **注**：本报告完成后进入外审（step 6）。外审判 Fail（3 Major / 4 Minor），主进程在
> `docs/reviews/s1-external-review.md` 内逐条回复并整改；本内审报告的补齐即为其 MAJOR-3 的闭合。
