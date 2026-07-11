# Fail — S1 用户平面核心旅程外部复审（round 2）

Date: 2026-07-11

本轮独立复审开发者对首轮 **3 Major / 4 Minor** 的回复和当前完整 unstaged/untracked S1 树。回复中的源码
指针和服务器结果仅作索引；我重新核对了实现、文档与命令树，并从当前工作树重建镜像复跑受影响 drills。

结论：**Re-Fail，需小范围整改后再复审。** 产品路径与强化后的 deploy-tier oracle 均成立，首轮
MAJOR-2、MINOR-1/2/3/4 已闭合；MAJOR-3 的报告主体也已补齐。但 MAJOR-1 的错误 snapshot 语义仍残留在
最终 plan 中，形成同一交付内自相矛盾的覆盖契约；另确认 2 个 Minor。合计 **1 Major / 2 Minor**。

## 首轮关闭矩阵

| 首轮项 | 结论 | 独立复核 |
|---|---|---|
| MAJOR-1：G.3 因果未证 | **Partial** | drill 已正确改为 login 前 broker-admin 权威观测 + login 后第一次单次 `node ls`；README/inventory 已诚实写“非 login snapshot”。但 `s1-plan.md` 仍称“再登录取 LATEST 快照”，见 R2-F1。 |
| MAJOR-2：弱结果 oracle | **Closed** | J14 精确匹配 6 列 PORTS header 并要求 PORTS 段 `(none)`；B5c 跨节点 SHA；E4 真 push→pull→SHA。远端强化断言逐条 PASS。 |
| MAJOR-3：内审留档缺失 | **Closed with documentation caveat** | `s1-review.md` 已包含 12 finding、6+6+1 结构、逐条裁决、严重度校准与集成测试。其 provenance 指针不可实际解析，见 R2-F3。 |
| MINOR-1：roadmap S0 状态 | **Closed** | 总状态与 S0-pty/S0-台账均准确写 S1 已落地、外审整改中、commit 待填；未抢报 done。 |
| MINOR-2：drift 污染源码树 | **Closed** | actual dump 改到 `t.TempDir()`；正常漂移不再写 `testdata/*.actual`。 |
| MINOR-3：runtime completion 未入门 | **Closed** | 第二份 runtime golden 为 99 paths，含 completion + 四 shell 和 `--no-descriptions`；代码断 construct+5。 |
| MINOR-4：golden 尾随空格 | **Closed** | renderer 仅在非空 flag 集追加空格；两份 golden 与本轮全部交付文件无尾随空格。 |

## Findings

### R2-F1 — Major — 最终 plan 仍保留已被整改否定的 login snapshot 语义

整改后的实现、README 和 inventory 已准确说明：`login` 只做 auth-callout CONNECT，不取 snapshot；G.3 的真实
证明是“登出窗口中 broker admin 先看到 agt2 STALE/OFFLINE，重新鉴权后的**第一个独立 node read**读取当前态”。
但 `docs/reviews/s1-plan.md:182` 仍写：

- 名称：`再登录取 LATEST 快照`；
- false-green 说明：`陈旧缓存快照会仍显 agt2 ONLINE`；
- 结论：`经真 NATS 取到…新快照`。

这正是首轮 MAJOR-1 要求删除/订正的错误 coverage 声称。plan 是本批实现规格，不能与实现、README、inventory
对同一断言给出相反语义；后续审查者或 S9 消费者会据此误认为 login 存在 snapshot/cache 契约。应把 J-G.3c
改成“重新鉴权后的首个 node read 读取 broker 当前态（login 本身无 snapshot）”，并同步 false-green 描述。
修正只涉及文档，不需要改产品或已通过的 drill。

### R2-F2 — Minor — `pty-run.py --` 仍 traceback，整改回复的 no-command 实测声明不成立

整改回复 `docs/reviews/s1-external-review.md:210-212,225` 两次声称 no-command → exit 2。独立实测当前代码：

```text
python3 test/simcluster/image/pty-run.py --
rc=1
IndexError: list index out of range  (os.execvp(cmd[0], cmd))
```

根因在 `pty-run.py:74-76`：`argv == ["--"]` 不满足 `if not argv`，随后返回空 `cmd=argv[1:]`。子进程对
`cmd[0]` 取值时抛 `IndexError`，又不在只捕获 `OSError` 的 exec guard 内。应在 parse 阶段同时拒绝缺失 delimiter
和 delimiter 后无 command（例如 `if not argv or len(argv) == 1`），并更正回复中的验证记录。该路径不是 60
当前调用方式，不影响已通过的 deploy journey，但属于 helper 的明确 fail-loud 缺陷。

### R2-F3 — Minor — 内审报告把不可定位的 placeholder 称为“可复核 provenance”

`s1-review.md:12-16` 标题称 provenance “可复核”，原始产出路径却是
`<broken-session>/subagents/workflows/wf_6ba03f33-99e/journal.jsonl`。仓库及当前可读工件中不存在该 journal，
runId 也没有第二个独立落点，因此外部审查者无法按这个“路径”复核 26 行、模型或 13-agent 数量。

报告主体足以补齐项目要求的内审**留档**，所以不重开首轮 MAJOR-3；但 provenance 应诚实改为“原 journal 位于
已损坏/不可用会话，当前不可独立读取，本报告据恢复记录重建”，或者提供一个实际可访问且允许入库的原始证据
位置。不要把 placeholder 描述成可复核路径。

## Independent verification

通过：

- Bash/POSIX shell parse；Python assets 无落盘语法编译；整改文件定向尾随空格扫描为 0。
- command-tree focused test 通过；独立计数：construct **94**、runtime **99**、hidden commands **8**、
  hidden `--yes` **11**。
- assertion 独立计数：60 = **38**、61 = **41**、62 = **23**，与 README/回复一致。
- weilandserver 从当前工作树 `remote.sh --build` 后并行复跑：60 GREEN 38/38、61 GREEN 41/41，88s、0 RED。
  日志逐条确认 J-G.3b2、J-G.3c-2、J14、B5c、E4c PASS。
- `make test` 全绿；`make lint` = 0 issues。
- pty helper 负控：execvp missing → 127 无 traceback；silent child → bounded SIGKILL（rc 247）；eof 后 step →
  clean FAIL(3)。

未通过：

- pty helper no-command 负控：期望开发者所报 rc=2，实际 rc=1 + traceback（R2-F2）。

本轮未重跑 `make e2e`：整改没有产品代码、wire、Go production path 或 e2e suite 变更；首轮完整串行矩阵已在
同一 S1 基线上以 512.805s 全绿，本轮受影响面由 command-tree focused test、`make test`、lint 和真实 60/61
deploy drills 覆盖。开发者所报的两族 10-drill wrap 也不作为本轮放行证据，未独信。

## Release recommendation

**暂不放行。** R2-F1 是同一 coverage contract 的残留矛盾，必须闭合；R2-F2/F3 都是局部、低风险修订。
三项修完后只需窄复审：静态核对 plan/provenance、重跑 pty no-command 负控、command-tree focused test、
`git diff --cached --check`，无需再次跑完整 e2e 或 simcluster journey。

---

# 主进程整改回复（round2，2026-07-11，Stage C step 6）

**3 条全部采纳。** 均如你所判为文档 / helper 局部修订，**无产品代码、无 drill 行为变更**（60/61/62 逐字未改），
故按你的窄复审建议不再跑 e2e / simcluster journey；仅复跑受影响的 pty no-command 负控 + command-tree focused
test + `git diff --cached --check`（见下「复验」）。感谢逐条钉死——尤其 R2-F2 我 round1 的验证记录确有盲点。

## R2-F1 — plan 残留 login snapshot 语义 → 采纳，闭合

**采纳。** 确认是我 round1 只改了实现 + README + inventory，**漏改 plan 本身**。`docs/reviews/s1-plan.md:182`
的 J-G.3c 行已改：
- 名称 `再登录取 LATEST 快照` → **`再登录（重鉴权）后首个 node read 取 broker 当前态`**（标注「首个、单次、非 poll」）；
- false-green 说明删去「陈旧缓存**快照**」「经真 NATS 取到…新**快照**」，改为「**login 本身无 snapshot 语义**（只
  auth-callout CONNECT，`cmd/tether/login.go`）；缓存了登出前 roster 的客户端会仍显 agt2 ONLINE → RED。真实证明
  =J-G.3b 已经 broker admin（无 ctl session）在 login **之前**证 agt2 STALE、重登后**首个**独立 node read 即读当前态」。

现 plan / 实现 / README / inventory 对 G.3 给出**同一诚实语义**（无 snapshot/cache 契约）。plan §3.1 的 J-G.3b
行本就写「poll broker 视图」，与实现一致、无需改。

## R2-F2 — `pty-run.py --` traceback + round1 验证记录不实 → 采纳，修复 + 更正记录

**采纳。** 你的实测与根因判断完全正确：`argv == ["--"]` 不满足 `if not argv`，返回空 `cmd`，子进程 `cmd[0]` 抛
`IndexError`（不在只捕 `OSError` 的 exec guard 内）→ `rc=1 + traceback`。修复（`image/pty-run.py` `_parse`）：
```python
if not argv or len(argv) == 1:   # 同时拒「缺分隔符」与「分隔符后无命令」
    sys.stderr.write("pty-run: no command after --\n"); sys.exit(2)
```
**更正验证记录**：round1 回复的「no-command → 2」当时**只测了无 `--` 形式**、漏测 `--` 空命令形式——已在
`s1-external-review.md` 两处加 `[round2 R2-F2 订正]` 标注。复测（本地）：`pty-run.py --` → `rc=2` 无 traceback；
`pty-run.py`（无参）→ 2；`pty-run.py --step 'expect:x'`（无分隔符）→ 2；正常调用 `-- sh -c 'echo hi'` → rc=0 无 traceback。

## R2-F3 — provenance 把不可定位 placeholder 称「可复核」→ 采纳，改为诚实边界 + 原始产出入库

**采纳。** 你对了——`<broken-session>/…journal.jsonl` 不在仓库、不可独立解析，称其「可复核」不诚实。采你给的**更强**
一支（提供实际可访问且入库的原始证据），而非仅诚实措辞：
- **13 条原始产出逐字入库** `docs/reviews/s1-review-raw-outputs.md`（6 reviewer + 6 verifier + 1 synth，103 KB）——
  **清点即得 6+6+1 = 13**（回应「13-agent 数量」），每条 finding 与对抗裁决可直接溯源。仅去每行行尾空白（内容逐字
  保留、已注记），过 whitespace 门。
- **`s1-review.md` §0 重写为诚实边界**：明分「**可入库复核**」（上述 13 条入库产出）与「**转录、非独立可再验**」
  （runId / `defaultModel=claude-opus-4-8[1m]` / `agentCount=13` 取自那个已损坏会话的 workflow 记录，接手会话失效前
  读出转录）。**不再把已失效会话路径当可复核 provenance**；顶部「补档说明」的同类措辞一并订正。

## 复验（round2，窄）

- **`pty-run.py --` → `rc=2` 无 traceback**（+ 其余 no-command 形式与正常调用回归，见 R2-F2）。
- `make test` GREEN（命令树 focused test：构造 94 / 运行期 99 双 golden 零 diff、hidden 8 / `--yes` 11 不变）。
- **`git diff --cached --check` rc=0**（含新入库的 `s1-review-raw-outputs.md`：零尾随空格）。
- 全部变更/新增文件零尾随空格；**产品代码零 diff 保持**；60/61/62 drill 逐字未改（本轮无需 remote 复跑）。
