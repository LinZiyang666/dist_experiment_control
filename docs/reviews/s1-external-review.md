# Fail — S1 用户平面核心旅程外部审查

Date: 2026-07-11

审查对象为 `HEAD` 之外全部 unstaged/untracked S1 内容。内部 plan、源码中的 `S1-xx` 订正号、inventory
landing 声明与既有实跑结论只作索引；本报告的结论来自独立源码复核、提交前全量门和从当前工作树重建的
weilandserver 单 drill 实跑。

结论：**Fail，整改后复审。** 三条新 deploy-tier journey 的真实 happy/refusal paths 全部能运行，且
`make test` / `make e2e` / `make lint` 全绿；但当前交付仍有 **3 Major / 4 Minor**。核心问题不是产品路径
跑不通，而是两处 coverage 声称没有被相应 oracle 证明，以及 S 系列自己的流程/状态真相源没有闭合。

## Findings

### MAJOR-1 — “登出窗口变化由 re-login 取到最新快照”的因果关系没有被测试

`60-user-journey.sh:61-73` 在 stop agt2 后立即执行 `login`，直到 **login 已完成之后**才用
`poll_until 45` 等待 agt2 变成 STALE/OFFLINE。`login` 的实现只做一次 auth-callout CONNECT、关闭连接、
写 `current_session`；它不取 node snapshot（`cmd/tether/login.go:63-103`）。随后每次 `node ls` 又是一个全新的
进程、CONNECT 和 `node.list.req`（`cmd/tether/node.go:37-70`）。因此当前测试在以下错误实现下仍会 GREEN：

1. re-login 完全不反映登出窗口状态；
2. login 后过几秒，后续独立的 `node ls -a` 才自然看到 heartbeat 过期；
3. `poll_until` 把这个 post-login 变化误标成 “re-login snapshot”。

这与 plan 的明确步骤“**先 poll broker 侧视图 agt2→STALE/OFFLINE 后再 login**”以及 inventory/README 的
G.3 landing 声称不一致。应在登出态使用不依赖 ctl active-session 的权威 broker/admin/DB 观测先证明状态
已经变化，再执行 login，并把 login 后的**第一次** node-list 结果作为 oracle；若产品的 login 本来就没有
snapshot 语义，则应删除“re-login 取最新快照”的错误 coverage 声称，改成它实际证明的“重新鉴权后首个命令
取得新状态”。

### MAJOR-2 — 两个已登记为覆盖的结果 oracle 允许明显 false-green

`60-user-journey.sh:140-142` 声称 J14 证明“N=1 PORTS 表为空且无 HOME 列”，实际唯一条件是输出中任意位置
出现字符串 `PORTS`。即使表结构多出 HOME、分配了意外端口、缺少 `(none)`，甚至只是错误/banner 含有
`PORTS`，断言仍通过。plan 要求的“6 列表头 + `(none)` + no HOME”没有落地，inventory 却已据此勾
`PORTS 表头〔N=1 无 HOME 列〕`。

同类问题出现在 `61-transfer-edges.sh:76-78,116-119`：B5b 名为“`--force` overwrites dst”，却只看 push
退出 0，不比较目标与源的 hash/size；E4 名为“round-trip”，却只 push 后 `test -s`，没有 pull/hash。一个
错误的 no-op success 会同时通过这些断言。应把结果 oracle 补齐后再维持当前 coverage landing：J14 精确解析
表头/空行；B5b 对目标做 SHA-256；E4 真正 pull 回来并比较 hash，或把名称/声称收窄为“恢复后 push 可用”。

### MAJOR-3 — 强制内审留档缺失，S1 的 Stage C 审计链不可复核

`CLAUDE.md:36-38` 和 roadmap `docs/simcluster-coverage-roadmap.md:14-15` 要求实现后先做对抗内审、形成
`docs/reviews/s<N>-review.md`，再进入外审。当前 `docs/reviews/` 只有 `s1-plan.md`，没有 `s1-review.md` 或任何
round 报告；但 plan、README、inventory 和实现注释已经引用 `S1-02`、`S1-04`、`S1-08`、`S1-12` 等“内审
订正”。外部审查无法知道完整 finding 集、主进程逐条采纳/驳回裁决、哪些新增测试来自内审，也无法验证
Workflow 模型/数量约束。

这不因本次外审独立重查而豁免：外审不能替代项目强制的内部审计记录。应补回真实的 S1 内审报告与主进程
逐条裁决；如果内审事实上未按规定运行，则应先完成 Stage C step 4/5，再提交复审。

### MINOR-1 — roadmap 的 S0 状态 SSOT 与已落地文件直接矛盾

roadmap 明文规定 S0 项“落地后在本表状态列登记，后续批据此判定已就绪”
（`docs/simcluster-coverage-roadmap.md:194-199`），但同文件 `:209-210` 仍把 S0-pty 标为“未落地”、把
S0-台账除旧 inventory 外标为“其余未落地”；文件头 `:11` 仍称整个 roadmap“未开工”。与此同时本次已经
提交 `pty-run.py`、gotcha ledger、README 重构、永久 command-tree gate，并在 inventory 写成 landing。
这会让 S9 或下一首开判断重复实现/错误依赖。应更新 S0 两行（含落地批与待填 commit），并把总体状态改成
能表达“S1 已落地、S2–S9 未开工”的准确文字。

### MINOR-2 — command-tree gate 在预期的漂移失败上污染源码树

`cmd/tether/command_tree_inventory_test.go:54-61` 在 golden 不一致时无条件写
`cmd/tether/testdata/command_tree_golden.txt.actual`。CLI 合法变更本来就应先让此测试失败，因此这不是异常
边缘路径；它会让普通 `go test ./...` 留下 untracked 文件，并可能被本项目收尾规定的 `git add -A` 误暂存。
建议用 `t.TempDir()` 写诊断文件并在错误中打印路径，或直接输出一个短 unified diff；至少应在成功路径清除
陈旧 `.actual`，但不应把测试诊断产物放进源码树。

### MINOR-3 — runtime completion 面不在“永久完整命令树门”内

golden 的 94 paths、8 hidden commands、11 个 hidden `--yes` 独立计数均正确；local/inherited flag 去重和
排序也正确。但测试只遍历 `newRootCmd()` 的构造树，Cobra 运行期注入的 `completion <shell>` 子树及
`--no-descriptions` 不在 golden 内。当前只有文件头注释和 drill 60 的宽松 `completion bash` 烟测，无法让
completion path/flag/Hidden 漂移触发这个结构门。roadmap round-6 已特别要求把“构造树 94 / 运行期 99”
convention 写进测试。建议另加一个显式初始化 default completion 后的计数/结构断言，或把 runtime tree 纳入
第二份 golden；不要把当前 gate 描述为覆盖“every command path”。

### MINOR-4 — 新 golden 自身使完整 diff-check 失败

暂存全部文件后，`git diff --cached --check` 报告
`cmd/tether/testdata/command_tree_golden.txt:1,8,12,74,94` 有 trailing whitespace。这五行都是无 flag 的
command，生成器固定拼接 `"\tflags: "+strings.Join(...)`，空集合仍留下行尾空格。因此当前提交不满足 plan
和 tasklist 的 whitespace 门。应让 renderer 在空 flag 集时输出无尾空格的稳定形式（例如 `flags:`），重新
生成 golden，并确认 `git diff --cached --check` 为 0。

## Doubts and recommendations

- `62-remote-fs-safe` 对 FUSE T-state、statfs block、auto/off+safe fast-fail、alive control 和 SIGCONT 恢复的
  证据链完整；真 D-state 与 mode:off-without-safe 仍是诚实的 NOT-COVERED。我的疑问仅是未来何时有专用隔离
  宿主承接它，当前不阻断 S1。
- `pty-run.py` 的核心 PTY/winsize/resize/Ctrl-C 路径经真实跨容器实跑成立。建议后续补极小的 helper 自测，覆盖
  无 command、execvp 失败、expect timeout、eof 后 step 与 silent-child；当前 execvp 失败会打印 Python traceback，
  不会按注释意图稳定返回 127。
- 本轮按变更比例单跑了 60/61/62，没有重新跑未修改的 grow/force-single 五 drill；plan 的两族 whole-suite
  wrap gate 仍应由整改方在最终收尾提供可追溯日志。新 N=1 drills 本身无 grow 语义。

## Independent verification

通过：

- Bash/POSIX shell parse；两份 Python asset 以 `compile()` 做无落盘语法校验。
- `go test ./cmd/tether -run TestCommandTreeInventory -count=1`；独立计数 = 94 paths / 8 hidden commands /
  11 hidden `--yes`，与 golden/inventory 一致。
- `make test` 全绿。首次沙箱内运行因多个 embedded NATS 无权限打开监听 socket 而失败；在沙箱外按相同命令
  重跑全绿，判定为审查环境限制而非产品失败。
- `make e2e` 全绿，`test/e2e` 总耗时 512.805s；P1–P13、D1–D9、phase-fluidity、remote-fs、proxy-tunnel
  reconnect 等矩阵全部通过。
- `make lint`（golangci-lint v2.5.0）= `0 issues`。沙箱内 package loader 返回空 context，沙箱外同命令通过。
- weilandserver 独立预检：Linux 6.8、Docker 29.6.1、根盘约 839 GiB 可用、
  `fs.inotify.max_user_instances=8192`、开跑前无遗留容器。
- 从当前工作树 `remote.sh --build` 重建/同步后单跑：`60-user-journey` GREEN **37/37**，
  `61-transfer-edges` GREEN **38/38**，`62-remote-fs-safe` GREEN **23/23**；三轮均正常 nuke。
- 61 内建 malformed-yaml G0 负控真实触发并被 freshness gate 捕获；这验证了 daemon reprovision 的关键负控，
  但不影响上文对其它 oracle 的静态反例。

未通过：

- 完整暂存后 `git diff --cached --check` 因 MINOR-4 的 5 行 golden 尾随空格失败；未把未跟踪文件纳入比较的
  早期 `git diff --check` 曾返回 0，不能作为最终提交证据。

## Release recommendation

**不放行 S1。** 先关闭 MAJOR-1/2 的 coverage 真实性问题并补齐 MAJOR-3 的内审审计链；同步 roadmap 状态，
修正测试诊断落盘、runtime completion gate 与 golden whitespace 后做窄复审。产品代码零 diff 边界可继续
保持，不需要为这些 finding 修改产品实现。

---

# 主进程整改回复（2026-07-11，Stage C step 6）

逐条评估：**7 条 finding 全部采纳**（3 Major + 4 Minor），另采纳「Doubts and recommendations」中的 pty-run
`execvp` 失败修复。**产品代码零 diff 边界保持**（无一处产品实现改动——全部是 drill oracle / 测试脚手架 /
文档）。整改后**从当前工作树**重建镜像并在 weilandserver 实跑验证；下方每条附服务器日志证据。

指导原则（与用户重申的 Mandate 一致）：**强化 oracle 是让它真正证明所声称的，不是改弱骗绿**——若强化后
变 RED 即暴露真问题。以下强化 oracle 全部实测 GREEN，证明 tether 行为正确，且现在能抓住旧弱 oracle 漏掉的回归。

## MAJOR-1 — G.3 因果未证 → 采纳，重写为诚实 oracle

**采纳。** 核实 `login` 确无 snapshot 语义（`cmd/tether/login.go`：一次 auth_callout CONNECT → `nc.Close()` →
`WriteCurrentSession`，不取 node 快照）；旧 oracle 在 login **后** `poll_until`，会被 login 后的自然心跳过期
冒充「re-login snapshot」。**注**：plan §3.1 定稿说明本就要求「poll 到 broker 侧视图 agt2 STALE/OFFLINE **后再
login**」——是**实现偏离了 plan**；整改让实现回到 plan 原意，并按你的两条建议同时做（`drills/60-user-journey.sh`）：

1. **登出窗口内经 broker 权威观测证状态已变**（新 `J-G.3b2`）：helper `_agt2_gone_admin` 经
   `exec brk1 -- runuser -u tether -- tether admin nodes`（brk1 本地 admin socket，**不依赖 ctl active-session**）
   `poll_until` 到 agt2 STALE/OFFLINE——钉死变更发生在 login **之前**。
2. **重登后首个 node ls 作 oracle**（`J-G.3c-2`：`poll_until` → **单次** `_agt2_gone`）：既然 b2 已证状态在 login
   前已变，首个 post-login 读若显示 agt2 gone 即证「重连读取当前态」；缓存旧 roster 的客户端会在此显示 ONLINE → RED。
3. **覆盖声称订正**：README:262 / inventory `login` 行 + §2/§4 的 G.3 措辞改为「登出窗口经 broker admin 证 agt2
   STALE、重登**首个**读即反映=重连读当前态，**非 login 取快照**」。

**验证**（服务器日志）：`J-G.3b2 ...authoritative, no ctl session` PASS、`J-G.3c-2 ...reconnected read = current
state; single check, no poll` PASS。60 GREEN **38/38**。

## MAJOR-2 — 假绿结果 oracle → 采纳，补齐真实结果 oracle

**采纳三处**（`drills/60`、`drills/61`）：
- **J14（PORTS）**：弃「输出含 `PORTS` 字符串」的松 grep，改 `_j14`：断**精确 6 列表头** `NAME NODE LOCAL PUBLIC
  STATE CREATED`（正则以 `CREATED$` 收尾，7 列 HOME 变体天然不匹配 → 抓住多出的 HOME 列）**且** PORTS 节体为
  `(none)`（`ps.go renderPortsTable`）。
- **B5b→加 B5c**：`--force` 后新增 `_sha_eq_xnode /tmp/a.bin agt1 /home/sim/dst.bin`——dst.bin 播为 100 随机字节、
  a.bin 4096 字节，**no-op 成功或写错内容都会 SHA 不符 → FAIL**；rc=0 不再是「覆盖成功」的证据。
- **E4→改真 round-trip**：`push` 后**真 pull 回** `/tmp/recover.back` 并 `_sha_eq /tmp/a.bin /tmp/recover.back`
  （E4/E4b/E4c），取代旧的「push && test -s」（后者对任何非空文件都过）。

**验证**（服务器日志）：`J14 ...exact 6-col header (no HOME) + empty (none)` PASS；`B5c --force dst content ==
source SHA-256` PASS；`E4c recover round-trip sha256 == source` PASS。61 GREEN **41/41**。

## MAJOR-3 — 内审留档缺失 → 采纳，补齐审计链

**采纳。** 内审 workflow **确已按 CLAUDE.md §3 step 4 运行**，但当时未落盘。新建 `docs/reviews/s1-review.md`，
据原始 journal 13 条产出重建：完整 12 finding 集、6 维度 reviewer + 6 恒等对抗 verifier 的裁决（**12/12 CONFIRMED,
0 REFUTED**）、主进程逐条采纳 + 严重度校准（N-1…N-5）、内审新增测试。**Workflow 约束可复核**（回应你「无法验证
模型/数量」）：runId `wf_6ba03f33-99e`、**agentCount=13**（6+6+1 静态）、**defaultModel `claude-opus-4-8[1m]`**、
脚本**无 `model:` 覆写**（全继承会话 Opus 4.8）。报告另如实记录一处跨会话接手时发现的「报告 ✅ 与代码不符」
（S1-10 当时仍是 `true`，已补落地为 `_statfs_healthy`）。

## MINOR-1 — roadmap S0 状态矛盾 → 采纳

**采纳。** `docs/simcluster-coverage-roadmap.md`：文件头 Status 改「S1 已落地（外审整改中，commit 待填）、S2–S9
未开工」；**S0-pty** 状态列 → 已落地（S1；`image/pty-run.py` 烘焙进镜像；commit 待填）；**S0-台账** 状态列 →
已落地（S1；gotcha ledger + README 编号族/drill 表重构 + 提交入仓命令树 golden gate；commit 待填）。

## MINOR-2 — 命令树 gate 污染源码树 → 采纳

**采纳。** `cmd/tether/command_tree_inventory_test.go`：漂移诊断从 `testdata/*.actual`（源码树）改写到
**`t.TempDir()`** 并在错误里打印路径；测试开头 `os.Remove` 清理历史 `.actual` 残留。`go test ./...` 在合法 CLI
变更下不再留 untracked 产物。

## MINOR-3 — runtime completion 不在门内 → 采纳

**采纳。** 新增第二份 golden `testdata/command_tree_golden_runtime.txt`：`InitDefaultCompletionCmd()` 注入后遍历
运行期树（**99 paths** = 构造 94 + `completion` + bash/fish/powershell/zsh 5 路径 + `--no-descriptions` flag），
零 diff 断言。另加显式不变量 `runtime == construct + 5`，把 roadmap round-6 的「94/99」convention 写进测试代码。

## MINOR-4 — golden 尾随空格致 diff-check 失败 → 采纳

**采纳。** `dumpCommandTree` 空 flag 集时输出 `flags:`（无尾随空格）而非 `flags: `；**两份 golden 重生成**。
直接 grep 核实：两 golden **零尾随空格**；全部变更/新增文件均无尾随空格（`git add -A` 后 `git diff --cached
--check` 将为 0）。

## Doubts / recommendations

- **pty-run `execvp` 失败 traceback**：**已修**——`execvp` 包 try/except，失败时 `sys.stderr` 提示 + `os._exit(127)`
  （shell「命令未找到」惯例）。本地烟测：`pty-run.py -- /nonexistent` → `exit 127`、**无 traceback**。
- **pty-run 守卫本地自测**（drill 不触发 eof/drain-idle/no-cmd/execvp）：本地逐一烟测通过——eof-after-step → clean
  `FAIL(3)`；silent-but-alive → SIGKILL、wall≈idle（非挂死）；no-command → `exit 2`；execvp-fail → `exit 127`。
  （落盘 helper 自测你标为「后续」，从之；已用上述实证覆盖。）
  > **[round2 R2-F2 订正]**：此处「no-command → 2」当时**只测了无 `--` 分隔符**的形式（`--step 'expect:x'`）；
  > **漏测** `pty-run.py --`（有分隔符、后无命令）——该形式当时实为 `rc=1 + IndexError traceback`。round2 已在
  > `_parse` 加 `if not argv or len(argv) == 1` 修复，两种 no-command 形式**均 `exit 2` 无 traceback**（已复测）。
- **62 真-D 专用宿主**：认同，Arm 2 真-D + mode:off-without-safe 维持 NOT-COVERED（未改）。
- **两族 whole-suite wrap 可追溯日志**：已按 S1-04 订正的**族分波两 pass**（grow 族 `-j 2` + N=1 族全并行）跑整套，
  日志见本次整改附带（下「复验」）。

## 复验（整改后，从当前工作树重建镜像 + 实跑）

- `make test` GREEN（含命令树守恒：构造 94 / 运行期 99 **双 golden 零 diff**）。
- **两族 whole-suite wrap（10 drill，按 S1-04 订正的族分波两 pass；weilandserver 2026-07-11）**：
  - PASS 1 grow/force-single 族（`-j 2`）：`10`(19)/`11`(13)/`12`(13)/`13`(11)/`20`(14) — **5 drills, 0 RED, 349s, ALL GREEN**（未改的 grow 族零回归）。
  - PASS 2 N=1 族（全并行）：`00`(13)/`21`(8)/`60`(**38**)/`61`(**41**)/`62`(23) — **5 drills, 0 RED, 87s, ALL GREEN**。
  - 服务器日志逐条确认强化 oracle PASS：`J-G.3b2`（broker admin，no ctl session）、`J-G.3c-2`（首个读单次校验）、
    `J14`（精确 6 列表头 + `(none)`）、`B5c`（`--force` 跨节点 SHA）、`E4c`（真 round-trip SHA）。
- pty-run 守卫本地烟测：eof-after-step→FAIL(3) 无 traceback；silent-child→SIGKILL wall≈idle；no-cmd→2（**[round2 R2-F2 订正]** `--` 空命令形式当时实为 1+traceback、当轮漏测，round2 已修为 2）；execvp→127。
- 全部变更/新增文件**零尾随空格**（`git add -A` 后 `git diff --cached --check` 将为 0）。**产品代码零 diff 保持**。
