# S1 Plan — 用户平面核心旅程（+ S 系列工艺底座）

Date: 2026-07-10. Batch: **S1**（S 系列首开批）. Flow: CLAUDE.md §3（3 阶段 7 步）.
Status: **主进程定稿**（Stage A step 2）. Roadmap 总纲：`docs/reviews/simcluster-coverage-roadmap.md` §3-S1；
无遗漏闸清单：`docs/reviews/simcluster-coverage-inventory.md`.

> 草拟法：本 plan 由 Stage A 对抗草拟工作流（4 视角 drafter → 4 对抗 critic → 1 synth，全部
> Opus 4.8、静态数量）产出候选，主进程逐条核实源码后定稿。**验收目标**：`60-user-journey` GREEN、
> `61-transfer-edges` GREEN、`62-remote-fs-safe` = 运行期实测的三选一定格结论。**零产品代码 diff**
> （唯一 Go 增量 = 提交入仓的命令树校验测试，属 test-tier，见 §2.4）。

> **使命一句话**（逐字约束每个断言）：像真实团队那样「用」一个真实部署的 tether 集群，让缺陷
> **暴露**——不是让操作「跑通」，而是让问题「露出来」（README Mandate ①–④）。测试基础设施的目的
> 是**暴露问题**，不是攒一堆 GREEN。

---

## 主进程定稿说明（vs 工作流综合稿）

综合稿（`scratchpad/s1-synth.md`）已被全盘核实，其 12 条源码级定格纠偏全部采纳。主进程仅做以下
**一处结构调整**并显式记录：

- **G.3 臂注入方式改写**：综合稿让「登出窗口内 `agent-join agt2`」作为状态变更注入，但 sim 的
  `cmd_agent_join` 内建的 ONLINE 自检走 **ctl 的 `node ls`**（`simcluster:_agent_online`）——ctl 登出
  期间该自检必失败、`agent-join` 会 `die`。故主进程改为：**setup 阶段先 join agt1+agt2 双 ONLINE**，
  G.3 臂在登出窗口内用 **`dexec systemctl stop tether-agent`（agt2）** 注入状态变更（不经 ctl，无依赖），
  poll 到 broker 侧视图 agt2 STALE/OFFLINE 后再 `login`，断言**重登后首个** `node ls -a` 即反映 agt2 已 OFFLINE。
  这证明**重连（重鉴权）后首个 node read 读取 broker 当前态**（反映登出期间的 kill）——**login 本身无 snapshot
  语义**（外审 R2-F1：不再表述为「取最新快照」），并把综合稿的 J-nodeA（OFFLINE 视图）合并进来。

其余全部依综合稿。下文即定稿全文。

---

## 0. 范围与边界

- **交付**：3 个 drill（`60-user-journey` GREEN、`61-transfer-edges` GREEN、`62-remote-fs-safe`
  feasibility spike）+ 首开批必落的两个 S0 底座（**S0-pty** = `image/pty-run.py`；**S0-台账** =
  `docs/deploy-tier-gotchas.md` + README 编号/表重构 + 提交入仓的命令树校验器 + 清单附录维护移交）。
- **拓扑（§0.4 最小化）**：全 N=1（S1 每条断言都不涉集群语义）。60 = 1 broker + **2 agent** + ctl；
  61 = 1 broker + 1 agent + ctl；62 = 1 broker + 1 agent + ctl。
- **只测不修（§0.2）**：S1 交付 drill + 暴露缺陷，**不交付产品修复**。任何 tether 缺陷 → 台账 `#25+`
  + `assert_bug` 钉住；修复另立叶子。harness/保真度债可随批修；任何超出「真实生产供给」的环境新增
  必带 Mandate-④ 说明（§2.2）。
- **深度闸门（§0.3）——逐 drill 的部署层增量**：(60) 真 `auth_callout` JWT 覆盖整个用户命令面 + 真
  **跨容器** PTY 字节流/信号/resize + 真 `User=sim` 隔离；(61) **真 daemon 从真实盘路径加载真
  `agent.yaml`、跨真实重启**执法三态 + tier 分界由**真 nats-server 广告的 `max_payload`** 决定；
  (62) 真挂载 + 真 `bootHangable`/statfs 探测姿态。hermetic 已密的纯逻辑（路径校验状态机、chooseTier
  算术、`sha_mismatch`/`path_race` 注入）**不在此重断**——`sha_mismatch`/`path_race` 留 hermetic。
- **无产品 diff**：三个 drill 全是 `drills/` 下的 shell；唯一 Go 增量 = §2.4 的命令树校验测试
  （test-tier）。`make test`/`make e2e`/`make lint` 为一次性守恒闸。

## 1. 依赖与 S0 落地项

- **上游依赖：无。** 60/61 从不拨反向隧道；61 的传输走 NATS + JS Object Store，不经 `tunnel_addr`。
  **S1 不落 S0-隧道**（expose 是 S3）——这里的 `agent.yaml` 供给只写 install.sh 头部 +
  `file_transfer`/`remote_fs` 块，不演练 expose 可达性。S0 状态台账须记录 S0-隧道（及 S0-ingress/
  布局/artifact/备份库/故障原语）在 S1 后**仍未落地**。
- **S0-pty**（`image/pty-run.py`）——60 需要；后续 S9-96 消费。规格 §2.1。
- **S0-台账**——首开批义务：建 `docs/deploy-tier-gotchas.md`；README drill 表 + 十位编号族重构
  （含 drill-11 行漂移清偿）；提交入仓的命令树校验器；清单附录维护移交。规格 §4/§2.4/§5。

## 2. Harness 增量

### 2.1 `image/pty-run.py`（S0-pty）— 接口

`pty-confirm.py`（`pty.fork()`、fail-loud、传播子退出码）的一般化：一个交互式 `run` 会话驱动，在
**ctl1 内以 user sim、HOME=/home/sim** 运行，在真控制 TTY 下驱动 `tether run <node> -- <argv>`
（必须真 TTY：`tether run` 从 `os.Stdout.Fd()` 读尺寸、并按 `isTTY` 门控 stdin pump，
`cmd/tether/run.go:313,181`）。

```
pty-run.py [--rows R=40] [--cols C=132] [--idle-timeout S=20] \
           --step '<verb>[:<arg>]' [--step ...] -- <cmd> [args...]
verbs（按序执行）:
  expect:<substr>  在 idle-timeout 内等 <substr> 出现，否则 FAIL(3)
  send:<text>      向 master 写 <text>+"\n"
  sendraw:<hex>    写十六进制字节（sendraw:03 = Ctrl-C）
  ctrlc            = sendraw:03 别名
  resize:<R>x<C>   对 master TIOCSWINSZ 到 R×C 再 kill(child, SIGWINCH)
  eof              关闭 master 写侧
```

工程要求：
- **无竞态 winsize**：`openpty()`，在 `execve` **前**把 slave 设成 R×C，再 fork；子进程
  `setsid()`+`TIOCSCTTY`+`dup2`+`execvp`。保证 `terminalSize()` 读到 R×C 而非 80×24 兜底
  （`run.go:316`）。60 的 `stty size`==`R C` 断言二次守卫。
- **回显 vs 输出消歧（硬规则）**：远端 PTY 是 cooked 模式、会回显输入，故等于某个按键的 `expect`
  token 会匹配到回显而非命令输出。**每个 marker 必须由 shell 计算**，使 token 不可能出现在回显的
  输入行里——例如 `send:printf 'R%sK\n' O` → 输出 `ROK`（输入是 `R%sK`）；或钉 `stty size` 输出
  （`40 132` ≠ 输入 `stty size`）/ `$?`。
- **fail-loud + 外部 watchdog**：任一 `expect` idle-timeout → SIGKILL 子进程、drain、`exit 3`。
  **drill** 另把整个调用包在 `timeout <N>s` 里，使卡死的 `tether run` 永不挂死 drill。用
  `os.waitstatus_to_exitcode` 传播子退出码（使 ctl 的 `os.Exit(7)`/`os.Exit(128)`/0 浮现）。

**生命周期元组（7 字段全写）**：归属批 S1 · 消费批 S1-60, S9-96 · 实例作用域 = 烘焙静态资产
`/opt/sim/pty-run.py`（无 per-instance 态）· 创建预检 = 经 `remote.sh build` 烘焙（Dockerfile `COPY`
+ 并入现有 `chmod` 行）；需 `python3`（在）+ `/dev/ptmx`（恒在——容器 `--privileged`，usage §9.5）·
密钥/信任材料 = 无 · 健康检查 = fail-loud `--idle-timeout`，每个 `expect` 自成 oracle · 最终清理 =
waitpid 收割子进程 + 每次退出关 pty；drill 的 `cmd_drill` trap nuke 实例。

**Mandate-④**：ctl 用户本就有终端；脚本化 pty 驱动是环境在**供给**终端，不是替 tether 弥补。它只喂
stdin/读 stdout 于未改动的产品 `tether run`——ptmx 权限从不是被测对象（部署价值在真**跨容器**
字节流/信号/resize）。

### 2.2 61/62 的 agent.yaml 供给 — 忠实做法

**critic 揭出的问题**：sim 的 `cmd_agent_join` **不写** `agent.yaml`，且其 unit 的 `ExecStart` 带
`--nats-url`（`simcluster:354`）会 flag-shadow 任何 yaml `broker_url`（`pickFlagOrYaml`）；而
`provision-node.sh` 建 `/home/sim/.tether` 是 **0755** vs install.sh 的 **0700**
（`install.sh:315-317` / `provision-node.sh:65`）。故「install.sh 忠实、零新增」的天真说法是**假的**、
会软掩盖保真度缺口。

**决策——忠实路径（非 GAP 标注）**：61/62 按 install.sh 真实 agent 的跑法供给（正是 sim 的活，
Mandate ③）：
- 落一个 helper `agent_provision_yaml <agt> <sid> <broker_url> <policy>`（放 `drills/lib/agentyaml.sh`；
  是 **drill fixture、非** operator verb），它：
  1. 在 `/home/sim/.tether/agent/<sid>/agent.yaml` 写**完整** `agent.yaml`——install.sh 头部
     （`broker_url/session/nid/tunnel_addr`）**加**被测的 `file_transfer`/`remote_fs` 块——属主
     `sim:sim`、**`chmod 600`**、置于 **`agent/<sid>` 子树 0700**（镜像 `install.sh:315-317,360`）；
  2. 生成 agent unit，`ExecStart=…/tether agent --session <sid> --nid <nid>` **不带 `--nats-url`**，
     使 yaml `broker_url` 权威、正如真 install.sh agent；
  3. `policy=narrow:<abs>` 时：**先** `install -d -o sim -g sim <abs>`（真 operator 的数据目录开机即在），
     **再**写 yaml，**再** `systemctl restart tether-agent`，**再** `poll_until` node ls ONLINE
     （证明严格 `KnownFields(true)` loader 解析通过且 daemon 活着——后续 refusal 是**传输** refusal、
     不是死 agent）。
- `policy` 映射三态（`resolveTransferMode` `internal/agent/transfer.go:705-713`；判别子
  `RootsConfigured = AllowRoots != nil` `cmd/tether/agent.go:230`）：`open` → 省略 `file_transfer`
  （modeOpen）；`narrow:<abs>` → `allow_roots:\n  - <abs>`（modeNarrow）；`disabled` → 字面
  `allow_roots: []`（modeDisabled）。

**Mandate-④（精确、不过度声称）**：helper 写的正是 install.sh 写的文件（0600 文件、0700 `agent/<sid>`
树、sim:sim）+ operator 文档化的 `file_transfer` 编辑，并以 flagless unit 跑使 yaml 全权威如生产。
**保真度债注记（入台账、不阻塞）**：sim 烘焙的顶层 `/home/sim/.tether` 是 0755 vs install.sh 0700——
传输策略面不读该目录 mode，故 S1 不依赖它；登记为 §1.4-邻接的保真度债行，而非静默 chown 烘焙镜像
（Mandate ①）。

### 2.3 2-agent 支持 — 无新 harness

`up --brokers 1 --agents 2 --ctl 1` 已 boot agt1+agt2（`cmd_up` 循环 `--agents`）；`cmd_agent_join
<agt>` 是 per-node（各自 `/etc/tether` 卷），两次顺序 join 直接可用。60 加「双 ONLINE」断言，并把
agt2 的 stop 用作 G.3 状态变更注入（§3.1，主进程定稿说明）。

### 2.4 提交入仓的命令树校验器（S0-台账，test-tier）

落一个**永久提交**的 `cmd/tether/command_tree_inventory_test.go`（package `main`，唯一能访问未导出
`newRootCmd()` 的家），它递归 `Commands()`、采集 `Local+Persistent+Inherited` flag（三集去重）、记录
command 级 Hidden（`Hidden:true` / `deprecatedClusterAlias` / `hiddenDebugCmd`）与 flag 级 Hidden
（`MarkHidden`/`registerYesRejector`），按清单 §2 统一排除规则归一化（仅
`--home/--nats-url/--socket/--json` 可省），**对一份 checked-in 的 golden dump 断言零 diff**。这把清单
§3 的生成法固化为每批收工闸可复跑者。**否决「用完即删」设计**——删掉的生成器在 S2…S9 收工闸跑不了。
它是 test-tier、是 S1 零-Go-diff 的唯一有意例外；`make test` 须带它保持绿。golden dump 落
`cmd/tether/testdata/command_tree_golden.txt`（Go testdata 惯例；本行原写 `docs/reviews/`，实现落 testdata/）。
> **S1 外审增强（MINOR-2/3/4）**：① 空 flag 集渲染 `flags:` 无尾随空格（过 `git diff --cached --check`）；
> ② 漂移诊断写 `t.TempDir()`、不落源码树 `.actual`；③ 另加**第二份 runtime golden**
> `command_tree_golden_runtime.txt`（`InitDefaultCompletionCmd()` 后 99 path，把 cobra 运行期注入的
> completion 子树纳入结构门），并断 `runtime == construct + 5`。

## 3. 逐 drill 规格

每断言约定：**名 · 精确命令 · 期望 · sig-regex(file:line) · false-green 备注 · Mandate**。
`assert_ok` / `assert_refuses "<regex>"` / `assert_bug "<gotcha>" "<sig>"`（`lib/assert.sh`，regex 匹配
combined stdout+stderr）。`poll_until` 不用固定 sleep；异步一律验 RESULT 非退出码；`drill_begin`
throwaway 门；`trap … nuke` 清理。`CTL()` = `dexec -u sim <ctl> -- env HOME=/home/sim tether …`。
`AEXEC(agt)` = root `dexec`；`AEXEC_sim(agt)` = `dexec -u sim`。

### 3.1 `60-user-journey`（GREEN，N=1 + agt1 + agt2 + ctl）

Setup：`up --brokers 1 --agents 2 --ctl 1` → `init brk1` → N=1 floor 健康（复用 00 的 leader/health 门）
→ `session lab --pin 135790` → `agent-join agt1` → `agent-join agt2`（两次 join 均在 ctl 登录态下，
`_agent_online` 自检可用）。

**头注 false-green 横幅（必写）**：60 可能因错误原因变绿——(a) 缓存 session 无真 CONNECT 就应答
〔缓解：G.3 断言登出→再登录的真往返 + 反映登出期变更〕；(b) PTY 断言接受 80×24 兜底而非所设尺寸
〔缓解：精确尺寸〕；(c) Ctrl-C「成功」只因 sleep 从未启动〔缓解：RUNNING 基线〕；(d) exit-code 断言
接受「非零」而非精确码；(e) `stty size` 是回显输入而非远端输出〔缓解：shell 计算 marker〕。

| # | 名 | 命令 | 期望 | sig (file:line) | false-green | Mandate |
|---|---|---|---|---|---|---|
| J1 | ctx 显活跃 | `CTL(ctx)` | 打印 `lab` 活跃 | `lab`（由 G.3 守卫） | 任何活跃 session 都过 → 由 G.3 门控 | ③ |
| J-G.3a | **登出清空 + session-gated 命令 REFUSES** | `CTL(logout)` 后 `CTL(node ls)` | node ls 拒「no active session」 | `no active session`（`cmd/tether/run.go:70` 风格；**Stage-B 实测 node ls 的真拒串**） | **承重负例**——no-op logout 仍让 node ls 列出双 agent；须断言中间 REFUSAL | ③ |
| J-G.3b | **窗口内注入状态变更** | 登出态下 `AEXEC(agt2 -- systemctl stop tether-agent)`；poll broker 视图 agt2→STALE/OFFLINE | agt2 不再 ONLINE | broker 侧 node 表 agt2 状态 ≠ ONLINE（poll_until，验 RESULT） | 定稿改用 stop（不经 ctl，规避 `_agent_online` 依赖）；kill agt2（非 agt1，后者后续要用） | deploy：真心跳过期状态机 |
| J-G.3c | **再登录（重鉴权）后首个 node read 取 broker 当前态** | `CTL(login -s lab --pin 135790)` 后 `CTL(node ls -a)`（**首个**、单次、非 poll） | agt1 ONLINE **且** agt2 OFFLINE/STALE | `agt1[[:space:]]+ONLINE` 且 `agt2[[:space:]]+(OFFLINE\|STALE)` | **login 本身无 snapshot 语义**（只 auth-callout CONNECT，`cmd/tether/login.go`）；缓存了登出前 roster 的客户端会仍显 agt2 ONLINE → RED。真实证明=J-G.3b 已经 broker admin（无 ctl session）在 login **之前**证 agt2 STALE、重登后**首个**独立 node read 即读 broker 当前态（外审 R2-F1：删除原「取 LATEST 快照」的错误 snapshot 声称） | **deploy：真断连+重连+auth_callout 重鉴权（G.3）** |
| J2 | node ls 真列 | `CTL(node ls)` | 表头 `NODE STATUS HEARTBEAT PROTO RELEASE`；agt1 行 ONLINE、PROTO=int、RELEASE 非空 | `agt1[[:space:]]+ONLINE[[:space:]]+\S+[[:space:]]+[0-9]+[[:space:]]+\S`（`cmd/tether/node.go:105,115`；tabwriter→对齐空格） | 只断 ONLINE 会漏错 PROTO/RELEASE；PROTO 是裸 int（`%d`）非点分 | ③ 真跨进程心跳/版本 |
| J3 | exec exit-0 | `CTL(exec agt1 -- true)` | rc 0 | assert_ok | — | JWT 门 |
| J4 | **exec 非零精确** | `sh -c 'CTL(exec agt1 -- sh -c "exit 7"); [ $? -eq 7 ]'` | ctl 退 **7** | assert_ok on wrapper（rc==7） | 「非零」太弱；钉 ==7（`os.Exit(chunk.ExitCode)` exec.go:136） | deploy：真跨进程 exit 透传 |
| J5 | **exec stdout 多块流** | `CTL(exec agt1 -- sh -c 'echo HEAD; head -c 262144 /dev/zero \| tr "\0" "."; echo; echo TAILxyz')` | HEAD 与 TAILxyz 间 256 KiB 点 | `HEAD` 与 `TAILxyz` 均在 + 收字节 > 262144 | **payload 必须由 agent 命令写 stdout**（非在 agent 上 pipe 进 wc）；TAIL 在 256 KiB 后证所有 4 KiB 块按序到（`internal/agent/exec.go:25`） | deploy：真分块流经 NATS reply inbox |
| J6 | exec `--cwd`（flag 在 node **前**） | `CTL(exec --cwd /tmp agt1 -- pwd)` | `/tmp` | `^/tmp$` | **flag 必须在 node 前**（SetInterspersed exec.go:163）；agent home=/home/sim 故 /tmp 有别 | ③ 真 chdir as sim |
| J7 | **exec 信号杀→扁平 128**（5 要素见下） | `sh -c 'CTL(exec agt1 -- sh -c "kill -TERM \$\$"); [ $? -eq 128 ]'` + 另断 stderr | rc **128** + stderr `terminated by signal` | wrapper rc==128 **且** `terminated by signal`（exec.go:131-135） | **非 128+signo**；143/137/255 必须 fail；`$$` 杀直接子 → ExitCode=-1 | deploy：真信号送真跨容器子进程 |
| J8 | **run 真 PTY 尺寸**（5 要素） | pty-run.py `--rows 40 --cols 132 --step 'send:stty size' --step 'expect:40 132' --step 'send:exit' -- tether run agt1 -- sh` | 远端 `stty size` = `40 132` | `(^\|[^0-9])40 132([^0-9]\|$)` | 拒 80×24 兜底（run.go:316）；`stty size` 输入 ≠ `40 132` 输出故无回显 false-green；idle-timeout > attach 15s | deploy：**跨容器 winsize** ctl-TTY→attach→agent SetSize→TIOCSWINSZ |
| J9 | run 交互往返 | pty-run.py `--step 'send:printf "R%sK\n" O' --step 'expect:ROK'` | `ROK` 回来 | `ROK` | shell 计算 marker（输入 `R%sK` ≠ 输出 `ROK`） | deploy：真双向字节流 |
| J10 | run resize 传播 | pty-run.py `--step 'resize:24x80' --step 'send:stty size' --step 'expect:24 80'` | `24 80` | `(^\|[^0-9])24 80([^0-9]\|$)` | 第二个不同尺寸抓住卡在首尺寸的情形 | deploy：SIGWINCH→pty.resize→远端 master TIOCSWINSZ |
| J11 | **run Ctrl-C 中断进程组 ≤~1s**（5 要素） | 基线 `send:sleep 999` + `AEXEC(agt1 -- pgrep -f 'sleep 999')` 证 RUNNING；再 `--step ctrlc`；再 `--step 'expect:<新提示 marker>'` ~2s 内 | 提示返回；`sleep 999` 没了 | 提示 marker 见 **且** `AEXEC(agt1 -- pgrep -f 'sleep 999')` 空（poll_until，断 stdout 空——**非** `! pgrep`，其非 1 错误码会 false-green） | **勿用 `sleep && echo X \|\| echo $?`**——交互 bash 在 SIGINT 上中止命令列表、永不到 `\|\|`；RUNNING 基线先行使「提示返回」非空洞 | deploy：0x03→行规程→SIGINT 送真远端进程组（`internal/agent/run.go:245`） |
| J12 | run 不留孤儿 | J11 `exit` 后：`AEXEC(agt1 -- pgrep -u sim -f 'sleep 999')` 空；`CTL(ps -a)` 显该 proc `EXITED` | 无孤儿；`EXITED` | pgrep stdout 空；`EXITED` 在 | 基线：proc 曾 RUNNING（否则空洞）；poll 异步 prune（run.go:285） | deploy：真进程组拆除 + a.procs prune |
| J13 | ps RUNNING→EXITED | `CTL(exec agt1 -- sleep 5) &` → `CTL(ps)`（RUNNING）→ poll → `CTL(ps -a)`（EXITED） | RUNNING 后 EXITED | 期间 `sleep.*RUNNING`；之后 `sleep.*EXITED` | 裸 `ps` 藏 EXITED（ps.go:142）→ 断 `-a` 门控（否则转移声称空洞）；poll | deploy：真跨进程 proc 状态机 |
| J14 | ps PORTS 节渲染 | `CTL(ps)` | 6 列 PORTS 节表头、空 | `PORTS` 与 `\(none\)` | N=1 → **无 HOME 列**（HOME 是集群独有 ps.go:184,192）——断 6 列表头，有值 PORTS/HOME 是 S3-70 的勾 | — |
| J15 | history -n 界 | `CTL(history -n 5)` | ≤5 行 | （行数） | — | JWT 门 |
| J16 | history --kind call/proc + 非法拒 | `CTL(history --kind call)`；`CTL(history --kind proc)`；`CTL(history --kind bogus)` | call-only/proc-only 行；bogus → usage 错 | `CALL`；`PROC`；`must be one of`（history.go:88） | 合法集 `call\|proc\|port\|transfer`；非法负例钉住校验器 | deploy：真 JS 审计回放 |
| J17 | history --follow 烟测 | `timeout 6 CTL(history --follow)` 后台 + 触发一次 exec | 新行 tail 进 | 新记录出现；rc∈{0,124} | 必须有界（trap-kill follower）——断有记录到、非精确计数 | deploy：真流式消费者 |
| J18 | version / completion | `CTL(version)`；`CTL(completion bash)` | 均退 0 | `[0-9]+\.[0-9]+\.[0-9]+`；`complete\|_tether` | 纯本地——**仅烟测、非部署价值声称** | 顺带 |

**§0.4 5 要素**：
- **J7（信号杀）**：① 基线 = 起 `exec agt1 -- sh -c 'sleep 30; echo NOPE'`、注入前经 ps 证 RUNNING；
  ② 观测 = ctl exec 进程自身 rc + stderr；③ 注入 = 杀**子**（被杀 shell 内 `$$`，或另 `pkill` 标记子）
  ——绝不杀 agent；④ oracle = ctl rc==128 **且** stderr `terminated by signal`（验结果、非 pkill rc）；
  ⑤ 清理 = trap 杀残存 + `nuke`。
- **J8–J12（PTY）**：① 基线 = 交互回显工作（J9）+ `sleep 999` 证 RUNNING；② 源 = agent 上
  `pgrep`/`ps`；③ 边界 = `0x03` 于 `pty.<pid>.in`；④ oracle = 目标 pid ≤~1s 消失（poll_until，空 stdout）
  + detach 后无孤儿；⑤ 清理 = pty-run.py fail-loud kill + trap + `nuke`。

### 3.2 `61-transfer-edges`（GREEN，N=1 + agt1 + ctl）

Setup：`up --brokers 1 --agents 1 --ctl 1` → `init brk1` → `session lab --pin` → `agent-join agt1` →
播 ctl-local 文件。**属主纪律（部署价值 = 真 `User=sim` 隔离）**：每个播的源都建成 sim-可读、每个
push-dest 父都 sim-可写（经 `AEXEC_sim` 或 root + 显式 `chown sim:sim`）；**每个策略 refusal 带负向守卫
`! grep -qi io_error`**，使权限墙永不能冒充策略码（修 MF-2）。

**头注 false-green 横幅**：(a)「全都拒」可能是死 agent → open 基线（A）+ narrow 内正控（F1）门控；
(b) `not_a_regular_file` 可能在缺失路径上通过 → 播真 `ln -s` 并钉精确码；(c) tier=b 可能走 8 MiB 静态
路径 → 升 tier 文件恰 1 MiB（< 8 MiB）使 tier=b 只能来自真 `max_payload/2` clamp；(d) 传输路径校验
逻辑 hermetic-密——唯一部署声称是真 daemon/真盘/真重启/真 max_payload 的 seam。

**Arm A — OPEN 基线（全 drill 的正控）**：
- **A1/A2** — `agent_provision_yaml agt1 lab <url> open`；播 `/tmp/a.bin` sim-owned；
  `CTL(push /tmp/a.bin agt1:/home/sim/a.bin)` OK；`CTL(pull agt1:/home/sim/a.bin /tmp/a.back)` OK + sha
  匹配。sig `OK`、sha eq。在任何收紧前确立传输活着（§0.4 不同态成功对照）。Mandate ③。

**Arm B — 机制墙（所有模式、OPEN）**：
| # | 名 | 命令 | 期望 | sig (file:line) | 备注 |
|---|---|---|---|---|---|
| B1 | push 父缺失 | `CTL(push /tmp/a.bin agt1:/no/such/dir/f)` | 拒 `path_parent_missing` | `path_parent_missing`（`transfer.go:750`）+ `! io_error` | vs pull 的不对称 |
| B2 | pull 未找到 | `CTL(pull agt1:/no/such/file /tmp/y)` | 拒 `path_not_found` | `path_not_found`（`:820,840`）+ `! io_error` | 钉文档化 push/pull 不对称 |
| B3 | push symlink 叶 | `AEXEC_sim(agt1 -- ln -s /etc/hostname /home/sim/evil)`；`CTL(push /tmp/a.bin agt1:/home/sim/evil)` | 拒 `not_a_regular_file` | `not_a_regular_file`（`:772`）+ `! io_error` | agent 盘上真 `ln -s`（lstat/O_NOFOLLOW） |
| B4 | pull symlink 叶 | `CTL(pull agt1:/home/sim/evil /tmp/z)` | 拒 `not_a_regular_file` | `not_a_regular_file`（`:845,849`）+ `! io_error` | 读侧 symlink 防御 |
| B5 | push dst_exists → --force | 播 regular `/home/sim/dst.bin`（sim）；`CTL(push /tmp/a.bin agt1:/home/sim/dst.bin)` 拒；再 `--force` OK | 拒 `dst_exists` 后 OK | `dst_exists`（`:1102`）后 `OK`；B5 基线 = dst 真存在 | agent 侧 Linkat-EEXIST（pull dst_exists 是 ctl-local → 用 push） |
| B6 | **>2 GiB pull too_large** | `AEXEC(agt1 -- truncate -s 3G /home/sim/big.sparse && chown sim /home/sim/big.sparse)`；`CTL(pull agt1:/home/sim/big.sparse /tmp/big)` | 拒 `too_large`，**无 3 GiB 搬运** | `too_large` 且 `2 GiB`（`:361-364`） | **3G（>2147483648），非 2.1e9**；必须 PULL（push >2 GiB 是 CLI-local `transfer.go:142`）；稀疏 inode 从不流 |

**Arm C — tier 分界（真 nats max_payload；tier-B 锚在落地 oracle）**：
| # | 名 | 命令 | 期望 | sig (file:line) | 备注 |
|---|---|---|---|---|---|
| C1 | ~500 KiB → tier A | 播 512000 B；`CTL(push … agt1:/home/sim/u.bin)` | `tier=a` | `tier=a`（`transfer.go:170`） | 512000 < 523264（旅程完整性、非部署敏感那条） |
| C2 | **1 MiB → tier B（升 tier）** | 播 1048576 B；`CTL(push … agt1:/home/sim/o.bin)` | `tier=b` + 落地 | `tier=b` **且** `AEXEC(agt1 -- test -s /home/sim/o.bin)` + sha 匹配 | **部署声称**：1 MiB < 8 MiB 静态 ⇒ tier=b 证真 `max_payload/2−1024` clamp 触发（`:745-758`）；banner 在 Put 前打印故锚在落地 sha | deploy |

**Arm D — NARROW（顺序修正：目录建+chown 在 restart 前）**：
- **F1（正控）**：`agent_provision_yaml agt1 lab <url> narrow:/srv/data`（helper 在 restart **前**装
  `/srv/data` sim-owned、poll ONLINE）；`CTL(push /tmp/a.bin agt1:/srv/data/ok.bin)` +
  `AEXEC(agt1 -- test -s /srv/data/ok.bin)` → OK。**关键控**：无它则 dropped root → reject-all 使一切
  「narrowing 有效」断言空洞（`CanonAllowRoots` `transfer.go:689,705-713`）。Mandate ③。
- **F2** push 根外 → `path_outside_roots`（`:763`）+ `! io_error`。
- **F3** pull 根外 → `path_outside_roots`（`:833`）+ `! io_error`（独立读路径——双向都断）。

**Arm E — DISABLED（§0.4 5 要素恢复臂）**：
- ① 基线 = Arm A 已证 open push/pull 工作；注入前再确认一次快速 push。② 观测 = push/pull 回复 `code=`
  （`transfer.go:211/464`）+ agt1 node ls ONLINE。③ 注入 = `agent_provision_yaml agt1 lab <url> disabled`
  （字面 `allow_roots: []` → modeDisabled `:710`）+ restart + poll ONLINE（验结果、非 restart rc）。
  ④ oracle（双向）：**E1** `CTL(push …)` → `transfer_disabled`（`:738`）；**E2** `CTL(pull …)` →
  `transfer_disabled`（`:808`）；各 + `! io_error`。⑤ 清理 = 恢复 `open` + restart + **再证往返**
  （可逆性对照——证 refusal 是配置翻转、非衰减）；trap `nuke`。

**Arm F — history --kind transfer 配对（按 path 消歧、两 kind 都要）**：
- A1（成功，`path=/home/sim/a.bin`）与 F2（agent 侧 refusal，`path=<outside>`）后：
  `CTL(history --kind transfer -n 20)` → 断成功 path 的 `start`+`complete` **且** refusal path 的
  `start`+`failed`，**作为按 `path=` 键定的独立 grep**（transfer_id 不渲染 `history.go:581-584`）。
  false-green：松的 `(complete|failed)` 交替只凭成功就绿；`failed` 对必须来自**agent 触达**的 F2
  （CLI-local/broker-gate refusal 不写 `start` 行）。deploy：receiver-finalization 审计经真 JS。

**显式裁剪（留 hermetic，§0.3）**：`sha_mismatch`、`path_race`（注入类）。

### 3.3 `62-remote-fs-safe`（FEASIBILITY SPIKE — OQ-2；「不可行」是合法且验收通过的结论）

**目标**：忠实复现挂死的网络挂载、观察 tether 在真栈上的三种姿态，**绝不静默把 FUSE stall 等同真
不可中断 D**，且 **drill 永不挂死**（每个远程命令包在外部 `timeout`；一次性实例退出即 nuke）。接地事实：
`remote_fs.mode ∈ {auto,off}`（`spawnsafe.go:57-66`；`"safe"` 非 mode、`--safe` 是 per-call flag）；
v0.3.3+ 缺省 `auto`；auto 仅当 `New()` 时存在 hangable 挂载才快速失败（`bootHangable` 冻结 `:240`，
短路 `!bootHangable && !safe` `:693`），探测是对 mountpoint `statfs`——注入的挂死须阻塞 **statfs**。故
对照是 **auto / off / off+--safe**，非「默认挂死 vs --safe」。

**能力探针（运行期分支选择器——被断言、绝不橡皮图章）**：
- 先试真 NFS：一个 NFS sidecar（kernel `nfs-kernel-server` 或用户态 `nfs-ganesha`/`unfsd`）导出目录，
  agent `mount -t nfs -o hard /mnt/hung`（共享宿主 kernel）。**判别测试（强制）**：独立探针
  `cat /mnt/hung/x` 分区后 → 读 `/proc/<pid>/stat` 态 + `timeout kill -9 <pid>`：态 `D` + 不可收割 =
  真 D；任何可收割 = 可杀近似。**记录是哪种**（这是 OQ-2 关键）。
- kernel NFS/D 不可用 → FUSE 兜底（`fusermount` + 一个 trivial FUSE fs、其 daemon 被 `SIGSTOP`）,
  **显式标 non-D**。
- **宿主保护**（主进程注记）：weilandserver 是共享机——D 态挂死可能残留卡死 mount。trap 用
  `umount -f -l /mnt/hung`（lazy）+ 杀 sidecar + 实例 nuke；外部 watchdog 保证 drill 自身不挂。

**臂（挂载在 agent 开机时在场，再分区）**：
- **Arm 1 — 忠实缺省 `auto` 快速失败**：开机时挂载健康（restart agent 使 `bootHangable=true`），分区使
  statfs 阻塞；`timeout 20 CTL(exec agt1 --cwd /mnt/hung -- whoami)` → `remote_fs_unsafe_cwd`；
  `timeout 20 CTL(exec agt1 -- /mnt/hung/bin/true)` → `remote_fs_unhealthy`。sig **不锚**
  `remote_fs_unsafe_cwd` / `remote_fs_unhealthy`（exec 渲染追加 `: <detail>`，`error_hints.go
  execFailureMessage`）。false-green：开机后才加挂载 → auto 盲 → 确认 `bootHangable`（挂载在 restart
  前）且路径在死挂载下；false-red：若 statfs 仍成功（只 read 挂）auto 正确保持健康——瞄准 statfs。
- **Arm 2 — 遗留 `mode: off` 真挂死（外部 watchdog）**：`agent_provision_yaml … remote_fs:{mode: off}`、
  restart；`timeout 15 CTL(exec agt1 -- /mnt/hung/bin/true)` → **外部 timeout 返 124**（tether 复现遗留
  `LookPath`/exec 在死挂载下绝对 argv[0] 的 D 挂死）。D 真相由**独立能力探针**（态 D + kill-9-证）确立、
  非解剖 agent 线程。**§0.4 5 要素**：① 基线 = mode:off 下健康 exec 在分区**前**快返；② 观测 = 外部
  `timeout` 退 124 **且**独立 `/proc/<pid>/stat` D 探针；③ 注入 = boot-带挂载+健康基线后分区 NFS server；
  ④ oracle = exec 超时(124) 且独立探针为 D + 挺过 kill -9；⑤ 清理(trap) = `umount -f -l /mnt/hung` +
  杀 NFS/FUSE sidecar + `nuke`。
- **Arm 3 — `mode: off` + `--safe` 覆写**：`timeout 20 CTL(exec agt1 --safe --cwd /mnt/hung -- whoami)`
  → 快速失败 `remote_fs_unsafe_cwd`（per-invocation 升级绕过 bootHangable `:693`）。证文档化手动逃生。

**三选一定格（运行期择一、皆诚实、皆验收通过）**：
- (a) 真 D 可造 → 3 臂 GREEN（部署价值真）。
- (b) 仅 FUSE → 臂 1+3 GREEN **若 statfs 阻塞**、显式标 FUSE-近似；**Arm 2 真不可中断 D → NOT-COVERED**
  登记（「真 D 需 kernel nfsd + hard mount；FUSE 是 S 态、kill-9 可收割，等同即 Mandate-① false-GREEN；
  留实机」）。
- (c) 两者皆无 → 整 drill **NOT-COVERED**、记原因。**NOT-COVERED 结论必须是能力探针在 weilandserver 上
  运行的 LIVE 断言判决——绝非 checked-in 的自声明 GREEN 骨架**（修 inverted-success）。drill 跑探针并断言
  其实测判决，使结论在套件内可见却是挣来的。
- 若发现 tether 缺陷（如 auto 在真 D 挂载上未快速失败）→ 台账 `#25` + `assert_bug`（修 = 另立叶子）。

## 4. 台账 + README/编号重构（S0-台账）

### 4.1 `docs/deploy-tier-gotchas.md`（建档）
- 序言互链 `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`（#1–#24，该段 SSOT）；编号 **#25+ 全局连续**
  （使 `assert_bug` token + `[GAP #N]` 标注跨两文件唯一）。迁移/互链 `#I1`（serve fail-closed 不变量、
  drill 11 断言的 KEPT 守卫）入序言并**关 `#I*` 族**。
- 每条 gotcha 模板：现象 / 机理(file:line) / 怎么自动化或修 / 钉住它的 drill + 签名。
- **S1 现实 gotcha 数 = 0**（60/61 是文档化行为的 GREEN 旅程；62 定格 NOT-COVERED、非 gotcha）。
  `#25+` 表**空落**；文件作为共用底座存在。
- **DOC 节（不占 gotcha 号）**：**DOC-5（S1 发现）**：exec 信号死不带信号值——agent ExitCode=-1
  （`internal/agent/exec.go:202`），ctl 塌成扁平 128（`cmd/tether/exec.go:131-135`）；「128+signo」是
  **有意**延后的 v2 wire-proto 项，由 J7 以扁平-128 契约钉住。**发布前核 `docs/usage.md` 无处声称
  128+signo**（若有，是真 DOC 缺陷要立项）。预登记（仅指针、S1 范围外）：**DOC-3** = `error_hints.go`
  指向不存在的 agent `--upgrade-url-allow` flag（发布该声称前核实该 flag 缺失；随 S5-31 共登）。按 roadmap
  带 DOC-1/2/4 指针。
- **保真度债注记**：sim 烘焙 `/home/sim/.tether` 0755 vs install.sh 0700（`provision-node.sh:65` /
  `install.sh:317`）——传输策略面 mode-无关；登记为 §1.4-邻接、不阻塞。

### 4.2 README 重构 + 编号族 + drill-11 漂移（盘上核实）
- **编号族节（新）**：`0x` 骨架 / `1x` grow / `2x` force-single·容量 / `3x` 升级 / `4x` 收缩·回归·割接 /
  `5x` 备份·灾备·轮换 / `6x` 用户平面 / `7x` expose·proxy 数据面 / `8x` session·安全·入群 /
  `9x` 观测·客户端视图·混沌。**历史例外保留：12/13/20/21**（号来自 gotcha 序数——12/20/21 取 gotcha id、
  13 为顺延号）。
- **drill-11 行漂移清偿（核实：drill 11 盘上 GREEN，trailer `GREW-VIA-TETHER-CLUSTER-ADD` `simcluster:219`；
  `#3/#4/#8` 钉 ABSENT、`#5/#10` 仅由结构门 A 覆盖、`#I1` 以 `assert_refuses` STAYS `drills/11-grow-gaps.sh:71`）。
  修全部五处滞后点**：
  - `README.md:233`（drill 表行）RED/`GREW-VIA-WORKAROUNDS` → **GREEN（G4 反转）**：grow 端到端驱动
    `tether cluster add`；#3/#4/#8 workaround 签名断 ABSENT；#I1 serve fail-closed 不变量 STAYS；trailer
    `GREW-VIA-TETHER-CLUSTER-ADD`。
  - `README.md:148`（walkthrough step 4）"prints a `GREW-VIA-WORKAROUNDS:` trailer" → `GREW-VIA-TETHER-CLUSTER-ADD`。
  - `README.md:174`（verbs 表 `grow` 行）"(honest, gap-labeled)" → "(honest; drives `tether cluster add`,
    `GREW-VIA-TETHER-CLUSTER-ADD` trailer)"。
  - `README.md:271-280`（"Gaps LABELED by grow but NOT yet signature-pinned"）——**重分类、不整段删**
    （Mandate ②）：#3/#4 从「未钉」移到「钉 ABSENT」（现由 cluster add 驱动）；**#5/#10 保留、重锚为由结构门
    A 覆盖**（`drills/11-grow-gaps.sh`「cmd_grow 不跑手动集群生命周期」），**非**行为签名；**#24（CN-only
    cert）/ #23（clean-exit-on-nats-loss）留作诚实未钉 backlog**。删 #5/#10 标注而不重锚会掩盖行为复现的缺失。
  - `simcluster:145`（滞后注释）"prints a `GREW-VIA-WORKAROUNDS: …` trailer" → `GREW-VIA-TETHER-CLUSTER-ADD`
    （harness-doc 真相对齐、无行为变更、Mandate-④ clean）。
- **加行** `60-user-journey`（GREEN）、`61-transfer-edges`（GREEN）、`62-remote-fs-safe`（spike——状态按
  OQ-2 结果）。表按号排序。每个新 drill 头注记时长/资源（全 N=1、~2–4 min、无 grow）。

## 5. 清单附录消费/更新 + 收工闸

**消费 → 重枚举 → diff → 落行再收工。** 采用**部分勾约定**（`S1✓(<臂>) · S2☐(<臂>)`）使共享行不被假闭合。

**§2/§4 行 S1 勾（臂级）**：
- `login/logout/ctx` → S1✓(激活 + logout + G.3 重连) · S2☐(CONNECT-拒非成员)。`completion` →
  S1✓(`completion bash` 烟测；`--no-descriptions` 未测 → 部分)。`login --broker` 别名 → S2(部分)。
- `exec` → S1✓(`--cwd`、exit/信号/流)；`--timeout` → **跑一条 short-`--timeout` 过期臂或显式登部分**
  （勿留裸 ✓——roadmap 闸禁「✓ 无臂」）；`--safe` → 62。
- `run` → S1✓(PTY/resize/Ctrl-C/attach)；`--cwd` → 可选一行臂或部分；`--safe` → 62；`--ack-alerts` → S9。
- `ps --all` → S1✓(RUNNING→EXITED、PORTS 表头) · S3☐(有值 PORTS/HOME) · S9☐(LOST 合成)。
- `push/pull` → S1✓(双向码、`--force`、tier 分界)；`--timeout` → 部分/hermetic；sha_mismatch/path_race
  显式 hermetic。
- `history --lines/--kind/--follow` → S1✓(`-n`、`--kind call/proc/transfer`、`--follow` 烟测、非法-kind 拒)。
- `node ls --all` → S1✓(`-a` OFFLINE/STALE 视图 + PROTO/RELEASE)；`--brokers` → S5。
- `agent` daemon → S1✓(register/心跳/重连 via setup + G.3)；`agent.yaml` 策略面 →
  S1✓(`file_transfer.allow_roots` open/narrow/disabled) · S1◐(`remote_fs` 62) · S4☐(`proxy`)。
  **`agent join` flag = S2，勿勾。**
- `version` → S1✓。

**事件侧——NULL-DIFF 是必需的闸步、非跳过**：S1 勾**零** §1.1/§1.2 `pubSysEvent`/alert 行（核实：用户面
命令不发 `sys.events` kind；transfer 在独立 subject `audit.transfer` 发、经 `history --kind transfer`
可见，非 pubSysEvent kind）。收工时**跑**事件生成法（`grep pubSysEvent` + authcallout `h.emit` +
`emitDrainEvent` + `proxy_cluster.go` + `alert_ops.go`）并**记「0 条 S1-引入 kind」**。

**收工闸 checklist**：
1. `make test` + `make e2e` + `make lint` 绿（守恒；提交的生成器须绿）。
2. **命令树重枚举** via §2.4 提交的生成器 → **断零 diff** vs 清单 §2。**收工时重推真实 path 数——勿把清单
   的「94」当真相**；非零 diff = 有东西漂了（依赖 bump / 无关改动）→ 调查、勿脏收。
3. **事件生成法**重跑 → **记 0 条 S1-引入 kind**（null-diff、非跳过）。
4. 附录臂注解落地（部分勾）；无 S1-owned 行未触及-且-未登记；62 若落 NOT-COVERED，用其**实测**理由登该行。
5. 台账 + README 落地；drill-11 漂移清除（全五处）；`#I*` 关；DOC-5 在。
6. `60`/`61` 在 run-drills 套件绿（落盘即自动发现）；`62` 达运行期实测三选一结论。

## 6. OQ 裁定

- **OQ-2（62）**：先试真 kernel-NFS hard-mount D 态；不可用则 FUSE 兜底仅供臂 1+3（probe-timeout 快速失败、
  **显式标 non-D**）、Arm 2 真 D **NOT-COVERED**（实机）；两者皆无则整 drill **NOT-COVERED**。强制的
  `/proc/<pid>/stat` + 有界 `kill -9` 判别子决定分支、其**实测**结果被记。**绝不**削弱 tether 或环境凑绿；
  **绝不**提交自声明 NOT-COVERED 骨架——判决须是 live 探针的断言结果。NOT-COVERED 结论是**成功**的 spike 产出。
- **OQ-8（横切，自 S1 立制）**：S1 的 3 drill 全 N=1 无 grow → **零**新并发 grow 负载；`10-grow-to-3` 的
  grow-timing 天花板不变。但收工 drill-all 跑**整**套（含 grow drill），故**自 S1 立族分波策略**（勿延后）：
  分族两 pass——grow/force-single 族（1x/2x）单独 serial 或 `-j 2` 一 pass；N=1 用户面族（含 00/21/6x）全并行
  另一 pass。run-drills 的 infra-flake 重跑只认既有签名（grow-timing RED 单跑复核、绝不自动吞）。记 wall-clock
  为 OQ-8 首个基线数据点（S9 固化 3× 连绿）。
  > **S1-04 内审订正**：本条草拟时写的「收工跑 `./run-drills.sh -j 6`（family-wave cap）」是**误述**——`-j`
  > 是无族感知的纯全局计数节流，`-j 6` 首波 `00,10,11,12,13,20` 反而把 5 个 grow/force-single 全塞一波、
  > 零削减、且放大不进 `FLAKE_SIG` 的 VOTER-timeout。正确策略=上文的**按族两 pass**；精确机理与命令见
  > `test/simcluster/README.md` OQ-8 段。

## 7. 验收出口

1. `60-user-journey` GREEN + `61-transfer-edges` GREEN 在 `run-drills.sh` 套件（落盘即自动发现）。
2. `62-remote-fs-safe` 达运行期实测**三选一**定格结论（真-D 全臂 / FUSE-近似-标注 + Arm-2 NOT-COVERED /
   整体 NOT-COVERED-带实测理由）；drill 永不挂死。
3. `image/pty-run.py` 烘焙（`remote.sh build`）带 7 字段生命周期元组 + Mandate-④ 注；`agent_provision_yaml`
   helper 落地（完整 yaml、flagless unit、0600/0700、sim:sim）；2-agent 路径被演练。
4. `docs/deploy-tier-gotchas.md` 建档（#25+ 连续、`#I1` 收编 + 关族、DOC-5 + 核实过的 DOC-3 指针、保真度债注）；
   README 编号族 + 60/61/62 行 + **drill-11 漂移清除（全五处）**；提交的命令树生成器 + 清单维护移交。
5. 收工闸过：命令树重枚举 = 零 diff（数重推、非假设）；事件生成法 = 0 条 S1 kind（记 null-diff）；清单臂部分勾；
   无 S1-owned 行未触及-且-未登记。
6. 每个 RED（若有）signature-guarded（`assert_bug`）到 `#25+` gotcha；每个不得已手动步 `[GAP #N]` 标 + sig 钉。
   无未登记的表外行；无 green-for-the-wrong-reason。
7. `make test` + `make e2e` + `make lint` 绿；drill-all 按**族分波两 pass**（grow 族 serial/`-j 2` + N=1 族全并行，
   见 §6 OQ-8 S1-04 订正）。**外审不过不算 done**——本 plan 定稿后走对抗内审 → 用户外审 → 才 commit；不自 commit。
