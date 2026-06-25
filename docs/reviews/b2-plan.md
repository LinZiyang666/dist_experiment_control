# B2 plan（定稿）— 机器消费面（--json + exit-code taxonomy + 错误码）

> Stage-A：9×Opus 对抗 workflow（4 起草 → 4 互评 → 1 综合）。主进程定稿采纳综合 plan。完整细节见
> workflow 输出（`tasks/wqcokwwwk.output`）；本文件记录**绑定决策 + 结构 + 测试 + 顺序**。
> **硬依赖：B1 先落地**（已完成）。B2 在 B1 的 `ClusterStatusReport.{View,Verdict,ViewHost,IsLeaderView}`
> 之上 rebase，只读消费、只加 additive 字段。build-and-prove：不动 `broker.go` serve wiring、不新构造
> `cluster.Node`、非集群 broker 字节等价。

## 0. 前置 refactor（gates ~80% 测试，先做）
现状：每个 list/result renderer 都**内联在 RunE 闭包里、紧跟 live `nc.Request`**，无 seam 喂合成回复；
`withBanner→renderBanner(os.Stderr,…)` 写进程全局 stderr、忽略 cobra writer。故先做**renderer 抽取**：
每命令把 RunE 在"检查完 `resp.Code`/`resp.Error`"处切成纯 `renderX(out io.Writer, resp, …, now time.Time)`
+ `emitXJSON(out, resp, …)`。RunE = decode → code 错→`brokerErrorMessage` → `if asJSON {emit}` → `render(…, time.Now())`。
文本分支**逐字搬入** renderX（字节等价，golden 锁）。`withBanner` 改签名收 `w io.Writer`，各 RunE 传 `cmd.OutOrStderr()`。
覆盖：node ls / ps / session ls / alert ls / expose / expose rm / admin sessions·nodes·audit / node upgrade --all / transfer push·pull。

## Item 1+2 — `--json` + 统一 schema_version
- 新 `cmd/tether/jsonout.go`：`emitJSON(w, payload)`（MarshalIndent + 失败回 `*ExitError{exitInternal}`）+ `normSlice[T]`（nil→`[]`，避免 `null`）。
- 每命令加 `--json`（opt-in，默认文本字节不变）。
- **每命令 CLI-local 输出结构体带 `Schema string`(判别串) + `SchemaVersion int`**（总 `(schema,schema_version)` 派发表）。结构体清单（见 plan 表）：`node_ls`/`ps`/`session_ls`/`alert_ls`(含 `dedup_key`)/`expose`{public_host,port,name,node}/`expose_rm`{name,node,port，**无 public_host**}/`admin_sessions`·`admin_nodes`·`admin_audit`/`node_upgrade`(per-node results+summary)/`transfer`。
- `schema`/`schema_version` **只在 CLI-local 结构体**，proto wire 不加字段（SSOT 字节兼容）。`cluster status` 的 `ClusterStatusReport` 加 `Kind="cluster_status"`（CLI-emitted only、非 omitempty）。
- **branch-load-bearing 字段（view/health/exit_code/errors/partial/schema/schema_version）永不 omitempty**；omitempty 只给信息性字段（active_sid/code/error/leader_host）。
- `node upgrade --all`：`dispatchUpgrade` 改签名 `(code string, err error)`，loop 累积 `{NID,Outcome∈{dispatched,skipped_transient,failed},Code}`；**退出行为不变**（沿用现 summary error 经分类器，无 worst-of-fleet 聚合）。
- `transfer --json`：`--json` 下所有进度 `Fprintf` gated，终态只 emit 一个 `transferJSON`。tier-B async 失败漏斗若难干净落地，B2 只发 push/pull tier-A，tier-B 延到 B2.1（DEFERRED 标注）。

## Item 3 — 进程级 exit-code taxonomy
- 新 `cmd/tether/exitcode.go` SSOT：`exitOK=0 / exitUsage=64 / exitUnavailable=69 / exitInternal=70 / exitTransient=75 / exitNoPerm=77`；**0–3 保留给 `cluster status` health（仅该命令发）**。`type ExitError struct{Class int; Err error}`（`Error`/`Unwrap`）+ `usageErr/unavailErr/internalErr/permErr/transientErr` helper。
- `main.go` 单 sink：`os.Exit(classifyExit(err))`。`classifyExit`：`errors.As(*ExitError)` 显式类优先 → `nats.ErrNoResponders→69` → `DeadlineExceeded/Canceled→75` → **未分类默认 70**（不 string-sniff prose，杀掉"改文案破坏分类"定时炸弹）。
- **未分类默认=70**（绑定决策，驳回 default-1：会重蹈 unclassified-transport 与 DEGRADED 都=1 的碰撞；64/77/75 必须**正向证据**，源头 wrap 或 broker `Code` 表，绝不从 `err.Error()` 推断）。
- 源头 wrap：本地参数校验→`usageErr`；`connectError`→`unavailErr`（Auth-Violation 分支→`permErr`）；`brokerErrorMessage`→`&ExitError{brokerCodeExitClass(code), <今日字节相同串>}`（一改全分类，人读串不变、回归锁）；`cluster status` 传输/参数错→`unavailErr`/`usageErr`（死 socket→69，不撞 health 2）。
- **`exec`/`run`/`session run` 透传 carve-out**：直接 `os.Exit(chunk.ExitCode)`、不入 sink，发远端进程退出码（任意 0–255）；与 0–3、taxonomy 一样是一等保留，文档明确"77=permission 仅限 broker-RPC 命令，不含远端透传"。
- `brokerCodeExitClass` 表（error_hints.go，仅 B2-surface 命令实际返回的 code）：permission→77；正向 transient（agent_no_responders/leader_unavailable/home_catching_up）→75；需人工动作（node_offline/port_exhausted/name_taken/…/session_not_found）→64；我方 bug/skew（agent_malformed_resp/json_parse/proto_bump/store_error）→70；adminsock cluster codes（not_leader→77、already_voter/not_a_voter/quorum_confirm_required/nonce_used/cluster_not_enabled/node_unknown→64/…、catch_up_stalled→75）。缺表→默认 70。**75 best-effort**：文档"robust retry 把 69/70/75 当可重试、仅 64/77 当终态"。

## Item 4 — adminsock `Code` 枚举 + 每个 populate 点
- `Response` 加 `Code string json:"code,omitempty"`（additive、旧端忽略、字节兼容）。
- 枚举（adminsock）：not_leader / already_voter / not_a_voter / catch_up_stalled / quorum_confirm_required / nonce_used / cluster_not_enabled / node_unknown / store_error + **bad_request**（第 10，覆盖 unknown op + 坏 join-token，→70）。
- **sentinel-promotion**：broker 错误多为 free-form；定义 `ErrNotAVoter/ErrAlreadyVoter/ErrNodeUnknown/ErrCatchUpStalled`，用 `%w` **wrap 不替换**（D7 断言的 `.Error()` 串字节保留），`clusterCodeFor(err)` 用 `errors.Is/As`。
- 每个 populate 点见 plan 表（clusterstatus.go 各 return 点 + server.go nil-backend/unknown-op + AddNode already-voter/catch-up）。
- CLI 消费：cluster.go 各 `fmt.Errorf("cluster X: %s", resp.Error)` → `&ExitError{brokerCodeExitClass(resp.Code), clusterAdminError("X", resp)}`；`brokerCodeHints` 扩 10 code 友好句；`resp.Code==""`（legacy）回退今日渲染→默认 70。

## Item 5 — `cluster status --json` 不吞 Error
- `ClusterStatusReport` 加 `Errors []string json:"errors"` + `Partial bool json:"partial"`（**非 omitempty、总在场**；CLI-emitted only、无旧 wire 端；与 B1 同 release 定义健康报告形状、schema_version 不变=1）。
- cluster.go：`rep==nil`→`*ExitError{69}`；`resp.Error!=""` 且有 report→`append(rep.Errors, …)`+`Partial=true`（折叠不丢）；`normSlice`。文本模式也打 `** PARTIAL: <err> **`。

## Item 6 — offline/online exit-2 语义
- **决策：按 `view` 打标 + 文档，不重编号**。`view=="ctl-nats"`+exit2=QUORUM_LOST；`view=="offline"`+exit2=无人应答 raft-ping。脚本读 `view` 再解释 `exit_code`/`health`。
- **health 串碰撞 → 甩回 B1**（B1 拥有 Health/verdict 串面）：offline exit-2 当前 `Health="DEGRADED"` 与 online DEGRADED→exit1 冲突；**B1 给 offline exit-2 独立 health 串 `ROSTER_UNREACHABLE`**，使 `(health→exit)` 视图无关。B2 只读消费 + 文档化，加 code-level guard：offline 只发 `{0,2,3}`、online health `{0,1,2,3}`、**任何 `cluster status` 路径都不发 taxonomy 值（64/69/70/75/77）**。

## 文档（usage.md）
新"退出码"节（64/69/70/75/77 + 0–3 保留 + exec/run 透传 carve-out + "0 仍=成功、旧 `$?` 脚本不受影响" + 75 best-effort 重试规则 + 未分类→70）；新"机器 JSON 接口"节（`(schema,schema_version)` 派发表 + bump 政策 + opt-in 字节稳定保证 + `alert ls --json|jq .dedup_key|xargs alert ack` 往返例 + errors[]/partial + (view,exit) 消歧）；adminsock code 枚举表（10 code + hint + exit class）。

## 测试（映射套件）
- **make test（fast，靠 §0 抽取 + subprocess harness）**：每 renderer golden（注入 `now` 杀 humanizeAgo flake；空集/单行/omitempty 缺位/unicode）；jsonout 表（schema 非空 + schema_version、`null` vs `[]`、alert ls→ack 往返、空 DedupKey 仍发）；exitcode 表（各 `*ExitError`、double-`%w`、ErrNoResponders→69、DeadlineExceeded→75、**未分类→70**、nil→0、**负向保留：无路径返 0/1/2/3**、`brokerCodeExitClass` 穷举每 hint code 有显式 class）；**subprocess exit harness**（re-exec：死 socket cluster status→69、DEGRADED→1、QUORUM_LOST→2、bad-arg→64、not_owner→77、远端 `exec -- sh -c 'exit 77'`→本地 77 不重分类）；error_hints 回归（wrap 后 `.Error()` 字节相同 + `errors.As(*ExitError)` 类对）；banner（render 空/非空 + grep-guard 每 --json RunE 传 jsonMode）；adminsock protocol（code 往返、旧形 decode ""、健康报告 errors:[]/partial:false 总在场）；clusterstatus（每 HandleCluster 错误路径→期望 Code + 反射"无静默 gap"门 + sentinel `.Error()` 不变）；cluster_status_json（折叠不吞 + nil→69 + view 消歧）；upgrade_all_json；transfer_json；expose_json（含破坏性 gate 下不发半成功 JSON）。
- **gated test/d9（`-tags d9_integration -race`）**：真 DEGRADED 3 节点 `cluster status --json`（errors/partial/view、exit2 view=ctl-nats；死 socket→69）；`admin nodes/sessions --json` 经真 socket（形状 + non-leader 带 `code=not_leader`）。
- **test/e2e（串行矩阵，加断言不新建并行套件）**：`node ls --json`/`cluster status --json` over 真 NATS 解析 + 有 schema_version。

## 顺序（B1 先）
1. §0 renderer 抽取 + `withBanner(w)`（golden 锁，先跑）。2. `exitcode.go` + main sink + subprocess harness。3. `brokerErrorMessage→*ExitError` + `brokerCodeExitClass`（字节相同串回归锁）。4. `jsonout.go` + 各 `--json`（先 wrapper：node ls/ps/session ls/alert ls/admin*；再 DTO：expose/upgrade/transfer）。5. adminsock `Code` + sentinel-promotion + populate + `clusterCodeFor` + 完整性门 + cluster.go 消费 + hint 扩。6. Item 5 + Item 6（含 B1 ROSTER_UNREACHABLE 协调）。7. 文档 + 全测试 sweep。

## DEFERRED
expose/cluster-add 幂等→B3/B4；声明式 apply→B7；metrics/--watch→B5；expose home_broker/epoch→B4；JSON 错误对象（B2 错误仍 human-stderr，仅 exit class + Code 机读）；session create/run/exec --json（透传 carve-out）；transfer tier-B async --json→B2.1；exec/run 信号号传播（不变）。

## B1 协调项（B2 依赖、B1 落实）
**offline exit-2 的 `Health` 串改 `ROSTER_UNREACHABLE`**（使 `(health→exit)` 视图无关）——在 B1 审查采纳阶段折入 `clusterStatusOffline`，并同步 b1 docs/测试。
