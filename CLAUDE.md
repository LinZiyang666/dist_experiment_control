# tether — 项目工作说明（CLAUDE.md）

> 本文件每个会话加载进上下文，定义**怎么推进本项目**。需求/架构/用法细节在 `docs/`，本文件不重复。
> 工作流移植自 `auto_daemon`，按 tether（Go-only、已发布、phase 序列推进）的实际情况改写。

## 1. 项目与文档地图

- 一句话：**"SSH + 端口暴露" 的 NAT 穿透控制面**——NAT 后的 agent 经 NATS 反连公网 broker，使用者（ctl）经同一 NATS 把命令路由到 agent。三角色同一二进制 `tether`，子命令切换。
- `docs/requirements.md` — 需求基线（唯一需求真相）。
- `docs/architecture.md` — 架构基线（实现尺）。关键：**「里程碑映射」（P0→P11 出口 + post-1.0 叶子增量登记）**、**「关键依赖警告」（先父后子硬约束）**、**「每进入新 phase 的 checklist」**。
- `docs/usage.md` — 全量用户/运维手册（怎么用、怎么部、怎么排错）。
- `docs/devices.md` — 实机/设备清单。
- `docs/reviews/` — 每个 phase / feature 的 plan 与各轮 review 报告（`p<N>-plan.md`、`p<N>-review.md`(+`-round2`/`-round3`)、`p<N>-external-review.md`；历史 feature 增量用 `<feature>-plan.md`）。
- `docs/reviews/quality-audit/` — 横切质量审计（concurrency / security / storage-protocol / cli-ux / tests / deadcode）。
- `log.md` — 实机测试日志（真 broker + 真 agent 的人工验证记录）。

## 2. 工作单元：一次一个 phase

- 按 `architecture.md`「里程碑映射」的 **P<N> 序列**推进，**一次只做一个 phase**。主线 P0–P11 已带到 **v0.1.0**；其后改为 **post-1.0 叶子增量**模式（各自独立 plan→实现→内审→外审，不在线性 P 序内、不阻塞主线），P12（expose `--remote-port`）/P13（proxy 订阅）等均按此走，当前已发布到 **v0.3.4**。
- **新工作**：除非用户明确要延续线性里程碑（则取下一个未做的 P 号），否则一律当作**新的叶子 feature 增量**——范围先与用户敲定，再按 §3 的 3 阶段 7 步开工。
- 依赖"**先父后子**"：任何 phase 只用已完成的前序产物，绝不超前——严格遵守 `architecture.md`「关键依赖警告」里的不可跳序约束。
- 每进入新 phase 先过 `architecture.md`「每进入新 phase 的 checklist」（前一 phase 出口断言全过、翻状态、开分支、**实现中发现设计问题先改文档再改代码**、单测+e2e 同 PR 落盘）。

## 3. 每个 phase 的工作流（3 阶段 · 7 步）

### 阶段 A — 规划
1. **多专家对抗性草拟 plan（用 Workflow 工具）**：主进程**为当前 phase 现场草拟一个 workflow 脚本**（按该 phase 的范围/风险定制 fan-out），并行多个不同视角的专家起草 → 对抗性互评 → 综合出候选 plan。
2. **主进程审核并修改 plan**：主进程是 plan 的**唯一定稿人**；定稿写入 `docs/reviews/p<N>-plan.md`。

### 阶段 B — 实现
3. **主进程按 plan 编写代码 + 测试**：连续块；遵守 §5 约定与 `architecture.md` 的不变量。

### 阶段 C — 审查与收尾
4. **多专家对抗性审查代码（用 Workflow 工具）**：主进程为当前 phase 现场草拟一个审查 workflow 脚本，并行多视角专家对抗性审查。**专家只读实现、可自行新增测试条目，但绝不修改实现代码。** 报告写入 `docs/reviews/p<N>-review.md`（多轮则 `-round2`/`-round3`）。
5. **主进程评估审查正确性并修改**：逐条采纳/驳回 finding；整合专家新增的测试；**只有主进程能改实现**。
6. **外部审查（用户本人）**：提交给用户做最终人工外审；用户出报告 `docs/reviews/p<N>-external-review.md`，主进程评估后**在报告文件内逐条回复**并修改。**外审不过不算 done。**
7. **phase 结束**：`git commit` + `git push`（见 §6）。

> **Workflow 不预置固定文件**：步骤 1、4 的多专家编排**每个 phase 自己即时草拟脚本**（用 Workflow 工具的 inline `script` 跑），不维护复用的 `.claude/workflows/` 文件。fan-out 的专家维度按当前 phase 现定。每个 phase 完成后**停下等用户外审/确认**再进下一个。
>
> **Workflow 模型硬约束**：所有 `agent()` 调用（drafter / critic / synth 等任意 subagent）**一律不得低于 Opus 4.8**——**禁 Haiku、禁 Sonnet**。做法：在 `agent()` 上**省略 `model`**，继承会话主模型（= Opus 4.8 `claude-opus-4-8[1m]`，最稳）；同理 `meta.phases[].model` 不设。fan-out 的 agent 数为**静态常量**（不由上一阶段动态决定）。若误用了低于 4.8 的模型跑出结果，**弃用并改 Opus 重跑**（resume 时改 `model` opt 会让缓存失效、自动重跑）。

## 4. 角色边界（不可越界）

| 谁 | 能做 | 不能做 |
|---|---|---|
| **主进程** | 定稿 plan、编写/修改实现、采纳审查、整合测试、commit/push | — |
| **专家**（workflow 内 agent） | 草拟 plan 建议（step1）；审查 + **新增测试**（step4） | **改实现代码**、定稿 plan、commit |
| **外部审查**（用户本人） | 独立人工外审、出报告 | 改代码 |

## 5. 编码与测试约定

- **语言**：团队讨论用中文；**代码 / 标识符 / 注释 / commit / 日志 / 错误串一律英文**。
- **Go-only**：`CGO_ENABLED=0` 静态二进制；工具链锁 **Go 1.25**（由 `nats-io/jwt/v2 ≥ v2.8.1` 拉高，**升级依赖前必验 go directive**）。
- **不变量**：以 `architecture.md` 为准（控制面/数据面分离、auth_callout nkey 身份、G.1/G.2 reconcile、proto wire 版本一致性、session ACL 隔离、port 分配带 …）。实现与审查都以它为尺。
- **wire 协议**：`internal/proto.ProtoVersion` 是 SSOT；任何破坏性 wire 变更走整次跨版本路径（`tether.v1.*` → `v2.*`），**不兼容则必须重装而非 upgrade**。
- **测试纪律**：
  - 表驱动；快测 `make test`（`go test ./...`，用嵌入式 `nats-server/v2/test`，**不需要本机 nats-server**）。
  - 端到端矩阵 `make e2e`（`-tags e2e_matrix`，`test/e2e/all_phases_test.go` 每 phase 一个子进程子测试；单 phase 用 `go test ./test/pX/...`）。**新 phase 的 e2e 进矩阵，作为跨 phase 回归网。**
  - 并发安全：`-race` + **仓库内建泄漏门**（`runtime.NumGoroutine` poll-with-tolerance + fd 基线，见 `test/concurrency/helpers_test.go`；**刻意不用 `go.uber.org/goleak`**）；触碰隧道/PTY/reconcile/传输/Raft 等并发面必须带 race + leak 门。
  - lint：`make lint`（golangci-lint **v2**；v1 在 Go 1.25 模块上会拒跑）。
- **提交前硬闸**：`make test` + `make e2e` + `make lint` 全绿，并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门（非 goleak）；**任一不过不算 done**。

## 6. Git

- 只在 phase 收尾（step 7）、内审+外审通过后才 `commit`/`push`；在默认分支（`main`）上**先开分支**（`phase/<N>-<slug>`，每个 phase 至少一个 PR）。
- commit message：conventional commits `<type>(scope): <imperative summary>`（如 `feat(ps): retention-bounded ps RPC`、`fix(auth): grant $JS.API.DIRECT.GET`）。
- **绝不添加 `Co-Authored-By: Claude` 或任何 AI 署名**（全局规则；已推送的若混入，用户会 rebase 移除）。作者/协作者只保留用户本人。

## 7. 当前状态（截至 2026-06）

> **⚠ 双线地雷**：`main` 顶端已是 **proto v2**（distributed-broker **D0** 已 commit），**连不上现网 proto-v1 车队**。面向用户的 patch 走 **v1 线**（从 `v0.3.5` 切 release 分支 + cherry-pick），**绝不从 main 发**给现网；详见 memory `project-release-lines-v1-v2`。

- **主线**：P0–P11 → **v0.1.0**（GitHub release，里程碑全达成）。
- **已发布的 post-1.0 叶子增量**（v1 线，按 tag 序，proto 始终 v1）：
  - file-transfer（`push`/`pull`，tier-A inline + tier-B JetStream Object Store）— **v0.2.0**（后 tier-B 上限提到 2 GiB）
  - run heartbeat watchdog、retention-bounded `ps` RPC + processes GC — **v0.2.8**
  - **P12** expose `--remote-port`（带内唯一索引仲裁） — **v0.2.9**
  - **P13** session-scoped proxy 订阅（内嵌纯 Go shadowsocks + 试解密多密钥，broker 托管 HTTPS 订阅 URL） — **v0.3.0**
  - compliance-cleanup 审计加固 — **v0.3.1**
  - transfer-unrestrict（`file_transfer.allow_roots` 可选收紧、缺省全盘） — **v0.3.2**
  - remote-fs-resilience（网络盘挂死时 `exec`/`run` 不卡死，`--safe`） — v0.3.3（随 v0.3.4 发布）
  - proxy-tunnel-reconnect（反向隧道自愈重连 + 就绪 liveness） — **v0.3.4**
  - history-snapshot-race（NumPending 驱动快照完成，修远程 broker 空输出） — **v0.3.5**
  - proxy-aware-dial（ctl/agent NATS 经本地代理：HTTP CONNECT + SOCKS5h，解 WSL fake-ip hang；默认关、零回归） — **v0.3.6**（WSL 实机验证过）
- **distributed-broker HA epic（D0–D9，proto v2，在 main）**：架构基线 `docs/distributed-broker-architecture.md` 过 4 轮外审 PASS；分解见 §19。
  - **D0**（前置门 + proto v2 SSOT + migrations 0008–0010 + PreVote 合并门）— **已 commit 进 main**。
  - **D1**（状态层：单节点 Raft FSM + SQLite Apply 同 txn `applied_index` + 幂等重投 + online-backup 快照/恢复 + kill-9 矩阵）— **DONE**（内审 + 外审 PASS，commit `bc181c1`）。报告 `docs/reviews/d1-{plan,review,external-review}.md`。
  - **D2**（op 集 + 全 mutator Plan/Apply 移植 → **N=1 功能等价**）— **DONE**（内审 + 外审 PASS，commit `38e2576` + pushed main）。**ops-only 不切线上 broker**（cutover=D9）；全 13 op + sqlbake(`LitTime=t.String()` 不强制 UTC、禁 `Args`、拒 NUL/非 UTF-8) + `Node.Propose` applyMu 接缝 + CHA §13.1 lint + DIFF-1 差分(UTC+非UTC) + TestD2Matrix。报告 `docs/reviews/d2-{plan,review,external-review}.md`。
- 实机环境与历史验证见 `log.md`（broker `pc732.emulab.net` / `weiland.top`，多 agent）。

### 已知未收口 / 缺口（接手时优先处理）

- **P13 阶段出口仍是 CONDITIONAL PASS**：真 Caddy/WSS + 真 Clash 端到端验证未跑（外审不过不算 done）。
- **e2e 矩阵覆盖洞**：`test/e2e/all_phases_test.go` 覆盖 p1–p10 + p13 + 叶子矩阵（TransferDefaults / RemoteFS / ProxyTunnelReconnect / **ProxyDial** / **D1** / **D2**）；**p11 及 post-1.0 的 file-transfer / ps-retention / P12 仍未进矩阵**。新增量进矩阵时一并回填。
- **D2 D9-staged 项（非欠账，已在 d2-review 确认边界）**：禁 FSM 外 INSERT lint（live 直连 mutator grandfathered）、leader-local 不碰身份列反向断言、`reconcileOnRegister`↔`resolveReconcileMarks` 真正共用重构、broker 切 FSM——全是 D9 cutover。
- **`README.md` 为空**（0 字节）——对外门面缺失。

### 下一步

- **D2 DONE（外审 PASS，已 push main）**；下一步 **D3**（NATS 集群层 ≥2 节点 routes + auth_callout 本地读 + fail-closed `T_fence` + broker-only ACL）。依赖 D2 出口（N=1 等价）已过。注意 proto v2 不发现网 v1 车队。
