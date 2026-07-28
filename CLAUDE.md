# tether — 项目工作说明（CLAUDE.md）

> 本文件每个会话加载进上下文，定义**怎么推进本项目**。需求/架构/用法细节在 `docs/`，本文件不重复。
> 工作流移植自 `auto_daemon`，按 tether（Go-only、已发布、phase 序列推进）的实际情况改写。

## 1. 项目与文档地图

- 一句话：**"SSH + 端口暴露" 的 NAT 穿透控制面**——NAT 后的 agent 经 NATS 反连公网 broker，使用者（ctl）经同一 NATS 把命令路由到 agent。三角色同一二进制 `tether`，子命令切换。
### 文档权威链 —— **冲突时上位覆盖下位**

| 层 | 文档 | 权威范围 |
|---|---|---|
| 1 · WHAT | `docs/requirements.md` | **需求**的唯一真相；不描述当前实现 |
| 2 · HOW（当前） | `docs/distributed-broker-architecture.md` + `docs/deploy-tier-gotchas.md` | **实现**的绑定契约 |
| 3 · HOW（历史） | `docs/architecture.md` §A–§K | 只提供**当初为何这样取舍**；标识符与拓扑细节已过时，**不作实现依据** |

**第 3 层从不覆盖第 2 层。** 但 `architecture.md` 的**「里程碑映射」「关键依赖警告」「每进入新 phase 的 checklist」**仍然有效，§2/§3 依赖它们。

其余文档：`usage.md`（使用者手册）、`broker-ops.md`（broker 运维）、`cluster.md` + `cluster-runbook.md`（HA）、
**`testing-standards.md`（怎么写测试——§5 讲跑哪些，它讲怎么写）**、`devices.md`（设备清单）、
`devices-ops.local.md`（凭据/资源）、`reviews/`（各 phase 的 plan 与审查报告）、
`test/simcluster/README.md`（deploy-tier drill 的 Mandate 与用法）。

## 2. 工作单元：一次一个 phase

- 按 `architecture.md`「里程碑映射」的 **P<N> 序列**推进，**一次只做一个 phase**。主线 P0–P11 已带到 **v0.1.0**；其后改为 **post-1.0 叶子增量**模式（各自独立 plan→实现→内审→外审，不在线性 P 序内、不阻塞主线），P12（expose `--remote-port`）/P13（proxy 订阅）等均按此走，当前已发布到 **v0.4.7**。
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
5b. **测试归位**：本轮新增/整合的测试文件**按被测单元命名**（`tunnel_fence_test.go`），
   **不允许**新建 `*_external_review_*_test.go` / `*_round<N>_*_test.go` / `p<N>_*` / `b<N>_*` / `g<N>_*`
   这类**按开发过程事件**命名的文件。审查者的 finding 写成测试函数上方的一行
   `// origin: p13 external review round 6 F2`——它扛得住改名，而文件名扛不住：
   下一个改 `CloseProxy` 的人 grep 不到 `p13_external_review_round2_test.go`，
   于是同一个不变量每轮审查被重新发现一次（`internal/tunnel` 曾有**四个**文件测同一条 fence，
   round2 抓到 `CloseProxy` 漏 fence、round5 抓到 `CloseSession` 同样的洞、round6 抓到 `ForgetSession`——
   **若 round2 就写成 `{verb, killFn}` 表，round5/round6 这两轮返工在结构上不会发生**）。
   由 `test/determinism/test_naming_test.go` 的冻结门在 `make test` 里拦下；
   存量 158 个在 `legacy_process_named_list.go` 的**递减账本**里，改名时同 commit 删掉对应行
   （账本里已不存在的条目会让门变红，所以它不会腐化成永久豁免）。
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
- **不变量**：控制面/数据面分离、auth_callout nkey 身份、G.1/G.2 reconcile、proto wire 版本一致性、session ACL 隔离、port 分配带 …。
  **按 §1 权威链取尺**（外审 R6 订正：这里原写"以 `architecture.md` 为准…都以它为尺"，与 §1"第 3 层不作为实现依据"直接冲突）——
  集群面/v2 的不变量以 `distributed-broker-architecture.md` + `deploy-tier-gotchas.md`（第 2 层）为准；
  `architecture.md` §A–§K 只提供**当初为何这样取舍**的论证，其标识符与拓扑细节已过时，不作实现依据。
- **wire 协议**：`internal/proto.ProtoVersion` 是 SSOT；任何破坏性 wire 变更走整次跨版本路径（`tether.v1.*` → `v2.*`），**不兼容则必须重装而非 upgrade**。
- **测试纪律**：
  - **按需测试，非必要不跑全量**：迭代时只跑碰过的那块——`go test ./test/pX/...`、
    `go test -tags dN_integration -race ./test/dN/`、`go test ./internal/broker/ ./internal/cluster/`。
  - **全矩阵闸门只有一个：`make e2e-parallel`**（约 3–4 min）。**并行全绿即通过，不得再串行"复核一遍"。**
    **全量串行的 target 已从 Makefile 删除**——不是弃用，是拿掉了，因为存在的 target 就会有人跑。
    串行唯一合法用途是**定位并行报出的那一个**：`make e2e-one T=TestD5Matrix`（`T` 强制、无 "all" 模式）。
    理由与四类根因见 `docs/reviews/parallel-flake-rootcause.md`；写测试的规范见 `docs/testing-standards.md`。
  - 表驱动；`make test` 用嵌入式 `nats-server/v2/test`，**不需要本机 nats-server**。
  - 并发安全：`-race` + **仓库内建泄漏门**（NumGoroutine + fd 基线，见 `test/concurrency/helpers_test.go`；
    **刻意不用 goleak**）；触碰隧道/PTY/reconcile/传输/Raft 必须带 race + leak 门。
  - lint：`make lint`（golangci-lint **v2**；v1 在 Go 1.25 模块上会拒跑）。
  - **simcluster deploy-tier drill（`test/simcluster/`）**：第三层测试，不进 `go test`/CI。
    **⚠ 按需运行、非必要绝不运行**——只在改动真实部署栈（`install.sh` / `nats.conf` / systemd unit /
    集群生命周期 / 跨机 route mTLS）时跑，且只跑相关的那一个。跑在 `weilandserver`；
    **本机就是它**（`hostname -I` 含 `192.168.1.150`）时直接 `cd test/simcluster && ./local.sh drill <name>`，
    不要 ssh、不要 `remote.sh`。全套用 `./run-drills.sh`（可并发）。
    **定位铁律：忠实复现真实部署环境、如实暴露缺陷，绝不替 tether 弥补**——
    tether 干不了的只呈现（标 `[GAP #N]`）、不代劳；靠复杂脚本才"成功"的操作是缺陷不是成就。
    完整 Mandate 与用法见 `test/simcluster/README.md`，凭据/资源见 `docs/devices-ops.local.md §6`。
- **提交前硬闸**：`make test` + **`make e2e-parallel`**（唯一的全矩阵闸门；全量串行 target 已删除，**严禁**并行过了再手工串行"复核一遍"）+ `make lint` 全绿，并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门（非 goleak）；**任一不过不算 done**。

## 6. Git

- 只在 phase 收尾（step 7）、内审+外审通过后才 `commit`/`push`；在默认分支（`main`）上**先开分支**（`phase/<N>-<slug>`，每个 phase 至少一个 PR）。
- commit message：conventional commits `<type>(scope): <imperative summary>`（如 `feat(ps): retention-bounded ps RPC`、`fix(auth): grant $JS.API.DIRECT.GET`）。
- **绝不添加 `Co-Authored-By: Claude` 或任何 AI 署名**（全局规则；已推送的若混入，用户会 rebase 移除）。作者/协作者只保留用户本人。
