# tether — 项目工作说明（CLAUDE.md）

> 本文件每个会话加载进上下文，定义**怎么推进本项目**。需求/架构/用法细节在 `docs/`，本文件不重复。
> 工作流移植自 `auto_daemon`，按 tether（Go-only、已发布、phase 序列推进）的实际情况改写。

## 1. 项目与文档地图

- 一句话：**"SSH + 端口暴露" 的 NAT 穿透控制面**——NAT 后的 agent 经 NATS 反连公网 broker，使用者（ctl）经同一 NATS 把命令路由到 agent。三角色同一二进制 `tether`，子命令切换。
- `docs/requirements.md` — 需求基线（唯一需求真相）。
- `docs/architecture.md` — 架构基线（实现尺）。关键：**「里程碑映射」（P0→P11 出口 + post-1.0 叶子增量登记）**、**「关键依赖警告」（先父后子硬约束）**、**「每进入新 phase 的 checklist」**。
- `docs/usage.md` — 使用者手册（`ctl`/`agent`：怎么连、跑命令、传文件、排错）。
- `docs/broker-ops.md` — broker 运维手册（部署 / 配置 / `serve` / `admin` / 备份 / 升级 / broker 侧排错，需 sudo + 域名）。
- `docs/cluster.md` — 集群（HA）手册（`cluster` / `alert` 命令 + quorum 概念；运维演练见 `docs/cluster-runbook.md`）。
- `docs/devices.md` — 实机/设备清单。
- `docs/reviews/` — 每个 phase / feature 的 plan 与各轮 review 报告（`p<N>-plan.md`、`p<N>-review.md`(+`-round2`/`-round3`)、`p<N>-external-review.md`；历史 feature 增量用 `<feature>-plan.md`）。
- `docs/reviews/quality-audit/` — 横切质量审计（concurrency / security / storage-protocol / cli-ux / tests / deadcode）。
- `test/simcluster/` — Docker 模拟集群 dev 工具（deploy-tier drills：真 systemd + 真独立 nats-server + 真跨进程/跨机 route mTLS，抓 hermetic Go 套件够不到的部署/升级 bug 类）。**定位铁律见 §5 「模拟集群定位铁律」+ 该目录 `README.md` 顶部 Mandate**——模拟真实部署、暴露缺陷、绝不替 tether 弥补。

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
  - **按需测试，非必要不跑全量**：开发/迭代时只跑你**碰过的那一块**——单 phase `go test ./test/pX/...`、单 D 矩阵 `go test -tags dN_integration -race ./test/dN/`、改 broker/cluster 逻辑 `go test ./internal/broker/ ./internal/cluster/`（不含重 gated 套件）。`make test`（全包）/`make e2e`（全矩阵 ~6min）只在**提交前硬闸**跑一次，**不要**为验证一处小改反复全量跑（既慢又因满负载竞争徒增 flake 噪声）。`make e2e` 刻意串行（重 clustered-JS/raft 子进程并行会饿死、"routed JS server not ready"——并行加速试过、必 flake、已弃，见 `Makefile`/`all_phases_test.go` 注释）。
  - 表驱动；快测 `make test`（`go test ./...`，用嵌入式 `nats-server/v2/test`，**不需要本机 nats-server**）。
  - 端到端矩阵 `make e2e`（`-tags e2e_matrix`，`test/e2e/all_phases_test.go` 每 phase 一个子进程子测试；单 phase 用 `go test ./test/pX/...`）。**新 phase 的 e2e 进矩阵，作为跨 phase 回归网。**
  - 并发安全：`-race` + **仓库内建泄漏门**（`runtime.NumGoroutine` poll-with-tolerance + fd 基线，见 `test/concurrency/helpers_test.go`；**刻意不用 `go.uber.org/goleak`**）；触碰隧道/PTY/reconcile/传输/Raft 等并发面必须带 race + leak 门。
  - lint：`make lint`（golangci-lint **v2**；v1 在 Go 1.25 模块上会拒跑）。
  - **模拟集群 deploy-tier drill（`test/simcluster/`）——与 `make test`/`make e2e` 并列的第三层测试,但不属于它们**（无 Go `_test.go`、不进 `go test`/CI）：**⚠ 按需运行、非必要绝不运行。** 只在改动了「真实部署栈」面时才跑——`scripts/install.sh` / `nats.conf` 渲染 / systemd unit / 集群生命周期（init/grow/retire/force-single/upgrade）/ 跨机 route mTLS——且平时**只跑相关的那一个** drill（`cd test/simcluster && ./remote.sh drill <name>`）；确需跑**全套**时首选**并发版 `run-drills.sh`**（drill 间 docker 隔离、本可自由并行——旧的"须串行"是误诊，真根因=宿主机 `fs.inotify.max_user_instances=128` 这个 per-uid 计数上限被并发 systemd 容器耗尽，与 CPU/内存/IO 无关；调高到 8192 后实测可稳态并发 ~600 容器/~150 drill，全套 7 个绰绰有余。`run-drills.sh` 自带 inotify preflight + `-j` 并发档 + infra-flake 重跑）。它抓 hermetic Go 套件结构上够不到的部署 bug 类（真 systemd + 真独立 nats-server + 真 install.sh 路径，见 `docs/v0.4.5-ha-grow-ops-gotchas.md`），代价是真起 Docker 容器 + 组 clustered JS（单 drill ~2–10 min、重；跑全套用 `run-drills.sh` 并发，别逐个串行等）。**跑在专用服务器 `weilandserver`（内网 `192.168.1.150`）、不是本地 WSL——`remote.sh` 从 WSL 编译 tether + re-vendor `scripts/install.sh` + rsync + ssh 到服务器上 `docker build` & 跑 drill；服务器凭据/连接/资源在 `docs/devices-ops.local.md §6`，跑 drill 前先读它（免密 SSH 已通、Docker 已装，别误判"本地跑不了"）。** **日常 Go 单元/集成/并发改动完全不碰它**；仅当发布会触碰部署面时,作为 deploy-tier 门跑一次相关 drill。定位/架构/用法见 `test/simcluster/README.md` + 本节下面的「模拟集群定位铁律」。
- **模拟集群定位铁律（`test/simcluster/`，dev 工具、不进 make test/e2e）**：唯一职责是**忠实复现"真实服务器集群的部署环境"**（真 systemd + 真独立 nats-server + 真跨进程/跨机 route mTLS + 真持久盘 + 真 install.sh 路径），并**如实暴露 tether 在真实部署下的一切缺陷**——不是让集群"跑起来"，而是让 tether 的问题"露出来"。四条不可违背：① **绝不迎合 tether 的错误设计**——环境按真实生产搭（如 `/etc/tether` 由 install.sh 留 root-owned），绝不为规避缺陷擅改环境（不 chown、不打补丁、不预置 workaround），tether 该踩的坑照踩；② **有问题就暴露、绝不替 tether 弥补**——凡 tether 本应"几条命令自动完成"、现实却要人工绕过的操作（grow/shrink/upgrade/去集群化…），职责是**原样呈现缺口并断言其存在**，绝不用脚本替 tether 把活干完从而掩盖缺口；每处不得不手动的绕过必须显式标注为 tether 缺陷（`[GAP #N]`）、尽量以 signature-guarded 断言钉住，产品修复落地后再翻成普通 GREEN 回归；③ **界限分明**——部署/供给机器（install.sh 安装、铸/放密钥、起容器）是模拟集群的活，集群操作（init/grow/retire/force-single…）是 **tether 的活**，tether 干不了或干不干净的只呈现、不代劳；④ **判定反转**——一次操作若靠模拟集群写的复杂脚本才"成功"，那不是模拟集群的成就、而是 tether 的失败被掩盖，应视为**缺陷**并改成暴露；模拟集群越"省事"越可疑。详见 `test/simcluster/README.md` 顶部 Mandate。
- **提交前硬闸**：`make test` + `make e2e` + `make lint` 全绿，并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门（非 goleak）；**任一不过不算 done**。

## 6. Git

- 只在 phase 收尾（step 7）、内审+外审通过后才 `commit`/`push`；在默认分支（`main`）上**先开分支**（`phase/<N>-<slug>`，每个 phase 至少一个 PR）。
- commit message：conventional commits `<type>(scope): <imperative summary>`（如 `feat(ps): retention-bounded ps RPC`、`fix(auth): grant $JS.API.DIRECT.GET`）。
- **绝不添加 `Co-Authored-By: Claude` 或任何 AI 署名**（全局规则；已推送的若混入，用户会 rebase 移除）。作者/协作者只保留用户本人。
