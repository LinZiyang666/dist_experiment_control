# 监工 #3 — 独立需求审查 C5–C6（after C6）

> 独立 agent（general-purpose, Opus 4.8），按 /goal「C6 后 spawn 监工查阳奉阴违 + 是否满足 4 个 v2 文件精神」。审计 C5（proxy cluster 化）+ C6（可观测/命名/recovery 别名）against docs/reviews/v2-{usability-proposals,usability-proposals-gap,automation-program,cli-consolidation-proposal}.md。CLI 精简按用户决定已挪 C8，本次只查 C5/C6 自身功能需求。

## 裁决：CONDITIONAL PASS

**无阳奉阴违 / 无虚假闭合。** 逐条代码核对确认 C5 的 11 个 finding（2 BLOCKER + 4 MAJOR + N0–N4 + N2）+ C6 的根因（LookupProxyByNode 漏 home/epoch）+ MINOR 全部在代码里真闭合（非仅声称）。两处此前可疑点已真修真验：
- **BD1**：decideProxyEvents 此前 0 生产调用方；现追踪到完整 live 链 `observability.go:246 → driveProxyReconcile → reconcileProxySession:172 → emitProxyCountEvents:184 → decideProxyEvents`，单一源、真 wire。
- **N2**：失败 rehome 的 unready 被 exact-equal re-ACK 覆盖——`rehomeFailed` flag 真抑制 + 测试改为真 embedded NATS（非此前 nil-conn 假过），断言终态 `unready`。
- **C5-B2**（FSM panic 根因）：PlanAllocateProxy 现在 Propose 闭包内 applyMu 下 plan，确认 applyMu 序列化为真。
- **C6 根因**：LookupProxyByNode 改 13 列 scanOneWithHome，`TestLookupProxyByNodeReturnsHomeAndEpoch` 钉死（旧 scanOne 会失败）；HR3 永久 rehome 健康 proxy 的门现为 false。

build + 目标测试套（Proxy|Homes|Health|Rehome|Recovery|Status|LookupProxy|Sub × 5 包）全绿。单模式字节等价（所有新路径 `if b.clusterMode`）确认。C5/C6 未让 CLI 变差（纯 additive），C8 可干净收敛。

## CONDITIONAL 条件（追踪 follow-up，非阻塞、非阳奉阴违）

1. **缺 test/c5 + test/c6 gated 多 broker 集成演练**：C5 头号验收「杀 home → 自动切换、无需 proxy off/on」目前只有单元/纯逻辑测试 + 代码逻辑正确性,**无真多 broker kill-home e2e drill**（环境无法实证）。与 D5–D8 gated 套件增量模式一致,但确是头号功能的实证覆盖洞。命名 drill（TestC5KillHomeRehome/NoQuorumWriteRefused/ProxyOffTearsDownDataPlane/RehomeCASSingleWinner 等）未写。
2. **C6-SECRETFREE-1**：homes SELECT 列集 + Errors[] 的 secret-free 静态 guard 测试未落（现状按 inspection 确为 secret-free）。
3. **keyset_stale reason 结构性不可达**：per-node agent keyset epoch 未持久化,故 proxy status/--homes 只surface 5 个 reason 中的 4 个（no_home/catching_up/tunnel_down/ready）——已文档化的限制,非虚假声称。
4. **no-quorum 少数派 ctl 超时**（非 typed degraded code）：N0 amendment,安全成立（无 quorum 不写）,degraded ClusterState 仍可经 proxy status --cluster 观测。

## 主进程处置
全部 ACCEPT 为追踪 follow-up（与 CLAUDE.md 既有 e2e 矩阵覆盖洞同类）：gated test/c5+test/c6 多 broker drill + secret-free guard test 纳入末尾 ≥20 专家大审计前的补测 / 或大审计覆盖。C5/C6 功能需求判定为真闭合,不阻塞推进 C7。
