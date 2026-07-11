# Fail - Simcluster Coverage Roadmap External Review Round 2

Date: 2026-07-10

Review target: unstaged rev3 changes to `docs/simcluster-coverage-roadmap.md`, relative to the
staged rev2 baseline, plus the maintainer response appended to the round-1 external review.

## Conclusion

**Fail.** Rev3 makes substantial, correct progress, but it does not close all round-1 findings. F4,
F7, and F9 are closed; most of F6's backup semantics are corrected; and the destructive-arm/result
terminology additions are useful. However, the replacement for the round-1 blocker is still not an
executable security boundary: S0-ingress specifies neither a shared network namespace nor HTTPS/CA
trust, even though the invite path requires HTTPS and `/sub` returns an HTTPS URL. Five other major
residuals remain in the completeness gate, S0 dependency ownership, PIN rate-limit oracle, backup
cleanup, and orphan fault fidelity.

Do not start S0-S9 implementation from rev3. This remains a roadmap-only failure: revise the roadmap
and maintainer response; do not add product or harness implementation in this stage.

## Round-1 closure matrix

| Item | Round-2 status | Result |
|---|---|---|
| F1 / D1 — loopback HTTP boundary and ingress decision | **Open** | Product listeners stay loopback, but the replacement lacks same-netns reachability and TLS trust; R2-F1. |
| F2 — shared tunnel/dependency ownership | **Open** | S0 registry added, but local dependencies and reorder ownership contradict it; R2-F3. |
| F3 / D2 — no-omission gate and event scope | **Open** | §4 rule improved, but the header still trusts `--help` for hidden commands and §4.6 is incomplete; R2-F2. |
| F4 — remote-fs default | **Closed** | Auto/off/off+safe arms and external watchdog are now distinct; FUSE is not equated with D-state. |
| F5 / D4 — isolation and PIN rate limit | **Partial** | CONNECT/ACL/owner split is correct; the rate-limit probe is vacuous; R2-F4. |
| F6 / D3 — backup and full-loss DR | **Partial** | Leader/follower and original-secret semantics are fixed; off-cluster storage escapes final cleanup; R2-F5. |
| F7 — G3 A/C/D sequencing | **Closed** | D precedes destructive C, broker roles are named, and C gets an isolated/rebuilt fixture. |
| F8 — orphan reachability | **Partial** | The original impossible SIGSTOP sequence is gone, but direct replicated-row deletion violates the state model it claims to simulate; R2-F6. |
| F9 — PTY victim and silent partition | **Closed** | Victim path is pre-proven; netns DROP/tc replaces `network disconnect`; cleanup is mandatory. |
| F10 — resource leak oracle | **Closed with minor correction** | FD/RSS/thread sampling and PID re-resolution are useful; the optional goroutine wording names telemetry that does not exist; R2-F8. |
| F11 — Git process | **Partial** | PR flow is referenced, but the example branch pattern does not match `CLAUDE.md`; R2-F7. |

## Findings

### R2-F1 — Blocker — S0-ingress is still unreachable or insecure under its written contract

Rev3 now correctly preserves the two product listeners as loopback-only, but describes the
replacement only as a “pure proxy role (bridge→loopback)” with no ACME or real domain
(`simcluster-coverage-roadmap.md:192,361-365,791-796`). Two load-bearing details are absent:

1. **Network namespace.** A normal proxy container attached to the same bridge cannot reach a
   broker container's `127.0.0.1`; its loopback belongs to itself. Docker documents that a container
   must use `--network container:<name>` to access another container's loopback/network stack
   ([Docker networking overview](https://docs.docker.com/engine/network/)). The roadmap must choose a
   broker-local process or a sidecar sharing each broker's netns, and define how its bridge listener
   is addressed in N=1/N=3 topologies.
2. **TLS and trust.** Agent invites reject a plaintext bootstrap URL unless the development bypass is
   enabled (`internal/clusterroster/invite.go:180-195`), and generated subscription URLs default to
   `https://<public-host>/sub/<token>` (`internal/broker/proxy.go:1069-1076`). The roadmap specifies
   neither TLS termination, SANs, per-instance CA distribution, nor client certificate verification.
   A plaintext bridge listener would recreate the token/PSK exposure that the product's
   `requireLoopback` check was designed to prevent. Setting `TETHER_DEV_NO_AUTH=1` would weaken the
   production contract and is not a faithful solution.

Specify a per-broker, same-netns HTTPS reverse proxy, an instance-scoped test CA and SAN/hostname
scheme, trust-store provisioning for ctl/agent consumers, positive certificate verification, and
wrong/untrusted-certificate negative controls. The public listener must be torn down with the
instance. “No ACME/real domain” is compatible with test TLS; it is not permission to omit TLS.

### R2-F2 — Major — The declared no-omission gate still contradicts itself and omits shipped events

The central §4 rule now correctly says source inventory must include `Hidden` and behavior flags
(`:597-606`), but the document header still says `tether <family> --help` catches hidden/new commands
(`:18-22`). That is the exact false claim F3 identified.

More importantly, rev3 expands scope to all operator-facing events and persistent alerts
(`:30-39`) but §4.6 is not a complete inventory. Current source/architecture exposes, among others:

- H.1 `session_created`, `session_destroyed`, `member_joined`, `tetherd_restarted`,
  `agent_registered`, plus promised `rotated_pin`, `kicked`, and `agent_unregistered`
  (`docs/architecture.md:1190-1192`, `internal/broker/audit.go:32-35`);
- `proxy_enabled`, `proxy_disabled`, `proxy_keyset_changed`, `proxy_node_unready`, and the exact
  transition kinds `sub_render_empty`, `proxy_no_ready_nodes`, `proxy_partial`;
- `nats_topology_*`, `grow_cutover_revival_failed`, and other current `pubSysEvent` emitters.

They are absent from §4.6. Conversely, the table lists `session_deleting` as a broadcast covered by
S2-81 (`:725`), while source search finds it as an error/state code but no corresponding
`pubSysEvent`; `docs/usage.md:580` nevertheless promises such a broadcast. That is precisely the kind
of product/document gap this roadmap must probe and record, not silently count as covered because
the broader session-removal journey runs.

Replace the stale header statement and generate the full inventory now (or attach it as a roadmap
appendix), including architecture-promised but currently writerless kinds. Every exact kind must map
to an observable assertion or explicit NOT-COVERED/gotcha probe. Generic labels such as “proxy
no-ready / partial” do not account for `sub_render_empty` and `proxy_node_unready`.

### R2-F3 — Major — S0 ownership and the per-batch dependency declarations disagree

The central graph says S2, S4, S6, S7, and S9 require S0 items (`:184-210`), but their local contracts
still say:

- S2 “dependency: none” (`:269`) despite S0-tunnel and S0-ingress;
- S4 “dependency: none” (`:338`) despite S0-tunnel and S0-ingress;
- S6 “no hard prerequisite” (`:417`) despite S0-tunnel for drill 40;
- S7 “no hard prerequisite” (`:468`) despite S0-tunnel and S0-backup;
- S9 “dependency: none” (`:554`) despite S0-tunnel and S0-fault.

The reorder rule also says the first opened batch inherits **all** unlanded S0 items (`:212-214`),
contradicting the preceding rule that a batch owns the items it actually needs (`:186-187`). Taken
literally, opening S4 first also absorbs unrelated upgrade artifacts, backups, remote-fs PTY work,
and S9 fault primitives. S0-pty says every typed-confirm arm consumes it, but S6/S7 do not declare
that dependency; S0-tunnel's consumer column omits its default consumer S3.

Make one rule authoritative: each batch must land every **required and not-yet-landed** S0 item, while
the first batch of the whole program additionally lands the all-batch ledger/README base. Repeat the
exact S0 requirements in every local `依赖`/`harness 增量` block, include S3 and all typed-confirm
consumers, and define how later plans record that a shared prerequisite is already landed.

### R2-F4 — Major — The PIN rate-limit probe cannot distinguish limiting from normal rejection

S2-80 sends more than ten **wrong** PINs and says to assert that attempts are limited
(`:279-281`). Every wrong PIN is rejected whether a limiter exists or not, so this test can pass on
the current unlimited implementation. No distinct rate-limit error/event is specified by
architecture §E.6, making “assert limited” undefined.

Use an externally observable black-box contrast. One workable design is eleven fresh ctl identities
from the same ctl container/IP using the **correct** PIN: at most ten may join in one minute and the
eleventh must be refused; a fresh identity from a second source IP must still succeed; after the
window, the held same-IP identity must succeed. If the intended limiter counts only failed PINs,
define a stable rate-limit response and test ten wrong attempts followed by a correct fresh identity,
with the same second-IP/window controls. Keep `pin_failed` auditing as a separate oracle.

The leaf plan must name the source-IP observation, the exact window reset rule, and the expected
membership/event counts. Otherwise the security RED remains false-green.

### R2-F5 — Major — The off-cluster backup deliberately escapes the only guaranteed cleanup path

S0 and S7 place the host backup directory outside the instance `nuke` scope
(`:195,483-498`). Current `simcluster nuke` is the drill's unconditional cleanup and removes the
instance's containers, volumes, and secrets stash (`test/simcluster/simcluster:467-471`). Therefore
every S7 run would leave host state behind; a retry could consume a stale bundle, parallel instances
could collide if naming is weak, and operational state would persist after the throwaway safety gate
claims cleanup.

The repository must survive **node/container/volume destruction**, not final instance teardown.
Define an instance-namespaced host path that is outside `rm_node --vols` but inside `simcluster nuke`;
teach the eventual nuke path to remove it. Require a fresh/empty preflight, restrictive permissions,
unique bundle names, and a trap. The original node secrets may remain in the existing instance
operations stash during the DR arm, but both secrets and backups must be deleted at final teardown.

### R2-F6 — Major — The revised orphan fixture mutates replicated state outside Raft

Rev3 creates the orphan by stopping the broker and deleting exactly one committed process row
(`:556-563`), calling this equivalent to row loss or rollback to an older backup. In cluster mode the
code explicitly forbids deleting process rows outside Raft because it forks replicated SQLite state
(`internal/broker/broker.go:1092-1096`). A one-row deletion that leaves `applied_index` and the Raft
log claiming the command was applied is not a coherent older backup and is not a production recovery
state.

This concern refines the example suggested in round 1: the state loss must be coherent with the
actual persistence model. Use either:

- a real older leader backup/product restore taken before the process starts, while the agent and
  process survive, followed by registration against the restored state; or
- a deterministic fault at the real agent-started-event/commit boundary; or
- explicitly run a single-mode broker and justify why direct row loss is a legitimate single-writer
  corruption test rather than a cluster/Raft rollback simulation.

In all cases prove the agent process is alive immediately before reconnect, preserve unrelated rows,
and assert the exact `killed_orphan` no-RC audit plus the returned drop directive.

### R2-F7 — Minor — The Git branch example still differs from the governing contract

Roadmap §6 uses `phase/s<N>-<slug>` (`:755`); `CLAUDE.md:71` requires
`phase/<N>-<slug>`. Referencing the authoritative process is correct, but copying a different pattern
immediately afterwards reintroduces ambiguity. Use the exact current pattern or omit the expansion
and refer only to `CLAUDE.md §6`.

The maintainer response also cites `cluster_offline.go:133-162` for `--guided`; the flag is currently
registered at `cmd/tether/cluster_offline.go:219`. Correct the evidence line while editing the
response.

### R2-F8 — Minor — The soak text names a goroutine source that does not exist

S9-97 says goroutines may be read through expvar (`:582-589`), but the current product exposes no
expvar or goroutine metric. `/proc/<pid>/status` supplies process RSS/thread data, not Go goroutine
count. FD/RSS/thread slopes are still valuable and are sufficient to close the main F10 concern.
Remove the unsupported goroutine wording, explicitly mark it NOT-COVERED at deploy tier, or assign a
future product metric as a separate feature rather than implying the current drill can read it.

## Remaining questions

1. Will S0-ingress be one TLS process inside each broker container, or a sidecar sharing the broker's
   network namespace? A normal bridge-attached “role container” is not viable for loopback upstreams.
2. Does “all operator-facing events” mean the architecture H.1 contract only, or every current
   `pubSysEvent` kind? Rev3's D2 decision reads as the latter; the generated inventory must state the
   boundary explicitly.
3. Is S9-94 intended to run in standalone or single-voter cluster mode? That choice determines
   whether direct SQLite mutation violates the replicated-state invariant.

## Recommendations

- Give every S0 resource a lifecycle tuple: owner batch, consumer batches, instance-scoped name,
  creation preflight, secret/trust material, health check, and final cleanup.
- Put the generated command/event inventory next to this roadmap rather than regenerating an
  unreviewed full inventory independently in nine leaf plans. Leaf plans can consume and update one
  checked source of truth.
- For every security throttle/revocation drill, require a successful control from a distinct actor or
  source so “all requests failed” can never count as evidence of the intended mechanism.

## Independent verification

Completed:

- Re-read the full rev3 roadmap, the complete rev2→rev3 diff, the maintainer response, round-1 report,
  governing workflow, and relevant architecture/usage/simcluster sources. The response was treated
  only as a claim index.
- Re-inspected the built command tree and exact help for cluster recovery/status/retire/backup and
  proxy status, plus source Hidden registrations and behavior flags.
- Enumerated current `pubSysEvent` call sites and alert writers and compared them with §4.6.
- Inspected current Docker instance/nuke lifecycle, listener security checks, HTTPS invite and
  subscription URL behavior, process reconciliation, and Raft/SQLite deletion invariant.
- Focused tests passed:
  `GOCACHE=/tmp/tether-review-gocache go test ./internal/authcallout ./internal/clusterroster ./internal/broker ./cmd/tether -run 'Test(ParseInviteRejectsNonHTTPSBootstrap|EmitEventOnPinFailure|DecideProxyEventsChangeGated|ResolveReconcileMarks_G1Cases|C8RecoveryIsPrimaryTree|C7RotationFlagsRequireCompromised|StatusCard)' -count=1`.
- `git diff --check` passed before round-2 artifacts were staged.

Not run:

- No live simcluster drill: rev3 changes only an unimplemented roadmap and response; existing drills
  cannot validate S0-S9, and the governing test policy prohibits unnecessary deploy-tier runs.
- No full `make test`/`make e2e`/`make lint`: there is no product or harness implementation diff.

Final cached-diff and staging checks are recorded by the round-2 tasklist and repository index.

---

# 主进程逐条回复（2026-07-10，roadmap rev4 落点）

独立复核：R2-F1 的 invite 强制 https（`invite.go:180-195`）与 `/sub` 默认 https
（`proxy.go:1069-1076`）、R2-F6 的 raft 行删除禁令（`broker.go:1092-1096` 注释原文）、
R2-F7 的 `--guided` 注册行（`cluster_offline.go:219`）逐处核对属实；`pubSysEvent` 发射点
全量重枚举与你的清单一致。**8 条 findings + 3 个遗留问题全部采纳**，roadmap 修订为 rev4，
并按你的 recommendation 建了单一真相源清单附录 `simcluster-coverage-inventory.md`。

## Findings 逐条

- **R2-F1（Blocker，采纳）**：rev3 的「bridge→loopback 纯反代」确实双重不可执行——普通
  bridge 容器到不了他容器的 127.0.0.1（须共享网络栈），且 invite 拒明文 bootstrap、`/sub`
  URL 默认 https，明文桥监听会重造 requireLoopback 防的 token/PSK 暴露。rev4 的 S0-ingress
  重定义为：**每 broker 一个共享其 netns 的 HTTPS 反代**（sidecar `--network container:<brk>`
  或容器内进程，与生产 Caddy 同宿主同拓扑）+ **实例作用域测试 CA**（与 S0-artifact 共用铸造
  设施）+ SAN=broker 主机名 + ctl/agent trust store 注入 + **证书正向校验断言与错误/不受信
  证书负例** + 随 instance 拆除；N=1/N=3 按 `https://<brkN>` 寻址。明确写入「绝不设
  `TETHER_DEV_NO_AUTH`」与「无 ACME/真域名 ≠ 免 TLS」。S4 harness/82/OQ-5 同步改写。
  遗留问题 1 的裁定：**sidecar 共享 netns 或容器内进程均可，s4 plan 二选一定稿**——排除的
  只是「普通 bridge 角色容器」。
- **R2-F2（Major，采纳）**：① 头部残留的 `--help` 谬称是 rev3 漏改，已替换为「源码级命令树
  与事件清单对照（Hidden 在 --help 不可见，文本 help 禁作来源）」。② 完整事件清单**已随
  rev4 生成落档**：`simcluster-coverage-inventory.md`——§1.1 全部现有 `pubSysEvent` kind
  （含你点名的 `session_created/destroyed`、`member_joined`、`tetherd_restarted`、
  `agent_registered`、proxy 五 kind、`nats_topology_<action>`、`grow_cutover_revival_failed`）
  逐行归批断言；§1.2 架构承诺但**无 writer/命名漂移**行（`rotated_pin`/`kicked` vs
  `agent_evicted`/`agent_unregistered`）列为 probe；§1.4 把「proxy no-ready/partial」拆到
  精确 kind（`sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial`）。③ `session_deleting`
  按你的定性改为 **probe 而非 covered**（usage:580 诺广播、源码无 writer——S2-81 验证 agent
  拒调用的真实机制，承诺不实则登 gotcha/DOC）。roadmap §4.6 不再内联复制表格、只留边界声明 +
  指针（防双份漂移）；遗留问题 2 的裁定在附录 §0：边界 = H.1 契约 ∪ 全部现有 pubSysEvent ∪
  专用通道 kind ∪ 告警 kind。
- **R2-F3（Major，采纳）**：归属规则统一为唯一规则——**每批落地「本批所需且尚未落地」的
  S0 项（不吸收无关项）；工程首开批额外落 S0-台账**；与之矛盾的「首开批继承全部未落地项」
  已删。S2/S4/S6/S7/S9 的局部「依赖：」行逐一改为点名具体 S0 项；S3 行改为「本批落地
  S0-隧道（默认归属）」；S0-隧道消费列补 S3-70/71；S0-pty 消费列改为 S1-60 + S9-96 并注明
  **typed-confirm 臂用既有 `pty-confirm.py`、不依赖本项**（rev3 的「各 typed-confirm 臂」
  是我写错了消费对象）；S0 表新增「状态」列 + 「落地后登记（批+commit），后续 plan 据此判定
  已就绪」的记录规则；每项落地批的 plan 须写全你建议的生命周期元组（已写入 S0 节导语）。
- **R2-F4（Major，采纳）**：你说得对——连发错误 PIN 无论有无限速都被拒，是假阳性 oracle。
  rev4 改为你给的黑盒判别设计：同源 IP 用**正确 PIN** 起 11 个全新身份一分钟内依次 join →
  ≤10 成功、第 11 个必须拒；**第二源 IP 对照**同窗成功（IP 限流而非全局故障）；窗口过后被扣
  身份成功；三点齐才算证明（§0.4 新增「安全限流/撤销类断言必须带不同 actor/源的成功对照」
  通则）。第 11 个也成功 → 限速缺失的 signature-guarded RED。源 IP 观测点/窗口重置/计数由
  s2 plan 命名。`pin_failed` 审计保留为独立 oracle。
- **R2-F5（Major，采纳）**：rev3 把备份库放在 nuke 之外确实泄漏跨轮状态。rev4 修正生命周期：
  **在 `rm_node --vols`（灾难注入）之外、在 `simcluster nuke` 之内**（nuke 学会删它——这是
  S0-备份库落地批的 harness 增量）；实例命名空间化路径 + fresh/空预检 + 0700 + bundle 名唯一 +
  trap；DR 臂期间原节点 secrets 留在既有 instance 密钥 stash，最终 teardown 一并清。
- **R2-F6（Major，采纳）**：直接删复制行确实违反 raft 状态模型（`broker.go:1092-1096` 明文：
  cluster 模式下 raft 之外删 processes 行会 fork 复制态）——rev3 的造法作废。rev4 采纳你的
  **方案一（产品路径）**：进程启动前取 leader `cluster backup` → 起进程 + **证明进程活着** →
  停 broker → `recovery restore`（产品级「回滚到更旧已提交态」，游标重置 + 重 bootstrap，
  raft 一致）→ 起 broker → agent 重注册 → orphan 被杀；断言 `killed_orphan` no-RC 审计 +
  **返回 agent 的 drop directive** + bundle 内无关行完好。遗留问题 3 的裁定：94 跑在**单
  voter 集群模式**（simcluster 全部 broker 经 init 后都是 cluster 模式），故直接 SQLite
  变更被排除、restore 是唯一忠实路径。
- **R2-F7（Minor，采纳）**：§6 不再复制展开分支模式（rev3 的 `phase/s<N>-<slug>` 与
  CLAUDE.md §6 的 `phase/<N>-<slug>` 不符），改为「以 §6 原文为准，本文不复制以免漂移」。
  round-1 回复中 `--guided` 的证据行已更正为 `cluster_offline.go:219`（flag 注册；:133/
  :159-162 为变量与消费点）。
- **R2-F8（Minor，采纳）**：产品无 expvar/pprof goroutine 观测口——97 的 oracle 改为
  fd 计数 + RSS/Threads（`/proc`）斜率/高水位，**goroutine 数显式 NOT-COVERED**（将来产品
  加 metrics 再收，不暗示现 drill 可读）。

## Recommendations 采纳

- **S0 生命周期元组**：已写入 S0 节导语（归属批/消费批/实例作用域名/创建预检/密钥信任材料/
  健康检查/最终清理），各落地批 plan 必须写全。
- **清单集中受审**：已建 `simcluster-coverage-inventory.md` 作为唯一清单（叶 plan 消费并
  增量更新，收工闸按附录 §3 生成法重枚举 + diff），不在九个叶 plan 里各自重生成。
- **限流/撤销的不同源成功对照**：已入 §0.4 通则（并已应用于 80 的 PIN 探针与 72 的 revoke 臂
  语义——后者的「其它订阅不受影响」即对照臂，s4 plan 按通则显式化）。

## 复审请求

roadmap rev4 + 清单附录 + 本回复一并提交 round-3 复审。本阶段依旧无产品/脚手架实现变更；
未触碰暂存区。
