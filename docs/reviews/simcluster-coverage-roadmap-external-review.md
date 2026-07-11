# Fail — Simcluster Coverage Roadmap External Review

Date: 2026-07-10

Review target: `docs/reviews/simcluster-coverage-roadmap.md` (rev2)

Review role: independent external reviewer. `simcluster-coverage-roadmap-review.md` was used only as
an index of claims; none of its conclusions were accepted without re-reading the roadmap, governing
documents, current command surface, implementation, harness, and relevant tests.

## Conclusion

**Fail.** This is correctly shaped as a roadmap-only stage, but it is not yet an executable or
complete coverage contract. One proposed harness change directly conflicts with a deliberate
loopback security boundary; several supposedly independent batches depend on tunnel wiring owned by
S3; three destructive/chaos journeys cannot reach the state they claim to test; and the “no omission”
gate cannot discover hidden commands and currently omits material B/C/security behavior.

No S1-S9 implementation should start from rev2. Revise the roadmap, then re-run external review. The
required revision is limited to the roadmap: this review does not request product or drill
implementation in this special stage.

## Findings

### F1 — Blocker — S4 requires an HTTP bind that the product intentionally refuses

Roadmap evidence:

- §S4 says to bind `broker.sub.listen` to the Docker bridge and treats this as ordinary environment
  provisioning (`simcluster-coverage-roadmap.md:322-323`).
- OQ-5 repeats that configurable listeners may be bridge-bound (`:690-692`).

Independent evidence:

- `internal/subhttp/subhttp.go:34-46,270-280` rejects every non-loopback address. The error explains
  why: this endpoint vends token/PSK material and must be fronted by Caddy; plaintext non-loopback
  exposure is forbidden.
- `internal/clustermanifest/manifest.go:19-33` imposes the same loopback-only boundary on the
  unauthenticated discovery manifest.
- The focused non-loopback rejection tests pass.

Therefore S4 cannot start as specified. Weakening the bind or classifying it as a harness fix would
violate both the product's security invariant and Mandate ①/④. The roadmap must instead choose one
of these faithful designs:

1. exercise the loopback handler from the broker namespace and add an environment-provided reverse
   proxy that faithfully represents the production Caddy trust boundary for cross-container HTTPS;
2. split handler correctness and public-ingress behavior, with the latter explicitly assigned to a
   staging/real-Caddy gate; or
3. mark the public leg NOT-COVERED with a precise owner while retaining broker-local assertions.

OQ-5 must say that **both** named listeners are loopback-only; “configurable” does not mean they may
be publicly rebound.

### F2 — Major — Batch independence is false because the shared tunnel prerequisite belongs only to S3

Roadmap §2 declares S2/S4/S6/S7/S8 independently reorderable and gives only S3 ownership of the
agent reverse-tunnel wiring (`:177-191,274-294`). Current `simcluster agent-join`, however, starts the
agent without `--tunnel-addr` and writes no equivalent `agent.yaml` setting
(`test/simcluster/simcluster:331-355`), so it falls back to `127.0.0.1:7000` instead of the broker
container.

This prerequisite is not S3-local. At minimum the following planned arms require a real tunnel:

- S2-81: active expose during eviction;
- every S4 proxy data-plane arm;
- S6-40: expose fixture during drain/retire;
- S7-50/51: expose backup/DR restoration;
- S9-94/95/96: port reconciliation and live expose/PTY chaos.

The independent agent tests also confirm that proxy readiness is not ACKed when no tunnel exists.
Move faithful tunnel provisioning into a shared S0/public harness prerequisite inherited by the
first data-plane batch, or declare hard dependencies on the batch that supplies it. The reordered
batch rules must carry this prerequisite just as they already carry the gotcha ledger and numbering
work.

### F3 — Major — The “no omission” gate is neither complete nor capable of enforcing its claim

Roadmap §0.1 promises that every shipped product surface is mapped or explicitly NOT-COVERED
(`:30-36`), while §0 and §4 claim that `tether <family> --help` catches hidden commands
(`:18-20,526-530`). Cobra intentionally omits `Hidden: true` commands from help. The current source
contains such commands—for example `takeover-natsconf` at
`cmd/tether/cluster_natsconf.go:382-390`—so the stated gate cannot prove what it says.

Inspection of the built v0.4.7-derived executable and source also found material current surfaces
without an explicit drill assertion or NOT-COVERED row, including:

- B7 `cluster status --card` (`cmd/tether/cluster.go:161`);
- B7 `cluster recovery force-single --guided`
  (`cmd/tether/cluster_offline.go:214-220`);
- C7 `cluster retire --compromised --require-credential-rotation`, including the persistent
  `manual:credrot:<node>` NOT-SAFE alert (`cmd/tether/cluster_retire.go:18-141`);
- the default follower-backup refusal and explicit `--allow-stale-follower` stale-snapshot path;
- `proxy status --cluster`;
- C1/C5/C6 operational events such as `agent_roster_stale`, proxy no-ready/partial transitions,
  `home_reassign_*`, `rehome_stalled`, and `broker_down_rehome_summary`.

These are especially important because the roadmap explicitly claims complete B/C coverage. Replace
the text-help diff with a source/command-tree inventory that records hidden state and flags, and add
an architecture event/workflow inventory. Each item must map to a meaningful assertion or an
explicit NOT-COVERED decision; a generic command smoke test is not sufficient.

### F4 — Major — S1's remote-filesystem contrast contradicts the shipped default

S1-62 expects “`exec` default hangs vs `--safe` fast failure”
(`simcluster-coverage-roadmap.md:226-228`). Since v0.3.3 the documented default is
`remote_fs.mode: auto`, which already detects an unhealthy network mount and fast-fails; `--safe`
is the manual override when configuration is `mode: off` (`docs/usage.md:1329-1342`).

As written, the baseline may correctly fast-fail, making the expected contrast false. Specify three
separate arms:

1. faithful default `auto` fast-failure;
2. an explicitly configured `mode: off` legacy-risk baseline, with a bounded external watchdog so
   the drill itself cannot hang forever;
3. the same `mode: off` configuration with `--safe`, proving the override.

The spike must distinguish true uninterruptible NFS semantics from a FUSE approximation rather than
silently treating them as equivalent.

### F5 — Major — S2 conflates CONNECT-time tenant isolation with application errors and exempts a security promise

S2-80 says every cross-session verb must return `not_a_member` and separately proposes a raw NATS ACL
check (`:244-250`). With auth_callout enabled, login activation for a non-member is rejected during
CONNECT (`cmd/tether/login.go:67-80`), and a credential activated for session A is denied by NATS
permissions before an application handler can consume a session-B subject. That path cannot reliably
produce an application-level `not_a_member` for “every verb.”

Split the security oracle into:

- non-member session activation rejected at CONNECT;
- raw cross-session publish **and subscribe** rejected by NATS ACL in both directions;
- within-session non-owner operations reaching the broker and returning `not_owner` or the precise
  resource-owner code.

The same drill says not to assert a nonexistent rate-limit promise (`:245-247`), but authoritative
architecture §E.6 promises at most 10 PIN attempts per IP per minute
(`docs/architecture.md:825`). The inspected authentication path verifies PINs and emits `pin_failed`,
but no per-IP PIN-attempt limiter was found. Under this roadmap's “expose defects” mandate, that is a
security gotcha candidate, not an allowed omission. Add a deploy-tier rate-limit probe and capture
absence as a signature-guarded product RED if reproduced.

### F6 — Major — S7 backup and total-loss DR lack the required data-survival semantics

S7-50 says an online backup may be taken on any node, including a follower (`:427-436`). The CLI
defaults to a leader-only fresh snapshot and refuses followers unless the operator explicitly passes
`--allow-stale-follower`; that path is labelled possibly stale
(`cmd/tether/cluster_backup.go:23-31,50-69`). A stale follower bundle cannot be treated as an
interchangeable source for the X/Y/Z content-identity oracle.

Add a paired default-refusal assertion and an explicit stale-follower arm. Use a leader-created
bundle for deterministic restore identity unless the test deliberately establishes and checks a
known stale boundary.

S7-51 then deletes every broker container and volume but never defines where “the latest bundle” is
stored (`:437-440`). A bundle inside a container writable layer or deleted volume disappears in the
same disaster. Define an off-cluster/host backup-store role, export the bundle there, prove it remains
readable after total deletion, and only then restore into fresh nodes. Also distinguish restoring
cluster identity/secrets from “secrets reset”; inventing fresh trust material would not be restoration
of the original fleet.

### F7 — Major — S8-91 destroys the only survivor before trying to use a different survivor

The planned sequence is grow → retire → offline force-single C arm (roster becomes only the survivor)
→ kill the floor broker → refresh through a non-floor survivor (`:456-463`). After offline
force-single there is one survivor by definition. If that node is the floor and is killed, there is
no non-floor survivor; if it is not the floor, killing the already-retired/dead floor does not inject
the intended failure.

The originating G3 plan treated A/B/C/D as independent arms. Preserve that isolation: use separate
fixtures/reset points, or execute and verify D on a healthy multi-broker topology before destructive
C and rebuild the topology afterwards. The roadmap must name which broker the ctl is pinned to and
prove the pre-failure path.

### F8 — Major — S9's proposed orphan cannot be created through the stated control path

S9-94 says to SIGSTOP the broker “while the agent starts a process” and then reconcile the orphan
(`:499-503`). Managed exec/run starts are broker-forwarded operations. Once the broker is stopped it
cannot deliver the start request to the agent, so the roadmap does not describe a reachable state.

Use a deterministic fault at the actual state boundary. For example, start a managed process
normally, stop the broker, deliberately lose only its matching committed row while preserving the
agent's local process state, then reconnect; alternatively define a narrowly controlled start/
commit interruption seam. Any fixture needs an explicit Mandate ④ justification and must assert the
specific `killed_orphan` audit record (with its no-RC semantics), not merely process disappearance.

### F9 — Major — S9 chaos has two false-green fault models

First, “kill a follower while a PTY session runs” (`:510-512`) does not establish that the PTY's ctl
or agent connection traverses that follower. The test can stay green while killing an unrelated
broker. Pin or observe the connection through the victim, prove that path before injection, then kill
it and assert the intended reconnection/continuity behavior.

Second, the roadmap equates `docker network disconnect` with silent packet loss (`:512-514`). Docker's
official command contract only says it disconnects a container from a network; it does not guarantee
a silent half-open partition or preserve an interface long enough to exercise timeout behavior
([Docker CLI reference](https://docs.docker.com/reference/cli/docker/network/disconnect/)). Use a
scoped `nftables`/`iptables` DROP or `tc` fault inside the target namespace, with positive traffic
before injection, proof that the processes and local ports remain alive during the fault, and a
cleanup trap that removes the rule. Separately retain `network disconnect` only if its actual reset/
route-removal semantics are the behavior being tested.

### F10 — Minor — The soak leak oracle cannot detect the leak class it claims to cover

S9-97 treats “no journal panic/FK/goroutine explosion string” as no leak evidence (`:516-519`). Slow
goroutine, file-descriptor, thread, or RSS growth normally produces no journal signature.

Record broker and agent baselines from `/proc/<pid>/fd` and `/proc/<pid>/status`, sample after each
settled cycle, and define bounded high-water/slope tolerances. Preserve journal/FK checks as separate
crash/integrity oracles. The roadmap should also say how restarted PIDs are re-resolved and how fault
rules/background clients are cleaned up.

### F11 — Minor — The future batch workflow contradicts the repository's Git contract

Roadmap §6 ends every batch with “直提 main” (`:654-660`), while `CLAUDE.md:69-72` requires a
`phase/<N>-<slug>` branch and at least one PR before main. Replace this with the authoritative branch/
PR flow. This is a process defect in the roadmap, not permission to change repository policy.

## Doubts requiring explicit roadmap decisions

1. Is a faithful reverse proxy part of the sim environment, or is public HTTPS intentionally a
   staging-only responsibility? The present “no Caddy” rule and S4 public HTTP acceptance cannot both
   stand without a clearly bounded substitute.
2. Does “all shipped product surfaces” include operational `sys.events` and persistent safety alerts?
   The architecture treats them as operator-facing contracts. If roadmap owners intend a narrower
   meaning, §0.1 must say so and every excluded event family must still be listed as NOT-COVERED.
3. Where is the authoritative off-cluster backup repository for a full-volume-loss exercise, and
   which identity material is expected to survive outside the failed cluster?
4. Is architecture §E.6's per-IP PIN limit still authoritative? Under the repository's stated
   precedence it is; if the product intentionally dropped it, the architecture must be corrected in
   a separate documentation change and the residual brute-force risk explicitly accepted.

## Recommendations

- Add a small “S0 shared fidelity prerequisites” section for tunnel wiring, identity/home layout,
  common public-ingress policy, artifact/backup stores, and reusable fault primitives. Then derive
  each batch's hard prerequisites from it.
- Replace prose totals and text-help checks with a generated inventory checked into each leaf plan:
  command path, hidden bit, behavior-changing flags, architecture event/alert, assigned drill, and
  deployment-specific assertion or NOT-COVERED reason.
- Define terminology for known defects: a reproduced product RED should be a **harness-green,
  signature-guarded known defect**, distinct from an infrastructure failure and from an unexpected
  product failure. “Three green runs (known RED excepted)” is otherwise ambiguous.
- For every destructive arm, require a named pre-fault data/connection baseline, authoritative
  observation source, exact injected boundary, post-fault semantic oracle, and unconditional cleanup.

## Independent verification log

Completed:

- Read the roadmap and internal review in full; read `CLAUDE.md`, authoritative architecture and
  user/broker/cluster/runbook material, relevant B/C/G plans and reviews, gotcha history, simcluster
  README/code/scripts, existing seven drills, and the local sim-server operations section. No
  credentials or private server details are reproduced here.
- Rebuilt the current command tree from a locally built binary and source, including hidden command
  registration and behavior-changing flags.
- Built `./cmd/tether` successfully using a temporary Go build cache.
- Ran focused tests for loopback HTTP boundaries, cluster manifest binding, auth/command-tree and C7/
  B7 behavior, and agent proxy/tunnel readiness. Sandbox networking initially denied loopback/socket
  creation; the identical focused sets were rerun in the approved host context and passed.
- Ran `bash -n` on simcluster Bash entrypoints/drills and `sh -n` on POSIX helper/provision scripts;
  all passed.

Not run:

- No live simcluster drill was run. This is a documentation-only roadmap stage; the current seven
  drills cannot validate unimplemented S1-S9 behavior, and `CLAUDE.md` explicitly prohibits
  unnecessary deploy-tier runs. The server information was nevertheless reviewed to validate that a
  future live run is available and to avoid mistaking local sandbox limits for product limits.
- Full `make test`, `make e2e`, and `make lint` were not run because there is no product or harness
  implementation diff. Focused independent checks directly exercised the disputed claims.

Final whitespace/cached-diff checks and staging status are recorded by the accompanying external
review tasklist and repository index.

---

# 主进程逐条回复（2026-07-10，roadmap rev3 落点）

独立复核结论先行：对 F1/F3/F5/F6 的 load-bearing 代码断言做了逐处抽验
（`internal/subhttp/subhttp.go:34-46` requireLoopback、`internal/clustermanifest/manifest.go:19-33`、
`cmd/tether/cluster_natsconf.go:389` Hidden、`docs/architecture.md:825` PIN ≤10/min/IP、
`cmd/tether/cluster_backup.go:69` `--allow-stale-follower`、`cluster.go:161` `--card`、
`cluster_offline.go:219`（flag 注册；:133/:159-162 为变量与消费点）`--guided`、
`cluster_retire.go:18-30` `--compromised`、
`proxy.go:215` `--cluster`、`broker.go:323`/`proxy_reconcile.go:152-262` 事件族、
`login.go:67-80` CONNECT 拒），**全部属实**。11 条 finding 全部采纳；4 个 doubt 均给出显式
roadmap 决定；4 条 recommendation 全部采纳。roadmap 已修订为 rev3。

## Findings 逐条

- **F1（Blocker，采纳）**：rev2 的「`broker.sub.listen` 绑桥地址」确与产品**有意的 loopback-only
  安全边界**冲突（两处 listener 都 fail-closed 拒非 loopback，错误文案自证边界意图）——该 harness
  方案作废。rev3 采纳你给出的**方案 1**：/sub 与 well-known manifest 保持产品 loopback bind，
  环境提供一个**显式标注的反代角色**（bridge→loopback，忠实代表生产 Caddy 的信任边界；无 ACME/
  无真域名，NON-GOAL 不变，rev3 在 §S0/S4 明确其「有界替身」地位与 Mandate ④ 说明）；72/73/82
  的跨容器消费一律经该反代，broker 本机 loopback 断言保留。OQ-5 重写为「两个 listener 均
  loopback-only 系产品设计，永不改绑」。
- **F2（Major，采纳）**：隧道接线确非 S3 私有前置——S2-81/S4 全部数据面/S6-40/S7-50/51/
  S9-94/95/96 都要它。rev3 新增 **§S0「公共保真前置包」**（隧道接线、身份/家目录布局、公网
  ingress 反代、artifact 库、离簇备份库、故障注入原语、pty 驱动、台账/README），每项标默认落地
  批 + 「**首开需要它的批负责落地**」规则；§2 依赖图为 S2/S4/S6/S7/S9 标注 S0-隧道硬前置；重排
  继承条款从「台账+README」扩为「S0 全部未落地项」。
- **F3（Major，采纳）**：`--help` 树确实看不见 `Hidden: true` 命令（`takeover-natsconf` 实证），
  该闸门声明不成立。rev3 改为**源码级命令树清单**（cmd/tether 源码枚举，含 Hidden 位与行为
  flag）+ **架构事件/告警清单**双对照；你列举的遗漏面全部补行：`status --card`→93、
  `recovery force-single --guided`→42、`retire --compromised --require-credential-rotation` +
  `manual:credrot:<node>` NOT-SAFE 告警→52、backup follower 默认拒 + `--allow-stale-follower`→50、
  `proxy status --cluster`→73；新增 **§4.6 事件/告警面清单**（`agent_roster_stale`、proxy
  no-ready/partial、`home_reassign_*`、`rehome_stalled`、`broker_down_rehome_summary`…逐行归批）。
- **F4（Major，采纳）**：rev2 的对照确实与 v0.3.3 缺省 `mode: auto`（已自动快速失败）相悖。
  62 重写为三臂（① 缺省 auto 快速失败=忠实基线；② 显式 `mode: off` 遗留风险基线 + **外部有界
  watchdog** 防 drill 自身挂死；③ 同 `mode: off` + `--safe` 证明覆写），并要求 spike 显式区分
  真 D 态与 FUSE 近似、不得静默等同。
- **F5（Major，采纳）**：auth_callout 下非成员在 **CONNECT 期即拒**（login.go 实证）、
  session-A 凭据到不了 session-B 的应用层——「每个动词拒 `not_a_member`」的 oracle 不成立。
  80 重写为三分 oracle（CONNECT 拒 / 裸 NATS 跨 session **pub+sub 双向** ACL 拒 / session 内
  非 owner 达 broker 返 `not_owner`）。PIN 限速：你纠正得对——架构 §E.6 明文承诺
  「每 IP 每分钟 ≤10 次」，rev2 的「不断言不存在的限速承诺」是我核对失误；rev3 加 deploy-tier
  限速探针，复现缺失即按 signature-guarded RED 登产品 gotcha（#25+ 候选）。
- **F6（Major，采纳）**：`cluster backup` 默认**拒 follower**（`--allow-stale-follower` 才放行、
  标 possibly-stale）——rev2 的「任意节点含 follower」错误。50 重写：默认拒断言 + 显式
  stale-follower 臂成对；X/Y/Z 同一性 oracle 用 **leader bundle**。51 补 **离簇备份库**
  （host 侧、在 instance nuke 范围之外，先证 bundle 在全灭后仍可读再恢复）；「secrets 复位」
  更正为「从运维密钥库**恢复原节点 secrets**」（绝非新铸信任材料）。
- **F7（Major，采纳）**：C 臂（offline force-single 后仅剩单 survivor）之后确实不存在
  「非-floor 幸存者」可用——rev2 的臂序自相矛盾。91 重排：**D 臂先行于破坏性 C 臂**（健康 N=3
  上：显式命名 ctl 钉定的 floor broker、先证 pre-failure 路径、杀 floor、经非-floor 幸存者刷新）；
  C 臂在独立 fixture/重建后执行，A/C/D 相互隔离（保持 g3-plan 的臂独立性）。
- **F8（Major，采纳）**：SIGSTOP broker 期间无法经产品路径起管理进程（start 是 broker 转发的）
  ——rev2 造法不可达；架构 P8 原型文本同病，登 DOC 候选。94 orphan 臂重定义：正常起管理进程 →
  停 broker → **环境级删除该进程的已提交行**（显式 Mandate ④ 说明：模拟「行丢失/回滚到更旧
  备份」的故障类）→ 重启 → agent 重注册 → orphan 被杀 + **断言 `killed_orphan` 审计记录**
  （含 no-RC 语义），不以「进程消失」为终点。
- **F9（Major，采纳）**：(a) 96 的 PTY 臂先**钉定并观测**连接确实经过受害 broker（显式
  `--nats-url` 指定 + 注入前验证 connected server），再杀再断言；(b) `docker network disconnect`
  文档契约不保证静默半开——分区臂改用目标 netns 内 **nftables/iptables DROP 或 tc**（注入前
  正流量基线、故障期进程/本地端口存活证明、无条件清理 trap）；`network disconnect` 仅在测试
  其真实 reset/路由移除语义时保留。
- **F10（Minor，采纳）**：97 的泄漏 oracle 改为 `/proc/<pid>/fd` + `/proc/<pid>/status` 基线 +
  每轮稳态采样 + 有界高水位/斜率容差；journal/FK 保留为独立崩溃/完整性 oracle；补 PID 重解析
  （重启后）与故障规则/后台客户端清理约定。
- **F11（Minor，采纳）**：roadmap §6 改为引用 CLAUDE.md §6 的权威分支/PR 流（`phase/s<N>-<slug>`
  + 至少一个 PR）。如实说明一处张力供 owner 裁断：仓库近期实际历史均直提 main（无 merge/PR），
  与 CLAUDE.md §6 成文流程不一致——是否修订 §6 以匹配实践属**独立文档决策**，本 roadmap 不越权；
  S 批执行时以届时 CLAUDE.md 为准。

## Doubts 显式决定

1. **反代归属**：采纳「有界替身」路线——sim 环境提供纯反代（无 ACME/无真域名）忠实代表 Caddy
   信任边界，NON-GOAL 保持「no ACME/real domains」，措辞在 rev3 §S0 澄清；真 Caddy+ACME 的公网
   ingress 仍是 staging/实机责任（§4.3 行保留 NOT-COVERED）。
2. **事件/告警属于功能面**：是。§0.1 范围句显式扩为「含 operator-facing `sys.events`/持久告警
   族」；新增 §4.6 清单，每族落批或 NOT-COVERED。
3. **离簇备份库**：host 侧目录（bind-mount，位于 instance nuke 作用域之外）为权威备份库；
   全灭演练先证 bundle 在库中可读。**幸存于集群之外的身份材料 = 各节点 secrets dir 的运维密钥库
   副本**（restore 前置恢复同节点 secrets；tunnel-cert 指纹是 provenance 锚，新铸=非恢复）。
4. **§E.6 限速权威性**：按仓库文档优先级，是权威承诺。S2-80 加探针；若产品缺失 → RED gotcha；
   若 owner 判定产品有意放弃该承诺 → 走独立文档修订把 §E.6 改掉并显式接受爆破残余风险
   （两条路都不允许静默豁免）。

## Recommendations 采纳

- **S0 公共前置节**：已建（见 F2 回复）。
- **生成式清单**：已采纳（源码命令树 + 事件/告警清单进各叶 plan 的收工闸，字段含 command path /
  hidden 位 / 行为 flag / 事件族 / 归属 drill / 断言或 NOT-COVERED 理由）。
- **已知缺陷运行语义**：已定义（§6）——「**harness-green 的 signature-guarded 已知缺陷**」区别于
  infra flake 与 unexpected failure；「全绿（已知 RED 除外）」一律指前者。
- **破坏性臂五要素模板**：已入 §0.4 断言纪律（命名的注入前数据/连接基线、权威观测源、精确注入
  边界、注入后语义 oracle、无条件清理）。

## 复审请求

roadmap 已修订为 **rev3**（同文件）；本回复 + rev3 一并提交复审。本阶段依旧无产品/脚手架实现
变更；未触碰暂存区。
