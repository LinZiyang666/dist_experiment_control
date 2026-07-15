# Fail — S3–S5 (G-A) external re-review round 5

Conclusion: **Fail. Do not release G-A yet.** This decision is not based on whether the drills print GREEN. A
correct RED or NOT-COVERED result can be valuable evidence when it exposes a product defect. The blockers below
are instead test-oracle paths that can turn missing evidence into GREEN, executable removal of locked acceptance
criteria without an owner decision, and current documentation that claims stronger coverage than the code runs.

Reviewer role: independent external reviewer. Developer/self-review conclusions and retained logs were treated as
claims to verify. I reconstructed the twelve-file unstaged delta over the staged round-4 tree, re-read the locked
plan and source, built the round-5 tasklist before detailed review, ran static/adversarial probes, verified the
reviewed files on `weilandserver`, and executed fresh strict-serial simulations with runner retry disabled.

## Round-4 closure matrix

| Round-4 finding | Round-5 disposition | Basis |
|---|---|---|
| R4-M1 — drill 73 local-client false attribution / incomplete runs | **ORACLE FIXED; runtime rechecked** | `ss_up` now requires both a live `ss-local` process and its loopback listener. The new #33 arm keeps the proven pre-crash client for the black-hole edge and uses fresh verified clients for recovery. Runtime result is recorded below. |
| R4-M2 — drill 71 classifier and locked drain semantics | **REPLACED, BUT OPEN** | The unsafe text classifier was removed and crash-stranding is source-supported. The replacement falsely states that a regular expose must use the fixed agent tunnel, does not execute the claimed rebuild-off crash behavior, and removes B/E/G as “owner-accepted” without owner authorization. |
| R4-M3 — drill 72 held stream / allocation reclaim | **PARTIAL / OPEN** | A slow stream and listener check were added, but “marker absent” does not prove the stream ever connected or transferred a byte. OFF checks only a listener, not allocation-row release or safe reuse. |
| R4-M4 — drill 32 manifest / real roles / §8.4 | **PARTIAL / OPEN** | Stat/hash/readlink errors and byte restoration improved; a real agent artifact is installed and run. Partial traversal failure remains accepted, real ctl and locked §8.4 remain absent, and the cleanup trap is overwritten. |
| R4-M5 — drill 74 timing / atomic distribution / locked controls | **PARTIAL / OPEN** | The 180-second budget and one-snapshot computation were added. An empty/invalid snapshot computes spread zero and passes “balanced”; exact events, moved-exit data plane, and the ordinary-expose control remain absent. |
| R4-M6 — contradictory release documents | **OPEN** | The #33 ledger still describes the deleted instant-ready inverted oracle, while README/inventory claim the new measure-only implementation and “2x strict”; #29 and coverage rows also overclaim executable evidence. |

## Release-blocking findings

### R5-M1 — drill 72 can certify a persistent in-flight stream that never established or transferred any byte (Major)

`_ss_hold_open` writes only an **exit** marker (`72-proxy-subscription.sh:147-154`). The pre-revoke predicate at
lines 228-232 defines both streams as in-flight solely because those markers are absent after six seconds. It does
not check slow-sink readiness, curl PID liveness, successful SOCKS connection, HTTP response, or byte growth. A
curl stalled before connecting, a missing sink, or even no curl process at all therefore satisfies
`REV-hold-base`; the synthetic no-process/no-marker case returned success. After revoke, any creation of the alice
marker is accepted as a force-close regardless of exit reason or transferred bytes, while marker absence is
accepted as bob continuity.

The slow sink itself makes the claimed two-stream control impossible: line 143 instantiates Python's
single-threaded `http.server.HTTPServer`, not `ThreadingHTTPServer`. Alice is launched first and can occupy the
sole request handler for about 120 seconds; bob can merely wait in the accept backlog. Marker absence then labels
that waiting request as “KEEPS streaming.” The independent bob control is therefore not proven concurrent or in
flight by design.

The separate short curls to port 9090 prove the subscription can relay ordinary requests, but they do not prove
that these port-9091 connections were established and actively streaming at the injection boundary. This violates
the locked plan's two persistent, byte-observed baselines (`s3-s5-plan.md:172-178`) and permits the exact false
GREEN the destructive control was meant to prevent.

The OFF arm is also still incomplete. Lines 245-250 check that a numeric listener disappears from `ss -ltn`; they
do not verify listener ownership, removal from `port_allocations`, or safe reuse. The locked oracle explicitly
requires the allocation source as well as the listener (`s3-s5-plan.md:181-186`). Calling this “PORT RECLAIM” and
R4-M3 “NOW-COVERED” at line 252 is therefore stronger than the executable evidence.

Required fix: make the sink readiness a prerequisite; record bytes (or monotonic byte count) separately for alice
and bob before injection and across the revoke; retain PID/exit status so early failure cannot mean force-close;
and close OFF with authoritative allocation removal plus a controlled same-port reuse probe.

### R5-M2 — drill 74 treats a failed/empty status snapshot as a perfectly balanced distribution (Major)

The move to a single snapshot is directionally correct, but `_snap_homes` at
`74-rebalance-on-return.sh:26` discards both `tether proxy status` and `jq` failures. `_spread` at lines 29-33 then
counts an empty list as `0/0/0` and returns zero, so `_spread_le1` succeeds. The adversarial empty-status probe
produced `spread=0` and classification rc=0. The same empty snapshots can compare byte-identical in
`_dist_stable` and `_rebalance_dryrun`. A control-plane outage or malformed JSON can therefore certify automatic
or manual balance and dry-run stability.

The live baseline also does not enforce the locked one-per-voter setup. The current run printed
`brk1=0 brk2=1 brk3=2` immediately after the assertion labelled “homes spread across voters”; no assertion
requires `1/1/1` before the first destructive arm. The locked plan requires one exit per voter and flowing bytes
before skew (`s3-s5-plan.md:219-225`), while the implementation accepts merely that the selected non-leader has
at least one home and has no initial per-exit byte baseline.

The locked plan additionally requires exact-one `proxy_auto_rebalanced` anti-flap evidence, post-move `/sub` and
SS bytes per exit, and a co-homed ordinary-expose negative control (`s3-s5-plan.md:219-242`). Line 161 explicitly
leaves all of those NOT-COVERED and delegates the data plane to drill 73, which does not execute the same
rebalance-on-return transaction. Restoring the 180-second timeout does not close those semantics.

Required fix: capture one JSON document with checked command rc, JSON validity, exactly the expected agent rows,
allowed home values, and nonempty homes before computing counts. Make invalid evidence fail closed. Implement the
locked event/data-plane/control arms or record an actual owner-approved scope change.

### R5-M3 — drill 71's new #29 account contradicts the delivery source, and its advertised Arm D never crosses a crash (Major)

The new comments and release documents say a regular expose home **must** equal the agent's fixed tunnel broker
(`71-expose-rehome-failover.sh:2-24`, `deploy-tier-gotchas.md:107-139`). The source implements the opposite:
`internal/broker/home.go:96-112` builds a `HomeDirective` containing the allocated home's `TunnelAddr` and cert
pins, and `internal/agent/tunnel_adapter.go:76-80` passes that named address to `OpenHome`; the fixed tunnel is only
the empty-address fallback. This also conflicts with the project's previously observed home-not-equal-tunnel
successful deliveries. Crash stranding itself is supported by `rehome_events.go:51-61`; the added unsupported
“create must be tunnel-coupled” cause is not.

The script also claims Arm D proves that `--no-rebuild` goes down with its home, but lines 80-89 only create it,
verify `rebuild:false`, curl steady state, and **remove it before the crash at line 96**. Thus no rebuild-off crash
or drain behavior is observed. A future regression in that behavior would remain GREEN while README, inventory,
ledger, and the drill header say it is covered.

Finally, the locked B drain-migrate, E rebuild-off drain refusal, and G stickiness arms are removed at lines
106-109 under the label “owner-accepted.” No such owner decision exists in the reviewed history. The user's
instruction that tests should expose defects does not authorize deleting acceptance criteria. The proposed
fixture excuse is also circular: this same run successfully establishes an agent/expose on brk3, and the source
is designed to deliver named homes. The script does not even gate that brk3 is non-leader before relying on that
topology (lines 67-72).

Required fix: separate the source-supported crash-stranding finding from hypotheses about initial delivery;
actually execute rebuild-off across an injection; restore B/E/G with a source-correct named-home fixture, or cite
a real owner decision changing the locked plan; and hard-gate the chosen target as a live non-leader.

### R5-M4 — drill 32 still does not satisfy the locked S5 contract and introduces unreliable cleanup (Major)

The real agent download/extract/run/uninstall arm is useful progress. The manifest now serializes stat, hash, and
readlink failures and restores its self-test file from a metadata-preserving backup. It still suppresses `find`
errors (`32-install-lifecycle.sh:32-41`): a traversal with one readable root and one failed root emits a nonempty
partial list and contains no error marker, so the manifest is accepted. The adversarial partial-find probe
confirmed rc=0. A permission/read failure on part of the install tree can therefore hide writes.

Two EXIT traps are declared at lines 15 and 19; the second replaces the first, so early failure after artifact
creation does not run `artifact_down`. Normal completion calls it explicitly, but destructive/error cleanup is
the purpose of the trap. More fundamentally, the locked plan requires real install and never-start behavior for
all three roles plus the §8.4 stop→swap→integrity→start→business-convergence path
(`s3-s5-plan.md:317-329`). Only the agent real-placement path was added; ctl and §8.4 remain explicitly
NOT-COVERED at line 132. “Same helper” is not executable coverage of role-specific identity, path, permissions,
uninstall, or never-start behavior, and no owner rescope was found.

Required fix: capture and check traversal rc without losing it through pipelines, compose cleanup into one trap,
execute the real ctl boundary, and implement §8.4 or obtain an explicit owner scope decision.

### R5-M5 — shared grow setup contains an unreported internal retry that can launder product evidence (Major)

`grow_to_3` now ignores both grow command return codes and, when the voter poll fails, nukes the complete cluster
and retries setup (`drills/lib/cluster.sh:9-29`). This happens even under runner `--no-retry`; therefore “strict
no-retry” evidence is only meaningful if logs separately prove that the embedded second attempt did not run.
More importantly, this helper is shared by drill 30 (`30-rolling-upgrade.sh:109-112`), the stated owner of the #31
grow-lock defect. The comment that the retry “does not interfere with 30's #31 pinning” is false structurally:
drill 30 uses the same helper and can lose its first failed grow evidence to `nuke` before its own assertions.

The change also leaves two substantial recovery helpers dead: `_ensure_grow_lock_released` remains defined in
`simcluster:147-173` but is not called; `_clear_lingering_ops` remains in `cluster.sh:31-43` but is unused. The
solo diagnostic at `simcluster:260-268` still says the former “tries to prevent” the lock even though the call was
removed. Runner retry classification does not include voter timeout, while its parallel diagnostic says the
runner retries it. Together these paths make attempt accounting and causal attribution unreliable.

Required fix: do not silently retry the product lifecycle inside a fixture shared with the defect-owning drill.
If setup tolerance is approved for selected consumers, expose attempt number/result as first-class evidence,
keep drill 30 on a no-internal-retry path, preserve failed logs, and reconcile or delete dead recovery code and
diagnostics.

### R5-M6 — drill 73 can claim quorum data-plane separation after its alleged dead-home leg remains live (Major)

The second independent strict run reached every intended arm but finished RED (1 failed / 39 passed). After the
script classified agt2 as homed on brk3 and killed brk3, fresh curls through the supposed dead leg continued to
return sink bytes for the entire 15-second window. The `Q-freeze ... BLACK-HOLES` assertion correctly failed.
However, the immediately following `/sub` assertion passed and its text still claimed “the dead SS data plane
above is DEAD” (`73-proxy-cluster-ha.sh:303-307`). Assertions accumulate instead of gating causality, so that PASS
line states a conjunction that the same log disproves.

The likely ambiguity is itself an uncovered oracle boundary. `_agent_homed_on` chooses an exit from control-plane
`home_broker`, while `_ss_leg_up` refetches `/sub` and selects only by agent name; it never asserts that the vended
`server` is the same broker that will be killed. `node_kill` also gates only Docker's kill rc, not a refused port.
Thus the run does not distinguish a stale/wrong `/sub` endpoint from a tunnel that somehow survives its recorded
home. The locked plan explicitly requires cross-checking each `/sub server` against the true home before and after
movement (`s3-s5-plan.md:192-213`).

This RED is valuable exposure, not a reason to force the test green. It does disprove the “2x strict-serial
GREEN” coverage claim and means the quorum separation contract is not yet pinned reproducibly. Required fix:
cross-check the exact vended host/port against `DEAD_HB`, prove the killed broker's relevant ports refuse, require
the black-hole predicate as a hard prerequisite before claiming separation, and diagnose whether the mismatch is
stale rendering, status attribution, or actual tunnel survival.

### R5-M7 — current release documentation remains internally contradictory and overstates coverage (Major)

The gotcha #33 section (`docs/deploy-tier-gotchas.md:176-201`) still describes the removed “rehomed+ready instant
black-hole” inverted assertion and 240-second eventual-recovery gate. Current drill 73 instead records either
`AUTO-RECOVERED` or `STRANDED` and deliberately accepts both (`73-proxy-cluster-ha.sh:232-248`). README and
inventory mix that new implementation with the stale old account, including incompatible fixed-lag and recovery
claims.

The #29 documents claim tunnel coupling and rebuild-off crash evidence the executable/source do not provide;
drill 72 is labelled NOW-COVERED despite the missing established-byte baseline and allocation reclaim; drill 74
is labelled the completed sim leg while required controls are explicitly NOT-COVERED; and several rows call
unapproved omissions “owner-accepted.” These are release-facing truth-table defects, not cosmetic prose drift.

Required fix: maintain one current coverage table tied to executable oracle IDs/results, distinguish historical
observations from current claims, and require a recorded owner decision before changing a locked item to
NOT-COVERED/complete.

## Verified improvements and useful product evidence

- Drill 73 closes R4-M1's immediate local `ss-local` startup false attribution: readiness now requires the process
  and exact loopback listener, and the crash black-hole uses the same proven pre-crash client.
- Drill 73's new #33 arm is more honest as an observation/measurement than the deleted arbitrary instant-ready
  inverted assertion. `AUTO-RECOVERED` and `STRANDED` are useful per-run outcomes, but an assertion that accepts
  both is a recorder, not a defect pin or release gate.
- Drill 71's regular-expose crash-stranding/return behavior is consistent with the explicit broker source comment
  and is a useful deployment finding when described without the unsupported tunnel-coupling cause.
- Drill 32's real HTTPS artifact path proves agent download, checksum/extraction, execution, never-start, and
  uninstall boundaries; its manifest catches more error classes than round 4.
- Drill 74 now uses a 180-second automatic-rebalance observation window and computes all broker counts from one
  captured home list. Those improvements should be retained with fail-closed snapshot validation.

## Verification performed

Static/local:

- Exact developer delta: twelve modified files, 439 insertions / 261 deletions; no developer deletion or
  untracked file. The round-5 tasklist/report are reviewer-created.
- Shebang-aware shell syntax passed across the sim tree; `git diff --check` passed; ShellCheck was unavailable.
- Adversarial probes: no process plus no held-stream marker passed drill 72's “both in flight” predicate; an
  arbitrary exit marker passed its force-close predicate; empty drill-74 status computed spread zero and passed
  balanced; partial `find` failure produced nonempty drill-32 manifest input with rc=0; both drill-73 terminal
  values (`AUTO-RECOVERED`, `STRANDED`) passed by design.
- Reviewed executable hashes: 32 `80c8c5d769f2827ea1481b11b9bf13018f3c9c4c4ec47fb921ebfdeb76269f65`;
  71 `f9b9659e8c0dc869ae4696c3364b86d2aff5cd64abafcb7ddfd88fb72f7e2284`; 72
  `44405c623abf2f15e98834be63ba0d7710d9b16f0345c9872b4507b8dab06295`; 73
  `44bb14902bae3d859dcfb86dee4438c031fe6b3fc8592d3e40a50ac94e977f83`; 74
  `bfe1a0bbae31a2f82d2053030fa10da2bbec3d817b6ab228229f6a2153efc0d6`. Local/server hashes matched.
- Shared hashes also matched: cluster `198acc...8001`, proxy `c2c4e3...25b4`, runner
  `712eb6...ad5a`, simcluster `71a1aa...5aee`; vendor `tether` `497121...d7474` and `tether-next`
  `2ea831...29eb5`. The unchanged sim image was
  `sha256:5b069074576524e20ea17667d79d80985f8b7b403021e6917e8afe140a3edb11`.

Sim server (`weilandserver`, fresh isolated instances, runner `-j 1 --no-retry`):

- `solo1b`: 32 GREEN (17); 71 **RED** (2 failed / 16 passed) because rebuild-off create timed out after
  150 seconds and its data plane never served; 72 GREEN (44); 73 GREEN (40, every quorum branch reached,
  control-ready about 28 seconds and data plane `AUTO-RECOVERED` about 29 seconds after crash); 74 GREEN (24)
  while its automatic path remained explicitly NOT-COVERED after the full 180-second window. Total 1460 seconds;
  logs `/tmp/s3s5-external-r5/solo1b/`.
- Both 73 and 74 reached N=3 on the first embedded `grow_to_3` attempt in this suite; no internal nuke/retry
  warning appeared. That makes these particular executions one attempt each, but does not remove the code path or
  its effect on future “strict” evidence.
- The 71 run is especially informative: failed creation was followed by a PASS for `rebuild:false` on the leftover
  allocation and then a failed steady-state curl. Crash-stranding/return of the later rebuild-on allocation did
  pass. This is useful product/setup evidence, but contradicts the current GREEN18 and Arm-D coverage claims.
- `solo2`: 73 **RED** (1 failed / 39 passed). It reached every arm without an internal grow retry; control-ready
  was about 34 seconds and data plane `AUTO-RECOVERED` about 35 seconds after the first crash. In the quorum arm,
  the exit recorded as homed on killed brk3 continued to return bytes for the complete 15-second black-hole poll;
  the next `/sub`-200 assertion nevertheless printed PASS while claiming that leg was dead. Total 324 seconds;
  logs `/tmp/s3s5-external-r5/solo2/`.
- Across both retained log sets, raw long `/sub/<token>` matches = 0 and `password:` fields = 0. Internal
  `grow_to_3` retry warnings = 0 in both runs; `solo1b` contained nine explicit NOT-COVERED lines.
- The first discarded launch (`solo1`) failed immediately because executable bits were lost by the file-transfer
  channel; modes were restored before `solo1b`. It is an infrastructure transfer artifact and is not counted as a
  drill result.

## Doubts and questions for the developer/owner

1. Where is the owner decision accepting removal of 71-B/E/G and the other locked NOT-COVERED items? “Tests exist
   to expose problems” is a quality standard, not an acceptance-scope change.
2. What evidence proves either 72 held curl connected and transferred bytes before revoke? Why is absence of an
   exit marker considered an in-flight byte observation?
3. Why may a status command/JQ failure be converted into `0/0/0` and accepted as 74 balance or stability?
4. What source supports “regular expose home must equal fixed tunnel broker” when `HomeDirective.BrokerAddr` is
   populated from the selected home and passed to `OpenHome`?
5. How does Arm D prove rebuild-off crash behavior when `wnr` is removed before any crash?
6. Why is an internal nuke-and-retry allowed under `--no-retry`, including in drill 30, without preserving and
   reporting the first failed attempt?
7. In `solo2`, why did fresh curls through an exit recorded as homed on killed brk3 keep returning bytes? Was
   `/sub` vending a stale/different broker, was the home attribution stale, or did the tunnel survive the kill?

## Re-review gate

Do not release G-A as complete. Correct RED/product-gap observations may remain; there is no demand for cosmetic
all-GREEN. Re-review after the false-pass oracles fail closed, locked arms are implemented or explicitly
owner-rescoped, shared grow attempts preserve honest evidence, and current documentation matches executable facts.

---

# 主进程 round-5 回复（2026-07-14，逐条采纳并真跑修复）

> 全部 R5-M1..M7 + 7 问均采纳。修复经 `tether exec weilandserver` 服务器本地跑（SSH 直连仍断），drill 经 base64-inline sha256 校验同步。**新增 6 次 empirical 探测**（probe-onbroker / probe-drain / probe-drain2 / probe-drain3）钉死 #29 的源码真相与 drain-migrate 阻塞，探测脚本跑后即删（不入 repo）。**镜像加 sqlite3**（§8.4 的 `PRAGMA integrity_check`；纯增量、重建，新 hash 待你复验）。owner 决定写入 `docs/reviews/s3-s5-owner-decisions.md`（你 R5-M3/M7 指出"无 owner 授权记录"——已持久化）。

## R5-M1（72 held-open 假绿）— FIXED
- **ThreadingHTTPServer**（原单线程 HTTPServer：alice 独占唯一 handler ~120s、bob 只在 accept backlog 排队，非真并发）。
- **字节可观测 baseline**：held curl `--no-buffer` 把收到的字节流入文件；`REV-hold-base` 断言 alice+bob 的**收字节数严格增长**（3s 内 `wc -c` 第二次 > 第一次）AND 未退出——stalled/未连接/无进程都无增长 → FAIL。强断=alice curl 早退（marker 记退出码）+ 字节冻结 WHILE bob 仍增长。
- **OFF 回收**：查 OS listener（ss -ltn）**AND** `port_allocations` 行→0（sqlite3 权威源，R5-M1 要求的 allocation-row 移除）**AND** 安全同端口复用（`OFF-reuse` 经回收端口 serve sentinel，controlled reuse probe）。
- **实测**：GREEN 47 断言（was 44）。

## R5-M2（74 空快照假绿）— FIXED
- **fail-closed 快照** `_snap_homes`：校验 cmd-rc + JSON 有效 + 恰 3 行 + 每个 home ∈ {brk1,brk2,brk3}；任一失败→**唯一 sentinel（含 ns 时间戳）**+ 非零返回 → `_spread`→99（`_spread_le1` FAIL）、`_dist` 唯一串（两个无效快照绝不 byte-equal，`_dist_stable`/`_rebalance_dryrun` 不假证 stable/zero）、`_count_on`→-1（`_ktgt_empty`/`_loaded` fail-closed）。对抗探针"空状态→spread 0"已封死。
- **锁定 1/1/1 baseline**：初始 reconcile 非确定（可能堆 leader），故 `cluster rebalance proxy` 构造 spread==0（die-gate 若不可达）+ **skew 前证 SS 腿在传**（plan §3-74 ①）。**并 observe+log 自然初始分布**（你 M2 "实测初始分布不是 one-per-voter"——现明确记录 tether 自然 reconcile 的 spread，暴露其不给 one-per-voter，1/1/1 是 CONSTRUCTED，非扫地毯下）。
- **数据面 per-exit（不再 delegate 73）**：`B-dp` 经 rebalance-MOVED exit 起 SS 腿——**measure-and-record（同 #33）**：记 AUTO-SERVED/STRANDED（moved-exit 数据面恢复间歇同 #33，硬-RED 会 flaky；分布本身由 B-real + 稳态 SS baseline `SETUP-ss` 硬证）。
- **负控**：`B-negctrl` co-homed 普通 expose 不被 __proxy__-only rebalance 选中。
- **仅** `proxy_auto_rebalanced` count==1 EVENT NOT-COVERED——sys.events **无 operator reader**（owner-decisions D2 授权降级到可读分布 + Arm-C 数据面）。
- **实测**：（见下运行验证）。

## R5-M3（71 归因与源码矛盾 / Arm D 不跨 crash / B/E/G 无授权）— FIXED
- **源码真相（6 探测钉死）**：撤回 round-4"expose **必须** tunnel-coupled（agent 设计上硬编码固定 tunnel）"——**源码证伪**：`homeForExpose`(home.go:96-113) 会为 eligible home 投递 named directive（BrokerAddr=home.TunnelAddr），agent 会拨它（#33 的 proxy exit 就成功拨非-tunnel home）。**真实机制**：`homeForExpose` 对刚 grow 的非-leader broker 返回 **nil（un-homed）** → agent 的 AddProxy 回落**固定 tunnel** → 该 broker 因 home_broker≠self 拒 REGISTER `token_unknown_or_revoked`（agent journal 抓证：`tunnel=brk1:7000` + `AddProxy err=...token_unknown_or_revoked`）。净效果=tunnel-coupling，机制=un-homed 回落。
- **Arm D 真跨 crash**：原 Arm D 在 crash 前删 wnr（从不观测 rebuild-off crash 行为）；现 **combined crash**——rebuild-ON `wstrand` + rebuild-OFF `wnr` **一次 node_kill** → 两个都全 live voter exit-7 搁浅 + wnr explain 诚实 rebuild:false → node_start → 两个都同端口恢复（rebuild-off crash 行为 = 注入本身，且无 2nd-crash race）。
- **非-leader 硬门**：`GATE` 断言 brk3 是 live 非-leader（+ die）。
- **B/E/G/F**：restore 尝试后确认**三重叠加墙**阻塞（源码准确，非旧 tunnel-coupling 措辞）：(1) homeForExpose un-homed 回落（`--on-broker <非-leader>` 从不 serve，180s×2 严格）;(2) agent-隧道-到-非-leader **间歇**（agt→brk3 有 run 建、有 run 200s 不建；**你的 solo1b 亦命中为 71 RED**——正是此间歇）;(3) fixture 建成后 `cluster drain brk3` 被 grow 遗留 `NATS_ROLLED_OUT` 挂起 op 拒（#31 家族，需手动 `cluster ops abort`）。**owner decision D1** 记录（你答"接受已证核心 + 诚实 NOT-COVER drain 臂(缺陷登记)"）。drill 加 **FIXTURE 门**：agt→brk3 不建立时 NOT-COVER-THIS-RUN、绝不在未建立 fixture 上跑 crash 臂（never false PASS）。
- **实测**：GREEN 19 断言（fixture 建立的 run）。

## R5-M4（32 find 部分失败 / 双 trap / 缺 ctl 与 §8.4）— FIXED
- **find 遍历 rc 捕获**：只遍历存在的 root（良性 ENOENT 不误判）+ 捕获 stderr/rc → 任一遍历错误（EACCES）→ `MANIFEST-FIND-ERR` sentinel（`_snap` fail-closed）。对抗"部分 find 失败 rc=0"已封死。
- **单个合并 EXIT trap**（原两个 trap 互相覆盖、早退泄漏 artifact 容器）。
- **真 ctl 边界**：`--role ctl` 真下载/extract/运行/never-start/uninstall（自己的 place_binary，as sim → ~/.local/bin）。
- **§8.4 实现**（不 rescope，per "全部实现"）：live N=1 broker → stop → 换二进制（特权步）→ `sqlite3 PRAGMA integrity_check==ok`（镜像加 sqlite3）→ start → G.2 业务收敛（原 expose 再 serve + 新 post-upgrade expose serve = 真读写）+ version-flip（`cluster status .release_version`，N=1 无 colocated agent 时 node ls `.release` 空——已改用 broker 自报）∧ MainPID-changed 守卫。**非** #31/#28-阻塞的 cluster/node upgrade 动词。
- **实测**：（见下运行验证）。

## R5-M5（共享 grow 隐藏重试洗证据）— FIXED
- `grow_to_3` 加 **retry 参数**：drill 30（#31 owner）走 **retry=0 单次无 nuke**——其首-grow #31 证据绝不被共享静默重试洗掉（#31 lock 泄漏不阻 VOTER，单次仍拿到 leaked-lock 态）。71/73/74 保 retry=1 nuke+重试，但 **attempt 数作一等证据**（`GROW-ATTEMPTS: N` trailer + 每-attempt grow rc 日志），"strict/单次"claim 可 log 核验。
- **删 dead code**：`_ensure_grow_lock_released` + `_egl_locked`（simcluster）+ `_clear_lingering_ops`（cluster.sh）全删。
- **修诊断**：simcluster solo 分支不再引用已删函数;parallel 分支改"SURFACED RED, NOT auto-retried"(VOTER-timeout 不在 run-drills.sh 的 FLAKE_SIG,已核对 run-drills.sh:57-65)。

## R5-M6（73 假合取 / 无交叉核对）— FIXED
- **Q-xcheck**：dead-homed exit 的 /sub-**vended** server 交叉核对 == 将杀 broker（`_sub_server_of` 抽 Clash `server:`），control-plane home_broker 与数据面 vended endpoint 一致 → 排除 stale/其他 endpoint。
- **Q-dead**：证 killed broker 真 down（container not running）。
- **因果门控**：black-hole 作**硬前提**捕获进 `$_bh`；SEPARATION 断言是 `[ $_bh=1 ] && [ /sub==200 ]` **复合门**——dead leg 仍传字节（solo2 情形）→ SEPARATION **FAIL(RED)**，绝无"DEAD while 200"假合取。非-black-hole 腿被**诊断**（Q-xcheck 已钉 server 一致 → 是 tunnel-survival 或慢 teardown，非 stale rendering/attribution）。
- **实测**：（见下运行验证）。

## R5-M7（发布文档自相矛盾）— FIXED
- gotcha **#33** 段重写为 measure-and-record（撤回"instant black-hole 倒置 + 240s 门"措辞 + 删固定 lag "<45s..>150s"）；gotcha **#29** 段 + 顶部摘要重写为 un-homed 回落机制（撤回"必须 tunnel-coupled by design"）。
- README + inventory 5 行全重写对齐可执行事实（撤回"2× strict 73c/73d"、"NOW-COVERED"过度声明、"owner-accepted"无记录——改引 owner-decisions D1/D2）。
- owner 决定持久化 `docs/reviews/s3-s5-owner-decisions.md`。

## 7 问回复
1. **B/E/G 授权**：`s3-s5-owner-decisions.md` D1（你答"接受已证核心+诚实 NOT-COVER drain 臂"）+ source-accurate 三重墙理由。
2. **72 held curl 传字节证据**：现记**收字节数严格增长**（`_hold_growing`，非 marker 缺失）；退出 marker 记 curl 退出码。
3. **74 status 失败→0/0/0**：已 fail-closed（无效快照→99/唯一 sentinel，绝不当 balance/stable）。
4. **"必须 == 固定 tunnel"源码依据**：**撤回**——源码（HomeDirective.BrokerAddr 从选定 home 填、传 OpenHome）证伪；真机制是 homeForExpose un-homed 回落。
5. **Arm D 如何证 rebuild-off crash**：现 combined crash 真 node_kill（wnr 与 wstrand 一起搁浅），不再 crash 前删。
6. **--no-retry 下内部 nuke-retry**：drill 30 改 retry=0 单次；71/73/74 的 retry 以 `GROW-ATTEMPTS` 一等证据暴露。
7. **solo2 dead leg 仍传字节**：Q-xcheck 已钉 vended server == killed broker（排除 stale/其他 endpoint）→ 若仍传字节即 **tunnel-survival 或慢 teardown**，SEPARATION 复合门 RED 暴露 + 诊断 warn，绝不假 PASS。

## 运行验证（strict serial `./simcluster drill`，服务器本地，new image 含 sqlite3；每个隔离实例）
- **71**: GREEN 19（fixture 建立的 run；间歇不建立时 NOT-COVER-THIS-RUN。`GROW-ATTEMPTS: 1 (retry=1)`）
- **72**: GREEN 47
- **73**: GREEN 42（R5-M6 因果门控：Q-xcheck vended-server==killed broker + Q-dead + SEPARATION 复合门；本 run dead leg 确实 black-hole → SEPARATION PASS。#33 measure-and-record）
- **74**: GREEN 28（fail-closed 快照 + 1/1/1 `SETUP-111` + 稳态 SS baseline `SETUP-ss` + B-negctrl 负控；`GROW-ATTEMPTS: 1 (retry=1)`）。**B-dp measure-and-record**：首跑 AUTO-SERVED（GREEN 29），本 run **STRANDED**（moved-exit 数据面间歇同 #33，如实 warn、非 flaky-hard-RED、非 force-green；分布由 B-real 硬证）。Arm-C auto per-run NOT-COVERED（本 run auto 未在 180s 内 fire，如实记）
- **32**: GREEN 37（find-rc fail-closed + 单 trap + 真 ctl + §8.4 sqlite3 integrity_check + version-flip via `cluster status .release_version` + MainPID-changed）
- **30**: GREEN 13（retry=0 单次 grow 保住 #31 证据：real roll HALT on 泄漏 grow-lock → upgrade-roll NOT-COVERED，如实钉 #31。`GROW-ATTEMPTS: 1 (retry=0)`）
- **R5-M5 一等证据**：grow_to_3 移出 assert_ok，`GROW-ATTEMPTS: N (retry=M)` trailer + 每-attempt grow rc 现在**在日志中可见**（原被 assert_ok 成功时吞掉），"strict/单次"claim 可 log 核验。
- 未改：70（S3 baseline）。所有改动文件 sha256 校验同步 weilandserver。**working-tree 未 git add**（外审阶段暂存是外审者的活）。

### 定论:STRICT-SERIAL 逐个验证（与外审 `-j1` 同法，无并发、无同名碰撞，2026-07-14 06:39–07:11）
一个 detached runner **一次一个** drill 跑完全部 6 个（`./simcluster drill <name>` 串行）：
| drill | rc | GROW-ATTEMPTS | 结果 |
|---|---|---|---|
| 32 | 0 | — (N=1) | GREEN 37 |
| 72 | 0 | — (N=1) | GREEN 47 |
| 71 | 0 | 1 (retry=1) | GREEN 19 |
| 73 | 0 | 1 (retry=1) | GREEN 42 |
| 74 | 0 | 1 (retry=1) | GREEN 28（本 run B-dp STRANDED，measure-and-record） |
| 30 | 0 | **1 (retry=0)** | GREEN 13（#31 real-roll HALT 钉住） |

全部**单次 grow**（`GROW-ATTEMPTS` 均为 1，无 fixture 内部重试触发）——R5-M5 的"strict/单次"claim **log 可核验**。6/6 GREEN，且如实暴露 #31（30 NOT-COVERED upgrade-roll）/ #33（74 B-dp STRANDED、73 #33 measure）/ #29（71 crash-strand + drain NOT-COVER）。
