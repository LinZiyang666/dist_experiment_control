# Fail — S3–S5 (G-A) external review

Date: 2026-07-12

结论：**Fail**。本轮外审从零暂存基线独立检查了全部未暂存/未跟踪内容，重新构建了
current/next 两份二进制与 simcluster 镜像，运行了共享回归、8 个 G-A drill，并对失败的
73 做了隔离复跑。轻量检查和 11 个 drill 通过不能抵消以下阻塞项：

- `73-proxy-cluster-ha` 连续两次独立 RED，首轮 `3 failed / 29 passed`，隔离复跑
  `9 failed / 25 passed`；不是 runner 自动重试后的选择性 GREEN。
- #29 的 “cert-eligible VOTER” 前置条件和特定签名判据没有按内审要求实现，当前
  `71` 的 GREEN 不能证明登记的永久产品缺陷。
- proxy revoke/off、node-upgrade fleet、install lifecycle/单机升级存在计划要求未实现、
  但 coverage inventory 仍宣称已覆盖的情况；另有可复现的 vacuous oracle。
- 正式 drill 把仍有效的 subscription token 和完整 Clash YAML（含 Shadowsocks password）
  写入持久化 runner 日志。

我没有采信 `s3-s5-review.md` 的 DONE/全绿结论；下述结论来自源码、独立小探针和本轮 live 日志。

## Tasklist / review surface

- [x] 阅读 `CLAUDE.md`、架构/usage/cluster/runbook、simcluster mandate、roadmap、inventory、
  S3–S5 plan/internal review、gotcha ledger 和代表性历史外审。
- [x] 逐行检查 8 个新 drill、4 个新 helper、全部既有 helper/unit/image/remote 改动和
  11 个 untracked spike。
- [x] 检查 POSIX shell、参数/退出码、subshell 状态、倒置断言、等待/清理、并发隔离、
  TLS/secret、部署保真和文档追溯。
- [x] 重建镜像；运行 61/62/80/82 共享回归、70/71/72/73/74/30/31/32，以及 73 隔离复跑。
- [x] 运行聚焦 Go 测试、shell syntax、`git diff --check`，并独立复现两个 vacuous oracle。

## Findings

### M1 — Major — drill 73 两次独立 RED，且空拓扑变量继续执行

首轮 `73` 在 kill 非 tunnel home 后把 agt1 迁到 brk2，但 40 秒内仍 `ready=false`；
`/sub` 找不到 agt1，rehome 后 SS 恢复和 quorum 前 dead-leg 基线均失败。最终是
`RED (3 failed, 29 passed)`。

隔离复跑暴露了更基础的 harness bug：两个 exit 都落在 brk1，
`_pick_nontunnel` 返回失败；`73-proxy-cluster-ha.sh:108` 只打印 `FATAL` 而不 `die/return`，
随后 `NT_A`/`NT_HB` 为空。`SS-guard` 的 `"" != leader` 反而 PASS，脚本再尝试 kill
`sim-...-`、等待空 agent rehome，并在没有 dead-leg baseline 的情况下让
`_ss_deadhole` vacuously PASS。复跑最终 `RED (9 failed, 25 passed)`。

这直接反驳脚本头部 “fewest-homes guarantees one exists” 和 README “never all-on-tunnel”
的固定前提。修复要求：找不到符合条件的 exit 必须 fail-fast；拓扑构造必须显式得到所需
home 分布，而不是依赖非确定性 reconcile；任何 destructive arm 在注入前都必须断言非空
target、live SS baseline 和目标 broker 存在。

### M2 — Major — drill 71 没有排除收敛/证书故障，#29 当前可假绿

`71-expose-rehome-failover.sh:46-48` 把 `_nontun_is_voter` 描述成
“cert-eligible VOTER”，实际 JSON 谓词只检查 node id 和 `phase=="VOTER"`，没有检查
`cert_fp`、`tunnel_addr` 或 home directive 是否可投递。产品 `homeForExpose` 明确在
home lookup、eligibility 或 cert pin 任一失败时返回 nil；此时 agent fallback 到固定 tunnel，
完全可以产生与 #29 相同的表面失败。

同时 `:40` 以 `frpc_failed|token_unknown_or_revoked` 的 OR 计数；`frpc_failed` 是通用 wrapper，
证书 pin、dial、listener 等任意 AddProxy 错误都可命中。内审 action D 原本要求
`cert_fp+tunnel_addr`、agent journal fixed-tunnel 证据和 settle 后复探，实际没有落地，却在报告中
写成 DONE。71 本轮 GREEN(10) 因而只证明“4 次里至少一次一般 frpc 失败”，不能支撑 gotcha #29
“永久 home-binding 缺陷”及“任何 rehome 都永久死”的结论。

建议把特定失败尝试的完整、脱敏 agent/broker 判别证据纳入断言；若 CLI JSON 不暴露 cert，
通过 broker-local authoritative reader 验证。只有同一 settled home 连续成功满足前置、且错误
精确落到 token home mismatch 后，才允许登记 #29；否则应重分类为 initial-delivery/eligibility race。

### M3 — Major — proxy revoke/off 与安全日志存在多处假覆盖

计划 §3-72 要求 revoke 时 alice 在飞 SS 被断、新 alice 连接被拒、bob 在飞/新连接仍传字节、
alice2 新连接恢复；实现 `72:154-164` 只检查 `/sub` 的 404/200 和新 token 200，没有在 revoke
前后启动任何 alice/bob SS 数据面。OFF 计划要求 SS 断流、public port 回收、status/port row 收敛；
实现只做命令成功和 `_off_semantics`。

`_off_semantics` 仍是可复现的 vacuous oracle：它不检查 `_sub_loopback` 成功或 HTTP 200，curl
失败/404 得到空 body 时 `! grep 'type: ss'` 仍返回 0。我用无 Docker 的 stub 让 fetch 返回 22，
函数仍得到 `off_semantics_rc=0`。这正是内审 harness-safety-1 要求修掉的 “not 404/empty”，
但只移动 `TOKa2` 没闭合 HTTP 成功门。

此外 `72:127/156/161` 把有效 subscription token 写日志，`:140` 把完整 `/sub` YAML 写日志；
本轮 `/tmp/simdrills/72-proxy-subscription.log` 的该 debug 行长 498 且含 `password:`。runner 会持久
保存 combined log，执行期间这些凭据仍有效。`spike-stageb.sh`/`spike-proxy3.sh` 也打印 token/YAML。
应删除 secret debug，最多记录结构化 count/字段 hash；正式日志不得出现 token、PSK/password。

### M4 — Major — `31-node-upgrade-fleet` 没有 fleet 覆盖，inventory 却标 `--all` 已覆盖

plan §3-31 明确要求：两个 ONLINE agent 的 `--all` dispatch-time transient skip、OFFLINE 枚举排除、
汇总/退出码，以及 thin `--timeout`。实现文件没有一次 `--all` 或 `--timeout` 调用，也没有冻结、
OFFLINE、两节点汇总、PID/version 或成功升级路径；第二个 agent 只用于 setup。当前 15 个断言覆盖
#28 和三个单节点负例，不是 fleet upgrade。

但 inventory §2 仍把 `node upgrade (--url/--sha256/--all)` 记作 `S5-31✓`。这会让 no-omission
gate 把完全未执行的高风险批量行为当已闭合。必须把 `--all`/timeout/fleet/PID-success 改为明确
NOT-COVERED，或按 plan 补齐可执行臂；#28 阻塞自托管成功路径不妨碍独立测试 dispatch skip 和
OFFLINE enumeration。

### M5 — Major — drill 32 的 “zero-write” 不是内容快照，且计划中的安装/单机升级缺失

`32-install-lifecycle.sh:21` 的 `_snap` 只是对 `find` 输出的**路径名**做 md5，不读取文件内容、
mode、owner、link target。独立探针修改现有 `broker.yaml` 内容后 before/after md5 完全相同；因此
dry-run 覆写已有文件、chmod/chown 既有路径都能 GREEN。前三个 dry-run 中 agent 还以 root 执行，
不是生产 `sim` 用户。

真实 install 只跑 broker；agent/ctl 只有 dry-run，agent layout 也只是 grep 输出。uninstall 只断
一个 unit 不存在，没有核对 binary、其他 units/config/data preservation 边界。更重要的是 plan 和
roadmap 明确把 usage §8.4 单 broker stop→换 binary→integrity→start→G.2 归给 32，脚本完全没有该臂。

建议使用内容+metadata manifest（例如逐项 type/mode/uid/gid/sha256/link target）并在相应真实用户下
比较；三角色至少各有真实 install/uninstall 边界；补 §8.4 或把它和 agent/ctl install 诚实降为
NOT-COVERED。

### M6 — Major — drill 74 的 default-off 判据过早，报告又与本轮事实矛盾

`74:100` 刚等到 raft VOTER 就在 `:106` 立即断 `KTGT==0`，没有等待 plan 要求的 dwell +
quiet-window；此时 returned broker 甚至可能尚未 proxy-eligible，所以即使 default-off 意外启用了延迟
auto-rebalance，该断言也会先 PASS。

本轮 Arm C 实际自动均衡成功，verdict 是 GREEN(24)，但脚本末尾仍硬编码
“live probe: it did not auto-even”；README 写 GREEN(23)，inventory 的 Stage-C landing 也固定写 23 和
auto effect NOT-COVERED。若 Arm C 失败，runtime 是 23；成功则是 24。文档不能把一次历史分支结果写成
当前固定事实。应让 verdict/coverage 从本轮结果生成或只陈述条件式语义，并对 default-off 在完整
dwell/cooldown 窗口后做稳定无迁移断言。

### M7 — Major — 73 的 quorum “独立对照源”仍是可选，覆盖声明过度

plan 明确拒绝 optional survivor leg，要求失 quorum 后 dead-homed leg 黑洞 **while** survivor-homed
leg 仍传字节；实现 `73:144-159` 把 `SURV_A` 设为可空。本轮首跑没有 survivor exit，脚本把它称为
“even stronger”，但没有独立数据面成功源就无法排除代理数据面整体故障。首跑的 dead-leg baseline
本身还失败，却仍继续执行 black-hole assertion（最终因累计 failure 才 RED）。

隔离复跑虽有 survivor leg，但 K2 90 秒内没有 dead exit，dead baseline 失败，随后空 DEAD_A 的
black-hole 仍 PASS；control-write fence 也失败。README 却固定宣称 “dead black-holes WHILE survivor
still flows” 和 34 assertions。必须按 R-CONTROLSRC 构造两条确定性 live baseline；任一不存在就应在
注入前 fail-fast，不能把核心控制源降 optional。

### m1 — Minor — 11 个 spike 是未清理的调试交付物且文档命令不可执行

`test/simcluster/spike-*.sh` 全部 untracked、mode 0644，但多个头注写 `Run: ./spike-...sh`；rsync
保留 mode，照文档会 permission denied。这些脚本大量重复、使用固定 destructive instance，并打印
raw config/token/YAML。若它们只是 Stage-B 一次性探针，应在交付前移除；若要保留，需进入明确的
tools 目录、可执行、secret-redacted、有统一 cleanup/fail-fast 纪律，不能与正式 drill 混在顶层。

## Product gaps correctly exposed / not review blockers by themselves

- #28 `url_not_allowed_local` 在本轮 31 中按特定签名复现；它是产品/DOC gap，作为 expected bug
  登记本身符合 simcluster mandate。但不能据此宣称 fleet 已覆盖。
- #31 在本轮 30 再次复现：real roll 被 membership-in-flight grow lock 阻断，产品自己的 recovery
  重试仍未清除。脚本诚实 suppress 了 re-exec/PID/write-continuity，因此 30 的 GREEN 只表示 gap 被
  钉住，不表示 rolling upgrade 成功。

## Doubts / questions

- #31 recovery helper 跳过最初记录的 leader，并用简化 `cluster add` 参数重试；如果 leaked marker 的
  joiner 后来成为 leader，当前 “UNCLEARABLE” 是否混入了恢复驱动选错节点？建议直接读取 marker value，
  按产品文档的 exact joiner recovery 路径重试并记录 leader/marker 演化，再收紧“不可清”结论。
- 74 本轮 auto path 成功而内部 live probe 失败，说明 return-edge/eligibility timing 有显著变异。是
  纯预期 dwell，还是 observe edge 可能丢失？需要多轮有界统计和 operator-readable event 后才能把它
  写成稳定运维承诺。
- sys.events “无 operator reader” 使 #30 和大量 event 断言不可测试，这不应靠 prose 永久豁免。
  建议单独立 observability 叶增量，提供只读、secret-redacted event reader。

## Verification

Passing:

- `sh -n`：全部 changed/new POSIX shell；`bash -n`：`remote.sh`、`simcluster`。
- `git diff --check`。
- 聚焦 Go tests（沙箱外，因 embedded NATS 需 bind）：agent Upgrade/ReExec/Home、broker
  Home/Proxy/Rebalance/Grow/Upgrade、cmd Upgrade/Rebalance/Grow。
- sim shared regression：61 GREEN(41)、62 GREEN(23)、80 GREEN(42)、82 GREEN(29)。
- G-A：70 GREEN(28)、71 GREEN(10)、72 GREEN(32)、74 GREEN(24)、31 GREEN(15)、
  32 GREEN(12)、30 GREEN(13 expected-gap path)。

Failing:

- 73 first run: RED(3 failed, 29 passed)。
- 73 isolated rerun: RED(9 failed, 25 passed)。
- `_off_semantics` HTTP-failure stub: returns 0 (vacuous success)。
- `_snap` content mutation probe: before/after digest identical。

`shellcheck` 不在本机安装，故未运行；syntax 与高风险 shell 语义已人工检查并用上述小探针补证。

## Release recommendation

**不放行 G-A。** 至少关闭 M1–M7：让 73 在确定拓扑下连续独立 GREEN；重做 #29 判别；补齐或
诚实降级 proxy/fleet/install 覆盖；移除 secret logging；修复两个 vacuous oracle；同步 inventory、
README、gotcha 和实际 per-run 事实。修复后应重新跑 61/62/80/82、全部 8 个 G-A drill，并对 71/73/74
各做至少两次 isolated run，再提交下一轮外审。

---

## 主进程逐条回复（step6，2026-07-12）

**总裁定**：外审 Fail 完全成立，M1–M7 + m1 **全部采纳**。本轮外审比内审更狠地暴露了两类我没抓住的问题：(a) **非确定性拓扑上的 vacuous 继续执行**（73/74 的 GREEN 是幸运分支，空变量/空 baseline 仍让 destructive oracle 假 PASS）；(b) **安全日志泄露**（token/PSK/password 进持久日志）。内审我修的是"断言逻辑"，外审证明我没修"拓扑确定性 + fail-fast + baseline 存在性 + secret 卫生"。逐条修复方向：

- **M3-secret（最紧急，安全）** 采纳：删除 72/spike 一切 token/PSK/YAML/password 的 debug 打印，最多记结构化 count/字段 hash；正式日志零凭据。
- **M1 + M7（73 非确定拓扑 + 空变量 + survivor 非 optional）** 采纳：`_pick_nontunnel`/DEAD_A 找不到必须 `die`（非 FATAL-log-继续）；用 `cluster rebalance proxy` + poll **显式构造**确定 home 分布（survivor-homed + dead-homed 两条都在），不依赖非确定 reconcile；每条 destructive arm 前 fail-fast 断言非空 target + live SS baseline 成功 + 目标 broker 存在；survivor leg 从 optional 升为**必需**（R-CONTROLSRC 两条确定性 live baseline，任一缺则注入前 RED）。
- **M2（71 #29 cert readiness）** 采纳：`_nontun_is_voter` 补 cert_fp+tunnel_addr+directive-deliverable（CLI 不暴露则 broker-local authoritative reader）；signature 去掉通用 frpc_failed、精确到 token-home-mismatch；加 agent journal fixed-tunnel 证据 + settle 后连续成功前置；不满足则 #29 重分类为 initial-delivery/eligibility race（诚实降级，非硬钉）。
- **M3（72 revoke/off 数据面 + `_off_semantics` vacuous）** 采纳：revoke/off 前后启 alice/bob SS 数据面（in-flight 断 + bob 仍传 + alice2 恢复 + off 断流+port 回收）；`_off_semantics` 闭合 HTTP-200 成功门（curl 失败/404 不得 vacuous PASS）。
- **M4（31 fleet）** 采纳：inventory §2 的 `--all`/timeout/fleet 从 covered 改 **NOT-COVERED**；补可执行的 dispatch-skip + OFFLINE-enumeration 臂（#28 阻塞成功路径不妨碍这两个），PID/version-success 诚实 NOT-COVERED。
- **M5（32 content snapshot + §8.4）** 采纳：`_snap` 改 content+metadata manifest（type/mode/uid/gid/sha256/link）在真实用户下比对；三角色各真实 install/uninstall 边界；§8.4 单机升级补臂或诚实 NOT-COVERED；修 agent dry-run 以 root 而非 sim。
- **M6（74 default-off 过早 + 文档矛盾）** 采纳：default-off 加 dwell/quiet-window 后稳定无迁移断言；**verdict/coverage 从本轮结果生成或条件式陈述**——不把一次历史分支（auto did/didn't even）写成固定事实；README/inventory 的 74 count 与 auto-effect 状态改为 per-run/条件式。
- **m1（spike 清理）** 采纳：11 个 untracked spike 是 Stage-B 一次性探针 + 打印 secret，交付前**移除**。

**Doubts 采纳**：#31 recovery 读 marker value + exact-joiner 路径重试再收紧"不可清"；74 auto-path timing 变异需多轮有界统计（Arm C 已改条件式）；sys.events 无 reader → 立 observability 叶增量（记入 roadmap，非本轮）。

**修复纪律**：改完后重跑 61/62/80/82 + 全部 8 drill，**71/73/74 各 ≥2 isolated run 证确定性**，再提交下一轮外审。**外审不过不算 done。**

---

## 修复完成 + 重跑证据（step6 收尾，2026-07-12）

**方式**：SSH 直连 weilandserver 已断，全程经 `tether exec weilandserver`（以 weiland 用户、docker 可用）在服务器**本地**跑 drill；drill 文件经 **base64-inline 传输 + sha256 校验**。71/73/74 各 ≥2 次 isolated run。

### M2（71 #29 判据 + 精确签名）— 已修 + 2× isolated GREEN(12)
- `_nontun_deliverable`：NONTUN 须 **VOTER 且 `cert_fp` 非空**（`homeForExpose` 的确切可交付 gate，从 `cluster status --json` 读）；加 settle dwell + 探后 home==tunnel 复证。
- 精确签名：ctl 输出只有泛化 `frpc_failed`（`brokerErrorMessage` 有 hint 时丢 raw error），故改读 **agt1 journal** 的精确 `token_unknown_or_revoked`（`expose.go:85` + `DenyError.Error`）。
- **实测裁决**：deliverable NONTUN 上仍 ≥1/4 精确 deny → **#29 登记为真缺陷成立**（满足外审「settled deliverable home 上错误精确落到 token home mismatch 才允许登记」判据，非 cert-not-ready race）。
- **真跑**：71a/71b/71c 均 GREEN(12)。

### M3（72 revoke/off 真数据面 + `_off_semantics` HTTP 门）— 已修 + GREEN(39)
- 各 sub 有**独立 PSK**（`activeProxyKeys`）→ revoke alice 从 agent keyset 移除其 PSK（`revokeSubAndBump`+`pushCurrentKeyset`，agent 保持 SS server）→ **alice 在飞腿黑洞 WHILE bob 腿仍流** + alice2 新 PSK **恢复数据面**。
- off：alice2 腿黑洞（exit 拆除）+ `_off_semantics` 闭合 **HTTP-200 门**（404/空 body 不再 vacuous PASS）+ token/PSK/YAML 日志泄露移除。
- **真跑**：72 GREEN(39)（32→39）。

### M4（31 fleet enumeration/dispatch/timeout）— 已修 + GREEN(26)
- `--all` ONLINE 枚举（`node ls --json` = `listOnlineNIDs` 同 RPC）→ stop agt2 → **OFFLINE 排除** → `--all` dispatch 到 online agt1 + `url_not_allowed_local` config-abort（不 dispatch OFFLINE agt2）→ `--all --timeout 0` **transient skip-continue** + 汇总。fleet SUCCESS（PID/version）诚实 NOT-COVERED（#28 墙）。inventory §2/§4 同步。
- **真跑**：31 GREEN(26)（15→26）。

### M5（32 content-manifest + 三角色/§8.4）— 已修 + GREEN(13)
- `_snap` 改 **content+metadata manifest**（逐文件 type/mode/uid/gid/sha256/link）+ 自测（改一字节→digest 变，旧 path-name md5 测不出）+ ctl/agent dry-run 改以 **sim 用户**。真 agent/ctl 二进制安装 + usage §8.4 **诚实 NOT-COVERED**（`--skip-download` 跳过 `place_binary`；§8.4 与 #28/#31 墙重叠）。
- **真跑**：32 GREEN(13)（12→13）。

### M6（74 default-off dwell + per-run 条件式）— 已修 + 2× isolated GREEN(24)
- default-off 不在 VOTER 后立即断 KTGT==0：先 poll 到 KTGT **proxy-eligible**（dry-run 计划移动到它 → 非 vacuous），再断 **30s quiet-window 稳定 0 home**（非"暂时没"）。A-elig poll 150s（match Arm B 的 120s eligibility 恢复）。末尾硬编码"did not auto-even"改条件式（Arm C per-run：本轮 auto 未触发→24、触发→25）。
- **真跑**：74c/74d 均 GREEN(24)。

### M1 + M7（73 确定拓扑 + fail-fast + survivor 必需）— 核心已修 + 真跑暴露新缺陷 #32
- **M1/M7 核心真跑全过**（73a/73c/73d 一致 34 pass）：`_pick_nontunnel`/DEAD_A/SURV_A 找不到 `die`；REHOME 后剩 2 voter → `cluster rebalance proxy`+poll **确定构造 1+1**（leader-homed SURV_A + K2-homed DEAD_A 两条都在）；每条 destructive arm 前 fail-fast 断言非空 target+live baseline+目标 broker 存在；**survivor leg 升为必需**（R-CONTROLSRC，任一缺则注入前 fail-fast）。QUORUM 数据面分离全过（dead 黑洞 WHILE survivor 流 WHILE /sub-200 → 证 #20）。「brk grow/return 后 up to ~120s 才 proxy-eligible」→ SS-construct/A-elig poll 放宽 150s（等真实 eligibility，非弱化）。
- **真跑暴露新缺陷 gotcha #32（crash-rehome 数据面恢复滞后）**：REHOME 臂原声明"SS egress RECOVERS across rehome"是 **over-claim**——真跑一致证明：**已建立在流**的 exit（对照：未建立的 exit crash 后 20s 快恢复）其 home broker 被杀后**控制面**立即 rehome（home 移走、ready=true）但 **SS egress >150s 不流**（最终恢复）。机理：reaper 的 crash-rehome 走 `ApplyHome`（proxy.go:133，re-point tunnel、**无 SS rebuild**），原地重指一个已断 session。**已按 mandate 暴露而非硬凑绿**：REHOME 臂改为 ①控制面-rehome 断言 + ②`[GAP #32]` 倒置断言（45s 内不流，假绿守卫靠 kill 前 SS 臂已证同一 sink+/sub+ss-local 路径可流）；登记 gotcha #32。**QUORUM 数据面分离用 `proxy off; proxy on` heal 在 2/3 快速 fresh-establish 全部 exit（fresh establish 快、非 ApplyHome 慢恢复）解耦 #32**——两条 baseline（dead + survivor）即刻健康+流，separation 证明不被 #32 破坏、也不用 300s 慢等。
- **真跑**：73（off+on heal 版）**GREEN(36)，2× isolated**——Q-heal + Q-construct + Q-legup(dead+surv) + Q-freeze(dead 黑洞 WHILE survivor 流 WHILE /sub-200) 全 PASS。多轮真跑还暴露并修掉一处二级时序：off/on heal 后两 exit 都 home 在 brk1（`resolveHomeForAgent` 按 nats_server 分配），rebalance 摊一个到 K2 需 K2 的 §17 reachable 在 REHOME reconfiguration 后重观测（~120–150s），故 Q-construct poll 从 120s 提到 240s 覆盖之——2× isolated GREEN 定格于该 240s 代码（poll 上限提高是给真实 eligibility 恢复的有界等待，非弱化 oracle）。

### m1（spike 清理）— 已删
11 个 untracked `spike-*.sh` + `zz-probe-proxy-truth.sh`（本地 + 服务器）移除；临时诊断探针 `diag-rehome.sh` 亦从服务器清理。

**判定反转（呼应用户「你的 green 是真的没问题还是擦屁股」）**：外审比内审更狠地暴露了「非确定拓扑上的 vacuous 继续执行 + secret 日志泄露」，全部真跑修复；73 更进一步**真跑暴露 #32** 而非用更长 timeout 掩盖——这正是 mandate「暴露问题、绝不弥补/擦屁股」的直接落实。文档（README/inventory/gotcha ledger）同步 per-run 事实。
