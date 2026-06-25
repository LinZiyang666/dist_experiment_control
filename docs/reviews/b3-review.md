# B3 review（Stage C 内审 + 主进程采纳）

> Stage C：6×Opus 对抗审查（5 视角 + 1 综合）。**Verdict：B3 SOUND，无 BLOCKER**——无私钥泄漏、无 proto/ACL 改、无硬门削弱、无向后兼容/ D9-cutover 破坏。1 个真 MAJOR 代码/UX 缺陷 + 4 个 MAJOR 测试缺口（plan §50–56 承诺但未写）+ MINOR/NIT。主进程逐条采纳；改完 lint 0 / make test 全套绿。

## 采纳（已修）— MAJOR
- **B3-M1 [MAJOR, 真缺陷]**：`ErrRemoveOwnsResources` 把操作员导向死路 `drain --retire`——但 `DrainNode` 只接受 live VOTER（phaseVoter），对 VOTER_ADD_FAILED 无效（会在 phase->DRAINING 失败）。**修**：错误改为"Free/re-home those exposes first … or pass --force"，并明说"`drain --retire` does NOT apply"。
- **B3-M2 [MAJOR, test]**：`RemoveNode(force)` 端到端未测。**修**：加 `TestB3RemoveForceRespectsPhaseGate`（internal/broker）——证**最关键安全属性**：live VOTER + force=true 仍撞 phase-gate、绝不撞 ownership 探测（--force 不削弱硬门）+ unknown-node。N>0/force-bypass 的完整 manufacture（需 ALLOCATED port 行）受 d7SingleNode 无 dbPath 限制，记 gated-d7 refinement。
- **B3-M3 [MAJOR, test]**：init NEXT 替换 + conf 交叉校验未端到端测（旧测试传不存在 conf → 交叉校验从不执行）。**修**：`TestReadClusterPublicIdentitiesCrossCheck`——真 conf 的 match / issuer-mismatch / broker-mismatch（证唯一"印错命令"路径 fail-closed）。
- **B3-M4 [MAJOR, test]**：整机 `Doctor()` 未测（只 leaf）。**修**：`TestDoctorDBReadOnlyNoMutation`（hash db+-wal+-shm 前后不变 = 零 mutation 中心证明）+ `TestDoctorNoEarlyExitOnFatal`（FATAL 后仍评 db 检查）。
- **B3-M5 [MAJOR, test]**：sign-join 向后兼容 + Example/wording 未测。**修**：`cluster_signjoin_test.go`（裸 token 默认 / 完整行 + cert WARN + placeholder / 缺 seed 先报错 / **全程不泄漏 seed**）+ `cluster_help_test.go`（14 命令有 Example + 4 组 + force-single 劈脑裂/takeover 安全网 Short + add Example 含 node-pub+sign-join + init alias）。

## 采纳（已修）— MINOR / NIT
- **B3-m1 [MINOR]**：`init --check` 在全字段门之后、却只用 doctor 相关字段。**修**：`--check` 分支前移到 `missingClusterInitFields` 之前（dry-run 不再索要 --name/--node-ident-pub/--tunnel-addr/--public-host）。
- **B3-m2 [MINOR]**：broker-nkey 交叉校验在多用户 conf 上是 no-op、"verified" over-claim。**修**：`cluster_secrets.go` 加注释说明、init NEXT 行软化为"broker-nkey verified too on a single-user conf; otherwise read from broker.nk"。
- **B3-m3 [MINOR, doc]**：§5.6 把 `drain --retire` 折进 `--yes` 拒绝器列表，但 drain 无 --yes flag（报错是 cobra unknown flag）。**修**：§5.6 限定到 4 个恒 Tier-2 命令 + 说明 drain。加 `TestDrainRetireHasNoYesRejector`（→ unknown flag、非 rejector 串）。
- **B3-m4 [MINOR, wording]**：add-success 指 `cluster status` 取 route/bus-nkey，但报告无这俩字段。**修**：软化为"node list + leader；route/bus nkey 从记录 / 各 broker nats.conf 取"。
- **B3-m5 [MINOR, test]**：`adminsock.Request.Force` 无往返/omitempty 测试。**修**：`TestRequestForceOmitemptyRoundTrip`。
- **B3-n2 [NIT]**：`--yes` 拒绝器在 `--self-id` 缺时印 `("")`。**修**：want 空时省 `(%q)`。
- **B3-n3 [NIT]**：sign-join 带 --raft-addr 但缺 tunnel/route/host 时 placeholder 无 WARN。**修**：缺任一 addr flag 时 stderr WARN（镜像 cert-fp WARN）。
- **B3-n4 [NIT]**：未跟踪的 `tether` ELF 二进制会被 commit。**修**：删除。
- **B3-n1 [NIT]**：生产 TTY 拒绝靠 `in==os.Stdin` 不变量。**修**：加 `TestConfirmTypedNodeIDNonTTYRefuses`（go test 下 stdin 非 TTY → 拒绝）锁回归。

## 残留（记 refinement，非 ship-blocker）
- RemoveNode(force) N>0 ownership-bypass 的完整端到端 manufacture（需 ALLOCATED port 行注入）→ gated d7（d7SingleNode 当前不暴露 dbPath；最关键安全属性[force 不绕 phase-gate]已在 internal/broker 测）。
- doctor 整机 all-clean 全 PASS + FDE 可见行（需伪造全 §15 secrets + 真 cert PEM）；FDE advisory 已被 SecretsPreflight 单测覆盖、portBindCheck 已覆盖。
- B3-n5：doctor bind-check 对 advertised-但非本地可绑 addr 误 FATAL（操作员传本地 IP，实际无碍）。

## 驳回（综合判定为 false alarm）
- Review 5 的两个 "BLOCKER" → 实为 M2/M3 测试缺口、代码正确（综合独立核验：proto diff 空、derivePublicKey 只返回 PublicKeyFromSeed 输出、hidden --yes 仅拒绝从不满足 confirm、--force 仅管 ownership 探测且 phase-gate 在前、生产 in==os.Stdin TTY 拒绝完好）。

## 出口
内审通过、硬闸全绿（lint 0 / make test）。外审统一留最后（按本轮 goal）。
