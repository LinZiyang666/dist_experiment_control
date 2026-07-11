# G1 deploy-tier hardening — external review

Verdict: **PASS after re-review**.

Initial external review was **FAIL**; the original findings are retained below for auditability. The executor
then replied and modified the tree. I re-reviewed those changes independently and closed the findings in
“外审复审（2026-07-06）”.

Scope: unstaged working-tree changes for G1 deploy-tier hardening (#22 / #6 / #23 / #24), including
implementation, docs, simcluster drills, and new regression tests. I treated `docs/reviews/g1-review.md`
as leads only; all material checks below were re-run or independently inspected.

## Findings

### BLOCKER-1 — `install.sh` executes backticked words inside the generated `tether-broker.service` heredoc

`scripts/install.sh` writes `tether-broker.service` with an unquoted heredoc (`<<EOF`). The new G1 #23 comment
inside that heredoc contains Markdown backticks:

```text
# (including clean 0) but does NOT revive a `systemctl stop`/`restart` (systemd knows it
```

Because the heredoc delimiter is unquoted, shell command substitution runs while generating the unit. In the
simcluster install this produced real stderr during provisioning:

```text
Too few arguments.
/opt/sim/install.sh: 1: restart: not found
```

The generated unit also proves the comment was mutated by command substitution:

```text
# (including clean 0) but does NOT revive a / (systemd knows it
```

This is a release blocker. Even though the final `Restart=always` directive is present, install scripts must
not execute commands embedded in comments. Today this happens to run `systemctl stop` without arguments and a
missing `restart` command; a future backticked phrase could have side effects. Fix by removing backticks from
all unquoted heredoc bodies (or restructuring the heredoc so variables are substituted safely without allowing
command substitution). Add a regression check that a real broker install emits no `systemctl`/`not found`
noise and that the generated unit preserves the intended text.

### MAJOR-1 — simcluster README still documents drill 13 as the old RED #22 reproduction

`test/simcluster/drills/13-inbroker-reconcile-perm.sh` is now a post-G1 GREEN regression: it asserts
`/etc/tether` stays root-owned, `/etc/tether/nats.d` is tether-owned, the second grow has no natsconf
permission-denied reason, and a `User=tether` write into nats.d succeeds.

But `test/simcluster/README.md` still says:

- drill `13-inbroker-reconcile-perm` is `RED (#22)`;
- the in-broker reconciler cannot `CreateTemp` in root-owned `/etc/tether`;
- a future fix is “chowning ETC → tether” and then “promote to a plain regression.”

That is now the rejected pre-Option-B security boundary. It directly contradicts the implemented invariant
and can send future reviewers toward the tether→root Caddyfile privesc path. Update the Drills table and the
#22 “Real findings surfaced” paragraph to say G1 fixed #22 via `/etc/tether/nats.d`, not by chowning
`/etc/tether`.

### MAJOR-2 — active architecture / HA docs still contain stale “no SAN / chain-to-CA only” and old #22 target text

The G1 #24 implementation correctly documents in `cluster-runbook.md` that NATS route mesh uses standard x509
verification and needs SAN matching the route URL host. However several active top-level docs still carry the
old conclusion:

- `docs/cluster-ha-realmachine-test-plan.md` says route mTLS “只验 chain-to-CA，无 SAN” before exposing
  `:7400` / `:6222`.
- `docs/architecture.md` still says “无 cert/SAN 改动 (route mTLS 仅验 chain-to-CA)” in the
  cluster-phase-fluidity section.
- `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md` still lists the automation target as “/etc/tether 须 tether 可写
  (#22)” in §B, while G1’s accepted security boundary is `/etc/tether` root-owned plus tether-owned
  `/etc/tether/nats.d`.

Some of these files are partly historical, but they are not under `docs/reviews/` and are part of the doc set
future implementors read. At minimum add G1 correction notes at those stale lines; for `cluster-ha-realmachine-test-plan.md`
the operational firewall/SAN guidance should be corrected directly.

### MINOR-1 — `CLAUDE.md` has a Markdown formatting typo in the simcluster paragraph

The updated simcluster paragraph now contains:

```text
...跑 drill 前先读它（免密 SSH 已通、Docker 已装，别误判"本地跑不了"）。****日常 Go...
```

The four asterisks are accidental formatting noise and render poorly. This is not semantic, but `CLAUDE.md`
is loaded every session; fix to normal punctuation / bold markers.

## Doubts / residual risks

- #23’s product root cause is still not located. `Restart=always` is a valid unit-level hardening and the
  sim doctor verifies it, but it is not proof the broker no longer clean-exits on nats-loss. I accept this as
  G1’s stated scope, with the residual correctly deferred.
- #22’s code default migration is intentionally sharp: upgrading a clustered host without first moving the
  conf and adding explicit `nats_conf_path` can repoint the reconciler at a nonexistent path. `broker-ops.md`
  warns about this; the release notes should carry the same warning.
- `simcluster doctor` returns clean on an empty instance. I did not file this as a blocker because I reran it
  on a non-empty temporary instance, but it can create false confidence in future reviews.

## Tasklist

- [x] T0 — Establish scope and review baseline: confirmed no staged content initially; reviewed `CLAUDE.md`,
  G1 plan/review, architecture, HA gotchas, and simcluster docs.
- [x] T1 — #22 path relocation correctness: checked live defaults / install.sh / CLI defaults / sim seams for
  `/etc/tether/nats.d/nats.conf`; found no live old-path code straggler.
- [x] T2 — #22 security boundary: verified `/etc/tether` remains root-owned and only `nats.d` is tether-owned
  in code and sim assertions.
- [x] T3 — #22 migration and user-facing docs: checked broker-ops migration; found stale top-level docs
  outside the updated manuals (MAJOR-2).
- [x] T4 — #6 root/offline-op guard: inspected warning helper and call sites for init / force-single /
  resnapshot / recover / restore; tests pass. Warn-only scope matches the G1 plan.
- [x] T5 — #23 systemd restart change: verified only broker unit changes to `Restart=always`; sim doctor on a
  non-empty instance sees `Restart=always`. Found the heredoc command-substitution blocker in the new unit
  comment.
- [x] T6 — #24 route certificate SAN guidance: verified transport comments and x509 test; found stale active
  docs still saying no SAN / chain-to-CA only.
- [x] T7 — simcluster fidelity: inspected drill 13 and grow path; ran real deploy-tier drill 13 on the
  simcluster server. Drill passed, but README is stale (MAJOR-1).
- [x] T8 — Independent grep/static checks: ran path/SAN/ownership/restart scans; `bash -n` and
  `git diff --check` passed.
- [x] T9 — Independent tests: ran focused Go tests, race tests, bash syntax checks, simcluster drill 13, and
  simcluster doctor on a non-empty temporary instance.
- [x] T10 — Synthesize findings: this report.

## Verification log

- `go test ./internal/natsconf ./internal/clusteroffline ./internal/cluster ./cmd/tether` — PASS.
- `go test -race ./internal/clusteroffline ./internal/natsconf` — PASS.
- `go test -race ./internal/cluster` — PASS when rerun outside the sandbox; sandbox run failed only because
  local listen on `127.0.0.1:0` was denied.
- `bash -n scripts/install.sh test/simcluster/simcluster test/simcluster/drills/11-grow-gaps.sh test/simcluster/drills/13-inbroker-reconcile-perm.sh test/simcluster/drills/20-forcesingle-natsconf.sh test/simcluster/drills/21-smalldisk-tierb.sh test/simcluster/image/provision-node.sh` — PASS.
- `git diff --check` — PASS.
- `test/simcluster/remote.sh --build drill 13-inbroker-reconcile-perm` on the simcluster server — PASS,
  11/11 assertions.
- `test/simcluster/remote.sh --instance g1-ext-doctor up --brokers 1` + `doctor` — PASS for non-empty
  instance; verified `/etc/tether` root-owned, `/etc/tether/nats.d` tether-owned, and broker
  `Restart=always`.

---

## 主进程回复(逐条处置 + 重验证,2026-07-05)

感谢外审——真跑了 drill 13 / doctor / race / bash-n,抓到的 BLOCKER 是我的真 bug。逐条处置如下,均已修复
并在 sim 服务器 `weilandserver` 重验证。

**BLOCKER-1 (install.sh heredoc 反引号被 command-subst 执行) — ACCEPTED + FIXED + RE-VERIFIED。**
确认:`tether-broker.service` 用 unquoted heredoc(`<<EOF`),我在 #23 注释里写的 markdown 反引号被 shell 当
命令替换执行(跑了 `systemctl stop` 无参 + `restart` not found)、并把生成的 unit 注释改坏成 "does NOT revive
a /"。修复:去掉该注释的反引号(`install.sh:744` → "does NOT revive an operator systemctl stop/restart")。`awk`
扫全部 `cat >…<<EOF…EOF` 块确认**无一处**残留反引号/`$(...)`(其余反引号都在普通 `#` 注释行、shell 整行忽略)。
**根因自我复盘**:我最初的 drill 13 GREEN 用的是**修反引号前**烤进镜像的 install.sh——`remote.sh --build up`
的 `--build` 只 stage vendor + rsync、**不 rebuild docker image**(provision 用 image baked 的
`/opt/sim/install.sh`),得 `remote.sh --build build` 才 re-bake。这是我漏掉的验证盲区,谢谢外审逼出来。
**重验证(sim 实测,修复后 rebuild 镜像)**:① `up --brokers 1` provision **无 "Too few arguments"/"not found"**;
② 生成的 unit 注释**完整无篡改**(`# … does NOT revive an operator systemctl stop/restart (systemd knows it`);
③ `systemctl show -p Restart` = `always`;④ **drill 13 在修好的镜像上重跑 = GREEN 11/11**(#22 nats.d/ 不受
反引号修复影响)。外审建议的"install 无 systemctl noise + unit 文本完整"回归由这次 sim 实测覆盖。

**MAJOR-1 (README 仍写 drill 13 旧 RED #22 + 暗示 chown ETC) — ACCEPTED + FIXED。**
`test/simcluster/README.md` 三处更新为 G1 已修 GREEN + Option B:Drills 表 drill 13 行(:206)、
provision-node.sh 行(:97)、"#22 Real findings surfaced"段(:216-)。全部改成 #22 经 `/etc/tether/nats.d/`
修复、`/etc/tether` 保持 root-owned、**chown 整个 /etc/tether 已否决(tether→root Caddyfile 提权)**;
drill 13 现为 GREEN 回归。

**MAJOR-2 (架构/HA docs 仍有旧"无 SAN / /etc/tether 须可写") — ACCEPTED + FIXED。** 三处加 G1 更正:
`cluster-ha-realmachine-test-plan.md`(firewall/SAN 直接更正:raft `:7400` 跳 SAN、nats route `:6222` 走标准
x509 **要求 SAN 匹配 route-URL host**)、`architecture.md`(#24 note 区分 raft transport vs nats route mesh)、
`v0.4.5-ha-grow-ops-gotchas.md §B`(⑤ 从"/etc/tether 须 tether 可写"改成 Option B:`/etc/tether` root-owned +
`nats.d/` tether-owned + 否决 chown ETC)。

**MINOR-1 (CLAUDE.md `****` typo) — ACCEPTED + FIXED。** `）。****日常` → `）。** **日常`(两个 bold 间补空格)。

**Residual / doubts:**
- #23 产品级 return 点未定位:接受为 G1 scope(unit-first 兜底、产品 arm DEFERRED,外审亦认同)。
- #22 migration sharp:`broker-ops §8.6` 已 warn;**发布说明须带同一 warning**(记为发布前待办)。
- doctor 空 instance 返回 clean(false confidence):**已修** —— `cmd_doctor` 现在对空 instance `err`
  ("no nodes — up first")+ `return 1`,不再假 clean。

**处置结论**:BLOCKER + 2 MAJOR + MINOR 全部 FIXED,doctor residual 顺手修;sim 实测(install 无 noise + unit
文本完整 + drill 13 GREEN 11/11 + Restart=always + doctor clean)。请复审(下一轮外审)。

---

## 外审复审（2026-07-06）

Verdict: **PASS**.

我没有把主进程回复当作事实，按初审 findings 重新做了静态审查、grep 抽验、本地测试和 simcluster 实测。结论：
初审 1 个 BLOCKER、2 个 MAJOR、1 个 MINOR 均已闭合；初审提出的 doctor 空实例 residual 也已修复。未发现新的
阻断问题。

### Closure

- **BLOCKER-1 CLOSED** — `scripts/install.sh` 中生成 `tether-broker.service` 的 #23 注释已移除反引号；
  全文件 heredoc 扫描未发现 unquoted heredoc body 中残留反引号或 `$(`。重新 rebuild sim image 后，单 broker
  provision 不再输出 `Too few arguments` / `not found`，生成的 unit 注释保持完整，`Restart=always` 保持存在。
- **MAJOR-1 CLOSED** — `test/simcluster/README.md` 已把 drill 13 从旧 RED 复现改为 G1 后 GREEN 回归，
  并明确 #22 的修复边界是 `/etc/tether` root-owned + `/etc/tether/nats.d/` tether-owned；旧的
  “chown 整个 /etc/tether” 不再作为目标方案出现。
- **MAJOR-2 CLOSED** — `docs/architecture.md`、`docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`、以及
  `docs/cluster-ha-realmachine-test-plan.md` 的相关位置已补 G1 更正：raft `:7400` 可跳过 hostname/SAN，
  但 NATS route mesh `:6222` 走标准 x509 校验，route URL host 必须匹配证书 SAN；#22 目标也改为
  `nats.d/` 可写而不是 `/etc/tether` 整体可写。
- **MINOR-1 CLOSED** — `CLAUDE.md` 不再有 `****日常`。当前写法 `** **日常` 不够美观，但不会形成初审所说的
  连续四星 Markdown 噪声。
- **doctor residual CLOSED** — 空 simcluster instance 现在返回 rc=1，并提示先 `up`，不再给出假 clean。

### Re-verification log

- Python scan of all `<<EOF` heredocs in `scripts/install.sh` — PASS，0 个反引号 / `$(` 命中。
- `bash -n scripts/install.sh test/simcluster/simcluster test/simcluster/drills/11-grow-gaps.sh test/simcluster/drills/13-inbroker-reconcile-perm.sh test/simcluster/drills/20-forcesingle-natsconf.sh test/simcluster/drills/21-smalldisk-tierb.sh test/simcluster/image/provision-node.sh` — PASS。
- `git diff --check` — PASS。
- `go test ./internal/natsconf ./internal/clusteroffline ./internal/cluster ./cmd/tether` — PASS。
- `test/simcluster/remote.sh --build build` — PASS；确认本轮不是只 vendor/stage，而是重新烤镜像。
- `test/simcluster/remote.sh --instance g1-ext-rereview up --brokers 1` — PASS；provision 无 heredoc command-subst 噪声。
- `test/simcluster/remote.sh --instance g1-ext-rereview exec brk1 -- sed -n '15,30p' /etc/systemd/system/tether-broker.service` — PASS；
  unit 注释保留 `systemctl stop/restart` 文本，`Restart=always` 保留。
- `test/simcluster/remote.sh --instance g1-ext-rereview doctor` — PASS；非空实例检查到 `/etc/tether` root-owned、
  `/etc/tether/nats.d` tether-owned、broker `Restart=always`。
- `test/simcluster/remote.sh drill 13-inbroker-reconcile-perm` — PASS，11/11 assertions。
- `test/simcluster/remote.sh --instance g1-ext-rereview doctor` on an empty/nuked instance — expected FAIL，rc=1。

### Remaining notes

- #23 产品级 clean-exit return 点仍是后续工作，不属于本轮 G1 已实现范围；当前接受 `Restart=always` 作为
  unit-level hardening。
- #22 升级迁移仍尖锐：发布说明需要重复 `broker-ops §8.6` 的 warning，避免升级时把 reconciler 指向不存在路径。

Final external review status: **PASS**.
