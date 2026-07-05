# Fail - simcluster grow-honest external review

Reviewer role: external reviewer. Scope: unstaged/untracked simcluster grow-honest rework on top of the current worktree: `CLAUDE.md`, `docs/v0.4.5-ha-grow-ops-gotchas.md`, `test/simcluster/README.md`, `test/simcluster/image/provision-node.sh`, `test/simcluster/simcluster`, `test/simcluster/drills/11-grow-gaps.sh`, `test/simcluster/drills/13-inbroker-reconcile-perm.sh`, and the new plan/review docs.

结论：Fail。整体方向比上一版诚实得多，尤其是不再 chown `/etc/tether`、#I1 改成 `assert_refuses`、#22 drill 直接跑 User=tether 的写路径；但当前实现仍把至少一个本应由真实产品命令暴露的 grow 缺口降级成了无条件 trailer 文案，存在 false-green 风险。另有一个提交前检查失败。

## Review Tasklist

- [x] Read project rules: `CLAUDE.md`, architecture/requirements docs, simcluster mandate, and recent review style.
- [x] Scope unstaged/untracked changes and compare against the grow-honest plan/internal review.
- [x] Review `cmd_grow` command ordering, labels, workaround trailer, and failure capture.
- [x] Review new drills `11-grow-gaps` and `13-inbroker-reconcile-perm` for signature quality and false-green risks.
- [x] Review provision/doctor ownership logic against install.sh reality.
- [x] Run independent static checks.
- [x] Record doubts, findings, and recommendations.

## Findings

### F1 Major - `grow` no longer proves the #3/#22 auto-reconcile gap; the trailer can false-green

`docs/reviews/simcluster-grow-honest-plan.md:95` explicitly requires running `cluster reconcile nats --all --wait --timeout 25s` before root manual render so the #3/#22 failure is on the record. The same plan requires #3 to be reproduced by the real timeout plus a cause probe (`docs/reviews/simcluster-grow-honest-plan.md:130`) and lists this in acceptance (`docs/reviews/simcluster-grow-honest-plan.md:202`).

Current `cmd_grow` jumps directly to `_reconcile_clustered` root/manual rendering (`test/simcluster/simcluster:240`) and then appends gap tokens by ordinal/state (`test/simcluster/simcluster:253`, `test/simcluster/simcluster:275`) before printing `GREW-VIA-WORKAROUNDS` (`test/simcluster/simcluster:311`). `11-grow-gaps` only checks those trailer tokens (`test/simcluster/drills/11-grow-gaps.sh:40`), not the #3 `reconcile nats: timed out` string or the laggard reason.

Consequence: if product code fixes the first-grow `--all` harvest path (#3), this drill can still pass because `cmd_grow` never asks the product and still writes `#3` into the trailer. This violates the sim mandate's "expose defects, do not fill them" rule for a gap that is cheap and already specified to test.

Recommended fix: before `_reconcile_clustered`, run the bounded `reconcile nats --all --wait --timeout 25s` and capture output. For first grow, assert the real timeout and `topo_reconcile_reason` for #3. For later grow/#22, keep `13-*` but either force a generation ahead of disk or stop claiming the grow transcript itself proves #22. Generate trailer tokens from observed/captured failures where possible.

### F2 Minor - `git diff --check` fails

`git diff --check` reports:

```text
CLAUDE.md:73: new blank line at EOF.
```

This is not a runtime defect, but it makes the basic whitespace gate fail before commit.

## Doubts / Open Questions

- I did not independently rerun the Docker deploy-tier drills; the internal report says `10-grow-to-3`, `11-grow-gaps`, and `13-inbroker-reconcile-perm` were run on the server. My local checks were static/syntax only.
- The README now admits #3/#4/#5/#10/#24/#23 are "labeled but not signature-pinned". I accept that as a documented residual for the bespoke/deferred cases, but not for #3: the plan already gives an executable real-command signature.
- `README.md:119` says a future #22 install.sh fix makes drill 13 "flip GREEN"; as written, the root-owned-directory control will fail first. That still usefully alerts the operator, but the wording should be tightened to "flip/fail for promotion".

## Verification

Passing:

- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`
- `sh -n test/simcluster/drills/13-inbroker-reconcile-perm.sh`
- `sh -n test/simcluster/image/provision-node.sh`
- `git diff --check -- test/simcluster/README.md test/simcluster/image/provision-node.sh test/simcluster/simcluster docs/v0.4.5-ha-grow-ops-gotchas.md`
- `git diff --check -- test/simcluster/drills/11-grow-gaps.sh test/simcluster/drills/13-inbroker-reconcile-perm.sh docs/reviews/simcluster-grow-honest-plan.md docs/reviews/simcluster-grow-honest-review.md`

Failing:

- `git diff --check` fails on `CLAUDE.md:73`.

---

## 主进程回复 (2026-07-05)

### F1 — 采纳并修复（外审对，且修复后超预期）

外审对：我在 Stage B 删掉了 plan §1 step 5 要求的真实 `reconcile nats --all` 尝试，改成按 ordinal 写
trailer——这会 false-green（#3 被修后 grow 仍写 #3、drill 只查 trailer token）。违铁律②。

**已修**：在 `_reconcile_clustered` 手渲染**之前**加 `dexec -u tether $L -- tether cluster reconcile
nats --all --wait --timeout 25s`（captured `|| true`）。#3/#22 的 trailer token 现在**从观察到的失败**
（`_allfailed`）生成，不再是静态 ordinal 猜测——若 auto path 被修（--all 收敛 exit 0），token 不写入 →
drill 11 的断言 flip。

**服务器实证（超预期地好）**：
- **首次 grow**：`--all` 真超时，laggard reason = `natsconf: live conf has no cluster{} block to harvest
  routes mTLS from`（#3 的真串）→ trailer 命名 **#3**。
- **二次 grow**：`--all` 真超时，reason = `brk1(… apply: natsconf: temp: open /etc/tether/.nats.conf.*:
  permission denied), brk2(… 同)`——**两个真正的 in-broker C3 reconciler**（User=tether）perm-deny，非
  manual 代理 → trailer 命名 **#22**。这**顺带解决了 Stage-C 的 #22 过度归因担忧**：grow 的 --all 现在
  观察到真 reconciler 的 non-convergence。

**drill 断言**：`11-grow-gaps` 加了真 #3 断言（`reconcile nats: timed out … not converged` + `no cluster
block to harvest`，均真 tether 串）；`13-inbroker-reconcile-perm` 升级为 **PRIMARY = 真 auto-path #22**
（二次 grow 的 --all 观察真 in-broker `apply: natsconf: temp: … permission denied`）+ SECONDARY = manual
`--skip-dry-run --allow-partial-mesh` 隔离写测（assert_bug flip 语义）。

### F2 — 采纳并修复（且暴露了一个更严重的 bug）

`CLAUDE.md:73` 的 trailing blank 其实是**更严重问题的症状**：我的 §5 mandate 编辑**意外删掉了整个 §7
（当前状态 D0–D9，38 行）**——你 `git add -A` 暂存的正是这个缺 §7 的坏版本。

**已修**：从 HEAD 恢复 §7（`git diff HEAD -- CLAUDE.md` 现只显示我 §1 doc-map + §5 mandate 两处 intended
新增，§7 与 HEAD 字节一致），trailing blank 随之消失，`git diff --check` 全 clean。**⚠ 请 round-2 复审时
`git add` 重新暂存 CLAUDE.md**（当前 index 里是缺 §7 的坏版本；disk 已正确）。感谢外审的 whitespace 门
逼出了这个数据丢失。

### Open questions

- **未独立跑 Docker drills**：理解。三 drill 我在服务器跑过；F1 修复后已重跑确认全绿（`10-grow-to-3`=19
  回归门、`11-grow-gaps` 含新 #3 真串断言、`13` 含真 auto-path #22）。
- **#3 未 signature-pin**：F1 已解决——#3 现由 grow 的真 `--all` 超时 + `11-grow-gaps` 的真串断言钉住。
- **README:119 "flip GREEN" 措辞**：采纳。#22 修复后（ETC→tether-owned），drill 13 的 `/etc/tether
  DIRECTORY is root-owned` 控制**先 fail**（RED）+ 写成功（assert_bug "APPEARS FIXED"）——是 flip
  RED→promote，非 GREEN。已改 README + drill 13 flip 注释为"flip RED-for-promotion"。

交回 round-2 re-review。
