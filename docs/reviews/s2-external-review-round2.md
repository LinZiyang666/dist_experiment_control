# Fail — S2 external re-review round 2

Date: 2026-07-11

Round 1 F1 and F3 are closed, and the specific F2 bug that matched a healthy `reaches VOTER` line is fixed.
However, the retry path still treats a VOTER timeout as infrastructure flake without checking whether the run
was actually highly concurrent, then overwrites the first-run evidence. F4 also regressed immediately after
Arm R gained two assertions. Result: **1 Major and 1 Minor remain; S2 is not releasable.**

## Closure matrix

| Round-1 item | Result | Independent re-review |
|---|---|---|
| F1 — correct-PIN rate-limit model | Closed | Arm R now issues ten wrong-PIN attempts from one ctl container/IP, requires all ten commands to fail, observes at least ten fresh-capture `pin_failed` events, then proves a fresh correct-PIN join still succeeds. Live 80 passed these three load-bearing assertions and finished 42/42. |
| F2 — healthy grow success matches flake regex | Partially closed | The new anchored regex rejects the healthy success line and accepts the two intended failure strings. A remaining concurrency/context defect is described below. |
| F3 — `! curl -f` accepts live 4xx/5xx listener | Closed | Cleanup now polls for curl exit 7 and independently requires the broker PORTS allocation named `web` to disappear. The sentinel baseline remains. Live 81 passed 40/40. |
| F4 — assertion ledger mismatch | Not closed | The developer changed inventory to 40/40/29, then changed drill 80 to 42 assertions. Live output and mechanical source count are 42/40/29; README and inventory still say 40/40/29. |
| `_ev_destroyed` / ingress scope recommendations | Closed | Event fields are now bound on one line; the Python helper explicitly limits itself to bounded loopback GET responses and rejects general proxy reuse. |

## Findings

### R2-F1 — Major — VOTER retry still ignores the concurrency condition and erases first-run evidence

`run-drills.sh` correctly narrowed `FLAKE_SIG`, but `is_flake` still decides from only final rc + any matching
log line. It never checks `JOBS`, number of simultaneous grow drills, or whether the failure occurred under the
documented peak-concurrency condition. Therefore `run-drills.sh -j 1 82-agent-onboarding-invite` will classify
a solo VOTER timeout as an infrastructure flake, although `simcluster:223-232` explicitly says only a
parallel-suite timeout is the known flake and a solo/low-concurrency timeout is a real regression.

The retry then calls `run_one` with the same `$LOGDIR/<name>.log` path, truncating the first-run log. A transient
product regression can thus become final GREEN with its evidence erased. The anchored regex closes the original
false match but not this release-gate failure mode.

Required fix: either remove VOTER timeout from automatic retry and rely on the documented two-wave family
scheduling, or gate retry on a demonstrably high concurrent-grow condition. Preserve the first attempt as a
separate log in all retry cases and expose the retry/flaky status in the final verdict rather than silently
replacing it.

### R2-F2 — Minor — F4's assertion-count correction is stale again

Mechanical count and live verdict are now:

- `80-session-isolation`: 42
- `81-admin-evict-session-rm`: 40
- `82-agent-onboarding-invite`: 29

`simcluster-coverage-inventory.md` and README still report 40/40/29. The developer response itself says
“40→42”, so the inconsistency is directly visible in the same change. Update both ledgers and preferably add
the mechanical check suggested in round 1; manually copying live counts has now failed twice.

## Residual recommendations

- Arm R's fresh capture makes unrelated events unlikely, but `_ev_pinfailed_ge10` counts only event type. For
  maximum false-green resistance, bind `sid=lab`, `role=ctl`, and ten distinct fingerprints in the same JSON
  lines. Current live behavior produced the intended ten events, so this is not separately release-blocking.
- Replace the obsolete correct-PIN rows in `s2-plan.md` rather than retaining them below an override note. A
  reader or generator consuming the table can otherwise still select the superseded recipe.

## Independent verification

- Rebuilt the staged-vs-unstaged closure scope: nine docs/test files changed; no `internal/` or `cmd/` product
  implementation diff.
- Shell syntax, Python syntax, `git diff --check`, command-tree inventory, security wrong-PIN focused test, and
  `make lint` (0 issues) passed. The first lint invocation was blocked by sandboxed Go-cache writes; the permitted
  rerun passed.
- Synthetic classifier controls passed: healthy `brk2 reaches VOTER` + unrelated failure did not match; the
  exact timeout and `INCOMPLETE` lines did match. Source inspection confirmed the same match occurs at `-j 1`
  because `is_flake` has no concurrency input, and retry overwrites the original log.
- Live `weilandserver`, `--no-retry -j 2`: 80 = 42/42 and 81 = 40/40, both first-run GREEN in 67s. The F1
  wrong-PIN/event/correct-PIN assertions and F3 transport+allocation assertion passed. No throwaway containers
  remained.
- Full `make test`/`make e2e` were not repeated: round 1 ran both successfully against the same staged product
  tree; round-2 changes are shell/docs plus a Python comment, with zero Go diff. Focused invariants and live
  deploy-tier tests cover the changed behavior.

## Release recommendation

**Do not release S2 yet.** Fix R2-F1 and R2-F2, rerun the two synthetic retry controls plus live 80, and submit
a narrow round-3 closure review.

---

## 主进程回复（外审 round-2 闭环，2026-07-11）

**2 条 finding 采纳并修复;两条 residual 亦处理。未 stage/commit——交 round-3 re-review。**

### R2-F1 — 采纳（Major）
**核实**:`is_flake` 只看 rc + FLAKE_SIG,无并发输入;`-j 1` 单跑的 VOTER timeout 是**真回归**（simcluster:223-232
「只有并行套件 timeout 才是已知 flake,solo timeout 是真回归」），却被当 flake 重试;且重试覆盖 `$LOGDIR/<name>.log`
首轮证据——**审查完全正确**。**修复（采纳选项 a——从自动重试移除 VOTER）**:
- `run-drills.sh:57` FLAKE_SIG **移除** `reaches VOTER` / `INCOMPLETE — did not reach VOTER`,回到仅 systemd/
  container 真 infra flake。VOTER timeout 现**永不自动重试**——显 RED,靠 OQ-8 两波家族分波（首要缓解,让 flake 根本
  不发生）+ CAVEAT 手动单跑（solo timeout 不被静默吞掉）。
- **保留首轮证据**:重试前 `cp <name>.log → <name>.attempt1.log`（+ .rc）,summary footer 显式提示 retried 及首轮
  日志位置——**首轮证据永不被静默截断**。
- 订正 README OQ-8 CAVEAT（回退为「VOTER timeout 不是 flake sig、手动单跑;真 infra flake 保留 attempt1.log」）。
**验证**:两条合成控制通过——`timed out … brk2 reaches VOTER` 与 `INCOMPLETE — did not reach VOTER` 均**不**匹配
FLAKE_SIG;`[ ok ] brk2 reaches VOTER` 成功行不匹配;`systemd never came up` 真 infra flake 仍匹配（保留重试）。

### R2-F2 — 采纳（Minor）
**核实**:80 因 F1 加了 2 个断言变 42,但 README/inventory 仍 40——**审查正确,手动 copy 又漏一处**。**修复**:
README（80 行尾 `(42 assertions)`）+ inventory（`80-session-isolation（GREEN，42）`）→ **42/40/29**。**采纳机械 count**:
inventory 注明 `grep -cE '^[[:space:]]*assert_(ok|refuses|bug)\b' drills/<name>.sh` = **80→42、81→40**（与 live
verdict 一致,无条件断言）;**82→33 静态 ≠ 29 live**——差 4 = U 臂在 `systemd --user` NOT-COVERED 路径不执行（else
只 warn、非 assert）,故 82 以 **live drill_end verdict 为真值**。给出静态 grep 交叉核对基准,避免第三次手动漂移。

### Residual 处置（非 release-blocking,一并做）
- **`_ev_pinfailed_ge10` 绑 sid+role**:采纳——改 piped-grep 单行绑定 `type:pin_failed | sid:lab | role:ctl` 计数
  ≥10（原只计 type）。10 distinct fp 未加（提取+去重脆弱,current live 实产 10 事件,非 release-blocking）。
- **替换 plan 陈旧 correct-PIN 行**:采纳——`s2-plan.md` 的 R-warm/R-11th 表行**直接替换**为 R-sub/R-fails/
  R-pinfailed/R-11th（错误-PIN 模型),不再保留被 override 注盖住的旧 recipe（防 generator/reader 误选）。
- **ingress-proxy.py scope 注**:已加（loopback GET-only,不作通用硬化反代）。

### 验证 + 边界
- **live 80 = 42/42 GREEN**（强化后的 pin_failed 绑定 + 错误-PIN Arm R 全过,--no-retry）;81 未改（F3 round-1 已闭,40/40 不变）;82 未改（29/29）。
- 零产品 Go diff（改动仅 `test/simcluster/` + `docs/`）;命令树 golden / make lint/test/e2e 守恒不变。
- **按「外审阶段不 git add」——未 stage、未 commit。** 请 round-3 narrow closure review;通过后主进程 stage+commit+push。
