# Fail — S2 session · multi-tenant security · admin · agent onboarding external review

Date: 2026-07-11

The three new drills pass on the live simulated-cluster server, and most identity, TLS, invite, eviction,
and session-removal controls are well constructed. However, this review found **3 Major** false-green risks
in release-gating behavior and **1 Minor** documentation inconsistency. S2 must not be released in its current
form.

## Findings

### F1 — Major — the PIN rate-limit drill never exercises a failed-PIN attempt

`80-session-isolation.sh:155-161` performs ten successful joins with the correct PIN and then asserts that an
eleventh correct-PIN join succeeds. That does not exercise the limiter trigger described by the authoritative
flow: `architecture.md` §E.2 increments the §E.6 counter only on the Argon2 verification failure branch, and
§E.6 defines the threat as PIN brute-force attempts. The existing security regression likewise models the
defect as repeated **wrong** PINs followed by a correct control (`test/security/auth_bypass_test.go:200-226`).

This is not a theoretical distinction. A correct implementation that counts failed PIN verifications but
allows any number of successful joins would still make the current R-warm/R-11th arm GREEN forever. Thus the
drill does not pin gotcha #25 and will not flip when the promised limiter lands. The same incorrect model is
repeated in `s2-plan.md`, README, and `deploy-tier-gotchas.md`.

Independent live evidence: a temporary review-only drill sent ten wrong-PIN joins from one ctl container/IP;
all ten were processed and refused, and the eleventh same-source correct-PIN join succeeded. This confirms the
product defect while also demonstrating the missing behavior in the committed drill. The temporary drill was
removed after capture.

Required fix: make Arm R issue ten wrong-PIN attempts from fresh identities at one source IP, require all ten
to reach the real auth path (preferably count/bind their `pin_failed` events), then require the in-window
eleventh attempt to demonstrate the current gap. The future GREEN form must additionally prove another source
can succeed during the first source's block and that the first source recovers after the window.

### F2 — Major — the new flake regex classifies unrelated post-grow failures as retryable infrastructure flakes

`run-drills.sh:60` adds bare `reaches VOTER|did not reach VOTER` alternatives. Every successful grow already
logs text such as `brk2 reaches VOTER`. Since `is_flake` searches the entire log whenever the final rc is
nonzero, a drill that grows successfully and later fails any security, cleanup, or data-integrity assertion is
now classified as a retryable grow flake. A second-run GREEN can erase the original failure from the final
summary and logs.

An independent classifier check using a log containing `[ ok ] brk2 reaches VOTER` followed by an unrelated
security assertion failure returned `FALSE_FLAKE_CLASSIFICATION`. The live `10-grow-to-3` log confirms that the
success phrase occurs during normal operation. README is also internally stale: its caveat still says the
VOTER timeout does not match `FLAKE_SIG`, while the implementation now deliberately matches it.

Required fix: match a failure-specific, anchored signature such as the complete timeout/error line, not the
success description. Prefer family-aware scheduling (`-j 2` for grow drills) as already required by the
roadmap, and preserve the first-run log when any retry occurs.

### F3 — Major — the evict expose-cleanup oracle accepts a live public listener returning any HTTP error

`81-admin-evict-session-rm.sh:163-164` claims the public expose port is cleaned/refused, but its oracle is only
the negation of `curl -sf`. Curl returns nonzero for HTTP 4xx/5xx as well as connection refusal. Therefore a
leaked public listener that remains allocated but returns 404/502 after the agent disappears makes the test
GREEN, even though the asserted port-cleanup contract is false. The pre-injection sentinel proves the route was
live before eviction, but it does not distinguish a closed port from a still-listening error responder after
eviction.

Required fix: assert the transport result explicitly (for example curl exit 7 / TCP connection refusal), and
independently assert that the broker's authoritative port-allocation row is absent. Keep the sentinel baseline
as the positive control.

### F4 — Minor — the S2 landing inventory understates every drill's assertion count

`simcluster-coverage-inventory.md:259-260` records 80/81/82 as 38/38/28. The scripts contain and the live server
reports 40/40/29; README already uses 40/40/29. This weakens the inventory as an auditable generated ledger.
Update it from the actual drill verdicts, ideally with a mechanical count/check.

## Doubts and recommendations

- `81`'s `_ev_destroyed` still uses independent whole-file greps for `type` and `sid`. The current one-session
  setup limits practical ambiguity, but the project-wide single-event discipline would be stronger if each
  JSON line were parsed and both fields bound in one predicate.
- The S0-ingress sidecar is acceptable for the current bounded GET-only manifest and future `/sub` response.
  It is test infrastructure, not a hardened general reverse proxy; keep it scoped to explicitly configured
  loopback routes and do not reuse it for streaming or untrusted request bodies without limits.
- The `systemd --user` journey and six-minute `agent_roster_stale` grace remain honestly NOT-COVERED. They need
  real-machine or dedicated long-running coverage before those behaviors can be claimed.
- Gotchas #26 and #27 were independently source-checked and reproduced by the live drills. Their classification
  is reasonable, subject to F3 tightening the port-cleanup portion of #26.

## Independent verification

Completed:

- Rebuilt the full unstaged/untracked boundary against `HEAD 49e563a`; no product Go implementation changes
  are present.
- Read the project workflow, relevant C1/C2 architecture, usage/broker/cluster operations, S2 roadmap and
  inventory, S2 plan/internal review, simcluster mandate, server operations guide, and recent external-review
  conventions. Internal conclusions were used only as indexes.
- Audited all three drills, identity/tunnel/ingress helpers, image provisioning, Python TLS proxy, runner retry
  logic, and all changed documentation.
- Shell syntax passed for all simcluster shell assets; Python compilation/import and route-selection negatives
  passed; `git diff --check` passed.
- Focused tests passed: command-tree inventory and `TestPINBruteForceNoLockout5Tries`.
- `make lint` passed with 0 issues; `make test` passed; `make e2e` passed in 517.784s. The first sandboxed e2e
  attempt was intentionally stopped after socket creation was denied by the sandbox; the complete rerun with
  loopback socket permission passed P1-P13 and every matrix.
- Rebuilt `tether-sim:dev` from the current tree on `weilandserver` (Docker 29.6.1,
  `fs.inotify.max_user_instances=8192`). With automatic retries disabled, first-run live results were:
  80 = 40/40, 81 = 40/40, 82 = 29/29, and pre-existing 10-grow = 19/19; total 4 GREEN in 263s.
- Ran and removed the review-only wrong-PIN live drill: ten same-source wrong attempts refused, eleventh correct
  attempt succeeded (5/5 assertions). Confirmed the broad flake-regex misclassification with an independent
  synthetic log.

## Release recommendation

**Do not release or commit S2 as complete.** Fix F1-F3, correct F4, rerun the affected drills with
`--no-retry`, and externally re-review the closure. Existing live GREEN results establish that the current
harness runs; they do not override the false-green defects above.

---

## 主进程回复（外审 round-1 闭环，2026-07-11）

**全部 4 条 finding 采纳并修复;每条附活体验证。未 commit/stage——交外审者 round-2 re-review。**

### F1 — 采纳（Major，最重要）
**核实**:架构 `§E.2` 明确「失败（Argon2 校验失败）分支 → 拒绝 connection + 写 pin_failed + **按 E.6 速率限制计数**」,
`§E.6` 威胁为「PIN 暴力」（错误 PIN）;hermetic `TestPINBruteForceNoLockout5Tries`（`auth_bypass_test.go:200-226`）
亦为「N 错 + 1 对仍成功」。原 drill 用正确 PIN 数不触发失败计数,即使限速器只封失败也永绿——**审查完全正确**。
**修复**（`80-session-isolation.sh` Arm R 重设计）:(a) **10 个同源错误-PIN 尝试全被拒**（reached auth path）;
(b) 起 sys.events 观测,**捕获 ≥10 个 `pin_failed` 事件**（限速器真触发点 fired 10×,证 10 次尝试非被预先封锁）;
(c) 倒置 `assert_ok`:**10 次失败后第 11 个同源正确-PIN 仍成功** = 缺失的 §E.6 per-IP 封锁。flip 条件含 post-fix
全形（second-source-in-window + window-reset 原源恢复）。**同步订正** ledger #25 / plan Arm R / README。
**活体验证**:`80` 单跑无重试 **GREEN 40→42**;R-fails/R-pinfailed/R-11th 三臂皆 PASS。

### F2 — 采纳（Major）
**核实**:`brk2 reaches VOTER` 是成功日志（poll_until 描述）;bare `reaches VOTER` 会把 grow 成功后的无关
security/cleanup 失败误判为可重试 flake——**审查正确**。**修复**（`run-drills.sh:57` FLAKE_SIG）:改匹配**失败特定
的锚定行**——`timed out after [0-9]+s waiting for: brk[0-9]+ reaches VOTER`（poll 超时）与 `INCOMPLETE — did not
reach VOTER`（grow 未完成）,**绝不匹配** `[ ok ] brkN reaches VOTER`（成功）。**订正 README OQ-8 CAVEAT**（原
「VOTER-timeout 不匹配 FLAKE_SIG」现已过时）。**本地验证**:合成日志确认 FLAKE_SIG 忽略成功行、匹配超时失败行。
（家族分波两 pass 仍是 OQ-8 首要策略;此锚定重试为兜底。）

### F3 — 采纳（Major）
**核实**:`! curl -sf` 对 HTTP 4xx/5xx 也非零 → 存活但报错的泄漏监听器会假绿——**审查正确**。**修复**
（`81-admin-evict-session-rm.sh` C-port）:(a) 断**传输层连接拒绝** `curl exit 7`（区分 closed port vs 存活错误
响应器,承接 spike 实测的 RC=7）;(b) **独立断 broker 权威端口分配行缺失**（`ps -a` PORTS 节无 `web`,FK cascade）;
两者 poll_until 组合谓词。SENTINEL 基线保留为正控。**活体验证**:`81` 单跑无重试 **GREEN 40/40**,C-port 臂 PASS。

### F4 — 采纳（Minor）
`simcluster-coverage-inventory.md` S2 landing 断言数 **38/38/28 → 40/40/29**（live server verdict）。已注明「= live
server 实测 verdict」。（机械 count/check:命令树面已有 golden 门;drill 断言数的机械核对可作后续小增量,当前以
live verdict 为准。）

### Doubts 处置
- **`81 _ev_destroyed` 单行绑定**:采纳——改为 piped-grep 单行绑定（与 80 事件 oracle 的 R5 单事件纪律一致）。
- **S0-ingress sidecar 范围**:接受——仅供 loopback GET-only manifest（+ 未来 `/sub`）,不作通用硬化反代;注记入 `ingress-proxy.py` 头。
- **`systemd --user` / `agent_roster_stale` 6-min NOT-COVERED**:同意保持——诚实实测边界,留实机/长稳。
- **#26/#27 分类合理**:F3 已收紧 #26 的端口清理部分。

### 硬闸 + 边界
零产品 Go diff（改动仅 `test/simcluster/` + `docs/`）→ `make lint`/`make test`/`make e2e` + 命令树 golden 守恒不变。
**按 memory「外审阶段不 git add」——未 stage、未 commit。** 请 round-2 re-review 闭环;通过后主进程 stage+commit。
