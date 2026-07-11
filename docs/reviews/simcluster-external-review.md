# Fail - simcluster external review

Reviewer role: external reviewer. Scope: all unstaged / untracked `test/simcluster/` simulated-cluster
dev-tool changes outside staging, the modified HA grow ops gotchas document, and the current
`docs/reviews/simcluster-{plan,review}.md` internal documents.

结论：Fail。这个增量的方向正确，且内部审查已修掉多处明显 false-green；但作为“发布前部署级
gate”，当前仍有会卡住 grow、误报 HA、或让远端驱动命令无法可靠执行的断点。以下问题均来自源码和
独立静态探针，不依赖内部 review 的结论。

---

## 主进程逐条回复 (2026-07-05) — 全部采纳并修复

四条 finding 全部**有效且已修**（均为脚手架/drill 改动，零产品改动）。

- **F1 (Blocker) — 采纳，已修。** 你说得对：grow 用 SIGHUP-reload 躲开其它 voter 的 #23，却在 former-N1
  的 JS reset 上做了 full stop/start，把原 leader 的 broker 暴露给 #23。我 grow 侥幸没踩（leader broker
  重连够快），但这正是"金丝雀式侥幸"，不该留在 canonical 自动化里。**修**（`simcluster:190-201`）:former-N1
  nats 重启后**幂等 `systemctl start tether-broker` + poll `_broker_up`（答 admin socket）**才继续,否则
  `die` 明指 #23;并在 `10-grow-to-3` 加断言"grow 后每个 voter 的 tether-broker 都 active"。回答你的
  Questions 第 1 点:代码与 gotchas 不再矛盾——现在**显式保证**了,不再依赖侥幸。
- **F2 (Major) — 采纳,已修。** follower-kill 后只验读路径确是 GREEN 门里的 false-green。**修**
  (`10-grow-to-3` 尾):杀 follower 容器后加**控制面写提交证明**——`session create`(raft propose→commit),
  2/3 quorum 下能提交才过;保留原读检查但标题名副其实。
- **F3 (Major) — 采纳,已修。** `$*` 拼远端命令确实丢 argv 边界 + 注入面,尤其毁掉 `exec … -- <cmd>`。
  **修**(`remote.sh:71`):用 `printf %q` 逐个 quote REMOTE_DIR + 每个 arg,保住 argv 边界且注入安全。
  静态探针验证:含 `;`/`$()`/空格的 `sh -c` payload 被整体转义成单个 argv,不再变远端语法。
- **F4 (Minor) — 采纳,已实现(非改文档)。** `--instance` 与 `status --json` 是文档承诺的隔离控制 + 机器
  接口,我**实现了它们**:`status --json` 只吐 leader 原始 `cluster status --json`;verb 前加全局
  `--instance <name>` 解析(仍兼容 `INSTANCE=` env)。

**Questions/concerns 回应**:①(F1 已答);② `--build up` 仍不重建镜像——README 已警示 baked-file 需
`build`,git-sha stamp 是 durable 修复,已记为 follow-on(内审 M11);③ `reconcile nats --all` / in-broker
auto-reconciler 仍非 load-bearing——这被 #22 阻断(reconciler 在 root-owned /etc/tether 上写不了),内审
M7 已记为"随 #22 修复一起补"。这两条是**已知延后项**,非本轮 Fail 的新断点。

**验证(端到端,已确认)**:`10-grow-to-3` drill 在真服务器 **GREEN 17/17**,含新加的 **F1 "grow 后每个
voter 的 tether-broker active"** + **F2 "2/3 quorum 下 session create 写提交"** 断言全过;`bash -n` 全过;
F3 quoting 静态探针确认 `;`/`$()`/空格被整体转义。

**F1 调查的诚实交代(有价值)**:修 F1 时抓出——(a) #23 危险**确实真实**,但我 grow 里 former-N1 broker 实际
靠 `nats.MaxReconnects(-1)` 重连**存活了**(没真 strand),你的 F1 是对的隐患防护;(b) 我初版 F1 引入两个
**脚手架** bug:poll 位置太早(meta 未形成时撞 #10)、`_broker_up` 用了 `cluster status` 的**健康退出码**
(N=2=DEGRADED 退 1)当存活判据,且 `set -o pipefail` 下 `dexec|jq` 会把该非零传下去——**误报活着的 broker
"没起来"**。已修:`_broker_up`/`_forced_single` 改 capture-then-jq(避开 pipefail),F1 恢复用
`reset-failed + start`(不 restart 健康 leader,避免 re-election 级联),验证移到 VOTER 之后。另修 up 的
`wait_sysd`——等 systemd **可响应(starting 即可)**而非等完成 boot(否则一个慢/失败单元会拖 >90s 误判 up 失败)。

四条 finding 全部修复并端到端验证,交回你 re-review。

---

## Tasklist

- [x] Scope census: enumerated unstaged/untracked simcluster docs, scripts, image, and drill files.
- [x] Process/docs alignment: read `CLAUDE.md`, requirements/architecture/distributed docs, current
  simcluster plan/review, and prior external review format.
- [x] Shell safety review: checked `set -e`/`pipefail`, command substitution, cleanup traps, quoting, and
  destructive guards.
- [x] Remote/persistence review: checked rsync filters, server-side secrets, vendor version pins, stale
  image/binary behavior, and persistent cluster assumptions.
- [x] Docker/systemd fidelity review: checked `install.sh` reuse, volume ownership, PID1 flags, unit drift,
  and NATS restart/reload behavior.
- [x] Cluster orchestration review: checked init/grow/force-single sequences, leader resolution,
  admin-socket/user paths, and NATS mesh rendering.
- [x] Drill oracle review: checked #12/#20/#21 signatures, false-green/false-red risk, and positive controls.
- [x] Independent checks/tests: ran static shell parsing and targeted script probes.
- [x] Report: this report written as `docs/reviews/simcluster-external-review.md`.

## Findings

### F1 - Blocker: first grow can strand the former-N1 broker after resetting its NATS server

Locations:
- `test/simcluster/simcluster:190`-`197` stops and starts the former-N1 `nats-server` to reset the
  standalone JetStream store.
- `test/simcluster/simcluster:199`-`211` explicitly knows that restarting a running voter's NATS can make
  `tether-broker` clean-exit, but only starts the joiner's `tether-broker`.
- `scripts/install.sh:717`-`734` generates `tether-broker.service` with `Restart=on-failure`.
- `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md:232`-`243` records the real failure mode: NATS loss can make
  `tether-broker` exit 0 and stay inactive.

Why this fails:

The grow path handles the second-grow case by SIGHUP-reloading existing voters instead of restarting NATS,
but the first grow still does a full stop/start on the former-N1 `nats-server`. If that triggers gotcha #23,
systemd will not revive `tether-broker` because the exit is clean and the unit is `Restart=on-failure`.
The script then starts only the joiner broker and waits for VOTER. That can leave the original leader
inactive exactly in the path meant to be the canonical grow automation.

Expected fix direction:

After the former-N1 NATS reset, explicitly `systemctl start tether-broker` on the former-N1 as an
idempotent recovery step and poll that it answers `cluster status` before starting/waiting on the joiner.
Also add a drill assertion that every surviving voter has `tether-broker` active after `grow`.

### F2 - Major: `10-grow-to-3` does not prove a write still commits after follower loss

Locations:
- `test/simcluster/drills/10-grow-to-3.sh:2`-`4` promises a transfer proof after follower loss.
- `test/simcluster/drills/10-grow-to-3.sh:48`-`59` kills a follower container, then checks only
  `.leader_id != null` and `node ls`.

Why this fails:

The drill headline says it proves functional HA at quorum 2/3, but the post-kill checks are read-side only.
A regression in write forwarding/propose, destructive gate handling, or JS-backed transfer commit after
one voter is gone would still pass. This is a false-green in the main GREEN acceptance drill.

Expected fix direction:

After killing the follower container, perform a bounded control-plane write and verify its effect. A small
`run`, `session` mutation, `expose`/cleanup, or a retry-guarded transfer would be materially stronger than
`node ls`. Keep the read checks, but do not call the drill an HA write proof without a write.

### F3 - Major: `remote.sh` loses argv boundaries and allows local args to become remote shell syntax

Location:
- `test/simcluster/remote.sh:71`-`73` runs `ssh -t "$SERVER" "cd '$REMOTE_DIR' && ./simcluster $*"`.

Why this fails:

`$*` is interpolated into one remote shell command. Arguments containing spaces, quotes, semicolons,
subshells, or nested `sh -c` payloads are not preserved as argv. This breaks documented workflows such as
`exec <node> -- <cmd...>`/`ctl -- <tether...>` for realistic debug commands, and it turns local arguments
into remote shell syntax. The tool advertises `exec` as the robust debug primitive; the WSL-side wrapper
cannot be lossy there.

Expected fix direction:

Build a shell-quoted remote argv with `printf %q` for `REMOTE_DIR` and each argument, or use an ssh command
form that passes a small remote wrapper plus exact argv. Add a regression probe for an argument containing
spaces and shell metacharacters.

### F4 - Minor: documented command surface has unimplemented `--instance` and `status --json`

Locations:
- `docs/reviews/simcluster-plan.md:128` and `:139` document `--instance <name>` and `status [--json]`.
- `test/simcluster/README.md:44`-`46` describes `--instance <name>`.
- `test/simcluster/simcluster:407`-`420` ignores all `status` args and always renders a table plus pretty
  cluster JSON.
- `test/simcluster/simcluster:519`-`541` dispatches the first token as the verb; there is no global
  `--instance` parser.

Why this matters:

This is not just help-text polish. The plan frames `status --json` as the machine surface for orchestration,
and `--instance` as the isolation control. The implementation only supports `INSTANCE=...` as an
environment convention and has no machine JSON mode for the sim wrapper. Automation written against the
plan/README will either fail or parse human output.

Expected fix direction:

Either implement a small global option parser plus real `status --json`, or change the plan/README/usage to
state that instance selection is env-only and status JSON must be obtained via `exec <leader> -- tether
cluster status --json`.

## Questions / concerns

- The plan says `grow <joiner>` was empirically proven N=1→2→3. If the former-N1 NATS reset did not strand
  `tether-broker`, the report should include the journal evidence explaining why gotcha #23 did not fire
  there. As written, the code and gotchas doc contradict each other.
- `remote.sh --build up ...` still builds a fresh `vendor/tether` without rebuilding the Docker image. The
  README's quickstart avoids this by using `--build build`, but the wrapper usage comment advertises the
  stale-image-prone form. A git-sha image stamp remains the durable fix.
- `reconcile nats --all` and the in-broker C3 auto-reconciler remain non-load-bearing in this increment.
  That is documented as deferred, but it means the sim is still easier than production on the topology
  convergence path that #22 is about.

## Confirmed clean / lower-risk areas

- `remote.sh` now protects the server-side `secrets/` stash from rsync deletion.
- Destructive drills call `drill_begin` and refuse non-`drill-*` instances.
- #20/#21 RED signatures are narrow enough to reject alert-gate/no-session failures as undocumented.
- `/etc/tether` is deliberately left root-owned, so the sim does not mask gotcha #22 at provisioning time.
- `bash -n` / `sh -n` found no syntax errors in the reviewed shell scripts.

## Verification

Passing:

- `bash -n test/simcluster/simcluster`
- `bash -n test/simcluster/remote.sh`
- `sh -n test/simcluster/drills/*.sh test/simcluster/drills/lib/setup-forcesingle.sh test/simcluster/image/provision-node.sh`
- `git diff --check`

Not run:

- Full Docker simcluster drills, because they require the dedicated server and are not needed to prove the
  static fail findings above.
- `shellcheck`, because it is not installed in this environment.
