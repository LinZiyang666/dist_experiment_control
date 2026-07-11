# S1 内审 workflow 原始产出（committed 审计证据）

Date: 2026-07-11. 归属：`docs/reviews/s1-review.md` §0 的可复核 provenance（外审 round2 R2-F3 补入库）。

> **这是什么**：S1 内审 workflow（`s1-code-review`，runId `wf_6ba03f33-99e`）的 **13 条 subagent 原始产出**
> 入库——6 reviewer + 6 verifier + 1 synth。该 workflow 运行于一个**其后损坏的会话**，其 `journal.jsonl`
> 是会话本地 Claude Code 工件、**不在仓库、随损坏会话已不可独立读取**；接手会话在其失效前读出这 13 条并
> 落此文件作为 **durable 审计证据**，使「13 agent（6+6+1）」可直接清点、每条 finding 可溯源。
> **归一化注记**：内容逐字保留，**仅去除每行行尾空白**（过 `git diff --cached --check` whitespace 门）。
>
> **Workflow 记录元数据**（转录自该会话 workflow 记录 `wf_6ba03f33-99e.json`，同为会话本地工件、此处转录、
> 非独立可再验）：`agentCount=13`、`defaultModel=claude-opus-4-8[1m]`、`status=completed`、phases=
> `[Review(6), Verify(6, identity-mapped), Synthesize(1)]`、脚本内**无 `model:` 覆写**（`agent()` 省略 model
> → 继承会话 Opus 4.8）。诚实边界：模型/数量取自转录、非独立可再验；**可入库复核的是下方 13 条产出本身**
> （清点即 6+6+1=13、内容即各维度 finding 与对抗裁决）。

---

## [6] reviewer-1 — coverage / inventory / ledger 真相

<sub>subagent key `v2:766b4aaad916a317e541a02be98be9a7b1edb1369e86ad02bce9d7f57729272c`</sub>

I've completed my verification across the coverage/inventory/ledger dimension. All source claims are grounded in code I read. Here are my findings.

---

**1. [minor] `docs/reviews/simcluster-coverage-inventory.md:234` — the null-diff writeup falsely claims `login` emits no sys.events kind (contradicts the inventory's own §1.1).**

The §4 null-diff record states: *"用户面命令（exec/run/ps/history/node ls/**login**/push/pull）**不发** `sys.events` kind"*. This is factually wrong for `login`. A ctl PIN first-connect emits `member_joined` on the events stream:
- `internal/broker/authcallout.go:81` wires `EmitEvent: b.pubSysEvent` with the literal comment `// P7: member_joined / pin_failed → events stream`.
- `internal/authcallout/handler.go:353` — `h.emit("member_joined", {..., "via":"pin", "role":"ctl"})` fires when a ctl PIN-bootstraps into a session (and `:348` emits `pin_failed` on a wrong PIN).
- `drills/60-user-journey.sh:51` does exactly this: `"$SIM" session "$SID" --pin "$PIN"` creates a fresh session and logs the ctl in via PIN → the ctl is not yet a member → `ensureMember` takes the PIN-bootstrap branch → `member_joined` is emitted.
- The inventory's **own §1.1** (line 27) lists `member_joined` as a `pubSysEvent` kind emitted by the authcallout `h.emit` path, attributed to S2-80.

So the null-diff sentence directly contradicts §1.1: the 60-user-journey login path *does* traverse a sys.events emission. Why it's wrong and matters: this is precisely the "green for a subtly-imprecise reason" the null-diff gate exists to catch — the honest statement is that 60's login **exercises** `member_joined` but 60 **does not assert** on it (the assertion is deferred to S2-80), so S1 still ticks **0** event rows. The coverage-tick *conclusion* (0 rows) remains correct; only the supporting rationale is false. **Fix:** amend line 234 to remove `login` from the "不发 sys.events kind" list and add a clause like: "`login` (ctl PIN first-connect) *does* emit `member_joined` via `pubSysEvent` (`authcallout.go:81` → `handler.go:353`); 60 exercises but does not assert it — the event assertion is deferred to S2-80, so S1 still ticks 0 event rows." (If a wrong-PIN negative-control arm is ever added to 60, `pin_failed` at `:348` applies too.)

---

**Everything else in my dimension verified accurate — no other findings:**

- **94/8/11 command-tree claim — CONFIRMED and deterministic.** `command_tree_golden.txt` has exactly 94 paths, 8 `[hidden]` commands, 11 `--yes(hidden)`. `go test ./cmd/tether -run TestCommandTreeInventory -count=2` passes (deterministic: `dumpCommandTree` dedups + `sort.Strings` on flags and `sort.Slice` on subcommands). The 11 = 7 canonical `registerYesRejector` + 4 shared-RunE deprecated aliases (`export-incident` correctly has no `--yes`), matching §4's decomposition. `agent` correctly lacks `--upgrade-url-allow`; `serve` correctly has it.
- **§4 arm-level ticks — no over-claim in spot-check.** Verified real arms behind: version/completion (`60:156-157` J18a/J18b), Ctrl-C→pgroup/no-orphan (`60:108-125` J11/J11b), ps PORTS + N=1-no-HOME (`60:139-140` J14), history `--follow` smoke (`60:152-153` J17), too_large pull >2 GiB (`61:74-76` B6). Partial/deferred arms are honestly marked `◐`/`☐` (e.g. `exec --timeout` S1◐, `completion --no-descriptions` partial, `--brokers` S5☐, `run --ack-alerts` S9☐).
- **#I1-folded — CONFIRMED.** `drills/11-grow-gaps.sh:17,61-72` carries the #I1 head-note and `assert_refuses "no raft state exists|never auto-bootstraps"`; the ledger's "#I* 族关闭, #I1 并入 drill 11" is accurate.
- **DOC-3-confirmed — accurate, exact citations.** `error_hints.go:34` references "the agent's `--upgrade-url-allow` flag"; `usage.md:1443` references "检查 agent 的 `--upgrade-url-allow`"; golden line 7 confirms `agent` has no such flag; `serve.go:267` confirms only `serve` has it.
- **DOC-5-non-defect — accurate.** `cmd/tether/exec.go:131-135` (`os.Exit(128)` on `ExitCode<0`) + `internal/agent/exec.go:202-208` (returns `ExitCode()=-1`) match; `usage.md:669-670` correctly documents the flat-128 contract, so "not a doc defect" holds.
- **#25-empty — defensible.** No runtime deploy gotcha surfaced; the one defect S1 found (DOC-3, a mis-pointed hint/doc with a latent missing-agent-knob flavor) is logged separately under DOC-n, consistent with the ledger's stated taxonomy (product-behavior gotcha vs doc defect). Not a mis-tick.
- **README drill-11 5-site drift — fully settled.** README mentions drill 11 only with the new `GREW-VIA-TETHER-CLUSTER-ADD` spelling (lines 148/175/246/273-296); no stale `GREW-VIA-WORKAROUNDS`/RED framing of drill 11 remains in README. The only `GREW-VIA-WORKAROUNDS` strings are inside `drills/11-grow-gaps.sh:47-49`, where they correctly assert the old trailer is **ABSENT**.


---

## [7] reviewer-2 — 并发 / 泄漏 / host-safety（-j 并行 drill-all）

<sub>subagent key `v2:28cf6356bdf3487d6ce75f747e3e3c4ecb37d28a6b73b4cffbd1b54300f22772`</sub>

## Findings — dimension: concurrency, leaks, host-safety under parallel drill-all (-j 6)

**1. [MAJOR] The OQ-8 "`-j 6` family-wave cap" is unsound — it MAXIMIZES concurrent grows instead of separating families.**
`README.md:230-234` claims `./run-drills.sh -j 6` is "a family-wave cap: the grow / force-single families (1x/2x) … → keep them off a single all-at-once burst." But `-j` is a pure global count throttle with zero family awareness (`run-drills.sh:144` `while [ "$(jobs -rp|wc -l)" -ge "$JOBS" ]`), and drills are launched in glob/alphabetical order (`run-drills.sh:84-89` discovery, `:142` launch loop). The actual order is `00,10,11,12,13,20,21,60,61,62`, so the first 6-wide wave is `00,10,11,12,13,20` — which is **five cluster-forming grow/force-single drills at once** (10,11,13 grow to N=3; 12,20 grow-then-force-single; verified via grep). That is the *exact* peak-concurrency scenario the CAVEAT (`README.md:225-228`) says blows the 150s VOTER-promotion window. Because all five grow-family drills sort *before* the light user-plane family (60/61/62), `-j 6` front-loads every heavy grow into wave 1 and defers the cheap drills — the opposite of the stated goal. `-j 6` cannot bound concurrent grows to a safe number: there are 5 grow drills and 6 > 5. Worse, a VOTER-timeout log line is `timed out after 150s waiting for: <j> reaches VOTER` (`simcluster:215` via `log.sh:33`), which does **not** match `FLAKE_SIG` (`run-drills.sh:57`, only `…waiting for: systemd responsive`), so these grow-timing REDs are never auto-re-run — every wrap run needs manual single re-runs, defeating the "one green wrap run" purpose.
*Fix:* schedule by family, not by a count. Run the grow/force-single family (`1x`/`2x`) in its own serial-or-`-j 2` pass and the N=1 user-plane family (`00/21/60/61/62`) at full parallel in a separate pass; or interleave the launch order so ≤2 grow drills are ever in flight. Do not describe `-j 6` as a family-wave cap in `README.md` — it isn't one.

**2. [MINOR] `62-remote-fs-safe.sh:38` uses a fixed `sleep 2` to wait for the FUSE mount — a fixed sleep that can spuriously RED under parallel CPU contention.**
`_mount_hangfs()` does `setsid python3 …/hangfs.py $MNT … & sleep 2; grep -q fuse.hangfs /proc/mounts`. This violates the "poll_until never a fixed sleep" discipline (every other wait in the tree uses `poll_until`, `log.sh:26`). Drill 62 launches late in the `-j 6` wrap (position 10) and its slot opens while wave-1 grows (10/11/13) are still forming raft/JS-meta clusters at peak CPU; 2s can be too short for fusepy to publish the mount, making the "probe: mount fuse.hangfs (healthy)" assert (`:55`) go RED. It is fail-closed (not a false-GREEN), but it is an avoidable concurrency flake.
*Fix:* replace `sleep 2; grep …` with `poll_until <t> 1 "hangfs mounted" -- "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts`.

**3. [MINOR] `62-remote-fs-safe.sh` alive-controls (`:65`, `:76`, `:91`) are NOT wrapped in `timeout`, contradicting the drill's own boundedness invariant and risking a 10-minute drill hang under a real wedge.**
The header (`:6`) asserts "Every wedge-touching command is bounded by an external `timeout`," and the probe helper `RFS()` (`:44`) is `timeout 25 …`. But the three alive-control assertions `"$SIM" ctl -- exec agt1 -- true` — whose *entire purpose* is to catch an agent wedge right after `_wedge` — have no outer `timeout`. `tether exec` defaults to `--timeout 10m` (`cmd/tether/exec.go:152`, `context.WithTimeout` at `:94`). So the one command designed to detect a wedged agent would, on an actual wedge, hang for up to 10 minutes rather than fail fast — holding a parallel job slot in `run-drills.sh` and deferring `cmd_drill`'s `nuke` (`simcluster:491`) that reaps the wedged FUSE mount. This only bites when a real agent-wedge bug is present (the measured premise is "no wedge"), hence MINOR, but it is exactly the failure mode the drill exists to surface, left unbounded.
*Fix:* wrap the alive-controls in `timeout` (e.g. `timeout 25 "$SIM" ctl -- exec agt1 -- true`) so a genuine wedge RED's fast instead of stalling the wrap run.

## Also checked — no finding
- **FUSE host-mount leak:** none. Containers run `--privileged` without shared bind-propagation (`docker.sh:29-43`), so the in-container `/mnt/hung` never propagates to the host namespace; `docker rm -f` in `cmd_drill`'s teardown (`simcluster:485-492`) SIGKILLs the SIGSTOP'd `hangfs.py` (SIGKILL is delivered to T-state) and the kernel `fuse_abort_conn`s the mount on namespace collapse. The drill's `_cleanup_hang` (SIGCONT→`umount -f -l`→`fusermount -u`→kill-9) + nuke are belt-and-suspenders; both idempotent, no double-cleanup race (sequential, `:107` then EXIT trap then nuke).
- **Cross-instance orphans:** each drill runs on its own `INSTANCE=drill-<name>` with unique container/volume/network names (`docker.sh:14-17`), nuked on every exit path including the INT/TERM trap (`simcluster:485`). Background jobs in 60 (J11/J13/J17) are all `wait`ed or killed in-subshell, and reaped by nuke regardless.
- **inotify / container count:** 60/61/62 are N=1 (3 containers each); at 8192 the preflight (`run-drills.sh:100-119`) covers the ~26-container wave-1 peak. FUSE adds no inotify. Only one 62 runs per suite → at most one `fuse.hangfs` connection host-wide; no FUSE over-consumption.
- **Host `/tmp` residue:** 60 writes `/tmp/sim-60-j11.out` / `/tmp/sim-60-follow.out` on the host (nit — drill-unique, overwritten, never a parallel collision); `agentyaml.sh:57` uses `mktemp` (unique, rm'd); `hangfs.log` is in-container.


---

## [8] reviewer-3 — Mandate ①–④ + 保真度

<sub>subagent key `v2:bf912649d7e1869b01abf95d4f9874b3369bea78ff211fd3d1f2d7df2f5e1c9d`</sub>

I have enough grounding to finalize. Let me give the verdict.

## Review findings — Dimension: Mandate ①–④ + fidelity

I verified `agent_provision_yaml` against `scripts/install.sh`, the spawnsafe reason-code logic in `internal/spawnsafe/spawnsafe.go`, the `62` FUSE approximation and its discriminators, `pty-run.py`/`hangfs.py`, the FD-1/OQ-2 ledger entries, and the sim's `cmd_agent_join` unit. Two real defects, one nit.

---

**1. [major] `test/simcluster/drills/62-remote-fs-safe.sh:13,15` — the drill header re-documents the exact false-green the main process already caught, contradicting the assertions, the code, the plan, and the ledger.**

The "Grounded facts (verified live 2026-07-11)" header states:
```
#   exec --cwd <wedged>        → code remote_fs_not_found  (argv[0] unresolvable in the network-safe PATH)
#   exec --safe --cwd <wedged> → code remote_fs_not_found  (per-call escalation past mode:off)
```
But the actual assertions (lines 73-75 and 88-90) check `remote_fs_unsafe_cwd`, and that is the correct code: `Policy.Prepare` (spawnsafe.go:711-713) does the lexical fail-fast `if cwd != "" && p.pathOnDeadMount(cwd) { return &FSError{Code: ReasonUnsafeCwd, ...} }` *before* argv[0] resolution, so a wedged `--cwd` always yields `remote_fs_unsafe_cwd`, never `remote_fs_not_found`. The parenthetical "(argv[0] unresolvable in the network-safe PATH)" is precisely the pre-fix mechanism — the one that fires only when `--cwd` lands *after* the node and `--cwd`/`$MNT` become argv[0]/argv[1], i.e. the false-green the main process fixed. Both `docs/reviews/s1-plan.md:285,298` and the `docs/deploy-tier-gotchas.md` OQ-2 entry (lines 77-78) already carry the correct `remote_fs_unsafe_cwd`; the drill header is the sole stale survivor (grep confirms `remote_fs_not_found` appears *only* in these two header lines across the whole repo). This violates the Mandate's "assert the DOCUMENTED cause" rule: the file now documents a different cause than it asserts, so a maintainer trusting the header could revert the assertion back to the wrong reason and resurrect the false-green.
*Fix*: change lines 13 and 15 to `remote_fs_unsafe_cwd` and rewrite the parentheticals — line 13 to "(lexical fail-fast on the dead cwd, spawnsafe.go:711)" and line 15 to "(per-call --safe escalates past mode:off, still fails the dead-cwd check)".

---

**2. [major] `test/simcluster/drills/lib/agentyaml.sh:94-98` — the ONLINE-poll "honest exposure" safety net is unreliable; it can pass on the OLD process's stale-ONLINE registration, so a failed reprovision (bad yaml / broker_url that didn't take) is not guaranteed to FAIL as the comment claims.**

The helper asserts: "poll the roster until the flagless, yaml-driven daemon parsed the strict-KnownFields yaml and re-registered ONLINE. A parse error … → never ONLINE → this FAILS." But node status is purely heartbeat-timestamp-driven: `internal/node/node.go:6-8,33` (`ONLINE` = last heartbeat within `StaleAfter=5s`), reconciled on a ticker, and the agent's graceful shutdown does only `nc.Drain()` (agent.go:630) with no explicit offline/leave publish. `sctl restart` returns sub-second after spawning the new process, and `poll_until` (lib/log.sh:30-31) tests the predicate *immediately* at t≈0 with no initial sleep. So when a reprovisioned agent crash-loops on a bad config, the OLD process's registration is still `< StaleAfter` old and `node ls | grep ONLINE` matches → the poll returns success on the stale row, before the new process ever registered. The poll checks only "some agent process is ONLINE," which is continuously true across a config-only restart, so it structurally cannot verify the new yaml loaded.
Note this did NOT produce a false-green in the S1 GREEN run — every policy S1 uses (open/disabled/narrow/remotefs:off) is valid, so the new process always comes up — and most arms have downstream behavioral controls (61 Arm E's `refuse transfer_disabled` would catch a stale open-process; 62 Arm1's control exec). But the helper's advertised integrity mechanism is the shared fixture for all of 61/62 and is not functional as documented; a future malformed policy would be masked in any arm lacking a distinguishing downstream probe (e.g. 62's `off` reprovision is not behaviorally distinguished from `auto`).
*Fix*: make the poll prove restart-took-effect, not just liveness: capture the agent PID (`systemctl show -p MainPID` or `pgrep`) before the restart and require the polled MainPID to CHANGE and then be ONLINE, or poll for a transient STALE/OFFLINE→ONLINE edge, or add an explicit post-restart behavioral probe keyed to the new config.

---

**3. [nit] `test/simcluster/drills/62-remote-fs-safe.sh:104-105` — `assert_ok "…NOT-COVERED registered…" true` is a self-declaring GREEN in a signature-guarded harness.** Asserting literal `true` always passes and carries no measurement. It is defensible here because the real evidence lives in the discriminators (`_fuse_stopped` line 70, `_statfs_blocks` line 71) and the comment (102-103) says so, and OQ-2 is genuinely registered in the ledger. But a stronger form would assert the registration is real — e.g. `assert_ok "OQ-2 registered in ledger" sh -c "grep -q 'OQ-2' <path>/docs/deploy-tier-gotchas.md"` — so the marker fails loud if the ledger entry is ever removed. Low priority.

---

**Checked and clean in this dimension:**
- **`agent_provision_yaml` fidelity vs install.sh**: 0600 file, 0700 `agent/<sid>` leaf, `sim:sim`, and a flagless ExecStart are faithful. The flagless unit is meaningful, not compensating: the sim's own `cmd_agent_join` unit carries `--nats-url ${NATS_URL}` (simcluster:355) which would flag-shadow yaml `broker_url` via `pickFlagOrYaml` (agent.go:196), and install.sh's real start command also passes `--session/--nid` but never `--broker`/`--nats-url` (install.sh:371, agent.go:386), so the rewrite reproduces the real yaml-authoritative path rather than papering over it. The narrow-root pre-create (`install -d -o sim`, line 51-53) is correctly justified as real operator provisioning (CanonAllowRoots drops non-existent roots), not a tether workaround.
- **FD-1 is registered, not chowned away**: `image/provision-node.sh:65` bakes `/home/sim/.tether` at 0755 vs install.sh's 0700; `docs/deploy-tier-gotchas.md:63-68` registers this as fidelity debt and explicitly forbids silently chowning the image (Mandate ①). The helper enforces 0700 only on the `agent/<sid>` leaf, consistent with the ledger's "only the top-level dir mode deviates."
- **62 NOT-COVERED honesty**: true-D is genuinely not force-greened — `_fuse_stopped` (T/S-state via `/proc/<pid>/stat` field 3) and `_statfs_blocks` (bounded `timeout 5 stat -f`) are live measurements proving the FUSE daemon is kill-9-reapable, and OQ-2 records the reason. The bounded probe is genuinely bounded (every wedge-touching command is wrapped in external `timeout`; alive-controls at lines 76,91 prove the agent doesn't wedge).
- **`pty-run.py` / `hangfs.py`** stay pure env-supply: pty-run only feeds stdin / reads stdout of the unchanged `tether run` and propagates exit code; hangfs is a passthrough FUSE supplying the "network mount." Neither does tether's job.


---

## [9] reviewer-4 — product-truth（反码 / 退出码 / 列 / flag / 阈值）

<sub>subagent key `v2:0b15d6f5053f80b91e1e29e54f1e9a7f3f32accd7ac864c2e1c20a3896d1dd13`</sub>

I've now traced every reason code, exit-code contract, column layout, flag name, and threshold in the three S1 drills against the actual source. Here are my product-truth findings.

## Findings

**1. [MINOR] `test/simcluster/drills/62-remote-fs-safe.sh:13` and `:15` — the "Grounded facts (verified live 2026-07-11)" header documents the WRONG reason code for both `--cwd` cases; it contradicts the source and the drill's own GREEN assertions.**

The header block claims:
```
#   exec --cwd <wedged>        → code remote_fs_not_found  (argv[0] unresolvable in the network-safe PATH)   ← line 13
#   exec <wedged>/abs-argv0    → code remote_fs_unhealthy  (argv[0] on an unresponsive mount)                 ← line 14 (correct)
#   exec --safe --cwd <wedged> → code remote_fs_not_found  (per-call escalation past mode:off)                ← line 15
```

But the source produces `remote_fs_unsafe_cwd`, not `remote_fs_not_found`, for a wedged `--cwd`. In `internal/spawnsafe/spawnsafe.go:711-713`, `Prepare` does the cwd lexical fail-fast **before** any argv[0] resolution:
```go
if cwd != "" && p.pathOnDeadMount(cwd) {
    return Decision{}, &FSError{Code: ReasonUnsafeCwd, Detail: cwd}   // ReasonUnsafeCwd = "remote_fs_unsafe_cwd" (:656)
}
```
So for `exec --cwd /mnt/hung agt1 -- whoami` (argv=`["whoami"]`, cwd=`/mnt/hung`), the cwd-on-dead-mount check short-circuits and returns `remote_fs_unsafe_cwd`; the argv[0]-resolution path (which is what yields `remote_fs_not_found`) is never reached. `remote_fs_not_found` is structurally impossible for this input.

This is confirmed by the drill's OWN shipped assertions, which correctly assert `remote_fs_unsafe_cwd` and pass GREEN:
- `Arm1a` (`:73-75`): `assert_refuses "...remote_fs_unsafe_cwd..." "remote_fs_unsafe_cwd" RFS --cwd "$MNT" agt1 -- whoami`
- `Arm3` (`:88-90`): `assert_refuses "...remote_fs_unsafe_cwd..." "remote_fs_unsafe_cwd" RFS --safe --cwd "$MNT" agt1 -- whoami`

Since `assert_refuses` grep-guards on the literal `remote_fs_unsafe_cwd`, a GREEN run *proves* the ctl output carried `remote_fs_unsafe_cwd` — which the header's `remote_fs_not_found` directly contradicts. The parenthetical rationale on lines 13/15 ("argv[0] unresolvable in the network-safe PATH") is the mechanism of the **pre-fix flag-after-node bug** that the main process already caught and fixed (`--cwd` being consumed as `argv[0]` → an unresolvable bogus path → `remote_fs_not_found`). The `Grounded facts` header was written from that stale pre-fix observation and never updated, while carrying a misleading "verified live 2026-07-11" freshness stamp.

Why it matters: it's comment-only (does not cause a false-green — the assertions are correct), but it's a wrong "verified" fact in a drill's authoritative doc block, and it would mislead a maintainer into believing the fixed behavior is still `remote_fs_not_found`.

Fix: change lines 13 and 15 to `code remote_fs_unsafe_cwd`, and replace the parenthetical with the real mechanism — "cwd is on an unresponsive mount → lexical cwd fail-fast (spawnsafe.go Prepare, before argv[0] resolution)". Line 14 (`remote_fs_unhealthy` for an absolute argv[0] on the mount) is already correct and matches `spawnsafe.go:716-718` + `Arm1b (:78-80)`.

## What I verified and found CORRECT (no findings)

Grounded against source, all of these drill signatures are accurate:

- **60 exec/exit contracts** (`exec.go:131-138`): J4 exact `7` passthrough; J7 flat `128` on signal-kill (`os.Exit(128)` on `ExitCode<0`, not 128+signo); J7b "terminated by signal" note (emitted by both `internal/agent/exec.go:203-206` and `cmd/tether/exec.go:132-133`); J6 `--cwd` flag-before-node via `SetInterspersed(false)`.
- **60 node ls columns** (`node.go:105,115-116` → `NODE STATUS HEARTBEAT PROTO RELEASE`): J2 regex correctly places HEARTBEAT (col3, non-space), PROTO (col4, int), RELEASE (col5) — accounts for the HEARTBEAT column the comment omits mentioning.
- **60 G.3** (`node.go:51` "no active session"; `internal/node/node.go:33` `DefaultStaleAfter=5s`) — refusal string and the 5s StaleAfter reference are exact.
- **60 ps** (`ps.go:136` STATE before CMD → two-grep is right; `:189` PORTS header always renders; N=1 → no HOME column via `:182-195`).
- **60 history** (`history.go:87-88` "must be one of"; `:562-585` rows are `HH:MM:SS␠␠KIND` timestamp-first, kinds `CALL/PROC/PORT/XFER`; `--kind transfer` renders `kind=` and `path=`, no transfer_id) — the `_HKIND` timestamp anchor and the CALL/PROC/PORT/XFER token set are correct.
- **61 push/pull reason asymmetry** (`internal/agent/transfer.go`): B1 `path_parent_missing` (write, `:750`) vs B2 `path_not_found` (read, `:820`); B3/B4 `not_a_regular_file` (`:772,845`); B5a `dst_exists` (`:1102`); B6 `too_large` (`:361-364`, 3G>2GiB pre-transfer); D-F2/D-F3 `path_outside_roots` (`:763,833`, parent-exists checked before containment per the comment's `:750`-then-`:763`); E1/E2 `transfer_disabled` (`:738,808`, and `disabled` yaml → modeDisabled via `resolveTransferMode:705-712`).
- **61 tier boundary anchored on the real max_payload clamp** (`transfer.go:750-756 chooseTier`, `maxInline = max_payload/2 - 1024`): with NATS default 1 MiB (install.sh does not set `max_payload`), boundary = 523264; C1 (512000→A) and C2 (1 MiB→B, only reachable if the clamp lowered maxInline below 1 MiB since 1 MiB < the 8 MiB static ceiling) are valid discriminators that fail loud (not false-green) if max_payload were larger. Comments' arithmetic is exact.
- **61 refuse_clean io_error negative guard** and **history transfer complete/failed pairing** (agent-reached `path_outside_roots` emits `ev.transfer.failed` at `transfer.go:89` → broker audit) — sound.
- **62 remote_fs assertions themselves** (Arm1a/Arm3 `remote_fs_unsafe_cwd`, Arm1b `remote_fs_unhealthy`) and the `fuse.hangfs` → `kindRemoteProbe` classification (`spawnsafe.go:124`) — correct; wire path FSError→`err.Error()`→ctl `execFailureMessage` preserves the code for the grep (`error_hints.go:188-197`).

All source citations are absolute-pathed under `/home/weiland/projects/dist_experiment_control/`.


---

## [10] reviewer-5 — harness-engineering + robustness

<sub>subagent key `v2:e5045dd98383b97846a08e70a13b06fb917383de7a952db791636952857dcb39`</sub>

I have enough grounded evidence. Here are my findings for the harness-engineering + robustness dimension.

---

**FINDINGS**

**1. [major] `drills/lib/agentyaml.sh:92-98` — the ONLINE poll is NOT a sound guard: it can match the *pre-restart* registration's residual ONLINE and silently pass a failed reprovision.**

The guard's own claim (lines 94-98): *"poll the roster until the flagless, yaml-driven daemon parsed the strict-KnownFields yaml and re-registered ONLINE. A parse error or a broker_url that didn't take → never ONLINE → this FAILS (the honest exposure, not a silent pass)."* This is false in a wide, easily-hit window. I traced the roster model:

- Node status is NOT connection-driven. `Register`/`Heartbeat` force `nodes.status='ONLINE'` (internal/node/node.go:120,157); the flip to STALE/OFFLINE happens only when the broker's periodic reconciler observes `age ≥ StaleAfter` (`ReconcileStates`, node.go:170-217), and `List` (node ls) reads the stored column.
- `StaleAfter = 5s`, `ReconcileInterval = 1s`, agent `HeartbeatInterval = 5s` (node.go:32, broker.go:580, agent.go:478). Graceful agent shutdown does **no** deregister — it only `nc.Drain()`s (agent.go:630); nothing writes OFFLINE.
- So after `sctl restart` sends SIGTERM to the OLD daemon, its `nodes.status` stays `'ONLINE'` until the reconciler sees the heartbeat age reach 5s — i.e. for up to ~5-6s.
- `poll_until` checks **immediately at t=0** before any sleep (lib/log.sh:26-36), and `sctl restart` returns ~1s after SIGTERM. So the first probe lands squarely inside the residual-ONLINE window and returns success — even if the NEW flagless daemon never came up (bad yaml, unreachable broker_url, unit error → `Restart=on-failure` crash-loop). The exact failure the guard exists to catch is masked, and it is masked on the *first* check in the common case, not a rare race.

Why it matters (Mandate ①): this helper is the sole gate for the yaml-provisioning fixture that 61 (all policy arms) and 62 (Arm1/Arm3 boot-with-mount) depend on. Downstream policy asserts catch a *wrong-policy* daemon in many arms, but the guard itself does not verify the new daemon registered, and same-policy reprovisions (e.g. 62 Arm1 `open`, which relies on the restart actually re-scanning with the mount present to set `bootHangable`) can propagate the false pass.

Fix: give the poll a definite fresh-registration edge instead of trusting a bare ONLINE. Concretely, replace `sctl restart` + immediate ONLINE poll with `sctl stop` → `poll_until` the agent leaves ONLINE (guaranteed within ~6s since nothing heartbeats) → `sctl start` → `poll_until` ONLINE. Any ONLINE observed after a confirmed non-ONLINE edge must be the freshly-started daemon, so a parse error fails loud as claimed. (Alternative if `node ls --json` surfaces it: capture pre-restart `boot_id` from the NodeListEntry — it changes every process start — and require it to change.)

**2. [major] `drills/62-remote-fs-safe.sh:13,15` — the "Grounded facts (verified live)" reason-code table contradicts the actual assertions and the product, re-documenting the very false-green the main process just fixed.**

Header lines 13/15 state:
- `exec --cwd <wedged> → code remote_fs_not_found`
- `exec --safe --cwd <wedged> → code remote_fs_not_found`

But the live assertions assert `remote_fs_unsafe_cwd` (lines 73-74 Arm1a, lines 88-89 Arm3), and the product agrees: `Prepare` checks the cwd **first** and returns `ReasonUnsafeCwd` before any argv[0]/PATH resolution (internal/spawnsafe/spawnsafe.go:711-713; `ReasonUnsafeCwd = "remote_fs_unsafe_cwd"` at :656). `remote_fs_not_found` is what the OLD, buggy `--cwd`-after-node form produced (when `$MNT` became argv[0]) — exactly the false-green the fix eliminated. The header even carries the stale rationale (line 13: *"argv[0] unresolvable in the network-safe PATH"*). Only line 14 (`abs-argv0 → remote_fs_unhealthy`) is correct (matches the explicit-dead-path branch spawnsafe.go:716-717 and assertion line 78).

Why it matters: the block is labeled authoritative ("verified live 2026-07-11"). A maintainer trusting it would "correct" the assertion at line 73/88 back to `remote_fs_not_found`, silently reintroducing the fixed false-green. Fix: change lines 13 and 15 to `remote_fs_unsafe_cwd` with the correct rationale (dead-cwd lexical fail-fast, checked before argv[0]).

**3. [minor] `drills/62-remote-fs-safe.sh:38,40` — fixed `sleep` used as a readiness wait, violating the "poll_until, never a fixed sleep" rule (flake hazard).**

`_mount_hangfs` backgrounds the FUSE daemon then `sleep 2; grep -q fuse.hangfs /proc/mounts` (line 38) — the 2s is a readiness wait for the mount to appear. On a loaded weilandserver (concurrent drills per README), fusepy import + mount can exceed 2s → the guarded grep fails → RED flake (loud, not a false-green, but still a spurious failure the rule bans). `_heal` (line 40) likewise `pkill -CONT ...; sleep 1` as a fixed resume wait. Fix: `_mount_hangfs` should `poll_until <t> 1 "fuse.hangfs mounted" -- sh -c "\"$SIM\" exec agt1 -- grep -q fuse.hangfs /proc/mounts"` (drop the sleep); `_heal` should poll on `_statfs_blocks` returning false (statfs healthy) rather than sleeping.

**4. [minor] `image/pty-run.py:181-193` — the drain-idle path falls through to a blocking `os.waitpid(pid, 0)` on a still-alive child, so pty-run's own FAIL-LOUD guarantee is incomplete (it relies entirely on the drill's outer `timeout`).**

If the last step is satisfied but the child stays alive and goes quiet, the drain loop breaks on `idle_deadline` (lines 188-189) with `master >= 0` and the child running, then line 193 `os.waitpid(pid, 0)` blocks forever. `die()` SIGKILLs+reaps on an expect-timeout, but this quiet-but-alive path does not — contradicting the module docstring's promise (lines 13-15) that "a wedged `tether run` never hangs the drill." No current step sequence triggers it (J8 self-terminates; J9-J11 end in `send:exit`), and the `ptyrun()` wrapper's `timeout 70` (60-user-journey.sh:23) is the actual safety net — but that means the guarantee lives in the caller, not pty-run. Fix: on the drain-idle break, SIGKILL+reap the child (mirror `die`) or use `os.waitpid(pid, os.WNOHANG)` in a bounded loop before an unconditional kill, so pty-run fails loud on its own.

**5. [nit] `image/pty-run.py:154-177` — a `send`/`sendraw`/`ctrlc`/`resize`/`expect` step after `eof` crashes on the closed master (fd `-1`).**

`eof` sets `master = -1` (line 175) but does not break the step loop. A subsequent `expect` calls `select.select([-1], …)` (line 105) → `ValueError`; a subsequent `send`/`sendraw`/`ctrlc`/`resize` does `os.write(-1, …)`/`_winsz(-1, …)` → `OSError [Errno 9]` — both uncaught, exiting with a traceback rather than a clean `FAIL(3)`. Latent (no drill uses `eof`). Fix: after `eof`, either `break` or guard the fd-touching verbs with `if master < 0: return die("step after eof", 2)`.

**6. [nit] `drills/62-remote-fs-safe.sh:33` — `_cleanup_hang`'s `pkill -9 -f hangfs.py` self-matches the cleanup wrapper's own `sh -c` cmdline.**

The wrapper is `sh -c "…pkill -9 -f hangfs.py…"`, whose `/proc/self/cmdline` contains `hangfs.py`, so `pkill -f hangfs.py` matches (and SIGKILLs) the wrapper shell in addition to the daemon. Harmless today because it is the terminal command (`; true` is lost but the daemon is already reaped, and the whole line is `… || true`), and container teardown reaps any survivor regardless — but it is fragile: reordering any command after the `pkill -9` would have it killed by its own cleanup. Fix: match a more specific pattern (e.g. `pkill -9 -f '/opt/sim/hangfs.py'` still self-matches; use `pgrep`-by-exact-comm or `pkill -f 'python3 /opt/sim/hangfs.py'` combined with excluding the current shell, or capture the daemon PID at mount time and kill by PID).

---

**Checked and found sound (no finding):** race-free winsize (slave sized at line 84 before `fork`, child does setsid/TIOCSCTTY/dup2 — J8's `stty size` reads 40×132, no fallback race); `expect` per-step `:<secs>` parsing via `rpartition` for every current marker (`$ `, `40 132`, `ROK`, `BACK:8`, `24 80` all parse correctly); cursor-advance echo-vs-output discipline (documented, and every J9-J11 marker is shell-computed, not an echoed keystroke); `resize` correctly TIOCSWINSZ's the master (winsize is a pty-pair property) plus an explicit SIGWINCH; `die()` SIGKILL+reap; exit propagation via `waitstatus_to_exitcode`. In agentyaml.sh: KnownFields fields (`broker_url/session/nid/tunnel_addr/file_transfer.allow_roots/remote_fs.mode`) and the `remote_fs.mode: "off"` quoting (YAML-1.1-false guard) are correct; narrow-dir is created sim-owned **before** restart (lines 51-53, correct vs `CanonAllowRoots` dropping non-existent roots); the unit heredoc's host-side `$_apy_sid/$_apy_agt` expansion is intended and safe. In 62: the trap SIGCONTs before umount with a bounded `timeout 8`, all wedge-touching commands are `timeout`-bounded, and everything is container-scoped so no host wedge survives teardown; `_fuse_stopped`'s `/proc/pid/stat` field-3 read is safe for `comm=python3`.


---

## [11] reviewer-6 — false-green / vacuous-pass 全局猎捕

<sub>subagent key `v2:2c47e19c4b97d63e8826efb5f4c389c8a14df2973829f07e8eb2c1c89d314081`</sub>

I've read all the drills, the assert/agentyaml/log/docker libs, both Python drivers, and the tether source they exercise (transfer.go, exec.go, history.go, ps.go, node.go, login.go, error_hints.go, spawnsafe.go, agent/transfer.go) plus the simcluster wrapper and the command-tree golden test. Grounding every claim in that code, here are the false-green / vacuous-pass findings in my dimension.

---

**1. [major] `61-transfer-edges.sh:79-80` — C1 tier-A assertion greps the pre-transfer banner and discards the push result, so it stays GREEN even if the tier-A transfer is refused.**

C1 is `push /tmp/u.bin agt1:/home/sim/u.bin 2>&1 | grep -q 'tier=a'`. In `cmd/tether/transfer.go:169-171` the `tier=` banner is printed **before** `pushTierA` runs — i.e. before the agent's `ValidateForWrite` verdict and before any refusal (`transfer.go:210-212` returns the refusal *after* the banner). Because the assertion is a pipeline (`push … | grep`), POSIX sh takes the pipeline's exit from `grep`, discarding push's non-zero exit. Result: if u.bin's push were refused for *any* reason (a stray narrow/disabled policy at Arm C, a symlink/`dst_exists`, a permission wall), the client still prints `tier=a` and C1 passes while the transfer never landed. This is exactly the wrong-reason pass the drill's own guard (c) worries about for tier=b — but the author anchored C2 with C2b (`_landed /home/sim/o.bin`, line 85) and C2c (sha round-trip, lines 86-87) and left C1 with **no** landing/result anchor.
**Fix:** mirror C2b — `assert_ok "C1b tier-A file landed" _landed /home/sim/u.bin`, and/or capture push's rc separately (`push … >out 2>&1; rc=$?; grep -q tier=a out && [ $rc -eq 0 ]`) so C1 proves the below-boundary transfer actually completes, not just that the client *chose* tier A.

**2. [minor] `62-remote-fs-safe.sh:13` and `:15` — the "Grounded facts (verified live 2026-07-11)" header states the wrong refusal codes; it contradicts both the shipped assertions and the source, and re-documents the very false-green that was fixed.**

Lines 13/15 claim `exec --cwd <wedged> → remote_fs_not_found` and `exec --safe --cwd <wedged> → remote_fs_not_found`. But Arm1a (lines 73-75) and Arm3 (lines 88-90) assert `remote_fs_unsafe_cwd`, and that is what `spawnsafe.Prepare` actually returns: with the flag parsed **before** the node, `cwd` is set and the lexical cwd check at `internal/spawnsafe/spawnsafe.go:711-713` (`if cwd != "" && p.pathOnDeadMount(cwd) → FSError{Code: ReasonUnsafeCwd}`) fires before argv[0] is ever resolved. `remote_fs_not_found` is the *pre-fix flag-after-node* behavior (argv[0] becomes the literal `--cwd`, unresolved in PATH) — the exact false-green the main process caught and fixed. `assert_refuses` would have failed loudly (RED) if the real code were `remote_fs_not_found`, so the drill being GREEN proves the code is `remote_fs_unsafe_cwd` and the comment is stale. Labeling stale pre-fix behavior "verified live" is a landmine: a future edit trusting the ledger would "correct" the assertion back to `remote_fs_not_found` and re-open the false-green.
**Fix:** change lines 13/15 to `remote_fs_unsafe_cwd`, or explicitly annotate them as the flag-AFTER-node contrast that the flag-BEFORE-node assertions deliberately avoid.

**3. [nit] `60-user-journey.sh:36-40,82` — J5's "ordered HEAD..TAILxyz" ordering is asserted in the description but never actually checked.**

`_j5` greps `HEAD` and `TAILxyz` independently and checks total bytes `> 262144`; it never verifies HEAD *precedes* TAILxyz. A stream that delivered the tail before the head (or interleaved) would still pass, so the "ordered" claim is unverified. In practice a single-subject NATS exec stream (`cmd/tether/exec.go:100-146` writes chunks in arrival order) preserves order, so the real risk is low — but the assertion doesn't test what the description states.
**Fix:** if the ordering claim is meant to be load-bearing, assert it (e.g. confirm the byte offset of `HEAD` is less than that of `TAILxyz`, or drop everything through the first `HEAD` and require `TAILxyz` only after); otherwise drop "ordered" from the description.

---

**Checked and found sound (no false-green) in my dimension:** J4/J7 exact-code asserts (128 comes only from `exec.go:134 os.Exit(128)`; 7 propagates cleanly through `dexec`→`docker exec`); J7b signal note; J6 `--cwd` (grep -qx /tmp genuinely fails if the flag is dropped); J8-J11 PTY markers (all shell-computed, cursor-advancing, echo-immune; J11 baseline + BACK:8 deadline + J11b pgroup teardown form a real oracle); J13a/b ps two-grep (STATE-before-CMD fix is correct); J16a `--kind proc` positive+negative discriminator; J16b `must be one of`; J17 `--follow` (runHistoryFollow uses DeliverNewPolicy — `history.go:125`, no backlog dump, and `>` truncates the host tmpfile so no stale-content pass); J-G.3 logout→refuse→mid-window-stop→re-login (self-guarding negative control; `node.go:50` returns "no active session"); refuse_clean codes B1/B2/B3/B4/B5a/B6/D-F2/D-F3/E1/E2 all match the exact `PathValidationError.Code` strings in `internal/agent/transfer.go` with the `! io_error` guard intact; the OPEN/NARROW/DISABLED arms are behaviorally self-verifying (a stuck-policy reprovision is caught by the next arm's refuse_clean SUCCEEDED-path, not masked by the ONLINE poll); C2/C2b/C2c tier=b is genuinely anchored on the real `max_payload/2-1024` clamp (`transfer.go:750-756`) plus landing; drill-62 wedge refusals require `pathOnDeadMount`→unhealthy (a healthy mount makes them pass through), and the statfs-blocks + T/S-state discriminators are ordered before the deploy asserts; the command-tree golden is a real zero-diff structural gate, not vacuous.


---

## [18] verifier-a — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:a8b326815e5e334f916d42b0290482ea59d623ac7b29e7a44e24759b29b1bb22`</sub>

I've traced the finding against both source files. Verdict below.

## Verdict on Finding 1: CONFIRMED (MINOR, comment-only)

The reviewer is correct. The header block at `test/simcluster/drills/62-remote-fs-safe.sh:13` and `:15` documents `remote_fs_not_found` for the two `--cwd`-on-wedged-mount cases, but the source produces `remote_fs_unsafe_cwd`, and the drill's own GREEN assertions assert `remote_fs_unsafe_cwd`. The header contradicts both.

**Source evidence (re-verified in `internal/spawnsafe/spawnsafe.go`):**
- `Prepare` does the cwd lexical fail-fast at lines 711-713, **before** any argv[0] resolution:
  ```go
  if cwd != "" && p.pathOnDeadMount(cwd) {
      return Decision{}, &FSError{Code: ReasonUnsafeCwd, Detail: cwd}
  }
  ```
- `ReasonUnsafeCwd = "remote_fs_unsafe_cwd"` (line 656). The argv[0]-resolution path that yields `ReasonNotFound = "remote_fs_not_found"` lives downstream in `boundedResolveInDirs` (line 833) and is unreachable once the cwd check short-circuits.
- I traced both drill inputs through `Prepare`'s guards:
  - **Arm1a** `exec --cwd /mnt/hung agt1 -- whoami` (argv=`["whoami"]`, cwd=`/mnt/hung`): mode=auto, bootHangable=true → falls through to the cwd check → `/mnt/hung` is `fuse.hangfs` → `kindRemoteProbe` (classifyFstype prefix `fuse.`, line 124), probes unhealthy under SIGSTOP → `remote_fs_unsafe_cwd`.
  - **Arm3** `exec --safe --cwd /mnt/hung agt1 -- whoami`: mode=off but `requestedSafe=true`, so the `ModeOff && !requestedSafe` early-return (line 690) and the `!bootHangable && !requestedSafe` return (line 693) are both bypassed → same cwd check → `remote_fs_unsafe_cwd`.
  - `remote_fs_not_found` is structurally impossible for either input.

**Drill self-contradiction confirms it:** the shipped assertions grep-guard on the literal `remote_fs_unsafe_cwd` — Arm1a (`:73-75`) and Arm3 (`:88-90`) — so a GREEN run *proves* the ctl output carried `remote_fs_unsafe_cwd`, which the header's `remote_fs_not_found` directly contradicts.

**Root cause of the stale header is exactly as the reviewer diagnosed:** the parentheticals ("argv[0] unresolvable in the network-safe PATH") describe the pre-fix flag-after-node bug the main process already caught (a `--cwd` positioned after the node was consumed as a bogus argv[0] → unresolvable → `remote_fs_not_found`). The header was written from that stale observation and never updated after the flag-before-node fix, while still carrying a "verified live 2026-07-11" freshness stamp.

**Line 14 is correct** (reviewer agrees): `exec <wedged>/abs-argv0 → remote_fs_unhealthy`. Arm1b (`:78-80`) sends `agt1 -- $MNT/probe` (explicit argv[0] with `/`, no cwd) → hits `if explicit && p.pathOnDeadMount(name)` (line 716-718) → `ReasonUnhealthy = "remote_fs_unhealthy"`. Matches.

**Impact assessment (agrees with reviewer's MINOR):** this is comment-only. It does NOT cause a false-green — the three assertions guard on the correct literals and would go RED if the wrong reason code were emitted. It is a wrong "verified" fact in an authoritative doc block that would mislead a maintainer into believing the fixed behavior is still `remote_fs_not_found`. Suggested fix: lines 13 and 15 → `code remote_fs_unsafe_cwd`, and replace the parenthetical with the real mechanism (cwd-on-dead-mount lexical fail-fast in `Prepare`, before argv[0] resolution).

No overreach in the finding: every source citation (`:711-713`, `:656`, `:716-718`) and every drill line citation (`:73-75`, `:78-80`, `:88-90`, `:13/:14/:15`) checks out against the files I read. The reviewer's "verified CORRECT" section is out of scope for this verify pass (only the one finding required a verdict), and I found no reason to dispute the single finding.


---

## [19] verifier-b — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:83a1d7ad99eb89769074d007914898c9241274d150973a487860f5a6989c2358`</sub>

All three findings are grounded in source I can now cross-check. Here are my independent verdicts.

---

**Finding 1 — C1 tier-A assertion has no landing anchor → passes on a refused push. VERDICT: CONFIRMED.**

Re-verified against both the drill and the product source:
- `transfer.go:169-171` prints the `(tier=a, ...)` banner to stdout **before** `pushTierA` is even called (`:173`). `pushTierA` returns the refusal only at `:210-212`, after the banner is already emitted. So the banner text `tier=a` is present regardless of whether the write is accepted.
- `61-transfer-edges.sh:79-80` runs `push … 2>&1 | grep -q 'tier=a'`. In POSIX sh the pipeline's exit status is `grep`'s, so push's non-zero exit is discarded. Any refusal (a stray narrow/disabled policy, `dst_exists`, `not_a_regular_file`, a permission wall) still leaves `tier=a` in the banner → grep matches → C1 GREEN with nothing landed.
- The author demonstrably knew this shape: C2 is anchored by C2b `_landed /home/sim/o.bin` (`:85`) and C2c sha round-trip (`:86-87`), and the file-header guard (c) explicitly worries about a banner-only tier pass. C1 was left with neither anchor — an inconsistency, not a deliberate exemption.

Practical exposure is bounded (policy is OPEN at Arm C, `u.bin` is a fresh path, and A1b independently proves tier-A landing with a 4 KiB file), so this is not currently masking a live bug. But the assertion as written proves only that the *client chose* tier A, not that the below-boundary transfer *completed* — a genuine false-green-prone gap. The reviewer's fix (mirror C2b: `assert_ok "C1b tier-A file landed" _landed /home/sim/u.bin`, or capture push's rc separately) is correct. I'd downgrade the severity label from "major" to **moderate** given the bounded live exposure, but the defect is real.

---

**Finding 2 — 62 header lines 13/15 state the wrong refusal codes (stale pre-fix behavior, contradicting the shipped assertions + source). VERDICT: CONFIRMED.**

- Header `:13` claims `exec --cwd <wedged> → remote_fs_not_found` and `:15` claims `exec --safe --cwd <wedged> → remote_fs_not_found`.
- The shipped assertions say otherwise: Arm1a (`:73-75`) and Arm3 (`:88-90`) both `assert_refuses … "remote_fs_unsafe_cwd"`.
- Source confirms the assertions, not the header: `spawnsafe.go:711-713` — `if cwd != "" && p.pathOnDeadMount(cwd) { return &FSError{Code: ReasonUnsafeCwd …} }`, with `ReasonUnsafeCwd = "remote_fs_unsafe_cwd"` (`:656`). This lexical cwd check fires *before* argv[0] resolution (`:714+`), so with the flag parsed **before** the node (cwd correctly = the wedged mount) the code is unambiguously `remote_fs_unsafe_cwd`. `remote_fs_not_found` (`ReasonNotFound`, `:657`) only comes from `boundedResolveInDirs` at `:833` when a bare name isn't found in PATH — i.e. exactly the pre-fix **flag-after-node** case where the string `--cwd` became argv[0]. The header's own parenthetical "(argv[0] unresolvable in the network-safe PATH)" is the flag-after-node explanation, pinned to a flag-before-node command form — internally inconsistent.

Because `assert_refuses` is signature-guarded, the drill being GREEN on the server *proves* the live code is `remote_fs_unsafe_cwd`, so the "verified live 2026-07-11" header is describing the very false-green the main process caught and fixed. Line `:14` (`… /abs-argv0 → remote_fs_unhealthy`, matched by Arm1b `:78-80` and `spawnsafe.go:716-717`) is correct; only `:13` and `:15` are wrong. This is a real landmine: a future editor trusting the "verified live" ledger could "correct" the assertion back to `remote_fs_not_found` and reopen the false-green. Fix (change `:13`/`:15` to `remote_fs_unsafe_cwd`, or annotate as the flag-AFTER-node contrast) is correct. Severity **minor** is fair — it's a comment, not executable logic.

---

**Finding 3 — J5 "ordered HEAD..TAILxyz" is asserted in the description but never checked. VERDICT: CONFIRMED (nit).**

`_j5` (`60-user-journey.sh:36-40`) does `grep -q HEAD`, `grep -q TAILxyz` (two independent presence checks — `grep -q` tests membership, not position) and `[ "$_j5_n" -gt 262144 ]` (a total byte count). None of these verifies HEAD *precedes* TAILxyz; a stream delivering the tail first, or interleaved, would still pass. The line `:82` description claims "ordered HEAD..TAILxyz". The reviewer's own mitigation is accurate: a single-subject NATS exec stream (`exec.go` writes chunks in arrival order) preserves order in practice, so the real risk is low — but the assertion genuinely does not test the ordering it advertises. Fix (assert HEAD's byte-offset < TAILxyz's, or drop "ordered" from the description) is correct. Nit severity is right.

---

**Summary: 3/3 findings CONFIRMED as real** (none refuted, none needing owner adjudication). Finding 1 is a real unanchored false-green with bounded live exposure (I'd call it moderate, not major); Finding 2 is a confirmed stale/wrong header comment contradicting the source and the shipped assertions — the highest-value catch because "verified live" invites a future regression; Finding 3 is a confirmed but low-risk description-vs-assertion mismatch. All three fixes proposed by the reviewer are sound.


---

## [20] verifier-c — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:f651be7bef71e2fa021abf8d70a9b048a750940f5bda097cf19b0c717ef75d50`</sub>

Verification complete. All claims independently re-checked against source.

===== VERDICT ON coverage-ledger FINDING #1 =====

**CONFIRMED (minor)** — `simcluster-coverage-inventory.md:234` null-diff rationale is factually wrong for `login`, contradicting the inventory's own §1.1.

Independent corroboration (every claim re-grounded in source, not trusted from the reviewer):

1. **Line 234 literally lists `login`** in the "不发 sys.events kind" set: *"用户面命令（exec/run/ps/history/node ls/**login**/push/pull）**不发** `sys.events` kind"*. Confirmed verbatim.

2. **§1.1 line 27** attributes `member_joined` to `internal/authcallout/handler.go`'s `h.emit`, explicitly noting **"PIN 首连两路径"** (two PIN first-connect paths), owned by S2-80. Confirmed verbatim.

3. **Code confirms the ctl emission** the reviewer cited:
   - `internal/broker/authcallout.go:81` — `EmitEvent: b.pubSysEvent, // P7: member_joined / pin_failed → events stream` (verbatim).
   - `internal/authcallout/handler.go:353` — `h.emit("member_joined", {"sid","fp","via":"pin","role":"ctl"})` fires on a successful ctl PIN join; `:348` emits `pin_failed` on `ErrInvalidPIN`. My grep confirms the two PIN-bootstrap emit sites = `:353` (role=ctl) + `:286` (role=agent), which is exactly the "PIN 首连两路径" of §1.1.

4. **The drill actually traverses it.** `60-user-journey.sh:51` runs `"$SIM" session "$SID" --pin "$PIN"`, and `cmd_session` (simcluster:305-319) expands to `tether session create <name> --pin` **plus** `tether login -s <name> --pin` — a genuine ctl PIN first-connect over auth_callout. Since the ctl fp is not yet a member of the freshly-created session, `ensureMember` takes the PIN-bootstrap branch (`handler.go:332-353`) and emits `member_joined`. So 60's login path **does** cross a `pubSysEvent` emission.

This is an internal contradiction: line 234 says "login 不发 sys.events" while §1.1 + the source + the drill's own login usage all say a ctl PIN first-connect emits `member_joined`. The reviewer's severity call is also correct: the **tick conclusion (0 event rows) still holds** — 60 *exercises* `member_joined` but never *asserts* on it (assertion deferred to S2-80), so it's a false rationale in the NULL-DIFF gate writeup, **not a mis-tick and not a false-green in the drill**. Given the gate's whole purpose is precision about which events are traversed, an "emits nothing" claim that's false for the very command the drill drives is a legitimate defect in a shipped S1 artifact. The reviewer's proposed fix (move `login` out of the "不发" list; state it emits `member_joined` via `authcallout.go:81`→`handler.go:353` but is exercised-not-asserted, deferred to S2-80) is accurate.

Note (not a new finding, adversarial caveat): a *steady-state* re-login by an already-member ctl (e.g. `60:66` after logout, since logout is ctl-local and doesn't drop broker-side membership) takes the `if member` branch at `handler.go:324` and emits nothing. So "login never emits" is only wrong for the *first* PIN connect — but 60 performs exactly that first connect, so the contradiction is real for S1 as shipped.

===== ON THE REVIEWER'S "NO OTHER FINDINGS" SECTION =====

These are the reviewer's negative confirmations (non-defects), not findings, so not in scope to fully re-derive. Cheap spot-checks I ran corroborate them and show no fabricated evidence: `command_tree_golden.txt` = **94 paths / 8 `[hidden]` / 11 `--yes(hidden)`** (exact match); the `agent` command carries no `--upgrade-url-allow` flag (DOC-3 basis holds). No over-cleared item surfaced in the spot-check.

Net: 1 finding verified → **1 CONFIRMED (minor doc-rationale imprecision), 0 REFUTED, 0 NEEDS-OWNER.**


---

## [21] verifier-d — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:7f76bf68d2cb55d661537b66a02724414642869d2d55c18154bcf149c148d926`</sub>

All claims verified against source. Here are my adversarial verdicts.

---

## Finding 1 — [MAJOR] "`-j 6` family-wave cap" is unsound → **CONFIRMED**

Every load-bearing claim checks out against source:

- **Pure count throttle, zero family awareness** — `run-drills.sh:144` is literally `while [ "$(jobs -rp | wc -l)" -ge "$JOBS" ]; do sleep 2; done`. No drill/family metadata anywhere in the launch loop (`:142-148`). Confirmed.
- **Glob/alpha launch order** — discovery `for f in "$HERE"/drills/*.sh` (`:85`) + launch in `DRILLS[]` order (`:142`) yields exactly `00,10,11,12,13,20,21,60,61,62` (verified by running the glob). Confirmed.
- **Wave-1 under `-j 6` = 5 concurrent grows + skeleton.** The 5 heavy multi-broker grow drills all sort first: `10-grow-to-3` (grow brk2+brk3), `11-grow-gaps` (`up --brokers 3`+grow brk2), `13-inbroker-reconcile-perm` (grow brk2+brk3), **and 12/20** — which my first grep missed because they form their cluster via `setup_forcesingle_n2` in `drills/lib/setup-forcesingle.sh:8-10` (`up --brokers 2` + `init` + **`grow brk2`**, same VOTER-promotion window). So `-j 6`'s first 6-wide burst = `00,10,11,12,13,20` = **all 5 grows at once** + the N=1 skeleton. The 4 genuinely-light N=1 drills (`21`+`60/61/62`, all `up --brokers 1`) are **deferred to wave 2** — the opposite of the stated goal. Confirmed.
- **`-j 6` cannot bound grows below 5** (5 grow drills, 6 > 5) → it delivers *zero* reduction in peak grow concurrency vs. full-parallel, which is exactly the scenario the CAVEAT (`README.md:225-228`, mirrored in `simcluster:223-230`) says blows the 150s window. Confirmed.
- **README text is objectively false.** `README.md:230-234` calls `-j 6` "a family-wave cap … keep them [grow/force-single] off a single all-at-once burst." It does the reverse. Confirmed.
- **VOTER-timeout evades auto-retry** — `simcluster:215` `poll_until 150 3 "$_j reaches VOTER" …` emits (via `log.sh:33`) `poll_until: timed out after 150s waiting for: brkN reaches VOTER`; `FLAKE_SIG` (`run-drills.sh:57`) only matches `…waiting for: systemd responsive`, so no match → not auto-re-run. Confirmed.

**One fairness caveat for the owner** (does not weaken the finding): the *non-retry* of VOTER-timeouts is documented-intentional (`simcluster:223-230`, README OQ-8 "never auto-swallowed"). And this is a **documentation/policy defect, not a false-GREEN** — a VOTER-timeout still REDs correctly. But the two facts *compound*: `-j 6` maximizes the very VOTER-timeouts that then require manual single re-runs, and the README promises a scheduling guarantee the code does not provide. Under the Mandate ("never make the harness/tether look more capable than it is"), that misdescription is a real defect. The reviewer's proposed fix (schedule the grow family in its own serial/`-j 2` pass, light family at full parallel) is the correct remedy.

## Finding 2 — [MINOR] fixed `sleep 2` for the FUSE mount → **CONFIRMED**

`62-remote-fs-safe.sh:38` is `… setsid python3 …/hangfs.py $MNT … & sleep 2; grep -q fuse.hangfs /proc/mounts` — a fixed sleep followed by a *single* check, which is precisely the anti-pattern the repo's own `poll_until` (`log.sh:26`, "Replaces every fixed sleep") exists to eliminate. It is **fail-closed** (too-short → grep fails → `assert_ok` at `:55` REDs; no false-GREEN), so MINOR is the right severity. The proposed poll_until fix is sound, though it requires splitting the launch (fire-and-forget `setsid … &`) from a host-side `poll_until … -- "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts`. Corroboration: the same drill also fixed-sleeps at `_heal()` (`:40` `sleep 1`) and `:39`/`:45`-region settles — the discipline slip is not isolated. Real, low-impact.

## Finding 3 — [MINOR] alive-controls not wrapped in `timeout` → **CONFIRMED**

- `:65`, `:76`, `:91` are bare `assert_ok "…" "$SIM" ctl -- exec agt1 -- true`, with **no** `timeout`, while the wedge-probe helper `RFS()` (`:44`) is `timeout 25 …`. Confirmed.
- The header (`:5-6`) makes a **blanket** claim: "Every wedge-touching command is bounded by an external `timeout`." Confirmed the claim exists.
- `tether exec` default is `--timeout 10m` (`cmd/tether/exec.go:152` `DurationVar(&timeout, "timeout", 10*time.Minute, …)`, applied at `:94` `context.WithTimeout`). Confirmed.
- `assert_ok` imposes no timeout of its own (`assert.sh:17-19` `_as_capture` just runs `"$@"`), and neither does the sim `ctl`/`exec` wrapper. So the only bound on the alive-control is tether's 10m default. Confirmed.

Consequence is exactly as stated: under a *genuine* agent-wedge (the failure mode the drill exists to surface), the very command meant to *detect* the wedge would hang up to 10m — holding a `run-drills.sh` job slot and deferring the EXIT-trap `_cleanup_hang` (`:35`) + nuke (`simcluster` teardown) that reaps the FUSE mount. It's **fail-closed** (a slow RED, never a false-GREEN) and only bites when a real wedge is present (the measured premise is "no wedge, reapable T/S FUSE"), so MINOR is correct.

**Scoping nuance for the owner:** whether `exec agt1 -- true` is "wedge-touching" is arguable — it doesn't touch the `$MNT` path. But (a) the header's wording is unconditional, and (b) the command's whole role is wedge-*detection*, so a detector that can hang 10m under the condition it detects is a genuine header/code inconsistency. I land on CONFIRMED; a reasonable owner could downgrade to "accept given the no-wedge premise," but the fix is trivial (`timeout 25` wrapper) and matches the drill's own standard, so there's no reason not to.

---

**Reviewer's "no finding" section:** spot-checked and consistent with source — `docker.sh` runs `--privileged` without shared bind-propagation (no host mount leak), per-drill `INSTANCE=drill-<name>` isolation with nuke on every exit path, and 60/61/62 are all N=1 (3 containers each) so no inotify pressure. Nothing there flips to a finding.


---

## [22] verifier-e — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:107c21ce2320fb253371a8ec83ed67e2fc8b553c7392ea314bde2d1ac33ce604`</sub>

All three findings are verified against source. No sim override for the state-machine thresholds (defaults 5s/5s/1s hold), and Arm3's off-vs-auto indistinguishability is confirmed. Final verdicts below.

---

**VERIFY VERDICTS — mandate-fidelity dimension (3 findings)**

**Finding 1 — header documents `remote_fs_not_found` while assertions check `remote_fs_unsafe_cwd` — CONFIRMED (real defect; comment-only).**
- Header `62-remote-fs-safe.sh:13,15` literally state `→ code remote_fs_not_found` for `exec --cwd <wedged>` and `exec --safe --cwd <wedged>`.
- Assertions at `73-75` (Arm1a) and `88-90` (Arm3) both check `"remote_fs_unsafe_cwd"`.
- Source proves the assertions are the correct code: `spawnsafe.go:711-712` does the lexical cwd fail-fast `if cwd != "" && p.pathOnDeadMount(cwd) { return &FSError{Code: ReasonUnsafeCwd} }` *before* argv[0] resolution; `ReasonUnsafeCwd = "remote_fs_unsafe_cwd"` (656). With flag-before-node (`RFS --cwd $MNT agt1 -- whoami`), cwd=$MNT and argv[0]=`whoami`, so the cwd branch always fires → `remote_fs_unsafe_cwd`. `ReasonNotFound = "remote_fs_not_found"` fires only in `boundedResolveInDirs` (833) when argv[0] can't be resolved — and `error_hints.go:172` glosses it as exactly the header's parenthetical "argv[0] was not found in the network-safe PATH." That is the pre-fix false-green mechanism (the `--cwd`-became-argv[0] case the main process already fixed). So the header re-documents the wrong cause.
- Note line 14 (`remote_fs_unhealthy`) IS correct (matches Arm1b:78-80 + spawnsafe 716-717). Only 13 and 15 are stale.
- One reviewer overclaim to flag: their "grep confirms `remote_fs_not_found` appears only in these two header lines across the whole repo" is false — it appears in 8 places repo-wide (spawnsafe.go×2, error_hints.go, 4 docs/reviews). The accurate statement is "only within the simcluster drill tree." The core contradiction stands regardless.
- Calibration: this is comment-only — the assertions themselves are correct, so there is no live false-green today. "Major" is defensible on the Mandate's "assert the DOCUMENTED cause" + future-maintainer-revert risk, but a case for "minor" exists. Fix as proposed (change 13/15 to `remote_fs_unsafe_cwd` + rewrite parentheticals).

**Finding 2 — ONLINE-poll integrity net can pass on a stale-ONLINE row — CONFIRMED (structural / latent, no S1 false-green).**
All four load-bearing mechanics verified independently:
1. `poll_until` tests the predicate immediately at t≈0 with no initial sleep (`log.sh:30-31`, `while :; do if "$@"; then return 0`).
2. `node ls` returns the STORED status column verbatim — `handleNodeListReq → node.List` (`exec.go:229`); `List` reads `s.Status` without recomputing from age (`node.go:286-315`). Status flips to STALE only when the reconcile ticker runs.
3. No offline/leave publish on shutdown (`internal/agent/agent.go:630` = `defer nc.Drain()` + a proxy-drain barrier, nothing else), and NO disconnect/last-will/$SYS→OFFLINE hook in the broker — every OFFLINE writer is the heartbeat-age reconcile ticker. So a killed old process leaves its row ONLINE.
4. Defaults confirmed unoverridden in the sim: HeartbeatInterval=5s (`agent.go:477-478`) == StaleAfter=5s (`node.go:33`), ReconcileInterval=1s (`broker.go:579-580`).
Consequence: `systemctl restart` returns after the old process exits and the new one execs but before the new one connects+registers (~1-3s of NATS+register latency). At that instant the old row is still within its ≤5s ONLINE window, so `node ls | grep ONLINE` matches the stale row and the poll returns success at t≈0 — before the new yaml ever loaded. A crash-looping reprovision (bad config) is therefore NOT guaranteed to FAIL, contradicting the helper's advertised "parse error … → never ONLINE → this FAILS" (`agentyaml.sh:94-96`). The reviewer's caveats are accurate: no actual S1 false-green (all policies valid) and most arms have downstream controls — but I independently confirmed the Arm3 corroboration: with `--safe`, `spawnsafe.go:690-692` (`mode==ModeOff && !requestedSafe → inert`) is bypassed in BOTH off and auto, then both hit the same cwd check → `remote_fs_unsafe_cwd`, so `62` Arm3's `off`-reprovision produces an identical result whether the agent is on off or auto. That arm genuinely relies on the (unreliable) poll to prove the config took effect, and the poll can't. Minor reviewer citation slip: `agent.go:630` is `internal/agent/agent.go`, not `cmd/tether/agent.go`. Fix direction (require MainPID to change / boot_id change / a config-keyed behavioral probe) is sound; note a "poll for a STALE→ONLINE edge" variant is itself flaky because HB==StaleAfter can hide the STALE edge, so MainPID-change is the robust choice.

**Finding 3 — `assert_ok "…NOT-COVERED registered…" true` is a self-declaring GREEN — CONFIRMED (nit, genuinely low-impact).**
`62-remote-fs-safe.sh:104-105` calls `assert_ok … true`; `true` always exits 0 and `assert_ok` only checks `_AS_RC = 0` (`assert.sh:23-28`), so it passes unconditionally and measures nothing. Factually accurate. It is defensible exactly as the reviewer says: the real evidence is the live discriminators `_fuse_stopped` (70) and `_statfs_blocks` (71), which are genuine measurements, and OQ-2 is registered in the ledger. So this is a cosmetic marker, not a false-green. The suggested hardening (`grep -q 'OQ-2' docs/deploy-tier-gotchas.md`) would make it fail-loud if the ledger entry is deleted and is a reasonable optional improvement; leaving it is also acceptable.

**Summary: 3/3 CONFIRMED** — Finding 1 (comment contradiction, comment-only, fix the two header lines), Finding 2 (structural weakness in the shared reprovision integrity net, no live S1 false-green but the advertised guarantee is unsound and Arm3 relies on it), Finding 3 (harmless no-op assert nit). No finding refuted; none needs-owner beyond the ordinary severity-calibration call on 1 and 2.


---

## [23] verifier-f — 对抗核验（恒等映射，独立复核源码）

<sub>subagent key `v2:ab57cad72b1db6dc2af1660153d62ce7ca783cf984646b5bd3d1b0fc5d14c8d4`</sub>

All six findings independently re-checked against source. Verdicts below.

===== VERIFICATION VERDICTS (harness-robustness reviewer) =====

**Finding 1 [major] — ONLINE poll can match residual pre-restart ONLINE → guard's self-description is false.** VERDICT: **CONFIRMED** (mechanism real; severity nuance flagged for owner).

Every load-bearing fact checks out against source:
- Node status is heartbeat-driven only, never connection-driven. `Register`/`Heartbeat` force `status='ONLINE'` (internal/node/node.go:120,157); STALE/OFFLINE flips happen solely in the age-based `ReconcileStates` (node.go:175-222). Grep for any connection-close → OFFLINE / deregister path found none in internal/broker or internal/agent.
- Thresholds: `StaleAfter=5s` (broker.go:583 → node.DefaultStaleAfter, node.go:33), `ReconcileInterval=1s` (broker.go:580), agent `HeartbeatInterval=5s` (agent.go:478). Graceful shutdown is `defer nc.Drain()` only, no OFFLINE write (agent.go:630).
- `poll_until` runs the probe at t=0 *before* the first sleep (lib/log.sh:30-31), and `sctl restart` = `systemctl restart` (tether.sh:17) on a `Type=simple` unit (agentyaml.sh:81), which returns at fork — so the restart's own exit check (agentyaml.sh:92) does NOT catch a crash-on-startup, and the first ONLINE probe lands ~1-2s after SIGTERM, inside the up-to-~6s residual window. Since the old daemon's last heartbeat age at SIGTERM is uniform in [0,5s), the residual ONLINE is present at the first probe in the large majority of failed-reprovision runs — "masked on the first check in the common case," exactly as stated. The comment's claim (agentyaml.sh:94-98) that "a parse error or a broker_url that didn't take → never ONLINE → this FAILS" is therefore provably false.

Severity nuance for the owner: I traced the actual blast radius. All six call sites (61:57,91,102,106; 62:63,85) are followed by a policy/exec assert that needs a *live* daemon (e.g. 61 A1 push, D-F1 push, E1 `transfer_disabled` refuse_clean; 62 Arm1 control `exec agt1 -- true` + the Arm1a `remote_fs_unsafe_cwd` assert that itself proves `bootHangable`). Because `restart` kills the old daemon, a failed reprovision leaves NO daemon → those downstream asserts go RED (a *mislocated* but loud failure, not a silent GREEN). So the demonstrated end-to-end false-GREEN risk in the current drills is bounded; the defect is genuinely "the reusable guard is unsound and its self-description is false," which is a real Mandate-① "green for the wrong reason" hazard at the helper level (and would bite any future arm with a weaker downstream assert). The reviewer's fix (stop → poll non-ONLINE edge → start → poll ONLINE; or require a `boot_id` delta) is sound. I'd place this at the minor/major boundary rather than clean major, but the finding is real.

**Finding 2 [major] — header reason-code table (62:13,15) contradicts the live assertions and the product, re-documenting the just-fixed false-green.** VERDICT: **CONFIRMED** (strong; real regression risk).

Verified directly. 62-remote-fs-safe.sh:13 says `exec --cwd <wedged> → remote_fs_not_found` and :15 says `exec --safe --cwd <wedged> → remote_fs_not_found`, but the live assertions assert `remote_fs_unsafe_cwd` (62:73-74 Arm1a, 62:88-89 Arm3), and the product agrees: `Prepare` checks the cwd FIRST and returns `ReasonUnsafeCwd` before any argv[0]/PATH resolution (spawnsafe.go:711-713; `ReasonUnsafeCwd = "remote_fs_unsafe_cwd"` at :656). Only 62:14 (`abs-argv0 → remote_fs_unhealthy`) is correct (spawnsafe.go:716-717). The header's rationale on :13 ("argv[0] unresolvable in the network-safe PATH") is doubly wrong: `whoami` IS resolvable, and the cwd short-circuits before argv[0] anyway — that rationale is precisely the OLD `--cwd`-after-node bug (where `$MNT` became argv[0] → `remote_fs_not_found`), i.e. the exact false-green the main process fixed. The block is stamped "Grounded facts (verified live 2026-07-11)," so a maintainer trusting it and "correcting" 62:73/88 back to `remote_fs_not_found` would silently reintroduce the fixed false-green. Fix as stated: change :13 and :15 to `remote_fs_unsafe_cwd` (dead-cwd lexical fail-fast, checked before argv[0]).

**Finding 3 [minor] — fixed `sleep` as readiness/resume wait at 62:38,40 violates the "poll_until, never a fixed sleep" rule.** VERDICT: **CONFIRMED** (rule violation + flake hazard).

62:38 `_mount_hangfs` backgrounds the FUSE daemon then `sleep 2; grep -q fuse.hangfs /proc/mounts` — the 2s is a readiness wait; under concurrent-drill load a slow fusepy import/mount overruns it → the guarded grep fails → spurious RED (loud, not a false-green, but a banned flake). 62:40 `_heal` uses `pkill -CONT ...; sleep 1` as a fixed resume wait. Both contradict the repo discipline (`poll_until` "Replaces every fixed sleep," log.sh:24-25). `_mount_hangfs` is the stronger case (genuine readiness wait that can flake); `_heal`'s impact is marginal (SIGCONT is instant and Arm3 re-wedges immediately after), but it is still a rule violation. Fix (poll on the mount appearing / on statfs-healthy) is correct.

**Finding 4 [minor] — drain-idle path falls to a blocking `os.waitpid(pid,0)` on a still-alive quiet child; pty-run's own fail-loud is incomplete.** VERDICT: **CONFIRMED** (latent, low severity; one framing nuance).

Verified: the post-step drain loop (pty-run.py:180-189) can break on `idle_deadline` with `master >= 0` and the child alive, then :193 `os.waitpid(pid, 0)` blocks indefinitely; `die()` (118-128) SIGKILLs on an expect-timeout but this path does not. Latent — no current step sequence reaches it: J8 runs `sh -c 'stty size'` which self-terminates (60:94), J9-J11 end in `send:exit` (60:98,106,116), so every current invocation ends with the pty closing (pump→None→break→waitpid returns). The outer `timeout 70` in `ptyrun()` (60:23) is the real net. Nuance: the docstring already *mandates* that outer watchdog (pty-run.py:15, "The DRILL must additionally wrap the whole invocation in `timeout <N>s`"), so "the guarantee lives in the caller" is partly by-design delegation rather than a pure oversight — but mirroring `die()`'s kill+reap on the drain-idle break to make pty-run self-contained is a legitimate, correct hardening. Real code path, correctly [minor].

**Finding 5 [nit] — a fd-touching step after `eof` crashes on the closed master (fd -1).** VERDICT: **CONFIRMED** (latent nit).

Verified: `eof` sets `master = -1` without breaking the step loop (pty-run.py:173-175). A subsequent `expect` pumps → `select.select([-1],…)` (:105) → `ValueError`; a subsequent `send`/`sendraw`/`ctrlc`/`resize` → `os.write(-1,…)`/`_winsz(-1,…)` → `OSError [Errno 9]`; both uncaught → traceback + non-clean exit instead of `FAIL(3)`. Confirmed latent: grep across drills/ and image/ shows no `eof` step is used anywhere; `eof`-as-last-step is also safe (drain loop guarded by `master >= 0`). Correct [nit]; the guard suggestion is sound.

**Finding 6 [nit] — `_cleanup_hang`'s `pkill -9 -f hangfs.py` self-matches the wrapper's own `sh -c` cmdline.** VERDICT: **CONFIRMED** (harmless today, fragile).

Verified: 62:33 runs the cleanup inside `sh -c "…pkill -9 -f hangfs.py…; true"`; that shell's `/proc/PID/cmdline` contains the literal `hangfs.py`, and `pkill -f` matches it. `pkill` excludes only its own PID, not the parent shell, so the wrapper is SIGKILLed alongside the daemon. Harmless today (the `pkill -9` is the terminal real command — the trailing `; true` is simply lost, the daemon is already reaped, the whole line is `… || true`, and container teardown reaps any survivor), but fragile: any command reordered after the `pkill -9` would be killed by it. Note the direct-form helpers (`_wedge`/`_heal`, 62:39-40) are NOT affected — there the `pkill` process's *own* cmdline matches but pkill excludes self; the finding correctly isolates the `sh -c`-wrapper-parent case. Correct [nit]; kill-by-captured-PID or a `sh -c`-excluding pattern fixes it.

===== SUMMARY =====
6/6 CONFIRMED, 0 REFUTED, 0 NEEDS-OWNER. Findings 2 (false reason-code doc re-introducing the fixed false-green) and 1 (unsound reprovision guard with a false self-description) are the substantive ones; 3-6 are correctly-graded minors/nits, all latent or bounded in the current drills. Two calibration notes for the main process: (a) Finding 1's demonstrated end-to-end false-GREEN blast radius is bounded by downstream live-daemon asserts, so it reads more minor/major-boundary than clean major — but the guard IS unsound and its comment IS false; (b) Finding 4's "guarantee lives in the caller" is partly by-design per the docstring's explicit outer-timeout mandate. No reviewer claim was overstated to the point of being wrong.


---

## [25] synth — 综合（CONFIRMED findings 汇总，most-severe-first）

<sub>subagent key `v2:cdc4d38a4bb59a48ff2ae8fecd1ee4f8bc9cfc716f1378d40773e6b255873ead`</sub>

All findings are grounded against source. spawnsafe reason codes confirmed (`ReasonUnsafeCwd="remote_fs_unsafe_cwd"` at :656, cwd check at :711-712 before argv[0]); README:231 "-j 6 (a family-wave cap)" confirmed; coverage-inventory:234 lists `login` in the "不发 sys.events kind" set; pty-run.py `eof` sets `master=-1` without break and the terminal `os.waitpid(pid,0)` is unconditional. No dimension produced a REFUTED verdict — all 6 verifiers confirmed. Here is the synthesized report.

---

# S1 batch — adversarial code review (synthesis)

*Consolidates 6 expert reviews + 6 independent verifications across dimensions: false-green-hunter, product-truth, mandate-fidelity, coverage-ledger, harness-robustness, concurrency-hostsafety. Every finding below was CONFIRMED by its verifier and re-grounded in source by the synthesizer. No finding was REFUTED. IDs are stable for the main process to reply against.*

## 0. Verdict summary

**12 CONFIRMED findings** (deduplicated from 20 raw finding-instances; the 62 header appeared in 4 dimensions, the ONLINE-poll and the 62 sleep each in 2).

| Severity | Count | IDs |
|---|---|---|
| major | 3 | S1-02, S1-01, S1-04 |
| moderate | 1 | S1-03 |
| minor | 4 | S1-05, S1-06, S1-07, S1-08 |
| nit | 4 | S1-09, S1-10, S1-11, S1-12 |

**No live false-green in the S1 GREEN run.** Every substantive finding is either comment-only (S1-01), a *latent* unsoundness bounded by downstream live-daemon asserts (S1-02), a bounded-exposure unanchored assert (S1-03), or a harness scheduling/doc defect (S1-04). None is currently masking a real tether bug — but S1-01, S1-02 and S1-03 are each a mechanism by which a *future* edit would silently re-open a false-green, which is exactly the class this review exists to kill. NEEDS-OWNER items are all severity-calibration adjudications (§2); nothing needs owner input on *whether* it's a defect.

---

## 1. CONFIRMED findings (most-severe first)

### S1-02 — [major · robustness/mandate · latent false-green generator] `test/simcluster/drills/lib/agentyaml.sh:92-98` — the ONLINE re-registration poll is unsound; it can pass on the OLD process's residual-ONLINE row, so a failed reprovision is NOT guaranteed to fail as the comment claims.

**Defect.** The helper's self-description (`:94-98`) is: *"poll the roster until the flagless, yaml-driven daemon parsed the strict-KnownFields yaml and re-registered ONLINE. A parse error or a broker_url that didn't take → never ONLINE → this FAILS (the honest exposure, not a silent pass)."* This is false in a wide, first-check window. Node status is heartbeat-timestamp-driven, never connection-driven:

- `Register`/`Heartbeat` force `status='ONLINE'`; the flip to STALE/OFFLINE happens **only** in the age-based reconcile ticker (`internal/node/node.go` `ReconcileStates`), and `node ls` reads the stored column.
- Thresholds unoverridden in the sim: `StaleAfter=5s` (`node.go:33`), `ReconcileInterval=1s` (`broker.go:580`), agent `HeartbeatInterval=5s` (`agent.go:478`). Graceful shutdown is `defer nc.Drain()` only (`internal/agent/agent.go:630`) — no OFFLINE/leave publish, and no broker-side disconnect→OFFLINE hook.
- `sctl restart` (`agentyaml.sh:92`) = `systemctl restart` on a `Type=simple` unit → returns at fork, so the restart's own rc-check (`:92`) does not catch a crash-on-startup. `poll_until` probes at **t≈0** before its first sleep (`lib/log.sh:30-31`).

So after SIGTERM to the old daemon, its row stays ONLINE for up to ~5-6s. The first probe lands inside that residual window and returns success **even if the new flagless daemon never came up** (bad yaml, unreachable `broker_url`, unit error → `Restart=on-failure` crash-loop). The poll verifies only "some agent process is ONLINE," which is continuously true across a config-only restart, so it structurally cannot prove the new yaml loaded.

**Evidence / blast radius.** This is the sole gate for the yaml-provisioning fixture that **all of 61 (every policy arm) and 62 (Arm1/Arm3)** depend on. No S1 false-green today: every S1 policy (open/disabled/narrow/remotefs:off) is valid so the new daemon always comes up, and `restart` kills the old daemon → a failed reprovision leaves *no* daemon → downstream asserts that need a live daemon (61 A1 push, E1 `transfer_disabled` refuse; 62 Arm1 control `exec agt1 -- true`) go RED (a mislocated-but-loud failure). But the reusable guard is unsound and its comment is a false guarantee; a future arm with a weaker downstream probe — notably **62 Arm3, where `off`+`--safe` is behaviorally indistinguishable from `auto`+`--safe` (both bypass the `ModeOff && !requestedSafe` early-return and hit the same cwd check → `remote_fs_unsafe_cwd`)** — relies on this poll to prove the config took effect, and it can't.

**Suggested fix.** Make the poll prove *restart-took-effect*, not bare liveness: capture the pre-restart `MainPID` (`systemctl show -p MainPID`) or `boot_id`, and require the polled row to be ONLINE **with a changed PID/boot_id**; or `stop → poll non-ONLINE edge → start → poll ONLINE`. (Note: a plain "poll for a STALE→ONLINE edge" is itself flaky since `HeartbeatInterval==StaleAfter` can hide the STALE edge — MainPID-change is the robust choice.)

---

### S1-01 — [major · mandate/false-green · comment-only] `test/simcluster/drills/62-remote-fs-safe.sh:13,15` — the "Grounded facts (verified live 2026-07-11)" header documents the WRONG reason code, contradicting the shipped assertions and the source, and re-documents the exact false-green the main process already fixed.

*(Reported independently by 4 of 6 dimensions — the highest-consensus catch.)*

**Defect.** Header `:13` states `exec --cwd <wedged> → code remote_fs_not_found  (argv[0] unresolvable in the network-safe PATH)` and `:15` states `exec --safe --cwd <wedged> → code remote_fs_not_found`. But the shipped assertions Arm1a (`:73-75`) and Arm3 (`:88-90`) both `assert_refuses … "remote_fs_unsafe_cwd"`, and the source agrees: `spawnsafe.go:711-712` does the lexical cwd fail-fast `if cwd != "" && p.pathOnDeadMount(cwd) { return &FSError{Code: ReasonUnsafeCwd} }` **before** any argv[0] resolution (`ReasonUnsafeCwd = "remote_fs_unsafe_cwd"`, `:656`). With the flag parsed before the node (`RFS --cwd $MNT agt1 -- whoami`), cwd=`$MNT` and argv[0]=`whoami`, so the cwd branch always fires → `remote_fs_unsafe_cwd`. `remote_fs_not_found` (`ReasonNotFound`, `:657`) comes only from `boundedResolveInDirs` (`:833`) when a bare argv[0] isn't in PATH — i.e. the **pre-fix flag-after-node** case where the string `--cwd` became argv[0]. The parenthetical "(argv[0] unresolvable in the network-safe PATH)" is that pre-fix mechanism, pinned to a flag-before-node command form — internally inconsistent, and doubly wrong since `whoami` *is* resolvable. Line `:14` (`abs-argv0 → remote_fs_unhealthy`) IS correct (matches Arm1b `:78-80` + `spawnsafe.go:716-717` explicit-argv[0]-on-dead-mount branch).

**Evidence.** `assert_refuses` is signature-guarded, so the drill being GREEN on the server *proves* the ctl output carried `remote_fs_unsafe_cwd` — which the header's `remote_fs_not_found` directly contradicts. Comment-only: no live false-green (the executable assertions are correct). The hazard is the "verified live" freshness stamp: a future editor trusting the ledger could "correct" `:73`/`:88` back to `remote_fs_not_found` and silently reopen the fixed false-green — the precise Mandate "assert the DOCUMENTED cause" violation.

**Suggested fix.** Change `:13` and `:15` to `code remote_fs_unsafe_cwd`, and replace the parenthetical with the real mechanism: "cwd is on an unresponsive mount → lexical cwd fail-fast (`spawnsafe.go` `Prepare`, before argv[0] resolution)"; for `:15` add "per-call `--safe` escalates past `mode:off`, still fails the dead-cwd check."

**Verifier caveat (correct one reviewer overclaim):** the mandate-fidelity reviewer wrote "grep confirms `remote_fs_not_found` appears only in these two header lines across the whole repo" — that is **false**; it appears in ~8 places repo-wide (`spawnsafe.go`×2, `error_hints.go`, and several `docs/reviews/*`). The accurate statement is "only within the simcluster drill tree." The core contradiction stands regardless; do not act on the "only these two lines" claim.

---

### S1-04 — [major · mandate/doc · harness misdescription] `test/simcluster/README.md:230-234` — the OQ-8 "`-j 6` family-wave cap" is not a family cap; it MAXIMIZES concurrent grows instead of separating families, and the resulting VOTER-timeouts evade the flake auto-retry.

**Defect.** `README.md:231` calls `./run-drills.sh -j 6` "a family-wave cap" that keeps the grow/force-single families "off a single all-at-once burst." But `-j` is a pure global count throttle with zero family awareness (`run-drills.sh:144` `while [ "$(jobs -rp|wc -l)" -ge "$JOBS" ]`), and drills launch in glob/alpha order (`:85` discovery, `:142` launch) = `00,10,11,12,13,20,21,60,61,62`. The first 6-wide wave is `00,10,11,12,13,20` = **five cluster-forming grow/force-single drills at once**: `10`/`11`/`13` grow to N=3, and `12`/`20` form via `setup_forcesingle_n2` (`drills/lib/setup-forcesingle.sh` `up --brokers 2 + init + grow brk2`, same VOTER-promotion window). Because all 5 grow-family drills sort *before* the light N=1 user-plane family (`21`/`60`/`61`/`62`), `-j 6` front-loads every heavy grow into wave 1 and defers the cheap drills — the opposite of the stated goal. `-j 6` cannot bound concurrent grows below 5 (5 grows, 6 > 5), i.e. zero reduction in peak grow concurrency vs full-parallel — exactly the scenario the CAVEAT (`README.md:225-228`) says blows the 150s VOTER window.

**Evidence.** A VOTER-promotion timeout emits (via `log.sh:33`) `poll_until: timed out after 150s waiting for: <j> reaches VOTER` (`simcluster:215`), which does **not** match `FLAKE_SIG` (`run-drills.sh:57`, only `…waiting for: systemd responsive`) → never auto-re-run. So `-j 6` maximizes the very VOTER-timeouts that then need manual single re-runs, defeating the "one green wrap run" purpose. Mandate violation: the README promises a scheduling *safety guarantee the code does not provide* — the harness is described as more capable than it is. (Not a false-GREEN: a VOTER-timeout still REDs correctly.)

**Suggested fix.** Schedule by family, not by a count: run the grow/force-single family (`1x`/`2x`) in its own serial-or-`-j 2` pass and the N=1 user-plane family (`00/21/60/61/62`) at full parallel in a separate pass; or interleave the launch order so ≤2 grow drills are ever in flight. And stop describing `-j 6` as a family-wave cap in `README.md` — it isn't one. (Owner note in §2: the VOTER non-retry is documented-intentional; the fix may be README-only, re-scheduling, or both.)

---

### S1-03 — [moderate · false-green] `test/simcluster/drills/61-transfer-edges.sh:79-80` — the C1 tier-A assertion greps the pre-transfer banner and discards the push result, so it stays GREEN even if the tier-A transfer is refused.

**Defect.** C1 is `sh -c "… push /tmp/u.bin agt1:/home/sim/u.bin 2>&1 | grep -q 'tier=a'"`. In `cmd/tether/transfer.go:169-171` the `tier=` banner prints **before** `pushTierA` runs (`:173`), and the refusal returns only at `:210-212` — after the banner is already emitted. Because the assertion is a pipeline, POSIX sh takes the exit from `grep`, discarding push's non-zero rc. So any refusal (a stray narrow/disabled policy, `dst_exists`, `not_a_regular_file`, a permission wall) still leaves `tier=a` in the banner → C1 GREEN with nothing landed. This is the exact banner-only-tier hazard guard (c) worries about — the author anchored C2 with C2b `_landed /home/sim/o.bin` (`:85`) and C2c sha round-trip (`:86-87`) but left C1 with **neither** anchor.

**Evidence.** Live exposure is bounded (policy is OPEN at Arm C, `u.bin` is a fresh path, and A1b independently proves tier-A landing with a 4 KiB file), so it is not masking a live bug today. But as written the assertion proves only that the *client chose* tier A, not that the below-boundary transfer *completed*.

**Suggested fix.** Mirror C2b: add `assert_ok "C1b tier-A file landed" _landed /home/sim/u.bin`, and/or capture push's rc separately (`push … >out 2>&1; rc=$?; grep -q tier=a out && [ $rc -eq 0 ]`).

---

### S1-05 — [minor · robustness] `test/simcluster/drills/62-remote-fs-safe.sh:38,40` — fixed `sleep` used as a readiness/resume wait, violating the "poll_until, never a fixed sleep" rule (flake hazard).

*(Reported by 2 dimensions.)* `_mount_hangfs` (`:38`) backgrounds the FUSE daemon then `sleep 2; grep -q fuse.hangfs /proc/mounts` — a fixed readiness wait for the mount. Under concurrent-drill load on weilandserver (fusepy import + mount, and drill 62 sits at wave-2 position while wave-1 grows peak CPU) 2s can be too short → the "probe: mount fuse.hangfs (healthy)" assert (`:55`) goes RED. Fail-closed (loud RED, not a false-green), but a banned flake. `_heal` (`:40`) likewise `pkill -CONT …; sleep 1` as a fixed resume wait (lower impact — SIGCONT is instant). **Fix:** `poll_until <t> 1 "hangfs mounted" -- "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts` (drop the sleep); `_heal` polls `_statfs_blocks` returning false.

### S1-06 — [minor · robustness/mandate] `test/simcluster/drills/62-remote-fs-safe.sh:65,76,91` — the alive-controls are NOT wrapped in `timeout`, contradicting the drill's own boundedness invariant.

The header (`:5-6`) asserts "Every wedge-touching command is bounded by an external `timeout`," and the probe helper `RFS()` (`:44`) is `timeout 25 …`. But the three alive-control asserts `"$SIM" ctl -- exec agt1 -- true` (`:65`, `:76`, `:91`) — whose whole purpose is to detect an agent wedge right after `_wedge` — have no outer `timeout`, and `tether exec` defaults to `--timeout 10m` (`cmd/tether/exec.go:152`). So on a *real* agent wedge (the failure the drill exists to surface) the detector hangs up to 10 minutes, holding a `run-drills.sh` slot and deferring the `_cleanup_hang`/nuke that reaps the FUSE mount. Fail-closed, bites only under a genuine wedge (measured premise is "no wedge"), hence minor. **Fix:** `timeout 25 "$SIM" ctl -- exec agt1 -- true`. *(Owner scoping note in §2: whether `exec … -- true` is "wedge-touching" is arguable — but the header wording is unconditional and the command's role is wedge-detection.)*

### S1-07 — [minor · robustness] `test/simcluster/image/pty-run.py:~180-193` — the drain-idle path falls through to a blocking `os.waitpid(pid, 0)` on a still-alive child, so pty-run's own FAIL-LOUD guarantee is incomplete.

If the last step is satisfied but the child stays alive and goes quiet, the drain loop breaks on `idle_deadline` with `master >= 0` and the child running, then the terminal `os.waitpid(pid, 0)` blocks forever. `die()` SIGKILLs+reaps on an expect-timeout, but this quiet-but-alive path does not — contradicting the docstring's "a wedged `tether run` never hangs the drill." Latent: no current step sequence reaches it (J8 self-terminates; J9-J11 end in `send:exit`), and the `ptyrun()` wrapper's `timeout 70` (`60-user-journey.sh:23`) is the real net — but the guarantee then lives in the caller, not pty-run. *(Docstring already mandates the outer watchdog, so this is partly by-design delegation.)* **Fix:** on the drain-idle break, SIGKILL+reap the child (mirror `die`) or use `WNOHANG` in a bounded loop before an unconditional kill.

### S1-08 — [minor · coverage/doc] `docs/reviews/simcluster-coverage-inventory.md:234` — the NULL-DIFF writeup falsely claims `login` emits no `sys.events` kind, contradicting the inventory's own §1.1.

Line 234 lists `login` in *"用户面命令（exec/run/ps/history/node ls/login/push/pull）不发 `sys.events` kind"*. But a ctl PIN first-connect emits `member_joined`: `internal/broker/authcallout.go:81` (`EmitEvent: b.pubSysEvent, // P7: member_joined / pin_failed`) → `internal/authcallout/handler.go:353` (`h.emit("member_joined", {…,"via":"pin","role":"ctl"})`). `60-user-journey.sh:51` does exactly this (`session … --pin` = create + ctl PIN login), so 60's login path traverses a `pubSysEvent` emission — and §1.1 (line 27) already attributes `member_joined` to that `h.emit` path. The **tick conclusion (0 event rows) still holds** — 60 *exercises* but does not *assert* `member_joined` (assertion deferred to S2-80) — so this is a false rationale in the null-diff gate, not a mis-tick and not a drill false-green. **Fix:** remove `login` from the "不发" list; state it emits `member_joined` (`authcallout.go:81`→`handler.go:353`) but is exercised-not-asserted, deferred to S2-80. *(Adversarial caveat: a steady-state re-login by an already-member ctl (`handler.go:324` `if member` branch) emits nothing — but 60 performs a genuine first PIN connect, so the contradiction is real for S1.)*

### S1-09 — [nit · false-green] `test/simcluster/drills/60-user-journey.sh:36-40,82` — J5's "ordered HEAD..TAILxyz" ordering is asserted in the description but never checked.

`_j5` greps `HEAD` and `TAILxyz` independently (`grep -q` = membership, not position) and checks total bytes `> 262144`; nothing verifies HEAD *precedes* TAILxyz. A stream delivering the tail first, or interleaved, would still pass. Real risk is low (single-subject NATS exec stream preserves arrival order), but the assertion doesn't test what the `:82` description claims. **Fix:** assert HEAD's byte-offset < TAILxyz's, or drop "ordered" from the description.

### S1-10 — [nit · mandate] `test/simcluster/drills/62-remote-fs-safe.sh:104-105` — `assert_ok "…NOT-COVERED registered…" true` is a self-declaring GREEN.

`true` always exits 0; `assert_ok` only checks rc=0, so it passes unconditionally and measures nothing. Defensible today — the real evidence is the live discriminators `_fuse_stopped` (`:70`) and `_statfs_blocks` (`:71`), and OQ-2 is genuinely in the ledger. **Fix (optional hardening):** `assert_ok "OQ-2 registered in ledger" sh -c "grep -q 'OQ-2' <repo>/docs/deploy-tier-gotchas.md"` so the marker fails loud if the ledger entry is ever deleted.

### S1-11 — [nit · robustness] `test/simcluster/image/pty-run.py:~173-175` — a fd-touching step after `eof` crashes on the closed master (fd -1).

`eof` sets `master = -1` without breaking the step loop. A subsequent `expect` → `select.select([-1],…)` → `ValueError`; a subsequent `send`/`sendraw`/`ctrlc`/`resize` → `os.write(-1,…)`/`_winsz(-1,…)` → `OSError [Errno 9]` — both uncaught → traceback instead of a clean `FAIL(3)`. Latent (no drill uses `eof`; `eof`-as-last-step is safe). **Fix:** after `eof`, `break`, or guard the fd-touching verbs with `if master < 0: return die("step after eof", 2)`.

### S1-12 — [nit · robustness] `test/simcluster/drills/62-remote-fs-safe.sh:33` — `_cleanup_hang`'s `pkill -9 -f hangfs.py` self-matches the wrapper's own `sh -c` cmdline.

The cleanup runs inside `sh -c "…pkill -9 -f hangfs.py…; true"`; that shell's `/proc/self/cmdline` contains `hangfs.py`, so `pkill -f` matches (and SIGKILLs) the wrapper shell alongside the daemon (`pkill` excludes only its own PID, not the parent). Harmless today (it's the terminal real command, daemon already reaped, whole line is `… || true`, container teardown reaps survivors) but fragile: reordering any command after the `pkill -9` would have it killed by its own cleanup. The direct-form `_wedge`/`_heal` (`:39-40`) are NOT affected (there pkill's own cmdline matches but pkill excludes self). **Fix:** capture the daemon PID at mount time and kill by PID, or use a `sh -c`-excluding pattern.

---

## 2. NEEDS-OWNER (severity-calibration / policy adjudications)

No verifier returned a NEEDS-OWNER verdict — all 12 findings are CONFIRMED defects. These are open *calibration* calls only:

- **N-1 (S1-01 severity):** major vs minor. Reviewers split 2/2. It is comment-only (assertions correct today) → argues minor; it re-documents the exact fixed false-green under a "verified live" stamp → argues major on the Mandate "documented cause" rule. Synthesizer leans **major** on the future-revert hazard + 4-dim consensus.
- **N-2 (S1-02 severity):** major vs minor/major-boundary. The end-to-end false-GREEN blast radius is bounded because all six call sites are followed by a live-daemon assert (a failed reprovision → no daemon → downstream RED). The *guard itself* is unsound and its comment is a false guarantee. Synthesizer leans **major** at the fixture level.
- **N-3 (S1-03 severity):** major vs moderate. Bounded live exposure (OPEN policy, fresh path, A1b independently proves tier-A landing) argues moderate; the unanchored-assert shape argues major. Synthesizer set **moderate**.
- **N-4 (S1-06 scoping):** is `exec agt1 -- true` "wedge-touching"? It doesn't touch `$MNT`, so an owner could accept it under the no-wedge premise. But the header wording is unconditional and the command's role is wedge-detection. Fix is trivial (`timeout 25`) and matches the drill's own standard.
- **N-5 (S1-04 remedy scope):** the VOTER-timeout non-retry is documented-intentional (OQ-8, "never auto-swallowed"). Owner decides: fix the README claim only, re-schedule the grow family (serial/`-j 2` pass), or both. Synthesizer recommends both — the misdescription and the front-loading are independent defects.

---

## 3. Suggested NEW test assertions (experts may add; main process integrates)

1. **S1-03 anchor:** `assert_ok "C1b tier-A file landed" _landed /home/sim/u.bin` after C1 (or capture push rc separately). Proves the below-boundary transfer *completed*, not just that the client chose tier A.
2. **S1-02 restart-took-effect gate:** in `agent_provision_yaml`, capture pre-restart `MainPID` (`systemctl show -p MainPID tether-agent`) and change the poll predicate to "ONLINE **and** MainPID changed"; a crash-looping bad-config reprovision then fails loud as the comment promises. Add a *negative self-test* arm: provision a deliberately malformed yaml once and assert the helper RETURNS NON-ZERO (proves the guard actually catches a failed reprovision).
3. **S1-09 ordering:** in `_j5`, assert the byte-offset of `HEAD` < the byte-offset of `TAILxyz` (e.g. `awk`/`grep -b`), or drop "ordered."
4. **S1-05 readiness poll:** replace `62:38` `sleep 2; grep` with `poll_until <t> 1 "hangfs mounted" -- "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts`; replace `_heal`'s `sleep 1` with a poll on `_statfs_blocks` returning false (statfs healthy).
5. **S1-06 boundedness:** wrap the three alive-controls in `timeout 25` so a genuine agent wedge REDs fast instead of stalling 10m.
6. **S1-10 ledger-real marker:** replace `assert_ok "…NOT-COVERED registered…" true` with `grep -q 'OQ-2' docs/deploy-tier-gotchas.md` so the marker fails loud if OQ-2 is ever removed.
7. **S1-07 / S1-11 pty-run self-containment:** on the drain-idle break, SIGKILL+reap the child (mirror `die`); after `eof`, `break` or guard fd-touching verbs — so pty-run fails loud on its own rather than relying on the caller's `timeout 70`.

---

## 4. Considered-and-dismissed appendix

**No finding was REFUTED** in any of the 6 dimensions. This appendix records the reviewers' *negative confirmations* (things adversarially probed and found sound) so the main process sees they were checked and need no action:

- **60 exit-code contracts:** J4 exact `7` passthrough; J7 flat `128` on signal-kill (`exec.go:131-135` `os.Exit(128)` on `ExitCode<0`, not `128+signo`) + J7b "terminated by signal" note. Correct.
- **60 flag-before-node:** J6 `--cwd` via `SetInterspersed(false)` genuinely fails (`grep -qx /tmp`) if the flag is dropped. Correct.
- **60 PTY oracles:** J8-J11 markers all shell-computed, cursor-advancing, echo-immune; J11 baseline + `BACK:8` deadline + J11b pgroup teardown form a real oracle; race-free winsize (slave sized before `fork`); per-step `:<secs>` expect timeouts parse for every marker; `resize` TIOCSWINSZ's the master. Sound.
- **60 ps / history / G.3:** J13a/b ps two-grep (STATE-before-CMD, `ps.go:136`); J16a `--kind proc` positive+negative discriminator + J16b "must be one of"; J17 `--follow` uses `DeliverNewPolicy` (`history.go:125`, no backlog dump) and `>` truncates the host tmpfile (no stale-content pass); J-G.3 logout→refuse→mid-window-stop→re-login is a self-guarding negative control (`node.go:51` "no active session"). Sound.
- **61 reason codes:** B1 `path_parent_missing`, B2 `path_not_found`, B3/B4 `not_a_regular_file`, B5a `dst_exists`, B6 `too_large` (>2 GiB pre-transfer), D-F2/D-F3 `path_outside_roots` (parent-exists before containment), E1/E2 `transfer_disabled` — all match the exact `PathValidationError.Code` strings in `internal/agent/transfer.go`, each guarded by `! io_error`. Sound.
- **61 tier boundary:** C2/C2b/C2c tier=b genuinely anchored on the real `max_payload/2 - 1024` clamp (1 MiB < 8 MiB static ceiling) *plus* landing + sha round-trip; fails loud if `max_payload` were larger. Narrow-dir pre-create justified (CanonAllowRoots drops non-existent roots = real operator provisioning). OPEN/NARROW/DISABLED arms behaviorally self-verifying with reversibility control (E3/E4). Sound.
- **agentyaml fidelity (apart from S1-02):** 0600 file, 0700 `agent/<sid>` leaf, `sim:sim`, flagless unit are faithful to `install.sh`; the flagless unit is meaningful not compensating (the sim's own `cmd_agent_join` unit carries `--nats-url` which would flag-shadow yaml `broker_url`, so the rewrite reproduces the real yaml-authoritative path). KnownFields set + `remote_fs.mode: "off"` quoting (YAML-1.1-false guard) correct. Sound.
- **FD-1 fidelity debt:** the image bakes `/home/sim/.tether` at 0755 vs install.sh's 0700 and this is *registered* as fidelity debt (not silently chowned) — Mandate ① honored. Sound.
- **62 NOT-COVERED honesty:** true-D + mode:off-without-safe are not force-greened; `_fuse_stopped` (T/S via `/proc/<pid>/stat` field 3) and `_statfs_blocks` (bounded `timeout 5 stat -f`) are live measurements proving the FUSE daemon is kill-9-reapable; every wedge-touching *probe* is `timeout`-bounded; alive-controls prove no agent wedge. Sound (S1-06/S1-10 are the only slips here).
- **pty-run.py / hangfs.py:** stay pure env-supply — pty-run only drives stdin/reads stdout of the unchanged `tether run` and propagates exit code; hangfs is a passthrough FUSE. Neither does tether's job. Sound.
- **command-tree golden:** 94 paths / 8 `[hidden]` / 11 `--yes(hidden)`, deterministic (dedup + sort), real zero-diff structural gate; `agent` correctly lacks `--upgrade-url-allow`, `serve` correctly has it. Sound.
- **ledger accounting:** #I1-folded (drill 11 head-note + `assert_refuses`), DOC-3-confirmed (`error_hints.go:34` + `usage.md:1443` point at a non-existent agent `--upgrade-url-allow`), DOC-5-non-defect (flat-128 contract documented), #25-empty defensible — all accurate. README drill-11 5-site drift fully settled (`GREW-VIA-TETHER-CLUSTER-ADD` everywhere; the only `GREW-VIA-WORKAROUNDS` strings assert the old trailer is ABSENT).
- **Host-safety under parallel `-j`:** no FUSE host-mount leak (`--privileged` without shared bind-propagation → in-container `/mnt/hung` never propagates; `docker rm -f` SIGKILLs the SIGSTOP'd daemon; kernel `fuse_abort_conn` on namespace collapse); per-drill `INSTANCE=drill-<name>` isolation with nuke on every exit path (incl. INT/TERM trap); 60/61/62 all N=1 (3 containers each) so no inotify pressure; host `/tmp` residue is drill-unique/overwritten (nit only). Sound.

*(Note — not a finding: `61:116-120` carries a self-documented conditional "verify on server: only AGENT-reached refusals write a start+failed audit pair; if no failed row, register as an observability finding, NOT force-greened." It went GREEN, so the failed row exists; the conditional resolved correctly and needs no action.)*
