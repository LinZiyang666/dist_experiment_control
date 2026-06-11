# Code Review — remote-fs-resilience (v0.3.3 leaf increment)

> Adversarial internal review of the IMPLEMENTED, all-gates-green feature.
> Reviewers: 5 expert dimensions (correctness/un-hang · concurrency/resource · wire/compat ·
> inertness/no-regression · config/cli-ux/test-quality), each finding adjudicated by an
> adversarial verdict pass (confirmed / uncertain / refuted). Every confirmed claim below was
> spot-checked against the actual source at the cited file:line.

## 1. Summary verdict

**NOT shippable to external review as-is.** The core un-hang mechanism for the *spawn* path
(exec/run/pty) is sound — the `&exec.Cmd{Path,Args}` struct-literal genuinely bypasses
`LookPath`, `sanitizePATH` drops a deterministically-dead mount before any stat walks it, the
fail-fast reason codes are correct, and the residual execve hang is bounded by the `RunStart`
watchdog. The wire change is clean (additive `Safe omitempty`, ProtoVersion stays 1, round-trips
the broker re-marshal on both verbs).

But **Component I (the agent's own `state.json` read) is not actually bounded**, and two distinct
blocker-class defects defeat the lifeline the feature exists to provide on the exact NFS-Home
target install:

| Severity | Count | IDs |
|---|---|---|
| **blocker** | 2 | `statestore-mutex-poison-unbounded-load`, `replayports-unbounded-read` |
| **major** | 8 | `abandon-start-session-race` / `pty-session-unsynchronized-start-vs-close` (same defect), `all-healthy-hangable-not-byte-identical` / `active-healthy-path-rewrite` / `inert-guarantee-not-test-pinned-healthy-hangable` (same defect, 3 frames), `active-healthy-pwd-divergence`, `exec-abandon-pipe-fd-leak`, `missing-concurrency-leak-test-entry` |
| **minor** | 7 | `relative-argv0-no-failfast`, `resolveindirs-toctou-unbounded`, `exec-fserror-no-ctl-hint`, `missing-agentyaml-remotefs-config-tests` / `remotefs-yaml-block-untested` (same gap), `leak-gate-missing-on-blocking-probe-tests`, `safe-ordering-not-in-long-help`, `auto-nolocal-procfs-read-per-spawn`, `off-mode-inert-not-pinned-at-handler`, `mounthealthy-joiner-latency-amplification` |
| **nit** | 3 | `safe-wire-additive-verified` (positive), `yaml-downgrade-skew-undocumented`, `usage-doc-wrong-section-xref` |
| **uncertain** | 1 | `autofs-drops-healthy-path-dir` (downgraded from major) |

The two blockers and the inertness/PWD/PATH regressions must be fixed before external review.
The fd-leak and the pty/run abandon race are real `-race`/resource defects the project's
discipline is meant to catch but no test exercises.

---

## 2. CONFIRMED findings — what the main process must act on

### BLOCKER

#### B1. `state.json` read holds `s.mu` across the wedging `os.ReadFile`; one abandoned bounded read poisons ALL state I/O
- **Where:** `internal/agent/state.go:69-88` (`load`/`loadLocked`), `internal/agent/agent.go:685-712` (`boundedLoad`)
- **Claim (verified at source):** `stateStore.load()` takes `s.mu.Lock()` with a deferred unlock (state.go:70-71) and calls `loadLocked()` which does `os.ReadFile(s.path)` (state.go:76) **while still holding `s.mu`**. `boundedLoad` (agent.go:700) runs `load()` in a goroutine and, on timeout (agent.go:708-710), returns degraded **without releasing the abandoned goroutine** — which is now parked in D-state inside `os.ReadFile` **holding `s.mu` forever**.
- **Impact:** Every subsequent `s.mu.Lock()` blocks indefinitely. (a) The next reconnect's `loadStateBounded` spawns a fresh goroutine that parks on `s.mu.Lock()` → **one wedged D-goroutine accumulated per reconnect** (the exact O(reconnects) accumulation §3.I claims to eliminate). (b) The WRITE paths `AddPort`/`RemovePort`/`SetProxy` (state.go:94, and via `expose.go`/`proxy.go`) are **not bounded at all** and D-hang permanently on `s.mu.Lock()`, silently corrupting port persistence on recovery. On the NFS-Home agent this feature targets, a single wedged read makes the Component-I bound hollow — the agent still wedges, one syscall later.
- **Why CI misses it:** `TestBoundedLoad` (remotefs_test.go) passes an inline closure as `load`, never the real `stateStore.load`, so the mutex is never exercised — the test passes **vacuously** w.r.t. this concern.
- **Fix:** Bound the lock acquisition, not just the call site. Add a bounded `loadState` API on `stateStore` that does the `os.ReadFile` **without holding `s.mu`** (the load is read-only; serialize against writers via a `TryLock`-with-deadline or a separate read path), so an abandoned read does not retain the mutex.
- **New test:** a `stateStore` whose `ReadFile` blocks on a test channel; assert that after **one** abandoned bounded read, (1) a second bounded read still completes/abandons within the deadline, (2) an `AddPort` still completes within the deadline (currently hangs unbounded), and (3) goroutine count does not grow per repeated reconnect.

#### B2. `replayPortsFromState` reads `state.json` UNBOUNDED, wedging the whole `Run()` loop before heartbeats start
- **Where:** `internal/agent/agent.go:490-508` (`replayPortsFromState`), called at `agent.go:481`
- **Claim (verified at source):** `replayPortsFromState` calls `a.stateStore.load()` **directly** at agent.go:494 — never through `loadStateBounded`. The two *other* boot reads ARE bounded (`buildLocalSnapshot` via `loadStateBounded` at agent.go:737; `applyReconciliation` at agent.go:772). This read site is the lone unguarded one. It runs at agent.go:481 inside `runRegisteredSession`, **after** register + `applyReconciliation` (474) but **before** `heartbeatLoop` (487).
- **Impact:** On a fresh boot with a wedged hangable Home, the two bounded reads degrade-and-continue (good), then `replayPortsFromState` hits the unbounded `load()` and **D-hangs the entire `Run()` goroutine** — so `heartbeatLoop` never starts and the node goes STALE→OFFLINE→port-revoke. This is the **opposite** of the plan §3.I.3 claim that New()-ordering makes a hangable-Home agent "degrade at boot instead of wedging"; New()'s reorder only ensures the classifier *exists*, it does not bound this read. It also holds `s.mu` (B1), poisoning everything else. (Scope: boot-only — `onNATSReconnect` does not call `replayPortsFromState` — but boot/restart is the exact recovery path the feature exists for.)
- **Fix:** Route `replayPortsFromState`'s read through `loadStateBounded` (or `boundedLoad` with `a.homeHangable`); on a degraded read, log a warn and **skip replay** (proxies re-establish on the next healthy reconnect via the same token path) rather than wedging.
- **New test:** in-process agent with Home marked hangable + a blocking `stateStore` read seam; assert `Run()` reaches `heartbeatLoop` within the deadline instead of hanging at `replayPortsFromState`.

> **Note on B1+B2 together:** fixing B2 to call `loadStateBounded` is necessary but **not sufficient** — without B1, the *first* abandoned read still poisons `s.mu` and the subsequent writes hang. Both must be fixed.

---

### MAJOR

#### M1. Abandoned `sess.Start` races `sess.Close()` on the unsynchronized `Session` (run spawn-timeout + recovery) — data race + slave-fd double-close
- **Where:** `internal/pty/pty.go:34-39` (no mutex), `pty.go:95-111` (Start writes `s.slave`/`s.cmd`), `pty.go:183-194` (Close nils/reads `s.Master`/`s.slave`); `internal/agent/run.go` spawn-timeout branch calls `sess.Close()` (run.go:195); `internal/spawnsafe/spawnsafe.go:679-697` (`RunStart` abandons the goroutine on `ErrSpawnTimeout`)
- **Claim (verified at source):** `Session` has no mutex over `Master`/`slave`/`cmd`. `run.go` wraps `sess.Start` in `RunStart`; on `ErrSpawnTimeout` it **abandons** the Start goroutine (parked at `cmd.Start()`, pty.go:102) and then calls `_ = sess.Close()`. If the wedged execve later returns after the mount recovers, the resumed abandoned Start writes `s.slave.Close(); s.slave=nil; s.cmd=cmd` (pty.go:109-111) **concurrently** with `Close()` reading+niling `s.Master`/`s.slave` (pty.go:189-194) — an unsynchronized read/write data race **and** a double-close of the slave fd (which may by then be reused elsewhere in the process). This is the **same class** the authors just fixed in `pty_test.go` (drain goroutine vs `Close` niling `s.Master`, fixed with a captured handle + join) but left unsynchronized on the abandon path. Only `ErrSpawnTimeout` is affected — `ErrTooManyWedged` returns before launching `start`. exec's abandon is race-free because its `*exec.Cmd` is local to `runChild` and untouched after timeout.
- **Impact:** Latent data race + fd double-close whenever a TOCTOU mount death causes a run spawn-timeout and the mount later recovers. `-race` would flag it; no test exercises abandon-then-recover + Close, so it is invisible to the gate.
- **Fix:** Either (a) guard `Session` field access (`s.cmd`/`s.slave`/`s.Master`) with a `sync.Mutex`, or (b) on `ErrSpawnTimeout` do **not** call `sess.Close()` — leave the Session to the abandoned goroutine to self-reap (matching exec's "abandon and own your own fds" semantics) and leak-bound it via `RunStart`'s wedge counter only.
- **New test (`-race`):** inject a start func wrapping a real `sess.Start` whose execve is delayed past the deadline then released; on abandon call `Close()`, then release Start; assert no race and no double-close.

> `abandon-start-session-race` and `pty-session-unsynchronized-start-vs-close` are the same defect from two reviewers; act on it once.

#### M2. Active-on-healthy-hangable is NOT byte-identical: PATH rewritten + explicit env + self-resolved argv[0], on the production target machine
- **Where:** `internal/spawnsafe/spawnsafe.go:544-548` (Prepare only short-circuits on `!hangable`), `spawnsafe.go:594-616` (`sanitizePATH` always dedups + appends fallback), `spawnsafe.go:568` (`resolveInDirs`), `spawnsafe.go:575/722-739` (`envWithPATH`)
- **Claim (verified at source):** `hasHangable` is set purely from fstype classification, **health-independent** — `Prepare` returns `Active:true` whenever a hangable mount is present **even when all are healthy and nothing is dropped**. The active path then (1) `sanitizePATH` unconditionally appends `/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin` (lines 609-614) and collapses PATH duplicates, (2) sets `cmd.Env` explicitly via `envWithPATH(os.Environ(),...)` instead of legacy nil-inherit, (3) self-resolves argv[0] via `resolveInDirs` over the fallback-appended PATH. Concretely: a bare-name binary resolvable only via an appended fallback dir **now runs** in active mode but would `command not found` in legacy mode — a real resolution change. `dropped==0` ⇒ `Warn==""` ⇒ **silent**.
- **Impact:** The plan §1 "inert guarantee" ("no hangable mounts **OR all-healthy** ⇒ byte-identical, no PATH change") is **false** for the all-healthy-but-hangable case — which is the confirmed timan1 production steady state (healthy NFS miniconda/home present). Any workflow relying on exact PATH (deliberate `/usr/sbin` exclusion, shadowing order, "not found" as a signal) silently changes behavior on every healthy production agent.
- **Fix:** Gate `Active:true` / the PATH+env+argv0 rewrite on `dropped>0` (i.e. only when an outage is actually detected — the same trigger already used for the cwd substitution and warn). When nothing is dropped on a healthy-hangable machine, return `Decision{Active:false}` so the legacy verbatim-PATH/nil-env path is used unchanged. **Or** tighten the plan/docs to admit "all-healthy-but-hangable is NOT byte-identical" and pin the exact divergence.
- **New test:** healthy hangable mount present; assert the active-mode child `PATH` and resolved argv[0] equal the legacy (off-mode) baseline **byte-for-byte** (currently they will not). This is the missing `TestRemoteFS_inertWhenNoHangableMounts` / a new `TestPrepare_healthyHangable` asserting `Active==true, dropped==0, Warn=="", Cwd=="", kept[:len(orig)]==orig`.

> `all-healthy-hangable-not-byte-identical`, `active-healthy-path-rewrite`, and `inert-guarantee-not-test-pinned-healthy-hangable` are the same root defect (the implementation + its missing test). Fix the gate once, add the byte-diff test once.

#### M3. Active-on-healthy-hangable leaks the agent's stale `$PWD` to the child for `--cwd` (os/exec only injects PWD when `cmd.Env==nil`)
- **Where:** `internal/agent/exec.go:305-321` (`buildExecCmd` builds `&exec.Cmd{Env:d.Env, Dir:req.Cwd}` with non-nil Env), `internal/spawnsafe/spawnsafe.go:589` (Prepare returns explicit Env)
- **Claim (verified against os/exec source):** `os/exec`'s `environ()` appends `PWD=<abs(c.Dir)>` **only inside `if env == nil`**. On a healthy-hangable machine the active path sets `cmd.Env` explicitly (derived from `os.Environ()`, which carries the **agent's** stale `$PWD`), so even though `cmd.Dir` is set, the child inherits the agent's PWD instead of getting `PWD=<cwd>`. Legacy nil-env path gives the child the correct `PWD=<cwd>`. `dropped==0` ⇒ no warning.
- **Impact:** Silent **wrong logical `$PWD`** for `tether exec --cwd <dir> <tool>` on any agent with even one healthy network mount. Breaks `$PWD`-sensitive commands (git, make, relative-path tools) with no warning, on a perfectly healthy machine.
- **Fix:** Either fold into M2's `dropped>0` gate (legacy nil-env path handles PWD correctly), or when `Active && Cwd != ""` explicitly inject `PWD=<cwd>` into `d.Env` to match os/exec's nil-env behavior.
- **New test:** agent with a healthy hangable mount; `exec --cwd <localdir> -- sh -c 'echo $PWD'`; assert child `PWD == requested cwd` byte-for-byte against the off-mode baseline.

#### M4. Abandoned safe-mode exec leaks its `StdoutPipe`/`StderrPipe` fds permanently (never `Wait`ed), unbounded over time
- **Where:** `internal/agent/exec.go:151-172`; reaper `internal/spawnsafe/spawnsafe.go:690-695`
- **Claim (verified against os/exec semantics):** `cmd.StdoutPipe()` (exec.go:151) and `cmd.StderrPipe()` (exec.go:155) allocate the `os.Pipe()` fd pairs at **call time**, before the bounded `cmd.Start`. On the active path, `RunStart(cmd.Start,...)` returns `ErrSpawnTimeout` and `runChild` returns `-1,err` at exec.go:165-166, **skipping `cmd.Wait()`** at line 180. Go closes `StdoutPipe`/`StderrPipe` fds **only inside `Wait()`**. The `RunStart` reaper only drains `done` and decrements `p.wedged`; it never calls `cmd.Wait()` or closes the pipes. So each abandoned safe-mode exec leaks both pipe ends permanently, and recovery does not reclaim them.
- **Impact:** The `wedgeCeiling` (64) bounds **concurrent** abandons but **not cumulative leaked fds** over time. A retrying ctl loop against a wedged mount during a multi-hour outage exhausts the agent's fd table. The plan claimed the watchdog "fixes" the per-hung-exec fd leak; it bounds the goroutine/wedged count but not these fds.
- **Fix:** On the abandon branch, explicitly close `stdoutPipe`/`stderrPipe` (and arrange to call `cmd.Wait()` once start eventually returns, or set `cmd.Cancel`/`cmd.WaitDelay`) so the fds are reclaimed when execve returns.
- **New test (`test/concurrency/fd_leak_test.go`):** spawn N safe-mode execs whose `Start` blocks past the spawn timeout (releasable fake), release, assert the agent fd count returns to baseline.

#### M5. The mandated `test/concurrency/` race+leak gate for this feature does not exist; `make test` runs no `-race`
- **Where:** plan §6 lines 413-415; `Makefile:20-21` (`test: go test ./...`); `test/concurrency/` (no spawnsafe reference)
- **Claim (verified):** Plan §6 mandates a `test/concurrency/` entry exercising concurrent `SanitizePATH`+`ResolveArgv0` during a generation-change snapshot swap under `-race` + `assertNoGoroutineLeak`. **No such entry exists** — `git grep` finds spawnsafe in tests only under `test/e2e`. The package's only concurrency coverage is its package-local `assertGoroutinesReturn` (spawnsafe_test.go:73), **not** the repo's canonical `assertNoGoroutineLeak`, and `make test` is plain `go test ./...` (no `-race`, no `-count=1`). The fake `mountSrc` returns fixed content, so no generation-change swap runs concurrently with resolution. The race-sensitive surfaces (`mountHealthy` lock-drop self-heal, `RunStart` accounting) run **without the race detector** in the standard gate.
- **Impact:** This is exactly the gate that would have caught B1 (mutex poison) and M1 (pty race). A regression in the lock-drop/self-heal or wedged accounting passes `make test` silently.
- **Fix:** Add a `test/concurrency/` file hammering `Policy.Prepare`/`mountHealthy`/`RunStart` from N goroutines across a `mountSrc` that flips content (gen change) mid-run, asserting via the repo's `assertNoGoroutineLeak`; ensure it runs under `-race` in the gate (or document the spawnsafe package must run with `-race` in the commit-blocker checklist).

---

### MINOR

#### m1. Relative argv[0] over a dead agent cwd degrades to a 30s `remote_fs_spawn_timeout` instead of a fast `remote_fs_unhealthy`
- **Where:** `internal/spawnsafe/spawnsafe.go:559-573`, `mountForPath:368-382`
- **Claim (verified empirically):** `filepath.Clean("./tool")=="tool"` stays relative; `pathHasPrefix` never matches an absolute mountpoint against a relative clean path, so `pathOnDeadMount` returns false ⇒ `abs=name` and we proceed. With an unset `--cwd` (so the cwd fail-fast at spawnsafe.go:551, guarded by `cwd != ""`, never runs), a relative argv[0] over a dead agent cwd D-hangs at execve and is caught only by the 30s `RunStart` watchdog returning `remote_fs_spawn_timeout`.
- **Impact:** Bounded (no unbounded hang) but a **misleading reason code** and a 30s wait. Narrow precondition (relative argv[0] AND unset `--cwd` AND dead agent cwd).
- **Fix / test:** resolve a relative argv[0] against the (health-checked) cwd/safe_dir before classifying, or require the cwd liveness check when argv[0] is relative. Table case: relative `./x` with agent cwd on a dead mount asserts a fast FSError, not a 30s spawn-timeout.

#### m2. Second unbounded TOCTOU hang window: `resolveInDirs` stats kept-healthy hangable dirs inside `Prepare` (not under any watchdog)
- **Where:** `internal/spawnsafe/spawnsafe.go:618-637`, kept-dir logic at `594-607`
- **Claim (verified):** `sanitizePATH` intentionally KEEPS healthy-hangable dirs; `resolveInDirs` then `os.Stat`s over them **inside `Prepare`**, which is **not** wrapped by `RunStart` (only `cmd.Start` is). A mount healthy at probe time that dies before resolution D-hangs `Prepare` unbounded. The deterministic-hang guarantee still holds (a known-dead dir is dropped before `resolveInDirs`), so this is a **plan-accuracy** defect, not a ship-stopper: plan §2 names only `cmd.Start` as the residual TOCTOU point and omits this second, wider window.
- **Fix:** Correct plan §2 to document `resolveInDirs` as a second residual TOCTOU hang point (or bound the bare-name resolution stats behind a short deadline if the wider window is unacceptable).

#### m3. `tether exec` surfaces the `remote_fs_*` reason RAW with no operator hint; `tether run` gets the friendly sentence
- **Where:** `cmd/tether/exec.go:135-136` (raw `fmt.Errorf("exec: %s", chunk.Error)`); `cmd/tether/error_hints.go:105-118` (`runFailureReasons` covers the codes, exec has no equivalent)
- **Claim (verified):** The five `remote_fs_*` codes + `too_many_wedged_spawns` get a hint **only** on the run path. The exec path prints `exec: remote_fs_unhealthy: /shared/nas/bin/python` with zero actionable guidance — a UX asymmetry across two verbs the feature treats as parallel.
- **Fix / test:** map the `remote_fs_*` codes to the same hint sentences on the exec error path (reuse `runFailureReasons`); assert `tether exec` surfaces the hint for at least `remote_fs_unhealthy` and `remote_fs_spawn_timeout`.

#### m4. The entire `remote_fs:` agent.yaml block + `parseOptDuration` have ZERO cmd/tether test coverage
- **Where:** `cmd/tether/agent.go:40-63,198-221` (config + `parseOptDuration`); `cmd/tether/agent_config_test.go` (no remote_fs case)
- **Claim (verified):** `grep remote_fs|RemoteFS|parseOptDuration` across `cmd/tether/*_test.go` returns nothing. `loadAgentYAML` never round-trips `remote_fs.{mode,safe_dir,probe_timeout,spawn_timeout,wedge_ceiling}`; `parseOptDuration`'s malformed-fails-loud (agent.go:56-58) and negative-guard (`d<0`, agent.go:59-61) are unreachable from any test; the nested-typo `KnownFields` rejection (e.g. `remote_fs:\n  moed: auto`) is unasserted. The plan §6's `TestAgentYAML_remoteFSEnum` was **not implemented**. Every sibling block (file_transfer, proxy) has a dedicated table.
- **Impact:** A regression dropping `KnownFields` strictness on the nested block, swapping probe/spawn timeout, or making `parseOptDuration` swallow a malformed duration would ship green. (The parsing logic itself is currently correct — this is a coverage/regression-risk gap.)
- **New tests:** table-drive: (1) absent block ⇒ `RemoteFSMode==""`; (2) `mode: off` accepted; (3) full block round-trips; (4) `moed:` and top-level `remotefs:` rejected via `KnownFields`; plus a `parseOptDuration` table: `""→(0,nil)`, `"2s"→(2s,nil)`, `"-5s"→error`, `"2sx"→error`, `"30"(bare int)→error`, asserting the field name is in the error.

> `missing-agentyaml-remotefs-config-tests` and `remotefs-yaml-block-untested` are the same gap.

#### m5. Two of three concurrency tests the plan listed under the leak gate omit `assertGoroutinesReturn`
- **Where:** `internal/spawnsafe/spawnsafe_test.go:169-216` (`TestResolveArgv0_neverWalksDeadDir`), `220-266` (`TestStickyProbe_singleFlight_selfHeals`)
- **Claim (verified):** Only `TestRunStart_abandonsAndCeiling` calls `assertGoroutinesReturn`. The other two spawn blocking probe goroutines (`fp.block`) but never record a baseline or assert release; `TestStickyProbe`'s `block` channel is never closed at cleanup and the test only checks `count==1`. A regression that leaked the probe goroutine (self-heal stops draining `h.result`, or the buffered(1) chan made unbuffered) would NOT be caught.
- **Fix:** add `before := runtime.NumGoroutine()` + `assertGoroutinesReturn(t, before)` to both; in `TestStickyProbe` add a `t.Cleanup` that releases any still-blocked probe goroutine.

#### m6. `--safe` ordering footgun is missing from `--help` and untested (plan required it in `Long` help)
- **Where:** `cmd/tether/exec.go:28-37,147-151`, `cmd/tether/run.go:50-60,281-285`
- **Claim (verified):** Plan §4/§11.5 required documenting `--safe` ordering in the `Long` help with the example `tether exec --safe gpu-01 -- whoami`. Only a **source comment** was added; the cobra `Long` text and `BoolVar` description say nothing. With `SetInterspersed(false)`, `tether exec gpu-01 --safe whoami` (the natural typo) parses to `safe=false` and ships argv `["--safe","whoami"]` to the agent — the lifeline **silently no-ops** and the child fails on executable `--safe`. No test pins the parse contract.
- **Fix / test:** append the ordering note + example to both `Long` strings; add a parse test asserting `[--safe node -- cmd]⇒safe=true,argv=[cmd]`, `[node --safe cmd]⇒safe=false,argv=[--safe,cmd]`, `[node -- --safe cmd]⇒safe=false,argv=[--safe,cmd]` (a real remote `--safe` survives the strip-`--`).

#### m7. Default `mode=auto` does a per-spawn `/proc/self/mountinfo` read even on fully-local machines (contradicts the "zero extra syscall" inert claim)
- **Where:** `internal/spawnsafe/spawnsafe.go:539-541` (off short-circuits), `542` (`refreshIfChanged` runs before the `!hangable` early-return at 546), `270-295`
- **Claim (verified):** Only `ModeOff` avoids `refreshIfChanged`. Default `ModeAuto` reads `/proc/self/mountinfo` + FNV-hashes on **every** exec/run, even on a zero-hangable local machine. Procfs is kernel-resident (never hangs) and the child still gets `Active:false`, so no child-observable difference and no hang — but it is not the claimed "zero-syscall" inert path, and the inert guarantee is not test-pinned (the fake `mountSrc` does not count calls).
- **Fix:** either correct the docs ("auto does one cheap procfs read per spawn"), or cache `hasHangable` from the boot snapshot and skip `refreshIfChanged` when the boot snapshot had zero hangable mounts. Add a test asserting `mountSource` call count on N local-machine spawns.

#### m8. `mode=off == today` inertness is pinned only inside spawnsafe, not at the agent handler (byte-level)
- **Where:** `internal/agent/exec.go:304-322` (`buildExecCmd`), `internal/agent/run.go:164-188`
- **Claim (verified):** off-mode is genuinely inert end-to-end (Prepare returns `Decision{}` before `refreshIfChanged`; legacy `exec.Command` branch; `cmd.Start` unwrapped, `cmd.Env==nil`). But no **agent-level** test pins it: `TestBuildExecCmd_inertWhenNoHangable` exercises the no-hangable-mounts path, not `mode=off` WITH a hangable mount present. A future change wrapping `cmd.Start` unconditionally would silently lose off-mode inertness.
- **Fix / test:** agent test with `RemoteFSMode:"off"` + a fake mountinfo containing a hangable mount; assert `buildExecCmd` returns `decision.Active==false`, `cmd.Env==nil`, and `cmd.Path == exec.Command("echo").Path`.

#### m9. Concurrent first-touch joiner on a healthy mount stalls up to `probeTimeout` (~2s)
- **Where:** `internal/spawnsafe/spawnsafe.go:445-484` (buffered(1) result chan, single receiver)
- **Claim (verified):** Two first-touch callers both wait on the shared buffered(1) `ch`; the single value is consumed by one receiver (sets `stHealthy`), the other's `<-ch` never fires and it blocks until `<-time.After(timeout)` before reading the now-`stHealthy` state. Verdict correct, but a concurrent joiner on a healthy mount is delayed up to `DefaultProbeTimeout` (2s). Bounded to the one-time pre-cache window.
- **Fix / test:** after the receive sets the state, close/broadcast to wake concurrent joiners (or signal via `sync.Cond`); test that N concurrent `mountHealthy` on a healthy mount all return within a small bound.

---

### NIT

#### n1. `safe:` agent.yaml downgrade trap is undocumented
- **Where:** `cmd/tether/agent.go:114` (`dec.KnownFields(true)`)
- A pre-feature binary parsing an agent.yaml with a `remote_fs:` block fails to boot ("field remote_fs not found"). Same accepted class as every prior additive yaml key, but the rollback caveat is undocumented. **Fix:** one line in plan §9 / docs/usage.md noting a `remote_fs:` block must be removed before downgrading to a pre-v0.3.3 binary.

#### n2. docs/usage.md cross-reference points to the wrong section
- **Where:** `docs/usage.md:273`
- The `remote_fs.mode` config-table row says "见 §7.6" but §7.6 is "监控 tether 自身" (monitoring); the actual remote-fs troubleshooting section is §7.7. **Fix:** change "见 §7.6。" to "见 §7.7。".

---

## 3. Refuted / uncertain — considered and (mostly) dismissed

- **`autofs-drops-healthy-path-dir` (downgraded major → uncertain/minor):** The mechanism is real — `classifyFstype("autofs")` returns `kindRemoteNever` and `pathOnDeadMount` returns true **unconditionally** without probing (spawnsafe.go:111-113, 406-408), so a PATH dir whose **longest-prefix** backing mount is the bare autofs mountpoint is dropped on a healthy box (with a spurious warning). **But** `mountForPath` does longest-prefix matching: once the per-user NFS submount `/home/<user>` is present in mountinfo (the normal steady state once `~` has been accessed — and the agent itself reads `~/.tether` at startup, triggering the automount), `/home/<user>/.local/bin` matches the deeper probed NFS submount, not the autofs parent. The flatly-dead drop only bites when the longest prefix is the autofs mountpoint **itself with no deeper submount in the current snapshot**, a narrow window. Cannot be proven to fire on the actual fleet without real mountinfo. **Action:** treat as a minor robustness note — consider only dropping a `kindRemoteNever` PATH dir when no deeper non-autofs mount covers it; add a test (autofs `/home` + healthy NFS `/home/u` submount; PATH `/home/u/.local/bin`) asserting it is KEPT and `Warn==""`.

- **`safe-wire-additive-verified` (nit, POSITIVE):** Confirmed the wire change is clean — `Safe` is additive+omitempty on both `ExecReq` (messages.go:190) and `RunReq` (messages.go:337), ProtoVersion stays 1, survives the broker `Unmarshal→stamp-ActorFP→Marshal` on both verbs by construction, and is pinned by `TestSafeFieldOmitemptyByteIdentical` + `TestSafeReqRoundTripsBrokerBothVerbs` (real embedded-NATS broker). No fix needed; optional: assert the on-wire `safe` key presence/absence post-forward (currently only the decoded bool is checked).

No finding was fully refuted — every raised claim was at least mechanism-confirmed. The adjudication caught no fabricated bugs.

---

## 4. Test-coverage gaps to add (consolidated, concrete cases)

1. **Component I mutex/abandon (for B1):** a `stateStore` with a channel-blockable `ReadFile`; after one abandoned bounded read assert (a) a second bounded read completes/abandons within deadline, (b) `AddPort` completes within deadline, (c) goroutine count flat across repeated reconnects.
2. **Boot replay bound (for B2):** in-process agent, hangable Home + blocking state read; assert `Run()` reaches `heartbeatLoop` within the deadline.
3. **pty abandon-then-recover race (for M1, `-race`):** delayed `sess.Start` past the deadline, `Close()` on abandon, then release Start; assert no race / no double-close.
4. **Healthy-hangable byte-diff (for M2/M3):** healthy hangable mount present; assert child `PATH`, `cmd.Env`, resolved argv[0], and child `$PWD` (with `--cwd`) all equal the off-mode legacy baseline byte-for-byte.
5. **exec fd leak (for M4, `test/concurrency/fd_leak_test.go`):** N safe-mode execs blocking past spawn timeout, released; assert agent fd count returns to baseline.
6. **Concurrency gen-swap gate (for M5, `test/concurrency/`, `-race` + `assertNoGoroutineLeak`):** N goroutines hammering `Prepare`/`mountHealthy`/`RunStart` over a `mountSrc` that flips content mid-run.
7. **remote_fs yaml + parseOptDuration (for m4):** the `TestAgentYAML_remoteFSEnum` table + a `parseOptDuration` table (`""`, `2s`, `-5s`, `2sx`, bare `30`), asserting `KnownFields` rejection of `moed:`/`remotefs:` and field-name-in-error for malformed durations.
8. **--safe parse contract (for m6):** leading vs trailing `--safe` parse cases, including a real remote `--safe` surviving the strip-`--`.
9. **Leak gates (for m5):** `assertGoroutinesReturn` added to `TestResolveArgv0_neverWalksDeadDir` and `TestStickyProbe_singleFlight_selfHeals` with cleanup that releases blocked probe goroutines.
10. **off-mode handler inertness (for m8):** `mode=off` + hangable mount present ⇒ `decision.Active==false`, `cmd.Env==nil`, `cmd.Path==exec.Command(...).Path`.
11. **relative argv[0] fast-fail (for m1):** `./x` with dead agent cwd ⇒ fast FSError, not 30s spawn-timeout.
12. **healthy-mount joiner latency (for m9):** N concurrent `mountHealthy` on a healthy mount all return within a small bound.

---

## 5. What was checked and held up (adversarial attempts that FAILED to break the code)

The core un-hang mechanism for the spawn path is **genuinely correct** — these are the breaks that were attempted and did not land:

- **LookPath bypass holds.** Verified against the Go os/exec source that `&exec.Cmd{Path,Args}` struct-literal `Start()` does **not** stat the path on linux/darwin (`lookExtensions` is Windows-only); `StdoutPipe`/`StderrPipe` create pipes with no stat. No `LookPath`/stat runs at construction.
- **`sanitizePATH` drops a deterministically-dead mount before `resolveInDirs` ever stats it**, so bare-name resolution never walks a known-dead dir — the headline deterministic-hang guarantee holds.
- **Fail-fast reason codes are correct:** explicit-path on a dead mount → `remote_fs_unhealthy`; dead cwd → `remote_fs_unsafe_cwd`; missing binary → `remote_fs_not_found`; residual execve hang → `remote_fs_spawn_timeout`.
- **`RunStart` accounting is correct:** no double-decrement of `wedged`, correct ceiling off-by-one, single increment/decrement; abandon-then-reap is sound for the goroutine count (the *fd* leak in M4 is a separate axis).
- **Probe state machine is correct:** `parseMountinfo` octal-unescape, longest-prefix component-boundary matching, `pathOnDeadMount`, and the single-flight/sticky/self-heal machine show no lost or double-applied late probe; the lock is correctly held across the self-heal drain; no stale-pointer corruption after a generation swap.
- **Wire/compat is solid:** `Safe omitempty` byte-identical, ProtoVersion 1, broker round-trip on both verbs, cross-version skew both directions (legacy body → `Safe=false`; new `--safe` on old agent silently no-ops; `Safe=true` correctly escalates over agent `mode:off`); no `AuditSchemaVersion`/`SubjectPrefix`/subject-tree change (the `remote_fs_*` strings are free-form Reason/Error *values*, not new fields).
- **No port-revoke from a degraded snapshot:** the broker's reconcile deliberately does NOT revoke ports an agent omits, so Component I's degraded (empty-ports) snapshot cannot cause port **data loss** — attempted and could not break it. (This is the saving grace that keeps B1/B2 to a hang rather than a data-loss class.)
- **`safe_dir` is genuinely used (not dead code):** substituted only when `dropped>0 && cwd unset`; `validSafeDir` refuses a hangable override by string; all three branches pinned by `TestPrepare_cwdFailFastAndSubstitute`.
- **off-mode + no-hangable-mounts ARE inert** (the code, not the test pin): off short-circuits before any refresh/probe and takes the legacy nil-env path; a zero-hangable machine early-returns `Active:false`.

The damage is concentrated at the **integration boundary the unit tests stub out** (Component I's real mutex-holding `state.json` read; the run/pty abandon-then-recover window; the all-healthy-hangable inertness case) and in **missing mandated gates**. The pure-spawnsafe primitives are robust.

---

## 6. Adjudication & remediation (main process — step C5)

Every confirmed finding was **accepted and fixed** (the main process is the sole implementer). The core
spawn-path mechanism was confirmed sound and unchanged; the damage was at the integration boundaries the
reviewers correctly targeted. All four hard gates re-pass after remediation: `make test`, `golangci-lint v2`
(0 issues), `-race` on spawnsafe/agent/pty/proto/test-concurrency, and `make e2e` (incl. the new gates).

### Blockers — fixed
- **B1** (state.json read holds `s.mu` across the wedged read; abandon poisons all state I/O; O(reconnects)
  pile-up): added `stateStore.loadNoLock` (lock-free `os.ReadFile`, torn-free because writes are atomic-rename)
  and rebuilt the bounded read as **single-flight** (`Agent.boundedHomeRead` + `homeReadInFlight`): an abandoned
  read holds no mutex AND a second wedged read does not spawn another goroutine ⇒ abandoned readers bounded to
  ONE. New tests: `TestBoundedHomeRead_singleFlightAbandon`, `TestStateStore_loadNoLockIsLockFree`.
- **B2** (`replayPortsFromState` unbounded boot read): routed through `loadStateBounded` (degrade + skip replay).
  New test: `TestReplayPortsFromState_boundedOnWedgedHome` (FIFO-blocked read; asserts it returns within deadline).

### Majors — fixed
- **M1** (abandoned `sess.Start` races `Close`, double-close): made `Session` concurrency-safe (mutex + `closed`
  + `started` slave-ownership transfer); `cmd.Start` runs lock-free; Close never touches the slave once Start
  owns it. New tests `TestSession_concurrentStartClose_noRace` (-race) + `TestSession_closeBeforeStart` — the
  first **caught a residual race in the initial fix**, which was then corrected (slave ownership), exactly the
  gate's purpose.
- **M2** (active-on-healthy-hangable not byte-identical): gated `Active` on `dropped>0` — a healthy machine (even
  with healthy network mounts) stays inert and uses the verbatim legacy PATH/env/argv0. New tests
  `TestBuildExecCmd_healthyHangableIsInert`, updated `TestPrepare_inertAndEscalation`.
- **M3** (stale `$PWD` leak): the active path injects `PWD=<cwd>` into the explicit env (matching os/exec's
  nil-env behavior). New test `TestBuildExecCmd_activeInjectsPWD`.
- **M4** (abandoned exec leaks pipe fds): `startBounded` now closes the pipe read ends on abandon and reaps via
  `cmd.Wait` when the execve returns; wedge slot held until then. New test
  `TestStartBounded_abandonClosesPipesAndReaps`.
- **M5** (missing `test/concurrency` gate): added `test/concurrency/spawnsafe_stress_test.go` (concurrent
  Prepare/RunStart over a generation-flipping mount table, `assertNoGoroutineLeak`, -race clean).

### Minors / nits — fixed
- **m3** exec now maps `remote_fs_*` codes to the same operator hints as run (`execFailureMessage`).
- **m4** added `TestAgentYAML_remoteFS` + `TestParseOptDuration` (KnownFields rejection, field-named errors).
- **m5** added `assertGoroutinesReturn` + releasable probe goroutines to the two probe tests.
- **m6** added the `--safe` ordering note to both `Long` helps + `TestExecSafeFlagOrdering`/`TestRunSafeFlagOrdering`.
- **m7** local-boot agents skip the per-spawn mountinfo read (`bootHangable` fast path) ⇒ genuinely
  zero-syscall-per-spawn; `TestPrepare_localMachineZeroSyscallPerSpawn`. Plan §1 wording corrected.
- **m8** added `TestBuildExecCmd_offModeInertWithHangable`.
- **m9** `mountHealthy` joiners now wake on a `done` broadcast (not a full probeTimeout); `TestMountHealthy_joinersWakeOnVerdict`.
- **n1** autofs: `TestAutofs_longestPrefixKeepsHealthySubmount` pins that longest-prefix protects the common
  healthy-submount case; downgrade caveat documented (plan §9 / usage §7.7).
- **n2** usage.md §7.6→§7.7 xref fixed.
- **m1** (relative argv[0] over dead cwd ⇒ bounded spawn-timeout, not fast-fail): accepted as a documented
  narrow case (plan §2) — bounded, not a hang. **m2** (resolveInDirs second TOCTOU window): documented in plan §2.

**Net:** 2 blockers + 8 majors + the actionable minors all remediated; the one residual race introduced by the
M1 fix was caught by the new -race gate and fixed. Ready for external review.
