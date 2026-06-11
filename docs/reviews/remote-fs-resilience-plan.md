# Remote-FS Resilience — hung-mount-safe `exec`/`run` spawn — Implementation Plan

Date: 2026-06-11
Status: **PASS — external re-review approved.** The first external review
(`remote-fs-resilience-external-review.md`) returned BLOCKED with 5 High + 6 Medium + 2 Low; the executor
remediated them, and the final re-review directly fixed 3 High + 3 Medium + 2 Low follow-up gaps before
release. See that report's "Reviewer final re-review" section. **Key design shift from F1/F2:** on a hangable
machine the agent now ALWAYS self-resolves argv[0] (never `exec.Command`'s LookPath, which was itself the
unbounded-hang surface) against the **agent** PATH, with bounded resolution + execve; compatibility now means
preserving child env/cwd/output when nothing is dropped (`Decision.Outage=false`), not "use legacy LookPath".
autofs is a guarded trigger-only category (F10); network Home is a documented best-effort contract (F9);
explicit `--safe` bypasses the
boot fast path (F7). The adversarial internal
review (`remote-fs-resilience-review.md`) found 2 blockers + 8 majors; all were remediated (see that file's §6
adjudication for the per-finding resolution + new tests). Post-review design deltas vs this plan body: the
Component-I bounded read is **single-flight + lock-free** (§3.I, review B1); `Session` is concurrency-safe with
slave-ownership transfer (review M1); healthy hangable/autofs machines self-resolve and bound Start while only
`Outage` is gated on `dropped>0` (§3.D/F, review M2/M3); exec abandon uses
`TryAcquireSpawnSlot`/`ReleaseSpawnSlot` + pipe-close + reap (review
M4); local-boot agents skip the per-spawn mountinfo read (§1, review m7).
Target release: **v0.3.3** (patch — additive wire, no default flip; see §10)
Convention: post-1.0 leaf increment (additive `ExecReq`/`RunReq` field, `ProtoVersion` stays 1), same shape as
P12 `expose --remote-port` and transfer-unrestrict (v0.4.0). Not on the P0–P11 milestone line.

> **How this plan was produced.** Stage-A per CLAUDE.md §3: a 5-expert + 3-critic adversarial Workflow drafted
> and synthesized a candidate; the main process then **verified every load-bearing in-tree claim** (see §0) and
> finalized — adopting the candidate's core, cutting the same footguns it cut, and adding two finalizer
> refinements (§0). The expert pass corrected three factual errors in the original design direction (RunChunk has
> no stderr field; `always`-mode would break healthy NFS-hosted toolchains; TTL re-probing leaks one D-goroutine
> per window) and surfaced one must-fix nobody on the original direction caught (agent-liveness D-hang on a
> hangable `~/.tether`, §3.I).

---

## 0. Finalizer notes (main process)

**Load-bearing claims verified in-tree before finalizing** (file:line):
- `exec.Command` hang site #1: `internal/agent/exec.go:141` (`runChild`). Site #2: `internal/pty/pty.go:77`
  (`Session.Start`). Both call `exec.Command(argv[0], …)` whose `LookPath` stats `$PATH` at construction.
- Broker re-marshal round-trips a new struct field on **both** verbs: `internal/broker/exec.go:89-95`,
  `internal/broker/run.go:75-81` (`Unmarshal → req.ActorFP=fp → Marshal`). A new exported field on the same
  struct survives by construction.
- `RunChunk` has **no** stderr/text/byte field (`internal/proto/messages.go`: `Kind/PID/Cols/Rows/ExitCode/Reason`).
  `ExecChunk{Kind:"stderr"}` **does** exist and is already used (`exec.go:185`).
- Component-I D-hang chain is real: `onNATSReconnect` (`proxy.go:342`) → `register` (`agent.go:522`) →
  `buildLocalSnapshot` (`agent.go:604` → `:523`) → `stateStore.load` → `os.ReadFile` (`state.go:76`),
  unbounded, on **every** NATS reconnect.
- Test scaffolding the plan leans on exists: `proto.allRoundtripCases()` (`proto_invariants_test.go:40`, ExecReq
  at `:54`), `runFailureReasons`/`runFailureMessage` (`error_hints.go:105/113`), strict
  `dec.KnownFields(true)` (`cmd/tether/agent.go:84`).
- **Leak gate:** the repo **deliberately avoids `goleak`** (`test/concurrency/helpers_test.go:5`) and uses the
  count-based `assertNoGoroutineLeak` (`:136`, polls until count returns near baseline). CLAUDE.md §5's mention
  of "goleak" is inaccurate vs the actual codebase — **this plan follows the codebase** (count-based helper).
  Consequence pinned into the test design: a count-based gate would *fail* on a genuinely-abandoned D-goroutine,
  so every hermetic test uses a **releasable** fake (channel the test closes at cleanup) so the abandoned
  goroutine exits and the count returns to baseline.

**Finalizer deltas vs the candidate** (two refinements, both reduce footgun risk, neither is gold-plating):
1. **Sticky-dead self-heals on late probe success.** The bounded probe goroutine already writes to a
   `buffered(1)` chan so it can send-and-exit if the mount recovers. The cache **consumes that late result**: a
   mount marked UNHEALTHY flips back to HEALTHY when its single outstanding probe goroutine eventually returns
   success — *without* issuing any new probe. This removes the candidate's one real downside (a transient
   latency spike permanently dropping a healthy mount from `$PATH` until a mountinfo generation change) while
   keeping "at most one in-flight probe per mount, ever".
2. **Default probe timeout 2s (was 800ms).** On the confirmed timan1 mounts (`timeo=600`), a *truly* wedged
   `statfs` does not answer for ~60s, so 2s separates dead (never answers) from healthy-but-loaded (answers in
   <2s) with comfortable headroom against false positives, while detection stays one-time and bounded. Tunable
   via `remote_fs.probe_timeout`.

**Implementation-discovered deltas (CLAUDE.md "先改文档再改代码"; folded back here after building):**
1. **`safe_dir` earns a real job → cwd substitution (§3.C/§3.F).** With HOME/cwd-forcing cut, `safe_dir`
   would have been a dead config knob (deadcode-audit risk). It now substitutes as the child cwd **only when**
   a spawn actually dropped a dead PATH dir AND the caller left `--cwd` unset — i.e. during a real outage,
   avoiding inheriting a possibly-dead agent cwd. On healthy operation (`dropped==0`) cwd is untouched, so this
   is inert when the fs is fine. `safe_dir` is validated local+writable once at `New()` (else `os.TempDir()`).
2. **Uniform agent-side spawn-window deadline (§3.G).** The candidate said exec's deadline "derives from the
   ctl `--timeout`", but `ExecReq` carries no timeout on the wire. Both verbs therefore use one agent-side
   `remote_fs.spawn_timeout` (default **30s**), wrapping **only the execve** (`cmd.Start` / `sess.Start`), never
   the command runtime. exec's ctl `--timeout` still bounds the overall call ctl-side.
3. **Component I read bounded via an extracted `boundedLoad` helper** (testable without a real hung fs).
4. **Config knobs:** added `remote_fs.probe_timeout` and `remote_fs.spawn_timeout` (Go duration strings, parsed
   with a loud error on a typo) alongside `mode`/`safe_dir`/`wedge_ceiling`.

Everything else below is the finalized design.

---

## 1. Goal & scope

Make `tether exec` and `tether run` **un-hang** when a network filesystem (NFS/CIFS/sshfs/lustre/ceph/9p/autofs…)
backing the agent's `$PATH` (or an explicit `argv[0]` / `--cwd`) is wedged in uninterruptible **D-state**.
Both verbs are in scope; `run` is **not** deferrable (§9 justification).

**Confirmed root cause (live timan1 + verified in-tree).** Both spawn sites build the child with
`exec.Command(argv[0], …)`. `exec.Command` runs `exec.LookPath(argv[0])` **synchronously at construction time**,
which `os.Stat()`s each `$PATH` directory **in order** against the **agent process's** `$PATH`. On timan1 the
child `$PATH` begins with NFS-backed dirs (`/shared/nas/.../miniconda3/bin` nfs4, `/home/<user>/.local/bin`
nfs `hard`), so `LookPath` D-hangs on the **first** entry **before any fork** — even for a trivial `whoami`.
Setting `cmd.Env`'s `PATH` afterward is too late; `LookPath` already ran. The agent daemon itself survives
(it runs from local disk; its steady-state hot path is memory + NATS), which is exactly why "everything except
`run`/`exec` keeps working".

**The fix has three load-bearing parts** (everything else is cut — §2, §3):

1. **Sanitize-then-resolve, then hand-build the `Cmd`.** Drop unhealthy-hangable `$PATH` entries, resolve
   `argv[0]` against the remaining PATH under a bounded resolver, and construct
   `&exec.Cmd{Path: abs, Args: argv}` **directly as a struct literal** at both sites — never
   `exec.Command(bareName)` then patch `cmd.Path` (by then `LookPath` already D-hung). **Order is load-bearing:
   sanitize PATH first, resolve second.**
2. **Agent-side `cmd.Start` watchdog + abandon** for both verbs. Even with an absolute `Path`, the kernel
   `execve(2)` stats `argv[0]`'s inode and the cwd, so it can still D-hang. `run` has **no** post-exec deadline
   today; `exec`'s `--timeout` is **ctl-side only** and leaks the agent goroutine. Abandon (never kill —
   D-state ignores SIGKILL), with a **concurrent-wedged ceiling**.
3. **Hang-safe agent liveness (§3.I).** The agent's own re-register path D-hangs reading `state.json` under a
   hangable `~/.tether` on **every** NATS reconnect. A lifeline that keeps `exec`/`run` alive while the agent
   silently wedges its own liveness is incomplete. Bound that read behind the same probe seam.

**Compatibility guarantee.**
- **No hangable mount at boot (the local/no-NFS agent):** one procfs `mountinfo` scan at `New()`, then **zero
  per-spawn syscalls** and the exact legacy spawn path — Prepare short-circuits on the cached boot verdict.
  Test-pinned by
  `TestPrepare_localMachineZeroSyscallPerSpawn`.
- **Hangable mount present but all-healthy (the timan steady state):** one cheap procfs `mountinfo` read per
  spawn + a one-time `statfs` per referenced remote mount (cached). The agent self-resolves argv[0] and bounds
  Start, but since **nothing is dropped** it preserves the legacy child env/cwd/argv and emits no warning
  (`Active=true`, `Outage=false`). The resolver retains Go's current-directory `ErrDot` protection.

On non-Linux (darwin CI/dev) the real mountinfo reader returns empty ⇒ zero hangable mounts ⇒ fully inert.

---

## 2. Non-goals & the irreducible limit (stated honestly)

- **Not** a way to run remote-FS-dependent workloads during an outage. If the requested binary **or its required
  data** genuinely lives on the dead fs, the kernel `execve`/`open` D-hangs and **nothing** can run it (D-state
  is unkillable). Safe mode keeps **FS-independent** commands working (diagnostics, `nvidia-smi`, `kill`, reading
  local disk) and makes the rest **fail-fast with a clear warning**. It is a triage/diagnostic lifeline.
- **Not** TOCTOU-free. A mount healthy at probe time can D-hang microseconds later. Both the `os.Stat` walk in
  `resolveInDirs` and `cmd.Start`'s `execve` are bounded and consume the shared wedge ceiling, but the underlying
  kernel threads remain blocked until the filesystem recovers. A relative `argv[0]` over a dead agent cwd with
  `--cwd` unset is bounded (not fast-failed) by the spawn watchdog.
- **Not** macOS-protective. On darwin the feature is inert; a production macOS agent over a hung SMB mount still
  hangs. Acceptable per project scope (macOS = dev/CI). Stated as a known limit, not a silent gap.
- **Not** a child-PATH guarantee. We sanitize the PATH we hand the child; we cannot stop the child from doing
  its own PATH walk (a shell that runs `python` re-hangs) or hardcoding a dead path. Irreducible; documented.

**Cut entirely** (footguns / scope creep the expert pass correctly removed; rationale inline in §3):
`always`-strip-all-healthy mode · HOME rewriting ·
`bash --noprofile --norc` argv injection · `/proc/<pid>/stat`/`wchan` D-state introspection · TTL re-probing ·
`debug.SetMaxThreads` · `x/sys/unix.Statfs(...).Type`-based classification · any new `audit.proc` kind ·
any new `RunChunk` byte/warning field.

---

## 3. Design — decisions made

A small new package **`internal/spawnsafe`** owns all of this behind a **func-injection seam** (matching the
repo's bare-`/proc` `readBootID` / `readStartTimeTicks` style and the `ExposeAdapter` injection at
`agent.go:78` — **not** a build-tag split, which diverges from convention and doubles surface). One
Linux-tolerant impl + the seam; on darwin the real syscalls are simply never reached (zero hangable mounts).

### A. Mount classification — snapshot once, pure string work

Parse `/proc/self/mountinfo` (procfs; the **read** is kernel-resident and never hangs) **once at agent `New()`**;
re-parse only on a **generation change** (mountinfo content hash differs). One `mountEntry{mountpoint, fstype}`
per line. Per spawn, do **longest-prefix** `path → mount` matching, **component-boundary aware** (`/nfs` must not
match `/nfslocal`) — pure string work on the cached slice, **zero syscalls**.

**Decision: fixed conservative remote-fstype denylist; unknown ⇒ local.** Denylist:
`nfs nfs4 cifs smb3 smbfs fuse.sshfs sshfs fuse.s3fs fuse.glusterfs glusterfs lustre ceph fuse.ceph 9p afs
ncpfs coda beegfs gpfs objectivefs` plus a `fuse.*` and `nfs*` **prefix** catch-all. **Rationale:** a "classify
every fs" engine is more code paths to get wrong, and `unknown ⇒ remote` risks self-inflicting an outage by
stripping a novel **local** fstype from PATH. Conservative = we only ever **add** safety. **`autofs` is special:**
classify it as a trigger-only guarded mount. It makes self-resolution/Start bounded, but is never probed or
dropped: probing can itself trigger a synchronous automount, while treating an untriggered autofs as dead would
break a healthy first access.

### B. Bounded health probe — one-shot, sticky (self-healing), single-flight, bounded

**Decision: probe each hangable mount AT MOST ONCE; cache the verdict; NEVER issue a second probe for a known
state until a mountinfo generation change** (rejects a plain ~5s TTL, which re-issues a fresh `statfs` against a
still-dead mount every window and leaks one D-state goroutine **per window** over a multi-hour outage — the exact
bug we fix).

Probe shape: `statfs(mountpoint)` in a throwaway goroutine writing to a **buffered(1)** chan; `select` on chan vs
a timer (default **2s**, §0). Non-answer ⇒ **UNHEALTHY** (cached). **Single-flight**: N concurrent spawns hitting
one dead mount spawn **one** probe, not N. **Self-heal (finalizer §0.1):** the cache consumes a *late* success
from that one outstanding goroutine — if the mount recovers, the goroutine sends `true`, the cache flips back to
HEALTHY, and the goroutine exits (reclaiming the thread) — **without** any new probe. The abandoned goroutine is
bounded to **O(distinct hangable mounts)**, never O(spawns) or O(time). **No `debug.SetMaxThreads`** (fix the
leak at the source, don't raise the process-wide thread cap).

### C. Safe-dir — one operator config value, validated once

**Decision: single optional `agent.yaml remote_fs.safe_dir`, validated local+writable ONCE at `New()` behind the
bounded seam.** Candidate order is override → `os.TempDir()` → `/tmp` → `/var/tmp`; every candidate is
mount-classified and touched under a deadline. If none is valid, no cwd substitution is attempted. The safe-dir
is used only when a spawn dropped a dead PATH dir AND `--cwd` was unset; it is never imposed on HOME or healthy
operation.

### D. PATH sanitization

Split the agent's `$PATH`; drop entries whose backing mount is **probeable-hangable AND unhealthy**. Append the
validated local fallback set (`/usr/local/bin /usr/bin /bin /usr/sbin /sbin`) only during an outage. Healthy
remote and autofs entries remain in order; their resolution is protected by the bounded resolver.

### E. argv[0] self-resolution (replaces `exec.Command`)

`ResolveArgv0(name, sanitizedPATH, policy) (abs string, err error)`:
- **name contains `/` (explicit path):** do NOT walk PATH. If its backing mount is hangable+unhealthy ⇒
  **fail-fast** `remote_fs_unhealthy` — **never silently rewrite** the user's chosen binary to a same-basename
  local one. If its backing mount is healthy/local ⇒ return verbatim, skipping probing entirely (a
  `/usr/local/bin/foo` must Just Work even when PATH is poisoned).
- **bare name:** iterate the sanitized PATH under the bounded resolver. Permission checks mirror Unix LookPath,
  and relative/current-directory hits preserve Go's `exec.ErrDot` rejection. None found ⇒
  `remote_fs_not_found`; a blocked resolution ⇒ `remote_fs_spawn_timeout`.

We do **NOT** call `exec.LookPath` (the hanging family) and do **NOT** feed a sanitized PATH back into
`exec.Command`. The real bypass is the hand-built `&exec.Cmd{Path: abs, Args: argv}` struct literal at both
sites. **`cmd.Args[0]` keeps the ORIGINAL `argv[0]`** (only `cmd.Path` is absolutized) so multi-call binaries
that inspect `os.Args[0]` (busybox, `gcc`-as-`cc`) still work.

### F. cwd / HOME — minimal; no rewriting on healthy

- **Explicit `--cwd` on a hangable+unhealthy mount ⇒ fail-fast** (`remote_fs_unsafe_cwd`).
- **cwd unset, healthy operation (nothing dropped) ⇒ leave as today** (do NOT force a safe dir; forcing it
  changes relative-path semantics for everyone).
- **cwd unset, outage detected (a dead PATH dir was dropped this spawn) ⇒ substitute `safe_dir`** so the child
  does not inherit a possibly-dead agent cwd. This is the one real use of `safe_dir` (§3.C); inert when healthy.
- **Do NOT rewrite HOME** in auto mode. Forcing `HOME=safe_dir` silently changes `~/.netrc`, `~/.kube`,
  `~/.aws`, `~/.ssh`, `~/.config` resolution for **every** safe-mode command, breaking healthy commands to
  defend a hang that argv[0]-resolution + PATH-sanitize already prevent.
- **Do NOT inject `bash --noprofile --norc`** — that is an argv rewrite, the exact thing §3.E forbids.

### G. Spawn watchdog — abandon, never kill, with a ceiling (BOTH verbs)

Even with an absolute `cmd.Path`, `cmd.Start → execve(abs)` can D-hang if `argv[0]`'s inode or the cwd is on the
dead fs. Both verbs wrap **only `cmd.Start` / `sess.Start`** (the execve) in a goroutine and `select` on done vs
a **spawn-window-only** deadline — a uniform agent-side `remote_fs.spawn_timeout` (default **30s**), since
`ExecReq` carries no ctl timeout on the wire (impl delta vs the candidate's "derive from ctl `--timeout`"):
- **exec:** the watchdog also fixes a pre-existing leak — today `runChild` blocks at `cmd.Start`/`wg.Wait`/
  `cmd.Wait` with no agent deadline (3 goroutines + fds leak per hung exec). The ctl `--timeout` still bounds the
  overall call ctl-side; the agent watchdog bounds only the execve.
- **run:** a NEW execve deadline (run had none; the ctl heartbeat watchdog at `run.go:393` never trips because
  the agent keeps beating). **Start-window-only, NOT total-runtime** — wrapping the runtime would kill healthy
  long-lived interactive shells. A child that wedges **after** a clean start is the accepted irreducible limit.

On deadline: **ABANDON** (no `Process.Kill` — D-state ignores SIGKILL; leave the labeled goroutine + fds to
self-reap when the fs recovers). Surface the timeout to ctl (§3.H). **Concurrent-wedged ceiling** (default **64**,
`agent.yaml`-tunable): past the ceiling, refuse new safe spawns with `too_many_wedged_spawns` instead of leaking
the agent to fd/thread exhaustion (rejects "harmless accumulation" — a retrying ctl loop turns a hung command
into a hung agent).

### H. Warning surfacing — the real channels (no phantom `RunChunk.stderr`)

`RunChunk` has **no** byte field (§0). Honest mapping:
- **exec warnings** (sanitize notice + abandon timeout): `ExecChunk{Kind:"stderr", Data:…}` — this channel exists
  (`exec.go:185`) and reaches ctl's stderr even when child stdout is wedged. Emit the sanitize notice **after**
  the `started` chunk, prefixed `[tether agent] ` (matching the existing signal-kill note) so machine consumers
  can filter.
- **run fail-fast** (argv0/cwd on dead mount, not-found, spawn timeout): `RunChunk{Kind:"failed",
  Reason:<value>}`. `Reason` is already free-form (additive, **no wire change**); add matching entries to
  `cmd/tether/error_hints.go runFailureReasons` (`:105`).
- **run non-fatal sanitize notice:** `agent.log` only. Do **NOT** inject a banner into `pty.<pid>.out` (corrupts
  a binary PTY stream / vim / tmux) and do **NOT** add a `RunChunk.Warning` field.

New `Reason`/`Error` string **values** (not new fields): `remote_fs_unhealthy`, `remote_fs_unsafe_cwd`,
`remote_fs_not_found`, `remote_fs_spawn_timeout`, `too_many_wedged_spawns`. **No new `audit.proc` kind:** a
fail-fast started no process, so it follows the existing no-child-started contract (`run.go:181-184` deliberately
doesn't pub `proc.started` pre-exec; `exec` returns `ExecChunk{error}` without `pubProcStarted`). Inventing a
kind would force `AuditSchemaVersion` up (consumer-visible break).

### I. Agent-data-on-remote-fs — hang-safe liveness (the must-fix nobody on the original direction caught)

**Verified chain (§0):** `onNATSReconnect → register → buildLocalSnapshot → stateStore.load → os.ReadFile`,
unbounded, under `~/.tether`, on **every** NATS reconnect. If `Home` is hangable, the re-register goroutine
wedges in D-state while the heartbeat loop keeps the node ONLINE — G.1 reconcile + proxy re-apply never complete,
and flapping links accumulate wedged goroutines. The "agent hot path never re-touches FS" assumption is **false**
for reconnect.

**Decision:**
1. At `New()`, classify `cfg.Home`'s backing fstype. If hangable, emit **one** loud `Logger.Warn` — but keep the
   lifeline independent of it.
2. **Guard `stateStore.load()` reads in `buildLocalSnapshot` (and `applyReconciliation`, `agent.go:656`) behind
   the bounded probe** when `Home` is on a hangable mount: a non-answering read returns a **degraded** snapshot
   (live procs from `a.procs` in memory + empty ports) within the deadline instead of wedging the re-register
   goroutine. Ports reconcile correctly on the next healthy read; a degraded snapshot is strictly better than a
   wedged agent.
3. Order `New()` so the classifier is built **before** `replayPortsFromState` (`agent.go:413`) so a
   hangable-`Home` agent fails loud / degrades at boot instead of wedging before the lifeline exists.

**Blast radius on the current fleet = zero:** all production agents (timan*, pc732, optiplex) run with `Home` on
**local** disk (`/srv/local/...`), so this guard is classified-inert there. It activates only for the
"naively installed with `Home` on NFS" case — exactly the gap it closes.

### Trigger model — `auto | off` (no `always`); wire is one omitempty bool

- `agent.yaml remote_fs.mode: auto | off`, **default `auto`** (key absent ⇒ auto ⇒ inert-when-healthy).
- `auto`: self-resolve + bound Start whenever a probeable remote/autofs mount is present; sanitize+warn only
  when a probeable mount is actually unhealthy. Healthy operation preserves child env/cwd/argv.
- `off`: feature fully disabled (today's exact path) — escape hatch if classification misbehaves on an exotic
  mount.
- **`always` is CUT:** on the confirmed timan1 layout the real toolchain (miniconda) is on a **healthy** NFS
  mount; `always`=strip-all-hangable would refuse working conda binaries during normal operation — a footgun
  that fires more often than it helps.
- **ctl `--safe` (bool):** per-call **opt-in escalation** — forces auto-behavior for this call. A bool cannot
  express per-call "off", which is correct: a ctl member must **not** be able to weaken a shared agent's posture
  (the `off` axis lives only in `agent.yaml`). One `omitempty` bool disappears when false ⇒ byte-identical
  default wire ⇒ `ProtoVersion` stays 1 trivially (matches P12 `RemotePort=0`). **Rejects** a tri-value string
  (puts agent policy on the wire, creates an empty-vs-`auto` decode ambiguity, lets ctl weaken posture).

---

## 4. Exact wire / config changes

### Proto (`internal/proto/messages.go`) — additive, `ProtoVersion` stays 1

Add **one** field to **both** `ExecReq` and `RunReq`:

```go
// Safe, when true, forces hung-mount-safe spawn for THIS call regardless of
// the agent's remote_fs default: the agent pre-resolves argv[0] against a PATH
// sanitized of unhealthy network mounts and fails fast (never hangs) if
// argv[0]/cwd is backed by a wedged mount. omitempty ⇒ absent when false ⇒
// byte-identical default wire (ProtoVersion stays 1). Broker-transparent
// (survives the forward re-marshal, same as ActorFP).
Safe bool `json:"safe,omitempty"`
```

**Broker re-marshal round-trip — VERIFIED SAFE on both verbs** (§0). New `RunChunk.Reason`/`ExecChunk.Error`
**values** are free-form strings — no struct/version change. **No** `AuditSchemaVersion`, `SubjectPrefix`, or
subject-tree change.

### Config (`internal/agent/agent.go Config`)

```go
RemoteFSMode         string        // "auto" (default) | "off"; empty ⇒ "auto". Validated in New().
RemoteFSSafeDir      string        // optional safe-dir override; empty ⇒ os.TempDir().
RemoteFSProbeTimeout time.Duration // 0 ⇒ spawnsafe.DefaultProbeTimeout (2s).
RemoteFSSpawnTimeout time.Duration // execve start-window deadline; 0 ⇒ defaultRemoteFSSpawnTimeout (30s).
RemoteFSWedgeCeiling int           // max concurrent abandoned spawns; 0 ⇒ spawnsafe.DefaultWedgeCeiling (64).
```

Plus **exported injectable seam fields** for hermetic tests (mirror `ExposeAdapter` at `:78`; exported because
`cmd/tether` and the agent test package both set them):

```go
RemoteFSMountSource spawnsafe.MountSource // nil ⇒ real /proc/self/mountinfo reader (empty on darwin)
RemoteFSProbe       spawnsafe.ProbeFn     // nil ⇒ real bounded statfs
```

`New()` validates `RemoteFSMode ∈ {"","auto","off"}` (unknown ⇒ startup error, matching `KnownFields`
strictness), defaults empty→`auto`, builds `a.spawnPolicy *spawnsafe.Policy` (snapshots mountinfo once; computes
the local PATH-fallback set + `safe_dir`), and classifies `cfg.Home` (§3.I).

### agent.yaml (`cmd/tether/agent.go`)

```go
type remoteFSConfig struct {
    Mode         string `yaml:"mode"`          // auto | off
    SafeDir      string `yaml:"safe_dir"`
    ProbeTimeout string `yaml:"probe_timeout"` // Go duration string, e.g. "2s" (yaml.v3 can't decode "2s" → time.Duration)
    SpawnTimeout string `yaml:"spawn_timeout"` // Go duration string, e.g. "30s"
    WedgeCeiling int    `yaml:"wedge_ceiling"`
}
// in agentYAML:
RemoteFS remoteFSConfig `yaml:"remote_fs"`
```

(The duration fields are strings — `yaml.v3` decodes `time.Duration` as raw int64 nanoseconds, so `"2s"` would
error; `cmd/tether` parses them with `time.ParseDuration` and fails loud on a typo.)

Wire at cfg build (`agent.go:168-178`, alongside `ProxyAllowPrivateDestinations`). `KnownFields(true)`
(`cmd/tether/agent.go:84`) already rejects typo'd keys (top-level **and** nested) at startup — fail-loud.

### ctl flags (`cmd/tether/exec.go`, `cmd/tether/run.go`)

Add `--safe` (bool, default false) to both; set `proto.ExecReq{…, Safe: safe}` / `proto.RunReq{…, Safe: safe}`.
Both already `SetInterspersed(false)`, so **`--safe` must precede the node arg** — document in `Long` help with
an example (`tether exec --safe gpu-01 -- whoami`). No broker ctl flag; no `--no-safe` (the off axis is
agent-side only).

---

## 5. Code change sites (checklist)

| File:line | Change |
|---|---|
| `internal/spawnsafe/spawnsafe.go` (NEW) | `mountEntry`, `parseMountinfo([]byte)`, `classifyFstype` (denylist + `fuse.*`/`nfs*` prefix + autofs-by-string), `longestPrefixMount`, `MountSource`/`ProbeFn` seam types, `Policy` (snapshot + single-flight sticky-self-healing probe cache + gen-change refresh), `SanitizePATH`, `ResolveArgv0`, `SafeDir`, `RunWithDeadline(start func() error, d, ceiling)` (abandon + wedge counter). ALL syscalls behind the seam. Linux-tolerant; real mountinfo reader returns empty on read error (darwin inert). No build-tag split. |
| `internal/spawnsafe/spawnsafe_test.go` (NEW) | Table-driven adversarial unit tests (§6). |
| `internal/agent/exec.go:141` | Replace `exec.Command(req.Argv[0], …)` with `spawnsafe` resolve → `&exec.Cmd{Path: abs, Args: req.Argv}` struct literal; `cmd.Env = sanitizedEnv`; `cmd.Dir = req.Cwd` (fail-fast if unhealthy). On resolve err → `ExecChunk{Kind:error}`. |
| `internal/agent/exec.go:161-191` | Wrap `cmd.Start` (keep `wg.Wait`/`cmd.Wait`) under `spawnsafe.RunWithDeadline`; on timeout emit `ExecChunk{Kind:stderr}` note + return `remote_fs_spawn_timeout`, abandon (no Kill). Emit sanitize notice as `ExecChunk{Kind:stderr}` after `started` (`handleExecForwarded:119`). |
| `internal/pty/pty.go:70-102` | Add a `StartResolved(resolvedPath string, argv, env []string, cwd string)` (or `resolvedPath` param on `Start`): build `&exec.Cmd{Path: resolvedPath, Args: argv}` struct literal instead of `exec.Command`. `SysProcAttr{Setsid,Setctty}` / slave binding unchanged. `resolvedPath==""` ⇒ legacy `exec.Command` fallback for non-safe callers. |
| `internal/agent/run.go:149-163` | Resolve `argv[0]` via `spawnsafe` BEFORE `sess.Start`; fail-fast → `RunChunk{Kind:failed, Reason:remote_fs_*}` + `pubPtyFailed`. Pass `resolvedPath` into `sess.Start`. |
| `internal/agent/run.go:155` | Wrap `sess.Start` under the start-window deadline; on timeout → `RunChunk{Kind:failed, Reason:remote_fs_spawn_timeout}`, `unregisterProc`, `sess.Close()`, abandon, RETURN. Start-window-only (never wraps `sess.Wait` at `:217`). |
| `internal/agent/agent.go:47-164` | Add `RemoteFSMode/SafeDir/ProbeTimeout/WedgeCeiling` + unexported `mountSource`/`probeFn` seams (doc-commented). |
| `internal/agent/agent.go (New)` | Validate `RemoteFSMode`; build `a.spawnPolicy`; classify `cfg.Home`, warn-once if hangable; order before `replayPortsFromState` (`:413`). |
| `internal/agent/agent.go:604-646 (buildLocalSnapshot) + :656-668 (applyReconciliation)` | Guard `stateStore.load()` behind the bounded probe when `Home` is hangable → degraded snapshot on non-answer (§3.I). |
| `internal/proto/messages.go` | Add `Safe bool json:"safe,omitempty"` to `ExecReq`+`RunReq`, doc-comment. |
| `internal/proto/proto_invariants_test.go:54` | Set `Safe: true` in the `ExecReq` and `RunReq` `allRoundtripCases()` rows. |
| `cmd/tether/agent.go:26-33, :168-178` | Add `remoteFSConfig` + `RemoteFS` field; wire the four values. |
| `cmd/tether/exec.go:67, :142` | Add `--safe` BoolVar; set `ExecReq.Safe`; `Long`-help flag-ordering note. |
| `cmd/tether/run.go:97, :277` | Add `--safe` BoolVar; set `RunReq.Safe`; `Long`-help note. |
| `cmd/tether/error_hints.go:105` | Add `runFailureReasons` entries for the five `remote_fs_*` / `too_many_wedged_spawns` reasons. |
| `docs/usage.md` | Troubleshooting subsection "Hung network filesystem makes run/exec hang" + `remote_fs auto/off` table + `--safe` (with the `SetInterspersed` ordering caveat) + the irreducible-limit paragraph. |
| `docs/architecture.md` (Part II roadmap tail) | One line recording the remote-fs-resilience leaf increment (additive `Safe` field, sanitize-then-resolve spawn, no proto bump). |
| `test/e2e/all_phases_test.go` | New `remotefs` subtest into the matrix (§6). |

---

## 6. Test plan (table-driven, adversarial, hermetic)

**Seam discipline (must-fix):** the repo deliberately uses count-based `assertNoGoroutineLeak`
(`test/concurrency/helpers_test.go`), **not** `goleak`. A count-based check fails on a genuinely-abandoned
D-goroutine, so all hermetic tests use a **channel-fake `ProbeFn`/start seam the test RELEASES at cleanup** so
the abandoned goroutine actually exits and the count returns to baseline. **No real FIFO** (`open()` blocks but
`os.Stat` returns immediately — wrong hang shape — and risks wedging the runner on darwin); the LookPath/statfs
hang is simulated purely by a `ProbeFn` that blocks on a test-controlled channel. **No real `/proc`, no real
NFS — runs identically on the macOS gate.**

| Test | Proves | Adversarial angle | Hermetic how |
|---|---|---|---|
| `TestParseMountinfo_table` | Parser handles octal-escaped mountpoints (`\040` space, `\011` tab), the variable optional-fields block before ` - `, bind mounts, `fuse.sshfs`; longest-prefix respects `/` boundaries. | Ugly real lines incl. a captured timan1 line; `/data` vs `/database` prefix collision; truncated/garbage line must not panic. | Pure func over a `[]byte` literal. Zero syscalls; identical on darwin. |
| `TestSanitizePATH_dropsUnhealthyHangable` | Auto drops only unhealthy-hangable entries; off drops none; local fallback appended; other order preserved; **healthy** hangable kept. | PATH whose FIRST entry is the dead nfs dir (exact timan1 ordering) — dropped AND never stat'd; an UNKNOWN fstype entry kept (unknown⇒local). | Fake `MountSource` literal + `ProbeFn` returning false for the nfs mountpoint. |
| `TestResolveArgv0_neverWalksDeadDir` (headline, `-timeout`) | Bare-name resolution never stats a hangable dir; absolute argv[0] on an unhealthy mount **fails-fast**, never hangs, **never silently rewrites** to a same-basename local binary. | `ProbeFn` for the nfs mount **blocks forever** on an unbuffered channel — if resolution ever probes the dropped dir the test deadlocks (caught by `go test -timeout`). Plant a local `/usr/bin/python`; assert `/shared/nas/bin/python` on the dead mount returns `remote_fs_unhealthy`, NOT the local path. | Blocking-channel fake; `-timeout` turns a regression into a hard CI failure. |
| `TestRunWithDeadline_abandonsAndCeiling` (`-race`, `assertNoGoroutineLeak`) | A start blocking past deadline returns `remote_fs_spawn_timeout`, abandons (no handler deadlock); the (N+1)th past the ceiling fails fast `too_many_wedged_spawns`. | Fake start blocks on a channel the test **closes at cleanup** (count recovers, gate passes); spawn N>ceiling; assert the agent handler still publishes the `ExecChunk{stderr}` timeout note (warning reaches ctl though child is wedged). | Start behind a func var; channel-fake. No NFS. |
| `TestStickyProbe_singleFlight_selfHeals` (`-race`) | A dead mount is probed **once** (single-flight under N concurrent callers); zero new probe goroutines until either a generation change OR the outstanding goroutine's late success flips it back to healthy (finalizer §0.1). | Hammer `IsHealthy` from N goroutines on the same dead mount → exactly one probe; then make the in-flight fake return success late → verdict flips to healthy with no new probe. | Counting `ProbeFn`; gen-change simulated by swapping fake `MountSource` content; late-success via the buffered chan. |
| `TestSafeDir_picksLocalWritable` | Skips a hangable-mount candidate / read-only; honors override only if local+writable; falls back to `os.TempDir()`. | `safe_dir` override points at the fake-nfs mountpoint ⇒ ignored + warning, not silently used. | Fake `MountSource` marks override mountpoint hangable; real touch in `t.TempDir()`. |
| `TestLivenessSnapshot_degradesOnHangableHome` (§3.I) | `buildLocalSnapshot` returns a degraded (in-memory procs, empty ports) snapshot within deadline when `Home`'s `state.json` read blocks — re-register does NOT wedge. | Fake `MountSource` marks `Home` hangable + a blocking read seam; assert return within deadline + a subsequent healthy read repopulates ports. | Inject the state-read seam + fake prober; no real hung fs. |
| `TestSafeReqRoundTripsBrokerBothVerbs` | `Safe:true` survives the broker Unmarshal→stamp-ActorFP→Marshal forward on **exec AND run**; legacy req (no `safe`) decodes to `Safe=false`; `ProtoVersion` stays 1. | Marshal `Safe:false` ⇒ assert `safe` key **absent** (byte-identical to v0.3.2; clone `expose_remote_port` omitempty test); run the real broker forward, assert `Safe==true` downstream AND `ActorFP` overwritten. | Embedded NATS + real `handleExecReq`/`handleRunReq`; capture the `.forwarded` body. |
| `TestRemoteFS_inertWhenNoHangableMounts` (e2e matrix) | All-local machine ⇒ byte-identical to today: PATH unchanged, output identical, zero probe goroutines. | In-process agent with all-local fake prober; `exec echo hi` + `printenv PATH`; diff vs a non-safe baseline byte-for-byte. | In-process agent (`test/p4` pattern) + injected all-local prober via the new Config seam. Added to `all_phases_test.go` as `remotefs`. |
| `TestRemoteFS_inertOnDarwinStub` | Non-Linux path is byte-identical to today (plain resolution, no warnings). | Guards against a silent darwin regression on the macOS gate. | Force the inert (zero-hangable) classification; assert resolved path == plain `exec.LookPath` result. |
| `TestAgentYAML_remoteFSEnum` | `remote_fs` absent⇒auto; `auto`/`off` accepted; anything else ⇒ startup error; nested typo (`remote_fs: {moed: auto}`) rejected by `KnownFields`. | Case (`AUTO`), `yes`, `''`, misspelled top-level `remotefs:`, misspelled nested key. | Raw-yaml here-doc strings via the agent.yaml loader; no broker. |

Concurrency gates per CLAUDE.md §5: `TestResolveArgv0_neverWalksDeadDir`, `TestRunWithDeadline_abandonsAndCeiling`,
`TestStickyProbe_singleFlight_selfHeals`, and a `test/concurrency/` entry exercising concurrent
`SanitizePATH`+`ResolveArgv0` during a generation-change snapshot swap, all under `-race` + `assertNoGoroutineLeak`.

---

## 7. Implementation order (smallest shippable first)

1. **`internal/spawnsafe` package + unit tests** (A–E + `RunWithDeadline`). Pure/seamed; no agent wiring yet.
   Lands the headline mechanism + its hermetic adversarial tests in isolation.
2. **Proto + broker round-trip:** add `Safe` to both reqs, the `allRoundtripCases` rows, the omitempty
   byte-identical test, the broker forward test. Pins wire safety before any handler depends on it.
3. **exec wiring** (`exec.go` + `pty.go` resolved-path param) + the exec watchdog/ceiling + ctl `--safe` +
   `error_hints`. **exec before run** because run depends on the `pty.Start` signature change and is the larger
   handler; exec proves the resolve+watchdog path end-to-end first.
4. **run wiring** (`run.go` resolve + start-window deadline + fail-fast reasons + ctl `--safe`).
5. **Agent-liveness hang-safety** (§3.I: classify `Home`, guard `buildLocalSnapshot`/`applyReconciliation`
   reads, reorder `New()`). Same PR — the lifeline is incomplete without it; classified-inert on the local-Home
   fleet.
6. **agent.yaml/config + docs + e2e matrix subtest**, then the full hard gate (`make test` + `make e2e` +
   `make lint` + `-race`/leak on concurrency surfaces).

---

## 8. Why `run` is in scope (not deferred)

The root-cause `LookPath` hang is **identical** at `pty.go:77`; an exec-only ship leaves `tether run bash`
hanging on the same syscall. run additionally has the **only** unbounded post-exec gap (no agent-side deadline
anywhere). The marginal cost is one shared resolver + one `Safe` field — both handlers are already structurally
parallel (`dispatchForwarded:57-59`).

---

## 9. Upgrade / migration

- Additive `Safe bool omitempty` ⇒ default wire byte-identical to v0.3.2 ⇒ `ProtoVersion` stays 1 ⇒ **no
  reinstall**, normal `tether node upgrade` path (J.4).
- `remote_fs` key absent in existing `agent.yaml` ⇒ `auto` ⇒ inert on healthy/local-Home agents ⇒ no behavior
  change on upgrade for the current fleet.
- Mixed-version skew during a rolling upgrade: an old agent ignores `Safe` (unknown JSON key dropped) and runs
  the legacy path — i.e. `--safe` silently no-ops against an un-upgraded agent. Documented as a known rollout
  limit (same class as P12 `--remote-port` against an old broker); no capability negotiation.
- **Downgrade caveat (review n1):** a `remote_fs:` block in `agent.yaml` must be **removed** before downgrading
  to a pre-v0.3.3 binary — the old loader uses `KnownFields(true)` and refuses to boot on an unknown top-level
  key. Same accepted class as every prior additive yaml block (`file_transfer`, `proxy`). Noted in usage §7.7.

---

## 10. Version

**v0.3.3** (patch). Unlike transfer-unrestrict (v0.4.0, which flipped a **security default**), this flips **no**
default — `auto` is byte-identical to today on healthy machines, the wire is one additive omitempty bool, no
reinstall.

---

## 11. Open decisions for the human owner (external review confirm list)

Each carries the finalizer recommendation; confirm or override at external review.

1. **Default mode `auto` vs `off`.** Finalizer: **`auto`** — the value is a first-outage lifeline with no
   pre-config; `auto` is provably inert when healthy (one boot mountinfo scan, zero steady-state probes,
   test-pinned). `off`-default is useless exactly when needed. **Recommend `auto`.**
2. **Probe timeout 2s & wedge ceiling 64** (both `agent.yaml`-tunable). Finalizer raised the timeout from the
   candidate's 800ms to **2s** for false-positive headroom (timan `timeo=600` ⇒ a real hang never answers
   anyway), paired with sticky-**self-healing** so even a false positive auto-recovers on the late probe.
   **Recommend 2s / 64.**
3. **run start-window deadline source.** run has no ctl `--timeout`. Finalizer: a generous **agent-side default
   (30s) for the execve start window only** (never the child lifetime), `agent.yaml`-tunable. A ctl
   `run --timeout` is a separate, larger UX change deferred to its own leaf. **Recommend 30s agent-side.**
4. **Version v0.3.3 (patch).** **Recommend v0.3.3**, or bundle into the next minor if you prefer batching.
5. **`--safe` flag ordering footgun.** `SetInterspersed(false)` ⇒ `tether exec node --safe …` sends `--safe`
   to the child. Finalizer: **document loudly + help example** (matches the existing `--cwd`/`--timeout`
   constraint); optionally also detect a leading literal `--safe` in the post-node argv and error with a hint.
   **Recommend doc-only (+ optional detection).**
6. **Darwin inertness.** Confirm macOS agents are dev/CI only and it is acceptable that the feature is fully
   inert (zero hung-mount protection) on darwin — stated as a known limit (§2).
7. **§3.I (agent-liveness guard) in this PR vs follow-up.** Finalizer: **this PR** — same root cause, same seam,
   classified-inert on the local-Home fleet (zero blast radius there), and the lifeline is hollow without it
   for NFS-Home installs. Flag for awareness since it touches the reconcile path.
