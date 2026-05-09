# Security Audit

Scope: `internal/auth`, `internal/authcallout`, `internal/agentprov`,
`internal/broker/{upgrade,admin,expose,exec,run,sessions,authcallout,
audit,reconcile,broker,disk}.go`, `internal/agent/{upgrade,expose,
exec,run,state,agent}.go`, `internal/adminsock/*`, `internal/proto/*`,
`internal/cli/identity.go`, `cmd/tether/{node,login,expose,admin}.go`.
Read-only review; nothing modified.

## Verdict
6 findings (0 critical, 1 high, 2 medium, 3 low/nit).

The B.1 actor-from-subject invariant is consistently honored — every
broker handler reads `actor` from `ParseCmdBy` / `ParseCtrlBy` (never
from the body) and immediately re-derives `fp = FingerprintFromActor`.
The owner / member / creator gates are present on every mutating
verb (exec, run, expose, expose-rm, upgrade, session.rm). PIN is
argon2id with constant-time compare; never logged; never echoed in
errors. Raw expose tokens never touch SQLite (only sha256 hex);
auth_callout validates sid/nid format, agent identity binding is
TX-protected with INSERT-OR-IGNORE + re-read for race resolution.

The findings below are mostly defense-in-depth / hardening rather
than reachable v1 bypasses.

## Findings

### F1 — [high] Upgrade tarball is buffered fully in memory then handed to host `tar`
**Where**: `internal/agent/upgrade.go:71` (`fetchURL` → `io.ReadAll`),
`installNewBinary` (line 205).
**Threat model**: Session OWNER (or anyone able to reach the
forwarded subject — e.g. broker compromise). Already trusted, so
this is hardening, not a bypass.
**Issue**: `fetchURL` reads the entire HTTP response into a `[]byte`
with no size cap; `installNewBinary` then writes the whole tarball
to a tmp file and shells out to host `tar -xzf`. A malicious or
mis-configured allowlisted URL could:

1. serve gigabytes → agent OOM (no `MaxBytesReader` / no
   `Content-Length` check; only `upgradeFetchTimeout = 30s`,
   which against fast bandwidth is plenty for >1 GiB);
2. serve a tarball whose extracted contents fill `/tmp`
   (`os.MkdirTemp(filepath.Dir(dst), ...)` puts it in the broker
   binary's directory — usually `/usr/local/bin` or `/home`);
3. serve a tarball with a path-traversal entry (`../../etc/foo`).
   Host `tar` (GNU tar ≥ 1.14, BusyBox tar) refuses `..` by
   default, but the agent doesn't probe / pin a known-safe
   extractor. A non-default tar (or a tar configured by a
   compromised PATH) could write outside `tmpDir`.

**Impact**: DoS of any agent reachable by an upgrade URL. The
attacker still needs broker-owner privilege to call `node upgrade`,
so this only widens the post-compromise blast radius (one rogue
URL crashes every agent in the fleet at once via `--all`).

**Fix**:
- Cap `fetchURL` body with `io.LimitReader` at a configurable max
  (e.g. 100 MiB, matches v1 binary size budget).
- Reject the tarball before extraction if any entry name contains
  `..` or starts with `/`. Easiest: switch from `exec.Command("tar",
  ...)` to `archive/tar` + `compress/gzip` and validate `hdr.Name`
  inline. The agent already imports neither, but the dependency
  cost is well under the security benefit.
- Optional: write `tmpDir` to `os.TempDir()` instead of
  `filepath.Dir(dst)` so a fat extraction doesn't fill the binary
  partition.

---

### F2 — [medium] `os.Executable()` upgrade target follows symlinks; tarball overwrite races a concurrent process
**Where**: `internal/agent/upgrade.go:91` (`os.Executable()`),
`installNewBinary:229` (`os.Rename(binPath, dst)`).
**Threat model**: Local user with write access to the directory
holding the agent binary, OR a session owner pointing the agent
at a binary path under their own control via a pre-deployed
symlink.
**Issue**: `os.Executable()` returns the resolved path of the
running binary on Linux (`/proc/self/exe` readlink). If the
operator deployed `tether` as `/usr/local/bin/tether → /opt/
foo/tether-v1`, the rename targets `/opt/foo/tether-v1`. That's
usually fine, but combined with `os.MkdirTemp(filepath.Dir(dst),
...)`: the attacker who can write to `/opt/foo/` (e.g. user `foo`
who owns that dir) can:
- pre-place a malicious tarball / pre-bind `tether-v1` to a hardlink;
- watch for the `.tether-upgrade-*` tmp dir to appear and race
  the `os.Rename`.
On the same filesystem, `os.Rename` is atomic at the kernel level,
so the race is small. But `defer os.RemoveAll(tmpDir)` runs even
on success — if an attacker symlinks `tmpDir/tether` to
`/etc/passwd` between extract and chmod, the chmod hits passwd.

**Impact**: With write access to the binary directory, an attacker
can force the agent to overwrite arbitrary files via well-timed
symlink swaps. Requires local-user-on-agent-host capability — the
v1 threat model already considers root-on-agent fully compromising,
but a non-root local user crossing a privilege boundary via this
path IS in scope.

**Fix**:
- After `os.Stat(binPath)`, use `os.Lstat` and refuse if mode
  contains `os.ModeSymlink`.
- Open the file with `O_NOFOLLOW` before `os.Chmod` (use
  `unix.FchmodAt` with `AT_SYMLINK_NOFOLLOW` if available; or open
  read-only with NOFOLLOW first, then chmod the fd).
- Lock down `tmpDir` to mode 0700 (currently inherits umask;
  `MkdirTemp` defaults to 0700 on Linux, but the *contents* —
  written via `os.WriteFile(tarPath, …, 0o600)` — are still
  attackable if `filepath.Dir(dst)` itself is world-traversable).

This isn't reachable by remote attackers, only local — keep at
medium.

---

### F3 — [medium] `agent_provisioned` re-connect skips post-tombstone session reads from `Lookup` cache; first-write race on (sid, nid)
**Where**: `internal/agentprov/agentprov.go:65` (`Provision`),
`agentprov_test.go` confirms the race window.
**Threat model**: Two agents started concurrently, both with
`--pin <correct PIN>`, both claiming the same `(sid, nid)`.
**Issue**: `Provision` is `INSERT OR IGNORE` then re-read; if N=0
and `existing != agentFP` returns `ErrAlreadyProvisioned`.
That's correct — the LATER caller loses. But `ProvisionWithPIN`
(line 104) wraps it in a tx that first reads `pin_hash, state`
from `sessions` then INSERTs. SQLite default is deferred
transactions: the read takes a SHARED lock; the INSERT promotes
to RESERVED. Two concurrent ProvisionWithPIN-with-different-fp
calls will serialize at the INSERT step (one wins, one sees
"database is locked" or the IGNORE+no-op path). The IGNORE-then-
re-read path then correctly returns ErrAlreadyProvisioned. So
**no race-induced data corruption** — but the loser sees the
generic "agent (sid=…, nid=…) is bound to a different agent
identity" message even though they were the legitimate first
caller (just lost the SQLite race by µs).

**Impact**: Recoverable UX bug, not a security finding by itself.
However, `auth_callout/handler.go:259` deliberately maps
`ErrAlreadyProvisioned` from a brand-new PIN-bootstrap to the
SAME message as a stale-fp re-CONNECT. An operator who's just
lost a race can't tell whether they typo'd the PIN or whether
they hit the race — they'll think the PIN was accepted by
someone else and rotate it (architecture H.1 calls
`rotated_pin` an audit event, but no immediate revocation).
That's an operational annoyance, not an attack vector.

**Fix** (nit-level): in the tx-aware `ProvisionWithPIN` re-read
path, distinguish "row pre-existed" vs "row inserted by my tx
then collision" — only the latter is a race. Surface that as a
distinct error so the caller can prompt "concurrent provisioning
detected; retry once". v1 can keep the current behavior; the
security invariant (first-write wins, no fp swap) holds.

---

### F4 — [low/nit] PIN passed via `--pin <pin>` command-line flag is in /proc/<pid>/cmdline
**Where**: `cmd/tether/login.go:64`, `cmd/tether/agent.go` (and
the `agent.PIN` field path), `nats.Token(pin)` in
`agent/agent.go:593`.
**Threat model**: Local user on the same host as the CLI / agent
process can `cat /proc/<pid>/cmdline` and read the PIN out of
flight.
**Issue**: PINs are short-lived (one-shot bootstrap), but the
process can exist for seconds while NATS round-trips. On a
multi-tenant host, that's a window. There's no `--pin-file` /
`--pin-stdin` alternative.
**Impact**: A local attacker on the same machine can grab a PIN,
then race to use it before the legitimate join consumes it.
(A PIN is single-use only as a bootstrap — once a member row is
created, the same PIN keeps working until rotation. So the window
is "until the operator rotates the PIN".) v1 user said "security
pragmatic" so this is acceptable; documenting as nit.
**Fix**: optional `--pin-file <path>` / `--pin-stdin` flag in
later work. No code change for v1.

---

### F5 — [low] Admin socket parent directory chmod fails closed but doesn't verify ownership
**Where**: `internal/adminsock/server.go:78-88`.
**Threat model**: Operator pre-creates `/var/run/tether/` with
mode 0755, owned by another uid. Broker comes up.
**Issue**: `MkdirAll(parent, 0o700)` is a no-op since dir exists.
`os.Chmod(parent, 0o700)` is then attempted. **If the broker is
not the owner** of `/var/run/tether/`, the chmod fails and
`Start` returns an error → broker exits. Good. **If the broker
IS the owner** but the parent directory is on a filesystem the
broker can't chmod (e.g. tmpfs with noexec), same result.
**However**: there's no check that the parent dir was already
owned by the broker uid before chmod. A malicious local user
who managed to create `/var/run/tether/` with their own uid +
mode 0700 + a symlink for `admin.sock` pointing somewhere
sensitive could survive past the chmod (because chmod 0700 of an
already-0700 dir succeeds for the owner).
**Impact**: Local privilege escalation requires winning a race
with a broker process that hasn't started yet AND owning a path
inside `/var/run` — both unlikely in v1's deployment model
(install.sh creates the dir as the tether user). Marking low.
**Fix**: after MkdirAll, `os.Lstat(parent)` and verify
`Sys().(*syscall.Stat_t).Uid == os.Geteuid()`. Refuse if not.

---

### F6 — [low] `tunnel.tunnelTokenLookup` returns generic "token_unknown_or_revoked" without distinguishing port-mismatch from absent-row
**Where**: `internal/broker/expose.go:50` (`tunnelTokenLookup`),
`internal/tunnel/tunnel.go:175` (broker writes the error string
verbatim back to the agent over plaintext-after-TLS).
**Threat model**: An attacker who has stolen a valid expose token
for one (sid, nid) tries it against a different (sid, nid).
**Issue**: The lookup uses `state='ALLOCATED'` only, so REVOKED /
FREED tokens look identical to never-existed. That's *correct* (we
deliberately don't disclose token validity over the unauth tunnel
control conn). But the error code "token_port_mismatch" IS
disclosed when the row exists and only port differs — gives a
small confirmation that the token is real. Not exploitable in v1
(an attacker who has the raw token already won). Marking nit.
**Impact**: Marginal information disclosure to a network attacker
who's already man-in-the-middled the agent→broker tunnel TLS.
**Fix**: collapse both error returns into `"register_denied"` so
the agent log line doesn't reveal which check failed. Keep richer
detail in the broker-side `s.logger.Info` only.

---

## Notes (considered, not flagged)

- **B.1 actor-from-subject invariant**: every handler obeys it.
  `ParseCmdBy` / `ParseCtrlBy` deliver `actor` directly to
  `FingerprintFromActor`; bodies never carry actor as input.
  `req.NID` is cross-checked against subject `nid` in
  `handleRegister` (line 496-499). ✓

- **TOCTOU IsActive → command forward**: in the rare interleaving
  where `Tombstone` runs after `IsActive` returns true and before
  the forward, the agent runs the command (≤ a few ms window).
  This is documented in C.1 §6 as accepted. Audit.call still
  fires. Not a security issue per architecture spec.

- **Path traversal via sid/nid**: `state.go:48-50` uses
  `filepath.Join(home, "agent", sid, "state.json")`. SID is
  validated by `proto.ValidateSID` in `agent.New()` (matches
  `[a-z0-9-]{1,32}` AND leading lowercase alpha) — `..` / `/` are
  syntactically rejected. Same for `cli.EnsureAgentIdentity`.
  ✓

- **PIN secret in logs / error returns**: searched all `Logger.*`
  / `Error: ...` paths. PIN values never appear; `pin_invalid:
  <err>` only carries the constant strings from `ValidPIN`. nkey
  seeds are wiped after use; never logged. Raw expose tokens are
  forwarded to agent over a per-connection inbox and then live
  only in agent's state.json (mode 0600). ✓

- **Owner/member gates**: every mutating broker verb gates on
  `IsActive` + `IsMember` (or `IsOwner` for upgrade /
  session.rm). `expose-rm` has the F.8 creator-OR-owner gate.
  No path skips them. ✓

- **JWT permissions pin actor and sid in subject template**: the
  `PermissionsForActivatedMember(actor, sid)` template hard-codes
  both — NATS rejects pubs to other-actor/other-sid subjects
  before they ever reach the broker subscription. So even if a
  broker handler skipped `ValidateSID`, a malformed sid couldn't
  arrive (the nkey JWT only allows that connection's specific
  sid). ✓

- **Defense-in-depth URL allowlist on agent**: agent has its own
  default allowlist + checks `urlAllowed` before download. ✓

- **Disk-pressure monitor**: edge-triggered, no event spam. Not a
  security path. ✓

- **Admin socket peer credential**: relies on filesystem perms
  (0700 dir / 0600 socket), no SO_PEERCRED check on accept. The
  user feedback memo says "v1 nicht für理论攻击链" — the dir
  permissions are sufficient against the documented threat
  model (non-root local users get EACCES at connect). Not flagged.

- **ctl trusts broker**: `expose.go` ctl renders `resp.PublicHost`
  / `resp.Port` directly. Architecture explicitly designates broker
  as the trusted root. Not a finding.

- **`unregister.req` permission granted but no broker handler**:
  agent JWT allows publishing `ctrl.s.<sid>.node.<nid>.unregister.
  req`, but `broker.go` never `nc.Subscribe`s it. Dead permission;
  not a security issue (no handler = no effect).

- **`MkdirTemp` umask race on socket file**: `net.Listen("unix",
  path)` creates the file with mode `0666 & ~umask`. The
  immediate `os.Chmod(s.path, 0o600)` (line 108) tightens it.
  The microsecond window is not exploitable in practice. ✓
