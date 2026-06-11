# Transfer Unrestrict (push/pull allow_roots → optional narrowing) — Implementation Plan

Date: 2026-06-11
Status: finalized (main process is sole author; drafted via 9-agent adversarial workflow, then overridden where noted)
Target release: v0.4.0 (recommended — see §Version)
Supersedes: the `allow_roots`-mandatory / `empty==disabled` / `/`-unsupported decision in `file-transfer-plan.md` §"Refusing dangerous paths".

## Background + threat model

`tether push`/`pull` today require `file_transfer.allow_roots` to be **non-empty** or the
agent rejects every transfer with `transfer_disabled`
(`internal/agent/transfer.go:628-632` write, `:690-694` read).
`scripts/install.sh:346-351` writes an `agent.yaml` with only
`broker_url/session/nid/tunnel_addr` — **no `file_transfer` block** — so **every fresh
install ships with push/pull DISABLED**. An operator must hand-edit yaml to use it at all.

Per `docs/requirements.md` §9.3: same-session members have **unrestricted run/exec** over
every node, with **zero per-member isolation**. A member blocked by `allow_roots` bypasses
it in one line:

```
tether run a100 -- cp /etc/passwd /tmp/x   # /tmp was a default allow_root anyway
tether pull a100:/tmp/x ./x
# or just: tether run a100 -- base64 /etc/passwd
```

Confirmed: `run`/`exec` have **zero** path containment. Therefore `allow_roots` is **not a
security boundary against the authenticated member** — it only adds friction (and, with the
empty-install default, makes push/pull dead on arrival).

**Decision (made by the user, not relitigated):** push/pull default to **whole-filesystem
reach** (= exactly what `run`/`exec` already touch); `allow_roots` becomes an **optional
narrowing**. **Mechanism-level hardening stays unconditional in every mode** — it defends a
*different* principal (a hostile **non-member** process co-resident on the agent host), a
threat this relaxation does not touch.

## Decision

### Config semantics — option 1c (nil vs explicit `[]`), single-axis

`gopkg.in/yaml.v3` (the repo's lib) distinguishes "key absent" (slice stays `nil`) from
"`allow_roots: []`" (non-nil len-0 slice). The discriminator is `AllowRoots != nil`,
captured at the yaml boundary in `cmd/tether/agent.go`.

| Config (`agent.yaml`) | Mode | Behavior |
|---|---|---|
| `file_transfer` block / `allow_roots:` key **absent** or `null` (incl. fresh install) | **Open** | whole-FS reach (= run/exec); skip containment; **all mechanism checks still run** |
| `allow_roots: [/a, /b]` (non-empty) | **Narrow** | `containedIn` prefix check; `path_outside_roots` on miss (unchanged) |
| `allow_roots: []` (explicit empty list) | **Disabled** | `transfer_disabled` before any path work |

**Rejected 1a** (`len(canonRoots)==0 → open`): conflates nil/`[]`, destroys the upgrade-safe
off idiom, and is a **fail-open footgun** — `CanonAllowRoots` silently drops malformed roots
(`transfer.go:545-582`), so a typo'd or transiently-unmounted narrow root would collapse
`len==0` and silently fall to whole-FS open.
**Rejected 1b** (`allow_roots: ["/"]` as the *default sentinel*): it leaves fresh installs
disabled unless install.sh writes the sentinel and overloads an ordinary path value with a
mode switch. Absence remains the canonical Open expression. External review also fixed the
pre-existing root-prefix bug, so `allow_roots: ["/"]` is now accepted as an ordinary
non-empty Narrow configuration whose single prefix happens to cover the whole filesystem.

### Off-switch — single-axis, NO new `enabled` key (override of candidate)

The 9-agent candidate (and the security/synthesis agents) proposed **also** adding
`file_transfer.enabled: false`. **I override that: keep a single axis.** `allow_roots: []`
*is* the off-switch.

Rationale for the override:
- The off-switch justification is **not** security — `run`/`exec` are un-disableable, so a
  member always has filesystem reach. The only honest rationale is JetStream object-store
  disk pressure, and even that is **partial**: `enabled:false` would stop the *agent-side*
  byte landing but **not** the broker pre-creating the tier-B bucket. Don't oversell a knob
  that doesn't fully close the resource it claims to.
- `allow_roots: []` already expresses "off" and is the **exact existing documented v0.2.x
  semantics** (`usage.md:255`). Keeping it means an operator who deliberately disabled
  push/pull historically stays disabled verbatim across upgrade — the upgrade-safety anchor
  falls out for free.
- A second axis creates a 2×3 matrix with ambiguous corners (`enabled:false` +
  `allow_roots:[/tmp]` — which wins?) and more test surface, for marginal benefit.

So: **one field, three meanings.** `nil → open`, `[] → disabled`, `[list] → narrow`. This is
listed as a confirm-at-external-review item (§Open questions #2) in case the user wants the
discoverable `enabled` name after all.

### Mode plumbing — explicit `transferMode` enum, decided from RAW config presence

The single most important security invariant: **mode is decided from the RAW config (whether
the key was present, and its raw element count), NEVER from `len(canonAllowRoots)`.** An
explicitly-narrow config whose roots **all drop** during canonicalization
(`RootsConfigured==true`, raw `len>0`, canon empty) must stay **Narrow → reject-all**
(`path_outside_roots` on every path), and must **never** silently become Open.

New unexported enum in `internal/agent` (e.g. `transfer.go`):

```go
type transferMode int
const (
    modeOpen     transferMode = iota // nil allow_roots → whole-FS, mechanism checks still run
    modeNarrow                       // non-empty allow_roots → containedIn
    modeDisabled                     // explicit allow_roots: [] → transfer_disabled
)
```

Derivation (in `agent.New`, from the raw `cfg.AllowRoots` + a `cfg.RootsConfigured` bool set
at the yaml boundary, **before** `CanonAllowRoots`):

```go
mode := modeOpen
if cfg.RootsConfigured {                 // key was present in yaml
    if len(cfg.AllowRoots) == 0 {        // explicit []  → off
        mode = modeDisabled
    } else {                             // non-empty raw → narrow (even if canon drops them all)
        mode = modeNarrow
    }
}
canon := CanonAllowRoots(cfg.AllowRoots) // stays a no-op for open/disabled; reject-all if narrow+all-dropped
```

`ValidateForWrite`/`ValidateForRead` take a new `mode transferMode` param and branch FIRST on
it:
- `modeDisabled` → `transfer_disabled` (unchanged code + reworded message).
- `modeOpen` → run the IDENTICAL mechanism chain (abs, EvalSymlinks-parent, parent-exists,
  leaf-lstat-symlink, regular-file) but **skip only the `containedIn` step**; return
  `ValidatedPath{Abs: abs, AllowRoot: ""}`. **No early-return shortcut** — falling through the
  same chain is what keeps symlink/TOCTOU defenses alive.
- `modeNarrow` → `containedIn` exactly as today; empty canon ⇒ matches nothing ⇒
  `path_outside_roots` on everything (fail-closed).

## Mechanism hardening kept (UNCONDITIONAL in all three modes)

Open mode skips **only** `containedIn`/`path_outside_roots`. Everything else is identical:

- Absolute-path requirement (`path_not_absolute`) — `transfer.go:633-636` / `:695-698`.
- `EvalSymlinks(parent)` before use — `:639-647` / `:701-709`.
- Parent-must-exist, **no auto-mkdir** (`path_parent_missing` write / `path_not_found` read)
  — blocks materializing arbitrary trees (e.g. `/etc/cron.d/...`) as the agent UID.
- Leaf-lstat symlink reject (`not_a_regular_file`) — `:657-665` / `:723-726`; the **primary**
  non-member defense once containment is gone.
- Regular-file-only (`not_a_regular_file` for dir/dev/fifo/socket) — `:662-665` / `:727-730`.
- Canonical parent components are opened one-by-one with
  `openat(O_DIRECTORY|O_NOFOLLOW)` and retained through write commit, so parent replacement
  cannot redirect a validated operation.
- `O_NOFOLLOW|O_EXCL|O_CREAT` write tmp (`OpenForWriteAtomic`).
- `O_RDONLY|O_NOFOLLOW` + dev/inode TOCTOU recheck (`OpenForReadAtomic`, `path_race`).
- Atomic tmp+fsync+`renameat` for force, or atomic no-clobber `linkat` for no-force
  (`RenameForWriteAtomic`); the latter closes the old Lstat+Rename overwrite race.

`CanonAllowRoots` keeps its drop-on-error behavior; only its caller's interpretation changes
(all-dropped narrow → fail-closed, decided upstream).

## Files Touched

| File | Change |
|---|---|
| `docs/usage.md` | **DOC-FIRST.** Flip the config table row + error-code table + §5.10/§6.7: `allow_roots` absent → whole-FS (= run/exec); non-empty → narrowed; `[]` → disabled. Add an **Upgrade / behavior-change callout** (the disabled→open posture flip). Fix the pre-existing `≤2 GiB` vs real tier-B ceiling wording while here (note only; don't change the limit). |
| `docs/reviews/file-transfer-plan.md` | Append a short **superseding note** (annotate, don't delete history): the original "allow_roots mandatory, empty==disabled, `/` unsupported" decision is reversed by this plan. |
| `docs/architecture.md` | Minimal: there is **no** existing allow_roots section (only §947 historical "v1 不做" + §2248 increment list). Add one line near §2248 recording that the transfer-unrestrict increment makes `allow_roots` an optional narrowing (not a security boundary; cross-ref requirements §9.3). |
| `internal/agent/transfer.go` | Add `transferMode` enum. `ValidateForWrite`/`ValidateForRead`: new `mode` param, branch-first as above. Call sites `:66` (push) / `:314` (pull) pass `a.transferMode`. Update both func doc-comments + the `CanonAllowRoots` doc (dropped roots shrink the narrow list but never fall to open). Note `ValidatedPath.AllowRoot` is `""` in open mode (and has zero readers today). |
| `internal/agent/agent.go` | `Config`: keep `AllowRoots []string`; add `RootsConfigured bool`. Rewrite the `AllowRoots` doc-comment (`:130-142`) to the tri-state. `New()` (`:265`) derives + stores `transferMode` next to `canonAllowRoots`. Add a **set-once WARN** when mode resolves to `modeOpen` with no `file_transfer` block: `"file transfer: whole-filesystem reach (no file_transfer.allow_roots configured); set allow_roots to narrow or allow_roots: [] to disable"` — a runtime signal at the posture change. |
| `cmd/tether/agent.go` | `fileTransferConfig` (`:43-45`): keep `AllowRoots []string`; rewrite the comment (`:39-42`) to the tri-state. At cfg build (`:138`): `RootsConfigured: ay.FileTransfer.AllowRoots != nil` alongside `AllowRoots: ay.FileTransfer.AllowRoots`. |
| `internal/proto/messages.go` | **Doc-comment only, no wire change.** `ProtoVersion` stays 1 (broker never validates these codes — confirmed; `Code` is free-form). Keep `transfer_disabled` (now = "explicitly off") + `path_outside_roots` (narrow mode) in the Code lists at `:617/:651/:690`; soften the `Path` comments from "must be under allow_roots" to "absolute; under allow_roots when narrowing is configured". |
| `scripts/install.sh` | Keep the generated `agent.yaml` **silent** on `file_transfer` (silence = open = the goal). Add a **commented** example block after `tunnel_addr` showing narrowing (`# allow_roots:` with real `# - /srv/data` dirs) and, as prose, `# allow_roots: []  # explicit empty list disables push/pull entirely`. No copy-pasteable bare `allow_roots: []`. |
| `cmd/tether/transfer.go` | Soften push/pull `Long` help (`~:50-53`, `~:324`): "must be absolute; by default any path the agent user can reach, optionally narrowed by file_transfer.allow_roots". Keep symlink/regular-file sentences verbatim. |

## Tests (adversarial, table-driven)

`ValidateForWrite`/`ValidateForRead` gain a `mode` param so unit tests exercise all three
modes in isolation. The e2e harness `startAgent(...func(*agent.Config))` gets two mutators:
`withTransferDisabled()` (sets `RootsConfigured=true`, empty `AllowRoots`) and reuse of
`withAllowRoots(...)` (now also sets `RootsConfigured=true`); open mode = no mutator.

### Flipping existing tests (must change in this PR or the suite goes red)

| Test | File:line | Flip |
|---|---|---|
| `TestValidate_TransferDisabledOnEmptyAllowRoots` | `internal/agent/transfer_test.go:24` | Split into three: (a) `modeDisabled` → `transfer_disabled`; (b) `modeOpen` (nil roots) on a real regular file → SUCCEEDS, `vp.AllowRoot==""`, **and** mechanism checks still bite (symlink leaf → `not_a_regular_file`; relative → `path_not_absolute`; missing parent → `path_parent_missing`/`path_not_found`); (c) `modeNarrow` all-dropped → reject (`path_outside_roots`), NOT open. |
| `TestTransfer_TierA_TransferDisabledWhenAllowRootsEmpty` | `test/cli_e2e/transfer_test.go:128` | Rename → `..._OpenByDefaultPushSucceeds`: no-opt agent, push to a `t.TempDir()` path, assert `pr.OK==true` + bytes/sha land. Add sibling `..._OffSwitchDisables` using `withTransferDisabled()` → `transfer_disabled`. |
| `TestPullDuplicateIDRejection...` | `cmd/tether/transfer_compliance_test.go:9` | No functional flip — `transfer_disabled` stays a finalize-needing code (now from `[]`). Optionally broaden the list with `path_not_found`/`not_a_regular_file`. |

### New adversarial cases

- **Open-mode mechanism survival (headline):** `modeOpen` + pre-planted leaf symlink
  (`/scratch/out → /etc/passwd`) → `not_a_regular_file` (push AND pull). Proves a hostile
  non-member can't redirect a member's write with containment off.
- **Open-mode TOCTOU (`-race` + `goleak`, currently UNTESTED — `grep path_race *_test.go` = 0):**
  the in-call lstat→open swap isn't reachable single-threaded, so use a **concurrent** test:
  goroutine A `os.Rename`-loops two different inodes over the path; goroutine B loops
  `OpenForReadAtomic`, asserting every return is OK-with-matching-inode, `path_race`, or
  `not_a_regular_file` — **never a silent wrong-file read**. Under the concurrency gate
  (`goleak.VerifyNone` / `test/concurrency/`). Per CLAUDE.md §5 this gate is non-negotiable.
- **Fail-CLOSED on all-dropped narrow:** construct config the way `agent.New` does
  (`RootsConfigured=true`, raw `[/no/such, relative, <a regular file>]`) → resolves
  `modeNarrow` → push/pull to `/etc/anything` REJECTED. Must go through the mode-derivation
  path, not a pre-emptied slice literal (which bypasses the footgun being guarded).
- **Parent-not-auto-created in open mode:** push to `/nonexistent-tree/deep/f` in `modeOpen`
  → `path_parent_missing` **AND** `os.Stat(parentDir)` is `IsNotExist` afterward (assert NO
  directory created — the persistence-vector guard).
- **Non-regular target in open mode:** `mkfifo` then push-to / pull-from in `modeOpen` →
  `not_a_regular_file`; assert the special file was not consumed.
- **Open-mode clobber gate:** `RenameForWriteAtomic` with `ValidatedPath{Abs:<tempfile>,
  AllowRoot:""}`, no `--force` on an existing file → `dst_exists`; `--force` → overwrites.
  Pins that the clobber gate is mode-independent (guards against a future refactor gating it
  on `AllowRoot` presence).
- **Parent-component replacement:** validate a path, rename its parent away, replace the
  parent with a symlink to an outside directory, then assert read/write open returns
  `path_race` and creates nothing outside.
- **Tier-B verify-before-commit:** submit wrong-SHA and wrong-size objects with `force=true`
  against an existing destination; both must emit failed and leave the original bytes intact.
- **Strict config parsing:** misspelled `file_transfer` / `allow_roots` keys and a second YAML
  document must fail agent startup rather than silently selecting Open.
- **Narrow-mode regression (must stay green):** `TestValidateForWrite_RejectsDirSymlinkEscape`,
  `_AcceptsDirSymlinkInsideRoot`, `TestCanonicalAllowRoots_LongestWins/_DropsBad`,
  `TestWriteAtomic_DstExistsHonored` — all use explicit non-empty roots, now run `modeNarrow`,
  behavior unchanged.
- **yaml decoder table (upgrade pin)** in a new `cmd/tether/agent_config_test.go`: raw-yaml
  strings (NOT Go `[]string{}` literals) for `no_block / file_transfer:{} / allow_roots:(null)
  / allow_roots:~ / allow_roots:[] / allow_roots:[/tmp]` → assert `(RootsConfigured, len,
  resolved mode)`. Pins the exact `yaml.v3` nil-vs-`[]` quirk the whole design hinges on.
- **Upgrade-safety pin:** `allow_roots: []` → `modeDisabled` (codifies `[] == disabled` so a
  future refactor can't silently invert it).
- **auth_callout open-default (only harness catching JWT/ACL regressions):**
  `test/security/transfer_authcallout_test.go` — start agent with nil roots (open) under real
  auth_callout, **tier-B** push to a temp path + pull back, assert OK + sha. The existing two
  auth tests pass non-empty roots → still narrow → zero open coverage without this.
- **Fresh-install contract:** assert the `install.sh` here-doc contains NO active
  (non-commented) `file_transfer`/`allow_roots` key (reuse the existing install.sh dry-run
  test pattern in `test/p10/` / `test/security/path_traversal_test.go`).

### e2e matrix

`test/e2e/all_phases_test.go` does **not** wire the cli_e2e transfer subtests. Add a dedicated
subtest there that boots a fresh-install-style agent (no `file_transfer` block) and asserts an
open-mode push+pull round trip (bytes+sha match) plus an `allow_roots: [] → transfer_disabled`
case — so the default-inversion is in the `-tags e2e_matrix` cross-phase regression net.

## Upgrade / migration notes

- **Posture flip (the real cost):** every agent with no `file_transfer` block — i.e. 100% of
  fresh installs — goes **disabled → whole-FS open** on binary upgrade, with no config of
  theirs to grep. Mitigations: (1) `allow_roots: []` re-asserts off; (2) the set-once WARN
  startup log; (3) loud usage.md + release-notes callout; (4) threat-model justification
  (§9.3 — grants no new capability to a member). Non-empty narrow configs and explicit `[]`
  are **unchanged** across upgrade.
- **No wire/proto change → no reinstall:** `ProtoVersion` stays 1; broker never validates
  these codes; `AuditTransfer` schema unchanged (no `root` field — open-decision #5 closes as
  "no audit change"). Binary swap + restart suffices.
- **Error-string drift:** `transfer_disabled`'s human message changes ("allow_roots is empty"
  → "allow_roots: [] — push/pull disabled"); the `Code` value is stable. Note for anyone
  grepping logs.

## Version

Recommend **v0.4.0** (minor) with a top-of-release-notes BREAKING-DEFAULT banner: a
security-relevant default flipping from fail-safe(off) to fail-open(whole-FS) is not a quiet
patch. Current latest tag is v0.3.1. Final number is the user's per semver policy. `log.md`
gets a real-hardware validation entry per workflow once implemented.

## Open questions for the human (external review)

1. **Version:** v0.4.0 (recommended) vs a patch with a BREAKING-DEFAULT banner — semver call.
2. **Off-switch surface:** single-axis `allow_roots: []` only (my finalized choice) vs also
   adding the more-discoverable `file_transfer.enabled: false` (candidate's choice). I dropped
   `enabled` for minimal diff + no precedence matrix; confirm or ask me to add it.
3. **Real-hardware validation:** does this no-wire-change patch need a fresh pc732 broker run,
   or does the e2e matrix + a `log.md` note suffice given no proto change?
4. **install.sh commented block:** ship the commented example at all (discoverability vs noise
   in a deliberately terse generated file)? Plan includes it, sans copy-pasteable `[]`.
