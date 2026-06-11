# Transfer Unrestrict — Internal Review (Round 1)

Date: 2026-06-11
Reviewers: 4-lens adversarial workflow (security-invariant / config-upgrade / test-rigor / docs-consistency)
Disposition author: main process (sole implementer)

## Headline

All four lenses independently concluded the **shipped code is correct**: the fail-closed
invariant holds (mode from RAW config, never `len(canonAllowRoots)`); `modeOpen` skips only
`containedIn` and keeps every mechanism check; the disabled gate fires at push *prepare*
(before tier-B commit caches a `vp`); the broker does zero path validation so the default
flip bypasses no gate; the nil-vs-`[]` discriminator is sound end to end. **No correctness
defects.** Every finding is a coverage gap or nit. One test-quality finding is real and
important (the TOCTOU test's `path_race` arm is dead).

## Dispositions

| # | Sev | Finding | Disposition |
|---|---|---|---|
| 1 | major | TOCTOU test never exercises the dev/inode `path_race` arm (0/8000 — only the symlink/O_NOFOLLOW arm runs) | **ADOPT** — split into two tests: a symlink-arm (asserts success ⇒ never EVIL) and a regular-vs-regular rename-swap arm that runs the dev/inode recheck under `-race`, asserting no torn/wrong read. |
| 2 | major | No open-mode (nil roots) tier-B test under real auth_callout — both existing auth tests pass non-empty roots (narrow) | **ADOPT** — add `TestTierB_OpenDefault_AuthCallout` (nil roots, tier-B push+pull-back, assert OK+sha). The only harness exercising JWT/ACL+JS in open mode. |
| 3 | major (×3) | No e2e_matrix transfer subtest; default-inversion absent from the cross-phase net | **ADOPT (adapted)** — the matrix is a phase-subprocess runner (`./test/pN/...`); transfer is a feature increment in `test/cli_e2e` (hard-gated by `make test`). Rather than mint a fake phase, add a `transfer-defaults` subtest to `all_phases_test.go` that runs the cli_e2e open/disabled subset as a subprocess under `-tags e2e_matrix`. Delivers the promised cross-phase guard. |
| 4 | minor/high | No install.sh fresh-install contract test (no active `file_transfer`/`allow_roots` line) — the inverse footgun | **ADOPT** — extend `test/p10/install_sh_test.go` to assert every non-comment line of the generated yaml lacks `allow_roots`/`file_transfer:`. |
| 5 | minor | No pull-side open-mode e2e (push tier-A/B open exist; pull open never round-tripped through broker) | **ADOPT** — add `TestTransfer_TierA_OpenByDefaultPullSucceeds`. |
| 6 | nit | Fail-closed pinned in two halves (resolveTransferMode + Validate) that never meet via `agent.New` | **ADOPT** — add a test that builds `New(Config{RootsConfigured:true, AllowRoots:[/no/such]})` and asserts the resulting `a.transferMode`+`a.canonAllowRoots` reject `/etc/x`. |
| 7 | nit | TOCTOU docstring says "Run under -race (make test)" but `make test` has no `-race` | **ADOPT** — fix docstring to `go test -race ./internal/agent/`. goleak gate: `internal/agent` has no goleak harness and the two goroutines join via `wg.Wait` (no leak); `test/concurrency` itself deliberately avoids goleak (hand-rolled). Recorded; not adding goleak for one test. |
| 8 | nit | Open-mode unit tests hard-depend on `/etc/hosts` (fail, not skip, on minimal images) | **ADOPT** — guard with `t.Skip` if `/etc/hosts` absent. |
| 9 | nit | usage.md:1019 embeds `file_transfer:\n  allow_roots: []` in an inline code span → literal `\n` on render | **ADOPT** — convert to a fenced yaml block. |
| 10 | nit | `PullPrepareReq.Path` comment less specific than `PushPrepareReq.Path` | **ADOPT** — align wording (cosmetic). |
| 11 | nit | file-transfer-plan.md body (§"Refusing dangerous paths", test-vector #8) still states old mandatory behavior; only the top banner flags the reversal | **ADOPT (light)** — add inline `(SUPERSEDED …)` markers at those spots; keep history per "annotate, don't delete". |
| 12 | nit | modeOpen WARN fires on every `New()` incl. test harnesses | **REJECT (no-op)** — confirmed cli_e2e + security harnesses use discard loggers (`silentLog`/`silentTransferLog`); WARN is once-per-process and correct. No change. |
| 13 | low | WARN text doesn't mention `file_transfer: {}` (empty block) also → open | **REJECT** — message is accurate; empty-block→open is intuitive. Leave as-is. |
| 14 | nit | open-mode dir-leaf has no dedicated test (existing coverage rejects via `IsRegular`) | **ADOPT (light)** — add a dir-at-leaf open-mode case (cheap; pins the now-load-bearing guard). |
| 15 | nit | No pull-side off-switch e2e (push off-switch + both-direction unit exist) | **REJECT** — `TestValidate_DisabledMode` covers both directions at the function layer; e2e duplication low-value. |

## Round 2 (verify fixes + fresh attack)

A second 4-lens adversarial pass on the Round-1-fixed tree.

**Outcome:** `correctness-deep` returned **zero findings** (re-derived the resolveTransferMode
truth table, swept the whole repo for inconsistent `agent.Config` setters, confirmed no
mode-bypass in the push/pull handlers). `verify-r1-fixes` **confirmed all five Round-1 fixes
are correct** — it independently instrumented the new TOCTOU regular-swap arm and measured
`path_race` = 61/6000 (the dev/inode branch that was 0/8000 dead before is now live).
Remaining findings were two cheap coverage gaps + report/tree consistency:

| # | Sev | Finding | Disposition |
|---|---|---|---|
| R2-1 | minor (×3 lenses) | Round-1 disposition #14 ("dir-leaf OPEN-mode case") did not actually land — the shipped dir-leaf test (`TestValidateForRead_RejectsDirectoryLeaf`) runs `modeNarrow`; only a FIFO case exercises the open-mode `IsRegular` guard | **ADOPT** — added `TestValidate_OpenMode_RejectsDirLeaf` (dir at leaf, both directions, `modeOpen` → `not_a_regular_file`, dir untouched). Disposition #14 is now true. |
| R2-2 | minor | No partial-drop narrow test (some valid + some dropped roots) — the exact typo'd/unmounted-root footgun the plan cites as the RAW-vs-canon motivation | **ADOPT** — added `TestNew_PartialDropNarrow` (via `agent.New`: survivor root accepted, outside-survivor → `path_outside_roots`, `canonAllowRoots` len 1). |
| R2-3 | nit | Plan's "fix `≤2 GiB` wording" line (docs/usage.md row) was never tracked as ADOPT/REJECT | **REJECT (record)** — the `≤2 GiB` ceiling is correct (`transfer.go` cap; `cmd/tether/transfer.go`); it is orthogonal to transfer-unrestrict and intentionally deferred out-of-scope. Recorded here so plan/report/tree are reconciled. |

`docs-final` confirmed all docs (usage.md, architecture.md, both plan docs, proto comments,
install.sh, CLI help) are mutually consistent and match the code; no contradictions or broken
renders remain.

**Post-Round-2 gates:** `make test`, `make e2e` (incl. `TestTransferDefaultsMatrix`),
golangci-lint (0 issues), `go test -race ./internal/agent/`, gofmt, go vet — all green.

## External review (round 3) — reviewer-applied hardening + main-process reconciliation

The external reviewer (user) directly applied a substantial security-hardening pass on top of
the round-2 tree, plus matching adversarial tests. Main-process reviewed all of it, ran the
full gates, and reconciled two regressions it introduced. **Approved by the reviewer.**

**Reviewer-applied hardening (all gates green):**
- `openat`-pinned parent directories (`openDirNoSymlinks` + `Fstatat`/`Openat` +
  `dirFDStillNamesPath`) — defeats parent-**component** TOCTOU swaps, not just the leaf.
  Read/write open + commit all re-verify the pinned parent (`path_race` on mismatch).
- `linkat`-based atomic no-clobber commit (no-force) + `renameat` (force) via `AtomicWrite`,
  closing the old `Lstat`+`Rename` overwrite race. Concurrent commits → exactly one winner.
- Tier-B **verify-before-commit**: size + SHA checked inside `objectStoreGetAndWrite` BEFORE
  rename, so a wrong-SHA/size object never becomes the destination.
- Streaming size caps everywhere (files can grow after `Stat`); tier-A→B fall-through on growth.
- Bounded `pushCommitCache` (TTL + max-entries) — fixes an unbounded-map memory/DoS vector.
- `containedIn` via `filepath.Rel` — fixes the `/`-root bug (`allow_roots: ["/"]` now works) while
  still excluding `/srv/localfoo` from `/srv/local`.
- **Strict `agent.yaml` decoding** (`KnownFields(true)` + reject a real second document) — a
  typo'd `allow_root` / `file_transfers` can no longer silently fall open.
- Broker-side size validation (negative size, tier-A 8 MiB cap).
- `golang.org/x/sys/unix` promoted to a direct dependency (used by the openat pipeline).

**Main-process reconciliation (2 regressions from the strict decoder, fixed):**
1. Empty / comment-only / whitespace-only `agent.yaml` was rejected with `decode: EOF` (agent
   failed to start). Restored tolerance (zero struct → fall through to CLI flags), matching the
   historical `yaml.Unmarshal` behavior. `KnownFields` strictness on real documents is retained.
2. A benign trailing `---` (empty second document) was mis-rejected as multi-doc. Now only a
   **non-nil** second document is rejected. The real-hidden-second-document rejection
   (`TestLoadAgentYAMLRejectsUnknownFields`) stays intact.
   Regression pinned by `TestLoadAgentYAML_ToleratesEmptyAndTrailingSeparator`.

**Conscious trade-offs signed off by the reviewer:**
- Strict `agent.yaml` parsing now rejects **any** unknown field at startup (broader than the
  transfer change; supersedes the old "unknown fields → warn not error" stance, which lives only
  in the frozen historical `requirements.md` §7.2). Documented in `usage.md`. Accepted: a typo
  silently falling open is worse than a loud startup error.
- Portability narrowed to `golang.org/x/sys/unix` (Linux agent + darwin dev only; darwin now gets
  full dev/inode TOCTOU instead of the old lossy fallback). Accepted.

**Post-round-3 gates:** `go test ./...` exit 0 · `make e2e` (incl. `TestTransferDefaultsMatrix`)
· golangci-lint 0 issues · `go test -race ./internal/agent/ ./cmd/tether/` · gofmt · go vet — all
green (darwin + `GOOS=linux` build).

## Notes for external review

- The e2e_matrix resolution (#3) is a finalizer call: transfer e2e is hard-gated through
  `make test` (cli_e2e) and additionally surfaced in the `-tags e2e_matrix` run via a thin
  subprocess subtest. The phase matrix stays phase-only by design.
- `goleak` (#7) is intentionally not used in `internal/agent`; matches `test/concurrency`'s
  documented choice. The `-race` gate covers the concurrency face.
