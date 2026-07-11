# S1 External Review Tasklist

Scope: independent external review of all unstaged/untracked S1 changes against `HEAD`. Internal plans,
reviews, landing claims, and prior live-run statements are indexes only, never trusted as evidence.
Reviewer may add isolated tests but must not modify product or submitted implementation. Final handoff stages
all files with `git add -A`.

## Boundary, contracts, and prior-work reconstruction

- [x] Read `CLAUDE.md`, requirements/architecture/user/cluster documentation, the S-series roadmap and S1 plan.
- [x] Rebuild the exact unstaged/untracked scope; confirm the staged baseline is empty and classify every file.
- [x] Read representative prior external tasklists/reports to reproduce severity, evidence, doubt, and staging practice.
- [x] Reconcile S1 deliverables and zero-product-diff boundary against the plan and coverage inventory, including
  the command/path/Hidden counts and the explicit NOT-COVERED claims.

## Static review: permanent command-tree gate

- [x] Audit Cobra traversal completeness: runtime completion injection, aliases, hidden commands/flags,
  persistent/inherited/local flags, duplicate names, deterministic ordering, and initialization side effects.
- [x] Verify the golden update path is opt-in, test-safe, does not create misleading residue on ordinary failures,
  and detects representative command/flag/Hidden mutations rather than merely snapshotting an incomplete tree.
- [x] Independently enumerate the current command tree and compare counts/content with the inventory and golden.

## Static review: simcluster harness and fidelity

- [x] Audit Docker dependencies/assets, executable modes, Python syntax/runtime failure handling, FUSE privileges,
  PTY lifecycle/signal/resize semantics, timeout behavior, child reaping, and cleanup on every exit path.
- [x] Audit `agentyaml.sh` for production-path fidelity, strict-YAML negative control, quoting/injection, ownership/mode,
  systemd unit semantics, stale-row avoidance, restart recovery, temporary-file cleanup, and parallel isolation.
- [x] Audit `run-drills.sh`, README, `simcluster`, gotcha ledger, and coverage inventory for behavioral/documentary drift,
  honest GREEN/RED language, family scheduling claims, line/count claims, and contradictions with source.

## Static review: drill oracle strength

- [x] Review `60-user-journey` assertion by assertion: auth/relogin causality, node status, exact exit/signal semantics,
  stream ordering, PTY prompt/echo discrimination, resize, Ctrl-C process-group proof, ps transition, history filters/follow,
  watchdogs, process cleanup, and shell exit-code propagation.
- [x] Review `61-transfer-edges` assertion by assertion: open/narrow/disabled controls, daemon freshness, symlink/TOCTOU
  walls, force overwrite, 2-GiB limit, real max-payload tier boundary, landed/hash oracles, audit pairing, and pipeline masking.
- [x] Review `62-remote-fs-safe` assertion by assertion: boot-time mount discovery, real wedge evidence, exact refusal source,
  auto/off+safe distinction, agent-alive controls, T/S-vs-D discriminator, SIGSTOP/CONT targeting, watchdog effectiveness,
  shared-host safety, and cleanup after partial setup/failure.
- [x] Search for false-green patterns globally: unchecked pipelines, broad regexes, stale files/processes, `|| true`,
  self-matching `pgrep/pkill`, assertion helpers that swallow status, fixed sleeps, and claims unsupported by an oracle.

## Verification

- [x] Run formatting/whitespace, shell parse, Python compile, focused Go command-tree tests, and proportional package tests.
- [x] Run `make test`, `make e2e`, and `make lint` once as the submission gate; classify environmental failures separately.
- [x] Inspect simcluster server health/capacity and remotely build the submitted tree; run drills 60, 61, and 62 singly,
  preserving logs and independently checking claimed assertion counts and failure signatures.
- [x] If a suspicious oracle can be isolated safely, run a negative-control/mutation probe without leaving product-code residue.

## Report and handoff

- [x] Write `docs/reviews/s1-external-review.md` beginning with `Fail` or `Pass`; include severity-ranked findings,
  doubts, verification evidence, live-sim results, residual risks, and an explicit release recommendation.
- [x] Re-read every changed artifact and the report, close checkboxes truthfully, run `git diff --check`, stage all files
  with `git add -A`, then verify no unstaged/untracked files remain and cached scope is complete.
