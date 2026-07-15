# S3–S5 (G-A) external re-review round 3 tasklist

Scope: independently re-review the developer follow-up layered over the fully staged round-2 tree.  The user
permits release when no Major issue remains; therefore Minor observations alone do not block, but false-green
oracles, live-secret disclosure, destructive continuation on invalid foundations, and unapproved removal of
locked G-A acceptance coverage remain Major.  No standalone developer response file is present in the current
unstaged delta, so code and updated documentation are the evidence.

## Boundary and closure matrix

- [x] Reconstruct the exact five-file unstaged delta and effective `HEAD`→worktree tree; verify deleted/untracked
  state and identify every claimed R2-M1–M8 disposition.
- [x] Check whether unchanged R2-M4/R2-M5/R2-M7/R2-M8 concerns were implemented, explicitly and consistently
  downgraded, or approved out of G-A scope; do not infer closure from silence.
- [x] Compare local drill/vendor/image inputs with `weilandserver`; rebuild only if an effective baked/runtime
  input differs or cannot be proven identical.

## Static and adversarial review

- [x] Re-audit drill 73 foundational fail-fast gates, control-rehome+ready prerequisites, #33 `ss_up` gate,
  secret hygiene, warning quoting, separate OFF/ON checks, and every later destructive transition.
- [x] Re-audit drill 73's still-unchanged 1+1 Q-construction for deterministic eligible-home placement and the
  required dead/survivor live baselines; investigate any timeout even if another run is GREEN.
- [x] Re-audit #33 documentation against the executable 45-second observation, number allocation, actual
  `ApplyHome` implementation, and absence of unmeasured eventual-recovery/root-cause claims.
- [x] Re-audit drill 31's strengthened fleet oracle for exact rc, ONLINE target-set equality, distinct per-node
  skip evidence, exact summary parsing, quoting, and false-pass resistance.
- [x] Re-evaluate unchanged drill 71 settle/cursor/allocation causality, drill 72 persistent in-flight and OFF
  port-reclaim coverage, drill 32 real three-role/§8.4 and manifest contract, and drill 74 60/180-second facts.
- [x] Reconcile README, inventory, gotcha ledger, plan, and reports for stale counts, stale “GREEN” claims,
  event-coverage contradictions, historical-vs-current facts, and use of external-verification language.
- [x] Review all effective changes for shell expansion, exit-code loss, race/flakiness windows, cleanup,
  destructive safety, sensitive log material, and misleading PASS/NOT-COVERED semantics.

## Independent verification

- [x] Run shebang-aware shell syntax checks, whitespace checks, ShellCheck if available, focused Go tests as
  needed, and local adversarial probes for the changed 31/73 oracles.
- [x] Run current-hash drill 31 and drill 73 on `weilandserver` with automatic retry disabled; run 73 at least
  twice strict-serial on fresh instances and retain complete logs.
- [x] Reuse round-2 shared/G-A evidence only for byte-identical runtime inputs; rerun any additional drill whose
  executable or claimed contract changed or whose prior result is needed to decide a current Major finding.

## Disposition

- [x] Write a round-3 report beginning with Pass or Fail, clearly separating release-blocking Major findings
  from non-blocking Minor/advisory items and stating whether G-A is released.
- [x] Complete every task, stage all files with `git add -A`, and verify no unstaged/untracked content remains;
  both cached and uncached whitespace checks must pass.  Do not commit or push.

## Completion evidence

- The developer follow-up was exactly five unstaged files over the fully staged round-2 tree. No standalone
  response or approved scope change was present. Drills 71/72/32/74 were byte-unchanged.
- Local/remote hashes matched for drills 31 and 73; vendored binaries and the existing image inputs were
  unchanged. The current files were run server-local on two fresh instances without automatic retry.
- Drill 31 was GREEN (28). Drill 73 was RED in both strict-serial runs: one failed the 150-second initial
  non-tunnel construction; the other completed that construction and control rehome, then failed the immediate
  proxy-ready hard gate. Complete logs remain in `/tmp/s3s5-external-r3/{solo1,solo2}` on `weilandserver`.
- Syntax/whitespace checks passed. ShellCheck was unavailable. Local adversarial probes rejected both the old
  one-line/nonzero fleet false-positive and the old `ss_up`-failure gap false-positive. No Go or baked runtime
  input changed, so the byte-identical round-2 focused Go/shared evidence was reused.
- The formal conclusion is Fail because release-blocking Major findings remain. All files were staged only
  after the report was completed; no commit or push was made.
