# S1 External Re-review Round 2 Tasklist

Scope: independent re-review of the developer response and current unstaged S1 tree against the round-1
Fail report. The response and reported server logs are claim indexes only. No product implementation edits.

## Boundary and round-1 closure

- [x] Rebuild the current unstaged/untracked scope and read the complete developer response.
- [x] MAJOR-1: verify broker-admin observation is ctl-session independent, occurs before login, and the first
  post-login node read is a single non-polling current-state oracle; reconcile all coverage wording.
- [x] MAJOR-2: verify J14 exact header/empty-body parsing, cross-node overwrite digest, and restored true
  push/pull digest round-trip cannot pass on rc-only, stale, or no-op behavior.
- [x] MAJOR-3: inspect `s1-review.md` for the required finding set, adversarial structure, owner dispositions,
  test integration, model/count statements, provenance honesty, and contradictions with current code.
- [x] MINOR-1: verify roadmap status and S0 registry now express S1 landed/pending-commit without claiming done.
- [x] MINOR-2/3/4: verify temp-only drift diagnostics, constructed/runtime double-golden coverage and 94/99
  invariant, whitespace-safe rendering, regenerated golden content, and absence of source-tree residue.

## Regression and new-risk review

- [x] Audit every remediation diff for new false-green paths, shell quoting/status masking, stale artifacts,
  command-tree initialization side effects, or documentation overclaim.
- [x] Verify `pty-run.py` exec/no-command/eof/silent-child guards against the developer's new claims.
- [x] Recount drill assertions and command-tree paths/Hidden flags independently; compare with response/docs.

## Verification and handoff

- [x] Run shell/Python syntax, focused command-tree Go tests, pty helper negative controls, and full whitespace checks.
- [x] Rebuild/sync the current tree and rerun changed deploy-tier drills 60 and 61 on weilandserver if static
  closure holds; inspect the strengthened assertion lines rather than trusting summary counts.
- [x] Run proportional regression gates; reuse round-1 full e2e only where no affected product/e2e code changed.
- [x] Write a round-2 report beginning with Pass or Fail, including closure matrix, new findings, doubts,
  verification, and release recommendation.
- [x] Re-read artifacts, close boxes truthfully, stage all files with `git add -A`, and verify no unstaged or
  untracked delivery files remain; record any cached diff-check failure rather than hiding it.
