# S2 External Re-review Round 2 Tasklist

Scope: narrow independent closure review of the unstaged developer response and fixes for round-1 F1-F4,
using the staged S2 tree as baseline. Prior internal/live claims are indexes, not evidence.

- [x] Rebuild staged-vs-unstaged scope and verify no unrelated product implementation change entered the fix.
- [x] F1: verify Arm R sends ten same-source wrong PINs, proves every attempt reached the intended failure path,
  then uses a fresh correct-PIN attempt; check event scoping, timing, flip behavior, cleanup, and documentation.
- [x] F2: prove the flake regex rejects healthy VOTER success plus an unrelated later failure, accepts only the
  documented timeout/incomplete failure signatures, and does not regress existing infra-flake matching.
- [x] F3: verify port cleanup requires transport-level connection refusal and an independently authoritative
  allocation-row absence, with a live sentinel baseline and no output/parser ambiguity.
- [x] F4: mechanically recount all S2 drill assertions and cross-check response, inventory, README, and live verdict.
- [x] Recheck `_ev_destroyed` single-event binding and the ingress-proxy scope note requested in round 1.
- [x] Run shell/Python syntax, whitespace, focused Go gates, and independent synthetic negative controls.
- [x] Sync/build as needed and run affected 80/81 drills on fresh live instances with automatic retry disabled;
  verify all review containers and temporary artifacts are cleaned.
- [x] Write a round-2 report beginning with Pass or Fail, with closure matrix, new findings, doubts, evidence,
  and explicit release recommendation.
- [x] Mark every item truthfully, stage all files with `git add -A`, and verify no unstaged/untracked residue plus
  `git diff --cached --check` success.
