# G4 external re-review tasklist

- [x] Re-read `CLAUDE.md`, G4 plan, G4 external report, cluster docs, simcluster mandate, and simcluster access notes.
- [x] Reconstruct staged plus unstaged G4 review boundary from `git status` and broad diffs.
- [x] Review the developer's round-1 fixes without trusting the embedded response.
- [x] Re-check `cluster init` / broker.yaml seam application and `cluster add` fail-closed behavior.
- [x] Re-check joiner lifecycle pause hints and local admin status classification.
- [x] Re-check grow-lock release binding and conditional metadata clearing.
- [x] Re-check cutover refusal handling and catch-up terminal-state barriers.
- [x] Add independent round-2 regression tests for any remaining high-risk behavior.
- [x] Run focused Go checks and formatting/diff checks; skip heavy simcluster drill because the deterministic local Fail is already reproduced and the clean-path drill would not cover stale seams.
- [x] Write the round-2 external review report with Pass/Fail, doubts, findings, recommendations, and verification.
- [x] Add all files to the git index after the report is complete.
