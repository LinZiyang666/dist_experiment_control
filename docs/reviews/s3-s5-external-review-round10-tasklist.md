# S3–S5 (G-A) external re-review round 10 tasklist

Scope: re-review the developer's concurrent response to round 9 in drills 71/74. Preserve round 9 as the historical
Fail report; verify the final worktree, not the temporarily split index/worktree state created while edits landed.

- [x] Reconstruct R9-M1/M2/M3 closure from the final 71/74 source and verify no additional concurrent edits remain.
- [x] Verify `_snap_nidhome` fails closed for command/JSON/row/nid/home errors and that callers preserve its rc rather
  than accepting empty output through a successful downstream pipeline.
- [x] Verify SKEW and B enumerate exactly all expected nids; failed snapshots/legs gate the complete transaction;
  failed negative-control rebalance gates both B post-control and C control.
- [x] Verify B's fresh attribution snapshot is itself a gate and cannot fail after pre-flow while dry-run/real still
  execute; verify exact moved-nid before/after evidence remains sound.
- [x] Verify Arm C revalidates target>0 and captures a fresh validated mapping adjacent to the kill after all per-nid
  pre-flow polls; reject stale-target/vacuous-kill paths caused by distribution drift during those polls.
- [x] Verify C auto before/after snapshots, pre-proven-nid membership and exact post-flow form one causal closure.
- [x] Verify 71 gates B on `_crec`, preserves E behavior and cleanup, and emits no false migration claim on recovery
  failure.
- [x] Run syntax/whitespace plus focused adversarial probes for snapshot failure, distribution drift during C flows,
  B fresh-snapshot failure, negative-control rc failure and 71 recovery failure.
- [ ] Synchronize current 71/74 to weilandserver using tether CLI only, verify hashes, and run both concurrently;
  retry environment failures without serializing the pair.
- [ ] Write a round-10 Pass/Fail report with findings, doubts, exact evidence and release disposition; scan logs and
  stage all files, leaving no unstaged/untracked content.
