# cluster phase-transition fluidity — internal review round 1 (v0.4.2)

> Stage C adversarial internal review (4 Opus lenses: raft-correctness, fsm-op-determinism,
> wiring-safety, render-compat-shrink → synthesis). All 4 lenses + synth: **CONDITIONAL**.
> Main-process dispositions inline (✅ adopt / ✎ adopt-with-change / ✗ reject).

## Verdict: CONDITIONAL — sound approach, all 6 load-bearing claims HOLD, but NOT shippable as-is.

All 6 claims verified against the real code: (1) no migration / no proto bump / commandVersion=2;
(2) raft_addr no-roster-bump vs nats_route topology-bump, change-gated; (3) AddVoter-on-existing =
in-place address rewrite, column-first-then-AddVoter; (4) inRaft-not-isVoter nonvoter guard,
promote-after-catch-up; (5) Standalone byte-equivalence, cluster-mode-only; (6) CLI-boundary loopback
guards, LitText injection-safe. No FAIL-class design defect.

## BLOCKERS

- **B1 — `set-raft-addr --node <peer>` ungated at N≥2 re-introduces the wedge.** clusterdrain.go
  SetRaftAddr makes no self-vs-peer / numVoters distinction; AddVoter(peer,newAddr) re-points the
  leader's replication to newAddr immediately, and if the peer is not yet serving it the config entry
  never reaches the new quorum → hard-wedge (the exact thing this feature removes). The runbook warns
  in prose only. ✅ **FIX**: at nodeID!=self && voters>1, REFUSE (hard error); reject newAddr
  colliding with another committed server's address (any rebind). Self-rebind at N=1 stays
  unconditionally safe.
- **B2 — the GROW keystone has ZERO test coverage.** No test references AddNonvoter / SetRaftAddr /
  SetNatsRoute / driveJoin nonvoter staging / the validators. Per CLAUDE.md §5 a concurrency change
  without -race + the leak gate is "not done"; the plan §5 wedge-proof drills (AddNonvoterUnreachableNoWedge
  etc.) none landed. ✅ **FIX**: land the keystone unit + gated integration drills (see Tests below).

## MAJOR

- **M1 — SHRINK is unreachable dead code.** Only the foundation (Config.Standalone, IsClusteredJetStream)
  landed, both used only in their own tests; no `cluster shrink-to-single` / `reconcile nats
  --to-standalone` / warnClusteredJSShrink / operator-gated JS-reset. Plan §2 lists shrink as
  in-scope-YES. ✅ **FIX**: build the operator-driven shrink path (reconcile nats --to-standalone +
  warnClusteredJSShrink + N=1 guard + full-restart not SIGHUP) — the user has confirmed shrink must
  ship. cluster{}-removal needs a FULL nats-server restart (not SIGHUP-reloadable).

## MINORS (dispositions)

- **m1** AddNode (latent OpClusterAdd) still direct-AddVoter (the wedge) + d7 drills go through it, so
  they don't prove driveJoin. ✎ **FIX**: route AddNode through AddNonvoter→catch-up→AddVoter (shared
  admission), so the d7 harness exercises the fixed flow.
- **m2** SelfConfiguredAddr dead code; AddVoter always logs a config entry (effect-idempotent only).
  ✎ **FIX**: read RaftConfiguration once in SetRaftAddr, skip AddVoter when the target's addr already
  equals newAddr (write-free no-op), and REMOVE the now-redundant SelfConfiguredAddr.
- **m3** Two Mechanism-C guards absent: `cluster init` loopback-advertise refusal + the offline-doctor
  bind-vs-advertise split. ✅ **FIX** both (CLI-only refusal + --allow-loopback; advertise validated
  non-loopback WITHOUT net.Listen).
- **m4** nats_route IS in the agent-facing roster SELECT + signed body, yet OpClusterNodeRoute bumps
  only topology_generation. Benign today (consumer deferred) but a latent anti-rollback trap. ✎ **FIX**:
  also bump roster_generation on a route change (consistent with PlanClusterCertRotate).
- **m5** node_readdr_test gaps: no "topology UNCHANGED across readdr" / "roster UNCHANGED across route"
  assertions; no NUL/invalid-UTF-8 cases. ✅ **FIX** in the test additions.
- **m6** No knownOps↔defaultAppliers symmetry guard (a missing applier → un-wrapped error → fail-stop
  → cluster-wide panic on replay). ✅ **FIX**: add the symmetry guard + the proto/migration regression guard.
- **m7** No unknown-OpType poison-skip test (the mixed-version path that justifies no-commandVersion-bump).
  ✅ **FIX**: add it.
- **m8** doctor route-only branch mislabeled 'raft_advertise'; empty nats_route not flagged. ✎ **FIX**:
  distinct 'nats_route' name + flag empty as mesh-unconfigured. (ADVISORY-vs-FATAL: keep ADVISORY for
  the general doctor; a FATAL belongs in a grow-preflight — tracked, not now.)
- **m9** PlanClusterNodeRoute does no structural validation (only LitText). ✎ **FIX**: mirror readdr's
  url.Parse(nats:// + host).
- **m10** Mechanism E (transport advertise decouple) + N=1 status banner + `join approve --check`
  absent. ✗ **DEFER** (Mechanism E is cosmetic per the plan; banner/check are nice-to-have) — track as
  open plan items, not v0.4.2 blockers.

## Tests to add (from the review's mustAddTests — all ✅ adopt)

Keystone driveJoin nonvoter drill (gated -race + leak gate); SetRaftAddr / SetNatsRoute admin units;
peer-rebind gate test (after B1); ForceSingleNeverSetByRebind; validator table test;
AddNonvoter/AddVoter raft-semantics unit; node_readdr additions (gen-unchanged + NUL/UTF-8 + zero-row);
unknown-op poison-skip; knownOps↔appliers symmetry + proto/migration guard; adminsock wire test;
StatusReport+doctor unit; gated GrowAfterRebindNoWedge / RebindSelfRaftAddrOnline; standalone
byte-equivalence regression; ShrinkToSingleReRendersStandalone (when shrink lands).

## Next

Fix B1+B2, M1 (build shrink), the adopted minors, land all tests, update docs (runbook §7, usage,
architecture). Then **internal review round 2** (per the user) on the complete grow+shrink+docs →
stop for external review.
