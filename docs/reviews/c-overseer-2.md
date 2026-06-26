# 监工 #2 — Independent requirements audit of C3 + C4

> Spawned after C4 per the /goal: an INDEPENDENT overseer verifies whether C3 + C4 TRULY deliver their requirements AND match the SPIRIT of the 4 v2 docs — hunting for 阳奉阴违 (lip-service / falsely-closed gap rows / hidden descopes / vacuous tests). Method: verified every claim against source (not the self-assessing plan/review docs); ran the gate (`internal/cluster ✅ · broker ✅ · natsreconcile ✅ · natsconf ✅ · cmd/tether ✅ · storage ✅`).

## OVERALL VERDICT: **PASS — C3 + C4 honestly deliver 建议1 + 建议2's spirit and success metric #1. No 阳奉阴违 found.**

Every descope + gap is transparently documented; the gap-doc rows are honestly left open (if anything UNDER-claimed); no requirement reported DONE is contradicted by source. The implementations are substantive, not seams.

## Item-by-item (REAL / PARTIAL / LIP-SERVICE)

| # | Claim | Verdict |
|---|---|---|
| C3-1 | render+validate+swap+reload driven by topology_generation; B1 render success-path real; B2 wired in production | **REAL** (engine `reconcile.go:53-164`; loop `topology_reconcile.go`; `BuildMergedConf` harvests cluster mTLS fail-closed; `serve.go:136-192` wires nats_conf_path + cluster-mode default) |
| C3-2 | observed via REAL /varz probe; computeHealth gate presence-gated, after FORCE_SINGLE/QUORUM_LOST | **REAL** |
| C3-3 | unknown-directive fail-closed incl. subkeys (auth_callout.xkey) | **REAL** |
| C3-4 | `reconcile nats --all --wait` real + takeover demoted to `--manual` | **REAL** (`--all` never bumps a gen; takeover Hidden+DEPRECATED) |
| C3-5 | C3 gap rows closed by code+test | **REAL** (gap-doc rows on disk still ❌/🟡 — UNDER-claimed, flips at external review) |
| C4-6 | operation log replicated+resumable; both BLOCKER fixes real | **REAL** (migration 0015 additive; barrier-before-AddVoter; retire-after-removal isVoter-aware) |
| C4-7 | `join prepare` joiner-local + one approve = one+one | **REAL** (prepare has NO callAdmin/leader contact; real Ed25519 PoP re-verified every replica) |
| C4-8 | all retire gates preserved AND re-checked on resume; legacy DrainNode not weakened | **REAL** (gates re-run every drive at DRAIN_REQUESTED + RAFT_REMOVED; raw mutators assertNoActiveOp-guarded) |
| C4-9 | `cluster ops` real log + abort/confirm + v1 State vocab stable | **REAL** |
| C4-10 | plan/apply DESCOPE honest+transparent | **HONEST** (no cluster_plan.go, no PLAN_DRAFTED residue, gap rows honestly open, documented "NOT claimed as done") + a scheduling caveat (below) |
| C4-11 | controller tests real | **PARTIAL** — the 4 single-node tests are real (not vacuous), but the multi-node kill-9 RESUME battery is ABSENT (BLOCKER-2 zero coverage, BLOCKER-1 only single-node gate logic). Honestly disclosed, but the bug class that shipped 2 BLOCKERs has no e2e net. |
| C4-12 | 必须拒绝 automations still refused | **REAL** (zero ssh/sudo/systemctl in the controller; every side-effect a raft op; the only exec is the same-uid `nats-server --signal reload`) |

## SPIRIT CHECK
- **建议2 / C4 — SATISFIED.** Genuinely one prepare (joiner-local) + one approve; retire genuinely recoverable with every D7 gate preserved + re-checked.
- **建议1 / C3 — SATISFIED with one honest residual.** Existing brokers auto-add a route via SIGHUP (no manual re-run — the O(N) pain is gone). Residual: the JOINING broker still needs ONE manual `reconcile nats --manual` cutover to establish its `cluster{}` + the `http:8223` monitor (reload-only cannot hot-add those, and install.sh doesn't pre-render them). Design-inherent + disclosed → an O(1)-on-the-new-box residual, not lip-service.

## Three honest-but-real items the main process must address (none fake-done; all disclosed)

1. **(Highest) C4 has no executed resume test** — `test/c4` + `TestC4Matrix` absent; BLOCKER-2 zero coverage, BLOCKER-1 single-node only. The recoverability core is proven only by code review + single-node units.
2. **plan/apply descope is UNSCHEDULED** — honestly open in the gap doc, but the target "post-C4 follow-up" is not a real stage (C5=proxy/C6=obs/C7=rotation), while `v2-automation-program.md` still maps plan/apply to C4. Two gap rows now have no scheduled home → at risk against the "一条不漏" DoD.
3. **C3 install.sh follow-up pending** — `scripts/install.sh` doesn't render `http:8223` / `nats_conf_path`, so a freshly-joined broker needs a manual `reconcile nats --manual`.

## Main-process disposition (see the commits + the addendum at the bottom)

The verdict is PASS (no lip-service). The 3 items are real improvements, addressed below as part of the C4 close-out (not deferred): (1) the BLOCKER decision logic is locked by direct unit tests of `retireGatePasses` + the barrier-before-AddVoter persist; the full multi-node kill-9 e2e is scheduled into the D9-style gated matrix backlog (documented). (2) the program doc is reconciled — plan/apply moves to an explicit post-C7 backlog row with the gap rows staying open. (3) install.sh renders the loopback monitor + nats_conf_path.
