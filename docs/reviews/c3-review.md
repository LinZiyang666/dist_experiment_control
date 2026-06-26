# C3 Stage-C Review + Adjudication — Topology Reconciler

> Stage-C adversarial internal review (7-agent workflow: 4 lens reviewers → 2 verifiers → synthesizer; raw `tasks/w35haj1vw.output`). **Initial verdict: FAIL — 3 BLOCKERs.** The synth independently re-traced every load-bearing claim. The blockers were correct and fundamental: the engine/status/CLI scaffolding was sound, but **the production render success-path was never wired and never tested**. This file records each finding + the main process's disposition. **All 3 BLOCKERs + 5 MAJORs fixed; the cheap MINORs fixed; 2 MINORs adjudicated accept-as-is. Full gate green. External review deferred to end of the C-program.**

## Initial verdict: FAIL — headline

The per-broker reconciler was a no-op on every real cluster: (B1) the engine omitted the mTLS cert paths so `Render` always failed; (B2) `NatsConfPath` was never wired into the production `Config`, so the loop never ran — yet the health gate still marked every grown cluster DEGRADED forever; (B3) the manual cutover conf omitted the HTTP monitor while the reconciler rendered it, so the first reload tried to hot-add `http:` (which nats-server rejects). Every `reconcile_test.go` case early-exited before the render step.

## HARD-INVARIANT re-confirmation (post-fix)

| # | Invariant | Status |
|---|---|---|
| 1 | bad/forged gen cannot brick | **HOLD** — DryRun `nats-server -t` gates before any swap; `.bak` kept; gen never stamped into the conf |
| 2 | unknown directive → fail-closed, no auto-overwrite | **HOLD (now incl. subkeys)** — M1 extended Preflight to `cluster.*` / `authorization.*` / `auth_callout.*` subkeys (xkey refused) |
| 3 | observed REAL, never synthesized | **HOLD** — `observedConfirmed` false on nil/err/stale; only a real `/varz config_load_time ≥ swap` confirms |
| 4 | no DEGRADED flap, change-gated | **HOLD** — mesh-ENTER-only; m1 split-delete so VOTER_ADD_FAILED removal does not bump; m2 bus-nkey gated on mesh phase |
| 5 | bus-nkey bounded, never drops a voter | **HOLD** — empty bus_nkey ⇒ Unresolvable (no render); self-backfill bounded + now heals MISMATCH (m3) |
| 6 | 5th loop ctx-bounded + leak-clean + joined | **HOLD** — loopCount math correct; M4 fixed the per-tick http.Client leak (shared client + DisableKeepAlives); m9 threads loop ctx into reload |
| 7 | non-cluster byte-equiv / inert | **HOLD** — B2 fix gates `TopoReported` on `NatsConfPath != ""` (nil topoSelf when inert) so a non-managing broker reports nothing |
| 8 | gate presence-gated, not magnitude; can't override FORCE_SINGLE/QUORUM_LOST | **HOLD** |
| 9 | `--all` never bumps a generation | **HOLD** — poll-only |
| 10 | FSM determinism | **HOLD** — all-literal baked SQL throughout |

## BLOCKERs — disposition (ALL FIXED)

| ID | Finding | Fix |
|---|---|---|
| **B1** | Reconciler can NEVER render — `natscluster.Config` omitted CAFile/CertFile/KeyFile/ClusterListen/ClusterName; the "harvested by BuildMergedConf" comment was false. | **FIXED:** `Ownership.ClusterMTLS()` harvests the routes-mTLS identity from the LIVE conf's `cluster{}` block; `BuildMergedConf` populates the cfg from it when the caller (reconciler) leaves them empty, fail-closed if the live conf has no cluster TLS. Test `TestBuildMergedConfHarvestsClusterTLSAndMonitor` + `…FailsClosedWithoutClusterTLS` (the render success-path that never existed). |
| **B2** | `NatsConfPath` never wired in production → loop never runs, yet `TopoReported=true,Observed=0` marks every grown cluster DEGRADED forever. | **FIXED (both):** (primary) added `serveconf` fields `broker.cluster.nats_conf_path`/`nats_server_bin` + `serve.go` flags + a cluster-mode default `/etc/tether/nats.conf`. (defense-in-depth) gate `topoSelf` on `NatsConfPath != ""` — a non-managing broker passes `nil` topoSelf ⇒ `TopoReported=false` ⇒ the gate cannot wedge it. |
| **B3** | Manual cutover omits `MonitorListen` while the reconciler forced it → first reload hot-adds `http:` which nats-server rejects → wedged. | **FIXED:** the reconciler NO LONGER forces a monitor — `BuildMergedConf` PRESERVES the live conf's `http:` (`Ownership.MonitorHTTP()`), never hot-adding one. The manual takeover NOW emits `MonitorListen:"127.0.0.1:8223"`, so the monitor is established only at a cutover+restart. Test asserts the harvested `http:` survives. |

## MAJORs — disposition (ALL FIXED)

- **M1** `cluster.*`/`authorization.*` subkeys not fail-closed → **FIXED:** Preflight refuses unrecognized `cluster`/`authorization`/`auth_callout` subkeys (xkey). Tests `TestPreflightFailsClosedOnAuthCalloutXkey` + `…UnknownClusterSubkey`.
- **M2** `reconcile nats --wait` false-green on an unreachable voter → **FIXED:** `topoLaggards` now flags `!Reachable` and `!TopoReported` voters too (no exit-0 while `cluster status` is DEGRADED).
- **M3** render failure mis-classified transient → **FIXED:** `BuildMergedConf`/Render errors return `ActionRejected` (sys.event + STUCK marker), not `Unresolvable`.
- **M4** probe leaks a goroutine+fd per tick → **FIXED:** shared `topoProbeClient` built once with `DisableKeepAlives` (no stranded persistConn).
- **M5** no stuck-vs-behind banner → **FIXED:** `computeHealth` emits a topology-specific banner+next-step (STUCK → `reconcile nats --manual`; behind → `reconcile nats --all --wait`).

## MINORs — disposition

- **m1** VOTER_ADD_FAILED removal bumped topology → **FIXED** (split-delete: roster bumps for either phase, topology only for RETIRING).
- **m2** bus-nkey set on a non-mesh node bumped topology → **FIXED** (UPDATE gated on mesh phase).
- **m3** self-backfill healed only EMPTY, not MISMATCH → **FIXED** (`selfBusNkeyNeedsBackfill` compares to the derived pub).
- **m5/m10** `topoKick` dead code / `--all` inert → **FIXED** (deleted the channel; the 5s loop converges automatically; `--all --wait` is the convergence-watch anchor — no false "kick" claim).
- **m6** async reload → false `swapped_reload_pending` text → **FIXED** (reworded; next tick's fast-path re-confirms; ≤5s self-heal).
- **m7** sys.event spam every 5s → **FIXED** (change-gated on `(action,reason)`).
- **m8** empty ServerName guard → **FIXED** (step-2 guard + `SelfServerName != ""`).
- **m9** reload used `context.Background()` → **FIXED** (threads the loop ctx).
- **m11** "converged to gen 0" → **FIXED** (special-cased "nothing being managed").
- **m4** forward verb does not bind NodeID to sender → **ACCEPT-AS-IS (security-pragmatic):** within the trusted-voter boundary; the m3 heal-on-mismatch removes the only DoS leverage (a victim self-corrects). Documented.
- **m12** `reconcile nats` uses its own `--socket` not the parent persistent flag → **ACCEPT-AS-IS:** the command-level `--socket` works (standard cobra shadowing); defaults match. Documented.

## Tests added

`internal/natsconf/c3_harvest_test.go` — the render SUCCESS-path that never existed: BuildMergedConf harvests cluster mTLS + monitor (B1/B3); fail-closed without cluster TLS; Preflight refuses auth_callout.xkey + unknown cluster subkey (M1). Plus the existing engine (fail-closed/observed-honesty), topology-gen (mesh-enter-only/change-gated), and computeHealth-gate suites.

## Gate (post-fix)

`go build ./...` ✅ · `go vet ./...` ✅ · `internal/cluster -race` ✅ · `internal/broker -race` (topo/health/status/D9 regression) ✅ · all leaf packages ✅ · `cmd/tether` ✅ · `golangci-lint` ✅ 0 issues.

## Known follow-ups (non-blocking, tracked)

- REAL-subprocess reload smoke test (the §1 spike, codified as an automated gated test) — the manual spike PASSED + is recorded in c3-plan.md §1; an automated `test/c3` is deferred with the other post-1.0 e2e-matrix backfill.
- install.sh: render the loopback `http: 127.0.0.1:8223` monitor + a `nats_conf_path`/`nats_server_bin` in broker.yaml; usage.md + cluster-runbook.md: the auto-convergence flow + the "manual cutover establishes the monitor at the one restart" note.
- C3 → the e2e matrix (D9 backfill pattern).

## Closure re-verification (independent) — PASS

An independent closure auditor re-traced all 3 BLOCKERs + 5 MAJORs + 4 spot-checked MINORs against the FIXED source (not merely compiling) + ran the full targeted gate (5 packages PASS). Verdict: **all 3 BLOCKERs + 5 MAJORs genuinely closed; the m1 split-delete change()-chain is truth-preserving; no regressions or cosmetic-but-not-real fixes. C3 ready to stage.** Two non-blocking observations (both defensible, never a false-GREEN): (1) `topoLaggards` flags an explicitly-opted-out (`NatsConfPath=""`) broker as a laggard while `computeHealth` does not degrade on it — a false-RED on an explicit opt-out only (standard installs default every broker to manage its conf); (2) a cluster broker whose live conf has no `http:` monitor shows DEGRADED until a manual takeover establishes it — the correct loud expression of the acceptance gate, not a silent wedge.

**C3 internal review CLOSED (FAIL → all blockers fixed → closure-verified PASS). Code staged.**
