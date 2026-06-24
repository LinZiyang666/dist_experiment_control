# D9 internal review — ROUND 1 (6 Opus-4.8 reviewers, adversarial)

Stage-C internal review of the D9 production cutover. 6 reviewers, read-only (they may author test
ideas but not modify impl); the main process is the sole fixer. Round 1 of 3 (the user expanded the
internal review to 3 rounds). Round verdict: **FAIL → all BLOCKERs fixed → re-verified green**.

## Verdicts (as filed)
- **DB-ownership / write routing — FAIL**: the cluster-mode write cutover was INCOMPLETE (4+ mutators still hit the read-only handle).
- **Construction / detection / shutdown — FAIL**: detection truth-table sound, but serve.go double-opened the DB + the authcallout PIN path + a shutdown-window audit race.
- **Migration — CONDITIONAL**: core idempotency/ordering correct; the plan-mandated SQLite-busy probe was dropped.
- **nats.conf takeover — CONDITIONAL**: install.sh path round-trips; missing dry-run gate + case-insensitivity + ClientListen guard.
- **§17 observability / guards — FAIL**: the observe poll used a subject the broker nkey can't publish (false broker_down storm); only 2 of 4 §17 rows built.
- **Red team / cross-cutting — FAIL**: no runtime cert_fp cross-check; `cluster add` couldn't make an added node a usable expose home.

## BLOCKERs — all ADOPTED + FIXED

| # | Finding | Fix |
|---|---|---|
| 1 | `serve.go` unconditionally `storage.Open(dbPath)` + passed `DB` in cluster mode → TWO write pools on the WAL DB (violates C-1) | `broker.DetectClusterMode` gates `storage.Open` (skipped in cluster mode); `broker.New` FATALs on `clusterMode && cfg.DB != nil` |
| 2 | `session rm` (Tombstone + `dropSessionRows`) wrote RODB → every `session rm` HARD-FAILS in a cluster; `resumeSessionRm` too | `VerbSessionTombstone`/`VerbSessionDrop` via `tombstoneSession`/`dropSession` (PlanTombstone + PlanHardDelete existed since D2, never wired) |
| 3 | `reconcileOnRegister` (every register) wrote RODB via `proc.MarkExited` ×4 → replicated procs diverge each reconnect | routed through `markProcExited`; audit emitted ONLY on a committed transition |
| 4 | `handleProcEvent` exit case `proc.MarkExited(b.cfg.DB)` (the started case WAS routed; exit was missed) | `VerbProcMarkExited` (`PlanMarkExited`, idempotent on `WHERE status='RUNNING'`) |
| 5 | authcallout PIN provision/join: handler `DB=RODB`, seams NOT attached in cluster mode → PIN bootstrap fails / bypasses raft | forwarder built in `Run` BEFORE `installAuthCallout`; handler gets `NewProvisionSeam`/`NewJoinSeam` |
| 6 | observe poll published `ctrl.by.<self>.cluster-health.req` — `PermissionsForBroker` has NO `ctrl.by.*` pub → DENIED → empty replies → FALSE broker_down for every voter every tick | new broker-only `tether.v2.cluster.cursor.req` (`SubscribeClusterCursor`) under the `cluster.>` grant |
| 7 | no runtime `cert_fp` cross-check: a drifted on-disk tunnel cert vs seeded `cluster_nodes.cert_fp` → silent total data-plane outage | `wireClusterEarly` FATAL-refuses unless the loaded cert matches `cert_fp` OR `cert_fp_prev` (rotation window) |
| 8 | plan-mandated (OQ-2 "BOTH") SQLite-busy interlock probe dropped to runbook-only | `storage.ProbeWriterLock` (no-migration `BEGIN IMMEDIATE` busy_timeout(0)) added to the `--from-existing` interlock |
| 9 | `cluster add` seeded empty `nats_server_id`/`tunnel_addr`/`cert_fp` → an added voter can NEVER serve as an expose home (D6 rehome impossible) | CLI collects `--tunnel-addr`/`--cert-fp`/`--public-host`/`--nats-route`; `handleAdd` threads them; `nats_server_id = node_id` (SSOT) |
| 10 | takeover-natsconf had NO `nats-server -t` dry-run → an invalid merged conf bricks the broker on restart | `natsconf.DryRun` (writes a temp + validates) before `Apply`; `--skip-dry-run` escape hatch |
| 11 | §17 only built broker_down + raft_lag; `cluster status` stamped every peer `Reachable:true`; offline view hardcoded `Reachable:false` | row 3: real per-peer reachability + applied-lag via the broker-only cursor scatter-gather (honest "unverified" fallback); row 4: offline `:7400` raft-ping |

## MAJORs — dispositions
- **§9 audit non-exhaustive** → ADOPTED: re-derived mechanically by grep; §9.1 addendum records the corrections; verified no un-routed mutator remains.
- **shutdown WaitTransferAudit ordering** → ADOPTED: `transferAuditDraining` flag flips the sink SYNCHRONOUS at shutdown start, so a late event drains within `nc.Drain`'s callback wait (no goroutine spawned after the Wait).
- **reconcile-tick port revocation not leader-gated** → ADOPTED: leader-gated in cluster mode (like proc GC); `reconcileStates` stays per-broker LIVENESS.
- **seedClusterState identity guard too narrow** → ADOPTED: widened to all identity columns (name/raft/ident/server/tunnel/host/cert_fp).
- **natsconf case-insensitivity** → ADOPTED: `lowerKeys` recursively normalizes the parsed map.
- **ClientListen empty guard** → ADOPTED: takeover refuses an empty client-listen (would default-bind 0.0.0.0:4222).
- **proxy ready/directive not clusterMode-gated** → ADOPTED: explicit `b.clusterMode` early-returns (not only the replicated flag).
- **SecretsPreflight not at startup** → ADOPTED: run in `buildClusterRuntime` (FATAL on missing/unreadable key; FDE advisory).
- **rotate-tunnel-cert on-disk swap / hot-reload** → PARTIAL: the cert_fp check accepts current|previous so a DB-only rotate does not outage; full on-disk swap + live reload is a follow-up feature (operator places the new cert + restart). Noted.
- **Off byte-equivalence floor (golden-byte)** → DEFERRED with rationale: single mode is byte-identical by construction (every router falls through to the identical pre-D9 mutator) and the `make e2e` all-phases matrix exercises single mode end-to-end as the regression net; the seam-nil guard + the new cluster-mode write test bound the cluster branch. Round 2/3 may strengthen.
- **server_name SSOT cross-validation across init+takeover** → DEFERRED: takeover is offline (no DB); a mismatch surfaces at `nats-server -t` or the cert_fp/auth_callout check. Noted.
- **missing tests (half-run matrix, natsconf integration, Step-10b suite)** → PARTIAL: added `SessionRmCascadeRoutesThroughRaft`; the heavier gated suites are candidates for round 2/3.

## MINORs
- proxy ready/directive gates (folded into the MAJOR fix above).

## Re-verification after round-1 fixes
`make test` 0 · `make lint` 0 · `TestD9Matrix -race` (incl. the new `SessionRmCascadeRoutesThroughRaft`) 0. The full `make e2e` is re-run before round 3 sign-off.
