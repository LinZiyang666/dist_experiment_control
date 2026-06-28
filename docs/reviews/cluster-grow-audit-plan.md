# cluster grow-onto-migrated-broker — 20-expert audit + fix roadmap

> Stage A of the grow-fix epic. 20-expert adversarial Workflow (102 agents, 10M tokens) audited the
> D0–D9 distributed-broker code + test coverage after the live pc732→racknerd grow failed with a
> cascade of bugs. **72 confirmed critical/high findings, 26 deduped.** Raw data: `cluster-grow-audit.json`.

## Root cause (one invariant break + a cascade on top)

`cluster init --from-existing` direct-seeds migrated v1 rows into SQLite (`clusteroffline/init.go`
`seedClusterState`) and `BootstrapSingleNode` (`cluster/offline.go`) lays only the `{self}` raft
config@1 with **no FSM snapshot** → the FSM is **NOT reconstructable from `(snapshot + log-from-1)`**,
which hashicorp/raft fundamentally assumes. A fresh joiner replays the log from index 1; entry ~14
(a `NodeRegister`, `nodes.sid → sessions(sid)`) references a direct-seeded row not created by entries
1..13 → `FOREIGN KEY constraint failed` → `fsm.go` plain error → 3-attempt FATAL panic → crash loop.

**v0.4.3 `SnapshotForJoin` is a confirmed NO-OP**: `raft.Snapshot()` will not compact a log shorter
than `TrailingLogs=10240` (`read.go:86`; gate `snapshot.go:226`), so `FirstIndex` stays 1, raft ships
the LOG not `InstallSnapshot`. Silent variant: if the post-init log is FK-safe, the joiner replays onto
EMPTY tables, passes the content-blind catch-up barrier, and is promoted as a **HOLLOW voter** (permanent
fork; instant data loss if pc732 is later drained to it) — strictly worse than the crash.

## Correct fix (respects L-2 raft-in-cluster-only, TrailingLogs/D5 audit window, proto v2)

- **STEP 0 — restore `FSM=f(snapshot,log)` at init.** In `internal/cluster` add a primitive that, after
  `BootstrapCluster` lays config@1, takes a full `fsm.Snapshot` (online SQLite backup of ALL seeded rows)
  AND `DeleteRange`-truncates the log so `FirstIndex` is past the seeded-dependent entries (exactly what
  `raft.RecoverCluster` does). `InitFromExisting` calls it AFTER `seedClusterState`; set
  `audit_published_index = snapshot index` (a v1→v2 migrant has zero prior cluster audit). → joiner
  `nextIndex` decays to 1, `GetLog(1)` → `ErrLogNotFound` → `SEND_SNAP` → `fsm.Restore` = full DB. No replay.
- **STEP 1 — one-time remediation for already-init'd pc732.** `tether cluster recovery resnapshot`
  (offline, daemon stopped): same snapshot+full-truncation, MINUS the force_single marker / config rewrite.
  MANDATORY audit guard `RecoverCluster` lacks: require `audit_published_index >= CommitIndex` before
  truncating (operator restarts daemon to let D5 drain, then resnapshots) or accept a bounded LOUD loss —
  never silent (protects the TrailingLogs/R-7 unpublished-audit window).
- **STEP 2 — content-digest parity gate before VOTER promotion.** Leader bakes a digest of the replicated
  tables at the barrier (same `VerifyLeaderRead`) via an all-literal genericExec raft op; controller refuses
  `setPhase(VOTER)` unless `joiner_digest == barrier_digest`. EXCLUDE §3.5 leader-local liveness columns
  (`last_heartbeat_at`/`status`) or honest followers false-fail. Catches the hollow-voter fork.
- **STEP 3 — fix/retire `SnapshotForJoin`.** Given STEP 0/1 it's redundant: keep as truthful no-op, fix the
  misdiagnosing comments (`node.go:411`/`controller.go:478` blame SnapshotThreshold; the real gate is
  TrailingLogs/compaction), rewrite `TestNode_SnapshotForJoinSwallowsNothingNew` to assert `LogFirstIndex`
  advances. (Optional online self-heal = `Node.CompactForJoin` after broker drives D5 publisher to the
  snapshot index — offline STEP 0/1 is the primary, safer mechanism.)
- **STEP 4 — reproducing gated d-test** (`test/d9`, external pkg may import `internal/cluster`): seed FK
  rows on the leader, commit an FK-referencing op THROUGH raft, drive the PRODUCTION grow path
  (`OpClusterJoinApprove`→`StartJoinOperation`→`driveInFlightOperations`, NOT the dead `AddNode`) with a
  REAL second `cluster.Node` (own empty DB, real mTLS `NetworkTransport`, real `caughtUpFn`); assert no
  panic, leader `LogFirstIndex>1` (InstallSnapshot used), joiner row-set == leader (no hollow fork).

## Prerequisite cascade — each independently blocks the LIVE grow (land alongside)

- **(A) carry `bus_nkey_pub` + `nats_server_id`(default=node_id) + `cert_fp`(auto-derived from joiner
  tunnel cert) through `JoinBundle`/`ToUpsertInput`/`PlanClusterNodeUpsert` at admission** — breaks the
  learner bus_nkey deadlock AND the empty-cert_fp `wireClusterEarly` crash-loop AND D6 home-ineligibility
  + cluster-wide topology-render stall. Mind the positional Aux cross-check splice (`membership_ops.go:177-193`).
- **(B) route certs with route-host IP/DNS SAN** + doctor/`SecretsPreflight` validator that PARSES
  `route-cert.pem` (currently only the tunnel cert is parsed) + fix the false runbook §1.0 "chain-only" claim.
- **(C) `/etc/tether` writable by `tether`**: `install -d -o tether -g tether -m 0750 $ETC_DIR` in install.sh
  + chown raft dir + recursive secrets; one-time `chown -R tether:tether /etc/tether /var/lib/tether/raft` for pc732.
- **(D) `natscluster.Render` fail-closed when not Standalone and rendered routes empty**; takeover + reconciler
  render Standalone when `peers==[self]` (covers N=1 grow-takeover AND N=2→N=1 retire brick); `http:` monitor
  stays emitted independent of `cluster{}`.
- **(E) catch-up gate distinguish "raft caught up, NATS unreachable" from "behind barrier"** in the BLOCKED
  reason; durable = raft-domain readiness probe over :7400.
- **(F) `defaultSeed = filepath.Join(secretsDir,'node-ident.nk')`** (join prepare/keygen/node-pub vs broker
  secrets_dir mismatch → FATAL preflight on the documented flow).
- **(G) gate boot history-stream + xfer-bucket orphan reapers leader-only + post-catch-up** (`audit.go:186-225`/
  `broker.go:869`). On a fresh joiner they read the local stale/empty FSM but hit the SHARED JS meta → **delete
  ALL cluster-wide JS audit streams** → first join wipes cluster history. CRITICAL data-loss; must land before any grow.
- **(H) per-cluster NATS isolation**: mint a fresh RANDOM cluster-id at bootstrap (NOT derived from the shared
  CA/account), render as the NATS cluster name + suffix ctl/auth/cluster-health queue groups → a
  separately-bootstrapped broker meshed on the shared bus cannot rogue-answer (the `session_not_found` flap).
  And/or sequence grow so routes don't mesh until raft membership commits.

## Other confirmed findings (not grow-blocking, fix in this epic)

- `cluster restore` re-bootstraps via `BootstrapSingleNode` (no snapshot) → DR re-grow crashes the joiner
  (same root) → route restore finalize through the STEP-0 path.
- `force-single` doesn't prune abandoned peers from `cluster_nodes` → survivor permanently INCONSISTENT after
  regrow (never-clearing broker_down alerts, unservable ports).
- auth_callout responder installs unconditionally on bus-connect (no caught-up/voter gate; fence covers
  partition not replication LAG) → a lagging/half-joined replica signs valid DENYs from its empty DB.
- B6 proto-version-skew gate is DEAD CODE; live `join approve` does no proto/schema gating → add proto +
  schema-floor to `JoinBundle`, enforce in `StartJoinOperation`.
- regular `tether expose` tunnels NOT auto-failed-over on unplanned home-broker crash (only proxies/drains) —
  D6 "data-plane failover" silently excludes exposes.
- `rotate-tunnel-cert` Previous/valid_until window inverted → regular expose that redials before re-register rejects.
- audit publisher: single global watermark + return-on-first-error → one stuck session head-of-line-blocks ALL audit.
- `cluster ops abort` stale predecessor-CAS → silent no-op-reporting-success.

## Test-gap summary (why every gate is green while the live grow is riddled)

The d1..d9 + phasefluidity harness substitutes away exactly the production realities that broke: (a) FRESH
EMPTY per-node DBs seeded only via the raft log → a leader NEVER carries migrated/direct-seeded rows (FK-on-replay
+ hollow-voter structurally unreachable); (b) localhost + a SINGLE shared in-process nats-server / `InmemTransport`
→ real multi-host route mTLS SAN, raft-up/NATS-down divergence, two-clusters-on-one-bus never exercised; (c) the
DEAD `AddNode`/`OpClusterAdd` path (unrouted, `protocol.go:108`) instead of the live `OpClusterJoinApprove`→driveJoin,
with phantom joiners + injected constant `caughtUpFn` → production `SnapshotForJoin`+`clusterCaughtUp` has ZERO e2e
coverage; (d) test-owned writable `t.TempDir` → the `/etc/tether` EACCES never hit; (e) confs are PARSED
(`nats-server -t`) but never BOOTED → the empty-routes-clustered N=1 brick invisible. Several tests ENSHRINE bugs:
`TestNode_SnapshotForJoinSwallowsNothingNew` asserts only no-error; `testD7ForceSingleRecover` asserts the roster is
NOT pruned; C8 join tests assert empty-cert_fp/nats_server_id is ACCEPTED.

New tests required: the seeded-leader grow d-test (STEP 4); route-mTLS SAN mesh test; natsconf-EACCES; boot a real
nats-server from N=1 Render (assert refused); clusterCaughtUp negative (raft-up/NATS-down); two-clusters-on-one-bus;
reaper no-cross-cluster-delete; bus_nkey-at-admission; content-digest hollow-voter; guard tests (defaultSeed,
install.sh /etc perms); + one heavy gated N-real-broker-processes harness (the automatable runbook drill).

## v0.4.4 implementation order (grow-blocking + data-safety first)

1. STEP 0 init snapshot+compact (keystone) + route `restore` finalize through it.
2. (G) reaper leader-only+post-catch-up gate (data-loss guard).
3. STEP 1 `cluster recovery resnapshot` (pc732 remediation, with audit guard).
4. (A) bus_nkey + nats_server_id + cert_fp at admission.
5. (B) route cert SAN + doctor validator + runbook fix.
6. (C) /etc/tether perms (install.sh + one-time pc732).
7. (D) N=1 render Standalone / fail-closed empty routes.
8. (F) node-ident defaultSeed.
9. (E) catch-up BLOCKED reason (mesh vs barrier).
10. STEP 2 content-digest parity gate.
11. (H) cluster isolation (rogue responder).
12. STEP 3 SnapshotForJoin comments/test honesty.
13. STEP 4 + new tests; Stage-C review workflow; `make test`/targeted gated matrices; release v0.4.4; re-do grow.
