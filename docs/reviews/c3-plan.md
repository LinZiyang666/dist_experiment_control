# C3 Plan (FINAL) — Per-broker topology reconciler (auto-converge NATS topology)

> Stage-A output. 9-agent adversarial workflow (4 lens drafters → 4 critics → 1 synth; full raw `tasks/wn3m0xwl6.output`). Main process is sole finalizer. Doc-first per CLAUDE.md §2. The synth verified facts against the tree (preflight `bucketOf` has no `pid_file`; `nats-server --signal reload` resolves via `pgrep` no-pidfile; broker runtime imports neither natsconf nor natscluster today; `cluster.go:397` hardcodes "re-run takeover").

## 0. Finalization decisions (main process — binding)

| # | Decision | Rationale |
|---|---|---|
| **D-A reload** | **`nats-server --signal reload` (SIGHUP via the official `pgrep` resolver). NO `pid_file`.** Broker + nats-server both `User=tether` → same-uid signal, no root/ssh. | A `pid_file` directive self-bricks: `pid_file ∉ preflight bucketOf` → the reconciler's next Preflight refuses its own output. `--signal reload` needs no conf change, no Preflight patch, no pre-existing pidfile (works on the already-running D9 fleet). |
| **D-B observed** | **observed_generation comes from a REAL probe (`/varz config_load_time` must advance past the swap), NEVER "signal returned nil".** | nats-server applies reload ASYNC + can REJECT a non-reloadable delta (keeps old in-memory config). A bare SIGHUP confirms delivery, not acceptance — synthesized green would false-HEALTHY-HA a broker whose reload failed. The whole acceptance gate depends on observed being real. |
| **D-C reload-only** | **NO restart seam in C3.** The steady-state topology delta = add/remove a route + its `auth_users`/static-`users` entry — all SIGHUP-reloadable. An **additive routes reload PRESERVES existing `:6222` connections**, so JS meta/stream quorum among the existing N is untouched. | A restart seam is unimplementable for a non-root sibling unit (SIGTERM = clean exit, `Restart=on-failure` won't respawn) and defends an unreachable state. The first single→cluster cutover (which DOES need a restart, adding `cluster.listen`) is the operator's `reconcile nats --manual` path, not the reconciler. |
| **D-D fallback** | A non-reloadable delta or a failed probe → `swapped_reload_pending` / `observed < applied` (loud, NEVER HEALTHY-HA, operator escape), **never auto-restart, never synthesized green**. | Safe regardless of the spike outcome: a delta the live server can't reload degrades visibly + the conf is validated+swapped for a later restart. |
| **D-E storage** | migration `0014` adds `cluster_nodes.bus_nkey_pub` (the one fact `Render` needs that isn't replicated) + a dedicated monotone `topology_generation` counter in `cluster_meta`. | Reuse `roster_generation` would flap to DEGRADED on every routine cert-rotate (rostergen bumps on cert-rotate; cert-rotate changes nothing rendered). |
| **D-F backfill** | Each broker **self-writes its own `bus_nkey_pub`** (from its own `broker.nk`) via the D9 `proposeOrForward` path (`OpClusterBusNkeySet`, genericExecApplier). | Backfills pre-C3 voters (DEFAULT `''`), handles joiners, avoids touching the delicate `clusterNodeUpsertApplier`, and bumps the generation so peers re-render. Empty bus_nkey ⇒ reconciler is `unresolvable` (fail-closed, never drops a voter from `auth_users`). |
| **D-G bump sites** | `topologyGenBumpStmt` appended LAST (after `rosterGenBumpStmt`) on `PlanClusterNodePhase` **only when `newPhase==CATCHING_UP` (mesh ENTER)** + `PlanClusterNodeRemove` (mesh LEAVE) + `OpClusterBusNkeySet`. NOT `PlanClusterNodeUpsert` (len(Body)==1 custom applier) or `PlanClusterCertRotate`. | Within-mesh transitions (→VOTER/→DRAINING/→RETIRING) don't change the rendered conf → no spurious DEGRADED flap. Change-gated (`changes()>0`) preserves idle-zero-writes; the chain reads off the preceding bump (test it). |

## 0.1 Doc-first precondition (same PR)

Amend `distributed-broker-architecture.md` §11(h)/§17 BEFORE the code: §11(h) currently fixes "tether 不编排 systemctl". C3 has each broker `--signal reload` its OWN co-located nats-server. Authorize this explicitly, distinguishing a **same-uid local reload signal** from systemctl/ssh/root orchestration; record the security argument. Note the **D9 boundary reversal**: d9-plan scoped natsconf/natscluster as admin-tooling-only; C3 makes the broker runtime import them for the reconciler (no active guard test enforced the old boundary — only a doc line — but record the change + the new bounded-subprocess discipline).

## 1. Stage-A exit gate: the reload spike — ✅ RAN, PASS (nats-server v2.10.22)

The whole reload-only design hinges on SIGHUP reloading `authorization` (auth_callout users) + `cluster.routes` on the **deployed binary** (v2.10.22, `~/go/bin/nats-server`, matches install.sh:598). **Spike result (recorded `scratchpad/reload_spike.sh`):** starting from a conf that ALREADY has `cluster{routes:[]}` (the C3 steady-state shape — `natscluster.Render` unconditionally emits `cluster{}`), adding a route + an auth user and `--signal reload`:
- **server survives** the reload (no crash);
- **`/varz config_load_time` ADVANCES** past the swap → the probe-based observed_generation is viable;
- **the new auth user authenticates** → the `authorization`/auth_callout block reloads LIVE;
- the new route is attempted (log "Reloaded server configuration" + route dial) → `cluster.routes` reloads;
- a held client connection survives (the server keeps pinging it).

The ONLY non-reloadable case observed is adding `cluster{}` from ABSENT (the first single→cluster cutover) — which is the operator's `reconcile nats --manual` path, never the reconciler. **The reload-only design is empirically confirmed.**

**IMPLEMENTATION NOTE (load-bearing):** the broker's `/varz` probe targets the LOOPBACK monitor, but this (WSL-style) environment sets `HTTP_PROXY=http://127.0.0.1:7897` — a default `http.Client` would route the loopback probe THROUGH the proxy and get nothing (this exact failure appeared in the spike until `--noproxy`). The C3 probe seam MUST use an `http.Client{Transport: &http.Transport{Proxy: nil}}` (no proxy) for the loopback `/varz`, OR honor `NO_PROXY` for 127.0.0.1.

## 2. Mechanism (see synth §2 for the exhaustive detail)

- **migration 0014** `ALTER TABLE cluster_nodes ADD COLUMN bus_nkey_pub TEXT NOT NULL DEFAULT ''`.
- **`internal/cluster/topologygen.go`** — `TopologyGeneration(ro)` + `topologyGenBumpStmt(now)` (mirror rostergen: MAX-floored, `changes()>0`, all-literal deterministic).
- **`OpClusterBusNkeySet`** + `PlanClusterBusNkeySet(nodeID, busNkeyPub, now)` (genericExecApplier; leader-local `nkeys.IsValidPublicUserKey` fail-fast; `UPDATE … WHERE bus_nkey_pub != <lit>` + bump; change-gated).
- **`internal/natsreconcile`** (NEW pure leaf, imports natsconf+natscluster+clusternodes only) — `ReconcileOnce(Inputs, reload func() error, probe func()(time.Time,error)) Outcome`. Step machine: desired==0 noop → unresolvable (empty bus_nkey / self∉peers) → `natsconf.Preflight` fail-closed → `natscluster.Render`+`natsconf.BuildMergedConf` → if merged==current: applied=desired, probe-or-reload → else `natsconf.DryRun` (nats-server -t) reject-safe → `natsconf.Apply` (atomic .bak) → `reload()` → `probe()`: advanced ⇒ observed=desired, else `swapped_reload_pending`. **applied recomputed from on-disk conf bytes each pass; observed from the probe — both REAL.**
- **`internal/broker/topology_reconcile.go`** — a 5th cluster loop (NOT leader-gated; every broker reconciles its own conf), started in `wireClusterLate` (+1 loopCount, joined by `clusterShutdownOrdered`, ctx-bounded, leak-gated). Cheap fast-path: read `TopologyGeneration(RODB())`; render only on a real advance or an explicit kick (`topoReconcileKick`). `ListPeersForTopology` materializes rows fully before close (D6 deadlock lesson). Reload seam = ctx-bounded single-flight `exec.Command(NatsServerBin,"--signal","reload")`; probe seam = HTTP GET `/varz` → `config_load_time`. Persist `{applied,observed}` to `<ClusterDataDir>/topology.gen` (hint only); publish `nats_topology_*` sys.events.
- **monitor port**: `natscluster.Config.MonitorListen` → `Render` emits `http: "127.0.0.1:8223"` (loopback); add `http` to preflight `bucketOf` (TetherGen).
- **status**: `proto.ClusterHealthResp` += `TopoApplied/TopoObserved/TopoReconcileReason/TopoReported` (omitempty, schema 1→2); thread a `topoSelf` accessor into `clusterHealthResponder`. `adminsock.ClusterStatusReport` += `TopoDesired`; `ClusterNodeStatus` += the 4 fields. **`computeHealth` gate** (after FORCE_SINGLE/QUORUM_LOST early returns): `if topoDesired>0 && n.TopoReported && reached && n.Phase==VOTER && n.TopoObserved<topoDesired { degraded=true }` (incl. 0 — presence via `TopoReported`, NOT a `>0` magnitude guard). `TOPO` column in `renderClusterStatus`; two banners (stuck vs behind).
- **CLI** `cmd/tether/cluster_reconcile.go` — `cluster reconcile nats --all --wait` (transient broker-only NATS nudge, NEVER a gen bump; `--wait` polls all voters for `TopoObserved==TopoDesired && reason==""`, times out NAMING the laggard) + `--manual` (the demoted takeover engine verbatim). Demote `takeover-natsconf` → `reconcile nats --manual` (one-tag Hidden deprecated alias). Rewrite the hardcoded "re-run takeover-natsconf on EVERY broker" strings (`cluster.go:397/355`) → reconciler-converges messaging; `init --from-existing` keeps pointing at `reconcile nats --manual` (first cutover IS manual).

## 3. What C3 closes (建议1 acceptance, verbatim)

> - 新 broker 加入后，不需要人工登录所有 broker 重跑 NATS 配置。
> - 任一 broker 未完成 topology apply 时，集群不能显示为 `HEALTHY-HA`。
> - unknown `nats.conf` 指令继续 fail-closed，不能被自动覆盖。

Gap rows: Raft 存期望 route/generation 🟡→✅; leader 提交 intent + 本机 reconciler 执行 ❌→✅; 每台自动渲 nats.conf + 重启 ❌→✅; status desired/applied/observed ❌→✅; reconcile nats --all --wait ❌→✅ + takeover demoted; 未 apply 不显 HEALTHY-HA 🟡→✅. Closes success metric #1 (the topology auto-convergence part). **Out of scope (C4):** `cluster plan add`/`apply <id>`/`ops` log/`join prepare+approve` (the operation controller that CONSUMES C3's observed_generation).

## 4–6. File list / wire / tests

Full file-level change list in synth §4; FSM-determinism/byte-equiv/non-cluster-inertness in §5; named adversarial tests in §6. Load-bearing tests: `TestTopologyGenChaining` (multi-FSM determinism + idle-zero-writes of the bump-chain), `TestTopologyGenMeshEnterOnly` (within-mesh transition does NOT bump), `TestReconcileUnresolvableEmptyBusNkey` (fail-closed, never drops a voter), `TestReconcileDryRunRejectKeepsBak` (bad gen can't brick), `TestReconcileUnknownDirectiveFailClosed`, `TestReconcileObservedFromProbeNotSignal` (probe-gated, not signal-returned), `TestComputeHealthNotApplied NotHealthyHA` (incl. observed=0 voter), `TestReconcileNatsAllNoGenBump`, plus a **REAL-subprocess reload smoke test** (launch nats-server v2.10.22, SIGHUP-reload routes+auth_callout, confirm via /varz — the spike, codified). The gated `cN_integration` matrix uses the embedded server + injected reload seam (seam-coverage); the real reload is proven by the subprocess smoke test only.

## 7. Risks

The reload spike (§1) is the load-bearing empirical precondition — run it first. The 5th broker loop touches the live nats-server reload path (the riskiest production surface) — mitigated by reload-only + DryRun-gate + probe-based-observed + restart-pending fallback (a bad/non-reloadable delta degrades loud, never bricks). `bus_nkey` self-backfill is the hard prerequisite (same PR). Doc-first amendment (§0.1) lands first.
