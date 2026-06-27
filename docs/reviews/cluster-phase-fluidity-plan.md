# cluster phase-transition fluidity — plan (v0.4.2)

> **Status**: Stage A finalized (main process is sole finalizer). Drafted by a 4-lens
> adversarial Workflow (3 drafters succeeded + 3 critics; the js-nats-topology drafter
> failed its schema cap, its surface was recovered by the critics + synthesis). This file
> is the authoritative implementation spec for Stage B.
>
> **Goal (user)**: 理顺 N=1 单机 → 双机 → 多机的 grow，以及多机 → 2 → 1 的 shrink，从代码层面
> 让这几个阶段顺畅流动；发布 v0.4.2 后全线更新。
>
> **⚠ RE-BASELINE (external-review F7, post-Fail→fix)**: this plan's original mechanism list is NOT
> the as-shipped scope. **Delivered**: online `set-raft-addr`/`set-route` (self-only + collision +
> write-free + **F5 mixed-version capability gate**), nonvoter-staging keystone + **F1 online cleanup
> of a failed/aborted staged nonvoter**, `OpClusterNodeReaddr/Route` (correct gen bumps), shrink
> `reconcile nats --to-standalone` (**F2 fail-closed N=1 machine gate**, **F3 JS-store_dir guard +
> post-apply proof**), **F4 N=1-standalone as a durable reconciler desired state**, **F6 bare-authority
> route validator**. **Deliberately DEFERRED → open plan items** (rationale in `architecture.md` 里程碑
> §cluster-phase-fluidity + the external-review reply): `cluster init` loopback-advertise guard,
> offline-doctor bind/advertise split, `join approve --check`, transport advertise/bind decouple
> (proven unnecessary — tether never reads the RPC-header address), shrink-as-replicated-operation, and
> the heavy multi-node gated `-race` drills (GrowAfterRebindNoWedge / ShrinkToSingleReRendersStandalone
> / production-mTLS-rebind / shrink→regrow) — covered at unit + reconciler level, multi-node real drills
> follow the project's gated-integration + pre-release 实机 pattern. See
> `cluster-phase-fluidity-{review,review-round2,external-review}.md`.

## 1. Problem

pc732 (the single live broker, public `155.98.36.32`) was migrated to a single-voter cluster
via `cluster init --from-existing`, which seeded **two persisted authorities** from broker.yaml
`cluster.raft_addr = 127.0.0.1:7400` (loopback):

1. the **raft Configuration** self-address (written once by `clusteroffline/init.go:227` →
   `cluster.BootstrapSingleNode(opts.RaftAddr)`, `offline.go:129-137`), and
2. the **`cluster_nodes.raft_addr`** column (seeded `init.go:280`).

At daemon start `cluster.New` always takes the `HasExistingState` branch in production, so it
loads the **persisted** loopback self-address; broker.yaml `cluster.raft_addr` is used ONLY as
the mTLS transport **BIND** (`cutover.go:199` → `ProductionConfig.RaftBind` → `transport.go:109`
`tls.Listen`). Growing N=1→N=2 by adding racknerd (different cloud) fails: racknerd replicates
pc732's committed config (`self@127.0.0.1:7400`) and dials its own loopback.

**Three defects, in priority order:**

- **D-WEDGE (keystone, root cause of the incident).** The join controller adds the joiner
  **directly as a VOTER** (`cluster_operation_controller.go:449` `driveJoin` `RaftAdding` →
  `AddVoter`, **no nonvoter staging**). Appending `{pc732,racknerd}` requires the NEW quorum (2);
  an unreachable racknerd means that config entry **never commits**, and hashicorp/raft refuses
  any further config change until it does → **pc732 hard-wedges** (cannot commit writes, cannot
  even online-`RemoveServer` the bad peer). This is almost certainly why force-single was reached
  for last time. *(pc732 is NOT in this state now — we never ran `join approve`; verified healthy
  N=1.)*
- **D-ADDR.** The only documented rewrite for the bootstrapped self-address is
  `cluster recovery force-single` (RecoverCluster, the "single most dangerous command"). Run on
  the LIVE daemon over flaky SSH it crashed pc732 and dropped 3 agents on fatal-auth. A routine
  grow must not need the quorum-loss escape hatch.
- **D-ROUTE (twin).** `nats_route` is the parallel advertise for the NATS route mesh
  (`topology_reconcile.go:134` → `natscluster.Broker.RouteURL` → `cluster{}` routes `:6222`).
  pc732's `nats_route` is almost certainly ALSO loopback, so fixing raft alone leaves the data
  plane split.
- **D-SHRINK.** The reverse (N≥2 → N=1 → standalone JS) is entirely unhandled: `natscluster.Render`
  always emits a `cluster{}` block (`config.go:108`), `jsstream.reconcileReplicas` is raise-only
  (`jsstream.go:246`), and there is no clustered→standalone nats.conf re-render.

**Verified core primitive (the fix's spine):** `raft.AddVoter` on an **existing** voter ID just
does `configuration.Servers[i].Address = newAddr` (hashicorp/raft@v1.7.3 `configuration.go:235-245`)
— an online, replicated config-change, **no wipe / no snapshot reset / no replay**. At N=1 the lone
leader rewrites its own committed self-address with quorum=1. `cluster.AddVoter` already exists
(`membership.go:32`); it is simply never used for self-rebind.

## 2. Scope decisions (finalized)

| Mechanism | In v0.4.2? | Note |
|---|---|---|
| A — online raft-addr rebind (force-single replacement) | **YES** | core |
| B — nonvoter staging in join (wedge prevention) | **YES** | keystone; the real root cause |
| C — preflight + doctor + init loopback guard | **YES** | stops operators reaching for force-single |
| D — nats_route twin fix (`set-addrs --raft --route`) | **YES** | data-plane grow fails without it |
| E — transport advertise/bind decouple field | **YES** | fresh-init grow-ready + clean status announce |
| Shrink → "N=1 single-voter cluster, standalone JS" | **YES** | the downgrade requirement |
| Full exit of cluster mode (clear `broker.cluster.*`, WAL → `storage.Open`) | **NO** | crosses DB-ownership invariant; separate epic |
| Offline rebind tool (`wipe raft/ + BootstrapSingleNode`) | **DROPPED** | unsafe (omits restore.go's applied_index/term/audit_published_index resets → write blackhole), duplicates restore.go, redundant — the online path covers healthy N=1; genuine wedge/quorum-loss is exactly force-single's job |
| `commandVersion` bump | **NO** | false dependency — envelope decoupled from proto; ~13 ops added D2–D8 without a bump |
| new migration | **NO** | `raft_addr`/`nats_route` are mutable TEXT since migration 0008; head is 0017 |
| proto bump | **NO** | `proto.ProtoVersion=2` governs only the agent/ctl wire |
| cert/PKI/SAN work | **NO** | route mTLS is chain-to-CA only (`transport.go:88-94`), no hostname/SAN |

## 3. Mechanisms

### A — ONLINE raft-addr rebind (replaces force-single for the routine case)
- New FSM op `OpClusterNodeReaddr` in `internal/cluster/command.go` (OpType const + `knownOps`
  entry; `commandVersion` unchanged). `PlanClusterNodeReaddr(nodeID,newAddr,now)` in
  `membership_ops.go` modeled **exactly** on `PlanClusterCertRotate` (line 344): change-gated
  all-literal `UPDATE cluster_nodes SET raft_addr=<lit> WHERE node_id=<lit> AND raft_addr != <lit>`;
  NUL/non-UTF-8 reject; loopback/unspecified reject. **NO `rosterGenBumpStmt`** — `raft_addr` is
  absent from the agent-facing roster SELECT (`cluster_roster.go:99`), so a raft_addr change must
  not force a spurious roster recompute/agent-push.
- `ClusterAdmin.SetRaftAddr(nodeID,newAddr)` in `clusterdrain.go` modeled on `RotateTunnelCert`
  (line 292): leader-gate with fail-fast naming the leader; structural host:port validate; refuse
  loopback/unspecified unless `--allow-loopback`; refuse while an op is in flight (`assertNoActiveOp`,
  controller line 27); **no-op if `RaftConfiguration()` self.Addr already equals target**.
- **Order = column-first then config**: `Propose(PlanClusterNodeReaddr)` (committed) **THEN**
  `node.AddVoter(nodeID,newAddr)`. Both idempotent so a crash-between heals on re-run.
- `--node` peer-rebind at N≥2 carries a **quorum hazard** (changes where the leader DIALS that
  peer): gate it — only after the peer has restarted listening on the new addr; refuse if the new
  addr equals another server's addr (raft rejects duplicate addresses anyway). **Self-rebind at
  N=1 (the pc732 case) is the only unconditionally-safe variant.**

### B — nonvoter staging (the keystone wedge-prevention)
- Add `Node.AddNonvoter(nodeID,raftAddr)` wrapper in `membership.go` (`raft.AddNonvoter` exists
  v1.7.3 `api.go:964`) alongside `AddVoter`.
- Change `driveJoin` `RaftAdding` (`cluster_operation_controller.go:441-454`): **`AddNonvoter`**
  to stage the joiner (no quorum impact → no wedge if unreachable); keep the existing `CatchingUp`
  barrier/deadline gate; **promote with `AddVoter` once caught up**. An unreachable joiner added to
  an N=1 cluster commits the add at the OLD quorum and simply never catches up → operator
  `RemoveServer`s it ONLINE. No wedge regardless of peer reachability.

### C — preflight + doctor (so nobody reaches for force-single by surprise)
- Online doctor (`cmd/tether/cluster_doctor_online.go`, fed from `RaftConfiguration` via
  `clusterstatus.go` `ClusterStatusReport`): **FATAL** when this leader's own committed self.Addr
  is loopback/unspecified before a cross-network grow, naming `cluster set-raft-addr` (NOT
  force-single); also surface a loopback `nats_route`. Add `join approve --check` read-only preflight.
- N=1 status banner when self-advertise is loopback (grow-blocked + next step).
- Offline doctor (`clusteroffline/doctor.go:68`): **split** the single `net.Listen` check —
  bind-check the BIND (`broker.yaml cluster.raft_addr`); for the ADVERTISE (`--raft-addr`) validate
  non-loopback/unspecified **WITHOUT** `net.Listen` (a public advertise IP may be unbindable locally).
- `cluster init`/`init --from-existing` CLI: refuse a loopback/unspecified `--raft-addr` advertise
  unless `--allow-loopback`. **Keep the library `InitFromExisting` permissive** so single-host test
  harnesses stay byte-equivalent (guard lives only in `cmd/tether`).

### D — nats_route twin fix
- `PlanClusterNodeRoute` (UPDATE `nats_route`, **WITH** `topologyGenBumpStmt` — unlike raft_addr,
  `nats_route` DOES feed the rendered conf, so reconcile must re-render + reload).
- Combined `cluster set-addrs --raft <h:p> --route <h:p>` (+ keep `set-raft-addr` as the raft-only
  subset) that updates both authorities and triggers `cluster reconcile nats`.

### E — bind/advertise decouple (fresh-init grow-ready + clean status)
- `internal/cluster/transport.go`: add `Advertise string` to `MTLSTransportConfig` + an advertise
  `net.Addr` to `tlsStreamLayer`; `Addr()` returns advertise-when-set else `s.ln.Addr()`; reject
  unspecified/loopback advertise (mirror raft `errNotAdvertisable`); **empty advertise ⇒ bind
  (byte-equivalent back-compat)**.
- Thread `AdvertiseAddr` through `production.go` `ProductionConfig` → `cutover.go`
  `buildClusterRuntime` → `broker.go` `Config.ClusterRaftAdvertise` → `serve.go` +
  `serveconf.go` `cluster.raft_advertise` (yaml) + `--cluster-raft-advertise`.
- **Why (not cosmetic):** `raft.go:39`/`:1471` — `trans.LocalAddr()` leaks into the RPCHeader
  `Addr`, which followers adopt as `leaderAddr`. With bind `0.0.0.0` and no advertise field, status
  / `LeaderWithID()` show `0.0.0.0:7400`. A FRESH grow-ready init wants bind `0.0.0.0` + advertise
  public; this field makes that clean. (Peers still DIAL by the committed config address, so this is
  not load-bearing for the join itself — it lands but is the lowest-priority piece.)

### Shrink → standalone JS
- `natsconf/preflight.go` `IsClusteredJetStream` (reverse of `IsStandaloneJetStream`).
- A **new standalone nats.conf render mode** (NO `cluster{}` block) in `natscluster`/`natsconf` —
  net-new since `Render` always emits `cluster{}`.
- `cluster shrink-to-single` op/state-machine (`operation_ops.go` OpKind + controller driver):
  refuse unless `NumVoters==1`; re-render nats.conf standalone; surface the clustered→standalone
  **JS-store reset** as an OPERATOR-GATED block with the exact command (mirror of grow §3a — jsstream
  is raise-only so R≥2 streams can't run on one node) — **never auto `rm -rf`**. Lands at the
  supported "N=1 single-voter cluster, standalone JS".
- N≥3→2→last-but-one: existing `cluster retire`/`driveRetire` already handles it (F==0 typed
  confirm, AllAtTarget gate, transfer-leader-first). No change.

## 4. Implementation sequence

1. `command.go`: `OpClusterNodeReaddr` + `OpClusterNodeRoute` consts + `knownOps`. `membership_ops.go`:
   `PlanClusterNodeReaddr` (no roster bump) + `PlanClusterNodeRoute` (topology bump), modeled on
   `PlanClusterCertRotate`. Unit goldens + change-gating + poison-skip-on-unknown-op.
2. `membership.go`: `AddNonvoter` wrapper + `SelfConfiguredAddr()` helper. Assert raft
   `ProtocolVersion>=3` invariant (so bind≠advertise self is tolerated at restart, `api.go:536`).
3. `cluster_operation_controller.go` `driveJoin`: `AddNonvoter` → catch up → `AddVoter` promote.
   Wedge-proof test.
4. `clusterdrain.go`: `ClusterAdmin.SetRaftAddr` (+ `SetNatsRoute`), leader-gate/validate/refuse-
   loopback/no-op-if-equal/column-first-then-AddVoter, idempotent.
5. adminsock seam: `protocol.go` `OpClusterSetRaftAddr` (+ `OpClusterSetRoute`) + Request fields +
   allowed-op-set; dispatch in `clusterstatus.go` `HandleCluster` (next to `OpClusterRotateCrt`
   line 631). Populate `ClusterStatusReport` self fields `RaftAdvertiseLoopback`/`NatsRouteLoopback`.
6. CLI `cmd/tether/cluster.go`: `cluster set-raft-addr [--node] <h:p>` + `set-addrs --raft --route`
   (ONLINE group, `leaderRedirect`); `cluster init` loopback-advertise guard + `--allow-loopback`.
7. Doctor: `cluster_doctor_online.go` FATAL on loopback self-advertise + loopback nats_route +
   `join approve --check`; `clusteroffline/doctor.go` bind-vs-advertise split; N=1 status banner.
8. (E) transport advertise: `transport.go` + thread `AdvertiseAddr` through production/cutover/
   broker/serve/serveconf.
9. Shrink: `natsconf` `IsClusteredJetStream` + standalone render mode; `cluster_natsconf.go`
   `warnClusteredJSShrink` + `reconcile nats --to-standalone`; `cluster shrink-to-single` op/driver.
10. Guards: extend `TestD9ProductionWiresNoCluster` to the new symbols; regression guard
    (`proto.ProtoVersion==2`, migration head 0017, new ops in `knownOps`).
11. Gated heavy drills (test/d9 or new dN, `-race` + in-repo NumGoroutine/fd leak gate, SERIAL):
    `RebindSelfRaftAddrOnline`, `GrowAfterRebindNoWedge` (distinct bind≠advertise addrs, joiner
    dials advertise, NATS mesh forms, agents stay connected), `AddNonvoterUnreachableNoWedge`,
    `ForceSingleNeverSetByRebind`, `ShrinkToSingleReRendersStandalone` + JS-reset-surfaced.
12. Docs: rewrite `docs/cluster-runbook.md` grow (lead with set-raft-addr + bind-restart +
    nonvoter-safe join), demote force-single to genuine quorum-loss, add shrink/de-cluster drill +
    the one-time pc732 procedure (inspect raft config FIRST) + the `:7400`/`:6222`
    firewall-before-bind-flip HARD gate; update `distributed-broker-architecture.md` §8/§17.

## 5. Test plan (highlights)

- Unit: `PlanClusterNodeReaddr` golden + idempotent no-op + asserts **NO** roster_gen bump;
  `PlanClusterNodeRoute` bumps topology_gen; both reject loopback/unspecified + NUL/non-UTF-8.
- Unit: unknown op poison-skips (applied_index advances, no SQL, no panic) — locks the no-bump claim.
- Unit: `SetRaftAddr` leader-gate/idempotent/refuse-while-op-active/updates-BOTH-authorities/
  partial-heal; `AddVoter`-on-existing changes `RaftConfiguration()` self.Addr in place (no snapshot
  reset); `AddNonvoter` adds a non-quorum-counting server.
- Unit: nonvoter-staged `driveJoin` promotes only after catch-up; `AddNonvoter` of an UNREACHABLE
  peer at N=1 still commits + is `RemoveServer`-able online.
- Unit: `cluster init` loopback-advertise guard + `--allow-loopback`; library `InitFromExisting`
  still permissive. Offline doctor bind-vs-advertise split. (E) transport `LocalAddr()==advertise`
  with listener on the bound ephemeral port; empty advertise==bind.
- Unit: `IsClusteredJetStream`; standalone render emits NO `cluster{}`.
- Integration (gated, -race, SERIAL): `RebindSelfRaftAddrOnline`; `GrowAfterRebindNoWedge` on
  **distinct bind≠advertise addrs** asserting the joiner dials the ADVERTISE AND its nats-server
  meshes (leader's public `:6222`) AND JS meta forms AND agents stay connected (no re-auth);
  `AddNonvoterUnreachableNoWedge`; `ForceSingleNeverSetByRebind`; `ShrinkToSingleReRendersStandalone`
  + JS-reset-surfaced (streams serviceable at the standalone shape).
- Guard: extended `TestD9ProductionWiresNoCluster` + the migration/proto regression guard.
- Pre-merge hard gate: `make test` + the gated dN matrix (`-race`) + `make lint`; concurrency-
  touching changes (`driveJoin`/`AddNonvoter`/`SetRaftAddr` Propose+applyMu) through the leak gate.

## 6. Migration / compat / live-fleet safety

**NO migration, NO proto bump, NO cert work.** `raft_addr`/`nats_route` are mutable TEXT since 0008;
`commandVersion` stays 2; route mTLS is chain-to-CA only so an advertise-IP change never touches
agents' tunnel `cert_fp` pins — the 3 live agents + the P13 proxy are insulated (an online rebind
mutates only the raft Configuration + `cluster_nodes.raft_addr`/`nats_route`).

**Live pc732 one-time procedure (post-v0.4.2 fleet update):**
1. Upgrade pc732 to v0.4.2 (new op dormant until invoked; cluster/non-cluster broker stays
   byte-equivalent — `DetectClusterMode` unchanged).
2. **INSPECT pc732's live `RaftConfiguration` FIRST.** If racknerd is present as an uncommitted
   voter → pc732 is WEDGED → genuine quorum loss → STOP the daemon, run
   `cluster recovery force-single --self-id pc732 --self-addr 155.98.36.32:7400 --confirm-peers-dead racknerd`
   OFFLINE (the daemon-stopped discipline violated last time; `--self-addr` also corrects the
   self-address), restart. **If healthy N=1 (current state, verified) → skip force-single entirely.**
3. Edit broker.yaml `cluster.raft_addr → 0.0.0.0:7400` (bind all) + `cluster.raft_advertise →
   155.98.36.32:7400`; ONE clean broker restart (agents reconnect on the same tunnel `cert_fp` —
   brief, NOT the fatal-auth crash; the integration drill proves this).
4. **Firewall `:7400` + `:6222` to peer IPs (racknerd) BEFORE the bind is externally reachable —
   HARD gate** (route mTLS is chain-to-CA only; a leaked route leaf = cluster takeover otherwise).
5. `tether cluster set-addrs --raft 155.98.36.32:7400 --route 155.98.36.32:6222` (online) +
   `cluster reconcile nats`.
6. `cluster join approve <racknerd-bundle>` — now succeeds via nonvoter staging.

**Mixed-version:** an un-upgraded voter poison-skips the new op (non-bricking) — harmless at the
current N=1 (no other voter); upgrade-all-voters-before-propose mandated for future N≥2; doctor
version-skew can gate.

## 7. Open items carried into Stage B / external review

- Confirm pc732's actual `cluster_nodes.nats_route` value (loopback?) at fleet-update time — the
  plan assumes it is loopback (same operator habit that seeded raft_addr loopback).
- Confirm pc732's original cluster-CA **signing** key still exists to sign racknerd's route leaf
  (`/etc/tether/secrets/ca-key.pem` was present at last inspection) — pre-grow preflight.
- The `--node` peer-rebind UX guard wording (self-only-at-N=1 is the unconditionally-safe variant).
