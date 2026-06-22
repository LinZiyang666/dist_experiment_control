# D3 Plan — NATS cluster layer (≥2 nodes) · FINALIZED (main process is sole finalizer)

> **Provenance**: Stage A adversarial workflow (5 expert drafters → 5 adversarial critics
> → 1 synthesizer; 11 Opus 4.8 agents, ~1.2M tok, run `wf_9c97debb-c16`) produced the
> candidate. This file is the **main-process finalized plan-of-record** (CLAUDE.md §3
> step 2). Every load-bearing raft v1.7.3 / nats-server v2.14.0 / repo fact below was
> **independently re-verified by the main process** (citations inline); the candidate's
> two open questions on agent reconnect + embedded route-mTLS were **resolved here** (see
> §Resolved); one genuine runtime unknown remains and is **elevated to a mandatory de-risk
> spike (R0)** that gates the rest of D3.
>
> **Doc anchors** (`docs/distributed-broker-architecture.md`): §2 (line 52), §3.2
> (70-72), §3.3 (74-79), §4.1 (119), §6.1 (149), §6.2 (152-161), §8.4 (230-232), §13.8
> (318), §18.2.16(item 16), §18.1, §19-D3 (520-524) + 关键依赖警告 (575-582) + per-phase checklist
> (589-596).

---

## D3 in one paragraph

D3 makes the trust surface **multi-node**: a real mTLS raft `NetworkTransport` (replacing
D1/D2's `InmemTransport`), a tetherd-owned `internal/natscluster` conf renderer (routes
mTLS + shared-account Issuer + every broker pub in every `auth_users` + static nkey
permissions including the RF1 `cluster.*` ACL + deterministic `server_name` + one JS
domain), the **one** fail-closed `T_fence` predicate on `cluster.Node`, auth_callout
already-provisioned reads served from the local replica (not via the leader) and gated by
that predicate, and PIN-writes routed through `Node.Propose` (leader-only). Like D2, D3 is
**build-and-prove only**: production `cmd/tether/serve.go` stays single-node direct-mutator
and constructs no `cluster.Node`; everything is proven by real ≥2-node test harnesses. No
`apply.*` write forwarding (D4), no dynamic membership / AddVoter / join-PoP (D7), no
`cluster_nodes` writes (D7), no production cutover / nats.conf takeover (D9).

---

## §0 — RULINGS

### D3-R0 — DE-RISK SPIKE FIRST (gates the whole phase): prove cross-server `$SYS.REQ.USER.AUTH` callout over real routes before committing the §13.8-a harness shape.

The single central D3 exit-criterion is "双节点跨服务器 callout 授权通" (§19-D3 line 524) —
client connects to nats-server A, broker B (in queue group `tether-authcallout`) answers,
A accepts B's account-signed response. §6.1 line 149 **assumes** this works. But
auth_callout requests ride the system account's `$SYS.REQ.USER.AUTH`; whether a
foreign-server queue-group responder receives them across a route in non-operator static
auth_callout mode is **not verified by any nats-server test we could cite** (the cited
`TestAuthCalloutServerClusterAndVersion` is single-node). **First Stage-B action**: stand
up two real routed embedded servers + a cross-server callout and confirm B answers A.
- If it works → proceed; the §6.1 "every pub in every `auth_users`" design holds.
- If `$SYS` system-account subjects do NOT propagate to a foreign responder → the fallback
  is a **co-located responder per server** (each broker answers only its own server's
  requests), which is still a valid ≥2-node design but **changes §6.1's rationale** and
  becomes a doc-first amendment. We do not pre-commit the harness until the spike resolves
  this. (User preference: 本地先测好再推进 — applies doubly to the load-bearing capability.)

### D3-R1 — SCOPE/CUTOVER: build + prove the ≥2-node trust surface via tests only; production `serve.go` stays single-node direct-mutator; the auth_callout seams default to today's exact behavior when no `cluster.Node` is injected. (Mirrors D2-R1, arch line 517.)

D3 delivers — and proves only by tests driving real ≥2-node harnesses — (a) the mTLS raft
`NetworkTransport` + static multi-node bootstrap; (b) the one fail-closed predicate on
`cluster.Node`; (c) auth_callout already-provisioned read = local-replica read gated by the
predicate, PIN write via `Node.Propose`; (d) the `internal/natscluster` conf renderer;
(e) the RF1 `cluster.apply.>`/`cluster.>` ACL. Production `cmd/tether/serve.go` constructs
**no** `cluster.Node` (verified: it does not today, and D3 does not add one); the handler
gains optional seams that **default to today's exact behavior when nil** (zero regression).
Justification: §19-D3 "做" (line 522) lists conf-templating + read-local + fail-closed +
RF1 ACL — **not** "embed `cluster.Node` in `broker.New`"; the cutover + single-WAL merge is
§19-D9 (`cluster init --from-existing`, §3.8 line 109). Cutting production over would pull
D9 forward (违反先父后子). **Locked by a guard test** asserting `serve.go` builds no Node
and wires no seam (so R1 is a standing invariant, not a one-time claim).

### D3-R2 — FAIL-CLOSED PREDICATE: ONE monotonic-clock predicate on `cluster.Node`. Stateless leader branch; follower/candidate via `raft.LastContact()`; `allow iff age ≤ T_fence`; **never** `VerifyLeader` on the read path. Residual stale-leader fail-open window is bounded `LeaderLeaseTimeout + T_fence`, accepted (NOT engineered around).

Place alongside `BoundedStaleRead`/`VerifyLeaderRead`/`AppliedIndex` in
`internal/cluster/read.go` (**verified** these three exist there; this is the established
read seam — no greenfield file):

```go
// advisory signature; main process writes the code
func (n *Node) LeaderContactStale(now time.Time) bool // true => fence (deny)
const TFence = 10 * time.Second
```

Dispatch on `n.raft.State()`, **leader check first**:

- **Leader → fresh (never stale), STATELESS.** No timestamp, no `LeaderCh` goroutine.
  **Verified**: raft's leader-lease loop (`raft.go:937` `maxDiff := r.checkLeaderLease()`,
  `checkLeaderLease` at `raft.go:1036`) calls `setState(Follower)` the moment it can't
  contact a quorum within `LeaderLeaseTimeout` — so `State()==Leader` **is** proof of
  quorum contact within the lease. **Rejected** the drafters' `lastLeaderConfirmed`
  stamped from `LeaderCh()`: **verified** `LeaderCh()` (`api.go:1106`) "delivers signals on
  acquiring or losing leadership" = transitions only, so a freshness age derived from it
  grows unbounded on a healthy long-lived leader and would false-fence it; it also adds a
  leak-prone goroutine.
- **Follower / Candidate → `age = now.Sub(raft.LastContact())`; stale iff `age > T_fence`.**
  **Verified** `LastContact()` (`api.go:1126`) "returns the time of last contact by a
  leader" and is set on every AppendEntries. Zero-value (never heard a leader) → stale
  (fail-closed). Candidate ages from last leader contact like a follower (a benign 1–3 s
  election must NOT spuriously deny every already-provisioned reconnect — that would defeat
  T_fence's 10 s margin, §8.4 ①).

**Step-down reset hole — accepted as bounded, documented, NOT closed.** **Verified**:
`setLastContact()` is called at step-down (`raft.go:510`), so a demoted ex-leader's
follower-clock restarts at demotion. Worst-case authorize-while-isolated window =
`LeaderLeaseTimeout (≤500 ms) + T_fence (10 s)`. We accept it: bounded, monotonic, and
§8.4(b) already concedes force-single is "不是反脑裂结构性保证 / 若 B 仅被分区但活着可能双写".
The doc amendment must state the TRUE window (`LeaderLeaseTimeout + T_fence`, not bare
`T_fence`) so the external reviewer is not misled. We do **not** invent a self-maintained
quorum-contact timestamp (it would duplicate raft's lease logic and risk divergence).

Boundary: **allow iff `age ≤ T_fence`; fence iff `age > T_fence`** (strict `>`, matches
§3.2 line 72 / §8.4 "> T_fence 即停止 authorize"). No `VerifyLeader` on the read path:
§6.2 line 152 is explicit — already-provisioned read is "失 quorum 不锁死、不经 leader";
`VerifyLeaderRead` (**verified** `read.go:20` calls `raft.VerifyLeader()`) would
block/deny every reconnect during any election. The predicate is purely self-observed (one
local monotonic clock). `VerifyLeaderRead` stays reserved for §3.2 correctness reads
(force-single peer health, revocation) = D7. The predicate fences on **leader-contact
loss**, not on apply liveness (a wedged-FSM leader still reads fresh; that is D1 fail-stop's
job, not the fence's).

Clock seam: `LeaderContactStale(now time.Time)` takes injected `now` for tests; prod passes
`time.Now()`. An injected `time.Date(...)` test clock carries no monotonic reading, so the
boundary test exercises wall-clock subtraction (fine for determinism); the prod path's
monotonicity is a code-comment invariant (NTP-step immunity).

### D3-R3 — PIN-WRITE PHASE BOUNDARY (refined by main process after verifying agent client behavior): D3 ships only the SECURITY half — leader-local `Node.Propose` succeeds (allow post-commit); follower / deposed-leader returns a deny that does NOT false-allow and does NOT do an un-replicated direct write, with the deny reason correctly classified transient (`not_leader`) vs permanent (bad PIN). D3 does **NOT** change the agent client's terminal-on-auth-deny behavior and does **NOT** claim transparent client recovery. Transparent forwarding + any client/forwarder retry is D4.

A PIN-join is a WRITE (`agentprov.ProvisionWithPIN` / `session.JoinWithPIN`, verified in
`handler.go`). §6.2 line 153: "leader 验 PIN + 提议 provision entry，allow 门控在 entry
提交后". In D3 multinode: on the leader, the handler's PIN path runs
`Node.Propose(PlanProvisionWithPIN / PlanJoinWithPIN)` (reusing the D2 planners) → commit →
allow. On a follower, `raft.Apply` returns `ErrNotLeader` UNWRAPPED (**verified** node.go:186
contract) → handler maps to a deny **classified transient**; on a deposed leader,
`ErrLeadershipLost` mid-Apply → same. It must NOT direct-`db.Exec` (un-replicated
split-brain write) and must NOT false-allow.

**Main-process refinement of the candidate's "retriable deny → client reconnects to
leader" framing** (resolves the candidate's Open Q2): **verified** the agent connect loop
(`internal/agent/agent.go:617-633` + `isAuthFailure` at `:1012`) treats **any** auth
rejection — matched on substrings "Authorization Violation" / "authorization violation" /
"nats: Authorization" — as **TERMINAL** ("Auth failures are NOT transient ... fail every
retry forever") and does **not** retry. Therefore an auth_callout DENY returned to a real
agent today **kills the agent**, not retries it. So in D3 we do **not** rely on client-side
retry and do **not** change `isAuthFailure` (broadening it would make a genuine bad-PIN
flap forever). The §13.8 "返回可重试 deny" requirement is satisfied at the **callout
boundary**: the test asserts (i) the response is a DENY (not allow), (ii) the deny reason is
classified retriable/transient, (iii) **no** `agent_provisioning` row is written on **any**
replica. The transparent path — follower **forwards** the PIN write to the leader so the
client never sees `not_leader`, and/or a D4-aware client retries a transient deny — is
**D4** (§4.1 line 119, §19-D4 line 528). 先父后子-legal: D3 publishes no
`tether.v2.cluster.apply.<verb>` (verified zero forwarding code exists). `nil` `Propose`
seam ⇒ production keeps today's direct `ProvisionWithPIN(h.DB, …)` (zero regression). The
deposed-leader retry-safety is further covered by idempotent `INSERT OR IGNORE` + the D1/D2
`r:ReqID` dedup.

### D3-R4 — NATS CLUSTER CONFIG: tetherd-owned `internal/natscluster` renderer (NOT install.sh); per-server completeness of `auth_users` AND static nkey permissions (R3F3); the `$SYS.REQ.USER.AUTH` subscribe becomes a QueueSubscribe; install.sh / serve.go conf takeover is D9.

The renderer emits, per server: `server_name=<deterministic>`,
`cluster{name, routes (flat full-mesh), tls{ca_file,cert_file,key_file,verify:true}}`, one
`jetstream{domain}`, `authorization.auth_callout{issuer=accountPub, account, auth_users=[EVERY broker pub]}`,
and a static `nkeys`/`users` entry per broker pub with `permissions=PermissionsForBroker()`
(incl RF1). Per §6.2 line 158 (R3F3) BOTH `auth_users` AND static permissions must be
present; per §6.1 line 149 **every** broker pub must be in **every** server's `auth_users`
(so B can answer A). install.sh keeps its minimal single-node conf (**verified** lines
673-688: no auth_callout, no routes today); D3 builds + golden-tests the renderer but does
NOT wire it into install.sh or rewrite the live conf (= D9 §11). Test mTLS = ephemeral
in-process CA + per-server leaf; prod material (§15 `/etc/tether/secrets/cluster-ca.pem`)
is provisioned by D7/D9 `cluster add/create`, not D3.

**One legitimate production-path code change**: `internal/broker/authcallout.go:84` is plain
`nc.Subscribe("$SYS.REQ.USER.AUTH", …)` (**verified**) — D3 changes it to
`nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", …)` (§6.2 line 152 names the
queue group). Without it, two routed brokers double-answer every callout. This is
zero-regression at N=1 (a one-member queue group behaves identically to a plain
subscription).

### D3-R5 — RAFT TRANSPORT: custom `tlsStreamLayer` (`net.Listener` + `Dial`) → `raft.NewNetworkTransportWithConfig`. There is NO `raft.NewTLSTransport`. Static multi-node bootstrap only (AddVoter / join-PoP = D7). `Node.Shutdown` closes the injected transport itself.

**Verified** (this killed two drafters' approach): raft v1.7.3 has **no** `NewTLSTransport`
— only `NewNetworkTransport` / `NewNetworkTransportWithConfig` (`net_transport.go:211,251`)
+ `NewTCPTransport*` (`tcp_transport.go`), and `StreamLayer = net.Listener + Dial`.
Implement `tlsStreamLayer` wrapping a `tls.Listener` (cluster-CA,
`RequireAndVerifyClientCert`) for `Accept()` and `tls.Dial` for `Dial`, pass it to
`NewNetworkTransportWithConfig`; inject at the existing `cluster.Config.Transport` seam
(node.go:34) so D1/D2 tests keep `NewInmemTransport` untouched. **Verified** `*NetworkTransport`
implements `RequestPreVote` (`net_transport.go:478`), so PreVote (`PreVoteDisabled=false`,
node.go:162) survives the real transport — but re-prove it (test below). Static bootstrap:
extend the `prevote_test.go` `BootstrapCluster(rc, …, raft.Configuration{Servers:[all peers]})`
pattern via a **test-only** multi-peer entry (e.g. `Config.BootstrapPeers []raft.Server`);
production `New()` keeps `{self}`-only (verified node.go:131-141). **No** `raft.AddVoter` /
join-PoP (= D7). `Node.Shutdown` must call the injected transport's `Close()` itself —
`raft.Shutdown()` does not reap an injected transport.

### D3-R6 — TIMEOUTS: multinode `Heartbeat = Election = 1000 ms, LeaderLeaseTimeout = 500 ms`; `TFence` is a decoupled constant with an invariant test; re-running D1 kill-9 + D2 DIFF-1/`TestD2Matrix` GREEN under the new config is a BLOCKING pre-gate.

**Verified** config validation (`config.go:369,372-373`): `LeaderLeaseTimeout ≤ HeartbeatTimeout`
and `ElectionTimeout ≥ HeartbeatTimeout` (equality allowed). Current `raftConfig` is 200 ms
across the board (node.go:169-171, comment "D3 tunes them for multinode"). Pick
**Lease = 500 ms (raft default), NOT 1000 ms** — a longer lease *widens* the R2 residual
stale-leader fail-open window, so a 1000 ms lease gets safety backwards. Parameterize the
three timeouts via `Config` knobs so D1/D2 **N=1** tests keep their fast values; decouple
`TFence = 10 s` into its own constant with a `TestFenceExceedsWorstCaseElection` invariant
pinning `TFence ≥ k_fence(10) × ElectionTimeout(1000 ms)` at PROD constants (so test-only
timeout shortening cannot drift from §8.4). T_fence margin phrased as "≥3× a typical 2–3 s
PreVote election", not a precise worst-case. **Pre-gate** (§19 checklist line 591): before
D3 implementation proceeds past the transport change, re-run D1 kill-9 + D2
DIFF-1/equivalence/`TestD2Matrix` under the new config — all green — else the retune
silently breaks the D2 regression base.

### D3-R7 — RF1 ACL SURFACE: literals are version-PREFIXED `tether.v2.cluster.apply.>` / `tether.v2.cluster.>`, SSOT in `internal/proto/subjects.go`; consumed only by `PermissionsForBroker()` (Pub AND Sub); member/agent/unactivated templates get nothing; the static guard matches EXACT literals.

**Verified** the doc is self-consistent: §4.1 line 119 (the actual forwarding subject) uses
**`tether.v2.cluster.apply.<verb>`** (prefixed); §6.2/§13.8 write bare `cluster.apply.>` as
RF1 shorthand for the same subject; §2 line 52 already says "`PermissionsForBroker()` 加
`cluster.apply.>`+`cluster.>` pub/sub". So the literal is **version-prefixed**, SSOT in
`internal/proto/subjects.go` (where `SubjectPrefix = "tether.v2"` lives), and the D4
forwarder will share the const. Add `SubjClusterApplyPrefix = SubjectPrefix+".cluster.apply"`,
`SubjClusterPrefix = SubjectPrefix+".cluster"`, and a `SubjClusterApply(verb)` builder.
`internal/auth/permissions.go` keeps its off-SSOT `subjectPrefix` copy (**verified** line 11,
guarded by `TestSubjectPrefixInSyncWithProto`) and adds `subjectPrefix+".cluster.apply.>"`
+ `subjectPrefix+".cluster.>"` to `PermissionsForBroker()` **Pub AND Sub** (the D4 leader
subscribes to receive forwards, the follower publishes them). The "no `cluster.*` in user
templates" guard matches the **exact literals**, not a `cluster.` substring.

### D3-R8 — `nats_server_id`: D3 renders a deterministic `server_name` ONLY; ZERO `cluster_nodes` writes; no agent self-report / NUID bridge redefinition.

Migration 0008 (**verified**) says the first writer of `cluster_nodes` is D7's
`ClusterNodeUpsert`, and `nats_server_id` is "NULL until D3" — meaning D3 *defines* the
deterministic name, not that D3 *writes the column*. D3 produces the deterministic
`server_name` string in the conf only; tests needing a server_id↔node mapping insert via
fixture, not a production write path. §6.5's "agent self-reports the N… nuid" + authoritative
home assignment is **D6** — D3 does NOT "resolve" §6.5 by switching the bridge key to
`server_name`. D3 renders a deterministic name and stops.

---

## Doc-first amendments (edit §0–§18 main text BEFORE code; §18/§19 are audit/decomposition)

1. **§6.2 / §3.2 (70-72) / §8.4 (230-232)** — pin the predicate's leader/follower asymmetry
   per raft v1.7.3: leader = stateless `State()==Leader → fresh` (because `checkLeaderLease`
   auto-demotes a quorum-lost leader, raft.go:1036/937); follower/candidate =
   `now − LastContact()` (LastContact follower-only, api.go:1126); cold-start zero → fenced;
   **state the true worst-case authorize-while-isolated window = `LeaderLeaseTimeout(500 ms) +
   T_fence(10 s)`** (the step-down `setLastContact()` reset, raft.go:510) as an accepted
   bounded fail-open. Read path NEVER calls `VerifyLeader`; `VerifyLeaderRead` is for §3.2
   correctness reads (D7). Predicate fences on leader-contact loss, not apply liveness.
2. **§6.2 (153) + §13.8 (318)** — D3 PIN path is the **security half only**: leader-local
   `Propose` (allow post-commit); follower / deposed-leader returns a deny classified
   transient (`not_leader`), NO false-allow, NO un-replicated write — asserted at the
   **callout boundary**. The agent client today treats auth-deny as terminal
   (agent.go:617), so D3 does NOT claim client recovery; transparent `apply.*` forwarding +
   client/forwarder retry are **D4**. Clarify "返回可重试 deny" = the deny REASON is
   classified retriable, not a D3 client behavior.
3. **§6.1 / §19-D3** — conf templating = tetherd's `internal/natscluster` renderer + golden
   tests; install.sh / serve.go conf takeover is D9 §11. The `$SYS.REQ.USER.AUTH` subscribe
   becomes queue group `tether-authcallout` in D3. **If R0's spike shows cross-server `$SYS`
   does not propagate**, amend §6.1's "every pub in every `auth_users`" rationale to the
   co-located-responder fallback.
4. **§8.4 / raftConfig** — record the multinode retune (Heartbeat = Election = 1000 ms,
   Lease = 500 ms), `TFence` as a decoupled constant with the invariant
   `TFence ≥ k_fence × ElectionTimeout`, and that static bootstrap is D3 while
   AddVoter / join-PoP is D7.
5. **§1.3 / §18.2.16(item 16) / §18.1** — close the raft-transport-mTLS fork: D3 raft `:7400` uses
   cluster-CA X.509 (static); node-identity nkey-leaf pinning of BOTH raft and route leaves
   is deferred to D7 join-PoP. §18.2.16 item 16 currently implies route leaves are nkey-pinned — amend
   to "D3 ships CA-only routes; nkey-pinning is D7" so a reviewer holding §18.2.16 does not FAIL
   D3.
6. **§19-D3** — append a "D3 范围定稿" line mirroring §19-D2 (line 517): build+prove,
   ops-only, no `apply.*` forwarding (D4), no `cluster_nodes` writes (D7), no production
   cutover (D9); predicate stateless-leader monotonic; nil seams keep production
   byte-unchanged.

---

## File-by-file work breakdown

### New
- `internal/cluster/transport.go` — `tlsStreamLayer` (`raft.StreamLayer`: `net.Listener` +
  `Dial`, cluster-CA mTLS, `RequireAndVerifyClientCert`) + `NewMTLSTransport(cfg)` via
  `NewNetworkTransportWithConfig`.
- `internal/natscluster/config.go` — `Render(roster, certPaths) (string, error)`: routes
  mTLS + shared-account Issuer + every broker pub in every `auth_users` + static nkey
  permissions = `PermissionsForBroker()` + deterministic `server_name` + one JS domain.
  Pure rendering; provisions no certs, writes no disk.
- `internal/natscluster/config_test.go` — per-server completeness golden (all N broker pubs
  in each conf's `auth_users` AND static perms with the exact `cluster.*` literals) +
  omission-detected variants.
- `test/d3/setup_test.go` — `startRoutedAuthCluster(t, n)` extending `test/p4`'s
  `startAuthNATS` to N routed servers (`opts.Cluster` + `RoutesFromStr` +
  `opts.Cluster.TLSConfig` route mTLS + every broker pub in each `AuthUsers`/`Nkeys`),
  brokers `QueueSubscribe("tether-authcallout")`, poll until each server reports `n-1`
  routes before returning.
- `test/d3/*_test.go` — the §13.8 suite (below).

### Edited
- `internal/cluster/read.go` — add `LeaderContactStale(now)` + `const TFence` next to
  `BoundedStaleRead`/`VerifyLeaderRead`/`AppliedIndex` (R2; reuse the seam).
- `internal/cluster/node.go` — parameterize raftConfig timeouts (multinode
  1000/1000/500 ms; N=1 keeps fast via Config knobs); test-only `BootstrapPeers`;
  `Shutdown` closes the injected transport; keep `PreVoteDisabled=false`; drop the stale
  "D3 tunes them" comment.
- `internal/proto/subjects.go` — `SubjClusterApplyPrefix` / `SubjClusterPrefix` +
  `SubjClusterApply(verb)` (version-prefixed SSOT).
- `internal/auth/permissions.go` — add `cluster.apply.>` + `cluster.>` to
  `PermissionsForBroker()` Pub+Sub only; exact-literal guard that user templates have none.
- `internal/authcallout/handler.go` — add `LeaderContactStale func(now time.Time) bool`
  (nil ⇒ never stale) consulted on the already-provisioned read path; add a PIN-write seam
  routing through `Node.Propose` (nil ⇒ today's direct mutator); map `ErrNotLeader` /
  `ErrLeadershipLost` → typed transient deny. Do NOT touch the deny encoding the agent
  reads as terminal beyond classification.
- `internal/broker/authcallout.go` — `Subscribe` → `QueueSubscribe("$SYS.REQ.USER.AUTH",
  "tether-authcallout")` (R4); wire the two seams ONLY when a Node is present (nil in D3
  prod).
- `test/e2e/all_phases_test.go` — `TestD3Matrix` (explicit ≥300 s timeout, NOT inherited
  90 s; mirror the D1/D2 subprocess pattern).
- `docs/distributed-broker-architecture.md` — the 6 amendments above. **NOT edited**:
  `cmd/tether/serve.go`, `scripts/install.sh`, `internal/storage/*`, any `cluster_nodes`
  write, any `apply.*` forwarding.

---

## Adversarial test plan (every §13.8 sub-req → a named test; vacuity bite mandatory)

All concurrency-touching tests: `-race` + the repo's BUILT-IN `runtime.NumGoroutine`
poll-with-tolerance + fd-baseline gates (**verified** `test/concurrency/helpers_test.go`
`assertNoGoroutineLeak`, `internal/cluster/wal_concurrency_test.go` `fdCount`, `>4`
tolerance) — **NOT goleak**.

| §13.8 sub-req (line 318/524) | Named test | Adversarial / vacuity bite |
|---|---|---|
| **(R0 spike → )** Cross-server callout (client→A, B answers) | `TestD3CrossServerCalloutAuthorizes` | Responder on B only; **wait for `n-1` routes before asserting**; vacuity control 1: remove B's pub from A's `auth_users` → CONNECT FAILS; vacuity control 2: B signs with a non-shared account key (Issuer mismatch) → A rejects even with AuthUsers membership (proves signature-trust, not mere reachability). |
| RF1 ACL pos/neg | `TestD3RF1ApplyACL` (table) | broker-nkey pub `tether.v2.cluster.apply.x` allowed (positive control delivered to a peer sub); member/agent JWT pub AND sub `cluster.>` denied via async `ErrPermissionViolation` handler (not absence); each denied conn still succeeds on an allowed subject (subject-scoped, not a dead conn). |
| Member/agent template has no cluster.* | `TestD3BrokerTemplateOnlyClusterHolder` | exact-literal match (not `cluster.` substring); also assert no `$SYS.*` in user templates. |
| Per-server R3F3 completeness | `TestD3NatsClusterRendersAllBrokerPubsEveryServer` | N-node roster: each conf has all N pubs in `auth_users` + static perms w/ cluster.* literals; dropping one pub on one server is detected. |
| No PIN/JWT bytes on non-broker subjects | `TestD3NoSecretLeakOnBusSubjects` | **positive control mandatory**: a broker-only subscriber DOES observe the PIN/JWT bytes in flight (proving they existed) while a member subscriber on its broadest allowed subject sees none — else structurally vacuous. |
| PIN-join election / deposed leader → transient deny, no false-allow, no write | `TestD3PINFollowerTransientDeny` + `TestD3PINDeposedLeader` + `TestD3PINLeaderSucceeds` | follower `Propose`→`ErrNotLeader`→typed transient deny, NO `agent_provisioning` row on EITHER replica; deposed mid-Apply `ErrLeadershipLost`→transient deny, exactly one row after a leader retry; leader→commit→allow. Assert the deny REASON is classified transient (R3). |
| Fail-closed predicate: > T_fence fences, bound = leader-contact-lag, non-vacuous | `TestD3FailClosedBoundary` (injected clock, table) | rows: `T_fence−ε`/`T_fence` allow, `T_fence+ε` fence, zero-value fence, `Leader` fresh regardless, `ex-leader demoted → follower-clock ages → fences` (executed, not prose). **Vacuity bite**: parameterize over a deliberately-wrong predicate (`now−bootTime` ignoring LastContact; and `>=` instead of `>`) and assert the SAME table FAILS against them (green-after-red). Drive non-vacuity on a **pure-follower** partition (not a demotion, to avoid masking the step-down reset). |
| Quorum-loss read not bricked, no leader round-trip | `TestD3QuorumLossReadNoBlock` | kill quorum; surviving node authorizes within T_fence and **returns < ~200 ms** (latency bound proves no `VerifyLeader` round-trip); after T_fence self-fences. **Live-partition row MANDATORY** (proves the handler actually consults the predicate, not just unit math). |
| Pre-vote live over REAL transport | `TestD3PreVoteRealTransport` | partition a follower over `tlsStreamLayer`; assert term does NOT bump + leader holds; **discriminator**: a `PreVoteDisabled=true` variant where term DOES bump (proves the test can observe a bump). |
| Routes mTLS mutual | `TestD3RouteMTLSRejectsBadCert` | foreign-CA leaf → mesh does NOT form (no route-count increment); positive control: correct CA → mesh forms + shared JS domain. **Live, not golden-string.** |
| Transport 2-node replicate | `TestD3TransportTwoNodeReplicate` + leak case | static bootstrap {A,B} over mTLS; Apply on A converges on B's replica; **repeated bootstrap+Shutdown churn** to beat the fd `>4` tolerance; asserts transport listener + (no) goroutine leaks. |
| No regression to production / single-node callout | `TestD3HandlerNilSeamsEqualToday` + `TestD3ServeBuildsNoClusterNode` | nil seams → always-authorize + direct mutator (byte-identical); guard asserts `serve.go` constructs no Node / wires no seam (locks R1 as a standing invariant). Existing `test/p3` / `test/p4` / `test/security` stay green unchanged. |
| e2e matrix | `TestD3Matrix` | explicit ≥300 s timeout; boundary rows use injected clock (no real 10 s sleep); cross-server + RF1 + fence + route-mTLS smoke. |

Pre-gate (R6): re-run D1 kill-9 + D2 DIFF-1/`TestD2Matrix` green under the new raftConfig
BEFORE D3 implementation proceeds. Final gates: `make test` + `make e2e` + `make lint` +
`-race` + built-in leak gates all green; every negative test green-after-red.

---

## Resolved during finalization (were the candidate's open questions)

- **Q2 — agent reconnect-on-auth-deny: RESOLVED → R3 refined.** Verified the agent treats
  any auth-deny as terminal (agent.go:617-633, `isAuthFailure` :1012). So D3 does NOT rely
  on client retry and ships only the callout-boundary security half; transparent recovery is
  D4. (See R3.)
- **Q3 — route mTLS in embedded `natstest`: RESOLVED → use it.** Verified
  `server.Options.Cluster.TLSConfig *tls.Config` exists in nats-server v2.14.0
  (`server/opts.go:74`). The behavioral routed mTLS harness is viable via
  `opts.Cluster.TLSConfig`; `TestD3RouteMTLSRejectsBadCert` still runs live (no golden-only
  mTLS gate).

## Remaining open (elevated to R0, gates the phase)

- **Q1 — cross-server `$SYS.REQ.USER.AUTH` propagation over routes** is the one genuine
  runtime unknown and the central D3 capability. **Mandatory de-risk spike is the first
  Stage-B action (R0)**; its outcome may trigger the §6.1 doc amendment (co-located-responder
  fallback).

---

## Implementation sequencing (Stage B order)

1. **R0 spike** (throwaway): two real routed servers + cross-server callout → confirm or
   trigger the §6.1 fallback. **Gate the rest of D3 on this.**
2. Doc-first amendments (1–6 above) committed to the architecture main text.
3. **R6 pre-gate**: parameterize raftConfig timeouts; re-run D1 kill-9 + D2
   DIFF-1/`TestD2Matrix` green under multinode constants.
4. R5 raft mTLS transport + static bootstrap (`internal/cluster/transport.go`, node.go
   knobs) + `TestD3TransportTwoNodeReplicate` + `TestD3PreVoteRealTransport`.
5. R2 fail-closed predicate (`read.go`) + `TestD3FailClosedBoundary` (with vacuity control).
6. R7 proto SSOT + R4 RF1 in `PermissionsForBroker()` + R4 renderer (`internal/natscluster`)
   + golden tests.
7. R4 QueueSubscribe + R2/R3 handler seams + nil-seam regression guards.
8. The full `test/d3` §13.8 suite + `TestD3Matrix` + final gates.

Each negative/vacuity test must be shown **green-after-red**. Stop at Stage C (internal
adversarial review) → external review (do NOT proceed to D4 until external review PASSES).
