# D6 — distributed data-plane — PLAN-OF-RECORD (finalized)

> Stage A per CLAUDE.md §3: 11× Opus-4.8 adversarial workflow (5 drafters / 5 critics / 1 synth)
> drafted the candidate; the **main process is the sole finalizer**. This file is the定稿
> (authoritative). The raw synthesis candidate is preserved at `d6-plan-synth.md` for the audit trail.
>
> **Scope**: D6 makes expose/tunnel fail over across home brokers, via seven coupled mechanisms,
> **build-and-prove** (production `serve.go` byte-unchanged, cutover = D9), proven by a multi-broker +
> agent-failover test harness that drives `cluster.Node` directly.

---

## 主进程定稿裁定 (finalization rulings — read first)

The synthesis is strong and I adopt its body. I personally re-verified every load-bearing code claim
before finalizing, and I make ONE override + resolve all 5 open questions.

### Re-verified against source (finalizer, not draft paraphrase)
- `internal/cluster/fsm.go:78-80` — `if l.Type != raft.LogCommand { return nil }`. Config/noop entries do
  NOT advance `applied_index`. Confirms the catch-up barrier CANNOT be `applied_index >= raft-commit-index`
  (the two are different domains). **The barrier is epoch-as-local-row-epoch (R-12/DA-7).** ✓
- `internal/cluster/node.go:25-26` — `LocalID raft.ServerID` is an EXPORTED field (== `cluster_nodes.node_id`).
  So "self" needs only a thin `SelfID() string` accessor = `string(n.LocalID)`; no new identity plumbing. ✓
- `internal/broker/expose.go:115` `handleExposeReq` → `port.Allocate` → `ExposeForwardedReq{Name,Port,LocalPort,
  Token,ActorFP}` (5 fields, NO home) → agent. The INITIAL expose path never touches `NodeRegisterResp`.
  **The C1 fix (DA-12: inject home into the expose-forward) is mandatory and confirmed.** ✓
- `internal/port/port.go` — `Allocation` has NO `HomeBroker`/`Epoch`; `LookupByTokenHash` SELECT does not
  read `home_broker`/`epoch`. Both are net-new (add fields + widen SELECT; legacy rows → `''`/`0`). ✓
- `internal/tunnel/tunnel.go` — `Client.brokerAddr` single shared field; `supervise` takes `token` as a
  spawn-time VALUE param; `NewServerWithCert` exists ONLY in comments; `parseRegisterLine` uses `len!=5` +
  `strconv.Atoi`; `tls.go:78` `clientTLSConfig` = `InsecureSkipVerify`. All confirmed. ✓

### OQ resolutions (all 5 resolved; §5 is now closed, not open)

- **OQ-1 (agent→server_name binding) — OVERRIDE the synth.** The synth recommended an in-memory
  `(sid,nid)→server_name` map. **REJECTED: it is not cluster-correct.** Home resolution is leader-authoritative
  (`PlanAllocate` runs on the leader via `Propose`), and the broker that handles an `expose.req` is NOT
  necessarily the broker the agent is connected to (NATS routing/queue-groups) — so neither the leader nor
  the expose-handling broker reliably has the agent's `server_name` in local memory. The binding **must be
  replicated**. RESOLUTION (R-26): **migration 0012 adds one nullable column `nodes.nats_server TEXT` (default
  NULL)**, written ADDITIVELY by BOTH register paths (live `node.Register` direct mutator AND FSM
  `OpNodeRegister`) so the D2 DIFF-1 equivalence stays consistent; the value is `req.ServerID`. It is INERT in
  production (only `homeDirectiveForExpose`, gated `b.node != nil`, ever reads it). This is D6's ONLY migration;
  it adds no index and no production read (the synth's "zero migrations" goal becomes "one additive nullable
  column, no index churn"). The leader's `PlanAllocate` resolves home = `clusternodes.LookupByNatsServer(
  nodes.nats_server) → VOTER row`.
- **OQ-2 (catch-up sufficiency under snapshot restore) — RESOLVED.** D1's online-backup snapshot is a
  consistent page-level copy at a txn boundary, and the `port_allocations` row + `applied_index` are written in
  the SAME FSM txn (fsm.go `applyCommand`), so a restored snapshot's row-epoch and `applied_index` are mutually
  consistent. The DA-7a sufficiency argument holds. **Add a `restore-then-REGISTER` harness case** to prove it.
- **OQ-3 (concrete RTO numbers) — DEFERRED to Stage B (mechanical).** Read the agent `nats.Options`
  (ReconnectWait/PingInterval/MaxReconnects) + `RegisterTimeout`/retry defaults from `cmd/tether/agent.go` at
  implementation time; the harness asserts a threshold derived from them (§4.5 gives the formula). Not a design
  blocker.
- **OQ-4 (RehomeDirective subject) — RESOLVED.** Reuse the existing agent-only `.req.forwarded`-style channel
  (no new subject; same secrecy boundary as `ProxyDirective`). Extend `proxy_no_secrets_test.go` to a
  `TestD6NoTokenOrPinOnSysEvents` rehome-storm assertion.
- **OQ-5 (R-15 rehome first-dial transient policy) — RESOLVED.** `applyReconciliation` classifies the returned
  `*tunnel.DenyError`; a transient `home_catching_up` is rescheduled with the same full-jitter backoff bound
  (NOT a changed `Open` contract); after a max-wait it logs `catch_up_stalled` (a LOG line — the alert ROW is
  D8b) and NEVER collapses to terminal.

Everything else in the synthesis I adopt as-is. The plan is implementation-ready.

---

## 0. SCOPE + BUILD-AND-PROVE BOUNDARY (the spine) + NON-GOALS

### 0.1 Scope
D6 makes expose/tunnel fail over across home brokers via seven coupled mechanisms, proven by a multi-broker +
agent-failover test harness that drives `cluster.Node` directly. D6 ships REAL proto-v2 wire and REAL
agent-binary changes (the v2 fleet reinstalls; no v1 back-compat — separate release line), but **every
cluster-specific behavior is gated on receiving a `HomeDirective`** (in the register reply OR — the C1 fix — in
the expose-forward), which a single-node production broker never emits.

### 0.2 Build-and-prove invariants (cutover = D9; violating any fails review)
- **B1.** `cmd/tether/serve.go` stays BYTE-UNCHANGED: constructs no `cluster.Node`, wires no home assignment,
  gives the tunnel server no stable cert (`tlsCert == nil` → ephemeral self-signed fallback, §16.7).
- **B2.** Every production `internal/broker/*.go` constructs no `cluster.Node`, emits no `HomeDirective`.
  `handleRegister` and `handleExposeReq` responses are BYTE-IDENTICAL to today in N=1 (new directive fields are
  pointer + `omitempty`, left nil by the production path; mirrors `Proxy *ProxyDirective`).
- **B3.** The production `port.Allocate` direct mutator stays BYTE-UNCHANGED (leaves `home_broker=''`,
  `epoch=0`). The FSM `port.PlanAllocate` learns to bake `home_broker`+`epoch` ONLY when called with a non-empty
  home; current callers pass `("", 0)` → byte-identical baked INSERT.
- **B4.** `internal/cluster` imports NEITHER `nats.go` NOR `internal/broker` (L-2). All home-assignment logic
  that needs NATS lives in `internal/broker`; the `cluster_nodes` read-by-server-name helper lives in a NEW
  `internal/clusternodes` package (pure SQL, no nats, no raft).
- **B5.** The agent binary changes, but every cluster path is inert without a `HomeDirective`: REGISTER 6th
  field always `0`; directives nil; `cert_pins` absent → `InsecureSkipVerify` N=1 fallback; per-expose
  `brokerAddr` collapses to the single `--tunnel-addr`; `home_broker==''` makes `tunnelTokenLookup`'s
  home/epoch branch inert.
- **B6 (finalizer).** The additive `nodes.nats_server` write (R-26) is benign and inert: production writes it
  (from `req.ServerID`) but NOTHING reads it unless `b.node != nil`. It does not change any RESPONSE bytes.
- **Guard:** `test/d6/regression_test.go::TestD6ProductionWiresNoClusterNode` extends the D5 token-scan over
  `serve.go` + `internal/broker/*.go` (excl. the build-and-prove file `internal/broker/home.go` + `_test.go`)
  **and `internal/agent/*.go`**.

### 0.3 NON-GOALS (explicitly D7 / D9 / later)
- `cluster_nodes` production writer (`ClusterNodeUpsert` via join-PoP) — **D7**. D6 seeds rows in the harness.
- Operator `cluster rotate-tunnel-cert` CLI — **D7**. D6 proves only the rotation WINDOW MECHANISM by
  harness-writing the cert columns. NO live `tunnel.Server` cert hot-swap primitive (rotation is proven by
  harness RESTART with a new cert, not a live swap).
- `cluster add` / `raft.AddVoter` / dynamic membership — **D7**.
- `drain` (migrate expose) — **D7**; D6 rehome is its prerequisite (先父后子) — do NOT pull forward.
- Production cutover (serve.go constructs `cluster.Node`, wires home assignment, stable cert, backfills
  `home_broker=self` for live rows via `cluster init --from-existing`) — **D9**.
- In-flight transfer/exec/run continuity across rehome — NOT preserved. Rehome tears the tunnel transport
  (Open-replace closes the old yamux); any in-flight TCP stream through an exposed port is severed and must be
  re-established by the end client. Control-plane PTY/exec/run (not riding the tunnel) is unaffected.
- `replication_degraded` / `broker_down` / `catch_up_stalled` ALERT TABLE ROWS — **D8b**. D6 emits a
  `catch_up_stalled` LOG line only.
- **No STEADY-STATE active-active for one expose** (corrected per review L-1; the original "FORBIDDEN
  invariant / exactly one bind" overclaimed). The per-port UNIQUE-active index guarantees one home owns the
  ALLOCATED row, but A and B bind real OS listeners on DIFFERENT hosts, which nothing serializes. A bounded
  CUTOVER WINDOW may transiently double-listen: if the agent↔ex-home(A) yamux drops for an unrelated blip
  BEFORE the rehome directive lands, the old supervisor `redialWithBackoff`s to A at the OLD epoch; if A has
  not yet applied the reassign (row still {home=A, epoch=N-1}) A ALLOWs and re-binds the public port, so until
  B catches up + the rehome's `OpenHome(B)` cancels the port-P supervisor (closing A's session), both hosts
  briefly forward the port. In-flight is severed either way (rehome is a hard cutover). The window is bounded
  by directive-delivery + ex-home apply latency and SELF-HEALS; the CAS fence (R-7) fences the DB write, not
  A's live listener. The AUTHORITATIVE ex-home listener kill (leader pushes a CloseProxy on reassign) is the
  D7 leader-push (needs broker-death detection). D6 documents + tests the bounded self-healing window.
- Agent-side home roster validation — the agent trusts the leader-signed directive's `BrokerAddr`; **the
  cert-pin is the agent's sole authentication that the rehome addr is a legitimate cluster broker** (makes the
  fp SSOT doubly load-bearing).
- v1 back-compat — non-goal (proto v2 reinstall).

---

## 0bis. DOC-FIRST AMENDMENTS (architecture, BEFORE any code)
All amend `docs/distributed-broker-architecture.md`; §18 is audit trail, the body (§0–§17) is the ruler.

- **DA-1 — §6.5 server_name correction (THE load-bearing doc fix).** Replace "(N… nuid)" with: *the agent
  self-reports `nc.ConnectedServerName()` (== `info.Name` == the deterministic `server_name` rendered by
  `internal/natscluster/config.go`, e.g. `"tether-1"`), matched against `cluster_nodes.nats_server_id`. The
  volatile per-boot NUID (`nc.ConnectedServerId()` == `info.ID`) is explicitly NOT used — it rotates on every
  nats-server reboot and would break the mapping on the exact home-failover event D6 handles.* The
  `NodeRegisterReq.ServerID` field carries the server_name (name kept for §6.5 continuity).
- **DA-2 — §6.5/§18.3 home eligibility.** A `cluster_nodes` row is home-eligible iff `phase == 'VOTER'`. Other
  phases yield no directive; §7.4 reconvergence picks it up next reconnect. Initial home = the broker the agent
  is currently connected to (a first-guess miss converges via §7.4).
- **DA-3 — §7.1 epoch SSOT.** `port_allocations.epoch` is a per-port MONOTONE counter: `0` at allocate
  (migration-0010 baseline), `+1` per `OpPortReassignHome`. NOT a raft index, NOT 1-based.
- **DA-4 — §7.1-7.2 the home/epoch lookup ladder.** Specify `tunnelTokenLookup`'s new `epoch` param, the
  `home_broker==self` filter, the TWO-DIMENSIONAL decision (R-9), and the inert `home_broker==''` branch.
- **DA-5 — §7.2(a) `home_catching_up` TRANSIENT reason.** Register the wire constant; pin that BOTH the broker
  emit-side and the agent `denyIsTransient` classifier reference a single shared `const` (no duplicated literal).
- **DA-6 — §7.2(b) 6-field REGISTER grammar.** `REGISTER <sid> <nid> <port> <token> <epoch>`; parser accepts
  EXACTLY 6 fields; epoch via `strconv.ParseInt(_,10,64)`; negative/overflow/non-int → `malformed_register`.
  REGISTER carries NO barrier (the barrier is derived home-locally, R-11/R-12).
- **DA-7 — §7.2(c) catch-up barrier predicate, CORRECTED.** The catch-up condition is EPOCH-AS-LOCAL-BARRIER:
  the new home compares the agent-presented epoch against the epoch of its OWN locally-applied
  `port_allocations` row for that port. The leader does NOT thread a raft index over the wire; the home does NOT
  call `VerifyLeader` on the read path. **DA-7a sufficiency:** because `OpPortReassignHome(epoch=N)` is applied
  in the same FSM txn that advances `applied_index`, a replica whose local row shows `epoch>=N` has, by
  construction, applied that entry — so `localRowEpoch >= presentedEpoch` is exactly "this replica has applied
  the directive's reassign". The leader's `VerifyLeaderRead` is used ONLY to stamp a fresh `epoch` into the
  directive at issue time; the home's local row-epoch comparison is the "compares its own local applied_index"
  half. (Verified: fsm.go:78-80 makes any `applied_index >= raft-commit-barrier` predicate WRONG — different
  domains.)
- **DA-8 — §7.4 self-driven rehome.** `onNATSReconnect` → re-register → fresh directives, AND the new
  expose-forward path (DA-12). Epoch-ordered `Open(newAddr)` atomic replace; old supervisor canceled (not stuck
  redialing the dead addr); leader-pushed `RehomeDirective` as BACKUP over the agent-only forwarded channel;
  K/sec leader push rate-limit + agent backoff bound; rehome `Open`s run CONCURRENTLY (R-14); transient denies
  on a rehome's first dial are retried (R-15).
- **DA-9 — §7.5 per-expose brokerAddr.** addr/epoch/certPins move to `clientSession` (keyed by publicPort); one
  Client fans out to N homes; `Client.brokerAddr` is RETAINED as the N=1 fallback when a session carries no addr.
- **DA-10 — §7.7/§15 cert pinning + rotation window.** `cert_fp` format SSOT = `"sha256:" + hex(SHA-256(
  cert.Raw))` (DER of leaf, never SPKI). One `tunnel.CertFingerprint(*x509.Certificate)` used by BOTH harness
  seeder and agent verifier. Agent verify uses `VerifyConnection` (resumption-safe), NOT `VerifyPeerCertificate`.
  `cert_pins{current, previous, valid_until}` dual-pin window; `previous` accepted iff `previous!="" &&
  valid_until>0 && now<valid_until`. N=1: empty pins → `InsecureSkipVerify` fallback (the ONLY fallback). NO
  first-dial-without-pins path for a clustered home: a clustered expose defers its dial until pins arrive.
- **DA-11 — §16.7 deviation registry.** Record the D6 build-and-prove deviation (production tunnel server stays
  ephemeral; stable-cert + pins reached only behind a `HomeDirective` the harness emits; cutover = D9).
- **DA-12 — §7.2/§6.5 INITIAL-home delivery (the C1 fix).** `ExposeForwardedReq` gains the home directive
  fields; `handleExposeReq` resolves home from the agent's persisted server_name binding (DA-13);
  `handleExposeForwarded` persists them into `PortToken` and opens against the home. Inert in N=1 (empty home →
  byte-identical `ExposeForwardedReq`).
- **DA-13 — §6.5 agent→server_name binding storage (finalized per OQ-1, R-26).** The broker persists the
  agent's last-reported `ServerID` (server_name) in a REPLICATED nullable column `nodes.nats_server` (migration
  0012), written additively by both register paths, so it is queryable at expose time by ANY broker and the
  leader. Inert in production (only read behind `b.node != nil`).
- **DA-14 — §18.2 audit trail + §18.2.18 RTO budget.** Append D6 entries: `OpPortReassignHome` promoted; the
  `HomeDirective`/`RehomeDirective` shapes; the 6-field REGISTER; the server_name-not-NUID ruling; the
  epoch-as-local-barrier ruling; the summed RTO budget (§4.5); the `nodes.nats_server` replicated-binding ruling.

---

## 1. NUMBERED RULINGS

### Mechanism 1 — server-id bridge + home assignment
- **R-1.** Bridge key = `nc.ConnectedServerName()` (deterministic server_name), NOT the NUID.
  `NodeRegisterReq` gains `ServerID string` (`json:"server_id,omitempty"`).
- **R-2.** Home assignment is leader-authoritative, broker-side (needs nats), eligibility = `phase=='VOTER'`.
  The agent never self-selects a home.
- **R-3.** New `cluster_nodes` read-by-`nats_server_id` helper lives in a NEW `internal/clusternodes` package
  (pure SQL; no nats, no raft). D7's `ClusterNodeUpsert` writer co-locates there later.
- **R-4.** The broker persists the agent's reported server_name at register time (R-26/DA-13) so
  `handleExposeReq` can resolve home at expose time and re-resolve on rehome. Inert in production.
- **R-26 (finalizer — OQ-1 resolution).** The server_name binding is REPLICATED: migration 0012 adds nullable
  `nodes.nats_server TEXT DEFAULT NULL`; both the live `node.Register` direct mutator and the FSM
  `OpNodeRegister` bake it from `req.ServerID` (keeps D2 DIFF-1 consistent). `clusternodes.LookupByNatsServer`
  resolves it. NO in-memory map (not cluster-correct). This is D6's only migration (no index, no production read).

### Mechanism 2 — per-expose home_broker/epoch + OpPortReassignHome
- **R-5.** `OpPortReassignHome` (command.go named-deferred) is promoted to a live op: one `defaultAppliers()`
  entry → `genericExecApplier{}` (stateless baked-SQL exec).
- **R-6.** `PlanAllocate` gains a home param and bakes `home_broker = LitText(homeNodeID)`, `epoch = LitInt(0)`
  ONLY when home is non-empty; all current callers pass `("", 0)` → byte-identical INSERT. The live
  `port.Allocate` direct mutator is byte-unchanged. (REJECT "born home-correct on the live allocate path" — the
  single worst boundary violation.)
- **R-7.** `PlanReassignHome(db, publicPort, newHome, now) (newEpoch int64, *cluster.Command, error)` reads the
  current epoch under `applyMu` (held by `Propose`), bakes an ALL-LITERAL `UPDATE ... SET home_broker=<lit>,
  epoch=<LitInt(curEpoch+1)> WHERE port=<lit> AND state='ALLOCATED' AND epoch < <LitInt(curEpoch+1)>`.
  *Rationale: leader-baked literal (NOT `epoch=epoch+1` column arithmetic) + monotonic CAS guard `epoch <
  newEpoch` makes a stale ex-leader's lower-epoch reassign a deterministic `RowsAffected==0` no-op on every
  replica — the ex-home double-bind FSM-layer fence. `WHERE epoch < newEpoch` (not `= curEpoch`) tolerates a
  missed intermediate.*
- **R-8.** The leader-driven `OpPortReassignHome` (broker-death backup path) carries NO reqID and relies solely
  on the R-7 CAS guard for idempotency. *Rationale: the D4 ledger requires the reqID be originating-broker-minted,
  never leader-minted; the leader-push path has no non-leader originator — like D4's provision/join "no key" ops,
  the CAS guard is the idempotency anchor.*

### Mechanism 3 — tunnelTokenLookup home/epoch/catch-up
- **R-9.** `tunnelTokenLookup` gains `epoch int64`; after the existing token/sid/nid/`__proxy__` checks, **if
  `a.HomeBroker == "" → skip the entire ladder` (inert, byte-equivalent to today)**. Else the decision is a
  function of BOTH `(home_broker vs self)` AND `(presentedEpoch vs a.Epoch)`:
  - `presentedEpoch < a.Epoch` → **terminal** `token_unknown_or_revoked` (agent holds a superseded directive;
    the higher-epoch directive rehomes it).
  - `presentedEpoch > a.Epoch` → **transient** `home_catching_up` (this replica has not yet applied the latest
    reassign — REGARDLESS of home-vs-self).
  - `presentedEpoch == a.Epoch && a.HomeBroker == self` → **allow**.
  - `presentedEpoch == a.Epoch && a.HomeBroker != self` → **terminal** `token_unknown_or_revoked` (genuine
    ex-home / never-home replica at the same epoch).
  *Rationale: REJECT unconditional `home != self → terminal` — it bricks the new home during catch-up (the new
  home holds the OLD row `{home=A,epoch=N-1}` with `self=B`, sees `home!=self` and terminally denies the very
  home the agent was directed to). The only terminal arms are `presented < row` (superseded) and `presented ==
  row && home != self` (genuine ex-home). `presented > row` is ALWAYS transient (a higher presented epoch can
  only come from a leader-committed directive this replica will eventually apply).*
- **R-10.** "self" = `b.selfNodeID()` = `string(b.node.LocalID)` (via a thin `SelfID()` accessor) when
  `b.node != nil`, else `""` (explicit nil-guard). In production `b.node == nil` → `self==""`, and the
  `home_broker==''` branch (R-9) short-circuits before `self` is consulted. The `broker.go NewServer(...,
  b.tunnelTokenLookup, ...)` call site stays TEXTUALLY IDENTICAL (the signature change rides the `TokenLookup`
  type).

### Mechanism 4 — REGISTER 6th field
- **R-11.** `dialAndRegister` writes `fmt.Sprintf("REGISTER %s %s %d %s %d\n", sid, nid, publicPort, token,
  epoch)`. `parseRegisterLine` changes `len(parts) != 5` → `!= 6`, parses epoch via
  `strconv.ParseInt(parts[5],10,64)` (NOT `Atoi`), rejects parse error / negative / overflow →
  `malformed_register`. `TokenLookup` type + `handleAgent` gain the epoch param. The barrier is NOT a wire
  field. N=1: epoch always `0`.

### Mechanism 5 — catch-up barrier
- **R-12.** Barrier predicate = epoch-as-local-barrier (DA-7): the home compares `presentedEpoch` vs its own
  locally-applied `a.Epoch`; `presented > local` → `home_catching_up`. No raft index threaded; no `VerifyLeader`
  on the read path. (Verified against fsm.go:78-80; the local row-epoch IS the only state the bind decision
  needs and advances in the same FSM txn as `applied_index`.)

### Mechanism 6 — agent per-expose brokerAddr + rehome + denyIsTransient
- **R-13.** Move `brokerAddr`/`epoch`/`certPins` onto `clientSession`; RETAIN `Client.brokerAddr` as the N=1
  fallback (used when a session's addr is empty). `Open` signature → `Open(publicPort, localPort int, token,
  brokerAddr string, epoch int64, certPins CertPins) error`. **The supervisor MUST receive these as value
  parameters snapshotted at the `go c.supervise(...)` spawn — NEVER read them back from `c.sessions[port]` inside
  the loop.** *Rationale: `token`'s race-freedom comes from being a spawn-time value param read by exactly one
  goroutine; reading the new fields from the shared map inside the loop is an unsynchronized read against
  `Open`-replace's map write → a hard `go test -race` failure. A `-race` unit test asserting this is mandatory.*
- **R-14.** Rehome runs each directive's `Open` CONCURRENTLY (bounded worker pool, one goroutine per expose), and
  the `Open`-replace must `old.cancel()` the old supervisor UP-FRONT so it stops redialing the dead addr
  immediately. *Rationale: `Open` blocks on the first dial; serial rehome of N exposes = N × dial-timeout, blowing
  the RTO budget. The rehome `Open` logs-not-rolls-back on first-dial error (no broker reply to send frpc_failed
  to).*
- **R-15.** A `home_catching_up` (or any transient) deny returned by a rehome's first `Open` dial must be retried
  by `applyReconciliation` (bounded reschedule, same full-jitter backoff), NOT dropped. *Rationale: the first
  `Open` dial returns the error before any supervisor exists, so the supervisor's transient-retry loop never sees
  it.*
- **R-16.** `home_catching_up` added to `denyIsTransient` via a single shared `const proto.ReasonHomeCatchingUp =
  "home_catching_up"` referenced by BOTH the broker emit-side and the tunnel classifier. (Lives in
  `internal/proto`, the SSOT both `internal/broker` and `internal/tunnel` import — a duplicated literal is the
  brick-the-fleet drift risk.)
- **R-17.** Rehome rides BOTH `onNATSReconnect` (re-register reply carries fresh directives) AND the
  expose-forward path (DA-12), with the leader-pushed `RehomeDirective` (agent-only forwarded channel, never
  sys.events) as BACKUP. All paths epoch-ordered (apply iff `directive.Epoch > clientSession.epoch`) →
  idempotent. K/sec leader push rate-limit + agent full-jitter backoff. *Rationale: the `home_catching_up`
  transient IS the natural backpressure (the lagging new home spreads the herd over the backoff loop).*
- **R-18.** `state.json` `PortToken` gains `HomeBrokerAddr string`, `Epoch int64` (both `omitempty`); `CertPins`
  are NOT persisted (re-delivered on every register/expose, like ProxyState PSKs). `replayPortsFromState`
  re-targets the right home on boot; empty addr → the `--tunnel-addr` fallback. A pre-D6 state.json loads
  (omitempty → empty/0).

### Mechanism 7 — cert pinning + stable cert + rotation window
- **R-19.** Add `tunnel.NewServerWithCert(addr, publicHost string, lookup TokenLookup, cert *tls.Certificate,
  logger) *Server` (net-new) — HARNESS/TEST only; production `broker.go` keeps calling `NewServer` (cert nil →
  ephemeral). Add `tunnel.LoadServerCert(certPEM, keyPEM) (*tls.Certificate, error)`. The guard bans
  `NewServerWithCert(`/`LoadServerCert(` in production files.
- **R-20.** Single fp SSOT: `tunnel.CertFingerprint(cert *x509.Certificate) string` = `"sha256:" +
  hex(SHA-256(cert.Raw))` (DER of leaf, never SPKI). Used by BOTH the harness `cluster_nodes.cert_fp` seeder and
  the agent verifier.
- **R-21.** Agent pin verification: `dialAndRegister` builds the per-session `tls.Config` from `sess.certPins`.
  **Empty pins → `InsecureSkipVerify: true`, no callback** (the ONLY N=1 fallback, byte-identical to today).
  **Non-empty pins → `InsecureSkipVerify: true` + a `VerifyConnection` callback** that: treats empty
  `cs.PeerCertificates` as REJECT; computes `CertFingerprint(cs.PeerCertificates[0])`; accepts iff `fp ==
  pins.Current` OR (`pins.Previous != "" && pins.ValidUntil != nil && now < *pins.ValidUntil && fp ==
  pins.Previous`); returns a non-nil error otherwise. *Rationale: `VerifyConnection` runs on EVERY handshake
  including TLS 1.3 session resumption (`VerifyPeerCertificate` does NOT); fail-closed on empty/nil-valid_until;
  the handshake fails BEFORE the REGISTER token is written, so a MITM never receives the bearer token.*
- **R-22.** NO InsecureSkipVerify path when a clustered home is targeted. A `PortToken`/directive with a
  non-empty `HomeBrokerAddr` but no pins yet DEFERS the dial until the register/expose reply delivers pins.
- **R-23.** `CertPins` wire shape: `{Current string; Previous string omitempty; ValidUntil *time.Time
  omitempty}` (pointer-time distinguishes NULL/no-rotation from a zero time). The leader maps
  `cluster_nodes.cert_fp/_prev/_valid_until` → `CertPins`; NULL prev/valid_until → `{Current, "", nil}`.
- **R-24.** Cert rotation forces the leader to RE-PUSH fresh directives (same addr, same home-epoch, new pins)
  to affected agents — a rotation is not a reconnect. A pure-pin update (same addr, same home-epoch) updates
  `sess.certPins` IN PLACE without tearing the transport (the rotation window keeps the old pin valid until
  `valid_until`). D6 proves the MECHANISM via harness; the operator command is D7.
- **R-25 (build-and-prove guard).** `TestD6ProductionWiresNoClusterNode` extends `d5BannedTokens` with
  `{"NewServerWithCert(", "LoadServerCert(", "HomeDirective{", "RehomeDirective{", "homeDirectivesForRegister",
  "homeDirectiveForExpose", "PlanReassignHome(", "OpPortReassignHome", "LookupByNatsServer(", ".SelfID("}` and
  scans `cmd/tether/serve.go` + `internal/broker/*.go` (EXCLUDING `internal/broker/home.go` + `_test.go`) AND
  `internal/agent/*.go`. Self-check proves the guard discriminates (constructor + struct-literal both caught;
  clean source not). The `RehomeDirective` subscribe-site + any `reassign_home` verb constant are gated behind
  `b.node != nil`.

---

## 2. EXACT SURFACE (concrete Go signatures)

### proto (`internal/proto/messages.go`, `version.go`)
```go
// NodeRegisterReq += (carries server_name, NOT the NUID; DA-1)
ServerID string `json:"server_id,omitempty"`

// NodeRegisterResp += (pointer + omitempty → nil in N=1 = byte-identical resp; the Proxy precedent).
Home *HomeAssignment `json:"home,omitempty"`

type HomeAssignment struct {
    Directives []HomeDirective `json:"directives,omitempty"`
}

// HomeDirective — authoritative home for ONE expose (per publicPort, §7.5). Epoch-ordered (agent applies
// iff Epoch > clientSession.epoch). Carries NO raw token (token unchanged across rehome). Travels ONLY over
// register-reply _INBOX / expose.req.forwarded / agent-only forwarded channel — NEVER sys.events.
type HomeDirective struct {
    Name       string   `json:"name"`
    PublicPort int      `json:"public_port"`
    NodeID     string   `json:"node_id"`     // home raft ServerID (display/audit)
    BrokerAddr string   `json:"broker_addr"` // home tunnel_addr the agent dials
    Epoch      int64    `json:"epoch"`
    CertPins   CertPins `json:"cert_pins,omitempty"`
}

type CertPins struct {
    Current    string     `json:"current,omitempty"`
    Previous   string     `json:"previous,omitempty"`    // non-empty only mid-rotation
    ValidUntil *time.Time `json:"valid_until,omitempty"` // nil outside a rotation window
}

// RehomeDirective — leader-pushed BACKUP (§7.4), agent-only forwarded channel.
type RehomeDirective struct { HomeDirective }

// ExposeForwardedReq += (the C1 INITIAL-home fix; omitempty → byte-identical N=1).
Home *HomeDirective `json:"home,omitempty"`

const ReasonHomeCatchingUp = "home_catching_up" // SSOT shared by broker emit + tunnel classify
```

### cluster + clusternodes + port
```go
// internal/cluster/command.go — promote OpPortReassignHome (one defaultAppliers entry).
// internal/cluster/node.go — add SelfID() string { return string(n.LocalID) } (LocalID already exported).

// internal/clusternodes/read.go  (NEW pkg; pure SQL, no nats, no raft — L-2 clean)
type HomeNode struct {
    NodeID, NatsServer, TunnelAddr, PublicHost, CertFP, CertFPPrev string
    CertValid *time.Time // nil outside a rotation window
    Phase     string
}
func LookupByNatsServer(db *sql.DB, server string) (*HomeNode, error) // ErrNotFound if no match

// internal/port/plan.go
func PlanAllocate(db *sql.DB, sid, nid, name string, localPort, desiredPort int,
    createdByFP, homeBroker string, cfg AllocCfg) (*Allocation, *cluster.Command, error) // home "" => default bake
func PlanReassignHome(db *sql.DB, publicPort int, newHome string, now time.Time) (newEpoch int64, _ *cluster.Command, _ error)

// internal/port/port.go — Allocation += HomeBroker string, Epoch int64;
// LookupByTokenHash SELECT widened to include home_broker, epoch (legacy rows => '' / 0).
// This SELECT-widening is a live production read-path change, ALLOWED but pinned by a differential test
// asserting legacy rows return ''/0 and tunnelTokenLookup stays byte-equivalent then.

// internal/node — RegisterInput += NatsServer string; node.Register + FSM OpNodeRegister both bake
// nodes.nats_server (R-26; migration 0012). Inert in production (only homeDirectiveForExpose reads it).
```

### tunnel (`internal/tunnel/tunnel.go`, `tls.go`)
```go
type TokenLookup func(sid, nid string, port int, tokenHash string, epoch int64) error // epoch LAST

// parseRegisterLine: len(parts) != 6; ParseInt(parts[5],10,64); reject neg/overflow.
// denyIsTransient: add case proto.ReasonHomeCatchingUp.
// clientSession += brokerAddr string, epoch int64, certPins CertPins. Client.brokerAddr RETAINED (fallback).
func (c *Client) Open(publicPort, localPort int, token, brokerAddr string, epoch int64, certPins CertPins) error
func (c *Client) ApplyHome(publicPort int, brokerAddr string, epoch int64, certPins CertPins) error // epoch-ordered rehome replace
func (c *Client) dialAndRegister(ctx context.Context, publicPort int, token, brokerAddr string, epoch int64, certPins CertPins) (net.Conn, *yamux.Session, error)
// supervise/redialWithBackoff/swapTransport: brokerAddr/epoch/certPins as VALUE PARAMS (R-13), never read from the map in-loop.
func NewServerWithCert(addr, publicHost string, lookup TokenLookup, cert *tls.Certificate, logger *slog.Logger) *Server // harness-only
func LoadServerCert(certPEM, keyPEM string) (*tls.Certificate, error)
func CertFingerprint(cert *x509.Certificate) string // "sha256:"+hex(sha256(cert.Raw))
func clientTLSConfigPinned(pins CertPins) *tls.Config // VerifyConnection; empty pins => InsecureSkipVerify
```

### broker (`internal/broker/`)
```go
// expose.go: tunnelTokenLookup(sid,nid string, publicPort int, tokenHash string, epoch int64) error
//   — home/epoch ladder (R-9), inert when a.HomeBroker == "".
//   The broker.go NewServer(..., b.tunnelTokenLookup, ...) call site stays TEXTUALLY IDENTICAL.
// broker.go selfNodeID() string { if b.node != nil { return b.node.SelfID() }; return "" }
// NEW internal/broker/home.go (the build-and-prove file, EXCLUDED from the guard scan):
//   homeDirectivesForRegister / homeDirectiveForExpose / RehomeDirective push + K/sec rate-limit.
//   handleRegister injects resp.Home only when b.node != nil (production: nil → byte-identical).
//   handleExposeReq injects fwdReq.Home only when b.node != nil.
```

### agent (`internal/agent/`)
```go
// agent.go register: req.ServerID = nc.ConnectedServerName()
// agent.go applyReconciliation / proxy.go onNATSReconnect: apply resp.Home directives (R-15/R-17 retry+concurrent)
// expose.go handleExposeForwarded: persist req.Home into PortToken, Open against it
// state.go PortToken += HomeBrokerAddr string `json:",omitempty"`, Epoch int64 `json:",omitempty"`
// tunnel_adapter.go: NewClient keeps brokerAddr (fallback); AddProxy passes per-expose addr/epoch/pins into Open
```

### build-and-prove seam
`b.node *cluster.Node` is the single seam. Production never sets it (`nil`). `selfNodeID()`,
`homeDirectivesForRegister`, `homeDirectiveForExpose`, the `RehomeDirective` push/subscribe, and the
`PlanAllocate(home!="")`/`PlanReassignHome` writes are all reachable ONLY when `b.node != nil`. The harness
constructs `b.node`; the guard (R-25) locks production out.

---

## 3. FULL EPOCH LIFECYCLE (end-to-end, every link)
```
ALLOCATE (initial expose, clustered harness):
  leader resolves home (nodes.nats_server → clusternodes row, phase==VOTER)        [R-26]
  → PlanAllocate(home=B) bakes INSERT ... home_broker='B', epoch=0                  [DA-3 baseline 0]
  → handleExposeReq injects fwdReq.Home = HomeDirective{Name,PublicPort,BrokerAddr=B.tunnel_addr,Epoch=0,CertPins=B.fp}
  → agent handleExposeForwarded persists PortToken{HomeBrokerAddr=B,Epoch=0} + Open(port,local,token,B,0,pins)
  → dialAndRegister to B: "REGISTER sid nid port token 0\n"                          [R-11 6th field = 0]
  → B.tunnelTokenLookup(...,epoch=0): a.HomeBroker='B'==self, presented(0)==a.Epoch(0) → ALLOW   [R-9]

N=1 (production, same code, inert):
  port.Allocate (direct mutator) leaves home_broker='', epoch=0
  → fwdReq.Home = nil (b.node==nil) → agent Open(port,local,token,"",0,{})           [empty addr → --tunnel-addr]
  → "REGISTER sid nid port token 0\n" → tunnelTokenLookup: a.HomeBroker=='' → SKIP ladder → ALLOW (byte-equiv)

REHOME (broker B dies; leader reassigns B→C):
  leader: PlanReassignHome bakes UPDATE ... home_broker='C', epoch=LitInt(1) WHERE port AND state='ALLOCATED' AND epoch<1
  → row on every replica that applies: {home='C', epoch=1}                           [R-7 +1, CAS guard]
  agent (B's nats died → conn bounced to C) → onNATSReconnect → re-register
        → resp.Home = HomeDirective{...,BrokerAddr=C,Epoch=1,CertPins=C.fp}
        → ApplyHome: directive.Epoch(1) > clientSession.epoch(0) → Open(port,local,token,C,1,C.pins)  [R-13/R-14]
        → "REGISTER sid nid port token 1\n"
  ex-home race (stale old supervisor hits B before B sheds, presents epoch 0):
        B (applied {home='C',epoch=1}): presented(0) < a.Epoch(1) → TERMINAL token_unknown_or_revoked  [R-9 B sheds]
  new-home catch-up (C not yet applied, still {home='B',epoch=0}):
        C: presented(1) > a.Epoch(0) → TRANSIENT home_catching_up                    [R-9/R-12 NOT a brick]
        → rehome Open first-dial gets home_catching_up → applyReconciliation RETRIES [R-15]
        → C applies → {home='C',epoch=1} → next REGISTER: presented(1)==a.Epoch(1) && home=='C'==self → ALLOW
  At the LADDER LAYER, ≤1 home ALLOWs a (re)REGISTER at each committed index (B terminal, C transient-then-allow;
  during catch-up ZERO new REGISTERs allowed). NOTE (review L-1): this is a property of the ladder gating NEW
  REGISTERs, NOT of the already-bound OS listeners — see §0.3's bounded cutover window (a stale ex-home redial
  at the old epoch can transiently re-bind until OpenHome(newHome) cancels the port supervisor).
```
Every link: `port_allocations.epoch` (leader-baked literal) ↔ `HomeDirective.Epoch` (read from the row) ↔
`clientSession.epoch` (set by ApplyHome/Open) ↔ REGISTER 6th field (`strconv` of `sess.epoch`) ↔
`tunnelTokenLookup` compare (`presented` vs local `a.Epoch`). The catch-up barrier is the local-row-epoch
comparison — NO raft index anywhere (DA-7).

---

## 4. TEST PLAN

### 4.1 Cheap unit + guard (in `make test`)
- **TestD6ProductionWiresNoClusterNode** (R-25): token-scan over serve.go + broker/*.go (excl. home.go) +
  agent/*.go; self-check discriminates constructor + struct-literal but not clean source.
- **TestD6ClusterNoNATSImport / TestD6ClusterNodesNoNATSNoClusterImport**: `go list -deps` — `internal/cluster`
  bans nats + broker; `internal/clusternodes` bans nats + cluster (L-2).
- **TestD6RegisterLineRoundTrip**: exactly-6 accept; 5/7 reject; non-int / negative / overflow epoch reject;
  trailing/embedded spaces; CRLF. Asserts agent emits `... 0\n` in N=1 (golden bytes).
- **TestD6DenyTransientClassification**: `denyIsTransient(proto.ReasonHomeCatchingUp)==true`; existing reasons
  unchanged; unknown still terminal; const-equality test pinning emit string == classify symbol.
- **TestD6HomeDirectiveByteIdentityN1**: `NodeRegisterResp{Home:nil}` marshals byte-identical to a pre-D6
  golden; `ExposeForwardedReq{Home:nil}` likewise; `CertPins` nil-time omits the key.
- **TestD6PlanAllocateInertHome**: `PlanAllocate(...,"",...)` bakes byte-identical INSERT to today (UTC +
  non-UTC); `PlanAllocate(...,"node-2",...)` bakes `home_broker='node-2', epoch=0` (LitText/LitInt, no Args).
- **TestD6ReassignHomeMonotonic**: `PlanReassignHome` bakes `epoch=LitInt(cur+1)` + `WHERE epoch < cur+1`;
  apply twice → row epoch unchanged (CAS no-op); `ErrNotFound` on absent/non-ALLOCATED.
- **TestD6TunnelTokenLookupLadder** (R-9): `home==''`→inert/byte-equivalent; `home==self && presented==row`→
  allow; `home!=self && presented==row`→terminal; `presented<row`→terminal; `presented>row` (BOTH home==self
  AND home!=self)→`home_catching_up`. Anti-enum: home-mismatch and absent yield identical code/bytes.
- **TestD6CertPinVerify** (R-21, adversarial): in-set accept; `previous` within window accept; `previous` after
  `valid_until` reject; `previous` with `valid_until==nil` reject (fail-closed); fp prefix-of-current reject
  (exact match only); empty `cs.PeerCertificates` reject; empty pins → InsecureSkipVerify no-callback path
  byte-identical to today's `clientTLSConfig()`.
- **TestD6CertFingerprintSSOT**: fixed DER → exact `sha256:...`; two certs same key diff serial → different fp;
  truncated/empty DER → stable error.
- **TestD6LookupByNatsServer**: match → row; no match → ErrNotFound; NULL prev/valid_until → `{...,"",nil}`;
  phase surfaced raw (eligibility decided by caller).
- **TestD6NodesNatsServerInert** (R-26): migration 0012 applies idempotently; live `node.Register` + FSM
  `OpNodeRegister` write the same `nats_server` value (DIFF-1 consistent); a NULL `nats_server` row reads back
  as `""`; nothing reads it without `b.node`.
- **TestD6TokenLookupArgOrder**: a port/epoch transposition fails (distinct values catch the swap).

### 4.2 Gated harness (`//go:build d6_integration`, `TestD6Matrix -race`, dedicated subprocess like TestD5Matrix)
Multi-broker (≥2-3 routed NATS + mTLS raft) each running a REAL `tunnel.Server` via `NewServerWithCert` (stable
cert) + a real agent `tunnel.Client`. Seed `cluster_nodes` rows directly (node_id, nats_server_id=server_name,
tunnel_addr, public_host, cert_fp/_prev/_valid_until, phase=VOTER) + seed `nodes.nats_server` — D5 precedent.
Plus a control `NewServer` (nil cert) instance asserting it stays ephemeral. Test seams: a `dialHook
func(addr string)` on the Client (dead-addr probe), and an FSM `pauseApplyAt(idx)` / gate channel to construct
catch-up mid-states deterministically.

### 4.3 §13.6 gates (concurrency → `-race` + in-repo NumGoroutine/fd leak gate, NOT goleak)
- **InitialHomeAssign** (the C1 fix): first expose lands on the seeded home (not `--tunnel-addr`);
  `ExposeForwardedReq.Home` carried + persisted; pinned dial to the home cert.
- **PerExposeScatter**: one agent, N exposes seeded to DIFFERENT homes; each `clientSession.brokerAddr` differs;
  one Client fans out.
- **SupervisorFieldRace** (R-13): spawn a supervisor, fire a concurrent `Open`-replace mid-redial, run under
  `-race` → no data race (proves value-param threading, not map read-back).
- **KillHomeRehome**: kill a home (nats + tunnel server); agent NATS bounces → onNATSReconnect → higher-epoch
  directive → ApplyHome → Open(newAddr) atomic replace; old supervisor exits, ZERO dials to the dead addr after
  rehome (dialHook probe).
- **ParallelRehome** (R-14): N exposes rehome in PARALLEL (one slow new-home does not serialize/stall others).
- **RehomeTransientRetry** (R-15): the new home lags so the rehome's first `Open` dial returns
  `home_catching_up`; assert `applyReconciliation` retries (not dropped) and converges once the home applies.
- **MassRehomeStorm**: kill a home hosting K exposes across M agents; bounded re-REGISTER (K/sec leader limit +
  jittered backoff); no goroutine/fd leak across the storm; `home_catching_up` is the backpressure.
- **HomeCatchingUpNoTerminal**: a `home_catching_up` deny never collapses to terminal; supervisor keeps
  retrying and succeeds post-catch-up.
- **RehomeRacesShutdown**: a rehome `Open` racing agent ctx-cancel hits the Open-after-Start-cancel rollback
  guard cleanly.
- **NotifyStateConverges**: the proxy-ready hook converges to `true` post-rehome.
- **RestoreThenRegister** (OQ-2): a replica that obtains the row at epoch N via a snapshot restore answers the
  ladder correctly (row-epoch ↔ applied_index co-consistency).

### 4.4 §13.7 cert gates + ex-home one-bind
- **CertRestartInvariance**: restart a home's tunnel server with the SAME stable cert → fp unchanged → agent
  re-pins without rehome; restart the control ephemeral instance → its fp CHANGES (proves the boundary isn't
  accidentally giving production a stable cert).
- **RotationWindowAgentRestart**: harness writes `{current=fpB, previous=fpA, valid_until=now+W}` + home swaps to
  certB (via restart, NOT live hot-swap); restart the agent mid-window → it accepts certB AND certA purely from
  the re-delivered directive (no local pin state); after `valid_until` → certA rejected; window-close
  `{current=fpB, previous=''}` → certA rejected.
- **CertPinBypass** (R-21/R-22 adversarial): a rogue broker at the home addr presenting a non-pinned cert →
  handshake fails in `VerifyConnection` → token NEVER written on the wire (assert the rogue never receives
  REGISTER bytes); force TLS 1.3 session resumption + a swapped cert → still rejected.
- **CertRotationRePush** (R-24): rotation re-pushes fresh directives to connected agents (not a reconnect);
  pure-pin update does NOT tear the transport.
- **ExHomeNewHomeOneBind** (the crux — LADDER-layer one-allow, plus L-1's bounded re-bind window): pause the new home's FSM apply (`applied < reassign`); drive the agent's
  stale old supervisor REGISTER at the ex-home (epoch e) while the new home holds epoch e+1; assert at EVERY
  committed index ≤1 home allows, during the catch-up window ZERO homes allow (ex=terminal, new=transient),
  ALLOW flips the instant the new home applies. Construct the MID-STATE, not just the end-state.
- **TestD6NoTokenOrPinOnSysEvents** (OQ-4): extend `proxy_no_secrets_test.go` — under a rehome storm, no raw
  token / cert pin / PSK appears on any member-readable subject.

### 4.5 RTO budget (§18.2.18) — SUMMED
Single-port rehome worst-case serial sum from verified constants:
- NATS reconnect detect: agent `nats.Options` reconnect-wait + ping (read `cmd/tether/agent.go` defaults, OQ-3).
- `register` round-trip: `RegisterTimeout` × retry.
- `Open` first dial: `tls.Dialer{Timeout: 5s}` + handshake + REGISTER (5s read deadline).
- catch-up backoff: `backoffBase=500ms` doubling, `backoffMax=30s` cap — the DOMINANT term until the new home
  applies past barrier.
- **Target asserted by the harness:** single-port rehome p99 ≤ ~40s worst-case (30s backoff cap + 5s dial + 5s
  register). Mass-rehome storm RTO = per-port RTO + (N / K) leader-push serialization for the last expose.

### 4.6 e2e matrix
Add `TestD6Matrix` to `test/e2e/all_phases_test.go` (`-tags d6_integration`, dedicated `-race` subprocess,
`-timeout 300s`, mirroring the D5 entry). Cheap guards/unit/window/cert-verify stay in `make test`.

**Merge gate:** `make test` + `make e2e` + `make lint` (golangci-lint v2) green; tunnel/rehome concurrency
surface also `-race` + in-repo NumGoroutine/fd leak gate (NOT goleak).

---

## 5. OPEN QUESTIONS — ALL RESOLVED (finalizer)
See 主进程定稿裁定 at the top. OQ-1 → R-26 (replicated `nodes.nats_server`, override the synth's in-memory map).
OQ-2 → resolved (co-consistency holds; add RestoreThenRegister). OQ-3 → deferred to Stage B (mechanical RTO
constants). OQ-4 → reuse agent-only forwarded channel + no-secrets test. OQ-5 → applyReconciliation reschedules
transient with jittered backoff + `catch_up_stalled` log. No design questions remain open.
