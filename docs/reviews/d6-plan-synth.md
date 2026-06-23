# D6 — distributed data-plane — PLAN-OF-RECORD (synthesis candidate)

> Synthesized from 5 adversarial drafts + 5 adversarial critiques. Every conflict the critics raised is resolved with a stated rationale; valid findings are integrated; genuinely unresolved items are listed as OPEN QUESTIONS (§5), not papered over. The main process is the sole finalizer.
>
> **Load-bearing facts re-verified against actual code during synthesis** (not draft paraphrase):
> - `internal/cluster/fsm.go:78-80` — `if l.Type != raft.LogCommand { return nil }`; config/noop entries do NOT advance `applied_index`. `read.go:57` `AppliedIndex()` reads the command-domain cursor; `read.go:99` `CommitIndex()` = `n.raft.CommitIndex()` (all-entries domain). **The two are structurally different domains** — this kills any `applied_index >= raft-commit-barrier` predicate (Critique 1's worst finding, confirmed).
> - `internal/broker/expose.go:115` `handleExposeReq` → `port.Allocate` → `proto.ExposeForwardedReq{Name,Port,LocalPort,Token,ActorFP}` (messages.go:471, **5 fields, NO home**) → `nc.Request(SubjCmdForwarded)` → agent `handleExposeForwarded` (agent/expose.go:46). **The initial expose path never touches `NodeRegisterResp`** (Critique 5's C1, confirmed FATAL for every draft).
> - No agent→server_name binding is persisted at the broker; `cluster_nodes.nats_server_id` exists (0008:16) but is NULL and unwritten until D7 (Critique 5's C2, confirmed).
> - `tunnel.go:546` `Client.brokerAddr` single shared field; `clientSession` (563) = `{publicPort,localPort,token,conn,yamuxSess,cancel,gen}`; `Open` (634) blocks on first dial; `supervise` (677) takes `token` as a **value parameter snapshotted at goroutine spawn** (Critique 2's race root, confirmed); `dialAndRegister` (693) reads `c.brokerAddr` + `clientTLSConfig()`.
> - `parseRegisterLine` (931) uses `len(parts) != 5` and `strconv.Atoi` (NOT ParseInt). `denyIsTransient` (83) default = terminal. `tunnelTokenLookup` (expose.go:53) has NO epoch param; `__proxy__` branch at 86. `tls.go:78` `clientTLSConfig` = `InsecureSkipVerify`. `serverTLSConfig` (56) already honors a non-nil cert; **`NewServerWithCert` is referenced only in comments — does NOT exist**.
> - D5 guard: `test/d5/regression_test.go` `d5BannedTokens` + `productionBrokerFiles` (excludes `audit_publisher.go` + `_test.go`) + self-check + `go list -deps` guards. The exclusion-file precedent for the build-and-prove mechanism file is confirmed.

---

## 0. SCOPE + BUILD-AND-PROVE BOUNDARY (the spine) + NON-GOALS

### 0.1 Scope

D6 makes **expose/tunnel fail over across home brokers**, via seven coupled mechanisms, proven by a multi-broker + agent-failover **test harness** that drives `cluster.Node` directly. D6 ships **real proto-v2 wire and real agent-binary changes** (the v2 fleet reinstalls; no v1 back-compat — separate release line), but **every cluster-specific behavior is gated on receiving a `HomeDirective`** (in the register reply OR — the fix for the fatal C1 gap — in the expose-forward), which a single-node production broker never emits.

### 0.2 Build-and-prove invariants (cutover = D9; violating any fails review)

- **B1.** `cmd/tether/serve.go` stays **byte-unchanged**: constructs no `cluster.Node`, wires no home assignment, gives the tunnel server no stable cert (`tlsCert == nil` → ephemeral self-signed fallback, §16.7).
- **B2.** Every production `internal/broker/*.go` file constructs no `cluster.Node`, emits no `HomeDirective`. `handleRegister`'s and `handleExposeReq`'s responses are **byte-identical** to today in N=1 (the new directive fields are pointer + `omitempty`, left nil by the production path; mirrors `Proxy *ProxyDirective`).
- **B3.** The production `port.Allocate` direct mutator (`internal/port/port.go`) stays **byte-unchanged** (leaves `home_broker=''`, `epoch=0`). The FSM `port.PlanAllocate` learns to bake `home_broker`+`epoch` **only when called with a non-empty home**; current callers pass `("", 0)` → byte-identical baked INSERT.
- **B4.** `internal/cluster` imports **neither** `nats.go` **nor** `internal/broker` (L-2). All home-assignment logic that needs NATS (`ConnectedServerName`, directive construction) lives in `internal/broker`; the `cluster_nodes` read-by-server-name helper lives in a **new `internal/clusternodes` package** (pure SQL, no nats, no raft).
- **B5.** The agent binary changes, but every cluster path is inert without a `HomeDirective`: REGISTER 6th field is always `0`; directives nil; `cert_pins` absent → `InsecureSkipVerify` N=1 fallback; per-expose `brokerAddr` collapses to the single configured `--tunnel-addr`; `home_broker==''` makes `tunnelTokenLookup`'s home/epoch branch inert.
- **Guard:** `test/d6/regression_test.go::TestD6ProductionWiresNoClusterNode` extends the D5 token-scan over `serve.go` + `internal/broker/*.go` **and** `internal/agent/*.go` (Critique 4's V2: the agent must be scanned too).

### 0.3 NON-GOALS (explicitly D7 / D9 / later)

- **`cluster_nodes` production writer** (`ClusterNodeUpsert` via join-PoP) — **D7**. D6 seeds rows directly in the harness.
- **Operator `cluster rotate-tunnel-cert` CLI** — **D7**. D6 proves only the rotation-**window mechanism** by harness-writing the cert columns. **No live `tunnel.Server` cert hot-swap primitive** (Critique 4's V5; rotation is proven by harness *restart* with a new cert, not a live swap).
- **`cluster add` / `raft.AddVoter` / dynamic membership** — D7.
- **`drain` (migrate expose)** — D7; D6 rehome is its prerequisite (先父后子) — do **not** pull forward.
- **Production cutover** (serve.go constructs `cluster.Node`, wires home assignment, gives tunnel server a stable cert, backfills `home_broker=self` for live rows via `cluster init --from-existing`) — **D9**.
- **In-flight transfer/exec/run continuity across rehome** — NOT preserved (Critique 5's N1). Rehome tears the tunnel transport (Open-replace closes the old yamux); any in-flight TCP stream through an exposed port is severed and must be re-established by the end client. Control-plane PTY/exec/run (not riding the tunnel) is unaffected.
- **`replication_degraded` / `broker_down` / `catch_up_stalled` ALERT TABLE ROWS** — **D8b**. D6 may emit a `catch_up_stalled` **log line** only (Critique 5's N2).
- **Active-active for one expose** (same publicPort served by two homes simultaneously) — forbidden invariant, not just unimplemented; the per-port UNIQUE-active index guarantees exactly one home serves; rehome is a hard cutover (Critique 5's N3).
- **Agent-side home roster validation** — the agent trusts the leader-signed directive's `BrokerAddr`; **the cert-pin is the agent's sole authentication that the rehome addr is a legitimate cluster broker** (Critique 5's N4 security-coherence note — makes the fp SSOT doubly load-bearing).
- **New migration** — D6 adds **zero** migrations (0008/0010/0011 already carry everything). Reject Draft 3's 0012 index (Critiques 1/4/5).
- **v1 back-compat** — non-goal (proto v2 reinstall).

---

## 0bis. DOC-FIRST AMENDMENTS (architecture, BEFORE any code)

All amend `docs/distributed-broker-architecture.md`; §18 is audit trail, the body (§0–§17) is the implementation ruler.

- **DA-1 — §6.5 server_name correction (THE load-bearing doc fix).** Replace the prose "(N… nuid)" with: *the agent self-reports `nc.ConnectedServerName()` (== `info.Name` == the deterministic `server_name` rendered by `internal/natscluster/config.go`, e.g. `"tether-1"`), matched against `cluster_nodes.nats_server_id`. The volatile per-boot NUID (`nc.ConnectedServerId()` == `info.ID`) is **explicitly NOT used** — it changes on every nats-server reboot and would break the mapping on the exact home-failover event D6 handles.* Note the `NodeRegisterReq.ServerID` field carries the server_name (name kept for §6.5 continuity).
- **DA-2 — §6.5/§18.3 home eligibility.** A `cluster_nodes` row is home-eligible iff `phase == 'VOTER'`. Other phases (`JOIN_VERIFIED_PENDING_VOTER`/`CATCHING_UP` not yet serving; `VOTER_ADD_FAILED`/`DRAINING`/`RETIRING` exiting) yield no directive; §7.4 reconvergence picks it up next reconnect. Initial home = the broker the agent is currently connected to (a first-guess miss converges via §7.4).
- **DA-3 — §7.1 epoch SSOT.** `port_allocations.epoch` is a **per-port monotone counter**: `0` at allocate (the migration-0010 baseline), `+1` per `OpPortReassignHome`. It is **NOT** a raft index, **NOT** `1`-based. (Resolves Critique 1 F2 + Critique 5 E1: reject Draft 2's `epoch=1` and Draft 4's `epoch=commit-index`.)
- **DA-4 — §7.1-7.2 the home/epoch lookup ladder.** Specify `tunnelTokenLookup`'s new `epoch` param, the `home_broker==self` filter, the **two-dimensional** decision (home-vs-self × presented-epoch-vs-row-epoch; see R-9), and the inert `home_broker==''` branch.
- **DA-5 — §7.2(a) `home_catching_up` TRANSIENT reason.** Register the wire constant; pin that BOTH the broker emit-side and the agent `denyIsTransient` classifier reference a single shared `const` (no duplicated literal).
- **DA-6 — §7.2(b) 6-field REGISTER grammar.** `REGISTER <sid> <nid> <port> <token> <epoch>`; parser accepts **exactly 6** fields; epoch parsed via `strconv.ParseInt(_, 10, 64)`; negative/overflow/non-int → `malformed_register`. **REGISTER carries NO barrier** (the barrier is derived home-locally, R-11).
- **DA-7 — §7.2(c) catch-up barrier predicate, CORRECTED.** The catch-up condition is **epoch-as-local-barrier**: the new home compares the **agent-presented epoch** against the **epoch of its own locally-applied `port_allocations` row** for that port. The leader does NOT thread a raft index over the wire; the home does NOT call `VerifyLeader` on the read path. Document the sufficiency argument (DA-7a): because `OpPortReassignHome(epoch=N)` is applied in the same FSM txn that advances `applied_index`, a replica whose local row shows `epoch>=N` has, by construction, applied that entry — so `localRowEpoch >= presentedEpoch` is exactly "this replica has applied the directive's reassign". The leader's `VerifyLeaderRead` is used **only** to stamp a fresh `epoch` into the directive at issue time (the "leader fetches a fresh barrier value" half); the home's local row-epoch comparison is the "compares its own local applied_index" half. (Resolves Critique 1's worst finding + Critique 5 E2/E3.)
- **DA-8 — §7.4 self-driven rehome.** `onNATSReconnect` → re-register → fresh directives, AND the new **expose-forward path** (DA-12). Epoch-ordered `Open(newAddr)` atomic replace; old supervisor canceled (not stuck redialing the dead addr); leader-pushed `RehomeDirective` as BACKUP over the agent-only forwarded channel; K/sec leader push rate-limit + agent backoff bound; **rehome `Open`s run concurrently** (R-14) and **transient denies on a rehome's first dial are retried** (R-15).
- **DA-9 — §7.5 per-expose brokerAddr.** addr/epoch/certPins move to `clientSession` (keyed by publicPort); one Client fans out to N homes; `Client.brokerAddr` is **retained** as the N=1 fallback when a session carries no addr (Critique 5 S5: minimizes N=1 churn).
- **DA-10 — §7.7/§15 cert pinning + rotation window.** `cert_fp` format SSOT = `"sha256:" + hex(SHA-256(cert.Raw))` (DER of leaf, never SPKI). One `tunnel.CertFingerprint(*x509.Certificate)` used by BOTH harness seeder and agent verifier. Agent verify uses **`VerifyConnection`** (resumption-safe), NOT `VerifyPeerCertificate`. `cert_pins{current, previous, valid_until}` dual-pin window; `previous` accepted iff `previous!="" && valid_until>0 && now<valid_until` (fail-closed on zero valid_until). N=1: empty pins → `InsecureSkipVerify` fallback (the ONLY fallback). **No first-dial-without-pins path for a clustered home** (Critique 3's worst finding): a clustered expose defers its dial until pins arrive.
- **DA-11 — §16.7 deviation registry.** Record the D6 build-and-prove deviation (production tunnel server stays ephemeral; stable-cert + pins reached only behind a `HomeDirective` the harness emits; cutover = D9), mirroring the D5 §16 entry.
- **DA-12 — §7.2/§6.5 INITIAL-home delivery (the C1 fix).** `ExposeForwardedReq` gains the home directive fields; `handleExposeReq` resolves home from the agent's **persisted server_name binding** (DA-13); `handleExposeForwarded` persists them into `PortToken` and opens against the home. Inert in N=1 (empty home → byte-identical `ExposeForwardedReq`).
- **DA-13 — §6.5 agent→server_name binding storage.** The broker persists the agent's last-reported `ServerID` (server_name) so it is queryable at expose time and re-resolved on rehome. Storage = a new column on the existing `nodes` registration row written at register time (in the FSM path it rides `OpNodeRegister`; production leaves it empty → inert). **OPEN QUESTION OQ-1** flags whether this needs a migration or can reuse an existing nullable column — see §5.
- **DA-14 — §18.2 audit trail + §18.2.18 RTO budget.** Append D6 entries: `OpPortReassignHome` promoted from deferred; the `HomeDirective`/`RehomeDirective` shapes; the 6-field REGISTER; the server_name-not-NUID ruling; the epoch-as-local-barrier ruling; the summed RTO budget (§4.5).

---

## 1. NUMBERED RULINGS

### Mechanism 1 — server-id bridge + home assignment

- **R-1.** Bridge key = `nc.ConnectedServerName()` (deterministic server_name), NOT the NUID. `NodeRegisterReq` gains `ServerID string` (`json:"server_id,omitempty"`). *Rationale: NUID rotates per nats-server reboot, breaking the mapping on the failover event itself (DA-1).*
- **R-2.** Home assignment is **leader-authoritative**, broker-side (needs nats), eligibility = `phase=='VOTER'`. The agent never self-selects a home. *Rationale: L-2 forbids nats in `internal/cluster`; only `VOTER` nodes serve (DA-2).*
- **R-3.** New `cluster_nodes` read-by-`nats_server_id` helper lives in a **new `internal/clusternodes` package** (pure SQL; no nats, no raft). D7's `ClusterNodeUpsert` writer co-locates there later. *Rationale: keeps L-2 clean; reject Draft 4's `cluster.LookupNodeByServerName` (cluster-prefix muddies L-2, and Draft 4 self-contradicts) — adopt Draft 1's placement (Critiques 1 F6, 4 V7).*
- **R-4.** The broker persists the agent's reported server_name at register time (DA-13) so `handleExposeReq` can resolve home at expose time and re-resolve on rehome. Inert in production (never populated; resolve returns empty → empty home).

### Mechanism 2 — per-expose home_broker/epoch + OpPortReassignHome

- **R-5.** `OpPortReassignHome` (command.go named-deferred) is promoted to a live op: one `defaultAppliers()` entry → `genericExecApplier{}` (stateless baked-SQL exec). *Rationale: rehome is a distinct transition from allocate; folding into allocate breaks the epoch/idempotency audit and the build-and-prove boundary (Critique 4 V1).*
- **R-6.** `PlanAllocate` gains a home param and bakes `home_broker = LitText(homeNodeID)`, `epoch = LitInt(0)` **only when home is non-empty**; all current callers pass `("", 0)` → byte-identical INSERT (default columns). The live `port.Allocate` direct mutator is byte-unchanged. *Rationale: REJECT Draft 2 L-1 "born home-correct on the live allocate path" — it is the single worst boundary violation (Critique 4 V1); the FSM-write seam must stay constructible-but-never-wired like D2-D5.*
- **R-7.** `PlanReassignHome(db, publicPort, newHome, now) (newEpoch int64, *cluster.Command, error)` reads the current epoch under `applyMu` (held by `Propose`), bakes an **all-literal** `UPDATE ... SET home_broker=<lit>, epoch=<LitInt(curEpoch+1)> WHERE port=<lit> AND state='ALLOCATED' AND epoch < <LitInt(curEpoch+1)>`. *Rationale: leader-baked literal (NOT `epoch=epoch+1` column arithmetic) + monotonic CAS guard `epoch < newEpoch` makes a stale ex-leader's lower-epoch reassign a deterministic `RowsAffected==0` no-op on every replica — the ex-home double-bind FSM-layer fence (Critique 1 F3; adopt Draft 5's `WHERE epoch < newEpoch` over Draft 2's `= curEpoch` — tolerates a missed intermediate).*
- **R-8.** The **leader-driven** `OpPortReassignHome` (broker-death backup path) carries **NO reqID** and relies solely on the R-7 CAS guard for idempotency. *Rationale: the D4 ledger requires the reqID be originating-broker-minted, never leader-minted; the leader-push path has no non-leader originator, so it cannot satisfy the invariant — like D4's provision/join "no key" ops, the CAS guard is the idempotency anchor (Critique 1 F4 — a real tension no draft resolved).*

### Mechanism 3 — tunnelTokenLookup home/epoch/catch-up

- **R-9.** `tunnelTokenLookup` gains `epoch int64`; after the existing token/sid/nid/`__proxy__` checks, **if `a.HomeBroker == ""` → skip the entire ladder (inert, byte-equivalent to today)**. Else, the decision is a function of BOTH `(home_broker vs self)` AND `(presentedEpoch vs a.Epoch)`:
  - `presentedEpoch < a.Epoch` → **terminal** `token_unknown_or_revoked` (agent holds a superseded directive; the higher-epoch directive will rehome it).
  - `presentedEpoch > a.Epoch` → **transient** `home_catching_up` (this replica has not yet applied the latest reassign — REGARDLESS of home-vs-self).
  - `presentedEpoch == a.Epoch && a.HomeBroker == self` → **allow**.
  - `presentedEpoch == a.Epoch && a.HomeBroker != self` → **terminal** `token_unknown_or_revoked` (genuinely an ex-home/never-home replica at the same epoch).
  *Rationale: REJECT Drafts 1/4's unconditional `home != self → terminal` — it bricks the new home during catch-up (the new home has the old row `{home=A,epoch=N-1}` with `self=B`, sees `home!=self` and terminally denies the very home the agent was directed to). The only terminal arms are `presented < row` (superseded) and `presented == row && home != self` (genuine ex-home). `presented > row` is ALWAYS transient (a higher presented epoch can only come from a leader-committed directive this replica will eventually apply) (Critique 2's HIGH finding; generalizes Draft 3 R-B3).*
- **R-10.** "self" = `b.selfNodeID()` = `b.node.SelfID()` when `b.node != nil`, else `""` (with an explicit nil-guard in the body). In production `b.node == nil` → `self==""`, and the `home_broker==''` branch (R-9) short-circuits before `self` is consulted. *Rationale: keeps `self` behind the `cluster.Node` seam the guard bans from production; the `broker.go` `NewServer(..., b.tunnelTokenLookup, ...)` call site stays textually identical because the method value's signature change rides the `TokenLookup` type (Critique 4 V4).*

### Mechanism 4 — REGISTER 6th field

- **R-11.** `dialAndRegister` writes `fmt.Sprintf("REGISTER %s %s %d %s %d\n", sid, nid, publicPort, token, epoch)`. `parseRegisterLine` changes `len(parts) != 5` → `!= 6`, parses epoch via `strconv.ParseInt(parts[5], 10, 64)` (NOT `Atoi`), rejects parse error / negative / overflow → `malformed_register`. `TokenLookup` type + `handleAgent` gain the epoch param. The barrier is NOT a wire field (R-9/DA-7). N=1: epoch always `0`. *Rationale: exact-6 strictness rejects malformed/partial-deploy lines; `ParseInt(_,64)` matches `port_allocations.epoch int64` and rejects overflow that `Atoi` would silently truncate (Critique 3 W1).*

### Mechanism 5 — catch-up barrier

- **R-12.** Barrier predicate = epoch-as-local-barrier (DA-7): the home compares `presentedEpoch` vs its own locally-applied `a.Epoch`; `presented > local` → `home_catching_up`. No raft index threaded; no `VerifyLeader` on the read path. *Rationale: `applied_index` (command-domain) and raft `CommitIndex` (all-entries domain) are incompatible; comparing them bricks the home permanently after the first post-genesis term (Critique 1's worst finding — verified at fsm.go:78-80). The local row-epoch IS the only state the bind decision needs and advances in the same FSM txn as `applied_index`.*

### Mechanism 6 — agent per-expose brokerAddr + rehome + denyIsTransient

- **R-13.** Move `brokerAddr`/`epoch`/`certPins` onto `clientSession`; **retain `Client.brokerAddr` as the N=1 fallback** (used when a session's addr is empty). `Open` signature → `Open(publicPort, localPort int, token, brokerAddr string, epoch int64, certPins CertPinSet) error`. **The supervisor MUST receive these as value parameters snapshotted at the `go c.supervise(...)` spawn — NEVER read them back from `c.sessions[port]` inside the loop.** *Rationale: `token`'s race-freedom comes from being a spawn-time value param read by exactly one goroutine; reading the new fields from the shared map inside the loop is an unsynchronized read against `Open`-replace's map write → a hard `go test -race` failure (Critique 2's CRITICAL worst bug, present in all 5 drafts). A `-race` unit test asserting this is mandatory.*
- **R-14.** Rehome runs each directive's `Open` **concurrently** (bounded worker pool, one goroutine per expose), and the `Open`-replace must `old.cancel()` the old supervisor **up-front** so it stops redialing the dead addr immediately. *Rationale: `Open` blocks on the first dial; serial rehome of N exposes = N × dial-timeout, blowing the RTO budget and leaving the old supervisor redialing the dead addr for `new_dial_timeout` per expose (Critique 2 HIGH; only Draft 3 R-D3a spotted it, under-specified). The rehome `Open` logs-not-rolls-back on first-dial error (no broker reply to send frpc_failed to).*
- **R-15.** A `home_catching_up` (or any transient) deny returned by a **rehome's first `Open` dial** must be retried by `applyReconciliation` (bounded retry/schedule), NOT dropped. *Rationale: the first `Open` dial returns the error before any supervisor exists, so the supervisor's transient-retry loop never sees it — the rehome would silently fail until the next reconnect/push (Critique 5's G4 "single most likely real bug"). Either rehome `Open` internally retries transient denies, or `applyReconciliation` classifies the returned `DenyError` and reschedules.*
- **R-16.** `home_catching_up` added to `denyIsTransient` via a **single shared `const proto.ReasonHomeCatchingUp = "home_catching_up"`** referenced by BOTH the broker emit-side and the tunnel classifier. *Rationale: a duplicated literal is the brick-the-fleet drift risk; a shared const makes the broker emit fail to compile without the symbol the classifier also uses. The const lives in `internal/proto` (the SSOT both `internal/broker` and `internal/tunnel` already import) (Critiques 2/3 W2/W3, Critique 5 S1).*
- **R-17.** Rehome rides BOTH `onNATSReconnect` (re-register reply carries fresh directives) AND the expose-forward path (DA-12), with the leader-pushed `RehomeDirective` (agent-only forwarded channel, never sys.events) as BACKUP. All paths epoch-ordered (apply iff `directive.Epoch > clientSession.epoch`) → idempotent. K/sec leader push rate-limit + agent full-jitter backoff. *Rationale: the leader-push rate-limit is NOT the primary herd control — the un-jittered first `Open` after a mass reconnect is; the `home_catching_up` transient IS the natural backpressure (the lagging new home spreads the herd over the backoff loop) (Critique 2 MEDIUM).*
- **R-18.** `state.json` `PortToken` gains `HomeBrokerAddr string`, `Epoch int64` (both `omitempty`); `CertPins` are NOT persisted (re-delivered on every register/expose, like ProxyState PSKs). `replayPortsFromState` re-targets the right home on boot; empty addr → the `--tunnel-addr` fallback (R-13). A pre-D6 state.json loads (omitempty → empty/0). *Rationale: omitempty keeps the N=1 file byte-stable; pin re-delivery avoids stale-pin-on-disk and the rotation-window agent-restart case (Critique 5 P3).*

### Mechanism 7 — cert pinning + stable cert + rotation window

- **R-19.** Add `tunnel.NewServerWithCert(addr, publicHost string, lookup TokenLookup, cert *tls.Certificate, logger) *Server` (net-new; the comment-referenced-but-missing constructor) — **harness/test only**; production `broker.go` keeps calling `NewServer` (cert nil → ephemeral). Add `tunnel.LoadServerCert(certPEM, keyPEM) (*tls.Certificate, error)` (harness now, D7/D9 later). *Rationale: gives the harness a stable cert without touching serve.go/broker.go construction; the guard bans `NewServerWithCert(`/`LoadServerCert(` in production files (Critiques 4/5 S4).*
- **R-20.** Single fp SSOT: `tunnel.CertFingerprint(cert *x509.Certificate) string` = `"sha256:" + hex(SHA-256(cert.Raw))` (DER of leaf, never SPKI). Used by BOTH the harness `cluster_nodes.cert_fp` seeder and the agent verifier. *Rationale: a divergent fp formula is the classic silent pin-bypass; one function eliminates drift; the `sha256:` prefix future-proofs the algorithm (Critiques 3/5 P1; adopt Draft 4 R-CERT-2).*
- **R-21.** Agent pin verification: `dialAndRegister` builds the per-session `tls.Config` from `sess.certPins`. **Empty pins → `InsecureSkipVerify: true`, no callback** (the ONLY N=1 fallback, §16.7, byte-identical to today). **Non-empty pins → `InsecureSkipVerify: true` + a `VerifyConnection` callback** that: treats empty `cs.PeerCertificates` as **reject**; computes `CertFingerprint(cs.PeerCertificates[0])`; accepts iff `fp == pins.Current` OR (`pins.Previous != "" && pins.ValidUntil > 0 && now < pins.ValidUntil && fp == pins.Previous`); returns a non-nil error otherwise. *Rationale: `VerifyConnection` runs on EVERY handshake including TLS 1.3 session resumption (`VerifyPeerCertificate` does NOT — Critique 3's WORST finding); fail-closed on empty/zero-valid_until; the handshake fails BEFORE the REGISTER token is written, so a MITM presenting a non-pinned cert never receives the bearer token. Reject Draft 4's `VerifyPeerCertificate` choice and Draft 3's first-dial-without-pins path (R-D8a).*
- **R-22.** **No InsecureSkipVerify path when a clustered home is targeted.** A `PortToken`/directive with a non-empty `HomeBrokerAddr` but no pins yet **defers the dial** until the register/expose reply delivers pins. *Rationale: closes the token-disclosure-to-MITM-on-every-clustered-dial hole (Critique 3 worst finding part 2).*
- **R-23.** `CertPins` wire shape: `{Current string; Previous string omitempty; ValidUntil *time.Time omitempty}` (pointer-time distinguishes NULL/no-rotation from a zero time). The leader maps `cluster_nodes.cert_fp/_prev/_valid_until` → `CertPins`; NULL prev/valid_until → `{Current, "", nil}`. *Rationale: int64-unix (Draft 4) cannot distinguish NULL from epoch-0; `*time.Time` matches the SQL `TIMESTAMP` nullability and the LitTime baking (Critiques 3 W6, 5 P2/P4).*
- **R-24.** Cert rotation forces the leader to **re-push fresh directives** (same addr, same home-epoch, new pins) to affected agents — a rotation is not a reconnect, so the agent will not otherwise learn the new pins. A pure-pin update (same addr, same home-epoch) updates `sess.certPins` **in place without tearing the transport** (the rotation window keeps the old pin valid until `valid_until`, so there is no urgency). *Rationale: the rotation re-push trigger is unspecified in all drafts except a partial Draft 4/3 mention (Critique 5 P3); D6 proves the MECHANISM via harness, the operator command is D7.*
- **R-25 (build-and-prove guard).** `TestD6ProductionWiresNoClusterNode` extends `d5BannedTokens` with `{"NewServerWithCert(", "LoadServerCert(", "HomeDirective{", "RehomeDirective{", "homeDirectivesForRegister", "PlanReassignHome(", "OpPortReassignHome", "ApplyHome("}` and scans `cmd/tether/serve.go` + `internal/broker/*.go` (EXCLUDING the build-and-prove file `internal/broker/home.go` + `_test.go`) **and `internal/agent/*.go`** for the cluster-emit tokens. Self-check proves the guard discriminates (constructor + struct-literal both caught; clean source not). The `RehomeDirective` subscribe-site + any `reassign_home` verb constant are gated behind `b.node != nil` and added to the ban list. *Rationale: the agent must be scanned (Critique 4 V2); the leader-push subscribe must not run in N=1 (Critique 4 V6).*

---

## 2. EXACT SURFACE (concrete Go signatures)

### proto (`internal/proto/messages.go`, `version.go`)
```go
// NodeRegisterReq += (carries server_name, NOT the NUID; see DA-1)
ServerID string `json:"server_id,omitempty"`

// NodeRegisterResp += (pointer + omitempty → nil in N=1 = byte-identical resp,
// the Proxy *ProxyDirective precedent). Slice form is REJECTED (an empty-but-
// non-nil []HomeDirective marshals "home":[] ≠ omitted, breaking B2 — Critiques 3 W4 / 4 V2).
Home *HomeAssignment `json:"home,omitempty"`

type HomeAssignment struct {
    Directives []HomeDirective `json:"directives,omitempty"`
}

// HomeDirective — authoritative home for ONE expose (per publicPort, §7.5).
// Epoch-ordered (agent applies iff Epoch > clientSession.epoch). Carries NO raw
// token (token unchanged across rehome). Travels ONLY over register-reply _INBOX /
// expose.req.forwarded / agent-only forwarded channel — NEVER sys.events.
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
    Previous   string     `json:"previous,omitempty"`     // non-empty only mid-rotation
    ValidUntil *time.Time `json:"valid_until,omitempty"`  // nil outside a rotation window
}

// RehomeDirective — leader-pushed BACKUP (§7.4), agent-only forwarded channel.
type RehomeDirective struct {
    HomeDirective
}

// ExposeForwardedReq += (the C1 INITIAL-home fix; omitempty → byte-identical N=1).
// Home *HomeDirective `json:"home,omitempty"`

// const ReasonHomeCatchingUp = "home_catching_up"  // SSOT shared by broker emit + tunnel classify
```

### cluster + clusternodes + port
```go
// internal/cluster/command.go — promote OpPortReassignHome (one defaultAppliers entry).
// internal/cluster/node.go — add SelfID() string (returns the raft ServerID it was built with).

// internal/clusternodes/read.go  (NEW pkg; pure SQL, no nats, no raft — L-2 clean)
type HomeNode struct {
    NodeID     string
    NatsServer string     // nats_server_id (server_name)
    TunnelAddr string
    PublicHost string
    CertFP     string
    CertFPPrev string      // "" outside a rotation window
    CertValid  *time.Time  // nil outside a rotation window
    Phase      string
}
func LookupByNatsServer(db *sql.DB, server string) (*HomeNode, error) // ErrNotFound if no match

// internal/port/plan.go
func PlanAllocate(db *sql.DB, sid, nid, name string, localPort, desiredPort int,
    createdByFP, homeBroker string, cfg AllocCfg) (*Allocation, *cluster.Command, error) // home "" => default bake
func PlanReassignHome(db *sql.DB, publicPort int, newHome string, now time.Time) (newEpoch int64, _ *cluster.Command, _ error)

// internal/port/port.go — Allocation += HomeBroker string, Epoch int64;
// LookupByTokenHash SELECT widened to include home_broker, epoch (legacy rows => '' / 0).
// NOTE (Critique 4 V1 part 3): this SELECT-widening is a live production read-path change,
// ALLOWED but pinned by a differential test asserting legacy rows return ''/0 and
// tunnelTokenLookup stays byte-equivalent then.
```

### tunnel (`internal/tunnel/tunnel.go`, `tls.go`)
```go
type TokenLookup func(sid, nid string, port int, tokenHash string, epoch int64) error // epoch LAST (Critique 5 S2: avoid port/epoch transposition)

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
`b.node *cluster.Node` is the single seam. Production never sets it (`nil`). `selfNodeID()`, `homeDirectivesForRegister`, `homeDirectiveForExpose`, the `RehomeDirective` push/subscribe, and the `PlanAllocate(home!="")`/`PlanReassignHome` writes are all reachable ONLY when `b.node != nil`. The harness constructs `b.node`; the guard (R-25) locks production out.

---

## 3. FULL EPOCH LIFECYCLE (end-to-end, every link)

```
ALLOCATE (initial expose, clustered harness):
  leader resolves home (server_name → cluster_nodes row, phase==VOTER)
  → PlanAllocate(home=B) bakes INSERT ... home_broker='B', epoch=0   [DA-3: baseline 0]
  → handleExposeReq injects fwdReq.Home = HomeDirective{Name, PublicPort, BrokerAddr=B.tunnel_addr, Epoch=0, CertPins=B.fp}
  → agent handleExposeForwarded persists PortToken{HomeBrokerAddr=B, Epoch=0} + Open(port, local, token, B, 0, pins)
  → dialAndRegister to B: "REGISTER sid nid port token 0\n"  [R-11: 6th field = 0]
  → B.tunnelTokenLookup(..., epoch=0): a.HomeBroker='B'==self, presented(0)==a.Epoch(0) → ALLOW   [R-9]

N=1 (production, same code, inert):
  port.Allocate (direct mutator) leaves home_broker='', epoch=0
  → fwdReq.Home = nil (b.node==nil)  → agent Open(port, local, token, "", 0, {})  [brokerAddr empty → --tunnel-addr fallback]
  → "REGISTER sid nid port token 0\n"  → tunnelTokenLookup: a.HomeBroker=='' → SKIP ladder → ALLOW (byte-equivalent to today)

REHOME (broker B dies; leader reassigns B→C):
  leader: PlanReassignHome bakes UPDATE ... home_broker='C', epoch=LitInt(1) WHERE port AND state='ALLOCATED' AND epoch<1
        [R-7: per-port counter +1; CAS monotonic guard]
  → row on every replica that applies: {home='C', epoch=1}
  agent path (R-17): onNATSReconnect (B's nats died → conn bounced to C) → re-register
        → resp.Home = HomeDirective{..., BrokerAddr=C, Epoch=1, CertPins=C.fp}
        → ApplyHome: directive.Epoch(1) > clientSession.epoch(0) → Open(port, local, token, C, 1, C.pins)  [R-13 atomic replace; old supervisor canceled R-14]
        → "REGISTER sid nid port token 1\n"
  ex-home race (agent's stale old supervisor hits B before B applies, presents epoch 0):
        B (applied, sees {home='C', epoch=1}): presented(0) < a.Epoch(1) → TERMINAL token_unknown_or_revoked  [R-9: B sheds it]
  new-home catch-up (C has not yet applied the reassign, still sees {home='B', epoch=0}):
        C: presented(1) > a.Epoch(0) → TRANSIENT home_catching_up  [R-9/R-12: C holds off, NOT a brick]
        → agent rehome Open first-dial gets home_catching_up → applyReconciliation RETRIES (R-15)
        → C applies the reassign (local row → {home='C', epoch=1}) → next REGISTER: presented(1)==a.Epoch(1) && home=='C'==self → ALLOW
  EXACTLY ONE BIND across the window: B terminal (epoch), C transient-then-allow (epoch); during catch-up ZERO homes allow.
```
Every link: `port_allocations.epoch` (leader-baked literal) ↔ `HomeDirective.Epoch` (read from the row) ↔ `clientSession.epoch` (set by ApplyHome/Open) ↔ REGISTER 6th field (`strconv` of `sess.epoch`) ↔ `tunnelTokenLookup` compare (`presented` vs local `a.Epoch`). The catch-up barrier is the local-row-epoch comparison — NO raft index anywhere (DA-7).

---

## 4. TEST PLAN

### 4.1 Cheap unit + guard (in `make test`)
- **TestD6ProductionWiresNoClusterNode** (R-25): token-scan over serve.go + broker/*.go (excl. home.go) + **agent/*.go**; self-check discriminates constructor + struct-literal but not clean source.
- **TestD6ClusterNoNATSImport / TestD6ClusterNodesNoNATSNoClusterImport**: `go list -deps` — `internal/cluster` bans nats + broker; `internal/clusternodes` bans nats + cluster (L-2).
- **TestD6RegisterLineRoundTrip**: exactly-6 accept; 5/7 reject; non-int / negative / overflow epoch reject; trailing/embedded spaces; CRLF. Asserts agent emits `...  0\n` in N=1 (golden bytes — Critique 4 V2).
- **TestD6DenyTransientClassification**: `denyIsTransient(proto.ReasonHomeCatchingUp)==true`; existing reasons unchanged; unknown still terminal; **const-equality** test pinning emit string == classify symbol (no-whitespace assertion — Critique 3 W2).
- **TestD6HomeDirectiveByteIdentityN1**: `NodeRegisterResp{Home:nil}` marshals byte-identical to a pre-D6 golden; `ExposeForwardedReq{Home:nil}` likewise; `CertPins` nil-time omits the key (Critiques 3 W4 / 4 V2).
- **TestD6PlanAllocateInertHome**: `PlanAllocate(...,"",...)` bakes byte-identical INSERT to today (UTC + non-UTC); `PlanAllocate(...,"node-2",...)` bakes `home_broker='node-2', epoch=0` (LitText/LitInt, no Args).
- **TestD6ReassignHomeMonotonic**: `PlanReassignHome` bakes `epoch=LitInt(cur+1)` + `WHERE epoch < cur+1`; apply twice → row epoch unchanged (CAS no-op); `ErrNotFound` on absent/non-ALLOCATED.
- **TestD6TunnelTokenLookupLadder** (the two-dimensional ladder, R-9): `home==''`→inert/byte-equivalent; `home==self && presented==row`→allow; `home!=self && presented==row`→terminal; `presented<row`→terminal; `presented>row` (BOTH home==self AND home!=self)→`home_catching_up`. Anti-enum: home-mismatch and absent yield identical code/bytes.
- **TestD6CertPinVerify** (R-21, adversarial): in-set accept; `previous` within window accept; `previous` after `valid_until` reject; `previous` with `valid_until==nil` reject (fail-closed); fp prefix-of-current reject (exact match only); empty `cs.PeerCertificates` reject; empty pins → InsecureSkipVerify no-callback path byte-identical to today's `clientTLSConfig()`.
- **TestD6CertFingerprintSSOT**: fixed DER → exact `sha256:...`; two certs same key diff serial → different fp (rotation detectability); truncated/empty DER → stable error.
- **TestD6LookupByNatsServer**: match → row; no match → ErrNotFound; NULL prev/valid_until → `{Current,"",nil}`; phase surfaced raw (eligibility decided by caller).
- **TestD6TokenLookupArgOrder** (Critique 5 S2): a port/epoch transposition fails (distinct values catch the swap).

### 4.2 Gated harness (`//go:build d6_integration`, `TestD6Matrix -race`, dedicated subprocess like TestD5Matrix)
Multi-broker (≥2-3 routed NATS + mTLS raft) each running a REAL `tunnel.Server` via `NewServerWithCert` (stable cert) + a real agent `tunnel.Client`. Seed `cluster_nodes` rows directly (node_id, nats_server_id=server_name, tunnel_addr, public_host, cert_fp/_prev/_valid_until, phase=VOTER) — D5 precedent. **Plus a control `NewServer` (nil cert) instance** asserting it stays ephemeral (Critique 5 G5). Test seams: a `dialHook func(addr string)` on the Client (positive dead-addr probe, G1), and an FSM `pauseApplyAt(idx)` / gate channel to construct catch-up mid-states deterministically (G6).

### 4.3 §13.6 gates (concurrency → `-race` + in-repo NumGoroutine/fd leak gate, NOT goleak)
- **InitialHomeAssign** (the C1 fix): first expose lands on the seeded home (not `--tunnel-addr`); `ExposeForwardedReq.Home` carried + persisted; pinned dial to the home cert.
- **PerExposeScatter**: one agent, N exposes seeded to DIFFERENT homes; each `clientSession.brokerAddr` differs; one Client fans out.
- **SupervisorFieldRace** (R-13): spawn a supervisor, fire a concurrent `Open`-replace mid-redial, run under `-race` → no data race (proves value-param threading, not map read-back).
- **KillHomeRehome**: kill a home (nats + tunnel server); agent NATS bounces → onNATSReconnect → higher-epoch directive → ApplyHome → Open(newAddr) atomic replace; **old supervisor exits, ZERO dials to the dead addr after rehome** (dialHook probe, G1).
- **ParallelRehome** (R-14, G2): N exposes rehome in PARALLEL (one slow new-home does not serialize/stall the others).
- **RehomeTransientRetry** (R-15, G4): the new home lags so the rehome's first `Open` dial returns `home_catching_up`; assert `applyReconciliation` retries (not dropped) and converges once the home applies.
- **MassRehomeStorm**: kill a home hosting K exposes across M agents; bounded re-REGISTER (K/sec leader limit + jittered backoff); no goroutine/fd leak across the storm; `home_catching_up` is the backpressure (no thundering herd).
- **HomeCatchingUpNoTerminal** (R-7-gate): a `home_catching_up` deny never collapses to terminal; supervisor keeps retrying and succeeds post-catch-up.
- **RehomeRacesShutdown** (G3): a rehome `Open` racing agent ctx-cancel hits the Open-after-Start-cancel rollback guard cleanly.
- **NotifyStateConverges** (Critique 2 LOW): the proxy-ready hook converges to `true` post-rehome.

### 4.4 §13.7 cert gates + ex-home one-bind
- **CertRestartInvariance**: restart a home's tunnel server with the SAME stable cert → fp unchanged → agent re-pins without rehome; restart the control ephemeral instance → its fp CHANGES (proves the boundary isn't accidentally giving production a stable cert, G5).
- **RotationWindowAgentRestart**: harness writes `{current=fpB, previous=fpA, valid_until=now+W}` + home swaps to certB (via restart, NOT live hot-swap — V5); restart the agent mid-window → it accepts certB AND certA purely from the re-delivered directive (no local pin state, R-18); after `valid_until` → certA rejected; window-close `{current=fpB, previous=''}` → certA rejected.
- **CertPinBypass** (R-21/R-22 adversarial): a rogue broker at the home addr presenting a non-pinned cert → handshake fails in `VerifyConnection` → token NEVER written on the wire (assert the rogue never receives REGISTER bytes); force TLS 1.3 session resumption + a swapped cert → still rejected (proves `VerifyConnection` not `VerifyPeerCertificate`).
- **CertRotationRePush** (R-24): rotation re-pushes fresh directives to connected agents (not a reconnect); pure-pin update does NOT tear the transport.
- **ExHomeNewHomeOneBind** (the crux): pause the new home's FSM apply (`applied < reassign`); drive the agent's stale old supervisor REGISTER at the ex-home (epoch e) while the new home holds epoch e+1; assert at EVERY committed index ≤1 home allows, during the catch-up window ZERO homes allow (ex=terminal, new=transient), ALLOW flips the instant the new home applies. Construct the MID-STATE (G6), not just the end-state.

### 4.5 RTO budget (§18.2.18) — SUMMED (Critique 5 T1)
Single-port rehome worst-case serial sum from verified constants:
- NATS reconnect detect: agent `nats.Options` reconnect-wait + ping (read `cmd/tether/agent.go` defaults — see OQ-3).
- `register` round-trip: `RegisterTimeout` × retry (read defaults).
- `Open` first dial: `tls.Dialer{Timeout: 5s}` (tunnel.go:694) + handshake + REGISTER (5s read deadline).
- catch-up backoff: `backoffBase=500ms` doubling, `backoffMax=30s` cap (tunnel.go:581-582) — the DOMINANT term until the new home applies past barrier.
- **Target asserted by the harness:** single-port rehome p99 ≤ ~40s worst-case (30s backoff cap + 5s dial + 5s register). **Mass-rehome storm RTO = per-port RTO + (N / K) leader-push serialization** for the last expose. The harness asserts both.

### 4.6 e2e matrix
Add `TestD6Matrix` to `test/e2e/all_phases_test.go` (`-tags d6_integration`, dedicated `-race` subprocess, `-timeout 300s`, mirroring the D5 entry). Cheap guards/unit/window/cert-verify stay in `make test` (the routed-NATS+JS+tunnel cluster would starve `make test`'s parallel run).

**Merge gate:** `make test` + `make e2e` + `make lint` (golangci-lint v2) green; tunnel/rehome concurrency surface also `-race` + in-repo NumGoroutine/fd leak gate (NOT goleak).

---

## 5. OPEN QUESTIONS (ranked for the main process)

1. **OQ-1 — agent→server_name binding storage (DA-13).** The broker must persist the agent's reported server_name to resolve home at *expose* time (the expose RPC carries no `ServerID`). Options: (a) a new nullable column on the `nodes` table written by `OpNodeRegister` (a migration — but D6 claims zero migrations); (b) reuse an existing nullable column; (c) an in-memory `(sid,nid)→server_name` map on the broker (lost across broker restart — but in build-and-prove the harness re-registers, and production never populates it → inert). **Recommendation: (c) in-memory for D6 build-and-prove (zero migration, inert in production), with a doc note that D9 cutover persists it.** Needs the main process to confirm the in-memory map is acceptable given a broker restart would lose the binding until the agent re-registers (which it does on every reconnect).

2. **OQ-2 — does the catch-up sufficiency argument (DA-7a) fully hold under snapshot restore?** Critique 5 E2 raised: a replica that obtains the row at epoch N via a *stale snapshot restore* could have `localRowEpoch==N` while its `applied_index` is otherwise behind. Because the snapshot is taken at a consistent `applied_index` (D1 online-backup invariant) and the row+`applied_index` are written in the same FSM txn, a restored snapshot's row epoch and applied_index are mutually consistent — so the argument *should* hold, but the main process must confirm the D1 snapshot/restore preserves the row↔applied_index co-consistency, and the harness must include a **restore-then-REGISTER** case to prove it.

3. **OQ-3 — concrete RTO numbers.** §4.5 needs the agent's actual `nats.Options` (ReconnectWait/MaxReconnects/PingInterval) and `RegisterTimeout`/retry defaults read from `cmd/tether/agent.go` to finalize the summed budget and the harness assertion threshold. (Mechanical; just not in the verified surface yet.)

4. **OQ-4 — `RehomeDirective` backup-push subject.** Reuse the existing agent-only forwarded channel vs a new broker-only `tether.v2.cluster.*` subject. Recommendation: reuse the existing agent-only `.req.forwarded`-style channel (no new subject; same secrecy boundary as `ProxyDirective`, enforced by extending `proxy_no_secrets_test.go` to a `TestD6NoTokenOrPinOnSysEvents` rehome-storm assertion — Critique 3 W5). Confirm no new subject is needed.

5. **OQ-5 — bounded retry policy for R-15 (rehome first-dial transient).** Internal `Open` retry vs `applyReconciliation`-level reschedule, and the max-wait before a `catch_up_stalled` LOG (not alert row — N2). Recommendation: `applyReconciliation` classifies the returned `DenyError`, reschedules transient `home_catching_up` with the same full-jitter backoff bound, logs `catch_up_stalled` after a max-wait, never collapses to terminal. Confirm the reschedule lives in the agent (not a changed `Open` contract).
