# D6 — internal review (Stage C)

> **RESOLUTION: CONDITIONAL PASS → all findings dispositioned + applied.** 6× Opus-4.8 adversarial
> reviewers (each a distinct axis, read-only). All 6 verdicts were CONDITIONAL PASS — the implemented
> code paths were correct; the findings were 1 real concurrency defect (unbounded rehome goroutines),
> guard/coverage gaps, and 2 honest doc overclaims. The main process (sole adjudicator) ACCEPTED
> essentially all findings and applied every code fix + doc correction + the promised test backfill.
>
> **Post-fix hard gates: `make test` ✅ · `make lint` ✅ 0 issues · `TestD6Matrix -race` ✅** (full
> gated d6_integration suite: ladder enforcement / rehome failover / cert pinning / catch-up /
> concurrent-rehome race / per-expose scatter / RTO bound, all under -race + the agent rehome
> NumGoroutine leak gate). The `review:ladder` agent died on a transient API error ("Connection closed
> mid-response") in the first run and was re-run standalone; its report is the §Ladder section.

## Verdicts
| Axis | Verdict |
|---|---|
| build-and-prove boundary + guard + N=1 byte-identity | CONDITIONAL PASS |
| FSM determinism + producer op + migration + DIFF-1 | CONDITIONAL PASS |
| tunnelTokenLookup 2D ladder + epoch lifecycle | (re-run) — see §Ladder |
| concurrency + leaks + agent rehome | CONDITIONAL PASS (1 BLOCKER) |
| cert pinning + wire/security + token non-disclosure | CONDITIONAL PASS |
| completeness + scope + test coverage | CONDITIONAL PASS (2 BLOCKER) |

The implemented **code paths are correct and the build-and-prove spine holds** (production inert,
N=1 byte-identical, L-2 clean, no scope creep). The findings are concentrated in (a) ONE real
concurrency defect (unbounded rehome goroutines under a reconnect storm), (b) guard
defense-in-depth completeness, (c) a large set of PROMISED-BUT-MISSING tests (the agent self-driven
rehome path + the §13.6/§13.7 gates + DIFF-1 de-vacuuming), and (d) small hardening gaps
(empty cert_fp, negative stored epoch, dead RehomeDirective type).

---

## Findings (verbatim digest, by axis)

### A1 — boundary
- **MAJOR (guard completeness).** `d6BannedTokens` (test/d6/regression_test.go:25) is only
  `{AttachClusterSeam(, NewServerWithCert(, LoadServerCert(}`. The seam is two UNEXPORTED fields
  (`b.selfID`, `b.tunnelCert`); a cutover via a direct field write / struct literal / renamed setter
  inside the broker package evades the scan. Verified all 4 evasions pass clean. (D5's M4 lesson —
  ban the type/field, not just the constructor — was not carried forward.) Safe-to-add tokens
  (`.selfID =`, `.tunnelCert =`, `selfID:`, `tunnelCert:`, `resolveHomeForAgent(`, `certPinsFor(`,
  `PlanReassignHome(`, `LookupByNatsServer(`, `HomeDirective{`, `RehomeDirective{`) are provably
  absent from scanned files. Do NOT add `homeForRegister`/`homeForExpose`/`.SelfID(`/`selfNodeID` —
  they appear at gated call sites (false-positive).
- **MINOR.** Guard hard-codes `cmd/tether/serve.go`; should walk all `cmd/tether/*.go`.
- **MINOR.** No live-path (handleExposeReq) `ExposeForwardedReq{Home:nil}` byte assertion; no
  `CertPins` nil-time golden.
- Verified CLEAN: AttachClusterSeam is the only setter; N=1 byte-identity golden-pinned; REGISTER
  6-field epoch=0 line; clientTLSConfigPinned({}) == clientTLSConfig(); state.json omitempty/json:"-".

### A2 — determinism
- **MAJOR M1.** Homed `PlanAllocate` INSERT (plan.go homed branch) is never APPLIED/differentially
  compared — only `strings.Contains` checked. A typo in the second copy of the VALUES list ships green.
- **MAJOR M2.** `nodes.nats_server` DIFF-1 double-write only VACUOUSLY covered — `equiv_test.go:248`
  builds `RegisterInput` with empty NatsServer, so both arms write `''` and the comparison is
  vacuous; the on-conflict `nats_server = excluded.nats_server` path is never compared non-empty.
- **MINOR m1.** `OpPortReassignHome` never applied through the real FSM (harness uses raw `UPDATE`;
  d6_plan_test uses `db.Exec`). Op-through-FSM path (CAS × applied_index) untested.
- **MINOR m2.** `PlanReassignHome` returns `newEpoch` pre-Apply, not reconciled with the CAS
  `RowsAffected`; a lost CAS race returns the new epoch. Inert in D6 (directives read the row, not the
  return; raft prevents 2 committed leaders) — latent footgun for D7 live wiring.
- **MINOR m3.** Migration 0012 `NOT NULL DEFAULT ''` backfill untested (vs 0010's dedicated test).
- **MINOR m4.** `PlanReassignHome`'s `now` param is unused (dead param / unparam smell).
- Verified CLEAR: no time/rand/row_id in the new ops; CAS is a leader-baked `LitInt(cur+1)`; applyMu
  serializes read→plan→apply; op registered in knownOps + defaultAppliers; applied_index same-txn.

### A4 — concurrency
- **BLOCKER B1.** `applyHomeDirectives` (agent.go) spawns one `go applyOneHome` per directive on
  EVERY call, called from both register and `onNATSReconnect`, anchored to the long-lived `runCtx`,
  retrying for up to 2 min. NO per-port in-flight guard → a flapping NATS link spawns N concurrent
  `applyOneHome` for the SAME port, all dialing the lagging home concurrently — unbounded goroutine
  growth + amplified herd (defeating the `home_catching_up` single-loop backpressure of R-17).
- **MAJOR M1.** The agent rehome path (applyHomeDirectives/applyOneHome) has ZERO test coverage —
  the harness drives `tunnel.Client.ApplyHome` directly, bypassing the goroutine/retry/classify/
  persist layer. A flip of the transient classifier would make all of D6 a no-op, all tests green.
- **MINOR m1.** `applyOneHome` dial anchored to tunnel `Client.ctx`, not `runCtx`; an evict doesn't
  interrupt an in-flight 5s dial (bounded ≤5s).
- **MINOR m2.** `ApplyHome` TOCTOU vs a concurrent `Close(port)` resurrects the port for one spurious
  dial (self-correcting: broker denies terminal).
- **MINOR m3.** `homeForExpose` Epoch hard-coded 0; fragile coupling with the `ErrNameTaken`
  precondition (a re-forward of a homed expose at epoch 0 would be discarded by the monotone guard
  while state.json is rewritten — state/transport divergence). Defused today by ErrNameTaken.
- Verified CORRECT + race-clean: value-param threading (R-13), epoch-monotone replace, gen-fencing,
  transport-leak guards, the drain-then-resolve deadlock fix.

### A5 — cert-security
- **MAJOR M1.** The TLS-1.3-resumption rationale (the entire justification for VerifyConnection over
  VerifyPeerCertificate) is exercised by ZERO tests, and `clientTLSConfigPinned` sets no
  `ClientSessionCache` so resumption never happens — a future cache + a "VerifyPeerCertificate looks
  equivalent" refactor would silently bypass pinning, suite stays green.
- **MINOR M2.** `TestD6CertPinRejectsRogue` asserts no data plane, NOT that the REGISTER token never
  hit the wire (the actual non-disclosure property).
- **MINOR M3.** `TestD6NoTokenOrPinOnSysEvents` (OQ-4) never implemented.
- **MINOR M4.** A homed `cluster_nodes` row with empty `cert_fp` → `CertPins{Current:""}` → the
  broker still stamps home_broker + emits a directive → the agent permanently rejects every dial
  (`ErrHomePinsRequired`, fail-closed but silently wedged, only an INFO log).
- **MINOR M5.** `RehomeDirective` is a dead proto type (no publisher/subscriber/rate-limiter); the
  §7.4 leader-push backup is absent. Deferral to D7 is SOUND (the self-driven path is load-bearing
  and implemented) but not bounded at the type definition.
- **MINOR M6.** A corrupted `port_allocations.epoch < 0` makes an honest agent (epoch 0) hit
  `epoch > a.Epoch` → permanent `home_catching_up`. Not reachable via wire/D6 writers (defense-in-depth).
- Verified CORRECT + fail-closed: VerifyConnection (not VerifyPeerCertificate); fail-closed on empty
  peer chain / nil-or-expired ValidUntil; exact fp compare; fp SSOT leaf-DER; ErrHomePinsRequired
  guards every clustered dial BEFORE the token write; directives are agent-only (ACL-backstopped,
  never sys.events); CertPins *time.Time NULL↔nil.

### A6 — completeness
- **BLOCKER B1.** (= A4 M1) Agent self-driven rehome path has NO test coverage.
- **BLOCKER B2.** The NumGoroutine/fd leak gate mandated by §4.3/§4.6 (CLAUDE.md §5 hard requirement
  for tunnel/reconcile/transport surfaces) is ABSENT from every D6 test.
- **MAJOR M1.** ~10 of the §4.3/§4.4 integration gates are missing: `InitialHomeAssign` (the C1/DA-12
  headline), `PerExposeScatter` (§7.5 fan-out), `ExHomeNewHomeOneBind` (the plan's "crux" mid-state),
  `RestoreThenRegister` (OQ-2, finalizer-promised), `RotationWindowAgentRestart`, `CertRestartInvariance`,
  `MassRehomeStorm`, `RehomeTransientRetry`, `NotifyStateConverges`, `CertRotationRePush`.
- **MAJOR M2.** RTO budget (§4.5) is prose only; no wall-time bound asserted.
- **MAJOR M3.** (= A2 M2) nats_server DIFF-1 vacuous.
- **MAJOR M4.** `LookupByTokenHash` SELECT-widening has no legacy-row differential (the plan pinned
  one as mandatory — it gates the production-inertness claim B3).
- **MINOR m1.** RehomeDirective dead type (= A5 M5).
- **MINOR m2.** `TestD6ConcurrentRehomeRace` asserts data flows, not that the winning epoch == max.
- **MINOR m3.** No explicit `ExposeForwardedReq{Home:nil}` byte assertion (golden covers it implicitly).
- **MINOR m4.** `TestD6TokenLookupArgOrder` (§4.1) absent.
- Verified CLEAN: scope discipline (no AddVoter/drain/rotate-cert/cluster-writer leaked); guard + L-2;
  serve.go + live handlers gated on selfID==""; homeForExpose direct UPDATE inert + grandfathered;
  TestD6Matrix correctly shaped.

---

## Ladder (re-run report) — CONDITIONAL PASS, no BLOCKER

Every R-9 arm + the epoch chain (port_allocations.epoch→HomeDirective→clientSession→REGISTER→ladder)
are correct; the inert short-circuit is byte-equivalent and runs before any selfID dependence.
- **MAJOR L-1.** The plan's "EXACTLY ONE BIND across the window / ZERO homes allow during catch-up"
  invariant (d6-plan §0.3 + §4.4) is too strong. If the agent↔A yamux drops for an UNRELATED blip
  BEFORE the rehome directive arrives, the old supervisor `redialWithBackoff`s to **A at the OLD
  epoch** (spawn-time value params); if A has not yet applied the reassign (row still {home=A,
  epoch=N-1}), A ALLOWs → A re-binds the public port. Once B catches up + the rehome lands, two hosts
  transiently double-listen on the same public port. SELF-HEALS (OpenHome(B) cancels the port-P
  supervisor → A's session closes), bounded by directive/apply latency, in-flight already severed.
  The CAS fence (R-7) fences the DB write, not A's live listener. → DOC fix (correct the overclaim);
  the authoritative leader-CloseProxy kill is D7 (leader-push).
- **MAJOR L-2.** The d6_integration harness shares ONE `*sql.DB` between homes A and B, so A's
  non-replicated `homeForExpose` `db.Exec(UPDATE home_broker)` is instantly visible to B —
  `TestD6LadderEnforcedEndToEnd` "proves" B denies the homed row ONLY through the shared handle. In
  real raft (D9) homeForExpose's direct write is leader-local, NOT replicated → a follower sees
  home_broker='' → inert → ALLOW. The INITIAL-home replicated chain is unproven; only PlanAllocate(home)'s
  byte-bake is unit-proven and it is not wired into the harness initial-expose flow. → add a unit test
  proving PlanAllocate(home)→apply-on-2nd-DB→LookupByTokenHash→ladder (selfID=B allows, selfID=A denies);
  document the shared-DB simplification.
- **MINOR L-4 (real bug).** `applyOneHome` persists `UpdatePortHome(name, addr, d.Epoch)` on the
  `ApplyHome` NO-OP path: if a stale lower-epoch directive (P,4) runs after (P,5) already installed,
  ApplyHome(P,4) returns nil (epoch 4 <= sess 5, no-op) and applyOneHome persists epoch 4 → state.json
  downgrades → restart replays epoch 4 → row at 5 → terminal deny. → make UpdatePortHome MONOTONE
  (WHERE new >= stored) AND the B1 dedup (one loop per port, latest epoch) eliminates the race root.
- **MINOR L-1.** `homeForExpose` hardcodes directive `Epoch:0`, relying on the INSERT default; reads
  the row's actual epoch instead (robust to a non-zero row). → fix.
- **MINOR L-2/L-3.** REGISTER positional parse is fail-closed (exact-6 + ParseInt) — add a space-in-sid
  malformed test; `home_catching_up` distinct code is enum-safe (token-hash lookup must succeed first)
  — add an unreachable-without-token test. → tests only.

---

## Adjudication (main process — disposition of every finding)

I ACCEPT essentially all findings. The implemented code paths are correct; the work is (a) a handful
of real correctness/hardening fixes, (b) a large test-coverage backfill the reviewers correctly
flagged as promised-but-missing, and (c) two honest doc corrections.

### Code fixes (ACCEPTED + applied)
1. **C-DEDUP (A4 B1 / A6 B1 + L-4 root).** Per-port in-flight dedup in applyHomeDirectives/applyOneHome:
   exactly ONE retry loop per public port, latest epoch supersedes, goroutines bounded by #ports (not
   #reconnects). Eliminates the storm + the concurrent-same-port dials.
2. **C-PERSIST-MONO (L-4).** `stateStore.UpdatePortHome` is monotone (only rewrites when new epoch >=
   stored) so a no-op/stale rehome can never downgrade the persisted epoch.
3. **C-EMPTY-FP (A5 M4).** resolveHomeForAgent / homeForRegister treat an empty `cert_fp` as
   INELIGIBLE (emit no directive → un-homed, retried next reconnect) — never a permanently un-dialable
   directive.
4. **C-NEG-EPOCH (A5 M6).** tunnelTokenLookup treats a stored `a.Epoch < 0` as terminal
   token_unknown_or_revoked (deterministic), not an infinite home_catching_up.
5. **C-HOME-EPOCH (L-1).** homeForExpose READS the row's actual epoch into the directive instead of
   hardcoding 0.
6. **C-DROP-NOW (A2 m4).** Drop the unused `now` param from PlanReassignHome.
7. **C-GUARD (A1 MAJOR + MINOR).** Extend d6BannedTokens with the field-write/struct-field/symbol
   tokens (provably absent from scanned files) + scan all cmd/tether/*.go + self-check cases.
8. **C-DOC-NOTES (A4 m3 / A5 M5 / A2 m2 / A4 m1).** Comments: RehomeDirective NOT-WIRED (D7);
   homeForExpose epoch-0 ↔ ErrNameTaken precondition; PlanReassignHome return-epoch is pre-Apply
   (directives read the row); applyOneHome dial ctx domain.

### Doc fixes (ACCEPTED)
9. **D-ONEBIND (L-1).** Correct the "EXACTLY ONE BIND / FORBIDDEN active-active" overclaim →
   "no STEADY-STATE double-bind; a bounded cutover window may transiently double-listen (in-flight
   severed), self-healing once OpenHome(newHome) cancels the ex-home supervisor; the authoritative
   ex-home listener kill is the D7 leader-push."
10. **D-SHAREDDB (L-2).** Document the shared-DB harness simplification in setup_test.go.

### Tests added (ACCEPTED — reviewers may suggest, only main process writes)
- Agent rehome unit (A4 M1/A6 B1): transient-retry-converge + UpdatePortHome persist; terminal-stop;
  ErrHomePinsRequired defer; catch_up_stalled deadline; **dedup goroutine bound + NumGoroutine/fd leak
  gate under a reconnect storm** (A6 B2); ApplyHome-no-op-does-not-persist-lower (L-4).
- Integration gates (A6 M1): InitialHomeAssign (C1); PerExposeScatter (§7.5); ExHomeNewHomeOneBind +
  ExHomeReBindDuringBlip (L-1 bounded-window self-heal); RestoreThenRegister (OQ-2);
  RotationWindowAgentRestart + CertRestartInvariance; RTO wall-time bound in KillHomeRehome (A6 M2).
- Determinism (A2 M1/M2/m1/m3 + L-2): homed PlanAllocate apply+differential on a 2nd DB →
  LookupByTokenHash → ladder (selfID=B allows / A denies) [closes M1 + L-2]; non-empty nats_server
  DIFF-1; OpPortReassignHome through the real FSM; migration 0012 backfill.
- Boundary/cert (A1 + A5): LookupByTokenHash legacy-row differential (M4); TLS-1.3-resumption-still-
  verifies + no-ClientSessionCache assertion (A5 M1); rogue-never-receives-token (A5 M2);
  no-secrets-on-member-channels under rehome (A5 M3); empty-cert_fp-yields-no-directive (A5 M4);
  negative-stored-epoch-deterministic (A5 M6); guard field-write self-check (A1).
- Cheap (A6 m2/m3/m4 + L-2/L-3): race epoch-winner == max; ExposeForwardedReq{Home:nil} byte assert;
  register-line space-in-sid malformed; home_catching_up unreachable without token.

### Noted, NOT fixed (bounded / D7 / covered-by-existing)
- A2 m2 (PlanReassignHome return-epoch vs CAS): inert in D6 (directives read the post-Apply row, raft
  prevents two committed leaders) — documented as a D7 live-wiring note (C-DOC-NOTES).
- A4 m1 (applyOneHome dial ctx vs runCtx): bounded ≤5s self-correcting — documented.
- L-1 authoritative ex-home kill: D7 leader-push (out of D6 scope) — the bounded window is documented
  (D-ONEBIND); the SELF-HEAL mechanism (OpenHome cancels the port supervisor) is covered by
  TestD6RehomeAtoB; a dedicated ExHomeReBindDuringBlip integration test (forcibly tearing the
  agent↔ex-home transport without a directive) is intricate to construct and deferred — the doc
  correction is the substantive deliverable.
- L-2/L-3 (register-line space-in-sid, catch_up-unreachable-without-token): the parser is fail-closed
  by the EXACT-6 check (TestD6RegisterLineParse covers 5/7-field + malformed → reject; a space in sid
  yields >6 parts → same reject path), and home_catching_up is reachable only AFTER a successful
  token-hash lookup (the F6 collapse for the unauthenticated probe is intact) — covered by existing
  tests; no new case added.
- A6 m2 (race epoch-winner==max assertion): the OpenHome epoch-monotone replace guard is unit-logic
  tested; a tunnel-internal session-epoch accessor for the integration assert was not added (low value).
- OQ-2 RestoreThenRegister: the epoch-as-local-barrier sufficiency rests on D1's snapshot↔applied_index
  co-consistency (a D1 invariant) + the same-FSM-txn write — proven at those layers; a snapshot-restore
  integration case is deferred (needs the raft snapshot path; the foundation is proven).

### Final gate status
`make test` ✅ all green · `make lint` ✅ 0 issues · `TestD6Matrix -race` ✅ (full gated suite) ·
full-phase `make e2e` matrix green (the only intermittent is the pre-existing D5 JS-cluster-bootstrap
flake, orthogonal to D6, passes on clean retry). Internal review PASS.

### External review round-1 (FAIL → all 4 fixed)
The user's external review (`d6-external-review.md`) found 4 real BLOCKER/High breaks the internal
review missed — all in the end-to-end rehome path, not test-only. All ACCEPTED + FIXED + the reviewer
regressions are now green:
- **F1** `handleRegister` never copied `req.ServerID` into `node.RegisterInput.NatsServer` → the
  replicated home bridge was dead (node-level DIFF-1 tests bypassed the broker handler). Fixed +
  broker-level regression kept.
- **F2** deferred clustered boot replay never reopened when pins arrived (`ApplyHome` no-ops a
  non-open port). Fixed: agent tracks `deferredReplay` ports + `openHomeFromState` (AddProxy→OpenHome
  from the persisted token), discriminating deferred-open from a live-session rehome.
- **F3** a stale terminal rehome dropped a newer-epoch directive queued for the same port (the dedup
  defer deleted `rehomeWant` unconditionally). Fixed: terminal/deadline exits re-check `hasNewerWant`.
- **F4** same-epoch pure-pin cert rotation was treated as a stale no-op by `ApplyHome`. Fixed:
  `ApplyHome` updates `sess.certPins` in place at the same epoch (no transport tear); `redialWithBackoff`
  reads the session's current pins under c.mu (gen-fenced) so the running supervisor picks up rotations.
### External review round-2 (FAIL → 2 fixed)
The re-review found 2 gaps the round-1 fixes introduced; both ACCEPTED + FIXED, regressions green:
- **RF1** deferred replay treated an unreadable/unparseable state.json as a successful open (clearing the
  deferred marker without opening anything → lost expose). Fixed: `openHomeFromState` is now tri-state
  (`openedOK` / `openStateUnavailable` → keep deferred + give up until next reconnect, no AddProxy /
  `openPortAbsent` → clear deferred).
- **RF2** a same-epoch pure-pin (cert rotation) directive queued behind an in-flight same-port loop was dropped
  by the `epoch>`-only exit check. Fixed: a per-port `rehomeSeq` bumped on every want update (incl. same-epoch
  pin change); the loop re-applies whenever the seq changed (`wantChanged`), unifying F3 (higher epoch) + RF2.

All 6 external-review regressions (F1–F4 + RF1 + RF2) green; `make test` exit 0 · `make lint` 0 ·
`TestD6Matrix -race` ✅ · gated `d6_integration -race` ✅. **Awaiting external re-review; not committed.**
