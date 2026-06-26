# 监工 #1 — Independent requirements audit of C1 + C2

> Spawned after C2 per the /goal: an INDEPENDENT overseer agent (not the Stage-C reviewers, not the main process) verifies whether C1 + C2 TRULY deliver the requirements they claim — hunting for 阳奉阴违 (lip-service: claimed-done that the code doesn't deliver, vacuous tests, stubs presented as real, doc claims contradicted by source). Method: read every impl+test file, traced production wiring, ran `go test ./internal/agent/ ./internal/broker/ ./internal/cluster/ ./internal/clusterroster/ ./internal/clustermanifest/ ./cmd/tether/ -count=1` (all 6 PASS). No code modified.

## OVERALL VERDICT: **PASS — C1 + C2 honestly deliver 建议3's consumer + entry sides and success metrics #2 / #3 / #7. No lip-service found.**

Every requirement maps to real code + a test that asserts the behavior (not a vacuous assertion). The single substantive limitation — no *direct* two-broker cross-port live-failover e2e — is **honestly disclosed** in `docs/reviews/c1-review.md` (not papered over), and the failover mechanism is otherwise proven (rebuild loop + exactly-once forwarded dispatch + dial-pool ordering). The overseer "could not find anything the non-independent Stage-C reviews rationalized as done that the code does not actually do."

## Item-by-item (REAL / PARTIAL / LIP-SERVICE)

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | consumer rejects sig/account/gen-rollback/expired/schema, keeps prior; pin is persisted/OOB not self-claimed | **REAL** | `roster.go AdoptDecision`; `TestAdoptRejectAccountMismatch` proves the PIN (not `r.AccountPub`) is used; `TestRosterRestartAntiRollback` proves persistence |
| 2 | roster_generation monotone + change-gated + floored | **REAL** | `rostergen.go MAX(+1) WHERE changes()>0`; `TestRosterGenMonotoneAndChangeGated`, `…NeverRegressesOnRetireOfNewest`, `…RollingUpgradeFloor` |
| 3 | ≤5min refresh, no raft write | **REAL** | `broker.go:1047` RosterRefreshOnly short-circuits before registerNode/reconcile; `TestRosterRefreshConvergesNoRaft` |
| 4 | L3 live-failover real; single-sub proven; cross-broker e2e honestly disclosed | **REAL + disclosure HONEST** | `fireRedial`+rebuild loop (runCtx published before connect — MAJOR-1 fix); `TestRebuildNoDoubleForwardedDispatch` (exactly-once); cross-port e2e genuinely un-templatable under D-3, correctly deferred |
| 5 | `agent join` works from only invite; broker_url authenticated; PIN never persisted | **REAL** | `agent_join.go:59-69` authenticated brokerURL or fail-loud-persist-nothing; no PIN field in agentYAML; `TestAgentJoin*` (fail-loud / valid / never-persists-PIN) |
| 6 | `config refresh --once` refuses first pin from HTTP | **REAL** | `agent_config.go:47-53`; `TestAgentConfigRefreshRefusesFirstPinFromHTTP` |
| 7 | `agent doctor` read-only | **REAL** (nuance: does an advisory TCP DialTimeout probe — no write/mint/NATS-session) | `agent_doctor.go`; `TestAgentDoctorReadOnlyNeverMintsKey`, `…ForgedManifestFATAL` |
| 8 | well-known manifest served + signed + inert non-cluster | **REAL** | `clustermanifest`; `broker.go:702` gated `selfID!="" && ManifestAddr!=""`; `TestManifest*` incl. MITM-swap reject |
| 9 | seeds-publish bumps ONLY seed_generation (D-8) | **REAL** | `seeds.go:106`; `TestSeedGenMonotoneAndNoRosterBump` |
| 10 | 必须拒绝 still refused (no key/seed copied; no HTTP TOFU) | **REAL** | invite/manifest carry only public fields; `TestInviteHasNoPinOrSeedMaterial`, `TestManifestNoSecrets` (greps raw seed bytes) |
| 11 | metrics #2 / #7 / #3 | **REAL** (with the #4 disclosed-e2e caveat on #2) | as above |
| 12 | dead state / write-only field | **REAL — the C2-review M1 catch (`seedURLs`) is now genuinely live** | `effectiveDialURLs` unions it; `TestEffectiveDialURLsIncludesVerifiedSeeds`; no remaining dead state, no stub/TODO/`not implemented` |

## Non-blocking caveats (doc-only / already tracked — NOT delivery gaps)

1. **gap-doc bookkeeping not yet flipped.** `docs/v2-usability-proposals-gap.md` still shows ❌ for 建议3 rows (a pre-C1/C2 snapshot). DoD requires flipping them ✅ at closure → **flip at the C-program external review.**
2. **"broker_url 降级为 bootstrap URL" wording** — implementation keeps broker_url as an authenticated NATS dial floor + adds bootstrap_url additively (D-7 sound; intent met). Doc-only (c2-review m7).
3. **Documented residuals** (c2-review "Known follow-ups", non-code): install.sh Caddy `/.well-known/tether/*` route, usage.md flow, `FetchManifest` SSRF private-IP block.

## Main-process disposition

**No code fixes required** — the overseer found zero lip-service. The 3 caveats are doc-only / already-tracked follow-ups (gap-row flip + usage.md land at the C-program external review). C1 + C2 stand. Proceeding to C3.
