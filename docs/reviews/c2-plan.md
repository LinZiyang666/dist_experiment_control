# C2 Plan (FINAL) — Agent join: bootstrap URL + signed invite + HTTP well-known manifest

> Stage-A output. 9-agent adversarial workflow (4 lens drafters → 4 critics → 1 synthesizer; full raw in `tasks/w0i76d08c.output`). Main process is sole finalizer. Build-and-prove: non-cluster broker byte-equivalent; ProtoVersion stays 2; `ClusterRosterSchemaVersion` stays 1 (roster canon UNTOUCHED).

## 0. Finalization decisions (main process — resolves §7 open questions)

| # | Question | DECISION |
|---|---|---|
| 1 | Internal-route exposure (roster carries `nats_route`/`tunnel_addr`) vs reduced public projection | **Accept + DOCUMENT the recon delta for v1** (routes are mTLS-protected, addresses are not secrets; agent dials `public_host`, never `nats_route`). Reduced-projection (omit route/tunnel) is a clean post-C2 hardening — deferred. Grep-guard test asserts NO actual secret in the served body. |
| 2 | Manifest cache: lazy-coalesce vs background ticker | **Lazy rate-limited rebuild** (no dedicated goroutine; GET serves an `atomic.Pointer`) — fewer goroutines, inert non-cluster. |
| 3 | `roster_cache.json` cross-process write | **Self-heal sufficient for v1** (monotone-gate via `AdoptDecision` + atomic tmp+fsync+rename + continuous NATS refresh). The load-bearing fix — splitting the cache OUT of daemon-owned `state.json` so expose tokens can't be lost — IS done. `flock` noted as optional hardening. |
| 4 | invite inline `seed=` | **OPTIONAL** (robustness floor); verified SeedBundle is the authoritative source; `agent join` FAILS LOUD + persists nothing when it can derive no dialable client URL. |
| 5 | `agent join --start` lifecycle | **In-process foreground** (PIN in memory, never in argv). Service-install (`systemctl --user enable --now`) printed when `--start` omitted; documented production path. |
| 6 | `cluster init --from-existing` pre-seeding seeds | **DEFER** (a second offline writer); online `cluster seeds publish` is canonical. |

**Verification obligations carried into Stage-B** (verify first; fix plan if false): (a) `agent.New` requires non-empty `cfg.NATSURL` (`agent.go:445`) → the "pure-invite empty-NATSURL" branch is dead code → DialURLs/effectiveDialURLs stay BYTE-UNCHANGED; (b) PIN is presented as `nats.Token` on every CONNECT before auth_callout adjudicates → an unsigned HTTP-sourced dial target = PIN-harvest, so the dial target MUST be authenticated; (c) `CanonicalRosterBytes` unchanged (no new signed roster field).

## 1. Objective — gap rows + success metric #3

C2 = the ENTRY/join side of 建议3. Closes gap rows: broker_url→bootstrap (56), `cluster seeds publish` (57), `agent join` (58), `agent config refresh` (59), `agent doctor` (60), HTTP well-known manifest (61), bootstrap+seed-cache startup-order (64), 验收 new-agent-only-invite (67). Closes **success metric #3** (row 128).

**建议3 acceptance (verbatim):** 「新 agent 只拿 invite 即可入群，不需要手写逗号分隔的 broker list。」「agent/ctl 保存 bootstrap URL 和最近一次有效 seed cache。」「agent 启动顺序：cached seeds → bootstrap URL → install-time fallback。」 Principle L21: 「只接受签名 roster 和单调递增 generation。」

C1 shipped the consumer authority (`adoptRoster`, `VerifyAt`/`DialURLs`, monotone `roster_generation`). **C2 funnels new entry points (boot bootstrap, HTTP manifest, `agent join`, `config refresh`) through ONE authority — never a second verify/adopt site — plus the client-dialable seed endpoints a brand-new agent needs before it has any NATS template.**

## 2. Mechanism (decisions settled)

**2.1 The load-bearing problem:** `DialURLs` templates each broker's `PublicHost` onto the agent's OWN `cfg.NATSURL` scheme/port; `NatsRoute` is the un-dialable route `:6222`. A brand-new agent with only an HTTPS bootstrap URL has NO NATS template → C2 must ship **explicit, authenticated client-dialable endpoints** (the `SeedBundle`).

**2.2 Three signed objects + one OOB token:**
- `ClusterRoster` (C1, UNCHANGED) — account-signed, `roster_generation`, broker set.
- **`SeedBundle`** (NEW) — account-signed (`CanonicalSeedBytes`), **`seed_generation`**, client-dialable endpoints + bootstrap URL.
- **`ClusterManifest`** (NEW) — `{Roster, Seeds}` envelope, both children independently account-signed.
- **invite** (NEW) — UNSIGNED, OOB-delivered: `tether-invite:v1?pin=<account_pub>&url=<bootstrap_https>&sid=<sid>[&seed=<client_nats_url>]`.

**2.3 Settled decisions:**
- **D-1 Manifest host:** dedicated **loopback** listener, Caddy-fronted. New leaf `internal/clustermanifest` (mirrors `brokermetrics` Bind/ServeListener/Handler). Flag `--cluster-manifest-listen`, yaml `broker.cluster.manifest_listen`. `Bind` FORCES loopback (unauthenticated + Caddy-fronted). NOT the metrics mux.
- **D-2 Listener gating:** bind only when `b.selfID != "" && addr != ""` → non-cluster binds NOTHING (byte-equiv even if flag set).
- **D-3 Manifest body:** `{roster (signed), seeds (signed)}` — both account-signed, both verified against the pinned account_pub. (Rejects unsigned envelope BLOCKER.)
- **D-4 Manifest cache:** time-bucketed — serve pre-signed bytes from `atomic.Pointer`; guarded rebuild rate-limited (≥30s between attempts), re-sign only when `(roster_generation, seed_generation)` advanced OR ≥`rosterTTL/4` (6h) since last sign. GET = atomic load + write, no DB/sign. (Rejects per-GET-sign DoS AND generation-only-cache staleness wedge.)
- **D-5 seed_generation:** dedicated `seed_generation` cluster_meta counter, bumped by `cluster seeds publish`. SeedBundle carries it; **agent persists a `SeedGen` hwm and rejects `seeds.Generation < SeedGen` in the SAME adopt funnel**. Dedicated (not reuse roster_generation) so a seeds-publish does NOT pollute the topology-lag signal `rostergen.go` keeps clean, and endpoint rotation gets its own anti-rollback.
- **D-6 Invite trust anchor:** account_pub is THE anchor (pre-seeds `Config.AccountPub` → closes C1 TOFU). A self-signed invite buys nothing (attacker signs with their own key; a victim with no prior pin can't tell) — integrity = OOB channel, parity with handing `broker_url+account_pub` today, strictly better than C1 TOFU. ONE format; never sent to any HTTP server. **NO PIN field, NO seed material.**
- **D-7 Cold-start dial floor:** invite inline `seed=` (OOB-trusted) AND/OR verified SeedBundle endpoints (authenticated vs pin) → written into `agent.yaml broker_url` (= `cfg.NATSURL`, the C1 permanent floor). Never an unsigned HTTP field.
- **D-8 Bump policy:** `cluster seeds publish` bumps ONLY `seed_generation`, not `roster_generation`. Seeds are bootstrap/cold-start; steady-state freshness is the roster's `public_host` templating over NATS (metric #2). Documented.

**2.4 Startup order (composed with C1):** (1) cached — `loadRosterCacheAtBoot` re-`VerifyAt`s the cached roster AND re-`VerifySeedsAt`s the cached SeedBundle vs pin; (2) bootstrap — if cold/expired AND `cfg.BootstrapURL != ""`, fire **ONE async best-effort `FetchManifest`→`adoptManifest` goroutine** (never blocks `Run`); (3) fallback — `effectiveDialURLs` (BYTE-UNCHANGED) unions `cfg.NATSURL` as permanent floor. Steady-state stays C1's `rosterRefreshLoop`.

**2.5 The single adopt authority:** extract the decision table into ONE pure exported helper `AdoptDecision(prev RosterState, r *ClusterRoster, s *SeedBundle, pin string, now) (next RosterState, accepted bool)` over `RosterState{Pin; RosterGen, SeedGen; DialURLs, SeedURLs; Roster, Seeds}`. Encodes rows 2–9 for roster AND mirrored sig/account/schema/expiry/gen-rollback for seeds; empty-pool → keep prior. BOTH the daemon (`adoptManifest`, holds `a.rosterMu`, persists) and the CLI (`config refresh`/`join`/`doctor`) call it — never a fake `Agent`, never a second enforcement site.

## 3. Security posture (security-pragmatic v1; honest residuals)

- **No private key ever leaves the broker (拒绝表 #2).** Manifest = public roster + public SeedBundle (account *pub*, public hosts, cert_fp, public endpoint URLs, sigs). Invite = pin/url/sid/seed only. Grep-guard test asserts no seed/nkey/CA/session-token substrings.
- **PIN-harvest BLOCKER folded:** the agent presents the session PIN as `nats.Token` on EVERY CONNECT before auth_callout adjudicates → an unsigned HTTP-sourced dial target lets a MITM (valid CA cert for their host) steer a fresh agent's first dial to attacker NATS = PIN harvest → nkey hijack. **Dial target MUST be authenticated** (account-signed SeedBundle vs pin, OR OOB invite `seed=`).
- **PIN-in-invite BLOCKER folded:** the session join-PIN is a bearer credential, not discovery data. `--pin` stays a separate ephemeral flag, in-memory only, never persisted (parity with `install.sh`). Acceptance #3 still fully met.
- **Account signature is the anchor, not TLS.** `AdoptDecision` enforces (必须拒绝 #3): roster/seed sig-fail → reject; account≠pin → reject; schema>supported → reject; `ExpiresAt` passed → reject-for-adoption (keep prior); `Generation < hwm` (roster OR seed) → reject. Every reject keeps prior good state.
- **TOFU only via the full invite.** `config refresh --once` REFUSES to create a first-ever pin from HTTP (only verifies vs an already-persisted pin); a new pin is established ONLY by `agent join` with `pin=`.
- **Bare-bootstrap loud-fail:** `agent join` FAILS LOUDLY + persists nothing when it cannot derive ≥1 dialable client URL.
- **Internal-route exposure:** ACCEPTED + DOCUMENTED (§0 #1). Real invariant (no secrets) grep-guard-tested.
- **SSRF/scheme hardening:** ONE hardened `FetchManifest` — http/https-only, bounded timeout, `io.LimitReader` (~1 MiB), capped same-scheme redirects, proxy-aware.
- **Defense-in-depth:** `agent join` prints the account_pub it pins LOUDLY; optional `--expect-account-pub` hard-fails on mismatch; `pin` validated via `nkeys.IsValidPublicAccountKey`. No `--force-rollback`/`--repin` escape hatches.

## 4. File-level change list

**proto** (additive; ProtoVersion=2, ClusterRosterSchemaVersion=1 UNCHANGED): `internal/proto/cluster_roster.go` append `SeedBundle{SchemaVersion,Generation,AccountPub,Endpoints[],BootstrapURL,IssuedAt,ExpiresAt,Sig}`, `ClusterManifest{SchemaVersion,Roster,Seeds}`, `SeedBundleSchemaVersion=1`, `ClusterManifestSchemaVersion=1`. No `ClusterRoster`/`CanonicalRosterBytes` change.

**clusterroster** (pure leaf): `seeds.go` (`CanonicalSeedBytes`, `BuildSeeds`, `VerifySeedsAt` — mirror `VerifyAt`), `invite.go` (`Invite{Pin,BootstrapURL,SID,Seed}`, `MintInvite`, `ParseInvite` strict `tether-invite:v1`), `fetch.go` (`FetchManifest` hardened; gains `net/http`, no raft/nats).

**cluster**: `seeds.go` (`metaKeySeed*`, `seedGenBumpStmt`, `PlanClusterSeedsPublish` leader-side URL validation + variadic bake, `Seeds(ro)` + `readMetaTextDB`), `command.go` `OpClusterSeedsPublish` + isWriteOp, `clustermeta.go` register applier=`genericExecApplier`.

**broker**: `cluster_manifest.go` (`buildSeedBundle`, `buildManifest`, `manifestBytes` D-4 cache; reuse `buildSignedRoster` verbatim), `broker.go` `Config.ManifestAddr` + Run gated bind, `clusteradmin.go` `PublishSeeds`/`ReadSeeds`, `clusterstatus.go HandleCluster` `seeds_show`/`seeds_publish`.

**clustermanifest** (NEW leaf, loopback-forced): `manifest.go` `Bind`(requireLoopback)/`ServeListener`/`Handler` — GET `/.well-known/tether/cluster.json` only (405/404/503; `recover()`; `ReadHeaderTimeout`). No DB/NATS/seed.

**adminsock**: `protocol.go` `OpClusterSeedsPublish`(+`Show`) + allow-list; `Request{Bootstrap,Endpoints[]}`, `Response{AccountPub,Generation,Endpoints[],Bootstrap}` (omitempty; AccountPub so CLI mints full invite).

**agent**: `roster.go` `Config.BootstrapURL`, `AdoptDecision` pure helper, refactor `adoptRoster`→`adoptManifest` via `AdoptDecision`, extend `loadRosterCacheAtBoot` to re-verify cached SeedBundle, async boot bootstrap tier in `Run`/`session`. **effectiveDialURLs/DialURLs BYTE-UNCHANGED.** `state.go` `RosterCache` += `SeedGen`+`Seeds *SeedBundle`; **move cache to its own `roster_cache.json`** with exported `ReadRosterCache`/`WriteRosterCache`; `state.json` keeps port-tokens+proxy only (its C1 `Roster` field → one-time migration-read). Decouples CLI refresh writer from daemon port-token writer (no lost-update).

**cmd/tether**: `agent.go` `agentYAML` += `AccountPub`/`BootstrapURL` (thread to cfg); `newAgentCmd` becomes parent-with-RunE + 3 children; extract `runAgentDaemon`. `agent_join.go` (parse→require-dialable-or-fail-loud→`WriteAgentConfig` typed rewrite 0600→`FetchManifest`+adopt pre-warm→print pinned pub→`--start` in-process). `agent_doctor.go` (read-only checks: config/identity[read-only, never mint]/seed_cache/manifest_verifies[gen-rollback FATAL]/tcp_reachable[advisory]/clock[advisory]/proxy_env; exit 77 security-FATAL; no side effects). `cluster_seeds.go` (`seeds publish --bootstrap --endpoint...`→admin; prints seed_generation + Caddy snippet + ready-to-paste invite via MintInvite full account_pub; `seeds show`). `serve.go` `--cluster-manifest-listen`→`Config.ManifestAddr`.

**install.sh / docs**: Caddy `handle /.well-known/tether/*` route + `manifest_listen`; usage.md the flow + OOB-trust + route-exposure + no-binary-downgrade notes.

## 5. Wire / byte-equivalence + non-cluster inertness

ProtoVersion=2; ClusterRosterSchemaVersion=1; `CanonicalRosterBytes` byte-identical (regression guard). New proto/yaml/adminsock fields additive+omitempty. Manifest listener bound only `selfID != "" && ManifestAddr != ""` → non-cluster starts NO listener/goroutine (NumGoroutine/fd leak gate). Register reply unchanged; SeedBundle NEVER on the register reply (only HTTP). `cluster seeds publish` inert single mode. Pre-C2 agent.yaml/state.json round-trip ("" account_pub→TOFU; `state.json.Roster`→`roster_cache.json` migration). Guard test (`TestD*ProductionWiresNoCluster` style) asserts non-cluster path builds no manifest listener / no SeedBundle.

## 6. Adversarial tests (named — see synth §6 for the full ~50-test list)

Key load-bearing tests: `TestParseInviteRejectsNonAccountPub`/`RejectsExtraParams`/`HasNoPinOrSeedMaterial` (grep); `TestCanonicalRosterBytesByteIdenticalToPreC2`; `TestFetchManifestRejectsFileScheme`/`CapsBodySize`; `TestManifestGETOnly`/`RejectsNonLoopbackBind`/`SignsOncePerCacheTick`(-race DoS)/`StaysValidPastRosterTTLWhenStable`/`NoSecrets`/`NonClusterNoListener`; `TestSeedGenStrictlyMonotone`/`SeedsPublishDoesNotBumpRosterGen`(D-8)/`SeedsPublishMultiFSMDeterministic`; `TestManifestMITMAccountSwap`(reject vs OOB pin); `TestAdoptRejectsForgedRoster`/`ForgedSeed`/`SeedGenRollback`/`ExpiredManifest`; `TestBootFetchIsAsyncNonBlocking`; `TestSeedCacheReVerifiedOnBoot`; `TestRosterCacheSeparateFromStateJSON`/`StateJSONRosterMigration`/`ConcurrentDaemonCLICacheWriteMonotone`(-race); `TestJoinFailsLoudWithoutDialableURL`/`ForgedManifestRejected`(exit77)/`StartInProcessNoPINInArgv`/`ConfigRefreshRefusesFirstPinFromHTTP`; `TestDoctorReadOnlyNoKeyMint`/`GenRollbackFATAL`. Dev: `go test ./internal/agent/ ./internal/broker/ ./cmd/tether/ ./internal/clusterroster/ ./internal/cluster/ ./internal/adminsock/ ./internal/clustermanifest/` + `-race` on bootstrap/join-start/cache-split.

## 7. Risks (all §7 open Qs decided in §0)

The plan is large (10 packages, 4 new CLI commands, 1 new leaf package, ~50 tests). Implement leaf→up, compile per package. The highest-risk pieces: the `AdoptDecision` refactor (must keep C1 behavior byte-identical for the roster path) and the `state.json`→`roster_cache.json` cache split + migration (must not break C1's restart anti-rollback). Both have dedicated regression tests.
