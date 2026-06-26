# C2 Stage-C Review + Adjudication — Agent join: bootstrap URL + signed invite

> Stage-C adversarial internal review (5-agent workflow: security / concurrency / requirements / wire-determinism → synthesizer). Raw: workflow `wstjmq9cv`. Verdict: **CONDITIONAL — NO BLOCKERS**. Synth independently re-traced every HARD INVARIANT in source and confirmed they HOLD. This file records each finding + the main process's disposition. **All accepted fixes landed; gates green. External review deferred to end of the C-program (per /goal).**

## Invariants the review independently CONFIRMED in source

- **No secret leak (拒绝表 #2):** invite + manifest carry only public fields (account *pub*, public hosts, signed URLs); the account seed is used solely by `SignWithSeed`. Grep-guarded.
- **Authenticated-first-dial (PIN-harvest defense):** `agent_join.go` derives `broker_url` ONLY from the OOB `inv.Seed` OR a `VerifySeedsAt`-verified SeedBundle, else FAILS LOUD + persists nothing.
- **No-TOFU-from-HTTP:** `agent config refresh` refuses to create a first pin from HTTP; `bootstrapFetchOnce` is a no-op without a pin.
- **Single authority:** `AdoptDecision` is the one verify/adopt site (daemon + all 3 CLIs funnel through it).
- **Cache split:** `roster_cache.json` is separate from daemon-owned `state.json` (no expose-token lost-update).
- **Byte-equivalence:** `CanonicalRosterBytes` byte-identical (pinned test); ProtoVersion 2 / ClusterRosterSchemaVersion 1; manifest listener inert non-cluster.
- **D-8:** `seed_generation` monotone (`MAX(+1)`), and a seeds-publish does NOT bump `roster_generation` (FSM test).

## Findings & disposition

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| **M1** | MAJOR | `a.seedURLs` was runtime write-only → the SeedBundle's "client-dialable" / endpoint-rotation tier was dead state (lip-service vs plan §2.1/D-5). | **ACCEPTED + FIXED (option b):** `effectiveDialURLs` now unions the verified `seedURLs` as a tier (roster → seed endpoints → cfg.NATSURL floor). Additive — empty for a non-cluster agent → byte-equivalence preserved; every seed endpoint was `VerifySeedsAt`-verified, so the tier is authenticated. Test `TestEffectiveDialURLsIncludesVerifiedSeeds`. |
| **m1** | MINOR | `agent doctor` exited 70, not the plan's 77 (security class). | **FIXED:** RunE returns `&ExitError{Class: exitNoPerm}` on a FATAL → exit 77. |
| **m2** | MINOR | doctor flagged only roster-gen rollback, not seed-gen rollback. | **FIXED:** added the symmetric `m.Seeds.Generation < rc.SeedGen → FATAL` check. |
| **m3** | MINOR | broker `validateSeedEndpoint` accepted plaintext `ws://` unconditionally (contradicting its own error string; the first CONNECT carries the PIN in cleartext). | **FIXED:** dropped `ws` from the publish allowlist (now nats/tls/wss only). |
| **m4** | MINOR | no-secret grep guards missed the account-seed `SA` prefix + didn't grep the literal seed bytes. | **FIXED:** `TestManifestNoSecrets` now asserts the body never contains `string(seed)` + greps SU*/SA* prefixes. |
| **m5** | MINOR | `FetchManifest` has no private/loopback/link-local SSRF block. | **ACCEPTED RESIDUAL (documented):** under the OOB-trust model a malicious invite already owns the pin; scheme allowlist + timeout + LimitReader + redirect cap are present. Optional `DialContext` Control-hook hardening deferred (security-pragmatic v1). |
| **m6** | MINOR | `agentYAML.AccountPub`/`BootstrapURL` lacked `,omitempty`. | **FIXED:** added `,omitempty` (a non-cluster agent.yaml emits no empty lines). |
| **m7** | MINOR | gap-row wording "broker_url downgraded to a bootstrap URL" vs the implemented "broker_url carries an authenticated NATS seed; bootstrap_url is additive". | **DOC-ONLY (noted):** D-7 is sound (the dial floor must be authenticated). The gap row's intent — "no hand-written broker list" — is met. No code change. |

## Tests added (T1 — the dominant gap: the CLI IS the acceptance surface)

`cmd/tether/agent_cli_test.go` (httptest-served signed manifest, hermetic `TETHER_HOME`):
- `TestAgentJoinFailsLoudWithoutDialableURL` — forged (foreign-signed) seeds + no inline seed → join errors AND writes NOTHING.
- `TestAgentJoinValidWritesConfigAndCache` — `broker_url` == the VERIFIED endpoint; account_pub pinned; `roster_cache.json` pre-warmed.
- `TestAgentJoinNeverPersistsPIN` — `agent.yaml` never contains the `--pin` value.
- `TestAgentConfigRefreshRefusesFirstPinFromHTTP` — no persisted pin → "refusing to TOFU-pin" error, cache unchanged.
- `TestAgentConfigRefreshHappyPath` — matching pin → cache updated.
- `TestAgentDoctorForgedManifestFATAL` — foreign-signed manifest → manifest FATAL.
- `TestAgentDoctorReadOnlyNeverMintsKey` — doctor never mints an identity key.

Plus: `TestEffectiveDialURLsIncludesVerifiedSeeds` (agent, M1); the existing clusterroster/cluster/clustermanifest/broker C2 suites (invite no-secret grep, seed canon, FSM seed-gen monotone + no-roster-bump, manifest GET-only/loopback/503/MITM-swap/no-secrets).

## Gate status (post-fix)

`go build ./...` ✅ · all touched packages ✅ · `internal/agent -race` ✅ · `internal/broker -race` (incl. the D8/D9 production-wiring guards — still hold with the C2 manifest/seeds additions) ✅ · `go vet ./...` ✅ · `golangci-lint` ✅ 0 issues.

## Known follow-ups (non-blocking)

- install.sh Caddy `handle /.well-known/tether/*` route + `manifest_listen` in broker.yaml; usage.md `seeds publish → invite → agent join` flow + OOB-trust + internal-route-exposure notes. (Non-code; tracked.)
- `FetchManifest` SSRF private-IP block (m5 residual).
- `cluster init --from-existing` seed pre-seeding (plan §0 #6 deferral).

**C2 internal review CLOSED. Code staged. Next: spawn 监工 #1 (independent C1–C2 requirements review), then C3.**
