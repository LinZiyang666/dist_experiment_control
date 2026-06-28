# ctl/cli cluster broker auto-failover — finalized plan

> Stage-A multi-expert adversarial workflow (8 agents) → main-process finalized. Fixes the v0.4.5 阶段4
> real-machine finding: the ctl/cli does NOT fail over when its configured broker dies (agents do, via a
> roster cache + multi-endpoint dial). User requirement: "cli 理应自己找出路".
>
> Process for this feature (per user 2026-06-28): Stage A plan (this doc) → Stage B implement + adversarial
> tests + green gates → **skip Stage C internal review, go straight to external review (user)** → experiment.

## APPROACH

Hand `nats.Connect` a **comma-separated, account-verified, floor-last** server list (the agent's proven
lever, `MaxReconnects(-1)` already set) built client-side from a **global, account_pub-keyed, fail-closed
discovery cache**. The cluster pin (account_pub) is established **only out-of-band** (never HTTP-TOFU,
never NATS-reply-TOFU — matches the *tested* agent invariant, agent_config.go:52). Failover endpoints come
from two channels, both reusing the existing `internal/clusterroster` authority:

- **Tier-1 (primary, deployment-free, fixes the incident today):** an OOB invite's inline `seed=`
  (operator-pasted, paste-trusted client-dialable URL) + the configured floor. After one
  `tether cluster pin <invite>`: dial = `[floor=pc732(dead), inviteSeed=racknerd(live)]` → failover works
  with **no manifest deployed, no broker change**.
- **Tier-2 (steady-state convenience, after operator deploys the manifest route):** the signed HTTP
  manifest (`clusterroster.FetchManifest`), refreshed opportunistically post-connect, TTL+pin-gated. No-op
  today (manifest route not deployed on pc732/racknerd) → zero cost on the live fleet.

**Expansion happens ONLY on the persisted `broker_url` (File) source.** `--nats-url` flag / `$TETHER_NATS_URL`
env / cobra default stay pinned-single (operator override is never silently widened).

### Cycle-correct structure
- **READ path** (dial-list build) lives in `internal/cli`, calling `clusterroster` primitives directly
  (`internal/cli` cannot import `internal/agent` — agent→cli cycle is real).
- **WRITE/refresh path** lives in `cmd/tether` (package main, imports both `agent` + `clusterroster`),
  reusing `agent.AdoptDecision`/`RosterState` **in place** via a small struct adapter.
- **floor-last byte-equivalence builder** extracted once into `clusterroster.BuildDialString`, called by
  BOTH agent (`effectiveDialURLs` becomes a thin wrapper) and cli — one invariant, no drift.

### Rejected (with reason)
1. HTTP-TOFU-by-default — a one-time DNS/BGP MITM with a valid ACME cert pins the **attacker's**
   account_pub → every later "verified" endpoint attacker-signed → full control-plane MITM. Pin OOB only.
2. NATS-reply discovery (`ClusterHealthResp.Roster/Seeds`) — couples a client fix to a broker fleet
   upgrade; trust basis false (members hold `Pub: _INBOX.>` → can race a self-signed reply + TOFU-pin an
   attacker on first contact); redundant with the HTTP manifest.
3. Moving `AdoptDecision` into `clusterroster` — blast radius into the agent reconnect hot path for zero
   new capability; reuse in place from package main.
4. Root `PersistentPostRunE` refresh hook — cobra runs only the nearest persistent hook (silently
   shadowed by any future subcommand hook); also fires only on success. Refresh goes inside the connect
   wrapper.
5. HTTP manifest as the only discovery — not deployed today → silent no-op for the real incident; tier-1
   invite-seed is the deployment-free fix.

## DIAL-LIST BUILD + PRECEDENCE

```
ResolveBrokerURLSource(flagVal, flagChanged, home) (url, src):
  flagChanged                  -> (flagVal, SourceFlag)
  $TETHER_NATS_URL != ""       -> (env,     SourceEnv)
  ~/.tether/broker_url != ""   -> (fileVal, SourceFile)
  else                         -> (flagVal, SourceDefault)

ResolveDial(flagVal, flagChanged, home, now) (base, dial):
  base, src := ResolveBrokerURLSource(...)
  if src != SourceFile                                  -> return base, base   // (A) override + (B) non-cluster
  cache := ReadClusterEndpoints(home)
  if cache==nil || cache.PinAccountPub=="" || cache.FloorURL!=base -> return base, base  // (B) cold/cross-cluster
  learned := DialURLs(cache.Roster, base)   if VerifyAt(cache.Roster, pin, now)==nil    else nil
  seeds   := SeedDialURLs(cache.Seeds)      if VerifySeedsAt(cache.Seeds, pin, now)==nil else nil
  floorCSV := join(base, cache.InviteSeeds...)          // OOB-trusted permanent floor
  return base, clusterroster.BuildDialString(learned, seeds, floorCSV)
```

`BuildDialString(learned, seeds []string, floorCSV string)` (lifted from `effectiveDialURLs`): tiers
**learned (VOTER-first) → signed seeds → floorCSV split**, dedup in order, **floor parts LAST / never
removed**, comma-join. Empty learned+seeds with `floorCSV==base` → returns `base` unchanged.

- **(A) Operator override:** Flag/Env/Default return `(base, base)` — pin to exactly that endpoint, no
  expansion, no cache writes. Only the persisted `broker_url` default expands.
- **(B) Non-cluster byte-equivalence (hard invariant):** no cache / `pin==""` / `FloorURL` mismatch /
  non-File source → `dial == base` byte-for-byte == today's `ResolveNATSURLFromHome` output.
- **`FloorURL` key:** cache only expands when learned under the *current* `broker_url`; repointing to a
  different cluster → cache ignored → clean re-pin (no stale cross-cluster expansion).

**Threading:** new `cmd/tether/ctl_connect.go::connectCtl(cmd, home, flagVal, flagChanged, id, name)`:
1. `base, dial := cli.ResolveDial(...)`
2. if `dial != base`: add `nats.DontRandomize()` (honor order) + `nats.Timeout(3s)` (bound per-endpoint
   stall; proxydial is 10s/endpoint). Single-URL path keeps today's opts → byte-equivalent.
3. `nc, err := cli.ConnectNATSWithNkey(dial, id, name, opts...)` (signature unchanged — accepts comma list).
4. on err: `connectError(verb, base, err)` (human text uses `base`).
5. post-connect best-effort TTL+pin-gated HTTP refresh (tier-2); failure = no-op.

`nats.Connect` semantics: the comma list fails over on the **initial** connect (walks the pool once, first
success wins, `ErrNoServers` only if all fail; refused advances instantly). No retry loop, `RetryOnFailedConnect`
stays false → short-lived cli **fails fast (exit 69)** when all down. `MaxReconnects(-1)` bonus: long-running
`run`/`exec`/PTY survives a broker bounce onto a survivor.

## PERSISTENCE + TRUST

**File:** `<home>/cluster_endpoints.json`, global (cluster/node/status are session-less), mode **0600**,
atomic (`CreateTemp→Chmod 0600→Write→fsync→Close→Rename→parent fsync`, copied from roster_cache.go, not
extracted).

```go
type ClusterEndpoints struct {
    SchemaVersion int                  `json:"schema_version"` // 1
    PinAccountPub string               `json:"pin_account_pub"` // set OOB only
    FloorURL      string               `json:"floor_url"`       // broker_url it was learned under = key
    InviteSeeds   []string             `json:"invite_seeds,omitempty"` // OOB-trusted, permanent floor
    RosterGen     uint64               `json:"roster_gen"`
    Roster        *proto.ClusterRoster `json:"roster,omitempty"`        // signed, HTTP-refreshed, re-verified on read
    SeedGen       uint64               `json:"seed_gen"`
    Seeds         *proto.SeedBundle    `json:"seeds,omitempty"`         // signed, HTTP-refreshed, re-verified on read
    FetchedAt     string               `json:"fetched_at"`             // RFC3339 refresh TTL gate
}
```

**Trust (fail-closed):**
- Pin set ONLY OOB (`cluster pin <invite>`; `login --account-pub` was a planned alias but cut — see Q1). Never HTTP/NATS-TOFU. First-write-wins;
  a different pin requires `--force`.
- Every expansion re-verifies cached signed Roster/Seeds against the pin at `now` (sig + AccountPub==pin +
  schema-compat + expiry + monotone gen). Verify-fail → that tier dropped → floor + OOB invite seeds.
- HTTP refresh pin-gated + no-poison: `AdoptDecision` adopts only if verified + monotone; **reject leaves
  cache bytes UNCHANGED**.
- WORST-THING: dialing an unverified learned endpoint → attacker NATS accepts the nkey CONNECT then
  exfiltrates the session PIN (`nats.Token` first join) + `push`/`exec` payloads + forges "ok". Pin +
  verify-before-add is the sole barrier → OOB-only pin, verify-on-every-read.

## FILES TO ADD / MODIFY

**ADD**
- `internal/clusterroster/dial.go` (+`_test.go`) — `BuildDialString(learned, seeds []string, floorCSV string) string`.
- `internal/cli/cluster_endpoints.go` (+`_test.go`) — `ClusterEndpoints`; `ReadClusterEndpoints`/`WriteClusterEndpoints`
  (atomic 0600, copied); `Source` enum; `ResolveBrokerURLSource`; `ResolveDial`. Imports `clusterroster`+`proto` only.
- `cmd/tether/ctl_connect.go` — `connectCtl(...)` + `connectCtlOpts(...)` (the latter takes extra NATS options,
  e.g. `nats.Token(pin)` for login PIN join). resolve→expand→connect with conditional DontRandomize+Timeout
  →post-connect HTTP refresh via FetchManifest + AdoptDecision through a ClusterEndpoints↔RosterState adapter +
  WriteClusterEndpoints. **The refresh is gated on SOURCE eligibility (persisted-default, not flag/env), NOT on
  `dial != base`** — so a bootstrap-only pin (whose dial has not expanded yet) still warms (external-review F1).
- `cmd/tether/cluster_pin.go` — `tether cluster pin <invite>` (parse discovery invite; write pin+bootstrap+inviteSeed;
  refuse different existing pin without `--force`; eager refresh) + `tether cluster invite` (mint a session-less
  discovery invite, broker-host).
- `cmd/tether/ctl_failover_test.go` — tests #11 (no-HTTP-TOFU), #12 (pin/forged-invite).
- `test/cli_failover/` (gated `//go:build cli_failover_integration`, `-race`) — integration #18–#21 reusing the
  `test/d3` auth_callout+WSS+mTLS-routes harness.

**MODIFY**
- `internal/agent/roster.go` — `effectiveDialURLs` → thin wrapper over `clusterroster.BuildDialString` (behavior-
  preserving; covered by existing agent byte-equivalence + leak gates).
- `internal/clusterroster/invite.go` — add SID-optional `MintDiscoveryInvite`/`ParseDiscoveryInvite` (token
  `tether-invite:v1?pin=&url=[&seed=]`, no `sid`). Required: both MintInvite + ParseInvite hard-reject empty SID.
  Leave the SID-required `agent join` path untouched.
- `cmd/tether/{node,ps,run,exec,expose,session,alert,proxy,transfer,history,cluster_status_nats}.go` — ~15–20 connect
  sites: `ResolveNATSURLFromHome+ConnectNATSWithNkey+connectError` triple → `connectCtl(...)`. **Including
  `session create`** (external-review F2: it is a ctl-over-NATS command + must fail over; broker_url is still
  persisted as the human `base`, and `--nats-url` still pins single). KEEP `ResolveNATSURLFromHome` (single URL)
  only where the resolved value is displayed/persisted, never to gate the dial.
- `cmd/tether/login.go` — keep broker_url write (only on explicit `--broker`/`--nats-url`); pure-auth → `connectCtl`,
  activation (incl. PIN join) → `connectCtlOpts` (external-review F3). `connectCtl`/`connectCtlOpts` recognize
  `--broker` (login's `--nats-url` alias) so an explicit broker still pins single. **`login --account-pub` was
  NOT shipped** (deliberate scope cut, external-review Q1): `cluster pin <invite>` is the single pin path.
- `cmd/tether/logout.go` — **NOT modified** (external-review Q2): logout does NOT clear `cluster_endpoints.json`.
  FloorURL keying ignores a cache from a different `broker_url` (cluster switch), and signed roster/seeds are
  fail-closed re-verified against the pin; only same-host reprovisioning (same `broker_url`, new account) needs
  `cluster pin --force`. Clearing on logout would break the common logout/login-same-cluster flow.
- `ReadClusterEndpoints` — rejects a future-`SchemaVersion` envelope (treats as absent → floor-only;
  external-review Q3).
- `TETHER_NO_DISCOVER` env escape hatch (R3): when set, skip expansion + refresh (pin-single, today's behavior).

**REUSE (no change):** `clusterroster.{FetchManifest, VerifyAt, VerifySeedsAt, DialURLs, SeedDialURLs}`;
`agent.{AdoptDecision, RosterState}` via adapter; atomic-write pattern; `signedManifestServer`; `test/d3` harness;
exit taxonomy. **No proto/wire change, no broker change.**

## ADVERSARIAL TEST MATRIX

Pure unit (`internal/cli`): 1 阶段4 repro (dial contains survivor) · 2 non-cluster byte-equivalence · 3 `--nats-url`
pins single · 4 `$TETHER_NATS_URL` pins single · 5 only SourceFile expands · 6 forged manifest → reject + cache not
poisoned · 7 account_pub mismatch → reject · 8 expired roster/seeds → dropped → floor-only · 9 future-schema rejected ·
10 monotone rollback rejected · 11 NO HTTP-TOFU (no pin → no populate) · 12 OOB invite pin (valid accepts / forged
refused) · 13 cache atomic+0600+corrupt-tolerant · 14 FloorURL key cluster-switch ignores cache · 15 wss `:443`/path
templating preserved · 16 floor never dropped · 17 undialable host filtered · 22 DontRandomize/Timeout only when
expanded · 23 `TETHER_NO_DISCOVER` pins single.

Gated integration (`test/cli_failover`, reuse `test/d3`): 18 阶段4 faithful repro (2-broker auth_callout+WSS; login;
**wait raft replication of cli identity to survivor**; warm cache / `cluster pin`; kill primary; `node ls` +
`cluster status --remote` succeed on survivor) · 19 identity-replication race (kill before settle → fails-auth;
settled → succeeds) · 20 all down → exit 69 no hang · 21 hanging (iptables-DROP) first endpoint → connects within ~3s.

Agent non-regression: existing `effectiveDialURLs` byte-equivalence + leak suites stay green after the lift.

## DECISIONS ON OPEN QUESTIONS (main process)
- **R1** cold-cache + broker-down needs one OOB `cluster pin` — ACCEPT (inherent to no-HTTP-TOFU).
- **R2** ship tier-1 invite-seed static failover now + tier-2 HTTP manifest auto-discovery included but no-op until
  the operator deploys the manifest route — ACCEPT.
- **R3** synchronous bounded refresh + `TETHER_NO_DISCOVER` escape hatch — ACCEPT.
- **R4** dedicated SID-optional discovery token (additive; leaves `agent join` untouched) — ADOPT.
- **R5** `BuildDialString` extraction (pure lift, guarded by agent tests) — DO IT.
- **R6** session/identity replication race — documented + tested (#19), not a blocker.
- **R7** rollout: client-side only, additive, no broker/fleet upgrade, no wire change → folds into HA-GA cli release;
  v1 patch line unaffected (floor-only no-op). Security focus = OOB-only pin + reject-leaves-cache-untouched +
  `src==File`-only gate on both expansion and writes.
- **Deferred:** completion_transport.go expanded-dial threading (out of scope for the smallest correct change).

## NON-REGRESSION ARGUMENT
The only hot-path change is `ResolveDial`, provably the identity function unless ALL of: `src==SourceFile` ∧ cache
exists ∧ `PinAccountPub!=""` ∧ `FloorURL==base` ∧ cached bundles verify at `now`. Otherwise `dial==base` byte-for-byte
and the `nats.Connect` option set is identical (two new opts only on the expanded path, no-ops for one server).
`ConnectNATSWithNkey` signature / `MaxReconnects(-1)` / Nkey sigCB / proxydial untouched; no `RetryOnFailedConnect`
→ fail-fast/exit-69 holds. The `effectiveDialURLs`→`BuildDialString` lift is a pure relocation behind a wrapper;
`AdoptDecision` reused in place; no proto/wire change → identical against v1 + v2 brokers (v1 has no manifest/invite-seed
→ floor-only no-op).
