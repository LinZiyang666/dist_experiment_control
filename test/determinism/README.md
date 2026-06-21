# determinism-lint (§13.1)

Machine-checks for the distributed-broker HA epic's determinism contract
(`docs/distributed-broker-architecture.md` §13.1 / §3.3, `docs/reviews/d0-plan.md` §6).

## D1 status (LIVE)

D0 shipped the skeleton with no FSM/Apply root. **D1 landed the FSM/Apply layer in
`internal/cluster`**, so the raft-confinement guard (L-2) is now LIVE — it actually
hits the whitelist branch (internal/cluster is the first PRODUCT raft importer) —
and the banned-import scan now also covers internal/cluster. The full
"Apply-reachable mutator" reachability graph is still D2.

| Check | Status |
|---|---|
| `TestDeterminismBannedImportBaseline` | Pins the CURRENT `crypto/rand` / `math/rand` / `oklog/ulid/v2` imports in `internal/{port,proc,node,session,agentprov}` as a monotonic baseline (`{port→crypto/rand, proc→oklog/ulid/v2}`). New banned import → red; removing a baseline hit → red. Warn-not-fail until the D2 Plan/Apply split. |
| `TestRaftConfinedToClusterPackage` | Forbids any **product** (non-test) package OUTSIDE `internal/cluster` from importing `hashicorp/raft` / `raft-boltdb`; `internal/cluster` is WHITELISTED (it now holds D1's product raft node/FSM). Shares the `raftConfinementOffender` predicate with the self-check + a visited-file-count floor so a broken traversal can't go vacuously green. |
| `TestRaftConfinementWhitelistIsLive` | **D1**: asserts the whitelist is NOT vacuous — a product raft import really exists under `internal/cluster`. |
| `TestRaftConfinementSelfCheck` | **D1**: non-vacuity — the predicate flags a raft import under `internal/port` and allows one under `internal/cluster`. |
| `TestClusterApplyNoNondeterministicImports` | **D1**: forbids `crypto/rand`/`math/rand`/`oklog/ulid` in `internal/cluster` product code (§3.4); green with a real subject (D1's op bakes a literal). |
| `TestLivenessColumnLintSelfCheck` | **D1**: static non-vacuous self-check of the liveness-column detector (flags `UPDATE nodes SET status`, ignores a non-liveness UPDATE). The real Apply-reachability column guard — which must NOT false-positive on restore-time `RebuildLiveness` — is D2. |
| `TestApplyReachabilityDeterminismLint` | `t.Skip` — the D2 continuation point (forbid FSM-external INSERT to Apply-owned tables, forbid `Apply→*sql.DB` mutators, column-level activity assertions). |
| `TestNoStrayVersionLiteral` | After the v1→v2 SSOT flip, forbids hardcoded `tether.v<N>` string literals in product code (only `internal/auth/permissions.go`'s import-cycle copy is whitelisted). |

## D2 (full lint)

When D2 introduces the op set + Plan/Apply split, replace the
`TestApplyReachabilityDeterminismLint` skip with the real reachability lint and
tighten `TestDeterminismBannedImportBaseline` from baseline-compare to
`t.Fatal` on any banned import reachable from `Apply`.

It is a plain `go test` package (not a golangci-lint plugin) — `make test`
covers it; `make lint` is unaffected (§18.3: "lint 只是 tripwire").
