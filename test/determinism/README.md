# determinism-lint (§13.1)

Machine-checks for the distributed-broker HA epic's determinism contract
(`docs/distributed-broker-architecture.md` §13.1 / §3.3, `docs/reviews/d0-plan.md` §6).

## D0 scope (this skeleton)

D0 has **no FSM/Apply root**, so the full "Apply-reachable mutator" graph cannot
be computed yet. The skeleton is runnable now and ships two checks that already
have bite, plus a named D2 continuation point:

| Check | What it does in D0 |
|---|---|
| `TestDeterminismBannedImportBaseline` | Pins the CURRENT set of `crypto/rand` / `math/rand` / `oklog/ulid/v2` imports in `internal/{port,proc,node,session,agentprov}` as a monotonic baseline (`{port→crypto/rand, proc→oklog/ulid/v2}`). A new banned import → red; removing a baseline hit → red. Warn-not-fail today (these imports are legal until the Plan/Apply split). |
| `TestRaftConfinedToClusterPackage` | Forbids any **product** (non-test) package outside `internal/cluster` from importing `hashicorp/raft` / `raft-boltdb`. Enforces §19 "❌ D1 之前碰任何 apply.*". |
| `TestApplyReachabilityDeterminismLint` | `t.Skip` — the D2 continuation point (forbid FSM-external INSERT to Apply-owned tables, forbid `Apply→*sql.DB` mutators, column-level activity assertions). |
| `TestNoStrayVersionLiteral` | After the v1→v2 SSOT flip, forbids hardcoded `tether.v<N>` string literals in product code (only `internal/auth/permissions.go`'s import-cycle copy is whitelisted). |

## D2 (full lint)

When D2 introduces the op set + Plan/Apply split, replace the
`TestApplyReachabilityDeterminismLint` skip with the real reachability lint and
tighten `TestDeterminismBannedImportBaseline` from baseline-compare to
`t.Fatal` on any banned import reachable from `Apply`.

It is a plain `go test` package (not a golangci-lint plugin) — `make test`
covers it; `make lint` is unaffected (§18.3: "lint 只是 tripwire").
