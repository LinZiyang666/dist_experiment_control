# P12 External Review

**RESULT: FAIL**

Date: 2026-06-09
Reviewer role: external reviewer

## Scope

Reviewed the uncommitted P12 change set on branch
`phase/12-expose-remote-port`, with emphasis on:

- `docs/reviews/p12-plan.md`
- `tether expose --remote-port`
- additive `ExposeReq.RemotePort` wire compatibility
- desired-port allocation, reuse, rejection, and concurrency
- broker error/audit/forwarding paths
- CLI, protocol, unit, race, and P6 e2e coverage

## Verdict

P12 is not approved yet.

The implementation path is coherent and the focused tests are stable. I did
not find a demonstrated allocation or concurrency bug. Two acceptance gaps
remain: the authoritative architecture still contradicts the feature, and the
CLI wire-through requirement is not actually tested.

## Findings

### F1 - Medium: the architecture SSOT still says expose always picks the first free port

`CLAUDE.md` defines `docs/architecture.md` as the implementation ruler, but
the architecture was not updated for P12:

- `docs/architecture.md:867-872` says every expose finds the first free port.
- `docs/architecture.md:876-905` documents only the automatic allocation flow.
- `docs/architecture.md:982-988` and `docs/architecture.md:2043` list the old
  command without `--remote-port`.

`docs/usage.md:849-862` correctly documents the new behavior, and the P12 plan
contains the design decisions, but neither replaces the architecture baseline.
Future work following F.3 can legitimately conclude that explicit selection is
not part of the contract.

Recommendation:

Update F.3/F.4/F.8 and the phase/milestone section to define:

- omitted/zero means lowest-free;
- nonzero means exact in-band port;
- `port_taken` is a hard failure with no fallback;
- REVOKED/FREED rows do not block reuse;
- the accepted same-proto cross-release silent-downgrade limitation.

### F2 - Medium: the planned CLI wire-through acceptance test is missing

`docs/reviews/p12-plan.md:121-125` explicitly requires a CLI test proving that
`--remote-port 14005` produces JSON containing `"remote_port":14005`.

The submitted CLI tests at `cmd/tether/expose_remote_port_test.go:19-71` cover
only local range validation and whether an in-range value reaches connection
setup. They never capture the NATS request body. The protocol tests prove that
an already-populated `ExposeReq` serializes correctly, while the P6 e2e tests
construct `ExposeReq{RemotePort: ...}` directly.

Therefore, deleting `RemotePort: remotePort` from
`cmd/tether/expose.go:78` would leave all new P12 tests green while making the
user-facing flag silently ineffective.

Recommendation:

Run `newExposeCmd` against an embedded NATS responder, capture the expose
request, and assert both paths:

- `--remote-port 14005` sends `remote_port: 14005`;
- omission sends no `remote_port` key.

Also tighten `TestExposeRequestedPortOutOfBand` to assert the recording adapter
received zero `AddProxy` calls, matching the reject-path invariant in
`p12-plan.md:108-110`.

## Questions and Suggestions

- The accepted new-CLI/old-broker behavior silently ignores the requested
  port because `ProtoVersion` remains 1. Is operational same-release rollout
  enforcement strong enough, or should a future capability check replace this
  documented downgrade?
- Add one real tunnel data-plane test requesting a non-lowest port and dialing
  that exact public listener. Current P12 P6 tests validate control-plane
  forwarding through a recording adapter, which is sufficient for this patch
  but leaves the final bind path implicit.
- Consider adding `RemotePort` to the populated `ExposeReq` case in
  `internal/proto/proto_invariants_test.go` so the central message catalogue
  also exercises the new nonzero field.

## Verification

Focused feature checks:

```text
go test ./internal/port ./internal/proto ./cmd/tether -count=1
# P12-related packages PASS; the combined cmd run initially hit the sandbox's
# local-listener restriction, then passed outside the sandbox.

go test ./test/p6 ./cmd/tether -count=1
# PASS

go test -race ./test/concurrency \
  -run 'TestConcurrentPortAllocations|TestConcurrentDesiredPortExactlyOneWins|TestSqliteFileTwoOpensCanCoexist' \
  -count=1
# PASS

go test ./internal/port \
  -run 'TestAllocateDesiredPort|TestIsUniqueViolation|TestTranslateInsertErr' \
  -count=50
# PASS

go test ./test/p6 \
  -run 'TestExpose(HonorsRequestedRemotePort|RequestedPortTakenHardFails|RequestedPortOutOfBand|RequestedPortReusableAfterRm|OmittedRemotePortUnchanged)$' \
  -count=10
# PASS

go test -race ./test/concurrency \
  -run '^TestConcurrentDesiredPortExactlyOneWins$' -count=20
# PASS

go test ./cmd/tether \
  -run 'TestExposeRemotePort|TestBrokerErrorMessageRegisteredCodes' -count=20
# PASS

go vet ./internal/port ./internal/proto ./cmd/tether ./internal/broker ./test/p6 ./test/concurrency
# PASS

go build ./...
# PASS, with a non-fatal module stat-cache permission warning
```

Full-suite status on `go1.26.3 darwin/arm64`:

```text
go test ./... -count=1
# FAIL on pre-existing macOS/platform-sensitive tests:
# - internal/agent: /var vs /private/var canonical-path expectations
# - test/cli_e2e, test/concurrency, test/p9: Unix socket path too long
# - test/p10: Linux agent install review test rejects macOS
# P12-related packages, including test/p6, PASS.

go test -count=1 -tags e2e_matrix -v ./test/e2e/...
# P1-P8 PASS, including P6 with the new remote-port tests.
# P9 and P10 FAIL for the same macOS-specific baseline reasons above.

make lint
# NOT RUN: golangci-lint is not installed in this environment.
```

No implementation or reviewer test files were changed during this external
review; only this report was added.

---

## Maintainer Response (P12 owner, 2026-06-09)

Both findings accepted as valid and **fixed**; all three suggestions addressed
(two adopted, one answered + deferred with rationale). Re-verified: `go build`
OK, `golangci-lint run` 0 issues, P12 packages + `test/p6` (incl. the e2e
matrix) + `-race` concurrency all PASS.

### F1 (Medium) — architecture SSOT not updated → FIXED

Agreed: `CLAUDE.md` makes `docs/architecture.md` the implementation ruler, so
P12 must land there, not only in `usage.md`/the plan. Updated `docs/architecture.md`:

- **F.3 端口按需分配** — rewritten to define both modes off the wire field
  `ExposeReq.remote_port`: omitted/0 = lowest-free; nonzero = exact in-band port;
  `port_out_of_band` / `port_taken` (hard fail, **no fallback**); REVOKED/FREED
  do not block; atomicity via the partial unique index; additive field / no proto
  bump; and the same-proto cross-release **silent-downgrade** limitation stated
  inline.
- **F.4** flow diagram — the ctl command and `expose.req` payload now show
  `[--remote-port]` / `remote_port`, and the port-selection step branches on it.
- **F.8 命令集对称** table + the **P6 milestone bullet** (§ near line 2043) — both
  expose commands now show `[--remote-port P]` with the reject semantics.

### F2 (Medium) — CLI wire-through acceptance test missing → FIXED

Agreed and confirmed your mutation: deleting `RemotePort: remotePort` from
`cmd/tether/expose.go` previously left every P12 test green. Fixed:

- Added **`TestExposeRemotePortWireThrough`** (`cmd/tether/expose_remote_port_test.go`):
  runs the real `newExposeCmd` against an embedded NATS responder
  (`TETHER_DEV_NO_AUTH=1`), captures the published `ExposeReq`, and asserts
  (a) `--remote-port 14005` → body contains `"remote_port":14005` and decodes to
  `RemotePort==14005`; (b) omission → **no** `remote_port` key. This now fails if
  the flag→marshal threading is removed.
- Tightened **`TestExposeRequestedPortOutOfBand`** (`test/p6/expose_remote_port_e2e_test.go`)
  to assert the recording adapter received **zero** `AddProxy` calls, matching the
  reject-path invariant (plan §"reject does not forward").

### Suggestions

- **(S1) capability check vs documented downgrade** — *Answered, deferred by
  design.* The silent same-proto downgrade is a deliberate locked decision
  (plan D-8): we chose additive-no-proto-bump over a `CapsResp.BrokerRelease`
  gate because release-string comparison is brittle and this is a
  single-broker, same-version-rollout deployment. Enforcement is operational
  (deploy ctl+broker from the same release). It is now documented as a known
  limitation in **both** `architecture.md` F.3 and `usage.md` §5.9. If a future
  phase introduces a real capability-negotiation surface, replacing the
  documented downgrade with a hard client-side refusal is the natural upgrade;
  filed as future work, not blocking P12.
- **(S2) real tunnel data-plane test (dial the exact public listener)** —
  *Deferred, consistent with existing design.* The P6 suite deliberately tests
  the control plane through a recording adapter; the real frps/yamux bind path
  is exercised manually per the P6 architecture note (`test/p6/expose_e2e_test.go`
  header). A non-lowest-port live-dial test would be a valuable addition but is a
  pre-existing P6 coverage boundary, not introduced by P12; out of scope for this
  patch.
- **(S3) add `RemotePort` to the central message catalogue** — *Adopted.*
  `internal/proto/proto_invariants_test.go` `ExposeReq` case now populates
  `RemotePort: 14005`, so the roundtrip / DeepEqual catalogue exercises the
  nonzero field.

### Note on `make lint`

golangci-lint wasn't installed in the review environment; I installed
`golangci-lint v2.5.0` (the pinned version) via `go install` and ran the full
`golangci-lint run` → **0 issues**.

**Status: external review findings resolved. Ready for re-review / step 7
(commit + push) on your approval.**
