# Original flow and SS hot-update tests

The [binding S5 plan](../../docs/reviews/hot-update-flow-carrier-plan.md) §8/§12 assigns P19–P22
these tests. The Go harness/tests and S5 fix-round implementations are present in this tree.
S5 final receipts and the user-approved ENDPOINT-RESUME carry into S6 are archived in
[the final evidence](../../docs/reviews/hot-update-evidence.md#17-s5-flow-carrier-ss-最终收尾收据2026-10-11).
The S5 audit below reconciles the approved 2026-10-11 C1 freeze and partial/carry signoff with the
archived receipts. It does not promote the historical §16 NOT_RUN rows or sign later-stage completion.
Product exit status lives in the [receipt index](../../docs/reviews/hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index).

| Layer / owner | Real resources and independent oracle | Boundary |
|---|---|---|
| P19 reference | Raw duplex seq/hash/identity ledgers and independent SS AEAD parser/golden, actual subkey+nonce ledger | Never generate expected bytes with the product codec or combine source/receiver ledgers to hide missing bytes. |
| Same-test-binary kernel harness | Frozen FD/replay/parser/nonce state through Prepared.Exec at the same PID; before/after kernel fingerprint | Proves kernel handoff only, distinct from a real product artifact upgrade. |
| P20 real flow / T39–T41 | Two SHA-distinct tether artifacts, four original bidirectional TCP flows, carrier lifecycle and half-close tail>8 MiB | Running /proc executable changes; application nonce/socket identity unchanged, target Accept=1, declared final seq/hash and EOF exact. |
| P21 real SS / T42–T43 | Independent client trace, already-Sealed partial suffix, silent revoke and fresh grant revision | Actual 0<n<attemptLen, no re-Seal/redial, revoked original/dup/public replay close while bob continues; zero target Write before grant. |
| P22 rollback / T44–T45 | Real A→B→A plus the genuine ready-pending120s watchdog as separate cases | Current offsets/nonces/revoke/retirement advance in B; failure after partial activation takes generation+1 current cut, no pristine Detach. |

Select the owning top-level test, for example `go test -race -count=1 -timeout 600s ./test/hotupdate/ -run '^TestRawProbeDetectsDroppedAndDuplicatedBytes$'`.
Real product suites use `hotupdate_integration` and per-test partitions; do not launch the accumulated
whole hotupdate package as one 30m worker. The installed matrix declarations are in test/e2e/all_phases_test.go.
The production artifact builds inherit `GOFLAGS=-overlay=<json>` and race instrumentation; outer
`go test -overlay` alone does not mutate a nested build. Use sandbox binaries and HOME/state directories;
never run installed tether or use production state. Same-PID helpers use exact selectors, unified spawnexec
init dispatch and bidirectional mode/handoff env stripping (testing-standards §0.4–§0.6).
Freeze deadlines include lock/writer queues; for cancelled yamux fence→TLS Close→yamux Close→join outside locks.

Every concurrency/leak test calls shared goroutine/FD helpers directly in its Test body and directly calls
an exercise in each of≥5 rounds. Observe by FD category while the receiver is still alive and after product
ownership retirement, before fixture teardown. flow is explicit risk even without yamux imports.
P27 must include whole-package race tuples, leak anchors and new names using append-only updaters;
watchdog units have timeout≥240s. Each defect in S5 §8 is injected independently, then run/restored;
save native rc, failing assertion, diff/SHA and restored baseline. No test process forks `go test`.
Darwin legacy lifecycle executes on a Darwin runner; Linux unsupported-body tests and cross-build are separate.

## Documentation audit

The existing Go gates below check tracked top-level docs layout, promises in Go comments, and CLAUDE's
Makefile gate coverage. They do not parse Markdown completion claims. Run this Python block from the
repository root for the additional P28 audit; it reads source/docs only and starts no service or binary.
It verifies paths, exact live test declarations, the single frozen plan, required receipt rows and unchecked
completion while receipts are absent. It checks raw-artifact existence for future COMPLETE rows, **not their
contents or validity**: P29/lead must independently review source/artifact/running identities, native results,
resources and each mutation before changing statuses/checks. Already reviewed S5 subitems require their exact scoped dependencies; D-4 retains the explicit
ENDPOINT-RESUME carry/red. New aggregate completion still requires P29 validation and every required
row. S6/S8/S9/S10 scope must be judged independently.

To execute the canonical Python fence without maintaining a second script:

```sh
python3 - <<'PY_AUDIT'
from pathlib import Path
path = Path("test/hotupdate/README.md")
code = path.read_text().split("```python\n", 1)[1].split("\n```", 1)[0]
exec(compile(code, str(path), "exec"))
PY_AUDIT
```

<!-- s5-doc-registry:start -->
| Path | Kind | Symbol |
|---|---|---|
| CLAUDE.md | doc | - |
| docs/distributed-broker-architecture.md | doc | - |
| docs/usage.md | doc | - |
| docs/broker-ops.md | doc | - |
| docs/deploy-tier-gotchas.md | doc | - |
| docs/reviews/hot-update-plan.md | doc | - |
| docs/reviews/hot-update-stages-plan.md | doc | - |
| docs/reviews/hot-update-handoff-failures.md | doc | - |
| docs/reviews/hot-update-tasklist.md | doc | - |
| docs/reviews/hot-update-flow-carrier-plan.md | doc | - |
| docs/reviews/hot-update-evidence.md | doc | - |
| docs/reviews/INDEX.md | doc | - |
| docs/reviews/hot-update-handoff-protocol.md | doc | - |
| docs/reviews/hot-update-handoff-process.md | doc | - |
| test/README.md | doc | - |
| test/hotupdate/README.md | doc | - |
| test/simcluster/README.md | doc | - |
| test/architecture/docs_layout_test.go | test | TestDocsTopLevelHoldsNoProcessArtifacts |
| test/determinism/promised_guard_test.go | test | TestPromisedGuardTestsExist |
| test/architecture/gate_registry_test.go | test | TestGatesTargetCoversEveryGateCLAUDEMdNames |
<!-- s5-doc-registry:end -->

<!-- s5-doc-audit:start -->
```python
# origin: hot-update S5 plan §11 / P28 documentation mutations.
from pathlib import Path
import hashlib
import re

root = Path.cwd().resolve()
assert (root / "go.mod").is_file(), "run the audit from the repository root"
readme = (root / "test/hotupdate/README.md").read_text()

def section(text, start, end):
    assert text.splitlines().count(start) == text.splitlines().count(end) == 1, "missing or duplicate audit boundaries"
    return text.split("\n" + start + "\n", 1)[1].split("\n" + end + "\n", 1)[0]

# Markdown is outside the Go comment-only promised-guard scan.
registry = section(readme, "<!-- s5-doc-registry:start -->", "<!-- s5-doc-registry:end -->")
registered = []
for line in registry.splitlines():
    fields = [x.strip() for x in line.strip().strip("|").split("|")]
    if len(fields) == 3 and fields[1] in {"doc", "test"}:
        path, kind, symbol = fields
        registered.append(path)
        file = root / path
        assert file.is_file(), "missing documentation/guard path: " + path
        if kind == "test":
            assert re.search(r"(?m)^func " + re.escape(symbol) + r"\(t \*testing\.T\)", file.read_text()), \
                "dead test promise: " + path + ": " + symbol
assert len(registered) == 20 and len(set(registered)) == 20, "documentation registry drift"

# One frozen source, including lead C1 (2026-10-11); evidence §17.6 recorded the old digest gap.
# Do not rewrite the plan or remove this mutation-sensitive digest check.
plan = (root / "docs/reviews/hot-update-flow-carrier-plan.md").read_bytes()
# The permitted location pointer does not change the frozen contract text.
if plan.startswith(b"> Documentation pointer: "):
    plan = plan.split(b"\n", 1)[1]
assert hashlib.sha256(plan).hexdigest() == "dd47cadc45f229b133822b29fde016958465388e556973efc66b437b4285d135", \
    "frozen plan changed"
assert not (root / "docs/reviews/hot-update-flow-carrier-ss-plan.md").exists(), "duplicate S5 plan"
assert not (root / "docs/gotchas.md").exists(), "S5 hazards must use the binding architecture and deployment ledger"

# Exact row membership catches missing receipts without deriving requirements from the rows themselves.
evidence = (root / "docs/reviews/hot-update-evidence.md").read_text()
table = section(evidence, "<!-- s5-receipts:start -->", "<!-- s5-receipts:end -->")
expected = {"exit-" + str(n) for n in range(1, 7)} | {"T%02d" % n for n in range(1, 52)}
expected |= set("75 75.A 75.C 75.R 75.B 75.W 75.L early-close wrong-offset 39.F "
                "36.A 36.W 36.C 36.L 39.P 34-all 35-all 33-boundary 70 72 fuzz-exact-two "
                "whole-race five-round-leaks public-inventory private-codec-inventory gate-controls "
                "domain-fd-budget alpn-four-quadrants freeze-full-window latency-max native-verdicts "
                "source-artifact-running make-test make-build make-e2e-parallel make-lint make-gates "
                "vet-tags darwin-cross-build darwin-native".split())
expected |= {"named:" + name for name in (
    "TestBootstrapFlowRestoreStartsNoWorkers",
    "TestCurrentOwnerTicketCoversLongIdleAndFirstDetach",
    "TestDomainCompleteActivationWindow",
    "TestFlowFrameInventoryControls",
    "TestFlowSSBudgetControls",
    "TestFlowSSContractControls",
    "TestFlowSSDataplaneLifetimeControls",
    "TestFlowSSEnumControls",
    "TestFlowSSExecFailureThawsBothDirections",
    "TestFlowSSGateRegistryControls",
    "TestFlowSSImageInventoryControls",
    "TestFlowSSLayeringControls",
    "TestFlowSSLeakRiskControls",
    "TestFlowSSMatrixControls",
    "TestFlowSSRedispatchUsesCurrentState",
    "TestPrepareActivateLostReplyHasOneOwner",
    "TestProxyApplyEntrypointsAreSerialized",
    "TestProxyFiredCleanupSurvivesCut",
    "TestProxyPairDigestIsNotSuppressed",
    "TestRawImageSalvageKeepsAllTakenFiles",
    "TestRawProbeDetectsDroppedAndDuplicatedBytes",
    "TestRawSocketDarwinImageUnsupported",
    "TestReplayACKKeepsCapacityReservation",
    "TestSSCapacityFailureStillRevokesRemovedKeys",
    "TestSSReferenceRejectsNonceReuse",
    "TestSSStateInventoryControls",
    "TestSealedStateRejectsUnsealedDescriptor",
)}
receipts = {}
validation = root / "docs/reviews/hot-update-flow-carrier-ss-validation.md"
for line in table.splitlines():
    fields = [x.strip() for x in line.strip().strip("|").split("|")]
    if len(fields) != 4 or fields[0] in {"Receipt", "---"}:
        continue
    ident, status, artifact, _ = fields
    assert ident not in receipts, "duplicate S5 receipt: " + ident
    assert status in {"NOT_RUN", "NOT_RECORDED", "NOT_MEASURED", "COMPLETE"}, "invalid receipt status: " + ident
    receipts[ident] = status
    if status == "COMPLETE":
        assert validation.is_file(), "COMPLETE receipt lacks P29 validation: " + ident
        link = re.fullmatch(r"\[[^]]+\]\(([^)#]+)(?:#[^)]*)?\)", artifact)
        assert link and (root / "docs/reviews" / link[1]).resolve().is_file(), \
            "COMPLETE receipt lacks raw artifact: " + ident
    else:
        assert artifact == "-", "unexecuted receipt masquerades as an artifact: " + ident
assert set(receipts) == expected, "S5 receipt membership drift: " + str(sorted(set(receipts) ^ expected))

# Historical partial signoff is distinct from final required receipt closure.
# origin: hot-update S10 plan §13.2 / §13.3 (S5 D6/P29 documentation carry)
tasklist = (root / "docs/reviews/hot-update-tasklist.md").read_text()
items = section(tasklist, "<!-- s5-tasklist:start -->", "<!-- s5-tasklist:end -->")
checks = re.findall(r"(?m)^- \[([ x])\] (S5 [^:]+):", items)
assert len(checks) == 9 and len({name for _, name in checks}) == 9, "S5 tasklist drift"
# These exact scoped dependencies are the reviewed §17 handoff, not synthetic P29 rows.
scoped_dependencies = {'S5 B-2': ['/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-DEPLOY-SUMMARY.md'], 'S5 D-1': ['/home/weiland/s5wt/prompts/result-p1.md', '/home/weiland/s5wt/prompts/result-p2.md', '/home/weiland/s5wt/prompts/result-fix-flow.md', '/home/weiland/s5wt/prompts/result-integ-verify.md', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-279/flow-race/receipt.json', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md'], 'S5 D-2 agent subitem': ['/home/weiland/s5wt/prompts/result-hotupdate-e2e.md', '/home/weiland/s5wt/prompts/result-private-state.md', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json'], 'S5 D-3': ['/home/weiland/s5wt/prompts/result-p3.md', '/home/weiland/s5wt/prompts/result-fix-broker.md', '/home/weiland/s5wt/prompts/result-fix-flow.md', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md#tests-and-native-receipts', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/39-parent/receipt.json'], 'S5 D-4 same-broker subitem': ['/home/weiland/s5wt/prompts/result-fix-broker.md', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md#tests-and-native-receipts', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json', '/home/weiland/s5wt/prompts/result-gates-final3.md', '/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/e2e-parallel.log']}
missing = sorted(ident for ident, status in receipts.items() if ident != "darwin-native" and status != "COMPLETE")
for mark, name in checks:
    if mark != "x":
        continue
    if name in scoped_dependencies:
        line = next(line for line in items.splitlines() if line.startswith("- [x] " + name + ":"))
        actual = re.findall(r"receipt id: `([^`]+)`", line)
        assert actual == scoped_dependencies[name], "S5 scoped receipt dependency drift: " + name
        for path in actual:
            raw = Path(path.split("#", 1)[0])
            assert raw.is_file() and raw.stat().st_size > 0, "missing scoped S5 receipt: " + path
        if name == "S5 D-4 same-broker subitem":
            assert "**例外：ENDPOINT-RESUME**" in line and "转 S6" in line, "S5 carry erased"
    elif name == "S5 Darwin native":
        assert receipts["darwin-native"] == "COMPLETE", "native Darwin checked without execution"
    else:
        assert validation.is_file() and not missing, "premature S5 completion: " + name + "; missing " + str(missing)
# Stage closure does not promote final six exits, D-5 joint fuzz or native validation.
assert "## 17. S5" in evidence and "## 18. S6" in evidence, "missing S5 closure/carry history"
print("S5_DOC_AUDIT_OK: 17 documents, 3 live guards, %d historical receipt rows; unsigned=%d; scoped=%d" %
      (len(receipts), len(missing), len(scoped_dependencies)))
```
<!-- s5-doc-audit:end -->

Mandatory on-disk controls (one edit at a time; restore in finally, then baseline again):

| Defect | Injection | Expected failing check |
|---|---|---|
| Fake all-complete | Change all S5 completion boxes to `[x]` without P29 receipts | premature S5 completion/native Darwin without execution |
| Wrong path | Replace the architecture path in the doc registry with a nonexistent `.md` path | missing documentation/guard path |
| Dead test promise | Replace the live promised-guard symbol in this registry with a nonexistent Test name | dead test promise |

Extract only the bounded Python fence into a temporary script to run it; the README is the canonical source.
It is not registered in `make gates` in this tree. P28 receipts record baseline, each independent mutation
and restored result; this audit never signs the six product exits or historical receipts.

## S6 documentation audit

This bounded read-only audit is P34's executable documentation check. Run it from the repository root;
it never starts tether, services or deploy tools. The S5 audit above remains separate historical validation.
It checks the eleven owned documents, landed source/tests and actual CLI declarations, then requires
unchecked S6 claims while final receipts are absent. It does not validate raw receipt contents: P35/lead
must review native verdicts, identities, mutations and same-candidate coverage before P34 changes a row.
The P35 evidence file is intentionally optional until a claim is signed. Production same-PID cut,
current-store reopen and rollback are present; final deployment receipts remain pending. The explicit
promises below are a bounded source check, not an exhaustive inventory of runtime wiring.

Run the canonical S6 fence without a second maintained script:

```sh
python3 - <<'PY_S6_AUDIT'
from pathlib import Path
path = Path("test/hotupdate/README.md")
body = path.read_text().split("\n<!-- s6-doc-audit:start -->\n", 1)[1].split("\n<!-- s6-doc-audit:end -->\n", 1)[0]
code = body.split("```python\n", 1)[1].split("\n```", 1)[0]
exec(compile(code, str(path), "exec"))
PY_S6_AUDIT
```

<!-- s6-doc-audit:start -->
```python
# origin: hot-update S6 plan §12.1 P34 / §14
from pathlib import Path
import re

root = Path.cwd()
documents = [
    "CLAUDE.md", "docs/distributed-broker-architecture.md", "docs/deploy-tier-gotchas.md",
    "docs/broker-ops.md", "docs/cluster-runbook.md", "docs/usage.md",
    "docs/reviews/hot-update-tasklist.md", "docs/reviews/hot-update-stages-plan.md",
    "docs/testing-standards.md", "test/simcluster/README.md", "test/hotupdate/README.md",
]
assert len(documents) == len(set(documents)) == 11
for path in documents:
    assert (root / path).is_file(), "missing S6 documentation: " + path

# Explicit landed promises; no runtime implementation is inferred from compile stubs.
promises = [
    ("internal/storage/current.go", "OpenCurrent"),
    ("internal/brokerimage/import.go", "Import"),
    ("internal/brokerimage/boot.go", "Boot"),
    ("internal/brokerimage/preflight.go", "Preflight"),
    ("internal/cli/retry_test.go", "TestRetryUsesRemainingAbsoluteBudget"),
    ("test/architecture/broker_state_inventory_test.go", "TestBrokerImageInventoryControls"),
    ("test/architecture/gate_registry_test.go", "TestBrokerGatesHaveUnfilteredEntryPoints"),
]
for path, symbol in promises:
    file = root / path
    assert file.is_file(), "missing S6 source: " + path
    assert re.search(r"(?m)^func " + re.escape(symbol) + r"\(", file.read_text()), \
        "dead S6 source/test promise: " + path + ": " + symbol

commands = (root / "cmd/tether/cluster_upgrade.go").read_text()
for verb in ("broker", "upgrade", "reload", "status"):
    assert re.search(r'Use:\s*"' + verb + r'"', commands), "missing broker CLI verb: " + verb
for flag in ("socket", "to-version", "expect-sha256", "broker-only"):
    assert re.search(r'(?:StringVar|BoolVar)\([^\n]*"' + flag + r'"', commands), \
        "missing broker CLI flag: " + flag
node = (root / "cmd/tether/node.go").read_text()
assert not re.search(r'upgrade[^\n]*--broker(?:[ "=]|$)', node), "unexpected node broker alias"

def section(text, start, end):
    assert text.count(start) == text.count(end) == 1, "duplicate/missing S6 audit boundary"
    return text.split(start, 1)[1].split(end, 1)[0]

expected = {"E-4", "D-2-broker", "D-4-broker", "D-6-broker", "B-4-broker",
            "coverage-4-5-8", "gates-platform", "ENDPOINT-RESUME"}
tasklist = (root / "docs/reviews/hot-update-tasklist.md").read_text()
checks = re.findall(r"(?m)^- \[([ x])\] S6 ([^:]+):",
                    section(tasklist, "<!-- s6-tasklist:start -->", "<!-- s6-tasklist:end -->"))
assert len(checks) == len(expected) and {c for _, c in checks} == expected, "S6 claim membership drift"
rows = {}
for line in section(tasklist, "<!-- s6-receipts:start -->", "<!-- s6-receipts:end -->").splitlines():
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    if len(cells) != 5 or cells[0] in {"Claim", "---"}:
        continue
    claim, status, source, artifact, receipt = cells
    assert claim not in rows and status in {"NOT_RUN", "COMPLETE"}, "invalid S6 receipt row: " + claim
    if status == "NOT_RUN":
        assert (source, artifact, receipt) == ("-", "-", "-"), "unrun S6 receipt has evidence: " + claim
    else:
        assert re.fullmatch(r"[0-9a-f]{40}", source), "missing final source SHA: " + claim
        assert re.fullmatch(r"[0-9a-f]{64}", artifact), "missing final artifact SHA: " + claim
        assert (root / "docs/reviews/hot-update-broker-evidence.md").is_file(), "missing P35 evidence index"
        link = re.fullmatch(r"\[[^]]+\]\(([^)#]+)(?:#[^)]*)?\)", receipt)
        assert link, "missing P35 raw receipt link: " + claim
        assert (root / "docs/reviews" / link[1]).resolve().is_file(), "missing P35 raw receipt: " + claim
    rows[claim] = (status, source)
assert set(rows) == expected, "S6 receipt membership drift"
for mark, claim in checks:
    assert mark != "x" or rows[claim][0] == "COMPLETE", "premature S6 completion: " + claim
signed = {source for status, source in rows.values() if status == "COMPLETE"}
assert len(signed) <= 1, "S6 claims mix final candidates"
print("S6_DOC_AUDIT_OK: 11 documents, 7 landed source/test promises, 8 unchecked-or-evidenced claims")
```
<!-- s6-doc-audit:end -->

Extract only this bounded Python fence to a temporary file. Mandatory on-disk edit/run/restore controls:

| Mutation | Required red oracle |
|---|---|
| Check all S6 boxes without receipts | premature S6 completion |
| Mark a receipt COMPLETE without evidence | missing final source SHA / missing P35 evidence index |
| Change one document to a nonexistent path | missing S6 documentation |
| Change a promised Test name to a nonexistent name | dead S6 source/test promise |
| Require a nonexistent CLI verb | missing broker CLI verb |

P34 records native rc and failure text for each independent mutant, then restores and reruns baseline.
No inventory/leak-anchor rows arise from this Python audit; P31/P33 must register any centralized entry.


## S7 documentation audit

This P36 bounded read-only block checks ten owned documents, P00 declaration promises, actual CLI
syntax, the frozen plan and unchecked S7 claims. It starts no product process and uses no production
state. P34's installed central T54 is `test/architecture/transfer_state_inventory_test.go: TestTransferDocumentationClaimsHaveReceipts`.
Final P30 coverage and P37 evidence remain pending; an installed predicate does not sign their results. Passing this
local audit signs no S7 product exit. The earlier S5/S6 audits remain separate historical checks.

Run from the repository root; this command is executable now:

```sh
python3 - <<'PY_S7_AUDIT'
from pathlib import Path
path = Path("test/hotupdate/README.md")
body = path.read_text().split("\n<!-- s7-doc-audit:start -->\n", 1)[1].split("\n<!-- s7-doc-audit:end -->", 1)[0]
code = body.split("```python\n", 1)[1].split("\n```", 1)[0]
exec(compile(code, str(path), "exec"))
PY_S7_AUDIT
```

DECLARED means source syntax exists and the documented workspace CLI help was checked, not runtime
continuity or settlement verification. CLI construction moved: status is owned by internal/cli, while
push/pull rate flags and session abandonment remain in cmd/tether. Source discovery cannot sign product claims.

<!-- s7-doc-registry:start -->
| Path | Kind | Symbol |
|---|---|---|
| CLAUDE.md | doc | - |
| docs/distributed-broker-architecture.md | doc | - |
| docs/usage.md | doc | - |
| docs/broker-ops.md | doc | - |
| docs/cluster-runbook.md | doc | - |
| docs/deploy-tier-gotchas.md | doc | - |
| docs/testing-standards.md | doc | - |
| docs/reviews/hot-update-tasklist.md | doc | - |
| test/hotupdate/README.md | doc | - |
| docs/reviews/hot-update-transfer-continuation-plan.md | doc | - |
| internal/xfercontract/state.go | type | WorkerState |
| internal/xfercontract/state.go | type | TrackerState |
| internal/xfercontract/ports.go | type | ObjectWriter |
| internal/xfercontract/contractstub/stub_test.go | test | TestTransferContractSignatures |
<!-- s7-doc-registry:end -->

<!-- s7-cli-registry:start -->
| Feature | Owner | Source | Declaration | Status |
|---|---|---|---|---|
| push | Existing | cmd/tether/transfer.go | use:push | DECLARED |
| pull | Existing | cmd/tether/transfer.go | use:pull | DECLARED |
| transfer | P27 | internal/cli/transfer_command.go | use:transfer | DECLARED |
| status | P27 | internal/cli/transfer_command.go | use:status | DECLARED |
| node | P27 | internal/cli/transfer_command.go | flag:node | DECLARED |
| json | P27 | internal/cli/transfer_command.go | flag:json | DECLARED |
| rate-limit | P27 | cmd/tether/transfer.go | flag:rate-limit | DECLARED |
| abandon-unsettled-transfers | P19/P27 | cmd/tether/session.go | flag:abandon-unsettled-transfers | DECLARED |
<!-- s7-cli-registry:end -->

These are the native drill meanings. Business rc75 and process/test rc1 are separate domains; this table
never translates a control's expected native red to GREEN0. Preserve the full existing DRILL-VERDICT
line grammar from plan §10.6, including all counters, child census and original rc.

<!-- s7-native-verdicts:start -->
| Verdict | Native rc |
|---|---|
| GREEN | 0 |
| ASSERT-FAIL | 1 |
| SETUP-RED | 2 |
| PRODUCT-RED | 3 |
| INCOMPLETE | 4 |
<!-- s7-native-verdicts:end -->


The format promise uses the actual helper at `test/simcluster/lib/assert.sh`; the frozen plan's
`drills/lib/assert.sh` location is historical shorthand, not a live path.

<!-- s7-native-format:start -->
```text
DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s nc_gap=%s nc_guard=%s pass=%s -- %s\n
```
<!-- s7-native-format:end -->

<!-- s7-doc-audit:start -->
```python
# origin: hot-update S7 plan §11.2 / §12.2 P36 / T54
from pathlib import Path
import hashlib
import re

root = Path.cwd().resolve()
assert (root / "go.mod").is_file(), "run the S7 audit from the repository root"
readme = (root / "test/hotupdate/README.md").read_text()

def section(text, label):
    start, end = "<!-- " + label + ":start -->", "<!-- " + label + ":end -->"
    assert text.splitlines().count(start) == text.splitlines().count(end) == 1, \
        "missing or duplicate S7 section: " + label
    return text.split("\n" + start + "\n", 1)[1].split("\n" + end + "\n", 1)[0]

def table(text, columns):
    rows = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        assert len(cells) == columns, "invalid S7 table shape: " + line
        if all(re.fullmatch(r"-+", cell) for cell in cells):
            continue
        rows.append(cells)
    assert len(rows) > 1, "empty S7 table"
    return rows[1:]

expected_docs = {
    "CLAUDE.md", "docs/distributed-broker-architecture.md", "docs/usage.md", "docs/broker-ops.md",
    "docs/cluster-runbook.md", "docs/deploy-tier-gotchas.md", "docs/testing-standards.md",
    "docs/reviews/hot-update-tasklist.md", "test/hotupdate/README.md",
    "docs/reviews/hot-update-transfer-continuation-plan.md",
}
registry = table(section(readme, "s7-doc-registry"), 3)
seen_docs, seen_promises = set(), set()
for path, kind, symbol in registry:
    file = root / path
    assert file.is_file(), "dead S7 documentation/source path: " + path
    if kind == "doc":
        assert symbol == "-" and path not in seen_docs, "invalid/duplicate S7 document"
        seen_docs.add(path)
    else:
        assert kind in {"type", "test"}, "unknown S7 promise kind: " + kind
        key = (path, symbol)
        assert key not in seen_promises, "duplicate S7 source promise"
        seen_promises.add(key)
        pattern = (r"(?m)^type " + re.escape(symbol) + r"\s" if kind == "type" else
                   r"(?m)^func " + re.escape(symbol) + r"\(t \*testing\.T\)")
        assert re.search(pattern, file.read_text()), "dead S7 declaration/test promise: " + path + ": " + symbol
assert seen_docs == expected_docs and len(seen_promises) == 4, "S7 registry membership drift"

plan = (root / "docs/reviews/hot-update-transfer-continuation-plan.md").read_bytes()
if plan.startswith(b"> Documentation pointer: "):
    plan = plan.split(b"\n", 1)[1]
assert hashlib.sha256(plan).hexdigest() == "416907f94ce200d4f5c7d3fa6ea3105310514cffebae8cb9980b7beb453f93ca", \
    "frozen S7 plan changed"

features, pending = set(), []
for feature, owner, path, declaration, status in table(section(readme, "s7-cli-registry"), 5):
    assert feature not in features, "duplicate S7 CLI feature"
    features.add(feature)
    file = root / path
    assert file.is_file(), "dead S7 command source path: " + path
    assert owner and status in {"DECLARED", "NOT_RUN"}, "invalid S7 command status"
    kind, value = declaration.split(":", 1)
    assert kind in {"use", "flag"} and value == feature, "invalid S7 command declaration"
    source = file.read_text()
    pattern = (r'Use:\s*"' + re.escape(value) + r'(?:\s|"|\[)' if kind == "use" else
               r'\.(?:PersistentFlags|Flags)\(\)\.[A-Za-z][A-Za-z0-9]*\([^\n]*"' + re.escape(value) + r'"')
    if status == "DECLARED":
        assert re.search(pattern, source), "dead S7 command promise: " + feature
    else:
        pending.append(feature)
assert features == {"push", "pull", "transfer", "status", "node", "json", "rate-limit", "abandon-unsettled-transfers"}, \
    "S7 command membership drift"

native = {}
for verdict, rc in table(section(readme, "s7-native-verdicts"), 2):
    assert verdict not in native, "duplicate S7 native verdict"
    assert re.fullmatch(r"[0-4]", rc), "invalid S7 native rc shape"
    native[verdict] = int(rc)
assert native == {"GREEN": 0, "ASSERT-FAIL": 1, "SETUP-RED": 2, "PRODUCT-RED": 3, "INCOMPLETE": 4}, \
    "S7 native rc meaning changed"

# Read the real helper rather than promising a dead plan-era path or a simplified grammar.
format_match = re.fullmatch(r"```text\n([^\n]+)\n```", section(readme, "s7-native-format").strip())
assert format_match, "invalid S7 native format promise"
helper = (root / "test/simcluster/lib/assert.sh").read_text()
assert "    printf '" + format_match[1] + "'" in helper, "S7 native verdict format promise differs from real helper"
source_codes = dict((name, int(rc)) for name, rc in
                    re.findall(r"_de_v=([A-Z-]+);\s*_de_rc=(\d+)", helper))
assert source_codes == native, "S7 native verdict codes differ from real helper"

claims = {"B-3-transfer", "B-6-transfer", "E-1", "E-2", "E-3", "E-5", "E-6"}
tasklist = (root / "docs/reviews/hot-update-tasklist.md").read_text()
checks = re.findall(r"(?m)^- \[([ x])\] S7 ([^:]+):", section(tasklist, "s7-claims"))
assert len(checks) == len(claims) and {name for _, name in checks} == claims, "S7 claim membership drift"
rows, candidates = {}, set()
index = root / "docs/reviews/hot-update-transfer-continuation-receipts.md"
for claim, status, source, artifact, running, rc, receipt in table(section(tasklist, "s7-receipts"), 7):
    assert claim not in rows and status in {"NOT_RUN", "COMPLETE"}, "invalid/duplicate S7 receipt row"
    if status == "NOT_RUN":
        assert (source, artifact, running, rc, receipt) == ("-", "-", "-", "-", "-"), \
            "unrun S7 receipt masquerades as evidence: " + claim
    else:
        assert re.fullmatch(r"[0-9a-f]{40}", source), "missing final S7 source SHA: " + claim
        assert re.fullmatch(r"[0-9a-f]{64}", artifact), "missing final S7 artifact SHA: " + claim
        assert re.fullmatch(r"[0-9a-f]{64}", running), "missing actual S7 running SHA: " + claim
        assert rc == "0", "S7 positive aggregate lacks native rc0: " + claim
        assert index.is_file(), "missing P37 final receipt index: " + claim
        link = re.fullmatch(r"\[[^]]+\]\(([^)#]+)(?:#[^)]*)?\)", receipt)
        assert link, "missing P37-reviewed S7 raw receipt link: " + claim
        raw = (root / "docs/reviews" / link[1]).resolve()
        assert raw.is_file() and raw.stat().st_size > 0, "missing/empty S7 raw receipt: " + claim
        assert raw not in {index.resolve(), (root / "docs/reviews/hot-update-tasklist.md").resolve()}, \
            "S7 claim/index cannot be its own raw evidence"
        candidates.add(source)
    rows[claim] = status
assert set(rows) == claims and len(candidates) <= 1, "S7 receipts missing or mix final candidates"
for mark, claim in checks:
    assert mark != "x" or rows[claim] == "COMPLETE", "premature S7 completion without receipt: " + claim
print("S7_DOC_AUDIT_OK: 10 docs, 4 declarations, 8 CLI entries; pending_commands=%s; unsigned_claims=%d" %
      (",".join(sorted(pending)), sum(status != "COMPLETE" for status in rows.values())))
```
<!-- s7-doc-audit:end -->

The audit checks source declarations, table shape/identity and raw file existence, **not raw content,
truth, complete child coverage or freshness**. P37/lead must review all plan §0 receipt fields, actual
source+dirty/artifact/running identities, cuts, native reds/restored greens, oracles and costs before P36
changes any claim to COMPLETE. Different artifacts within the same candidate may serve different roles;
each running SHA must be matched to its own actual artifact by that review. No audit pass waives a missing
P30 child, P34 control, P35 matrix/anchor or native platform receipt.

Mandatory independent on-disk edit/run/restore inputs for T54 (P34 may consume the same sections):

| Defect | Mutation input | Required red oracle |
|---|---|---|
| Dead path | Change one s7-doc-registry path to a nonexistent file | dead S7 documentation/source path |
| Dead Test declaration | Change the P00 Test symbol to a nonexistent name | dead S7 declaration/test promise |
| Dead command | Change DECLARED push to a nonexistent use token, including its feature cell | dead S7 command promise |
| Stale moved command path | Point DECLARED status back at cmd/tether/transfer.go | dead S7 command promise |
| Receiptless completion | Check S7 E-1 without changing its NOT_RUN row | premature S7 completion without receipt |
| Fake COMPLETE | Change E-1 row status alone to COMPLETE | missing final S7 source SHA |
| Lost claim row | Remove a required S7 receipt row | S7 receipts missing or mix final candidates |
| Native red remapped | Change PRODUCT-RED3 to 0 | S7 native rc meaning changed |
| Malformed native code | Change INCOMPLETE4 to 75 | invalid S7 native rc shape |
| Truncated native grammar | Remove `nc_guard=%s` from the s7-native-format record | S7 native verdict format promise differs from real helper |
| Frozen contract drift | Modify a non-pointer byte in the S7 plan | frozen S7 plan changed |

P36 records native rc/failing check/restored baseline for each local input. T54's Go predicate/controls,
complete command golden execution and final receipts remain NOT_RUN pending their owners; the local block is not
registered as a make gate. It adds no mechanical inventory or leak anchor. Global gates, matrix, Darwin,
Docker and deploy drills belong to lead/P37 under L18 and are not run by this package.

## Integrated S8/S9/S10 acceptance (NOT_RUN final receipts)

S8 product paths use target describe→prepare→explicit root stage→same-TxID Start→fresh query/wait.
Legacy node/reload envelopes are refused before dispatch; historical register/release or disk SHA is
not completion. View keeps history separate from current observation and the three receipts. S9 uses
a separate infra component protocol, ordinary auth-first reconnect and pre-repair ACK-prefix gate;
status is infracontract.Status JSON, not the image View or broker's five-field image status.

The installed matrices partition heavy tests by exact top-level names at 600s, preserving whole-package
race coverage as a union. Use the relevant suite tag and exact selector from test/e2e/all_phases_test.go;
new fullstack acceptance/evidencecmd and fullstackharness are separate packages. All spawned processes
reuse shared internal/testharness subreaper/CLOEXEC lease/pidfd supervision, including nested artifact
builds/dispatchers. Panic/timeout/SIGKILL must leave zero descendants by PID+start ticks; cleanup-found
leaks remain failures. Setpgid/Pdeathsig alone cannot prove this. Each resource test has direct shared
goroutine/FD assertions and at least five direct exercise calls, with retirement observed before teardown.

38/39/85/86/99/100 live runs, final-candidate three hard gates/separate gates/whole-race, native Darwin
and S10 aggregate A–H closure are NOT_RUN/PENDING. S9 hot scope is linux/amd64 + systemd/pidfd/cgroup/nft;
Darwin cross-build is independent of native lifecycle. Historical expected PRODUCT-RED3/INCOMPLETE4
remain native reds. Same-source SHA variants prove mechanisms, not real release N-1 quadrants.

[S9 P39](../../docs/reviews/hot-update-infra-nats-caddy-plan.md#p39--最终候选收据与关闭--hard)
owns infra receipts; S10 P34 owns coverage and P35 final candidate validation. Workers run bounded
source/docs audits only; docs audit GREEN does not certify product continuity or close a carry.
