Fail

# simcluster acceleration external review

> Reviewer: independent external reviewer, with no reliance on the main process's verdict.
> Scope: every unstaged/untracked file present at review start on 2026-07-24. The index was empty.
> Method: rough diff inventory → tasklist → source/contract audit → adversarial hermetic tests →
> project gates → direct read-only inspection of the two cited `weilandserver` archives.

## 0. Conclusion

**Fail. This increment is not release-ready.** Four major findings are independently confirmed:

1. nested polls do not preserve the caller's fast/fixed mode, so a nested call changes the outer poll's
   sampling grid;
2. the expectation validator accepts ownerless/unknown-defect bands, duplicate drill rows, and
   prose-only signature mentions;
3. the two post-lever acceptance sweeps do not satisfy the plan's unchanged-deviation-set criterion,
   and several deviations have no legal closed disposition;
4. the implementation/status document claims M3/M5/V4/H3 landed although material committed behavior
   from each requirement is absent or contradictory.

The shipped 12 simcluster gates all pass, but the independent adversarial gate has **five RED cases**.
`make lint` passes. A second `make test` passes after one load-sensitive first-run failure. `make e2e`
itself fails once in p13 even though the exact case passes three focused reruns. A clean focused rerun
does not turn a failed release command into a green release gate.

No reviewed change deletes an aggregate assertion-family call or converts a non-GREEN verdict to
success. Drills 20/91's `--reset-js` repair and store-move oracle are faithful, and drill 90 retains all
72 product invocations. Those positive results do not offset the blockers below.

## 1. Findings

### Major 1 — nested poll frames omit `mode`; an inner poll changes the outer sampling contract

`lib/log.sh:45-52` stores only `end|timeout|interval|desc` in each frame. `_poll_impl` assigns mode to
the process-global `_pu_mode` at `lib/log.sh:77`, while the sleep decision reads that global at
`lib/log.sh:120`. `_pu_peek` restores every other field but not mode. Therefore:

- outer `poll_until_fixed` + inner `poll_until` becomes fast after the inner call;
- outer `poll_until` + inner `poll_until_fixed` becomes fixed after the inner call.

The independent virtual-clock regression at
`tests/simcluster-accel-external-review-test.sh:11-43` produces:

```text
outer fixed grid was corrupted: sleeps=1,1,1,3, calls=5 (want 3,3, / 3)
FAIL an inner fast poll flips the outer fixed poll
```

A real-clock probe also made a nominal fixed `4s/3s` outer poll execute five predicate calls and sleep
for six seconds instead of staying on its fixed grid. Existing `poll-reentrancy-test` exercises only
same-mode nesting; `poll-mode-test.sh:67-78` exercises the two modes only as separate top-level calls.

This is not merely a timing optimization bug. `poll_until_fixed` is the declared fidelity boundary for
effectful and stability-window predicates (`lib/log.sh:70-73`). Losing that boundary can issue extra
product mutations or bank a transiently true state. Store mode in each frame and add both cross-mode
nesting directions to the permanent gate.

The claimed exemption audit is also incomplete. The gate enumerates eight hand-maintained sites
(`tests/poll-mode-test.sh:44-53`) but says the set is gate-enforced. At least these unlisted plain-fast
predicates execute product actions on every sample:

- `drills/22-forcesingle-online.sh:163`: `_sra_ok` invokes `set-raft-addr`;
- `drills/82-agent-onboarding-invite.sh:184`: invokes `agent config refresh --once`;
- `drills/92-js503-remote-alert.sh:156`: invokes `login`.

The test can prove that listed sites remain fixed; it cannot prove that the list is complete. Replace
the hand-maintained completeness claim with a mechanically classified inventory or explicitly justify
and pin every effectful site.

### Major 2 — verdict-table validation is fail-open on four band/table authority boundaries

`tests/validate-verdicts.sh` reads columns 1, 2, 3, 4 and 6 at `:45-55`, but never reads or validates the
owner column. It accepts defect-shaped strings at `:88-91` and checks only whether a named defect is
closed at `:119-133`; it never proves the defect exists. It checks a signature using a free-form
substring search at `:112-116`, although runtime resolution requires an exact
`sig:<slug> := <ERE>` definition (`run-drills.sh:389-395`). Finally, its two-way drill/table comparison
at `:136-151` does not reject duplicate drill rows, while `_exp_field` silently uses the first row
(`run-drills.sh:384-386`).

The independent mutation test proves all four malformed states currently return validator rc=0:

```text
FAIL a band cannot be ownerless (validator rc=0)
FAIL a band must name a defect that exists in the ledger (validator rc=0)
FAIL duplicate drill rows are forbidden (validator rc=0)
FAIL a prose mention is not a signature definition (validator rc=0)
```

The first two defects directly permit an unsupported band to produce `MATCH-BAND`; the duplicate case
makes table authority depend silently on row order; the signature mismatch makes validation disagree
with runtime. This contradicts the plan's “zero signature-less or owner-less bands
(validator-enforced)” acceptance rule (`simcluster-accel-plan.md:435-437`).

Required remediation:

- reject empty/`-` owner for every non-GREEN expectation and every band, and enforce the intended
  owner/defect relationship;
- parse ledger headings into an exact open-ID set and require membership;
- reject duplicate drill IDs before consumers run;
- parse signature definitions with the same grammar as `_sig_regex`, require exactly one definition,
  and validate the ERE;
- tighten defect-ID syntax (`'#'[0-9]*` is a shell glob, not an exact numeric grammar).

The TSV split itself is sound: all 38 rows, expected values, owners, and prose history migrated without
loss. The defects are in the new authority checks, not the data migration.

### Major 3 — post-lever evidence fails the plan's acceptance gate; “all deferred phases closed” is false

The plan requires two consecutive post-lever sweeps whose deviation set is unchanged versus B1 except
the repaired `c6b9c9e` rows (`simcluster-accel-plan.md:438-441`). Directly reading the cited server
archives confirms:

- run 1: `{30,52,74,96}`;
- run 2: `{30,50,52,96}`.

The disposition document itself records this shift at
`simcluster-accel-dispositions.md:13-27`. Relative to the earlier set `{20,30,52,74,91}`, repairing
20/91 does not explain the new 96, the run-2 50, or 74's disappearance. The assertion that a lever bug
“would red the SAME drills every run” (`:23-27`) is logically invalid: timing/scheduling bugs are
precisely capable of shifting between drills.

There are also unclosed deviations:

- run-2 drill 50 has no disposition section at all. Its archived log ends INCOMPLETE after a
  180-second history-reader recovery runtime guard;
- drill 52 exposes a genuine `not leader` product UX gap (`:57-73`) but no `[GAP #N]`/pin was minted,
  despite the plan requiring fixture fix, minted defect+pin, or owned signature band;
- drill 30 is called closed while deliberately retaining an unbanded, variable first-class deviation
  (`:41-55`);
- drill 74 is `REGRESSION` in main and solo at a new assertion (`:86-105`) and explicitly deferred to a
  follow-up, not closed.

Accordingly, `simcluster-accel-dispositions.md:145-155` and
`simcluster-accel-plan.md:500-510` overstate closure. A `LOAD-SENSITIVE` label is additive attribution,
not an acquittal; the plan itself says it remains a blocker until written legal disposition.

Do not update the baseline from these sweeps. First resolve or formally pin every new deviation, then
run two consecutive acceptance sweeps on the corrected tree and compare exact sets.

### Major 4 — implementation status claims requirements that did not land

The plan's status line says P0.1, M1–M5, V1/V2/V4/V5/V6 and H3 landed
(`simcluster-accel-plan.md:466-470`). Material requirements are absent:

- **M3 telemetry:** the plan requires a same-filesystem fsync preflight that infra-aborts on failure,
  a 60-second background `host-telemetry.tsv` sampler, and per-assertion elapsed stamps
  (`:237-242`). The runner only takes one idle p50/p99 probe; unavailable probe results become `- -`,
  a different filesystem is advisory, and no `host-telemetry.tsv` exists
  (`run-drills.sh:450-482,635-637`).
- **M5 preflight:** the plan explicitly includes `net.ipv4.neigh.default.gc_thresh3` and says
  check-and-refuse (`simcluster-accel-plan.md:270-277`). The implementation omits that counter, skips
  unreadable values, and unconditionally returns 0 for the extended table
  (`run-drills.sh:211-260`). Its own comments say both “CHECK AND REFUSE” and “REPORT-not-refuse”.
- **V4 agent join:** the plan requires replacing fixed `timeout 6` with a poll on persisted bind state
  (`simcluster-accel-plan.md:357-364`). `simcluster:438` still runs exactly `timeout 6 ... || true`;
  only the later ONLINE state is polled.
- **H3:** the plan requires classifying all 36 sleeps and adding a lint rule
  (`simcluster-accel-plan.md:349-355`). Status explicitly admits the lint/tag sweep was not done while
  still calling H3 landed (`:490-492`).

The same document later says these phases are “Not yet done” at `:566-567`, contradicting its earlier
landed/closed sections. Either implement the accepted plan or revise the plan/status and acceptance
criteria through an explicit reviewed decision. A status paragraph cannot silently narrow a committed
requirement after implementation.

### Medium 5 — `SIM_IMAGE_CHECKED=1` is a user-controlled bypass of the stale-image safety gate

`simcluster:593-601` trusts an ambient boolean. This command bypasses the guard:

```sh
SIM_IMAGE_CHECKED=1 ./simcluster drill 00-skeleton
```

The comment warning users not to export it does not make the boundary fail-closed. It also creates a
TOCTOU window: vendor/image state can change after the runner's one check. Use a runner-created,
unforgeable-per-sweep token or a private wrapper/file descriptor, and bind the attestation to both
hashes. Direct drill calls must never be able to opt themselves out with an environment value.

### Medium 6 — the flight recorder persists full argv/output without private permissions or redaction

`lib/assert.sh:148-176` appends full command argv and output. The runner creates `$LOGDIR` and
`$LOGDIR/evidence` without setting a restrictive umask or modes (`run-drills.sh:264-269`). On the
normal `022` umask this yields world-readable directories/files on a shared sim host. Drill argv and
output routinely include session PINs, tokens, invite material, node identifiers, and confirmation
environment values.

Create the log root as 0700 and evidence/log files as 0600, redact known secret-bearing flags/env, and
document archive retention. The flight recorder is useful, but full evidence should not silently
become a credential archive.

### Medium 7 — `--logdir /` remains a destructive cleanup target; replay also mutates its archive

The new guard rejects only an empty logdir (`run-drills.sh:153-159`). A literal `/` passes, after which
the live cleanup attempts to delete root-level `/*.log`, `/*.rc`, `/*.timeout`, `/*.secs`,
`/rollup.*`, `/progress.tsv`, and recursively `/evidence` (`:264-268`). This is especially dangerous
if a diagnostic run is launched with elevated privileges.

Canonicalize and reject `/`, the repository root, home, and any non-dedicated existing directory;
prefer creating a fresh run directory with an ownership/sentinel check before cleanup. Also, the
documented “replay” path deletes and rewrites `rollup.txt/tsv` at `:270-272`; describe it as
non-executing but artifact-mutating, or write derived output elsewhere so an archive remains immutable.

### Low 8 — parallel provisioning temp state is predictable and not signal-cleaned

`simcluster:128-142` uses `${TMPDIR}/simup.$INSTANCE.$$`, not `mktemp -d`, and installs no trap around
the provisioning phase. An interrupt can leave logs/rc files behind; a local same-host actor can
pre-create the predictable path. Use `mktemp -d`, validate the resulting path, and trap cleanup while
preserving the existing drill-level teardown behavior.

## 2. Doubts and limitations

- I did not launch a fourth 20–30 minute full deploy-tier sweep after deterministic local blockers were
  established. The server was initially offline, later returned ONLINE; I then inspected both cited
  `-j 6` archives directly via `tether exec`, including rollups and the relevant 50/74 logs. A new sweep
  on unchanged code would not resolve the acceptance or validator defects.
- The archive proves deviation-set instability, not whether each red is caused by a particular speed
  lever. The available telemetry is insufficient to make the document's strong causality claim.
- The parallel `cmd_up` success path looks structurally sound and multiple archived drills exercised
  it, but there is no deploy-tier failure-injection proving cleanup after interrupt/partial provision.
- `make test` and p13 both show load-sensitive deadline behavior. Focused reruns reduce confidence that
  these are stable product regressions, but they do not provide a clean first-pass release-gate record.

## 3. Verification record

- `sh test/simcluster/tests/run-all.sh`: PASS, 12 shipped gates plus kept-sites.
- `sh test/simcluster/tests/simcluster-accel-external-review-test.sh`: **FAIL, 5/5 adversarial cases**.
- Shell syntax: PASS under `sh -n`, `dash -n`, and `bash -n` as applicable.
- `shellcheck`: unavailable on this host.
- `make lint`: PASS, `0 issues`.
- `make test`:
  - attempt 1: FAIL at `internal/agent.TestRebuildNoGoroutineLeak`;
  - exact test `-count=3`: PASS;
  - attempt 2: PASS.
- `make e2e`: **FAIL** after 665 seconds at
  `TestProxyDisableDuringTunnelDropStaysDown` (`lab-1` not ONLINE within 3 seconds); every other matrix
  completed green. Exact p13 test `-count=3`: PASS.
- `weilandserver`: ONLINE at final check. Cited run-1 and run-2 rollups were read directly and match the
  deviation sets reported above.
- Migration inventory: 38/38 rows and prose sections preserved.
- Assertion inventory: no aggregate deletion (`assert_ok` 1114→1116, `assert_setup` 176→177,
  `assert_refuses` 98→98, `assert_bug` 4→4, `product_red` 28→28, `not_covered` 92→93).

## 4. Required disposition before re-review

1. Fix cross-mode poll frames and add permanent nested fast↔fixed tests.
2. Make validator ownership, ledger membership, uniqueness, and signature-definition checks fail closed;
   add the independent RED cases to the normal gate.
3. Reconcile M3/M5/V4/H3 implementation with the accepted plan and remove contradictory status claims.
4. Legally dispose 30/50/52/74/96, then produce two consecutive post-fix sweeps meeting the exact-set
   acceptance criterion.
5. Remove the ambient image-check bypass, secure evidence permissions/redaction, and harden logdir
   cleanup.
6. Obtain clean `make test`, `make e2e`, `make lint`, shipped simcluster gates, and independent gates on
   the final tree.
