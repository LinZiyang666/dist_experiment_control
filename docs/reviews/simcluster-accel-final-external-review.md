Pass

# simcluster acceleration final external review

> Reviewer: independent external reviewer. Developer responses and internal review conclusions were
> treated as claims until reproduced from implementation, adversarial tests, and primary artifacts.
>
> Scope: the complete round-2 developer tree, both corrected-tree deploy archives, the prior external
> review workflow, and the reviewer remediations left outside the index.

## 0. Conclusion

**Pass the simcluster acceleration/tooling increment after the reviewer remediations in this round.**
No unresolved release-blocking implementation defect remains in the reviewed acceleration, verdict,
signal, cleanup, evidence-capture, or hermetic-gate paths.

This is not a declaration that every product drill is green. Historical minority-write gap `#71`
remains **OPEN**, and load-sensitive deviations such as `#70` remain first-class deviations rather than
being banded away. The release decision is that this increment preserves those reds honestly and no
longer converts ambiguous evidence into a false green.

The developer-submitted tree by itself was not releasable. The new independent gate first reproduced
four deterministic defect classes: stale cause authorization, orphaned signal descendants, replay
blessing an arbitrary cleanup directory, and telemetry failure at zero completed attempts. Direct
archive inspection also disproved the claimed closure of `#71` and the claimed causal root for drill
50. Those defects and evidence overclaims were corrected directly after the developer tree had been
frozen in the index. The same adversarial gate and the complete hermetic gate set now pass.

## 1. Final-round findings and remediation

### Closed 1 — a stale diagnostic could authorize a later unrelated failure

The submitted `_fail_context` searched a ten-line lookback and retained any prior `[simcluster]` or
`[warn]` line. A valid rc=70 diagnostic followed by an unrelated successful line and a later assertion
failure still produced `MATCH-BAND`. That is a false-green authority defect.

`run-drills.sh` now accepts cause input only when the physical line immediately before the first
`[err]` is a cause diagnostic. The independent test separates the old cause and failure with a PASS
line and proves that the result remains a deviation.

### Closed 2 — runner signals killed a wrapper but could leave the drill alive

The submitted handler tracked the `run_one` wrapper PID. TERM could stop that wrapper while its timed
drill descendant survived and continued writing artifacts. Serial retry/attribution attempts were also
outside the same tracking model.

Every drill attempt now runs in a dedicated process group with a recorded run PID. INT, TERM, and HUP
terminate the group, use a recursive fallback for the wrapper tree, wait for reaping, remove tracking
state, return 130/143/129, and never write `RUN-COMPLETE`. Parallel and serial attempts use the same
tracked path. Permanent tests prove the descendant cannot write its delayed marker.

### Closed 3 — replay could manufacture cleanup ownership

The submitted replay path wrote `.simdrills-owned` into an arbitrary directory. A later live invocation
then treated that directory as owned and removed unrelated `*.log` files.

Replay is now read-only with respect to ownership. Live use rejects symlink markers and non-empty
unowned directories, writes an exact versioned marker only for a safe empty directory, and permits a
narrow legacy migration only when the prior completed-run artifacts are present. Root, home, source,
relative/absolute alias, symlink, `..`, interrupted, owned, and unowned cases remain covered.

### Closed 4 — zero-match telemetry could terminate the sampler

`grep -c ... || echo 0` returned two textual zeroes when there were no completion records. Arithmetic
evaluation then failed, silently defeating the telemetry authority for the earliest interval.

The count now suppresses the non-match status without appending output and validates both numeric
inputs. An accelerated fake clock proves a sample with one launched and zero completed attempts is
written and reports one running drill.

### Closed 5 — provisioning cleanup and captured argv were incomplete

`cmd_up` now terminates and reaps the full provisioning process tree on INT/TERM, removes its private
temporary directory on success, partial failure, and signals, and preserves the signal-derived status.
The hermetic fake-Docker cases cover unusual `TMPDIR`, success, failure, INT, TERM, and a descendant that
would otherwise outlive its provisioner.

Captured assertion argv are redacted before persistence while preserving argument boundaries. Separate
and equals-form flags, upper/lower assignments, URI query secrets, spaces, quotes, and multiline values
are pinned. A multiline URI secret is replaced as an entire URI argument instead of leaking through a
line-oriented substitution.

### Closed 6 — acceptance documents upgraded inference into fact

The corrected run-1 archive contains deviations `{20,50,52,74,92,96}` and run-2 contains
`{20,73,74}`. Their union is seven drills, not six, and run-2 drill 73 was outside the response's stated
allowed set.

For drill 96, corrected run 1 records:

```text
pre-heal committer = no
post-heal brk1 handler evidence = no
raw D6b reachability = yes
```

That corrected run did not reproduce the older `#71` post-heal evidence. More importantly, the
pre-heal snapshot and later partition removal are not an atomic boundary, so a post-heal broker line
cannot by itself distinguish a minority commit from a legitimate majority commit delayed across that
boundary. Drill 96 now keeps a pre-heal YES as strong `#65` product-red evidence, labels the ambiguous
boundary as not covered, and does not close historical `#71`.

For drill 50, the concurrent corrected run failed setup at `grow_to_2`; it did not reproduce the older
L3 reader-recovery miss. The solo green retry is useful operational evidence, but it is not a paired
experiment proving that the L3 miss was host contention. The reports and drill evidence no longer make
that causal overclaim.

## 2. Primary-evidence review

- `weilandserver` was inspected directly; both cited corrected-run archives were complete.
- The reviewed remote runner, simcluster entrypoint, drills 50/52/74/96, and expected-verdict log had
  the same SHA-256 content as the developer tree frozen locally.
- Raw logs, rollups, progress records, evidence directories, and the cited solo drill-50 attempt were
  read independently rather than relying on the disposition summary.
- Source tracing confirmed that the broker log is a handler-side commit-success witness; describing
  that broker unconditionally as the raft leader/committer was too strong.
- The disposition, deploy-tier gotchas, task plan, ledger, expected-verdict contracts, and runner
  classification were reconciled. Open product gaps remain visible and cannot authorize an unrelated
  assertion.

## 3. Verification record

- Before reviewer remediation:
  `simcluster-accel-final-review-test.sh` — **FAIL**, 4/4 deterministic adversarial defect classes.
- After reviewer remediation:
  `simcluster-accel-final-review-test.sh` — **PASS**, including stale cause, INT/TERM/HUP descendants,
  replay ownership, zero-completion telemetry, argv redaction, and `cmd_up` success/failure/signals.
- Complete `test/simcluster/tests/run-all.sh` — **PASS**, all 14 named scripts plus
  `kept-sites --check`.
- Both earlier independent external-review gates — **PASS** as part of the complete run.
- Shell syntax for every reviewer-modified shell file — **PASS**.
- `git diff --check` and `git diff --cached --check` — **PASS**.
- `make lint` — **PASS**, `0 issues`.
- `make test` — **PASS**.
- Developer-frozen tree `make e2e` — **PASS** in 659.767 seconds, including D7.

The final reviewer changes are confined to simcluster shell tooling, drills, hermetic tests, and review
documentation; no Go/product implementation changed after that full e2e run. The final shell-specific
complete gate was rerun after all reviewer edits.

## 4. Doubts, limitations, and recommendations

- No additional 20–30 minute deploy-tier sweep was launched after the reviewer-only shell hardening.
  The cited corrected archives exactly match the frozen developer implementation, and every changed
  authority/cleanup branch is now hermetically exercised, but a future scheduled sweep should also
  consume the hardened runner.
- `shellcheck` is not installed on this review host. Interpreter syntax, repository lint, adversarial
  shell gates, unit tests, and the earlier full e2e gate are green.
- The drill-96 observation boundary should eventually be made atomic if the project needs a decisive
  in-sim closure of `#71`; until then, preserve `NOT-COVERED[gap]` rather than inferring a root.
- A future investigation of drill 50 should reproduce the same L3 phase under controlled parallel and
  solo conditions with resource telemetry. A setup-red plus unrelated solo green is not causal proof.

## 5. Index boundary and handoff

Per the final-round instruction, the complete developer response and review baseline were added to the
index before direct remediation. At that freeze point there were no unstaged or untracked files.

The following reviewer changes are intentionally left outside the index:

- runner/simcluster/assertion hardening:
  `run-drills.sh`, `simcluster`, and `lib/assert.sh`;
- evidence corrections: drills 50 and 96, `deploy-tier-gotchas.md`, and
  `simcluster-accel-dispositions.md`;
- permanent gate coverage: `tests/run-all.sh` and
  `tests/simcluster-accel-final-review-test.sh`;
- completed final tasklist and this final report.

The index was not modified after the reviewer remediation began. No commit or push was performed.
