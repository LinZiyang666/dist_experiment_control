# Pass — S2 external re-review round 3

Date: 2026-07-12

Round-2's **1 Major and 1 Minor are closed**. VOTER failures are no longer automatically retried without
concurrency context, genuine infrastructure retries preserve first-run evidence, and all assertion ledgers now
agree with the live 42/40/29 verdicts. No new release-blocking finding was found.

## Closure matrix

| Round-2 item | Result | Independent evidence |
|---|---|---|
| R2-F1 — concurrency-blind VOTER retry + erased evidence | Closed | VOTER success, timeout, and `INCOMPLETE` strings no longer match `FLAKE_SIG`; systemd/container infrastructure signatures still match. A fake-runner retry preserved failing `.attempt1.log/.rc`, wrote a separate GREEN retry log, and marked `(retried)` plus the evidence path in the summary. |
| R2-F2 — stale assertion counts | Closed | Source count is 80=42, 81=40, 82=33 static; the four conditional U-arm assertions do not execute on the measured NOT-COVERED path, yielding live 82=29. README and inventory consistently record live 42/40/29 and explain the conditional discrepancy. |
| Arm R event scoping recommendation | Closed | `_ev_pinfailed_ge10` now binds `type=pin_failed`, `sid=lab`, and `role=ctl` on each counted line. Live 80 passed the strengthened count. |
| Obsolete plan recipe recommendation | Closed | The executable table now contains R-sub/R-fails/R-pinfailed/R-11th only; the superseded ten-correct-PIN warmup rows are gone. |

## Independent verification

- Rebuilt the staged-vs-unstaged boundary: six changed files, all under `docs/` or `test/simcluster/`; no product
  Go implementation diff.
- Synthetic flake controls: healthy VOTER success, VOTER timeout, and `INCOMPLETE — did not reach VOTER` all
  remained non-flakes; `systemd never came up` and `container not running` remained retryable.
- Ran `run-drills.sh` against a temporary fake simcluster: first attempt failed with the systemd infra signature,
  retry passed, original log/rc survived as `.attempt1.*`, current log/rc represented attempt 2, and summary
  disclosed the retry and evidence location. Temporary files were removed.
- Mechanical assertion counts: 80=42, 81=40, 82=33; inspection confirmed U1-U4 are conditional and the measured
  no-user-manager path legitimately reports live 82=29.
- Shell syntax and `git diff --check` passed. Focused command-tree/security tests passed; `make lint` reported
  0 issues.
- Live `weilandserver`, `--no-retry`: strengthened drill 80 passed 42/42 in 61s. R-fails, scoped R-pinfailed,
  and R-11th all passed. No throwaway container remained.
- Full `make test`/`make e2e` were not repeated: round 1 completed both against the same staged product tree;
  rounds 2-3 contain only shell/docs/comment changes with zero Go diff.

## Residual notes

- Evidence preservation uses best-effort `cp ... || true`. Under normal runner ownership this is reliable and
  was independently exercised; if the log filesystem is full or becomes unwritable, preservation can still
  fail. A future hardening change may fail the retry closed when copying attempt 1 fails.
- The remaining generic `is not running` flake alternative predates this closure and is intended to catch the
  Docker daemon's container error. Tightening it to the full Docker message would further reduce accidental
  matches in future drill prose.

## Release recommendation

**Release S2.** The three external-review rounds have closed all release-blocking findings. Proceed with the
project's stage/commit/push workflow while retaining the explicit NOT-COVERED boundaries for systemd-user and
the six-minute roster-stale path.
