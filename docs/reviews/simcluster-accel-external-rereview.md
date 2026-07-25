Fail

# simcluster acceleration external re-review

> Reviewer: independent external reviewer; the developer response is treated as a claim, not as
> evidence.
> Scope: the developer's unstaged response/fixes layered over the first external-review index, plus
> the release state those fixes claim to establish.
> Method: re-run the first adversarial gate → diff/contract audit → new cause-separating mutations →
> project gates → direct read-only inspection of the cited `weilandserver` archives.

## 0. Conclusion

**Fail. Do not release this increment yet.** The response closes the first review's poll-frame defect
and most of the verdict-validator mutations, and it honestly amends several plan requirements. It does
not close the review as a whole:

1. the new drill-74 `#67` band matches the invariant assertion title, so an unrelated product failure
   at that assertion is laundered as `MATCH-BAND(#67)`;
2. the telemetry signal trap returns to the runner after TERM/HUP/INT, allowing an interrupted sweep to
   write a false `RUN-COMPLETE` sentinel; its documented `running_drills` field is actually a cumulative
   completed-rc-file count;
3. the deploy-tier evidence still contains an unresolved possible minority-write/drill-attribution
   defect (`#71`) and an unexplained drill-50 recovery miss, while the disposition nevertheless says
   the acceleration is clean of lever regressions.

The new independent re-review test has **five deterministic RED cases**. The full hermetic simcluster
suite, `make lint`, and `make test` pass, but `make e2e` fails its release invocation at D7; the exact D7
case then passes three focused repetitions. That focused result makes a load flake plausible, but does
not make the recorded release gate green.

## 1. Findings

### Major 1 — the new `#67` band distinguishes an assertion location, not the defect's root cause

Drill 74 suppresses the actual `expose` output and retains only its rc
(`drills/74-rebalance-on-return.sh:425-427`). It then asserts a shell comparison whose failure line is
always:

```text
[err ] FAIL B-negctrl-create ordinary expose reg create command succeeded (rc=0) ...
```

The registered signature is only that fixed title
(`expected-verdicts-log.md:166`), and the table authorizes
`ASSERT-FAIL@#67@sig:b-negctrl-create` (`expected-verdicts.tsv:42`). Runtime classification matches the
first `[err ]` line, not the preceding free-form log (`run-drills.sh:420-453`). Therefore the claimed
“specific” signature cannot tell rc=70/JetStream tier-B unavailability from permission denied, malformed
arguments, a missing binary, or any other failure at this command.

The independent mutation replaces the recorded rc=70 cause with rc=64/permission denied while preserving
the assertion. The runner still returns `MATCH-BAND(#67)`. This directly contradicts the legal-closure
claim in `simcluster-accel-dispositions.md:58-65`.

This is a release blocker because it recreates the exact laundering class the band mechanism is meant
to prevent. Either capture the command's stderr and make the first-failure signature identify the
`#67` cause, or remove the band. A fixed assertion label is not a defect signature.

### Major 2 — TERM/HUP/INT can forge sweep completion; M3 telemetry semantics are false

The telemetry cleanup trap is installed for `EXIT INT TERM HUP`, but its handler only kills/waits for
the sampler and does not exit or set an interrupted state (`run-drills.sh:526-538`). After a signal,
execution can continue through the drill waits and summary, where every live run unconditionally
writes `RUN-COMPLETE` (`run-drills.sh:872-877`).

The independent signal test starts a five-second drill, sends TERM to the runner after one second, and
observes that the runner exits early **with `RUN-COMPLETE` present**. A reader is then told the sweep
finished even though the selected drill did not finish. This invalidates the sentinel's stated
authority and can turn a partial archive into a settled acceptance result.

M3 is also not recording its declared schema. The plan says column 3 is `running_drills`
(`simcluster-accel-plan.md:589-595`), while the sampler computes it as the number of `*.rc` files
(`run-drills.sh:531`), which is the cumulative number of completed drill attempts. It never decreases
and is not the number running.

Signal handlers must terminate with a non-zero signal-derived status after cleanup, prevent sentinel
emission, and reap/terminate drill children. Track live drill PIDs (or derive launched minus completed)
for the telemetry field and add a permanent TERM/HUP regression.

### Major 3 — the corrected acceptance story still claims more than the evidence establishes

The rewritten disposition now correctly admits that the original exact-set acceptance criterion is not
met: the two `-j 6` deviation sets are `{30,52,74,96}` and `{30,50,52,96}`
(`simcluster-accel-dispositions.md:15-31,99-121`). Direct inspection of the cited server archives agrees.
An explicit criterion amendment is reviewable; it is not retroactive proof that the observed reds are
safe.

Two entries prevent the amended “every deviation attributed and legally disposed” conclusion:

- drill 96's run-1 evidence claims the isolated minority committed `canary3`; the disposition itself
  says this is either a genuine minority-write durability defect or a drill-attribution bug and remains
  under investigation as `#71` (`:67-78`);
- drill 50 missed history-reader recovery under load. Calling its runtime guard “honest” proves only
  that the arm did not materialize; it does not prove the miss is unrelated to the new concurrency
  regime (`:81-88`).

Nevertheless, the document concludes that the acceleration is “clean of lever regressions”
(`:123-126`). That conclusion does not follow while `#71` has two materially different possible roots
and drill 50 has no causal evidence. There is also no new deploy-tier sweep of the final corrected tree:
the cited archives predate the response's poll, band, telemetry, and runner changes.

Resolve `#71`, establish the drill-50 root, correct the deterministic runner/band defects, and then
produce acceptance evidence from the corrected tree. Until then, retain the measured speedup as a
performance result but do not label it release-clean.

### Medium 4 — `--image-prechecked` remains a public, forgeable bypass

`cmd_drill` skips `check_image_or_die` whenever the supplied flag equals
`sha256sum vendor/tether` (`simcluster:592-610`). That hash is public workspace state, not evidence that
the image was checked. A direct caller can compute it and run:

```sh
./simcluster drill <name> --image-prechecked "$(sha256sum vendor/tether | awk '{print $1}')"
```

The independent test does exactly this with no usable image and a vendor binary that cannot exist in an
image; the drill succeeds without any image inspection. Moving the bypass from an environment variable
to a command-line value prevents accidental inheritance but does not establish provenance. Bind the
optimization to runner-created state that records the actually inspected image identity, or remove the
public bypass.

### Medium 5 — a non-existent `--logdir` alias can delete files in an existing parent

Canonicalization falls back to the raw string when `cd "$LOGDIR"` fails
(`run-drills.sh:157-167`). A path such as `victim/new/..` is therefore not recognized as `victim`; after
`mkdir -p`, shell path resolution targets the parent and startup cleanup deletes its `*.log`, `*.rc`,
and evidence files (`:286-291`).

The independent temp-only test places `victim/keep.log`, runs with `--logdir victim/new/..`, and observes
that `keep.log` is deleted. Existing arbitrary directories are also accepted without an ownership
sentinel. Canonicalize after safely creating a dedicated directory, reject unresolved `..`/symlink
aliases, and require a runner-owned marker before cleanup.

### Medium 6 — validator and runtime still do not share one signature-slug grammar

The validator accepts any non-empty `sig:?*` slug and interpolates it unescaped into grep/sed
(`tests/validate-verdicts.sh:101-104,122-138`). Runtime also interpolates the slug into sed
(`run-drills.sh:420-423`). A definition such as `sig:x/y := exact-failure` is accepted by the validator,
but the slash terminates the runtime sed expression, so the signature resolves to empty and can never
match.

Use one literal safe slug grammar in both places, for example `[A-Za-z0-9][A-Za-z0-9._-]*`, and pin
metacharacter mutations in the permanent validator self-test.

### Low 7 — cleanup/redaction hardening is improved but not fully signal- or token-complete

- `cmd_up` now uses `mktemp -d`, but its INT/TERM trap only removes the directory and returns
  (`simcluster:128-149`). Like the runner trap, it does not explicitly stop provisioning or exit with a
  signal status; background provisioners can continue after the path is removed.
- evidence permissions are private, but `_as_redact` masks only flag values and uppercase assignment
  forms (`lib/assert.sh:148-156`). It does not mask lower-case URI query material such as
  `tether-invite:v1?pin=...`, even though drill 82 passes full invite URIs in captured argv.

These are not the primary release blockers, but the response should narrow its “fixed” claim or extend
the signal/redaction tests.

## 2. What the response did close

- Poll mode is now stored/restored in each nested frame; both fast→fixed and fixed→fast regressions pass.
- Drills 22, 82, and 92 use fixed polling at the previously unlisted effectful sites. The new mechanical
  matcher is explicitly described as narrow rather than a complete shell semantic analysis.
- The first review's ownerless band, unknown defect, duplicate row, prose-only signature, invalid ERE,
  and exact defect-ID mutations are rejected.
- The drill-52 `#69` signature includes the observed `not leader` cause and distinguishes a different
  failure materially better than the drill-74 band.
- M5, V4, and H3 scope changes are now explicit plan amendments instead of contradictory landed claims.
- Log/evidence directories use restrictive permissions, and provisioning temp names are unpredictable.

## 3. Doubts and limitations

- I did not launch another 20–30 minute deploy-tier sweep after deterministic local authority defects
  were reproduced. A new sweep on the same code cannot validate a root-insensitive band or a forged
  completion sentinel.
- The server archives preserve the drill-96 product-red statement but not the raw broker-log match used
  to attribute the commit; that missing primary artifact is one reason `#71` cannot be independently
  resolved from the archive.
- The focused D7 rerun reduces confidence that the `make e2e` failure is a repeatable product defect,
  but the complete gate was not green on this reviewed tree.
- `shellcheck` is not installed on the review host.

## 4. Verification record

- First-review independent gate:
  `sh test/simcluster/tests/simcluster-accel-external-review-test.sh` — PASS, 5/5.
- Full simcluster hermetic gate: `sh test/simcluster/tests/run-all.sh` — PASS, all 13 tests plus
  kept-sites.
- Re-review independent gate:
  `sh test/simcluster/tests/simcluster-accel-external-rereview-test.sh` — **FAIL, 5/5 deterministic
  adversarial cases**:
  root-insensitive `#67` band, forgeable image precheck, unsafe logdir alias, invalid signature slug,
  and forged completion after TERM.
- Shell syntax under `sh -n`, `dash -n`, and `bash -n` as applicable — PASS.
- `make lint` — PASS, `0 issues`.
- `make test` — PASS.
- `make e2e` — **FAIL** after 672.5 seconds at
  `TestD7Matrix/DrainRefusesRebuildOff` while seeding node 2 (`node is not the leader`); all other
  reported matrices passed.
- Exact D7 case with race detector and `-count=3` — PASS, 3/3.
- `weilandserver` — ONLINE during review. Run-1/run-2/pre-lever rollups and drill 52/74/96 logs/evidence
  were inspected directly and agree with the sets quoted above.

## 5. Required before another re-review

1. Remove or root-bind drill 74's `#67` band and permanently test a different cause at the same
   assertion.
2. Make runner signals terminate partial sweeps without `RUN-COMPLETE`; fix `running_drills` telemetry.
3. Resolve `#71` and drill 50 before claiming the speed levers are release-clean; run acceptance on the
   corrected tree.
4. Replace the forgeable image bypass, make logdir cleanup ownership-safe, and unify signature-slug
   grammar.
5. Obtain green simcluster, lint, unit, e2e, and independent gates on the final tree.
