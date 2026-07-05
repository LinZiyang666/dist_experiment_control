# simcluster grow-honest rework — Stage C internal review + main-process adjudication

Stage C = 6-expert adversarial workflow (`wf_301cb8bf-56a`, all Opus 4.8) reviewing the reworked
`cmd_grow` + the two new drills + the provision #22 fix, against the sim Mandate + correctness. Lenses:
mandate-honesty · grow-correctness-regression · drill-false-green · 22-fidelity-fix ·
completeness-defer-honesty · adversarial-break. All returned `material-issues` except
grow-correctness-regression (`minor-fixes`). Below: every finding, my ruling, and the fix applied.
Main-process rule (§3 step 5): only the main process edits implementation; experts review + propose.

## BLOCKER — adopted + fixed

- **provision forced `chown root:root /etc/tether` MASKED the #22 product FIX.** The rework swung the
  bug from "always tether-owned (masks the bug)" to "always root-owned (masks the fix)": when install.sh
  is fixed to chown ETC to tether, provision would clobber it back → drill 13 stays RED forever, the
  `assert_bug` "APPEARS FIXED → promote" contract defeated.
  **Investigation overturned my own premise:** an empty docker named volume mounts **root:root** (verified
  on the server), and install.sh never chowns ETC — so `/etc/tether` is **naturally root-owned**; the
  `999:997` I had blamed on the volume was a **stale BAKED image** (an old provision that chowned ETC,
  from before that chown was removed, never rebuilt). **Fix:** REMOVED the forced chown entirely
  (`provision-node.sh`) — `/etc/tether` is naturally root-owned, #22 reproduces, and a #22-fixed install.sh
  now makes ETC tether-owned → drill 13 flips. Verified on the server: fresh `up` → `/etc/tether` = root,
  grow still reaches VOTER, #22 still reproduces. The `doctor` tripwire remains the guard for a masking
  regression. README + gotchas #22 correction rewritten (my earlier docker-volume-uid-999 theory was
  wrong).

## MAJOR — trailer honesty (flagged by 4 lenses) — adopted + fixed

- **Over-reports #3/#22** — grow's mesh render runs `reconcile nats --manual` as ROOT with explicit
  `--peer`, triggering NEITHER #3 (harvest) NOR #22 (perm-deny); and #3/#22 are mutually exclusive per
  grow, so the first-grow trailer `…#22…#3…` was self-contradictory. **Fix:** gate #22 to grow-ordinal
  ≥2 only (`_n_before -ge 2`); #3 stays first-grow-only. The `[workaround #22/#3]` label + drill 13 carry
  the rationale; the trailer no longer lists both.
- **Under-reports #5/#24** — grow works around #5 (pty confirm) and #24 (SAN certs) on every grow but
  never listed them. **Fix:** append `#24` after the SAN mint and `#5` after the pty init.
- **broker.yaml seam mislabeled `[env]`** — it is `cluster init`'s printed-but-not-applied step 3
  (`cluster.go:808`). **Fix:** relabeled `[workaround: init prints the seam but does not apply it]`.
- **Trailer only checked for `#8`** in drill 11. **Fix:** drill 11 now asserts the FULL first-grow set
  (`#I1,#24,#5,#8,#10,#3,#4,#23`) token-by-token AND asserts `#22` is ABSENT on the first grow — so
  dropping ANY workaround (a product fix) flips the drill.

## MAJOR — drill rigor — adopted + fixed

- **Drill 13 over-attributed to the in-broker reconciler** — it runs a manual `reconcile nats --manual`
  as tether (a syscall proxy sharing the same `natsconf.Apply` write), never the auto reconciler firing
  (which grow's root render front-runs). **Fix:** retitled to state the honest scope (reproduces the
  perm-denied WRITE the in-broker reconciler SHARES, not the auto non-convergence), in the header +
  `doctor` + README.
- **Drill 13 `.bak` signature gap** — `natsconf.Apply` writes a pristine `.bak` BEFORE the temp when none
  exists; on a pristine ETC the FIRST perm-deny is the `.bak` (`natsconf: write .bak:`), matching neither
  signature alt → HARD-FAIL. Passed today only because init/grow pre-create a `.bak` as root. **Fix:**
  broadened the signature to `natsconf: *(temp|write \.bak):.*permission denied|…`.
- **Drill 13 fragile to a nats-server bump** — the CreateTemp perm-deny is gated behind `nats-server -t`
  DryRun, which a version/conf-strictness change could reject with a non-#22 error → HARD-FAIL. **Fix:**
  added `--skip-dry-run` (verified real flag, `cluster_natsconf.go:57`) to go straight to the write.
- **Drill 13 roster-race + weak args control** — the `--manual` peer/roster check could refuse for a
  non-#22 reason if brk2's VOTER status races; the arg control only checked non-emptiness. **Fix:** added
  a "brk2 is VOTER" poll control + an nkey-shape control (account `A…`, broker `U…`, peer `,U…`).
- **11's #I1 used `assert_bug` on a PERMANENT invariant** — the serve refusal ("never auto-bootstraps") is
  a fail-closed guarantee tether KEEPS; `assert_bug` would celebrate its REMOVAL (a safety regression) as
  "APPEARS FIXED". **Fix:** changed to `assert_refuses`; distribute brk3 secrets first so the refusal is
  isolated to the missing raft state (verified `DetectClusterMode` precedes `SecretsPreflight`,
  `serve.go:127`).
- **11's #8 keyed a phantom token** — grep included `catch-up exceeded`, never emitted by `waitForOp`
  (only `still in flight` / `is BLOCKED`). **Fix:** dropped the phantom in grow + drill 11.

## MAJOR/minor — grow correctness — adopted + fixed

- **First-grow former-N1 recovery deadlock** — the pre-poll `start` (former-N1 leader) lacked the
  `reset-failed` the post-poll recovery has; if the leader trips StartLimit during the JS-reset nats-down
  window, `start` is refused → its `driveJoin` stays dead → the joiner never reaches VOTER → the post-poll
  recovery (gated inside the VOTER-success branch) is unreachable. **Fix:** added `reset-failed` before the
  pre-poll `start` (mirrors the post-poll recovery).
- **#8 elif keyed on `SERVING`** (always printed in the `(driving to SERVING)` preamble), misclassifying a
  terminal failure as success. **Fix:** elif now keys on `operation .* reached|is now a VOTER|^approved`.

## Deferred with rationale (adopted the reasoning, not a new drill this pass)

- **#23 defer JUSTIFIED with new evidence** — the reviewer challenged the defer (long-outage recipe never
  tried). I ran it: a long N=1 outage (`stop nats; dwell 8s`) leaves the broker **active**
  (`MaxReconnects(-1)` reconnects); a follower nats restart yields an exit-70 crash that
  `Restart=on-failure` revives. NEITHER reproduces the clean-exit-0 strand → confirms gotcha #23's
  "trigger unlocated". Documented both recipes in the README; grow labels + recovers the #10-adjacent
  crash it does hit.
- **#10, #5, #24, #4 drills deferred** — grow LABELS + trailer-names all four, and drill 11's full-trailer
  assertion is the interim regression guard (dropping any label flips it). Added a "Gaps LABELED but NOT
  yet signature-pinned (regression-unprotected)" subsection to the README so the coverage gap is
  transparent (the reviewer's completeness finding). The three "cheap" ones (#10/#5/#24) are good
  follow-on drills; not blocking this rework.
- **#6 dodge documented** — the sim standardizes on `User=tether` for `cluster init` (dodging the
  root-owned `tether.lock`); documented as deliberately out of scope in the README rather than left silent.
- **config_load_time DEGRADED reclassified** — from "not attributed to tether" to a candidate tether
  OBSERVABILITY gap (the observer keys on `/varz config_load_time` which a byte-identical SIGHUP never
  advances); noted in the README.
- **BLOCKED-op `cluster ops confirm` recovery** — the plan specified it for the (rare, >2min) BLOCKED
  case; the sim's pace makes it unlikely and grow's staged op self-drives. Left as a documented known
  limitation (grow's approve comment already notes the op is staged). Not adding the op-id extraction this
  pass.
- **doctor `/var/lib/tether` tripwire** — ADDED (the symmetric artifact class: a root-owned
  /var/lib/tether would brick the broker as a sim artifact).

## Rejected / no-change

- **"joiner uses `--from-existing` on a fake DB not `--from-manifest`"** (minor) — faithful enough: a
  fresh broker that ran standalone first DOES have a DB to migrate; the I1 finding holds either way. Noted
  in the label; not switching to the manifest path (needs C2 manifest infra the sim doesn't set up).
- **"joiner's brief standalone boot never resets its JS store (#4 asymmetry)"** (minor) — empirically the
  ≤20s idle boot creates no persistent streams; documented rather than adding a joiner JS reset.

## Verification

After all fixes: re-ran `10-grow-to-3` (regression gate), `11-grow-gaps` (full-trailer set +
`assert_refuses` #I1), `13-inbroker-reconcile-perm` (`--skip-dry-run` + broadened signature + VOTER
control) on the server. `/etc/tether` naturally root-owned without the provision chown. Zero product-code
changes (dev tool only).

## External review round (Fail → fixed) — see `simcluster-grow-honest-external-review.md`

The external reviewer (Fail) tightened two things this internal pass under-did:
- **F1**: my Stage-C choice to gate #3/#22 by ordinal ALONE (not run the real `reconcile nats --all`) was
  insufficient — an ordinal-only token false-greens if the product fixes the auto path. FIXED: grow now
  runs the real `reconcile nats --all --wait --timeout 25s` before the manual render; the #3/#22 token is
  generated from the OBSERVED failure. Server-verified the `--all` genuinely times out with the real #3
  harvest reason (first grow) and the real in-broker #22 `apply: natsconf: temp: … permission denied`
  (second grow, brk1+brk2) — which ALSO resolves the Stage-C #22 over-attribution (the grow now observes
  the real auto reconciler). `11` asserts the real #3 string; `13` PRIMARY now asserts the real auto-path
  #22.
- **F2**: the `CLAUDE.md:73` whitespace gate caught a data-loss bug — my §5 mandate edit had deleted §7
  (D0–D9 status, 38 lines). Restored from HEAD (byte-identical); `git diff --check` clean.

**Stops at the external-review round-2 gate** (goal: 按流程前进于外审门停止).
