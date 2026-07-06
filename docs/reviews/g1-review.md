# G1 — deploy-tier hardening — Stage-C internal review + adjudication

Date: 2026-07-05
Plan: `docs/reviews/g1-plan.md`. Implementation: Stage-B (unstaged diff at review time).

> **How this review was produced (CLAUDE.md §3 step 4/5).** A 4-lens adversarial Workflow (all agents Opus
> 4.8, `model` omitted) reviewed the Stage-B diff — lenses: install-sh / go-code / optionB-consistency /
> sim-fidelity. Experts read-only + may add tests; only the main process edits the implementation. The main
> process then adjudicated every finding (below), fixed the accepted ones, and added the reviewer-suggested
> regression guards. Verdicts: install-sh = ship-with-fixes; go-code = ship; optionB = blocked; sim-fidelity
> = ship-with-fixes. The **single root cause** behind all blockers/majors: the #22 nats.conf relocation
> (`/etc/tether/nats.conf` → `/etc/tether/nats.d/nats.conf`) missed several **user-facing / sim seam**
> references. The four PRIMARY product seams (install.sh write, nats-server ExecStart, code default
> serve/serveconf, reconcile `--conf` default, `cluster init` print) DID agree; the stragglers were docs, a
> runtime hint, and sim scripts.

## Adjudication

### Blockers — ACCEPTED + FIXED

- **B1 — `test/simcluster/simcluster:407` cmd_init broker.yaml seam still wrote the OLD path** (flagged by
  optionB + sim-fidelity + install-sh independently). cmd_grow's sibling seam (:187) was retargeted but
  cmd_init's was not, so the drill base node (brk1, the leader) pinned its C3 reconciler at
  `/etc/tether/nats.conf` — a file install.sh no longer creates. brk1's reconciler would fail with a
  non-perm `stat source: no such file` reason that slips past drill 13's narrow perm-denied signature →
  drill 13 GREEN while MASKING a non-converging leader (exactly the mis-verification the Mandate forbids).
  **Fix**: retargeted `:407` seam to `/etc/tether/nats.d/nats.conf` + updated the stale `:395-398` comment
  to the Option B (tether-owned nats.d/) layout. **Guard added**: drill 13 now asserts EVERY voter's
  broker.yaml pins `nats_conf_path` at nats.d/, so a future seam straggler fails RED instead of masking.

- **B2 — `docs/broker-ops.md §3.4:228/262` fresh-install auth_callout runbook wrote the authorization{}
  block to the OLD path** (install-sh; optionB rated it major). Post-G1 nats-server loads
  `nats.d/nats.conf`, so an operator following §3.4 writes auth_callout to an orphan `/etc/tether/nats.conf`
  that nats-server never loads (`nats-server -t` validates the orphan → looks done), and after restart
  auth_callout is silently unconfigured — bypassing the nkey-identity/session-ACL invariant. **Fix**:
  retargeted the `tee` target + `-t` validation to `nats.d/nats.conf` and added a warning note that
  nats-server.service loads the nats.d/ file (writing the other path is a silently-ignored orphan).

### Majors — ACCEPTED + FIXED

- **M1 — `internal/broker/broker.go:905` quorum-loss runtime hint pointed at the OLD path** (optimB rated
  major; go-code minor). This error fires during a real routes-not-formed incident and would send the
  operator to a nonexistent file at the worst moment. It is product code, so it ships the wrong path to the
  fleet. **Fix**: repointed the hint to `/etc/tether/nats.d/nats.conf`.

- **M2 — `docs/broker-ops.md §8.6` #22 migration assumed clustered brokers carry an explicit
  `nats_conf_path`** (optionB). The plan's safety premise was true only in the SIM; the REAL `cluster init`
  printed step 3 (`cmd/tether/cluster.go:808`) only sets data_dir/raft_addr/secrets_dir — never
  `nats_conf_path`. So live clustered brokers rely on the code default, and the binary upgrade flips that
  default to nats.d/ while the conf may still be at the old path; the migration's `sed -i 's#nats_conf_path
  …#'` is a no-op for exactly those hosts. **Fix**: (a) rewrote §8.6 to instruct operators to ADD an
  explicit `nats_conf_path: /etc/tether/nats.d/nats.conf` line (not sed-replace) and stated the hard
  migration ORDER (migrate conf + add the line BEFORE the binary upgrade); (b) added `nats_conf_path` to
  `cluster init`'s printed step 3 (`cluster.go:808`) so fresh clusters persist it.

- **M3 — `test/simcluster/drills/20-forcesingle-natsconf.sh:19/30/32` read the OLD path** (sim-fidelity).
  Not in the G1 diff, but the global path move breaks a sibling drill: line 30's `grep '^cluster'
  /etc/tether/nats.conf` on a missing file exits non-zero → the drill goes RED for a PATH reason, silently
  invalidating the #20 reproduction. **Fix**: retargeted all three to nats.d/.

### Minors — ACCEPTED + FIXED

- `docs/cluster.md:178` takeover-natsconf `--conf` default → nats.d/ (matches the moved `defaultNatsConfPath`).
- `internal/natsconf/preflight.go:1` package doc path → nats.d/.
- `test/simcluster/simcluster:395-398` cmd_init comment + `:680` nats-conf help + `README.md:182` → nats.d/.
- `test/simcluster/drills/11-grow-gaps.sh:70` brk3 seam → nats.d/ (inert there — the #I1 refusal is
  raft-state-driven — but fixed for whole-tree consistency).
- `test/simcluster/simcluster:283` grow #22 detector: tightened `permission denied` (too broad) to the exact
  `natsconf: (temp|write .bak): … permission denied` signature so the #22 token keys strictly on the natsconf
  write and stays consistent with drill 13:52.
- `§8.6` ⛔ warning: extended to note a bare install.sh re-run ALSO overwrites broker.yaml (erasing the
  cluster: block → single-mode restart, a worse R3 hazard) + Caddyfile, not just nats.conf.

### Adjudicated — NOT changed (with reason)

- **install-sh m5 — `scripts/install.sh:556` broker.yaml `nats_conf_path` left commented** (deviation from
  plan DECISION-2b's "uncomment + activate"). **Driver conceded benign** ("no change required"): fresh
  installs are consistent-by-construction because the code default was correctly moved to nats.d/ and
  install.sh writes/reads nats.d/, so no fresh host relies on a wrong default. The commented line already
  shows the correct nats.d/ path for when an operator opts into cluster mode. **Rejected the change**; kept
  the belt-and-suspenders out (the code default is the SSOT).
- **go-code m11 — `warnRootDataDirOwner` silently skips on a nonexistent DataDir** (root-init into a
  not-yet-created dir). **Acceptable-by-design** (DECISION 4 best-effort; in the real flow install.sh
  pre-creates /var/lib/tether tether-owned so the warn fires). Kept as-is, but **added a test**
  (`TestWarnRootDataDirOwnerBestEffort`) pinning the never-panics/never-fails contract for a nonexistent dir
  + nil logger.
- **sim-fidelity m12 — the gitignored `test/simcluster/vendor/install.sh` is stale on disk.** Not a diff
  defect — it is the plan's stated deploy-tier precondition ("re-vendor scripts/install.sh + rebuild the
  image FIRST"). Recorded as a **deploy-tier gate action item** (below); no code change.

## Regression guards added (reviewer-suggested)

- **drill 13** + **`simcluster doctor`**: assert every voter's broker.yaml `nats_conf_path` equals
  `/etc/tether/nats.d/nats.conf` — a seam straggler on the old path now fails RED instead of masking a
  non-converging node.
- **`TestWarnRootDataDirOwnerBestEffort`** (`internal/clusteroffline`): pins the #6 WARN best-effort contract.
- Repo-wide `/etc/tether/nats.conf` straggler grep-guard test: **not added** (the exclusion set — historical
  `docs/reviews/*` plans/reviews and the migration comments that legitimately name the old path — makes a
  clean automated guard noisy). Instead the tree was manually swept (all active code / current manuals / sim
  scripts retargeted; historical review docs left as the record they are), and the doctor + drill-13
  broker.yaml assertions guard the sim-facing seam that actually matters.

## Hard gates

- `make lint` (golangci-lint v2): **0 issues**.
- `make test`: green on the touched packages; the only failures were two `public_port_bind_failed` tunnel
  port-bind FLAKES under full-load parallelism (internal/tunnel, test/concurrency) — UNRELATED to G1 (no
  tunnel-port code touched), confirmed by a clean pass on an isolated re-run of both packages.
- **Deploy-tier gate — PASSED** (run on `weilandserver` 192.168.1.150 via `remote.sh --build`, which
  re-vendors the fixed scripts/install.sh + rebuilds the image; 2026-07-05):
  - `drill 13-inbroker-reconcile-perm`: **GREEN, 11/11 assertions** — real N=1→2→3 cross-process grow;
    `/etc/tether` stays root-owned + `/etc/tether/nats.d` tether-owned; **every voter's broker.yaml pins
    nats_conf_path at nats.d/** (proves the B1 cmd_init seam fix — no masked non-converging leader); grow's
    `reconcile nats --all` shows NO natsconf permission-denied reason + the **#22 trailer token dropped**; and
    a **User=tether write into nats.d/ SUCCEEDS** (the pre-G1 perm-denied path is fixed).
  - `doctor`: **clean** — `/etc/tether` root-owned (Option B invariant) + `/etc/tether/nats.d` tether-owned
    (#22) + tether-broker **`Restart=always`** (#23 drift). Instance nuked after.
  - (#24 = docs/comment/hermetic-x509-test and #6 = docs/WARN have no sim runtime flip, so they are not part
    of the deploy-tier gate; the #23/#24 grow-trailer tokens correctly STAY.)

## Closing-verification (Stage-C phase 3, feedback: audit-three-stage) — 4-expert parallel, ALL CLOSED

Per [audit-three-stage], phase 3 is parallel read-only. FOUR independent experts (path-consistency /
finding-closure / correctness-regression / sim-mandate), each grounding checks in file:line and NOT trusting
the "fixed" labels, verified the fixes. **All four verdicts: ALL CLOSED.**
- Every accepted finding (B1/B2/M1/M2/M3 + minors + guards) confirmed closed at its file:line.
- **Path-consistency chain CONSISTENT**: install.sh write + nats-server ExecStart `-c`, code default
  (`defaultNatsConfPath` SSOT → serve/serveconf), reconcile `--conf` default, and `cluster init`'s printed
  step (no `--conf` → relies on the default) all agree on `/etc/tether/nats.d/nats.conf`. Straggler grep = 7
  hits, all legitimate (migration source refs / "don't write the old path" warnings / seam-guard comments).
- **#6 wiring COMPLETE** (all five offline ops); **#23 CORRECT** (Restart=always+RestartSec=2 broker-only,
  nats-server unchanged, `/etc/tether` never chowned); the 3 new Go tests pass; the seam-guard shell logic is
  sound (drill 13 grows all three voters so its brk1/2/3 loop cannot false-RED).

Two minor items raised + adjudicated:
- **sim-mandate — grow LOG branch still coarse** (`simcluster:253-257`): the #22 TOKEN gate was tightened but
  the LOG branch above it still keyed #22 on the coarse OR, so a post-fix non-perm timeout (#8/#10) would
  mis-log "[GAP #22] perm-denied". **ACCEPTED + FIXED**: the log branch now keys #22 on the same
  `natsconf: (temp|write .bak): … permission denied` signature as the token gate; a non-perm timeout logs a
  non-#22 (#8/#10) line.
- **finding-closure — vendored install.sh stale on disk** (`test/simcluster/vendor/install.sh`): a gitignored
  local build artifact (`git check-ignore` confirms untracked); `remote.sh --build` regenerates it from the
  corrected scripts/install.sh, and drill 13 self-guards (a stale image → RED). **NO CHANGE** — it is the
  deploy-tier gate precondition, not a committed straggler.

Verdict: **ALL CLOSED — ready for external review.**
