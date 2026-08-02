# expected-verdicts-log.md — prose histories for `expected-verdicts.tsv`

Moved verbatim out of the TSV so the machine table can be parsed strictly. One section per drill,
keyed by the `note-ref` column. Signature slugs referenced by the `bands` column are defined here.

## 00-skeleton

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 10-grow-to-3

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 11-grow-gaps

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 12-ghost-voter

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 13-inbroker-reconcile-perm

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 20-forcesingle-natsconf

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 21-smalldisk-tierb

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 22-forcesingle-online

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R14 flip (#36): YES-online arm now asserts the product-FIXED behaviour — `--online --yes` is Tier-2-rejected IDENTICALLY to offline ('NO --yes override'/'cannot run unattended', exit 64, rejectedUnattendedYes runs in the online branch before the admin socket, cluster_offline.go:165-173). r14d GREEN pass=34, nc_guard=0 (the TAMED bounce guard did not fire — POSITIVE dwell). Was GREEN (stable both runs) [D2/N1: same intermittent C1-grow(N=2) ASSERT-FAIL band as 42/51/82 = #GROW-ONTO-RECOVERED family (R16; r15-finalization §9.1) — expected stays GREEN, band registered honestly here so a full-suite red on 22 is attributable, not ownerless]

## 30-rolling-upgrade

- **batch**: `R9-D`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R9-D rewrite: assert_fail=0 in r9d-b + r9d-c + r9d-d (pass=50/53/53). The 2 gaps are the STATED-REASON ledger items (b)(c); OQ-6 (a) is RETIRED — colocated.sh now supplies it. Was ASSERT-FAIL (P3+H1+H3) [D2 2026-07-21 定案: deploy-tier serial×2+-j3×1 = 1 fire/2 clean → phase-2 leader-hop 写窗口=#66（LIVE-CONFIRMED，scene 抓到 brk1→brk2 换届+健康重收敛，非 infra）。间歇 ASSERT-FAIL 属 roll-窗口 band（非排他，外审 M-1 收窄）：phase-2 leader-hop 换届命中=#66（scene-proven）；phase-1 命中形态未定性（外审样本 #4：leader 未换届+集群健康，不属 #66 机理，签名行未存档），watcher 自捕待定因。谓词保持严格。verdict 维持 INCOMPLETE（b/c 两结构 gap）。]

## 31-node-upgrade-fleet

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: stable across both runs

upgrade-safety follow-up (2026-08-01): F4 rewritten for the CANARY contract (`--all --timeout 0` now
aborts on the canary's transient failure instead of skip-continuing across the fleet — a product
behavior change this drill would have caught red on its next run); allow-config bodies extracted to
drills/lib/upgradecfg.sh (thin shells, behavior unchanged); nc_gap rescoped — single-node
success/rollback/`--wait` ownership moved to drill 33, only the fleet-wide fan-out after a committed
canary remains here. **REAL RUN 2026-08-02: INCOMPLETE, pass=30, assert_fail=0, nc_gap=1 — matches
this row.** The first attempt ran against a STALE sim image and the three new F4 assertions went red
on the pre-canary binary — an unplanned but decisive two-sided proof that they have teeth: red on the
old contract, green on the new one after `./local.sh --build build`.

## 98-stuck-redial-recovery

- **batch**: `gotcha #72 fix`  _(expected/owner authoritative in expected-verdicts.tsv)_

Born INCOMPLETE/1 by design and honestly scoped: this is the POST-FIX bounded-teardown RECOVERY
regression over nats:// (black-hole the connected broker's client port, assert heartbeat resumes via
another voter within the written budget, classify the recovery path by MainPID three ways) — NOT a
pre-fix reproduction of #72 itself: the live incident rode a half-dead wss:// handshake and simcluster
fronts no wss:// listener, so that arm is the drill's own [GAP #72] and the ledger's flip condition.
The row exists from the fix increment's day one so ledger-crosscheck has a non-GREEN owner for #72
(gotcha stays OPEN until the wss arm lands and runs GREEN repeatedly). Plan: docs/reviews/gotcha72-teardown-plan.md.

**REAL RUN 2026-08-02 (weilandserver): INCOMPLETE, pass=13, assert_fail=0, nc_gap=1 — matches this
row.** Five rounds, and the IMPACT arm (added by internal review F98-2: prove the fault bit the LIVE
connection before claiming any recovery) caught every one of them:
  1. "the agent is on brk1 because agent-join dialled it first" — WRONG on a 3-voter cluster: after
     the agent adopts the signed roster its dial pool is VOTER-first with an intra-voter shuffle.
  2. "whichever broker LOGGED the register holds the connection" — also wrong: register is a
     QUEUE-GROUP subject, so the handling member need not own the TCP connection. (Measured runs put
     the agent on brk2 and later brk3.) The authoritative source is nats-server's own `/connz`,
     which drill 41 already uses; the drill now discovers the edge from it and asserts recovery
     against it too.
  3. A "heartbeat stalls" impact probe compared against a watermark captured BEFORE the discovery
     and injection steps, so the heartbeat had already advanced past it through the healthy link.
     Replaced by the unambiguous fact from the same source: the client connection LEFT the cut broker.
  4. The recovery budget was 90s — structurally unsatisfiable, and its one PASS was luck. tether does
     not set `nats.Options.PingInterval`/`MaxPingsOut`, so under a SILENT DROP (no RST) nats.go takes
     up to ~4min to declare the disconnect. Budget re-derived term by term to 330s and written into
     the script. The product's published ≤60s bound covers only the part AFTER that declaration
     (usage.md §9.9), so a drill measuring detection + recovery must budget for both.

## 33-node-upgrade-success

- **batch**: `upgrade-safety follow-up`  _(expected/owner authoritative in expected-verdicts.tsv)_

Born INCOMPLETE/1 by design: the drill's own not_covered names gotcha #73 (a NON-tether artifact that
fakes the frozen version line passes the smoke gate and then has no boot shim — budget never ticks,
marker pends forever). Staging that artifact would wedge agt1 into exactly the stranded state it
describes, so the gap is registered, not exercised; it flips when #73 lands an owner (probe drill 34
or a product shim-self-attestation defense). Plan and oracle table: docs/reviews/upgrade-success-drill-plan.md.

**REAL RUN 2026-08-02 (weilandserver, `./local.sh drill`): INCOMPLETE, pass=29, assert_fail=0,
nc_gap=1 — matches this row.** The expected verdict is now measured, not claimed.

Took four rounds, and every red was THIS DRILL'S ORACLE — the product behaved correctly on the very
first run (real in-place `syscall.Exec` with PID and `ExecMainStartTimestamp` unchanged, real 120s
watchdog rollback, real domain refusal, real domain release). The oracle defects, worth recording
because two of them are traps any future drill can fall into:
  1. `jq .release` — the wire field is `release_version` (proto.NodeListEntry). OLD_RELEASE captured
     the literal "null" and five downstream assertions silently compared against it.
  2. C's phrase-pin. ctl replaces the agent's own sentence with the operator HINT keyed by the wire
     code, so the three `upgrade_in_progress` emitters are indistinguishable from the ctl side. Now
     pinned on the `agent_rejected:` prefix (proves it came from the agent process, not the broker);
     WHICH gate fired stays owned by the hermetic test that can read the raw reply.
  3. Reading the broker log from journald. install.sh's broker unit sets
     `StandardOutput=append:/var/log/tether/broker.log`, so `journalctl -u tether-broker` is EMPTY by
     construction — the first "fix" (widening the journal window) did nothing, which is what exposed
     the real cause. Read the file the unit writes.

## 32-install-lifecycle

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 40-drain-retire

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 41-shrink-to-standalone

- **batch**: `B2-debt`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

B2-debt (2026-07-28, post-release technical-debt cleanup): verdict stays INCOMPLETE / 2 gaps, but BOTH
GAP TEXTS ARE REPLACED, because the old reasoning was measured and found false and the new reasoning was
measured too. The old gaps said agt1 "does not physically leave the retired-but-still-meshed broker
in-window", blamed a suspected host/IP match failure in `rosterRequiresReconnect`, and flagged their own
reasoning as "unconfirmed from a torn-down run".

WHAT FIVE RUNS ACTUALLY SHOW.

- It DOES leave. A live journal capture has `rosterRequiresReconnect` firing with
  `connected_url=nats://brk2:4222` — a HOSTNAME, so the guessed host/IP gap is NOT the mechanism — and
  the agent re-registering on a remaining voter 62 MILLISECONDS later, with `/connz` reading brk2
  (retiring) 0 connections and brk1 (voter) 1.
- It does NOT do so reliably. Runs 1–2 moved ~57s after registration (88 ms apart). Run 3 passed at
  `poll_until 60 3`. Run 4 TIMED OUT at 60s on both arms. Run 5 TIMED OUT at **210s** — the 180s
  full-jitter ceiling plus margin, and the window this arm carried historically.

So the honest claim is not "it doesn't work" and not "it works"; it is "the move is confirmed, its
LATENCY is not bounded by anything this drill can justify". Widening past 210s would be inventing an SLA
to turn a red green, which is the one thing the harness must never do.

CANDIDATE MECHANISMS, recorded as candidates. The re-home rides the roster refresh loop, whose timer is
`jitterDur(3 min)` = UNIFORM(0, 180s] REDRAWN after every wake; the `nats_topology_*` sys.event that can
wake it early is best-effort and UNRETRIED — buffered-1, coalescing, DROPPED with a fresh full-jitter
reset if the loop wakes while `reconnectInFlight` or `rebuilding` is set, so two consecutive drops
already exceed 210s. The second candidate is the string-identity blindness now pinned as an executable
statement in `internal/agent`. Neither is confirmed, and the notes say so — assuming one and repeating it
as fact is exactly how the previous pair of gaps came to be wrong.

The correctness invariant is untouched and still asserted: agt1 stays functionally reachable across the
retire, and escapes via the #48 silence-rebuild path on true decommission — which passed in every run
above, including both runs where the fast path missed its window. | G69 (2026-07-22): this drill was the THIRD call site of `reconcile nats --to-standalone`, whose contract R16's A4 changed (add --reset-js on a data-bearing JS store). It was failing ASSERT-FAIL because it called the bare verb and then hand-rolled `mv /var/lib/tether/jetstream` — a Mandate-2 concealment that A4 also made BROKEN (refusal => conf never swapped => the hand-mv restarted a lone voter onto a still-CLUSTERED conf => n1ClusteredJetStreamFatal => the whole recovery leg cascaded). Now: one product verb `--to-standalone --reset-js`, with the ACKNOWLEDGEMENT GATE pinned BEFORE the happy path, a standalone-bak.* move-aside postcondition, and (internal review G-16) a PRECONDITION asserting the conf is still clustered - without it the already-standalone refusal, which also mentions --reset-js, would bank the guard for the wrong reason and the de-cluster arm would go green without de-clustering. Only the service restart remains sim-side. Verdict unchanged at INCOMPLETE: its 2 gaps are the pre-existing first-retire PROACTIVE fast-path pair. | R15: was timing-flake; r15w2 confirmed INCOMPLETE

## 42-rejoin-returning

- **batch**: `R16`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R16 FLIP → GREEN (2026-07-22, deploy-tier, final image): #GROW-ONTO-FORCE-SINGLE is FIXED. The whole recovery journey now completes end-to-end — force-single --reset-js (A3: the PRODUCT verb that retired this drill's hand-rolled `mv` of the survivor's store, a Mandate-④ concealment) → rejoin prepare → init --from-manifest → resnapshot → `cluster add` → `✓ brk2 is now a VOTER` + `cluster add complete` + REJOIN TERMINUS. verdict=GREEN pass=48 assert_fail=0 product_red=0 not_covered=0. TWO tether fixes beyond the JS-meta root were required, both found ON this drill: (a) A1 — the RETURNING joiner's stale JS store is moved aside at grow P5 (rejoin-prepare wipes raft/+tether.db but NOT the JS store, so the joiner booted a dead-epoch clustered meta and fail-stopped on n1ClusteredJetStreamFatal); (b) the start-joiner readiness check is now a BOUNDED POLL — the one-shot probe raced the joiner's own boot (the admin socket is served at the END of Run) and HALTed a CORRECT grow in 3 of 4 runs while telling the operator to start daemons that were already running. Pre-R16 this arm was a hard deadlock EVERY run. STABILITY (R16, final image): 4 runs on the deploy tier — the R16 rejoin arm passed in all 4 of the runs that REACHED it (3 repeats + the original), verdict GREEN. Repeat run 3 died EARLIER, in the shared setup fixture, at `baseline: tier-B push works on healthy N=2` with `bucket_create_failed: create_bucket: context deadline exceeded` (pass=47, assert_fail=1) — that is #67 face A, a PRE-EXISTING defect on the tier-B push-prepare path which R16 never touched (git diff proves this file's R16 edits all sit AFTER the bucket create). Recorded as #67 and NOT laundered into this row's verdict: the expectation stays GREEN because GREEN is what a run of this drill normally and correctly produces; #67 has its own dedicated owner drill (67-transient-js-refusal). | R15: R9-D: H8 unblocked the spine, all 42 assertions green; the TERMINUS (returning node re-grow) reproduces #47 CATCHING_UP. Stable across r9d-b + r9d-c + r9d-d. #49 re-verified GREEN. Was SETUP-RED (H8)

## 43-migrate-live-data

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 50-backup-restore

- **batch**: `R10`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

r10d 2026-07-19: PRODUCT-RED (only DOC-27, the /var/backups example) pass=86. #50 FIXED (R3a-g: doctor now FATALs on all six bad-DB states, exit 64) + #64 FIXED (K-#64a/b/c: restore leads with the de-cluster step, runnable, prediction held) + #53-silence CLOSED (D-#53/J2e). #50/#64 owed a LEDGER CLOSE in docs/deploy-tier-gotchas.md (drill-side flipped) [2026-07-21 CLOSED: deploy-tier drill-50 verified GREEN (pass=87, 0 gaps) on weilandserver — DOC-27 arm C flipped to positive regression, the runbook §5 + CLI --help ONLINE-backup example now uses /var/lib/tether/backups and runs as User=tether; gotchas DOC-27 marked CLOSED in the same edit.]

## 51-full-dr

- **batch**: `R16`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R16 FLIP → INCOMPLETE (2026-07-22, deploy-tier): #GROW-ONTO-RECOVERED is FIXED — `I re-grow to N=2 succeeded after the DR` + `I2 the data plane STILL serves the original sentinel`, product_red=0 pass=72. Root was restore-not-grow-ready: RestoreFromBackup bootstrapped a single voter but never took the GrowReadySnapshot that `cluster init --from-existing` takes, so a fresh joiner replayed a log that never carried the direct-installed rows (the pc732 hollow-voter/FK class) and the re-grow could not converge. A2c adds it; B1 then stops restore_in_progress riding that snapshot into the joiner. Verdict stays INCOMPLETE ONLY on ORTHOGONAL pre-existing gaps — [GAP #6-chown] (runbook §5.2 omits the chown that restore --config forces), #53-scope (WONTFIX-BY-DESIGN, state.db-only bundle) and H1a (sub-second offline window not observable in-sim). NEVER laundered GREEN. | R15: r14d 2026-07-20 PRODUCT-RED (#31/#45), nc_gap=2 nc_guard=0 pass=70. R14: the H1a offline-window guard (441) was reclassified runtime-guard→gap — a byte-identical-cert DR reconnect RELIABLY out-races the shell (re-running never catches the sub-second offline window; H1b/H2 cover the end state), so it is a persistent drill coverage hole, not a re-run valve. r10d 2026-07-19: DR TAIL RUNS END-TO-END — H2 terminus served the original sentinel. #51 FIXED (F-b7/b8: restore --config applied the 5-field seam) + #52 addressed (G-nats) + #53-silence CLOSED (B-vault1c/J1) / #53-scope WONTFIX gap (J). Lands PRODUCT-RED on the re-grow #31/#45 (arm I). Residual DR-STEP gap: #6-chown. #51/#52 owed a LEDGER CLOSE (drill-side flipped)

## 52-credential-rotation

  sig:retire-not-leader := 52 D-spine: retire .* error: not leader

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

r11f 2026-07-19 GREEN (pass=62). R11 CLOSED #54/#55/#56/#63/DOC-23 — B2/B3/55a/55b/55c are now POSITIVE GREEN regressions. Drill-side finish (harness transport, not product): D8a/D8c 'alert clear' routed through the broker admin socket (operator-only verb, alert.go:29-31 — ctl has no admin socket, was rc=69); D2c narrowed to the refused retire's genuine side-effects (no new retire/drain op + no credrot alert) — the old 'ops ls | grep brk2' false-failed on brk2's healthy join/done membership row once R11 fixed the admin-socket output pollution. Only intermittent non-green if it fires: D-spine #31 grow-lock (PRODUCT-RED, owner R14) / A7 runtime-guard (INCOMPLETE).

## 60-user-journey

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 61-transfer-edges

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 62-remote-fs-safe

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R2 predicted change (pre-enumerated in r2-plan §2)

## 67-transient-js-refusal

- **batch**: `G67+G69`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

G69 (2026-07-22) added a POSITIVE oracle to this drill and pass is now 18, not 17. WHY: the sub-face-4 `not_covered` gap is NOT unconditional - it fires only when the first post-grow push FAILS and the retry succeeds - and the PRE-fix baseline recorded below is itself nc_gap=1 pass=17, so 'the gap disappeared' was byte-identical to the pre-fix result and proved NOTHING (internal review G-3 caught the main process citing it as acceptance evidence). The positive oracle is checkable on EVERY run, loaded or not: after the grow, assert no `WITHOUT proving JetStream placement` degrade entry in any op timeline. Evidence: PASSED both unloaded and under 7-way saturation (the regime that originally produced '3 attempts over 8s all timed out'), with the sub-face-4 gap not firing. LIMIT: that is ONE-ARMED, not a differential - the pre-G69 arm was not built (stash-build on a 51-changed/20-new uncommitted tree). The remaining nc_gap=1 is face B, which has no deploy-tier oracle and keeps this row INCOMPLETE by construction. | face A FLIPPED PRODUCT-RED -> GREEN by G67 (2026-07-22, deploy-tier verified: verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0 not_covered=1 nc_gap=1 pass=17 (the face-A ARM is green; the drill is INCOMPLETE by construction, see below)). The refusal is now HONEST: `code=jetstream_not_ready ... after 3 attempt(s) over 8s: create_bucket: context deadline exceeded - ... usually transient ...`, where it used to be the terminal `code=bucket_create_failed create_bucket: context deadline exceeded` with no retry hint. NON-VACUITY TOOTH: brk1's own journal must show `tier-B bucket provisioning retried`. Internal review correction - this tooth is a not_covered, NOT an _as_fail, so deleting the bounded retry moves nc_gap 1->2 rather than turning the drill red; the BASELINE nc_gap for a healthy run is therefore recorded here as 1 (face B) and a run reporting 2 means the retry stopped running. The tooth deliberately does NOT accept the `gave up` line, which is emitted even for a PERMANENT single-attempt refusal. Two drill bugs were found and fixed by running it: (1) assertions written as `sh -c "... \$_G67_OUT ..."` silently tested the EMPTY STRING because the child shell does not inherit the variable - that produced two false FAILs and one VACUOUS PASS on the first post-fix run, and is why the checks now go through functions; (2) the first tooth accepted `retried|gave up`. History: this drill was created by G67 itself as #67's deterministic pin, and its oracle went through three versions, two forced by real runs - see docs/deploy-tier-gotchas.md #67. Verdict is INCOMPLETE, not GREEN, and deliberately so: face B of #67 has NO deploy-tier oracle (the only injection that reproduced it, SIGSTOP on the peer, was retired for producing connection-level failures that are a DIFFERENT defect), so the drill records it as a first-class nc_gap. A clean GREEN here would assert that #67 is closed when it is not.

## 70-expose-journey

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 71-expose-rehome-failover

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: R14: the four #29-family THIS-RUN guards (Arm E / B-silent / Arm B drain-migrate / crash-strand core) were reclassified runtime-guard→gap — they fire when the #29-family agent-tunnel-to-non-leader fixture does NOT establish, i.e. because an OPEN product defect (#29) reproduces, NOT intrinsic sim non-determinism; they turn GREEN when #29 lands, so they are debt owned by #29 (matching line-291's sibling #31 gap). Landing verdict unchanged. R9-D: arm B is now P1/R8s POSITIVE verifier — r9d-a measured drain rc=0 + migrated+serving + ZERO agent re-registrations, assert_fail=0. The 2 gaps are long-registered. r9d-d re-confirmed it (assert_fail=0, pass=25, drain rc=0). r9d-b/c aborted SETUP-RED on the expose_serve_sentinel fixture — root cause: this drill TITLE gained an apostrophe and the sentinel token embeds $_AS_DRILL into a single-quoted sh -c payload; token now sanitised + bounded-retried. Was ASSERT-FAIL (P1 #29 + H9)

## 72-proxy-subscription

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 73-proxy-cluster-ha

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: R14: the QUORUM data-plane-separation THIS-RUN guard (384) was reclassified runtime-guard→gap — it fires when a rebalance-MOVED dead-homed exit fails to render+serve, i.e. the CONFIRMED product defect #33/#34 reproducing, NOT intrinsic sim non-determinism; it turns GREEN when moved-exit rendering is made deterministic in the product (matching line-381's sibling gap). Landing verdict unchanged. r9d-a/b/d=INCOMPLETE r9d-c=ASSERT-FAIL(Q-xcheck endpoint mismatch — the drills own registered exposure) — flake band unchanged. R9-D: the REHOME live-target gate no longer reports #34 drift as a broken foundation; it records product_red "#34" and SKIPS the arm (proved live by a forced-drift mutation run)

## 74-rebalance-on-return

  sig:b-negctrl-create := negative-control expose reg create rc=70

  sig:c-ss-preflow := poll_until: timed out .* C pre-kill SS flows via
  <!-- MAJOR-2 (round-2 review): C-ss-pre is now stage-classified like B-dp — a harness-* failure (/sub
       fetch or ss-local bind) becomes a not_covered gap, NOT this ASSERT-FAIL, so this band matches ONLY
       a genuine product strand (the local SS client PROVEN READY, bytes still not flowing). The band is a
       real #34 reproduction, not a harness flake dressed as one. -->

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: r1a=GREEN r1b=ASSERT-FAIL r2=INCOMPLETE — flake band

## 80-session-isolation

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #25 CLOSED (per-IP PIN rate limit). Arm R rewritten to a POSITIVE regression — same-IP wrong-PIN storm then the 11th same-IP CORRECT-PIN join is REFUSED at CONNECT (assert_refuses), guarded by _rl_logged (broker rate-limit log) + R-2ndsrc (different IP still joins = per-IP scope) + E-pos (under-threshold correct PIN succeeds). Was PRODUCT-RED (#25)

## 81-admin-evict-session-rm

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #26 CLOSED (evict reaps managed OS children). C-GAP-proc → C-reap: after evict the setsid-nohup managed child is GONE from the host process table (daemon exited AND pgrep empty); C-base-proc first proves it was running. C-sysd-reap: reaps under systemd too. Was PRODUCT-RED (#26)

## 82-agent-onboarding-invite

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #27 CLOSED (manifest_listen defaults to 127.0.0.1:7480 in cluster mode). SETUP-27 → after init the listener is BOUND by default (curl != 000, GREEN), gated by SETUP-27-control (unbound port still 000); M1/M2 pass without the retired ingress_enable_manifest workaround. Removing the #27 product_red UNMASKS the pre-existing INCOMPLETE that R2 §2 predicted ('product_red 压过 INCOMPLETE'): the U1-U4 user-service arm records not_covered(gap) because the sim container has no systemd --user manager (registered in simcluster-coverage-inventory.md — usage §6.1 path left to real machines). r12d live: nc_gap=1 pass=29, assert_fail=0 product_red=0 setup_red=0. Was PRODUCT-RED (#27)

## 90-alerts-lifecycle

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 91-client-converge

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #46 CLOSED (leader periodic re-converge; the "drops the 3rd voter" hypothesis was REFUTED — the real fix is the trigger). A2-brk3 → once brk3 reaches VOTER it MUST enter `seeds show` endpoints within 120s (assert_ok); brk1/brk2 present = non-vacuous. Was PRODUCT-RED (#46). Residual intermittent NON-green (owners retained ELSEWHERE): A3 retire grow-lock #31 → PRODUCT-RED if it fires (#31 also owned by 41/51); brk3 grow flake → INCOMPLETE runtime-guard

## 92-js503-remote-alert

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 93-metrics-observability

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: R13 (item 4): the ONLY non-GREEN is the #42 BOUNDED observation gap (quorum-loss ~TFence(10s) cluster status --remote window = PHYSICAL LIMIT of raft-lease safety, not a defect) — a not_covered(gap) that pins #42 with a non-GREEN owner cell. +ADMINRT (admin runtime process-introspection: schema/goroutines/threads/uptime + reconciler last_tick FRESHNESS advancing). CARD/JSON-2/3/4 races FIXED via `cluster status --settle 30s` (R13 debounce of the benign post-obs-restart DEGRADED transient; sustained DEGRADED still exits 1). WEBHOOK-raised flake FIXED by stabilizing leadership + re-capturing LDR before the delta arm (the reconciler re-seeds → fires nothing on any leadership move, alert_reconcile.go:120-123,177 — a PRE-EXISTING H13 timing race, NOT an R13/R12 regression: warmup+cleared ALWAYS POST the correct exact schema, and the raise landed intermittently — r13d/r13d2 missed, r13e/r13f landed). r13f INCOMPLETE pass=47 assert_fail=0. Was ASSERT-FAIL (H13)

## 94-agent-reconcile

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13-D6: `ps` LOST is now a REAL assertion (was an overclaim — the header/title named it but the drill only checked NODE status). A1d/e/f: agt1's exec children DERIVE LOST while agt1 is OFFLINE (storage-RUNNING row + OFFLINE owning node, exec.go:326-345) while agt2's stays RUNNING (the discriminator) — closing the RUNNING(A0d)→LOST(A1)→EXITED(A2) three-state chain. r13d GREEN pass=54. Was GREEN (stable both runs)

## 95-broker-selfheal

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13 (item 2): 95-D was a FALSE gap (R6: _d_raft_ok hard-pinned leader=="brk1", which T1a+T2a make impossible ⇒ a false not_covered). Predicate tightened to leader-EXISTS-AND-STABLE (any voter, not brk1); D-neg1/D-neg2 prove it can RED (no-leader + churn). The fix un-masked a bogus `session rm --yes` flag (removed — session.go has only --ack-alerts) → DELETING now constructs; then un-masked the ONE-SHOT boot resumer's 1s-JS-probe race (broker.go:1023-1038, no retry) → D4b waits for JS meta to re-form before D5. r13e GREEN pass=44: DELETING parked (D3) + boot resume finished the delete (D6b) end-to-end. Was INCOMPLETE (95-D-suspect-false-gap)

## 96-mid-flight-chaos

- **batch**: `R16`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

ROUND-4 R4-F3 (2026-07-23): the #58 cross-home GC deploy-tier gap is now booked EXACTLY ONCE - unconditionally, in the A-arm setup. The duplicate registration under the same title in the A2 branch is DELETED, so the coverage account no longer depends on which branch the A-arm takes (a run that reaches A2 now records one fewer nc_gap than the R14/R15 counts quoted below). The gap itself is unchanged and still OPEN: a >15m run is what would close it. | ROUND-3 R3-F3 (2026-07-23): the #58 arm no longer compresses xfer_cross_home_reap_age - external review F2 clamped that production knob to >= 15m (a lower floor lets the leader delete an object still live on ANOTHER home), so a 5s value can no longer even LOAD. The FIXED/REGRESSION/SPLIT-HOME judges that depended on it are DELETED, not relocated; the arm records an unconditional not_covered instead. Only the CADENCE knob (xfer_reap_interval) is still compressed. The #57 arm now brings brk2 back and waits for the finalize-on-recovery pass BEFORE judging - the previous revision declared #57 'forever' while the crashed home was still down, so it measured the crash rather than the product's recovery and could neither certify nor refute R16/G67. | R16 (2026-07-22 deploy-tier): product_red 1→0, assert_fail 1→0, pass=38. Lane B (#57 finalize-on-recovery: node-local durable in-flight ledger + a DETERMINISTIC synthetic terminal committed BEFORE the ledger is deleted) and Lane C (#58 leader cross-home GC for a bucket no HOME can reap) both SHIPPED and are pinned HERMETICALLY. Their DEPLOY-TIER demonstration did NOT happen: the A-arm's 1 GiB tier-B upload again reached a terminal before the docker kill (the standing in-sim interruption gap), so no chunks were stranded — peak orphan count 2 vs tombstone floor 6. R16 therefore ADDED a NON-VACUITY GATE to the #58 arm: when the peak orphan count never exceeds the floor the arm records not_covered instead of banking a 'count is at the floor' PASS that would assert the reap works on a run where no reap was needed. #57/#58 stay OPEN in the ledger — the product fix is in, the deploy-tier proof is owed. Drill also fixed: the leader-side #58 knobs (xfer_reap_interval + the new xfer_cross_home_reap_age) now load via a restart that RE-ESTABLISHES brk1 as leader — the cross-home GC is leader-only, so the first attempt put the compressed knobs on a node that had just lost leadership. | R15: R15: R14 drill flips: Q3 held-foreground seeds (F0/F0b `tether exec --timeout 30m -- sleep N` HELD by tether, not `nohup sleep &` — F0c/F3/F4 now ask a real RUNNING↔EXITED question the old fixture made self-contradictory); Q4 D3 clean best-effort-success positive; D6b + COMMITTER ATTRIBUTION (reads brk1's own broker.log 'session created' line) correctly separates a queue-group MAJORITY commit from a true #65 — r14d canary3 was durable on all 3 brokers yet brk1 did NOT commit it ⇒ recorded NOT-#65 (R6's exact insight, was the old ledger's phantom '5/6 durable minority writes'). #57 is the current PRODUCT-RED driver (see owner); #58-split-home now counts under nc_gap (deterministic structural cause, NOT a PINS-LIVE leak — retires at per-transfer-owner refinement). Reclassified runtime-guard→gap: #57 dangling-audit (determinization MEASURED insufficient — 1 GiB STILL completes before the docker-kill on the 88-vCPU host; bandwidth-shaping would destabilise the cluster; hermetic-owned) + #57 audit-unreadable (audit sits on the killed home broker) + D6b minority-write (R6: isolated minority can't auth a fresh CLI connection, rc=69) + #58 split-home (drill line 447: deterministic structural cause, a defect-tied gap). A-arm payload 12MiB→1GiB (helps #58 strand orphans + sometimes catches #57 IN-FLIGHT = live PRODUCT-RED). r14d nc_guard=0 EVERY run — TERMINAL-GATE CLEAN (all 7 of 96's runtime-guards eliminated; #58-split-home now counts under nc_gap: nc_gap=5 nc_guard=0). A1e (the #57 anti-vacuity control) fixed: it ran on agt1 whose HOME broker brk2 is dead when #57 pins → a false ASSERT-FAIL; now runs on agt2 (homed on the live brk1) so #57 lands PRODUCT-RED not ASSERT-FAIL. D3(Q4) is a DETERMINISTIC POSITIVE now (best-effort-success fix killed the apply-lag non-idempotent flake — the D3 that was 60× red). F-arm (Q3 held seeds) is GATED as a gap every run: arm-D partition recovery to FULL health (brk1 re-VOTER + agt2 re-ONLINE off its just-healed home) is >360s in-sim, so the double-fault arm is not run over cross-arm residue (a pre-existing gap, NOT caused by Q3 — the held-seed fixture is sound by exec.go:192 inspection). Was UNSTABLE (flake band)

## 97-soak-cycles

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13: the PERMANENT goroutine NOT-COVERED is RETIRED — admin runtime .goroutines (runtime.NumGoroutine, the in-process truth, not /proc Threads) enables a real cross-process leak gate: load(6-cycle soak)→quiesce→FLOOR returns to the pre-load baseline within GOR_TOL. Contract FIXED in the drill header (tol=2*NPROC+16, 3+3 floor samples, GOR_QUIESCE=SOAK_SETTLE). r13d GREEN pass=43 (brk1 floor 76→77, tol 192 on the 88-core host). Was GREEN (stable both runs)
