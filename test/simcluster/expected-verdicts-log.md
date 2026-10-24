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

2026-09-19 simcluster-speed H / X14 (the paragraphs below this one describe the PRE-H drill and are kept as
history): the client-liveness contract is now 20 s × 2 on BOTH halves (`internal/agent` AgentPingInterval /
AgentMaxPingsOut; install.sh nats.conf `ping_interval "20s"` / `ping_max 2`; drill `PING_INTERVAL_S=20` /
`PING_MAX=2`, reconciled by test/architecture/nats_ping_defaults_test.go), and the drill was RE-ORDERED
because H inverted its causality: the agent now declares the link dead at (PING_MAX+1)×PING_INTERVAL_S and
re-registers through a survivor BEFORE the server drops the old connection, so the old "IMPACT first, then
RECOVERY past a post-impact watermark" was structurally always-true. Now: the heartbeat watermark HB_INJ is
taken AT injection (after the three injection self-proofs, plus self-proof C — the cut broker still holds
the connection at that instant, internal review round 1 R1-F3); RECOVERY = `_hb_advanced ∧
_registered_on_another_voter` (heartbeat past HB_INJ AND a survivor's /connz lists agt1 — a conjunction, one
half alone has a false positive); the old IMPACT became the SERVER-SIDE evidence assertion (the cut broker
drops the dead client itself; `closed reason` logged). `RECOVERY_BUDGET = 2×((PING_MAX+1)×PING_INTERVAL_S
+ 20 + 10 + 10 + 30) = 260 s`, one shared DEADLINE for both polls (tests/teardown-recovery-nonvacuity-test.sh
pins the order, the conjunction, the CUT_BROKER exclusion, the shared budget and the SERVER-SIDE line).
Receipts (image #2 solo ×2): INCOMPLETE nc=1 pass=12 (13 before X14 folded two RECOVERY claims into the
conjunction; 14 with self-proof C), RECOVERY ≈57 s / 58 s after injection, SERVER-SIDE drop ≈58 s with
`Stale Connection`; pre-H the same injection took 4:03 / 4:00 (measured between two heartbeat timestamps,
so "≈4 min", not "exactly 4:00"). The #48 roster-silence path is no longer sampled here (the client ping
fires first) — registry `48-silence-rebuild-under-drop`, H-dependent. Expected row unchanged: INCOMPLETE 1
(the #72 wss arm).

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

2026-09-20 round-2 review (R1-F7 + the watermark SETUP-RED root cause): (1) the header sentence that justified the
watermark retry ("a LATER watermark is strictly harder for RECOVERY to beat") argued the wrong direction — `_hb_advanced`
is an inequality against the watermark and under a dead link the heartbeat is frozen, so a later read is neutral; what
the retry window DID re-open was self-proof C's gap, so C is now RE-ASSERTED at the watermark instant (`C′`, +1 assert_ok,
pass 13 → 14). (2) The S2 -j6 SETUP-REDs at the watermark (both CUT_BROKER=brk2) and the round-2 re-run's (brk3) had one
cause, visible only because the retry now logs each miss: `node ls --json` without `-a` lists ONLINE nodes only, and at the
watermark instant agt1 is often already STALE (its heartbeat rides the edge just cut; the G.2 sweep flips status after
5 s) → `"nodes": []` five times in 15 s. The brk1 solos passed only because the read landed inside the 5 s. `_hb_of` and
the watermark read now use `node ls -a` (a heartbeat timestamp is valid whatever the status column says; ONLINE is
`_online`'s separate question). Receipt: round-2 re-run INCOMPLETE 1 pass=14, watermark on read 1, C and C′ PASS,
RECOVERY same-PID in-process rebuild. drill-costs row 98 re-seeded from this complete run (R1-F9: the S2 row was the
186 s SETUP-RED).

## 33-node-upgrade-success

**Current final S5 serial acceptance**: native rc4/INCOMPLETE, 219 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/33-node-upgrade-success.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

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
  3. Reading the broker log from journald. The broker's application lines go to a FILE, so
     `journalctl -u tether-broker` does not carry them — the first "fix" (widening the journal
     window) did nothing, which is what exposed the real cause. Read the file.
     h1 F3 UPDATE: WHICH file, and why, both changed. The unit no longer redirects at all
     (`StandardOutput=journal`/`StandardError=journal`); the slog is a process-owned rotating file
     named by broker.yaml's `log_file:` (/var/log/tether/broker.log). The journal is consequently NO
     LONGER empty — it now holds panics and stacktraces. The lesson survives the change but its
     sharper form is: the two streams are DISTINCT, and an oracle that reads the wrong one fails in
     the worst possible direction, reporting a healthy product as broken. drills/lib/logs.sh is the
     one place that knows the mapping.

**S3 REGRESSION RUN 2026-10-02 — first run ASSERT-FAIL, a PRODUCT defect this drill alone caught.**
On image `c346a962…` (S3 as reviewed) C2 failed: `agent_rejected:install_failed` "running image differs from
the on-disk binary; restart the agent before upgrading" (pass=28, assert_fail=1, nc_gap=1). S3 plan §2.1 had
added a pre-download gate `running image == dst` while §8 promised "33 A/C/C2 unchanged": after A commits,
the shared-binary sibling agt1b has dst == the very candidate it is asked to install, yet runs the old image —
refused on every retry, recoverable only by a restart. Fixed in the product (prev ∈ {running image, candidate},
judged after extract; `internal/agent` `TestInstallPrevSlotAcceptsOnlyRollbackCapableImages` + three mutations).
**Re-run on image `4fe3646b…` (tree `7094d4bb`): INCOMPLETE rc=4, pass=29, assert_fail=0, nc_gap=1 (#73),
206 s — matches this row; B4's watchdog conjunction (`register deadline exceeded`, `.prev` consumed,
exe==OLD, MainPID unchanged) holds with the S3 bundle path in place.** The drill script is unchanged.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc4/INCOMPLETE, 216 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/33-node-upgrade-success.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process

**Current final parent receipt:** A/G/U/N/P/W/F GREEN0 with original assertions. Parent native 1, wall 1921.302 s; exact command `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/regression-sweep/receipt.json`. One deadline-fixed product image/tree; all product and drill inputs unchanged through serial acceptance.


**Declared legacy baseline: PRODUCT-RED, nc_gap=0; deploy-tier execution NOT_RUN.**
Owner: `docs/reviews/hot-update-plan.md` H0 process continuity; implementation and real-drill receipts remain separate.
One real success arm reuses 33's initial supply, trusted artifact and operator allowlists. Every fixture restart
finishes before a long-running real `tether exec` producer begins. The oracle requires a nonempty no-upgrade
source/output baseline, original PID/boot/startticks/nonce and parent, a committed same-agent-PID exec into the
new running SHA, and independent source progress between two post-upgrade observations.

Only the conjunction of both original pipes reporting EPIPE, original managed task reconciled EXITED(-1),
missing output in both original ctl transcripts and missing exact exit code produces the known signature:
`H0_LEGACY_EXEC_INTERRUPTED output=epipe management=exited_minus_one pid=<pid> nonce=<nonce>`.
No broad failure band is registered. Empty, malformed, wrong-image or unrelated-death evidence cannot match it.
`continuity` remains the positive oracle: exact source/transcript sequence equality, RUNNING ownership while
alive, no pipe errors and exit code 23 through the original ctl. When fixed, `assert_bug` intentionally reports
APPEARS FIXED / ASSERT-FAIL; promote that call to `assert_ok _continuity` and flip this expectation with real evidence.
This is not a hot-update Pass: no PTY, SSH, transfer, rollback, NATS/Caddy replacement or N-1 claim.
The four log streams use `drills/lib/logs.sh`; raw probe receipts are retained under SIM_EVIDENCE_DIR.
Hermetic controls run through `tests/hot-update-process-probe-test.sh` and `tests/run-all.sh`; their success proves
the sensor only. 34 is assigned here; the unrelated #73 non-tether artifact gap in 33 remains open and unnumbered.

**S1 re-pin (2026-09-21, hot-update-reliable-exec-plan §5.3).** The paragraph above is the pre-S1 receipt and is
kept verbatim. Two of the three halves of the known failure are unchanged — `node upgrade` is a `syscall.Exec`, so
the child's pipe read ends close (`output=epipe`) and the new image's empty table reconciles the row EXITED(-1)
(`management=exited_minus_one`). The CTL half changed: `tether exec` now takes the reliable path, the exec'd agent
is a new runtime incarnation, its registry refuses the original lease (`procrequest.ErrInstance` →
`process_instance_lost`), and the ctl exits **74** within seconds instead of idling to its `--timeout`. The
signature is therefore
`H0_LEGACY_EXEC_INTERRUPTED output=epipe management=exited_minus_one ctl=owner_lost_rc74 pid=<pid> nonce=<nonce>`;
the pre-S1 `{"finished": false}` outcome and an rc 74 with a COMPLETE transcript are both hermetic negatives now.
Expected verdict stays PRODUCT-RED, nc_gap=0. **Observed under the S1 signature on 2026-09-21** (55 s, image
`sha256:ea43e478…`, pass=17 product_red=1 assert_fail=0): the ctl exited **74** promptly with `process request: lease was
issued by another runtime instance; task continuity lost — the remote task was accepted and its result is unknown`, both
pipes EPIPE, row EXITED(-1); the positive `_continuity` oracle still refused (`original live task lost management`), so
this is the known defect reproduced, not a hot-update pass (docs/reviews/hot-update-evidence.md §12.2). The pre-S1 run
(2026-09-20, old signature, ctl `{"finished":false}`) is in docs/reviews/hot-update-process-baseline.md. The positive `_continuity` oracle is untouched; S3
promotes it to `assert_ok`.

**S3 declaration (2026-10-02, plan §8; deployment NOT_RUN).** The historical S1 paragraph above
is preserved verbatim. Drill 34 now declares seven GREEN arms; its parent is their DERIVED join, with
zero coverage gaps. These are acceptance expectations, not observed deployment receipts. The lead
must run them after merging CUT/BOOT/MIXEDPROBE. Same-description `_continuity` is now `assert_ok`
in A. The S1 signature remains a hermetic negative of `hotupdate`; the real legacy exporter moves to 39.Y.
Import-refusal redispatch is covered by the real-binary hermetic bootstrap tests, and W exercises the
actual watchdog redispatch path. No injection switch or synthetic release is introduced into deployment.
Freeze-window samples and mixed-request percentiles are calibration only; the sensor rate stays fixed. Activation and audit readers decode actual text/JSON slog records; shared golden records are pinned by the daemon logger test and consumed by the Python oracle. DR-3 remains a product dependency: W still requires generation 2 and two cut receipts.

**Hermetic repair mutation receipts (2026-10-02; deployment still NOT_RUN).**
The real logger golden and both Python judges were exercised with injected implementation defects.
Each mutation below made its targeted control exit 1; every edited source was restored immediately.
Logger mutations used Go overlays. These receipts do not claim a deployment or resolve DR-3.

| Injected defect | Control result |
|---|---|
| activation parser ignores slog envelope | RED (exit 1) |
| activation allows duplicate fields | RED (exit 1) |
| activation allows duplicate generations | RED (exit 1) |
| activation allows zero fields | RED (exit 1) |
| activation accepts malformed numeric suffix | RED (exit 1) |
| activation defaults a missing field | RED (exit 1) |
| exec audit calls JSON true legacy | RED (exit 1) |
| exec audit ignores omitted legacy flag | RED (exit 1) |
| upgrade permits starvation | RED (exit 1) |
| upgrade permits refusal side effects | RED (exit 1) |
| upgrade permits unrequested effects | RED (exit 1) |
| upgrade permits no successful read | RED (exit 1) |
| upgrade permits no successful short | RED (exit 1) |
| upgrade permits no successful write overlap | RED (exit 1) |
| upgrade permits excessive refusals | RED (exit 1) |
| outside-cut refusal not counted as failure | RED (exit 1) |
| unrequested token reconciliation omitted | RED (exit 1) |
| successful counts include refusals | RED (exit 1) |
| write overlap counts any successful request | RED (exit 1) |
| judge uses scheduled upgrade/end decoys | RED (exit 1) |
| until-mark stops without activation | RED (exit 1) |
| until-mark drops post-activation tail | RED (exit 1) |
| mark uses another clock | RED (exit 1) |
| F restores local-only boot order | RED (exit 1) |
| F permits a non-probe spawn timeout | RED (exit 1) |
| F omits retained probe census | RED (exit 1) |
| P omits start handshake | RED (exit 1) |
| P marks without log observation | RED (exit 1) |
| P skips cut observation | RED (exit 1) |
| P skips activation observation | RED (exit 1) |
| P judges scheduled window | RED (exit 1) |
| shell judge omits .rate>=(0.9*.configured_rate) | RED (exit 1) |
| shell judge omits .effect_without_rc0==0 | RED (exit 1) |
| shell judge omits .unrequested_effects==0 | RED (exit 1) |
| shell judge omits .successful.read>0 | RED (exit 1) |
| shell judge omits .successful.write>0 | RED (exit 1) |
| shell judge omits .successful.short>0 | RED (exit 1) |
| shell judge omits .writes_in_window>=1 | RED (exit 1) |
| shell judge omits .agent_upgrading<=.starts_in_window | RED (exit 1) |
| daemon handler changed | RED (exit 1) |
| activation message layout changed | RED (exit 1) |
| tail exceeds maximum accepted | RED (exit 1) |
| scheduled activation allowed | RED (exit 1) |
| start receipt ignored | RED (exit 1) |
| path audit reader failure ignored | RED (exit 1) |
| path audit parser failure ignored | RED (exit 1) |
| upgrade shell rate floor omitted | RED (exit 1) |
| activation body layout golden mismatch | RED (exit 1) |

**REAL RUN 2026-10-02 (weilandserver, `./run-drills.sh -j4 --no-attribute 34-hot-update-process`): GREEN ×7,
parent GREEN, NO DEVIATIONS.** The S3 declaration above is now measured. Final receipts are two clean
sweeps on image `sha256:4fe3646b…` (tree `7094d4bb`, `vendor/tether` `81eb1d76…`, `tether-next` `73415cde…`,
`tether-prev` `1cfd3ec6…`): 218 s and 220 s wall; per arm A 63 / G 77 / U 68 / N 72 / P 102 / W 217 / F 81 s.
The very first sweep (image `c346a962…`, before the drill-33 fix below) was 6/7: **G ASSERT-FAIL
`HOT_UPDATE_EVIDENCE_INVALID: empty activation import_ms`** — THIS DRILL'S ORACLE, not the product. `shim_ms`
and `import_ms` are `Duration.Milliseconds()` truncations and are legitimately `0` when the step took under
1 ms (the gen-2 hop imported in <1 ms). The judge now requires `>0` only for `generation`, `tasks` and
`freeze_window_ms`; `0` is accepted for the two step timings, with a positive sub-ms selftest row and the
`activation allows zero fields` mutation still RED for the three fields that can never be zero.
Calibration samples (not budgets): `freeze_window_ms` 106–131 (budget 500), shim 92–107 ms (gen-2 hop 5 ms),
import 0–6 ms.


**Final S5 parent receipt:** All A/G/U/N/P/W/F nativeGREEN0 in257 on the final image; both-generation/watchdog/legacy/domain/calibration oracles retain their original assertions.

## 34-hot-update-process.A

**Current final S5 serial acceptance**: native rc0/GREEN, 67 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.A.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (63 s; ×3 sweeps). One image hop: original child PID/boot/startticks/nonce/parent, complete stdout/stderr and original ctl rc23; final ps EXITED(23), generation 1 active/committed, exactly one execution and activation, stable service identity, replay/pipe/pidfd census and empty inherited-FD audit.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 67 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.A.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.G

**Current final S5 serial acceptance**: native rc0/GREEN, 78 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.G.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (77 s; first sweep ASSERT-FAIL was the import_ms judge defect recorded in the parent section). One producer across tether → tether-next → tether, with artifact_add serving the second tarball from the same backend; generations 1 and 2, two activations, one execution, three FD/identity observations and progress before each hop.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 79 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.G.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.U

**Current final S5 serial acceptance**: native rc0/GREEN, 69 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.U.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (68 s; ×3 sweeps). Product agentUserUnitBody rendered verbatim with User=sim as a SYSTEM unit. This proves body-equivalent launch behavior; it does not prove systemctl --user (82 U3 owns that gap).


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 67 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.U.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.N

**Current final S5 serial acceptance**: native rc0/GREEN, 72 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.N.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (72 s; ×3 sweeps). Unsupervised setsid/nohup under sim: nonce pidfile, one matching argv, vendor SHA, system unit MainPID zero; stable process startticks and fd2 inode, pidfd-pinned cleanup.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 72 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.N.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.P

**Current final S5 serial acceptance**: native rc0/GREEN, 103 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.P.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (102 s; ×3 sweeps). P0: 30-second calibration, zero request/connect failures and rate ≥9, otherwise SETUP-RED. P1 starts the upgrade only after the probe start receipt. Agent slog cut/activation events after the cursor create marks in the probe monotonic clock; traffic continues through activation plus 10 seconds, with a 180-second maximum. Require ≥0.9×configured rate, successful read/write/short classes and a successful write overlapping cut..activated. Duplicate, missing, unrequested and refusal side effects fail; agent_upgrading is allowed only for requests starting inside the observed cut and is bounded by that count. Percentiles, over_5s and reliable/legacy exec counts remain calibration records. Observation timestamps include polling and transport delay; they are not nanosecond exec timestamps.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 104 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.P.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.W

**Current final S5 serial acceptance**: native rc0/GREEN, 218 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.W.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (217 s; ×3 sweeps). Established producer, SYN-only NATS blackhole, new image imports/thaws but cannot register. The 120-second watchdog reexports current state to the previous image: rolled_back, register deadline exceeded, generation 2, redispatched, previous slot consumed, original SHA/PID/start. Heal then LOST→RUNNING, source progress and EXITED(23) from the previous courier. The original ctl outcome is recorded only.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 215 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.W.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 34-hot-update-process.F

**Current final S5 serial acceptance**: native rc0/GREEN, 82 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/34-hot-update-process.F.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

S3 acceptance expectation; **REAL RUN 2026-10-02: GREEN** (81 s; ×3 sweeps). Healthy hangfs is mounted before the final setup restart and before the producer starts, matching drill 62 and the boot-time mount scan. The wedged cwd must refuse with remote_fs_unsafe_cwd; the upgrade refusal must carry remote_fs_probe_in_flight and a positive in-flight/retained probe census, corroborated by agent slog, before the healed upgrade. Unchanged binary/marker/PID and advancing source/ctl remain required. Heal then normal image continuity; thread census and freeze samples retained.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 80 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/34-hot-update-process.F.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 35-hot-update-exec-carrier

**Current final parent receipt:** A/B/C GREEN0; scoped registration fixture was frozen before this image build. Parent native 1, wall 1921.302 s; exact command `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/regression-sweep/receipt.json`. One deadline-fixed product image/tree; all product and drill inputs unchanged through serial acceptance.


**GREEN on all three arms — observed 2026-09-21 (first real run; hot-update S1, docs/reviews/hot-update-reliable-exec-plan.md §5.2).**
`./run-drills.sh 35-hot-update-exec-carrier` -j3 on image `sha256:ea43e478…`, vendor `tether` 6b180be0… / `tether-prev` v0.6.0 @ a3431a1
(1cfd3ec6…): A GREEN 90 s pass=29, B GREEN 51 s pass=21, C GREEN 84 s pass=40, NO DEVIATIONS. Receipts (carrier.json per leg,
legacy-outcome.json, calibration) in docs/reviews/hot-update-evidence.md §12.2. Highlights: A2 crossed redialAfter (`rebuild_lines=1`,
source 167→626 through the 25 s outage); calibration post-A 10/10 rc 0 all reliable, max 246 ms; B-new (a new exec launched right after the
broker restart) rc 0 reliable in 2.0 s; C1 the v0.6.0 ctl lost 450/522 records per stream from seq 55 (≥ the 54 it held at injection: attributable)
while today's agent kept the child RUNNING and delivered EXITED(23); C2 `probe_reached_lines=4` (the old agent's `unknown forwarded verb`), legacy,
complete. Parent row DERIVED (children's precedence join). The drill pins the S1 exit criterion that the hermetic
real-binary suite (`test/cli_e2e`, `TestReliableExecMatrix`) cannot: a REAL nats-server / tether-broker under
systemd on a real container host, replaced under a running `tether exec`. Fixture: N=1 (`up --brokers 1
--agents 1 --ctl 1; init brk1; session lab; agent-join agt1`) plus the H0 producer/oracle copied in — no
upgrade, no artifact server. Every log read goes through `drills/lib/logs.sh`.

The oracle is `drills/lib/hot_update_process_probe.py carrier` (arms A/B, and C2 with `--path legacy`) and
`legacy-carrier` (C1); both have positive AND negative synthetic controls in the probe's `selftest`, which
`tests/run-all.sh` runs hermetically, and the drill's shell gates around them are proved two-sided in
`tests/r9d-nonvacuity.sh`. Non-vacuity: `_stream_advancing` (two live samples strictly increasing) gates every
injection, and each fault window must show >= 20 new source records — recorded in `carrier.json.faults[]` and
judged.


**Final S5 parent receipt:** All A/B/C nativeGREEN0; final NATS A/C in264 and corrected broker-only B in265. Initial stale-ONLINE35A1 and overly broad guard264B2 remain preserved as fixture defects. The final predicate changes no deadline/progress assertion. One product image and427 unchanged production/build inputs; exact per-attempt fixture digests disclosed.

## 35-hot-update-exec-carrier.A

**Current final S5 serial acceptance**: native rc0/GREEN, 89 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/35-hot-update-exec-carrier.A.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

A1 `systemctl restart nats-server` (seconds), then A2 `stop; sleep 25; start` — 25 s is deliberately past the
agent's `redialAfter` (20 s), so the agent rebuilds its WHOLE NATS session (`agent: NATS stuck-disconnected;
rebuilding session…` is asserted from the leg's slog cursor). GREEN = original child alive under the same PID
triple/parent, agent MainPID + ExecMainStartTimestamp unchanged, both ctl transcripts byte-equal to the source,
no pipe_error, ctl rc 23 on its own, exactly one `agent: exec … reliable=true` for the nonce, exactly one
`ps -a` row EXITED(23). The 10 post-recovery `exec agt1 -- true` wall times and which path each took are
CALIBRATION in `carrier.json.calibration` — recorded, never judged. Known deploy-tier risk: the 25 s outage may
trip tether-broker's systemd StartLimit; that lands SETUP-RED here and a gotcha there, and the 25 s is not
tuned down to buy green.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 91 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `34b246959068b299bac4d479118915795f1ce90b4de235997ef4545efc859212`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-264/runner/35-hot-update-exec-carrier.A.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 35-hot-update-exec-carrier.B

**Current final S5 serial acceptance**: native rc0/GREEN, 51 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/35-hot-update-exec-carrier.B.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

`systemctl restart tether-broker` (nats-server stays up; the broker's subscriptions vanish and its auth_callout
black window opens). Same GREEN conjunction as A. `B-new` launches a NEW exec immediately after the restart and
records its rc / path / wall — plan §4 exit #5: new-request latency is not a criterion, and the connection-level
retry through the callout black window is proven hermetically (`TestCtlConnectRetriesThroughCalloutBlackWindow`).


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 52 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `2d15b0230013f12cedfab95aecdb9ecf23d9d897f51a866023cf462c4d780403`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-265/runner/35-hot-update-exec-carrier.B.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 35-hot-update-exec-carrier.C

**Current final S5 serial acceptance**: native rc0/GREEN, 86 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/35-hot-update-exec-carrier.C.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

The N-1 window, both directions, against the REAL previous release (`vendor/tether-prev`, built by
`lib/stage.sh sim_stage_prev` from the newest `v*` tag reachable from HEAD — today v0.6.0 — in a detached
worktree with that tag's own Makefile/go.mod; `vendor/tether-prev-source.json` is the receipt and
`sim_prev_source_ok` the die-gate; tether-next is NEVER accepted as "previous").
**C1** — the v0.6.0 ctl against today's agent under the A2 fault. A previous-release ctl can only take the
legacy exec path, and requirements §6.7 promises interoperability across the window, not carrier survival, so
GREEN = `reliable_lines == 0 && exec_start_lines == 1` (legacy path, executed once), today's agent kept the
child RUNNING and reported EXITED(23), and the discontinuity is RECORDED HONESTLY: transcript ≠ source (a clean
prefix or an internal break) or exit undelivered. `legacy-outcome.json` is the receipt. A fully continuous old
ctl would REFUSE the judge — the "N-1 is not continuous" claim must not be asserted on nothing. C1 is also the
real old-binary NEGATIVE control of the A/B oracle. **C2** — today's ctl against the v0.6.0 AGENT (binary
swapped under the same unit and home, `_agt1_at_prev` = ONLINE at the tag): the reliable probe reaches the old
agent (`unknown forwarded verb … process-resume` ≥ 1 is the receipt), the ctl falls back to legacy after the
2 s probe tax, and the run is complete (rc 23, transcript == source, one exec line, one EXITED(23) row).


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 87 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `34b246959068b299bac4d479118915795f1ce90b4de235997ef4545efc859212`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-264/runner/35-hot-update-exec-carrier.C.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

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

2026-09-20 round-2 review (R6-7: the 2026-09-19 oracle fix was carried only in drill comments): A8d/A8e read the wrong
STREAM. `matches neither the pinned` is written by the broker at BOOT (systemd journal, before slog opens), so the
oracle reads `sim_broker_panic_journal brk2` and the DOC-23 file-recovery diag dumps `sim_broker_panic_journal_dump brk2
400`; the claim texts say BOOT stream. Product text unchanged. Receipt: 52b solo GREEN pass=62 (batch21, image #13).

## 60-user-journey

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

2026-09-20 round-2 review (R6-7): the `_agt2_gone` oracle read the wrong COLUMN — `node ls` prints NODE KIND STATUS since
1e9d32a (node.go:112), the regex was matching STATUS at column 2 and so never saw OFFLINE/STALE; now
`^agt2[[:space:]]+[^[:space:]]+[[:space:]]+(OFFLINE|STALE)`. Product unchanged. Receipt: 60 solo GREEN pass=38 (batch21).

## 61-transfer-edges

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs

## 62-remote-fs-safe

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

2026-08-31 (remote-fs stale-health): Arm 1S added — the stale-healthy transition (mount judged healthy
FIRST, dies AFTER), which is the production shape #81 was reported from and which Arm 1 structurally
cannot reach. Each of its two abs-argv0 commands carries THREE oracles: A = did ctl receive a terminal
state, B = did the broker forward and the agent log this request start, C = is the product code expected.
C runs only when A and B both hold; otherwise an upstream failure cannot be duplicated as a product red.
The split exists because earlier one- and two-layer forms repeatedly blamed the wrong owner. A SIGQUIT
dump finally attributed the missing terminal state to a direct `cmd.Start` abandoned outside a GC
safepoint: the next stop-the-world froze heartbeat, timer and NATS together. Safe exec/run now launch the
risky target in a local re-exec helper and wait on a cancellable pipe. Three fresh-image isolated runs
were stable at `INCOMPLETE pass=41 assert_fail=0 product_red=0`, with only OQ-2 true-D remaining; #81 is
closed and its temporary band removed. R2 predicted change (pre-enumerated in r2-plan §2)

## 67-transient-js-refusal

- **batch**: `G67+G69`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

2026-09-19 simcluster-speed (P-a/P-b, plan §8.0/§8.2; **corrected the same day by internal review round 1
R1-F1**): expected INCOMPLETE 1 → **PRODUCT-RED 1**, owner `#67 #84`. What P-a/P-b changed: the drill used to
hit the runner's 2700 s ceiling (INFRA-ABORT, 2026-09-04 sweep) because a tier-B `Put` on a stalled JetStream
waited the flat 37 min `--timeout` default; the size-derived budget bounds it at ≈7 min for 12 MB, and the ctl
releases the per-bucket slot on the way out (P-a), so CONTROL(after) no longer sees `too_many_in_flight`. What
comes out the other side (954 s): the push-while-stalled returns `push (tier B): Put: nats: timeout` after the
whole budget. **The first 09-19 revision booked that as a coverage gap** ("the classifier can't name the face")
and wrote INCOMPLETE 2 — the exact move plan X27 / §5.2 forbade ("判定分支不得把 deadline exceeded 洗成 gap"):
a broker where every degraded-JS push sits the full budget would have matched the expectation with zero
deviation. The review's argument holds: since 0b204b5 (RESOLVE-BEFORE-CREATE) prepare resolves the bucket
locally and succeeds on a JS that has lost quorum, so #67's operator-facing defect (told nothing transient,
no retry vocabulary, no bound) simply moved one leg down, to the Put; the G67 wording judges (a)–(d) are
unreachable from this injection and pass=14 (not the calibrated 18) said so. The drill now judges it: a Put-leg
`nats: timeout | context deadline exceeded | no responders` on the injected push is `product_red` **#84**
(new ledger entry), and the CONTROL pushes carry X27's `--timeout 120s` (the injected push keeps the CLI
default — the one sample of the default path). The post-recovery face is judged too: on 2026-09-19 the FIRST
CONTROL(after) attempt sat ≈425 s on a JS meta that had already re-formed and the second succeeded in 192 ms;
an attempt that loses the whole --timeout on the Put leg after recovery is a second #84 `product_red`, not
"evidence". nc_gap 1 = face B (unchanged). Flips to GREEN when the product bounds a Put on a lost-quorum JS
and says so in the error (transient code + retry hint), i.e. when #84 closes.

2026-09-19, later the same day — **#84 closed, expected back to INCOMPLETE 1** (owner `#67`; the two #84
`product_red` sites stay armed as regression pins). The product fix took four images because each receipt
was a new fact, not the same one unfixed: image #6 — the Put watchdog (STREAM.INFO every 10 s, 3 strikes) cut
the injected stall at 36 s and 67 read `INCOMPLETE 1 pass=18`, but CONTROL(after) attempt 1 was cut on a
HEALTHY JS because the probe's other half (`$JS.API.INFO`) is outside the ctl's ACL and a permissions
violation looks exactly like a dead JetStream; image #7 — probe ACL-correct, the injected push showed the
INSTANT face (`Put: nats: no responders` in 0.9 s → bounded retry added) and the post-recovery attempt still
sat 121 s; image #8 — injected stall cut at 30 s, non-vacuity tooth PASS, `INCOMPLETE 1 pass=18 270 s`, but
the post-recovery attempt sat 121 s AGAIN, now worded transient (`refused 1 attempt(s) over 2m0s`) — the
post-recovery predicate had been text-only and let it pass, so it now judges on DURATION (≥100 s, next
attempt succeeds) whatever the wording, and the drill samples /jsz + the ctl's /connz every 5 s across the
CONTROL(after) window. Those samples explained the third face: 285 msgs / 25 MB entered brk1 in the first
5 s while the stream was leaderless (`leader:null`), then `leader=brk1, current=true` and `msgs`/`last_seq`
frozen for 115 s — nats-server drops publishes to a leaderless stream without a NAK, so a Put whose whole
burst lands in the election waits for acks that never come while every liveness probe says healthy. Image
#9 — the watchdog also reads `last_seq` and cuts a healthy-but-frozen stream after 3 still readings
(`stallNoProgress`, retryable): 67 `INCOMPLETE 1 pass=18` in **187 s**, CONTROL(after) recovered inside ONE
push (the ctl's own retry, visible as the `STREAM.PURGE` permissions-violation line of the first, cut
attempt). pass=18 is the G67 calibration; the drill's `_G67_AFTER_FIRST_S`/jsz replay stays as forensics.
The `STREAM.PURGE` violation line itself is by design (bucket lifecycle is the broker's) — what it used to
leave behind, chunk groups with no meta that the object reaper never sees, is #85 (broker-side chunk sweep,
`internal/broker/transfer_reconcile.go`).

G69 (2026-07-22) added a POSITIVE oracle to this drill and pass is now 18, not 17. WHY: the sub-face-4 `not_covered` gap is NOT unconditional - it fires only when the first post-grow push FAILS and the retry succeeds - and the PRE-fix baseline recorded below is itself nc_gap=1 pass=17, so 'the gap disappeared' was byte-identical to the pre-fix result and proved NOTHING (internal review G-3 caught the main process citing it as acceptance evidence). The positive oracle is checkable on EVERY run, loaded or not: after the grow, assert no `WITHOUT proving JetStream placement` degrade entry in any op timeline. Evidence: PASSED both unloaded and under 7-way saturation (the regime that originally produced '3 attempts over 8s all timed out'), with the sub-face-4 gap not firing. LIMIT: that is ONE-ARMED, not a differential - the pre-G69 arm was not built (stash-build on a 51-changed/20-new uncommitted tree). The remaining nc_gap=1 is face B, which has no deploy-tier oracle and keeps this row INCOMPLETE by construction. | face A FLIPPED PRODUCT-RED -> GREEN by G67 (2026-07-22, deploy-tier verified: verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0 not_covered=1 nc_gap=1 pass=17 (the face-A ARM is green; the drill is INCOMPLETE by construction, see below)). The refusal is now HONEST: `code=jetstream_not_ready ... after 3 attempt(s) over 8s: create_bucket: context deadline exceeded - ... usually transient ...`, where it used to be the terminal `code=bucket_create_failed create_bucket: context deadline exceeded` with no retry hint. NON-VACUITY TOOTH: brk1's own journal must show `tier-B bucket provisioning retried`. Internal review correction - this tooth is a not_covered, NOT an _as_fail, so deleting the bounded retry moves nc_gap 1->2 rather than turning the drill red; the BASELINE nc_gap for a healthy run is therefore recorded here as 1 (face B) and a run reporting 2 means the retry stopped running. The tooth deliberately does NOT accept the `gave up` line, which is emitted even for a PERMANENT single-attempt refusal. Two drill bugs were found and fixed by running it: (1) assertions written as `sh -c "... \$_G67_OUT ..."` silently tested the EMPTY STRING because the child shell does not inherit the variable - that produced two false FAILs and one VACUOUS PASS on the first post-fix run, and is why the checks now go through functions; (2) the first tooth accepted `retried|gave up`. History: this drill was created by G67 itself as #67's deterministic pin, and its oracle went through three versions, two forced by real runs - see docs/deploy-tier-gotchas.md #67. Verdict is INCOMPLETE, not GREEN, and deliberately so: face B of #67 has NO deploy-tier oracle (the only injection that reproduced it, SIGSTOP on the peer, was retired for producing connection-level failures that are a DIFFERENT defect), so the drill records it as a first-class nc_gap. A clean GREEN here would assert that #67 is closed when it is not.

2026-09-20 round-2 review (R1-F3 / R1-F4 / R3-F14 / R5-F5; product R2-F1 / R2-F3): (1) the INJECTED push is timed and
judged on DURATION first — ≥150 s is a product_red whatever the wording (the ladder's legitimate bound is ≈3×30 s probes +
9 s backoff); the wording judge for a bare `Put: nats: timeout` stays as the second branch. (2) The non-vacuity tooth's
bounded-retry branch requires `refused ≥2 attempt(s)`: `refused 1 attempt(s)` IS printed for a single attempt whose transient
face arrived with the ctx spent, so it proved nothing — a one-attempt refusal is now its own not_covered (the tooth's old
comment said the opposite). (3) CONTROL(after) attempt 1 is timed whatever its outcome, and a ≥100 s SUCCESS is a second
post-recovery product_red (a 12 MB push is seconds; 100 s of it was an uncut stall). (4) The /jsz sampler runs until the
loop ends (was 60 samples = 5 min against a 25-min loop) and is reaped by the drill's EXIT trap. (5) The success line
keeps 600 chars. Product side: `putWithJSWatchdog.cut` now honours the Put's own result (nil = the upload was complete and
only nats.go's ACL-denied trailing purge was cut; a non-artifact error goes to the classifier), so an existing-name retry
no longer re-uploads three times into `jetstream_not_ready`, and a chunk-ack `no response from stream` is retried as the
instant face instead of being laundered into 30 s no-progress windows. Receipt (image #14): injected push rc=75 after 70 s
worded `refused ≥2 attempt(s)` (the retry actually ran), tooth PASS; CONTROL(after) recovered on attempt 2 with attempt 1 at
1 s (an instant refusal, then success); INCOMPLETE 1 pass=19 = MATCH. Kept-sites 27 → 30, identity +2 product_red
+1 not_covered, one line reworded.

## 70-expose-journey

**Current final S5 serial acceptance**: native rc0/GREEN, 53 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-275/runner/70-expose-journey.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 57 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `2d15b0230013f12cedfab95aecdb9ecf23d9d897f51a866023cf462c4d780403`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-258/runner/70-expose-journey.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 71-expose-rehome-failover

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: R14: the four #29-family THIS-RUN guards (Arm E / B-silent / Arm B drain-migrate / crash-strand core) were reclassified runtime-guard→gap — they fire when the #29-family agent-tunnel-to-non-leader fixture does NOT establish, i.e. because an OPEN product defect (#29) reproduces, NOT intrinsic sim non-determinism; they turn GREEN when #29 lands, so they are debt owned by #29 (matching line-291's sibling #31 gap). Landing verdict unchanged. R9-D: arm B is now P1/R8s POSITIVE verifier — r9d-a measured drain rc=0 + migrated+serving + ZERO agent re-registrations, assert_fail=0. The 2 gaps are long-registered. r9d-d re-confirmed it (assert_fail=0, pass=25, drain rc=0). r9d-b/c aborted SETUP-RED on the expose_serve_sentinel fixture — root cause: this drill TITLE gained an apostrophe and the sentinel token embeds $_AS_DRILL into a single-quoted sh -c payload; token now sanitised + bounded-retried. Was ASSERT-FAIL (P1 #29 + H9)

## 72-proxy-subscription

**Current final S5 serial acceptance**: native rc0/GREEN, 90 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-275/runner/72-proxy-subscription.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

- **batch**: `-`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

stable across both runs


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 100 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `2d15b0230013f12cedfab95aecdb9ecf23d9d897f51a866023cf462c4d780403`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-258/runner/72-proxy-subscription.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 73-proxy-cluster-ha

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

simcluster-speed M0 (2026-09-19): expected GREEN 0 → **INCOMPLETE 1**, owner gains **#33**. The REHOME arm's `[#33]` line was a measure-and-record `assert_ok` whose predicate accepted BOTH outcomes (`AUTO-RECOVERED || STRANDED`) — a pass that could not fail — so the drill was GREEN while #33 stayed open with no non-GREEN owner; tests/ledger-crosscheck.sh did not notice because the ledger's own "#32 (CANDIDATE)" cross-reference sat in #33's first three lines and read as #33's status. The measurement is unchanged (and now also records which broker held the exit agent's NATS connection at the kill and after — the fact the evidence-flip protocol in docs/reviews/simcluster-speed-plan.md §0 D-P needs); it is filed as an explicit `not_covered … gap` until that protocol closes #33, at which point the line becomes the positive "auto-recovers within observed max + slack" assertion and this row returns to GREEN 0. Claim strengthened, not weakened: the drill now says out loud what it was silently accepting.

simcluster-speed 2b (2026-09-19, later the same day): **the flip happened — INCOMPLETE 1 → GREEN 0, #33 FIXED (by #80)**.
The protocol asked for the mechanism receipt, not for a run that happened to recover: 7 of 7 measurable samples
(73r1–r4 solo, G1 cap 0 and cap 5, live-grow #2) had agt's NATS connection ON the killed broker at the kill and on a
survivor afterwards, AUTO-RECOVERED 16–29 s after the crash with the data plane ≤1 s behind the control plane — the
old face (control rehomed+ready, data plane dark for minutes, `proxy off/on` to recover) is #80's root cause seen
from the exit's side (the SS server was anchored on the per-session runCtx; a crash-rehome killed it with the
session). The REHOME `[#33]` not_covered is now `REHOME [#33 FIXED by #80]`, an assert_ok with three conjuncts on
every run: connection was on the killed broker (`_brk_holding_agent`), the slog shows `agent: re-registered after
reconnect` past a pre-kill cursor (nats.go reconnects inside the roster pool; the agent's own "rebuilding NATS
session on the freshest roster" is the stuck-disconnect path and does not run here — the first flip draft asserted
that line and went red on a healthy run), and the data plane flows within 90 s (observed max 29 s + slack; an
observation, not an SLA). First solo of the flipped drill: GREEN pass=46. What stays loud: the #34 face-1 drift
(constructed spread moving before the kill) is still a first-class `product_red` DEVIATION — it fired once in the
G1 cap-8 round and now dumps the leader's proxy/rehome events + reconcile slog lines when it does.

R15: R14: the QUORUM data-plane-separation THIS-RUN guard (384) was reclassified runtime-guard→gap — it fires when a rebalance-MOVED dead-homed exit fails to render+serve, i.e. the CONFIRMED product defect #33/#34 reproducing, NOT intrinsic sim non-determinism; it turns GREEN when moved-exit rendering is made deterministic in the product (matching line-381's sibling gap). Landing verdict unchanged. r9d-a/b/d=INCOMPLETE r9d-c=ASSERT-FAIL(Q-xcheck endpoint mismatch — the drills own registered exposure) — flake band unchanged. R9-D: the REHOME live-target gate no longer reports #34 drift as a broken foundation; it records product_red "#34" and SKIPS the arm (proved live by a forced-drift mutation run)

2026-09-20 round-2 review (R6-4 / R3-F13 / R1-F5 / R5-F6 / R6-1): the flipped `[#33 FIXED by #80]` assert was one conjunction
of four, two of which were not product properties. It is now TWO claims: the HARD claim `[#33 FIXED]` = AUTO-RECOVERED ∧
≤90 s (the observed maximum with slack, an observation not an SLA); and the mechanism observation `[#33 mechanism: pool
reconnect]` (connection was on the killed broker → moved to a survivor → `agent: re-registered after reconnect` after the
cursor), asserted ONLY when conn==home held at kill time — that equality is a fixture correlation (allocation homes an exit
on the agent's NATS server; `cluster rebalance proxy` moves homes without moving connections), so a run where the agent's
NATS sat on a survivor takes the tunnel-only path and would have been a false red; it is a not_covered gap instead. The
plan's third conjunct was misnamed: the drill asserts the nats.go RECONNECT line, not the session-rebuild line (the
first flipped run went red on that, batch21/73flip.log), and no sample exercised #80's runCtx path — the ledger heading
now says "symptom closed; mechanism attribution #80 = CANDIDATE". Q arm: Q-xcheck is POLLED into agreement for 60 s
before it is asserted (3/14 samples read `vended=brk1 ≠ home=brk3` one-shot right after a leg that still served through
the previous home; a mismatch outliving 60 s is the defect face and now transcribes `_drift_evidence`). Receipts (image
#14): round-2 -j5 run — hard + mechanism PASS (AUTO-RECOVERED 23 s, pre=brk2 post=brk1), Q-xcheck one-shot mismatch
(the 3rd sample, before the poll); solo re-run after the poll: GREEN pass=47 — hard + mechanism PASS (AUTO-RECOVERED
27 s, pre=brk3 post=brk1), Q-xcheck PASS. Expected stays GREEN 0; a run whose precondition fails is a loud INCOMPLETE 1
DEVIATION whose text says why, by design.

## 74-rebalance-on-return

  sig:b-negctrl-create := negative-control expose reg create rc=70

  sig:c-ss-preflow := poll_until: timed out .* C pre-kill SS flows via
  <!-- MAJOR-2 (round-2 review): C-ss-pre is now stage-classified like B-dp — a harness-* failure (/sub
       fetch or ss-local bind) becomes a not_covered gap, NOT this ASSERT-FAIL, so this band matches ONLY
       a genuine product strand (the local SS client PROVEN READY, bytes still not flowing). The band is a
       real #34 reproduction, not a harness flake dressed as one. -->

- **batch**: `R15`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R15: r1a=GREEN r1b=ASSERT-FAIL r2=INCOMPLETE — flake band

2026-09-19 simcluster-speed ARM SPLIT (plan §5.6, X18/X30): two units, `74-rebalance-on-return.SRAB` (the
manual path: SKEW-reconstruct → every exit flowing → SKEW → RETURN → A default-off → B) and
`74-rebalance-on-return.C` (the automatic path). Until the split C ran after B on the same cluster and inherited
B's `reg` negative-control expose; it now builds its own pre-control (`C-negctrl-fixture`, `-pre`, and its own
gap when that does not establish). The two bands move to the arm they pin, each under an arm-suffixed slug with
the SAME ERE for now — to be re-calibrated from each unit's first solo log (X18): `#67@b-negctrl-create` → .SRAB
(the B negative-control create is a SRAB claim), `#34@c-ss-preflow` → .C. The parent row keeps `-` bands and `-`
nc_gap (derived; both children are `-` because the drill's gap set was never deterministic — see the arm
sections). The #34 persistent gap is drill-level (X8) and is counted by both arms.

2026-09-20 round-2 review (R6-3 / R1-F8 / R6-14): the persistent `_gap_drill_level` text now names ONLY face 1 (the
constructed spread drifting, 1/13 concurrent samples, unattributed; candidate mechanisms in ledger #34 incl. the M3 rotate
re-minting the home from the agent's NATS server): the rc=64 negctrl face was #86 (fixed), and auto-rebalance-on-return DOES
fire (C5/C7/C8/C9/C10, lg2, S2, V7 all `proxy_auto_rebalanced 0→1`), so "blocked by the #31 fire-gate" left the gap text.
The two arm bands (`ASSERT-FAIL@#67@sig:b-negctrl-create-SRAB`, rc=70; `ASSERT-FAIL@#34@sig:c-ss-preflow-C`) were carried
from the unsplit drill "to be re-calibrated from the first solo log" and never fired in ≥11 SRAB / ≥12 C samples; they are
RETIRED (bands → `-`; the `sig:` definitions below stay as history). A future rc=70 or SS-preflow timeout arrives as a
DEVIATION to attribute, not as a match to a band with no living sample. Post-split receipts (all image #10–#13, plan §8.5b):
SRAB solos S1–S5 402/414/406/371/403 s, C solos C1–C10 (C1/C6 harness-invalid, C2–C4 #86 pre-fix, C5/C7/C8/C9/C10 INCOMPLETE
pass=31 with `0→1`), live-grow SRAB 2/2 C 1/2, S2 -j6 and V7 -j12 one each.

## 74-rebalance-on-return.SRAB

  sig:b-negctrl-create-SRAB := negative-control expose reg create rc=70

nc_gap `-`: this arm's gaps are all conditional on where the #34 instability bites (SKEW-reconstruct failing →
"destructive arms THIS RUN"; B pre-flow failing → "B injection THIS RUN"; B-move / B-dp harness-stage /
B-negctrl pre-control each have their own), so the count ranges from 1 (drill-level only, a clean run) upward
and pinning any single value would make every other run a standing deviation. The verdict enum is pinned.

## 74-rebalance-on-return.C

  sig:c-ss-preflow-C := poll_until: timed out .* C pre-kill SS flows via

nc_gap `-` for the same reason as SRAB: C-negctrl-fixture, C-ss-pre harness stage, C-skew-adjacent, the
invalid-edge gap and the C-dp-when-auto-did-not-fire gap are each conditional on #34 / #31 manifesting. The
band keeps the round-2 MAJOR-2 discipline: only a strand with the local SS client PROVEN READY matches it;
a harness-* stage is a gap, not this ASSERT-FAIL.

## 78-proxy-dial-backoff

- **batch**: `g75-g78`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

Deploy-tier regression for gotcha #78 (agent proxy first-dial exponential backoff + broker read-REGISTER
WARN damping + `proxy.participate` opt-out). Verdict **INCOMPLETE** (1 nc_gap):
- **Arm A** (agent backoff): dial cadence via the netfilter attempt-counter (NOT product logs) — total
  attempts must fall well below the no-backoff ~39 baseline (≤20) AND show a decreasing per-bucket
  trend. Fault-speed-decoupled (an absolute bucket count would couple to REJECT's fast-fail vs a slow
  half-open timeout). GREEN on fixed binary (measured 6/4/3, total 13).
- **Arm B** (operator bypass): `proxy off/on` mints a new epoch and establishes within 60s — no
  backoff hangover. GREEN.
- **Arm C** (broker WARN damping): **not_covered gap** — the `read REGISTER` WARN site is post-TLS
  (control listener is `tls.NewListener`), so a raw junk-TCP storm fails at the TLS handshake and hits
  the `accept` site, never reaching read-REGISTER (measured WARN delta 0). The half-open
  TLS-completed-then-EOF shape needs a real TLS middlebox this sim doesn't front — same limit as the
  header's WSS [NOT] clause. Hermetically covered by `internal/tunnel/register_log_damping_test.go`.
- **Arm D** (opt-out): `proxy.participate: false` → zero dials across a live repair loop + allocation
  freed + `proxy status` shows opted-out, then flip back re-enters the pool and serves. GREEN — the
  strongest deploy-tier evidence for #78.

Mutation note (against a pre-fix v0.5.0 binary): arms A/D predicted RED, arm B green. Plan:
`docs/reviews/g75-g78-deploy-defaults-plan.md` §D8.

## 80-session-isolation

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #25 CLOSED (per-IP PIN rate limit). Arm R rewritten to a POSITIVE regression — same-IP wrong-PIN storm then the 11th same-IP CORRECT-PIN join is REFUSED at CONNECT (assert_refuses), guarded by _rl_logged (broker rate-limit log) + R-2ndsrc (different IP still joins = per-IP scope) + E-pos (under-threshold correct PIN succeeds). Was PRODUCT-RED (#25)

## 81-admin-evict-session-rm

- **batch**: `R12`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R12 flip: #26 CLOSED (evict reaps managed OS children). C-GAP-proc → C-reap: after evict the setsid-nohup managed child is GONE from the host process table (daemon exited AND pgrep empty); C-base-proc first proves it was running. C-sysd-reap: reaps under systemd too. Was PRODUCT-RED (#26)

2026-09-20 round-2 review (R6-7): B3's refusal oracle read the wrong stream — the auth_callout rejection lands in the
agent's BOOT stream, not slog. `_b3_refused_for_auth` now takes a cursor (`sim_agent_panic_cursor agt1`), requires rc≠0 AND
`sim_agent_panic_sink_since agt1 'auth_callout rejected|Authorization Violation' <cursor>`; the claim is one assert_ok
(equal or stronger than the assert_refuses it replaced — the refusal is proven by the reason, not the rc alone). Receipt:
81b solo GREEN pass=40 (batch21).

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

**Current final S5 serial acceptance**: native rc0/GREEN, 128 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/94-agent-reconcile.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13-D6: `ps` LOST is now a REAL assertion (was an overclaim — the header/title named it but the drill only checked NODE status). A1d/e/f: agt1's exec children DERIVE LOST while agt1 is OFFLINE (storage-RUNNING row + OFFLINE owning node, exec.go:326-345) while agt2's stays RUNNING (the discriminator) — closing the RUNNING(A0d)→LOST(A1)→EXITED(A2) three-state chain. r13d GREEN pass=54. Was GREEN (stable both runs)

2026-09-20 round-2 review (R6-7): 94's B3-timeout in the 2026-09-03 table was #87 (the orphan-kill fail-closed gate read a
RUNNING/LOST snapshot as "history", so a node whose jobs had all exited never received the drop — fixed 2026-09-19,
`proc.NodeHasHistory`). B5 reworded, not re-judged: DOC-25 closed — `agent: re-registered after reconnect` now carries
reconciled/drop_procs/revoke_ports, and B5 reads it instead of inferring the directive from the kill line. Receipt: 94c
solo GREEN pass=54 (batch21, image #13).


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 128 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/94-agent-reconcile.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 95-broker-selfheal

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13 (item 2): 95-D was a FALSE gap (R6: _d_raft_ok hard-pinned leader=="brk1", which T1a+T2a make impossible ⇒ a false not_covered). Predicate tightened to leader-EXISTS-AND-STABLE (any voter, not brk1); D-neg1/D-neg2 prove it can RED (no-leader + churn). The fix un-masked a bogus `session rm --yes` flag (removed — session.go has only --ack-alerts) → DELETING now constructs; then un-masked the ONE-SHOT boot resumer's 1s-JS-probe race (broker.go:1023-1038, no retry) → D4b waits for JS meta to re-form before D5. r13e GREEN pass=44: DELETING parked (D3) + boot resume finished the delete (D6b) end-to-end. Was INCOMPLETE (95-D-suspect-false-gap)

## 96-mid-flight-chaos

- **batch**: `R16`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

2026-09-19 simcluster-speed ARM SPLIT (plan §5.5 "96 首拆", X5/X8/X16/X22): the drill is now three units,
`96-mid-flight-chaos.{A,D,F}`, each on its own fresh N=3; the parent row is derived from the child rows.
nc_gap 5 → **7**, in two steps, neither of them a new gap:
- **5 was stale.** The 2026-09-04 sweep note already said the declared gaps were 6 (and 6 solo too); the
  unsplit solo run on 2026-09-19 (image #2, 1151 s) is nc=6 again: `:346` #58 cross-home GC (structural),
  `:368` arms B/C (drill-level), `:408` #57 transfer completed before the crash, `:528` B0 not refused,
  `:678` D6b legitimate majority commit, `:745` F gated off — the cluster did not return to full health
  within 360 s of D's heal, even solo. The sixth site is that F precondition (X16).
- **The split moves two sites and counts one thrice.** The F precondition gap (`:745`) is structurally
  gone — F starts from its own fixture — and its 360 s window becomes the D arm's END-STATE
  measure-and-record (the recovery lag is logged; >360 s is a gap, as before, but on D's row). The
  drill-level gap (`:368`) is `_gap_drill_level`, called by every arm (X8), so it counts once per arm.
  6 − 1 (F precondition) + 2 (drill-level on D and F) = 7.
Child rows, written from claim ownership BEFORE the first split run (X5):
- `.A` INCOMPLETE **`-`** (see `## 96-mid-flight-chaos.A` below): #58 cross-home (structural) + drill-level
  + the #57 branch (both of its in-sim outcomes — "audit unreadable" and "completed before the crash" —
  are gaps; only a real #57 pin is PRODUCT-RED) + B0 (the kill's alert state has never gated `run` here)
  = 4, plus a 5th whenever the A2 arm finds NO orphan set to reap (the vacuous-reap guard). The first
  two samples went both ways (unsplit 96a: 444 stranded objects, 4 gaps; split 96A: 2 objects, 5 gaps),
  so pinning either number would make every other run a standing "deviation" that trains the reader to
  wave deviations through. The verdict enum is still pinned; only the count is two-valued.
- `.D` INCOMPLETE **2** = drill-level + D6b (on this host the branch is "legitimate majority commit" or
  "minority-write variant structurally unreachable"; both gaps). A 3rd gap = the end-state recovery
  measure exceeding 360 s — a product recovery-cost observation the split exists to surface, reported as
  a DEVIATION. `# forgoes:` names `71-minority-commit`: #71's world was the post-A cluster; the registry
  row is `none-after-split`.
- `.F` INCOMPLETE **1** = drill-level only; every F claim is a positive.
Identity: two claims reworded on purpose (assert-identity baseline header, same change): D0a no longer
says "after the #58 arm restarted it"; the F cross-arm-residue gap became the D end-state gap.

**First solo receipts (2026-09-19, image #2) and what they taught:**
- `.F` run 1: ASSERT-FAIL — F4 (agt2's held seed no longer RUNNING / a seed row closed) and F5 (no
  `reconciled_closed` row within 120 s) with nothing but poll timeouts on record; run 2 (with the F4/F5
  diagnostics now in the drill): INCOMPLETE nc=1 = the child row, F4/F5 PASS, agt2's seed still RUNNING by
  pid, both agt1 seeds `reconciled_closed rc=-1`. 1/2 red, unattributed — the next red carries the tables.
  NOT written into the expectation; a DEVIATION to attribute in the sweep.
- `.D` runs 1–2: INCOMPLETE nc=3 — the end-state measure timed out at 360 s BOTH times, and run 2's
  diagnostic said why: `three_voters=yes agt1_online=no agt2_online=no`, `node ls` → **"(no nodes)"**.
  The cluster was healthy; the ctl was listing the wrong session. D3's survivor write and D4b's minority
  write are `session create canary2/canary3`, and a `session create` moves the HOME's active-session
  pointer (drill 30's PROBE-ISOLATION documents the same trap). `_f_precond_healthy` — the D end-state
  measure, and BEFORE the split the F arm's 360 s precondition — polls `node ls` under that pointer, so
  it could never see lab's agents after D3. **That is the most likely reason the old `:745` gap fired on
  every run for months ("did not fully recover within 240 s/360 s"): the F arm was gated off by the
  harness's own session pointer, not by tether's recovery.** Fix (D7, an added claim): re-login the ctl
  into $SID before the measure, as D0d does before the injection. Run 3 (328 s, with D7): INCOMPLETE
  nc=2 = the child row, and full health was back **3 s** after the D6b readback settled — the "recovery
  lag" was the pointer, entirely. The child row stays INCOMPLETE 2 (drill-level + D6b); a 3rd gap after
  D7 would be a real recovery lag.
- `.A` run 1: INCOMPLETE nc=5 (the two-valued A2 leg, see `## 96-mid-flight-chaos.A`).
Per-arm wall (solo, s): F 398 / 220, D 681 / 695 / 328 (with D7), A 333 — max 333 against the unsplit
1151; the declared `# worst:` (A 1350, D 1500, F 800) are the pre-split estimates and will be re-seeded
from the sweep.

2026-09-20 round-2 review (R1-F1 / R5-F2 — BLOCKER; R1-F6; R6-12): the three concurrent 96.D PRODUCT-RED of 2026-09-19 (G1 cap 0,
G1 cap 8, S2 -j6) printed `PRODUCT-RED #65` from the pre-heal committer snapshot (`brk1's OWN broker.log names canary3
while ISOLATED? yes`); the plan and this log had filed them as "#71 sensor / LOAD-SENSITIVE" with the registry row at
`none-after-split`. That was a mis-filing of the drill's own decisive #65 reading. Attribution: gotcha #89 — brk1's
BROKER was not on brk1's NATS (nats.go's discovered-server pool moved it to a peer after the topology reconciler's hard
restart of the local nats-server under load); with routes+raft cut it forwarded the ctl's create through brk2's NATS to
the live leader, answered rc=0 in a second and logged `session created`. #65 stays REFUTED; #89 is FIXED (broker pinned to
its configured server). The D arm now: `D0f PREMISE (#89)` asserts every broker's /connz holds exactly one loopback
`tetherd` (red = ASSERT-FAIL), the D6b #65 judge is gated on it (premise red → runtime-guard, never a false #65), and a
committer CENSUS over all three brokers is logged beside the snapshot. Registry: `71-minority-commit default
96-mid-flight-chaos.D`; manifest `# forgoes: D=-`. Receipt (image #14): D0f PASS, census brk1=no brk2=yes brk3=no,
INCOMPLETE 2 = MATCH. .A: the post-restart "no orphan manufactured" record is `runtime-guard` like the pre-restart one
(same fact read after the restart), so nc_gap is deterministic: .A pinned INCOMPLETE 4 (round-2 run: nc_gap=4 nc_guard=1),
parent INCOMPLETE 7 (4+2+1) — the `-` that had switched off added-gap detection is gone. .F: owner column names #88
(an INCOMPLETE row may own a CANDIDATE; ledger-crosscheck now prints ok instead of R6-CAND); #88 status = 5/9 samples
unattributed, see the ledger.

## 96-mid-flight-chaos.A

nc_gap is `-` because the A2 (#58) leg is two-valued by construction and both values are honest gaps:
the 1 GiB in-flight pull is raced against `docker kill brk2`, and on this host it sometimes strands
hundreds of objects (unsplit run 2026-09-19: 444 above a floor of 1 → the R4-F3 no-verdict branch, 4 gaps
in all) and sometimes drains before the kill lands (split run 2026-09-19: 2 above 1, below the tombstone
floor of 6 → the vacuous-reap guard records a 5th gap). Neither is a regression and neither is a pass;
the arm cannot make the race deterministic without bandwidth-shaping the agent (ruled out in the drill
header). The other four gaps are constant: `:346`-class #58 cross-home GC (structural), the drill-level
B/C gap, the #57 branch (both in-sim outcomes are gaps), and B0. Expected verdict stays INCOMPLETE; a
PRODUCT-RED here (a real #57 pin, or a #58 orphan that survives the reap) is a DEVIATION as before.

ROUND-4 R4-F3 (2026-07-23): the #58 cross-home GC deploy-tier gap is now booked EXACTLY ONCE - unconditionally, in the A-arm setup. The duplicate registration under the same title in the A2 branch is DELETED, so the coverage account no longer depends on which branch the A-arm takes (a run that reaches A2 now records one fewer nc_gap than the R14/R15 counts quoted below). The gap itself is unchanged and still OPEN: a >15m run is what would close it. | ROUND-3 R3-F3 (2026-07-23): the #58 arm no longer compresses xfer_cross_home_reap_age - external review F2 clamped that production knob to >= 15m (a lower floor lets the leader delete an object still live on ANOTHER home), so a 5s value can no longer even LOAD. The FIXED/REGRESSION/SPLIT-HOME judges that depended on it are DELETED, not relocated; the arm records an unconditional not_covered instead. Only the CADENCE knob (xfer_reap_interval) is still compressed. The #57 arm now brings brk2 back and waits for the finalize-on-recovery pass BEFORE judging - the previous revision declared #57 'forever' while the crashed home was still down, so it measured the crash rather than the product's recovery and could neither certify nor refute R16/G67. | R16 (2026-07-22 deploy-tier): product_red 1→0, assert_fail 1→0, pass=38. Lane B (#57 finalize-on-recovery: node-local durable in-flight ledger + a DETERMINISTIC synthetic terminal committed BEFORE the ledger is deleted) and Lane C (#58 leader cross-home GC for a bucket no HOME can reap) both SHIPPED and are pinned HERMETICALLY. Their DEPLOY-TIER demonstration did NOT happen: the A-arm's 1 GiB tier-B upload again reached a terminal before the docker kill (the standing in-sim interruption gap), so no chunks were stranded — peak orphan count 2 vs tombstone floor 6. R16 therefore ADDED a NON-VACUITY GATE to the #58 arm: when the peak orphan count never exceeds the floor the arm records not_covered instead of banking a 'count is at the floor' PASS that would assert the reap works on a run where no reap was needed. #57/#58 stay OPEN in the ledger — the product fix is in, the deploy-tier proof is owed. Drill also fixed: the leader-side #58 knobs (xfer_reap_interval + the new xfer_cross_home_reap_age) now load via a restart that RE-ESTABLISHES brk1 as leader — the cross-home GC is leader-only, so the first attempt put the compressed knobs on a node that had just lost leadership. | R15: R15: R14 drill flips: Q3 held-foreground seeds (F0/F0b `tether exec --timeout 30m -- sleep N` HELD by tether, not `nohup sleep &` — F0c/F3/F4 now ask a real RUNNING↔EXITED question the old fixture made self-contradictory); Q4 D3 clean best-effort-success positive; D6b + COMMITTER ATTRIBUTION (reads brk1's own broker.log 'session created' line) correctly separates a queue-group MAJORITY commit from a true #65 — r14d canary3 was durable on all 3 brokers yet brk1 did NOT commit it ⇒ recorded NOT-#65 (R6's exact insight, was the old ledger's phantom '5/6 durable minority writes'). #57 is the current PRODUCT-RED driver (see owner); #58-split-home now counts under nc_gap (deterministic structural cause, NOT a PINS-LIVE leak — retires at per-transfer-owner refinement). Reclassified runtime-guard→gap: #57 dangling-audit (determinization MEASURED insufficient — 1 GiB STILL completes before the docker-kill on the 88-vCPU host; bandwidth-shaping would destabilise the cluster; hermetic-owned) + #57 audit-unreadable (audit sits on the killed home broker) + D6b minority-write (R6: isolated minority can't auth a fresh CLI connection, rc=69) + #58 split-home (drill line 447: deterministic structural cause, a defect-tied gap). A-arm payload 12MiB→1GiB (helps #58 strand orphans + sometimes catches #57 IN-FLIGHT = live PRODUCT-RED). r14d nc_guard=0 EVERY run — TERMINAL-GATE CLEAN (all 7 of 96's runtime-guards eliminated; #58-split-home now counts under nc_gap: nc_gap=5 nc_guard=0). A1e (the #57 anti-vacuity control) fixed: it ran on agt1 whose HOME broker brk2 is dead when #57 pins → a false ASSERT-FAIL; now runs on agt2 (homed on the live brk1) so #57 lands PRODUCT-RED not ASSERT-FAIL. D3(Q4) is a DETERMINISTIC POSITIVE now (best-effort-success fix killed the apply-lag non-idempotent flake — the D3 that was 60× red). F-arm (Q3 held seeds) is GATED as a gap every run: arm-D partition recovery to FULL health (brk1 re-VOTER + agt2 re-ONLINE off its just-healed home) is >360s in-sim, so the double-fault arm is not run over cross-arm residue (a pre-existing gap, NOT caused by Q3 — the held-seed fixture is sound by exec.go:192 inspection). Was UNSTABLE (flake band)

## 97-soak-cycles

- **batch**: `R13`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

R13: the PERMANENT goroutine NOT-COVERED is RETIRED — admin runtime .goroutines (runtime.NumGoroutine, the in-process truth, not /proc Threads) enables a real cross-process leak gate: load(6-cycle soak)→quiesce→FLOOR returns to the pre-load baseline within GOR_TOL. Contract FIXED in the drill header (tol=2*NPROC+16, 3+3 floor samples, GOR_QUIESCE=SOAK_SETTLE). r13d GREEN pass=43 (brk1 floor 76→77, tol 192 on the 88-core host). Was GREEN (stable both runs)

## 83-cloned-image-instances

- **batch**: `cloned-credential-instances`  _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

PRODUCT-RED RECORDED, then GREEN. A drill that has only ever been seen green proves nothing, so both readings were taken on the same host, same image build, minutes apart. Against **pre-increment code (HEAD cdce494)**: pass=12 fail=4 — B1 (the clone never becomes a second row), B2b/B2c (ONE `exec` produced TWO start rows and TWO exit rows: the live-fleet double-execution itself), B3 (`node_not_found` — a cloned instance is unaddressable). Against the increment: **pass=16 fail=0**. The four RED arms are exactly the four the increment exists to fix, which is the property that makes this drill an oracle rather than a ritual. It also earned its keep mid-development: after all three hermetic gates were green (`make test` rc=0, `e2e-parallel` ALL PASS, `lint` rc=0) it caught B1/B2b/B2c/B3 again on an intermediate revision — the incumbent's register→subscribe turnaround on a loaded container host is ~2s, and `leaseGrantWindow` had been shrunk to 1s while fixing an unrelated hermetic red. Every hermetic test registers and subscribes inside one fast process, so none of them can see that window at all. See docs/reviews/cloned-credential-instances-review.md §3e.

## 84-shared-home-instances

- **batch**: `cloned-credential-instances` _(expected/owner are authoritative in expected-verdicts.tsv — not duplicated here, MI6)_

ADDED IN RESPONSE TO EXTERNAL REVIEW F14. Drill 83 copies a home between containers, which reproduces
the CREDENTIAL sharing but not the reference deployment's actual shape — on the live fleet ~/.tether is
an NFS mount, so the instances of one cloned image write ONE state.json and ONE upgrade marker. 83 was
registered GREEN while that highest-risk form had no deploy-tier oracle at all. This drill is that
oracle: `up --shared-agent-home` mounts a single named volume as `/home/sim` on every agent, and
the first SETUP assertion checks both the `.tether` directory and `.local/bin/tether` device/inode
pair on both containers. A false green can no longer come from sharing only state while silently
leaving the binary private.

First run: **pass=17 assert_fail=0 product_red=0**, and the arms that matter all held on genuinely
shared storage — two distinct rows (incumbent keeps its name, the second is leased `-02`), ONE start
row and ONE exit row for one `exec`, the leased instance separately addressable, and the incumbent's
persisted port token SURVIVING the leased instance's arrival (a leased instance must not write the file
that belongs to the basename holder).

Final external-review rerun closes the former S6 gap instead of asserting ownership that the topology
cannot provide: remote upgrade is now refused for both the leased row and its basename once a clone
family exists. The drill checks both refusals with the stable `clone_family_upgrade_unsupported` code,
before any artifact download. Expected verdict is therefore GREEN; the supported upgrade path for a
clone family is rebuilding the source image and restarting its instances.

## 2026-09-03 — 六条「HEAD 就红」的登记表过期项（发布前审计 deploy-tier 复跑）

**这一节故意不改 `expected-verdicts.tsv`。** 下面六条在跑全量 sweep 时偏离登记表，经 HEAD 基线
复跑证实**在增量 2 之前就已经是红的**——即它们是**登记表过期**，不是回归。表上次校准是 2026-08-19
的 `cloned-credential-instances` 批次，此后没有人跑过全量。

| drill | 现状 | HEAD 基线 | 首个失败 |
|---|---|---|---|
| `30-rolling-upgrade` | ASSERT-FAIL | ASSERT-FAIL | PHASE-1 CONTINUITY（写探针命中失败签名） |
| `52-credential-rotation` | ASSERT-FAIL | ASSERT-FAIL | A8d broker slog 的 pin-mismatch 拒绝行 |
| `60-user-journey` | ASSERT-FAIL | ASSERT-FAIL | J-G.3c-2 首次 post-login `node ls -a` |
| `67-transient-js-refusal` | INFRA-ABORT | INFRA-ABORT | （abort，无 verdict 行） |
| `81-admin-evict-session-rm` | ASSERT-FAIL | ASSERT-FAIL | B3 重连被拒但**不是**期望的那个理由 |
| `94-agent-reconcile` | ASSERT-FAIL | ASSERT-FAIL | B3-timeout 孤儿进程未被 KILL |

**为什么不把 `expected` 改成 ASSERT-FAIL 让表变干净**：那等于把六个**未经分诊**的红永久祝福掉。
一条登记为 expected 的红就不再是信号——它连偏离都不算，下一个人看到的是一片安静。simcluster 的
Mandate 是「如实暴露缺陷，绝不替 tether 弥补」，把红写进期望正是最省事的那种弥补。所以它们继续
以 DEVIATION 的形式响着，直到有人真的去分诊。

**分诊时的两条已知线索**（本轮顺手取到的，不构成结论）：

- `81` 的 agent 打印了正常启动横幅后 rc=70 退出，stdout 里**没有**认证拒绝——它的 slog 落在
  log sink 文件（`~/.tether/agent/<sid>/agent.log`）里，而 drill grep 的是 stdout。
  若属实，这是 drill 的取证面问题而非产品问题。
- `30` 的 PHASE-1 命中已由 watcher 抓到 scene：命中瞬间 **leader 未换届、HEALTHY-HA、三节点全
  VOTER、LAG=0、TOPO 收敛**——不符合 gotcha #66 的 leader-hop 机理，是 #66 条目里外审样本 #4
  记的那个**已登记未定性**的 phase-1 形态。**scene 记了失败行的行号却没记内容**
  （`first failure-signature line: 14:WRITEFAIL`），而探针日志随容器销毁——
  这正是该形态被反复目击却始终无法定性的原因，watcher 的取证面值得补。

**方法论**（值得记住的那一条）：**过期的登记表长得跟回归一模一样**。区分二者的唯一办法是拿基线
commit 建 git worktree、用基线源码烘镜像、跑基线的 drill——不能靠读表，也不能靠推理改动面。
本轮正是这么把 8 条偏离拆成「6 条过期 + 2 条真回归」的。

## 2026-09-04 — 全量复跑：五条仍是登记表过期，第六条不是

发布前审计收尾时按用户要求跑了完整 43 条（`-j6`）。上一节那六条「HEAD 就红」里，
`52`/`60`/`67`/`81`/`94` 的**首个失败签名逐字未变**，仍是登记表过期。

**`30-rolling-upgrade` 是例外，而且差点被同一个标签盖过去。** 它这次的首个失败是
`UNLOCK-safety`，而上一节记的是 `PHASE-1 CONTINUITY`——**签名变了**。拿 `021c970` 建 worktree、
用它自己的源码烘独立镜像 `tether-sim:base021` 单跑，基线是
`INCOMPLETE assert_fail=0 pass=53`（干净），当前树是 `ASSERT-FAIL assert_fail=1`。所以它是
本增量带来的差异，不是过期。

追下去是**有意变更**：本轮外审 H-2 的 `PlanExpireUpgradeLease` 让 HALT 的编排者退出时主动把
自己的租约盖成已过期、同时保留 marker（marker 继续围栏 join/retire，过期租约放行「修好后重跑」）。
那条断言的前提「编排者刚死几秒、可能还有人在续租」正是被这次变更作废的。已把该臂改为断言新行为
（`UNLOCK-halt-admits`），并把**不能在本 drill 构造**的另一半（活租约的拒绝 / `--force` 覆盖）
以 `not_covered … gap` 登记，指向 `cmd/tether/cluster_unlock_test.go` 的
`TestUnlockRefusesALiveLease` + `TestUnlockForceOverridesALiveLease`。复跑
`INCOMPLETE assert_fail=0 pass=52`。

**教训**：六条一起登记为「过期」之后，第七次看到它们时最省事的动作是整批跳过。
**签名是唯一能拦住这个动作的东西**——它变了就必须重新拿基线跑一次，否则一条真变更会藏在
一条已知红的后面。

其余：`40`（SETUP-RED→单跑 GREEN）与 `74`（ASSERT-FAIL×2→单跑 INCOMPLETE/assert_fail=0）是
负载敏感；`95` 三次三个样子（`D6b` / `T2c`+`T2f`+`D0` / 清空容器后 GREEN pass=44），失败全是
wall-clock 轮询超时，证据里 `fsync_4k_ms=15.3`。`96` 的声明式缺口是 6 条而登记为 5，且**单跑同样
是 6**——表的注记写成「`-j6` 下的 #71」不准确。以上均**未**写进 `expected-verdicts.tsv`。

**结案（2026-09-20，simcluster-speed round-2 review R6-7 补记）**：上面两节登记的六条「登记表过期」项在
2026-09-19 的 simcluster-speed 增量里逐条分诊完毕，处置写在各自的 `## <drill>` 段里：`52` / `60` / `81` 是 oracle
读错了流或列（52 BOOT journal、60 KIND 列、81 agent BOOT 流 + cursor），产品文本没变，solo 全 GREEN；`94` 的 B3
超时是产品缺陷 #87（已修）；`67` 的 CONTROL(after) 面是 #84（已修，三张脸）；`30` 是负载面（无变更）。「直到有人真的
去分诊」——已分诊；「five remain stale」——不再成立。

## 39-hot-update-legacy-n1

**Current final parent receipt:** X0/Y3/P3/F3, frozen P pin unchanged; unpinned F272 native2 reconfirms exact reviewed reset, pinned F3. Parent native 0, wall 670.711 s; exact command `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/39-parent/receipt.json`. One deadline-fixed product image/tree; all product and drill inputs unchanged through serial acceptance.


S3 N-1 boundary, **REAL RUN 2026-10-02: PRODUCT-RED (derived), NO DEVIATIONS, three sweeps.** Parent PRODUCT-RED is DERIVED from X GREEN and Y PRODUCT-RED.
Owner: hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration. Both arms use the real
`vendor/tether-prev` with source receipt, in-container SHA, v0.6.0 tag and absent image-ABI line verified.
No arbitrary failure band is registered.


**Final S5 parent receipt:** Final serial parent256 runner policy0 with --allow-product-red: X0/Y3/P3/F3. Exact frozen39P pin unchanged; actual reset F matched the existing reviewed pin. Owner: hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration; hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration; hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration

## 39-hot-update-legacy-n1.X

**Current final S5 serial acceptance**: native rc0/GREEN, 63 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/runner/39-hot-update-legacy-n1.X.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN acceptance expectation, **REAL RUN 2026-10-02: GREEN** (pass=20, 64 s, three sweeps). A live reliable task makes a legacy target inadmissible:
`agent_rejected:image_abi_unsupported`, rc64, unchanged destination/exe SHA, no marker, original PID/start,
continued producer output and original ctl rc23. This is a safe refusal, not a downgrade continuity claim.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 63 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-256/runner/39-hot-update-legacy-n1.X.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 39-hot-update-legacy-n1.Y

**Current final S5 serial acceptance**: native rc3/PRODUCT-RED, 114 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/runner/39-hot-update-legacy-n1.Y.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

PRODUCT-RED expected; **signature is a prediction to be re-pinned after the first real run**:
`HOT_UPDATE_LEGACY_EXPORTER output=epipe management=exited_minus_one ctl=idle_timeout_rc70 pid=<pid> nonce=<nonce>`.
The running exporter is v0.6.0; the dev ctl probes then falls back to legacy exec. Its legacy
`NextMsgWithContext` deadline returns the unwrapped `exec: timed out after 1m0s waiting for chunk`
error (`cmd/tether/exec.go`), so `exitcode.go` defaults to 70, unlike reliable-path owner-loss rc74.
The judge requires both original EPIPEs, independent continued source, EXITED(-1), one legacy execution,
a reached reliable probe, the exact timeout diagnostic and rc70. Complete continuity and S1 owner-loss
receipts are negative controls. A different first-run shape must be investigated and re-pinned honestly.

**REAL RUN 2026-10-02 (`./run-drills.sh -j2 --no-attribute --allow-product-red 39-hot-update-legacy-n1`,
three sweeps on images `c346a962…` and `4fe3646b…`): PRODUCT-RED rc=3, pass=20, the predicted signature
matched verbatim every time** (`/^HOT_UPDATE_LEGACY_EXPORTER output=epipe management=exited_minus_one
ctl=idle_timeout_rc70 pid=[0-9]+ nonce=[0-9a-f]{32}$/`), 117 s. No re-pin was needed: the prediction is now a
measurement. The waiver flag is the owner's (S8); the arm stays PRODUCT-RED until S8 migrates a legacy exporter.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc3/PRODUCT-RED, 116 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-256/runner/39-hot-update-legacy-n1.Y.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 36-hot-update-pty

**Current final parent receipt:** A/W/C/L GREEN0; actual generation1→2 preserved. Parent native 1, wall 1921.302 s; exact command `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/regression-sweep/receipt.json`. One deadline-fixed product image/tree; all product and drill inputs unchanged through serial acceptance.


Origin: hot-update S4 plan §9.2 / §9.3; internal review rv-drill DRILL-1..8 / V1.
Hermetic predicates and real-binary tests do not supply deployment receipts.

S4 plan §9.3 acceptance expectation: Parent GREEN, nc_gap=0, bands=-, owner=-,
derived from A/W/C/L GREEN. Freeze windows are calibration; the five-second
new-request and normal business-stall limits remain hard checks.

**First complete real run 2026-10-03 (run 5, image 78f2c62d, -j10 with 34/35/33/39/94):
A/W/C/L GREEN, parent GREEN (123 passes), every unit matched; costs recorded in
drill-costs.tsv.** Runs 1–4 never completed 36 and each exposed real defects, all
fixed before run 5 (evidence `~/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run{1..5}`):
drill-side — state established inside `assert_*` capture subshells (`ART_SHA` unset;
whole-suite guard `tests/assert-shell-state.py`, drill 94 fixed too), the probe's
`census` crashing on pid 1 of a real `/proc`, `node ls` hiding offline nodes, a
rollback-phase predicate, C1/C2 nonce join, fixture ordering for old brokers, W's
unACKed-output precondition; PRODUCT — spawnsafe counted in-flight spawns as wedged
(a busy node could never upgrade), the retained PTY terminal was lost after an image
switch (ctl `workload: unknown process`, exit 74), the ctl printed NATS transport
errors into the raw terminal during a carrier outage, and legacy-wire refusals of an
upgrading agent used a code N-1 clients classify as 70 (now `try_again` +
`agent_upgrading` detail ⇒ 75). See docs/reviews/hot-update-pty-terminal-review.md §4.


**Final S5 parent receipt:** All A/W/C/L nativeGREEN0 in257 on the final image. A actual generation1→2, exact duplex bytes, resource ownership, control semantics and watchdog assertions retained.

## 36-hot-update-pty.A

**Current final S5 serial acceptance**: native rc0/GREEN, 107 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/36-hot-update-pty.A.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **real run 5 GREEN (104 s)**. Original raw duplex
PTY, foreground and ctl must survive a distinct-image upgrade with exact bytes,
zero HUP/early EOF, resize across the cut, rc23 and settled resources. Strict
mixed requests must meet the five-second budget, after healthy strict calibration
without synthetic fault markers. Full freeze maxima above 500 ms remain
calibration under lead verdict L4; unexplained five-second stalls fail.
Estimate 120 s; declared worst 300 s.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 104 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/36-hot-update-pty.A.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 36-hot-update-pty.W

**Current final S5 serial acceptance**: native rc0/GREEN, 218 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/36-hot-update-pty.W.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **real run 5 GREEN (216 s)**. Accepted pending input
and unACKed output must advance in the disconnected candidate; the real 120 s
register watchdog must restore the previous image from current state, preserving
the original handle/master and absolute KA deadline. Actual resizes bracket
the blackhole and recovery; activation alone cannot prove queued-byte progress.
Estimate 240 s; declared worst 420 s.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 216 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/36-hot-update-pty.W.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 36-hot-update-pty.C

**Current final S5 serial acceptance**: native rc0/GREEN, 115 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/36-hot-update-pty.C.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **real run 5 GREEN (112 s)**. Both C1's 25 s carrier
outage with observed session restoration and C2's draining leader, tail beyond
200 ms, FIN, canonical/raw Ctrl-D/Ctrl-C and real terminal outcome must pass.
C1's observed carrier outage must fit the original bounded recovery window
with at most five seconds of unexplained silence at either edge. C2 permits
only the original leader's identity-bound ReapHeld zombie during the measured
live slave tail; unrelated, late, and settled zombies fail.
Estimate 150 s; declared worst 360 s.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 115 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/36-hot-update-pty.C.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 36-hot-update-pty.L

**Current final S5 serial acceptance**: native rc0/GREEN, 201 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-274/runner/36-hot-update-pty.L.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **real run 5 GREEN (193 s)**. Legacy admission before
attach and while running must defer upgrade without disk mutation; original
resize/dual-publish bytes and old-wire fallback quadrants remain usable. Pending
admission reaches a registered candidate whose commit persistence is faulted
and must return legacy75 — the refusal leads with `try_again` (a code the real
v0.6.0 ctl already maps to 75; run 4 showed a bare `agent_upgrading` exits 70 on
it) and carries the product reason `agent_upgrading` in its detail, zero
Start/effect, and the same candidate still pending. Transport is healed before
the admission request; no responders cannot prove the pending gate.
Estimate 120 s; declared worst 300 s.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc0/GREEN, 202 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-257/runner/36-hot-update-pty.L.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 39-hot-update-legacy-n1.P

**Current final S5 serial acceptance**: native rc3/PRODUCT-RED, 349 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/runner/39-hot-update-legacy-n1.P.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

PRODUCT-RED, nc_gap=0, bands=-; **pinned; real run 5 PRODUCT-RED match (348 s)**.
Owner: hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration.
The real previous exporter and old ctl use the same raw-byte continuity oracle
and upgrade fault as 36.A. Missing legacy wait/management ownership is preserved as
absence after proving
old supply, original start, committed same-PID image replacement, readable fresh
four-stream logs, and original ctl waitpid. No terminal audit is invented. If the
original ctl misses its deadline, the collector labels the measured timeout and
retains the actual waitpid signal status; it does not report an autonomous exit.
The first unverified real failure is native ASSERT-FAIL, never PRODUCT-RED;
the lead must retain and review the complete evidence, then pin its one exact
observed signature. No rc70 or predicted failure shape is registered as a fact.
A reviewed failure returns continuity_oracle_rc=1 and native PRODUCT-RED rc=3;
invalid evidence, another shape or unexpected continuity are ASSERT-FAIL.
Estimate 150 s; declared worst 300 s.

**Pinned 2026-10-03 (lead review of the first real run).** Drill runs 1–3 never
reached the classifier (drill-side defects, fixed); run 4 (image ab9568ac) did, as
native ASSERT-FAIL with the UNPINNED notice, and retained its evidence under
`~/tether-hot-update-handoff-20260920T2356Z/s4-evidence/drill-run4/` (39.P log,
`evidence/hot-update-pty.P.*`). Observed signature, reproduced by a deterministic
replay of the archived evidence:
`HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=0 management_terminal=0 ctl=-9 pid=1397 nonce=c53008f2… reason=bytes_input,bytes_output,postprogress_input_received,postprogress_output_sent,postprogress_output_received,identity_window_leader,early_eof_foreground,identity_window_foreground,courier_terminal,resize_nonempty,resize_cut,ctl_reaped,ctl_exit,zombies,resources_stable,foreground_retained`.
Reviewed meaning: the v0.6.0 exporter bare-execs and COMMITS, the original task is
reconciled to `EXITED(-1)` with no terminal audit, the foreground reads early EOF,
and the original ctl never ends on its own — the fixture kills it after its measured
300.006 s timeout (`ctl=-9`). That is the H1 owner's defect, nothing else. The pin
(`PTY_LEGACY_SIGNATURE` in the drill) fixes interrupted output, no management
terminal, `ctl=-9`, and the presence of `bytes_output` then `courier_terminal` in the
reason list; pid/nonce are per run and the HUP-vs-EOF order and zombie timing were
seen to vary, so they are deliberately outside the pin. Negative checks: an
autonomous ctl exit, exact output, or a delivered courier terminal all fail the pin.

### S4 parent 39 declaration update — NOT_RUN

The historical parent 39 and X/Y real-run receipts above are retained. With P
added, the declared parent remains PRODUCT-RED, nc_gap=0, bands=-, derived from
X GREEN and Y/P PRODUCT-RED. Its owner preserves the complete H0 owner and appends
H1 with `; `: hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration; hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration.
This is a declared S4 update, not a new parent or P deployment receipt.


**Final S5 serial measured receipt (supersedes older NOT_RUN/status statements above):**

Measured 2026-10-11: native rc3/PRODUCT-RED, 349 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-256/runner/39-hot-update-legacy-n1.P.log`.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows

**Current final parent receipt:** All A/C/R/B/W/L GREEN0; C1 saturation, finite FIN/recovery and normal phase in one N3-live run. Parent native 0, wall 1286.019 s; exact command `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json`. One deadline-fixed product image/tree; all product and drill inputs unchanged through serial acceptance.


GREEN, nc_gap=0, bands=-, owner=-; **final serial parent GREEN**.

All six A/C/R/B/W/L arms completed on one image/tree in run254; parent runner native0,1307.368 s. The adopted C1 saturation/recovery and normal phases both passed. Frozen manifest headers, product budgets and deadlines are unchanged.

Image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`; product build patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. The later35 fixture-only stale-ONLINE correction preserves all427 production inputs and every unrelated drill input (run263/UNAFFECTED-INPUT-PROOF.json). Parent command/receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/75-parent/receipt.json`.

## 75-hot-update-flows.A

**Current final S5 serial acceptance**: native rc0/GREEN, 166 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.A.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 0.153/1.603/3.013 s; original identities and target Accept=1 retained.

435 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

The same original SSH/TCP/OPEN/half-close/SS cohort spans a real same-PID cut, silent-victim revocation and forced carrier replacement. No application reconnect is permitted.

Measured 2026-10-11: native rc0/GREEN, 157 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.A.log`.

Normal per-capture maximum stalls p50/p95/max: 0.165/1.375/3.016 s. Original identities and target Accept=1 passed.

451 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows.C

**Current final S5 serial acceptance**: native rc0/GREEN, 385 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.C.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 0.964/1.563/3.023 s; original identities and target Accept=1 retained.

403 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

Lead C1: two phases in one arm, image and N3-live cluster. Saturation proves 32 active originals/full 256 MiB, one typed native flow refusal and two typed native exec refusals, 32 natural FINs / 64 complete direction ledgers, zero reservation and original-socket/new-exec recovery. Normal traffic reserves at most 224 MiB, preserves original identity/Accept=1 and exact post-cut samples, and passes the unchanged five-second and 10/s zero-failure checks.

Measured 2026-10-11: native rc0/GREEN, 398 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.C.log`.

Normal per-capture maximum stalls p50/p95/max: 1.006/1.384/3.024 s. Original identities and target Accept=1 passed.

437 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows.R

**Current final S5 serial acceptance**: native rc0/GREEN, 163 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.R.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 1.096/2.683/3.039 s; original identities and target Accept=1 retained.

513 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

The actual PREPARE/final-cut/RESUME revocation windows close the original victims and release their FD/replay ownership while bob continues on his original socket.

Measured 2026-10-11: native rc0/GREEN, 177 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.R.log`.

Normal per-capture maximum stalls p50/p95/max: 1.052/3.305/3.305 s. Original identities and target Accept=1 passed.

584 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows.B

**Current final S5 serial acceptance**: native rc0/GREEN, 157 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.B.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 1.031/1.604/3.008 s; original identities and target Accept=1 retained.

447 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

Two communicating flow/SS-capable artifacts perform A→B→A; offsets, replay Base, nonce and retirement state advance before the return cut. Both real upgrades commit.

Measured 2026-10-11: native rc0/GREEN, 158 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.B.log`.

Normal per-capture maximum stalls p50/p95/max: 0.972/1.109/3.014 s. Original identities and target Accept=1 passed.

425 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows.W

**Current final S5 serial acceptance**: native rc0/GREEN, 271 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.W.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 1.038/1.671/3.007 s; original identities and target Accept=1 retained.

1584 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

The first real register commit fails after persisted boot count. The unchanged 120-second watchdog takes the second current-state cut automatically; original cohort continuity and rollback identity pass.

Measured 2026-10-11: native rc0/GREEN, 277 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.W.log`.

Normal per-capture maximum stalls p50/p95/max: 0.260/1.052/3.020 s. Original identities and target Accept=1 passed.

1617 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 75-hot-update-flows.L

**Current final S5 serial acceptance**: native rc0/GREEN, 136 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/runner/75-hot-update-flows.L.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 0.114/2.854/3.033 s; original identities and target Accept=1 retained.

307 requests at 10.0/s, terminal failures=0.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

GREEN, nc_gap=0, bands=-, owner=-; **final serial acceptance GREEN**.

The fixed v0.6.0/a3431a1 broker runs before the first business operation. Empty ALPN/raw first bytes and actual running SHA prove supply; native75/legacy_session_busy safely refuses upgrade while original traffic remains usable.

Measured 2026-10-11: native rc0/GREEN, 133 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-254/runner/75-hot-update-flows.L.log`.

Normal per-capture maximum stalls p50/p95/max: 0.111/2.395/3.005 s. Original identities and target Accept=1 passed.

297 requests at 10.0/s; terminal failures=0.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

## 39-hot-update-legacy-n1.F

**Current final S5 serial acceptance**: native rc3/PRODUCT-RED, 141 s. Image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; build source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`; per-command source `09b96f95bfc66519320ab4f46e54eb8cd85e1b2429e7d510f9ef0804f206b336`. Exact command, measured oracles, host load and receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/runner/39-hot-update-legacy-n1.F.log` and `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.

Normal per-capture max-stall p50/p95/max 5.767/50.800/55.489 s; original identities and target Accept=1 retained.

603 requests at 10.0/s, terminal failures=9.

**Historical measurements and invariant rationale (previous image; current receipt above is authoritative):**

PRODUCT-RED, nc_gap=0, bands=-; **final native PRODUCT-RED**.

Owner: hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration.

The real fixed v0.6.0 exporter exchanges nonempty original traffic before bare exec. The unpinned run255 intentionally returned SETUP-RED2 with actual reset,67 pre-fault samples, original_socket=true and zero redials. The exact existing reviewed reset pin is confirmed by PIN-REVIEW.json; pinned F then returns native3. The common continuity oracle and fault-validity guards remain unchanged.

Measured 2026-10-11: native rc3/PRODUCT-RED, 147 s; one final serial image `sha256:049b3c3bc69a1f3b9e88b1dbc3998cf5b809c076f6eb9b0e9edfe0fe39b55086`, source patch `647e54d6f8457e651249f37f773842fa1f40ab39b2cdc87238f5d8af8305f63b`. Receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-256/runner/39-hot-update-legacy-n1.F.log`.

Normal per-capture maximum stalls p50/p95/max: 5.831/50.881/56.768 s. Original identities and target Accept=1 passed.

613 requests at 10.0/s; terminal failures=10.

Full byte, nonce, Accept, calibration and host-load oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`.


### S5 final serial regression receipt — 2026-10-11

Parent39 owner: hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration; hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration; hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration.

Parent75 is GREEN. Parent39 joins X GREEN with Y/P/F native PRODUCT-RED3; --allow-product-red gives runner native0. The frozen S4 P pin is unchanged. All36/34/35/94 and full70/72 pass. Native33 remains INCOMPLETE4 at its existing #73 NC_GAP, as required by frozen S5 §9.5; the original regression runner native2 (35.A assertion plus native33 INCOMPLETE) is retained, and corrected35 rerun native0 is joined explicitly; native33 stays4. Independent early-close and actual A+1 ACK negatives each return native ASSERT-FAIL1; restored original image A returns GREEN0. Complete receipts, defect classifications and measured oracles: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/RECEIPTS.md`.


### Deadline-fixed final S5 receipt

All current acceptance arms use one final image `sha256:9d8a232c3c02d0e1de59a6d7ad0354647dd19bbb39aec574d915e8f29682708c`; previous049 serial successes remain historical because post-drill broker verification found a product deadline provenance defect. The root was regression/mutation verified, then a new image was rebuilt before the full serial acceptance. Parent75 native0; parent39 allow-product-red policy0; all36/34/35/94 and full70/72 native0. Regression274 policy1 preserves native33 INCOMPLETE4 at the frozen §9.5 #73 NC_GAP; no status is relabeled. Both native deploy negatives assert1/control0; restored original A0. Detailed receipt: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/RECEIPTS.md`.
## 37-hot-update-broker

S6 declaration / NOT_RUN. Expected native GREEN (rc 0), nc_gap=0. All declared A/A1/B/T/R children must join without translating their native verdicts. Blocked claims remain in hot-update-broker-ledger.tsv until native evidence is archived.

## 37-hot-update-broker.A

S6 declaration / NOT_RUN. Expected native GREEN (rc 0), nc_gap=0. The frozen fixture, original cohort and same-candidate receipt are pending P29/P30/P35. Blocked claims remain in hot-update-broker-ledger.tsv until native evidence is archived.

## 37-hot-update-broker.A1

S6 declaration / NOT_RUN. Expected native GREEN (rc 0), nc_gap=0. The frozen fixture, original cohort and same-candidate receipt are pending P29/P30/P35. Blocked claims remain in hot-update-broker-ledger.tsv until native evidence is archived.

## 37-hot-update-broker.B

S6 declaration / NOT_RUN. Expected native GREEN (rc 0), nc_gap=0. The frozen fixture, original cohort and same-candidate receipt are pending P29/P30/P35. Blocked claims remain in hot-update-broker-ledger.tsv until native evidence is archived.

## 37-hot-update-broker.T

NOT_RUN pending S7 P29/P32/P33/P37. Target native GREEN0 requires two independent
named lanes: N3 and default single. Each has four original capable transfers,
both legs, real agent/broker A→B→A cuts before completion, original origin/ID/NUID/
PID/OFD, postcut durable progress, default reaper and exact history. A capable
transfer-busy refusal or terminal-only cut is INCOMPLETE, never this receipt.
Separate retained legacy and unknown-census windows require actual reload native75,
exact reason, unchanged running identity and zero cut effects. Business75 never
becomes a drill4 by numeric mapping. Declared worst1200s, runner ceiling2400s;
actual wall NOT_MEASURED. The old S6 defer key may remain NOT_RUN in the lead-owned
ledger until migration; an old PASS cannot sign this S7 capable-cut claim.

## 37-hot-update-broker.R

S6 declaration / NOT_RUN. Expected native GREEN (rc 0), nc_gap=0. The frozen fixture, original cohort and same-candidate receipt are pending P29/P30/P35. Blocked claims remain in hot-update-broker-ledger.tsv until native evidence is archived.

## 39-hot-update-legacy-n1.BR

S6 declaration / NOT_RUN. Expected native PRODUCT-RED (rc 3), nc_gap=0. Owner: hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration. The genuine previous artifact must exercise N3 follower/home Run unwind; its exact historical signature is pending independent P35 pinning. An N1 leader refusal, loader failure or preflight refusal cannot satisfy this claim.

## S7 transfer-continuation targets (NOT_RUN)

These are frozen acceptance expectations, not observations. P37 records native
child lines, candidate/source/artifact/running SHA, dirty digest, command, wall,
reader digest and archived oracle inputs before making a claim. The runner's
existing ARMS/missing-child join owns the parents. A missing child, early finish,
empty data window or a prepared record offered as sender coverage cannot sign
GREEN. A fixture/custody failure is SETUP-RED. Business75 is separate from drill4.

63 contains positive arms only. 64 contains independently built production
controls only: each expected native ASSERT-FAIL remains rc1 in the log and parent
join. Compile/setup/incomplete red, a patch that never ran, or a lost nested
GOFLAGS overlay cannot produce a control-receipt.json with killed=true. No waivers
or signature bands are attached to controls. Genuine old N3 Run-unwind stays in
39.BR with its existing PRODUCT-RED owner and reviewed signature.

## 63-hot-update-transfer

NOT_RUN pending P29/P32/P33/P34/P35/P37. Target native GREEN0 is the AND of all eleven required child receipts on one candidate. Controls never enter this parent.

## 63-hot-update-transfer.A

NOT_RUN. Target native GREEN0. Single, four cohorts, both legs: agent forward, broker forward, current-state rollback. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.A1

NOT_RUN. Target native GREEN0. Sole voter, four cohorts, both legs with actual same-PID cuts and default reaper. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.B

NOT_RUN. Target native GREEN0. N3, each leg covers source follower, transferred original leader and third voter; broker-only roll. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.C1

NOT_RUN. Target native GREEN0. Actual S5 TLS/yamux/WSS carrier replacement, four cohorts both legs. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.C2

NOT_RUN. Target native GREEN0. Actual transfer NATS data attachment reconstruction, four cohorts both legs. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.C3

NOT_RUN. Target native GREEN0. Same NATS binary and store_dir server restart, four cohorts both legs. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.R1

NOT_RUN. Target native GREEN0. Pristine import failures, agent/broker-store/broker-control, each tier and direction. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.R2

NOT_RUN. Target native GREEN0. ACTIVE failures after new progress, current-owner rollback, each tier and direction. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.X

NOT_RUN. Target native GREEN0. Independent Ctrl-C and session-rm victims, four successful survivors keep their own buckets. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.W1

NOT_RUN. Target native GREEN0. One actual120s data+REGISTER outage per call, leg1 pull sender, original deadline. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 63-hot-update-transfer.W2

NOT_RUN. Target native GREEN0. One actual120s data+REGISTER outage per call, leg2 push receiver, original deadline. Every cut is nonempty before, progresses after, and retains original ID/version/PID/OFD and exact history.

## 64-hot-update-transfer-controls

NOT_RUN. Target native ASSERT-FAIL1 is the original join of all23 independent control children. A killed mutation is an expected/actual receipt relation; it does not turn native1 into0.

## 64-hot-update-transfer-controls.M01

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: SDK Put or new NUID after sent-unknown. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M02

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: remove chunk CAS receipt guard. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M03

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: publish metadata before final receipt. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M04

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: skip reader rebuild after healthy drop. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M05

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: forget dirty barrier on last close. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M06

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: watchdog fails an outstanding Grant. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M07

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: create with expired or foreign-home permit. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M08A

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: drop synced checkpoint. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M08B

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: restore wrong offset or hash ahead. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M08C

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: ConfirmSynced before successful Sync. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M09

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: reopen original OFD by path. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M10A

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: repeat successful commit primitive. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M10B

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: write partial destination in place then restore. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M11A

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: skip tracker import. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M11B

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: late source seal overwrites terminal. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M11C

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: reset original deadline on import. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M12A

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: any terminal is an exact ACK. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M12B

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: remove history subject CAS. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M12C

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: replace failed outcome with complete. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M13A

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: bypass live owner GC protection. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M13B

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: discard cleanup responsibility. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M13C

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: stop reaper to fake live protection. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 64-hot-update-transfer-controls.M14

NOT_RUN pending owner production patch and P29 runtime/oracle witnesses. Target native ASSERT-FAIL1: full ObjectStoreConfig loses Duplicates or TTL. Source patch/hash, nested build selector, artifact and actual running SHA, exercised predicate and original judge inputs are required.

## 38-hot-update-rollback-compat

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.A-single

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.A-sole

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.A-n3

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.R0

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.R1

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.R2

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-root

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-user

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-no-systemd

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-colocated

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-sibling

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.H-readonly

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.G-two

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.G-third

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.G-concurrent

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-driver

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-reopen

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-gc

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-late-writer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-history-rollback

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.C-owner-dead

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W00-F0-agent-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W01-F1-single-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W02-F2-sole-voter-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W03-F3-N3-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W04-F4-agent-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W05-F5-single-L-known-inside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W06-F6-sole-voter-L-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W07-F7-N3-L-known-inside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W08-F0-agent-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F1-single-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W10-F2-sole-voter-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W11-F8-N3-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W12-F9-N3-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W05-F5-single-L-known-outside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W07-F7-N3-L-known-outside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F5-agent-D-known-inside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F5-agent-D-known-outside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F7-N3-D-known-inside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F7-N3-D-known-outside

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W08-F0-single-D-unknown-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F1-agent-D-unknown-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W12-F9-N3-D-unknown-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W08-F3-single-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F4-sole-voter-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.W09-F6-agent-D-known-none

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.I-late-revoke-current-rollback

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.I-nats-cut-leader-move

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.I-begin-archive-gc

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.I-driver-death-host-reservation

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M01

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M02

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M03

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M04

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M05

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M06

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M07

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M08

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M09

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M10

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M11

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M12

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M13

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M14

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M15

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M16

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M17

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M18

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M19

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M20

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 38-hot-update-rollback-compat.M21

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/drill38-mutation-receipts-02dy1kmf/expected-verdicts-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S8 receipt spelling `ASSERT-RED` denotes the same native rc1 as the shell and expectation table spelling `ASSERT-FAIL`; neither is success. Positive and mutation arms keep separate native rows.

## 39-hot-update-legacy-n1.Q00B0-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B0-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B0-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B0-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B0-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B0-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B0-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B0-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B0-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B0-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B0-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B0-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B0-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B0-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B0-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B0-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B0-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B0-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B0-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B0-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B1-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B1-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B1-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B1-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q00B1-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B1-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B1-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B1-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B1-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q01B1-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B1-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B1-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B1-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B1-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q10B1-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B1-exec

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B1-run

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B1-kill

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B1-expose

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.Q11B1-transfer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.LB-new

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.LB-old

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.LI-defer

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.LI-interrupting

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.M-route

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CC-idle

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CC-late-create

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CL-reject

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.ABI-adjacent

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.ABI-too-far

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.ABI-unknown

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.GO-hash

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CFG-nats-old

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CFG-nats-new

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CFG-caddy-old

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CFG-caddy-new

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s8wt/receipts/s8-p28/s8-p28-expected-rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.A1

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.D

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.Q

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.L

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.S

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.J

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 85-hot-update-nats.F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.R

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.I

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.H

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.C

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.K

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 86-hot-update-caddy.U

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.NI

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 39-hot-update-legacy-n1.CI

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s9wt/receipts/s9-p37-receipts.xUDKSg/requested-expected-verdicts.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.

## 99-hot-update-fullstack

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.C1

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.C3

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.S1

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.S3

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.F1

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.F3

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 99-hot-update-fullstack.K

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p19-verify-g2iu2ttv/expected-verdicts.rows`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 calibration C1/C3, full rollout A/B, prefix S1/S3/F1/F3 and soak K retain separate cells and original cohorts. Quiet/default/live-grow regimes need the same candidate; no retry or attribution may certify these declarations.

## 100-hot-update-fullstack-controls

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M01A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M01B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M02E

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M02P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M02F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M02X

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M03E

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M03P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M03F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M03X

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M04A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M04B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M04P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M05E

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M05P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M05X

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M05R

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M06A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M06B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M06R

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M06F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M07A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M07B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M07N

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M07C

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08E

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08D

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08X

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08C

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M08O

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M09A

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M09B

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M10S

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M10C

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11W

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11I

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11O

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11P

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11Q

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11F

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11S

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11X

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11D

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11C

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11L

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11U

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 100-hot-update-fullstack-controls.M11K

Declared expectation / **NOT_RUN**. Source: `/home/weiland/s10wt/receipts/s10-p20/expected-verdicts.rows.tsv`. No live native result, source/artifact/running identity or measured cost is claimed.
S10 P20 requires an independent baseline0 / mutant1 / restore0 archive for every applicable topology cell, exact nonempty hit window and original supervised waiter Join. This control is excluded from the positive 99 parent.

## 31-node-upgrade-fleet.V1

Declared expectation / **NOT_RUN**. Source: `S8 P29: exact historical parent copied to V1`. No live native result, source/artifact/running identity or measured cost is claimed.

## 31-node-upgrade-fleet.V2

Declared expectation / **NOT_RUN**. Source: `S8 P29: declared v2 regression expectation`. No live native result, source/artifact/running identity or measured cost is claimed.

## 33-node-upgrade-success.V1

Declared expectation / **NOT_RUN**. Source: `S8 P29: exact historical parent copied to V1`. No live native result, source/artifact/running identity or measured cost is claimed.

## 33-node-upgrade-success.V2

Declared expectation / **NOT_RUN**. Source: `S8 P29: declared v2 regression expectation`. No live native result, source/artifact/running identity or measured cost is claimed.

### S10 P31 declaration reconciliation

The 99/100 expectations above remain declarations / NOT_RUN pending
S10/P19/P20/P26/P27/P32/P35 and upstream manifest owners. No expectation or
historical 39.P/F/BR PRODUCT-RED3 was changed. The P31 crosscheck joins the actual
shell and JSON child sets to expected native values, histories, launch costs,
claim identities and the full-directory declaration catalog. Its mutation
controls are hermetic gate evidence only. Live source/artifact/running identities,
native chains and measured resources remain absent until their exact owner
supplies original same-candidate raw evidence. Cost input changes require a new
candidate seal; they are never receipt-only updates.
