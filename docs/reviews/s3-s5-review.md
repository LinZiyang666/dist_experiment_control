# S3+S4+S5 (simcluster G-A) — Stage-C Internal Review

## Executive summary
- **22 findings, all verifier-CONFIRMED** (0 uncertain, 0 refuted). Severity: **0 blocker · 11 major · 10 minor · 1 nit**.
- **No BLOCKER** — the external-review gate is **not** hard-blocked, but the 11 majors include multiple plan-mandated **false-green / vacuous-oracle** defects that undercut the deliverables' honesty and should be fixed first.
- **Three lanes independently flagged the same #20 false-green** in drill 73's QUORUM freeze arm (mandate-1 = pin-2 = dp-1) — highest-confidence, single-fix defect.
- **Three findings converge on drill 30's write-continuity vacuity** (mandate-2 = pin-1, plus cluster-2) — the M1 GREEN is satisfiable by a probe that observed nothing.
- Recommendation: land the 11 major fixes (or convert the un-closable ones to honest NOT-COVERED) before submitting for external review; minors/nit can ride the same pass since they are cheap doc/one-line-assert edits.

---

## Lane: mandate-fidelity

### mandate-1 — major — `test/simcluster/drills/73-proxy-cluster-ha.sh:122`
**Defect.** The QUORUM freeze GREEN closes on `/sub`→200 (a read gate) + a control-write fence, silently dropping the plan-mandated data-plane separation; a freeze whose SS data plane is dead under quorum loss still passes green (#20 pattern).
**Evidence.** Plan s3-s5-plan.md:210 (F3/MF-3, R-DATAPLANE) verbatim requires "survivor-homed exit SS 腿仍传字节 WHILE 死-homed exit SS 腿黑洞 … 绝不以 /sub 200 单独收口". The QUORUM arm (117-123) asserts only SEMANTIC-1 (`/sub` still 200, line 122) and SEMANTIC-2 (control write fenced, line 123); `ss_down ctl1` at line 108 tears down the only SS leg before quorum loss, so no byte-continuity is asserted. The write-fence proves quorum is lost, not that SS egress still flows. No honest NOT-COVERED for the dropped leg.
**Fix.** Pre-quorum-loss, home ≥1 exit on the survivor leader (plan:206) and start an SS leg through it AND a dead-homed exit; after killing to 1/3, assert the survivor-homed SS leg still returns the RFC1918 sink bytes WHILE the dead-homed leg black-holes. Keep `/sub` 200 + write-fence as corroborating discriminators, not the terminal oracle.

### mandate-2 — major — `test/simcluster/drills/30-rolling-upgrade.sh:157`
**Defect.** M1 ("write-probe saw NO not_leader/503") is asserted as a clean GREEN while the plan-mandated non-vacuity negative control (Arm 30-C, N=2 write-fence) is dropped to NOT-COVERED with no stated reason — a broken/silent probe or a real roll-induced not_leader would false-green.
**Evidence.** Plan:298 "零-not_leader 单看可能因 probe 没在写 → 必须由 30-C(N=2 fence)做负对照; 缺 30-C 则 30-B 空转"; plan:446 repeats it; Arm 30-C at plan:304. Drill has no N=2 arm; the final warn (163) lists "N=2 write-fence" as NOT-COVERED but gives a reason only for the colocated-agent leg. `_clear_and_roll` (63-76: up to 6×8s retries + roll) can outlast the fixed 400-iter probe (line 85), so `_probe_clean` can grep a log with no roll-concurrent writes.
**Fix.** Implement Arm 30-C (drop to N=2, run the writer during an upgrade, assert it DOES observe not_leader under F=0) so M1's zero-not_leader is a proven N≥3 property; OR, if infeasible in-batch, state the reason and demote M1 to a labeled NOT-COVERED. Also gate M1 to run only while the probe demonstrably wrote across the roll window (assert wp.log grew during the roll).

### mandate-3 — major — `test/simcluster/drills/71-expose-rehome-failover.sh:70`
**Defect.** The #29 inverted-assert pin + its large NOT-COVERED assume `home!=tunnel` expose is PERMANENTLY dead, but the source implies a one-shot initial-delivery race against the home's cert registration; the guard cannot distinguish a real tether defect from a post-grow settling artifact, so blanket "drain-rehome recovery NOT-COVERED (unreachable until #29 fixed)" hides coverable ground.
**Evidence.** gotcha #29 (deploy-tier-gotchas.md:115-120) claims `homeForExpose` returns nil for non-self homes AND that `cluster_nodes tunnel_addr/cert_fp` is correctly filled — contradictory. home.go:96-113 delivers `BrokerAddr=home.TunnelAddr` for any `Eligible()` home with `CertFP!=''`; read.go:45 `Eligible()==Phase==VOTER`, and grow_to_3 makes NONTUN a VOTER. The only way the fallback fires is `CertFP==''` at expose time = a settling-window race, not a permanent binding defect. Drill 71:50-52 picks NONTUN as merely the first non-brk1 `node_running` broker, never asserts it is a cert-eligible VOTER in the leader DB, and never captures agt1's journal to confirm OpenHome dialed brk1.
**Fix.** Before the 4× probe, assert NONTUN is Phase=VOTER with non-empty cert_fp AND tunnel_addr in the leader's `cluster status --json`, and capture agt1's journal during a failing attempt to confirm the frpc/OpenHome dialed the fixed tunnel_addr (brk1). Re-probe after a settle delay: if `home!=tunnel` succeeds once NONTUN is cert-ready, reclassify #29 as an initial-delivery race and re-open drain-rehome-onto-settled-voter as COVERABLE.

### mandate-4 — minor — `docs/deploy-tier-gotchas.md:149`
**Defect.** The #31 honesty-confession is factually wrong: it credits tolerance to a non-existent `drill-retry.sh` grow-flake retry and over-attributes the consecutive-grow INCOMPLETE flake to root cause #31.
**Evidence.** `find . -name drill-retry*` returns nothing, yet the file is named in gotchas:149-151, inventory §4:370, README:280, drill 30:136. The real runner run-drills.sh:65 `FLAKE_SIG` excludes grow/VOTER timeouts, and its R2-F1 comment says a VOTER/grow timeout is "DELIBERATELY NOT a flake signature" (surfaced RED for manual re-run). simcluster:223-231 attributes the same symptom to "clustered-JS meta-group formation + raft VOTER promotion … timing-sensitive", not the grow-lock leak; #31's "几乎总残留" would deterministically block the sequential brk3 grow in grow_to_3, which nonetheless succeeds routinely.
**Fix.** Correct the confession in all four locations to describe actual harness behavior (wave-split concurrency reduction; grow/VOTER timeouts surface as RED, NOT in FLAKE_SIG); either substantiate "#31 == grow-flake root cause" with a leaked `cluster_grow_active` demonstrably blocking a sequential grow, or retract it in favor of the JS-formation-timing diagnosis. Remove the `drill-retry.sh` reference. (#31-as-upgrade-blocker remains soundly pinned by drill 30's real-roll HALT.)

---

## Lane: pin-integrity

### pin-1 — major — `test/simcluster/drills/30-rolling-upgrade.sh:157`
**Defect.** The headline write-continuity assertion (`_probe_clean`, G5/M1) has NO non-vacuity guard and false-greens when the write probe silently never runs.
**Evidence.** `_probe_clean()` (92) = `! dexec ctl1 -- grep -qiE '…|WRITEFAIL' /tmp/wp.log`. On a missing file `grep -q` exits 2, on empty exits 1; the leading `!` inverts both to PASS. `_start_write_probe` (85) ends with `& echo ok` fully redirected, so it always returns 0 and never verifies wp.log holds ≥1 successful `session create`. Arm 30-C (the plan's non-vacuity control) is dropped (163). Plan:298/446 state 30-B is "空转" without 30-C.
**Fix.** Before the roll, assert wp.log has ≥1 `session create` line and no WRITEFAIL in a quiescent baseline; make `_probe_clean` FAIL when wp.log is missing/empty. Alternatively restore Arm 30-C as the mandated negative control. *(Same root as mandate-2 — coordinate one fix.)*

### pin-2 — major — `test/simcluster/drills/73-proxy-cluster-ha.sh:122`
**Defect.** The QUORUM freeze arm closes "freeze keeps serving" on `/sub`-200 + a control-write fence, dropping the plan-mandated SS data-plane separation; a broken freeze vending a 200 doc over dead exits false-greens.
**Evidence.** Arm drives ZERO SS bytes (no ss_up/ss_curl/ss_egress); asserts only SEMANTIC-1 (`/sub`→200, 122) + SEMANTIC-2 (`_write_fenced`, 123). Plan:206 required ≥1 exit explicitly homed on the survivor (never done); plan:210/443 require the survivor-carries-bytes WHILE dead-homed-blackholes separation and forbid `/sub`-200-alone.
**Fix.** Home one exit on the surviving leader before the kills; under freeze assert a survivor-homed SS leg still returns the sink sentinel WHILE a dead-homed exit's SS leg blackholes. If SS-under-freeze is infeasible in this topology, add an explicit NOT-COVERED note. *(Duplicate of mandate-1 / dp-1 — single fix.)*

### pin-3 — major — `test/simcluster/drills/74-rebalance-on-return.sh:106`
**Defect.** The auto-rebalance-on-return path — the drill's namesake — is NOT-COVERED with a reason that hides a coverable point: distribution-evening under `TETHER_AUTO_REBALANCE=on` is observable via the exact home-count oracle already used for the manual verb.
**Evidence.** Drill never sets `TETHER_AUTO_REBALANCE=on`; tests only default-off (Arm A) and the manual verb (Arm B). Line 106 justifies omission as "the manual verb covers the rebalance mechanism", but the auto return-edge TRIGGER (dwell + quiet-window + return detect + cooldown) is distinct code from the manual PLANNER, and its effect (distribution evens WITHOUT invoking the verb) is measurable by `_dist`/`_spread` (19-30). Only the `proxy_auto_rebalanced` count==1 anti-flap event is genuinely uncoverable (no sys.events reader). Plan Arm C (235-242) budgeted the full env-on-broker-unit + poll≥180s journey.
**Fix.** Add Arm C: set `TETHER_AUTO_REBALANCE=on` on broker units (live-verify via `systemctl show tether-broker -p Environment`), create skew via kill+return, assert distribution evens (spread≤1 ∧ KTGT reloaded) WITHOUT running `cluster rebalance proxy`, poll ≥180s. Keep only the count==1 event as the NOT-COVERED sub-point.

### pin-4 — minor — `test/simcluster/drills/71-expose-rehome-failover.sh:35`
**Defect.** The #29 flaky pin's `≥1/4` threshold can stay green post-fix if a single spurious transient matching a generic signature occurs during the four rapid expose/rm cycles.
**Evidence.** `_home_nontunnel_unreliable` (31-40) counts a failure on `frpc_failed|token_unknown|agent_rejected` (35) and pins on `_cnt≥1` (39). Flip is sound (fix → all 4 succeed → `_cnt=0` → RED), but `agent_rejected` is a generic wrapper; a post-fix transient (e.g. a port-alloc race across 4 back-to-back cycles) keeps `_cnt≥1`, defeating the flip.
**Fix.** Tighten the counted signature to the specific #29 broker deny `token_unknown_or_revoked` (expose.go:141) and/or require the line to carry the home-binding token, so a fixed system deterministically drives all 4 to success and the pin flips RED.

### pin-5 — minor — `docs/reviews/s3-s5-plan.md:347`
**Defect.** #30 is referenced by number as a code-confirmed gap "pinned by an INVERTED assert_ok that flips on fix", but no such assertion exists and #30 is not registered in the gotchas ledger — a phantom pin.
**Evidence.** `grep '#30\|proxy_keyset_changed' docs/deploy-tier-gotchas.md` returns nothing (only #25-#29, #31). Plan §5 (347-349) describes #30 as a live flipping pin, but drill 73 line 115 is just `warn '73 NOT-COVERED [#30]'` — nothing flips if tether adds the emit. Source confirms the gap is real and un-pinnable (handleProxySubRevokeCluster never `pubSysEvent`; sole writer is single-mode proxy.go:665; no ctl-side reader). Inventory §4:340 honestly records it as NOT-COVERED.
**Fix.** Correct plan §5 to describe #30 as "code-confirmed but un-pinnable (no operator reader); data-effect pinned by 73 REVOKE", and either add a matching NOT-COVERED entry to the gotchas ledger or drop the numbered "#30" so it is not read as a live flipping pin.

### pin-6 — minor — `test/simcluster/drills/74-rebalance-on-return.sh:84`
**Defect.** The SKEW arm never asserts KTGT started with ≥1 `__proxy__` home, so if the pre-kill load-spread left all non-leaders at 0 homes the `_ktgt_empty` oracle is vacuously satisfied and SKEW passes without any rehome.
**Evidence.** KTGT = heaviest non-leader (77-81); `_kmx` only logged (83), never asserted >0. `_skew` (36) polls `_ktgt_empty` (count==0). Header (33-35) acknowledges "killing a 0-home broker would make rehome vacuous" but mitigates only with "pick heaviest", not "assert heaviest>0"; if every non-leader is at 0, `_ktgt_empty` is trivially true before any kill effect.
**Fix.** Before `_skew`, assert `[ "$(_count_on "$KTGT")" -gt 0 ]` (or `_kmx>0`) so a vacuous kill-of-a-0-home-broker fails loud.

---

## Lane: dataplane-proof

### dp-1 — major — `test/simcluster/drills/73-proxy-cluster-ha.sh:117`
**Defect.** The N=3 quorum-loss FREEZE arm drives ZERO data-plane bytes — closes entirely on `/sub`→200 (control read) + a control-write fence, exactly the `/sub`-200-alone close the plan forbade, silently dropping the MANDATORY R-DATAPLANE separated SS-byte assertion.
**Evidence.** SEMANTIC-1 (122) = `/sub` still 200; SEMANTIC-2 (123, `_write_fenced`) = sub-create must not mint a token. Last SS drive is the REHOME arm (107) then `ss_down ctl1` (108); no `ss_up`/`ss_curl_ok` in REVOKE or QUORUM arms. Plan:210 mandates the F3/MF-3 separation, plan:213 the disable-on-quorum-loss control arm, plan:443/37 the R-DATAPLANE iron law ("视图在死数据面上收敛 = #20 假绿"). Neither drill, README:276, inventory landing, nor ledger declares this dimension NOT-COVERED — inventory honesty note ⑤ instead asserts "73 的 GREEN 是真的". Separation is constructible (post-2nd-kill only LDR survives; 2 exits, one LDR-homed=live, one K2-homed=dead).
**Fix.** After Q-kill, `ss_up` an exit homed on the surviving LDR and assert `ss_curl_ok … $SINK_TOK` WHILE an exit homed on the just-killed K2 blackholes; optionally restore the planned disable-on-quorum-loss control arm to prove a 404 is policy, not a dead broker. If infeasible, replace the silent drop with an explicit NOT-COVERED note. *(Same defect as mandate-1 / pin-2 — one fix closes all three.)*

### dp-2 — minor — `test/simcluster/drills/71-expose-rehome-failover.sh:8`
**Defect.** The #29 arm's header CLAIMS a data-plane defect ("data plane DEAD … ALLOCATED + the (dead) home … #20 class") but the assertion never drives/observes any bytes and never checks the ALLOCATED-while-dead divergence; it pins solely on a control-plane CLI signature, and the divergence claim is contradicted by the code's own rollback.
**Evidence.** The only assertion (`_home_nontunnel_unreliable`, 31-40) greps CLI output for `frpc_failed|token_unknown|agent_rejected` then `expose rm` each attempt — no `dp_curl` byte oracle. Lines 28-29 describe live behavior as `agent_rejected:frpc_failed + rollback`, i.e. no persistent ALLOCATED row and no curlable dead port — contradicting the header. Gotcha #29 ledger independently confirms rollback (exit 70). Not a false-green in the flip sense (correctly RED-flips when all 4 succeed); the defect is the overclaiming header.
**Fix.** Align the header to the rollback truth: drop the "ALLOCATED + (dead) home / #20 class" language, describe #29 as "home!=tunnel expose ROLLS BACK unreliably (frpc_failed)". If a live spike ever shows the row DOES persist ALLOCATED with a dead public port, honor the header with a real `ps -a` ALLOCATED + `dp_curl_refused` proof.

### dp-3 — minor — `test/simcluster/drills/72-proxy-subscription.sh:10`
**Defect.** Every drill/lib comment claims `/sub` "stays loopback-only" (127.0.0.1:8090), but no drill asserts the loopback-only NEGATIVE — a cross-container `curl http://brk1:8090/sub/<tok>` expecting connection-refused is never issued, so a regression rebinding `/sub` to 0.0.0.0 (leaking SS PSKs over cleartext to any bridge peer) would pass undetected.
**Evidence.** Loopback-only is asserted only implicitly (via the ingress front) and stated in comments (72:10/51/127, 73:91, ingress.sh:9/13). Every 8090 access is loopback-internal (`exec brk1 -- curl 127.0.0.1:8090`) or via ingress TLS (`https://brk1/sub/…`). A 0.0.0.0 bind is a superset, so both paths keep working and the drills are structurally blind to the regression. The `/sub` body carries SS `password:` PSK literals. Neither pinned nor declared NOT-COVERED.
**Fix.** Add a one-line negative to 72 (and/or 73): `dp_curl_refused ctl1 "http://brk1:8090/sub/$TOKa"` (curl exit 7) — the same cross-container idiom drill 70:85/97 already uses — so the loopback-only claim becomes a pinned GREEN that RED-flips on a non-loopback rebind.

---

## Lane: cluster-correctness

### cluster-1 — major — `test/simcluster/drills/30-rolling-upgrade.sh:124`
**Defect.** The headline G5 deliverable — the roll ORDER (followers-first/leader-last) — is claimed as covered but no oracle ever verifies order.
**Evidence.** The only ordering oracle is `_dry_run` (77): `_BUP … --dry-run | grep -qiE 'dry-run|would|follower|leader|roll|TRANSFER|UPGRADE'` — a loose OR where the single token `UPGRADE` (printed by any upgrade dry-run) satisfies it; it never checks followers precede the leader. The real-roll arm (154-157) asserts all-on-next + PID-same + probe-clean, none observing order. An unpinned claim masquerading as GREEN.
**Fix.** Capture the dry-run output, extract the ordered broker sequence, and assert `index(LDR)==last`, e.g. `seq=$(… | grep -oE 'brk[123]'); [ "$(printf '%s\n' $seq | tail -1)" = "$LDR" ]`. If dry-run emits no parseable order, downgrade the label to an explicit NOT-COVERED with that reason.

### cluster-2 — minor — `test/simcluster/drills/30-rolling-upgrade.sh:85`
**Defect.** The raft write-probe has a fixed 400-iteration budget decoupled from the roll duration, so on the #31-retry path (fires ~every run) it can self-terminate before the real roll's leader re-exec, making `_probe_clean` a partial/false green.
**Evidence.** `_start_write_probe` (85) loops `while [ $i -lt 400 ]` with `session create` + `sleep 0.3`. #31 HALTs the first `_do_roll` ~every run (136); the real roll runs later inside `_clear_and_roll` retries (63-76, up to 6×8s + full roll). During the HALT window the cluster is healthy so session-creates succeed and keep wp.log clean; if retries+roll outlast the 400-iter budget the probe exits before leader re-exec — a GREEN that observed nothing. `_probe_clean` never asserts the probe PID was still alive.
**Fix.** Make the probe unbounded (`while :` stopped only by `_stop_write_probe`) so its lifetime brackets the actual roll; or before `_probe_clean`, assert the probe PID was still alive at stop time. *(Same drill-30 write-probe area as mandate-2 / pin-1.)*

### cluster-3 — minor — `test/simcluster/drills/74-rebalance-on-return.sh:102`
**Defect.** B-real (spread≤1) paired with B-real-effect (KTGT≥1 home) can both go GREEN while one agent is silently unhomed (home="none"), because no oracle asserts total-homed == number of agents.
**Evidence.** `_spread` (23-27) and `_count_on` (20) count only `.home_broker` values equal to a broker id; an unhomed agent contributes to none. With 3 agents, one stuck-unhomed leaves 2 homes; brk1=1, brk2=0, KTGT=1 gives `_spread=1` (102 passes) and `_count_on KTGT=1` (103 passes) — both green while an agent is unhomed. No post-rebalance re-assert of `_proxy_ready 3` or total-homed==3.
**Fix.** Pair the spread assertion with a total-homed check, e.g. `[ "$(_homes | grep -c .)" -eq 3 ]`, or re-run `poll_until … _proxy_ready 3` after B-real.

### cluster-4 — nit — `test/simcluster/drills/73-proxy-cluster-ha.sh:103`
**Defect.** The REHOME kill target NT_HB is asserted-by-label to be a "non-leader" but nothing enforces `NT_HB != leader`; it is only guaranteed non-leader when brk1 happens to be the leader.
**Evidence.** `_pick_nontunnel` (49-52) selects an exit homed `!= brk1` (tunnel broker) only — it does not exclude the current raft leader. If leadership has moved off brk1 and an exit homes there, `NT_HB==leader` and REHOME kills the leader, forcing a re-election the label denies (quorum stays 2/3 so the drill still passes; at most an election delay against the 45s poll at 104). No false green — the QUORUM arm re-reads `sim_leader` at 118.
**Fix.** In `_pick_nontunnel` also skip homes equal to `$(sim_leader)`, or add `[ "$NT_HB" != "$(sim_leader)" ]` before the REHOME kill, so the "non-leader" claim is enforced.

---

## Lane: harness-safety

### harness-safety-1 — major — `test/simcluster/drills/72-proxy-subscription.sh:91`
**Defect.** The recovered subscription token `TOKa2` is assigned inside `_rev_recover`, which runs in an `assert_ok` command-substitution subshell, so it never reaches the drill shell; `_off_semantics` falls back to the REVOKED `$TOKa` and the final OFF2 assertion passes vacuously via a 404 — the exact case its description forbids.
**Evidence.** L91 `_rev_recover(){ TOKa2=$(_sub_token alice2); … }` is invoked at L150 `assert_ok … _rev_recover`; assert_ok routes through `_as_capture → _AS_OUT=$("$@" …)` (lib/assert.sh:17-19), a subshell, so `TOKa2=` is discarded. L92 `_off_semantics` uses `${TOKa2:-$TOKa}` = `$TOKa`, revoked at L148, so `_sub_loopback` returns 404/empty and `! grep 'type: ss'` is trivially true. OFF2's desc (L154) demands "0 ss nodes (DIRECT fallback, not 404/empty)". Same "global lost in a command-sub subshell" bug avoided for artifact_up (called directly at 31:68) reintroduced here.
**Fix.** Capture the token in the drill MAIN shell like TOKa/TOKb: `TOKa2=$(_sub_token alice2)`, then `assert_ok "REV3 …" sh -c "[ -n '$TOKa2' ]"` + `poll_until … _sub_is200 "$TOKa2"`. `_off_semantics` then reads the live recovered token and asserts a 200 body carrying 0 ss proxies (DIRECT), so a `proxy off`→404 regression flips RED.

### harness-safety-2 — major — `test/simcluster/drills/72-proxy-subscription.sh:77`
**Defect.** `_aead_wrongpsk`'s discriminator is vacuous: it sends a wrong-PSK relay to agt1's exit and asserts agt1 emits no `block.*destination` log, but agt1 is the `allow_private` agent that never blocks the RFC1918 sink anyway — so SS-aead greens whether or not AEAD failed, and never distinguishes AEAD-drop from dest-policy-block.
**Evidence.** L74 targets `EXIT1_PORT` = agt1's exit (extracted L136-138). L77 asserts absence of `block.*destination` on agt1, but agt1 is `proxyprivate` → `allow_private_destinations:true` (L101, agentyaml.sh:56-58), so agt1 NEVER logs a blocked-destination for the sink — the absence is guaranteed independent of AEAD. Copy/paste tells: `_t0` is measured on agt2 (L72) yet the journal is grepped on agt1 (L77); the relay result is discarded (`ss_curl … >/dev/null`, L75). The intended contrast `_ss_neg_privdest` (64-70) DOES produce a block log on agt2 (default-deny). Net: assert_ok SS-aead (L141) is pass-always.
**Fix.** Point the wrong-PSK ss-local at agt2's exit (extract `EXIT2_PORT`), take `_t0` on agt2, assert agt2 shows NO `block.*destination` in the window (contrasting `_ss_neg_privdest` which DOES on agt2 with a correct PSK), and assert the relay failed (`! ss_curl …`) — making "AEAD-drop vs dest-policy-block" a real discriminator.

---

## Lane: ledger-consistency

### ledger-1 — major — `docs/reviews/simcluster-coverage-inventory.md:123`
**Defect.** The coverage SSOT marks expose `--ack-alerts` (and `--no-rebuild`) as covered by S3-70/71 with an "independent-arm" claim, but no G-A drill exercises either flag — a false-green in the no-omission gate that also violates the plan's own disposition.
**Evidence.** Line 123 lists 6 flags but says "五 flag 全部独立臂"; both `--ack-alerts` and `--no-rebuild` are attributed to S3-70/71; line 125 routes `expose rm --ack-alerts` to S3-70; the new §4 note (351) re-asserts it. `grep -rn 'ack-alerts\|no-rebuild'` over all 8 G-A drills returns NOTHING (70 uses only --local/--remote-port/--name/--on-broker; restructured 71 uses only --on-broker, no rebuild-OFF arm). Both are real CLI flags (expose.go:140/134). Plan §6-A (375) dispositioned `--ack-alerts` → S8-92(a) NOT-COVERED-in-batch and instructed the exact G1 fix (relabel lines 123/125, fix the 五-vs-六 count) — never applied.
**Fix.** Reassign `--ack-alerts` on lines 123 and 125 from S3-70/71 to S8-92(a) (NOT-COVERED-in-batch); drop `--no-rebuild` from the "全部独立臂" claim and mark it NOT-COVERED; fix the "五 flag" count; in §4 line 351 list only the 4 drilled flags with `--ack-alerts`/`--no-rebuild` called out as NOT-COVERED-in-batch.

### ledger-2 — minor — `docs/reviews/simcluster-coverage-inventory.md:322`
**Defect.** Inventory §4 labels drill 71's #29 pin mechanism as `assert_bug` (twice), but the shipped drill uses an inverted `assert_ok`, contradicting both the drill and the gotcha ledger.
**Evidence.** §4:322 "…倒置 assert_bug + BASE…" and §4:337 "…71 assert_bug + BASE". Drill 71:70 pins #29 with `assert_ok "#29 [INVERTED] …" _home_nontunnel_unreliable` (helper returns 0 while defect live, line 39). The gotcha ledger correctly says "#29 倒置 `assert_ok`" and README says "INVERTED `assert_ok`". `assert_bug` (lib/assert.sh:42) and inverted-assert_ok (:23) are distinct constructs; "倒置 assert_bug" is also self-contradictory.
**Fix.** Change "assert_bug" → "倒置 assert_ok" on inventory lines 322 and 337.

### ledger-3 — minor — `docs/reviews/simcluster-coverage-inventory.md:357`
**Defect.** Inventory §4 attributes drill 30's partial upgrade-roll coverage SOLELY to #31 (grow lock), omitting the colocated-agent whole-host / N=2 write-fence / mid-run-HALT NOT-COVEREDs the drill itself discloses — the no-omission SSOT understates what is uncovered.
**Evidence.** §2:357 and the §4 30 bullet cite only #31 + the #31-blocked mechanism, but drill 30:163 warns "colocated-agent whole-host leg (OQ-6 — sim brokers run no colocated agent) + N=2 write-fence + mid-run HALT/resume". 30 runs `grow_to_3 0 1` (line 96, zero colocated agents) and `_all_on_next` (51) reads only broker versions, so the colocated-agent RELEASE-flip/skew leg (plan §3-30 Arm 30-B-whole-host, §11-U4) is structurally absent independent of #31. None of the three appear in the §4 landing.
**Fix.** Add the three drill-disclosed NOT-COVEREDs to the §4 30 bullet and the §2 S5-30◐ note, flagging the colocated-agent gap as a structural OQ-6 harness gap independent of #31, not merely the grow-lock block.

---

## Main-process action list
The main process is the only actor permitted to edit the drills and docs. Concrete edits, grouped to avoid duplicate work:

**A. Drill 73 — QUORUM freeze data-plane separation (closes mandate-1 + pin-2 + dp-1, one fix).**
1. In `73-proxy-cluster-ha.sh`, before the quorum kills, explicitly home ≥1 exit on the surviving leader (`--on-broker $LDR`) per plan:206.
2. In the QUORUM arm, after killing to 1/3, `ss_up` the survivor-homed exit and assert `ss_curl_ok … $SINK_TOK` (bytes still flow) WHILE a K2-homed exit's SS leg black-holes; keep `/sub`→200 + `_write_fenced` as corroborating discriminators only.
3. Optionally restore the planned disable-on-quorum-loss control arm (404 = policy, not dead broker). If SS-under-freeze is truly infeasible in-topology, replace the silent drop with an explicit NOT-COVERED note (and correct inventory honesty note ⑤).

**B. Drill 30 — write-continuity non-vacuity (closes mandate-2 + pin-1 + cluster-2).**
4. Make the write probe unbounded (`while :`, stopped only by `_stop_write_probe`) so its lifetime brackets the actual (retried) roll; make `_probe_clean` FAIL when `/tmp/wp.log` is missing/empty; assert the probe PID was alive at stop and that wp.log grew across the roll window before asserting M1.
5. Add a pre-roll baseline assert: wp.log has ≥1 successful `session create` and no WRITEFAIL in a quiescent window.
6. Either implement Arm 30-C (drop to N=2, run the writer during an upgrade, assert it DOES observe not_leader) as the plan-mandated negative control, or state the infeasibility reason and demote M1 to a labeled NOT-COVERED per plan:298.

**C. Drill 30 — roll order (cluster-1).**
7. Add an order oracle over `_BUP --dry-run` output asserting `index(LDR)==last`; if unparseable, downgrade the label to explicit NOT-COVERED.

**D. Drill 71 — #29 pin robustness + settling-vs-defect (mandate-3, pin-4, dp-2).**
8. Before the 4× probe, assert NONTUN is Phase=VOTER with non-empty cert_fp + tunnel_addr in the leader's `cluster status --json`; capture agt1's journal during a failing attempt to confirm the fixed-tunnel fallback fired; re-probe after a settle delay and reclassify #29 as an initial-delivery race (re-opening drain-rehome as COVERABLE) if it then succeeds.
9. Tighten the counted signature to `token_unknown_or_revoked` (require the home-binding token) so a fix deterministically flips the pin RED.
10. Fix the overclaiming header: drop "ALLOCATED + (dead) home / #20 class"; describe #29 as "home!=tunnel expose ROLLS BACK unreliably (frpc_failed)".

**E. Drill 72 — harness-safety false-greens (harness-safety-1, harness-safety-2) + loopback negative (dp-3).**
11. Capture `TOKa2` in the drill MAIN shell (not inside the `assert_ok` subshell); assert `_off_semantics` returns a 200 DIRECT body with 0 ss proxies (not 404).
12. Point `_aead_wrongpsk` at agt2's exit (extract `EXIT2_PORT`), take `_t0` on agt2, assert agt2 shows NO `block.*destination` and the relay failed — a real AEAD-vs-dest-policy discriminator.
13. Add `dp_curl_refused ctl1 "http://brk1:8090/sub/$TOKa"` (exit 7) as a pinned loopback-only regression.

**F. Drill 74 — auto-rebalance coverage + vacuity + unhomed (pin-3, pin-6, cluster-3).**
14. Add Arm C: set `TETHER_AUTO_REBALANCE=on` on broker units (live-verify via `systemctl show … Environment`), create skew via kill+return, assert distribution evens WITHOUT the manual verb (poll ≥180s); keep only `proxy_auto_rebalanced` count==1 as NOT-COVERED.
15. Before `_skew`, assert `[ "$(_count_on "$KTGT")" -gt 0 ]`.
16. Pair the spread assertion with a total-homed check (`_homes | grep -c .` == 3) or re-run `_proxy_ready 3`.

**G. Drill 73 nit (cluster-4).**
17. In `_pick_nontunnel`, also exclude `$(sim_leader)`, or guard `NT_HB != $(sim_leader)` before the REHOME kill.

**H. Docs / ledger corrections (mandate-4, pin-5, ledger-1, ledger-2, ledger-3).**
18. `docs/deploy-tier-gotchas.md`: remove the non-existent `drill-retry.sh` reference (also in inventory §4:370, README:280, drill 30:136); rewrite the #31 confession to the actual harness behavior (wave-split; grow/VOTER timeouts → RED, not in FLAKE_SIG); substantiate or retract the "#31 == grow-flake root cause" claim in favor of the JS-formation-timing diagnosis.
19. `docs/reviews/s3-s5-plan.md §5`: reword #30 as "code-confirmed but un-pinnable (no operator reader)"; add a NOT-COVERED ledger entry for #30 or drop the numbered "#30" live-pin reference.
20. `simcluster-coverage-inventory.md`: relabel `--ack-alerts` (lines 123, 125, 351) → S8-92(a) NOT-COVERED-in-batch, mark `--no-rebuild` NOT-COVERED, fix the 五/六-flag count; change "assert_bug" → "倒置 assert_ok" on lines 322 and 337; add the colocated-agent whole-host / N=2 write-fence / mid-run-HALT NOT-COVEREDs to the §4 30 bullet and §2 S5-30◐ note (colocated-agent = structural OQ-6 gap, not #31).

**Gate note.** No blocker; the external-review gate is not hard-blocked. Land the 11 majors (fix or honest NOT-COVERED) before submitting — several are plan-mandated false-greens whose current GREEN would mislead the external reviewer.
---

## 主进程逐条处置（step5，2026-07-12）

**总裁定**：22 findings 全部 verifier-CONFIRMED，主进程复核后**全部采纳**（无驳回）——它们精确命中了用户「你的 green 是真的没问题还是擦屁股」质疑的实证：多处 plan-mandated 假绿/vacuous-oracle + 我的事实错误。分两批修复。

### 批 A — 文档/台账修正（不需真跑）
- **mandate-4 + ledger-3**（#31 confession 事实错误）：删除 repo 里不存在的 `drill-retry.sh` 引用（它是我临时的 server-local retry 调试脚本、不入 repo）；澄清两类 grow flake——(i) 我 SOLO 遇的 `serialized fence`（brk2 grow-lock release 间歇失败挡 brk3）确是 grow-lock/#31；(ii) simcluster:223-229 记录的**并发** VOTER-timeout 是 clustered-JS 形成时序、由正式 `run-drills.sh` 以 wave-split 缓解且**不** auto-retry（FLAKE_SIG 不含 grow/VOTER timeout，surface RED 手动重跑）。#31 只钉 upgrade-blocked（30 real-roll HALT 3/3）+ serialized-fence flake，不 claim 并发 VOTER-timeout。「几乎总残留」限定为 upgrade 场景（brk3 是最后 grow、其 release 时序最差）。改 4 处：gotchas #31 / inventory §4 / README 30 行 / drill 30 注释。
- **pin-5**（#30 phantom pin）：plan §5 把 #30 描述为「INVERTED assert_ok flips on fix」，实为 `warn` NOT-COVERED（无 flipping assert）。改 plan §5 为「code-confirmed 但 un-pinnable（无 operator reader）；data-effect 由 73 REVOKE 钉」。
- **ledger-1**（inventory ack-alerts/no-rebuild 假标 covered）：改 inventory §2 相关行为 NOT-COVERED（G-A 无 drill 实测这两 flag）。
- **ledger-2**（inventory 把 71 #29 pin 写成 assert_bug，实为 inverted assert_ok）：改 inventory §4 用词。
- **dp-2**（71 header claim view/data-plane divergence 未断言）：补一句 explain/ps 仍 ALLOCATED 的 divergence 断言，或收敛 header 措辞。

### 批 B — drill 假绿修复（需真跑验证）
- **mandate-1 = pin-2 = dp-1**（73 QUORUM freeze 丢数据面分离，3 lane 汇聚、最高优先）：freeze 前 home ≥1 exit 在 survivor leader，起 SS 腿；quorum-loss 后断言 survivor-homed SS 腿仍传 sink 字节 WHILE dead-homed SS 腿黑洞。/sub-200 + write-fence 降为佐证、非终判。若拓扑不可行则显式 NOT-COVERED。
- **mandate-2 = pin-1 = cluster-2**（30 M1 write-continuity vacuity）：`_probe_clean` 在 wp.log 缺失/空时必须 FAIL（非空 guard）；roll 前断言 wp.log 有 ≥1 `session create`、roll 窗口内 wp.log 增长；30-C N=2 负对照若不可行则显式标注理由 + 降 M1 为 labeled NOT-COVERED（plan 自己说「缺 30-C 则 30-B 空转」）。
- **harness-safety-1**（72 TOKa2 在 assert_ok 子 shell 丢失，同 STAGED_SHA 坑）：TOKa2 移到主 shell 计算。
- **harness-safety-2**（72 aead wrong-PSK discriminator vacuous——agt1 是 allow_private，wrong-PSK 到它本就不 block）：改用 default-deny 的 agt2 或修 discriminator。
- **cluster-1**（30 roll order followers-first/leader-last 无 oracle）：dry-run 输出解析出顺序断言，或 roll 日志断言 leader 最后。
- **mandate-3 + pin-4**（71 #29 race vs 永久 + flaky signature）：probe 前断言 NONTUN 是 cert-eligible VOTER（leader cluster status --json）；收紧 counted signature 到 `token_unknown_or_revoked`；捕 agt1 journal 确认拨 tunnel_addr；settle 后 re-probe——若 cert-ready 后 home!=tunnel 成功则 #29 重分类为 initial-delivery race + 重开 drain-rehome-onto-settled-voter COVERABLE。
- **pin-3**（74 auto-rebalance-on-return namesake NOT-COVERED 隐藏可覆盖）：加 Arm C——TETHER_AUTO_REBALANCE=on（systemctl show 验证）+ kill+return 造 skew + 断言分布自动 even（无 manual verb）+ poll≥180s；仅 proxy_auto_rebalanced count==1 事件保留 NOT-COVERED（无 reader）。
- **pin-6 + cluster-3**（74 SKEW vacuous + unhomed）：SKEW 前断言 `_count_on KTGT > 0`；rebalance 后断言无 agent home=="none"。
- **cluster-4**（73 NT_HB non-leader 未强制）：断言 NT_HB != leader。
- **dp-3**（72 /sub loopback-only 从未断言 negative）：加 cross-container 直连 8090 应拒（loopback-only 边界）。

**流程**：批 A 先落（快），批 B 逐个改 + server-local 真跑验证；全部落地后重跑受影响 drill 确认仍 GREEN（含新断言），再交外审。**外审不过不算 done。**

---

## 批次执行状态（2026-07-12）

- **批 A（文档/事实错误修正）= DONE** ✓：mandate-4/ledger-3（#31 confession：删不存在的 `drill-retry.sh` 引用 + 区分两类 grow flake〔SOLO serialized-fence=grow-lock vs 并发 VOTER-timeout=JS-formation〕 + 订正「清 lock 后测机制」为「恢复清不掉→机制 NOT-COVERED」，改 5 处）· pin-5（plan §5 #30 从 flipping-pin 改为 code-confirmed-un-pinnable）· ledger-1（inventory §2+§4 expose `--no-rebuild`/`--ack-alerts` → NOT-COVERED）· ledger-2（inventory 71 #29 `assert_bug`→`inverted assert_ok`）· dp-2（71 header 收敛 view/data-plane divergence 措辞至实际断言范围）。

- **批 B（drill 假绿真跑修复）= TODO**，需专门工作周期（改 6-7 drill + 数十次 N=3 server-local 真跑 + grow-flake retry，与整个 G-A 同量级）。逐项修法已在上方「主进程逐条处置 · 批 B」详列。优先级：73 quorum-freeze SS 数据面分离（3 lane 汇聚）> 30 M1 write-continuity 非空 guard > 72 TOKa2/aead harness 假绿 > 71 #29 race-vs-permanent 判定 > 74 auto-rebalance Arm C > 30 roll-order oracle > 74 SKEW/unhomed + 73 NT_HB + 72 dp-3 loopback-negative。**在批 B 全部落地 + 受影响 drill 重跑仍 GREEN（含新断言）之前，G-A 不交外审、不 commit。**

**诚实定位**：Stage-C 内审的价值正是暴露了这些 G-A 交付里的假绿/vacuous-oracle——它们此前会被当成「GREEN 通过」。修复它们（批 B）是 mandate「暴露问题、绝不弱化 oracle」的直接落实，不是可跳过的收尾。

---

## 批 B 真跑落地完成（2026-07-12）

**批 B（drill 假绿真跑修复）= DONE** ✓ — 全部受影响 drill 改 + server-local 真跑验证仍 GREEN（含新断言）：

| drill | verdict | 修复的 findings | 备注 |
|---|---|---|---|
| `73` | GREEN (34) | mandate-1 = pin-2 = dp-1（quorum 数据面分离）+ cluster-4（NT_HB 非-leader） | 4 次迭代定格：freeze 下 survivor-homed SS 传字节 WHILE dead-homed SS 黑洞 + /sub-200 → 证 /sub-200≠数据面（#20 point）。rebalance+K2-poll 稳 DEAD_A、poll-18 修 SS-腿 timing、survivor 条件式（proxy home 常避 leader） |
| `30` | GREEN (13) | mandate-2 = pin-1 = cluster-2（M1 WROTE-guard 非空）+ cluster-1（roll-order oracle） | `_probe_clean` 在 wp.log 缺失/空时 FAIL；roll-order 断言 leader-last；30-C N=2 缺失给出理由（#31 阻断 upgrade → M1 suppress，负对照无对象） |
| `71` | GREEN (10) | mandate-3 + pin-4（#29 signature 收紧 + NONTUN-VOTER race 判定） | signature 收到 frpc_failed/token_unknown_or_revoked（去泛 agent_rejected）；断言 NONTUN 是 settled cert-eligible VOTER → 排除非-eligibility 假象，则 #29 失败=真缺陷非 race |
| `72` | GREEN (32) | harness-safety-1（TOKa2 主 shell）+ harness-safety-2（aead）+ dp-3（loopback-neg） | TOKa2 移出 assert_ok 子 shell；**aead 二次修正**——改用 agt1(allow_private)+断言 wrong-PSK **无 sink 字节**（agt1 从不 dest-block，无字节=纯 AEAD；agt2 的 journal-区分真跑不可靠）；cross-container 直连 8090→000 证 loopback-only |
| `74` | GREEN (23) | pin-3（Arm C auto-rebalance）+ pin-6（SKEW-precond KTGT>0）+ cluster-3（no-unhomed） | SKEW 前断言 KTGT>0 home（防 vacuous）；rebalance 后断言无 home=none；**Arm C 诚实降级**——真跑显示 `TETHER_AUTO_REBALANCE=on` 在 sim **没触发 auto-even**（post-auto 聚 leader），故 auto-path EFFECT **NOT-COVERED with reason**（非硬凑绿），manual path（Arm B）covered |

**两处"修复假设不成立"的诚实二次修正**（本身即"暴露问题"）：① 72 aead——reviewer 建议的 agt2-journal 区分真跑不可靠（agt2 wrong-PSK 也记 block-ish 行），改用 agt1-数据面-效果这个可靠 discriminator；② 74 Arm C——auto-rebalance 真跑没触发，诚实 NOT-COVERED 而非假装 auto works。二者都真跑验证、绝不弱化 oracle 凑绿。

**assertion 增量**（新断言落地）：73 27→**34** · 30 12→**13** · 71 9→**10** · 72 30→**32** · 74 17→**23**。

**结论**：Stage-C 22 findings（含 11 major 假绿）全部真跑修复绿化，无一驳回、无一硬凑。G-A 现可交外审。**外审不过不算 done。**
