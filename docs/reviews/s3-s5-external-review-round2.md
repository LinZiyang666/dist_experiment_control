# Fail — S3–S5 (G-A) external re-review round 2

Conclusion: **Fail. Do not release G-A.**  The developer did close useful pieces of the first review:
drill 72 now has an HTTP-200 gate and real revoke/off new-connection data-plane checks; drill 74's
default-off arm now proves target eligibility and a 30-second quiet window; temporary spikes are gone; and
the N=1/shared regressions passed independently.  Those improvements do not close the release gate.

The central HA drill is still non-deterministic and can continue after a foundational cluster failure.  In
three current-tree runs it produced one GREEN and two materially different REDs.  One RED had a healthy N=3
setup but could not construct the required 1+1 quorum topology after 240 seconds.  The other was a strict
serial run whose grow failed, yet later arms continued: the control-plane rehome failed, while the inverted
`[GAP #32]` data-plane assertion still passed.  The same drill also writes a live subscription bearer token
to the persistent log.  In addition, the #29 settle predicate is not a dwell, the fleet timeout assertion is
vacuous with respect to exit status/count, the install lifecycle still omits roadmap-mandated real roles and
§8.4, and the coverage documents materially overstate what the executable tests prove.

Reviewer role: independent external re-reviewer.  The appended main-process response and its claimed runs
were treated as leads only.  I reviewed the effective `HEAD`→worktree tree, checked the unstaged developer
delta separately, compared local and server hashes, and reran the current files on `weilandserver` without
automatic retry.

## Prior-finding disposition

| Prior finding | Round-2 disposition | Reason |
|---|---|---|
| M1 / M7 — drill 73 topology and required survivor control | **OPEN** | Required controls are now explicit, but construction remains non-deterministic and foundational failures do not stop later arms. One valid N=3 run timed out at Q-construct; one strict-serial run continued after failed grow/rehome. |
| M2 — drill 71 #29 attribution | **OPEN** | The claimed settle dwell is a first-success poll, and the journal evidence is not cursor/name/port scoped. Current strict runs were 4/4 and 2/4 deny/success splits. |
| M3 — revoke/off dataplane and secret hygiene | **PARTIAL** | HTTP-200 gating, cached-PSK new-connection denial, bob control, and 72's secret cleanup are real. Persistent in-flight streams and port reclaim are still absent, while 73 newly/continuingly logs the live bearer token. |
| M4 — fleet upgrade | **PARTIAL** | Runtime produced the intended two-node skip summary, but F4 never asserts rc=0 or two skipped nodes; successful fleet PID/version remains honestly NOT-COVERED. |
| M5 — install lifecycle | **OPEN** | Content hashing improved, but the manifest does not implement its claimed metadata contract; real agent/ctl lifecycle and usage §8.4 remain explicitly NOT-COVERED despite being S5 exit criteria. |
| M6 — default-off and documentation | **PARTIAL** | The eligibility + quiet-window arm passed twice serially. The executable auto wait is 60s while warnings/README claim 180s, and several old/new inventory claims contradict each other. |
| m1 — temporary spikes | **CLOSED** | The 11 staged-then-deleted spike files disappear from the effective tree after `git add -A`. |

## Blocking findings

### R2-M1 — drill 73 still has invalid-run continuation and non-deterministic topology construction (Major)

Evidence:

- `test/simcluster/drills/73-proxy-cluster-ha.sh:108` records `grow_to_3` through `assert_ok`, whose
  contract accumulates a failure and continues.  It does not fail-fast before session creation, proxy setup,
  or destructive broker kills.
- In `/tmp/s3s5-external-r2/base/73-proxy-cluster-ha.log`, grow and all pre-kill controls passed, but
  `Q-construct` waited the full 240 seconds and still could not place one exit on the leader and one on K2.
  The script then died because the mandatory survivor was empty.  Result: rc=1, no final verdict.
- In strict-serial `/tmp/s3s5-external-r2/solo2/73-proxy-cluster-ha.log`, `grow_to_3` failed with brk3 not
  reaching VOTER.  The drill continued anyway.  `REHOME [CONTROL]` timed out, the status line was empty, yet
  `[GAP #32]` passed; carol mint/revoke and Q-heal/Q-ready then failed.  This is not a valid #32 or quorum run.
- `Q-heal` at line 186 joins `proxy off; sleep; proxy on` with `;`, so an OFF failure is discarded if ON prints
  `proxy ON`.  The independent shell probe returned success with a deliberately failing OFF function.

This leaves both original M1 and M7 open.  A required positive control that exists only in some runs is not
deterministic coverage.  Worse, continuing after an invalid N=3 foundation lets later inverted/negative
oracles record misleading PASS lines.

Required fix: make foundational setup and every causal transition fail-fast (or make `assert_ok` return a
checked status at those sites); check OFF and ON as separate assertions or with `&&`; construct the exact
eligible 1+1 state before proceeding; and require two clean strict-serial runs with no setup failure, timeout,
empty status, or branch-dependent control.

### R2-M2 — the new #32 inverted oracle can pass without the state it claims, and its diagnosis is unsupported (Major)

`_ss_no_prompt_recovery` at lines 79–82 requires `/sub` fetch success, but deliberately ignores `ss_up`
failure and then negates a curl poll.  Therefore “exit absent from the rendered config”, “ss-local never
started”, and “control-plane rehome never happened” all satisfy the asserted product defect.  The local
adversarial probe confirmed the same control flow: successful fetch + failed `ss_up` + failed poll returns 0.
The strict solo2 log is the live demonstration: `REHOME [CONTROL]` failed and homes were empty, immediately
followed by a PASS for `[GAP #32]`.

The accompanying factual claims also exceed the code:

- The executable oracle observes only a 45-second non-recovery window.  It does not prove the documented
  `>150s`, “eventually ~300s”, or “never-established exit recovers in 20s” comparisons.
- The later “same exit flows” proof occurs only after `proxy off; proxy on`, which destroys and freshly
  establishes the state.  It cannot prove eventual automatic recovery of the original crash-rehome.
- `internal/tunnel/tunnel.go:937-964` shows `Client.ApplyHome` calls `OpenHome` for a newer epoch—an atomic
  session replacement and redial to the new home.  The report/README/gotcha statement that ApplyHome merely
  “re-points” an already-dead session is not what the cited implementation does.  “No SS rebuild” alone does
  not establish the asserted root cause because the SS server is intentionally independent of the tunnel.
- `docs/reviews/s3-s5-plan.md` already uses candidate `#32` for a different stale-listener hypothesis, so the
  new ledger entry also creates an unresolved identifier collision inside the same review program.

There may be a real crash-rehome latency defect—the valid runs consistently failed to send bytes within the
45-second probe—but this drill does not yet characterize or attribute it safely.  Required fix: hard-gate on
successful control rehome and the intended ready/rendered state, require `ss_up` success, record first-byte
latency without converting missing prerequisites into PASS, observe eventual automatic recovery before any
manual OFF/ON, and diagnose from broker/agent/tunnel timelines rather than a contradicted source comment.

### R2-M3 — drill 73 logs a live bearer credential and executes backticks from a warning (Major, security)

`test/simcluster/drills/73-proxy-cluster-ha.sh:126` logs `alice token=$TOK_A`.  Every current run retained the
full 43-character subscription bearer in `/tmp/s3s5-external-r2/{base,solo1,solo2}/...log`; those tokens were
valid while the session was active.  Removing the equivalent log from drill 72 did not close secret hygiene
for the delivery.

At line 167 the double-quoted warning contains unescaped backticks around `proxy off; proxy on`.  Every live
run emitted two `proxy: not found` errors and removed the text from the warning, proving the shell actually
performed command substitution on the sim host.  Today the command is absent; if a host command with that
name exists, a documentation string executes it.

Required fix: never log raw bearer tokens, PSKs, passwords, or full subscription URLs; log a count or
non-secret correlation hash only.  Escape/remove shell metacharacters in runtime strings and add a log scan
covering every G-A drill, not only 72.

### R2-M4 — drill 71's “settled deliverable home” is not settled and does not isolate #29 from a race (Major)

At `test/simcluster/drills/71-expose-rehome-failover.sh:105`, the alleged dwell is
`poll_until 12 3 ... _nontun_deliverable`.  `poll_until` returns on the first successful sample, so this can
complete immediately; it never proves continuous eligibility.  The predicate checks VOTER + `cert_fp`, but
not `tunnel_addr`, allocation replication on the selected home, or successful delivery of the exact Home
directive.  `journalctl --since <second>` is neither cursor-scoped nor tied to the attempt name/public port,
so it is weaker than an exact attempt boundary.

The current runs reinforce the concern: solo1 saw 4 precise denies/0 successes, while solo2 saw 2 denies/2
successes against the same nominal predicate.  A precise `token_unknown_or_revoked` string establishes the
deny location, but “at least one of four” does not establish the claimed permanent home-binding mechanism
when the same home also succeeds and the allocation/delivery settle state is unmeasured.

Required fix: implement a real quiet window (multiple timestamped samples or an explicit dwell followed by
revalidation), capture a journal cursor immediately before each request, bind evidence to the request's
name/port, verify non-empty tunnel address and selected-home allocation visibility, and classify mixed
outcomes as an initial-delivery/replication race unless stronger evidence rules that out.

### R2-M5 — drill 72 does not test the mandated in-flight revoke semantics or port reclamation (Major)

The roadmap (`docs/simcluster-coverage-roadmap.md:399-401`) and locked plan
(`docs/reviews/s3-s5-plan.md:173-185`) require two persistent streams: revoke must force-close alice's
already-established stream while bob's established stream continues, separately deny alice's cached-PSK new
connection, and OFF must reclaim `__proxy__` ports.

The revised drill starts two `ss-local` processes, but every `ss_curl_ok` invocation opens a new SOCKS/TCP/HTTP
connection.  It proves cached-PSK **new connection** denial and a bob new-connection control; it never keeps a
byte stream open across revoke, so it cannot prove force-close or “bob in-flight unaffected”.  The OFF arm
likewise checks new curl failure and `/sub` rendering but never asserts port state/reclamation.  Nevertheless
the header and README call this “in-flight cut” / full-stop + port reclaim and the inventory marks the surface
covered without a NOT-COVERED qualification.

Required fix: add long-lived, observable alice/bob streams with monotonically increasing bytes across the
injection; separately probe new connections with cached credentials; assert the precise allocation/listener
reclamation after OFF; or explicitly downgrade those roadmap requirements to NOT-COVERED and do not mark S4
complete.

### R2-M6 — fleet timeout coverage still has a false-pass oracle (Major)

Drill 31 captures `_allto_rc` and the full output at lines 124–125, but F4 at line 126 checks only whether the
text contains `skipped|transient`.  It does not require rc=0, two per-node skip lines, both `agt1` and `agt2`,
or the exact `(2 node(s) skipped...)` summary.  The local adversarial probe showed that rc=1 plus one synthetic
“skipped (transient)” line satisfies the current assertion.

The independent live run happened to produce correct output—rc=0, agt1 and agt2 each skipped, exact two-node
summary—but the test would not catch regressions in those properties.  `F3b` is also weak runtime evidence:
because a config error aborts on agt1, absence of `agt2` in that output follows even if agt2 were mistakenly
enumerated after agt1.

Required fix: assert rc=0; parse exact distinct skipped NIDs and count; require the final count to match the
pre-captured ONLINE set; and use dispatch instrumentation/order that can prove the OFFLINE node was not in the
target list.  Successful fleet PID/version remains honestly NOT-COVERED behind #28.

### R2-M7 — install lifecycle remains below the S5 contract and the new manifest overclaims metadata coverage (Major)

The locked S5 plan requires real install/never-start behavior for all three roles and a real usage §8.4
single-broker stop→swap→integrity→start→G.2 business convergence arm
(`docs/reviews/s3-s5-plan.md:317-327`, roadmap lines 453–455 and 745–746).  Drill 32 explicitly warns that real
agent/ctl binary placement and §8.4 are NOT-COVERED.  Line 90 is labelled “agent install” but calls
`_agent_layout`, which only executes agent `--dry-run`; no real ctl lifecycle exists.  Honest disclosure is
better than a false GREEN, but it means original M5 and the G-A exit criterion remain open.

The manifest at lines 25–31 is content-sensitive for regular files, but it does not match the claimed
“type/mode/uid/gid/sha256/link” contract:

- symlinks record only path and target, not mode/owner/group;
- regular files/directories record symbolic `%U|%G`, not uid/gid;
- FIFO/socket/device/unknown types are silently omitted;
- `for f in $(find ...)` splits whitespace-bearing paths;
- `find`, `stat`, `readlink`, and `sha256sum` errors are largely suppressed, so unreadable or vanished paths can
  disappear from both snapshots;
- the content self-test appends a blank line plus marker, then removes only the marker line, so its claimed
  restore is not byte-exact before the reinstall/idempotence arm.

Required fix: use a NUL-safe deterministic manifest with explicit lstat type, numeric uid/gid, mode, size,
link target/content digest, and loud errors; self-test each metadata dimension without leaving mutations; then
implement the missing real role and §8.4 arms or keep S5 incomplete.

### R2-M8 — the audited documentation remains internally contradictory and claims external verification that did not occur (Major)

Examples from the effective tree:

- Inventory core rows still list old counts (71=9, 72=30, 73=27, 74=17, 31=15, 32=12) while a later appendix
  lists new counts.  Event tables say S3/S4 drills assert events, while the same file later says all such events
  are NOT-COVERED because no operator reader exists.
- Drill 74 executes `poll_until 60` for Arm C (`74-rebalance-on-return.sh:139`), but its warning, README row,
  and trailing note all say 180 seconds.  Both strict runs were NOT-COVERED after the executable 60-second wait.
- The 74 header/trailing warning still says rehome is “recovering in drill 73”, contradicting the new #32 story.
- README/inventory call #32 “external-review-verified” and claim two deterministic GREEN runs.  This reviewer
  found one valid N=3 Q-construct RED and one strict-serial invalid-foundation RED in the current tree.
- README/inventory repeat the contradicted ApplyHome mechanism and claim eventual recovery using a path that
  manually cycles OFF/ON.

Coverage inventory is a release control, not narrative history.  Required fix: keep one current fact table,
move historical outcomes to dated reports, generate or parse actual assertion counts/timeouts where practical,
and reserve “external-review-verified” for an external report whose current conclusion is Pass.

## Verification performed

Static/local:

- Current remote drill and vendor binary hashes exactly matched local files; image
  `sha256:5b069074...` was built 2026-07-12 and the developer delta changed no binary/image input.
- All `#!/bin/sh` simcluster scripts passed `sh -n`; all shell scripts passed `bash -n`.
- `git diff --check` and `git diff --cached --check` passed.
- ShellCheck was unavailable on the review host.
- Focused Go tests passed outside the restricted listener sandbox with `GOCACHE=/tmp/tether-gocache`:
  `internal/agent` Upgrade/ReExec/Home/Proxy, `internal/broker` Home/Proxy/Rebalance/Grow/Upgrade, and
  `cmd/tether` Upgrade/Rebalance/Grow.
- Adversarial shell probes proved F4 accepts nonzero/one-skip output, Q-heal accepts OFF failure, and the #32
  helper accepts `ss_up` failure.

Sim server (`weilandserver`, current file hashes, no automatic retry):

- Base `-j 3`, logs `/tmp/s3s5-external-r2/base`: 61 GREEN(41), 62 GREEN(23), 80 GREEN(42), 82 GREEN(29),
  70 GREEN(28), 72 GREEN(39), 31 GREEN(26), 32 GREEN(13), 74 GREEN(24); 71 RED(1/11) from concurrent grow;
  73 RED after valid setup at Q-construct; 30 RED after concurrent grow reduced it to N=2.
- Strict serial round 1, logs `/tmp/s3s5-external-r2/solo1`: 30 GREEN(13 expected-gap path), 71 GREEN(12,
  deny:success 4:0), 73 GREEN(36), 74 GREEN(24).
- Strict serial round 2, logs `/tmp/s3s5-external-r2/solo2`: 71 GREEN(12, deny:success 2:2), 73 RED after solo
  grow/control-rehome/heal failures while #32 still recorded PASS, 74 GREEN(24).
- Drill 72 log scan found no `password:`, raw subscription URL, or long `token=` credential.  Drill 73 logs
  contained a different live alice bearer token on every run.

## Doubts and questions for the developer

1. What broker/agent/tunnel evidence demonstrates automatic recovery at ~300 seconds before any OFF/ON?  The
   submitted drill never observes that state.
2. Why is the #29 result classified as permanent when the same declared-deliverable home succeeded 2/4 times
   and the “dwell” can return immediately?
3. Is a failed solo grow now a known product gotcha distinct from the documented concurrency caveat?  The
   runner text labels every VOTER timeout a concurrency flake even under `-j 1`, contradicting its own policy.
4. Is honest NOT-COVERED intended to remove roadmap §8.4 and real three-role lifecycle from G-A acceptance, or
   are those still required before release?  No approved scope change was found.
5. Why does inventory retain historical core tables and append newer contradictory facts instead of updating
   the source-of-truth rows?

## Recommendation / next re-review gate

Do not release or merge G-A as complete.  Re-review after R2-M1 through R2-M8 are corrected.  At minimum:

1. make 73 fail-fast and causally valid, remove credential/command-substitution logging, and obtain at least
   two strict-serial valid-foundation runs with deterministic required baselines;
2. replace #32 with a prerequisite-gated latency diagnostic and correct/withhold the root-cause claim;
3. implement a real #29 settle/cursor/allocation gate;
4. add 72 persistent in-flight streams and OFF port reclaim, and make 31 assert rc/exact fleet set/count;
5. complete the S5 real lifecycle/§8.4 contract with a NUL-safe metadata manifest; and
6. reconcile README/inventory/gotcha/plan facts to the executable behavior and the actual external verdict.

Then rerun the shared set and all eight G-A drills without retry, with 71/73/74 each twice strict-serial, and
submit the raw logs plus exact file/image hashes for the next independent review.
