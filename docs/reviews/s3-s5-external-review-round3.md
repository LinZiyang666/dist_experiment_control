# Fail — S3–S5 (G-A) external re-review round 3

Conclusion: **Fail. Do not release G-A.** The developer closed four concrete round-2 defects: drill 73 now
fails fast after an invalid grow/foundation, requires a successful control rehome and `ss_up`, separates proxy
OFF and ON, and no longer logs the bearer token or executes backticks from a warning; gotcha #33 is correctly
renumbered and narrowed to a bounded 45-second observation; and drill 31 now checks rc, the complete ONLINE
target set, distinct per-node skips, and the exact fleet summary.

Those fixes are useful but do not satisfy the release condition. The current-hash drill 73 was **RED in both
fresh strict-serial runs without retry**. One valid N=3 run could not construct a non-tunnel home in 150 seconds;
the other constructed the topology and completed the control rehome, then died because the exit was not yet
proxy-ready. Neither run reached the #33 observation and quorum-loss proof. Separately, the unchanged locked
coverage gaps in drills 71, 72, and 32 remain open, and the release-control documents still claim GREEN and
coverage that the executable tree and current external evidence contradict.

Reviewer role: independent external reviewer. Internal reports and developer claims were treated only as leads.
I reconstructed the five-file follow-up separately from the staged round-2 tree, reread the locked plan and
coverage controls, reviewed the effective tree, compared local/server hashes, ran adversarial probes, and ran
the current drills directly on `weilandserver` with automatic retry disabled.

## Round-2 disposition

| Round-2 finding | Round-3 disposition | Basis |
|---|---|---|
| R2-M1 — drill 73 invalid continuation/topology | **PARTIAL / OPEN** | Foundation and rehome fail-fast gates are fixed. Required topology/readiness remains non-deterministic; current strict runs are 0/2 GREEN. |
| R2-M2 — vacuous #32 oracle/unsupported diagnosis | **CLOSED** | Now #33; `ss_up` and ready/rehome are prerequisites; claims are limited to 45 seconds and the wrong ApplyHome diagnosis is retracted. |
| R2-M3 — bearer leak/backtick execution | **CLOSED** | Current logs contain only token length and no `proxy: not found`; no raw token pattern was found. |
| R2-M4 — drill 71 causality | **OPEN** | Drill 71 was unchanged; the first-success poll is still described as a dwell, and evidence remains unscoped to a journal cursor/allocation. |
| R2-M5 — drill 72 in-flight/reclaim | **OPEN** | Drill 72 was unchanged; it still creates new HTTP connections and never proves persistent-stream closure or listener/port reclamation. |
| R2-M6 — drill 31 fleet oracle | **CLOSED** | The strengthened oracle rejects the synthetic bad output and passed live with rc=0, two distinct skips, and the exact two-node summary. |
| R2-M7 — drill 32/S5 contract | **OPEN** | Drill 32 was unchanged and explicitly leaves real agent/ctl placement plus usage §8.4 NOT-COVERED. |
| R2-M8 — contradictory release documentation | **OPEN** | Core rows/counts and current status remain stale or mutually contradictory, including a current RED drill labelled GREEN. |

## Release-blocking findings

### R3-M1 — the central HA drill is 0/2 GREEN on the submitted hash (Major)

The added hard gates prevent the old false-green continuation, but they expose that the required scenario is
still not reliably constructible:

- `_construct_nontunnel` at `test/simcluster/drills/73-proxy-cluster-ha.sh:65` is only a rebalance plus a status
  sample. The test calls it for 150 seconds at lines 149–155, assuming a fresh voter's proxy eligibility will
  become usable during that interval. In `solo1`, grow, all three voters, both exits, ingress, and proxy readiness
  passed, but both exits remained `brk1/rdy=true` for the full interval. The drill died before its first
  steady-state data-plane proof.
- In `solo2`, the same submitted file built a valid non-tunnel topology, passed steady-state byte flow, killed
  the live non-leader home, and passed the bounded `_rehomed_off` control transition. It then immediately called
  `_agent_ready` at line 182 without a poll and died with `REHOME agt2 not proxy-ready after rehome`. Home movement
  and ready/render propagation are asynchronous states; an instantaneous sample converts an ordinary transition
  window into a branch-dependent RED. If readiness never returns, that is a product finding that must be measured,
  not an unclassified one-sample failure.
- The later quorum constructor at lines 95–102 retains the same rebalance/status-placement dependency. Because
  both current runs die earlier, neither #33 nor the required dead-leg/survivor-leg quorum separation received
  any round-3 execution evidence.

The logs are `/tmp/s3s5-external-r3/solo1/73-proxy-cluster-ha.log` and
`/tmp/s3s5-external-r3/solo2/73-proxy-cluster-ha.log` on `weilandserver`. Both runs used local hash
`b2520f31c0c91eaa3bb78af20f2bb77ae7ead9bade4b17eae689c6e28e2eb8aa`, matched remotely, ran fresh and
strict-serial, and had no automatic retry.

Required fix: gate construction on the actual broker proxy-eligibility state rather than a fixed-time assumption;
poll the complete rehome state (new non-dead home **and** rendered/ready) with a diagnostic deadline; preserve
fail-fast behavior; and obtain at least two complete strict-serial GREEN runs reaching every required data-plane
baseline and destructive arm. A longer blind sleep alone is not a deterministic oracle.

### R3-M2 — drill 71 still does not distinguish a stable #29 mechanism from allocation/delivery races (Major)

The developer did not modify drill 71. Its claimed settle at lines 100–102 is still `poll_until 12 ...`, which
returns on the first successful sample and therefore does not establish a continuous dwell. The attempt evidence
uses `journalctl --since <wall-clock second>` and a generic `token_unknown_or_revoked` match; it is not bounded by
a journal cursor and is not tied to the attempt name, public port, token, or selected-home allocation. The test
also does not prove the selected home has observed the exact allocation before counting a deny.

This matters because round-2 strict runs produced materially mixed outcomes (4 deny/0 success and 2 deny/2
success) under the same declared predicate. The precise journal string locates a denial, but the current oracle
does not justify the README's permanent-mechanism and “LIVE-CONFIRMED” language.

Required fix: implement an actual quiet window with repeated samples, capture a journal cursor immediately before
each attempt, bind the evidence to that attempt, and prove selected-home allocation/directive visibility. Classify
mixed results as a race unless stronger evidence rules it out.

### R3-M3 — drill 72 still omits locked persistent in-flight and OFF reclaim semantics (Major)

The locked plan requires established alice and bob streams to span revoke, with alice's existing stream force-closed
while bob's continues, separately from cached-credential new-connection denial; it also requires `proxy off` to
reclaim the `__proxy__` listener/port. Drill 72 was unchanged. Lines 187–193 keep `ss-local` processes alive but
each `ss_curl_ok` opens a new SOCKS/TCP/HTTP connection, so the assertions prove new-connection behavior rather
than closure/continuity of already-established byte streams. Lines 199–202 assert new curl failure and `/sub`
rendering only; no listener or allocation port is inspected. The header still claims “in-flight cut” and “port
reclaim” (`72-proxy-subscription.sh:5-6`).

Required fix: run observable long-lived alice/bob byte streams across revoke, independently test cached-PSK new
connections, and assert listener/allocation disappearance plus safe port reuse after OFF. Otherwise obtain an
explicit scope change and mark those acceptance points NOT-COVERED instead of GREEN.

### R3-M4 — S5 install acceptance remains explicitly incomplete (Major)

The locked S5 plan assigns real install/never-start behavior for all three roles and usage §8.4's single-broker
stop→replace→integrity→start→business-convergence path to drill 32. The file was unchanged and explicitly
warns that real agent/ctl placement and §8.4 are NOT-COVERED at lines 92–94. Line 90's label says “agent install”,
but `_agent_layout` only runs `--dry-run` and greps for `.local/bin`; there is no real ctl lifecycle.

The manifest also still falls short of the README's stated per-file type/mode/uid/gid contract: symlinks omit
ownership/mode, regular entries record symbolic users/groups rather than numeric uid/gid, unsupported types and
many command errors disappear silently, whitespace-bearing paths are split by `for f in $(find ...)`, and the
self-test's cleanup removes only the marker line rather than restoring an exact byte snapshot.

Honest NOT-COVERED is preferable to a fabricated pass, but no approved change removes these items from G-A.
Required fix: complete the real role and §8.4 arms and make the manifest deterministic/NUL-safe/error-strict, or
formally move the criteria out of this release and update every source of truth.

### R3-M5 — release-control documents still contradict current executable evidence (Major)

Examples in the submitted tree:

- README line 276 labels drill 73 GREEN, describes its construction as deterministic, retains the stale phrase
  `#32's slow crash-rehome recovery`, and simultaneously says the round-2 remediation is not reverified. The
  current external result is two REDs, not GREEN.
- README line 278 says drill 31 has 26 assertions; the submitted script and live verdict have 28. README line 277
  and drill 74 lines 139/144–147 claim a 180-second auto window while the executable poll is 60 seconds.
- Inventory core rows 321–333 retain obsolete counts (71=9, 72=30, 73=27, 74=17, 31=15, 32=12), while later
  appendices list different counts. Its event tables assign proxy/rehome events to drills, but lines 342–348 later
  say all those events are NOT-COVERED because no operator reader exists.
- README line 279 states the stronger manifest contract that drill 32 does not implement, and README lines 274–275
  overclaim the unchanged dwell/in-flight/port-reclaim semantics described above.

These files are used as the coverage/release inventory, so stale GREEN status is not cosmetic: it would authorize
a release whose central acceptance drill cannot complete. Required fix: maintain one current fact table generated
or checked against live verdicts/counts/timeouts, move historical results into dated reports, and never label a
current externally-RED or unexecuted remediation GREEN.

## Verified closures and non-blocking observations

- Drill 31's R2-M6 fix is sound for the requested failure path. A synthetic `rc=1`/single-skip output now returns
  failure. The live strict run was GREEN (28), logging `rc=0 online_n=2 skip_lines=2`, both NIDs, and the exact
  `(2 node(s) skipped due to transient errors ...)` summary. Successful fleet PID/version remains honestly gated
  by gotcha #28 and is not counted as newly covered.
- Drill 73's `ss_up`-failure adversarial probe now returns failure, so missing setup can no longer satisfy #33.
  The raw bearer token and warning-command-substitution findings are closed; current logs contain token length only.
- Gotcha #33's renumbering and factual narrowing are correct. It no longer collides with the plan's #32 candidate,
  no longer attributes the issue to ApplyHome re-pointing a dead session, and claims only the executable 45-second
  observation.
- The initial server file transfer required explicit acknowledgement because the outer control cluster reported
  `force_single_active`; this acknowledgement was used only to copy the two reviewed drill files. Tests themselves
  ran inside fresh throwaway sim instances and did not use automatic retry.

## Verification performed

Static/local:

- Reconstructed the exact five-file developer delta and verified that drills 71/72/32/74, Go code, vendored
  binaries, and baked image inputs were unchanged.
- All shebang-aware `/bin/sh` syntax checks and all shell `bash -n` checks passed. `git diff --check` and
  `git diff --cached --check` passed. ShellCheck was unavailable on the host.
- Local hashes: drill 31 `e08dedf68f9f469a3a23fc6efe68684661af2e45a50157ac733f21013a3f197b`;
  drill 73 `b2520f31c0c91eaa3bb78af20f2bb77ae7ead9bade4b17eae689c6e28e2eb8aa`. Remote hashes matched.
- Adversarial probes confirmed the new fleet oracle rejects nonzero/partial output and the #33 helper rejects a
  failed `ss_up`. No changed Go/runtime input justified rebuilding the byte-identical image or repeating the
  already-passing round-2 focused Go/shared suite.

Sim server (`weilandserver`, fresh strict-serial instances, retry disabled):

- `solo1`: drill 31 GREEN (28); drill 73 RED after a valid N=3/2-ready foundation because the 150-second
  non-tunnel construction left both exits on brk1.
- `solo2`: drill 73 RED after valid construction and steady-state byte flow; control rehome completed, but the
  immediate ready hard gate failed. Neither run emitted a final GREEN verdict.
- Log scan found no raw bearer-token pattern, `password:`, or backtick-induced `proxy: not found` in the current
  drill 73 logs.

## Doubts and questions for the developer

1. What observable state proves brk2/brk3 are proxy-home eligible, and why should it necessarily appear within
   150 seconds? The current voter/ready predicates do not establish that state.
2. Is post-rehome `ready=false` expected transiently after `home_broker` changes? If yes, why is it sampled once
   instead of polled as one compound transition; if no, should the solo2 trace be registered as a product defect?
3. Where is the approved decision removing persistent-stream revoke, OFF port reclaim, real agent/ctl placement,
   and §8.4 from the locked G-A gate? None exists in the reviewed tree.
4. Why does the current README retain a GREEN label for 73 after explicitly noting that the remediation had not
   been reverified, and why are executable counts/timeouts not mechanically checked?

## Re-review gate

Do not release or merge G-A as complete. Re-review after R3-M1–M5 are resolved. At minimum, obtain two complete
strict-serial current-hash GREEN runs of drill 73 that reach #33 and the quorum data-plane separation, implement
or formally rescope the unchanged locked gaps in 71/72/32, and reconcile the README/inventory/ledger with actual
executable results. Preserve the newly added fail-fast and secret-hygiene protections.
