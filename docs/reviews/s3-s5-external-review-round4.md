# Fail — S3–S5 (G-A) external re-review round 4

Conclusion: **Fail. Do not release G-A yet.** This decision does **not** require every drill to be green. The
purpose of this suite is to expose defects, and a RED/inverted assertion can be the correct result when it is
causally tied to the intended product gap. The release blockers below are different: false-pass/false-attribution
paths in the test oracles, locked acceptance semantics that are still executablely absent without an approved
scope change, and release documents that present those gaps as GREEN/current coverage.

The developer did make useful progress. In particular, drill 73 now runs the admin-only rebalance verb on the
leader broker instead of silently issuing it from ctl; several destructive baselines are hard-gated; drill 32's
manifest records numeric IDs and more file types; drill 71 retracts the unsupported “permanent binding” claim;
and drills 32/71/72/74 now state several limits honestly. Those changes do not close the round-3 gate.

Reviewer role: independent external reviewer. Developer/self-review statements were treated as hypotheses, not
authority. I reconstructed the eight-file unstaged delta over the staged round-3 tree, rechecked the locked plan
and current code, ran adversarial probes, verified local/server hashes, and ran the changed drills on
`weilandserver` in fresh strict-serial instances with automatic retry disabled.

## Round-3 closure matrix

| Round-3 finding | Round-4 disposition | Basis |
|---|---|---|
| R3-M1 — drill 73 construction/readiness | **PARTIAL / OPEN** | The admin-command routing bug and several fail-fast baselines are fixed. The new #33 oracle can pass on local `ss-local` startup failure/race, and the first current-hash strict run again died before reaching any #33/quorum arm. |
| R3-M2 — drill 71 causality | **OPEN** | Permanent-binding wording was retracted and repeated samples were added, but command rc is discarded, “success” has no data-plane proof, journal evidence is not attempt-scoped, and four successes still produce GREEN without exposing the claimed fallback. |
| R3-M3 — drill 72 in-flight/reclaim | **OPEN / ADMITTED** | No executable implementation was added; the new warning correctly says persistent-stream force-close and listener/port reclamation are NOT-COVERED. No owner-approved scope change was found. |
| R3-M4 — drill 32/S5 contract | **PARTIAL / OPEN** | Manifest metadata coverage improved. Real agent/ctl placement, three-role never-start/uninstall, and usage §8.4 remain explicitly NOT-COVERED; the manifest remains fail-open on traversal/stat/hash errors. |
| R3-M5 — release documentation | **OPEN** | Some rows were softened, but the gotcha ledger still carries the retracted #29 account, current documents contradict one another, and locked criteria are still labelled GREEN. |

## Release-blocking findings

### R4-M1 — drill 73's new #33 “instant black-hole” assertion cannot distinguish a product gap from its fresh local client startup (Major)

The new oracle at `test/simcluster/drills/73-proxy-cluster-ha.sh:216-222` fetches a subscription, backgrounds a
new `ss-local`, starts the clock, and immediately treats **any** failed curl as the product black-hole. That does
not observe “the instant control reports rehomed+ready”:

- The control transition can consume the preceding 60-second home poll and 90-second ready poll, followed by a
  status log, subscription fetch, YAML parse, and process launch. `_t33` starts only after all of that. The logged
  lag is therefore from local client launch, not from the control-ready edge.
- `ss_up` in `drills/lib/proxy.sh:33-45` only asks a remote shell to execute `nohup ss-local ... & echo ok`. It
  never proves that the child survived, bound `127.0.0.1:1081`, or is accepting SOCKS. A synthetic nonexistent
  executable returned rc=0 through the same wrapper. `_ss_deadhole` at drill 73 line 111 accepts every curl
  failure, including no local listener, malformed/stale YAML, and process death.
- The pre-kill baseline does not protect this fresh process: the drill kills all `ss-local` processes at line
  200 and creates a different client only after rehome. Thus “this exact leg flowed” in the assertion text is
  false at the process/listener boundary.
- The product code weakens the claimed “physical guarantee.” `TunnelExposeAdapter.ApplyHome` calls the blocking
  `Client.ApplyHome`/`OpenHome`; `OpenHome` does not return until dial+REGISTER+yamux succeeds and the replacement
  session is installed (`internal/tunnel/tunnel.go:849-963`). The agent then re-ACKs `ready=true`
  (`internal/agent/proxy.go:132-183`). That does not prove all end-to-end traffic is already good, but it directly
  refutes treating an unready local SOCKS client as inevitable evidence that product data-plane recovery trails
  control readiness.

The fixed-threshold claim is also internally inconsistent. The drill says it does not gate a fixed lag, yet it
requires the data plane to be dead at one arbitrary post-ready sample and turns a prompt recovery into RED. A
flip-on-fix is valid for an inverted defect pin only when the sample causally measures that defect; this one does
not.

Runtime evidence is also incomplete: both `/tmp/s3s5-external-r4/solo1/73-proxy-cluster-ha.log` and
`/tmp/s3s5-external-r4/solo2/73-proxy-cluster-ha.log` were RED before the first substantive arm because
`grow_to_3` left brk3 short of VOTER. Both were `-j 1 --no-retry`, so the emitted “concurrency flake” label is not
supported by the run conditions. The required current-hash evidence is therefore 0/2 complete executions
reaching #33 and every quorum branch; a setup RED is useful evidence of constructibility, not something an
automatic rerun may erase.

Required fix: make local SOCKS readiness a hard prerequisite (child alive plus listener/protocol readiness), bind
timestamps to an observed control transition, keep setup failures separate from the product negative, and use a
continuous/attempt-scoped trace that distinguishes local-client startup, tunnel readiness, proxy readiness, and
first successful bytes. Then obtain two complete no-retry strict runs.

### R4-M2 — drill 71's outcome classifier can turn command failure into “success” and can GREEN without exposing #29 (Major)

`_home_nontunnel_probed` captures command text at lines 51-52 but discards the command exit status. Lines 53-59
classify any output lacking a short error-word list as success. The exact adversarial case `rc=1, output=""`
produced `deny=0 success=1 unexpected=0`; repeated four times it satisfies the final gate at line 71. Even a real
successful command is not resolved to its allocated port and curled, so the comment “works” is not established.

The new gate also accepts `0 deny / 4 success`. Given the stated purpose of this drill—expose the silent-fallback
problem—that outcome shows no occurrence of the target gap and should be reported as “not observed,” not a
completed defect pin. Conversely, a precise deny match is gathered with `journalctl --since <wall-clock second>`
and a generic token string, not a cursor bounded before/after the attempt or evidence tied to its name/port. A
stale deny within the same second can therefore bless an unrelated `frpc_failed`.

The source attribution remains stronger than the evidence. `grow_to_3` explicitly provisions broker tunnel
addresses; without observing the selected home's delivered directive/dial target, the same symptoms can arise
from allocation visibility or directive-delivery timing. Four samples at approximately t=0/3/6/9 followed by a
final sleep are also not a verified t=0..12 continuous dwell. Finally, the locked S3 plan's forward/drain rehome
and recovery semantics remain explicitly NOT-COVERED at line 133, with no approved scope change.

Required fix: capture and gate rc; on success resolve the new allocation and prove sentinel bytes; on failure use
an attempt cursor plus attempt-specific allocation/name/port evidence; report zero target denies as not observed;
and either implement the locked rehome/drain arms or record an owner-approved scope change in the source of truth.

### R4-M3 — drill 72 still omits the locked persistent-stream and OFF reclamation semantics (Major)

This round changes only claims/warnings. The executable still polls fresh curls after revoke and OFF
(`72-proxy-subscription.sh:183-207`). It therefore proves rejection of a **new** stream, not force-closure of an
already-established alice stream while an already-established bob control stream continues. It also never
inspects the `__proxy__` public listener or `port_allocations` and never proves safe port reuse after OFF.

That warning is honest and useful; it is not a fix or a scope decision. The locked plan at
`docs/reviews/s3-s5-plan.md:172-186` requires both semantics and even describes the allowable downgrade to
NOT-COVERED. The current coverage documents nevertheless label drill 72 GREEN for G-A. Required fix: implement
held-open observable byte streams and listener/allocation reclamation, or obtain and record an explicit release
scope change instead of treating an admitted gap as completed coverage.

### R4-M4 — drill 32 remains fail-open and still does not execute the locked S5 role/install contract (Major)

The numeric `%u/%g`, symlink metadata, other-type branch, and whitespace-safe `read` are improvements. The
manifest at lines 30-37 still suppresses `find`, `stat`, `readlink`, and `sha256sum` errors inside a pipeline whose
status is the final `while`. Missing roots therefore produced rc=0 and an empty digest. Two equally failed
snapshots can compare equal and certify “zero write.” Empty stat/hash substitutions are also serialized rather
than rejected.

The content self-test appends a blank line plus marker, then deletes only the marker line; it does not restore the
original bytes. More importantly, the executable still performs only a real broker install. The “agent install”
assertion at line 96 is a dry-run path grep, while lines 99-100 explicitly leave real agent/ctl binary placement
and usage §8.4 NOT-COVERED. The locked plan at lines 317-327 requires real install/never-start behavior for all
three roles, uninstall cleanup, and the single-broker stop→replace→integrity→start→business-convergence path.

Required fix: make manifest production fail closed and NUL-safe, preserve/restore the self-test target byte for
byte, execute real role artifacts (the existing drill-31 artifact staging is a viable source), and implement or
formally rescope the remaining S5 arms.

### R4-M5 — drill 74 weakens a locked timing oracle and builds distribution facts from non-atomic snapshots (Major)

The changed text now reconciles the executable to a 60-second Arm-C poll. The locked plan requires a tolerant
`poll>=180s` because the trigger includes about 30 seconds of dwell plus a 60-second quiet window
(`s3-s5-plan.md:235-242`). Changing the narrative from 180 to 60 does not satisfy that oracle; it makes a delayed
valid auto-trigger look NOT-COVERED. The same locked section also requires the exact-one event anti-flap oracle,
per-exit `/sub` homes and SS bytes after movement, and an ordinary expose negative control. The drill explicitly
leaves these absent and delegates data-plane proof to drill 73, which is currently RED and whose rehome oracle is
invalid.

There is an additional observed oracle defect. `_dist` and `_spread` (`74-rebalance-on-return.sh:19-30`) execute a
new full `proxy status` query for each broker count, so one “distribution” can combine three moments during
reconciliation. The live log printed `post-return distribution: brk1=0 brk2=2 brk3=1` and on the same line claimed
“brk2 ... carries 0 homes”; the subsequent stable-empty assertion passed. The contradiction is direct evidence
that log/current-state claims cannot be inferred from the composite snapshot. The same helper underpins
byte-identical dry-run and spread checks.

Required fix: capture one JSON status document per sample and compute every count/spread from it; use the locked
timing budget and required event/data-plane/control oracles, or record an approved scope revision. A per-run
NOT-COVERED outcome is acceptable evidence, but not completion of the locked acceptance item.

### R4-M6 — current release documents still contain mutually exclusive facts (Major)

Examples:

- `docs/deploy-tier-gotchas.md:88-135` still says #29 is a home-binding death, that any rehome makes it permanently
  dead, and that a `>=1/4` inverted pin live-confirms the defect. Drill 71, README, and inventory now retract that
  claim and accept all-success runs.
- README's drill-73 row says it does not claim eventual automatic recovery or any `>45s` figure while the same
  paragraph says `<45s..>150s` and the executable requires automatic recovery within 240 seconds. It also retains
  the stale `#32's slow crash-rehome` phrase although the finding was renumbered #33.
- README/inventory label 71, 72, 32, and 74 GREEN while their executables explicitly warn that locked acceptance
  semantics are NOT-COVERED. Historical reports are useful evidence, but the inventory also calls older round
  states authoritative/current while newer current-hash evidence contradicts them.
- The 74 timeout was normalized to 60 seconds in prose, but the locked plan still requires at least 180 seconds;
  this is not a harmless count correction but an unresolved scope/acceptance disagreement.

Required fix: keep one explicit current release table generated or mechanically checked against executable
counts/timeouts/results; mark historical facts as dated; and require an owner decision before a locked criterion
can move from required to NOT-COVERED/GREEN.

## Verified improvements and correctly exposed/recorded issues

- Drill 73's `_rebal` now executes the admin verb on the current leader under the tether user. This is a concrete
  root-cause fix for round-3's silently ineffective ctl invocation.
- Drill 73's eligibility query and fail-fast steady-state/pre-kill/Q baselines are materially safer than the prior
  accumulated-assert continuation.
- Drill 71 correctly retracts the permanent-home-binding account in its header/README/inventory and records the
  observed deny/success split. The first strict run observed 4/4 precise denies; this is useful evidence, though
  the classifier still needs the controls above.
- Drill 72 correctly records that new-connection denial is not persistent-stream force-close and that OFF status
  is not port reclamation. Drill 74 similarly records when its auto path does not fire. These are valid exposed
  gaps; they become release blockers only because the locked gate was neither implemented nor formally changed.
- Drill 74's new nonzero-home precondition prevents a vacuous destructive kill. Its first strict run correctly
  reported the auto effect NOT-COVERED after a 60-second timeout while retaining a GREEN harness verdict.

## Verification performed

Static/local:

- Exact unstaged developer delta: eight modified files, 227 insertions / 146 deletions; no unstaged deletion or
  extra developer file. The round-4 tasklist/report are reviewer-created files.
- Shebang-aware `sh -n`/`bash -n` passed for the simcluster shell tree. `git diff --check` and
  `git diff --cached --check` passed before final staging. ShellCheck was unavailable.
- Adversarial probes: drill-71 classifier mapped command `rc=1` plus empty output to success; drill-32's
  missing-root manifest pipeline returned rc=0; drill-73's background launch wrapper returned rc=0 for a
  nonexistent executable.
- Local and remote drill hashes matched: 32 `55109dfbdcf34a7ea3c51ca6edf88e3ac65c11208f4e310a181187a0d520883e`;
  71 `4b9ea2298681fe800d45ea898965a3c27e148fcf3019a5adaebac85746f80bcc`; 72
  `b760e7d83c362605cc81d3b1ea860b50ab40544f119a6ca464a454cf55343420`; 73
  `09890d524371c6d0ccb7a818ce5838c04b52bd2ae8b977aebb6d0d61f5cb27d6`; 74
  `3f50020b1cb39d739cf30dc7b470a3baafec3e4090fbd585c0a6477bedf88c5d`.
- No changed image/runtime input required a rebuild. Remote image was
  `sha256:5b069074576524e20ea17667d79d80985f8b7b403021e6917e8afe140a3edb11`; vendored `tether` and
  `tether-next` hashes were `497121d...d7474` and `2ea831d...eb5`.

Sim server (`weilandserver`, fresh strict-serial instances, no automatic retry):

- `solo1`: 32 GREEN (13), 71 GREEN (12; split 4 deny / 0 success / 0 unexpected), 72 GREEN (39),
  73 RED before drill_end because brk3 did not reach VOTER, 74 GREEN (24) while explicitly recording its 60-second
  auto-rebalance effect NOT-COVERED. Full logs: `/tmp/s3s5-external-r4/solo1/`.
- `solo2`: 71 GREEN (12; again 4 deny / 0 success / 0 unexpected); 73 RED before drill_end because brk3 again
  did not reach VOTER. The two required 71/73 runs were therefore completed, but drill 73 reached none of its
  intended product-gap or quorum assertions in either run. Full logs: `/tmp/s3s5-external-r4/solo2/`.
- A count-only scan across both retained log sets found zero raw long `/sub/<token>` values, zero `password:`
  fields, and zero backtick-induced `proxy: not found` signatures.

No GREEN above was treated as proof of release readiness by label alone; each was compared with its actual
assertions and NOT-COVERED warnings.

## Doubts and questions for the developer/owner

1. What observation isolates product data-plane lag from the newly launched local `ss-local` process, and where
   is the timestamp of the actual rehomed+ready edge captured?
2. Why should a zero-deny drill-71 run count as “#29 exposed,” and why is command rc not part of the classifier?
3. Where is the owner-approved decision moving persistent in-flight revoke, OFF port reclamation, forward/drain
   expose rehome, real agent/ctl install, §8.4, and drill-74's event/data-plane controls out of G-A? No such decision
   is present in the reviewed tree.
4. Is the 74 plan intentionally changed from `poll>=180s` to 60 seconds? If so, which source is authoritative and
   why does the locked plan remain unchanged?
5. Why does a strict-serial drill-73 grow failure emit an unconditional “concurrency flake” diagnosis? What
   evidence distinguishes it from a real serialized constructibility defect?

## Re-review gate

Do not release G-A as complete. This is not a demand for cosmetically all-GREEN results. Correctly exposed known
product gaps may remain pinned with precise inverted assertions or formally accepted NOT-COVERED status. Re-review
after the test false-pass/attribution paths are fixed, locked gaps are implemented or explicitly owner-rescoped,
the release documents agree, and drill 73 completes every arm in two current-hash strict no-retry runs.
