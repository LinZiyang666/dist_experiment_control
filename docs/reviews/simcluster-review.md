# Stage C Internal Review — `test/simcluster/` (Docker sim-cluster dev tool)

> **Provenance:** 6 adversarial reviewers (shell / fidelity / drills / plan-attribution / security /
> reproducibility) + 1 synthesizer, all Opus 4.8. Every fix is a scaffolding/drill edit — zero product changes.

## Main-process adjudication (2026-07-05)

Verdict adopted: sound for external review after the BLOCKER + false-green MAJORs are fixed. Below is
what the main process FIXED vs DEFERRED (with justification). The synthesized report follows verbatim.

**FIXED (this pass):**
- **B1** rsync no longer wipes the server-side `secrets/` stash (protect-filters, drop `--delete-excluded`).
- **M1** #20 signature restored to the conjunction `bucket_create_failed.*(503|10008)` (no bare `no responders`/`timeout`).
- **M2** #12 signatures pinned to captured product strings (dropped `already standalone`, `refusing`, dead `voters need`, `raft config`, `not a member`).
- **M3** `agent-join` now polls the leader roster for `ONLINE` before printing `ok`.
- **M4** `cmd_drill` guards `|| _rc=$?` + an `EXIT/INT/TERM` trap so teardown always runs.
- **M5** every destructive drill + `setup-forcesingle.sh` refuses to run on a non-`drill-*` instance.
- **M6** removed the `/etc/tether` chown that MASKED #22; provisioning now leaves it root-owned (faithful); `doctor` INVERTED to flag root-owned as the reproduced-#22 tripwire; grow works because the *manual* reconcile writes as root (the operator path), the in-broker auto-reconcile stays broken exactly as on the fleet.
- **M8** grow-to-3 restored the cheap OQ-6 hollow-voter parity (joiner `cluster_nodes` row-count == leader's + journal grep for `panic`/`FOREIGN KEY`/`unrecognized raft role`).
- **M9** follower-kill HA proof now `docker kill`s the follower CONTAINER (drops its raft node → real 2/3) + leader-resolved reads.
- **M12** #21 tier-B push regains `--ack-alerts` so the signal stays on the JS storage-admission path.
- **M13** drills/README use `SIMPIN` (default `135790`), not the fleet PIN `040415`.
- **M10** nats-server pin now derived from `vendor/install.sh` and asserted against it (no hardcoded literal).
- **m1** deleted the dead cluster-seam block in `provision-node.sh`; **m2** rewrote the stale CN-only header in `secrets.sh`; **m6** grow-to-3 leader-resolves the leader-only fields; **m8** sharpened gotcha #23's mechanism (root fix is the unit `Restart=always`; broker already `MaxReconnects(-1)`).

**DEFERRED (documented follow-on, justification):**
- **M7** exercise `reconcile nats --all` / in-broker auto-convergence — blocked on the #22 fix (the reconciler can't write until then); a convergence drill lands with the #22 remediation.
- **M11** git-sha image stamp + `doctor` freshness check — README already flags the baked-file-needs-`build` rule; a sha stamp is the durable fix, follow-on.
- **M6-drill** a dedicated RED #22 drill (beyond the inverted `doctor` tripwire) — follow-on with M7.
- **m3** `flock` mutating-verb guard, **m4** pty post-answer deadline, **m5** #20 recovery arm, **m7** #12 shape-split, **m9** `nuke-all` reaper, **m10–m14** version-pinning/checksum/portability/preflight polish — all noted; none is a today-false-green on the shipped gates.
- The InstallSnapshot-forcing arm (OQ-6) remains deferred per the plan; the cheap hollow-voter parity (M8) is now shipped.

---

# Stage C Internal Review — Synthesized Report: `test/simcluster/` (Docker sim-cluster dev tool)

All paths repo-relative to `/home/weiland/projects/dist_experiment_control/`. Findings merged from 6 adversarial reviews, deduped and severity-ranked. Known-open items are dropped unless a reviewer showed them materially worse. Every fix below is a drill/scaffolding edit — **zero product changes**.

---

## BLOCKER

**B1 — `remote.sh` rsync `--delete-excluded` silently deletes the server-side `secrets/` stash on every call → the documented persistent-cluster workflow is broken and minted key material is destroyed**
`test/simcluster/remote.sh:59-61` — *(R1-MAJOR1, R6-BLOCKER1; R6 reproduced live)*
`--delete-excluded` implies `--delete`; `secrets/` is minted **server-side** (`lib/secrets.sh`, `$HERE==$REMOTE_DIR`) and never exists in the WSL source tree, so it is wiped on **every** `remote.sh` verb — including read-only `status`/`logs`. Failure: `./remote.sh init brk1` mints `_shared/{cluster-ca,account}`; the next `./remote.sh grow brk2` rsyncs first (wiping the stash), then `secrets_ensure_shared` mints a **brand-new CA** → brk2's route cert chains to a CA the running brk1 doesn't trust → route mTLS never handshakes → grow fails. Also `secrets_broker_pub brk1` reads a deleted `broker.nk` → garbage `--peer` triple rendered into brk1's nats.conf. A previously-healthy persistent instance loses its stash on the next verb and can no longer be grown/rekeyed. The drill path escapes this only because `cmd_drill` runs the whole up→init→grow flow in one server-side process with no intervening rsync — which is exactly why Phase-2 grow proved green while this path was never exercised.
**Fix:** protect the gitignored server-generated paths (mirror `.gitignore`): drop `--delete-excluded`, use plain `--delete` with `--filter 'P /secrets/***' --filter 'P /ssh_config' --filter 'P *.local'` (or `--exclude secrets`); never rsync for read-only verbs. Add a post-rsync guard that the stash survived.

---

## MAJOR

**M1 — #20 force-single-natsconf signature broadened from the plan's conjunction into a flat alternation → false-green on session-drop / broker-down**
`test/simcluster/drills/20-forcesingle-natsconf.sh:42` — *(R3-MAJOR1, R4-M2, R2-MINOR4, R5-MINOR6 — 4 reviewers)*
Plan §5.2/§11 specified `bucket_create_failed.*(503|10008)`. Shipped as `bucket_create_failed|503|10008|…|no responders|timeout.*JetStream|JetStream.*unavailable`. `no responders` **cannot** match the real #20 (force-single leaves nats-server up with JS-no-quorum → 503/10008 JSON reply, never `no responders`) — it only appears when the broker is down or the subject is unsubscribed. Combined with the known-open "ctl session drops after force-single" + the best-effort `|| true` re-login at `:36`, a push that fails at the session/transport layer with `no responders`/`timeout` is scored "**#20 JS-503 rot reproduced**" though JetStream never entered the picture.
**Fix:** restore the conjunction `bucket_create_failed.*(503|10008)`; drop bare `no responders`/`503`/`timeout`. Move the positive controls to immediately before the push so the push's own transport state is covered.

**M2 — #12 ghost-voter refusal signatures include success-adjacent and over-broad tokens → false-green + defeats the flip-to-regression signal**
`test/simcluster/drills/12-ghost-voter.sh:28,31,34` — *(R2-MAJOR1, R3-MAJOR2, R1-MINOR5 — grounded against `cmd/tether/cluster_natsconf.go`)*
The `--to-standalone` guard `unrecognized raft role|cannot prove N=1|EXACTLY 1|voters need|refusing|already standalone` is unsafe: **`already standalone`** describes the *fixed* world — after the #20 survivor-de-cluster fix ships, `reconcile nats --to-standalone` on an already-standalone brk1 refuses with "already standalone" → still scored "#12 reproduced" → the "APPEARS FIXED — promote" signal never fires. **`refusing`** matches every refusal the command emits (`:135/:153/:156/:232`); **`cannot prove N=1`** also matches "ran on non-leader"/"partial status"; **`voters need`** is a dead token (real string is `voters (need EXACTLY 1)`). Node-remove guard adds `raft config`/`not a member`, which appear in **no** real `clusterdrain.go` refusal.
**Fix:** pin to captured product strings only — `unrecognized raft role.*cannot prove N=1` (anchored); drop `already standalone`, `refusing`, `voters need`, `raft config`, `not a member`.

**M3 — `agent-join` verb declares "onboarded + tether-agent running" unconditionally → the verb's own false-green**
`test/simcluster/simcluster:322-343` — *(R1-MAJOR2)*
Bind is `timeout 6 tether agent … || true` (fixed 6s, failure swallowed). The persistent unit is `Type=simple`, so `enable --now` returns 0 as soon as the process is exec'd, before it binds. `:343` prints `ok` regardless. Wrong `--pin`/still-electing/slow-bind → first connect killed at 6s, unit crash-loops on `Restart=on-failure`, and the verb still reports success. Drills partly mask via a downstream `node ls | grep ONLINE`, but `simcluster agent-join` standalone lies.
**Fix:** after `enable --now`, poll-until the agent shows `ONLINE` in the leader roster (bounded, `die` on timeout) before printing `ok`.

**M4 — `cmd_drill` teardown is dead code on a RED/interrupted drill (`set -e` aborts before `_rc=$?`/nuke) → throwaway instance leaks**
`test/simcluster/simcluster:437-444` — *(R1-MAJOR3, R5-MAJOR2, R6-MAJOR4 — reproduced by R5 & R6)*
Under `set -euo pipefail`, `env INSTANCE=… sh "$_script"` is a bare simple command; on non-zero exit the shell exits immediately — `_rc=$?`, the `SIM_KEEP` notice, and the `nuke` all never run. On exactly the common flake/RED path, the `drill-<name>` instance (privileged containers + volumes + bridge network + minted key stash) leaks silently and accumulates on the shared 88-core box, worsening the very contention that caused the flake.
**Fix:** `env … sh "$_script" || _rc=$?` (guard so cleanup always runs) **and** install `trap 'INSTANCE=$_inst … nuke' INT TERM EXIT` unless `SIM_KEEP=1`.

**M5 — Destructive drills have no throwaway-instance self-guard; isolation is convention-only → can destroy the persistent `sim` cluster**
`test/simcluster/drills/*.sh`, `drills/lib/setup-forcesingle.sh:7` — *(R5-MAJOR1)*
Drills never assert what instance they run on — they inherit `INSTANCE` from `cmd_drill`'s `env` injection and open with `"$SIM" nuke`. When `INSTANCE` is unset, both `simcluster` and `lib/docker.sh` default to **`sim`** (the persistent cluster). Any off-path invocation (running the script directly, a copied README line, or an `env`-plumbing regression) → `nuke` destroys the persistent cluster, then `force-single brk1 --dead brk2` kills persistent brk2.
**Fix (2 lines, structural):** at the top of each destructive drill + `setup-forcesingle.sh`: `case "${INSTANCE:-}" in drill-*) ;; *) die "refusing destructive drill on non-throwaway instance '${INSTANCE:-sim}'";; esac`.

**M6 — provisioner pre-chowns `/etc/tether` to `tether`, masking #22, and `doctor` asserts the workaround as "clean" → sim can no longer regression-detect #22**
`test/simcluster/image/provision-node.sh:61`; `doctor` at `simcluster:458-462` — *(R2-MAJOR2, R4-m5; grounded: `install.sh:483` bare `mkdir -p`, chown only LIB/LOG/RUN)*
On the real fleet `/etc/tether` is root-owned — that IS #22, and it's what makes the in-broker C3 reconciler's `os.CreateTemp("/etc/tether",…)` perm-deny. The overlay unconditionally corrects the one ownership fact #22 is about (and it is **not** one of the plan's three sanctioned overlay deltas, §2). Consequence: the sim runs green whether or not tether fixes #22, and `doctor` flags *absence* of the workaround as an error — an inverted tripwire. So #22 is attributed correctly but is now **unguarded** by the tool built to guard it.
**Fix:** leave `/etc/tether` root-owned and add a RED #22 drill (start broker in cluster mode, drive a membership change, assert the reconciler logs `permission denied` on `/etc/tether/.nats.conf.*` and topology stays DEGRADED) — signature-locked like #20/#21 so a future fix flips it; or apply the exact fix `install.sh` will ship (sourced from the reused installer, not a sim-only overlay). At minimum `doctor` must WARN "sim overlay applied," not report a product check.

**M7 — grow/init hand-render the full mesh via `reconcile nats --manual`; the in-broker C3 auto-reconciler and the `reconcile nats --all` harvest path are never load-bearing → topology-convergence regressions pass while prod breaks**
`test/simcluster/simcluster:96-108` (`_reconcile_clustered`), called per-node in `cmd_grow` and `cmd_init` — *(R2-MAJOR3; corroborated R4 note)*
Real-fleet convergence after a membership change is the in-broker reconciler rendering nats.conf itself (what #22 breaks, what the DEGRADED-lingering item is about). The sim re-renders every broker via explicit `--peer` triples and never depends on the broker's own reconciler; `reconcile nats --all` (the runbook's primary grow command + gotcha #3's `BuildMergedConf` harvest) is **never exercised anywhere**. A regression in `--all` harvesting cluster{}/route-mTLS would be invisible. *(The "auto-convergence not asserted / DEGRADED lingers" half overlaps the known-open; the NEW gap is zero coverage of the `--all` harvest path.)*
**Fix:** drive at least one grow via `reconcile nats --all`; add one drill/arm that lets the in-broker reconciler converge and asserts `topo_observed>=topo_desired` within a timeout — RED-guard it if it genuinely can't converge today (the `config_load_time` issue), so a future fix flips it.

**M8 — grow-to-3 drops the OQ-6 cheap hollow-voter / FK-panic parity assertion (not just the deferred InstallSnapshot arm) → structurally blind to the `32b28e9` "preserve joiner identity" bug class**
`test/simcluster/drills/10-grow-to-3.sh` (nothing after `:31` checks joiner DB integrity) — *(R4-M1; worse than the known-open, which defers only the InstallSnapshot arm)*
OQ-6 (plan §5.1) is explicit that if the InstallSnapshot arm defers, "plain-grow + the FK-panic/hollow-voter assertion **stay**." The shipped drill keeps neither. Its checks (3 VOTER / reachable / R=3 / meta) are all true even with a **hollow `cluster_nodes`** (joiner VOTER but missing peer-identity rows) → a reintroduced hollow-voter/FK-panic-on-migrated-replay regression passes GREEN with silently-corrupt roster/failover state.
**Fix (cheap, no resnapshot needed):** add row-count parity — `exec brk3 -- cluster status --json | jq '.nodes|length'` must equal the leader's — plus grep the joiner's `tether-broker` journal for `panic`/`FOREIGN KEY`/`unrecognized raft role`. The InstallSnapshot-forcing arm can stay deferred.

**M9 — grow-to-3 "follower-kill HA proof" is non-falsifiable and contradicts its own header/§5.1**
`test/simcluster/drills/10-grow-to-3.sh:42-48` — *(R3-MAJOR3)*
The header promises "a tier-B push that still works after a follower's nats is killed (quorum 2/3)." The code does **no post-kill transfer** — it asserts only `leader_id != null` and that `node ls` still lists agt1. Both hold regardless: tether raft runs on its own `:7400` `NetworkTransport` independent of nats-server, so `systemctl stop nats-server` does not remove the follower from raft (quorum stays 3/3, not 2/3), and `node ls` is served by brk1 over core NATS. The assertion can only fail if two nodes die — it cannot catch a data-plane HA regression. The `:44-46` comment justifying the deleted tier-A push ("even tier-A records to R=3 streams so it degrades") is also dubious (R=3 at 2/3 live still has JS quorum).
**Fix:** kill the whole follower **container** (removes its raft node) and assert a control-plane **write** (e.g. `expose`/session op) still commits at 2/3; or restore a retry-guarded tier-A push. Make the header/§5.1 wording match what the code asserts.

**M10 — the "nats-server pinned == install.sh" guarantee is never actually verified; three decoupled hardcoded copies drift silently**
`test/simcluster/remote.sh:14`, `simcluster:39-40`, `Dockerfile:33` (none read `scripts/install.sh:NATS_SERVER_VERSION`) — *(R6-MAJOR3)*
`cmd_build` logs "want 2.10.22 == install.sh pin" and dies on mismatch, but compares the baked binary to a **hardcoded literal**, not to install.sh. If install.sh bumps `NATS_SERVER_VERSION` (or its `TETHER_NATS_SERVER_VERSION` override is set), `remote.sh` still fetches 2.10.22, `build` still asserts 2.10.22 → GREEN, and the sim now runs a different nats-server than the fleet — silently defeating the tool's raison d'être.
**Fix:** derive the version from the rsync'd `vendor/install.sh` (`grep -oE 'NATS_SERVER_VERSION="[^"}]*'`) in both `remote.sh` and `cmd_build`; assert against that grep, not a literal.

**M11 — silent stale `tether` binary: `--build` does not rebuild the image, and `_bring_up_node` reuses existing containers → you test old code and draw false conclusions**
`test/simcluster/remote.sh:22,54` + `simcluster:45-51` — *(R6-MAJOR2)*
`remote.sh --build` rebuilds the binary into `vendor/` and rsyncs it, but only the `build` verb rebuilds the docker image — so `remote.sh --build up` boots from the stale `tether-sim:dev` layer. Even after `simcluster build`, `_bring_up_node` just starts an existing container on the old layer. `make build` defaults `VERSION=v0.0.0-simcluster`, so `tether version` can't distinguish builds, and `doctor` has no freshness check. Dev edits a `.go` file, re-runs the repro, sees "same" behavior, and concludes wrongly.
**Fix:** stamp git short-sha into the image (LABEL/env); `doctor` tripwire that running container `.Image` == current `tether-sim:dev` `.Id`; `build` warns persistent instances need `nuke`+`up`; consider `tether-sim:<sha>` tags.

**M12 — #21 drops the plan-mandated `--ack-alerts`; a `disk_pressure` alert (the drill's own condition) flips the push into an undocumented HARD-FAIL false-RED**
`test/simcluster/drills/21-smalldisk-tierb.sh:29` — *(R3-MAJOR4; grounded: `gateDestructive` runs before transfer, `disk_pressure` is a real leader-committed alert)*
Plan §5.3 specifies `--ack-alerts`. The 4g store deliberately sits near its usable ceiling — precisely what raises `disk_pressure`. If it fires, the push is blocked at the alert gate with a message that is **not** `10047` → `assert_bug` hits the "failed for an UNDOCUMENTED reason" branch → false-RED under the drill's own designed condition. Latent today only because the current threshold isn't tripped. (Secondary: the plan's `tier-A push` positive control was removed, so the drill no longer discriminates "only the 8 GiB OBJ_xfer bucket overshoots" from "store too small for JS at all.")
**Fix:** add `--ack-alerts` to the push so the signal stays on the JS storage-admission path, not the alert gate; consider restoring the tier-A positive control.

**M13 — the session PIN literal `040415` (per project memory, the live-fleet PIN and racknerd/optiplex host root password) is hardcoded into all drills + README and committed** *(severity conditional on repo visibility)*
`test/simcluster/drills/*.sh`, `README.md:37-38` (+ pre-existing `docs/broker-ops.md:283`) — *(R5-MAJOR3)*
The sim mints its own account/CA, so any PIN works — the real value is not needed. If the git remote is public, a real host root password now lives in history via `test/simcluster/`. The sim itself can't reach production (R5 confirmed: no fleet endpoints, own CA, no published ports), so the leaked value has low utility *against the sim* — the concern is purely that a real root credential is committed. Partly pre-existing (`broker-ops.md:283`), so simcluster propagates rather than originates.
**Fix:** decouple — `SIMPIN=${SIMPIN:-135790}` across drills + README; scrub `040415` from `broker-ops.md`. If the repo is public, treat the root password as compromised and rotate.

---

## MINOR

**m1 — `provision-node.sh` cluster-seam block is dead code and a latent YAML-corruption trap**
`test/simcluster/image/provision-node.sh:40-51` — *(R1-MINOR6, R2-MINOR5, R4-m7, R6-MINOR7 — 4 reviewers)* — `grep -q 'cluster:'` matches install.sh's commented `# cluster:` lines, so the block never runs; the real seam is written by `cmd_init`/`cmd_grow` (which include `nats_server_bin`, this block omits it). If anyone "fixes" the grep, it would append a **second top-level `broker:` key** (duplicate mapping key → yaml.v3 rejects → `serve` won't parse). **Fix:** delete the dead block, or anchor to an uncommented key (`grep -qE '^[[:space:]]+cluster:'`) and nest under the existing `broker:`.

**m2 — `lib/secrets.sh` header comment asserts the WRONG (pre-OQ-3) CN-only reasoning that the code and #24 refute**
`test/simcluster/lib/secrets.sh:5-7` — *(R2-MINOR6, R3-TRIVIAL7, R4-m4 — 3 reviewers)* — Header says "route mTLS verifies the CA chain, NOT a SAN hostname, so CN-only ed25519 leaves are accepted"; the code (`_mint_leaf:38`) correctly adds `subjectAltName`, and #24 documents that the nats route mesh needs the SAN. A maintainer trusting the header could "simplify" the SAN out and silently break the mesh (`num_routes=0`, JS meta never forms) with no drill catching it. **Fix:** rewrite the header to match the body (SANs mandatory).

**m3 — planned `flock` mutating-subcommand guard is absent**
`test/simcluster/simcluster` (whole), plan §3 — *(R1-MINOR7, R4-m6, R5-MINOR4, R6 — 4 reviewers)* — Plan promised `flock /run/lock/simcluster-<instance>.lock` on mutating verbs; `grep -rn flock` finds nothing. Two concurrent mutating verbs on one instance (accidental double `grow`, or `grow` racing a `status`-triggered nats restart) can interleave offline raft surgery. Low-probability for a single operator. **Fix:** implement it, or strike the claim from the plan.

**m4 — `pty-confirm.py` has no deadline after it answers → a post-confirm hang wedges `init`/`force-single` forever; and `os._exit(127)` is unreachable**
`test/simcluster/image/pty-confirm.py:53-54,63` + `simcluster:244,372-374` — *(R1-MINOR1, R5-MINOR5)* — The 60s deadline guards only `not answered`; if the irreversible `cluster init --from-existing` hangs *after* the confirm, the feeder blocks forever and `cmd_init`'s unbounded `_out=$(…)` capture wedges the whole tool (a real "20-min hang" class exists). Separately, `execvp` raises rather than returns, so `os._exit(127)` never runs → a bad `tether`/`python3` path surfaces as an uncaught traceback + full 60s stall. Not a false-green (can't report success), but a liveness gap on the one-way migration path. **Fix:** overall wall-clock deadline covering the post-answer phase (kill child, non-zero), or wrap the `dexec` in `timeout`.

**m5 — #20 drill drops the plan-mandated GREEN recovery arm**
`test/simcluster/drills/20-forcesingle-natsconf.sh` — *(R2-MINOR7)* — Plan §5.2 required driving the documented manual workaround (strip `cluster{}` via awk → `nats-server -t` → `.bak` → `mv jetstream` → restart) and asserting JS heals. The drill reproduces the rot but never validates the operator's escape hatch. Coverage gap. **Fix:** add the recovery arm as an `assert_ok` tail (also gives a natural clean teardown of the wedged JS).

**m6 — grow-to-3 reads leader-only fields from a hardcoded `brk1` → false-RED if leadership moved**
`test/simcluster/drills/10-grow-to-3.sh:27-31` — *(R3-MINOR6)* — `reachable`/`stream_actual`/`stream_target` are leader-only-populated (plan §4) but queried via `exec brk1`; the follower-kill section (`:39`) correctly resolves from `.leader_id`. If leadership moved off brk1 during grow/SIGHUP-reload, the follower view returns null/false → the "3 reachable / streams R=3" asserts false-RED. **Fix:** resolve the leader (like `:39`) before reading those fields.

**m7 — #12 GREEN control hard-pins ghost==VOTER while the assert_bug sigs claim to cover both shapes; the two halves disagree on the eject run**
`test/simcluster/drills/12-ghost-voter.sh:19-20` — *(R3-MINOR5; marginally worse than the known-open "broadened to cover both shapes")* — `:19` requires brk2 `phase==VOTER`; on the fully-ejected variant this false-REDs, while the broadened remove-sigs (`no such roster node`) would score a clean eject as "#12 reproduced." So the drill passes cleanly **only** in the ghost-retained shape. **Fix:** gate the whole drill on ghost-presence (skip-with-notice on eject), or split into two shape-specific drills, and narrow the eject-only tokens.

**m8 — #23 attribution is root-cause imprecise: the broker already sets `nats.MaxReconnects(-1)`, so its recommended "infinite-reconnect" fix is a partial no-op**
`internal/broker/authcallout.go:36` (product) vs gotcha #23 — *(R4-m3)* — The unit half is solid (`install.sh:734` `Restart=on-failure`), but the broker already reconnects forever, so the true clean-exit path (which loop/subscription returns on nats loss) is unpinned, and no drill asserts #23 (the sim uses SIGHUP reload, avoiding it). **Fix:** capture the survivor journal at the exact clean exit and cite the returning call site before #23 drives a product change; the durable fix is almost certainly the unit (`Restart=always`), not reconnect config. Sharpen the gotcha's mechanism sentence.

**m9 — no `nuke-all` reaper, no INT/TERM trap, and `network rm … || true` swallows in-use failures → orphaned instances accumulate with no signal**
`test/simcluster/simcluster` (`cmd_drill:431-445`, `cmd_down:417-425`) — *(R6-MAJOR4, partial)* — Plan §3 promised `nuke[-all]`; only per-instance `nuke` shipped, and it only reaps what the label filter sees (unlabeled `tcp_refused`/force-single probe containers are missed). A Ctrl-C'd drill leaks the whole instance (subnet included → `ensure_net` eventually fails). **Fix:** add a `nuke-all` sweeping `docker {ps -a,volume ls,network ls}` by the `sim-*`/`label=sim.instance` prefix; `warn` on `network rm` failure instead of swallowing. (The INT/TERM trap overlaps M4's fix.)

**m10 — `nats`/`nk` CLIs installed `@latest` and cached forever → non-reproducible, and `nk` mints the auth keys**
`test/simcluster/remote.sh:41-48` — *(R6-MINOR6)* — Both guarded by `[ ! -x … ]` (fetched once, never refreshed). The nats-server is pinned but the **key minter is not**; two operators (or an `rm vendor/nk`) get whatever `nk` is latest, and it produces the account/user nkeys the whole auth_callout chain depends on. **Fix:** pin `nats@vX`/`nk@vY` (or vendor them next to the pinned nats-server).

**m11 — supply-chain drift: nats-server tarball fetched over curl with no checksum; mutable base image + `:dev` tag**
`test/simcluster/remote.sh:33-36`, `Dockerfile:9` — *(R6-MINOR8)* — The product `install.sh:630` verifies SHA256SUMS; the sim does not. `FROM ubuntu:24.04` + the `tether-sim:dev` tag are mutable, so builds months apart drift in systemd/openssl. Acceptable for a dev tool but worth noting. **Fix:** reuse install.sh's SHA256 check for the download; optionally digest-pin the base image.

**m12 — README is not portable off the author's host**
`test/simcluster/remote.sh:12-14`, `README.md` — *(R6-MINOR5)* — Hardcoded `SERVER=weiland@192.168.1.150`/`REMOTE_DIR`/`SIM_VERSION` are env-overridable (`SIM_SERVER`/`SIM_REMOTE_DIR`) but the README documents none of them (nor `DOCKER_SUDO`, nor the WSL prereqs: Go toolchain, `rsync`, network for `go install …@latest`). **Fix:** add a short "Configuration" block.

**m13 — planned drift/preflight guardrails are stubs: anti-drift tripwire is presence-only, no `doctor` host preflight, egress check + `ssh_config` unimplemented**
`test/simcluster/simcluster` (`cmd_doctor:447-474`), plan §2/§3/§8 — *(R4-m6, R1 suggestion)* — The anti-drift tripwire (plan §2: grep the sim's laid-down units *against install.sh's `write_systemd_units` output*) is only `test -f` on the unit files — an install.sh **content** change the sim forks is not caught, the specific justification for reusing install.sh. `doctor` also lacks a host-side `command -v docker jq openssl date` preflight (a missing host `jq` degrades every `leader_node`/`status`/drill assertion into a confusing timeout). **Fix:** content-compare the units; add the host preflight.

**m14 — assorted shell-robustness trivia (fail-safe or cosmetic, no false-green)** — *(R1 MINOR2/3/4/8)*
- `simcluster:372-374` — force-single's `… | tail -6 || true` discards pty-confirm's rc, including its deliberate exit-3 "prompt-not-seen"; the signal is lost and only surfaces ~30s later as a generic `die`. Capture the rc.
- `simcluster:38-40` — `cmd_build` version guard: on a grep-miss, `pipefail`+`set -e` exits the assignment before the informative `die` runs (opaque abort). Add `|| true` on the substitution.
- `lib/docker.sh:75-78` — `tcp_refused` scores docker-run/daemon/image errors as "port refused" (benign today: the `--dry-run` dwell gate follows). Separate "connect refused" from "could not probe."
- `simcluster:419-420` — `down <non "-v">` prints "volumes removed" while keeping them (gate the message on the same `-v` test).

---

## Verdict

**Sound enough for external review after the BLOCKER and MAJORs are fixed — yes.** The harness's core design is correct: `assert_bug` treats exit-0 as "APPEARS FIXED → fail" and a non-matching stderr as a HARD FAIL, the strongest guards are real (`setup-forcesingle.sh`'s `cluster_size==2` + real tier-B baseline before the kill genuinely blocks the "meta never formed" false-green; #21's `10047` token is appropriately narrow), the six proven items hold, and every fix here is a drill/scaffolding edit with **no product change**, so remediation risk is low. The problems cluster in four bands: (1) one workflow-breaking, key-destroying data-loss bug on the non-drill persistent path (B1 — the drills structurally never exercised it, which is why it survived to here); (2) signature-breadth conditional false-greens that directly undercut the "never a false green" mandate (M1, M2) and a verb that self-reports success without a post-condition (M3); (3) proofs hollowed below their headers — grow-to-3 asserts routes/meta but is blind to the hollow-voter bug class it exists to catch, and its "follower-kill HA proof" is non-falsifiable (M8, M9); and (4) fidelity gaps that make the sim *easier than prod on the exact axes it was built to guard* — it pre-chowns `/etc/tether` (masking #22 and inverting the doctor tripwire), hand-renders every nats.conf so the in-broker auto-reconciler and `reconcile --all` are never load-bearing, and never verifies the nats-server pin against install.sh's SSOT (M6, M7, M10, M11). None of these is a today-false-green on the shipped grow-to-3 gate, but several are conditional false-greens or unguarded regressions, so they must be closed before this is trusted as a pre-release gate.

**The three tether findings are correctly attributed, with one caveat.** #22 (`/etc/tether` root-owned → in-broker reconciler perm-denies its atomic temp write) and #24 (route cert needs a SAN because `natscluster/config.go` renders `verify: true` for the nats route mesh while `internal/cluster/transport.go` skips hostname verify only for raft) are both grounded in the product and correct. #23 (broker clean-exits on nats loss + `Restart=on-failure`) is **plausible but root-cause-imprecise**: the systemd half is solid, but the broker already sets `nats.MaxReconnects(-1)`, so the finding's own "should infinite-reconnect" fix is a partial no-op and the true clean-exit call site is unpinned — capture that journal/call site before #23 drives any product change (durable fix is likely `Restart=always`, not reconnect config). Note also that although #22's *attribution* is right, the tool as shipped can no longer *regression-detect* #22 because M6's overlay masks it — fixing M6 is what makes the #22 attribution actionable rather than merely documented.