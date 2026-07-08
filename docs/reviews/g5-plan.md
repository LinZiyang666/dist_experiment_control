# G5 — rolling broker upgrade `tether cluster upgrade` (#13 / #14 / #19) — Implementation Plan

Date: 2026-07-07
Status: **FINALIZED (main process, Stage-A step 2).** Synthesized from a 6-drafter + 3-adversarial-critic Workflow, then adjudicated + locked by the main process. Post-1.0 leaf increment; NOT on the P0–P11 / D0–D9 line. Fifth batch of the grow/force-single/deploy roadmap (`docs/reviews/ha-grow-ops-remediation-roadmap.md` §G5). Target release: patch (additive; **ProtoVersion stays 2** — the `ClusterHealthResp` struct schema bumps, not the wire proto).

> **Context (verified in-tree 2026-07-07).** G1 (177e3d4), G2+G6 (6a6536b), G3 (92872f2) are landed on `main`; only G4/G5/G7 remain. G1's `Restart=always` is present (`scripts/install.sh:748`). Every load-bearing claim in §0.0 was checked at file:line (and the keystone privilege-wall was independently re-verified by the main process). The 5 open questions the synth surfaced are adjudicated in §0.7; they remain **open to external-review veto**.

---

## 0. Adjudications

### 0.0 Load-bearing claims verified in-tree (file:line)

- **Binary-write privilege wall (the keystone constraint).** `scripts/install.sh:225-236` `place_binary` does `mv "$tmp/tether" "$dest/tether"` + `chmod +x` — **no chown**, run as root, `dest=/usr/local/bin` ⇒ `/usr/local/bin/tether` is **root-owned**. Both `tether-broker.service` (`:731-732`) and the co-located agent run `User=tether`. `internal/agent/upgrade.go:238 installNewBinary` does `os.MkdirTemp(filepath.Dir(dst), …)` + `os.Rename` — **the atomic install needs DIRECTORY write on the binary's parent**. Therefore on a real broker host **neither the broker NOR the co-located agent (both `User=tether`) can self-install onto the shared root-owned `/usr/local/bin/tether`**. This voids every "broker self-installs" / "agent installs via node upgrade" design *for the install step*. (Main-process re-verified 2026-07-07.)
- **Restart mechanism surface.** `cmd/tether/serve.go:232` `b.Run(ctx)` returns → `!errors.Is(err, context.Canceled)` gives an error else `return nil` (`:235`). The signal ctx is `signal.NotifyContext(SIGINT,SIGTERM)` (`:221`). This is the exact trampoline point for a self-re-exec. `install.sh:748 Restart=always` + `:749 RestartSec=2` (G1) revives any self-exit but not an operator `systemctl stop/restart`. `Type=simple` (`:731`) ⇒ a PID-preserving `syscall.Exec` is transparent to systemd.
- **Three-axis wire hazard (NOT proto-only).** `internal/cluster/command.go:211 commandVersion=2`, doc `:202-204` "**DECOUPLED from proto.ProtoVersion**"; `decodeCommand:288` **POISONs** an entry on a Version mismatch (`:293-294`) OR an unknown op (`:296-297 !knownOps`) — advances applied_index, drops the op = silent per-replica FSM fork. **Existing precedent**: `HasPhaseFluidityOps()` (`:365-372`) + `internal/broker/clusterdrain.go:437 checkVotersSupportPhaseFluidityOps` — the leader refuses a membership op unless EVERY non-self voter advertises the capability in its live health report, "to avoid a mixed-version replica fork (review F5)." Schema floor: `internal/broker/clusterstatus.go:842 MAJ-11` documents an in-band schema/migration-version join floor as a **tracked v2.x enforcement gap** (join only WARNs on release skew today).
- **The proto gate is ineffective against a tarball.** `internal/broker/upgrade.go:91` compares `req.ProtoVersion` (the CALLER's binary proto) to `proto.ProtoVersion` — **not the tarball's**. A same-version ctl passes it 2==2 always; a proto/command-bumped tarball is never caught here. The only ground truth is the restarted broker's LIVE self-report.
- **Health self-report already carries versions.** `internal/broker/cluster_health.go:64 clusterHealthResponder` stamps `SchemaVersion` (`:70` = `proto.ClusterHealthSchemaVersion`, currently 2), `ReleaseVersion` (`:74`), `ProtoVer` (`:75`), `PhaseFluidityOps` (`:76`), `NodeID`, `AppliedIndex`, `LeaderID`. Struct at `internal/proto/alerts.go:12`; additions are additive omitempty and bump `ClusterHealthSchemaVersion` 2→3 — **not** `ProtoVersion`.
- **Transfer-leader + converge machinery all exist.** `internal/broker/clusterdrain.go:642 transferLeadershipOff` uses the TARGETED `node.LeadershipTransferToServer` (review m1). `internal/broker/clusterstatus.go:713 TransferLeaderTo` via `adminsock.OpClusterTransfer` (`:638`). `cmd/tether/cluster.go:593` transfer-leader `--wait` predicate = `rep.LeaderID == node`. `cmd/tether/cluster_wait.go:72 waitForConverge(pred, timeout, interval)` → timeout/cancel = exit 75 transient (`:106/:110`). `internal/adminsock/protocol.go:481 ClusterNodeStatus` already has `Phase:484 Role:485 AppliedLag:486 StreamActual:491 StreamTarget:492 Reachable:493 Inconsistent:495 ReleaseVersion:513` — the full converge-barrier field set.
- **N=2 physics.** `internal/cluster/node.go:72-73` `MultinodeElectionTimeout=1000ms`, `MultinodeLeaderLeaseTimeout=500ms`; `internal/cluster/read.go:29` "the instant it cannot reach a quorum within LeaderLeaseTimeout" the leader steps down. `ProjectQuorum(2,false).FaultTolerance==0` (`clusterdrain.go:36`). ⇒ transfer-leader is COSMETIC at N=2.
- **`-a` is taken.** `cmd/tether/node.go:122` `-a/--all` = "include OFFLINE / STALE nodes"; RELEASE column at `:97/:103`. A #19 broker-version view needs a distinct flag.
- **Two-phase admin-op precedent.** `internal/adminsock/protocol.go:88-89` `OpClusterForceSingleArm` / `OpClusterForceSingleCommit` — an existing arm/commit shape to mirror for a stage/commit broker-reload op. Single-active-op slot = `OpClusterOps` (`:74`); `newClusterBackupCmd` + `OpClusterBackup` (`:58`) exist.

### 0.1 DECISION — Binary staging is decomposed OUT as a privileged precondition; the RELOAD is privilege-free self-re-exec

**ADOPTED.** Split the per-host upgrade into two clearly-separated concerns:
1. **Binary staging (privileged, a PRECONDITION for v1):** replacing the on-disk shared `/usr/local/bin/tether` with the verified target binary. Because the binary is root-owned and both processes run `User=tether`, this **cannot** be done by the broker or the co-located agent as written. For G5 v1 it is an **explicit, documented privileged step** performed by the operator's install path (SSH + root `install.sh`, or the fleet's existing NOPASSWD-sudo staging per project memory), NOT by tether-over-NATS. G5 verifies (via sha256) and refuses to proceed if staging did not land the intended version.
2. **Reload (privilege-free, G5 OWNS this):** once the new binary is on disk, both co-located processes load it by **`syscall.Exec` self-re-exec** — which needs only *execute* permission on the binary (`User=tether` has it), **no directory write, no sudo, no systemctl**. This is the quorum-safe, transfer-leader-gated, resumable orchestration that is the real substance of #13/#14.

**Rationale.** The privilege wall is a hard, verified fact; pretending the broker can self-install is the "one command that secretly needs root" trap. Decomposing it makes G5 tractable AND honest (sim-mandate: expose the gap, do not paper over it). The reload via self-re-exec is fully privilege-free and PID-preserving, so G5 delivers the *dangerous* part (quorum-safe restart choreography) cleanly while the *privileged* part (writing a root-owned file) is surfaced as an explicit precondition. As a bonus this **eliminates #13's redundant-re-download by construction**: staging installs the binary once for both processes; each just re-execs (no per-process download at all).

**Rejected.** (a) *Broker self-downloads+installs onto `os.Executable()`*: fails EACCES on the root-owned dir — verified. (b) *Agent installs via `node upgrade`, broker only re-execs*: the co-located agent is ALSO `User=tether` and ALSO cannot write the root-owned dir — verified `installNewBinary` needs dir-write. (c) *Make G5 "truly one command" by chowning the bin dir or adding a root helper NOW*: a real deploy-tier change (new privilege boundary) that contradicts the roadmap's "G5 独立" claim and needs its own G1-style security review — **surfaced as OQ-1** (§0.7), not silently absorbed.

> **How to close the "one command" gap later** (OQ-1): a versioned-symlink layout (`/usr/local/bin/tether` → `/var/lib/tether/bin/tether-<ver>`, target dir tether-owned so `User=tether` can atomic-rename the symlink) OR a minimal root-owned staging helper. Either is a deploy-tier leaf; deferred to a follow-up so G5 ships the quorum-safe reload now.

### 0.2 DECISION — Broker-daemon reload = graceful-quiesce-then-`syscall.Exec` self-re-exec

**ADOPTED** (all three critiques converged here). Add `broker.RequestReExec()` that signals the run loop to unwind through its EXISTING ordered shutdown (raft/NATS/admin-socket teardown), then in `cmd/tether/serve.go` — **after `b.Run(ctx)` returns and the graceful teardown has completed (`:232`)** — if a re-exec was requested, `syscall.Exec(exePath, argv[0]=exePath, os.Environ())`. Same PID ⇒ systemd `Type=simple` sees no exit ⇒ structurally immune to #23. On `syscall.Exec` failure, `os.Exit(non-zero)` so `Restart=always` (G1, `install.sh:748`) revives with the new on-disk binary. A normal SIGTERM (`context.Canceled`) still `return nil`s (exit 0) unchanged.

**Rationale.** Privilege-free (execute-only); PID-preserving (no #23 exposure on the happy path); the ordered shutdown closes raft bolt.db + WAL + admin.sock BEFORE the image swap, so there is no abrupt mid-commit hazard. The oft-cited **flock/fd corruption objection is overstated**: Go opens bolt/SQLite/listener fds `O_CLOEXEC`, so `syscall.Exec` closes them and the new image reopens cleanly — and the graceful path closes them explicitly first anyway.

**Rejected.** (a) *Clean self-exit(0) + rely on `Restart=always`*: still leans on G1 being deployed fleet-wide and re-introduces a fragile exit-0 path; self-re-exec is strictly safer (immune on the happy path, `Restart=always` only as the exec-failure backstop). (b) *`systemctl restart tether-broker` via the co-located agent*: restarting a system unit needs root the `User=tether` agent lacks; also drops the `tether exec` channel that routes the command (gotcha §C). (c) *Abrupt agent-style `syscall.Exec` with no quiesce*: the broker holds raft leadership + single-writer WAL; reuse the ordered teardown first.

**Composition rule (adjudicated):** the reload primitive **REFUSES if it is the current raft leader** (`Code=is_leader`, symmetric with the F5/set-raft-addr self-only rule); the *orchestrator* owns transfer-leader-first. This keeps the primitive minimal and guarantees a leader is never abruptly re-exec'd.

### 0.3 DECISION — Roll state machine: ctl-driven, stateless progress, single-active-op LOCK, canary-first

**ADOPTED** (synthesis of the state-machine + adversary drafts):
- **Ctl-driven, external to the brokers.** The orchestrator survives every broker restart including the leader's. Rejected leader-driven (dies the instant it restarts itself; roll state unavailable across the very restart it induces).
- **Stateless PROGRESS.** Per-host progress is recomputed each run from LIVE version state (broker VER + agent RELEASE + role/lag/streams); resume = re-run the identical command with the same `--to-version`/`--url`/`--sha256`. NO per-host raft progress ledger (it cannot commit during the leader's own restart / the N=2 blip).
- **Single-active-op LOCK (not a ledger).** Acquire the EXISTING `OpClusterOps` single-active-op slot once at roll start for **mutual exclusion** against a concurrent grow/retire/force-single and a second operator. This is a lock, released at the end; it is never written mid-restart.
- **Canary-first.** Upgrade exactly ONE non-leader broker, wait for it to rejoin as a healthy voter, read its LIVE self-reported {proto, commandVersion, op-set, schema-floor}, and only then proceed. Blast radius = one broker; skew becomes a recoverable HALT.
- **Mandatory verified pre-roll `cluster backup`** (reuse `OpClusterBackup`) as a HARD precondition — the only recovery path when a canary's new binary runs a one-way forward migration (MAJ-11; reinstalling the old binary does NOT roll back).

**Rejected.** A raft-journaled progress ledger (bootstrap hole at the leader's own restart / N=2); pure statelessness WITHOUT the lock (leaves concurrent-roll + racing-membership-op unguarded).

### 0.4 DECISION — Transfer-leader timing (#14): leader-last, transfer-only-off-the-leader, targeted, gate-on-success; N=2 is honest-cosmetic

**ADOPTED** (raft-verified):
- **Followers-first, leader-last.** Transfer leadership ONLY when the host about to be restarted IS the current leader; RE-READ `LeaderID` live immediately before each restart to catch mid-roll drift (a static plan could restart a freshly-elected leader with no hand-off).
- **Targeted transfer only.** Hand off via `transferLeadershipOff`/`TransferLeaderTo` (the TARGETED `LeadershipTransferToServer`) to an ALREADY-UPGRADED, caught-up (`AppliedLag==0`), reachable STAYING voter; wait `LeaderID==target`; **gate the leader restart on transfer SUCCESS** (on failure: HALT, do not restart the leader). Never the untargeted `node.TransferLeadership()`.
- **Post-restart convergence BARRIER before touching the next voter:** `Phase==VOTER && Role∈{voter,leader} && AppliedLag==0 && Reachable && StreamActual==StreamTarget && ReleaseVersion==target && !Inconsistent`. VER-alone is NEVER "done" (a half-restarted node reports new VER with LAG>0). This keeps live voters ≥ quorum across the WHOLE roll, one node at a time.
- **N=2 honesty (verified).** Transfer-leader does NOT avoid the write-fence at N=2: the lone survivor demotes within the 500ms lease and cannot re-elect alone (`read.go:29`, `node.go:72-73`), so writes fence `not_leader` for the whole restart duration regardless of who held leadership. Require an explicit `--ack-writefence` (§0.7 OQ-4), mandate production N≥3 / N=2-transitional in docs, and **HALT-not-de-cluster** (R3) if the survivor is not caught-up.

**Rejected.** "Transfer before EVERY restart" (needless elections for followers; useless at N=2); "transfer-leader makes N=2 zero-downtime" (false, would mislead operators).

### 0.5 DECISION — Version observability (#19): distinct `--brokers` flag, self-declared `ColocatedAgentNID`, one whole-host criterion

**ADOPTED**:
- **Flag:** a NEW `node ls --brokers` (opt-in; NOT `-a`, which is include-OFFLINE — verified `node.go:122`). Default `node ls` / `--json` stays byte-identical. Broadcasts `probeClusterHealth`, joins client-side.
- **Correlation:** broker self-declares its co-located agent nid via `broker.yaml broker.cluster.colocated_agent_nid` (+ `--colocated-agent-nid` flag) → additive `ClusterHealthResp.ColocatedAgentNID` (bump `ClusterHealthSchemaVersion` 2→3). Absent (pre-G5 broker) ⇒ fall back to the `node_id==nid` convention labelled `(assumed)`, never a silent mispair.
- **Whole-host criterion (the single trusted #19 judgement, also the roll's skip predicate):** `broker_ver==target && agent_ver==target && caught-up voter && streams-at-target && !proto/command/schema-skew`. Target from an explicit `--to-version` (NOT the tarball `--sha256`, which is the download-integrity anchor, not a version anchor).
- **Pure core:** `correlateBrokerVersions(nodes, health, target)` in a new `cmd/tether/node_versions.go`, consumed by both `node ls --brokers` and the orchestrator's skip decision. Render version-skew and **proto/command/schema-skew distinctly** (the latter must abort the roll).
- **SECURITY:** `ColocatedAgentNID` drives DISPLAY/skew only. The orchestrator MUST target each agent's re-exec by the host it is currently acting on, NEVER fan out by advertised nid (a wrong/malicious value would re-exec the WRONG host's agent).

### 0.6 DECISION — Mixed-version / proto policy: THREE-axis gate; ProtoVersion NOT bumped; G5 adds NO raft op

**ADOPTED** (the single most important safety finding — 5 of 6 drafts were unsafe here):
- **Rolling-safe requires proto UNCHANGED AND commandVersion UNCHANGED AND op-set-superset AND schema-floor-compatible** — not proto alone. Add `CommandVer` + `SchemaFloor` (+ reuse `PhaseFluidityOps`/op-set capability) to `ClusterHealthResp` (additive omitempty, bump `ClusterHealthSchemaVersion`), stamped by `clusterHealthResponder`; export a `cluster.CommandVersion()` accessor (currently an unexported const). REFUSE the roll on any skew, **fail-closed** when a broker omits a field (pre-G5 broker → unknown axis → refuse; that first transition is a documented flag-day).
- **Detection = canary live-read (§0.3), NOT the `upgrade.go:91` caller-proto pre-flight** (proven ineffective). Reuse the F5 capability-advertisement STYLE (`checkVotersSupportPhaseFluidityOps`) rather than a parallel mechanism.
- **G5 itself bumps NEITHER `ProtoVersion` NOR `commandVersion`, and adds NO new raft OpType** — otherwise its own first roll would poison un-upgraded peers (self-defeating). G5's new ops are ADMIN-SOCKET / additive-subject only; the lock reuses `OpClusterOps`.
- **A genuine proto/command/schema bump is a flag-day reinstall (architecture J.3 场景二 / cluster-runbook §schema), never a roll.**

**Rejected.** Proto-only gating (silent-FSM-fork footgun); trusting `upgrade.go:91` as the reinstall guard (checks the wrong proto); bumping `ProtoVersion` for the new health fields (unnecessary; would itself force a reinstall).

### 0.7 Main-process adjudication of the 5 open questions (open to external-review veto)

| OQ | Decision (ADOPTED) | Why | Deferred / fast-follow |
|---|---|---|---|
| **OQ-1 binary staging** | **(A)** staging = explicit privileged PRECONDITION for v1; G5 owns only the privilege-free reload/transfer/observability. | Privilege wall is real+verified; absorbing a deploy-tier symlink/root-helper balloons scope + needs G1-style security review; roadmap says "G5 独立"; sim-mandate = expose the gap. Eliminates #13 redundant-redownload by construction. | The versioned-symlink "one-command" staging helper is its own follow-up deploy leaf (candidate G8). |
| **OQ-2 multi-axis floor** | **(a)** canary live-read + MANDATORY verified pre-roll backup + `--to-version` as the v1 floor. | Cannot detect migration intent before install; canary blast-radius=1 + backup makes a poison-before-detect canary a recoverable stop. | **(b)** a signed release manifest `{proto,commandVersion,schemaFloor}` for blast-radius-0 pre-flight REFUSE — strongly-recommended fast-follow. Release-engineering discipline note: same-proto rolling releases MUST NOT bump commandVersion / add ungated ops / add migrations; new ops must be F5-capability-gated. |
| **OQ-3 orchestrator transport** | **(ii)** add ONE new **additive** over-NATS remote-trigger subject (`SubjCtrlBy(actor,"cluster-upgrade.req")` — hyphen-leaf keeps §13.8 green, **ProtoVersion stays 2**) so the orchestrator runs external to all brokers and re-resolves the leader after each restart. This is the ONE genuinely-new wire addition in G5. | Per-host-manual (iii) defeats #13's "one orchestrated command"; tether-exec (i) self-severs when it restarts the broker it routes through. **AUTH (main-process adjudication — the plan first said "owner-scoped", but a cluster-wide op has no per-session owner; resolved here per CLAUDE.md §2 doc-first): the request is ACCOUNT-SEED-SIGNED** — the orchestrator signs `CanonicalUpgradeReqBytes` with the cluster account seed (the operator's root authority, same trust anchor as the signed roster/seeds), and each broker VERIFIES against its pinned account_pub before acting. Two layers: NATS actor-scoped ACL (publish only on own `ctrl.by.<self>.*`) + the account signature (only the account-seed holder is honored). Replay-bounded by `IssuedAt`. An old broker without the responder → `ErrNoResponders` → the roll reports it cannot orchestrate that host (no silent skip). The orchestrator needs the account seed (a broker `secrets_dir` / operator-exported) — documented; a pure laptop without it cannot drive an upgrade (correct: it lacks cluster authority). | — (this is the keystone that makes `cluster upgrade` actually orchestrate; the account-signed auth is the security crux — implement the verify path with care). |
| **OQ-4 N=2 policy** | **(a)** hard-REFUSE at N=2 without `--ack-writefence`; still transfer-leader-first for determinism (NOT advertised write-safe); mandate production N≥3 in docs; HALT-not-de-cluster (R3) if the survivor is not caught-up. | Don't strand the current racknerd-class single/dual deployments (outright refuse would); the explicit ack + honest wording is the root-cause fix #14 demands. Pinned by a test asserting the N=2 stream DOES fence. | — |
| **OQ-5 backup scope** | **(a)** mandatory verified pre-roll backup for EVERY roll. | Cannot detect migration intent pre-install; one-way migrations + the canary poison-before-detect window mean a missing backup can strand a half-migrated cluster with no forward or backward path; a verified backup is cheap insurance. | — |

> **Scope note for external review.** G5's real scope is larger than the roadmap sketch implied: the verified privilege wall forces the staging/reload split (OQ-1), and honest orchestration requires ONE new additive owner-scoped NATS subject (OQ-3). Both are called out here rather than buried. Everything remains additive (ProtoVersion 2, no new raft op, no commandVersion bump).

---

## 1. Work items grouped by gotcha

### #13 — `tether cluster upgrade`: reach + reload the broker daemon + re-exec the co-located agent, quorum-safe, idempotent, resumable

- **W1 — Broker re-exec trampoline (§0.2).** Add `broker.RequestReExec()` (stores intent + unwinds the run loop through the existing ordered shutdown); in `cmd/tether/serve.go` after `b.Run(ctx)` returns (`:232`), if requested, `syscall.Exec(exePath, argv, os.Environ())`; on exec error `os.Exit(70)` (→ `Restart=always` backstop). SIGTERM path untouched (`return nil`). *Files:* `internal/broker/broker.go` (Run + ordered shutdown; add `reExec` field + sentinel), `cmd/tether/serve.go:232`. *Risk: high (keystone).*
- **W2 — Local reload admin-op (arm/commit, mirrors force-single).** `OpBrokerUpgradeArm` + `OpBrokerUpgradeCommit` (or a single `OpBrokerUpgradeReload`) on the TARGET broker's local admin socket. Gate order: (1) idempotency skip if live `ReleaseVersion == --to-version`; (2) REFUSE if this broker is the raft leader (`Code=is_leader`); (3) verify the on-disk binary sha256 matches the staged target — refuse if stale; (4) reply OK FIRST, then `RequestReExec()` after a short reply-drain. *Files:* `internal/adminsock/protocol.go:29-136` (new op consts + routing), new `internal/broker/broker_upgrade.go`. *Risk: med-high (security-sensitive gating).*
- **W2b — Remote-trigger seam (OQ-3 (ii)).** A NEW additive, owner-scoped over-NATS subject (derived from `SubjectVersionToken`, ProtoVersion 2) that reaches each broker's W2 reload-op + the existing transfer-leader so the ctl orchestrator drives every host (incl. re-resolving the new leader after each restart) without a broker admin socket. Old broker w/o responder → `ErrNoResponders` → orchestrator reports "cannot orchestrate this host" (no silent skip). ACL: owner-scoped, fail-closed, mirrors `node upgrade` owner-only. *Files:* `internal/proto/subjects.go` (new subject), `internal/auth/permissions.go` (owner carve-out), broker responder wiring. *Risk: high (new mutating wire surface — security-critical).*
- **W3 — Co-located agent skip-re-exec.** Because staging installs the shared binary once, the co-located agent needs only to re-exec (verify on-disk `tether version`/sha == target, then `syscall.Exec`). Extend `handleUpgradeForwarded` with a `ReExecOnly` branch (additive `UpgradeForwardedReq` field, no ProtoVersion bump) that skips `fetchURL`/`installNewBinary`. *Files:* `internal/agent/upgrade.go:56`, `internal/proto/messages.go`. *Risk: med (must verify on-disk version before exec).*
- **W4 — Ctl orchestrator `newClusterUpgradeCmd`.** New `cmd/tether/cluster_upgrade.go` wired into `newClusterCmd`'s "online" group (`cluster.go:49`). Flags: `--all` | explicit `<node-id>…`; `--url`+`--sha256` (broker-allowlist-gated, reuse node-upgrade semantics); `--to-version`; `--dry-run`; `--canary` (default 1); `--wait-timeout`; `--ack-writefence` (N=2); `--notify-webhook`; `registerYesRejector`. Per-host loop: re-read live status → transfer-if-leader (§0.4) → verify staging → arm/commit reload (W2/W2b) → converge barrier (W6) → per-host notify → canary halt. Stateless resume + single-active-op lock (§0.3). *Risk: high.*
- **W5 — Pure ordered-plan differ.** `internal/clusterupgrade/plan.go` (clusterspec-style, table-testable, no IO): input = live nodes (id, role, VER, proto/cmd/schema, phase) + agent releases + target → ordered `[]Step` {SKIP / TRANSFER / UPGRADE}, followers-first, leader-last, `sort.SliceStable`; N=2 warn flag; REFUSED reasons (no caught-up staying voter; proto/cmd/schema skew; pre-G5 broker missing axes). *Risk: low.*
- **W6 — Converge-barrier predicate.** Add an upgrade predicate to `waitForConverge` (`cluster_wait.go:72`): `Phase==VOTER && voter/leader && AppliedLag==0 && Reachable && StreamActual==StreamTarget && ReleaseVersion==target && !Inconsistent`; sustained over a short dwell. Timeout → exit 75 transient, HALT without touching the next host. *Risk: med (timeout budget — a snapshot-catching-up voter can exceed the 2-min default; document/make adaptive).*
- **W7 — Pre-roll guards.** Refuse if a membership op is active (`OpClusterOps` slot); refuse if the cluster is not currently write-healthy; mandatory verified `cluster backup` (reuse); N=2 → `--ack-writefence` + N≥3 recommendation. *Risk: med.*
- **W8 — ctl-side notification (#13 chatroom).** Per-milestone POST to `--notify-webhook` (roll start / per-host done / roll complete / roll HALTED) reusing the alert-webhook JSON shape. **Ctl-side, NEVER a raft-committed `OpClusterAlertRaise`** — the HALT message must fire even when the halting host is the one being torn down. *Risk: low.*

### #14 — quorum-safe leadership timing during the roll

- **W9 — Transfer-leader-first wiring + N=2 honesty (§0.4).** Reuse `TransferLeaderTo`/`transferLeadershipOff` (targeted) + the `LeaderID==target` wait predicate; gate the leader restart on transfer success; leader-last ordering in W5; live leader re-read before each restart; N=2 `--ack-writefence` + docs. *Files:* `cmd/tether/cluster_upgrade.go`, reuse `clusterdrain.go:642`, `clusterstatus.go:713`. *Risk: med.*
- **W10 — Docs: N≥3 mandate.** Rewrite `docs/cluster-runbook.md §6` to lead with `cluster upgrade` (manual sequence kept as fallback); add the exact production wording (N=3 zero-write-interruption / N=2 transitional inherent ~restart-duration fence / N=1 no-quorum-to-lose) to `docs/cluster.md`; update `docs/broker-ops.md §8.4/8.5`. Document that upgrading a broker **blips ITS homed data plane** (auto-recovers via failover; proactive rebalance is G7) — do NOT claim zero data-plane disruption. *Risk: low (N=2 wording is load-bearing).*

### #19 — dual-version observability + one whole-host criterion (§0.5, §0.6)

- **W11 — `ClusterHealthResp` additive fields.** Add `ColocatedAgentNID` + `CommandVer` + `SchemaFloor` (all omitempty), bump `ClusterHealthSchemaVersion` 2→3; stamp in `clusterHealthResponder` (`cluster_health.go:64`). Export `cluster.CommandVersion()` + a schema-floor accessor. Proto additive-guard test (ProtoVersion stays 2; old-broker-omits decodes clean). *Files:* `internal/proto/alerts.go`, `internal/broker/cluster_health.go`, `internal/broker/broker.go` (Config), `internal/cluster/command.go`, `internal/serveconf/serveconf.go` + `cmd/tether/serve.go`. *Risk: low.*
- **W12 — `node ls --brokers` + `correlateBrokerVersions`.** New `cmd/tether/node_versions.go` pure core + the opt-in flag on `newNodeLsCmd` (`node.go:36`); render BROKER_VER + SKEW + PROTO/CMD/SCHEMA-SKEW columns; JSON via a client-side merged type (do NOT touch `proto.NodeListEntry`), bump `nodeLsJSON.SchemaVersion` only under `--brokers`. Single-mode / no responder ⇒ byte-identical to plain `node ls`. *Risk: med.*

---

## 2. Invariants to protect

1. **ProtoVersion SSOT (=2) is NOT bumped.** All wire additions are additive omitempty and bump `ClusterHealthSchemaVersion` (the struct's own SSOT), not `internal/proto.ProtoVersion`. `proto_invariants_test.go` unknown-field-ignore stays green.
2. **G5 adds NO new raft OpType and does NOT bump `commandVersion`.** New ops are admin-socket / additive-subject only; the mutual-exclusion lock reuses `OpClusterOps`.
3. **Three-axis wire-compat gate** (proto + commandVersion + op-set + schema-floor), self-reported and canary-verified, fail-closed on any missing/mismatched axis; reuse the F5 capability-advertisement precedent.
4. **R3 — never silently de-cluster.** On any quorum-unsafe condition the roll HALTS; it never de-clusters/force-singles to "escape."
5. **No-restart-into-a-bad-binary.** sha256 verified BEFORE the on-disk swap and re-verified before any re-exec; a bad/stale artifact aborts the host BEFORE any transfer-leader or restart.
6. **Quorum-safety across the WHOLE roll.** One node at a time; the post-restart convergence barrier must pass before touching the next voter; live voters never drop below quorum.
7. **Targeted transfer only,** to an already-upgraded caught-up staying voter; the leader restart is gated on transfer success; never the untargeted variant.
8. **Voter set held INVARIANT.** A restart is not an `AddVoter`/`RemoveVoter` — the G5/G4 boundary.
9. **Recovery = restore, not reinstall.** Mandatory verified pre-roll backup; forward migrations are one-way (MAJ-11).
10. **`ColocatedAgentNID` drives display/skew only,** never a re-exec mutation; the orchestrator targets by the host it acts on.
11. **The broker reload must NOT touch `nats-server.service`** (separate unit; the JS-mesh peer stays up so the restarting broker just reconnects).
12. **Notification is ctl-side,** so a HALT fires even when the halting host is the one being torn down.
13. **The N=2 write-fence is inherent (F=0):** transfer-leader is cosmetic at N=2 and MUST NOT be advertised as write-safe; zero-write-interruption is N≥3-only (pinned by a test asserting N=2 DOES fence).
14. **The remote-trigger subject is owner-scoped + fail-closed;** an old broker without the responder is reported, never silently skipped.

---

## 3. Test surface

### 3.1 Hermetic Go (adversarial, table-driven — `make test`)

- **Plan differ** (`internal/clusterupgrade/plan_test.go`): followers-first/leader-last ordering; leader is the ONLY not-at-target host (transfer to an already-upgraded survivor, or REFUSE if none); all-at-target → empty; broker-VER==target but agent-RELEASE stale → NOT skipped (#19 half-upgrade); learner/CATCHING_UP/INCONSISTENT present; N=1 (no transfer); N=2 warn flag; absent/duplicate host-id → error.
- **Multi-axis gate / skew** (`cmd/tether/node_versions_test.go`, pure): version-skew vs proto-skew vs commandVersion-skew vs schema-floor-skew rendered distinctly; whole-host verdict true ONLY when all==target && no skew; pre-G5 broker (missing axes) → fail-closed refuse; `(assumed)` correlation labelled; two brokers advertising the same `ColocatedAgentNID` → ambiguous, not last-wins.
- **decodeCommand poison proof** (`internal/cluster/command_test.go` extension): a vN+1-Version or unknown-op entry POISONs — the machine-checked reason the gate must exist; assert `cluster upgrade` REFUSES on the corresponding health skew.
- **Broker re-exec durability** (extend the restart/durability harness): commit an op, trigger self-re-exec into a version-stamped test binary, assert the op is still readable and raft.db/tether.db reopen cleanly; SIGTERM path still exits 0 (no exec).
- **Leader-refuse:** `OpBrokerUpgrade*` on the leader → `is_leader`, no reload.
- **Remote-trigger ACL:** a non-owner identity on the new subject → refused; an old broker (no responder) → `ErrNoResponders` surfaced, not silently skipped.
- **Idempotency/resume:** skip when live VER==`--to-version`; re-run after a partial roll skips already-upgraded hosts; on-disk sha stale → refuse.
- **Transfer-failure gating:** stub `LeadershipTransferToServer` to error → orchestrator does NOT restart the leader, HALTs, resumable.
- **Converge barrier:** `Phase==VOTER` but `AppliedLag>0` → keep waiting; `StreamActual<StreamTarget` → keep waiting; VER==new but LAG>0 → keep waiting; deadline → exit 75, do NOT proceed.
- **N=2 honesty:** on 2 voters transfer-leaders to the survivor then upgrades the FOLLOWER; the leader is never the restarting node; a companion case asserts the N=2 write stream DOES fence; `--ack-writefence` required.
- **Malicious inputs:** URL not in allowlist → abort whole roll fail-fast; bad `--sha256`; non-broker node-id → clear error.
- **Concurrency/leak:** drive the orchestrator (stubbed status seam) under `-race` + the repo NumGoroutine/fd leak gate.
- **Proto additive-guard:** `ClusterHealthResp` with the new fields round-trips, `ProtoVersion` stays 2, old reply omitting them decodes to zero.

### 3.2 e2e matrix (`test/e2e/all_phases_test.go`, hermetic, embedded nats-server)

- In-proc N=3 roll via the D7 harness with real install/restart STUBBED (routed-JS flake stays out of the serial matrix): planner ordering + transfer-before-restart + idempotent-resume + mid-roll-HALT + skew-refusal. A background writer asserts NOT A SINGLE write observes `not_leader` at N=3; a companion N=2 case asserts the stream DOES fence. Map: ordering→#14, resume→#13, skew→#19/#13.

### 3.3 NEW sim deploy-tier drill — `test/simcluster/drills/30-rolling-upgrade.sh` (real N=3, run via `remote.sh`, serial)

Real systemd `Type=simple` units + real independent nats-servers + real route mTLS + real clustered JS. Per-assertion → gotcha map:

| Assertion | Gotcha |
|---|---|
| Every host that is leader at its turn is transfer-leader'd FIRST; raft `LeaderID` moves off H before H restarts | #14 |
| A continuous tier-A writer/expose probe returns ZERO `not_leader` across the whole N=3 roll (contrast: N=2 arm asserts it DOES fence) | #14 |
| PID-preserving self-re-exec: `systemctl show tether-broker -p MainPID` unchanged across each broker's reload | #13 |
| `nats-server.service` MainPID unchanged (reload does not touch nats-server) | #13 / invariant 11 |
| After the roll, all three brokers AND their co-located agents report `--to-version` via `node ls --brokers` (no skew) | #13 + #19 |
| Mid-roll: `node ls --brokers` shows SKEW on a host whose broker restarted but agent not yet re-exec'd | #19 |
| Idempotent re-run + SIGKILL-orchestrator-mid-roll → resume converges all 3 | #13 |
| Synthetic commandVersion/proto-skewed target → roll HALTS after the canary, hosts 2/3 untouched | #13 / §0.6 |

Signature-guarded RED (no `cluster upgrade` verb today) → flips to普通 GREEN when the mechanism lands. The binary-staging privileged precondition (§0.1) is performed by the drill's provision path (operator role), NOT papered over by tether — and is asserted to be a distinct privileged step (sim-mandate honesty).

---

## 4. Scope boundary

**In-scope (G5):**
- The ctl-driven rolling orchestration `tether cluster upgrade [--all|<node>…]`: pure plan differ, per-host loop, stateless resume + single-active-op lock, dry-run, pre-roll guards + mandatory backup, ctl-side webhook notify.
- The privilege-free broker-daemon **reload** primitive (graceful-quiesce self-re-exec + serve.go trampoline) and the co-located agent skip-re-exec.
- The ONE new additive owner-scoped remote-trigger NATS subject (OQ-3) so the orchestrator drives every host over NATS.
- The transfer-leader-first / converge-barrier quorum-safety (#14) + the honest N=2/N≥3 doctrine.
- The three-axis wire-compat gate + canary detection + `ColocatedAgentNID`/`CommandVer`/`SchemaFloor` self-report additions (ProtoVersion NOT bumped).
- `#19` `node ls --brokers` dual-version display + the one whole-host criterion consumed as the idempotency SSOT.
- Docs (runbook §6 rewrite, N≥3 mandate, staging-precondition note) + the N=3 sim drill.

**Deferred / NOT in G5:**
- **Binary STAGING as a "one command" privileged actor** (versioned-symlink / root helper / NOPASSWD sudo) — a deploy-tier follow-up leaf (OQ-1).
- **A signed release manifest** (`{proto, commandVersion, schemaFloor}`) for blast-radius-0 pre-flight refusal — recommended fast-follow (OQ-2 (b)).
- **Proactive proxy rebalance after a broker restart (#18), JS-down alerting (#20③), `--remote` for `--homes`/seeds (#16)** — G7.
- **grow orchestration `cluster add` (#3/#4/#5/#7/#8, §B)** — G4 (NOT absorbed; G5 holds the voter set invariant).
- **`nats-server` binary upgrade** — OUT (separate unit lifecycle); G5 must not restart it. Mixed nats-server-version route mesh is a doc-noted untested axis.
- **The `node upgrade` (agent-only) internals** — reused unmodified (extended additively for skip-re-exec).

**The G5 / G4 / node-upgrade line:** `node upgrade` = the fleet-wide AGENT tool (in-process `syscall.Exec`, cannot reach a sibling systemd broker unit). `cluster upgrade` = the whole-broker-host tool that reaches the broker daemon AND re-execs its co-located agent, with the **voter set held INVARIANT** (a restart is never an `AddVoter`/`RemoveVoter` — that is G4 `cluster add`). On any quorum-unsafe condition G5 **HALTS**, never de-clusters (R3).
