# G1 — deploy-tier hardening (#22 / #6 / #23 / #24) — Implementation Plan

Date: 2026-07-05
Status: **FINALIZED (main process) — ready for Stage-B implementation.** First batch of the grow/force-single/
deploy remediation roadmap (`docs/reviews/ha-grow-ops-remediation-roadmap.md`). Post-1.0 leaf increment; NOT on
the P0–P11 or D0–D9 line. Target release: patch (additive; no wire, no default-behavior flip beyond a systemd
directive + a config-path relocation gated by migration).

> **How this plan was produced (Stage-A per CLAUDE.md §3).** A 4-drafter + 4-critic + 1-synth adversarial
> Workflow (all agents Opus 4.8, `model` omitted) drafted candidates from distinct lenses (install-packaging /
> systemd-lifecycle / pki-cert / integration-minimal), adversarially cross-reviewed (scope-risk /
> compat-migration / invariants-correctness / sim-fidelity), and synthesized one candidate. The
> `systemd-lifecycle` drafter died on a mid-response API error; the main process **re-ran it standalone** (Opus
> 4.8) to restore the #23 depth. The main process then **verified every load-bearing claim in-tree** (§0),
> resolved the 9 synthesis DECISIONs, and put **DECISION 1 (the security-boundary choice) to the user**, who
> chose **Option B**. This document is the finalization.

---

## 0. Finalizer notes — load-bearing claims verified in-tree (file:line)

- **#22 root cause**: `scripts/install.sh:484` `mkdir -p …'$ETC_DIR'…` (no owner) + `:491`
  `install -d -o tether -g tether -m 0750 "$LIB_DIR" "$LOG_DIR"` (ETC not chowned) ⇒ `/etc/tether` stays
  root:root. Victim: `internal/natsconf/takeover.go:185` (`.bak` write) + `:190` `os.CreateTemp(dir, …)` with
  `dir=filepath.Dir(confPath)=/etc/tether`, then `:210` `os.Rename(tmp, confPath)` — **the atomic rename needs
  DIRECTORY write**, so a `User=tether` reconciler perm-denies. Confirmed the reconciler shares this exact path
  (`internal/broker/clusterwrite.go:274` gates topology reporting on `NatsConfPath != ""`).
- **#22 privesc (DECISION 1)**: `caddy.service` (`install.sh:743-757`) has **no `User=` ⇒ runs as root** and
  `ExecStart` reads `$etc/Caddyfile`; `install.sh:460` **forces `--domain` for broker** ⇒ real broker hosts
  DO run root-caddy over `/etc/tether/Caddyfile`. So chowning `/etc/tether` to tether would give the
  unprivileged `tether` user rename-rights over a root-loaded Caddyfile = a real (not theoretical) tether→root
  local privesc. **User chose Option B** (keep `/etc/tether` root-owned; relocate only `nats.conf`).
- **#22 nats.conf path seams** (Option B must move all): default `/etc/tether/nats.conf` is hardcoded at
  `cmd/tether/serve.go:137` + documented default at `internal/serveconf/serveconf.go:72-73`; install.sh writes
  it at `:683` (`cat > "$etc/nats.conf"`), `chmod 644` `:698`, `nats-server` `ExecStart -c $etc/nats.conf`
  `:709`; broker.yaml example (COMMENTED) `:549`. **Migration trap**: install.sh's broker.yaml has NO active
  `nats_conf_path` line (`:549` is a comment) ⇒ existing hosts rely on the CODE default, so relocating the code
  default alone would repoint a live reconciler at a nonexistent path.
- **#6 root cause**: `internal/clusteroffline/offline.go:616` `acquireFlock` → `os.OpenFile(path, O_RDWR|
  O_CREATE, 0o600)` (owner = creating euid); called by `cluster init` and every offline op (ForceSingle `:70`,
  Resnapshot `:150`, Recover `:405`, Restore `restore.go:74`). **Deeper truth (why lock-only chown is wrong)**: `internal/cluster/
  offline.go:42-52` `RaftStoreLockedByDaemon` returns a **hard error** (not `bolt.ErrTimeout`) on a root-owned
  `raft.db` EACCES ⇒ a root-run init poisons `raft.db`/`tether.db` too; chowning only the lock removes the loud
  tripwire and deepens silent breakage. Real defect = "offline ops must run as the data-dir owner."
- **#23 root cause**: `cmd/tether/serve.go:228` treats `b.Run(ctx)` returning `context.Canceled` as **clean
  exit 0** (`return nil`); `broker.go:1003-1007` main loop's only return is `case <-ctx.Done()`; root ctx =
  `signal.NotifyContext(SIGINT, SIGTERM)` (`serve.go:217`); broker's own nats conn is `MaxReconnects(-1)`
  (`authcallout.go:36`) so the main conn never closes on nats loss; `clusterwrite.go:153 b.cl.cancel()` is
  NORMAL shutdown (not the trigger). **`b.Run` stores the signal ctx directly (`broker.go:601 b.runCtx=ctx`),
  derives NO nats-driven child ctx** — confirming the true clean-exit trigger is NOT statically locatable (as
  gotcha #23 states; needs a real journal). Unit `install.sh:734 Restart=on-failure` ⇒ a clean exit-0 is not
  revived. install.sh does NOT `daemon-reload` (`:591` is printed heredoc text).
- **#24 root cause**: `internal/cluster/transport.go` misleads in TWO places — the func doc-comment ~`:52-54`
  and inline `:91` (`InsecureSkipVerify:true, // …raft addrs have no SANs`). Both are correct for tether's OWN
  raft TCP transport (custom `verifyChainToCA` `:63-81`, hostname skipped, CN-only OK) but the SAME
  `route-cert.pem` is consumed by nats-server's cluster route mesh via `internal/natscluster/config.go`
  (`cluster{tls{verify:true}}`, standard Go x509, Go 1.15+ REQUIRES a SAN). **IP-SAN nuance**: `config.go:33`
  RouteURL example is `nats://10.0.0.2:6222` (an IP); nats matches the SAN against the DIALED route host, so
  the SAN must match the route-URL host (`DNS:` for a hostname, `IP:` for an IP). tether ships no x509 minter
  (`cluster keygen`, `cmd/tether/cluster.go`, mints only `node-ident.nk`). `cluster-runbook.md:63` currently
  states "route mTLS verifies chain-to-CA only (no SAN/hostname)" — WRONG for the mesh.
- **sim reaches the fix via the REAL install.sh**: `test/simcluster/Dockerfile:40` `COPY vendor/install.sh`;
  provision runs it (doctor `:637` asserts units come from install.sh). So the #22/#23 install.sh changes flip
  drill 13 / doctor **product-driven, not sim-masking** (Mandate-compliant). `provision-node.sh` deliberately
  does not chown ETC.

## 0.1 Decision ledger (9 synthesis DECISIONs — adjudicated)

| # | Decision | Ruling | Rationale |
|---|---|---|---|
| 1 | #22 privesc boundary | **Option B** (relocate nats.conf to a tether-owned subdir; `/etc/tether` stays root-owned) | **User choice.** Avoids the tether→root Caddyfile privesc entirely. |
| 2 | subdir mode/owner | `/etc/tether/nats.d/` `0750` owner `tether:tether` | matches LIB/LOG; only nats.conf lives there. |
| 3 | macOS BSD-`install` gate | verify `install -d -o` behavior on the macOS test gate during impl | BSD `install` differs; LIB/LOG already rely on it, but nats.d is new. |
| 4 | #6 guard | **WARN + strong docs** (no hard REFUSE, no lock-only chown) | non-breaking; refuse could strand a legit root recovery; lock-only chown masks deeper raft.db poison. |
| 5 | #23 StartLimit / token | keep DEFAULT StartLimit; **REJECT** `StartLimitIntervalSec=0`; **#23 trailer token STAYS — only #22 drops in G1** | auto-recovery flip is entangled with #10 (out of G1); dropping #23 token now = masking. **Corrects the roadmap G1 exit line** ("drops #22/#23/#24" → "drops #22 only"). |
| 6 | #23 characterization | optional sim journal-capture experiment, **non-blocking** (impl-phase, may skip) | diagnosis only; informs a later batch whether a product return-point fix is needed. |
| 7 | nats-server unit | leave `Restart=on-failure` | no clean-exit-0 pathology; #23's anchor is the broker. |
| 8 | #24 x509 invariant test | **include** (hermetic, off-root) | cheap; pins the SAN-required contract next to the corrected comment. |
| 9 | #23/#24 trailer tokens | **STAY** (never edit `lib/secrets.sh` or `drills/11-grow-gaps.sh:48`) | docs-only #24 and unit-only-not-auto-recovering #23 have no honest runtime flip. |

---

## 1. Per-gotcha plan

### #22 — relocate the reconciler's nats.conf to a tether-owned subdir (Option B)

**Fix.** Keep `/etc/tether` root-owned (Caddyfile/broker.yaml stay out of `tether`'s reach); move ONLY the
reconciler-managed `nats.conf` into a dedicated tether-owned `/etc/tether/nats.d/`. Concrete edits:

1. `scripts/install.sh`:
   - After the existing `install -d` block (`:491`), add: `install -d -o tether -g tether -m 0750
     "$ETC_DIR/nats.d"` (+ mirror into the dry-run log `:494`).
   - Write the standalone conf to `"$etc/nats.d/nats.conf"` (`:683`, `:698 chmod 644`).
   - `nats-server` `ExecStart -c $etc/nats.d/nats.conf` (`:709`).
   - **Uncomment + activate** the broker.yaml `nats_conf_path` line (`:549`) to the EXPLICIT
     `nats_conf_path: /etc/tether/nats.d/nats.conf` — so a fresh install never relies on the code default
     (removes the migration trap for new hosts).
   - Fix the dry-run summary at `:672`.
2. Code default: repoint `cmd/tether/serve.go:137` + `internal/serveconf/serveconf.go:72-73` doc + the flag
   help `serve.go:264` to `/etc/tether/nats.d/nats.conf`. **Because install.sh now writes an explicit
   broker.yaml path, the code default only affects hosts with NO explicit path** — see migration.
3. `cmd/tether/cluster_natsconf.go` (the `reconcile nats` CLI): confirm its `--conf` default / `route-cert.pem`
   sibling references (`:328`) still resolve; the reconcile path is passed explicitly by callers, so this is a
   default-string + doc update, not a logic change.

**takeover.go**: NO change — it writes wherever `confPath` points; once `confPath=/etc/tether/nats.d/nats.conf`
(a tether-owned dir), its `CreateTemp`+`Rename` succeed as `User=tether`.

**Migration / compat (the cost of Option B — spell it out).**
- **New hosts**: clean — install.sh lays down nats.d/ + explicit broker.yaml path.
- **Existing hosts (already-installed brokers)**: a **multi-step, per-host migration** (NOT a plain install.sh
  re-run — that rewrites nats.conf to the STANDALONE template and does not daemon-reload, an R3 de-cluster
  hazard on a clustered member): (1) `install -d -o tether -g tether -m 0750 /etc/tether/nats.d`; (2)
  `mv /etc/tether/nats.conf /etc/tether/nats.d/nats.conf`; (3) edit `nats-server.service` ExecStart `-c` →
  new path + `systemctl daemon-reload`; (4) set `broker.yaml nats_conf_path` explicitly to the new path;
  (5) `systemctl restart nats-server` (detached, since it drops the ctl channel). Document in broker-ops.md as
  a dedicated "#22 nats.conf relocation" runbook, with an explicit CLUSTERED-member warning. **Un-migrated
  hosts keep working** on the old root-owned `/etc/tether/nats.conf` via their existing broker.yaml/default —
  they simply remain #22-afflicted until migrated (the honest Option-B tradeoff vs Option A's one-line
  auto-fix).
- **DECISION-2b (RULED during impl): unify on `/etc/tether/nats.d/nats.conf` end-to-end** — install.sh writes
  the conf there, `nats-server` ExecStart reads there, AND the code default (`serve.go:137` + `serveconf.go`)
  moves there. Rejected the "keep old code default" variant: it creates a single-mode(`/etc/tether/nats.conf`)
  vs cluster(`nats.d/`) path SPLIT that the reconciler-vs-nats-server-ExecStart must both agree on, which is
  more fragile than one consistent path. **Consequence for the live fleet (enforced by the runbook ORDER)**: a
  host whose broker.yaml lacks an explicit `nats_conf_path` relies on the code default, so the binary upgrade
  would repoint its reconciler to `nats.d/` — therefore the migration MUST run **before/with** the binary
  upgrade: (i) `mv` the conf into `nats.d/` + fix `nats-server.service` ExecStart + `daemon-reload` + restart
  nats-server, (ii) set broker.yaml `nats_conf_path` explicitly to the new path, (iii) THEN upgrade the tether
  binary. Most clustered brokers already carry an explicit `nats_conf_path` (written by the `cluster init`
  seam, e.g. sim `simcluster:186`), so their reconciler is unaffected until the migration flips it; a host
  with NO explicit path is the one that must migrate conf+ExecStart first. The runbook states this order as a
  hard precondition and warns that a bare install.sh re-run on a clustered member is forbidden (standalone
  template clobber). New installs are consistent by construction.

**Go tests.** Table-driven `internal/natsconf` test on `Apply`: (a) writable dir → succeeds; (b)
`chmod 0555` parent dir → returns an error whose text contains the `natsconf: temp` / `permission denied`
tokens drill 13 greps. **Arm (b) MUST `t.Skip` when `os.Geteuid()==0`** (root ignores dir perms; false-pass
under root CI / sim). This pins the drill-13 error contract independent of the relocation.

**Sim acceptance (drill 13 flip — product-driven).** Re-vendor `scripts/install.sh` + rebuild the sim image
FIRST (a stale baked image once masked #22). Then legitimate edits (product genuinely fixed):
- `drills/13-inbroker-reconcile-perm.sh`: the control "`/etc/tether` DIRECTORY is root-owned" **STAYS
  root-owned** (Option B keeps it so — Caddyfile safe); ADD a control "`/etc/tether/nats.d` is tether-owned";
  the `assert_bug` write (`:67-71`) retargets `--conf /etc/tether/nats.d/nats.conf` and flips to `assert_ok`
  (the `User=tether` write now succeeds). Scope the real-auto-path assertion (`:51-52`) NARROWLY to the
  disappearance of the `apply: natsconf: temp: … permission denied` reason (NOT full convergence — that
  conflates #22 with #3/#10, sim-fidelity blocker).
- `image/provision-node.sh:32-35`: read/patch `$ETC/nats.d/nats.conf` (follows the product path; NOT a chown,
  NOT masking).
- `simcluster` grow `_reconcile_clustered` (`:130-134`) + broker.yaml seam (`:186`) + `cmd_natsconf` (#20
  probe) + drill 21 (`:22`): retarget to `/etc/tether/nats.d/nats.conf`.
- Doctor #22 tripwire (`simcluster:620-624`): invert to assert `/etc/tether/nats.d` tether-owned = `ok`;
  root-owned nats.d (or missing) = WARN (un-upgraded host). Keep `/etc/tether` root-owned as the Caddyfile
  guard (NEW doctor check: `/etc/tether` MUST stay root-owned — Option B invariant).
- **grow `#22` trailer token**: run a REAL second grow (N=2→3) post-fix and inspect whether `_allfailed`
  (`simcluster:280`) goes 0. Drop `#22` from the trailer + `drills/11-grow-gaps.sh` ONLY if the empirical grow
  confirms the perm-denied reason is gone; if `_allfailed` stays 1 for a NON-perm reason, refine the grow
  detector (`:252`) to attribute #22 specifically (split the `permission denied` case out of the coarse OR).

**Risk.** Medium (up from A's low) — the relocation touches install.sh + a code default + several sim paths,
and existing-host migration is multi-step. Mitigated by keeping `/etc/tether` root-owned (no privesc, no
Caddyfile risk) and by the backward-safe default (recommend variant (a)).

### #6 — offline ops must run as the data-dir owner (docs + WARN guard)

**Fix.** PRIMARY = docs mandate in broker-ops.md / cluster.md / cluster-runbook.md: run `cluster init` and ALL
offline ops (`resnapshot`/`force-single`/`recover`/`restore`/`dump`) as `sudo -u tether` (matches the
tether-owned `data_dir` at `install.sh:491` and the sim standard). PLUS a small (~6-line, table-testable)
guard in the offline CLI path (`cmd/tether/cluster_offline.go`): when `os.Geteuid()==0` AND `DataDir` is owned
by a non-root user, emit a WARNING ("offline op as root against a tether-owned data dir; created files will be
root-owned — run `sudo -u tether`"). **Do NOT ship a lock-only chown** (masks the deeper raft.db poison, §0).

**Go tests.** Table-test the pure euid/owner-mismatch decision helper (euid + dir-owner → warn?), runs
off-root; any real chown/syscall arm root-gated (`t.Skip` when `euid!=0`). `-race` on `internal/clusteroffline`
(flock-adjacent).

**Sim.** NONE — the sim standardizes on `User=tether` init and does not reproduce #6; do NOT add contrived
root-init scaffolding (that GAP drill belongs to a later batch). Regression guard = the Go decision test +
docs. `#6` has no trailer token (sim never labeled it).

**Migration.** Hosts poisoned by a past root init: stop the daemon, `sudo chown -R tether:tether
/var/lib/tether` (recursive is correct here — whole tree was root-poisoned, unlike #22) or re-init as tether.
Document beside the #22 runbook.

**Risk.** Very low (docs + non-fatal WARN; no flock-semantics change).

### #23 — unit-first durable revival (`Restart=always`); product return-point DEFERRED

**Fix.** `scripts/install.sh:734`: `Restart=on-failure` → `Restart=always`, add `RestartSec=2` to
`[Service]`. systemd semantics (verified): `always` revives ANY exit incl. clean 0 but does NOT revive a unit
stopped via `systemctl stop`/`restart` (systemd knows it stopped it) — operator stop/restart/upgrade
unaffected; only a self-exit is revived. The broker is a long-running daemon with no legit "job done, exit 0"
path (serve.go:231 `return nil` is only the SIGTERM/Canceled shutdown, which systemd won't revive), so `always`
is safe. **Keep the DEFAULT StartLimit; REJECT `StartLimitIntervalSec=0`** (it would let a genuinely-wedged
broker — bad config / corrupt raft / boot panic, all non-zero exits — restart forever every ~2s and never
enter `failed`, blinding unit-state alerting; its only justification is surviving the #10 exit-70 crash-loop,
which is OUT of G1).

**Scope honesty (DECISION 5).** Because the true clean-exit trigger is unlocated, G1 does NOT claim a durable
auto-recovery flip: under a sustained outage with immediate re-exit, `always`+default StartLimit could
re-latch `failed`. So **KEEP the sim's `reset-failed`+`start` recovery (`simcluster:325-333`) and KEEP the
`#23` trailer token; do NOT edit `drills/11-grow-gaps.sh:48`; do NOT drop the `simcluster:318` append.** The
honest auto-recovery flip (which must reach into #10's StartLimit gate) is DEFERRED to the #10 batch.

**Product return-point (DEFERRED, optional).** Do NOT invent a guessed change. DECISION 6: optionally run ONE
cheap deploy-tier sim journal-capture experiment to CHARACTERIZE the broker's exit on a sustained nats outage
(single exit vs rapid re-exit) — diagnosis, not a fix, non-blocking, not a G1 exit gate.

**Migration.** install.sh does NOT daemon-reload, and re-running it on a clustered member is forbidden (#22).
So live-fleet remediation = a **systemd drop-in**, not an install.sh re-run: `systemctl edit tether-broker`
adding `[Service]\nRestart=always\nRestartSec=2`, then `daemon-reload`. Survives future install.sh re-runs,
touches no nats.conf, applies on next exit with no broker restart. Document the TWO distinct migration
profiles: #22 relocation (multi-step, per §1); #23 drop-in (`daemon-reload`, no restart).

**Test / sim.** No Go change (unit is a bash heredoc). Regression guard = a deploy-tier doctor drift
assertion: `systemctl show tether-broker -p Restart` == `Restart=always`, added beside the unit-presence
tripwire (`simcluster:637`). No dedicated RED reproduction drill (trigger unlocated — do not invent a flaky
one). Reaches the sim via the real vendored install.sh; re-vendor + rebuild + run `doctor`.

**Risk.** Low. `always`+`RestartSec=2` cannot affect operator stop/restart and only additionally revives
exit-0 (no legit broker cause). The masking risk (`StartLimitIntervalSec=0`) is explicitly rejected.

### #24 — SAN docs + correct both misleading comments + hermetic x509 test

**Fix (docs + comment only; no runtime/cert-format change — the mesh always required the SAN).**
- Correct BOTH `transport.go` comments (~`:52-54` and `:91`): scope the "no SAN" claim to tether's OWN raft
  TCP transport, and cross-reference `internal/natscluster/config.go`'s `verify:true` as the SAN-requiring
  consumer of the same `route-cert.pem`.
- Docs (`cluster.md`, `broker-ops.md`, `cluster-runbook.md` — fix the reverse-misleading `runbook:63` line +
  augment route-leaf minting guidance + `cluster_rotation.go:85` "YOUR PKI"): the ROUTE leaf MUST carry
  `subjectAltName` **matching the route-URL host** (`DNS:<host>` for a hostname route URL, `IP:<addr>` for an
  IP route URL) + `extendedKeyUsage=serverAuth,clientAuth`. Give both copy-pasteable openssl recipes mirroring
  the sim's proven `_mint_leaf` (`test/simcluster/lib/secrets.sh`). Note the TUNNEL leaf is fingerprint-pinned
  (`internal/broker/cutover.go` RF3) so its SAN is RECOMMENDED (uniform, future-proof), not strictly required.
- **REJECT the SAN-minting helper in G1** (reopens tether's "never handles your private keys" posture;
  belongs in a G2/G4 cert-tooling leaf).

**Go test (DECISION 8).** Hermetic table-driven x509 test in `internal/natscluster` (or `internal/cluster`):
build a CN-only leaf and a SAN-bearing leaf off a test CA, run each through `x509.Certificate.Verify` with a
`DNSName`/`IPAddresses` (as nats `verify:true` does) → assert CN-only FAILS (`legacy Common Name`), SAN
passes. No root.

**Sim.** NO edit, NO drill flip. G1 ships no minter, so the sim keeps legitimately minting SAN certs
(`lib/secrets.sh` — Mandate ③) and the `#24` trailer token (`simcluster:159`, asserted `drills/
11-grow-gaps.sh:48`) STAYS. A CN-only RED drill is DEFERRED (a docs-only fix has no runtime flip; a CN-only
cert CORRECTLY still fails the mesh — editing the sim to "pass" it would be masking).

**Risk.** Very low (docs + comments + one hermetic test).

---

## 2. Cross-cutting

**Ordering (all four independent).** (1) `scripts/install.sh` — #22 (nats.d relocation) + #23 (`Restart=
always`) in one coherent diff (shared file); (2) the code default repoint for #22 (`serve.go`/`serveconf.go`,
recommend backward-safe variant (a)); (3) #6 docs + offline-CLI WARN; (4) #24 two `transport.go` comments +
docs + x509 test. Do the three docs (`cluster.md`/`broker-ops.md`/`cluster-runbook.md`) in ONE pass — shared
by #24 (SAN), #6 (`sudo -u tether`), #22/#23 (migration runbooks).

**Shared files.** `scripts/install.sh` (#22+#23); `cmd/tether/serve.go` + `internal/serveconf/serveconf.go`
(#22 default); the three docs; `internal/cluster/transport.go` (#24); `test/simcluster/` (drill-13 rewrite +
provision/grow/probe path retarget + doctor #22-tripwire invert + new `/etc/tether` root-owned guard + #23
Restart-drift assert) — and **re-vendor `scripts/install.sh` + rebuild the sim image** so fixes reach the sim
through the real product.

**Hard gates (Stage-C exit).** `make test` (#22 `natsconf.Apply` perm test root-guarded, #6 decision test,
#24 x509 test) + `make lint` (golangci-lint v2) + `make e2e` (unaffected) + `-race` on
`internal/clusteroffline`. **Deploy-tier gate**: after re-vendor + rebuild, run ONLY `drill 13` (#22 flip) +
`doctor` (#22 tripwire invert + `/etc/tether` root guard + #23 Restart drift) + one REAL second grow to
confirm the #22 token drop — NOT the full loop, NOT drill-11/#23/#24 token edits. Land each product fix + the
sim edit it flips in the SAME change so the tree never sits RED-without-explanation.

**Git (per user memory).** Commit directly to `main` (single-repo, no `phase/*` branch), conventional-commit,
**no AI co-author trailer**. **This plan stops at the EXTERNAL-REVIEW gate**: after Stage-C internal review +
hard gates pass, do NOT `git add` (staging is the external reviewer's step) — hand off for external review.

## 3. Wire impact

**NONE.** `internal/proto.ProtoVersion` untouched; no NATS subject / message schema / serialization /
`auth_callout` identity change. #22 = filesystem path relocation + ownership; #6 = ownership + a WARN; #23 = a
systemd directive; #24 = docs + two comments + one hermetic x509 test. The #24 SAN is a TLS-transport identity
matter orthogonal to the control-plane wire, and the mesh ALWAYS required the SAN (`config.go verify:true`
unchanged) — not even a new cert-format contract. No v1→v2 cross-version path; the mixed-version fleet is
unaffected. R3 "never silently de-cluster" is untouched; the only R3-adjacent hazard — "re-run install.sh on a
clustered member" — is explicitly forbidden in the #22 migration runbook.

## 4. Deferred out of G1 (with reasons)

- **#24 SAN-minting helper** + CN-only RED drill — new crypto/x509 + CA-key handling reopens "tether never
  handles your private keys"; belongs in a G2/G4 cert-tooling leaf (also the change that lets the sim drop the
  `#24` token).
- **#23 product return-point location** — trigger unlocatable, needs a real sim journal; unit-first is the
  durable G1 fix. Optional characterization arm (DECISION 6).
- **#23 auto-recovery token-drop / `StartLimitIntervalSec=0`** — entangled with #10 (exit-70 crash-loop) via
  the StartLimit gate; defer to the #10 batch; keep the sim `reset-failed` recovery + `#23` token.
- **#6 whole-tree chown / root-init GAP drill** — docs-mandate + WARN suffices; defer the deeper repro.
- **All other gotchas** — force-single #12/#20, ghost-voter #10/#15, grow orchestration #3/#4/#8, small-disk
  #21, upgrade/observability — out of G1.

## 5. Roadmap correction (feeds back to `ha-grow-ops-remediation-roadmap.md`)

The roadmap's G1 exit line ("grow trailer drops #22/#23/#24 tokens") is **too optimistic** and must read:
**G1 drops ONLY the `#22` token** (empirically, via a real second grow). `#23` (auto-recovery entangled with
#10) and `#24` (docs-only, no minter) tokens STAY until their deferred arms land. Update the roadmap G1 row's
`sim 验收` cell accordingly during Stage-C.
