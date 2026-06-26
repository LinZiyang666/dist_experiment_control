# C8 Plan (FINAL) — Cluster CLI command-surface consolidation

> Stage-A output. 9-agent workflow (4 lens drafters → 4 cross-critiques → 1 synth; synth re-run after an ECONNRESET on the first attempt; all Opus 4.8; full raw tasks/wo7vl7d0s.output). Main process sole finalizer. Implements docs/v2-cli-consolidation-proposal.md (the delete-as-you-build C3–C6 skipped). Every decision file:line-verified. Key catches: the cobra .Deprecated stdout-pollution trap (D2), keygen is the ONLY node-ident.nk minter so node-pub/keygen DEMOTE-not-delete (D7), the apply spec.go prints deleted verbs so 3 bounded internal touches are required (D11), the D10 expose-home/NATS-peer identity gate must be preserved at join prepare + JoinBundle.Validate.

All claims verified against ground truth. Synthesizing the binding plan now.

---

# C8 — Cluster CLI Consolidation — Binding Plan

> Implements `docs/v2-cli-consolidation-proposal.md` — the delete-as-you-build that C3–C6 skipped. CLI-surface phase: favor deletion over abstraction, never weaken a gate, never leave a live dual surface. Ground truth re-verified for every decision below (file:line cited).

## 0. Objective + acceptance

**Goal.** C1–C7 only *added* cluster commands (24 visible top-level). C8 deletes/demotes the now-redundant manual primitives the C3 reconciler + C4 operations replaced, and fixes the C6 recovery inversion. cluster CLI is v2-only / unreleased → deleting commands has zero wire/script compat burden; the one-cycle hidden aliases protect only operator runbook muscle-memory.

**Before → after command tree (visible top-level):**

```
BEFORE (24 visible, 4 groups online/migrate/escape/local)
 online : status add drain remove transfer-leader rotate-tunnel-cert wait
          backup export-incident ops apply seeds reconcile join retire
 migrate: init doctor                         (+ takeover-natsconf hidden)
 escape : force-single recover restore recovery
 local  : sign-join node-pub keygen

AFTER (14 visible, 3 groups online/migrate/escape)
 online : status drain transfer-leader rotate-tunnel-cert backup ops apply
          seeds reconcile join retire                       (11)
 migrate: init doctor                                       (2)
 escape : recovery                                          (1)
   recovery
     ├─ diagnose            --self-id [--offline] [--db]         (read-only)
     ├─ force-single        --confirm-peers-dead …  (PRIMARY)
     ├─ rejoin prepare      --dump-divergent …      (PRIMARY; was `recover`)
     ├─ restore <bundle>    --confirm-node-id …      (PRIMARY; moved in)
     ├─ incident export     [--since|--sid|--out]    (PRIMARY; was `export-incident`)
     └─ node remove <id>    --manual [--force]        (PRIMARY; was `remove`, Class A)

 HIDDEN (no group): takeover-natsconf, node-pub, keygen (debug),
   + one-cycle deprecated aliases: force-single, recover, restore,
     export-incident, remove  → each warns to stderr + delegates, deleted next tag
 GONE: add, sign-join, wait
```

**Net count:** visible top-level **24 → 14 (−10)**; cobra groups **4 → 3** (`local` emptied + removed). Day-2 add = `join prepare` + `join approve --wait`; day-2 remove = `retire --wait` (routine) / `recovery node remove --manual` (escape).

**Acceptance (DoD):** (1) `add`/`sign-join`/`wait` absent from the command set; no command is a *visible* child of two parents. (2) every deleted automation keeps an escape hatch (table §2). (3) every safety gate byte-preserved (§4 proof). (4) `make test` + `make e2e` + `make lint` green; `go test ./cmd/tether/ ./internal/clusterspec/ ./internal/cluster/` + `test/d7` green. (5) runbook + usage rewritten in the same PR (no broken-runbook). (6) the named adversarial tests (§6) all present and green.

## 1. Binding decisions (every tension resolved)

| # | Tension | **Decision** | Rationale |
|---|---|---|---|
| D1 | recovery inversion (C6 built backwards) | **FLIP**: `recovery` is PRIMARY and hosts force-single/rejoin prepare/restore/incident export/node remove/diagnose; top-level escapes become **hidden one-cycle deprecated aliases**. `newClusterRecoveryCmd` gains `socketPath *string`. | Proposal §3B/§6.4. |
| D2 | deprecation mechanism | **Manual `Hidden:true` + `fmt.Fprintln(cmd.ErrOrStderr(), "deprecated: …")` RunE wrapper** (helper `deprecatedClusterAlias`). **REJECT cobra `.Deprecated`.** | Verified: `.Deprecated` prints via `OutOrStderr()` which returns `c.outWriter` after `SetOut` (`cobra@v1.10.2/command.go:911,1435`) → rides **stdout**, polluting `export-incident` JSON and failing the stderr-isolation tests. Matches repo precedent `cluster_natsconf.go:205`. |
| D3 | `--manual` on `recovery node remove`: required vs inert | **REQUIRED + fail-closed.** Without it: refuse, point at `cluster retire`. Order in shared RunE: `rejectedUnattendedYes` → `--manual` gate → `confirmTypedNodeID`. | Brief Class A says "REQUIRED"; inert would weaken the guardrail (§6.3). Order keeps `cluster_confirm_test.go:21` ("cannot run unattended") green. |
| D4 | `drain --retire` | **Redirect-error**, NOT `MarkDeprecated`. Keep the flag, `MarkHidden("retire")`, and at top of RunE `if retire { return usageErr("`cluster drain --retire` is removed; run `tether cluster retire %s` (resumable, same F==0 gate)", node) }`. | `MarkDeprecated` still parses+honors the flag → `OpClusterDrain{Retire:true}` stays a **live second mutation path** = the forbidden dual surface (§6.1). Redirect kills it immediately. |
| D5 | plan/apply | **KEEP `apply` as-is (name unchanged); do NOT build `apply <plan-id>`.** But **rewrite its plan-output engine `internal/clusterspec/spec.go`** off the deleted verbs. | Lightest correct: executing pair is a feature (descoped `docs/v2-automation-program.md:68`). Renaming→`plan` adds alias/doc churn on an unreleased command for cosmetic gain. The spec.go rewrite is mandatory either way (see D11). |
| D6 | `membership` namespace (proposal §4) | **Do NOT build it.** Keep join/drain/retire/transfer-leader top-level under the `online` group. | cobra groups already give the visual framing; a `membership` parent lengthens the flagship short path (contradicts §1/§7). delete>abstract. `recovery` is the only new namespace. |
| D7 | `keygen` / `node-pub` | **DEMOTE to hidden (Hidden:true, GroupID=""), never delete.** | `keygen` is the **only** in-binary minter of `/etc/tether/node-ident.nk`; install.sh:547 only comments the layout, broker never auto-creates it, `internal/clusteroffline/preflight.go:40` requires it pre-exist. Deleting orphans 2nd-node bootstrap. |
| D8 | `takeover-natsconf` hidden alias | **KEEP one more cycle (no change).** | Already hidden+deprecated since C3; the cluster tree never shipped so the "cycle" is nominal; deleting forces a `cluster_help_test.go:53` `find()` edit for zero gain. Mark delete-next-cycle as a doc follow-up. |
| D9 | lost `versionSkewResponse` gate (no Proto/Release in `JoinBundle`) | **ACCEPT the loss + document.** Keep `handleAdd`/`OpClusterAdd`/`versionSkewResponse` as **latent internal code** (referenced by `b6_skew_test.go`, `internal/adminsock/*_test.go`). Backlog: add Proto/Release to `JoinBundle` + skew-check in `StartJoinOperation`. | Not a safety gate: proto mismatch is hard-fenced at the proto-v2 wire layer regardless; release skew was *advisory only* (`clusterstatus.go:761-779`). Only a friendlier pre-admission diagnostic is lost. |
| D10 | lost D9 identity-completeness gate (add required tunnel+cert-fp+nats; join doesn't) | **PRESERVE.** (a) `join prepare`: require `--tunnel-addr` + `--nats-route` (CLI-level); keep `--cert-fp` optional (D6 backfills — intentional divergence). (b) `JoinBundle.Validate` (`internal/cluster/join_bundle.go:62`): add `|| b.TunnelAddr=="" || b.NatsRoute==""` to the reject — leader-side authoritative enforcement so a hand-crafted bundle can't bypass. | This *is* a load-bearing gate ("a voter that can never serve as expose home / NATS peer"). One-line internal guard is justified (we already touch `internal/clusterspec`) and has zero existing-test fallout (no `join_bundle_test.go`). |
| D11 | "no internal/* change" framing | **False for C8.** Scope is CLI-surface **plus** three tightly-bounded internal touches: `internal/clusterspec/spec.go` verb strings (+ its test), and the `JoinBundle.Validate` one-liner (D10). No reconciler/op/raft logic changes. | The `apply` plan output literally prints the deleted verbs (`spec.go:110,168,191,214`) — keeping `apply` correct is impossible without it. |
| D12 | seed-reader strictness regression | `join prepare` must `strings.TrimSpace` the seed before `PublicKeyFromSeed`/`SignWithSeed` (`cluster_join.go:44-48,67`). | Deleted `sign-join`/`node-pub` trimmed; `join prepare` reads raw. keygen writes no newline so happy-path works, but a hand-edited `node-ident.nk` with a trailing `\n` would now fail. One-line fix restores tolerance. |

## 2. Deletions / demotions (replaced-by · surviving-internal · escape-hatch)

| Cmd | Action | Replaced by | Surviving internal logic | Escape hatch |
|---|---|---|---|---|
| `cluster add` (`cluster.go:428` `newClusterAddCmd`) | **DELETE** | `join prepare` (`cluster_join.go:35`) + `join approve --wait` (`:97`); `OpClusterJoinApprove→StartJoinOperation` is independent of `handleAdd` (`internal/broker/clusterstatus.go:600` vs `:598`) | broker `OpClusterAdd`/`handleAdd`/`versionSkewResponse` left **latent** (D9; test-referenced) — never CLI-reachable, not a dual surface | `join approve` is recoverable: `ops abort <id>` + re-approve; `transfer-leader` for leadership |
| `cluster sign-join` (`cluster_offline.go:157`) | **DELETE** | folded into `join prepare` (same crypto `cluster.JoinSignBytes`+`auth.SignWithSeed`, `cluster_join.go:61-67`) | `JoinSignBytes`/`SignWithSeed`/`PublicKeyFromSeed` already called by `join prepare` | none needed (dies with `add`) |
| `cluster wait` (`cluster_wait.go:73` `newClusterWaitCmd`) | **DELETE** (+ `phaseGone` const `:68`, sole use `:88`) | per-op `--wait` (`join approve --wait`/`retire --wait`/`transfer-leader --wait`) + `ops show` + `status --watch` | **KEEP** `waitForConverge`/`watchClusterStatus`/`nowFunc`/`minWatchInterval` (used by `transfer-leader --wait` `cluster.go:620` + `status --watch` + `cluster_wait_test.go`) | `ops show <id>` / `status --watch` |
| `cluster node-pub` (`cluster_offline.go:240`) | **DEMOTE → hidden** (Hidden:true, GroupID="") | `join prepare` derives pub internally | RunE intact; runnable for debug | is itself the debug escape |
| `cluster keygen` (`cluster_offline.go:264`) | **DEMOTE → hidden** (never delete — D7) | n/a (sole seed minter) | RunE intact | **IS** the bootstrap escape for fresh `node-ident.nk` |
| `cluster remove` (`cluster.go:554` `newClusterRemoveCmd`) | **DEMOTE → `recovery node remove --manual`** (primary) + **hidden deprecated top-level alias** one cycle | routine path = `cluster retire` (`cluster_retire.go`) | `OpClusterRemove→admin.RemoveNode` (`clusterstatus.go:588`) **KEEP** — the raw raft-config removal for a node that can't retire (VOTER_ADD_FAILED) | **`recovery node remove --manual`** is the raw escape; new `--manual` makes the bypass opt-in (stronger) |
| `takeover-natsconf` (`cluster_natsconf.go:200`, already hidden) | **CONFIRM** unchanged | `reconcile nats` auto | `runNatsconfTakeover` shared with `reconcile nats --manual` | **`reconcile nats --manual`** |

## 3. Merges + recovery-inversion + plan/apply

**3.1 recovery inversion (D1).** Rewrite `newClusterRecoveryCmd(socketPath *string)` to host PRIMARY commands (fresh constructor instances → gate-identical by construction, since a `*cobra.Command` can't have two parents):
- `diagnose` (existing, keep), `force-single` = `newClusterForceSingleCmd()`, `rejoin prepare` = `newClusterRecoverCmd()` (`Use="prepare"`, clear inherited Example), `restore <bundle>` = `newClusterRestoreCmd()`, `incident`→`export` = `newClusterExportIncidentCmd(socketPath)`, `node`→`remove` = `newClusterRemoveCmd(socketPath)` (+ required `--manual`).
- Reword the recovery `Short`/`Long` and the `escape` **group title** to drop the absolute "daemon STOPPED / offline" claim: diagnose/force-single/rejoin are offline; **restore/incident export/node remove are last-resort ONLINE ops** (they call `callAdmin`). (critique-3 C6.)
- In `newClusterCmd`: register `addGrouped(newClusterRecoveryCmd(&socketPath), "escape")`; the four old top-level escapes + `remove` become `root.AddCommand(deprecatedClusterAlias(newClusterX Cmd(...), "recovery …"))`. Remove their `addGrouped` lines. `recover` (hidden alias) and `recovery` (primary) stay unambiguous (`EnablePrefixMatching` unset; `root.Commands()` still returns hidden → exact-name resolves).

**3.2 `drain --retire` (D4).** In `newClusterDrainCmd`: keep flag, `MarkHidden("retire")`, redirect-error at RunE top. `drain` alone unchanged (`OpClusterDrain{Retire:false}`). Rewrite `spec.go:214` to emit `cluster retire` (D11) so the plan never prints the redirected flag.

**3.3 `reconcile nats` / `status` (confirm only).** `reconcile nats` auto + `--manual` (`cluster_reconcile.go:52`) and the single `status` with `--homes/--remote/--offline/--watch/--card/--json` are already correct — no change.

**3.4 plan/apply (D5+D11).** Keep `cluster apply -f roster.yaml` (plan-only). Rewrite the verb templates in `internal/clusterspec/spec.go` (the engine that *generates the printed plan*) off the deleted surface:

| line | before | after |
|---|---|---|
| `:110` | `cluster sign-join … paste cluster add …` | `tether cluster join prepare …  # on %s, then on LEADER: tether cluster join approve <bundle> --wait` |
| `:168` | `tether cluster remove %s` | `tether cluster recovery node remove %s --manual` |
| `:173` | `… cluster drain %s --retire …` | `… let it reach VOTER then `cluster retire %s` …` |
| `:191` | `complete the pending add(s) (sign-join + cluster add …)` | `complete the pending join(s) (cluster join approve …)` |
| `:214` | `tether cluster drain %s --retire` | `tether cluster retire %s   # migrate exposes off + remove (resumable)` |
| `:85,:87-88` doc | `cluster add` / `drain --retire` | `cluster join approve` / `cluster retire` |

Fix `cluster_apply.go:77` trailing hint "re-run takeover-natsconf …" → "`tether cluster reconcile nats --all --wait`". (`:209` transfer-leader survives unchanged.)

## 4. Safety-gate preservation proof + deprecation mechanism

**4.1 Gate proof (no gate weakened).**
- **Gate parity by construction.** Every relocated/aliased command is a fresh instance of its *unchanged* constructor; the only mutations are `Hidden=true`, `GroupID=""`, an Example/Use override, and a stderr-prefix RunE wrapper. None touches a gate. So `force-single` keeps `rejectedUnattendedYes` + mandatory `--self-id/--self-addr` + `--confirm-peers-dead` listing every peer + typed-confirm with `allowMachineEscape=false` (`cluster_offline.go:57-74`); `restore` keeps `--confirm-node-id` + Tier-2 typed-confirm (`cluster_backup.go`); `incident export` keeps `O_EXCL|O_NOFOLLOW` + redaction; `recover/rejoin prepare` keep `--dump-divergent` + typed-confirm.
- **The lone machine-escape stays lone.** `confirmTypedNodeID(..., allowMachineEscape=true, confirmNodeID)` at `cluster.go:573` is the **only** `true` site (both `--confirm-node-id` AND `$TETHER_CONFIRM_NODE_ID` must equal the node_id). It moves *with* `remove` into `recovery node remove` and stays the only `true`.
- **Order is load-bearing** (D3): `rejectedUnattendedYes` FIRST → `--manual` gate → `confirmTypedNodeID`. So `recovery node remove brk-x --yes` still yields "cannot run unattended" (keeps `cluster_confirm_test.go:21` green); `--manual` only gates the canonical-vs-raw distinction.
- **F==0 quorum gate** unaffected: `drain` and `retire` both carry it (`cluster.go:518` / `cluster_retire.go:85`); removing the `--retire` flag deletes neither.
- **D10** restores the expose-home/NATS-peer admission gate at both `join prepare` and `JoinBundle.Validate`.

**4.2 Deprecation-alias mechanism (D2).** New helper in `cluster.go`:
```go
func deprecatedClusterAlias(c *cobra.Command, replacement string) *cobra.Command {
    c.Hidden, c.GroupID = true, "" // GroupID="" so dropping the local/escape groups can't panic checkCommandGroups
    inner := c.RunE
    c.RunE = func(cmd *cobra.Command, a []string) error {
        fmt.Fprintf(cmd.ErrOrStderr(),
            "deprecated: `tether cluster %s` moved to `tether cluster %s`; this alias is removed next release.\n",
            c.Name(), replacement)
        return inner(cmd, a) // same constructor => byte-identical gates
    }
    return c
}
```
ErrOrStderr → `errWriter`, independent of `outWriter`: the note never rides `export-incident`'s stdout JSON. Hidden aliases are auto-skipped by `cluster_help_test.go:20`.

## 5. File-level change list

**Code (cmd/tether):**
- `cluster.go` — delete `newClusterAddCmd` (`:428-498`) + its `--joiner-proto/--joiner-release`; drop `addGrouped` for add/wait/remove/force-single/recover/restore; remove the `local` group; add the `deprecatedClusterAlias` helper; register `recovery(&socketPath)` + 5 hidden aliases (force-single→`recovery force-single`, recover→`recovery rejoin prepare`, restore→`recovery restore`, export-incident→`recovery incident export`, remove→`recovery node remove --manual`); add required `--manual` to `newClusterRemoveCmd` (ordered after `rejectedUnattendedYes`); reword root `Short`/`Long` + the 3 group titles; fix `init` NEXT-step `cluster add` → `cluster join prepare/approve` (`:780`) and the offline-status banner `force-single` hint → `recovery force-single`.
- `cluster_recovery.go` — rewrite to PRIMARY tree (take `socketPath`; add restore/incident export/node remove; extract diagnose; reword online/offline).
- `cluster_join.go` — `TrimSpace` seed (`:44-48,67`); require `--tunnel-addr` + `--nats-route` (keep `--cert-fp` optional) (D10/D12).
- `cluster_offline.go` — delete `newClusterSignJoinCmd` (`:157-238`); `Hidden:true` on node-pub (`:240`) + keygen (`:264`), reword to "debug" + fix node-pub Example (`:245`); rewrite recover hint strings `cluster add` → `cluster join approve` (`:104-105,141`); tidy now-unused imports `encoding/hex`, `internal/cluster`, `internal/proto` (**keep `strings`** — node-pub still uses `TrimSpace` at `:252`).
- `cluster_offline_wizard.go` — rewrite `recoverGuided` step 3 (`:73`) `cluster add … sign-join` → `cluster join prepare`/`approve`; `:48` bare `force-single` → `recovery force-single`.
- `cluster_backup.go` — restore re-grow hints `cluster add` → `cluster join approve` (`:96,:118`); restore/export-incident now constructed under recovery + hidden aliases (constructors reusable, Use/Example overridden at call site).
- `cluster_apply.go` — fix `:77` hint to `reconcile nats --all --wait`.
- `cluster_wait.go` — delete `newClusterWaitCmd` + `phaseGone`; keep the rest.
- `cluster_status_card.go` — `incidentExportHint` (`:139-144`) → `tether cluster recovery incident export --since 24h --out incident.json`.

**Code (internal — bounded, D11):**
- `internal/clusterspec/spec.go` — rewrite verb templates `:85,87-88,110,168,172-173,191,214` (table §3.4).
- `internal/cluster/join_bundle.go` — `Validate` (`:62`): add `|| b.TunnelAddr=="" || b.NatsRoute==""` (D10).
- **No change**: `internal/broker/*` (keep `handleAdd`/`OpClusterAdd`/`versionSkewResponse` latent), `internal/adminsock/*`, all reconciler/op/raft logic.

**Tests:**
- `cluster_help_test.go` — `len(g) != 4` → `!= 3`; in `TestClusterSafetyWording` **delete** the `find("add")` block, keep `find("takeover-natsconf")`/`find("init")`, retarget force-single Short check into the `recovery` primary + assert top-level `force-single` is `Hidden`.
- `cluster_recovery_test.go` — rewrite: assert recovery children = {diagnose, force-single, rejoin→prepare, restore, incident→export, node→remove} and `!recovery.Hidden`; assert top-level force-single/recover/restore/export-incident/remove are `Hidden` with a stderr deprecation; keep the recover-vs-recovery no-prefix-ambiguity check.
- `cluster_machineconfirm_test.go` — repoint `newClusterRemoveCmd` usage; **add `--manual`** to the escape-path args (else it fails the new gate); keep the never-escapable refusals.
- `cluster_confirm_test.go` — no edit (the `--yes` path still hits `rejectedUnattendedYes` first → "cannot run unattended").
- `cluster_signjoin_test.go` — **delete**; add a `join prepare` PoP round-trip + newline-seed test to preserve crypto coverage.
- `cluster_status_card_test.go` — `:24/:43/:55` substring `export-incident` → `incident export`.
- `internal/clusterspec/spec_test.go` — `:88` `"cluster remove"` → `"recovery node remove"`; `:145/:154/:172` `"drain"+"--retire"` counting → `"cluster retire"`; update `:62-63` comment.
- `test/d7/external_review_test.go` — repoint the runbook guard (`:44-47`) from `cluster recover --dump-divergent /root/divergent-$(hostname).json` to the new primary `cluster recovery rejoin prepare … --self-id` (else it passes vacuously).
- `cluster_wait_test.go` — no change (drives `waitForConverge`, kept).

**Docs / runbook (follow-up, SAME PR — broken-runbook is a landing-discipline failure):**
- `docs/cluster-runbook.md` — full rewrite of deleted verbs: add a name-migration table at top; `add`+`sign-join` two-phase → `join prepare`/`approve`; per-node `takeover-natsconf` → `reconcile nats --all --wait`; document the one-time hidden `cluster keygen --out /etc/tether/node-ident.nk` bootstrap prereq (else a fresh joiner is un-discoverable, D7); recovery verbs → `recovery *`; `cluster wait --phase` → `--wait`/`ops show`; `cluster remove` → `cluster retire` (+ `recovery node remove --manual` escape); restore/re-grow → `recovery restore` / `cluster join`.
- `docs/usage.md` — command/flag tables + prose: replace add/sign-join/node-pub/keygen/wait/remove/force-single/recover/restore/export-incident rows; mark node-pub/keygen "hidden debug".
- **No change**: `scripts/install.sh` (already on new names), `docs/reviews/*`, the proposal/program docs.
- Note follow-up: delete the `takeover-natsconf` hidden alias next cycle (D8); add Proto/Release skew to `JoinBundle` next cycle (D9).

## 6. Adversarial test list (named)

1. `TestClusterAddSignJoinWaitGone` — `add`/`sign-join`/`wait` → cobra "unknown command".
2. `TestNoLiveDualSurface` — exactly one mutating join (`join`), one mutating retire (`retire`); each migrated verb's top-level instance is `Hidden`, its `recovery` child visible; no visible double-parent.
3. `TestRecoveryIsPrimaryTreeShape` — recovery children {diagnose, force-single, rejoin prepare, restore, incident export, node remove} all `!Hidden`.
4. `TestTopLevelEscapesHiddenDeprecatedAliases` — force-single/recover/restore/export-incident/remove are `Hidden`, emit a stderr deprecation, and still reach their gate (non-TTY → typed-confirm refuses *after* the warning).
5. `TestDeprecationNoteStderrNotStdout` + `TestExportIncidentStdoutStaysPureJSON` — alias notice lands only in the `SetErr` buffer; `incident export` (and its alias) stdout parses as JSON. (Guards the cobra-`.Deprecated` trap.)
6. `TestRecoveryNodeRemoveRequiresManual` — `recovery node remove <id>` without `--manual` fails closed naming `cluster retire`; with `--manual` + typed-confirm proceeds; `--yes` (no `--manual`) still yields "cannot run unattended" (order proof).
7. `TestRemoveMachineEscapePreservedAndLone` — flag+env unattended escape works on `recovery node remove --manual`; force-single/recover/restore STILL refuse with the same flag+env (allowMachineEscape stays the only `true`).
8. `TestDrainRetireRedirects` — `drain --retire <node>` returns a usage error naming `cluster retire`; `callAdmin` is never called with `OpClusterDrain{Retire:true}`; `drain` alone still sends `Retire:false`; `--retire` is hidden in help.
9. `TestForceSingleRequiresAllPeersDead` — a missed peer in `--confirm-peers-dead` → usage error (split-brain prevention), on the recovery primary.
10. `TestKeygenHiddenStillMintsSeed` + `TestKeygenSeedThenJoinPrepareRoundTrip` — hidden `keygen --out X` mints a seed; `join prepare --seed X` derives the same pub (2nd-node bootstrap survives without add/sign-join).
11. `TestJoinPrepareToleratesNewlineSeed` — a `\n`-terminated `node-ident.nk` derives the same pub as raw (D12).
12. `TestJoinPrepareRequiresHomeIdentity` + `TestJoinBundleValidateRejectsEmptyTunnelNats` — `join prepare` without `--tunnel-addr`/`--nats-route` refuses; a bundle with empty tunnel/nats is rejected by `Validate`; `--cert-fp` stays optional (D10).
13. `TestApplyPlanEmitsNoDeletedVerbs` — `clusterspec.Diff` over an add+retire+inconsistent roster: no step Verb contains `cluster add`/`sign-join`/`drain --retire`/bare `cluster remove`; it does contain `join approve`/`cluster retire`/`recovery node remove --manual`.
14. `TestClusterGroupsAfterC8` — exactly 3 groups {online, migrate, escape}; no `local`; `recovery` in `escape`; no command carries a `GroupID` without a registered group (run `--help` to trip `checkCommandGroups`).
15. `TestRecoverVsRecoveryNoPrefixAmbiguity` — with prefix-matching off, `cluster recover` → hidden alias, `cluster recovery` → namespace.
16. `TestRecoveryIncidentExportFailClosedWrite` — moved `recovery incident export --out <existing>` still refuses (`O_EXCL`) and refuses a symlink target.
17. `TestRecoveryNamespaceSubcommandsHaveExamples` — recurse one level into `recovery`; every child has an Example.
18. `TestStatusCardHintPointsAtRecovery` — DEGRADED/FORCE-SINGLE card hint contains `recovery incident export`, not bare `export-incident`.
19. `TestRunbookCLIConsistency` (update of `test/d7/external_review_test.go`) — runbook presents `cluster join prepare/approve` and `cluster recovery rejoin prepare … --self-id` as primary, not the deprecated bare spellings.

---

**Key files**: `cmd/tether/cluster.go` (`newClusterAddCmd:428` / `newClusterRemoveCmd:554` / groups `:39-71` / init hint `:780`), `cmd/tether/cluster_recovery.go` (the inversion), `cmd/tether/cluster_join.go:44-48,89-93` (seed trim + required identity), `cmd/tether/cluster_offline.go:157,240,264,104-141` (sign-join delete / node-pub+keygen hide / hints), `cmd/tether/cluster_offline_wizard.go:48,73`, `cmd/tether/cluster_backup.go:96,118` (restore/export-incident), `cmd/tether/cluster_apply.go:77`, `cmd/tether/cluster_wait.go:68,73`, `cmd/tether/cluster_status_card.go:139-144`, `internal/clusterspec/spec.go:85,110,168,173,191,214` (+ `spec_test.go:62,82,88,145,172`), `internal/cluster/join_bundle.go:62`. **Tests to update**: `cluster_help_test.go`, `cluster_recovery_test.go`, `cluster_machineconfirm_test.go`, `cluster_signjoin_test.go` (delete+replace), `cluster_status_card_test.go`, `internal/clusterspec/spec_test.go`, `test/d7/external_review_test.go`. **Latent-kept (do not delete)**: `internal/broker/clusterstatus.go` `handleAdd`/`versionSkewResponse`, `internal/adminsock/protocol.go` `OpClusterAdd` (test-referenced; D9). **Docs same-PR**: `docs/cluster-runbook.md`, `docs/usage.md`.