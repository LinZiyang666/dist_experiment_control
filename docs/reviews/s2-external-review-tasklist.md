# S2 External Review Tasklist

Scope: independent external review of all unstaged and untracked S2 changes against `HEAD` (`49e563a`).
Internal plan/review conclusions are navigation aids only. Product implementation is read-only; review-owned
tests and review documents may be added. Live simcluster evidence must come from a fresh throwaway instance.

## Baseline, scope, and requirements

- [x] Read `CLAUDE.md`, the authoritative architecture/usage/ops documents relevant to C1/C2, the S2 roadmap
  slice, S2 plan, S2 internal review, and recent external-review/tasklist conventions.
- [x] Rebuild the complete unstaged/untracked boundary (including files omitted by ordinary `git diff`) and
  classify every changed file as drill, shared harness, image/provisioning, runner, or documentation.
- [x] Trace every S2 assertion to authoritative product code or documented behavior; treat internal review and
  claimed live spikes as untrusted until independently reproduced or source-verified.
- [x] Verify the batch remains a deploy-tier test-only leaf: no product behavior is altered or silently
  compensated for, and every harness deviation satisfies simcluster Mandate ①–④.

## Shared harness and infrastructure

- [x] Audit `ident.sh` for shell quoting/injection, identity separation, real nkey use, session-independent
  ONLINE oracles, service lifecycle, secret exposure, and cleanup/idempotence.
- [x] Audit S0-tunnel provisioning (`agentyaml.sh`, `provision-node.sh`) against the real installer layout and
  DNS/public-host semantics; prove it exercises a real reverse tunnel without weakening production boundaries.
- [x] Audit S0-ingress end to end: CA reuse, leaf SANs, file permissions, system trust injection, same-netns
  topology, route matching, TLS verification, sidecar ownership/cleanup, and fail-loud behavior.
- [x] Security-review `ingress-proxy.py`: request parsing, header forwarding, hop-by-hop headers, body bounds,
  timeout/thread/resource behavior, error disclosure, path routing, TLS setup, and shutdown behavior.
- [x] Audit `run-drills.sh` flake classification so retry signatures cannot hide real product/assertion failures.

## Drill 80 — session and tenant isolation

- [x] Verify fresh identities and two sessions are genuinely distinct; non-member CONNECT denial must not
  persist activation state.
- [x] Verify bidirectional cross-session publish/subscribe denial has same-session positive controls and
  anchored subjects, and application-layer node isolation plus owner-only checks cannot pass vacuously.
- [x] Verify wrong-PIN/correct-PIN event assertions bind fields from one event, exclude rate-limit-arm pollution,
  and distinguish generic CONNECT errors from product-observable events.
- [x] Verify the rate-limit gotcha probe has independent-source success controls, correct time/IP semantics,
  signature guarding, and will flip loudly when fixed.
- [x] Verify `TETHER_SESSION` no-crosstalk assertions observe both override and durable current-session state.

## Drill 81 — admin evict and session removal

- [x] Verify admin-socket protection is an OS permission denial with authorized positive control.
- [x] Verify evict causality and timing: event, daemon exit, reconnect refusal, roster/DB removal, rejoin with the
  byte-identical nkey, and no broad regex or stale output can satisfy an oracle.
- [x] Verify the child-leak gotcha has a named pre-injection process row and OS-child baseline, exact injection
  boundary, authoritative post-state, systemd-cgroup counterexample, expose data-plane baseline, and unconditional
  cleanup; distinguish product teardown from incidental tunnel loss.
- [x] Verify session-rm three phases through independent JetStream/SQLite/event oracles and accurately classify
  post-delete CONNECT denial versus the hermetic-only DELETING application gate.

## Drill 82 — C2 invite onboarding

- [x] Verify the default-off manifest listener gotcha has a healthy-broker positive control before refusal and
  that the labeled operator enable step mirrors documented production topology without masking the gap.
- [x] Verify seed publish/invite/join use a truly fresh agent, correct bootstrap pin/account provenance, real
  HTTPS Go-x509 fetch, restrictive filesystem modes, daemon start, ONLINE state, and roster-cache adoption.
- [x] Verify wrong-SAN and untrusted-CA negative arms traverse the product Go verifier, match specific x509
  failures, retain positive controls, and cannot be satisfied by incidental identity/config errors.
- [x] Verify forged/tampered invite residue assertions pin exact expected files and error cause; verify doctor and
  config-refresh success assert semantic verification rather than exit status alone.
- [x] Verify grow/roster generation convergence is polled beyond manifest resign throttling; document honest
  NOT-COVERED boundaries for stale grace and systemd-user lifecycle.
- [x] Verify all background join/proxy/service processes and netns-sharing sidecars are cleaned on every exit path.

## Documentation, verification, and handoff

- [x] Cross-check plan, README, roadmap, inventory, gotcha ledger, and internal review against implemented
  assertion counts, names, outcomes, limitations, and source behavior; flag overclaims and contradictions.
- [x] Run shell/Python syntax checks, `git diff --check`, focused command-tree/event checks, and any independent
  local negative tests needed to validate helper behavior.
- [x] Inspect simcluster server connection/resource state, build the current image, run drills 80/81/82 on fresh
  isolated instances, retain exact results, and clean all review instances even on failure.
- [x] Run a proportional regression wave (at minimum affected S2 drills plus a pre-existing tunnel/grow drill),
  and ensure retry behavior does not convert a product failure into green.
- [x] Write `s2-external-review.md` beginning with `Pass` or `Fail`, separating release-blocking findings,
  doubts, recommendations, independent evidence, limitations, and an explicit release recommendation.
- [x] Re-read every item, mark boxes only with evidence, stage all files via `git add -A`, then verify no
  unstaged/untracked files and `git diff --cached --check` passes.
