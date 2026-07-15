# S3–S5 (G-A) — scope handling + verifiable technical constraints

> **Round-6 correction (R6-M2).** The round-5 version of this file claimed an *owner authorization* to NOT-COVER
> drill 71's drain arms, quoting a conversational answer the external reviewer cannot see. A developer-authored
> file cannot authenticate an owner decision, and past conversational statements are not acceptance authority.
> **This file therefore claims NO owner scope-change.** It documents (a) how locked behaviors that tether cannot
> currently satisfy are HANDLED — as explicit RED failing gates, not silent NOT-COVERED — and (b) the ONE
> constraint that is independently verifiable from the product CLI itself. Nothing here rescopes a locked item.

## Handling principle (no owner approval needed, because nothing is exempted)

A locked acceptance behavior that tether cannot currently satisfy is **exposed as a HARD failing gate (RED)** that
affects the release verdict — never downgraded to a GREEN warn / NOT-COVERED. This is the mandate ("测试的目的是
暴露问题，不是给 tether 擦屁股") and matches the reviewer's R6-M1/M2 requirement that failures affect the verdict.
Round-5's `measure-and-record` (74 B-dp / Arm-C) and self-authored `owner-accepted` NOT-COVERED (71 B/E/G/F) were
both wrong — they turned failed locked behavior GREEN. Reverted:

- **drill 74** — the moved-exit data-plane closure (B-dp) and the auto-rebalance-on-return EFFECT (C-auto) are now
  **HARD assertions**: RED when the #33-family moved-exit stranding manifests or the auto path does not fire. No
  data-plane / auto-effect carve-out is claimed.
- **drill 71** — the drain-migrate (arm B) is now a **HARD failing gate**: `cluster drain brk3` MUST migrate a
  rebuild-ON expose to a survivor voter that serves; RED when blocked by the empirically-reproduced walls (#29
  homeForExpose un-homed fallback / #31 lingering grow op refusing the drain / intermittent agent-tunnel-to-non-
  leader). The FIXTURE establishment is also HARD (RED if agt→brk3 never establishes). E/G/F need a *successful*
  drain as their precondition, which arm B RED-exposes as blocked. No owner rescope is claimed for any of these.

Everything the owner's standing instruction ("全部实现，不 rescope") required to be *implemented* was implemented
(drill 32 §8.4 + real ctl; drill 74 fail-closed snapshot + 1/1/1 + all-three SS legs + rc-checked rebalance +
non-vacuous negative control; drill 71 rebuild-off crash across a real injection). Where tether cannot satisfy a
locked behavior, the drill RED-exposes it — which is *attempting + failing loudly*, the opposite of a silent rescope.

## The one independently-verifiable technical constraint — raw sys.events have no operator reader

`tether admin` exposes `audit`, `evict`, `nodes`, `sessions` — there is **no** `events` / `sys.events` read
subcommand (verifiable by running `tether admin --help` and `tether admin events --help` against any broker). So a
RAW EVENT assertion — "`proxy_auto_rebalanced` fired exactly once", "`home_reassign_succeeded` was emitted",
"cluster-mode revoke emitted no `proxy_keyset_changed` (#30)" — **cannot be READ, therefore cannot be asserted**.
This is a factual product-observability limitation, not a scope decision. Per the reviewer (R6-M1), this covers
**ONLY raw event assertions** — never a data-plane or auto-EFFECT criterion:

- drill 74 — the raw `proxy_auto_rebalanced` count==1 EVENT stays NOT-COVERED; the auto EFFECT (distribution
  auto-evens) is the HARD C-auto assertion (RED if it does not happen).
- drill 71 — the raw `home_reassign_*` / `broker_down_rehome_summary` / `expose_rehomed` / `rehome_stalled` events
  stay NOT-COVERED; the crash-strand/return/rebuild-off EFFECTS are asserted via curl-through + explain + cluster
  status reachability, and the drain-migrate is the HARD arm-B gate.
- drill 73 — the absence of `proxy_keyset_changed` on a cluster-mode revoke (#30) stays NOT-COVERED; the revoke's
  data EFFECT (/sub 404) is asserted.
