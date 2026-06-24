# D9 plan — Stage-A synth trajectory (audit trail)

> How `d9-plan.md` was produced. The finalized plan is `d9-plan.md`; this records the adversarial process.

## Workflow
Inline Stage-A workflow `d9-plan-draft` (all agents Opus 4.8, static fan-out, no model override): **5 drafters → 5 critics → 1 synth**, barriers between phases (critics need all drafts; synth needs all drafts + critiques). 11 agents, ~1.2M subagent tokens, ~30 min.

- **Drafters** (one per D9 dimension): (1) production cutover wiring, (2) `cluster init --from-existing`, (3) nats.conf takeover, (4) release hardening + test plan + GA exit, (5) safety/rollback/sequencing red-team.
- **Critics** (each cross-examines all 5 drafts): cross-dimension conflict, build-and-prove teardown correctness, migration safety+rollback, nats.conf completeness, test sufficiency + GA exit.
- **Synth**: combined into scope+boundary, ordered cutover checklist, from-existing flow, nats.conf takeover, test plan, release plan, riskiest-first sequencing, open questions, deferrals, risks.

Each agent was grounded in a main-process-verified DIGEST (cutover seam inventory from `serve.go`/`broker.go`/the six guard tests, migration/bootstrap facts from `node.go`/`storage.go`/`clusteroffline`, nats.conf facts from `install.sh`/`natscluster`, and the v1→v2 fleet landmine).

## What the critics caught (high-value)
- **Several drafter "open questions" were SETTLED contracts** the drafts re-derived (Draft 1) or violated (Draft 2): DB = single merged WAL (§3.8/§590 + `storage.go:81`), proxy-off in cluster mode (§16.4/§483), `bootstrapped` is a dead key (`fsm.go:18/275`), migration range 0008–0013. Synth correctly re-framed these as **AFFIRM, do not re-open** (plan §1).
- **DB-ownership** was the #1 BLOCKER across multiple critics: a separate FSM sidecar file (Draft 2) would strand `cluster_nodes`/`home_broker` away from the broker's reads → every D6 home directive mis-resolves. Resolved to the arch-mandated single-WAL merge + a grandfathered-mutator audit (`home.go:109` the smoking gun).
- **nats.conf fail-closed-on-`max_payload`** would brick every docs-compliant file-transfer broker (usage.md:970) → `TETHER_PASSTHROUGH` allowlist + harvest `ClientListen` from `host`+`port` (install.sh writes no `listen`) + `server_name` SSOT (silent D6/ACL break otherwise) + `include`-scan-before-parse.
- **Guard teardown** plans collided (4 drafts, 4 schemes); resolved to delete the six `ProductionWires`+selfcheck, KEEP every L-2 import guard verbatim, replace with a two-mode invariant + planted-regression self-check, same commit.
- **Cutover-then-prove sequencing**: prove all seams in tests (steps 1–7) before any live-box disk/`/etc` mutation (steps 8–10); guard teardown (7) only after the behavioral replacement is green.

## Main-process adjudications (finalizer; see `d9-plan.md` §5)
OQ-2 disk-lock = flock+bolt+**new SQLite-busy probe**; OQ-3 = halt-and-print restart; OQ-4 = FDE-absent advisory / secrets-unreadable FATAL (split); OQ-5 = seed `applied_index=0` ON CONFLICT DO NOTHING; OQ-6 = serveconf `cluster:` paths + identity DERIVED from secrets+existing conf.

**OQ-1 reversed by user directive (2026-06-23, "这是最终阶段了要做完所有事"):** the synth recommended splitting the §17 observability rows to a D9.1 leaf; the user directed that the final phase finish everything, so the **full §17 matrix is IN D9** (Step 10b / `d9-plan.md` §6) — no split, GA claims the complete matrix.

## Raw artifacts
Drafts/critiques/synth JSON: workflow run `wf_77c2c355-39f` (task `wtf47hbl1`). The synth's cutoverChecklist/sequencing/testPlan were adopted near-verbatim into `d9-plan.md` §2 with the finalizer rulings layered on top.
