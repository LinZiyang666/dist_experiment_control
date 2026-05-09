# Dead Code & Abstraction Drift Audit

## Verdict

13 findings (0 critical, 3 high, 6 medium, 4 low/nit).

The codebase has been mostly cleaned of phase-tag noise; what remains splits into two unequal piles:

1. **Stale "frp / frpc / frps / P6-6" vocabulary** — when P6 deviated from the spec (frp embed → in-process yamux tunnel), the README§"Notable deviations" got updated but a long tail of doc comments, `internal/agent/expose.go` package banner, and one outright dead import path (`internal/frpmgr`, doesn't exist) were missed. This is the most concentrated drift cluster.
2. **"lands in P<N>" comments where P<N> is shipped** — function docs and `--help` `Long:` strings still talk about future phases. Two are user-visible (`tether exec --help` says "PTY mode lands in P5"; `tether session rm --help` says "full delete in P7"). The rest are godoc-level.

Real dead code is small: two `var _ = X` rebuild-marker placeholders kept solely to preserve an otherwise-unused import (`errors` in `cmd/tether/error_hints.go`, `cobra` in `cmd/tether/agent_config_test.go`), one false-future godoc reference to a renamed/deleted test (`TestHandleAgentRoleIsDeniedUntilP4`), and a leaf `hashToken` re-implementation in `internal/tunnel` that duplicates `port.HashToken` (deliberate package-decoupling, but flag-worthy).

No exported symbol is unreferenced. No interface is mock-only or single-impl. `unused` linter reports clean. `unparam` flags 25 sites, but 22 are test helpers passing literal `"lab"` / `"lab-1"` (test-only parameterization-for-clarity, ignore) and 3 are real but minor (F12).

Architecture-stable anchors (`C.1 §6`, `H.3`, `F.4`, `J.4 § 安全约束`) are correctly preserved everywhere — the cleanup boundary the user drew between "stable spec ref" vs "phase-history tag" was respected by the previous two cleanup commits, this audit only finds what slipped through.

---

## Findings

### F1 — high — `internal/cli/natsconn.go:71-78` AgentName godoc says role is denied, code & all sites disagree

**Where**: `/home/weiland/projects/dist_experiment_control/internal/cli/natsconn.go:69-79` (godoc on `AgentName`)

**Issue**: The block reads:
> "In P3, auth_callout HARD-DENIES this role: the test `internal/authcallout/handler_test.go::TestHandleAgentRoleIsDeniedUntilP4` pins that. Real agent provisioning ... lands in P4 and **will re-enable the role** behind ..."

Two layers of drift:
- The cited test `TestHandleAgentRoleIsDeniedUntilP4` does not exist anywhere in the tree (`grep -rn DeniedUntilP4` returns only this comment). It was renamed/removed and the godoc reference dangles.
- The role is enabled and exercised every time an agent connects: `authcallout/handler.go:162` matches `tether-agent:` prefix, `agent.go:589` calls `AgentName` to set the connection name, every p4-p10 e2e test relies on it. The "will re-enable" future tense is now four phases out of date.

**Why it matters**: Anyone reading `natsconn.go` to understand the agent-connect path is told "this is denied, only the risk test uses it" — exactly inverted from reality. The dangling test name actively misleads anyone who tries to grep for it.

**Fix**: Replace the whole P3-history block with one sentence describing the current invariant: "AgentName produces the connection-Name `tether-agent:<sid>:<nid>` that `internal/authcallout.parseRole` decodes to authorize the agent role; the broker authorizes the connection only after `agent_provisioning` confirms the (sid, nid, fp) tuple."

---

### F2 — high — `internal/broker/expose.go:2-3` references `internal/frpmgr` package that doesn't exist

**Where**: `/home/weiland/projects/dist_experiment_control/internal/broker/expose.go:1-13` (package banner)

**Issue**: Banner asserts:
> "The actual TCP forwarding (frps embed + plugin hook for authorizing frpc's per-port token) **lives in internal/frpmgr**; this file only owns the SQLite-state + audit + agent-forward layer."

`find internal -type d -name frpmgr` returns nothing. The actual TCP forwarding lives in `internal/tunnel/` (yamux-over-TCP, ~400 LOC, no frp dependency). README§"Notable deviations" documents the spec deviation, but this banner was not updated when the deviation landed.

**Why it matters**: A reader following the file's own pointer will grep `internal/frpmgr`, get zero hits, and have to reverse-engineer the relationship between `broker/expose.go` ↔ data plane themselves. The pointer is worse than no pointer.

**Fix**: Replace `internal/frpmgr` with `internal/tunnel` and drop "frps embed + plugin hook for authorizing frpc's per-port token" — the actual mechanism is `tunnel.Server` calling the broker-supplied `TokenLookup` callback (`broker.go:91-94` wires it).

---

### F3 — high — `internal/agent/expose.go` & `agent.go:71-76` package banner promises "P6-6" frp adapter

**Where**:
- `/home/weiland/projects/dist_experiment_control/internal/agent/expose.go:6-12, 22-26` (package banner + `ExposeAdapter` godoc)
- `/home/weiland/projects/dist_experiment_control/internal/agent/agent.go:71-76` (`Config.ExposeAdapter` field doc)

**Issue**: Three clustered drifts:
- `expose.go:8-9`: "pluggable through the ExposeAdapter interface — P6-this file calls the adapter, **P6-6 ships the real frp-backed adapter**"
- `expose.go:23-24`: "P6 will provide a real implementation that adds/removes proxies on a frpc instance and reloads it"
- `agent.go:73`: "Production agents inject the **real frp-backed adapter (P6-6)**; in-process control-plane tests leave it nil so they exercise only the SQLite + state.json path without standing up frp."

The "P6-6" subdivision is internal phase plan that never happened (no review file references it, no commit history confirms it landed). The real implementation IS shipped: `internal/agent/tunnel_adapter.go::TunnelExposeAdapter`, wired in `cmd/tether/agent.go:131` — and it's yamux-tunnel-backed, not frp-backed.

**Why it matters**: A new reader sees "P6-6 will ship" and concludes the production path is missing → wastes time looking for it. Worse, when they grep for `frp-backed`, they'll find these comments but no implementation, and may be tempted to "complete" the supposedly-missing code.

**Fix**: Rewrite the three blocks against current reality: `ExposeAdapter` is satisfied by `TunnelExposeAdapter` (production, yamux) and test-only `recordingAdapter` (test/p6); nil disables the data plane for tests that only exercise control flow.

---

### F4 — medium — `cmd/tether/error_hints.go:103-105` `var _ = errors.New` placeholder keeps an unused import

**Where**: `/home/weiland/projects/dist_experiment_control/cmd/tether/error_hints.go:3-7, 103-105`

**Issue**: The file imports `"errors"` but never calls anything from it. To keep the import compiling, line 105 contains:
```go
// ensure we don't accidentally drop the errors import; brokerError
// chains via %w so callers can errors.Is downstream if they want.
var _ = errors.New
```
The `errors.New` is not invoked anywhere in this file. The `%w` chaining (cited in the comment) uses `fmt.Errorf`, which is in `fmt`, not `errors`. So the import + `var _` exist solely to anchor a documentation point. This is exactly the "rebuild marker that didn't get cleaned up" pattern in the user's brief.

**Why it matters**: Two costs. (a) Future readers see `var _ = errors.New` and assume there's a deeper reason (Go idiom for static interface checks etc.), wasting time. (b) `goimports` / `gofmt -s` won't drop the import because the marker keeps it referenced; the noise is self-protecting.

**Fix**: Drop the `import "errors"` line and the `var _ = errors.New` block. Move the "callers can errors.Is" hint into the godoc on `brokerErrorMessage` if it's worth preserving, or drop it (it's a property of `fmt.Errorf("%w")`, not specific to this function).

---

### F5 — medium — `cmd/tether/agent_config_test.go:134-137` `var _ = cobra.Command{}` placeholder keeps an unused import

**Where**: `/home/weiland/projects/dist_experiment_control/cmd/tether/agent_config_test.go:3-9, 134-137`

**Issue**: Same anti-pattern as F4. Test file imports `github.com/spf13/cobra` but never references any cobra symbol — the only `cobra.` token in the file is `var _ = cobra.Command{}` on line 137, with a comment explaining "_ keeps cobra referenced even if a future edit removes the cobra usage above". `pickFlagOrYaml(cmd, ...)` does take `*cobra.Command` as its parameter type, but Go's import rules don't require the cobra import for *that* — the type comes from `newAgentCmd()`'s declared return type.

**Why it matters**: Same as F4 — invents an "in case future" need that the type system doesn't actually require, leaving a marker that misleads about whether the import is load-bearing.

**Fix**: Drop the `cobra` import and the `var _ = cobra.Command{}` block. Tests will still compile; cobra arrives transitively through `newAgentCmd()`'s return type.

---

### F6 — medium — `cmd/tether/exec.go:33` and `cmd/tether/session.go:131` user-visible `--help` text references future phases that have shipped

**Where**:
- `/home/weiland/projects/dist_experiment_control/cmd/tether/exec.go:33` — `tether exec --help` Long: "PTY mode (vim, htop, progress bars with cursor moves) **lands in P5** as 'tether run'."
- `/home/weiland/projects/dist_experiment_control/cmd/tether/session.go:131` — `tether session rm --help` Short: "Tombstone session (owner-only; ACTIVE → DELETING; **full delete in P7**)"

**Issue**: P5 and P7 are both shipped. `tether run` IS available right now (`cmd/tether/run.go`); `session rm` DOES perform the full three-stage delete inline (`broker/sessions.go:147-170` calls `Tombstone` then `finalizeSessionRm`). An operator reading the help is told functionality is unimplemented when it's actually live.

**Why it matters**: User-visible regression: someone who runs `tether exec --help`, sees "PTY mode lands in P5", concludes interactive mode isn't ready, and never tries `tether run`. The session-rm one is less harmful (rm still works) but signals "this is incomplete, expect surprises."

**Fix**:
- `exec.go:33`: replace with "Use `tether run` for interactive PTY mode (vim, htop, etc.)."
- `session.go:131`: replace short with "Tombstone + cascade delete (owner only; `state=DELETING` then drops history stream and SQLite rows)."

---

### F7 — medium — `internal/broker/sessions.go:113-114` doc says stages 2/3 "land in P7", code immediately calls them

**Where**: `/home/weiland/projects/dist_experiment_control/internal/broker/sessions.go:113-115` (godoc on `handleSessionRm`)

**Issue**: Godoc claims "Stage 2/3 of H.3 land in P7". The function body that follows (line 167) explicitly invokes `b.finalizeSessionRm`, which executes phases ②③④ inline and is documented as such in `audit.go:51-60`. So the godoc directly contradicts the next 50 lines.

**Why it matters**: The exact case the user flagged: "注释说的行为和代码现在做的不一样 — 函数签名改了但 doc 没改". Anyone debugging an "rm fails halfway" issue will read the godoc, conclude only stage 1 runs, and not check `finalizeSessionRm`.

**Fix**: Replace with: "Owner-only; runs all four stages of H.3 inline: ① tombstone (DELETING), ②③④ delegated to `finalizeSessionRm`. Phase ① failures fail the rm; phases ②③④ failures log loudly but the rm still returns OK because the boot reconciler retries from `state=DELETING`."

---

### F8 — medium — `internal/proto/messages.go:148`, `internal/agent/agent.go:190-191`, `internal/session/session.go:4-6` "lands in P<N>" godoc for shipped phases

**Where**:
- `/home/weiland/projects/dist_experiment_control/internal/proto/messages.go:148` — `ExecReq` doc: "PTY mode (`run`) lands in P5; this is the simpler `exec` from P4"
- `/home/weiland/projects/dist_experiment_control/internal/agent/agent.go:190-191` — `Agent.Run` doc: "subscribe to `cmd.node.<nid>.*.req.forwarded` (P4 exec; **P5 will add run/PTY; P6 expose; etc.**)"
- `/home/weiland/projects/dist_experiment_control/internal/session/session.go:4-6` — package banner: "The full three-stage delete ... **lives in P7**; this package only implements stage 1"
- `/home/weiland/projects/dist_experiment_control/internal/broker/broker.go:13` — package banner: "`cmd.*.req.forwarded` (architecture C.4) command-routing **lands in P4**"
- `/home/weiland/projects/dist_experiment_control/internal/authcallout/handler.go:65` — `EmitEvent` doc: "Nil callback = no emission, fine for unit tests / **pre-P7 builds**"

**Issue**: All five describe future-tense phase work that is currently shipped. None contradict the code (unlike F7); they're just stale framing.

**Why it matters**: Cumulative — each individually is a nit, but together they create a "this is mid-development" feel that's no longer true. Newcomers waste time mentally translating "P5 will add" → "P5 added long ago, where is it?". Particularly bad in package banners (`broker.go:13`, `session.go:4-6`) which set the reader's mental model for the whole file.

**Fix**: Convert each to present-tense without phase numbers. Keep architecture anchors (`C.4`, `H.3`). Examples:
- `proto/messages.go:148`: "Non-interactive remote command. The interactive PTY equivalent is `RunReq` (see `pty.<sid>.<pid>.*` subjects)."
- `agent/agent.go:190-191`: "subscribe to `cmd.node.<nid>.*.req.forwarded` for exec / run / kill / expose / expose-rm / upgrade verbs"
- `broker/broker.go:13`: drop the line entirely; the file's structure makes the routing role obvious.

---

### F9 — medium — `internal/broker/broker.go:339-367` subscription block uses phase tags as section dividers

**Where**: `/home/weiland/projects/dist_experiment_control/internal/broker/broker.go:339, 347, 354, 360, 365`

**Issue**: The big subscription table is divided by `// P3 session management subjects.`, `// P4 control plane.`, `// P5 PTY control plane.`, `// P6 data-plane control (expose / expose-rm).`, `// P10 J.4 — \`tether node upgrade <nid>\``. The phase numbers are pure history; they tell you when each group was added, not what they do (the trailing description does that).

**Why it matters**: A nitty version of F8 — the phase prefix adds zero present-day information, and grouping by "when added" rather than "what it does" makes the next addition awkward (does a P11 cleanup go under "P10 J.4"? Add a new "P11" header?). Architecture-stable anchors (`J.4`, `C.4`) already cover the spec link.

**Fix**: Drop the `P<N>` prefix, keep the descriptive part: `// session management`, `// non-interactive exec / ps / node listing`, `// PTY run / kill`, `// expose / expose-rm`, `// node upgrade (J.4)`.

---

### F10 — low — `internal/tunnel/tunnel.go:484-487` re-implements `port.HashToken`

**Where**: `/home/weiland/projects/dist_experiment_control/internal/tunnel/tunnel.go:484` (private `hashToken`) vs `/home/weiland/projects/dist_experiment_control/internal/port/port.go:356` (exported `HashToken`)

**Issue**: Both compute `hex.EncodeToString(sha256.Sum256(rawToken))`, byte-identical. `port.HashToken` exists exactly for this — its godoc says "Public so callers outside this package ... can compute the lookup key from a frpc-supplied token without re-implementing the hash choice." But `internal/tunnel` (the actual frpc-replacement) re-implements it locally.

**Why it matters**: Mild — both are 3-line SHA256 hex helpers, the algorithm is unlikely to drift. But (a) it directly contradicts `HashToken`'s "public so callers don't re-implement" promise, (b) if the broker ever changes the hash (e.g. to argon2 to slow down brute-force from a leaked DB), the change has to be applied in two places, and the second-place miss won't fail any test — the broker would just reject every tunnel registration silently.

**Fix**: One of:
- **(preferred)** Have `tunnel/tunnel.go` import `internal/port` and use `port.HashToken`. The decoupling argument ("tunnel is a leaf") is weak — the rest of `tunnel` already knows about ports (TokenLookup callback signature is `func(sid, nid, port int, hash string) error`).
- **(alternative)** Move both into a tiny new package `internal/portsec` (or keep `port.HashToken` and update `port.HashToken`'s godoc to acknowledge `tunnel` re-implements deliberately). Either way, the duplication should be flagged in `port.HashToken`'s comment so the next change-maker checks both sites.

---

### F11 — low — `internal/agent/agent.go:621-622` (`readBootID`) carries P2/P8 phase narrative in helper godoc

**Where**: `/home/weiland/projects/dist_experiment_control/internal/agent/agent.go:620-622`

**Issue**: Comment reads: "Used in **P8's** reconciliation (architecture G.1) for PID-reuse detection; recorded already in **P2** so the column is never NULL once populated."

The function does what `/proc/sys/kernel/random/boot_id` does. The G.1 architecture anchor is fine. The "P8/P2" tags add zero behavioral info; a reader who isn't watching the project's git history learns nothing from "recorded already in P2" — they learn what they need from "the column is never NULL once populated".

**Why it matters**: Same class as F8/F9; tiny per site, accumulates.

**Fix**: "Used by reconciliation (architecture G.1) for PID-reuse detection. Captured at first agent boot so `nodes.boot_id` is never NULL once populated."

---

### F12 — low — `unparam` reports 3 production-code unused-result / always-nil-error sites

**Where**:
- `/home/weiland/projects/dist_experiment_control/internal/agent/agent.go:565` — `(*Agent).buildConnOptions() ([]nats.Option, error)`: error is always nil
- `/home/weiland/projects/dist_experiment_control/internal/agent/expose.go:46` — `handleExposeForwarded(nc *nats.Conn, ...)`: `nc` unused
- `/home/weiland/projects/dist_experiment_control/internal/agent/expose.go:97` — `handleExposeRmForwarded(nc *nats.Conn, ...)`: `nc` unused
- (also `internal/agent/run.go:293` `handleKillForwarded`, `internal/agent/upgrade.go:45` `handleUpgradeForwarded` — same `nc` unused pattern)

**Issue**: Two sub-issues:
- `buildConnOptions` advertises `(opts, error)` but the error path is dead — the only failure point (`nkeys.FromSeed` inside `sigCB`) errors at sigCB call time, not at build time. The `error` return is misleading.
- The four `handle*Forwarded` methods take `nc *nats.Conn` purely for dispatcher uniformity (`exec.go:30-58` calls them in a switch, all with `(nc, msg)`). They reply via `msg.Respond` which doesn't need `nc`. Genuinely unused.

**Why it matters**: Low. The `nc` uniformity is defensible (one switch → all same signature; renaming to `_` per-handler would actually obscure the dispatch pattern). The `buildConnOptions` error is a small lie — callers (`connectNATS`) treat it as fallible, adding error-handling code that can't fire.

**Fix**:
- `buildConnOptions`: drop the `error` return, remove the now-trivial err handling in the one caller. (Or keep the signature if there's a planned source of failure — but the comment doesn't suggest one.)
- `handle*Forwarded`: leave as-is; the dispatcher uniformity argument is real. Optionally, add a one-line comment on the dispatcher acknowledging "`nc` is passed for signature uniformity even when the callee replies via msg.Respond" so future readers don't try to "fix" it.

---

### F13 — nit — `docs/architecture.md:2037` references `internal/frpmgr` package that doesn't exist

**Where**: `/home/weiland/projects/dist_experiment_control/docs/architecture.md:2037`

**Issue**: Architecture spec lists `internal/frpmgr` as the broker-side frp embed + agent-side frpc subprocess. Package was never created; the deviation is captured in `README.md:207-213`§"Notable deviations" and the actual code is `internal/tunnel`.

**Why it matters**: Strictly speaking, architecture.md is the design doc and may legitimately stay aspirational while README captures shipped reality — that IS the convention this project follows for a few other deviations. Borderline.

**Fix**: Add a one-line "shipped as `internal/tunnel`, see README§Notable deviations" annotation at line 2037 so a reader walking from architecture → code doesn't dead-end.

---

## Out of scope / already fine

Worth recording so the next auditor doesn't re-investigate:

- `unused` linter clean (0 issues across `./...` with tests=true).
- All exported names referenced ≥2 times (no orphan API surface).
- Two production interfaces (`agent.ExposeAdapter`, unexported `proc.scanner`) both have ≥2 implementations — not premature.
- Duplicate `urlAllowed` (broker + agent) is intentional (defense-in-depth, called out explicitly in `agent/upgrade.go:154-158`).
- TODO/FIXME/XXX/HACK markers: zero in `*.go`. The two cleanup commits (`94275f4`, `321c901`) did good work here.
- `internal/serveconf.FrpSection` keeps the `frp:` YAML key for backward compat with architecture A.3 — operators' broker.yaml files use it. The Go-side struct name is the only stale piece (`FrpSection` could be `TunnelSection`), but renaming would force a yaml schema change too. Leave it.
- Phase tags inside test file paths/names (`test/p4/`, `test/p7/`) are the architecture's phase-aligned acceptance test convention (P11 spec line 2141), not residue.
- `var _ embed.FS` in `internal/storage/storage.go:39` is a real `//go:embed` directive, not a placeholder — keep.
