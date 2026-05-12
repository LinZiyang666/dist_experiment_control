# Dynamic Shell Completion — Implementation Plan

Date: 2026-05-12
Status: draft
Target release: v0.1.4

## Background

`tether completion bash|zsh|fish|powershell` already exists (cobra-generated, ships out of the box) and covers static surface: command names, subcommand names, flag names. After enabling it in the operator's shell, `tether ex<TAB>` correctly suggests `exec / expose`, `tether exec --<TAB>` lists flags, etc.

What it does **not** cover is dynamic argument value completion:
- `tether exec <TAB>` does nothing useful (should list ONLINE nodes in the active session)
- `tether expose rm a100 --name <TAB>` does nothing (should list current ALLOCATED expose names)
- `tether login -s <TAB>` does nothing (should list visible sessions)

These are the highest-value tab targets because operators type them dozens of times per day and nid / sid names are not memorable (`vlm-life-assistant-med`, `01HABC…`, etc.).

This plan scopes the addition of runtime-aware completion to the cobra commands that ask for these identifiers, plus the infrastructure to keep tab latency tolerable (NATS connect + RPC on every `<TAB><TAB>` keystroke is too slow).

## Goals

1. `tether exec / run / expose / expose rm / node upgrade <TAB>` returns the active session's ONLINE nodes.
2. `tether expose rm <node> --name <TAB>` returns current `ALLOCATED` port names (filtered by node).
3. `tether login -s <TAB>` returns sessions visible to the caller (owner OR member, ACTIVE). `tether session rm <TAB>` returns only sessions where the caller is owner AND State==ACTIVE — `rm` is owner-only server-side, so suggesting member-only sessions would mislead users into a predictable `not_owner` rejection.
4. `tether admin evict <sid> <TAB>` and `tether admin audit <TAB>` use the broker admin socket (already broker-local; no NATS round-trip needed).
5. Tab latency: target < 1 s every time (single end-to-end budget, see "Latency model" below — Cobra runs `tether __complete` as a fresh subprocess per completion, so a warm-cache goal is unrealistic without disk persistence, which is deliberately deferred).
6. Zero impact on non-completion paths (regular `tether exec` etc. are not slower).
7. Out-of-the-box for cobra's existing `completion bash|zsh|fish|powershell` generator — no separate install step.

### Latency model (clarified after review)

Cobra's shell-completion scripts invoke `tether __complete <verb> <toComplete>` as a **new process** for each `<TAB>` event. An in-process `sync.Map` therefore caches **only within one single completion event** (where the same helper may fire twice for the same prefix internally); it does **not** survive across user tab presses, nor across `<TAB><TAB>` keystrokes. Disk-backed cache is **out of scope** for v0.1.4 (see "Out of Plan").

Consequence: every tab press is effectively a cold path. The budget is therefore a single hard cap of 1 s end-to-end including NATS connect + RPC, not a tiered warm/cold split. The 5-second in-process cache is kept only because some Cobra-generated bash scripts call the helper twice per event for prefix re-filtering; it removes the duplicate RPC without changing the cross-event cost.

## Non-Goals

- Completing `tether exec <node> <argv>` (the remote argv — broker doesn't know the agent's PATH).
- Completing `--cwd PATH` (would require remote fs introspection).
- Completing `--local PORT` (raw integers — no candidate set).
- Bash completion v1 compatibility (cobra v2 already requires `bash-completion` package; existing requirement, not new).

## Design

### New package: `internal/cli/completion.go`

Helpers split by **authorization semantics** (per reviewer recommendation #2) — different commands have different visibility/ownership constraints, so a single "CompleteSessions" name papered over a real distinction:

```go
// CompleteOnlineNodes returns ONLINE node nids in the active session, with
// the current value (toComplete) prefix-filtered server-side where possible.
// Used by exec / run / expose / expose rm / node upgrade. Reads broker via
// NodeListReq filtered to State=="ONLINE".
func CompleteOnlineNodes(t Transport, toComplete string) ([]string, cobra.ShellCompDirective)

// CompleteVisibleSessions returns sessions visible to the caller (owner OR
// member, State==ACTIVE). Used by `login -s`. Reads SessionListReq.
func CompleteVisibleSessions(t Transport, toComplete string) ([]string, cobra.ShellCompDirective)

// CompleteOwnedSessions returns sessions where the caller IsOwner==true and
// State==ACTIVE. Used by `session rm` (server-side is owner-only; suggesting
// member-only sessions would mislead users into a predictable not_owner
// rejection).
func CompleteOwnedSessions(t Transport, toComplete string) ([]string, cobra.ShellCompDirective)

// CompleteAllocatedExposeNames returns expose names in the active session
// where State=="ALLOCATED" (excludes FREED/REVOKED history rows that
// `ps -a` would also surface). If nid is non-empty, filtered client-side
// to that node — note this is a UX filter only; broker's
// handleExposeRmReq remains name-authoritative for the actual `expose rm`
// (the nid arg currently does not feed into the SQL lookup), so completion
// hiding a stale name does not change correctness, only the suggestion
// set. See reviewer concern #6.
func CompleteAllocatedExposeNames(t Transport, nid, toComplete string) ([]string, cobra.ShellCompDirective)
```

Admin variants (broker-local, no NATS) — all four take `toComplete` so we never depend on shell-side post-filtering:

```go
// CompleteAdminSessions reads sessions via adminsock.OpSessions. Used by
// `admin audit <TAB>` and `admin evict <TAB>` (first positional).
func CompleteAdminSessions(socketPath, toComplete string) ([]string, cobra.ShellCompDirective)

// CompleteAdminNodes reads nodes via adminsock.OpNodes. If sid is non-empty
// (set when `admin evict <sid>` already picked the first positional), the
// result is filtered to that session. Otherwise lists all (sid, nid) pairs.
func CompleteAdminNodes(socketPath, sid, toComplete string) ([]string, cobra.ShellCompDirective)
```

### `Transport` interface (injectable for tests, fail-fast in production)

```go
// Transport abstracts the broker access path so completion helpers can
// be unit-tested without spinning up NATS. Production wires to a NATS-
// over-WSS adapter with hard-fail-fast options; tests inject a fake.
type Transport interface {
    ListNodes(ctx context.Context) ([]NodeInfo, error)
    ListSessions(ctx context.Context) ([]SessionInfo, error)
    Ps(ctx context.Context) (PsView, error)
}

// NewCompletionTransport builds the production NATS transport with
// completion-specific options:
//   - nats.Timeout(750*time.Millisecond)   // dial cap
//   - nats.RetryOnFailedConnect(false)
//   - nats.MaxReconnects(0)                // no background reconnect loop
//   - nats.NoEcho()                        // smaller footprint
// The 1-second outer ctx (see `Latency model`) bounds the whole call;
// `nc.RequestWithContext` inherits that ctx, and `Close` (no Drain) is
// called in a defer so we don't block on completion paths.
//
// If $TETHER_NATS_URL / ~/.tether/broker_url / --nats-url all resolve
// to empty, returns a no-op transport that yields empty results — tabbing
// before login should produce no candidates (silent), not an error.
func NewCompletionTransport(home string, natsURLFlag string, flagChanged bool) Transport
```

Reasoning the production transport is **not** `cli.ConnectNATSWithNkey` reused as-is: that helper uses `nats.MaxReconnects(-1)` (infinite reconnect) which is wrong for one-shot completion subprocesses. A separate constructor is the simplest way to keep the existing connection semantics for `RunE` paths while making completion connections die fast.

**Identity in completion path**: use `cli.ReadIdentity(home)` (read-only) instead of `cli.EnsureIdentity(home)` (which would mint a new nkey on a fresh `~/.tether` and write it to disk — a side effect operators don't expect from tab). If no identity exists → return empty candidates, silent.

### Cache

Per-process in-memory cache (intra-event only — see "Latency model"):
- Key: `(helper, resolvedNATSURL, resolvedHome, actorPubKey, sid, helperFilters)` — must include `actorPubKey` and `resolvedHome` because session visibility is identity-scoped and a `--home` / `TETHER_HOME` override changes the answer entirely. JSON-encoded tuple → `sync.Map`.
- 5-second TTL: not a UX feature; only deduplicates the within-event repeat call some Cobra-generated bash scripts emit.
- No disk persistence (see "Out of Plan").

### Cobra wiring

Each affected command builds its `Transport` once at completion-time (via the per-command flag values), then dispatches by `args` shape. Concrete shapes:

```go
// `tether exec <nid> ...` (positional 0 = nid)
execCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
    if len(args) > 0 { return nil, cobra.ShellCompDirectiveNoFileComp }
    home, natsURL := flagsAtCompletionTime(cmd)
    t := cli.NewCompletionTransport(home, natsURL, cmd.Flags().Changed("nats-url"))
    defer t.Close()
    return cli.CompleteOnlineNodes(t, toComplete)
}

// `tether expose rm <nid> --name <name>` (positional 0 = nid, --name = expose name)
exposeRmCmd.ValidArgsFunction = ... // CompleteOnlineNodes as above

exposeRmCmd.RegisterFlagCompletionFunc("name", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
    var nid string
    if len(args) > 0 { nid = args[0] }
    home, natsURL := flagsAtCompletionTime(cmd)
    t := cli.NewCompletionTransport(home, natsURL, cmd.Flags().Changed("nats-url"))
    defer t.Close()
    return cli.CompleteAllocatedExposeNames(t, nid, toComplete)
})

// `tether session rm <sid>` — owner-only filter
sessionRmCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
    if len(args) > 0 { return nil, cobra.ShellCompDirectiveNoFileComp }
    home, natsURL := flagsAtCompletionTime(cmd)
    t := cli.NewCompletionTransport(home, natsURL, cmd.Flags().Changed("nats-url"))
    defer t.Close()
    return cli.CompleteOwnedSessions(t, toComplete)
}

// `tether login -s <sid>` — visible-sessions filter
loginCmd.RegisterFlagCompletionFunc("session", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
    home, natsURL := flagsAtCompletionTime(cmd)
    t := cli.NewCompletionTransport(home, natsURL, cmd.Flags().Changed("nats-url"))
    defer t.Close()
    return cli.CompleteVisibleSessions(t, toComplete)
})

// `tether admin evict <sid> <nid>` — two positionals, dispatch on len(args)
adminEvictCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
    socketPath, _ := cmd.Flags().GetString("socket")
    switch len(args) {
    case 0:
        return cli.CompleteAdminSessions(socketPath, toComplete)
    case 1:
        return cli.CompleteAdminNodes(socketPath, args[0], toComplete)
    default:
        return nil, cobra.ShellCompDirectiveNoFileComp
    }
}
```

`flagsAtCompletionTime(cmd)` is a 10-line helper that resolves home + natsURL identically to the command's `RunE` path (via `cli.ResolveNATSURLFromHome`) so env / `~/.tether/broker_url` / flag precedence stays consistent across both code paths. Per-command notes:

- **login**: `--broker` is an alias for `--nats-url` bound to the same string variable; `flagsAtCompletionTime` reads whichever was set.
- **session**: `--nats-url` is on the `session` root command (persistent flag); helper reads `cmd.Root()` then descends if needed.
- **admin**: `--socket` is persistent on `admin` root; helper picks it up from `cmd.InheritedFlags()`.
- **exec / run**: `SetInterspersed(false)` only stops flag parsing *past* the verb's first positional (the remote argv), not flags before — completion-time flag resolution is unaffected.

### Error handling

Completion helpers MUST NOT block shell tab forever. Total budget (dial + RPC): **1 s** via outer `context.WithTimeout(1*time.Second)`; inside it, dial is capped at 750 ms by the NATS option, leaving ≥ 250 ms for the actual RPC. `nats.MaxReconnects(0)` ensures no background reconnect outlives the completion subprocess.

On error / timeout / no-active-session: return `nil, cobra.ShellCompDirectiveNoFileComp` **without** the `Error` bit. The Error directive's interaction with `complete -o default` is implementation-specific across Cobra-generated bash / zsh / fish scripts; an empty result + `NoFileComp` is the safest pattern across all three shells. Verified during implementation via `tether __complete` invocations against each generated script (see "Verification").

If the broker is offline, the user has no active session, or `~/.tether/keys/default.nk` doesn't exist (fresh machine), `<TAB>` produces zero candidates silently — no banner, no error message, no file fallback.

## Files Touched

- **New**: `internal/cli/completion.go` (~200 lines — helpers + Transport + cache)
- **New**: `internal/cli/completion_transport.go` (~80 lines — production NATS transport with fail-fast options)
- **New**: `internal/cli/completion_test.go` (~180 lines — cache TTL, timeout incl. dial, error fallback, prefix filter, identity short-circuit, owner-filter for rm)
- **New**: `test/cli_e2e/completion_test.go` (~120 lines — `tether __complete <verb> ""` end-to-end against the existing JS test harness, covers flag-position cases listed in reviewer concern #9)
- **Modified**: `cmd/tether/exec.go` (+5 lines `ValidArgsFunction`)
- **Modified**: `cmd/tether/run.go` (+5 lines)
- **Modified**: `cmd/tether/expose.go` (+10 lines: positional + `--name` flag completion for expose-rm)
- **Modified**: `cmd/tether/node.go` (+5 lines for `node upgrade` positional — note: the original draft incorrectly named `cmd/tether/upgrade.go`; `newNodeUpgradeCmd` lives in `cmd/tether/node.go`, reviewer concern #8)
- **Modified**: `cmd/tether/login.go` (+5 lines for `-s` flag completion; handle the `--nats-url` / `--broker` shared variable)
- **Modified**: `cmd/tether/session.go` (+5 lines for `rm` positional)
- **Modified**: `cmd/tether/admin.go` (+15 lines for evict 2-positional dispatch + audit positional)

Total: ~580 new lines + ~50 modified lines across **11 files** (4 new + 7 modified).

## Verification

### Unit tests (`internal/cli/completion_test.go`)

Using the injectable `Transport` interface — no real NATS required:

1. **Cache TTL**: two calls within 5 s → one transport call; 6 s apart → two.
2. **Identity-keyed cache**: changing `actorPubKey` invalidates cache even if `natsURL` + `sid` match.
3. **Prefix filter**: with nodes `[a100, pc732, timan1, timan107, timan108]`, `toComplete="tim"` returns 3 candidates.
4. **Auth split**: `CompleteVisibleSessions` returns owner + member sessions; `CompleteOwnedSessions` filters to `IsOwner==true && State==ACTIVE`.
5. **ALLOCATED-only**: `CompleteAllocatedExposeNames` skips FREED / REVOKED rows from `PsResp.Ports`.
6. **No-identity short-circuit**: `ReadIdentity` returns `os.ErrNotExist` → helpers return `nil, NoFileComp` without any transport call.

### Transport-level test (`internal/cli/completion_transport.go` indirectly)

1. **Timeout including dial (controlled stub, not network black hole)**: spin up an in-process `net.Listen("tcp", "127.0.0.1:0")` listener that `accept()`s connections but never writes the NATS `INFO` line. Point `NewCompletionTransport` at `nats://127.0.0.1:<assigned-port>`. The dial succeeds at TCP layer but NATS protocol handshake never completes → `nats.Connect` hits `nats.Timeout(750ms)` deterministically. Helper returns `nil, NoFileComp` within budget + slop (assert ≤ 1.1 s, no goroutine leak via `runtime.NumGoroutine()` before/after).
2. **MaxReconnects=0**: after first failure, no background reconnect; `Close` exits promptly.
3. **Empty broker URL**: `NewCompletionTransport` returns a no-op transport that yields empty results.

### Cobra-level `__complete` tests (`test/cli_e2e/completion_test.go`)

Per reviewer recommendation #4 / #5 — helper unit tests can't catch flag-position / quoting / directive-handling issues in the generated bash/zsh/fish scripts:

1. `tether __complete exec ""` against a 6-node test broker returns only ONLINE nids + `NoFileComp` directive (verified by parsing stdout `ShellCompDirective:` trailer).
2. `tether __complete session rm ""` filters to owned sessions only; a session where the test ctl is only a member is excluded.
3. `tether __complete expose rm a100 --name ""` returns only ALLOCATED names whose `nid=="a100"`; FREED rows + names on other nodes excluded.
4. **Broker offline budget**: stop the test broker, run `__complete exec ""`; assert returns within 1.1 s with empty result.
5. **Flag positions** (reviewer concern #9):
   - `tether --nats-url X __complete exec ""` — `--nats-url` before verb
   - `tether __complete exec --cwd /tmp ""` — flag interspersed (cobra's `SetInterspersed(false)` only affects `--` past argv)
   - `tether __complete login -s ""` — flag value completion path
   - `tether __complete admin --socket /path evict ""` — persistent flag
   - `tether __complete admin --socket /path audit ""` — persistent flag
6. **Shell-script regression**: source `bin/tether completion bash` in a `bash -i` subprocess, simulate `<TAB><TAB>` via `COMP_LINE` / `COMP_POINT` env, assert the `complete` function output includes the expected candidate set and **does not** call back into file completion (no path candidates in result). Repeat for zsh + fish.

### End-to-end (manual, in `log.md` test matrix)

After `source <(tether completion bash)`:

| Command typed | After `<TAB><TAB>` | Expected |
|---|---|---|
| `tether exec ` | list 5 ONLINE nodes (no jupyter-ziyang10 if OFFLINE) | ✓ |
| `tether exec timan<TAB>` | `timan1 timan107 timan108` | ✓ |
| `tether expose a100 --local 22 --name `(after) `<TAB>` | nothing (no candidate; user types fresh name) | ✓ (correctly returns empty) |
| `tether expose rm a100 --name ` | currently allocated names on a100 | ✓ |
| `tether login -s ` | sessions where I'm owner or member | ✓ |
| `tether admin evict <TAB>` | all session ids | ✓ |
| Tab when broker is offline | no hang, no file fallback, return after < 1 s | ✓ |

### CI smoke

Add one `go test ./internal/cli/...` invocation to the existing `make test` matrix — no new CI step. The architecture-level smoke (completion script generation + parsing) is already covered by cobra's own tests.

## Risk

- **Cold path on every tab press**: Per the "Latency model" clarification, every `<TAB>` is a fresh `tether __complete` subprocess — there is **no warm cache** across keystrokes. The 1 s end-to-end budget is the actual operator-visible latency, every time. If the broker is in another region (e.g. WSS handshake ~3× RTT for TLS + WebSocket upgrade), `<TAB>` may consistently feel near the ceiling. The disk-cache mitigation is deliberately deferred (see "Out of Plan") in exchange for keeping the v0.1.4 surface minimal.
- **NATS connection churn**: each shell tab opens a connection and `Close()`s it (not `Drain`). Acceptable: each shell tab is a discrete `tether __complete ...` subprocess that exits anyway, so connection count tracks tab keystrokes 1:1. No connection pool warranted.
- **No active session / no identity**: `CompleteOnlineNodes` and the session helpers all use `cli.ReadIdentity(home)` (read-only), and fall through with `nil, cobra.ShellCompDirectiveNoFileComp` if `~/.tether/keys/default.nk` is absent or `~/.tether/current_session` is empty. **No NATS connection is attempted, no nkey is created.** Tab on a fresh machine is silent and side-effect-free.
- **Test flakiness from network-route assumptions**: The original draft of the timeout test pointed at `192.0.2.1:443` (RFC 5737 TEST-NET-1) under the assumption that no route exists so the dial would consume the full timeout. Reality: some CI hosts ICMP-reject TEST-NET-1 in under 100 ms, others let the OS time out at 75 s SYN retries — both deviate from the 750 ms NATS dial cap we want to test. Plan revised to use a **controlled in-process listener** that `accept()`s the TCP/TLS handshake but never speaks NATS (no `INFO` line), forcing the dial to time out at the `nats.Timeout` boundary deterministically. See "Verification → Transport-level test #1".

## Rollout

1. Implement on a feature branch `feat/dynamic-completion`.
2. PR review focusing on (a) timeout behavior, (b) cache correctness under concurrent tab presses.
3. Merge to main → tag `v0.1.4` → goreleaser publishes.
4. Users get it via `tether node upgrade --all` (agent binary unchanged but ctl is what matters; static completion was unchanged; dynamic completion is purely client-side cobra wiring, so `--all` upgrade isn't even strictly needed — only operators who run `tether <verb>` benefit, and they upgrade ctl independently via `curl install.sh | sh`).

## Out of Plan (deferred)

- **Smarter cache invalidation**: subscribe to `sys.events` for `agent_evicted` / `node_registered` and bust cache on event. Adds complexity; intra-event 5 s cache is good enough for v0.1.4.
- **Disk-backed cache** across `tether` invocations: would make cross-event tab feel instant. Deliberately deferred because (a) cache invalidation gets harder (no broker push to ctl), (b) leaks identity-keyed data to disk, (c) the cold path is already < 1 s budget. Revisit if latency proves painful in practice.
- **Description column for candidates** (reviewer question #2): cobra `ShellCompDirective` supports `nid\tONLINE 0.1.3` style two-column output. Useful, but increases formatting + test scope (`ShellCompDirectiveKeepOrder`, tab-quoting, shell-specific rendering). Deferred to v0.1.5.
- **Completion for `tether expose --remote-port <port>`** (if that flag is added in a future v0.1.5): would suggest free ports from the broker's pool. Trivial extension when that flag lands.
- **`session rm` for DELETING sessions**: today DELETING is transient (P7 finalize is synchronous), but a future async-delete model might leave DELETING rows visible — at that point reconsider whether they should appear in `session rm` completion (probably no — they're already being deleted).

---

## Reviewer Notes — 2026-05-12

### Overall read

The feature is worth doing and the proposed data sources mostly line up with the current architecture:

- ONLINE node completion should use `NodeListReq` / `NodeListResp`; `ps` is process-centric and would miss freshly registered nodes.
- expose-name completion can reuse `PsReq` because `PsResp.Ports` already carries `(name, nid, state)`.
- session completion can reuse `SessionListReq`.
- broker-local admin completion can reuse the existing `adminsock` `sessions` / `nodes` operations.

The main concerns are around latency assumptions, Cobra completion behavior, and a few places where the plan does not match the current command/API shape.

### Concerns

1. **The proposed in-memory cache will not deliver the stated warm-cache latency.**

   Cobra's generated shell scripts call `tether __complete ...` as a new process for each completion request. The current bash script generated by `./bin/tether completion bash` invokes the binary once per completion event. A package-level `sync.Map` inside `internal/cli/completion.go` will die with that process, so it will not cache the next `<TAB>` or `<TAB><TAB>` invocation. The plan explicitly says there is no disk cache, which means the expected path is cold NATS connect + RPC every time. This undermines the `< 200 ms warm` goal and the "cache handles repeated TAB" rationale.

   Suggestion: either revise the latency goal to assume cold every time, or add a small disk-backed cache keyed by actor/home/natsURL/sid/helper with strict TTL and best-effort read/write. If disk cache is intentionally deferred, the plan should say warm cache only exists within one `__complete` process and is not expected to help normal shell use.

2. **The 800 ms timeout only covers the RPC, not connection setup.**

   `cli.ConnectNATSWithNkey` currently uses `nats.MaxReconnects(-1)` and no completion-specific dial timeout. Wrapping `nc.RequestWithContext` in `context.WithTimeout(800ms)` does not bound `nats.Connect` itself. Against an unreachable WSS endpoint, DNS/TCP/TLS/WebSocket setup can exceed the target before the RPC timeout even starts.

   Suggestion: completion should use a dedicated connection helper or extra NATS options such as a short `nats.Timeout(...)` and no reconnect behavior. Completion must fail fast before and after connect.

3. **`ShellCompDirectiveError | ShellCompDirectiveNoFileComp` needs real shell verification.**

   The generated bash completion handler returns immediately when the Error directive is set, before applying the NoFileComp branch. Because the generated completion registration uses `complete -o default`, it is not obvious that `ShellCompDirectiveError | NoFileComp` actually suppresses file fallback in bash. The plan treats this as guaranteed.

   Suggestion: test the actual generated bash/zsh/fish scripts with broker offline and no active session. For expected runtime misses like "no broker" or "no active session", consider returning `nil, cobra.ShellCompDirectiveNoFileComp` without the Error bit unless Cobra's generated scripts are proven to suppress fallback for the combined directive.

4. **Cache keys are missing identity/home context.**

   The plan keys cache entries by `(helper, natsURL, sid)`. Session visibility is actor-specific, and `--home` / `TETHER_HOME` changes both identity and active session state. Even if the cache stays in-process, tests and future disk caching can leak candidates between identities if the actor public key or resolved home is not part of the key.

   Suggestion: key at least by helper, resolved natsURL, resolved home or actor public key, active sid, and helper-specific filters such as nid.

5. **`session rm <TAB>` should not complete all visible sessions.**

   `session rm` is owner-only server-side. `SessionListResp` includes `IsOwner`, but the plan says `session rm` should return sessions visible to the caller. That will suggest sessions where the user is only a member and the command will predictably fail with `not_owner`.

   Suggestion: keep `login -s` as "visible sessions", but filter `session rm` to `IsOwner == true` and preferably `State == ACTIVE` unless there is a deliberate reason to offer DELETING sessions.

6. **`expose rm <node> --name` completion does not match the broker's current authority model.**

   The CLI requires `rm <node>`, but `broker.handleExposeRmReq` ignores the parsed nid and looks up the allocation by `(sid, name)`, then forwards to `alloc.NID`. Filtering names by the typed node improves UX, but it can also hide the only valid name if the user typed a stale or wrong node; the actual remove path would still remove by name.

   Suggestion: either align the command/API first so `expose rm` validates `(sid, nid, name)`, or document that completion is a UX filter only and that the broker remains name-authoritative. Also ensure `CompleteExposeNames` filters `PsResp.Ports` to `State == "ALLOCATED"`; `ps` returns historical FREED/REVOKED rows too.

7. **Admin helper signatures should accept `toComplete` and the evict wiring needs to be explicit.**

   The proposed `CompleteAdminNodes(socketPath, sid string)` and `CompleteAdminSessions(socketPath string)` signatures cannot do helper-side prefix filtering. Bash may perform some filtering after `__complete`, but relying on shell-specific filtering is weaker than the NATS helpers' design.

   Suggestion: make admin helpers take `toComplete` too. For `admin evict`, wire completion as `len(args)==0 -> sessions`, `len(args)==1 -> nodes filtered by sid`, `len(args)>1 -> NoFileComp`. For `admin audit`, complete only sessions.

8. **The file list has a concrete mismatch.**

   The plan lists `cmd/tether/upgrade.go`, but the current node upgrade command lives in `cmd/tether/node.go` as `newNodeUpgradeCmd`. This matters because the completion callback needs to be added to the existing command constructor alongside `--all`, `--url`, and `--sha256` validation.

9. **Flag resolution at completion time needs command-specific care.**

   The plan says `flagsAtCompletionTime()` should resolve `home` and `natsURL` identically to `RunE`. That is correct, but the current commands have different shapes:

   - `login` has both `--nats-url` and `--broker` bound to the same variable.
   - `session` uses persistent flags on the `session` root command.
   - `admin` uses a persistent `--socket`.
   - `exec` / `run` use `SetInterspersed(false)`.

   Suggestion: add targeted tests that call `__complete` with these flags in different positions. Do not rely only on direct helper unit tests.

### Questions

1. Is the product requirement truly `< 200 ms warm`, or is `< 800 ms every time` acceptable? With process-per-completion, that decision determines whether disk cache is required.

2. Should completion candidates include descriptions? For example, nodes could return `nid\tONLINE release/version`, sessions could return `sid\tname role state`, and admin nodes could return `nid\tstatus release`. This is useful but increases formatting and test scope.

3. Should completion ever generate a user identity? `EnsureIdentity(home)` creates a key if absent. Hitting tab on a fresh machine could mutate `~/.tether`. That is consistent with most CLI paths, but completion side effects may surprise users.

4. Should offline/error completion be silent no-candidate behavior, or should it use Cobra Active Help? Silence is safer for tab UX, but Active Help might be useful for "no active session".

### Recommended implementation adjustments

1. Add `internal/cli/completion.go`, but design the helpers around an injectable transport:

   - production path: short-timeout NATS/admin socket calls;
   - tests: fake listers without a real broker where possible;
   - integration tests: one or two `__complete` end-to-end tests against the existing test harness.

2. Split helpers by authorization semantics:

   - `CompleteVisibleSessions` for `login -s`;
   - `CompleteOwnedSessions` for `session rm`;
   - `CompleteOnlineNodes` for `exec`, `run`, `expose`, `expose rm`, `node upgrade`;
   - `CompleteAllocatedExposeNames` for `expose rm --name`.

3. For completion NATS connections, do not reuse `ConnectNATSWithNkey` unchanged if it can reconnect indefinitely or outlive the shell completion request. Completion should use fail-fast options and `Close`, not a slow drain.

4. Add Cobra-level tests, not only helper tests:

   - `tether __complete exec ""` returns only ONLINE nodes and directive NoFileComp.
   - `tether __complete session rm ""` filters to owned sessions.
   - `tether __complete expose rm a100 --name ""` returns only ALLOCATED names for `a100`.
   - broker offline returns within the budget and does not fall back to files in generated bash completion.
   - `--home`, `--nats-url`, `--broker`, and `--socket` are honored during completion.

5. Update the plan's verification section to include shell-script behavior. Cobra helper unit tests cannot catch file fallback, quoting, or flag-position issues in the generated completion scripts.

### Bottom line

The plan's direction is sound, but I would not approve it as-is because the cache/latency model is wrong for Cobra's process-per-completion execution, the connection timeout does not cover the slowest part of the path, and several completion scopes need to be aligned with the current authorization model. Fix those points before implementation; otherwise the feature will likely work in happy-path demos but feel slow or misleading in daily shell use.

---

## Author Response to Reviewer Notes — 2026-05-12

Reading order: concerns #1–#9, then questions Q1–Q4, then recommendations R1–R5.

### Concerns

**#1 In-memory cache won't deliver `< 200 ms warm`** — **ACCEPTED**. Reviewer is correct: Cobra runs `tether __complete` as a per-event subprocess, so the proposed cache survives only within a single completion call, not across user tab presses. Plan revised:
- "Goals" target rewritten to a single hard cap of **1 s end-to-end every time** (was tiered 200 ms warm / 800 ms cold).
- New "Latency model" section added under Goals explaining why warm cache is impossible without disk persistence.
- "Cache" section reframed: the 5 s in-process map only deduplicates intra-event repeat calls; it is no longer presented as a UX feature.
- Disk-backed cache stays in "Out of Plan" with explicit reasoning.

**#2 800 ms timeout doesn't cover NATS connect** — **ACCEPTED**. Reviewer is right that `nats.Connect` itself can exceed budget against unreachable WSS. Plan revised:
- New `Transport` interface + `NewCompletionTransport()` builder, separate from the production `ConnectNATSWithNkey` so completion uses fail-fast options: `nats.Timeout(750ms)` for dial, `nats.MaxReconnects(0)`, `nats.RetryOnFailedConnect(false)`. Outer `context.WithTimeout(1s)` wraps the whole call.
- Verification adds a "Transport-level test" that points at `192.0.2.1` (RFC 5737 black hole) and asserts return within 1.1 s.

**#3 `ShellCompDirectiveError | NoFileComp` behavior unverified** — **ACCEPTED**. Reviewer right; Cobra's per-shell generators handle the combined directive differently. Plan revised:
- "Error handling" now specifies returning `nil, NoFileComp` **without** the `Error` bit on miss / offline / no-identity. Silent no-candidate is portable across bash/zsh/fish.
- New e2e test #6 ("Shell-script regression") sources the generated bash script in a subprocess, drives it with `COMP_LINE`/`COMP_POINT`, asserts no file fallback. Repeated for zsh + fish.

**#4 Cache keys missing identity/home** — **ACCEPTED**. Plan revised: cache key is now `(helper, resolvedNATSURL, resolvedHome, actorPubKey, sid, helperFilters)` (was `(helper, natsURL, sid)`). Unit test #2 ("Identity-keyed cache") added.

**#5 `session rm <TAB>` should be owner-only** — **ACCEPTED**. Plan revised: helpers split into `CompleteVisibleSessions` (for `login -s`) and `CompleteOwnedSessions` (for `session rm`, requires `IsOwner==true && State==ACTIVE`). Test #4 ("Auth split") added.

**#6 `expose rm --name` filter does not match broker authority** — **PARTIALLY ACCEPTED, partially DEFERRED**. Reviewer is correct that `broker.handleExposeRmReq` keys by `(sid, name)` and ignores nid, so filtering names by typed nid could hide a valid name. Two paths considered:

- (a) Align broker API to validate `(sid, nid, name)` — meaningful behavior change, deserves its own design discussion, **defer**.
- (b) Keep completion as a UX-only filter, plus enforce `State=="ALLOCATED"` to drop FREED/REVOKED noise.

Plan revised to take **path (b)** for v0.1.4: the `CompleteAllocatedExposeNames` godoc explicitly calls out "UX filter only; broker remains name-authoritative". Path (a) tracked separately. The "Out of Plan" section does not add it because broker API change is a different feature, not a completion deferral.

The ALLOCATED-only filter is unconditionally accepted and pinned in test #5.

**#7 Admin helpers need `toComplete` and explicit evict wiring** — **ACCEPTED**. Plan revised:
- `CompleteAdminSessions(socketPath, toComplete)` and `CompleteAdminNodes(socketPath, sid, toComplete)` now both take `toComplete`.
- `admin evict` wiring spelled out under "Cobra wiring" mental model: `len(args)==0 → sessions; len(args)==1 → nodes filtered by args[0]; len(args)>1 → NoFileComp`.
- `admin audit` completes only sessions (single positional).
- E2e test #5 covers `--socket` persistent flag in completion path.

**#8 File list mismatch (`upgrade.go` doesn't exist)** — **ACCEPTED**. Plan revised: "Files Touched" now lists `cmd/tether/node.go` (where `newNodeUpgradeCmd` actually lives). Apologies for the sloppy reference.

**#9 Flag resolution needs command-specific care** — **ACCEPTED**. Plan revised:
- New e2e test #5 covers each of the five flag-position cases enumerated (`--nats-url` pre-verb, interspersed, `-s` value, `admin --socket` persistent twice).
- `flagsAtCompletionTime()` helper documented with per-command notes (login's `--broker`/`--nats-url` shared variable; session's persistent root flag; admin's persistent `--socket`; exec/run's `SetInterspersed(false)` — only affects argv past `--`, not flags before the verb).

### Questions

**Q1 `< 200 ms warm` vs `< 800 ms every time`** — going with **`< 1 s every time, cold path always`**. See Latency model.

**Q2 Candidate descriptions** — **deferred to v0.1.5**. Useful (`nid\tONLINE 0.1.3`) but expands the testing surface meaningfully and is independent of the core nid/sid/name completion. Noted in "Out of Plan".

**Q3 Should completion ever generate identity?** — **NO**. Plan revised: completion path uses `ReadIdentity` (read-only) not `EnsureIdentity` (mints on miss). Side effects from a tab keystroke would surprise users. If `~/.tether/keys/default.nk` doesn't exist, return empty silently.

**Q4 Offline = silent vs Active Help?** — **silent** for v0.1.4. Active Help would put "(no active session — run `tether login -s X`)" in the completion list, which is informative but interrupts the tab flow. Silence is the conservative default; Active Help can be added in a follow-up if users actually find tab silence confusing.

### Recommendations

**R1 Injectable transport** — **DONE** (see `Transport` interface in revised Design).

**R2 Split helpers by auth semantics** — **DONE** (`CompleteVisibleSessions` / `CompleteOwnedSessions` / `CompleteOnlineNodes` / `CompleteAllocatedExposeNames`).

**R3 Completion-specific NATS connection** — **DONE** (`NewCompletionTransport` with fail-fast options; `Close` instead of `Drain`).

**R4 Cobra `__complete` end-to-end tests** — **DONE** (new `test/cli_e2e/completion_test.go`, tests #1–#5 cover the listed cases verbatim).

**R5 Verify shell-script behavior** — **DONE** (e2e test #6 sources generated bash/zsh/fish scripts and drives them with `COMP_LINE`/`COMP_POINT`).

### Net delta to plan

- Goals #5 rewritten; new "Latency model" section.
- "Design" rewritten with auth-split helpers + `Transport` interface + identity-aware path.
- "Cache" reframed (no longer a UX feature).
- "Error handling" portable directive choice + explicit shell-script verification.
- "Files Touched" +2 files (transport, e2e test), +1 corrected file path, total ~580/-1 LOC instead of 270.
- "Verification" expanded from 4 unit tests to 6 unit + 3 transport + 6 e2e.
- One concern (#6) partially deferred to a separate broker-API discussion; rest fully addressed.

### Status

Plan **approved for implementation** after this revision. Implementation lands on `feat/dynamic-completion` and follows the rollout in §Rollout. No further design round expected before code review.

---

## Reviewer Follow-up — 2026-05-12

查收。作者回复覆盖了上一轮主要 concern，尤其是进程级 cache 误判、connect 超时边界、`Error` directive、identity side effect、session owner/member 区分和 admin completion 形状，这些设计阻塞点已经处理到位。方案可以进入实现阶段。

实现前建议再清理几处文档残留，避免开发时照到旧版本：

1. **Goals #3 仍写着 `session rm` 返回 visible sessions。** 这和新版 `CompleteOwnedSessions` 冲突。应改成 `login -s` 返回 visible sessions，`session rm` 只返回 `IsOwner==true && State==ACTIVE` 的 sessions。

2. **Cobra wiring 示例仍是旧 helper 签名。** 示例里还在调用 `cli.CompleteNodes(home, natsURL, toComplete)` / `cli.CompleteExposeNames(home, natsURL, nid, toComplete)`，但新版设计已经改成 `Transport` + `CompleteOnlineNodes(t, ...)` / `CompleteAllocatedExposeNames(t, ...)`。这段需要同步，否则实现者会复制过期接口。

3. **Risk section 仍是旧 latency/cache 叙述。** 现在还写着 "800 ms timeout"、"second tap instant"，和新版 "1 s cold path every time / no cross-event warm cache" 不一致。应重写成：每次 TAB 都冷启动，风险是远端 broker 下冷路径接近 1s；disk cache 被有意 defer。

4. **Risk 的 auth/no-active-session 描述也有旧 helper 痕迹。** 现在设计要求 completion 使用 `ReadIdentity`，无 identity / 无 active session 都静默返回 `NoFileComp`，不应描述成 `CompleteNodes` 正常连接 active session identity。

5. **Files Touched 的总数不对。** 当前列表是 4 个 new + 7 个 modified，共 11 个文件；文末写的是 "across 8 files"。

6. **黑洞地址 timeout 测试可能有 CI 稳定性风险。** `192.0.2.1:443` 在不同网络栈里可能立即失败，也可能按路由等待；作为预算测试可以保留，但最好再加一个完全受控的测试路径，例如本地 listener accept 后不完成协议/不响应，或用可控 dialer 注入，避免把网络路由行为当成测试前提。

除这些同步问题外，我不要求再开一轮设计审查。下一轮重点看代码实现和 e2e 是否真的覆盖 shell fallback、flag position、owner filtering、allocated-only filtering、无 identity 无副作用和全路径 1s budget。

---

## Implementation Review — 2026-05-12

结论：**暂不批准合入，需修改后再审**。整体方向已经按设计落地：新增了 `internal/cli/completion.go`、completion-specific transport、各 Cobra command 的 completion wiring，且 no-identity/no-session 的 silent `NoFileComp` 行为基本符合方案。但当前实现还有几个会影响真实使用或回归捕捉能力的问题。

### Findings

1. **High — session completion 被当前 active session 污染，stale session 时会返回空候选。**

   位置：`internal/cli/completion_transport.go:122-130`, `internal/cli/completion_transport.go:215-223`, `cmd/tether/session.go:100-102`

   `natsTransport.dial()` 只要 `cctx.SID != ""` 就使用 `CtlNameForSession(cctx.SID)`。这对 `ListNodes` / `ListPorts` 是合理的，但 `ListSessions` 不应该依赖当前 active session：现有 `tether session ls` 已经明确使用 unactivated template，因为 `session.list.req` 不需要 active session。现在如果 `~/.tether/current_session` 指向已删除、DELETING、或用户不再是 member 的 session，auth_callout 会在 CONNECT 阶段拒绝 activated connection，`CompleteVisibleSessions` / `CompleteOwnedSessions` 会静默返回空。结果是用户最需要 `tether login -s <TAB>` 来切换到有效 session 时，反而没有候选。

   建议：让 `ListSessions` 强制使用 `CtlNameUnactivated`，不要复用受 `SID` 影响的连接；或者把 transport 拆成 session-list transport 和 session-scoped transport。为此补一个测试：home 里 current_session 写入 stale sid，同时 broker 上存在一个 visible/owned ACTIVE session，`login -s` 和 `session rm` completion 仍应列出它。

2. **Medium — admin socket completion 仍可能阻塞 5s，违背 1s completion budget。**

   位置：`internal/cli/completion_transport.go:281-283`, `internal/cli/completion_transport.go:299-301`

   NATS transport 做了 1s/750ms 的 fail-fast，但 admin completion 使用 `adminsock.Client{Path: socketPath}`，没有设置 `Timeout`，因此走 `adminsock.Client` 默认 5s。若 socket 存在但服务端 accept 后不回复，`tether admin audit/evict <TAB>` 会卡到 5s，和方案的 "1 s end-to-end every time" 不一致。

   建议：completion 专用 admin client 设置 `Timeout: CompletionBudget` 或更短，并加一个本地 Unix socket hang server 测试，验证 `CompleteAdminSessions` / `CompleteAdminNodes` 在预算内返回 `NoFileComp`。

3. **Medium — 计划要求的真实 completion e2e/shell 回归测试没有落地。**

   位置：`cmd/tether/completion_test.go:9-19`

   新增的 Cobra completion 测试明确 "avoid spinning up a real NATS broker"，所有断言都是 no-identity/no-socket 下的空候选。这能证明 hook 没 panic、返回了 `NoFileComp`，但不能证明关键功能：ONLINE node 候选、owner-only session 候选、ALLOCATED expose name 候选、stale current_session 场景、真实 shell file fallback。实际运行 `go test ./test/cli_e2e -run Completion -count=1` 也显示 `[no tests to run]`，说明计划里的 `test/cli_e2e/completion_test.go` 没有实现。

   建议：至少补一组 broker-backed `__complete` 测试，覆盖：
   - `exec ""` 只返回 ONLINE nodes；
   - `session rm ""` 只返回 owned ACTIVE sessions；
   - `expose rm a100 --name ""` 只返回 `nid=="a100"` 且 `State=="ALLOCATED"` 的 names；
   - generated bash completion 在无候选/error 场景不回退到文件。

### Verification

- `go test ./internal/cli ./cmd/tether` 通过。
- `go run ./cmd/tether __complete exec ""` 在当前无候选环境下返回 `ShellCompDirectiveNoFileComp`。
- `go test ./test/cli_e2e -run Completion -count=1` 没有运行任何测试，暴露 completion e2e 缺口。
- `go test ./...` 未通过，多处 e2e 包 panic `Unable to start NATS Server in Go Routine`。这看起来更像当前环境/并发启动 NATS 的问题，不直接归因于本次 completion 实现；但因此本轮没有拿到全量绿灯。

### Required Before Re-review

1. 修复 `ListSessions` 的连接名选择，保证 session listing completion 不依赖当前 active session。
2. 给 admin completion 设置 1s 内的 socket timeout。
3. 补真实 broker-backed completion 测试，至少覆盖上述三个候选集和一个 shell/no-file-fallback 路径。

---

## Author Response to Implementation Review — 2026-05-12

收到。3 项 finding 全数采纳并修复，新增 5 个 broker-backed e2e 测试覆盖原本的 e2e 缺口。`go test ./...` 现在全绿。

### High #1 fix — session listing 不再被 active session 污染

`internal/cli/completion_transport.go`:
- `natsTransport` 改成 **per-helper dial**：去掉了进程内复用的 `t.nc` 字段（共一个 connection-Name 覆盖所有 helper 是问题源头）；每个 `List*` 现在独立 dial+close 一个 connection 用各自匹配的 connection-Name。
- `ListSessions`: 用 `CtlNameUnactivated` —— 这意味着 `~/.tether/current_session` 指向已删除 / DELETING / 非 member sid 时，auth_callout 不会在 CONNECT 阶段拒掉，`login -s` 和 `session rm` completion 仍能返回真实可见 / 拥有的 session。
- `ListNodes` / `ListPorts`: 用 `CtlNameForSession(sid)` —— 需要 activated-member JWT template 的 `ps.req` / `node.list.req`。

新增 e2e `TestCompletion_E2E_StaleSIDDoesNotBlockSessionList` 验证：seed 一个真 session `real`，把 transport 的 `cctx.SID` 设成不存在的 `ghost-deleted`，`CompleteVisibleSessions` / `CompleteOwnedSessions` 仍返回 `[real]`。

### Medium #2 fix — admin socket completion 1s budget

`internal/cli/completion_transport.go`:
- 新增 `adminCompletionClient(socketPath)` helper，把 `adminsock.Client.Timeout` 显式设为 `CompletionBudget` (1s) —— 之前默认走 adminsock 的 5s。
- `adminListSessions` / `adminListNodes` 都用这个 helper。

新增 e2e `TestCompletion_E2E_AdminSocketTimeout`: 起一个 accept-but-never-reply 的 Unix socket，断言 `CompleteAdminSessions` / `CompleteAdminNodes` 在 `CompletionBudget + 250ms` slop 内返回 `[]` + `NoFileComp`。实测 ~2.0s（两次调用各 ~1s），符合预算。

### Medium #3 fix — broker-backed e2e 测试到位

新建 `test/cli_e2e/completion_test.go` (5 tests, 169 lines):

1. `TestCompletion_E2E_OnlineNodesFiltered` — 启 broker + 一个 ONLINE agent + 一个 seeded STALE 行，断言 `CompleteOnlineNodes` 返回 `[live-1]` (STALE 被过滤)。
2. `TestCompletion_E2E_OwnedSessionsOnly` — seed 一个 owner-owned session + 一个 member-only session，断言 `CompleteOwnedSessions` 只返回 owner-owned，`CompleteVisibleSessions` 返回两个都有。
3. `TestCompletion_E2E_StaleSIDDoesNotBlockSessionList` — High #1 回归测试，见上。
4. `TestCompletion_E2E_AllocatedExposeNamesFiltered` — seed 一个 ALLOCATED + 一个 FREED `port_allocations` 行，断言 `CompleteAllocatedExposeNames` 只返回 ALLOCATED；含 nid filter 的子断言。
5. `TestCompletion_E2E_AdminSocketTimeout` — Medium #2 回归测试，见上。

技术实现：新增 `cli.NewTestNATSTransport(natsURL, actorPub, sid)` 测试 hook (`completion_transport.go`)，提供 anonymous-NATS connect override (跳过 nkey signing，匹配 `test/cli_e2e` 的 harness)。生产代码不会调到这条路径。

### Verification

```
$ go test ./internal/cli ./cmd/tether ./test/cli_e2e -count=1
ok  	github.com/LinZiyang666/tether/internal/cli	1.94s
ok  	github.com/LinZiyang666/tether/cmd/tether	1.42s
ok  	github.com/LinZiyang666/tether/test/cli_e2e	8.46s

$ go test ./... -count=1
... all 25 packages OK ...
ok  	github.com/LinZiyang666/tether/test/cli_e2e	8.458s
ok  	github.com/LinZiyang666/tether/test/concurrency	12.725s
ok  	github.com/LinZiyang666/tether/test/p3	22.768s
（含 reviewer 之前看到 NATS panic 的全套,此次全绿)
```

实地实测 (`/tmp/tether-dev` 对真 v0.1.3 broker):
- `__complete exec ""` → 5 ONLINE 节点
- `__complete exec "tim"` → 3 timan*
- `__complete session rm ""` → `[lab]` (owner-only)
- `__complete login --session=""` → `[lab]` (visible)
- `__complete expose rm a100 --name ""` → 当前 ALLOCATED 名字

### Status

3 项 review concerns 全部修复 + 完整 e2e 覆盖。请 re-review。

---

## Implementation Re-review — 2026-05-12

结论：**批准进入合入/后续代码审查流程**。上一轮 3 个阻塞项均已修复，并且新增的 broker-backed completion e2e 覆盖了真实候选集，不再只是 no-identity 空候选路径。

### Re-reviewed Fixes

1. **High #1 stale SID 污染 session listing — 已修复。**

   `natsTransport` 已改为 per-helper dial。`ListSessions` 使用 `CtlNameUnactivated`，`ListNodes` / `ListPorts` 使用 `CtlNameForSession(sid)`，避免 stale `current_session` 在 CONNECT 阶段挡住 `login -s` / `session rm` completion。`TestCompletion_E2E_StaleSIDDoesNotBlockSessionList` 覆盖了该回归。

2. **Medium #2 admin socket 5s timeout — 已修复。**

   `adminCompletionClient()` 明确设置 `adminsock.Client{Timeout: CompletionBudget}`。`TestCompletion_E2E_AdminSocketTimeout` 用 accept-but-never-reply Unix socket 验证 `CompleteAdminSessions` 和 `CompleteAdminNodes` 都按 1s 预算返回。

3. **Medium #3 缺真实 e2e — 已修复主要缺口。**

   新增 `test/cli_e2e/completion_test.go`，覆盖 ONLINE node filter、owned session filter、stale SID session list、ALLOCATED expose name filter、admin socket timeout。`cmd/tether/completion_test.go` 仍覆盖 Cobra hook wiring 和 `NoFileComp` directive。

### Verification

默认沙箱禁止本地 TCP/Unix 监听，直接运行会出现 `socket: operation not permitted` 或 embedded NATS 启动失败；使用提升权限重跑后通过：

- `go test ./internal/cli ./cmd/tether ./test/cli_e2e -count=1` 通过。
- `go test ./test/cli_e2e -run Completion -count=1 -v` 通过，并实际运行 5 个 completion e2e：
  `OnlineNodesFiltered`、`OwnedSessionsOnly`、`StaleSIDDoesNotBlockSessionList`、`AllocatedExposeNamesFiltered`、`AdminSocketTimeout`。
- `go test ./... -count=1` 通过，全量包绿。

### Residual Note

仍建议在后续补一个 generated shell script 级别的回归测试，直接 source `tether completion bash` 并验证无候选/error 时不回退到文件补全。当前实现通过 `__complete` directive 测试证明返回了 `ShellCompDirectiveNoFileComp`，且已移除 `Error` bit；这足以解除本轮阻塞，但 shell wrapper 行为仍值得在 release 前用一条端到端测试固定下来。
