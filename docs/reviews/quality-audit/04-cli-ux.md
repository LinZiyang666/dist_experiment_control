# CLI / UX Audit

Scope: `cmd/tether/*.go` (cobra subcommand tree). Read-only review against the
ten quality dimensions in the audit brief. No code changes.

## Verdict
17 findings (0 critical, 4 high, 8 medium, 5 low).

The tree is generally tidy — P11/3 error UX walk left a clear pattern (broker
codes → `brokerErrorMessage` → human hint) — but the pattern was applied
incompletely, several flags are silently ignored on certain code paths, and
Ctrl-C semantics for one-shot subcommands are weak because `main` doesn't wire
a signal-aware context.

## Findings

### F1 — high: `main.go` doesn't propagate signals to `cmd.Context()`
**Where**: `cmd/tether/main.go:51-56`
**Issue**: `newRootCmd().Execute()` is used instead of `ExecuteContext(ctx)`.
Cobra falls back to `context.Background()` for every `cmd.Context()` call. Only
`agent` and `serve` set up their own `signal.NotifyContext` inside RunE; every
other subcommand (`exec`, `run`, `expose`, `expose rm`, `ps`, `history`,
`session create/ls/rm`, `node upgrade`, `admin *`) has no signal-aware
context.
**Impact**: Ctrl-C while waiting on a slow broker (`tether ps` hung on NATS
timeout, `tether history --follow` blocked on `it.Next()`, `tether exec`
mid-stream, `tether session rm` waiting 5 s) doesn't tear down cleanly — the
user only escapes when the per-command timeout fires (5 s for ps/session,
15 s for expose, 10 min for exec). `tether history --follow` in particular
relies on `cmd.Context().Done()` being signal-aware.
**Fix**: In `main()`, build `ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)`, defer `stop()`, and call `newRootCmd().ExecuteContext(ctx)`. Drop the per-RunE signal wiring in `agent` / `serve` (they'll inherit it).

### F2 — high: `tether agent --install-user-service` silently drops several flags
**Where**: `cmd/tether/agent.go:64-75`, `cmd/tether/agent.go:225-241`
**Issue**: When `--install-user-service` is set, RunE bails to `runInstallUserService` immediately. The generated unit body (`agentUserUnitBody`) only embeds `exe`, `sid`, `nid`. `--nats-url`, `--tunnel-addr`, `--pin`, `--uninstall`, and `$TETHER_HOME` are silently ignored — neither baked into the unit nor warned about.
**Impact**: Operator runs `tether agent --install-user-service --session lab --nid lab-1 --nats-url nats://broker.example.com:4222 --tunnel-addr broker.example.com:7000 --pin 1234`, expecting all four to be persisted. The unit file actually starts `tether agent --session lab --nid lab-1` and the agent then connects to `nats://127.0.0.1:4222` (the cobra default) — connect fails, logs are silent about the mistake.
**Fix**: Either (a) propagate non-default flag values into the unit's `ExecStart=` line, or (b) refuse the install when any of `--nats-url`, `--tunnel-addr`, `--pin` is non-default and tell the operator to put them in `agent.yaml` instead. Mention `agent.yaml` precedence in the install confirmation output.

### F3 — high: `--install-user-service` and `--uninstall` aren't mutually exclusive
**Where**: `cmd/tether/agent.go:64-75`
**Issue**: `if installUserService { return runInstallUserService(...) }` runs first; `if uninstall { return runUninstallUserService(...) }` runs only if the first is false. Passing both → install wins, uninstall is silently ignored.
**Impact**: A scripted "reinstall" (`tether agent --uninstall --install-user-service ...`) leaves the old unit untouched (install is a no-op overwrite, but the user expected an explicit teardown step). Same for `--uninstall --install-user-service` typo from a half-edited shell history line.
**Fix**: Up-front check: `if installUserService && uninstall { return errors.New("--install-user-service and --uninstall are mutually exclusive") }`.

### F4 — high: `tether ps` connect/request errors are dev-style
**Where**: `cmd/tether/ps.go:41-51`
**Issue**: NATS connect failure → `return err` (raw `nats: no servers available`). RPC failure → `return fmt.Errorf("ps: %w", err)`. Broker rejection → `errors.New("ps rejected: " + resp.Code + " " + resp.Error)`. None of these uses `connectError` or `brokerErrorMessage`. The audit brief explicitly flags `ps` as untouched by P11/3.
**Impact**: User running `tether ps` against a wrong `--nats-url` sees `Error: nats: no servers available` with no hint to check `--nats-url` (which `expose` / `run` / `exec` / `node upgrade` do via `connectError`). Broker rejection prints the raw code (`not_a_member`) instead of "you're not a member of this session; ask the owner for a PIN…".
**Fix**: Mirror `expose.go`: wrap connect with `connectError("ps", natsURL, err)`, wrap RPC failure with the same NATS-unreachable suffix, and route `resp.Code != ""` through `brokerErrorMessage("ps", resp.Code, resp.Error)`.

### F5 — medium: `tether history` connect error is dev-style and lacks hint coverage
**Where**: `cmd/tether/history.go:57-65`
**Issue**: `cli.ConnectNATSWithNkey` failure → `fmt.Errorf("history: connect: %w", err)`. JetStream errors → `fmt.Errorf("history: jetstream: %w", err)`, `history: stream <name>: %w`, `history: consumer: %w`. None routed through `connectError`. There's no broker `Code` reply on this path (JetStream is a NATS API, not our protocol), but `Stream not found` is a real failure mode the operator should be told to fix by asking the broker operator to enable JetStream / check `broker.yaml store-dir`.
**Impact**: First-time user running `tether history` against a broker without JetStream sees `history: stream history-01HXY...: nats: stream not found` with no remediation. This is the P7 H.4 path the architecture explicitly calls out as "broker operator may have disabled audit retention".
**Fix**: Use `connectError("history", natsURL, err)`. Map the specific JetStream error families (`nats: no responders` → "broker has no JetStream enabled (`broker.yaml broker.storage.js_store` empty)"; `stream not found` → "this session has no audit history yet — try after the next exec/run").

### F6 — medium: `tether session create` / `session ls` skip the broker-code hint table
**Where**: `cmd/tether/session.go:60-62`, `cmd/tether/session.go:102-104`
**Issue**: Both use `errors.New(resp.Error)` directly, ignoring `resp.Code`. The brief notes `session create` is in the "still dev-style" set.
**Impact**: User runs `tether session create lab --pin xxx` against a broker that already has a `lab` session for them; broker replies `Code:"name_taken", Error:"name already in use"` → user sees only the raw text and doesn't get the hint to pick another name. `not_owner` / `not_a_member` from `ls` are similarly bare.
**Fix**: Apply `brokerErrorMessage("session create", resp.Code, resp.Error)` and `brokerErrorMessage("session ls", resp.Code, resp.Error)`. `SessionListResp` has both `Code` and `Error` fields, so the change is mechanical.

### F7 — medium: `tether admin *` errors are flat `broker: <text>`
**Where**: `cmd/tether/admin.go:49-50,76-77,113-114,150-151`
**Issue**: All four admin subcommands wrap broker rejections as `fmt.Errorf("broker: %s", resp.Error)`. No code hint, no socket-path remediation when `callAdmin` itself fails (e.g. wrong `--socket`, broker not running).
**Impact**: Operator running `tether admin nodes` on the wrong host sees `Error: adminsock client: dial: dial unix /var/run/tether/admin.sock: connect: no such file or directory` — technically accurate but doesn't say "are you running this on the broker host? check `--socket` if your install used a non-default path". `tether admin evict` rejection codes (`not_found`, etc.) bypass any hint table.
**Fix**: Add a small `adminConnectError` analogous to `connectError`, suggesting "ensure broker is running and `--socket` matches `broker.yaml broker.admin.socket` (default `/var/run/tether/admin.sock`)". For `Response.Code` (the architecture P9 evict codes), grow a code→hint table (or extend `brokerCodeHints`).

### F8 — medium: `tether session rm` requires the rm target be the active session
**Where**: `cmd/tether/session.go:140-143`
**Issue**: `rm` opens the connection with `cli.CtlNameForSession(args[0])` — the activated-member template. The template only grants pub allow if `<sid>` matches the operator's currently-active session. So `tether session rm <other-sid>` (without first `tether login -s <other-sid>`) is silently rejected by NATS auth_callout, not by an obvious CLI message.
**Impact**: Confusing for operators: `tether session rm prod` after `tether login -s lab` returns `nats: authorization violation` from `connectError`. The fix the user actually needs (`tether login -s prod` first, then `tether session rm prod`) is not surfaced.
**Fix**: Either (a) auto-`login -s <args[0]>` inside `session rm` before the connect, or (b) explicit pre-check: if `cli.ReadCurrentSession(home) != args[0]`, error out with "session rm <sid> requires the same sid to be the active session — run `tether login -s <sid>` first". Option (b) is the safer one (avoids surprise membership writes).

### F9 — medium: `tether exec` exit-code propagation can wrap signal-killed children to 255
**Where**: `cmd/tether/exec.go:104-107`, `internal/agent/exec.go:136-141`
**Issue**: Agent's `exitErr.ExitCode()` returns `-1` when the remote child is killed by a signal (Go's documented behavior). That `-1` flows through `chunk.ExitCode` and into `os.Exit(-1)`, which the libc/runtime truncates to a uint8 → user sees exit code 255 with no indication "the process was killed by SIGKILL/SIGTERM".
**Impact**: CI pipelines and shell scripts can't distinguish "remote process exited 255 on purpose" from "remote process was OOM-killed" from "broker terminated us". Convention (mirrored by `ssh`) is `128 + signum` for signal exits.
**Fix**: Extend `proto.ExecChunk` (and `RunChunk`) with an explicit `Signal int` field and have the agent populate it when `exitErr.ExitCode() == -1`. CLI then exits `128 + signum`. Until that protocol bump, at minimum print "remote process killed by signal" on stderr before `os.Exit(255)`.

### F10 — medium: `tether exec` `os.Exit` skips deferred NATS cleanup
**Where**: `cmd/tether/exec.go:104-106`
**Issue**: `os.Exit(chunk.ExitCode)` bypasses `defer nc.Close()` (line 54) and `defer sub.Unsubscribe()` (line 68). NATS server side will eventually time the connection out, but until then it sees an open subscription on the inbox.
**Impact**: Mostly cosmetic (broker logs a bunch of "client disconnected without CLOSE" lines); under heavy `exec` use against a non-JetStream NATS, ephemeral subscription leaks may add up before the client-timeout sweep. `tether run` already calls `restore()` explicitly before `os.Exit` — exec should do the symmetric call.
**Fix**: Refactor: have RunE return an `exitCodeError` (a typed error carrying the int), let RunE return normally, and translate to `os.Exit` in `main`. Or simpler: explicitly `nc.Drain(); nc.Close()` before `os.Exit`.

### F11 — medium: `--local` validation fires before "missing flag" check, hiding the real problem
**Where**: `cmd/tether/expose.go:53-55,95`
**Issue**: `cmd.Flags().IntVar(&local, "local", 0, ...)`. Default 0. Validation: `if local <= 0 || local > 65535 { return fmt.Errorf("--local must be 1..65535, got %d", local) }`. User who forgets `--local` sees "got 0" instead of "required".
**Impact**: Minor confusion; the user does eventually figure out the flag is required (the help text says "(required)"), but error wording doesn't match the actual cause.
**Fix**: `if !cmd.Flags().Changed("local") { return fmt.Errorf("--local is required") }`, then the range check.

### F12 — medium: `tether run`/`exec` don't disable interspersed flag parsing → `tether run node1 ls -la` fails confusingly
**Where**: `cmd/tether/run.go:46-58`, `cmd/tether/exec.go:24-36`
**Issue**: Cobra by default treats unknown flag-looking tokens (e.g. `-la`) on the command line as flag parse errors. Both `Use:` strings document the `--` separator (`tether run <node> -- <argv...>`), but cobra is not told to stop parsing at the first positional with `cmd.Flags().SetInterspersed(false)`. Without `--`, `tether exec node1 ls -la` errors with `Error: unknown shorthand flag: 'l' in -la`.
**Impact**: Every operator hits this within a day or two. Error message looks like a tether bug rather than a "you forgot `--`" hint.
**Fix**: `cmd.Flags().SetInterspersed(false)` on both `run` and `exec`. The `--` separator becomes optional; flag handling stops at the first positional. Update the long-help to note `--` is no longer required.

### F13 — low: `--n` short-flag-as-long-flag in `tether admin audit`
**Where**: `cmd/tether/admin.go:128`
**Issue**: `cmd.Flags().IntVarP(&n, "n", "n", 50, ...)`. Both long- and short-flag are `n` → user invokes as `tether admin audit lab --n 200` (works) or `-n 200` (works), but the help screen shows `-n, --n int` which is confusing.
**Impact**: Inconsistent with `tether history -n / --lines` (which uses `--lines` long form). Operator who's learned `--lines` from `history` finds it doesn't work in `admin audit`.
**Fix**: `cmd.Flags().IntVarP(&n, "lines", "n", 50, ...)`. Same long flag as `history`.

### F14 — low: `tether ctx` exits 0 silently on no active session
**Where**: `cmd/tether/login.go:107-121`
**Issue**: `Run` (not `RunE`) prints nothing and returns 0 if `ReadCurrentSession` is empty. Script-friendly (`s=$(tether ctx)` → empty), but indistinguishable from "command crashed silently".
**Impact**: Diagnostic friction. Operator wonders "did `tether ctx` actually run?"
**Fix**: Print `(no active session)` to stderr in the empty case. stdout still empty so script use is unaffected; exit 0 stays.

### F15 — low: `--nid` is unvalidated and gets injected into the systemd unit body unquoted
**Where**: `cmd/tether/agent.go:225-241`
**Issue**: `agentUserUnitBody` does `fmt.Sprintf(... ExecStart=%s agent --session %s --nid %s ...)` with no quoting / sanitization. SIDs are server-generated (ULIDs), but `--nid` is freeform user input — `tether agent --install-user-service --session lab --nid 'foo bar; rm -rf /'` writes a unit that systemd will interpret unpredictably.
**Impact**: Self-inflicted, since the operator owns their machine. But a copy-paste from a malformed `agent.yaml` could produce a broken unit that fails to start with `Failed to parse command line: Invalid argument` — diagnosis is non-obvious.
**Fix**: Validate `nid` (and `sid` defensively) against `^[A-Za-z0-9._-]+$` in both `runInstallUserService` and the regular agent RunE before doing anything.

### F16 — low: `runInstallUserService` ignores `$TETHER_HOME`
**Where**: `cmd/tether/agent.go:181-198`
**Issue**: Uses `os.UserHomeDir()` directly to compute `unitDir`, while the rest of the agent uses `cli.DefaultHome()` which honors `$TETHER_HOME`. The unit body's `StandardOutput=append:%h/.tether/agent/<sid>/agent.log` then uses systemd's `%h` (the user's home), not `$TETHER_HOME`.
**Impact**: A user whose `$TETHER_HOME=/srv/tether` runs the agent under `/srv/tether` but the systemd-installed copy logs to `~/.tether/agent/...`. Logs appear "missing" until the operator notices.
**Fix**: Either resolve via `cli.DefaultHome()` and inject the chosen path into `StandardOutput=`, or refuse `--install-user-service` when `$TETHER_HOME` is set with a clear "user-service installs assume the default ~/.tether layout".

### F17 — low: `tether session ls` and `tether admin sessions` use different "no rows" wording
**Where**: `cmd/tether/session.go:122-124`, `cmd/tether/admin.go:58-59,90-91,122-123`
**Issue**: `session ls` prints `(no sessions; create one with \`tether session create <name> --pin <pin>\`)`. The four `admin` commands print `(no sessions)`, `(no nodes registered)`, `(no audit entries)` — bare, no remediation.
**Impact**: Cosmetic, but inconsistent. The `session ls` form is much more useful for first-time users.
**Fix**: Add a one-line "next step" hint to each empty-state message in `admin` (e.g. "(no nodes registered — agents auto-register on first connect)"). Optional.

## Notes (not findings, kept for cross-reference)

- **Default `--nats-url` consistency** — every CLI subcommand uses `nats://127.0.0.1:4222`. `serve.go` honors `broker.yaml broker.nats.url` over the cobra default; `agent.go` honors `agent.yaml broker_url` similarly. Client subcommands (`exec`, `run`, `expose`, `ps`, `history`, `session *`, `node upgrade`, `login`) have **no** equivalent config-file fallback — they always read the cobra default unless `--nats-url` is passed. This is intentional per architecture (no `~/.tether/config.toml` exists yet), but users with a non-default broker will need to set `--nats-url` on every invocation. Worth a v1.1 follow-up: add `~/.tether/config.toml` with a `[broker]\nnats_url = "..."` for client commands. Not a finding because it's a known omission.
- **Stream selection** — every fmt.Print* in the audited files goes through `cmd.OutOrStdout()` / `cmd.ErrOrStderr()`. No misrouted stdout/stderr writes spotted. (Item 9 of the brief = clean.)
- **Dead code / commented-out blocks** — none in scope. `error_hints.go:104` has an intentional `var _ = errors.New` to keep the import; that's documented in the file. (Item 10 of the brief = clean.)
- **panic-able paths** — every `json.Unmarshal` is followed by `nil`-tolerant access (`range resp.Sessions` is safe on nil; `r := resp.Evict; if r == nil` is checked at admin.go:154). No unchecked type assertions, no array indexing on length-unknown slices, no map access without ok-pattern that would panic. (Item 1 of the brief = clean.)
