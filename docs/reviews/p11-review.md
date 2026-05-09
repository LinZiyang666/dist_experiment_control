# P11 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `1a78384 P11 self-use hardening: CI lint pin + make e2e + error UX walk`

Focus:

- P11 release-hardening requirements in `docs/architecture.md`
- CI workflow and `make e2e`
- `test/e2e` phase matrix
- README / quickstart / troubleshooting freshness
- CLI error-hint wiring

Note: per current instruction, no public release/tag/announcement action was
performed or requested. I did not treat the absent public v0.1.0 release as a
finding in this review.

## Verdict

P11 is not approved yet.

The error-hint work is useful and the normal package baseline is green, but the
P11 release-hardening gates are not satisfied: the nightly e2e job is not wired
into CI, `make e2e` failed locally, and README still describes the project as
P7-era with stale commands.

## Findings

### F1 - High: CI does not run the nightly P2-P10 e2e matrix

P11 requires "P2-P10 各 phase 测试合并到 `test/e2e/`，CI 每夜跑".

`.github/workflows/ci.yml:3-5` triggers only `push` and `pull_request`, and the
workflow jobs at `.github/workflows/ci.yml:19-28` run `make build` and
`make test` but never `make e2e`. `Makefile:30-31` defines `make e2e`, but it is
not connected to CI and therefore cannot establish the "nightly stable" signal.

Reviewer test added:

```text
GOCACHE=/tmp/tether-gocache go test ./test/p11 -count=1

P11 requires CI nightly e2e runs, but .github/workflows/ci.yml has no schedule trigger
```

Recommendation:

Add a scheduled workflow trigger and a job/step that runs `make e2e`, e.g. a
nightly `schedule` event. Keep it separate from push/PR if runtime is too high.

### F2 - High: `make e2e` is not stable enough for nightly acceptance

The new e2e matrix starts every phase subprocess in parallel
(`test/e2e/all_phases_test.go:45-50`). On this review run, `make e2e` failed:

```text
make e2e

phase p5 failed:
--- FAIL: TestRunExitCodePropagates (3.39s)
    run_e2e_test.go:257: run failed: attach_timeout
```

The same P5 test and whole P5 package passed when run alone:

```text
go test ./test/p5 -run TestRunExitCodePropagates -count=1 -v
# PASS

go test ./test/p5 -count=1
# PASS
```

That points at matrix-level contention/flakiness, not a deterministic P5
regression. P11's exit criterion is nightly stability for at least seven days;
an e2e target that fails on first review cannot satisfy that gate.

Recommendation:

Run phases serially by default, or cap concurrency to a small number and prove
it is stable. The matrix is a release gate, so runtime is less important than
repeatability.

### F3 - High: README / quickstart are still P7-era and contain invalid commands

P11 explicitly includes README / quickstart / troubleshooting. Current
`README.md` is not release-current:

- `README.md:8` says `Pre-alpha (phase P7 complete)`.
- `README.md:32` and `README.md:68` describe older phase layering instead of
  a current install/deploy path.
- `README.md:116-117` says P8 reconciliation "lands in P8", even though P8 is
  complete.
- `README.md:134` still comments `make tools` as installing `v1.62.2`, while
  the Makefile pins `v2.5.0`.
- `README.md:138-146` is a P2 quick-start and includes
  `tether admin nodes --db ./tether.db`, but the current admin command is the
  P9 Unix-socket client and has no `--db` flag.
- There is no release-current install.sh quickstart or troubleshooting section.

Reviewer test added:

```text
GOCACHE=/tmp/tether-gocache go test ./test/p11 -count=1

README still contains stale P11 release-hardening text "P7 complete"
```

Recommendation:

Rewrite README around the current P10/P11 install flows:

- ctl install: `curl install.sh | sh`
- broker install: `install.sh --role broker ...` plus manual `systemctl`
- agent install: `install.sh --role agent ...` plus manual/user-service start
- basic `login`, `session create`, `agent`, `exec`, `run`, `expose`, `history`,
  `admin`, `node upgrade`
- troubleshooting for auth, proto mismatch, agent offline, tunnel failures,
  admin socket permissions, and lint/tooling setup.

### F4 - Medium: P11 push/pull residue check still fails in docs

P11 testing asks for a full-repo grep for `push|pull|文件搬运|文件传输` with
only pub/sub and git-related exceptions. `tether --help` has no push/pull
subcommands, which is good:

```text
go run ./cmd/tether --help
# no push / pull command listed
```

But the repository still has verb-level remnants in docs, notably:

```text
docs/requirements.md:142: run / exec / push / pull / expose / ...
docs/requirements.md:163: push | 本地文件/目录 → 节点路径
docs/requirements.md:164: pull | 节点路径 → 本地文件/目录
docs/requirements.md:501: M5 — 文件传输：push / pull ...
```

`docs/architecture.md:941` explicitly says file transfer is not v1, which is
good, but the older requirements doc still presents push/pull as active
requirements. That is exactly the kind of release-doc ambiguity P11 is meant to
remove.

Recommendation:

Either mark `docs/requirements.md` as historical/non-release input at the top,
or update it to match the v1 scope. Keep the architecture's "v2 file transfer"
note, but remove verb-level v1-looking push/pull requirements.

## Verification

Reviewer tests:

```text
GOCACHE=/tmp/tether-gocache go test ./test/p11 -count=1
# FAIL:
# - TestReviewCIHasNightlyE2EMatrix
# - TestReviewReadmeIsReleaseCurrent
```

Submitted P11 error-hint tests:

```text
GOCACHE=/tmp/tether-gocache go test ./cmd/tether -run 'TestBrokerErrorMessage|TestConnectError|TestRunFailure' -count=1
# PASS
```

E2E matrix:

```text
make e2e
# FAIL: TestAllPhases/p5 -> TestRunExitCodePropagates attach_timeout
```

P5 isolated reruns:

```text
go test ./test/p5 -run TestRunExitCodePropagates -count=1 -v
# PASS

go test ./test/p5 -count=1
# PASS
```

Baseline regression/static checks, excluding the new reviewer tests:

```text
go test ./... -skip '^TestReview' -count=1
# PASS

go build ./...
# PASS

GOCACHE=/tmp/tether-gocache go vet ./...
# PASS

/home/weiland/go/bin/golangci-lint run ./...
# PASS, 0 issues
```

Commands requiring embedded NATS/HTTP listeners or default Go/lint cache writes
were run outside the default sandbox.

## Reviewer Tests Added

Added:

- `test/p11/ci_review_test.go`

Keep these as acceptance tests for the next P11 review round.

---

## Maintainer Response (round 1)

Date: 2026-05-09

All four findings accepted. F3 in particular was a self-inflicted
gap — the previous round I argued "no public release → skip
README", but P11 spec asks for the doc work regardless of when
publication happens, and a stale README hurts the maintainer too
(I forget how my own commands work after a week away). Reviewer
correctly caught it.

### F1 — accepted, fixed

`.github/workflows/ci.yml` now has a `schedule:` trigger
(02:30 UTC daily) and a dedicated `e2e:` job that runs `make e2e`.
The job is gated by `github.event_name == 'schedule' ||
(push && refs/heads/main)` so PR latency is not affected by the
~80s e2e runtime — the matrix runs nightly + on every main push,
which is the architecture line 2147 cadence.

Reviewer's `TestReviewCIHasNightlyE2EMatrix` now passes.

### F2 — accepted, fixed

`test/e2e/all_phases_test.go` removed `t.Parallel()`. Phases
now run serially (~67s on this box vs ~24s parallel) and the
P5 attach_timeout flake is gone. P11 is a release gate so
repeatability beats wall-clock — the architecture line 2148
mandates "nightly stable ≥ 7 days no flaky fail", which the
parallel version was never going to clear. Confirmed stable
across 3 consecutive `make e2e` runs.

### F3 — accepted, fixed

`README.md` rewritten end-to-end. Removed every stale phrase the
reviewer test pinned (`P7 complete`, `P6 features still apply`,
`lands in P8`, `admin nodes --db`, `v1.62.2`). Added:

- Status table showing P0-P11 phases.
- Build + dev-tools section (golangci-lint v2.5.0).
- install.sh quickstart for all three roles (ctl / agent /
  broker) matching the current K.1 / K.2 / K.3 commands.
- Daily commands table (login, session, ps, exec, run, expose,
  history, node upgrade, admin).
- Local laptop demo (TETHER_DEV_NO_AUTH path).
- Troubleshooting section: connect failures, session not found,
  proto mismatch, attach_timeout, url_not_allowed, admin socket
  perms, lint version mismatch, agent OFFLINE, disk pressure.
- Architecture deep-dive pointer with the v1 deviations called
  out (yamux-instead-of-frp, no public v0.1.0 tag yet).

Reviewer's `TestReviewReadmeIsReleaseCurrent` now passes.

### F4 — accepted, fixed

`docs/requirements.md` got a HISTORICAL banner at the top. It
explicitly lists `push` / `pull` / file-transfer (and
`tether tag` / `tether plugin`) as v2-deferred, and points
readers at `architecture.md` as the v1 authority. The doc is
preserved as a record of the v1-pre design draft (not deleted)
because some context — non-functional req tables, decision
constraints — doesn't appear in architecture.md.

### Round-1 verification

```text
go build ./...                                                     → ok
go vet ./...                                                       → ok
golangci-lint run ./...                                            → 0 issues
go test ./...                                                      → all packages PASS
go test -count=1 ./test/p11/... -v                                 → 2 reviewer tests PASS
make e2e × 3 consecutive runs                                      → all PASS, ~67s each
```

The 3-consecutive-make-e2e probe is the local proxy for
architecture line 2148's "7-day nightly stable" — full proof of
that requires actually waiting 7 days of nightly runs, which is
out of scope for a single review round; what's in scope is
removing the obvious flake source the reviewer flagged.

---

## Round 2 Review

Date: 2026-05-09
Reviewer role: test engineer

Reviewed commit:

- `54283fe Address P11 review: nightly e2e (F1) + serial matrix (F2) + README rewrite (F3) + requirements banner (F4)`

### Verdict

P11 hardening is approved for the current non-public state.

All four round-1 findings are fixed:

- F1: CI now has a daily `schedule` trigger and an `e2e` job that runs
  `make e2e` on nightly schedule and push-to-main.
- F2: `test/e2e/all_phases_test.go` now runs phases serially; local
  `make e2e` passed in review without the P5 attach-timeout flake.
- F3: README was rewritten around current P0-P11 state, install.sh flows,
  daily commands, local demo, and troubleshooting.
- F4: `docs/requirements.md` now has a historical banner making
  architecture.md the v1 authority and explicitly deferring push/pull/file
  transfer to v2.

No new blocking issue found in this round.

Public release caveat: this review did not cut or verify a public `v0.1.0`
tag/GitHub Release, per the current "temporarily not public" instruction. The
"7 days nightly stable" release gate also cannot be proven inside one review
turn; the schedule is now wired, and actual nightly history should be checked
before external publication.

### Round 2 Verification

Reviewer tests:

```text
GOCACHE=/tmp/tether-gocache go test ./test/p11 -count=1 -v
# PASS
```

E2E matrix:

```text
make e2e
# PASS, 67.06s
```

Full regression/static checks:

```text
go test ./... -count=1
# PASS

go build ./...
# PASS

GOCACHE=/tmp/tether-gocache go vet ./...
# PASS

/home/weiland/go/bin/golangci-lint run ./...
# PASS, 0 issues
```

Help surface:

```text
GOCACHE=/tmp/tether-gocache go run ./cmd/tether --help
# PASS: no push / pull subcommands listed
```

Commands requiring embedded NATS/HTTP listeners or default Go/lint cache writes
were run outside the default sandbox.
