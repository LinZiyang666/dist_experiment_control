# P0 Scaffold Review

Date: 2026-05-08
Reviewer role: test engineer

## Review Scope

Reviewed P0 scaffold against `docs/architecture.md` P0 requirements:

- `go.mod`
- `cmd/tether/main.go`
- `cmd/tether/version_test.go`
- `Makefile`
- `.github/workflows/ci.yml`
- `README.md`

P0 target is intentionally small: `git clone && make build && ./bin/tether version` should work.

## Verdict

P0 is acceptable as a scaffold. The binary builds, the version command runs, and the existing Go test passes.

I would allow moving to P1 after tightening the test and lint reproducibility items below. None of the findings indicate a runtime architecture violation yet, but they are worth fixing before more packages and commands accumulate.

## Findings

### 1. Local lint target is not reproducible without an external tool install

Evidence:

- `Makefile` defines `lint` as `golangci-lint run`.
- Local verification failed with `golangci-lint: command not found`.
- CI uses `golangci/golangci-lint-action@v6`, so CI may still pass even when a developer cannot run the same target locally.

Reason this matters:

P0 explicitly says the Makefile should provide `build / test / lint`. A lint target that assumes a globally installed binary is workable, but the installation requirement should be documented or bootstrapped. Otherwise failures become environment-dependent and reviewers cannot reproduce CI locally.

Suggested fix:

- Document the required `golangci-lint` version in README, or
- Add a `make tools` / `make lint-install` helper, or
- Make CI call `make lint` after installing the pinned tool so the Makefile path is exercised.

### 2. `version` command test is too weak for the P0 exit condition

Evidence:

- `cmd/tether/version_test.go` only checks `strings.Contains(out, Version)`.
- The architecture P0 test says `./bin/tether version` should print `v0.0.0-dev`.

Reason this matters:

The current smoke test would not catch output shape regressions. It also becomes vacuous if `Version` is ever accidentally set to an empty string, because every string contains `""`.

Suggested fix:

- Assert the exact first line is `tether v0.0.0-dev` for the default build.
- Assert there are three output lines: release version, `GOOS/GOARCH`, and Go runtime version.
- Optionally add a build-level test in CI: `make build && ./bin/tether version`.

### 3. Version naming is slightly misaligned with the later architecture plan

Evidence:

- `Makefile` injects `-X main.Version=$(VERSION)`.
- Architecture section J.1 describes `ReleaseVersion` plus `internal/proto.ProtoVersion`.
- Architecture section I.5 shows future linker injection into release/proto version fields.

Reason this matters:

This is not a P0 blocker, because P0 has no protocol package. But P1 will introduce `internal/proto`, and version identity becomes part of handshake and upgrade safety. If the public variable name remains `Version`, later refactoring may churn the CLI, linker flags, tests, and release scripts.

Suggested fix:

- In P1, introduce `internal/proto.ProtoVersion = 1`.
- Rename or wrap the CLI release variable consistently, for example `ReleaseVersion`.
- Decide whether `tether version` should print both release and proto version once P1 lands.

### 4. CI does not explicitly verify the built binary's version output

Evidence:

- CI runs `make build` and `make test`.
- The unit test executes the Cobra command in-process, but CI does not run `./bin/tether version`.

Reason this matters:

The P0 exit criterion is specifically about the built binary. In-process command tests are useful, but they do not prove the linker flag and produced artifact behave correctly.

Suggested fix:

- Add a CI step after `make build`:

```bash
./bin/tether version | grep 'tether v0.0.0-dev'
```

## Doubts

### Test placement

The architecture expects e2e tests under `test/e2e/`, while Go unit tests usually live next to package code. I initialized `test/` as the project-level test workspace and will use it for black-box and e2e tests. Package-local unit tests should still be allowed when they need unexported package access.

### Lint policy

There is no `.golangci.yml` yet. That is fine for P0, but default linter behavior can drift as the action or tool version changes. Before P1 grows multiple packages, a small explicit config would make lint results more predictable.

## Verification

Commands run locally:

```bash
go test ./...
make build
./bin/tether version
make test
make lint
```

Results:

- `go test ./...`: passed.
- `make build`: passed.
- `./bin/tether version`: printed `tether v0.0.0-dev`, `linux/amd64`, `go1.23.4`.
- `make test`: passed.
- `make lint`: failed locally because `golangci-lint` is not installed.

## Recommendation

Proceed to P1 only after either documenting or fixing local lint setup and strengthening the version command test. These are small P0 hygiene fixes that will prevent noisy failures once `internal/proto`, `internal/schema`, `internal/auth`, and `internal/storage` start landing.

