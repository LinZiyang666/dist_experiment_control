# P9 Round 2 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `55ea345 Address P9 review: stale-socket probe (F1) + serve default (F2) + parent chmod (F3) + yaml wiring (F4)`

Focus:

- Round-1 findings in `docs/reviews/p9-review.md`
- admin socket active/stale path behavior
- `tether serve` and `tether admin` default socket compatibility
- broker.yaml `domain` and `frp.port_range` wiring
- P9 and full-repo regression tests

## Verdict

P9 is approved.

All four round-1 findings are fixed and verified. I did not find a new
P9-blocking issue in this round.

## Verified Fixes

### F1 - Active admin socket is no longer unlinked

`adminsock.Server.Start` now probes an existing socket path with a short Unix
dial. A live listener returns `active socket already exists`; only unreachable
socket dirents are reclaimed. `acceptLoop` now owns a listener parameter, so
`Close` no longer races by setting `s.listener=nil` under the accept loop.

Reviewer acceptance test:

```text
go test -count=1 -run TestReview ./test/p9 -v
# PASS
```

### F2 - `serve` and `admin` defaults now match

`tether serve --admin-socket` now defaults to `/var/run/tether/admin.sock`,
matching `tether admin --socket`. Library use can still disable the admin
endpoint by leaving `broker.Config.AdminSocketPath` empty.

Reviewer acceptance test:

```text
go test -count=1 -run TestReview ./cmd/tether -v
# PASS
```

### F3 - Existing parent directories are hardened

`adminsock.Server.Start` now calls `os.Chmod(parent, 0700)` after `MkdirAll`.
This closes the case where `/var/run/tether` already exists with wider
permissions.

### F4 - `broker.domain` and `frp.port_range` are wired

`pickPublicHost` now falls back through:

```text
explicit --public-host > broker.public_host > broker.domain > cobra default
```

`parsePortBand` parses `frp.port_range` into `broker.Config.PortBandLow/High`,
with bad input failing startup instead of silently using the default band.

Config parser tests were repeated 10 times:

```text
go test ./cmd/tether -run 'TestPick|TestParse|TestReviewServe' -count=10 -v
# PASS
```

## Verification

Commands requiring embedded NATS/JetStream or Go cache writes were run outside
the default sandbox.

```text
go test ./test/p9 -count=1 -v
# PASS, 12 tests

go test ./internal/adminsock ./internal/broker ./internal/agent ./internal/serveconf -count=1
# PASS

go test ./... -count=1
# PASS

go vet ./...
# PASS

PATH=$PATH:/home/weiland/go/bin make lint
# PASS, 0 issues

go build ./...
# PASS
```

## Residual Notes

- `parsePortBand` currently allows any valid TCP port range in `1..65535`, not
  only the architecture's default public range. I do not consider that a P9
  blocker because it is an explicit operator config, but future docs should say
  whether custom ranges outside `14000-14999` are supported.
