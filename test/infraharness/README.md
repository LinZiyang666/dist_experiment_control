# Infrastructure integration harness

`test/infraharness` provides the S9 process, embedded-NATS and artifact fixtures used by the
infranats/infracaddy/infrabusiness/infraprivilege suites. It extends shared
`internal/testharness` subreaper + CLOEXEC lease + pidfd supervision (S9 lead L22); it does not introduce
an independent process supervisor. PID/start ticks and immutable invocation identity distinguish
retirement from PID reuse. Stop/cancel joins the actual supervised waiter; fixture teardown cannot
substitute for product old-owner retirement.

The installed per-test race matrices and suite tags live in `test/e2e/all_phases_test.go`. Each heavy
top-level test has a 600s unit ceiling; an exercise above420s needs decomposition or lower cost.
Whole-package coverage is the exact union of these partitions. Risk tests call both shared goroutine/FD
assertions directly in their Test body and directly exercise at least five rounds. Nested artifact
builds inherit GOFLAGS overlays; a mutated outer harness alone does not mutate the real product.

Stock adjacent-version cuts require real catalog archives/executables, current process/socket/store
proof, auth-first ordinary CONNECT and normal-queue probe, original business bytes/effects, current
ACK-prefix verification before recovery, and joined old/temporary retirement. Missing bindings or
support fail closed; a fake backend table only signs its mechanism. linux/amd64 + systemd/pidfd/cgroup/nft
is the hot support boundary; other platform build/cold behavior is separate.

Drills85/86 and final candidate/global/native-platform receipts remain NOT_RUN/PENDING. Final evidence
belongs to [S9 P39](../../docs/reviews/hot-update-infra-nats-caddy-plan.md#p39--最终候选收据与关闭--hard),
not this helper. See [testing standards](../../docs/testing-standards.md) and the
[deploy harness mandate](../simcluster/README.md) before selecting the owning targeted check. Package
workers do not run global make targets, Docker or deployment drills.
