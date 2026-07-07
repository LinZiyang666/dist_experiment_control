# Pass - G2/G6 external re-review 2

Conclusion: Pass. The latest follow-up fixes the deploy-tier blocker from my prior re-review: `12-ghost-voter`
has been rewritten from a stale RED drill into a GREEN regression for the fresh force-single auto-prune
behavior, and it passed independently on the simcluster server.

I reviewed only the delta since the previous staged state: `test/simcluster/drills/12-ghost-voter.sh`,
`test/simcluster/README.md`, and the main-process reply appended to the previous re-review report. I did
not treat the reply text as authority; I re-ran the relevant checks.

## Tasklist / review surface

- [x] Rebuilt the review boundary from `git status`, staged files, and the latest unstaged follow-up delta.
- [x] Reviewed the rewritten #12 drill for RED/GREEN semantics and simcluster mandate compliance.
- [x] Verified prior F1/F2/F3 closure remains covered by focused Go tests.
- [x] Ran shell syntax checks, `git diff --check`, focused Go tests, compile-only all-package test, and `go vet`.
- [x] Ran `./remote.sh drill 12-ghost-voter` on the simcluster server.

## Findings

No blocking findings in this re-review.

The previous blocker is closed:

- `test/simcluster/drills/12-ghost-voter.sh` no longer asserts the obsolete "VOTER ghost remains and three
  removal paths refuse" behavior.
- It now proves the fresh-force-single fix directly: after an N=2 setup, it kills `brk2`, runs reliable
  offline force-single on `brk1`, restarts the survivor services, asserts `brk2` is absent from the roster,
  and asserts `recovery node remove brk2` cleanly reports `no such roster node`.
- `test/simcluster/README.md` now marks `12-ghost-voter` as GREEN and explains the legacy-upgrade ghost
  path is covered by hermetic tests because this deploy tier cannot manufacture that old-binary state.

## Doubts / residual risk

- Online force-single still has no passing deploy-tier drill in this batch. The rewritten #12 drill
  intentionally uses offline force-single to avoid the container harness's online dwell/leadership race.
  Given the offline path is the supported floor and the G2 fresh-prune behavior is now deploy-proven, I am
  not treating this as a blocker here. A future simcluster increment should make the online helper
  diagnosable/retryable enough to prove the preferred path.
- The legacy upgrade ghost passthrough (`VOTER` row absent from committed raft config, left by an old binary)
  remains hermetic-only coverage via `TestG2RemoveNodeGhostPassthrough` and `TestFilterGhostPeers...`. That
  is acceptable for this review because the current deploy tier does not have the old binary/DB surgery
  mechanism needed to create the state without adding a separate fixture.

## Verification

- PASS: `sh -n test/simcluster/drills/12-ghost-voter.sh`
- PASS: `git diff --check`
- PASS: `GOCACHE=/tmp/tether-gocache go test ./internal/broker ./internal/cluster ./internal/clusteroffline -run 'TestG2RemoveNodeGhostPassthrough|TestFilterGhostPeers|TestG2ExternalReview|TestG2DataPlaneDegradedBanner|TestPlanClusterNodePrune|TestPruneRosterPeers' -count=1`
- PASS: `./remote.sh drill 12-ghost-voter`
  - GREEN, 13 assertions.
- PASS: `GOCACHE=/tmp/tether-gocache go test ./... -run '^$'`
- PASS: `GOCACHE=/tmp/tether-gocache go vet ./...`
- PASS: `sh -n test/simcluster/drills/20-forcesingle-natsconf.sh`
- PASS: `sh -n test/simcluster/drills/21-smalldisk-tierb.sh`
