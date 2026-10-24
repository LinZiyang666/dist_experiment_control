//go:build linux

package cli_e2e_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 plan §6.2 / T35 / T36; PRODUCT-R4 arm A
// Hold an original ctl's resize BEFORE forwarding it, let the original child
// exit after real activation, then deliver the late request to the retained
// terminal. The original ctl must drain, ACK, settle and return exactly 23.
func TestRunReliableRetainedTerminalAfterActivation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedReliableTest(t) {
		return
	}
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			candidate := buildUpgradeArtifact(t, "v0.0.0-retained-terminal")
			g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
			for round := 0; round < 5; round++ {
				for _, upgrade := range []bool{false, true} {
					t.Run(fmt.Sprintf("round_%d/upgrade_%v", round, upgrade), func(t *testing.T) {
						// origin: hot-update S5 PROC-LEAK / final process census.
						testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

						srv := serveUpgradeArtifacts(t, candidate)
						s := newS8UpgradeStack(t, srv)
						trace, replies := tracePTYWire(t, s), observePTYReplies(t, s)
						blocked, release := make(chan struct{}), make(chan struct{})
						var releaseOnce sync.Once
						t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
						var armed atomic.Bool
						_, url := startNATSRelay(t, s.url, func(subject string, body []byte) bool {
							var req proto.ProcessResumeReq
							if strings.Contains(subject, "process-resume.req") && json.Unmarshal(body, &req) == nil && req.Op == "resize" && armed.CompareAndSwap(true, false) {
								close(blocked)
								<-release
							}
							return false
						})
						f := newPTYFixture(t)
						ctl := startTerminalCtlStderr(t, s, s.bin, url, true, f.argv("gated-exit")...)
						id := f.identity(t)
						ctl.expect(t, []byte("READY\n"))
						requirePTYReliable(t, s, 1)
						pid := s.agentPID()
						ticks, err := upgradeProcessIdentity(pid)
						if err != nil {
							t.Fatal(err)
						}
						if upgrade {
							// The S8 transaction owns this live cut; a legacy upgrade
							// cannot authorize retained terminal custody (§7.2).
							// origin: S8 internal review upgrade-maketest F4
							grant := completeS8NodeUpgrade(t, s, srv, 0, candidate)
							requireS8UpgradeReceipt(t, s, pid, ticks, grant, 1)
						}
						armed.Store(true)
						ctl.resize(t, 101, 37)
						select {
						case <-blocked:
						case <-time.After(10 * time.Second):
							t.Fatal("original ctl did not issue the delayed resize")
						}
						f.gate(t, "finish")
						if !testharnessWait(10*time.Second, func() bool { return s.agentLogCount(`agent: run reliable=true exited`) == 1 }) {
							t.Fatal("child did not exit after activation")
						}
						if s.agentLogCount(`agent: run reliable=true settled`) != 0 {
							t.Fatal("terminal settled before the original ctl received it")
						}
						releaseOnce.Do(func() { close(release) })
						ctl.wait(t, 23)
						ctl.expect(t, []byte("READY\nTAIL-OUT\nTAIL-ERR\n"))
						trace.requireOriginalHandle(t)
						f.once(t)
						requirePTYResourcesReleased(t, s, id)
						if s.agentLogCount(`agent: run reliable=true settled`) != 1 {
							t.Fatal("missing or duplicate settlement receipt")
						}
						var terminal string
						settled := false
						for _, reply := range replies.snapshot() {
							if reply.TerminalID == "" {
								continue
							}
							if terminal == "" {
								terminal = reply.TerminalID
							}
							if reply.TerminalID != terminal || reply.ExitCode != 23 || reply.Signal != 0 || reply.Error != "" {
								t.Fatalf("terminal changed: %+v", reply)
							}
							settled = settled || reply.Phase == "settled"
						}
						acks := 0
						requests, _ := trace.snapshot()
						for _, req := range requests {
							if req.Op == "terminal-ack" && req.TerminalID == terminal {
								acks++
							}
						}
						if terminal == "" || !settled || acks == 0 {
							t.Fatal("original client did not receive and acknowledge the exact terminal")
						}
					})
				}
			}
			testharness.AssertNoGoroutineLeak(t, "retained terminal real image switch", g0)
			testharness.AssertNoFDLeak(t, "retained terminal real image switch", fd0, 0)

		})
	}
}
