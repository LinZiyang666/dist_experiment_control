package main

import (
	"bytes"
	"github.com/LinZiyang666/tether/internal/testharness"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

var wireTransferID = regexp.MustCompile(`"transfer_id":"([^"]*)"`)

// isPushCommit matches the ctl's public commit carrier (SubjCmdBy ... push-commit).
func isPushCommit(subject string) bool { return strings.HasSuffix(subject, ".push-commit.req") }

// TestTransferCommitLostResumesTheSameTransfer loses the real push commit at
// the socket boundary in both directions: the broker's reply (the broker has
// accepted the commit) or the request itself (the broker never saw it). The
// same ctl invocation must query status first, replay only the byte-identical
// commit (same CallID) when status leaves it live, and finish with one exact
// receipt. Across the whole invocation every ctl publication names one
// transfer ID, and every OBJ chunk publication names one fixed NUID: SDK Put
// mints a fresh NUID per call, so a second NUID is the physical evidence of a
// capable path falling back to Put (red line #1).
// origin: hot-update S7 plan §7 / L1 / T33 / INTEG-D commit_lost
func TestTransferCommitLostResumesTheSameTransfer(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferCommitLostResumesTheSameTransfer(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferCommitLostResumesTheSameTransfer", g)
	testharness.AssertNoFDLeak(t, "TestTransferCommitLostResumesTheSameTransfer", fd, 0)
}

func exerciseTransferCommitLostResumesTheSameTransfer(t *testing.T) {
	t.Helper()

	s := startCapableStack(t)
	for _, tc := range []struct {
		name    string
		request bool
	}{{"reply-lost", false}, {"request-lost", true}} {
		t.Run(tc.name, func(t *testing.T) {
			fence, url := startReplyFence(t, s.url, fenceSpec{drops: 1, request: tc.request, match: isPushCommit})
			payload := bytes.Repeat([]byte(tc.name+"|"), (3<<20+7)/len(tc.name+"|")+1)[:3<<20+7]
			src, dst := filepath.Join(t.TempDir(), tc.name+".src"), filepath.Join(t.TempDir(), tc.name+".dst")
			if err := os.WriteFile(src, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, err := s.runTransferCLIAt(t, url, newPushCmd, src, "lab-1:"+dst)
			if err != nil {
				t.Fatalf("push across a lost commit failed: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
			}
			id := transferField(stdout, "transfer_id")
			if transferField(stdout, "kind") != "complete" || transferField(stdout, "sha256") != sha256Hex(payload) || id == "" {
				t.Fatalf("no exact receipt after the lost commit: %q", stdout)
			}
			if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("destination differs after the lost commit: %v", err)
			}
			if drops := fence.drops(); len(drops) != 1 || len(drops[0].Body) == 0 {
				t.Fatalf("the commit was not consumed behind the fence: %d drops", len(drops))
			}
			requireOneTransferAcrossCommitLoss(t, fence.publications(), id, tc.request)
			status, stderr, err := s.runTransferCLI(t, func() *cobra.Command { return newTransferCmd(transferCLI) }, "status", id, "--node", "lab-1", "--json")
			if err != nil || !strings.Contains(status, `"kind":"complete"`) || !strings.Contains(status, `"transfer_id":"`+id+`"`) {
				t.Fatalf("status after the lost commit: %v %s %s", err, status, stderr)
			}
		})
	}
}

// requireOneTransferAcrossCommitLoss is the relay-side oracle. The swallowed
// request is recorded too, so a request-lost run must show a later replay.
func requireOneTransferAcrossCommitLoss(t *testing.T, pubs []fencedPublication, id string, replayRequired bool) {
	t.Helper()
	ids, nuids := map[string]bool{}, map[string]bool{}
	chunkPrefix := "$O." + proto.XferBucketName("lab") + ".C."
	var commits [][]byte
	var order []string
	for _, p := range pubs {
		for _, m := range wireTransferID.FindAllSubmatch(p.Body, -1) {
			ids[string(m[1])] = true
		}
		if _, _, _, tid, op, ok := proto.ParseCtrlTransferRequest(p.Subject); ok {
			ids[tid] = true
			order = append(order, string(op))
		}
		if nuid, ok := strings.CutPrefix(p.Subject, chunkPrefix); ok {
			nuids[nuid] = true
		}
		if isPushCommit(p.Subject) {
			commits = append(commits, p.Body)
			order = append(order, "commit")
		}
	}
	if len(ids) != 1 || !ids[id] {
		t.Fatalf("one invocation named more than one transfer ID: %v (receipt %s)", ids, id)
	}
	if len(nuids) != 1 {
		t.Fatalf("object chunks were published under %d NUIDs, want the one fixed version: %v", len(nuids), nuids)
	}
	if len(commits) == 0 || replayRequired && len(commits) < 2 {
		t.Fatalf("a live lost commit was not replayed: %d commit publications", len(commits))
	}
	for _, body := range commits[1:] {
		if !bytes.Equal(body, commits[0]) {
			t.Fatalf("commit replay changed its request (CallID/identity):\n%s\n%s", commits[0], body)
		}
	}
	first := -1
	for i, op := range order {
		if op != "commit" {
			continue
		}
		if first < 0 {
			first = i
			if !strings.Contains(strings.Join(order[i+1:], " "), "status") {
				t.Fatalf("the lost commit was never reconciled by status: %v", order)
			}
			continue
		}
		if !strings.Contains(strings.Join(order[first+1:i], " "), "status") {
			t.Fatalf("commit was replayed before a status reconciliation: %v", order)
		}
		first = i
	}
}
