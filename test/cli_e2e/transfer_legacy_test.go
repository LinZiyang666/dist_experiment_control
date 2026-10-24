//go:build linux

package cli_e2e_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// This immutable pre-hot-update source is locally archived and compiled, never
// supplied by PATH, a staged deployment, or another engineer's worktree.
// origin: hot-update S7 plan §9 / T40
const transferLegacyRevision = "cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025"

var transferLegacyBinary string

func buildTransferLegacy(t *testing.T) string {
	t.Helper()
	if transferLegacyBinary == "" {
		transferLegacyBinary = buildHistoricalPTYBinary(t, transferLegacyRevision, "transfer-legacy", "v0.0.0-transfer-legacy")
	}
	if upgradeFileSHA(t, transferLegacyBinary) == upgradeFileSHA(t, buildTether(t)) {
		t.Fatal("legacy and capable artifacts have the same SHA")
	}
	t.Logf("legacy source=%s artifact=%s race=%t", transferLegacyRevision, upgradeFileSHA(t, transferLegacyBinary), raceEnabled)
	return transferLegacyBinary
}

// All four broker/fleet quadrants use real binaries. The two mixed extra rows
// separate old-ctl compatibility events from old-agent cap0 negotiation; a new
// ctl's status query cannot certify the original old ctl's native success.
// origin: hot-update S7 plan §9 / T40
// origin: S7 internal review MAKETEST-TRANSFER F7 / F9
// origin: S7 internal review TRANSFER-MAKETEST-2 F3
func TestTransferBinaryLegacyInteroperability(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	current, previous := buildTether(t), buildTransferLegacy(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferBinaryLegacy(t, current, previous) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer legacy quadrants", g)
	testharness.AssertNoFDLeak(t, "transfer legacy quadrants", fd, 0)
}

func exerciseTransferBinaryLegacy(t *testing.T, current, previous string) {
	for _, q := range []struct {
		name               string
		broker, agent, ctl string
		capable            bool
	}{
		{"new-new", current, current, current, true},
		{"new-old", current, previous, previous, false},
		{"old-new", previous, current, current, false},
		{"old-old", previous, previous, previous, false},
		{"new-agent-old-ctl", current, current, previous, false},
		{"old-agent-new-ctl", current, previous, current, false},
	} {
		t.Run(q.name, func(t *testing.T) {
			// Each combination owns its NATS, store, homes and mutable binaries.
			// The enclosing round joins all six before the next leak exercise.
			// origin: hot-update S10 plan §11.1 / TEST-BUDGET
			t.Parallel()
			f := newTransferStack(t, q.broker, q.agent, false, "legacy")
			p := f.peers[0]
			ctlBinary := copyTransferArtifact(t, f.h.Root, "selected-ctl", q.ctl)
			brokerIdentity := sampleTransferIdentity(t, f.broker)
			agentIdentity := sampleTransferIdentity(t, p.agent)
			if brokerIdentity.RunningSHA != upgradeFileSHA(t, q.broker) || agentIdentity.RunningSHA != upgradeFileSHA(t, q.agent) {
				t.Fatal("quadrant did not run its declared broker/agent artifacts")
			}
			if q.capable {
				f.requireCapable(p)
			}
			for _, tier := range []string{"a", "b"} {
				for _, verb := range []string{"push", "pull"} {
					t.Run(tier+"-"+verb, func(t *testing.T) {
						size := 32 << 10
						if tier == "b" {
							size = 9 << 20
						}
						src, digest := transferPayload(t, f.h.Root, tier+verb+"-source", size)
						dst := filepath.Join(f.h.Root, tier+verb+"-destination")
						base := len(f.rows())
						ctl := f.transfer(p, ctlBinary, f.url, verb, src, dst)
						original := sampleTransferIdentity(t, ctl)
						if original.RunningSHA != upgradeFileSHA(t, ctlBinary) {
							t.Fatal("original ctl is not the selected quadrant artifact")
						}
						r, stdout, stderr := transferNative(f.ctx, t, ctl)
						if err := checkTransferNativeResult(r.NativeRC, stdout, 0); err != nil {
							t.Fatalf("quadrant native outcome: %v: %s (P19-P27 integration prerequisite)", err, stderr)
						}
						if upgradeFileSHA(t, dst) != digest {
							t.Fatal("native success did not produce complete destination bytes")
						}
						tid, callID, err := transferInvocation(f.rows()[base:], p, verb)
						if err != nil || !strings.Contains(stdout, tid) {
							t.Fatalf("native output lost original invocation: id=%q call=%q err=%v output=%q", tid, callID, err, stdout)
						}
						t.Logf("original invocation pid=%d transfer_id=%s call_id=%s", r.PID, tid, callID)
						if q.capable {
							status := f.status(p, tid)
							if callID == "" {
								t.Fatal("capable invocation omitted CallID")
							}
							if err := checkTransferHistory(f.history(p), status); err != nil {
								t.Fatal(err)
							}
						} else {
							f.requireLegacyHistory(p, tid, verb, tier, int64(size), q.broker == current)
							if verb == "push" {
								requireLegacyTransferEvent(t, f.rows(), p, tid, int64(size))
							}
							if q.ctl == current && !strings.Contains(stdout, "best_effort=true") {
								t.Fatal("cap0 result was represented as exact capable success")
							}
						}
						// A second non-force invocation must report the real refusal.
						// This kills the oldctl-always-OK mutation independently of status.
						r, stdout, stderr = transferNative(f.ctx, t, f.transfer(p, ctlBinary, f.url, verb, src, dst))
						wantRC := 64
						// Preserve the historical CLI's native classification. The old
						// local pull guard returned an untyped error (rc70); S7 uses rc64.
						if q.ctl == previous && verb == "pull" {
							wantRC = 70
						}
						if err := checkTransferNativeResult(r.NativeRC, stdout, wantRC); err != nil {
							t.Fatalf("existing destination native refusal: %v: %s", err, stderr)
						}
						if upgradeFileSHA(t, dst) != digest {
							t.Fatal("refused invocation changed destination")
						}
					})
				}
			}
		})
	}
}

func transferInvocation(rows []transferWireRow, p *transferPeer, verb string) (string, string, error) {
	var tid, call string
	for _, row := range rows {
		if row.Subject != proto.SubjCmdBy(p.sid, p.id.PublicKey, p.nid, verb) {
			continue
		}
		var req struct {
			TransferID string `json:"transfer_id"`
			CallID     string `json:"call_id"`
		}
		if json.Unmarshal(row.Data, &req) != nil || req.TransferID == "" {
			return "", "", errors.New("prepare omitted transfer identity")
		}
		if tid != "" && (req.TransferID != tid || req.CallID != call) {
			return "", "", errors.New("retry changed original transfer ID or CallID")
		}
		tid, call = req.TransferID, req.CallID
	}
	if tid == "" {
		return "", "", errors.New("no original prepare was observed")
	}
	return tid, call, nil
}

func checkTransferNativeResult(rc int, stdout string, wantRC int) error {
	results := strings.Count(stdout, ": OK (") + strings.Count(stdout, "kind=complete")
	if wantRC == 0 {
		if rc != 0 || results != 1 {
			return fmt.Errorf("native success needs rc0 and one result, got rc=%d results=%d", rc, results)
		}
	} else if rc != wantRC || results != 0 {
		return fmt.Errorf("native path refusal needs rc%d and no OK, got rc=%d results=%d", wantRC, rc, results)
	}
	return nil
}

func (f *transferStack) requireLegacyHistory(p *transferPeer, tid, verb, tier string, size int64, newBroker bool) {
	f.t.Helper()
	// Old one-way events have asynchronous audit publication. Wait for a real
	// terminal, then scan every physical sequence; counts never use deduped maps.
	if !testharnessWait(5*time.Second, func() bool {
		for _, row := range f.rows() {
			if row.Subject != proto.SubjAuditTransfer(p.sid) {
				continue
			}
			var rec struct {
				TransferID string `json:"transfer_id"`
				Kind       string `json:"kind"`
			}
			if json.Unmarshal(row.Data, &rec) == nil && rec.TransferID == tid && rec.Kind == "complete" {
				return true
			}
		}
		return false
	}) {
		f.t.Fatal("old ctl exited without an observed terminal")
	}
	records, err := transferRecords(f.history(p), tid)
	if err != nil || len(records) != 2 || records[0].Kind != "start" || records[1].Kind != "complete" ||
		records[1].Verb != verb || records[1].Tier != tier || records[1].Bytes != size || records[1].OutcomeUnknown {
		f.t.Fatalf("legacy history disagrees with native result: %+v err=%v", records, err)
	}
	if newBroker && (records[1].EntryNonce == "" || records[1].TerminalID == "") {
		f.t.Fatal("new broker emitted legacy success before retaining canonical terminal identity")
	}
}

func requireLegacyTransferEvent(t *testing.T, rows []transferWireRow, p *transferPeer, tid string, size int64) {
	t.Helper()
	count := 0
	for _, row := range rows {
		if row.Subject != proto.SubjEvTransfer(p.sid, p.nid, tid, "complete") {
			continue
		}
		var ev proto.TransferEvent
		if json.Unmarshal(row.Data, &ev) != nil || ev.TransferID != tid || ev.Kind != "complete" || ev.Bytes != size {
			t.Fatal("old ctl compatibility event changed the original outcome")
		}
		count++
	}
	if count != 1 {
		t.Fatalf("old ctl received %d complete events, want one", count)
	}
}

// The expected outcome comes from the exercised path, never from the child's
// claimed OK string. These controls are not claimed as old-binary execution.
// origin: hot-update S7 plan §12.2 / T40 mandatory oldctl-false-OK control
func TestTransferBinaryLegacyNativeResultControls(t *testing.T) {
	if err := checkTransferNativeResult(0, "tether push: OK (tier A)\n", 0); err != nil {
		t.Fatal(err)
	}
	if err := checkTransferNativeResult(64, "tether push: destination exists\n", 64); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct {
		rc     int
		out    string
		wantRC int
	}{
		{0, "tether push: OK (tier A)\n", 64},
		{64, "tether push: OK (tier A)\n", 64},
		{0, "", 0},
		{74, "tether push: OK (tier A)\n", 0},
		{75, "tether push: OK (tier A)\n", 0},
		{0, "kind=complete\nkind=complete\n", 0},
	} {
		if err := checkTransferNativeResult(bad.rc, bad.out, bad.wantRC); err == nil {
			t.Fatalf("false native success accepted: %+v", bad)
		}
	}
}

// An absent real responder is uncertainty, never an explicit cap0 answer.
// The stopped broker cannot forge an unsupported capability for this test.
// origin: hot-update S7 plan §7 / §9 / T40
func TestTransferBinaryProbeSilenceNeverSelectsLegacy(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferProbeSilence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer probe uncertainty", g)
	testharness.AssertNoFDLeak(t, "transfer probe uncertainty", fd, 0)
}

func exerciseTransferProbeSilence(t *testing.T) {
	f := newTransferStack(t, buildTether(t), buildTether(t), false, "probe")
	p := f.peers[0]
	if err := f.broker.Stop(); err != nil {
		t.Fatal(err)
	}
	src, _ := transferPayload(t, f.h.Root, "source", 1024)
	dst := filepath.Join(f.h.Root, "destination")
	base := len(f.rows())
	ctl := f.transfer(p, f.bin, f.url, "push", src, dst)
	sampleTransferIdentity(t, ctl)
	r, stdout, stderr := transferNative(f.ctx, t, ctl)
	if (r.NativeRC != 74 && r.NativeRC != 75) || strings.Contains(stdout, "complete") || strings.Contains(stdout, "OK") {
		t.Fatalf("silent probe selected legacy success: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
	rejectTransferCreation(t, f.rows()[base:])
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("silent probe touched destination", err)
	}
}
