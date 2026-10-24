//go:build linux

package flow

import (
	"github.com/LinZiyang666/tether/internal/workload"
	"testing"
)

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestRevokeDecisionSurvivesShutdownWriteError(t *testing.T) {
	r := &record{revoked: true, reason: ReasonRevoked, closeMode: CloseModeRevoke}
	abortLocked(r, CloseModeWriteAbort)
	if r.reason != ReasonRevoked || r.closeMode != CloseModeRevoke {
		t.Fatal("shutdown write error replaced the irreversible revoke decision")
	}
}

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestRevokeDefersCloseUntilShutdownSnapshotSettles(t *testing.T) {
	replay, err := workload.NewReplay(&workload.ReplayBudget{}, workload.DefaultReplayCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replay.Close() }()
	if _, err := replay.Append([]byte("already delivered source")); err != nil {
		t.Fatal(err)
	}
	r := &record{open: OpenRequest{ID: ID{1}}, carrierGen: 1, replay: replay, revoked: true, reason: ReasonRevoked, closeMode: CloseModeRevoke, rxApplied: 17, change: make(chan struct{})}
	m := &Manager{state: &managerState{records: map[ID]*record{r.open.ID: r}}}
	ack := Frame{Version: Version, Kind: KindACK, FlowID: r.open.ID, CarrierGen: 1, Flags: FlagDirection, Offset: 24}
	if err := Receive(m, ack); err != nil {
		t.Fatalf("in-flight ACK fenced carrier during revoke: %v", err)
	}
	frames, notification, err := Controls(m, r.open.ID, 1)
	if err != nil || notification == nil {
		t.Fatalf("shutdown snapshot was prematurely terminal: %v", err)
	}
	for _, frame := range frames {
		if frame.Kind == KindClose {
			t.Fatal("CLOSE published before shutdown fixed its final byte counters")
		}
	}
	r.closed = true
	r.finalEnd = [2]uint64{24, 17}
	r.finalApplied = [2]uint64{0, 17}
	frames, notification, err = Controls(m, r.open.ID, 1)
	if err != nil || notification != nil || len(frames) != 2 || frames[1].Kind != KindClose {
		t.Fatalf("settled revoke lost final CLOSE: %v", err)
	}
	if _, _, ends, _, err := closeFields(frames[1], 0); err != nil || ends != r.finalEnd {
		t.Fatalf("settled CLOSE lost counters: %v", err)
	}
	ack.Offset = 25
	if err := Receive(m, ack); err == nil {
		t.Fatal("revoked receipt accepted a future ACK")
	}
}
