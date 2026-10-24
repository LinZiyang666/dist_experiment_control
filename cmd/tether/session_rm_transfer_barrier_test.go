package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/LinZiyang666/tether/internal/testharness"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func isReserve(subject string) bool { return strings.HasSuffix(subject, ".reserve.req") }

// TestSessionRemovalHonoursTheTransferSettlementBarrier leaves one real pull
// with an issued receiver Grant and no outcome: the ctl receiver is cut off
// for good the instant the broker reserves its commit. Without the explicit
// flag `session rm` must refuse with rc75 naming that transfer, and delete
// nothing. With --abandon-unsettled-transfers the broker must record exactly
// one canonical failed terminal with outcome_unknown=true for it.
// origin: hot-update S7 plan lead LQ1 / §6.4 / T31 / INTEG-D session abandonment
// origin: S7 internal review MAKETEST-TRANSFER F4 / F7
func TestSessionRemovalHonoursTheTransferSettlementBarrier(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseSessionRemovalHonoursTheTransferSettlementBarrier(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSessionRemovalHonoursTheTransferSettlementBarrier", g)
	testharness.AssertNoFDLeak(t, "TestSessionRemovalHonoursTheTransferSettlementBarrier", fd, 0)
}

func exerciseSessionRemovalHonoursTheTransferSettlementBarrier(t *testing.T) {
	t.Helper()

	s := startCapableStack(t)
	remote := t.TempDir()
	payload := bytes.Repeat([]byte("unsettled|"), (1<<20+9)/len("unsettled|")+1)[:1<<20+9]
	src := filepath.Join(remote, "unsettled.src")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	fence, url := startReplyFence(t, s.url, fenceSpec{drops: 1, sever: true, match: isReserve})
	stdout, stderr, err := s.runTransferCLIAt(t, url, newPullCmd, "lab-1:"+src, filepath.Join(t.TempDir(), "unsettled.dst"), "--timeout", "8s")
	if err == nil || classifyExit(err) != 74 {
		t.Fatalf("a pull cut off after its Grant did not end unresolved (rc74): %v rc=%d\nstdout=%s\nstderr=%s", err, classifyExit(err), stdout, stderr)
	}
	if len(fence.drops()) != 1 {
		t.Fatal("the reserve reply was not consumed before the ctl was cut off")
	}
	id := ""
	for _, p := range fence.publications() {
		if m := wireTransferID.FindSubmatch(p.Body); m != nil {
			id = string(m[1])
		}
	}
	if id == "" {
		t.Fatal("no transfer ID was published before the cut")
	}

	_, stderr, err = s.runTransferCLI(t, newSessionCmd, "rm", "lab")
	if err == nil || classifyExit(err) != 75 || !strings.Contains(err.Error(), id) {
		t.Fatalf("session rm without the flag did not refuse rc75 naming %s: %v rc=%d %s", id, err, classifyExit(err), stderr)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM sessions WHERE sid = ?`, "lab").Scan(&state); err != nil || state != "DELETING" {
		t.Fatalf("refused removal changed or deleted the session row: state=%q err=%v", state, err)
	}
	nc, err := nats.Connect(s.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: bounded test inspection.
	defer cancel()
	if _, err := js.Stream(ctx, jsstream.HistoryStreamName("lab")); err != nil {
		t.Fatalf("refused removal deleted history: %v", err)
	}

	// Observe history as it is published: the flagged removal deletes the
	// history stream afterwards, in the normal history→OBJ→SQL order.
	audit, err := nc.SubscribeSync(proto.SubjAuditTransfer("lab"))
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	_, stderr, err = s.runTransferCLI(t, newSessionCmd, "rm", "lab", "--abandon-unsettled-transfers")
	if err != nil {
		t.Fatalf("flagged session rm did not complete: %v rc=%d %s", err, classifyExit(err), stderr)
	}
	terminals := 0
	for {
		msg, err := audit.NextMsg(500 * time.Millisecond)
		if err != nil {
			break
		}
		var fields map[string]any
		if json.Unmarshal(msg.Data, &fields) != nil || fields["transfer_id"] != id || fields["kind"] == "start" {
			continue
		}
		unknown, ok := fields["outcome_unknown"].(bool)
		if fields["kind"] != "failed" || fields["code"] != "settlement_abandoned" || !ok || !unknown {
			t.Fatalf("abandonment published a non-abandoned terminal: %s", msg.Data)
		}
		terminals++
	}
	if terminals != 1 {
		t.Fatalf("abandonment published %d terminals for %s, want exactly 1", terminals, id)
	}
	if err := s.db.QueryRow(`SELECT state FROM sessions WHERE sid = ?`, "lab").Scan(&state); err == nil {
		t.Fatalf("flagged removal left the session row (state=%s)", state)
	}
	if _, err := js.Stream(ctx, jsstream.HistoryStreamName("lab")); err == nil {
		t.Fatal("flagged removal left the history stream")
	}
	if _, err := js.Stream(ctx, "OBJ_"+proto.XferBucketName("lab")); err == nil {
		t.Fatal("flagged removal left the session object bucket")
	}
}
