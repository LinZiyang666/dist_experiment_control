package xferworker

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A runner can pass its phase check before a receipt control takes the entry
// token. Its next step must observe that settlement already retired the entry;
// a retained sender/preparation has no courier to freeze a second time.
// origin: S10 internal review POLISH-A F1 / hot-update S7 plan §4 / §5.3
func TestTransferQueuedStepKeepsRetainedReceiptSettled(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("a", "retained-receipt")
	d := deadline(t, time.Second)
	if _, err := o.Prepare(p, d); err != nil {
		t.Fatal(err)
	}
	id := p.Identity
	sum := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	receipt := x.TerminalReceipt{TerminalID: hex.EncodeToString(sum[:]), Kind: "failed", CanonicalDigest: "canonical", ReqID: "request", HistoryStream: "history", HistorySeq: 7}
	before, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: id, Receipt: &receipt}, d)
	if err != nil || before.Phase != x.PhaseSettled || before.Terminal == nil || *before.Terminal != receipt {
		t.Fatalf("retained receipt did not settle: %+v %v", before, err)
	}
	e := o.entries[key(id)]
	until := e.retention.ResultUntil
	if e.courier != nil {
		t.Fatal("retained preparation unexpectedly owns a courier")
	}
	if err := o.step(e, d); err != nil {
		t.Fatalf("queued step did not preserve settlement: %v", err)
	}
	after, err := o.Status(id)
	if err != nil || !reflect.DeepEqual(before, after) || e.retention.ResultUntil != until {
		t.Fatalf("queued step changed receipt or retention: before=%+v after=%+v err=%v", before, after, err)
	}
}
