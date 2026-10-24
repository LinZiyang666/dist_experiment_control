package xfercontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Copy into internal/xfercontrol. The real controller must admit the frozen
// inline carrier that is already present in x.Request, before forwarding it.
// origin: S7 internal review rv-wiring F3
func TestTransferControllerAdmitsCapableInlineCarrier(t *testing.T) {
	f := newControlFixture(t)
	data := []byte("inline")
	sum := sha256.Sum256(data)
	f.intent.Tier, f.intent.Size, f.intent.SHA256, f.intent.ChunkSize = "a", int64(len(data)), hex.EncodeToString(sum[:]), 0
	f.id.ObjectVersion, f.id.IntentDigest = "", IntentDigest(f.intent)
	f.permit.Identity = f.id
	f.status.Identity = f.id
	req := f.request(x.OpPrepare)
	req.Inline = data
	response, err := f.ctl.Handle(req, f.principal, testDeadline(t))
	if err != nil || response.Code != "" || f.admits != 1 || f.forwards != 1 {
		t.Fatalf("valid inline carrier refused: code=%s err=%v admits=%d forwards=%d", response.Code, err, f.admits, f.forwards)
	}
}
