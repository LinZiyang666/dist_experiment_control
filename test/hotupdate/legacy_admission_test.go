//go:build linux

// Copy into test/hotupdate. These are negative regression tests, not N-1
// deployment receipts. No tether executable or child process is started.
package hotupdate_test

import (
	"encoding/json"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: S8 internal review rv-n1 F1
func TestLegacyUpgradeWireRequiresQuiescenceFence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(string(rune('0'+round)), func(t *testing.T) {
			exerciseLegacyUpgradeWireFence(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "legacy admission review x5", g)
	testharness.AssertNoFDLeak(t, "legacy admission review x5", fd, 0)
}

func exerciseLegacyUpgradeWireFence(t *testing.T) {
	t.Helper()
	db := testharness.OpenDB(t)
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authorityCreate(t, db, id.Fingerprint, 1)
	_, nc := authorityBroker(t, db, "https://review.invalid/")
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.6.0", BootID: "legacy-boot"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var forwards atomic.Int64
	sub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "node", "upgrade"), func(m *nats.Msg) {
		forwards.Add(1)
		_ = m.Respond([]byte(`{"ok":true,"new_version":"v0.0.0-s8-review"}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// Exactly the old upgrade envelope, without any S8 authorization.
	body, err := json.Marshal(proto.UpgradeReq{URL: "https://review.invalid/candidate.tar.gz", SHA256: strings.Repeat("a", 64), ProtoVersion: proto.ProtoVersion})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(proto.SubjCmdBy("lab", id.PublicKey, "node", "upgrade"), body, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var response proto.UpgradeResp
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Code != "try_again" || forwards.Load() != 0 {
		t.Errorf("unfenced legacy request must defer before forwarding: response=%s forwarded=%d", msg.Data, forwards.Load())
	}
}
