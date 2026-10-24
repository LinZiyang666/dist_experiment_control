package cli

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

type releaseFailureCapacity struct {
	x.CapacityAccount
	failure error
}

func (c releaseFailureCapacity) Release(id x.ReservationID) error {
	return errors.Join(c.CapacityAccount.Release(id), c.failure)
}

// origin: hot-update S10 plan §0 / LINT-L2 errcheck
func TestTransferSelectionPreservesCapacityReleaseFailure(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferCapacityReleaseFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseTransferCapacityReleaseFailure(t *testing.T) {
	t.Helper()
	s, nc, actor, stop := transferConnectionFixture(t)
	defer stop()
	caps := answerTransferCaps(t, nc, actor.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Error(err)
		}
	}()
	subject, err := proto.SubjCtrlTransferRequest(actor.PublicKey, "lab", "node", "original", x.OpProbe)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		if err := m.Respond([]byte(`{"version":"transfer-v1","history_guard_version":"transfer-v1","bucket_config_version":"transfer-v1","instance_id":"agent","max_payload":1048576,"max_msg_size":1048576,"code":"","error":""}`)); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Error(err)
		}
	}()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), actor, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Error(err)
		}
	}()
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Source, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cfg.Source.Close(); err != nil {
			t.Error(err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/destination", Size: 6}
	releaseErr := errors.New("capacity release failed")
	cfg.Capacity = releaseFailureCapacity{CapacityAccount: cfg.Capacity, failure: releaseErr}
	legacy, err := SelectTransferClient(&cfg, retryDeadline(t, time.Second))
	if legacy || !errors.Is(err, releaseErr) {
		t.Fatalf("selection hid capacity release failure: legacy=%v err=%v", legacy, err)
	}
}
