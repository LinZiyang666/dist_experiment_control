//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
)

// This ordinary transfer must complete through the real binary route before
// any cut can certify continuation. Capability or selection alone cannot sign
// an inline sender, the original receiver's primitive or exact history.
// origin: S7 internal review INTEG-O F4
func TestTransferSmallPullUsesInlineAndExactTerminal(t *testing.T) {
	a := buildTransferArtifacts(t)
	requireTransferIntegration(t, a)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseSmallPullRoute(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

// An accepted prepare can outlive one control request. The original small
// pull must reconcile it and finish within its unchanged transfer budget.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F6
func TestTransferSmallPullReconcilesAcceptedPrepare(t *testing.T) {
	a := buildTransferArtifacts(t)
	requireTransferIntegration(t, a)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseAcceptedSmallPull(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseAcceptedSmallPull(t *testing.T, a transferArtifacts) {
	t.Helper()
	exerciseSmallPull(t, a, true)
}

func exerciseSmallPullRoute(t *testing.T, a transferArtifacts) {
	t.Helper()
	exerciseSmallPull(t, a, false)
}

func exerciseSmallPull(t *testing.T, a transferArtifacts, holdPrepare bool) {
	t.Helper()
	s := newTransferStack(t, a)
	s.requireCapable()
	var mu sync.Mutex
	var permits []proto.TransferPermitReq
	var prepared []proto.Identity
	permitSubject := proto.SubjectPrefix + ".ctrl.by.*.s." + s.sid + ".node." + s.nid + ".transfer.*.permit.req"
	permitSub, err := s.nc.Subscribe(permitSubject, func(m *nats.Msg) {
		var req proto.TransferPermitReq
		if json.Unmarshal(m.Data, &req) == nil {
			mu.Lock()
			permits = append(permits, req)
			mu.Unlock()
		}
	})
	transferMust(t, err)
	// origin: hot-update S10 lint L3a / errcheck
	defer func() { transferMust(t, permitSub.Unsubscribe()) }()
	prepareSub, err := s.nc.Subscribe(proto.SubjectPrefix+".s."+s.sid+".cmd.by.*.node."+s.nid+".pull.req", func(m *nats.Msg) {
		// Retain only public identity, never the permit or settlement capsule.
		var req proto.PullPrepareReq
		if json.Unmarshal(m.Data, &req) == nil && req.Continuation != nil {
			mu.Lock()
			prepared = append(prepared, req.Continuation.Identity)
			mu.Unlock()
		}
	})
	transferMust(t, err)
	// origin: hot-update S10 lint L3a / errcheck
	defer func() { transferMust(t, prepareSub.Unsubscribe()) }()
	transferMust(t, s.nc.FlushTimeout(time.Second))
	cell := transferCell{"a", "pull", "agent", "prepare-accepted"}
	var hold *transferHold
	if holdPrepare {
		hold = armTransferHold(t, s.h.Root, cell, int64(transferCellSize(cell)))
		defer hold.releaseQuietly()
	}
	r := startTransferRun(t, s, cell)
	defer r.watch.close()
	if hold != nil {
		r.boundary = hold.await(s, r)
		timer := time.NewTimer(upgradebudget.RequestLatency + time.Second)
		select {
		case <-timer.C:
		case <-t.Context().Done():
			timer.Stop()
			t.Fatal(t.Context().Err())
		}
		hold.releaseQuietly()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	result, waitErr := s.ctl.Wait(ctx)
	transferMust(t, s.nc.FlushTimeout(time.Second))
	mu.Lock()
	requests := append([]proto.TransferPermitReq(nil), permits...)
	identities := append([]proto.Identity(nil), prepared...)
	mu.Unlock()
	if len(requests) != 1 || len(identities) == 0 {
		t.Fatalf("original pull did not acquire one permit and prepare: permits=%d prepares=%d native_rc=%d %s", len(requests), len(identities), result.NativeRC, s.output(s.ctl))
	}
	if holdPrepare && len(identities) < 2 {
		t.Fatal("accepted prepare did not cross the control deadline and replay its original authority")
	}
	request, identity := requests[0], identities[0]
	if request.Intent.Tier != "a" || request.Intent.ChunkSize != 0 || identity.ObjectVersion != "" || identity.TransferID != request.TransferID || identity.EntryNonce == "" {
		t.Fatal("small pull created an OBJ invocation before the inline route was selected")
	}
	for _, id := range identities {
		if id != identity {
			t.Fatal("inline replay replaced the original transfer authority")
		}
	}
	if waitErr != nil || result.NativeRC != 0 {
		t.Fatalf("selected inline pull did not reach its exact terminal: native_rc=%d err=%v %s", result.NativeRC, waitErr, s.output(s.ctl))
	}
	if result.PID != r.before.PID || result.ProcessStart != r.before.ProcessStart {
		t.Fatal("completion belongs to a replacement invocation")
	}
	body, err := os.ReadFile(r.target)
	transferMust(t, err)
	if !bytes.Equal(body, r.body) {
		t.Fatal("inline receiver did not land the original complete source")
	}
	transferMust(t, r.watch.check())
	stream, err := s.js.Stream(ctx, jsstream.HistoryStreamName(s.sid))
	transferMust(t, err)
	rows, _, err := transferharness.ScanStream(ctx, stream, 0)
	transferMust(t, err)
	transferMust(t, validateTransferHistory(rows, identity.TransferID, identity.EntryNonce, transferDigest(r.body), int64(len(r.body))))
	observations, err := s.collectors["broker"].snapshot()
	transferMust(t, err)
	transferMust(t, validateTransferHistoryReceipts(rows, observations, identity.TransferID))
	for _, collector := range s.collectors {
		observations, err := collector.snapshot()
		transferMust(t, err)
		for _, o := range observations {
			if o.TransferID == identity.TransferID && o.ObjectVersion != "" {
				t.Fatal("inline transfer negotiated an object version")
			}
		}
	}
	var effects []x.Observation
	for _, role := range []string{"agent", "ctl"} {
		observations, err := s.collectors[role].snapshot()
		transferMust(t, err)
		effects = append(effects, observations...)
	}
	transferMust(t, validateTransferEffects(effects, identity.TransferID))
}
