package xferclient

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

type recoveringPullAttachment struct {
	x.Transport
	opens              int
	statusAfterFailure bool
	refusal            string
}

func (p *recoveringPullAttachment) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	if p.opens > 0 && r.Op == x.OpStatus {
		p.statusAfterFailure = true
		if p.refusal != "" {
			if p.refusal == x.CodeUnknown {
				return x.Response{Status: &x.Status{State: x.StatusUnknown, Identity: r.Identity, Origin: r.Origin}}, x.Attempt{}, nil
			}
			return x.Response{Code: p.refusal}, x.Attempt{}, nil
		}
	}
	return p.Transport.Request(r, d)
}

// An accepted prepare is not authority to read bytes after an absence or a
// permission refusal. Only a live, matching original owner admits reattachment.
// origin: S7 internal review FIX-TIERA2 F6 / hot-update S7 plan §6.1 / §7 / T46
func TestTransferPreparedPullRecoveryRequiresLiveOriginalOwner(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePreparedPullRecoveryRefusal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "prepared pull recovery authority", g)
	testharness.AssertNoFDLeak(t, "prepared pull recovery authority", fd, 0)
}

func exercisePreparedPullRecoveryRefusal(t *testing.T) {
	t.Helper()
	for _, code := range []string{x.CodeUnknown, x.CodeNotMember} {
		f := newClientFixture(t, "pull")
		transport := &recoveringPullAttachment{Transport: f.transport, refusal: code}
		f.client.cfg.Transport = transport
		_, err := f.client.Run(testDeadline(t, 3*time.Second))
		f.close()
		want := code
		if code == x.CodeUnknown {
			want = x.CodeResultUnknown
		}
		if ErrorCode(err) != want || transport.opens != 1 || f.atomic.commits != 0 {
			t.Fatalf("recovery after %s bypassed original-owner authority: opens=%d commits=%d err=%v", code, transport.opens, f.atomic.commits, err)
		}
	}
}
func (p *recoveringPullAttachment) OpenData(id x.Identity, d x.Deadline) (jetstream.JetStream, error) {
	p.opens++
	if p.opens == 1 {
		return nil, errors.New("original data attachment temporarily unavailable")
	}
	if !p.statusAfterFailure {
		return nil, errors.New("attachment retried before original-owner status")
	}
	return p.Transport.OpenData(id, d)
}

// The actual client and fixed-object fixture remain original; only the first
// data attachment fails, as it can during the same-store restart.
// origin: S7 internal review FIX-TIERA2 F6
func TestTransferPreparedPullReconcilesLostDataAttachment(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePreparedPullAttachmentRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "prepared pull attachment recovery", g)
	testharness.AssertNoFDLeak(t, "prepared pull attachment recovery", fd, 0)
}

func exercisePreparedPullAttachmentRecovery(t *testing.T) {
	t.Helper()
	f := newClientFixture(t, "pull")
	defer f.close()
	transport := &recoveringPullAttachment{Transport: f.transport}
	f.client.cfg.Transport = transport
	receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("accepted pull exited before its original attachment recovered: code=%s err=%v", ErrorCode(err), err)
	}
	if !transport.statusAfterFailure || f.transport.counts[x.OpPermit] != 1 || f.transport.counts[x.OpPrepare] != 1 || f.atomic.commits != 1 {
		t.Fatal("attachment recovery changed the original invocation")
	}
}
