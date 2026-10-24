package xfersend

import (
	"bytes"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review rv-wiring F8
func TestTransferSenderInlineRateRefusesBeforeReservation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSenderInlineAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}
func exerciseSenderInlineAdmission(t *testing.T) {
	f := newSenderFixture(t, bytes.Repeat([]byte{'x'}, 1<<20))
	defer requireStop(t, f)
	config := f.config
	config.Intent.Tier, config.Intent.ChunkSize, config.Intent.RateLimitBPS = "a", 0, 32768
	config.Identity.ObjectVersion, config.Writer = "", nil
	config.Budget.RateLimitBPS = config.Intent.RateLimitBPS
	config.Budget.EffectiveBudgetNS = int64(proto.XferTimeoutTierA)
	config.Budget.RemainingNS = int64(proto.XferTimeoutTierA)
	capacity := &senderCapacity{}
	config.Capacity = capacity
	actor, err := New(config)
	if err == nil || actor != nil || !strings.Contains(err.Error(), x.CodeBudgetExceeded) || capacity.held {
		t.Fatalf("slow inline source admitted before original budget: actor=%T err=%v reservation=%v", actor, err, capacity.held)
	}
	if _, err := f.file.Stat(); err != nil {
		t.Fatal("rejected constructor consumed source ownership")
	}
}

// origin: S7 internal review rv-wiring F8
func TestTransferSenderInlineRateMatchesAdmissionBudget(t *testing.T) {
	for _, rate := range []int64{0, 32768, 65536, 69906, 1 << 20} {
		intent := x.Intent{Tier: "a", Size: 1 << 20, RateLimitBPS: rate}
		shared, sharedErr := proto.TransferBudget(intent.Tier, intent.Size, rate)
		source, sourceErr := sourceBudget(intent, intent.Size)
		if shared != source || (sharedErr == nil) != (sourceErr == nil) {
			t.Fatalf("rate=%d shared=%v/%v sender=%v/%v", rate, shared, sharedErr, source, sourceErr)
		}
	}
}
