package proto

import (
	"math"
	"testing"
	"time"
)

// origin: hot-update S7 plan §1.2 / lead LQ2 / P01
func TestTransferRateBudgetKeepsZeroRateAndRejectsOverflow(t *testing.T) {
	cases := []struct {
		tier       string
		size, rate int64
		seconds    int64
	}{
		{"a", 0, 0, 30}, {"a", 8 << 20, 0, 30}, {"a", 15, 1, 30},
		{"b", 0, 0, 300}, {"b", 2 << 30, 0, 2108},
		{"b", 2 << 30, 4 << 20, 2108}, {"b", 2 << 30, math.MaxInt64, 2108},
		{"b", 1 << 30, 1 << 20, 2108}, {"b", (120 << 20) + 1, 1 << 20, 302},
		{"b", 120 << 20, 1 << 20, 300}, {"b", 1024, 1, 2108},
	}
	for _, c := range cases {
		got, err := TransferBudget(c.tier, c.size, c.rate)
		if err != nil || got != time.Duration(c.seconds)*time.Second {
			t.Errorf("tier=%s size=%d rate=%d: budget=%v err=%v, want %ds", c.tier, c.size, c.rate, got, err, c.seconds)
		}
		if c.rate == 0 && got != XferBudget(c.tier, c.size, XferPushLegs) {
			t.Error("zero requested rate changed legacy budget")
		}
	}
	for _, c := range []struct {
		tier       string
		size, rate int64
	}{
		{"a", 8 << 20, 1}, {"a", 16, 1}, {"a", 1024, 1}, {"a", 1025, 1}, {"b", 1025, 1}, {"b", 2 << 30, 1 << 20}, {"b", 1, -1},
		{"b", -1, 0}, {"b", math.MaxInt64, 1}, {"b", math.MaxInt64, math.MaxInt64},
		{"a", (8 << 20) + 1, 0}, {"b", (2 << 30) + 1, 0}, {"unknown", 0, 0},
	} {
		if got, err := TransferBudget(c.tier, c.size, c.rate); err == nil || got != 0 || err.Error() != CodeTransferBudgetExceeded {
			t.Errorf("unsafe tier=%s size=%d rate=%d admitted: budget=%v err=%v", c.tier, c.size, c.rate, got, err)
		}
	}
	for _, c := range []struct{ size, rate, seconds int64 }{{0, 1, 0}, {1, 1, 1}, {3, 2, 2}, {2 << 30, 0, 1024}} {
		if got, err := TransferLegBudget(c.size, c.rate); err != nil || got != time.Duration(c.seconds)*time.Second {
			t.Errorf("leg size=%d rate=%d: %s %v", c.size, c.rate, got, err)
		}
	}
	for _, c := range [][2]int64{{-1, 0}, {1, -1}, {1025, 1}, {math.MaxInt64, 1}} {
		if got, err := TransferLegBudget(c[0], c[1]); got != 0 || err == nil {
			t.Errorf("unsafe leg admitted: %v %s %v", c, got, err)
		}
	}
}

// origin: hot-update S7 plan §1.2 / P01
func TestTransferBudgetSealConsumesOriginalStartAndRequestElapsed(t *testing.T) {
	start := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	seal, err := NewTransferBudgetSeal("b", 0, 1<<20, start, 100*time.Second, false)
	if err != nil || seal.EffectiveBudgetNS != int64(2108*time.Second) || seal.RemainingNS != int64(2008*time.Second) || seal.SourceSealed {
		t.Fatalf("unknown-source provisional seal: %+v %v", seal, err)
	}
	consumed, err := ConsumeTransferBudget(seal, 2*time.Second+123*time.Nanosecond)
	if err != nil || consumed.RemainingNS != int64(2006*time.Second)-123 || consumed.StartedAt != start || consumed.RateLimitBPS != 1<<20 {
		t.Fatalf("request transit was renewed or rounded: %+v %v", consumed, err)
	}
	sealed, err := SealTransferSourceBudget(consumed, "b", SourceSeal{Size: 120 << 20, RateLimitBPS: 1 << 20}, 102*time.Second+123*time.Nanosecond)
	if err != nil || !sealed.SourceSealed || sealed.StartedAt != start || sealed.Size != 120<<20 ||
		sealed.EffectiveBudgetNS != int64(300*time.Second) || sealed.RemainingNS != int64(198*time.Second)-123 {
		t.Fatalf("source discovery reset original budget: %+v %v", sealed, err)
	}
	// A stricter remote deadline remains stricter even if size arithmetic grants more.
	consumed.RemainingNS = int64(7 * time.Second)
	sealed, err = SealTransferSourceBudget(consumed, "b", SourceSeal{Size: 120 << 20, RateLimitBPS: 1 << 20}, 102*time.Second)
	if err != nil || sealed.RemainingNS != int64(7*time.Second) {
		t.Fatalf("source sealing extended remaining: %+v %v", sealed, err)
	}
	if _, err := SealTransferSourceBudget(sealed, "b", SourceSeal{Size: 1, RateLimitBPS: 1 << 20}, 103*time.Second); err == nil {
		t.Fatal("sealed source rewritten")
	}
	if _, err := SealTransferSourceBudget(seal, "b", SourceSeal{Size: 1, RateLimitBPS: 0}, 1); err == nil {
		t.Fatal("source changed original rate")
	}
	if _, err := SealTransferSourceBudget(seal, "b", SourceSeal{Size: 1, RateLimitBPS: 1 << 20}, 300*time.Second); err == nil {
		t.Fatal("discovery revived expired budget")
	}
}

// origin: hot-update S7 plan §1.2 / P01
func TestTransferBudgetSealRefusesInvalidAndExpiredReceipts(t *testing.T) {
	start := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	for _, elapsed := range []time.Duration{-1, 300 * time.Second, time.Duration(math.MaxInt64)} {
		if _, err := NewTransferBudgetSeal("b", 1, 0, start, elapsed, true); err == nil {
			t.Errorf("invalid elapsed %s admitted", elapsed)
		}
	}
	if _, err := NewTransferBudgetSeal("b", 1, 0, time.Time{}, 0, true); err == nil {
		t.Fatal("missing start admitted")
	}
	if _, err := NewTransferBudgetSeal("a", 1, 0, start, 30*time.Second, false); err == nil {
		t.Fatal("inline provisional source renewed tier A")
	}
	seal, err := NewTransferBudgetSeal("b", 1, 0, start, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, elapsed := range []time.Duration{-1, 300 * time.Second, time.Duration(math.MaxInt64)} {
		if _, err := ConsumeTransferBudget(seal, elapsed); err == nil {
			t.Errorf("receipt elapsed %s admitted", elapsed)
		}
	}
	mutations := []func(*BudgetSeal){
		func(s *BudgetSeal) { s.StartedAt = time.Time{} }, func(s *BudgetSeal) { s.EffectiveBudgetNS = 0 },
		func(s *BudgetSeal) { s.EffectiveBudgetNS = int64(2109 * time.Second) },
		func(s *BudgetSeal) { s.RemainingNS = -1 }, func(s *BudgetSeal) { s.RemainingNS = int64(301 * time.Second) },
	}
	for i, mutate := range mutations {
		copy := seal
		mutate(&copy)
		if _, err := ConsumeTransferBudget(copy, 0); err == nil {
			t.Errorf("invalid seal %d admitted", i)
		}
	}
}

// origin: hot-update S7 plan §2.1 / P01 (T09 integration belongs to P02)
func TestTransferChunkGeometryReservesMinimumAndCountsCeiling(t *testing.T) {
	cases := []struct {
		size, ctl, agent, broker, stream, headers int64
		chunk, count                              uint32
	}{
		{2 << 30, 2 << 20, 2 << 20, 2 << 20, -1, 512, 1 << 20, 2048},
		{2 << 30, 32768 + 511, 2 << 20, 2 << 20, 0, 511, 32768, 65536},
		{65537, 2 << 20, 65536 + 512, 2 << 20, 0, 512, 65536, 2},
		{65537, 2 << 20, 2 << 20, 32768 + 512, 0, 512, 32768, 3},
		{65537, 2 << 20, 2 << 20, 2 << 20, 32768 + 512, 512, 32768, 3},
		{0, 2 << 20, 2 << 20, 2 << 20, 0, 512, 1 << 20, 0},
	}
	for _, c := range cases {
		chunk, count, err := TransferChunkGeometry(c.size, c.ctl, c.agent, c.broker, c.stream, c.headers)
		if err != nil || chunk != c.chunk || count != c.count {
			t.Errorf("geometry %+v: chunk=%d count=%d err=%v", c, chunk, count, err)
		}
	}
	for _, c := range [][6]int64{
		{-1, 2 << 20, 2 << 20, 2 << 20, 0, 512}, {math.MaxInt64, 2 << 20, 2 << 20, 2 << 20, 0, 512},
		{1, 0, 2 << 20, 2 << 20, 0, 512}, {1, 2 << 20, -1, 2 << 20, 0, 512}, {1, 2 << 20, 2 << 20, 0, 0, 512},
		{1, 32768 + 511, 2 << 20, 2 << 20, 0, 512}, {1, 2 << 20, 2 << 20, 2 << 20, 511, 512},
		{1, 2 << 20, 2 << 20, 2 << 20, 0, 0}, {1, math.MaxInt64, math.MaxInt64, math.MaxInt64, 0, math.MaxInt64},
	} {
		chunk, count, err := TransferChunkGeometry(c[0], c[1], c[2], c[3], c[4], c[5])
		if err == nil || chunk != 0 || count != 0 {
			t.Errorf("unsafe geometry %v admitted: %d %d %v", c, chunk, count, err)
		}
	}
	if ObjectRecoveryHorizon != 2228*time.Second || TransferResultRetention != 10*time.Minute || TransferSettlementAttempt != 5*time.Second {
		t.Fatal("recovery/settlement/retention budget drift")
	}
}
