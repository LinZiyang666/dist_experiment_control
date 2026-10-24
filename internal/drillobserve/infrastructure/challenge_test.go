package infrastructure

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"
)

// origin: S10 internal review ARCH-REDS R2
func TestInfrastructureChallengeUsesInjectedAbsoluteClock(t *testing.T) {
	const budget = int64(30 * time.Millisecond)
	request := append([]byte("TDI1"), make([]byte, 12)...)
	request[4] = 1
	request = binary.LittleEndian.AppendUint64(request, uint64(budget))
	for _, tc := range []struct {
		name string
		at   int64
		err  error
		pass bool
	}{
		{"known clock", 41, nil, true},
		{"missing epoch", 0, nil, false},
		{"overflow", math.MaxInt64, nil, false},
		{"clock unavailable", 41, errors.New("private-clock-error"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, seals := 0, 0
			var reply []byte
			handled := Request(request, func(d int64) error {
				calls++
				if d != tc.at+budget {
					t.Fatalf("challenge changed absolute budget: got %d", d)
				}
				return nil
			}, func() (int64, error) { return tc.at, tc.err }, func() (uint64, uint64) {
				seals++
				return 7, 0
			}, func(data []byte) { reply = append([]byte(nil), data...) })
			if !handled || seals != 1 || len(reply) != 32 || string(reply[:4]) != "TDIA" || binary.LittleEndian.Uint64(reply[16:24]) != 7 {
				t.Fatal("challenge lost its sealed response")
			}
			if clean := binary.LittleEndian.Uint64(reply[24:32]) == 0; clean != tc.pass || (calls == 1) != tc.pass {
				t.Fatalf("invalid clock signed complete: calls=%d clean=%v", calls, clean)
			}
		})
	}
	for _, missing := range []string{"clock", "sampler"} {
		t.Run("missing "+missing, func(t *testing.T) {
			clock := func() (int64, error) { return 41, nil }
			sample := func(int64) error { t.Fatal("incomplete composition reached a borrower"); return nil }
			if missing == "clock" {
				clock = nil
			} else {
				sample = nil
			}
			var reply []byte
			if !Request(request, sample, clock, func() (uint64, uint64) { return 7, 0 }, func(data []byte) { reply = data }) ||
				len(reply) != 32 || binary.LittleEndian.Uint64(reply[24:32]) == 0 {
				t.Fatal("incomplete composition signed a clean challenge")
			}
		})
	}
}

// origin: S10 internal review ARCH-REDS R2
func TestInfrastructureSamplingKeepsOneAbsoluteDeadline(t *testing.T) {
	var registry Registry
	registry.Rows = make(map[[16]byte]func(int64) error)
	now, calls := int64(10), 0
	clock := func() (int64, error) { return now, nil }
	for _, id := range [][16]byte{{2}, {1}} {
		registry.Register(id, func(d int64) error {
			calls++
			if d != 30 {
				t.Fatalf("borrower received a renewed deadline: %d", d)
			}
			// The final borrower consumes exactly the original absolute budget.
			now += 10
			return nil
		}, true)
	}
	if err := registry.Sample(30, true, true, clock); !errors.Is(err, ErrInfrastructureObservation) || calls != 2 {
		t.Fatal("last borrower certified an expired roster", calls, err)
	}
	now, calls = 30, 0
	if err := registry.Sample(30, true, true, clock); !errors.Is(err, ErrInfrastructureObservation) || calls != 0 {
		t.Fatal("expired admission reached a borrower", calls, err)
	}
	now, calls = 1, 0
	if err := registry.Sample(30, true, true, clock); err != nil || calls != 2 {
		t.Fatal("complete roster lost its original budget", calls, err)
	}
}
