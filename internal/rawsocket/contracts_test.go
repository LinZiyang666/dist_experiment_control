package rawsocket

import (
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Real: frozen signatures and owner sentinels. Stubbed: all S5 IO/state owners.
// Does not exercise sockets, goroutines, image codecs or carriers.
// origin: hot-update S5 plan §2 / §12.2 P0
func TestRawSocketContractStubsRejectOperations(t *testing.T) {
	s := &Socket{}
	cases := []struct {
		name string
		want error
		call func() []any
	}{
		{"Adopt", ErrIdentity, func() []any { a, b := Adopt(nil, Fingerprint{}); return []any{a, b} }},
		{"Connect", ErrIdentity, func() []any { a, b := Connect(netip.AddrPort{}, time.Time{}, nil, nil); return []any{a, b} }},
		{"CompleteConnect", ErrClosed, func() []any { a := s.CompleteConnect(time.Time{}, nil); return []any{a} }},
		{"Read", ErrClosed, func() []any { a, b := s.Read([]byte{1}); return []any{a, b} }},
		{"Write", ErrClosed, func() []any { a, b := s.Write([]byte{1}); return []any{a, b} }},
		{"SetReadDeadline", ErrClosed, func() []any { a := s.SetReadDeadline(time.Time{}); return []any{a} }},
		{"SetWriteDeadline", ErrClosed, func() []any { a := s.SetWriteDeadline(time.Time{}); return []any{a} }},
		{"CloseWrite", ErrClosed, func() []any { a := s.CloseWrite(); return []any{a} }},
		{"Shutdown", ErrClosed, func() []any { a := s.Shutdown(); return []any{a} }},
		{"Close", ErrClosed, func() []any { a := s.Close(); return []any{a} }},
		{"ValidateFile", ErrIdentity, func() []any { a := ValidateFile(nil, Fingerprint{}); return []any{a} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := tc.call()
			err, ok := values[len(values)-1].(error)
			if !ok || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.want.Error()) {
				t.Fatalf("stub returned %v instead of owner sentinel %v", values[len(values)-1], tc.want)
			}
			for i, value := range values[:len(values)-1] {
				if value != nil && !reflect.ValueOf(value).IsZero() {
					t.Errorf("stub result %d grants ownership or progress: %T", i, value)
				}
			}
		})
	}
}

// origin: hot-update S5 plan §2.1 / §12.2 P0
func TestRawSocketFingerprintStubHasNoIdentity(t *testing.T) {
	if got := (&Socket{}).Fingerprint(); got != (Fingerprint{}) {
		t.Fatal("stub must identify no socket")
	}
}
