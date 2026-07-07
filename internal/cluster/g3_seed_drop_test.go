package cluster

import (
	"reflect"
	"testing"
)

// g3_seed_drop_test.go — G3 #1 offline drop-only pure function (SeedEndpointsDropHosts). Adversarial
// table: departed hosts drop (port-stripped so wss://h:443 and tls://h drop together), VIP/LB hosts that
// are not departed are kept, and a no-match input is returned unchanged (the caller change-gates on it).

func TestSeedEndpointsDropHosts(t *testing.T) {
	tests := []struct {
		name     string
		eps      []string
		departed []string
		want     []string
	}{
		{
			name: "drop the departed peer's endpoint",
			eps:  []string{"wss://a:443", "wss://b:443"}, departed: []string{"b"},
			want: []string{"wss://a:443"},
		},
		{
			name: "keep a VIP/LB host that is not a departed peer",
			eps:  []string{"wss://a:443", "wss://vip.lb.example:443"}, departed: []string{"a"},
			want: []string{"wss://vip.lb.example:443"},
		},
		{
			name: "port-stripped: both schemes of a departed host drop together",
			eps:  []string{"wss://h:443", "tls://h:6222"}, departed: []string{"h"},
			want: []string{},
		},
		{
			name: "nothing matches -> returned unchanged (caller change-gates)",
			eps:  []string{"wss://a:443"}, departed: []string{"z"},
			want: []string{"wss://a:443"},
		},
		{
			name: "empty departed -> unchanged",
			eps:  []string{"wss://a:443"}, departed: nil,
			want: []string{"wss://a:443"},
		},
		{
			name: "empty endpoints -> empty",
			eps:  nil, departed: []string{"a"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SeedEndpointsDropHosts(tt.eps, tt.departed)
			// normalize nil vs empty-slice for the "nothing matches" / non-empty cases
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("SeedEndpointsDropHosts(%v,%v) = %v, want %v", tt.eps, tt.departed, got, tt.want)
			}
		})
	}
}
