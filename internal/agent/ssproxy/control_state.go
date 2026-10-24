package ssproxy

import (
	"reflect"

	"github.com/LinZiyang666/tether/internal/proxyimage"
)

// ProxyState and ProxyCut retain the public boundary while their metadata and
// legacy decoding belong to the proxy image domain, independently of SS actors.
// origin: hot-update S5 plan §10.2 / L12 — merged metadata budget extraction.
type ProxyState = proxyimage.ProxyState
type ProxyCut = proxyimage.ProxyCut

func validProxyState(p ProxyState) bool { return proxyimage.ValidProxyState(p) }

// MatchIDs proves exact owner closure, including duplicate list entries. Counts
// alone would accept one omitted actor/material paired with one repeated ID.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func matchPrivateIDs(ids []byte, want [][16]byte) bool {
	if len(ids) != 16*len(want) {
		return false
	}
	expected := make(map[[16]byte]bool, len(want))
	for _, id := range want {
		expected[id] = true
	}
	actual := make(map[[16]byte]bool, len(want))
	for len(ids) > 0 {
		actual[[16]byte(ids[:16])] = true
		ids = ids[16:]
	}
	return len(expected) == len(want) && len(actual) == len(want) && reflect.DeepEqual(actual, expected)
}
