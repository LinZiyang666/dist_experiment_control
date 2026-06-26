package cluster

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
)

// join_bundle_c8_test.go (C8 D10) — DecodeJoinBundle preserves the expose-home/NATS-peer identity gate
// the deleted `cluster add` enforced: a bundle missing tunnel_addr or nats_route is rejected leader-
// side (authoritative, so a hand-crafted bundle can't bypass), while cert_fp stays optional.
func TestJoinBundleValidateRejectsEmptyTunnelNats(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(mut func(*JoinBundle)) string {
		nonce := "deadbeefdeadbeef"
		sig, err := auth.SignWithSeed(seed, JoinSignBytes("brk-b", pub, nonce))
		if err != nil {
			t.Fatal(err)
		}
		b := JoinBundle{
			NodeID: "brk-b", Name: "brk-b", NodeIdentPub: pub, NatsServerID: "brk-b",
			RaftAddr: "10.0.0.2:7400", NatsRoute: "nats://10.0.0.2:6222", TunnelAddr: "brk-b:7000",
			JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig),
		}
		mut(&b)
		enc, err := EncodeJoinBundle(b)
		if err != nil {
			t.Fatal(err)
		}
		return enc
	}

	// Complete bundle (no cert_fp) → accepted: cert_fp stays OPTIONAL (D6 backfills).
	if _, err := DecodeJoinBundle(mk(func(b *JoinBundle) { b.CertFP = "" })); err != nil {
		t.Errorf("a complete bundle with no cert_fp must be ACCEPTED (cert_fp optional): %v", err)
	}
	// Empty tunnel_addr → rejected.
	if _, err := DecodeJoinBundle(mk(func(b *JoinBundle) { b.TunnelAddr = "" })); err == nil || !strings.Contains(err.Error(), "tunnel_addr") {
		t.Errorf("empty tunnel_addr must be REJECTED naming the field, got %v", err)
	}
	// Empty nats_route → rejected.
	if _, err := DecodeJoinBundle(mk(func(b *JoinBundle) { b.NatsRoute = "" })); err == nil || !strings.Contains(err.Error(), "nats_route") {
		t.Errorf("empty nats_route must be REJECTED naming the field, got %v", err)
	}
}
