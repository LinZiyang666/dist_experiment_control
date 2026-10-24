package brokermaint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func proofFixture() (Record, Challenge, SignProof, VerifySignature) {
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{23}, ed25519.SeedSize))
	sign := func(b []byte) (string, error) { return hex.EncodeToString(ed25519.Sign(key, b)), nil }
	verify := func(b []byte, sig string) error {
		s, err := hex.DecodeString(sig)
		if err != nil || !ed25519.Verify(key.Public().(ed25519.PublicKey), b, s) {
			return ErrIdentity
		}
		return nil
	}
	r := Record{CutID: "cut-a", Holder: "holder-a", Home: "home-a", InstanceID: "instance-a", IssuerBootID: "issuer-boot", Mode: "quorum", Generation: 3, Phase: MaintPrepared, IssuedBootNS: int64(600 * time.Second), ExpiresBootNS: int64(600*time.Second + MaxLifetime), Term: 9, ConfigIndex: 12, VoterDigest: strings.Repeat("a", 64), BaselineDigest: strings.Repeat("b", 64), RequiredCapability: Capability}
	c := Challenge{Nonce: "nonce-a", ObserverBootID: "observer-boot", IssuerBootID: r.IssuerBootID, SentBootNS: int64(time.Second)}
	return r, c, sign, verify
}

// origin: hot-update S6 plan §7.1 / T39
func TestMaintenanceFixedCrossClockExpiry(t *testing.T) {
	if MaxLifetime != 8*time.Second {
		t.Fatal("maintenance lifetime changed")
	}
	r, c, sign, verify := proofFixture()
	p, err := MakeProof(r, c.Nonce, r.IssuerBootID, r.IssuedBootNS+int64(time.Second), 42, sign)
	if err != nil {
		t.Fatal(err)
	}
	now := c.SentBootNS + int64(2*time.Second)
	lease, err := ObserveProof(r, p, c, c.ObserverBootID, now, 42, nil, verify)
	if err != nil {
		t.Fatal(err)
	}
	want := c.SentBootNS + int64(MaxLifetime-time.Second)
	if lease.ExpiresLocalBootNS != want {
		t.Fatalf("foreign-clock mapping: expiry=%d want=%d", lease.ExpiresLocalBootNS, want)
	}
	if !Protects(lease, r, want-1) || Protects(lease, r, want) {
		t.Fatal("expiry boundary changed")
	}
	missing := lease
	missing.Proof = Proof{}
	if Protects(missing, r, want-1) {
		t.Fatal("missing proof protected home")
	}
	for _, wallOffset := range []time.Duration{-30 * time.Second, 30 * time.Second} {
		t.Run("wall_"+wallOffset.String(), func(t *testing.T) {
			// There is deliberately no wall-time argument in the production seam.
			if got, e := ObserveProof(r, p, c, c.ObserverBootID, now, 42, nil, verify); e != nil || got != lease {
				t.Fatal("wall offset affected lease", e)
			}
		})
	}
	t.Run("late_first_reply", func(t *testing.T) {
		if _, e := ObserveProof(r, p, c, c.ObserverBootID, want, 42, nil, verify); !errors.Is(e, ErrExpired) {
			t.Fatalf("late first proof granted lifetime: %v", e)
		}
	})
	t.Run("later_nonce_cannot_renew", func(t *testing.T) {
		later := c
		later.Nonce = "nonce-b"
		later.SentBootNS += int64(3 * time.Second)
		p2, e := MakeProof(r, later.Nonce, r.IssuerBootID, r.IssuedBootNS+int64(2*time.Second), 43, sign)
		if e != nil {
			t.Fatal(e)
		}
		got, e := ObserveProof(r, p2, later, c.ObserverBootID, later.SentBootNS, 42, &lease, verify)
		if e != nil || got.ExpiresLocalBootNS != want {
			t.Fatalf("late proof extended cached expiry: %d %v", got.ExpiresLocalBootNS, e)
		}
		if _, e = ObserveProof(r, p2, later, c.ObserverBootID, want, 42, &lease, verify); !errors.Is(e, ErrExpired) {
			t.Fatalf("expired cache renewed: %v", e)
		}
	})
	t.Run("leader_change_uses_original_cut", func(t *testing.T) {
		active := r
		active.Phase = MaintActive
		p2, e := MakeProof(active, c.Nonce, r.IssuerBootID, r.IssuedBootNS+int64(2*time.Second), 43, sign)
		if e != nil {
			t.Fatal(e)
		}
		got, e := ObserveProof(active, p2, c, c.ObserverBootID, now, 42, &lease, verify)
		if e != nil || got.ExpiresLocalBootNS > lease.ExpiresLocalBootNS {
			t.Fatalf("leadership renewed lease: %v", e)
		}
		newTerm := active
		newTerm.Term++
		p3, e := MakeProof(newTerm, c.Nonce, r.IssuerBootID, r.IssuedBootNS+int64(2*time.Second), 43, sign)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = ObserveProof(newTerm, p3, c, c.ObserverBootID, now, 42, &lease, verify); e == nil {
			t.Fatal("changed term redefined same cut")
		}
	})
	t.Run("boot_nonce_digest_cursor_signature", func(t *testing.T) {
		type proofCase struct {
			name  string
			r     Record
			p     Proof
			c     Challenge
			boot  string
			floor uint64
		}
		cases := []proofCase{
			{"unknown_local_boot", r, p, c, "", 42},
			{"changed_local_boot", r, p, c, "reboot", 42},
			{"missing_cursor", r, p, c, c.ObserverBootID, 43},
		}
		for _, mutation := range []struct {
			name   string
			change func(*Proof)
		}{
			{"changed_issuer_boot", func(p *Proof) { p.IssuerBootID = "reboot" }},
			{"wrong_nonce", func(p *Proof) { p.Nonce = "wrong" }},
			{"wrong_digest", func(p *Proof) { p.RecordDigest = strings.Repeat("c", 64) }},
			{"wrong_expiry", func(p *Proof) { p.ExpiresBootNS++ }},
		} {
			bad := p
			mutation.change(&bad)
			// A valid signature on a contradictory identity must still be refused.
			b, e := CanonicalProofBytes(bad)
			if e != nil {
				t.Fatal(e)
			}
			bad.Sig, e = sign(b)
			if e != nil {
				t.Fatal(e)
			}
			cases = append(cases, proofCase{mutation.name, r, bad, c, c.ObserverBootID, 42})
		}
		bad := p
		bad.Sig = strings.Repeat("0", 128)
		cases = append(cases, proofCase{"wrong_signature", r, bad, c, c.ObserverBootID, 42})
		wrongChallenge := c
		wrongChallenge.IssuerBootID = "wrong-boot"
		cases = append(cases, proofCase{"challenge_issuer_boot", r, p, wrongChallenge, c.ObserverBootID, 42})
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if _, e := ObserveProof(tc.r, tc.p, tc.c, tc.boot, now, tc.floor, nil, verify); e == nil {
					t.Fatal("invalid proof protected home")
				}
			})
		}
		if _, e := ObserveProof(r, p, c, c.ObserverBootID, now, 42, nil, nil); e == nil {
			t.Fatal("missing verifier accepted")
		}
	})
	t.Run("fixed_issuer_expiry_and_watchdog", func(t *testing.T) {
		for _, lifetime := range []time.Duration{MaxLifetime + 1, 120 * time.Second} {
			bad := r
			bad.ExpiresBootNS = bad.IssuedBootNS + int64(lifetime)
			if ValidateRecord(bad) == nil {
				t.Fatalf("accepted lifetime %v", lifetime)
			}
		}
		changed := r
		changed.ExpiresBootNS++
		if ValidateTransition(r, changed) == nil {
			t.Fatal("transition extended fixed expiry")
		}
		changed = r
		changed.ExpiresBootNS--
		if ValidateTransition(r, changed) == nil {
			t.Fatal("transition rewrote original expiry")
		}
		changed = r
		changed.IssuedBootNS++
		if ValidateTransition(r, changed) == nil {
			t.Fatal("transition rewrote original creation time")
		}
		if _, e := MakeProof(r, c.Nonce, "reboot", r.IssuedBootNS, 42, sign); e == nil {
			t.Fatal("rebooted issuer signed old cut")
		}
		if _, e := MakeProof(r, c.Nonce, r.IssuerBootID, r.ExpiresBootNS, 42, sign); !errors.Is(e, ErrExpired) {
			t.Fatal("expired issuer created proof", e)
		}
	})
	t.Run("overflow_and_reversed_clock", func(t *testing.T) {
		bad := c
		bad.SentBootNS = math.MaxInt64 - 1
		if _, e := ObserveProof(r, p, bad, c.ObserverBootID, math.MaxInt64, 42, nil, verify); e == nil {
			t.Fatal("overflow expiry accepted")
		}
		if _, e := ObserveProof(r, p, c, c.ObserverBootID, c.SentBootNS-1, 42, nil, verify); e == nil {
			t.Fatal("backward clock accepted")
		}
	})
}

// origin: hot-update S6 plan §7.1 / §9.1
func TestMaintenanceCanonicalAuthority(t *testing.T) {
	r, c, sign, _ := proofFixture()
	raw, e := CanonicalRecordBytes(r)
	if e != nil {
		t.Fatal(e)
	}
	if got, e := DecodeRecord(raw); e != nil || got != r {
		t.Fatal("canonical record did not round trip", e)
	}
	for _, bad := range [][]byte{append(raw, ' '), append(raw, raw...), []byte(strings.Replace(string(raw), "\"holder\":\"holder-a\"", "\"holder\":\"holder-a\",\"holder\":\"holder-a\"", 1)), []byte(strings.Replace(string(raw), "\"term\":9", "\"term\":9,\"unknown\":0", 1)), []byte(strings.Replace(string(raw), "\"ingress_server_id\":\"\",", "", 1)), []byte("null")} {
		if _, e := DecodeRecord(bad); e == nil {
			t.Fatal("noncanonical record accepted")
		}
	}
	// origin: hot-update S6 plan §7.1 / T39
	// Case folding would create two spellings for the same signed digest.
	badDigest := r
	badDigest.VoterDigest = strings.Repeat("A", 64)
	if _, err := CanonicalRecordBytes(badDigest); err == nil {
		t.Fatal("uppercase digest accepted")
	}
	ended := r
	ended.Phase = MaintEnded
	if ValidateTransition(r, ended) != nil || ValidateTransition(ended, r) == nil {
		t.Fatal("phase transition ordering changed")
	}
	p, e := MakeProof(r, c.Nonce, r.IssuerBootID, r.IssuedBootNS, 42, sign)
	if e != nil {
		t.Fatal(e)
	}
	b, e := CanonicalProofBytes(p)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(b, []byte("sig")) {
		t.Fatal("signature included in signed proof")
	}
	proofDigest, e := ProofDigest(p)
	if e != nil || proofDigest == p.RecordDigest || len(proofDigest) != 64 {
		t.Fatal("proof digest failed to bind proof bytes", e)
	}
	// Reflect every frozen field: a handwritten mutation list would silently miss
	// a later ABI addition. Sig alone is excluded from the authenticated body.
	for i := 0; i < reflect.TypeFor[Proof]().NumField(); i++ {
		field := reflect.TypeFor[Proof]().Field(i)
		cp := p
		v := reflect.ValueOf(&cp).Elem().Field(i)
		switch v.Kind() {
		case reflect.String:
			v.SetString("alternate")
		case reflect.Int64:
			v.SetInt(v.Int() + 1)
		case reflect.Uint64:
			v.SetUint(v.Uint() + 1)
		}
		got, err := CanonicalProofBytes(cp)
		if field.Name == "Sig" {
			if err != nil || !bytes.Equal(got, b) {
				t.Fatal("signature changes signed body")
			}
			continue
		}
		if err == nil && bytes.Equal(got, b) {
			t.Fatalf("unsigned proof field: %s", field.Name)
		}
	}
	request, e := CanonicalRequestBytes(r, p, "node-a", r.Home, "", strings.Repeat("c", 64))
	if e != nil {
		t.Fatal(e)
	}
	if _, e := CanonicalRequestBytes(r, p, "node-a", "other-home", "", strings.Repeat("c", 64)); e == nil {
		t.Fatal("request bound the wrong home")
	}
	ack, e := CanonicalAckBytes(r.CutID, c.Nonce, "node-a", r.Home, "", r.InstanceID, strings.Repeat("c", 64), strings.Repeat("d", 64), true, int64(MaxLifetime))
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(request, ack) || bytes.Contains(request, []byte("\"sig\"")) {
		t.Fatal("signature domain separation changed")
	}
	r.CutID = "cut\nforged"
	if _, e := CanonicalRecordBytes(r); e == nil {
		t.Fatal("newline identifier accepted")
	}
}

// origin: hot-update S6 plan §13
func FuzzMaintenanceRecordCanonical(f *testing.F) {
	r, _, _, _ := proofFixture()
	b, _ := CanonicalRecordBytes(r)
	f.Add(b)
	f.Add([]byte("{}"))
	f.Add([]byte("null"))
	f.Add(append(b, b...))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, e := DecodeRecord(b)
		if e != nil {
			return
		}
		out, e := CanonicalRecordBytes(r)
		if e != nil || !bytes.Equal(b, out) || !json.Valid(out) {
			t.Fatal("accepted noncanonical authority")
		}
	})
}

// Golden bytes come from the declared unsigned wire shapes, independent of
// either consumer adapter. Nested and outer signatures never enter a body.
// origin: hot-update S6 internal review rv-cluster-4
func TestMaintenanceSignedBytesHaveOneCanonicalGolden(t *testing.T) {
	r := Record{CutID: "cut", Holder: "holder", Home: "home", InstanceID: "instance", IssuerBootID: "boot", Mode: "quorum", Generation: 1, Phase: MaintPrepared, IssuedBootNS: 1, ExpiresBootNS: 8, Term: 1, ConfigIndex: 2, VoterDigest: strings.Repeat("a", 64), BaselineDigest: strings.Repeat("b", 64), RequiredCapability: Capability}
	const record = `{"cut_id":"cut","holder":"holder","home":"home","instance_id":"instance","issuer_boot_id":"boot","mode":"quorum","ingress_server_id":"","generation":1,"phase":1,"issued_boot_ns":1,"expires_boot_ns":8,"term":1,"config_index":2,"voter_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","baseline_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","required_capability":"broker-maint-v1"}`
	digest := sha256.Sum256([]byte(record))
	p := Proof{CutID: "cut", Nonce: "nonce", IssuerBootID: "boot", ResponseBootNS: 2, ExpiresBootNS: 8, RecordDigest: hex.EncodeToString(digest[:]), AppliedIndex: 3, Sig: "excluded"}
	proof := `{"cut_id":"cut","nonce":"nonce","issuer_boot_id":"boot","response_boot_ns":2,"expires_boot_ns":8,"record_digest":"` + p.RecordDigest + `","applied_index":3}`
	route := strings.Repeat("c", 64)
	raw, err := CanonicalRecordBytes(r)
	if err != nil || string(raw) != record {
		t.Fatalf("record golden: %s %v", raw, err)
	}
	raw, err = CanonicalProofBytes(p)
	if err != nil || string(raw) != "broker-maint-proof-v1\n"+proof {
		t.Fatalf("proof golden: %s %v", raw, err)
	}
	expected := `{"record":` + record + `,"proof":` + proof + `,"nid":"agent","connected_broker_id":"home","ingress_server_id":"","route_digest":"` + route + `"}`
	raw, err = CanonicalRequestBytes(r, p, "agent", "home", "", route)
	if err != nil || string(raw) != "broker-maint-request-v1\n"+expected {
		t.Fatalf("request golden: %s %v", raw, err)
	}
	challenge := `{"cut_id":"","nonce":"","issuer_boot_id":"","response_boot_ns":0,"expires_boot_ns":0,"record_digest":"","applied_index":0}`
	expected = `{"record":` + record + `,"proof":` + challenge + `,"nid":"agent","connected_broker_id":"home","ingress_server_id":"","route_digest":""}`
	raw, err = CanonicalRequestBytes(r, Proof{}, "agent", "home", "", "")
	if err != nil || string(raw) != "broker-maint-request-v1\n"+expected {
		t.Fatalf("challenge golden: %s %v", raw, err)
	}
	proofHash := sha256.Sum256([]byte("broker-maint-proof-v1\n" + proof))
	proofDigest := hex.EncodeToString(proofHash[:])
	expected = `{"cut_id":"cut","nonce":"nonce","nid":"agent","connected_broker_id":"home","ingress_server_id":"","instance_id":"instance","route_digest":"` + route + `","proof_digest":"` + proofDigest + `","accepted":true,"remaining_ns":5}`
	raw, err = CanonicalAckBytes("cut", "nonce", "agent", "home", "", "instance", route, proofDigest, true, 5)
	if err != nil || string(raw) != "broker-maint-ack-v1\n"+expected {
		t.Fatalf("ACK golden: %s %v", raw, err)
	}
	if actual, err := ProofDigest(p); err != nil || actual != proofDigest {
		t.Fatalf("proof digest golden: %s %v", actual, err)
	}
}
