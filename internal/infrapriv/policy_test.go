// Real canonical codecs and signatures; the peer DTO is varied adversarially.
// Persistence callbacks record policy effects, without claiming journal fsync.
// Root/multi-UID end-to-end authorization belongs to P31's privileged harness.
package infrapriv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §2.2 / §6.3 / T07
func TestInfraPeerAndPolicyAuthorization(t *testing.T) {
	policy, keys := policyFixture(t)
	peer := c.Peer{PID: 42, UID: policy.ServiceUID, GID: policy.ServiceGID, BootID: policy.HostBootID, StartTicks: 91}
	store, err := NewPolicyStore(policy)
	if err != nil {
		t.Fatal(err)
	}
	r := signedRequest(t, requestFixture(c.Stage), "local", keys[LocalAuthority])
	if err := store.Authorize(r, peer); err != nil {
		t.Fatalf("valid local authority: %v", err)
	}
	for _, change := range []struct {
		name string
		edit func(*c.Peer)
	}{
		{"ordinary-uid", func(p *c.Peer) { p.UID++ }}, {"wrong-gid", func(p *c.Peer) { p.GID++ }},
		{"root-with-service-key", func(p *c.Peer) { p.UID = 0; p.GID = 0 }}, {"cross-boot-peer", func(p *c.Peer) { p.BootID = "other-boot" }},
		{"missing-pid", func(p *c.Peer) { p.PID = 0 }}, {"missing-start", func(p *c.Peer) { p.StartTicks = 0 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := peer
			change.edit(&bad)
			if err := store.Authorize(r, bad); err == nil {
				t.Fatal("unauthorized peer accepted")
			}
		})
	}
	t.Run("every-signed-field", func(t *testing.T) {
		for _, rule := range c.HostOpDescriptors() {
			opRequest := signedRequest(t, requestFixture(rule.Op), "local", keys[LocalAuthority])
			if rule.Op == c.RotatePolicy || rule.Op == c.UpdateSupport {
				opRequest = signedRequest(t, requestFixture(rule.Op), "root", keys[RootAuthority])
			}
			if rule.Op == c.LaunchClaim || rule.Op == c.LaunchAck {
				opRequest = signedRequest(t, requestFixture(rule.Op), "launch", keys[LaunchAuthority])
			}
			opPeer := peer
			if rule.Op == c.RotatePolicy || rule.Op == c.UpdateSupport {
				opPeer.UID = 0
				opPeer.GID = 0
			}
			if err := store.Authorize(opRequest, opPeer); err != nil {
				t.Fatalf("op %d baseline: %v", rule.Op, err)
			}
			for i := 0; i < reflect.TypeFor[c.HostRequest]().NumField(); i++ {
				name := reflect.TypeFor[c.HostRequest]().Field(i).Name
				t.Run(strings.Join([]string{strconv.Itoa(int(rule.Op)), name}, "/"), func(t *testing.T) {
					data, err := c.EncodeRequest(opRequest)
					if err != nil {
						t.Fatal(err)
					}
					bad, err := c.DecodeRequest(data)
					if err != nil {
						t.Fatal(err)
					}
					mutateValue(reflect.ValueOf(&bad).Elem().Field(i))
					if err := store.Authorize(bad, opPeer); err == nil {
						t.Fatalf("unsigned mutation accepted: op=%d field=%s", rule.Op, name)
					}
				})
			}
		}
	})
	t.Run("signature-independent-of-policy-matching", func(t *testing.T) {
		for _, field := range []string{"HostBootID", "Domain", "PolicyRevision", "KeyID"} {
			t.Run(field, func(t *testing.T) {
				original := requestFixture(c.Inspect)
				original.Expected = c.Process{}
				original = signedRequest(t, original, "local", keys[LocalAuthority])
				bad := original
				mutateValue(reflect.ValueOf(&bad).Elem().FieldByName(field))
				matching := clonePolicy(policy)
				matching.HostBootID, matching.Domain, matching.Revision = bad.HostBootID, bad.Domain, bad.PolicyRevision
				matching.Keys[1].KeyID = bad.KeyID
				matchingPeer := peer
				matchingPeer.BootID = bad.HostBootID
				verifier, err := NewPolicyStore(matching)
				if err != nil {
					t.Fatal(err)
				}
				if err := verifier.Authorize(bad, matchingPeer); !errors.Is(err, ErrPermission) {
					t.Fatalf("matching policy substituted for signature: %v", err)
				}
			})
		}
	})
	t.Run("valid-signature-stale-envelope", func(t *testing.T) {
		for _, field := range []string{"HostBootID", "Domain", "PolicyRevision", "KeyID"} {
			bad := r
			mutateValue(reflect.ValueOf(&bad).Elem().FieldByName(field))
			if field == "HostBootID" {
				bad.Expected.BootID = bad.HostBootID
			}
			bad = signedRequest(t, bad, bad.KeyID, keys[LocalAuthority])
			if err := store.Authorize(bad, peer); !errors.Is(err, ErrPolicyStale) {
				t.Fatalf("%s: stale signed request accepted: %v", field, err)
			}
		}
	})
	t.Run("same-uid-cannot-borrow-launch-or-admin-rights", func(t *testing.T) {
		bad := signedRequest(t, requestFixture(c.Stage), "launch", keys[LaunchAuthority])
		if err := store.Authorize(bad, peer); !errors.Is(err, ErrPermission) {
			t.Fatalf("launcher staged arbitrary component: %v", err)
		}
		bad = signedRequest(t, requestFixture(c.RotatePolicy), "local", keys[LocalAuthority])
		if err := store.Authorize(bad, peer); !errors.Is(err, ErrPermission) {
			t.Fatalf("local key rotated policy: %v", err)
		}
		bad = signedRequest(t, requestFixture(c.UpdateSupport), "local", keys[LocalAuthority])
		if err := store.Authorize(bad, peer); !errors.Is(err, ErrPermission) {
			t.Fatalf("local key updated root support: %v", err)
		}
		bad = signedRequest(t, requestFixture(c.LaunchClaim), "local", keys[LocalAuthority])
		if err := store.Authorize(bad, peer); !errors.Is(err, ErrPermission) {
			t.Fatalf("local key claimed launch: %v", err)
		}
		bad = signedRequest(t, requestFixture(c.Stage), "local", keys[LaunchAuthority])
		if err := store.Authorize(bad, peer); !errors.Is(err, ErrPermission) {
			t.Fatalf("key ID substituted another public key: %v", err)
		}
	})
	t.Run("root-management-can-use-the-fixed-account-issuer", func(t *testing.T) {
		shared := clonePolicy(policy)
		shared.Keys[3].PublicKey = bytes.Clone(shared.AccountPublicKey)
		verifier, err := NewPolicyStore(shared)
		if err != nil {
			t.Fatal(err)
		}
		root := peer
		root.UID, root.GID = 0, 0
		r := signedRequest(t, requestFixture(c.RotatePolicy), "root", keys[AccountAuthority])
		if err := verifier.Authorize(r, root); err != nil {
			t.Fatalf("root issuer management: %v", err)
		}
		if err := verifier.Authorize(r, peer); !errors.Is(err, ErrPermission) {
			t.Fatal("service UID borrowed root issuer key scope")
		}
	})
	t.Run("rotation-revokes-signed-unsent-and-lost-reply-tickets", func(t *testing.T) { exercisePolicyRotation(t, policy, keys, peer, false) })
	t.Run("partial-policy-update-seals-mutation-admission", func(t *testing.T) { exercisePolicyRotation(t, policy, keys, peer, true) })
	t.Run("rotation-requires-fresh-key-material", func(t *testing.T) {
		for _, retained := range []string{"key-id", "public-key"} {
			store, err := NewPolicyStore(policy)
			if err != nil {
				t.Fatal(err)
			}
			next := clonePolicy(policy)
			next.Revision++
			for i := range next.Keys {
				next.Keys[i].PublicKey, _ = testKey(t)
				next.Keys[i].KeyID += "-next"
			}
			next.AccountPublicKey = bytes.Clone(next.Keys[0].PublicKey)
			if retained == "key-id" {
				next.Keys[1].KeyID = policy.Keys[1].KeyID
			} else {
				next.Keys[1].PublicKey = bytes.Clone(policy.Keys[1].PublicKey)
			}
			data, err := EncodePolicy(next)
			if err != nil {
				t.Fatal(err)
			}
			r := requestFixture(c.RotatePolicy)
			r.Payload = sha256.Sum256(data)
			r = signedRequest(t, r, "root", keys[RootAuthority])
			root := peer
			root.UID, root.GID = 0, 0
			release, err := store.begin(r, root)
			if err != nil {
				t.Fatal(err)
			}
			err = store.Rotate(r, next, func(Policy, Policy) error { t.Fatal("retained key was persisted"); return nil })
			release()
			if err == nil {
				t.Fatalf("rotation retained %s", retained)
			}
		}
	})
	t.Run("rotation-keeps-local-identity-and-increases-epoch", func(t *testing.T) {
		for _, invalid := range []string{"boot", "domain", "revision", "uid", "gid"} {
			store, err := NewPolicyStore(policy)
			if err != nil {
				t.Fatal(err)
			}
			next := clonePolicy(policy)
			next.Revision++
			for i := range next.Keys {
				next.Keys[i].PublicKey, _ = testKey(t)
				next.Keys[i].KeyID += "-next"
			}
			next.AccountPublicKey = bytes.Clone(next.Keys[0].PublicKey)
			switch invalid {
			case "boot":
				next.HostBootID = "other-boot"
			case "domain":
				next.Domain[0]++
			case "revision":
				next.Revision = policy.Revision
			case "uid":
				next.ServiceUID++
			case "gid":
				next.ServiceGID++
			}
			for i := range next.Keys {
				if next.Keys[i].Scope != RootAuthority {
					next.Keys[i].UID, next.Keys[i].GID = next.ServiceUID, next.ServiceGID
				}
			}
			data, err := EncodePolicy(next)
			if err != nil {
				t.Fatal(err)
			}
			r := requestFixture(c.RotatePolicy)
			r.Payload = sha256.Sum256(data)
			r = signedRequest(t, r, "root", keys[RootAuthority])
			root := peer
			root.UID, root.GID = 0, 0
			release, err := store.begin(r, root)
			if err != nil {
				t.Fatal(err)
			}
			err = store.Rotate(r, next, func(Policy, Policy) error { t.Fatal("foreign/non-increasing policy persisted"); return nil })
			release()
			if err == nil {
				t.Fatalf("rotation changed local identity/epoch: %s", invalid)
			}
		}
	})
	t.Run("policy-codec-and-alias-isolation", func(t *testing.T) { exercisePolicyCodec(t, policy) })
}

func exercisePolicyRotation(t *testing.T, policy Policy, keys map[Scope]ed25519.PrivateKey, peer c.Peer, partial bool) {
	t.Helper()
	store, err := NewPolicyStore(policy)
	if err != nil {
		t.Fatal(err)
	}
	old := signedRequest(t, requestFixture(c.Stage), "local", keys[LocalAuthority])
	releaseOld, err := store.begin(old, peer)
	if err != nil {
		t.Fatal(err)
	}
	next := clonePolicy(policy)
	next.Revision++
	var private ed25519.PrivateKey
	for i := range next.Keys {
		public, secret := testKey(t)
		next.Keys[i].PublicKey = public
		next.Keys[i].KeyID += "-next"
		if next.Keys[i].Scope == LocalAuthority {
			private = secret
		}
		if next.Keys[i].Scope == AccountAuthority {
			next.AccountPublicKey = bytes.Clone(public)
		}
	}
	encoded, err := EncodePolicy(next)
	if err != nil {
		t.Fatal(err)
	}
	rotate := requestFixture(c.RotatePolicy)
	rotate.Payload = sha256.Sum256(encoded)
	rotate = signedRequest(t, rotate, "root", keys[RootAuthority])
	root := peer
	root.UID, root.GID = 0, 0
	if release, err := store.begin(rotate, root); !errors.Is(err, ErrBusy) {
		if release != nil {
			release()
		}
		t.Fatalf("rotation overlapped live mutation: %v", err)
	}
	releaseOld()
	releaseOld()
	release, err := store.begin(rotate, root)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if done, err := store.begin(old, peer); !errors.Is(err, ErrBusy) {
		if done != nil {
			done()
		}
		t.Fatalf("mutation entered during rotation: %v", err)
	}
	if err := store.Rotate(rotate, policy, func(Policy, Policy) error { t.Fatal("stale policy persisted"); return nil }); err == nil {
		t.Fatal("non-increasing revision accepted")
	}
	badNext := clonePolicy(next)
	badNext.Domain[0]++
	if err := store.Rotate(rotate, badNext, func(Policy, Policy) error { t.Fatal("foreign policy persisted"); return nil }); err == nil {
		t.Fatal("policy changed domain")
	}
	badNext = clonePolicy(next)
	badNext.ServiceUID++
	if err := store.Rotate(rotate, badNext, func(Policy, Policy) error { t.Fatal("foreign service policy persisted"); return nil }); err == nil {
		t.Fatal("policy changed service UID")
	}
	badNext = clonePolicy(next)
	badNext.HostBootID = "other"
	if err := store.Rotate(rotate, badNext, func(Policy, Policy) error { t.Fatal("cross-boot policy persisted"); return nil }); err == nil {
		t.Fatal("policy changed boot")
	}
	forged := rotate
	forged.Sequence++
	if err := store.Rotate(forged, next, func(Policy, Policy) error { t.Fatal("unadmitted request persisted"); return nil }); err == nil {
		t.Fatal("rotation did not bind admitted request digest")
	}
	deadline := testDeadline(t, time.Second)
	commits := 0
	backend := &PolicyBackend{Store: store, Resolve: func(d c.Digest, dl c.Deadline, stop c.Stop) (Policy, error) {
		if d != rotate.Payload || dl != deadline || stop != nil {
			t.Fatal("lost resolver budget/payload")
		}
		return next, nil
	}, Persist: func(before, after Policy, dl c.Deadline, stop c.Stop) error {
		commits++
		if dl != deadline || stop != nil || before.Revision != policy.Revision || after.Revision != next.Revision {
			t.Fatal("lost persistence epoch/budget")
		}
		if store.Authorize(old, peer) != nil {
			t.Fatal("old policy revoked before durable commit")
		}
		if err := store.Rotate(rotate, next, func(Policy, Policy) error { t.Fatal("duplicate concurrent commit"); return nil }); err == nil {
			t.Fatal("parallel rotation commit accepted")
		}
		if partial {
			return errors.New("unknown durable effect")
		}
		return nil
	}}
	receipt, err := backend.Do(rotate, deadline, nil)
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
	if partial {
		if !errors.Is(err, ErrPolicyStale) {
			t.Fatalf("partial commit: %v", err)
		}
		if !errors.Is(store.Authorize(old, peer), ErrPolicyStale) {
			t.Fatal("partial persistence admitted old ticket")
		}
		return
	}
	if err != nil || !receipt.Applied || store.Snapshot().Revision != next.Revision {
		t.Fatalf("rotation receipt=%+v err=%v", receipt, err)
	}
	if !errors.Is(store.Authorize(old, peer), ErrPolicyStale) {
		t.Fatal("signed unsent/lost-reply old ticket survived revision")
	}
	old.PolicyRevision = next.Revision
	old = signedRequest(t, old, "local", keys[LocalAuthority])
	if !errors.Is(store.Authorize(old, peer), ErrPolicyStale) {
		t.Fatal("revoked old key signed a new request")
	}
	fresh := requestFixture(c.Stage)
	fresh.PolicyRevision = next.Revision
	fresh = signedRequest(t, fresh, "local-next", private)
	if err := store.Authorize(fresh, peer); err != nil {
		t.Fatalf("new key: %v", err)
	}
	if _, err := backend.Do(rotate, deadline, nil); err == nil || commits != 1 {
		t.Fatal("old rotate replay caused second commit")
	}
}

func exercisePolicyCodec(t *testing.T, p Policy) {
	t.Helper()
	golden := []string{"Version", "HostBootID", "Domain", "Revision", "AccountPublicKey", "TrustSHA256", "ServiceUID", "ServiceGID", "Keys"}
	rows := PolicyDescriptors()
	if len(rows) != 2 || len(rows[0].Fields) != len(golden) {
		t.Fatal("policy descriptor changed")
	}
	for i, name := range golden {
		if rows[0].Fields[i].Name != name {
			t.Fatal("policy field order changed")
		}
	}
	data, err := EncodePolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePolicy(data)
	if err != nil || !reflect.DeepEqual(p, decoded) {
		t.Fatalf("policy roundtrip: %v", err)
	}
	for _, data := range [][]byte{append(bytes.Clone(data), '\n'), bytes.Replace(data, []byte(`"Revision":1`), []byte(`"Revision":1,"Revision":1`), 1),
		bytes.Replace(data, []byte(`"Version"`), []byte(`"version"`), 1), bytes.Replace(data, []byte(`"Keys"`), []byte(`"Unknown"`), 1),
		[]byte(`{"Version":1}`), append(bytes.Clone(data), 0xff)} {
		if _, err := DecodePolicy(data); err == nil {
			t.Fatal("noncanonical policy accepted")
		}
	}
	for _, edit := range []func(*Policy){
		func(p *Policy) { p.Version++ }, func(p *Policy) { p.ServiceUID = 0 }, func(p *Policy) { p.ServiceGID = 0 },
		func(p *Policy) { p.AccountPublicKey = nil }, func(p *Policy) { p.Keys = nil }, func(p *Policy) { p.Keys[0].Scope = 255 },
		func(p *Policy) { p.Keys = append(p.Keys, p.Keys[0]) }, func(p *Policy) { p.Keys[0].PublicKey[0]++ },
		func(p *Policy) { p.Keys[1].UID++ }, func(p *Policy) { p.Keys[3].GID++ },
	} {
		bad := clonePolicy(p)
		edit(&bad)
		if _, err := EncodePolicy(bad); err == nil {
			t.Fatal("invalid policy admitted")
		}
	}
	store, err := NewPolicyStore(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Keys[1].PublicKey[0]++
	snapshot := store.Snapshot()
	snapshot.Keys[1].PublicKey[0]++
	if reflect.DeepEqual(snapshot, store.Snapshot()) {
		t.Fatal("snapshot aliases store public keys")
	}
	// The store's state leaves only through Snapshot's deep copy (checked just
	// above). Encoded the way a status or log record would carry it, the store
	// must show nothing: an exported field would be a second, unlocked path to
	// the authority policy. (A bare Marshal(store) only checked err, which a
	// struct without exported fields or Marshal* methods can never return.)
	encoded, err := json.Marshal(struct{ Store *PolicyStore }{store})
	if err != nil || string(encoded) != `{"Store":{}}` {
		t.Fatalf("policy store exposed state to an encoder: %s %v", encoded, err)
	}
}

func policyFixture(t *testing.T) (Policy, map[Scope]ed25519.PrivateKey) {
	t.Helper()
	p := Policy{Version: 1, HostBootID: "test-boot", Domain: c.ID{2}, Revision: 1, ServiceUID: 1000, ServiceGID: 1000}
	p.TrustSHA256 = c.Digest{12}
	keys := make(map[Scope]ed25519.PrivateKey)
	for i, scope := range []Scope{AccountAuthority, LocalAuthority, LaunchAuthority, RootAuthority} {
		public, private := testKey(t)
		keys[scope] = private
		uid, gid := p.ServiceUID, p.ServiceGID
		if scope == RootAuthority {
			uid, gid = 0, 0
		}
		p.Keys = append(p.Keys, PolicyKey{KeyID: []string{"account", "local", "launch", "root"}[i], PublicKey: public, UID: uid, GID: gid, Scope: scope})
		if scope == AccountAuthority {
			p.AccountPublicKey = bytes.Clone(public)
		}
	}
	return p, keys
}

func testKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return public, private
}

func requestFixture(op c.HostOp) c.HostRequest {
	rule := c.HostOpDescriptors()[int(op)-1]
	r := c.HostRequest{Version: 1, Op: op, Component: rule.Components[0], Tx: c.ID{1}, Step: 1, Domain: c.ID{2}, Sequence: 1,
		PolicyRevision: 1, KeyID: "local", HostBootID: "test-boot", Nonce: c.ID{3}, RemainingNS: uint64(5 * time.Second)}
	for _, name := range append(rule.Required, rule.Optional...) {
		switch name {
		case "Expected":
			r.Expected = c.Process{BootID: r.HostBootID, PID: 123, StartTicks: 7, InvocationID: "invocation", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{4}, Cgroup: "/system.slice/fixed.service"}
		case "Candidate":
			kind := c.NATSBinary
			if r.Component == c.Caddy {
				kind = c.CaddyBinary
			}
			if r.Component == c.HostSupport {
				kind = c.HelperBinary
			}
			r.Candidate = c.Artifact{Kind: kind, Version: "candidate", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{5}, BinarySHA256: c.Digest{6}, CatalogSHA256: c.Digest{7}}
		case "Store":
			r.Store = c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5}
		default:
			reflect.ValueOf(&r).Elem().FieldByName(name).Set(reflect.ValueOf(c.Digest{8}))
		}
	}
	return r
}

func signedRequest(t *testing.T, r c.HostRequest, keyID string, key ed25519.PrivateKey) c.HostRequest {
	t.Helper()
	r.KeyID = keyID
	r.Signature = nil
	data, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(key, data)
	return r
}

func mutateValue(v reflect.Value) {
	switch v.Kind() {
	case reflect.Struct:
		mutateValue(v.Field(0))
	case reflect.Array:
		mutateValue(v.Index(0))
	case reflect.Slice:
		if v.Len() == 0 {
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		}
		mutateValue(v.Index(0))
	case reflect.String:
		v.SetString(v.String() + "-changed")
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Int32:
		v.SetInt(v.Int() + 1)
	default:
		panic("missing mutation kind")
	}
}

func testDeadline(t *testing.T, budget time.Duration) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
