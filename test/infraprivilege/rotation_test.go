//go:build linux && amd64

package infraprivilege

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
)

// The default lane proves the real policy codec/loader authorization boundary
// after durable policy reload. The integration lane additionally drives the
// installed RotatePolicy effect, busy admission and explicit reauthorization.
// origin: hot-update S9 plan §6.3 / §6.5 / T75 / M05
func TestInfraAuthorityRotationRevokesTickets(t *testing.T) {
	before, next := newAuthorityFixture(t), newAuthorityFixture(t)
	next.policy.Revision = before.policy.Revision + 1
	next.peer = before.peer
	for i := range next.policy.Keys {
		next.policy.Keys[i].KeyID += "-next"
	}
	old, err := infrapriv.NewPolicyStore(before.policy)
	if err != nil {
		t.Fatal(err)
	}
	unsent := signRequest(t, before, authorityRequest(before, c.Stage), infrapriv.LocalAuthority)
	lostReply := signRequest(t, before, authorityRequest(before, c.RecoverHost), infrapriv.LocalAuthority)
	if old.Authorize(unsent, before.peer) != nil || old.Authorize(lostReply, before.peer) != nil {
		t.Fatal("old tickets were not valid before rotation")
	}
	data, err := infrapriv.EncodePolicy(next.policy)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := infrapriv.DecodePolicy(data)
	if err != nil {
		t.Fatal(err)
	}
	current, err := infrapriv.NewPolicyStore(durable)
	if err != nil {
		t.Fatal(err)
	}
	for _, ticket := range []c.HostRequest{unsent, lostReply} {
		if err := current.Authorize(ticket, before.peer); !errors.Is(err, infrapriv.ErrPolicyStale) {
			t.Fatalf("revoked signed ticket accepted: %v", err)
		}
		// Keeping the old seed cannot mint a fresh-revision request, including
		// recovery of a pending transaction whose reply was lost.
		ticket.PolicyRevision = durable.Revision
		ticket = signRequest(t, before, ticket, infrapriv.LocalAuthority)
		if current.Authorize(ticket, before.peer) == nil {
			t.Fatal("old seed minted a new-revision ticket")
		}
		ticket.KeyID = next.policy.Keys[1].KeyID
		signing, err := c.SigningBytes(ticket)
		if err != nil {
			t.Fatal(err)
		}
		ticket.Signature = ed25519.Sign(before.keys[infrapriv.LocalAuthority], signing)
		if err := current.Authorize(ticket, before.peer); !errors.Is(err, infrapriv.ErrPermission) {
			t.Fatalf("fresh key ID borrowed by old seed: %v", err)
		}
	}
	fresh := authorityRequest(next, c.Inspect)
	fresh.KeyID = next.policy.Keys[1].KeyID
	signing, err := c.SigningBytes(fresh)
	if err != nil {
		t.Fatal(err)
	}
	fresh.Signature = ed25519.Sign(next.keys[infrapriv.LocalAuthority], signing)
	if err := current.Authorize(fresh, next.peer); err != nil {
		t.Fatalf("new policy read-only query: %v", err)
	}
	for _, field := range []string{"boot", "domain", "revision"} {
		bad := fresh
		switch field {
		case "boot":
			bad.HostBootID = "other-boot"
		case "domain":
			bad.Domain[0]++
		case "revision":
			bad.PolicyRevision--
		}
		signing, err := c.SigningBytes(bad)
		if err != nil {
			t.Fatal(err)
		}
		bad.Signature = ed25519.Sign(next.keys[infrapriv.LocalAuthority], signing)
		if err := current.Authorize(bad, next.peer); !errors.Is(err, infrapriv.ErrPolicyStale) {
			t.Fatalf("signed stale %s accepted: %v", field, err)
		}
	}
	t.Run("rotation-cannot-bypass-admission", func(t *testing.T) {
		request := authorityRequest(before, c.RotatePolicy)
		request.Payload = sha256.Sum256(data)
		request = signRequest(t, before, request, infrapriv.RootAuthority)
		persisted := false
		err := old.Rotate(request, next.policy, func(infrapriv.Policy, infrapriv.Policy) error { persisted = true; return nil })
		if !errors.Is(err, infrapriv.ErrPolicyStale) || persisted || !reflect.DeepEqual(old.Snapshot(), before.policy) {
			t.Fatal("unadmitted rotation persisted policy")
		}
	})
	t.Run("partial-canonical-policy", func(t *testing.T) {
		for _, bad := range [][]byte{data[:len(data)-1], append(append([]byte(nil), data...), '\n'), bytes.Replace(data, []byte(`"Revision":8`), []byte(`"Revision":8,"Revision":7`), 1)} {
			if bytes.Equal(bad, data) {
				t.Fatal("partial-policy negative control did not change bytes")
			}
			if _, err := infrapriv.DecodePolicy(bad); !errors.Is(err, infrapriv.ErrPolicy) {
				t.Fatalf("partial/ambiguous policy accepted: %v", err)
			}
		}
	})
	t.Run("credential-readers-are-closed", checkCredentialReaders)
	if installedRotationCheck != nil {
		installedRotationCheck(t)
	}
}

// A new helper loader must not grow a reader for account/broker/tunnel seeds.
// The file contents are canaries, not credentials; failure must happen before a
// nonroot/untrusted parent is interpreted as root-owned policy or key material.
func checkCredentialReaders(t *testing.T) {
	expected := []infrapriv.CredentialDescriptor{
		{Kind: infrapriv.SYSRead, Name: "infra-sys-read.nk", OwnerUID: 0, Mode: 0640, Encoding: "nkey-user", Reader: "sys-read-adapter"},
		{Kind: infrapriv.SYSStep, Name: "infra-sys-step.nk", OwnerUID: 0, Mode: 0640, Encoding: "nkey-user", Reader: "sys-step-adapter"},
		{Kind: infrapriv.LocalInfra, Name: "infra-local.nk", OwnerUID: 0, Mode: 0640, Encoding: "nkey-user", Reader: "local-infra-authority"},
		{Kind: infrapriv.LocalLaunch, Name: "infra-launch.nk", OwnerUID: 0, Mode: 0640, Encoding: "nkey-user", Reader: "fixed-launcher"},
	}
	if !reflect.DeepEqual(infrapriv.CredentialDescriptors(), expected) {
		t.Fatal("credential loader reader set changed")
	}
	dir := t.TempDir()
	canary := []byte("private-loader-canary-31")
	for _, row := range expected {
		if err := os.WriteFile(filepath.Join(dir, row.Name), canary, 0640); err != nil {
			t.Fatal(err)
		}
		key, err := infrapriv.LoadCredential(dir, row.Kind, uint32(os.Getegid()))
		if key != nil {
			_ = key.Close()
			t.Fatal("untrusted credential directory admitted")
		}
		if !errors.Is(err, infrapriv.ErrCredential) {
			t.Fatalf("loader refusal: %v", err)
		}
		assertNoCanary(t, fmt.Sprintf("%v %#v", err, err), canary)
	}
	if _, err := infrapriv.LoadPolicy(dir); !errors.Is(err, infrapriv.ErrCredential) {
		t.Fatalf("untrusted policy directory admitted: %v", err)
	}
}
