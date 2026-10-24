package authcallout

import (
	"slices"
	"testing"

	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// managedSystemPrincipals renders the two product descriptors for fresh keys,
// exactly as the broker hands the loaded $SYS block to the callout seam.
func managedSystemPrincipals(t *testing.T) (read, step SystemPrincipal) {
	t.Helper()
	readPub, stepPub := freshUserPub(t), freshUserPub(t)
	descriptors, err := natsconf.SystemPrincipalDescriptors(natsconf.SystemPrincipals{ReadNkey: readPub, StepNkey: stepPub})
	if err != nil || len(descriptors) != 2 {
		t.Fatalf("product descriptors: %v", err)
	}
	convert := func(d natsconf.SystemPrincipalDescriptor) SystemPrincipal {
		return SystemPrincipal{Nkey: d.Nkey, Publish: d.Publish, Subscribe: d.Subscribe}
	}
	return convert(descriptors[0]), convert(descriptors[1])
}

func systemPrincipalHandler(t *testing.T, principals ...SystemPrincipal) *Handler {
	t.Helper()
	h, _ := freshHandler(t)
	h.SystemPrincipals = func() []SystemPrincipal { return principals }
	return h
}

// decodeAllowed returns the issued user claims, failing on any denial.
func decodeAllowed(t *testing.T, h *Handler, request string) (*jwt.AuthorizationResponseClaims, *jwt.UserClaims) {
	t.Helper()
	resp, err := jwt.DecodeAuthorizationResponseClaims(mustHandle(t, h, request))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("expected allow, got %q", resp.Error)
	}
	uc, err := jwt.DecodeUserClaims(resp.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	return resp, uc
}

// The admitted SYS identity is placed in $SYS with the managed lists verbatim,
// bound to the server's ephemeral subject and signed by the configured issuer.
// Its connection name is irrelevant: claiming an agent role cannot downgrade or
// mix the grant, and an ordinary key cannot select $SYS by any name.
// origin: hot-update S9 plan §3.3 / P12 / T27 — normal-queue SYS admission
func TestSystemPrincipalAdmissionIsKeyBoundAndExact(t *testing.T) {
	read, step := managedSystemPrincipals(t)
	h := systemPrincipalHandler(t, read, step)
	issuer, err := h.AccountKp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		principal SystemPrincipal
		name      string
	}{{read, ""}, {step, ""}, {read, "tether-agent:lab:lab-1"}, {step, "tether-cli:lab"}} {
		ephemeral := freshUserPub(t)
		resp, uc := decodeAllowed(t, h, signedRequest(t, ephemeral, tc.principal.Nkey, tc.name, ""))
		if uc.Audience != SystemAccount || uc.Subject != ephemeral || uc.Issuer != issuer || resp.Issuer != issuer || uc.Expires == 0 {
			t.Fatalf("SYS grant placement/binding: aud=%q sub=%q iss=%q exp=%d", uc.Audience, uc.Subject, uc.Issuer, uc.Expires)
		}
		if !slices.Equal([]string(uc.Pub.Allow), tc.principal.Publish) || !slices.Equal([]string(uc.Sub.Allow), tc.principal.Subscribe) ||
			len(uc.Pub.Deny) != 0 || len(uc.Sub.Deny) != 0 || uc.Resp != nil {
			t.Fatalf("SYS grant is not the managed descriptor verbatim: %+v", uc.Permissions)
		}
	}
	// An ordinary key keeps the ordinary placement whatever it is called.
	_, uc := decodeAllowed(t, h, signedRequest(t, freshUserPub(t), freshUserPub(t), "tether-cli", ""))
	if uc.Audience == SystemAccount {
		t.Fatal("an ordinary identity was placed in $SYS")
	}
	// Without the seam the same key is an unknown ordinary identity.
	h.SystemPrincipals = nil
	if reason := denyReason(t, mustHandle(t, h, signedRequest(t, freshUserPub(t), read.Nkey, "", ""))); reason == "" {
		t.Fatal("a SYS key was admitted with no system principal source")
	}
}

// Every refusal is opaque on the wire and none falls back to an ordinary role.
// origin: hot-update S9 plan §3.3 / P12 / M14-05..M14-09 analogues at the callout
func TestSystemPrincipalAdmissionRefusesForgedAmbiguousOrWidenedAuthority(t *testing.T) {
	read, step := managedSystemPrincipals(t)
	other := freshUserPub(t)
	widen := func(mutate func(*SystemPrincipal)) SystemPrincipal {
		clone := SystemPrincipal{Nkey: read.Nkey, Publish: slices.Clone(read.Publish), Subscribe: slices.Clone(read.Subscribe)}
		mutate(&clone)
		return clone
	}
	cases := []struct {
		name       string
		principals []SystemPrincipal
	}{
		{"listed twice", []SystemPrincipal{read, step, read}},
		{"shared reply root", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Subscribe = []string{SystemInboxRoot + ".>"} })}},
		{"another identity's root", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Subscribe = []string{SystemInboxRoot + "." + other + ".>"} })}},
		{"root prefix collision", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Subscribe = []string{SystemInboxRoot + "." + read.Nkey + "X.>"} })}},
		{"business subscribe", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Subscribe = append(p.Subscribe, "tether.>") })}},
		{"full publish wildcard", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, ">") })}},
		{"SYS publish subtree", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, "$SYS.REQ.>") })}},
		{"leading token wildcard", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, "*.*.*") })}},
		{"partial wildcard token", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, "$SYS.REQ.SERVER.a*.JSZ") })}},
		{"empty token", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, "$SYS..JSZ") })}},
		{"whitespace", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = append(p.Publish, "$SYS.REQ. JSZ") })}},
		{"no publish", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Publish = nil })}},
		{"no subscribe", []SystemPrincipal{widen(func(p *SystemPrincipal) { p.Subscribe = nil })}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := systemPrincipalHandler(t, tc.principals...)
			// The name would earn an ordinary unactivated grant if SYS refusal fell through.
			reason := denyReason(t, mustHandle(t, h, signedRequest(t, freshUserPub(t), read.Nkey, "tether-cli", "")))
			if reason != "system principal authentication denied" {
				t.Fatalf("widened or ambiguous SYS authority was not refused opaquely: %q", reason)
			}
		})
	}
	// The seam never replaces the nonce proof: the public key alone is not the identity.
	h := systemPrincipalHandler(t, read, step)
	attacker, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []string{
		forgedNkeyRequest(t, step.Nkey, attacker, "", ""),
		unsignedNkeyRequest(t, step.Nkey, "", ""),
	} {
		if reason := denyReason(t, mustHandle(t, h, request)); reason == "" {
			t.Fatal("an unproven SYS public key was admitted")
		}
	}
}
