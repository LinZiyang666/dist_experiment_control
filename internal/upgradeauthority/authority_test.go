// Session writers depend on upgradeauthority; keep their integration fixtures
// in the external test package so the tests cannot introduce the reverse edge.
// origin: S8 internal review FIX-VETTAGS F2
package upgradeauthority_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Bind the real leaf functions, rather than contractstub's compile substitutes,
// to the P00 signatures without modifying its independently owned contracts.
var (
	_ upgradeauthority.DeriveFunc            = upgradeauthority.Derive
	_ upgradeauthority.MatchFunc             = upgradeauthority.Match
	_ upgradeauthority.ReadCreationFunc      = upgradeauthority.ReadCreation
	_ upgradeauthority.CreationPredicateFunc = upgradeauthority.CreationPredicate
)

// origin: hot-update S8 plan §6.2 / P08
func TestIncarnationBindsEveryCreationField(t *testing.T) {
	salt := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	before := bytes.Clone(salt)
	const createdAt int64 = 1791820800123456789
	actual, err := upgradeauthority.Derive("lab", "owner", createdAt, salt)
	if err != nil {
		t.Fatal(err)
	}
	// Computed independently with Python struct.pack('>Q', length) for each
	// field, including the framed eight-byte timestamp. A round trip alone
	// would let exporter and reader share the same erroneous framing.
	const want = "38ce9560d7ff82cb3bcb5a8f424d0bcd21d5504fe6660fa4119584e603cda92d"
	if actual != want {
		t.Fatal("incarnation changed its frozen field framing")
	}
	changedSalt := bytes.Clone(salt)
	changedSalt[15]++
	for _, test := range []struct {
		name, sid, owner string
		at               int64
		salt             []byte
	}{
		{"sid", "other", "owner", createdAt, salt},
		{"owner", "lab", "other", createdAt, salt},
		{"creation time", "lab", "owner", createdAt + 1, salt},
		{"creation salt", "lab", "owner", createdAt, changedSalt},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed, err := upgradeauthority.Derive(test.sid, test.owner, test.at, test.salt)
			if err != nil || changed == actual {
				t.Fatal("a changed creation field retained the old incarnation")
			}
		})
	}
	a, err := upgradeauthority.Derive("a", "bc", createdAt, salt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := upgradeauthority.Derive("ab", "c", createdAt, salt)
	if err != nil || a == b {
		t.Fatal("ambiguous concatenation lost the SID/owner boundary")
	}
	if !bytes.Equal(salt, before) {
		t.Fatal("derivation mutated the caller's creation salt")
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestIncarnationMatchPreservesInheritedIdentity(t *testing.T) {
	inherited, replacement := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, test := range []struct {
		name, expected, actual string
		match                  bool
	}{
		{"ordinary first register learns", "", replacement, true},
		{"image keeps original creation", inherited, inherited, true},
		{"recreated SID is rejected", inherited, replacement, false},
		{"legacy empty reply is rejected", inherited, "", false},
		{"unknown is not a first register proof", "", "", false},
		{"malformed actual cannot be learned", "", "invalid", false},
		{"malformed expected cannot be replaced", "invalid", replacement, false},
		{"uppercase actual is noncanonical", inherited, strings.ToUpper(inherited), false},
		{"uppercase expected is noncanonical", strings.ToUpper(inherited), inherited, false},
		{"matching malformed values are rejected", "invalid", "invalid", false},
		{"nonhex actual is rejected", "", strings.Repeat("g", 64), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := upgradeauthority.Match(test.expected, test.actual)
			if test.match {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			assertAuthorityError(t, err, upgradetx.CodeSessionIncarnationMismatch, test.expected, test.actual)
		})
	}
}

// origin: hot-update S8 plan §6.2 / P08
func TestIncarnationRejectsIncompleteCreationWithoutDisclosure(t *testing.T) {
	salt := bytes.Repeat([]byte("private-material"), 2)[:16]
	for _, test := range []struct {
		name, sid, owner string
		at               int64
		salt             []byte
	}{
		{"missing SID", "", "owner", 1, salt},
		{"missing owner", "lab", "", 1, salt},
		{"missing timestamp", "lab", "owner", 0, salt},
		{"negative timestamp", "lab", "owner", -1, salt},
		{"missing salt", "lab", "owner", 1, nil},
		{"truncated salt", "lab", "owner", 1, salt[:15]},
		{"oversized salt", "lab", "owner", 1, append(bytes.Clone(salt), 0)},
		{"oversized SID", strings.Repeat("x", 33), "owner", 1, salt},
		{"oversized owner", "lab", strings.Repeat("x", 257), 1, salt},
		{"NUL material", "lab", "private-material\x00", 1, salt},
		{"invalid UTF8 material", "lab", "private-material\xff", 1, salt},
	} {
		t.Run(test.name, func(t *testing.T) {
			digest, err := upgradeauthority.Derive(test.sid, test.owner, test.at, test.salt)
			if digest != "" {
				t.Fatal("invalid creation returned an authority proof")
			}
			assertAuthorityError(t, err, upgradetx.CodeDeferred, "private-material", string(salt))
		})
	}
}

func assertAuthorityError(t *testing.T, err error, code string, private ...string) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != code || refusal.Accepted {
		t.Fatal("authority failure lost its refusal classification")
	}
	encoded, marshalErr := json.Marshal(refusal)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, material := range private {
		if len(material) > 3 && (strings.Contains(err.Error(), material) || bytes.Contains(encoded, []byte(material))) {
			t.Fatal("authority diagnostics disclosed creation material")
		}
	}
}
