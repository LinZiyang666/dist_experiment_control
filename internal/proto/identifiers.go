package proto

import (
	"fmt"
	"regexp"
)

// Identifier validation per architecture B.5 + requirements §7.4 / §7.5.
//
// sid: [a-z0-9-]{1,32}, leading [a-z], no trailing '-', not reserved,
//      no reserved prefix.
// nid: same character class; uniqueness is per-session, enforced by tetherd.
// actor: NATS user nkey public key, base32 with leading 'U', 56 chars
//        (1 prefix + 32 byte key + 2 byte CRC = 35 bytes → 56 base32 chars).

var (
	idCharset    = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	actorPattern = regexp.MustCompile(`^U[A-Z2-7]{55}$`)
)

var sidReserved = map[string]struct{}{
	"default": {}, "system": {}, "admin": {}, "global": {}, "all": {}, "*": {},
}

var sidReservedPrefixes = []string{"system-", "ctl-"}

// ValidateSID returns nil iff s is a syntactically valid session id.
// Uniqueness is checked separately by tetherd against the sessions table.
func ValidateSID(s string) error {
	if !idCharset.MatchString(s) {
		return fmt.Errorf("sid %q: must match [a-z0-9-]{1,32}", s)
	}
	if !isLowerAlpha(s[0]) {
		return fmt.Errorf("sid %q: must start with [a-z]", s)
	}
	if s[len(s)-1] == '-' {
		return fmt.Errorf("sid %q: must not end with '-'", s)
	}
	if _, bad := sidReserved[s]; bad {
		return fmt.Errorf("sid %q: reserved", s)
	}
	for _, p := range sidReservedPrefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return fmt.Errorf("sid %q: prefix %q is reserved", s, p)
		}
	}
	return nil
}

// ValidateNID applies the per-session node-id syntax. Uniqueness is per
// session (sid, nid) and enforced by tetherd.
func ValidateNID(s string) error {
	if !idCharset.MatchString(s) {
		return fmt.Errorf("nid %q: must match [a-z0-9-]{1,32}", s)
	}
	if !isLowerAlpha(s[0]) {
		return fmt.Errorf("nid %q: must start with [a-z]", s)
	}
	if s[len(s)-1] == '-' {
		return fmt.Errorf("nid %q: must not end with '-'", s)
	}
	return nil
}

// ValidateActorToken checks the syntactic shape of a NATS user nkey public key.
// Authoritative semantics (does this key actually exist? has it presented a
// valid signature?) are enforced by NATS during CONNECT.
func ValidateActorToken(s string) error {
	if !actorPattern.MatchString(s) {
		return fmt.Errorf("actor %q: must match U[A-Z2-7]{55}", s)
	}
	return nil
}

func isLowerAlpha(b byte) bool { return b >= 'a' && b <= 'z' }
