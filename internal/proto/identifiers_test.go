package proto

import (
	"strings"
	"testing"
)

func TestValidateSID(t *testing.T) {
	good := []string{"a", "lab", "lab-1", "abcd1234", "x" + strings.Repeat("a", 31)}
	for _, s := range good {
		if err := ValidateSID(s); err != nil {
			t.Errorf("ValidateSID(%q) unexpected error: %v", s, err)
		}
	}
	bad := []struct {
		s    string
		want string // substring expected in error
	}{
		{"", "must match"},
		{"1lab", "start with"},
		{"-lab", "start with"},
		{"Lab", "must match"},
		{"lab_1", "must match"},
		{"lab-", "must not end with"},
		{strings.Repeat("a", 33), "must match"},
		{"default", "reserved"},
		{"system", "reserved"},
		{"admin", "reserved"},
		{"global", "reserved"},
		{"all", "reserved"},
		{"system-x", "reserved"},
		{"ctl-foo", "reserved"},
	}
	for _, c := range bad {
		err := ValidateSID(c.s)
		if err == nil {
			t.Errorf("ValidateSID(%q): expected error containing %q, got nil", c.s, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("ValidateSID(%q): error %q missing substring %q", c.s, err, c.want)
		}
	}
}

func TestValidateNID(t *testing.T) {
	// B.5 only constrains the character class; leading-digit, trailing-dash,
	// and reserved names (which apply to sid) are intentionally allowed for nid.
	good := []string{"a", "lab-1", "node-007", "1gpu", "node-", "default"}
	for _, s := range good {
		if err := ValidateNID(s); err != nil {
			t.Errorf("ValidateNID(%q) unexpected error: %v", s, err)
		}
	}
	bad := []string{"", "Lab", "lab_1", strings.Repeat("a", 33)}
	for _, s := range bad {
		if err := ValidateNID(s); err == nil {
			t.Errorf("ValidateNID(%q): expected error, got nil", s)
		}
	}
}

func TestValidateActorToken(t *testing.T) {
	// Construct a syntactically valid actor token: 'U' + 55 base32 chars.
	good := "U" + strings.Repeat("A", 55)
	if err := ValidateActorToken(good); err != nil {
		t.Errorf("ValidateActorToken(%q): unexpected error: %v", good, err)
	}

	bad := []string{
		"",
		"u" + strings.Repeat("A", 55), // lowercase prefix
		"U" + strings.Repeat("A", 54), // too short
		"U" + strings.Repeat("A", 56), // too long
		"U" + strings.Repeat("1", 55), // illegal base32 char (1)
	}
	for _, s := range bad {
		if err := ValidateActorToken(s); err == nil {
			t.Errorf("ValidateActorToken(%q): expected error, got nil", s)
		}
	}
}
