package tokenhash

import "testing"

// Batch-A A11. Collapsing four copies into one is only safe if the VALUE is
// unchanged: internal/port persists these strings in
// port_allocations.token_hash, and proxysub persists them on subscription rows.
// A different value here does not fail loudly — it fails as "every agent's
// tunnel REGISTER is suddenly refused", fleet-wide, with error messages that
// point at token distribution rather than at a hash function.
//
// So these tests pin the algorithm to fixed, externally verifiable digests
// (`printf '%s' <input> | sha256sum`) rather than comparing against another Go
// expression, which would drift together with the code it is meant to guard.
//
// Batch-A review M4: the third entry originally held a FABRICATED digest that a
// special case then skipped, comparing against sha256.Sum256 instead — the exact
// thing the sentence above rejects, sitting three lines below it. Every value
// here is now reproducible with the command named above.
func TestSumMatchesPinnedDigests(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty string",
			in:   "",
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "abc",
			in:   "abc",
			want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{
			name: "token-shaped base64 input",
			in:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			want: "51643eac9777b63a7b268174d1fd4276daedec9bc9ea0bc6e5abf69047bc54f6",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Sum(tc.in)
			if got != tc.want {
				t.Errorf("Sum(%q) = %q, want %q — this value is PERSISTED in "+
					"port_allocations.token_hash; changing it invalidates every stored hash "+
					"(a data migration, not a refactor)", tc.in, got, tc.want)
			}
		})
	}
}

func TestSumAndSumBytesAgree(t *testing.T) {
	for _, in := range []string{"", "a", "token", "with spaces and \x00 nul"} {
		if Sum(in) != SumBytes([]byte(in)) {
			t.Errorf("Sum(%q)=%q disagrees with SumBytes=%q; the two entry points must be "+
				"interchangeable or callers will silently split into two schemes again",
				in, Sum(in), SumBytes([]byte(in)))
		}
	}
}

func TestSumIsLowercaseHex(t *testing.T) {
	got := Sum("anything")
	if len(got) != 64 {
		t.Fatalf("digest length %d, want 64", len(got))
	}
	for i, c := range got {
		isDigit := c >= '0' && c <= '9'
		isLowerHex := c >= 'a' && c <= 'f'
		if !isDigit && !isLowerHex {
			t.Fatalf("digest contains %q at %d; stored hashes are compared as strings, so case matters", c, i)
		}
	}
}
