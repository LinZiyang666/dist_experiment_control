package main

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestPickPublicHostPrecedence pins the architecture A.3 public_host
// fallback chain: explicit --public-host > yaml broker.public_host >
// yaml broker.domain > cobra default.
func TestPickPublicHostPrecedence(t *testing.T) {
	const cobraDefault = "localhost"

	cases := []struct {
		name           string
		flagChanged    bool
		flagVal        string
		yamlPublicHost string
		yamlDomain     string
		want           string
	}{
		{"explicit flag wins", true, "flag.example.com", "yaml-public.example.com", "domain.example.com", "flag.example.com"},
		{"yaml public_host wins over domain", false, cobraDefault, "yaml-public.example.com", "domain.example.com", "yaml-public.example.com"},
		{"yaml domain fallback when public_host empty", false, cobraDefault, "", "domain.example.com", "domain.example.com"},
		{"cobra default when neither set", false, cobraDefault, "", "", cobraDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newServeCmd()
			if tc.flagChanged {
				if err := cmd.Flags().Set("public-host", tc.flagVal); err != nil {
					t.Fatal(err)
				}
			}
			got := pickPublicHost(cmd, tc.flagVal, tc.yamlPublicHost, tc.yamlDomain)
			if got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

// TestParsePortBand pins the frp.port_range parser. Empty → (0,0)
// (broker falls back to its 14000-14999 default); valid range
// passes through; bad inputs are rejected.
func TestParsePortBand(t *testing.T) {
	cases := []struct {
		in       string
		wantLow  int
		wantHigh int
		wantErr  bool
	}{
		{"", 0, 0, false},
		{"14000-14999", 14000, 14999, false},
		{" 14000 - 14999 ", 14000, 14999, false},
		{"14000", 0, 0, true},
		{"14000-", 0, 0, true},
		{"-14999", 0, 0, true},
		{"abc-14999", 0, 0, true},
		{"14999-14000", 0, 0, true},
		{"0-14999", 0, 0, true},
		{"14000-99999", 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			low, high, err := parsePortBand(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q; got %d-%d", tc.in, low, high)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if low != tc.wantLow || high != tc.wantHigh {
				t.Errorf("got %d-%d want %d-%d", low, high, tc.wantLow, tc.wantHigh)
			}
		})
	}
}

// TestPickFlagOrYamlPrecedence pins the standard "flag > yaml >
// default" precedence used by the rest of the serve flags. Cobra
// supplies the default into flagVal; the helper just chooses
// between flagVal (when --foo was explicitly passed) and yamlVal.
func TestPickFlagOrYamlPrecedence(t *testing.T) {
	cmd := &cobra.Command{Use: "x"}
	cmd.Flags().String("opt", "cobra-default", "")

	// Not changed → yaml wins when non-empty.
	if got := pickFlagOrYaml(cmd, "opt", "cobra-default", "yaml-val"); got != "yaml-val" {
		t.Errorf("yaml fallback: got %q want yaml-val", got)
	}
	// Not changed + yaml empty → cobra default.
	if got := pickFlagOrYaml(cmd, "opt", "cobra-default", ""); got != "cobra-default" {
		t.Errorf("default fallback: got %q want cobra-default", got)
	}
	// Changed → flag wins regardless of yaml.
	if err := cmd.Flags().Set("opt", "flag-val"); err != nil {
		t.Fatal(err)
	}
	if got := pickFlagOrYaml(cmd, "opt", "flag-val", "yaml-val"); got != "flag-val" {
		t.Errorf("explicit flag: got %q want flag-val", got)
	}
}
