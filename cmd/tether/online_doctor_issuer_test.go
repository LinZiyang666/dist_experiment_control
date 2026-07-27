package main

import (
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/nats-io/nkeys"
	"github.com/spf13/cobra"
)

// online_doctor_issuer_test.go (batch B, B4) — TODO(n3-online-doctor) is closed.
//
// The gap: `cluster doctor` auto-detects. On a LIVE cluster it takes the ONLINE branch, and that
// branch never ran clusterAuthIssuerSkewChecks — so a rotated-but-not-re-rendered account.nk was
// invisible on exactly the cluster state where it matters. The offline branch ran the check, but the
// offline branch is the pre-`init` path nobody takes after a rotation.
//
// The recorded blocker was real: the check needs a conf path, and the CLI only had its own --conf
// DEFAULT. On a custom-conf host that default names a different file, so running the check anyway
// would either emit a permanent ADVISORY or — worse — a FATAL "issuer skew" computed from a file the
// broker does not use. These tests pin the resolution order that makes the check safe to run.

func doctorCmdWithConfFlag(t *testing.T, confValue string, changed bool) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	var conf string
	cmd.Flags().StringVar(&conf, "conf", "/etc/nats/nats.conf", "nats.conf to preflight")
	if changed {
		if err := cmd.Flags().Set("conf", confValue); err != nil {
			t.Fatal(err)
		}
	}
	return cmd
}

func namesCheck(checks []clusteroffline.DoctorCheck, name string) *clusteroffline.DoctorCheck {
	for i := range checks {
		if checks[i].Name == name {
			return &checks[i]
		}
	}
	return nil
}

// TestOnlineDoctorSkipsLoudlyWhenTheBrokerReportsNoConf is the case the TODO was worried about, and
// the ONLY correct behaviour for it: say the check did not run. Silently skipping (the old
// behaviour) reads as a clean bill of health; guessing the CLI default would compute a verdict from
// a file the broker does not use.
func TestOnlineDoctorSkipsLoudlyWhenTheBrokerReportsNoConf(t *testing.T) {
	cmd := doctorCmdWithConfFlag(t, "", false) // --conf left at its default, broker reported nothing
	checks := onlineIssuerSkewChecks(cmd, t.TempDir(), "/etc/nats/nats.conf", "")

	c := namesCheck(checks, "auth-issuer-unverified")
	if c == nil {
		t.Fatalf("no auth-issuer-unverified check emitted; the online doctor silently reported "+
			"nothing about the issuer, which is what TODO(n3-online-doctor) recorded. Got %+v", checks)
	}
	if c.Status != clusteroffline.DoctorAdvisory {
		t.Errorf("an ABSENT check must be ADVISORY, not %v — FATAL here would fail doctor on every "+
			"host running a broker older than this CLI", c.Status)
	}
	if !strings.Contains(c.Detail, "NOT proof") {
		t.Errorf("the detail must say a skipped check is not a match, got %q", c.Detail)
	}
	// It must NOT have read the CLI's default path and produced a verdict from it.
	if namesCheck(checks, "auth-issuer-skew") != nil {
		t.Error("a skew verdict was computed even though no conf path was resolved — it can only " +
			"have come from the CLI's --conf DEFAULT, which on a custom-conf host is the wrong file")
	}
}

// TestOnlineDoctorUsesTheBrokerReportedConf is the fix itself: with the daemon reporting its conf,
// the check runs against the file the daemon actually reads.
func TestOnlineDoctorUsesTheBrokerReportedConf(t *testing.T) {
	dir, skewedConf := writeIssuerSkewConf(t)

	cmd := doctorCmdWithConfFlag(t, "", false)
	checks := onlineIssuerSkewChecks(cmd, dir, "/nonexistent/cli-default.conf", skewedConf)

	if namesCheck(checks, "auth-issuer-unverified") != nil && namesCheck(checks, "auth-issuer-skew") == nil {
		t.Fatalf("the broker reported %q but the check still reported itself as unverified — the "+
			"reported path is not being used, so the online doctor is still blind after a rotation. "+
			"Got %+v", skewedConf, checks)
	}
	if namesCheck(checks, "auth-issuer-skew") == nil {
		t.Fatalf("a conf whose rendered issuer does NOT match the on-disk account.nk produced no "+
			"skew check: %+v", checks)
	}
	if got := namesCheck(checks, "auth-issuer-skew"); got.Status != clusteroffline.DoctorFatal {
		t.Errorf("a KNOWN issuer mismatch must be FATAL (fail closed), got %v", got.Status)
	}
}

// TestExplicitConfFlagBeatsTheBrokerReport pins the precedence. An operator who passes --conf is
// diagnosing something specific — often "is THIS file the problem?" — and the daemon's own answer
// must not silently override them.
func TestExplicitConfFlagBeatsTheBrokerReport(t *testing.T) {
	dir, skewedConf := writeIssuerSkewConf(t)

	// The broker reports a path that does not exist; the operator explicitly points at the skewed
	// one. The skew must be found, which is only possible if the flag won.
	cmd := doctorCmdWithConfFlag(t, skewedConf, true)
	checks := onlineIssuerSkewChecks(cmd, dir, skewedConf, "/nonexistent/broker-reported.conf")

	if namesCheck(checks, "auth-issuer-skew") == nil {
		t.Fatalf("an explicit --conf was ignored in favour of the broker's reported path; the "+
			"operator cannot diagnose a specific file. Got %+v", checks)
	}
}

// TestOnlineDoctorIsQuietOnAMatchingConf is the non-vacuity half of the two tests above. Without it
// they would also pass against an implementation that emitted "skew" unconditionally.
func TestOnlineDoctorIsQuietOnAMatchingConf(t *testing.T) {
	dir, matching := writeIssuerMatchingConf(t)

	cmd := doctorCmdWithConfFlag(t, "", false)
	checks := onlineIssuerSkewChecks(cmd, dir, "/nonexistent/cli-default.conf", matching)

	for _, c := range checks {
		if c.Status == clusteroffline.DoctorFatal {
			t.Errorf("a conf whose issuer MATCHES the on-disk account.nk produced a FATAL %q: %s. "+
				"Every online doctor on a correctly-rendered cluster would now fail.", c.Name, c.Detail)
		}
	}
}

// writeIssuerSkewConf returns (secretsDir, confPath) for a nats.conf whose auth_callout issuer is a
// DIFFERENT account key than the on-disk account.nk — the rotated-but-not-re-rendered state. Built on
// skewFixture (r11_issuer_skew_test.go) so this file cannot drift from the detector's real inputs.
func writeIssuerSkewConf(t *testing.T) (secretsDir, confPath string) {
	t.Helper()
	dir, conf, _, brkPub := skewFixture(t, "PLACEHOLDER_A", "PLACEHOLDER_B")
	otherKP, _ := nkeys.CreateAccount()
	otherPub, _ := otherKP.PublicKey()
	// Rendered from a DIFFERENT account key: the issuer no longer matches the on-disk seed.
	if err := os.WriteFile(conf, []byte(confWith(otherPub, brkPub)), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, conf
}

// writeIssuerMatchingConf returns (secretsDir, confPath) rendered from the SAME account key.
func writeIssuerMatchingConf(t *testing.T) (secretsDir, confPath string) {
	t.Helper()
	dir, conf, acctPub, brkPub := skewFixture(t, "PLACEHOLDER_A", "PLACEHOLDER_B")
	if err := os.WriteFile(conf, []byte(confWith(acctPub, brkPub)), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, conf
}
