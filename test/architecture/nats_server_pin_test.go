package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// nats_server_pin_test.go — the nats-server this project TESTS against must be the
// nats-server it SHIPS.
//
// origin: prerelease audit increment 2 internal review, ops-upgrade/L16-F1.
//
// tether does not embed a broker: it requires an out-of-process nats-server, and this
// repository names that server in two independent places.
//
//	go.mod                  the LIBRARY, linked into `make test` as an embedded server
//	scripts/install.sh      the BINARY, downloaded onto an operator's broker host
//
// Nothing kept them equal, and they drifted four minor versions apart — go.mod on
// v2.14.x while every real deployment ran v2.10.22. Everything hermetic in this
// repository therefore measured a server nobody runs.
//
// THAT IS NOT AN ABSTRACT RISK. Twice now it has produced a real defect:
//
//   - docs/reviews/INDEX.md, 2026-08-06 (smalldisk) records mistaking go.mod's
//     nats-server (the embedded test server) for the v2.10.22 that production runs — one
//     of two lessons that entry draws about "two different things with the same name".
//   - the private reply inbox. Its isolation rested on `deny _INBOX.*.*.>`, which the
//     server installs lazily under a predicate that changed between v2.12 and v2.14.
//     Against the tested server the deny held; against the SHIPPED server it was never
//     installed, and a holder of the compatibility grant read every private reply
//     beneath any prefix it could name. Measured on 2.10.22 / 2.11.0 / 2.11.9 / 2.12.0.
//     The design no longer depends on that behaviour (auth.InboxRoot is a separate
//     root), but the mechanism that hid it for a whole release is this drift, and this
//     gate is the part of that fix which outlives the one bug.
//
// The simcluster image is deliberately NOT a third source: test/simcluster/lib/stage.sh
// greps the version out of scripts/install.sh precisely so it cannot drift. This gate
// asserts that it still does, because a hard-coded literal there would restore the
// three-way split.

// gate-control: TestNatsServerPinGateSeesADrift

var (
	goModPinRe    = regexp.MustCompile(`(?m)^\s*github\.com/nats-io/nats-server/v2\s+(v\d+\.\d+\.\d+)`)
	installPinRe  = regexp.MustCompile(`(?m)^NATS_SERVER_VERSION="\$\{TETHER_NATS_SERVER_VERSION:-(v\d+\.\d+\.\d+)\}"`)
	makefilePinRe = regexp.MustCompile(`(?m)^NATS_SERVER_VERSION \?= (v\d+\.\d+\.\d+)`)
)

// natsServerPins reads the version each source names. A source that cannot be parsed
// returns "" and is reported as a failure by the caller: silently skipping a source
// would turn this gate off exactly when someone reformats one of the files.
func natsServerPins(t *testing.T, root string) map[string]string {
	t.Helper()
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		return string(b)
	}
	first := func(re *regexp.Regexp, s string) string {
		m := re.FindStringSubmatch(s)
		if len(m) < 2 {
			return ""
		}
		return m[1]
	}
	return map[string]string{
		"go.mod (the server `make test` links)":          first(goModPinRe, read("go.mod")),
		"scripts/install.sh (the server operators run)":  first(installPinRe, read("scripts/install.sh")),
		"Makefile (the server `make nats-dev` installs)": first(makefilePinRe, read("Makefile")),
	}
}

func TestTheTestedNatsServerIsTheShippedNatsServer(t *testing.T) {
	root := repoRoot(t)
	pins := natsServerPins(t, root)

	var want string
	for source, got := range pins {
		if got == "" {
			t.Fatalf("could not read the nats-server version from %s.\n\n"+
				"The gate is blind rather than green: fix the pattern in this file, or restore the "+
				"line it reads.", source)
		}
		if want == "" {
			want = got
		}
	}
	for source, got := range pins {
		if got != want {
			t.Errorf("nats-server pin disagreement: %s says %s, another source says %s.\n\n"+
				"The server this repository tests against must be the server it ships, or every "+
				"hermetic assertion about server BEHAVIOUR — permission matching, deny filters, "+
				"subject rules — is being made about a binary no operator runs.", source, got, want)
		}
	}

	// And simcluster must keep single-sourcing from install.sh rather than hard-coding a
	// literal of its own. A literal here would be a third pin and this gate would not see
	// it, which is how the split arose in the first place.
	stage, err := os.ReadFile(filepath.Join(root, "test/simcluster/lib/stage.sh"))
	if err != nil {
		t.Fatalf("read test/simcluster/lib/stage.sh: %v", err)
	}
	if !strings.Contains(string(stage), "NATS_SERVER_VERSION") ||
		!strings.Contains(string(stage), "install.sh") {
		t.Error("test/simcluster/lib/stage.sh no longer derives the nats-server version from " +
			"scripts/install.sh.\n\n" +
			"It must keep grepping the installer rather than naming a version itself, or the " +
			"deploy-tier drills start exercising a different server from the one this release " +
			"ships — which is the exact drift this gate exists to prevent.")
	}
}

// gate-control for the gate above: a drift must be detected. Driven on a synthetic tree
// so it cannot depend on the repository being broken.
func TestNatsServerPinGateSeesADrift(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module x\n\ngo 1.25.0\n\nrequire (\n\tgithub.com/nats-io/nats-server/v2 v2.14.5\n)\n")
	write("scripts/install.sh", "#!/bin/sh\nNATS_SERVER_VERSION=\"${TETHER_NATS_SERVER_VERSION:-v2.10.22}\"\n")
	write("Makefile", "NATS_SERVER_VERSION ?= v2.14.5\n")

	pins := natsServerPins(t, dir)
	if pins["go.mod (the server `make test` links)"] != "v2.14.5" {
		t.Fatalf("the go.mod reader did not see v2.14.5: %v", pins)
	}
	if pins["scripts/install.sh (the server operators run)"] != "v2.10.22" {
		t.Fatalf("the install.sh reader did not see v2.10.22 — it cannot detect the exact drift "+
			"this release shipped with: %v", pins)
	}
	if pins["Makefile (the server `make nats-dev` installs)"] != "v2.14.5" {
		t.Fatalf("the Makefile reader did not see v2.14.5: %v", pins)
	}
	// The negative half: identical pins must NOT look like a drift.
	write("scripts/install.sh", "#!/bin/sh\nNATS_SERVER_VERSION=\"${TETHER_NATS_SERVER_VERSION:-v2.14.5}\"\n")
	agreed := natsServerPins(t, dir)
	seen := map[string]bool{}
	for _, v := range agreed {
		seen[v] = true
	}
	if len(seen) != 1 {
		t.Fatalf("three sources naming the same version were read as %v", agreed)
	}
}
