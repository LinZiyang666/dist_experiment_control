package architecture_test

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// nats_server_security_floor_test.go — the versions this project SHIPS must be at or above
// a stated security floor, on both the server it exposes and the toolchain it is built with.
//
// origin: prerelease audit external review R2-B3, then widened by the main process.
//
// The reviewer's first cut rejected the single literal `v2.10.22`, which is where the
// regression actually landed. A literal is a DENYLIST, and a denylist answers only the
// question somebody already asked: pinning v2.10.21 — older, and inside the same upstream
// affected ranges — would have passed it unchanged. The value being guarded is a FLOOR, so
// the guard compares versions.
//
// Upstream lists v2.10.22 as affected by an unauthenticated WebSocket remote crash, a
// WebSocket compression memory DoS, and a JetStream admin-API authorization bypass:
//
//	https://github.com/nats-io/nats-server/security/advisories/GHSA-pq2q-rcw4-3hr6
//	https://github.com/nats-io/nats-server/security/advisories/GHSA-qrvq-68c2-7grw
//	https://github.com/nats-io/nats-server/security/advisories/GHSA-fhg8-qxh5-7q3w
//
// This project fronts NATS WebSockets through Caddy on the public internet
// (docs/architecture.md), so the first two are reachable BEFORE authentication. Upstream
// also maintains only the current and previous minor (RELEASES.md), and 2.10 is in
// neither — an unsupported line receives no further fixes at all, which is the reason the
// floor is a minor and not just "past the listed advisories".
//
// IT IS A FLOOR, NOT A "NEWEST" PREFERENCE. Raising the pin is a deliberate act with its own
// verification (deploy drills 50/95 and the full sweep); this only refuses to go BELOW a
// line whose reason is written down.
const (
	natsServerFloorMajor = 2
	natsServerFloorMinor = 14
	// The toolchain floor exists for the same reason, one layer down. `go 1.25.0` in a
	// go.mod is a MINIMUM REQUIREMENT, not the compiler that produced the binary — and this
	// repo was built with go1.25.0 exactly, the first release of that line, which govulncheck
	// reported as carrying 31 REACHABLE standard-library vulnerabilities (net/http,
	// crypto/tls, net/url, crypto/x509 — the whole public-facing path). A pinned `toolchain`
	// line is what makes the compiler a decision instead of whatever the builder happened to
	// have installed.
	goToolchainFloorMajor = 1
	goToolchainFloorMinor = 26
	// Go 1.26.4, .5 and .6 each shipped standard-library security fixes; the
	// last of those is therefore the patch-level floor for a release binary.
	// Source: https://go.dev/doc/devel/release#go1.26.0
	goToolchainFloorPatch = 6
)

var (
	natsServerRequire = regexp.MustCompile(`github\.com/nats-io/nats-server/v2 v(\d+)\.(\d+)\.(\d+)`)
	goToolchainLine   = regexp.MustCompile(`(?m)^toolchain go(\d+)\.(\d+)\.(\d+)\s*$`)
)

func TestPublicWebSocketServerIsAtOrAboveTheSecurityFloor(t *testing.T) {
	body := readGoMod(t)
	m := natsServerRequire.FindStringSubmatch(body)
	if m == nil {
		t.Fatal("go.mod has no github.com/nats-io/nats-server/v2 requirement; this guard cannot " +
			"see the version it exists to bound, which is indistinguishable from passing")
	}
	major, minor := atoiOrFail(t, m[1]), atoiOrFail(t, m[2])
	if major < natsServerFloorMajor || (major == natsServerFloorMajor && minor < natsServerFloorMinor) {
		t.Fatalf("nats-server is pinned to v%s.%s.%s, below the v%d.%d security floor.\n\n"+
			"This project exposes NATS over public WebSockets, and the lines below the floor carry "+
			"pre-auth remote crash / memory-DoS advisories and are outside upstream's supported "+
			"window. A deploy-drill regression must be fixed ON a supported version, not by "+
			"restoring one that is known-affected — 'the reason we upgraded is obsolete' does not "+
			"imply 'any older version may ship'.", m[1], m[2], m[3],
			natsServerFloorMajor, natsServerFloorMinor)
	}
}

func TestTheBuildToolchainIsPinnedAtOrAboveTheSecurityFloor(t *testing.T) {
	body := readGoMod(t)
	m := goToolchainLine.FindStringSubmatch(body)
	if m == nil {
		t.Fatal("go.mod has NO `toolchain` line.\n\n" +
			"The `go` directive is a minimum REQUIREMENT, not the compiler that builds the release " +
			"binary: without a toolchain line the binary carries whatever standard library the " +
			"builder happened to have installed. That is how this repo shipped a go1.25.0 build " +
			"with 31 reachable stdlib vulnerabilities while go.mod looked entirely current.")
	}
	major, minor := atoiOrFail(t, m[1]), atoiOrFail(t, m[2])
	if major < goToolchainFloorMajor || (major == goToolchainFloorMajor && minor < goToolchainFloorMinor) {
		t.Fatalf("toolchain is pinned to go%s.%s.%s, below the go%d.%d floor; the network-facing "+
			"standard library (net/http, crypto/tls, crypto/x509) is part of the attack surface "+
			"this project exposes", m[1], m[2], m[3], goToolchainFloorMajor, goToolchainFloorMinor)
	}
}

// TestTheBuildToolchainIncludesTheSecurityPatchFloor is deliberately separate
// from the developer's minor-line support check above. A security floor that
// compares only major/minor accepts go1.26.0, even though official Go release
// notes record security fixes in 1.26.4 through 1.26.6. The current pin is safe;
// this companion guard makes a same-minor downgrade mechanically visible.
func TestTheBuildToolchainIncludesTheSecurityPatchFloor(t *testing.T) {
	m := goToolchainLine.FindStringSubmatch(readGoMod(t))
	if m == nil {
		t.Fatal("go.mod has no parseable toolchain line")
	}
	major, minor, patch := atoiOrFail(t, m[1]), atoiOrFail(t, m[2]), atoiOrFail(t, m[3])
	if major < goToolchainFloorMajor ||
		(major == goToolchainFloorMajor && minor < goToolchainFloorMinor) ||
		(major == goToolchainFloorMajor && minor == goToolchainFloorMinor && patch < goToolchainFloorPatch) {
		t.Fatalf("toolchain go%d.%d.%d is below the go%d.%d.%d security-patch floor",
			major, minor, patch, goToolchainFloorMajor, goToolchainFloorMinor, goToolchainFloorPatch)
	}
}

// TestTheOperatorRunbookDoesNotRecommendAnAffectedServerLine extends the same
// floor to the production-facing install guide. The executable pins can agree
// while the runbook still tells an operator that the old affected pin is the
// deliberate deployment choice, which is an actionable downgrade instruction.
func TestTheOperatorRunbookDoesNotRecommendAnAffectedServerLine(t *testing.T) {
	body, err := os.ReadFile("../../docs/broker-ops.md")
	if err != nil {
		t.Fatal(err)
	}
	mentions := regexp.MustCompile(`nats-server v(\d+)\.(\d+)\.(\d+)`).FindAllStringSubmatch(string(body), -1)
	if len(mentions) == 0 {
		t.Fatal("broker-ops.md names no nats-server version; the production runbook has drifted out of this guard's view")
	}
	for _, m := range mentions {
		major, minor := atoiOrFail(t, m[1]), atoiOrFail(t, m[2])
		if major < natsServerFloorMajor || (major == natsServerFloorMajor && minor < natsServerFloorMinor) {
			t.Fatalf("broker-ops.md still recommends or justifies %q, below the v%d.%d supported security floor; "+
				"an operator following the release runbook can undo the executable pin upgrade",
				m[0], natsServerFloorMajor, natsServerFloorMinor)
		}
	}
}

func TestCISetupGoUnderstandsTheToolchainDirective(t *testing.T) {
	body, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	matches := regexp.MustCompile(`actions/setup-go@v(\d+)`).FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		t.Fatal("ci.yml contains no setup-go action; release toolchain selection is unguarded")
	}
	for _, m := range matches {
		if atoiOrFail(t, m[1]) < 6 {
			t.Fatalf("ci.yml uses %q, but setup-go learned to honor go.mod's toolchain directive in v6; "+
				"older actions read only the go directive and initially install the zero-patch minimum", m[0])
		}
	}
}

// TestTheSecurityFloorGuardCanActuallyFail is the non-vacuity control. A floor check whose
// regex silently stopped matching would pass for any pin at all — including the one it was
// written for.
func TestTheSecurityFloorGuardCanActuallyFail(t *testing.T) {
	const affected = "	github.com/nats-io/nats-server/v2 v2.10.22 // indirect"
	m := natsServerRequire.FindStringSubmatch(affected)
	if m == nil {
		t.Fatal("the nats-server regex no longer matches a real require line")
	}
	if got := fmt.Sprintf("%s.%s", m[1], m[2]); got != "2.10" {
		t.Fatalf("regex extracted %q from a v2.10.22 line", got)
	}
	if atoiOrFail(t, m[2]) >= natsServerFloorMinor {
		t.Fatal("the floor has been lowered to admit v2.10.x; that is the exact version the " +
			"external review rejected, and lowering it needs a written reason here, not a silent edit")
	}
	const stale = "toolchain go1.25.14"
	tm := goToolchainLine.FindStringSubmatch(stale + "\n")
	if tm == nil {
		t.Fatal("the toolchain regex no longer matches a real toolchain line")
	}
	if atoiOrFail(t, tm[2]) >= goToolchainFloorMinor {
		t.Fatal("the toolchain floor no longer rejects go1.25.x")
	}
}

func readGoMod(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "module github.com/LinZiyang666/tether") {
		t.Fatal("read something that is not this repo's go.mod")
	}
	return string(body)
}

func atoiOrFail(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("version component %q is not a number: %v", s, err)
	}
	return n
}
