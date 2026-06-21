// Package proto holds the wire/contract layer: subject names, request and
// response payload structs, and identifier validation.
//
// Two version axes (architecture J.1):
//
//   - ProtoVersion (int): bumped only on breaking-change releases; checked at
//     handshake. Strict same-version policy in v1.
//   - ReleaseVersion (string): the human release tag ("v0.3.2"); informational
//     only, overridden via -ldflags at build time.
package proto

const (
	// ProtoVersion is the wire/contract version. Bump only on breaking changes.
	//
	// v2 = the distributed-broker HA epic (proto v1→v2 HARD upgrade, architecture
	// distributed-broker §16.2): incompatible with v1 agents, rolled out as a
	// coordinated fleet-wide reinstall. The wire struct SHAPES are unchanged at
	// the v2 baseline (D0) — the breaking field additions (REGISTER epoch,
	// HomeDirective.cert_pins) land in D6.
	ProtoVersion = 2

	// SubjectVersionToken is the single source of truth for the version segment
	// ("v<ProtoVersion>") of every NATS subject. Every subject builder AND parser
	// derives from it — never hardcode "v1"/"v2" elsewhere. Enforced by the
	// determinism-lint TestNoStrayVersionLiteral tripwire and asserted in sync
	// with ProtoVersion by TestProtoVersionStillPositive.
	SubjectVersionToken = "v2"

	// SubjectPrefix is the global root for all NATS subjects, derived from
	// SubjectVersionToken so the version lives in exactly one place.
	SubjectPrefix = "tether." + SubjectVersionToken
)

// ReleaseVersion is the human-facing release tag. Defaults to dev value;
// overridden at build time via:
//
//	-ldflags "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.3.2"
var ReleaseVersion = "v0.0.0-dev"
