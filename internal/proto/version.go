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

import "github.com/LinZiyang666/tether/internal/upgradeprotocol"

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

// VersionLine is the single source of `tether version`'s FIRST LINE. It is a
// cross-version compatibility seam, not cosmetics (internal review S5): the
// upgrade smoke gate of every DEPLOYED agent parses this line out of the
// binary it is about to install — emitter (cmd/tether newVersionCmd) and
// parser (internal/agent smokeVersion) live in different releases of the
// binary, so the format is frozen here where both sides import it. The
// second whitespace-separated field must remain the bare release tag: an
// N-1 agent extracts it with strings.Fields(line)[1] and reports it as
// UpgradeForwardedResp.NewVersion, which ctl --wait compares against the
// new binary's registered ReleaseVersion. Changing this shape strands the
// FLEET's ability to upgrade INTO the release that changed it.
func VersionLine() string { return upgradeprotocol.VersionLine(ReleaseVersion, SubjectVersionToken) }

// ImageABIVersion is the epoch of the agent's runtime-layer image
// (agentimage.Bundle.Version reads it): a same-PID upgrade may only carry
// live work INTO a binary that speaks the same epoch. `tether version`
// prints it as its LAST line (ImageABILine) so an upgrading agent can ask
// the candidate before the flip; a candidate with no such line predates
// hot update and is refused when there is live work to carry
// (image_abi_unsupported — hot-update S3 plan §2.1). Append-only: the line
// stays last, earlier lines keep their shape (the N-1 smoke gate reads the
// first line only).
const ImageABIVersion = 1

// ImageABILine is the frozen shape of that last line: `image-abi v<N>`.
func ImageABILine() string { return upgradeprotocol.ImageABILine(ImageABIVersion) }

// ImageDomainsLine advertises which additive image domains this reader can
// restore. ABI v1 alone only proves the original exec domain is understood.
// These are reader declarations, including on Darwin; Linux data-plane
// availability is negotiated separately and cannot be inferred from this line.
// origin: hot-update S4 plan §5.2; hot-update S5 plan §5.2 / L1
// origin: hot-update S6 plan §9.1 / T46
// origin: hot-update S7 plan §6.2 / §8.4 / P01
func ImageDomainsLine() string { return transferImageDomainsLine }

// ImageVersionLines keeps the domain advertisement before the final ABI line.
// Version emitters can append these lines without changing the N-1 first or
// last line contract. The CLI emitter is wired by the upgrade owner.
// origin: hot-update S4 plan §5.2
func ImageVersionLines() []string {
	return upgradeprotocol.ImageVersionLines(ImageDomainsLine(), ImageABILine())
}
