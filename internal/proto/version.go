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
	ProtoVersion = 1

	// SubjectPrefix is the global root for all v1 NATS subjects.
	SubjectPrefix = "tether.v1"
)

// ReleaseVersion is the human-facing release tag. Defaults to dev value;
// overridden at build time via:
//
//	-ldflags "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.3.2"
var ReleaseVersion = "v0.0.0-dev"
