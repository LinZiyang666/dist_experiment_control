// Package upgradeartifact names the two immutable images bound to a grant.
// A publication receipt is neither Start nor a daemon journal write. Identical
// SHAs from another grant do not authorize adoption of that publication.
// origin: hot-update S8 plan §2.2 / §5.2 / P00
package upgradeartifact

import (
	"context"
	"io"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type Receipt struct {
	Grant          upgradetx.Grant `json:"grant"`
	CanonicalPath  string          `json:"canonical_path"`
	SourcePath     string          `json:"source_path"`
	TargetPath     string          `json:"target_path"`
	SourceSHA      string          `json:"source_sha"`
	TargetSHA      string          `json:"target_sha"`
	ManifestDigest string          `json:"manifest_digest"`
}

// P05 implements publication, verification and cold-start resolution. Resolve
// follows the component's effective selector, never the newest pathname alone.
type PublishFunc = func(ctx context.Context, grant upgradetx.Grant, source io.Reader, target io.Reader) (Receipt, error)
type VerifyFunc = func(receipt Receipt, grant upgradetx.Grant) error
type ResolveFunc = func(key upgradetx.Key, view upgradetx.View) (string, error)
