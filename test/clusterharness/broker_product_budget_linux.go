//go:build linux

package clusterharness

import (
	"context"
	"os"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Keep the observer overlay in both builds. Selecting a release budget must
// change instrumentation only, never bypass the real store/custody checkpoints.
// origin: hot-update S10 lead decision D-A / sole-voter reload SLO
func productCandidateBuild(t *testing.T, ctx context.Context, repo, binary, sourceDigest, overlay string) error {
	if !testharness.ReleaseBudgetRun(t) {
		return productObservationBuild(ctx, repo, binary, sourceDigest, overlay)
	}
	testharness.BuildReleaseCandidate(t, repo, binary, proto.ReleaseVersion, os.Getenv("GOFLAGS")+" -overlay="+overlay)
	return nil
}
