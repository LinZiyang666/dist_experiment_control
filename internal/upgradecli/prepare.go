package upgradecli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Target is what the ctl must know about the running component before it can
// ask for a grant: its exact journal Key, the running source image and the
// original scope (every domain, with explicit not_applicable rows, and the
// original resource identities). The frozen PrepareRequest carries all of
// them, so none may be guessed: a wrong Key is denied by the target, a wrong
// source SHA is deferred at Start, and only the target can enumerate its own
// retained work — a ctl-invented scope could hide it (M06).
// origin: hot-update S8 plan §2.1 / §3.1 / §4.2 / §11.2 P19
type Target struct {
	Key           upgradetx.Key
	SourceSHA     string
	SourceRelease string
	Scope         upgradetx.Scope
}

// Describer supplies the Target from the component itself. A production
// route must answer from the live target, never from a cached roster.
type Describer func(context.Context) (Target, error)

// UndescribedTarget is the Describer of a port that has no description route.
// It refuses before any download or side effect, so the operator sees a
// precise pre-acceptance deferral instead of a guessed Key. The production
// node and broker ports describe through upgrade.describe (DescribeNode,
// DescribeBroker).
// origin: hot-update S8 plan §7.1 / §11.2 P19 (request R1 to P13/P14/P15)
func UndescribedTarget(context.Context) (Target, error) {
	return Target{}, &upgradetx.Error{Code: upgradetx.CodeDeferred,
		Detail: "target_description_unavailable: the control plane cannot yet report this component's upgrade identity"}
}

// Candidate is one verified executable: the bytes the grant will name.
type Candidate struct {
	Path, SHA, Release, CapabilityDigest string
	cleanup                              func() error
}

// Close removes the candidate's private copy.
func (c Candidate) Close() error {
	if c.cleanup == nil {
		return nil
	}
	return c.cleanup()
}

// CapabilityDigest retains the CLI contract of the stage's canonical reader digest.
func CapabilityDigest(component upgradetx.Component, caps agentimage.Capabilities) string {
	return upgradestage.CapabilityDigest(component, caps)
}

// ProbeImage runs the bounded, isolated `version` probe on bytes the caller
// already holds and binds the declarations to their SHA.
type ProbeImage func(path string) (string, agentimage.Capabilities, error)

// FetchCandidate downloads the release archive, checks the archive digest the
// operator supplied, extracts the single `tether` entry and probes it. The
// probe runs on the ctl host, exactly like the old agent smoke gate did; the
// target re-probes the published bytes before any activation.
// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P19
func FetchCandidate(component upgradetx.Component, url, archiveSHA string, fetch func(string, time.Duration) ([]byte, error), probe ProbeImage) (Candidate, error) {
	if !hexValue(archiveSHA, 64) || url == "" || fetch == nil || probe == nil {
		return Candidate{}, rejected("candidate requires url, lowercase sha256 and a probe")
	}
	archive, err := fetch(url, upgradebudget.ComponentHandover)
	if err != nil {
		return Candidate{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "candidate archive is unavailable"}
	}
	sum := sha256.Sum256(archive)
	if hex.EncodeToString(sum[:]) != archiveSHA {
		return Candidate{}, rejected("candidate archive does not match --sha256")
	}
	dir, err := os.MkdirTemp("", "tether-candidate-*")
	if err != nil {
		return Candidate{}, unsettled("candidate workspace is unavailable")
	}
	cleanup := func() error { return os.RemoveAll(dir) }
	path := filepath.Join(dir, "tether")
	if err := agentimage.ExtractBinary(archive, path, binaryupgrade.MaxTarballBytes, (*os.File).Sync); err != nil {
		_ = cleanup()
		return Candidate{}, rejected("candidate archive has no usable tether executable")
	}
	release, caps, err := probe(path)
	if err != nil || caps.SHA256 == "" || release == "" {
		_ = cleanup()
		return Candidate{}, rejected("candidate executable failed its version probe")
	}
	return Candidate{Path: path, SHA: caps.SHA256, Release: release, CapabilityDigest: CapabilityDigest(component, caps), cleanup: cleanup}, nil
}

// BuildPrepare assembles the inert standalone request. The ctl chooses only a
// fresh client nonce; every identity and the original scope come from the
// target description or the verified candidate. The scope must name exactly
// this one participant. Expiry is the standalone absolute TTL (§3.1), not a
// renewable lease.
// origin: hot-update S8 plan §2.1 / §3.1 / §11.2 P19
func BuildPrepare(target Target, candidate Candidate, actorFP string, now time.Time) (upgradetx.PrepareRequest, error) {
	if upgradetx.ValidateKey(target.Key) != nil || !hexValue(target.SourceSHA, 64) || target.SourceRelease == "" ||
		len(target.Scope.Participants) != 1 || target.Scope.Participants[0] != target.Key ||
		!hexValue(candidate.SHA, 64) || candidate.Release == "" || !hexValue(candidate.CapabilityDigest, 64) || actorFP == "" {
		return upgradetx.PrepareRequest{}, rejected("prepare requires a described target, a verified candidate and an actor")
	}
	if target.SourceSHA == candidate.SHA {
		return upgradetx.PrepareRequest{}, rejected("candidate is already the running image")
	}
	nonce, err := randomHex()
	if err != nil {
		return upgradetx.PrepareRequest{}, err
	}
	scope, err := upgradetx.CanonicalScope(target.Scope)
	if err != nil {
		return upgradetx.PrepareRequest{}, err
	}
	return upgradetx.PrepareRequest{ClientNonce: nonce, ActorFP: actorFP, Mode: upgradetx.ModeHot,
		SourceRelease: target.SourceRelease, TargetRelease: candidate.Release, SourceSHA: target.SourceSHA, TargetSHA: candidate.SHA,
		Scope: scope, CapabilityDigest: candidate.CapabilityDigest, ExpiresAt: now.Add(upgradebudget.ComponentHandover).UnixNano()}, nil
}

// LegacyInterruptingDispatched is the only outcome an accepted explicit
// first-migration request can have. It is never hot: the old route returns no
// causal receipt tying the agent's later image to this dispatch, so it is 74
// (accepted, unsettled) and must never count as a hot completion. Refusals keep
// the legacy code namespace the caller already classifies.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P19
func LegacyInterruptingDispatched() error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true,
		Detail: "legacy_interrupting non_hot: dispatched without a causal completion proof; verify the node manually"}
}
