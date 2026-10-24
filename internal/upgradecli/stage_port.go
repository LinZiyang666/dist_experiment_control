package upgradecli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// StagePort is the root stage's production PublicationPort. It supplies the
// authority facts the frozen Grant does not carry: the component's registered
// installation, the live issuer and its running source bytes, the domain
// reservation, and the decoder check of each published image. It writes no
// journal, starts nothing, and treats every missing fact as a refusal.
// origin: hot-update S8 plan §5.2 / §11.2 P19 (P05 PublicationPort)
type StagePort struct {
	// ControlDir is the component's registered control directory, resolved by
	// the caller from the role configuration, never from the grant file.
	ControlDir string
	// Probe must support independent invocations: stage publishes its two
	// images concurrently, with each probe isolated under its own supervisor.
	Probe ProbeImage
	Now   func() time.Time

	mu    sync.Mutex
	grant upgradetx.Grant
}

var _ upgradeartifact.PublicationPort = (*StagePort)(nil)

// ParallelImages opts the product stage into overlapping its independent
// source and target publications. Grant access is serialized; each image has
// its own reader, SHA slot and bounded probe. Publication joins both before
// the final issuer/expiry check and durable receipt.
// origin: hot-update S8 plan §5.2 / S8 internal review PREPARE-LATENCY R-10
func (p *StagePort) ParallelImages() bool { return true }

// Authorize is evaluated twice by Publish (before and after the slow copy and
// probes). Every evaluation re-reads the registration and the issuer, so an
// expired grant, a restarted issuer or a replaced running image cannot obtain
// a late receipt.
func (p *StagePort) Authorize(ctx context.Context, grant upgradetx.Grant) (upgradeartifact.Location, error) {
	if err := ctx.Err(); err != nil {
		return upgradeartifact.Location{}, err
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	if grant.ExpiresAt <= now().UnixNano() {
		return upgradeartifact.Location{}, &upgradetx.Error{Code: upgradetx.CodeGrantExpired, Detail: "grant expired before publication"}
	}
	want := grant.Handle.Key
	want.DomainID = ""
	key, reg, registered, err := upgradeartifact.RegisteredKey(p.ControlDir, want)
	if err != nil {
		return upgradeartifact.Location{}, err
	}
	if !registered || key != grant.Handle.Key {
		return upgradeartifact.Location{}, rejected("grant names a component that is not registered here")
	}
	// The issuer is the daemon that signed the grant. Its kernel identity and
	// running bytes must still be the grant's source: a restarted issuer has
	// lost every unstarted grant (§3.1), and a different running image would
	// pin the wrong rollback target.
	owner, err := ProcessOwner(grant.Issuer.PID)
	if err != nil || owner.BootID != grant.Issuer.BootID || owner.StartTicks != grant.Issuer.StartTicks {
		return upgradeartifact.Location{}, &upgradetx.Error{Code: upgradetx.CodeGrantUnknown, Detail: "grant issuer is no longer running"}
	}
	running, err := upgradeartifact.FileSHA(fmt.Sprintf("/proc/%d/exe", grant.Issuer.PID))
	if err != nil || running != grant.SourceSHA {
		return upgradeartifact.Location{}, rejected("grant source is not the issuer's running image")
	}
	p.mu.Lock()
	if p.grant != (upgradetx.Grant{}) && p.grant != grant {
		p.mu.Unlock()
		return upgradeartifact.Location{}, rejected("one stage publishes exactly one grant")
	}
	p.grant = grant
	p.mu.Unlock()
	return upgradeartifact.Location{CanonicalPath: reg.CanonicalPath, Directory: filepath.Join(reg.ControlDir, "artifacts")}, nil
}

// Reserve takes the domain reservation. The registered parent's execution token lets
// the stage borrow the parent's live OFD instead of flocking it a second time.
func (p *StagePort) Reserve(ctx context.Context, grant upgradetx.Grant) (upgradelock.Lease, error) {
	return upgradelock.Acquire(ctx, grant.Handle.Key, grant.ParentTxID)
}

// CheckImage probes the published bytes. Both images must report the release
// the grant names; the target must also carry the exact reader declarations
// the grant's CapabilityDigest was computed from. The returned manifest digest
// binds the probed declarations to the published SHA.
func (p *StagePort) CheckImage(ctx context.Context, path, sha, release, capabilities string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	grant := p.grant
	p.mu.Unlock()
	if p.Probe == nil || grant == (upgradetx.Grant{}) || capabilities != grant.CapabilityDigest {
		return "", rejected("image check has no authorized grant or probe")
	}
	gotRelease, caps, err := p.Probe(path)
	if err != nil || caps.SHA256 != sha || gotRelease != release {
		return "", rejected("published image does not report the authorized release")
	}
	if sha == grant.TargetSHA && CapabilityDigest(grant.Handle.Key.Component, caps) != grant.CapabilityDigest {
		return "", rejected("candidate reader declarations differ from the prepared grant")
	}
	data, _ := json.Marshal(struct {
		SHA, Release, Capabilities string
	}{sha, gotRelease, CapabilityDigest(grant.Handle.Key.Component, caps)})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
