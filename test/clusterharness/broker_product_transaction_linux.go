//go:build linux

package clusterharness

// broker_product_transaction_linux.go cuts a product fixture's broker through
// the authorized S8 route instead of the legacy OpBrokerUpgradeReload, which
// the shared legacy policy refuses for a broker with business (legacy_busy):
// the broker is registered and configured as an installer retrofit leaves it
// before its first start; each cut is describe → Prepare → root stage of the
// candidate at the registered canonical path → Start over the same admin
// socket. Start returns once the S6 store cut is armed (its final image
// exists over the closed store), so the fixture's closed-store, prepared and
// exec checkpoints observe exactly the product cut they observed before; the
// imported target completes the original transaction, and the next cut first
// requires that completion. The quorum roll keeps its own route.
// origin: hot-update S8 plan §5.2 / §7.1 / §7.2 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11 (lead acceptance: S10 MAKETEST-BROKER-IMAGE request 2)

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// productTransactionBudget bounds one admin exchange of the fixture's ctl. A
// Start answers when the S6 cut is armed, which a lifecycle fixture may hold
// at its own checkpoints; the product ctl's request budget is not the
// fixture's to shorten.
const productTransactionBudget = time.Minute

// productTransaction is the registered S8 identity of one product broker.
type productTransaction struct {
	root, binary, config string
	// last is the most recent accepted transaction; the next cut waits for
	// its imported target to complete it.
	last struct {
		key  upgradetx.Key
		txID string
	}
}

// productPINHash is a real PHC creation salt for fixture sessions: an S8 cut
// carries every session's creation incarnation, and a placeholder hash has
// none (every cut would defer as authority_incarnation_unknown).
var productPINHash = sync.OnceValues(func() (string, error) { return auth.HashPIN("s8-product-fixture") })

// newProductTransaction writes the broker configuration its serve command
// names (--config): the config path and data directory are the broker's S8
// identity.
func newProductTransaction(root, binary, dbPath string) (*productTransaction, error) {
	config := filepath.Join(root, "broker.yaml")
	if err := os.WriteFile(config, []byte("broker:\n  storage:\n    db: "+dbPath+"\n"), 0o600); err != nil {
		return nil, err
	}
	return &productTransaction{root: root, binary: binary, config: config}, nil
}

// register records the broker's S8 domain once its canonical executable
// exists, before the first start: canonical entry = the product binary path,
// owner-only control directory with its artifacts slot, one participant.
func (p *productTransaction) register(dbPath string) error {
	// A registered canonical boot accepts only installation-controlled bytes
	// (no group/other write); the build and copies inherit the test umask.
	for _, path := range []string{p.binary, filepath.Join(p.root, "candidate")} {
		if err := os.Chmod(path, 0o755); err != nil {
			return err
		}
	}
	role := upgradecli.BrokerBootRole(upgradecli.BrokerDataDir("", dbPath), p.config, "", uint32(os.Geteuid()))
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0o700); err != nil {
		return err
	}
	if err := os.Chmod(role.ControlDir, 0o700); err != nil {
		return err
	}
	_, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: p.binary, ControlDir: role.ControlDir,
		DeploymentID: "s8-product-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-broker"}}})
	return err
}

func (p *productTransaction) endpoint() upgradecli.Remote {
	ports := upgradecli.BrokerPorts{Call: func(_ context.Context, socket string, request adminsock.Request) (*adminsock.Response, error) {
		return (&adminsock.Client{Path: socket, Timeout: productTransactionBudget}).Call(request)
	}}
	return upgradecli.AdminEndpoint(ports, filepath.Join(p.root, "admin.sock"))
}

// prepare describes the broker, prepares a grant for exactly the staged
// candidate (sha) and root-stages both images. It has no effect on the
// running broker; Start is the only step that cuts.
func (p *productTransaction) prepare(ctx context.Context, pid int, sha string) (upgradetx.Grant, string, error) {
	if err := p.settled(ctx); err != nil {
		return upgradetx.Grant{}, "", err
	}
	endpoint := p.endpoint()
	target, staged, err := endpoint.DescribeTarget(ctx, upgradetx.Key{Component: upgradetx.ComponentBroker})
	if err != nil {
		return upgradetx.Grant{}, "", fmt.Errorf("describe: %w", err)
	}
	if staged == nil || staged.SHA != sha {
		return upgradetx.Grant{}, "", fmt.Errorf("describe: staged candidate %+v is not %s", staged, sha)
	}
	sum := sha256.Sum256([]byte("s8 product fixture operator"))
	actor := "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:])
	request, err := upgradecli.BuildPrepare(target, upgradecli.Candidate{SHA: staged.SHA, Release: staged.Release, CapabilityDigest: staged.CapabilityDigest}, actor, time.Now())
	if err != nil {
		return upgradetx.Grant{}, "", err
	}
	grant, err := endpoint.Prepare(ctx, target.Key, request)
	if err != nil {
		return upgradetx.Grant{}, "", fmt.Errorf("prepare: %w", err)
	}
	receipt, err := p.stage(ctx, pid, grant)
	return grant, receipt, err
}

// A quorum child publishes exactly its parent-issued grant through the same
// product root-stage command as the standalone fixture, before the timed cut.
// origin: hot-update S8 plan §3.1 / §5.2; S8 internal review P13-COHORT F2
func (p *productTransaction) stage(ctx context.Context, pid int, grant upgradetx.Grant) (string, error) {
	dir, err := os.MkdirTemp(p.root, "transaction-")
	if err != nil {
		return "", err
	}
	grantPath, receiptPath := filepath.Join(dir, "grant.json"), filepath.Join(dir, "receipt.json")
	if err := upgradecli.WriteGrant(grantPath, grant); err != nil {
		return "", err
	}
	// The product's own root stage publishes the running source bytes and the
	// candidate staged at the registered canonical path.
	stage := exec.CommandContext(ctx, p.binary, "upgrade", "stage", "--grant", grantPath, "--source", fmt.Sprintf("/proc/%d/exe", pid),
		"--candidate", p.binary, "--receipt-out", receiptPath)
	stage.Dir = p.root
	stage.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+p.root, "TETHER_HOME="+p.root)
	if out, err := testharness.CombinedOutput(stage); err != nil {
		return "", fmt.Errorf("root stage: %v\n%s", err, out)
	}
	return receiptPath, nil
}

func prepareProductStart(f *brokerProductFixture, sha string) (func() error, error) {
	if f.quorum != nil {
		return f.quorum.prepare(f, sha)
	}
	if f.transaction == nil {
		return nil, nil
	}
	grant, receipt, err := f.transaction.prepare(f.test.Context(), f.cmd.Process.Pid, sha)
	if err != nil {
		return nil, err
	}
	return func() error { return f.transaction.start(f.test.Context(), grant, receipt) }, nil
}

// start sends the original grant's Start. The armed broker answers
// broker_activation_dispatched before its exec (nil here); a lost reply is
// the product fixture's unknown arm result, never a reason to cut again.
func (p *productTransaction) start(ctx context.Context, grant upgradetx.Grant, receiptPath string) error {
	receipt, err := upgradecli.ReadArtifactReceipt(receiptPath)
	if err != nil {
		return err
	}
	p.last.key, p.last.txID = grant.Handle.Key, grant.Handle.TxID
	_, err = p.endpoint().Start(ctx, grant, receipt)
	var typed *upgradetx.Error
	switch {
	case err == nil:
		return nil
	case errors.As(err, &typed) && typed.Code == upgradetx.CodeUnsettled && typed.Detail == "broker_activation_dispatched":
		// The wire answer of an accepted, armed cut (74): its outcome belongs
		// to the imported target, which the next cut waits for.
		return nil
	case errors.As(err, &typed) && typed.Accepted && strings.Contains(typed.Detail, "reply was lost"):
		return errProductReloadUnknown
	}
	return fmt.Errorf("authorized broker Start: %w", err)
}

// settled requires the previous accepted transaction to be completed by its
// imported target before another cut is described.
func (p *productTransaction) settled(ctx context.Context) error {
	if p.last.txID == "" {
		return nil
	}
	deadline := time.Now().Add(2 * productTransactionBudget)
	var last error
	for time.Now().Before(deadline) {
		nonce := fmt.Sprintf("%032x", time.Now().UnixNano())
		view, err := p.endpoint().Query(ctx, p.last.key, p.last.txID, nonce)
		phase := view.Historical.Forward.Phase
		if view.Historical.Reverse.Handle.TxID != "" {
			phase = view.Historical.Reverse.Phase
		}
		switch {
		case err == nil && phase == upgradetx.PhaseCompleted:
			return nil
		case err == nil && (phase == upgradetx.PhaseRecoveryRequired || phase == upgradetx.PhaseRollbackBlocked || phase == upgradetx.PhaseAbandoned):
			return fmt.Errorf("previous broker transaction %s requires recovery (%s)", p.last.txID, view.Historical.Reason)
		}
		last = fmt.Errorf("phase=%s err=%v", phase, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("previous broker transaction %s did not complete: %v", p.last.txID, last)
}
