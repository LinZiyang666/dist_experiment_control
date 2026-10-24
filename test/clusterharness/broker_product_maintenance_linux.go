// Store fixtures use real registered agents and observe their installed
// maintenance authority before starting the measured cut.
// origin: hot-update S8 plan §4.2 / §6.3; S8 internal review upgrade-maketest F7
package clusterharness

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/storage"
)

// Run and rollback fixtures retain their original three allocations, while
// replacing the bare tunnel peer with agents that install real signed holds.
// The full flow cohort keeps its additional declared streams and witnesses.
// origin: S6 lead decision D-C / plan §7.1
func newMaintainedProductCohort(f *brokerProductFixture, full bool) *productCohort {
	c := newProductCohort(f)
	c.observerOnly = !full
	return c
}

// Single registration intentionally carries no cluster roster. Supply the
// issuer pin through the agent's normal configuration so its maintenance
// handler can authenticate the broker rather than trusting the NATS payload.
// origin: S6 lead decision D-C / plan §7.1 / §7.3
func setupMaintainedProductCohort(c *productCohort, db *sql.DB, accountPub string) error {
	if err := c.setup(db); err != nil {
		return err
	}
	homes := []string{c.agentHome}
	if c.f.storeOnly || c.observerOnly {
		homes = append(homes, filepath.Join(c.f.root, "short-agent-home"))
	}
	for _, home := range homes {
		path := filepath.Join(home, "agent", "product", "agent.yaml")
		config, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, append(config, []byte("account_pub: "+accountPub+"\n")...), 0600); err != nil {
			return err
		}
	}
	return nil
}

// The raw-flow cohort owns explicit TCP/SS allocations, not a subscription
// egress proxy. Declare that role before the first registration: advertising an
// unconfigured proxy leaves capable=true/ready=false, which must block a planned
// hot cut. The real agent still installs and signs the maintenance route hold.
// origin: hot-update S6 plan lead L2 / §7; hot-update S8 plan §7.2;
// S8 internal review P13-COHORT F1
func setupCohortMaintenanceAgent(c *productCohort, state []byte) error {
	dir := filepath.Join(c.agentHome, "agent", "product")
	if err := os.WriteFile(filepath.Join(dir, "agent.yaml"), []byte("proxy:\n  participate: false\n"), 0600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "state.json"), state, 0600)
}

// Registration and public LISTEN sockets precede installation of the observer
// metadata. Admission's own census must see the original agent before the
// caller starts its measured cohort; no fixture writes readiness or fake ACKs.
// origin: hot-update S6 plan lead L2 / §7; S8 internal review P13-COHORT F1
func awaitCohortMaintenanceAgent(c *productCohort) error {
	return awaitProductMaintenanceAgents(c, 1)
}

func awaitProductMaintenanceAgents(c *productCohort, count int) error {
	db, err := storage.OpenReadOnly(filepath.Join(c.f.root, "tether.db"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(c.f.test.Context(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		peers, e := brokercontrol.MaintenanceAgents(db, "", true)
		if e == nil && len(peers) == count && peers[0].SID == "product" && peers[0].NID == "agent" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cohort maintenance observer unavailable: %w; census: %v; agent: %s", ctx.Err(), e, c.agentLogs.String())
		case <-tick.C:
		}
	}
}

func startMaintainedBrokerStoreFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	t.Helper()
	f, err := startBrokerProductFixture(t, cfg, true, false, true)
	if err != nil {
		return f, err
	}
	// origin: hot-update S10 plan §12.2
	product, ok := f.api.(*brokerProductFixture)
	if !ok || product == nil {
		return f, fmt.Errorf("store fixture requires a broker product implementation, got %T", f.api)
	}
	db, err := storage.OpenReadOnly(filepath.Join(product.root, "tether.db"))
	if err != nil {
		return f, err
	}
	defer func() { _ = db.Close() }()
	// Registration logs precede observer metadata and proxy readiness. Read the
	// same authenticated observer census admission will use; a missing agent or
	// unready proxy stays a failure, with no forged database state or ACK.
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		peers, e := brokercontrol.MaintenanceAgents(db, "", true)
		if e == nil && len(peers) == 2 {
			return f, nil
		}
		select {
		case <-ctx.Done():
			return f, fmt.Errorf("store fixture maintenance observers unavailable: %w; census: %v; agent: %s; control agent: %s", ctx.Err(), e, product.cohort.agentLogs.String(), product.cohort.controlLogs.String())
		case <-tick.C:
		}
	}
}

// The original store fixture seeds non-proxy agent rows and owns only its
// explicit raw/SS allocations. Its real replacements retain that cold scope;
// opting into an unconfigured egress role would add unrelated readiness work.
// No running agent, allocation, directory mode or business stream is changed.
// origin: hot-update S8 plan §4.2 / §6.3; S8 internal review upgrade-maketest F7
func setupStoreControlAgents(c *productCohort, db *sql.DB) error {
	if err := c.setupControlAgent(db); err != nil {
		return err
	}
	for _, home := range []string{c.agentHome, filepath.Join(c.f.root, "short-agent-home")} {
		path := filepath.Join(home, "agent", "product", "agent.yaml")
		if err := os.WriteFile(path, []byte("proxy:\n  participate: false\n"), 0600); err != nil {
			return err
		}
	}
	return nil
}
