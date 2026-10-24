//go:build d7_integration

package d7_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

func (c *d7Cluster) leaderProbes() []clusterharness.LeaderProbe {
	probes := make([]clusterharness.LeaderProbe, len(c.nodes))
	for i, node := range c.nodes {
		probes[i] = node
	}
	return probes
}

func (c *d7Cluster) waitMetaApplied(replica int, key, value string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for {
		var got string
		err := c.nodes[replica].BoundedStaleRead(func(db *sql.DB) error {
			return db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&got)
		})
		if err == nil && got == value {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("observe committed meta on replica: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replica %d did not apply meta %q=%q within %s", replica, key, value, budget)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
// The seed is local SQL, so leadership movement requires reseeding the NEW
// leader's DB as well as rebinding the admin. INSERT OR IGNORE makes a retry safe
// without repairing a rehome defect. Assertions return errors inside the segment:
// a Fatal there would prevent WithLeader from rejecting a stale premise.
func (c *d7Cluster) drainRefusesRebuildOff(t *testing.T, beforeDrain func(int) error) (int, error) {
	return clusterharness.WithLeader(t, c.leaderProbes(), 15*time.Second, func(leader int) error {
		// This test exercises draining a follower. If node 2 has won an election,
		// establish that precondition by transferring away; WithLeader retries.
		if leader == 2 {
			if err := broker.NewClusterAdmin(c.nodes[leader], nil).TransferLeaderTo(c.ids[0]); err != nil {
				return err
			}
			return fmt.Errorf("drain-follower premise moved leadership off node 2; retry on the new leader")
		}
		if err := c.nodes[leader].Barrier(5 * time.Second); err != nil {
			return err
		}
		if err := seedRebuildOffExpose(c.dbPath[leader], c.ids[2]); err != nil {
			return err
		}
		if beforeDrain != nil {
			if err := beforeDrain(leader); err != nil {
				return err
			}
		}
		err := broker.NewClusterAdmin(c.nodes[leader], nil).DrainNode(c.ids[2], false, true, time.Now(), nil)
		var ro *broker.ErrRebuildOffExposes
		if !errors.As(err, &ro) {
			return fmt.Errorf("drain with a rebuild-OFF expose must refuse (ErrRebuildOffExposes), got %v", err)
		}
		if len(ro.Ports) != 1 || ro.Ports[0] != 9090 {
			return fmt.Errorf("refusal must enumerate the rebuild-OFF port 9090, got %v", ro.Ports)
		}
		// Query the same DB the acting admin read; a different replica might
		// still hold the old value and conceal a silent rehome.
		var home string
		if err := c.nodes[leader].BoundedStaleRead(func(db *sql.DB) error {
			return db.QueryRow(`SELECT home_broker FROM port_allocations WHERE port=9090`).Scan(&home)
		}); err != nil {
			return fmt.Errorf("read expose home: %w", err)
		}
		if home != c.ids[2] {
			return fmt.Errorf("rebuild-OFF expose was silently rehomed: home_broker=%q, want %q", home, c.ids[2])
		}
		return nil
	})
}

func seedRebuildOffExpose(path, home string) error {
	db, err := storage.OpenWAL("file:" + path)
	if err != nil {
		return fmt.Errorf("open leader db: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('s1','s1','SHA256:o','h')`); err != nil {
		return fmt.Errorf("seed session: %w", err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('s1','n1',?, 'ONLINE')`, time.Now().UTC()); err != nil {
		return fmt.Errorf("seed node: %w", err)
	}
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)
		 VALUES (9090,'s1','n1','off-expose',8080,'th','ALLOCATED','SHA256:o',?,?,0,0)`,
		time.Now().UTC(), home,
	); err != nil {
		return fmt.Errorf("seed rebuild-OFF port: %w", err)
	}
	return nil
}

// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
// Force a REAL raft transfer after the local seed and before DrainNode. The old
// observe-once/admin-bound-to-that-node shape must produce the reported failure;
// the new segment must retry, reseed, and retain every product assertion.
func testD7DrainRefusalRetriesAfterLeadershipMoves(t *testing.T) {
	c := startD7Cluster(t, 3)
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		caughtUp := func(barrier uint64) (bool, error) {
			cur, err := c.nodes[i].AppliedIndex()
			return cur >= barrier, err
		}
		if err := c.addNodeRetry(t, in, c.ids[i], caughtUp, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	moved := false
	attempts := 0
	var firstLeader, retryLeader int
	retries, err := c.drainRefusesRebuildOff(t, func(leader int) error {
		attempts++
		if moved {
			retryLeader = leader
			return nil
		}
		firstLeader = leader
		next := 1 - leader // node 2 must remain the drained follower
		admin := broker.NewClusterAdmin(c.nodes[leader], nil)
		if err := admin.TransferLeaderTo(c.ids[next]); err != nil {
			return fmt.Errorf("force leadership move: %w", err)
		}
		if !clusterharness.WaitForCond(5*time.Second, func() bool {
			return !c.nodes[leader].IsLeader() && c.nodes[next].IsLeader()
		}) {
			return fmt.Errorf("forced transfer did not establish new leader %s", c.ids[next])
		}
		oldErr := admin.DrainNode(c.ids[2], false, true, time.Now(), nil)
		if !d7TransientAddErr(oldErr) {
			t.Fatalf("old leader-bound drain must reproduce not-leader failure, got %v", oldErr)
		}
		t.Logf("old leader-bound control: %v", oldErr)
		moved = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !moved || retries < 1 || attempts < 2 || firstLeader == retryLeader {
		t.Fatalf("forced move did not exercise retry: moved=%v retries=%d attempts=%d leaders=%d->%d", moved, retries, attempts, firstLeader, retryLeader)
	}
	t.Logf("new segment passed: retries=%d attempts=%d leaders=%d->%d", retries, attempts, firstLeader, retryLeader)
}

// Each child subtest completes its raft cleanup before the next round begins,
// so the enclosing TestD7Matrix assertion sees resources after all five teardowns.
func exerciseD7LeadershipMove(t *testing.T, round int) {
	t.Helper()
	t.Run(fmt.Sprintf("round-%d", round), testD7DrainRefusalRetriesAfterLeadershipMoves)
}
