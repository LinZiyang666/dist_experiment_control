package cluster

import (
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// inmemFactory returns a transportFactory producing inmem transports bound to addr (== the node id), so
// the post-recover NewRaft recognizes itself as the {self} voter and becomes leader.
func inmemFactory(addr raft.ServerID) func() (raft.Transport, error) {
	return func() (raft.Transport, error) {
		_, tr := raft.NewInmemTransport(raft.ServerAddress(addr))
		return tr, nil
	}
}

// #5 RecoversWritable_InProcess: the online force-single swaps the raft instance in-process to a writable
// N=1 cluster WITHOUT a restart — data survives the swap and a fresh Propose commits.
func TestRecoverToSelfOnlineRecoversWritable(t *testing.T) {
	dir := t.TempDir()
	id := raft.ServerID("brk-a")
	n := mustNode(t, dir, id)
	n.transportFactory = inmemFactory(id)

	if err := n.ApplyMetaSet("t:online", "before"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := n.RecoverToSelfOnline(string(id)); err != nil {
		t.Fatalf("RecoverToSelfOnline: %v", err)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("not leader after in-process recover: %v", err)
	}
	// data preserved across the swap (RODB read of a pre-recover row)
	var v string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:online'`).Scan(&v); err != nil || v != "before" {
		t.Fatalf("data lost across recover: got %q err=%v", v, err)
	}
	// writable WITHOUT a restart: a new commit lands
	if err := n.ApplyMetaSet("t:after", "yes"); err != nil {
		t.Fatalf("not writable after recover: %v", err)
	}
}

// #8 NewRaftFailureLeavesReadOnlySurvivor: if the transport rebuild fails AFTER RecoverCluster wrote
// {self}, the node is NOT bricked (no panic, not leader, RODB still serves) and a plain restart on the
// {self} stores comes up N=1 leader (the floor).
func TestRecoverToSelfOnlineFailureLeavesReadOnlyThenRestartLeader(t *testing.T) {
	dir := t.TempDir()
	id := raft.ServerID("brk-b")
	n, err := openNodeRaw(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	if err := n.ApplyMetaSet("t:floor", "kept"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n.transportFactory = func() (raft.Transport, error) { return nil, errors.New("inject transport rebuild failure") }

	err = n.RecoverToSelfOnline(string(id))
	if err == nil {
		t.Fatal("expected RecoverToSelfOnline to fail (injected transport failure)")
	}
	// NOT bricked: no panic, not leader (old instance is Shutdown), RODB still serves.
	if n.IsLeader() {
		t.Fatal("failed recover must leave the node NOT leader (old raft is Shutdown)")
	}
	var v string
	if e := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:floor'`).Scan(&v); e != nil || v != "kept" {
		t.Fatalf("RODB must still serve after a failed recover: got %q err=%v", v, e)
	}
	// Floor: a plain restart on the {self} stores (which RecoverCluster already wrote) comes up leader.
	if e := n.Shutdown(); e != nil {
		t.Logf("shutdown after failed recover (non-fatal): %v", e)
	}
	n2, err := openNodeRaw(dir, id)
	if err != nil {
		t.Fatalf("restart open: %v", err)
	}
	t.Cleanup(func() { _ = n2.Shutdown() })
	if err := n2.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("restart on {self} stores must come up leader: %v", err)
	}
	if e := n2.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:floor'`).Scan(&v); e != nil || v != "kept" {
		t.Fatalf("data lost across the restart floor: got %q err=%v", v, e)
	}
}

// #7 ConcurrentProposeRace: hammering Propose/IsLeader across the in-process swap must be race-clean and
// never panic (the atomic.Pointer guarantee). Run under -race.
func TestRecoverToSelfOnlineConcurrentPropose(t *testing.T) {
	dir := t.TempDir()
	id := raft.ServerID("brk-c")
	n := mustNode(t, dir, id)
	n.transportFactory = inmemFactory(id)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = n.IsLeader()
				_ = n.ApplyMetaSet("t:hammer", "x") // may return ErrNotLeader during the swap; never panics
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = n.LeaderContactStale(time.Now())
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	if err := n.RecoverToSelfOnline(string(id)); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("RecoverToSelfOnline under load: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("not leader after concurrent recover: %v", err)
	}
}

// #10 MarkerViaRaft: PlanSetForceSingle / PlanForceSingleEpoch render commands that, applied through the
// FSM (Propose), set the replicated force_single_active marker + recovery epoch in cluster_meta.
func TestPlanSetForceSingleMarkerViaRaft(t *testing.T) {
	dir := t.TempDir()
	n := mustNode(t, dir, "brk-e")
	now := time.Unix(1700000000, 0).UTC()
	if err := n.Propose(func(*sql.DB) (*Command, error) { return PlanSetForceSingle(now) }); err != nil {
		t.Fatalf("propose set marker: %v", err)
	}
	var v string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, MetaKeyForceSingle).Scan(&v); err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if v != now.Format(time.RFC3339Nano) {
		t.Fatalf("force_single_active=%q want %q", v, now.Format(time.RFC3339Nano))
	}
	if err := n.Propose(func(*sql.DB) (*Command, error) { return PlanForceSingleEpoch("ep-123") }); err != nil {
		t.Fatalf("propose epoch: %v", err)
	}
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, MetaKeyForceSingleEpoch).Scan(&v); err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	if v != "ep-123" {
		t.Fatalf("force_single_epoch=%q want ep-123", v)
	}
	// PlanClearForceSingle removes it (HA restored).
	if err := n.Propose(func(*sql.DB) (*Command, error) { return PlanClearForceSingle() }); err != nil {
		t.Fatalf("propose clear: %v", err)
	}
	var cnt int
	if err := n.RODB().QueryRow(`SELECT COUNT(*) FROM cluster_meta WHERE key=?`, MetaKeyForceSingle).Scan(&cnt); err != nil {
		t.Fatalf("read after clear: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("force_single_active not cleared: count=%d", cnt)
	}
}

// A nil transportFactory => online recover is unavailable (the offline floor still works); the node is
// left untouched (still leader).
func TestRecoverToSelfOnlineNilFactoryRefuses(t *testing.T) {
	dir := t.TempDir()
	id := raft.ServerID("brk-d")
	n := mustNode(t, dir, id)
	// transportFactory deliberately nil
	if err := n.RecoverToSelfOnline(string(id)); err == nil {
		t.Fatal("nil transportFactory must refuse online recover")
	}
	if !n.IsLeader() {
		t.Fatal("a refused (nil-factory) recover must leave the node untouched (still leader)")
	}
}
