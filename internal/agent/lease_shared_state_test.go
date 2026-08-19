package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Shared-home reality for cloned-credential instances.
//
// origin: docs/reviews/cloned-credential-instances-plan.md §0.6
//
// The plan's spike found that on the reference deployment ~/.tether is a SHARED
// NFS mount and both instances see ONE state.json at ONE inode. The replay gate
// in session() is memory-only for exactly that reason. These tests ask the
// question the gate does not answer: with the two processes now carrying
// DIFFERENT nids, is the shared file still safe from the leased instance?

// leaseStateAgent builds an agent whose Home is `home` and whose configured
// basename is "lab-1" — the shape both instances of a cloned image have.
func leaseStateAgent(t *testing.T, home string) *Agent {
	t.Helper()
	a := &Agent{
		cfg:            Config{Logger: d6Logger(), NID: "lab-1", SID: "lab", Home: home},
		stateStore:     newStateStore(home, "lab"),
		rehomeWant:     map[int]proto.HomeDirective{},
		rehomeRunning:  map[int]bool{},
		rehomeSeq:      map[int]uint64{},
		deferredReplay: map[int]bool{},
		procs:          map[string]*procRec{},
	}
	a.courier = newProcCourier(a)
	return a
}

// TestStateFilePathIsNotKeyedByNodeName is the structural fact every other
// finding in this area rests on: the per-session state file is addressed by
// (Home, SID) alone. Before this increment two clones shared a file that
// described ONE node, so the rows in it were at least interchangeable. The
// lease deliberately manufactures a SECOND node identity that keeps sharing
// that file — and the file has no room to say which node a row belongs to.
//
// MUTATION: key newStateStore on the routing nid (or add a nid field to
// PortToken and filter on it) and this test turns red.
func TestStateFilePathIsNotKeyedByNodeName(t *testing.T) {
	home := t.TempDir()
	incumbent := newStateStore(home, "lab")
	t.Logf("every instance of this session addresses %s", incumbent.path)

	if err := incumbent.AddPort(PortToken{Name: "web", Port: 14001, LocalPort: 8080, Token: "tok-basename"}); err != nil {
		t.Fatal(err)
	}

	// THE STRUCTURAL FACT REMAINS: the path is (Home, SID) and nothing else, so
	// a second instance addresses the SAME file and PortToken has no room to say
	// which node a row belongs to. The reviewer is right about that.
	//
	// What closes it is not a path change but a PRODUCT rule: an instance
	// running under a lease detaches its store and writes nothing at all. So
	// this asserts the rule where it actually lives — on an Agent that has
	// adopted a lease — rather than on a bare store, which no leased instance
	// ever gets to use.
	leasedAgent := leaseStateAgent(t, home)
	adoptRoutingNID(leasedAgent, "lab-1-02")
	if err := leasedAgent.stateStore.AddPort(PortToken{
		Name: "web", Port: 14002, LocalPort: 8080, Token: "tok-leased",
	}); err != nil {
		t.Fatal(err)
	}

	sf, err := incumbent.load()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range sf.PortTokens {
		if p.Port == 14001 && p.Token == "tok-basename" {
			return // the incumbent's row survived
		}
	}
	t.Errorf("the basename holder's row was destroyed by the leased instance's expose; state.json now holds %+v.\n"+
		"On the incumbent's next restart that port is never re-presented, the broker leaves it ALLOCATED, "+
		"and the offline reaper never fires for an online node — the exact black-hole the replay gate's "+
		"comment says must never happen.", sf.PortTokens)
}

// TestLeasedInstanceDoesNotReportTheIncumbentsPortsAtRegister pins the input
// half of the defect. The replay gate stops a leased instance from DIALING the
// inherited tokens, but buildLocalSnapshot — which feeds NodeRegisterReq.
// LocalPorts — is not lease-aware, so the leased instance claims the
// incumbent's allocations as its own on every register.
//
// The broker's reconcileOnRegister filters port_allocations by nid
// (internal/broker/reconcile.go:326-332), so every one of those ports lands in
// the "broker has no record of this" arm and comes back as RevokePorts
// (reconcile.go:336-340) plus an audit `reconciled` row attributed to the
// LEASED nid.
//
// MUTATION: report ports only when nidOf(a) == a.cfg.NID (the same predicate
// the replay gate already uses) and this test turns green.
func TestLeasedInstanceDoesNotReportTheIncumbentsPortsAtRegister(t *testing.T) {
	a := leaseStateAgent(t, t.TempDir())
	if err := a.stateStore.AddPort(PortToken{Name: "web", Port: 14001, LocalPort: 8080, Token: "tok-basename"}); err != nil {
		t.Fatal(err)
	}
	if err := a.stateStore.SetProxy(&ProxyState{PublicPort: 14000, LocalPort: 1080, Token: "tok-proxy", Epoch: 1}); err != nil {
		t.Fatal(err)
	}

	adoptRoutingNID(a, "lab-1-02") // what session() does on a contested register

	_, ports := a.buildLocalSnapshot()
	if len(ports) != 0 {
		t.Errorf("an instance leased %q reports %d port(s) it does not own: %+v\n"+
			"reconcileOnRegister keys port_allocations on nid, so each of these is revoked "+
			"against the leased nid and pruned from the SHARED state.json.", nidOf(a), len(ports), ports)
	}
}

// TestLeasedInstanceNeverPrunesTheSharedStateFile pins the output half: the
// write the register round-trip provokes.
//
// applyReconciliation (agent.go:1091) runs BEFORE the replay gate (agent.go:1115)
// and is not gated by it at all. Given the RevokePorts the previous test shows
// the broker will send, it calls stateStore.RemovePort — on a file that belongs
// to the basename holder.
//
// MUTATION: gate the RevokePorts arm of applyReconciliation on
// nidOf(a) == a.cfg.NID and this test turns green.
func TestLeasedInstanceNeverPrunesTheSharedStateFile(t *testing.T) {
	home := t.TempDir()
	a := leaseStateAgent(t, home)
	if err := a.stateStore.AddPort(PortToken{Name: "web", Port: 14001, LocalPort: 8080, Token: "tok-basename"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(home, "agent", "lab", "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	adoptRoutingNID(a, "lab-1-02")

	// Exactly what the broker replies to a leased instance that re-presented
	// the incumbent's tokens: they match no allocation under `lab-1-02`.
	a.applyReconciliation(context.Background(), proto.NodeRegisterResp{
		OK: true, RevokePorts: []int{14001},
	})

	after, err := os.ReadFile(filepath.Join(home, "agent", "lab", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("a leased instance rewrote the basename holder's state.json.\nbefore:\n%s\nafter:\n%s\n"+
			"The replay gate is memory-only, but this write path is not gated at all — "+
			"the incumbent's only copy of its raw tunnel token is now gone.", before, after)
	}
}

// TestLeasedInstanceFailClosedDoesNotWipeTheSharedProxyFootprint pins the
// third write path into the shared file, and the one that needs no broker reply
// at all.
//
// armFailClosed is installed unconditionally on every NATS disconnect
// (agent.go:1948) and failClosedFire calls proxyTeardownLocked(..., clearPersist=true)
// (proxy.go:748), whose clearPersist arm calls stateStore.SetProxy(nil)
// (proxy.go:571) WITHOUT first checking that this agent ever served a proxy. A
// leased instance is proxy-ineligible by design — it never persists a
// footprint — so on a shared Home the only footprint it can erase is the
// basename holder's.
//
// MUTATION: guard the clearPersist write on "this agent owns the footprint"
// (p.srv != nil, or nidOf(a) == a.cfg.NID) and this test turns green.
func TestLeasedInstanceFailClosedDoesNotWipeTheSharedProxyFootprint(t *testing.T) {
	home := t.TempDir()
	a := leaseStateAgent(t, home)
	a.proxy = &proxyRuntime{}
	// The basename holder's live footprint, written by the OTHER process.
	if err := a.stateStore.SetProxy(&ProxyState{
		PublicPort: 14000, LocalPort: 1080, Token: "tok-incumbent-proxy", Epoch: 7,
	}); err != nil {
		t.Fatal(err)
	}

	adoptRoutingNID(a, "lab-1-02")
	a.failClosedFire() // 15 minutes partitioned; this instance never served a proxy

	// Read the FILE through an independent store, not through this agent's.
	// Adoption detaches the agent's own store — a leased instance must neither
	// read nor write that file, because it belongs to the basename holder — so
	// asking the detached store would report "empty" no matter what is on disk
	// and could not distinguish "protected" from "erased".
	ps, err := newStateStore(home, "lab").GetProxy()
	if err != nil {
		t.Fatal(err)
	}
	if ps == nil || ps.Token != "tok-incumbent-proxy" {
		t.Errorf("a leased instance's fail-closed timer erased the basename holder's proxy footprint (now %+v).\n"+
			"The incumbent keeps serving from memory, so nothing repairs the file until it restarts — "+
			"and then it cannot bootstrap from the footprint at all.", ps)
	}
}

// TestBootRollbackCarriesTheInstanceLineageForward pins what execEnv promises.
//
// upgrade_state.go:367-370 says both rollback paths run through realExec so "a
// rollback that renamed the node" cannot happen. But execEnv() STRIPS the
// lineage out of os.Environ() and re-adds it only from the package-level
// execLineage, which is populated by agent.New — and the boot rollback runs
// from cmd/tether/main.go:78, pre-Cobra, before any Agent exists. The inherited
// values are therefore actively deleted at the one exec site that could only
// ever have received them through the environment.
//
// MUTATION: make execEnv fall back to the inherited values when execLineage is
// empty and this test turns green.
func TestBootRollbackCarriesTheInstanceLineageForward(t *testing.T) {
	// The boot path's view: values inherited from the pre-exec process, and a
	// package-level lineage nothing has populated yet.
	savedID, savedNID := execLineage.instanceID.Load(), execLineage.routingNID.Load()
	t.Cleanup(func() {
		execLineage.instanceID.Store(savedID)
		execLineage.routingNID.Store(savedNID)
	})
	empty := ""
	execLineage.instanceID.Store(&empty)
	execLineage.routingNID.Store(&empty)

	t.Setenv(instanceIDEnv, "abcdefghijklmnopqrstuvwxyz")
	t.Setenv(routingNIDEnv, "lab-1-02")

	env := execEnv()
	if got := envGet(env, instanceIDEnv); got != "abcdefghijklmnopqrstuvwxyz" {
		t.Errorf("boot rollback re-exec drops %s (got %q): the restored image mints a fresh id, "+
			"contests the name its own predecessor holds, and burns another suffix", instanceIDEnv, got)
	}
	if got := envGet(env, routingNIDEnv); got != "lab-1-02" {
		t.Errorf("boot rollback re-exec drops %s (got %q): a leased instance comes back under its "+
			"basename, which is exactly the rename realExec's comment says cannot happen", routingNIDEnv, got)
	}
}

// origin: docs/reviews/cloned-credential-instances-review-round2.md M1
//
// FALLING BACK TO THE BASENAME MAKES THE STATE FILE OURS AGAIN.
//
// Adoption detaches the store because the file belongs to whoever holds the
// basename, and on the reference deployment ~/.tether is a shared NFS mount —
// the two instances are looking at one inode. dropLease is the exact inverse:
// the agent has returned to its configured name and is now, by definition, an
// ordinary single-instance agent. Staying detached leaves it with a store that
// swallows every write and returns nothing on every read, so its exposes stop
// surviving restarts — permanently, and silently.
//
// MUTATION: remove the reattach() call in dropLease and this goes red.
func TestDroppingALeaseMakesTheStateStoreLiveAgain(t *testing.T) {
	home := t.TempDir()
	a := &Agent{
		cfg:        Config{SID: "lab", NID: "gpu1", Logger: d6Logger()},
		stateStore: newStateStore(home, "lab"),
	}
	a.instanceID = "aaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Adopt: the store must go quiet so we do not scribble on the incumbent's file.
	adoptRoutingNID(a, "gpu1-02")
	if err := a.stateStore.AddPort(PortToken{Name: "web", Port: 14001, LocalPort: 8080}); err != nil {
		t.Fatalf("AddPort while leased: %v", err)
	}
	if sf, _ := newStateStore(home, "lab").load(); sf != nil && len(sf.PortTokens) != 0 {
		t.Fatalf("a leased instance wrote to the shared state file: %+v", sf.PortTokens)
	}

	// Drop back to the basename: this process owns the file again.
	dropLease(a)
	if err := a.stateStore.AddPort(PortToken{Name: "web", Port: 14002, LocalPort: 8080}); err != nil {
		t.Fatalf("AddPort after dropLease: %v", err)
	}
	sf, err := newStateStore(home, "lab").load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ports := []PortToken{}
	if sf != nil {
		ports = sf.PortTokens
	}
	if len(ports) != 1 || ports[0].Port != 14002 {
		t.Fatalf("after falling back to its own name the agent still cannot persist ports (on disk: %+v). "+
			"It is an ordinary single-instance agent now, and its exposes will not survive a restart.",
			ports)
	}
}
