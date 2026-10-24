package broker

import (
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/nats-io/nats.go/jetstream"
)

// A follower's replica can remain ACTIVE throughout removal. Teardown must run
// on the leader, including history/OBJ, and the delayed original guard must be
// refused before touching a new creation's external stores.
// origin: S7 internal review BROKER-REDS F3; hot-update S7 plan §6.4 / S8 plan §6.2
func TestForwardedSessionCascadeDeletesOnlyCapturedCreation(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseForwardedSessionCascade(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "forwarded session cascade", g)
	testharness.AssertNoFDLeak(t, "forwarded session cascade", fd, 0)
}

func exerciseForwardedSessionCascade(t *testing.T) {
	t.Helper()
	leader, _ := d7SingleNode(t, "cascade-leader")
	f := newRmFixture(t, &recordingBarrier{decide: func(bool) error { return nil }})
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	// Copy the actual creation timestamp as well as the hash: these two pools
	// represent the same replicated create, rather than two independent creates.
	created, err := session.Get(f.db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	create := func(salt byte) {
		t.Helper()
		if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return session.PlanCreate(db, "lab", "lab", fp, brokerAuthorityPIN(salt), created.CreatedAt)
		}); err != nil {
			t.Fatalf("seed leader creation salt %d: %v", salt, err)
		}
	}
	create(1)
	guard, err := upgradeauthority.Capture(f.db, upgradeauthority.Guard{SID: "lab"})
	if err != nil {
		t.Fatal(err)
	}
	current := &Broker{cfg: Config{DB: leader.RODB(), Now: time.Now, Logger: silentLogger()}, clusterMode: true, cl: &clusterRuntime{node: leader}}
	current.nc.Store(f.nc)
	setBrokerJS(current, f.js)
	t.Cleanup(func() { setBrokerJS(current, nil) })
	apply, err := subscribeClusterApplyOwned(f.nc, leader, time.Now, silentLogger(), nil, nil, current)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = apply.Unsubscribe() })
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	f.b.clusterMode = true
	f.b.cl = &clusterRuntime{node: nonLeaderNode(t, "cascade-follower"), forwarder: NewForwarder(f.nc, 3*time.Second)}
	// The ingress deliberately has no JetStream owner. A local-only cascade
	// cannot delete these streams, even if forwarding its SQL happens to work.
	setBrokerJS(f.b, nil)
	if r := requestSessionRm(t, f.nc, f.actor, `{}`); !r.OK || r.Code != "" {
		t.Fatalf("forwarded removal: %+v", r)
	}
	for _, stream := range []string{"history-lab", "OBJ_xfer-lab"} {
		if f.streamExists(t, stream) {
			t.Fatalf("leader cascade retained %s", stream)
		}
	}
	if _, err := session.Get(leader.RODB(), "lab"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("leader cascade retained SQL: %v", err)
	}
	if state := f.state(t); state != "ACTIVE" {
		t.Fatalf("follower applied the tombstone, invalidating the lag premise: %q", state)
	}
	create(2)
	replacementGuard, err := upgradeauthority.Capture(leader.RODB(), upgradeauthority.Guard{SID: "lab"})
	if err != nil {
		t.Fatal(err)
	}
	// The replacement can itself be DELETING. Its state must not let the old
	// remover borrow its cleanup authority or erase its settlement evidence.
	if err := tombstoneSessionAuthority(current, replacementGuard); err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(t.Context(), f.js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	const evidence = "replacement-history"
	if _, err := f.js.Publish(t.Context(), proto.SubjAuditTransfer("lab"), []byte(evidence)); err != nil {
		t.Fatal(err)
	}
	object, err := f.js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-lab"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := object.PutBytes(t.Context(), "replacement", []byte("replacement-object")); err != nil {
		t.Fatal(err)
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := cascadeSessionAuthority(f.b, d, t.Context().Done(), guard); err == nil {
		t.Fatal("delayed follower cascade accepted a recreated session")
	}
	history, err := f.js.Stream(t.Context(), "history-lab")
	if err != nil {
		t.Fatal("late cascade deleted replacement history:", err)
	}
	message, err := history.GetMsg(t.Context(), 1)
	if err != nil || string(message.Data) != evidence {
		t.Fatal("late cascade changed replacement history:", err)
	}
	data, err := object.GetBytes(t.Context(), "replacement")
	if err != nil || string(data) != "replacement-object" {
		t.Fatal("late cascade changed replacement object:", err)
	}
	replacement, err := session.Get(leader.RODB(), "lab")
	if err != nil || replacement.State != session.StateDeleting {
		t.Fatal("late cascade changed replacement SQL:", err)
	}
}

// Behavioral forwarding tests install a current broker explicitly. Both real
// wiring paths must pass their own owner too; the storage-only public adapter
// has no external stores and cannot substitute for this production binding.
// origin: S7 internal review BROKER-REDS F3; hot-update S7 plan §11.4 / S8 plan §6.2
func TestClusterSessionCascadeOwnersAreWired(t *testing.T) {
	for file, function := range map[string]string{"clusterwrite.go": "wireClusterLate", "cluster_forward.go": "rebindBrokerImageAuthority"} {
		source, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		for _, decl := range source.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok || name.Name != "subscribeClusterApplyOwned" {
					return true
				}
				calls++
				if len(call.Args) != 7 {
					t.Errorf("%s: session cascade owner is absent", function)
					return true
				}
				owner, ok := call.Args[6].(*ast.Ident)
				if !ok || owner.Name != "b" {
					t.Errorf("%s: responder does not retain its current broker", function)
				}
				return true
			})
		}
		if calls != 1 {
			t.Errorf("%s: found %d current apply responders, want exactly one", function, calls)
		}
	}
}
