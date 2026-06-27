package adminsock

import "testing"

// D7: a cluster verb with no ClusterAdminBackend wired (production until the D9
// cutover) replies "cluster mode not enabled"; with a backend it routes there.

func TestD7ClusterModeNotEnabled(t *testing.T) {
	s := New("/unused", Backend{}) // Backend.Cluster == nil
	// OpClusterAdd is intentionally omitted — v0.4.2 retired it from clusterOps (its direct-AddVoter
	// backend could wedge N=1); it is now an unrouted op, not a cluster op.
	for _, op := range []string{OpClusterStatus, OpClusterDrain, OpClusterRemove, OpClusterTransfer, OpClusterRotateCrt, OpClusterSetRaftAddr} {
		resp := s.dispatch(Request{Op: op})
		if resp.Error != "cluster mode not enabled" {
			t.Errorf("op %s with nil cluster backend: got %q, want \"cluster mode not enabled\"", op, resp.Error)
		}
	}
}

type stubClusterBackend struct{ called string }

func (b *stubClusterBackend) HandleCluster(req Request) Response {
	b.called = req.Op
	return Response{Op: req.Op, OK: true}
}

func TestD7ClusterRoutesToBackend(t *testing.T) {
	stub := &stubClusterBackend{}
	s := New("/unused", Backend{Cluster: stub})
	resp := s.dispatch(Request{Op: OpClusterStatus})
	if !resp.OK || stub.called != OpClusterStatus {
		t.Fatalf("cluster op did not route to the backend: resp=%+v called=%q", resp, stub.called)
	}
	// A non-cluster unknown op is still "unknown op".
	if got := s.dispatch(Request{Op: "bogus"}).Error; got != "unknown op: bogus" {
		t.Fatalf("unknown op: got %q", got)
	}
}
