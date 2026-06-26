package natsreconcile

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/natscluster"
)

// reconcile_test.go (C3) — the engine's fail-closed + observed-honesty invariants. NOTE (mega-audit
// MAJ-12): the full render→DryRun→swap→reload→probe path against a REAL nats-server is a TRACKED,
// NOT-YET-DELIVERED follow-up (the gated test/c3 reload smoke is deferred — see c3-review.md); it is
// NOT proven here. This file pins the security-load-bearing early exits + the rule that observed is
// NEVER synthesized from "reload returned nil".

func twoPeers() []natscluster.Broker {
	return []natscluster.Broker{
		{ServerName: "brk-a", NkeyPub: "UAAA", RouteURL: "nats://10.0.0.1:6222"},
		{ServerName: "brk-b", NkeyPub: "UBBB", RouteURL: "nats://10.0.0.2:6222"},
	}
}

func TestReconcileDesiredZeroIsNoop(t *testing.T) {
	out := ReconcileOnce(Inputs{SelfServerName: "brk-a", Peers: twoPeers(), DesiredGen: 0}, 0, 0, nil, nil)
	if out.Action != ActionNoop {
		t.Fatalf("desired==0 must be a noop, got %q", out.Action)
	}
}

func TestReconcileUnresolvableEmptyBusNkey(t *testing.T) {
	peers := twoPeers()
	peers[1].NkeyPub = "" // brk-b has not self-backfilled its bus nkey yet
	out := ReconcileOnce(Inputs{SelfServerName: "brk-a", Peers: peers, DesiredGen: 5, ConfPath: "/nonexistent"}, 0, 0,
		func() error { t.Fatal("must NOT reload while unresolvable"); return nil }, nil)
	if out.Action != ActionUnresolvable {
		t.Fatalf("a mesh voter with no bus nkey must be UNRESOLVABLE (never render a conf dropping it), got %q", out.Action)
	}
	if out.AppliedGen != 0 || out.ObservedGen != 0 {
		t.Fatalf("unresolvable must not advance applied/observed: %+v", out)
	}
}

func TestReconcileUnresolvableSelfNotInPeers(t *testing.T) {
	out := ReconcileOnce(Inputs{SelfServerName: "brk-Z", Peers: twoPeers(), DesiredGen: 5, ConfPath: "/nonexistent"}, 0, 0, nil, nil)
	if out.Action != ActionUnresolvable {
		t.Fatalf("self absent from the mesh must be UNRESOLVABLE, got %q", out.Action)
	}
}

func TestReconcileUnknownDirectiveFailClosed(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	// A hand-tuned conf with a directive the takeover does not recognize → Preflight must refuse, and
	// the reconciler must NOT swap (never silently overwrite a hand-tuned conf).
	if err := os.WriteFile(conf, []byte("port: 4222\nbogus_unknown_directive: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := ReconcileOnce(Inputs{SelfServerName: "brk-a", Peers: twoPeers(), DesiredGen: 5, ConfPath: conf}, 0, 0,
		func() error { t.Fatal("must NOT reload on an unknown-directive conf"); return nil }, nil)
	if out.Action != ActionUnknownDirective {
		t.Fatalf("an unknown directive must be fail-closed (no swap), got %q (%v)", out.Action, out.Err)
	}
	// The conf is untouched.
	b, _ := os.ReadFile(conf)
	if string(b) != "port: 4222\nbogus_unknown_directive: 1\n" {
		t.Fatal("the hand-tuned conf must NOT be overwritten")
	}
}

// TestObservedNeverSynthesized pins the D-B invariant: observed depends on the REAL probe, never on
// "reload returned nil". A nil probe, a probe error, and a stale config_load_time all mean NOT-confirmed.
func TestObservedNeverSynthesized(t *testing.T) {
	swap := time.Unix(1700000000, 0)
	cases := []struct {
		name  string
		probe func() (time.Time, error)
		want  bool
	}{
		{"nil probe", nil, false},
		{"probe error", func() (time.Time, error) { return time.Time{}, os.ErrDeadlineExceeded }, false},
		{"stale load time (before swap)", func() (time.Time, error) { return swap.Add(-time.Second), nil }, false},
		{"advanced load time (>= swap)", func() (time.Time, error) { return swap.Add(time.Second), nil }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := observedConfirmed(tc.probe, swap); got != tc.want {
				t.Fatalf("observedConfirmed=%v want %v — observed must reflect the LIVE server, not a synthesized green", got, tc.want)
			}
		})
	}
}
