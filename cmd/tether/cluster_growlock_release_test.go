package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// cluster_growlock_release_test.go (R7a, #31) — the CLI half of the grow-lock fix.
//
// The broker-side reconciliation pass makes a leaked cluster_grow_active marker
// recoverable; these tests pin the two CLI-side properties that no reconciler can
// supply:
//
//   1. a transient release failure is RETRIED rather than surrendered on the first
//      attempt (the common case is an election or a dropped reply, and a second
//      try a second later succeeds), and
//   2. an unrecovered release makes `cluster add` exit NON-ZERO. Before R7 it
//      printed a warning and exited 0 — handing automation a success signal over a
//      cluster whose entire membership control plane was fenced. That "control
//      plane wrote, data plane did not converge, rc=0" shape is the failure mode
//      the R7 exit criterion targets.

func fastGrowLockBackoff(t *testing.T) {
	t.Helper()
	prev := growLockReleaseBackoff
	growLockReleaseBackoff = time.Millisecond
	t.Cleanup(func() { growLockReleaseBackoff = prev })
}

// serveLeaderHealth answers the cluster-health probe so currentLeader resolves.
func serveLeaderHealth(t *testing.T, nc *nats.Conn, actor, leader string) {
	t.Helper()
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.ClusterHealthResp{
			NodeID: leader, WritableLeaderConfirmed: true, LeaderID: leader,
		})
		_ = m.Respond(body)
	})
}

// TestReleaseGrowLockRetriesTransientFailure: the first two release attempts fail,
// the third succeeds. The CLI must converge and report success — the whole point of
// retrying is that the overwhelmingly common failure here is transient.
func TestReleaseGrowLockRetriesTransientFailure(t *testing.T) {
	fastGrowLockBackoff(t)
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-r7-retry"
	serveLeaderHealth(t, nc, actor, "brk-leader")

	var calls int32
	mustSub(t, nc, proto.SubjCtrlClusterGrow(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterGrowReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op != "release-lock" {
			return
		}
		resp := proto.ClusterGrowResp{OK: true}
		if atomic.AddInt32(&calls, 1) <= 2 {
			resp = proto.ClusterGrowResp{Code: "not_leader", Error: "leadership moved"}
		}
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := releaseGrowLock(context.Background(), nc, actor, seed, "brk-joiner", &out); err != nil {
		t.Fatalf("a transient release failure must be retried to success, got %v", err)
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("expected 3 attempts (2 transient failures + 1 success), got %d — the release is not retrying", n)
	}
	if !strings.Contains(out.String(), "retrying grow lock release") {
		t.Fatalf("the operator must SEE the retries, got %q", out.String())
	}
}

// TestReleaseGrowLockFailsNonZero: every attempt is refused. The CLI must surface a
// non-zero exit class, not a warning and rc=0.
func TestReleaseGrowLockFailsNonZero(t *testing.T) {
	fastGrowLockBackoff(t)
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-r7-fail"
	serveLeaderHealth(t, nc, actor, "brk-leader")

	var calls int32
	mustSub(t, nc, proto.SubjCtrlClusterGrow(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		atomic.AddInt32(&calls, 1)
		body, _ := json.Marshal(proto.ClusterGrowResp{Code: "store_error", Error: "raft down"})
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err = releaseGrowLock(context.Background(), nc, actor, seed, "brk-joiner", &out)
	if err == nil {
		t.Fatal("#31: an unconfirmed grow-lock release must NOT be reported as success — membership is left fenced")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatalf("classifyExit returned %d for an unreleased grow lock; convergence was not reached so rc must be non-zero", rc)
	} else if rc != exitUnavailable {
		t.Fatalf("expected the EX_UNAVAILABLE class (%d) for an unreachable/refusing leader, got %d", exitUnavailable, rc)
	}
	if n := atomic.LoadInt32(&calls); int(n) != growLockReleaseAttempts {
		t.Fatalf("expected %d attempts before giving up, got %d", growLockReleaseAttempts, n)
	}
	// The message must tell the operator BOTH that membership is blocked and how it clears.
	for _, want := range []string{"cluster_grow_active", "BLOCKED", "reconciler"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure message must mention %q so the operator knows the blast radius and the remedy; got: %v", want, err)
		}
	}
}

// TestReleaseGrowLockNoLeaderFailsNonZero: the leader cannot even be resolved. This
// is the exact path that used to print "could not resolve the leader …" and return
// as if nothing had happened.
func TestReleaseGrowLockNoLeaderFailsNonZero(t *testing.T) {
	fastGrowLockBackoff(t)
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)

	var out bytes.Buffer
	// No health responder at all ⇒ currentLeader fails on every attempt.
	err = releaseGrowLock(context.Background(), nc, "Uactor-r7-noleader", seed, "brk-joiner", &out)
	if err == nil {
		t.Fatal("#31: an unresolvable leader must not be swallowed — the lock stays set and membership stays fenced")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatalf("rc must be non-zero when the grow lock could not be released, got %d", rc)
	}
}
