package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// cluster_unlock_test.go (R7b) — the explicit stale-lock clear.
//
// The lease alone leaves two gaps a TTL cannot close: an operator who should not have to wait out a
// deliberately-conservative 15 minutes, and a pre-R7b lock that carries no lease and therefore NEVER
// expires. This verb closes both. Its danger is the mirror image of the reaper's: the reaper must not
// expire a live lock, and this verb must not let an operator rip one out by accident.

var unlockNow = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)

// unlockFixture wires a fake cluster: a health responder advertising the given lock state, and trigger
// responders that record the release-lock requests they receive.
type unlockFixture struct {
	nc      *nats.Conn
	actor   string
	seed    []byte
	mu      sync.Mutex
	cleared []string // "upgrade" / "grow", in the order released
	// stillHeld makes the health responder keep advertising the lock even after a successful release —
	// the "the trigger said OK but nothing changed" shape.
	stillHeld atomic.Bool
	refuse    atomic.Bool
}

func newUnlockFixture(t *testing.T, name string, upgradeHeld, growHeld bool, upgradeLease, growLease string) *unlockFixture {
	t.Helper()
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	seed, _ := cliExternalAccount(t)
	f := &unlockFixture{nc: nc, actor: "Uactor-unlock-" + name, seed: seed}

	var live struct {
		sync.Mutex
		upgrade, grow bool
	}
	live.upgrade, live.grow = upgradeHeld, growHeld

	mustSub(t, nc, proto.SubjCtrlClusterHealth(f.actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		live.Lock()
		u, g := live.upgrade, live.grow
		live.Unlock()
		body, _ := json.Marshal(proto.ClusterHealthResp{
			NodeID: "brk-leader", WritableLeaderConfirmed: true, LeaderID: "brk-leader",
			UpgradeLockActive: u, GrowLockActive: g,
			UpgradeLeaseExpiry: upgradeLease, GrowLeaseExpiry: growLease,
		})
		_ = m.Respond(body)
	})
	release := func(kind string) bool {
		if f.refuse.Load() {
			return false
		}
		f.mu.Lock()
		f.cleared = append(f.cleared, kind)
		f.mu.Unlock()
		if !f.stillHeld.Load() {
			live.Lock()
			if kind == "upgrade" {
				live.upgrade = false
			} else {
				live.grow = false
			}
			live.Unlock()
		}
		return true
	}
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(f.actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op != "release-lock" {
			return
		}
		resp := proto.ClusterUpgradeResp{OK: release("upgrade")}
		if !resp.OK {
			resp.Code, resp.Error = "store_error", "raft down"
		}
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterGrow(f.actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterGrowReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op != "release-lock" {
			return
		}
		resp := proto.ClusterGrowResp{OK: release("grow")}
		if !resp.OK {
			resp.Code, resp.Error = "store_error", "raft down"
		}
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *unlockFixture) clears() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cleared...)
}

func rfc(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// TestUnlockRefusesALiveLease is the safety property. A lease in the future means somebody IS renewing —
// a fact, not a guess. Clearing it would drop the membership mutex under a running roll/grow and let a
// second membership change interleave, which is exactly the corruption the lock exists to prevent.
func TestUnlockRefusesALiveLease(t *testing.T) {
	f := newUnlockFixture(t, "live", true, false, rfc(unlockNow.Add(10*time.Minute)), "")
	var out bytes.Buffer

	err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, true, false, false)
	if err == nil {
		t.Fatal("clearing a lock whose lease is still being renewed must be REFUSED — an orchestrator is running")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatalf("a refusal must be non-zero, got %d", rc)
	}
	if got := f.clears(); len(got) != 0 {
		t.Fatalf("the lock was released despite a live lease: %v", got)
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Fatalf("the refusal must tell the operator about the override; got %v", err)
	}
	// The report must name the expiry so the operator can decide how long to wait.
	if !strings.Contains(out.String(), "lease LIVE until") {
		t.Fatalf("the report must show the live lease; got %q", out.String())
	}
}

// TestUnlockForceOverridesALiveLease: the escape hatch works, and says so loudly.
func TestUnlockForceOverridesALiveLease(t *testing.T) {
	f := newUnlockFixture(t, "force", true, false, rfc(unlockNow.Add(10*time.Minute)), "")
	var out bytes.Buffer

	if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, true, true, false); err != nil {
		t.Fatalf("--force must clear a live lock: %v", err)
	}
	if got := f.clears(); len(got) != 1 || got[0] != "upgrade" {
		t.Fatalf("expected exactly the upgrade lock to be released, got %v", got)
	}
	if !strings.Contains(out.String(), "--force") {
		t.Fatalf("forcing past a live lease must be LOUD in the output; got %q", out.String())
	}
}

// TestUnlockClearsAnExpiredLease: the impatient-operator case. The reaper would get there too, but the
// operator should not have to wait for it.
func TestUnlockClearsAnExpiredLease(t *testing.T) {
	f := newUnlockFixture(t, "expired", false, true, "", rfc(unlockNow.Add(-time.Minute)))
	var out bytes.Buffer

	if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, true, false, false); err != nil {
		t.Fatalf("an expired lease must clear without --force: %v", err)
	}
	if got := f.clears(); len(got) != 1 || got[0] != "grow" {
		t.Fatalf("expected the grow lock to be released, got %v", got)
	}
	if !strings.Contains(out.String(), "lease EXPIRED") {
		t.Fatalf("the report must show WHY it was safe to clear; got %q", out.String())
	}
}

// TestUnlockClearsALeaselessLock is case (2) of the verb's reason to exist: a marker acquired by a
// pre-R7b broker carries no lease, the reaper is fail-closed on exactly that shape, and so it NEVER
// expires. Without this path the only remedy is hand-editing a replicated SQLite row.
func TestUnlockClearsALeaselessLock(t *testing.T) {
	f := newUnlockFixture(t, "noleases", true, true, "", "")
	var out bytes.Buffer

	if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, true, false, false); err != nil {
		t.Fatalf("a leaseless lock must be clearable — nothing else in the system will EVER release it: %v", err)
	}
	got := f.clears()
	if len(got) != 2 {
		t.Fatalf("both held locks should have been released, got %v", got)
	}
	if !strings.Contains(out.String(), "NEVER expire") {
		t.Fatalf("the report must say the lock would never self-heal, or the operator cannot tell this apart from "+
			"an ordinary expired lease; got %q", out.String())
	}
}

// TestUnlockConfirmsOrFails is the rc discipline. A trigger that replies OK while the lock stays
// advertised means membership is STILL fenced — reporting success there is the "control plane wrote, data
// plane did not converge, rc=0" shape this whole batch targets.
func TestUnlockConfirmsOrFails(t *testing.T) {
	f := newUnlockFixture(t, "unconfirmed", true, false, rfc(unlockNow.Add(-time.Minute)), "")
	f.stillHeld.Store(true) // the release "succeeds" but nothing changes
	var out bytes.Buffer

	err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, false, false, false)
	if err == nil {
		t.Fatal("the lock is still advertised as held after the release — that is NOT a success, membership stays fenced")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatalf("rc must be non-zero when convergence was not confirmed, got %d", rc)
	}
	if !strings.Contains(err.Error(), "STILL advertised") {
		t.Fatalf("the error must say what is actually wrong; got %v", err)
	}
}

// TestUnlockSurfacesARefusedRelease: a leader that refuses the release must not be swallowed.
func TestUnlockSurfacesARefusedRelease(t *testing.T) {
	f := newUnlockFixture(t, "refused", true, false, rfc(unlockNow.Add(-time.Minute)), "")
	f.refuse.Store(true)
	var out bytes.Buffer

	err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, false, false, false)
	if err == nil {
		t.Fatal("a refused release must be reported, not swallowed")
	}
	if rc := classifyExit(err); rc == exitOK {
		t.Fatalf("rc must be non-zero, got %d", rc)
	}
}

// TestUnlockNothingHeldIsACleanNoOp: running the verb on a healthy cluster must be safe and silent-ish.
// An operator reaching for this while debugging must not be able to make things worse.
func TestUnlockNothingHeldIsACleanNoOp(t *testing.T) {
	f := newUnlockFixture(t, "clean", false, false, "", "")
	var out bytes.Buffer

	if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, true, false, false); err != nil {
		t.Fatalf("running unlock on a cluster holding no locks must be a clean no-op: %v", err)
	}
	if got := f.clears(); len(got) != 0 {
		t.Fatalf("nothing was held, yet a release was sent: %v", got)
	}
	if !strings.Contains(out.String(), "no membership locks are held") {
		t.Fatalf("the operator must be told nothing needed doing; got %q", out.String())
	}
}

// TestUnlockSelectorsAreHonored: --upgrade must not touch the grow lock, and vice versa.
func TestUnlockSelectorsAreHonored(t *testing.T) {
	t.Run("upgrade only", func(t *testing.T) {
		f := newUnlockFixture(t, "sel-up", true, true, rfc(unlockNow.Add(-time.Minute)), rfc(unlockNow.Add(-time.Minute)))
		var out bytes.Buffer
		if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, false, false, false); err != nil {
			t.Fatal(err)
		}
		if got := f.clears(); len(got) != 1 || got[0] != "upgrade" {
			t.Fatalf("--upgrade released %v — it must not touch the grow lock", got)
		}
	})
	t.Run("grow only", func(t *testing.T) {
		f := newUnlockFixture(t, "sel-grow", true, true, rfc(unlockNow.Add(-time.Minute)), rfc(unlockNow.Add(-time.Minute)))
		var out bytes.Buffer
		if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, false, true, false, false); err != nil {
			t.Fatal(err)
		}
		if got := f.clears(); len(got) != 1 || got[0] != "grow" {
			t.Fatalf("--grow released %v", got)
		}
	})
	// A live lease on a lock the operator did NOT select must not block the one they did.
	t.Run("a live lease on the unselected lock does not block", func(t *testing.T) {
		f := newUnlockFixture(t, "sel-mixed", true, true, rfc(unlockNow.Add(-time.Minute)), rfc(unlockNow.Add(time.Hour)))
		var out bytes.Buffer
		if err := runClusterUnlock(context.Background(), f.nc, f.actor, f.seed, &out, unlockNow, true, false, false, false); err != nil {
			t.Fatalf("a live GROW lease must not block an --upgrade-only clear: %v", err)
		}
		if got := f.clears(); len(got) != 1 || got[0] != "upgrade" {
			t.Fatalf("expected only the upgrade lock, got %v", got)
		}
	})
}

// TestUnlockDryRunTouchesNothing.
func TestUnlockDryRunTouchesNothing(t *testing.T) {
	f := newUnlockFixture(t, "dryrun", true, false, rfc(unlockNow.Add(-time.Minute)), "")
	var out bytes.Buffer
	if err := runClusterUnlock(context.Background(), f.nc, f.actor, nil, &out, unlockNow, true, true, false, true); err != nil {
		t.Fatalf("--dry-run must work without a seed and without touching anything: %v", err)
	}
	if got := f.clears(); len(got) != 0 {
		t.Fatalf("--dry-run released %v", got)
	}
	if !strings.Contains(out.String(), "dry-run") {
		t.Fatalf("got %q", out.String())
	}
}

// TestUnlockRequiresASeedToExecute: the release is an account-signed trigger. Executing without a seed
// must fail with a usage error naming the flag, never a bare "nothing to do" over a fenced cluster.
func TestUnlockRequiresASeedToExecute(t *testing.T) {
	f := newUnlockFixture(t, "noseed", true, false, rfc(unlockNow.Add(-time.Minute)), "")
	var out bytes.Buffer
	err := runClusterUnlock(context.Background(), f.nc, f.actor, nil, &out, unlockNow, true, true, false, false)
	if err == nil {
		t.Fatal("executing without a seed must fail, not silently skip a fenced cluster")
	}
	if !strings.Contains(err.Error(), "--account-seed") {
		t.Fatalf("the error must name the missing flag; got %v", err)
	}
}

// TestUnlockLeaseMergeIsFailClosed: brokers disagree (a lagging follower still has the OLD expiry, the
// leader has the renewed one). The verb must take the LATEST — treating the lock as live — or a lagging
// replica's stale row would authorize ripping out a lock that is actively being renewed.
func TestUnlockLeaseMergeIsFailClosed(t *testing.T) {
	var v lockView
	v.name = "upgrade"
	mergeLease(&v, rfc(unlockNow.Add(-time.Hour))) // a lagging follower: expired
	mergeLease(&v, rfc(unlockNow.Add(time.Hour)))  // the leader: renewed
	if !v.live(unlockNow) {
		t.Fatal("a lagging follower's stale expiry overrode the leader's renewal — the verb would rip out a LIVE lock")
	}
	// Order must not matter.
	var w lockView
	mergeLease(&w, rfc(unlockNow.Add(time.Hour)))
	mergeLease(&w, rfc(unlockNow.Add(-time.Hour)))
	if !w.live(unlockNow) {
		t.Fatal("mergeLease is order-dependent — it must always keep the LATEST expiry")
	}
	// Garbage contributes nothing rather than reading as "expired".
	var x lockView
	mergeLease(&x, "not-a-timestamp")
	if x.leased {
		t.Fatal("an unparseable expiry was accepted as a lease")
	}
	mergeLease(&x, "")
	if x.leased {
		t.Fatal("an empty expiry was accepted as a lease")
	}
}
