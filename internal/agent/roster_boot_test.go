package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// roster_boot_test.go (C1 Stage-C TEST-7/10/11) — restart anti-rollback round-trip through a REAL
// stateStore, and the wire/state byte-equivalence guards that rest on omitempty tags.

// TestRosterRestartAntiRollback (TEST-7): the persisted pin + generation hwm survive a restart, an
// EXPIRED cached roster contributes pin+hwm but NOT its URLs (seed-only), and a validly-signed lower
// generation is still rejected after the restart (anti-replay is durable, not process-local).
func TestRosterRestartAntiRollback(t *testing.T) {
	seed, pub := mustAccount(t)
	store := newStateStore(t.TempDir(), "lab")
	expired := buildTestRoster(t, seed, pub, 50, -time.Hour) // expires_at in the past
	if err := store.SetRosterCache(&RosterCache{PinAccountPub: pub, Generation: 50, Roster: expired}); err != nil {
		t.Fatal(err)
	}

	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.stateStore = store
	a.loadRosterCacheAtBoot()

	if a.pinnedAccountPub() != pub {
		t.Fatalf("pin must survive restart: %q", a.pinnedAccountPub())
	}
	if a.cachedRosterGen() != 50 {
		t.Fatalf("generation hwm must survive restart: %d", a.cachedRosterGen())
	}
	if got := a.effectiveDialURLs(); got != "nats://seed.example.com:4222" {
		t.Fatalf("an expired cached roster must drop its URLs (seed-only): %q", got)
	}
	// Anti-rollback survives the restart: a validly-signed lower generation is rejected.
	a.adoptRoster(buildTestRoster(t, seed, pub, 49, time.Hour))
	if a.cachedRosterGen() != 50 {
		t.Fatalf("post-restart rollback must be rejected: %d", a.cachedRosterGen())
	}
	// And a fresh higher-gen roster is adopted (re-learns URLs).
	a.adoptRoster(buildTestRoster(t, seed, pub, 51, time.Hour))
	if a.cachedRosterGen() != 51 || !strings.Contains(a.effectiveDialURLs(), "b1.example.com") {
		t.Fatalf("post-restart higher gen must be accepted + relearn URLs: gen=%d urls=%q", a.cachedRosterGen(), a.effectiveDialURLs())
	}
}

// TestNodeRegisterReqOmitsRosterFieldsWhenZero (TEST-10): the REQUEST byte-equivalence rests on the
// two omitempty tags — a pre-C1-shaped request must omit both.
func TestNodeRegisterReqOmitsRosterFieldsWhenZero(t *testing.T) {
	b, _ := json.Marshal(proto.NodeRegisterReq{ProtoVersion: proto.ProtoVersion, NID: "n"})
	if s := string(b); strings.Contains(s, "roster_gen") || strings.Contains(s, "roster_refresh_only") {
		t.Fatalf("zero roster fields must be omitted (request byte-equiv): %s", s)
	}
	b2, _ := json.Marshal(proto.NodeRegisterReq{ProtoVersion: proto.ProtoVersion, NID: "n", RosterGen: 5, RosterRefreshOnly: true})
	if s := string(b2); !strings.Contains(s, "roster_gen") || !strings.Contains(s, "roster_refresh_only") {
		t.Fatalf("set roster fields must serialize: %s", s)
	}
}

// TestStateFileRosterForwardBackCompat (TEST-11): a pre-C1 state.json (no roster key) loads with
// Roster==nil and re-marshals without the key; a roster-bearing file round-trips (downgrade ignores
// the key — no DisallowUnknownFields).
func TestStateFileRosterForwardBackCompat(t *testing.T) {
	sf, err := parseStateBytes([]byte(`{"port_tokens":[]}`), nil)
	if err != nil || sf.Roster != nil {
		t.Fatalf("pre-C1 state must load with nil Roster: %+v %v", sf, err)
	}
	out, _ := json.Marshal(sf)
	if strings.Contains(string(out), "roster") {
		t.Fatalf("nil Roster must omit the key: %s", out)
	}
	sf2, err := parseStateBytes([]byte(`{"port_tokens":[],"roster":{"pin_account_pub":"P","generation":7}}`), nil)
	if err != nil || sf2.Roster == nil || sf2.Roster.Generation != 7 || sf2.Roster.PinAccountPub != "P" {
		t.Fatalf("roster cache must round-trip: %+v %v", sf2, err)
	}
}
