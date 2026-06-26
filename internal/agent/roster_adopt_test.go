package agent

import (
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// roster_adopt_test.go (C1 §8) — adversarial unit tests of the CONSUMER decision table
// (adoptRoster). These prove the verify/reject rules are TRULY enforced at the agent, not
// merely available in the verifier library: sig-fail / account-mismatch / generation-rollback /
// expired / schema / TOFU / empty-set-floor / nil all behave per c1-plan.md §3.

var rosterTestBase = time.Unix(1700000000, 0).UTC()

func newRosterTestAgent(oobPin string, now func() time.Time) *Agent {
	a := &Agent{cfg: Config{
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		NATSURL:    "nats://seed.example.com:4222",
		AccountPub: oobPin,
		Now:        now,
	}}
	a.loadRosterCacheAtBoot() // seed pinAccount from the OOB override (no stateStore → in-memory only)
	return a
}

func mustAccount(t *testing.T) (seed []byte, pub string) {
	t.Helper()
	s, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	p, err := auth.PublicKeyFromSeed(s)
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}

// buildTestRoster signs a roster with one VOTER broker (dialable host) at the given generation
// and expiry (expiresAt relative to rosterTestBase).
func buildTestRoster(t *testing.T, seed []byte, pub string, gen uint64, expiresIn time.Duration, hosts ...string) *proto.ClusterRoster {
	t.Helper()
	if len(hosts) == 0 {
		hosts = []string{"b1.example.com"}
	}
	var bs []proto.RosterBroker
	for i, h := range hosts {
		bs = append(bs, proto.RosterBroker{
			NodeID:     "b" + string(rune('1'+i)),
			NatsRoute:  "nats://10.0.0.1:6222",
			PublicHost: h,
			Phase:      proto.RosterPhaseVoter,
		})
	}
	r, err := clusterroster.Build(seed, pub, gen, bs,
		rosterTestBase.Format(time.RFC3339Nano),
		rosterTestBase.Add(expiresIn).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAdoptTOFUFirstWins(t *testing.T) {
	seedA, pubA := mustAccount(t)
	seedB, pubB := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })

	// First valid roster TOFU-pins account A and learns its URL.
	a.adoptRoster(buildTestRoster(t, seedA, pubA, 10, time.Hour))
	if a.pinnedAccountPub() != pubA {
		t.Fatalf("TOFU must pin first account: got %q want %q", a.pinnedAccountPub(), pubA)
	}
	if a.cachedRosterGen() != 10 {
		t.Fatalf("gen hwm not advanced: %d", a.cachedRosterGen())
	}
	if got := a.effectiveDialURLs(); !strings.Contains(got, "b1.example.com") {
		t.Fatalf("learned URL not in dial pool: %q", got)
	}

	// A later roster from a DIFFERENT account (validly self-signed) must be rejected — the pin sticks.
	a.adoptRoster(buildTestRoster(t, seedB, pubB, 11, time.Hour))
	if a.pinnedAccountPub() != pubA {
		t.Fatalf("a foreign-account roster must not repin: got %q", a.pinnedAccountPub())
	}
	if a.cachedRosterGen() != 10 {
		t.Fatalf("a rejected roster must not advance gen: %d", a.cachedRosterGen())
	}
}

func TestAdoptOOBPinBeatsForeignRoster(t *testing.T) {
	seedA, pubA := mustAccount(t)
	seedB, pubB := mustAccount(t)
	// OOB pin = A; a roster validly self-signed by B must be rejected (no TOFU).
	a := newRosterTestAgent(pubA, func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seedB, pubB, 5, time.Hour))
	if a.cachedRosterGen() != 0 {
		t.Fatalf("OOB pin must reject a foreign-account roster, gen=%d", a.cachedRosterGen())
	}
	// A roster from A is accepted.
	a.adoptRoster(buildTestRoster(t, seedA, pubA, 5, time.Hour))
	if a.cachedRosterGen() != 5 {
		t.Fatalf("OOB-pinned account roster must be accepted, gen=%d", a.cachedRosterGen())
	}
}

func TestAdoptRejectSigFail(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seed, pub, 3, time.Hour)) // pin + gen 3
	r := buildTestRoster(t, seed, pub, 4, time.Hour)
	// Flip a sig nibble.
	b := []byte(r.Sig)
	if b[0] == 'a' {
		b[0] = 'b'
	} else {
		b[0] = 'a'
	}
	r.Sig = string(b)
	a.adoptRoster(r)
	if a.cachedRosterGen() != 3 {
		t.Fatalf("a sig-fail roster must be rejected (gen kept at 3): %d", a.cachedRosterGen())
	}
}

// TestAdoptRejectAccountMismatch proves adoptRoster passes the PINNED account to VerifyAt, never the
// roster's self-claimed AccountPub: a roster validly self-signed by B (B claims B) is rejected once A
// is pinned. If the call used r.AccountPub the self-signed B roster would wrongly verify.
func TestAdoptRejectAccountMismatch(t *testing.T) {
	seedA, pubA := mustAccount(t)
	seedB, pubB := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seedA, pubA, 7, time.Hour)) // pin A
	a.adoptRoster(buildTestRoster(t, seedB, pubB, 8, time.Hour)) // B self-signed
	if a.cachedRosterGen() != 7 {
		t.Fatalf("account-mismatch roster must be rejected against the PIN: gen=%d", a.cachedRosterGen())
	}
}

func TestAdoptRejectGenRollback(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seed, pub, 5, time.Hour)) // hwm 5
	a.adoptRoster(buildTestRoster(t, seed, pub, 4, time.Hour)) // rollback → reject
	if a.cachedRosterGen() != 5 {
		t.Fatalf("generation rollback must be rejected: %d", a.cachedRosterGen())
	}
	a.adoptRoster(buildTestRoster(t, seed, pub, 5, time.Hour)) // idempotent
	if a.cachedRosterGen() != 5 {
		t.Fatalf("equal gen must stay 5: %d", a.cachedRosterGen())
	}
	a.adoptRoster(buildTestRoster(t, seed, pub, 6, time.Hour)) // advance
	if a.cachedRosterGen() != 6 {
		t.Fatalf("higher gen must advance to 6: %d", a.cachedRosterGen())
	}
}

func TestAdoptRejectExpired(t *testing.T) {
	seed, pub := mustAccount(t)
	now := rosterTestBase
	a := newRosterTestAgent("", func() time.Time { return now })
	a.adoptRoster(buildTestRoster(t, seed, pub, 9, time.Hour)) // valid, learns b1
	priorURLs := a.effectiveDialURLs()
	// A roster that already expired at `now` is rejected for adoption; prior URLs + gen kept.
	a.adoptRoster(buildTestRoster(t, seed, pub, 10, -time.Minute))
	if a.cachedRosterGen() != 9 {
		t.Fatalf("expired roster must not advance gen: %d", a.cachedRosterGen())
	}
	if a.effectiveDialURLs() != priorURLs {
		t.Fatalf("expired roster must keep prior URLs")
	}
}

func TestAdoptRejectSchema(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seed, pub, 2, time.Hour)) // pin
	// Hand-build a future-schema roster and sign it validly — VerifyAt must reject on SCHEMA, not sig.
	r := &proto.ClusterRoster{
		SchemaVersion: proto.ClusterRosterSchemaVersion + 1,
		Generation:    3,
		AccountPub:    pub,
		Brokers:       []proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		IssuedAt:      rosterTestBase.Format(time.RFC3339Nano),
		ExpiresAt:     rosterTestBase.Add(time.Hour).Format(time.RFC3339Nano),
	}
	sig, err := auth.SignWithSeed(seed, clusterroster.CanonicalRosterBytes(r))
	if err != nil {
		t.Fatal(err)
	}
	r.Sig = hex.EncodeToString(sig)
	a.adoptRoster(r)
	if a.cachedRosterGen() != 2 {
		t.Fatalf("future-schema roster must be rejected: %d", a.cachedRosterGen())
	}
}

func TestAdoptNilNoOp(t *testing.T) {
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(nil) // must not panic, must not pin
	if a.pinnedAccountPub() != "" || a.cachedRosterGen() != 0 {
		t.Fatalf("nil roster must be a no-op")
	}
}

func TestAdoptEmptySetFloorKeepsPrior(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seed, pub, 1, time.Hour, "b1.example.com")) // learns b1
	prior := a.effectiveDialURLs()
	// A newer, valid roster whose only broker is loopback → DialURLs empty → keep prior URLs,
	// but the generation still advances (the roster was valid).
	a.adoptRoster(buildTestRoster(t, seed, pub, 2, time.Hour, "127.0.0.1"))
	if a.cachedRosterGen() != 2 {
		t.Fatalf("a valid (if all-loopback) roster should still advance gen: %d", a.cachedRosterGen())
	}
	if a.effectiveDialURLs() != prior {
		t.Fatalf("empty usable set must keep prior URLs (never empty the pool): %q vs %q", a.effectiveDialURLs(), prior)
	}
}

func TestAdoptMalformedNoPanic(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	a.adoptRoster(buildTestRoster(t, seed, pub, 4, time.Hour)) // pin + gen 4
	for _, bad := range []*proto.ClusterRoster{
		{Generation: 5, AccountPub: pub, Sig: ""},   // empty sig
		{Generation: 5, AccountPub: pub, Sig: "zz"}, // non-hex
		{Generation: 5, AccountPub: "", Sig: "ab"},  // empty account
		{Generation: 5, AccountPub: pub},            // no brokers, no sig
	} {
		a.adoptRoster(bad) // must not panic
	}
	if a.cachedRosterGen() != 4 || a.pinnedAccountPub() != pub {
		t.Fatalf("malformed rosters must all be rejected, mirror intact: gen=%d pin=%q", a.cachedRosterGen(), a.pinnedAccountPub())
	}
}

// TestEffectiveDialURLsSeedFloor proves the configured seed is always present (permanent floor) and
// the learned VOTER URL precedes it.
func TestEffectiveDialURLsSeedFloor(t *testing.T) {
	seed, pub := mustAccount(t)
	a := newRosterTestAgent("", func() time.Time { return rosterTestBase })
	if got := a.effectiveDialURLs(); got != "nats://seed.example.com:4222" {
		t.Fatalf("with no roster the dial string is just the seed: %q", got)
	}
	a.adoptRoster(buildTestRoster(t, seed, pub, 1, time.Hour, "b1.example.com"))
	got := a.effectiveDialURLs()
	if !strings.Contains(got, "b1.example.com:4222") || !strings.Contains(got, "seed.example.com:4222") {
		t.Fatalf("dial pool must union learned + seed floor: %q", got)
	}
	if strings.Index(got, "b1.example.com") > strings.Index(got, "seed.example.com") {
		t.Fatalf("learned VOTER URL must precede the seed floor: %q", got)
	}
}
