package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
)

func mustCache(t *testing.T, home string, ce *ClusterEndpoints) {
	t.Helper()
	if err := WriteClusterEndpoints(home, ce); err != nil {
		t.Fatalf("write cache: %v", err)
	}
}

// #2 non-cluster byte-equivalence: no cache → dial == base, byte-for-byte.
func TestDialForNonClusterByteEquivalence(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary.example:443/p"
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("no cache must be byte-equivalent: dial=%q want %q", got, base)
	}
}

// #3/#4 operator override: --nats-url (flagChanged) AND $TETHER_NATS_URL pin to exactly one endpoint even
// with a matching cache present — never silently widened.
func TestDialForOperatorOverridePinsSingle(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "A", FloorURL: base, InviteSeeds: []string{"wss://survivor:443"}})

	if got := DialFor(true, base, home, time.Now()); got != base {
		t.Fatalf("--nats-url must pin single despite cache: dial=%q", got)
	}
	t.Setenv(DefaultBrokerURLEnv, base)
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("$TETHER_NATS_URL must pin single despite cache: dial=%q", got)
	}
}

// #23 TETHER_NO_DISCOVER escape hatch pins single.
func TestDialForNoDiscoverEnvPinsSingle(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "A", FloorURL: base, InviteSeeds: []string{"wss://survivor:443"}})
	t.Setenv(NoDiscoverEnv, "1")
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("TETHER_NO_DISCOVER must pin single: dial=%q", got)
	}
}

// #1 阶段4 repro (tier-1): a pinned cache with an OOB invite seed expands the dead configured broker into a
// floor-first list that CONTAINS the survivor → nats.Connect fails over.
func TestDialForFailoverViaInviteSeed(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443" // the configured broker (dead in the incident)
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "A", FloorURL: base, InviteSeeds: []string{"wss://survivor:443"}})
	got := DialFor(false, base, home, time.Now())
	if !strings.Contains(got, "wss://survivor:443") {
		t.Fatalf("failover dial must contain the survivor: %q", got)
	}
	if !strings.HasPrefix(got, base) {
		t.Fatalf("the configured broker must remain the floor (first): %q", got)
	}
}

// #14 FloorURL selection key: a cache learned under a DIFFERENT broker_url is ignored (no stale
// cross-cluster expansion).
func TestDialForFloorURLKeyIgnoresStale(t *testing.T) {
	home := t.TempDir()
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "A", FloorURL: "wss://old:443", InviteSeeds: []string{"wss://survivor:443"}})
	base := "wss://new:443"
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("cache keyed to a different broker_url must be ignored: dial=%q want %q", got, base)
	}
}

// #11(partial) unpinned cache never expands (the cli never trusts an endpoint set without an OOB pin).
func TestDialForUnpinnedCacheNeverExpands(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "", FloorURL: base, InviteSeeds: []string{"wss://survivor:443"}})
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("unpinned cache must NOT expand: dial=%q", got)
	}
}

// #6/#7/#8 signed seeds are re-verified against the pin on every read; a foreign-account or expired bundle
// is DROPPED (fail-closed → floor-only), while a valid one expands.
func TestDialForSignedSeedsVerifyAndReject(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	now := time.Unix(1700000000, 0).UTC()
	sb, err := clusterroster.BuildSeeds(seed, pub, 3, []string{"wss://survivor:443"}, "",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("build seeds: %v", err)
	}

	// (a) valid pin → seeds expand
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: pub, FloorURL: base, SeedGen: 3, Seeds: sb})
	if got := DialFor(false, base, home, now.Add(time.Minute)); !strings.Contains(got, "wss://survivor:443") {
		t.Fatalf("valid signed seeds must expand: %q", got)
	}

	// (b) foreign-account pin (#7) → seeds dropped → floor-only
	_, otherPub := mustUserKey(t)
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: otherPub, FloorURL: base, SeedGen: 3, Seeds: sb})
	if got := DialFor(false, base, home, now.Add(time.Minute)); got != base {
		t.Fatalf("account-mismatch seeds must be dropped → floor-only: %q", got)
	}

	// (c) expired (#8) → dropped → floor-only
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: pub, FloorURL: base, SeedGen: 3, Seeds: sb})
	if got := DialFor(false, base, home, now.Add(2*time.Hour)); got != base {
		t.Fatalf("expired seeds must be dropped → floor-only: %q", got)
	}
}

func mustUserKey(t *testing.T) (string, string) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("gen seed: %v", err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	return string(seed), pub
}

// #13 cache is corrupt-tolerant (garbage JSON → treated as absent → floor-only, no panic) and written 0600.
func TestClusterEndpointsCorruptTolerantAnd0600(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	if err := os.WriteFile(ClusterEndpointsPath(home), []byte("{not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := DialFor(false, base, home, time.Now()); got != base {
		t.Fatalf("corrupt cache must fall back to floor: dial=%q", got)
	}
	mustCache(t, home, &ClusterEndpoints{PinAccountPub: "A", FloorURL: base})
	fi, err := os.Stat(ClusterEndpointsPath(home))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("cache mode = %v, want 0600", fi.Mode().Perm())
	}
}

// ResolveBrokerURLSource precedence: flag > env > file > default, with the source tag the expansion gate
// relies on.
func TestResolveBrokerURLSource(t *testing.T) {
	home := t.TempDir()
	// default (no file, no env)
	if u, s := ResolveBrokerURLSource("nats://d:4222", false, home); u != "nats://d:4222" || s != SourceDefault {
		t.Fatalf("default: %q %v", u, s)
	}
	// file
	if err := WriteDefaultBrokerURL(home, "wss://file:443"); err != nil {
		t.Fatal(err)
	}
	if u, s := ResolveBrokerURLSource("nats://d:4222", false, home); u != "wss://file:443" || s != SourceFile {
		t.Fatalf("file: %q %v", u, s)
	}
	// env beats file
	t.Setenv(DefaultBrokerURLEnv, "wss://env:443")
	if u, s := ResolveBrokerURLSource("nats://d:4222", false, home); u != "wss://env:443" || s != SourceEnv {
		t.Fatalf("env: %q %v", u, s)
	}
	// flag beats env
	if u, s := ResolveBrokerURLSource("wss://flag:443", true, home); u != "wss://flag:443" || s != SourceFlag {
		t.Fatalf("flag: %q %v", u, s)
	}
}
