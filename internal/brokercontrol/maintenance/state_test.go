package maintenance

import (
	"bytes"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

func maintenanceFixture(t testing.TB) (brokermaint.Record, brokermaint.Challenge, MaintenanceBaseline, brokermaint.SignProof, brokermaint.VerifySignature) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	sign := func(b []byte) (string, error) { return hex.EncodeToString(ed25519.Sign(key, b)), nil }
	verify := func(b []byte, sig string) error {
		s, err := hex.DecodeString(sig)
		if err != nil || !ed25519.Verify(key.Public().(ed25519.PublicKey), b, s) {
			return brokermaint.ErrIdentity
		}
		return nil
	}
	b := MaintenanceBaseline{Home: "home-a", InstanceID: "instance-a", DwellNS: int64(4 * time.Second), PlannedDown: true, ReturnPending: true}
	digest, err := MaintenanceBaselineDigest(b)
	if err != nil {
		t.Fatal(err)
	}
	r := brokermaint.Record{CutID: "cut-a", Holder: "holder-a", Home: b.Home, InstanceID: b.InstanceID, IssuerBootID: "issuer-boot", Mode: "quorum", Generation: 3, Phase: brokermaint.MaintPrepared, IssuedBootNS: int64(100 * time.Second), ExpiresBootNS: int64(100*time.Second + brokermaint.MaxLifetime), Term: 9, ConfigIndex: 12, VoterDigest: strings.Repeat("a", 64), BaselineDigest: digest, RequiredCapability: brokermaint.Capability}
	c := brokermaint.Challenge{Nonce: "nonce-a", ObserverBootID: "observer-boot", IssuerBootID: r.IssuerBootID, SentBootNS: int64(time.Second)}
	return r, c, b, sign, verify
}

// origin: hot-update S6 plan §7.1 / T39
func TestMaintenanceCachePreservesExpiryAndBaseline(t *testing.T) {
	r, c, baseline, sign, verify := maintenanceFixture(t)
	cache, err := NewMaintenanceCache(c.ObserverBootID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := brokermaint.MakeProof(r, c.Nonce, r.IssuerBootID, r.IssuedBootNS, 42, sign)
	if err != nil {
		t.Fatal(err)
	}
	if err = cache.Observe(r, p, c, baseline, c.SentBootNS, 42, verify); err != nil {
		t.Fatal(err)
	}
	expiry := c.SentBootNS + int64(brokermaint.MaxLifetime)
	snapshot := cache.Snapshot()
	if snapshot.Observations[0].Lease.ExpiresLocalBootNS != expiry {
		t.Fatal("initial cache expiry changed")
	}
	// Actual serialization exercises the value projection P02 must include.
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MaintenanceSnapshot
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreMaintenanceCache(decoded, c.ObserverBootID, verify)
	if err != nil {
		t.Fatal(err)
	}
	for _, now := range []int64{expiry - 1, expiry, expiry + int64(time.Second)} {
		b, protected := restored.Lookup(r, now)
		if b != baseline || protected != (now < expiry) {
			t.Fatalf("restored deadline/dwell changed: now=%d baseline=%+v protection=%v", now, b, protected)
		}
	}
	// A new nonce and concurrent callbacks cannot extend a cached deadline.
	later := c
	later.Nonce = "nonce-b"
	later.SentBootNS += int64(3 * time.Second)
	p2, err := brokermaint.MakeProof(r, later.Nonce, r.IssuerBootID, r.IssuedBootNS+int64(time.Second), 43, sign)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if e := restored.Observe(r, p2, later, baseline, later.SentBootNS, 42, verify); e != nil {
				t.Error(e)
			}
			_ = restored.Snapshot()
		})
	}
	wg.Wait()
	if got := restored.Snapshot().Observations[0].Lease.ExpiresLocalBootNS; got != expiry {
		t.Fatalf("new leader/proof gave fresh TTL: %d", got)
	}
	refreshed := restored.Snapshot()
	if _, err := RestoreMaintenanceCache(refreshed, c.ObserverBootID, verify); err != nil {
		t.Fatal("minimum-expiry proof did not survive refresh", err)
	}
	refreshed.Observations[0].Lease.ExpiresLocalBootNS++
	if _, err := RestoreMaintenanceCache(refreshed, c.ObserverBootID, verify); err == nil {
		t.Fatal("newest proof hid the original minimum expiry")
	}
	if err := restored.Observe(r, p2, later, baseline, expiry, 42, verify); !errors.Is(err, brokermaint.ErrExpired) {
		t.Fatalf("expired cut renewed: %v", err)
	}
	if _, err := RestoreMaintenanceCache(decoded, "rebooted-observer", verify); err == nil {
		t.Fatal("rebooted cache protected home")
	}
	for _, mutate := range []func(*MaintenanceSnapshot){
		func(s *MaintenanceSnapshot) { s.Observations[0].Lease.ExpiresLocalBootNS++ },
		func(s *MaintenanceSnapshot) { s.Observations[0].Baseline.DwellNS = 0 },
		func(s *MaintenanceSnapshot) { s.Observations[0].Challenge.ObserverBootID = "reboot" },
		func(s *MaintenanceSnapshot) { s.Observations[0].Lease.Proof.Sig = "bad" },
		func(s *MaintenanceSnapshot) { s.Observations = append(s.Observations, s.Observations[0]) },
	} {
		s := cache.Snapshot()
		mutate(&s)
		if _, e := RestoreMaintenanceCache(s, c.ObserverBootID, verify); e == nil {
			t.Fatal("corrupt snapshot accepted")
		}
	}
	ended := r
	ended.Phase = brokermaint.MaintEnded
	if err := restored.End(r, ended); err != nil {
		t.Fatal(err)
	}
	if b, on := restored.Lookup(r, c.SentBootNS); on || b != baseline {
		t.Fatal("ending cut lost baseline or kept protection")
	}
	if err := restored.Observe(r, p, c, baseline, c.SentBootNS, 42, verify); err == nil {
		t.Fatal("late reply resurrected ended cut")
	}
	endedCache, err := RestoreMaintenanceCache(restored.Snapshot(), c.ObserverBootID, verify)
	if err != nil {
		t.Fatal(err)
	}
	if b, on := endedCache.Lookup(r, c.SentBootNS); on || b != baseline {
		t.Fatal("ended import reset planned return")
	}
	other := r
	other.Home = "home-b"
	if _, on := restored.Lookup(other, c.SentBootNS); on {
		t.Fatal("unrelated home protected")
	}
}

func readMaintenanceRecord(t *testing.T, db *sql.DB, r brokermaint.Record) brokermaint.Record {
	t.Helper()
	var raw string
	if err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", brokermaint.MetaKeyPrefix+r.Home).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	got, err := brokermaint.DecodeRecord([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// origin: hot-update S6 plan §7.1 / T39
func TestMaintenanceRecordExactCAS(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	r, _, baseline, _, _ := maintenanceFixture(t)
	create, err := PlanMaintenanceRecord(db, nil, r, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if create.Op != cluster.OpClusterMetaSet || len(create.Body) != 2 || len(create.Body[0].Args) != 0 || len(create.Body[1].Args) != 0 {
		t.Fatal("new op or nonliteral maintenance command")
	}
	if err = cluster.ExecCommand(db, create); err != nil {
		t.Fatal(err)
	}
	if got := readMaintenanceRecord(t, db, r); got != r {
		t.Fatal("record readback changed")
	}
	if got, e := ReadMaintenanceBaseline(db, r); e != nil || got != baseline {
		t.Fatal("durable historical baseline missing", e)
	}
	if _, err = PlanMaintenanceRecord(db, nil, r, baseline); !errors.Is(err, brokermaint.ErrConflict) {
		t.Fatal("duplicate acquisition accepted", err)
	}
	active := r
	active.Phase = brokermaint.MaintActive
	activate, err := PlanMaintenanceRecord(db, &r, active, baseline)
	if err != nil {
		t.Fatal(err)
	}
	ended := r
	ended.Phase = brokermaint.MaintEnded
	end, err := PlanMaintenanceRecord(db, &r, ended, baseline)
	if err != nil {
		t.Fatal(err)
	}
	// Park the ACTIVE plan while an abort commits. Applied SQL must not overwrite
	// the changed phase merely because holder/cut/epoch still match.
	if err = cluster.ExecCommand(db, end); err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, activate); err != nil {
		t.Fatal(err)
	}
	if got := readMaintenanceRecord(t, db, r); got != ended {
		t.Fatal("late ACTIVE overwrote ended authority")
	}
	newCut := r
	newCut.CutID = "cut-b"
	newCut.Holder = "holder-b"
	newCut.IssuedBootNS = r.ExpiresBootNS
	newCut.ExpiresBootNS = newCut.IssuedBootNS + int64(brokermaint.MaxLifetime)
	newBaseline := baseline
	newBaseline.DwellNS += int64(time.Second)
	newBaseline.ReturnPending = false
	newCut.BaselineDigest, err = MaintenanceBaselineDigest(newBaseline)
	if err != nil {
		t.Fatal(err)
	}
	acquire, err := PlanMaintenanceRecord(db, &ended, newCut, newBaseline)
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, acquire); err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, create); err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, end); err != nil {
		t.Fatal(err)
	}
	if got := readMaintenanceRecord(t, db, r); got != newCut {
		t.Fatal("stale acquisition/end damaged new holder")
	}
	if got, e := ReadMaintenanceBaseline(db, newCut); e != nil || got != newBaseline {
		t.Fatal("stale plan overwrote new cut baseline", e)
	}
	if _, err = PlanMaintenanceRecord(db, &r, active, baseline); !errors.Is(err, brokermaint.ErrConflict) {
		t.Fatal("stale planning read accepted", err)
	}
	renewed := newCut
	renewed.ExpiresBootNS++
	if _, err = PlanMaintenanceRecord(db, &newCut, renewed, newBaseline); err == nil {
		t.Fatal("fixed expiry changed")
	}
	active = newCut
	active.Phase = brokermaint.MaintActive
	valid, err := PlanMaintenanceRecord(db, &newCut, active, newBaseline)
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, valid); err != nil {
		t.Fatal(err)
	}
	if got := readMaintenanceRecord(t, db, r); got != active {
		t.Fatal("valid ACTIVE transition refused")
	}
	baselineRaw, err := json.Marshal(newBaseline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE cluster_meta SET value=? WHERE key=?", string(baselineRaw)+" ", brokermaint.MetaKeyPrefix+"baseline/"+active.Home); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMaintenanceBaseline(db, active); err == nil {
		t.Fatal("noncanonical durable baseline accepted")
	}
	wrongBaseline := newBaseline
	wrongBaseline.DwellNS++
	wrongRaw, err := json.Marshal(wrongBaseline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE cluster_meta SET value=? WHERE key=?", string(wrongRaw), brokermaint.MetaKeyPrefix+"baseline/"+active.Home); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMaintenanceBaseline(db, active); err == nil {
		t.Fatal("canonical baseline digest mismatch accepted")
	}
	if _, err := db.Exec("UPDATE cluster_meta SET value='{}' WHERE key=?", brokermaint.MetaKeyPrefix+"baseline/"+active.Home); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMaintenanceBaseline(db, active); err == nil {
		t.Fatal("corrupt durable baseline accepted")
	}
}

func signMaintenanceAck(t *testing.T, ack *proto.BrokerMaintenanceAck, sign brokermaint.SignProof) {
	t.Helper()
	b, err := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID, ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
	if err != nil {
		t.Fatal(err)
	}
	ack.Sig, err = sign(b)
	if err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §7.1 / T39
func TestMaintenanceRequiresEveryObserverWindow(t *testing.T) {
	r, _, _, sign, verify := maintenanceFixture(t)
	r.Phase = brokermaint.MaintActive
	now := r.IssuedBootNS + int64(time.Second)
	required := []MaintenanceObserver{
		{ID: "voter-a", NID: "voter-a", ConnectedBrokerID: r.Home, RouteDigest: strings.Repeat("c", 64), Nonce: "nonce-voter", ProofDigest: strings.Repeat("d", 64), Capability: brokermaint.Capability, SentBootNS: r.IssuedBootNS},
		{ID: "agent-a", NID: "agent-a", ConnectedBrokerID: r.Home, RouteDigest: strings.Repeat("e", 64), Nonce: "nonce-agent", ProofDigest: strings.Repeat("f", 64), Capability: brokermaint.Capability, SentBootNS: r.IssuedBootNS},
	}
	acks := make([]MaintenanceAck, len(required))
	for i, o := range required {
		proof, e := brokermaint.MakeProof(r, o.Nonce, r.IssuerBootID, r.IssuedBootNS, 42, sign)
		if e != nil {
			t.Fatal(e)
		}
		digest, e := brokermaint.ProofDigest(proof)
		if e != nil {
			t.Fatal(e)
		}
		required[i].ProofDigest = digest
		o = required[i]
		a := proto.BrokerMaintenanceAck{CutID: r.CutID, Nonce: o.Nonce, NID: o.NID, ConnectedBrokerID: o.ConnectedBrokerID, InstanceID: r.InstanceID, RouteDigest: o.RouteDigest, ProofDigest: o.ProofDigest, Accepted: true, RemainingNS: int64(brokermaint.MaxLifetime - time.Second)}
		signMaintenanceAck(t, &a, sign)
		acks[i] = MaintenanceAck{ObserverID: o.ID, Reply: a}
	}
	check := func(_ MaintenanceObserver, b []byte, sig string) error { return verify(b, sig) }
	deadline, err := MaintenanceCutDeadline(r, r.IssuerBootID, now, required, acks, check)
	if err != nil || deadline != r.IssuedBootNS+int64(brokermaint.MaxLifetime-time.Second) {
		t.Fatal("valid observer cohort refused", deadline, err)
	}
	// Exactly five seconds remains; one nanosecond less must defer despite the
	// issuer still having plenty of its own lifetime left.
	acks[1].Reply.RemainingNS = int64(upgradebudget.RequestLatency) + now - required[1].SentBootNS
	signMaintenanceAck(t, &acks[1].Reply, sign)
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, acks, check); err != nil {
		t.Fatal("exact normal cut window refused", err)
	}
	acks[1].Reply.RemainingNS--
	signMaintenanceAck(t, &acks[1].Reply, sign)
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, acks, check); !errors.Is(err, brokermaint.ErrExpired) {
		t.Fatal("early observer expiry ignored", err)
	}
	acks[1].Reply.RemainingNS = int64(brokermaint.MaxLifetime - time.Second)
	signMaintenanceAck(t, &acks[1].Reply, sign)
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, acks[:1], check); !errors.Is(err, ErrMaintenanceObserverUnsupported) {
		t.Fatal("missing ACK allowed cut", err)
	}
	old := append([]MaintenanceObserver(nil), required...)
	old[1].Capability = ""
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, old, acks, check); !errors.Is(err, ErrMaintenanceObserverUnsupported) {
		t.Fatal("old observer allowed cut", err)
	}
	if _, err = MaintenanceCutDeadline(r, "reboot", now, required, acks, check); err == nil {
		t.Fatal("unknown issuer boot allowed cut")
	}
	refused := append([]MaintenanceAck(nil), acks...)
	refused[1].Reply.Accepted = false
	signMaintenanceAck(t, &refused[1].Reply, sign)
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, refused, check); err == nil {
		t.Fatal("signed hold refusal allowed cut")
	}
	for i := 0; i < reflect.TypeFor[proto.BrokerMaintenanceAck]().NumField(); i++ {
		bad := append([]MaintenanceAck(nil), acks...)
		value := reflect.ValueOf(&bad[1].Reply).Elem().Field(i)
		switch value.Kind() {
		case reflect.String:
			value.SetString("changed")
		case reflect.Bool:
			value.SetBool(false)
		case reflect.Int64:
			value.SetInt(value.Int() - 1)
		}
		if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, bad, check); err == nil {
			t.Fatalf("changed ACK field accepted: %s", reflect.TypeFor[proto.BrokerMaintenanceAck]().Field(i).Name)
		}
	}
	duplicate := append([]MaintenanceAck(nil), acks...)
	duplicate[1] = duplicate[0]
	if _, err = MaintenanceCutDeadline(r, r.IssuerBootID, now, required, duplicate, check); err == nil {
		t.Fatal("duplicate cohort member accepted")
	}
	// Single binds the actual NATS ingress while SQL home remains empty.
	single := r
	single.Mode = "single"
	single.Home = ""
	single.IngressServerID = "nats-ingress"
	single.Term = 0
	single.ConfigIndex = 0
	single.VoterDigest = ""
	for i := range required {
		required[i].ConnectedBrokerID = ""
		required[i].IngressServerID = single.IngressServerID
		acks[i].Reply.ConnectedBrokerID = ""
		acks[i].Reply.IngressServerID = single.IngressServerID
		signMaintenanceAck(t, &acks[i].Reply, sign)
	}
	if _, err = MaintenanceCutDeadline(single, single.IssuerBootID, now, required, acks, check); err != nil {
		t.Fatal("single ingress cohort refused", err)
	}
	acks[1].Reply.IngressServerID = "different-ingress"
	signMaintenanceAck(t, &acks[1].Reply, sign)
	if _, err = MaintenanceCutDeadline(single, single.IssuerBootID, now, required, acks, check); err == nil {
		t.Fatal("wrong single ingress allowed cut")
	}
}

// origin: hot-update S6 plan §12.1 P15 / §9.3
func TestBrokerMaintenanceLeakCycles(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	beforeIdentities := maintenanceFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseMaintenanceExpiry(t)
	}
	testharness.AssertNoGoroutineLeak(t, "maintenance expiry", beforeG)
	testharness.AssertNoFDLeak(t, "maintenance expiry", beforeFD, 0)
	if after := maintenanceFDIdentities(t); !reflect.DeepEqual(beforeIdentities, after) {
		t.Fatal("maintenance changed the original FD identity multiset")
	}
}

// Counts alone would miss closing an unrelated owner and leaking a new handle.
// /proc's socket/pipe inode names and file paths retain descriptor provenance.
func maintenanceFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		if runtime.GOOS != "linux" {
			return nil
		}
		t.Fatal(err)
	}
	identities := make(map[string]int)
	for _, entry := range entries {
		identity, err := os.Readlink("/proc/self/fd/" + entry.Name())
		// ReadDir's own descriptor has already closed; it is not an owner.
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		identities[identity]++
	}
	return identities
}

func exerciseMaintenanceExpiry(t *testing.T) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	r, c, baseline, sign, verify := maintenanceFixture(t)
	cache, err := NewMaintenanceCache(c.ObserverBootID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := brokermaint.MakeProof(r, c.Nonce, r.IssuerBootID, r.IssuedBootNS, 7, sign)
	if err != nil {
		t.Fatal(err)
	}
	if err = cache.Observe(r, p, c, baseline, c.SentBootNS, 7, verify); err != nil {
		t.Fatal(err)
	}
	cmd, err := PlanMaintenanceRecord(db, nil, r, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	current := readMaintenanceRecord(t, db, r)
	durableBaseline, err := ReadMaintenanceBaseline(db, current)
	if err != nil || durableBaseline != baseline {
		t.Fatal("replicated baseline mismatch", err)
	}
	restored, err := RestoreMaintenanceCache(cache.Snapshot(), c.ObserverBootID, verify)
	if err != nil {
		t.Fatal(err)
	}
	if b, on := restored.Lookup(current, c.SentBootNS+int64(brokermaint.MaxLifetime)); b != baseline || on {
		t.Fatal("expiry lost history or protected crashed home")
	}
}

// origin: hot-update S6 plan §13
func FuzzMaintenanceBaselineCanonical(f *testing.F) {
	r, _, baseline, _, _ := maintenanceFixture(f)
	canonical, err := json.Marshal(baseline)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(canonical)
	f.Add(append(append([]byte(nil), canonical...), ' '))
	f.Add([]byte("{}"))
	f.Add([]byte("null"))
	db, err := storage.Open(":memory:")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() {
		if err := db.Close(); err != nil {
			f.Error(err)
		}
	})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if _, err := db.Exec("INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", brokermaint.MetaKeyPrefix+"baseline/"+r.Home, string(raw)); err != nil {
			t.Fatal(err)
		}
		got, err := ReadMaintenanceBaseline(db, r)
		if err == nil && (got != baseline || !bytes.Equal(raw, canonical)) {
			t.Fatal("accepted noncanonical or mismatched durable baseline")
		}
	})
}
