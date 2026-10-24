package maintenance

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S6 internal review SINGLEACK F3 / lead decision D-C / plan §7.1
func TestSingleMaintenanceOwnerRetainsCASAndFixedProof(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("maintenance proof requires the Linux boot clock")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSingleMaintenanceOwner(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "single maintenance owner", g)
	testharness.AssertNoFDLeak(t, "single maintenance owner", fd, 0)
}

func exerciseSingleMaintenanceOwner(t *testing.T) {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	r, _, baseline, sign, verify := maintenanceFixture(t)
	r.Mode, r.Home, r.IngressServerID = "single", "", "single-ingress"
	r.Term, r.ConfigIndex, r.VoterDigest = 0, 0, ""
	r.IssuerBootID, err = procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	r.IssuedBootNS, err = procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	r.ExpiresBootNS = r.IssuedBootNS + int64(brokermaint.MaxLifetime)
	baseline.Home = ""
	r.BaselineDigest, err = MaintenanceBaselineDigest(baseline)
	if err != nil {
		t.Fatal(err)
	}
	c := LocalMaintenanceConfig{DB: db}
	commit := func(next brokermaint.Record) error { return commitLocalMaintenance(c, next, baseline) }
	lease, err := EstablishLocalMaintenance(r, nil, commit, nil, sign, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, err := ReadLocalMaintenance(db, "")
	if err != nil || current != lease.Record || current.Phase != brokermaint.MaintActive {
		t.Fatalf("single owner was not committed/read back: %+v %v", current, err)
	}
	// Image serialization must retain the original proof and expiry. Import
	// cannot reconstruct either by adding a fresh lifetime to its current clock.
	raw, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	var retained brokermaint.LocalLease
	if err := json.Unmarshal(raw, &retained); err != nil || retained != lease {
		t.Fatalf("local image changed its proof or expiry: %v", err)
	}
	bound, err := BindLocalMaintenance(db, retained)
	if err != nil || bound != lease || bound.ExpiresLocalBootNS != r.ExpiresBootNS {
		t.Fatalf("single image failed to retain exact local owner: %+v %v", bound, err)
	}
	proofBody, err := brokermaint.CanonicalProofBytes(retained.Proof)
	if err != nil || verify(proofBody, retained.Proof.Sig) != nil {
		t.Fatal("retained single proof lost its signature")
	}
	for _, mutate := range []func(*brokermaint.Record){
		func(next *brokermaint.Record) { next.Holder = "other-holder" },
		func(next *brokermaint.Record) { next.ExpiresBootNS++ },
		func(next *brokermaint.Record) { next.CutID, next.Phase = "new-cut", brokermaint.MaintPrepared },
	} {
		next := current
		mutate(&next)
		if err := commit(next); err == nil {
			t.Fatal("single owner accepted a competing holder, renewed TTL or live replacement")
		}
	}
	stale := current
	ended := current
	ended.Phase = brokermaint.MaintEnded
	if err := commit(ended); err != nil {
		t.Fatal(err)
	}
	// The same old active expectation cannot overwrite an ended owner, even
	// while its original fixed deadline has not yet elapsed.
	if _, err := PlanMaintenanceRecord(db, &stale, ended, baseline); !errors.Is(err, brokermaint.ErrConflict) {
		t.Fatalf("stale single CAS passed: %v", err)
	}
	next := r
	next.CutID, next.Holder = "next-cut", "local-next-cut"
	if err := commit(next); err != nil {
		t.Fatal(err)
	}
	bound, err = BindLocalMaintenance(db, retained)
	if err != nil || bound.Record.CutID != "" {
		t.Fatalf("old image reclaimed the next local owner: %+v %v", bound, err)
	}
}
