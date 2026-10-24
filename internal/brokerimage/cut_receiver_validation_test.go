//go:build linux

package brokerimage

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The exporter must not seal (and later exec) an image its own receiver's
// Decode/Validate refuses. A post-exec Boot refusal has no pristine owner and
// turns a planned hot cut into a fail-stop restart after the writer closed.
// origin: hot-update S6 internal review rv-writer
func TestBrokerPreparedBundleDecodesAtReceiver(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exercisePreparedReceiverBundle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "prepared receiver bundle", beforeG)
	testharness.AssertNoFDLeak(t, "prepared receiver bundle", beforeFD, 0)
}

func exercisePreparedReceiverBundle(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tether.db")
	db, err := storage.OpenWAL("file:" + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	identity, err := storage.CanonicalStoreIdentity(path, dir)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, identity)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	f := newCutFixture(t)
	f.store.stamp = cloneStamp(stamp)
	f.store.seal = storage.StoreSeal{Stamp: cloneStamp(stamp), Writer: storage.CloseClosed, ReadOnly: storage.CloseClosed,
		Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed}
	cut := freezeFixture(t, f)
	bundle, resources := exportFixture(t, f, cut)
	_, sealed := prepareFixture(t, cut, bundle, resources) // Prepare accepted this bundle
	data, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	state := handoff.State{Role: ImageRole, Instance: sealed.InstanceID, Generation: sealed.Generation, Data: data}
	if _, err := Decode(state); err != nil {
		fixed := sealed
		fixed.Maintenance = []brokermaint.LocalLease{}
		fixedData, _ := json.Marshal(fixed)
		_, fixedErr := Decode(handoff.State{Role: ImageRole, Instance: fixed.InstanceID, Generation: fixed.Generation, Data: fixedData})
		t.Fatalf("Prepare sealed a bundle the receiver rejects: %v (with maintenance=[]: %v)", err, fixedErr)
	}
}

// Adapter metadata must satisfy the receiver before Prepare creates exec aliases.
// These malformed fields passed the earlier, weaker bundle identity predicate.
// origin: hot-update S6 plan §4.1; internal review rv-writer-10
func TestBrokerPrepareRejectsReceiverInvalidMetadata(t *testing.T) {
	for _, defect := range []string{"maintenance", "path-binding", "listener-flags", "argument-size", "argument-utf8"} {
		t.Run(defect, func(t *testing.T) {
			f := newCutFixture(t)
			if defect == "path-binding" {
				f.store.stamp.Identity.LockPath = "/different/tether.lock"
				f.store.seal.Stamp = cloneStamp(f.store.stamp)
			}
			f.keeper.invalidFlags = defect == "listener-flags"
			cut := freezeFixture(t, f)
			bundle, resources := exportFixture(t, f, cut)
			bundle.Generation, bundle.InstanceID, bundle.BootID = 2, "instance", "boot"
			bundle.ExporterSHA256, bundle.CandidateSHA256 = strings.Repeat("a", 64), strings.Repeat("b", 64)
			bundle.PreviousSHA256, bundle.ConfigDigest = bundle.ExporterSHA256, strings.Repeat("c", 64)
			bundle.Args, bundle.StateResource = []string{"tether", "serve"}, "broker.state"
			switch defect {
			case "maintenance":
				bundle.Maintenance = nil
			case "argument-size":
				bundle.Args[1] = strings.Repeat("x", 65537)
			case "argument-utf8":
				bundle.Args[1] = string([]byte{0xff})
			}
			snapshot, err := sealedstate.New(bundle.StateResource, []byte{1})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = snapshot.Close() }()
			prepared, err := cut.Prepare(bundle, append(resources, snapshot.Resources...))
			if prepared != nil {
				_ = prepared.Close()
			}
			if prepared != nil || !errors.Is(err, ErrImage) {
				t.Fatalf("receiver-invalid metadata sealed: prepared=%v err=%v", prepared != nil, err)
			}
		})
	}
}

// Final validation under the exec latch is mandatory even when the caller's
// optional reader/domain callback succeeds.
// origin: hot-update S6 plan §4.1 / §6.2; internal review rv-writer-10
func TestBrokerExecCommitRevalidatesReceiverMetadata(t *testing.T) {
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	bundle, resources := exportFixture(t, f, cut)
	prepared, bundle := prepareFixture(t, cut, bundle, resources)
	cut.(*frozenCut).preparedBundle.Maintenance = nil
	called := false
	err := ExecCommit(cut, prepared, func(Bundle) error { called = true; return nil }, "/nonexistent-broker-image-test", bundle.Args, nil)
	if !errors.Is(err, ErrImage) || called {
		t.Fatalf("invalid final metadata reached caller/exec: err=%v called=%v", err, called)
	}
}
