//go:build linux

package xferagentimage

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// An agent image restore adopts the sealed worker as the handler's ONLY
// worker: a prepared entry crosses freeze → seal → decode → import, the
// imported owner thaws at the next generation with the same entry, and an
// owner whose handler was already built refuses the import instead of
// keeping two workers.
// origin: hot-update S7 plan §8.1–§8.2 / P21 / T14 — agent image cut continuity
func TestTransferImageImportAdoptsThePreparedEntry(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exercisePreparedImageAdoption(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exercisePreparedImageAdoption(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	build := func() *filetransfer.Handler {
		return &filetransfer.Handler{SID: "lab", Roots: []string{root}, Logger: testharness.SilentLog(),
			NID: func() string { return "node" }, Instance: func() string { return "agentinstance" },
			JetStream: func() jetstream.JetStream { return nil }}
	}
	d := func() x.Deadline {
		value, err := controlbudget.After(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var exporter filetransfer.WorkerOwner
	h := filetransfer.LoadWorkerHandler(&exporter, filetransfer.WorkerDependencies{}, build)
	if h.Runtime == nil || h.Worker == nil {
		t.Fatalf("production worker was not built: %v", h.WorkerError)
	}
	defer func() { _ = filetransfer.StopWorker(h) }()
	now := time.Now().UTC()
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "SHA256:" + strings.Repeat("A", 43), AgentInstance: "agentinstance",
		TransferID: "imported", EntryNonce: "entrynonce", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "objectversion"}
	origin := x.Origin{BrokerID: "single", InstanceID: "origininstance", BootID: "originboot", Generation: 1}
	sum := sha256.Sum256([]byte("retained"))
	prepare := x.Prepare{CallID: "imported-prepare", Identity: id, Origin: origin,
		Intent: x.Intent{Verb: "push", Tier: "b", Path: root + "/target", Size: 8, SHA256: hex.EncodeToString(sum[:]), ChunkSize: 32 << 10},
		Permit: x.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permitnonce", Signature: "signature"},
		Budget: x.BudgetSeal{StartedAt: now, Size: 8, EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(300 * time.Second), SourceSealed: true}}
	if _, err := h.Worker.Prepare(prepare, d()); err != nil {
		t.Fatal(err)
	}
	owner := Worker(h.Runtime)
	if owner == nil {
		t.Fatal("bound worker has no image owner")
	}
	frozen, err := owner.Freeze(d())
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil || len(resources) != 0 || state.Entries != 1 || state.Generation != 1 || state.InstanceID != "agentinstance" || len(state.Encoded) == 0 {
		t.Fatalf("prepared export: entries=%d generation=%d instance=%q bytes=%d resources=%d err=%v",
			state.Entries, state.Generation, state.InstanceID, len(state.Encoded), len(resources), err)
	}
	// The built owner refuses to adopt a second (imported) worker.
	if _, _, err := Importer(&exporter, filetransfer.WorkerDependencies{}, build).Import(state, nil); err == nil {
		t.Fatal("a built handler adopted a second worker")
	}
	var importer filetransfer.WorkerOwner
	sealed := Importer(&importer, filetransfer.WorkerDependencies{}, build)
	decoded, err := sealed.Decode(state.Encoded)
	if err != nil || decoded.Entries != 1 || decoded.Generation != 1 {
		t.Fatalf("decode: entries=%d generation=%d err=%v", decoded.Entries, decoded.Generation, err)
	}
	worker, salvage, err := sealed.Import(decoded, nil)
	if err != nil || worker == nil || len(salvage.Resources) != 0 {
		t.Fatalf("import = %v %+v", err, salvage)
	}
	adopted := importer.Loaded()
	if adopted == nil || adopted.Runtime == nil || adopted.Worker == nil {
		t.Fatal("import did not build the handler around the sealed worker")
	}
	defer func() { _ = filetransfer.StopWorker(adopted) }()
	if err := worker.Thaw(d()); err != nil {
		t.Fatal(err)
	}
	status, err := adopted.Worker.Status(id)
	if err != nil || status.Phase != x.PhasePrepared || status.Identity != id {
		t.Fatalf("imported entry = %+v %v", status, err)
	}
	// The next cut of the adopted worker carries it at the next generation.
	again, err := Worker(adopted.Runtime).Freeze(d())
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := again.Export()
	if err != nil || next.Generation != 2 || next.Entries != 1 {
		t.Fatalf("re-export: entries=%d generation=%d err=%v", next.Entries, next.Generation, err)
	}
	if err := Worker(adopted.Runtime).Thaw(d()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Thaw(d()); err != nil {
		t.Fatal(err)
	}
}
