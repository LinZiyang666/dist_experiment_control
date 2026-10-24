package xferserve

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type idleCensus struct{}

func (idleCensus) Responsible(string, controlbudget.Deadline) (int, error) { return 0, nil }

func assemblyHost(t *testing.T, base string) (Host, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return Host{DBPath: filepath.Join(base, "state.db"), BrokerID: "single", Conn: func() *nats.Conn { return nc }, Census: idleCensus{},
		Tracker: func() x.TrackerManager { return nil }, Ledger: func() x.LedgerPort { return nil },
		Rows: func(x.Identity, x.Deadline) (x.LedgerRow, error) { return x.LedgerRow{}, errors.New("no rows") }}, js
}

func assemblyDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// A graph reinstalled on the same origin (cut rollback, or the restored
// process of the same boot) is its retired bucket-config writer's successor:
// it presents the joined retirement proof, takes the next generation, and is
// refused authority while a recorded attempt is still sent-unknown or the
// record is unreadable. Re-bootstrapping generation one would be a second
// writer for the same buckets.
// origin: hot-update S7 plan §2.1 / T05 — lead follow-up 2 item 3 (ConfigAttempts had no production producer)
func TestTransferConfigWriterSucceedsAcrossGraphReinstall(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseConfigWriterReinstall(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "config writer reinstall", g)
	testharness.AssertNoFDLeak(t, "config writer reinstall", fd, 0)
}

func exerciseConfigWriterReinstall(t *testing.T) {
	t.Helper()
	base := t.TempDir()
	host, js := assemblyHost(t, base)
	var origin x.Origin
	var retired x.ConfigWriterState
	for want := uint64(1); want <= 3; want++ {
		d := assemblyDeadline(t)
		asm, err := Assemble(host, js, d)
		if err != nil {
			t.Fatalf("assembly %d: %v", want, err)
		}
		if asm.Generation != want {
			t.Fatalf("writer generation = %d, want %d (a reinstall must succeed its retired writer)", asm.Generation, want)
		}
		if want == 1 {
			origin = asm.Config.Origin
		} else if asm.Config.Origin != origin {
			t.Fatalf("same-boot reinstall changed origin %+v -> %+v", origin, asm.Config.Origin)
		}
		if _, err := asm.Local.Apply(x.ConfigPatch{Bucket: "xfer-succession"}, d); err != nil {
			t.Fatalf("generation %d config write: %v", want, err)
		}
		// Force a real mutation in every generation: an INFO-only ensure has
		// correctly issued no JetStream config request to checkpoint.
		if _, err := asm.Local.(xferbucket.Shrinker).Shrink(x.ConfigPatch{Bucket: "xfer-succession", MaxBytes: (1 << 30) - int64(want)*(64<<20)}, d); err != nil {
			t.Fatalf("generation %d shrink: %v", want, err)
		}
		if err := asm.Local.Stop(d); err != nil {
			t.Fatal(err)
		}
		if err := asm.RetireConfigWriter(d); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(asm.Root, "config-writer.json"))
		if err != nil {
			t.Fatal(err)
		}
		var stored configWriterFile
		if err := json.Unmarshal(data, &stored); err != nil || stored.Generation != want || !stored.State.Admission.Closed ||
			len(stored.State.Proofs) != 1 || !stored.State.Proofs[0].RequestsJoined {
			t.Fatalf("retirement record = %+v %v", stored, err)
		}
		if len(stored.State.Attempts) == 0 {
			t.Fatal("config writer dropped its attempt")
		}
		attempt := stored.State.Attempts[0].Attempt
		if attempt.LogicalID == "" || attempt.Ordinal == 0 || !controlbudget.ValidPhase(attempt.Phase) || attempt.StartedMonoNS <= 0 || attempt.FinishedMonoNS < attempt.StartedMonoNS {
			t.Fatalf("config writer did not record its real request: %+v", attempt)
		}
		retired = stored.State
	}
	record := filepath.Join(base, "xfer-continuation", origin.InstanceID, "config-writer.json")
	for _, tc := range []struct {
		name  string
		write func() error
		code  string
	}{
		{"sent-unknown attempt", func() error {
			state := retired
			state.Attempts = []x.ConfigAttempt{{Patch: x.ConfigPatch{Bucket: "xfer-succession"}, Proof: state.Proofs[0], SentUnknown: true}}
			return writeDurable(record, configWriterFile{Generation: 3, State: state})
		}, x.CodeResultUnknown},
		{"unreadable record", func() error { return os.WriteFile(record, []byte("{"), 0o600) }, "refusing a second writer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.write(); err != nil {
				t.Fatal(err)
			}
			if _, err := Assemble(host, js, assemblyDeadline(t)); err == nil || !strings.Contains(err.Error(), tc.code) {
				t.Fatalf("assembly with %s = %v, want %s", tc.name, err, tc.code)
			}
		})
	}
}
